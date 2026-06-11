// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Unified backfill and gap repair: fills the disk cache FROM ClickHouse.
//!
//! One task serves three hole sources — the cold-start window, slots the live
//! path deferred (incomplete head view, full queue, write errors), and holes
//! found by periodic coverage scans. Work is planned newest-first (recent slots
//! serve the most traffic), rate-limited, and resumable: coverage IS the
//! cursor, so every committed batch is durable progress and a restart simply
//! recomputes the remaining holes.
//!
//! The filler never writes RocksDB itself; it ships batches to the writer
//! thread's bulk channel, which always yields to live slots.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::mpsc::{SyncSender, TrySendError};
use std::time::{Duration, Instant};

use tracing::{debug, info, warn};

use crate::clickhouse::{BlockMetadataRecord, ClickHouseClient, StoredTransactionRecord};
use crate::head_cache::HeadCache;

use super::ingest::RepairQueue;
use super::writer::DiskWriteJob;
use super::{DiskCache, schema};

#[derive(Debug, Clone)]
pub(crate) struct FillerConfig {
    pub(crate) retain_slots: u64,
    /// Slots fetched per ClickHouse range query.
    pub(crate) slots_per_query: u64,
    /// Token-bucket rate limit on fetched slots.
    pub(crate) max_slots_per_sec: u64,
    /// Per-query deadline (range scans need more than interactive reads).
    pub(crate) query_timeout: Duration,
    /// Idle wait between planning rounds when there is nothing to do.
    pub(crate) repair_interval: Duration,
    /// Never fetch slots ClickHouse ingestion may not have landed yet.
    pub(crate) repair_min_lag_slots: u64,
    /// After this many incomplete or empty fill results a slot stays a hole
    /// (falls through to ClickHouse on reads) and is only logged once.
    pub(crate) max_attempts: u32,
}

impl Default for FillerConfig {
    fn default() -> Self {
        Self {
            retain_slots: 4_320_000,
            slots_per_query: 8,
            max_slots_per_sec: 50,
            query_timeout: Duration::from_secs(30),
            repair_interval: Duration::from_secs(5),
            repair_min_lag_slots: 75,
            max_attempts: 10,
        }
    }
}

/// Upper bound on slots planned per round; keeps each round's coverage
/// re-planning cheap and the shutdown latency low.
const MAX_SLOTS_PER_ROUND: u64 = 64;

pub(crate) async fn run(
    cache: Arc<DiskCache>,
    clickhouse: ClickHouseClient,
    bulk: SyncSender<DiskWriteJob>,
    repair: Arc<RepairQueue>,
    head: Option<Arc<HeadCache>>,
    cfg: FillerConfig,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) {
    info!(
        retain_slots = cfg.retain_slots,
        slots_per_query = cfg.slots_per_query,
        max_slots_per_sec = cfg.max_slots_per_sec,
        "disk cache: filler started"
    );

    let mut attempts: HashMap<u64, u32> = HashMap::new();
    let mut given_up: HashSet<u64> = HashSet::new();
    let mut backoff = Duration::from_millis(250);
    let max_backoff = Duration::from_secs(10);
    let mut tokens = f64::from(u32::try_from(cfg.slots_per_query).unwrap_or(8));
    let mut last_refill = Instant::now();

    loop {
        let window = match claimable_window(&cache, &clickhouse, head.as_deref(), &cfg).await {
            Some(window) => window,
            None => {
                if wait_or_shutdown(&mut shutdown, &repair, cfg.repair_interval).await {
                    break;
                }
                continue;
            }
        };

        prune_tracking(&mut attempts, &mut given_up, window.floor);

        let repair_slots = repair.drain(MAX_SLOTS_PER_ROUND as usize);
        let holes = cache.holes_in(window.floor, window.tip);
        let hole_slots: u64 = holes.iter().map(|&(start, end)| end - start + 1).sum();
        crate::metrics::disk_cache_backfill_remaining(hole_slots + repair.len() as u64);
        let ranges = plan_ranges(
            &repair_slots,
            &holes,
            &given_up,
            window.floor,
            window.tip,
            cfg.slots_per_query,
            MAX_SLOTS_PER_ROUND,
        );

        if ranges.is_empty() {
            if wait_or_shutdown(&mut shutdown, &repair, cfg.repair_interval).await {
                break;
            }
            continue;
        }

        let total: u64 = ranges.iter().map(|range| range.len_slots()).sum();
        debug!(
            ranges = ranges.len(),
            slots = total,
            floor = window.floor,
            tip = window.tip,
            "disk cache: filler round planned"
        );

        for range in ranges {
            // Token-bucket rate limit on fetched slots.
            loop {
                let elapsed = last_refill.elapsed().as_secs_f64();
                last_refill = Instant::now();
                tokens = (tokens + elapsed * cfg.max_slots_per_sec as f64)
                    .min(cfg.max_slots_per_sec.max(cfg.slots_per_query) as f64 * 2.0);
                if tokens >= range.len_slots() as f64 {
                    tokens -= range.len_slots() as f64;
                    break;
                }
                if sleep_or_shutdown(&mut shutdown, Duration::from_millis(100)).await {
                    return;
                }
            }

            match fill_range(&cache, &clickhouse, &bulk, &mut shutdown, &range, &cfg).await {
                Ok(outcome) => {
                    backoff = Duration::from_millis(250);
                    warn_for_given_up(record_fill_attempt_result(
                        &mut attempts,
                        &mut given_up,
                        FillAttemptResult::IncompleteSlots(&outcome.incomplete_slots),
                        cfg.max_attempts,
                    ));
                }
                Err(FillError::Shutdown) => return,
                Err(FillError::ClickHouse(err)) => {
                    crate::metrics::disk_cache_fill_error();
                    warn!(
                        start = range.start,
                        end = range.end,
                        "disk cache: backfill fetch failed ({err}); backing off"
                    );
                    warn_for_given_up(record_fill_attempt_result(
                        &mut attempts,
                        &mut given_up,
                        FillAttemptResult::TransientClickHouse,
                        cfg.max_attempts,
                    ));
                    if sleep_or_shutdown(&mut shutdown, backoff).await {
                        return;
                    }
                    backoff = (backoff * 2).min(max_backoff);
                }
            }
        }

        // Let the writer drain before re-planning, so freshly written slots
        // are visible as coverage and are not fetched twice.
        if sleep_or_shutdown(&mut shutdown, Duration::from_millis(50)).await {
            break;
        }
    }

    info!("disk cache: filler stopped");
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SlotRange {
    pub(crate) start: u64,
    pub(crate) end: u64,
}

impl SlotRange {
    fn len_slots(&self) -> u64 {
        self.end - self.start + 1
    }
}

struct ClaimableWindow {
    floor: u64,
    tip: u64,
}

/// `[tip - retain + 1, tip]` where `tip` is the newest slot ClickHouse can be
/// trusted to have fully ingested: min of the ClickHouse and head-cache
/// finalized tips, minus the configured lag.
async fn claimable_window(
    cache: &DiskCache,
    clickhouse: &ClickHouseClient,
    head: Option<&HeadCache>,
    cfg: &FillerConfig,
) -> Option<ClaimableWindow> {
    let ch_tip = match clickhouse.get_latest_finalized_slot().await {
        Ok(Some(tip)) => tip,
        Ok(None) => return None,
        Err(err) => {
            warn!("disk cache: filler could not resolve the ClickHouse tip: {err}");
            return None;
        }
    };
    let head_tip = head
        .map(|cache| {
            cache.latest_slot_at_least(solana_commitment_config::CommitmentLevel::Finalized)
        })
        .filter(|&tip| tip > 0);

    let tip = head_tip
        .map_or(ch_tip, |head_tip| ch_tip.min(head_tip))
        .saturating_sub(cfg.repair_min_lag_slots);
    if tip == 0 {
        return None;
    }
    let floor = tip
        .saturating_sub(cfg.retain_slots.saturating_sub(1))
        .max(cache.min_retained_slot());
    Some(ClaimableWindow { floor, tip })
}

/// Pure planning: repair slots first, then coverage holes, both newest-first,
/// clamped to the window, skipping given-up slots, coalesced into consecutive
/// runs and chunked to the per-query size. Total slots bounded by `max_slots`.
pub(crate) fn plan_ranges(
    repair_slots: &[u64],
    holes: &[(u64, u64)],
    given_up: &HashSet<u64>,
    floor: u64,
    tip: u64,
    slots_per_query: u64,
    max_slots: u64,
) -> Vec<SlotRange> {
    let chunk = slots_per_query.max(1);
    let mut planned: Vec<u64> = Vec::new();
    let mut seen: HashSet<u64> = HashSet::new();

    let push = |slot: u64, planned: &mut Vec<u64>, seen: &mut HashSet<u64>| {
        if slot >= floor && slot <= tip && !given_up.contains(&slot) && seen.insert(slot) {
            planned.push(slot);
        }
    };

    // Repair slots arrive newest-first from RepairQueue::drain.
    for &slot in repair_slots {
        if planned.len() as u64 >= max_slots {
            break;
        }
        push(slot, &mut planned, &mut seen);
    }

    // Holes are ascending ranges; walk them backwards, newest slots first.
    'outer: for &(start, end) in holes.iter().rev() {
        let mut slot = end.min(tip);
        loop {
            if planned.len() as u64 >= max_slots {
                break 'outer;
            }
            push(slot, &mut planned, &mut seen);
            if slot == start.max(floor) || slot == 0 {
                break;
            }
            slot -= 1;
        }
    }

    // Coalesce into consecutive ascending runs, keeping newest-first order
    // between runs, then chunk to the query size.
    planned.sort_unstable_by(|a, b| b.cmp(a));
    let mut ranges: Vec<SlotRange> = Vec::new();
    for slot in planned {
        match ranges.last_mut() {
            Some(range) if range.start == slot + 1 => range.start = slot,
            _ => ranges.push(SlotRange {
                start: slot,
                end: slot,
            }),
        }
    }

    ranges
        .into_iter()
        .flat_map(|range| {
            // Split a long run into chunks, newest chunk first.
            let mut chunks = Vec::new();
            let mut end = range.end;
            loop {
                let start = end.saturating_sub(chunk - 1).max(range.start);
                chunks.push(SlotRange { start, end });
                if start == range.start {
                    break;
                }
                end = start - 1;
            }
            chunks
        })
        .collect()
}

pub(crate) type FilledBlock = (BlockMetadataRecord, Vec<Arc<StoredTransactionRecord>>);

/// Group fetched blocks, dropping any slot whose transaction set does not
/// match its metadata (ClickHouse ingestion may still be catching up).
/// Returns ascending blocks plus the mismatched slots.
pub(crate) fn assemble_blocks(
    metas: Vec<BlockMetadataRecord>,
    txs: Vec<StoredTransactionRecord>,
) -> (Vec<FilledBlock>, Vec<u64>) {
    let mut by_slot: BTreeMap<u64, Vec<Arc<StoredTransactionRecord>>> = BTreeMap::new();
    for record in txs {
        by_slot
            .entry(record.slot)
            .or_default()
            .push(Arc::new(record));
    }

    let mut blocks = Vec::with_capacity(metas.len());
    let mut mismatched = Vec::new();
    for meta in metas {
        let slot_txs = by_slot.remove(&meta.slot).unwrap_or_default();
        if slot_txs.len() as u64 != meta.executed_transaction_count {
            mismatched.push(meta.slot);
            continue;
        }
        blocks.push((meta, slot_txs));
    }
    (blocks, mismatched)
}

enum FillError {
    ClickHouse(crate::processing::ProcessingError),
    Shutdown,
}

struct FillOutcome {
    incomplete_slots: Vec<u64>,
}

async fn fill_range(
    cache: &DiskCache,
    clickhouse: &ClickHouseClient,
    bulk: &SyncSender<DiskWriteJob>,
    shutdown: &mut tokio::sync::broadcast::Receiver<()>,
    range: &SlotRange,
    cfg: &FillerConfig,
) -> Result<FillOutcome, FillError> {
    let (metas, _) = clickhouse
        .get_block_metadata_by_slot_range(range.start, range.end, cfg.query_timeout)
        .await
        .map_err(FillError::ClickHouse)?;
    if metas.is_empty() {
        // Either an all-skipped run (proven by a later block's parent link) or
        // ClickHouse is missing the range; attempts accounting decides.
        return Ok(FillOutcome {
            incomplete_slots: uncached_slots(cache, range),
        });
    }
    let (txs, _) = clickhouse
        .get_block_full_transactions_by_slot_range(range.start, range.end, cfg.query_timeout)
        .await
        .map_err(FillError::ClickHouse)?;

    let (blocks, mismatched) = assemble_blocks(metas, txs);
    if !mismatched.is_empty() {
        debug!(
            slots = ?mismatched,
            "disk cache: ClickHouse rows incomplete for slots; left as holes for retry"
        );
    }
    if blocks.is_empty() {
        return Ok(FillOutcome {
            incomplete_slots: uncached_slots(cache, range),
        });
    }

    let last_slot = blocks.last().map(|(meta, _)| meta.slot);
    let mut job = DiskWriteJob::FillBatch {
        source: schema::COVERAGE_SOURCE_BACKFILL,
        blocks,
    };
    loop {
        match bulk.try_send(job) {
            Ok(()) => break,
            Err(TrySendError::Full(returned)) => {
                job = returned;
                if sleep_or_shutdown(shutdown, Duration::from_millis(50)).await {
                    return Err(FillError::Shutdown);
                }
            }
            Err(TrySendError::Disconnected(_)) => return Err(FillError::Shutdown),
        }
    }

    // Wait briefly for the batch to land so the next planning round sees it
    // as coverage instead of re-fetching the same range.
    if let Some(last_slot) = last_slot {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !cache.covers_slot(last_slot) && Instant::now() < deadline {
            if sleep_or_shutdown(shutdown, Duration::from_millis(20)).await {
                return Err(FillError::Shutdown);
            }
        }
    }
    Ok(FillOutcome {
        incomplete_slots: uncached_slots(cache, range),
    })
}

fn prune_tracking(attempts: &mut HashMap<u64, u32>, given_up: &mut HashSet<u64>, floor: u64) {
    attempts.retain(|&slot, _| slot >= floor);
    given_up.retain(|&slot| slot >= floor);
}

fn uncached_slots(cache: &DiskCache, range: &SlotRange) -> Vec<u64> {
    (range.start..=range.end)
        .filter(|&slot| !cache.covers_slot(slot))
        .collect()
}

enum FillAttemptResult<'a> {
    IncompleteSlots(&'a [u64]),
    TransientClickHouse,
}

fn record_fill_attempt_result(
    attempts: &mut HashMap<u64, u32>,
    given_up: &mut HashSet<u64>,
    result: FillAttemptResult<'_>,
    max_attempts: u32,
) -> Vec<(u64, u32)> {
    let FillAttemptResult::IncompleteSlots(slots) = result else {
        return Vec::new();
    };

    let mut newly_given_up = Vec::new();
    for &slot in slots {
        if given_up.contains(&slot) {
            continue;
        }
        let entry = attempts.entry(slot).or_insert(0);
        *entry += 1;
        if *entry >= max_attempts && given_up.insert(slot) {
            newly_given_up.push((slot, *entry));
        }
    }
    newly_given_up
}

fn warn_for_given_up(slots: Vec<(u64, u32)>) {
    for (slot, attempts) in slots {
        warn!(
            slot,
            attempts, "disk cache: giving up on slot; it stays a ClickHouse read"
        );
    }
}

/// Returns true when shutdown was requested.
async fn wait_or_shutdown(
    shutdown: &mut tokio::sync::broadcast::Receiver<()>,
    repair: &RepairQueue,
    interval: Duration,
) -> bool {
    tokio::select! {
        _ = shutdown.recv() => true,
        _ = repair.notified() => false,
        _ = tokio::time::sleep(interval) => false,
    }
}

/// Returns true when shutdown was requested.
async fn sleep_or_shutdown(
    shutdown: &mut tokio::sync::broadcast::Receiver<()>,
    duration: Duration,
) -> bool {
    tokio::select! {
        _ = shutdown.recv() => true,
        _ = tokio::time::sleep(duration) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk_cache::tests::{block_metadata, transaction};

    #[test]
    fn plan_ranges_orders_newest_first_and_chunks() {
        let holes = vec![(10, 14), (20, 21), (30, 30)];
        let ranges = plan_ranges(&[], &holes, &HashSet::new(), 0, 100, 3, 64);

        assert_eq!(
            ranges,
            vec![
                SlotRange { start: 30, end: 30 },
                SlotRange { start: 20, end: 21 },
                SlotRange { start: 12, end: 14 },
                SlotRange { start: 10, end: 11 },
            ]
        );
    }

    #[test]
    fn plan_ranges_repair_slots_lead_and_merge() {
        let holes = vec![(10, 12)];
        let repair = vec![50, 49, 11];
        let ranges = plan_ranges(&repair, &holes, &HashSet::new(), 0, 100, 8, 64);

        assert_eq!(
            ranges,
            vec![
                SlotRange { start: 49, end: 50 },
                SlotRange { start: 10, end: 12 },
            ]
        );
    }

    #[test]
    fn plan_ranges_respects_window_given_up_and_budget() {
        let holes = vec![(0, 100)];
        let mut given_up = HashSet::new();
        given_up.insert(98u64);

        let ranges = plan_ranges(&[], &holes, &given_up, 90, 99, 4, 6);
        // Newest-first from 99 down, skipping 98, six slots total: 99, 97..=93.
        assert_eq!(
            ranges,
            vec![
                SlotRange { start: 99, end: 99 },
                SlotRange { start: 94, end: 97 },
                SlotRange { start: 93, end: 93 },
            ]
        );

        // Everything below the floor or above the tip is ignored.
        let ranges = plan_ranges(&[200, 5], &holes, &HashSet::new(), 90, 99, 4, 64);
        let total: u64 = ranges.iter().map(|range| range.len_slots()).sum();
        assert_eq!(total, 10);
    }

    #[test]
    fn assemble_blocks_groups_and_validates_counts() {
        let meta_ok = block_metadata(100, 99, 2);
        let meta_mismatch = block_metadata(101, 100, 3);
        let meta_empty = block_metadata(102, 101, 0);

        let txs = vec![
            transaction(100, 0),
            transaction(100, 1),
            transaction(101, 0), // only 1 of 3
        ];

        let (blocks, mismatched) = assemble_blocks(vec![meta_ok, meta_mismatch, meta_empty], txs);
        assert_eq!(mismatched, vec![101]);
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].0.slot, 100);
        assert_eq!(blocks[0].1.len(), 2);
        assert_eq!(blocks[1].0.slot, 102);
        assert!(blocks[1].1.is_empty());
    }

    #[test]
    fn incomplete_fill_attempts_give_up_once_at_limit() {
        let mut attempts = HashMap::new();
        let mut given_up = HashSet::new();
        let slots = vec![10, 11];

        assert!(
            record_fill_attempt_result(
                &mut attempts,
                &mut given_up,
                FillAttemptResult::IncompleteSlots(&slots),
                2
            )
            .is_empty()
        );

        let newly_given_up = record_fill_attempt_result(
            &mut attempts,
            &mut given_up,
            FillAttemptResult::IncompleteSlots(&slots),
            2,
        );
        assert_eq!(newly_given_up, vec![(10, 2), (11, 2)]);
        assert!(given_up.contains(&10));
        assert!(given_up.contains(&11));

        assert!(
            record_fill_attempt_result(
                &mut attempts,
                &mut given_up,
                FillAttemptResult::IncompleteSlots(&slots),
                2
            )
            .is_empty()
        );
        assert_eq!(attempts.get(&10), Some(&2));
        assert_eq!(attempts.get(&11), Some(&2));
    }

    #[test]
    fn clickhouse_errors_do_not_count_toward_given_up_slots() {
        let mut attempts = HashMap::new();
        let mut given_up = HashSet::new();
        let range = SlotRange { start: 10, end: 11 };

        let newly_given_up = record_fill_attempt_result(
            &mut attempts,
            &mut given_up,
            FillAttemptResult::TransientClickHouse,
            1,
        );
        assert!(newly_given_up.is_empty());
        assert!(attempts.is_empty());
        assert!(given_up.is_empty());

        assert_eq!(
            plan_ranges(&[], &[(10, 11)], &given_up, 10, 11, 8, 64),
            vec![range]
        );
    }
}
