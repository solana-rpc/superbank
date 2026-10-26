// SPDX-License-Identifier: AGPL-3.0-only

//! Durable full-history block-time index with a segmented in-process read path.
//! A slot is covered only when a source row or a later row's parent link proves it.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clickhouse::Row;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::clickhouse::{BlockTimeRangeRow, ClickHouseClient};

use super::DiskCacheError;
use super::coverage::CoverageMap;
use super::filler::{MAX_SKIPPED_RUN, next_block_after};

const SEGMENT_SLOTS: u64 = 1_000_000;
const UNKNOWN: i64 = i64::MIN;
const SKIPPED: i64 = i64::MIN + 1;
const NULL_TIME: i64 = i64::MIN + 2;
const DATA_TABLE: &str = "block_times";
const COVERAGE_TABLE: &str = "proven_coverage";
/// Coverage table that does not prove skipped slots; `open` drops it so the index rebuilds.
const UNPROVEN_COVERAGE_TABLE: &str = "coverage";
/// A forward scan waits this long behind the finalized tip for a missing block before it
/// leaves the block's slots uncovered and moves on.
const UNPROVEN_SETTLE_SLOTS: u64 = 1_500;

fn segment_memory_bytes() -> u64 {
    SEGMENT_SLOTS * std::mem::size_of::<AtomicI64>() as u64
        + SEGMENT_SLOTS.div_ceil(64) * std::mem::size_of::<AtomicU64>() as u64
}

pub(crate) fn minimum_memory_bytes() -> u64 {
    segment_memory_bytes()
}

#[derive(Debug, Clone)]
pub(crate) struct BlockIndexConfig {
    pub(crate) database: String,
    pub(crate) max_memory_bytes: u64,
    pub(crate) slots_per_query: u64,
    pub(crate) max_slots_per_sec: u64,
    pub(crate) query_timeout: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BlockTimeLookup {
    NotCovered,
    Skipped,
    Found(Option<i64>),
}

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
struct PersistedBlockTimeRow {
    slot: u64,
    block_time: Option<i64>,
    version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
struct CoverageRow {
    range_start: u64,
    range_end: u64,
    version: u64,
}

#[derive(Debug, Clone, Deserialize, Row)]
struct HydrateCoverageRow {
    range_start: u64,
    range_end: u64,
}

struct SegmentData {
    times: Box<[AtomicI64]>,
    produced: Box<[AtomicU64]>,
}

type Segment = Arc<SegmentData>;

pub(crate) struct BlockIndex {
    cfg: BlockIndexConfig,
    local: ClickHouseClient,
    segments: RwLock<HashMap<u64, Segment>>,
    durable_coverage: RwLock<CoverageMap>,
    coverage: RwLock<CoverageMap>,
}

fn append_segment_coverage(
    proof: &mut crate::slot_coverage::SlotCoverage,
    segments: &HashMap<u64, Segment>,
    start: u64,
    end: u64,
) {
    let mut cursor = start;
    loop {
        let segment_id = cursor / SEGMENT_SLOTS;
        let tip = end.min(cursor.saturating_add(SEGMENT_SLOTS - 1 - cursor % SEGMENT_SLOTS));
        if let Some(segment) = segments.get(&segment_id) {
            let slots = segment_slots(segment, segment_id, cursor, tip);
            proof.merge(crate::slot_coverage::SlotCoverage::new(slots, cursor, tip));
        }
        if tip == end {
            break;
        }
        cursor = tip + 1;
    }
}

fn segment_slots(segment: &Segment, id: u64, start: u64, end: u64) -> Vec<u64> {
    let mut slots = Vec::new();
    let first = start % SEGMENT_SLOTS;
    let last = end % SEGMENT_SLOTS;
    for index in first / 64..=last / 64 {
        let mut word = segment.produced[index as usize].load(Ordering::Relaxed);
        if index == first / 64 {
            word &= u64::MAX << (first % 64);
        }
        if index == last / 64 && last % 64 != 63 {
            word &= (1u64 << (last % 64 + 1)) - 1;
        }
        while word != 0 {
            slots.push(id * SEGMENT_SLOTS + index * 64 + u64::from(word.trailing_zeros()));
            word &= word - 1;
        }
    }
    slots
}

impl BlockIndex {
    #[cfg(test)]
    pub(crate) fn for_range_tests(start: u64, end: u64, slots: &[u64]) -> Self {
        let index = tests::test_index();
        let rows = slots
            .iter()
            .map(|&slot| BlockTimeRangeRow {
                slot,
                parent_slot: slot.saturating_sub(1),
                block_time: None,
            })
            .collect::<Vec<_>>();
        assert!(index.publish_memory(start, end, &rows));
        index
    }

    pub(crate) async fn open(
        cfg: BlockIndexConfig,
        admin: &ClickHouseClient,
    ) -> Result<Self, DiskCacheError> {
        if cfg.max_memory_bytes < segment_memory_bytes() {
            return Err(DiskCacheError::Config(format!(
                "DISK_CACHE_BLOCK_INDEX_MAX_MEMORY_BYTES must be at least {} bytes",
                segment_memory_bytes()
            )));
        }
        let database = quote_identifier(&cfg.database)?;
        // Bootstrap DDL stays on the writable admin connection.
        admin
            .client
            .query(&format!(
                "CREATE DATABASE IF NOT EXISTS {database} ENGINE = Atomic"
            ))
            .execute()
            .await
            .map_err(|err| DiskCacheError::ClickHouse(err.to_string()))?;
        admin
            .client
            .query(&format!(
                "CREATE TABLE IF NOT EXISTS {database}.{DATA_TABLE} (\
                 slot UInt64, block_time Nullable(Int64), version UInt64) \
                 ENGINE = ReplacingMergeTree(version) \
                 PARTITION BY intDiv(slot, {SEGMENT_SLOTS}) ORDER BY slot"
            ))
            .execute()
            .await
            .map_err(|err| DiskCacheError::ClickHouse(err.to_string()))?;
        admin
            .client
            .query(&format!(
                "CREATE TABLE IF NOT EXISTS {database}.{COVERAGE_TABLE} (\
                 range_start UInt64, range_end UInt64, version UInt64) \
                 ENGINE = ReplacingMergeTree(version) ORDER BY range_start"
            ))
            .execute()
            .await
            .map_err(|err| DiskCacheError::ClickHouse(err.to_string()))?;
        admin
            .client
            .query(&format!(
                "DROP TABLE IF EXISTS {database}.{UNPROVEN_COVERAGE_TABLE}"
            ))
            .execute()
            .await
            .map_err(|err| DiskCacheError::ClickHouse(err.to_string()))?;

        let mut local = admin.background_read_client(1);
        local.database = cfg.database.clone();
        local.query_timeout = cfg.query_timeout;
        local.client = local.client.clone().with_database(cfg.database.clone());
        Ok(Self {
            cfg,
            local,
            segments: RwLock::new(HashMap::new()),
            durable_coverage: RwLock::new(CoverageMap::new()),
            coverage: RwLock::new(CoverageMap::new()),
        })
    }

    pub(crate) fn tip_span(&self) -> Option<(u64, u64)> {
        self.coverage
            .read()
            .expect("block-index coverage lock")
            .contiguous_tip_span()
    }

    fn durable_covered_span(&self) -> Option<(u64, u64)> {
        self.durable_coverage
            .read()
            .expect("block-index durable coverage lock")
            .covered_span()
    }

    pub(crate) fn block_time(&self, slot: u64) -> BlockTimeLookup {
        let started = Instant::now();
        let result = if !self
            .coverage
            .read()
            .expect("block-index coverage lock")
            .contains(slot)
        {
            BlockTimeLookup::NotCovered
        } else {
            // Eviction drops a segment before its coverage, so a missing segment is not covered.
            match self.value(slot) {
                None => BlockTimeLookup::NotCovered,
                Some(NULL_TIME) => BlockTimeLookup::Found(None),
                Some(UNKNOWN | SKIPPED) => BlockTimeLookup::Skipped,
                Some(value) => BlockTimeLookup::Found(Some(value)),
            }
        };
        crate::metrics::block_index_lookup("get_block_time", started.elapsed().as_secs_f64());
        result
    }

    #[cfg(test)]
    pub(crate) fn slots_in_range(&self, start: u64, end: u64) -> Option<Vec<u64>> {
        let proof = self.range_coverage(start, end);
        proof.gaps(start, end).is_empty().then_some(proof.slots)
    }

    pub(crate) fn range_coverage(
        &self,
        start: u64,
        end: u64,
    ) -> crate::slot_coverage::SlotCoverage {
        let started = Instant::now();
        // Lock order matches reserve_segments. Capture coverage and segment identity
        // together so an evicted segment cannot be replaced beneath a proof.
        let segments = self.segments.read().expect("block-index segments lock");
        let coverage = self.coverage.read().expect("block-index coverage lock");
        let mut proof = crate::slot_coverage::SlotCoverage::default();
        for (floor, tip) in coverage.intersections(start, end) {
            append_segment_coverage(&mut proof, &segments, floor, tip);
        }
        crate::metrics::block_index_lookup("get_blocks", started.elapsed().as_secs_f64());
        proof
    }

    fn value(&self, slot: u64) -> Option<i64> {
        let segment_id = slot / SEGMENT_SLOTS;
        let offset = (slot % SEGMENT_SLOTS) as usize;
        let segment = self
            .segments
            .read()
            .expect("block-index segments lock")
            .get(&segment_id)
            .cloned();
        segment.map(|segment| segment.times[offset].load(Ordering::Relaxed))
    }

    fn new_segment() -> Segment {
        Arc::new(SegmentData {
            times: (0..SEGMENT_SLOTS)
                .map(|_| AtomicI64::new(UNKNOWN))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            produced: (0..SEGMENT_SLOTS.div_ceil(64))
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        })
    }

    fn reserve_segments(&self, start: u64, end: u64) -> HashMap<u64, Segment> {
        let first = start / SEGMENT_SLOTS;
        let last = end / SEGMENT_SLOTS;
        let mut segments = self.segments.write().expect("block-index segments lock");
        let max_segments = usize::try_from(self.cfg.max_memory_bytes / segment_memory_bytes())
            .unwrap_or(usize::MAX);
        let mut evicted_through = None;

        for segment_id in (first..=last).rev() {
            if segments.contains_key(&segment_id) {
                continue;
            }
            if segments.len() >= max_segments {
                let Some(oldest) = segments.keys().copied().min() else {
                    break;
                };
                if segment_id <= oldest {
                    continue;
                }
                segments.remove(&oldest);
                evicted_through =
                    Some(evicted_through.map_or(oldest, |value: u64| value.max(oldest)));
            }
            segments.insert(segment_id, Self::new_segment());
        }

        let reserved = (first..=last)
            .filter_map(|segment_id| {
                segments
                    .get(&segment_id)
                    .cloned()
                    .map(|segment| (segment_id, segment))
            })
            .collect();
        drop(segments);

        if let Some(evicted_through) = evicted_through {
            self.coverage
                .write()
                .expect("block-index coverage lock")
                .remove_below(
                    evicted_through
                        .saturating_add(1)
                        .saturating_mul(SEGMENT_SLOTS),
                );
        }
        reserved
    }

    fn publish_memory(&self, start: u64, end: u64, rows: &[BlockTimeRangeRow]) -> bool {
        if end < start {
            return true;
        }
        let segments = self.reserve_segments(start, end);
        let mut published_all = true;
        let mut cursor = start;
        let mut row_index = 0;
        while cursor <= end {
            let segment_id = cursor / SEGMENT_SLOTS;
            let segment_end = end.min((segment_id + 1) * SEGMENT_SLOTS - 1);
            while row_index < rows.len() && rows[row_index].slot < cursor {
                row_index += 1;
            }
            if let Some(segment) = segments.get(&segment_id) {
                while row_index < rows.len() && rows[row_index].slot <= segment_end {
                    let row = &rows[row_index];
                    let offset = row.slot % SEGMENT_SLOTS;
                    segment.times[offset as usize]
                        .store(row.block_time.unwrap_or(NULL_TIME), Ordering::Relaxed);
                    segment.produced[(offset / 64) as usize]
                        .fetch_or(1u64 << (offset % 64), Ordering::Relaxed);
                    row_index += 1;
                }
                self.coverage
                    .write()
                    .expect("block-index coverage lock")
                    .insert_range(cursor, segment_end);
            } else {
                published_all = false;
                while row_index < rows.len() && rows[row_index].slot <= segment_end {
                    row_index += 1;
                }
            }
            cursor = segment_end.saturating_add(1);
        }
        self.publish_metrics();
        published_all
    }

    fn publish_metrics(&self) {
        let (floor, head) = self.tip_span().unwrap_or((0, 0));
        let bytes = self
            .segments
            .read()
            .expect("block-index segments lock")
            .len() as u64
            * segment_memory_bytes();
        crate::metrics::block_index_state(floor, head, bytes);
    }

    async fn load_local(&self) -> Result<(), DiskCacheError> {
        let database = quote_identifier(&self.cfg.database)?;
        let ranges = self
            .local
            .read_all::<HydrateCoverageRow>(
                &format!(
                    "SELECT range_start, argMax(range_end, version) AS range_end \
                 FROM {database}.{COVERAGE_TABLE} GROUP BY range_start ORDER BY range_start DESC"
                ),
                "block_index_hydrate_coverage",
            )
            .await
            .map_err(|err| DiskCacheError::ClickHouse(err.to_string()))?;
        for range in ranges {
            self.durable_coverage
                .write()
                .expect("block-index durable coverage lock")
                .insert_range(range.range_start, range.range_end);
            // Stored rows keep no parent link; hydration only republishes proven coverage.
            let rows = self
                .local
                .read_all::<BlockTimeRangeRow>(&format!(
                    "SELECT slot, toUInt64(0) AS parent_slot, \
                     tupleElement(argMax(tuple(block_time), version), 1) AS block_time \
                     FROM {database}.{DATA_TABLE} WHERE slot BETWEEN {} AND {} GROUP BY slot ORDER BY slot",
                    range.range_start, range.range_end
                ), "block_index_hydrate_rows").await
                .map_err(|err| DiskCacheError::ClickHouse(err.to_string()))?;
            let _ = self.publish_memory(range.range_start, range.range_end, &rows);
        }
        Ok(())
    }

    async fn persist_ranges(
        &self,
        ranges: &[(u64, u64)],
        rows: &[BlockTimeRangeRow],
    ) -> Result<(), DiskCacheError> {
        if ranges.is_empty() {
            return Ok(());
        }
        let version = now_version();
        let mut insert = self
            .local
            .client
            .insert::<PersistedBlockTimeRow>(DATA_TABLE)
            .await
            .map_err(|err| DiskCacheError::ClickHouse(err.to_string()))?;
        for row in rows.iter().filter(|row| {
            ranges
                .iter()
                .any(|&(start, end)| (start..=end).contains(&row.slot))
        }) {
            insert
                .write(&PersistedBlockTimeRow {
                    slot: row.slot,
                    block_time: row.block_time,
                    version,
                })
                .await
                .map_err(|err| DiskCacheError::ClickHouse(err.to_string()))?;
        }
        insert
            .end()
            .await
            .map_err(|err| DiskCacheError::ClickHouse(err.to_string()))?;
        let mut coverage = self
            .local
            .client
            .insert::<CoverageRow>(COVERAGE_TABLE)
            .await
            .map_err(|err| DiskCacheError::ClickHouse(err.to_string()))?;
        for &(start, end) in ranges {
            coverage
                .write(&CoverageRow {
                    range_start: start,
                    range_end: end,
                    version,
                })
                .await
                .map_err(|err| DiskCacheError::ClickHouse(err.to_string()))?;
        }
        coverage
            .end()
            .await
            .map_err(|err| DiskCacheError::ClickHouse(err.to_string()))?;
        for &(start, end) in ranges {
            self.durable_coverage
                .write()
                .expect("block-index durable coverage lock")
                .insert_range(start, end);
            if !self.publish_memory(start, end, rows) {
                tracing::debug!(
                    start,
                    end,
                    max_memory_bytes = self.cfg.max_memory_bytes,
                    "block index: durable range is outside the bounded in-process cache"
                );
            }
        }
        Ok(())
    }

    /// Reads one window and returns its rows with the ranges that rows and parent links prove.
    async fn scan(
        &self,
        source: &ClickHouseClient,
        start: u64,
        end: u64,
    ) -> Result<(Vec<BlockTimeRangeRow>, Vec<(u64, u64)>), DiskCacheError> {
        let (rows, _) = source
            .get_block_times_by_slot_range(start, end, self.cfg.query_timeout)
            .await
            .map_err(|err| DiskCacheError::ClickHouse(err.to_string()))?;
        // The next block's parent link proves the skipped slots after the last row.
        let successor = if rows.last().is_none_or(|row| row.slot < end) {
            next_block_after(source, end, self.cfg.query_timeout).await?
        } else {
            None
        };
        let links = rows
            .iter()
            .map(|row| (row.slot, row.parent_slot))
            .chain(successor.map(|row| (row.slot, row.parent_slot)));
        let proven = proven_ranges(start, end, links);
        Ok((rows, proven))
    }

    /// Scans and persists one window. Returns the window, or `None` when nothing is left to scan.
    async fn scan_once(
        &self,
        source: &ClickHouseClient,
        latest: u64,
        cursors: &mut ScanCursors,
    ) -> Option<(u64, u64)> {
        let ScanWindow {
            start,
            end,
            forward,
        } = cursors.window(latest, self.cfg.slots_per_query)?;
        let (rows, proven) = match self.scan(source, start, end).await {
            Ok(scanned) => scanned,
            Err(err) => {
                crate::metrics::block_index_error("source_read");
                warn!(start, end, "block index: source read failed: {err}");
                return Some((start, end));
            }
        };
        // Backfill never revisits a window, so it publishes every proven range at once.
        let settled = if forward {
            latest.saturating_sub(UNPROVEN_SETTLE_SLOTS)
        } else {
            u64::MAX
        };
        let plan = plan_scan(start, end, &proven, settled);
        if let Err(err) = self.persist_ranges(&plan.publish, &rows).await {
            crate::metrics::block_index_error("persist");
            warn!(start, end, "block index: local persist failed: {err}");
            return Some((start, end));
        }
        if forward {
            cursors.forward_next = Some(plan.next);
            cursors.backfill_floor.get_or_insert(start);
        } else {
            cursors.backfill_floor = Some(start);
        }
        Some((start, end))
    }

    pub(crate) async fn run(
        self: Arc<Self>,
        source: ClickHouseClient,
        mut shutdown: tokio::sync::broadcast::Receiver<()>,
    ) {
        // One historical scan runs at a time, independently of interactive reads.
        let source = source.background_read_client(1);
        crate::metrics::block_index_enabled(true);
        if let Err(err) = self.load_local().await {
            crate::metrics::block_index_error("hydrate");
            warn!("block index: local hydration failed: {err}");
        }
        info!(
            database = self.cfg.database,
            "block index: historical worker started"
        );
        let mut cursors = ScanCursors::seeded(self.durable_covered_span());
        let mut last_latest = None;
        loop {
            let latest = match source.get_latest_finalized_slot_since(last_latest).await {
                Ok(Some(slot)) => {
                    last_latest = Some(slot);
                    slot
                }
                Ok(None) => {
                    if wait_or_shutdown(&mut shutdown, Duration::from_secs(5)).await {
                        break;
                    }
                    continue;
                }
                Err(err) => {
                    crate::metrics::block_index_error("source_tip");
                    warn!("block index: source tip query failed: {err}");
                    if wait_or_shutdown(&mut shutdown, Duration::from_secs(5)).await {
                        break;
                    }
                    continue;
                }
            };
            let started = Instant::now();
            let Some((start, end)) = self.scan_once(&source, latest, &mut cursors).await else {
                if wait_or_shutdown(&mut shutdown, Duration::from_secs(5)).await {
                    break;
                }
                continue;
            };
            let target = Duration::from_secs_f64(
                (end - start + 1) as f64 / self.cfg.max_slots_per_sec.max(1) as f64,
            );
            if let Some(delay) = target.checked_sub(started.elapsed())
                && wait_or_shutdown(&mut shutdown, delay).await
            {
                break;
            }
        }
        crate::metrics::block_index_enabled(false);
    }
}

/// Where the worker's next scans start, seeded from durable coverage.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct ScanCursors {
    /// First slot the next forward scan reads; `None` until the first scan at the tip.
    forward_next: Option<u64>,
    /// Lowest slot scanned so far; backfill reads below it.
    backfill_floor: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ScanWindow {
    start: u64,
    end: u64,
    forward: bool,
}

impl ScanCursors {
    fn seeded(durable_span: Option<(u64, u64)>) -> Self {
        Self {
            forward_next: durable_span.map(|(_, head)| head.saturating_add(1)),
            backfill_floor: durable_span.map(|(floor, _)| floor),
        }
    }

    /// New tip slots come first, then history below the backfill floor.
    fn window(&self, latest: u64, slots_per_query: u64) -> Option<ScanWindow> {
        let width = slots_per_query.max(1);
        match self.forward_next {
            Some(next) if next <= latest => Some(ScanWindow {
                start: next,
                end: latest.min(next.saturating_add(width - 1)),
                forward: true,
            }),
            None => Some(ScanWindow {
                start: latest.saturating_sub(width - 1),
                end: latest,
                forward: true,
            }),
            Some(_) => match self.backfill_floor {
                Some(floor) if floor > 0 => Some(ScanWindow {
                    start: floor.saturating_sub(width),
                    end: floor - 1,
                    forward: false,
                }),
                _ => None,
            },
        }
    }
}

/// Merged ranges of `[start, end]` that rows prove produced or skipped. `links` are
/// `(slot, parent_slot)` in ascending slot order; the slots between a parent and its child are skipped.
fn proven_ranges(
    start: u64,
    end: u64,
    links: impl IntoIterator<Item = (u64, u64)>,
) -> Vec<(u64, u64)> {
    let mut ranges: Vec<(u64, u64)> = Vec::new();
    let mut previous_slot = None;
    for (slot, parent_slot) in links {
        // A link proves its gap only when no earlier row lies inside it.
        let linked = parent_slot < slot
            && slot - parent_slot - 1 <= MAX_SKIPPED_RUN
            && previous_slot.is_none_or(|previous| parent_slot >= previous);
        previous_slot = Some(slot);
        let first = if linked { parent_slot + 1 } else { slot }.max(start);
        let last = slot.min(end);
        if first > last {
            continue;
        }
        match ranges.last_mut() {
            Some((_, tip)) if tip.saturating_add(1) >= first => *tip = (*tip).max(last),
            _ => ranges.push((first, last)),
        }
    }
    ranges
}

#[derive(Debug, PartialEq, Eq)]
struct ScanPlan {
    publish: Vec<(u64, u64)>,
    next: u64,
}

/// Publishes proven ranges in order and stops at the first unproven run that ends after
/// `settled`. A settled unproven run stays uncovered, so reads fall through to the source.
fn plan_scan(start: u64, end: u64, proven: &[(u64, u64)], settled: u64) -> ScanPlan {
    let mut publish = Vec::new();
    let mut next = start;
    for &(first, last) in proven {
        if first > next && first - 1 > settled {
            return ScanPlan { publish, next };
        }
        publish.push((first, last));
        next = last.saturating_add(1);
    }
    if next <= end && end <= settled {
        next = end.saturating_add(1);
    }
    ScanPlan { publish, next }
}

fn quote_identifier(value: &str) -> Result<String, DiskCacheError> {
    if value.is_empty()
        || !value
            .chars()
            .all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
    {
        return Err(DiskCacheError::Config(format!(
            "invalid block-index database {value:?}"
        )));
    }
    Ok(format!("`{value}`"))
}

fn now_version() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u128::from(u64::MAX)) as u64
}

async fn wait_or_shutdown(
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
    use crate::clickhouse::{
        ClickHouseClientOptions, RoutingPolicy, RoutingScope, RoutingTransport,
    };

    pub(super) fn test_index() -> BlockIndex {
        let options = ClickHouseClientOptions::new(
            RoutingPolicy {
                transport: RoutingTransport::Http,
                scope: RoutingScope::Distributed,
            },
            None,
            Vec::new(),
            "default.gsfa_hot".to_string(),
            "default.gsfa_hot_local".to_string(),
        );
        let local = ClickHouseClient::new("http://127.0.0.1:1", "test", "", "", options);
        BlockIndex {
            cfg: BlockIndexConfig {
                database: "test_block_index".to_string(),
                max_memory_bytes: segment_memory_bytes(),
                slots_per_query: 50_000,
                max_slots_per_sec: 25_000,
                query_timeout: Duration::from_secs(300),
            },
            local,
            segments: RwLock::new(HashMap::new()),
            durable_coverage: RwLock::new(CoverageMap::new()),
            coverage: RwLock::new(CoverageMap::new()),
        }
    }

    #[test]
    fn eviction_between_segment_and_coverage_removal_cannot_claim_empty_success() {
        let index = BlockIndex::for_range_tests(10, 12, &[10, 12]);
        let proof = index.range_coverage(10, 12);
        // Model reserve_segments' window before it removes old coverage.
        index.segments.write().unwrap().remove(&0);
        assert_eq!(index.range_coverage(10, 12).gaps(10, 12), vec![(10, 12)]);
        assert_eq!(proof.slots, vec![10, 12]);
        assert!(proof.gaps(10, 12).is_empty());
    }

    #[test]
    fn sentinels_do_not_overlap_valid_block_times() {
        assert_ne!(UNKNOWN, SKIPPED);
        assert_ne!(SKIPPED, NULL_TIME);
    }

    #[test]
    fn derived_database_identifier_is_strict() {
        assert_eq!(
            quote_identifier("superbank_disk_cache_v2_block_index").unwrap(),
            "`superbank_disk_cache_v2_block_index`"
        );
        assert!(quote_identifier("default; DROP DATABASE default").is_err());
    }

    #[test]
    fn narrow_rows_preserve_times_produced_slots_and_skips() {
        let index = test_index();
        assert!(index.publish_memory(
            42,
            44,
            &[
                BlockTimeRangeRow {
                    slot: 42,
                    parent_slot: 41,
                    block_time: Some(1_700_000_000),
                },
                BlockTimeRangeRow {
                    slot: 44,
                    parent_slot: 42,
                    block_time: None,
                },
            ],
        ));

        assert_eq!(
            index.block_time(42),
            BlockTimeLookup::Found(Some(1_700_000_000))
        );
        assert_eq!(index.block_time(43), BlockTimeLookup::Skipped);
        assert_eq!(index.block_time(44), BlockTimeLookup::Found(None));
        assert_eq!(index.block_time(45), BlockTimeLookup::NotCovered);
        assert_eq!(index.slots_in_range(42, 44), Some(vec![42, 44]));
    }

    #[test]
    fn in_process_cache_keeps_the_newest_segments_within_budget() {
        let index = test_index();
        assert!(index.publish_memory(0, 0, &[]));
        assert!(!index.publish_memory(
            SEGMENT_SLOTS - 1,
            SEGMENT_SLOTS,
            &[
                BlockTimeRangeRow {
                    slot: SEGMENT_SLOTS - 1,
                    parent_slot: SEGMENT_SLOTS - 2,
                    block_time: Some(1),
                },
                BlockTimeRangeRow {
                    slot: SEGMENT_SLOTS,
                    parent_slot: SEGMENT_SLOTS - 1,
                    block_time: Some(2),
                },
            ],
        ));

        assert_eq!(
            index
                .segments
                .read()
                .expect("block-index segments lock")
                .len(),
            1
        );
        assert_eq!(index.tip_span(), Some((SEGMENT_SLOTS, SEGMENT_SLOTS)));
        assert_eq!(index.block_time(0), BlockTimeLookup::NotCovered);
        assert_eq!(
            index.block_time(SEGMENT_SLOTS),
            BlockTimeLookup::Found(Some(2))
        );

        assert!(!index.publish_memory(0, 0, &[]));
        assert_eq!(index.tip_span(), Some((SEGMENT_SLOTS, SEGMENT_SLOTS)));
    }

    fn row(slot: u64, parent_slot: u64) -> BlockTimeRangeRow {
        BlockTimeRangeRow {
            slot,
            parent_slot,
            block_time: Some(1_700_000_000 + slot as i64),
        }
    }

    #[test]
    fn proven_ranges_cover_rows_and_the_gaps_their_parents_skip() {
        assert_eq!(
            proven_ranges(10, 14, [(10, 9), (13, 10), (14, 13)]),
            vec![(10, 14)]
        );
    }

    #[test]
    fn proven_ranges_leave_a_missing_block_and_its_gap_uncovered() {
        // 13 is a real block that the read did not return; 14 links to it.
        assert_eq!(
            proven_ranges(10, 16, [(10, 9), (11, 10), (14, 13), (15, 14)]),
            vec![(10, 11), (14, 15)]
        );
        assert_eq!(proven_ranges(10, 16, [(14, 13)]), vec![(14, 14)]);
        assert_eq!(proven_ranges(10, 16, []), Vec::<(u64, u64)>::new());
    }

    #[test]
    fn proven_ranges_take_the_skipped_tail_from_the_successor() {
        assert_eq!(proven_ranges(10, 15, [(10, 9), (20, 10)]), vec![(10, 15)]);
        assert_eq!(proven_ranges(10, 15, [(20, 4)]), vec![(10, 15)]);
        assert_eq!(
            proven_ranges(10, 15, [(10, 9), (20, 12)]),
            vec![(10, 10), (13, 15)]
        );
    }

    #[test]
    fn proven_ranges_reject_contradicting_and_oversized_links() {
        // 12 claims parent 8, but 10 sits inside that gap.
        assert_eq!(
            proven_ranges(10, 12, [(10, 9), (12, 8)]),
            vec![(10, 10), (12, 12)]
        );
        let far = 10 + MAX_SKIPPED_RUN + 2;
        assert_eq!(proven_ranges(10, far, [(far, 9)]), vec![(far, far)]);
        assert_eq!(proven_ranges(0, 3, [(0, 0), (2, 0)]), vec![(0, 2)]);
    }

    #[test]
    fn plan_scan_holds_at_a_young_hole_and_accepts_a_settled_one() {
        let proven = [(10, 11), (14, 20)];
        assert_eq!(
            plan_scan(10, 20, &proven, 12),
            ScanPlan {
                publish: vec![(10, 11)],
                next: 12,
            }
        );
        assert_eq!(
            plan_scan(10, 20, &proven, 13),
            ScanPlan {
                publish: vec![(10, 11), (14, 20)],
                next: 21,
            }
        );
        assert_eq!(
            plan_scan(10, 20, &[(14, 20)], 12),
            ScanPlan {
                publish: Vec::new(),
                next: 10,
            }
        );
    }

    #[test]
    fn plan_scan_moves_past_an_unproven_tail_only_once_it_settles() {
        assert_eq!(
            plan_scan(10, 20, &[(10, 15)], 19),
            ScanPlan {
                publish: vec![(10, 15)],
                next: 16,
            }
        );
        assert_eq!(
            plan_scan(10, 20, &[(10, 15)], 20),
            ScanPlan {
                publish: vec![(10, 15)],
                next: 21,
            }
        );
        assert_eq!(
            plan_scan(10, 20, &[], 20),
            ScanPlan {
                publish: Vec::new(),
                next: 21,
            }
        );
        assert_eq!(
            plan_scan(10, 20, &[], 19),
            ScanPlan {
                publish: Vec::new(),
                next: 10,
            }
        );
    }

    #[test]
    fn scan_cursors_read_the_tip_first_and_then_history() {
        let window = |start, end, forward| {
            Some(ScanWindow {
                start,
                end,
                forward,
            })
        };
        assert_eq!(
            ScanCursors::default().window(1_000, 100),
            window(901, 1_000, true)
        );
        let cursors = ScanCursors::seeded(Some((500, 900)));
        assert_eq!(cursors.window(1_000, 50), window(901, 950, true));
        assert_eq!(cursors.window(900, 50), window(450, 499, false));
        assert_eq!(ScanCursors::seeded(Some((0, 900))).window(900, 50), None);
    }

    #[test]
    fn evicted_segment_is_not_covered_until_its_coverage_is_removed() {
        let index = BlockIndex::for_range_tests(10, 12, &[10, 12]);
        assert_eq!(index.block_time(11), BlockTimeLookup::Skipped);
        // Model reserve_segments' window before it removes old coverage.
        index.segments.write().unwrap().remove(&0);
        assert_eq!(index.block_time(11), BlockTimeLookup::NotCovered);
        assert_eq!(index.block_time(12), BlockTimeLookup::NotCovered);
    }

    #[test]
    fn missing_block_stays_out_of_block_time_and_block_ranges() {
        let index = test_index();
        let rows = [row(10, 9), row(11, 10), row(14, 13), row(15, 14)];
        let links = rows.iter().map(|row| (row.slot, row.parent_slot));
        let plan = plan_scan(10, 15, &proven_ranges(10, 15, links), u64::MAX);
        for &(start, end) in &plan.publish {
            assert!(index.publish_memory(start, end, &rows));
        }

        assert_eq!(
            index.block_time(11),
            BlockTimeLookup::Found(Some(1_700_000_011))
        );
        assert_eq!(index.block_time(12), BlockTimeLookup::NotCovered);
        assert_eq!(index.block_time(13), BlockTimeLookup::NotCovered);
        assert_eq!(
            index.block_time(14),
            BlockTimeLookup::Found(Some(1_700_000_014))
        );
        let coverage = index.range_coverage(10, 15);
        assert_eq!(coverage.gaps(10, 15), vec![(12, 13)]);
        assert_eq!(coverage.slots, vec![10, 11, 14, 15]);
    }

    async fn execute(client: &clickhouse::Client, sql: &str) {
        client
            .query(sql)
            .execute()
            .await
            .unwrap_or_else(|err| panic!("{err}: {sql}"));
    }

    async fn insert_blocks(client: &clickhouse::Client, database: &str, blocks: &[(u64, u64)]) {
        let values = blocks
            .iter()
            .map(|(slot, parent)| format!("({slot}, {parent}, {})", 1_700_000_000 + slot))
            .collect::<Vec<_>>()
            .join(", ");
        execute(
            client,
            &format!("INSERT INTO {database}.blocks_metadata (slot, parent_slot, block_time) VALUES {values}"),
        )
        .await;
    }

    /// Produced blocks in `first..=last` without `skipped`, each linked to the one before.
    fn chain(first: u64, last: u64, first_parent: u64, skipped: &[u64]) -> Vec<(u64, u64)> {
        let mut parent = first_parent;
        let mut blocks = Vec::new();
        for slot in (first..=last).filter(|slot| !skipped.contains(slot)) {
            blocks.push((slot, parent));
            parent = slot;
        }
        blocks
    }

    #[tokio::test]
    #[ignore = "requires disposable ClickHouse: DISK_CACHE_TEST_URL=http://127.0.0.1:18123"]
    async fn scan_once_covers_only_proven_slots_clickhouse_integration() {
        let url = std::env::var("DISK_CACHE_TEST_URL").expect("explicit disposable ClickHouse URL");
        assert!(url.starts_with("http://127.0.0.1:"));
        let database = format!("test_block_index_source_{}", now_version());
        let index_database = format!("{database}_block_index");
        let client = clickhouse::Client::default()
            .with_url(&url)
            .with_user("default");
        execute(&client, &format!("CREATE DATABASE {database}")).await;
        execute(
            &client,
            &format!(
                "CREATE TABLE {database}.blocks_metadata (slot UInt64, parent_slot UInt64, \
                 block_time Nullable(Int64)) ENGINE = ReplacingMergeTree(slot) \
                 PARTITION BY intDiv(slot, 432000) ORDER BY slot"
            ),
        )
        .await;
        execute(&client, &format!("CREATE DATABASE {index_database}")).await;
        execute(
            &client,
            &format!(
                "CREATE TABLE {index_database}.{UNPROVEN_COVERAGE_TABLE} (x UInt8) ENGINE = Memory"
            ),
        )
        .await;

        let mut source = ClickHouseClient::new(
            &url,
            &database,
            "default",
            "",
            ClickHouseClientOptions::new(
                RoutingPolicy {
                    transport: RoutingTransport::Http,
                    scope: RoutingScope::Distributed,
                },
                None,
                Vec::new(),
                format!("{database}.gsfa_hot"),
                format!("{database}.gsfa_hot"),
            ),
        );
        source.use_table_names(crate::clickhouse::ClickHouseTableNames::in_database(
            &database,
        ));
        source.initialize_read_cancellation().await.unwrap();
        let index = BlockIndex::open(
            BlockIndexConfig {
                database: index_database.clone(),
                max_memory_bytes: segment_memory_bytes(),
                slots_per_query: 150,
                max_slots_per_sec: 25_000,
                query_timeout: Duration::from_secs(30),
            },
            &source,
        )
        .await
        .unwrap();
        let legacy = client
            .query(&format!(
                "SELECT count() FROM system.tables WHERE database = '{index_database}' \
                 AND name = '{UNPROVEN_COVERAGE_TABLE}'"
            ))
            .fetch_one::<u64>()
            .await
            .unwrap();
        assert_eq!(legacy, 0);

        // 10_100 and 10_101 are skipped; block 10_150 lands only after the first scan.
        let blocks = chain(9_990, 10_200, 9_980, &[10_100, 10_101]);
        let late = blocks
            .iter()
            .copied()
            .filter(|&(slot, _)| slot == 10_150)
            .collect::<Vec<_>>();
        let early = blocks
            .iter()
            .copied()
            .filter(|&(slot, _)| slot != 10_150)
            .collect::<Vec<_>>();
        insert_blocks(&client, &database, &early).await;

        let mut cursors = ScanCursors::default();
        assert_eq!(
            index.scan_once(&source, 10_200, &mut cursors).await,
            Some((10_051, 10_200))
        );
        assert_eq!(index.block_time(10_100), BlockTimeLookup::Skipped);
        assert_eq!(
            index.block_time(10_120),
            BlockTimeLookup::Found(Some(1_700_010_120))
        );
        assert_eq!(index.block_time(10_150), BlockTimeLookup::NotCovered);
        assert_eq!(index.block_time(10_160), BlockTimeLookup::NotCovered);
        assert_eq!(
            cursors,
            ScanCursors {
                forward_next: Some(10_150),
                backfill_floor: Some(10_051),
            }
        );

        insert_blocks(&client, &database, &late).await;
        assert_eq!(
            index.scan_once(&source, 10_200, &mut cursors).await,
            Some((10_150, 10_200))
        );
        assert_eq!(
            index.block_time(10_150),
            BlockTimeLookup::Found(Some(1_700_010_150))
        );
        assert_eq!(
            index.block_time(10_160),
            BlockTimeLookup::Found(Some(1_700_010_160))
        );

        // 10_300 never lands; 10_345..=10_355 are skipped, so the successor proves the window's tail.
        let skipped = (10_345..=10_355).chain([10_300]).collect::<Vec<_>>();
        let mut later = chain(10_201, 12_000, 10_200, &skipped);
        for block in &mut later {
            if block.0 == 10_301 {
                block.1 = 10_300;
            }
        }
        insert_blocks(&client, &database, &later).await;
        assert_eq!(
            index.scan_once(&source, 12_000, &mut cursors).await,
            Some((10_201, 10_350))
        );
        assert_eq!(
            index.block_time(10_299),
            BlockTimeLookup::Found(Some(1_700_010_299))
        );
        assert_eq!(index.block_time(10_300), BlockTimeLookup::NotCovered);
        assert_eq!(index.block_time(10_348), BlockTimeLookup::Skipped);
        assert_eq!(cursors.forward_next, Some(10_351));

        // Backfill below the first window: 9_990's parent proves 9_981..=9_989.
        assert_eq!(
            index.scan_once(&source, 10_350, &mut cursors).await,
            Some((9_901, 10_050))
        );
        assert_eq!(index.block_time(9_985), BlockTimeLookup::Skipped);
        assert_eq!(index.block_time(9_980), BlockTimeLookup::NotCovered);
        assert_eq!(cursors.backfill_floor, Some(9_901));

        // A restart rebuilds the same coverage from the local tables.
        let reopened = BlockIndex::open(index.cfg.clone(), &source).await.unwrap();
        reopened.load_local().await.unwrap();
        for slot in [9_985, 10_100, 10_150, 10_299, 10_300, 10_348] {
            assert_eq!(
                reopened.block_time(slot),
                index.block_time(slot),
                "slot {slot}"
            );
        }

        if std::env::var_os("DISK_CACHE_TEST_KEEP").is_none() {
            execute(&client, &format!("DROP DATABASE {database}")).await;
            execute(&client, &format!("DROP DATABASE {index_database}")).await;
        }
    }
}
