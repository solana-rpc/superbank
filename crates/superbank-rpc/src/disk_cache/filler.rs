// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Unified source-to-local forwarding, backfill, and gap repair.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clickhouse::Row;
use futures_util::{StreamExt, stream::FuturesUnordered};
use serde::Deserialize;
use tracing::{debug, info, warn};

use crate::clickhouse::{BlockMetadataRecord, ClickHouseClient};

use super::schema::{CacheTableKind, SourceTableSchema};
use super::{DiskCache, DiskCacheError, SlotStatus};

const MIN_SLOTS_PER_ROUND: u64 = 64;
const MAX_SLOTS_PER_ROUND: u64 = 4_096;
const MAX_SKIPPED_RUN: u64 = 100_000;

#[derive(Debug, Clone, Deserialize, Row)]
struct NextBlockRow {
    slot: u64,
    parent_slot: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct FillerConfig {
    pub(crate) retain_slots: u64,
    pub(crate) slots_per_query: u64,
    pub(crate) max_concurrency: usize,
    pub(crate) max_slots_per_sec: u64,
    pub(crate) query_timeout: Duration,
    pub(crate) repair_interval: Duration,
    pub(crate) repair_min_lag_slots: u64,
    pub(crate) max_attempts: u32,
}

impl Default for FillerConfig {
    fn default() -> Self {
        Self {
            retain_slots: 432_000,
            slots_per_query: 8,
            max_concurrency: 4,
            max_slots_per_sec: 50,
            query_timeout: Duration::from_secs(30),
            repair_interval: Duration::from_secs(5),
            repair_min_lag_slots: 75,
            max_attempts: 10,
        }
    }
}

struct SlotRateLimiter {
    rate: f64,
    capacity: f64,
    tokens: f64,
    last_refill: Instant,
}

impl SlotRateLimiter {
    fn new(slots_per_query: u64, max_concurrency: usize, max_slots_per_sec: u64) -> Self {
        let initial = slots_per_query.saturating_mul(max_concurrency as u64) as f64;
        let rate = max_slots_per_sec as f64;
        Self {
            rate,
            capacity: (rate.max(initial) * 2.0).max(1.0),
            tokens: initial,
            last_refill: Instant::now(),
        }
    }

    fn refill(&mut self) {
        let now = Instant::now();
        self.refill_elapsed(now.duration_since(self.last_refill));
        self.last_refill = now;
    }

    fn refill_elapsed(&mut self, elapsed: Duration) {
        self.tokens = (self.tokens + elapsed.as_secs_f64() * self.rate).min(self.capacity);
    }

    /// Consumes the requested slots, or returns how long admission should wait.
    fn admit_or_wait(&mut self, slots: u64) -> Option<Duration> {
        self.refill();
        let slots = slots as f64;
        if self.tokens >= slots {
            self.tokens -= slots;
            return None;
        }
        Some(Duration::from_secs_f64(
            ((slots - self.tokens) / self.rate).max(0.001),
        ))
    }
}

struct FillOutcome {
    range: SlotRange,
    elapsed: Duration,
    result: Result<HashSet<u64>, DiskCacheError>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SlotRange {
    pub(crate) start: u64,
    pub(crate) end: u64,
}

impl SlotRange {
    fn len_slots(self) -> u64 {
        self.end - self.start + 1
    }
}

struct ClaimableWindow {
    floor: u64,
    tip: u64,
}

pub(crate) async fn run(
    cache: Arc<DiskCache>,
    source: ClickHouseClient,
    cfg: FillerConfig,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) {
    info!(
        retain_slots = cfg.retain_slots,
        slots_per_query = cfg.slots_per_query,
        max_concurrency = cfg.max_concurrency,
        max_slots_per_sec = cfg.max_slots_per_sec,
        "disk cache: ClickHouse forwarder started"
    );
    let mut attempts: HashMap<u64, u32> = HashMap::new();
    let mut given_up: HashSet<u64> = HashSet::new();
    let mut backoff = Duration::from_millis(250);
    let mut rate_limiter = SlotRateLimiter::new(
        cfg.slots_per_query,
        cfg.max_concurrency,
        cfg.max_slots_per_sec,
    );
    let mut last_schema_check = Instant::now();

    loop {
        if last_schema_check.elapsed() >= cache.inner.cfg.schema_check_interval {
            match cache.refresh_schema(&source).await {
                Ok(true) => {
                    attempts.clear();
                    given_up.clear();
                    info!("disk cache: source schema changed; local cache rebuilt");
                }
                Ok(false) => {}
                Err(err) => {
                    cache.set_ready(false);
                    crate::metrics::disk_cache_fill_error();
                    warn!("disk cache: source schema refresh failed: {err}");
                }
            }
            last_schema_check = Instant::now();
        }

        let Some(window) = claimable_window(&source, &cfg).await else {
            if wait_or_shutdown(&mut shutdown, cfg.repair_interval).await {
                break;
            }
            continue;
        };
        attempts.retain(|slot, _| *slot >= window.floor);
        given_up.retain(|slot| *slot >= window.floor);

        let holes = cache.holes_in(window.floor, window.tip);
        let remaining = holes.iter().map(|(start, end)| end - start + 1).sum();
        crate::metrics::disk_cache_backfill_remaining(remaining);
        let ranges = plan_ranges(
            &holes,
            &given_up,
            cfg.slots_per_query,
            round_slot_limit(cfg.slots_per_query, cfg.max_concurrency),
        );
        if ranges.is_empty() {
            if let Err(err) = cache.maybe_evict().await {
                warn!("disk cache: retention check failed: {err}");
            }
            if wait_or_shutdown(&mut shutdown, cfg.repair_interval).await {
                break;
            }
            continue;
        }

        let Some(outcomes) = fill_ranges_concurrently(
            &cache,
            &source,
            &cfg,
            ranges,
            &mut rate_limiter,
            &mut shutdown,
        )
        .await
        else {
            break;
        };

        let mut succeeded = false;
        let mut failed = false;
        for outcome in outcomes {
            let range = outcome.range;
            debug!(
                start = range.start,
                end = range.end,
                elapsed_ms = outcome.elapsed.as_millis(),
                "disk cache: forward batch complete"
            );
            match outcome.result {
                Ok(published) => {
                    succeeded = true;
                    for slot in range.start..=range.end {
                        if published.contains(&slot) {
                            attempts.remove(&slot);
                        } else {
                            let attempt = attempts.entry(slot).or_default();
                            *attempt += 1;
                            if *attempt >= cfg.max_attempts && given_up.insert(slot) {
                                warn!(
                                    slot,
                                    attempts = *attempt,
                                    "disk cache: giving up incomplete slot; source fallback remains active"
                                );
                            }
                        }
                    }
                }
                Err(err) => {
                    failed = true;
                    crate::metrics::disk_cache_fill_error();
                    crate::metrics::disk_cache_write_error();
                    warn!(
                        start = range.start,
                        end = range.end,
                        "disk cache: forward batch failed: {err}"
                    );
                }
            }
        }

        if succeeded {
            cache.set_ready(true);
            if let Err(err) = cache.maybe_evict().await {
                warn!("disk cache: retention check failed: {err}");
            }
        } else if failed {
            cache.set_ready(false);
        }

        if failed {
            if sleep_or_shutdown(&mut shutdown, backoff).await {
                break;
            }
            backoff = (backoff * 2).min(Duration::from_secs(10));
        } else {
            backoff = Duration::from_millis(250);
        }
    }
    crate::metrics::disk_cache_backfill_inflight(0);
    cache.set_ready(false);
    info!("disk cache: ClickHouse forwarder stopped");
}

fn round_slot_limit(slots_per_query: u64, max_concurrency: usize) -> u64 {
    let concurrency = u64::try_from(max_concurrency).unwrap_or(u64::MAX);
    slots_per_query
        .saturating_mul(concurrency)
        .saturating_mul(2)
        .clamp(MIN_SLOTS_PER_ROUND, MAX_SLOTS_PER_ROUND)
}

async fn fill_ranges_concurrently(
    cache: &Arc<DiskCache>,
    source: &ClickHouseClient,
    cfg: &FillerConfig,
    ranges: Vec<SlotRange>,
    rate_limiter: &mut SlotRateLimiter,
    shutdown: &mut tokio::sync::broadcast::Receiver<()>,
) -> Option<Vec<FillOutcome>> {
    let mut pending = VecDeque::from(ranges);
    let mut in_flight = FuturesUnordered::new();
    let mut outcomes = Vec::with_capacity(pending.len());
    let max_concurrency = cfg.max_concurrency.max(1);

    while !pending.is_empty() || !in_flight.is_empty() {
        let mut admission_wait = None;
        while in_flight.len() < max_concurrency {
            let Some(range) = pending.front().copied() else {
                break;
            };
            if let Some(wait) = rate_limiter.admit_or_wait(range.len_slots()) {
                admission_wait = Some(wait);
                break;
            }
            pending.pop_front();

            let cache = Arc::clone(cache);
            let source = source.clone();
            let cfg = cfg.clone();
            in_flight.push(async move {
                let started = Instant::now();
                let result = fill_range(&cache, &source, range, &cfg).await;
                FillOutcome {
                    range,
                    elapsed: started.elapsed(),
                    result,
                }
            });
        }
        crate::metrics::disk_cache_backfill_inflight(in_flight.len());

        if in_flight.is_empty() {
            let wait = admission_wait.unwrap_or(Duration::from_millis(1));
            if sleep_or_shutdown(shutdown, wait).await {
                crate::metrics::disk_cache_backfill_inflight(0);
                return None;
            }
            continue;
        }

        if let Some(wait) = admission_wait {
            tokio::select! {
                _ = shutdown.recv() => {
                    crate::metrics::disk_cache_backfill_inflight(0);
                    return None;
                }
                _ = tokio::time::sleep(wait) => {}
                Some(outcome) = in_flight.next() => outcomes.push(outcome),
            }
        } else {
            tokio::select! {
                _ = shutdown.recv() => {
                    crate::metrics::disk_cache_backfill_inflight(0);
                    return None;
                }
                Some(outcome) = in_flight.next() => outcomes.push(outcome),
            }
        }
    }

    crate::metrics::disk_cache_backfill_inflight(0);
    Some(outcomes)
}

async fn claimable_window(
    source: &ClickHouseClient,
    cfg: &FillerConfig,
) -> Option<ClaimableWindow> {
    let tip = match source.get_latest_finalized_slot().await {
        Ok(Some(tip)) => tip.saturating_sub(cfg.repair_min_lag_slots),
        Ok(None) => return None,
        Err(err) => {
            warn!("disk cache: cannot resolve source finalized tip: {err}");
            return None;
        }
    };
    if tip == 0 {
        return None;
    }
    // Always plan from the configured retention floor. Clamping this to the
    // current cache floor prevents a warm or partially filled cache from ever
    // expanding backward when retention increases.
    let floor = retention_floor(tip, cfg.retain_slots);
    Some(ClaimableWindow { floor, tip })
}

fn retention_floor(tip: u64, retain_slots: u64) -> u64 {
    tip.saturating_sub(retain_slots.saturating_sub(1))
}

pub(crate) fn plan_ranges(
    holes: &[(u64, u64)],
    given_up: &HashSet<u64>,
    slots_per_query: u64,
    max_slots: u64,
) -> Vec<SlotRange> {
    let mut slots = Vec::new();
    for &(start, end) in holes.iter().rev() {
        for slot in (start..=end).rev() {
            if !given_up.contains(&slot) {
                slots.push(slot);
                if slots.len() as u64 >= max_slots {
                    break;
                }
            }
        }
        if slots.len() as u64 >= max_slots {
            break;
        }
    }
    slots.sort_unstable();
    let mut runs = Vec::new();
    for slot in slots {
        match runs.last_mut() {
            Some(SlotRange { end, .. }) if end.saturating_add(1) == slot => *end = slot,
            _ => runs.push(SlotRange {
                start: slot,
                end: slot,
            }),
        }
    }

    let width = slots_per_query.max(1);
    let mut chunked = Vec::new();
    for run in runs.into_iter().rev() {
        let mut end = run.end;
        loop {
            let start = run.start.max(end.saturating_sub(width - 1));
            chunked.push(SlotRange { start, end });
            if start == run.start {
                break;
            }
            end = start - 1;
        }
    }
    chunked
}

async fn fill_range(
    cache: &DiskCache,
    source: &ClickHouseClient,
    range: SlotRange,
    cfg: &FillerConfig,
) -> Result<HashSet<u64>, DiskCacheError> {
    let started = Instant::now();
    let (metadata, _) = source
        .get_block_metadata_by_slot_range(range.start, range.end, cfg.query_timeout)
        .await
        .map_err(|err| DiskCacheError::ClickHouse(err.to_string()))?;
    let successor = next_block_after(source, range.end, cfg.query_timeout).await?;
    let expected: HashMap<u64, u64> = metadata
        .iter()
        .filter(|record| record.slot >= range.start && record.slot <= range.end)
        .map(|record| (record.slot, record.executed_transaction_count))
        .collect();

    let snapshot = cache.source_schema();
    let blocks = snapshot
        .table(CacheTableKind::BlocksMetadata)
        .ok_or_else(|| DiskCacheError::Config("blocks_metadata schema missing".to_string()))?;
    let transactions = snapshot
        .table(CacheTableKind::Transactions)
        .ok_or_else(|| DiskCacheError::Config("transactions schema missing".to_string()))?;
    // Forward the durable fact table first. The block table can use Memory,
    // which cannot deduplicate a retry after a later stage fails.
    native_forward(cache, source, transactions, range, cfg.query_timeout).await?;
    cache
        .validate_transaction_counts(range.start, range.end, &expected)
        .await?;
    native_forward(cache, source, blocks, range, cfg.query_timeout).await?;

    let coverage = coverage_from_metadata(range, &metadata, successor.as_ref());
    let published: HashSet<u64> = coverage.iter().map(|(slot, _)| *slot).collect();
    cache.publish_range_coverage(coverage).await?;
    let transactions_written = expected.values().copied().sum();
    crate::metrics::disk_cache_write(
        "backfill",
        transactions_written,
        started.elapsed().as_secs_f64(),
    );
    Ok(published)
}

async fn next_block_after(
    source: &ClickHouseClient,
    slot: u64,
    timeout: Duration,
) -> Result<Option<NextBlockRow>, DiskCacheError> {
    let max_slot = slot.saturating_add(MAX_SKIPPED_RUN).saturating_add(1);
    let query = format!(
        "SELECT slot, parent_slot FROM {} WHERE slot > {slot} AND slot <= {max_slot} ORDER BY slot LIMIT 1",
        source.blocks_metadata_table
    );
    tokio::time::timeout(
        timeout,
        source.client.query(&query).fetch_optional::<NextBlockRow>(),
    )
    .await
    .map_err(|_| {
        DiskCacheError::ClickHouse(format!(
            "source successor query timed out after {timeout:?}"
        ))
    })?
    .map_err(|err| DiskCacheError::ClickHouse(err.to_string()))
}

fn coverage_from_metadata(
    range: SlotRange,
    metadata: &[BlockMetadataRecord],
    successor: Option<&NextBlockRow>,
) -> Vec<(u64, SlotStatus)> {
    let mut covered = BTreeMap::new();
    for (slot, parent_slot, tx_count) in metadata
        .iter()
        .map(|record| {
            (
                record.slot,
                record.parent_slot,
                Some(record.executed_transaction_count),
            )
        })
        .chain(
            successor
                .into_iter()
                .map(|record| (record.slot, record.parent_slot, None)),
        )
    {
        if slot >= range.start && slot <= range.end {
            covered.insert(
                slot,
                SlotStatus::Covered {
                    tx_count: u32::try_from(tx_count.expect("in-range rows have counts"))
                        .unwrap_or(u32::MAX),
                },
            );
        }
        if parent_slot < slot {
            let gap = slot - parent_slot - 1;
            if gap <= MAX_SKIPPED_RUN {
                let start = range.start.max(parent_slot.saturating_add(1));
                let end = range.end.min(slot.saturating_sub(1));
                if start <= end {
                    for slot in start..=end {
                        covered.entry(slot).or_insert(SlotStatus::Skipped);
                    }
                }
            }
        }
    }
    covered.into_iter().collect()
}

async fn native_forward(
    cache: &DiskCache,
    source: &ClickHouseClient,
    table: &SourceTableSchema,
    range: SlotRange,
    timeout: Duration,
) -> Result<(), DiskCacheError> {
    let columns = table.insert_columns();
    if columns.is_empty() {
        return Err(DiskCacheError::Config(format!(
            "{} has no insertable columns",
            table.logical_name
        )));
    }
    let column_list = columns
        .iter()
        .map(|name| format!("`{name}`"))
        .collect::<Vec<_>>()
        .join(", ");
    let order_and_limit = match table.kind {
        CacheTableKind::Transactions => {
            " ORDER BY slot ASC, slot_idx ASC, signature ASC LIMIT 1 BY signature"
        }
        CacheTableKind::BlocksMetadata => " ORDER BY slot ASC LIMIT 1 BY slot",
        _ => {
            return Err(DiskCacheError::Config(format!(
                "native forwarding is not valid for derived table {:?}",
                table.kind
            )));
        }
    };
    let settings = source.query_settings_enabled().then(|| {
        format!(
            " SETTINGS max_execution_time = {}",
            timeout.as_secs_f64().ceil().max(1.0) as u64
        )
    });
    let select = format!(
        "SELECT {column_list} FROM {} WHERE slot BETWEEN {} AND {}{order_and_limit}{} FORMAT Native",
        table.logical_name,
        range.start,
        range.end,
        settings.as_deref().unwrap_or_default()
    );
    let target = format!(
        "`{}`.`{}`",
        cache.inner.cfg.database,
        table.kind.local_name()
    );
    let insert = format!(
        "INSERT INTO {target} ({column_list}) SETTINGS materialized_views_ignore_errors = 0 FORMAT Native"
    );

    tokio::time::timeout(
        timeout,
        forward_http_stream(cache, source, &select, &insert),
    )
    .await
    .map_err(|_| {
        DiskCacheError::ClickHouse(format!(
            "native forward {:?} timed out after {timeout:?}",
            table.kind
        ))
    })??;
    Ok(())
}

async fn forward_http_stream(
    cache: &DiskCache,
    source: &ClickHouseClient,
    select: &str,
    insert: &str,
) -> Result<(), DiskCacheError> {
    let http = &cache.inner.http;
    let mut source_url = reqwest::Url::parse(&source.url)
        .map_err(|err| DiskCacheError::ClickHouse(format!("invalid source URL: {err}")))?;
    source_url
        .query_pairs_mut()
        .append_pair("database", &source.database);
    let mut source_request = http.post(source_url).body(select.to_string());
    if !source.username.is_empty() {
        source_request = source_request.basic_auth(&source.username, Some(&source.password));
    }
    let source_response = source_request
        .send()
        .await
        .map_err(|err| DiskCacheError::ClickHouse(err.to_string()))?;
    if !source_response.status().is_success() {
        return Err(http_error("source SELECT", source_response).await);
    }

    let body = reqwest::Body::wrap_stream(source_response.bytes_stream());
    let mut local_url = reqwest::Url::parse(&cache.inner.cfg.url)
        .map_err(|err| DiskCacheError::ClickHouse(format!("invalid cache URL: {err}")))?;
    local_url
        .query_pairs_mut()
        .append_pair("database", &cache.inner.cfg.database)
        .append_pair("query", insert)
        .append_pair("wait_end_of_query", "1");
    let mut local_request = http.post(local_url).body(body);
    if !cache.inner.cfg.username.is_empty() {
        local_request =
            local_request.basic_auth(&cache.inner.cfg.username, Some(&cache.inner.cfg.password));
    }
    let local_response = local_request
        .send()
        .await
        .map_err(|err| DiskCacheError::ClickHouse(err.to_string()))?;
    if !local_response.status().is_success() {
        return Err(http_error("local INSERT", local_response).await);
    }
    Ok(())
}

async fn http_error(stage: &str, response: reqwest::Response) -> DiskCacheError {
    let status = response.status();
    let mut body = response.text().await.unwrap_or_default();
    body.truncate(2_048);
    DiskCacheError::ClickHouse(format!("{stage} returned {status}: {body}"))
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

async fn sleep_or_shutdown(
    shutdown: &mut tokio::sync::broadcast::Receiver<()>,
    duration: Duration,
) -> bool {
    wait_or_shutdown(shutdown, duration).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn planner_prefers_newest_holes_and_chunks() {
        let ranges = plan_ranges(&[(1, 5), (10, 20)], &HashSet::new(), 4, 8);
        assert_eq!(
            ranges,
            vec![
                SlotRange { start: 17, end: 20 },
                SlotRange { start: 13, end: 16 },
            ]
        );
    }

    #[test]
    fn round_size_scales_with_batch_width_and_concurrency() {
        assert_eq!(round_slot_limit(8, 1), 64);
        assert_eq!(round_slot_limit(8, 4), 64);
        assert_eq!(round_slot_limit(32, 8), 512);
        assert_eq!(round_slot_limit(1_024, 64), MAX_SLOTS_PER_ROUND);
    }

    #[test]
    fn retention_floor_expands_to_the_full_configured_window() {
        assert_eq!(retention_floor(1_500_000, 1_200_000), 300_001);
        assert_eq!(retention_floor(10, 1_200_000), 0);
    }

    #[test]
    fn rate_limiter_allows_one_initial_range_per_worker() {
        let mut limiter = SlotRateLimiter::new(8, 4, 50);
        for _ in 0..4 {
            assert!(limiter.admit_or_wait(8).is_none());
        }
        assert!(limiter.admit_or_wait(8).is_some());

        limiter.refill_elapsed(Duration::from_millis(160));
        assert!(limiter.admit_or_wait(8).is_none());
    }

    #[test]
    fn planner_builds_enough_ranges_to_fill_all_workers() {
        let ranges = plan_ranges(&[(1, 1_000)], &HashSet::new(), 32, round_slot_limit(32, 8));
        assert_eq!(ranges.len(), 16);
        assert!(ranges.iter().all(|range| range.len_slots() == 32));
        assert_eq!(
            ranges.first(),
            Some(&SlotRange {
                start: 969,
                end: 1_000
            })
        );
    }

    #[test]
    fn parent_links_prove_skipped_slots_but_not_unbounded_tail() {
        let metadata = vec![BlockMetadataRecord {
            slot: 105,
            parent_slot: 100,
            blockhash: [0; 32],
            parent_blockhash: [0; 32],
            block_time: None,
            block_height: None,
            executed_transaction_count: 2,
            entry_count: 0,
            rewards_present: false,
            rewards_pubkey: Vec::new(),
            rewards_lamports: Vec::new(),
            rewards_post_balance: Vec::new(),
            rewards_type: Vec::new(),
            rewards_commission: Vec::new(),
            rewards_commission_bps: Vec::new(),
            rewards_num_partitions: None,
        }];
        let rows = coverage_from_metadata(
            SlotRange {
                start: 101,
                end: 107,
            },
            &metadata,
            None,
        );
        assert_eq!(rows.len(), 5);
        assert!(matches!(
            rows.last(),
            Some((105, SlotStatus::Covered { tx_count: 2 }))
        ));
    }

    #[test]
    fn successor_proves_a_trailing_skipped_run() {
        let rows = coverage_from_metadata(
            SlotRange {
                start: 101,
                end: 104,
            },
            &[],
            Some(&NextBlockRow {
                slot: 105,
                parent_slot: 100,
            }),
        );
        assert_eq!(
            rows,
            vec![
                (101, SlotStatus::Skipped),
                (102, SlotStatus::Skipped),
                (103, SlotStatus::Skipped),
                (104, SlotStatus::Skipped),
            ]
        );
    }
}
