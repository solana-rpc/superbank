// SPDX-License-Identifier: AGPL-3.0-only

//! Durable full-history block-time index with a segmented in-process read path.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clickhouse::Row;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::clickhouse::{BlockMetadataRecord, ClickHouseClient};

use super::DiskCacheError;
use super::coverage::CoverageMap;

const SEGMENT_SLOTS: u64 = 1_000_000;
const UNKNOWN: i64 = i64::MIN;
const SKIPPED: i64 = i64::MIN + 1;
const NULL_TIME: i64 = i64::MIN + 2;
const DATA_TABLE: &str = "block_times";
const COVERAGE_TABLE: &str = "coverage";

#[derive(Debug, Clone)]
pub(crate) struct BlockIndexConfig {
    pub(crate) database: String,
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
struct BlockTimeRow {
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

#[derive(Debug, Clone, Deserialize, Row)]
struct HydrateRow {
    slot: u64,
    block_time: Option<i64>,
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
    coverage: RwLock<CoverageMap>,
}

impl BlockIndex {
    pub(crate) async fn open(
        cfg: BlockIndexConfig,
        admin: &ClickHouseClient,
    ) -> Result<Self, DiskCacheError> {
        let database = quote_identifier(&cfg.database)?;
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

        let mut local = admin.clone();
        local.database = cfg.database.clone();
        local.client = local.client.clone().with_database(cfg.database.clone());
        Ok(Self {
            cfg,
            local,
            segments: RwLock::new(HashMap::new()),
            coverage: RwLock::new(CoverageMap::new()),
        })
    }

    pub(crate) fn tip_span(&self) -> Option<(u64, u64)> {
        self.coverage
            .read()
            .expect("block-index coverage lock")
            .contiguous_tip_span()
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
            match self.value(slot) {
                NULL_TIME => BlockTimeLookup::Found(None),
                UNKNOWN | SKIPPED => BlockTimeLookup::Skipped,
                value => BlockTimeLookup::Found(Some(value)),
            }
        };
        crate::metrics::block_index_lookup("get_block_time", started.elapsed().as_secs_f64());
        result
    }

    pub(crate) fn slots_in_range(&self, start: u64, end: u64) -> Option<Vec<u64>> {
        let started = Instant::now();
        let result = self.slots_in_range_inner(start, end);
        crate::metrics::block_index_lookup("get_blocks", started.elapsed().as_secs_f64());
        result
    }

    fn slots_in_range_inner(&self, start: u64, end: u64) -> Option<Vec<u64>> {
        if end < start {
            return Some(Vec::new());
        }
        let coverage = self.coverage.read().expect("block-index coverage lock");
        if !coverage.holes_in(start, end).is_empty() {
            return None;
        }
        drop(coverage);
        let capacity = usize::try_from(end - start + 1).unwrap_or(0);
        let mut slots = Vec::with_capacity(capacity);
        let mut cursor = start;
        while cursor <= end {
            let segment_id = cursor / SEGMENT_SLOTS;
            let segment_end = end.min((segment_id + 1) * SEGMENT_SLOTS - 1);
            let segment = self
                .segments
                .read()
                .expect("block-index segments lock")
                .get(&segment_id)
                .cloned()?;
            let first = cursor % SEGMENT_SLOTS;
            let last = segment_end % SEGMENT_SLOTS;
            let first_word = first / 64;
            let last_word = last / 64;
            for word_index in first_word..=last_word {
                let mut word = segment.produced[word_index as usize].load(Ordering::Relaxed);
                if word_index == first_word {
                    word &= u64::MAX << (first % 64);
                }
                if word_index == last_word && last % 64 != 63 {
                    word &= (1u64 << (last % 64 + 1)) - 1;
                }
                while word != 0 {
                    let bit = word.trailing_zeros() as u64;
                    slots.push(segment_id * SEGMENT_SLOTS + word_index * 64 + bit);
                    word &= word - 1;
                }
            }
            cursor = segment_end.saturating_add(1);
        }
        Some(slots)
    }

    fn value(&self, slot: u64) -> i64 {
        let segment_id = slot / SEGMENT_SLOTS;
        let offset = (slot % SEGMENT_SLOTS) as usize;
        let segment = self
            .segments
            .read()
            .expect("block-index segments lock")
            .get(&segment_id)
            .cloned();
        segment.map_or(UNKNOWN, |segment| {
            segment.times[offset].load(Ordering::Relaxed)
        })
    }

    fn segment(&self, segment_id: u64) -> Segment {
        if let Some(segment) = self
            .segments
            .read()
            .expect("block-index segments lock")
            .get(&segment_id)
            .cloned()
        {
            return segment;
        }
        let segment: Segment = Arc::new(SegmentData {
            times: (0..SEGMENT_SLOTS)
                .map(|_| AtomicI64::new(UNKNOWN))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            produced: (0..SEGMENT_SLOTS.div_ceil(64))
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        });
        self.segments
            .write()
            .expect("block-index segments lock")
            .entry(segment_id)
            .or_insert_with(|| segment.clone())
            .clone()
    }

    fn publish_memory(&self, start: u64, end: u64, rows: &[BlockMetadataRecord]) {
        if end < start {
            return;
        }
        let mut cursor = start;
        let mut row_index = 0;
        while cursor <= end {
            let segment_id = cursor / SEGMENT_SLOTS;
            let segment_end = end.min((segment_id + 1) * SEGMENT_SLOTS - 1);
            let segment = self.segment(segment_id);
            while row_index < rows.len() && rows[row_index].slot < cursor {
                row_index += 1;
            }
            while row_index < rows.len() && rows[row_index].slot <= segment_end {
                let row = &rows[row_index];
                let offset = row.slot % SEGMENT_SLOTS;
                segment.times[offset as usize]
                    .store(row.block_time.unwrap_or(NULL_TIME), Ordering::Relaxed);
                segment.produced[(offset / 64) as usize]
                    .fetch_or(1u64 << (offset % 64), Ordering::Relaxed);
                row_index += 1;
            }
            cursor = segment_end.saturating_add(1);
        }
        self.coverage
            .write()
            .expect("block-index coverage lock")
            .insert_range(start, end);
        self.publish_metrics();
    }

    fn publish_metrics(&self) {
        let (floor, head) = self.tip_span().unwrap_or((0, 0));
        let bytes = self
            .segments
            .read()
            .expect("block-index segments lock")
            .len() as u64
            * (SEGMENT_SLOTS * std::mem::size_of::<i64>() as u64
                + SEGMENT_SLOTS.div_ceil(64) * std::mem::size_of::<u64>() as u64);
        crate::metrics::block_index_state(floor, head, bytes);
    }

    async fn load_local(&self) -> Result<(), DiskCacheError> {
        let database = quote_identifier(&self.cfg.database)?;
        let ranges = self
            .local
            .client
            .query(&format!(
                "SELECT range_start, argMax(range_end, version) AS range_end \
                 FROM {database}.{COVERAGE_TABLE} GROUP BY range_start ORDER BY range_start DESC"
            ))
            .fetch_all::<HydrateCoverageRow>()
            .await
            .map_err(|err| DiskCacheError::ClickHouse(err.to_string()))?;
        for range in ranges {
            let rows = self
                .local
                .client
                .query(&format!(
                    "SELECT slot, tupleElement(argMax(tuple(block_time), version), 1) AS block_time \
                     FROM {database}.{DATA_TABLE} WHERE slot BETWEEN {} AND {} GROUP BY slot ORDER BY slot",
                    range.range_start, range.range_end
                ))
                .fetch_all::<HydrateRow>()
                .await
                .map_err(|err| DiskCacheError::ClickHouse(err.to_string()))?;
            let metadata = rows
                .into_iter()
                .map(|row| minimal_metadata(row.slot, row.block_time))
                .collect::<Vec<_>>();
            self.publish_memory(range.range_start, range.range_end, &metadata);
        }
        Ok(())
    }

    async fn persist_range(
        &self,
        start: u64,
        end: u64,
        rows: &[BlockMetadataRecord],
    ) -> Result<(), DiskCacheError> {
        let version = now_version();
        let mut insert = self
            .local
            .client
            .insert::<BlockTimeRow>(DATA_TABLE)
            .await
            .map_err(|err| DiskCacheError::ClickHouse(err.to_string()))?;
        for row in rows {
            insert
                .write(&BlockTimeRow {
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
        coverage
            .write(&CoverageRow {
                range_start: start,
                range_end: end,
                version,
            })
            .await
            .map_err(|err| DiskCacheError::ClickHouse(err.to_string()))?;
        coverage
            .end()
            .await
            .map_err(|err| DiskCacheError::ClickHouse(err.to_string()))?;
        self.publish_memory(start, end, rows);
        Ok(())
    }

    pub(crate) async fn run(
        self: Arc<Self>,
        source: ClickHouseClient,
        mut shutdown: tokio::sync::broadcast::Receiver<()>,
    ) {
        crate::metrics::block_index_enabled(true);
        if let Err(err) = self.load_local().await {
            crate::metrics::block_index_error("hydrate");
            warn!("block index: local hydration failed: {err}");
        }
        info!(
            database = self.cfg.database,
            "block index: historical worker started"
        );
        loop {
            let latest = match source.get_latest_finalized_slot().await {
                Ok(Some(slot)) => slot,
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
            let (start, end) = match self.tip_span() {
                Some((_, head)) if head < latest => {
                    (head + 1, latest.min(head + self.cfg.slots_per_query))
                }
                Some((floor, _)) if floor > 0 => {
                    let end = floor - 1;
                    (end.saturating_sub(self.cfg.slots_per_query - 1), end)
                }
                Some(_) => {
                    if wait_or_shutdown(&mut shutdown, Duration::from_secs(5)).await {
                        break;
                    }
                    continue;
                }
                None => (latest.saturating_sub(self.cfg.slots_per_query - 1), latest),
            };
            let started = Instant::now();
            match source
                .get_block_metadata_by_slot_range(start, end, self.cfg.query_timeout)
                .await
            {
                Ok((rows, _)) => {
                    if let Err(err) = self.persist_range(start, end, &rows).await {
                        crate::metrics::block_index_error("persist");
                        warn!(start, end, "block index: local persist failed: {err}");
                    }
                }
                Err(err) => {
                    crate::metrics::block_index_error("source_read");
                    warn!(start, end, "block index: source read failed: {err}");
                }
            }
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

fn minimal_metadata(slot: u64, block_time: Option<i64>) -> BlockMetadataRecord {
    BlockMetadataRecord {
        slot,
        parent_slot: 0,
        blockhash: [0; 32],
        parent_blockhash: [0; 32],
        block_time,
        block_height: None,
        executed_transaction_count: 0,
        entry_count: 0,
        rewards_present: false,
        rewards_pubkey: Vec::new(),
        rewards_lamports: Vec::new(),
        rewards_post_balance: Vec::new(),
        rewards_type: Vec::new(),
        rewards_commission: Vec::new(),
        rewards_commission_bps: Vec::new(),
        rewards_num_partitions: None,
    }
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
    fn hydration_metadata_preserves_nullable_time() {
        assert_eq!(
            minimal_metadata(42, Some(1_700_000_000)).block_time,
            Some(1_700_000_000)
        );
        assert_eq!(minimal_metadata(43, None).block_time, None);
    }
}
