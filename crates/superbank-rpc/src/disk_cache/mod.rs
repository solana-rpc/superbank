// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! RocksDB-backed cache of recent finalized slots, served in place of ClickHouse.
//!
//! Stores only finalized data (no fork handling). It is strictly a cache: every
//! miss, decode failure, or inconsistency degrades to a ClickHouse read. A slot is
//! "covered" only when its data and coverage marker landed in one atomic
//! `WriteBatch`, so the cache never claims a slot it holds partially.
//!
//! The ClickHouse-to-disk fill is called "backfill" everywhere — "hydration" in
//! this crate means converting stored records to RPC JSON.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use rocksdb::{
    BoundColumnFamily, DBWithThreadMode, IteratorMode, MultiThreaded, Options, ReadOptions,
    WriteBatch,
};
use solana_transaction_status::TransactionDetails;
use tokio::sync::Semaphore;
use tracing::{info, warn};

use crate::clickhouse::{
    BlockMetadataRecord, StoredAccountsTransactionRecord, StoredBlockPayload, StoredBlockRecord,
    StoredTransactionRecord,
};

pub(crate) mod codec;
pub(crate) mod coverage;
pub(crate) mod filler;
pub(crate) mod index;
pub(crate) mod ingest;
pub(crate) mod schema;
pub(crate) mod writer;

use codec::CoverageValue;
use coverage::CoverageMap;
pub(crate) use index::DiskSigStatus;

type Db = DBWithThreadMode<MultiThreaded>;

/// Longest run of slots we will mark Skipped from one block's parent link. Runs
/// beyond this indicate corrupt metadata rather than a real chain gap.
const MAX_SKIPPED_RUN: u64 = 100_000;

/// When the byte budget trips, evict down to this fraction of it (hysteresis).
const BYTE_BUDGET_LOW_WATER: f64 = 0.93;

/// Column families range-deleted immediately by eviction. Index CFs are cleaned
/// by compaction filters and can lag without changing the slot floor estimate.
const BYTE_BUDGET_EVICTION_CFS: [&str; 3] = [
    schema::CF_SLOT_COVERAGE,
    schema::CF_BLOCK_META,
    schema::CF_TX,
];

fn byte_budget_floor(
    head: u64,
    max_bytes: u64,
    data_live_bytes: u64,
    covered_slots: u64,
) -> Option<u64> {
    if max_bytes == 0 || data_live_bytes < max_bytes {
        return None;
    }

    let bytes_per_slot = (data_live_bytes / covered_slots.max(1)).max(1);
    let target_bytes = (max_bytes as f64 * BYTE_BUDGET_LOW_WATER) as u64;
    let keep_slots = (target_bytes / bytes_per_slot).max(1);
    Some(head.saturating_sub(keep_slots - 1))
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum DiskCacheError {
    #[error("rocksdb: {0}")]
    Rocks(#[from] rocksdb::Error),
    #[error("encode: {0}")]
    Encode(#[from] bincode::Error),
    #[error("slot {slot} incomplete: expected {expected} transactions, got {actual}")]
    IncompleteSlot {
        slot: u64,
        expected: u64,
        actual: usize,
    },
    #[error("slot {slot} is below the retention floor {floor}")]
    BelowFloor { slot: u64, floor: u64 },
    #[error("corrupt address-index entry (slot {slot:?})")]
    CorruptIndexEntry { slot: Option<u64> },
}

#[derive(Debug, Clone)]
pub(crate) struct DiskCacheConfig {
    pub(crate) path: PathBuf,
    pub(crate) retain_slots: u64,
    /// Disk byte budget; 0 = unlimited. The tighter of byte budget / slot window wins.
    pub(crate) max_bytes: u64,
    pub(crate) block_cache_bytes: usize,
    pub(crate) read_concurrency: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SlotStatus {
    Covered { tx_count: u32 },
    Skipped,
    NotCovered,
}

#[derive(Debug)]
pub(crate) enum DiskBlockResult {
    Found(Box<StoredBlockPayload>),
    Skipped,
    NotCovered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DiskBlockTime {
    Found(Option<i64>),
    Skipped,
    NotCovered,
}

/// One page of a newest-first address-index scan over the contiguous tip span.
#[derive(Debug)]
pub(crate) struct DiskGsfaPage {
    pub(crate) records: Vec<crate::clickhouse::SignatureRecord>,
    /// The scan hit the coverage floor before filling the limit: ClickHouse must
    /// be consulted for slots strictly below `floor`.
    pub(crate) reached_floor: bool,
    /// Coverage floor the scan was evaluated against.
    pub(crate) floor: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct EvictionStats {
    pub(crate) old_floor: u64,
    pub(crate) new_floor: u64,
    pub(crate) byte_budget_bound: bool,
}

pub(crate) struct DiskCache {
    inner: Arc<DiskCacheInner>,
}

pub(crate) struct DiskCacheInner {
    db: Db,
    cfg: DiskCacheConfig,
    coverage: RwLock<CoverageMap>,
    /// Eviction floor: nothing below this slot may be claimed or served. Shared
    /// with the compaction filters of the `sig`/`addr_sig`/`token_owner` CFs.
    min_retained: Arc<AtomicU64>,
    ready: AtomicBool,
    read_sem: Arc<Semaphore>,
}

impl DiskCache {
    /// Open (or wipe-and-recreate) the cache at `cfg.path`.
    ///
    /// Corruption and schema-version mismatches destroy and rebuild the database —
    /// it is only a cache. Path or permission errors propagate so a misconfigured
    /// deployment fails loudly at startup.
    pub(crate) fn open(cfg: DiskCacheConfig) -> Result<Self, DiskCacheError> {
        let min_retained = Arc::new(AtomicU64::new(0));
        let opts = schema::DiskCacheOptions {
            block_cache_bytes: cfg.block_cache_bytes,
            ..Default::default()
        };

        let db = match open_db(&cfg.path, &opts, min_retained.clone()) {
            Ok(db) => db,
            Err(err) => {
                warn!(
                    path = %cfg.path.display(),
                    "disk cache: open failed ({err}); destroying and rebuilding"
                );
                crate::metrics::disk_cache_wipe();
                destroy_db(&cfg.path)?;
                open_db(&cfg.path, &opts, min_retained.clone())?
            }
        };

        let db = match check_schema_version(db)? {
            SchemaCheck::Ok(db) => db,
            SchemaCheck::Mismatch { db, found } => {
                warn!(
                    found,
                    expected = schema::SCHEMA_VERSION,
                    path = %cfg.path.display(),
                    "disk cache: schema version mismatch; wiping"
                );
                drop(db);
                crate::metrics::disk_cache_wipe();
                destroy_db(&cfg.path)?;
                let db = open_db(&cfg.path, &opts, min_retained.clone())?;
                match check_schema_version(db)? {
                    SchemaCheck::Ok(db) => db,
                    SchemaCheck::Mismatch { .. } => {
                        unreachable!("fresh disk cache database has a schema version")
                    }
                }
            }
        };

        let inner = Arc::new(DiskCacheInner {
            db,
            read_sem: Arc::new(Semaphore::new(cfg.read_concurrency.max(1))),
            cfg,
            coverage: RwLock::new(CoverageMap::new()),
            min_retained,
            ready: AtomicBool::new(false),
        });

        inner.load_persisted_state()?;
        inner.ready.store(true, Ordering::Release);

        let (span, slots) = {
            let map = inner.coverage.read().expect("coverage lock");
            (map.contiguous_tip_span(), map.covered_slot_count())
        };
        info!(
            path = %inner.cfg.path.display(),
            covered_slots = slots,
            tip_span = ?span,
            "disk cache: open"
        );

        Ok(Self { inner })
    }

    pub(crate) fn ready(&self) -> bool {
        self.inner.ready.load(Ordering::Acquire)
    }

    /// The contiguous covered range ending at the newest covered slot — the only
    /// span address-index scans may be answered from.
    pub(crate) fn tip_span(&self) -> Option<(u64, u64)> {
        self.inner
            .coverage
            .read()
            .expect("coverage lock")
            .contiguous_tip_span()
    }

    pub(crate) fn covers_slot(&self, slot: u64) -> bool {
        self.inner
            .coverage
            .read()
            .expect("coverage lock")
            .contains(slot)
    }

    pub(crate) fn min_retained_slot(&self) -> u64 {
        self.inner.min_retained.load(Ordering::Relaxed)
    }

    pub(crate) fn holes_in(&self, start: u64, end: u64) -> Vec<(u64, u64)> {
        self.inner
            .coverage
            .read()
            .expect("coverage lock")
            .holes_in(start, end)
    }

    /// Write one finalized slot (block metadata + every transaction) atomically,
    /// together with its coverage marker and Skipped markers for the parent gap.
    /// Production writes flow through the writer thread; this is the direct
    /// surface used by tests.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn write_finalized_slot(
        &self,
        meta: &BlockMetadataRecord,
        txs: &[Arc<StoredTransactionRecord>],
        source: u8,
    ) -> Result<(), DiskCacheError> {
        self.inner.write_finalized_slot(meta, txs, source)
    }

    /// Apply window and byte-budget eviction; returns stats when the floor moved.
    /// Production eviction runs on the writer thread's tick; tests call this.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn maybe_evict(&self) -> Result<Option<EvictionStats>, DiskCacheError> {
        self.inner.maybe_evict()
    }

    pub(crate) fn inner_arc(&self) -> Arc<DiskCacheInner> {
        self.inner.clone()
    }

    pub(crate) async fn slot_status(&self, slot: u64) -> SlotStatus {
        let status = self
            .run_read("slot_status", move |inner| Ok(inner.slot_status_sync(slot)))
            .await
            .unwrap_or(SlotStatus::NotCovered);
        let outcome = match status {
            SlotStatus::Covered { .. } => "hit",
            SlotStatus::Skipped => "skipped",
            SlotStatus::NotCovered => "not_covered",
        };
        crate::metrics::disk_cache_read("slot_status", outcome);
        status
    }

    pub(crate) async fn get_block(
        &self,
        slot: u64,
        transaction_details: TransactionDetails,
    ) -> DiskBlockResult {
        let result = self
            .run_read("get_block", move |inner| {
                Ok(inner.get_block_sync(slot, transaction_details))
            })
            .await
            .unwrap_or(DiskBlockResult::NotCovered);
        let outcome = match &result {
            DiskBlockResult::Found(_) => "hit",
            DiskBlockResult::Skipped => "skipped",
            DiskBlockResult::NotCovered => "not_covered",
        };
        crate::metrics::disk_cache_read("get_block", outcome);
        result
    }

    pub(crate) async fn block_time_for_slot(&self, slot: u64) -> DiskBlockTime {
        let result = self
            .run_read("block_time", move |inner| Ok(inner.block_time_sync(slot)))
            .await
            .unwrap_or(DiskBlockTime::NotCovered);
        let outcome = match result {
            DiskBlockTime::Found(_) => "hit",
            DiskBlockTime::Skipped => "skipped",
            DiskBlockTime::NotCovered => "not_covered",
        };
        crate::metrics::disk_cache_read("block_time", outcome);
        result
    }

    /// Covered (non-skipped) slots within `[start, end]`, ascending.
    pub(crate) async fn covered_slots_in_range(&self, start: u64, end: u64) -> Option<Vec<u64>> {
        self.run_read("slots_in_range", move |inner| {
            inner.covered_slots_in_range_sync(start, end)
        })
        .await
    }

    /// Full transaction lookup by any of its signatures.
    pub(crate) async fn get_tx(
        &self,
        signature: solana_sdk::signature::Signature,
    ) -> Option<StoredTransactionRecord> {
        let result = self
            .run_read("get_tx", move |inner| Ok(inner.get_tx_sync(&signature)))
            .await
            .flatten();
        crate::metrics::disk_cache_read("get_tx", if result.is_some() { "hit" } else { "miss" });
        result
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn get_sig_status(
        &self,
        signature: solana_sdk::signature::Signature,
    ) -> Option<DiskSigStatus> {
        let result = self
            .run_read("get_sig_status", move |inner| {
                Ok(inner.get_sig_status_sync(&signature))
            })
            .await
            .flatten();
        crate::metrics::disk_cache_read(
            "get_sig_status",
            if result.is_some() { "hit" } else { "miss" },
        );
        result
    }

    /// Batch signature-status lookup; one blocking hop for the whole batch.
    pub(crate) async fn get_sig_statuses(
        &self,
        signatures: Vec<solana_sdk::signature::Signature>,
    ) -> Vec<Option<DiskSigStatus>> {
        let count = signatures.len();
        let results: Vec<Option<DiskSigStatus>> = self
            .run_read("get_sig_statuses", move |inner| {
                Ok(signatures
                    .iter()
                    .map(|signature| inner.get_sig_status_sync(signature))
                    .collect())
            })
            .await
            .unwrap_or_else(|| vec![None; count]);
        for result in &results {
            crate::metrics::disk_cache_read(
                "get_sig_statuses",
                if result.is_some() { "hit" } else { "miss" },
            );
        }
        results
    }

    pub(crate) async fn signature_position(
        &self,
        signature: solana_sdk::signature::Signature,
    ) -> Option<crate::clickhouse::SignatureSlot> {
        let result = self
            .run_read("signature_position", move |inner| {
                Ok(inner.signature_position_sync(&signature))
            })
            .await
            .flatten();
        crate::metrics::disk_cache_read(
            "signature_position",
            if result.is_some() { "hit" } else { "miss" },
        );
        result
    }

    /// getTransactionsForAddress page from the contiguous tip span; same
    /// `None` semantics as [`Self::signatures_for_address`].
    pub(crate) async fn transactions_for_address(
        &self,
        address: solana_sdk::pubkey::Pubkey,
        query: index::DiskTfaQuery,
    ) -> Option<DiskGsfaPage> {
        let result = self
            .run_read("transactions_for_address", move |inner| {
                inner.transactions_for_address_sync(&address, &query)
            })
            .await
            .flatten();
        crate::metrics::disk_cache_read(
            "transactions_for_address",
            if result.is_some() { "hit" } else { "miss" },
        );
        result
    }

    /// Batch full-record fetch by `(slot, idx)` position; per-entry `None` on a
    /// miss (e.g. the eviction floor passed the slot after the index scan).
    pub(crate) async fn get_txs_by_position(
        &self,
        positions: Vec<(u64, u32)>,
    ) -> Vec<Option<StoredTransactionRecord>> {
        let count = positions.len();
        self.run_read("get_txs_by_position", move |inner| {
            Ok(inner.get_txs_by_position_sync(&positions))
        })
        .await
        .unwrap_or_else(|| vec![None; count])
    }

    /// Newest-first gSFA page from the contiguous tip span. `None` means the
    /// disk tier contributed nothing (no coverage, or a read error) and the
    /// caller must use ClickHouse with its original bounds.
    pub(crate) async fn signatures_for_address(
        &self,
        address: solana_sdk::pubkey::Pubkey,
        before: Option<crate::clickhouse::SlotBoundary>,
        until: Option<crate::clickhouse::SlotBoundary>,
        limit: usize,
    ) -> Option<DiskGsfaPage> {
        let result = self
            .run_read("signatures_for_address", move |inner| {
                inner.signatures_for_address_sync(&address, before, until, limit)
            })
            .await
            .flatten();
        crate::metrics::disk_cache_read(
            "signatures_for_address",
            if result.is_some() { "hit" } else { "miss" },
        );
        result
    }

    /// Run a blocking read against RocksDB off the async runtime, bounded by the
    /// read semaphore. Any error is logged and becomes a miss (`None`).
    async fn run_read<T, F>(&self, op: &'static str, f: F) -> Option<T>
    where
        T: Send + 'static,
        F: FnOnce(&DiskCacheInner) -> Result<T, DiskCacheError> + Send + 'static,
    {
        if !self.ready() {
            return None;
        }
        let permit = self.inner.read_sem.clone().acquire_owned().await.ok()?;
        let inner = self.inner.clone();
        let result = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            f(&inner)
        })
        .await;

        match result {
            Ok(Ok(value)) => Some(value),
            Ok(Err(err)) => {
                warn!(op, "disk cache: read failed: {err}");
                None
            }
            Err(err) => {
                warn!(op, "disk cache: read task panicked: {err}");
                None
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn inner_for_tests(&self) -> &DiskCacheInner {
        &self.inner
    }
}

impl DiskCacheInner {
    fn cf(&self, name: &str) -> Arc<BoundColumnFamily<'_>> {
        self.db
            .cf_handle(name)
            .unwrap_or_else(|| panic!("column family {name} exists"))
    }

    pub(crate) fn min_retained(&self) -> u64 {
        self.min_retained.load(Ordering::Relaxed)
    }

    pub(crate) fn covers_slot(&self, slot: u64) -> bool {
        self.coverage.read().expect("coverage lock").contains(slot)
    }

    pub(crate) fn flush_wal(&self) -> Result<(), DiskCacheError> {
        self.db.flush_wal(true)?;
        Ok(())
    }

    fn publish_coverage_metrics(&self) {
        let map = self.coverage.read().expect("coverage lock");
        let (min_covered, max_covered) = map.covered_span().unwrap_or((0, 0));
        let contiguous_floor = map.contiguous_tip_span().map_or(0, |(floor, _)| floor);
        crate::metrics::disk_cache_coverage(min_covered, max_covered, contiguous_floor);
    }

    fn load_persisted_state(&self) -> Result<(), DiskCacheError> {
        let meta_cf = self.cf(schema::CF_META);
        let floor = self
            .db
            .get_cf(&meta_cf, schema::META_MIN_RETAINED)?
            .and_then(|raw| raw.try_into().ok().map(u64::from_be_bytes))
            .unwrap_or(0);
        self.min_retained.store(floor, Ordering::Relaxed);

        let coverage_cf = self.cf(schema::CF_SLOT_COVERAGE);
        let mut read_opts = ReadOptions::default();
        read_opts.set_iterate_lower_bound(schema::slot_key(floor).to_vec());
        let iter = self
            .db
            .iterator_cf_opt(&coverage_cf, read_opts, IteratorMode::Start);

        let mut map = CoverageMap::new();
        for entry in iter {
            let (key, value) = entry?;
            let Some(slot) = key
                .as_ref()
                .try_into()
                .ok()
                .map(|bytes: [u8; 8]| u64::from_be_bytes(bytes))
            else {
                continue;
            };
            if codec::decode_coverage_value(&value).is_some() {
                map.insert(slot);
            }
        }

        *self.coverage.write().expect("coverage lock") = map;
        Ok(())
    }

    fn write_finalized_slot(
        &self,
        meta: &BlockMetadataRecord,
        txs: &[Arc<StoredTransactionRecord>],
        source: u8,
    ) -> Result<(), DiskCacheError> {
        let slot = meta.slot;
        let floor = self.min_retained.load(Ordering::Relaxed);
        if slot < floor {
            return Err(DiskCacheError::BelowFloor { slot, floor });
        }
        if txs.len() as u64 != meta.executed_transaction_count {
            return Err(DiskCacheError::IncompleteSlot {
                slot,
                expected: meta.executed_transaction_count,
                actual: txs.len(),
            });
        }

        let mut batch = WriteBatch::default();
        let block_meta_cf = self.cf(schema::CF_BLOCK_META);
        let tx_cf = self.cf(schema::CF_TX);
        let coverage_cf = self.cf(schema::CF_SLOT_COVERAGE);
        let sig_cf = self.cf(schema::CF_SIG);
        let addr_cf = self.cf(schema::CF_ADDR_SIG);
        let token_cf = self.cf(schema::CF_TOKEN_OWNER);

        batch.put_cf(
            &block_meta_cf,
            schema::slot_key(slot),
            codec::encode_record(meta)?,
        );
        for record in txs {
            batch.put_cf(
                &tx_cf,
                schema::tx_key(slot, record.slot_idx),
                codec::encode_record(record.as_ref())?,
            );

            let entries = index::derive_index_entries(record);
            // signatures.sql ARRAY JOINs tx_signatures: any signature resolves.
            let sig_value = codec::encode_sig_value(slot, record.slot_idx, entries.err.as_deref());
            for tx_signature in &entries.signatures {
                batch.put_cf(&sig_cf, tx_signature, &sig_value);
            }

            let addr_value = codec::encode_addr_sig_value(&codec::AddrSigValue {
                signature: record.signature,
                err: entries.err.clone(),
                memo: entries.memo.clone(),
                block_time: record.block_time,
                balance_changed: false,
            });
            for address in &entries.addresses {
                batch.put_cf(
                    &addr_cf,
                    schema::addr_sig_key(address, slot, record.slot_idx),
                    &addr_value,
                );
            }

            for token_entry in &entries.token_entries {
                let token_value = codec::encode_addr_sig_value(&codec::AddrSigValue {
                    signature: record.signature,
                    err: entries.err.clone(),
                    memo: entries.memo.clone(),
                    block_time: record.block_time,
                    balance_changed: token_entry.balance_changed,
                });
                batch.put_cf(
                    &token_cf,
                    schema::token_owner_key(
                        &token_entry.owner,
                        slot,
                        record.slot_idx,
                        &token_entry.token_account,
                    ),
                    &token_value,
                );
            }
        }
        batch.put_cf(
            &coverage_cf,
            schema::slot_key(slot),
            codec::encode_coverage_value(CoverageValue::Covered {
                tx_count: txs.len() as u32,
                source,
            }),
        );

        // The parent link of a finalized block proves every slot in between was
        // skipped on the finalized chain.
        let claim_start = if meta.parent_slot < slot {
            let gap = slot - meta.parent_slot - 1;
            if gap > 0 && gap <= MAX_SKIPPED_RUN {
                let skipped_value = codec::encode_coverage_value(CoverageValue::Skipped);
                let first = (meta.parent_slot + 1).max(floor);
                for skipped_slot in first..slot {
                    batch.put_cf(&coverage_cf, schema::slot_key(skipped_slot), &skipped_value);
                }
                first
            } else {
                if gap > MAX_SKIPPED_RUN {
                    warn!(
                        slot,
                        parent_slot = meta.parent_slot,
                        "disk cache: implausible parent gap; not marking skipped slots"
                    );
                }
                slot
            }
        } else {
            slot
        };

        self.db.write(batch)?;

        {
            let mut map = self.coverage.write().expect("coverage lock");
            map.insert_range(claim_start, slot);
        }
        self.publish_coverage_metrics();
        Ok(())
    }

    fn maybe_evict(&self) -> Result<Option<EvictionStats>, DiskCacheError> {
        let head = match self.coverage.read().expect("coverage lock").covered_span() {
            Some((_, head)) => head,
            None => return Ok(None),
        };

        let window_floor = head.saturating_sub(self.cfg.retain_slots.saturating_sub(1));

        let mut byte_budget_bound = false;
        let bytes_floor = if self.cfg.max_bytes > 0 {
            let data_live = self.live_sst_bytes_for(&BYTE_BUDGET_EVICTION_CFS);
            if data_live >= self.cfg.max_bytes {
                let covered_slots = self
                    .coverage
                    .read()
                    .expect("coverage lock")
                    .covered_slot_count();
                byte_budget_bound = true;
                byte_budget_floor(head, self.cfg.max_bytes, data_live, covered_slots)
                    .unwrap_or_default()
            } else {
                0
            }
        } else {
            0
        };

        let old_floor = self.min_retained.load(Ordering::Relaxed);
        let new_floor = window_floor.max(bytes_floor);
        if new_floor <= old_floor {
            return Ok(None);
        }

        // Raise the floor BEFORE deleting: readers bound by the floor can never
        // observe a partially deleted slot.
        self.min_retained.store(new_floor, Ordering::Relaxed);

        let mut batch = WriteBatch::default();
        batch.put_cf(
            &self.cf(schema::CF_META),
            schema::META_MIN_RETAINED,
            new_floor.to_be_bytes(),
        );
        let from = schema::slot_key(old_floor);
        let to = schema::slot_key(new_floor);
        batch.delete_range_cf(&self.cf(schema::CF_SLOT_COVERAGE), from, to);
        batch.delete_range_cf(&self.cf(schema::CF_BLOCK_META), from, to);
        batch.delete_range_cf(
            &self.cf(schema::CF_TX),
            schema::tx_key(old_floor, 0),
            schema::tx_key(new_floor, 0),
        );
        self.db.write(batch)?;

        self.coverage
            .write()
            .expect("coverage lock")
            .remove_below(new_floor);

        crate::metrics::disk_cache_evicted(
            if byte_budget_bound { "bytes" } else { "window" },
            new_floor - old_floor,
        );
        crate::metrics::disk_cache_size_bytes(self.live_sst_bytes());
        self.publish_coverage_metrics();

        Ok(Some(EvictionStats {
            old_floor,
            new_floor,
            byte_budget_bound,
        }))
    }

    fn live_sst_bytes(&self) -> u64 {
        self.live_sst_bytes_for(&schema::ALL_CFS)
    }

    fn live_sst_bytes_for(&self, cf_names: &[&str]) -> u64 {
        cf_names
            .iter()
            .filter_map(|name| {
                self.db
                    .property_int_value_cf(&self.cf(name), "rocksdb.live-sst-files-size")
                    .ok()
                    .flatten()
            })
            .sum()
    }

    /// Drop a slot from coverage after a decode or consistency failure. The slot
    /// degrades to ClickHouse until repair re-ingests it.
    fn poison_slot(&self, slot: u64) {
        warn!(slot, "disk cache: poisoning slot after inconsistency");
        crate::metrics::disk_cache_poisoned_slot();
        if let Err(err) = self
            .db
            .delete_cf(&self.cf(schema::CF_SLOT_COVERAGE), schema::slot_key(slot))
        {
            warn!(slot, "disk cache: failed to delete coverage marker: {err}");
        }
        self.coverage.write().expect("coverage lock").remove(slot);
        self.publish_coverage_metrics();
    }

    pub(crate) fn slot_status_sync(&self, slot: u64) -> SlotStatus {
        if slot < self.min_retained.load(Ordering::Relaxed) {
            return SlotStatus::NotCovered;
        }
        let Ok(Some(raw)) = self
            .db
            .get_cf(&self.cf(schema::CF_SLOT_COVERAGE), schema::slot_key(slot))
        else {
            return SlotStatus::NotCovered;
        };
        match codec::decode_coverage_value(&raw) {
            Some(CoverageValue::Covered { tx_count, .. }) => SlotStatus::Covered { tx_count },
            Some(CoverageValue::Skipped) => SlotStatus::Skipped,
            None => SlotStatus::NotCovered,
        }
    }

    pub(crate) fn get_block_sync(
        &self,
        slot: u64,
        transaction_details: TransactionDetails,
    ) -> DiskBlockResult {
        let tx_count = match self.slot_status_sync(slot) {
            SlotStatus::NotCovered => return DiskBlockResult::NotCovered,
            SlotStatus::Skipped => return DiskBlockResult::Skipped,
            SlotStatus::Covered { tx_count } => tx_count,
        };

        let Ok(Some(raw_meta)) = self
            .db
            .get_cf(&self.cf(schema::CF_BLOCK_META), schema::slot_key(slot))
        else {
            self.poison_slot(slot);
            return DiskBlockResult::NotCovered;
        };
        let Some(metadata) = codec::decode_record::<BlockMetadataRecord>(&raw_meta) else {
            self.poison_slot(slot);
            return DiskBlockResult::NotCovered;
        };

        if transaction_details == TransactionDetails::None {
            return DiskBlockResult::Found(Box::new(StoredBlockPayload::Metadata(metadata)));
        }

        let transactions = match self.read_slot_transactions(slot, tx_count) {
            Some(transactions) => transactions,
            None => {
                self.poison_slot(slot);
                return DiskBlockResult::NotCovered;
            }
        };

        let payload = match transaction_details {
            TransactionDetails::None => unreachable!("handled above"),
            TransactionDetails::Signatures => StoredBlockPayload::Signatures {
                metadata,
                signatures: transactions
                    .iter()
                    .map(|record| bs58::encode(record.signature).into_string())
                    .collect(),
            },
            TransactionDetails::Accounts => StoredBlockPayload::Accounts {
                metadata,
                transactions: transactions
                    .into_iter()
                    .map(StoredAccountsTransactionRecord::from)
                    .collect(),
            },
            TransactionDetails::Full => StoredBlockPayload::Full(StoredBlockRecord {
                metadata,
                transactions,
            }),
        };
        DiskBlockResult::Found(Box::new(payload))
    }

    /// All transactions of `slot` in execution order, or `None` when the stored
    /// set does not exactly match the claimed count.
    fn read_slot_transactions(
        &self,
        slot: u64,
        expected: u32,
    ) -> Option<Vec<StoredTransactionRecord>> {
        let mut transactions = Vec::with_capacity(expected as usize);
        if expected == 0 {
            return Some(transactions);
        }

        let tx_cf = self.cf(schema::CF_TX);
        let mut read_opts = ReadOptions::default();
        if let Some(next_slot) = slot.checked_add(1) {
            read_opts.set_iterate_upper_bound(schema::tx_key(next_slot, 0).to_vec());
        }
        read_opts.set_prefix_same_as_start(true);
        let start = schema::tx_key(slot, 0);
        let iter = self.db.iterator_cf_opt(
            &tx_cf,
            read_opts,
            IteratorMode::From(&start, rocksdb::Direction::Forward),
        );

        for entry in iter {
            let (_, value) = entry.ok()?;
            let record = codec::decode_record::<StoredTransactionRecord>(&value)?;
            transactions.push(record);
            if transactions.len() > expected as usize {
                return None;
            }
        }

        (transactions.len() == expected as usize).then_some(transactions)
    }

    pub(crate) fn block_time_sync(&self, slot: u64) -> DiskBlockTime {
        match self.slot_status_sync(slot) {
            SlotStatus::NotCovered => return DiskBlockTime::NotCovered,
            SlotStatus::Skipped => return DiskBlockTime::Skipped,
            SlotStatus::Covered { .. } => {}
        }
        let Ok(Some(raw)) = self
            .db
            .get_cf(&self.cf(schema::CF_BLOCK_META), schema::slot_key(slot))
        else {
            self.poison_slot(slot);
            return DiskBlockTime::NotCovered;
        };
        match codec::decode_record::<BlockMetadataRecord>(&raw) {
            Some(metadata) => DiskBlockTime::Found(metadata.block_time),
            None => {
                self.poison_slot(slot);
                DiskBlockTime::NotCovered
            }
        }
    }

    pub(crate) fn covered_slots_in_range_sync(
        &self,
        start: u64,
        end: u64,
    ) -> Result<Vec<u64>, DiskCacheError> {
        let floor = self.min_retained.load(Ordering::Relaxed);
        let start = start.max(floor);
        if end < start {
            return Ok(Vec::new());
        }

        let coverage_cf = self.cf(schema::CF_SLOT_COVERAGE);
        let mut read_opts = ReadOptions::default();
        if let Some(after_end) = end.checked_add(1) {
            read_opts.set_iterate_upper_bound(schema::slot_key(after_end).to_vec());
        }
        let start_key = schema::slot_key(start);
        let iter = self.db.iterator_cf_opt(
            &coverage_cf,
            read_opts,
            IteratorMode::From(&start_key, rocksdb::Direction::Forward),
        );

        let mut slots = Vec::new();
        for entry in iter {
            let (key, value) = entry?;
            let Some(slot) = key
                .as_ref()
                .try_into()
                .ok()
                .map(|bytes: [u8; 8]| u64::from_be_bytes(bytes))
            else {
                continue;
            };
            if matches!(
                codec::decode_coverage_value(&value),
                Some(CoverageValue::Covered { .. })
            ) {
                slots.push(slot);
            }
        }
        Ok(slots)
    }

    #[cfg(test)]
    pub(crate) fn delete_tx_for_tests(&self, slot: u64, idx: u32) {
        self.db
            .delete_cf(&self.cf(schema::CF_TX), schema::tx_key(slot, idx))
            .expect("delete tx");
    }

    #[cfg(test)]
    pub(crate) fn corrupt_block_meta_for_tests(&self, slot: u64) {
        self.db
            .put_cf(
                &self.cf(schema::CF_BLOCK_META),
                schema::slot_key(slot),
                [codec::VALUE_VERSION_V1, 0xFF, 0xFF],
            )
            .expect("corrupt block meta");
    }
}

fn open_db(
    path: &Path,
    opts: &schema::DiskCacheOptions,
    min_retained: Arc<AtomicU64>,
) -> Result<Db, rocksdb::Error> {
    let db_opts = schema::db_options(opts);
    let descriptors = schema::cf_descriptors(opts, min_retained);
    Db::open_cf_descriptors(&db_opts, path, descriptors)
}

fn destroy_db(path: &Path) -> Result<(), DiskCacheError> {
    Db::destroy(&Options::default(), path)?;
    Ok(())
}

enum SchemaCheck {
    Ok(Db),
    Mismatch { db: Db, found: u32 },
}

fn check_schema_version(db: Db) -> Result<SchemaCheck, DiskCacheError> {
    let meta_cf = db
        .cf_handle(schema::CF_META)
        .expect("meta column family exists");
    let stored = db
        .get_cf(&meta_cf, schema::META_SCHEMA_VERSION)?
        .and_then(|raw| raw.try_into().ok().map(u32::from_be_bytes));

    match stored {
        Some(version) if version == schema::SCHEMA_VERSION => {
            drop(meta_cf);
            Ok(SchemaCheck::Ok(db))
        }
        Some(version) => {
            drop(meta_cf);
            Ok(SchemaCheck::Mismatch { db, found: version })
        }
        None => {
            db.put_cf(
                &meta_cf,
                schema::META_SCHEMA_VERSION,
                schema::SCHEMA_VERSION.to_be_bytes(),
            )?;
            drop(meta_cf);
            Ok(SchemaCheck::Ok(db))
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use solana_sdk::hash::Hash;

    pub(crate) fn test_config(dir: &tempfile::TempDir) -> DiskCacheConfig {
        DiskCacheConfig {
            path: dir.path().join("db"),
            retain_slots: 1_000,
            max_bytes: 0,
            block_cache_bytes: 8 << 20,
            read_concurrency: 4,
        }
    }

    pub(crate) fn open_cache(dir: &tempfile::TempDir) -> DiskCache {
        DiskCache::open(test_config(dir)).expect("open disk cache")
    }

    pub(crate) fn block_metadata(
        slot: u64,
        parent_slot: u64,
        tx_count: u64,
    ) -> BlockMetadataRecord {
        BlockMetadataRecord {
            slot,
            parent_slot,
            blockhash: Hash::new_unique().to_bytes(),
            parent_blockhash: Hash::new_unique().to_bytes(),
            block_time: Some(1_700_000_000 + slot as i64),
            block_height: Some(slot.saturating_sub(5)),
            executed_transaction_count: tx_count,
            entry_count: tx_count,
            rewards_present: false,
            rewards_pubkey: Vec::new(),
            rewards_lamports: Vec::new(),
            rewards_post_balance: Vec::new(),
            rewards_type: Vec::new(),
            rewards_commission: Vec::new(),
            rewards_num_partitions: None,
        }
    }

    pub(crate) fn transaction(slot: u64, idx: u32) -> StoredTransactionRecord {
        let mut signature = [0u8; 64];
        signature[..8].copy_from_slice(&slot.to_be_bytes());
        signature[8..12].copy_from_slice(&idx.to_be_bytes());
        StoredTransactionRecord {
            signature,
            slot,
            slot_idx: idx,
            block_time: Some(1_700_000_000 + slot as i64),
            tx_version: None,
            tx_signatures: vec![signature],
            tx_num_required_signatures: 1,
            tx_num_readonly_signed_accounts: 0,
            tx_num_readonly_unsigned_accounts: 0,
            tx_account_keys: vec![[1u8; 32]],
            tx_recent_blockhash: [2u8; 32],
            tx_instructions_program_id_index: Vec::new(),
            tx_instructions_accounts: Vec::new(),
            tx_instructions_data: Vec::new(),
            tx_address_table_lookups_present: false,
            tx_address_table_lookup_account_key: Vec::new(),
            tx_address_table_lookup_writable_indexes: Vec::new(),
            tx_address_table_lookup_readonly_indexes: Vec::new(),
            meta_status_ok: true,
            meta_err: None,
            meta_fee: 5_000,
            meta_pre_balances: Vec::new(),
            meta_post_balances: Vec::new(),
            meta_inner_instructions_present: false,
            meta_inner_instructions_index: Vec::new(),
            meta_inner_instructions_program_id_index: Vec::new(),
            meta_inner_instructions_accounts: Vec::new(),
            meta_inner_instructions_data: Vec::new(),
            meta_inner_instructions_stack_height: Vec::new(),
            meta_log_messages_present: false,
            meta_log_messages: Vec::new(),
            meta_pre_token_balances_present: false,
            meta_pre_token_account_index: Vec::new(),
            meta_pre_token_mint: Vec::new(),
            meta_pre_token_owner: Vec::new(),
            meta_pre_token_program_id: Vec::new(),
            meta_pre_token_amount: Vec::new(),
            meta_pre_token_decimals: Vec::new(),
            meta_pre_token_ui_amount: Vec::new(),
            meta_pre_token_ui_amount_string: Vec::new(),
            meta_post_token_balances_present: false,
            meta_post_token_account_index: Vec::new(),
            meta_post_token_mint: Vec::new(),
            meta_post_token_owner: Vec::new(),
            meta_post_token_program_id: Vec::new(),
            meta_post_token_amount: Vec::new(),
            meta_post_token_decimals: Vec::new(),
            meta_post_token_ui_amount: Vec::new(),
            meta_post_token_ui_amount_string: Vec::new(),
            meta_rewards_present: false,
            meta_reward_pubkey: Vec::new(),
            meta_reward_lamports: Vec::new(),
            meta_reward_post_balance: Vec::new(),
            meta_reward_type: Vec::new(),
            meta_reward_commission: Vec::new(),
            meta_loaded_addresses_writable: Vec::new(),
            meta_loaded_addresses_readonly: Vec::new(),
            meta_return_data_present: false,
            meta_return_data_program_id: None,
            meta_return_data_data: None,
            meta_compute_units_consumed: None,
            meta_cost_units: None,
        }
    }

    pub(crate) fn write_slot(cache: &DiskCache, slot: u64, parent_slot: u64, tx_count: u32) {
        let meta = block_metadata(slot, parent_slot, u64::from(tx_count));
        let txs: Vec<_> = (0..tx_count)
            .map(|idx| Arc::new(transaction(slot, idx)))
            .collect();
        cache
            .write_finalized_slot(&meta, &txs, schema::COVERAGE_SOURCE_LIVE)
            .expect("write slot");
    }

    fn flush_all_cfs(cache: &DiskCache) {
        cache
            .inner_for_tests()
            .db
            .flush_cfs_opt(
                &schema::ALL_CFS
                    .iter()
                    .map(|name| cache.inner_for_tests().cf(name))
                    .collect::<Vec<_>>()
                    .iter()
                    .collect::<Vec<_>>(),
                &rocksdb::FlushOptions::default(),
            )
            .expect("flush");
    }

    fn unwrap_found(result: DiskBlockResult) -> StoredBlockPayload {
        match result {
            DiskBlockResult::Found(payload) => *payload,
            other => panic!("expected found block, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn write_then_read_block_all_detail_levels() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = open_cache(&dir);
        write_slot(&cache, 100, 99, 3);

        match unwrap_found(cache.get_block(100, TransactionDetails::None).await) {
            StoredBlockPayload::Metadata(meta) => {
                assert_eq!(meta.slot, 100);
            }
            other => panic!("unexpected: {other:?}"),
        }

        match unwrap_found(cache.get_block(100, TransactionDetails::Signatures).await) {
            StoredBlockPayload::Signatures { signatures, .. } => {
                assert_eq!(signatures.len(), 3);
            }
            other => panic!("unexpected: {other:?}"),
        }

        match unwrap_found(cache.get_block(100, TransactionDetails::Accounts).await) {
            StoredBlockPayload::Accounts { transactions, .. } => {
                assert_eq!(transactions.len(), 3);
            }
            other => panic!("unexpected: {other:?}"),
        }

        match unwrap_found(cache.get_block(100, TransactionDetails::Full).await) {
            StoredBlockPayload::Full(block) => {
                assert_eq!(block.transactions.len(), 3);
                // Execution order by slot_idx.
                let indexes: Vec<u32> = block
                    .transactions
                    .iter()
                    .map(|record| record.slot_idx)
                    .collect();
                assert_eq!(indexes, vec![0, 1, 2]);
            }
            other => panic!("unexpected: {other:?}"),
        }

        assert_eq!(
            cache.block_time_for_slot(100).await,
            DiskBlockTime::Found(Some(1_700_000_100))
        );
        assert!(matches!(
            cache.get_block(101, TransactionDetails::Full).await,
            DiskBlockResult::NotCovered
        ));
    }

    #[tokio::test]
    async fn zero_transaction_block_round_trips() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = open_cache(&dir);
        write_slot(&cache, 50, 49, 0);

        match unwrap_found(cache.get_block(50, TransactionDetails::Full).await) {
            StoredBlockPayload::Full(block) => {
                assert!(block.transactions.is_empty());
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn parent_gap_marks_skipped_slots() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = open_cache(&dir);
        write_slot(&cache, 100, 99, 1);
        // 101..=104 skipped on the finalized chain.
        write_slot(&cache, 105, 100, 1);

        assert_eq!(cache.slot_status(103).await, SlotStatus::Skipped);
        assert!(matches!(
            cache.get_block(103, TransactionDetails::Full).await,
            DiskBlockResult::Skipped
        ));
        assert_eq!(cache.block_time_for_slot(103).await, DiskBlockTime::Skipped);
        assert_eq!(cache.tip_span(), Some((100, 105)));

        assert_eq!(
            cache.covered_slots_in_range(99, 106).await,
            Some(vec![100, 105])
        );
    }

    #[tokio::test]
    async fn incomplete_slot_is_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = open_cache(&dir);
        let meta = block_metadata(10, 9, 2);
        let txs = vec![Arc::new(transaction(10, 0))];
        let err = cache
            .write_finalized_slot(&meta, &txs, schema::COVERAGE_SOURCE_LIVE)
            .expect_err("must reject");
        assert!(matches!(err, DiskCacheError::IncompleteSlot { .. }));
        assert!(!cache.covers_slot(10));
    }

    #[tokio::test]
    async fn missing_transaction_poisons_slot() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = open_cache(&dir);
        write_slot(&cache, 100, 99, 2);
        cache.inner_for_tests().delete_tx_for_tests(100, 1);

        assert!(matches!(
            cache.get_block(100, TransactionDetails::Full).await,
            DiskBlockResult::NotCovered
        ));
        // Poisoned: no longer claimed at all.
        assert!(!cache.covers_slot(100));
        assert_eq!(cache.slot_status(100).await, SlotStatus::NotCovered);
    }

    #[tokio::test]
    async fn corrupt_block_meta_poisons_slot() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = open_cache(&dir);
        write_slot(&cache, 100, 99, 1);
        cache.inner_for_tests().corrupt_block_meta_for_tests(100);

        assert!(matches!(
            cache.get_block(100, TransactionDetails::None).await,
            DiskBlockResult::NotCovered
        ));
        assert!(!cache.covers_slot(100));
    }

    #[tokio::test]
    async fn coverage_survives_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let cache = open_cache(&dir);
            write_slot(&cache, 100, 99, 2);
            write_slot(&cache, 101, 100, 1);
        }
        let cache = open_cache(&dir);
        assert_eq!(cache.tip_span(), Some((100, 101)));
        match unwrap_found(cache.get_block(101, TransactionDetails::Full).await) {
            StoredBlockPayload::Full(block) => {
                assert_eq!(block.transactions.len(), 1);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn schema_version_mismatch_wipes_database() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let cache = open_cache(&dir);
            write_slot(&cache, 100, 99, 1);
            // Force a version mismatch for the next open.
            cache
                .inner_for_tests()
                .db
                .put_cf(
                    &cache.inner_for_tests().cf(schema::CF_META),
                    schema::META_SCHEMA_VERSION,
                    9999u32.to_be_bytes(),
                )
                .expect("overwrite version");
        }
        let cache = open_cache(&dir);
        assert_eq!(cache.tip_span(), None);
        assert!(!cache.covers_slot(100));
    }

    #[tokio::test]
    async fn window_eviction_raises_floor_and_deletes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = test_config(&dir);
        cfg.retain_slots = 5;
        let cache = DiskCache::open(cfg).expect("open");

        for slot in 100..=110 {
            write_slot(&cache, slot, slot - 1, 1);
        }
        let stats = cache
            .maybe_evict()
            .expect("evict")
            .expect("floor should move");
        assert_eq!(stats.new_floor, 106);
        assert!(!stats.byte_budget_bound);

        assert_eq!(cache.min_retained_slot(), 106);
        assert!(!cache.covers_slot(105));
        assert!(cache.covers_slot(106));
        assert!(matches!(
            cache.get_block(105, TransactionDetails::Full).await,
            DiskBlockResult::NotCovered
        ));
        assert!(matches!(
            cache.get_block(110, TransactionDetails::Full).await,
            DiskBlockResult::Found(_)
        ));

        // Below-floor writes are refused.
        let err = cache
            .write_finalized_slot(
                &block_metadata(50, 49, 0),
                &[],
                schema::COVERAGE_SOURCE_LIVE,
            )
            .expect_err("below floor");
        assert!(matches!(err, DiskCacheError::BelowFloor { .. }));

        // Floor persists across reopen.
        drop(cache);
        let cache = open_cache(&dir);
        assert_eq!(cache.min_retained_slot(), 106);
        assert_eq!(cache.tip_span(), Some((106, 110)));
    }

    #[tokio::test]
    async fn byte_budget_eviction_binds_tighter_than_window() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = test_config(&dir);
        cfg.retain_slots = 1_000_000;
        cfg.max_bytes = 1; // Anything on disk trips the budget.
        let cache = DiskCache::open(cfg).expect("open");

        for slot in 100..=120 {
            write_slot(&cache, slot, slot - 1, 2);
        }
        // SST sizes only materialize after a flush.
        flush_all_cfs(&cache);

        let stats = cache
            .maybe_evict()
            .expect("evict")
            .expect("budget should bind");
        assert!(stats.byte_budget_bound);
        assert!(stats.new_floor > 100);
    }

    #[test]
    fn byte_budget_floor_uses_data_cf_pressure() {
        let head = 120;
        let max_bytes = 1_000;
        let covered_slots = 21;

        // The estimator receives evictable data-CF bytes, not total RocksDB
        // bytes, so lagging index CFs alone cannot move the floor.
        assert_eq!(
            byte_budget_floor(head, max_bytes, max_bytes - 1, covered_slots),
            None
        );
        assert!(byte_budget_floor(head, max_bytes, max_bytes, covered_slots).is_some());
    }

    #[tokio::test]
    async fn byte_budget_eviction_ignores_lagging_index_cf_pressure() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = test_config(&dir);
        cfg.retain_slots = 1_000_000;
        let cache = DiskCache::open(cfg.clone()).expect("open");

        for slot in 100..=104u64 {
            let mut record = transaction(slot, 0);
            record.tx_account_keys = (0..192)
                .map(|idx| {
                    let mut key = [0u8; 32];
                    key[..8].copy_from_slice(&slot.to_be_bytes());
                    key[8..16].copy_from_slice(&(idx as u64).to_be_bytes());
                    key
                })
                .collect();
            write_block(&cache, slot, slot - 1, vec![record]);
        }
        flush_all_cfs(&cache);

        let data_live = cache
            .inner_for_tests()
            .live_sst_bytes_for(&BYTE_BUDGET_EVICTION_CFS);
        let total_live = cache.inner_for_tests().live_sst_bytes();
        assert!(data_live > 0);
        assert!(total_live > data_live + 1);

        drop(cache);
        cfg.max_bytes = data_live + 1;
        let cache = DiskCache::open(cfg).expect("reopen");

        assert!(cache.maybe_evict().expect("evict").is_none());
        assert_eq!(cache.min_retained_slot(), 0);
    }

    fn transaction_with(
        slot: u64,
        idx: u32,
        addresses: &[[u8; 32]],
        status_err: Option<&str>,
    ) -> StoredTransactionRecord {
        let mut record = transaction(slot, idx);
        record.tx_account_keys = addresses.to_vec();
        if let Some(err) = status_err {
            record.meta_status_ok = false;
            record.meta_err = Some(err.to_string());
        }
        record
    }

    pub(crate) fn write_block(
        cache: &DiskCache,
        slot: u64,
        parent: u64,
        txs: Vec<StoredTransactionRecord>,
    ) {
        let meta = block_metadata(slot, parent, txs.len() as u64);
        let txs: Vec<_> = txs.into_iter().map(Arc::new).collect();
        cache
            .write_finalized_slot(&meta, &txs, schema::COVERAGE_SOURCE_LIVE)
            .expect("write block");
    }

    #[tokio::test]
    async fn get_tx_resolves_any_listed_signature() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = open_cache(&dir);

        let mut record = transaction(100, 0);
        let mut second_sig = [7u8; 64];
        second_sig[0] = 42;
        record.tx_signatures = vec![record.signature, second_sig];
        write_block(&cache, 100, 99, vec![record.clone()]);

        let primary = solana_sdk::signature::Signature::from(record.signature);
        let secondary = solana_sdk::signature::Signature::from(second_sig);
        let unknown = solana_sdk::signature::Signature::from([255u8; 64]);

        assert_eq!(
            cache.get_tx(primary).await.map(|r| r.signature),
            Some(record.signature)
        );
        assert_eq!(
            cache.get_tx(secondary).await.map(|r| r.signature),
            Some(record.signature)
        );
        assert!(cache.get_tx(unknown).await.is_none());

        let position = cache.signature_position(primary).await.expect("position");
        assert_eq!((position.slot, position.slot_idx), (100, 0));
    }

    #[tokio::test]
    async fn sig_status_propagates_error_and_respects_floor() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = test_config(&dir);
        cfg.retain_slots = 3;
        let cache = DiskCache::open(cfg).expect("open");

        let failed = transaction_with(
            100,
            0,
            &[[1u8; 32]],
            Some("{\"InstructionError\":[0,{\"Custom\":42}]}"),
        );
        let ok = transaction_with(100, 1, &[[1u8; 32]], None);
        let failed_sig = solana_sdk::signature::Signature::from(failed.signature);
        let ok_sig = solana_sdk::signature::Signature::from(ok.signature);
        write_block(&cache, 100, 99, vec![failed, ok]);

        let status = cache.get_sig_status(failed_sig).await.expect("status");
        assert_eq!(status.slot, 100);
        assert_eq!(status.slot_idx, 0);
        assert!(status.err.is_some());
        assert!(
            cache
                .get_sig_status(ok_sig)
                .await
                .expect("status")
                .err
                .is_none()
        );

        // Push the floor past slot 100: lingering index entries become invisible.
        for slot in 101..=110 {
            write_block(&cache, slot, slot - 1, vec![transaction(slot, 0)]);
        }
        cache.maybe_evict().expect("evict").expect("floor moved");
        assert!(cache.min_retained_slot() > 100);
        assert!(cache.get_sig_status(failed_sig).await.is_none());
        assert!(cache.get_tx(failed_sig).await.is_none());

        let batch = cache.get_sig_statuses(vec![failed_sig, ok_sig]).await;
        assert_eq!(batch, vec![None, None]);
    }

    #[tokio::test]
    async fn gsfa_scan_orders_filters_and_bounds() {
        use crate::clickhouse::{SignatureSlot, SlotBoundary};

        let dir = tempfile::tempdir().expect("tempdir");
        let cache = open_cache(&dir);

        let addr_a = [10u8; 32];
        let addr_b = [11u8; 32];
        let address = solana_sdk::pubkey::Pubkey::from(addr_a);

        // Slots 100..=104; address A in every slot, B only in 102.
        for slot in 100..=104u64 {
            let mut txs = vec![transaction_with(slot, 0, &[addr_a], None)];
            if slot == 102 {
                txs.push(transaction_with(slot, 1, &[addr_b], None));
            }
            write_block(&cache, slot, slot - 1, txs);
        }

        // Unbounded scan: newest-first, only address A, hits the floor unfilled.
        let page = cache
            .signatures_for_address(address, None, None, 10)
            .await
            .expect("page");
        let slots: Vec<u64> = page.records.iter().map(|r| r.slot).collect();
        assert_eq!(slots, vec![104, 103, 102, 101, 100]);
        assert!(page.reached_floor);
        assert_eq!(page.floor, 100);

        // Limit satisfied: no floor escape.
        let page = cache
            .signatures_for_address(address, None, None, 2)
            .await
            .expect("page");
        let slots: Vec<u64> = page.records.iter().map(|r| r.slot).collect();
        assert_eq!(slots, vec![104, 103]);
        assert!(!page.reached_floor);

        // before excludes the position itself.
        let before = SlotBoundary::Position(SignatureSlot {
            slot: 104,
            slot_idx: 0,
        });
        let page = cache
            .signatures_for_address(address, Some(before), None, 10)
            .await
            .expect("page");
        let slots: Vec<u64> = page.records.iter().map(|r| r.slot).collect();
        assert_eq!(slots, vec![103, 102, 101, 100]);

        // before as a slot bound: strictly below it.
        let page = cache
            .signatures_for_address(address, Some(SlotBoundary::Slot(103)), None, 10)
            .await
            .expect("page");
        let slots: Vec<u64> = page.records.iter().map(|r| r.slot).collect();
        assert_eq!(slots, vec![102, 101, 100]);

        // until inside the span: fully answered, no floor escape.
        let page = cache
            .signatures_for_address(address, None, Some(SlotBoundary::Slot(101)), 10)
            .await
            .expect("page");
        let slots: Vec<u64> = page.records.iter().map(|r| r.slot).collect();
        assert_eq!(slots, vec![104, 103, 102]);
        assert!(!page.reached_floor);

        // until below the floor: the floor still bounds the scan.
        let page = cache
            .signatures_for_address(address, None, Some(SlotBoundary::Slot(50)), 10)
            .await
            .expect("page");
        assert_eq!(page.records.len(), 5);
        assert!(page.reached_floor);
    }

    #[tokio::test]
    async fn gsfa_scan_orders_within_slot_and_carries_metadata() {
        use std::str::FromStr;

        let dir = tempfile::tempdir().expect("tempdir");
        let cache = open_cache(&dir);

        let addr = [20u8; 32];
        let address = solana_sdk::pubkey::Pubkey::from(addr);
        let memo_program =
            solana_sdk::pubkey::Pubkey::from_str("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr")
                .unwrap()
                .to_bytes();

        let mut with_memo = transaction_with(100, 1, &[addr], None);
        with_memo.tx_account_keys.push(memo_program);
        with_memo.tx_instructions_program_id_index = vec![1];
        with_memo.tx_instructions_data = vec![b"hi".to_vec()];

        let failed = transaction_with(100, 0, &[addr], Some("\"AccountNotFound\""));
        write_block(&cache, 100, 99, vec![failed, with_memo]);

        let page = cache
            .signatures_for_address(address, None, None, 10)
            .await
            .expect("page");
        assert_eq!(page.records.len(), 2);
        // Desc by slot_idx within the slot.
        assert_eq!(page.records[0].slot_idx, 1);
        assert_eq!(page.records[0].memo.as_deref(), Some("[2] hi"));
        assert!(page.records[0].err.is_none());
        assert_eq!(page.records[1].slot_idx, 0);
        assert!(page.records[1].err.is_some());
        assert_eq!(page.records[0].block_time, Some(1_700_000_100));
    }

    #[tokio::test]
    async fn gsfa_scan_without_coverage_returns_none() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = open_cache(&dir);
        let address = solana_sdk::pubkey::Pubkey::from([1u8; 32]);
        assert!(
            cache
                .signatures_for_address(address, None, None, 10)
                .await
                .is_none()
        );
    }

    #[test]
    fn holes_in_reflects_coverage() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = open_cache(&dir);
        write_slot(&cache, 100, 99, 1);
        write_slot(&cache, 105, 100, 1); // covers 101..=105 via skip markers
        write_slot(&cache, 110, 108, 1); // covers 109..=110; 106..=108 hole

        assert_eq!(cache.holes_in(100, 110), vec![(106, 108)]);
        assert_eq!(cache.tip_span(), Some((109, 110)));
    }
}
