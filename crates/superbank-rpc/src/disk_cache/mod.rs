// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Local ClickHouse forward cache for recent finalized slots.
//!
//! The source ClickHouse cluster is authoritative. This tier only serves slot
//! ranges whose base rows and dependent materialized views completed and whose
//! coverage marker was published last.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clickhouse::Row;
use serde::{Deserialize, Serialize};
use solana_transaction_status::TransactionDetails;
use tracing::{info, warn};

use crate::clickhouse::{
    ClickHouseClient, ClickHouseClientOptions, ClickHouseTableNames, PaginationToken,
    QueryCacheConfig, RoutingPolicy, RoutingScope, RoutingTransport, SignatureRecord, SlotBoundary,
    SortOrder, StoredBlockPayload, StoredBlockRecord, StoredTransactionRecord, TokenAccountsFilter,
    TransactionsForAddressQuery,
};
use crate::config::ClickHouseStartupTableCheck;
use crate::solana_sdk;

pub(crate) mod coverage;
pub(crate) mod filler;
pub(crate) mod index;
pub(crate) mod schema;

use coverage::CoverageMap;
pub(crate) use index::DiskSigStatus;
use schema::{CacheSchemaConfig, CacheTableKind, SourceSchemaSnapshot};

const BYTE_BUDGET_LOW_WATER_PERCENT: u64 = 90;

#[derive(Debug, thiserror::Error)]
pub(crate) enum DiskCacheError {
    #[error(transparent)]
    Schema(#[from] schema::SchemaError),
    #[error("clickhouse cache operation failed: {0}")]
    ClickHouse(String),
    #[error("invalid disk-cache configuration: {0}")]
    Config(String),
    #[error("slot {slot} incomplete: expected {expected} transactions, got {actual}")]
    IncompleteSlot {
        slot: u64,
        expected: u64,
        actual: u64,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct DiskCacheConfig {
    pub(crate) url: String,
    pub(crate) database: String,
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) required: bool,
    pub(crate) retain_slots: u64,
    pub(crate) max_bytes: u64,
    pub(crate) partition_slots: u64,
    pub(crate) query_timeout: Duration,
    pub(crate) schema_check_interval: Duration,
    pub(crate) memory_blocks_metadata: bool,
    pub(crate) memory_retain_slots: Option<u64>,
    pub(crate) memory_max_bytes: Option<u64>,
}

impl DiskCacheConfig {
    pub(crate) fn schema_config(&self) -> CacheSchemaConfig {
        CacheSchemaConfig {
            database: self.database.clone(),
            partition_slots: self.partition_slots,
            memory_blocks_metadata: self.memory_blocks_metadata,
            memory_retain_slots: self.memory_retain_slots,
            memory_max_bytes: self.memory_max_bytes,
        }
    }
}

pub(crate) fn automatic_partition_slots(retain_slots: u64) -> u64 {
    let width = retain_slots.saturating_add(127) / 128;
    let rounded = (width.saturating_add(999) / 1_000).saturating_mul(1_000);
    rounded.clamp(10_000, 432_000)
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

#[derive(Debug)]
pub(crate) struct DiskGsfaPage {
    pub(crate) records: Vec<SignatureRecord>,
    pub(crate) reached_floor: bool,
    pub(crate) reached_tip: bool,
    pub(crate) floor: u64,
    pub(crate) tip: u64,
}

#[derive(Debug, Clone, Deserialize, Row)]
struct CoverageReadRow {
    slot: u64,
    status: i8,
    tx_count: u32,
}

#[derive(Debug, Clone, Serialize, Row)]
struct CoverageWriteRow {
    slot: u64,
    status: i8,
    tx_count: u32,
    version: u64,
}

#[derive(Debug, Clone, Deserialize, Row)]
struct SlotRow {
    slot: u64,
}

#[derive(Debug, Clone, Deserialize, Row)]
struct CountRow {
    slot: u64,
    count: u64,
}

#[derive(Debug, Clone, Deserialize, Row)]
struct BytesRow {
    bytes: u64,
}

#[derive(Debug, Clone, Deserialize, Row)]
struct HealthRow {
    ok: u8,
}

#[derive(Debug, Clone, Deserialize, Row)]
struct PartitionRow {
    partition_id: String,
}

#[derive(Clone)]
pub(crate) struct DiskCache {
    inner: Arc<DiskCacheInner>,
}

pub(crate) struct DiskCacheInner {
    pub(crate) cfg: DiskCacheConfig,
    admin: ClickHouseClient,
    pub(crate) local: ClickHouseClient,
    query_client: RwLock<ClickHouseClient>,
    pub(crate) http: reqwest::Client,
    pub(crate) schema: RwLock<Arc<SourceSchemaSnapshot>>,
    coverage: RwLock<CoverageMap>,
    min_retained: AtomicU64,
    ready: AtomicBool,
}

impl DiskCache {
    pub(crate) async fn open(
        cfg: DiskCacheConfig,
        source: &ClickHouseClient,
    ) -> Result<Self, DiskCacheError> {
        validate_config(&cfg)?;
        let table_names = ClickHouseTableNames::in_database(&cfg.database);
        // Bootstrap through ClickHouse's built-in database. A client whose
        // default database is the cache database cannot create that database
        // on first boot because ClickHouse rejects the request first.
        let mut admin = ClickHouseClient::new(
            &cfg.url,
            "default",
            &cfg.username,
            &cfg.password,
            ClickHouseClientOptions::new(
                RoutingPolicy {
                    transport: RoutingTransport::Http,
                    scope: RoutingScope::Distributed,
                },
                None,
                source.gsfa_hot_addresses.clone(),
                table_names.gsfa_hot.clone(),
                table_names.gsfa_hot.clone(),
            )
            .with_query_timeout(cfg.query_timeout)
            .with_query_cache_config(QueryCacheConfig::default())
            .with_http_concurrency(8)
            .with_startup_table_check(ClickHouseStartupTableCheck::Exists),
        );
        admin.use_table_names(table_names.clone());
        let mut local = ClickHouseClient::new(
            &cfg.url,
            &cfg.database,
            &cfg.username,
            &cfg.password,
            ClickHouseClientOptions::new(
                RoutingPolicy {
                    transport: RoutingTransport::Http,
                    scope: RoutingScope::Distributed,
                },
                None,
                source.gsfa_hot_addresses.clone(),
                table_names.gsfa_hot.clone(),
                table_names.gsfa_hot.clone(),
            )
            .with_query_timeout(cfg.query_timeout)
            .with_query_cache_config(QueryCacheConfig::default())
            .with_http_concurrency(64)
            .with_startup_table_check(ClickHouseStartupTableCheck::Exists),
        );
        local.use_table_names(table_names);
        local.set_blocks_metadata_supports_prewhere(!cfg.memory_blocks_metadata);

        let schema_config = cfg.schema_config();
        let snapshot = schema::inspect_source_schema(source, &schema_config).await?;
        let rebuilt = schema::initialize_cache_schema(&admin, &snapshot, &schema_config).await?;
        if rebuilt {
            crate::metrics::disk_cache_wipe();
        }
        local
            .create_tables()
            .await
            .map_err(|err| DiskCacheError::ClickHouse(err.to_string()))?;

        let inner = Arc::new(DiskCacheInner {
            cfg,
            admin,
            query_client: RwLock::new(local.clone()),
            local,
            http: reqwest::Client::new(),
            schema: RwLock::new(Arc::new(snapshot)),
            coverage: RwLock::new(CoverageMap::new()),
            min_retained: AtomicU64::new(0),
            ready: AtomicBool::new(false),
        });
        let cache = Self { inner };
        cache.reload_coverage().await?;
        cache.set_ready(true);
        cache.publish_coverage_metrics();
        info!(
            url = cache.inner.cfg.url,
            database = cache.inner.cfg.database,
            retain_slots = cache.inner.cfg.retain_slots,
            partition_slots = cache.inner.cfg.partition_slots,
            "disk cache: local ClickHouse ready"
        );
        Ok(cache)
    }

    pub(crate) fn ready(&self) -> bool {
        self.inner.ready.load(Ordering::Acquire)
    }

    pub(crate) fn required(&self) -> bool {
        self.inner.cfg.required
    }

    pub(crate) async fn healthy(&self) -> bool {
        self.ready() && self.ping().await
    }

    async fn ping(&self) -> bool {
        matches!(
            tokio::time::timeout(
                self.inner.cfg.query_timeout,
                self.inner
                    .local
                    .client
                    .query("SELECT toUInt8(1) AS ok")
                    .fetch_one::<HealthRow>(),
            )
            .await,
            Ok(Ok(HealthRow { ok: 1 }))
        )
    }

    pub(crate) fn set_ready(&self, ready: bool) {
        self.inner.ready.store(ready, Ordering::Release);
        crate::metrics::disk_cache_set_active(ready);
    }

    pub(crate) fn tip_span(&self) -> Option<(u64, u64)> {
        if !self.ready() {
            return None;
        }
        self.inner
            .coverage
            .read()
            .expect("coverage lock")
            .contiguous_tip_span()
    }

    pub(crate) fn covers_slot(&self, slot: u64) -> bool {
        self.ready()
            && slot >= self.min_retained_slot()
            && self
                .inner
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

    pub(crate) fn source_schema(&self) -> Arc<SourceSchemaSnapshot> {
        self.inner.schema.read().expect("schema lock").clone()
    }

    fn query_client(&self) -> ClickHouseClient {
        self.inner
            .query_client
            .read()
            .expect("query client lock")
            .clone()
    }

    async fn reload_coverage(&self) -> Result<(), DiskCacheError> {
        let query = format!(
            "SELECT slot, status, tx_count FROM {} FINAL WHERE status != 0 ORDER BY slot",
            schema::COVERAGE_TABLE
        );
        let rows = self
            .inner
            .local
            .client
            .query(&query)
            .fetch_all::<CoverageReadRow>()
            .await
            .map_err(|err| DiskCacheError::ClickHouse(err.to_string()))?;
        let mut map = CoverageMap::new();
        for row in &rows {
            map.insert(row.slot);
        }
        let floor = map.covered_span().map_or(0, |(floor, _)| floor);
        *self.inner.coverage.write().expect("coverage lock") = map;
        self.inner.min_retained.store(floor, Ordering::Relaxed);
        Ok(())
    }

    pub(crate) async fn refresh_schema(
        &self,
        source: &ClickHouseClient,
    ) -> Result<bool, DiskCacheError> {
        let schema_config = self.inner.cfg.schema_config();
        let snapshot = schema::inspect_source_schema(source, &schema_config).await?;
        if snapshot.fingerprint == self.source_schema().fingerprint {
            if !self.ready() && self.ping().await {
                self.set_ready(true);
            }
            return Ok(false);
        }

        self.set_ready(false);
        let result = async {
            schema::initialize_cache_schema(&self.inner.admin, &snapshot, &schema_config).await?;
            let mut query_client = self.inner.local.clone();
            query_client
                .create_tables()
                .await
                .map_err(|err| DiskCacheError::ClickHouse(err.to_string()))?;
            *self.inner.query_client.write().expect("query client lock") = query_client;
            *self.inner.schema.write().expect("schema lock") = Arc::new(snapshot);
            self.reload_coverage().await?;
            Ok::<(), DiskCacheError>(())
        }
        .await;
        if result.is_ok() {
            crate::metrics::disk_cache_wipe();
            self.set_ready(true);
            self.publish_coverage_metrics();
        }
        result.map(|()| true)
    }

    fn publish_coverage_metrics(&self) {
        let map = self.inner.coverage.read().expect("coverage lock");
        let (min_covered, max_covered) = map.covered_span().unwrap_or((0, 0));
        let contiguous_floor = map.contiguous_tip_span().map_or(0, |(floor, _)| floor);
        crate::metrics::disk_cache_coverage(min_covered, max_covered, contiguous_floor);
    }

    pub(crate) async fn slot_status(&self, slot: u64) -> SlotStatus {
        if !self.ready() || !self.covers_slot(slot) {
            crate::metrics::disk_cache_read("slot_status", "not_covered");
            return SlotStatus::NotCovered;
        }
        let query = format!(
            "SELECT slot, status, tx_count FROM {} FINAL WHERE slot = {slot} AND status != 0 LIMIT 1",
            schema::COVERAGE_TABLE
        );
        let result = self
            .inner
            .local
            .client
            .query(&query)
            .fetch_optional::<CoverageReadRow>()
            .await;
        let status = match result {
            Ok(Some(row)) if row.status == 1 => SlotStatus::Covered {
                tx_count: row.tx_count,
            },
            Ok(Some(row)) if row.status == 2 => SlotStatus::Skipped,
            Ok(_) => SlotStatus::NotCovered,
            Err(err) => {
                warn!(slot, "disk cache: slot coverage read failed: {err}");
                SlotStatus::NotCovered
            }
        };
        crate::metrics::disk_cache_read(
            "slot_status",
            match status {
                SlotStatus::Covered { .. } => "hit",
                SlotStatus::Skipped => "skipped",
                SlotStatus::NotCovered => "not_covered",
            },
        );
        status
    }

    pub(crate) async fn get_block(
        &self,
        slot: u64,
        transaction_details: TransactionDetails,
    ) -> DiskBlockResult {
        let tx_count = match self.slot_status(slot).await {
            SlotStatus::Covered { tx_count } => tx_count,
            SlotStatus::Skipped => return DiskBlockResult::Skipped,
            SlotStatus::NotCovered => return DiskBlockResult::NotCovered,
        };
        let metadata = match self
            .inner
            .local
            .get_block_metadata_by_slot(slot, true)
            .await
        {
            Ok((Some(metadata), _)) => metadata,
            Ok((None, _)) => {
                self.poison_slot(slot).await;
                return DiskBlockResult::NotCovered;
            }
            Err(err) => {
                warn!(slot, "disk cache: block metadata read failed: {err}");
                return DiskBlockResult::NotCovered;
            }
        };

        let payload = match transaction_details {
            TransactionDetails::None => StoredBlockPayload::Metadata(metadata),
            TransactionDetails::Signatures => {
                let signatures = match self.inner.local.get_block_signatures_by_slot(slot).await {
                    Ok((records, _)) if records.len() == tx_count as usize => records,
                    Ok((records, _)) => {
                        warn!(
                            slot,
                            expected = tx_count,
                            actual = records.len(),
                            "disk cache: incomplete block signature projection"
                        );
                        self.poison_slot(slot).await;
                        return DiskBlockResult::NotCovered;
                    }
                    Err(err) => {
                        warn!(slot, "disk cache: block signature read failed: {err}");
                        return DiskBlockResult::NotCovered;
                    }
                };
                StoredBlockPayload::Signatures {
                    metadata,
                    signatures,
                }
            }
            TransactionDetails::Accounts => {
                let transactions = match self.inner.local.get_block_accounts_by_slot(slot).await {
                    Ok((records, _)) if records.len() == tx_count as usize => records,
                    Ok((records, _)) => {
                        warn!(
                            slot,
                            expected = tx_count,
                            actual = records.len(),
                            "disk cache: incomplete block accounts projection"
                        );
                        self.poison_slot(slot).await;
                        return DiskBlockResult::NotCovered;
                    }
                    Err(err) => {
                        warn!(slot, "disk cache: block accounts read failed: {err}");
                        return DiskBlockResult::NotCovered;
                    }
                };
                StoredBlockPayload::Accounts {
                    metadata,
                    transactions,
                }
            }
            TransactionDetails::Full => {
                let transactions = match self
                    .inner
                    .local
                    .get_block_full_transactions_by_slot(slot)
                    .await
                {
                    Ok((records, _)) if records.len() == tx_count as usize => records,
                    Ok((records, _)) => {
                        warn!(
                            slot,
                            expected = tx_count,
                            actual = records.len(),
                            "disk cache: incomplete block transaction projection"
                        );
                        self.poison_slot(slot).await;
                        return DiskBlockResult::NotCovered;
                    }
                    Err(err) => {
                        warn!(slot, "disk cache: block transaction read failed: {err}");
                        return DiskBlockResult::NotCovered;
                    }
                };
                StoredBlockPayload::Full(StoredBlockRecord {
                    metadata,
                    transactions,
                })
            }
        };
        crate::metrics::disk_cache_read("get_block", "hit");
        DiskBlockResult::Found(Box::new(payload))
    }

    pub(crate) async fn block_time_for_slot(&self, slot: u64) -> DiskBlockTime {
        match self.slot_status(slot).await {
            SlotStatus::Skipped => return DiskBlockTime::Skipped,
            SlotStatus::NotCovered => return DiskBlockTime::NotCovered,
            SlotStatus::Covered { .. } => {}
        }
        match self.inner.local.get_block_time_by_slot(slot).await {
            Ok((Some(value), _)) => {
                crate::metrics::disk_cache_read("block_time", "hit");
                DiskBlockTime::Found(value)
            }
            Ok((None, _)) => {
                self.poison_slot(slot).await;
                DiskBlockTime::NotCovered
            }
            Err(err) => {
                warn!(slot, "disk cache: block-time read failed: {err}");
                DiskBlockTime::NotCovered
            }
        }
    }

    pub(crate) async fn covered_slots_in_range(&self, start: u64, end: u64) -> Option<Vec<u64>> {
        if !self.ready() || end < start {
            return None;
        }
        let start = start.max(self.min_retained_slot());
        let query = format!(
            "SELECT slot FROM {} FINAL WHERE status = 1 AND slot BETWEEN {start} AND {end} ORDER BY slot",
            schema::COVERAGE_TABLE
        );
        match self
            .inner
            .local
            .client
            .query(&query)
            .fetch_all::<SlotRow>()
            .await
        {
            Ok(rows) => Some(rows.into_iter().map(|row| row.slot).collect()),
            Err(err) => {
                warn!("disk cache: range coverage read failed: {err}");
                None
            }
        }
    }

    pub(crate) async fn get_tx(
        &self,
        signature: solana_sdk::signature::Signature,
    ) -> Option<StoredTransactionRecord> {
        if !self.ready() {
            return None;
        }
        let signature = signature.to_string();
        let record = self
            .inner
            .local
            .get_transaction_by_signature(&signature)
            .await
            .ok()
            .and_then(|(record, _)| record)
            .filter(|record| self.covers_slot(record.slot));
        crate::metrics::disk_cache_read("get_tx", if record.is_some() { "hit" } else { "miss" });
        record
    }

    pub(crate) async fn get_sig_statuses(
        &self,
        signatures: Vec<solana_sdk::signature::Signature>,
    ) -> Vec<Option<DiskSigStatus>> {
        if !self.ready() {
            return vec![None; signatures.len()];
        }
        let encoded: Vec<String> = signatures.iter().map(ToString::to_string).collect();
        let records = match self.query_client().get_signature_statuses(&encoded).await {
            Ok((records, _)) => records,
            Err(err) => {
                warn!("disk cache: signature-status read failed: {err}");
                return vec![None; signatures.len()];
            }
        };
        let by_signature: HashMap<_, _> = records
            .into_iter()
            .filter(|record| self.covers_slot(record.slot))
            .map(|record| {
                let err = record
                    .err
                    .and_then(|value| serde_json::to_string(&value).ok());
                (
                    record.signature,
                    DiskSigStatus {
                        slot: record.slot,
                        err,
                    },
                )
            })
            .collect();
        encoded
            .into_iter()
            .map(|signature| by_signature.get(&signature).cloned())
            .collect()
    }

    pub(crate) async fn signature_position(
        &self,
        signature: solana_sdk::signature::Signature,
    ) -> Option<crate::clickhouse::SignatureSlot> {
        if !self.ready() {
            return None;
        }
        let signature = signature.to_string();
        self.query_client()
            .get_signature_slot(&signature)
            .await
            .ok()
            .and_then(|(position, _)| position)
            .filter(|position| self.covers_slot(position.slot))
    }

    pub(crate) async fn signatures_for_address(
        &self,
        address: solana_sdk::pubkey::Pubkey,
        before: Option<SlotBoundary>,
        until: Option<SlotBoundary>,
        limit: usize,
    ) -> Option<DiskGsfaPage> {
        let (floor, tip) = self.tip_span()?;
        let (until, floor_effective) = clamp_until_to_floor(until, floor);
        let client = self.query_client_for_address(&address, TokenAccountsFilter::None)?;
        let (records, _) = client
            .get_signatures_for_address_with_positions(
                &address.to_string(),
                limit as u64,
                before,
                until,
            )
            .await
            .ok()?;
        let reached_floor = records.len() < limit && floor_effective;
        crate::metrics::disk_cache_read(
            "signatures_for_address",
            if records.is_empty() && !reached_floor {
                "miss"
            } else {
                "hit"
            },
        );
        Some(DiskGsfaPage {
            records,
            reached_floor,
            reached_tip: false,
            floor,
            tip,
        })
    }

    pub(crate) async fn transactions_for_address(
        &self,
        address: solana_sdk::pubkey::Pubkey,
        query: index::DiskTfaQuery,
    ) -> Option<DiskGsfaPage> {
        let (floor, tip) = self.tip_span()?;
        let floor_effective = lower_bound_reaches_floor(&query, floor);
        let tip_effective = upper_bound_reaches_tip(&query, tip);
        let mut slot_filter = query.slot_filter.clone().unwrap_or_default();
        slot_filter.gte = Some(slot_filter.gte.map_or(floor, |value| value.max(floor)));
        slot_filter.lte = Some(slot_filter.lte.map_or(tip, |value| value.min(tip)));
        let clickhouse_query = TransactionsForAddressQuery {
            address: address.to_string(),
            limit: query.limit as u64,
            sort_order: query.sort_order,
            pagination: query.pagination.map(|position| PaginationToken::SlotIndex {
                slot: position.slot,
                idx: position.slot_idx,
            }),
            resolved_pagination: query.pagination,
            slot_filter: Some(slot_filter),
            block_time_filter: query.block_time_filter,
            signature_filter: None,
            resolved_signature_filter: query.signature_filter,
            status: query.status,
            token_accounts: query.token_accounts,
        };
        let client = self.query_client_for_address(&address, query.token_accounts)?;
        let (records, _) = client
            .get_transactions_for_address_signatures(&clickhouse_query)
            .await
            .ok()?;
        let records: Vec<SignatureRecord> = records
            .into_iter()
            .filter(|record| self.covers_slot(record.slot))
            .map(|record| SignatureRecord {
                signature: record.signature,
                slot: record.slot,
                slot_idx: record.slot_idx,
                err: record.err,
                memo: record.memo,
                block_time: record.block_time,
            })
            .collect();
        let reached_floor =
            query.sort_order == SortOrder::Desc && records.len() < query.limit && floor_effective;
        let reached_tip =
            query.sort_order == SortOrder::Asc && records.len() < query.limit && tip_effective;
        Some(DiskGsfaPage {
            records,
            reached_floor,
            reached_tip,
            floor,
            tip,
        })
    }

    pub(crate) async fn get_txs_by_position(
        &self,
        positions: Vec<(u64, u32)>,
    ) -> Vec<Option<StoredTransactionRecord>> {
        let mut by_slot: HashMap<u64, Vec<StoredTransactionRecord>> = HashMap::new();
        let unique_slots: HashSet<u64> = positions.iter().map(|(slot, _)| *slot).collect();
        for slot in unique_slots {
            if !self.covers_slot(slot) {
                continue;
            }
            if let Ok((records, _)) = self
                .inner
                .local
                .get_block_full_transactions_by_slot(slot)
                .await
            {
                by_slot.insert(slot, records);
            }
        }
        positions
            .into_iter()
            .map(|(slot, idx)| {
                by_slot
                    .get(&slot)
                    .and_then(|records| records.iter().find(|record| record.slot_idx == idx))
                    .cloned()
            })
            .collect()
    }

    fn query_client_for_address(
        &self,
        address: &solana_sdk::pubkey::Pubkey,
        token_accounts: TokenAccountsFilter,
    ) -> Option<ClickHouseClient> {
        let mut client = self.query_client();
        if client.is_gsfa_hot_address(address) {
            if token_accounts != TokenAccountsFilter::None
                || !self.source_schema().has_table(CacheTableKind::GsfaHot)
            {
                return None;
            }
            client.gsfa_table = client.gsfa_hot_table.clone();
            client.gsfa_hot_pubkeys.clear();
        }
        Some(client)
    }

    pub(crate) async fn validate_transaction_counts(
        &self,
        start: u64,
        end: u64,
        expected: &HashMap<u64, u64>,
    ) -> Result<(), DiskCacheError> {
        let query = format!(
            "SELECT slot, count() AS count FROM {} FINAL WHERE slot BETWEEN {start} AND {end} GROUP BY slot",
            self.inner.local.transaction_table
        );
        let rows = self
            .inner
            .local
            .client
            .query(&query)
            .fetch_all::<CountRow>()
            .await
            .map_err(|err| DiskCacheError::ClickHouse(err.to_string()))?;
        let actual: HashMap<u64, u64> = rows.into_iter().map(|row| (row.slot, row.count)).collect();
        for (&slot, &expected_count) in expected {
            let actual_count = actual.get(&slot).copied().unwrap_or(0);
            if actual_count != expected_count {
                return Err(DiskCacheError::IncompleteSlot {
                    slot,
                    expected: expected_count,
                    actual: actual_count,
                });
            }
        }
        Ok(())
    }

    pub(crate) async fn publish_range_coverage(
        &self,
        rows: Vec<(u64, SlotStatus)>,
    ) -> Result<(), DiskCacheError> {
        if rows.is_empty() {
            return Ok(());
        }
        let version = now_version();
        let mut insert = self
            .inner
            .local
            .client
            .insert::<CoverageWriteRow>(schema::COVERAGE_TABLE)
            .await
            .map_err(|err| DiskCacheError::ClickHouse(err.to_string()))?;
        let mut published = Vec::new();
        for (slot, status) in rows {
            let (status, tx_count) = match status {
                SlotStatus::Covered { tx_count } => (1, tx_count),
                SlotStatus::Skipped => (2, 0),
                SlotStatus::NotCovered => continue,
            };
            insert
                .write(&CoverageWriteRow {
                    slot,
                    status,
                    tx_count,
                    version,
                })
                .await
                .map_err(|err| DiskCacheError::ClickHouse(err.to_string()))?;
            published.push(slot);
        }
        insert
            .end()
            .await
            .map_err(|err| DiskCacheError::ClickHouse(err.to_string()))?;
        if !published.is_empty() {
            let mut coverage = self.inner.coverage.write().expect("coverage lock");
            for slot in published {
                coverage.insert(slot);
            }
            let floor = coverage.covered_span().map_or(0, |(floor, _)| floor);
            drop(coverage);
            self.inner.min_retained.store(floor, Ordering::Relaxed);
            self.publish_coverage_metrics();
        }
        Ok(())
    }

    async fn poison_slot(&self, slot: u64) {
        self.inner
            .coverage
            .write()
            .expect("coverage lock")
            .remove(slot);
        let row = CoverageWriteRow {
            slot,
            status: 0,
            tx_count: 0,
            version: now_version(),
        };
        match self
            .inner
            .local
            .client
            .insert::<CoverageWriteRow>(schema::COVERAGE_TABLE)
            .await
        {
            Ok(mut insert) => {
                if insert.write(&row).await.is_ok() {
                    let _ = insert.end().await;
                }
            }
            Err(err) => warn!(slot, "disk cache: failed to persist poisoned slot: {err}"),
        }
        crate::metrics::disk_cache_poisoned_slot();
        self.publish_coverage_metrics();
    }

    pub(crate) async fn maybe_evict(&self) -> Result<bool, DiskCacheError> {
        let Some((_, head)) = self
            .inner
            .coverage
            .read()
            .expect("coverage lock")
            .covered_span()
        else {
            return Ok(false);
        };
        let old_floor = self.min_retained_slot();
        let effective_retain_slots = self
            .inner
            .cfg
            .memory_retain_slots
            .filter(|_| self.inner.cfg.memory_blocks_metadata)
            .map_or(self.inner.cfg.retain_slots, |memory| {
                memory.min(self.inner.cfg.retain_slots)
            });
        let mut new_floor = head.saturating_sub(effective_retain_slots.saturating_sub(1));
        let mut byte_budget_bound = false;
        if self.inner.cfg.max_bytes > 0 {
            let bytes = self.cache_bytes().await?;
            crate::metrics::disk_cache_size_bytes(bytes);
            if bytes >= self.inner.cfg.max_bytes {
                byte_budget_bound = true;
                let target = self
                    .inner
                    .cfg
                    .max_bytes
                    .saturating_mul(BYTE_BUDGET_LOW_WATER_PERCENT)
                    / 100;
                let covered = head.saturating_sub(old_floor).saturating_add(1).max(1);
                let bytes_per_slot = (bytes / covered).max(1);
                let keep = (target / bytes_per_slot).max(1);
                new_floor = new_floor.max(head.saturating_sub(keep.saturating_sub(1)));
            }
        }
        if new_floor <= old_floor {
            return Ok(false);
        }

        self.inner.min_retained.store(new_floor, Ordering::Relaxed);
        self.inner
            .coverage
            .write()
            .expect("coverage lock")
            .remove_below(new_floor);
        self.drop_partitions_below(new_floor).await?;
        crate::metrics::disk_cache_evicted(
            if byte_budget_bound { "bytes" } else { "window" },
            new_floor - old_floor,
        );
        self.publish_coverage_metrics();
        Ok(true)
    }

    async fn cache_bytes(&self) -> Result<u64, DiskCacheError> {
        let database = self.inner.cfg.database.replace('\'', "''");
        let row = self
            .inner
            .local
            .client
            .query(&format!(
                "SELECT toUInt64(coalesce(sum(bytes_on_disk), 0)) AS bytes FROM system.parts WHERE active AND database = '{database}'"
            ))
            .fetch_one::<BytesRow>()
            .await
            .map_err(|err| DiskCacheError::ClickHouse(err.to_string()))?;
        Ok(row.bytes)
    }

    async fn drop_partitions_below(&self, floor: u64) -> Result<(), DiskCacheError> {
        let floor_partition = floor / self.inner.cfg.partition_slots;
        let database = self.inner.cfg.database.replace('\'', "''");
        let tables = self.source_schema().tables.clone();
        for table in tables {
            if table.kind == CacheTableKind::BlocksMetadata && self.inner.cfg.memory_blocks_metadata
            {
                continue;
            }
            self.drop_table_partitions(&database, table.kind.local_name(), floor_partition)
                .await?;
        }
        self.drop_table_partitions(&database, schema::COVERAGE_TABLE, floor_partition)
            .await
    }

    async fn drop_table_partitions(
        &self,
        database: &str,
        table: &str,
        floor_partition: u64,
    ) -> Result<(), DiskCacheError> {
        let table_literal = table.replace('\'', "''");
        let query = format!(
            "SELECT DISTINCT partition_id FROM system.parts WHERE active AND database = '{database}' \
             AND table = '{table_literal}'"
        );
        let partitions = self
            .inner
            .local
            .client
            .query(&query)
            .fetch_all::<PartitionRow>()
            .await
            .map_err(|err| DiskCacheError::ClickHouse(err.to_string()))?;
        for partition in partitions {
            let Some(value) = partition.partition_id.parse::<u64>().ok() else {
                continue;
            };
            if value >= floor_partition {
                continue;
            }
            let partition_id = partition.partition_id.replace('\'', "''");
            let sql = format!(
                "ALTER TABLE `{}`.`{}` DROP PARTITION ID '{partition_id}'",
                self.inner.cfg.database, table
            );
            self.inner
                .local
                .client
                .query(&sql)
                .execute()
                .await
                .map_err(|err| DiskCacheError::ClickHouse(err.to_string()))?;
        }
        Ok(())
    }
}

fn validate_config(cfg: &DiskCacheConfig) -> Result<(), DiskCacheError> {
    if cfg.retain_slots == 0 {
        return Err(DiskCacheError::Config(
            "DISK_CACHE_RETAIN_SLOTS must be nonzero".to_string(),
        ));
    }
    if cfg.partition_slots == 0 {
        return Err(DiskCacheError::Config(
            "DISK_CACHE_PARTITION_SLOTS must be nonzero".to_string(),
        ));
    }
    if cfg.memory_blocks_metadata {
        let retain = cfg.memory_retain_slots.ok_or_else(|| {
            DiskCacheError::Config(
                "Memory blocks_metadata requires DISK_CACHE_MEMORY_RETAIN_SLOTS".to_string(),
            )
        })?;
        if retain > cfg.retain_slots {
            return Err(DiskCacheError::Config(
                "DISK_CACHE_MEMORY_RETAIN_SLOTS cannot exceed DISK_CACHE_RETAIN_SLOTS".to_string(),
            ));
        }
        if cfg.memory_max_bytes.is_none() {
            return Err(DiskCacheError::Config(
                "Memory blocks_metadata requires DISK_CACHE_MEMORY_MAX_BYTES".to_string(),
            ));
        }
    }
    Ok(())
}

fn now_version() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u128::from(u64::MAX)) as u64
}

fn clamp_until_to_floor(until: Option<SlotBoundary>, floor: u64) -> (Option<SlotBoundary>, bool) {
    if floor == 0 {
        return (until, false);
    }
    let requested_floor = match until {
        Some(SlotBoundary::Position(position)) => position.slot.saturating_add(1),
        Some(SlotBoundary::Slot(slot)) => slot.saturating_add(1),
        None => 0,
    };
    if requested_floor >= floor {
        (until, false)
    } else {
        (Some(SlotBoundary::Slot(floor - 1)), true)
    }
}

fn lower_bound_reaches_floor(query: &index::DiskTfaQuery, floor: u64) -> bool {
    let mut lower: Option<u64> = None;
    if let Some(filter) = query.slot_filter.as_ref() {
        for value in [filter.gte, filter.gt.map(|value| value.saturating_add(1))]
            .into_iter()
            .flatten()
        {
            lower = Some(lower.map_or(value, |current| current.max(value)));
        }
    }
    if let Some(filter) = query.signature_filter.as_ref() {
        for position in [filter.gte, filter.gt].into_iter().flatten() {
            lower = Some(lower.map_or(position.slot, |current| current.max(position.slot)));
        }
    }
    lower.is_none_or(|value| value <= floor)
}

fn upper_bound_reaches_tip(query: &index::DiskTfaQuery, tip: u64) -> bool {
    let mut upper: Option<u64> = None;
    if let Some(filter) = query.slot_filter.as_ref() {
        for value in [filter.lte, filter.lt.map(|value| value.saturating_sub(1))]
            .into_iter()
            .flatten()
        {
            upper = Some(upper.map_or(value, |current| current.min(value)));
        }
    }
    if let Some(filter) = query.signature_filter.as_ref() {
        for position in [filter.lte, filter.lt].into_iter().flatten() {
            upper = Some(upper.map_or(position.slot, |current| current.min(position.slot)));
        }
    }
    upper.is_none_or(|value| value > tip)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn automatic_partition_width_is_bounded() {
        assert_eq!(automatic_partition_slots(1), 10_000);
        assert_eq!(automatic_partition_slots(432_000), 10_000);
        assert_eq!(automatic_partition_slots(4_320_000), 34_000);
        assert_eq!(automatic_partition_slots(u64::MAX), 432_000);
    }

    #[test]
    fn floor_clamps_unbounded_signature_scan() {
        assert_eq!(
            clamp_until_to_floor(None, 100),
            (Some(SlotBoundary::Slot(99)), true)
        );
        assert_eq!(
            clamp_until_to_floor(Some(SlotBoundary::Slot(150)), 100),
            (Some(SlotBoundary::Slot(150)), false)
        );
    }

    fn tfa_query(sort_order: SortOrder) -> index::DiskTfaQuery {
        index::DiskTfaQuery {
            limit: 100,
            sort_order,
            pagination: None,
            slot_filter: None,
            block_time_filter: None,
            signature_filter: None,
            status: crate::clickhouse::TransactionStatusFilter::Any,
            token_accounts: TokenAccountsFilter::None,
        }
    }

    #[test]
    fn tfa_bounds_identify_source_remainders_outside_cache_span() {
        let mut descending = tfa_query(SortOrder::Desc);
        assert!(lower_bound_reaches_floor(&descending, 100));
        descending.slot_filter = Some(crate::clickhouse::NumericFilter {
            gte: Some(101),
            ..Default::default()
        });
        assert!(!lower_bound_reaches_floor(&descending, 100));

        let mut ascending = tfa_query(SortOrder::Asc);
        assert!(upper_bound_reaches_tip(&ascending, 200));
        ascending.slot_filter = Some(crate::clickhouse::NumericFilter {
            lte: Some(200),
            ..Default::default()
        });
        assert!(!upper_bound_reaches_tip(&ascending, 200));
        ascending.slot_filter = Some(crate::clickhouse::NumericFilter {
            lte: Some(201),
            ..Default::default()
        });
        assert!(upper_bound_reaches_tip(&ascending, 200));
    }
}
