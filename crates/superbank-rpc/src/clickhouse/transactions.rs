// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::cmp::Ordering;
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use crate::solana_sdk::pubkey::Pubkey;
use ch_cityhash102::cityhash64;
use serde::Deserialize;
use tokio::task::JoinSet;

use crate::processing::{ProcessingError, ProcessingResult};

use super::QueryFreshnessClass;
use super::cache::{CacheStart, SignatureBytes};
use super::client::{ClickHouseClient, execute_shard_tcp_query_block};
use super::constants::SLOT_SHARD_DIVISOR;
use super::queries::{
    TRANSACTION_SELECT_COLUMNS, TransactionsForAddressTables,
    build_transactions_by_slot_signatures_query, build_transactions_for_address_hot_query,
    build_transactions_for_address_query,
};
use super::read_query::admission;
use super::rows::{TransactionRow, fetch_single_transaction_row, map_transaction_row};
use super::sharding::ShardTopology;
use super::types::{
    PaginationToken, QueryTimings, SignatureSlot, SortOrder, StoredTransactionRecord,
    TokenAccountsFilter, TransactionsForAddressQuery, TransactionsForAddressRecord,
};
use super::util::{
    append_max_execution_time_setting, format_gsfa_memo, parse_err_json,
    transient_shard_local_error_reason,
};

// Keep the signature predicate even on the exact path: an index hint alone is
// not identity, particularly while repairing historical rows.
fn transaction_lookup_key(
    slot: u64,
    slot_idx: Option<u32>,
    signature: &str,
) -> ProcessingResult<(u64, Option<u32>, String)> {
    let (_, literal) = decode_transaction_signature(signature)?;
    Ok((slot, slot_idx, literal))
}

fn unresolved_transaction_pairs(
    pairs: &[(u64, Option<u32>, String)],
    records: &[StoredTransactionRecord],
) -> Vec<(u64, Option<u32>, String)> {
    let found: HashSet<_> = records
        .iter()
        .map(|record| {
            (
                record.slot,
                format!(
                    "toFixedString(unhex('{}'), 64)",
                    hex::encode_upper(record.signature)
                ),
            )
        })
        .collect();
    pairs
        .iter()
        .filter(|(slot, _, literal)| !found.contains(&(*slot, literal.clone())))
        .map(|(slot, _, literal)| (*slot, None, literal.clone()))
        .collect()
}

#[derive(Deserialize, clickhouse::Row)]
struct TransactionsForAddressQueryRow {
    signature: String,
    slot: u64,
    slot_idx: u32,
    err: Option<String>,
    memo: Option<String>,
    block_time: Option<i64>,
}

fn map_transactions_for_address_row(
    row: TransactionsForAddressQueryRow,
) -> TransactionsForAddressRecord {
    let parsed_err = row
        .err
        .and_then(|err_str| parse_err_json(&row.signature, err_str));
    TransactionsForAddressRecord {
        signature: row.signature,
        slot: row.slot,
        slot_idx: row.slot_idx,
        err: parsed_err,
        memo: format_gsfa_memo(row.memo),
        block_time: row.block_time,
    }
}

fn compare_transactions_for_address_records(
    sort_order: SortOrder,
    a: &TransactionsForAddressRecord,
    b: &TransactionsForAddressRecord,
) -> Ordering {
    match sort_order {
        SortOrder::Desc => b
            .slot
            .cmp(&a.slot)
            .then_with(|| b.slot_idx.cmp(&a.slot_idx))
            .then_with(|| b.signature.cmp(&a.signature)),
        SortOrder::Asc => a
            .slot
            .cmp(&b.slot)
            .then_with(|| a.slot_idx.cmp(&b.slot_idx))
            .then_with(|| a.signature.cmp(&b.signature)),
    }
}

const TRANSACTIONS_FOR_ADDRESS_MIN_BATCH_SIZE: u64 = 64;
const TRANSACTIONS_FOR_ADDRESS_MAX_BATCH_SIZE: u64 = 2_000;
/// One entry per served page (~150 B); matches the signature-slot cache's bound and TTL.
const TRANSACTIONS_FOR_ADDRESS_CURSOR_CAPACITY: u64 = 50_000;
const TRANSACTIONS_FOR_ADDRESS_CURSOR_TTL: std::time::Duration =
    std::time::Duration::from_secs(6 * 60 * 60);

fn transactions_for_address_batch_size(remaining: u64) -> u64 {
    remaining.saturating_mul(2).clamp(
        TRANSACTIONS_FOR_ADDRESS_MIN_BATCH_SIZE,
        TRANSACTIONS_FOR_ADDRESS_MAX_BATCH_SIZE,
    )
}

fn order_transactions_for_address_records(
    mut records: Vec<TransactionsForAddressRecord>,
    sort_order: SortOrder,
    limit: u64,
) -> Vec<TransactionsForAddressRecord> {
    records.sort_unstable_by(|a, b| compare_transactions_for_address_records(sort_order, a, b));
    records.truncate(limit as usize);
    records
}

fn append_unique_transactions_for_address_records(
    output: &mut Vec<TransactionsForAddressRecord>,
    seen: &mut HashSet<String>,
    batch: Vec<TransactionsForAddressRecord>,
    limit: usize,
) {
    for record in batch {
        if seen.insert(record.signature.clone()) {
            output.push(record);
            if output.len() >= limit {
                break;
            }
        }
    }
}

fn decode_transaction_signature(signature: &str) -> ProcessingResult<([u8; 64], String)> {
    let signature_bytes = bs58::decode(signature)
        .into_vec()
        .map_err(|e| ProcessingError::deserialization("Invalid signature", e))?;

    if signature_bytes.len() != 64 {
        return Err(ProcessingError::deserialization_msg(format!(
            "Invalid signature length {} (expected 64 bytes)",
            signature_bytes.len()
        )));
    }

    let signature_literal = super::owner_shard::signature_literal(&signature_bytes);
    let signature_bytes = signature_bytes.as_slice().try_into().map_err(|_| {
        ProcessingError::deserialization_msg("Invalid signature length".to_string())
    })?;

    Ok((signature_bytes, signature_literal))
}

fn build_get_transaction_by_signature_query(
    transaction_table: &str,
    signature_literal: &str,
    slot: u64,
    slot_idx: Option<u32>,
    settings_clause: &str,
) -> String {
    match slot_idx {
        Some(slot_idx) => {
            format!(
                "SELECT
                    {columns}
                 FROM {transaction_table}
                 PREWHERE slot = {slot} AND slot_idx = {slot_idx} AND signature = {signature_literal}
                 LIMIT 1
                 {settings_clause}",
                transaction_table = transaction_table,
                signature_literal = signature_literal,
                slot = slot,
                slot_idx = slot_idx,
                settings_clause = settings_clause,
                columns = TRANSACTION_SELECT_COLUMNS
            )
        }
        None => {
            format!(
                "SELECT
                    {columns}
                 FROM {transaction_table}
                 PREWHERE slot = {slot} AND signature = {signature_literal}
                 ORDER BY slot_idx DESC
                 LIMIT 1
                 {settings_clause}",
                transaction_table = transaction_table,
                signature_literal = signature_literal,
                slot = slot,
                settings_clause = settings_clause,
                columns = TRANSACTION_SELECT_COLUMNS
            )
        }
    }
}

const SINGLE_ROUND_TRIP_OPERATION: &str = "get_transaction_single_rt";

/// Position the single-round-trip scalar yields for an unknown signature. No transaction lives at
/// this slot, and the outer `WHERE` rejects it before any shard is read.
const SINGLE_ROUND_TRIP_MISSING_SLOT: u64 = u64::MAX;

/// Resolves the latest `(slot, slot_idx)` for a signature and reads its payload in one query.
///
/// The scalar is an aggregate without `GROUP BY`, so it returns exactly one row even for an
/// unknown signature (an empty non-Nullable scalar is error 125); `max((slot, slot_idx))` matches
/// the two-query path's `ORDER BY slot DESC, slot_idx DESC LIMIT 1`. With the analyzer
/// (`enable_analyzer=1`; verified locally on 26.8.11.7) ClickHouse folds the scalar to a constant
/// before `optimize_skip_unused_shards`, so the outer read goes to the one shard owning the epoch,
/// and not at all for the sentinel.
///
/// The outer read keys on the full `(slot, slot_idx, signature)` so it selects one granule like
/// the exact two-query read. It has no legacy fallback: a payload row whose `slot_idx` differs
/// from its `signatures` row is not found, where the two-query path would retry without
/// `slot_idx`.
fn build_get_transaction_single_round_trip_query(
    transaction_table: &str,
    signatures_table: &str,
    sig_bucket: u64,
    signature_literal: &str,
    settings_clause: &str,
) -> String {
    format!(
        "WITH (
            SELECT if(count() = 0, (toUInt64({missing_slot}), toUInt32(0)), max((slot, slot_idx)))
            FROM {signatures_table}
            PREWHERE sig_bucket = {sig_bucket} AND signature = {signature_literal}
         ) AS pos
         SELECT
            {columns}
         FROM {transaction_table}
         PREWHERE slot = tupleElement(pos, 1)
            AND slot_idx = tupleElement(pos, 2)
            AND signature = {signature_literal}
         WHERE tupleElement(pos, 1) != {missing_slot}
         LIMIT 1
         {settings_clause}",
        missing_slot = SINGLE_ROUND_TRIP_MISSING_SLOT,
        columns = TRANSACTION_SELECT_COLUMNS
    )
}

impl ClickHouseClient {
    /// Enables the one-query primary getTransaction fallback
    /// (`CLICKHOUSE_GET_TRANSACTION_SINGLE_ROUND_TRIP`).
    pub(crate) fn set_get_transaction_single_round_trip(&mut self, enabled: bool) {
        self.get_transaction_single_round_trip = enabled;
    }

    pub(crate) fn set_transactions_for_address_union_pushdown(&mut self, enabled: bool) {
        self.transactions_for_address_union_pushdown = enabled;
    }

    pub(crate) fn set_transactions_for_address_position_tokens(&mut self, enabled: bool) {
        self.transactions_for_address_position_tokens = enabled;
    }

    pub(crate) fn transactions_for_address_position_tokens(&self) -> bool {
        self.transactions_for_address_position_tokens
    }

    pub(crate) fn set_transactions_for_address_cursor_cache(&mut self, enabled: bool) {
        self.transactions_for_address_cursors = enabled.then(|| {
            moka::future::Cache::builder()
                .max_capacity(TRANSACTIONS_FOR_ADDRESS_CURSOR_CAPACITY)
                .time_to_live(TRANSACTIONS_FOR_ADDRESS_CURSOR_TTL)
                .build()
        });
    }

    /// Position of a previously served ClickHouse page's last row, if remembered.
    pub(crate) async fn transactions_for_address_cursor(
        &self,
        signature: &str,
    ) -> Option<SignatureSlot> {
        let position = self
            .transactions_for_address_cursors
            .as_ref()?
            .get(signature)
            .await;
        crate::metrics::transactions_for_address_cursor_cache_access(if position.is_some() {
            "hit"
        } else {
            "miss"
        });
        position
    }

    pub(crate) async fn remember_transactions_for_address_cursor(
        &self,
        signature: &str,
        position: SignatureSlot,
    ) {
        if let Some(cursors) = &self.transactions_for_address_cursors {
            cursors.insert(signature.to_owned(), position).await;
            crate::metrics::transactions_for_address_cursor_cache_access("insert");
        }
    }

    // Shard-direct keeps its local-table routing, and local-cache clients never use the
    // signature-slot cache, so both stay on the two-query path.
    fn get_transaction_single_round_trip_enabled(&self) -> bool {
        self.get_transaction_single_round_trip
            && self.cache_partition.is_none()
            && !self.scope_shard_direct()
    }

    // Each fused query is unique per signature, so the query cache would only take writes.
    fn single_round_trip_settings_clause(&self) -> String {
        crate::metrics::clickhouse_query_cache_classified(SINGLE_ROUND_TRIP_OPERATION, false);
        if !self.allow_query_settings {
            return String::new();
        }
        let settings = append_max_execution_time_setting(
            "SETTINGS optimize_skip_unused_shards=1, use_query_cache=0",
            self.query_timeout,
        );
        if self.signatures_owner_shard_routed() {
            super::owner_shard::with_owner_shard_settings(&settings)
        } else {
            settings
        }
    }

    pub async fn get_transactions_for_address_signatures(
        &self,
        query: &TransactionsForAddressQuery,
    ) -> ProcessingResult<(Vec<TransactionsForAddressRecord>, QueryTimings)> {
        self.with_http_query_timeout("get_transactions_for_address_signatures", async {
            let pubkey = Pubkey::from_str(&query.address)
                .map_err(|e| ProcessingError::deserialization("Invalid address", e))?;

            if query.token_accounts != TokenAccountsFilter::None
                && !self.token_owner_activity_available
            {
                return Err(ProcessingError::database_msg(format!(
                    "tokenAccounts filters require token owner activity table '{}'",
                    self.token_owner_activity_table
                )));
            }

            let requested_limit = query.limit;
            let mut page_query = query.clone();
            let mut seen = HashSet::with_capacity(requested_limit as usize);
            let mut records = Vec::with_capacity(requested_limit as usize);
            let mut timings = QueryTimings::zero();

            while records.len() < requested_limit as usize {
                let remaining = requested_limit.saturating_sub(records.len() as u64);
                let batch_limit = transactions_for_address_batch_size(remaining);
                page_query.limit = batch_limit;

                let (mut batch, batch_timings) = self
                    .get_transactions_for_address_signatures_batch(&page_query, &pubkey)
                    .await?;
                timings.add(batch_timings);

                batch.sort_unstable_by(|a, b| {
                    compare_transactions_for_address_records(query.sort_order, a, b)
                });
                let raw_count = batch.len() as u64;
                let continuation = batch.last().map(|record| SignatureSlot {
                    slot: record.slot,
                    slot_idx: record.slot_idx,
                });

                append_unique_transactions_for_address_records(
                    &mut records,
                    &mut seen,
                    batch,
                    requested_limit as usize,
                );

                if records.len() >= requested_limit as usize || raw_count < batch_limit {
                    break;
                }

                let Some(continuation) = continuation else {
                    break;
                };
                if page_query.resolved_pagination == Some(continuation) {
                    break;
                }
                page_query.pagination = Some(PaginationToken::SlotIndex {
                    slot: continuation.slot,
                    idx: continuation.slot_idx,
                });
                page_query.resolved_pagination = Some(continuation);
            }

            timings.rows_returned = records.len() as u64;
            Ok((records, timings))
        })
        .await
    }

    async fn get_transactions_for_address_signatures_batch(
        &self,
        query: &TransactionsForAddressQuery,
        pubkey: &Pubkey,
    ) -> ProcessingResult<(Vec<TransactionsForAddressRecord>, QueryTimings)> {
        if self.should_use_gsfa_hot_fanout(pubkey)
            && query.token_accounts == TokenAccountsFilter::None
        {
            return self
                .get_hot_transactions_for_address_signatures(query, pubkey)
                .await;
        }

        if self.should_use_gsfa_shard_routing(pubkey) && self.shard_topology.is_some() {
            let router = self.gsfa_router.as_ref().ok_or_else(|| {
                ProcessingError::database_msg(
                    "Shard topology is configured but GSFA owner-shard routing is unavailable",
                )
            })?;
            let token_owner_local_table = if query.token_accounts != TokenAccountsFilter::None {
                Some(
                    self.token_owner_activity_local_table
                        .as_deref()
                        .ok_or_else(|| {
                            ProcessingError::database_msg(
                                "Shard topology is configured but token-owner shard routing is unavailable",
                            )
                        })?,
                )
            } else {
                Some(self.token_owner_activity_table.as_str())
            };

            let mut allow_local_http = self.transport_http();

            if self.transport_tcp() {
                match self
                    .try_get_transactions_for_address_signatures_tcp(
                        router,
                        token_owner_local_table,
                        query,
                        pubkey,
                    )
                    .await?
                {
                    Some(result) => return Ok(result),
                    None => allow_local_http = true,
                }
            }

            if allow_local_http {
                return self
                    .try_get_transactions_for_address_signatures_http(
                        router,
                        token_owner_local_table,
                        query,
                        pubkey,
                    )
                    .await?
                    .ok_or_else(|| {
                        ProcessingError::database_msg(
                            "Shard-local getTransactionsForAddress query was unavailable",
                        )
                    });
            }

            return Err(ProcessingError::database_msg(
                "No shard-local transport is available for getTransactionsForAddress",
            ));
        }

        let settings_clause = self.select_settings_clause_with_condition_cache(
            "get_transactions_for_address_signatures",
            QueryFreshnessClass::Historical,
        );
        let gsfa_table = self.gsfa_table_for_address(pubkey);
        let gsfa_bucket_modulus = self.gsfa_bucket_modulus_for_address(pubkey);
        let tables = TransactionsForAddressTables {
            cache_partition: self.cache_partition,
            gsfa_table,
            gsfa_bucket_modulus,
            token_owner_table: &self.token_owner_activity_table,
            token_owner_bucket_modulus: self.token_owner_bucket_modulus(),
            signatures_table: &self.signature_statuses_table,
            signature_bucket_modulus: self.signatures_bucket_modulus(),
            union_pushdown: self.transactions_for_address_union_pushdown,
        };
        let query = build_transactions_for_address_query(&tables, query, &settings_clause)?;

        let start = Instant::now();
        let mut cursor = self
            .read::<TransactionsForAddressQueryRow>(&query, "cache_address_transactions")
            .await
            .map_err(|e| ProcessingError::database(e.to_string(), e))?;

        let mut results = Vec::new();
        while let Some(row) = cursor
            .next()
            .await
            .map_err(|e| ProcessingError::database(e.to_string(), e))?
        {
            results.push(row);
        }

        let records = results
            .into_iter()
            .map(map_transactions_for_address_row)
            .collect::<Vec<_>>();

        let timings = QueryTimings {
            elapsed_ms: start.elapsed().as_millis() as u64,
            received_bytes: cursor.received_bytes(),
            decoded_bytes: cursor.decoded_bytes(),
            rows_read: Some(0),
            rows_read_unknown: true,
            rows_returned: records.len() as u64,
        };

        Ok((records, timings))
    }

    async fn get_hot_transactions_for_address_signatures(
        &self,
        query: &TransactionsForAddressQuery,
        pubkey: &Pubkey,
    ) -> ProcessingResult<(Vec<TransactionsForAddressRecord>, QueryTimings)> {
        let topology = self.hot_shard_topology()?.clone();

        if self.scope_shard_direct() && self.transport_tcp() {
            match self
                .get_hot_transactions_for_address_signatures_tcp(&topology, query, pubkey)
                .await
            {
                Ok(result) => Ok(result),
                Err(err) => {
                    if let Some(reason) = transient_shard_local_error_reason(&err) {
                        crate::metrics::clickhouse_transport_fallback(
                            "get_transactions_for_address_hot_local_tcp",
                            "tcp",
                            "http",
                            reason,
                        );
                        tracing::warn!(
                            "Shard-local getTransactionsForAddress hot TCP query failed; falling back to HTTP: {}",
                            err
                        );
                        self.get_hot_transactions_for_address_signatures_http(
                            &topology, query, pubkey,
                        )
                        .await
                    } else {
                        Err(err)
                    }
                }
            }
        } else {
            self.get_hot_transactions_for_address_signatures_http(&topology, query, pubkey)
                .await
        }
    }

    /// Hydrate exact positions, retrying historical index mismatches by identity.
    /// Both passes and every bounded batch share the outer query timeout.
    /// Return every version; the RPC encoder must report unsupported versions.
    pub async fn get_transactions_by_positions(
        &self,
        positions: &[(u64, u32, String)],
    ) -> ProcessingResult<(Vec<StoredTransactionRecord>, QueryTimings)> {
        let mut seen = HashSet::new();
        let pairs = positions
            .iter()
            .filter(|(slot, _, signature)| seen.insert((*slot, signature.as_str())))
            .map(|(slot, idx, signature)| transaction_lookup_key(*slot, Some(*idx), signature))
            .collect::<ProcessingResult<Vec<_>>>()?;
        self.with_http_query_timeout("get_transactions_by_positions", async {
            let mut records = Vec::with_capacity(pairs.len());
            let mut timings = QueryTimings::zero();
            // A full address page is normally <=100 rows. Keep larger internal
            // callers bounded too, independent of how many slots they span.
            for chunk in pairs.chunks(100) {
                let (mut batch, exact_timings) = self.fetch_transaction_pairs(chunk).await?;
                timings.add(exact_timings);
                let unresolved = unresolved_transaction_pairs(chunk, &batch);
                if !unresolved.is_empty() {
                    let (fallback, fallback_timings) =
                        self.fetch_transaction_pairs(&unresolved).await?;
                    batch.extend(fallback);
                    timings.add(fallback_timings);
                }
                records.extend(batch);
            }
            let mut seen = HashSet::new();
            records.retain(|record| seen.insert((record.slot, record.signature)));
            Ok((records, timings))
        })
        .await
    }

    async fn fetch_transaction_pairs(
        &self,
        pairs: &[(u64, Option<u32>, String)],
    ) -> ProcessingResult<(Vec<StoredTransactionRecord>, QueryTimings)> {
        if pairs.is_empty() {
            return Ok((Vec::new(), QueryTimings::zero()));
        }
        if self.scope_shard_direct()
            && self.transport_http()
            && let (Some(topology), Some(local_table)) =
                (&self.shard_topology, &self.transactions_local_table)
            && let Some(result) = self
                .try_get_transactions_by_slot_signatures_local(topology, local_table, pairs)
                .await?
        {
            return Ok(result);
        }

        let settings_clause = self.select_settings_clause(
            "get_transactions_by_slot_signatures",
            QueryFreshnessClass::Historical,
        );
        let query = build_transactions_by_slot_signatures_query(
            &self.transaction_table,
            pairs,
            &settings_clause,
            self.in_clause_chunk,
        );

        let start = Instant::now();
        let mut cursor = self
            .read::<TransactionRow>(&query, "get_transactions_by_slot_signatures")
            .await
            .map_err(|e| ProcessingError::database(e.to_string(), e))?;

        let mut records = Vec::new();
        while let Some(row) = cursor
            .next()
            .await
            .map_err(|e| ProcessingError::database(e.to_string(), e))?
        {
            records.push(map_transaction_row(row));
        }

        let timings = QueryTimings {
            elapsed_ms: start.elapsed().as_millis() as u64,
            received_bytes: cursor.received_bytes(),
            decoded_bytes: cursor.decoded_bytes(),
            rows_read: Some(0),
            rows_read_unknown: true,
            rows_returned: records.len() as u64,
        };

        Ok((records, timings))
    }

    async fn fetch_transaction_lookup(
        &self,
        query: &str,
    ) -> ProcessingResult<(Option<TransactionRow>, QueryTimings)> {
        fetch_single_transaction_row(&self.client, &self.read_endpoint, query).await
    }

    pub async fn get_transaction_by_signature(
        &self,
        signature: &str,
    ) -> ProcessingResult<(Option<StoredTransactionRecord>, QueryTimings)> {
        self.with_operation_timeout("get_transaction_by_signature", async {
            let (signature_bytes, signature_literal) = decode_transaction_signature(signature)?;
            if self.get_transaction_single_round_trip_enabled() {
                return self
                    .get_transaction_single_round_trip(signature_bytes, &signature_literal)
                    .await;
            }
            let (slot_opt, mut timings) = self
                .get_signature_slot_by_signature_bytes(signature_bytes)
                .await?;

            let Some(position) = slot_opt else {
                return Ok((None, timings));
            };
            let _http_permit = self.acquire_http_query_permit().await?;
            let (record, payload_timings) = self
                .fetch_transaction_at_position(&signature_literal, position)
                .await?;
            timings.add(payload_timings);
            Ok((record, timings))
        })
        .await
    }

    /// [`Self::get_transaction_by_signature`] with one query per signature-slot cache miss.
    ///
    /// The signature-slot cache and its singleflight are kept: a cached position reads the
    /// payload only, a cached miss returns `None`, and the leader runs the fused query under one
    /// HTTP permit. The leader caches the returned row's position, or a miss when no row comes
    /// back. A signature with a `signatures` row but no payload row at that exact position
    /// therefore caches a miss (1 s TTL by default) instead of its position, and the address
    /// history `before`/`until` cursors see that miss too. getTransaction returns `null` for it,
    /// including a payload at another `slot_idx` that the two-query legacy fallback would find.
    async fn get_transaction_single_round_trip(
        &self,
        signature_bytes: SignatureBytes,
        signature_literal: &str,
    ) -> ProcessingResult<(Option<StoredTransactionRecord>, QueryTimings)> {
        let call_start = Instant::now();
        let mut waited = false;

        loop {
            match self
                .signature_slot_cache
                .get_or_start(signature_bytes)
                .await
            {
                CacheStart::Hit(value) => {
                    let mut timings = if waited {
                        QueryTimings {
                            elapsed_ms: call_start.elapsed().as_millis() as u64,
                            received_bytes: 0,
                            decoded_bytes: 0,
                            rows_read: Some(0),
                            rows_read_unknown: true,
                            rows_returned: 0,
                        }
                    } else {
                        QueryTimings::zero()
                    };
                    let Some(position) = value else {
                        return Ok((None, timings));
                    };
                    let _http_permit = self.acquire_http_query_permit().await?;
                    let (record, payload_timings) = self
                        .fetch_transaction_at_position(signature_literal, position)
                        .await?;
                    timings.add(payload_timings);
                    return Ok((record, timings));
                }
                CacheStart::Wait(wait) => {
                    waited = true;
                    wait.await;
                }
                CacheStart::Leader(leader) => {
                    let result = async {
                        let _http_permit = self.acquire_http_query_permit().await?;
                        self.fetch_transaction_single_round_trip(signature_bytes, signature_literal)
                            .await
                    }
                    .await;

                    match result {
                        Ok((row_opt, timings)) => {
                            let position = row_opt.as_ref().map(|row| SignatureSlot {
                                slot: row.slot,
                                slot_idx: row.slot_idx,
                            });
                            leader.finish(position).await;
                            return Ok((row_opt.map(map_transaction_row), timings));
                        }
                        Err(err) => {
                            leader.fail().await;
                            return Err(err);
                        }
                    }
                }
            }
        }
    }

    // The caller owns HTTP admission and the operation deadline.
    async fn fetch_transaction_single_round_trip(
        &self,
        signature_bytes: SignatureBytes,
        signature_literal: &str,
    ) -> ProcessingResult<(Option<TransactionRow>, QueryTimings)> {
        let sig_bucket = cityhash64(signature_bytes.as_ref()) % self.signatures_bucket_modulus();
        let query = build_get_transaction_single_round_trip_query(
            &self.transaction_table,
            self.signature_lookup_source(),
            sig_bucket,
            signature_literal,
            &self.single_round_trip_settings_clause(),
        );
        let start = Instant::now();
        let mut cursor = self
            .read_endpoint
            .fetch::<TransactionRow>(&self.client, &query, SINGLE_ROUND_TRIP_OPERATION)
            .await
            .map_err(|e| ProcessingError::database(e.to_string(), e))?;
        let row_opt = cursor
            .next_optional()
            .await
            .map_err(|e| ProcessingError::database(e.to_string(), e))?;
        let timings = QueryTimings {
            elapsed_ms: start.elapsed().as_millis() as u64,
            received_bytes: cursor.received_bytes(),
            decoded_bytes: cursor.decoded_bytes(),
            rows_read: Some(0),
            rows_read_unknown: true,
            rows_returned: u64::from(row_opt.is_some()),
        };
        Ok((row_opt, timings))
    }

    /// Read an already resolved position without another signature lookup.
    /// Admission and all position/legacy attempts share one operation deadline.
    #[cfg(feature = "disk-cache")]
    pub(crate) async fn get_transaction_by_signature_and_position(
        &self,
        signature: &str,
        position: SignatureSlot,
    ) -> ProcessingResult<(Option<StoredTransactionRecord>, QueryTimings)> {
        self.with_http_query_timeout("get_transaction_by_signature_and_position", async {
            let (_, literal) = decode_transaction_signature(signature)?;
            self.fetch_transaction_at_position(&literal, position).await
        })
        .await
    }

    // The caller owns HTTP admission and the operation deadline.
    async fn fetch_transaction_at_position(
        &self,
        signature_literal: &str,
        position: SignatureSlot,
    ) -> ProcessingResult<(Option<StoredTransactionRecord>, QueryTimings)> {
        let slot = position.slot;
        let slot_idx = position.slot_idx;

        let build_query = |table: &str, slot_idx: Option<u32>, settings_clause: &str| {
            build_get_transaction_by_signature_query(
                table,
                signature_literal,
                slot,
                slot_idx,
                settings_clause,
            )
        };

        let (mut row_opt, mut timings, used_local) = self
            .fetch_first_transaction_position(signature_literal, position)
            .await?;

        if used_local && row_opt.is_none() {
            // The shard-local table is expected to contain the same data as the distributed table,
            // but fall back to the distributed table to avoid false negatives if local data is
            // incomplete (e.g. during backfills or replication lag).
            let settings_clause = self.select_get_transaction_settings_clause(
                "get_transaction_by_signature_distributed_retry",
                QueryFreshnessClass::Historical,
            );
            let query = build_query(&self.transaction_table, Some(slot_idx), &settings_clause);
            let (fallback_opt, fallback_timings) = self.fetch_transaction_lookup(&query).await?;
            timings.add(fallback_timings);
            row_opt = fallback_opt;
        }

        if row_opt.is_none() {
            // Fall back to the legacy query that doesn't require slot_idx to match.
            let settings_clause = self.select_get_transaction_settings_clause(
                "get_transaction_by_signature_legacy_fallback",
                QueryFreshnessClass::Historical,
            );
            let query = build_query(&self.transaction_table, None, &settings_clause);
            let (fallback_opt, fallback_timings) = self.fetch_transaction_lookup(&query).await?;
            timings.add(fallback_timings);
            row_opt = fallback_opt;
        }

        let Some(row) = row_opt else {
            return Ok((None, timings));
        };

        Ok((Some(map_transaction_row(row)), timings))
    }

    async fn fetch_first_transaction_position(
        &self,
        signature_literal: &str,
        position: SignatureSlot,
    ) -> ProcessingResult<(Option<TransactionRow>, QueryTimings, bool)> {
        let slot = position.slot;
        let slot_idx = position.slot_idx;
        let build_query = |table: &str, slot_idx: Option<u32>, settings_clause: &str| {
            build_get_transaction_by_signature_query(
                table,
                signature_literal,
                slot,
                slot_idx,
                settings_clause,
            )
        };
        let result = if self.scope_shard_direct()
            && self.transport_http()
            && let (Some(topology), Some(local_table)) =
                (&self.shard_topology, &self.transactions_local_table)
        {
            let shard = topology.shard_for_hash(slot / SLOT_SHARD_DIVISOR);
            let settings_clause = topology.get_transaction_settings_clause(
                "get_transaction_by_signature_local_http",
                QueryFreshnessClass::Historical,
            );
            let query = build_query(local_table, Some(slot_idx), &settings_clause);

            match fetch_single_transaction_row(&shard.http_client, &shard.read_endpoint, &query)
                .await
            {
                Ok(result) => (result.0, result.1, true),
                Err(err) => {
                    if transient_shard_local_error_reason(&err).is_some() {
                        topology.failover_from(&shard);
                    }
                    tracing::warn!(
                        "Shard {}:{} HTTP query failed; falling back to distributed table: {}",
                        shard.host,
                        shard.tcp_port,
                        err
                    );
                    let settings_clause = self.select_get_transaction_settings_clause(
                        "get_transaction_by_signature_fallback_distributed",
                        QueryFreshnessClass::Historical,
                    );
                    let query =
                        build_query(&self.transaction_table, Some(slot_idx), &settings_clause);
                    let result = self.fetch_transaction_lookup(&query).await?;
                    (result.0, result.1, false)
                }
            }
        } else {
            let settings_clause = self.select_get_transaction_settings_clause(
                "get_transaction_by_signature_distributed",
                QueryFreshnessClass::Historical,
            );
            let query = build_query(&self.transaction_table, Some(slot_idx), &settings_clause);
            let result = self.fetch_transaction_lookup(&query).await?;
            (result.0, result.1, false)
        };
        Ok(result)
    }

    pub async fn get_transaction_by_signature_and_slot(
        &self,
        signature: &str,
        slot: u64,
    ) -> ProcessingResult<(Option<StoredTransactionRecord>, QueryTimings)> {
        self.with_http_query_timeout("get_transaction_by_signature_and_slot", async {
            let (_signature_bytes, signature_literal) = decode_transaction_signature(signature)?;
            let build_query = |table: &str, settings_clause: &str| {
                build_get_transaction_by_signature_query(
                    table,
                    &signature_literal,
                    slot,
                    None,
                    settings_clause,
                )
            };

            let (mut row_opt, mut timings, used_local) = if self.scope_shard_direct()
                && self.transport_http()
                && let (Some(topology), Some(local_table)) =
                    (&self.shard_topology, &self.transactions_local_table)
            {
                let shard = topology.shard_for_hash(slot / SLOT_SHARD_DIVISOR);
                let settings_clause = topology.get_transaction_settings_clause(
                    "get_transaction_by_signature_slot_local_http",
                    QueryFreshnessClass::Historical,
                );
                let query = build_query(local_table, &settings_clause);

                match fetch_single_transaction_row(&shard.http_client, &shard.read_endpoint, &query)
                    .await
                {
                    Ok(result) => (result.0, result.1, true),
                    Err(err) => {
                        if transient_shard_local_error_reason(&err).is_some() {
                            topology.failover_from(&shard);
                        }
                        tracing::warn!(
                            "Shard {}:{} HTTP query failed; falling back to distributed table: {}",
                            shard.host,
                            shard.tcp_port,
                            err
                        );
                        let settings_clause = self.select_get_transaction_settings_clause(
                            "get_transaction_by_signature_slot_fallback_distributed",
                            QueryFreshnessClass::Historical,
                        );
                        let query = build_query(&self.transaction_table, &settings_clause);
                        let result = self.fetch_transaction_lookup(&query).await?;
                        (result.0, result.1, false)
                    }
                }
            } else {
                let settings_clause = self.select_get_transaction_settings_clause(
                    "get_transaction_by_signature_slot_distributed",
                    QueryFreshnessClass::Historical,
                );
                let query = build_query(&self.transaction_table, &settings_clause);
                let result = self.fetch_transaction_lookup(&query).await?;
                (result.0, result.1, false)
            };

            if used_local && row_opt.is_none() {
                let settings_clause = self.select_get_transaction_settings_clause(
                    "get_transaction_by_signature_slot_distributed_retry",
                    QueryFreshnessClass::Historical,
                );
                let query = build_query(&self.transaction_table, &settings_clause);
                let (fallback_opt, fallback_timings) =
                    self.fetch_transaction_lookup(&query).await?;
                timings.add(fallback_timings);
                row_opt = fallback_opt;
            }

            let Some(row) = row_opt else {
                return Ok((None, timings));
            };

            Ok((Some(map_transaction_row(row)), timings))
        })
        .await
    }

    async fn try_get_transactions_for_address_signatures_tcp(
        &self,
        router: &super::gsfa::GsfaShardRouter,
        token_owner_local_table: Option<&str>,
        query: &TransactionsForAddressQuery,
        pubkey: &Pubkey,
    ) -> ProcessingResult<Option<(Vec<TransactionsForAddressRecord>, QueryTimings)>> {
        if query.token_accounts != TokenAccountsFilter::None && token_owner_local_table.is_none() {
            return Ok(None);
        }

        let shard = router.topology.shard_for_hash(cityhash64(pubkey.as_ref()));
        let query_timeout = self.shard_tcp_query_timeout();

        let settings_clause = append_max_execution_time_setting(
            &router.topology.settings_clause_with_condition_cache(
                "get_transactions_for_address_signatures_local_tcp",
                QueryFreshnessClass::Historical,
            ),
            query_timeout,
        );
        let token_owner_table = token_owner_local_table.unwrap_or(&self.token_owner_activity_table);
        let gsfa_table = self.gsfa_local_table(router);
        let gsfa_bucket_modulus = self.gsfa_bucket_modulus_for_address(pubkey);
        let tables = TransactionsForAddressTables {
            cache_partition: self.cache_partition,
            gsfa_table,
            gsfa_bucket_modulus,
            token_owner_table,
            token_owner_bucket_modulus: self.token_owner_bucket_modulus(),
            signatures_table: &self.signature_statuses_table,
            signature_bucket_modulus: self.signatures_bucket_modulus(),
            union_pushdown: self.transactions_for_address_union_pushdown,
        };
        let query_sql = build_transactions_for_address_query(&tables, query, &settings_clause)?;

        let (block, timings) = match execute_shard_tcp_query_block(
            shard.clone(),
            query_timeout,
            "get_transactions_for_address_signatures_local_tcp",
            "transactions_for_address_local_tcp",
            query_sql,
        )
        .await
        {
            Ok(result) => result,
            Err(err) => {
                if let Some(reason) = transient_shard_local_error_reason(&err) {
                    crate::metrics::clickhouse_transport_fallback(
                        "get_transactions_for_address_signatures_local_tcp",
                        "tcp",
                        "http",
                        reason,
                    );
                    tracing::warn!(
                        "Shard {}:{} TCP getTransactionsForAddress query failed; falling back to HTTP: {}",
                        shard.host,
                        shard.tcp_port,
                        err
                    );
                    return Ok(None);
                }
                return Err(err);
            }
        };

        let mut results = Vec::new();
        for row in block.rows() {
            let signature: String = row
                .get("signature")
                .map_err(|e| ProcessingError::database(e.to_string(), e))?;
            let slot: u64 = row
                .get("slot")
                .map_err(|e| ProcessingError::database(e.to_string(), e))?;
            let slot_idx: u32 = row
                .get("slot_idx")
                .map_err(|e| ProcessingError::database(e.to_string(), e))?;
            let err: Option<String> = row
                .get("err")
                .map_err(|e| ProcessingError::database(e.to_string(), e))?;
            let memo: Option<String> = row
                .get("memo")
                .map_err(|e| ProcessingError::database(e.to_string(), e))?;
            let block_time: Option<i64> = row
                .get("block_time")
                .map_err(|e| ProcessingError::database(e.to_string(), e))?;

            let parsed_err = err.and_then(|err_str| parse_err_json(&signature, err_str));
            results.push(TransactionsForAddressRecord {
                signature,
                slot,
                slot_idx,
                err: parsed_err,
                memo: format_gsfa_memo(memo),
                block_time,
            });
        }

        let mut timings = timings;
        timings.rows_returned = results.len() as u64;

        Ok(Some((results, timings)))
    }

    async fn get_hot_transactions_for_address_signatures_tcp(
        &self,
        topology: &ShardTopology,
        query: &TransactionsForAddressQuery,
        pubkey: &Pubkey,
    ) -> ProcessingResult<(Vec<TransactionsForAddressRecord>, QueryTimings)> {
        let local_table: Arc<str> = self.gsfa_hot_local_table.clone().into();
        let gsfa_bucket_modulus = self.gsfa_bucket_modulus_for_address(pubkey);
        let fanout_sem = self.fanout_sem.clone();
        let query_timeout = self.shard_tcp_query_timeout();
        let settings_clause: Arc<str> = append_max_execution_time_setting(
            &topology.settings_clause_with_condition_cache(
                "get_transactions_for_address_hot_local_tcp",
                QueryFreshnessClass::Historical,
            ),
            query_timeout,
        )
        .into();
        let mut join_set = JoinSet::new();

        for shard in topology.active_shards() {
            let local_table = local_table.clone();
            let fanout_sem = fanout_sem.clone();
            let settings_clause = settings_clause.clone();
            let hot_query = query.clone();

            let inherited_admission = admission::current();
            join_set.spawn(admission::scope_with(inherited_admission, async move {
                admission::run_with_permit(
                    fanout_sem,
                    |_| {
                        (
                            shard.host.clone(),
                            shard.tcp_port,
                            ProcessingError::database_msg("ClickHouse fanout admission closed"),
                        )
                    },
                    async {
                        let query_sql = build_transactions_for_address_hot_query(
                            local_table.as_ref(),
                            gsfa_bucket_modulus,
                            &hot_query,
                            settings_clause.as_ref(),
                        )
                        .map_err(|e| (shard.host.clone(), shard.tcp_port, e))?;

                        match execute_shard_tcp_query_block(
                            shard.clone(),
                            query_timeout,
                            "get_transactions_for_address_hot_local_tcp",
                            "transactions_for_address_hot_local_tcp",
                            query_sql,
                        )
                        .await
                        {
                            Ok((block, timings)) => {
                                let mut records = Vec::new();
                                for row in block.rows() {
                                    let query_row = TransactionsForAddressQueryRow {
                                        signature: row.get("signature").map_err(|e| {
                                            (
                                                shard.host.clone(),
                                                shard.tcp_port,
                                                ProcessingError::database(e.to_string(), e),
                                            )
                                        })?,
                                        slot: row.get("slot").map_err(|e| {
                                            (
                                                shard.host.clone(),
                                                shard.tcp_port,
                                                ProcessingError::database(e.to_string(), e),
                                            )
                                        })?,
                                        slot_idx: row.get("slot_idx").map_err(|e| {
                                            (
                                                shard.host.clone(),
                                                shard.tcp_port,
                                                ProcessingError::database(e.to_string(), e),
                                            )
                                        })?,
                                        err: row.get("err").map_err(|e| {
                                            (
                                                shard.host.clone(),
                                                shard.tcp_port,
                                                ProcessingError::database(e.to_string(), e),
                                            )
                                        })?,
                                        memo: row.get("memo").map_err(|e| {
                                            (
                                                shard.host.clone(),
                                                shard.tcp_port,
                                                ProcessingError::database(e.to_string(), e),
                                            )
                                        })?,
                                        block_time: row.get("block_time").map_err(|e| {
                                            (
                                                shard.host.clone(),
                                                shard.tcp_port,
                                                ProcessingError::database(e.to_string(), e),
                                            )
                                        })?,
                                    };
                                    records.push(map_transactions_for_address_row(query_row));
                                }

                                Ok((records, timings))
                            }
                            Err(err) => Err((shard.host.clone(), shard.tcp_port, err)),
                        }
                    },
                )
                .await
            }));
        }

        let mut records = Vec::new();
        let mut timings = QueryTimings::zero();
        while let Some(joined) = join_set.join_next().await {
            match joined {
                Ok(Ok((shard_records, shard_timings))) => {
                    records.extend(shard_records);
                    timings.merge_parallel(shard_timings);
                }
                Ok(Err((host, port, err))) => {
                    if transient_shard_local_error_reason(&err).is_some() {
                        topology.failover_endpoint(&host, port);
                    }
                    let context = format!(
                        "Shard-local getTransactionsForAddress hot TCP query failed on {host}:{port}: {err}"
                    );
                    tracing::warn!("{context}");
                    return Err(ProcessingError::database(context, err));
                }
                Err(err) => {
                    return Err(ProcessingError::database_msg(format!(
                        "Shard-local getTransactionsForAddress hot TCP task failed: {err}"
                    )));
                }
            }
        }

        let records =
            order_transactions_for_address_records(records, query.sort_order, query.limit);
        timings.rows_returned = records.len() as u64;
        Ok((records, timings))
    }

    async fn try_get_transactions_for_address_signatures_http(
        &self,
        router: &super::gsfa::GsfaShardRouter,
        token_owner_local_table: Option<&str>,
        query: &TransactionsForAddressQuery,
        pubkey: &Pubkey,
    ) -> ProcessingResult<Option<(Vec<TransactionsForAddressRecord>, QueryTimings)>> {
        if query.token_accounts != TokenAccountsFilter::None && token_owner_local_table.is_none() {
            return Ok(None);
        }

        let shard = router.topology.shard_for_hash(cityhash64(pubkey.as_ref()));

        let settings_clause = router.topology.settings_clause_with_condition_cache(
            "get_transactions_for_address_signatures_local_http",
            QueryFreshnessClass::Historical,
        );
        let token_owner_table = token_owner_local_table.unwrap_or(&self.token_owner_activity_table);
        let gsfa_table = self.gsfa_local_table(router);
        let gsfa_bucket_modulus = self.gsfa_bucket_modulus_for_address(pubkey);
        let tables = TransactionsForAddressTables {
            cache_partition: self.cache_partition,
            gsfa_table,
            gsfa_bucket_modulus,
            token_owner_table,
            token_owner_bucket_modulus: self.token_owner_bucket_modulus(),
            signatures_table: &self.signature_statuses_table,
            signature_bucket_modulus: self.signatures_bucket_modulus(),
            union_pushdown: self.transactions_for_address_union_pushdown,
        };
        let query_sql = build_transactions_for_address_query(&tables, query, &settings_clause)?;

        #[derive(Deserialize, clickhouse::Row)]
        struct QueryResult {
            signature: String,
            slot: u64,
            slot_idx: u32,
            err: Option<String>,
            memo: Option<String>,
            block_time: Option<i64>,
        }

        let start = Instant::now();
        let mut cursor = match shard
            .read_endpoint
            .fetch::<QueryResult>(
                &shard.http_client,
                &query_sql,
                "get_transactions_for_address_signatures_local_http",
            )
            .await
        {
            Ok(cursor) => cursor,
            Err(err) => {
                return Err(ProcessingError::database(
                    format!(
                        "Shard {}:{} HTTP getTransactionsForAddress query failed",
                        shard.host, shard.tcp_port
                    ),
                    err,
                ));
            }
        };

        let mut results = Vec::new();
        loop {
            match cursor.next().await {
                Ok(Some(row)) => {
                    let parsed_err = row
                        .err
                        .and_then(|err_str| parse_err_json(&row.signature, err_str));
                    results.push(TransactionsForAddressRecord {
                        signature: row.signature,
                        slot: row.slot,
                        slot_idx: row.slot_idx,
                        err: parsed_err,
                        memo: format_gsfa_memo(row.memo),
                        block_time: row.block_time,
                    });
                }
                Ok(None) => break,
                Err(err) => {
                    return Err(ProcessingError::database(
                        format!(
                            "Shard {}:{} HTTP getTransactionsForAddress query stream failed",
                            shard.host, shard.tcp_port
                        ),
                        err,
                    ));
                }
            }
        }

        let timings = QueryTimings {
            elapsed_ms: start.elapsed().as_millis() as u64,
            received_bytes: cursor.received_bytes(),
            decoded_bytes: cursor.decoded_bytes(),
            rows_read: Some(0),
            rows_read_unknown: true,
            rows_returned: results.len() as u64,
        };

        Ok(Some((results, timings)))
    }

    async fn get_hot_transactions_for_address_signatures_http(
        &self,
        topology: &ShardTopology,
        query: &TransactionsForAddressQuery,
        pubkey: &Pubkey,
    ) -> ProcessingResult<(Vec<TransactionsForAddressRecord>, QueryTimings)> {
        let local_table: Arc<str> = self.gsfa_hot_local_table.clone().into();
        let gsfa_bucket_modulus = self.gsfa_bucket_modulus_for_address(pubkey);
        let settings_clause: Arc<str> = topology
            .settings_clause_with_condition_cache(
                "get_transactions_for_address_hot_local_http",
                QueryFreshnessClass::Historical,
            )
            .into();
        let fanout_sem = self.fanout_sem.clone();
        let query_timeout = self.query_timeout;
        let mut join_set = JoinSet::new();

        for shard in topology.active_shards() {
            let local_table = local_table.clone();
            let settings_clause = settings_clause.clone();
            let fanout_sem = fanout_sem.clone();
            let hot_query = query.clone();

            let inherited_admission = admission::current();
            join_set.spawn(admission::scope_with(inherited_admission, async move {
                admission::run_with_permit(fanout_sem, |_| {
                    (
                        shard.host.clone(),
                        shard.tcp_port,
                        ProcessingError::database_msg("ClickHouse fanout admission closed"),
                    )
                }, async {
                let timed = tokio::time::timeout(query_timeout, async {
                    let query_sql = build_transactions_for_address_hot_query(
                        local_table.as_ref(),
                        gsfa_bucket_modulus,
                        &hot_query,
                        settings_clause.as_ref(),
                    )?;
                    let start = Instant::now();
                    let mut cursor = shard
                        .read_endpoint
                        .fetch::<TransactionsForAddressQueryRow>(
                            &shard.http_client,
                            &query_sql,
                            "get_transactions_for_address_hot_local_http",
                        )
                        .await
                        .map_err(|e| ProcessingError::database(e.to_string(), e))?;

                    let mut records = Vec::new();
                    while let Some(row) = cursor
                        .next()
                        .await
                        .map_err(|e| ProcessingError::database(e.to_string(), e))?
                    {
                        records.push(map_transactions_for_address_row(row));
                    }

                    let shard_timings = QueryTimings {
                        elapsed_ms: start.elapsed().as_millis() as u64,
                        received_bytes: cursor.received_bytes(),
                        decoded_bytes: cursor.decoded_bytes(),
                        rows_read: Some(0),
                        rows_read_unknown: true,
                        rows_returned: records.len() as u64,
                    };
                    Ok::<_, ProcessingError>((records, shard_timings))
                })
                .await;

                match timed {
                    Ok(result) => result.map_err(|e| (shard.host.clone(), shard.tcp_port, e)),
                    Err(_) => {
                        crate::metrics::clickhouse_timeout(
                            "get_transactions_for_address_hot_local_http",
                        );
                        Err((
                            shard.host.clone(),
                            shard.tcp_port,
                            ProcessingError::timeout_msg(
                                "Shard-local getTransactionsForAddress hot HTTP query timed out",
                            ),
                        ))
                    }
                }
                }).await
            }));
        }

        let mut records = Vec::new();
        let mut timings = QueryTimings::zero();
        while let Some(joined) = join_set.join_next().await {
            match joined {
                Ok(Ok((shard_records, shard_timings))) => {
                    records.extend(shard_records);
                    timings.merge_parallel(shard_timings);
                }
                Ok(Err((host, port, err))) => {
                    if transient_shard_local_error_reason(&err).is_some() {
                        topology.failover_endpoint(&host, port);
                    }
                    return Err(ProcessingError::database_msg(format!(
                        "Shard-local getTransactionsForAddress hot HTTP query failed on {host}:{port}: {err}"
                    )));
                }
                Err(err) => {
                    return Err(ProcessingError::database_msg(format!(
                        "Shard-local getTransactionsForAddress hot HTTP task failed: {err}"
                    )));
                }
            }
        }

        let records =
            order_transactions_for_address_records(records, query.sort_order, query.limit);
        timings.rows_returned = records.len() as u64;
        Ok((records, timings))
    }

    async fn try_get_transactions_by_slot_signatures_local(
        &self,
        topology: &ShardTopology,
        local_table: &str,
        pairs: &[(u64, Option<u32>, String)],
    ) -> ProcessingResult<Option<(Vec<StoredTransactionRecord>, QueryTimings)>> {
        let local_table: std::sync::Arc<str> = local_table.to_string().into();
        let fanout_sem = self.fanout_sem.clone();
        let query_timeout = self.query_timeout;
        let in_clause_chunk = self.in_clause_chunk;
        let settings_clause: std::sync::Arc<str> = topology
            .settings_clause(
                "get_transactions_by_slot_signatures_local_http",
                QueryFreshnessClass::Historical,
            )
            .into();

        let mut per_shard: Vec<Vec<(u64, Option<u32>, String)>> =
            vec![Vec::new(); topology.shard_count()];
        for (slot, idx, literal) in pairs {
            let shard_idx = topology.shard_index_for_hash(slot / SLOT_SHARD_DIVISOR);
            per_shard[shard_idx].push((*slot, *idx, literal.clone()));
        }

        let mut join_set = JoinSet::new();
        for (idx, shard_pairs) in per_shard.into_iter().enumerate() {
            if shard_pairs.is_empty() {
                continue;
            }
            let shard = topology
                .shard_at(idx)
                .expect("validated topology contains every routed shard");
            let local_table = local_table.clone();
            let fanout_sem = fanout_sem.clone();
            let settings_clause = settings_clause.clone();

            let inherited_admission = admission::current();
            join_set.spawn(admission::scope_with(inherited_admission, async move {
                admission::run_with_permit(
                    fanout_sem,
                    |_| {
                        (
                            shard.host.clone(),
                            shard.tcp_port,
                            ProcessingError::database_msg("ClickHouse fanout admission closed"),
                        )
                    },
                    async {
                        let query = build_transactions_by_slot_signatures_query(
                            local_table.as_ref(),
                            &shard_pairs,
                            settings_clause.as_ref(),
                            in_clause_chunk,
                        );

                        let timed = tokio::time::timeout(query_timeout, async {
                            let start = Instant::now();
                            let mut cursor = shard
                                .read_endpoint
                                .fetch::<TransactionRow>(
                                    &shard.http_client,
                                    &query,
                                    "get_transactions_by_slot_signatures_local_http",
                                )
                                .await
                                .map_err(|e| ProcessingError::database(e.to_string(), e))?;

                            let mut records = Vec::new();
                            while let Some(row) = cursor
                                .next()
                                .await
                                .map_err(|e| ProcessingError::database(e.to_string(), e))?
                            {
                                records.push(map_transaction_row(row));
                            }

                            let shard_timings = QueryTimings {
                                elapsed_ms: start.elapsed().as_millis() as u64,
                                received_bytes: cursor.received_bytes(),
                                decoded_bytes: cursor.decoded_bytes(),
                                rows_read: Some(0),
                                rows_read_unknown: true,
                                rows_returned: records.len() as u64,
                            };
                            Ok::<_, ProcessingError>((records, shard_timings))
                        })
                        .await;

                        match timed {
                            Ok(result) => {
                                result.map_err(|e| (shard.host.clone(), shard.tcp_port, e))
                            }
                            Err(_) => {
                                crate::metrics::clickhouse_timeout(
                                    "get_transactions_by_slot_signatures_local",
                                );
                                Err((
                                    shard.host.clone(),
                                    shard.tcp_port,
                                    ProcessingError::timeout_msg(
                                        "Shard-local transaction fetch timed out",
                                    ),
                                ))
                            }
                        }
                    },
                )
                .await
            }));
        }

        let mut records = Vec::new();
        let mut timings = QueryTimings::zero();
        while let Some(joined) = join_set.join_next().await {
            match joined {
                Ok(Ok((shard_records, shard_timings))) => {
                    records.extend(shard_records);
                    timings.merge_parallel(shard_timings);
                }
                Ok(Err((host, port, err))) => {
                    if transient_shard_local_error_reason(&err).is_some() {
                        topology.failover_endpoint(&host, port);
                    }
                    tracing::warn!(
                        "Shard {}:{} HTTP query failed; falling back to distributed table: {}",
                        host,
                        port,
                        err
                    );
                    return Ok(None);
                }
                Err(err) => {
                    tracing::warn!(
                        "Shard-local task failed; falling back to distributed table: {}",
                        err
                    );
                    return Ok(None);
                }
            }
        }

        if records.is_empty() {
            return Ok(None);
        }

        Ok(Some((records, timings)))
    }
}

#[cfg(test)]
mod tests {
    use super::super::cache::SignatureSlotCacheLeader;
    use super::*;

    #[test]
    fn hydration_exact_query_keeps_slot_index_and_signature_for_each_key() {
        let pairs = vec![
            transaction_lookup_key(10, Some(3), &bs58::encode([1; 64]).into_string()).unwrap(),
            transaction_lookup_key(20, Some(7), &bs58::encode([2; 64]).into_string()).unwrap(),
        ];
        let sql = build_transactions_by_slot_signatures_query("transactions", &pairs, "", 1);
        assert_eq!(sql.matches("(slot, slot_idx, signature) IN").count(), 2);
        assert!(sql.contains("(10, 3, toFixedString("));
        assert!(sql.contains("(20, 7, toFixedString("));
        assert!(sql.contains(" OR "));
        assert!(!sql.contains("tx_version <="));
        assert!(!sql.contains("tx_version IS NULL"));
    }

    #[test]
    fn hydration_fallback_query_keeps_identity_and_drops_only_index() {
        let pairs = vec![
            transaction_lookup_key(10, None, &bs58::encode([1; 64]).into_string()).unwrap(),
            transaction_lookup_key(10, None, &bs58::encode([2; 64]).into_string()).unwrap(),
        ];
        let sql = build_transactions_by_slot_signatures_query("transactions", &pairs, "", 100);
        assert_eq!(sql.matches("(slot, signature) IN").count(), 1);
        assert!(!sql.contains("(slot, slot_idx, signature) IN"));
        assert_eq!(sql.matches("(10, toFixedString(").count(), 2);
    }

    #[test]
    fn hydration_keys_reject_invalid_signatures_before_building_sql() {
        assert!(transaction_lookup_key(1, Some(1), "'").is_err());
        assert!(transaction_lookup_key(1, Some(1), "1").is_err());
    }

    fn normalize_sql(sql: &str) -> String {
        sql.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    fn transactions_for_address_record(
        signature: &str,
        slot: u64,
        slot_idx: u32,
    ) -> TransactionsForAddressRecord {
        TransactionsForAddressRecord {
            signature: signature.to_string(),
            slot,
            slot_idx,
            err: None,
            memo: None,
            block_time: None,
        }
    }

    #[test]
    fn transactions_for_address_batch_size_overfetches_with_bounds() {
        assert_eq!(transactions_for_address_batch_size(1), 64);
        assert_eq!(transactions_for_address_batch_size(100), 200);
        assert_eq!(transactions_for_address_batch_size(1_000), 2_000);
        assert_eq!(transactions_for_address_batch_size(u64::MAX), 2_000);
    }

    #[test]
    fn transactions_for_address_deduplicates_across_internal_batches() {
        let mut output = Vec::new();
        let mut seen = HashSet::new();

        append_unique_transactions_for_address_records(
            &mut output,
            &mut seen,
            vec![
                transactions_for_address_record("sig-c", 30, 3),
                transactions_for_address_record("sig-c", 30, 3),
                transactions_for_address_record("sig-b", 20, 2),
            ],
            3,
        );
        append_unique_transactions_for_address_records(
            &mut output,
            &mut seen,
            vec![
                transactions_for_address_record("sig-b", 20, 2),
                transactions_for_address_record("sig-a", 10, 1),
                transactions_for_address_record("sig-old", 5, 0),
            ],
            3,
        );

        assert_eq!(
            output
                .iter()
                .map(|record| record.signature.as_str())
                .collect::<Vec<_>>(),
            vec!["sig-c", "sig-b", "sig-a"]
        );
    }

    #[test]
    fn strict_get_transaction_query_uses_slot_and_signature_without_slot_idx() {
        let query = build_get_transaction_by_signature_query(
            "default.transactions",
            "toFixedString(unhex('AB'), 64)",
            42,
            None,
            "SETTINGS use_query_cache = 1",
        );
        let query = normalize_sql(&query);

        assert!(query.contains("FROM default.transactions"));
        assert!(
            query.contains("PREWHERE slot = 42 AND signature = toFixedString(unhex('AB'), 64)")
        );
        assert!(query.contains("ORDER BY slot_idx DESC LIMIT 1"));
        assert!(!query.contains("AND slot_idx ="));
        assert!(query.contains("SETTINGS use_query_cache = 1"));
    }

    #[test]
    fn resolved_get_transaction_query_includes_slot_idx_when_available() {
        let query = build_get_transaction_by_signature_query(
            "default.transactions",
            "toFixedString(unhex('AB'), 64)",
            42,
            Some(7),
            "",
        );
        let query = normalize_sql(&query);

        assert!(query.contains(
            "PREWHERE slot = 42 AND slot_idx = 7 AND signature = toFixedString(unhex('AB'), 64)"
        ));
        assert!(!query.contains("ORDER BY slot_idx DESC"));
    }

    fn single_round_trip_test_client(url: &str, database: &str, enabled: bool) -> ClickHouseClient {
        let mut client = ClickHouseClient::new(
            url,
            "default",
            "default",
            "",
            super::super::ClickHouseClientOptions::new(
                super::super::RoutingPolicy {
                    transport: super::super::RoutingTransport::Http,
                    scope: super::super::RoutingScope::Distributed,
                },
                None,
                Vec::new(),
                format!("{database}.gsfa_hot"),
                format!("{database}.gsfa_hot_local"),
            ),
        );
        client.transaction_table = format!("{database}.transactions");
        client.signature_statuses_table = format!("{database}.signatures");
        client.set_get_transaction_single_round_trip(enabled);
        client
    }

    #[test]
    fn single_round_trip_query_resolves_position_and_payload_in_one_statement() {
        let signature = bs58::encode([7u8; 64]).into_string();
        let (_, literal) = decode_transaction_signature(&signature).unwrap();
        let sql = build_get_transaction_single_round_trip_query(
            "default.transactions",
            "default.signatures",
            5,
            &literal,
            "SETTINGS optimize_skip_unused_shards=1, use_query_cache=0",
        );

        // One aggregate row even for an unknown signature, with an unreachable sentinel slot.
        assert!(sql.contains(
            "SELECT if(count() = 0, (toUInt64(18446744073709551615), toUInt32(0)), max((slot, slot_idx)))"
        ));
        assert!(sql.contains(&format!(
            "FROM default.signatures\n            PREWHERE sig_bucket = 5 AND signature = {literal}"
        )));
        // The full primary key, so the payload read selects one granule.
        assert!(sql.contains(&format!(
            "FROM default.transactions\n         PREWHERE slot = tupleElement(pos, 1)\n            AND slot_idx = tupleElement(pos, 2)\n            AND signature = {literal}"
        )));
        assert!(sql.contains("WHERE tupleElement(pos, 1) != 18446744073709551615"));
        assert!(!sql.contains("ORDER BY"));
        assert!(sql.contains("LIMIT 1"));
        assert!(sql.contains(TRANSACTION_SELECT_COLUMNS));
        assert!(sql.trim_end().ends_with("use_query_cache=0"));
    }

    #[test]
    fn single_round_trip_settings_disable_query_cache() {
        let mut client = single_round_trip_test_client("http://127.0.0.1:1", "default", true);
        client.query_cache = super::super::util::QueryCacheConfig::new(true, 30, false, false)
            .with_get_transaction_overrides(300, 2);
        client.allow_query_settings = true;
        let settings = client.single_round_trip_settings_clause();
        assert!(settings.starts_with("SETTINGS optimize_skip_unused_shards=1, use_query_cache=0"));
        assert!(settings.contains("max_execution_time="));
        assert!(!settings.contains("use_query_cache=1"));
        assert!(!settings.contains("enable_writes_to_query_cache"));

        client.allow_query_settings = false;
        assert_eq!(client.single_round_trip_settings_clause(), "");
    }

    #[test]
    fn single_round_trip_reads_owner_shard_source_when_routed() {
        let mut client = single_round_trip_test_client("http://127.0.0.1:1", "default", true);
        client.allow_query_settings = true;
        assert!(
            !client
                .single_round_trip_settings_clause()
                .contains("force_optimize_skip_unused_shards")
        );
        assert_eq!(client.signature_lookup_source(), "default.signatures");
        client
            .set_signatures_owner_shard_routing(true, "my_cluster", None)
            .unwrap();
        assert_eq!(
            client.signature_lookup_source(),
            "cluster('my_cluster', default.signatures_local, cityHash64(signature))"
        );
        let settings = client.single_round_trip_settings_clause();
        assert!(settings.starts_with("SETTINGS optimize_skip_unused_shards=1, use_query_cache=0"));
        assert!(settings.ends_with(", force_optimize_skip_unused_shards=0"));
        assert_eq!(settings.matches("optimize_skip_unused_shards=1").count(), 1);
    }

    #[test]
    fn single_round_trip_applies_only_to_distributed_primary_reads() {
        let mut client = single_round_trip_test_client("http://127.0.0.1:1", "default", false);
        assert!(!client.get_transaction_single_round_trip_enabled());
        client.set_get_transaction_single_round_trip(true);
        assert!(client.get_transaction_single_round_trip_enabled());
        client.cache_partition = Some((100, 1));
        assert!(!client.get_transaction_single_round_trip_enabled());
    }

    #[tokio::test]
    async fn single_round_trip_cached_miss_returns_none_without_query() {
        // Port 1 refuses connections, so any query would fail the call.
        let client = single_round_trip_test_client("http://127.0.0.1:1", "default", true);
        let signature_bytes = [9u8; 64];
        client
            .signature_slot_cache
            .prime_for_tests(signature_bytes, None)
            .await;
        let (record, timings) = client
            .get_transaction_by_signature(&bs58::encode(signature_bytes).into_string())
            .await
            .expect("cached miss");
        assert!(record.is_none());
        assert_eq!(timings.rows_returned, 0);
    }

    #[tokio::test]
    async fn single_round_trip_query_error_releases_singleflight() {
        let client = single_round_trip_test_client("http://127.0.0.1:1", "default", true);
        let signature_bytes = [10u8; 64];
        let signature = bs58::encode(signature_bytes).into_string();
        assert!(
            client
                .get_transaction_by_signature(&signature)
                .await
                .is_err()
        );
        // A failed leader leaves no entry, so the next caller leads again instead of waiting.
        assert!(matches!(
            client
                .signature_slot_cache
                .get_or_start(signature_bytes)
                .await,
            CacheStart::Leader(_)
        ));
    }

    /// Spawns a flag-on getTransaction for `signature_bytes` while the test holds the cache
    /// leader, and returns once the call is waiting on that leader.
    async fn spawn_single_round_trip_waiter(
        client: &Arc<ClickHouseClient>,
        signature_bytes: SignatureBytes,
    ) -> tokio::task::JoinHandle<ProcessingResult<(Option<StoredTransactionRecord>, QueryTimings)>>
    {
        let spawned = client.clone();
        let signature = bs58::encode(signature_bytes).into_string();
        let handle =
            tokio::spawn(async move { spawned.get_transaction_by_signature(&signature).await });
        for _ in 0..1_000 {
            if client
                .signature_slot_cache
                .in_flight_waiters_for_tests(signature_bytes)
                .await
                == 1
            {
                return handle;
            }
            tokio::task::yield_now().await;
        }
        panic!("getTransaction never waited on the cache leader");
    }

    fn expect_leader(start: CacheStart<Option<SignatureSlot>>) -> SignatureSlotCacheLeader {
        match start {
            CacheStart::Leader(leader) => leader,
            other => panic!("expected cache leader, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn single_round_trip_waiter_uses_leader_miss_without_query() {
        // Port 1 refuses connections, so any query would fail the call.
        let client = Arc::new(single_round_trip_test_client(
            "http://127.0.0.1:1",
            "default",
            true,
        ));
        let signature_bytes = [11u8; 64];
        let leader = expect_leader(
            client
                .signature_slot_cache
                .get_or_start(signature_bytes)
                .await,
        );
        let waiter = spawn_single_round_trip_waiter(&client, signature_bytes).await;

        leader.finish(None).await;
        let (record, timings) = waiter.await.expect("join").expect("leader miss");
        assert!(record.is_none());
        // Waited timings, not the zero timings of an immediate cache hit.
        assert!(timings.rows_read_unknown);
        assert_eq!(timings.rows_returned, 0);
    }

    #[tokio::test]
    async fn single_round_trip_waiter_reads_payload_at_leader_position() {
        let client = Arc::new(single_round_trip_test_client(
            "http://127.0.0.1:1",
            "default",
            true,
        ));
        let signature_bytes = [12u8; 64];
        let leader = expect_leader(
            client
                .signature_slot_cache
                .get_or_start(signature_bytes)
                .await,
        );
        let waiter = spawn_single_round_trip_waiter(&client, signature_bytes).await;

        leader
            .finish(Some(SignatureSlot {
                slot: 42,
                slot_idx: 7,
            }))
            .await;
        // The payload read at the leader's position reaches the refused port.
        assert!(waiter.await.expect("join").is_err());
        // The position stays cached; the failed payload read does not evict it.
        assert!(matches!(
            client
                .signature_slot_cache
                .get_or_start(signature_bytes)
                .await,
            CacheStart::Hit(Some(SignatureSlot {
                slot: 42,
                slot_idx: 7
            }))
        ));
    }

    #[tokio::test]
    async fn single_round_trip_waiter_leads_after_leader_failure() {
        let client = Arc::new(single_round_trip_test_client(
            "http://127.0.0.1:1",
            "default",
            true,
        ));
        let signature_bytes = [13u8; 64];
        let leader = expect_leader(
            client
                .signature_slot_cache
                .get_or_start(signature_bytes)
                .await,
        );
        let waiter = spawn_single_round_trip_waiter(&client, signature_bytes).await;

        leader.fail().await;
        // The waiter leads its own fused query, which fails against the refused port and releases
        // the entry again.
        assert!(waiter.await.expect("join").is_err());
        expect_leader(
            client
                .signature_slot_cache
                .get_or_start(signature_bytes)
                .await,
        );
    }

    /// Compares the one-query path with the two-query path on a dedicated database of an
    /// explicitly supplied local ClickHouse: found, legacy `slot_idx`, orphan and unknown. The
    /// paths agree except for the legacy row, which only the two-query fallback finds.
    #[tokio::test]
    #[ignore = "requires SUPERBANK_SINGLE_RT_CLICKHOUSE_TEST_URL pointing to local ClickHouse"]
    async fn single_round_trip_matches_two_query_path_clickhouse() {
        let url = std::env::var("SUPERBANK_SINGLE_RT_CLICKHOUSE_TEST_URL")
            .expect("set SUPERBANK_SINGLE_RT_CLICKHOUSE_TEST_URL");
        let http = reqwest::Client::new();
        let database = format!(
            "single_rt_{}_{}",
            std::process::id(),
            crate::util::current_time_millis()
        );
        async fn execute(http: &reqwest::Client, url: &str, sql: String) {
            let response = http.post(url).body(sql).send().await.expect("request");
            let status = response.status();
            let body = response.text().await.unwrap();
            assert!(status.is_success(), "ClickHouse {status}: {body}");
        }
        execute(&http, &url, format!("CREATE DATABASE {database}")).await;
        for ddl in [
            include_str!("../../../../ddl/local/transactions.sql"),
            include_str!("../../../../ddl/local/signatures.sql"),
        ] {
            for statement in ddl
                .replace("default.", &format!("{database}."))
                .split(";\n")
            {
                if !statement.trim().is_empty() {
                    execute(&http, &url, statement.to_string()).await;
                }
            }
        }
        let insert_transaction = |fill: char, slot: u64, slot_idx: u32| {
            format!(
                "INSERT INTO {database}.transactions (signature, slot, slot_idx, tx_signatures, tx_num_required_signatures, tx_account_keys, tx_recent_blockhash, meta_status_ok, meta_pre_balances, meta_post_balances) VALUES (repeat('{fill}', 64), {slot}, {slot_idx}, [repeat('{fill}', 64)], 1, [repeat('k', 32)], repeat('h', 32), 1, [10], [10])"
            )
        };
        // Found: the view writes the matching signatures row.
        execute(&http, &url, insert_transaction('a', 700, 3)).await;
        // Legacy: signatures points at slot_idx 9, the payload lives at slot_idx 5.
        execute(&http, &url, insert_transaction('l', 701, 5)).await;
        execute(&http, &url, format!("INSERT INTO {database}.signatures (signature, slot, slot_idx, err) VALUES (repeat('l', 64), 701, 9, NULL)")).await;
        // Orphan: a signatures row without a payload row.
        execute(&http, &url, format!("INSERT INTO {database}.signatures (signature, slot, slot_idx, err) VALUES (repeat('o', 64), 702, 1, NULL)")).await;

        let two_query = single_round_trip_test_client(&url, &database, false);
        let one_query = single_round_trip_test_client(&url, &database, true);
        for client in [&two_query, &one_query] {
            client
                .initialize_read_cancellation()
                .await
                .expect("cancellation preflight");
        }
        let key = |record: &Option<StoredTransactionRecord>| {
            record
                .as_ref()
                .map(|record| (record.signature, record.slot, record.slot_idx))
        };
        for (fill, two_query_expected, expected) in [
            (b'a', Some((700, 3)), Some((700, 3))),
            (b'l', Some((701, 5)), None),
            (b'o', None, None),
            (b'u', None, None),
        ] {
            let signature_bytes = [fill; 64];
            let signature = bs58::encode(signature_bytes).into_string();
            let (before, _) = two_query
                .get_transaction_by_signature(&signature)
                .await
                .expect("two-query read");
            let (after, timings) = one_query
                .get_transaction_by_signature(&signature)
                .await
                .expect("one-query read");
            let position = |record: &Option<StoredTransactionRecord>| {
                key(record).map(|(_, slot, slot_idx)| (slot, slot_idx))
            };
            assert_eq!(
                position(&before),
                two_query_expected,
                "signature {}",
                fill as char
            );
            assert_eq!(position(&after), expected, "signature {}", fill as char);
            assert_eq!(timings.rows_returned, u64::from(expected.is_some()));

            // The leader cached the served position, or a miss.
            let cached = match one_query
                .signature_slot_cache
                .get_or_start(signature_bytes)
                .await
            {
                CacheStart::Hit(value) => value.map(|pos| (pos.slot, pos.slot_idx)),
                _ => panic!("signature {} not cached", fill as char),
            };
            assert_eq!(cached, expected, "signature {}", fill as char);

            // A second read takes the cached path and returns the same result.
            let (again, _) = one_query
                .get_transaction_by_signature(&signature)
                .await
                .expect("cached read");
            assert_eq!(key(&again), key(&after), "signature {}", fill as char);
        }
        execute(&http, &url, format!("DROP DATABASE {database}")).await;
    }
}

#[cfg(all(test, feature = "disk-cache"))]
pub(crate) mod diagnostics;
