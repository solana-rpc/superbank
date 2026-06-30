// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::cmp::Ordering;
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use ch_cityhash102::cityhash64;
use serde::Deserialize;
use solana_sdk::pubkey::Pubkey;
use tokio::task::JoinSet;

use crate::processing::{ProcessingError, ProcessingResult};

use super::QueryFreshnessClass;
use super::client::{ClickHouseClient, execute_shard_tcp_query_block};
use super::constants::SLOT_SHARD_DIVISOR;
use super::queries::{
    TRANSACTION_SELECT_COLUMNS, TransactionsForAddressTables,
    build_transactions_by_slot_signatures_query, build_transactions_for_address_hot_query,
    build_transactions_for_address_query,
};
use super::rows::{TransactionRow, fetch_single_transaction_row, map_transaction_row};
use super::sharding::ShardTopology;
use super::types::{
    QueryTimings, SortOrder, StoredTransactionRecord, TokenAccountsFilter,
    TransactionsForAddressQuery, TransactionsForAddressRecord,
};
use super::util::{
    append_max_execution_time_setting, format_gsfa_memo, parse_err_json,
    transient_shard_local_error_reason,
};

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

fn merge_hot_transactions_for_address_records(
    mut records: Vec<TransactionsForAddressRecord>,
    sort_order: SortOrder,
    limit: u64,
) -> Vec<TransactionsForAddressRecord> {
    records.sort_unstable_by(|a, b| compare_transactions_for_address_records(sort_order, a, b));

    let mut seen = HashSet::with_capacity(records.len());
    let mut merged = Vec::with_capacity(records.len().min(limit as usize));
    for record in records {
        if !seen.insert(record.signature.clone()) {
            continue;
        }
        merged.push(record);
        if merged.len() >= limit as usize {
            break;
        }
    }

    merged
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

    let signature_literal = format!(
        "toFixedString(unhex('{}'), 64)",
        hex::encode(&signature_bytes).to_uppercase()
    );
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

impl ClickHouseClient {
    pub async fn get_transactions_for_address_signatures(
        &self,
        query: &TransactionsForAddressQuery,
    ) -> ProcessingResult<(Vec<TransactionsForAddressRecord>, QueryTimings)> {
        self.with_timeout("get_transactions_for_address_signatures", async {
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

            if self.should_use_gsfa_hot_fanout(&pubkey)
                && query.token_accounts == TokenAccountsFilter::None
            {
                return self
                    .get_hot_transactions_for_address_signatures(query, &pubkey)
                    .await;
            }

            if self.should_use_gsfa_shard_routing(&pubkey)
                && let Some(router) = &self.gsfa_router
            {
                let token_owner_local_table = if query.token_accounts != TokenAccountsFilter::None {
                    self.token_owner_activity_local_table.as_deref()
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
                            &pubkey,
                        )
                        .await?
                    {
                        Some(result) => return Ok(result),
                        None => allow_local_http = true,
                    }
                }

                if allow_local_http
                    && let Some(result) = self
                        .try_get_transactions_for_address_signatures_http(
                            router,
                            token_owner_local_table,
                            query,
                            &pubkey,
                        )
                        .await?
                {
                    return Ok(result);
                }
            }

            let settings_clause = self.select_settings_clause_with_condition_cache(
                "get_transactions_for_address_signatures",
                QueryFreshnessClass::Historical,
            );
            let gsfa_table = self.gsfa_table_for_address(&pubkey);
            let gsfa_bucket_modulus = self.gsfa_bucket_modulus_for_address(&pubkey);
            let tables = TransactionsForAddressTables {
                gsfa_table,
                gsfa_bucket_modulus,
                token_owner_table: &self.token_owner_activity_table,
                token_owner_bucket_modulus: self.token_owner_bucket_modulus(),
                signatures_table: &self.signature_statuses_table,
                signature_bucket_modulus: self.signatures_bucket_modulus(),
            };
            let query = build_transactions_for_address_query(&tables, query, &settings_clause)?;

            let start = Instant::now();
            let mut cursor = self
                .client
                .query(&query)
                .fetch::<TransactionsForAddressQueryRow>()
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
        })
        .await
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

    pub async fn get_transactions_by_slot_signatures(
        &self,
        signatures: &[(u64, String)],
        max_supported_transaction_version: Option<u8>,
    ) -> ProcessingResult<(Vec<StoredTransactionRecord>, QueryTimings)> {
        self.with_timeout("get_transactions_by_slot_signatures", async {
            if signatures.is_empty() {
                return Ok((
                    Vec::new(),
                    QueryTimings {
                        elapsed_ms: 0,
                        received_bytes: 0,
                        decoded_bytes: 0,
                        rows_read: Some(0),
                        rows_read_unknown: true,
                        rows_returned: 0,
                    },
                ));
            }

            let mut pairs = Vec::with_capacity(signatures.len());
            for (slot, signature) in signatures {
                let signature_bytes = bs58::decode(signature)
                    .into_vec()
                    .map_err(|e| ProcessingError::deserialization("Invalid signature", e))?;
                if signature_bytes.len() != 64 {
                    return Err(ProcessingError::deserialization_msg(format!(
                        "Invalid signature length {} (expected 64 bytes)",
                        signature_bytes.len()
                    )));
                }

                let signature_hex = hex::encode(signature_bytes).to_uppercase();
                let signature_literal = format!("toFixedString(unhex('{signature_hex}'), 64)");
                pairs.push((*slot, signature_literal));
            }

            let version_filter = match max_supported_transaction_version {
                Some(max_version) => {
                    format!("(tx_version IS NULL OR tx_version <= {max_version})")
                }
                None => "tx_version IS NULL".to_string(),
            };

            if self.scope_shard_direct()
                && self.transport_http()
                && let (Some(topology), Some(local_table)) =
                    (&self.shard_topology, &self.transactions_local_table)
                && let Some(result) = self
                    .try_get_transactions_by_slot_signatures_local(
                        topology,
                        local_table,
                        &pairs,
                        &version_filter,
                    )
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
                &pairs,
                &version_filter,
                &settings_clause,
                self.in_clause_chunk,
            );

            let start = Instant::now();
            let mut cursor = self
                .client
                .query(&query)
                .fetch::<TransactionRow>()
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
        })
        .await
    }

    pub async fn get_transaction_by_signature(
        &self,
        signature: &str,
    ) -> ProcessingResult<(Option<StoredTransactionRecord>, QueryTimings)> {
        self.with_timeout("get_transaction_by_signature", async {
            let (signature_bytes, signature_literal) = decode_transaction_signature(signature)?;
            let (slot_opt, mut timings) = self
                .get_signature_slot_by_signature_bytes(signature_bytes)
                .await?;

            let Some(position) = slot_opt else {
                return Ok((None, timings));
            };
            let slot = position.slot;
            let slot_idx = position.slot_idx;

            let build_query = |table: &str, slot_idx: Option<u32>, settings_clause: &str| {
                build_get_transaction_by_signature_query(
                    table,
                    &signature_literal,
                    slot,
                    slot_idx,
                    settings_clause,
                )
            };

            let (mut row_opt, query_timings, used_local) = if self.scope_shard_direct()
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

                match fetch_single_transaction_row(&shard.http_client, &query).await {
                    Ok(result) => (result.0, result.1, true),
                    Err(err) => {
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
                        let result = fetch_single_transaction_row(&self.client, &query).await?;
                        (result.0, result.1, false)
                    }
                }
            } else {
                let settings_clause = self.select_get_transaction_settings_clause(
                    "get_transaction_by_signature_distributed",
                    QueryFreshnessClass::Historical,
                );
                let query = build_query(&self.transaction_table, Some(slot_idx), &settings_clause);
                let result = fetch_single_transaction_row(&self.client, &query).await?;
                (result.0, result.1, false)
            };
            timings.add(query_timings);

            if used_local && row_opt.is_none() {
                // The shard-local table is expected to contain the same data as the distributed table,
                // but fall back to the distributed table to avoid false negatives if local data is
                // incomplete (e.g. during backfills or replication lag).
                let settings_clause = self.select_get_transaction_settings_clause(
                    "get_transaction_by_signature_distributed_retry",
                    QueryFreshnessClass::Historical,
                );
                let query = build_query(&self.transaction_table, Some(slot_idx), &settings_clause);
                let (fallback_opt, fallback_timings) =
                    fetch_single_transaction_row(&self.client, &query).await?;
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
                let (fallback_opt, fallback_timings) =
                    fetch_single_transaction_row(&self.client, &query).await?;
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

    pub async fn get_transaction_by_signature_and_slot(
        &self,
        signature: &str,
        slot: u64,
    ) -> ProcessingResult<(Option<StoredTransactionRecord>, QueryTimings)> {
        self.with_timeout("get_transaction_by_signature_and_slot", async {
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

                match fetch_single_transaction_row(&shard.http_client, &query).await {
                    Ok(result) => (result.0, result.1, true),
                    Err(err) => {
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
                        let result = fetch_single_transaction_row(&self.client, &query).await?;
                        (result.0, result.1, false)
                    }
                }
            } else {
                let settings_clause = self.select_get_transaction_settings_clause(
                    "get_transaction_by_signature_slot_distributed",
                    QueryFreshnessClass::Historical,
                );
                let query = build_query(&self.transaction_table, &settings_clause);
                let result = fetch_single_transaction_row(&self.client, &query).await?;
                (result.0, result.1, false)
            };

            if used_local && row_opt.is_none() {
                let settings_clause = self.select_get_transaction_settings_clause(
                    "get_transaction_by_signature_slot_distributed_retry",
                    QueryFreshnessClass::Historical,
                );
                let query = build_query(&self.transaction_table, &settings_clause);
                let (fallback_opt, fallback_timings) =
                    fetch_single_transaction_row(&self.client, &query).await?;
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
            gsfa_table,
            gsfa_bucket_modulus,
            token_owner_table,
            token_owner_bucket_modulus: self.token_owner_bucket_modulus(),
            signatures_table: &self.signature_statuses_table,
            signature_bucket_modulus: self.signatures_bucket_modulus(),
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

        for shard in topology.shards.iter().cloned() {
            let local_table = local_table.clone();
            let fanout_sem = fanout_sem.clone();
            let settings_clause = settings_clause.clone();
            let hot_query = query.clone();

            join_set.spawn(async move {
                let _permit = fanout_sem.acquire().await.ok();
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
            });
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
            merge_hot_transactions_for_address_records(records, query.sort_order, query.limit);
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
            gsfa_table,
            gsfa_bucket_modulus,
            token_owner_table,
            token_owner_bucket_modulus: self.token_owner_bucket_modulus(),
            signatures_table: &self.signature_statuses_table,
            signature_bucket_modulus: self.signatures_bucket_modulus(),
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
        let mut cursor = match shard.http_client.query(&query_sql).fetch::<QueryResult>() {
            Ok(cursor) => cursor,
            Err(err) => {
                crate::metrics::clickhouse_transport_fallback(
                    "get_transactions_for_address_signatures_local_http",
                    "http",
                    "distributed",
                    "query_init",
                );
                tracing::warn!(
                    "Shard {}:{} HTTP query failed; falling back to distributed table: {}",
                    shard.host,
                    shard.tcp_port,
                    err
                );
                return Ok(None);
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
                    crate::metrics::clickhouse_transport_fallback(
                        "get_transactions_for_address_signatures_local_http",
                        "http",
                        "distributed",
                        "stream_error",
                    );
                    tracing::warn!(
                        "Shard {}:{} HTTP query stream failed; falling back to distributed table: {}",
                        shard.host,
                        shard.tcp_port,
                        err
                    );
                    return Ok(None);
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

        for shard in topology.shards.iter().cloned() {
            let local_table = local_table.clone();
            let settings_clause = settings_clause.clone();
            let fanout_sem = fanout_sem.clone();
            let hot_query = query.clone();

            join_set.spawn(async move {
                let _permit = fanout_sem.acquire().await.ok();
                let timed = tokio::time::timeout(query_timeout, async {
                    let query_sql = build_transactions_for_address_hot_query(
                        local_table.as_ref(),
                        gsfa_bucket_modulus,
                        &hot_query,
                        settings_clause.as_ref(),
                    )?;
                    let start = Instant::now();
                    let mut cursor = shard
                        .http_client
                        .query(&query_sql)
                        .fetch::<TransactionsForAddressQueryRow>()
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
            });
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
            merge_hot_transactions_for_address_records(records, query.sort_order, query.limit);
        timings.rows_returned = records.len() as u64;
        Ok((records, timings))
    }

    async fn try_get_transactions_by_slot_signatures_local(
        &self,
        topology: &ShardTopology,
        local_table: &str,
        pairs: &[(u64, String)],
        version_filter: &str,
    ) -> ProcessingResult<Option<(Vec<StoredTransactionRecord>, QueryTimings)>> {
        let local_table: std::sync::Arc<str> = local_table.to_string().into();
        let version_filter: std::sync::Arc<str> = version_filter.to_string().into();
        let fanout_sem = self.fanout_sem.clone();
        let query_timeout = self.query_timeout;
        let in_clause_chunk = self.in_clause_chunk;
        let settings_clause: std::sync::Arc<str> = topology
            .settings_clause(
                "get_transactions_by_slot_signatures_local_http",
                QueryFreshnessClass::Historical,
            )
            .into();

        let mut per_shard: Vec<Vec<(u64, String)>> = vec![Vec::new(); topology.shards.len()];
        for (slot, literal) in pairs {
            let shard_idx = topology.shard_index_for_hash(slot / SLOT_SHARD_DIVISOR);
            per_shard[shard_idx].push((*slot, literal.clone()));
        }

        let mut join_set = JoinSet::new();
        for (idx, shard_pairs) in per_shard.into_iter().enumerate() {
            if shard_pairs.is_empty() {
                continue;
            }
            let shard = topology.shards[idx].clone();
            let local_table = local_table.clone();
            let version_filter = version_filter.clone();
            let fanout_sem = fanout_sem.clone();
            let settings_clause = settings_clause.clone();

            join_set.spawn(async move {
                let _permit = fanout_sem.acquire().await.ok();
                let query = build_transactions_by_slot_signatures_query(
                    local_table.as_ref(),
                    &shard_pairs,
                    version_filter.as_ref(),
                    settings_clause.as_ref(),
                    in_clause_chunk,
                );

                let timed = tokio::time::timeout(query_timeout, async {
                    let start = Instant::now();
                    let mut cursor = shard
                        .http_client
                        .query(&query)
                        .fetch::<TransactionRow>()
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
                    Ok(result) => result.map_err(|e| (shard.host.clone(), shard.tcp_port, e)),
                    Err(_) => {
                        crate::metrics::clickhouse_timeout(
                            "get_transactions_by_slot_signatures_local",
                        );
                        Err((
                            shard.host.clone(),
                            shard.tcp_port,
                            ProcessingError::timeout_msg("Shard-local transaction fetch timed out"),
                        ))
                    }
                }
            });
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
    use super::*;

    fn normalize_sql(sql: &str) -> String {
        sql.split_whitespace().collect::<Vec<_>>().join(" ")
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
}
