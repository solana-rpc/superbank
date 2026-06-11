// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::cmp::Ordering;
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ch_cityhash102::cityhash64;
use serde::Deserialize;
use solana_sdk::pubkey::Pubkey;
use tokio::task::JoinSet;

use crate::processing::{ProcessingError, ProcessingResult};

use super::QueryFreshnessClass;
use super::client::{ClickHouseClient, execute_shard_tcp_query_block};
use super::queries::{build_hot_position_pagination_clauses, build_pagination_clauses};
use super::sharding::{ShardTarget, ShardTopology};
use super::types::{QueryTimings, SignatureRecord, SlotBoundary};
use super::util::{
    GsfaFallbackMode, annotate_query, annotate_required_query, append_max_execution_time_setting,
    format_gsfa_memo, gsfa_fallback_mode, http_query_with_id, parse_err_json, pubkey_literal,
    transient_shard_local_error_reason,
};

#[derive(Clone)]
pub(crate) struct GsfaShardRouter {
    pub(crate) local_table: String,
    pub(crate) topology: Arc<ShardTopology>,
    pub(crate) query_timeout: Duration,
}

fn build_gsfa_signatures_query(
    with_clause: &str,
    gsfa_table: &str,
    addr_bucket: u64,
    address_literal: &str,
    where_clause: &str,
    limit: u64,
    settings_clause: &str,
) -> String {
    format!(
        "{with_clause}SELECT
            base58Encode(signature) as signature,
            slot,
            slot_idx,
            err,
            memo,
            block_time
         FROM
         (
             SELECT
                 signature,
                 slot,
                 slot_idx,
                 err,
                 memo,
                 block_time
             FROM {gsfa_table}
             PREWHERE
                 addr_bucket = {addr_bucket}
                 AND address = {address_literal}
             WHERE {where_clause}
             ORDER BY slot DESC, slot_idx DESC, signature
             LIMIT {limit}
             {settings_clause}
         )",
        with_clause = with_clause,
        gsfa_table = gsfa_table,
        addr_bucket = addr_bucket,
        address_literal = address_literal,
        where_clause = where_clause,
        limit = limit,
        settings_clause = settings_clause
    )
}

#[derive(Deserialize, clickhouse::Row)]
struct GsfaSignatureQueryRow {
    signature: String,
    slot: u64,
    slot_idx: u32,
    err: Option<String>,
    memo: Option<String>,
    block_time: Option<i64>,
}

struct HotGsfaQuerySpec {
    local_table: String,
    addr_bucket: u64,
    address_literal: String,
    with_clause: String,
    where_clause: String,
    limit: u64,
}

fn map_gsfa_signature_row(row: GsfaSignatureQueryRow) -> SignatureRecord {
    let parsed_err = row
        .err
        .and_then(|err_str| parse_err_json(&row.signature, err_str));
    SignatureRecord {
        signature: row.signature,
        slot: row.slot,
        slot_idx: row.slot_idx,
        err: parsed_err,
        memo: format_gsfa_memo(row.memo),
        block_time: row.block_time,
    }
}

fn compare_signature_records(a: &SignatureRecord, b: &SignatureRecord) -> Ordering {
    b.slot
        .cmp(&a.slot)
        .then_with(|| b.slot_idx.cmp(&a.slot_idx))
        .then_with(|| a.signature.cmp(&b.signature))
}

fn merge_hot_signature_records(
    mut records: Vec<SignatureRecord>,
    limit: u64,
) -> Vec<SignatureRecord> {
    records.sort_unstable_by(compare_signature_records);

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

impl ClickHouseClient {
    pub(crate) fn is_gsfa_hot_address(&self, pubkey: &Pubkey) -> bool {
        self.gsfa_hot_pubkeys.contains(pubkey)
    }

    pub(crate) fn should_use_gsfa_hot_fanout(&self, pubkey: &Pubkey) -> bool {
        self.is_gsfa_hot_address(pubkey) && self.shard_topology.is_some()
    }

    pub(crate) fn should_use_gsfa_shard_routing(&self, pubkey: &Pubkey) -> bool {
        !self.is_gsfa_hot_address(pubkey)
    }

    pub(crate) fn gsfa_table_for_address<'a>(&'a self, pubkey: &Pubkey) -> &'a str {
        if self.is_gsfa_hot_address(pubkey) {
            &self.gsfa_hot_table
        } else {
            &self.gsfa_table
        }
    }

    pub(crate) fn gsfa_local_table<'a>(&'a self, router: &'a GsfaShardRouter) -> &'a str {
        &router.local_table
    }

    pub(crate) async fn initialize_gsfa_hot_addresses(&mut self) {
        self.gsfa_hot_pubkeys.clear();

        if self.gsfa_hot_addresses.is_empty() {
            return;
        }

        let mut candidates = Vec::new();
        for address in &self.gsfa_hot_addresses {
            let address = address.trim();
            if address.is_empty() {
                tracing::warn!("Empty GSFA hot address provided; ignoring");
                continue;
            }
            match Pubkey::from_str(address) {
                Ok(pubkey) => candidates.push(pubkey),
                Err(err) => {
                    tracing::warn!(
                        "Invalid GSFA hot address '{}'; ignoring (error: {})",
                        address,
                        err
                    );
                }
            }
        }

        if candidates.is_empty() {
            return;
        }

        let hot_table = &self.gsfa_hot_table;
        let total_rows = match self
            .client
            .query(&format!("SELECT COUNT(*) FROM {}", hot_table))
            .fetch_one::<u64>()
            .await
        {
            Ok(count) => count,
            Err(err) => {
                tracing::warn!(
                    "GSFA hot table '{}' unavailable; hot routing disabled (error: {})",
                    hot_table,
                    err
                );
                return;
            }
        };

        if total_rows == 0 {
            tracing::warn!(
                "GSFA hot table '{}' is empty; hot routing disabled",
                hot_table
            );
            return;
        }

        let mut active = HashSet::with_capacity(candidates.len());
        for pubkey in candidates {
            let address = pubkey.to_string();
            let address_literal = pubkey_literal(&pubkey);
            let addr_bucket = cityhash64(pubkey.as_ref()) % self.bucket_moduli.gsfa_hot;
            let query = format!(
                "SELECT COUNT(*) FROM {hot_table} \
                 PREWHERE addr_bucket = {addr_bucket} AND address = {address_literal}"
            );
            match self.client.query(&query).fetch_one::<u64>().await {
                Ok(count) if count > 0 => {
                    active.insert(pubkey);
                }
                Ok(_) => {
                    tracing::warn!(
                        "GSFA hot address '{}' has no rows in {}; using standard table",
                        address,
                        hot_table
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        "Failed to verify GSFA hot address '{}' in {}: {}; using standard table",
                        address,
                        hot_table,
                        err
                    );
                }
            }
        }

        if !active.is_empty() {
            let mut enabled: Vec<String> = active.iter().map(|key| key.to_string()).collect();
            enabled.sort();
            tracing::info!(
                "GSFA hot routing enabled for {} addresses: {}",
                enabled.len(),
                enabled.join(", ")
            );
        }

        self.gsfa_hot_pubkeys = active;
    }

    async fn maybe_apply_gsfa_fallback(
        &self,
        address: &str,
        limit: u64,
        before_pos: Option<SlotBoundary>,
        until_pos: Option<SlotBoundary>,
        records: Vec<SignatureRecord>,
        timings: QueryTimings,
    ) -> ProcessingResult<(Vec<SignatureRecord>, QueryTimings)> {
        let mode = gsfa_fallback_mode();
        let should_fallback = match mode {
            GsfaFallbackMode::Disabled => false,
            GsfaFallbackMode::EmptyOnly => records.is_empty(),
            GsfaFallbackMode::Incomplete => records.len() < limit as usize,
        };
        if !should_fallback {
            return Ok((records, timings));
        }

        let (fallback_records, fallback_timings) = self
            .get_signatures_for_address_fallback(address, limit, before_pos, until_pos)
            .await?;
        let mut combined = timings;
        combined.add(fallback_timings);
        if !fallback_records.is_empty() {
            return Ok((fallback_records, combined));
        }

        Ok((records, combined))
    }

    async fn get_hot_signatures_for_address_with_positions(
        &self,
        address: &str,
        pubkey: &Pubkey,
        limit: u64,
        before_pos: Option<SlotBoundary>,
        until_pos: Option<SlotBoundary>,
    ) -> ProcessingResult<(Vec<SignatureRecord>, QueryTimings)> {
        let topology = self.hot_shard_topology()?.clone();
        let hot_table = self.gsfa_hot_local_table.clone();
        let addr_bucket = cityhash64(pubkey.as_ref()) % self.bucket_moduli.gsfa_hot;
        let address_literal = pubkey_literal(pubkey);
        let (with_clause, where_clause) =
            build_hot_position_pagination_clauses(before_pos, until_pos);
        let spec = HotGsfaQuerySpec {
            local_table: hot_table,
            addr_bucket,
            address_literal,
            with_clause,
            where_clause,
            limit,
        };

        let (records, mut timings) = if self.scope_shard_direct() && self.transport_tcp() {
            match self
                .get_hot_signatures_for_address_with_positions_tcp(&topology, &spec)
                .await
            {
                Ok(result) => result,
                Err(err) => {
                    if let Some(reason) = transient_shard_local_error_reason(&err) {
                        crate::metrics::clickhouse_transport_fallback(
                            "get_signatures_for_address_hot_local_tcp",
                            "tcp",
                            "http",
                            reason,
                        );
                        tracing::warn!(
                            "Shard-local GSFA hot TCP query failed; falling back to HTTP: {}",
                            err
                        );
                        self.get_hot_signatures_for_address_with_positions_http(&topology, &spec)
                            .await?
                    } else {
                        return Err(err);
                    }
                }
            }
        } else {
            self.get_hot_signatures_for_address_with_positions_http(&topology, &spec)
                .await?
        };

        timings.rows_returned = records.len() as u64;
        self.maybe_apply_gsfa_fallback(address, limit, before_pos, until_pos, records, timings)
            .await
    }

    async fn get_hot_signatures_for_address_with_positions_tcp(
        &self,
        topology: &ShardTopology,
        spec: &HotGsfaQuerySpec,
    ) -> ProcessingResult<(Vec<SignatureRecord>, QueryTimings)> {
        let local_table: Arc<str> = spec.local_table.clone().into();
        let address_literal: Arc<str> = spec.address_literal.clone().into();
        let with_clause: Arc<str> = spec.with_clause.clone().into();
        let where_clause: Arc<str> = spec.where_clause.clone().into();
        let addr_bucket = spec.addr_bucket;
        let limit = spec.limit;
        let fanout_sem = self.fanout_sem.clone();
        let query_timeout = self.shard_tcp_query_timeout();
        let settings_clause: Arc<str> = append_max_execution_time_setting(
            &topology.settings_clause(
                "get_signatures_for_address_hot_local_tcp",
                QueryFreshnessClass::Historical,
            ),
            query_timeout,
        )
        .into();

        let mut join_set = JoinSet::new();
        for shard in topology.shards.iter().cloned() {
            let local_table = local_table.clone();
            let address_literal = address_literal.clone();
            let with_clause = with_clause.clone();
            let where_clause = where_clause.clone();
            let settings_clause = settings_clause.clone();
            let fanout_sem = fanout_sem.clone();

            join_set.spawn(async move {
                let _permit = fanout_sem.acquire().await.ok();
                let query = build_gsfa_signatures_query(
                    with_clause.as_ref(),
                    local_table.as_ref(),
                    addr_bucket,
                    address_literal.as_ref(),
                    where_clause.as_ref(),
                    limit,
                    settings_clause.as_ref(),
                );

                match execute_shard_tcp_query_block(
                    shard.clone(),
                    query_timeout,
                    "get_signatures_for_address_hot_local_tcp",
                    "gsfa_hot_signatures_local_tcp",
                    query,
                )
                .await
                {
                    Ok((block, timings)) => {
                        let mut records = Vec::new();
                        for row in block.rows() {
                            let query_row = GsfaSignatureQueryRow {
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
                            records.push(map_gsfa_signature_row(query_row));
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
                    let context =
                        format!("Shard-local GSFA hot TCP query failed on {host}:{port}: {err}");
                    tracing::warn!("{context}");
                    return Err(ProcessingError::database(context, err));
                }
                Err(err) => {
                    return Err(ProcessingError::database_msg(format!(
                        "Shard-local GSFA hot TCP task failed: {err}"
                    )));
                }
            }
        }

        Ok((merge_hot_signature_records(records, limit), timings))
    }

    async fn get_hot_signatures_for_address_with_positions_http(
        &self,
        topology: &ShardTopology,
        spec: &HotGsfaQuerySpec,
    ) -> ProcessingResult<(Vec<SignatureRecord>, QueryTimings)> {
        let local_table: Arc<str> = spec.local_table.clone().into();
        let address_literal: Arc<str> = spec.address_literal.clone().into();
        let with_clause: Arc<str> = spec.with_clause.clone().into();
        let where_clause: Arc<str> = spec.where_clause.clone().into();
        let addr_bucket = spec.addr_bucket;
        let limit = spec.limit;
        let settings_clause: Arc<str> = topology
            .settings_clause(
                "get_signatures_for_address_hot_local_http",
                QueryFreshnessClass::Historical,
            )
            .into();
        let fanout_sem = self.fanout_sem.clone();
        let query_timeout = self.query_timeout;

        let mut join_set = JoinSet::new();
        for shard in topology.shards.iter().cloned() {
            let local_table = local_table.clone();
            let address_literal = address_literal.clone();
            let with_clause = with_clause.clone();
            let where_clause = where_clause.clone();
            let settings_clause = settings_clause.clone();
            let fanout_sem = fanout_sem.clone();

            join_set.spawn(async move {
                let _permit = fanout_sem.acquire().await.ok();
                let query = build_gsfa_signatures_query(
                    with_clause.as_ref(),
                    local_table.as_ref(),
                    addr_bucket,
                    address_literal.as_ref(),
                    where_clause.as_ref(),
                    limit,
                    settings_clause.as_ref(),
                );
                let (query, query_id) = annotate_query(query, "gsfa_hot_signatures_local_http");

                let timed = tokio::time::timeout(query_timeout, async {
                    let start = Instant::now();
                    let mut cursor = http_query_with_id(&shard.http_client, &query, query_id)
                        .fetch::<GsfaSignatureQueryRow>()
                        .map_err(|e| ProcessingError::database(e.to_string(), e))?;

                    let mut records = Vec::new();
                    while let Some(row) = cursor
                        .next()
                        .await
                        .map_err(|e| ProcessingError::database(e.to_string(), e))?
                    {
                        records.push(map_gsfa_signature_row(row));
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
                            "get_signatures_for_address_hot_local_http",
                        );
                        Err((
                            shard.host.clone(),
                            shard.tcp_port,
                            ProcessingError::timeout_msg(
                                "Shard-local GSFA hot HTTP query timed out",
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
                        "Shard-local GSFA hot HTTP query failed on {host}:{port}: {err}"
                    )));
                }
                Err(err) => {
                    return Err(ProcessingError::database_msg(format!(
                        "Shard-local GSFA hot HTTP task failed: {err}"
                    )));
                }
            }
        }

        Ok((merge_hot_signature_records(records, limit), timings))
    }

    pub(crate) async fn get_signatures_for_address_with_positions(
        &self,
        address: &str,
        limit: u64,
        before_pos: Option<SlotBoundary>,
        until_pos: Option<SlotBoundary>,
    ) -> ProcessingResult<(Vec<SignatureRecord>, QueryTimings)> {
        self.with_timeout("get_signatures_for_address_with_positions", async {
            let pubkey = Pubkey::from_str(address)
                .map_err(|e| ProcessingError::deserialization("Invalid address", e))?;

            if self.should_use_gsfa_hot_fanout(&pubkey) {
                return self
                    .get_hot_signatures_for_address_with_positions(
                        address, &pubkey, limit, before_pos, until_pos,
                    )
                    .await;
            }

            if self.should_use_gsfa_shard_routing(&pubkey)
                && let Some(router) = &self.gsfa_router
            {
                let local_table = self.gsfa_local_table(router);
                let mut allow_local_http = self.transport_http();

                if self.transport_tcp() {
                    match router
                        .get_signatures_for_address_with_options(
                            local_table,
                            self.bucket_moduli.gsfa,
                            &pubkey,
                            limit,
                            before_pos,
                            until_pos,
                        )
                        .await
                    {
                        Ok((records, timings)) => {
                            return self
                                .maybe_apply_gsfa_fallback(
                                    address, limit, before_pos, until_pos, records, timings,
                                )
                                .await;
                        }
                        Err(err) => {
                            if let Some(reason) = transient_shard_local_error_reason(&err) {
                                crate::metrics::clickhouse_transport_fallback(
                                    "get_signatures_for_address_with_options_local_tcp",
                                    "tcp",
                                    "http",
                                    reason,
                                );
                                tracing::warn!(
                                    "GSFA shard-local TCP query failed; falling back to HTTP: {}",
                                    err
                                );
                                allow_local_http = true;
                            } else {
                                return Err(err);
                            }
                        }
                    }
                }

                if allow_local_http {
                    match router
                        .get_signatures_for_address_with_options_http(
                            local_table,
                            self.bucket_moduli.gsfa,
                            &pubkey,
                            limit,
                            before_pos,
                            until_pos,
                        )
                        .await
                    {
                        Ok((records, timings)) => {
                            return self
                                .maybe_apply_gsfa_fallback(
                                    address, limit, before_pos, until_pos, records, timings,
                                )
                                .await;
                        }
                        Err(err) => {
                            crate::metrics::clickhouse_transport_fallback(
                                "get_signatures_for_address_with_options_local_http",
                                "http",
                                "distributed",
                                "query_error",
                            );
                            tracing::warn!(
                                "GSFA shard-local HTTP query failed; falling back to distributed query: {}",
                                err
                            );
                        }
                    }
                }
            }

            let address_literal = pubkey_literal(&pubkey);
            let gsfa_table = self.gsfa_table_for_address(&pubkey);
            let addr_bucket =
                cityhash64(pubkey.as_ref()) % self.gsfa_bucket_modulus_for_address(&pubkey);
            let (with_clause, where_clause) = build_pagination_clauses(before_pos, until_pos);

            let settings_clause = self.select_settings_clause(
                "get_signatures_for_address_with_positions",
                QueryFreshnessClass::Historical,
            );
            let query = build_gsfa_signatures_query(
                &with_clause,
                gsfa_table,
                addr_bucket,
                &address_literal,
                &where_clause,
                limit,
                &settings_clause,
            );
            let (query, query_id) = annotate_query(query, "gsfa_signatures");

            let start = Instant::now();

            let mut cursor = http_query_with_id(&self.client, &query, query_id)
                .fetch::<GsfaSignatureQueryRow>()
                .map_err(|e| ProcessingError::database(e.to_string(), e))?;

            let mut results = Vec::new();
            while let Some(row) = cursor
                .next()
                .await
                .map_err(|e| ProcessingError::database(e.to_string(), e))?
            {
                results.push(row);
            }

            let signature_records = results
                .into_iter()
                .map(map_gsfa_signature_row)
                .collect::<Vec<_>>();

            let timings = QueryTimings {
                elapsed_ms: start.elapsed().as_millis() as u64,
                received_bytes: cursor.received_bytes(),
                decoded_bytes: cursor.decoded_bytes(),
                rows_read: Some(0),
                rows_read_unknown: true,
                rows_returned: signature_records.len() as u64,
            };

            self.maybe_apply_gsfa_fallback(
                address,
                limit,
                before_pos,
                until_pos,
                signature_records,
                timings,
            )
            .await
        })
        .await
    }

    async fn get_signatures_for_address_fallback(
        &self,
        address: &str,
        limit: u64,
        before: Option<SlotBoundary>,
        until: Option<SlotBoundary>,
    ) -> ProcessingResult<(Vec<SignatureRecord>, QueryTimings)> {
        let pubkey = Pubkey::from_str(address)
            .map_err(|e| ProcessingError::deserialization("Invalid address", e))?;

        let address_literal = pubkey_literal(&pubkey);
        let (pagination_with, where_clause) = build_pagination_clauses(before, until);

        let memo_with = "memo_program_ids AS [\
            CAST(base58Decode('Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo') AS FixedString(32)),\
            CAST(base58Decode('MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr') AS FixedString(32))\
        ]";

        let pagination_with = pagination_with.strip_prefix("WITH ").unwrap_or("").trim();
        let with_clause = if pagination_with.is_empty() {
            format!("WITH {memo_with} ")
        } else {
            format!("WITH {memo_with}, {pagination_with} ")
        };

        let settings_clause = self.select_settings_clause_with_condition_cache(
            "get_signatures_for_address_fallback",
            QueryFreshnessClass::Historical,
        );
        let query = format!(
            "{with_clause}SELECT
                base58Encode(signature) as signature,
                slot,
                slot_idx,
                err,
                memo,
                block_time
             FROM (
                 WITH arrayConcat(tx_account_keys, meta_loaded_addresses_writable, meta_loaded_addresses_readonly) AS account_keys_all
                 SELECT
                     signature,
                     slot,
                     slot_idx,
                     block_time,
                     if(meta_status_ok = 1, NULL, meta_err) AS err,
                     nullIf(
                         arrayStringConcat(
                             arrayMap(x -> x.2,
                                 arrayFilter(
                                     x -> has(memo_program_ids, x.1) AND isValidUTF8(x.2),
                                     arrayZip(
                                         arrayMap(idx -> arrayElement(account_keys_all, idx + 1), tx_instructions_program_id_index),
                                         tx_instructions_data
                                     )
                                 )
                             ),
                             '; '
                         ),
                         ''
                     ) AS memo
                 FROM {transaction_table}
                 WHERE has(account_keys_all, {address_literal})
             )
             WHERE {where_clause}
             ORDER BY slot DESC, slot_idx DESC, signature
             LIMIT {limit}
             {settings_clause}",
            with_clause = with_clause,
            transaction_table = self.transaction_table,
            address_literal = address_literal,
            where_clause = where_clause,
            limit = limit,
            settings_clause = settings_clause
        );
        // This fallback is a full transaction-table scan, so register a KILL-on-drop cleanup:
        // if the caller times out or cancels, the heavy server-side query is aborted instead of
        // running to `max_execution_time`. Cleanup dispatch is concurrency-capped (see
        // kill_query_semaphore), so it cannot itself storm ClickHouse.
        let (query, query_id) = annotate_required_query(query, "gsfa_fallback");
        let mut cleanup =
            self.http_query_cleanup("get_signatures_for_address_fallback", query_id.clone());

        let start = Instant::now();
        let mut cursor = http_query_with_id(&self.client, &query, Some(query_id))
            .fetch::<GsfaSignatureQueryRow>()
            .map_err(|e| ProcessingError::database(e.to_string(), e))?;

        let mut results = Vec::new();
        while let Some(row) = cursor
            .next()
            .await
            .map_err(|e| ProcessingError::database(e.to_string(), e))?
        {
            results.push(row);
        }

        let signature_records = results
            .into_iter()
            .map(map_gsfa_signature_row)
            .collect::<Vec<_>>();

        let timings = QueryTimings {
            elapsed_ms: start.elapsed().as_millis() as u64,
            received_bytes: cursor.received_bytes(),
            decoded_bytes: cursor.decoded_bytes(),
            rows_read: Some(0),
            rows_read_unknown: true,
            rows_returned: signature_records.len() as u64,
        };

        cleanup.disarm();
        Ok((signature_records, timings))
    }
}

impl GsfaShardRouter {
    pub(crate) fn shard_for_pubkey(&self, pubkey: &Pubkey) -> &ShardTarget {
        let hash = cityhash64(pubkey.as_ref());
        self.topology.shard_for_hash(hash)
    }

    pub(crate) async fn get_signatures_for_address_with_options(
        &self,
        gsfa_table: &str,
        gsfa_bucket_modulus: u64,
        pubkey: &Pubkey,
        limit: u64,
        before: Option<SlotBoundary>,
        until: Option<SlotBoundary>,
    ) -> ProcessingResult<(Vec<SignatureRecord>, QueryTimings)> {
        let shard = self.shard_for_pubkey(pubkey);

        let address_literal = pubkey_literal(pubkey);
        let addr_bucket = cityhash64(pubkey.as_ref()) % gsfa_bucket_modulus;
        let (with_clause, where_clause) = build_pagination_clauses(before, until);

        let settings_clause = append_max_execution_time_setting(
            &self.topology.settings_clause(
                "get_signatures_for_address_with_options_local_tcp",
                QueryFreshnessClass::Historical,
            ),
            self.query_timeout,
        );
        let query = build_gsfa_signatures_query(
            &with_clause,
            gsfa_table,
            addr_bucket,
            &address_literal,
            &where_clause,
            limit,
            &settings_clause,
        );
        let (block, timings) = execute_shard_tcp_query_block(
            shard.clone(),
            self.query_timeout,
            "get_signatures_for_address_with_options_local_tcp",
            "gsfa_signatures_local",
            query,
        )
        .await?;

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
            results.push(SignatureRecord {
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

        Ok((results, timings))
    }

    pub(crate) async fn get_signatures_for_address_with_options_http(
        &self,
        gsfa_table: &str,
        gsfa_bucket_modulus: u64,
        pubkey: &Pubkey,
        limit: u64,
        before: Option<SlotBoundary>,
        until: Option<SlotBoundary>,
    ) -> ProcessingResult<(Vec<SignatureRecord>, QueryTimings)> {
        let shard = self.shard_for_pubkey(pubkey);

        let address_literal = pubkey_literal(pubkey);
        let addr_bucket = cityhash64(pubkey.as_ref()) % gsfa_bucket_modulus;
        let (with_clause, where_clause) = build_pagination_clauses(before, until);

        let settings_clause = self.topology.settings_clause(
            "get_signatures_for_address_with_options_local_http",
            QueryFreshnessClass::Historical,
        );
        let query = build_gsfa_signatures_query(
            &with_clause,
            gsfa_table,
            addr_bucket,
            &address_literal,
            &where_clause,
            limit,
            &settings_clause,
        );
        let (query, query_id) = annotate_query(query, "gsfa_signatures_local_http");

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
        let mut cursor = http_query_with_id(&shard.http_client, &query, query_id)
            .fetch::<QueryResult>()
            .map_err(|e| ProcessingError::database(e.to_string(), e))?;

        let mut results = Vec::new();
        while let Some(row) = cursor
            .next()
            .await
            .map_err(|e| ProcessingError::database(e.to_string(), e))?
        {
            let parsed_err = row
                .err
                .and_then(|err_str| parse_err_json(&row.signature, err_str));
            results.push(SignatureRecord {
                signature: row.signature,
                slot: row.slot,
                slot_idx: row.slot_idx,
                err: parsed_err,
                memo: format_gsfa_memo(row.memo),
                block_time: row.block_time,
            });
        }

        let timings = QueryTimings {
            elapsed_ms: start.elapsed().as_millis() as u64,
            received_bytes: cursor.received_bytes(),
            decoded_bytes: cursor.decoded_bytes(),
            rows_read: Some(0),
            rows_read_unknown: true,
            rows_returned: results.len() as u64,
        };

        Ok((results, timings))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clickhouse::{
        ClickHouseClientOptions, RoutingPolicy, RoutingScope, RoutingTransport, SignatureSlot,
    };

    fn normalize_sql(sql: &str) -> String {
        sql.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    fn test_client() -> ClickHouseClient {
        ClickHouseClient::new(
            "http://localhost:8123",
            "default",
            "default",
            "",
            ClickHouseClientOptions::new(
                RoutingPolicy {
                    transport: RoutingTransport::Http,
                    scope: RoutingScope::ShardDirect,
                },
                None,
                Vec::new(),
                "default.gsfa_hot".to_string(),
                "default.gsfa_hot_local".to_string(),
            ),
        )
    }

    #[test]
    fn gsfa_signatures_query_uses_inner_raw_signature_ordering() {
        let sql = build_gsfa_signatures_query(
            "",
            "default.gsfa_local",
            2,
            "toFixedString(unhex('ABCD'), 32)",
            "1",
            1000,
            "SETTINGS use_query_cache=1",
        );
        let sql = normalize_sql(&sql);

        assert!(sql.contains(
            "SELECT base58Encode(signature) as signature, slot, slot_idx, err, memo, block_time"
        ));
        assert!(sql.contains(
            "FROM ( SELECT signature, slot, slot_idx, err, memo, block_time FROM default.gsfa_local"
        ));
        assert!(sql.contains(
            "ORDER BY slot DESC, slot_idx DESC, signature LIMIT 1000 SETTINGS use_query_cache=1"
        ));
        assert!(!sql.contains("ORDER BY slot DESC, slot_idx DESC, base58Encode(signature)"));
    }

    #[test]
    fn gsfa_signatures_query_preserves_before_until_pagination_clause() {
        let (_, where_clause) = build_pagination_clauses(
            Some(SlotBoundary::Position(SignatureSlot {
                slot: 220584742,
                slot_idx: 286,
            })),
            Some(SlotBoundary::Position(SignatureSlot {
                slot: 220580000,
                slot_idx: 10,
            })),
        );

        let sql = build_gsfa_signatures_query(
            "",
            "default.gsfa_local",
            2,
            "toFixedString(unhex('ABCD'), 32)",
            &where_clause,
            1000,
            "",
        );
        let sql = normalize_sql(&sql);

        assert!(
            sql.contains(
                "WHERE (slot + toUInt64(0) < 220584742 OR (slot + toUInt64(0) = 220584742 AND slot_idx + toUInt32(0) < 286)) AND (slot + toUInt64(0) > 220580000 OR (slot + toUInt64(0) = 220580000 AND slot_idx + toUInt32(0) > 10))"
            )
        );
    }

    #[test]
    fn gsfa_signatures_query_supports_slot_boundaries() {
        let (_, where_clause) = build_pagination_clauses(
            Some(SlotBoundary::Slot(220584742)),
            Some(SlotBoundary::Slot(220580000)),
        );

        let sql = build_gsfa_signatures_query(
            "",
            "default.gsfa_local",
            2,
            "toFixedString(unhex('ABCD'), 32)",
            &where_clause,
            1000,
            "",
        );
        let sql = normalize_sql(&sql);

        assert!(sql.contains("WHERE slot < 220584742 AND slot > 220580000"));
        assert!(!sql.contains("slot_idx + toUInt32"));
    }

    #[test]
    fn gsfa_signatures_query_keeps_default_where_true_clause() {
        let (_, where_clause) = build_pagination_clauses(None, None);
        let sql = build_gsfa_signatures_query(
            "",
            "default.gsfa_local",
            2,
            "toFixedString(unhex('ABCD'), 32)",
            &where_clause,
            500,
            "",
        );
        let sql = normalize_sql(&sql);

        assert!(sql.contains("WHERE 1"));
        assert!(sql.contains("LIMIT 500"));
    }

    #[test]
    fn gsfa_signatures_query_regression_same_slot_before_boundary_clause() {
        // Regression coverage for the incident where:
        // - before signature 2xC1... resolved to (slot=400179920, slot_idx=678)
        // - signature 3gY9... at the same slot with slot_idx=673 was skipped.
        // The predicate must always preserve the "same slot, smaller idx" branch and
        // use non-identity arithmetic casts to avoid reverse-key analyzer regressions.
        let (_, where_clause) = build_pagination_clauses(
            Some(SlotBoundary::Position(SignatureSlot {
                slot: 400_179_920,
                slot_idx: 678,
            })),
            None,
        );
        let sql = build_gsfa_signatures_query(
            "",
            "default.gsfa_local",
            2,
            "toFixedString(unhex('ABCD'), 32)",
            &where_clause,
            1000,
            "",
        );
        let sql = normalize_sql(&sql);

        assert!(
            sql.contains(
                "WHERE (slot + toUInt64(0) < 400179920 OR (slot + toUInt64(0) = 400179920 AND slot_idx + toUInt32(0) < 678))"
            )
        );
        assert!(!sql.contains("WHERE (slot < 400179920 OR (slot = 400179920 AND slot_idx < 678))"));
    }

    #[test]
    fn gsfa_signatures_query_regression_same_slot_until_boundary_clause() {
        // Regression coverage for the "until"-only pagination case at the same slot.
        // The predicate must preserve the "same slot, larger idx" branch and use
        // non-identity arithmetic casts to avoid reverse-key analyzer regressions.
        let (_, where_clause) = build_pagination_clauses(
            None,
            Some(SlotBoundary::Position(SignatureSlot {
                slot: 400_179_920,
                slot_idx: 678,
            })),
        );
        let sql = build_gsfa_signatures_query(
            "",
            "default.gsfa_local",
            2,
            "toFixedString(unhex('ABCD'), 32)",
            &where_clause,
            1000,
            "",
        );
        let sql = normalize_sql(&sql);

        assert!(
            sql.contains(
                "WHERE (slot + toUInt64(0) > 400179920 OR (slot + toUInt64(0) = 400179920 AND slot_idx + toUInt32(0) > 678))"
            )
        );
        assert!(!sql.contains("WHERE (slot > 400179920 OR (slot = 400179920 AND slot_idx > 678))"));
    }

    #[test]
    fn hot_addresses_use_distributed_gsfa_table_and_skip_shard_routing() {
        let mut client = test_client();
        let hot_pubkey = Pubkey::new_from_array([7; 32]);
        client.gsfa_hot_pubkeys.insert(hot_pubkey);

        assert_eq!(
            client.gsfa_table_for_address(&hot_pubkey),
            "default.gsfa_hot"
        );
        assert!(!client.should_use_gsfa_shard_routing(&hot_pubkey));
    }

    #[test]
    fn hot_addresses_without_topology_use_distributed_hot_table_not_fanout() {
        let mut client = test_client();
        let hot_pubkey = Pubkey::new_from_array([7; 32]);
        client.gsfa_hot_pubkeys.insert(hot_pubkey);

        assert_eq!(
            client.gsfa_table_for_address(&hot_pubkey),
            "default.gsfa_hot"
        );
        assert!(!client.should_use_gsfa_hot_fanout(&hot_pubkey));
        assert!(!client.should_use_gsfa_shard_routing(&hot_pubkey));
    }

    #[test]
    fn non_hot_addresses_keep_regular_gsfa_table_and_shard_routing() {
        let client = test_client();
        let cold_pubkey = Pubkey::new_from_array([9; 32]);

        assert_eq!(client.gsfa_table_for_address(&cold_pubkey), "default.gsfa");
        assert!(client.should_use_gsfa_shard_routing(&cold_pubkey));
    }
}
