// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::collections::BTreeMap;
use std::time::Instant;

use ch_cityhash102::cityhash64;
use serde::Deserialize;
use tokio::task::JoinSet;

use crate::processing::{ProcessingError, ProcessingResult};

use super::QueryFreshnessClass;
use super::cache::{CacheStart, SignatureBytes};
use super::client::{ClickHouseClient, execute_shard_tcp_query_block};
use super::sharding::ShardTopology;
use super::types::{QueryTimings, SignatureSlot, SignatureStatusRecord};
use super::util::{
    annotate_query, append_max_execution_time_setting, http_query_with_id, parse_err_json,
    transient_shard_local_error_reason,
};

fn build_signature_filter(
    bucketed_signatures: BTreeMap<u64, Vec<String>>,
    in_clause_chunk: usize,
) -> String {
    let chunk_size = in_clause_chunk.max(1);
    let mut bucket_clauses = Vec::with_capacity(bucketed_signatures.len());
    for (bucket, literals) in bucketed_signatures {
        if literals.len() == 1 {
            bucket_clauses.push(format!(
                "(sig_bucket = {bucket} AND signature = {})",
                literals[0]
            ));
            continue;
        }

        for chunk in literals.chunks(chunk_size) {
            if chunk.len() == 1 {
                bucket_clauses.push(format!(
                    "(sig_bucket = {bucket} AND signature = {})",
                    chunk[0]
                ));
            } else {
                bucket_clauses.push(format!(
                    "(sig_bucket = {bucket} AND signature IN ({}))",
                    chunk.join(",")
                ));
            }
        }
    }
    bucket_clauses.join(" OR ")
}

impl ClickHouseClient {
    pub(crate) async fn get_signature_slot(
        &self,
        signature: &str,
    ) -> ProcessingResult<(Option<SignatureSlot>, QueryTimings)> {
        let signature_bytes = bs58::decode(signature)
            .into_vec()
            .map_err(|e| ProcessingError::deserialization("Invalid signature", e))?;
        if signature_bytes.len() != 64 {
            return Err(ProcessingError::deserialization_msg(format!(
                "Invalid signature length {} (expected 64 bytes)",
                signature_bytes.len()
            )));
        }

        let signature_bytes: SignatureBytes =
            signature_bytes.as_slice().try_into().map_err(|_| {
                ProcessingError::deserialization_msg("Invalid signature length".to_string())
            })?;

        self.get_signature_slot_by_signature_bytes(signature_bytes)
            .await
    }

    pub(crate) async fn get_signature_slot_by_signature_bytes(
        &self,
        signature_bytes: SignatureBytes,
    ) -> ProcessingResult<(Option<SignatureSlot>, QueryTimings)> {
        let signature_hash = cityhash64(signature_bytes.as_ref());
        let sig_bucket = signature_hash % self.signatures_bucket_modulus();
        let signature_hex = hex::encode(signature_bytes.as_ref()).to_uppercase();
        let signature_literal = format!("toFixedString(unhex('{signature_hex}'), 64)");

        let call_start = Instant::now();
        let mut waited = false;

        loop {
            match self
                .signature_slot_cache
                .get_or_start(signature_bytes)
                .await
            {
                CacheStart::Hit(value) => {
                    let timings = if waited {
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
                    return Ok((value, timings));
                }
                CacheStart::Wait(wait) => {
                    waited = true;
                    wait.await;
                }
                CacheStart::Leader(leader) => {
                    let result = self
                        .get_signature_slot_by_signature_uncached(
                            signature_hash,
                            sig_bucket,
                            &signature_literal,
                        )
                        .await;

                    match result {
                        Ok((value, timings)) => {
                            leader.finish(value).await;
                            return Ok((value, timings));
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

    async fn get_signature_slot_by_signature_uncached(
        &self,
        signature_hash: u64,
        sig_bucket: u64,
        signature_literal: &str,
    ) -> ProcessingResult<(Option<SignatureSlot>, QueryTimings)> {
        self.with_http_query_timeout("get_signature_slot", async {
            #[derive(Deserialize, clickhouse::Row)]
            struct SignatureSlotRow {
                slot: u64,
                slot_idx: u32,
            }

            if self.scope_shard_direct()
                && let (Some(topology), Some(local_table)) =
                    (&self.shard_topology, &self.signatures_local_table)
            {
                let mut allow_local_http = self.transport_http();

                if self.transport_tcp() {
                    match self
                        .try_signature_slot_by_signature_tcp(
                            topology,
                            local_table,
                            signature_hash,
                            sig_bucket,
                            signature_literal,
                        )
                        .await?
                    {
                        Some(result) => return Ok(result),
                        None => allow_local_http = true,
                    }
                }

                if allow_local_http
                    && let Some(result) = self
                        .try_signature_slot_by_signature_http(
                            topology,
                            local_table,
                            signature_hash,
                            sig_bucket,
                            signature_literal,
                        )
                        .await?
                {
                    return Ok(result);
                }
            }

            let signature_statuses_table = &self.signature_statuses_table;
            let settings_clause =
                self.select_settings_clause("get_signature_slot", QueryFreshnessClass::Historical);
            let query = format!(
                "SELECT
                    slot,
                    slot_idx
                 FROM {signature_statuses_table}
                 PREWHERE sig_bucket = {sig_bucket} AND signature = {signature_literal}
                 ORDER BY slot DESC, slot_idx DESC, signature
                 LIMIT 1
                 {settings_clause}",
                signature_statuses_table = signature_statuses_table,
                sig_bucket = sig_bucket,
                signature_literal = signature_literal,
                settings_clause = settings_clause
            );
            let (query, query_id) = annotate_query(query, "signature_slot");

            let start = Instant::now();
            let mut cursor = http_query_with_id(&self.client, &query, query_id)
                .fetch::<SignatureSlotRow>()
                .map_err(|e| ProcessingError::database(e.to_string(), e))?;

            let row_opt = cursor
                .next()
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

            Ok((
                row_opt.map(|row| SignatureSlot {
                    slot: row.slot,
                    slot_idx: row.slot_idx,
                }),
                timings,
            ))
        })
        .await
    }

    pub async fn get_signature_statuses(
        &self,
        signatures: &[String],
    ) -> ProcessingResult<(Vec<SignatureStatusRecord>, QueryTimings)> {
        self.with_http_query_timeout("get_signature_statuses", async {
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

            if self.scope_shard_direct()
                && let (Some(topology), Some(local_table)) =
                    (&self.shard_topology, &self.signatures_local_table)
            {
                let mut allow_local_http = self.transport_http();

                if self.transport_tcp() {
                    match self
                        .try_get_signature_statuses_tcp(topology, local_table, signatures)
                        .await?
                    {
                        Some(result) => return Ok(result),
                        None => allow_local_http = true,
                    }
                }

                if allow_local_http
                    && let Some(result) = self
                        .try_get_signature_statuses_http(topology, local_table, signatures)
                        .await?
                {
                    return Ok(result);
                }
            }

            let signature_bucket_modulus = self.signatures_bucket_modulus();
            let mut bucketed_signatures: BTreeMap<u64, Vec<String>> = BTreeMap::new();
            for sig in signatures {
                let signature_bytes = bs58::decode(sig)
                    .into_vec()
                    .map_err(|e| ProcessingError::deserialization("Invalid signature", e))?;
                if signature_bytes.len() != 64 {
                    return Err(ProcessingError::deserialization_msg(format!(
                        "Invalid signature length {} (expected 64 bytes)",
                        signature_bytes.len()
                    )));
                }

                let bucket = cityhash64(signature_bytes.as_slice()) % signature_bucket_modulus;
                let signature_hex = hex::encode(signature_bytes).to_uppercase();
                let signature_literal = format!("toFixedString(unhex('{signature_hex}'), 64)");
                bucketed_signatures
                    .entry(bucket)
                    .or_default()
                    .push(signature_literal);
            }

            let signature_filter =
                build_signature_filter(bucketed_signatures, self.in_clause_chunk);

            let signature_statuses_table = &self.signature_statuses_table;
            let settings_clause = self
                .select_settings_clause("get_signature_statuses", QueryFreshnessClass::Historical);
            let query = format!(
                "SELECT
                    signature,
                    tupleElement(latest, 1) AS slot,
                    tupleElement(latest, 2) AS err
                 FROM (
                    SELECT
                        signature,
                        argMax(tuple(slot, err), tuple(slot, slot_idx)) AS latest
                    FROM {signature_statuses_table}
                    PREWHERE {signature_filter}
                    GROUP BY signature
                 )
                 {settings_clause}",
                signature_statuses_table = signature_statuses_table,
                signature_filter = signature_filter,
                settings_clause = settings_clause
            );
            let (query, query_id) = annotate_query(query, "signature_statuses");

            #[derive(Deserialize, clickhouse::Row)]
            struct StatusRow {
                signature: serde_big_array::Array<u8, 64>,
                slot: u64,
                err: Option<String>,
            }

            let start = Instant::now();
            let mut cursor = http_query_with_id(&self.client, &query, query_id)
                .fetch::<StatusRow>()
                .map_err(|e| ProcessingError::database(e.to_string(), e))?;

            let mut results = Vec::new();
            while let Some(row) = cursor
                .next()
                .await
                .map_err(|e| ProcessingError::database(e.to_string(), e))?
            {
                let signature = bs58::encode(row.signature.0).into_string();
                let parsed_err = row
                    .err
                    .and_then(|err_str| parse_err_json(&signature, err_str));
                results.push(SignatureStatusRecord {
                    signature,
                    slot: row.slot,
                    err: parsed_err,
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
        })
        .await
    }

    async fn try_signature_slot_by_signature_tcp(
        &self,
        topology: &ShardTopology,
        local_table: &str,
        signature_hash: u64,
        sig_bucket: u64,
        signature_literal: &str,
    ) -> ProcessingResult<Option<(Option<SignatureSlot>, QueryTimings)>> {
        let shard = topology.shard_for_hash(signature_hash);
        let query_timeout = self.shard_tcp_query_timeout();
        let settings_clause = append_max_execution_time_setting(
            &topology.settings_clause("get_signature_slot_local", QueryFreshnessClass::Historical),
            query_timeout,
        );
        let query = format!(
            "SELECT
                slot,
                slot_idx
             FROM {signature_table}
             PREWHERE sig_bucket = {sig_bucket} AND signature = {signature_literal}
             ORDER BY slot DESC, slot_idx DESC
             LIMIT 1
             {settings_clause}",
            signature_table = local_table,
            sig_bucket = sig_bucket,
            signature_literal = signature_literal,
            settings_clause = settings_clause
        );
        let (block, timings) = match execute_shard_tcp_query_block(
            shard.clone(),
            query_timeout,
            "get_signature_slot_local",
            "signature_slot_local",
            query,
        )
        .await
        {
            Ok(result) => result,
            Err(err) => {
                if let Some(reason) = transient_shard_local_error_reason(&err) {
                    crate::metrics::clickhouse_transport_fallback(
                        "get_signature_slot_local",
                        "tcp",
                        "http",
                        reason,
                    );
                    tracing::warn!(
                        "Shard {}:{} TCP signature slot query failed; falling back to HTTP: {}",
                        shard.host,
                        shard.tcp_port,
                        err
                    );
                    return Ok(None);
                }
                return Err(err);
            }
        };

        let slot_opt = if let Some(row) = block.rows().next() {
            let slot: u64 = row
                .get("slot")
                .map_err(|e| ProcessingError::database(e.to_string(), e))?;
            let slot_idx: u32 = row
                .get("slot_idx")
                .map_err(|e| ProcessingError::database(e.to_string(), e))?;
            Some(SignatureSlot { slot, slot_idx })
        } else {
            None
        };

        let mut timings = timings;
        timings.rows_returned = u64::from(slot_opt.is_some());

        Ok(Some((slot_opt, timings)))
    }

    async fn try_signature_slot_by_signature_http(
        &self,
        topology: &ShardTopology,
        local_table: &str,
        signature_hash: u64,
        sig_bucket: u64,
        signature_literal: &str,
    ) -> ProcessingResult<Option<(Option<SignatureSlot>, QueryTimings)>> {
        #[derive(Deserialize, clickhouse::Row)]
        struct SignatureSlotRow {
            slot: u64,
            slot_idx: u32,
        }

        let shard = topology.shard_for_hash(signature_hash);
        let settings_clause = topology.settings_clause(
            "get_signature_slot_local_http",
            QueryFreshnessClass::Historical,
        );
        let query = format!(
            "SELECT
                slot,
                slot_idx
             FROM {signature_table}
             PREWHERE sig_bucket = {sig_bucket} AND signature = {signature_literal}
             ORDER BY slot DESC, slot_idx DESC
             LIMIT 1
             {settings_clause}",
            signature_table = local_table,
            sig_bucket = sig_bucket,
            signature_literal = signature_literal,
            settings_clause = settings_clause
        );

        let start = Instant::now();
        let mut cursor = match shard.http_client.query(&query).fetch::<SignatureSlotRow>() {
            Ok(cursor) => cursor,
            Err(err) => {
                crate::metrics::clickhouse_transport_fallback(
                    "get_signature_slot_local_http",
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

        let row_opt = match cursor.next().await {
            Ok(row_opt) => row_opt,
            Err(err) => {
                crate::metrics::clickhouse_transport_fallback(
                    "get_signature_slot_local_http",
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
        };

        let timings = QueryTimings {
            elapsed_ms: start.elapsed().as_millis() as u64,
            received_bytes: cursor.received_bytes(),
            decoded_bytes: cursor.decoded_bytes(),
            rows_read: Some(0),
            rows_read_unknown: true,
            rows_returned: u64::from(row_opt.is_some()),
        };

        Ok(Some((
            row_opt.map(|row| SignatureSlot {
                slot: row.slot,
                slot_idx: row.slot_idx,
            }),
            timings,
        )))
    }

    async fn try_get_signature_statuses_tcp(
        &self,
        topology: &ShardTopology,
        local_table: &str,
        signatures: &[String],
    ) -> ProcessingResult<Option<(Vec<SignatureStatusRecord>, QueryTimings)>> {
        #[derive(Clone)]
        struct BucketedSignature {
            bucket: u64,
            literal: String,
        }

        let signature_bucket_modulus = self.signatures_bucket_modulus();
        let mut per_shard: Vec<Vec<BucketedSignature>> = vec![Vec::new(); topology.shards.len()];
        for sig in signatures {
            let signature_bytes = bs58::decode(sig)
                .into_vec()
                .map_err(|e| ProcessingError::deserialization("Invalid signature", e))?;
            if signature_bytes.len() != 64 {
                return Err(ProcessingError::deserialization_msg(format!(
                    "Invalid signature length {} (expected 64 bytes)",
                    signature_bytes.len()
                )));
            }

            let hash = cityhash64(signature_bytes.as_slice());
            let bucket = hash % signature_bucket_modulus;
            let signature_hex = hex::encode(signature_bytes).to_uppercase();
            let signature_literal = format!("toFixedString(unhex('{signature_hex}'), 64)");
            let shard_idx = topology.shard_index_for_hash(hash);
            per_shard[shard_idx].push(BucketedSignature {
                bucket,
                literal: signature_literal,
            });
        }

        let local_table: std::sync::Arc<str> = local_table.to_string().into();
        let fanout_sem = self.fanout_sem.clone();
        let query_timeout = self.shard_tcp_query_timeout();
        let settings_clause: std::sync::Arc<str> = append_max_execution_time_setting(
            &topology.settings_clause(
                "get_signature_statuses_local",
                QueryFreshnessClass::Historical,
            ),
            query_timeout,
        )
        .into();
        let in_clause_chunk = self.in_clause_chunk;

        enum ShardFailure {
            Fallback {
                host: String,
                port: u16,
                reason: String,
            },
            Error(ProcessingError),
        }

        let mut join_set = JoinSet::new();
        for (idx, bucketed) in per_shard.into_iter().enumerate() {
            if bucketed.is_empty() {
                continue;
            }
            let shard = topology.shards[idx].clone();
            let local_table = local_table.clone();
            let fanout_sem = fanout_sem.clone();
            let settings_clause = settings_clause.clone();

            join_set.spawn(async move {
                let _permit = fanout_sem.acquire().await.ok();
                let mut bucket_map: BTreeMap<u64, Vec<String>> = BTreeMap::new();
                for entry in bucketed {
                    bucket_map
                        .entry(entry.bucket)
                        .or_default()
                        .push(entry.literal);
                }

                let signature_filter = build_signature_filter(bucket_map, in_clause_chunk);
                let query = format!(
                    "SELECT
                        base58Encode(signature) AS signature,
                        tupleElement(latest, 1) AS slot,
                        tupleElement(latest, 2) AS err
                     FROM (
                        SELECT
                            signature,
                            argMax(tuple(slot, err), tuple(slot, slot_idx)) AS latest
                        FROM {signature_table}
                        PREWHERE {signature_filter}
                        GROUP BY signature
                     )
                     {settings_clause}",
                    signature_table = local_table,
                    signature_filter = signature_filter,
                    settings_clause = settings_clause.as_ref()
                );

                match execute_shard_tcp_query_block(
                    shard.clone(),
                    query_timeout,
                    "get_signature_statuses_local",
                    "signature_statuses_local",
                    query,
                )
                .await
                {
                    Ok((block, timings)) => {
                        let mut results = Vec::new();
                        for row in block.rows() {
                            let signature: String = row
                                .get("signature")
                                .map_err(|e| ProcessingError::database(e.to_string(), e))
                                .map_err(ShardFailure::Error)?;
                            let slot: u64 = row
                                .get("slot")
                                .map_err(|e| ProcessingError::database(e.to_string(), e))
                                .map_err(ShardFailure::Error)?;
                            let err: Option<String> = row
                                .get("err")
                                .map_err(|e| ProcessingError::database(e.to_string(), e))
                                .map_err(ShardFailure::Error)?;
                            let parsed_err =
                                err.and_then(|err_str| parse_err_json(&signature, err_str));
                            results.push(SignatureStatusRecord {
                                signature,
                                slot,
                                err: parsed_err,
                            });
                        }

                        Ok((results, timings))
                    }
                    Err(err) => {
                        if let Some(reason) = transient_shard_local_error_reason(&err) {
                            crate::metrics::clickhouse_transport_fallback(
                                "get_signature_statuses_local",
                                "tcp",
                                "http",
                                reason,
                            );
                            Err(ShardFailure::Fallback {
                                host: shard.host.clone(),
                                port: shard.tcp_port,
                                reason: reason.to_string(),
                            })
                        } else {
                            Err(ShardFailure::Error(err))
                        }
                    }
                }
            });
        }

        let mut results = Vec::new();
        let mut timings = QueryTimings::zero();
        timings.rows_read_unknown = true;
        while let Some(joined) = join_set.join_next().await {
            match joined {
                Ok(Ok((shard_results, shard_timings))) => {
                    results.extend(shard_results);
                    timings.merge_parallel(shard_timings);
                }
                Ok(Err(ShardFailure::Fallback { host, port, reason })) => {
                    tracing::warn!(
                        "Shard {}:{} TCP signature status query failed; falling back to HTTP: {}",
                        host,
                        port,
                        reason
                    );
                    return Ok(None);
                }
                Ok(Err(ShardFailure::Error(err))) => return Err(err),
                Err(err) => {
                    crate::metrics::clickhouse_transport_fallback(
                        "get_signature_statuses_local",
                        "tcp",
                        "http",
                        "task_failed",
                    );
                    tracing::warn!(
                        "Shard-local status task failed; falling back to HTTP: {}",
                        err
                    );
                    return Ok(None);
                }
            }
        }

        Ok(Some((results, timings)))
    }

    async fn try_get_signature_statuses_http(
        &self,
        topology: &ShardTopology,
        local_table: &str,
        signatures: &[String],
    ) -> ProcessingResult<Option<(Vec<SignatureStatusRecord>, QueryTimings)>> {
        #[derive(Clone)]
        struct BucketedSignature {
            bucket: u64,
            literal: String,
        }

        #[derive(Deserialize, clickhouse::Row)]
        struct StatusRow {
            signature: serde_big_array::Array<u8, 64>,
            slot: u64,
            err: Option<String>,
        }

        let signature_bucket_modulus = self.signatures_bucket_modulus();
        let mut per_shard: Vec<Vec<BucketedSignature>> = vec![Vec::new(); topology.shards.len()];
        for sig in signatures {
            let signature_bytes = bs58::decode(sig)
                .into_vec()
                .map_err(|e| ProcessingError::deserialization("Invalid signature", e))?;
            if signature_bytes.len() != 64 {
                return Err(ProcessingError::deserialization_msg(format!(
                    "Invalid signature length {} (expected 64 bytes)",
                    signature_bytes.len()
                )));
            }

            let hash = cityhash64(signature_bytes.as_slice());
            let bucket = hash % signature_bucket_modulus;
            let signature_hex = hex::encode(signature_bytes).to_uppercase();
            let signature_literal = format!("toFixedString(unhex('{signature_hex}'), 64)");
            let shard_idx = topology.shard_index_for_hash(hash);
            per_shard[shard_idx].push(BucketedSignature {
                bucket,
                literal: signature_literal,
            });
        }

        let local_table: std::sync::Arc<str> = local_table.to_string().into();
        let settings_clause: std::sync::Arc<str> = topology
            .settings_clause(
                "get_signature_statuses_local_http",
                QueryFreshnessClass::Historical,
            )
            .into();
        let fanout_sem = self.fanout_sem.clone();
        let query_timeout = self.query_timeout;
        let in_clause_chunk = self.in_clause_chunk;

        let mut join_set = JoinSet::new();
        for (idx, bucketed) in per_shard.into_iter().enumerate() {
            if bucketed.is_empty() {
                continue;
            }
            let shard = topology.shards[idx].clone();
            let local_table = local_table.clone();
            let fanout_sem = fanout_sem.clone();
            let settings_clause = settings_clause.clone();

            join_set.spawn(async move {
                let _permit = fanout_sem.acquire().await.ok();

                let mut bucket_map: BTreeMap<u64, Vec<String>> = BTreeMap::new();
                for entry in bucketed {
                    bucket_map
                        .entry(entry.bucket)
                        .or_default()
                        .push(entry.literal);
                }
                let signature_filter = build_signature_filter(bucket_map, in_clause_chunk);
                let query = format!(
                    "SELECT
                        signature,
                        tupleElement(latest, 1) AS slot,
                        tupleElement(latest, 2) AS err
                     FROM (
                        SELECT
                            signature,
                            argMax(tuple(slot, err), tuple(slot, slot_idx)) AS latest
                        FROM {signature_table}
                        PREWHERE {signature_filter}
                        GROUP BY signature
                     )
                     {settings_clause}",
                    signature_table = local_table,
                    signature_filter = signature_filter,
                    settings_clause = settings_clause.as_ref()
                );

                let timed = tokio::time::timeout(query_timeout, async {
                    let start = Instant::now();
                    let mut cursor = shard
                        .http_client
                        .query(&query)
                        .fetch::<StatusRow>()
                        .map_err(|e| ProcessingError::database(e.to_string(), e))?;

                    let mut shard_results = Vec::new();
                    while let Some(row) = cursor
                        .next()
                        .await
                        .map_err(|e| ProcessingError::database(e.to_string(), e))?
                    {
                        let signature = bs58::encode(row.signature.0).into_string();
                        let parsed_err = row
                            .err
                            .and_then(|err_str| parse_err_json(&signature, err_str));
                        shard_results.push(SignatureStatusRecord {
                            signature,
                            slot: row.slot,
                            err: parsed_err,
                        });
                    }
                    let shard_timings = QueryTimings {
                        elapsed_ms: start.elapsed().as_millis() as u64,
                        received_bytes: cursor.received_bytes(),
                        decoded_bytes: cursor.decoded_bytes(),
                        rows_read: Some(0),
                        rows_read_unknown: true,
                        rows_returned: shard_results.len() as u64,
                    };

                    Ok::<_, ProcessingError>((shard_results, shard_timings))
                })
                .await;

                match timed {
                    Ok(result) => result.map_err(|e| (shard.host.clone(), shard.tcp_port, e)),
                    Err(_) => {
                        crate::metrics::clickhouse_timeout("get_signature_statuses_local_http");
                        Err((
                            shard.host.clone(),
                            shard.tcp_port,
                            ProcessingError::timeout_msg(
                                "Shard-local signature status HTTP query timed out",
                            ),
                        ))
                    }
                }
            });
        }

        let mut results = Vec::new();
        let mut timings = QueryTimings::zero();
        while let Some(joined) = join_set.join_next().await {
            match joined {
                Ok(Ok((shard_results, shard_timings))) => {
                    results.extend(shard_results);
                    timings.merge_parallel(shard_timings);
                }
                Ok(Err((host, port, err))) => {
                    crate::metrics::clickhouse_transport_fallback(
                        "get_signature_statuses_local_http",
                        "http",
                        "distributed",
                        "shard_error",
                    );
                    tracing::warn!(
                        "Shard {}:{} HTTP query failed; falling back to distributed table: {}",
                        host,
                        port,
                        err
                    );
                    return Ok(None);
                }
                Err(err) => {
                    crate::metrics::clickhouse_transport_fallback(
                        "get_signature_statuses_local_http",
                        "http",
                        "distributed",
                        "task_failed",
                    );
                    tracing::warn!(
                        "Shard-local signature status HTTP task failed; falling back to distributed table: {}",
                        err
                    );
                    return Ok(None);
                }
            }
        }

        Ok(Some((results, timings)))
    }
}
