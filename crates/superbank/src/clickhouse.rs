// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use anyhow::{Context, Result};
use clickhouse::{Client as ClickHouseClient, Row, RowOwned, RowWrite};
use serde::{Deserialize, Serialize};
use serde_big_array::Array;
use serde_bytes::ByteBuf;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::{info, warn};

use crate::cli::Args;
use crate::metrics;

#[derive(Row, Serialize)]
pub(crate) struct TransactionRow {
    pub(crate) signature: Array<u8, 64>,
    pub(crate) slot: u64,
    pub(crate) slot_idx: u32,
    pub(crate) block_time: Option<i64>,
    pub(crate) message_hash: Array<u8, 32>,
    pub(crate) is_vote: u8,
    pub(crate) tx_version: Option<u8>,
    pub(crate) tx_signatures: Vec<Array<u8, 64>>,
    pub(crate) tx_num_required_signatures: u8,
    pub(crate) tx_num_readonly_signed_accounts: u8,
    pub(crate) tx_num_readonly_unsigned_accounts: u8,
    pub(crate) tx_account_keys: Vec<Array<u8, 32>>,
    pub(crate) tx_recent_blockhash: Array<u8, 32>,
    pub(crate) tx_instructions_program_id_index: Vec<u8>,
    pub(crate) tx_instructions_accounts: Vec<Vec<u8>>,
    pub(crate) tx_instructions_data: Vec<ByteBuf>,
    pub(crate) tx_address_table_lookups_present: u8,
    pub(crate) tx_address_table_lookup_account_key: Vec<Array<u8, 32>>,
    pub(crate) tx_address_table_lookup_writable_indexes: Vec<Vec<u8>>,
    pub(crate) tx_address_table_lookup_readonly_indexes: Vec<Vec<u8>>,
    pub(crate) meta_status_ok: u8,
    pub(crate) meta_err: Option<String>,
    pub(crate) meta_fee: u64,
    pub(crate) meta_pre_balances: Vec<u64>,
    pub(crate) meta_post_balances: Vec<u64>,
    pub(crate) meta_inner_instructions_present: u8,
    pub(crate) meta_inner_instructions_index: Vec<u8>,
    pub(crate) meta_inner_instructions_program_id_index: Vec<Vec<u8>>,
    pub(crate) meta_inner_instructions_accounts: Vec<Vec<Vec<u8>>>,
    pub(crate) meta_inner_instructions_data: Vec<Vec<ByteBuf>>,
    pub(crate) meta_inner_instructions_stack_height: Vec<Vec<Option<u32>>>,
    pub(crate) meta_log_messages_present: u8,
    pub(crate) meta_log_messages: Vec<String>,
    pub(crate) meta_pre_token_balances_present: u8,
    pub(crate) meta_pre_token_account_index: Vec<u8>,
    pub(crate) meta_pre_token_mint: Vec<Array<u8, 32>>,
    pub(crate) meta_pre_token_owner: Vec<Option<Array<u8, 32>>>,
    pub(crate) meta_pre_token_program_id: Vec<Option<Array<u8, 32>>>,
    pub(crate) meta_pre_token_amount: Vec<String>,
    pub(crate) meta_pre_token_decimals: Vec<u8>,
    pub(crate) meta_pre_token_ui_amount: Vec<Option<f64>>,
    pub(crate) meta_pre_token_ui_amount_string: Vec<String>,
    pub(crate) meta_post_token_balances_present: u8,
    pub(crate) meta_post_token_account_index: Vec<u8>,
    pub(crate) meta_post_token_mint: Vec<Array<u8, 32>>,
    pub(crate) meta_post_token_owner: Vec<Option<Array<u8, 32>>>,
    pub(crate) meta_post_token_program_id: Vec<Option<Array<u8, 32>>>,
    pub(crate) meta_post_token_amount: Vec<String>,
    pub(crate) meta_post_token_decimals: Vec<u8>,
    pub(crate) meta_post_token_ui_amount: Vec<Option<f64>>,
    pub(crate) meta_post_token_ui_amount_string: Vec<String>,
    pub(crate) meta_rewards_present: u8,
    pub(crate) meta_reward_pubkey: Vec<String>,
    pub(crate) meta_reward_lamports: Vec<i64>,
    pub(crate) meta_reward_post_balance: Vec<u64>,
    pub(crate) meta_reward_type: Vec<Option<String>>,
    pub(crate) meta_reward_commission: Vec<Option<u8>>,
    pub(crate) meta_loaded_addresses_writable: Vec<Array<u8, 32>>,
    pub(crate) meta_loaded_addresses_readonly: Vec<Array<u8, 32>>,
    pub(crate) meta_return_data_present: u8,
    pub(crate) meta_return_data_program_id: Option<Array<u8, 32>>,
    pub(crate) meta_return_data_data: Option<ByteBuf>,
    pub(crate) meta_compute_units_consumed: Option<u64>,
    pub(crate) meta_cost_units: Option<u64>,
}

#[derive(Row, Serialize)]
pub(crate) struct BlockMetadataRow {
    pub(crate) slot: u64,
    pub(crate) parent_slot: u64,
    pub(crate) blockhash: Array<u8, 32>,
    pub(crate) parent_blockhash: Array<u8, 32>,
    pub(crate) block_time: Option<i64>,
    pub(crate) block_height: Option<u64>,
    pub(crate) executed_transaction_count: u64,
    pub(crate) entry_count: u64,
    pub(crate) rewards_present: u8,
    pub(crate) rewards_pubkey: Vec<Array<u8, 32>>,
    pub(crate) rewards_lamports: Vec<i64>,
    pub(crate) rewards_post_balance: Vec<u64>,
    pub(crate) rewards_type: Vec<Option<String>>,
    pub(crate) rewards_commission: Vec<Option<u8>>,
    pub(crate) rewards_num_partitions: Option<u64>,
}

#[derive(Row, Serialize)]
pub(crate) struct EntryRow {
    pub(crate) slot: u64,
    pub(crate) entry_index: u32,
    pub(crate) block_time: Option<i64>,
    pub(crate) starting_transaction_index: u32,
    pub(crate) transaction_count: u32,
    pub(crate) num_hashes: u64,
    pub(crate) hash: Array<u8, 32>,
}

#[derive(Clone, Copy)]
pub(crate) struct ProgressSnapshot {
    pub(crate) processed: u64,
    pub(crate) total: u64,
    pub(crate) percent: f64,
    pub(crate) eta_seconds: Option<u64>,
    pub(crate) rpc_request_count: usize,
    pub(crate) rpc_avg_latency_ms: Option<f64>,
    pub(crate) rpc_rate_limited_ms: u64,
}

#[derive(Clone)]
pub(crate) struct InsertTables {
    pub(crate) transactions_table: String,
    pub(crate) blocks_table: String,
    pub(crate) entries_table: Option<String>,
}

impl InsertTables {
    pub(crate) fn from_args(args: &Args) -> Self {
        Self {
            transactions_table: args.transactions_table.clone(),
            blocks_table: args.blocks_table.clone(),
            entries_table: args.entries_table.clone(),
        }
    }
}

pub(crate) fn build_clickhouse_client(args: &Args) -> ClickHouseClient {
    let mut client = ClickHouseClient::default()
        .with_url(&args.clickhouse_url)
        .with_database(&args.clickhouse_database)
        .with_option(
            "async_insert",
            if args.clickhouse_async_insert {
                "1"
            } else {
                "0"
            },
        );

    if !args.clickhouse_user.is_empty() {
        client = client.with_user(&args.clickhouse_user);
    }

    if !args.clickhouse_password.is_empty() {
        client = client.with_password(&args.clickhouse_password);
    }

    client
}

pub(crate) async fn fetch_latest_slot_from_blocks(
    clickhouse: &ClickHouseClient,
    blocks_table: &str,
) -> Result<Option<u64>> {
    #[derive(Debug, Deserialize, Row)]
    struct MaxSlotRow {
        max_slot: Option<u64>,
    }

    let query = format!("SELECT maxOrNull(slot) AS max_slot FROM {blocks_table}");
    let row = clickhouse
        .query(&query)
        .fetch_one::<MaxSlotRow>()
        .await
        .with_context(|| format!("query latest slot from {blocks_table}"))?;

    Ok(row.max_slot)
}

pub(crate) async fn fetch_present_slots_in_range(
    clickhouse: &ClickHouseClient,
    blocks_table: &str,
    start: u64,
    end: u64,
) -> Result<Vec<u64>> {
    #[derive(Debug, Deserialize, Row)]
    struct SlotRow {
        slot: u64,
    }

    let query = format!("SELECT slot FROM {blocks_table} WHERE slot >= {start} AND slot <= {end}");
    let rows = clickhouse
        .query(&query)
        .fetch_all::<SlotRow>()
        .await
        .with_context(|| format!("query present slots in [{start}, {end}] from {blocks_table}"))?;

    Ok(rows.into_iter().map(|row| row.slot).collect())
}

pub(crate) async fn flush_buffers(
    client: &ClickHouseClient,
    tables: &InsertTables,
    transaction_rows: &mut Vec<TransactionRow>,
    block_rows: &mut Vec<BlockMetadataRow>,
    entry_rows: &mut Vec<EntryRow>,
    progress: Option<ProgressSnapshot>,
) -> Result<()> {
    if !transaction_rows.is_empty() {
        flush_transaction_rows(
            client,
            &tables.transactions_table,
            transaction_rows,
            progress,
        )
        .await?;
    }

    if !block_rows.is_empty() {
        flush_block_rows(client, &tables.blocks_table, block_rows, progress).await?;
    }

    if !entry_rows.is_empty()
        && let Some(entries_table) = tables.entries_table.as_deref()
    {
        flush_entry_rows(client, entries_table, entry_rows, progress).await?;
    }

    Ok(())
}

pub(crate) struct RetryConfig {
    pub(crate) max_retries: u32,
    pub(crate) base_ms: u64,
    pub(crate) max_ms: u64,
}

pub(crate) async fn flush_buffers_with_retry(
    client: &ClickHouseClient,
    tables: &InsertTables,
    transaction_rows: &mut Vec<TransactionRow>,
    block_rows: &mut Vec<BlockMetadataRow>,
    entry_rows: &mut Vec<EntryRow>,
    progress: Option<ProgressSnapshot>,
    retry: &RetryConfig,
) -> Result<()> {
    let mut attempt = 0u32;
    loop {
        match flush_buffers(
            client,
            tables,
            transaction_rows,
            block_rows,
            entry_rows,
            progress,
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(err) if attempt < retry.max_retries => {
                attempt += 1;
                let delay_ms = retry
                    .base_ms
                    .saturating_mul(1u64 << (attempt - 1).min(62))
                    .min(retry.max_ms);
                warn!(
                    attempt,
                    max_retries = retry.max_retries,
                    delay_ms,
                    error = %err,
                    "ClickHouse insert failed, retrying"
                );
                sleep(Duration::from_millis(delay_ms)).await;
            }
            Err(err) => {
                warn!(
                    attempt,
                    max_retries = retry.max_retries,
                    error = %err,
                    "ClickHouse insert failed, giving up"
                );
                return Err(err);
            }
        }
    }
}

fn split_qualified_table(name: &str) -> Option<(&str, &str)> {
    let (db, table) = name.split_once('.')?;
    if db.is_empty() || table.is_empty() || table.contains('.') {
        return None;
    }
    Some((db, table))
}

async fn flush_transaction_rows(
    client: &ClickHouseClient,
    table: &str,
    rows: &mut Vec<TransactionRow>,
    progress: Option<ProgressSnapshot>,
) -> Result<()> {
    let row_count = rows.len();
    let slot_range = transaction_slot_range(rows);
    let started = Instant::now();

    let result = insert_rows(client, table, rows).await;

    match result {
        Ok(()) => {
            metrics::observe_flush_duration(table, started.elapsed().as_secs_f64());
            metrics::observe_transaction_insert(table, row_count);
            log_insert_commit(table, row_count, slot_range, progress);
            rows.clear();
            Ok(())
        }
        Err(err) => {
            metrics::observe_flush_failure(table, "insert");
            Err(err)
        }
    }
}

async fn flush_block_rows(
    client: &ClickHouseClient,
    table: &str,
    rows: &mut Vec<BlockMetadataRow>,
    progress: Option<ProgressSnapshot>,
) -> Result<()> {
    let row_count = rows.len();
    let slot_range = block_slot_range(rows);
    let started = Instant::now();

    let result = insert_rows(client, table, rows).await;

    match result {
        Ok(()) => {
            metrics::observe_flush_duration(table, started.elapsed().as_secs_f64());
            metrics::observe_block_insert(
                table,
                row_count,
                slot_range.map(|(_, max_slot)| max_slot),
            );
            log_insert_commit(table, row_count, slot_range, progress);
            rows.clear();
            Ok(())
        }
        Err(err) => {
            metrics::observe_flush_failure(table, "insert");
            Err(err)
        }
    }
}

async fn flush_entry_rows(
    client: &ClickHouseClient,
    table: &str,
    rows: &mut Vec<EntryRow>,
    progress: Option<ProgressSnapshot>,
) -> Result<()> {
    let row_count = rows.len();
    let slot_range = entry_slot_range(rows);
    insert_rows(client, table, rows).await?;
    log_insert_commit(table, row_count, slot_range, progress);
    rows.clear();
    Ok(())
}

async fn insert_rows<T: RowOwned + RowWrite>(
    client: &ClickHouseClient,
    table: &str,
    rows: &[T],
) -> Result<()> {
    let (insert_client, insert_table) = match split_qualified_table(table) {
        // clickhouse::Client always sets the "current database" separately; if callers pass a
        // qualified table (e.g. `default.transactions`), normalize it for the insert API.
        Some((db, table_name)) => (client.clone().with_database(db), table_name),
        None => (client.clone(), table),
    };
    let mut insert = insert_client
        .insert::<T>(insert_table)
        .await
        .with_context(|| format!("prepare insert into {table}"))?;

    for row in rows {
        insert.write(row).await.context("write row")?;
    }

    insert.end().await.context("finish insert")?;
    Ok(())
}

fn log_insert_commit(
    table: &str,
    rows: usize,
    slot_range: Option<(u64, u64)>,
    progress: Option<ProgressSnapshot>,
) {
    match (slot_range, progress) {
        (Some((min_slot, max_slot)), Some(progress)) if min_slot == max_slot => {
            info!(
                table,
                rows,
                slot = min_slot,
                progress_processed = progress.processed,
                progress_total = progress.total,
                progress_percent = progress.percent,
                progress_eta_seconds = progress.eta_seconds,
                rpc_requests = progress.rpc_request_count,
                rpc_avg_latency_ms = progress.rpc_avg_latency_ms,
                rpc_rate_limited_ms = progress.rpc_rate_limited_ms,
                "clickhouse insert committed"
            );
        }
        (Some((min_slot, max_slot)), Some(progress)) => {
            info!(
                table,
                rows,
                slot_min = min_slot,
                slot_max = max_slot,
                progress_processed = progress.processed,
                progress_total = progress.total,
                progress_percent = progress.percent,
                progress_eta_seconds = progress.eta_seconds,
                rpc_requests = progress.rpc_request_count,
                rpc_avg_latency_ms = progress.rpc_avg_latency_ms,
                rpc_rate_limited_ms = progress.rpc_rate_limited_ms,
                "clickhouse insert committed"
            );
        }
        (None, Some(progress)) => {
            info!(
                table,
                rows,
                progress_processed = progress.processed,
                progress_total = progress.total,
                progress_percent = progress.percent,
                progress_eta_seconds = progress.eta_seconds,
                rpc_requests = progress.rpc_request_count,
                rpc_avg_latency_ms = progress.rpc_avg_latency_ms,
                rpc_rate_limited_ms = progress.rpc_rate_limited_ms,
                "clickhouse insert committed"
            );
        }
        (Some((min_slot, max_slot)), None) if min_slot == max_slot => {
            info!(table, rows, slot = min_slot, "clickhouse insert committed");
        }
        (Some((min_slot, max_slot)), None) => {
            info!(
                table,
                rows,
                slot_min = min_slot,
                slot_max = max_slot,
                "clickhouse insert committed"
            );
        }
        (None, None) => {
            info!(table, rows, "clickhouse insert committed");
        }
    }
}

fn transaction_slot_range(rows: &[TransactionRow]) -> Option<(u64, u64)> {
    let mut iter = rows.iter();
    let first = iter.next()?;
    let mut min_slot = first.slot;
    let mut max_slot = first.slot;
    for row in iter {
        min_slot = min_slot.min(row.slot);
        max_slot = max_slot.max(row.slot);
    }
    Some((min_slot, max_slot))
}

fn block_slot_range(rows: &[BlockMetadataRow]) -> Option<(u64, u64)> {
    let mut iter = rows.iter();
    let first = iter.next()?;
    let mut min_slot = first.slot;
    let mut max_slot = first.slot;
    for row in iter {
        min_slot = min_slot.min(row.slot);
        max_slot = max_slot.max(row.slot);
    }
    Some((min_slot, max_slot))
}

fn entry_slot_range(rows: &[EntryRow]) -> Option<(u64, u64)> {
    let mut iter = rows.iter();
    let first = iter.next()?;
    let mut min_slot = first.slot;
    let mut max_slot = first.slot;
    for row in iter {
        min_slot = min_slot.min(row.slot);
        max_slot = max_slot.max(row.slot);
    }
    Some((min_slot, max_slot))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{Args, IngestSource};

    fn sample_args() -> Args {
        Args {
            source: IngestSource::Grpc,
            endpoint: Some("https://example.invalid".to_string()),
            x_token: None,
            fumarole_endpoint: None,
            fumarole_x_token: None,
            fumarole_consumer_group: None,
            fumarole_create_consumer_group: false,
            fumarole_data_plane_tcp_connections: 4,
            fumarole_concurrent_download_limit_per_tcp: 2,
            fumarole_data_channel_capacity: 4096,
            fumarole_memory_soft_limit_bytes: crate::cli::DEFAULT_FUMAROLE_MEMORY_SOFT_LIMIT_BYTES,
            fumarole_commit_interval_secs: 10,
            fumarole_no_commit: false,
            commitment: "finalized".to_string(),
            dragonsmouth_from_slot: None,
            fumarole_from_slot: None,
            rpc_from_slot: None,
            grpc_max_decoding_bytes: 64 * 1024 * 1024,
            grpc_http2_adaptive_window: false,
            grpc_idle_timeout_secs: 30,
            grpc_health_watch_enabled: true,
            grpc_slot_notifications: true,
            rpc_url: None,
            rpc_to_slot: None,
            rpc_slot_count: None,
            rpc_timeout_secs: 30,
            rpc_retry_backoff_ms: 500,
            rpc_max_inflight: 64,
            rpc_max_supported_tx_version: 0,
            rpc_flush_every_slots: 500,
            rpc_progress_every_slots: 100,
            rpc_discovery_chunk_slots: 10_000,
            rpc_skip_ingested_slots: false,
            bigtable_range: None,
            bigtable_slot_file: None,
            bigtable_instance: "solana-ledger".to_string(),
            bigtable_app_profile: "default".to_string(),
            bigtable_timeout_secs: None,
            bigtable_max_message_bytes: 64 * 1024 * 1024,
            bigtable_credential_path: None,
            bigtable_credential_json: None,
            bigtable_discovery_limit: 10_000,
            bigtable_fetch_batch_size: 500,
            bigtable_fetch_concurrency: 4,
            bigtable_insert_concurrency: 1,
            bigtable_decode_concurrency: 4,
            bigtable_progress_every_slots: 10_000,
            clickhouse_url: "http://localhost:8123".to_string(),
            metrics_host: "0.0.0.0".to_string(),
            metrics_port: 9901,
            health_stale_secs: 120,
            metrics_cluster_label: None,
            clickhouse_database: "default".to_string(),
            clickhouse_user: "default".to_string(),
            clickhouse_password: String::new(),
            clickhouse_async_insert: false,
            transactions_table: "default.transactions".to_string(),
            blocks_table: "default.blocks_metadata".to_string(),
            entries_table: None,
            transactions_flush_rows: 25_000,
            blocks_flush_rows: 2_000,
            flush_interval_secs: 5,
            flush_every_block: false,
            insert_max_retries: 5,
            insert_retry_base_ms: 1_000,
            insert_retry_max_ms: 30_000,
        }
    }

    #[test]
    fn build_clickhouse_client_disables_async_insert_by_default() {
        let client = build_clickhouse_client(&sample_args());

        assert_eq!(client.get_option("async_insert"), Some("0"));
    }

    #[test]
    fn build_clickhouse_client_can_enable_async_insert() {
        let mut args = sample_args();
        args.clickhouse_async_insert = true;

        let client = build_clickhouse_client(&args);

        assert_eq!(client.get_option("async_insert"), Some("1"));
    }

    #[test]
    fn split_qualified_table_accepts_single_database_prefix() {
        assert_eq!(
            split_qualified_table("default.transactions"),
            Some(("default", "transactions"))
        );
    }

    #[test]
    fn split_qualified_table_rejects_ambiguous_names() {
        assert_eq!(split_qualified_table("transactions"), None);
        assert_eq!(split_qualified_table(".transactions"), None);
        assert_eq!(split_qualified_table("default."), None);
        assert_eq!(split_qualified_table("a.b.c"), None);
        assert_eq!(split_qualified_table(""), None);
    }
}
