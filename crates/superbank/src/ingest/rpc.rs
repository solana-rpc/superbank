// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use blake3::Hasher;
use serde_big_array::Array;
use solana_commitment_config::{CommitmentConfig, CommitmentLevel as SolanaCommitmentLevel};
use solana_message::VersionedMessage;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::client_error::{
    Error as RpcClientError, ErrorKind as RpcClientErrorKind,
};
use solana_rpc_client_api::request::RpcError;
use solana_rpc_client_types::config::RpcBlockConfig;
use solana_transaction::versioned::{TransactionVersion, VersionedTransaction};
use solana_transaction_status::{
    ConfirmedBlock, InnerInstruction, InnerInstructions, TransactionStatusMeta,
    TransactionWithStatusMeta,
};
use solana_transaction_status_client_types::{
    EncodedTransactionWithStatusMeta, Reward as UiReward, TransactionDetails,
    TransactionTokenBalance, UiConfirmedBlock, UiInstruction, UiReturnDataEncoding,
    UiTransactionEncoding, UiTransactionError, UiTransactionStatusMeta, UiTransactionTokenBalance,
    option_serializer::OptionSerializer,
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
    time::{MissedTickBehavior, interval},
};
use tracing::{info, warn};

use crate::cli::{Args, FromSlotSpec};
use crate::clickhouse::{
    BlockMetadataRow, InsertTables, ProgressSnapshot, RetryConfig, TransactionRow,
    build_clickhouse_client, fetch_latest_slot_from_blocks, flush_buffers_with_retry,
};
use crate::commitment::parse_commitment_config;
use crate::metrics;
use crate::rpc_client::build_rpc_client;
use crate::shutdown::spawn_shutdown_watch;
use crate::utils::{bytes_to_array, decode_base58_32};

#[derive(Clone, Copy)]
struct RpcRange {
    start: u64,
    end: u64,
}

struct RpcBlockBatch {
    block_row: BlockMetadataRow,
    transaction_rows: Vec<TransactionRow>,
}

struct RpcSlotResult {
    slot: u64,
    batch: Option<RpcBlockBatch>,
}

struct RpcInserterOutcome {
    shutdown_requested: bool,
}

struct FlushBatch {
    transaction_rows: Vec<TransactionRow>,
    block_rows: Vec<BlockMetadataRow>,
    progress: Option<ProgressSnapshot>,
}

struct RpcLatencyStats {
    request_count: usize,
    avg_latency_ms: Option<f64>,
    rate_limited_ms: u64,
}

struct RpcInserterArgs<'a> {
    clickhouse: Arc<clickhouse::Client>,
    insert_tables: Arc<InsertTables>,
    insert_concurrency: usize,
    args: &'a Args,
    rpc_clients: Arc<Vec<Arc<RpcClient>>>,
    result_rx: mpsc::Receiver<RpcSlotResult>,
    progress_rx: watch::Receiver<u64>,
    shutdown_rx: watch::Receiver<u64>,
    fatal_rx: mpsc::Receiver<anyhow::Error>,
    range: RpcRange,
    start_time: std::time::Instant,
}

type InnerInstructionFields = (
    u8,
    Vec<u8>,
    Vec<Vec<u8>>,
    Vec<Vec<Vec<u8>>>,
    Vec<Vec<serde_bytes::ByteBuf>>,
    Vec<Vec<Option<u32>>>,
);
type RewardsFields = (
    u8,
    Vec<String>,
    Vec<i64>,
    Vec<u64>,
    Vec<Option<String>>,
    Vec<Option<u8>>,
);
type LoadedAddressFields = (Vec<Array<u8, 32>>, Vec<Array<u8, 32>>);
type RpcInstructionFields = (Vec<u8>, Vec<Vec<u8>>, Vec<serde_bytes::ByteBuf>);
type RpcAddressTableLookupFields = (Vec<Array<u8, 32>>, Vec<Vec<u8>>, Vec<Vec<u8>>);
type RpcBlockRewardsFields = (
    u8,
    Vec<Array<u8, 32>>,
    Vec<i64>,
    Vec<u64>,
    Vec<Option<String>>,
    Vec<Option<u8>>,
);

pub(crate) async fn run_rpc_ingest(args: &Args) -> Result<()> {
    let rpc_url = args
        .rpc_url
        .as_ref()
        .context("rpc source requires --rpc-url / RPC_URL / config rpc_url")?;
    let clickhouse = Arc::new(build_clickhouse_client(args));
    let insert_tables = Arc::new(InsertTables::from_args(args));
    let commitment = parse_commitment_config(&args.commitment)?;
    let worker_count = args.rpc_max_inflight.max(1);
    let client_pool_size = worker_count.clamp(1, 16);
    let mut rpc_clients = Vec::with_capacity(client_pool_size);
    for _ in 0..client_pool_size {
        rpc_clients.push(build_rpc_client(
            rpc_url,
            commitment,
            args.rpc_timeout_secs,
            worker_count,
        )?);
    }
    let discovery_client = rpc_clients
        .first()
        .cloned()
        .context("rpc client pool is empty")?;

    let range = resolve_rpc_range(args, discovery_client.as_ref(), clickhouse.as_ref()).await?;
    let block_config = build_rpc_block_config(args, commitment);

    info!(
        source = "rpc",
        rpc_url = %rpc_url,
        from_slot = range.start,
        to_slot = range.end,
        transactions_table = %args.transactions_table,
        blocks_table = %args.blocks_table,
        "starting superbank ingest"
    );

    let shutdown_rx = spawn_shutdown_watch();
    let queue_capacity = worker_count.saturating_mul(100).clamp(1_000, 50_000);
    let worker_queue_capacity = (queue_capacity / worker_count).max(1);
    let result_capacity = worker_count.saturating_mul(4).clamp(1_000, 10_000);

    let (slot_tx, slot_rx) = mpsc::channel(queue_capacity);
    let (progress_tx, progress_rx) = watch::channel(range.start.saturating_sub(1));
    let (result_tx, result_rx) = mpsc::channel(result_capacity);
    let (fatal_tx, fatal_rx) = mpsc::channel(1);

    let discovery_backoff = Duration::from_millis(args.rpc_retry_backoff_ms);
    let discovery_handle = tokio::spawn(discover_slots(
        discovery_client.clone(),
        range,
        commitment,
        args.rpc_discovery_chunk_slots,
        discovery_backoff,
        slot_tx,
        progress_tx,
    ));

    let mut worker_txs = Vec::with_capacity(worker_count);
    let mut worker_handles = Vec::with_capacity(worker_count);
    for worker_id in 0..worker_count {
        let (worker_tx, worker_rx) = mpsc::channel(worker_queue_capacity);
        worker_txs.push(worker_tx);
        let rpc_client = rpc_clients[worker_id % rpc_clients.len()].clone();
        let backoff = Duration::from_millis(args.rpc_retry_backoff_ms);
        let result_tx = result_tx.clone();
        let fatal_tx = fatal_tx.clone();
        worker_handles.push(tokio::spawn(run_rpc_worker(
            worker_id,
            rpc_client,
            block_config,
            backoff,
            worker_rx,
            result_tx,
            fatal_tx,
        )));
    }
    drop(result_tx);
    drop(fatal_tx);

    let dispatcher_handle =
        tokio::spawn(run_rpc_dispatcher(slot_rx, worker_txs, shutdown_rx.clone()));

    let start_time = std::time::Instant::now();
    let insert_concurrency = worker_count.clamp(2, 4);
    let inserter_result = run_rpc_inserter(RpcInserterArgs {
        clickhouse: clickhouse.clone(),
        insert_tables: insert_tables.clone(),
        insert_concurrency,
        args,
        rpc_clients: Arc::new(rpc_clients),
        result_rx,
        progress_rx,
        shutdown_rx: shutdown_rx.clone(),
        fatal_rx,
        range,
        start_time,
    })
    .await;

    match inserter_result {
        Ok(outcome) => {
            if outcome.shutdown_requested {
                dispatcher_handle.abort();
                for handle in worker_handles {
                    handle.abort();
                }
                discovery_handle.abort();
                return Ok(());
            }
        }
        Err(err) => {
            dispatcher_handle.abort();
            for handle in worker_handles {
                handle.abort();
            }
            discovery_handle.abort();
            return Err(err);
        }
    }

    if let Err(err) = dispatcher_handle.await {
        warn!("rpc slot dispatcher task failed: {err}");
    }

    if let Err(err) = discovery_handle.await {
        warn!("rpc slot discovery task failed: {err}");
    }

    for handle in worker_handles {
        if let Err(err) = handle.await {
            warn!("rpc worker task failed: {err}");
        }
    }

    Ok(())
}

fn collect_rpc_latency_stats(rpc_clients: &[Arc<RpcClient>]) -> RpcLatencyStats {
    let mut total_requests = 0usize;
    let mut total_elapsed = Duration::ZERO;
    let mut total_rate_limited = Duration::ZERO;
    for client in rpc_clients {
        let stats = client.get_transport_stats();
        total_requests = total_requests.saturating_add(stats.request_count);
        total_elapsed += stats.elapsed_time;
        total_rate_limited += stats.rate_limited_time;
    }

    let avg_latency_ms = if total_requests > 0 {
        Some((total_elapsed.as_secs_f64() * 1000.0) / total_requests as f64)
    } else {
        None
    };

    RpcLatencyStats {
        request_count: total_requests,
        avg_latency_ms,
        rate_limited_ms: total_rate_limited.as_millis() as u64,
    }
}

fn build_progress_snapshot(
    processed: u64,
    total: u64,
    start_time: std::time::Instant,
    rpc_clients: &[Arc<RpcClient>],
) -> ProgressSnapshot {
    let elapsed = start_time.elapsed().as_secs_f64().max(0.001);
    let rate = (processed as f64) / elapsed;
    let percent = (processed as f64 / total as f64) * 100.0;
    let remaining = total.saturating_sub(processed);
    let eta_seconds = if rate > 0.0 {
        Some((remaining as f64 / rate).ceil() as u64)
    } else {
        None
    };
    let rpc_stats = collect_rpc_latency_stats(rpc_clients);

    ProgressSnapshot {
        processed,
        total,
        percent,
        eta_seconds,
        rpc_request_count: rpc_stats.request_count,
        rpc_avg_latency_ms: rpc_stats.avg_latency_ms,
        rpc_rate_limited_ms: rpc_stats.rate_limited_ms,
    }
}

fn take_flush_batch(
    args: &Args,
    transaction_rows: &mut Vec<TransactionRow>,
    block_rows: &mut Vec<BlockMetadataRow>,
    progress: Option<ProgressSnapshot>,
) -> Option<FlushBatch> {
    if transaction_rows.is_empty() && block_rows.is_empty() {
        return None;
    }

    let mut tx_rows = Vec::with_capacity(args.transactions_flush_rows);
    let mut blk_rows = Vec::with_capacity(args.blocks_flush_rows);
    std::mem::swap(&mut tx_rows, transaction_rows);
    std::mem::swap(&mut blk_rows, block_rows);

    Some(FlushBatch {
        transaction_rows: tx_rows,
        block_rows: blk_rows,
        progress,
    })
}

async fn enqueue_flush(
    insert_tasks: &mut JoinSet<Result<()>>,
    insert_concurrency: usize,
    clickhouse: Arc<clickhouse::Client>,
    insert_tables: Arc<InsertTables>,
    batch: FlushBatch,
    retry: Arc<RetryConfig>,
) -> Result<()> {
    let max_inflight = insert_concurrency.max(1);
    while insert_tasks.len() >= max_inflight {
        if let Some(result) = insert_tasks.join_next().await {
            result??;
        }
    }

    insert_tasks.spawn(async move {
        let mut transaction_rows = batch.transaction_rows;
        let mut block_rows = batch.block_rows;
        let mut entry_rows = Vec::new();
        flush_buffers_with_retry(
            clickhouse.as_ref(),
            insert_tables.as_ref(),
            &mut transaction_rows,
            &mut block_rows,
            &mut entry_rows,
            batch.progress,
            &retry,
        )
        .await
    });

    Ok(())
}

async fn drain_insert_tasks(
    insert_tasks: &mut JoinSet<Result<()>>,
    mut shutdown_rx: Option<watch::Receiver<u64>>,
) -> Result<()> {
    loop {
        if insert_tasks.is_empty() {
            return Ok(());
        }

        match shutdown_rx.as_mut() {
            Some(rx) => {
                tokio::select! {
                    Some(result) = insert_tasks.join_next() => {
                        result??;
                    }
                    _ = rx.changed() => {
                        return Ok(());
                    }
                }
            }
            None => {
                if let Some(result) = insert_tasks.join_next().await {
                    result??;
                } else {
                    return Ok(());
                }
            }
        }
    }
}

async fn run_rpc_dispatcher(
    mut slot_rx: mpsc::Receiver<u64>,
    mut worker_txs: Vec<mpsc::Sender<u64>>,
    mut shutdown_rx: watch::Receiver<u64>,
) {
    let mut next_worker = 0usize;
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => break,
            slot = slot_rx.recv() => {
                let Some(slot) = slot else { break; };
                if worker_txs.is_empty() {
                    break;
                }
                loop {
                    if worker_txs.is_empty() {
                        return;
                    }
                    let idx = next_worker % worker_txs.len();
                    let send_result = tokio::select! {
                        result = worker_txs[idx].send(slot) => result,
                        _ = shutdown_rx.changed() => return,
                    };
                    match send_result {
                        Ok(()) => {
                            next_worker = idx.saturating_add(1);
                            break;
                        }
                        Err(_) => {
                            worker_txs.swap_remove(idx);
                        }
                    }
                }
            }
        }
    }
}

async fn run_rpc_worker(
    worker_id: usize,
    rpc_client: Arc<RpcClient>,
    block_config: RpcBlockConfig,
    backoff: Duration,
    mut slot_rx: mpsc::Receiver<u64>,
    result_tx: mpsc::Sender<RpcSlotResult>,
    fatal_tx: mpsc::Sender<anyhow::Error>,
) {
    while let Some(slot) = slot_rx.recv().await {
        match fetch_rpc_block_batch(rpc_client.as_ref(), slot, &block_config, backoff).await {
            Ok(batch) => {
                if result_tx.send(RpcSlotResult { slot, batch }).await.is_err() {
                    return;
                }
            }
            Err(err) => {
                warn!(
                    slot,
                    worker_id,
                    error = %err,
                    "rpc worker failed fetching block"
                );
                let _ = fatal_tx.try_send(err);
                return;
            }
        }
    }
}

async fn run_rpc_inserter(args: RpcInserterArgs<'_>) -> Result<RpcInserterOutcome> {
    let RpcInserterArgs {
        clickhouse,
        insert_tables,
        insert_concurrency,
        args: cli_args,
        rpc_clients,
        mut result_rx,
        mut progress_rx,
        mut shutdown_rx,
        mut fatal_rx,
        range,
        start_time,
    } = args;
    let retry_config = Arc::new(RetryConfig {
        max_retries: cli_args.insert_max_retries,
        base_ms: cli_args.insert_retry_base_ms,
        max_ms: cli_args.insert_retry_max_ms,
    });
    let mut transaction_rows: Vec<TransactionRow> =
        Vec::with_capacity(cli_args.transactions_flush_rows);
    let mut block_rows: Vec<BlockMetadataRow> = Vec::with_capacity(cli_args.blocks_flush_rows);

    let total_slots = range.end.saturating_sub(range.start).saturating_add(1);
    let flush_every = cli_args.rpc_flush_every_slots.max(1);
    let progress_every = cli_args.rpc_progress_every_slots.max(1);
    let mut next_progress_slot = range.start.saturating_add(progress_every.saturating_sub(1));
    let mut slots_since_flush = 0u64;
    let mut processed_slots = 0u64;
    let mut last_progress: Option<ProgressSnapshot> = None;
    let mut insert_tasks: JoinSet<Result<()>> = JoinSet::new();

    let mut flush_timer = interval(Duration::from_secs(cli_args.flush_interval_secs));
    flush_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let mut shutdown_requested = false;

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                shutdown_requested = true;
                info!("shutdown signal received; flushing remaining rows");
                break;
            }
            Some(result) = insert_tasks.join_next(), if !insert_tasks.is_empty() => {
                result??;
            }
            fatal = fatal_rx.recv() => {
                if let Some(err) = fatal {
                    return Err(err);
                }
            }
            _ = flush_timer.tick() => {
                if let Some(batch) = take_flush_batch(
                    cli_args,
                    &mut transaction_rows,
                    &mut block_rows,
                    last_progress,
                ) {
                    enqueue_flush(
                        &mut insert_tasks,
                        insert_concurrency,
                        clickhouse.clone(),
                        insert_tables.clone(),
                        batch,
                        retry_config.clone(),
                    )
                    .await?;
                    last_progress = None;
                }
            }
            progress = progress_rx.changed() => {
                if progress.is_ok() {
                    let new_cursor = *progress_rx.borrow();
                    let new_processed = new_cursor
                        .saturating_sub(range.start)
                        .saturating_add(1)
                        .min(total_slots);
                    processed_slots = processed_slots.max(new_processed);
                    if new_cursor >= next_progress_slot {
                        last_progress = Some(build_progress_snapshot(
                            processed_slots,
                            total_slots,
                            start_time,
                            &rpc_clients,
                        ));
                        next_progress_slot = next_progress_slot.saturating_add(progress_every);
                    }
                }
            }
            result = result_rx.recv() => {
                let Some(result) = result else {
                    break;
                };
                metrics::set_last_processed_slot(result.slot);
                if let Some(batch) = result.batch {
                    transaction_rows.extend(batch.transaction_rows);
                    block_rows.push(batch.block_row);
                }

                processed_slots = processed_slots.saturating_add(1);
                slots_since_flush = slots_since_flush.saturating_add(1);
                if slots_since_flush >= flush_every
                    || transaction_rows.len() >= cli_args.transactions_flush_rows
                    || block_rows.len() >= cli_args.blocks_flush_rows
                {
                    if let Some(progress) = last_progress {
                        last_progress = None;
                        if let Some(batch) = take_flush_batch(
                            cli_args,
                            &mut transaction_rows,
                            &mut block_rows,
                            Some(progress),
                        ) {
                            enqueue_flush(
                                &mut insert_tasks,
                                insert_concurrency,
                                clickhouse.clone(),
                                insert_tables.clone(),
                                batch,
                                retry_config.clone(),
                            )
                            .await?;
                        }
                    } else if let Some(batch) = take_flush_batch(
                        cli_args,
                        &mut transaction_rows,
                        &mut block_rows,
                        None,
                    ) {
                        enqueue_flush(
                            &mut insert_tasks,
                            insert_concurrency,
                            clickhouse.clone(),
                            insert_tables.clone(),
                            batch,
                            retry_config.clone(),
                        )
                        .await?;
                    }
                    slots_since_flush = 0;
                }
            }
        }
    }

    if let Some(batch) = take_flush_batch(
        cli_args,
        &mut transaction_rows,
        &mut block_rows,
        last_progress.take(),
    ) {
        enqueue_flush(
            &mut insert_tasks,
            insert_concurrency,
            clickhouse.clone(),
            insert_tables.clone(),
            batch,
            retry_config.clone(),
        )
        .await?;
    }

    if shutdown_requested {
        let shutdown_count = *shutdown_rx.borrow();
        tokio::select! {
            result = drain_insert_tasks(&mut insert_tasks, Some(shutdown_rx.clone())) => {
                result?;
            }
            _ = shutdown_rx.changed() => {
                let new_count = *shutdown_rx.borrow();
                if new_count <= shutdown_count {
                    warn!("shutdown signal updated without count increase; exiting");
                }
                warn!("second SIGINT received; exiting before flush completes");
                return Ok(RpcInserterOutcome { shutdown_requested: true });
            }
        }
    } else {
        drain_insert_tasks(&mut insert_tasks, None).await?;
    }

    Ok(RpcInserterOutcome { shutdown_requested })
}

fn build_rpc_block_config(args: &Args, commitment: CommitmentConfig) -> RpcBlockConfig {
    RpcBlockConfig {
        encoding: Some(UiTransactionEncoding::Base64),
        transaction_details: Some(TransactionDetails::Full),
        rewards: Some(true),
        commitment: Some(commitment),
        max_supported_transaction_version: Some(args.rpc_max_supported_tx_version),
    }
}

async fn resolve_rpc_range(
    args: &Args,
    rpc_client: &RpcClient,
    clickhouse: &clickhouse::Client,
) -> Result<RpcRange> {
    let start = match args.rpc_from_slot {
        Some(FromSlotSpec::LatestDb) => {
            let latest = fetch_latest_slot_from_blocks(clickhouse, &args.blocks_table)
                .await?
                .ok_or_else(|| {
                    anyhow!(
                        "rpc-from-slot '*' requires at least one row in {}",
                        args.blocks_table
                    )
                })?;
            info!(
                slot = latest,
                table = %args.blocks_table,
                "resolved rpc-from-slot='*' to latest slot in blocks_metadata"
            );
            latest
        }
        Some(FromSlotSpec::Slot(0)) => {
            let earliest = rpc_client
                .get_first_available_block()
                .await
                .context("fetch first available block")?;
            info!(
                slot = earliest,
                "resolved rpc-from-slot=0 to earliest available slot"
            );
            earliest
        }
        Some(FromSlotSpec::Slot(slot)) => slot,
        None => return Err(anyhow!("rpc source requires --rpc-from-slot")),
    };

    let end = match (args.rpc_to_slot, args.rpc_slot_count) {
        (Some(to_slot), None) => {
            if to_slot < start {
                return Err(anyhow!(
                    "to-slot {} must be greater than or equal to rpc-from-slot {}",
                    to_slot,
                    start
                ));
            }
            to_slot
        }
        (None, Some(count)) => {
            let count_minus_one = count
                .checked_sub(1)
                .ok_or_else(|| anyhow!("rpc slot-count must be greater than 0"))?;
            start
                .checked_add(count_minus_one)
                .ok_or_else(|| anyhow!("rpc slot range exceeds u64::MAX"))?
        }
        (Some(_), Some(_)) => {
            return Err(anyhow!(
                "rpc source requires either --rpc-to-slot or --rpc-slot-count (not both)"
            ));
        }
        (None, None) => {
            return Err(anyhow!(
                "rpc source requires --rpc-to-slot or --rpc-slot-count to define a range"
            ));
        }
    };

    Ok(RpcRange { start, end })
}

fn rpc_discovery_commitment(commitment: CommitmentConfig) -> CommitmentConfig {
    if commitment.commitment == SolanaCommitmentLevel::Processed {
        warn!("rpc discovery does not support processed commitment; using confirmed");
        return CommitmentConfig::confirmed();
    }
    commitment
}

async fn discover_slots(
    rpc_client: Arc<RpcClient>,
    range: RpcRange,
    commitment: CommitmentConfig,
    chunk_slots: u64,
    backoff: Duration,
    slot_tx: mpsc::Sender<u64>,
    progress_tx: watch::Sender<u64>,
) -> Result<()> {
    let mut cursor = range.start;
    let commitment = rpc_discovery_commitment(commitment);

    while cursor <= range.end {
        let end = cursor
            .saturating_add(chunk_slots.saturating_sub(1))
            .min(range.end);
        let slots = loop {
            match rpc_client
                .get_blocks_with_commitment(cursor, Some(end), commitment)
                .await
            {
                Ok(slots) => break slots,
                Err(err) => {
                    metrics::observe_source_error("rpc_slot_discovery", "retryable");
                    warn!(
                        start_slot = cursor,
                        end_slot = end,
                        error = %err,
                        "rpc slot discovery failed; retrying"
                    );
                    if backoff.is_zero() {
                        tokio::task::yield_now().await;
                    } else {
                        tokio::time::sleep(backoff).await;
                    }
                }
            }
        };

        for slot in slots {
            if slot > range.end {
                break;
            }
            if slot < range.start {
                continue;
            }
            if slot_tx.send(slot).await.is_err() {
                return Ok(());
            }
        }

        cursor = end.saturating_add(1);
        let _ = progress_tx.send(end);
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RpcBlockErrorKind {
    NotAvailable,
    Skipped,
    Retryable,
    Fatal,
}

fn classify_rpc_block_error(err: &RpcClientError) -> RpcBlockErrorKind {
    match err.kind() {
        RpcClientErrorKind::RpcError(RpcError::RpcResponseError { code, message, .. }) => {
            let normalized = message.to_ascii_lowercase();
            if normalized.contains("was skipped")
                || normalized.contains("missing in long-term storage")
                || *code == -32009
            {
                return RpcBlockErrorKind::Skipped;
            }
            if normalized.contains("block not available")
                || normalized.contains("block not found")
                || normalized.contains("slot was not found")
                || *code == -32007
            {
                return RpcBlockErrorKind::NotAvailable;
            }
            RpcBlockErrorKind::Fatal
        }
        RpcClientErrorKind::Reqwest(err) => {
            let retryable = err.is_timeout()
                || err.is_connect()
                || err.status() == Some(reqwest::StatusCode::TOO_MANY_REQUESTS)
                || matches!(err.status(), Some(code) if code.is_server_error());

            if retryable {
                RpcBlockErrorKind::Retryable
            } else {
                RpcBlockErrorKind::Fatal
            }
        }
        RpcClientErrorKind::Io(_) => RpcBlockErrorKind::Retryable,
        _ => RpcBlockErrorKind::Fatal,
    }
}

async fn fetch_rpc_block_batch(
    rpc_client: &RpcClient,
    slot: u64,
    config: &RpcBlockConfig,
    backoff: Duration,
) -> Result<Option<RpcBlockBatch>> {
    let block = fetch_rpc_block_with_retry(rpc_client, slot, config, backoff).await?;
    let Some(block) = block else {
        return Ok(None);
    };

    let tx_rows = map_rpc_transactions(slot, block.block_time, &block)?;
    let block_row = map_rpc_block_metadata(slot, &block, tx_rows.len() as u64)?;

    Ok(Some(RpcBlockBatch {
        block_row,
        transaction_rows: tx_rows,
    }))
}

async fn fetch_rpc_block_with_retry(
    rpc_client: &RpcClient,
    slot: u64,
    config: &RpcBlockConfig,
    backoff: Duration,
) -> Result<Option<UiConfirmedBlock>> {
    loop {
        match rpc_client.get_block_with_config(slot, *config).await {
            Ok(block) => return Ok(Some(block)),
            Err(err) => match classify_rpc_block_error(&err) {
                RpcBlockErrorKind::Skipped => {
                    info!(slot, "rpc getBlock skipped or missing in long-term storage");
                    return Ok(None);
                }
                RpcBlockErrorKind::NotAvailable | RpcBlockErrorKind::Retryable => {
                    metrics::observe_source_error("rpc_get_block", "retryable");
                    warn!(slot, error = %err, "rpc getBlock not available yet; retrying");
                    if backoff.is_zero() {
                        tokio::task::yield_now().await;
                    } else {
                        tokio::time::sleep(backoff).await;
                    }
                }
                RpcBlockErrorKind::Fatal => {
                    metrics::observe_source_error("rpc_get_block", "fatal");
                    return Err(anyhow!("rpc getBlock failed for slot {slot}: {err}"));
                }
            },
        }
    }
}

pub(crate) fn map_rpc_block_metadata(
    slot: u64,
    block: &UiConfirmedBlock,
    executed_transaction_count: u64,
) -> Result<BlockMetadataRow> {
    let blockhash = decode_base58_32(&block.blockhash).context("decode blockhash")?;
    let parent_blockhash =
        decode_base58_32(&block.previous_blockhash).context("decode parent blockhash")?;

    let (
        rewards_present,
        rewards_pubkey,
        rewards_lamports,
        rewards_post_balance,
        rewards_type,
        rewards_commission,
    ) = convert_rpc_block_rewards(block.rewards.as_ref())?;

    Ok(BlockMetadataRow {
        slot,
        parent_slot: block.parent_slot,
        blockhash,
        parent_blockhash,
        block_time: block.block_time,
        block_height: block.block_height,
        executed_transaction_count,
        entry_count: 0,
        rewards_present,
        rewards_pubkey,
        rewards_lamports,
        rewards_post_balance,
        rewards_type,
        rewards_commission,
        rewards_num_partitions: block.num_reward_partitions,
    })
}

pub(crate) fn map_rpc_transactions(
    slot: u64,
    block_time: Option<i64>,
    block: &UiConfirmedBlock,
) -> Result<Vec<TransactionRow>> {
    let transactions = block
        .transactions
        .as_ref()
        .context("missing transactions in getBlock response")?;
    let mut rows = Vec::with_capacity(transactions.len());
    for (index, tx) in transactions.iter().enumerate() {
        let slot_idx: u32 = index.try_into().context("slot_idx out of range")?;
        rows.push(map_rpc_transaction(slot, block_time, slot_idx, tx)?);
    }
    Ok(rows)
}

fn map_rpc_transaction(
    slot: u64,
    block_time: Option<i64>,
    slot_idx: u32,
    tx: &EncodedTransactionWithStatusMeta,
) -> Result<TransactionRow> {
    let decoded = tx.transaction.decode().context("decode transaction")?;
    let signature = decoded
        .signatures
        .first()
        .context("missing transaction signature")?;

    let message_hash = compute_message_hash_versioned(&decoded.message)?;
    let header = decoded.message.header();

    let tx_signatures = decoded
        .signatures
        .iter()
        .map(|sig| Array(*sig.as_array()))
        .collect();
    let tx_account_keys = decoded
        .message
        .static_account_keys()
        .iter()
        .map(|key| Array(*key.as_array()))
        .collect();
    let tx_recent_blockhash = bytes_to_array::<32>(decoded.message.recent_blockhash().as_ref())
        .context("recent blockhash length")?;

    let (tx_instructions_program_id_index, tx_instructions_accounts, tx_instructions_data) =
        convert_rpc_instructions(decoded.message.instructions())?;

    let (
        tx_address_table_lookup_account_key,
        tx_address_table_lookup_writable_indexes,
        tx_address_table_lookup_readonly_indexes,
    ) = convert_rpc_address_table_lookups(decoded.message.address_table_lookups())?;

    let tx_address_table_lookups_present = decoded
        .message
        .address_table_lookups()
        .is_some_and(|lookups| !lookups.is_empty());

    let is_vote = if is_simple_vote_transaction(&decoded)? {
        1
    } else {
        0
    };
    let tx_version = match decoded.message {
        VersionedMessage::Legacy(_) => None,
        VersionedMessage::V0(_) => Some(0),
    };

    let meta_fields = map_rpc_meta_fields(tx.meta.as_ref())?;

    Ok(TransactionRow {
        signature: Array(*signature.as_array()),
        slot,
        slot_idx,
        block_time,
        message_hash,
        is_vote,
        tx_version,
        tx_signatures,
        tx_num_required_signatures: header.num_required_signatures,
        tx_num_readonly_signed_accounts: header.num_readonly_signed_accounts,
        tx_num_readonly_unsigned_accounts: header.num_readonly_unsigned_accounts,
        tx_account_keys,
        tx_recent_blockhash,
        tx_instructions_program_id_index,
        tx_instructions_accounts,
        tx_instructions_data,
        tx_address_table_lookups_present: if tx_address_table_lookups_present {
            1
        } else {
            0
        },
        tx_address_table_lookup_account_key,
        tx_address_table_lookup_writable_indexes,
        tx_address_table_lookup_readonly_indexes,
        meta_status_ok: meta_fields.meta_status_ok,
        meta_err: meta_fields.meta_err,
        meta_fee: meta_fields.meta_fee,
        meta_pre_balances: meta_fields.meta_pre_balances,
        meta_post_balances: meta_fields.meta_post_balances,
        meta_inner_instructions_present: meta_fields.meta_inner_instructions_present,
        meta_inner_instructions_index: meta_fields.meta_inner_instructions_index,
        meta_inner_instructions_program_id_index: meta_fields
            .meta_inner_instructions_program_id_index,
        meta_inner_instructions_accounts: meta_fields.meta_inner_instructions_accounts,
        meta_inner_instructions_data: meta_fields.meta_inner_instructions_data,
        meta_inner_instructions_stack_height: meta_fields.meta_inner_instructions_stack_height,
        meta_log_messages_present: meta_fields.meta_log_messages_present,
        meta_log_messages: meta_fields.meta_log_messages,
        meta_pre_token_balances_present: meta_fields.meta_pre_token_balances_present,
        meta_pre_token_account_index: meta_fields.meta_pre_token_account_index,
        meta_pre_token_mint: meta_fields.meta_pre_token_mint,
        meta_pre_token_owner: meta_fields.meta_pre_token_owner,
        meta_pre_token_program_id: meta_fields.meta_pre_token_program_id,
        meta_pre_token_amount: meta_fields.meta_pre_token_amount,
        meta_pre_token_decimals: meta_fields.meta_pre_token_decimals,
        meta_pre_token_ui_amount: meta_fields.meta_pre_token_ui_amount,
        meta_pre_token_ui_amount_string: meta_fields.meta_pre_token_ui_amount_string,
        meta_post_token_balances_present: meta_fields.meta_post_token_balances_present,
        meta_post_token_account_index: meta_fields.meta_post_token_account_index,
        meta_post_token_mint: meta_fields.meta_post_token_mint,
        meta_post_token_owner: meta_fields.meta_post_token_owner,
        meta_post_token_program_id: meta_fields.meta_post_token_program_id,
        meta_post_token_amount: meta_fields.meta_post_token_amount,
        meta_post_token_decimals: meta_fields.meta_post_token_decimals,
        meta_post_token_ui_amount: meta_fields.meta_post_token_ui_amount,
        meta_post_token_ui_amount_string: meta_fields.meta_post_token_ui_amount_string,
        meta_rewards_present: meta_fields.meta_rewards_present,
        meta_reward_pubkey: meta_fields.meta_reward_pubkey,
        meta_reward_lamports: meta_fields.meta_reward_lamports,
        meta_reward_post_balance: meta_fields.meta_reward_post_balance,
        meta_reward_type: meta_fields.meta_reward_type,
        meta_reward_commission: meta_fields.meta_reward_commission,
        meta_loaded_addresses_writable: meta_fields.meta_loaded_addresses_writable,
        meta_loaded_addresses_readonly: meta_fields.meta_loaded_addresses_readonly,
        meta_return_data_present: meta_fields.meta_return_data_present,
        meta_return_data_program_id: meta_fields.meta_return_data_program_id,
        meta_return_data_data: meta_fields.meta_return_data_data,
        meta_compute_units_consumed: meta_fields.meta_compute_units_consumed,
        meta_cost_units: meta_fields.meta_cost_units,
    })
}

pub(crate) fn map_bigtable_block_metadata(
    slot: u64,
    block: &ConfirmedBlock,
    executed_transaction_count: u64,
) -> Result<BlockMetadataRow> {
    let blockhash = decode_base58_32(&block.blockhash).context("decode blockhash")?;
    let parent_blockhash =
        decode_base58_32(&block.previous_blockhash).context("decode parent blockhash")?;

    let (
        rewards_present,
        rewards_pubkey,
        rewards_lamports,
        rewards_post_balance,
        rewards_type,
        rewards_commission,
    ) = convert_rpc_block_rewards(Some(&block.rewards))?;

    Ok(BlockMetadataRow {
        slot,
        parent_slot: block.parent_slot,
        blockhash,
        parent_blockhash,
        block_time: block.block_time,
        block_height: block.block_height,
        executed_transaction_count,
        entry_count: 0,
        rewards_present,
        rewards_pubkey,
        rewards_lamports,
        rewards_post_balance,
        rewards_type,
        rewards_commission,
        rewards_num_partitions: block.num_partitions,
    })
}

pub(crate) fn map_bigtable_transactions(
    slot: u64,
    block_time: Option<i64>,
    block: &ConfirmedBlock,
    max_supported_tx_version: u8,
) -> Result<Vec<TransactionRow>> {
    let mut rows = Vec::with_capacity(block.transactions.len());
    for (index, tx) in block.transactions.iter().enumerate() {
        let slot_idx: u32 = index.try_into().context("slot_idx out of range")?;
        rows.push(map_bigtable_transaction(
            slot,
            block_time,
            slot_idx,
            tx,
            max_supported_tx_version,
        )?);
    }
    Ok(rows)
}

fn map_bigtable_transaction(
    slot: u64,
    block_time: Option<i64>,
    slot_idx: u32,
    tx: &TransactionWithStatusMeta,
    max_supported_tx_version: u8,
) -> Result<TransactionRow> {
    match tx {
        TransactionWithStatusMeta::MissingMetadata(transaction) => {
            let versioned = VersionedTransaction::from(transaction.clone());
            map_versioned_transaction_with_meta(
                slot,
                block_time,
                slot_idx,
                &versioned,
                None,
                max_supported_tx_version,
            )
        }
        TransactionWithStatusMeta::Complete(tx_with_meta) => map_versioned_transaction_with_meta(
            slot,
            block_time,
            slot_idx,
            &tx_with_meta.transaction,
            Some(&tx_with_meta.meta),
            max_supported_tx_version,
        ),
    }
}

fn map_versioned_transaction_with_meta(
    slot: u64,
    block_time: Option<i64>,
    slot_idx: u32,
    tx: &VersionedTransaction,
    meta: Option<&TransactionStatusMeta>,
    max_supported_tx_version: u8,
) -> Result<TransactionRow> {
    let signature = tx
        .signatures
        .first()
        .context("missing transaction signature")?;

    let message_hash = compute_message_hash_versioned(&tx.message)?;
    let header = tx.message.header();

    let tx_signatures = tx
        .signatures
        .iter()
        .map(|sig| Array(*sig.as_array()))
        .collect();
    let tx_account_keys = tx
        .message
        .static_account_keys()
        .iter()
        .map(|key| Array(*key.as_array()))
        .collect();
    let tx_recent_blockhash =
        bytes_to_array::<32>(tx.message.recent_blockhash().as_ref()).context("recent blockhash")?;

    let (tx_instructions_program_id_index, tx_instructions_accounts, tx_instructions_data) =
        convert_rpc_instructions(tx.message.instructions())?;

    let (
        tx_address_table_lookup_account_key,
        tx_address_table_lookup_writable_indexes,
        tx_address_table_lookup_readonly_indexes,
    ) = convert_rpc_address_table_lookups(tx.message.address_table_lookups())?;

    let tx_address_table_lookups_present = tx
        .message
        .address_table_lookups()
        .is_some_and(|lookups| !lookups.is_empty());

    let is_vote = if is_simple_vote_transaction(tx)? {
        1
    } else {
        0
    };
    let tx_version = match tx.version() {
        TransactionVersion::LEGACY => None,
        TransactionVersion::Number(version) => {
            if version > max_supported_tx_version {
                return Err(anyhow!(
                    "unsupported transaction version {version}; max supported is {max_supported_tx_version}"
                ));
            }
            Some(version)
        }
    };

    let meta_fields = map_native_meta_fields(meta)?;

    Ok(TransactionRow {
        signature: Array(*signature.as_array()),
        slot,
        slot_idx,
        block_time,
        message_hash,
        is_vote,
        tx_version,
        tx_signatures,
        tx_num_required_signatures: header.num_required_signatures,
        tx_num_readonly_signed_accounts: header.num_readonly_signed_accounts,
        tx_num_readonly_unsigned_accounts: header.num_readonly_unsigned_accounts,
        tx_account_keys,
        tx_recent_blockhash,
        tx_instructions_program_id_index,
        tx_instructions_accounts,
        tx_instructions_data,
        tx_address_table_lookups_present: if tx_address_table_lookups_present {
            1
        } else {
            0
        },
        tx_address_table_lookup_account_key,
        tx_address_table_lookup_writable_indexes,
        tx_address_table_lookup_readonly_indexes,
        meta_status_ok: meta_fields.meta_status_ok,
        meta_err: meta_fields.meta_err,
        meta_fee: meta_fields.meta_fee,
        meta_pre_balances: meta_fields.meta_pre_balances,
        meta_post_balances: meta_fields.meta_post_balances,
        meta_inner_instructions_present: meta_fields.meta_inner_instructions_present,
        meta_inner_instructions_index: meta_fields.meta_inner_instructions_index,
        meta_inner_instructions_program_id_index: meta_fields
            .meta_inner_instructions_program_id_index,
        meta_inner_instructions_accounts: meta_fields.meta_inner_instructions_accounts,
        meta_inner_instructions_data: meta_fields.meta_inner_instructions_data,
        meta_inner_instructions_stack_height: meta_fields.meta_inner_instructions_stack_height,
        meta_log_messages_present: meta_fields.meta_log_messages_present,
        meta_log_messages: meta_fields.meta_log_messages,
        meta_pre_token_balances_present: meta_fields.meta_pre_token_balances_present,
        meta_pre_token_account_index: meta_fields.meta_pre_token_account_index,
        meta_pre_token_mint: meta_fields.meta_pre_token_mint,
        meta_pre_token_owner: meta_fields.meta_pre_token_owner,
        meta_pre_token_program_id: meta_fields.meta_pre_token_program_id,
        meta_pre_token_amount: meta_fields.meta_pre_token_amount,
        meta_pre_token_decimals: meta_fields.meta_pre_token_decimals,
        meta_pre_token_ui_amount: meta_fields.meta_pre_token_ui_amount,
        meta_pre_token_ui_amount_string: meta_fields.meta_pre_token_ui_amount_string,
        meta_post_token_balances_present: meta_fields.meta_post_token_balances_present,
        meta_post_token_account_index: meta_fields.meta_post_token_account_index,
        meta_post_token_mint: meta_fields.meta_post_token_mint,
        meta_post_token_owner: meta_fields.meta_post_token_owner,
        meta_post_token_program_id: meta_fields.meta_post_token_program_id,
        meta_post_token_amount: meta_fields.meta_post_token_amount,
        meta_post_token_decimals: meta_fields.meta_post_token_decimals,
        meta_post_token_ui_amount: meta_fields.meta_post_token_ui_amount,
        meta_post_token_ui_amount_string: meta_fields.meta_post_token_ui_amount_string,
        meta_rewards_present: meta_fields.meta_rewards_present,
        meta_reward_pubkey: meta_fields.meta_reward_pubkey,
        meta_reward_lamports: meta_fields.meta_reward_lamports,
        meta_reward_post_balance: meta_fields.meta_reward_post_balance,
        meta_reward_type: meta_fields.meta_reward_type,
        meta_reward_commission: meta_fields.meta_reward_commission,
        meta_loaded_addresses_writable: meta_fields.meta_loaded_addresses_writable,
        meta_loaded_addresses_readonly: meta_fields.meta_loaded_addresses_readonly,
        meta_return_data_present: meta_fields.meta_return_data_present,
        meta_return_data_program_id: meta_fields.meta_return_data_program_id,
        meta_return_data_data: meta_fields.meta_return_data_data,
        meta_compute_units_consumed: meta_fields.meta_compute_units_consumed,
        meta_cost_units: meta_fields.meta_cost_units,
    })
}

struct RpcMetaFields {
    meta_status_ok: u8,
    meta_err: Option<String>,
    meta_fee: u64,
    meta_pre_balances: Vec<u64>,
    meta_post_balances: Vec<u64>,
    meta_inner_instructions_present: u8,
    meta_inner_instructions_index: Vec<u8>,
    meta_inner_instructions_program_id_index: Vec<Vec<u8>>,
    meta_inner_instructions_accounts: Vec<Vec<Vec<u8>>>,
    meta_inner_instructions_data: Vec<Vec<serde_bytes::ByteBuf>>,
    meta_inner_instructions_stack_height: Vec<Vec<Option<u32>>>,
    meta_log_messages_present: u8,
    meta_log_messages: Vec<String>,
    meta_pre_token_balances_present: u8,
    meta_pre_token_account_index: Vec<u8>,
    meta_pre_token_mint: Vec<Array<u8, 32>>,
    meta_pre_token_owner: Vec<Option<Array<u8, 32>>>,
    meta_pre_token_program_id: Vec<Option<Array<u8, 32>>>,
    meta_pre_token_amount: Vec<String>,
    meta_pre_token_decimals: Vec<u8>,
    meta_pre_token_ui_amount: Vec<Option<f64>>,
    meta_pre_token_ui_amount_string: Vec<String>,
    meta_post_token_balances_present: u8,
    meta_post_token_account_index: Vec<u8>,
    meta_post_token_mint: Vec<Array<u8, 32>>,
    meta_post_token_owner: Vec<Option<Array<u8, 32>>>,
    meta_post_token_program_id: Vec<Option<Array<u8, 32>>>,
    meta_post_token_amount: Vec<String>,
    meta_post_token_decimals: Vec<u8>,
    meta_post_token_ui_amount: Vec<Option<f64>>,
    meta_post_token_ui_amount_string: Vec<String>,
    meta_rewards_present: u8,
    meta_reward_pubkey: Vec<String>,
    meta_reward_lamports: Vec<i64>,
    meta_reward_post_balance: Vec<u64>,
    meta_reward_type: Vec<Option<String>>,
    meta_reward_commission: Vec<Option<u8>>,
    meta_loaded_addresses_writable: Vec<Array<u8, 32>>,
    meta_loaded_addresses_readonly: Vec<Array<u8, 32>>,
    meta_return_data_present: u8,
    meta_return_data_program_id: Option<Array<u8, 32>>,
    meta_return_data_data: Option<serde_bytes::ByteBuf>,
    meta_compute_units_consumed: Option<u64>,
    meta_cost_units: Option<u64>,
}

impl RpcMetaFields {
    fn missing() -> Self {
        Self {
            meta_status_ok: 1,
            meta_err: None,
            meta_fee: 0,
            meta_pre_balances: Vec::new(),
            meta_post_balances: Vec::new(),
            meta_inner_instructions_present: 0,
            meta_inner_instructions_index: Vec::new(),
            meta_inner_instructions_program_id_index: Vec::new(),
            meta_inner_instructions_accounts: Vec::new(),
            meta_inner_instructions_data: Vec::new(),
            meta_inner_instructions_stack_height: Vec::new(),
            meta_log_messages_present: 0,
            meta_log_messages: Vec::new(),
            meta_pre_token_balances_present: 0,
            meta_pre_token_account_index: Vec::new(),
            meta_pre_token_mint: Vec::new(),
            meta_pre_token_owner: Vec::new(),
            meta_pre_token_program_id: Vec::new(),
            meta_pre_token_amount: Vec::new(),
            meta_pre_token_decimals: Vec::new(),
            meta_pre_token_ui_amount: Vec::new(),
            meta_pre_token_ui_amount_string: Vec::new(),
            meta_post_token_balances_present: 0,
            meta_post_token_account_index: Vec::new(),
            meta_post_token_mint: Vec::new(),
            meta_post_token_owner: Vec::new(),
            meta_post_token_program_id: Vec::new(),
            meta_post_token_amount: Vec::new(),
            meta_post_token_decimals: Vec::new(),
            meta_post_token_ui_amount: Vec::new(),
            meta_post_token_ui_amount_string: Vec::new(),
            meta_rewards_present: 0,
            meta_reward_pubkey: Vec::new(),
            meta_reward_lamports: Vec::new(),
            meta_reward_post_balance: Vec::new(),
            meta_reward_type: Vec::new(),
            meta_reward_commission: Vec::new(),
            meta_loaded_addresses_writable: Vec::new(),
            meta_loaded_addresses_readonly: Vec::new(),
            meta_return_data_present: 0,
            meta_return_data_program_id: None,
            meta_return_data_data: None,
            meta_compute_units_consumed: None,
            meta_cost_units: None,
        }
    }
}

fn map_rpc_meta_fields(meta: Option<&UiTransactionStatusMeta>) -> Result<RpcMetaFields> {
    let Some(meta) = meta else {
        return Ok(RpcMetaFields::missing());
    };

    let meta_err = meta
        .err
        .as_ref()
        .map(|err| serde_json::to_string(err).unwrap_or_else(|_| format!("{err:?}")));
    let meta_status_ok = if meta.err.is_some() { 0 } else { 1 };

    let (
        meta_inner_instructions_present,
        meta_inner_instructions_index,
        meta_inner_instructions_program_id_index,
        meta_inner_instructions_accounts,
        meta_inner_instructions_data,
        meta_inner_instructions_stack_height,
    ) = convert_rpc_inner_instructions(&meta.inner_instructions)?;

    let (meta_log_messages_present, meta_log_messages) =
        convert_rpc_log_messages(&meta.log_messages);

    let (
        meta_pre_token_balances_present,
        meta_pre_token_account_index,
        meta_pre_token_mint,
        meta_pre_token_owner,
        meta_pre_token_program_id,
        meta_pre_token_amount,
        meta_pre_token_decimals,
        meta_pre_token_ui_amount,
        meta_pre_token_ui_amount_string,
    ) = convert_rpc_token_balances(&meta.pre_token_balances)?;

    let (
        meta_post_token_balances_present,
        meta_post_token_account_index,
        meta_post_token_mint,
        meta_post_token_owner,
        meta_post_token_program_id,
        meta_post_token_amount,
        meta_post_token_decimals,
        meta_post_token_ui_amount,
        meta_post_token_ui_amount_string,
    ) = convert_rpc_token_balances(&meta.post_token_balances)?;

    let (
        meta_rewards_present,
        meta_reward_pubkey,
        meta_reward_lamports,
        meta_reward_post_balance,
        meta_reward_type,
        meta_reward_commission,
    ) = convert_rpc_rewards(&meta.rewards)?;

    let (meta_loaded_addresses_writable, meta_loaded_addresses_readonly) =
        convert_rpc_loaded_addresses(&meta.loaded_addresses)?;

    let (meta_return_data_present, meta_return_data_program_id, meta_return_data_data) =
        convert_rpc_return_data(&meta.return_data)?;

    let meta_compute_units_consumed = option_serializer_to_option(&meta.compute_units_consumed);
    let meta_cost_units = option_serializer_to_option(&meta.cost_units);

    Ok(RpcMetaFields {
        meta_status_ok,
        meta_err,
        meta_fee: meta.fee,
        meta_pre_balances: meta.pre_balances.clone(),
        meta_post_balances: meta.post_balances.clone(),
        meta_inner_instructions_present,
        meta_inner_instructions_index,
        meta_inner_instructions_program_id_index,
        meta_inner_instructions_accounts,
        meta_inner_instructions_data,
        meta_inner_instructions_stack_height,
        meta_log_messages_present,
        meta_log_messages,
        meta_pre_token_balances_present,
        meta_pre_token_account_index,
        meta_pre_token_mint,
        meta_pre_token_owner,
        meta_pre_token_program_id,
        meta_pre_token_amount,
        meta_pre_token_decimals,
        meta_pre_token_ui_amount,
        meta_pre_token_ui_amount_string,
        meta_post_token_balances_present,
        meta_post_token_account_index,
        meta_post_token_mint,
        meta_post_token_owner,
        meta_post_token_program_id,
        meta_post_token_amount,
        meta_post_token_decimals,
        meta_post_token_ui_amount,
        meta_post_token_ui_amount_string,
        meta_rewards_present,
        meta_reward_pubkey,
        meta_reward_lamports,
        meta_reward_post_balance,
        meta_reward_type,
        meta_reward_commission,
        meta_loaded_addresses_writable,
        meta_loaded_addresses_readonly,
        meta_return_data_present,
        meta_return_data_program_id,
        meta_return_data_data,
        meta_compute_units_consumed,
        meta_cost_units,
    })
}

fn map_native_meta_fields(meta: Option<&TransactionStatusMeta>) -> Result<RpcMetaFields> {
    let Some(meta) = meta else {
        return Ok(RpcMetaFields::missing());
    };

    let meta_err = meta.status.as_ref().err().map(|err| {
        let ui_err = UiTransactionError::from(err.clone());
        serde_json::to_string(&ui_err).unwrap_or_else(|_| format!("{err:?}"))
    });
    let meta_status_ok = if meta.status.is_ok() { 1 } else { 0 };

    let (
        meta_inner_instructions_present,
        meta_inner_instructions_index,
        meta_inner_instructions_program_id_index,
        meta_inner_instructions_accounts,
        meta_inner_instructions_data,
        meta_inner_instructions_stack_height,
    ) = convert_native_inner_instructions(&meta.inner_instructions)?;

    let (meta_log_messages_present, meta_log_messages) =
        convert_native_log_messages(&meta.log_messages);

    let (
        meta_pre_token_balances_present,
        meta_pre_token_account_index,
        meta_pre_token_mint,
        meta_pre_token_owner,
        meta_pre_token_program_id,
        meta_pre_token_amount,
        meta_pre_token_decimals,
        meta_pre_token_ui_amount,
        meta_pre_token_ui_amount_string,
    ) = convert_native_token_balances(&meta.pre_token_balances)?;

    let (
        meta_post_token_balances_present,
        meta_post_token_account_index,
        meta_post_token_mint,
        meta_post_token_owner,
        meta_post_token_program_id,
        meta_post_token_amount,
        meta_post_token_decimals,
        meta_post_token_ui_amount,
        meta_post_token_ui_amount_string,
    ) = convert_native_token_balances(&meta.post_token_balances)?;

    let (
        meta_rewards_present,
        meta_reward_pubkey,
        meta_reward_lamports,
        meta_reward_post_balance,
        meta_reward_type,
        meta_reward_commission,
    ) = convert_native_rewards(&meta.rewards)?;

    let (meta_loaded_addresses_writable, meta_loaded_addresses_readonly) =
        convert_native_loaded_addresses(&meta.loaded_addresses)?;

    let (meta_return_data_present, meta_return_data_program_id, meta_return_data_data) =
        match &meta.return_data {
            Some(return_data) => (
                1,
                Some(Array(*return_data.program_id.as_array())),
                Some(serde_bytes::ByteBuf::from(return_data.data.clone())),
            ),
            None => (0, None, None),
        };

    Ok(RpcMetaFields {
        meta_status_ok,
        meta_err,
        meta_fee: meta.fee,
        meta_pre_balances: meta.pre_balances.clone(),
        meta_post_balances: meta.post_balances.clone(),
        meta_inner_instructions_present,
        meta_inner_instructions_index,
        meta_inner_instructions_program_id_index,
        meta_inner_instructions_accounts,
        meta_inner_instructions_data,
        meta_inner_instructions_stack_height,
        meta_log_messages_present,
        meta_log_messages,
        meta_pre_token_balances_present,
        meta_pre_token_account_index,
        meta_pre_token_mint,
        meta_pre_token_owner,
        meta_pre_token_program_id,
        meta_pre_token_amount,
        meta_pre_token_decimals,
        meta_pre_token_ui_amount,
        meta_pre_token_ui_amount_string,
        meta_post_token_balances_present,
        meta_post_token_account_index,
        meta_post_token_mint,
        meta_post_token_owner,
        meta_post_token_program_id,
        meta_post_token_amount,
        meta_post_token_decimals,
        meta_post_token_ui_amount,
        meta_post_token_ui_amount_string,
        meta_rewards_present,
        meta_reward_pubkey,
        meta_reward_lamports,
        meta_reward_post_balance,
        meta_reward_type,
        meta_reward_commission,
        meta_loaded_addresses_writable,
        meta_loaded_addresses_readonly,
        meta_return_data_present,
        meta_return_data_program_id,
        meta_return_data_data,
        meta_compute_units_consumed: meta.compute_units_consumed,
        meta_cost_units: meta.cost_units,
    })
}

fn option_serializer_to_option<T: Copy>(value: &OptionSerializer<T>) -> Option<T> {
    match value {
        OptionSerializer::Some(val) => Some(*val),
        OptionSerializer::None | OptionSerializer::Skip => None,
    }
}

fn convert_rpc_log_messages(value: &OptionSerializer<Vec<String>>) -> (u8, Vec<String>) {
    match value {
        OptionSerializer::Some(logs) => (1, logs.clone()),
        OptionSerializer::None | OptionSerializer::Skip => (0, Vec::new()),
    }
}

fn convert_native_log_messages(value: &Option<Vec<String>>) -> (u8, Vec<String>) {
    match value {
        Some(logs) => (1, logs.clone()),
        None => (0, Vec::new()),
    }
}

fn convert_native_inner_instructions(
    inner: &Option<Vec<InnerInstructions>>,
) -> Result<InnerInstructionFields> {
    let Some(groups) = inner else {
        return Ok((
            0,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ));
    };

    let mut indexes = Vec::with_capacity(groups.len());
    let mut program_ids = Vec::with_capacity(groups.len());
    let mut accounts = Vec::with_capacity(groups.len());
    let mut data = Vec::with_capacity(groups.len());
    let mut stack_heights = Vec::with_capacity(groups.len());

    for group in groups {
        indexes.push(group.index);

        let mut group_program_ids = Vec::with_capacity(group.instructions.len());
        let mut group_accounts = Vec::with_capacity(group.instructions.len());
        let mut group_data = Vec::with_capacity(group.instructions.len());
        let mut group_stack = Vec::with_capacity(group.instructions.len());

        for instruction in &group.instructions {
            let InnerInstruction {
                instruction,
                stack_height,
            } = instruction;
            group_program_ids.push(instruction.program_id_index);
            group_accounts.push(instruction.accounts.clone());
            group_data.push(serde_bytes::ByteBuf::from(instruction.data.clone()));
            group_stack.push(*stack_height);
        }

        program_ids.push(group_program_ids);
        accounts.push(group_accounts);
        data.push(group_data);
        stack_heights.push(group_stack);
    }

    Ok((1, indexes, program_ids, accounts, data, stack_heights))
}

#[allow(clippy::type_complexity)]
fn convert_native_token_balances(
    balances: &Option<Vec<TransactionTokenBalance>>,
) -> Result<(
    u8,
    Vec<u8>,
    Vec<Array<u8, 32>>,
    Vec<Option<Array<u8, 32>>>,
    Vec<Option<Array<u8, 32>>>,
    Vec<String>,
    Vec<u8>,
    Vec<Option<f64>>,
    Vec<String>,
)> {
    let Some(balances) = balances else {
        return Ok((
            0,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ));
    };

    let mut account_indexes = Vec::with_capacity(balances.len());
    let mut mints = Vec::with_capacity(balances.len());
    let mut owners = Vec::with_capacity(balances.len());
    let mut program_ids = Vec::with_capacity(balances.len());
    let mut amounts = Vec::with_capacity(balances.len());
    let mut decimals = Vec::with_capacity(balances.len());
    let mut ui_amounts = Vec::with_capacity(balances.len());
    let mut ui_amount_strings = Vec::with_capacity(balances.len());

    for balance in balances {
        account_indexes.push(balance.account_index);
        mints.push(decode_base58_32(&balance.mint).context("decode token mint")?);
        owners.push(decode_optional_pubkey(&balance.owner)?);
        program_ids.push(decode_optional_pubkey(&balance.program_id)?);

        let ui = &balance.ui_token_amount;
        amounts.push(ui.amount.clone());
        decimals.push(ui.decimals);
        ui_amounts.push(ui.ui_amount);
        ui_amount_strings.push(ui.ui_amount_string.clone());
    }

    Ok((
        1,
        account_indexes,
        mints,
        owners,
        program_ids,
        amounts,
        decimals,
        ui_amounts,
        ui_amount_strings,
    ))
}

fn decode_optional_pubkey(value: &str) -> Result<Option<Array<u8, 32>>> {
    if value.is_empty() {
        Ok(None)
    } else {
        Ok(Some(decode_base58_32(value)?))
    }
}

fn convert_native_rewards(rewards: &Option<Vec<UiReward>>) -> Result<RewardsFields> {
    let Some(rewards) = rewards else {
        return Ok((
            0,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ));
    };

    let mut pubkeys = Vec::with_capacity(rewards.len());
    let mut lamports = Vec::with_capacity(rewards.len());
    let mut post_balances = Vec::with_capacity(rewards.len());
    let mut reward_types = Vec::with_capacity(rewards.len());
    let mut commissions = Vec::with_capacity(rewards.len());

    for reward in rewards {
        pubkeys.push(reward.pubkey.clone());
        lamports.push(reward.lamports);
        post_balances.push(reward.post_balance);
        reward_types.push(reward.reward_type.map(|ty| ty.to_string()));
        commissions.push(reward.commission);
    }

    Ok((
        if rewards.is_empty() { 0 } else { 1 },
        pubkeys,
        lamports,
        post_balances,
        reward_types,
        commissions,
    ))
}

fn convert_native_loaded_addresses(
    addresses: &solana_message::v0::LoadedAddresses,
) -> Result<LoadedAddressFields> {
    let mut writable = Vec::with_capacity(addresses.writable.len());
    let mut readonly = Vec::with_capacity(addresses.readonly.len());

    for key in &addresses.writable {
        writable.push(Array(*key.as_array()));
    }
    for key in &addresses.readonly {
        readonly.push(Array(*key.as_array()));
    }

    Ok((writable, readonly))
}

fn convert_rpc_instructions(
    instructions: &[solana_message::compiled_instruction::CompiledInstruction],
) -> Result<RpcInstructionFields> {
    let mut program_ids = Vec::with_capacity(instructions.len());
    let mut accounts = Vec::with_capacity(instructions.len());
    let mut data = Vec::with_capacity(instructions.len());

    for ix in instructions {
        program_ids.push(ix.program_id_index);
        accounts.push(ix.accounts.clone());
        data.push(serde_bytes::ByteBuf::from(ix.data.clone()));
    }

    Ok((program_ids, accounts, data))
}

fn convert_rpc_address_table_lookups(
    lookups: Option<&[solana_message::v0::MessageAddressTableLookup]>,
) -> Result<RpcAddressTableLookupFields> {
    let mut account_keys = Vec::new();
    let mut writable_indexes = Vec::new();
    let mut readonly_indexes = Vec::new();

    let Some(lookups) = lookups else {
        return Ok((account_keys, writable_indexes, readonly_indexes));
    };

    account_keys.reserve(lookups.len());
    writable_indexes.reserve(lookups.len());
    readonly_indexes.reserve(lookups.len());

    for lookup in lookups {
        account_keys.push(Array(*lookup.account_key.as_array()));
        writable_indexes.push(lookup.writable_indexes.clone());
        readonly_indexes.push(lookup.readonly_indexes.clone());
    }

    Ok((account_keys, writable_indexes, readonly_indexes))
}

fn convert_rpc_inner_instructions(
    inner: &OptionSerializer<Vec<solana_transaction_status_client_types::UiInnerInstructions>>,
) -> Result<InnerInstructionFields> {
    let OptionSerializer::Some(groups) = inner else {
        return Ok((
            0,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ));
    };

    let mut indexes = Vec::with_capacity(groups.len());
    let mut program_ids = Vec::with_capacity(groups.len());
    let mut accounts = Vec::with_capacity(groups.len());
    let mut data = Vec::with_capacity(groups.len());
    let mut stack_heights = Vec::with_capacity(groups.len());

    for group in groups {
        indexes.push(group.index);

        let mut group_program_ids = Vec::with_capacity(group.instructions.len());
        let mut group_accounts = Vec::with_capacity(group.instructions.len());
        let mut group_data = Vec::with_capacity(group.instructions.len());
        let mut group_stack = Vec::with_capacity(group.instructions.len());

        for instruction in &group.instructions {
            match instruction {
                UiInstruction::Compiled(compiled) => {
                    group_program_ids.push(compiled.program_id_index);
                    group_accounts.push(compiled.accounts.clone());
                    let decoded = bs58::decode(&compiled.data)
                        .into_vec()
                        .context("decode inner instruction data")?;
                    group_data.push(serde_bytes::ByteBuf::from(decoded));
                    group_stack.push(compiled.stack_height);
                }
                UiInstruction::Parsed(_) => {
                    return Err(anyhow!(
                        "parsed inner instruction not supported; use base58/base64 encoding"
                    ));
                }
            }
        }

        program_ids.push(group_program_ids);
        accounts.push(group_accounts);
        data.push(group_data);
        stack_heights.push(group_stack);
    }

    Ok((1, indexes, program_ids, accounts, data, stack_heights))
}

#[allow(clippy::type_complexity)]
fn convert_rpc_token_balances(
    balances: &OptionSerializer<Vec<UiTransactionTokenBalance>>,
) -> Result<(
    u8,
    Vec<u8>,
    Vec<Array<u8, 32>>,
    Vec<Option<Array<u8, 32>>>,
    Vec<Option<Array<u8, 32>>>,
    Vec<String>,
    Vec<u8>,
    Vec<Option<f64>>,
    Vec<String>,
)> {
    let OptionSerializer::Some(balances) = balances else {
        return Ok((
            0,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ));
    };

    let mut account_indexes = Vec::with_capacity(balances.len());
    let mut mints = Vec::with_capacity(balances.len());
    let mut owners = Vec::with_capacity(balances.len());
    let mut program_ids = Vec::with_capacity(balances.len());
    let mut amounts = Vec::with_capacity(balances.len());
    let mut decimals = Vec::with_capacity(balances.len());
    let mut ui_amounts = Vec::with_capacity(balances.len());
    let mut ui_amount_strings = Vec::with_capacity(balances.len());

    for balance in balances {
        account_indexes.push(balance.account_index);
        mints.push(decode_base58_32(&balance.mint).context("decode token mint")?);
        owners.push(decode_option_serializer_pubkey(&balance.owner)?);
        program_ids.push(decode_option_serializer_pubkey(&balance.program_id)?);

        let ui = &balance.ui_token_amount;
        amounts.push(ui.amount.clone());
        decimals.push(ui.decimals);
        ui_amounts.push(ui.ui_amount);
        ui_amount_strings.push(ui.ui_amount_string.clone());
    }

    Ok((
        1,
        account_indexes,
        mints,
        owners,
        program_ids,
        amounts,
        decimals,
        ui_amounts,
        ui_amount_strings,
    ))
}

fn decode_option_serializer_pubkey(
    value: &OptionSerializer<String>,
) -> Result<Option<Array<u8, 32>>> {
    match value {
        OptionSerializer::Some(value) => {
            if value.is_empty() {
                Ok(None)
            } else {
                Ok(Some(decode_base58_32(value)?))
            }
        }
        OptionSerializer::None | OptionSerializer::Skip => Ok(None),
    }
}

fn convert_rpc_rewards(rewards: &OptionSerializer<Vec<UiReward>>) -> Result<RewardsFields> {
    let OptionSerializer::Some(rewards) = rewards else {
        return Ok((
            0,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ));
    };

    let mut pubkeys = Vec::with_capacity(rewards.len());
    let mut lamports = Vec::with_capacity(rewards.len());
    let mut post_balances = Vec::with_capacity(rewards.len());
    let mut reward_types = Vec::with_capacity(rewards.len());
    let mut commissions = Vec::with_capacity(rewards.len());

    for reward in rewards {
        pubkeys.push(reward.pubkey.clone());
        lamports.push(reward.lamports);
        post_balances.push(reward.post_balance);
        reward_types.push(reward.reward_type.map(|ty| ty.to_string()));
        commissions.push(reward.commission);
    }

    Ok((
        if rewards.is_empty() { 0 } else { 1 },
        pubkeys,
        lamports,
        post_balances,
        reward_types,
        commissions,
    ))
}

fn convert_rpc_loaded_addresses(
    addresses: &OptionSerializer<solana_transaction_status_client_types::UiLoadedAddresses>,
) -> Result<LoadedAddressFields> {
    let OptionSerializer::Some(addresses) = addresses else {
        return Ok((Vec::new(), Vec::new()));
    };

    let mut writable = Vec::with_capacity(addresses.writable.len());
    let mut readonly = Vec::with_capacity(addresses.readonly.len());

    for key in &addresses.writable {
        writable.push(decode_base58_32(key).context("decode loaded writable address")?);
    }
    for key in &addresses.readonly {
        readonly.push(decode_base58_32(key).context("decode loaded readonly address")?);
    }

    Ok((writable, readonly))
}

fn convert_rpc_return_data(
    return_data: &OptionSerializer<solana_transaction_status_client_types::UiTransactionReturnData>,
) -> Result<(u8, Option<Array<u8, 32>>, Option<serde_bytes::ByteBuf>)> {
    let OptionSerializer::Some(return_data) = return_data else {
        return Ok((0, None, None));
    };

    let (data, encoding) = &return_data.data;
    if *encoding != UiReturnDataEncoding::Base64 {
        return Err(anyhow!("unsupported return data encoding"));
    }

    let program_id =
        decode_base58_32(&return_data.program_id).context("decode return data program id")?;
    let decoded = BASE64_STANDARD.decode(data).context("decode return data")?;
    Ok((
        1,
        Some(program_id),
        Some(serde_bytes::ByteBuf::from(decoded)),
    ))
}

fn convert_rpc_block_rewards(rewards: Option<&Vec<UiReward>>) -> Result<RpcBlockRewardsFields> {
    let Some(rewards) = rewards else {
        return Ok((
            0,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ));
    };

    let mut pubkeys = Vec::with_capacity(rewards.len());
    let mut lamports = Vec::with_capacity(rewards.len());
    let mut post_balances = Vec::with_capacity(rewards.len());
    let mut reward_types = Vec::with_capacity(rewards.len());
    let mut commissions = Vec::with_capacity(rewards.len());

    for reward in rewards {
        pubkeys.push(decode_base58_32(&reward.pubkey).context("decode reward pubkey")?);
        lamports.push(reward.lamports);
        post_balances.push(reward.post_balance);
        reward_types.push(reward.reward_type.map(|ty| ty.to_string()));
        commissions.push(reward.commission);
    }

    Ok((
        if rewards.is_empty() { 0 } else { 1 },
        pubkeys,
        lamports,
        post_balances,
        reward_types,
        commissions,
    ))
}

fn is_simple_vote_transaction(tx: &VersionedTransaction) -> Result<bool> {
    let is_legacy_message = matches!(tx.message, VersionedMessage::Legacy(_));
    if !is_legacy_message {
        return Ok(false);
    }
    if tx.signatures.len() >= 3 {
        return Ok(false);
    }
    let instructions = tx.message.instructions();
    if instructions.len() != 1 {
        return Ok(false);
    }
    let program_id_index = instructions[0].program_id_index as usize;
    let account_keys = tx.message.static_account_keys();
    if program_id_index >= account_keys.len() {
        return Ok(false);
    }
    let vote_id = vote_program_id_bytes()?;
    Ok(account_keys[program_id_index].as_array() == &vote_id.0)
}

fn vote_program_id_bytes() -> Result<&'static Array<u8, 32>> {
    static VOTE_PROGRAM_ID: OnceLock<Result<Array<u8, 32>>> = OnceLock::new();
    match VOTE_PROGRAM_ID
        .get_or_init(|| decode_base58_32("Vote111111111111111111111111111111111111111"))
    {
        Ok(value) => Ok(value),
        Err(err) => Err(anyhow!("decode vote program id: {err}")),
    }
}

fn compute_message_hash_versioned(message: &VersionedMessage) -> Result<Array<u8, 32>> {
    let message_bytes =
        crate::message_wire::serialize_versioned_message(message).context("serialize message")?;

    let mut hasher = Hasher::new();
    hasher.update(b"solana-tx-message-v1");
    hasher.update(&message_bytes);

    let hash_bytes: [u8; 32] = hasher.finalize().into();
    Ok(Array(hash_bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_address::Address;
    use solana_hash::Hash;
    use solana_message::{
        MessageHeader, compiled_instruction::CompiledInstruction, legacy::Message,
    };
    use solana_transaction_status_client_types::{EncodedTransaction, TransactionBinaryEncoding};

    fn build_test_transaction() -> VersionedTransaction {
        let payer = Address::from([1u8; 32]);
        let program = Address::from([2u8; 32]);
        let instruction = CompiledInstruction {
            program_id_index: 1,
            accounts: vec![0],
            data: vec![1, 2, 3],
        };
        let message = Message {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![payer, program],
            recent_blockhash: Hash::default(),
            instructions: vec![instruction],
        };
        VersionedTransaction {
            signatures: vec![Default::default()],
            message: VersionedMessage::Legacy(message),
        }
    }

    #[test]
    fn rpc_missing_meta_creates_empty_fields() {
        let tx = build_test_transaction();
        let tx_bytes = crate::message_wire::serialize_versioned_transaction(&tx)
            .expect("serialize transaction");
        let encoded = EncodedTransaction::Binary(
            BASE64_STANDARD.encode(tx_bytes),
            TransactionBinaryEncoding::Base64,
        );
        let tx_with_meta = EncodedTransactionWithStatusMeta {
            transaction: encoded,
            meta: None,
            version: None,
        };

        let row =
            map_rpc_transaction(42, Some(123), 0, &tx_with_meta).expect("map rpc transaction");

        assert_eq!(row.meta_status_ok, 1);
        assert!(row.meta_err.is_none());
        assert_eq!(row.meta_fee, 0);
        assert_eq!(row.meta_log_messages_present, 0);
        assert_eq!(row.meta_inner_instructions_present, 0);
        assert_eq!(row.meta_rewards_present, 0);
        assert!(row.meta_compute_units_consumed.is_none());
        assert!(row.meta_cost_units.is_none());
    }
}
