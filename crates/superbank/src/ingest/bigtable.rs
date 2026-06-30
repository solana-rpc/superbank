// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

#![allow(deprecated)]

use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow};
use solana_storage_bigtable::{CredentialType, LedgerStorage, LedgerStorageConfig};
use solana_transaction_status::ConfirmedBlock;
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, BufReader},
    sync::{Semaphore, mpsc, watch},
    task::JoinSet,
};
use tracing::{info, warn};

use crate::cli::Args;
use crate::clickhouse::{
    BlockMetadataRow, InsertTables, RetryConfig, TransactionRow, build_clickhouse_client,
    flush_buffers_with_retry,
};
use crate::commitment::parse_commitment_config;
use crate::ingest::rpc::{map_bigtable_block_metadata, map_bigtable_transactions};
use crate::metrics;
use crate::range::{RangeSpec, parse_range_spec};
use crate::rpc_client::build_rpc_client;
use crate::shutdown::spawn_shutdown_watch;

#[derive(Clone, Copy)]
struct SlotRange {
    start: u64,
    end: u64,
}

struct BigtableBatch {
    transaction_rows: Vec<TransactionRow>,
    block_rows: Vec<BlockMetadataRow>,
    last_slot: Option<u64>,
}

struct BigtableInserterOutcome {
    shutdown_requested: bool,
}

#[derive(Clone, Copy)]
struct BigtableFlushConfig {
    transactions_flush_rows: usize,
    blocks_flush_rows: usize,
    flush_interval_secs: u64,
    flush_every_block: bool,
}

struct BigtableFlushBatch {
    transaction_rows: Vec<TransactionRow>,
    block_rows: Vec<BlockMetadataRow>,
}

struct BigtableInserterArgs {
    clickhouse: clickhouse::Client,
    insert_tables: InsertTables,
    flush_config: BigtableFlushConfig,
    insert_concurrency: usize,
    total_slots: u64,
    progress_every: u64,
    result_rx: mpsc::Receiver<Result<BigtableBatch>>,
    shutdown_rx: watch::Receiver<u64>,
    fatal: Arc<AtomicBool>,
    retry_config: Arc<RetryConfig>,
}

async fn load_slot_list(path: &Path) -> Result<Vec<u64>> {
    let file = File::open(path)
        .await
        .with_context(|| format!("open slot list {}", path.display()))?;
    let mut lines = BufReader::new(file).lines();
    let mut slots = Vec::new();
    let mut line_number = 0usize;

    while let Some(line) = lines
        .next_line()
        .await
        .with_context(|| format!("read slot list {}", path.display()))?
    {
        line_number = line_number.saturating_add(1);
        for token in line.split_whitespace() {
            if token.starts_with('#') {
                break;
            }
            let slot = token.parse::<u64>().with_context(|| {
                format!(
                    "invalid slot '{}' on line {} in {}",
                    token,
                    line_number,
                    path.display()
                )
            })?;
            slots.push(slot);
        }
    }

    if slots.is_empty() {
        return Err(anyhow!("slot list {} was empty", path.display()));
    }

    slots.sort_unstable();
    slots.dedup();
    Ok(slots)
}

pub(crate) async fn run_bigtable_ingest(args: &Args) -> Result<()> {
    let slot_list = if let Some(slot_file) = args.bigtable_slot_file.as_ref() {
        Some(load_slot_list(slot_file).await?)
    } else {
        None
    };

    let range_spec_label = if slot_list.is_none() {
        Some(args.bigtable_range.as_ref().ok_or_else(|| {
            anyhow!("bigtable source requires --bigtable-range / BIGTABLE_RANGE / config")
        })?)
    } else {
        None
    };

    let slot_range = match slot_list.as_ref() {
        Some(slots) => SlotRange {
            start: *slots.first().ok_or_else(|| anyhow!("slot list is empty"))?,
            end: *slots.last().ok_or_else(|| anyhow!("slot list is empty"))?,
        },
        None => {
            let range_spec = parse_range_spec(range_spec_label.unwrap())?;
            resolve_slot_range(args, range_spec).await?
        }
    };

    let storage = Arc::new(build_bigtable_storage(args).await?);
    let clickhouse = build_clickhouse_client(args);
    let insert_tables = InsertTables::from_args(args);
    let retry_config = Arc::new(RetryConfig {
        max_retries: args.insert_max_retries,
        base_ms: args.insert_retry_base_ms,
        max_ms: args.insert_retry_max_ms,
    });
    let flush_config = BigtableFlushConfig {
        transactions_flush_rows: args.transactions_flush_rows,
        blocks_flush_rows: args.blocks_flush_rows,
        flush_interval_secs: args.flush_interval_secs,
        flush_every_block: args.flush_every_block,
    };

    if let Some(slot_file) = args.bigtable_slot_file.as_ref() {
        info!(
            source = "bigtable",
            slot_list_path = %slot_file.display(),
            slot_count = slot_list.as_ref().map(|slots| slots.len()).unwrap_or(0),
            range_start = slot_range.start,
            range_end = slot_range.end,
            transactions_table = %args.transactions_table,
            blocks_table = %args.blocks_table,
            "starting superbank ingest"
        );
    } else {
        info!(
            source = "bigtable",
            range_start = slot_range.start,
            range_end = slot_range.end,
            range_spec = %range_spec_label.unwrap(),
            transactions_table = %args.transactions_table,
            blocks_table = %args.blocks_table,
            "starting superbank ingest"
        );
    }

    let total_slots = match slot_list.as_ref() {
        Some(slots) => slots.len() as u64,
        None => slot_range
            .end
            .saturating_sub(slot_range.start)
            .saturating_add(1),
    };
    let progress_every = args.bigtable_progress_every_slots.max(1);
    let mut shutdown_rx = spawn_shutdown_watch();
    let fatal = Arc::new(AtomicBool::new(false));
    let fetch_concurrency = args.bigtable_fetch_concurrency.max(1);
    let decode_concurrency = args.bigtable_decode_concurrency.max(1);
    let decode_semaphore = Arc::new(Semaphore::new(decode_concurrency));
    let result_capacity = fetch_concurrency
        .max(decode_concurrency)
        .saturating_mul(4)
        .clamp(100, 10_000);

    let (result_tx, result_rx) = mpsc::channel(result_capacity);
    let inserter_handle = tokio::spawn(run_bigtable_inserter(BigtableInserterArgs {
        clickhouse,
        insert_tables,
        flush_config,
        insert_concurrency: args.bigtable_insert_concurrency,
        total_slots,
        progress_every,
        result_rx,
        shutdown_rx: shutdown_rx.clone(),
        fatal: fatal.clone(),
        retry_config: retry_config.clone(),
    }));

    let semaphore = Arc::new(Semaphore::new(fetch_concurrency));
    let mut join_set = JoinSet::new();
    if let Some(slots) = slot_list {
        for slot_chunk in slots.chunks(args.bigtable_fetch_batch_size) {
            if *shutdown_rx.borrow() > 0 || fatal.load(Ordering::Relaxed) {
                info!("shutdown signal received; draining in-flight batches");
                break;
            }

            let slots = slot_chunk.to_vec();
            let storage = storage.clone();
            let result_tx = result_tx.clone();
            let permit = match semaphore.clone().acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => break,
            };
            let fatal = fatal.clone();
            let decode_semaphore = decode_semaphore.clone();
            let max_supported_tx_version = args.rpc_max_supported_tx_version;
            let flush_every_block = flush_config.flush_every_block;

            join_set.spawn(async move {
                let _permit = permit;
                if fatal.load(Ordering::Relaxed) {
                    return;
                }

                let blocks = match fetch_bigtable_slot_chunk(storage, slots).await {
                    Ok(blocks) => blocks,
                    Err(err) => {
                        metrics::observe_source_error("bigtable_fetch", "error");
                        fatal.store(true, Ordering::Relaxed);
                        let _ = result_tx.send(Err(err)).await;
                        return;
                    }
                };

                if blocks.is_empty() {
                    return;
                }

                let decode_permit = match decode_semaphore.acquire_owned().await {
                    Ok(permit) => permit,
                    Err(_) => return,
                };
                let result_tx = result_tx.clone();
                let fatal = fatal.clone();
                tokio::task::spawn_blocking(move || {
                    let _permit = decode_permit;
                    if fatal.load(Ordering::Relaxed) {
                        return;
                    }
                    if let Err(err) = decode_bigtable_blocks(
                        blocks,
                        max_supported_tx_version,
                        flush_every_block,
                        &result_tx,
                    ) {
                        metrics::observe_source_error("bigtable_decode", "error");
                        fatal.store(true, Ordering::Relaxed);
                        let _ = result_tx.blocking_send(Err(err));
                    }
                });
            });
        }
    } else {
        let mut cursor = slot_range.start;
        'discovery: while cursor <= slot_range.end {
            if *shutdown_rx.borrow() > 0 || fatal.load(Ordering::Relaxed) {
                info!("shutdown signal received; draining in-flight batches");
                break;
            }

            let slots = storage
                .get_confirmed_blocks(cursor, args.bigtable_discovery_limit)
                .await
                .context("bigtable get_confirmed_blocks")?;

            if slots.is_empty() {
                break;
            }

            let mut filtered = Vec::with_capacity(slots.len());
            for slot in slots {
                if slot > slot_range.end {
                    break;
                }
                if slot < slot_range.start {
                    continue;
                }
                filtered.push(slot);
            }

            if filtered.is_empty() {
                break;
            }

            cursor = filtered.last().copied().unwrap_or(cursor).saturating_add(1);

            for slot_chunk in filtered.chunks(args.bigtable_fetch_batch_size) {
                if *shutdown_rx.borrow() > 0 || fatal.load(Ordering::Relaxed) {
                    break 'discovery;
                }

                let slots = slot_chunk.to_vec();
                let storage = storage.clone();
                let result_tx = result_tx.clone();
                let permit = match semaphore.clone().acquire_owned().await {
                    Ok(permit) => permit,
                    Err(_) => break 'discovery,
                };
                let fatal = fatal.clone();
                let decode_semaphore = decode_semaphore.clone();
                let max_supported_tx_version = args.rpc_max_supported_tx_version;
                let flush_every_block = flush_config.flush_every_block;

                join_set.spawn(async move {
                    let _permit = permit;
                    if fatal.load(Ordering::Relaxed) {
                        return;
                    }

                    let blocks = match fetch_bigtable_slot_chunk(storage, slots).await {
                        Ok(blocks) => blocks,
                        Err(err) => {
                            metrics::observe_source_error("bigtable_fetch", "error");
                            fatal.store(true, Ordering::Relaxed);
                            let _ = result_tx.send(Err(err)).await;
                            return;
                        }
                    };

                    if blocks.is_empty() {
                        return;
                    }

                    let decode_permit = match decode_semaphore.acquire_owned().await {
                        Ok(permit) => permit,
                        Err(_) => return,
                    };
                    let result_tx = result_tx.clone();
                    let fatal = fatal.clone();
                    tokio::task::spawn_blocking(move || {
                        let _permit = decode_permit;
                        if fatal.load(Ordering::Relaxed) {
                            return;
                        }
                        if let Err(err) = decode_bigtable_blocks(
                            blocks,
                            max_supported_tx_version,
                            flush_every_block,
                            &result_tx,
                        ) {
                            metrics::observe_source_error("bigtable_decode", "error");
                            fatal.store(true, Ordering::Relaxed);
                            let _ = result_tx.blocking_send(Err(err));
                        }
                    });
                });
            }
        }
    }

    drop(result_tx);

    if fatal.load(Ordering::Relaxed) {
        join_set.abort_all();
    }

    let mut forced_shutdown = false;
    let mut shutdown_count = *shutdown_rx.borrow_and_update();
    loop {
        if join_set.is_empty() {
            break;
        }
        tokio::select! {
            result = join_set.join_next() => {
                match result {
                    Some(result) => {
                        if let Err(err) = result
                            && !err.is_cancelled()
                        {
                            warn!("bigtable fetch task failed: {err}");
                        }
                    }
                    None => break,
                }
            }
            _ = shutdown_rx.changed() => {
                let new_count = *shutdown_rx.borrow();
                if new_count <= shutdown_count {
                    warn!("shutdown signal updated without count increase; exiting");
                }
                if new_count >= 2 {
                    warn!("second SIGINT received; aborting in-flight bigtable fetches");
                    join_set.abort_all();
                    forced_shutdown = true;
                    break;
                }
                shutdown_count = new_count;
            }
        }
    }

    if forced_shutdown {
        inserter_handle.abort();
        return Ok(());
    }

    let inserter_outcome = inserter_handle.await.context("bigtable inserter task")??;
    if inserter_outcome.shutdown_requested {
        return Ok(());
    }

    Ok(())
}

async fn fetch_bigtable_slot_chunk(
    storage: Arc<LedgerStorage>,
    slots: Vec<u64>,
) -> Result<Vec<(u64, ConfirmedBlock)>> {
    let blocks = storage
        .get_confirmed_blocks_with_data(slots)
        .await
        .context("bigtable get_confirmed_blocks_with_data")?;
    Ok(blocks.collect())
}

fn decode_bigtable_blocks(
    blocks: Vec<(u64, ConfirmedBlock)>,
    max_supported_tx_version: u8,
    flush_every_block: bool,
    result_tx: &mpsc::Sender<Result<BigtableBatch>>,
) -> Result<()> {
    if blocks.is_empty() {
        return Ok(());
    }

    if flush_every_block {
        for (slot, block) in blocks {
            let tx_rows = map_bigtable_transactions(
                slot,
                block.block_time,
                &block,
                max_supported_tx_version,
            )?;
            let block_row = map_bigtable_block_metadata(slot, &block, tx_rows.len() as u64)?;
            let batch = BigtableBatch {
                transaction_rows: tx_rows,
                block_rows: vec![block_row],
                last_slot: Some(slot),
            };

            if result_tx.blocking_send(Ok(batch)).is_err() {
                return Ok(());
            }
        }

        return Ok(());
    }

    let mut transaction_rows = Vec::new();
    let mut block_rows = Vec::with_capacity(blocks.len());
    let mut last_slot = None;

    for (slot, block) in blocks {
        let tx_rows =
            map_bigtable_transactions(slot, block.block_time, &block, max_supported_tx_version)?;
        let block_row = map_bigtable_block_metadata(slot, &block, tx_rows.len() as u64)?;
        transaction_rows.extend(tx_rows);
        block_rows.push(block_row);
        last_slot = Some(slot);
    }

    if transaction_rows.is_empty() && block_rows.is_empty() {
        return Ok(());
    }

    let batch = BigtableBatch {
        transaction_rows,
        block_rows,
        last_slot,
    };
    if result_tx.blocking_send(Ok(batch)).is_err() {
        return Ok(());
    }

    Ok(())
}

async fn run_bigtable_inserter(args: BigtableInserterArgs) -> Result<BigtableInserterOutcome> {
    let BigtableInserterArgs {
        clickhouse,
        insert_tables,
        flush_config,
        insert_concurrency,
        total_slots,
        progress_every,
        mut result_rx,
        mut shutdown_rx,
        fatal,
        retry_config,
    } = args;
    let clickhouse = Arc::new(clickhouse);
    let insert_tables = Arc::new(insert_tables);
    let mut insert_tasks: JoinSet<Result<()>> = JoinSet::new();
    let mut transaction_rows: Vec<TransactionRow> =
        Vec::with_capacity(flush_config.transactions_flush_rows);
    let mut block_rows: Vec<BlockMetadataRow> = Vec::with_capacity(flush_config.blocks_flush_rows);

    let mut processed_slots = 0u64;
    let mut next_progress = progress_every;
    let mut last_flush = Instant::now();
    let mut shutdown_requested = false;

    while let Some(batch_result) = result_rx.recv().await {
        if *shutdown_rx.borrow() > 0 {
            shutdown_requested = true;
        }

        let batch = match batch_result {
            Ok(batch) => batch,
            Err(err) => {
                fatal.store(true, Ordering::Relaxed);
                return Err(err);
            }
        };

        let last_slot = batch.last_slot;
        if let Some(last_slot) = last_slot {
            metrics::set_last_processed_slot(last_slot);
        }
        if !batch.transaction_rows.is_empty() {
            transaction_rows.extend(batch.transaction_rows);
        }
        if !batch.block_rows.is_empty() {
            processed_slots = processed_slots.saturating_add(batch.block_rows.len() as u64);
            block_rows.extend(batch.block_rows);
        }

        if processed_slots >= next_progress {
            let percent = (processed_slots as f64 / total_slots as f64) * 100.0;
            info!(
                processed_slots,
                total_slots, percent, last_slot, "bigtable ingest progress"
            );
            next_progress = next_progress.saturating_add(progress_every);
        }

        if (flush_config.flush_every_block
            || shutdown_requested
            || should_flush(&flush_config, last_flush, &transaction_rows, &block_rows))
            && let Some(batch) =
                take_bigtable_flush_batch(&flush_config, &mut transaction_rows, &mut block_rows)
        {
            let abort = enqueue_bigtable_flush(
                &mut insert_tasks,
                insert_concurrency,
                clickhouse.clone(),
                insert_tables.clone(),
                batch,
                &mut shutdown_rx,
                shutdown_requested,
                retry_config.clone(),
            )
            .await
            .inspect_err(|_| {
                fatal.store(true, Ordering::Relaxed);
            })?;
            if abort {
                return Ok(BigtableInserterOutcome {
                    shutdown_requested: true,
                });
            }
            last_flush = Instant::now();
        }
    }

    if let Some(batch) =
        take_bigtable_flush_batch(&flush_config, &mut transaction_rows, &mut block_rows)
    {
        let abort = enqueue_bigtable_flush(
            &mut insert_tasks,
            insert_concurrency,
            clickhouse.clone(),
            insert_tables.clone(),
            batch,
            &mut shutdown_rx,
            shutdown_requested,
            retry_config.clone(),
        )
        .await
        .inspect_err(|_| {
            fatal.store(true, Ordering::Relaxed);
        })?;
        if abort {
            return Ok(BigtableInserterOutcome {
                shutdown_requested: true,
            });
        }
    }

    let abort =
        drain_bigtable_insert_tasks(&mut insert_tasks, &mut shutdown_rx, shutdown_requested)
            .await
            .inspect_err(|_| {
                fatal.store(true, Ordering::Relaxed);
            })?;
    if abort {
        return Ok(BigtableInserterOutcome {
            shutdown_requested: true,
        });
    }

    Ok(BigtableInserterOutcome { shutdown_requested })
}

fn sort_bigtable_rows(
    transaction_rows: &mut [TransactionRow],
    block_rows: &mut [BlockMetadataRow],
) {
    if transaction_rows.len() > 1 {
        transaction_rows.sort_unstable_by(|left, right| {
            left.slot
                .cmp(&right.slot)
                .then_with(|| left.slot_idx.cmp(&right.slot_idx))
        });
    }
    if block_rows.len() > 1 {
        block_rows.sort_unstable_by_key(|row| row.slot);
    }
}

fn take_bigtable_flush_batch(
    config: &BigtableFlushConfig,
    transaction_rows: &mut Vec<TransactionRow>,
    block_rows: &mut Vec<BlockMetadataRow>,
) -> Option<BigtableFlushBatch> {
    if transaction_rows.is_empty() && block_rows.is_empty() {
        return None;
    }

    let mut tx_rows = Vec::with_capacity(config.transactions_flush_rows);
    let mut blk_rows = Vec::with_capacity(config.blocks_flush_rows);
    std::mem::swap(&mut tx_rows, transaction_rows);
    std::mem::swap(&mut blk_rows, block_rows);

    Some(BigtableFlushBatch {
        transaction_rows: tx_rows,
        block_rows: blk_rows,
    })
}

async fn flush_bigtable_batch(
    clickhouse: &clickhouse::Client,
    insert_tables: &InsertTables,
    mut batch: BigtableFlushBatch,
    retry: &RetryConfig,
) -> Result<()> {
    sort_bigtable_rows(&mut batch.transaction_rows, &mut batch.block_rows);
    let mut entry_rows = Vec::new();
    flush_buffers_with_retry(
        clickhouse,
        insert_tables,
        &mut batch.transaction_rows,
        &mut batch.block_rows,
        &mut entry_rows,
        None,
        retry,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn enqueue_bigtable_flush(
    insert_tasks: &mut JoinSet<Result<()>>,
    insert_concurrency: usize,
    clickhouse: Arc<clickhouse::Client>,
    insert_tables: Arc<InsertTables>,
    batch: BigtableFlushBatch,
    shutdown_rx: &mut watch::Receiver<u64>,
    allow_abort: bool,
    retry: Arc<RetryConfig>,
) -> Result<bool> {
    let max_inflight = insert_concurrency.max(1);
    while insert_tasks.len() >= max_inflight {
        if allow_abort && *shutdown_rx.borrow() >= 2 {
            insert_tasks.abort_all();
            return Ok(true);
        }

        if allow_abort {
            let shutdown_count = *shutdown_rx.borrow_and_update();
            tokio::select! {
                Some(result) = insert_tasks.join_next() => {
                    result??;
                }
                _ = shutdown_rx.changed() => {
                    let new_count = *shutdown_rx.borrow();
                    if new_count <= shutdown_count {
                        warn!("shutdown signal updated without count increase; exiting");
                    }
                    if new_count >= 2 {
                        warn!("second SIGINT received; aborting in-flight inserts");
                        insert_tasks.abort_all();
                        return Ok(true);
                    }
                }
            }
        } else if let Some(result) = insert_tasks.join_next().await {
            result??;
        }
    }

    insert_tasks.spawn(async move {
        flush_bigtable_batch(clickhouse.as_ref(), insert_tables.as_ref(), batch, &retry).await
    });

    Ok(false)
}

async fn drain_bigtable_insert_tasks(
    insert_tasks: &mut JoinSet<Result<()>>,
    shutdown_rx: &mut watch::Receiver<u64>,
    allow_abort: bool,
) -> Result<bool> {
    loop {
        if insert_tasks.is_empty() {
            return Ok(false);
        }

        if allow_abort {
            let shutdown_count = *shutdown_rx.borrow_and_update();
            tokio::select! {
                Some(result) = insert_tasks.join_next() => {
                    result??;
                }
                _ = shutdown_rx.changed() => {
                    let new_count = *shutdown_rx.borrow();
                    if new_count <= shutdown_count {
                        warn!("shutdown signal updated without count increase; exiting");
                    }
                    if new_count >= 2 {
                        warn!("second SIGINT received; aborting in-flight inserts");
                        insert_tasks.abort_all();
                        return Ok(true);
                    }
                }
            }
        } else if let Some(result) = insert_tasks.join_next().await {
            result??;
        }
    }
}

fn should_flush(
    config: &BigtableFlushConfig,
    last_flush: Instant,
    transaction_rows: &[TransactionRow],
    block_rows: &[BlockMetadataRow],
) -> bool {
    transaction_rows.len() >= config.transactions_flush_rows
        || block_rows.len() >= config.blocks_flush_rows
        || last_flush.elapsed() >= Duration::from_secs(config.flush_interval_secs.max(1))
}

async fn resolve_slot_range(args: &Args, range_spec: RangeSpec) -> Result<SlotRange> {
    match range_spec {
        RangeSpec::Slots { start, end } => Ok(SlotRange { start, end }),
        RangeSpec::Epochs { start, end } => {
            let rpc_url = args
                .rpc_url
                .as_ref()
                .context("epoch ranges require --rpc-url / RPC_URL")?;
            let commitment = parse_commitment_config(&args.commitment)?;
            let rpc_client = build_rpc_client(
                rpc_url,
                commitment,
                args.rpc_timeout_secs,
                args.rpc_max_inflight.max(1),
            )?;
            let schedule = rpc_client
                .get_epoch_schedule()
                .await
                .context("fetch epoch schedule")?;
            let start_slot = schedule.get_first_slot_in_epoch(start);
            let end_slot = schedule.get_last_slot_in_epoch(end);
            info!(
                epoch_start = start,
                epoch_end = end,
                slot_start = start_slot,
                slot_end = end_slot,
                "resolved epoch range to slots"
            );
            Ok(SlotRange {
                start: start_slot,
                end: end_slot,
            })
        }
    }
}

async fn build_bigtable_storage(args: &Args) -> Result<LedgerStorage> {
    let credential_type = match (
        args.bigtable_credential_json.as_ref(),
        args.bigtable_credential_path.as_ref(),
    ) {
        (Some(json), None) => CredentialType::Stringified(json.clone()),
        (None, Some(path)) => CredentialType::Filepath(Some(path.clone())),
        (None, None) => CredentialType::Filepath(None),
        (Some(_), Some(_)) => {
            return Err(anyhow!(
                "bigtable credential json and credential path are mutually exclusive"
            ));
        }
    };

    let timeout = args.bigtable_timeout_secs.map(Duration::from_secs);

    let config = LedgerStorageConfig {
        read_only: true,
        timeout,
        credential_type,
        instance_name: args.bigtable_instance.clone(),
        app_profile_id: args.bigtable_app_profile.clone(),
        max_message_size: args.bigtable_max_message_bytes,
    };

    LedgerStorage::new_with_config(config)
        .await
        .context("create bigtable client")
}
