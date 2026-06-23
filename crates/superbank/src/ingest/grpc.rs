// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::{collections::HashMap, pin::Pin, time::Duration};

use anyhow::{Context, Result, anyhow};
use clickhouse::Client as ClickHouseClient;
use futures::StreamExt;
use serde_big_array::Array;
use serde_bytes::ByteBuf;
use tokio::{
    sync::mpsc,
    task::JoinHandle,
    time::{Instant, MissedTickBehavior, Sleep, interval, sleep_until},
};
use tonic::{Code, Status};
use tracing::{debug, info, warn};
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::prelude::{
    RewardType, SubscribeRequest, SubscribeRequestFilterBlocks, SubscribeRequestFilterSlots,
    SubscribeUpdate, SubscribeUpdateBlock, SubscribeUpdateEntry, SubscribeUpdateTransactionInfo,
    subscribe_update::UpdateOneof,
};

use crate::cli::{Args, FromSlotSpec};
use crate::clickhouse::{
    BlockMetadataRow, EntryRow, InsertTables, RetryConfig, TransactionRow, build_clickhouse_client,
    fetch_latest_slot_from_blocks, flush_buffers, flush_buffers_with_retry,
};
use crate::commitment::parse_commitment_level;
use crate::metrics;
use crate::shutdown::spawn_shutdown_watch;
use crate::utils::{bytes_to_array, decode_base58_32};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FromSlotMode {
    Strict,
    LatestDb,
    Zero,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AvailableSlotKind {
    Earliest,
    Latest,
}

#[derive(Debug)]
struct ParsedAvailableSlot {
    slot: u64,
    kind: AvailableSlotKind,
    label: &'static str,
}

const GRPC_HEALTH_STATUS_UNKNOWN: i32 = 0;
const GRPC_HEALTH_STATUS_SERVING: i32 = 1;
const GRPC_HEALTH_STATUS_NOT_SERVING: i32 = 2;
const GRPC_HEALTH_STATUS_SERVICE_UNKNOWN: i32 = 3;

#[derive(Default)]
struct AbortTaskGuard(Option<JoinHandle<()>>);

impl AbortTaskGuard {
    fn set(&mut self, handle: JoinHandle<()>) {
        self.0 = Some(handle);
    }
}

impl Drop for AbortTaskGuard {
    fn drop(&mut self) {
        if let Some(handle) = self.0.take() {
            handle.abort();
        }
    }
}

pub(crate) struct BufferedRows {
    transaction_rows: Vec<TransactionRow>,
    block_rows: Vec<BlockMetadataRow>,
    entry_rows: Vec<EntryRow>,
    last_durable_block_slot: Option<u64>,
}

impl BufferedRows {
    pub(crate) fn new(args: &Args) -> Self {
        Self {
            transaction_rows: Vec::with_capacity(args.transactions_flush_rows),
            block_rows: Vec::with_capacity(args.blocks_flush_rows),
            entry_rows: Vec::with_capacity(args.transactions_flush_rows),
            last_durable_block_slot: None,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.transaction_rows.is_empty() && self.block_rows.is_empty() && self.entry_rows.is_empty()
    }

    pub(crate) async fn flush(
        &mut self,
        clickhouse: &ClickHouseClient,
        insert_tables: &InsertTables,
    ) -> Result<()> {
        let flushed_block_slot = max_block_slot(&self.block_rows);
        flush_buffers(
            clickhouse,
            insert_tables,
            &mut self.transaction_rows,
            &mut self.block_rows,
            &mut self.entry_rows,
            None,
        )
        .await?;
        if let Some(slot) = flushed_block_slot {
            self.last_durable_block_slot = Some(
                self.last_durable_block_slot
                    .map_or(slot, |prev| prev.max(slot)),
            );
        }
        Ok(())
    }

    pub(crate) async fn flush_with_retry(
        &mut self,
        clickhouse: &ClickHouseClient,
        insert_tables: &InsertTables,
        retry: &RetryConfig,
    ) -> Result<()> {
        let flushed_block_slot = max_block_slot(&self.block_rows);
        flush_buffers_with_retry(
            clickhouse,
            insert_tables,
            &mut self.transaction_rows,
            &mut self.block_rows,
            &mut self.entry_rows,
            None,
            retry,
        )
        .await?;
        if let Some(slot) = flushed_block_slot {
            self.last_durable_block_slot = Some(
                self.last_durable_block_slot
                    .map_or(slot, |prev| prev.max(slot)),
            );
        }
        Ok(())
    }
}

pub(crate) async fn run_grpc_ingest(args: &Args) -> Result<()> {
    let endpoint = args
        .endpoint
        .as_ref()
        .context("grpc source requires --endpoint / DRAGONSMOUTH_ENDPOINT / config endpoint")?;
    let commitment = parse_commitment_level(&args.commitment)? as i32;
    let clickhouse = build_clickhouse_client(args);

    info!(
        source = "grpc",
        endpoint = %endpoint,
        transactions_table = %args.transactions_table,
        blocks_table = %args.blocks_table,
        grpc_max_decoding_bytes = args.grpc_max_decoding_bytes,
        grpc_http2_adaptive_window = args.grpc_http2_adaptive_window,
        grpc_idle_timeout_secs = args.grpc_idle_timeout_secs,
        grpc_health_watch_enabled = args.grpc_health_watch_enabled,
        "starting superbank ingest"
    );

    let (initial_from_slot, initial_from_slot_mode) =
        resolve_initial_from_slot(args, &clickhouse).await?;

    let mut buffered_rows = BufferedRows::new(args);
    let insert_tables = InsertTables::from_args(args);
    let retry_config = RetryConfig {
        max_retries: args.insert_max_retries,
        base_ms: args.insert_retry_base_ms,
        max_ms: args.insert_retry_max_ms,
    };
    let include_entries = args.entries_table.is_some();

    let mut flush_timer = interval(Duration::from_secs(args.flush_interval_secs));
    flush_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let mut shutdown_rx = spawn_shutdown_watch();
    let mut last_processed_block_slot = None;
    let (health_failure_tx, mut health_failure_rx) = mpsc::unbounded_channel();
    let _health_failure_guard = if args.grpc_health_watch_enabled {
        None
    } else {
        Some(health_failure_tx.clone())
    };
    let mut _health_watch_guard = AbortTaskGuard::default();
    if args.grpc_health_watch_enabled {
        _health_watch_guard.set(start_grpc_health_watch(endpoint, args, health_failure_tx).await?);
    }

    let subscribe_from_slot =
        next_subscribe_from_slot(initial_from_slot, buffered_rows.last_durable_block_slot)?;
    let subscribe_from_slot_mode = next_subscribe_from_slot_mode(
        initial_from_slot_mode,
        buffered_rows.last_durable_block_slot,
    );
    let (pending_update, mut stream) = connect_grpc_stream(
        endpoint,
        args,
        commitment,
        subscribe_from_slot,
        subscribe_from_slot_mode,
        include_entries,
        args.grpc_slot_notifications,
    )
    .await?;

    info!(
        from_slot = subscribe_from_slot,
        resume_from_durable_slot = buffered_rows.last_durable_block_slot.is_some(),
        "subscribed to gRPC stream"
    );

    let idle_timeout = Duration::from_secs(args.grpc_idle_timeout_secs);
    let idle_timer = sleep_until(Instant::now() + idle_timeout);
    tokio::pin!(idle_timer);

    if let Some(update) = pending_update {
        reset_idle_timer(idle_timer.as_mut(), idle_timeout);
        if let Some(slot) = processed_block_slot(&update) {
            last_processed_block_slot = Some(slot);
            metrics::set_last_processed_slot(slot);
        }
        process_update(
            update,
            args,
            &insert_tables,
            &clickhouse,
            &mut buffered_rows,
            Some(&retry_config),
        )
        .await?;
    }

    loop {
        if *shutdown_rx.borrow() > 0 {
            info!("shutdown signal received; flushing remaining rows");
            break;
        }
        tokio::select! {
            _ = shutdown_rx.changed() => {
                info!("shutdown signal received; flushing remaining rows");
                break;
            }
            _ = flush_timer.tick() => {
                buffered_rows.flush_with_retry(&clickhouse, &insert_tables, &retry_config).await?;
            }
            health_failure = health_failure_rx.recv() => {
                let reason = health_failure
                    .unwrap_or_else(|| "gRPC health watch task stopped unexpectedly".to_string());
                metrics::observe_source_error("grpc_health_watch", "unhealthy");
                warn!(
                    reason = %reason,
                    last_processed_block_slot,
                    last_durable_block_slot = buffered_rows.last_durable_block_slot,
                    "fatal gRPC health condition; flushing pending rows before exit"
                );
                flush_after_fatal_condition(
                    &clickhouse,
                    &insert_tables,
                    &mut buffered_rows,
                    &reason,
                )
                .await?;
                return Err(anyhow!(reason));
            }
            _ = &mut idle_timer => {
                let reason = format!(
                    "gRPC stream idle for more than {} seconds",
                    args.grpc_idle_timeout_secs
                );
                metrics::observe_source_error("grpc_stream", "idle_timeout");
                warn!(
                    reason = %reason,
                    last_processed_block_slot,
                    last_durable_block_slot = buffered_rows.last_durable_block_slot,
                    "fatal gRPC idle timeout; flushing pending rows before exit"
                );
                flush_after_fatal_condition(
                    &clickhouse,
                    &insert_tables,
                    &mut buffered_rows,
                    &reason,
                )
                .await?;
                return Err(anyhow!(reason));
            }
            update = stream.next() => {
                match update {
                    Some(Ok(update)) => {
                        reset_idle_timer(idle_timer.as_mut(), idle_timeout);
                        if let Some(slot) = processed_block_slot(&update) {
                            last_processed_block_slot = Some(slot);
                            metrics::set_last_processed_slot(slot);
                        }
                        if let Some(UpdateOneof::Slot(slot_update)) = &update.update_oneof {
                            metrics::set_network_tip_slot(slot_update.slot);
                        }
                        process_update(
                            update,
                            args,
                            &insert_tables,
                            &clickhouse,
                            &mut buffered_rows,
                            Some(&retry_config),
                        )
                        .await?;
                    }
                    Some(Err(status)) => {
                        let reason = format!(
                            "gRPC stream error: {}",
                            grpc_status_summary(&status, Some(args.grpc_max_decoding_bytes))
                        );
                        metrics::observe_source_error("grpc_stream", "error");
                        if is_oversized_grpc_message(&status) {
                            warn!(
                                code = ?status.code(),
                                message = status.message(),
                                grpc_max_decoding_bytes = args.grpc_max_decoding_bytes,
                                last_processed_block_slot,
                                last_durable_block_slot = buffered_rows.last_durable_block_slot,
                                "gRPC stream exceeded configured decoding limit; flushing pending rows before exit"
                            );
                        } else {
                            warn!(
                                code = ?status.code(),
                                message = status.message(),
                                last_processed_block_slot,
                                last_durable_block_slot = buffered_rows.last_durable_block_slot,
                                "gRPC stream error; flushing pending rows before exit"
                            );
                        }
                        flush_after_fatal_condition(
                            &clickhouse,
                            &insert_tables,
                            &mut buffered_rows,
                            &reason,
                        )
                        .await?;
                        return Err(anyhow!(reason));
                    }
                    None => {
                        let reason = "gRPC stream ended".to_string();
                        metrics::observe_source_error("grpc_stream", "ended");
                        warn!(
                            last_processed_block_slot,
                            last_durable_block_slot = buffered_rows.last_durable_block_slot,
                            "gRPC stream ended; flushing pending rows before exit"
                        );
                        flush_after_fatal_condition(
                            &clickhouse,
                            &insert_tables,
                            &mut buffered_rows,
                            &reason,
                        )
                        .await?;
                        return Err(anyhow!(reason));
                    }
                }
            }
        }
    }
    let shutdown_count = *shutdown_rx.borrow();
    tokio::select! {
        result = buffered_rows.flush_with_retry(&clickhouse, &insert_tables, &retry_config) => {
            result?;
        }
        _ = shutdown_rx.changed() => {
            let new_count = *shutdown_rx.borrow();
            if new_count <= shutdown_count {
                warn!("shutdown signal updated without count increase; exiting");
            }
            warn!("second SIGINT received; exiting before flush completes");
            return Ok(());
        }
    }
    Ok(())
}

async fn connect_grpc_stream(
    endpoint: &str,
    args: &Args,
    commitment: i32,
    subscribe_from_slot: Option<u64>,
    subscribe_from_slot_mode: Option<FromSlotMode>,
    include_entries: bool,
    include_slot_notifications: bool,
) -> Result<(
    Option<SubscribeUpdate>,
    impl futures::Stream<Item = Result<SubscribeUpdate, Status>>,
)> {
    let mut client = build_grpc_client(endpoint, args).await?;
    let mut pending_update = None;
    let build_request = |from_slot| {
        build_subscribe_request(
            commitment,
            from_slot,
            include_entries,
            include_slot_notifications,
        )
    };
    let stream = match subscribe_from_slot_mode {
        Some(FromSlotMode::Zero) | Some(FromSlotMode::LatestDb) => {
            let request = build_request(subscribe_from_slot);
            let mut stream = client.subscribe_once(request).await?;
            match stream.next().await {
                Some(Ok(update)) => {
                    pending_update = Some(update);
                    stream
                }
                Some(Err(status)) => {
                    let message = status.message();
                    let parsed = parse_available_slot_from_error(message).ok_or_else(|| {
                        anyhow!("failed to parse available slot from gRPC error: {message}")
                    })?;
                    match subscribe_from_slot_mode {
                        Some(FromSlotMode::Zero) => {
                            if parsed.kind != AvailableSlotKind::Earliest {
                                warn!(
                                    slot = parsed.slot,
                                    label = parsed.label,
                                    "dragonsmouth-from-slot=0 error returned non-earliest label; using parsed slot"
                                );
                            } else {
                                info!(
                                    slot = parsed.slot,
                                    label = parsed.label,
                                    "resolved dragonsmouth-from-slot=0 to earliest available slot"
                                );
                            }
                        }
                        Some(FromSlotMode::LatestDb) => {
                            if let Some(attempted) = subscribe_from_slot {
                                warn!(
                                    slot = attempted,
                                    "dragonsmouth-from-slot='*' slot not available; falling back to gRPC available slot"
                                );
                            }
                            if parsed.kind != AvailableSlotKind::Latest {
                                warn!(
                                    slot = parsed.slot,
                                    label = parsed.label,
                                    "dragonsmouth-from-slot='*' error returned non-latest label; using parsed slot"
                                );
                            } else {
                                info!(
                                    slot = parsed.slot,
                                    label = parsed.label,
                                    "resolved dragonsmouth-from-slot='*' to latest available slot"
                                );
                            }
                        }
                        _ => {}
                    }
                    let request = build_request(Some(parsed.slot));
                    client.subscribe_once(request).await?
                }
                None => {
                    return Err(anyhow!("gRPC stream ended before first update"));
                }
            }
        }
        _ => {
            client
                .subscribe_once(build_request(subscribe_from_slot))
                .await?
        }
    };
    Ok((pending_update, stream))
}

async fn resolve_initial_from_slot(
    args: &Args,
    clickhouse: &ClickHouseClient,
) -> Result<(Option<u64>, Option<FromSlotMode>)> {
    match args.dragonsmouth_from_slot {
        Some(FromSlotSpec::LatestDb) => {
            let latest = fetch_latest_slot_from_blocks(clickhouse, &args.blocks_table)
                .await?
                .ok_or_else(|| {
                    anyhow!(
                        "dragonsmouth-from-slot '*' requires at least one row in {}",
                        args.blocks_table
                    )
                })?;
            info!(
                slot = latest,
                table = %args.blocks_table,
                "resolved dragonsmouth-from-slot='*' to latest slot in blocks_metadata"
            );
            Ok((Some(latest), Some(FromSlotMode::LatestDb)))
        }
        Some(FromSlotSpec::Slot(0)) => Ok((Some(0), Some(FromSlotMode::Zero))),
        Some(FromSlotSpec::Slot(slot)) => Ok((Some(slot), Some(FromSlotMode::Strict))),
        None => Ok((None, None)),
    }
}

fn next_subscribe_from_slot(
    initial_from_slot: Option<u64>,
    last_durable_block_slot: Option<u64>,
) -> Result<Option<u64>> {
    let durable_resume_slot = last_durable_block_slot
        .map(|slot| {
            slot.checked_add(1)
                .ok_or_else(|| anyhow!("cannot resume after u64::MAX slot"))
        })
        .transpose()?;
    Ok(durable_resume_slot.or(initial_from_slot))
}

fn next_subscribe_from_slot_mode(
    initial_from_slot_mode: Option<FromSlotMode>,
    last_durable_block_slot: Option<u64>,
) -> Option<FromSlotMode> {
    if last_durable_block_slot.is_some() {
        Some(FromSlotMode::Strict)
    } else {
        initial_from_slot_mode
    }
}

pub(crate) fn build_subscribe_request(
    commitment: i32,
    from_slot: Option<u64>,
    include_entries: bool,
    include_slot_notifications: bool,
) -> SubscribeRequest {
    let mut blocks = HashMap::new();
    blocks.insert(
        "blocks".to_string(),
        SubscribeRequestFilterBlocks {
            account_include: Vec::new(),
            include_transactions: Some(true),
            include_accounts: Some(false),
            include_entries: Some(include_entries),
        },
    );

    let mut slots = HashMap::new();
    if include_slot_notifications {
        slots.insert(
            "slots".to_string(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(true),
                interslot_updates: Some(false),
            },
        );
    }

    SubscribeRequest {
        blocks,
        slots,
        commitment: Some(commitment),
        from_slot,
        ..Default::default()
    }
}

async fn build_grpc_client(endpoint: &str, args: &Args) -> Result<GeyserGrpcClient> {
    let builder = GeyserGrpcClient::build_from_shared(endpoint.to_string())?
        .x_token(args.x_token.clone())?
        .http2_adaptive_window(args.grpc_http2_adaptive_window)
        .max_decoding_message_size(args.grpc_max_decoding_bytes)
        .tls_config(ClientTlsConfig::new().with_native_roots())?;

    Ok(builder.connect().await?)
}

async fn start_grpc_health_watch(
    endpoint: &str,
    args: &Args,
    health_failure_tx: mpsc::UnboundedSender<String>,
) -> Result<JoinHandle<()>> {
    let mut client = build_grpc_client(endpoint, args).await?;
    let initial_health = client
        .health_check()
        .await
        .context("gRPC health check failed")?;
    if !grpc_health_status_is_serving(initial_health.status) {
        return Err(anyhow!(
            "gRPC health check returned {} ({})",
            grpc_health_status_label(initial_health.status),
            initial_health.status
        ));
    }
    info!(
        status = grpc_health_status_label(initial_health.status),
        raw_status = initial_health.status,
        "gRPC health check passed"
    );
    let endpoint = endpoint.to_string();
    let args = args.clone();
    Ok(tokio::spawn(async move {
        let mut client = match build_grpc_client(&endpoint, &args).await {
            Ok(client) => client,
            Err(err) => {
                let _ =
                    health_failure_tx.send(format!("failed to connect gRPC health watch: {err:#}"));
                return;
            }
        };
        let mut stream = match client.health_watch().await {
            Ok(stream) => stream,
            Err(err) => {
                let _ = health_failure_tx.send(format!("failed to start gRPC health watch: {err}"));
                return;
            }
        };
        let failure = loop {
            match stream.next().await {
                Some(Ok(update)) => {
                    if grpc_health_status_is_serving(update.status) {
                        continue;
                    }
                    break format!(
                        "gRPC health degraded to {} ({})",
                        grpc_health_status_label(update.status),
                        update.status
                    );
                }
                Some(Err(status)) => {
                    break format!(
                        "gRPC health watch error: {}",
                        grpc_status_summary(&status, None)
                    );
                }
                None => break "gRPC health watch ended".to_string(),
            }
        };
        let _ = health_failure_tx.send(failure);
    }))
}

async fn flush_after_fatal_condition(
    clickhouse: &ClickHouseClient,
    insert_tables: &InsertTables,
    buffered_rows: &mut BufferedRows,
    reason: &str,
) -> Result<()> {
    buffered_rows
        .flush(clickhouse, insert_tables)
        .await
        .with_context(|| format!("flush buffered rows after fatal gRPC condition: {reason}"))
}

fn reset_idle_timer(idle_timer: Pin<&mut Sleep>, idle_timeout: Duration) {
    idle_timer.reset(Instant::now() + idle_timeout);
}

fn grpc_health_status_is_serving(status: i32) -> bool {
    status == GRPC_HEALTH_STATUS_SERVING
}

fn grpc_health_status_label(status: i32) -> &'static str {
    match status {
        GRPC_HEALTH_STATUS_UNKNOWN => "unknown",
        GRPC_HEALTH_STATUS_SERVING => "serving",
        GRPC_HEALTH_STATUS_NOT_SERVING => "not_serving",
        GRPC_HEALTH_STATUS_SERVICE_UNKNOWN => "service_unknown",
        _ => "unrecognized",
    }
}

fn is_oversized_grpc_message(status: &Status) -> bool {
    status.code() == Code::OutOfRange && status.message().contains("message length too large")
}

fn grpc_status_summary(status: &Status, grpc_max_decoding_bytes: Option<usize>) -> String {
    if let Some(limit) = grpc_max_decoding_bytes.filter(|_| is_oversized_grpc_message(status)) {
        format!(
            "{:?}: {} (configured decode limit {} bytes)",
            status.code(),
            status.message(),
            limit
        )
    } else {
        format!("{:?}: {}", status.code(), status.message())
    }
}

fn parse_available_slot_from_error(message: &str) -> Option<ParsedAvailableSlot> {
    const EARLIEST_LABELS: [&str; 6] = [
        "first available",
        "earliest available",
        "first slot available",
        "earliest slot available",
        "first available slot",
        "earliest available slot",
    ];
    const LATEST_LABELS: [&str; 4] = [
        "last available",
        "latest available",
        "last slot available",
        "latest slot available",
    ];

    let normalized = message.to_ascii_lowercase();
    for label in EARLIEST_LABELS {
        if let Some(slot) = parse_slot_after_label(&normalized, label) {
            return Some(ParsedAvailableSlot {
                slot,
                kind: AvailableSlotKind::Earliest,
                label,
            });
        }
    }
    for label in LATEST_LABELS {
        if let Some(slot) = parse_slot_after_label(&normalized, label) {
            return Some(ParsedAvailableSlot {
                slot,
                kind: AvailableSlotKind::Latest,
                label,
            });
        }
    }
    None
}

fn parse_slot_after_label(message: &str, label: &str) -> Option<u64> {
    let label_start = message.find(label)?;
    let after_label = &message[label_start + label.len()..];
    let digits_start = after_label.find(|ch: char| ch.is_ascii_digit())?;
    let digits: String = after_label[digits_start..]
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect();
    digits.parse().ok()
}

pub(crate) async fn process_update(
    update: SubscribeUpdate,
    args: &Args,
    insert_tables: &InsertTables,
    clickhouse: &ClickHouseClient,
    buffered_rows: &mut BufferedRows,
    retry: Option<&RetryConfig>,
) -> Result<bool> {
    let mut flushed = false;
    match update.update_oneof {
        Some(UpdateOneof::Block(block)) => {
            {
                let entry_rows = if args.entries_table.is_some() {
                    Some(&mut buffered_rows.entry_rows)
                } else {
                    None
                };
                handle_block_update(
                    block,
                    &mut buffered_rows.transaction_rows,
                    &mut buffered_rows.block_rows,
                    entry_rows,
                )?;
            }
            if args.flush_every_block
                || buffered_rows.transaction_rows.len() >= args.transactions_flush_rows
                || buffered_rows.block_rows.len() >= args.blocks_flush_rows
                || buffered_rows.entry_rows.len() >= args.transactions_flush_rows
            {
                match retry {
                    Some(r) => {
                        buffered_rows
                            .flush_with_retry(clickhouse, insert_tables, r)
                            .await?
                    }
                    None => buffered_rows.flush(clickhouse, insert_tables).await?,
                }
                flushed = true;
            }
        }
        Some(UpdateOneof::BlockMeta(meta)) => {
            if meta.executed_transaction_count > 0 {
                warn!(
                    slot = meta.slot,
                    executed_transaction_count = meta.executed_transaction_count,
                    "received block meta update without transactions; ignoring"
                );
            } else {
                debug!(
                    slot = meta.slot,
                    "received block meta update without transactions"
                );
            }
        }
        Some(UpdateOneof::Ping(_)) | Some(UpdateOneof::Pong(_)) => {}
        _ => {}
    }
    Ok(flushed)
}

pub(crate) fn processed_block_slot(update: &SubscribeUpdate) -> Option<u64> {
    match update.update_oneof.as_ref() {
        Some(UpdateOneof::Block(block)) => Some(block.slot),
        _ => None,
    }
}

fn max_block_slot(rows: &[BlockMetadataRow]) -> Option<u64> {
    rows.iter().map(|row| row.slot).max()
}

fn handle_block_update(
    block: SubscribeUpdateBlock,
    transaction_rows: &mut Vec<TransactionRow>,
    block_rows: &mut Vec<BlockMetadataRow>,
    entry_rows: Option<&mut Vec<EntryRow>>,
) -> Result<()> {
    let slot = block.slot;
    let block_time = block.block_time.as_ref().map(|bt| bt.timestamp);

    let block_row = map_block_metadata(&block)?;
    let tx_rows = map_transactions(slot, block_time, &block.transactions)?;

    let expected = block.executed_transaction_count as usize;
    let got = tx_rows.len();
    if expected > 0 && got == 0 {
        warn!(
            slot,
            executed_transaction_count = expected,
            "block has executed_transaction_count but zero transactions"
        );
    } else if expected > 0 && got != expected {
        warn!(
            slot,
            executed_transaction_count = expected,
            transactions = got,
            "block transaction count mismatch"
        );
    }

    if let Some(entry_rows) = entry_rows {
        let mapped_entries = map_entries(slot, block_time, &block.entries)?;
        let expected_entries = block.entries_count as usize;
        let got_entries = mapped_entries.len();
        if expected_entries > 0 && got_entries == 0 {
            warn!(
                slot,
                entry_count = expected_entries,
                "block has entry_count but zero entries"
            );
        } else if expected_entries != got_entries {
            warn!(
                slot,
                entry_count = expected_entries,
                entries = got_entries,
                "block entry count mismatch"
            );
        }
        entry_rows.extend(mapped_entries);
    }

    block_rows.push(block_row);
    transaction_rows.extend(tx_rows);

    Ok(())
}

fn map_block_metadata(block: &SubscribeUpdateBlock) -> Result<BlockMetadataRow> {
    let blockhash = decode_base58_32(&block.blockhash).context("decode blockhash")?;
    let parent_blockhash =
        decode_base58_32(&block.parent_blockhash).context("decode parent blockhash")?;

    let block_time = block.block_time.as_ref().map(|bt| bt.timestamp);
    let block_height = block.block_height.as_ref().map(|bh| bh.block_height);

    let mut rewards_present = 0u8;
    let mut rewards_pubkey = Vec::new();
    let mut rewards_lamports = Vec::new();
    let mut rewards_post_balance = Vec::new();
    let mut rewards_type = Vec::new();
    let mut rewards_commission = Vec::new();
    let mut rewards_num_partitions = None;

    if let Some(rewards) = block.rewards.as_ref() {
        rewards_present = if rewards.rewards.is_empty() { 0 } else { 1 };
        for reward in &rewards.rewards {
            rewards_pubkey.push(decode_base58_32(&reward.pubkey).context("decode reward pubkey")?);
            rewards_lamports.push(reward.lamports);
            rewards_post_balance.push(reward.post_balance);
            rewards_type.push(reward_type_to_string(reward.reward_type));
            rewards_commission.push(parse_commission(&reward.commission));
        }
        rewards_num_partitions = rewards.num_partitions.as_ref().map(|p| p.num_partitions);
    }

    Ok(BlockMetadataRow {
        slot: block.slot,
        parent_slot: block.parent_slot,
        blockhash,
        parent_blockhash,
        block_time,
        block_height,
        executed_transaction_count: block.executed_transaction_count,
        entry_count: block.entries_count,
        rewards_present,
        rewards_pubkey,
        rewards_lamports,
        rewards_post_balance,
        rewards_type,
        rewards_commission,
        rewards_num_partitions,
    })
}

fn map_transactions(
    slot: u64,
    block_time: Option<i64>,
    transactions: &[SubscribeUpdateTransactionInfo],
) -> Result<Vec<TransactionRow>> {
    let mut rows = Vec::with_capacity(transactions.len());
    for tx in transactions {
        rows.push(map_transaction(slot, block_time, tx)?);
    }
    Ok(rows)
}

fn map_entries(
    block_slot: u64,
    block_time: Option<i64>,
    entries: &[SubscribeUpdateEntry],
) -> Result<Vec<EntryRow>> {
    let mut rows = Vec::with_capacity(entries.len());
    for entry in entries {
        if entry.slot != block_slot {
            warn!(
                block_slot,
                entry_slot = entry.slot,
                entry_index = entry.index,
                "block entry slot mismatch"
            );
        }
        rows.push(EntryRow {
            slot: block_slot,
            entry_index: entry.index.try_into().context("entry index out of range")?,
            block_time,
            starting_transaction_index: entry
                .starting_transaction_index
                .try_into()
                .context("starting_transaction_index out of range")?,
            transaction_count: entry
                .executed_transaction_count
                .try_into()
                .context("entry executed_transaction_count out of range")?,
            num_hashes: entry.num_hashes,
            hash: bytes_to_array::<32>(&entry.hash).context("entry hash length")?,
        });
    }
    Ok(rows)
}

fn map_transaction(
    slot: u64,
    block_time: Option<i64>,
    tx_info: &SubscribeUpdateTransactionInfo,
) -> Result<TransactionRow> {
    let signature = bytes_to_array::<64>(&tx_info.signature).context("signature length")?;
    let slot_idx: u32 = tx_info.index.try_into().context("slot_idx out of range")?;

    let transaction = tx_info
        .transaction
        .as_ref()
        .context("missing transaction")?;
    let meta = tx_info.meta.as_ref().context("missing transaction meta")?;

    let message = transaction
        .message
        .as_ref()
        .context("missing transaction message")?;

    let message_hash = compute_message_hash(message)?;
    let header = message.header.as_ref().context("missing message header")?;

    let tx_signatures = convert_signatures(&transaction.signatures)?;
    let tx_account_keys = convert_account_keys(&message.account_keys)?;
    let tx_recent_blockhash =
        bytes_to_array::<32>(&message.recent_blockhash).context("recent blockhash length")?;

    let (tx_instructions_program_id_index, tx_instructions_accounts, tx_instructions_data) =
        convert_instructions(&message.instructions)?;

    let (
        tx_address_table_lookup_account_key,
        tx_address_table_lookup_writable_indexes,
        tx_address_table_lookup_readonly_indexes,
    ) = convert_address_table_lookups(&message.address_table_lookups)?;

    let (meta_status_ok, meta_err) = decode_transaction_error(meta.err.as_ref())?;

    let (
        meta_inner_instructions_present,
        meta_inner_instructions_index,
        meta_inner_instructions_program_id_index,
        meta_inner_instructions_accounts,
        meta_inner_instructions_data,
        meta_inner_instructions_stack_height,
    ) = convert_inner_instructions(meta)?;

    let (meta_log_messages_present, meta_log_messages) = if meta.log_messages_none {
        (0, Vec::new())
    } else {
        (1, meta.log_messages.clone())
    };

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
    ) = convert_token_balances(&meta.pre_token_balances)?;

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
    ) = convert_token_balances(&meta.post_token_balances)?;

    let (
        meta_reward_pubkey,
        meta_reward_lamports,
        meta_reward_post_balance,
        meta_reward_type,
        meta_reward_commission,
    ) = convert_rewards(&meta.rewards);

    // gRPC does not distinguish between absent and empty rewards. Treat empty as present.
    let meta_rewards_present = 1;

    let meta_loaded_addresses_writable = convert_pubkeys(&meta.loaded_writable_addresses)?;
    let meta_loaded_addresses_readonly = convert_pubkeys(&meta.loaded_readonly_addresses)?;

    let (meta_return_data_present, meta_return_data_program_id, meta_return_data_data) =
        convert_return_data(meta)?;

    Ok(TransactionRow {
        signature,
        slot,
        slot_idx,
        block_time,
        message_hash,
        is_vote: u8::from(tx_info.is_vote),
        tx_version: if message.versioned { Some(0) } else { None },
        tx_signatures,
        tx_num_required_signatures: header
            .num_required_signatures
            .try_into()
            .context("num_required_signatures out of range")?,
        tx_num_readonly_signed_accounts: header
            .num_readonly_signed_accounts
            .try_into()
            .context("num_readonly_signed_accounts out of range")?,
        tx_num_readonly_unsigned_accounts: header
            .num_readonly_unsigned_accounts
            .try_into()
            .context("num_readonly_unsigned_accounts out of range")?,
        tx_account_keys,
        tx_recent_blockhash,
        tx_instructions_program_id_index,
        tx_instructions_accounts,
        tx_instructions_data,
        tx_address_table_lookups_present: if message.address_table_lookups.is_empty() {
            0
        } else {
            1
        },
        tx_address_table_lookup_account_key,
        tx_address_table_lookup_writable_indexes,
        tx_address_table_lookup_readonly_indexes,
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

fn compute_message_hash(
    message: &yellowstone_grpc_proto::prelude::Message,
) -> Result<Array<u8, 32>> {
    let versioned_message = create_versioned_message(message)?;
    let message_bytes = crate::message_wire::serialize_versioned_message(&versioned_message)
        .context("serialize message")?;

    let mut hasher = blake3::Hasher::new();
    hasher.update(b"solana-tx-message-v1");
    hasher.update(&message_bytes);

    let hash_bytes: [u8; 32] = hasher.finalize().into();
    Ok(Array(hash_bytes))
}

fn create_versioned_message(
    message: &yellowstone_grpc_proto::prelude::Message,
) -> Result<solana_message::VersionedMessage> {
    use solana_message::{
        Address, Hash, Message as LegacyMessage, MessageHeader, VersionedMessage,
        compiled_instruction::CompiledInstruction,
        v0::{Message as MessageV0, MessageAddressTableLookup},
    };

    let header = message.header.as_ref().context("missing message header")?;
    let header = MessageHeader {
        num_required_signatures: header
            .num_required_signatures
            .try_into()
            .context("num_required_signatures out of range")?,
        num_readonly_signed_accounts: header
            .num_readonly_signed_accounts
            .try_into()
            .context("num_readonly_signed_accounts out of range")?,
        num_readonly_unsigned_accounts: header
            .num_readonly_unsigned_accounts
            .try_into()
            .context("num_readonly_unsigned_accounts out of range")?,
    };
    let recent_blockhash = Hash::new_from_array(
        message
            .recent_blockhash
            .as_slice()
            .try_into()
            .context("recent blockhash length")?,
    );
    let account_keys = message
        .account_keys
        .iter()
        .map(|key| Address::try_from(key.as_slice()).context("account key length"))
        .collect::<Result<Vec<_>>>()?;
    let instructions = message
        .instructions
        .iter()
        .map(|ix| {
            Ok(CompiledInstruction {
                program_id_index: ix
                    .program_id_index
                    .try_into()
                    .context("program_id_index out of range")?,
                accounts: ix.accounts.clone(),
                data: ix.data.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    if message.versioned {
        let address_table_lookups = message
            .address_table_lookups
            .iter()
            .map(|lookup| {
                Ok(MessageAddressTableLookup {
                    account_key: Address::try_from(lookup.account_key.as_slice())
                        .context("address lookup account key length")?,
                    writable_indexes: lookup.writable_indexes.clone(),
                    readonly_indexes: lookup.readonly_indexes.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(VersionedMessage::V0(MessageV0 {
            header,
            account_keys,
            recent_blockhash,
            instructions,
            address_table_lookups,
        }))
    } else {
        Ok(VersionedMessage::Legacy(LegacyMessage {
            header,
            account_keys,
            recent_blockhash,
            instructions,
        }))
    }
}

fn convert_signatures(signatures: &[Vec<u8>]) -> Result<Vec<Array<u8, 64>>> {
    signatures
        .iter()
        .map(|sig| bytes_to_array::<64>(sig).context("signature length"))
        .collect()
}

fn convert_account_keys(keys: &[Vec<u8>]) -> Result<Vec<Array<u8, 32>>> {
    keys.iter()
        .map(|key| bytes_to_array::<32>(key).context("account key length"))
        .collect()
}

type InstructionConversion = (Vec<u8>, Vec<Vec<u8>>, Vec<ByteBuf>);
type AddressLookupConversion = (Vec<Array<u8, 32>>, Vec<Vec<u8>>, Vec<Vec<u8>>);
type InnerInstructionsConversion = (
    u8,
    Vec<u8>,
    Vec<Vec<u8>>,
    Vec<Vec<Vec<u8>>>,
    Vec<Vec<ByteBuf>>,
    Vec<Vec<Option<u32>>>,
);
type RewardsConversion = (
    Vec<String>,
    Vec<i64>,
    Vec<u64>,
    Vec<Option<String>>,
    Vec<Option<u8>>,
);

fn convert_instructions(
    instructions: &[yellowstone_grpc_proto::prelude::CompiledInstruction],
) -> Result<InstructionConversion> {
    let mut program_ids = Vec::with_capacity(instructions.len());
    let mut accounts = Vec::with_capacity(instructions.len());
    let mut data = Vec::with_capacity(instructions.len());

    for ix in instructions {
        program_ids.push(
            ix.program_id_index
                .try_into()
                .context("program_id_index out of range")?,
        );
        accounts.push(ix.accounts.clone());
        data.push(ByteBuf::from(ix.data.clone()));
    }

    Ok((program_ids, accounts, data))
}

fn convert_address_table_lookups(
    lookups: &[yellowstone_grpc_proto::prelude::MessageAddressTableLookup],
) -> Result<AddressLookupConversion> {
    let mut account_keys = Vec::with_capacity(lookups.len());
    let mut writable_indexes = Vec::with_capacity(lookups.len());
    let mut readonly_indexes = Vec::with_capacity(lookups.len());

    for lookup in lookups {
        account_keys.push(
            bytes_to_array::<32>(&lookup.account_key)
                .context("address lookup account key length")?,
        );
        writable_indexes.push(lookup.writable_indexes.clone());
        readonly_indexes.push(lookup.readonly_indexes.clone());
    }

    Ok((account_keys, writable_indexes, readonly_indexes))
}

fn convert_inner_instructions(
    meta: &yellowstone_grpc_proto::prelude::TransactionStatusMeta,
) -> Result<InnerInstructionsConversion> {
    if meta.inner_instructions_none {
        return Ok((
            0,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ));
    }

    let mut indexes = Vec::with_capacity(meta.inner_instructions.len());
    let mut program_ids = Vec::with_capacity(meta.inner_instructions.len());
    let mut accounts = Vec::with_capacity(meta.inner_instructions.len());
    let mut data = Vec::with_capacity(meta.inner_instructions.len());
    let mut stack_heights = Vec::with_capacity(meta.inner_instructions.len());

    for ix in &meta.inner_instructions {
        indexes.push(
            ix.index
                .try_into()
                .context("inner instruction index out of range")?,
        );

        let mut group_program_ids = Vec::with_capacity(ix.instructions.len());
        let mut group_accounts = Vec::with_capacity(ix.instructions.len());
        let mut group_data = Vec::with_capacity(ix.instructions.len());
        let mut group_stack = Vec::with_capacity(ix.instructions.len());

        for inner in &ix.instructions {
            group_program_ids.push(
                inner
                    .program_id_index
                    .try_into()
                    .context("inner program_id_index out of range")?,
            );
            group_accounts.push(inner.accounts.clone());
            group_data.push(ByteBuf::from(inner.data.clone()));
            group_stack.push(inner.stack_height);
        }

        program_ids.push(group_program_ids);
        accounts.push(group_accounts);
        data.push(group_data);
        stack_heights.push(group_stack);
    }

    Ok((1, indexes, program_ids, accounts, data, stack_heights))
}

#[allow(clippy::type_complexity)]
fn convert_token_balances(
    balances: &[yellowstone_grpc_proto::prelude::TokenBalance],
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
    // gRPC does not encode optionality for token balances, so empty means "present but empty".
    let mut account_indexes = Vec::with_capacity(balances.len());
    let mut mints = Vec::with_capacity(balances.len());
    let mut owners = Vec::with_capacity(balances.len());
    let mut program_ids = Vec::with_capacity(balances.len());
    let mut amounts = Vec::with_capacity(balances.len());
    let mut decimals = Vec::with_capacity(balances.len());
    let mut ui_amounts = Vec::with_capacity(balances.len());
    let mut ui_amount_strings = Vec::with_capacity(balances.len());

    for balance in balances {
        account_indexes.push(
            balance
                .account_index
                .try_into()
                .context("token account_index out of range")?,
        );
        mints.push(decode_base58_32(&balance.mint).context("decode token mint")?);
        owners.push(optional_pubkey(&balance.owner)?);
        program_ids.push(optional_pubkey(&balance.program_id)?);

        if let Some(ui) = balance.ui_token_amount.as_ref() {
            amounts.push(ui.amount.clone());
            decimals.push(
                ui.decimals
                    .try_into()
                    .context("token decimals out of range")?,
            );
            ui_amounts.push(Some(ui.ui_amount));
            ui_amount_strings.push(ui.ui_amount_string.clone());
        } else {
            amounts.push(String::new());
            decimals.push(0);
            ui_amounts.push(None);
            ui_amount_strings.push(String::new());
        }
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

fn convert_rewards(rewards: &[yellowstone_grpc_proto::prelude::Reward]) -> RewardsConversion {
    let mut pubkeys = Vec::with_capacity(rewards.len());
    let mut lamports = Vec::with_capacity(rewards.len());
    let mut post_balances = Vec::with_capacity(rewards.len());
    let mut reward_types = Vec::with_capacity(rewards.len());
    let mut commissions = Vec::with_capacity(rewards.len());

    for reward in rewards {
        pubkeys.push(reward.pubkey.clone());
        lamports.push(reward.lamports);
        post_balances.push(reward.post_balance);
        reward_types.push(reward_type_to_string(reward.reward_type));
        commissions.push(parse_commission(&reward.commission));
    }

    (pubkeys, lamports, post_balances, reward_types, commissions)
}

fn convert_pubkeys(keys: &[Vec<u8>]) -> Result<Vec<Array<u8, 32>>> {
    keys.iter()
        .map(|key| bytes_to_array::<32>(key).context("pubkey length"))
        .collect()
}

fn optional_pubkey(value: &str) -> Result<Option<Array<u8, 32>>> {
    if value.is_empty() {
        return Ok(None);
    }

    Ok(Some(decode_base58_32(value)?))
}

fn convert_return_data(
    meta: &yellowstone_grpc_proto::prelude::TransactionStatusMeta,
) -> Result<(u8, Option<Array<u8, 32>>, Option<ByteBuf>)> {
    if meta.return_data_none {
        return Ok((0, None, None));
    }

    let Some(return_data) = meta.return_data.as_ref() else {
        return Ok((0, None, None));
    };

    let program_id =
        bytes_to_array::<32>(&return_data.program_id).context("return data program id length")?;
    Ok((
        1,
        Some(program_id),
        Some(ByteBuf::from(return_data.data.clone())),
    ))
}

fn reward_type_to_string(value: i32) -> Option<String> {
    let parsed = RewardType::try_from(value).ok()?;
    match parsed {
        RewardType::Unspecified => None,
        other => Some(other.as_str_name().to_string()),
    }
}

fn parse_commission(value: &str) -> Option<u8> {
    if value.is_empty() {
        return None;
    }
    value.parse::<u8>().ok()
}

fn decode_transaction_error(
    err: Option<&yellowstone_grpc_proto::prelude::TransactionError>,
) -> Result<(u8, Option<String>)> {
    let Some(err) = err else {
        return Ok((1, None));
    };

    match wincode05::deserialize::<solana_transaction_error::TransactionError>(&err.err) {
        Ok(decoded) => {
            let serialized =
                serde_json::to_string(&decoded).unwrap_or_else(|_| format!("{decoded:?}"));
            Ok((0, Some(serialized)))
        }
        Err(_) => {
            let fallback = hex::encode(&err.err);
            warn!("failed to decode transaction error; storing hex fallback");
            Ok((0, Some(format!("\"{}\"", fallback))))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BlockMetadataRow, FromSlotMode, grpc_health_status_is_serving, grpc_health_status_label,
        grpc_status_summary, is_oversized_grpc_message, map_transaction, max_block_slot,
        next_subscribe_from_slot, next_subscribe_from_slot_mode,
    };
    use serde_big_array::Array;
    use tonic::Status;
    use yellowstone_grpc_proto::prelude::{
        CompiledInstruction, Message, MessageHeader, SubscribeUpdateTransactionInfo, Transaction,
        TransactionStatusMeta,
    };

    fn build_test_transaction_info(cost_units: Option<u64>) -> SubscribeUpdateTransactionInfo {
        SubscribeUpdateTransactionInfo {
            signature: vec![9u8; 64],
            is_vote: false,
            transaction: Some(Transaction {
                signatures: vec![vec![9u8; 64]],
                message: Some(Message {
                    header: Some(MessageHeader {
                        num_required_signatures: 1,
                        num_readonly_signed_accounts: 0,
                        num_readonly_unsigned_accounts: 0,
                    }),
                    account_keys: vec![vec![1u8; 32], vec![2u8; 32]],
                    recent_blockhash: vec![3u8; 32],
                    instructions: vec![CompiledInstruction {
                        program_id_index: 1,
                        accounts: vec![0],
                        data: vec![1, 2, 3],
                    }],
                    versioned: false,
                    address_table_lookups: Vec::new(),
                }),
            }),
            meta: Some(TransactionStatusMeta {
                err: None,
                fee: 5_000,
                pre_balances: vec![10, 20],
                post_balances: vec![5, 25],
                inner_instructions: Vec::new(),
                inner_instructions_none: true,
                log_messages: Vec::new(),
                log_messages_none: true,
                pre_token_balances: Vec::new(),
                post_token_balances: Vec::new(),
                rewards: Vec::new(),
                loaded_writable_addresses: Vec::new(),
                loaded_readonly_addresses: Vec::new(),
                return_data: None,
                return_data_none: true,
                compute_units_consumed: Some(123),
                cost_units,
            }),
            index: 0,
        }
    }

    #[test]
    fn map_transaction_preserves_cost_units_from_grpc_meta() {
        let tx_info = build_test_transaction_info(Some(456));

        let row = map_transaction(42, Some(1_700_000_000), &tx_info).expect("map transaction");

        assert_eq!(row.meta_compute_units_consumed, Some(123));
        assert_eq!(row.meta_cost_units, Some(456));
    }

    #[test]
    fn map_transaction_leaves_cost_units_empty_when_grpc_meta_omits_it() {
        let tx_info = build_test_transaction_info(None);

        let row = map_transaction(42, Some(1_700_000_000), &tx_info).expect("map transaction");

        assert_eq!(row.meta_compute_units_consumed, Some(123));
        assert!(row.meta_cost_units.is_none());
    }

    #[test]
    fn next_subscribe_from_slot_uses_initial_slot_without_durable_progress() {
        let next = next_subscribe_from_slot(Some(123), None).expect("next subscribe slot");

        assert_eq!(next, Some(123));
    }

    #[test]
    fn next_subscribe_from_slot_resumes_after_last_durable_block() {
        let next = next_subscribe_from_slot(Some(123), Some(456)).expect("next subscribe slot");

        assert_eq!(next, Some(457));
    }

    #[test]
    fn next_subscribe_from_slot_rejects_resume_past_u64_max() {
        let err = next_subscribe_from_slot(Some(123), Some(u64::MAX)).expect_err("overflow");

        assert!(
            err.to_string()
                .contains("cannot resume after u64::MAX slot")
        );
    }

    #[test]
    fn next_subscribe_from_slot_mode_switches_to_strict_after_durable_flush() {
        let mode = next_subscribe_from_slot_mode(Some(FromSlotMode::LatestDb), Some(456));

        assert_eq!(mode, Some(FromSlotMode::Strict));
    }

    #[test]
    fn max_block_slot_returns_latest_slot_from_pending_rows() {
        let rows = vec![
            build_block_metadata_row(41),
            build_block_metadata_row(43),
            build_block_metadata_row(42),
        ];

        assert_eq!(max_block_slot(&rows), Some(43));
    }

    #[test]
    fn grpc_health_status_helpers_match_serving_contract() {
        assert!(grpc_health_status_is_serving(1));
        assert!(!grpc_health_status_is_serving(0));
        assert!(!grpc_health_status_is_serving(2));
        assert!(!grpc_health_status_is_serving(3));
        assert_eq!(grpc_health_status_label(1), "serving");
        assert_eq!(grpc_health_status_label(2), "not_serving");
        assert_eq!(grpc_health_status_label(99), "unrecognized");
    }

    #[test]
    fn oversized_grpc_message_detection_matches_tonic_out_of_range() {
        let status = Status::out_of_range(
            "Error, decoded message length too large: found 10 bytes, the limit is: 4 bytes",
        );

        assert!(is_oversized_grpc_message(&status));
        assert_eq!(
            grpc_status_summary(&status, Some(32 * 1024 * 1024)),
            "OutOfRange: Error, decoded message length too large: found 10 bytes, the limit is: 4 bytes (configured decode limit 33554432 bytes)"
        );
    }

    #[test]
    fn grpc_status_summary_leaves_normal_errors_unchanged() {
        let status = Status::internal("broken stream");

        assert!(!is_oversized_grpc_message(&status));
        assert_eq!(
            grpc_status_summary(&status, Some(32 * 1024 * 1024)),
            "Internal: broken stream"
        );
    }

    fn build_block_metadata_row(slot: u64) -> BlockMetadataRow {
        BlockMetadataRow {
            slot,
            parent_slot: slot.saturating_sub(1),
            blockhash: Array([1u8; 32]),
            parent_blockhash: Array([2u8; 32]),
            block_time: Some(1_700_000_000),
            block_height: Some(slot),
            executed_transaction_count: 1,
            entry_count: 1,
            rewards_present: 0,
            rewards_pubkey: Vec::new(),
            rewards_lamports: Vec::new(),
            rewards_post_balance: Vec::new(),
            rewards_type: Vec::new(),
            rewards_commission: Vec::new(),
            rewards_num_partitions: None,
        }
    }
}
