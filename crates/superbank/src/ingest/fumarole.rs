// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::{
    collections::HashMap,
    fs,
    num::{NonZeroU8, NonZeroUsize},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, ensure};
use clickhouse::Client as ClickHouseClient;
use futures::StreamExt;
use prost::Message as _;
use serde_yaml::{Mapping, Value};
use tokio::time::{Instant, MissedTickBehavior, Sleep, interval, sleep_until};
use tonic::Code;
use tracing::{info, warn};
use yellowstone_fumarole_client::{
    FumaroleClient, FumaroleSubscribeConfig,
    config::FumaroleConfig,
    proto::{CreateConsumerGroupRequest, InitialOffsetPolicy},
    stream::{FumaroleEvent, FumaroleStream},
};
use yellowstone_grpc_proto::prelude::{
    SubscribeRequest, SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterEntry,
    SubscribeRequestFilterTransactions, SubscribeUpdate, SubscribeUpdateBlock,
    SubscribeUpdateBlockMeta, SubscribeUpdateEntry, SubscribeUpdateTransactionInfo,
    subscribe_update::UpdateOneof,
};

use crate::cli::{Args, FUMAROLE_CONCURRENT_DOWNLOAD_LIMIT_PER_TCP, FromSlotSpec};
use crate::clickhouse::{InsertTables, build_clickhouse_client, fetch_latest_slot_from_blocks};
use crate::commitment::parse_durable_commitment;
use crate::ingest::grpc::{BufferedRows, process_update, validate_block_completeness};
use crate::metrics;
use crate::shutdown::spawn_shutdown_watch;

pub(crate) async fn run_fumarole_ingest(args: &Args) -> Result<()> {
    let endpoint = fumarole_endpoint(args)?;
    let consumer_group = args
        .fumarole_consumer_group
        .as_deref()
        .context("fumarole source requires consumer group")?;
    let commitment = parse_durable_commitment(&args.commitment)? as i32;
    let clickhouse = build_clickhouse_client(args);

    if args.fumarole_concurrent_download_limit_per_tcp != FUMAROLE_CONCURRENT_DOWNLOAD_LIMIT_PER_TCP
    {
        warn!(
            requested = args.fumarole_concurrent_download_limit_per_tcp,
            effective = FUMAROLE_CONCURRENT_DOWNLOAD_LIMIT_PER_TCP,
            "fumarole-concurrent-download-limit-per-tcp is deprecated and ignored by the Fumarole client"
        );
    }

    info!(
        source = "fumarole",
        endpoint = %endpoint,
        consumer_group = %consumer_group,
        transactions_table = %args.transactions_table,
        blocks_table = %args.blocks_table,
        grpc_max_decoding_bytes = args.grpc_max_decoding_bytes,
        grpc_idle_timeout_secs = args.grpc_idle_timeout_secs,
        fumarole_data_plane_tcp_connections = args.fumarole_data_plane_tcp_connections,
        fumarole_requested_concurrent_download_limit_per_tcp = args.fumarole_concurrent_download_limit_per_tcp,
        fumarole_effective_concurrent_download_limit_per_tcp = FUMAROLE_CONCURRENT_DOWNLOAD_LIMIT_PER_TCP,
        fumarole_data_channel_capacity = args.fumarole_data_channel_capacity,
        fumarole_memory_soft_limit_bytes = args.fumarole_memory_soft_limit_bytes,
        fumarole_commit_interval_secs = args.fumarole_commit_interval_secs,
        fumarole_no_commit = args.fumarole_no_commit,
        "starting superbank fumarole ingest"
    );
    metrics::set_fumarole_data_channel_capacity(args.fumarole_data_channel_capacity);
    metrics::set_fumarole_memory_soft_limit_bytes(args.fumarole_memory_soft_limit_bytes);

    if args.fumarole_from_slot.is_some() && !args.fumarole_create_consumer_group {
        warn!(
            "fumarole-from-slot is ignored for existing Fumarole consumer groups; create the group with --fumarole-create-consumer-group to initialize it from a slot"
        );
    }

    let mut client = FumaroleClient::connect(build_fumarole_config(args)?)
        .await
        .context("connect to Fumarole")?;

    create_consumer_group_if_requested(&mut client, args, &clickhouse).await?;

    let include_entries = args.entries_table.is_some();
    let request = build_fumarole_subscribe_request(commitment, include_entries);
    let subscribe_config = build_subscribe_config(args)?;
    let subscription = client
        .subscribe_with_config(consumer_group, request, subscribe_config)
        .await
        .with_context(|| format!("subscribe to Fumarole consumer group {consumer_group}"))?;
    let (_sink, stream) = subscription.split();
    let mut stream = stream;
    let track_estimated_bytes = args.fumarole_memory_soft_limit_bytes > 0;
    let mut block_assembler = FumaroleBlockAssembler::new(track_estimated_bytes, include_entries);
    let mut pressure_guard = FumarolePressureGuard::new(args.fumarole_memory_soft_limit_bytes);
    pressure_guard.observe(&block_assembler);

    let mut buffered_rows = BufferedRows::new(args);
    let insert_tables = InsertTables::from_args(args);
    let mut flush_timer = interval(Duration::from_secs(args.flush_interval_secs));
    flush_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let mut shutdown_rx = spawn_shutdown_watch();
    let mut last_processed_block_slot = None;
    let mut bound_exceeded = false;
    let idle_timeout = Duration::from_secs(args.grpc_idle_timeout_secs);
    let idle_timer = sleep_until(Instant::now() + idle_timeout);
    tokio::pin!(idle_timer);

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
                flush_and_commit(
                    &clickhouse,
                    &insert_tables,
                    &mut buffered_rows,
                    &mut stream,
                    &block_assembler,
                    false,
                )
                .await?;
                pressure_guard.observe(&block_assembler);
            }
            _ = &mut idle_timer => {
                let reason = format!(
                    "Fumarole stream idle for more than {} seconds",
                    args.grpc_idle_timeout_secs
                );
                metrics::observe_source_error("fumarole_stream", "idle_timeout");
                warn!(
                    reason = %reason,
                    last_processed_block_slot,
                    "fatal Fumarole idle timeout; flushing pending rows before exit"
                );
                flush_after_fatal_condition(
                    &clickhouse,
                    &insert_tables,
                    &mut buffered_rows,
                    &mut stream,
                    &block_assembler,
                    &reason,
                )
                .await?;
                return Err(anyhow!(reason));
            }
            event = stream.next() => {
                match event {
                    Some(Ok(event)) => {
                        reset_idle_timer(idle_timer.as_mut(), idle_timeout);
                        let event_slot = match &event {
                            FumaroleEvent::Data { slot, .. } | FumaroleEvent::SlotEnded { slot, .. } => *slot,
                        };
                        if stop_at_historical_bound(event_slot, args, &block_assembler, &mut bound_exceeded, &clickhouse, &insert_tables, &mut buffered_rows).await? {
                            return Err(anyhow!("Fumarole reached slot {event_slot} beyond its qualified historical bound; prior valid rows flushed without acknowledging rejected progress"));
                        }
                        if bound_exceeded && event_slot > historical_bound(args)? {
                            continue;
                        }
                        match event {
                            FumaroleEvent::Data { slot, blockhash, update } => {
                                match block_assembler.handle_update((slot, blockhash), update)? {
                                    FumaroleAssembledUpdate::None => {}
                                    FumaroleAssembledUpdate::SlotStatus(status_slot, status) => {
                                        observe_processed_slot(&mut last_processed_block_slot, status_slot);
                                        if status == commitment {
                                            metrics::set_network_tip_slot(status_slot);
                                        }
                                    }
                                    FumaroleAssembledUpdate::Block(update) => {
                                        let update = *update;
                                        let block_slot = processed_fumarole_block_slot(&update);
                                        let flushed = process_update(
                                            update,
                                            args,
                                            &insert_tables,
                                            &clickhouse,
                                            &mut buffered_rows,
                                            None,
                                        )
                                        .await?;
                                        if let Some(slot) = block_slot {
                                            observe_processed_slot(&mut last_processed_block_slot, slot);
                                        }
                                        if flushed {
                                            commit_if_assembled(&block_assembler, || stream.commit());
                                        }
                                    }
                                }
                            }
                            FumaroleEvent::SlotEnded { slot, blockhash } => {
                                // Validation errors exit before flushing or committing. In
                                // particular, do not use the fatal transport-error flush path:
                                // it could acknowledge the rejected slot's source offset.
                                if let Some(update) = block_assembler.finish_slot((slot, blockhash))? {
                                    if process_update(
                                        update,
                                        args,
                                        &insert_tables,
                                        &clickhouse,
                                        &mut buffered_rows,
                                        None,
                                    )
                                    .await?
                                    {
                                        commit_if_assembled(&block_assembler, || stream.commit());
                                    }
                                } else if buffered_rows.is_empty() {
                                    commit_if_assembled(&block_assembler, || stream.commit());
                                }
                                observe_processed_slot(&mut last_processed_block_slot, slot);
                            }
                        }
                        if stop_at_historical_bound(event_slot, args, &block_assembler, &mut bound_exceeded, &clickhouse, &insert_tables, &mut buffered_rows).await? {
                            return Err(anyhow!("Fumarole completed every bank at or below its qualified historical bound; prior valid rows flushed without acknowledging rejected progress"));
                        }
                        maybe_flush_for_pressure(
                            &clickhouse,
                            &insert_tables,
                            &mut buffered_rows,
                            &mut stream,
                            &block_assembler,
                            &mut pressure_guard,
                        )
                        .await?;
                    }
                    Some(Err(err)) => {
                        let reason = format!("Fumarole stream error: {err}");
                        metrics::observe_source_error("fumarole_stream", "error");
                        warn!(
                            reason = %reason,
                            last_processed_block_slot,
                            "Fumarole stream error; flushing pending rows before exit"
                        );
                        flush_after_fatal_condition(
                            &clickhouse,
                            &insert_tables,
                            &mut buffered_rows,
                            &mut stream,
                            &block_assembler,
                            &reason,
                        )
                        .await?;
                        return Err(anyhow!(reason));
                    }
                    None => {
                        let reason = "Fumarole stream ended".to_string();
                        metrics::observe_source_error("fumarole_stream", "ended");
                        warn!(
                            last_processed_block_slot,
                            "Fumarole stream ended; flushing pending rows before exit"
                        );
                        flush_after_fatal_condition(
                            &clickhouse,
                            &insert_tables,
                            &mut buffered_rows,
                            &mut stream,
                            &block_assembler,
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
        result = flush_and_commit(
            &clickhouse,
            &insert_tables,
            &mut buffered_rows,
            &mut stream,
            &block_assembler,
            false,
        ) => {
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

fn fumarole_endpoint(args: &Args) -> Result<&str> {
    args.fumarole_endpoint
        .as_deref()
        .context("fumarole source requires --fumarole-endpoint / FUMAROLE_ENDPOINT / config fumarole_endpoint")
}

fn fumarole_x_token(args: &Args) -> Option<&String> {
    args.fumarole_x_token.as_ref()
}

fn build_fumarole_config(args: &Args) -> Result<FumaroleConfig> {
    let mut mapping = Mapping::new();
    mapping.insert(
        Value::String("endpoint".to_string()),
        Value::String(fumarole_endpoint(args)?.to_string()),
    );
    mapping.insert(
        Value::String("max_decoding_message_size_bytes".to_string()),
        Value::Number(args.grpc_max_decoding_bytes.into()),
    );
    if let Some(x_token) = fumarole_x_token(args) {
        mapping.insert(
            Value::String("x-token".to_string()),
            Value::String(x_token.clone()),
        );
    }
    serde_yaml::from_value(Value::Mapping(mapping)).context("build Fumarole config")
}

fn build_subscribe_config(args: &Args) -> Result<FumaroleSubscribeConfig> {
    Ok(FumaroleSubscribeConfig {
        num_data_plane_tcp_connections: NonZeroU8::new(args.fumarole_data_plane_tcp_connections)
            .context("fumarole data-plane-tcp-connections must be greater than 0")?,
        data_channel_capacity: NonZeroUsize::new(args.fumarole_data_channel_capacity)
            .context("fumarole data-channel-capacity must be greater than 0")?,
        commit_interval: Duration::from_secs(args.fumarole_commit_interval_secs),
        no_commit: args.fumarole_no_commit,
        auto_commit: false,
        ..Default::default()
    })
}

fn build_fumarole_subscribe_request(commitment: i32, include_entries: bool) -> SubscribeRequest {
    let mut transactions = std::collections::HashMap::new();
    transactions.insert(
        "transactions".to_string(),
        SubscribeRequestFilterTransactions::default(),
    );

    let mut blocks_meta = std::collections::HashMap::new();
    blocks_meta.insert(
        "blocks_meta".to_string(),
        SubscribeRequestFilterBlocksMeta::default(),
    );

    let mut entry = std::collections::HashMap::new();
    if include_entries {
        entry.insert(
            "entries".to_string(),
            SubscribeRequestFilterEntry::default(),
        );
    }

    SubscribeRequest {
        transactions,
        blocks_meta,
        entry,
        commitment: Some(commitment),
        ..Default::default()
    }
}

struct FumarolePressureGuard {
    soft_limit_bytes: u64,
    last_rss_bytes: Option<u64>,
    last_rss_sample: Option<Instant>,
    pressure_logged: bool,
}

struct FumarolePressureSnapshot {
    soft_limit_bytes: u64,
    buffered_bytes: u64,
    pending_slots: usize,
    rss_bytes: Option<u64>,
    over_limit_reason: Option<&'static str>,
}

impl FumarolePressureGuard {
    const RSS_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

    const fn new(soft_limit_bytes: u64) -> Self {
        Self {
            soft_limit_bytes,
            last_rss_bytes: None,
            last_rss_sample: None,
            pressure_logged: false,
        }
    }

    fn observe(&mut self, block_assembler: &FumaroleBlockAssembler) -> FumarolePressureSnapshot {
        let buffered_bytes = block_assembler.estimated_buffered_bytes();
        let pending_slots = block_assembler.pending_slots();

        if self.soft_limit_bytes > 0
            && self
                .last_rss_sample
                .is_none_or(|sampled_at| sampled_at.elapsed() >= Self::RSS_SAMPLE_INTERVAL)
        {
            self.last_rss_bytes = current_rss_bytes();
            self.last_rss_sample = Some(Instant::now());
        }

        metrics::set_fumarole_buffered_bytes(buffered_bytes);
        metrics::set_fumarole_pending_slots(pending_slots);
        if let Some(rss_bytes) = self.last_rss_bytes {
            metrics::set_fumarole_rss_bytes(rss_bytes);
        }

        let over_limit_reason = if self.soft_limit_bytes == 0 {
            None
        } else if buffered_bytes >= self.soft_limit_bytes {
            Some("buffered_bytes")
        } else if self
            .last_rss_bytes
            .is_some_and(|rss_bytes| rss_bytes >= self.soft_limit_bytes)
        {
            Some("rss")
        } else {
            None
        };

        FumarolePressureSnapshot {
            soft_limit_bytes: self.soft_limit_bytes,
            buffered_bytes,
            pending_slots,
            rss_bytes: self.last_rss_bytes,
            over_limit_reason,
        }
    }

    fn warn_once(&mut self, warn: impl FnOnce()) {
        if self.pressure_logged {
            return;
        }
        warn();
        self.pressure_logged = true;
    }

    fn clear_logged(&mut self) {
        self.pressure_logged = false;
    }
}

fn current_rss_bytes() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    parse_proc_status_rss_bytes(&status)
}

fn parse_proc_status_rss_bytes(status: &str) -> Option<u64> {
    for line in status.lines() {
        let Some(value) = line.strip_prefix("VmRSS:") else {
            continue;
        };
        let mut fields = value.split_whitespace();
        let amount = fields.next()?.parse::<u64>().ok()?;
        let unit = fields.next().unwrap_or("kB");
        return match unit {
            "kB" | "KB" | "Kb" => amount.checked_mul(1024),
            "B" => Some(amount),
            _ => None,
        };
    }
    None
}

enum FumaroleAssembledUpdate {
    None,
    SlotStatus(u64, i32),
    Block(Box<SubscribeUpdate>),
}

// Fumarole IDs are sealed blockhashes, rather than validator-local Geyser counters.
type FumaroleBankKey = (u64, Option<Arc<str>>);

struct FumaroleBlockAssembler {
    blocks: HashMap<FumaroleBankKey, FumaroleBlockParts>,
    estimated_buffered_bytes: u64,
    track_estimated_bytes: bool,
    include_entries: bool,
}

#[derive(Default)]
struct FumaroleBlockParts {
    bank_id: Option<u64>,
    block_meta: Option<SubscribeUpdateBlockMeta>,
    block_meta_created_at: Option<prost_types::Timestamp>,
    block_meta_bytes: u64,
    transactions: Vec<SubscribeUpdateTransactionInfo>,
    entries: Vec<SubscribeUpdateEntry>,
    estimated_bytes: u64,
}

impl FumaroleBlockAssembler {
    fn new(track_estimated_bytes: bool, include_entries: bool) -> Self {
        Self {
            blocks: HashMap::new(),
            estimated_buffered_bytes: 0,
            track_estimated_bytes,
            include_entries,
        }
    }

    fn estimated_buffered_bytes(&self) -> u64 {
        self.estimated_buffered_bytes
    }

    fn pending_slots(&self) -> usize {
        self.blocks.len()
    }

    fn has_pending_at_or_below(&self, slot: u64) -> bool {
        self.blocks.keys().any(|(pending, _)| *pending <= slot)
    }

    fn handle_update(
        &mut self,
        key: FumaroleBankKey,
        update: SubscribeUpdate,
    ) -> Result<FumaroleAssembledUpdate> {
        let stream_slot = key.0;
        let bank = match update.update_oneof.as_ref() {
            Some(UpdateOneof::BlockMeta(meta)) => {
                validate_fumarole_hash(&key, &meta.blockhash)?;
                Some((meta.slot, meta.bank_id))
            }
            Some(UpdateOneof::Transaction(tx)) => Some((tx.slot, tx.bank_id)),
            Some(UpdateOneof::Entry(entry)) => Some((entry.slot, entry.bank_id)),
            Some(UpdateOneof::Block(block)) => {
                validate_fumarole_hash(&key, &block.blockhash)?;
                Some((block.slot, block.bank_id))
            }
            _ => None,
        };
        if let Some((slot, bank_id)) = bank {
            anyhow::ensure!(
                slot == stream_slot,
                "Fumarole payload slot {slot} differs from envelope slot {stream_slot}"
            );
            let previous = self.blocks.get(&key);
            ensure!(
                key.1.is_some()
                    || previous
                        .and_then(|p| p.bank_id)
                        .is_none_or(|id| id == bank_id),
                "Fumarole bank identity changed within slot {slot}; cannot combine node-local bank IDs"
            );
        }
        match update.update_oneof.as_ref() {
            Some(UpdateOneof::Block(block)) => {
                validate_block_completeness(block, self.include_entries)?
            }
            Some(UpdateOneof::Transaction(tx)) => ensure!(
                tx.transaction.is_some(),
                "Fumarole transaction update missing transaction info"
            ),
            Some(UpdateOneof::BlockMeta(meta)) => ensure!(
                self.blocks
                    .get(&key)
                    .and_then(|p| p.block_meta.as_ref())
                    .is_none_or(|old| old == meta),
                "Fumarole block {stream_slot} received conflicting block meta"
            ),
            _ => {}
        }
        if let Some((_, bank_id)) = bank {
            self.blocks.entry(key.clone()).or_default().bank_id = Some(bank_id);
        }
        let update_bytes = self.estimated_update_bytes(&update);
        match update.update_oneof {
            Some(UpdateOneof::Slot(slot)) => {
                Ok(FumaroleAssembledUpdate::SlotStatus(slot.slot, slot.status))
            }
            Some(UpdateOneof::Block(block)) => {
                if let Some(parts) = self.blocks.remove(&key) {
                    self.estimated_buffered_bytes = self
                        .estimated_buffered_bytes
                        .saturating_sub(parts.estimated_bytes);
                }
                Ok(FumaroleAssembledUpdate::Block(Box::new(SubscribeUpdate {
                    filters: update.filters,
                    created_at: update.created_at,
                    update_oneof: Some(UpdateOneof::Block(block)),
                })))
            }
            Some(UpdateOneof::BlockMeta(meta)) => {
                let block = self.blocks.entry(key).or_default();
                if block.block_meta.is_some() {
                    self.estimated_buffered_bytes = self
                        .estimated_buffered_bytes
                        .saturating_sub(block.block_meta_bytes);
                    block.estimated_bytes =
                        block.estimated_bytes.saturating_sub(block.block_meta_bytes);
                }
                block.block_meta_created_at = update.created_at;
                block.block_meta = Some(meta);
                block.block_meta_bytes = update_bytes;
                block.estimated_bytes = block.estimated_bytes.saturating_add(update_bytes);
                self.estimated_buffered_bytes =
                    self.estimated_buffered_bytes.saturating_add(update_bytes);
                Ok(FumaroleAssembledUpdate::None)
            }
            Some(UpdateOneof::Transaction(tx)) => {
                if let Some(info) = tx.transaction {
                    let block = self.blocks.entry(key).or_default();
                    block.transactions.push(info);
                    block.estimated_bytes = block.estimated_bytes.saturating_add(update_bytes);
                    self.estimated_buffered_bytes =
                        self.estimated_buffered_bytes.saturating_add(update_bytes);
                } else {
                    warn!(
                        slot = stream_slot,
                        "Fumarole transaction update missing transaction info"
                    );
                }
                Ok(FumaroleAssembledUpdate::None)
            }
            Some(UpdateOneof::Entry(entry)) => {
                let block = self.blocks.entry(key).or_default();
                block.entries.push(entry);
                block.estimated_bytes = block.estimated_bytes.saturating_add(update_bytes);
                self.estimated_buffered_bytes =
                    self.estimated_buffered_bytes.saturating_add(update_bytes);
                Ok(FumaroleAssembledUpdate::None)
            }
            Some(UpdateOneof::Ping(_)) | Some(UpdateOneof::Pong(_)) | None => {
                Ok(FumaroleAssembledUpdate::None)
            }
            Some(_) => Ok(FumaroleAssembledUpdate::None),
        }
    }

    fn estimated_update_bytes(&self, update: &SubscribeUpdate) -> u64 {
        if !self.track_estimated_bytes {
            return 0;
        }

        match update.update_oneof.as_ref() {
            Some(UpdateOneof::BlockMeta(_))
            | Some(UpdateOneof::Transaction(_))
            | Some(UpdateOneof::Entry(_)) => update.encoded_len() as u64,
            _ => 0,
        }
    }

    fn finish_slot(&mut self, key: FumaroleBankKey) -> Result<Option<SubscribeUpdate>> {
        let slot = key.0;
        let Some(parts) = self.blocks.remove(&key) else {
            return Ok(None);
        };
        self.estimated_buffered_bytes = self
            .estimated_buffered_bytes
            .saturating_sub(parts.estimated_bytes);
        parts.into_subscribe_update(slot, self.include_entries)
    }
}

impl FumaroleBlockParts {
    fn into_subscribe_update(
        self,
        slot: u64,
        include_entries: bool,
    ) -> Result<Option<SubscribeUpdate>> {
        let Self {
            bank_id: _,
            block_meta,
            block_meta_created_at,
            block_meta_bytes: _,
            transactions,
            entries,
            estimated_bytes: _,
        } = self;

        let Some(meta) = block_meta else {
            if transactions.is_empty() && entries.is_empty() {
                return Ok(None);
            }
            return Err(anyhow!(
                "Fumarole block {slot} ended without block meta after receiving {} transactions and {} entries",
                transactions.len(),
                entries.len()
            ));
        };

        let update = SubscribeUpdate {
            filters: Vec::new(),
            created_at: block_meta_created_at,
            update_oneof: Some(UpdateOneof::Block(SubscribeUpdateBlock {
                slot,
                bank_id: meta.bank_id,
                blockhash: meta.blockhash,
                rewards: meta.rewards,
                block_time: meta.block_time,
                block_height: meta.block_height,
                parent_slot: meta.parent_slot,
                parent_blockhash: meta.parent_blockhash,
                executed_transaction_count: meta.executed_transaction_count,
                transactions,
                updated_account_count: 0,
                accounts: Vec::new(),
                entries_count: meta.entries_count,
                entries,
            })),
        };
        if let Some(UpdateOneof::Block(block)) = update.update_oneof.as_ref() {
            validate_block_completeness(block, include_entries)?;
        }
        Ok(Some(update))
    }
}

fn validate_fumarole_hash(key: &FumaroleBankKey, blockhash: &str) -> Result<()> {
    if let Some(expected) = &key.1 {
        anyhow::ensure!(
            !expected.is_empty() && expected.as_ref() == blockhash,
            "Fumarole sealed blockhash differs from envelope for slot {}",
            key.0
        );
    }
    Ok(())
}

fn processed_fumarole_block_slot(update: &SubscribeUpdate) -> Option<u64> {
    match update.update_oneof.as_ref() {
        Some(UpdateOneof::Block(block)) => Some(block.slot),
        _ => None,
    }
}

async fn create_consumer_group_if_requested(
    client: &mut FumaroleClient,
    args: &Args,
    clickhouse: &ClickHouseClient,
) -> Result<()> {
    if !args.fumarole_create_consumer_group {
        return Ok(());
    }

    let consumer_group = args
        .fumarole_consumer_group
        .as_deref()
        .context("fumarole source requires consumer group")?;
    let from_slot = resolve_create_consumer_group_from_slot(args, clickhouse).await?;
    let initial_offset_policy = if from_slot.is_some() {
        InitialOffsetPolicy::FromSlot
    } else {
        InitialOffsetPolicy::Latest
    };

    let request = CreateConsumerGroupRequest {
        consumer_group_name: consumer_group.to_string(),
        initial_offset_policy: initial_offset_policy.into(),
        from_slot,
    };

    match client.create_consumer_group(request).await {
        Ok(_) => {
            info!(
                consumer_group = %consumer_group,
                from_slot,
                "created Fumarole consumer group"
            );
            Ok(())
        }
        Err(status) if status.code() == Code::AlreadyExists => {
            warn!(
                consumer_group = %consumer_group,
                "Fumarole consumer group already exists; using existing committed offset"
            );
            Ok(())
        }
        Err(status) => {
            Err(status).with_context(|| format!("create Fumarole consumer group {consumer_group}"))
        }
    }
}

async fn resolve_create_consumer_group_from_slot(
    args: &Args,
    clickhouse: &ClickHouseClient,
) -> Result<Option<u64>> {
    match args.fumarole_from_slot {
        Some(FromSlotSpec::LatestDb) => {
            let latest = fetch_latest_slot_from_blocks(clickhouse, &args.blocks_table)
                .await?
                .ok_or_else(|| {
                    anyhow!(
                        "fumarole-from-slot '*' requires at least one row in {}",
                        args.blocks_table
                    )
                })?;
            info!(
                slot = latest,
                table = %args.blocks_table,
                "resolved Fumarole consumer group fumarole-from-slot='*' to latest slot in blocks_metadata"
            );
            Ok(Some(latest))
        }
        Some(FromSlotSpec::Slot(slot)) => Ok(Some(slot)),
        None => Ok(None),
    }
}

fn historical_bound(args: &Args) -> Result<u64> {
    args.fumarole_alpenglow_genesis_slot
        .or(args.fumarole_preactivation_through_slot)
        .context("Fumarole requires an evidenced historical bound")
}

async fn stop_at_historical_bound(
    event_slot: u64,
    args: &Args,
    block_assembler: &FumaroleBlockAssembler,
    bound_exceeded: &mut bool,
    clickhouse: &ClickHouseClient,
    tables: &InsertTables,
    rows: &mut BufferedRows,
) -> Result<bool> {
    let bound = historical_bound(args)?;
    if event_slot > bound {
        *bound_exceeded = true;
    }
    // Banks at or below the bound can still complete after a later slot's data arrives.
    if !*bound_exceeded || block_assembler.has_pending_at_or_below(bound) {
        return Ok(false);
    }
    metrics::observe_source_error("fumarole_stream", "historical_bound");
    // Flush only validated complete blocks. Offsets stay unacknowledged, so restart replays the prefix.
    rows.flush(clickhouse, tables).await?;
    warn!(
        event_slot,
        bound,
        "Fumarole historical bound reached; flushed valid prior rows without committing offsets"
    );
    Ok(true)
}

async fn flush_and_commit(
    clickhouse: &ClickHouseClient,
    insert_tables: &InsertTables,
    buffered_rows: &mut BufferedRows,
    stream: &mut FumaroleStream,
    block_assembler: &FumaroleBlockAssembler,
    commit_if_empty: bool,
) -> Result<()> {
    let had_rows = !buffered_rows.is_empty();
    buffered_rows.flush(clickhouse, insert_tables).await?;
    if had_rows || commit_if_empty {
        commit_if_assembled(block_assembler, || stream.commit());
    }
    Ok(())
}

async fn flush_after_fatal_condition(
    clickhouse: &ClickHouseClient,
    insert_tables: &InsertTables,
    buffered_rows: &mut BufferedRows,
    stream: &mut FumaroleStream,
    block_assembler: &FumaroleBlockAssembler,
    reason: &str,
) -> Result<()> {
    flush_and_commit(
        clickhouse,
        insert_tables,
        buffered_rows,
        stream,
        block_assembler,
        false,
    )
    .await
    .with_context(|| format!("flush buffered rows after fatal Fumarole condition: {reason}"))
}

fn commit_if_assembled(block_assembler: &FumaroleBlockAssembler, commit: impl FnOnce()) {
    // The client acknowledges all pending source progress together. A flush of
    // complete rows must not acknowledge other slots still being reconstructed.
    if block_assembler.pending_slots() == 0 {
        commit();
    }
}

async fn maybe_flush_for_pressure(
    clickhouse: &ClickHouseClient,
    insert_tables: &InsertTables,
    buffered_rows: &mut BufferedRows,
    stream: &mut FumaroleStream,
    block_assembler: &FumaroleBlockAssembler,
    pressure_guard: &mut FumarolePressureGuard,
) -> Result<()> {
    let snapshot = pressure_guard.observe(block_assembler);
    let Some(reason) = snapshot.over_limit_reason else {
        pressure_guard.clear_logged();
        return Ok(());
    };

    if buffered_rows.is_empty() {
        pressure_guard.warn_once(|| {
            warn!(
                reason,
                soft_limit_bytes = snapshot.soft_limit_bytes,
                buffered_bytes = snapshot.buffered_bytes,
                pending_slots = snapshot.pending_slots,
                rss_bytes = snapshot.rss_bytes,
                "Fumarole memory pressure detected but no complete rows are ready to flush; continuing to drain until a slot completes"
            );
        });
        return Ok(());
    }

    warn!(
        reason,
        soft_limit_bytes = snapshot.soft_limit_bytes,
        buffered_bytes = snapshot.buffered_bytes,
        pending_slots = snapshot.pending_slots,
        rss_bytes = snapshot.rss_bytes,
        "Fumarole memory pressure detected; flushing buffered rows before polling more events"
    );
    metrics::observe_fumarole_pressure_flush();
    flush_and_commit(
        clickhouse,
        insert_tables,
        buffered_rows,
        stream,
        block_assembler,
        false,
    )
    .await?;
    pressure_guard.clear_logged();
    pressure_guard.observe(block_assembler);
    Ok(())
}

fn reset_idle_timer(idle_timer: std::pin::Pin<&mut Sleep>, idle_timeout: Duration) {
    idle_timer.reset(Instant::now() + idle_timeout);
}

fn observe_processed_slot(last_processed_block_slot: &mut Option<u64>, slot: u64) {
    if last_processed_block_slot.is_none_or(|last| slot > last) {
        *last_processed_block_slot = Some(slot);
        metrics::set_last_processed_slot(slot);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yellowstone_grpc_proto::prelude::SubscribeUpdateTransaction;

    #[test]
    fn fumarole_assembly_preserves_unknown_enum_vat_debit() {
        let meta = SubscribeUpdate {
            update_oneof: Some(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                slot: 42,
                rewards: Some(yellowstone_grpc_proto::prelude::Rewards {
                    rewards: vec![yellowstone_grpc_proto::prelude::Reward {
                        pubkey: "11111111111111111111111111111111".to_owned(),
                        lamports: -10,
                        post_balance: 90,
                        reward_type: 6,
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            })),
            ..Default::default()
        };
        let wire = meta.encode_to_vec();
        let decoded = SubscribeUpdate::decode(wire.as_slice()).unwrap();
        let mut assembler = FumaroleBlockAssembler::new(false, false);
        assembler.handle_update((42, None), decoded).unwrap();
        let update = assembler.finish_slot((42, None)).unwrap().unwrap();
        let Some(UpdateOneof::Block(block)) = update.update_oneof else {
            panic!("block")
        };
        let reward = &block.rewards.unwrap().rewards[0];
        assert_eq!(reward.reward_type, 6);
        assert_eq!(reward.lamports, -10);
        assert_eq!(reward.post_balance, 90);
    }

    #[test]
    fn fumarole_block_assembler_builds_block_update_without_block_stream_adapter() {
        let mut assembler = FumaroleBlockAssembler::new(false, false);
        assembler
            .handle_update(
                (42, None),
                SubscribeUpdate {
                    update_oneof: Some(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                        slot: 42,
                        blockhash: "hash".to_string(),
                        executed_transaction_count: 1,
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .expect("block meta");
        assembler
            .handle_update(
                (42, None),
                SubscribeUpdate {
                    update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
                        slot: 42,
                        transaction: Some(SubscribeUpdateTransactionInfo {
                            index: 0,
                            ..Default::default()
                        }),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .expect("transaction");

        let update = assembler
            .finish_slot((42, None))
            .expect("finish slot")
            .expect("block update");

        let Some(UpdateOneof::Block(block)) = update.update_oneof else {
            panic!("expected assembled block update");
        };
        assert_eq!(block.slot, 42);
        assert_eq!(block.transactions.len(), 1);
        assert_eq!(block.transactions[0].index, 0);
    }

    #[test]
    fn fumarole_block_assembler_rejects_payload_without_block_meta() {
        let mut assembler = FumaroleBlockAssembler::new(false, false);
        assembler
            .handle_update(
                (42, None),
                SubscribeUpdate {
                    update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
                        slot: 42,
                        transaction: Some(SubscribeUpdateTransactionInfo {
                            index: 0,
                            ..Default::default()
                        }),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .expect("transaction");

        let err = assembler
            .finish_slot((42, None))
            .expect_err("missing block meta");

        assert!(
            err.to_string()
                .contains("ended without block meta after receiving 1 transactions")
        );
    }

    #[test]
    fn fumarole_block_assembler_tracks_pending_slots_and_estimated_bytes() {
        let mut assembler = FumaroleBlockAssembler::new(true, false);

        assembler
            .handle_update(
                (42, None),
                SubscribeUpdate {
                    update_oneof: Some(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                        slot: 42,
                        blockhash: "hash".to_string(),
                        executed_transaction_count: 0,
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .expect("block meta");
        let after_meta_bytes = assembler.estimated_buffered_bytes();
        assert_eq!(assembler.pending_slots(), 1);
        assert!(after_meta_bytes > 0);

        assembler
            .handle_update(
                (43, None),
                SubscribeUpdate {
                    update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
                        slot: 43,
                        transaction: Some(SubscribeUpdateTransactionInfo {
                            index: 0,
                            ..Default::default()
                        }),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .expect("transaction");
        assert_eq!(assembler.pending_slots(), 2);
        assert!(assembler.estimated_buffered_bytes() > after_meta_bytes);

        let update = assembler
            .finish_slot((42, None))
            .expect("finish slot")
            .expect("block update");
        assert_eq!(processed_fumarole_block_slot(&update), Some(42));
        assert_eq!(assembler.pending_slots(), 1);
        assert!(assembler.estimated_buffered_bytes() > 0);

        let err = assembler
            .finish_slot((43, None))
            .expect_err("missing block meta");
        assert!(
            err.to_string()
                .contains("ended without block meta after receiving 1 transactions")
        );
        assert_eq!(assembler.pending_slots(), 0);
        assert_eq!(assembler.estimated_buffered_bytes(), 0);
    }

    #[test]
    fn fumarole_block_assembler_skips_estimated_bytes_when_tracking_disabled() {
        let mut assembler = FumaroleBlockAssembler::new(false, false);

        assembler
            .handle_update(
                (42, None),
                SubscribeUpdate {
                    update_oneof: Some(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                        slot: 42,
                        blockhash: "hash".to_string(),
                        executed_transaction_count: 1,
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .expect("block meta");
        assembler
            .handle_update(
                (42, None),
                SubscribeUpdate {
                    update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
                        slot: 42,
                        transaction: Some(SubscribeUpdateTransactionInfo {
                            index: 0,
                            ..Default::default()
                        }),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .expect("transaction");

        assert_eq!(assembler.pending_slots(), 1);
        assert_eq!(assembler.estimated_buffered_bytes(), 0);

        let update = assembler
            .finish_slot((42, None))
            .expect("finish slot")
            .expect("block update");
        assert_eq!(processed_fumarole_block_slot(&update), Some(42));
        assert_eq!(assembler.pending_slots(), 0);
        assert_eq!(assembler.estimated_buffered_bytes(), 0);
    }

    #[test]
    fn parse_proc_status_rss_bytes_reads_kilobytes() {
        let status = "\
Name:\tsuperbank
VmPeak:\t  123456 kB
VmRSS:\t   12345 kB
";

        assert_eq!(parse_proc_status_rss_bytes(status), Some(12_641_280));
    }

    #[test]
    fn parse_proc_status_rss_bytes_ignores_missing_or_unknown_units() {
        assert_eq!(parse_proc_status_rss_bytes("Name:\tsuperbank\n"), None);
        assert_eq!(parse_proc_status_rss_bytes("VmRSS:\t123 MB\n"), None);
    }
    #[test]
    fn fumarole_requires_finalized_canonical_ingestion() {
        assert!(parse_durable_commitment("processed").is_err());
        assert!(parse_durable_commitment("confirmed").is_err());
        assert_eq!(parse_durable_commitment("finalized").unwrap() as i32, 2);
    }

    #[test]
    fn protobuf_decoding_preserves_competing_bank_ids() {
        let a = SubscribeUpdateTransaction::decode(&[0x10, 42, 0x18, 1][..]).unwrap();
        let b = SubscribeUpdateTransaction::decode(&[0x10, 42, 0x18, 2][..]).unwrap();
        assert_eq!(a.slot, b.slot);
        assert_ne!(a.bank_id, b.bank_id);
    }

    #[test]
    fn fumarole_interleaves_same_slot_banks_and_finishes_only_the_named_hash() {
        let mut assembler = FumaroleBlockAssembler::new(true, false);
        let a = (42, Some(Arc::<str>::from("hash-a")));
        let b = (42, Some(Arc::<str>::from("hash-b")));
        for (key, bank_id) in [(&a, 1), (&b, 2)] {
            assembler
                .handle_update(
                    key.clone(),
                    SubscribeUpdate {
                        update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
                            slot: 42,
                            bank_id,
                            transaction: Some(SubscribeUpdateTransactionInfo {
                                index: 0,
                                signature: vec![bank_id as u8; 64],
                                ..Default::default()
                            }),
                        })),
                        ..Default::default()
                    },
                )
                .unwrap();
        }
        for (key, bank_id) in [(&b, 2), (&a, 1)] {
            assembler
                .handle_update(
                    key.clone(),
                    SubscribeUpdate {
                        update_oneof: Some(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                            slot: 42,
                            bank_id,
                            blockhash: key.1.as_ref().unwrap().to_string(),
                            executed_transaction_count: 1,
                            ..Default::default()
                        })),
                        ..Default::default()
                    },
                )
                .unwrap();
        }
        let Some(UpdateOneof::Block(block)) =
            assembler.finish_slot(b).unwrap().unwrap().update_oneof
        else {
            panic!("block");
        };
        assert_eq!(block.bank_id, 2);
        assert_eq!(block.blockhash, "hash-b");
        assert_eq!(block.transactions.len(), 1);
        assert_eq!(block.transactions[0].index, 0);
        assert_eq!(block.transactions[0].signature, vec![2; 64]);
        assert_eq!(assembler.pending_slots(), 1);
        let Some(UpdateOneof::Block(block)) =
            assembler.finish_slot(a).unwrap().unwrap().update_oneof
        else {
            panic!("block");
        };
        assert_eq!(block.bank_id, 1);
        assert_eq!(block.transactions[0].index, 0);
        assert_eq!(assembler.estimated_buffered_bytes(), 0);
    }

    #[test]
    fn fumarole_rejects_ambiguous_legacy_banks_and_wrong_envelopes() {
        let mut assembler = FumaroleBlockAssembler::new(false, false);
        for bank_id in [1, 2] {
            let result = assembler.handle_update(
                (42, None),
                SubscribeUpdate {
                    update_oneof: Some(UpdateOneof::Entry(SubscribeUpdateEntry {
                        slot: 42,
                        bank_id,
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            );
            assert_eq!(result.is_ok(), bank_id == 1);
        }
        assert!(
            assembler
                .handle_update(
                    (42, Some(Arc::from("hash-a"))),
                    SubscribeUpdate {
                        update_oneof: Some(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                            slot: 42,
                            bank_id: 1,
                            blockhash: "hash-b".to_string(),
                            ..Default::default()
                        })),
                        ..Default::default()
                    }
                )
                .is_err()
        );
        assert!(
            assembler
                .handle_update(
                    (42, None),
                    SubscribeUpdate {
                        update_oneof: Some(UpdateOneof::Entry(SubscribeUpdateEntry {
                            slot: 43,
                            ..Default::default()
                        })),
                        ..Default::default()
                    }
                )
                .is_err()
        );
    }
}

#[cfg(test)]
mod completeness_tests;
