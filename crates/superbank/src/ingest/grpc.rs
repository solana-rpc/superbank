// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    pin::Pin,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, ensure};
use clickhouse::Client as ClickHouseClient;
use futures::StreamExt;
use prost::Message;
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
    CommitmentLevel, SlotStatus, SubscribeRequest, SubscribeRequestFilterBlockFooter,
    SubscribeRequestFilterBlocks, SubscribeRequestFilterSlots, SubscribeUpdate,
    SubscribeUpdateBlock, SubscribeUpdateBlockFooter, SubscribeUpdateEntry,
    SubscribeUpdateTransactionInfo, subscribe_update::UpdateOneof,
};

use crate::cli::{Args, FromSlotSpec, IngestSource};
use crate::clickhouse::{
    BlockMetadataRow, EntryRow, FooterFields, InsertTables, RetryConfig, TransactionRow,
    build_clickhouse_client, fetch_latest_slot_from_blocks, fetch_stored_footers, flush_buffers,
    flush_buffers_with_retry, merge_stored_footers,
};
use crate::commitment::parse_durable_commitment;
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
    // Replayed rows at or below this slot may already have stored footer fields.
    footer_merge_ceiling: Option<u64>,
}

impl BufferedRows {
    pub(crate) fn new(args: &Args) -> Self {
        Self {
            transaction_rows: Vec::with_capacity(args.transactions_flush_rows),
            block_rows: Vec::with_capacity(args.blocks_flush_rows),
            entry_rows: Vec::with_capacity(args.transactions_flush_rows),
            last_durable_block_slot: None,
            footer_merge_ceiling: None,
        }
    }

    pub(crate) fn with_footer_merge_ceiling(mut self, ceiling: Option<u64>) -> Self {
        self.footer_merge_ceiling = ceiling;
        self
    }

    // Replay carries no footers, so keep the fields a stored row already has.
    async fn merge_stored_footers(
        &mut self,
        clickhouse: &ClickHouseClient,
        insert_tables: &InsertTables,
    ) -> Result<()> {
        let Some(ceiling) = self.footer_merge_ceiling else {
            return Ok(());
        };
        let mut slots: Vec<u64> = self
            .block_rows
            .iter()
            .filter(|row| row.bank_hash.is_none() && row.slot <= ceiling)
            .map(|row| row.slot)
            .collect();
        slots.sort_unstable();
        slots.dedup();
        if slots.is_empty() {
            return Ok(());
        }
        let stored = fetch_stored_footers(clickhouse, &insert_tables.blocks_table, &slots).await?;
        let merged = merge_stored_footers(&mut self.block_rows, &stored);
        if merged > 0 {
            info!(merged, "kept stored footer fields on replayed blocks");
        }
        Ok(())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.transaction_rows.is_empty() && self.block_rows.is_empty() && self.entry_rows.is_empty()
    }

    pub(crate) async fn flush(
        &mut self,
        clickhouse: &ClickHouseClient,
        insert_tables: &InsertTables,
    ) -> Result<()> {
        self.merge_stored_footers(clickhouse, insert_tables).await?;
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
        self.merge_stored_footers(clickhouse, insert_tables).await?;
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

const FOOTER_JOIN_WINDOW_SLOTS: u64 = 8192;
const FOOTER_WAIT: Duration = Duration::from_secs(2);
const FOOTER_AWAIT_MAX_BLOCKS: usize = 64;
const PENDING_IDENTITY_MAX_BLOCKS: usize = 256;
const PENDING_IDENTITY_MAX_BYTES: usize = 128 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CanonicalIdentity {
    Unknown,
    Legacy,
    Bank(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FooterRelease {
    Due,
    Drain,
    Skip,
}

struct PreparedBlock {
    metadata: BlockMetadataRow,
    transactions: Vec<TransactionRow>,
    entries: Vec<EntryRow>,
    encoded_bytes: usize,
}

impl PreparedBlock {
    fn append(
        self,
        transaction_rows: &mut Vec<TransactionRow>,
        block_rows: &mut Vec<BlockMetadataRow>,
        entry_rows: Option<&mut Vec<EntryRow>>,
    ) {
        transaction_rows.extend(self.transactions);
        block_rows.push(self.metadata);
        if let Some(entry_rows) = entry_rows {
            entry_rows.extend(self.entries);
        }
    }
}

struct AwaitingFooter {
    prepared: PreparedBlock,
    bank_id: Option<u64>,
    deadline: Instant,
}

#[derive(Default)]
struct FinalizedFooterJoin {
    // How long an identified block waits for its footer before it is written without one.
    footer_wait: Duration,
    pending: HashMap<(u64, u64), FooterFields>,
    finalized: HashMap<u64, u64>,
    footer_seen: bool,
    min_footer_slot: Option<u64>,
    highest_slot: u64,
    // These proofs and pending payloads belong only to this subscription.
    known_banks: HashSet<(u64, u64)>,
    legacy_slots: HashSet<u64>,
    pending_identity: BTreeMap<u64, PreparedBlock>,
    pending_identity_bytes: usize,
    // Identified blocks wait here in slot order for their footer.
    awaiting: BTreeMap<(u64, u64), AwaitingFooter>,
    awaiting_bytes: usize,
    awaiting_seq: u64,
}

impl FinalizedFooterJoin {
    fn new() -> Self {
        Self {
            footer_wait: FOOTER_WAIT,
            ..Default::default()
        }
    }

    fn block_identity(&self, slot: u64, scalar_id: u64) -> Result<CanonicalIdentity> {
        if self.legacy_slots.contains(&slot) {
            ensure!(
                scalar_id == 0,
                "bank-aware block contradicts legacy status at slot {slot}"
            );
            return Ok(CanonicalIdentity::Legacy);
        }
        // Nonzero scalars cannot be protobuf's missing-field default. Zero needs
        // optional-ID evidence for this exact slot on this same subscription.
        if scalar_id != 0 || self.known_banks.contains(&(slot, scalar_id)) {
            return Ok(CanonicalIdentity::Bank(scalar_id));
        }
        Ok(CanonicalIdentity::Unknown)
    }

    fn stage_identity_block(
        &mut self,
        mut prepared: PreparedBlock,
        encoded_bytes: usize,
    ) -> Result<()> {
        let slot = prepared.metadata.slot;
        ensure!(
            !self.pending_identity.contains_key(&slot),
            "duplicate unidentified block at slot {slot}"
        );
        let bytes = self
            .pending_identity_bytes
            .checked_add(encoded_bytes)
            .context("pending gRPC identity bytes overflow")?;
        ensure!(
            self.pending_identity.len() < PENDING_IDENTITY_MAX_BLOCKS
                && bytes <= PENDING_IDENTITY_MAX_BYTES,
            "gRPC blocks lack same-subscription bank identity; pending identity limit exceeded"
        );
        self.highest_slot = self.highest_slot.max(slot);
        prepared.encoded_bytes = encoded_bytes;
        self.pending_identity.insert(slot, prepared);
        self.pending_identity_bytes = bytes;
        self.prune()
    }

    fn take_ready_blocks(&mut self) -> Result<Vec<(PreparedBlock, CanonicalIdentity)>> {
        let mut identities = Vec::with_capacity(self.pending_identity.len());
        for (&slot, pending) in &self.pending_identity {
            let identity = self.block_identity(slot, pending.metadata.bank_id.unwrap_or(0))?;
            if identity == CanonicalIdentity::Unknown {
                return Ok(Vec::new());
            }
            identities.push(identity);
        }
        let pending = std::mem::take(&mut self.pending_identity);
        self.pending_identity_bytes = 0;
        Ok(pending.into_values().zip(identities).collect())
    }

    fn observe(&mut self, update: SubscribeUpdate) -> Result<()> {
        match update.update_oneof {
            Some(UpdateOneof::BlockFooter(footer)) => self.observe_footer(&footer)?,
            Some(UpdateOneof::Slot(slot)) => self.observe_slot(&slot)?,
            _ => {}
        }
        self.prune()
    }

    fn observe_footer(&mut self, footer: &SubscribeUpdateBlockFooter) -> Result<()> {
        ensure!(
            !self.legacy_slots.contains(&footer.slot),
            "footer at slot {} lacks proven bank identity",
            footer.slot
        );
        let fields = map_block_footer(footer)?;
        self.footer_seen = true;
        self.min_footer_slot = Some(
            self.min_footer_slot
                .map_or(footer.slot, |min| min.min(footer.slot)),
        );
        self.highest_slot = self.highest_slot.max(footer.slot);
        if self
            .finalized
            .get(&footer.slot)
            .is_some_and(|winner| *winner != footer.bank_id)
        {
            return Ok(());
        }
        self.pending.insert((footer.slot, footer.bank_id), fields);
        Ok(())
    }

    fn observe_slot(
        &mut self,
        slot: &yellowstone_grpc_proto::prelude::SubscribeUpdateSlot,
    ) -> Result<()> {
        let Ok(status) = SlotStatus::try_from(slot.status) else {
            return Ok(());
        };
        if matches!(
            status,
            SlotStatus::SlotCreatedBank
                | SlotStatus::SlotProcessed
                | SlotStatus::SlotConfirmed
                | SlotStatus::SlotFinalized
        ) {
            self.highest_slot = self.highest_slot.max(slot.slot);
            if let Some(bank_id) = slot.bank_id {
                ensure!(
                    !self.legacy_slots.contains(&slot.slot),
                    "bank identity protocol changed within slot {}",
                    slot.slot
                );
                self.known_banks.insert((slot.slot, bank_id));
            } else if matches!(
                status,
                SlotStatus::SlotCreatedBank | SlotStatus::SlotFinalized
            ) {
                ensure!(
                    !self
                        .known_banks
                        .iter()
                        .any(|(candidate, _)| *candidate == slot.slot),
                    "bank-aware slot {} lost optional bank identity",
                    slot.slot
                );
                self.legacy_slots.insert(slot.slot);
            }
        }
        match status {
            SlotStatus::SlotFinalized => {
                let Some(bank_id) = slot.bank_id else {
                    return Ok(());
                };
                ensure!(
                    self.finalized
                        .get(&slot.slot)
                        .is_none_or(|winner| *winner == bank_id),
                    "conflicting finalized banks at slot {}",
                    slot.slot
                );
                self.finalized.insert(slot.slot, bank_id);
                self.pending
                    .retain(|(candidate, bank), _| *candidate != slot.slot || *bank == bank_id);
            }
            SlotStatus::SlotDead => {
                self.pending.retain(|(candidate, bank), _| {
                    *candidate != slot.slot || slot.bank_id.is_some_and(|id| *bank != id)
                });
            }
            _ => {}
        }
        Ok(())
    }

    // Footers are not replayed, so slots below this subscription's first footer never get one.
    fn footer_expected(&self, slot: u64) -> bool {
        !(self.footer_seen && self.min_footer_slot.is_some_and(|min| slot < min))
    }

    fn await_footer(
        &mut self,
        mut prepared: PreparedBlock,
        identity: CanonicalIdentity,
        now: Instant,
    ) -> Result<()> {
        let bank_id = match identity {
            CanonicalIdentity::Bank(bank_id) => Some(bank_id),
            CanonicalIdentity::Legacy => None,
            CanonicalIdentity::Unknown => {
                return Err(anyhow!("cannot persist unidentified gRPC bank"));
            }
        };
        let slot = prepared.metadata.slot;
        ensure!(
            bank_id.is_none_or(|id| self.finalized.get(&slot).is_none_or(|winner| *winner == id)),
            "finalized block conflicts with winning bank at slot {slot}"
        );
        prepared.metadata.bank_id = bank_id;
        self.awaiting_bytes = self.awaiting_bytes.saturating_add(prepared.encoded_bytes);
        self.awaiting_seq += 1;
        self.awaiting.insert(
            (slot, self.awaiting_seq),
            AwaitingFooter {
                prepared,
                bank_id,
                deadline: now + self.footer_wait,
            },
        );
        Ok(())
    }

    // Releases blocks in slot order once their footer joins or the wait ends.
    fn release_awaiting(
        &mut self,
        now: Instant,
        mode: FooterRelease,
        include_entries: bool,
        rows: &mut BufferedRows,
    ) {
        if mode == FooterRelease::Skip {
            return;
        }
        while let Some((&(slot, _), entry)) = self.awaiting.first_key_value() {
            let bank_id = entry.bank_id;
            let usable = bank_id.is_some_and(|id| {
                self.finalized.get(&slot) == Some(&id) && self.pending.contains_key(&(slot, id))
            });
            let overflow = self.awaiting.len() > FOOTER_AWAIT_MAX_BLOCKS
                || self.awaiting_bytes > PENDING_IDENTITY_MAX_BYTES;
            let due = mode == FooterRelease::Drain
                || overflow
                || bank_id.is_none()
                || now >= entry.deadline
                || !self.footer_expected(slot);
            if !usable && !due {
                break;
            }
            let Some((_, mut entry)) = self.awaiting.pop_first() else {
                break;
            };
            self.awaiting_bytes = self
                .awaiting_bytes
                .saturating_sub(entry.prepared.encoded_bytes);
            let footer = bank_id
                .filter(|_| usable)
                .and_then(|id| self.pending.remove(&(slot, id)));
            match footer {
                Some(footer) => footer.apply(&mut entry.prepared.metadata),
                None if mode == FooterRelease::Due
                    && bank_id.is_some()
                    && self.footer_seen
                    && self.footer_expected(slot) =>
                {
                    metrics::observe_source_error("grpc_footer", "missing");
                    warn!(
                        slot,
                        "footer unavailable for finalized block; footer columns stay NULL"
                    );
                }
                None => {}
            }
            entry.prepared.append(
                &mut rows.transaction_rows,
                &mut rows.block_rows,
                include_entries.then_some(&mut rows.entry_rows),
            );
        }
    }

    fn prune(&mut self) -> Result<()> {
        let oldest = self.highest_slot.saturating_sub(FOOTER_JOIN_WINDOW_SLOTS);
        self.pending.retain(|(slot, _), _| *slot >= oldest);
        self.finalized.retain(|slot, _| *slot >= oldest);
        ensure!(
            !self.pending_identity.keys().any(|slot| *slot < oldest),
            "finalized block lacks same-subscription bank identity within {FOOTER_JOIN_WINDOW_SLOTS} slots"
        );
        self.known_banks.retain(|(slot, _)| *slot >= oldest);
        self.legacy_slots.retain(|slot| *slot >= oldest);
        Ok(())
    }
}

pub(crate) async fn run_grpc_ingest(args: &Args) -> Result<()> {
    let endpoint = args
        .endpoint
        .as_ref()
        .context("grpc source requires --endpoint / DRAGONSMOUTH_ENDPOINT / config endpoint")?;
    let commitment = parse_durable_commitment(&args.commitment)? as i32;
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

    let mut buffered_rows = BufferedRows::new(args).with_footer_merge_ceiling(
        fetch_latest_slot_from_blocks(&clickhouse, &args.blocks_table).await?,
    );
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
    let mut _health_watch_guard = AbortTaskGuard::default();
    let _health_failure_guard = if args.grpc_health_watch_enabled {
        _health_watch_guard.set(start_grpc_health_watch(endpoint, args, health_failure_tx).await?);
        None
    } else {
        Some(health_failure_tx)
    };

    let subscribe_from_slot =
        next_subscribe_from_slot(initial_from_slot, buffered_rows.last_durable_block_slot)?;
    let subscribe_from_slot_mode = next_subscribe_from_slot_mode(
        initial_from_slot_mode,
        buffered_rows.last_durable_block_slot,
    );
    let mut footer_join = FinalizedFooterJoin::new();
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
        process_canonical_update(
            update,
            args,
            &insert_tables,
            &clickhouse,
            &mut buffered_rows,
            &retry_config,
            &mut footer_join,
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
                flush_canonical_rows(&clickhouse, &insert_tables, &mut buffered_rows, Some(&retry_config), &mut footer_join, FooterRelease::Due).await?;
            }
            health_failure = health_failure_rx.recv() => {
                let reason = health_failure
                    .unwrap_or_else(|| "gRPC health watch task stopped unexpectedly".to_string());
                metrics::observe_source_error(grpc_auxiliary_source(&reason), "unhealthy");
                warn!(
                    reason = %reason,
                    last_processed_block_slot,
                    last_durable_block_slot = buffered_rows.last_durable_block_slot,
                    "fatal gRPC auxiliary stream condition; flushing pending rows before exit"
                );
                flush_after_fatal_condition(
                    &clickhouse,
                    &insert_tables,
                    &mut buffered_rows,
                    &reason,
                    &mut footer_join,
                    FooterRelease::Drain,
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
                    &mut footer_join,
                    FooterRelease::Drain,
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
                        if let Some(UpdateOneof::Slot(slot_update)) = &update.update_oneof
                            && Some(slot_update.status) == commitment_slot_status(commitment)
                        {
                            metrics::set_network_tip_slot(slot_update.slot);
                        }
                        process_canonical_update(
                            update,
                            args,
                            &insert_tables,
                            &clickhouse,
                            &mut buffered_rows,
                            &retry_config,
            &mut footer_join,
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
                            &mut footer_join,
                            FooterRelease::Drain,
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
                            &mut footer_join,
                            FooterRelease::Drain,
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
        result = flush_canonical_rows(&clickhouse, &insert_tables, &mut buffered_rows, Some(&retry_config), &mut footer_join, FooterRelease::Drain) => {
            result?;
            ensure!(footer_join.pending_identity.is_empty(),
                "gRPC shutdown with unidentified banks; replay from the last durable slot");
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

fn commitment_slot_status(commitment: i32) -> Option<i32> {
    match CommitmentLevel::try_from(commitment).ok()? {
        CommitmentLevel::Processed => Some(SlotStatus::SlotProcessed as i32),
        CommitmentLevel::Confirmed => Some(SlotStatus::SlotConfirmed as i32),
        CommitmentLevel::Finalized => Some(SlotStatus::SlotFinalized as i32),
    }
}

fn grpc_auxiliary_source(reason: &str) -> &'static str {
    if reason.starts_with("footer stream:") {
        "grpc_footer_stream"
    } else {
        "grpc_health_watch"
    }
}

async fn process_canonical_update(
    update: SubscribeUpdate,
    args: &Args,
    tables: &InsertTables,
    clickhouse: &ClickHouseClient,
    rows: &mut BufferedRows,
    retry: &RetryConfig,
    join: &mut FinalizedFooterJoin,
) -> Result<()> {
    let affected_slot = match update.update_oneof.as_ref() {
        Some(UpdateOneof::Block(block)) => Some(block.slot),
        Some(UpdateOneof::Slot(slot)) => Some(slot.slot),
        Some(UpdateOneof::BlockFooter(footer)) => Some(footer.slot),
        _ => None,
    };
    let result =
        process_canonical_update_inner(update, args, tables, clickhouse, rows, retry, join).await;
    if let Err(error) = &result {
        // A rejected later update must not repeatedly starve a complete prefix.
        // Same-slot contradictions cannot qualify that slot's buffered data,
        // and unresolved identities retain the existing all-data progress hold.
        if affected_slot.is_some_and(|slot| rows.block_rows.iter().all(|row| row.slot < slot)) {
            warn!(%error, affected_slot, "canonical update rejected; flushing only complete earlier qualified data");
            flush_after_fatal_condition(
                clickhouse,
                tables,
                rows,
                &error.to_string(),
                join,
                FooterRelease::Skip,
            )
            .await?;
        }
    }
    result
}

async fn process_canonical_update_inner(
    update: SubscribeUpdate,
    args: &Args,
    tables: &InsertTables,
    clickhouse: &ClickHouseClient,
    rows: &mut BufferedRows,
    retry: &RetryConfig,
    join: &mut FinalizedFooterJoin,
) -> Result<()> {
    match update.update_oneof {
        Some(UpdateOneof::Block(block)) => {
            validate_block_bank(
                &block,
                args.source,
                args.fumarole_alpenglow_genesis_slot
                    .or(args.fumarole_preactivation_through_slot),
            )?;
            ensure!(
                join.finalized
                    .get(&block.slot)
                    .is_none_or(|winner| *winner == block.bank_id),
                "finalized block conflicts with winning bank at slot {}",
                block.slot
            );
            join.block_identity(block.slot, block.bank_id)?;
            // Decode and validate before touching pending identity or writer buffers.
            let prepared = prepare_block(&block, args.entries_table.is_some())?;
            join.stage_identity_block(prepared, block.encoded_len())?;
        }
        Some(UpdateOneof::BlockFooter(footer)) => {
            if let Err(error) = join.observe_footer(&footer) {
                metrics::observe_source_error("grpc_footer", "invalid");
                warn!(slot = footer.slot, %error, "discarding unqualified footer; canonical identity checks remain required");
            }
            join.prune()?;
        }
        other => {
            join.observe(SubscribeUpdate {
                update_oneof: other,
                ..update
            })?;
        }
    }
    // Later complete blocks remain held behind any unidentified earlier block.
    // Nothing can advance the metadata tip until every held identity is resolved.
    let now = Instant::now();
    for (prepared, identity) in join.take_ready_blocks()? {
        join.await_footer(prepared, identity, now)?;
    }
    join.release_awaiting(now, FooterRelease::Due, args.entries_table.is_some(), rows);
    if join.pending_identity.is_empty() {
        let pressure = args.flush_every_block
            || rows.transaction_rows.len() >= args.transactions_flush_rows
            || rows.block_rows.len() >= args.blocks_flush_rows
            || rows.entry_rows.len() >= args.transactions_flush_rows;
        if pressure {
            flush_canonical_rows(
                clickhouse,
                tables,
                rows,
                Some(retry),
                join,
                FooterRelease::Skip,
            )
            .await?;
        }
    }
    Ok(())
}

async fn flush_canonical_rows(
    clickhouse: &ClickHouseClient,
    tables: &InsertTables,
    rows: &mut BufferedRows,
    retry: Option<&RetryConfig>,
    join: &mut FinalizedFooterJoin,
    release: FooterRelease,
) -> Result<bool> {
    join.release_awaiting(
        Instant::now(),
        release,
        tables.entries_table.is_some(),
        rows,
    );
    if !join.pending_identity.is_empty() {
        warn!(
            first_pending_slot = join
                .pending_identity
                .first_key_value()
                .map(|(slot, _)| *slot),
            pending_blocks = join.pending_identity.len(),
            "gRPC bank identity unresolved; retaining buffered data for replay"
        );
        return Ok(false);
    }
    match retry {
        Some(retry) => rows.flush_with_retry(clickhouse, tables, retry).await?,
        None => rows.flush(clickhouse, tables).await?,
    }
    Ok(true)
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

fn build_footer_request(from_slot: Option<u64>) -> SubscribeRequest {
    let mut block_footer = HashMap::new();
    block_footer.insert(
        "block_footer".to_string(),
        SubscribeRequestFilterBlockFooter {
            include_certificates: Some(false),
        },
    );
    let mut slots = HashMap::new();
    slots.insert(
        "footer_finality".to_string(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: Some(false),
            interslot_updates: Some(true),
        },
    );
    SubscribeRequest {
        block_footer,
        slots,
        commitment: Some(0),
        from_slot,
        ..Default::default()
    }
}

async fn resolve_initial_from_slot(
    args: &Args,
    clickhouse: &ClickHouseClient,
) -> Result<(Option<u64>, Option<FromSlotMode>)> {
    match args.dragonsmouth_from_slot {
        Some(FromSlotSpec::LatestDb) => {
            match fetch_latest_slot_from_blocks(clickhouse, &args.blocks_table).await? {
                Some(latest) => {
                    info!(
                        slot = latest,
                        table = %args.blocks_table,
                        "resolved dragonsmouth-from-slot='*' to latest slot in blocks_metadata"
                    );
                    Ok((Some(latest), Some(FromSlotMode::LatestDb)))
                }
                None => {
                    info!(
                        table = %args.blocks_table,
                        "dragonsmouth-from-slot='*' found no rows in {}; falling back to gRPC-reported available slot",
                        args.blocks_table
                    );
                    Ok((None, Some(FromSlotMode::LatestDb)))
                }
            }
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
            cuckoo_account_include: None,
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

    // Bank counters are local to this subscription: footer, status and data share it.
    let footer = build_footer_request(from_slot);
    slots.extend(footer.slots);
    SubscribeRequest {
        block_footer: footer.block_footer,
        blocks,
        slots,
        commitment: Some(commitment),
        from_slot,
        ..Default::default()
    }
}

async fn build_grpc_client(endpoint: &str, args: &Args) -> Result<GeyserGrpcClient> {
    let builder = GeyserGrpcClient::build_from_shared(endpoint.as_bytes().to_vec())?
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
    join: &mut FinalizedFooterJoin,
    release: FooterRelease,
) -> Result<()> {
    flush_canonical_rows(
        clickhouse,
        insert_tables,
        buffered_rows,
        None,
        join,
        release,
    )
    .await
    .map(|_| ())
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
    match update.update_oneof {
        Some(UpdateOneof::Block(block)) => {
            process_block_update(block, args, insert_tables, clickhouse, buffered_rows, retry).await
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
            Ok(false)
        }
        Some(UpdateOneof::BlockFooter(_)) => Ok(false),
        Some(UpdateOneof::Ping(_)) | Some(UpdateOneof::Pong(_)) => Ok(false),
        _ => Ok(false),
    }
}

async fn process_block_update(
    block: SubscribeUpdateBlock,
    args: &Args,
    insert_tables: &InsertTables,
    clickhouse: &ClickHouseClient,
    buffered_rows: &mut BufferedRows,
    retry: Option<&RetryConfig>,
) -> Result<bool> {
    validate_block_bank(
        &block,
        args.source,
        args.fumarole_alpenglow_genesis_slot
            .or(args.fumarole_preactivation_through_slot),
    )?;
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
    flush_block_if_needed(args, insert_tables, clickhouse, buffered_rows, retry).await
}

fn validate_block_bank(
    block: &SubscribeUpdateBlock,
    source: IngestSource,
    fumarole_alpenglow_genesis_slot: Option<u64>,
) -> Result<()> {
    if source == IngestSource::Fumarole {
        let genesis_slot = fumarole_alpenglow_genesis_slot
            .context("Fumarole requires an evidenced historical slot bound")?;
        if block.slot > genesis_slot {
            return Err(anyhow!(
                "legacy Fumarole stream exceeded its trusted historical bound at slot {}; qualify a new bound or use the bank-tagged gRPC source",
                block.slot
            ));
        }
    }
    if source == IngestSource::Grpc
        && block
            .entries
            .iter()
            .any(|entry| entry.bank_id != block.bank_id)
    {
        return Err(anyhow!(
            "gRPC block at slot {} contains entries from another bank",
            block.slot
        ));
    }
    Ok(())
}

async fn flush_block_if_needed(
    args: &Args,
    insert_tables: &InsertTables,
    clickhouse: &ClickHouseClient,
    buffered_rows: &mut BufferedRows,
    retry: Option<&RetryConfig>,
) -> Result<bool> {
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
        return Ok(true);
    }
    Ok(false)
}

fn map_block_footer(footer: &SubscribeUpdateBlockFooter) -> Result<FooterFields> {
    Ok(FooterFields {
        bank_hash: bytes_to_array::<32>(&footer.bank_hash)
            .context("decode footer bank hash")?
            .0,
        block_producer_time_nanos: footer.block_producer_time_nanos,
        block_user_agent: footer.block_user_agent.clone(),
    })
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
    prepare_block(&block, entry_rows.is_some())?.append(transaction_rows, block_rows, entry_rows);
    Ok(())
}

fn prepare_block(block: &SubscribeUpdateBlock, include_entries: bool) -> Result<PreparedBlock> {
    validate_block_completeness(block, include_entries)?;
    let block_time = block.block_time.as_ref().map(|bt| bt.timestamp);
    Ok(PreparedBlock {
        metadata: map_block_metadata(block)?,
        transactions: map_transactions(block.slot, block_time, &block.transactions)?,
        entries: if include_entries {
            map_entries(block.slot, block_time, &block.entries)?
        } else {
            Vec::new()
        },
        encoded_bytes: 0,
    })
}

/// Both subscriptions request all transactions. Entries are optional, so metadata
/// counts for an omitted entry payload cannot be used to reject a block.
pub(crate) fn validate_block_completeness(
    block: &SubscribeUpdateBlock,
    include_entries: bool,
) -> Result<()> {
    let slot = block.slot;
    ensure!(
        block.executed_transaction_count == block.transactions.len() as u64,
        "block {slot} transaction count mismatch: expected {}, received {}",
        block.executed_transaction_count,
        block.transactions.len()
    );

    // Unique indices in [0, count) plus count equality prove full coverage, even
    // when updates arrive out of order. Allocate from received data, not metadata.
    let mut transaction_indices = vec![false; block.transactions.len()];
    let mut signatures = HashSet::with_capacity(block.transactions.len());
    for transaction in &block.transactions {
        let index = usize::try_from(transaction.index)
            .ok()
            .and_then(|index| transaction_indices.get_mut(index))
            .with_context(|| {
                format!(
                    "block {slot} transaction index {} outside expected coverage",
                    transaction.index
                )
            })?;
        ensure!(
            !*index,
            "block {slot} duplicate transaction index {}",
            transaction.index
        );
        *index = true;
        ensure!(
            signatures.insert(transaction.signature.as_slice()),
            "block {slot} duplicate transaction signature at index {}",
            transaction.index
        );
    }

    if !include_entries {
        return Ok(());
    }
    ensure!(
        block.entries_count == block.entries.len() as u64,
        "block {slot} entry count mismatch: expected {}, received {}",
        block.entries_count,
        block.entries.len()
    );
    let mut entries_by_index = vec![None; block.entries.len()];
    for entry in &block.entries {
        ensure!(
            entry.slot == slot,
            "block {slot} entry slot mismatch: received {} at index {}",
            entry.slot,
            entry.index
        );
        let indexed_entry = usize::try_from(entry.index)
            .ok()
            .and_then(|index| entries_by_index.get_mut(index))
            .with_context(|| {
                format!(
                    "block {slot} entry index {} outside expected coverage",
                    entry.index
                )
            })?;
        ensure!(
            indexed_entry.is_none(),
            "block {slot} duplicate entry index {}",
            entry.index
        );
        *indexed_entry = Some(entry);
    }

    // Entry transaction ranges must partition the same transaction indices.
    // No tick count or num_hashes assumption: this works for both TowerBFT and
    // Alpenglow's terminal Alpentick.
    let mut next_transaction_index = 0u64;
    for entry in entries_by_index.into_iter().flatten() {
        ensure!(
            entry.starting_transaction_index == next_transaction_index,
            "block {slot} entry {} transaction coverage mismatch: expected start {next_transaction_index}, received {}",
            entry.index,
            entry.starting_transaction_index
        );
        next_transaction_index = next_transaction_index
            .checked_add(entry.executed_transaction_count)
            .with_context(|| format!("block {slot} entry transaction count overflow"))?;
        ensure!(
            next_transaction_index <= block.executed_transaction_count,
            "block {slot} entry {} transaction range exceeds block transaction count",
            entry.index
        );
    }
    ensure!(
        next_transaction_index == block.executed_transaction_count,
        "block {slot} entries cover {next_transaction_index} transactions, expected {}",
        block.executed_transaction_count
    );
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
    let mut rewards_commission_bps = Vec::new();
    let mut rewards_num_partitions = None;

    if let Some(rewards) = block.rewards.as_ref() {
        rewards_present = if rewards.rewards.is_empty() { 0 } else { 1 };
        for reward in &rewards.rewards {
            rewards_pubkey.push(decode_base58_32(&reward.pubkey).context("decode reward pubkey")?);
            rewards_lamports.push(reward.lamports);
            rewards_post_balance.push(reward.post_balance);
            rewards_type.push(reward_type_to_string(reward.reward_type));
            rewards_commission.push(parse_commission(&reward.commission));
            rewards_commission_bps.push(parse_commission_bps(&reward.commission_bps));
        }
        rewards_num_partitions = rewards.num_partitions.as_ref().map(|p| p.num_partitions);
    }

    Ok(BlockMetadataRow {
        slot: block.slot,
        parent_slot: block.parent_slot,
        blockhash,
        parent_blockhash,
        bank_id: (block.bank_id != 0).then_some(block.bank_id),
        bank_hash: None,
        block_producer_time_nanos: None,
        block_user_agent: None,
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
        rewards_commission_bps,
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
    if message.config.is_some() && !message.address_table_lookups.is_empty() {
        return Err(anyhow!("v1 transaction contains address table lookups"));
    }

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
        meta_reward_commission_bps,
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
        tx_version: if message.config.is_some() {
            Some(1)
        } else if message.versioned {
            Some(0)
        } else {
            None
        },
        tx_config_priority_fee: message
            .config
            .as_ref()
            .and_then(|config| config.priority_fee),
        tx_config_compute_unit_limit: message
            .config
            .as_ref()
            .and_then(|config| config.compute_unit_limit),
        tx_config_loaded_accounts_data_size_limit: message
            .config
            .as_ref()
            .and_then(|config| config.loaded_accounts_data_size_limit),
        tx_config_heap_size: message.config.as_ref().and_then(|config| config.heap_size),
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
        meta_reward_commission_bps,
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
        v1::{Message as MessageV1, TransactionConfig},
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

    if let Some(config) = &message.config {
        if !message.address_table_lookups.is_empty() {
            return Err(anyhow!("v1 transaction contains address table lookups"));
        }
        Ok(VersionedMessage::V1(MessageV1 {
            header,
            config: TransactionConfig {
                priority_fee: config.priority_fee,
                compute_unit_limit: config.compute_unit_limit,
                loaded_accounts_data_size_limit: config.loaded_accounts_data_size_limit,
                heap_size: config.heap_size,
            },
            lifetime_specifier: recent_blockhash,
            account_keys,
            instructions,
        }))
    } else if message.versioned {
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
    Vec<Option<u16>>,
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
    let mut commission_bps = Vec::with_capacity(rewards.len());

    for reward in rewards {
        pubkeys.push(reward.pubkey.clone());
        lamports.push(reward.lamports);
        post_balances.push(reward.post_balance);
        reward_types.push(reward_type_to_string(reward.reward_type));
        commissions.push(parse_commission(&reward.commission));
        commission_bps.push(parse_commission_bps(&reward.commission_bps));
    }

    (
        pubkeys,
        lamports,
        post_balances,
        reward_types,
        commissions,
        commission_bps,
    )
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
    use yellowstone_grpc_proto::prelude::RewardType;
    let reward_type = RewardType::try_from(value).ok()?;
    (reward_type != RewardType::Unspecified).then(|| reward_type.as_str_name().to_owned())
}

fn parse_commission(value: &str) -> Option<u8> {
    if value.is_empty() {
        return None;
    }
    value.parse::<u8>().ok()
}

fn parse_commission_bps(value: &str) -> Option<u16> {
    if value.is_empty() {
        return None;
    }
    value.parse::<u16>().ok()
}

fn decode_transaction_error(
    err: Option<&yellowstone_grpc_proto::prelude::TransactionError>,
) -> Result<(u8, Option<String>)> {
    let Some(err) = err else {
        return Ok((1, None));
    };

    match wincode::deserialize::<solana_transaction_error::TransactionError>(&err.err) {
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
        BlockMetadataRow, BufferedRows, CanonicalIdentity, FinalizedFooterJoin, FooterRelease,
        FromSlotMode, PreparedBlock, build_footer_request, build_subscribe_request,
        grpc_health_status_is_serving, grpc_health_status_label, grpc_status_summary,
        is_oversized_grpc_message, map_block_footer, map_transaction, max_block_slot,
        next_subscribe_from_slot, next_subscribe_from_slot_mode, parse_commission_bps,
        validate_block_bank,
    };
    use crate::cli::IngestSource;
    use serde_big_array::Array;
    use tokio::time::Instant;

    #[test]
    fn generated_reward_types_preserve_storage_names_and_reject_unknown_values() {
        for (wire, expected) in [
            (1, "Fee"),
            (2, "Rent"),
            (3, "Staking"),
            (4, "Voting"),
            (5, "DeactivatedStake"),
            (6, "VATDebit"),
        ] {
            assert_eq!(
                super::reward_type_to_string(wire).as_deref(),
                Some(expected)
            );
        }
        for wire in [i32::MIN, -1, 0, 7, i32::MAX] {
            assert_eq!(super::reward_type_to_string(wire), None);
        }
    }

    #[test]
    fn parses_reward_commission_bps() {
        assert_eq!(parse_commission_bps("300"), Some(300));
        assert_eq!(parse_commission_bps("10000"), Some(10_000));
        assert_eq!(parse_commission_bps(""), None);
        assert_eq!(parse_commission_bps("invalid"), None);
        assert_eq!(parse_commission_bps("65536"), None);
    }
    use tonic::Status;
    use yellowstone_grpc_proto::prelude::{
        CompiledInstruction, Message, MessageAddressTableLookup, MessageHeader, Reward, RewardType,
        SlotStatus, SubscribeUpdate, SubscribeUpdateBlock, SubscribeUpdateBlockFooter,
        SubscribeUpdateSlot, SubscribeUpdateTransactionInfo, Transaction, TransactionConfig,
        TransactionStatusMeta, subscribe_update::UpdateOneof,
    };

    #[test]
    fn footer_subscription_is_processed_and_receives_all_bank_statuses() {
        let footer = build_footer_request(Some(42));
        assert!(footer.block_footer.contains_key("block_footer"));
        assert_eq!(footer.commitment, Some(0));
        assert_eq!(footer.from_slot, Some(42));
        assert_eq!(
            footer.slots["footer_finality"].filter_by_commitment,
            Some(false)
        );
        assert_eq!(
            footer.slots["footer_finality"].interslot_updates,
            Some(true)
        );

        let finalized = build_subscribe_request(2, Some(42), true, true);
        assert!(finalized.block_footer.contains_key("block_footer"));
        assert!(finalized.slots.contains_key("footer_finality"));
        assert_eq!(finalized.commitment, Some(2));
    }

    fn footer_update(slot: u64, bank_id: u64) -> SubscribeUpdate {
        SubscribeUpdate {
            update_oneof: Some(UpdateOneof::BlockFooter(SubscribeUpdateBlockFooter {
                slot,
                bank_id,
                bank_hash: vec![bank_id as u8; 32],
                block_producer_time_nanos: 123,
                block_user_agent: b"agave".to_vec(),
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    fn slot_update(slot: u64, bank_id: u64, status: SlotStatus) -> SubscribeUpdate {
        SubscribeUpdate {
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot,
                bank_id: Some(bank_id),
                status: status as i32,
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    fn rows() -> BufferedRows {
        BufferedRows {
            transaction_rows: Vec::new(),
            block_rows: Vec::new(),
            entry_rows: Vec::new(),
            last_durable_block_slot: None,
            footer_merge_ceiling: None,
        }
    }

    fn prepared(slot: u64) -> PreparedBlock {
        PreparedBlock {
            metadata: build_block_metadata_row(slot),
            transactions: Vec::new(),
            entries: Vec::new(),
            encoded_bytes: 10,
        }
    }

    fn row_slots(rows: &BufferedRows) -> Vec<u64> {
        rows.block_rows.iter().map(|row| row.slot).collect()
    }

    fn row_hash(rows: &BufferedRows, slot: u64) -> Option<[u8; 32]> {
        let row = rows.block_rows.iter().find(|row| row.slot == slot).unwrap();
        row.bank_hash.as_ref().map(|hash| hash.0)
    }

    #[test]
    fn footer_merges_into_the_block_row_in_either_arrival_order() {
        let now = Instant::now();
        let mut join = FinalizedFooterJoin::new();
        let mut rows = rows();
        join.observe(footer_update(42, 8)).unwrap();
        join.observe(slot_update(42, 8, SlotStatus::SlotFinalized))
            .unwrap();
        join.await_footer(prepared(42), CanonicalIdentity::Bank(8), now)
            .unwrap();
        join.release_awaiting(now, FooterRelease::Due, true, &mut rows);
        assert_eq!(row_hash(&rows, 42), Some([8; 32]));
        let row = &rows.block_rows[0];
        assert_eq!(row.block_producer_time_nanos, Some(123));
        assert_eq!(row.block_user_agent.as_deref(), Some(&b"agave".to_vec()));
        assert_eq!(row.bank_id, Some(8));

        join.await_footer(prepared(43), CanonicalIdentity::Bank(9), now)
            .unwrap();
        join.release_awaiting(now, FooterRelease::Due, true, &mut rows);
        assert_eq!(row_slots(&rows), vec![42]);
        join.observe(slot_update(43, 9, SlotStatus::SlotFinalized))
            .unwrap();
        join.release_awaiting(now, FooterRelease::Due, true, &mut rows);
        assert_eq!(row_slots(&rows), vec![42]);
        join.observe(footer_update(43, 9)).unwrap();
        join.release_awaiting(now, FooterRelease::Due, true, &mut rows);
        assert_eq!(row_slots(&rows), vec![42, 43]);
        assert_eq!(row_hash(&rows, 43), Some([9; 32]));
        assert!(join.awaiting.is_empty() && join.pending.is_empty());
    }

    #[test]
    fn footer_join_discards_losing_banks() {
        let now = Instant::now();
        let mut join = FinalizedFooterJoin::new();
        let mut rows = rows();
        join.observe(slot_update(42, 8, SlotStatus::SlotFinalized))
            .unwrap();
        join.observe(footer_update(42, 7)).unwrap();
        join.observe(footer_update(42, 8)).unwrap();
        assert!(!join.pending.contains_key(&(42, 7)));
        assert!(
            join.await_footer(prepared(42), CanonicalIdentity::Bank(7), now)
                .is_err()
        );
        join.await_footer(prepared(42), CanonicalIdentity::Bank(8), now)
            .unwrap();
        join.release_awaiting(now, FooterRelease::Due, true, &mut rows);
        assert_eq!(row_hash(&rows, 42), Some([8; 32]));
    }

    #[test]
    fn block_is_written_without_footer_columns_once_the_wait_ends() {
        crate::metrics::force_init("grpc", None);
        let now = Instant::now();
        let mut join = FinalizedFooterJoin::new();
        let mut rows = rows();
        join.observe(footer_update(40, 5)).unwrap();
        join.observe(slot_update(42, 7, SlotStatus::SlotFinalized))
            .unwrap();
        join.await_footer(prepared(42), CanonicalIdentity::Bank(7), now)
            .unwrap();
        join.release_awaiting(now, FooterRelease::Due, true, &mut rows);
        assert!(rows.block_rows.is_empty());
        join.release_awaiting(
            now + super::FOOTER_WAIT,
            FooterRelease::Due,
            true,
            &mut rows,
        );
        assert_eq!(row_slots(&rows), vec![42]);
        assert_eq!(row_hash(&rows, 42), None);
        assert!(rows.block_rows[0].block_producer_time_nanos.is_none());
        assert!(rows.block_rows[0].block_user_agent.is_none());
        assert!(join.awaiting.is_empty());
    }

    #[test]
    fn blocks_below_the_first_footer_never_wait() {
        let now = Instant::now();
        let mut join = FinalizedFooterJoin::new();
        let mut rows = rows();
        join.await_footer(prepared(10), CanonicalIdentity::Bank(4), now)
            .unwrap();
        join.release_awaiting(now, FooterRelease::Due, true, &mut rows);
        assert!(rows.block_rows.is_empty(), "no footer seen yet, so wait");
        join.observe(footer_update(100, 5)).unwrap();
        join.release_awaiting(now, FooterRelease::Due, true, &mut rows);
        assert_eq!(row_slots(&rows), vec![10]);
        assert!(!join.footer_expected(99));
        assert!(join.footer_expected(100));
    }

    #[test]
    fn blocks_leave_the_wait_in_slot_order() {
        let now = Instant::now();
        let mut join = FinalizedFooterJoin::new();
        let mut rows = rows();
        join.observe(footer_update(40, 5)).unwrap();
        join.observe(slot_update(43, 9, SlotStatus::SlotFinalized))
            .unwrap();
        join.observe(footer_update(43, 9)).unwrap();
        join.await_footer(prepared(42), CanonicalIdentity::Bank(7), now)
            .unwrap();
        join.await_footer(prepared(43), CanonicalIdentity::Bank(9), now)
            .unwrap();
        join.release_awaiting(now, FooterRelease::Due, true, &mut rows);
        assert!(rows.block_rows.is_empty(), "slot 43 stays behind slot 42");
        join.release_awaiting(
            now + super::FOOTER_WAIT,
            FooterRelease::Due,
            true,
            &mut rows,
        );
        assert_eq!(row_slots(&rows), vec![42, 43]);
        assert_eq!(row_hash(&rows, 42), None);
        assert_eq!(row_hash(&rows, 43), Some([9; 32]));
    }

    #[test]
    fn legacy_blocks_skip_the_wait_and_drain_releases_everything() {
        let now = Instant::now();
        let mut join = FinalizedFooterJoin::new();
        let mut rows = rows();
        join.await_footer(prepared(41), CanonicalIdentity::Legacy, now)
            .unwrap();
        join.await_footer(prepared(42), CanonicalIdentity::Bank(7), now)
            .unwrap();
        join.release_awaiting(now, FooterRelease::Due, true, &mut rows);
        assert_eq!(row_slots(&rows), vec![41]);
        assert_eq!(rows.block_rows[0].bank_id, None);
        join.release_awaiting(now, FooterRelease::Skip, true, &mut rows);
        assert_eq!(row_slots(&rows), vec![41]);
        join.release_awaiting(now, FooterRelease::Drain, true, &mut rows);
        assert_eq!(row_slots(&rows), vec![41, 42]);
    }

    #[test]
    fn wait_queue_is_bounded_without_failing_ingestion() {
        let now = Instant::now();
        let mut join = FinalizedFooterJoin::new();
        let mut rows = rows();
        for slot in 1..=(super::FOOTER_AWAIT_MAX_BLOCKS as u64 + 1) {
            join.await_footer(prepared(slot), CanonicalIdentity::Bank(slot), now)
                .unwrap();
        }
        join.release_awaiting(now, FooterRelease::Due, true, &mut rows);
        assert_eq!(row_slots(&rows), vec![1]);
        assert_eq!(join.awaiting.len(), super::FOOTER_AWAIT_MAX_BLOCKS);
    }

    #[test]
    fn footer_gap_expires_without_blocking_canonical_ingestion() {
        let mut join = FinalizedFooterJoin::new();
        join.observe(footer_update(42, 7)).unwrap();
        join.observe(slot_update(42, 8, SlotStatus::SlotFinalized))
            .unwrap();
        assert!(
            join.observe(footer_update(42 + super::FOOTER_JOIN_WINDOW_SLOTS + 1, 9))
                .is_ok()
        );
        assert!(!join.finalized.contains_key(&42));
        assert!(!join.pending.contains_key(&(42, 7)));
    }

    #[test]
    fn commitment_slot_status_matches_the_durable_commitment() {
        assert_eq!(
            super::commitment_slot_status(super::CommitmentLevel::Finalized as i32),
            Some(SlotStatus::SlotFinalized as i32)
        );
        assert_eq!(
            super::commitment_slot_status(super::CommitmentLevel::Processed as i32),
            Some(SlotStatus::SlotProcessed as i32)
        );
        assert_eq!(super::commitment_slot_status(99), None);
    }

    #[test]
    fn footer_cannot_establish_identity_for_a_bank_blind_status() {
        let mut join = FinalizedFooterJoin::default();
        join.observe(footer_update(42, 7)).unwrap();
        let mut finalized = slot_update(42, 7, SlotStatus::SlotFinalized);
        if let Some(UpdateOneof::Slot(slot)) = &mut finalized.update_oneof {
            slot.bank_id = None;
        }
        join.observe(finalized).unwrap();
        assert_eq!(
            join.block_identity(42, 0).unwrap(),
            super::CanonicalIdentity::Legacy
        );
        assert!(join.observe(footer_update(42, 7)).is_err());
    }

    #[test]
    fn distant_turbine_first_shred_cannot_prune_footer_or_identity_proofs() {
        let mut join = FinalizedFooterJoin::default();
        join.observe(slot_update(42, 7, SlotStatus::SlotFinalized))
            .unwrap();
        join.observe(footer_update(42, 7)).unwrap();
        join.observe(slot_update(100_000, 9, SlotStatus::SlotFirstShredReceived))
            .unwrap();
        assert_eq!(join.highest_slot, 42);
        assert!(join.pending.contains_key(&(42, 7)));
        assert!(join.known_banks.contains(&(42, 7)));
    }

    #[test]
    fn footer_requires_bank_identity_and_full_hash() {
        let footer = SubscribeUpdateBlockFooter {
            slot: 42,
            bank_id: 7,
            bank_hash: vec![3; 32],
            block_producer_time_nanos: 123,
            block_user_agent: b"agave".to_vec(),
            ..Default::default()
        };
        let row = map_block_footer(&footer).unwrap();
        assert_eq!(row.bank_hash, [3; 32]);
        assert_eq!(row.block_user_agent, b"agave");
        assert!(
            map_block_footer(&SubscribeUpdateBlockFooter {
                bank_hash: vec![3; 31],
                ..footer.clone()
            })
            .is_err()
        );
        assert!(
            map_block_footer(&SubscribeUpdateBlockFooter {
                bank_id: 0,
                ..footer
            })
            .is_ok()
        );
    }

    pub(super) fn build_test_transaction_info(
        cost_units: Option<u64>,
    ) -> SubscribeUpdateTransactionInfo {
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
                    config: None,
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
    fn map_transaction_treats_config_presence_as_v1_and_preserves_all_fields() {
        let mut tx_info = build_test_transaction_info(None);
        let message = tx_info
            .transaction
            .as_mut()
            .and_then(|transaction| transaction.message.as_mut())
            .expect("test message");
        message.versioned = false;
        message.config = Some(TransactionConfig {
            priority_fee: Some(42),
            compute_unit_limit: Some(1_000_000),
            loaded_accounts_data_size_limit: Some(65_536),
            heap_size: Some(32_768),
        });

        let row = map_transaction(42, Some(1_700_000_000), &tx_info).expect("map v1");
        assert_eq!(row.tx_version, Some(1));
        assert_eq!(row.tx_recent_blockhash.0, [3; 32]);
        assert_eq!(row.tx_config_priority_fee, Some(42));
        assert_eq!(row.tx_config_compute_unit_limit, Some(1_000_000));
        assert_eq!(row.tx_config_loaded_accounts_data_size_limit, Some(65_536));
        assert_eq!(row.tx_config_heap_size, Some(32_768));
    }

    #[test]
    fn vat_debit_survives_protobuf_round_trip_and_ingestion() {
        use prost::Message as _;
        let mut tx = build_test_transaction_info(None);
        tx.meta.as_mut().unwrap().rewards = vec![Reward {
            pubkey: "11111111111111111111111111111111".to_owned(),
            lamports: -10,
            post_balance: 90,
            reward_type: 6,
            commission: String::new(),
            commission_bps: String::new(),
        }];
        let bytes = tx.encode_to_vec();
        let decoded = SubscribeUpdateTransactionInfo::decode(bytes.as_slice()).unwrap();
        let row = map_transaction(42, None, &decoded).unwrap();
        assert_eq!(row.meta_reward_type, vec![Some("VATDebit".to_owned())]);
        assert_eq!(row.meta_reward_lamports, vec![-10]);
        assert_eq!(row.meta_reward_post_balance, vec![90]);
    }

    #[test]
    fn map_transaction_preserves_empty_v1_config_and_deactivated_stake() {
        let mut tx_info = build_test_transaction_info(None);
        let message = tx_info
            .transaction
            .as_mut()
            .and_then(|transaction| transaction.message.as_mut())
            .expect("test message");
        message.versioned = true;
        message.config = Some(TransactionConfig {
            priority_fee: None,
            compute_unit_limit: None,
            loaded_accounts_data_size_limit: None,
            heap_size: None,
        });
        tx_info.meta.as_mut().expect("test meta").rewards = vec![Reward {
            pubkey: "11111111111111111111111111111111".to_string(),
            lamports: 1,
            post_balance: 2,
            reward_type: RewardType::DeactivatedStake as i32,
            commission: String::new(),
            commission_bps: String::new(),
        }];

        let row = map_transaction(42, None, &tx_info).expect("map empty v1");
        assert_eq!(row.tx_version, Some(1));
        assert_eq!(row.tx_config_priority_fee, None);
        assert_eq!(row.tx_config_compute_unit_limit, None);
        assert_eq!(row.tx_config_loaded_accounts_data_size_limit, None);
        assert_eq!(row.tx_config_heap_size, None);
        assert_eq!(
            row.meta_reward_type,
            vec![Some("DeactivatedStake".to_string())]
        );
    }

    #[test]
    fn map_transaction_rejects_v1_address_table_lookups() {
        let mut tx_info = build_test_transaction_info(None);
        let message = tx_info
            .transaction
            .as_mut()
            .and_then(|transaction| transaction.message.as_mut())
            .expect("test message");
        message.config = Some(TransactionConfig {
            priority_fee: None,
            compute_unit_limit: None,
            loaded_accounts_data_size_limit: None,
            heap_size: None,
        });
        message.address_table_lookups = vec![MessageAddressTableLookup {
            account_key: vec![4; 32],
            writable_indexes: vec![0],
            readonly_indexes: vec![],
        }];

        let err = match map_transaction(42, None, &tx_info) {
            Ok(_) => panic!("v1 lookup transaction must be rejected"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("v1 transaction contains address table lookups")
        );
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

    #[test]
    fn legacy_fumarole_stops_after_genesis_and_grpc_accepts_zero_bank_id() {
        let mut block = SubscribeUpdateBlock {
            slot: 100,
            ..Default::default()
        };
        assert!(validate_block_bank(&block, IngestSource::Fumarole, None).is_err());
        assert!(validate_block_bank(&block, IngestSource::Fumarole, Some(100)).is_ok());
        block.slot = 101;
        assert!(validate_block_bank(&block, IngestSource::Fumarole, Some(100)).is_err());
        assert!(validate_block_bank(&block, IngestSource::Grpc, None).is_ok());
        block.bank_id = 7;
        assert!(validate_block_bank(&block, IngestSource::Grpc, None).is_ok());
    }

    fn build_block_metadata_row(slot: u64) -> BlockMetadataRow {
        BlockMetadataRow {
            slot,
            parent_slot: slot.saturating_sub(1),
            blockhash: Array([1u8; 32]),
            parent_blockhash: Array([2u8; 32]),
            bank_id: Some(0),
            bank_hash: None,
            block_producer_time_nanos: None,
            block_user_agent: None,
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
            rewards_commission_bps: Vec::new(),
            rewards_num_partitions: None,
        }
    }
}

#[cfg(test)]
mod completeness_tests;
