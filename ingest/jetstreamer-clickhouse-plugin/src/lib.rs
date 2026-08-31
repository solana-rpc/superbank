// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::{fmt, sync::Arc, time::Duration};

use clickhouse::inserter::Inserter;
use clickhouse::{Client, Row, RowOwned, RowWrite};
use futures_util::FutureExt;
use jetstreamer_firehose::firehose::{BlockData, EntryData, TransactionData};
use jetstreamer_plugin::{Plugin, PluginFuture};
use parking_lot::Mutex;
use serde::Serialize;
use solana_message::VersionedMessage;
use tokio::sync::{mpsc, oneshot};
use url::Url;

/// Configuration for the ClickHouse ingest plugin.
#[derive(Clone, Debug)]
pub struct ClickhouseIngestConfig {
    /// Optional DSN for the ingest ClickHouse cluster. If set, this overrides the
    /// ClickHouse client provided by the runner.
    pub ingest_dsn: Option<String>,
    /// Toggle for single-node deployments. Defaults to false (clustered).
    pub single_node: bool,
    /// ClickHouse database name.
    pub database: String,
    /// Base table for transactions.
    pub transactions_table: String,
    /// Base table for block metadata.
    pub blocks_metadata_table: String,
    /// Base table for PoH entries.
    pub entries_table: String,
    /// Max rows per ClickHouse insert batch.
    pub flush_max_rows: u64,
    /// Max bytes per ClickHouse insert batch.
    pub flush_max_bytes: u64,
    /// Periodic flush interval in milliseconds.
    pub flush_interval_ms: u64,
    /// Max concurrent insert workers per thread (also sizes the inbound queue).
    pub max_inflight_batches: usize,
    /// Max retries for insert failures.
    pub retry_max: usize,
    /// Base backoff (ms) between retries.
    pub retry_backoff_ms: u64,
    /// Reserve capacity for per-slot transaction and entry buffers.
    pub pending_tx_capacity: usize,
    /// Enable async insert (server-side).
    pub async_insert: bool,
    /// Wait for async insert to complete.
    pub wait_for_async_insert: bool,
    /// Timeout for sending insert data chunks to ClickHouse (ms). 0 disables.
    pub insert_send_timeout_ms: u64,
    /// Timeout for finalizing an insert request (ms). 0 disables.
    pub insert_end_timeout_ms: u64,
    /// Disable schema validation for maximum throughput.
    pub validate_schema: bool,
}

impl Default for ClickhouseIngestConfig {
    fn default() -> Self {
        Self {
            ingest_dsn: None,
            single_node: false,
            database: "default".to_string(),
            transactions_table: "transactions".to_string(),
            blocks_metadata_table: "blocks_metadata".to_string(),
            entries_table: "entries".to_string(),
            flush_max_rows: 100_000,
            flush_max_bytes: 64 * 1024 * 1024,
            flush_interval_ms: 10_000,
            max_inflight_batches: 8,
            retry_max: 5,
            retry_backoff_ms: 50,
            pending_tx_capacity: 4096,
            async_insert: true,
            wait_for_async_insert: false,
            insert_send_timeout_ms: 10_000,
            insert_end_timeout_ms: 60_000,
            validate_schema: false,
        }
    }
}

impl ClickhouseIngestConfig {
    fn transactions_fqn(&self) -> String {
        format!("{}.{}", self.database, self.transactions_table)
    }

    fn blocks_metadata_fqn(&self) -> String {
        format!("{}.{}", self.database, self.blocks_metadata_table)
    }

    fn entries_fqn(&self) -> String {
        format!("{}.{}", self.database, self.entries_table)
    }
}

fn apply_env_overrides(config: &mut ClickhouseIngestConfig) {
    if let Some(value) = env_u64("JETSTREAMER_CLICKHOUSE_FLUSH_MAX_ROWS") {
        config.flush_max_rows = value;
    }
    if let Some(value) = env_u64("JETSTREAMER_CLICKHOUSE_FLUSH_MAX_BYTES") {
        config.flush_max_bytes = value;
    }
    if let Some(value) = env_u64("JETSTREAMER_CLICKHOUSE_FLUSH_INTERVAL_MS") {
        config.flush_interval_ms = value;
    }
    if let Some(value) = env_usize("JETSTREAMER_CLICKHOUSE_MAX_INFLIGHT_BATCHES") {
        config.max_inflight_batches = value;
    }
    if let Some(value) = env_usize("JETSTREAMER_CLICKHOUSE_RETRY_MAX") {
        config.retry_max = value;
    }
    if let Some(value) = env_u64("JETSTREAMER_CLICKHOUSE_RETRY_BACKOFF_MS") {
        config.retry_backoff_ms = value;
    }
    if let Some(value) = env_usize("JETSTREAMER_CLICKHOUSE_PENDING_TX_CAPACITY") {
        config.pending_tx_capacity = value;
    }
    if let Some(value) = env_bool("JETSTREAMER_CLICKHOUSE_ASYNC_INSERT") {
        config.async_insert = value;
    }
    if let Some(value) = env_bool("JETSTREAMER_CLICKHOUSE_WAIT_FOR_ASYNC_INSERT") {
        config.wait_for_async_insert = value;
    }
    if let Some(value) = env_u64("JETSTREAMER_CLICKHOUSE_INSERT_SEND_TIMEOUT_MS") {
        config.insert_send_timeout_ms = value;
    }
    if let Some(value) = env_u64("JETSTREAMER_CLICKHOUSE_INSERT_END_TIMEOUT_MS") {
        config.insert_end_timeout_ms = value;
    }
}

fn env_u64(name: &str) -> Option<u64> {
    let raw = std::env::var(name).ok()?;
    match raw.parse::<u64>() {
        Ok(value) => Some(value),
        Err(err) => {
            log::warn!("invalid {} '{}': {}", name, raw, err);
            None
        }
    }
}

fn env_usize(name: &str) -> Option<usize> {
    let raw = std::env::var(name).ok()?;
    match raw.parse::<usize>() {
        Ok(value) => Some(value),
        Err(err) => {
            log::warn!("invalid {} '{}': {}", name, raw, err);
            None
        }
    }
}

fn env_bool(name: &str) -> Option<bool> {
    let raw = std::env::var(name).ok()?;
    let normalized = raw.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => {
            log::warn!("invalid {} '{}': expected true/false (or 1/0)", name, raw);
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ClickhouseIngestConfig, EntryRow, apply_env_overrides};
    use jetstreamer_firehose::firehose::EntryData;

    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    struct EnvGuard {
        saved: Vec<(String, Option<String>)>,
    }

    impl EnvGuard {
        fn new() -> Self {
            Self { saved: Vec::new() }
        }

        fn set(&mut self, key: &str, value: &str) {
            if !self.saved.iter().any(|(k, _)| k == key) {
                self.saved.push((key.to_string(), std::env::var(key).ok()));
            }
            unsafe { std::env::set_var(key, value) };
        }

        fn unset(&mut self, key: &str) {
            if !self.saved.iter().any(|(k, _)| k == key) {
                self.saved.push((key.to_string(), std::env::var(key).ok()));
            }
            unsafe { std::env::remove_var(key) };
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (key, value) in self.saved.drain(..) {
                match value {
                    Some(value) => unsafe { std::env::set_var(key, value) },
                    None => unsafe { std::env::remove_var(key) },
                }
            }
        }
    }

    #[test]
    fn env_overrides_apply() {
        let _lock = ENV_LOCK.lock().unwrap();
        let mut guard = EnvGuard::new();
        guard.set("JETSTREAMER_CLICKHOUSE_FLUSH_MAX_ROWS", "123");
        guard.set("JETSTREAMER_CLICKHOUSE_FLUSH_MAX_BYTES", "456");
        guard.set("JETSTREAMER_CLICKHOUSE_FLUSH_INTERVAL_MS", "789");
        guard.set("JETSTREAMER_CLICKHOUSE_MAX_INFLIGHT_BATCHES", "3");
        guard.set("JETSTREAMER_CLICKHOUSE_RETRY_MAX", "9");
        guard.set("JETSTREAMER_CLICKHOUSE_RETRY_BACKOFF_MS", "77");
        guard.set("JETSTREAMER_CLICKHOUSE_PENDING_TX_CAPACITY", "2048");
        guard.set("JETSTREAMER_CLICKHOUSE_ASYNC_INSERT", "false");
        guard.set("JETSTREAMER_CLICKHOUSE_WAIT_FOR_ASYNC_INSERT", "0");
        guard.set("JETSTREAMER_CLICKHOUSE_INSERT_SEND_TIMEOUT_MS", "1500");
        guard.set("JETSTREAMER_CLICKHOUSE_INSERT_END_TIMEOUT_MS", "2500");

        let mut config = ClickhouseIngestConfig::default();
        apply_env_overrides(&mut config);

        assert_eq!(config.flush_max_rows, 123);
        assert_eq!(config.flush_max_bytes, 456);
        assert_eq!(config.flush_interval_ms, 789);
        assert_eq!(config.max_inflight_batches, 3);
        assert_eq!(config.retry_max, 9);
        assert_eq!(config.retry_backoff_ms, 77);
        assert_eq!(config.pending_tx_capacity, 2048);
        assert!(!config.async_insert);
        assert!(!config.wait_for_async_insert);
        assert_eq!(config.insert_send_timeout_ms, 1500);
        assert_eq!(config.insert_end_timeout_ms, 2500);
    }

    #[test]
    fn invalid_env_values_are_ignored() {
        let _lock = ENV_LOCK.lock().unwrap();
        let mut guard = EnvGuard::new();
        guard.set("JETSTREAMER_CLICKHOUSE_FLUSH_MAX_ROWS", "nope");
        guard.set("JETSTREAMER_CLICKHOUSE_ASYNC_INSERT", "maybe");
        guard.set("JETSTREAMER_CLICKHOUSE_INSERT_END_TIMEOUT_MS", "na");
        guard.unset("JETSTREAMER_CLICKHOUSE_RETRY_MAX");

        let mut config = ClickhouseIngestConfig::default();
        let defaults = config.clone();
        apply_env_overrides(&mut config);

        assert_eq!(config.flush_max_rows, defaults.flush_max_rows);
        assert_eq!(config.async_insert, defaults.async_insert);
        assert_eq!(config.retry_max, defaults.retry_max);
        assert_eq!(config.insert_end_timeout_ms, defaults.insert_end_timeout_ms);
    }

    #[test]
    fn default_entries_table_fqn() {
        let config = ClickhouseIngestConfig::default();
        assert_eq!(config.entries_table, "entries");
        assert_eq!(config.entries_fqn(), "default.entries");
    }

    #[test]
    fn entry_rows_preserve_poh_metadata() {
        let entry = EntryData {
            slot: 42,
            entry_index: 7,
            transaction_indexes: 11..14,
            num_hashes: 999,
            hash: Default::default(),
        };

        let row = EntryRow::from_entry(&entry);

        assert_eq!(row.slot, 42);
        assert_eq!(row.entry_index, 7);
        assert_eq!(row.block_time, None);
        assert_eq!(row.starting_transaction_index, 11);
        assert_eq!(row.transaction_count, 3);
        assert_eq!(row.num_hashes, 999);
        assert_eq!(row.hash, [0u8; 32]);
    }
}

/// ClickHouse ingest plugin that writes `transactions`, `blocks_metadata`, and `entries`.
pub struct ClickhouseIngestPlugin {
    config: Arc<ClickhouseIngestConfig>,
    tables: Tables,
    threads: Vec<Mutex<ThreadState>>,
    ingest_client: Mutex<Option<Arc<Client>>>,
}

impl ClickhouseIngestPlugin {
    /// Build a new plugin instance with a fixed firehose thread count.
    pub fn new(config: ClickhouseIngestConfig, worker_threads: usize) -> Self {
        let mut config = config;
        apply_env_overrides(&mut config);
        log::info!(
            "ClickHouse ingest backpressure config: flush_max_rows={}, flush_max_bytes={}, flush_interval_ms={}, max_inflight_batches={}, retry_max={}, retry_backoff_ms={}, pending_tx_capacity={}, async_insert={}, wait_for_async_insert={}, insert_send_timeout_ms={}, insert_end_timeout_ms={}",
            config.flush_max_rows,
            config.flush_max_bytes,
            config.flush_interval_ms,
            config.max_inflight_batches,
            config.retry_max,
            config.retry_backoff_ms,
            config.pending_tx_capacity,
            config.async_insert,
            config.wait_for_async_insert,
            config.insert_send_timeout_ms,
            config.insert_end_timeout_ms
        );
        let worker_threads = worker_threads.max(1);
        let tables = Tables {
            transactions: config.transactions_fqn(),
            blocks_metadata: config.blocks_metadata_fqn(),
            entries: config.entries_fqn(),
        };
        let mut threads = Vec::with_capacity(worker_threads);
        for _ in 0..worker_threads {
            threads.push(Mutex::new(ThreadState::new(config.pending_tx_capacity)));
        }
        Self {
            config: Arc::new(config),
            tables,
            threads,
            ingest_client: Mutex::new(None),
        }
    }

    fn resolve_db(&self, fallback: Option<Arc<Client>>) -> Option<Arc<Client>> {
        if let Some(client) = self.ingest_client.lock().clone() {
            Some(client)
        } else {
            fallback
        }
    }

    fn thread_state(&self, thread_id: usize) -> Result<&Mutex<ThreadState>, PluginError> {
        self.threads
            .get(thread_id)
            .ok_or_else(|| PluginError::new(format!("thread_id {} out of range", thread_id)))
    }

    fn ensure_writer(
        &self,
        thread_id: usize,
        db: Arc<Client>,
    ) -> Result<mpsc::Sender<WriterCommand>, PluginError> {
        let state_lock = self.thread_state(thread_id)?;
        let mut state = state_lock.lock();
        if let Some(writer) = &state.writer {
            return Ok(writer.sender.clone());
        }

        let capacity = self.config.max_inflight_batches.max(1);
        let (sender, receiver) = mpsc::channel(capacity);
        let config = Arc::clone(&self.config);
        let tables = self.tables.clone();
        let handle = tokio::spawn(writer_loop(db, receiver, config, tables));
        state.writer = Some(WriterHandle {
            sender: sender.clone(),
            handle: Some(handle),
        });
        Ok(sender)
    }
}

impl Plugin for ClickhouseIngestPlugin {
    fn name(&self) -> &'static str {
        "ClickHouse Ingest"
    }

    fn on_load(&self, db: Option<Arc<Client>>) -> PluginFuture<'_> {
        let this = self;
        async move {
            if let Some(dsn) = this
                .config
                .ingest_dsn
                .clone()
                .or_else(|| std::env::var("JETSTREAMER_INGEST_CLICKHOUSE_DSN").ok())
            {
                let client = Arc::new(build_clickhouse_client(&dsn));
                *this.ingest_client.lock() = Some(client);
            }

            let effective_db = this.resolve_db(db);
            if effective_db.is_none() {
                log::warn!("ClickHouse ingest plugin loaded with clickhouse disabled.");
                return Ok(());
            }
            if this.config.single_node {
                log::info!("ClickHouse ingest plugin in single-node mode.");
            } else {
                log::info!("ClickHouse ingest plugin in clustered mode.");
            }
            Ok(())
        }
        .boxed()
    }

    fn on_transaction<'a>(
        &'a self,
        thread_id: usize,
        db: Option<Arc<Client>>,
        transaction: &'a TransactionData,
    ) -> PluginFuture<'a> {
        async move {
            let Some(db) = self.resolve_db(db) else {
                return Ok(());
            };
            let row = TransactionRow::from_transaction(transaction);
            let _sender = self.ensure_writer(thread_id, db)?;

            let state_lock = self.thread_state(thread_id)?;
            let mut state = state_lock.lock();
            let slot = transaction.slot;
            match state.pending_slot {
                Some(current) if current != slot => {
                    log::warn!(
                        "dropping {} buffered transactions and {} buffered entries for slot {} (next slot {})",
                        state.pending_transactions.len(),
                        state.pending_entries.len(),
                        current,
                        slot
                    );
                    state.pending_transactions.clear();
                    state.pending_entries.clear();
                    state.pending_slot = Some(slot);
                }
                None => {
                    state.pending_slot = Some(slot);
                }
                _ => {}
            }
            state.pending_transactions.push(row);
            Ok(())
        }
        .boxed()
    }

    fn on_entry<'a>(
        &'a self,
        thread_id: usize,
        db: Option<Arc<Client>>,
        entry: &'a EntryData,
    ) -> PluginFuture<'a> {
        async move {
            let Some(db) = self.resolve_db(db) else {
                return Ok(());
            };
            let row = EntryRow::from_entry(entry);
            let _sender = self.ensure_writer(thread_id, db)?;

            let state_lock = self.thread_state(thread_id)?;
            let mut state = state_lock.lock();
            let slot = entry.slot;
            match state.pending_slot {
                Some(current) if current != slot => {
                    log::warn!(
                        "dropping {} buffered transactions and {} buffered entries for slot {} (next slot {})",
                        state.pending_transactions.len(),
                        state.pending_entries.len(),
                        current,
                        slot
                    );
                    state.pending_transactions.clear();
                    state.pending_entries.clear();
                    state.pending_slot = Some(slot);
                }
                None => {
                    state.pending_slot = Some(slot);
                }
                _ => {}
            }
            state.pending_entries.push(row);
            Ok(())
        }
        .boxed()
    }

    fn on_block<'a>(
        &'a self,
        thread_id: usize,
        db: Option<Arc<Client>>,
        block: &'a BlockData,
    ) -> PluginFuture<'a> {
        async move {
            let Some(db) = self.resolve_db(db) else {
                return Ok(());
            };
            let slot = block.slot();

            if block.was_skipped() {
                let state_lock = self.thread_state(thread_id)?;
                let mut state = state_lock.lock();
                if state.pending_slot == Some(slot) {
                    if !state.pending_transactions.is_empty() || !state.pending_entries.is_empty() {
                        log::warn!(
                            "clearing {} buffered transactions and {} buffered entries for skipped slot {}",
                            state.pending_transactions.len(),
                            state.pending_entries.len(),
                            slot
                        );
                    }
                    state.pending_slot = None;
                    state.pending_transactions.clear();
                    state.pending_entries.clear();
                } else if !state.pending_transactions.is_empty() || !state.pending_entries.is_empty() {
                    log::debug!(
                        "skipped slot {} leaving pending_slot={:?} pending_transactions={} pending_entries={}",
                        slot,
                        state.pending_slot,
                        state.pending_transactions.len(),
                        state.pending_entries.len()
                    );
                }
                return Ok(());
            }

            let block_row = match BlocksMetadataRow::from_block(block) {
                Some(row) => row,
                None => return Ok(()),
            };

            let mut transactions = Vec::new();
            let mut entries = Vec::new();
            let pending_slot;
            {
                let state_lock = self.thread_state(thread_id)?;
                let mut state = state_lock.lock();
                pending_slot = state.pending_slot;
                if state.pending_slot == Some(slot) {
                    transactions = state.pending_transactions.split_off(0);
                    entries = state.pending_entries.split_off(0);
                } else if state.pending_slot.is_some()
                    && (!state.pending_transactions.is_empty() || !state.pending_entries.is_empty())
                {
                    log::warn!(
                        "dropping {} buffered transactions and {} buffered entries for slot {} (block slot {})",
                        state.pending_transactions.len(),
                        state.pending_entries.len(),
                        state.pending_slot.unwrap_or_default(),
                        slot
                    );
                    state.pending_transactions.clear();
                    state.pending_entries.clear();
                }
                state.pending_slot = None;
            }

            if !transactions.is_empty() {
                for row in transactions.iter_mut() {
                    row.block_time = block_row.block_time;
                }
            }
            if !entries.is_empty() {
                for row in entries.iter_mut() {
                    row.block_time = block_row.block_time;
                }
            }

            let expected = block_row.executed_transaction_count as usize;
            let got = transactions.len();
            if expected > 0 && got == 0 {
                log::warn!(
                    "block {} has executed_transaction_count={} but zero buffered transactions (thread={}, pending_slot={:?})",
                    slot,
                    expected,
                    thread_id,
                    pending_slot
                );
            } else if expected > 0 && got != expected {
                log::warn!(
                    "block {} transaction count mismatch: executed_transaction_count={}, buffered_transactions={} (thread={}, pending_slot={:?})",
                    slot,
                    expected,
                    got,
                    thread_id,
                    pending_slot
                );
            }

            let expected_entries = block_row.entry_count as usize;
            let got_entries = entries.len();
            if expected_entries > 0 && got_entries == 0 {
                log::warn!(
                    "block {} has entry_count={} but zero buffered entries (thread={}, pending_slot={:?})",
                    slot,
                    expected_entries,
                    thread_id,
                    pending_slot
                );
            } else if expected_entries > 0 && got_entries != expected_entries {
                log::warn!(
                    "block {} entry count mismatch: entry_count={}, buffered_entries={} (thread={}, pending_slot={:?})",
                    slot,
                    expected_entries,
                    got_entries,
                    thread_id,
                    pending_slot
                );
            }

            let sender = self.ensure_writer(thread_id, db)?;
            if !transactions.is_empty() {
                sender
                    .send(WriterCommand::Transactions(transactions))
                    .await
                    .map_err(|err| PluginError::new(err.to_string()))?;
            }
            if !entries.is_empty() {
                sender
                    .send(WriterCommand::Entries(entries))
                    .await
                    .map_err(|err| PluginError::new(err.to_string()))?;
            }
            sender
                .send(WriterCommand::Blocks(vec![block_row]))
                .await
                .map_err(|err| PluginError::new(err.to_string()))?;
            Ok(())
        }
        .boxed()
    }

    fn on_exit(&self, _db: Option<Arc<Client>>) -> PluginFuture<'_> {
        let this = self;
        async move {
            let mut shutdowns = Vec::new();
            let mut handles = Vec::new();
            let mut writers = Vec::new();
            for thread_state in &this.threads {
                let mut state = thread_state.lock();
                if state.pending_slot.is_some()
                    && (!state.pending_transactions.is_empty() || !state.pending_entries.is_empty())
                {
                    log::warn!(
                        "dropping {} pending transactions and {} pending entries for slot {:?} during shutdown",
                        state.pending_transactions.len(),
                        state.pending_entries.len(),
                        state.pending_slot
                    );
                }
                state.pending_transactions.clear();
                state.pending_entries.clear();
                state.pending_slot = None;
                if let Some(writer) = state.writer.take() {
                    writers.push(writer);
                }
            }

            for writer in writers {
                let (tx, rx) = oneshot::channel();
                shutdowns.push(rx);
                if let Err(err) = writer.sender.send(WriterCommand::Shutdown(tx)).await {
                    log::warn!("failed to signal writer shutdown: {}", err);
                }
                if let Some(handle) = writer.handle {
                    handles.push(handle);
                }
            }

            for rx in shutdowns {
                let _ = rx.await;
            }
            for handle in handles {
                let _ = handle.await;
            }
            Ok(())
        }
        .boxed()
    }
}

#[derive(Clone)]
struct Tables {
    transactions: String,
    blocks_metadata: String,
    entries: String,
}

struct ThreadState {
    pending_slot: Option<u64>,
    pending_transactions: Vec<TransactionRow>,
    pending_entries: Vec<EntryRow>,
    writer: Option<WriterHandle>,
}

impl ThreadState {
    fn new(pending_capacity: usize) -> Self {
        Self {
            pending_slot: None,
            pending_transactions: Vec::with_capacity(pending_capacity),
            pending_entries: Vec::with_capacity(pending_capacity),
            writer: None,
        }
    }
}

struct WriterHandle {
    sender: mpsc::Sender<WriterCommand>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

struct WorkerHandle {
    sender: mpsc::Sender<WriterCommand>,
    handle: tokio::task::JoinHandle<()>,
}

#[derive(Debug)]
struct PluginError(String);

impl PluginError {
    fn new(msg: String) -> Self {
        Self(msg)
    }
}

impl fmt::Display for PluginError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for PluginError {}

enum WriterCommand {
    Transactions(Vec<TransactionRow>),
    Blocks(Vec<BlocksMetadataRow>),
    Entries(Vec<EntryRow>),
    Shutdown(oneshot::Sender<()>),
}

async fn writer_loop(
    db: Arc<Client>,
    mut receiver: mpsc::Receiver<WriterCommand>,
    config: Arc<ClickhouseIngestConfig>,
    tables: Tables,
) {
    let worker_count = config.max_inflight_batches.max(1);
    let worker_capacity = 1usize;
    let mut workers = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        let (sender, worker_rx) = mpsc::channel(worker_capacity);
        let handle = tokio::spawn(writer_worker_loop(
            db.clone(),
            worker_rx,
            Arc::clone(&config),
            tables.clone(),
        ));
        workers.push(WorkerHandle { sender, handle });
    }

    let mut next_worker = 0usize;

    loop {
        let cmd = receiver.recv().await;
        match cmd {
            Some(WriterCommand::Shutdown(ack)) => {
                shutdown_workers(&mut workers).await;
                let _ = ack.send(());
                break;
            }
            Some(cmd) => {
                if !dispatch_to_worker(cmd, &mut workers, &mut next_worker).await {
                    log::warn!("no available writer workers; dropping batch");
                }
            }
            None => {
                shutdown_workers(&mut workers).await;
                break;
            }
        }
    }
}

async fn writer_worker_loop(
    db: Arc<Client>,
    mut receiver: mpsc::Receiver<WriterCommand>,
    config: Arc<ClickhouseIngestConfig>,
    tables: Tables,
) {
    let db = db.as_ref().clone().with_validation(config.validate_schema);
    let mut transactions_inserter =
        build_inserter::<TransactionRow>(&db, &config, &tables.transactions);
    let mut blocks_inserter =
        build_inserter::<BlocksMetadataRow>(&db, &config, &tables.blocks_metadata);
    let mut entries_inserter = build_inserter::<EntryRow>(&db, &config, &tables.entries);

    let interval = Duration::from_millis(config.flush_interval_ms.max(1));
    let mut ticker = tokio::time::interval(interval);

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let _ = transactions_inserter.commit().await;
                let _ = blocks_inserter.commit().await;
                let _ = entries_inserter.commit().await;
            }
            cmd = receiver.recv() => {
                match cmd {
                    Some(WriterCommand::Transactions(rows)) => {
                        write_with_backoff(
                            &rows,
                            &mut transactions_inserter,
                            &db,
                            &config,
                            &tables.transactions,
                        )
                        .await;
                    }
                    Some(WriterCommand::Blocks(rows)) => {
                        write_with_backoff(
                            &rows,
                            &mut blocks_inserter,
                            &db,
                            &config,
                            &tables.blocks_metadata,
                        )
                        .await;
                    }
                    Some(WriterCommand::Entries(rows)) => {
                        write_with_backoff(
                            &rows,
                            &mut entries_inserter,
                            &db,
                            &config,
                            &tables.entries,
                        )
                        .await;
                    }
                    Some(WriterCommand::Shutdown(ack)) => {
                        let _ = flush_inserter(&mut transactions_inserter).await;
                        let _ = flush_inserter(&mut blocks_inserter).await;
                        let _ = flush_inserter(&mut entries_inserter).await;
                        let _ = ack.send(());
                        break;
                    }
                    None => break,
                }
            }
        }
    }
}

async fn dispatch_to_worker(
    mut cmd: WriterCommand,
    workers: &mut Vec<WorkerHandle>,
    next_worker: &mut usize,
) -> bool {
    while !workers.is_empty() {
        let idx = *next_worker % workers.len();
        *next_worker = (*next_worker + 1) % workers.len();
        let sender = workers[idx].sender.clone();
        match sender.send(cmd).await {
            Ok(()) => return true,
            Err(err) => {
                cmd = err.0;
                let worker = workers.remove(idx);
                drop(worker);
                *next_worker = 0;
            }
        }
    }
    false
}

async fn shutdown_workers(workers: &mut Vec<WorkerHandle>) {
    let senders = workers
        .iter()
        .map(|worker| worker.sender.clone())
        .collect::<Vec<_>>();
    let mut acks = Vec::with_capacity(senders.len());
    for sender in senders {
        let (tx, rx) = oneshot::channel();
        if sender.send(WriterCommand::Shutdown(tx)).await.is_ok() {
            acks.push(rx);
        }
    }
    for rx in acks {
        let _ = rx.await;
    }
    for worker in workers.drain(..) {
        let _ = worker.handle.await;
    }
}

fn build_inserter<T: Row>(
    db: &Client,
    config: &ClickhouseIngestConfig,
    table: &str,
) -> Inserter<T> {
    let mut inserter = db
        .inserter::<T>(table)
        .with_max_rows(config.flush_max_rows)
        .with_max_bytes(config.flush_max_bytes)
        .with_period(Some(Duration::from_millis(config.flush_interval_ms.max(1))));

    if config.async_insert {
        inserter = inserter.with_option("async_insert", "1").with_option(
            "wait_for_async_insert",
            if config.wait_for_async_insert {
                "1"
            } else {
                "0"
            },
        );
    } else {
        inserter = inserter.with_option("async_insert", "0");
    }

    let send_timeout = ms_to_duration(config.insert_send_timeout_ms);
    let end_timeout = ms_to_duration(config.insert_end_timeout_ms);
    if send_timeout.is_some() || end_timeout.is_some() {
        inserter = inserter.with_timeouts(send_timeout, end_timeout);
    }

    inserter
}

fn ms_to_duration(value_ms: u64) -> Option<Duration> {
    if value_ms == 0 {
        None
    } else {
        Some(Duration::from_millis(value_ms))
    }
}

async fn flush_inserter<T: Row>(
    inserter: &mut Inserter<T>,
) -> Result<(), clickhouse::error::Error> {
    let _ = inserter.force_commit().await?;
    Ok(())
}

async fn write_with_retry<T>(
    rows: &[T],
    inserter: &mut Inserter<T>,
    db: &Client,
    config: &ClickhouseIngestConfig,
    table: &str,
) -> Result<(), clickhouse::error::Error>
where
    T: RowOwned + RowWrite,
{
    if rows.is_empty() {
        return Ok(());
    }
    let mut attempt = 0usize;
    let mut delay = config.retry_backoff_ms.max(1);
    loop {
        match write_batch(inserter, rows).await {
            Ok(()) => return Ok(()),
            Err(err) => {
                attempt += 1;
                if attempt > config.retry_max {
                    *inserter = build_inserter(db, config, table);
                    return Err(err);
                }
                log::warn!("retrying insert into {} (attempt {})", table, attempt);
                tokio::time::sleep(Duration::from_millis(delay)).await;
                delay = delay.saturating_mul(2).min(2000);
                *inserter = build_inserter(db, config, table);
            }
        }
    }
}

async fn write_with_backoff<T>(
    rows: &[T],
    inserter: &mut Inserter<T>,
    db: &Client,
    config: &ClickhouseIngestConfig,
    table: &str,
) where
    T: RowOwned + RowWrite,
{
    if rows.is_empty() {
        return;
    }
    let mut delay = config.retry_backoff_ms.max(1);
    loop {
        match write_with_retry(rows, inserter, db, config, table).await {
            Ok(()) => return,
            Err(err) => {
                log::error!(
                    "insert into {} failed after {} retries: {}; backing off {}ms",
                    table,
                    config.retry_max,
                    err,
                    delay
                );
                tokio::time::sleep(Duration::from_millis(delay)).await;
                delay = delay.saturating_mul(2).min(30_000);
            }
        }
    }
}

async fn write_batch<T>(
    inserter: &mut Inserter<T>,
    rows: &[T],
) -> Result<(), clickhouse::error::Error>
where
    T: RowOwned + RowWrite,
{
    for row in rows {
        inserter.write(row).await?;
    }
    let _ = inserter.commit().await?;
    Ok(())
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(transparent)]
struct FixedSignature(#[serde(with = "serde_big_array::BigArray")] [u8; 64]);

#[derive(Row, Serialize, Clone, Debug)]
struct BlocksMetadataRow {
    slot: u64,
    parent_slot: u64,
    blockhash: [u8; 32],
    parent_blockhash: [u8; 32],
    block_time: Option<i64>,
    block_height: Option<u64>,
    executed_transaction_count: u64,
    entry_count: u64,
    rewards_present: u8,
    rewards_pubkey: Vec<[u8; 32]>,
    rewards_lamports: Vec<i64>,
    rewards_post_balance: Vec<u64>,
    rewards_type: Vec<Option<String>>,
    rewards_commission: Vec<Option<u8>>,
    rewards_commission_bps: Vec<Option<u16>>,
    rewards_num_partitions: Option<u64>,
}

impl BlocksMetadataRow {
    fn from_block(block: &BlockData) -> Option<Self> {
        match block {
            BlockData::Block {
                parent_slot,
                parent_blockhash,
                slot,
                blockhash,
                rewards,
                block_time,
                block_height,
                executed_transaction_count,
                entry_count,
            } => {
                let mut rewards_pubkey = Vec::with_capacity(rewards.keyed_rewards.len());
                let mut rewards_lamports = Vec::with_capacity(rewards.keyed_rewards.len());
                let mut rewards_post_balance = Vec::with_capacity(rewards.keyed_rewards.len());
                let mut rewards_type = Vec::with_capacity(rewards.keyed_rewards.len());
                let mut rewards_commission = Vec::with_capacity(rewards.keyed_rewards.len());
                let mut rewards_commission_bps = Vec::with_capacity(rewards.keyed_rewards.len());

                for (pubkey, reward) in rewards.keyed_rewards.iter() {
                    rewards_pubkey.push(pubkey.to_bytes());
                    rewards_lamports.push(reward.lamports);
                    rewards_post_balance.push(reward.post_balance);
                    rewards_type.push(Some(reward.reward_type.to_string()));
                    rewards_commission.push(reward.commission);
                    rewards_commission_bps.push(None);
                }

                let rewards_present =
                    (!rewards_pubkey.is_empty() || rewards.num_partitions.is_some()) as u8;

                Some(Self {
                    slot: *slot,
                    parent_slot: *parent_slot,
                    blockhash: blockhash.to_bytes(),
                    parent_blockhash: parent_blockhash.to_bytes(),
                    block_time: *block_time,
                    block_height: *block_height,
                    executed_transaction_count: *executed_transaction_count,
                    entry_count: *entry_count,
                    rewards_present,
                    rewards_pubkey,
                    rewards_lamports,
                    rewards_post_balance,
                    rewards_type,
                    rewards_commission,
                    rewards_commission_bps,
                    rewards_num_partitions: rewards.num_partitions,
                })
            }
            BlockData::PossibleLeaderSkipped { .. } => None,
        }
    }
}

#[derive(Row, Serialize, Clone, Debug)]
struct EntryRow {
    slot: u64,
    entry_index: u32,
    block_time: Option<i64>,
    starting_transaction_index: u32,
    transaction_count: u32,
    num_hashes: u64,
    hash: [u8; 32],
}

impl EntryRow {
    fn from_entry(entry: &EntryData) -> Self {
        Self {
            slot: entry.slot,
            entry_index: u32::try_from(entry.entry_index).unwrap_or(u32::MAX),
            block_time: None,
            starting_transaction_index: u32::try_from(entry.transaction_indexes.start)
                .unwrap_or(u32::MAX),
            transaction_count: u32::try_from(entry.transaction_indexes.len()).unwrap_or(u32::MAX),
            num_hashes: entry.num_hashes,
            hash: entry.hash.to_bytes(),
        }
    }
}

#[derive(Row, Serialize, Clone, Debug)]
struct TransactionRow {
    signature: FixedSignature,
    slot: u64,
    slot_idx: u32,
    block_time: Option<i64>,
    message_hash: [u8; 32],
    is_vote: u8,
    tx_version: Option<u8>,
    tx_signatures: Vec<FixedSignature>,
    tx_num_required_signatures: u8,
    tx_num_readonly_signed_accounts: u8,
    tx_num_readonly_unsigned_accounts: u8,
    tx_account_keys: Vec<[u8; 32]>,
    tx_recent_blockhash: [u8; 32],
    tx_instructions_program_id_index: Vec<u8>,
    tx_instructions_accounts: Vec<Vec<u8>>,
    tx_instructions_data: Vec<Vec<u8>>,
    tx_address_table_lookups_present: u8,
    tx_address_table_lookup_account_key: Vec<[u8; 32]>,
    tx_address_table_lookup_writable_indexes: Vec<Vec<u8>>,
    tx_address_table_lookup_readonly_indexes: Vec<Vec<u8>>,
    meta_status_ok: u8,
    meta_err: Option<String>,
    meta_fee: u64,
    meta_pre_balances: Vec<u64>,
    meta_post_balances: Vec<u64>,
    meta_inner_instructions_present: u8,
    meta_inner_instructions_index: Vec<u8>,
    meta_inner_instructions_program_id_index: Vec<Vec<u8>>,
    meta_inner_instructions_accounts: Vec<Vec<Vec<u8>>>,
    meta_inner_instructions_data: Vec<Vec<Vec<u8>>>,
    meta_inner_instructions_stack_height: Vec<Vec<Option<u32>>>,
    meta_log_messages_present: u8,
    meta_log_messages: Vec<String>,
    meta_pre_token_balances_present: u8,
    meta_pre_token_account_index: Vec<u8>,
    meta_pre_token_mint: Vec<[u8; 32]>,
    meta_pre_token_owner: Vec<Option<[u8; 32]>>,
    meta_pre_token_program_id: Vec<Option<[u8; 32]>>,
    meta_pre_token_amount: Vec<String>,
    meta_pre_token_decimals: Vec<u8>,
    meta_pre_token_ui_amount: Vec<Option<f64>>,
    meta_pre_token_ui_amount_string: Vec<String>,
    meta_post_token_balances_present: u8,
    meta_post_token_account_index: Vec<u8>,
    meta_post_token_mint: Vec<[u8; 32]>,
    meta_post_token_owner: Vec<Option<[u8; 32]>>,
    meta_post_token_program_id: Vec<Option<[u8; 32]>>,
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
    meta_reward_commission_bps: Vec<Option<u16>>,
    meta_loaded_addresses_writable: Vec<[u8; 32]>,
    meta_loaded_addresses_readonly: Vec<[u8; 32]>,
    meta_return_data_present: u8,
    meta_return_data_program_id: Option<[u8; 32]>,
    meta_return_data_data: Option<Vec<u8>>,
    meta_compute_units_consumed: Option<u64>,
    meta_cost_units: Option<u64>,
}

impl TransactionRow {
    fn from_transaction(transaction: &TransactionData) -> Self {
        let message = &transaction.transaction.message;
        let header = message.header();
        let instructions = message.instructions();

        let tx_signatures = transaction
            .transaction
            .signatures
            .iter()
            .map(|sig| FixedSignature(*sig.as_array()))
            .collect::<Vec<_>>();

        let tx_account_keys = message
            .static_account_keys()
            .iter()
            .map(|key| key.to_bytes())
            .collect::<Vec<_>>();

        let tx_recent_blockhash = match message {
            VersionedMessage::Legacy(msg) => msg.recent_blockhash.to_bytes(),
            VersionedMessage::V0(msg) => msg.recent_blockhash.to_bytes(),
        };

        let tx_version = match message {
            VersionedMessage::Legacy(_) => None,
            VersionedMessage::V0(_) => Some(0),
        };

        let tx_instructions_program_id_index = instructions
            .iter()
            .map(|ix| ix.program_id_index)
            .collect::<Vec<_>>();
        let tx_instructions_accounts = instructions
            .iter()
            .map(|ix| ix.accounts.clone())
            .collect::<Vec<_>>();
        let tx_instructions_data = instructions
            .iter()
            .map(|ix| ix.data.clone())
            .collect::<Vec<_>>();

        let (lookups_present, lookup_account_key, lookup_writable, lookup_readonly) =
            match message.address_table_lookups() {
                Some(lookups) if !lookups.is_empty() => {
                    let mut account_keys = Vec::with_capacity(lookups.len());
                    let mut writable = Vec::with_capacity(lookups.len());
                    let mut readonly = Vec::with_capacity(lookups.len());
                    for lookup in lookups.iter() {
                        account_keys.push(lookup.account_key.to_bytes());
                        writable.push(lookup.writable_indexes.clone());
                        readonly.push(lookup.readonly_indexes.clone());
                    }
                    (1u8, account_keys, writable, readonly)
                }
                _ => (0u8, Vec::new(), Vec::new(), Vec::new()),
            };

        let meta = &transaction.transaction_status_meta;
        let (meta_status_ok, meta_err) = match &meta.status {
            Ok(()) => (1u8, None),
            Err(err) => (0u8, Some(err.to_string())),
        };

        let (inner_present, inner_idx, inner_prog_idx, inner_accounts, inner_data, inner_stack) =
            map_inner_instructions(meta.inner_instructions.as_ref());

        let (pre_present, pre_balances) = map_token_balances(meta.pre_token_balances.as_ref());
        let (post_present, post_balances) = map_token_balances(meta.post_token_balances.as_ref());

        let (
            rewards_present,
            rewards_pubkey,
            rewards_lamports,
            rewards_post_balance,
            rewards_type,
            rewards_commission,
            rewards_commission_bps,
        ) = map_rewards(meta.rewards.as_ref());

        let (return_present, return_program, return_data) =
            map_return_data(meta.return_data.as_ref());

        let meta_log_messages_present = meta.log_messages.is_some() as u8;
        let meta_log_messages = meta.log_messages.clone().unwrap_or_default();

        let loaded_writable = meta
            .loaded_addresses
            .writable
            .iter()
            .map(|key| key.to_bytes())
            .collect::<Vec<_>>();
        let loaded_readonly = meta
            .loaded_addresses
            .readonly
            .iter()
            .map(|key| key.to_bytes())
            .collect::<Vec<_>>();

        let slot_idx = u32::try_from(transaction.transaction_slot_index).unwrap_or(u32::MAX);

        Self {
            signature: FixedSignature(*transaction.signature.as_array()),
            slot: transaction.slot,
            slot_idx,
            block_time: None,
            message_hash: transaction.message_hash.to_bytes(),
            is_vote: transaction.is_vote as u8,
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
            tx_address_table_lookups_present: lookups_present,
            tx_address_table_lookup_account_key: lookup_account_key,
            tx_address_table_lookup_writable_indexes: lookup_writable,
            tx_address_table_lookup_readonly_indexes: lookup_readonly,
            meta_status_ok,
            meta_err,
            meta_fee: meta.fee,
            meta_pre_balances: meta.pre_balances.clone(),
            meta_post_balances: meta.post_balances.clone(),
            meta_inner_instructions_present: inner_present,
            meta_inner_instructions_index: inner_idx,
            meta_inner_instructions_program_id_index: inner_prog_idx,
            meta_inner_instructions_accounts: inner_accounts,
            meta_inner_instructions_data: inner_data,
            meta_inner_instructions_stack_height: inner_stack,
            meta_log_messages_present,
            meta_log_messages,
            meta_pre_token_balances_present: pre_present,
            meta_pre_token_account_index: pre_balances.account_index,
            meta_pre_token_mint: pre_balances.mint,
            meta_pre_token_owner: pre_balances.owner,
            meta_pre_token_program_id: pre_balances.program_id,
            meta_pre_token_amount: pre_balances.amount,
            meta_pre_token_decimals: pre_balances.decimals,
            meta_pre_token_ui_amount: pre_balances.ui_amount,
            meta_pre_token_ui_amount_string: pre_balances.ui_amount_string,
            meta_post_token_balances_present: post_present,
            meta_post_token_account_index: post_balances.account_index,
            meta_post_token_mint: post_balances.mint,
            meta_post_token_owner: post_balances.owner,
            meta_post_token_program_id: post_balances.program_id,
            meta_post_token_amount: post_balances.amount,
            meta_post_token_decimals: post_balances.decimals,
            meta_post_token_ui_amount: post_balances.ui_amount,
            meta_post_token_ui_amount_string: post_balances.ui_amount_string,
            meta_rewards_present: rewards_present,
            meta_reward_pubkey: rewards_pubkey,
            meta_reward_lamports: rewards_lamports,
            meta_reward_post_balance: rewards_post_balance,
            meta_reward_type: rewards_type,
            meta_reward_commission: rewards_commission,
            meta_reward_commission_bps: rewards_commission_bps,
            meta_loaded_addresses_writable: loaded_writable,
            meta_loaded_addresses_readonly: loaded_readonly,
            meta_return_data_present: return_present,
            meta_return_data_program_id: return_program,
            meta_return_data_data: return_data,
            meta_compute_units_consumed: meta.compute_units_consumed,
            meta_cost_units: meta.cost_units,
        }
    }
}

struct TokenBalanceColumns {
    account_index: Vec<u8>,
    mint: Vec<[u8; 32]>,
    owner: Vec<Option<[u8; 32]>>,
    program_id: Vec<Option<[u8; 32]>>,
    amount: Vec<String>,
    decimals: Vec<u8>,
    ui_amount: Vec<Option<f64>>,
    ui_amount_string: Vec<String>,
}

fn map_token_balances(
    balances: Option<&Vec<solana_transaction_status::TransactionTokenBalance>>,
) -> (u8, TokenBalanceColumns) {
    let mut columns = TokenBalanceColumns {
        account_index: Vec::new(),
        mint: Vec::new(),
        owner: Vec::new(),
        program_id: Vec::new(),
        amount: Vec::new(),
        decimals: Vec::new(),
        ui_amount: Vec::new(),
        ui_amount_string: Vec::new(),
    };

    let present = balances.is_some() as u8;
    if let Some(balances) = balances {
        columns.account_index.reserve(balances.len());
        columns.mint.reserve(balances.len());
        columns.owner.reserve(balances.len());
        columns.program_id.reserve(balances.len());
        columns.amount.reserve(balances.len());
        columns.decimals.reserve(balances.len());
        columns.ui_amount.reserve(balances.len());
        columns.ui_amount_string.reserve(balances.len());
        for balance in balances.iter() {
            columns.account_index.push(balance.account_index);
            columns
                .mint
                .push(parse_pubkey_bytes(&balance.mint).unwrap_or([0u8; 32]));
            columns.owner.push(parse_pubkey_bytes(&balance.owner));
            columns
                .program_id
                .push(parse_pubkey_bytes(&balance.program_id));
            columns.amount.push(balance.ui_token_amount.amount.clone());
            columns.decimals.push(balance.ui_token_amount.decimals);
            columns.ui_amount.push(balance.ui_token_amount.ui_amount);
            columns
                .ui_amount_string
                .push(balance.ui_token_amount.ui_amount_string.clone());
        }
    }

    (present, columns)
}

type InnerInstructionColumns = (
    u8,
    Vec<u8>,
    Vec<Vec<u8>>,
    Vec<Vec<Vec<u8>>>,
    Vec<Vec<Vec<u8>>>,
    Vec<Vec<Option<u32>>>,
);

fn map_inner_instructions(
    inner: Option<&Vec<solana_transaction_status::InnerInstructions>>,
) -> InnerInstructionColumns {
    let present = inner.is_some() as u8;
    let mut indices = Vec::new();
    let mut program_ids = Vec::new();
    let mut accounts = Vec::new();
    let mut data = Vec::new();
    let mut stack = Vec::new();

    if let Some(inner) = inner {
        indices.reserve(inner.len());
        program_ids.reserve(inner.len());
        accounts.reserve(inner.len());
        data.reserve(inner.len());
        stack.reserve(inner.len());
        for group in inner.iter() {
            indices.push(group.index);
            let mut group_program_ids = Vec::with_capacity(group.instructions.len());
            let mut group_accounts = Vec::with_capacity(group.instructions.len());
            let mut group_data = Vec::with_capacity(group.instructions.len());
            let mut group_stack = Vec::with_capacity(group.instructions.len());
            for ix in group.instructions.iter() {
                group_program_ids.push(ix.instruction.program_id_index);
                group_accounts.push(ix.instruction.accounts.clone());
                group_data.push(ix.instruction.data.clone());
                group_stack.push(ix.stack_height);
            }
            program_ids.push(group_program_ids);
            accounts.push(group_accounts);
            data.push(group_data);
            stack.push(group_stack);
        }
    }

    (present, indices, program_ids, accounts, data, stack)
}

type RewardColumns = (
    u8,
    Vec<String>,
    Vec<i64>,
    Vec<u64>,
    Vec<Option<String>>,
    Vec<Option<u8>>,
    Vec<Option<u16>>,
);

fn map_rewards(rewards: Option<&Vec<solana_transaction_status::Reward>>) -> RewardColumns {
    let present = rewards.is_some() as u8;
    let mut pubkeys = Vec::new();
    let mut lamports = Vec::new();
    let mut post_balance = Vec::new();
    let mut reward_type = Vec::new();
    let mut commission = Vec::new();
    let mut commission_bps = Vec::new();

    if let Some(rewards) = rewards {
        pubkeys.reserve(rewards.len());
        lamports.reserve(rewards.len());
        post_balance.reserve(rewards.len());
        reward_type.reserve(rewards.len());
        commission.reserve(rewards.len());
        commission_bps.reserve(rewards.len());
        for reward in rewards.iter() {
            pubkeys.push(reward.pubkey.clone());
            lamports.push(reward.lamports);
            post_balance.push(reward.post_balance);
            reward_type.push(reward.reward_type.map(|t| t.to_string()));
            commission.push(reward.commission);
            commission_bps.push(None);
        }
    }

    (
        present,
        pubkeys,
        lamports,
        post_balance,
        reward_type,
        commission,
        commission_bps,
    )
}

fn map_return_data(
    return_data: Option<&solana_transaction_context::TransactionReturnData>,
) -> (u8, Option<[u8; 32]>, Option<Vec<u8>>) {
    match return_data {
        Some(data) => (1, Some(data.program_id.to_bytes()), Some(data.data.clone())),
        None => (0, None, None),
    }
}

fn parse_pubkey_bytes(value: &str) -> Option<[u8; 32]> {
    if value.is_empty() {
        return None;
    }
    let mut out = [0u8; 32];
    let decoded = bs58::decode(value).onto(&mut out).ok()?;
    if decoded != 32 {
        return None;
    }
    Some(out)
}

fn build_clickhouse_client(dsn: &str) -> Client {
    let mut client = Client::default();
    if let Ok(mut url) = Url::parse(dsn) {
        let username = url.username().to_string();
        let password = url.password().map(|p| p.to_string());
        if !username.is_empty() || password.is_some() {
            let _ = url.set_username("");
            let _ = url.set_password(None);
        }
        client = client.with_url(url.as_str());
        if !username.is_empty() {
            client = client.with_user(username);
        }
        if let Some(password) = password {
            client = client.with_password(password);
        }
    } else {
        client = client.with_url(dsn);
    }
    client
}
