// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::{
    fmt,
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::{Context, Result, anyhow};
use clap::parser::ValueSource;
use clap::{ArgMatches, CommandFactory, FromArgMatches, Parser};
use serde::{Deserialize, Serialize, de};

fn default_bigtable_decode_concurrency() -> usize {
    std::thread::available_parallelism()
        .map(|threads| threads.get())
        .unwrap_or(4)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FromSlotSpec {
    Slot(u64),
    LatestDb,
}

impl FromStr for FromSlotSpec {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let normalized = value.trim();
        if normalized == "*" {
            return Ok(FromSlotSpec::LatestDb);
        }
        if normalized.is_empty() {
            return Err("start slot cannot be empty".to_string());
        }
        let slot = normalized
            .parse::<u64>()
            .map_err(|_| format!("invalid start slot '{value}'"))?;
        Ok(FromSlotSpec::Slot(slot))
    }
}

impl<'de> Deserialize<'de> for FromSlotSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct FromSlotVisitor;

        impl<'de> de::Visitor<'de> for FromSlotVisitor {
            type Value = FromSlotSpec;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a non-negative integer slot or '*'")
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(FromSlotSpec::Slot(value))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if value < 0 {
                    return Err(E::custom("start slot must be non-negative"));
                }
                Ok(FromSlotSpec::Slot(value as u64))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                FromSlotSpec::from_str(value).map_err(E::custom)
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                self.visit_str(&value)
            }
        }

        deserializer.deserialize_any(FromSlotVisitor)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum IngestSource {
    Fumarole,
    Grpc,
    Rpc,
    Bigtable,
}

impl IngestSource {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            IngestSource::Fumarole => "fumarole",
            IngestSource::Grpc => "grpc",
            IngestSource::Rpc => "rpc",
            IngestSource::Bigtable => "bigtable",
        }
    }
}

#[derive(Parser, Debug)]
#[command(about = "Superbank ingestor")]
struct CliArgs {
    /// Ingest source: fumarole | grpc | rpc | bigtable
    #[arg(long, env = "SUPERBANK_SOURCE", value_enum)]
    source: Option<IngestSource>,

    /// Path to YAML config file
    #[arg(long, env = "SUPERBANK_CONFIG", value_name = "PATH")]
    config: Option<PathBuf>,

    /// Yellowstone gRPC endpoint (Dragons Mouth)
    #[arg(long, env = "DRAGONSMOUTH_ENDPOINT")]
    endpoint: Option<String>,

    /// x-token for Dragons Mouth auth
    #[arg(long, env = "DRAGONSMOUTH_X_TOKEN")]
    x_token: Option<String>,

    /// Fumarole endpoint
    #[arg(long, env = "FUMAROLE_ENDPOINT")]
    fumarole_endpoint: Option<String>,

    /// x-token for Fumarole auth
    #[arg(long, env = "FUMAROLE_X_TOKEN")]
    fumarole_x_token: Option<String>,

    /// Fumarole persistent consumer group name
    #[arg(long, env = "FUMAROLE_CONSUMER_GROUP")]
    fumarole_consumer_group: Option<String>,

    /// Create the Fumarole consumer group before subscribing
    #[arg(
        long,
        env = "FUMAROLE_CREATE_CONSUMER_GROUP",
        default_value_t = false,
        default_missing_value = "true",
        num_args = 0..=1,
        require_equals = true,
        action = clap::ArgAction::Set
    )]
    fumarole_create_consumer_group: bool,

    /// Number of Fumarole data-plane TCP connections
    #[arg(long, env = "FUMAROLE_DATA_PLANE_TCP_CONNECTIONS", default_value_t = 4)]
    fumarole_data_plane_tcp_connections: u8,

    /// Concurrent Fumarole shard downloads per TCP connection
    #[arg(
        long,
        env = "FUMAROLE_CONCURRENT_DOWNLOAD_LIMIT_PER_TCP",
        default_value_t = 2
    )]
    fumarole_concurrent_download_limit_per_tcp: usize,

    /// Fumarole stream output channel capacity
    #[arg(long, env = "FUMAROLE_DATA_CHANNEL_CAPACITY", default_value_t = 4096)]
    fumarole_data_channel_capacity: usize,

    /// Fumarole offset commit interval (seconds)
    #[arg(long, env = "FUMAROLE_COMMIT_INTERVAL_SECS", default_value_t = 10)]
    fumarole_commit_interval_secs: u64,

    /// Disable Fumarole offset commits
    #[arg(long, env = "FUMAROLE_NO_COMMIT", default_value_t = false)]
    fumarole_no_commit: bool,

    /// Commitment level: processed | confirmed | finalized
    #[arg(long, env = "DRAGONSMOUTH_COMMITMENT", default_value = "finalized")]
    commitment: String,

    /// Starting slot for DragonsMouth/gRPC replay (use '*' for latest in blocks_metadata).
    #[arg(long = "dragonsmouth-from-slot", env = "DRAGONSMOUTH_FROM_SLOT")]
    dragonsmouth_from_slot: Option<FromSlotSpec>,

    /// Starting slot for Fumarole consumer group initialization.
    #[arg(long = "fumarole-from-slot", env = "FUMAROLE_FROM_SLOT")]
    fumarole_from_slot: Option<FromSlotSpec>,

    /// Starting slot for RPC replay (use '*' for latest in blocks_metadata, '0' for earliest available).
    #[arg(long = "rpc-from-slot", env = "RPC_FROM_SLOT")]
    rpc_from_slot: Option<FromSlotSpec>,

    /// Deprecated shared start-slot option. Use source-specific options instead.
    #[arg(long = "from-slot", hide = true)]
    legacy_from_slot: Option<FromSlotSpec>,

    /// Max gRPC decoding message size (bytes)
    #[arg(long, env = "GRPC_MAX_DECODING_BYTES", default_value_t = 64 * 1024 * 1024)]
    grpc_max_decoding_bytes: usize,

    /// Enable HTTP/2 adaptive window sizing for gRPC
    #[arg(
        long,
        env = "GRPC_HTTP2_ADAPTIVE_WINDOW",
        default_value_t = false,
        default_missing_value = "true",
        num_args = 0..=1,
        require_equals = true,
        action = clap::ArgAction::Set
    )]
    grpc_http2_adaptive_window: bool,

    /// Exit if no gRPC messages arrive for this many seconds
    #[arg(long, env = "GRPC_IDLE_TIMEOUT_SECS", default_value_t = 30)]
    grpc_idle_timeout_secs: u64,

    /// Enable gRPC health watch and fail if stream health degrades
    #[arg(
        long,
        env = "GRPC_HEALTH_WATCH_ENABLED",
        default_value_t = true,
        default_missing_value = "true",
        num_args = 0..=1,
        require_equals = true,
        action = clap::ArgAction::Set
    )]
    grpc_health_watch_enabled: bool,

    /// Subscribe to slot notifications on the gRPC stream to populate the chain-tip lag metric.
    #[arg(
        long,
        env = "GRPC_SLOT_NOTIFICATIONS",
        default_value_t = true,
        default_missing_value = "true",
        num_args = 0..=1,
        require_equals = true,
        action = clap::ArgAction::Set
    )]
    grpc_slot_notifications: bool,

    /// Solana JSON-RPC URL (required for rpc source)
    #[arg(long, env = "RPC_URL")]
    rpc_url: Option<String>,

    /// End slot (inclusive) for rpc source
    #[arg(long, env = "RPC_TO_SLOT")]
    rpc_to_slot: Option<u64>,

    /// Slot count for rpc source (exclusive with --to-slot)
    #[arg(long, env = "RPC_SLOT_COUNT")]
    rpc_slot_count: Option<u64>,

    /// RPC timeout (seconds)
    #[arg(long, env = "RPC_TIMEOUT_SECS", default_value_t = 30)]
    rpc_timeout_secs: u64,

    /// RPC retry backoff (milliseconds)
    #[arg(long, env = "RPC_RETRY_BACKOFF_MS", default_value_t = 500)]
    rpc_retry_backoff_ms: u64,

    /// Max in-flight RPC getBlock requests
    #[arg(long, env = "RPC_MAX_INFLIGHT", default_value_t = 64)]
    rpc_max_inflight: usize,

    /// Max supported transaction version for getBlock (default: 0)
    #[arg(long, env = "RPC_MAX_SUPPORTED_TX_VERSION", default_value_t = 0)]
    rpc_max_supported_tx_version: u8,

    /// Flush RPC inserts every N slots
    #[arg(long, env = "RPC_FLUSH_EVERY_SLOTS", default_value_t = 500)]
    rpc_flush_every_slots: u64,

    /// Log RPC progress every N slots
    #[arg(long, env = "RPC_PROGRESS_EVERY_SLOTS", default_value_t = 100)]
    rpc_progress_every_slots: u64,

    /// Slot range size for RPC discovery requests
    #[arg(long, env = "RPC_DISCOVERY_CHUNK_SLOTS", default_value_t = 10_000)]
    rpc_discovery_chunk_slots: u64,

    /// Bigtable range spec: slots "123:456", epochs "1-10", or single epoch "5"
    #[arg(long, env = "BIGTABLE_RANGE")]
    bigtable_range: Option<String>,

    /// Bigtable slot list file (whitespace-separated slot numbers). Mutually exclusive with --bigtable-range.
    #[arg(long, env = "BIGTABLE_SLOT_FILE", value_name = "PATH")]
    bigtable_slot_file: Option<PathBuf>,

    /// Bigtable instance name
    #[arg(long, env = "BIGTABLE_INSTANCE", default_value = "solana-ledger")]
    bigtable_instance: String,

    /// Bigtable app profile id
    #[arg(long, env = "BIGTABLE_APP_PROFILE", default_value = "default")]
    bigtable_app_profile: String,

    /// Bigtable request timeout (seconds)
    #[arg(long, env = "BIGTABLE_TIMEOUT_SECS")]
    bigtable_timeout_secs: Option<u64>,

    /// Bigtable max gRPC message size (bytes)
    #[arg(long, env = "BIGTABLE_MAX_MESSAGE_BYTES", default_value_t = 64 * 1024 * 1024)]
    bigtable_max_message_bytes: usize,

    /// Bigtable credential JSON file path
    #[arg(long, env = "BIGTABLE_CREDENTIAL_PATH")]
    bigtable_credential_path: Option<String>,

    /// Bigtable credential JSON (stringified)
    #[arg(long, env = "BIGTABLE_CREDENTIAL_JSON")]
    bigtable_credential_json: Option<String>,

    /// Max slots to fetch per Bigtable discovery call
    #[arg(long, env = "BIGTABLE_DISCOVERY_LIMIT", default_value_t = 10_000)]
    bigtable_discovery_limit: usize,

    /// Max slots per Bigtable multi-row fetch
    #[arg(long, env = "BIGTABLE_FETCH_BATCH_SIZE", default_value_t = 500)]
    bigtable_fetch_batch_size: usize,

    /// Max in-flight Bigtable fetch batches
    #[arg(long, env = "BIGTABLE_FETCH_CONCURRENCY", default_value_t = 4)]
    bigtable_fetch_concurrency: usize,

    /// Max in-flight Bigtable insert batches
    #[arg(long, env = "BIGTABLE_INSERT_CONCURRENCY", default_value_t = 1)]
    bigtable_insert_concurrency: usize,

    /// Max in-flight Bigtable decode tasks
    #[arg(
        long,
        env = "BIGTABLE_DECODE_CONCURRENCY",
        default_value_t = default_bigtable_decode_concurrency()
    )]
    bigtable_decode_concurrency: usize,

    /// Log Bigtable progress every N slots
    #[arg(long, env = "BIGTABLE_PROGRESS_EVERY_SLOTS", default_value_t = 10_000)]
    bigtable_progress_every_slots: u64,

    /// ClickHouse HTTP URL
    #[arg(long, env = "CLICKHOUSE_URL", default_value = "http://localhost:8123")]
    clickhouse_url: String,

    /// Host to bind the Prometheus metrics server.
    #[arg(long, env = "METRICS_HOST", default_value = "0.0.0.0")]
    metrics_host: String,

    /// Port to bind the Prometheus metrics server.
    #[arg(long, env = "METRICS_PORT", default_value = "9901")]
    metrics_port: u16,

    /// Seconds since the last successful ClickHouse flush before /health returns 503.
    #[arg(long, env = "HEALTH_STALE_SECS", default_value_t = 120)]
    health_stale_secs: u64,

    /// Optional static cluster label added to all Prometheus metrics.
    #[arg(long, env = "METRICS_CLUSTER_LABEL")]
    metrics_cluster_label: Option<String>,

    /// ClickHouse database name
    #[arg(long, env = "CLICKHOUSE_DATABASE", default_value = "default")]
    clickhouse_database: String,

    /// ClickHouse user
    #[arg(long, env = "CLICKHOUSE_USER", default_value = "default")]
    clickhouse_user: String,

    /// ClickHouse password
    #[arg(long, env = "CLICKHOUSE_PASSWORD", default_value = "")]
    clickhouse_password: String,

    /// Enable ClickHouse async inserts
    #[arg(long, env = "CLICKHOUSE_ASYNC_INSERT", default_value_t = false)]
    clickhouse_async_insert: bool,

    /// Target ClickHouse transactions table (default: distributed)
    #[arg(
        long,
        env = "CLICKHOUSE_TRANSACTIONS_TABLE",
        default_value = "default.transactions"
    )]
    transactions_table: String,

    /// Target ClickHouse blocks metadata table (default: distributed)
    #[arg(
        long,
        env = "CLICKHOUSE_BLOCKS_TABLE",
        default_value = "default.blocks_metadata"
    )]
    blocks_table: String,

    /// Optional ClickHouse PoH entries table (gRPC live ingest only)
    #[arg(
        long,
        env = "CLICKHOUSE_ENTRIES_TABLE",
        default_value = "default.entries"
    )]
    entries_table: Option<String>,

    /// Flush transactions when this many rows are buffered
    #[arg(long, env = "TRANSACTIONS_FLUSH_ROWS", default_value_t = 25_000)]
    transactions_flush_rows: usize,

    /// Flush blocks when this many rows are buffered
    #[arg(long, env = "BLOCKS_FLUSH_ROWS", default_value_t = 2_000)]
    blocks_flush_rows: usize,

    /// Periodic flush interval (seconds)
    #[arg(long, env = "FLUSH_INTERVAL_SECS", default_value_t = 5)]
    flush_interval_secs: u64,

    /// Flush after every block update (disables batching)
    #[arg(long, env = "FLUSH_EVERY_BLOCK", default_value_t = false)]
    flush_every_block: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct Args {
    pub(crate) source: IngestSource,
    pub(crate) endpoint: Option<String>,
    pub(crate) x_token: Option<String>,
    pub(crate) fumarole_endpoint: Option<String>,
    pub(crate) fumarole_x_token: Option<String>,
    pub(crate) fumarole_consumer_group: Option<String>,
    pub(crate) fumarole_create_consumer_group: bool,
    pub(crate) fumarole_data_plane_tcp_connections: u8,
    pub(crate) fumarole_concurrent_download_limit_per_tcp: usize,
    pub(crate) fumarole_data_channel_capacity: usize,
    pub(crate) fumarole_commit_interval_secs: u64,
    pub(crate) fumarole_no_commit: bool,
    pub(crate) commitment: String,
    pub(crate) dragonsmouth_from_slot: Option<FromSlotSpec>,
    pub(crate) fumarole_from_slot: Option<FromSlotSpec>,
    pub(crate) rpc_from_slot: Option<FromSlotSpec>,
    pub(crate) grpc_max_decoding_bytes: usize,
    pub(crate) grpc_http2_adaptive_window: bool,
    pub(crate) grpc_idle_timeout_secs: u64,
    pub(crate) grpc_health_watch_enabled: bool,
    pub(crate) grpc_slot_notifications: bool,
    pub(crate) rpc_url: Option<String>,
    pub(crate) rpc_to_slot: Option<u64>,
    pub(crate) rpc_slot_count: Option<u64>,
    pub(crate) rpc_timeout_secs: u64,
    pub(crate) rpc_retry_backoff_ms: u64,
    pub(crate) rpc_max_inflight: usize,
    pub(crate) rpc_max_supported_tx_version: u8,
    pub(crate) rpc_flush_every_slots: u64,
    pub(crate) rpc_progress_every_slots: u64,
    pub(crate) rpc_discovery_chunk_slots: u64,
    pub(crate) bigtable_range: Option<String>,
    pub(crate) bigtable_slot_file: Option<PathBuf>,
    pub(crate) bigtable_instance: String,
    pub(crate) bigtable_app_profile: String,
    pub(crate) bigtable_timeout_secs: Option<u64>,
    pub(crate) bigtable_max_message_bytes: usize,
    pub(crate) bigtable_credential_path: Option<String>,
    pub(crate) bigtable_credential_json: Option<String>,
    pub(crate) bigtable_discovery_limit: usize,
    pub(crate) bigtable_fetch_batch_size: usize,
    pub(crate) bigtable_fetch_concurrency: usize,
    pub(crate) bigtable_insert_concurrency: usize,
    pub(crate) bigtable_decode_concurrency: usize,
    pub(crate) bigtable_progress_every_slots: u64,
    pub(crate) clickhouse_url: String,
    pub(crate) metrics_host: String,
    pub(crate) metrics_port: u16,
    pub(crate) health_stale_secs: u64,
    pub(crate) metrics_cluster_label: Option<String>,
    pub(crate) clickhouse_database: String,
    pub(crate) clickhouse_user: String,
    pub(crate) clickhouse_password: String,
    pub(crate) clickhouse_async_insert: bool,
    pub(crate) transactions_table: String,
    pub(crate) blocks_table: String,
    pub(crate) entries_table: Option<String>,
    pub(crate) transactions_flush_rows: usize,
    pub(crate) blocks_flush_rows: usize,
    pub(crate) flush_interval_secs: u64,
    pub(crate) flush_every_block: bool,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, rename_all = "kebab-case")]
struct FileConfig {
    source: Option<IngestSource>,
    endpoint: Option<String>,
    #[serde(alias = "x_token")]
    x_token: Option<String>,
    #[serde(alias = "fumarole_endpoint")]
    fumarole_endpoint: Option<String>,
    #[serde(alias = "fumarole_x_token")]
    fumarole_x_token: Option<String>,
    #[serde(alias = "fumarole_consumer_group")]
    fumarole_consumer_group: Option<String>,
    #[serde(alias = "fumarole_create_consumer_group")]
    fumarole_create_consumer_group: Option<bool>,
    #[serde(alias = "fumarole_data_plane_tcp_connections")]
    fumarole_data_plane_tcp_connections: Option<u8>,
    #[serde(alias = "fumarole_concurrent_download_limit_per_tcp")]
    fumarole_concurrent_download_limit_per_tcp: Option<usize>,
    #[serde(alias = "fumarole_data_channel_capacity")]
    fumarole_data_channel_capacity: Option<usize>,
    #[serde(alias = "fumarole_commit_interval_secs")]
    fumarole_commit_interval_secs: Option<u64>,
    #[serde(alias = "fumarole_no_commit")]
    fumarole_no_commit: Option<bool>,
    commitment: Option<String>,
    #[serde(alias = "dragonsmouth_from_slot")]
    dragonsmouth_from_slot: Option<FromSlotSpec>,
    #[serde(alias = "fumarole_from_slot")]
    fumarole_from_slot: Option<FromSlotSpec>,
    #[serde(alias = "rpc_from_slot")]
    rpc_from_slot: Option<FromSlotSpec>,
    #[serde(rename = "from-slot", alias = "from_slot")]
    legacy_from_slot: Option<FromSlotSpec>,
    #[serde(alias = "grpc_max_decoding_bytes")]
    grpc_max_decoding_bytes: Option<usize>,
    #[serde(alias = "grpc_http2_adaptive_window")]
    grpc_http2_adaptive_window: Option<bool>,
    #[serde(alias = "grpc_idle_timeout_secs")]
    grpc_idle_timeout_secs: Option<u64>,
    #[serde(alias = "grpc_health_watch_enabled")]
    grpc_health_watch_enabled: Option<bool>,
    #[serde(alias = "grpc_slot_notifications")]
    grpc_slot_notifications: Option<bool>,
    #[serde(alias = "rpc_url")]
    rpc_url: Option<String>,
    #[serde(alias = "rpc_to_slot")]
    rpc_to_slot: Option<u64>,
    #[serde(alias = "rpc_slot_count")]
    rpc_slot_count: Option<u64>,
    #[serde(alias = "rpc_timeout_secs")]
    rpc_timeout_secs: Option<u64>,
    #[serde(alias = "rpc_retry_backoff_ms")]
    rpc_retry_backoff_ms: Option<u64>,
    #[serde(alias = "rpc_max_inflight")]
    rpc_max_inflight: Option<usize>,
    #[serde(alias = "rpc_max_supported_tx_version")]
    rpc_max_supported_tx_version: Option<u8>,
    #[serde(alias = "rpc_flush_every_slots")]
    rpc_flush_every_slots: Option<u64>,
    #[serde(alias = "rpc_progress_every_slots")]
    rpc_progress_every_slots: Option<u64>,
    #[serde(alias = "rpc_discovery_chunk_slots")]
    rpc_discovery_chunk_slots: Option<u64>,
    #[serde(alias = "bigtable_range")]
    bigtable_range: Option<String>,
    #[serde(alias = "bigtable_slot_file")]
    bigtable_slot_file: Option<PathBuf>,
    #[serde(alias = "bigtable_instance")]
    bigtable_instance: Option<String>,
    #[serde(alias = "bigtable_app_profile")]
    bigtable_app_profile: Option<String>,
    #[serde(alias = "bigtable_timeout_secs")]
    bigtable_timeout_secs: Option<u64>,
    #[serde(alias = "bigtable_max_message_bytes")]
    bigtable_max_message_bytes: Option<usize>,
    #[serde(alias = "bigtable_credential_path")]
    bigtable_credential_path: Option<String>,
    #[serde(alias = "bigtable_credential_json")]
    bigtable_credential_json: Option<String>,
    #[serde(alias = "bigtable_discovery_limit")]
    bigtable_discovery_limit: Option<usize>,
    #[serde(alias = "bigtable_fetch_batch_size")]
    bigtable_fetch_batch_size: Option<usize>,
    #[serde(alias = "bigtable_fetch_concurrency")]
    bigtable_fetch_concurrency: Option<usize>,
    #[serde(alias = "bigtable_insert_concurrency")]
    bigtable_insert_concurrency: Option<usize>,
    #[serde(alias = "bigtable_decode_concurrency")]
    bigtable_decode_concurrency: Option<usize>,
    #[serde(alias = "bigtable_progress_every_slots")]
    bigtable_progress_every_slots: Option<u64>,
    #[serde(alias = "clickhouse_url")]
    clickhouse_url: Option<String>,
    #[serde(alias = "metrics_host")]
    metrics_host: Option<String>,
    #[serde(alias = "metrics_port")]
    metrics_port: Option<u16>,
    #[serde(alias = "health_stale_secs")]
    health_stale_secs: Option<u64>,
    #[serde(alias = "metrics_cluster_label")]
    metrics_cluster_label: Option<String>,
    #[serde(alias = "clickhouse_database")]
    clickhouse_database: Option<String>,
    #[serde(alias = "clickhouse_user")]
    clickhouse_user: Option<String>,
    #[serde(alias = "clickhouse_password")]
    clickhouse_password: Option<String>,
    #[serde(alias = "clickhouse_async_insert")]
    clickhouse_async_insert: Option<bool>,
    #[serde(alias = "transactions_table")]
    transactions_table: Option<String>,
    #[serde(alias = "blocks_table")]
    blocks_table: Option<String>,
    #[serde(alias = "entries_table")]
    entries_table: Option<String>,
    #[serde(alias = "transactions_flush_rows")]
    transactions_flush_rows: Option<usize>,
    #[serde(alias = "blocks_flush_rows")]
    blocks_flush_rows: Option<usize>,
    #[serde(alias = "flush_interval_secs")]
    flush_interval_secs: Option<u64>,
    #[serde(alias = "flush_every_block")]
    flush_every_block: Option<bool>,
}

pub(crate) fn resolve_args() -> Result<Args> {
    let matches = CliArgs::command().get_matches();
    let cli = CliArgs::from_arg_matches(&matches)?;
    let file_config = load_config(cli.config.as_deref())?.unwrap_or_default();

    reject_legacy_from_slot(cli.legacy_from_slot, file_config.legacy_from_slot)?;

    let source = merge_option(&matches, "source", cli.source, file_config.source)
        .ok_or_else(|| anyhow!("missing --source / SUPERBANK_SOURCE / config source"))?;
    let endpoint = merge_option(&matches, "endpoint", cli.endpoint, file_config.endpoint);

    let args = Args {
        source,
        endpoint,
        x_token: merge_option(&matches, "x_token", cli.x_token, file_config.x_token),
        fumarole_endpoint: merge_option(
            &matches,
            "fumarole_endpoint",
            cli.fumarole_endpoint,
            file_config.fumarole_endpoint,
        ),
        fumarole_x_token: merge_option(
            &matches,
            "fumarole_x_token",
            cli.fumarole_x_token,
            file_config.fumarole_x_token,
        ),
        fumarole_consumer_group: merge_option(
            &matches,
            "fumarole_consumer_group",
            cli.fumarole_consumer_group,
            file_config.fumarole_consumer_group,
        ),
        fumarole_create_consumer_group: merge_value(
            &matches,
            "fumarole_create_consumer_group",
            cli.fumarole_create_consumer_group,
            file_config.fumarole_create_consumer_group,
        ),
        fumarole_data_plane_tcp_connections: merge_value(
            &matches,
            "fumarole_data_plane_tcp_connections",
            cli.fumarole_data_plane_tcp_connections,
            file_config.fumarole_data_plane_tcp_connections,
        ),
        fumarole_concurrent_download_limit_per_tcp: merge_value(
            &matches,
            "fumarole_concurrent_download_limit_per_tcp",
            cli.fumarole_concurrent_download_limit_per_tcp,
            file_config.fumarole_concurrent_download_limit_per_tcp,
        ),
        fumarole_data_channel_capacity: merge_value(
            &matches,
            "fumarole_data_channel_capacity",
            cli.fumarole_data_channel_capacity,
            file_config.fumarole_data_channel_capacity,
        ),
        fumarole_commit_interval_secs: merge_value(
            &matches,
            "fumarole_commit_interval_secs",
            cli.fumarole_commit_interval_secs,
            file_config.fumarole_commit_interval_secs,
        ),
        fumarole_no_commit: merge_value(
            &matches,
            "fumarole_no_commit",
            cli.fumarole_no_commit,
            file_config.fumarole_no_commit,
        ),
        commitment: merge_value(
            &matches,
            "commitment",
            cli.commitment,
            file_config.commitment,
        ),
        dragonsmouth_from_slot: merge_option(
            &matches,
            "dragonsmouth_from_slot",
            cli.dragonsmouth_from_slot,
            file_config.dragonsmouth_from_slot,
        ),
        fumarole_from_slot: merge_option(
            &matches,
            "fumarole_from_slot",
            cli.fumarole_from_slot,
            file_config.fumarole_from_slot,
        ),
        rpc_from_slot: merge_option(
            &matches,
            "rpc_from_slot",
            cli.rpc_from_slot,
            file_config.rpc_from_slot,
        ),
        grpc_max_decoding_bytes: merge_value(
            &matches,
            "grpc_max_decoding_bytes",
            cli.grpc_max_decoding_bytes,
            file_config.grpc_max_decoding_bytes,
        ),
        grpc_http2_adaptive_window: merge_value(
            &matches,
            "grpc_http2_adaptive_window",
            cli.grpc_http2_adaptive_window,
            file_config.grpc_http2_adaptive_window,
        ),
        grpc_idle_timeout_secs: merge_value(
            &matches,
            "grpc_idle_timeout_secs",
            cli.grpc_idle_timeout_secs,
            file_config.grpc_idle_timeout_secs,
        ),
        grpc_health_watch_enabled: merge_value(
            &matches,
            "grpc_health_watch_enabled",
            cli.grpc_health_watch_enabled,
            file_config.grpc_health_watch_enabled,
        ),
        grpc_slot_notifications: merge_value(
            &matches,
            "grpc_slot_notifications",
            cli.grpc_slot_notifications,
            file_config.grpc_slot_notifications,
        ),
        rpc_url: merge_option(&matches, "rpc_url", cli.rpc_url, file_config.rpc_url),
        rpc_to_slot: merge_option(
            &matches,
            "rpc_to_slot",
            cli.rpc_to_slot,
            file_config.rpc_to_slot,
        ),
        rpc_slot_count: merge_option(
            &matches,
            "rpc_slot_count",
            cli.rpc_slot_count,
            file_config.rpc_slot_count,
        ),
        rpc_timeout_secs: merge_value(
            &matches,
            "rpc_timeout_secs",
            cli.rpc_timeout_secs,
            file_config.rpc_timeout_secs,
        ),
        rpc_retry_backoff_ms: merge_value(
            &matches,
            "rpc_retry_backoff_ms",
            cli.rpc_retry_backoff_ms,
            file_config.rpc_retry_backoff_ms,
        ),
        rpc_max_inflight: merge_value(
            &matches,
            "rpc_max_inflight",
            cli.rpc_max_inflight,
            file_config.rpc_max_inflight,
        ),
        rpc_max_supported_tx_version: merge_value(
            &matches,
            "rpc_max_supported_tx_version",
            cli.rpc_max_supported_tx_version,
            file_config.rpc_max_supported_tx_version,
        ),
        rpc_flush_every_slots: merge_value(
            &matches,
            "rpc_flush_every_slots",
            cli.rpc_flush_every_slots,
            file_config.rpc_flush_every_slots,
        ),
        rpc_progress_every_slots: merge_value(
            &matches,
            "rpc_progress_every_slots",
            cli.rpc_progress_every_slots,
            file_config.rpc_progress_every_slots,
        ),
        rpc_discovery_chunk_slots: merge_value(
            &matches,
            "rpc_discovery_chunk_slots",
            cli.rpc_discovery_chunk_slots,
            file_config.rpc_discovery_chunk_slots,
        ),
        bigtable_range: merge_option(
            &matches,
            "bigtable_range",
            cli.bigtable_range,
            file_config.bigtable_range,
        ),
        bigtable_slot_file: merge_option(
            &matches,
            "bigtable_slot_file",
            cli.bigtable_slot_file,
            file_config.bigtable_slot_file,
        ),
        bigtable_instance: merge_value(
            &matches,
            "bigtable_instance",
            cli.bigtable_instance,
            file_config.bigtable_instance,
        ),
        bigtable_app_profile: merge_value(
            &matches,
            "bigtable_app_profile",
            cli.bigtable_app_profile,
            file_config.bigtable_app_profile,
        ),
        bigtable_timeout_secs: merge_option(
            &matches,
            "bigtable_timeout_secs",
            cli.bigtable_timeout_secs,
            file_config.bigtable_timeout_secs,
        ),
        bigtable_max_message_bytes: merge_value(
            &matches,
            "bigtable_max_message_bytes",
            cli.bigtable_max_message_bytes,
            file_config.bigtable_max_message_bytes,
        ),
        bigtable_credential_path: merge_option(
            &matches,
            "bigtable_credential_path",
            cli.bigtable_credential_path,
            file_config.bigtable_credential_path,
        ),
        bigtable_credential_json: merge_option(
            &matches,
            "bigtable_credential_json",
            cli.bigtable_credential_json,
            file_config.bigtable_credential_json,
        ),
        bigtable_discovery_limit: merge_value(
            &matches,
            "bigtable_discovery_limit",
            cli.bigtable_discovery_limit,
            file_config.bigtable_discovery_limit,
        ),
        bigtable_fetch_batch_size: merge_value(
            &matches,
            "bigtable_fetch_batch_size",
            cli.bigtable_fetch_batch_size,
            file_config.bigtable_fetch_batch_size,
        ),
        bigtable_fetch_concurrency: merge_value(
            &matches,
            "bigtable_fetch_concurrency",
            cli.bigtable_fetch_concurrency,
            file_config.bigtable_fetch_concurrency,
        ),
        bigtable_insert_concurrency: merge_value(
            &matches,
            "bigtable_insert_concurrency",
            cli.bigtable_insert_concurrency,
            file_config.bigtable_insert_concurrency,
        ),
        bigtable_decode_concurrency: merge_value(
            &matches,
            "bigtable_decode_concurrency",
            cli.bigtable_decode_concurrency,
            file_config.bigtable_decode_concurrency,
        ),
        bigtable_progress_every_slots: merge_value(
            &matches,
            "bigtable_progress_every_slots",
            cli.bigtable_progress_every_slots,
            file_config.bigtable_progress_every_slots,
        ),
        clickhouse_url: merge_value(
            &matches,
            "clickhouse_url",
            cli.clickhouse_url,
            file_config.clickhouse_url,
        ),
        metrics_host: merge_value(
            &matches,
            "metrics_host",
            cli.metrics_host,
            file_config.metrics_host,
        ),
        metrics_port: merge_value(
            &matches,
            "metrics_port",
            cli.metrics_port,
            file_config.metrics_port,
        ),
        health_stale_secs: merge_value(
            &matches,
            "health_stale_secs",
            cli.health_stale_secs,
            file_config.health_stale_secs,
        ),
        metrics_cluster_label: merge_option(
            &matches,
            "metrics_cluster_label",
            cli.metrics_cluster_label,
            file_config.metrics_cluster_label,
        ),
        clickhouse_database: merge_value(
            &matches,
            "clickhouse_database",
            cli.clickhouse_database,
            file_config.clickhouse_database,
        ),
        clickhouse_user: merge_value(
            &matches,
            "clickhouse_user",
            cli.clickhouse_user,
            file_config.clickhouse_user,
        ),
        clickhouse_password: merge_value(
            &matches,
            "clickhouse_password",
            cli.clickhouse_password,
            file_config.clickhouse_password,
        ),
        clickhouse_async_insert: merge_value(
            &matches,
            "clickhouse_async_insert",
            cli.clickhouse_async_insert,
            file_config.clickhouse_async_insert,
        ),
        transactions_table: merge_value(
            &matches,
            "transactions_table",
            cli.transactions_table,
            file_config.transactions_table,
        ),
        blocks_table: merge_value(
            &matches,
            "blocks_table",
            cli.blocks_table,
            file_config.blocks_table,
        ),
        entries_table: merge_option(
            &matches,
            "entries_table",
            cli.entries_table,
            file_config.entries_table,
        ),
        transactions_flush_rows: merge_value(
            &matches,
            "transactions_flush_rows",
            cli.transactions_flush_rows,
            file_config.transactions_flush_rows,
        ),
        blocks_flush_rows: merge_value(
            &matches,
            "blocks_flush_rows",
            cli.blocks_flush_rows,
            file_config.blocks_flush_rows,
        ),
        flush_interval_secs: merge_value(
            &matches,
            "flush_interval_secs",
            cli.flush_interval_secs,
            file_config.flush_interval_secs,
        ),
        flush_every_block: merge_value(
            &matches,
            "flush_every_block",
            cli.flush_every_block,
            file_config.flush_every_block,
        ),
    };

    validate_args(&args)?;
    Ok(args)
}

fn validate_args(args: &Args) -> Result<()> {
    validate_start_slot_ownership(args)?;

    match args.source {
        IngestSource::Fumarole => {
            if args.fumarole_endpoint.as_deref().is_none_or(str::is_empty) {
                return Err(anyhow!(
                    "fumarole source requires --fumarole-endpoint / FUMAROLE_ENDPOINT / config fumarole_endpoint"
                ));
            }
            if args
                .fumarole_consumer_group
                .as_deref()
                .is_none_or(str::is_empty)
            {
                return Err(anyhow!(
                    "fumarole source requires --fumarole-consumer-group / FUMAROLE_CONSUMER_GROUP / config fumarole_consumer_group"
                ));
            }
            if args.grpc_max_decoding_bytes == 0 {
                return Err(anyhow!(
                    "fumarole max-decoding-bytes must be greater than 0"
                ));
            }
            if args.grpc_idle_timeout_secs == 0 {
                return Err(anyhow!("fumarole idle-timeout-secs must be greater than 0"));
            }
            if args.fumarole_data_plane_tcp_connections == 0 {
                return Err(anyhow!(
                    "fumarole data-plane-tcp-connections must be greater than 0"
                ));
            }
            if args.fumarole_data_plane_tcp_connections > 20 {
                return Err(anyhow!(
                    "fumarole data-plane-tcp-connections must be less than or equal to 20"
                ));
            }
            if args.fumarole_concurrent_download_limit_per_tcp == 0 {
                return Err(anyhow!(
                    "fumarole concurrent-download-limit-per-tcp must be greater than 0"
                ));
            }
            if args.fumarole_data_channel_capacity == 0 {
                return Err(anyhow!(
                    "fumarole data-channel-capacity must be greater than 0"
                ));
            }
            if args.fumarole_commit_interval_secs == 0 {
                return Err(anyhow!(
                    "fumarole commit-interval-secs must be greater than 0"
                ));
            }
        }
        IngestSource::Grpc => {
            if args.endpoint.is_none() {
                return Err(anyhow!(
                    "grpc source requires --endpoint / DRAGONSMOUTH_ENDPOINT / config endpoint"
                ));
            }
            if args.grpc_max_decoding_bytes == 0 {
                return Err(anyhow!("grpc max-decoding-bytes must be greater than 0"));
            }
            if args.grpc_idle_timeout_secs == 0 {
                return Err(anyhow!("grpc idle-timeout-secs must be greater than 0"));
            }
        }
        IngestSource::Rpc => {
            if args.rpc_url.is_none() {
                return Err(anyhow!(
                    "rpc source requires --rpc-url / RPC_URL / config rpc_url"
                ));
            }
            if args.rpc_from_slot.is_none() {
                return Err(anyhow!(
                    "rpc source requires --rpc-from-slot / RPC_FROM_SLOT / config rpc-from-slot"
                ));
            }
            if args.rpc_to_slot.is_some() && args.rpc_slot_count.is_some() {
                return Err(anyhow!(
                    "rpc source requires either --to-slot or --slot-count (not both)"
                ));
            }
            if args.rpc_to_slot.is_none() && args.rpc_slot_count.is_none() {
                return Err(anyhow!(
                    "rpc source requires --to-slot or --slot-count to define a range"
                ));
            }
            if let Some(count) = args.rpc_slot_count
                && count == 0
            {
                return Err(anyhow!("rpc slot-count must be greater than 0"));
            }
            if args.rpc_max_inflight == 0 {
                return Err(anyhow!("rpc max-inflight must be greater than 0"));
            }
            if args.rpc_flush_every_slots == 0 {
                return Err(anyhow!("rpc flush-every-slots must be greater than 0"));
            }
            if args.rpc_progress_every_slots == 0 {
                return Err(anyhow!("rpc progress-every-slots must be greater than 0"));
            }
            if args.rpc_discovery_chunk_slots == 0 {
                return Err(anyhow!("rpc discovery-chunk-slots must be greater than 0"));
            }
        }
        IngestSource::Bigtable => {
            let has_range = args.bigtable_range.is_some();
            let has_slot_file = args.bigtable_slot_file.is_some();
            if has_range == has_slot_file {
                return Err(anyhow!(
                    "bigtable source requires exactly one of --bigtable-range or --bigtable-slot-file"
                ));
            }
            if args.bigtable_credential_json.is_some() && args.bigtable_credential_path.is_some() {
                return Err(anyhow!(
                    "bigtable credential json and credential path are mutually exclusive"
                ));
            }
            if let Some(timeout) = args.bigtable_timeout_secs
                && timeout == 0
            {
                return Err(anyhow!("bigtable timeout must be greater than 0"));
            }
            if args.bigtable_max_message_bytes == 0 {
                return Err(anyhow!("bigtable max-message-bytes must be greater than 0"));
            }
            if args.bigtable_discovery_limit == 0 {
                return Err(anyhow!("bigtable discovery-limit must be greater than 0"));
            }
            if args.bigtable_fetch_batch_size == 0 {
                return Err(anyhow!("bigtable fetch-batch-size must be greater than 0"));
            }
            if args.bigtable_fetch_concurrency == 0 {
                return Err(anyhow!("bigtable fetch-concurrency must be greater than 0"));
            }
            if args.bigtable_insert_concurrency == 0 {
                return Err(anyhow!(
                    "bigtable insert-concurrency must be greater than 0"
                ));
            }
            if args.bigtable_decode_concurrency == 0 {
                return Err(anyhow!(
                    "bigtable decode-concurrency must be greater than 0"
                ));
            }
            if args.bigtable_progress_every_slots == 0 {
                return Err(anyhow!(
                    "bigtable progress-every-slots must be greater than 0"
                ));
            }
            if let Some(range) = args.bigtable_range.as_ref() {
                let spec = crate::range::parse_range_spec(range)?;
                if spec.is_epoch() && args.rpc_url.is_none() {
                    return Err(anyhow!(
                        "bigtable epoch ranges require --rpc-url / RPC_URL for epoch schedule"
                    ));
                }
            }
        }
    }
    Ok(())
}

const DRAGONSMOUTH_FROM_SLOT_LABEL: &str =
    "--dragonsmouth-from-slot / DRAGONSMOUTH_FROM_SLOT / config dragonsmouth-from-slot";
const FUMAROLE_FROM_SLOT_LABEL: &str =
    "--fumarole-from-slot / FUMAROLE_FROM_SLOT / config fumarole-from-slot";
const RPC_FROM_SLOT_LABEL: &str = "--rpc-from-slot / RPC_FROM_SLOT / config rpc-from-slot";

fn reject_legacy_from_slot(
    cli_value: Option<FromSlotSpec>,
    config_value: Option<FromSlotSpec>,
) -> Result<()> {
    if cli_value.is_some() || config_value.is_some() {
        return Err(anyhow!(
            "from-slot is no longer supported; use {RPC_FROM_SLOT_LABEL} for RPC, {DRAGONSMOUTH_FROM_SLOT_LABEL} for gRPC, or {FUMAROLE_FROM_SLOT_LABEL} for Fumarole"
        ));
    }
    Ok(())
}

fn validate_start_slot_ownership(args: &Args) -> Result<()> {
    match args.source {
        IngestSource::Fumarole => {
            reject_start_slot_for_source(
                args.dragonsmouth_from_slot,
                args.source,
                DRAGONSMOUTH_FROM_SLOT_LABEL,
                FUMAROLE_FROM_SLOT_LABEL,
            )?;
            reject_start_slot_for_source(
                args.rpc_from_slot,
                args.source,
                RPC_FROM_SLOT_LABEL,
                FUMAROLE_FROM_SLOT_LABEL,
            )?;
        }
        IngestSource::Grpc => {
            reject_start_slot_for_source(
                args.fumarole_from_slot,
                args.source,
                FUMAROLE_FROM_SLOT_LABEL,
                DRAGONSMOUTH_FROM_SLOT_LABEL,
            )?;
            reject_start_slot_for_source(
                args.rpc_from_slot,
                args.source,
                RPC_FROM_SLOT_LABEL,
                DRAGONSMOUTH_FROM_SLOT_LABEL,
            )?;
        }
        IngestSource::Rpc => {
            reject_start_slot_for_source(
                args.dragonsmouth_from_slot,
                args.source,
                DRAGONSMOUTH_FROM_SLOT_LABEL,
                RPC_FROM_SLOT_LABEL,
            )?;
            reject_start_slot_for_source(
                args.fumarole_from_slot,
                args.source,
                FUMAROLE_FROM_SLOT_LABEL,
                RPC_FROM_SLOT_LABEL,
            )?;
        }
        IngestSource::Bigtable => {
            let bigtable_range_label =
                "--bigtable-range / BIGTABLE_RANGE or --bigtable-slot-file / BIGTABLE_SLOT_FILE";
            reject_start_slot_for_source(
                args.dragonsmouth_from_slot,
                args.source,
                DRAGONSMOUTH_FROM_SLOT_LABEL,
                bigtable_range_label,
            )?;
            reject_start_slot_for_source(
                args.fumarole_from_slot,
                args.source,
                FUMAROLE_FROM_SLOT_LABEL,
                bigtable_range_label,
            )?;
            reject_start_slot_for_source(
                args.rpc_from_slot,
                args.source,
                RPC_FROM_SLOT_LABEL,
                bigtable_range_label,
            )?;
        }
    }
    Ok(())
}

fn reject_start_slot_for_source(
    value: Option<FromSlotSpec>,
    source: IngestSource,
    provided: &str,
    use_instead: &str,
) -> Result<()> {
    if value.is_some() {
        return Err(anyhow!(
            "{} source does not accept {}; use {}",
            source.as_str(),
            provided,
            use_instead
        ));
    }
    Ok(())
}

fn load_config(path: Option<&Path>) -> Result<Option<FileConfig>> {
    let Some(path) = path else {
        return Ok(None);
    };

    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("read config file {}", path.display()))?;
    let config = serde_yaml::from_str::<FileConfig>(&contents)
        .with_context(|| format!("parse config file {}", path.display()))?;
    Ok(Some(config))
}

fn should_use_config(matches: &ArgMatches, name: &str) -> bool {
    matches
        .value_source(name)
        .is_none_or(|source| matches!(source, ValueSource::DefaultValue))
}

fn merge_value<T>(matches: &ArgMatches, name: &str, cli: T, config: Option<T>) -> T {
    if should_use_config(matches, name) {
        config.unwrap_or(cli)
    } else {
        cli
    }
}

fn merge_option<T>(
    matches: &ArgMatches,
    name: &str,
    cli: Option<T>,
    config: Option<T>,
) -> Option<T> {
    if should_use_config(matches, name) {
        cli.or(config)
    } else {
        cli
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn clickhouse_async_insert_defaults_to_false() {
        let matches = CliArgs::command().get_matches_from(["superbank", "--source", "grpc"]);
        let cli = CliArgs::from_arg_matches(&matches).expect("parse cli args");

        assert!(!cli.clickhouse_async_insert);
    }

    #[test]
    fn file_config_parses_clickhouse_async_insert() {
        let config: FileConfig =
            serde_yaml::from_str("clickhouse-async-insert: true\n").expect("parse config");

        assert_eq!(config.clickhouse_async_insert, Some(true));
    }

    #[test]
    fn grpc_health_watch_enabled_defaults_to_true() {
        let matches = CliArgs::command().get_matches_from(["superbank", "--source", "grpc"]);
        let cli = CliArgs::from_arg_matches(&matches).expect("parse cli args");

        assert!(cli.grpc_health_watch_enabled);
    }

    #[test]
    fn grpc_http2_adaptive_window_defaults_to_false() {
        let matches = CliArgs::command().get_matches_from(["superbank", "--source", "grpc"]);
        let cli = CliArgs::from_arg_matches(&matches).expect("parse cli args");

        assert!(!cli.grpc_http2_adaptive_window);
    }

    #[test]
    fn cli_parses_explicit_false_for_grpc_health_watch_enabled() {
        let matches = CliArgs::command().get_matches_from([
            "superbank",
            "--source",
            "grpc",
            "--grpc-health-watch-enabled=false",
        ]);
        let cli = CliArgs::from_arg_matches(&matches).expect("parse cli args");

        assert!(!cli.grpc_health_watch_enabled);
    }

    #[test]
    fn file_config_parses_grpc_stream_options() {
        let config: FileConfig = serde_yaml::from_str(
            r#"
grpc-http2-adaptive-window: true
grpc-idle-timeout-secs: 45
grpc-health-watch-enabled: false
"#,
        )
        .expect("parse config");

        assert_eq!(config.grpc_http2_adaptive_window, Some(true));
        assert_eq!(config.grpc_idle_timeout_secs, Some(45));
        assert_eq!(config.grpc_health_watch_enabled, Some(false));
    }

    #[test]
    fn cli_parses_fumarole_options() {
        let matches = CliArgs::command().get_matches_from([
            "superbank",
            "--source",
            "fumarole",
            "--fumarole-endpoint",
            "https://fumarole.example:443",
            "--fumarole-x-token",
            "secret",
            "--fumarole-consumer-group",
            "superbank-mainnet",
            "--fumarole-create-consumer-group=true",
            "--fumarole-data-plane-tcp-connections",
            "8",
            "--fumarole-concurrent-download-limit-per-tcp",
            "3",
            "--fumarole-data-channel-capacity",
            "1234",
            "--fumarole-commit-interval-secs",
            "2",
        ]);
        let cli = CliArgs::from_arg_matches(&matches).expect("parse cli args");

        assert_eq!(cli.source, Some(IngestSource::Fumarole));
        assert_eq!(
            cli.fumarole_endpoint.as_deref(),
            Some("https://fumarole.example:443")
        );
        assert_eq!(cli.fumarole_x_token.as_deref(), Some("secret"));
        assert_eq!(
            cli.fumarole_consumer_group.as_deref(),
            Some("superbank-mainnet")
        );
        assert!(cli.fumarole_create_consumer_group);
        assert_eq!(cli.fumarole_data_plane_tcp_connections, 8);
        assert_eq!(cli.fumarole_concurrent_download_limit_per_tcp, 3);
        assert_eq!(cli.fumarole_data_channel_capacity, 1234);
        assert_eq!(cli.fumarole_commit_interval_secs, 2);
    }

    #[test]
    fn file_config_parses_fumarole_options() {
        let config: FileConfig = serde_yaml::from_str(
            r#"
source: fumarole
fumarole-endpoint: https://fumarole.example:443
fumarole-x-token: secret
fumarole-consumer-group: superbank-mainnet
fumarole-create-consumer-group: true
fumarole-data-plane-tcp-connections: 8
fumarole-concurrent-download-limit-per-tcp: 3
fumarole-data-channel-capacity: 1234
fumarole-commit-interval-secs: 2
fumarole-no-commit: true
"#,
        )
        .expect("parse config");

        assert_eq!(config.source, Some(IngestSource::Fumarole));
        assert_eq!(
            config.fumarole_endpoint.as_deref(),
            Some("https://fumarole.example:443")
        );
        assert_eq!(config.fumarole_x_token.as_deref(), Some("secret"));
        assert_eq!(
            config.fumarole_consumer_group.as_deref(),
            Some("superbank-mainnet")
        );
        assert_eq!(config.fumarole_create_consumer_group, Some(true));
        assert_eq!(config.fumarole_data_plane_tcp_connections, Some(8));
        assert_eq!(config.fumarole_concurrent_download_limit_per_tcp, Some(3));
        assert_eq!(config.fumarole_data_channel_capacity, Some(1234));
        assert_eq!(config.fumarole_commit_interval_secs, Some(2));
        assert_eq!(config.fumarole_no_commit, Some(true));
    }

    #[test]
    fn cli_parses_source_specific_from_slots() {
        let matches = CliArgs::command().get_matches_from([
            "superbank",
            "--source",
            "grpc",
            "--dragonsmouth-from-slot",
            "*",
            "--fumarole-from-slot",
            "123",
            "--rpc-from-slot",
            "456",
        ]);
        let cli = CliArgs::from_arg_matches(&matches).expect("parse cli args");

        assert_eq!(cli.dragonsmouth_from_slot, Some(FromSlotSpec::LatestDb));
        assert_eq!(cli.fumarole_from_slot, Some(FromSlotSpec::Slot(123)));
        assert_eq!(cli.rpc_from_slot, Some(FromSlotSpec::Slot(456)));
    }

    #[test]
    fn file_config_parses_source_specific_from_slots() {
        let config: FileConfig = serde_yaml::from_str(
            r#"
dragonsmouth-from-slot: "*"
fumarole-from-slot: 123
rpc-from-slot: 456
"#,
        )
        .expect("parse config");

        assert_eq!(config.dragonsmouth_from_slot, Some(FromSlotSpec::LatestDb));
        assert_eq!(config.fumarole_from_slot, Some(FromSlotSpec::Slot(123)));
        assert_eq!(config.rpc_from_slot, Some(FromSlotSpec::Slot(456)));
    }

    #[test]
    fn legacy_from_slot_is_detected_for_migration_error() {
        let matches = CliArgs::command().get_matches_from([
            "superbank",
            "--source",
            "rpc",
            "--from-slot",
            "123",
        ]);
        let cli = CliArgs::from_arg_matches(&matches).expect("parse cli args");
        let config: FileConfig = serde_yaml::from_str("from-slot: 456\n").expect("parse config");

        assert_eq!(cli.legacy_from_slot, Some(FromSlotSpec::Slot(123)));
        assert_eq!(config.legacy_from_slot, Some(FromSlotSpec::Slot(456)));
        let err = reject_legacy_from_slot(cli.legacy_from_slot, None).expect_err("legacy cli");

        assert!(err.to_string().contains("from-slot is no longer supported"));
        assert!(err.to_string().contains("RPC_FROM_SLOT"));
        assert!(err.to_string().contains("FUMAROLE_FROM_SLOT"));
        assert!(err.to_string().contains("DRAGONSMOUTH_FROM_SLOT"));
    }

    #[test]
    fn validate_accepts_minimal_fumarole_source() {
        let args = fumarole_args();

        validate_args(&args).expect("valid fumarole args");
    }

    #[test]
    fn validate_accepts_fumarole_from_slot() {
        let mut args = fumarole_args();
        args.fumarole_create_consumer_group = true;
        args.fumarole_from_slot = Some(FromSlotSpec::Slot(123));

        validate_args(&args).expect("valid fumarole args");
    }

    #[test]
    fn validate_accepts_grpc_dragonsmouth_from_slot() {
        let mut args = fumarole_args();
        args.source = IngestSource::Grpc;
        args.endpoint = Some("https://dragonsmouth.example:443".to_string());
        args.dragonsmouth_from_slot = Some(FromSlotSpec::LatestDb);

        validate_args(&args).expect("valid grpc args");
    }

    #[test]
    fn validate_accepts_rpc_from_slot() {
        let mut args = fumarole_args();
        args.source = IngestSource::Rpc;
        args.rpc_url = Some("https://api.mainnet-beta.solana.com".to_string());
        args.rpc_from_slot = Some(FromSlotSpec::Slot(200_000_000));
        args.rpc_slot_count = Some(100);

        validate_args(&args).expect("valid rpc args");
    }

    #[test]
    fn validate_rejects_rpc_without_rpc_from_slot() {
        let mut args = fumarole_args();
        args.source = IngestSource::Rpc;
        args.rpc_url = Some("https://api.mainnet-beta.solana.com".to_string());
        args.rpc_slot_count = Some(100);

        let err = validate_args(&args).expect_err("missing rpc from slot");

        assert!(
            err.to_string()
                .contains("rpc source requires --rpc-from-slot")
        );
    }

    #[test]
    fn validate_rejects_rpc_with_dragonsmouth_from_slot() {
        let mut args = fumarole_args();
        args.source = IngestSource::Rpc;
        args.rpc_url = Some("https://api.mainnet-beta.solana.com".to_string());
        args.rpc_from_slot = Some(FromSlotSpec::Slot(200_000_000));
        args.dragonsmouth_from_slot = Some(FromSlotSpec::LatestDb);
        args.rpc_slot_count = Some(100);

        let err = validate_args(&args).expect_err("wrong source slot option");

        assert!(err.to_string().contains("rpc source does not accept"));
        assert!(err.to_string().contains("DRAGONSMOUTH_FROM_SLOT"));
        assert!(err.to_string().contains("RPC_FROM_SLOT"));
    }

    #[test]
    fn validate_rejects_fumarole_with_rpc_from_slot() {
        let mut args = fumarole_args();
        args.rpc_from_slot = Some(FromSlotSpec::Slot(123));

        let err = validate_args(&args).expect_err("wrong source slot option");

        assert!(err.to_string().contains("fumarole source does not accept"));
        assert!(err.to_string().contains("RPC_FROM_SLOT"));
        assert!(err.to_string().contains("FUMAROLE_FROM_SLOT"));
    }

    #[test]
    fn validate_rejects_bigtable_with_source_from_slot() {
        let mut args = fumarole_args();
        args.source = IngestSource::Bigtable;
        args.bigtable_range = Some("1-2".to_string());
        args.rpc_url = Some("https://api.mainnet-beta.solana.com".to_string());
        args.rpc_from_slot = Some(FromSlotSpec::Slot(123));

        let err = validate_args(&args).expect_err("wrong source slot option");

        assert!(err.to_string().contains("bigtable source does not accept"));
        assert!(err.to_string().contains("RPC_FROM_SLOT"));
        assert!(err.to_string().contains("BIGTABLE_RANGE"));
    }

    #[test]
    fn validate_rejects_fumarole_without_consumer_group() {
        let mut args = fumarole_args();
        args.fumarole_consumer_group = None;

        let err = validate_args(&args).expect_err("missing consumer group");

        assert!(
            err.to_string()
                .contains("fumarole source requires --fumarole-consumer-group")
        );
    }

    #[test]
    fn validate_rejects_fumarole_with_only_dragonsmouth_endpoint() {
        let mut args = fumarole_args();
        args.endpoint = Some("https://dragonsmouth.example:443".to_string());
        args.fumarole_endpoint = None;

        let err = validate_args(&args).expect_err("missing fumarole endpoint");

        assert!(
            err.to_string()
                .contains("fumarole source requires --fumarole-endpoint")
        );
    }

    #[test]
    fn validate_rejects_fumarole_parallelism_above_client_limit() {
        let mut args = fumarole_args();
        args.fumarole_data_plane_tcp_connections = 21;

        let err = validate_args(&args).expect_err("too many connections");

        assert!(
            err.to_string()
                .contains("data-plane-tcp-connections must be less than or equal to 20")
        );
    }

    #[test]
    fn validate_rejects_zero_fumarole_data_channel_capacity() {
        let mut args = fumarole_args();
        args.fumarole_data_channel_capacity = 0;

        let err = validate_args(&args).expect_err("zero channel capacity");

        assert!(
            err.to_string()
                .contains("data-channel-capacity must be greater than 0")
        );
    }

    fn fumarole_args() -> Args {
        Args {
            source: IngestSource::Fumarole,
            endpoint: None,
            x_token: None,
            fumarole_endpoint: Some("https://fumarole.example:443".to_string()),
            fumarole_x_token: Some("secret".to_string()),
            fumarole_consumer_group: Some("superbank-mainnet".to_string()),
            fumarole_create_consumer_group: false,
            fumarole_data_plane_tcp_connections: 4,
            fumarole_concurrent_download_limit_per_tcp: 2,
            fumarole_data_channel_capacity: 4096,
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
            bigtable_decode_concurrency: default_bigtable_decode_concurrency(),
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
            entries_table: Some("default.entries".to_string()),
            transactions_flush_rows: 25_000,
            blocks_flush_rows: 2_000,
            flush_interval_secs: 5,
            flush_every_block: false,
        }
    }
}
