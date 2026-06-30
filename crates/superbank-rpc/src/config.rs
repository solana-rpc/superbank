// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::str::FromStr;

use clap::{ArgAction, Parser};
use solana_sdk::pubkey::Pubkey;

const METRICS_CAPTURE_HEADER_X_ENDPOINT: &str = "X-Endpoint";
const METRICS_CAPTURE_HEADER_X_RPC_NODE: &str = "X-RPC-Node";
const METRICS_CAPTURE_HEADER_X_SUBSCRIPTION_ID: &str = "X-Subscription-ID";
const METRICS_CAPTURE_HEADER_X_ACCOUNT_ID: &str = "X-Account-ID";

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum ClickHouseStartupTableCheck {
    /// Run a lightweight `SELECT count() ... WHERE 0` to validate table access without scanning.
    Exists,
    /// Run `SELECT COUNT(*)` to validate table access (can be slow on large tables).
    Count,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum, PartialEq, Eq)]
pub enum ClickHouseTransport {
    Tcp,
    Http,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum, PartialEq, Eq)]
pub enum ClickHouseScope {
    Distributed,
    ShardDirect,
}

#[cfg(feature = "pyroscope")]
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum PyroscopeReportEncoding {
    Pprof,
    Folded,
}

#[cfg(feature = "pyroscope")]
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum PyroscopeCompression {
    Gzip,
    Off,
}

#[derive(Debug, Clone, Parser)]
#[command(
    author,
    version,
    about = "Solana RPC server serving data from ClickHouse"
)]
pub struct RpcConfig {
    /// Maximum accepted JSON-RPC request body size (bytes).
    #[arg(long, env = "RPC_MAX_BODY_BYTES", default_value_t = 1_048_576)]
    pub(crate) rpc_max_body_bytes: usize,

    /// End-to-end JSON-RPC request timeout (milliseconds).
    #[arg(long, env = "RPC_REQUEST_TIMEOUT_MS", default_value_t = 10_000)]
    pub(crate) rpc_request_timeout_ms: u64,

    /// Maximum number of in-flight JSON-RPC requests.
    #[arg(long, env = "RPC_CONCURRENCY_LIMIT", default_value_t = 512)]
    pub(crate) rpc_concurrency_limit: usize,

    /// Maximum number of JSON-RPC calls accepted in a single batch request.
    #[arg(long, env = "RPC_MAX_BATCH_SIZE", default_value_t = 64)]
    pub(crate) rpc_max_batch_size: usize,

    /// Maximum number of JSON-RPC calls executed concurrently within a single batch.
    #[arg(long, env = "RPC_BATCH_CONCURRENCY_LIMIT", default_value_t = 8)]
    pub(crate) rpc_batch_concurrency_limit: usize,

    /// Emit HTTP 503 for JSON-RPC server-side failures while keeping response bodies unchanged.
    #[arg(long, env = "SUPERBANK_RPC_EMIT_HTTP_ERRORS", default_value_t = false)]
    pub(crate) emit_http_errors: bool,

    #[arg(long, env = "RPC_HOST", default_value = "0.0.0.0")]
    pub(crate) host: String,

    #[arg(long, env = "RPC_PORT", default_value = "8899")]
    pub(crate) port: u16,

    /// Host to bind the Prometheus metrics server.
    #[arg(long, env = "METRICS_HOST", default_value = "0.0.0.0")]
    pub(crate) metrics_host: String,

    /// Port to bind the Prometheus metrics server.
    #[arg(long, env = "METRICS_PORT", default_value = "9900")]
    pub(crate) metrics_port: u16,

    // --- Optional Superbank gRPC streaming API ---
    #[cfg(feature = "grpc-streaming")]
    /// Enable the Superbank gRPC streaming API.
    #[arg(long, env = "SUPERBANK_GRPC_ENABLED", default_value_t = false)]
    pub(crate) superbank_grpc_enabled: bool,

    #[cfg(feature = "grpc-streaming")]
    /// Host to bind the Superbank gRPC streaming API.
    #[arg(long, env = "SUPERBANK_GRPC_HOST", default_value = "0.0.0.0")]
    pub(crate) superbank_grpc_host: String,

    #[cfg(feature = "grpc-streaming")]
    /// Port to bind the Superbank gRPC streaming API.
    #[arg(long, env = "SUPERBANK_GRPC_PORT", default_value_t = 10_000)]
    pub(crate) superbank_grpc_port: u16,

    #[cfg(feature = "grpc-streaming")]
    /// Maximum inclusive slot range accepted by the Superbank gRPC streaming API.
    #[arg(
        long,
        env = "SUPERBANK_GRPC_MAX_SLOT_RANGE",
        default_value_t = 100,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub(crate) superbank_grpc_max_slot_range: u64,

    #[cfg(feature = "grpc-streaming")]
    /// Timeout for each Superbank gRPC ClickHouse chunk query (milliseconds).
    #[arg(
        long,
        env = "SUPERBANK_GRPC_QUERY_TIMEOUT_MS",
        default_value_t = 30_000,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub(crate) superbank_grpc_query_timeout_ms: u64,

    #[cfg(feature = "grpc-streaming")]
    /// Number of slots fetched per Superbank gRPC ClickHouse chunk query.
    #[arg(
        long,
        env = "SUPERBANK_GRPC_CHUNK_SLOTS",
        default_value_t = 8,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub(crate) superbank_grpc_chunk_slots: u64,

    #[cfg(feature = "grpc-streaming")]
    /// Maximum encoded gRPC message size sent by the Superbank gRPC service.
    #[arg(
        long,
        env = "SUPERBANK_GRPC_MAX_SEND_BYTES",
        default_value_t = 104_857_600
    )]
    pub(crate) superbank_grpc_max_send_bytes: usize,

    #[cfg(feature = "grpc-streaming")]
    /// Maximum concurrent HTTP/2 streams per gRPC connection.
    #[arg(
        long,
        env = "SUPERBANK_GRPC_MAX_CONCURRENT_STREAMS",
        default_value_t = 20,
        value_parser = clap::value_parser!(u32).range(1..)
    )]
    pub(crate) superbank_grpc_max_concurrent_streams: u32,

    /// Request headers to capture in route/request metrics (`X-Endpoint`, `X-RPC-Node`, `X-Subscription-ID`, `X-Account-ID`).
    #[arg(
        long = "metrics-capture-header",
        env = "METRICS_CAPTURE_HEADERS",
        action = ArgAction::Append,
        value_delimiter = ',',
        value_parser = parse_metrics_capture_header
    )]
    pub(crate) metrics_capture_headers: Vec<String>,

    #[arg(long, env = "CLICKHOUSE_URL", default_value = "http://localhost:8123")]
    pub(crate) clickhouse_url: String,

    #[arg(long, env = "CLICKHOUSE_DATABASE", default_value = "default")]
    pub(crate) clickhouse_database: String,

    #[arg(long, env = "CLICKHOUSE_USER", default_value = "default")]
    pub(crate) clickhouse_user: String,

    #[arg(long, env = "CLICKHOUSE_PASSWORD", default_value = "")]
    pub(crate) clickhouse_password: String,

    #[arg(long, env = "MAX_SIGNATURES_LIMIT", default_value = "1000")]
    pub(crate) max_signatures_limit: u64,

    /// Timeout for ClickHouse queries (milliseconds).
    #[arg(long, env = "CLICKHOUSE_QUERY_TIMEOUT_MS", default_value_t = 8_000)]
    pub(crate) clickhouse_query_timeout_ms: u64,

    /// Enable ClickHouse query cache for historical read queries.
    #[arg(long, env = "CLICKHOUSE_QUERY_CACHE_ENABLED", default_value_t = false)]
    pub(crate) clickhouse_query_cache_enabled: bool,

    /// ClickHouse query cache TTL (seconds) for historical read queries.
    #[arg(
        long,
        env = "CLICKHOUSE_QUERY_CACHE_TTL_SECONDS",
        default_value_t = 1,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub(crate) clickhouse_query_cache_ttl_seconds: u64,

    /// ClickHouse query cache TTL (seconds) used only for historical getTransaction point lookups.
    #[arg(
        long,
        env = "CLICKHOUSE_GET_TRANSACTION_QUERY_CACHE_TTL_SECONDS",
        default_value_t = 300,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub(crate) clickhouse_get_transaction_query_cache_ttl_seconds: u64,

    /// Minimum identical getTransaction query executions before ClickHouse writes the result into cache.
    #[arg(
        long,
        env = "CLICKHOUSE_GET_TRANSACTION_QUERY_CACHE_MIN_QUERY_RUNS",
        default_value_t = 2,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub(crate) clickhouse_get_transaction_query_cache_min_query_runs: u64,

    /// Share ClickHouse query cache entries between users.
    #[arg(
        long,
        env = "CLICKHOUSE_QUERY_CACHE_SHARE_BETWEEN_USERS",
        default_value_t = false
    )]
    pub(crate) clickhouse_query_cache_share_between_users: bool,

    /// Enable ClickHouse query condition cache for selected historical address-filtered reads.
    #[arg(
        long,
        env = "CLICKHOUSE_QUERY_CONDITION_CACHE_ENABLED",
        default_value_t = false
    )]
    pub(crate) clickhouse_query_condition_cache_enabled: bool,

    /// Max number of concurrent per-shard queries for shard-local fanout.
    #[arg(long, env = "CLICKHOUSE_SHARD_FANOUT_CONCURRENCY", default_value_t = 8)]
    pub(crate) clickhouse_shard_fanout_concurrency: usize,

    /// Max number of concurrent direct (scalar/lookup) ClickHouse HTTP queries in flight
    /// server-wide. Bounds HTTP connections to ClickHouse independently of shard fanout and
    /// JSON-RPC batching; set at or below the ClickHouse per-user connection/query budget.
    #[arg(long, env = "CLICKHOUSE_HTTP_MAX_CONCURRENCY", default_value_t = 512)]
    pub(crate) clickhouse_http_max_concurrency: usize,

    /// TCP connect timeout (ms) for ClickHouse HTTP connections. Bounds how long a new
    /// connection attempt can hang during ClickHouse backpressure before it fails fast.
    #[arg(
        long,
        env = "CLICKHOUSE_HTTP_CONNECT_TIMEOUT_MS",
        default_value_t = 2000
    )]
    pub(crate) clickhouse_http_connect_timeout_ms: u64,

    /// Minimum connections retained per shard in each ClickHouse native (TCP) connection pool.
    #[arg(long, env = "CLICKHOUSE_TCP_POOL_MIN", default_value_t = 10)]
    pub(crate) clickhouse_tcp_pool_min: usize,

    /// Maximum connections per shard in each ClickHouse native (TCP) connection pool. Total
    /// native connections per instance are bounded by this value times the number of shards.
    #[arg(long, env = "CLICKHOUSE_TCP_POOL_MAX", default_value_t = 20)]
    pub(crate) clickhouse_tcp_pool_max: usize,

    /// Chunk size for large IN(...) filters to cap SQL string size.
    #[arg(long, env = "CLICKHOUSE_IN_CLAUSE_CHUNK", default_value_t = 512)]
    pub(crate) clickhouse_in_clause_chunk: usize,

    /// Startup table access validation strategy.
    #[arg(
        long,
        env = "CLICKHOUSE_STARTUP_TABLE_CHECK",
        value_enum,
        default_value = "exists"
    )]
    pub(crate) clickhouse_startup_table_check: ClickHouseStartupTableCheck,

    /// Max concurrent CPU-heavy hydration jobs (limits spawn_blocking usage).
    #[arg(long, env = "HYDRATION_CPU_CONCURRENCY", default_value_t = 8)]
    pub(crate) hydration_cpu_concurrency: usize,

    /// ClickHouse transport used for all shard-direct queries.
    #[arg(long, env = "CLICKHOUSE_TRANSPORT", value_enum, default_value = "http")]
    pub(crate) clickhouse_transport: ClickHouseTransport,

    /// ClickHouse routing scope used for all queries.
    #[arg(
        long,
        env = "CLICKHOUSE_SCOPE",
        value_enum,
        default_value = "distributed"
    )]
    pub(crate) clickhouse_scope: ClickHouseScope,

    /// Timeout for the ClickHouse TCP access check during startup (milliseconds).
    #[arg(
        long,
        env = "CLICKHOUSE_TCP_ACCESS_CHECK_TIMEOUT_MS",
        default_value_t = 2_000,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub(crate) clickhouse_tcp_access_check_timeout_ms: u64,

    /// ClickHouse cluster name used to discover shard topology (supports macros like {cluster}).
    #[arg(long, env = "CLICKHOUSE_CLUSTER", default_value = "{cluster}")]
    pub(crate) clickhouse_cluster: String,

    /// Optional authoritative YAML topology file for shard-direct ClickHouse connections.
    /// When set, superbank-rpc skips system.clusters discovery and uses the YAML mapping directly.
    #[arg(long, env = "CLICKHOUSE_TOPOLOGY_CONFIG")]
    pub(crate) clickhouse_topology_config: Option<String>,

    /// Local gsfa table name on each shard (required for CLICKHOUSE_SCOPE=shard-direct).
    #[arg(long, env = "CLICKHOUSE_GSFA_LOCAL_TABLE")]
    pub(crate) clickhouse_gsfa_local_table: Option<String>,

    /// Local signatures table name on each shard (defaults to CLICKHOUSE_SIGNATURE_STATUSES_TABLE + _local).
    #[arg(long, env = "CLICKHOUSE_SIGNATURES_LOCAL_TABLE")]
    pub(crate) clickhouse_signatures_local_table: Option<String>,

    /// Local token owner activity table name on each shard (defaults to CLICKHOUSE_TOKEN_OWNER_ACTIVITY_TABLE + _local).
    #[arg(long, env = "CLICKHOUSE_TOKEN_OWNER_ACTIVITY_LOCAL_TABLE")]
    pub(crate) clickhouse_token_owner_activity_local_table: Option<String>,

    /// Local transactions table name on each shard (defaults to CLICKHOUSE_TRANSACTION_TABLE + _local).
    #[arg(long, env = "CLICKHOUSE_TRANSACTIONS_LOCAL_TABLE")]
    pub(crate) clickhouse_transactions_local_table: Option<String>,

    /// Local blocks metadata table name on each shard (defaults to CLICKHOUSE_BLOCKS_METADATA_TABLE + _local).
    #[arg(long, env = "CLICKHOUSE_BLOCKS_METADATA_LOCAL_TABLE")]
    pub(crate) clickhouse_blocks_metadata_local_table: Option<String>,

    /// Override shard HTTP port for shard-local HTTP queries (defaults to port in CLICKHOUSE_URL).
    #[arg(long, env = "CLICKHOUSE_SHARD_HTTP_PORT")]
    pub(crate) clickhouse_shard_http_port: Option<u16>,

    /// Addresses to route to the GSFA hot table (repeat flag for multiple, or comma-separated via env).
    #[arg(
        long = "clickhouse-hot-address",
        env = "CLICKHOUSE_GSFA_HOT_ADDRESSES",
        action = ArgAction::Append,
        value_delimiter = ','
    )]
    pub(crate) clickhouse_hot_addresses: Vec<String>,

    /// Distributed GSFA hot table name used for active hot-address reads.
    #[arg(
        long,
        env = "CLICKHOUSE_GSFA_HOT_TABLE",
        default_value = "default.gsfa_hot"
    )]
    pub(crate) clickhouse_gsfa_hot_table: String,

    /// Local GSFA hot table backing CLICKHOUSE_GSFA_HOT_TABLE on each shard.
    #[arg(
        long,
        env = "CLICKHOUSE_GSFA_HOT_LOCAL_TABLE",
        default_value = "default.gsfa_hot_local"
    )]
    pub(crate) clickhouse_gsfa_hot_local_table: String,

    // --- Optional gRPC head cache (Yellowstone DragonsMouth) ---
    #[cfg(feature = "grpc-head-cache")]
    /// Enable an in-memory head cache fed by a Yellowstone DragonsMouth gRPC stream.
    #[arg(long, env = "HEAD_CACHE_ENABLED", default_value_t = false)]
    pub(crate) head_cache_enabled: bool,

    #[cfg(feature = "grpc-head-cache")]
    /// Yellowstone gRPC endpoint (DragonsMouth).
    #[arg(long, env = "DRAGONSMOUTH_ENDPOINT")]
    pub(crate) dragonsmouth_endpoint: Option<String>,

    #[cfg(feature = "grpc-head-cache")]
    /// Optional `x-token` header for DragonsMouth.
    #[arg(long, env = "DRAGONSMOUTH_X_TOKEN")]
    pub(crate) dragonsmouth_x_token: Option<String>,

    #[cfg(feature = "grpc-head-cache")]
    /// How many slots of head data to retain in memory.
    #[arg(long, env = "HEAD_CACHE_RETAIN_SLOTS", default_value_t = 32)]
    pub(crate) head_cache_retain_slots: u64,

    #[cfg(feature = "grpc-head-cache")]
    /// Minimum commitment exposed by the head cache: processed|confirmed|finalized.
    #[arg(long, env = "HEAD_CACHE_MIN_COMMITMENT", default_value = "processed")]
    pub(crate) head_cache_min_commitment: String,

    #[cfg(feature = "grpc-head-cache")]
    /// Max gRPC decoding message size (bytes).
    #[arg(long, env = "GRPC_MAX_DECODING_BYTES", default_value_t = 67_108_864)]
    pub(crate) grpc_max_decoding_bytes: usize,

    // --- Optional RocksDB disk cache of recent finalized slots ---
    #[cfg(feature = "disk-cache")]
    /// Enable the RocksDB-backed disk cache of recent finalized slots, served in
    /// place of ClickHouse. Requires the gRPC head cache (its DragonsMouth
    /// stream is the live ingestion source) and DISK_CACHE_PATH.
    #[arg(long, env = "DISK_CACHE_ENABLED", default_value_t = false)]
    pub(crate) disk_cache_enabled: bool,

    #[cfg(feature = "disk-cache")]
    /// Filesystem path for the disk cache database (required when enabled).
    #[arg(long, env = "DISK_CACHE_PATH")]
    pub(crate) disk_cache_path: Option<String>,

    #[cfg(feature = "disk-cache")]
    /// How many finalized slots to retain on disk (default ~10 epochs). At
    /// mainnet volume the full window needs tens of TB; the tighter of this
    /// window and DISK_CACHE_MAX_BYTES wins.
    #[arg(long, env = "DISK_CACHE_RETAIN_SLOTS", default_value_t = 4_320_000)]
    pub(crate) disk_cache_retain_slots: u64,

    #[cfg(feature = "disk-cache")]
    /// Disk byte budget; 0 = unlimited. When live data exceeds it, the slot
    /// window shrinks until usage fits.
    #[arg(long, env = "DISK_CACHE_MAX_BYTES", default_value_t = 0)]
    pub(crate) disk_cache_max_bytes: u64,

    #[cfg(feature = "disk-cache")]
    /// RocksDB block-cache size (bytes) shared across column families.
    #[arg(
        long,
        env = "DISK_CACHE_BLOCK_CACHE_BYTES",
        default_value_t = 4_294_967_296
    )]
    pub(crate) disk_cache_block_cache_bytes: usize,

    #[cfg(feature = "disk-cache")]
    /// Live write queue capacity in slots (overflow defers slots to repair).
    #[arg(long, env = "DISK_CACHE_WRITE_QUEUE_SLOTS", default_value_t = 64)]
    pub(crate) disk_cache_write_queue_slots: usize,

    #[cfg(feature = "disk-cache")]
    /// Max concurrent blocking disk-cache reads.
    #[arg(long, env = "DISK_CACHE_READ_CONCURRENCY", default_value_t = 64)]
    pub(crate) disk_cache_read_concurrency: usize,

    #[cfg(feature = "disk-cache")]
    /// Enable the ClickHouse->disk backfill/repair task (disable for debugging only).
    #[arg(long, env = "DISK_CACHE_BACKFILL_ENABLED", default_value_t = true)]
    pub(crate) disk_cache_backfill_enabled: bool,

    #[cfg(feature = "disk-cache")]
    /// Slots fetched per ClickHouse backfill range query.
    #[arg(
        long,
        env = "DISK_CACHE_BACKFILL_SLOTS_PER_QUERY",
        default_value_t = 8,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub(crate) disk_cache_backfill_slots_per_query: u64,

    #[cfg(feature = "disk-cache")]
    /// Backfill rate limit (slots per second). The default fills the full
    /// 10-epoch window in roughly a day.
    #[arg(
        long,
        env = "DISK_CACHE_BACKFILL_MAX_SLOTS_PER_SEC",
        default_value_t = 50,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub(crate) disk_cache_backfill_max_slots_per_sec: u64,

    #[cfg(feature = "disk-cache")]
    /// Timeout for backfill range queries (milliseconds); range scans need more
    /// than the interactive CLICKHOUSE_QUERY_TIMEOUT_MS.
    #[arg(
        long,
        env = "DISK_CACHE_BACKFILL_QUERY_TIMEOUT_MS",
        default_value_t = 30_000,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub(crate) disk_cache_backfill_query_timeout_ms: u64,

    #[cfg(feature = "disk-cache")]
    /// Idle wait between repair/backfill planning rounds (milliseconds).
    #[arg(
        long,
        env = "DISK_CACHE_REPAIR_INTERVAL_MS",
        default_value_t = 5_000,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub(crate) disk_cache_repair_interval_ms: u64,

    #[cfg(feature = "disk-cache")]
    /// Never backfill slots within this distance of the finalized tip, so
    /// ClickHouse ingestion has had time to land them.
    #[arg(long, env = "DISK_CACHE_REPAIR_MIN_LAG_SLOTS", default_value_t = 75)]
    pub(crate) disk_cache_repair_min_lag_slots: u64,

    // --- Optional Pyroscope continuous profiling ---
    #[cfg(feature = "pyroscope")]
    /// Enable Pyroscope continuous profiling (requires `PYROSCOPE_URL` / `--pyroscope-url`).
    #[arg(long = "pyroscope", env = "PYROSCOPE_ENABLED", default_value_t = false)]
    pub(crate) pyroscope_enabled: bool,

    #[cfg(feature = "pyroscope")]
    /// Pyroscope server URL, e.g. `http://localhost:4040`.
    #[arg(long, env = "PYROSCOPE_URL")]
    pub(crate) pyroscope_url: Option<String>,

    #[cfg(feature = "pyroscope")]
    /// Application name to show in Pyroscope.
    #[arg(long, env = "PYROSCOPE_APP_NAME", default_value = "superbank-rpc")]
    pub(crate) pyroscope_app_name: String,

    #[cfg(feature = "pyroscope")]
    /// CPU sampling rate (Hz).
    #[arg(long, env = "PYROSCOPE_SAMPLE_RATE", default_value_t = 100)]
    pub(crate) pyroscope_sample_rate: u32,

    #[cfg(feature = "pyroscope")]
    /// Include thread names in profiles.
    #[arg(long, env = "PYROSCOPE_REPORT_THREAD_NAME", default_value_t = true)]
    pub(crate) pyroscope_report_thread_name: bool,

    #[cfg(feature = "pyroscope")]
    /// Include thread IDs in profiles.
    #[arg(long, env = "PYROSCOPE_REPORT_THREAD_ID", default_value_t = false)]
    pub(crate) pyroscope_report_thread_id: bool,

    #[cfg(feature = "pyroscope")]
    /// Tags to attach to profiles (repeat flag, or comma-separated via env).
    #[arg(
        long = "pyroscope-tags",
        env = "PYROSCOPE_TAGS",
        action = ArgAction::Append,
        value_delimiter = ','
    )]
    pub(crate) pyroscope_tags: Vec<String>,

    #[cfg(feature = "pyroscope")]
    /// Report encoding format.
    #[arg(
        long,
        env = "PYROSCOPE_REPORT_ENCODING",
        value_enum,
        default_value = "pprof"
    )]
    pub(crate) pyroscope_report_encoding: PyroscopeReportEncoding,

    #[cfg(feature = "pyroscope")]
    /// HTTP request body compression.
    #[arg(
        long,
        env = "PYROSCOPE_COMPRESSION",
        value_enum,
        default_value = "gzip"
    )]
    pub(crate) pyroscope_compression: PyroscopeCompression,

    #[cfg(feature = "pyroscope")]
    /// Bearer token for Pyroscope ingestion.
    #[arg(long, env = "PYROSCOPE_AUTH_TOKEN")]
    pub(crate) pyroscope_auth_token: Option<String>,

    #[cfg(feature = "pyroscope")]
    /// Basic auth username for Pyroscope ingestion.
    #[arg(long, env = "PYROSCOPE_BASIC_AUTH_USER")]
    pub(crate) pyroscope_basic_auth_user: Option<String>,

    #[cfg(feature = "pyroscope")]
    /// Basic auth password for Pyroscope ingestion.
    #[arg(long, env = "PYROSCOPE_BASIC_AUTH_PASS")]
    pub(crate) pyroscope_basic_auth_pass: Option<String>,

    #[cfg(feature = "pyroscope")]
    /// Tenant ID for multi-tenant Pyroscope (sent as `X-Scope-OrgID`).
    #[arg(long, env = "PYROSCOPE_TENANT_ID")]
    pub(crate) pyroscope_tenant_id: Option<String>,

    #[cfg(feature = "pyroscope")]
    /// Extra HTTP headers to include with ingestion requests (repeat flag, or comma-separated via env).
    #[arg(
        long = "pyroscope-http-header",
        env = "PYROSCOPE_HTTP_HEADERS",
        action = ArgAction::Append,
        value_delimiter = ','
    )]
    pub(crate) pyroscope_http_headers: Vec<String>,
}

pub(crate) fn has_usable_gsfa_hot_addresses(addresses: &[String]) -> bool {
    addresses.iter().any(|address| {
        let address = address.trim();
        !address.is_empty() && Pubkey::from_str(address).is_ok()
    })
}

fn parse_metrics_capture_header(value: &str) -> Result<String, String> {
    let trimmed = value.trim();
    // Treat blank values (e.g. METRICS_CAPTURE_HEADERS="") as "capture disabled".
    if trimmed.is_empty() {
        return Ok(String::new());
    }
    if trimmed.eq_ignore_ascii_case(METRICS_CAPTURE_HEADER_X_ENDPOINT) {
        return Ok(METRICS_CAPTURE_HEADER_X_ENDPOINT.to_string());
    }
    if trimmed.eq_ignore_ascii_case(METRICS_CAPTURE_HEADER_X_RPC_NODE) {
        return Ok(METRICS_CAPTURE_HEADER_X_RPC_NODE.to_string());
    }
    if trimmed.eq_ignore_ascii_case(METRICS_CAPTURE_HEADER_X_SUBSCRIPTION_ID) {
        return Ok(METRICS_CAPTURE_HEADER_X_SUBSCRIPTION_ID.to_string());
    }
    if trimmed.eq_ignore_ascii_case(METRICS_CAPTURE_HEADER_X_ACCOUNT_ID) {
        return Ok(METRICS_CAPTURE_HEADER_X_ACCOUNT_ID.to_string());
    }
    Err(format!(
        "unsupported metrics capture header '{trimmed}' (supported: {METRICS_CAPTURE_HEADER_X_ENDPOINT}, {METRICS_CAPTURE_HEADER_X_RPC_NODE}, {METRICS_CAPTURE_HEADER_X_SUBSCRIPTION_ID}, {METRICS_CAPTURE_HEADER_X_ACCOUNT_ID})"
    ))
}

impl RpcConfig {
    pub(crate) fn metrics_capture_x_endpoint(&self) -> bool {
        self.metrics_capture_headers
            .iter()
            .any(|name| name == METRICS_CAPTURE_HEADER_X_ENDPOINT)
    }

    pub(crate) fn metrics_capture_x_rpc_node(&self) -> bool {
        self.metrics_capture_headers
            .iter()
            .any(|name| name == METRICS_CAPTURE_HEADER_X_RPC_NODE)
    }

    pub(crate) fn metrics_capture_x_subscription_id(&self) -> bool {
        self.metrics_capture_headers
            .iter()
            .any(|name| name == METRICS_CAPTURE_HEADER_X_SUBSCRIPTION_ID)
    }

    pub(crate) fn metrics_capture_x_account_id(&self) -> bool {
        self.metrics_capture_headers
            .iter()
            .any(|name| name == METRICS_CAPTURE_HEADER_X_ACCOUNT_ID)
    }
}

/// Serializes tests that read or mutate process-global environment variables through
/// `RpcConfig::parse_from`. Shared across test modules in this crate (e.g. `server::tests`,
/// which parse a default config) so an env-mutating test cannot race a parse in another module.
#[cfg(test)]
pub(crate) static ENV_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
mod config_tests {
    use clap::Parser;

    use super::{ENV_TEST_LOCK as ENV_LOCK, RpcConfig};

    struct EnvVarGuard {
        key: &'static str,
        original: Option<std::ffi::OsString>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let original = std::env::var_os(key);
            // SAFETY: this test holds ENV_LOCK while mutating process environment and restores
            // the previous value before releasing it.
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, original }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            // SAFETY: this test holds ENV_LOCK while restoring process environment.
            unsafe {
                match &self.original {
                    Some(value) => std::env::set_var(self.key, value),
                    None => std::env::remove_var(self.key),
                }
            }
        }
    }

    #[test]
    fn clickhouse_query_cache_defaults() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        let cfg = RpcConfig::parse_from(["superbank-rpc"]);

        assert!(!cfg.clickhouse_query_cache_enabled);
        assert_eq!(cfg.clickhouse_query_cache_ttl_seconds, 1);
        assert!(!cfg.clickhouse_query_cache_share_between_users);
        assert!(!cfg.clickhouse_query_condition_cache_enabled);
        assert!(!cfg.emit_http_errors);
        assert!(!cfg.metrics_capture_x_endpoint());
        assert!(!cfg.metrics_capture_x_rpc_node());
        assert!(!cfg.metrics_capture_x_subscription_id());
        assert!(!cfg.metrics_capture_x_account_id());
    }

    #[test]
    fn clickhouse_query_cache_flags_parse() {
        let cfg = RpcConfig::parse_from([
            "superbank-rpc",
            "--clickhouse-query-cache-enabled",
            "--clickhouse-query-cache-ttl-seconds",
            "5",
            "--clickhouse-query-cache-share-between-users",
            "--clickhouse-query-condition-cache-enabled",
        ]);

        assert!(cfg.clickhouse_query_cache_enabled);
        assert_eq!(cfg.clickhouse_query_cache_ttl_seconds, 5);
        assert!(cfg.clickhouse_query_cache_share_between_users);
        assert!(cfg.clickhouse_query_condition_cache_enabled);
    }

    #[test]
    fn emit_http_errors_flag_parses() {
        let cfg = RpcConfig::parse_from(["superbank-rpc", "--emit-http-errors"]);

        assert!(cfg.emit_http_errors);
    }

    #[test]
    fn emit_http_errors_env_parses() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        let _env = EnvVarGuard::set("SUPERBANK_RPC_EMIT_HTTP_ERRORS", "true");

        let cfg = RpcConfig::parse_from(["superbank-rpc"]);

        assert!(cfg.emit_http_errors);
    }

    #[test]
    fn clickhouse_topology_config_flag_parses() {
        let cfg = RpcConfig::parse_from([
            "superbank-rpc",
            "--clickhouse-topology-config",
            "/etc/superbank/topology.yaml",
        ]);

        assert_eq!(
            cfg.clickhouse_topology_config.as_deref(),
            Some("/etc/superbank/topology.yaml")
        );
    }

    #[test]
    fn clickhouse_topology_config_env_parses() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        let _env = EnvVarGuard::set("CLICKHOUSE_TOPOLOGY_CONFIG", "/etc/superbank/topology.yaml");

        let cfg = RpcConfig::parse_from(["superbank-rpc"]);

        assert_eq!(
            cfg.clickhouse_topology_config.as_deref(),
            Some("/etc/superbank/topology.yaml")
        );
    }

    #[test]
    fn metrics_capture_headers_parse_and_normalize() {
        let cfg = RpcConfig::parse_from([
            "superbank-rpc",
            "--metrics-capture-header",
            "x-endpoint",
            "--metrics-capture-header",
            "X-RPC-Node",
            "--metrics-capture-header",
            "x-subscription-id",
            "--metrics-capture-header",
            "X-Account-ID",
        ]);

        assert!(cfg.metrics_capture_x_endpoint());
        assert!(cfg.metrics_capture_x_rpc_node());
        assert!(cfg.metrics_capture_x_subscription_id());
        assert!(cfg.metrics_capture_x_account_id());
        assert_eq!(
            cfg.metrics_capture_headers,
            vec![
                "X-Endpoint",
                "X-RPC-Node",
                "X-Subscription-ID",
                "X-Account-ID"
            ]
        );
    }

    #[test]
    fn metrics_capture_headers_reject_unknown_values() {
        let err =
            RpcConfig::try_parse_from(["superbank-rpc", "--metrics-capture-header", "X-Unknown"])
                .expect_err("unknown capture header should fail to parse");

        let message = err.to_string();
        assert!(message.contains("unsupported metrics capture header"));
    }

    #[test]
    fn metrics_capture_headers_reject_legacy_x_token() {
        let err =
            RpcConfig::try_parse_from(["superbank-rpc", "--metrics-capture-header", "X-Token"])
                .expect_err("legacy x-token header should fail to parse");

        let message = err.to_string();
        assert!(message.contains("unsupported metrics capture header"));
    }

    #[test]
    fn metrics_capture_headers_empty_value_is_treated_as_disabled() {
        let cfg = RpcConfig::parse_from(["superbank-rpc", "--metrics-capture-header", ""]);

        assert!(!cfg.metrics_capture_x_endpoint());
        assert!(!cfg.metrics_capture_x_rpc_node());
        assert!(!cfg.metrics_capture_x_subscription_id());
        assert!(!cfg.metrics_capture_x_account_id());
    }

    #[test]
    fn metrics_capture_headers_ignore_empty_entries_in_comma_lists() {
        let cfg =
            RpcConfig::parse_from(["superbank-rpc", "--metrics-capture-header", ",X-Endpoint,"]);

        assert!(cfg.metrics_capture_x_endpoint());
        assert!(!cfg.metrics_capture_x_rpc_node());
        assert!(!cfg.metrics_capture_x_subscription_id());
        assert!(!cfg.metrics_capture_x_account_id());
    }
}

#[cfg(all(test, feature = "disk-cache"))]
mod disk_cache_config_tests {
    use clap::Parser;

    use super::RpcConfig;

    #[test]
    fn disk_cache_defaults() {
        let cfg = RpcConfig::parse_from(["superbank-rpc"]);

        assert!(!cfg.disk_cache_enabled);
        assert_eq!(cfg.disk_cache_path, None);
        assert_eq!(cfg.disk_cache_retain_slots, 4_320_000);
        assert_eq!(cfg.disk_cache_max_bytes, 0);
        assert_eq!(cfg.disk_cache_block_cache_bytes, 4_294_967_296);
        assert_eq!(cfg.disk_cache_write_queue_slots, 64);
        assert_eq!(cfg.disk_cache_read_concurrency, 64);
        assert!(cfg.disk_cache_backfill_enabled);
        assert_eq!(cfg.disk_cache_backfill_slots_per_query, 8);
        assert_eq!(cfg.disk_cache_backfill_max_slots_per_sec, 50);
        assert_eq!(cfg.disk_cache_backfill_query_timeout_ms, 30_000);
        assert_eq!(cfg.disk_cache_repair_interval_ms, 5_000);
        assert_eq!(cfg.disk_cache_repair_min_lag_slots, 75);
    }

    #[test]
    fn disk_cache_flags_parse() {
        let cfg = RpcConfig::parse_from([
            "superbank-rpc",
            "--disk-cache-enabled",
            "--disk-cache-path",
            "/var/lib/superbank/disk-cache",
            "--disk-cache-retain-slots",
            "432000",
            "--disk-cache-max-bytes",
            "2199023255552",
            "--disk-cache-backfill-max-slots-per-sec",
            "200",
        ]);

        assert!(cfg.disk_cache_enabled);
        assert_eq!(
            cfg.disk_cache_path.as_deref(),
            Some("/var/lib/superbank/disk-cache")
        );
        assert_eq!(cfg.disk_cache_retain_slots, 432_000);
        assert_eq!(cfg.disk_cache_max_bytes, 2_199_023_255_552);
        assert_eq!(cfg.disk_cache_backfill_max_slots_per_sec, 200);
    }

    #[test]
    fn disk_cache_rejects_zero_rate_limits() {
        assert!(
            RpcConfig::try_parse_from([
                "superbank-rpc",
                "--disk-cache-backfill-slots-per-query",
                "0",
            ])
            .is_err()
        );
        assert!(
            RpcConfig::try_parse_from([
                "superbank-rpc",
                "--disk-cache-backfill-max-slots-per-sec",
                "0",
            ])
            .is_err()
        );
    }
}

#[cfg(all(test, feature = "pyroscope"))]
mod pyroscope_config_tests {
    use clap::Parser;

    use super::RpcConfig;

    #[test]
    fn parse_pyroscope_flag_defaults() {
        let cfg = RpcConfig::parse_from([
            "superbank-rpc",
            "--pyroscope",
            "--pyroscope-url",
            "http://localhost:4040",
        ]);

        assert!(cfg.pyroscope_enabled);
        assert_eq!(cfg.pyroscope_app_name, "superbank-rpc");
        assert_eq!(cfg.pyroscope_sample_rate, 100);
    }

    #[test]
    fn parse_pyroscope_tags() {
        let cfg = RpcConfig::parse_from([
            "superbank-rpc",
            "--pyroscope",
            "--pyroscope-url",
            "http://localhost:4040",
            "--pyroscope-tags",
            "env=dev,region=us-east-1",
        ]);
        assert_eq!(cfg.pyroscope_tags, vec!["env=dev", "region=us-east-1"]);
    }

    #[test]
    fn parse_pyroscope_headers() {
        let cfg = RpcConfig::parse_from([
            "superbank-rpc",
            "--pyroscope",
            "--pyroscope-url",
            "http://localhost:4040",
            "--pyroscope-http-header",
            "X-Test=1",
            "--pyroscope-http-header",
            "X-Foo=bar",
        ]);
        assert_eq!(cfg.pyroscope_http_headers, vec!["X-Test=1", "X-Foo=bar"]);
    }
}
