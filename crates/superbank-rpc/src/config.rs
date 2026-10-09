// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::{path::PathBuf, str::FromStr};

use crate::solana_sdk::pubkey::Pubkey;
use clap::{ArgAction, Parser};

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
    /// Path to the shared YAML configuration file.
    #[arg(long, env = "SUPERBANK_CONFIG", value_name = "PATH")]
    pub(crate) config: Option<PathBuf>,

    /// Maximum accepted JSON-RPC request body size (bytes).
    #[arg(long, env = "RPC_MAX_BODY_BYTES", default_value_t = 1_048_576)]
    pub(crate) rpc_max_body_bytes: usize,

    /// End-to-end JSON-RPC request timeout (milliseconds).
    #[arg(long, env = "RPC_REQUEST_TIMEOUT_MS", default_value_t = 10_000)]
    pub(crate) rpc_request_timeout_ms: u64,

    /// Maximum number of in-flight JSON-RPC requests.
    #[arg(long, env = "RPC_CONCURRENCY_LIMIT", default_value_t = 512)]
    pub(crate) rpc_concurrency_limit: usize,

    /// Maximum serialized getBlock result bytes retained in memory; zero disables the cache.
    #[arg(long, env = "GET_BLOCK_RESPONSE_CACHE_MAX_BYTES", default_value_t = 0)]
    pub(crate) get_block_response_cache_max_bytes: u64,

    /// Maximum approximate bytes of primary-served getTransaction records retained in memory;
    /// zero disables the cache.
    #[arg(
        long,
        env = "GET_TRANSACTION_PRIMARY_CACHE_MAX_BYTES",
        default_value_t = 0
    )]
    pub(crate) get_transaction_primary_cache_max_bytes: u64,

    /// Seconds a primary-served getTransaction record stays cached after insertion.
    #[arg(
        long,
        env = "GET_TRANSACTION_PRIMARY_CACHE_TTL_SECS",
        default_value_t = 600,
        value_parser = clap::value_parser!(u64).range(1..=86_400)
    )]
    pub(crate) get_transaction_primary_cache_ttl_secs: u64,

    /// Let confirmed getBlock requests read and populate the finalized response
    /// cache when their data is provably finalized (set false to disable).
    #[arg(
        long,
        env = "GET_BLOCK_RESPONSE_CACHE_SHARE_CONFIRMED",
        default_value_t = true
    )]
    pub(crate) get_block_response_cache_share_confirmed: bool,

    /// Remember deterministic getBlock `-32015` (unsupported transaction version) answers for
    /// finalized blocks while the response cache is enabled. `false` recomputes them on every
    /// request.
    #[arg(
        long,
        env = "GET_BLOCK_RESPONSE_CACHE_UNSUPPORTED_VERSION",
        default_value_t = true,
        action = ArgAction::Set
    )]
    pub(crate) get_block_response_cache_unsupported_version: bool,

    /// Signatures whose empty primary history-search answer is remembered for
    /// getSignatureStatuses; zero (the default) disables the absence cache.
    #[arg(
        long,
        env = "SIGNATURE_STATUS_HISTORY_CACHE_ENTRIES",
        default_value_t = 0
    )]
    pub(crate) signature_status_history_cache_entries: u64,

    /// Memory bound for the getSignatureStatuses absence cache (bytes; ~384 per entry).
    #[arg(
        long,
        env = "SIGNATURE_STATUS_HISTORY_CACHE_MAX_BYTES",
        default_value_t = 64 * 1024 * 1024
    )]
    pub(crate) signature_status_history_cache_max_bytes: u64,

    /// How long an empty primary history-search answer may skip the primary (seconds, 1-300).
    #[arg(
        long,
        env = "SIGNATURE_STATUS_HISTORY_CACHE_TTL_SECS",
        default_value_t = 300,
        value_parser = clap::value_parser!(u64).range(1..=300)
    )]
    pub(crate) signature_status_history_cache_ttl_secs: u64,

    /// Compress JSON-RPC responses when the client advertises gzip support.
    #[arg(long, env = "RPC_RESPONSE_GZIP_ENABLED", default_value_t = false)]
    pub(crate) rpc_response_gzip_enabled: bool,

    /// Maximum number of JSON-RPC calls accepted in a single batch request.
    #[arg(long, env = "RPC_MAX_BATCH_SIZE", default_value_t = 64)]
    pub(crate) rpc_max_batch_size: usize,

    /// Maximum number of JSON-RPC calls executed concurrently within a single batch.
    #[arg(long, env = "RPC_BATCH_CONCURRENCY_LIMIT", default_value_t = 8)]
    pub(crate) rpc_batch_concurrency_limit: usize,

    /// Maximum concurrent primary getSignatureStatuses workflows, including cancellation cleanup.
    #[arg(
        long,
        env = "GET_SIGNATURE_STATUSES_MAX_CONCURRENCY",
        default_value_t = 4
    )]
    pub(crate) get_signature_statuses_max_concurrency: usize,

    /// Primary getSignatureStatuses execution and primary-index filtering thread cap.
    #[arg(long, env = "GET_SIGNATURE_STATUSES_MAX_THREADS", default_value_t = 2)]
    pub(crate) get_signature_statuses_max_threads: usize,

    /// Maximum number of addresses accepted by getInflationReward; zero disables the limit.
    #[arg(
        long,
        env = "GET_INFLATION_REWARD_MAX_ADDRESSES",
        default_value_t = 100
    )]
    pub(crate) get_inflation_reward_max_addresses: usize,

    /// Maximum concurrent getInflationReward workflows; zero disables method admission control.
    #[arg(
        long,
        env = "GET_INFLATION_REWARD_MAX_CONCURRENCY",
        default_value_t = 20
    )]
    pub(crate) get_inflation_reward_max_concurrency: usize,

    /// End-to-end ClickHouse budget for one getInflationReward lookup (milliseconds).
    #[arg(
        long,
        env = "GET_INFLATION_REWARD_QUERY_TIMEOUT_MS",
        default_value_t = 5_000
    )]
    pub(crate) get_inflation_reward_query_timeout_ms: u64,

    /// Per-query ClickHouse thread cap for getInflationReward.
    #[arg(long, env = "GET_INFLATION_REWARD_MAX_THREADS", default_value_t = 2)]
    pub(crate) get_inflation_reward_max_threads: usize,

    /// Per-query ClickHouse memory cap for getInflationReward (bytes).
    #[arg(
        long,
        env = "GET_INFLATION_REWARD_MAX_MEMORY_BYTES",
        default_value_t = 536_870_912
    )]
    pub(crate) get_inflation_reward_max_memory_bytes: u64,

    /// Per-query ClickHouse read cap for getInflationReward (bytes).
    #[arg(
        long,
        env = "GET_INFLATION_REWARD_MAX_BYTES_TO_READ",
        default_value_t = 536_870_912
    )]
    pub(crate) get_inflation_reward_max_bytes_to_read: u64,

    /// Byte budget for the in-process cache of validated getInflationReward epoch boundary and
    /// partition-slot metadata; zero disables the cache.
    #[arg(
        long,
        env = "GET_INFLATION_REWARD_EPOCH_CACHE_MAX_BYTES",
        default_value_t = 16_777_216
    )]
    pub(crate) get_inflation_reward_epoch_cache_max_bytes: u64,

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

    /// Path to this RPC target cluster's genesis.bin, read at startup for epoch math.
    #[arg(long, env = "GENESIS_PATH")]
    pub(crate) genesis_path: Option<String>,

    /// Trusted same-cluster RPC endpoint supporting getAgGenesisCert (Agave 4.3+).
    #[arg(long, env = "AG_GENESIS_CERT_RPC_URL")]
    pub(crate) ag_genesis_cert_rpc_url: Option<String>,

    /// Total getAgGenesisCert source budget, including refresh admission (milliseconds).
    #[arg(
        long,
        env = "AG_GENESIS_CERT_RPC_TIMEOUT_MS",
        default_value_t = 2_000,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub(crate) ag_genesis_cert_rpc_timeout_ms: u64,

    /// Seconds an authoritative pre-migration null may be reused before refreshing.
    #[arg(
        long,
        env = "AG_GENESIS_CERT_REFRESH_INTERVAL_SECS",
        default_value_t = 5,
        value_parser = clap::value_parser!(u64).range(1..=300)
    )]
    pub(crate) ag_genesis_cert_refresh_interval_secs: u64,

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

    /// Resolve an uncached getTransaction signature position and read its payload in one
    /// primary ClickHouse query (one round trip) instead of two sequential queries.
    /// Distributed scope only; shard-direct keeps the two-query path.
    #[arg(
        long,
        env = "CLICKHOUSE_GET_TRANSACTION_SINGLE_ROUND_TRIP",
        default_value_t = false
    )]
    pub(crate) clickhouse_get_transaction_single_round_trip: bool,

    /// Bound the latest-finalized-slot query to slots at or above the caller's previous
    /// answer minus a margin. `false` sends the unbounded query every time.
    #[arg(
        long,
        env = "CLICKHOUSE_LATEST_SLOT_HINT",
        default_value_t = true,
        action = ArgAction::Set
    )]
    pub(crate) clickhouse_latest_slot_hint: bool,

    /// Return `slot:idx` paginationTokens for ClickHouse-sourced getTransactionsForAddress
    /// rows (head/disk rows already do), so the next page needs no primary signature lookup.
    /// Signature tokens are still accepted.
    #[arg(
        long,
        env = "CLICKHOUSE_TRANSACTIONS_FOR_ADDRESS_POSITION_TOKENS",
        default_value_t = false
    )]
    pub(crate) clickhouse_transactions_for_address_position_tokens: bool,

    /// Remember the position of each ClickHouse-sourced getTransactionsForAddress page's last
    /// row (in process, keyed by signature) so a follow-up page on this node that sends that
    /// signature as its cursor skips the primary signature lookup.
    #[arg(
        long,
        env = "CLICKHOUSE_TRANSACTIONS_FOR_ADDRESS_CURSOR_CACHE",
        default_value_t = false
    )]
    pub(crate) clickhouse_transactions_for_address_cursor_cache: bool,

    /// Push the getTransactionsForAddress token-accounts filter, ORDER BY and LIMIT into each
    /// UNION branch. `false` applies the filter outside the union, as before.
    #[arg(
        long,
        env = "CLICKHOUSE_TRANSACTIONS_FOR_ADDRESS_UNION_PUSHDOWN",
        default_value_t = true,
        action = ArgAction::Set
    )]
    pub(crate) clickhouse_transactions_for_address_union_pushdown: bool,

    /// Resolve a getSignaturesForAddress `before`/`until` cursor that the head and local
    /// tiers missed inside the primary page query (one round trip) instead of a separate
    /// primary lookup first. Distributed scope only; shard-direct and the transaction-table
    /// GSFA fallback keep the separate lookup.
    #[arg(long, env = "CLICKHOUSE_GSFA_INLINE_CURSOR", default_value_t = false)]
    pub(crate) clickhouse_gsfa_inline_cursor: bool,

    /// Route primary signature lookups (signature -> slot and getSignatureStatuses history) to
    /// the owner shard: read `cluster(CLICKHOUSE_CLUSTER, <signatures local table>,
    /// cityHash64(signature))` with `optimize_skip_unused_shards=1` instead of the
    /// `default.signatures` view, which queries every shard. Verified at startup.
    #[arg(
        long,
        env = "CLICKHOUSE_SIGNATURES_OWNER_SHARD_ROUTING",
        default_value_t = false,
        action = ArgAction::Set
    )]
    pub(crate) clickhouse_signatures_owner_shard_routing: bool,

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

    /// Per-query timeout (ms) for cancellation macro resolution, discovery and startup probes.
    #[arg(long, env = "CLICKHOUSE_STARTUP_VERIFICATION_TIMEOUT_MS",
        default_value_t = crate::clickhouse::verification::DEFAULT_STARTUP_VERIFICATION_TIMEOUT_MS,
        value_parser = clap::value_parser!(u64).range(1..))]
    pub(crate) clickhouse_startup_verification_timeout_ms: u64,

    /// Per-batch timeout (ms) for abandoned-query termination verification.
    #[arg(long, env = "CLICKHOUSE_RUNTIME_VERIFICATION_TIMEOUT_MS",
        default_value_t = crate::clickhouse::verification::DEFAULT_RUNTIME_VERIFICATION_TIMEOUT_MS,
        value_parser = clap::value_parser!(u64).range(1..))]
    pub(crate) clickhouse_runtime_verification_timeout_ms: u64,

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

    /// Max blocking threads one getBlock full/accounts build may use. Extra
    /// threads come from the hydration pool only when free; 1 disables.
    #[arg(long, env = "GET_BLOCK_HYDRATION_PARALLELISM", default_value_t = 4)]
    pub(crate) get_block_hydration_parallelism: usize,

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

    /// Interval between background health checks for unavailable shard replicas (milliseconds).
    #[arg(
        long,
        env = "CLICKHOUSE_REPLICA_HEALTH_CHECK_INTERVAL_MS",
        default_value_t = 10_000,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub(crate) clickhouse_replica_health_check_interval_ms: u64,

    /// ClickHouse cluster used for primary status-query cancellation and shard-direct discovery.
    /// Supports macros such as {cluster}; empty selects local-only cancellation on standalone nodes.
    #[arg(long, env = "CLICKHOUSE_CLUSTER", default_value = "{cluster}")]
    pub(crate) clickhouse_cluster: String,

    /// Optional authoritative YAML topology file for shard-local ClickHouse connections.
    /// In shard-direct scope, this skips system.clusters discovery and uses the YAML mapping.
    /// Distributed scope ignores this setting.
    #[arg(long, env = "CLICKHOUSE_TOPOLOGY_CONFIG")]
    pub(crate) clickhouse_topology_config: Option<String>,

    /// Local gsfa table used by shard-direct address queries.
    #[arg(long, env = "CLICKHOUSE_GSFA_LOCAL_TABLE")]
    pub(crate) clickhouse_gsfa_local_table: Option<String>,

    /// Local signatures table used in shard-direct scope.
    /// Defaults to CLICKHOUSE_SIGNATURE_STATUSES_TABLE + _local.
    #[arg(long, env = "CLICKHOUSE_SIGNATURES_LOCAL_TABLE")]
    pub(crate) clickhouse_signatures_local_table: Option<String>,

    /// Local token owner activity table used in shard-direct scope.
    /// Defaults to CLICKHOUSE_TOKEN_OWNER_ACTIVITY_TABLE + _local.
    #[arg(long, env = "CLICKHOUSE_TOKEN_OWNER_ACTIVITY_LOCAL_TABLE")]
    pub(crate) clickhouse_token_owner_activity_local_table: Option<String>,

    /// Local transactions table used in shard-direct scope.
    /// Defaults to CLICKHOUSE_TRANSACTION_TABLE + _local.
    #[arg(long, env = "CLICKHOUSE_TRANSACTIONS_LOCAL_TABLE")]
    pub(crate) clickhouse_transactions_local_table: Option<String>,

    /// Local blocks metadata table used in shard-direct scope.
    /// Defaults to CLICKHOUSE_BLOCKS_METADATA_TABLE + _local.
    #[arg(long, env = "CLICKHOUSE_BLOCKS_METADATA_LOCAL_TABLE")]
    pub(crate) clickhouse_blocks_metadata_local_table: Option<String>,

    /// Override the shard HTTP port in shard-direct scope.
    /// Defaults to the port in CLICKHOUSE_URL.
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

    /// Local GSFA hot table used by shard-direct hot-address fanout.
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
    /// Clamp an explicit getBlocks/getBlocksWithLimit end to the trusted head tip.
    /// `false` keeps the requested end and asks the primary for slots above the tip.
    #[arg(
        long,
        env = "GET_BLOCKS_CLAMP_TO_HEAD_TIP",
        default_value_t = true,
        action = ArgAction::Set
    )]
    pub(crate) get_blocks_clamp_to_head_tip: bool,

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

    // --- Optional local ClickHouse cache of recent finalized slots ---
    #[cfg(feature = "disk-cache")]
    /// Enable the localhost ClickHouse forward cache of recent finalized slots.
    #[arg(long, env = "DISK_CACHE_ENABLED", default_value_t = false)]
    pub(crate) disk_cache_enabled: bool,

    #[cfg(feature = "disk-cache")]
    /// Local ClickHouse HTTP endpoint.
    #[arg(
        long,
        env = "DISK_CACHE_CLICKHOUSE_URL",
        default_value = "http://127.0.0.1:8123"
    )]
    pub(crate) disk_cache_clickhouse_url: String,

    #[cfg(feature = "disk-cache")]
    /// Dedicated database owned by the disposable cache.
    #[arg(
        long,
        env = "DISK_CACHE_CLICKHOUSE_DATABASE",
        default_value = "superbank_disk_cache"
    )]
    pub(crate) disk_cache_clickhouse_database: String,

    #[cfg(feature = "disk-cache")]
    #[arg(long, env = "DISK_CACHE_CLICKHOUSE_USER", default_value = "default")]
    pub(crate) disk_cache_clickhouse_user: String,

    #[cfg(feature = "disk-cache")]
    #[arg(long, env = "DISK_CACHE_CLICKHOUSE_PASSWORD", default_value = "")]
    pub(crate) disk_cache_clickhouse_password: String,

    #[cfg(feature = "disk-cache")]
    /// Make local-cache initialization and health mandatory. RPC reads still
    /// fall back to the source cluster on individual cache failures.
    #[arg(long, env = "DISK_CACHE_REQUIRED", default_value_t = false)]
    pub(crate) disk_cache_required: bool,

    #[cfg(feature = "disk-cache")]
    /// Finalized slots to retain. Required when the cache is enabled.
    #[arg(long, env = "DISK_CACHE_RETAIN_SLOTS", value_parser = clap::value_parser!(u64).range(1..))]
    pub(crate) disk_cache_retain_slots: Option<u64>,

    #[cfg(feature = "disk-cache")]
    /// MergeTree byte budget; 0 = unlimited. The cache drops oldest complete
    /// slot partitions until usage falls below the low-water mark.
    #[arg(long, env = "DISK_CACHE_MAX_BYTES", default_value_t = 0)]
    pub(crate) disk_cache_max_bytes: u64,

    #[cfg(feature = "disk-cache")]
    /// Slot width of local MergeTree partitions. Omit to derive a width which
    /// keeps at most 128 active partitions.
    #[arg(long, env = "DISK_CACHE_PARTITION_SLOTS", value_parser = clap::value_parser!(u64).range(1..))]
    pub(crate) disk_cache_partition_slots: Option<u64>,

    #[cfg(feature = "disk-cache")]
    /// Timeout for one local cache read.
    #[arg(long, env = "DISK_CACHE_QUERY_TIMEOUT_MS", default_value_t = 2_000, value_parser = clap::value_parser!(u64).range(1..))]
    pub(crate) disk_cache_query_timeout_ms: u64,

    #[cfg(feature = "disk-cache")]
    /// Budget for one local getTransaction attempt; the primary starts when it expires.
    /// Capped at DISK_CACHE_QUERY_TIMEOUT_MS.
    #[arg(long, env = "DISK_CACHE_GET_TX_TIMEOUT_MS", default_value_t = 1_000, value_parser = clap::value_parser!(u64).range(1..))]
    pub(crate) disk_cache_get_tx_timeout_ms: u64,

    #[cfg(feature = "disk-cache")]
    /// Resolve a local getTransaction position and read its payload in one query, falling
    /// back to the two-step lookup. `false` uses only the two-step lookup.
    #[arg(
        long,
        env = "DISK_CACHE_FUSED_GET_TX",
        default_value_t = true,
        action = ArgAction::Set
    )]
    pub(crate) disk_cache_fused_get_tx: bool,

    #[cfg(feature = "disk-cache")]
    /// After an empty fused getTransaction read over several candidate partitions, ask the
    /// whole span for a position once before the per-partition probes. `false` always runs
    /// the per-partition probes.
    #[arg(
        long,
        env = "DISK_CACHE_GET_TX_SPAN_CHECK",
        default_value_t = true,
        action = ArgAction::Set
    )]
    pub(crate) disk_cache_get_tx_span_check: bool,

    #[cfg(feature = "disk-cache")]
    /// Serve a local getTransaction hit that raced an eviction while its slot stays covered.
    /// `false` discards every read that raced an eviction.
    #[arg(
        long,
        env = "DISK_CACHE_EVICTION_SAFE_HITS",
        default_value_t = true,
        action = ArgAction::Set
    )]
    pub(crate) disk_cache_eviction_safe_hits: bool,

    #[cfg(feature = "disk-cache")]
    /// Look up local signature statuses with one query over the candidate slot span.
    /// `false` queries each candidate partition in turn.
    #[arg(
        long,
        env = "DISK_CACHE_STATUS_SPAN_QUERY",
        default_value_t = true,
        action = ArgAction::Set
    )]
    pub(crate) disk_cache_status_span_query: bool,

    #[cfg(feature = "disk-cache")]
    /// Write new local `transactions` parts as Compact (see README). `false` resets the
    /// layout settings so new parts use the server default again.
    #[arg(
        long,
        env = "DISK_CACHE_COMPACT_TRANSACTIONS_PARTS",
        default_value_t = false
    )]
    pub(crate) disk_cache_compact_transactions_parts: bool,

    #[cfg(feature = "disk-cache")]
    /// Race the local getSignaturesForAddress page against the primary's full page.
    /// `false` awaits the local page first and asks the primary only for the remainder.
    #[arg(
        long,
        env = "GSFA_RACE_PRIMARY",
        default_value_t = true,
        action = ArgAction::Set
    )]
    pub(crate) gsfa_race_primary: bool,

    #[cfg(feature = "disk-cache")]
    /// Budget for one local getTransaction attempt while the signature index has more
    /// than 4 unknown-membership partitions (after a restart). Capped at the
    /// getTransaction budget; set it equal to DISK_CACHE_GET_TX_TIMEOUT_MS to disable.
    #[arg(long, env = "DISK_CACHE_GET_TX_UNKNOWN_TIMEOUT_MS", default_value_t = 150, value_parser = clap::value_parser!(u64).range(1..))]
    pub(crate) disk_cache_get_tx_unknown_timeout_ms: u64,

    #[cfg(feature = "disk-cache")]
    /// Shared cache budget for one address request, including cursor lookup and hydration.
    #[arg(long, env = "DISK_CACHE_ADDRESS_QUERY_TIMEOUT_MS", default_value_t = 100, value_parser = clap::value_parser!(u64).range(1..))]
    pub(crate) disk_cache_address_query_timeout_ms: u64,

    #[cfg(feature = "disk-cache")]
    /// How long getSignaturesForAddress remembers that an address had no primary rows at or
    /// below a finalized slot, so a repeat request is answered from the head and local tiers
    /// when they prove the newer range. 0 disables the watermark cache.
    #[arg(
        long,
        env = "DISK_CACHE_GSFA_EMPTY_WATERMARK_TTL_SECS",
        default_value_t = 0
    )]
    pub(crate) disk_cache_gsfa_empty_watermark_ttl_secs: u64,

    #[cfg(feature = "disk-cache")]
    /// Maximum addresses in the getSignaturesForAddress empty-address watermark cache
    /// (about 128 bytes each).
    #[arg(long, env = "DISK_CACHE_GSFA_EMPTY_WATERMARK_MAX_ENTRIES", default_value_t = 100_000, value_parser = clap::value_parser!(u64).range(1..=10_000_000))]
    pub(crate) disk_cache_gsfa_empty_watermark_max_entries: u64,

    #[cfg(feature = "disk-cache")]
    /// Total partition routing index budget, including build buffers.
    #[arg(long, env = "DISK_CACHE_KEY_INDEX_MAX_MEMORY_BYTES", default_value_t = 4_294_967_296, value_parser = clap::value_parser!(u64).range(67_108_864..))]
    pub(crate) disk_cache_key_index_max_memory_bytes: u64,

    #[cfg(feature = "disk-cache")]
    /// Concurrent local interactive queries.
    #[arg(long, env = "DISK_CACHE_QUERY_CONCURRENCY", default_value_t = 8, value_parser = clap::value_parser!(u64).range(1..=64))]
    pub(crate) disk_cache_query_concurrency: u64,

    #[cfg(feature = "disk-cache")]
    /// Concurrent local background reads (coverage reloads, fill count validation,
    /// signature-membership scans). Defaults to min(query concurrency, 8), so raising
    /// interactive concurrency above 8 does not also raise background load.
    #[arg(long, env = "DISK_CACHE_BACKGROUND_QUERY_CONCURRENCY", value_parser = clap::value_parser!(u64).range(1..=64))]
    pub(crate) disk_cache_background_query_concurrency: Option<u64>,

    #[cfg(feature = "disk-cache")]
    /// Execution threads per local interactive query.
    #[arg(long, env = "DISK_CACHE_QUERY_MAX_THREADS", default_value_t = 2, value_parser = clap::value_parser!(u64).range(1..=16))]
    pub(crate) disk_cache_query_max_threads: u64,

    #[cfg(feature = "disk-cache")]
    /// Interval for checking the source schema fingerprint.
    #[arg(long, env = "DISK_CACHE_SCHEMA_CHECK_INTERVAL_SECS", default_value_t = 300, value_parser = clap::value_parser!(u64).range(1..))]
    pub(crate) disk_cache_schema_check_interval_secs: u64,

    #[cfg(feature = "disk-cache")]
    /// Query-facing tables allowed to use ClickHouse Memory. Version 1 accepts
    /// only blocks_metadata.
    #[arg(long, env = "DISK_CACHE_MEMORY_TABLES", value_delimiter = ',')]
    pub(crate) disk_cache_memory_tables: Vec<String>,

    #[cfg(feature = "disk-cache")]
    #[arg(long, env = "DISK_CACHE_MEMORY_RETAIN_SLOTS", value_parser = clap::value_parser!(u64).range(1..))]
    pub(crate) disk_cache_memory_retain_slots: Option<u64>,

    #[cfg(feature = "disk-cache")]
    #[arg(long, env = "DISK_CACHE_MEMORY_MAX_BYTES", value_parser = clap::value_parser!(u64).range(1..))]
    pub(crate) disk_cache_memory_max_bytes: Option<u64>,

    #[cfg(feature = "disk-cache")]
    /// Enable the durable full-history block-time index and in-process read cache.
    #[arg(long, env = "DISK_CACHE_BLOCK_INDEX_ENABLED", default_value_t = false)]
    pub(crate) disk_cache_block_index_enabled: bool,

    /// Maximum bytes retained by the block index's in-process segment cache.
    #[arg(long, env = "DISK_CACHE_BLOCK_INDEX_MAX_MEMORY_BYTES", value_parser = clap::value_parser!(u64).range(1..))]
    pub(crate) disk_cache_block_index_max_memory_bytes: Option<u64>,

    #[cfg(feature = "disk-cache")]
    #[arg(long, env = "DISK_CACHE_BLOCK_INDEX_SLOTS_PER_QUERY", default_value_t = 250_000, value_parser = clap::value_parser!(u64).range(1..))]
    pub(crate) disk_cache_block_index_slots_per_query: u64,

    #[cfg(feature = "disk-cache")]
    #[arg(long, env = "DISK_CACHE_BLOCK_INDEX_MAX_SLOTS_PER_SEC", default_value_t = 25_000, value_parser = clap::value_parser!(u64).range(1..))]
    pub(crate) disk_cache_block_index_max_slots_per_sec: u64,

    #[cfg(feature = "disk-cache")]
    #[arg(long, env = "DISK_CACHE_BLOCK_INDEX_QUERY_TIMEOUT_MS", default_value_t = 300_000, value_parser = clap::value_parser!(u64).range(1..))]
    pub(crate) disk_cache_block_index_query_timeout_ms: u64,

    #[cfg(feature = "disk-cache")]
    /// Enable the source-to-local ClickHouse forward/repair task (disable for debugging only).
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
    /// Maximum number of independent source-to-local ranges forwarded at once.
    #[arg(
        long,
        env = "DISK_CACHE_BACKFILL_CONCURRENCY",
        default_value_t = 4,
        value_parser = clap::value_parser!(u64).range(1..=64)
    )]
    pub(crate) disk_cache_backfill_concurrency: u64,

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

    #[cfg(feature = "disk-cache")]
    /// Seconds before the forwarder retries a slot it gave up on, in case the
    /// source has since backfilled it; `0` keeps given-up slots missing until
    /// they leave the retention window.
    #[arg(long, env = "DISK_CACHE_GIVEN_UP_RETRY_SECS", default_value_t = 600)]
    pub(crate) disk_cache_given_up_retry_secs: u64,

    // Recognize removed RocksDB settings for one release so operators receive a
    // useful error instead of silently believing they still take effect.
    #[cfg(feature = "disk-cache")]
    #[arg(long, env = "DISK_CACHE_PATH", hide = true)]
    pub(crate) deprecated_disk_cache_path: Option<String>,

    #[cfg(feature = "disk-cache")]
    #[arg(long, env = "DISK_CACHE_BLOCK_CACHE_BYTES", hide = true)]
    pub(crate) deprecated_disk_cache_block_cache_bytes: Option<usize>,

    #[cfg(feature = "disk-cache")]
    #[arg(long, env = "DISK_CACHE_WRITE_QUEUE_SLOTS", hide = true)]
    pub(crate) deprecated_disk_cache_write_queue_slots: Option<usize>,

    #[cfg(feature = "disk-cache")]
    #[arg(long, env = "DISK_CACHE_READ_CONCURRENCY", hide = true)]
    pub(crate) deprecated_disk_cache_read_concurrency: Option<usize>,

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

    /// Background local-read lane size. Unset keeps the historical lane (equal to query
    /// concurrency) up to 8 and stops it growing with interactive concurrency beyond that.
    #[cfg(feature = "disk-cache")]
    pub(crate) fn disk_cache_background_query_concurrency(&self) -> u64 {
        self.disk_cache_background_query_concurrency
            .unwrap_or_else(|| {
                self.disk_cache_query_concurrency
                    .min(DEFAULT_DISK_CACHE_BACKGROUND_QUERY_CONCURRENCY_CAP)
            })
    }
}

/// Upper bound of the default background lane: the lane size every deployment had at the
/// default `DISK_CACHE_QUERY_CONCURRENCY=8` before the lane was configurable.
#[cfg(feature = "disk-cache")]
const DEFAULT_DISK_CACHE_BACKGROUND_QUERY_CONCURRENCY_CAP: u64 = 8;

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
    fn verification_timeout_defaults_overrides_and_validation() {
        // Other feature tests parse configuration without ENV_LOCK. Isolate all env mutation.
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "config::config_tests::verification_timeout_config_in_isolation",
                "--ignored",
                "--nocapture",
            ])
            .env_clear()
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    #[ignore = "run by verification_timeout_defaults_overrides_and_validation in an isolated process"]
    fn verification_timeout_config_in_isolation() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        let cfg = RpcConfig::parse_from(["superbank-rpc"]);
        assert_eq!(cfg.clickhouse_startup_verification_timeout_ms, 10_000);
        assert_eq!(cfg.clickhouse_runtime_verification_timeout_ms, 10_000);
        for (flag, env, startup) in [
            (
                "--clickhouse-startup-verification-timeout-ms",
                "CLICKHOUSE_STARTUP_VERIFICATION_TIMEOUT_MS",
                true,
            ),
            (
                "--clickhouse-runtime-verification-timeout-ms",
                "CLICKHOUSE_RUNTIME_VERIFICATION_TIMEOUT_MS",
                false,
            ),
        ] {
            let values = |cfg: RpcConfig| {
                if startup {
                    (
                        cfg.clickhouse_startup_verification_timeout_ms,
                        cfg.clickhouse_runtime_verification_timeout_ms,
                    )
                } else {
                    (
                        cfg.clickhouse_runtime_verification_timeout_ms,
                        cfg.clickhouse_startup_verification_timeout_ms,
                    )
                }
            };
            assert_eq!(
                values(RpcConfig::parse_from(["superbank-rpc", flag, "1234"])),
                (1234, 10_000)
            );
            let _env = EnvVarGuard::set(env, "2345");
            assert_eq!(
                values(RpcConfig::parse_from(["superbank-rpc"])),
                (2345, 10_000)
            );
            assert_eq!(
                values(RpcConfig::parse_from(["superbank-rpc", flag, "3456"])),
                (3456, 10_000)
            );
            for invalid in ["0", "-1", "1.5", "no", "18446744073709551616"] {
                assert!(RpcConfig::try_parse_from(["superbank-rpc", flag, invalid]).is_err());
                let _bad_env = EnvVarGuard::set(env, invalid);
                assert!(RpcConfig::try_parse_from(["superbank-rpc"]).is_err());
            }
        }
        let _startup = EnvVarGuard::set("CLICKHOUSE_STARTUP_VERIFICATION_TIMEOUT_MS", "12000");
        let _runtime = EnvVarGuard::set("CLICKHOUSE_RUNTIME_VERIFICATION_TIMEOUT_MS", "13000");
        let cfg = RpcConfig::parse_from(["superbank-rpc"]);
        assert_eq!(cfg.clickhouse_startup_verification_timeout_ms, 12000);
        assert_eq!(cfg.clickhouse_runtime_verification_timeout_ms, 13000);
    }

    #[test]
    fn signature_status_history_cache_flags() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        let cfg = RpcConfig::parse_from(["superbank-rpc"]);
        assert_eq!(cfg.signature_status_history_cache_entries, 0);
        assert_eq!(
            cfg.signature_status_history_cache_max_bytes,
            64 * 1024 * 1024
        );
        assert_eq!(cfg.signature_status_history_cache_ttl_secs, 300);

        let _entries = EnvVarGuard::set("SIGNATURE_STATUS_HISTORY_CACHE_ENTRIES", "100000");
        let _bytes = EnvVarGuard::set("SIGNATURE_STATUS_HISTORY_CACHE_MAX_BYTES", "1048576");
        let _ttl = EnvVarGuard::set("SIGNATURE_STATUS_HISTORY_CACHE_TTL_SECS", "60");
        let cfg = RpcConfig::parse_from(["superbank-rpc"]);
        assert_eq!(cfg.signature_status_history_cache_entries, 100_000);
        assert_eq!(cfg.signature_status_history_cache_max_bytes, 1_048_576);
        assert_eq!(cfg.signature_status_history_cache_ttl_secs, 60);

        for ttl in ["0", "301"] {
            assert!(
                RpcConfig::try_parse_from([
                    "superbank-rpc",
                    "--signature-status-history-cache-ttl-secs",
                    ttl,
                ])
                .is_err(),
                "{ttl}"
            );
        }
    }

    #[test]
    fn signature_status_limits_cli_overrides() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        let cfg = RpcConfig::parse_from([
            "superbank-rpc",
            "--get-signature-statuses-max-concurrency",
            "3",
            "--get-signature-statuses-max-threads",
            "1",
            "--clickhouse-cluster",
            "rbx2",
        ]);
        assert_eq!(cfg.get_signature_statuses_max_concurrency, 3);
        assert_eq!(cfg.get_signature_statuses_max_threads, 1);
        assert_eq!(cfg.clickhouse_cluster, "rbx2");
    }

    #[test]
    fn clickhouse_query_cache_defaults() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        let cfg = RpcConfig::parse_from(["superbank-rpc"]);

        assert!(!cfg.clickhouse_query_cache_enabled);
        assert_eq!(cfg.clickhouse_query_cache_ttl_seconds, 1);
        assert!(!cfg.clickhouse_query_cache_share_between_users);
        assert!(!cfg.clickhouse_query_condition_cache_enabled);
        assert!(!cfg.clickhouse_get_transaction_single_round_trip);
        assert!(cfg.clickhouse_latest_slot_hint);
        assert!(!cfg.clickhouse_transactions_for_address_position_tokens);
        assert!(!cfg.clickhouse_transactions_for_address_cursor_cache);
        assert!(cfg.clickhouse_transactions_for_address_union_pushdown);
        assert!(!cfg.clickhouse_gsfa_inline_cursor);
        assert!(!cfg.emit_http_errors);
        assert_eq!(cfg.get_block_response_cache_max_bytes, 0);
        assert_eq!(cfg.get_transaction_primary_cache_max_bytes, 0);
        assert_eq!(cfg.get_transaction_primary_cache_ttl_secs, 600);
        assert!(cfg.get_block_response_cache_share_confirmed);
        assert!(cfg.get_block_response_cache_unsupported_version);
        assert_eq!(cfg.get_block_hydration_parallelism, 4);
        assert!(!cfg.rpc_response_gzip_enabled);
        assert!(!cfg.metrics_capture_x_endpoint());
        assert!(!cfg.metrics_capture_x_rpc_node());
        assert!(!cfg.metrics_capture_x_subscription_id());
        assert!(!cfg.metrics_capture_x_account_id());
        assert_eq!(cfg.get_signature_statuses_max_concurrency, 4);
        assert_eq!(cfg.get_signature_statuses_max_threads, 2);
        assert_eq!(cfg.get_inflation_reward_max_addresses, 100);
        assert_eq!(cfg.get_inflation_reward_max_concurrency, 20);
        assert_eq!(cfg.get_inflation_reward_query_timeout_ms, 5_000);
        assert_eq!(cfg.get_inflation_reward_max_threads, 2);
        assert_eq!(cfg.get_inflation_reward_max_memory_bytes, 536_870_912);
        assert_eq!(cfg.get_inflation_reward_max_bytes_to_read, 536_870_912);
        assert_eq!(cfg.get_inflation_reward_epoch_cache_max_bytes, 16_777_216);
    }

    #[test]
    fn shared_config_path_flag_parses() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        let cfg = RpcConfig::parse_from(["superbank-rpc", "--config", "superbank.yaml"]);

        assert_eq!(
            cfg.config.as_deref(),
            Some(std::path::Path::new("superbank.yaml"))
        );
    }

    #[test]
    fn inflation_reward_limits_parse() {
        let cfg = RpcConfig::parse_from([
            "superbank-rpc",
            "--get-inflation-reward-max-addresses",
            "50",
            "--get-inflation-reward-max-concurrency",
            "3",
            "--get-inflation-reward-query-timeout-ms",
            "4000",
            "--get-inflation-reward-max-threads",
            "4",
            "--get-inflation-reward-max-memory-bytes",
            "268435456",
            "--get-inflation-reward-max-bytes-to-read",
            "1073741824",
            "--get-inflation-reward-epoch-cache-max-bytes",
            "0",
        ]);

        assert_eq!(cfg.get_inflation_reward_max_addresses, 50);
        assert_eq!(cfg.get_inflation_reward_max_concurrency, 3);
        assert_eq!(cfg.get_inflation_reward_query_timeout_ms, 4_000);
        assert_eq!(cfg.get_inflation_reward_max_threads, 4);
        assert_eq!(cfg.get_inflation_reward_max_memory_bytes, 268_435_456);
        assert_eq!(cfg.get_inflation_reward_max_bytes_to_read, 1_073_741_824);
        assert_eq!(cfg.get_inflation_reward_epoch_cache_max_bytes, 0);
    }

    #[test]
    fn inflation_reward_admission_limits_accept_zero_as_disabled() {
        let cfg = RpcConfig::parse_from([
            "superbank-rpc",
            "--get-inflation-reward-max-addresses",
            "0",
            "--get-inflation-reward-max-concurrency",
            "0",
        ]);

        assert_eq!(cfg.get_inflation_reward_max_addresses, 0);
        assert_eq!(cfg.get_inflation_reward_max_concurrency, 0);
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
    fn get_transaction_single_round_trip_flag_parses() {
        let cfg = RpcConfig::parse_from([
            "superbank-rpc",
            "--clickhouse-get-transaction-single-round-trip",
        ]);

        assert!(cfg.clickhouse_get_transaction_single_round_trip);
    }

    #[test]
    fn transactions_for_address_position_tokens_flag_and_env_parse() {
        let cfg = RpcConfig::parse_from([
            "superbank-rpc",
            "--clickhouse-transactions-for-address-position-tokens",
        ]);
        assert!(cfg.clickhouse_transactions_for_address_position_tokens);

        let _guard = ENV_LOCK.lock().expect("env lock");
        let _env = EnvVarGuard::set(
            "CLICKHOUSE_TRANSACTIONS_FOR_ADDRESS_POSITION_TOKENS",
            "true",
        );
        let cfg = RpcConfig::parse_from(["superbank-rpc"]);
        assert!(cfg.clickhouse_transactions_for_address_position_tokens);
    }

    #[test]
    fn transactions_for_address_cursor_cache_flag_and_env_parse() {
        let cfg = RpcConfig::parse_from([
            "superbank-rpc",
            "--clickhouse-transactions-for-address-cursor-cache",
        ]);
        assert!(cfg.clickhouse_transactions_for_address_cursor_cache);

        let _guard = ENV_LOCK.lock().expect("env lock");
        let _env = EnvVarGuard::set("CLICKHOUSE_TRANSACTIONS_FOR_ADDRESS_CURSOR_CACHE", "true");
        let cfg = RpcConfig::parse_from(["superbank-rpc"]);
        assert!(cfg.clickhouse_transactions_for_address_cursor_cache);
    }

    #[test]
    fn gsfa_inline_cursor_env_parses() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        let _env = EnvVarGuard::set("CLICKHOUSE_GSFA_INLINE_CURSOR", "true");

        let cfg = RpcConfig::parse_from(["superbank-rpc"]);

        assert!(cfg.clickhouse_gsfa_inline_cursor);
    }

    #[cfg(feature = "disk-cache")]
    #[test]
    fn gsfa_empty_watermark_defaults_off_and_env_parses() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        let cfg = RpcConfig::parse_from(["superbank-rpc"]);
        assert_eq!(cfg.disk_cache_gsfa_empty_watermark_ttl_secs, 0);
        assert_eq!(cfg.disk_cache_gsfa_empty_watermark_max_entries, 100_000);

        let _ttl = EnvVarGuard::set("DISK_CACHE_GSFA_EMPTY_WATERMARK_TTL_SECS", "600");
        let _max = EnvVarGuard::set("DISK_CACHE_GSFA_EMPTY_WATERMARK_MAX_ENTRIES", "5000");
        let cfg = RpcConfig::parse_from(["superbank-rpc"]);
        assert_eq!(cfg.disk_cache_gsfa_empty_watermark_ttl_secs, 600);
        assert_eq!(cfg.disk_cache_gsfa_empty_watermark_max_entries, 5000);
    }

    #[test]
    fn get_transaction_single_round_trip_env_parses() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        let _env = EnvVarGuard::set("CLICKHOUSE_GET_TRANSACTION_SINGLE_ROUND_TRIP", "true");

        let cfg = RpcConfig::parse_from(["superbank-rpc"]);

        assert!(cfg.clickhouse_get_transaction_single_round_trip);
    }

    /// Rollout switches: default-on kill switches take an explicit value on the command
    /// line, so each can be turned off by flag as well as by environment.
    #[test]
    fn rollout_switch_flags_parse() {
        let cfg = RpcConfig::parse_from(["superbank-rpc", "--clickhouse-latest-slot-hint=false"]);
        assert!(!cfg.clickhouse_latest_slot_hint);
        assert!(!cfg.clickhouse_signatures_owner_shard_routing);
        let cfg = RpcConfig::parse_from([
            "superbank-rpc",
            "--clickhouse-signatures-owner-shard-routing=true",
        ]);
        assert!(cfg.clickhouse_signatures_owner_shard_routing);
        assert!(
            RpcConfig::try_parse_from([
                "superbank-rpc",
                "--clickhouse-signatures-owner-shard-routing=yes please",
            ])
            .is_err()
        );
        let cfg = RpcConfig::parse_from([
            "superbank-rpc",
            "--get-block-response-cache-unsupported-version=false",
        ]);
        assert!(!cfg.get_block_response_cache_unsupported_version);
        let cfg = RpcConfig::parse_from([
            "superbank-rpc",
            "--clickhouse-transactions-for-address-union-pushdown=false",
        ]);
        assert!(!cfg.clickhouse_transactions_for_address_union_pushdown);
        let cfg = RpcConfig::parse_from(["superbank-rpc", "--clickhouse-latest-slot-hint", "true"]);
        assert!(cfg.clickhouse_latest_slot_hint);
        assert!(
            RpcConfig::try_parse_from(["superbank-rpc", "--clickhouse-latest-slot-hint=maybe"])
                .is_err()
        );
        #[cfg(feature = "grpc-head-cache")]
        {
            assert!(RpcConfig::parse_from(["superbank-rpc"]).get_blocks_clamp_to_head_tip);
            let cfg =
                RpcConfig::parse_from(["superbank-rpc", "--get-blocks-clamp-to-head-tip=false"]);
            assert!(!cfg.get_blocks_clamp_to_head_tip);
        }
        #[cfg(feature = "disk-cache")]
        {
            let cfg = RpcConfig::parse_from([
                "superbank-rpc",
                "--disk-cache-fused-get-tx=false",
                "--disk-cache-get-tx-span-check=false",
                "--disk-cache-eviction-safe-hits=false",
                "--disk-cache-status-span-query=false",
                "--gsfa-race-primary=false",
                "--disk-cache-compact-transactions-parts",
            ]);
            assert!(!cfg.disk_cache_fused_get_tx);
            assert!(!cfg.disk_cache_get_tx_span_check);
            assert!(!cfg.disk_cache_eviction_safe_hits);
            assert!(!cfg.disk_cache_status_span_query);
            assert!(!cfg.gsfa_race_primary);
            assert!(cfg.disk_cache_compact_transactions_parts);
        }
    }

    #[test]
    fn rollout_switch_env_overrides() {
        // Other tests parse defaults without ENV_LOCK. Isolate all env mutation.
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "config::config_tests::rollout_switch_env_in_isolation",
                "--ignored",
                "--nocapture",
            ])
            .env_clear()
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("1 passed"),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
    }

    #[test]
    #[ignore = "run by rollout_switch_env_overrides in an isolated process"]
    fn rollout_switch_env_in_isolation() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        let _hint = EnvVarGuard::set("CLICKHOUSE_LATEST_SLOT_HINT", "false");
        let _unsupported =
            EnvVarGuard::set("GET_BLOCK_RESPONSE_CACHE_UNSUPPORTED_VERSION", "false");
        let _pushdown = EnvVarGuard::set(
            "CLICKHOUSE_TRANSACTIONS_FOR_ADDRESS_UNION_PUSHDOWN",
            "false",
        );
        #[cfg(feature = "grpc-head-cache")]
        let _clamp = EnvVarGuard::set("GET_BLOCKS_CLAMP_TO_HEAD_TIP", "false");
        #[cfg(feature = "disk-cache")]
        let _disk = [
            EnvVarGuard::set("DISK_CACHE_FUSED_GET_TX", "false"),
            EnvVarGuard::set("DISK_CACHE_GET_TX_SPAN_CHECK", "false"),
            EnvVarGuard::set("DISK_CACHE_EVICTION_SAFE_HITS", "false"),
            EnvVarGuard::set("DISK_CACHE_STATUS_SPAN_QUERY", "false"),
            EnvVarGuard::set("GSFA_RACE_PRIMARY", "false"),
            EnvVarGuard::set("DISK_CACHE_COMPACT_TRANSACTIONS_PARTS", "true"),
        ];
        let cfg = RpcConfig::parse_from(["superbank-rpc"]);
        assert!(!cfg.clickhouse_latest_slot_hint);
        assert!(!cfg.get_block_response_cache_unsupported_version);
        assert!(!cfg.clickhouse_transactions_for_address_union_pushdown);
        #[cfg(feature = "grpc-head-cache")]
        assert!(!cfg.get_blocks_clamp_to_head_tip);
        #[cfg(feature = "disk-cache")]
        {
            assert!(!cfg.disk_cache_fused_get_tx);
            assert!(!cfg.disk_cache_get_tx_span_check);
            assert!(!cfg.disk_cache_eviction_safe_hits);
            assert!(!cfg.disk_cache_status_span_query);
            assert!(!cfg.gsfa_race_primary);
            assert!(cfg.disk_cache_compact_transactions_parts);
        }
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
    fn get_block_response_performance_flags_parse() {
        let cfg = RpcConfig::parse_from([
            "superbank-rpc",
            "--get-block-response-cache-max-bytes",
            "1073741824",
            "--rpc-response-gzip-enabled",
            "--get-transaction-primary-cache-max-bytes",
            "536870912",
            "--get-transaction-primary-cache-ttl-secs",
            "300",
        ]);

        assert_eq!(cfg.get_block_response_cache_max_bytes, 1_073_741_824);
        assert_eq!(cfg.get_transaction_primary_cache_max_bytes, 536_870_912);
        assert_eq!(cfg.get_transaction_primary_cache_ttl_secs, 300);
        assert!(
            RpcConfig::try_parse_from([
                "superbank-rpc",
                "--get-transaction-primary-cache-ttl-secs",
                "0",
            ])
            .is_err()
        );
        assert!(cfg.rpc_response_gzip_enabled);
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

#[cfg(test)]
mod genesis_path_config_tests {
    use clap::Parser;

    use super::RpcConfig;

    #[test]
    fn genesis_path_flag_parses() {
        let cfg = RpcConfig::parse_from([
            "superbank-rpc",
            "--genesis-path",
            "/etc/superbank/genesis.bin",
        ]);

        assert_eq!(
            cfg.genesis_path.as_deref(),
            Some("/etc/superbank/genesis.bin")
        );
    }
}

#[cfg(all(test, feature = "disk-cache"))]
mod disk_cache_config_tests {
    use clap::Parser;

    use super::RpcConfig;

    #[test]
    fn address_cache_budget_is_positive_and_independent() {
        let cfg = RpcConfig::parse_from([
            "superbank-rpc",
            "--disk-cache-address-query-timeout-ms",
            "75",
            "--disk-cache-query-timeout-ms",
            "1500",
        ]);
        assert_eq!(cfg.disk_cache_address_query_timeout_ms, 75);
        assert_eq!(cfg.disk_cache_query_timeout_ms, 1500);
        assert!(
            RpcConfig::try_parse_from([
                "superbank-rpc",
                "--disk-cache-address-query-timeout-ms",
                "0",
            ])
            .is_err()
        );
    }

    #[test]
    fn get_tx_cache_budget_is_positive_and_independent() {
        let cfg = RpcConfig::parse_from(["superbank-rpc", "--disk-cache-get-tx-timeout-ms", "750"]);
        assert_eq!(cfg.disk_cache_get_tx_timeout_ms, 750);
        assert_eq!(cfg.disk_cache_query_timeout_ms, 2_000);
        assert!(
            RpcConfig::try_parse_from(["superbank-rpc", "--disk-cache-get-tx-timeout-ms", "0"])
                .is_err()
        );
    }

    #[test]
    fn get_tx_unknown_cache_budget_is_positive_and_independent() {
        let cfg = RpcConfig::parse_from([
            "superbank-rpc",
            "--disk-cache-get-tx-unknown-timeout-ms",
            "300",
        ]);
        assert_eq!(cfg.disk_cache_get_tx_unknown_timeout_ms, 300);
        assert_eq!(cfg.disk_cache_get_tx_timeout_ms, 1_000);
        assert!(
            RpcConfig::try_parse_from([
                "superbank-rpc",
                "--disk-cache-get-tx-unknown-timeout-ms",
                "0"
            ])
            .is_err()
        );
    }

    #[test]
    fn disk_cache_defaults() {
        let cfg = RpcConfig::parse_from(["superbank-rpc"]);

        assert!(!cfg.disk_cache_enabled);
        assert_eq!(cfg.disk_cache_clickhouse_url, "http://127.0.0.1:8123");
        assert_eq!(cfg.disk_cache_clickhouse_database, "superbank_disk_cache");
        assert_eq!(cfg.disk_cache_retain_slots, None);
        assert_eq!(cfg.disk_cache_max_bytes, 0);
        assert_eq!(cfg.disk_cache_partition_slots, None);
        assert_eq!(cfg.disk_cache_query_timeout_ms, 2_000);
        assert_eq!(cfg.disk_cache_get_tx_timeout_ms, 1_000);
        assert!(cfg.disk_cache_fused_get_tx);
        assert!(cfg.disk_cache_get_tx_span_check);
        assert!(cfg.disk_cache_eviction_safe_hits);
        assert!(cfg.disk_cache_status_span_query);
        assert!(!cfg.disk_cache_compact_transactions_parts);
        assert!(cfg.gsfa_race_primary);
        assert_eq!(cfg.disk_cache_get_tx_unknown_timeout_ms, 150);
        assert_eq!(cfg.disk_cache_address_query_timeout_ms, 100);
        assert!(cfg.disk_cache_memory_tables.is_empty());
        assert_eq!(cfg.disk_cache_memory_retain_slots, None);
        assert_eq!(cfg.disk_cache_memory_max_bytes, None);
        assert!(!cfg.disk_cache_block_index_enabled);
        assert_eq!(cfg.disk_cache_block_index_max_memory_bytes, None);
        assert_eq!(cfg.disk_cache_block_index_slots_per_query, 250_000);
        assert_eq!(cfg.disk_cache_block_index_max_slots_per_sec, 25_000);
        assert_eq!(cfg.disk_cache_block_index_query_timeout_ms, 300_000);
        assert!(cfg.disk_cache_backfill_enabled);
        assert_eq!(cfg.disk_cache_backfill_slots_per_query, 8);
        assert_eq!(cfg.disk_cache_backfill_concurrency, 4);
        assert_eq!(cfg.disk_cache_query_concurrency, 8);
        assert_eq!(cfg.disk_cache_background_query_concurrency, None);
        assert_eq!(cfg.disk_cache_background_query_concurrency(), 8);
        assert_eq!(cfg.disk_cache_backfill_max_slots_per_sec, 50);
        assert_eq!(cfg.disk_cache_backfill_query_timeout_ms, 30_000);
        assert_eq!(cfg.disk_cache_repair_interval_ms, 5_000);
        assert_eq!(cfg.disk_cache_repair_min_lag_slots, 75);
        assert_eq!(cfg.disk_cache_given_up_retry_secs, 600);
    }

    #[test]
    fn disk_cache_flags_parse() {
        let cfg = RpcConfig::parse_from([
            "superbank-rpc",
            "--disk-cache-enabled",
            "--disk-cache-clickhouse-url",
            "http://localhost:18123",
            "--disk-cache-clickhouse-database",
            "local_cache",
            "--disk-cache-retain-slots",
            "432000",
            "--disk-cache-partition-slots",
            "10000",
            "--disk-cache-max-bytes",
            "2199023255552",
            "--disk-cache-block-index-enabled",
            "--disk-cache-block-index-max-memory-bytes",
            "536870912",
            "--disk-cache-block-index-slots-per-query",
            "100000",
            "--disk-cache-block-index-max-slots-per-sec",
            "10000",
            "--disk-cache-block-index-query-timeout-ms",
            "120000",
            "--disk-cache-backfill-max-slots-per-sec",
            "200",
            "--disk-cache-backfill-concurrency",
            "12",
            "--disk-cache-given-up-retry-secs",
            "0",
        ]);

        assert!(cfg.disk_cache_enabled);
        assert_eq!(cfg.disk_cache_clickhouse_url, "http://localhost:18123");
        assert_eq!(cfg.disk_cache_clickhouse_database, "local_cache");
        assert_eq!(cfg.disk_cache_retain_slots, Some(432_000));
        assert_eq!(cfg.disk_cache_partition_slots, Some(10_000));
        assert_eq!(cfg.disk_cache_max_bytes, 2_199_023_255_552);
        assert!(cfg.disk_cache_block_index_enabled);
        assert_eq!(
            cfg.disk_cache_block_index_max_memory_bytes,
            Some(536_870_912)
        );
        assert_eq!(cfg.disk_cache_block_index_slots_per_query, 100_000);
        assert_eq!(cfg.disk_cache_block_index_max_slots_per_sec, 10_000);
        assert_eq!(cfg.disk_cache_block_index_query_timeout_ms, 120_000);
        assert_eq!(cfg.disk_cache_backfill_max_slots_per_sec, 200);
        assert_eq!(cfg.disk_cache_backfill_concurrency, 12);
        assert_eq!(cfg.disk_cache_given_up_retry_secs, 0);
    }

    #[test]
    fn disk_cache_background_lane_default_tracks_query_concurrency_up_to_eight() {
        let lane = |args: &[&str]| {
            let mut argv = vec!["superbank-rpc"];
            argv.extend_from_slice(args);
            RpcConfig::parse_from(argv).disk_cache_background_query_concurrency()
        };
        // Unset: identical to the historical lane (== query concurrency) through 8.
        for q in 1..=8u64 {
            let q_arg = q.to_string();
            assert_eq!(lane(&["--disk-cache-query-concurrency", &q_arg]), q);
        }
        // Unset: independent of interactive concurrency above 8.
        for q in ["9", "16", "24", "64"] {
            assert_eq!(lane(&["--disk-cache-query-concurrency", q]), 8);
        }
        // Explicit values win in both directions.
        assert_eq!(
            lane(&[
                "--disk-cache-query-concurrency",
                "16",
                "--disk-cache-background-query-concurrency",
                "12",
            ]),
            12
        );
        assert_eq!(
            lane(&[
                "--disk-cache-query-concurrency",
                "4",
                "--disk-cache-background-query-concurrency",
                "16",
            ]),
            16
        );
        for invalid in ["0", "65"] {
            assert!(
                RpcConfig::try_parse_from([
                    "superbank-rpc",
                    "--disk-cache-background-query-concurrency",
                    invalid,
                ])
                .is_err()
            );
        }
    }

    #[test]
    fn disk_cache_rejects_invalid_backfill_limits() {
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
        assert!(
            RpcConfig::try_parse_from(["superbank-rpc", "--disk-cache-backfill-concurrency", "0",])
                .is_err()
        );
        assert!(
            RpcConfig::try_parse_from(
                ["superbank-rpc", "--disk-cache-backfill-concurrency", "65",]
            )
            .is_err()
        );
        assert!(
            RpcConfig::try_parse_from([
                "superbank-rpc",
                "--disk-cache-block-index-slots-per-query",
                "0",
            ])
            .is_err()
        );
        assert!(
            RpcConfig::try_parse_from([
                "superbank-rpc",
                "--disk-cache-block-index-max-slots-per-sec",
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
