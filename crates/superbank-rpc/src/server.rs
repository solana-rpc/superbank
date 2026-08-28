// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use axum::{
    Router,
    extract::State,
    http::StatusCode,
    routing::{get, post},
};
use hyper::Error as HyperError;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tower_http::cors::CorsLayer;
use tracing::{info, warn};

use crate::clickhouse::{
    ClickHouseClient, ClickHouseClientOptions, InflationRewardQueryLimits, QueryCacheConfig,
    RoutingPolicy, RoutingScope, RoutingTransport, ShardRoutingConfig,
};
use crate::config::{ClickHouseScope, ClickHouseTransport, RpcConfig};
use crate::handlers::handle_json_rpc_with_headers;
use crate::metrics;
use crate::metrics::metrics_handler;
use crate::processing::ProcessingError;
use crate::state::{AppState, LatestBlockHeightCache, LatestSlotCache, MetricsHeaderCaptureConfig};

#[cfg(feature = "grpc-streaming")]
use crate::grpc::service::{self as superbank_grpc, SuperbankGrpcConfig};

#[cfg(feature = "disk-cache")]
use crate::disk_cache::{DiskCache, DiskCacheConfig, automatic_partition_slots, filler};
#[cfg(feature = "grpc-head-cache")]
use crate::head_cache::dragonsmouth::DragonsmouthHeadCacheConfig;
#[cfg(feature = "grpc-head-cache")]
use crate::head_cache::{HeadCache, dragonsmouth};
#[cfg(feature = "grpc-head-cache")]
use solana_commitment_config::CommitmentLevel;

pub type RpcResult<T> = Result<T, RpcError>;

fn build_shard_routing_config(args: &RpcConfig) -> Option<ShardRoutingConfig> {
    // Distributed mode must remain a pure Distributed-table client.  In particular, hot-address
    // configuration changes the distributed table selected by address queries; it must not make
    // startup depend on system.clusters or shard-local connectivity.
    if args.clickhouse_scope != ClickHouseScope::ShardDirect {
        if args
            .clickhouse_topology_config
            .as_deref()
            .is_some_and(|path| !path.trim().is_empty())
        {
            warn!("CLICKHOUSE_TOPOLOGY_CONFIG is ignored because CLICKHOUSE_SCOPE=distributed");
        }
        return None;
    }

    let topology_config_path = args
        .clickhouse_topology_config
        .as_deref()
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .map(str::to_string);

    if args.clickhouse_scope == ClickHouseScope::ShardDirect {
        Some(ShardRoutingConfig {
            cluster: args.clickhouse_cluster.clone(),
            topology_config_path,
            shard_http_port: args.clickhouse_shard_http_port,
            gsfa_local_table: args.clickhouse_gsfa_local_table.clone(),
            signatures_local_table: args.clickhouse_signatures_local_table.clone(),
            token_owner_activity_local_table: args
                .clickhouse_token_owner_activity_local_table
                .clone(),
            transactions_local_table: args.clickhouse_transactions_local_table.clone(),
            blocks_metadata_local_table: args.clickhouse_blocks_metadata_local_table.clone(),
        })
    } else {
        None
    }
}

fn build_routing_policy(args: &RpcConfig) -> Result<RoutingPolicy, ProcessingError> {
    let transport = match args.clickhouse_transport {
        ClickHouseTransport::Tcp => RoutingTransport::Tcp,
        ClickHouseTransport::Http => RoutingTransport::Http,
    };
    let scope = match args.clickhouse_scope {
        ClickHouseScope::Distributed => RoutingScope::Distributed,
        ClickHouseScope::ShardDirect => RoutingScope::ShardDirect,
    };
    if scope == RoutingScope::Distributed && transport == RoutingTransport::Tcp {
        return Err(ProcessingError::database_msg(
            "Invalid routing policy: CLICKHOUSE_TRANSPORT=tcp requires CLICKHOUSE_SCOPE=shard-direct",
        ));
    }
    Ok(RoutingPolicy { transport, scope })
}

#[derive(Debug, Error)]
pub enum RpcError {
    #[error("ClickHouse initialization failed: {0}")]
    ClickHouse(#[from] ProcessingError),
    #[error("Failed to bind RPC listener: {0}")]
    Bind(#[from] std::io::Error),
    #[error("Server error: {0}")]
    Server(#[from] HyperError),
    #[cfg(feature = "grpc-streaming")]
    #[error("gRPC server error: {0}")]
    Grpc(#[from] tonic::transport::Error),
    #[error("Invalid configuration: {0}")]
    Config(String),
}

pub async fn run_server(args: RpcConfig) -> RpcResult<()> {
    info!("Starting Solana RPC server on {}:{}", args.host, args.port);
    info!(
        transport = ?args.clickhouse_transport,
        scope = ?args.clickhouse_scope,
        "ClickHouse routing policy"
    );
    info!(
        enabled = args.clickhouse_query_cache_enabled,
        ttl_seconds = args.clickhouse_query_cache_ttl_seconds,
        get_transaction_ttl_seconds = args.clickhouse_get_transaction_query_cache_ttl_seconds,
        get_transaction_min_query_runs = args.clickhouse_get_transaction_query_cache_min_query_runs,
        share_between_users = args.clickhouse_query_cache_share_between_users,
        condition_cache_enabled = args.clickhouse_query_condition_cache_enabled,
        "ClickHouse query cache config"
    );
    if args.clickhouse_query_timeout_ms >= args.rpc_request_timeout_ms {
        warn!(
            clickhouse_query_timeout_ms = args.clickhouse_query_timeout_ms,
            rpc_request_timeout_ms = args.rpc_request_timeout_ms,
            "CLICKHOUSE_QUERY_TIMEOUT_MS should remain below RPC_REQUEST_TIMEOUT_MS so the ClickHouse-side query cap fires before the outer RPC timeout"
        );
    }
    if args.get_inflation_reward_max_addresses > 100 {
        return Err(RpcError::Config(
            "GET_INFLATION_REWARD_MAX_ADDRESSES must be 0 (disabled) or between 1 and 100"
                .to_string(),
        ));
    }
    if args.get_inflation_reward_max_threads == 0
        || args.get_inflation_reward_max_memory_bytes == 0
        || args.get_inflation_reward_max_bytes_to_read == 0
        || args.get_inflation_reward_query_timeout_ms == 0
    {
        return Err(RpcError::Config(
            "getInflationReward ClickHouse resource limits must all be greater than zero"
                .to_string(),
        ));
    }
    if args.get_inflation_reward_max_addresses == 0 {
        warn!("getInflationReward address admission limit is disabled");
    }
    if args.get_inflation_reward_max_concurrency == 0 {
        warn!("getInflationReward concurrency admission limit is disabled");
    }
    if args.get_inflation_reward_query_timeout_ms >= args.rpc_request_timeout_ms {
        return Err(RpcError::Config(
            "GET_INFLATION_REWARD_QUERY_TIMEOUT_MS must be below RPC_REQUEST_TIMEOUT_MS"
                .to_string(),
        ));
    }

    // Initialize ClickHouse client
    let shard_routing = build_shard_routing_config(&args);
    let routing_policy = build_routing_policy(&args)?;

    let mut clickhouse = ClickHouseClient::new(
        &args.clickhouse_url,
        &args.clickhouse_database,
        &args.clickhouse_user,
        &args.clickhouse_password,
        ClickHouseClientOptions::new(
            routing_policy,
            shard_routing,
            args.clickhouse_hot_addresses.clone(),
            args.clickhouse_gsfa_hot_table.clone(),
            args.clickhouse_gsfa_hot_local_table.clone(),
        )
        .with_query_timeout(Duration::from_millis(args.clickhouse_query_timeout_ms))
        .with_tcp_access_check_timeout(Duration::from_millis(
            args.clickhouse_tcp_access_check_timeout_ms,
        ))
        .with_replica_health_check_interval(Duration::from_millis(
            args.clickhouse_replica_health_check_interval_ms,
        ))
        .with_query_cache_config(
            QueryCacheConfig::new(
                args.clickhouse_query_cache_enabled,
                args.clickhouse_query_cache_ttl_seconds,
                args.clickhouse_query_cache_share_between_users,
                args.clickhouse_query_condition_cache_enabled,
            )
            .with_get_transaction_overrides(
                args.clickhouse_get_transaction_query_cache_ttl_seconds,
                args.clickhouse_get_transaction_query_cache_min_query_runs,
            ),
        )
        .with_fanout_concurrency(args.clickhouse_shard_fanout_concurrency)
        .with_http_concurrency(args.clickhouse_http_max_concurrency)
        .with_http_connect_timeout(Duration::from_millis(
            args.clickhouse_http_connect_timeout_ms,
        ))
        .with_tcp_pool_sizing(args.clickhouse_tcp_pool_min, args.clickhouse_tcp_pool_max)
        .with_in_clause_chunk(args.clickhouse_in_clause_chunk)
        .with_startup_table_check(args.clickhouse_startup_table_check)
        .with_inflation_reward_limits(InflationRewardQueryLimits {
            query_timeout: Duration::from_millis(args.get_inflation_reward_query_timeout_ms),
            max_threads: args.get_inflation_reward_max_threads,
            max_memory_bytes: args.get_inflation_reward_max_memory_bytes,
            max_bytes_to_read: args.get_inflation_reward_max_bytes_to_read,
        }),
    );

    // Verify ClickHouse connection
    clickhouse.create_tables().await?;
    if !clickhouse.query_settings_enabled() {
        warn!(
            "ClickHouse query SETTINGS are disabled; getInflationReward query shape and RPC admission limits remain active, but ClickHouse-side thread, memory, read-byte, and execution-time caps cannot be applied"
        );
    }

    if let Err(err) = metrics::force_init() {
        warn!("Metrics initialization failed; metrics disabled: {err}");
    }

    #[cfg(feature = "grpc-head-cache")]
    metrics::head_cache_set_active(false);

    #[cfg(feature = "pyroscope")]
    let pyroscope_agent = crate::profiling::start_pyroscope(&args);

    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);

    #[cfg(feature = "disk-cache")]
    validate_disk_cache_args(&args)?;

    #[cfg(feature = "disk-cache")]
    let disk_runtime = if args.disk_cache_enabled {
        Some(start_disk_cache(&args, &clickhouse, &shutdown_tx).await?)
    } else {
        None
    };

    #[cfg(feature = "grpc-head-cache")]
    let (head_cache, head_cache_task): (Option<Arc<HeadCache>>, Option<JoinHandle<()>>) = if args
        .head_cache_enabled
    {
        if let Some(endpoint) = args
            .dragonsmouth_endpoint
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            let min_commitment = parse_commitment_level(&args.head_cache_min_commitment);
            let retain_slots = args.head_cache_retain_slots.max(1);
            let max_per_address = args.max_signatures_limit as usize;

            let cache = Arc::new(HeadCache::new(retain_slots, max_per_address));

            let cfg = DragonsmouthHeadCacheConfig {
                endpoint: endpoint.to_string(),
                x_token: args.dragonsmouth_x_token.clone(),
                max_decoding_bytes: args.grpc_max_decoding_bytes,
                min_commitment,
            };

            let task = tokio::spawn(dragonsmouth::run(cache.clone(), cfg));
            metrics::head_cache_set_active(true);
            (Some(cache), Some(task))
        } else {
            warn!(
                "HEAD_CACHE_ENABLED=true but DRAGONSMOUTH_ENDPOINT is missing; head cache disabled"
            );
            (None, None)
        }
    } else {
        (None, None)
    };
    #[cfg(not(feature = "grpc-head-cache"))]
    let head_cache_task: Option<JoinHandle<()>> = None;

    let state = Arc::new(AppState {
        clickhouse,
        max_signatures_limit: args.max_signatures_limit,
        rpc_max_batch_size: args.rpc_max_batch_size.max(1),
        rpc_batch_concurrency_limit: args.rpc_batch_concurrency_limit.max(1),
        get_inflation_reward_max_addresses: (args.get_inflation_reward_max_addresses > 0)
            .then_some(args.get_inflation_reward_max_addresses),
        get_inflation_reward_sem: (args.get_inflation_reward_max_concurrency > 0).then(|| {
            Arc::new(tokio::sync::Semaphore::new(
                args.get_inflation_reward_max_concurrency,
            ))
        }),
        latest_slot_cache: LatestSlotCache::new(Duration::from_millis(1000)),
        latest_block_height_cache: LatestBlockHeightCache::new(Duration::from_millis(1000)),
        rpc_request_timeout: Duration::from_millis(args.rpc_request_timeout_ms),
        emit_http_errors: args.emit_http_errors,
        metrics_header_capture: MetricsHeaderCaptureConfig {
            capture_x_endpoint: args.metrics_capture_x_endpoint(),
            capture_x_rpc_node: args.metrics_capture_x_rpc_node(),
            capture_x_subscription_id: args.metrics_capture_x_subscription_id(),
            capture_x_account_id: args.metrics_capture_x_account_id(),
        },
        hydration_sem: Arc::new(tokio::sync::Semaphore::new(
            args.hydration_cpu_concurrency.max(1),
        )),
        #[cfg(feature = "grpc-head-cache")]
        head_cache,
        #[cfg(feature = "disk-cache")]
        disk_cache: disk_runtime.as_ref().map(|runtime| runtime.cache.clone()),
    });

    // Build the router
    let rpc_layers = tower::ServiceBuilder::new()
        .layer(axum::extract::DefaultBodyLimit::max(
            args.rpc_max_body_bytes,
        ))
        .layer(tower::limit::ConcurrencyLimitLayer::new(
            args.rpc_concurrency_limit.max(1),
        ))
        .layer(CorsLayer::permissive());

    let app = Router::new()
        .route("/", post(handle_json_rpc_with_headers))
        .route("/health", get(health))
        .layer(rpc_layers)
        .with_state(state.clone());

    // Metrics server on a dedicated listener
    let metrics_app = Router::new().route("/metrics", get(metrics_handler));

    // Start the server
    let addr = format!("{}:{}", args.host, args.port);
    let listener = TcpListener::bind(&addr).await?;
    info!("RPC server listening on http://{}", addr);

    let metrics_addr = format!("{}:{}", args.metrics_host, args.metrics_port);
    let metrics_listener = TcpListener::bind(&metrics_addr).await?;
    info!(
        "Metrics server listening on http://{}/metrics",
        metrics_addr
    );

    #[cfg(feature = "grpc-streaming")]
    let grpc_server = if args.superbank_grpc_enabled {
        let grpc_addr = format!("{}:{}", args.superbank_grpc_host, args.superbank_grpc_port);
        let grpc_listener = TcpListener::bind(&grpc_addr).await?;
        let grpc_config = SuperbankGrpcConfig {
            max_slot_range: args.superbank_grpc_max_slot_range,
            query_timeout: Duration::from_millis(args.superbank_grpc_query_timeout_ms),
            chunk_slots: args.superbank_grpc_chunk_slots,
            max_send_bytes: args.superbank_grpc_max_send_bytes,
            max_concurrent_streams: args.superbank_grpc_max_concurrent_streams,
        };
        info!("gRPC server listening on http://{}", grpc_addr);
        Some(superbank_grpc::serve(
            state.clone(),
            grpc_config,
            grpc_listener,
            shutdown_tx.subscribe(),
        ))
    } else {
        None
    };

    let shutdown_signal_tx = shutdown_tx.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        if let Some(task) = head_cache_task {
            task.abort();
        }
        #[cfg(feature = "pyroscope")]
        if let Some(agent) = pyroscope_agent {
            // `pyroscope` uses threads and blocking IO; stop it in `spawn_blocking`.
            match tokio::task::spawn_blocking(move || match agent.stop() {
                Ok(agent_ready) => agent_ready.shutdown(),
                Err(err) => warn!("pyroscope stop failed: {err}"),
            })
            .await
            {
                Ok(_) => {}
                Err(err) => warn!("pyroscope shutdown task failed: {err}"),
            }
        }
        let _ = shutdown_signal_tx.send(());
    });

    let mut rpc_shutdown_rx = shutdown_tx.subscribe();
    let rpc_server = axum::serve(listener, app).with_graceful_shutdown(async move {
        let _ = rpc_shutdown_rx.recv().await;
    });

    let mut metrics_shutdown_rx = shutdown_tx.subscribe();
    let metrics_server =
        axum::serve(metrics_listener, metrics_app).with_graceful_shutdown(async move {
            let _ = metrics_shutdown_rx.recv().await;
        });

    #[cfg(feature = "grpc-streaming")]
    if let Some(grpc_server) = grpc_server {
        tokio::try_join!(
            async { rpc_server.await.map_err(RpcError::from) },
            async { metrics_server.await.map_err(RpcError::from) },
            async { grpc_server.await.map_err(RpcError::from) },
        )?;
    } else {
        tokio::try_join!(async { rpc_server.await.map_err(RpcError::from) }, async {
            metrics_server.await.map_err(RpcError::from)
        },)?;
    }

    #[cfg(not(feature = "grpc-streaming"))]
    tokio::try_join!(rpc_server, metrics_server)?;

    #[cfg(feature = "disk-cache")]
    if let Some(runtime) = disk_runtime {
        match tokio::time::timeout(Duration::from_secs(10), runtime.supervisor_task).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => warn!("disk cache: supervisor task failed: {err}"),
            Err(_) => warn!("disk cache: supervisor shutdown timed out"),
        }
        metrics::disk_cache_set_active(false);
    }

    Ok(())
}

#[cfg(feature = "disk-cache")]
struct DiskCacheRuntime {
    cache: Arc<tokio::sync::OnceCell<Arc<DiskCache>>>,
    supervisor_task: JoinHandle<()>,
}

#[cfg(feature = "disk-cache")]
fn validate_disk_cache_args(args: &RpcConfig) -> Result<(), RpcError> {
    let removed = [
        args.deprecated_disk_cache_path
            .as_ref()
            .map(|_| "DISK_CACHE_PATH"),
        args.deprecated_disk_cache_block_cache_bytes
            .map(|_| "DISK_CACHE_BLOCK_CACHE_BYTES"),
        args.deprecated_disk_cache_write_queue_slots
            .map(|_| "DISK_CACHE_WRITE_QUEUE_SLOTS"),
        args.deprecated_disk_cache_read_concurrency
            .map(|_| "DISK_CACHE_READ_CONCURRENCY"),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();
    if !removed.is_empty() {
        return Err(RpcError::Config(format!(
            "removed RocksDB disk-cache settings are configured: {}; use the DISK_CACHE_CLICKHOUSE_* settings",
            removed.join(", ")
        )));
    }
    if !args.disk_cache_enabled {
        return Ok(());
    }
    if args.disk_cache_retain_slots.is_none() {
        return Err(RpcError::Config(
            "DISK_CACHE_ENABLED=true requires DISK_CACHE_RETAIN_SLOTS".to_string(),
        ));
    }
    let url = reqwest::Url::parse(args.disk_cache_clickhouse_url.trim())
        .map_err(|err| RpcError::Config(format!("DISK_CACHE_CLICKHOUSE_URL is invalid: {err}")))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(RpcError::Config(
            "DISK_CACHE_CLICKHOUSE_URL must use http or https".to_string(),
        ));
    }
    let local = url.host_str().is_some_and(|host| {
        let host = host.trim_start_matches('[').trim_end_matches(']');
        host.eq_ignore_ascii_case("localhost")
            || host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|address| address.is_loopback())
    });
    if !local {
        return Err(RpcError::Config(
            "DISK_CACHE_CLICKHOUSE_URL must use localhost or a loopback IP address".to_string(),
        ));
    }
    let database = args.disk_cache_clickhouse_database.trim();
    let valid_database = database
        .chars()
        .next()
        .is_some_and(|first| first == '_' || first.is_ascii_alphabetic())
        && database
            .chars()
            .all(|ch| ch == '_' || ch.is_ascii_alphanumeric());
    if !valid_database {
        return Err(RpcError::Config(
            "DISK_CACHE_CLICKHOUSE_DATABASE must contain only ASCII letters, digits, and underscores and must not start with a digit"
                .to_string(),
        ));
    }
    if matches!(
        database.to_ascii_lowercase().as_str(),
        "default" | "system" | "information_schema"
    ) {
        return Err(RpcError::Config(
            "DISK_CACHE_CLICKHOUSE_DATABASE must be a dedicated non-system database".to_string(),
        ));
    }

    let memory_tables = args
        .disk_cache_memory_tables
        .iter()
        .map(|table| table.trim())
        .filter(|table| !table.is_empty())
        .collect::<Vec<_>>();
    if let Some(table) = memory_tables
        .iter()
        .find(|table| **table != "blocks_metadata")
    {
        return Err(RpcError::Config(format!(
            "DISK_CACHE_MEMORY_TABLES does not support {table:?}; only blocks_metadata is safe in this release"
        )));
    }
    if !memory_tables.is_empty()
        && (args.disk_cache_memory_retain_slots.is_none()
            || args.disk_cache_memory_max_bytes.is_none())
    {
        return Err(RpcError::Config(
            "DISK_CACHE_MEMORY_TABLES requires both DISK_CACHE_MEMORY_RETAIN_SLOTS and DISK_CACHE_MEMORY_MAX_BYTES"
                .to_string(),
        ));
    }
    if let (Some(memory), Some(retain)) = (
        args.disk_cache_memory_retain_slots,
        args.disk_cache_retain_slots,
    ) && memory > retain
    {
        return Err(RpcError::Config(
            "DISK_CACHE_MEMORY_RETAIN_SLOTS cannot exceed DISK_CACHE_RETAIN_SLOTS".to_string(),
        ));
    }
    Ok(())
}

#[cfg(feature = "disk-cache")]
async fn start_disk_cache(
    args: &RpcConfig,
    clickhouse: &ClickHouseClient,
    shutdown_tx: &tokio::sync::broadcast::Sender<()>,
) -> Result<DiskCacheRuntime, RpcError> {
    let retain_slots = args.disk_cache_retain_slots.ok_or_else(|| {
        RpcError::Config("DISK_CACHE_ENABLED=true requires DISK_CACHE_RETAIN_SLOTS".to_string())
    })?;
    let memory_blocks_metadata = args
        .disk_cache_memory_tables
        .iter()
        .any(|table| table.trim() == "blocks_metadata");
    let disk_cfg = DiskCacheConfig {
        url: args.disk_cache_clickhouse_url.trim().to_string(),
        database: args.disk_cache_clickhouse_database.trim().to_string(),
        username: args.disk_cache_clickhouse_user.clone(),
        password: args.disk_cache_clickhouse_password.clone(),
        required: args.disk_cache_required,
        retain_slots,
        max_bytes: args.disk_cache_max_bytes,
        partition_slots: args
            .disk_cache_partition_slots
            .unwrap_or_else(|| automatic_partition_slots(retain_slots)),
        query_timeout: Duration::from_millis(args.disk_cache_query_timeout_ms),
        schema_check_interval: Duration::from_secs(args.disk_cache_schema_check_interval_secs),
        memory_blocks_metadata,
        memory_retain_slots: args.disk_cache_memory_retain_slots,
        memory_max_bytes: args.disk_cache_memory_max_bytes,
    };
    info!(
        url = disk_cfg.url,
        database = disk_cfg.database,
        retain_slots = disk_cfg.retain_slots,
        partition_slots = disk_cfg.partition_slots,
        max_bytes = disk_cfg.max_bytes,
        backfill_enabled = args.disk_cache_backfill_enabled,
        "disk cache: starting"
    );

    let filler_cfg = args
        .disk_cache_backfill_enabled
        .then(|| filler::FillerConfig {
            retain_slots,
            slots_per_query: args.disk_cache_backfill_slots_per_query,
            max_concurrency: usize::try_from(args.disk_cache_backfill_concurrency)
                .expect("disk-cache backfill concurrency is bounded to 64"),
            max_slots_per_sec: args.disk_cache_backfill_max_slots_per_sec,
            query_timeout: Duration::from_millis(args.disk_cache_backfill_query_timeout_ms),
            repair_interval: Duration::from_millis(args.disk_cache_repair_interval_ms),
            repair_min_lag_slots: args.disk_cache_repair_min_lag_slots,
            ..Default::default()
        });

    let initial = match DiskCache::open(disk_cfg.clone(), clickhouse).await {
        Ok(cache) => Some(Arc::new(cache)),
        Err(err) if args.disk_cache_required => {
            return Err(RpcError::Config(format!("disk cache open failed: {err}")));
        }
        Err(err) => {
            warn!(
                "disk cache: startup failed; source ClickHouse fallback remains active and initialization will retry: {err}"
            );
            None
        }
    };
    let cache = Arc::new(tokio::sync::OnceCell::new());
    if let Some(initial) = initial.as_ref() {
        cache
            .set(initial.clone())
            .map_err(|_| RpcError::Config("disk cache initialization raced".to_string()))?;
    }
    let supervisor_task = tokio::spawn(run_disk_cache_supervisor(
        cache.clone(),
        initial,
        disk_cfg,
        clickhouse.clone(),
        filler_cfg,
        shutdown_tx.subscribe(),
    ));

    Ok(DiskCacheRuntime {
        cache,
        supervisor_task,
    })
}

#[cfg(feature = "disk-cache")]
async fn run_disk_cache_supervisor(
    published: Arc<tokio::sync::OnceCell<Arc<DiskCache>>>,
    mut cache: Option<Arc<DiskCache>>,
    disk_cfg: DiskCacheConfig,
    source: ClickHouseClient,
    filler_cfg: Option<filler::FillerConfig>,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) {
    let mut retry_delay = Duration::from_secs(1);
    while cache.is_none() {
        tokio::select! {
            _ = shutdown.recv() => return,
            _ = tokio::time::sleep(retry_delay) => {}
        }
        match DiskCache::open(disk_cfg.clone(), &source).await {
            Ok(opened) => {
                let opened = Arc::new(opened);
                if published.set(opened.clone()).is_err() {
                    warn!(
                        "disk cache: a second initialization completed; keeping the published cache"
                    );
                    return;
                }
                cache = Some(opened);
            }
            Err(err) => {
                warn!("disk cache: initialization retry failed: {err}");
                retry_delay = (retry_delay * 2).min(Duration::from_secs(30));
            }
        }
    }

    let cache = cache.expect("cache initialized");
    if let Some(filler_cfg) = filler_cfg {
        filler::run(cache, source, filler_cfg, shutdown).await;
    } else {
        let _ = shutdown.recv().await;
        cache.set_ready(false);
    }
}

async fn health(State(state): State<Arc<AppState>>) -> StatusCode {
    #[cfg(not(feature = "disk-cache"))]
    let _ = state;
    #[cfg(feature = "disk-cache")]
    if let Some(cache) = state.disk_cache()
        && cache.required()
        && !cache.healthy().await
    {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    StatusCode::OK
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            warn!("Failed to install Ctrl+C handler: {err}");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut sigterm) => {
                sigterm.recv().await;
            }
            Err(err) => warn!("Failed to install SIGTERM handler: {err}"),
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

#[cfg(feature = "grpc-head-cache")]
fn parse_commitment_level(value: &str) -> CommitmentLevel {
    let normalized = value.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "processed" => CommitmentLevel::Processed,
        "confirmed" => CommitmentLevel::Confirmed,
        "finalized" => CommitmentLevel::Finalized,
        other => {
            warn!("Invalid HEAD_CACHE_MIN_COMMITMENT '{other}'; defaulting to 'processed'");
            CommitmentLevel::Processed
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "disk-cache")]
    use super::validate_disk_cache_args;
    use super::{build_routing_policy, build_shard_routing_config};
    use crate::clickhouse::{RoutingScope, RoutingTransport};
    use crate::config::{ClickHouseScope, ClickHouseTransport, RpcConfig};

    #[test]
    fn shard_routing_enabled_for_shard_direct_scope() {
        use clap::Parser;

        let _env_lock = crate::config::ENV_TEST_LOCK.lock().expect("env lock");
        let mut cfg = RpcConfig::parse_from(["superbank-rpc"]);
        cfg.clickhouse_scope = ClickHouseScope::ShardDirect;

        let routing = build_shard_routing_config(&cfg).expect("routing config");
        assert_eq!(routing.cluster, "{cluster}");
        assert_eq!(routing.topology_config_path, None);
    }

    #[test]
    fn shard_routing_disabled_for_distributed_scope() {
        use clap::Parser;

        let _env_lock = crate::config::ENV_TEST_LOCK.lock().expect("env lock");
        let mut cfg = RpcConfig::parse_from(["superbank-rpc"]);
        cfg.clickhouse_scope = ClickHouseScope::Distributed;

        assert!(build_shard_routing_config(&cfg).is_none());
    }

    #[test]
    fn shard_routing_disabled_for_hot_routing_in_distributed_scope() {
        use clap::Parser;

        let _env_lock = crate::config::ENV_TEST_LOCK.lock().expect("env lock");
        let mut cfg = RpcConfig::parse_from(["superbank-rpc"]);
        cfg.clickhouse_scope = ClickHouseScope::Distributed;
        cfg.clickhouse_hot_addresses =
            vec!["EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".to_string()];

        assert!(build_shard_routing_config(&cfg).is_none());
    }

    #[test]
    fn shard_routing_disabled_for_topology_config_in_distributed_scope() {
        use clap::Parser;

        let _env_lock = crate::config::ENV_TEST_LOCK.lock().expect("env lock");
        let mut cfg = RpcConfig::parse_from(["superbank-rpc"]);
        cfg.clickhouse_scope = ClickHouseScope::Distributed;
        cfg.clickhouse_topology_config = Some(" /etc/superbank/topology.yaml ".to_string());

        assert!(build_shard_routing_config(&cfg).is_none());
    }

    #[test]
    fn shard_routing_disabled_for_blank_hot_routing_in_distributed_scope() {
        use clap::Parser;

        let _env_lock = crate::config::ENV_TEST_LOCK.lock().expect("env lock");
        let mut cfg = RpcConfig::parse_from(["superbank-rpc"]);
        cfg.clickhouse_scope = ClickHouseScope::Distributed;
        cfg.clickhouse_hot_addresses = vec!["   ".to_string()];

        assert!(build_shard_routing_config(&cfg).is_none());
    }

    #[test]
    fn shard_routing_disabled_for_invalid_hot_routing_in_distributed_scope() {
        use clap::Parser;

        let _env_lock = crate::config::ENV_TEST_LOCK.lock().expect("env lock");
        let mut cfg = RpcConfig::parse_from(["superbank-rpc"]);
        cfg.clickhouse_scope = ClickHouseScope::Distributed;
        cfg.clickhouse_hot_addresses = vec!["not-a-pubkey".to_string()];

        assert!(build_shard_routing_config(&cfg).is_none());
    }

    #[test]
    fn routing_policy_maps_transport_and_scope() {
        use clap::Parser;

        let _env_lock = crate::config::ENV_TEST_LOCK.lock().expect("env lock");
        let mut cfg = RpcConfig::parse_from(["superbank-rpc"]);
        cfg.clickhouse_transport = ClickHouseTransport::Tcp;
        cfg.clickhouse_scope = ClickHouseScope::ShardDirect;

        let policy = build_routing_policy(&cfg).expect("routing policy");
        assert_eq!(policy.transport, RoutingTransport::Tcp);
        assert_eq!(policy.scope, RoutingScope::ShardDirect);
    }

    #[test]
    fn routing_policy_rejects_tcp_distributed_combo() {
        use clap::Parser;

        let _env_lock = crate::config::ENV_TEST_LOCK.lock().expect("env lock");
        let mut cfg = RpcConfig::parse_from(["superbank-rpc"]);
        cfg.clickhouse_transport = ClickHouseTransport::Tcp;
        cfg.clickhouse_scope = ClickHouseScope::Distributed;

        let err = build_routing_policy(&cfg).expect_err("invalid policy should fail");
        assert!(
            err.to_string()
                .contains("CLICKHOUSE_TRANSPORT=tcp requires CLICKHOUSE_SCOPE=shard-direct")
        );
    }

    #[test]
    fn tcp_access_check_timeout_parses_and_defaults() {
        use clap::Parser;

        let cfg = RpcConfig::parse_from(["superbank-rpc"]);
        assert_eq!(cfg.clickhouse_tcp_access_check_timeout_ms, 2_000);

        let cfg = RpcConfig::parse_from([
            "superbank-rpc",
            "--clickhouse-tcp-access-check-timeout-ms",
            "20000",
        ]);
        assert_eq!(cfg.clickhouse_tcp_access_check_timeout_ms, 20_000);
    }

    #[test]
    fn replica_health_check_interval_parses_and_defaults() {
        use clap::Parser;

        let cfg = RpcConfig::parse_from(["superbank-rpc"]);
        assert_eq!(cfg.clickhouse_replica_health_check_interval_ms, 10_000);

        let cfg = RpcConfig::parse_from([
            "superbank-rpc",
            "--clickhouse-replica-health-check-interval-ms",
            "30000",
        ]);
        assert_eq!(cfg.clickhouse_replica_health_check_interval_ms, 30_000);
    }

    #[cfg(feature = "disk-cache")]
    #[test]
    fn disk_cache_requires_a_retention_window() {
        use clap::Parser;

        let _env_lock = crate::config::ENV_TEST_LOCK.lock().expect("env lock");
        let mut cfg = RpcConfig::parse_from(["superbank-rpc"]);
        cfg.disk_cache_enabled = true;
        let err = validate_disk_cache_args(&cfg).expect_err("missing retention must fail");
        assert!(err.to_string().contains("DISK_CACHE_RETAIN_SLOTS"));
    }

    #[cfg(feature = "disk-cache")]
    #[test]
    fn disk_cache_requires_loopback_and_a_dedicated_database() {
        use clap::Parser;

        let _env_lock = crate::config::ENV_TEST_LOCK.lock().expect("env lock");
        let mut cfg = RpcConfig::parse_from(["superbank-rpc"]);
        cfg.disk_cache_enabled = true;
        cfg.disk_cache_retain_slots = Some(1000);
        cfg.disk_cache_clickhouse_url = "http://clickhouse.example:8123".to_string();
        assert!(
            validate_disk_cache_args(&cfg)
                .expect_err("remote endpoint must fail")
                .to_string()
                .contains("loopback")
        );

        cfg.disk_cache_clickhouse_url = "http://[::1]:8123".to_string();
        cfg.disk_cache_clickhouse_database = "default".to_string();
        assert!(
            validate_disk_cache_args(&cfg)
                .expect_err("default database must fail")
                .to_string()
                .contains("dedicated")
        );

        cfg.disk_cache_clickhouse_database = "recent_cache".to_string();
        validate_disk_cache_args(&cfg).expect("loopback cache config");
    }

    #[cfg(feature = "disk-cache")]
    #[test]
    fn disk_cache_rejects_unsafe_memory_and_removed_rocksdb_settings() {
        use clap::Parser;

        let _env_lock = crate::config::ENV_TEST_LOCK.lock().expect("env lock");
        let mut cfg = RpcConfig::parse_from(["superbank-rpc"]);
        cfg.disk_cache_enabled = true;
        cfg.disk_cache_retain_slots = Some(1000);
        cfg.disk_cache_memory_tables = vec!["transactions".to_string()];
        assert!(
            validate_disk_cache_args(&cfg)
                .expect_err("unsafe Memory table must fail")
                .to_string()
                .contains("only blocks_metadata")
        );

        cfg.disk_cache_memory_tables.clear();
        cfg.deprecated_disk_cache_path = Some("/tmp/old-cache".to_string());
        assert!(
            validate_disk_cache_args(&cfg)
                .expect_err("removed setting must fail")
                .to_string()
                .contains("DISK_CACHE_PATH")
        );
    }
}
