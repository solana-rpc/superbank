// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use axum::{
    Router,
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
    ClickHouseClient, ClickHouseClientOptions, QueryCacheConfig, RoutingPolicy, RoutingScope,
    RoutingTransport, ShardRoutingConfig,
};
use crate::config::{
    ClickHouseScope, ClickHouseTransport, RpcConfig, has_usable_gsfa_hot_addresses,
};
use crate::handlers::handle_json_rpc_with_headers;
use crate::metrics;
use crate::metrics::metrics_handler;
use crate::processing::ProcessingError;
use crate::state::{AppState, LatestBlockHeightCache, LatestSlotCache, MetricsHeaderCaptureConfig};

#[cfg(feature = "disk-cache")]
use crate::disk_cache::{
    DiskCache, DiskCacheConfig, filler, ingest::DiskIngestSink, ingest::RepairQueue,
    writer::DiskWriterHandle,
};
#[cfg(feature = "grpc-head-cache")]
use crate::head_cache::dragonsmouth::DragonsmouthHeadCacheConfig;
#[cfg(feature = "grpc-head-cache")]
use crate::head_cache::{HeadCache, dragonsmouth};
#[cfg(feature = "grpc-head-cache")]
use solana_commitment_config::CommitmentLevel;

pub type RpcResult<T> = Result<T, RpcError>;

fn build_shard_routing_config(args: &RpcConfig) -> Option<ShardRoutingConfig> {
    let topology_config_path = args
        .clickhouse_topology_config
        .as_deref()
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .map(str::to_string);

    if args.clickhouse_scope == ClickHouseScope::ShardDirect
        || has_usable_gsfa_hot_addresses(&args.clickhouse_hot_addresses)
        || topology_config_path.is_some()
    {
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
        .with_startup_table_check(args.clickhouse_startup_table_check),
    );

    // Verify ClickHouse connection
    clickhouse.create_tables().await?;

    if let Err(err) = metrics::force_init() {
        warn!("Metrics initialization failed; metrics disabled: {err}");
    }

    #[cfg(feature = "grpc-head-cache")]
    metrics::head_cache_set_active(false);

    #[cfg(feature = "pyroscope")]
    let pyroscope_agent = crate::profiling::start_pyroscope(&args);

    #[cfg(feature = "disk-cache")]
    if args.disk_cache_enabled {
        let endpoint_usable = args
            .dragonsmouth_endpoint
            .as_deref()
            .map(str::trim)
            .is_some_and(|endpoint| !endpoint.is_empty());
        if !args.head_cache_enabled || !endpoint_usable {
            return Err(RpcError::Config(
                "DISK_CACHE_ENABLED=true requires HEAD_CACHE_ENABLED=true and a usable \
                 DRAGONSMOUTH_ENDPOINT (the DragonsMouth stream is the live ingestion source)"
                    .to_string(),
            ));
        }
        if args
            .disk_cache_path
            .as_deref()
            .map(str::trim)
            .is_none_or(str::is_empty)
        {
            return Err(RpcError::Config(
                "DISK_CACHE_ENABLED=true requires DISK_CACHE_PATH".to_string(),
            ));
        }
    }

    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);

    #[cfg(feature = "disk-cache")]
    let mut disk_runtime: Option<DiskCacheRuntime> = None;

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
            #[allow(unused_mut)]
            let mut retain_slots = args.head_cache_retain_slots.max(1);
            #[cfg(feature = "disk-cache")]
            if args.disk_cache_enabled && retain_slots < DISK_CACHE_MIN_HEAD_RETAIN_SLOTS {
                warn!(
                    configured = retain_slots,
                    clamped = DISK_CACHE_MIN_HEAD_RETAIN_SLOTS,
                    "HEAD_CACHE_RETAIN_SLOTS is below the mainnet finalization lag; raising it \
                     so finalized slots are still resident when the disk cache snapshots them"
                );
                retain_slots = DISK_CACHE_MIN_HEAD_RETAIN_SLOTS;
            }
            let max_per_address = args.max_signatures_limit as usize;

            let cache = Arc::new(HeadCache::new(retain_slots, max_per_address));

            #[cfg(feature = "disk-cache")]
            let disk_sink = if args.disk_cache_enabled {
                let runtime = start_disk_cache(&args, &clickhouse, &cache, &shutdown_tx).await?;
                let sink = runtime.sink.clone();
                disk_runtime = Some(runtime);
                Some(sink)
            } else {
                None
            };

            let cfg = DragonsmouthHeadCacheConfig {
                endpoint: endpoint.to_string(),
                x_token: args.dragonsmouth_x_token.clone(),
                max_decoding_bytes: args.grpc_max_decoding_bytes,
                min_commitment,
                #[cfg(feature = "disk-cache")]
                disk_sink,
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
        .route("/health", get(|| async { "OK" }))
        .layer(rpc_layers)
        .with_state(state);

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

    tokio::try_join!(rpc_server, metrics_server)?;

    // The filler heard the shutdown broadcast; stop the writer thread last so
    // every queued slot drains and the WAL is flushed. A timeout only costs a
    // hole that the next start repairs from ClickHouse.
    #[cfg(feature = "disk-cache")]
    if let Some(runtime) = disk_runtime {
        let writer = runtime.writer;
        let shutdown = tokio::task::spawn_blocking(move || writer.shutdown());
        if tokio::time::timeout(Duration::from_secs(10), shutdown)
            .await
            .is_err()
        {
            warn!("disk cache: writer shutdown timed out; holes will be repaired at next start");
        }
        metrics::disk_cache_set_active(false);
    }

    Ok(())
}

#[cfg(feature = "disk-cache")]
const DISK_CACHE_MIN_HEAD_RETAIN_SLOTS: u64 = 64;

#[cfg(feature = "disk-cache")]
struct DiskCacheRuntime {
    cache: Arc<DiskCache>,
    writer: DiskWriterHandle,
    sink: Arc<DiskIngestSink>,
}

/// Open the disk cache and start its writer thread and backfill/repair filler.
/// Corruption and schema mismatches are handled inside `DiskCache::open`
/// (wipe-and-rebuild); any other open failure is an operator error that fails
/// startup loudly.
#[cfg(feature = "disk-cache")]
async fn start_disk_cache(
    args: &RpcConfig,
    clickhouse: &ClickHouseClient,
    head_cache: &Arc<HeadCache>,
    shutdown_tx: &tokio::sync::broadcast::Sender<()>,
) -> Result<DiskCacheRuntime, RpcError> {
    let path = args
        .disk_cache_path
        .as_deref()
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .ok_or_else(|| {
            RpcError::Config("DISK_CACHE_ENABLED=true requires DISK_CACHE_PATH".to_string())
        })?;

    let disk_cfg = DiskCacheConfig {
        path: path.into(),
        retain_slots: args.disk_cache_retain_slots.max(1),
        max_bytes: args.disk_cache_max_bytes,
        block_cache_bytes: args.disk_cache_block_cache_bytes,
        read_concurrency: args.disk_cache_read_concurrency.max(1),
    };
    info!(
        path,
        retain_slots = disk_cfg.retain_slots,
        max_bytes = disk_cfg.max_bytes,
        backfill_enabled = args.disk_cache_backfill_enabled,
        "disk cache: starting"
    );

    let cache = tokio::task::spawn_blocking(move || DiskCache::open(disk_cfg))
        .await
        .map_err(|err| RpcError::Config(format!("disk cache open task panicked: {err}")))?
        .map_err(|err| RpcError::Config(format!("disk cache open failed: {err}")))?;
    let cache = Arc::new(cache);

    let repair = Arc::new(RepairQueue::new(100_000));
    let writer = cache.spawn_writer(repair.clone(), args.disk_cache_write_queue_slots.max(1));

    if args.disk_cache_backfill_enabled {
        let filler_cfg = filler::FillerConfig {
            retain_slots: args.disk_cache_retain_slots.max(1),
            slots_per_query: args.disk_cache_backfill_slots_per_query,
            max_slots_per_sec: args.disk_cache_backfill_max_slots_per_sec,
            query_timeout: Duration::from_millis(args.disk_cache_backfill_query_timeout_ms),
            repair_interval: Duration::from_millis(args.disk_cache_repair_interval_ms),
            repair_min_lag_slots: args.disk_cache_repair_min_lag_slots,
            ..Default::default()
        };
        tokio::spawn(filler::run(
            cache.clone(),
            clickhouse.clone(),
            writer.bulk.clone(),
            repair.clone(),
            Some(head_cache.clone()),
            filler_cfg,
            shutdown_tx.subscribe(),
        ));
    }

    let sink = Arc::new(DiskIngestSink::new(
        cache.clone(),
        writer.sender.clone(),
        repair,
    ));
    metrics::disk_cache_set_active(true);

    Ok(DiskCacheRuntime {
        cache,
        writer,
        sink,
    })
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
    fn shard_routing_enabled_for_hot_routing_in_distributed_scope() {
        use clap::Parser;

        let _env_lock = crate::config::ENV_TEST_LOCK.lock().expect("env lock");
        let mut cfg = RpcConfig::parse_from(["superbank-rpc"]);
        cfg.clickhouse_scope = ClickHouseScope::Distributed;
        cfg.clickhouse_hot_addresses =
            vec!["EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".to_string()];

        let routing = build_shard_routing_config(&cfg).expect("routing config");
        assert_eq!(routing.cluster, "{cluster}");
    }

    #[test]
    fn shard_routing_enabled_for_topology_config_in_distributed_scope() {
        use clap::Parser;

        let _env_lock = crate::config::ENV_TEST_LOCK.lock().expect("env lock");
        let mut cfg = RpcConfig::parse_from(["superbank-rpc"]);
        cfg.clickhouse_scope = ClickHouseScope::Distributed;
        cfg.clickhouse_topology_config = Some(" /etc/superbank/topology.yaml ".to_string());

        let routing = build_shard_routing_config(&cfg).expect("routing config");
        assert_eq!(
            routing.topology_config_path.as_deref(),
            Some("/etc/superbank/topology.yaml")
        );
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
}
