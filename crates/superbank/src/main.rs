// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

mod cli;
mod clickhouse;
mod commitment;
mod ingest;
mod message_wire;
mod metrics;
mod range;
mod rpc_client;
mod shutdown;
mod utils;

use anyhow::Result;
use axum::{Router, routing::get};
use tokio::net::TcpListener;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = cli::resolve_args()?;
    metrics::force_init(args.source.as_str(), args.metrics_cluster_label.as_deref());
    let git_sha = option_env!("SUPERBANK_GIT_SHA")
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .unwrap_or("unknown");
    tracing::info!(
        name = env!("CARGO_PKG_NAME"),
        version = env!("CARGO_PKG_VERSION"),
        git_sha = git_sha,
        "starting"
    );

    let metrics_addr = format!("{}:{}", args.metrics_host, args.metrics_port);
    let metrics_listener = TcpListener::bind(&metrics_addr).await?;
    info!(
        "Ingest metrics server listening on http://{}/metrics",
        metrics_addr
    );
    let health_stale_secs = args.health_stale_secs;
    let metrics_server = tokio::spawn(async move {
        let app = Router::new()
            .route("/metrics", get(metrics::metrics_handler))
            .route(
                "/health",
                get(move || metrics::health_handler(health_stale_secs)),
            );
        axum::serve(metrics_listener, app).await
    });

    let ingest_result = match args.source {
        cli::IngestSource::Fumarole => ingest::fumarole::run_fumarole_ingest(&args).await,
        cli::IngestSource::Grpc => ingest::grpc::run_grpc_ingest(&args).await,
        cli::IngestSource::Rpc => ingest::rpc::run_rpc_ingest(&args).await,
        cli::IngestSource::Bigtable => ingest::bigtable::run_bigtable_ingest(&args).await,
    };

    metrics_server.abort();
    if let Err(err) = metrics_server.await
        && !err.is_cancelled()
    {
        warn!("metrics server task failed: {err}");
    }

    ingest_result
}
