// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

mod chain;
mod chdb;
mod checkpoint;
mod cli;
mod epoch;
mod eras;
mod fixture;
mod metrics;
mod range;
mod report;
mod runner;
mod verify;

use axum::{Router, routing::get};
use tokio::net::TcpListener;
use tracing::{error, info, warn};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = match cli::resolve_args() {
        Ok(args) => args,
        Err(err) => {
            eprintln!("superbank-verify: {err:#}");
            std::process::exit(report::EXIT_OPERATIONAL_ERROR);
        }
    };
    metrics::force_init(args.mode.as_str(), args.metrics_cluster_label.as_deref());
    let git_sha = option_env!("SUPERBANK_GIT_SHA")
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .unwrap_or("unknown");
    info!(
        name = env!("CARGO_PKG_NAME"),
        version = env!("CARGO_PKG_VERSION"),
        git_sha = git_sha,
        "starting"
    );

    let exit_code = match run(&args).await {
        Ok(code) => code,
        Err(err) => {
            error!(error = format!("{err:#}"), "superbank-verify aborted");
            report::EXIT_OPERATIONAL_ERROR
        }
    };
    std::process::exit(exit_code);
}

async fn run(args: &cli::Args) -> anyhow::Result<i32> {
    let metrics_addr = format!("{}:{}", args.metrics_host, args.metrics_port);
    let metrics_listener = TcpListener::bind(&metrics_addr).await?;
    info!(
        "Verify metrics server listening on http://{}/metrics",
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

    let result = match &args.export_fixture {
        Some((slot, out)) => fixture::export_fixture(args, *slot, out)
            .await
            .map(|()| report::EXIT_OK),
        None => runner::run(args)
            .await
            .map(|counters| report::exit_code(&counters, args.allow_unverifiable)),
    };

    metrics_server.abort();
    if let Err(err) = metrics_server.await
        && !err.is_cancelled()
    {
        warn!("metrics server task failed: {err}");
    }

    result
}
