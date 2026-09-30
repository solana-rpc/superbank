// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use clap::Parser;
use superbank_rpc::{RpcConfig, run_server};
use tracing_subscriber::EnvFilter;

/// Worker and blocking-thread stack for unoptimized builds.
///
/// Debug builds keep large async state machines on the stack while polling the
/// getSignaturesForAddress handler, and overflow tokio's default 2 MiB worker stack there.
/// Release builds need far less, so they keep tokio's default.
#[cfg(debug_assertions)]
const DEBUG_THREAD_STACK_BYTES: usize = 16 * 1024 * 1024;

fn main() {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    #[cfg(debug_assertions)]
    builder.thread_stack_size(DEBUG_THREAD_STACK_BYTES);
    let runtime = match builder.build() {
        Ok(runtime) => runtime,
        Err(err) => {
            eprintln!("superbank-rpc failed to start the tokio runtime: {err}");
            std::process::exit(1);
        }
    };
    runtime.block_on(run());
}

async fn run() {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,clickhouse_rs=warn"));
    let log_format = std::env::var("LOG_FORMAT")
        .unwrap_or_else(|_| "plain".to_string())
        .to_lowercase();

    // Install a global tracing subscriber.
    //
    // Avoid `SubscriberInitExt::init()` here because it also installs a `LogTracer` and will panic
    // if the `log` global logger is already set. We install `LogTracer` below on a best-effort
    // basis instead.
    let subscriber_result = if log_format == "json" {
        tracing::subscriber::set_global_default(
            tracing_subscriber::fmt()
                .with_env_filter(env_filter)
                .json()
                .finish(),
        )
    } else {
        tracing::subscriber::set_global_default(
            tracing_subscriber::fmt()
                .with_env_filter(env_filter)
                .finish(),
        )
    };

    if let Err(err) = subscriber_result {
        eprintln!("Failed to install tracing subscriber: {err}");
    }

    // Bridge `log` records (from deps) into the `tracing` subscriber (best-effort).
    //
    // This may fail if something else installed a global logger first; treat that as non-fatal.
    let _ = tracing_log::LogTracer::init();

    let config = RpcConfig::parse();
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
    if let Err(err) = run_server(config).await {
        eprintln!("superbank-rpc exited with error: {err}");
        std::process::exit(1);
    }
}
