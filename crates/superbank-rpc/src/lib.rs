// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Superbank JSON-RPC server library.

mod clickhouse;
mod metrics;
mod processing;

mod config;
#[cfg(feature = "disk-cache")]
mod disk_cache;
#[cfg(feature = "grpc-streaming")]
mod grpc;
mod handlers;
#[cfg(feature = "grpc-head-cache")]
mod head_cache;
mod hydration;
#[cfg(feature = "pyroscope")]
mod profiling;
mod rpc;
mod server;
mod state;
mod util;

/// CLI/env configuration for the RPC server.
pub use config::RpcConfig;
/// Error type returned by [`run_server`].
pub use server::RpcError;
/// Result type returned by [`run_server`].
pub use server::RpcResult;
/// Run the RPC and metrics servers.
pub use server::run_server;

#[cfg(test)]
mod tests;
