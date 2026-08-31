// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Superbank JSON-RPC server library.

// Compatibility namespace for the SDK modules used by the RPC implementation. The
// concrete types come from Agave's split crates, which keeps message and transaction
// types aligned with transaction-status instead of pulling in the monolithic SDK.
#[allow(unused_imports)]
mod solana_sdk {
    pub mod hash {
        pub use solana_hash::*;
    }
    pub mod instruction {
        pub use solana_instruction::error::InstructionError;
        pub use solana_instruction::*;
    }
    pub mod message {
        pub use solana_message::*;
    }
    pub mod pubkey {
        pub use solana_address::{Address as Pubkey, *};
    }
    pub mod signature {
        pub use solana_keypair::Keypair;
        pub use solana_signature::Signature;
        pub use solana_signer::Signer;
    }
    pub mod transaction {
        pub use solana_transaction::versioned::{TransactionVersion, VersionedTransaction};
        pub use solana_transaction::*;
        pub use solana_transaction_error::TransactionError;
    }
}

mod clickhouse;
mod metrics;
mod processing;
mod request_filter;

mod config;
#[cfg(feature = "disk-cache")]
mod disk_cache;
mod genesis;
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
