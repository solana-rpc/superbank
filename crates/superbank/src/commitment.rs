// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use anyhow::{Result, anyhow};
use solana_commitment_config::CommitmentConfig;
use yellowstone_grpc_proto::prelude::CommitmentLevel;

pub(crate) fn parse_commitment_level(value: &str) -> Result<CommitmentLevel> {
    let normalized = value.trim().to_lowercase();
    let level = match normalized.as_str() {
        "processed" => CommitmentLevel::Processed,
        "confirmed" => CommitmentLevel::Confirmed,
        "finalized" => CommitmentLevel::Finalized,
        _ => return Err(anyhow!("invalid commitment '{value}'")),
    };

    Ok(level)
}

/// Persistent slot-keyed tables cannot safely retain competing processed banks.
pub(crate) fn parse_durable_commitment(value: &str) -> Result<CommitmentLevel> {
    let level = parse_commitment_level(value)?;
    if level != CommitmentLevel::Finalized {
        return Err(anyhow!(
            "gRPC and Fumarole ClickHouse ingestion requires finalized commitment; serve processed data from the RPC head cache"
        ));
    }
    Ok(level)
}

pub(crate) fn parse_commitment_config(value: &str) -> Result<CommitmentConfig> {
    let normalized = value.trim().to_lowercase();
    let config = match normalized.as_str() {
        "processed" => CommitmentConfig::processed(),
        "confirmed" => CommitmentConfig::confirmed(),
        "finalized" => CommitmentConfig::finalized(),
        _ => return Err(anyhow!("invalid commitment '{value}'")),
    };

    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durable_ingestion_rejects_unfinalized_banks() {
        assert_eq!(
            parse_durable_commitment(" finalized ").unwrap(),
            CommitmentLevel::Finalized
        );
        assert!(parse_durable_commitment("processed").is_err());
        assert!(parse_durable_commitment("confirmed").is_err());
        assert!(parse_durable_commitment("unknown").is_err());
    }
}
