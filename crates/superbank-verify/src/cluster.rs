// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Per-cluster PoH parameters: genesis hash, epoch warmup, hashes_per_tick
//! history and the Alpenglow genesis certificate block.

use anyhow::Result;
use serde::Deserialize;

use crate::cli::parse_hash32;
use crate::eras::HashesPerTickSchedule;
use crate::verify::poh::Hash32;

/// Solana cluster whose PoH parameters apply. All use 64 ticks per slot and
/// 432000-slot epochs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum SolanaCluster {
    #[default]
    #[serde(alias = "mainnet-beta")]
    #[value(alias = "mainnet-beta")]
    Mainnet,
    Devnet,
    Testnet,
}

impl SolanaCluster {
    pub(crate) fn genesis_hash(self) -> &'static str {
        match self {
            SolanaCluster::Mainnet => crate::cli::MAINNET_GENESIS_HASH,
            SolanaCluster::Devnet => "EtWTRABZaYq6iMfeYKouRu166VU2xqa1wcaWoxPkrZBG",
            SolanaCluster::Testnet => "4uhcVJyU9pJkvQyS88uRDiswHXSCkY3zQawwpjk2NsNY",
        }
    }

    pub(crate) fn hashes_per_tick_schedule(self) -> HashesPerTickSchedule {
        match self {
            SolanaCluster::Mainnet => HashesPerTickSchedule::mainnet(),
            SolanaCluster::Devnet => HashesPerTickSchedule::devnet(),
            SolanaCluster::Testnet => HashesPerTickSchedule::testnet(),
        }
    }

    /// From `getEpochSchedule`: only testnet has warmup epochs.
    pub(crate) fn epoch_warmup(self) -> bool {
        self == SolanaCluster::Testnet
    }

    /// `getAgGenesisCert` block from the public Solana Foundation RPCs, read on 2026-11-06.
    fn alpenglow_genesis_spec(self) -> (u64, &'static str) {
        match self {
            SolanaCluster::Mainnet => (459_652_999, "4q1ScaDS6x3diDLCSTrEt6GWVyJ5oHKVCPmMb1LBH9iL"),
            SolanaCluster::Devnet => (504_148_999, "2LqZ2TsCdiWAzT7oqk4GjvgQUTN6argondnE6HJpYgTp"),
            SolanaCluster::Testnet => (444_625_255, "734ibDTXnoVibyhc1D1Y5xo1ZCPwyh6K6vjMxDQiDHJ5"),
        }
    }

    pub fn alpenglow_genesis_slot(self) -> u64 {
        self.alpenglow_genesis_spec().0
    }

    pub(crate) fn alpenglow_genesis_block(self) -> Result<(u64, Hash32)> {
        let (slot, block_id) = self.alpenglow_genesis_spec();
        Ok((slot, parse_hash32(block_id)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_cluster_has_valid_built_in_parameters() {
        for cluster in [
            SolanaCluster::Mainnet,
            SolanaCluster::Devnet,
            SolanaCluster::Testnet,
        ] {
            parse_hash32(cluster.genesis_hash()).unwrap();
            let (slot, _) = cluster.alpenglow_genesis_block().unwrap();
            assert_eq!(slot, cluster.alpenglow_genesis_slot());
        }
        assert_eq!(SolanaCluster::Mainnet.alpenglow_genesis_slot(), 459_652_999);
        assert_eq!(SolanaCluster::Devnet.alpenglow_genesis_slot(), 504_148_999);
        assert_eq!(SolanaCluster::Testnet.alpenglow_genesis_slot(), 444_625_255);
    }

    #[test]
    fn mainnet_beta_is_an_alias() {
        let cluster: SolanaCluster = serde_yaml::from_str("mainnet-beta").unwrap();
        assert_eq!(cluster, SolanaCluster::Mainnet);
        assert_eq!(
            <SolanaCluster as clap::ValueEnum>::from_str("mainnet-beta", true).unwrap(),
            SolanaCluster::Mainnet
        );
    }
}
