// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use solana_commitment_config::CommitmentLevel;
use solana_sdk::{hash::Hash, pubkey::Pubkey};
use tokio::time::sleep;
use tracing::{info, warn};
use yellowstone_block_machine::dragonsmouth::{
    client_ext::{GeyserBlockStream, GeyserGrpcExt},
    stream::BlockMachineOutput,
};
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::prelude::{
    GetVersionRequest, SubscribeRequest, SubscribeRequestFilterTransactions, SubscribeUpdate,
};

use crate::clickhouse::BlockMetadataRecord;
#[cfg(feature = "disk-cache")]
use crate::disk_cache::ingest::DiskIngestSink;
use crate::head_cache::HeadCache;
use crate::metrics;

#[derive(Debug, Clone)]
pub(crate) struct DragonsmouthHeadCacheConfig {
    pub(crate) endpoint: String,
    pub(crate) x_token: Option<String>,
    pub(crate) max_decoding_bytes: usize,
    pub(crate) min_commitment: CommitmentLevel,
    /// Receives finalized slots for the disk cache; never blocks the stream.
    #[cfg(feature = "disk-cache")]
    pub(crate) disk_sink: Option<Arc<DiskIngestSink>>,
}

const TRANSACTIONS_FILTER_NAME: &str = "_superbank_rpc";
const BLOCK_META_FILTER_NAME: &str = "_superbank_rpc_block_meta";

pub(crate) async fn run(cache: Arc<HeadCache>, cfg: DragonsmouthHeadCacheConfig) {
    tokio::join!(
        run_block_machine_stream(cache.clone(), cfg.clone()),
        run_block_meta_stream(cache, cfg)
    );
}

async fn run_block_machine_stream(cache: Arc<HeadCache>, cfg: DragonsmouthHeadCacheConfig) {
    let mut backoff = Duration::from_millis(250);
    let max_backoff = Duration::from_secs(5);

    loop {
        match connect_and_subscribe(&cfg).await {
            Ok(mut stream) => {
                info!(
                    endpoint = cfg.endpoint.as_str(),
                    min_commitment = ?cfg.min_commitment,
                    "head cache: subscribed to DragonsMouth"
                );
                backoff = Duration::from_millis(250);

                while let Some(result) = stream.next().await {
                    match result {
                        Ok(output) => handle_output(&cache, output, &cfg),
                        Err(err) => {
                            warn!("head cache: block-machine error: {err:?}");
                            break;
                        }
                    }
                }

                warn!("head cache: stream ended; reconnecting");
            }
            Err(err) => {
                warn!(
                    endpoint = cfg.endpoint.as_str(),
                    "head cache: failed to subscribe: {err}"
                );
            }
        }

        metrics::head_cache_reconnect();
        sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}

async fn run_block_meta_stream(cache: Arc<HeadCache>, cfg: DragonsmouthHeadCacheConfig) {
    let mut backoff = Duration::from_millis(250);
    let max_backoff = Duration::from_secs(5);

    loop {
        let builder = match GeyserGrpcClient::build_from_shared(cfg.endpoint.clone().into_bytes()) {
            Ok(builder) => builder,
            Err(err) => {
                warn!(
                    endpoint = cfg.endpoint.as_str(),
                    "head cache: invalid block-meta endpoint: {err}"
                );
                sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
                continue;
            }
        };

        let builder = match builder.x_token(cfg.x_token.clone()) {
            Ok(builder) => builder,
            Err(err) => {
                warn!(
                    endpoint = cfg.endpoint.as_str(),
                    "head cache: invalid block-meta x-token: {err}"
                );
                sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
                continue;
            }
        };

        let builder = builder.max_decoding_message_size(cfg.max_decoding_bytes);
        let builder = match builder.tls_config(ClientTlsConfig::new().with_native_roots()) {
            Ok(builder) => builder,
            Err(err) => {
                warn!(
                    endpoint = cfg.endpoint.as_str(),
                    "head cache: block-meta tls config error: {err}"
                );
                sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
                continue;
            }
        };

        let mut client = match builder.connect().await {
            Ok(client) => client,
            Err(err) => {
                warn!(
                    endpoint = cfg.endpoint.as_str(),
                    "head cache: failed to connect block-meta stream: {err}"
                );
                sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
                continue;
            }
        };

        let mut blocks_meta = HashMap::new();
        blocks_meta.insert(BLOCK_META_FILTER_NAME.to_string(), Default::default());
        let request = SubscribeRequest {
            blocks_meta,
            commitment: Some(grpc_commitment(cfg.min_commitment) as i32),
            ..Default::default()
        };

        match client.subscribe_with_request(Some(request)).await {
            Ok((_sink, mut stream)) => {
                info!(
                    endpoint = cfg.endpoint.as_str(),
                    min_commitment = ?cfg.min_commitment,
                    "head cache: subscribed to DragonsMouth block-meta stream"
                );
                backoff = Duration::from_millis(250);

                while let Some(result) = stream.next().await {
                    match result {
                        Ok(update) => handle_block_meta_update(&cache, update),
                        Err(err) => {
                            warn!("head cache: block-meta stream error: {err:?}");
                            break;
                        }
                    }
                }

                warn!("head cache: block-meta stream ended; reconnecting");
            }
            Err(err) => {
                warn!(
                    endpoint = cfg.endpoint.as_str(),
                    "head cache: failed to subscribe block-meta stream: {err}"
                );
            }
        }

        sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}

#[cfg_attr(not(feature = "disk-cache"), allow(unused_variables))]
fn handle_output(cache: &HeadCache, output: BlockMachineOutput, cfg: &DragonsmouthHeadCacheConfig) {
    match output {
        BlockMachineOutput::FrozenBlock(block) => {
            let slot = block.slot;
            // Ensure we can serve immediately even if the commitment update races behind the block.
            cache.note_slot_commitment(slot, CommitmentLevel::Processed);

            let mut ingested_txs = 0u64;
            for idx in block.transaction_idx_map.iter().copied() {
                let Some(ev) = block.events.get(idx) else {
                    continue;
                };
                let Some(oneof) = ev.update_oneof.as_ref() else {
                    continue;
                };
                let yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof::Transaction(
                    update,
                ) = oneof
                else {
                    continue;
                };
                let Some(tx_info) = update.transaction.as_ref() else {
                    continue;
                };
                cache.ingest_transaction(update.slot, tx_info);
                ingested_txs = ingested_txs.saturating_add(1);
            }

            metrics::head_cache_observe_block(
                cache.latest_slot(),
                ingested_txs,
                cache.tx_entries(),
                cache.address_entries(),
                cache.slot_entries(),
            );
        }
        BlockMachineOutput::SlotCommitmentUpdate(update) => {
            cache.note_slot_commitment(update.slot, update.commitment);
            #[cfg(feature = "disk-cache")]
            if update.commitment == CommitmentLevel::Finalized
                && let Some(sink) = cfg.disk_sink.as_ref()
            {
                sink.on_slot_finalized(update.slot, cache);
            }
        }
        BlockMachineOutput::ForkDetected(fork) => {
            warn!(slot = fork.slot, "head cache: fork detected; dropping slot");
            cache.remove_slot(fork.slot);
            metrics::head_cache_drop_slot(
                cache.latest_slot(),
                cache.tx_entries(),
                cache.address_entries(),
                cache.slot_entries(),
            );
        }
        BlockMachineOutput::DeadBlockDetect(dead) => {
            warn!(
                slot = dead.slot,
                "head cache: dead block detected; dropping slot"
            );
            cache.remove_slot(dead.slot);
            metrics::head_cache_drop_slot(
                cache.latest_slot(),
                cache.tx_entries(),
                cache.address_entries(),
                cache.slot_entries(),
            );
        }
    }
}

fn handle_block_meta_update(cache: &HeadCache, update: SubscribeUpdate) {
    let Some(yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof::BlockMeta(meta)) =
        update.update_oneof
    else {
        return;
    };
    apply_block_meta(cache, &meta);
}

fn apply_block_meta(
    cache: &HeadCache,
    meta: &yellowstone_grpc_proto::prelude::SubscribeUpdateBlockMeta,
) {
    let slot = meta.slot;

    if !meta.blockhash.is_empty() {
        if let Ok(hash) = meta.blockhash.parse::<Hash>() {
            cache.note_blockhash(slot, hash.to_bytes());
        } else {
            warn!(slot, "head cache: failed to parse blockhash from BlockMeta");
        }
    }

    if let Some(height) = meta.block_height.as_ref().map(|bh| bh.block_height) {
        cache.note_block_height(slot, height);
    }
    if let Some(block_time) = meta.block_time.as_ref().map(|ts| ts.timestamp) {
        cache.note_block_time(slot, block_time);
    }

    let blockhash = match parse_hash(slot, "blockhash", meta.blockhash.as_str()) {
        Some(hash) => hash,
        None => return,
    };
    let parent_blockhash =
        match parse_hash(slot, "parent_blockhash", meta.parent_blockhash.as_str()) {
            Some(hash) => hash,
            None => return,
        };
    let (
        rewards_present,
        rewards_pubkey,
        rewards_lamports,
        rewards_post_balance,
        rewards_type,
        rewards_commission,
        rewards_num_partitions,
    ) = match parse_block_rewards(slot, meta.rewards.as_ref()) {
        Some(parts) => parts,
        None => return,
    };

    cache.note_block_metadata(BlockMetadataRecord {
        slot,
        parent_slot: meta.parent_slot,
        blockhash,
        parent_blockhash,
        block_time: meta.block_time.as_ref().map(|ts| ts.timestamp),
        block_height: meta.block_height.as_ref().map(|bh| bh.block_height),
        executed_transaction_count: meta.executed_transaction_count,
        entry_count: meta.entries_count,
        rewards_present,
        rewards_pubkey,
        rewards_lamports,
        rewards_post_balance,
        rewards_type,
        rewards_commission,
        rewards_num_partitions,
    });
}

fn parse_hash(slot: u64, field: &str, value: &str) -> Option<[u8; 32]> {
    if value.is_empty() {
        warn!(slot, field, "head cache: missing {field} in BlockMeta");
        return None;
    }

    match value.parse::<Hash>() {
        Ok(hash) => Some(hash.to_bytes()),
        Err(_) => {
            warn!(
                slot,
                field, "head cache: failed to parse {field} from BlockMeta"
            );
            None
        }
    }
}

type ParsedRewards = (
    bool,
    Vec<[u8; 32]>,
    Vec<i64>,
    Vec<u64>,
    Vec<Option<String>>,
    Vec<Option<u8>>,
    Option<u64>,
);

fn parse_block_rewards(
    slot: u64,
    rewards: Option<&yellowstone_grpc_proto::prelude::Rewards>,
) -> Option<ParsedRewards> {
    let Some(rewards) = rewards else {
        return Some((
            false,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            None,
        ));
    };

    let mut rewards_pubkey = Vec::with_capacity(rewards.rewards.len());
    let mut rewards_lamports = Vec::with_capacity(rewards.rewards.len());
    let mut rewards_post_balance = Vec::with_capacity(rewards.rewards.len());
    let mut rewards_type = Vec::with_capacity(rewards.rewards.len());
    let mut rewards_commission = Vec::with_capacity(rewards.rewards.len());

    for reward in &rewards.rewards {
        let pubkey = match reward.pubkey.parse::<Pubkey>() {
            Ok(pubkey) => pubkey,
            Err(err) => {
                warn!(
                    slot,
                    pubkey = reward.pubkey.as_str(),
                    "head cache: failed to parse reward pubkey from BlockMeta: {err}"
                );
                return None;
            }
        };

        rewards_pubkey.push(pubkey.to_bytes());
        rewards_lamports.push(reward.lamports);
        rewards_post_balance.push(reward.post_balance);
        rewards_type.push(
            match yellowstone_grpc_proto::prelude::RewardType::try_from(reward.reward_type) {
                Ok(yellowstone_grpc_proto::prelude::RewardType::Unspecified) => None,
                Ok(yellowstone_grpc_proto::prelude::RewardType::Fee) => Some("Fee".to_string()),
                Ok(yellowstone_grpc_proto::prelude::RewardType::Rent) => Some("Rent".to_string()),
                Ok(yellowstone_grpc_proto::prelude::RewardType::Staking) => {
                    Some("Staking".to_string())
                }
                Ok(yellowstone_grpc_proto::prelude::RewardType::Voting) => {
                    Some("Voting".to_string())
                }
                Err(_) => {
                    warn!(
                        slot,
                        reward_type = reward.reward_type,
                        "head cache: failed to parse reward type from BlockMeta"
                    );
                    return None;
                }
            },
        );
        rewards_commission.push(if reward.commission.is_empty() {
            None
        } else {
            match reward.commission.parse::<u8>() {
                Ok(value) => Some(value),
                Err(err) => {
                    warn!(
                        slot,
                        commission = reward.commission.as_str(),
                        "head cache: failed to parse reward commission from BlockMeta: {err}"
                    );
                    return None;
                }
            }
        });
    }

    Some((
        true,
        rewards_pubkey,
        rewards_lamports,
        rewards_post_balance,
        rewards_type,
        rewards_commission,
        rewards
            .num_partitions
            .as_ref()
            .map(|value| value.num_partitions),
    ))
}

async fn connect_and_subscribe(
    cfg: &DragonsmouthHeadCacheConfig,
) -> Result<GeyserBlockStream, String> {
    let mut client = GeyserGrpcClient::build_from_shared(cfg.endpoint.clone().into_bytes())
        .map_err(|e| format!("invalid endpoint: {e}"))?
        .x_token(cfg.x_token.clone())
        .map_err(|e| format!("invalid x-token: {e}"))?
        .max_decoding_message_size(cfg.max_decoding_bytes)
        .tls_config(ClientTlsConfig::new().with_native_roots())
        .map_err(|e| format!("tls config error: {e}"))?
        .connect()
        .await
        .map_err(|e| format!("connect error: {e}"))?;

    // Probe response metadata on the active gRPC channel to capture the upstream node label.
    match client.geyser.get_version(GetVersionRequest {}).await {
        Ok(response) => {
            let x_rpc_node = response
                .metadata()
                .get("x-rpc-node")
                .and_then(|value| value.to_str().ok())
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or("unknown");
            metrics::head_cache_set_active_node(x_rpc_node);
        }
        Err(status) => {
            if let Some(x_rpc_node) = status
                .metadata()
                .get("x-rpc-node")
                .and_then(|value| value.to_str().ok())
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                metrics::head_cache_set_active_node(x_rpc_node);
            } else {
                metrics::head_cache_set_active_node("unknown");
                warn!(
                    code = ?status.code(),
                    "head cache: get_version metadata did not include x-rpc-node"
                );
            }
        }
    }

    // Subscribe to all transaction updates; the block machine will add the reserved slot/meta/entry
    // filters needed to safely freeze blocks at the requested minimum commitment level.
    let mut transactions = HashMap::new();
    transactions.insert(
        TRANSACTIONS_FILTER_NAME.to_string(),
        SubscribeRequestFilterTransactions::default(),
    );

    let request = SubscribeRequest {
        transactions,
        commitment: Some(grpc_commitment(cfg.min_commitment) as i32),
        ..Default::default()
    };

    client
        .subscribe_block(request)
        .await
        .map_err(|e| format!("subscribe_block error: {e}"))
}

fn grpc_commitment(level: CommitmentLevel) -> yellowstone_grpc_proto::prelude::CommitmentLevel {
    match level {
        CommitmentLevel::Processed => yellowstone_grpc_proto::prelude::CommitmentLevel::Processed,
        CommitmentLevel::Confirmed => yellowstone_grpc_proto::prelude::CommitmentLevel::Confirmed,
        CommitmentLevel::Finalized => yellowstone_grpc_proto::prelude::CommitmentLevel::Finalized,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_block_meta_updates_slot_metadata() {
        let cache = HeadCache::new(32, 64);
        let slot = 42u64;
        let hash = Hash::new_unique();
        let parent_hash = Hash::new_unique();
        let height = 1_234_567u64;
        let block_time = 1_700_000_123i64;
        let reward_pubkey = Pubkey::new_unique();

        cache.note_slot_commitment(slot, CommitmentLevel::Processed);
        apply_block_meta(
            &cache,
            &yellowstone_grpc_proto::prelude::SubscribeUpdateBlockMeta {
                slot,
                blockhash: hash.to_string(),
                rewards: Some(yellowstone_grpc_proto::prelude::Rewards {
                    rewards: vec![yellowstone_grpc_proto::prelude::Reward {
                        pubkey: reward_pubkey.to_string(),
                        lamports: 55,
                        post_balance: 99,
                        reward_type: yellowstone_grpc_proto::prelude::RewardType::Fee as i32,
                        commission: "7".to_string(),
                        commission_bps: String::new(),
                    }],
                    num_partitions: Some(yellowstone_grpc_proto::prelude::NumPartitions {
                        num_partitions: 4,
                    }),
                }),
                block_time: Some(yellowstone_grpc_proto::prelude::UnixTimestamp {
                    timestamp: block_time,
                }),
                block_height: Some(yellowstone_grpc_proto::prelude::BlockHeight {
                    block_height: height,
                }),
                parent_slot: slot - 1,
                parent_blockhash: parent_hash.to_string(),
                executed_transaction_count: 0,
                entries_count: 3,
            },
        );

        assert_eq!(
            cache.latest_blockhash_info_at_least(CommitmentLevel::Processed),
            Some((slot, hash.to_bytes(), height))
        );

        // Verify that block_time was also stored for the slot.
        assert_eq!(cache.slot_block_time_for_tests(slot), Some(block_time));

        let block = cache
            .get_block(
                slot,
                CommitmentLevel::Processed,
                solana_transaction_status::TransactionDetails::None,
            )
            .expect("zero-tx block available from metadata");
        let metadata = block.metadata();
        assert_eq!(metadata.parent_slot, slot - 1);
        assert_eq!(metadata.parent_blockhash, parent_hash.to_bytes());
        assert_eq!(metadata.entry_count, 3);
        assert!(metadata.rewards_present);
        assert_eq!(metadata.rewards_pubkey, vec![reward_pubkey.to_bytes()]);
        assert_eq!(metadata.rewards_lamports, vec![55]);
        assert_eq!(metadata.rewards_post_balance, vec![99]);
        assert_eq!(metadata.rewards_type, vec![Some("Fee".to_string())]);
        assert_eq!(metadata.rewards_commission, vec![Some(7)]);
        assert_eq!(metadata.rewards_num_partitions, Some(4));
    }
}
