// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::solana_sdk::{hash::Hash, pubkey::Pubkey};
use futures_util::StreamExt;
use solana_commitment_config::CommitmentLevel;
use tokio::time::sleep;
use tracing::{info, warn};
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient, GeyserStream};
use yellowstone_grpc_proto::prelude::{
    GetVersionRequest, SlotStatus, SubscribeRequest, SubscribeRequestFilterBlocks,
    SubscribeRequestFilterSlots, SubscribeUpdate, SubscribeUpdateBlock, SubscribeUpdateSlot,
    subscribe_update::UpdateOneof,
};

use crate::clickhouse::BlockMetadataRecord;
use crate::head_cache::HeadCache;
use crate::head_cache::coverage::Link;
use crate::metrics;

#[derive(Debug, Clone)]
pub(crate) struct DragonsmouthHeadCacheConfig {
    pub(crate) endpoint: String,
    pub(crate) x_token: Option<String>,
    pub(crate) max_decoding_bytes: usize,
    pub(crate) min_commitment: CommitmentLevel,
}

pub(crate) async fn run(cache: Arc<HeadCache>, cfg: DragonsmouthHeadCacheConfig) {
    run_loop(cache, cfg).await;
}

async fn run_loop(cache: Arc<HeadCache>, cfg: DragonsmouthHeadCacheConfig) {
    let mut backoff = Duration::from_millis(250);
    let max_backoff = Duration::from_secs(5);
    loop {
        match connect_and_subscribe(&cfg).await {
            Ok(stream) => {
                cache.clear_from(0);
                let _session = CoverageSession::new(cache.clone());
                info!(
                    endpoint = cfg.endpoint.as_str(),
                    min_commitment = ?cfg.min_commitment,
                    "head cache: subscribed to bank-tagged DragonsMouth blocks"
                );
                backoff = Duration::from_millis(250);
                consume_stream(&cache, stream, cfg.min_commitment).await;
                warn!("head cache: stream ended; reconnecting");
            }
            Err(err) => {
                warn!(
                    endpoint = cfg.endpoint.as_str(),
                    "head cache: subscribe failed: {err}"
                );
            }
        }
        metrics::head_cache_reconnect();
        sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}

async fn consume_stream(cache: &HeadCache, mut stream: GeyserStream, minimum: CommitmentLevel) {
    while let Some(result) = stream.next().await {
        match result {
            Ok(update) => handle_update(cache, update, minimum),
            Err(err) => {
                warn!("head cache: stream error: {err:?}");
                break;
            }
        }
    }
}

fn handle_update(cache: &HeadCache, update: SubscribeUpdate, minimum: CommitmentLevel) {
    match update.update_oneof {
        Some(UpdateOneof::Block(block)) => handle_block(cache, &block, minimum),
        Some(UpdateOneof::Slot(slot)) => handle_slot(cache, &slot, minimum),
        _ => {}
    }
}

fn handle_block(cache: &HeadCache, block: &SubscribeUpdateBlock, minimum: CommitmentLevel) {
    let slot = block.slot;
    if block.bank_id == 0 && slot > 0 {
        warn!(slot, "head cache: block has no bank ID; dropping branch");
        cache.remove_slot(slot);
        return;
    }
    if block.transactions.len() != block.executed_transaction_count as usize {
        warn!(
            slot,
            expected = block.executed_transaction_count,
            actual = block.transactions.len(),
            "head cache: incomplete block; dropping branch"
        );
        cache.remove_slot(slot);
        return;
    }
    let Some(metadata) = parse_block_metadata(block) else {
        cache.remove_slot(slot);
        return;
    };
    cache.select_bank(slot, block.bank_id);
    cache.note_slot_commitment(slot, CommitmentLevel::Processed);
    cache.note_block_metadata(metadata.clone());

    {
        let mut proof = cache.coverage.write().expect("head coverage lock");
        proof.metadata(Link {
            slot,
            hash: metadata.blockhash,
            parent: metadata.parent_slot,
            parent_hash: metadata.parent_blockhash,
        });
        proof.publish(slot, CommitmentLevel::Processed);
        if minimum == CommitmentLevel::Processed {
            proof.observe(slot, CommitmentLevel::Processed, Instant::now());
        }
        proof.retain(cache.latest_slot(), cache.retain_slots);
    }

    for tx in &block.transactions {
        cache.ingest_transaction(slot, block.bank_id, tx);
    }
    metrics::head_cache_observe_block(
        cache.latest_slot(),
        block.transactions.len() as u64,
        cache.tx_entries(),
        cache.address_entries(),
        cache.slot_entries(),
    );
}

fn handle_slot(cache: &HeadCache, slot: &SubscribeUpdateSlot, minimum: CommitmentLevel) {
    let status = match SlotStatus::try_from(slot.status) {
        Ok(status) => status,
        Err(_) => return,
    };
    match status {
        SlotStatus::SlotDead => drop_dead_slot(cache, slot),
        SlotStatus::SlotCreatedBank => maybe_replace_bank(cache, slot),
        SlotStatus::SlotProcessed | SlotStatus::SlotConfirmed | SlotStatus::SlotFinalized => {
            promote_slot(cache, slot, status, minimum);
        }
        _ => {}
    }
}

fn drop_dead_slot(cache: &HeadCache, slot: &SubscribeUpdateSlot) {
    if slot
        .bank_id
        .is_some_and(|bank_id| cache.current_bank(slot.slot) != Some(bank_id))
    {
        return;
    }
    cache.remove_slot(slot.slot);
    metrics::head_cache_drop_slot(
        cache.latest_slot(),
        cache.tx_entries(),
        cache.address_entries(),
        cache.slot_entries(),
    );
}

fn maybe_replace_bank(cache: &HeadCache, slot: &SubscribeUpdateSlot) {
    if let Some(bank_id) = slot.bank_id
        && cache
            .current_bank(slot.slot)
            .is_some_and(|current| current != bank_id)
    {
        cache.clear_from(slot.slot);
    }
}

fn promote_slot(
    cache: &HeadCache,
    slot: &SubscribeUpdateSlot,
    status: SlotStatus,
    minimum: CommitmentLevel,
) {
    let Some(bank_id) = slot.bank_id else {
        warn!(slot = slot.slot, "head cache: commitment lacks bank ID");
        return;
    };
    if cache.current_bank(slot.slot) != Some(bank_id) {
        // The matching complete block may still be in flight. Never promote
        // a different bank merely because the slot matches.
        if status == SlotStatus::SlotFinalized {
            cache.clear_from(slot.slot);
        }
        return;
    }
    let commitment = match status {
        SlotStatus::SlotProcessed => CommitmentLevel::Processed,
        SlotStatus::SlotConfirmed => CommitmentLevel::Confirmed,
        SlotStatus::SlotFinalized => CommitmentLevel::Finalized,
        _ => unreachable!(),
    };
    cache.note_slot_commitment(slot.slot, commitment);
    let mut proof = cache.coverage.write().expect("head coverage lock");
    proof.validate_parent(slot.slot, slot.parent);
    proof.publish(slot.slot, commitment);
    if super::commitment_meets(commitment, minimum) {
        proof.observe(slot.slot, commitment, Instant::now());
    }
    proof.retain(cache.latest_slot(), cache.retain_slots);
}

fn parse_block_metadata(meta: &SubscribeUpdateBlock) -> Option<BlockMetadataRecord> {
    let slot = meta.slot;

    let blockhash = parse_hash(slot, "blockhash", meta.blockhash.as_str())?;
    let parent_blockhash = parse_hash(slot, "parent_blockhash", meta.parent_blockhash.as_str())?;
    let (
        rewards_present,
        rewards_pubkey,
        rewards_lamports,
        rewards_post_balance,
        rewards_type,
        rewards_commission,
        rewards_commission_bps,
        rewards_num_partitions,
    ) = parse_block_rewards(slot, meta.rewards.as_ref())?;

    Some(BlockMetadataRecord {
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
        rewards_commission_bps,
        rewards_num_partitions,
    })
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
    Vec<Option<u16>>,
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
            Vec::new(),
            None,
        ));
    };

    let mut rewards_pubkey = Vec::with_capacity(rewards.rewards.len());
    let mut rewards_lamports = Vec::with_capacity(rewards.rewards.len());
    let mut rewards_post_balance = Vec::with_capacity(rewards.rewards.len());
    let mut rewards_type = Vec::with_capacity(rewards.rewards.len());
    let mut rewards_commission = Vec::with_capacity(rewards.rewards.len());
    let mut rewards_commission_bps = Vec::with_capacity(rewards.rewards.len());

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
        rewards_type.push(match reward.reward_type {
            0 => None,
            value => match super::convert::reward_type_to_string(value) {
                Some(name) => Some(name),
                None => {
                    warn!(
                        slot,
                        reward_type = value,
                        "head cache: failed to parse reward type from BlockMeta"
                    );
                    return None;
                }
            },
        });
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
        rewards_commission_bps.push(if reward.commission_bps.is_empty() {
            None
        } else {
            match reward.commission_bps.parse::<u16>() {
                Ok(value) => Some(value),
                Err(err) => {
                    warn!(
                        slot,
                        commission_bps = reward.commission_bps.as_str(),
                        "head cache: failed to parse reward commission bps from BlockMeta: {err}"
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
        rewards_commission_bps,
        rewards
            .num_partitions
            .as_ref()
            .map(|value| value.num_partitions),
    ))
}

async fn connect_and_subscribe(cfg: &DragonsmouthHeadCacheConfig) -> Result<GeyserStream, String> {
    let mut client = GeyserGrpcClient::build_from_shared(cfg.endpoint.clone())
        .map_err(|e| format!("invalid endpoint: {e}"))?
        .x_token(cfg.x_token.clone())
        .map_err(|e| format!("invalid x-token: {e}"))?
        .max_decoding_message_size(cfg.max_decoding_bytes)
        .tls_config(ClientTlsConfig::new().with_native_roots())
        .map_err(|e| format!("tls config error: {e}"))?
        .connect()
        .await
        .map_err(|e| format!("connect error: {e}"))?;
    record_upstream_node(&mut client).await;

    let mut blocks = HashMap::new();
    blocks.insert(
        "superbank_head".to_string(),
        SubscribeRequestFilterBlocks {
            include_transactions: Some(true),
            include_accounts: Some(false),
            include_entries: Some(false),
            ..Default::default()
        },
    );
    let mut slots = HashMap::new();
    slots.insert(
        "superbank_head".to_string(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: Some(false),
            interslot_updates: Some(true),
        },
    );
    let request = SubscribeRequest {
        blocks,
        slots,
        commitment: Some(0),
        ..Default::default()
    };
    let (_sink, stream) = client
        .subscribe_with_request(Some(request))
        .await
        .map_err(|e| format!("subscribe error: {e}"))?;
    Ok(stream)
}

async fn record_upstream_node(client: &mut GeyserGrpcClient) {
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
}

struct CoverageSession(Arc<HeadCache>);
impl CoverageSession {
    fn new(cache: Arc<HeadCache>) -> Self {
        cache
            .coverage
            .write()
            .expect("head coverage lock")
            .connect();
        Self(cache)
    }
}
impl Drop for CoverageSession {
    fn drop(&mut self) {
        self.0
            .coverage
            .write()
            .expect("head coverage lock")
            .disconnect();
        self.0.clear_from(0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(slot: u64, parent: u64, bank_id: u64) -> SubscribeUpdateBlock {
        SubscribeUpdateBlock {
            slot,
            bank_id,
            parent_slot: parent,
            blockhash: Hash::new_from_array([bank_id as u8; 32]).to_string(),
            parent_blockhash: Hash::new_from_array([parent as u8; 32]).to_string(),
            ..Default::default()
        }
    }

    fn status(slot: u64, parent: u64, bank_id: u64, state: SlotStatus) -> SubscribeUpdateSlot {
        SubscribeUpdateSlot {
            slot,
            parent: Some(parent),
            bank_id: Some(bank_id),
            status: state as i32,
            ..Default::default()
        }
    }

    #[test]
    fn same_slot_bank_replacement_evicts_descendants_and_new_bank_is_served() {
        let cache = HeadCache::new(32, 64);
        cache.coverage.write().unwrap().connect();
        handle_block(&cache, &block(10, 9, 1), CommitmentLevel::Processed);
        handle_block(&cache, &block(11, 10, 2), CommitmentLevel::Processed);
        assert_eq!(cache.current_bank(10), Some(1));
        assert_eq!(cache.current_bank(11), Some(2));

        handle_block(&cache, &block(10, 9, 3), CommitmentLevel::Processed);
        assert_eq!(cache.current_bank(10), Some(3));
        assert_eq!(cache.current_bank(11), None);
        let payload = cache
            .get_block(
                10,
                CommitmentLevel::Processed,
                solana_transaction_status::TransactionDetails::None,
            )
            .unwrap();
        assert_eq!(payload.metadata().blockhash, [3; 32]);
    }

    #[test]
    fn commitment_for_other_bank_never_promotes_selected_bank() {
        let cache = HeadCache::new(32, 64);
        cache.coverage.write().unwrap().connect();
        handle_block(&cache, &block(10, 9, 1), CommitmentLevel::Processed);
        handle_slot(
            &cache,
            &status(10, 9, 2, SlotStatus::SlotConfirmed),
            CommitmentLevel::Processed,
        );
        assert_eq!(cache.slot_commitment(10), CommitmentLevel::Processed);
        handle_slot(
            &cache,
            &status(10, 9, 1, SlotStatus::SlotConfirmed),
            CommitmentLevel::Processed,
        );
        assert_eq!(cache.slot_commitment(10), CommitmentLevel::Confirmed);
    }

    #[test]
    fn dead_losing_bank_does_not_evict_selected_bank() {
        let cache = HeadCache::new(32, 64);
        cache.coverage.write().unwrap().connect();
        handle_block(&cache, &block(10, 9, 1), CommitmentLevel::Processed);
        handle_slot(
            &cache,
            &status(10, 9, 2, SlotStatus::SlotDead),
            CommitmentLevel::Processed,
        );
        assert_eq!(cache.current_bank(10), Some(1));
        handle_slot(
            &cache,
            &status(10, 9, 1, SlotStatus::SlotDead),
            CommitmentLevel::Processed,
        );
        assert_eq!(cache.current_bank(10), None);
    }

    #[test]
    fn block_rewards_preserve_vat_debit() {
        let cache = HeadCache::new(32, 64);
        cache.coverage.write().unwrap().connect();
        let mut block = block(42, 41, 7);
        block.rewards = Some(yellowstone_grpc_proto::prelude::Rewards {
            rewards: vec![yellowstone_grpc_proto::prelude::Reward {
                pubkey: Pubkey::new_unique().to_string(),
                lamports: -10,
                post_balance: 90,
                reward_type: 6,
                ..Default::default()
            }],
            ..Default::default()
        });
        handle_block(&cache, &block, CommitmentLevel::Processed);
        let payload = cache
            .get_block(
                42,
                CommitmentLevel::Processed,
                solana_transaction_status::TransactionDetails::None,
            )
            .unwrap();
        assert_eq!(
            payload.metadata().rewards_type,
            vec![Some("VATDebit".to_owned())]
        );
    }
}
