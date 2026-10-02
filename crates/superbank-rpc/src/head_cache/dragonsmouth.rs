// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::solana_sdk::{hash::Hash, pubkey::Pubkey};
use futures_util::StreamExt;
use solana_commitment_config::CommitmentLevel;
use tokio::time::sleep;
use tracing::{info, warn};
use yellowstone_block_machine::{
    dragonsmouth::{
        RESERVED_FILTER_NAME,
        block_accumulator::{BankBuffer, DragonsmouthBlockCumulator},
    },
    stream::{BlockEventStore, BlockMachineOutput, BlockStream},
};
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::prelude::{
    GetVersionRequest, SubscribeRequest, SubscribeRequestFilterTransactions, SubscribeUpdate,
};

use crate::clickhouse::BlockMetadataRecord;
use crate::head_cache::HeadCache;
use crate::metrics;

#[derive(Debug, Clone)]
pub(crate) struct DragonsmouthHeadCacheConfig {
    pub(crate) endpoint: String,
    pub(crate) x_token: Option<String>,
    pub(crate) max_decoding_bytes: usize,
    pub(crate) min_commitment: CommitmentLevel,
}

const TRANSACTIONS_FILTER_NAME: &str = "_superbank_rpc";

pub(crate) async fn run(cache: Arc<HeadCache>, cfg: DragonsmouthHeadCacheConfig) {
    run_block_machine_stream(cache, cfg).await;
}

async fn run_block_machine_stream(cache: Arc<HeadCache>, cfg: DragonsmouthHeadCacheConfig) {
    let mut backoff = Duration::from_millis(250);
    let max_backoff = Duration::from_secs(5);

    loop {
        let session = CoverageSession::new(cache.clone());
        match connect_and_subscribe(&cfg, cache.clone(), session.id).await {
            Ok(mut stream) => {
                info!(
                    endpoint = cfg.endpoint.as_str(),
                    min_commitment = ?cfg.min_commitment,
                    "head cache: subscribed to DragonsMouth"
                );
                backoff = Duration::from_millis(250);

                while let Some(result) = stream.next().await {
                    match result {
                        Ok(output) => handle_output(&cache, session.id, output),
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

        drop(session);
        metrics::head_cache_reconnect();
        sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}

fn handle_output<S: BlockEventStore<EventT = SubscribeUpdate>>(
    cache: &HeadCache,
    session: u64,
    output: BlockMachineOutput<S>,
) {
    match output {
        BlockMachineOutput::FrozenBlock(block) => {
            let transactions = block
                .events
                .iter()
                .filter_map(|event| match event.update_oneof.as_ref() {
                    Some(
                        yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof::Transaction(
                            update,
                        ),
                    ) if update.slot == block.slot && update.bank_id == block.bank_id => {
                        update.transaction.clone()
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            let count = transactions.len() as u64;
            cache.freeze_bank(
                session,
                block.slot,
                block.bank_id,
                block.blockhash,
                transactions,
            );
            metrics::head_cache_observe_block(
                cache.latest_slot(),
                count,
                cache.tx_entries(),
                cache.address_entries(),
                cache.slot_entries(),
            );
        }
        BlockMachineOutput::SlotCommitmentUpdate(update) => {
            cache.commit_bank(
                session,
                update.slot,
                update.bank_id,
                update.commitment,
                update.parent_slot,
            );
        }
        BlockMachineOutput::ForkDetected(fork) => {
            for bank_id in fork.bank_ids {
                cache.discard_bank(session, fork.slot, bank_id);
            }
        }
        BlockMachineOutput::DeadBlockDetected(dead) => {
            for bank_id in dead.bank_ids {
                cache.discard_bank(session, dead.slot, bank_id);
            }
        }
        BlockMachineOutput::BankDiscarded(discarded) => {
            cache.discard_bank(session, discarded.slot, discarded.bank_id);
        }
    }
}

pub(super) fn parse_block_meta(
    meta: &yellowstone_grpc_proto::prelude::SubscribeUpdateBlockMeta,
) -> Option<BlockMetadataRecord> {
    let slot = meta.slot;
    let blockhash = parse_hash(slot, "blockhash", &meta.blockhash)?;
    let parent_blockhash = if slot == 0 && meta.parent_blockhash.is_empty() {
        [0; 32]
    } else {
        parse_hash(slot, "parent_blockhash", &meta.parent_blockhash)?
    };
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
                Ok(yellowstone_grpc_proto::prelude::RewardType::DeactivatedStake) => {
                    Some("DeactivatedStake".to_string())
                }
                Ok(yellowstone_grpc_proto::prelude::RewardType::VatDebit) => {
                    Some("VATDebit".to_string())
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

async fn connect_and_subscribe(
    cfg: &DragonsmouthHeadCacheConfig,
    cache: Arc<HeadCache>,
    session: u64,
) -> Result<
    impl futures_util::Stream<Item = Result<BlockMachineOutput<BankBuffer>, String>> + Unpin,
    String,
> {
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

    record_upstream_node(&mut client).await;

    // Subscribe to all transaction updates and the reserved reconstruction filters below.
    let mut transactions = HashMap::new();
    transactions.insert(
        TRANSACTIONS_FILTER_NAME.to_string(),
        SubscribeRequestFilterTransactions::default(),
    );

    // Equivalent to subscribe_block's filters, with a tap before the block machine
    // discards metadata. Proof evidence must come from this same subscription.
    let mut request = SubscribeRequest {
        transactions,
        commitment: Some(0),
        ..Default::default()
    };
    request.slots.insert(
        RESERVED_FILTER_NAME.to_owned(),
        yellowstone_grpc_proto::prelude::SubscribeRequestFilterSlots {
            interslot_updates: Some(true),
            ..Default::default()
        },
    );
    request
        .blocks_meta
        .insert(RESERVED_FILTER_NAME.to_owned(), Default::default());
    request
        .entry
        .insert(RESERVED_FILTER_NAME.to_owned(), Default::default());
    request.accounts.insert(
        RESERVED_FILTER_NAME.to_owned(),
        yellowstone_grpc_proto::prelude::SubscribeRequestFilterAccounts {
            owner: vec!["Sysvar1111111111111111111111111111111111111".to_string()],
            ..Default::default()
        },
    );
    let (_sink, source) = client
        .subscribe_with_request(Some(request))
        .await
        .map_err(|e| format!("subscribe_block error: {e}"))?;
    let source = source
        .scan(super::protocol::Protocol::default(), |protocol, event| {
            futures_util::future::ready(Some(protocol.adapt(event)))
        })
        .flat_map(futures_util::stream::iter)
        .inspect(move |event| {
            let _ = event
                .as_ref()
                .map(|update| observe_bank_metadata(&cache, session, update));
        });
    Ok(BlockStream::<_, SubscribeUpdate, _>::new(
        source,
        DragonsmouthBlockCumulator::default(),
        cfg.min_commitment,
    )
    .map(|result| result.map_err(|e| e.to_string())))
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

struct CoverageSession {
    cache: Arc<HeadCache>,
    id: u64,
}
impl CoverageSession {
    fn new(cache: Arc<HeadCache>) -> Self {
        let id = cache.start_bank_session();
        Self { cache, id }
    }
}
impl Drop for CoverageSession {
    fn drop(&mut self) {
        self.cache.end_bank_session(self.id);
    }
}

fn observe_bank_metadata(cache: &HeadCache, session: u64, update: &SubscribeUpdate) {
    use yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof;
    match update.update_oneof.as_ref() {
        Some(UpdateOneof::BlockMeta(meta)) => {
            if let Some(metadata) = parse_block_meta(meta) {
                cache.stage_bank_metadata(session, meta.bank_id, metadata);
            }
        }
        Some(UpdateOneof::Entry(entry)) => cache.stage_bank_entry(session, entry),
        Some(UpdateOneof::EntryUpdateParent(marker)) => {
            cache.discard_bank(session, marker.slot, marker.cleared_bank_id);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn coverage_events(slot: u64, parent: u64) -> Vec<SubscribeUpdate> {
        use yellowstone_grpc_proto::prelude::{
            SlotStatus, SubscribeUpdateBlockMeta, SubscribeUpdateSlot,
            subscribe_update::UpdateOneof,
        };
        let status = |status: SlotStatus| SubscribeUpdate {
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot,
                parent: Some(parent),
                status: status as i32,
                bank_id: Some(slot),
                ..Default::default()
            })),
            ..Default::default()
        };
        let mut events = vec![
            status(SlotStatus::SlotCreatedBank),
            status(SlotStatus::SlotFirstShredReceived),
            status(SlotStatus::SlotCompleted),
            SubscribeUpdate {
                update_oneof: Some(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                    slot,
                    parent_slot: parent,
                    bank_id: slot,
                    entries_count: 1,
                    blockhash: Hash::new_from_array([slot as u8; 32]).to_string(),
                    parent_blockhash: Hash::new_from_array([parent as u8; 32]).to_string(),
                    ..Default::default()
                })),
                ..Default::default()
            },
            status(SlotStatus::SlotProcessed),
            status(SlotStatus::SlotConfirmed),
            status(SlotStatus::SlotFinalized),
        ];
        for pubkey in [
            "SysvarC1ock11111111111111111111111111111111",
            "SysvarS1otHashes111111111111111111111111111",
            "SysvarS1otHistory11111111111111111111111111",
            "SysvarRecentB1ockHashes11111111111111111111",
        ] {
            events.insert(
                1,
                SubscribeUpdate {
                    filters: vec![RESERVED_FILTER_NAME.to_string()],
                    update_oneof: Some(UpdateOneof::Account(
                        yellowstone_grpc_proto::prelude::SubscribeUpdateAccount {
                            slot,
                            bank_id: Some(slot),
                            account: Some(
                                yellowstone_grpc_proto::prelude::SubscribeUpdateAccountInfo {
                                    pubkey: pubkey.parse::<Pubkey>().unwrap().to_bytes().to_vec(),
                                    owner: "Sysvar1111111111111111111111111111111111111"
                                        .parse::<Pubkey>()
                                        .unwrap()
                                        .to_bytes()
                                        .to_vec(),
                                    ..Default::default()
                                },
                            ),
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                },
            );
        }
        events.insert(
            5,
            SubscribeUpdate {
                filters: vec![RESERVED_FILTER_NAME.to_string()],
                update_oneof: Some(UpdateOneof::Entry(
                    yellowstone_grpc_proto::prelude::SubscribeUpdateEntry {
                        slot,
                        bank_id: slot,
                        hash: vec![slot as u8; 32],
                        ..Default::default()
                    },
                )),
                ..Default::default()
            },
        );
        events
    }

    #[tokio::test]
    async fn block_machine_subscription_publishes_matching_range_proofs() {
        let cache = Arc::new(HeadCache::new(32, 64));
        let session = CoverageSession::new(cache.clone());
        let events = coverage_events(10, 9)
            .into_iter()
            .chain(coverage_events(12, 10));
        let source = futures_util::stream::iter(events.map(Ok::<_, std::io::Error>))
            .inspect(|event| observe_bank_metadata(&cache, session.id, event.as_ref().unwrap()));
        let mut stream = BlockStream::<_, SubscribeUpdate, _>::new(
            source,
            DragonsmouthBlockCumulator::default(),
            CommitmentLevel::Processed,
        );
        while let Some(output) = stream.next().await {
            handle_output(&cache, session.id, output.unwrap());
        }
        let (tip, proof) = cache
            .coverage
            .read()
            .unwrap()
            .snapshot(
                10,
                None,
                CommitmentLevel::Finalized,
                std::time::Instant::now(),
            )
            .unwrap();
        assert_eq!(tip, 12);
        assert_eq!(proof.slots, vec![10, 12]);
        assert!(proof.gaps(10, 12).is_empty());
        drop(session);
        assert!(
            cache
                .coverage
                .read()
                .unwrap()
                .snapshot(
                    10,
                    None,
                    CommitmentLevel::Finalized,
                    std::time::Instant::now()
                )
                .is_err()
        );
    }

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
        let metadata =
            parse_block_meta(&yellowstone_grpc_proto::prelude::SubscribeUpdateBlockMeta {
                slot,
                blockhash: hash.to_string(),
                rewards: Some(yellowstone_grpc_proto::prelude::Rewards {
                    rewards: vec![yellowstone_grpc_proto::prelude::Reward {
                        pubkey: reward_pubkey.to_string(),
                        lamports: 55,
                        post_balance: 99,
                        reward_type: yellowstone_grpc_proto::prelude::RewardType::Fee as i32,
                        commission: "7".to_string(),
                        commission_bps: "725".to_string(),
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
                ..Default::default()
            })
            .expect("metadata");
        cache.note_block_metadata(metadata);

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
        assert_eq!(metadata.rewards_commission_bps, vec![Some(725)]);
        assert_eq!(metadata.rewards_num_partitions, Some(4));
    }
    #[tokio::test]
    async fn interleaved_wire_banks_publish_only_the_confirmed_winner() {
        use crate::solana_sdk::signature::Signature;
        use yellowstone_grpc_proto::prelude::{
            SlotStatus, SubscribeUpdateTransaction, subscribe_update::UpdateOneof,
        };
        let cache = Arc::new(HeadCache::new(32, 64));
        let session = CoverageSession::new(cache.clone());
        let make_bank = |bank_id: u64, marker: u8| {
            let mut events = coverage_events(42, 41);
            events.retain(|event| !matches!(event.update_oneof.as_ref(), Some(UpdateOneof::Slot(slot)) if slot.status == SlotStatus::SlotConfirmed as i32 || slot.status == SlotStatus::SlotFinalized as i32));
            for event in &mut events {
                match event.update_oneof.as_mut().unwrap() {
                    UpdateOneof::Slot(slot) => slot.bank_id = Some(bank_id),
                    UpdateOneof::Account(account) => account.bank_id = Some(bank_id),
                    UpdateOneof::Entry(entry) => {
                        entry.bank_id = bank_id;
                        entry.executed_transaction_count = 1;
                    }
                    UpdateOneof::BlockMeta(meta) => {
                        meta.bank_id = bank_id;
                        meta.blockhash = bs58::encode([marker; 32]).into_string();
                        meta.executed_transaction_count = 1;
                    }
                    _ => {}
                }
            }
            events.insert(
                6,
                SubscribeUpdate {
                    filters: vec![TRANSACTIONS_FILTER_NAME.to_string()],
                    update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
                        slot: 42,
                        bank_id,
                        transaction: Some(super::super::banks::tests::transaction(marker)),
                    })),
                    ..Default::default()
                },
            );
            events
        };
        let mut a = make_bank(1, 1).into_iter();
        let mut b = make_bank(2, 2).into_iter();
        let mut events = Vec::new();
        while let (Some(left), Some(right)) = (a.next(), b.next()) {
            events.extend([left, right]);
        }
        for status in [SlotStatus::SlotConfirmed, SlotStatus::SlotFinalized] {
            events.push(SubscribeUpdate {
                update_oneof: Some(UpdateOneof::Slot(
                    yellowstone_grpc_proto::prelude::SubscribeUpdateSlot {
                        slot: 42,
                        parent: Some(41),
                        bank_id: Some(2),
                        status: status as i32,
                        ..Default::default()
                    },
                )),
                ..Default::default()
            });
        }
        // Metadata/status arriving after selection for the loser must not overwrite the winner.
        events.extend(make_bank(1, 1));
        let source = futures_util::stream::iter(events.into_iter().map(Ok::<_, std::io::Error>))
            .inspect(|event| {
                observe_bank_metadata(&cache, session.id, event.as_ref().unwrap());
            });
        let mut stream = BlockStream::<_, SubscribeUpdate, _>::new(
            source,
            DragonsmouthBlockCumulator::default(),
            CommitmentLevel::Processed,
        );
        while let Some(output) = stream.next().await {
            handle_output(&cache, session.id, output.unwrap());
        }
        assert!(
            cache
                .get_tx(&Signature::from([1; 64]), CommitmentLevel::Processed)
                .is_none()
        );
        assert!(
            cache
                .get_tx(&Signature::from([2; 64]), CommitmentLevel::Finalized)
                .is_some()
        );
        assert_eq!(
            cache
                .get_block(
                    42,
                    CommitmentLevel::Finalized,
                    solana_transaction_status::TransactionDetails::Full
                )
                .unwrap()
                .metadata()
                .blockhash,
            [2; 32]
        );
    }
    #[tokio::test]
    async fn legacy_pre_alpenglow_bank_blind_stream_keeps_finalized_blocks() {
        use yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof;
        let cache = Arc::new(HeadCache::new(32, 64));
        let session = CoverageSession::new(cache.clone());
        let events = coverage_events(42, 41).into_iter().map(|mut event| {
            match event.update_oneof.as_mut().unwrap() {
                UpdateOneof::Slot(slot) => slot.bank_id = None,
                UpdateOneof::Account(account) => account.bank_id = None,
                UpdateOneof::BlockMeta(meta) => meta.bank_id = 0,
                UpdateOneof::Entry(entry) => entry.bank_id = 0,
                _ => {}
            }
            Ok::<_, yellowstone_grpc_proto::tonic::Status>(event)
        });
        let source = futures_util::stream::iter(events)
            .scan(
                super::super::protocol::Protocol::default(),
                |protocol, event| futures_util::future::ready(Some(protocol.adapt(event))),
            )
            .flat_map(futures_util::stream::iter)
            .inspect(|event| {
                observe_bank_metadata(&cache, session.id, event.as_ref().unwrap());
            });
        let mut stream = BlockStream::<_, SubscribeUpdate, _>::new(
            source,
            DragonsmouthBlockCumulator::default(),
            CommitmentLevel::Processed,
        );
        while let Some(output) = stream.next().await {
            handle_output(&cache, session.id, output.unwrap());
        }
        assert!(
            cache
                .get_block(
                    42,
                    CommitmentLevel::Finalized,
                    solana_transaction_status::TransactionDetails::None
                )
                .is_some()
        );
    }
    #[test]
    fn same_subscription_metadata_preserves_vat_debit_and_basis_points() {
        let meta = yellowstone_grpc_proto::prelude::SubscribeUpdateBlockMeta {
            slot: 42,
            bank_id: 0,
            blockhash: bs58::encode([1; 32]).into_string(),
            parent_blockhash: bs58::encode([2; 32]).into_string(),
            rewards: Some(yellowstone_grpc_proto::prelude::Rewards {
                rewards: vec![yellowstone_grpc_proto::prelude::Reward {
                    pubkey: bs58::encode([3; 32]).into_string(),
                    lamports: -10,
                    post_balance: 90,
                    reward_type: 6,
                    commission_bps: "725".into(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        };
        let row = parse_block_meta(&meta).unwrap();
        assert_eq!(row.rewards_type, vec![Some("VATDebit".into())]);
        assert_eq!(row.rewards_lamports, vec![-10]);
        assert_eq!(row.rewards_post_balance, vec![90]);
        assert_eq!(row.rewards_commission_bps, vec![Some(725)]);
    }
}
