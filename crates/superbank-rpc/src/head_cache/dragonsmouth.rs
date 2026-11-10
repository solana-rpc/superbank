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
use tracing::{error, info, warn};
use yellowstone_block_machine::{
    dragonsmouth::{
        RESERVED_FILTER_NAME,
        block_accumulator::{BankBuffer, DragonsmouthBlockCumulator},
    },
    stream::{BlockEventStore, BlockMachineOutput, BlockStream},
    wrapper::BlockMachineConfig,
};
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::prelude::{
    GetVersionRequest, SubscribeRequest, SubscribeRequestFilterTransactions, SubscribeUpdate,
};

use crate::clickhouse::{BlockFooterRecord, BlockMetadataRecord};
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

// Banks freeze on BlockMeta alone, so clusters that send no footers still publish.
const BLOCK_MACHINE_CONFIG: BlockMachineConfig = BlockMachineConfig {
    require_block_footer: false,
};

async fn run_block_machine_stream(cache: Arc<HeadCache>, cfg: DragonsmouthHeadCacheConfig) {
    let mut backoff = Duration::from_millis(250);
    let max_backoff = Duration::from_secs(5);

    loop {
        let session = CoverageSession::new(cache.clone(), cfg.min_commitment);
        match connect_and_subscribe(&cfg, cache.clone(), session.id).await {
            Ok(mut stream) => {
                info!(
                    endpoint = cfg.endpoint.as_str(),
                    min_commitment = ?cfg.min_commitment,
                    "head cache: subscribed to DragonsMouth"
                );
                backoff = Duration::from_millis(250);

                let end = pump(&cache, session.id, &mut stream).await;

                warn!(?end, "head cache: stream ended; reconnecting");
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

/// Why [`pump`] stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PumpEnd {
    StreamEnded,
    StreamError,
    WorkerFailed,
}

/// Applies each output on a blocking thread and waits for it before the next one.
async fn apply_output(
    cache: &Arc<HeadCache>,
    session: u64,
    output: BlockMachineOutput<BankBuffer>,
) -> Result<(), tokio::task::JoinError> {
    let cache = cache.clone();
    let queued = Instant::now();
    tokio::task::spawn_blocking(move || {
        metrics::head_cache_worker_wait(queued.elapsed().as_secs_f64());
        handle_output(&cache, session, output);
    })
    .await
}

/// Applies outputs in stream order and stops at a stream error or a failed worker.
/// A worker panic that poisoned a head cache lock is re-raised; any other ends the session.
async fn pump<St, E>(cache: &Arc<HeadCache>, session: u64, stream: &mut St) -> PumpEnd
where
    St: futures_util::Stream<Item = Result<BlockMachineOutput<BankBuffer>, E>> + Unpin,
    E: std::fmt::Debug,
{
    while let Some(result) = stream.next().await {
        let output = match result {
            Ok(output) => output,
            Err(err) => {
                warn!("head cache: block-machine error: {err:?}");
                return PumpEnd::StreamError;
            }
        };
        if let Err(err) = apply_output(cache, session, output).await {
            match err.try_into_panic() {
                Ok(panic) if cache.locks_poisoned() => std::panic::resume_unwind(panic),
                Ok(_) => {
                    error!("head cache: output worker panicked");
                    return PumpEnd::WorkerFailed;
                }
                Err(err) => {
                    warn!("head cache: output worker cancelled: {err}");
                    return PumpEnd::WorkerFailed;
                }
            }
        }
    }
    PumpEnd::StreamEnded
}

fn handle_output<S: BlockEventStore<EventT = SubscribeUpdate>>(
    cache: &HeadCache,
    session: u64,
    output: BlockMachineOutput<S>,
) {
    match output {
        BlockMachineOutput::FrozenBlock(block) => {
            let (slot, bank_id, blockhash) = (block.slot, block.bank_id, block.blockhash);
            let transactions = BlockEventStore::into_iter(block.events)
                .into_iter()
                .filter_map(|event| match event.update_oneof {
                    Some(
                        yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof::Transaction(
                            update,
                        ),
                    ) if update.slot == slot && update.bank_id == bank_id => update.transaction,
                    _ => None,
                })
                .collect::<Vec<_>>();
            let count = transactions.len() as u64;
            cache.freeze_bank(session, slot, bank_id, blockhash, transactions);
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
        footer: None,
    })
}

/// A footer needs the full 32-byte bank hash, as the ingestor requires before it stores one.
fn parse_block_footer(
    footer: &yellowstone_grpc_proto::prelude::SubscribeUpdateBlockFooter,
) -> Option<BlockFooterRecord> {
    if footer.bank_hash.len() != 32 {
        warn!(
            slot = footer.slot,
            "head cache: ignoring a block footer without a full bank hash"
        );
        return None;
    }
    Some(BlockFooterRecord {
        block_producer_time_nanos: footer.block_producer_time_nanos,
        block_user_agent: String::from_utf8_lossy(&footer.block_user_agent).into_owned(),
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
    // Footers are optional for freezing a bank. They are tapped to serve getBlock footers.
    request.block_footer.insert(
        RESERVED_FILTER_NAME.to_owned(),
        yellowstone_grpc_proto::prelude::SubscribeRequestFilterBlockFooter {
            include_certificates: Some(false),
        },
    );
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
    Ok(BlockStream::<_, SubscribeUpdate, _>::new_with_config(
        source,
        DragonsmouthBlockCumulator::default(),
        cfg.min_commitment,
        BLOCK_MACHINE_CONFIG,
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
    fn new(cache: Arc<HeadCache>, minimum: CommitmentLevel) -> Self {
        let id = cache.start_bank_session(minimum);
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
        Some(UpdateOneof::BlockFooter(footer)) => {
            if let Some(record) = parse_block_footer(footer) {
                cache.stage_bank_footer(session, footer.slot, footer.bank_id, record);
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
        let session = CoverageSession::new(cache.clone(), CommitmentLevel::Processed);
        let events = coverage_events(10, 9)
            .into_iter()
            .chain(coverage_events(12, 10));
        let source = futures_util::stream::iter(events.map(Ok::<_, std::io::Error>))
            .inspect(|event| observe_bank_metadata(&cache, session.id, event.as_ref().unwrap()));
        let mut stream = BlockStream::<_, SubscribeUpdate, _>::new_with_config(
            source,
            DragonsmouthBlockCumulator::default(),
            CommitmentLevel::Processed,
            BLOCK_MACHINE_CONFIG,
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

    #[tokio::test]
    async fn pump_applies_each_output_on_a_blocking_thread_in_order() {
        let waits = worker_waits();
        let cache = Arc::new(HeadCache::new(32, 64));
        let session = CoverageSession::new(cache.clone(), CommitmentLevel::Processed);
        let events = coverage_events(10, 9)
            .into_iter()
            .chain(coverage_events(12, 10));
        let source = futures_util::stream::iter(events.map(Ok::<_, std::io::Error>))
            .inspect(|event| observe_bank_metadata(&cache, session.id, event.as_ref().unwrap()));
        let mut stream = BlockStream::<_, SubscribeUpdate, _>::new_with_config(
            source,
            DragonsmouthBlockCumulator::default(),
            CommitmentLevel::Processed,
            BLOCK_MACHINE_CONFIG,
        );
        assert_eq!(
            pump(&cache, session.id, &mut stream).await,
            PumpEnd::StreamEnded
        );
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
        // One wait per output, and at least one output per block.
        assert!(worker_waits() >= waits + 2.0);
    }

    /// Events of one block with a single transaction whose signature is `[marker; 64]`.
    fn block_with_transaction(slot: u64, parent: u64, marker: u8) -> Vec<SubscribeUpdate> {
        use yellowstone_grpc_proto::prelude::{
            SubscribeUpdateTransaction, subscribe_update::UpdateOneof,
        };
        let mut events = coverage_events(slot, parent);
        for event in &mut events {
            match event.update_oneof.as_mut().unwrap() {
                UpdateOneof::Entry(entry) => entry.executed_transaction_count = 1,
                UpdateOneof::BlockMeta(meta) => meta.executed_transaction_count = 1,
                _ => {}
            }
        }
        events.insert(
            6,
            SubscribeUpdate {
                filters: vec![TRANSACTIONS_FILTER_NAME.to_string()],
                update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
                    slot,
                    bank_id: slot,
                    transaction: Some(super::super::banks::tests::transaction(marker)),
                })),
                ..Default::default()
            },
        );
        events
    }

    type Outputs = Arc<std::sync::Mutex<Vec<&'static str>>>;

    /// A block stream over `events` that applies the metadata tap and records the kind of
    /// every output it yields. It owns its handles, so a task can run it.
    fn recorded_stream(
        cache: &Arc<HeadCache>,
        session: u64,
        events: Vec<Result<SubscribeUpdate, std::io::Error>>,
    ) -> (
        impl futures_util::Stream<Item = Result<BlockMachineOutput<BankBuffer>, std::io::Error>>
        + Unpin
        + Send
        + 'static,
        Outputs,
    ) {
        let tap = cache.clone();
        let source = futures_util::stream::iter(events).inspect(move |event| {
            if let Ok(event) = event {
                observe_bank_metadata(&tap, session, event);
            }
        });
        let outputs = Outputs::default();
        let record = outputs.clone();
        let stream = BlockStream::<_, SubscribeUpdate, _>::new_with_config(
            source,
            DragonsmouthBlockCumulator::default(),
            CommitmentLevel::Processed,
            BLOCK_MACHINE_CONFIG,
        )
        .inspect(move |output| {
            let kind = match output {
                Ok(BlockMachineOutput::FrozenBlock(_)) => "frozen",
                Ok(BlockMachineOutput::SlotCommitmentUpdate(_)) => "commitment",
                Ok(_) => "other",
                Err(_) => "error",
            };
            record.lock().unwrap().push(kind);
        });
        (stream, outputs)
    }

    fn worker_waits() -> f64 {
        super::super::banks::tests::exported_count("superbank_head_cache_worker_wait_seconds")
    }

    /// Waits for a held worker without blocking the runtime that must keep driving the loop.
    async fn reached(rx: std::sync::mpsc::Receiver<u64>) {
        tokio::task::spawn_blocking(move || {
            rx.recv_timeout(Duration::from_secs(60))
                .expect("the worker reaches its pause point")
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn frozen_block_is_published_before_its_commitment_update() {
        let waits = worker_waits();
        let cache = Arc::new(HeadCache::new(32, 64));
        let session = CoverageSession::new(cache.clone(), CommitmentLevel::Processed);
        let events = block_with_transaction(42, 41, 1)
            .into_iter()
            .map(Ok)
            .collect();
        let (mut stream, outputs) = recorded_stream(&cache, session.id, events);
        let (paused, resume) = cache.arm_pause(crate::head_cache::PausePoint::Prepared);
        let task = tokio::spawn({
            let cache = cache.clone();
            let id = session.id;
            async move { pump(&cache, id, &mut stream).await }
        });
        reached(paused).await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        // The loop is still awaiting the freeze: it has taken no later output.
        assert_eq!(*outputs.lock().unwrap(), ["frozen"]);
        assert!(cache.banks.try_read().is_ok());
        assert!(
            cache
                .get_block(
                    42,
                    CommitmentLevel::Processed,
                    solana_transaction_status::TransactionDetails::Full
                )
                .is_none()
        );
        assert!(!cache.slot_commitment.contains_key(&42));
        assert!(
            cache
                .get_tx(
                    &crate::solana_sdk::signature::Signature::from([1; 64]),
                    CommitmentLevel::Processed
                )
                .is_none()
        );
        resume.send(()).unwrap();
        assert_eq!(task.await.unwrap(), PumpEnd::StreamEnded);
        assert_eq!(outputs.lock().unwrap()[0], "frozen");
        assert!(outputs.lock().unwrap().contains(&"commitment"));
        let block = cache
            .get_block(
                42,
                CommitmentLevel::Finalized,
                solana_transaction_status::TransactionDetails::Full,
            )
            .expect("served at the committed level");
        assert_eq!(block.metadata().executed_transaction_count, 1);
        assert!(
            cache
                .get_tx(
                    &crate::solana_sdk::signature::Signature::from([1; 64]),
                    CommitmentLevel::Finalized
                )
                .is_some()
        );
        assert!(worker_waits() >= waits + outputs.lock().unwrap().len() as f64);
    }

    #[tokio::test]
    async fn a_worker_panic_outside_a_hold_ends_the_session_and_the_next_one_publishes() {
        let cache = Arc::new(HeadCache::new(32, 64));
        let session = CoverageSession::new(cache.clone(), CommitmentLevel::Processed);
        let events = block_with_transaction(10, 9, 1)
            .into_iter()
            .chain(block_with_transaction(12, 10, 2))
            .map(Ok)
            .collect();
        let (mut stream, outputs) = recorded_stream(&cache, session.id, events);
        // Dropping the release sender makes the worker panic before it takes the bank lock.
        let (_paused, resume) = cache.arm_pause(crate::head_cache::PausePoint::Prepared);
        drop(resume);
        assert_eq!(
            pump(&cache, session.id, &mut stream).await,
            PumpEnd::WorkerFailed
        );
        // No later output was applied, and no lock is poisoned.
        assert_eq!(*outputs.lock().unwrap(), ["frozen"]);
        assert!(cache.banks.try_read().is_ok());
        assert!(!cache.locks_poisoned());
        assert_eq!(cache.latest_slot(), 0);
        for slot in [10, 12] {
            assert!(
                cache
                    .get_block(
                        slot,
                        CommitmentLevel::Processed,
                        solana_transaction_status::TransactionDetails::None
                    )
                    .is_none()
            );
        }
        // The next session starts clean and publishes normally.
        drop(session);
        let session = CoverageSession::new(cache.clone(), CommitmentLevel::Processed);
        let events = block_with_transaction(12, 10, 2)
            .into_iter()
            .map(Ok)
            .collect();
        let (mut stream, _) = recorded_stream(&cache, session.id, events);
        assert_eq!(
            pump(&cache, session.id, &mut stream).await,
            PumpEnd::StreamEnded
        );
        assert!(
            cache
                .get_block(
                    12,
                    CommitmentLevel::Finalized,
                    solana_transaction_status::TransactionDetails::Full
                )
                .is_some()
        );
    }

    #[tokio::test]
    async fn a_worker_panic_inside_a_bank_hold_is_re_raised_on_the_stream_task() {
        let cache = Arc::new(HeadCache::new(32, 64));
        let session = CoverageSession::new(cache.clone(), CommitmentLevel::Processed);
        let events = block_with_transaction(10, 9, 1)
            .into_iter()
            .map(Ok)
            .collect();
        let (mut stream, _) = recorded_stream(&cache, session.id, events);
        // Dropping the release sender makes the worker panic while it holds the bank lock.
        let (_paused, resume) = cache.arm_pause(crate::head_cache::PausePoint::RowsInserted);
        drop(resume);
        let task = tokio::spawn({
            let cache = cache.clone();
            let id = session.id;
            async move { pump(&cache, id, &mut stream).await }
        });
        assert!(task.await.unwrap_err().is_panic());
        assert!(cache.banks.is_poisoned());
        // Dropping the session would take the poisoned lock.
        std::mem::forget(session);
    }

    #[tokio::test]
    async fn pump_returns_on_a_stream_error_after_applying_earlier_outputs() {
        let cache = Arc::new(HeadCache::new(32, 64));
        let session = CoverageSession::new(cache.clone(), CommitmentLevel::Processed);
        let events = block_with_transaction(10, 9, 1)
            .into_iter()
            .map(Ok)
            .chain(std::iter::once(Err(std::io::Error::other("stream failed"))))
            .collect();
        let (mut stream, outputs) = recorded_stream(&cache, session.id, events);
        assert_eq!(
            pump(&cache, session.id, &mut stream).await,
            PumpEnd::StreamError
        );
        assert_eq!(outputs.lock().unwrap().last(), Some(&"error"));
        assert!(
            cache
                .get_block(
                    10,
                    CommitmentLevel::Finalized,
                    solana_transaction_status::TransactionDetails::Full
                )
                .is_some()
        );
    }

    fn footer_event(slot: u64, hash_len: usize, agent: &[u8]) -> SubscribeUpdate {
        SubscribeUpdate {
            update_oneof: Some(
                yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof::BlockFooter(
                    yellowstone_grpc_proto::prelude::SubscribeUpdateBlockFooter {
                        slot,
                        bank_id: slot,
                        bank_hash: vec![7; hash_len],
                        block_producer_time_nanos: 1_750_000_000_000_000_001,
                        block_user_agent: agent.to_vec(),
                        ..Default::default()
                    },
                ),
            ),
            ..Default::default()
        }
    }

    async fn published_footer(
        events: Vec<SubscribeUpdate>,
        slot: u64,
    ) -> Option<BlockFooterRecord> {
        let cache = Arc::new(HeadCache::new(32, 64));
        let session = CoverageSession::new(cache.clone(), CommitmentLevel::Processed);
        let source = futures_util::stream::iter(events.into_iter().map(Ok::<_, std::io::Error>))
            .inspect(|event| observe_bank_metadata(&cache, session.id, event.as_ref().unwrap()));
        let mut stream = BlockStream::<_, SubscribeUpdate, _>::new_with_config(
            source,
            DragonsmouthBlockCumulator::default(),
            CommitmentLevel::Processed,
            BLOCK_MACHINE_CONFIG,
        );
        while let Some(output) = stream.next().await {
            handle_output(&cache, session.id, output.unwrap());
        }
        let metadata = cache
            .slot_block_metadata
            .get(&slot)
            .expect("published block");
        metadata.footer.clone()
    }

    fn expected_footer(agent: &str) -> BlockFooterRecord {
        BlockFooterRecord {
            block_producer_time_nanos: 1_750_000_000_000_000_001,
            block_user_agent: agent.to_string(),
        }
    }

    #[tokio::test]
    async fn head_block_carries_a_footer_that_arrives_before_the_block_freezes() {
        let mut events = coverage_events(10, 9);
        events.insert(0, footer_event(10, 32, b"agave/4.4.0"));
        assert_eq!(
            published_footer(events, 10).await,
            Some(expected_footer("agave/4.4.0"))
        );
    }

    #[tokio::test]
    async fn head_block_gains_a_footer_that_arrives_after_it_is_published() {
        let mut events = coverage_events(10, 9);
        events.push(footer_event(10, 32, b"agave/4.4.0"));
        assert_eq!(
            published_footer(events, 10).await,
            Some(expected_footer("agave/4.4.0"))
        );
    }

    #[tokio::test]
    async fn head_block_without_a_footer_has_none() {
        assert_eq!(published_footer(coverage_events(10, 9), 10).await, None);
    }

    #[tokio::test]
    async fn head_footer_without_a_full_bank_hash_is_ignored() {
        let mut events = coverage_events(10, 9);
        events.insert(0, footer_event(10, 31, b"agave/4.4.0"));
        assert_eq!(published_footer(events, 10).await, None);
    }

    #[tokio::test]
    async fn head_footer_user_agent_is_decoded_like_the_stored_footer() {
        let mut events = coverage_events(10, 9);
        events.insert(0, footer_event(10, 32, &[0x61, 0xff]));
        assert_eq!(
            published_footer(events, 10).await,
            Some(expected_footer("a\u{fffd}"))
        );
    }

    #[tokio::test]
    async fn head_footer_of_another_bank_does_not_attach() {
        let mut events = coverage_events(10, 9);
        let mut other = footer_event(10, 32, b"other");
        if let Some(yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof::BlockFooter(
            footer,
        )) = other.update_oneof.as_mut()
        {
            footer.bank_id = 99;
        }
        events.insert(0, other);
        assert_eq!(published_footer(events, 10).await, None);
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
        let session = CoverageSession::new(cache.clone(), CommitmentLevel::Processed);
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
        let mut stream = BlockStream::<_, SubscribeUpdate, _>::new_with_config(
            source,
            DragonsmouthBlockCumulator::default(),
            CommitmentLevel::Processed,
            BLOCK_MACHINE_CONFIG,
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
        let session = CoverageSession::new(cache.clone(), CommitmentLevel::Processed);
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
        let mut stream = BlockStream::<_, SubscribeUpdate, _>::new_with_config(
            source,
            DragonsmouthBlockCumulator::default(),
            CommitmentLevel::Processed,
            BLOCK_MACHINE_CONFIG,
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
    #[tokio::test]
    async fn uncached_root_and_bank_blind_dead_event_discard_cached_processed_branch() {
        use yellowstone_grpc_proto::prelude::{
            SlotStatus, SubscribeUpdateSlot, subscribe_update::UpdateOneof,
        };
        for dead in [false, true] {
            let cache = Arc::new(HeadCache::new(32, 64));
            let session = CoverageSession::new(cache.clone(), CommitmentLevel::Processed);
            let mut events = coverage_events(42, 41);
            events.retain(|event| !matches!(event.update_oneof.as_ref(), Some(UpdateOneof::Slot(slot))
                if slot.status == SlotStatus::SlotConfirmed as i32 || slot.status == SlotStatus::SlotFinalized as i32));
            let status = |slot, parent, bank_id, status| SubscribeUpdate {
                update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                    slot,
                    parent,
                    bank_id,
                    status: status as i32,
                    ..Default::default()
                })),
                ..Default::default()
            };
            if dead {
                events.push(status(42, Some(41), None, SlotStatus::SlotDead));
            } else {
                // The new root's content is absent, but its confirmed parent
                // skips the cached branch and must still invalidate that branch.
                events.push(status(43, Some(41), Some(43), SlotStatus::SlotCreatedBank));
                events.push(status(43, Some(41), Some(43), SlotStatus::SlotConfirmed));
                events.push(status(43, Some(41), Some(43), SlotStatus::SlotFinalized));
            }
            let source =
                futures_util::stream::iter(events.into_iter().map(Ok::<_, std::io::Error>))
                    .inspect(|event| {
                        observe_bank_metadata(&cache, session.id, event.as_ref().unwrap())
                    });
            let mut stream = BlockStream::<_, SubscribeUpdate, _>::new_with_config(
                source,
                DragonsmouthBlockCumulator::default(),
                CommitmentLevel::Processed,
                BLOCK_MACHINE_CONFIG,
            );
            let mut saw_processed = false;
            while let Some(output) = stream.next().await {
                handle_output(&cache, session.id, output.unwrap());
                saw_processed |= cache
                    .get_block(
                        42,
                        CommitmentLevel::Processed,
                        solana_transaction_status::TransactionDetails::None,
                    )
                    .is_some();
            }
            assert!(
                saw_processed,
                "test must exercise an exposed abandoned branch"
            );
            assert!(
                cache
                    .get_block(
                        42,
                        CommitmentLevel::Processed,
                        solana_transaction_status::TransactionDetails::None
                    )
                    .is_none()
            );
            assert!(
                cache
                    .get_block(
                        43,
                        CommitmentLevel::Finalized,
                        solana_transaction_status::TransactionDetails::None
                    )
                    .is_none()
            );
        }
    }
    #[tokio::test]
    async fn uncached_old_root_and_bank_blind_minority_dead_preserve_winning_branch() {
        use yellowstone_grpc_proto::prelude::{
            SlotStatus, SubscribeUpdateSlot, subscribe_update::UpdateOneof,
        };
        let cache = Arc::new(HeadCache::new(32, 64));
        let session = CoverageSession::new(cache.clone(), CommitmentLevel::Processed);
        let mut events = coverage_events(42, 41);
        events.retain(|event| !matches!(event.update_oneof.as_ref(), Some(UpdateOneof::Slot(slot))
            if slot.status == SlotStatus::SlotConfirmed as i32 || slot.status == SlotStatus::SlotFinalized as i32));
        events.extend(coverage_events(43, 41));
        for (slot, parent, bank_id, status) in [
            (40, Some(39), Some(40), SlotStatus::SlotCreatedBank),
            (40, Some(39), Some(40), SlotStatus::SlotFinalized),
            (42, Some(41), None, SlotStatus::SlotDead),
        ] {
            events.push(SubscribeUpdate {
                update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                    slot,
                    parent,
                    bank_id,
                    status: status as i32,
                    ..Default::default()
                })),
                ..Default::default()
            });
        }
        let source = futures_util::stream::iter(events.into_iter().map(Ok::<_, std::io::Error>))
            .inspect(|event| observe_bank_metadata(&cache, session.id, event.as_ref().unwrap()));
        let mut stream = BlockStream::<_, SubscribeUpdate, _>::new_with_config(
            source,
            DragonsmouthBlockCumulator::default(),
            CommitmentLevel::Processed,
            BLOCK_MACHINE_CONFIG,
        );
        let mut saw_winner = false;
        while let Some(output) = stream.next().await {
            handle_output(&cache, session.id, output.unwrap());
            let winner = cache.get_block(
                43,
                CommitmentLevel::Finalized,
                solana_transaction_status::TransactionDetails::None,
            );
            if let Some(block) = winner {
                assert_eq!(block.metadata().blockhash, [43; 32]);
                saw_winner = true;
            } else {
                assert!(
                    !saw_winner,
                    "late old-root or minority Dead must preserve winning branch"
                );
            }
        }
        assert!(saw_winner);
        assert!(
            cache
                .get_block(
                    40,
                    CommitmentLevel::Processed,
                    solana_transaction_status::TransactionDetails::None
                )
                .is_none()
        );
        assert!(
            cache
                .get_block(
                    42,
                    CommitmentLevel::Processed,
                    solana_transaction_status::TransactionDetails::None
                )
                .is_none()
        );
        assert!(
            cache
                .get_block(
                    43,
                    CommitmentLevel::Finalized,
                    solana_transaction_status::TransactionDetails::None
                )
                .is_some()
        );
    }
}
