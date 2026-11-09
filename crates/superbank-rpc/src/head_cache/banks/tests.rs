// SPDX-License-Identifier: AGPL-3.0-only
use super::*;
use crate::head_cache::dragonsmouth::parse_block_meta;
use crate::head_cache::{HeadBankEvent, HeadBankEventKind};
use crate::solana_sdk::{pubkey::Pubkey, signature::Signature};
use yellowstone_grpc_proto::prelude::{
    Message, MessageHeader, SubscribeUpdateBlockMeta, Transaction, TransactionStatusMeta,
};

pub(crate) fn transaction(marker: u8) -> SubscribeUpdateTransactionInfo {
    SubscribeUpdateTransactionInfo {
        signature: vec![marker; 64],
        transaction: Some(Transaction {
            signatures: vec![vec![marker; 64]],
            message: Some(Message {
                header: Some(MessageHeader {
                    num_required_signatures: 1,
                    ..Default::default()
                }),
                account_keys: vec![vec![marker; 32]],
                recent_blockhash: vec![9; 32],
                ..Default::default()
            }),
        }),
        meta: Some(TransactionStatusMeta::default()),
        ..Default::default()
    }
}

fn metadata(slot: u64, marker: u8) -> BlockMetadataRecord {
    parse_block_meta(&SubscribeUpdateBlockMeta {
        slot,
        blockhash: bs58::encode([marker; 32]).into_string(),
        parent_slot: slot - 1,
        parent_blockhash: bs58::encode([0; 32]).into_string(),
        executed_transaction_count: 1,
        entries_count: 1,
        block_time: Some(yellowstone_grpc_proto::prelude::UnixTimestamp {
            timestamp: marker as i64,
        }),
        ..Default::default()
    })
    .unwrap()
}

fn freeze(cache: &HeadCache, session: u64, slot: u64, bank: u64, marker: u8) {
    cache.stage_bank_metadata(session, bank, metadata(slot, marker));
    stage_entry(cache, session, slot, bank, 1);
    cache.freeze_bank(session, slot, bank, [marker; 32], vec![transaction(marker)]);
}

fn stage_entry(cache: &HeadCache, session: u64, slot: u64, bank_id: u64, count: u64) {
    cache.stage_bank_entry(
        session,
        &yellowstone_grpc_proto::prelude::SubscribeUpdateEntry {
            slot,
            bank_id,
            hash: vec![9; 32],
            executed_transaction_count: count,
            ..Default::default()
        },
    );
}

#[test]
fn winning_bank_replaces_all_loser_indexes_and_never_promotes_old_results() {
    let cache = HeadCache::new(32, 64);
    let session = cache.start_bank_session(CommitmentLevel::Processed);
    freeze(&cache, session, 42, 1, 1);
    let old_meta = cache
        .get_meta(&Signature::from([1; 64]), CommitmentLevel::Processed)
        .unwrap();
    freeze(&cache, session, 42, 2, 2);
    assert_eq!(cache.slot_commitment(42), CommitmentLevel::Processed);
    assert!(
        cache
            .get_tx(&Signature::from([1; 64]), CommitmentLevel::Confirmed)
            .is_none()
    );
    cache.commit_bank(session, 42, 2, CommitmentLevel::Confirmed, Some(41));
    assert!(
        cache
            .get_tx(&Signature::from([1; 64]), CommitmentLevel::Processed)
            .is_none()
    );
    let tx = cache
        .get_tx(&Signature::from([2; 64]), CommitmentLevel::Confirmed)
        .unwrap();
    assert_eq!(tx.block_time, Some(2));
    assert!(
        cache
            .signatures_for_address(
                &Pubkey::from([1; 32]),
                None,
                None,
                10,
                CommitmentLevel::Processed
            )
            .is_empty()
    );
    assert_eq!(cache.confirmation_status_string(&old_meta), "processed");
    cache.commit_bank(session, 42, 2, CommitmentLevel::Finalized, Some(41));
    assert_eq!(cache.confirmation_status_string(&old_meta), "processed");
    assert!(
        cache
            .get_tx(&Signature::from([2; 64]), CommitmentLevel::Finalized)
            .is_some()
    );
    let block = cache
        .get_block(
            42,
            CommitmentLevel::Finalized,
            solana_transaction_status::TransactionDetails::Full,
        )
        .unwrap();
    assert_eq!(block.metadata().blockhash, [2; 32]);
}

#[test]
fn late_loser_metadata_status_and_freeze_cannot_replace_the_winner() {
    let cache = HeadCache::new(32, 64);
    let session = cache.start_bank_session(CommitmentLevel::Processed);
    freeze(&cache, session, 42, 1, 1);
    freeze(&cache, session, 42, 2, 2);
    cache.commit_bank(session, 42, 2, CommitmentLevel::Finalized, Some(41));
    freeze(&cache, session, 42, 1, 3);
    cache.commit_bank(session, 42, 1, CommitmentLevel::Finalized, Some(41));
    cache.discard_bank(session, 42, 1);
    assert_eq!(cache.slot_block_time_for_tests(42), Some(2));
    assert!(
        cache
            .get_tx(&Signature::from([2; 64]), CommitmentLevel::Finalized)
            .is_some()
    );
    assert!(
        cache
            .get_tx(&Signature::from([3; 64]), CommitmentLevel::Processed)
            .is_none()
    );
}

#[test]
fn pruning_one_bank_preserves_its_sibling_and_other_slots() {
    let cache = HeadCache::new(32, 64);
    let session = cache.start_bank_session(CommitmentLevel::Processed);
    freeze(&cache, session, 42, 1, 1);
    freeze(&cache, session, 42, 2, 2);
    freeze(&cache, session, 43, 3, 3);
    cache.discard_bank(session, 42, 1);
    cache.commit_bank(session, 42, 2, CommitmentLevel::Confirmed, Some(41));
    assert!(
        cache
            .get_tx(&Signature::from([2; 64]), CommitmentLevel::Confirmed)
            .is_some()
    );
    assert!(
        cache
            .get_tx(&Signature::from([3; 64]), CommitmentLevel::Processed)
            .is_some()
    );
}

#[test]
fn reconnect_reusing_bank_ids_cannot_promote_old_transactions_or_accept_old_events() {
    let cache = HeadCache::new(32, 64);
    let first = cache.start_bank_session(CommitmentLevel::Processed);
    freeze(&cache, first, 42, 7, 1);
    let old_meta = cache
        .get_meta(&Signature::from([1; 64]), CommitmentLevel::Processed)
        .unwrap();
    let old_block = cache
        .get_block(
            42,
            CommitmentLevel::Processed,
            solana_transaction_status::TransactionDetails::Full,
        )
        .unwrap();
    cache.end_bank_session(first);
    assert!(
        cache
            .get_tx(&Signature::from([1; 64]), CommitmentLevel::Processed)
            .is_none()
    );
    assert!(
        cache
            .coverage
            .read()
            .unwrap()
            .snapshot(
                42,
                None,
                CommitmentLevel::Processed,
                std::time::Instant::now()
            )
            .is_err()
    );
    let second = cache.start_bank_session(CommitmentLevel::Processed);
    cache.commit_bank(second, 42, 7, CommitmentLevel::Finalized, Some(41));
    assert!(
        cache
            .get_tx(&Signature::from([1; 64]), CommitmentLevel::Finalized)
            .is_none()
    );
    freeze(&cache, second, 42, 7, 2);
    freeze(&cache, first, 42, 7, 3);
    cache.discard_bank(first, 42, 7);
    cache.end_bank_session(first);
    assert!(
        cache
            .get_tx(&Signature::from([2; 64]), CommitmentLevel::Finalized)
            .is_some()
    );
    assert!(
        cache
            .get_tx(&Signature::from([3; 64]), CommitmentLevel::Processed)
            .is_none()
    );
    assert_eq!(cache.confirmation_status_string(&old_meta), "processed");
    assert_eq!(old_block.metadata().blockhash, [1; 32]);
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

#[test]
fn status_before_sealing_selects_only_that_bank_and_retention_removes_bank_buffers() {
    let cache = HeadCache::new(2, 64);
    let session = cache.start_bank_session(CommitmentLevel::Processed);
    cache.commit_bank(session, 42, 2, CommitmentLevel::Confirmed, Some(41));
    freeze(&cache, session, 42, 1, 1);
    freeze(&cache, session, 42, 2, 2);
    assert!(
        cache
            .get_tx(&Signature::from([2; 64]), CommitmentLevel::Confirmed)
            .is_some()
    );
    freeze(&cache, session, 44, 4, 4);
    assert!(
        cache
            .get_tx(&Signature::from([2; 64]), CommitmentLevel::Processed)
            .is_none()
    );
    assert!(
        !cache
            .banks
            .read()
            .unwrap()
            .banks
            .keys()
            .any(|key| key.0 == 42)
    );
}

#[test]
fn metadata_with_an_unmatched_sealed_hash_cannot_publish_transactions() {
    let cache = HeadCache::new(32, 64);
    let session = cache.start_bank_session(CommitmentLevel::Processed);
    cache.stage_bank_metadata(session, 7, metadata(42, 1));
    cache.freeze_bank(session, 42, 7, [2; 32], vec![transaction(2)]);
    cache.commit_bank(session, 42, 7, CommitmentLevel::Finalized, Some(41));
    assert!(
        cache
            .get_tx(&Signature::from([2; 64]), CommitmentLevel::Finalized)
            .is_none()
    );
}

#[test]
fn winner_status_removes_a_visible_loser_before_winner_content_and_blocks_old_tip_fallback() {
    let cache = HeadCache::new(32, 64);
    let session = cache.start_bank_session(CommitmentLevel::Processed);
    freeze(&cache, session, 42, 1, 1);
    cache.commit_bank(session, 42, 2, CommitmentLevel::Confirmed, Some(41));
    assert!(
        cache
            .get_tx(&Signature::from([1; 64]), CommitmentLevel::Processed)
            .is_none()
    );
    assert!(
        cache
            .coverage
            .read()
            .unwrap()
            .snapshot(
                42,
                None,
                CommitmentLevel::Confirmed,
                std::time::Instant::now()
            )
            .is_err()
    );
    freeze(&cache, session, 42, 2, 2);
    cache.commit_bank(session, 43, 3, CommitmentLevel::Confirmed, Some(42));
    assert!(
        cache
            .coverage
            .read()
            .unwrap()
            .snapshot(
                42,
                None,
                CommitmentLevel::Confirmed,
                std::time::Instant::now()
            )
            .is_err()
    );
}

#[test]
fn incomplete_competing_bank_cannot_publish_or_borrow_winner_commitment() {
    let cache = HeadCache::new(32, 64);
    let session = cache.start_bank_session(CommitmentLevel::Processed);
    freeze(&cache, session, 42, 1, 1);
    let loser = cache
        .get_meta(&Signature::from([1; 64]), CommitmentLevel::Processed)
        .unwrap();
    let mut meta = metadata(42, 2);
    meta.executed_transaction_count = 2;
    cache.stage_bank_metadata(session, 2, meta);
    stage_entry(&cache, session, 42, 2, 2);
    cache.freeze_bank(session, 42, 2, [2; 32], vec![transaction(2)]);
    cache.commit_bank(session, 42, 2, CommitmentLevel::Finalized, Some(41));
    assert!(
        cache
            .get_block(
                42,
                CommitmentLevel::Finalized,
                solana_transaction_status::TransactionDetails::Full
            )
            .is_none()
    );
    assert!(
        cache
            .get_meta(&Signature::from([2; 64]), CommitmentLevel::Processed)
            .is_none()
    );
    assert_eq!(cache.confirmation_status_string(&loser), "processed");
    let mut second = transaction(3);
    second.index = 1;
    cache.freeze_bank(session, 42, 2, [2; 32], vec![transaction(2), second]);
    assert!(
        cache
            .get_tx(&Signature::from([2; 64]), CommitmentLevel::Finalized)
            .is_some()
    );
    assert!(
        cache
            .get_tx(&Signature::from([1; 64]), CommitmentLevel::Finalized)
            .is_none()
    );
    assert_eq!(cache.confirmation_status_string(&loser), "processed");
}

#[test]
fn duplicate_indices_signatures_and_malformed_transactions_cannot_seal_a_bank() {
    for corrupt in 0..3 {
        let cache = HeadCache::new(32, 64);
        let session = cache.start_bank_session(CommitmentLevel::Processed);
        let mut meta = metadata(42, 2);
        meta.executed_transaction_count = 2;
        cache.stage_bank_metadata(session, 2, meta);
        stage_entry(&cache, session, 42, 2, 2);
        let mut second = transaction(3);
        second.index = 1;
        match corrupt {
            0 => second.index = 0,
            1 => second.signature = transaction(2).signature,
            _ => second.meta = None,
        }
        cache.commit_bank(session, 42, 2, CommitmentLevel::Finalized, Some(41));
        cache.freeze_bank(session, 42, 2, [2; 32], vec![transaction(2), second]);
        assert!(
            cache
                .get_meta(&Signature::from([2; 64]), CommitmentLevel::Processed)
                .is_none()
        );
        assert!(
            cache
                .get_block(
                    42,
                    CommitmentLevel::Finalized,
                    solana_transaction_status::TransactionDetails::Full
                )
                .is_none()
        );
    }
}

#[test]
fn entry_count_indices_ranges_and_hashes_must_be_complete_before_publication() {
    use yellowstone_grpc_proto::prelude::SubscribeUpdateEntry;
    for corrupt in 0..4 {
        let cache = HeadCache::new(32, 64);
        let session = cache.start_bank_session(CommitmentLevel::Processed);
        let mut meta = metadata(42, 2);
        meta.entry_count = 2;
        cache.stage_bank_metadata(session, 2, meta);
        let first = SubscribeUpdateEntry {
            slot: 42,
            bank_id: 2,
            hash: vec![9; 32],
            executed_transaction_count: 1,
            ..Default::default()
        };
        let mut last = SubscribeUpdateEntry {
            slot: 42,
            bank_id: 2,
            index: 1,
            hash: vec![9; 32],
            starting_transaction_index: 1,
            ..Default::default()
        };
        cache.stage_bank_entry(session, &first);
        match corrupt {
            0 => {}
            1 => {
                last.index = 0;
                cache.stage_bank_entry(session, &last);
            }
            2 => {
                last.starting_transaction_index = 0;
                cache.stage_bank_entry(session, &last);
            }
            _ => {
                last.hash.pop();
                cache.stage_bank_entry(session, &last);
            }
        }
        cache.commit_bank(session, 42, 2, CommitmentLevel::Finalized, Some(41));
        cache.freeze_bank(session, 42, 2, [2; 32], vec![transaction(2)]);
        assert!(
            cache
                .get_tx(&Signature::from([2; 64]), CommitmentLevel::Finalized)
                .is_none()
        );
        assert!(
            cache
                .get_block(
                    42,
                    CommitmentLevel::Finalized,
                    solana_transaction_status::TransactionDetails::Full
                )
                .is_none()
        );
    }
}

#[test]
fn frozen_bank_cannot_expose_processed_data_below_session_minimum_to_concurrent_readers() {
    for minimum in [CommitmentLevel::Confirmed, CommitmentLevel::Finalized] {
        let cache = Arc::new(HeadCache::new(32, 64));
        let session = cache.start_bank_session(minimum);
        freeze(&cache, session, 42, 7, 1);
        std::thread::scope(|scope| {
            for _ in 0..4 {
                let cache = &cache;
                scope.spawn(move || {
                    for _ in 0..1000 {
                        assert!(
                            cache
                                .get_tx(&Signature::from([1; 64]), CommitmentLevel::Processed)
                                .is_none()
                        );
                        assert!(
                            cache
                                .get_meta(&Signature::from([1; 64]), CommitmentLevel::Processed)
                                .is_none()
                        );
                        assert!(
                            cache
                                .get_block(
                                    42,
                                    CommitmentLevel::Processed,
                                    solana_transaction_status::TransactionDetails::Full
                                )
                                .is_none()
                        );
                        assert!(
                            cache
                                .signatures_for_address(
                                    &Pubkey::from([1; 32]),
                                    None,
                                    None,
                                    10,
                                    CommitmentLevel::Processed
                                )
                                .is_empty()
                        );
                        assert_eq!(cache.latest_slot(), 0);
                    }
                });
            }
        });
        if minimum == CommitmentLevel::Finalized {
            cache.commit_bank(session, 42, 7, CommitmentLevel::Confirmed, Some(41));
            assert!(
                cache
                    .get_tx(&Signature::from([1; 64]), CommitmentLevel::Processed)
                    .is_none()
            );
        }
        cache.commit_bank(session, 42, 7, minimum, Some(41));
        let metadata = cache
            .get_meta(&Signature::from([1; 64]), CommitmentLevel::Processed)
            .unwrap();
        assert_eq!(
            cache.confirmation_status_string(&metadata),
            match minimum {
                CommitmentLevel::Confirmed => "confirmed",
                _ => "finalized",
            }
        );
        assert!(cache.get_tx(&Signature::from([1; 64]), minimum).is_some());
    }
}

#[test]
fn repeated_signature_on_processed_winner_is_reprojected_when_abandoned_slot_is_discarded() {
    for discard_first in [false, true] {
        let cache = HeadCache::new(32, 64);
        let session = cache.start_bank_session(CommitmentLevel::Processed);
        freeze(&cache, session, 42, 1, 1);
        let old = cache
            .get_meta(&Signature::from([1; 64]), CommitmentLevel::Processed)
            .unwrap();
        cache.stage_bank_metadata(session, 2, metadata(43, 2));
        stage_entry(&cache, session, 43, 2, 1);
        cache.freeze_bank(session, 43, 2, [2; 32], vec![transaction(1)]);
        assert_eq!(
            cache
                .signature_position(&Signature::from([1; 64]))
                .unwrap()
                .slot,
            42
        );
        if discard_first {
            cache.discard_bank(session, 42, 1);
        }
        cache.commit_bank(session, 43, 2, CommitmentLevel::Confirmed, Some(41));
        if !discard_first {
            cache.discard_bank(session, 42, 1);
        }
        let winning = cache
            .get_tx(&Signature::from([1; 64]), CommitmentLevel::Confirmed)
            .unwrap();
        assert_eq!(winning.slot, 43);
        assert_eq!(winning.block_time, Some(2));
        assert_eq!(
            cache
                .signature_position(&Signature::from([1; 64]))
                .unwrap()
                .slot,
            43
        );
        assert_eq!(
            cache
                .signatures_for_address(
                    &Pubkey::from([1; 32]),
                    None,
                    None,
                    10,
                    CommitmentLevel::Processed
                )
                .len(),
            1
        );
        assert!(
            cache
                .get_block(
                    43,
                    CommitmentLevel::Confirmed,
                    solana_transaction_status::TransactionDetails::Full
                )
                .is_some()
        );
        assert_eq!(cache.confirmation_status_string(&old), "processed");
        cache.commit_bank(session, 42, 1, CommitmentLevel::Finalized, Some(41));
        assert_eq!(
            cache.confirmation_status_string(&old),
            "processed",
            "discarded records never borrow the winner's token"
        );
    }
}

#[test]
fn republishing_a_projected_bank_skips_record_conversion() {
    let cache = HeadCache::new(32, 64);
    let session = cache.start_bank_session(CommitmentLevel::Processed);
    freeze(&cache, session, 42, 1, 1);
    let signature = Signature::from([1; 64]);
    let token = cache.banks.read().unwrap().banks[&(42, 1)]
        .commitment
        .clone();
    assert!(cache.keeps_existing_projection(&signature, 42, Some(&token)));
    assert!(!cache.keeps_existing_projection(&Signature::from([2; 64]), 42, Some(&token)));
    // A malformed payload for an already-projected signature is never converted.
    let mut malformed = transaction(1);
    malformed.transaction = None;
    cache.ingest_bank_transaction(42, &malformed, Some(token));
    assert!(
        cache
            .get_tx(&signature, CommitmentLevel::Processed)
            .is_some()
    );
}

#[test]
fn reclaimed_signature_keeps_address_index_newest_first() {
    let cache = HeadCache::new(32, 64);
    let session = cache.start_bank_session(CommitmentLevel::Processed);
    let address = Pubkey::from([1; 32]);
    freeze(&cache, session, 100, 1, 1);
    cache.stage_bank_metadata(session, 2, metadata(101, 2));
    stage_entry(&cache, session, 101, 2, 1);
    cache.freeze_bank(session, 101, 2, [2; 32], vec![transaction(1)]);
    for (slot, bank, marker) in [(102, 3, 3), (103, 4, 4)] {
        let mut tx = transaction(marker);
        tx.transaction
            .as_mut()
            .unwrap()
            .message
            .as_mut()
            .unwrap()
            .account_keys
            .push(vec![1; 32]);
        cache.stage_bank_metadata(session, bank, metadata(slot, marker));
        stage_entry(&cache, session, slot, bank, 1);
        cache.freeze_bank(session, slot, bank, [marker; 32], vec![tx]);
    }
    cache.discard_bank(session, 100, 1);
    cache.commit_bank(session, 101, 2, CommitmentLevel::Confirmed, Some(100));
    let slots: Vec<_> = cache
        .signatures_for_address(&address, None, None, 10, CommitmentLevel::Processed)
        .iter()
        .map(|meta| meta.pos.slot)
        .collect();
    assert_eq!(slots, vec![103, 102, 101], "{slots:?}");
}

#[test]
fn finalized_minimum_publishes_banks_older_than_the_retention_window() {
    let cache = HeadCache::new(32, 64);
    let session = cache.start_bank_session(CommitmentLevel::Finalized);
    freeze(&cache, session, 10, 1, 1);
    for slot in 11..=45 {
        cache.stage_bank_metadata(session, slot, metadata(slot, slot as u8));
    }
    cache.commit_bank(session, 10, 1, CommitmentLevel::Finalized, Some(9));
    assert!(
        cache
            .get_tx(&Signature::from([1; 64]), CommitmentLevel::Finalized)
            .is_some()
    );
}

async fn rpc_statuses(
    state: std::sync::Arc<crate::state::AppState>,
    commitment: &str,
    minimum: u64,
) -> serde_json::Value {
    let signatures: Vec<_> = (1..=3)
        .map(|marker| Signature::from([marker; 64]).to_string())
        .collect();
    let response = crate::handlers::signatures::handle_get_signature_statuses(
        state,
        serde_json::json!(1),
        Some(vec![
            serde_json::json!(signatures),
            serde_json::json!({"commitment": commitment, "minContextSlot": minimum}),
        ]),
    )
    .await
    .unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}

#[tokio::test]
async fn status_rpc_does_not_promote_replaced_or_reconnected_bank_results() {
    let cache = std::sync::Arc::new(HeadCache::new(32, 64));
    let mut state = crate::tests::test_state();
    std::sync::Arc::get_mut(&mut state).unwrap().head_cache = Some(cache.clone());
    let session = cache.start_bank_session(CommitmentLevel::Processed);
    freeze(&cache, session, 42, 1, 1);
    let before = rpc_statuses(state.clone(), "processed", 42).await;
    assert_eq!(
        before["result"]["value"][0]["confirmationStatus"],
        "processed"
    );

    freeze(&cache, session, 42, 2, 2);
    cache.commit_bank(session, 42, 2, CommitmentLevel::Confirmed, Some(41));
    let winner = rpc_statuses(state.clone(), "confirmed", 42).await;
    assert_eq!(winner["result"]["context"]["slot"], 42);
    assert!(winner["result"]["value"][0].is_null());
    assert_eq!(
        winner["result"]["value"][1]["confirmationStatus"],
        "confirmed"
    );

    cache.end_bank_session(session);
    let disconnected = rpc_statuses(state.clone(), "confirmed", 42).await;
    assert_eq!(disconnected["error"]["code"], -32016);
    assert_eq!(disconnected["error"]["data"]["contextSlot"], 1);

    let session = cache.start_bank_session(CommitmentLevel::Processed);
    freeze(&cache, session, 42, 2, 3);
    let unconfirmed = rpc_statuses(state.clone(), "confirmed", 42).await;
    assert_eq!(unconfirmed["error"]["code"], -32016);
    let processed = rpc_statuses(state, "processed", 42).await;
    assert!(processed["result"]["value"][1].is_null());
    assert_eq!(
        processed["result"]["value"][2]["confirmationStatus"],
        "processed"
    );
}

fn drain(
    events: &mut tokio::sync::broadcast::Receiver<HeadBankEvent>,
) -> Vec<(u64, u8, HeadBankEventKind)> {
    std::iter::from_fn(|| events.try_recv().ok())
        .map(|event| (event.slot, event.blockhash[0], event.kind))
        .collect()
}

#[test]
fn bank_events_announce_each_commitment_once_per_published_bank() {
    use HeadBankEventKind::{Confirmed, Discarded, Finalized};
    let cache = HeadCache::new(32, 64);
    let session = cache.start_bank_session(CommitmentLevel::Processed);
    let mut events = cache.subscribe_bank_events();

    freeze(&cache, session, 42, 1, 1);
    assert_eq!(drain(&mut events), vec![]);
    cache.commit_bank(session, 42, 1, CommitmentLevel::Confirmed, Some(41));
    cache.commit_bank(session, 42, 1, CommitmentLevel::Confirmed, Some(41));
    assert_eq!(drain(&mut events), vec![(42, 1, Confirmed)]);
    cache.commit_bank(session, 42, 1, CommitmentLevel::Finalized, Some(41));
    cache.commit_bank(session, 42, 1, CommitmentLevel::Finalized, Some(41));
    assert_eq!(drain(&mut events), vec![(42, 1, Finalized)]);

    // Status before sealing: announced when the block becomes readable.
    cache.commit_bank(session, 43, 3, CommitmentLevel::Confirmed, Some(42));
    assert_eq!(drain(&mut events), vec![]);
    freeze(&cache, session, 43, 3, 3);
    assert_eq!(drain(&mut events), vec![(43, 3, Confirmed)]);

    // A jump to finalized still announces confirmed first.
    freeze(&cache, session, 44, 4, 4);
    cache.commit_bank(session, 44, 4, CommitmentLevel::Finalized, Some(43));
    assert_eq!(
        drain(&mut events),
        vec![(44, 4, Confirmed), (44, 4, Finalized)]
    );

    // A canonical status for another bank discards the published processed bank.
    freeze(&cache, session, 45, 5, 5);
    freeze(&cache, session, 45, 6, 6);
    cache.commit_bank(session, 45, 6, CommitmentLevel::Confirmed, Some(44));
    assert_eq!(
        drain(&mut events),
        vec![(45, 5, Discarded), (45, 6, Confirmed)]
    );

    // Discarding a published bank; an unpublished sibling sends nothing.
    freeze(&cache, session, 46, 7, 7);
    freeze(&cache, session, 46, 8, 8);
    cache.discard_bank(session, 46, 8);
    assert_eq!(drain(&mut events), vec![]);
    cache.discard_bank(session, 46, 7);
    assert_eq!(drain(&mut events), vec![(46, 7, Discarded)]);
}

#[test]
fn bank_events_report_slots_leaving_the_head() {
    use HeadBankEventKind::{Confirmed, Evicted};
    let cache = HeadCache::new(2, 64);
    let session = cache.start_bank_session(CommitmentLevel::Confirmed);
    let mut events = cache.subscribe_bank_events();
    freeze(&cache, session, 10, 1, 1);
    cache.commit_bank(session, 10, 1, CommitmentLevel::Confirmed, Some(9));
    freeze(&cache, session, 11, 2, 2);
    cache.commit_bank(session, 11, 2, CommitmentLevel::Confirmed, Some(10));
    assert_eq!(
        drain(&mut events),
        vec![(10, 1, Confirmed), (11, 2, Confirmed)]
    );
    freeze(&cache, session, 12, 3, 3);
    cache.commit_bank(session, 12, 3, CommitmentLevel::Confirmed, Some(11));
    assert_eq!(
        drain(&mut events),
        vec![(10, 1, Evicted), (12, 3, Confirmed)]
    );
    // A late status for an evicted slot does not announce it again.
    cache.commit_bank(session, 10, 1, CommitmentLevel::Finalized, Some(9));
    assert_eq!(drain(&mut events), vec![]);
    cache.end_bank_session(session);
    let mut left = drain(&mut events);
    left.sort_unstable_by_key(|event| event.0);
    assert_eq!(left, vec![(11, 2, Evicted), (12, 3, Evicted)]);
}

#[test]
fn commit_notify_wakes_waiters_after_commit_and_discard() {
    use futures_util::FutureExt;
    let cache = HeadCache::new(32, 64);
    let session = cache.start_bank_session(CommitmentLevel::Processed);
    freeze(&cache, session, 42, 1, 1);
    for step in 0..2 {
        let notified = cache.commit_notify().notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        cache.stage_bank_metadata(session, 2, metadata(43, 2));
        assert!(
            notified.as_mut().now_or_never().is_none(),
            "staging is silent"
        );
        if step == 0 {
            cache.commit_bank(session, 42, 1, CommitmentLevel::Confirmed, Some(41));
        } else {
            cache.discard_bank(session, 43, 2);
        }
        assert!(notified.now_or_never().is_some(), "step {step}");
    }
}

/// A cache whose bank 1 holds `[1; 64]` at `processed` in slot 42.
fn waiting_cache(wait_ms: u64, max_waiters: usize) -> (Arc<HeadCache>, u64) {
    let cache = HeadCache::new(32, 64)
        .with_commitment_wait(std::time::Duration::from_millis(wait_ms), max_waiters);
    let session = cache.start_bank_session(CommitmentLevel::Processed);
    freeze(&cache, session, 42, 1, 1);
    (Arc::new(cache), session)
}

fn spawn_waiter(
    cache: &Arc<HeadCache>,
    commitment: CommitmentLevel,
) -> tokio::task::JoinHandle<Option<Arc<crate::clickhouse::StoredTransactionRecord>>> {
    let cache = cache.clone();
    tokio::spawn(async move {
        cache
            .get_tx_waiting(&Signature::from([1; 64]), commitment, Some(42))
            .await
    })
}

const SHORT: std::time::Duration = std::time::Duration::from_millis(20);

#[tokio::test]
async fn commitment_wait_returns_a_transaction_committed_during_the_wait() {
    let (cache, session) = waiting_cache(10_000, 4);
    let started = Instant::now();
    let waiter = spawn_waiter(&cache, CommitmentLevel::Confirmed);
    tokio::time::sleep(SHORT).await;
    // Another bank wakes the waiter without raising its signature.
    freeze(&cache, session, 43, 2, 2);
    tokio::time::sleep(SHORT).await;
    assert!(!waiter.is_finished());
    cache.commit_bank(session, 42, 1, CommitmentLevel::Confirmed, Some(41));
    let record = waiter.await.unwrap().expect("confirmed during the wait");
    assert_eq!(record.slot, 42);
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
}

#[tokio::test]
async fn commitment_wait_skips_when_off_or_the_head_lacks_the_signature() {
    use crate::head_cache::commitment_wait::CAPPED;
    use futures_util::FutureExt;
    let held = Signature::from([1; 64]);
    let off = HeadCache::new(32, 64);
    let session = off.start_bank_session(CommitmentLevel::Processed);
    freeze(&off, session, 42, 1, 1);
    let lookup = |cache: &HeadCache, signature: &Signature, commitment, slot| {
        cache
            .get_tx_waiting(signature, commitment, slot)
            .now_or_never()
            .expect("resolves without waiting")
    };
    assert!(lookup(&off, &held, CommitmentLevel::Confirmed, None).is_none());

    let (cache, _) = waiting_cache(10_000, 4);
    let unknown = Signature::from([9; 64]);
    assert!(lookup(&cache, &unknown, CommitmentLevel::Confirmed, None).is_none());
    assert!(lookup(&cache, &held, CommitmentLevel::Confirmed, Some(41)).is_none());
    assert!(lookup(&cache, &held, CommitmentLevel::Processed, None).is_some());

    let (none_allowed, _) = waiting_cache(10_000, 0);
    let capped = crate::metrics::head_cache_commitment_waits_count(CAPPED);
    assert!(lookup(&none_allowed, &held, CommitmentLevel::Confirmed, None).is_none());
    assert!(crate::metrics::head_cache_commitment_waits_count(CAPPED) > capped);
}

#[tokio::test]
async fn commitment_wait_times_out_without_returning_a_lower_commitment() {
    use crate::head_cache::commitment_wait::TIMEOUT;
    let (cache, session) = waiting_cache(150, 4);
    let timeouts = crate::metrics::head_cache_commitment_waits_count(TIMEOUT);
    let started = Instant::now();
    let waiter = spawn_waiter(&cache, CommitmentLevel::Finalized);
    tokio::time::sleep(SHORT).await;
    cache.commit_bank(session, 42, 1, CommitmentLevel::Confirmed, Some(41));
    assert!(waiter.await.unwrap().is_none());
    let elapsed = started.elapsed();
    assert!(
        elapsed >= std::time::Duration::from_millis(150),
        "{elapsed:?}"
    );
    assert!(elapsed < std::time::Duration::from_secs(5), "{elapsed:?}");
    assert!(crate::metrics::head_cache_commitment_waits_count(TIMEOUT) > timeouts);
}

#[tokio::test]
async fn commitment_wait_stops_when_the_head_drops_the_signature() {
    use crate::head_cache::commitment_wait::GONE;
    let (cache, session) = waiting_cache(10_000, 4);
    let gone = crate::metrics::head_cache_commitment_waits_count(GONE);
    let started = Instant::now();
    let waiter = spawn_waiter(&cache, CommitmentLevel::Confirmed);
    tokio::time::sleep(SHORT).await;
    cache.discard_bank(session, 42, 1);
    assert!(waiter.await.unwrap().is_none());
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
    assert!(crate::metrics::head_cache_commitment_waits_count(GONE) > gone);
}

#[tokio::test]
async fn commitment_wait_caps_concurrent_waiters() {
    use futures_util::FutureExt;
    let (cache, session) = waiting_cache(10_000, 1);
    let held = Signature::from([1; 64]);
    let first = cache.get_tx_waiting(&held, CommitmentLevel::Confirmed, None);
    tokio::pin!(first);
    assert!(futures_util::poll!(first.as_mut()).is_pending());
    let second = cache.get_tx_waiting(&held, CommitmentLevel::Confirmed, None);
    assert!(matches!(second.now_or_never(), Some(None)), "past the cap");

    cache.commit_bank(session, 42, 1, CommitmentLevel::Confirmed, Some(41));
    assert!(first.await.is_some());
    // A finished or dropped waiter returns its permit.
    for _ in 0..2 {
        let next = cache.get_tx_waiting(&held, CommitmentLevel::Finalized, None);
        tokio::pin!(next);
        assert!(futures_util::poll!(next.as_mut()).is_pending());
    }
}

#[test]
fn block_identity_matches_get_block() {
    use solana_transaction_status::TransactionDetails;
    let cache = HeadCache::new(32, 64);
    let session = cache.start_bank_session(CommitmentLevel::Processed);
    let identity_and_block = |commitment| {
        let identity = cache.block_identity(42, commitment);
        let block = cache
            .get_block(42, commitment, TransactionDetails::Full)
            .map(|block| block.metadata().blockhash);
        assert_eq!(identity.map(|(hash, _)| hash), block, "{commitment:?}");
        identity
    };
    assert_eq!(identity_and_block(CommitmentLevel::Processed), None);
    freeze(&cache, session, 42, 1, 1);
    assert_eq!(
        identity_and_block(CommitmentLevel::Processed),
        Some(([1; 32], CommitmentLevel::Processed))
    );
    assert_eq!(identity_and_block(CommitmentLevel::Confirmed), None);
    freeze(&cache, session, 42, 2, 2);
    cache.commit_bank(session, 42, 2, CommitmentLevel::Confirmed, Some(41));
    assert_eq!(
        identity_and_block(CommitmentLevel::Confirmed),
        Some(([2; 32], CommitmentLevel::Confirmed))
    );
    assert_eq!(identity_and_block(CommitmentLevel::Finalized), None);
    cache.commit_bank(session, 42, 2, CommitmentLevel::Finalized, Some(41));
    assert_eq!(
        identity_and_block(CommitmentLevel::Confirmed),
        Some(([2; 32], CommitmentLevel::Finalized))
    );
    cache.discard_bank(session, 42, 2);
    assert_eq!(identity_and_block(CommitmentLevel::Processed), None);
}

/// Stages bank 1 of `slot` on `parent`; `transactions` other than 1 cannot seal.
fn finalize_on(cache: &HeadCache, session: u64, slot: u64, parent: u64, transactions: u64) {
    let mut meta = metadata(slot, slot as u8);
    meta.parent_slot = parent;
    meta.parent_blockhash = [parent as u8; 32];
    meta.executed_transaction_count = transactions;
    cache.stage_bank_metadata(session, 1, meta);
    stage_entry(cache, session, slot, 1, 1);
    let tx = transaction(slot as u8);
    cache.freeze_bank(session, slot, 1, [slot as u8; 32], vec![tx]);
    cache.commit_bank(session, slot, 1, CommitmentLevel::Finalized, Some(parent));
}

#[test]
fn slot_status_proves_skips_on_sealed_banks_and_stops_at_an_unsealed_one() {
    use crate::head_cache::coverage::SlotStatus::{
        AboveFinalizedTip, Present, ProvenSkipped, Unknown,
    };
    let cache = HeadCache::new(32, 64);
    let session = cache.start_bank_session(CommitmentLevel::Processed);
    finalize_on(&cache, session, 40, 39, 1);
    finalize_on(&cache, session, 42, 40, 1);
    let status = |slots: std::ops::RangeInclusive<u64>| {
        slots
            .map(|slot| cache.slot_status(slot))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        status(39..=43),
        vec![Unknown, Present, ProvenSkipped, Present, AboveFinalizedTip]
    );
    // Slot 43 is a finalized block whose bank never sealed: no link, so no trusted chain.
    finalize_on(&cache, session, 43, 42, 2);
    assert_eq!(status(40..=44), vec![Unknown; 5]);
    finalize_on(&cache, session, 44, 43, 1);
    finalize_on(&cache, session, 46, 44, 1);
    assert_eq!(
        status(40..=47),
        vec![
            Unknown,
            Unknown,
            Unknown,
            Unknown,
            Present,
            ProvenSkipped,
            Present,
            AboveFinalizedTip
        ]
    );
    cache.end_bank_session(session);
    assert_eq!(status(44..=47), vec![Unknown; 4]);
}

type Corruption = dyn Fn(
    &mut BlockMetadataRecord,
    &mut Vec<SubscribeUpdateTransactionInfo>,
    &mut Vec<yellowstone_grpc_proto::prelude::SubscribeUpdateEntry>,
    &mut [u8; 32],
);

#[test]
fn rejected_banks_are_counted_by_the_failed_check() {
    let attempt = |reason: &'static str, corrupt: &Corruption| {
        let cache = HeadCache::new(32, 64);
        let session = cache.start_bank_session(CommitmentLevel::Processed);
        let mut meta = metadata(42, 2);
        let mut txs = vec![transaction(2)];
        let mut entries = vec![yellowstone_grpc_proto::prelude::SubscribeUpdateEntry {
            slot: 42,
            bank_id: 2,
            hash: vec![9; 32],
            executed_transaction_count: 1,
            ..Default::default()
        }];
        let mut hash = [2; 32];
        corrupt(&mut meta, &mut txs, &mut entries, &mut hash);
        let before = crate::metrics::head_cache_rejected_banks_count(reason);
        if reason != "metadata_missing" {
            cache.stage_bank_metadata(session, 2, meta);
        }
        for entry in &entries {
            cache.stage_bank_entry(session, entry);
        }
        cache.freeze_bank(session, 42, 2, hash, txs);
        assert!(
            crate::metrics::head_cache_rejected_banks_count(reason) > before,
            "{reason}"
        );
        assert!(
            cache
                .get_meta(&Signature::from([2; 64]), CommitmentLevel::Processed)
                .is_none(),
            "{reason}"
        );
    };
    attempt("metadata_missing", &|_, _, _, _| {});
    attempt("blockhash_mismatch", &|_, _, _, hash| *hash = [3; 32]);
    attempt("transaction_count", &|meta, _, _, _| {
        meta.executed_transaction_count = 2
    });
    attempt("entry_count", &|meta, _, _, _| meta.entry_count = 2);
    attempt("transaction_index", &|_, txs, _, _| txs[0].index = 1);
    attempt("duplicate_signature", &|meta, txs, entries, _| {
        meta.executed_transaction_count = 2;
        entries[0].executed_transaction_count = 2;
        let mut second = transaction(2);
        second.index = 1;
        txs.push(second);
    });
    attempt("malformed_transaction", &|_, txs, _, _| txs[0].meta = None);
    attempt("entry_sequence", &|_, _, entries, _| entries[0].index = 1);
    attempt("entry_hash", &|_, _, entries, _| {
        entries[0].hash.pop();
    });
    attempt("entry_range", &|_, _, entries, _| {
        entries[0].executed_transaction_count = 2
    });
    attempt("entry_range", &|_, _, entries, _| {
        entries[0].executed_transaction_count = 0
    });
}

#[test]
fn bank_write_lock_hold_is_observed_per_operation() {
    let cache = HeadCache::new(32, 64);
    let session = cache.start_bank_session(CommitmentLevel::Processed);
    freeze(&cache, session, 42, 1, 1);
    cache.commit_bank(session, 42, 1, CommitmentLevel::Confirmed, Some(41));
    cache.discard_bank(session, 42, 1);
    let exported = String::from_utf8(crate::metrics::export_metrics().unwrap()).unwrap();
    for op in [
        "session_start",
        "stage_metadata",
        "stage_entry",
        "freeze",
        "commit",
        "discard",
    ] {
        let series = format!("superbank_head_cache_write_lock_hold_seconds_count{{op=\"{op}\"}}");
        assert!(exported.contains(&series), "{series}");
    }
}

#[tokio::test]
async fn lagging_bank_event_receivers_are_counted() {
    let (sender, mut receiver) = tokio::sync::broadcast::channel(1);
    let event = |slot| HeadBankEvent {
        slot,
        blockhash: [0; 32],
        kind: HeadBankEventKind::Evicted,
    };
    sender.send(event(1)).unwrap();
    sender.send(event(2)).unwrap();
    let before = crate::metrics::head_cache_bank_events_lagged_count();
    assert!(matches!(
        crate::head_cache::recv_bank_event(&mut receiver).await,
        Err(tokio::sync::broadcast::error::RecvError::Lagged(1))
    ));
    assert!(crate::metrics::head_cache_bank_events_lagged_count() > before);
    assert_eq!(
        crate::head_cache::recv_bank_event(&mut receiver)
            .await
            .unwrap(),
        event(2)
    );
}

#[tokio::test]
async fn recent_store_prebuilds_confirmed_blocks_and_drops_discarded_ones() {
    use crate::recent_block_responses::{
        Prebuild, RecentBlockResponses, parse_prebuild_shapes, run,
    };
    let cache = std::sync::Arc::new(HeadCache::new(32, 64));
    let mut state = crate::tests::test_state();
    {
        let state = std::sync::Arc::get_mut(&mut state).unwrap();
        state.head_cache = Some(cache.clone());
        state.recent_block_responses = RecentBlockResponses::new(1 << 20);
    }
    let shapes = parse_prebuild_shapes("maxSupportedTransactionVersion=0")
        .unwrap()
        .0;
    let key = shapes[0].key(42, [2; 32]);
    let task = tokio::spawn(run(
        state.clone(),
        cache.clone(),
        cache.subscribe_bank_events(),
        Prebuild::new(shapes, 1),
    ));
    let entries = || state.recent_block_responses.resident().0;
    async fn wait(done: impl Fn() -> bool) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !done() {
            assert!(std::time::Instant::now() < deadline, "timed out");
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }
    let built = crate::metrics::get_block_recent_store_prebuild_count("built");

    let session = cache.start_bank_session(CommitmentLevel::Processed);
    freeze(&cache, session, 42, 1, 1);
    freeze(&cache, session, 42, 2, 2);
    cache.commit_bank(session, 42, 2, CommitmentLevel::Confirmed, Some(41));
    wait(|| entries() == 1).await;
    assert!(state.recent_block_responses.contains(&key));
    assert!(crate::metrics::get_block_recent_store_prebuild_count("built") > built);

    cache.discard_bank(session, 42, 2);
    wait(|| entries() == 0).await;
    assert!(!state.recent_block_responses.contains(&key));
    task.abort();
}
