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
fn republishing_a_projected_bank_reuses_its_rows() {
    let cache = HeadCache::new(32, 64);
    let session = cache.start_bank_session(CommitmentLevel::Processed);
    freeze(&cache, session, 42, 1, 1);
    let signature = Signature::from([1; 64]);
    let token = cache.banks.read().unwrap().banks[&(42, 1)]
        .commitment
        .clone();
    assert!(cache.keeps_existing_projection(&signature, 42, Some(&token)));
    assert!(!cache.keeps_existing_projection(&Signature::from([2; 64]), 42, Some(&token)));
    let before = cache
        .get_tx(&signature, CommitmentLevel::Processed)
        .expect("projected row");
    // Each commit republishes the bank; a projected signature is neither converted nor replaced.
    let conversions = crate::head_cache::convert::conversions_on_this_thread();
    cache.commit_bank(session, 42, 1, CommitmentLevel::Confirmed, Some(41));
    cache.commit_bank(session, 42, 1, CommitmentLevel::Finalized, Some(41));
    assert_eq!(
        crate::head_cache::convert::conversions_on_this_thread(),
        conversions
    );
    let after = cache
        .get_tx(&signature, CommitmentLevel::Finalized)
        .expect("projected row");
    assert!(Arc::ptr_eq(&before, &after));
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
    let prepared = exported_count("superbank_head_cache_freeze_prepare_seconds");
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
    assert!(exported_count("superbank_head_cache_freeze_prepare_seconds") >= prepared + 1.0);
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

#[test]
fn rejection_label_order_is_kept() {
    const LABELS: [&str; 10] = [
        "metadata_missing",
        "blockhash_mismatch",
        "transaction_count",
        "entry_count",
        "transaction_index",
        "duplicate_signature",
        "malformed_transaction",
        "entry_sequence",
        "entry_hash",
        "entry_range",
    ];
    let counts = || LABELS.map(crate::metrics::head_cache_rejected_banks_count);
    // The counters are process-wide and other tests reject banks too, so retry
    // until one freeze call moves only the expected label by exactly one.
    let case = |expected: &'static str, corrupt: &Corruption| {
        for _ in 0..200 {
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
            cache.stage_bank_metadata(session, 2, meta);
            for entry in &entries {
                cache.stage_bank_entry(session, entry);
            }
            let before = counts();
            cache.freeze_bank(session, 42, 2, hash, txs);
            let after = counts();
            let moved: Vec<u64> = after.iter().zip(&before).map(|(a, b)| a - b).collect();
            assert!(
                cache
                    .get_meta(&Signature::from([2; 64]), CommitmentLevel::Processed)
                    .is_none(),
                "{expected}"
            );
            if moved == LABELS.map(|label| u64::from(label == expected)) {
                return;
            }
        }
        panic!("{expected}: no freeze call moved only that label");
    };
    // Malformed at position 0 wins over a duplicate signature at position 1.
    case("malformed_transaction", &|meta, txs, entries, _| {
        meta.executed_transaction_count = 2;
        entries[0].executed_transaction_count = 2;
        txs[0].meta = None;
        let mut second = transaction(2);
        second.index = 1;
        txs.push(second);
    });
    // A duplicate signature at position 1 wins over a malformed transaction at position 2.
    case("duplicate_signature", &|meta, txs, entries, _| {
        meta.executed_transaction_count = 3;
        entries[0].executed_transaction_count = 3;
        let mut second = transaction(2);
        second.index = 1;
        let mut third = transaction(3);
        third.index = 2;
        third.meta = None;
        txs.extend([second, third]);
    });
    // The index check wins over conversion for the same transaction.
    case("transaction_index", &|_, txs, _, _| {
        txs[0].index = 1;
        txs[0].meta = None;
    });
    // Per-transaction checks run before the entry checks.
    case("malformed_transaction", &|_, txs, entries, _| {
        txs[0].meta = None;
        entries[0].index = 1;
    });
    case("transaction_count", &|meta, txs, _, _| {
        meta.executed_transaction_count = 2;
        txs[0].meta = None;
    });
    case("entry_count", &|meta, txs, _, _| {
        meta.entry_count = 2;
        txs[0].meta = None;
    });
    case("blockhash_mismatch", &|meta, txs, entries, hash| {
        *hash = [3; 32];
        meta.executed_transaction_count = 2;
        meta.entry_count = 2;
        txs[0].meta = None;
        entries[0].index = 1;
        entries[0].hash.pop();
    });
}

#[test]
fn prepared_rows_take_block_time_and_token_at_insert() {
    let restaged = |slot, marker| {
        let mut meta = metadata(slot, marker);
        meta.block_time = Some(99);
        meta
    };

    // The repair path keeps the block time of the first publish and shares the bank token.
    let cache = HeadCache::new(32, 64);
    let session = cache.start_bank_session(CommitmentLevel::Processed);
    let signature = Signature::from([1; 64]);
    freeze(&cache, session, 42, 1, 1);
    cache.stage_bank_metadata(session, 2, metadata(43, 2));
    stage_entry(&cache, session, 43, 2, 1);
    cache.freeze_bank(session, 43, 2, [2; 32], vec![transaction(1)]);
    assert_eq!(cache.signature_position(&signature).unwrap().slot, 42);
    cache.stage_bank_metadata(session, 2, restaged(43, 2));
    cache.discard_bank(session, 42, 1);
    assert!(
        cache
            .get_meta(&signature, CommitmentLevel::Processed)
            .is_none()
    );
    cache.commit_bank(session, 43, 2, CommitmentLevel::Confirmed, Some(42));
    let record = cache
        .get_tx(&signature, CommitmentLevel::Confirmed)
        .unwrap();
    assert_eq!(record.slot, 43);
    assert_eq!(record.block_time, Some(2));
    assert_eq!(cache.block_time_for_slot(43), Some(2));
    let meta = cache
        .get_meta(&signature, CommitmentLevel::Confirmed)
        .unwrap();
    assert_eq!(meta.block_time, Some(2));
    assert_eq!(cache.confirmation_status_string(&meta), "confirmed");
    cache.commit_bank(session, 43, 2, CommitmentLevel::Finalized, Some(42));
    assert_eq!(cache.confirmation_status_string(&meta), "finalized");

    // A Confirmed-minimum session publishes at commit time with the block time staged by then.
    let cache = HeadCache::new(32, 64);
    let session = cache.start_bank_session(CommitmentLevel::Confirmed);
    let signature = Signature::from([2; 64]);
    freeze(&cache, session, 43, 2, 2);
    assert!(
        cache
            .get_tx(&signature, CommitmentLevel::Processed)
            .is_none()
    );
    cache.stage_bank_metadata(session, 2, restaged(43, 2));
    cache.commit_bank(session, 43, 2, CommitmentLevel::Confirmed, Some(42));
    let record = cache
        .get_tx(&signature, CommitmentLevel::Confirmed)
        .unwrap();
    assert_eq!(record.block_time, Some(99));
    let meta = cache
        .get_meta(&signature, CommitmentLevel::Confirmed)
        .unwrap();
    assert_eq!(meta.block_time, Some(99));
    assert_eq!(cache.block_time_for_slot(43), Some(99));
}

#[test]
fn frozen_bank_rows_keep_index_order_err_memo_addresses_and_missing_block_time() {
    let cache = HeadCache::new(32, 64);
    let session = cache.start_bank_session(CommitmentLevel::Processed);
    let slot = 100;
    let key = |marker: u8| vec![marker; 32];
    let memo_program = bs58::decode("Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo")
        .into_vec()
        .unwrap();
    let info = |marker: u8, index: u64, keys: Vec<Vec<u8>>, meta: TransactionStatusMeta| {
        let mut info = transaction(marker);
        info.index = index;
        let message = info.transaction.as_mut().unwrap().message.as_mut().unwrap();
        message.account_keys = keys;
        info.meta = Some(meta);
        info
    };
    // Signature order (0xA1 < 0xB0) is the reverse of block index order.
    let loaded = info(
        0xA1,
        1,
        vec![key(0x51)],
        TransactionStatusMeta {
            loaded_writable_addresses: vec![key(0x51), key(0x52)],
            loaded_readonly_addresses: vec![key(0x53)],
            ..Default::default()
        },
    );
    let mut failed = info(
        0xB0,
        0,
        vec![key(0x61), memo_program],
        TransactionStatusMeta {
            err: Some(yellowstone_grpc_proto::prelude::TransactionError {
                err: bincode::serialize(
                    &crate::solana_sdk::transaction::TransactionError::AccountNotFound,
                )
                .unwrap(),
            }),
            ..Default::default()
        },
    );
    failed
        .transaction
        .as_mut()
        .unwrap()
        .message
        .as_mut()
        .unwrap()
        .instructions = vec![yellowstone_grpc_proto::prelude::CompiledInstruction {
        program_id_index: 1,
        accounts: vec![0],
        data: b"golden memo".to_vec(),
    }];
    let transactions = vec![loaded, failed];
    let converted = |tx: &SubscribeUpdateTransactionInfo| {
        crate::head_cache::convert::stored_record_from_transaction_info(slot, tx).unwrap()
    };
    let failed_record = converted(&transactions[1]);
    let expected_err = failed_record
        .meta_err
        .as_ref()
        .map(|raw| serde_json::from_str::<serde_json::Value>(raw.trim()).unwrap());
    let expected_memo = crate::clickhouse::extract_memo(&failed_record);
    assert!(expected_err.is_some() && expected_memo.is_some());

    let no_block_time = parse_block_meta(&SubscribeUpdateBlockMeta {
        slot,
        blockhash: bs58::encode([7; 32]).into_string(),
        parent_slot: slot - 1,
        parent_blockhash: bs58::encode([0; 32]).into_string(),
        executed_transaction_count: 2,
        entries_count: 1,
        block_time: None,
        ..Default::default()
    })
    .unwrap();
    cache.stage_bank_metadata(session, 1, no_block_time);
    stage_entry(&cache, session, slot, 1, 2);
    cache.freeze_bank(session, slot, 1, [7; 32], transactions);

    let (loaded_sig, failed_sig) = (Signature::from([0xA1; 64]), Signature::from([0xB0; 64]));
    let signatures = match cache.get_block(
        slot,
        CommitmentLevel::Processed,
        solana_transaction_status::TransactionDetails::Signatures,
    ) {
        Some(crate::clickhouse::StoredBlockPayload::Signatures { signatures, .. }) => signatures,
        other => panic!("unexpected block payload: {}", other.is_some()),
    };
    assert_eq!(
        signatures,
        vec![failed_sig.to_string(), loaded_sig.to_string()]
    );

    let loaded_meta = cache
        .get_meta(&loaded_sig, CommitmentLevel::Processed)
        .unwrap();
    let failed_meta = cache
        .get_meta(&failed_sig, CommitmentLevel::Processed)
        .unwrap();
    assert_eq!((loaded_meta.pos.slot, loaded_meta.pos.idx), (slot, 1));
    assert_eq!((failed_meta.pos.slot, failed_meta.pos.idx), (slot, 0));
    assert_eq!(
        &*loaded_meta.signature_str,
        bs58::encode([0xA1; 64]).into_string()
    );
    assert_eq!(
        &*failed_meta.signature_str,
        bs58::encode([0xB0; 64]).into_string()
    );
    assert_eq!(failed_meta.err, expected_err);
    assert_eq!(failed_meta.memo, expected_memo);
    assert!(loaded_meta.err.is_none() && loaded_meta.memo.is_none());

    // The key that is both static and loaded writable appears once.
    for (marker, owner) in [(0x51, 0xA1), (0x52, 0xA1), (0x53, 0xA1), (0x61, 0xB0)] {
        let listed = cache.signatures_for_address(
            &Pubkey::from([marker; 32]),
            None,
            None,
            10,
            CommitmentLevel::Processed,
        );
        assert_eq!(listed.len(), 1, "address {marker:#x}");
        assert_eq!(
            &*listed[0].signature_str,
            bs58::encode([owner; 64]).into_string()
        );
    }

    assert_eq!(loaded_meta.block_time, None);
    assert_eq!(failed_meta.block_time, None);
    assert_eq!(cache.block_time_for_slot(slot), None);
    for signature in [&loaded_sig, &failed_sig] {
        let record = cache.get_tx(signature, CommitmentLevel::Processed).unwrap();
        assert_eq!(record.block_time, None);
        assert_eq!(record.slot, slot);
    }
}

/// Stages bank `bank` of `slot` on `parent` for `count` transactions; its blockhash is `[marker; 32]`.
fn stage_block(
    cache: &HeadCache,
    session: u64,
    slot: u64,
    bank: u64,
    marker: u8,
    parent: u64,
    count: u8,
) {
    let mut meta = metadata(slot, marker);
    meta.parent_slot = parent;
    meta.parent_blockhash = [parent as u8; 32];
    meta.executed_transaction_count = u64::from(count);
    cache.stage_bank_metadata(session, bank, meta);
    stage_entry(cache, session, slot, bank, u64::from(count));
}

/// Transactions whose signatures are `[first; 64]`, `[first + 1; 64]` and so on.
fn transactions_from(first: u8, count: u8) -> Vec<SubscribeUpdateTransactionInfo> {
    (0..count)
        .map(|index| {
            let mut tx = transaction(first + index);
            tx.index = u64::from(index);
            tx
        })
        .collect()
}

fn signatures_from(first: u8, count: u8) -> Vec<Signature> {
    (first..first + count)
        .map(|marker| Signature::from([marker; 64]))
        .collect()
}

fn addresses_from(first: u8, count: u8) -> Vec<Pubkey> {
    (first..first + count)
        .map(|marker| Pubkey::from([marker; 32]))
        .collect()
}

fn freeze_block(cache: &HeadCache, session: u64, slot: u64, bank: u64, marker: u8, first: u8) {
    cache.freeze_bank(
        session,
        slot,
        bank,
        [marker; 32],
        transactions_from(first, 3),
    );
}

/// Value of the `_count` series of the label-less histogram `family` in the exported metrics.
pub(crate) fn exported_count(family: &str) -> f64 {
    let text = String::from_utf8(crate::metrics::export_metrics().unwrap()).unwrap();
    let name = format!("{family}_count ");
    text.lines()
        .find_map(|line| line.strip_prefix(name.as_str())?.trim().parse().ok())
        .unwrap_or_else(|| panic!("{family} is exported"))
}

fn conversions() -> u64 {
    crate::head_cache::convert::conversions_on_this_thread()
}

/// Runs `freeze` on its own thread and holds it at `point` while `during` runs on this one.
/// A panic in `during` drops the release channel, which makes the held thread panic too.
fn freeze_paused<T: Send, R>(
    cache: &HeadCache,
    point: crate::head_cache::PausePoint,
    freeze: impl FnOnce() -> T + Send,
    during: impl FnOnce() -> R,
) -> (T, R) {
    let (frozen, observed, _) = freeze_paused_counting(cache, point, freeze, during);
    (frozen, observed)
}

/// Like [`freeze_paused`], and also returns the conversions the held thread had started.
fn freeze_paused_counting<T: Send, R>(
    cache: &HeadCache,
    point: crate::head_cache::PausePoint,
    freeze: impl FnOnce() -> T + Send,
    during: impl FnOnce() -> R,
) -> (T, R, u64) {
    let (reached, resume) = cache.arm_pause(point);
    std::thread::scope(|scope| {
        let freezer = scope.spawn(freeze);
        let converted = reached
            .recv_timeout(std::time::Duration::from_secs(60))
            .expect("the freeze reaches its pause point");
        let resume = resume;
        let observed = during();
        resume.send(()).unwrap();
        (freezer.join().unwrap(), observed, converted)
    })
}

/// Whether the coverage lock alone lists `slot` as a block of the chain at `commitment`.
fn claims(cache: &HeadCache, slot: u64, commitment: CommitmentLevel) -> bool {
    cache
        .coverage
        .read()
        .unwrap()
        .snapshot(slot, Some(slot), commitment, Instant::now())
        .is_ok_and(|(_, coverage)| coverage.slots.contains(&slot))
}

fn block_signature_count(cache: &HeadCache, slot: u64, level: CommitmentLevel) -> Option<usize> {
    let block = cache.get_block(
        slot,
        level,
        solana_transaction_status::TransactionDetails::Signatures,
    )?;
    let crate::clickhouse::StoredBlockPayload::Signatures { signatures, .. } = block else {
        panic!("signature payload");
    };
    Some(signatures.len())
}

/// No reader and no index holds a row of `slot` for these signatures and addresses.
fn assert_no_rows(cache: &HeadCache, slot: u64, signatures: &[Signature], addresses: &[Pubkey]) {
    for signature in signatures {
        for level in [
            CommitmentLevel::Processed,
            CommitmentLevel::Confirmed,
            CommitmentLevel::Finalized,
        ] {
            assert!(cache.get_tx(signature, level).is_none());
            assert!(cache.get_meta(signature, level).is_none());
        }
        assert!(!cache.tx_by_signature.contains_key(signature));
        assert!(!cache.meta_by_signature.contains_key(signature));
        assert!(
            cache
                .sigs_by_slot
                .get(&slot)
                .is_none_or(|held| !held.contains(signature))
        );
    }
    for address in addresses {
        assert!(
            cache
                .addrs_by_slot
                .get(&slot)
                .is_none_or(|held| !held.contains(address))
        );
        assert!(!cache.sigs_by_address.contains_key(address));
    }
}

#[test]
fn prepare_runs_outside_the_bank_lock() {
    let cache = HeadCache::new(32, 64);
    let session = cache.start_bank_session(CommitmentLevel::Processed);
    finalize_on(&cache, session, 40, 39, 1);
    finalize_on(&cache, session, 41, 40, 1);
    stage_block(&cache, session, 42, 1, 42, 41, 3);
    cache.commit_bank(session, 42, 1, CommitmentLevel::Processed, Some(41));
    let published = Signature::from([41; 64]);
    let (converted, (), converted_at_pause) = freeze_paused_counting(
        &cache,
        crate::head_cache::PausePoint::Prepared,
        || converted_by(|| freeze_block(&cache, session, 42, 1, 42, 1)),
        || {
            // These readers would block for the whole conversion if it held the bank lock.
            assert!(cache.banks.try_read().is_ok());
            assert_eq!(
                cache
                    .get_tx(&published, CommitmentLevel::Finalized)
                    .map(|record| record.slot),
                Some(41)
            );
            assert_eq!(
                block_signature_count(&cache, 41, CommitmentLevel::Finalized),
                Some(1)
            );
            for signature in signatures_from(1, 3) {
                assert!(
                    cache
                        .get_tx(&signature, CommitmentLevel::Processed)
                        .is_none()
                );
            }
            assert_eq!(
                block_signature_count(&cache, 42, CommitmentLevel::Processed),
                None
            );
            assert_eq!(cache.latest_slot(), 41);
            assert!(!claims(&cache, 42, CommitmentLevel::Processed));
            assert_ne!(
                cache.slot_status(42),
                crate::head_cache::coverage::SlotStatus::Present
            );
        },
    );
    assert_eq!(converted_at_pause, 3, "every conversion precedes the lock");
    assert_eq!(converted, 3, "and none repeats under it");
    assert_eq!(
        block_signature_count(&cache, 42, CommitmentLevel::Processed),
        Some(3)
    );
    assert!(claims(&cache, 42, CommitmentLevel::Processed));
    cache.commit_bank(session, 42, 1, CommitmentLevel::Finalized, Some(41));
    assert_eq!(
        cache.slot_status(42),
        crate::head_cache::coverage::SlotStatus::Present
    );
}

#[test]
fn coverage_proof_is_published_after_the_rows() {
    use crate::head_cache::{PausePoint::RowsInserted, coverage::SlotStatus};
    let held_rows = |cache: &HeadCache| {
        assert!(matches!(
            cache.banks.try_read(),
            Err(std::sync::TryLockError::WouldBlock)
        ));
        for signature in signatures_from(1, 3) {
            assert!(cache.tx_by_signature.contains_key(&signature));
            assert!(cache.meta_by_signature.contains_key(&signature));
        }
        assert_eq!(cache.sigs_by_slot.get(&42).unwrap().len(), 3);
        assert!(cache.slot_commitment.contains_key(&42));
    };

    // Processed session: the freeze publishes. Tip 42 is observed first, so a proof that
    // is visible too early shows up in the coverage read below.
    let cache = HeadCache::new(32, 64);
    let session = cache.start_bank_session(CommitmentLevel::Processed);
    finalize_on(&cache, session, 40, 39, 1);
    finalize_on(&cache, session, 41, 40, 1);
    stage_block(&cache, session, 42, 1, 42, 41, 3);
    cache.commit_bank(session, 42, 1, CommitmentLevel::Processed, Some(41));
    freeze_paused(
        &cache,
        RowsInserted,
        || freeze_block(&cache, session, 42, 1, 42, 1),
        || {
            held_rows(&cache);
            assert!(!claims(&cache, 42, CommitmentLevel::Processed));
            assert_ne!(cache.slot_status(42), SlotStatus::Present);
        },
    );
    assert!(claims(&cache, 42, CommitmentLevel::Processed));
    cache.commit_bank(session, 42, 1, CommitmentLevel::Finalized, Some(41));
    assert_eq!(cache.slot_status(42), SlotStatus::Present);
    assert_eq!(
        block_signature_count(&cache, 42, CommitmentLevel::Finalized),
        Some(3)
    );

    // Confirmed-minimum session: the first publish runs under `commit_bank`.
    let cache = HeadCache::new(32, 64);
    let session = cache.start_bank_session(CommitmentLevel::Confirmed);
    finalize_on(&cache, session, 40, 39, 1);
    finalize_on(&cache, session, 41, 40, 1);
    stage_block(&cache, session, 42, 1, 42, 41, 3);
    freeze_block(&cache, session, 42, 1, 42, 1);
    assert!(
        !cache
            .tx_by_signature
            .contains_key(&Signature::from([1; 64]))
    );
    freeze_paused(
        &cache,
        RowsInserted,
        || cache.commit_bank(session, 42, 1, CommitmentLevel::Confirmed, Some(41)),
        || {
            held_rows(&cache);
            assert!(!claims(&cache, 42, CommitmentLevel::Confirmed));
            assert_ne!(cache.slot_status(42), SlotStatus::Present);
        },
    );
    assert!(claims(&cache, 42, CommitmentLevel::Confirmed));
    assert_eq!(
        block_signature_count(&cache, 42, CommitmentLevel::Confirmed),
        Some(3)
    );
}

#[test]
fn coverage_readers_never_see_a_slot_without_its_rows() {
    use std::sync::atomic::AtomicBool;
    const FIRST: u64 = 100;
    const LAST: u64 = 160;
    const PER_SLOT: u8 = 8;
    let bytes = |slot: u64, index: u8| {
        let mut bytes = [0x55; 64];
        bytes[0] = slot as u8;
        bytes[1] = index;
        bytes
    };
    let block = |slot: u64| {
        (0..PER_SLOT)
            .map(|index| {
                let mut tx = transaction(1);
                tx.index = u64::from(index);
                tx.signature = bytes(slot, index).to_vec();
                tx.transaction.as_mut().unwrap().signatures = vec![tx.signature.clone()];
                tx
            })
            .collect::<Vec<_>>()
    };
    // Reads the proof first, then requires the rows of every slot it lists.
    let verify = |cache: &HeadCache| -> usize {
        let Ok((_, coverage)) = cache.coverage.read().unwrap().snapshot(
            0,
            Some(LAST),
            CommitmentLevel::Processed,
            Instant::now(),
        ) else {
            return 0;
        };
        for &slot in &coverage.slots {
            let indexed = (0..PER_SLOT)
                .filter(|&index| {
                    cache
                        .tx_by_signature
                        .contains_key(&Signature::from(bytes(slot, index)))
                })
                .count();
            let served = block_signature_count(cache, slot, CommitmentLevel::Processed);
            // A slot may leave the retention window after the proof was read.
            let evicted = slot < cache.min_retained_slot();
            assert!(
                evicted || (indexed == usize::from(PER_SLOT) && served == Some(PER_SLOT.into())),
                "slot {slot}: {indexed} indexed rows, served {served:?}"
            );
        }
        coverage.slots.len()
    };

    let cache = HeadCache::new(32, 64);
    let session = cache.start_bank_session(CommitmentLevel::Processed);
    let finished = AtomicBool::new(false);
    let verified = std::sync::atomic::AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for _ in 0..4 {
            scope.spawn(|| {
                loop {
                    let last_pass = finished.load(Ordering::Acquire);
                    verified.fetch_add(verify(&cache), Ordering::Relaxed);
                    if last_pass {
                        break;
                    }
                }
            });
        }
        for slot in FIRST..=LAST {
            stage_block(&cache, session, slot, 1, slot as u8, slot - 1, PER_SLOT);
            cache.freeze_bank(session, slot, 1, [slot as u8; 32], block(slot));
            cache.commit_bank(session, slot, 1, CommitmentLevel::Processed, Some(slot - 1));
        }
        finished.store(true, Ordering::Release);
    });
    // Each reader makes one pass after the last commit, so the check is never vacuous.
    assert!(verified.load(Ordering::Relaxed) > 0);
}

/// Freezes bank 1 of slot 42 (signatures `[1..=3; 64]`), paused after its prepare step, while `during` runs.
/// Tip 42 is observed first, so a bank published anyway shows in the coverage reads.
fn freeze_interleaved_with(during: impl FnOnce(&HeadCache, u64)) -> (HeadCache, u64) {
    let cache = HeadCache::new(32, 64);
    let session = cache.start_bank_session(CommitmentLevel::Processed);
    stage_block(&cache, session, 42, 1, 1, 41, 3);
    cache.commit_bank(session, 42, 1, CommitmentLevel::Processed, Some(41));
    freeze_paused(
        &cache,
        crate::head_cache::PausePoint::Prepared,
        || freeze_block(&cache, session, 42, 1, 1, 1),
        || during(&cache, session),
    );
    (cache, session)
}

/// Bank 1 of slot 42 left no row, no selection and no proof entry.
fn assert_bank_one_absent(cache: &HeadCache) {
    assert_no_rows(cache, 42, &signatures_from(1, 3), &addresses_from(1, 3));
    assert_ne!(cache.banks.read().unwrap().selected.get(&42), Some(&1));
}

#[test]
fn discarding_a_bank_during_its_prepare_leaves_no_rows() {
    let (cache, _) = freeze_interleaved_with(|cache, session| cache.discard_bank(session, 42, 1));
    assert_bank_one_absent(&cache);
    assert!(!claims(&cache, 42, CommitmentLevel::Processed));
    assert_eq!(cache.latest_slot(), 0);
}

#[test]
fn ending_the_session_during_a_prepare_leaves_no_rows() {
    let (cache, _) = freeze_interleaved_with(|cache, session| cache.end_bank_session(session));
    assert_bank_one_absent(&cache);
    assert!(!claims(&cache, 42, CommitmentLevel::Processed));
    assert_eq!(cache.latest_slot(), 0);
}

#[test]
fn a_sibling_becoming_canonical_during_a_prepare_keeps_only_its_own_rows() {
    let (cache, _) = freeze_interleaved_with(|cache, session| {
        stage_block(cache, session, 42, 2, 2, 41, 3);
        freeze_block(cache, session, 42, 2, 2, 11);
        cache.commit_bank(session, 42, 2, CommitmentLevel::Confirmed, Some(41));
    });
    assert_bank_one_absent(&cache);
    for signature in signatures_from(11, 3) {
        assert!(
            cache
                .get_tx(&signature, CommitmentLevel::Confirmed)
                .is_some()
        );
    }
    assert_eq!(
        block_signature_count(&cache, 42, CommitmentLevel::Confirmed),
        Some(3)
    );
    let block = cache
        .get_block(
            42,
            CommitmentLevel::Confirmed,
            solana_transaction_status::TransactionDetails::None,
        )
        .unwrap();
    assert_eq!(block.metadata().blockhash, [2; 32]);
    assert_eq!(cache.banks.read().unwrap().selected.get(&42), Some(&2));
    assert!(claims(&cache, 42, CommitmentLevel::Confirmed));
}

/// Bank 1 of slot 42 staged for 3 transactions in a fresh Processed session.
fn staged_bank_one() -> (HeadCache, u64) {
    let cache = HeadCache::new(32, 64);
    let session = cache.start_bank_session(CommitmentLevel::Processed);
    stage_block(&cache, session, 42, 1, 1, 41, 3);
    (cache, session)
}

/// Freezes bank 1 of slot 42 paused after its prepare step. Returns the rejection reasons the
/// freezing thread counted.
fn freeze_bank_one_during(
    cache: &HeadCache,
    session: u64,
    during: impl FnOnce(),
) -> Vec<&'static str> {
    let (rejected, ()) = freeze_paused(
        cache,
        crate::head_cache::PausePoint::Prepared,
        || {
            freeze_block(cache, session, 42, 1, 1, 1);
            crate::metrics::head_cache_rejected_banks_on_this_thread()
        },
        during,
    );
    rejected
}

#[test]
fn a_bank_restaged_in_a_new_session_during_a_prepare_is_not_sealed_by_the_old_freeze() {
    let (cache, session) = staged_bank_one();
    let rejected = freeze_bank_one_during(&cache, session, || {
        cache.end_bank_session(session);
        let next = cache.start_bank_session(CommitmentLevel::Processed);
        stage_block(&cache, next, 42, 1, 1, 41, 3);
    });
    assert!(rejected.is_empty());
    assert_bank_one_absent(&cache);
    assert!(cache.banks.read().unwrap().selected.is_empty());
    assert!(!claims(&cache, 42, CommitmentLevel::Processed));
    assert!(!cache.slot_commitment.contains_key(&42));
    let state = cache.banks.read().unwrap();
    let bank = &state.banks[&(42, 1)];
    assert!(bank.sealed_hash.is_none());
    assert!(bank.transactions.is_none());
}

#[test]
fn a_bank_restaged_with_another_blockhash_during_a_prepare_is_rejected() {
    let (cache, session) = staged_bank_one();
    let rejected = freeze_bank_one_during(&cache, session, || {
        let mut meta = metadata(42, 5);
        meta.parent_slot = 41;
        meta.executed_transaction_count = 3;
        cache.stage_bank_metadata(session, 1, meta);
    });
    assert_eq!(rejected, ["blockhash_mismatch"]);
    assert_bank_one_absent(&cache);
    assert!(!claims(&cache, 42, CommitmentLevel::Processed));
    let state = cache.banks.read().unwrap();
    let bank = &state.banks[&(42, 1)];
    assert!(bank.sealed_hash.is_none());
    assert!(bank.transactions.is_none());
}

#[test]
fn discarded_and_losing_banks_leave_no_rows_behind() {
    let cache = HeadCache::new(32, 64);
    let session = cache.start_bank_session(CommitmentLevel::Processed);
    stage_block(&cache, session, 42, 1, 1, 41, 3);
    freeze_block(&cache, session, 42, 1, 1, 1);
    assert_eq!(
        block_signature_count(&cache, 42, CommitmentLevel::Processed),
        Some(3)
    );
    cache.discard_bank(session, 42, 1);
    assert_no_rows(&cache, 42, &signatures_from(1, 3), &addresses_from(1, 3));
    assert!(!cache.sigs_by_slot.contains_key(&42));
    assert!(!cache.addrs_by_slot.contains_key(&42));

    // A fork loser that was published first, then replaced by the canonical bank.
    stage_block(&cache, session, 43, 2, 2, 42, 3);
    freeze_block(&cache, session, 43, 2, 2, 11);
    stage_block(&cache, session, 43, 3, 3, 42, 3);
    freeze_block(&cache, session, 43, 3, 3, 21);
    cache.commit_bank(session, 43, 3, CommitmentLevel::Confirmed, Some(42));
    assert_no_rows(&cache, 43, &signatures_from(11, 3), &addresses_from(11, 3));
    assert_eq!(cache.sigs_by_slot.get(&43).unwrap().len(), 3);
    assert_eq!(cache.addrs_by_slot.get(&43).unwrap().len(), 3);
    for signature in signatures_from(21, 3) {
        assert!(
            cache
                .get_tx(&signature, CommitmentLevel::Confirmed)
                .is_some()
        );
    }
}

#[test]
fn each_transaction_is_converted_once() {
    let confirmed = Signature::from([1; 64]);

    // Processed session: the freeze converts and no commit converts again.
    let cache = HeadCache::new(32, 64);
    let session = cache.start_bank_session(CommitmentLevel::Processed);
    stage_block(&cache, session, 42, 1, 1, 41, 3);
    let start = conversions();
    freeze_block(&cache, session, 42, 1, 1, 1);
    assert_eq!(conversions() - start, 3);
    for level in [
        CommitmentLevel::Processed,
        CommitmentLevel::Confirmed,
        CommitmentLevel::Finalized,
    ] {
        cache.commit_bank(session, 42, 1, level, Some(41));
    }
    assert_eq!(conversions() - start, 3);
    assert!(
        cache
            .get_tx(&confirmed, CommitmentLevel::Finalized)
            .is_some()
    );

    // Confirmed-minimum session: the commit that first publishes only inserts.
    let cache = HeadCache::new(32, 64);
    let session = cache.start_bank_session(CommitmentLevel::Confirmed);
    stage_block(&cache, session, 42, 1, 1, 41, 3);
    let start = conversions();
    freeze_block(&cache, session, 42, 1, 1, 1);
    assert_eq!(conversions() - start, 3);
    assert!(!cache.tx_by_signature.contains_key(&confirmed));
    cache.commit_bank(session, 42, 1, CommitmentLevel::Confirmed, Some(41));
    assert_eq!(conversions() - start, 3);
    assert_eq!(
        block_signature_count(&cache, 42, CommitmentLevel::Confirmed),
        Some(3)
    );

    // A signature first seen on an abandoned slot is reclaimed from the stored rows.
    let cache = HeadCache::new(32, 64);
    let session = cache.start_bank_session(CommitmentLevel::Processed);
    stage_block(&cache, session, 42, 1, 1, 41, 3);
    freeze_block(&cache, session, 42, 1, 1, 1);
    stage_block(&cache, session, 43, 2, 2, 42, 3);
    freeze_block(&cache, session, 43, 2, 2, 1);
    assert_eq!(cache.signature_position(&confirmed).unwrap().slot, 42);
    let start = conversions();
    cache.discard_bank(session, 42, 1);
    cache.commit_bank(session, 43, 2, CommitmentLevel::Confirmed, Some(42));
    assert_eq!(conversions() - start, 0);
    assert_eq!(
        cache
            .get_tx(&confirmed, CommitmentLevel::Confirmed)
            .map(|record| record.slot),
        Some(43)
    );
}

/// Conversions started on this thread while `freeze` runs.
fn converted_by(freeze: impl FnOnce()) -> u64 {
    let start = conversions();
    freeze();
    conversions() - start
}

#[test]
fn rejected_or_ignored_freeze_converts_nothing() {
    let rejected = crate::metrics::head_cache_rejected_banks_count;
    let first = Signature::from([1; 64]);
    let fresh = || {
        let cache = HeadCache::new(32, 64);
        let session = cache.start_bank_session(CommitmentLevel::Processed);
        (cache, session)
    };

    // A check that precedes the per-transaction ones rejects the bank.
    for label in [
        "metadata_missing",
        "blockhash_mismatch",
        "transaction_count",
        "entry_count",
    ] {
        let (cache, session) = fresh();
        if label != "metadata_missing" {
            stage_block(&cache, session, 42, 1, 1, 41, 3);
        }
        let before = rejected(label);
        let converted = converted_by(|| match label {
            "blockhash_mismatch" => freeze_block(&cache, session, 42, 1, 3, 1),
            "transaction_count" => {
                cache.freeze_bank(session, 42, 1, [1; 32], transactions_from(1, 2))
            }
            "entry_count" => {
                stage_entry(&cache, session, 42, 1, 0);
                freeze_block(&cache, session, 42, 1, 1, 1)
            }
            _ => freeze_block(&cache, session, 42, 1, 1, 1),
        });
        assert_eq!(converted, 0, "{label}");
        assert!(rejected(label) > before, "{label}");
        assert!(!cache.tx_by_signature.contains_key(&first), "{label}");
    }

    // Silent skips.
    let (cache, session) = fresh();
    stage_block(&cache, session, 42, 1, 1, 41, 3);
    freeze_block(&cache, session, 42, 1, 1, 1);
    let converted = converted_by(|| freeze_block(&cache, session, 42, 1, 1, 1));
    assert_eq!(converted, 0, "sealed");
    assert!(cache.tx_by_signature.contains_key(&first));

    let (cache, session) = fresh();
    stage_block(&cache, session, 42, 1, 1, 41, 3);
    cache.discard_bank(session, 42, 1);
    let converted = converted_by(|| freeze_block(&cache, session, 42, 1, 1, 1));
    assert_eq!(converted, 0, "discarded");
    assert!(!cache.tx_by_signature.contains_key(&first));

    let (cache, session) = fresh();
    stage_block(&cache, session, 42, 1, 1, 41, 3);
    stage_block(&cache, session, 42, 2, 2, 41, 3);
    cache.commit_bank(session, 42, 2, CommitmentLevel::Confirmed, Some(41));
    let converted = converted_by(|| freeze_block(&cache, session, 42, 1, 1, 1));
    assert_eq!(converted, 0, "not canonical");
    assert!(!cache.tx_by_signature.contains_key(&first));

    let (cache, session) = fresh();
    stage_block(&cache, session, 42, 1, 1, 41, 3);
    stage_block(&cache, session, 100, 1, 100, 99, 3);
    let converted = converted_by(|| freeze_block(&cache, session, 42, 1, 1, 1));
    assert_eq!(converted, 0, "below the bank floor");
    assert!(!cache.tx_by_signature.contains_key(&first));

    let (cache, session) = fresh();
    stage_block(&cache, session, 42, 1, 1, 41, 3);
    cache.start_bank_session(CommitmentLevel::Processed);
    let converted = converted_by(|| freeze_block(&cache, session, 42, 1, 1, 1));
    assert_eq!(converted, 0, "stale session");
    assert!(!cache.tx_by_signature.contains_key(&first));
}

#[test]
fn stale_peek_still_seals_the_bank() {
    let cache = HeadCache::new(32, 64);
    let session = cache.start_bank_session(CommitmentLevel::Processed);
    // The peek finds no metadata, so nothing is prepared; the hold prepares inline.
    let (converted, (), converted_at_pause) = freeze_paused_counting(
        &cache,
        crate::head_cache::PausePoint::Prepared,
        || converted_by(|| freeze_block(&cache, session, 42, 1, 1, 1)),
        || stage_block(&cache, session, 42, 1, 1, 41, 3),
    );
    assert_eq!(converted_at_pause, 0);
    assert_eq!(converted, 3);
    assert_eq!(
        block_signature_count(&cache, 42, CommitmentLevel::Processed),
        Some(3)
    );
}

#[test]
fn evicted_records_are_freed_after_the_hold() {
    use crate::head_cache::PausePoint::RowsInserted;
    let cache = HeadCache::new(4, 64);
    let session = cache.start_bank_session(CommitmentLevel::Processed);
    for slot in 100..104u64 {
        let first = 1 + 3 * (slot - 100) as u8;
        stage_block(&cache, session, slot, 1, slot as u8, slot - 1, 3);
        freeze_block(&cache, session, slot, 1, slot as u8, first);
    }
    let evicted = Signature::from([1; 64]);
    let record = cache.tx_by_signature.get(&evicted).unwrap().clone();
    let weak = Arc::downgrade(&record);
    drop(record);
    assert!(weak.upgrade().is_some());

    stage_block(&cache, session, 104, 1, 104, 103, 3);
    freeze_paused(
        &cache,
        RowsInserted,
        || freeze_block(&cache, session, 104, 1, 104, 13),
        || {
            assert!(matches!(
                cache.banks.try_read(),
                Err(std::sync::TryLockError::WouldBlock)
            ));
            assert!(!cache.tx_by_signature.contains_key(&evicted));
            assert!(!cache.meta_by_signature.contains_key(&evicted));
            assert!(weak.upgrade().is_some(), "the free waits for the release");
        },
    );
    assert!(weak.upgrade().is_none());
    assert_eq!(cache.retired_len(), 0);
}

#[test]
fn retired_queue_is_empty_after_each_draining_bank_operation() {
    let cache = HeadCache::new(4, 64);
    let session = cache.start_bank_session(CommitmentLevel::Processed);
    assert_eq!(cache.retired_len(), 0);

    // Freeze with eviction.
    for slot in 100..106u64 {
        let first = 1 + 3 * (slot - 100) as u8;
        stage_block(&cache, session, slot, 1, slot as u8, slot - 1, 3);
        freeze_block(&cache, session, slot, 1, slot as u8, first);
        assert_eq!(cache.retired_len(), 0, "freeze of {slot}");
    }

    // Freeze rejected after the per-transaction checks passed.
    let mut meta = metadata(106, 106);
    meta.executed_transaction_count = 3;
    cache.stage_bank_metadata(session, 1, meta);
    stage_entry(&cache, session, 106, 1, 2);
    freeze_block(&cache, session, 106, 1, 106, 19);
    assert_eq!(cache.retired_len(), 0, "rejected freeze");
    assert!(
        !cache
            .tx_by_signature
            .contains_key(&Signature::from([19; 64]))
    );

    // Commit that removes a losing sibling and the slot it published.
    stage_block(&cache, session, 105, 2, 55, 104, 3);
    cache.commit_bank(session, 105, 2, CommitmentLevel::Confirmed, Some(104));
    assert_eq!(cache.retired_len(), 0, "commit");
    assert!(
        !cache
            .tx_by_signature
            .contains_key(&Signature::from([16; 64]))
    );

    // Discard of a published bank.
    cache.discard_bank(session, 104, 1);
    assert_eq!(cache.retired_len(), 0, "discard");
    assert!(
        !cache
            .tx_by_signature
            .contains_key(&Signature::from([13; 64]))
    );

    // A removal outside a hold stays queued through a staging hold until the next release.
    cache.remove_slot(103);
    assert_eq!(cache.retired_len(), 1);
    stage_entry(&cache, session, 107, 1, 1);
    assert_eq!(cache.retired_len(), 1, "staging hold");
    cache.discard_bank(session, 107, 1);
    assert_eq!(cache.retired_len(), 0, "next release");

    // End of the session.
    cache.end_bank_session(session);
    assert_eq!(cache.retired_len(), 0, "end_bank_session");
    assert_eq!(cache.tx_entries(), 0);
}

#[test]
fn staging_holds_leave_retired_banks_for_the_next_freeze_or_commit() {
    type NextHold = fn(&HeadCache, u64);
    let freeze: NextHold = |cache, session| freeze_block(cache, session, 120, 1, 1, 7);
    let commit: NextHold = |cache, session| {
        cache.commit_bank(session, 120, 1, CommitmentLevel::Confirmed, Some(119));
    };
    for next_hold in [freeze, commit] {
        let (cache, session, record) = unpublished_bank();
        // Staging slot 120 trims the bank of slot 42 below the floor.
        stage_block(&cache, session, 120, 1, 1, 119, 3);
        assert_eq!(cache.retired_len(), 1);
        assert!(record.upgrade().is_some());
        assert!(!cache.banks.read().unwrap().banks.contains_key(&(42, 1)));
        next_hold(&cache, session);
        assert!(record.upgrade().is_none());
        assert_eq!(cache.retired_len(), 0);
    }
}

#[test]
fn a_staging_hold_frees_a_retired_backlog() {
    let cache = HeadCache::new(4, 64);
    let session = cache.start_bank_session(CommitmentLevel::Processed);
    for _ in 0..crate::head_cache::RETIRED_BACKLOG {
        cache.retire(());
    }
    stage_block(&cache, session, 42, 1, 1, 41, 3);
    assert_eq!(cache.retired_len(), crate::head_cache::RETIRED_BACKLOG);
    cache.retire(());
    stage_block(&cache, session, 43, 1, 1, 42, 3);
    assert_eq!(cache.retired_len(), 0);
}

/// A Confirmed-minimum session holding bank 1 of slot 42, frozen but unpublished, and a weak
/// handle to a record that only that bank owns.
fn unpublished_bank() -> (
    HeadCache,
    u64,
    std::sync::Weak<crate::clickhouse::StoredTransactionRecord>,
) {
    let cache = HeadCache::new(4, 64);
    let session = cache.start_bank_session(CommitmentLevel::Confirmed);
    stage_block(&cache, session, 42, 1, 1, 41, 3);
    freeze_block(&cache, session, 42, 1, 1, 1);
    let record = {
        let state = cache.banks.read().unwrap();
        Arc::downgrade(&state.banks[&(42, 1)].transactions.as_ref().unwrap()[0].record)
    };
    assert!(record.upgrade().is_some());
    assert!(
        !cache
            .tx_by_signature
            .contains_key(&Signature::from([1; 64]))
    );
    (cache, session, record)
}

/// Runs `operation` held at the release of its bank write lock and checks that the bank it
/// retired is still alive there and is freed once the lock is released.
fn assert_bank_freed_after_release<T: Send>(
    cache: &HeadCache,
    record: &std::sync::Weak<crate::clickhouse::StoredTransactionRecord>,
    operation: impl FnOnce() -> T + Send,
) {
    freeze_paused(
        cache,
        crate::head_cache::PausePoint::Releasing,
        operation,
        || {
            assert!(matches!(
                cache.banks.try_read(),
                Err(std::sync::TryLockError::WouldBlock)
            ));
            assert!(record.upgrade().is_some(), "the free waits for the release");
        },
    );
    assert!(record.upgrade().is_none());
    assert_eq!(cache.retired_len(), 0);
}

#[test]
fn retired_values_are_freed_after_the_lock_is_released() {
    let (cache, session, record) = unpublished_bank();
    freeze_paused(
        &cache,
        crate::head_cache::PausePoint::Released,
        || cache.discard_bank(session, 42, 1),
        || {
            assert!(cache.banks.try_read().is_ok());
            assert!(record.upgrade().is_some(), "the free follows the release");
        },
    );
    assert!(record.upgrade().is_none());
    assert_eq!(cache.retired_len(), 0);
}

#[test]
fn retired_banks_are_freed_after_the_hold() {
    // A commit that removes a losing sibling.
    let (cache, session, record) = unpublished_bank();
    stage_block(&cache, session, 42, 2, 2, 41, 3);
    freeze_block(&cache, session, 42, 2, 2, 4);
    assert_bank_freed_after_release(&cache, &record, || {
        cache.commit_bank(session, 42, 2, CommitmentLevel::Confirmed, Some(41));
    });

    // A discard.
    let (cache, session, record) = unpublished_bank();
    assert_bank_freed_after_release(&cache, &record, || cache.discard_bank(session, 42, 1));

    // A floor eviction by the staging of the next block waits for the next freeze.
    let (cache, session, record) = unpublished_bank();
    stage_block(&cache, session, 120, 1, 1, 119, 3);
    assert!(record.upgrade().is_some());
    assert_bank_freed_after_release(&cache, &record, || {
        freeze_block(&cache, session, 120, 1, 1, 7);
    });

    // A new session replaces the state.
    let (cache, _, record) = unpublished_bank();
    assert_bank_freed_after_release(&cache, &record, || {
        cache.start_bank_session(CommitmentLevel::Confirmed);
    });

    // The end of the session.
    let (cache, session, record) = unpublished_bank();
    assert_bank_freed_after_release(&cache, &record, || cache.end_bank_session(session));
}
