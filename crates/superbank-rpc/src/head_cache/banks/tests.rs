// SPDX-License-Identifier: AGPL-3.0-only
use super::*;
use crate::head_cache::dragonsmouth::parse_block_meta;
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
