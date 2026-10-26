// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2026 Triton One Limited. All rights reserved.
 */

use super::*;
use crate::ingest::grpc::process_update;
use yellowstone_grpc_proto::prelude::{SubscribeUpdateBlockFooter, SubscribeUpdateTransaction};

fn complete_block(slot: u64) -> SubscribeUpdateBlock {
    SubscribeUpdateBlock {
        slot,
        executed_transaction_count: 2,
        transactions: (0..2)
            .map(|index| SubscribeUpdateTransactionInfo {
                index,
                signature: vec![index as u8 + 1; 64],
                ..Default::default()
            })
            .collect(),
        entries_count: 3,
        entries: (0..3)
            .map(|index| SubscribeUpdateEntry {
                slot,
                index,
                num_hashes: 1,
                hash: vec![1; 32],
                executed_transaction_count: u64::from(index < 2),
                starting_transaction_index: index,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn submit(assembler: &mut FumaroleBlockAssembler, slot: u64, update: UpdateOneof) -> Result<()> {
    assembler.handle_update(
        (slot, None),
        SubscribeUpdate {
            update_oneof: Some(update),
            ..Default::default()
        },
    )?;
    Ok(())
}

fn assemble(assembler: &mut FumaroleBlockAssembler, block: SubscribeUpdateBlock) -> Result<()> {
    for transaction in block.transactions {
        submit(
            assembler,
            block.slot,
            UpdateOneof::Transaction(SubscribeUpdateTransaction {
                slot: block.slot,
                transaction: Some(transaction),
                ..Default::default()
            }),
        )?;
    }
    for entry in block.entries {
        submit(assembler, block.slot, UpdateOneof::Entry(entry))?;
    }
    submit(
        assembler,
        block.slot,
        UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
            slot: block.slot,
            executed_transaction_count: block.executed_transaction_count,
            entries_count: block.entries_count,
            ..Default::default()
        }),
    )
}

#[test]
fn slot_end_rejects_missing_overfull_duplicate_and_zero_count_payloads_and_allows_replay() {
    let edits: &[fn(&mut SubscribeUpdateBlock)] = &[
        |block| {
            block.transactions.pop();
        },
        |block| block.transactions.clear(),
        |block| block.transactions.push(block.transactions[0].clone()),
        |block| block.executed_transaction_count = 0,
        |block| block.transactions[1] = block.transactions[0].clone(),
        |block| block.transactions[1].index = 2,
        |block| {
            block.entries.pop();
        },
        |block| block.entries.clear(),
        |block| block.entries.push(block.entries[0].clone()),
        |block| block.entries_count = 0,
        |block| block.entries[1] = block.entries[0].clone(),
        |block| block.entries[1].index = 3,
        |block| block.entries[1].starting_transaction_index = 0,
    ];
    for edit in edits {
        let mut assembler = FumaroleBlockAssembler::new(true, true);
        submit(
            &mut assembler,
            43,
            UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                slot: 43,
                ..Default::default()
            }),
        )
        .unwrap();
        let other_slot_bytes = assembler.estimated_buffered_bytes();
        let mut invalid = complete_block(42);
        edit(&mut invalid);
        assemble(&mut assembler, invalid).unwrap();
        assert_eq!(assembler.pending_slots(), 2);
        assert!(assembler.estimated_buffered_bytes() > other_slot_bytes);
        assert!(assembler.finish_slot((42, None)).is_err());
        assert_eq!(assembler.pending_slots(), 1);
        assert_eq!(assembler.estimated_buffered_bytes(), other_slot_bytes);

        assemble(&mut assembler, complete_block(42)).unwrap();
        let update = assembler.finish_slot((42, None)).unwrap().unwrap().update;
        let Some(UpdateOneof::Block(block)) = update.update_oneof else {
            panic!("expected complete replayed block");
        };
        assert_eq!(block.transactions.len(), 2);
        assert_eq!(block.entries.len(), 3);
        assert_eq!(assembler.pending_slots(), 1);
        assert_eq!(assembler.estimated_buffered_bytes(), other_slot_bytes);
        assembler.finish_slot((43, None)).unwrap().unwrap();
        assert_eq!(assembler.pending_slots(), 0);
        assert_eq!(assembler.estimated_buffered_bytes(), 0);
    }
}

#[test]
fn malformed_updates_cannot_mutate_pending_assembly() {
    let mut assembler = FumaroleBlockAssembler::new(true, true);
    let meta = SubscribeUpdateBlockMeta {
        slot: 42,
        ..Default::default()
    };
    submit(&mut assembler, 42, UpdateOneof::BlockMeta(meta.clone())).unwrap();
    let before_bytes = assembler.estimated_buffered_bytes();
    let updates = [
        UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
            slot: 43,
            ..Default::default()
        }),
        UpdateOneof::Transaction(SubscribeUpdateTransaction {
            slot: 43,
            transaction: Some(SubscribeUpdateTransactionInfo::default()),
            ..Default::default()
        }),
        UpdateOneof::Transaction(SubscribeUpdateTransaction {
            slot: 42,
            transaction: None,
            ..Default::default()
        }),
        UpdateOneof::Entry(SubscribeUpdateEntry {
            slot: 43,
            ..Default::default()
        }),
        UpdateOneof::Block(complete_block(43)),
        UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
            executed_transaction_count: 1,
            ..meta.clone()
        }),
    ];
    for update in updates {
        assert!(submit(&mut assembler, 42, update).is_err());
        assert_eq!(assembler.pending_slots(), 1);
        assert_eq!(assembler.estimated_buffered_bytes(), before_bytes);
    }
    submit(&mut assembler, 42, UpdateOneof::BlockMeta(meta)).unwrap();
    assembler.finish_slot((42, None)).unwrap().unwrap();
    assert_eq!(assembler.pending_slots(), 0);
    assert_eq!(assembler.estimated_buffered_bytes(), 0);
}

#[test]
fn entries_disabled_assembly_accepts_omitted_entries_and_keeps_transaction_validation() {
    let mut assembler = FumaroleBlockAssembler::new(true, false);
    let mut block = complete_block(42);
    block.entries.clear();
    assemble(&mut assembler, block.clone()).unwrap();
    assembler.finish_slot((42, None)).unwrap().unwrap();
    block.transactions.pop();
    assemble(&mut assembler, block).unwrap();
    assert!(assembler.finish_slot((42, None)).is_err());
}

#[test]
fn commit_waits_for_all_pending_slots_and_rejection_prevents_acknowledgment() {
    let mut assembler = FumaroleBlockAssembler::new(true, true);
    let mut invalid = complete_block(42);
    invalid.transactions.pop();
    assemble(&mut assembler, invalid).unwrap();
    assemble(&mut assembler, complete_block(43)).unwrap();
    assembler.finish_slot((43, None)).unwrap().unwrap();
    let mut commits = 0;
    commit_if_assembled(&assembler, || commits += 1);
    assert_eq!(
        commits, 0,
        "a complete later slot cannot acknowledge an incomplete slot"
    );

    let result: Result<()> = (|| {
        assembler.finish_slot((42, None))?;
        commit_if_assembled(&assembler, || commits += 1);
        Ok(())
    })();
    assert!(result.is_err());
    assert_eq!(commits, 0, "rejection must exit before commit");
    assert_eq!(assembler.pending_slots(), 0);
    assert_eq!(assembler.estimated_buffered_bytes(), 0);

    assemble(&mut assembler, complete_block(42)).unwrap();
    assembler.finish_slot((42, None)).unwrap().unwrap();
    commit_if_assembled(&assembler, || commits += 1);
    assert_eq!(commits, 1);
}

#[test]
fn sealed_same_slot_winner_cannot_acknowledge_an_incomplete_sibling_on_reconnect() {
    let mut assembler = FumaroleBlockAssembler::new(true, false);
    let a = (42, Some(std::sync::Arc::from("loser")));
    let b = (42, Some(std::sync::Arc::from("winner")));
    for (key, bank, expected) in [(a.clone(), 1, 1), (b.clone(), 2, 0)] {
        assembler
            .handle_update(
                key,
                SubscribeUpdate {
                    update_oneof: Some(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                        slot: 42,
                        bank_id: bank,
                        blockhash: if bank == 1 { "loser" } else { "winner" }.into(),
                        executed_transaction_count: expected,
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .unwrap();
    }
    assembler.finish_slot(b.clone()).unwrap().unwrap();
    let mut acknowledgments = 0;
    commit_if_assembled(&assembler, || acknowledgments += 1);
    assert_eq!(acknowledgments, 0);
    assert!(assembler.finish_slot(a).is_err());
    assert_eq!(acknowledgments, 0);
    // Reconnect starts with a fresh envelope-scoped assembler and replays uncommitted data.
    let mut replay = FumaroleBlockAssembler::new(true, false);
    replay
        .handle_update(
            b.clone(),
            SubscribeUpdate {
                update_oneof: Some(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                    slot: 42,
                    bank_id: 0,
                    blockhash: "winner".into(),
                    ..Default::default()
                })),
                ..Default::default()
            },
        )
        .unwrap();
    replay.finish_slot(b).unwrap().unwrap();
    commit_if_assembled(&replay, || acknowledgments += 1);
    assert_eq!(acknowledgments, 1);
}

#[tokio::test]
async fn boundary_stop_flushes_buffered_genesis_before_timer_without_acknowledging_a_sibling_or_rejected_slot()
 {
    use axum::{Router, body::Body, extract::Request};
    use tokio::{net::TcpListener, sync::mpsc};
    metrics::force_init("fumarole", None);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let app = Router::new().fallback(move |request: Request<Body>| {
        let tx = tx.clone();
        async move {
            let query = request.uri().query().unwrap_or_default().to_string();
            let body = axum::body::to_bytes(request.into_body(), usize::MAX)
                .await
                .unwrap();
            tx.send((query, body)).unwrap();
            "Ok"
        }
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = ClickHouseClient::default()
        .with_url(format!("http://{addr}"))
        .with_validation(false)
        .with_compression(clickhouse::Compression::None);
    let mut args = crate::cli::test_args();
    args.source = crate::cli::IngestSource::Fumarole;
    args.fumarole_alpenglow_genesis_slot = Some(42);
    args.entries_table = None;
    let tables = InsertTables::from_args(&args);
    let mut assembler = FumaroleBlockAssembler::new(true, false);
    // An incomplete sibling must survive the cutoff and prevent broad commit.
    submit(
        &mut assembler,
        41,
        UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
            slot: 41,
            executed_transaction_count: 1,
            ..Default::default()
        }),
    )
    .unwrap();
    let block = SubscribeUpdateBlock {
        slot: 42,
        parent_slot: 41,
        blockhash: bs58::encode([1; 32]).into_string(),
        parent_blockhash: bs58::encode([2; 32]).into_string(),
        ..Default::default()
    };
    let mut rows = BufferedRows::new(&args);
    let envelope = |block| SubscribeUpdate {
        update_oneof: Some(UpdateOneof::Block(block)),
        ..Default::default()
    };
    assert!(
        !process_update(
            envelope(block.clone()),
            &args,
            &tables,
            &client,
            &mut rows,
            None
        )
        .await
        .unwrap()
    );
    assert!(
        rx.try_recv().is_err(),
        "genesis remains buffered before timer"
    );
    let mut exceeded = false;
    assert!(
        !stop_at_historical_bound(
            42,
            &args,
            &assembler,
            &mut exceeded,
            &client,
            &tables,
            &mut rows
        )
        .await
        .unwrap()
    );
    assert!(
        !stop_at_historical_bound(
            43,
            &args,
            &assembler,
            &mut exceeded,
            &client,
            &tables,
            &mut rows
        )
        .await
        .unwrap(),
        "a later slot must not stop ingestion while a bank at or below the bound is in flight"
    );
    assert!(exceeded);
    assert!(!rows.is_empty());
    assert!(rx.try_recv().is_err(), "nothing flushes while deferring");
    let mut acknowledgments = 0;
    commit_if_assembled(&assembler, || acknowledgments += 1);
    assert_eq!(acknowledgments, 0);
    assert_eq!(assembler.pending_slots(), 1);
    assembler.blocks.clear();
    assert!(
        stop_at_historical_bound(
            43,
            &args,
            &assembler,
            &mut exceeded,
            &client,
            &tables,
            &mut rows
        )
        .await
        .unwrap()
    );
    assert!(rows.is_empty());
    let (query, body) = rx.try_recv().unwrap();
    assert!(query.contains("blocks_metadata"));
    assert_eq!(u64::from_le_bytes(body[..8].try_into().unwrap()), 42);
    assert!(rx.try_recv().is_err(), "no rejected block can be inserted");
    // Replay of the valid prefix reuses the same slot-keyed metadata row.
    process_update(envelope(block), &args, &tables, &client, &mut rows, None)
        .await
        .unwrap();
    stop_at_historical_bound(
        43,
        &args,
        &assembler,
        &mut exceeded,
        &client,
        &tables,
        &mut rows,
    )
    .await
    .unwrap();
    assert_eq!(rx.try_recv().unwrap().1, body);
    server.abort();
}

fn footer_update(slot: u64, bank_id: u64, hash_len: usize) -> UpdateOneof {
    UpdateOneof::BlockFooter(SubscribeUpdateBlockFooter {
        slot,
        bank_id,
        bank_hash: vec![7; hash_len],
        block_producer_time_nanos: 5,
        block_user_agent: b"agave".to_vec(),
        ..Default::default()
    })
}

fn finish(assembler: &mut FumaroleBlockAssembler, slot: u64) -> FinishedBank {
    assembler.finish_slot((slot, None)).unwrap().unwrap()
}

#[test]
fn footer_before_the_block_data_is_attached() {
    let mut assembler = FumaroleBlockAssembler::new(false, true);
    submit(&mut assembler, 42, footer_update(42, 0, 32)).unwrap();
    assemble(&mut assembler, complete_block(42)).unwrap();
    let finished = finish(&mut assembler, 42);
    let footer = finished.footer.expect("footer");
    assert_eq!(footer.bank_hash, [7; 32]);
    assert_eq!(footer.block_producer_time_nanos, 5);
    assert_eq!(footer.block_user_agent, b"agave".to_vec());
    assert!(!finished.footer_missing);
}

#[test]
fn footer_after_the_block_data_and_before_slot_end_is_attached() {
    let mut assembler = FumaroleBlockAssembler::new(false, true);
    assemble(&mut assembler, complete_block(42)).unwrap();
    submit(&mut assembler, 42, footer_update(42, 0, 32)).unwrap();
    assert!(finish(&mut assembler, 42).footer.is_some());
}

#[test]
fn no_footer_before_activation_is_null_and_not_counted() {
    let mut assembler = FumaroleBlockAssembler::new(false, true);
    assemble(&mut assembler, complete_block(42)).unwrap();
    let finished = finish(&mut assembler, 42);
    assert!(finished.footer.is_none());
    assert!(!finished.footer_missing);
}

#[test]
fn a_bank_without_a_footer_after_footers_started_is_counted() {
    let mut assembler = FumaroleBlockAssembler::new(false, true);
    assemble(&mut assembler, complete_block(41)).unwrap();
    submit(&mut assembler, 42, footer_update(42, 0, 32)).unwrap();
    assemble(&mut assembler, complete_block(42)).unwrap();
    assemble(&mut assembler, complete_block(43)).unwrap();
    assert!(
        !finish(&mut assembler, 41).footer_missing,
        "below the first footer slot"
    );
    assert!(!finish(&mut assembler, 42).footer_missing);
    let missing = finish(&mut assembler, 43);
    assert!(missing.footer.is_none());
    assert!(missing.footer_missing);
}

#[test]
fn invalid_or_conflicting_footers_are_dropped_without_failing_ingestion() {
    metrics::force_init("fumarole", None);
    let cases: Vec<Vec<UpdateOneof>> = vec![
        vec![footer_update(42, 0, 32), {
            let UpdateOneof::BlockFooter(mut other) = footer_update(42, 0, 32) else {
                unreachable!()
            };
            other.bank_hash = vec![8; 32];
            UpdateOneof::BlockFooter(other)
        }],
        vec![footer_update(99, 0, 32)],
        vec![footer_update(42, 0, 31)],
        vec![footer_update(42, 5, 32)],
    ];
    for footers in cases {
        let mut assembler = FumaroleBlockAssembler::new(false, true);
        for footer in footers {
            submit(&mut assembler, 42, footer).unwrap();
        }
        assemble(&mut assembler, complete_block(42)).unwrap();
        let finished = finish(&mut assembler, 42);
        assert!(finished.footer.is_none());
        assert!(
            !finished.footer_missing,
            "an invalid footer is not a missing one"
        );
    }
}

#[test]
fn an_identical_duplicate_footer_is_kept() {
    let mut assembler = FumaroleBlockAssembler::new(false, true);
    submit(&mut assembler, 42, footer_update(42, 0, 32)).unwrap();
    submit(&mut assembler, 42, footer_update(42, 0, 32)).unwrap();
    assemble(&mut assembler, complete_block(42)).unwrap();
    assert!(finish(&mut assembler, 42).footer.is_some());
}

#[test]
fn the_subscribe_request_asks_for_footers_without_certificates() {
    let request = build_fumarole_subscribe_request(2, true);
    let filter = request.block_footer.values().next().expect("footer filter");
    assert_eq!(filter.include_certificates, Some(false));
}

#[tokio::test]
async fn an_unset_bound_never_stops_ingestion() {
    let mut args = crate::cli::test_args();
    args.source = crate::cli::IngestSource::Fumarole;
    args.fumarole_alpenglow_genesis_slot = None;
    args.fumarole_preactivation_through_slot = None;
    assert_eq!(historical_bound(&args), None);
    let client = ClickHouseClient::default().with_url("http://127.0.0.1:1");
    let tables = InsertTables::from_args(&args);
    let assembler = FumaroleBlockAssembler::new(false, false);
    let mut rows = BufferedRows::new(&args);
    let mut exceeded = false;
    assert!(
        !stop_at_historical_bound(
            u64::MAX,
            &args,
            &assembler,
            &mut exceeded,
            &client,
            &tables,
            &mut rows
        )
        .await
        .unwrap()
    );
    assert!(!exceeded);
    args.fumarole_preactivation_through_slot = Some(10);
    assert_eq!(historical_bound(&args), Some(10));
}
