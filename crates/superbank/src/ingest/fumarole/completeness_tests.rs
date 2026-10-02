// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2026 Triton One Limited. All rights reserved.
 */

use super::*;
use yellowstone_grpc_proto::prelude::SubscribeUpdateTransaction;

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
        let update = assembler.finish_slot((42, None)).unwrap().unwrap();
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
