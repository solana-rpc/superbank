// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2026 Triton One Limited. All rights reserved.
 */

use super::*;

fn complete_block(slot: u64) -> SubscribeUpdateBlock {
    let transactions = (0..2)
        .map(|index| {
            let mut transaction = super::tests::build_test_transaction_info(None);
            transaction.index = index;
            transaction.signature = vec![index as u8 + 1; 64];
            transaction.transaction.as_mut().unwrap().signatures =
                vec![transaction.signature.clone()];
            transaction
        })
        .collect();
    let entries = (0..3)
        .map(|index| SubscribeUpdateEntry {
            slot,
            index,
            num_hashes: 1,
            hash: vec![index as u8 + 1; 32],
            executed_transaction_count: u64::from(index < 2),
            starting_transaction_index: index,
            ..Default::default()
        })
        .collect();
    SubscribeUpdateBlock {
        slot,
        blockhash: bs58::encode([1; 32]).into_string(),
        parent_slot: slot - 1,
        parent_blockhash: bs58::encode([2; 32]).into_string(),
        executed_transaction_count: 2,
        transactions,
        entries_count: 3,
        entries,
        ..Default::default()
    }
}

fn buffered_rows() -> BufferedRows {
    BufferedRows {
        transaction_rows: Vec::new(),
        block_rows: Vec::new(),
        entry_rows: Vec::new(),
        last_durable_block_slot: Some(40),
    }
}

fn buffer_block(rows: &mut BufferedRows, block: SubscribeUpdateBlock) -> Result<()> {
    handle_block_update(
        block,
        &mut rows.transaction_rows,
        &mut rows.block_rows,
        Some(&mut rows.entry_rows),
    )
}

fn snapshot(rows: &BufferedRows) -> serde_json::Value {
    serde_json::to_value((
        &rows.transaction_rows,
        &rows.block_rows,
        &rows.entry_rows,
        rows.last_durable_block_slot,
    ))
    .unwrap()
}

#[test]
fn malformed_blocks_leave_successful_buffers_and_resume_unchanged() {
    let valid = complete_block(42);
    let mut cases = Vec::new();
    let mut add = |name: &str, edit: fn(&mut SubscribeUpdateBlock)| {
        let mut block = valid.clone();
        edit(&mut block);
        cases.push((name.to_string(), block));
    };
    add("missing transaction", |block| {
        block.transactions.pop();
    });
    add("no transactions", |block| block.transactions.clear());
    add("overfull transactions", |block| {
        block.transactions.push(block.transactions[0].clone());
    });
    add("unexpected transaction in zero-count block", |block| {
        block.executed_transaction_count = 0;
    });
    add("duplicate transaction hiding a hole", |block| {
        block.transactions[1] = block.transactions[0].clone();
    });
    add("transaction index hole", |block| {
        block.transactions[1].index = 2;
    });
    add("transaction index overflow", |block| {
        block.transactions[1].index = u64::MAX;
    });
    add("duplicate signature at different indices", |block| {
        block.transactions[1].signature = block.transactions[0].signature.clone();
    });
    add("missing entry", |block| {
        block.entries.pop();
    });
    add("no entries", |block| block.entries.clear());
    add("overfull entries", |block| {
        block.entries.push(block.entries[0].clone());
    });
    add("unexpected entry in zero-count block", |block| {
        block.entries_count = 0;
    });
    add("duplicate entry hiding a hole", |block| {
        block.entries[1] = block.entries[0].clone();
    });
    add("entry index hole", |block| block.entries[1].index = 3);
    add("entry index overflow", |block| {
        block.entries[1].index = u64::MAX;
    });
    add("entry from a different slot", |block| {
        block.entries[1].slot += 1;
    });
    add("entry transaction range hole", |block| {
        block.entries[1].starting_transaction_index = 2;
    });
    add("entry transaction range overlap", |block| {
        block.entries[1].starting_transaction_index = 0;
    });
    add("entry transaction range overfull", |block| {
        block.entries[1].executed_transaction_count = 2;
    });
    add("entry transaction range overflow", |block| {
        block.entries[1].executed_transaction_count = u64::MAX;
    });
    add("entries omit executed transaction", |block| {
        block.entries[1].executed_transaction_count = 0;
        block.entries[2].starting_transaction_index = 1;
    });
    // Decoding failures after completeness validation must also be atomic.
    add("invalid metadata hash", |block| block.blockhash.clear());
    add("invalid transaction payload", |block| {
        block.transactions[1].meta = None;
    });
    add("invalid final entry hash", |block| {
        block.entries[2].hash.pop();
    });

    for (name, block) in cases {
        let mut rows = buffered_rows();
        buffer_block(&mut rows, complete_block(41)).unwrap();
        let before = snapshot(&rows);
        let resume_before =
            next_subscribe_from_slot(Some(40), rows.last_durable_block_slot).unwrap();
        assert!(buffer_block(&mut rows, block).is_err(), "accepted {name}");
        assert_eq!(snapshot(&rows), before, "mutated buffers for {name}");
        assert_eq!(
            next_subscribe_from_slot(Some(40), rows.last_durable_block_slot).unwrap(),
            resume_before,
            "advanced resume for {name}"
        );
        buffer_block(&mut rows, valid.clone()).unwrap();
        assert_eq!(rows.block_rows.len(), 2, "failed replay for {name}");
        assert_eq!(rows.transaction_rows.len(), 4, "failed replay for {name}");
        assert_eq!(rows.entry_rows.len(), 6, "failed replay for {name}");
        assert_eq!(rows.last_durable_block_slot, Some(40));
    }
}

#[test]
fn complete_blocks_accept_out_of_order_updates_and_omitted_entries() {
    let mut block = complete_block(42);
    block.transactions.reverse();
    block.entries.reverse();
    let mut rows = buffered_rows();
    buffer_block(&mut rows, block).unwrap();
    assert_eq!(rows.transaction_rows.len(), 2);
    assert_eq!(rows.entry_rows.len(), 3);
    assert_eq!(rows.block_rows.len(), 1);

    let mut block = complete_block(43);
    block.entries.clear();
    handle_block_update(
        block,
        &mut rows.transaction_rows,
        &mut rows.block_rows,
        None,
    )
    .unwrap();
    assert_eq!(rows.transaction_rows.len(), 4);
    assert_eq!(rows.block_rows.len(), 2);
    assert_eq!(rows.entry_rows.len(), 3);
    assert_eq!(rows.block_rows[1].entry_count, 3);
}

#[test]
fn empty_blocks_accept_zero_entries_alpentick_and_historical_ticks() {
    for entry_count in [0, 1, 64] {
        let mut block = complete_block(42);
        block.executed_transaction_count = 0;
        block.transactions.clear();
        block.entries_count = entry_count;
        block.entries = (0..entry_count)
            .map(|index| SubscribeUpdateEntry {
                slot: 42,
                index,
                num_hashes: if entry_count == 64 { 12_500 } else { 1 },
                hash: vec![1; 32],
                ..Default::default()
            })
            .collect();
        let mut rows = buffered_rows();
        buffer_block(&mut rows, block).unwrap();
        assert!(rows.transaction_rows.is_empty());
        assert_eq!(rows.block_rows.len(), 1);
        assert_eq!(rows.entry_rows.len() as u64, entry_count);
    }
}

#[tokio::test]
async fn rejection_does_not_write_and_complete_replay_preserves_flush_order() {
    use axum::{Router, body::Body, extract::Request};
    use tokio::{net::TcpListener, sync::mpsc};

    metrics::force_init("grpc", None);
    let (request_tx, mut request_rx) = mpsc::unbounded_channel();
    let app = Router::new().fallback(move |request: Request<Body>| {
        let request_tx = request_tx.clone();
        async move {
            let query = request.uri().query().unwrap_or_default().to_string();
            axum::body::to_bytes(request.into_body(), usize::MAX)
                .await
                .unwrap();
            request_tx.send(query).unwrap();
            "Ok"
        }
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = ClickHouseClient::default()
        .with_url(format!("http://{address}"))
        .with_validation(false)
        .with_compression(clickhouse::Compression::None);
    let tables = InsertTables {
        transactions_table: "transactions".to_string(),
        blocks_table: "blocks_metadata".to_string(),
        entries_table: Some("entries".to_string()),
    };
    let mut rows = buffered_rows();
    let mut invalid = complete_block(42);
    invalid.transactions.pop();
    assert!(buffer_block(&mut rows, invalid).is_err());
    rows.flush(&client, &tables).await.unwrap();
    assert!(request_rx.try_recv().is_err());
    assert_eq!(rows.last_durable_block_slot, Some(40));

    buffer_block(&mut rows, complete_block(42)).unwrap();
    rows.flush(&client, &tables).await.unwrap();
    let requests: Vec<_> = std::iter::from_fn(|| request_rx.try_recv().ok()).collect();
    assert_eq!(requests.len(), 3);
    for (query, table) in requests
        .iter()
        .zip(["transactions", "blocks_metadata", "entries"])
    {
        assert!(query.contains(table), "unexpected flush query: {query}");
        assert!(query.contains("INSERT"), "unexpected flush query: {query}");
    }
    assert!(rows.is_empty());
    assert_eq!(rows.last_durable_block_slot, Some(42));
    assert_eq!(
        next_subscribe_from_slot(Some(40), rows.last_durable_block_slot).unwrap(),
        Some(43)
    );
    server.abort();
}

#[tokio::test]
async fn early_footer_and_winner_cannot_write_incomplete_data_or_a_losing_bank() {
    use axum::{Router, body::Body, extract::Request};
    use tokio::{net::TcpListener, sync::mpsc};
    use yellowstone_grpc_proto::prelude::SubscribeUpdateSlot;
    metrics::force_init("grpc", None);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let app = Router::new().fallback(move |request: Request<Body>| {
        let tx = tx.clone();
        async move {
            let query = request.uri().query().unwrap_or_default().to_string();
            axum::body::to_bytes(request.into_body(), usize::MAX)
                .await
                .unwrap();
            tx.send(query).unwrap();
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
    let args = crate::cli::test_args();
    let tables = InsertTables::from_args(&args);
    let retry = RetryConfig {
        max_retries: 0,
        base_ms: 1,
        max_ms: 1,
    };
    let mut rows = buffered_rows();
    let mut join = FinalizedFooterJoin::default();
    let envelope = |update_oneof| SubscribeUpdate {
        update_oneof: Some(update_oneof),
        ..Default::default()
    };
    for update in [
        UpdateOneof::BlockFooter(SubscribeUpdateBlockFooter {
            slot: 42,
            bank_id: 0,
            bank_hash: vec![7; 32],
            ..Default::default()
        }),
        UpdateOneof::Slot(SubscribeUpdateSlot {
            slot: 42,
            bank_id: Some(0),
            status: SlotStatus::SlotFinalized as i32,
            ..Default::default()
        }),
    ] {
        process_canonical_update(
            envelope(update),
            &args,
            &tables,
            &client,
            &mut rows,
            &retry,
            &mut join,
        )
        .await
        .unwrap();
    }
    assert!(rx.try_recv().is_err());
    let mut incomplete = complete_block(42);
    incomplete.transactions.pop();
    assert!(
        process_canonical_update(
            envelope(UpdateOneof::Block(incomplete)),
            &args,
            &tables,
            &client,
            &mut rows,
            &retry,
            &mut join
        )
        .await
        .is_err()
    );
    assert!(rx.try_recv().is_err());
    assert!(rows.is_empty());
    assert!(!join.complete_blocks.contains(&(42, 0)));
    let mut losing = complete_block(42);
    losing.bank_id = 9;
    assert!(
        process_canonical_update(
            envelope(UpdateOneof::Block(losing)),
            &args,
            &tables,
            &client,
            &mut rows,
            &retry,
            &mut join
        )
        .await
        .is_err()
    );
    assert!(rx.try_recv().is_err());
    process_canonical_update(
        envelope(UpdateOneof::Block(complete_block(42))),
        &args,
        &tables,
        &client,
        &mut rows,
        &retry,
        &mut join,
    )
    .await
    .unwrap();
    let requests: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
    assert_eq!(requests.len(), 4, "complete data precedes footer insert");
    assert!(requests.last().unwrap().contains("block_footers"));
    assert_eq!(rows.last_durable_block_slot, Some(42));
    server.abort();
}

fn canonical_envelope(update_oneof: UpdateOneof) -> SubscribeUpdate {
    SubscribeUpdate {
        update_oneof: Some(update_oneof),
        ..Default::default()
    }
}

fn identity_status(slot: u64, bank_id: Option<u64>, status: SlotStatus) -> SubscribeUpdate {
    canonical_envelope(UpdateOneof::Slot(
        yellowstone_grpc_proto::prelude::SubscribeUpdateSlot {
            slot,
            bank_id,
            status: status as i32,
            ..Default::default()
        },
    ))
}

#[tokio::test]
async fn canonical_legacy_missing_id_and_modern_zero_are_distinct_in_both_orders() {
    let args = crate::cli::test_args();
    let tables = InsertTables::from_args(&args);
    let client = ClickHouseClient::default().with_url("http://127.0.0.1:1");
    let retry = RetryConfig {
        max_retries: 0,
        base_ms: 1,
        max_ms: 1,
    };
    for bank_id in [None, Some(0)] {
        for data_first in [false, true] {
            let mut rows = buffered_rows();
            let mut join = FinalizedFooterJoin::default();
            let block = canonical_envelope(UpdateOneof::Block(complete_block(42)));
            let status = identity_status(42, bank_id, SlotStatus::SlotCreatedBank);
            let updates = if data_first {
                [block, status]
            } else {
                [status, block]
            };
            for (index, update) in updates.into_iter().enumerate() {
                process_canonical_update(
                    update, &args, &tables, &client, &mut rows, &retry, &mut join,
                )
                .await
                .unwrap();
                if data_first && index == 0 {
                    assert!(rows.is_empty());
                    assert_eq!(join.pending_identity.len(), 1);
                    assert!(!join.complete_blocks.contains(&(42, 0)));
                }
            }
            assert_eq!(rows.block_rows.len(), 1);
            assert_eq!(rows.block_rows[0].bank_id, bank_id);
            assert_eq!(join.complete_blocks.contains(&(42, 0)), bank_id.is_some());
            assert!(join.pending_identity.is_empty());
        }
    }
    // A historical replay without CreatedBank still has the trusted finalized
    // full-block/status contract, but no proof of a node-local scalar zero.
    let mut rows = buffered_rows();
    let mut join = FinalizedFooterJoin::default();
    for update in [
        canonical_envelope(UpdateOneof::Block(complete_block(42))),
        identity_status(42, None, SlotStatus::SlotFinalized),
    ] {
        process_canonical_update(
            update, &args, &tables, &client, &mut rows, &retry, &mut join,
        )
        .await
        .unwrap();
    }
    assert_eq!(rows.block_rows[0].bank_id, None);
}

#[tokio::test]
async fn pending_identity_holds_later_banks_and_all_flush_paths_until_replay() {
    use axum::{Router, body::Body, extract::Request};
    use tokio::{net::TcpListener, sync::mpsc};
    metrics::force_init("grpc", None);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let app = Router::new().fallback(move |request: Request<Body>| {
        let tx = tx.clone();
        async move {
            let query = request.uri().query().unwrap_or_default().to_string();
            axum::body::to_bytes(request.into_body(), usize::MAX)
                .await
                .unwrap();
            tx.send(query).unwrap();
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
    args.flush_every_block = true; // Both pressure and per-block flushes must wait.
    let tables = InsertTables::from_args(&args);
    let retry = RetryConfig {
        max_retries: 0,
        base_ms: 1,
        max_ms: 1,
    };
    let mut rows = buffered_rows();
    let mut join = FinalizedFooterJoin::default();
    let mut known = complete_block(43);
    known.bank_id = 7;
    for entry in &mut known.entries {
        entry.bank_id = 7;
    }
    for update in [
        canonical_envelope(UpdateOneof::Block(complete_block(42))),
        canonical_envelope(UpdateOneof::Block(known.clone())),
        canonical_envelope(UpdateOneof::BlockFooter(SubscribeUpdateBlockFooter {
            slot: 43,
            bank_id: 7,
            bank_hash: vec![7; 32],
            ..Default::default()
        })),
        identity_status(43, Some(7), SlotStatus::SlotFinalized),
        identity_status(44, Some(0), SlotStatus::SlotCreatedBank),
    ] {
        process_canonical_update(
            update, &args, &tables, &client, &mut rows, &retry, &mut join,
        )
        .await
        .unwrap();
    }
    assert_eq!(join.pending_identity.len(), 2);
    assert!(rows.is_empty());
    assert!(rx.try_recv().is_err());
    // These are the shared timer, shutdown, and fatal transport/health guards.
    assert!(
        !flush_canonical_rows(&client, &tables, &mut rows, Some(&retry), &mut join)
            .await
            .unwrap()
    );
    flush_after_fatal_condition(&client, &tables, &mut rows, "disconnect", &mut join)
        .await
        .unwrap();
    assert!(rx.try_recv().is_err());
    assert_eq!(rows.last_durable_block_slot, Some(40));
    assert_eq!(
        next_subscribe_from_slot(Some(40), rows.last_durable_block_slot).unwrap(),
        Some(41)
    );

    // A reconnect discards subscription-local proofs and unflushed data. The old
    // slot-44 zero cannot qualify the replayed slot-42 block on a new connection.
    drop(join);
    let mut rows = buffered_rows();
    let mut join = FinalizedFooterJoin::default();
    process_canonical_update(
        canonical_envelope(UpdateOneof::Block(complete_block(42))),
        &args,
        &tables,
        &client,
        &mut rows,
        &retry,
        &mut join,
    )
    .await
    .unwrap();
    assert!(rows.is_empty());
    assert!(rx.try_recv().is_err());
    for update in [
        identity_status(42, Some(0), SlotStatus::SlotFinalized),
        canonical_envelope(UpdateOneof::Block(known)),
        identity_status(43, Some(7), SlotStatus::SlotFinalized),
        canonical_envelope(UpdateOneof::BlockFooter(SubscribeUpdateBlockFooter {
            slot: 43,
            bank_id: 7,
            bank_hash: vec![7; 32],
            ..Default::default()
        })),
    ] {
        process_canonical_update(
            update, &args, &tables, &client, &mut rows, &retry, &mut join,
        )
        .await
        .unwrap();
    }
    assert!(join.complete_blocks.contains(&(42, 0)));
    assert_eq!(rows.last_durable_block_slot, Some(43));
    assert!(join.completed.contains(&(43, 7)));
    let requests: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
    assert_eq!(
        requests.len(),
        7,
        "both complete banks persist before the winner footer"
    );
    assert!(requests.last().unwrap().contains("block_footers"));
    server.abort();
}

#[test]
fn unknown_identity_queue_is_bounded_and_does_not_mutate_on_rejection() {
    let mut join = FinalizedFooterJoin::default();
    let prepared = prepare_block(&complete_block(42), true).unwrap();
    assert!(
        join.stage_identity_block(prepared, PENDING_IDENTITY_MAX_BYTES + 1)
            .is_err()
    );
    assert!(join.pending_identity.is_empty());
    assert_eq!(join.pending_identity_bytes, 0);
    for slot in 42..42 + PENDING_IDENTITY_MAX_BLOCKS as u64 {
        join.stage_identity_block(prepare_block(&complete_block(slot), true).unwrap(), 1)
            .unwrap();
    }
    assert!(join.take_ready_blocks().unwrap().is_empty());
    let before = join.pending_identity_bytes;
    assert!(
        join.stage_identity_block(prepare_block(&complete_block(1000), true).unwrap(), 1)
            .is_err()
    );
    assert_eq!(join.pending_identity_bytes, before);
    assert_eq!(join.pending_identity.len(), PENDING_IDENTITY_MAX_BLOCKS);
}

#[tokio::test]
async fn canonical_optional_zero_proof_is_not_reused_after_reconnect() {
    let args = crate::cli::test_args();
    let tables = InsertTables::from_args(&args);
    let client = ClickHouseClient::default().with_url("http://127.0.0.1:1");
    let retry = RetryConfig {
        max_retries: 0,
        base_ms: 1,
        max_ms: 1,
    };
    let mut rows = buffered_rows();
    let mut old = FinalizedFooterJoin::default();
    for update in [
        identity_status(42, Some(0), SlotStatus::SlotCreatedBank),
        canonical_envelope(UpdateOneof::Block(complete_block(42))),
    ] {
        process_canonical_update(update, &args, &tables, &client, &mut rows, &retry, &mut old)
            .await
            .unwrap();
    }
    assert_eq!(rows.block_rows[0].bank_id, Some(0));
    // The runtime creates a fresh join and buffers for every subscription.
    let mut rows = buffered_rows();
    let mut reconnect = FinalizedFooterJoin::default();
    process_canonical_update(
        canonical_envelope(UpdateOneof::Block(complete_block(42))),
        &args,
        &tables,
        &client,
        &mut rows,
        &retry,
        &mut reconnect,
    )
    .await
    .unwrap();
    assert!(rows.is_empty());
    assert!(!reconnect.complete_blocks.contains(&(42, 0)));
    process_canonical_update(
        identity_status(42, None, SlotStatus::SlotFinalized),
        &args,
        &tables,
        &client,
        &mut rows,
        &retry,
        &mut reconnect,
    )
    .await
    .unwrap();
    assert_eq!(rows.block_rows[0].bank_id, None);
}

#[tokio::test]
async fn ready_earlier_footer_persists_while_later_identity_holds_restart_progress() {
    use axum::{Router, body::Body, extract::Request};
    use tokio::{net::TcpListener, sync::mpsc};
    metrics::force_init("grpc", None);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let app = Router::new().fallback(move |request: Request<Body>| {
        let tx = tx.clone();
        async move {
            let query = request.uri().query().unwrap_or_default().to_string();
            axum::body::to_bytes(request.into_body(), usize::MAX)
                .await
                .unwrap();
            tx.send(query).unwrap();
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
    for flush_earlier in [false, true] {
        let mut args = crate::cli::test_args();
        args.flush_every_block = flush_earlier;
        let tables = InsertTables::from_args(&args);
        let retry = RetryConfig {
            max_retries: 0,
            base_ms: 1,
            max_ms: 1,
        };
        let mut rows = buffered_rows();
        let mut join = FinalizedFooterJoin::default();
        let mut known = complete_block(42);
        known.bank_id = 7;
        for entry in &mut known.entries {
            entry.bank_id = 7;
        }
        process_canonical_update(
            canonical_envelope(UpdateOneof::Block(known)),
            &args,
            &tables,
            &client,
            &mut rows,
            &retry,
            &mut join,
        )
        .await
        .unwrap();
        assert_eq!(
            std::iter::from_fn(|| rx.try_recv().ok()).count(),
            if flush_earlier { 3 } else { 0 }
        );
        assert_eq!(
            rows.last_durable_block_slot,
            Some(if flush_earlier { 42 } else { 40 })
        );
        for update in [
            canonical_envelope(UpdateOneof::Block(complete_block(43))),
            canonical_envelope(UpdateOneof::BlockFooter(SubscribeUpdateBlockFooter {
                slot: 42,
                bank_id: 7,
                bank_hash: vec![7; 32],
                ..Default::default()
            })),
            identity_status(42, Some(7), SlotStatus::SlotFinalized),
        ] {
            process_canonical_update(
                update, &args, &tables, &client, &mut rows, &retry, &mut join,
            )
            .await
            .unwrap();
        }
        let requests: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
        assert_eq!(
            requests.len(),
            usize::from(flush_earlier),
            "only durable complete bank 42's footer may bypass the bank 43 hold"
        );
        if flush_earlier {
            assert!(requests[0].contains("block_footers"));
            assert!(rows.is_empty());
            assert!(join.durable_blocks.contains(&(42, 7)));
        } else {
            assert_eq!(rows.block_rows.len(), 1);
            assert!(!join.durable_blocks.contains(&(42, 7)));
        }
        assert_eq!(join.pending_identity.len(), 1);
        assert!(
            !flush_canonical_rows(&client, &tables, &mut rows, Some(&retry), &mut join)
                .await
                .unwrap()
        );
        flush_after_fatal_condition(&client, &tables, &mut rows, "disconnect", &mut join)
            .await
            .unwrap();
        assert!(rx.try_recv().is_err());
        assert_eq!(
            rows.last_durable_block_slot,
            Some(if flush_earlier { 42 } else { 40 })
        );
        assert_eq!(
            next_subscribe_from_slot(None, rows.last_durable_block_slot).unwrap(),
            Some(if flush_earlier { 43 } else { 41 })
        );
    }
    server.abort();
}

#[tokio::test]
async fn validation_exit_flushes_only_an_earlier_qualified_prefix_and_replays_safely() {
    use axum::{Router, body::Body, extract::Request};
    use tokio::{net::TcpListener, sync::mpsc};
    metrics::force_init("grpc", None);
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
    let args = crate::cli::test_args();
    let tables = InsertTables::from_args(&args);
    let retry = RetryConfig {
        max_retries: 0,
        base_ms: 1,
        max_ms: 1,
    };
    for (held, conflict) in [(false, false), (true, false), (false, true)] {
        let mut first_replay = None;
        for _ in 0..2 {
            let mut rows = buffered_rows();
            let mut join = FinalizedFooterJoin::default();
            let mut valid = complete_block(42);
            valid.bank_id = 7;
            for entry in &mut valid.entries {
                entry.bank_id = 7;
            }
            for update in [
                identity_status(42, Some(7), SlotStatus::SlotFinalized),
                canonical_envelope(UpdateOneof::Block(valid)),
            ] {
                process_canonical_update(
                    update, &args, &tables, &client, &mut rows, &retry, &mut join,
                )
                .await
                .unwrap();
            }
            assert!(
                rx.is_empty(),
                "valid prefix must be buffered before the timer"
            );
            if held {
                process_canonical_update(
                    canonical_envelope(UpdateOneof::Block(complete_block(43))),
                    &args,
                    &tables,
                    &client,
                    &mut rows,
                    &retry,
                    &mut join,
                )
                .await
                .unwrap();
            }
            let rejected = if conflict {
                identity_status(42, Some(8), SlotStatus::SlotFinalized)
            } else {
                let mut invalid = complete_block(if held { 44 } else { 43 });
                invalid.transactions.pop();
                canonical_envelope(UpdateOneof::Block(invalid))
            };
            assert!(
                process_canonical_update(
                    rejected, &args, &tables, &client, &mut rows, &retry, &mut join
                )
                .await
                .is_err()
            );
            let requests: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
            if held || conflict {
                assert!(
                    requests.is_empty(),
                    "unidentified or contradicted data must not flush"
                );
                assert_eq!(rows.last_durable_block_slot, Some(40));
                assert_eq!(rows.block_rows.len(), 1);
            } else {
                assert_eq!(requests.len(), 3);
                assert!(rows.is_empty());
                assert_eq!(rows.last_durable_block_slot, Some(42));
                assert_eq!(
                    next_subscribe_from_slot(None, rows.last_durable_block_slot).unwrap(),
                    Some(43)
                );
                if let Some(first) = &first_replay {
                    assert_eq!(first, &requests);
                }
                first_replay = Some(requests);
            }
        }
    }
    server.abort();
}

#[tokio::test]
async fn absent_or_failed_footer_storage_cannot_stop_valid_canonical_data() {
    use axum::{Router, body::Body, extract::Request, http::StatusCode};
    use tokio::{net::TcpListener, sync::mpsc};
    metrics::force_init("grpc", None);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let app = Router::new().fallback(move |request: Request<Body>| {
        let tx = tx.clone();
        async move {
            let query = request.uri().query().unwrap_or_default().to_string();
            let body = axum::body::to_bytes(request.into_body(), usize::MAX)
                .await
                .unwrap();
            let footer = query.contains("block_footers")
                || String::from_utf8_lossy(&body).contains("block_footers");
            tx.send(query).unwrap();
            if footer {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "footer table unavailable",
                )
            } else {
                (StatusCode::OK, "Ok")
            }
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
    args.flush_every_block = true;
    let tables = InsertTables::from_args(&args);
    let retry = RetryConfig {
        max_retries: 0,
        base_ms: 1,
        max_ms: 1,
    };
    assert!(!qualify_footer_storage(&client, &args.block_footers_table).await);
    for availability in [Some(false), Some(true)] {
        let mut join = FinalizedFooterJoin {
            footer_storage_available: availability,
            ..Default::default()
        };
        let mut rows = buffered_rows();
        let bank_block = |slot, bank_id| {
            let mut block = complete_block(slot);
            block.bank_id = bank_id;
            for entry in &mut block.entries { entry.bank_id = bank_id; }
            canonical_envelope(UpdateOneof::Block(block))
        };
        for update in [
            identity_status(42, Some(7), SlotStatus::SlotFinalized),
            canonical_envelope(UpdateOneof::BlockFooter(SubscribeUpdateBlockFooter {
                slot: 42,
                bank_id: 7,
                bank_hash: vec![9; 32],
                ..Default::default()
            })),
            bank_block(42, 7),
            bank_block(43, 8),
        ] {
            process_canonical_update(
                update, &args, &tables, &client, &mut rows, &retry, &mut join,
            )
            .await
            .unwrap();
        }
        assert_eq!(rows.last_durable_block_slot, Some(43));
        assert_eq!(
            join.ready_footers.len(),
            usize::from(availability == Some(true))
        );
    }
    let requests: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
    assert!(
        requests
            .iter()
            .filter(|query| query.contains("blocks_metadata"))
            .count()
            >= 4
    );
    server.abort();
}

#[tokio::test]
async fn native_footer_startup_qualifies_fqn_and_rejects_absent_table() {
    let Ok(url) = std::env::var("DISK_CACHE_TEST_URL") else {
        return;
    };
    assert!(url.starts_with("http://127.0.0.1:"));
    metrics::force_init("grpc", None);
    let client = ClickHouseClient::default().with_url(url);
    let table = format!(
        "review_footer_qualification_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    client.query(&format!("CREATE TABLE default.{table} (slot UInt64, bank_id UInt64, bank_hash FixedString(32), block_producer_time_nanos UInt64, block_user_agent String) ENGINE=ReplacingMergeTree ORDER BY slot")).execute().await.unwrap();
    assert!(qualify_footer_storage(&client, &format!("default.{table}")).await);
    assert!(!qualify_footer_storage(&client, &format!("default.{table}_absent")).await);
    client
        .query(&format!("DROP TABLE default.{table}"))
        .execute()
        .await
        .unwrap();
}

