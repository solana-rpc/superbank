// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use super::*;
use clickhouse::test::{Mock, handlers};
use serde::Serialize;
use serde_big_array::Array;

#[derive(Serialize, clickhouse::Row)]
struct Boundary {
    slot: u64,
    parent_slot: u64,
    parent_blockhash: Array<u8, 32>,
    block_height: Option<u64>,
    rewards_num_partitions: Option<u64>,
}

#[derive(Serialize, clickhouse::Row)]
struct Reward {
    pubkey: Array<u8, 32>,
    effective_slot: u64,
    lamports: i64,
    post_balance: u64,
    commission: Option<u8>,
    commission_bps: Option<u16>,
}

async fn initialized_state(mock: &Mock) -> Arc<AppState> {
    #[derive(Serialize, clickhouse::Row)]
    struct Discovery {
        node: String,
        expected: u64,
        coordinator: u8,
    }
    #[derive(Serialize, clickhouse::Row)]
    struct Probe {
        node: String,
        active_id: String,
    }
    mock.add(handlers::provide([Discovery {
        node: "rewards-fixture".into(),
        expected: 1,
        coordinator: 1,
    }]));
    mock.add(handlers::provide([Probe {
        node: "rewards-fixture".into(),
        active_id: String::new(),
    }]));
    let state = test_state_with_emit_http_errors(test_state_with_clickhouse_url(mock.url()));
    state
        .clickhouse
        .initialize_read_cancellation()
        .await
        .expect("reward fixture cancellation preflight");
    state
}

fn boundary(partitions: Option<u64>) -> Boundary {
    Boundary {
        slot: 19_008_000,
        parent_slot: 19_007_999,
        parent_blockhash: Array([1; 32]),
        block_height: None,
        rewards_num_partitions: partitions,
    }
}

fn request() -> Value {
    let address = crate::solana_sdk::pubkey::Pubkey::new_from_array([2; 32]).to_string();
    let missing = crate::solana_sdk::pubkey::Pubkey::new_from_array([3; 32]).to_string();
    json!({"jsonrpc": "2.0", "id": 1, "method": "getInflationReward",
        "params": [[address, missing, address], {"epoch": 43}]})
}

#[tokio::test]
async fn non_partitioned_rewards_do_not_require_block_height() {
    let mock = Mock::new();
    let state = initialized_state(&mock).await;
    mock.add(handlers::provide([boundary(None)]));
    mock.add(handlers::provide([Reward {
        pubkey: Array([2; 32]),
        effective_slot: 19_008_000,
        lamports: 42,
        post_balance: 100,
        commission: None,
        commission_bps: None,
    }]));
    let response = handle_json_rpc_value(state, &request()).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = parse_json_value_response(response).await;
    let reward = json!({"epoch": 43, "effectiveSlot": 19_008_000,
        "amount": 42, "postBalance": 100, "commission": null});
    assert_eq!(
        body,
        json!({"jsonrpc": "2.0", "id": 1,
        "result": [reward, null, reward]})
    );
}

#[tokio::test]
async fn partitioned_rewards_still_require_block_height() {
    let mock = Mock::new();
    let state = initialized_state(&mock).await;
    mock.add(handlers::provide([boundary(Some(1))]));
    mock.add(handlers::provide(Vec::<Reward>::new()));
    let response = handle_json_rpc_value(state, &request()).await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = parse_json_value_response(response).await;
    assert_eq!(body["error"]["code"], -32603);
}

#[derive(Serialize, clickhouse::Row)]
struct PartitionSlot {
    slot: u64,
    block_height: Option<u64>,
}

const BOUNDARY_SLOT: u64 = 19_008_000;
const BOUNDARY_HEIGHT: u64 = 1_000;

fn partitioned_boundary(partitions: u64) -> Boundary {
    Boundary {
        block_height: Some(BOUNDARY_HEIGHT),
        ..boundary(Some(partitions))
    }
}

fn reward(pubkey: u8, slot: u64, lamports: i64) -> Reward {
    Reward {
        pubkey: Array([pubkey; 32]),
        effective_slot: slot,
        lamports,
        post_balance: 100,
        commission: None,
        commission_bps: Some(250),
    }
}

/// Partition slot `BOUNDARY_SLOT + 1 + index` holds block height `BOUNDARY_HEIGHT + 1 + index`.
fn partition_slots(partitions: u64) -> Vec<PartitionSlot> {
    (0..partitions)
        .map(|index| PartitionSlot {
            slot: BOUNDARY_SLOT + 1 + index,
            block_height: Some(BOUNDARY_HEIGHT + 1 + index),
        })
        .collect()
}

fn partition_slot_for(pubkey: u8, partitions: u64) -> u64 {
    let partition = solana_epoch_rewards_hasher::EpochRewardsHasher::new(
        partitions as usize,
        &crate::solana_sdk::hash::Hash::new_from_array([1; 32]),
    )
    .hash_address_to_partition(&crate::solana_sdk::pubkey::Pubkey::new_from_array(
        [pubkey; 32],
    ));
    BOUNDARY_SLOT + 1 + partition as u64
}

async fn rewards_body(state: &Arc<AppState>) -> Value {
    let response = handle_json_rpc_value(state.clone(), &request()).await;
    assert_eq!(response.status(), StatusCode::OK);
    parse_json_value_response(response).await
}

async fn uncached_state(mock: &Mock) -> Arc<AppState> {
    let state = initialized_state(mock).await;
    let mut state = match Arc::try_unwrap(state) {
        Ok(state) => state,
        Err(_) => panic!("reward fixture state should have a single Arc owner"),
    };
    state.clickhouse.disable_inflation_epoch_cache();
    Arc::new(state)
}

#[tokio::test]
async fn non_partitioned_repeat_skips_boundary_query() {
    let mock = Mock::new();
    let state = initialized_state(&mock).await;
    mock.add(handlers::provide([boundary(None)]));
    mock.add(handlers::provide([reward(2, BOUNDARY_SLOT, 42)]));
    let first = rewards_body(&state).await;

    // Second request: only the boundary reward query reaches ClickHouse.
    mock.add(handlers::provide([reward(2, BOUNDARY_SLOT, 42)]));
    let second = rewards_body(&state).await;
    assert_eq!(first, second);
    assert_eq!(first["result"][0]["amount"], 42);
    assert_eq!(first["result"][1], Value::Null);
}

#[tokio::test]
async fn completed_partitioned_epoch_is_served_in_one_query_with_identical_results() {
    const PARTITIONS: u64 = 4;
    let stake_slot = partition_slot_for(2, PARTITIONS);

    // Reference: cache disabled, sequential four-query lookup.
    let reference_mock = Mock::new();
    let reference_state = uncached_state(&reference_mock).await;
    reference_mock.add(handlers::provide([partitioned_boundary(PARTITIONS)]));
    reference_mock.add(handlers::provide(Vec::<Reward>::new()));
    reference_mock.add(handlers::provide(partition_slots(PARTITIONS)));
    reference_mock.add(handlers::provide([reward(2, stake_slot, 77)]));
    let reference = rewards_body(&reference_state).await;
    assert_eq!(reference["result"][0]["effectiveSlot"], stake_slot);
    assert_eq!(reference["result"][0]["commissionBps"], 250);
    assert_eq!(reference["result"][1], Value::Null);

    // Cold cached state: same four queries, then the complete partition map is cached.
    let mock = Mock::new();
    let state = initialized_state(&mock).await;
    mock.add(handlers::provide([partitioned_boundary(PARTITIONS)]));
    mock.add(handlers::provide(Vec::<Reward>::new()));
    mock.add(handlers::provide(partition_slots(PARTITIONS)));
    mock.add(handlers::provide([reward(2, stake_slot, 77)]));
    let cold = rewards_body(&state).await;
    assert_eq!(cold, reference);

    // Warm: one folded query returns (pubkey, slot) rows.
    mock.add(handlers::provide([reward(2, stake_slot, 77)]));
    let warm = rewards_body(&state).await;
    assert_eq!(warm, reference);
}

#[tokio::test]
async fn active_rewards_period_is_never_cached() {
    const PARTITIONS: u64 = 2;
    let mock = Mock::new();
    let state = initialized_state(&mock).await;
    let progress = || {
        handlers::provide([PartitionSlot {
            slot: BOUNDARY_SLOT,
            block_height: Some(BOUNDARY_HEIGHT),
        }])
    };
    mock.add(handlers::provide([partitioned_boundary(PARTITIONS)]));
    mock.add(handlers::provide(Vec::<Reward>::new()));
    mock.add(handlers::provide(Vec::<PartitionSlot>::new()));
    mock.add(progress());
    let first = rewards_body(&state).await;
    assert_eq!(first["error"]["code"], -32017);
    assert_eq!(
        first["error"]["data"],
        json!({"slot": BOUNDARY_SLOT, "currentBlockHeight": BOUNDARY_HEIGHT,
            "rewardsCompleteBlockHeight": BOUNDARY_HEIGHT + PARTITIONS + 1})
    );

    // The validated boundary is reused; partition availability is re-read.
    mock.add(handlers::provide(Vec::<Reward>::new()));
    mock.add(handlers::provide(Vec::<PartitionSlot>::new()));
    mock.add(progress());
    let second = rewards_body(&state).await;
    assert_eq!(second, first);

    // Once every partition landed, the answer switches to rewards without a restart.
    mock.add(handlers::provide(Vec::<Reward>::new()));
    mock.add(handlers::provide(partition_slots(PARTITIONS)));
    mock.add(handlers::provide(Vec::<Reward>::new()));
    let third = rewards_body(&state).await;
    assert_eq!(third["result"], json!([null, null, null]));
}

#[tokio::test]
async fn incomplete_partition_map_is_not_cached() {
    const PARTITIONS: u64 = 64;
    let stake_slot = partition_slot_for(2, PARTITIONS);
    let missing_slot = partition_slot_for(3, PARTITIONS);
    // Only the two requested partitions exist; the remaining partitions have not landed.
    let mut landed = vec![PartitionSlot {
        slot: stake_slot,
        block_height: Some(BOUNDARY_HEIGHT + (stake_slot - BOUNDARY_SLOT)),
    }];
    if missing_slot != stake_slot {
        landed.push(PartitionSlot {
            slot: missing_slot,
            block_height: Some(BOUNDARY_HEIGHT + (missing_slot - BOUNDARY_SLOT)),
        });
    }
    let rows = || {
        landed
            .iter()
            .map(|row| PartitionSlot {
                slot: row.slot,
                block_height: row.block_height,
            })
            .collect::<Vec<_>>()
    };

    let mock = Mock::new();
    let state = initialized_state(&mock).await;
    mock.add(handlers::provide([partitioned_boundary(PARTITIONS)]));
    mock.add(handlers::provide(Vec::<Reward>::new()));
    mock.add(handlers::provide(rows()));
    mock.add(handlers::provide([reward(2, stake_slot, 5)]));
    let first = rewards_body(&state).await;
    assert_eq!(first["result"][0]["amount"], 5);

    // Boundary cached, partition map not: three queries, same answer.
    mock.add(handlers::provide(Vec::<Reward>::new()));
    mock.add(handlers::provide(rows()));
    mock.add(handlers::provide([reward(2, stake_slot, 5)]));
    let second = rewards_body(&state).await;
    assert_eq!(second, first);
}

#[tokio::test]
async fn boundary_unavailable_is_never_cached() {
    let mock = Mock::new();
    let state = initialized_state(&mock).await;
    mock.add(handlers::provide(Vec::<Boundary>::new()));
    let first = rewards_body(&state).await;
    assert_eq!(first["error"]["code"], -32004);

    mock.add(handlers::provide(Vec::<Boundary>::new()));
    let second = rewards_body(&state).await;
    assert_eq!(second, first);

    // Once the boundary lands it is served.
    mock.add(handlers::provide([boundary(None)]));
    mock.add(handlers::provide([reward(2, BOUNDARY_SLOT, 42)]));
    let third = rewards_body(&state).await;
    assert_eq!(third["result"][0]["amount"], 42);
}

#[tokio::test]
async fn partitioned_boundary_without_block_height_is_not_cached() {
    let mock = Mock::new();
    let state = initialized_state(&mock).await;
    for _ in 0..2 {
        mock.add(handlers::provide([boundary(Some(1))]));
        mock.add(handlers::provide(Vec::<Reward>::new()));
        let response = handle_json_rpc_value(state.clone(), &request()).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
