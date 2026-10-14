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

#[derive(Serialize, clickhouse::Row)]
struct BoundaryStake {
    boundary_rows: u64,
    stake_rows: u64,
}

/// The stake check of an undeclared boundary with one stored row.
fn boundary_stake(holds_stake: bool) -> [BoundaryStake; 1] {
    [BoundaryStake {
        boundary_rows: 1,
        stake_rows: u64::from(holds_stake),
    }]
}

#[derive(Serialize, clickhouse::Row)]
struct LayoutRow {
    slot: u64,
    block_height: Option<u64>,
    stake_rewards: u64,
}

/// Sums the exported lookup counter for `operation="partitioned",outcome="invalid_derived_partitions"`.
fn invalid_derived_partition_lookups() -> u64 {
    let text = String::from_utf8(metrics::export_metrics().expect("metrics export"))
        .expect("metrics should be valid UTF-8");
    text.lines()
        .filter(|line| {
            line.starts_with("superbank_rpc_inflation_reward_lookups_total_total{")
                && line.contains("operation=\"partitioned\"")
                && line.contains("outcome=\"invalid_derived_partitions\"")
        })
        .map(|line| {
            line.rsplit(' ')
                .next()
                .and_then(|value| value.parse::<u64>().ok())
                .expect("counter value")
        })
        .sum()
}

#[derive(Serialize, clickhouse::Row)]
struct SampleRow {
    slot: u64,
    sample: Vec<Array<u8, 32>>,
}

/// A boundary that has a block height but lost its declared partition count.
fn undeclared_boundary() -> Boundary {
    Boundary {
        block_height: Some(BOUNDARY_HEIGHT),
        ..boundary(None)
    }
}

/// `partitions` stake blocks after the boundary, then one block without stake rewards.
fn layout(partitions: u64) -> Vec<LayoutRow> {
    (0..=partitions)
        .map(|index| LayoutRow {
            slot: BOUNDARY_SLOT + 1 + index,
            block_height: Some(BOUNDARY_HEIGHT + 1 + index),
            stake_rewards: u64::from(index < partitions) * 3,
        })
        .collect()
}

/// One sampled stake pubkey per partition block, hashed with the fixture seed.
fn samples(partitions: u64) -> Vec<SampleRow> {
    (0..partitions)
        .map(|index| {
            let pubkey = (0..=u8::MAX)
                .find(|value| partition_slot_for(*value, partitions) == BOUNDARY_SLOT + 1 + index)
                .expect("a fixture pubkey for every partition");
            SampleRow {
                slot: BOUNDARY_SLOT + 1 + index,
                sample: vec![Array([pubkey; 32])],
            }
        })
        .collect()
}

/// Queues the boundary, stake check, layout and sample queries that derive `partitions`.
fn add_derivation(mock: &Mock, partitions: u64) {
    mock.add(handlers::provide([undeclared_boundary()]));
    mock.add(handlers::provide(boundary_stake(false)));
    mock.add(handlers::provide(layout(partitions)));
    mock.add(handlers::provide(samples(partitions)));
}

#[tokio::test]
async fn undeclared_partition_count_is_derived_and_matches_a_declared_count() {
    const PARTITIONS: u64 = 4;
    let stake_slot = partition_slot_for(2, PARTITIONS);

    let declared_mock = Mock::new();
    let declared_state = initialized_state(&declared_mock).await;
    declared_mock.add(handlers::provide([partitioned_boundary(PARTITIONS)]));
    declared_mock.add(handlers::provide(Vec::<Reward>::new()));
    declared_mock.add(handlers::provide(partition_slots(PARTITIONS)));
    declared_mock.add(handlers::provide([reward(2, stake_slot, 77)]));
    let declared = rewards_body(&declared_state).await;
    assert_eq!(declared["result"][0]["effectiveSlot"], stake_slot);
    assert_eq!(declared["result"][0]["amount"], 77);

    // Cold: the derivation queries run once, then the declared-count queries.
    let mock = Mock::new();
    let state = initialized_state(&mock).await;
    add_derivation(&mock, PARTITIONS);
    mock.add(handlers::provide(Vec::<Reward>::new()));
    mock.add(handlers::provide(partition_slots(PARTITIONS)));
    mock.add(handlers::provide([reward(2, stake_slot, 77)]));
    let cold = rewards_body(&state).await;
    assert_eq!(cold, declared);

    // Warm: the derived count and partition map are cached; one folded query remains.
    mock.add(handlers::provide([reward(2, stake_slot, 77)]));
    let warm = rewards_body(&state).await;
    assert_eq!(warm, declared);
}

#[tokio::test]
async fn undeclared_partition_count_is_derived_without_the_epoch_cache() {
    const PARTITIONS: u64 = 4;
    let stake_slot = partition_slot_for(2, PARTITIONS);
    let mock = Mock::new();
    let state = uncached_state(&mock).await;
    for _ in 0..2 {
        add_derivation(&mock, PARTITIONS);
        mock.add(handlers::provide(Vec::<Reward>::new()));
        mock.add(handlers::provide(partition_slots(PARTITIONS)));
        mock.add(handlers::provide([reward(2, stake_slot, 77)]));
        let body = rewards_body(&state).await;
        assert_eq!(body["result"][0]["effectiveSlot"], stake_slot);
        assert_eq!(body["result"][1], Value::Null);
    }
}

#[tokio::test]
async fn invalid_derived_partition_layout_is_an_uncached_database_error() {
    const PARTITIONS: u64 = 4;
    let mock = Mock::new();
    let state = initialized_state(&mock).await;
    let assert_database_error = |response: Response| async move {
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = parse_json_value_response(response).await;
        assert_eq!(body["error"]["code"], -32603);
    };
    let errors_before = invalid_derived_partition_lookups();

    // A stake reward paid in another partition's block.
    let mut misplaced = samples(PARTITIONS);
    let foreign = misplaced[3].sample[0].0;
    misplaced[1].sample.push(Array(foreign));
    mock.add(handlers::provide([undeclared_boundary()]));
    mock.add(handlers::provide(boundary_stake(false)));
    mock.add(handlers::provide(layout(PARTITIONS)));
    mock.add(handlers::provide(misplaced));
    assert_database_error(handle_json_rpc_value(state.clone(), &request()).await).await;

    // A run whose end has not landed cannot prove the count; no sample query is sent.
    let mut unfinished = layout(PARTITIONS);
    unfinished.pop();
    mock.add(handlers::provide([undeclared_boundary()]));
    mock.add(handlers::provide(boundary_stake(false)));
    mock.add(handlers::provide(unfinished));
    assert_database_error(handle_json_rpc_value(state.clone(), &request()).await).await;

    // Unmerged boundary copies that disagree on stake rewards; no layout query is sent.
    mock.add(handlers::provide([undeclared_boundary()]));
    mock.add(handlers::provide([BoundaryStake {
        boundary_rows: 2,
        stake_rows: 1,
    }]));
    assert_database_error(handle_json_rpc_value(state.clone(), &request()).await).await;
    // Other tests share the global registry, so the counter grows by at least this test's errors.
    assert!(invalid_derived_partition_lookups() >= errors_before + 3);

    // Nothing was cached: a valid layout is derived from a fresh boundary read.
    let stake_slot = partition_slot_for(2, PARTITIONS);
    add_derivation(&mock, PARTITIONS);
    mock.add(handlers::provide(Vec::<Reward>::new()));
    mock.add(handlers::provide(partition_slots(PARTITIONS)));
    mock.add(handlers::provide([reward(2, stake_slot, 77)]));
    let body = rewards_body(&state).await;
    assert_eq!(body["result"][0]["effectiveSlot"], stake_slot);
}

#[tokio::test]
async fn non_partitioned_epoch_with_block_height_keeps_boundary_stake_rewards() {
    let mock = Mock::new();
    let state = initialized_state(&mock).await;
    // The boundary holds stake rewards, so the epoch is not partitioned and no layout is scanned.
    mock.add(handlers::provide([undeclared_boundary()]));
    mock.add(handlers::provide(boundary_stake(true)));
    mock.add(handlers::provide([reward(2, BOUNDARY_SLOT, 42)]));
    let first = rewards_body(&state).await;
    let expected = json!({"epoch": 43, "effectiveSlot": BOUNDARY_SLOT,
        "amount": 42, "postBalance": 100, "commission": null, "commissionBps": 250});
    assert_eq!(first["result"], json!([expected, null, expected]));

    // The non-partitioned boundary is cached; the stake check does not run again.
    mock.add(handlers::provide([reward(2, BOUNDARY_SLOT, 42)]));
    let second = rewards_body(&state).await;
    assert_eq!(second, first);
}

#[tokio::test]
async fn epoch_without_stake_rewards_needs_the_next_block() {
    let mock = Mock::new();
    let state = initialized_state(&mock).await;
    let fees_only = |first_index: u64| {
        (first_index..3)
            .map(|index| LayoutRow {
                slot: BOUNDARY_SLOT + 1 + index,
                block_height: Some(BOUNDARY_HEIGHT + 1 + index),
                stake_rewards: 0,
            })
            .collect::<Vec<_>>()
    };
    let errors_before = invalid_derived_partition_lookups();

    // The block at boundary height + 1 is missing, so missing payouts cannot be told apart.
    mock.add(handlers::provide([undeclared_boundary()]));
    mock.add(handlers::provide(boundary_stake(false)));
    mock.add(handlers::provide(fees_only(1)));
    let response = handle_json_rpc_value(state.clone(), &request()).await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = parse_json_value_response(response).await;
    assert_eq!(body["error"]["code"], -32603);
    assert!(invalid_derived_partition_lookups() > errors_before);

    // Once it is stored with no stake rewards, the epoch is not partitioned and is cached.
    mock.add(handlers::provide([undeclared_boundary()]));
    mock.add(handlers::provide(boundary_stake(false)));
    mock.add(handlers::provide(fees_only(0)));
    mock.add(handlers::provide([reward(2, BOUNDARY_SLOT, 42)]));
    let first = rewards_body(&state).await;
    assert_eq!(first["result"][0]["effectiveSlot"], BOUNDARY_SLOT);
    assert_eq!(first["result"][1], Value::Null);

    mock.add(handlers::provide([reward(2, BOUNDARY_SLOT, 42)]));
    let second = rewards_body(&state).await;
    assert_eq!(second, first);
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
