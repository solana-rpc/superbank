// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use super::*;
use clickhouse::test::{Mock, handlers};
use serde::Serialize;
use serde_big_array::Array;
use std::sync::atomic::AtomicUsize;

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

const STAKE_TYPES: &str =
    "lowerUTF8(ifNull(reward.4, '')) IN ('staking', 'deactivatedstake', 'deactivated-stake')";
const VOTE_TYPES: &str = "lowerUTF8(ifNull(reward.4, '')) IN ('voting')";

fn assert_never_selects_vat(query: &str) {
    for excluded in [
        "'vatdebit'",
        "'validator-admission-ticket-debit'",
        "'fee'",
        "'rent'",
    ] {
        assert!(!query.contains(excluded), "{excluded} in {query}");
    }
}

#[tokio::test]
async fn non_partitioned_boundary_query_selects_both_deactivated_stake_spellings() {
    let mock = Mock::new();
    let state = initialized_state(&mock).await;
    mock.add(handlers::provide([boundary(None)]));
    let boundary_rewards = mock.add(handlers::record_ddl());
    assert_eq!(
        rewards_body(&state).await["result"],
        json!([null, null, null])
    );
    let query = boundary_rewards.query().await;
    assert!(
        query.contains("IN ('staking', 'deactivatedstake', 'deactivated-stake', 'voting')"),
        "{query}"
    );
    assert_never_selects_vat(&query);
}

#[tokio::test]
async fn partitioned_queries_select_deactivated_stake_only_from_partition_blocks() {
    const PARTITIONS: u64 = 4;
    let mock = Mock::new();
    let state = initialized_state(&mock).await;

    // Cold sequential path: boundary votes only, then stake types from partition blocks.
    mock.add(handlers::provide([partitioned_boundary(PARTITIONS)]));
    let boundary_rewards = mock.add(handlers::record_ddl());
    mock.add(handlers::provide(partition_slots(PARTITIONS)));
    let partition_rewards = mock.add(handlers::record_ddl());
    let cold = rewards_body(&state).await;
    assert_eq!(cold["result"], json!([null, null, null]));
    let boundary_query = boundary_rewards.query().await;
    assert!(boundary_query.contains(VOTE_TYPES), "{boundary_query}");
    assert!(!boundary_query.contains("staking"), "{boundary_query}");
    assert!(!boundary_query.contains("deactivated"), "{boundary_query}");
    let partition_query = partition_rewards.query().await;
    assert!(partition_query.contains(STAKE_TYPES), "{partition_query}");
    assert!(!partition_query.contains("'voting'"), "{partition_query}");
    assert_never_selects_vat(&partition_query);

    // Warm folded path keeps the same per-slot reward types.
    let folded_rewards = mock.add(handlers::record_ddl());
    assert_eq!(rewards_body(&state).await, cold);
    let folded_query = folded_rewards.query().await;
    assert!(
        folded_query.contains(&format!(
            "((slot = {BOUNDARY_SLOT} AND {VOTE_TYPES}) OR (slot != {BOUNDARY_SLOT} AND {STAKE_TYPES}))"
        )),
        "{folded_query}"
    );
    assert_never_selects_vat(&folded_query);
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

// Response cache (`GET_INFLATION_REWARD_RESPONSE_CACHE_*`).

const TEST_SETTLE_SLOTS: u64 = 1_000;
const SETTLED_TIP: u64 = BOUNDARY_SLOT + 1_000_000;

fn pubkey_string(byte: u8) -> String {
    crate::solana_sdk::pubkey::Pubkey::new_from_array([byte; 32]).to_string()
}

fn request_for(addresses: &[u8]) -> Value {
    let addresses = addresses
        .iter()
        .map(|byte| pubkey_string(*byte))
        .collect::<Vec<_>>();
    json!({"jsonrpc": "2.0", "id": 1, "method": "getInflationReward",
        "params": [addresses, {"epoch": 43}]})
}

async fn cached_state_with(mock: &Mock, max_bytes: u64, ttl: Duration, tip: u64) -> Arc<AppState> {
    Arc::new(cached_app_state(mock, max_bytes, ttl, tip).await)
}

/// Cache on, method admission wide enough for concurrent requests, and a known finalized tip.
async fn cached_app_state(mock: &Mock, max_bytes: u64, ttl: Duration, tip: u64) -> AppState {
    let state = initialized_state(mock).await;
    let mut state = match Arc::try_unwrap(state) {
        Ok(state) => state,
        Err(_) => panic!("reward fixture state should have a single Arc owner"),
    };
    state
        .clickhouse
        .enable_inflation_response_cache(max_bytes, ttl, TEST_SETTLE_SLOTS);
    state
        .clickhouse
        .set_latest_finalized_slot_for_tests(Some(tip));
    state.latest_slot_cache.value.store(tip, Ordering::Relaxed);
    state
        .latest_slot_cache
        .last_updated_ms
        .store(current_time_millis(), Ordering::Relaxed);
    state.get_inflation_reward_sem = Some(Arc::new(Semaphore::new(8)));
    state
}

async fn cached_state(mock: &Mock) -> Arc<AppState> {
    cached_state_with(mock, 1 << 20, Duration::from_secs(3_600), SETTLED_TIP).await
}

struct RawResponse {
    status: StatusCode,
    sources: String,
    metrics: String,
    body: Bytes,
}

async fn raw_response(state: &Arc<AppState>, request: &Value) -> RawResponse {
    let response = handle_json_rpc_value(state.clone(), request).await;
    let header = |name: &str| {
        response
            .headers()
            .get(name)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string()
    };
    let (status, sources, metrics) = (
        response.status(),
        header("X-Superbank-Sources"),
        header("X-Superbank-Metrics"),
    );
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    RawResponse {
        status,
        sources,
        metrics,
        body,
    }
}

fn body_json(response: &RawResponse) -> Value {
    serde_json::from_slice(&response.body).expect("json body")
}

fn response_cache_count(outcome: &str) -> u64 {
    let text = String::from_utf8(metrics::export_metrics().expect("metrics export"))
        .expect("metrics should be valid UTF-8");
    let prefix =
        format!("superbank_rpc_inflation_reward_response_cache_total{{outcome=\"{outcome}\"}} ");
    text.lines()
        .find_map(|line| line.strip_prefix(prefix.as_str()))
        .map_or(0, |value| value.trim().parse().expect("counter value"))
}

fn queue_non_partitioned(mock: &Mock) {
    mock.add(handlers::provide([boundary(None)]));
    mock.add(handlers::provide([reward(2, BOUNDARY_SLOT, 42)]));
}

#[tokio::test]
async fn repeated_request_is_served_from_the_response_cache_byte_for_byte() {
    let mock = Mock::new();
    let state = cached_state(&mock).await;
    queue_non_partitioned(&mock);
    let cold = raw_response(&state, &request()).await;
    assert_eq!(cold.status, StatusCode::OK);
    assert_eq!(cold.sources, "clickhouse");

    // No mock handler is queued: any ClickHouse query would fail the request and the mock.
    let warm = raw_response(&state, &request()).await;
    assert_eq!(warm.status, StatusCode::OK);
    assert_eq!(warm.body, cold.body);
    assert_eq!(warm.sources, "response-cache");
    assert_eq!(
        warm.metrics,
        "rows_read=0;rows_returned=0;data_read_bytes=0"
    );
    state
        .clickhouse
        .inflation_response_cache
        .run_pending_tasks()
        .await;
    assert_eq!(state.clickhouse.inflation_response_cache.entry_count(), 1);

    let text = String::from_utf8(metrics::export_metrics().expect("metrics export"))
        .expect("metrics should be valid UTF-8");
    assert!(
        text.lines()
            .any(|line| line.starts_with("superbank_rpc_route_total_total{")
                && line.contains("method=\"getInflationReward\"")
                && line.contains("source=\"response_cache\"")
                && line.contains("outcome=\"success\"")),
        "a hit must be routed as response_cache: {text}"
    );
}

#[tokio::test]
async fn completed_partitioned_answer_is_served_from_the_response_cache() {
    const PARTITIONS: u64 = 4;
    let stake_slot = partition_slot_for(2, PARTITIONS);
    let mock = Mock::new();
    let state = cached_state(&mock).await;
    mock.add(handlers::provide([partitioned_boundary(PARTITIONS)]));
    mock.add(handlers::provide(Vec::<Reward>::new()));
    mock.add(handlers::provide(partition_slots(PARTITIONS)));
    mock.add(handlers::provide([reward(2, stake_slot, 77)]));
    let cold = raw_response(&state, &request()).await;
    assert_eq!(body_json(&cold)["result"][0]["effectiveSlot"], stake_slot);

    let warm = raw_response(&state, &request()).await;
    assert_eq!(warm.body, cold.body);
    assert_eq!(warm.sources, "response-cache");
}

#[tokio::test]
async fn reordered_set_with_duplicates_hits_and_keeps_request_shape() {
    let mock = Mock::new();
    let state = cached_state(&mock).await;
    queue_non_partitioned(&mock);
    let cold = raw_response(&state, &request_for(&[2, 3, 2])).await;
    let reward = json!({"epoch": 43, "effectiveSlot": BOUNDARY_SLOT,
        "amount": 42, "postBalance": 100, "commission": null, "commissionBps": 250});
    assert_eq!(body_json(&cold)["result"], json!([reward, null, reward]));

    let reordered = raw_response(&state, &request_for(&[3, 2, 3, 3, 2])).await;
    assert_eq!(reordered.sources, "response-cache");
    assert_eq!(
        body_json(&reordered)["result"],
        json!([null, reward, null, null, reward])
    );
}

#[tokio::test]
async fn hit_attaches_no_downstream_timings() {
    let mock = Mock::new();
    let state = cached_state(&mock).await;
    let params = || {
        Some(vec![
            json!([pubkey_string(2), pubkey_string(3)]),
            json!({ "epoch": 43 }),
        ])
    };
    queue_non_partitioned(&mock);
    let cold = handle_get_inflation_reward(state.clone(), json!(1), params())
        .await
        .expect("cold response");
    assert!(crate::util::extract_downstream_timings(&cold).is_some());

    let warm = handle_get_inflation_reward(state.clone(), json!(1), params())
        .await
        .expect("warm response");
    assert!(crate::util::extract_downstream_timings(&warm).is_none());
    let warm = parse_json_value_response(warm).await;
    assert_eq!(warm["result"][0]["amount"], 42);
}

#[tokio::test]
async fn unavailable_answers_are_shared_but_never_stored() {
    let mock = Mock::new();
    let state = cached_state(&mock).await;
    for _ in 0..2 {
        mock.add(handlers::provide(Vec::<Boundary>::new()));
        let unavailable = raw_response(&state, &request()).await;
        assert_eq!(body_json(&unavailable)["error"]["code"], -32004);
    }

    // The boundary lands: the answer switches without a restart, then is cached.
    queue_non_partitioned(&mock);
    let landed = raw_response(&state, &request()).await;
    assert_eq!(body_json(&landed)["result"][0]["amount"], 42);
    let cached = raw_response(&state, &request()).await;
    assert_eq!(cached.body, landed.body);
    assert_eq!(cached.sources, "response-cache");
}

#[tokio::test]
async fn active_rewards_period_is_never_stored() {
    const PARTITIONS: u64 = 2;
    let mock = Mock::new();
    let state = cached_state(&mock).await;
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
    let first = raw_response(&state, &request()).await;
    assert_eq!(body_json(&first)["error"]["code"], -32017);

    mock.add(handlers::provide(Vec::<Reward>::new()));
    mock.add(handlers::provide(Vec::<PartitionSlot>::new()));
    mock.add(progress());
    let second = raw_response(&state, &request()).await;
    assert_eq!(second.body, first.body);
    assert_eq!(second.sources, "clickhouse");

    mock.add(handlers::provide(Vec::<Reward>::new()));
    mock.add(handlers::provide(partition_slots(PARTITIONS)));
    mock.add(handlers::provide(Vec::<Reward>::new()));
    let complete = raw_response(&state, &request()).await;
    assert_eq!(body_json(&complete)["result"], json!([null, null, null]));
    let cached = raw_response(&state, &request()).await;
    assert_eq!(cached.body, complete.body);
    assert_eq!(cached.sources, "response-cache");
}

#[tokio::test]
async fn internal_errors_are_never_stored() {
    let mock = Mock::new();
    let state = cached_state(&mock).await;
    // A partitioned boundary without a block height fails after both reads complete.
    for _ in 0..2 {
        mock.add(handlers::provide([boundary(Some(1))]));
        mock.add(handlers::provide(Vec::<Reward>::new()));
        let failed = raw_response(&state, &request()).await;
        assert_eq!(failed.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body_json(&failed)["error"]["code"], -32603);
    }

    queue_non_partitioned(&mock);
    let recovered = raw_response(&state, &request()).await;
    assert_eq!(body_json(&recovered)["result"][0]["amount"], 42);
    let cached = raw_response(&state, &request()).await;
    assert_eq!(cached.body, recovered.body);
    assert_eq!(cached.sources, "response-cache");
}

async fn concurrent_pair(state: &Arc<AppState>, request: &Value) -> [RawResponse; 2] {
    let (first, second) = tokio::join!(raw_response(state, request), raw_response(state, request));
    [first, second]
}

fn sorted_sources(pair: &[RawResponse; 2]) -> Vec<&str> {
    let mut sources = pair.iter().map(|r| r.sources.as_str()).collect::<Vec<_>>();
    sources.sort_unstable();
    sources
}

#[tokio::test]
async fn concurrent_identical_requests_run_one_lookup() {
    let mock = Mock::new();
    let state = cached_state(&mock).await;
    // Without an epoch cache entry the requests coalesce on the cold lookup.
    queue_non_partitioned(&mock);
    let cold = concurrent_pair(&state, &request_for(&[2, 3])).await;
    assert_eq!(cold[0].body, cold[1].body);
    assert_eq!(sorted_sources(&cold), ["clickhouse", "response-cache"]);

    // With the boundary cached they coalesce on the epoch-cache key: one reward query.
    mock.add(handlers::provide(Vec::<Reward>::new()));
    let warm = concurrent_pair(&state, &request_for(&[4, 5])).await;
    assert_eq!(warm[0].body, warm[1].body);
    assert_eq!(body_json(&warm[0])["result"], json!([null, null]));
}

#[tokio::test]
async fn concurrent_requests_share_answers_that_are_not_stored() {
    // Unsettled answers and -32004 are never stored, so a second lookup would need its own
    // mock handlers: one queued set proves the waiter shared the leader's answer.
    let mock = Mock::new();
    let state = cached_state_with(&mock, 1 << 20, Duration::from_secs(3_600), BOUNDARY_SLOT).await;
    queue_non_partitioned(&mock);
    let cold = concurrent_pair(&state, &request()).await;
    assert_eq!(cold[0].body, cold[1].body);
    assert_eq!(body_json(&cold[0])["result"][1], Value::Null);
    assert_eq!(sorted_sources(&cold), ["clickhouse", "response-cache"]);

    mock.add(handlers::provide([reward(2, BOUNDARY_SLOT, 42)]));
    let warm = concurrent_pair(&state, &request()).await;
    assert_eq!(warm[0].body, cold[0].body);
    assert_eq!(sorted_sources(&warm), ["clickhouse", "response-cache"]);

    let mock = Mock::new();
    let state = cached_state(&mock).await;
    mock.add(handlers::provide(Vec::<Boundary>::new()));
    let unavailable = concurrent_pair(&state, &request()).await;
    for response in &unavailable {
        assert_eq!(body_json(response)["error"]["code"], -32004);
    }
    assert_eq!(
        sorted_sources(&unavailable),
        ["clickhouse", "response-cache"]
    );
}

#[tokio::test]
async fn waiter_retries_a_failed_leader_once_without_coalescing() {
    let mock = Mock::new();
    let state = cached_state(&mock).await;
    let retries_before = response_cache_count("retry");
    // The leader fails (partitioned boundary without a block height); the waiter's retry succeeds.
    mock.add(handlers::provide([boundary(Some(1))]));
    mock.add(handlers::provide(Vec::<Reward>::new()));
    queue_non_partitioned(&mock);
    let pair = concurrent_pair(&state, &request()).await;
    let mut codes = pair
        .iter()
        .map(|r| body_json(r)["error"]["code"].as_i64())
        .collect::<Vec<_>>();
    codes.sort_unstable();
    assert_eq!(codes, [None, Some(-32603)]);
    assert!(pair.iter().all(|r| r.sources == "clickhouse"));
    assert!(response_cache_count("retry") > retries_before);
}

#[tokio::test]
async fn empty_addresses_bypass_the_response_cache() {
    let mock = Mock::new();
    let state = cached_state(&mock).await;
    for _ in 0..2 {
        let empty = raw_response(&state, &request_for(&[])).await;
        assert_eq!(body_json(&empty)["result"], json!([]));
        assert_eq!(empty.sources, "clickhouse");
    }
    state
        .clickhouse
        .inflation_response_cache
        .run_pending_tasks()
        .await;
    assert_eq!(state.clickhouse.inflation_response_cache.entry_count(), 0);
}

#[tokio::test]
async fn method_admission_runs_before_the_response_cache() {
    let mock = Mock::new();
    let state = cached_state(&mock).await;
    queue_non_partitioned(&mock);
    let cold = raw_response(&state, &request()).await;
    assert_eq!(cold.status, StatusCode::OK);

    let semaphore = state.get_inflation_reward_sem.clone().expect("admission");
    let _permits = semaphore
        .clone()
        .try_acquire_many_owned(8)
        .expect("all permits");
    let shed = raw_response(&state, &request()).await;
    assert_eq!(body_json(&shed)["error"]["code"], -32005);
    assert_ne!(shed.sources, "response-cache");
}

#[tokio::test]
async fn entries_expire_after_the_ttl() {
    let mock = Mock::new();
    let state = cached_state_with(&mock, 1 << 20, Duration::from_millis(100), SETTLED_TIP).await;
    queue_non_partitioned(&mock);
    let cold = raw_response(&state, &request()).await;
    let warm = raw_response(&state, &request()).await;
    assert_eq!(warm.sources, "response-cache");

    tokio::time::sleep(Duration::from_millis(250)).await;
    // The epoch cache still holds the boundary, so only the reward query runs again.
    mock.add(handlers::provide([reward(2, BOUNDARY_SLOT, 42)]));
    let expired = raw_response(&state, &request()).await;
    assert_eq!(expired.sources, "clickhouse");
    assert_eq!(expired.body, cold.body);
}

#[tokio::test]
async fn cache_is_bounded_by_bytes() {
    // One single-address answer weighs about 380 bytes, so at most two fit.
    const MAX_BYTES: u64 = 1_024;
    let mock = Mock::new();
    let state = cached_state_with(&mock, MAX_BYTES, Duration::from_secs(3_600), SETTLED_TIP).await;
    mock.add(handlers::provide([boundary(None)]));
    for address in 10..16u8 {
        mock.add(handlers::provide([reward(address, BOUNDARY_SLOT, 1)]));
        let response = raw_response(&state, &request_for(&[address])).await;
        assert_eq!(body_json(&response)["result"][0]["amount"], 1);
    }
    let cache = &state.clickhouse.inflation_response_cache;
    cache.run_pending_tasks().await;
    assert!(
        cache.weighted_size() <= MAX_BYTES,
        "{}",
        cache.weighted_size()
    );
    assert!(cache.entry_count() <= 2, "{}", cache.entry_count());
}

#[tokio::test]
async fn settle_gate_stores_nulls_only_once_the_tip_passes_the_margin() {
    let mock = Mock::new();
    // The finalized tip sits inside the margin after the boundary block.
    let state = cached_state_with(&mock, 1 << 20, Duration::from_secs(3_600), BOUNDARY_SLOT).await;
    let unsettled_before = response_cache_count("unsettled");
    queue_non_partitioned(&mock);
    let first = raw_response(&state, &request()).await;
    assert_eq!(body_json(&first)["result"][1], Value::Null);
    // The answer with a null was served but not stored.
    mock.add(handlers::provide([reward(2, BOUNDARY_SLOT, 42)]));
    let second = raw_response(&state, &request()).await;
    assert_eq!(second.sources, "clickhouse");
    assert_eq!(second.body, first.body);
    assert!(response_cache_count("unsettled") >= unsettled_before + 2);

    // Every requested address has a reward: stored inside the margin.
    mock.add(handlers::provide([reward(2, BOUNDARY_SLOT, 42)]));
    let positive = raw_response(&state, &request_for(&[2])).await;
    assert_eq!(positive.sources, "clickhouse");
    let positive_again = raw_response(&state, &request_for(&[2])).await;
    assert_eq!(positive_again.sources, "response-cache");
    assert_eq!(positive_again.body, positive.body);

    // Past the margin the answer with a null is stored too.
    state
        .latest_slot_cache
        .value
        .store(BOUNDARY_SLOT + TEST_SETTLE_SLOTS, Ordering::Relaxed);
    mock.add(handlers::provide([reward(2, BOUNDARY_SLOT, 42)]));
    let settled = raw_response(&state, &request()).await;
    assert_eq!(settled.sources, "clickhouse");
    let cached = raw_response(&state, &request()).await;
    assert_eq!(cached.sources, "response-cache");
    assert_eq!(cached.body, first.body);
}

#[tokio::test]
async fn unknown_tip_stores_only_answers_without_nulls() {
    let mock = Mock::new();
    let mut state = cached_app_state(&mock, 1 << 20, Duration::from_secs(3_600), 0).await;
    state.clickhouse.set_latest_finalized_slot_for_tests(None);
    state
        .latest_slot_cache
        .last_updated_ms
        .store(0, Ordering::Relaxed);
    let state = Arc::new(state);
    queue_non_partitioned(&mock);
    let first = raw_response(&state, &request()).await;
    assert_eq!(body_json(&first)["result"][0]["amount"], 42);
    mock.add(handlers::provide([reward(2, BOUNDARY_SLOT, 42)]));
    let second = raw_response(&state, &request()).await;
    assert_eq!(second.sources, "clickhouse");

    // An answer where every address has a reward does not depend on the tip.
    mock.add(handlers::provide([reward(2, BOUNDARY_SLOT, 42)]));
    let positive = raw_response(&state, &request_for(&[2])).await;
    assert_eq!(positive.sources, "clickhouse");
    let cached = raw_response(&state, &request_for(&[2])).await;
    assert_eq!(cached.sources, "response-cache");
}

/// A rewarded address in the lowest partition and an unrewarded one in the highest.
fn low_and_high_partition_addresses(partitions: u64) -> ((u8, u64), (u8, u64)) {
    let slots = (2..=64u8)
        .map(|byte| (byte, partition_slot_for(byte, partitions)))
        .collect::<Vec<_>>();
    let low = *slots
        .iter()
        .min_by_key(|(_, slot)| *slot)
        .expect("addresses");
    let high = *slots
        .iter()
        .max_by_key(|(_, slot)| *slot)
        .expect("addresses");
    assert!(low.1 < high.1, "fixture addresses must span two partitions");
    (low, high)
}

#[tokio::test]
async fn partitioned_nulls_settle_after_the_last_selected_partition_block() {
    const PARTITIONS: u64 = 4;
    let ((rewarded, low_slot), (missing, high_slot)) = low_and_high_partition_addresses(PARTITIONS);
    let mixed = request_for(&[rewarded, missing]);
    let mock = Mock::new();
    // Past the margin of the boundary and the low partition, one slot short for the high one.
    let tip = high_slot + TEST_SETTLE_SLOTS - 1;
    let state = cached_state_with(&mock, 1 << 20, Duration::from_secs(3_600), tip).await;
    let cache = &state.clickhouse.inflation_response_cache;

    // Sequential lookup: the answer with a null is served but not stored.
    mock.add(handlers::provide([partitioned_boundary(PARTITIONS)]));
    mock.add(handlers::provide(Vec::<Reward>::new()));
    mock.add(handlers::provide(partition_slots(PARTITIONS)));
    mock.add(handlers::provide([reward(rewarded, low_slot, 77)]));
    let sequential = raw_response(&state, &mixed).await;
    assert_eq!(
        body_json(&sequential)["result"][0]["effectiveSlot"],
        low_slot
    );
    assert_eq!(body_json(&sequential)["result"][1], Value::Null);
    cache.run_pending_tasks().await;
    assert_eq!(cache.entry_count(), 0);

    // The epoch cache holds the complete partition map: one folded query, still not stored.
    mock.add(handlers::provide([reward(rewarded, low_slot, 77)]));
    let folded = raw_response(&state, &mixed).await;
    assert_eq!(folded.sources, "clickhouse");
    assert_eq!(folded.body, sequential.body);
    cache.run_pending_tasks().await;
    assert_eq!(cache.entry_count(), 0);

    // A new set on the folded path where every address has a reward is stored at once.
    mock.add(handlers::provide([reward(rewarded, low_slot, 77)]));
    let positive = raw_response(&state, &request_for(&[rewarded])).await;
    assert_eq!(positive.sources, "clickhouse");
    let positive_again = raw_response(&state, &request_for(&[rewarded])).await;
    assert_eq!(positive_again.sources, "response-cache");

    // Once the tip passes the highest selected partition block plus the margin, it is stored.
    state
        .latest_slot_cache
        .value
        .store(high_slot + TEST_SETTLE_SLOTS, Ordering::Relaxed);
    mock.add(handlers::provide([reward(rewarded, low_slot, 77)]));
    let settled = raw_response(&state, &mixed).await;
    assert_eq!(settled.sources, "clickhouse");
    let cached = raw_response(&state, &mixed).await;
    assert_eq!(cached.sources, "response-cache");
    assert_eq!(cached.body, sequential.body);
}

#[tokio::test]
async fn entries_of_a_replaced_boundary_are_not_served() {
    let mock = Mock::new();
    let mut state = cached_app_state(&mock, 1 << 20, Duration::from_secs(3_600), SETTLED_TIP).await;
    state
        .clickhouse
        .set_inflation_epoch_cache_ttl(Duration::from_millis(100));
    let state = Arc::new(state);
    queue_non_partitioned(&mock);
    let original = raw_response(&state, &request()).await;
    assert_eq!(
        raw_response(&state, &request()).await.sources,
        "response-cache"
    );

    // The epoch cache entry expires, and another set re-reads a repaired boundary.
    tokio::time::sleep(Duration::from_millis(250)).await;
    mock.add(handlers::provide([Boundary {
        parent_blockhash: Array([9; 32]),
        ..boundary(None)
    }]));
    mock.add(handlers::provide(Vec::<Reward>::new()));
    let other = raw_response(&state, &request_for(&[4])).await;
    assert_eq!(body_json(&other)["result"], json!([null]));

    // The original set's entry belongs to the old boundary, so it is looked up again.
    mock.add(handlers::provide([reward(2, BOUNDARY_SLOT, 43)]));
    let repaired = raw_response(&state, &request()).await;
    assert_eq!(repaired.sources, "clickhouse");
    assert_eq!(body_json(&repaired)["result"][0]["amount"], 43);
    let cached = raw_response(&state, &request()).await;
    assert_eq!(cached.sources, "response-cache");
    assert_eq!(cached.body, repaired.body);
    assert_ne!(cached.body, original.body);
}

/// Holds every ClickHouse HTTP permit, so a lookup waits until its budget runs out.
async fn hold_http_permits(state: &AppState) -> tokio::sync::OwnedSemaphorePermit {
    let permits = state.clickhouse.http_query_sem.clone();
    let count = u32::try_from(permits.available_permits()).expect("permit count");
    permits
        .acquire_many_owned(count)
        .await
        .expect("hold every HTTP permit")
}

#[tokio::test]
async fn takeover_after_an_aborted_leader_keeps_the_waiters_own_budget() {
    const BUDGET: Duration = Duration::from_millis(1_000);
    let mock = Mock::new();
    let mut state = cached_app_state(&mock, 1 << 20, Duration::from_secs(3_600), SETTLED_TIP).await;
    state.clickhouse.inflation_reward_limits.query_timeout = BUDGET;
    let state = Arc::new(state);
    let _held = hold_http_permits(&state).await;
    let lookup = |state: Arc<AppState>, lookups: Arc<AtomicUsize>| async move {
        state
            .clickhouse
            .get_inflation_rewards_cached(
                &[[2; 32]],
                43,
                &state.epoch_schedule,
                || {
                    lookups.fetch_add(1, Ordering::Relaxed);
                },
                |_| async { true },
            )
            .await
    };
    let leader_lookups = Arc::new(AtomicUsize::new(0));
    let leader = tokio::spawn(lookup(state.clone(), leader_lookups.clone()));
    tokio::time::sleep(Duration::from_millis(50)).await;
    let waiter_lookups = Arc::new(AtomicUsize::new(0));
    let joined = std::time::Instant::now();
    let waiter = tokio::spawn(lookup(state.clone(), waiter_lookups.clone()));
    tokio::time::sleep(Duration::from_millis(550)).await;
    leader.abort();
    let result = waiter.await.expect("waiter task");
    let elapsed = joined.elapsed();

    assert_eq!(leader_lookups.load(Ordering::Relaxed), 1);
    assert_eq!(
        waiter_lookups.load(Ordering::Relaxed),
        1,
        "the waiter takes over"
    );
    assert!(
        matches!(
            result,
            Err(crate::processing::ProcessingError::Timeout { .. })
        ),
        "{result:?}"
    );
    // The waiter's lookup gets what is left of its own budget, not a fresh one.
    assert!(elapsed < BUDGET + Duration::from_millis(300), "{elapsed:?}");
}

#[tokio::test]
async fn a_lookup_cut_off_by_the_request_timeout_still_reports_clickhouse() {
    let mock = Mock::new();
    let mut state = cached_app_state(&mock, 1 << 20, Duration::from_secs(3_600), SETTLED_TIP).await;
    state.rpc_request_timeout = Duration::from_millis(200);
    let state = Arc::new(state);
    let _held = hold_http_permits(&state).await;
    let timed_out = raw_response(&state, &request()).await;
    assert_eq!(body_json(&timed_out)["error"]["code"], -32000);
    assert_eq!(timed_out.sources, "clickhouse");

    // The route metric starts after the dispatch timer, so the drop can count `abandoned`.
    let text = String::from_utf8(metrics::export_metrics().expect("metrics export"))
        .expect("metrics should be valid UTF-8");
    assert!(
        text.lines()
            .any(|line| line.starts_with("superbank_rpc_route_total_total{")
                && line.contains("method=\"getInflationReward\"")
                && line.contains("source=\"clickhouse\"")
                && (line.contains("outcome=\"timeout\"")
                    || line.contains("outcome=\"abandoned\""))),
        "a timed-out lookup must stay routed as clickhouse: {text}"
    );
}
