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
    mock.add(handlers::provide([boundary(None)]));
    mock.add(handlers::provide([Reward {
        pubkey: Array([2; 32]),
        effective_slot: 19_008_000,
        lamports: 42,
        post_balance: 100,
        commission: None,
        commission_bps: None,
    }]));
    let state = test_state_with_emit_http_errors(test_state_with_clickhouse_url(mock.url()));
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
    mock.add(handlers::provide([boundary(Some(1))]));
    mock.add(handlers::provide(Vec::<Reward>::new()));
    let state = test_state_with_emit_http_errors(test_state_with_clickhouse_url(mock.url()));
    let response = handle_json_rpc_value(state, &request()).await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = parse_json_value_response(response).await;
    assert_eq!(body["error"]["code"], -32603);
}
