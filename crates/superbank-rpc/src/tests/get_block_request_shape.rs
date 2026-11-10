// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::metrics::{GetBlockRequestShape, get_block_request_shape_count_for_tests};

// No other test requests this shape, so the shared counters move only here.
fn shape(commitment: &'static str) -> GetBlockRequestShape {
    GetBlockRequestShape {
        encoding: "jsonParsed",
        details: "none",
        rewards: "false",
        msv: "set",
        commitment,
    }
}

fn params(slot: u64, commitment: &str) -> Vec<Value> {
    vec![
        json!(slot),
        json!({
            "encoding": "jsonParsed",
            "transactionDetails": "none",
            "rewards": false,
            "commitment": commitment,
            "maxSupportedTransactionVersion": 0
        }),
    ]
}

fn counts(commitment: &'static str) -> [u64; 5] {
    ["head", "response_cache", "disk", "clickhouse", "other"]
        .map(|tier| get_block_request_shape_count_for_tests(shape(commitment), tier))
}

#[tokio::test]
async fn get_block_request_shape_counts_each_parsed_request_by_serving_tier() {
    let slot = 7_301u64;
    let cache = Arc::new(HeadCache::new(32, TEST_MAX_LIMIT as usize));
    let mut block = base_block_record(slot);
    block.metadata.executed_transaction_count = 0;
    cache.note_block_metadata(block.metadata);
    cache.note_slot_commitment(slot, CommitmentLevel::Finalized);
    let mut state = test_state_with_head_cache_and_clickhouse_url(cache, "http://127.0.0.1:1");
    Arc::get_mut(&mut state)
        .expect("unique state")
        .block_response_cache = BlockResponseCache::new(1024 * 1024);

    let finalized = counts("finalized");
    let response = handle_get_block(state.clone(), json!(1), Some(params(slot, "finalized")))
        .await
        .expect("head response");
    assert!(parse_json_rpc_response(response).await.error.is_none());
    let request = json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "getBlock",
        "params": params(slot, "finalized")
    });
    let response = handle_json_rpc_value(state.clone(), &request).await;
    assert_eq!(response.headers()["X-Superbank-Sources"], "response-cache");
    assert!(parse_json_rpc_response(response).await.error.is_none());
    let [head, response_cache, disk, clickhouse, other] = finalized;
    assert_eq!(
        counts("finalized"),
        [head + 1, response_cache + 1, disk, clickhouse, other]
    );

    // A failed request counts as `other`; a rejected commitment is not counted.
    let confirmed = counts("confirmed");
    let response = handle_get_block(state.clone(), json!(3), Some(params(slot + 1, "confirmed")))
        .await
        .expect("error response");
    assert!(parse_json_rpc_response(response).await.error.is_some());
    let response = handle_get_block(state, json!(4), Some(params(slot, "processed")))
        .await
        .expect("invalid params response");
    let error = parse_json_rpc_response(response)
        .await
        .error
        .expect("error");
    assert_eq!(error.code, -32602);
    let [head, response_cache, disk, clickhouse, other] = confirmed;
    assert_eq!(
        counts("confirmed"),
        [head, response_cache, disk, clickhouse, other + 1]
    );
}
