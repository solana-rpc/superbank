// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::handlers::transactions::handle_get_transaction;

const SIGNATURE: [u8; 64] = [44; 64];

fn set_context(state: &mut Arc<AppState>, slot: u64) {
    let state = Arc::get_mut(state).unwrap();
    state.latest_slot_cache = LatestSlotCache::new(Duration::from_secs(600));
    state.latest_slot_cache.value.store(slot, Ordering::Relaxed);
    state
        .latest_slot_cache
        .last_updated_ms
        .store(current_time_millis(), Ordering::Relaxed);
}

fn old_transaction() -> StoredTransactionRecord {
    let mut record = base_transaction_record();
    record.slot = 10;
    record.signature = SIGNATURE;
    record.tx_signatures = vec![SIGNATURE];
    record
}

async fn cached_state() -> Arc<AppState> {
    let mut state = test_state_with_primary_cache("http://127.0.0.1:1", 1 << 20);
    set_context(&mut state, 100);
    let record = Arc::new(old_transaction());
    for pinned_slot in [None, Some(record.slot)] {
        state
            .get_transaction_primary_cache
            .insert(primary_cache_key(SIGNATURE, pinned_slot), record.clone())
            .await;
    }
    state
}

async fn request(state: Arc<AppState>, config: Value) -> JsonRpcResponse {
    let response = handle_get_transaction(
        state,
        json!(44),
        Some(vec![json!(bs58::encode(SIGNATURE).into_string()), config]),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    parse_json_rpc_response(response).await
}

fn assert_context_error(response: JsonRpcResponse, context_slot: u64) {
    let error = response.error.expect("minimum context error");
    assert_eq!(
        error.code,
        JSON_RPC_SERVER_ERROR_MIN_CONTEXT_SLOT_NOT_REACHED as i32
    );
    assert_eq!(error.message, "Minimum context slot has not been reached");
    assert_eq!(error.data, Some(json!({"contextSlot": context_slot})));
}

#[tokio::test]
async fn minimum_context_gates_primary_cache_and_does_not_filter_old_transaction_slots() {
    let state = cached_state().await;
    for commitment in ["confirmed", "finalized"] {
        for slot in [Value::Null, json!(10)] {
            let config = json!({"commitment": commitment, "slot": slot, "minContextSlot": 101});
            assert_context_error(request(state.clone(), config).await, 100);
            for minimum in [0, 99, 100] {
                let response = request(
                    state.clone(),
                    json!({
                        "commitment": commitment, "slot": slot, "minContextSlot": minimum
                    }),
                )
                .await;
                assert!(response.error.is_none(), "{:?}", response.error);
                assert_eq!(response.result.unwrap()["slot"], 10);
            }
        }
    }
}

#[tokio::test]
async fn omitted_and_null_minimum_do_not_require_a_context_lookup_on_a_cache_hit() {
    let mut state = cached_state().await;
    Arc::get_mut(&mut state).unwrap().latest_slot_cache =
        LatestSlotCache::new(Duration::from_secs(600));
    for config in [json!({}), json!({"minContextSlot": null}), Value::Null] {
        let response = request(state.clone(), config).await;
        assert!(response.error.is_none(), "{:?}", response.error);
        assert_eq!(response.result.unwrap()["slot"], 10);
    }
}

#[tokio::test]
async fn minimum_context_is_checked_before_missing_or_wrong_slot_lookups() {
    let mut state = test_state_with_clickhouse_url("http://127.0.0.1:1");
    set_context(&mut state, 100);
    for slot in [Value::Null, json!(999)] {
        let response = request(state.clone(), json!({"slot": slot, "minContextSlot": 101})).await;
        assert_context_error(response, 100);
    }
}

#[tokio::test]
async fn missing_context_fails_even_when_the_primary_cache_has_the_transaction() {
    let mut state = cached_state().await;
    let inner = Arc::get_mut(&mut state).unwrap();
    inner.latest_slot_cache = LatestSlotCache::new(Duration::from_secs(600));
    inner.clickhouse.set_latest_finalized_slot_for_tests(None);
    let response = request(state, json!({"minContextSlot": 100})).await;
    assert_eq!(response.error.unwrap().code, -32603);
}

#[tokio::test]
async fn malformed_minimum_context_is_rejected_before_cache_access() {
    let state = cached_state().await;
    for minimum in [json!(-1), json!(1.5), json!("100"), json!(true), json!({})] {
        let response = request(state.clone(), json!({"minContextSlot": minimum})).await;
        assert_eq!(response.error.unwrap().code, -32602);
    }
}

#[tokio::test]
async fn encoding_validation_precedes_minimum_context() {
    let response = request(
        cached_state().await,
        json!({
            "encoding": "base58", "maxSupportedTransactionVersion": 1, "minContextSlot": 101
        }),
    )
    .await;
    let error = response.error.unwrap();
    assert_eq!(error.code, -32602);
    assert_eq!(
        error.message,
        "base58 encoding is not supported with maxSupportedTransactionVersion >= 1"
    );
}

#[cfg(feature = "grpc-head-cache")]
fn head_state() -> Arc<AppState> {
    let cache = Arc::new(HeadCache::new(256, TEST_MAX_LIMIT as usize));
    cache.insert_for_tests(
        Signature::from(SIGNATURE),
        old_transaction(),
        0,
        &[],
        CommitmentLevel::Finalized,
    );
    cache.note_slot_commitment(80, CommitmentLevel::Finalized);
    cache.note_slot_commitment(90, CommitmentLevel::Confirmed);
    cache.note_slot_commitment(100, CommitmentLevel::Processed);
    test_state_with_head_cache(cache)
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn head_cache_minimum_uses_requested_commitment_before_returning_old_transactions() {
    let state = head_state();
    for (commitment, context) in [("finalized", 80), ("confirmed", 90), ("processed", 100)] {
        let rejected = request(
            state.clone(),
            json!({
                "commitment": commitment, "minContextSlot": context + 1
            }),
        )
        .await;
        assert_context_error(rejected, context);
        let accepted = request(
            state.clone(),
            json!({
                "commitment": commitment, "minContextSlot": context
            }),
        )
        .await;
        assert!(accepted.error.is_none(), "{:?}", accepted.error);
        assert_eq!(accepted.result.unwrap()["slot"], 10);
    }
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn head_cache_slot_mismatch_cannot_bypass_minimum_context() {
    let state = head_state();
    let rejected = request(state.clone(), json!({"slot": 11, "minContextSlot": 81})).await;
    assert_context_error(rejected, 80);
    let accepted = request(state, json!({"slot": 11, "minContextSlot": 80})).await;
    assert!(accepted.error.is_none(), "{:?}", accepted.error);
    assert_eq!(accepted.result, Some(Value::Null));
}
