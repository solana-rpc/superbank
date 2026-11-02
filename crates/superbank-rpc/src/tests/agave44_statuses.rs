// SPDX-License-Identifier: AGPL-3.0-only

use super::*;

fn signature(marker: u8) -> String {
    solana_sdk::signature::Signature::from([marker; 64]).to_string()
}

async fn statuses(
    state: Arc<AppState>,
    signatures: Value,
    config: Option<Value>,
) -> JsonRpcResponse {
    let mut params = vec![signatures];
    params.extend(config);
    let response = handle_get_signature_statuses(state, json!(1), Some(params))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    parse_json_rpc_response(response).await
}

#[tokio::test]
async fn minimum_context_precedes_missing_status_and_history_lookup() {
    for history in [false, true] {
        let response = statuses(
            test_state(),
            json!([signature(1)]),
            Some(json!({"minContextSlot": 2, "searchTransactionHistory": history})),
        )
        .await;
        let error = response.error.unwrap();
        assert_eq!(error.code, -32016);
        assert_eq!(error.message, "Minimum context slot has not been reached");
        assert_eq!(error.data, Some(json!({"contextSlot": 1})));
    }
}

#[tokio::test]
async fn finalized_only_context_supports_all_status_commitments() {
    for commitment in ["processed", "confirmed", "finalized"] {
        for minimum in [Value::Null, json!(0), json!(1)] {
            let response = statuses(
                test_state(),
                json!([signature(1), signature(1)]),
                Some(json!({"commitment": commitment, "minContextSlot": minimum})),
            )
            .await;
            assert!(response.error.is_none(), "{response:?}");
            assert_eq!(
                response.result.unwrap(),
                json!({"context": {"slot": 1}, "value": [null, null]})
            );
        }
    }
}

#[tokio::test]
async fn invalid_status_config_is_rejected_before_context_lookup() {
    for config in [
        json!({"commitment": "invalid"}),
        json!({"commitment": 1}),
        json!({"minContextSlot": -1}),
        json!({"minContextSlot": "1"}),
        json!({"minContextSlot": 1.5}),
        json!({"searchTransactionHistory": "true"}),
    ] {
        let response = statuses(test_state(), json!([signature(1)]), Some(config)).await;
        assert_eq!(response.error.unwrap().code, -32602);
    }
}

#[cfg(feature = "grpc-head-cache")]
fn head_fixture() -> (Arc<AppState>, Arc<HeadCache>) {
    let cache = Arc::new(HeadCache::new(128, 128));
    for (marker, slot, commitment) in [
        (1, 10, CommitmentLevel::Finalized),
        (2, 20, CommitmentLevel::Confirmed),
        (3, 30, CommitmentLevel::Processed),
    ] {
        let mut record = base_transaction_record();
        record.slot = slot;
        record.signature = [marker; 64];
        cache.insert_for_tests(
            Signature::from(record.signature),
            record,
            0,
            &[],
            commitment,
        );
    }
    let state = test_state_with_head_cache_and_clickhouse_url(cache.clone(), "http://127.0.0.1:1");
    (state, cache)
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn status_commitment_selects_both_context_and_eligible_head_results() {
    let (state, _) = head_fixture();
    for (commitment, context, visible) in [
        ("processed", 30, 3),
        ("confirmed", 20, 2),
        ("finalized", 10, 1),
    ] {
        let response = statuses(
            state.clone(),
            json!([signature(1), signature(2), signature(3), signature(2)]),
            Some(json!({"commitment": commitment, "minContextSlot": context})),
        )
        .await;
        assert!(response.error.is_none(), "{response:?}");
        let result = response.result.unwrap();
        assert_eq!(result["context"]["slot"], context);
        for (index, status) in ["finalized", "confirmed", "processed"].iter().enumerate() {
            if index < visible {
                assert_eq!(result["value"][index]["confirmationStatus"], *status);
            } else {
                assert!(result["value"][index].is_null());
            }
        }
        assert_eq!(result["value"][1], result["value"][3]);
        assert_eq!(result["value"][0]["slot"], 10);
    }
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn omitted_and_null_status_commitment_default_to_processed() {
    let (state, _) = head_fixture();
    for config in [
        None,
        Some(Value::Null),
        Some(json!({})),
        Some(json!({"commitment": null})),
    ] {
        let response = statuses(state.clone(), json!([signature(3)]), config).await;
        assert!(response.error.is_none(), "{response:?}");
        let result = response.result.unwrap();
        assert_eq!(result["context"]["slot"], 30);
        assert_eq!(result["value"][0]["confirmationStatus"], "processed");
    }
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn a_faster_processed_tip_cannot_satisfy_a_stronger_minimum_context() {
    let (state, _) = head_fixture();
    for (commitment, context) in [("confirmed", 20), ("finalized", 10)] {
        for history in [false, true] {
            let response = statuses(
                state.clone(),
                json!([signature(1), signature(3), signature(4)]),
                Some(
                    json!({"commitment": commitment, "minContextSlot": context + 1,
                    "searchTransactionHistory": history}),
                ),
            )
            .await;
            let error = response.error.unwrap();
            assert_eq!(error.code, -32016);
            assert_eq!(error.data, Some(json!({"contextSlot": context})));
        }
    }
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn removed_branch_statuses_disappear_and_retried_signatures_use_the_new_commitment() {
    let (state, cache) = head_fixture();
    cache.remove_slot(20);
    let response = statuses(
        state.clone(),
        json!([signature(2)]),
        Some(json!({"commitment": "confirmed"})),
    )
    .await;
    assert!(response.error.is_none(), "{response:?}");
    assert_eq!(response.result.unwrap()["value"], json!([null]));

    let mut record = base_transaction_record();
    record.slot = 31;
    record.signature = [2; 64];
    cache.insert_for_tests(
        Signature::from(record.signature),
        record,
        0,
        &[],
        CommitmentLevel::Processed,
    );
    let response = statuses(
        state.clone(),
        json!([signature(2)]),
        Some(json!({"commitment": "confirmed"})),
    )
    .await;
    assert!(response.error.is_none(), "{response:?}");
    assert_eq!(response.result.unwrap()["value"], json!([null]));
    cache.note_slot_commitment(31, CommitmentLevel::Confirmed);
    let response = statuses(
        state,
        json!([signature(2)]),
        Some(json!({"commitment": "confirmed"})),
    )
    .await;
    assert!(response.error.is_none(), "{response:?}");
    let result = response.result.unwrap();
    assert_eq!(result["context"]["slot"], 31);
    assert_eq!(result["value"][0]["slot"], 31);
    assert_eq!(result["value"][0]["confirmationStatus"], "confirmed");
}
