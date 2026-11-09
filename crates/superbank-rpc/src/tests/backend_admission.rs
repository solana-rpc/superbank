// SPDX-License-Identifier: AGPL-3.0-only
use std::time::Instant;

use super::*;
use crate::admission::BackendAdmission;

/// Gives `state` a one-permit backend admission, held by another call for the whole test.
async fn saturated(
    state: Arc<AppState>,
    queue_timeout: Duration,
) -> (Arc<AppState>, tokio::sync::OwnedSemaphorePermit) {
    let mut state = match Arc::try_unwrap(state) {
        Ok(state) => state,
        Err(_) => panic!("test state should have a single Arc owner"),
    };
    let admission = BackendAdmission::new(1, queue_timeout).expect("limit 1 enables admission");
    let held = admission
        .semaphore()
        .clone()
        .acquire_owned()
        .await
        .expect("free permit");
    state.clickhouse.set_backend_admission(Some(admission));
    (Arc::new(state), held)
}

async fn call(state: Arc<AppState>, method: &str, params: Value) -> (StatusCode, Value, Duration) {
    let request = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
    let started = Instant::now();
    let response = handle_json_rpc_value(state, &request).await;
    let elapsed = started.elapsed();
    let status = response.status();
    (status, parse_json_value_response(response).await, elapsed)
}

fn get_transaction_params(signature: &str) -> Value {
    json!([signature, {
        "encoding": "json",
        "commitment": "confirmed",
        "maxSupportedTransactionVersion": 0,
    }])
}

fn shed_count(operation: &str) -> u64 {
    let exported = String::from_utf8(metrics::export_metrics().expect("metrics export"))
        .expect("metrics are UTF-8");
    let series = format!(
        "superbank_rpc_backend_admission_total{{operation=\"{operation}\",outcome=\"shed\"}} "
    );
    exported
        .lines()
        .find_map(|line| line.strip_prefix(series.as_str())?.parse().ok())
        .unwrap_or(0)
}

#[tokio::test]
async fn saturated_backend_sheds_a_backend_bound_call_as_http_503() {
    let queue_timeout = Duration::from_millis(50);
    let (state, _held) = saturated(
        test_state_with_emit_http_errors(test_state()),
        queue_timeout,
    )
    .await;
    let shed_before = shed_count("get_transaction_by_signature");
    let signature = bs58::encode([7u8; 64]).into_string();
    let (status, body, elapsed) =
        call(state, "getTransaction", get_transaction_params(&signature)).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["error"]["code"], json!(-32603), "{body}");
    assert_eq!(body["error"]["message"], json!("Internal error"), "{body}");
    assert!(
        elapsed >= queue_timeout && elapsed < Duration::from_secs(5),
        "{elapsed:?}"
    );
    assert!(shed_count("get_transaction_by_signature") > shed_before);
}

#[tokio::test]
async fn saturated_backend_does_not_delay_a_call_served_from_memory() {
    let queue_timeout = Duration::from_secs(30);
    let (state, _held) = saturated(test_state(), queue_timeout).await;
    let (status, body, elapsed) = call(state, "getSlot", json!([])).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"], json!(1), "{body}");
    assert!(elapsed < Duration::from_secs(5), "{elapsed:?}");
}

#[test]
fn default_state_has_no_backend_admission() {
    assert!(test_state().clickhouse.backend_admission.is_none());
}

#[cfg(feature = "grpc-head-cache")]
mod head_cache {
    use super::*;

    const SLOT: u64 = 4_242;

    fn head_state() -> (Arc<AppState>, String) {
        let cache = Arc::new(HeadCache::new(32, TEST_MAX_LIMIT as usize));
        let mut record = transaction_variant_records()
            .into_iter()
            .next()
            .expect("variant record");
        record.slot = SLOT;
        let signature = Signature::from(record.signature);
        let address = Pubkey::from(record.tx_account_keys[0]);
        cache.insert_for_tests(signature, record, 0, &[address], CommitmentLevel::Finalized);
        let mut metadata = base_block_record(SLOT).metadata;
        metadata.executed_transaction_count = 1;
        cache.note_block_metadata(metadata);
        let state = test_state_with_head_cache_and_clickhouse_url(cache, "http://127.0.0.1:1");
        (state, signature.to_string())
    }

    #[tokio::test]
    async fn saturated_backend_does_not_delay_recent_get_transaction() {
        let (state, signature) = head_state();
        let (state, _held) = saturated(state, Duration::from_secs(30)).await;
        let (status, body, elapsed) =
            call(state, "getTransaction", get_transaction_params(&signature)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["result"]["slot"], json!(SLOT), "{body}");
        assert!(elapsed < Duration::from_secs(5), "{elapsed:?}");
    }

    #[tokio::test]
    async fn saturated_backend_does_not_delay_recent_get_block() {
        let (state, _) = head_state();
        let (state, _held) = saturated(state, Duration::from_secs(30)).await;
        let params = json!([SLOT, {
            "encoding": "json",
            "transactionDetails": "full",
            "rewards": false,
            "commitment": "confirmed",
            "maxSupportedTransactionVersion": 0,
        }]);
        let (status, body, elapsed) = call(state, "getBlock", params).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body["result"]["transactions"].as_array().map(Vec::len),
            Some(1),
            "{body}"
        );
        assert!(elapsed < Duration::from_secs(5), "{elapsed:?}");
    }
}
