// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::get_transaction_primary_cache::record_heap_bytes;
use crate::handlers::RouteMetric;
use crate::handlers::transactions::{handle_get_transaction, respond_with_hydrated_transaction};

const PENDING: Duration = Duration::from_millis(50);
const DONE: Duration = Duration::from_secs(10);

fn state_with(permits: usize, inline_max_bytes: usize) -> Arc<AppState> {
    let mut state = test_state_with_primary_cache("http://127.0.0.1:1", 1 << 20);
    let inner = Arc::get_mut(&mut state).expect("unique state");
    inner.hydration_sem = Arc::new(Semaphore::new(permits));
    inner.get_transaction_inline_hydration_max_bytes = inline_max_bytes;
    state
}

async fn respond(
    state: &AppState,
    record: &Arc<StoredTransactionRecord>,
    encoding: UiTransactionEncoding,
    max_version: Option<u8>,
) -> Bytes {
    let signature = bs58::encode(record.signature).into_string();
    let mut route = RouteMetric::for_state("getTransaction", state);
    let response = respond_with_hydrated_transaction(
        state,
        json!(9),
        &mut route,
        &signature,
        record.clone(),
        encoding,
        max_version,
        None,
    )
    .await
    .expect("response");
    response_body_bytes(response).await
}

fn records() -> Vec<Arc<StoredTransactionRecord>> {
    let mut records: Vec<_> = transaction_variant_records().into_iter().collect();
    records.extend(realistic_synthetic_block_record(4).transactions);
    // Fails hydration: two required signatures but only one present.
    let mut malformed = transaction_variant_record(None, false);
    malformed.tx_num_required_signatures = 2;
    records.push(malformed);
    records.into_iter().map(Arc::new).collect()
}

#[tokio::test]
async fn get_transaction_inline_hydration_matches_permit_path_bytes() {
    let permit = state_with(8, 0);
    let inline = state_with(0, usize::MAX);
    let mut errors = 0;
    for record in records() {
        for encoding in [
            UiTransactionEncoding::Json,
            UiTransactionEncoding::JsonParsed,
            UiTransactionEncoding::Base58,
            UiTransactionEncoding::Base64,
        ] {
            for max_version in [None, Some(0), Some(1)] {
                let expected = respond(&permit, &record, encoding, max_version).await;
                let actual = respond(&inline, &record, encoding, max_version).await;
                assert_eq!(
                    actual, expected,
                    "{encoding:?} {max_version:?} {:?}",
                    record.tx_version
                );
                let parsed: JsonRpcResponse = serde_json::from_slice(&actual).expect("json");
                errors += usize::from(parsed.error.is_some());
            }
        }
    }
    // Unsupported-version and hydration errors are covered as well as successes.
    assert!(errors > 0);
}

#[tokio::test]
async fn get_transaction_inline_hydration_default_keeps_the_permit_path() {
    let record = Arc::new(transaction_variant_record(Some(0), false));
    let size = record_heap_bytes(&record);
    assert_eq!(test_state().get_transaction_inline_hydration_max_bytes, 0);

    // With no free permit, the permit path waits; the wait is recorded even when cancelled.
    for inline_max_bytes in [0, size - 1] {
        let state = state_with(0, inline_max_bytes);
        let waits = metrics::hydration_permit_wait_count_for_tests("getTransaction");
        let pending = tokio::time::timeout(
            PENDING,
            respond(&state, &record, UiTransactionEncoding::Json, Some(0)),
        )
        .await;
        assert!(pending.is_err(), "inline_max_bytes={inline_max_bytes}");
        assert!(metrics::hydration_permit_wait_count_for_tests("getTransaction") > waits);
    }

    let state = state_with(0, size);
    let inline = tokio::time::timeout(
        DONE,
        respond(&state, &record, UiTransactionEncoding::Json, Some(0)),
    )
    .await
    .expect("inline hydration takes no permit");
    let permit = respond(
        &state_with(1, 0),
        &record,
        UiTransactionEncoding::Json,
        Some(0),
    )
    .await;
    assert_eq!(inline, permit);
}

#[tokio::test]
async fn get_transaction_handler_hydrates_small_records_inline() {
    let record = Arc::new(transaction_variant_record(None, false));
    let signature = bs58::encode(record.signature).into_string();
    let mut bodies = Vec::new();
    for (permits, inline_max_bytes) in [(1, 0), (0, usize::MAX)] {
        let state = state_with(permits, inline_max_bytes);
        state
            .get_transaction_primary_cache
            .insert(primary_cache_key(record.signature, None), record.clone())
            .await;
        let response = tokio::time::timeout(
            DONE,
            handle_get_transaction(
                state,
                json!(5),
                Some(vec![json!(&signature), json!({"commitment": "confirmed"})]),
            ),
        )
        .await
        .expect("response without waiting")
        .expect("response");
        bodies.push(response_body_bytes(response).await);
    }
    assert_eq!(bodies[0], bodies[1]);
    let parsed: JsonRpcResponse = serde_json::from_slice(&bodies[0]).expect("json");
    assert!(parsed.error.is_none());
}
