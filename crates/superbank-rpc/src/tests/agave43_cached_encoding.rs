// SPDX-License-Identifier: AGPL-3.0-only
use super::*;
use crate::handlers::transactions::handle_get_transaction;

fn head_fixture(record: StoredTransactionRecord) -> (Arc<AppState>, Signature, u64) {
    let cache = Arc::new(HeadCache::new(32, TEST_MAX_LIMIT as usize));
    let slot = record.slot;
    let signature = Signature::from(record.signature);
    let address = Pubkey::from(record.tx_account_keys[0]);
    cache.insert_for_tests(signature, record, 0, &[address], CommitmentLevel::Confirmed);
    let mut metadata = base_block_record(slot).metadata;
    metadata.executed_transaction_count = 1;
    cache.note_block_metadata(metadata);
    (
        test_state_with_head_cache_and_clickhouse_url(cache, "http://127.0.0.1:1"),
        signature,
        slot,
    )
}

fn expected_error(version: Option<u8>, encoding: &str, maximum: Option<u8>) -> Option<i32> {
    if matches!(encoding, "base58" | "binary") && maximum.is_some_and(|v| v >= 1) {
        return Some(-32602);
    }
    match (version, maximum) {
        (Some(version), Some(maximum)) if version <= maximum => None,
        (Some(_), _) => Some(-32015),
        (None, _) => None,
    }
}

async fn assert_response(response: Response, expected: Option<i32>) {
    assert_eq!(response.status(), StatusCode::OK);
    let body = parse_json_rpc_response(response).await;
    assert_eq!(
        body.error.as_ref().map(|error| error.code),
        expected,
        "{body:?}"
    );
    if expected == Some(-32602) {
        let error = body.error.unwrap();
        assert_eq!(
            error.message,
            "base58 encoding is not supported with maxSupportedTransactionVersion >= 1"
        );
        assert!(error.data.is_none());
    } else if expected.is_none() {
        assert!(body.result.is_some_and(|value| !value.is_null()));
    }
}

#[tokio::test]
async fn agave43_head_cached_transaction_encoding_matrix() {
    for record in transaction_variant_records() {
        let version = record.tx_version;
        let (state, signature, _) = head_fixture(record);
        for encoding in ["base58", "binary", "base64", "json", "jsonParsed"] {
            for maximum in [None, Some(0), Some(1), Some(255)] {
                let config = json!({"commitment":"confirmed", "encoding":encoding, "maxSupportedTransactionVersion":maximum});
                let response = handle_get_transaction(
                    state.clone(),
                    json!(1),
                    Some(vec![json!(signature.to_string()), config]),
                )
                .await
                .unwrap();
                assert_response(response, expected_error(version, encoding, maximum)).await;
            }
        }
    }
}

#[tokio::test]
async fn agave43_head_cached_block_encoding_matrix() {
    for record in transaction_variant_records() {
        let version = record.tx_version;
        let (state, _, slot) = head_fixture(record);
        assert_block_encodings(state, slot, version).await;
    }
}

async fn assert_block_encodings(state: Arc<AppState>, slot: u64, version: Option<u8>) {
    for details in ["full", "accounts", "signatures", "none"] {
        for encoding in ["base58", "binary", "base64", "json", "jsonParsed"] {
            for maximum in [None, Some(0), Some(1), Some(255)] {
                let config = json!({"commitment":"confirmed", "encoding":encoding, "maxSupportedTransactionVersion":maximum,"transactionDetails":details});
                let response =
                    handle_get_block(state.clone(), json!(1), Some(vec![json!(slot), config]))
                        .await
                        .unwrap();
                let projected_version = if matches!(details, "none" | "signatures") {
                    None
                } else {
                    version
                };
                assert_response(
                    response,
                    expected_error(projected_version, encoding, maximum),
                )
                .await;
            }
        }
    }
}
