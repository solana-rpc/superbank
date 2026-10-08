// SPDX-License-Identifier: AGPL-3.0-only
use super::*;
use crate::block_response_cache::BlockResponseCacheKey;
use crate::clickhouse::BlockFooterRecord;

const SLOT: u64 = 4_242;

fn footer(nanos: u64, agent: &str) -> BlockFooterRecord {
    BlockFooterRecord {
        block_producer_time_nanos: nanos,
        block_user_agent: agent.to_string(),
    }
}

/// A head-cache fixture whose finalized block metadata carries `footer`.
fn state_with_footer(footer: Option<BlockFooterRecord>, response_cache: bool) -> Arc<AppState> {
    let cache = Arc::new(HeadCache::new(32, TEST_MAX_LIMIT as usize));
    let mut record = transaction_variant_records().into_iter().next().unwrap();
    record.slot = SLOT;
    let signature = Signature::from(record.signature);
    let address = Pubkey::from(record.tx_account_keys[0]);
    cache.insert_for_tests(signature, record, 0, &[address], CommitmentLevel::Finalized);
    let mut metadata = base_block_record(SLOT).metadata;
    metadata.executed_transaction_count = 1;
    metadata.footer = footer;
    cache.note_block_metadata(metadata);
    let mut state = test_state_with_head_cache_and_clickhouse_url(cache, "http://127.0.0.1:1");
    if response_cache {
        Arc::get_mut(&mut state).unwrap().block_response_cache = BlockResponseCache::new(1 << 20);
    }
    state
}

async fn get_block(state: &Arc<AppState>, config: Value) -> Value {
    let response = handle_get_block(state.clone(), json!(1), Some(vec![json!(SLOT), config]))
        .await
        .unwrap();
    let body = parse_json_rpc_response(response).await;
    assert!(body.error.is_none(), "{body:?}");
    body.result.expect("block result")
}

fn config(footer: Option<Value>) -> Value {
    let mut config = json!({"commitment": "confirmed", "transactionDetails": "none"});
    if let Some(footer) = footer {
        config["footer"] = footer;
    }
    config
}

#[tokio::test]
async fn footer_is_omitted_by_default() {
    let state = state_with_footer(Some(footer(7, "agent")), false);
    let result = get_block(&state, config(None)).await;
    assert!(result.get("footer").is_none(), "{result}");
    assert!(result.get("previousBlockhash").is_some());
    assert_eq!(result, get_block(&state, config(Some(json!(false)))).await);
}

#[tokio::test]
async fn footer_true_includes_the_stored_footer() {
    let state = state_with_footer(
        Some(footer(1_750_176_982_899_968_023, "agave/v4.3.0")),
        false,
    );
    let result = get_block(&state, config(Some(json!(true)))).await;
    assert_eq!(
        result["footer"],
        json!({"blockProducerTimeNanos": 1_750_176_982_899_968_023u64, "blockUserAgent": "agave/v4.3.0"})
    );
    assert!(result["footer"]["blockProducerTimeNanos"].is_u64());
    assert!(result.get("previousBlockhash").is_some());
}

#[tokio::test]
async fn block_without_a_footer_returns_null_when_requested() {
    let state = state_with_footer(None, false);
    let result = get_block(&state, config(Some(json!(true)))).await;
    assert_eq!(result.get("footer"), Some(&Value::Null));
}

#[tokio::test]
async fn footer_is_added_for_every_transaction_detail_level() {
    let state = state_with_footer(Some(footer(9, "agent")), false);
    for details in ["none", "signatures"] {
        let mut config = config(Some(json!(true)));
        config["transactionDetails"] = json!(details);
        let result = get_block(&state, config).await;
        assert_eq!(
            result["footer"]["blockProducerTimeNanos"],
            json!(9),
            "{details}"
        );
    }
}

#[tokio::test]
async fn non_boolean_footer_is_rejected() {
    let state = state_with_footer(None, false);
    for bad in [json!("true"), json!(1), json!({}), json!([])] {
        let response = handle_get_block(
            state.clone(),
            json!(1),
            Some(vec![json!(SLOT), config(Some(bad.clone()))]),
        )
        .await
        .unwrap();
        let body = parse_json_rpc_response(response).await;
        let error = body.error.unwrap_or_else(|| panic!("{bad} accepted"));
        assert_eq!(error.code, -32602);
        assert_eq!(error.message, "Invalid params: footer must be a boolean");
    }
}

#[tokio::test]
async fn footer_response_from_the_head_cache_is_not_cached() {
    let state = state_with_footer(None, true);
    let key = |footer| BlockResponseCacheKey {
        slot: SLOT,
        commitment: CommitmentLevel::Finalized,
        encoding: 3,
        transaction_details: 2,
        show_rewards: true,
        max_supported_transaction_version: None,
        footer,
    };
    let mut finalized = config(Some(json!(true)));
    finalized["commitment"] = json!("finalized");
    get_block(&state, finalized.clone()).await;
    assert!(state.block_response_cache.get(&key(true)).await.is_none());

    finalized["footer"] = json!(false);
    get_block(&state, finalized).await;
    assert!(
        state.block_response_cache.get(&key(false)).await.is_some(),
        "responses without a footer are cacheable"
    );
    assert!(state.block_response_cache.get(&key(true)).await.is_none());
}
