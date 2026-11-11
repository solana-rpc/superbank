// SPDX-License-Identifier: AGPL-3.0-only
use super::*;
use crate::clickhouse::BlockFooterRecord;
use crate::head_cache::{HeadBankEvent, HeadBankEventKind};
use crate::recent_block_responses::{
    PrebuildShape, RecentAccess, RecentBlockResponses, parse_prebuild_shapes,
};

const SLOT: u64 = 4_242;
const STORE_BYTES: u64 = 64 << 20;

/// 300 transactions take the chunked hydration path; the block has v0 transactions.
fn block() -> StoredBlockRecord {
    let mut block = large_synthetic_block_record(300, false);
    block.metadata.blockhash = [7; 32];
    block
}

fn footer() -> BlockFooterRecord {
    BlockFooterRecord {
        block_producer_time_nanos: 1_750_176_982_899_968_023,
        block_user_agent: "agave/v4.4.0".to_string(),
    }
}

fn head_with(block: &StoredBlockRecord, commitment: CommitmentLevel) -> Arc<HeadCache> {
    let cache = Arc::new(HeadCache::new(64, TEST_MAX_LIMIT as usize));
    for record in &block.transactions {
        let addresses: Vec<_> = record
            .tx_account_keys
            .iter()
            .take(1)
            .map(|key| Pubkey::from(*key))
            .collect();
        cache.insert_for_tests(
            Signature::from(record.signature),
            record.clone(),
            record.slot_idx,
            &addresses,
            commitment,
        );
    }
    cache.note_block_metadata(block.metadata.clone());
    cache
}

fn state_with(cache: Arc<HeadCache>, store_bytes: u64) -> Arc<AppState> {
    let mut state = test_state_with_head_cache_and_clickhouse_url(cache, "http://127.0.0.1:1");
    Arc::get_mut(&mut state).unwrap().recent_block_responses =
        RecentBlockResponses::new(store_bytes);
    state
}

async fn body(state: &Arc<AppState>, config: &Value) -> Bytes {
    let response = handle_get_block(
        state.clone(),
        json!(1),
        Some(vec![json!(SLOT), config.clone()]),
    )
    .await
    .unwrap();
    to_bytes(response.into_body(), usize::MAX).await.unwrap()
}

fn config(commitment: &str, shape: PrebuildShape, footer: bool) -> Value {
    let mut config = json!({
        "commitment": commitment,
        "encoding": shape.encoding,
        "transactionDetails": shape.transaction_details,
        "rewards": shape.rewards,
        "footer": footer,
    });
    if let Some(version) = shape.max_supported_transaction_version {
        config["maxSupportedTransactionVersion"] = json!(version);
    }
    config
}

fn shapes(list: &str) -> Vec<PrebuildShape> {
    parse_prebuild_shapes(list).unwrap().0
}

/// Each encoding family and detail level, with and without rewards.
fn tested_shapes() -> Vec<PrebuildShape> {
    shapes(
        "rewards=false:maxSupportedTransactionVersion=0,\
         maxSupportedTransactionVersion=0,\
         encoding=base64:rewards=false:maxSupportedTransactionVersion=0,\
         encoding=jsonParsed:maxSupportedTransactionVersion=0,\
         transactionDetails=accounts:maxSupportedTransactionVersion=0,\
         transactionDetails=signatures:rewards=false:maxSupportedTransactionVersion=0,\
         transactionDetails=none",
    )
}

fn is_success(body: &Bytes) -> bool {
    let response: JsonRpcResponse = serde_json::from_slice(body).unwrap();
    response.error.is_none() && response.result.is_some()
}

async fn wait_until(mut done: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("condition reached");
}

#[tokio::test]
async fn stored_responses_match_the_live_head_path() {
    let mut block = block();
    block.metadata.footer = Some(footer());
    for head_commitment in [CommitmentLevel::Confirmed, CommitmentLevel::Finalized] {
        let live = state_with(head_with(&block, head_commitment), 0);
        let stored = state_with(head_with(&block, head_commitment), STORE_BYTES);
        for shape in tested_shapes() {
            for commitment in ["confirmed", "finalized"] {
                for footer in [false, true] {
                    let config = config(commitment, shape, footer);
                    let expected = body(&live, &config).await;
                    let from_head =
                        commitment == "confirmed" || head_commitment == CommitmentLevel::Finalized;
                    assert_eq!(is_success(&expected), from_head, "{config}");
                    assert_eq!(body(&stored, &config).await, expected, "{config}");
                    assert_eq!(body(&stored, &config).await, expected, "{config} (hit)");
                }
            }
        }
        // Footer and commitment variants share the stored result of their shape.
        let (entries, _) = stored.recent_block_responses.resident();
        assert_eq!(entries, tested_shapes().len() as u64, "{head_commitment:?}");
        assert_eq!(live.recent_block_responses.resident(), (0, 0));
    }
}

#[tokio::test]
async fn unsupported_version_errors_match_and_are_not_stored() {
    let block = block();
    let live = state_with(head_with(&block, CommitmentLevel::Confirmed), 0);
    let stored = state_with(head_with(&block, CommitmentLevel::Confirmed), STORE_BYTES);
    let config = json!({"commitment": "confirmed", "rewards": false});
    let expected = body(&live, &config).await;
    let error: JsonRpcResponse = serde_json::from_slice(&expected).unwrap();
    assert_eq!(error.error.expect("error").code, -32015);
    for _ in 0..2 {
        assert_eq!(body(&stored, &config).await, expected);
    }
    assert_eq!(stored.recent_block_responses.resident(), (0, 0));
}

#[tokio::test]
async fn prebuilt_results_match_live_results_for_each_shape() {
    let mut block = block();
    block.metadata.footer = Some(footer());
    let live = state_with(head_with(&block, CommitmentLevel::Confirmed), 0);
    let head = head_with(&block, CommitmentLevel::Confirmed);
    let stored = state_with(head.clone(), STORE_BYTES);
    let semaphore = Arc::new(Semaphore::new(1));
    for shape in tested_shapes() {
        let lead = semaphore.clone().acquire_owned().await.unwrap();
        let access = crate::handlers::blocks::prebuild_recent_block(
            &stored,
            &head,
            shape.key(SLOT, block.metadata.blockhash),
            shape,
            semaphore.clone(),
            lead,
        )
        .await
        .expect("prebuilt");
        assert_eq!(access, RecentAccess::Built, "{shape:?}");
    }
    assert_eq!(semaphore.available_permits(), 1);

    // An extra signature leaves the head block incomplete: only stored results answer.
    let mut extra = block.transactions[0].clone();
    extra.signature = [0xee; 64];
    extra.tx_signatures = vec![[0xee; 64]];
    head.insert_for_tests(
        Signature::from(extra.signature),
        extra,
        9_999,
        &[],
        CommitmentLevel::Confirmed,
    );
    for shape in tested_shapes() {
        for footer in [false, true] {
            let config = config("confirmed", shape, footer);
            let expected = body(&live, &config).await;
            assert!(is_success(&expected), "{config}");
            assert_eq!(body(&stored, &config).await, expected, "{config}");
        }
    }
}

#[tokio::test]
async fn footer_requests_read_the_current_footer() {
    let mut block = block();
    let head = head_with(&block, CommitmentLevel::Confirmed);
    let stored = state_with(head.clone(), STORE_BYTES);
    let live = state_with(head.clone(), 0);
    let config = config("confirmed", tested_shapes()[0], true);
    let before = body(&stored, &config).await;
    assert_eq!(before, body(&live, &config).await);

    block.metadata.footer = Some(footer());
    head.note_block_metadata(block.metadata.clone());
    let after = body(&stored, &config).await;
    assert_ne!(after, before);
    assert_eq!(after, body(&live, &config).await);
    assert_eq!(stored.recent_block_responses.resident().0, 1);
}

#[tokio::test]
async fn another_blockhash_for_the_slot_misses() {
    let mut block = block();
    let head = head_with(&block, CommitmentLevel::Confirmed);
    let stored = state_with(head.clone(), STORE_BYTES);
    let live = state_with(head.clone(), 0);
    let config = config("confirmed", tested_shapes()[0], false);
    let first = body(&stored, &config).await;

    block.metadata.blockhash = [8; 32];
    head.note_block_metadata(block.metadata.clone());
    let second = body(&stored, &config).await;
    assert_ne!(first, second);
    assert_eq!(second, body(&live, &config).await);
    assert_eq!(stored.recent_block_responses.resident().0, 2);
}

#[tokio::test]
async fn concurrent_requests_share_one_stored_result() {
    let block = block();
    let live = state_with(head_with(&block, CommitmentLevel::Confirmed), 0);
    let stored = state_with(head_with(&block, CommitmentLevel::Confirmed), STORE_BYTES);
    let config = config("confirmed", tested_shapes()[0], false);
    let expected = body(&live, &config).await;
    let requests: Vec<_> = (0..8)
        .map(|_| {
            let stored = stored.clone();
            let config = config.clone();
            tokio::spawn(async move { body(&stored, &config).await })
        })
        .collect();
    for request in requests {
        assert_eq!(request.await.unwrap(), expected);
    }
    assert_eq!(stored.recent_block_responses.resident().0, 1);
}

#[tokio::test]
async fn store_task_evicts_discarded_blocks_and_clears_after_lag() {
    let block = block();
    let head = head_with(&block, CommitmentLevel::Confirmed);
    let stored = state_with(head.clone(), STORE_BYTES);
    let config = config("confirmed", tested_shapes()[0], false);
    let event = |blockhash, kind| HeadBankEvent {
        slot: SLOT,
        blockhash,
        kind,
    };
    let entries = || stored.recent_block_responses.resident().0;

    body(&stored, &config).await;
    assert_eq!(entries(), 1);
    let (sender, events) = tokio::sync::broadcast::channel(16);
    let task = tokio::spawn(crate::recent_block_responses::run(
        stored.clone(),
        head.clone(),
        events,
        None,
    ));
    sender
        .send(event([9; 32], HeadBankEventKind::Discarded))
        .unwrap();
    sender
        .send(event([9; 32], HeadBankEventKind::Confirmed))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(entries(), 1, "another fork of the slot");
    sender
        .send(event(
            block.metadata.blockhash,
            HeadBankEventKind::Discarded,
        ))
        .unwrap();
    wait_until(|| entries() == 0).await;
    task.abort();

    body(&stored, &config).await;
    assert_eq!(entries(), 1);
    let (sender, events) = tokio::sync::broadcast::channel(1);
    for _ in 0..2 {
        sender
            .send(event([9; 32], HeadBankEventKind::Confirmed))
            .unwrap();
    }
    let task = tokio::spawn(crate::recent_block_responses::run(
        stored.clone(),
        head.clone(),
        events,
        None,
    ));
    wait_until(|| entries() == 0).await;
    task.abort();
}

#[tokio::test]
async fn disabled_store_is_never_touched() {
    let block = block();
    let state = state_with(head_with(&block, CommitmentLevel::Confirmed), 0);
    for shape in tested_shapes() {
        body(&state, &config("confirmed", shape, false)).await;
    }
    assert!(!state.recent_block_responses.enabled());
    assert_eq!(state.recent_block_responses.resident(), (0, 0));
}
