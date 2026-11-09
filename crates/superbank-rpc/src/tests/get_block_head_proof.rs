// SPDX-License-Identifier: AGPL-3.0-only
//! getBlock misses that the head's finalized parent-link chain answers without ClickHouse.
use super::*;
use crate::head_cache::coverage::{Link, TIP_MAX_AGE};
use solana_rpc_client_api::custom_error::{
    JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE, JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED,
};
use std::time::Instant;

/// An unreachable primary: any request that reaches ClickHouse answers `-32603`.
const NO_CLICKHOUSE: &str = "http://127.0.0.1:1";

fn hash(slot: u64) -> [u8; 32] {
    let mut hash = [0; 32];
    hash[..8].copy_from_slice(&slot.to_le_bytes());
    hash
}

/// Finalized chain 100 <- 103 <- 104: 101 and 102 are proven skipped, 104 is the tip.
fn finalize(cache: &HeadCache, at: Instant) {
    let mut proof = cache.coverage.write().unwrap();
    proof.connect();
    for (slot, parent) in [(100, 99), (103, 100), (104, 103)] {
        proof.metadata(Link {
            slot,
            hash: hash(slot),
            parent,
            parent_hash: hash(parent),
        });
        proof.observe(slot, CommitmentLevel::Finalized, at);
        proof.publish(slot, CommitmentLevel::Finalized);
    }
}

fn head_state(at: Instant) -> (Arc<HeadCache>, Arc<AppState>) {
    let cache = Arc::new(HeadCache::new(64, TEST_MAX_LIMIT as usize));
    finalize(&cache, at);
    let state = test_state_with_head_cache_and_clickhouse_url(cache.clone(), NO_CLICKHOUSE);
    (cache, state)
}

fn config(commitment: &str) -> Value {
    json!({
        "encoding": "json",
        "transactionDetails": "full",
        "rewards": false,
        "commitment": commitment,
        "maxSupportedTransactionVersion": 0,
    })
}

async fn get_block(state: &Arc<AppState>, slot: u64, commitment: &str) -> (StatusCode, Bytes) {
    let response = handle_get_block(
        state.clone(),
        json!(1),
        Some(vec![json!(slot), config(commitment)]),
    )
    .await
    .unwrap();
    let status = response.status();
    (
        status,
        to_bytes(response.into_body(), usize::MAX).await.unwrap(),
    )
}

async fn expected(code: i64, message: String) -> (StatusCode, Bytes) {
    let response = json_rpc_error_response(json!(1), code as i32, message, None);
    let status = response.status();
    (
        status,
        to_bytes(response.into_body(), usize::MAX).await.unwrap(),
    )
}

async fn error_code(state: &Arc<AppState>, slot: u64, commitment: &str) -> i64 {
    let (_, body) = get_block(state, slot, commitment).await;
    let body: Value = serde_json::from_slice(&body).unwrap();
    body["error"]["code"]
        .as_i64()
        .unwrap_or_else(|| panic!("{body}"))
}

#[tokio::test]
async fn proven_skip_returns_the_disk_tier_skipped_error_without_clickhouse() {
    let (_, state) = head_state(Instant::now());
    for commitment in ["confirmed", "finalized"] {
        for slot in [101, 102] {
            let before = metrics::get_block_not_found_count("proven_skipped");
            assert_eq!(
                get_block(&state, slot, commitment).await,
                expected(
                    JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED,
                    format!("Slot {slot} was skipped, or missing in long-term storage"),
                )
                .await,
                "{commitment} {slot}"
            );
            assert!(metrics::get_block_not_found_count("proven_skipped") > before);
        }
    }
    let text = String::from_utf8(metrics::export_metrics().unwrap()).unwrap();
    assert!(text.contains("superbank_get_block_not_found_total{reason=\"proven_skipped\"}"));
}

#[tokio::test]
async fn above_the_finalized_tip_returns_block_not_available_without_clickhouse() {
    let (_, state) = head_state(Instant::now());
    for commitment in ["confirmed", "finalized"] {
        for slot in [105, 1_000_000] {
            let before = metrics::get_block_not_found_count("above_head_tip");
            assert_eq!(
                get_block(&state, slot, commitment).await,
                expected(
                    JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE,
                    format!("Block not available for slot {slot}"),
                )
                .await,
                "{commitment} {slot}"
            );
            assert!(metrics::get_block_not_found_count("above_head_tip") > before);
        }
    }
}

#[tokio::test]
async fn unproven_slots_fall_through_to_clickhouse() {
    let (_, state) = head_state(Instant::now());
    // Below the chain, and a finalized link whose block the head does not serve.
    for slot in [99, 100, 103, 104] {
        assert_eq!(
            error_code(&state, slot, "confirmed").await,
            -32603,
            "{slot}"
        );
    }
}

#[tokio::test]
async fn above_tip_falls_through_while_the_head_holds_the_slot_at_that_commitment() {
    let (cache, state) = head_state(Instant::now());
    // A confirmed block with a missing transaction: get_block misses, block_identity does not.
    let mut metadata = base_block_record(105).metadata;
    metadata.executed_transaction_count = 2;
    cache.note_block_metadata(metadata);
    cache.note_slot_commitment(105, CommitmentLevel::Confirmed);
    assert_eq!(error_code(&state, 105, "confirmed").await, -32603);
    assert_eq!(
        error_code(&state, 105, "finalized").await,
        JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE
    );
}

#[tokio::test]
async fn primary_seen_at_the_slot_disables_the_above_tip_answer() {
    let (_, state) = head_state(Instant::now());
    state.latest_slot_cache.value.store(106, Ordering::Relaxed);
    for slot in [105, 106] {
        assert_eq!(
            error_code(&state, slot, "finalized").await,
            -32603,
            "{slot}"
        );
    }
    assert_eq!(
        error_code(&state, 107, "finalized").await,
        JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE
    );
    // The skip proof does not depend on the primary's tip.
    assert_eq!(
        error_code(&state, 101, "finalized").await,
        JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED
    );
}

#[tokio::test]
async fn stale_or_disconnected_head_falls_through_unchanged() {
    let cold = test_state_with_head_cache_and_clickhouse_url(
        Arc::new(HeadCache::new(64, TEST_MAX_LIMIT as usize)),
        NO_CLICKHOUSE,
    );
    let stale_at = Instant::now() - TIP_MAX_AGE - Duration::from_millis(50);
    let (_, stale) = head_state(stale_at);
    let (disconnected_cache, disconnected) = head_state(Instant::now());
    disconnected_cache.coverage.write().unwrap().disconnect();
    for slot in [101, 105] {
        let unchanged = get_block(&cold, slot, "confirmed").await;
        assert_eq!(unchanged.0, StatusCode::OK);
        assert!(
            String::from_utf8_lossy(&unchanged.1).contains("-32603"),
            "{unchanged:?}"
        );
        assert_eq!(get_block(&stale, slot, "confirmed").await, unchanged);
        assert_eq!(get_block(&disconnected, slot, "confirmed").await, unchanged);
    }
}
