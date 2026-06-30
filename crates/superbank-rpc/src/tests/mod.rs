// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use axum::{
    body::{Bytes, to_bytes},
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode},
    response::Response,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use solana_sdk::{
    hash::Hash,
    instruction::InstructionError,
    message::VersionedMessage,
    signature::{Keypair, Signer},
    transaction::{Transaction, TransactionError},
};
use solana_transaction_status::{
    BlockEncodingOptions, ConfirmedBlock, EncodedTransaction, TransactionBinaryEncoding,
    TransactionDetails, TransactionWithStatusMeta, UiConfirmedBlock, UiTransactionEncoding,
    VersionedTransactionWithStatusMeta,
};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::Semaphore;

use crate::clickhouse::{
    BlockMetadataRecord, ClickHouseClient, ClickHouseClientOptions, RoutingPolicy, RoutingScope,
    RoutingTransport, SignatureSlot, StoredBlockPayload, StoredBlockRecord,
    StoredTransactionRecord,
};
use crate::handlers::blocks::{
    handle_get_block, handle_get_block_height, handle_get_block_time, handle_get_blocks,
    handle_get_blocks_with_limit, handle_get_first_available_block, handle_get_health,
    handle_get_inflation_reward, handle_get_latest_blockhash, handle_get_slot,
    handle_get_transaction_count, handle_minimum_ledger_slot,
};
use crate::handlers::handle_json_rpc_with_headers;
use crate::handlers::signatures::{
    handle_get_signature_statuses, handle_get_signatures_for_address,
};
use crate::handlers::transactions::handle_get_transactions_for_address;
use crate::handlers::types::MAX_GET_BLOCKS_RANGE;
use crate::hydration::BlockHydrationError;
use crate::hydration::build_transaction_status_meta;
use crate::hydration::{
    TransactionHydrationError, build_versioned_transaction, hydrate_block_payload,
    hydrate_block_record, hydrate_transaction_record, parse_instruction_error_display,
    parse_transaction_error_display,
};
use crate::metrics;
use crate::rpc::json_rpc_error_response;
use crate::rpc::types::{JsonRpcRequest, JsonRpcResponse as JsonRpcResponseGeneric};
use crate::state::{AppState, LatestBlockHeightCache, LatestSlotCache, MetricsHeaderCaptureConfig};
use crate::util::current_time_millis;
#[cfg(feature = "grpc-head-cache")]
use solana_rpc_client_api::custom_error::JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_UNREACHABLE;
use solana_rpc_client_api::custom_error::{
    JSON_RPC_SERVER_ERROR_FILTER_TRANSACTION_NOT_FOUND,
    JSON_RPC_SERVER_ERROR_MIN_CONTEXT_SLOT_NOT_REACHED,
};

#[cfg(feature = "grpc-head-cache")]
use crate::head_cache::HeadCache;
#[cfg(feature = "grpc-head-cache")]
use solana_commitment_config::CommitmentLevel;
#[cfg(feature = "grpc-head-cache")]
use solana_sdk::{pubkey::Pubkey, signature::Signature};

const TEST_MAX_LIMIT: u64 = 1000;

type JsonRpcResponse = JsonRpcResponseGeneric<Value>;

#[derive(Clone, Default)]
struct SharedLogBuffer(Arc<Mutex<Vec<u8>>>);

struct SharedLogWriter(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for SharedLogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut lock = self
            .0
            .lock()
            .expect("shared log buffer mutex poisoned while writing");
        lock.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SharedLogBuffer {
    type Writer = SharedLogWriter;

    fn make_writer(&'a self) -> Self::Writer {
        SharedLogWriter(self.0.clone())
    }
}

impl SharedLogBuffer {
    fn snapshot(&self) -> String {
        let bytes = self
            .0
            .lock()
            .expect("shared log buffer mutex poisoned while reading")
            .clone();
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

fn default_routing_policy() -> RoutingPolicy {
    RoutingPolicy {
        transport: RoutingTransport::Http,
        scope: RoutingScope::Distributed,
    }
}

fn test_state_with_token_owner_activity_available(available: bool) -> Arc<AppState> {
    let cache = LatestSlotCache::new(Duration::from_millis(1000));
    cache.value.store(1, Ordering::Relaxed);
    cache
        .last_updated_ms
        .store(current_time_millis(), Ordering::Relaxed);

    let height_cache = LatestBlockHeightCache::new(Duration::from_millis(1000));
    height_cache.value.store(1, Ordering::Relaxed);
    height_cache
        .last_updated_ms
        .store(current_time_millis(), Ordering::Relaxed);

    let mut clickhouse = ClickHouseClient::new(
        "http://localhost:8123",
        "default",
        "default",
        "",
        ClickHouseClientOptions::new(
            default_routing_policy(),
            None,
            Vec::new(),
            "default.gsfa_hot".to_string(),
            "default.gsfa_hot_local".to_string(),
        ),
    );
    clickhouse.set_token_owner_activity_available_for_tests(available);

    Arc::new(AppState {
        clickhouse,
        max_signatures_limit: TEST_MAX_LIMIT,
        rpc_max_batch_size: 64,
        rpc_batch_concurrency_limit: 8,
        latest_slot_cache: cache,
        latest_block_height_cache: height_cache,
        rpc_request_timeout: Duration::from_millis(10_000),
        metrics_header_capture: Default::default(),
        hydration_sem: Arc::new(Semaphore::new(8)),
        #[cfg(feature = "grpc-head-cache")]
        head_cache: None,
        #[cfg(feature = "disk-cache")]
        disk_cache: None,
    })
}

fn test_state() -> Arc<AppState> {
    test_state_with_token_owner_activity_available(true)
}

fn test_state_with_metrics_header_capture(capture: MetricsHeaderCaptureConfig) -> Arc<AppState> {
    let mut state = match Arc::try_unwrap(test_state()) {
        Ok(state) => state,
        Err(_) => panic!("test_state should have a single Arc owner"),
    };
    state.metrics_header_capture = capture;
    Arc::new(state)
}

fn test_state_with_clickhouse_latest_slot(latest_slot: Option<u64>) -> Arc<AppState> {
    let mut state = match Arc::try_unwrap(test_state()) {
        Ok(state) => state,
        Err(_) => panic!("test_state should have a single Arc owner"),
    };
    state
        .clickhouse
        .set_latest_finalized_slot_for_tests(latest_slot);
    Arc::new(state)
}

fn test_state_with_clickhouse_url(clickhouse_url: &str) -> Arc<AppState> {
    let cache = LatestSlotCache::new(Duration::from_millis(1000));
    cache.value.store(1, Ordering::Relaxed);
    cache
        .last_updated_ms
        .store(current_time_millis(), Ordering::Relaxed);

    let height_cache = LatestBlockHeightCache::new(Duration::from_millis(1000));
    height_cache.value.store(1, Ordering::Relaxed);
    height_cache
        .last_updated_ms
        .store(current_time_millis(), Ordering::Relaxed);

    let mut clickhouse = ClickHouseClient::new(
        clickhouse_url,
        "default",
        "default",
        "",
        ClickHouseClientOptions::new(
            default_routing_policy(),
            None,
            Vec::new(),
            "default.gsfa_hot".to_string(),
            "default.gsfa_hot_local".to_string(),
        ),
    );
    clickhouse.set_token_owner_activity_available_for_tests(true);

    Arc::new(AppState {
        clickhouse,
        max_signatures_limit: TEST_MAX_LIMIT,
        rpc_max_batch_size: 64,
        rpc_batch_concurrency_limit: 8,
        latest_slot_cache: cache,
        latest_block_height_cache: height_cache,
        rpc_request_timeout: Duration::from_millis(10_000),
        metrics_header_capture: Default::default(),
        hydration_sem: Arc::new(Semaphore::new(8)),
        #[cfg(feature = "grpc-head-cache")]
        head_cache: None,
        #[cfg(feature = "disk-cache")]
        disk_cache: None,
    })
}

async fn test_state_with_clickhouse_cached_signature_slot(
    clickhouse_url: &str,
    signature_bytes: [u8; 64],
    value: Option<SignatureSlot>,
) -> Arc<AppState> {
    let cache = LatestSlotCache::new(Duration::from_millis(1000));
    cache.value.store(1, Ordering::Relaxed);
    cache
        .last_updated_ms
        .store(current_time_millis(), Ordering::Relaxed);

    let height_cache = LatestBlockHeightCache::new(Duration::from_millis(1000));
    height_cache.value.store(1, Ordering::Relaxed);
    height_cache
        .last_updated_ms
        .store(current_time_millis(), Ordering::Relaxed);

    let mut clickhouse = ClickHouseClient::new(
        clickhouse_url,
        "default",
        "default",
        "",
        ClickHouseClientOptions::new(
            default_routing_policy(),
            None,
            Vec::new(),
            "default.gsfa_hot".to_string(),
            "default.gsfa_hot_local".to_string(),
        ),
    );
    clickhouse.set_token_owner_activity_available_for_tests(true);

    clickhouse
        .signature_slot_cache
        .prime_for_tests(signature_bytes, value)
        .await;

    Arc::new(AppState {
        clickhouse,
        max_signatures_limit: TEST_MAX_LIMIT,
        rpc_max_batch_size: 64,
        rpc_batch_concurrency_limit: 8,
        latest_slot_cache: cache,
        latest_block_height_cache: height_cache,
        rpc_request_timeout: Duration::from_millis(10_000),
        metrics_header_capture: Default::default(),
        hydration_sem: Arc::new(Semaphore::new(8)),
        #[cfg(feature = "grpc-head-cache")]
        head_cache: None,
        #[cfg(feature = "disk-cache")]
        disk_cache: None,
    })
}

#[cfg(feature = "grpc-head-cache")]
fn test_state_with_head_cache(head_cache: Arc<HeadCache>) -> Arc<AppState> {
    let latest_slot_cache = LatestSlotCache::new(Duration::from_millis(1000));
    latest_slot_cache.value.store(1, Ordering::Relaxed);
    latest_slot_cache
        .last_updated_ms
        .store(current_time_millis(), Ordering::Relaxed);

    let latest_block_height_cache = LatestBlockHeightCache::new(Duration::from_millis(1000));
    latest_block_height_cache.value.store(1, Ordering::Relaxed);
    latest_block_height_cache
        .last_updated_ms
        .store(current_time_millis(), Ordering::Relaxed);

    let mut clickhouse = ClickHouseClient::new(
        "http://localhost:8123",
        "default",
        "default",
        "",
        ClickHouseClientOptions::new(
            default_routing_policy(),
            None,
            Vec::new(),
            "default.gsfa_hot".to_string(),
            "default.gsfa_hot_local".to_string(),
        ),
    );
    clickhouse.set_token_owner_activity_available_for_tests(true);

    Arc::new(AppState {
        clickhouse,
        max_signatures_limit: TEST_MAX_LIMIT,
        rpc_max_batch_size: 64,
        rpc_batch_concurrency_limit: 8,
        latest_slot_cache,
        latest_block_height_cache,
        rpc_request_timeout: Duration::from_millis(10_000),
        metrics_header_capture: Default::default(),
        hydration_sem: Arc::new(Semaphore::new(8)),
        head_cache: Some(head_cache),
        #[cfg(feature = "disk-cache")]
        disk_cache: None,
    })
}

#[cfg(feature = "grpc-head-cache")]
fn test_state_with_head_cache_and_clickhouse_url(
    head_cache: Arc<HeadCache>,
    clickhouse_url: &str,
) -> Arc<AppState> {
    let latest_slot_cache = LatestSlotCache::new(Duration::from_millis(1000));
    latest_slot_cache.value.store(1, Ordering::Relaxed);
    latest_slot_cache
        .last_updated_ms
        .store(current_time_millis(), Ordering::Relaxed);

    let latest_block_height_cache = LatestBlockHeightCache::new(Duration::from_millis(1000));
    latest_block_height_cache.value.store(1, Ordering::Relaxed);
    latest_block_height_cache
        .last_updated_ms
        .store(current_time_millis(), Ordering::Relaxed);

    let mut clickhouse = ClickHouseClient::new(
        clickhouse_url,
        "default",
        "default",
        "",
        ClickHouseClientOptions::new(
            default_routing_policy(),
            None,
            Vec::new(),
            "default.gsfa_hot".to_string(),
            "default.gsfa_hot_local".to_string(),
        ),
    );
    clickhouse.set_token_owner_activity_available_for_tests(true);

    Arc::new(AppState {
        clickhouse,
        max_signatures_limit: TEST_MAX_LIMIT,
        rpc_max_batch_size: 64,
        rpc_batch_concurrency_limit: 8,
        latest_slot_cache,
        latest_block_height_cache,
        rpc_request_timeout: Duration::from_millis(10_000),
        metrics_header_capture: Default::default(),
        hydration_sem: Arc::new(Semaphore::new(8)),
        head_cache: Some(head_cache),
        #[cfg(feature = "disk-cache")]
        disk_cache: None,
    })
}

#[cfg(feature = "grpc-head-cache")]
async fn test_state_with_head_cache_and_cached_signature_slot(
    head_cache: Arc<HeadCache>,
    clickhouse_url: &str,
    signature_bytes: [u8; 64],
    value: Option<SignatureSlot>,
) -> Arc<AppState> {
    let latest_slot_cache = LatestSlotCache::new(Duration::from_millis(1000));
    latest_slot_cache.value.store(1, Ordering::Relaxed);
    latest_slot_cache
        .last_updated_ms
        .store(current_time_millis(), Ordering::Relaxed);

    let latest_block_height_cache = LatestBlockHeightCache::new(Duration::from_millis(1000));
    latest_block_height_cache.value.store(1, Ordering::Relaxed);
    latest_block_height_cache
        .last_updated_ms
        .store(current_time_millis(), Ordering::Relaxed);

    let mut clickhouse = ClickHouseClient::new(
        clickhouse_url,
        "default",
        "default",
        "",
        ClickHouseClientOptions::new(
            default_routing_policy(),
            None,
            Vec::new(),
            "default.gsfa_hot".to_string(),
            "default.gsfa_hot_local".to_string(),
        ),
    );
    clickhouse.set_token_owner_activity_available_for_tests(true);

    clickhouse
        .signature_slot_cache
        .prime_for_tests(signature_bytes, value)
        .await;

    Arc::new(AppState {
        clickhouse,
        max_signatures_limit: TEST_MAX_LIMIT,
        rpc_max_batch_size: 64,
        rpc_batch_concurrency_limit: 8,
        latest_slot_cache,
        latest_block_height_cache,
        rpc_request_timeout: Duration::from_millis(10_000),
        metrics_header_capture: Default::default(),
        hydration_sem: Arc::new(Semaphore::new(8)),
        head_cache: Some(head_cache),
        #[cfg(feature = "disk-cache")]
        disk_cache: None,
    })
}

async fn parse_json_rpc_response(response: Response) -> JsonRpcResponse {
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    serde_json::from_slice(&bytes).expect("json parse")
}

async fn parse_json_value_response(response: Response) -> Value {
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    serde_json::from_slice(&bytes).expect("json parse")
}

async fn handle_json_rpc_body_with_headers(
    state: Arc<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    tokio::spawn(async move {
        handle_json_rpc_with_headers(State(state), headers, body)
            .await
            .expect("handler response")
    })
    .await
    .expect("handler task should not panic")
}

async fn handle_json_rpc_value_with_headers(
    state: Arc<AppState>,
    headers: HeaderMap,
    value: &Value,
) -> Response {
    let body = serde_json::to_vec(value).expect("serialize request");
    handle_json_rpc_body_with_headers(state, headers, body.into()).await
}

async fn handle_json_rpc_value(state: Arc<AppState>, value: &Value) -> Response {
    handle_json_rpc_value_with_headers(state, HeaderMap::new(), value).await
}

async fn handle_json_rpc_request(state: Arc<AppState>, request: &JsonRpcRequest) -> Response {
    let value = serde_json::to_value(request).expect("serialize request");
    handle_json_rpc_value(state, &value).await
}

fn base_transaction_record() -> StoredTransactionRecord {
    StoredTransactionRecord {
        signature: [0u8; 64],
        slot: 1,
        slot_idx: 0,
        block_time: Some(123),
        tx_version: None,
        tx_signatures: vec![[0u8; 64]],
        tx_num_required_signatures: 1,
        tx_num_readonly_signed_accounts: 0,
        tx_num_readonly_unsigned_accounts: 0,
        tx_account_keys: vec![[0u8; 32]],
        tx_recent_blockhash: [0u8; 32],
        tx_instructions_program_id_index: Vec::new(),
        tx_instructions_accounts: Vec::new(),
        tx_instructions_data: Vec::new(),
        tx_address_table_lookups_present: false,
        tx_address_table_lookup_account_key: Vec::new(),
        tx_address_table_lookup_writable_indexes: Vec::new(),
        tx_address_table_lookup_readonly_indexes: Vec::new(),
        meta_status_ok: true,
        meta_err: None,
        meta_fee: 0,
        meta_pre_balances: Vec::new(),
        meta_post_balances: Vec::new(),
        meta_inner_instructions_present: false,
        meta_inner_instructions_index: Vec::new(),
        meta_inner_instructions_program_id_index: Vec::new(),
        meta_inner_instructions_accounts: Vec::new(),
        meta_inner_instructions_data: Vec::new(),
        meta_inner_instructions_stack_height: Vec::new(),
        meta_log_messages_present: false,
        meta_log_messages: Vec::new(),
        meta_pre_token_balances_present: false,
        meta_pre_token_account_index: Vec::new(),
        meta_pre_token_mint: Vec::new(),
        meta_pre_token_owner: Vec::new(),
        meta_pre_token_program_id: Vec::new(),
        meta_pre_token_amount: Vec::new(),
        meta_pre_token_decimals: Vec::new(),
        meta_pre_token_ui_amount: Vec::new(),
        meta_pre_token_ui_amount_string: Vec::new(),
        meta_post_token_balances_present: false,
        meta_post_token_account_index: Vec::new(),
        meta_post_token_mint: Vec::new(),
        meta_post_token_owner: Vec::new(),
        meta_post_token_program_id: Vec::new(),
        meta_post_token_amount: Vec::new(),
        meta_post_token_decimals: Vec::new(),
        meta_post_token_ui_amount: Vec::new(),
        meta_post_token_ui_amount_string: Vec::new(),
        meta_rewards_present: false,
        meta_reward_pubkey: Vec::new(),
        meta_reward_lamports: Vec::new(),
        meta_reward_post_balance: Vec::new(),
        meta_reward_type: Vec::new(),
        meta_reward_commission: Vec::new(),
        meta_loaded_addresses_writable: Vec::new(),
        meta_loaded_addresses_readonly: Vec::new(),
        meta_return_data_present: false,
        meta_return_data_program_id: None,
        meta_return_data_data: None,
        meta_compute_units_consumed: None,
        meta_cost_units: None,
    }
}

fn base_block_record(slot: u64) -> StoredBlockRecord {
    StoredBlockRecord {
        metadata: BlockMetadataRecord {
            slot,
            parent_slot: slot.saturating_sub(1),
            blockhash: [1u8; 32],
            parent_blockhash: [2u8; 32],
            block_time: Some(0),
            block_height: Some(0),
            executed_transaction_count: 0,
            entry_count: 0,
            rewards_present: false,
            rewards_pubkey: Vec::new(),
            rewards_lamports: Vec::new(),
            rewards_post_balance: Vec::new(),
            rewards_type: Vec::new(),
            rewards_commission: Vec::new(),
            rewards_num_partitions: None,
        },
        transactions: Vec::new(),
    }
}

#[cfg(feature = "grpc-head-cache")]
fn head_cache_metadata(
    slot: u64,
    parent_slot: u64,
    executed_transaction_count: u64,
) -> BlockMetadataRecord {
    let mut metadata = base_block_record(slot).metadata;
    metadata.parent_slot = parent_slot;
    metadata.block_time = Some(1_700_000_000 + slot as i64);
    metadata.block_height = Some(slot);
    metadata.executed_transaction_count = executed_transaction_count;
    metadata.entry_count = executed_transaction_count;
    metadata
}

fn projection_equivalence_block_record() -> StoredBlockRecord {
    let mut block = base_block_record(10);
    block.metadata.block_time = Some(1_700_000_000);
    block.metadata.block_height = Some(123);
    block.metadata.executed_transaction_count = 2;

    let mut first = base_transaction_record();
    first.signature = [11u8; 64];
    first.tx_signatures = vec![[11u8; 64]];
    first.tx_account_keys = vec![[1u8; 32], [2u8; 32]];
    first.tx_num_required_signatures = 1;
    first.tx_num_readonly_unsigned_accounts = 1;
    first.meta_pre_balances = vec![10, 20];
    first.meta_post_balances = vec![11, 19];

    let mut second = base_transaction_record();
    second.signature = [12u8; 64];
    second.tx_signatures = vec![[12u8; 64]];
    second.tx_account_keys = vec![[3u8; 32], [4u8; 32]];
    second.tx_num_required_signatures = 1;
    second.tx_num_readonly_unsigned_accounts = 1;
    second.meta_pre_balances = vec![30, 40];
    second.meta_post_balances = vec![31, 39];

    block.transactions = vec![first, second];
    block
}

fn encode_block_via_legacy_full_path(
    record: StoredBlockRecord,
    encoding: UiTransactionEncoding,
    transaction_details: TransactionDetails,
    max_supported_transaction_version: Option<u8>,
) -> Result<UiConfirmedBlock, BlockHydrationError> {
    let metadata = record.metadata;
    let block_time = metadata.block_time.filter(|value| *value != 0);
    let block_height = match metadata.block_height {
        Some(0) if metadata.slot != 0 => None,
        other => other,
    };

    let mut transactions = Vec::with_capacity(record.transactions.len());
    for tx_record in record.transactions {
        let meta = build_transaction_status_meta(&tx_record)?
            .expect("projection equivalence test requires metadata-present transactions");
        let transaction = build_versioned_transaction(&tx_record)?;
        transactions.push(TransactionWithStatusMeta::Complete(
            VersionedTransactionWithStatusMeta { transaction, meta },
        ));
    }

    ConfirmedBlock {
        previous_blockhash: Hash::from(metadata.parent_blockhash).to_string(),
        blockhash: Hash::from(metadata.blockhash).to_string(),
        parent_slot: metadata.parent_slot,
        transactions,
        rewards: Vec::new(),
        num_partitions: metadata.rewards_num_partitions,
        block_time,
        block_height,
    }
    .encode_with_options(
        encoding,
        BlockEncodingOptions {
            transaction_details,
            show_rewards: false,
            max_supported_transaction_version,
        },
    )
    .map_err(BlockHydrationError::from)
}

#[test]
fn hydrate_transaction_record_base64_round_trip() {
    let payer = Keypair::new();
    let tx =
        Transaction::new_signed_with_payer(&[], Some(&payer.pubkey()), &[&payer], Hash::default());
    let versioned = solana_sdk::transaction::VersionedTransaction::from(tx);
    let mut meta = solana_transaction_status::TransactionStatusMeta::default();
    let message = match &versioned.message {
        VersionedMessage::Legacy(message) => message,
        VersionedMessage::V0(_) => panic!("expected legacy message"),
    };
    meta.pre_balances = vec![0; message.account_keys.len()];
    meta.post_balances = vec![0; message.account_keys.len()];

    let stored = StoredTransactionRecord {
        signature: *versioned.signatures[0].as_array(),
        slot: 99,
        slot_idx: 7,
        block_time: Some(123),
        tx_version: None,
        tx_signatures: versioned
            .signatures
            .iter()
            .map(|sig| *sig.as_array())
            .collect(),
        tx_num_required_signatures: message.header.num_required_signatures,
        tx_num_readonly_signed_accounts: message.header.num_readonly_signed_accounts,
        tx_num_readonly_unsigned_accounts: message.header.num_readonly_unsigned_accounts,
        tx_account_keys: message
            .account_keys
            .iter()
            .map(|key| key.to_bytes())
            .collect(),
        tx_recent_blockhash: message.recent_blockhash.to_bytes(),
        tx_instructions_program_id_index: message
            .instructions
            .iter()
            .map(|ix| ix.program_id_index)
            .collect(),
        tx_instructions_accounts: message
            .instructions
            .iter()
            .map(|ix| ix.accounts.clone())
            .collect(),
        tx_instructions_data: message
            .instructions
            .iter()
            .map(|ix| ix.data.clone())
            .collect(),
        tx_address_table_lookups_present: false,
        tx_address_table_lookup_account_key: Vec::new(),
        tx_address_table_lookup_writable_indexes: Vec::new(),
        tx_address_table_lookup_readonly_indexes: Vec::new(),
        meta_status_ok: meta.status.is_ok(),
        meta_err: None,
        meta_fee: meta.fee,
        meta_pre_balances: meta.pre_balances.clone(),
        meta_post_balances: meta.post_balances.clone(),
        meta_inner_instructions_present: false,
        meta_inner_instructions_index: Vec::new(),
        meta_inner_instructions_program_id_index: Vec::new(),
        meta_inner_instructions_accounts: Vec::new(),
        meta_inner_instructions_data: Vec::new(),
        meta_inner_instructions_stack_height: Vec::new(),
        meta_log_messages_present: false,
        meta_log_messages: Vec::new(),
        meta_pre_token_balances_present: false,
        meta_pre_token_account_index: Vec::new(),
        meta_pre_token_mint: Vec::new(),
        meta_pre_token_owner: Vec::new(),
        meta_pre_token_program_id: Vec::new(),
        meta_pre_token_amount: Vec::new(),
        meta_pre_token_decimals: Vec::new(),
        meta_pre_token_ui_amount: Vec::new(),
        meta_pre_token_ui_amount_string: Vec::new(),
        meta_post_token_balances_present: false,
        meta_post_token_account_index: Vec::new(),
        meta_post_token_mint: Vec::new(),
        meta_post_token_owner: Vec::new(),
        meta_post_token_program_id: Vec::new(),
        meta_post_token_amount: Vec::new(),
        meta_post_token_decimals: Vec::new(),
        meta_post_token_ui_amount: Vec::new(),
        meta_post_token_ui_amount_string: Vec::new(),
        meta_rewards_present: false,
        meta_reward_pubkey: Vec::new(),
        meta_reward_lamports: Vec::new(),
        meta_reward_post_balance: Vec::new(),
        meta_reward_type: Vec::new(),
        meta_reward_commission: Vec::new(),
        meta_loaded_addresses_writable: Vec::new(),
        meta_loaded_addresses_readonly: Vec::new(),
        meta_return_data_present: false,
        meta_return_data_program_id: None,
        meta_return_data_data: None,
        meta_compute_units_consumed: meta.compute_units_consumed,
        meta_cost_units: meta.cost_units,
    };

    let encoded = hydrate_transaction_record(&stored, UiTransactionEncoding::Base64, Some(0))
        .expect("encode succeeds");

    assert_eq!(encoded.slot, 99);
    assert_eq!(encoded.block_time, Some(123));
    assert_eq!(encoded.transaction_index, Some(7));
    match encoded.transaction.transaction {
        EncodedTransaction::Binary(blob, TransactionBinaryEncoding::Base64) => {
            let decoded = STANDARD.decode(blob).expect("base64 decode");
            let expected = bincode::serialize(&versioned).expect("serialize tx");
            assert_eq!(decoded, expected);
        }
        other => panic!("Unexpected encoded payload: {:?}", other),
    }
    let meta_out = encoded.transaction.meta.expect("meta present");
    assert_eq!(meta_out.fee, meta.fee);
}

#[tokio::test]
async fn get_signatures_for_address_rejects_invalid_address() {
    // ClickHouse won't be contacted because validation exits early
    let state = test_state();

    let response =
        handle_get_signatures_for_address(state, json!(1), Some(vec![json!("not_base58")]))
            .await
            .expect("response");

    let (parts, body) = response.into_parts();
    assert_eq!(parts.status, StatusCode::OK);

    let body_bytes = to_bytes(body, usize::MAX).await.expect("body bytes");
    let parsed: JsonRpcResponse = serde_json::from_slice(&body_bytes).expect("json parse");

    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid param: Invalid");
    assert!(parsed.result.is_none());
}

#[tokio::test]
async fn get_signatures_for_address_rejects_zero_limit() {
    let state = test_state();

    let response = handle_get_signatures_for_address(
        state,
        json!(1),
        Some(vec![
            json!("11111111111111111111111111111111"), // valid base58 pubkey
            json!({ "limit": 0 }),
        ]),
    )
    .await
    .expect("response");

    let (parts, body) = response.into_parts();
    assert_eq!(parts.status, StatusCode::OK);

    let body_bytes = to_bytes(body, usize::MAX).await.expect("body bytes");
    let parsed: JsonRpcResponse = serde_json::from_slice(&body_bytes).expect("json parse");

    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(
        err.message,
        format!("Invalid limit; max {}", TEST_MAX_LIMIT)
    );
    assert!(parsed.result.is_none());
}

#[tokio::test]
async fn get_signatures_for_address_rejects_non_string_address() {
    let state = test_state();

    let response = handle_get_signatures_for_address(
        state,
        serde_json::json!(1),
        Some(vec![serde_json::json!(12345)]), // not a string
    )
    .await
    .expect("response");

    let (parts, body) = response.into_parts();
    assert_eq!(parts.status, StatusCode::OK);

    let body_bytes = to_bytes(body, usize::MAX).await.expect("body bytes");
    let parsed: JsonRpcResponse = serde_json::from_slice(&body_bytes).expect("json parse");

    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid params: address must be a string");
    assert!(parsed.result.is_none());
}

#[tokio::test]
async fn get_transactions_for_address_rejects_invalid_address() {
    let state = test_state();

    let response =
        handle_get_transactions_for_address(state, json!(1), Some(vec![json!("not_base58")]))
            .await
            .expect("response");

    let (parts, body) = response.into_parts();
    assert_eq!(parts.status, StatusCode::OK);

    let body_bytes = to_bytes(body, usize::MAX).await.expect("body bytes");
    let parsed: JsonRpcResponse = serde_json::from_slice(&body_bytes).expect("json parse");

    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid param: Invalid");
    assert!(parsed.result.is_none());
}

#[tokio::test]
async fn get_transactions_for_address_rejects_zero_limit() {
    let state = test_state();

    let response = handle_get_transactions_for_address(
        state,
        json!(1),
        Some(vec![
            json!("11111111111111111111111111111111"),
            json!({ "limit": 0 }),
        ]),
    )
    .await
    .expect("response");

    let (parts, body) = response.into_parts();
    assert_eq!(parts.status, StatusCode::OK);

    let body_bytes = to_bytes(body, usize::MAX).await.expect("body bytes");
    let parsed: JsonRpcResponse = serde_json::from_slice(&body_bytes).expect("json parse");

    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid limit; max 1000");
    assert!(parsed.result.is_none());
}

#[tokio::test]
async fn get_transactions_for_address_rejects_invalid_status_filter() {
    let state = test_state();

    let response = handle_get_transactions_for_address(
        state,
        json!(1),
        Some(vec![
            json!("11111111111111111111111111111111"),
            json!({ "filters": { "status": "nope" } }),
        ]),
    )
    .await
    .expect("response");

    let (parts, body) = response.into_parts();
    assert_eq!(parts.status, StatusCode::OK);

    let body_bytes = to_bytes(body, usize::MAX).await.expect("body bytes");
    let parsed: JsonRpcResponse = serde_json::from_slice(&body_bytes).expect("json parse");

    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert!(err.message.contains("unsupported status"));
}

#[tokio::test]
async fn get_transactions_for_address_rejects_token_accounts_when_table_missing() {
    let state = test_state_with_token_owner_activity_available(false);

    let response = handle_get_transactions_for_address(
        state,
        json!(1),
        Some(vec![
            json!("11111111111111111111111111111111"),
            json!({ "filters": { "tokenAccounts": "all" } }),
        ]),
    )
    .await
    .expect("response");

    let (parts, body) = response.into_parts();
    assert_eq!(parts.status, StatusCode::OK);

    let body_bytes = to_bytes(body, usize::MAX).await.expect("body bytes");
    let parsed: JsonRpcResponse = serde_json::from_slice(&body_bytes).expect("json parse");

    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert!(err.message.contains("tokenAccounts"));
    assert!(parsed.result.is_none());
}

#[tokio::test]
async fn handle_json_rpc_missing_params_returns_json_rpc_error() {
    let state = test_state();

    let request = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        id: json!(1),
        method: "getSignaturesForAddress".to_string(),
        params: None,
    };

    let response = handle_json_rpc_request(state, &request).await;

    assert_eq!(response.status(), StatusCode::OK);

    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let parsed: JsonRpcResponse = serde_json::from_slice(&bytes).expect("json parse");

    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid params: missing address");
    assert!(parsed.result.is_none());
}

#[tokio::test]
async fn handle_json_rpc_non_string_address_returns_json_rpc_error() {
    let state = test_state();

    let request = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        id: json!(1),
        method: "getSignaturesForAddress".to_string(),
        params: Some(vec![json!(12345)]),
    };

    let response = handle_json_rpc_request(state, &request).await;

    assert_eq!(response.status(), StatusCode::OK);

    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let parsed: JsonRpcResponse = serde_json::from_slice(&bytes).expect("json parse");

    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid params: address must be a string");
    assert!(parsed.result.is_none());
}

#[tokio::test]
async fn handle_json_rpc_invalid_address_matches_solana_error() {
    let state = test_state();

    let request = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        id: json!(1),
        method: "getSignaturesForAddress".to_string(),
        params: Some(vec![json!("not_base58")]),
    };

    let response = handle_json_rpc_request(state, &request).await;

    assert_eq!(response.status(), StatusCode::OK);

    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let parsed: JsonRpcResponse = serde_json::from_slice(&bytes).expect("json parse");

    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid param: Invalid");
    assert!(parsed.result.is_none());
}

#[tokio::test]
async fn get_signatures_for_address_rejects_invalid_before_signature() {
    let state = test_state();

    let response = handle_get_signatures_for_address(
        state,
        json!(1),
        Some(vec![
            json!("11111111111111111111111111111111"),
            json!({ "before": "not_base58" }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(
        err.message,
        "Invalid params: before must be a valid signature"
    );
}

#[tokio::test]
async fn get_signatures_for_address_rejects_invalid_until_signature() {
    let state = test_state();

    let response = handle_get_signatures_for_address(
        state,
        json!(1),
        Some(vec![
            json!("11111111111111111111111111111111"),
            json!({ "until": "not_base58" }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(
        err.message,
        "Invalid params: until must be a valid signature"
    );
}

#[tokio::test]
async fn get_signatures_for_address_rejects_before_and_before_slot() {
    let state = test_state();
    let signature = bs58::encode([1u8; 64]).into_string();

    let response = handle_get_signatures_for_address(
        state,
        json!(1),
        Some(vec![
            json!("11111111111111111111111111111111"),
            json!({ "before": signature, "beforeSlot": 123 }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(
        err.message,
        "Invalid params: before and beforeSlot are mutually exclusive"
    );
}

#[tokio::test]
async fn get_signatures_for_address_rejects_until_and_until_slot() {
    let state = test_state();
    let signature = bs58::encode([2u8; 64]).into_string();

    let response = handle_get_signatures_for_address(
        state,
        json!(1),
        Some(vec![
            json!("11111111111111111111111111111111"),
            json!({ "until": signature, "untilSlot": 123 }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(
        err.message,
        "Invalid params: until and untilSlot are mutually exclusive"
    );
}

#[tokio::test]
async fn get_signatures_for_address_missing_before_returns_filter_transaction_not_found() {
    let missing_signature_bytes = [42u8; 64];
    let state = test_state_with_clickhouse_cached_signature_slot(
        "http://127.0.0.1:1",
        missing_signature_bytes,
        None,
    )
    .await;
    let missing_signature = bs58::encode(missing_signature_bytes).into_string();

    let response = handle_get_signatures_for_address(
        state,
        json!(1),
        Some(vec![
            json!("11111111111111111111111111111111"),
            json!({ "before": missing_signature }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(
        err.code,
        JSON_RPC_SERVER_ERROR_FILTER_TRANSACTION_NOT_FOUND as i32
    );
    assert_eq!(
        err.message,
        format!("Transaction {missing_signature} not found")
    );
    assert_eq!(err.data, None);
}

#[tokio::test]
async fn get_signatures_for_address_missing_until_returns_filter_transaction_not_found() {
    let missing_signature_bytes = [43u8; 64];
    let state = test_state_with_clickhouse_cached_signature_slot(
        "http://127.0.0.1:1",
        missing_signature_bytes,
        None,
    )
    .await;
    let missing_signature = bs58::encode(missing_signature_bytes).into_string();

    let response = handle_get_signatures_for_address(
        state,
        json!(1),
        Some(vec![
            json!("11111111111111111111111111111111"),
            json!({ "until": missing_signature }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(
        err.code,
        JSON_RPC_SERVER_ERROR_FILTER_TRANSACTION_NOT_FOUND as i32
    );
    assert_eq!(
        err.message,
        format!("Transaction {missing_signature} not found")
    );
    assert_eq!(err.data, None);
}

#[tokio::test]
async fn get_signatures_for_address_rejects_unknown_format_option() {
    let state = test_state();

    let response = handle_get_signatures_for_address(
        state,
        json!(1),
        Some(vec![
            json!("11111111111111111111111111111111"),
            json!({ "format": "json" }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert!(err.message.contains("failed to parse options"));
    assert!(err.message.contains("format"));
}

#[tokio::test]
async fn get_signatures_for_address_rejects_unknown_columns_option() {
    let state = test_state();

    let response = handle_get_signatures_for_address(
        state,
        json!(1),
        Some(vec![
            json!("11111111111111111111111111111111"),
            json!({ "columns": ["signature"] }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert!(err.message.contains("failed to parse options"));
    assert!(err.message.contains("columns"));
}

#[tokio::test]
async fn get_signature_statuses_invalid_entries_return_nulls() {
    let state = test_state();

    let response = handle_get_signature_statuses(
        state,
        json!(1),
        Some(vec![json!(["not_base58", 12345, "also_bad"])]),
    )
    .await
    .expect("response");

    let (parts, body) = response.into_parts();
    assert_eq!(parts.status, StatusCode::OK);

    let body_bytes = to_bytes(body, usize::MAX).await.expect("body bytes");
    let parsed: JsonRpcResponse = serde_json::from_slice(&body_bytes).expect("json parse");

    assert!(parsed.error.is_none());
    let result = parsed.result.expect("result present");
    let context_slot = result
        .get("context")
        .and_then(|ctx| ctx.get("slot"))
        .and_then(|slot| slot.as_u64())
        .expect("context slot");
    assert_eq!(context_slot, 1);

    let values = result
        .get("value")
        .and_then(|value| value.as_array())
        .expect("value array");
    assert_eq!(values.len(), 3);
    assert!(values.iter().all(|entry| entry.is_null()));
}

#[tokio::test]
async fn get_signature_statuses_rejects_too_many_signatures() {
    let state = test_state();

    let signatures = vec![json!("bad"); 257];
    let response =
        handle_get_signature_statuses(state, json!(1), Some(vec![Value::Array(signatures)]))
            .await
            .expect("response");

    let (parts, body) = response.into_parts();
    assert_eq!(parts.status, StatusCode::OK);

    let body_bytes = to_bytes(body, usize::MAX).await.expect("body bytes");
    let parsed: JsonRpcResponse = serde_json::from_slice(&body_bytes).expect("json parse");

    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid params: too many signatures (max 256)");
    assert!(parsed.result.is_none());
}

#[tokio::test]
async fn get_block_rejects_missing_slot() {
    let state = test_state();

    let response = handle_get_block(state, json!(1), None)
        .await
        .expect("response");

    let (parts, body) = response.into_parts();
    assert_eq!(parts.status, StatusCode::OK);

    let body_bytes = to_bytes(body, usize::MAX).await.expect("body bytes");
    let parsed: JsonRpcResponse = serde_json::from_slice(&body_bytes).expect("json parse");

    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid params: missing slot");
}

#[tokio::test]
async fn get_block_rejects_non_numeric_slot() {
    let state = test_state();

    let response = handle_get_block(state, json!(1), Some(vec![json!("not_a_slot")]))
        .await
        .expect("response");

    let (parts, body) = response.into_parts();
    assert_eq!(parts.status, StatusCode::OK);

    let body_bytes = to_bytes(body, usize::MAX).await.expect("body bytes");
    let parsed: JsonRpcResponse = serde_json::from_slice(&body_bytes).expect("json parse");

    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid params: slot must be a number");
}

#[tokio::test]
async fn get_block_rejects_processed_commitment() {
    let state = test_state();

    let response = handle_get_block(
        state,
        json!(1),
        Some(vec![json!(0u64), json!({ "commitment": "processed" })]),
    )
    .await
    .expect("response");

    let (parts, body) = response.into_parts();
    assert_eq!(parts.status, StatusCode::OK);

    let body_bytes = to_bytes(body, usize::MAX).await.expect("body bytes");
    let parsed: JsonRpcResponse = serde_json::from_slice(&body_bytes).expect("json parse");

    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(
        err.message,
        "Only confirmed or finalized commitments are supported"
    );
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_block_signatures_served_from_head_cache_when_clickhouse_unreachable() {
    let cache = Arc::new(HeadCache::new(32, TEST_MAX_LIMIT as usize));
    let slot = 42u64;
    let address = Pubkey::new_unique();
    let sig_bytes = [7u8; 64];
    let sig = Signature::from(sig_bytes);
    let sig_str = bs58::encode(sig_bytes).into_string();

    let mut record = base_transaction_record();
    record.slot = slot;
    record.signature = sig_bytes;
    record.tx_signatures = vec![sig_bytes];
    record.tx_account_keys = vec![address.to_bytes()];
    record.meta_pre_balances = vec![1];
    record.meta_post_balances = vec![1];

    cache.insert_for_tests(sig, record, 3, &[address], CommitmentLevel::Confirmed);

    let mut metadata = base_block_record(slot).metadata;
    metadata.executed_transaction_count = 1;
    metadata.block_height = Some(99);
    metadata.block_time = Some(1_700_000_123);
    cache.note_block_metadata(metadata);

    let state = test_state_with_head_cache_and_clickhouse_url(cache, "http://127.0.0.1:1");
    let response = handle_get_block(
        state,
        json!(1),
        Some(vec![
            json!(slot),
            json!({
                "transactionDetails": "signatures",
                "commitment": "confirmed"
            }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    assert!(
        parsed.error.is_none(),
        "expected success: {:?}",
        parsed.error
    );
    let result = parsed.result.expect("result");
    let signatures = result
        .get("signatures")
        .and_then(|value| value.as_array())
        .expect("signatures array");
    assert_eq!(signatures.len(), 1);
    assert_eq!(signatures[0].as_str(), Some(sig_str.as_str()));
    assert!(result.get("transactions").is_none());
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_block_none_served_from_head_cache_without_rewards() {
    let cache = Arc::new(HeadCache::new(32, TEST_MAX_LIMIT as usize));
    let slot = 43u64;

    let mut metadata = base_block_record(slot).metadata;
    metadata.executed_transaction_count = 0;
    metadata.block_height = Some(100);
    metadata.block_time = Some(1_700_000_124);
    cache.note_block_metadata(metadata);
    cache.note_slot_commitment(slot, CommitmentLevel::Confirmed);

    let state = test_state_with_head_cache_and_clickhouse_url(cache, "http://127.0.0.1:1");
    let response = handle_get_block(
        state,
        json!(1),
        Some(vec![
            json!(slot),
            json!({
                "transactionDetails": "none",
                "rewards": false,
                "commitment": "confirmed"
            }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    assert!(
        parsed.error.is_none(),
        "expected success: {:?}",
        parsed.error
    );
    let result = parsed.result.expect("result");
    assert!(result.get("transactions").is_none());
    assert!(result.get("signatures").is_none());
    assert!(result.get("rewards").is_none());
    assert_eq!(
        result.get("blockHeight").and_then(|value| value.as_u64()),
        Some(100)
    );
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_block_none_served_from_head_cache_for_incomplete_nonempty_block() {
    let cache = Arc::new(HeadCache::new(32, TEST_MAX_LIMIT as usize));
    let slot = 143u64;

    let mut metadata = base_block_record(slot).metadata;
    metadata.executed_transaction_count = 2;
    metadata.entry_count = 2;
    metadata.block_height = Some(101);
    metadata.block_time = Some(1_700_000_125);
    cache.note_block_metadata(metadata);
    cache.note_slot_commitment(slot, CommitmentLevel::Confirmed);

    let state = test_state_with_head_cache_and_clickhouse_url(cache, "http://127.0.0.1:1");
    let response = handle_get_block(
        state,
        json!(1),
        Some(vec![
            json!(slot),
            json!({
                "transactionDetails": "none",
                "rewards": false,
                "commitment": "confirmed"
            }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    assert!(
        parsed.error.is_none(),
        "expected success: {:?}",
        parsed.error
    );
    let result = parsed.result.expect("result");
    assert!(result.get("transactions").is_none());
    assert!(result.get("signatures").is_none());
    assert!(result.get("rewards").is_none());
    assert_eq!(
        result.get("blockHeight").and_then(|value| value.as_u64()),
        Some(101)
    );
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_block_accounts_served_from_head_cache() {
    let cache = Arc::new(HeadCache::new(32, TEST_MAX_LIMIT as usize));
    let slot = 44u64;
    let address = Pubkey::new_unique();
    let sig_bytes = [8u8; 64];
    let sig = Signature::from(sig_bytes);
    let sig_str = bs58::encode(sig_bytes).into_string();

    let mut record = base_transaction_record();
    record.slot = slot;
    record.signature = sig_bytes;
    record.tx_signatures = vec![sig_bytes];
    record.tx_account_keys = vec![address.to_bytes()];
    record.meta_pre_balances = vec![1];
    record.meta_post_balances = vec![1];

    cache.insert_for_tests(sig, record, 1, &[address], CommitmentLevel::Confirmed);

    let mut metadata = base_block_record(slot).metadata;
    metadata.executed_transaction_count = 1;
    cache.note_block_metadata(metadata);

    let state = test_state_with_head_cache_and_clickhouse_url(cache, "http://127.0.0.1:1");
    let response = handle_get_block(
        state,
        json!(1),
        Some(vec![
            json!(slot),
            json!({
                "transactionDetails": "accounts",
                "commitment": "confirmed",
                "maxSupportedTransactionVersion": 0
            }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    assert!(
        parsed.error.is_none(),
        "expected success: {:?}",
        parsed.error
    );
    let result = parsed.result.expect("result");
    let transactions = result
        .get("transactions")
        .and_then(|value| value.as_array())
        .expect("transactions array");
    assert_eq!(transactions.len(), 1);
    assert_eq!(
        transactions[0]
            .get("transaction")
            .and_then(|value| value.get("signatures"))
            .and_then(|value| value.as_array())
            .and_then(|value| value.first())
            .and_then(|value| value.as_str()),
        Some(sig_str.as_str())
    );
    assert_eq!(
        transactions[0]
            .get("transaction")
            .and_then(|value| value.get("accountKeys"))
            .and_then(|value| value.as_array())
            .and_then(|value| value.first())
            .and_then(|value| value.get("pubkey"))
            .and_then(|value| value.as_str()),
        Some(address.to_string().as_str())
    );
    assert!(
        transactions[0]
            .get("meta")
            .and_then(|value| value.get("logMessages"))
            .is_none()
    );
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_block_uses_complete_head_cache_before_clickhouse() {
    let cache = Arc::new(HeadCache::new(32, TEST_MAX_LIMIT as usize));
    let slot = 99u64;
    let mut block = base_block_record(slot);
    block.metadata.blockhash = [7u8; 32];
    block.metadata.parent_blockhash = [8u8; 32];
    block.metadata.block_time = Some(1_700_000_123);
    block.metadata.block_height = Some(456);
    block.metadata.executed_transaction_count = 1;
    block.metadata.entry_count = 1;
    cache.note_block_metadata(block.metadata.clone());
    cache.note_slot_commitment(slot, CommitmentLevel::Confirmed);

    let address = Pubkey::new_unique();
    let signature = Signature::new_unique();
    let mut record = base_transaction_record();
    record.slot = slot;
    record.block_time = Some(1_700_000_123);
    record.signature = *signature.as_array();
    record.tx_signatures = vec![*signature.as_array()];
    record.tx_account_keys = vec![address.to_bytes()];
    cache.insert_for_tests(signature, record, 3, &[address], CommitmentLevel::Confirmed);

    let state = test_state_with_head_cache_and_clickhouse_url(cache, "http://127.0.0.1:1");
    let response = handle_get_block(
        state,
        json!(1),
        Some(vec![
            json!(slot),
            json!({ "transactionDetails": "signatures", "commitment": "confirmed" }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let result = parsed.result.expect("result");
    let signatures = result
        .get("signatures")
        .and_then(|value| value.as_array())
        .expect("signatures array");
    assert_eq!(signatures, &vec![json!(signature.to_string())]);
    assert!(parsed.error.is_none());
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_block_incomplete_head_cache_falls_back_to_clickhouse() {
    let cache = Arc::new(HeadCache::new(32, TEST_MAX_LIMIT as usize));
    let slot = 100u64;
    let mut block = base_block_record(slot);
    block.metadata.executed_transaction_count = 2;
    block.metadata.entry_count = 2;
    cache.note_block_metadata(block.metadata.clone());
    cache.note_slot_commitment(slot, CommitmentLevel::Finalized);

    let address = Pubkey::new_unique();
    let signature = Signature::new_unique();
    let mut record = base_transaction_record();
    record.slot = slot;
    record.signature = *signature.as_array();
    record.tx_signatures = vec![*signature.as_array()];
    record.tx_account_keys = vec![address.to_bytes()];
    cache.insert_for_tests(signature, record, 0, &[address], CommitmentLevel::Finalized);

    let state = test_state_with_head_cache_and_clickhouse_url(cache, "http://127.0.0.1:1");
    let response = handle_get_block(
        state,
        json!(1),
        Some(vec![json!(slot), json!({ "commitment": "finalized" })]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error");
    assert_eq!(err.code, -32603);
}

#[tokio::test]
async fn get_block_height_rejects_processed_commitment_without_head_cache() {
    let state = test_state();

    let response = handle_get_block_height(
        state,
        json!(1),
        Some(vec![json!({ "commitment": "processed" })]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(
        err.message,
        "Only confirmed or finalized commitments are supported"
    );
}

#[tokio::test]
async fn get_block_height_rejects_multiple_params() {
    let state = test_state();

    let response = handle_get_block_height(
        state,
        json!(1),
        Some(vec![json!({ "commitment": "finalized" }), json!(1)]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(
        err.message,
        "Invalid params: expected a single config object"
    );
}

#[tokio::test]
async fn get_block_height_rejects_non_object_config() {
    let state = test_state();

    let response = handle_get_block_height(state, json!(1), Some(vec![json!("nope")]))
        .await
        .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid params: config must be an object");
}

#[tokio::test]
async fn get_block_height_rejects_min_context_slot_not_reached() {
    let state = test_state();

    let response =
        handle_get_block_height(state, json!(1), Some(vec![json!({ "minContextSlot": 2 })]))
            .await
            .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(
        err.code,
        JSON_RPC_SERVER_ERROR_MIN_CONTEXT_SLOT_NOT_REACHED as i32
    );
    assert_eq!(err.message, "Minimum context slot has not been reached");
    assert_eq!(
        err.data.and_then(|d| d.get("contextSlot").cloned()),
        Some(json!(1u64))
    );
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_block_height_processed_from_head_cache() {
    let cache = Arc::new(HeadCache::new(32, 1024));
    cache.note_block_height(10, 123);
    cache.note_slot_commitment(10, CommitmentLevel::Processed);

    let state = test_state_with_head_cache(cache);

    let response = handle_get_block_height(
        state,
        json!(1),
        Some(vec![json!({ "commitment": "processed" })]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    assert!(parsed.error.is_none());
    assert_eq!(parsed.result, Some(json!(123u64)));
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_block_height_confirmed_head_fast_path_avoids_clickhouse() {
    let cache = Arc::new(HeadCache::new(32, 1024));
    cache.note_block_height(10, 123);
    cache.note_slot_commitment(10, CommitmentLevel::Confirmed);

    let state = test_state_with_head_cache_and_clickhouse_url(cache, "http://127.0.0.1:1");

    let response = handle_get_block_height(
        state,
        json!(1),
        Some(vec![json!({ "commitment": "confirmed" })]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    assert!(parsed.error.is_none());
    assert_eq!(parsed.result, Some(json!(123u64)));
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_block_height_min_context_head_fast_path_avoids_clickhouse() {
    let cache = Arc::new(HeadCache::new(32, 1024));
    cache.note_block_height(10, 123);
    cache.note_slot_commitment(10, CommitmentLevel::Confirmed);

    let state = test_state_with_head_cache_and_clickhouse_url(cache, "http://127.0.0.1:1");

    let response = handle_get_block_height(
        state,
        json!(1),
        Some(vec![json!({
            "commitment": "confirmed",
            "minContextSlot": 10
        })]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    assert!(parsed.error.is_none());
    assert_eq!(parsed.result, Some(json!(123u64)));
}

#[tokio::test]
async fn get_slot_rejects_processed_commitment_without_head_cache() {
    let state = test_state();

    let response = handle_get_slot(
        state,
        json!(1),
        Some(vec![json!({ "commitment": "processed" })]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(
        err.message,
        "Only confirmed or finalized commitments are supported"
    );
}

#[tokio::test]
async fn get_slot_rejects_multiple_params() {
    let state = test_state();

    let response = handle_get_slot(
        state,
        json!(1),
        Some(vec![json!({ "commitment": "finalized" }), json!(1)]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(
        err.message,
        "Invalid params: expected a single config object"
    );
}

#[tokio::test]
async fn get_slot_rejects_non_object_config() {
    let state = test_state();

    let response = handle_get_slot(state, json!(1), Some(vec![json!("nope")]))
        .await
        .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid params: config must be an object");
}

#[tokio::test]
async fn get_slot_rejects_min_context_slot_not_reached() {
    let state = test_state();

    let response = handle_get_slot(state, json!(1), Some(vec![json!({ "minContextSlot": 2 })]))
        .await
        .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(
        err.code,
        JSON_RPC_SERVER_ERROR_MIN_CONTEXT_SLOT_NOT_REACHED as i32
    );
    assert_eq!(err.message, "Minimum context slot has not been reached");
    assert_eq!(
        err.data.and_then(|d| d.get("contextSlot").cloned()),
        Some(json!(1u64))
    );
}

#[tokio::test]
async fn get_transaction_count_rejects_processed_commitment_without_head_cache() {
    let state = test_state();

    let response = handle_get_transaction_count(
        state,
        json!(1),
        Some(vec![json!({ "commitment": "processed" })]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(
        err.message,
        "Only confirmed or finalized commitments are supported"
    );
}

#[tokio::test]
async fn get_transaction_count_rejects_multiple_params() {
    let state = test_state();

    let response = handle_get_transaction_count(
        state,
        json!(1),
        Some(vec![json!({ "commitment": "finalized" }), json!(1)]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(
        err.message,
        "Invalid params: expected a single config object"
    );
}

#[tokio::test]
async fn get_transaction_count_rejects_non_object_config() {
    let state = test_state();

    let response = handle_get_transaction_count(state, json!(1), Some(vec![json!("nope")]))
        .await
        .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid params: config must be an object");
}

#[tokio::test]
async fn get_transaction_count_rejects_min_context_slot_not_reached() {
    let state = test_state();

    let response =
        handle_get_transaction_count(state, json!(1), Some(vec![json!({ "minContextSlot": 2 })]))
            .await
            .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(
        err.code,
        JSON_RPC_SERVER_ERROR_MIN_CONTEXT_SLOT_NOT_REACHED as i32
    );
    assert_eq!(err.message, "Minimum context slot has not been reached");
    assert_eq!(
        err.data.and_then(|d| d.get("contextSlot").cloned()),
        Some(json!(1u64))
    );
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_transaction_count_min_context_uses_head_overlay_context() {
    let cache = Arc::new(HeadCache::new(32, 1024));
    cache.note_block_metadata(head_cache_metadata(100, 95, 3));
    cache.note_block_metadata(head_cache_metadata(105, 100, 5));
    cache.note_slot_commitment(105, CommitmentLevel::Confirmed);

    let state = test_state_with_head_cache_and_clickhouse_url(cache, "http://127.0.0.1:1");
    state.latest_slot_cache.value.store(95, Ordering::Relaxed);
    state
        .latest_slot_cache
        .last_updated_ms
        .store(current_time_millis(), Ordering::Relaxed);

    let response = handle_get_transaction_count(
        state,
        json!(1),
        Some(vec![json!({
            "commitment": "confirmed",
            "minContextSlot": 106
        })]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(
        err.code,
        JSON_RPC_SERVER_ERROR_MIN_CONTEXT_SLOT_NOT_REACHED as i32
    );
    assert_eq!(err.message, "Minimum context slot has not been reached");
    assert_eq!(
        err.data.and_then(|d| d.get("contextSlot").cloned()),
        Some(json!(105u64))
    );
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_slot_processed_from_head_cache() {
    let cache = Arc::new(HeadCache::new(32, 1024));
    cache.note_slot_commitment(10, CommitmentLevel::Processed);

    let state = test_state_with_head_cache(cache);

    let response = handle_get_slot(
        state,
        json!(1),
        Some(vec![json!({ "commitment": "processed" })]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    assert!(parsed.error.is_none());
    assert_eq!(parsed.result, Some(json!(10u64)));
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_slot_finalized_uses_head_cache_when_clickhouse_unreachable() {
    let cache = Arc::new(HeadCache::new(32, 1024));
    cache.note_slot_commitment(10, CommitmentLevel::Finalized);

    let state = test_state_with_head_cache_and_clickhouse_url(cache, "http://127.0.0.1:1");
    state
        .latest_slot_cache
        .last_updated_ms
        .store(0, Ordering::Relaxed);

    let response = handle_get_slot(
        state,
        json!(1),
        Some(vec![json!({ "commitment": "finalized" })]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    assert!(parsed.error.is_none());
    assert_eq!(parsed.result, Some(json!(10u64)));
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_slot_finalized_falls_back_to_clickhouse_when_head_not_finalized() {
    let cache = Arc::new(HeadCache::new(32, 1024));
    cache.note_slot_commitment(10, CommitmentLevel::Processed);

    let state = test_state_with_head_cache_and_clickhouse_url(cache, "http://127.0.0.1:1");
    state
        .latest_slot_cache
        .last_updated_ms
        .store(0, Ordering::Relaxed);

    let response = handle_get_slot(
        state,
        json!(1),
        Some(vec![json!({ "commitment": "finalized" })]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32603);
    assert_eq!(err.message, "Internal error");
    assert!(err.data.is_none());
}

#[tokio::test]
async fn json_rpc_internal_error_never_includes_data() {
    let response = json_rpc_error_response(
        json!(1),
        -32603,
        "do not leak this message",
        Some(json!({ "details": "do not leak this data" })),
    );

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32603);
    assert_eq!(err.message, "Internal error");
    assert!(err.data.is_none());
}

#[tokio::test]
async fn get_latest_blockhash_rejects_processed_commitment_without_head_cache() {
    let state = test_state();

    let response = handle_get_latest_blockhash(
        state,
        json!(1),
        Some(vec![json!({ "commitment": "processed" })]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(
        err.message,
        "Only confirmed or finalized commitments are supported"
    );
}

#[tokio::test]
async fn get_latest_blockhash_rejects_multiple_params() {
    let state = test_state();

    let response = handle_get_latest_blockhash(
        state,
        json!(1),
        Some(vec![json!({ "commitment": "finalized" }), json!(1)]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(
        err.message,
        "Invalid params: expected a single config object"
    );
}

#[tokio::test]
async fn get_latest_blockhash_rejects_non_object_config() {
    let state = test_state();

    let response = handle_get_latest_blockhash(state, json!(1), Some(vec![json!(1)]))
        .await
        .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid params: config must be an object");
}

#[tokio::test]
async fn get_latest_blockhash_rejects_min_context_slot_not_reached() {
    let state = test_state();

    let response =
        handle_get_latest_blockhash(state, json!(1), Some(vec![json!({ "minContextSlot": 2 })]))
            .await
            .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(
        err.code,
        JSON_RPC_SERVER_ERROR_MIN_CONTEXT_SLOT_NOT_REACHED as i32
    );
    assert_eq!(err.message, "Minimum context slot has not been reached");
    assert_eq!(
        err.data.and_then(|d| d.get("contextSlot").cloned()),
        Some(json!(1u64))
    );
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_latest_blockhash_processed_from_head_cache() {
    let cache = Arc::new(HeadCache::new(32, 1024));
    cache.note_slot_commitment(10, CommitmentLevel::Processed);
    cache.note_block_height(10, 123);
    cache.note_blockhash(10, [1u8; 32]);

    let state = test_state_with_head_cache(cache);

    let response = handle_get_latest_blockhash(
        state,
        json!(1),
        Some(vec![json!({ "commitment": "processed" })]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    assert!(parsed.error.is_none());

    let expected_hash = Hash::from([1u8; 32]).to_string();
    let expected_last_valid = 123u64 + solana_clock::MAX_PROCESSING_AGE as u64;
    assert_eq!(
        parsed.result,
        Some(json!({
            "context": { "slot": 10u64 },
            "value": {
                "blockhash": expected_hash,
                "lastValidBlockHeight": expected_last_valid,
            },
        }))
    );
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_latest_blockhash_finalized_uses_head_cache_when_clickhouse_unreachable() {
    let cache = Arc::new(HeadCache::new(32, 1024));
    cache.note_slot_commitment(20, CommitmentLevel::Finalized);
    cache.note_block_height(20, 555);
    cache.note_blockhash(20, [2u8; 32]);

    let state = test_state_with_head_cache_and_clickhouse_url(cache, "http://127.0.0.1:1");
    state
        .latest_slot_cache
        .last_updated_ms
        .store(0, Ordering::Relaxed);

    let response = handle_get_latest_blockhash(
        state,
        json!(1),
        Some(vec![json!({ "commitment": "finalized" })]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    assert!(parsed.error.is_none());

    let expected_hash = Hash::from([2u8; 32]).to_string();
    let expected_last_valid = 555u64 + solana_clock::MAX_PROCESSING_AGE as u64;
    assert_eq!(
        parsed.result,
        Some(json!({
            "context": { "slot": 20u64 },
            "value": {
                "blockhash": expected_hash,
                "lastValidBlockHeight": expected_last_valid,
            },
        }))
    );
}

#[tokio::test]
async fn get_inflation_reward_rejects_missing_addresses() {
    let state = test_state();

    let response = handle_get_inflation_reward(state, json!(1), None)
        .await
        .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid params: missing addresses");
}

#[tokio::test]
async fn get_inflation_reward_rejects_non_array_addresses() {
    let state = test_state();

    let response = handle_get_inflation_reward(state, json!(1), Some(vec![json!("not_an_array")]))
        .await
        .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid params: addresses must be an array");
}

#[tokio::test]
async fn get_inflation_reward_rejects_non_string_address() {
    let state = test_state();

    let response = handle_get_inflation_reward(state, json!(1), Some(vec![json!([123])]))
        .await
        .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(
        err.message,
        "Invalid params: addresses must be an array of strings"
    );
}

#[tokio::test]
async fn get_inflation_reward_rejects_invalid_address() {
    let state = test_state();

    let response = handle_get_inflation_reward(state, json!(1), Some(vec![json!(["bad"])]))
        .await
        .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid param: Invalid");
}

#[tokio::test]
async fn get_inflation_reward_rejects_processed_commitment() {
    let state = test_state();

    let response = handle_get_inflation_reward(
        state,
        json!(1),
        Some(vec![json!([]), json!({ "commitment": "processed" })]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(
        err.message,
        "Only confirmed or finalized commitments are supported"
    );
}

#[tokio::test]
async fn get_inflation_reward_rejects_non_object_config() {
    let state = test_state();

    let response = handle_get_inflation_reward(state, json!(1), Some(vec![json!([]), json!(1)]))
        .await
        .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid params: config must be an object");
}

#[tokio::test]
async fn get_inflation_reward_rejects_min_context_slot_not_reached() {
    let state = test_state();

    let response = handle_get_inflation_reward(
        state,
        json!(1),
        Some(vec![json!([]), json!({ "minContextSlot": 2 })]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(
        err.code,
        JSON_RPC_SERVER_ERROR_MIN_CONTEXT_SLOT_NOT_REACHED as i32
    );
    assert_eq!(err.message, "Minimum context slot has not been reached");
    assert_eq!(
        err.data.and_then(|d| d.get("contextSlot").cloned()),
        Some(json!(1u64))
    );
}

#[tokio::test]
async fn get_inflation_reward_empty_addresses_returns_empty_result() {
    let state = test_state();

    let response = handle_get_inflation_reward(
        state,
        json!(1),
        Some(vec![json!([]), json!({ "epoch": 0u64 })]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    assert!(parsed.error.is_none());
    assert_eq!(parsed.result, Some(json!([])));
}

#[tokio::test]
async fn get_block_time_rejects_missing_slot() {
    let state = test_state();

    let response = handle_get_block_time(state, json!(1), None)
        .await
        .expect("response");

    let (parts, body) = response.into_parts();
    assert_eq!(parts.status, StatusCode::OK);

    let body_bytes = to_bytes(body, usize::MAX).await.expect("body bytes");
    let parsed: JsonRpcResponse = serde_json::from_slice(&body_bytes).expect("json parse");

    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid params: missing slot");
}

#[tokio::test]
async fn get_block_time_rejects_non_numeric_slot() {
    let state = test_state();

    let response = handle_get_block_time(state, json!(1), Some(vec![json!("not_a_slot")]))
        .await
        .expect("response");

    let (parts, body) = response.into_parts();
    assert_eq!(parts.status, StatusCode::OK);

    let body_bytes = to_bytes(body, usize::MAX).await.expect("body bytes");
    let parsed: JsonRpcResponse = serde_json::from_slice(&body_bytes).expect("json parse");

    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid params: slot must be a number");
}

#[tokio::test]
async fn get_block_time_rejects_unexpected_config() {
    let state = test_state();

    let response = handle_get_block_time(
        state,
        json!(1),
        Some(vec![json!(0u64), json!({ "commitment": "processed" })]),
    )
    .await
    .expect("response");

    let (parts, body) = response.into_parts();
    assert_eq!(parts.status, StatusCode::OK);

    let body_bytes = to_bytes(body, usize::MAX).await.expect("body bytes");
    let parsed: JsonRpcResponse = serde_json::from_slice(&body_bytes).expect("json parse");

    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid params: unexpected config");
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_block_time_above_head_tip_returns_block_not_available() {
    let cache = Arc::new(HeadCache::new(32, TEST_MAX_LIMIT as usize));
    cache.note_slot_commitment(100, CommitmentLevel::Processed);

    let state = test_state_with_head_cache_and_clickhouse_url(cache, "http://127.0.0.1:1");
    let response = handle_get_block_time(state, json!(1), Some(vec![json!(101u64)]))
        .await
        .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32004);
    assert_eq!(err.message, "Block not available for slot 101");
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_block_time_serves_from_head_cache_when_slot_time_present() {
    let cache = Arc::new(HeadCache::new(32, TEST_MAX_LIMIT as usize));
    cache.note_slot_commitment(100, CommitmentLevel::Processed);
    cache.note_block_time(99, 1_700_000_000);

    let state = test_state_with_head_cache_and_clickhouse_url(cache, "http://127.0.0.1:1");
    let response = handle_get_block_time(state, json!(1), Some(vec![json!(99u64)]))
        .await
        .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    assert!(
        parsed.error.is_none(),
        "expected success: {:?}",
        parsed.error
    );
    assert_eq!(parsed.result, Some(json!(1_700_000_000)));
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_block_time_head_cache_miss_falls_back_to_clickhouse() {
    let cache = Arc::new(HeadCache::new(32, TEST_MAX_LIMIT as usize));
    cache.note_slot_commitment(100, CommitmentLevel::Processed);

    let state = test_state_with_head_cache_and_clickhouse_url(cache, "http://127.0.0.1:1");
    let response = handle_get_block_time(state, json!(1), Some(vec![json!(99u64)]))
        .await
        .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32603);
    assert_eq!(err.message, "Internal error");
}

#[tokio::test]
async fn get_blocks_rejects_missing_start_slot() {
    let state = test_state();

    let response = handle_get_blocks(state, json!(1), None)
        .await
        .expect("response");

    let (parts, body) = response.into_parts();
    assert_eq!(parts.status, StatusCode::OK);

    let body_bytes = to_bytes(body, usize::MAX).await.expect("body bytes");
    let parsed: JsonRpcResponse = serde_json::from_slice(&body_bytes).expect("json parse");

    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid params: missing start_slot");
}

#[tokio::test]
async fn get_blocks_rejects_non_numeric_start_slot() {
    let state = test_state();

    let response = handle_get_blocks(state, json!(1), Some(vec![json!("not_a_slot")]))
        .await
        .expect("response");

    let (parts, body) = response.into_parts();
    assert_eq!(parts.status, StatusCode::OK);

    let body_bytes = to_bytes(body, usize::MAX).await.expect("body bytes");
    let parsed: JsonRpcResponse = serde_json::from_slice(&body_bytes).expect("json parse");

    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid params: start_slot must be a number");
}

#[tokio::test]
async fn get_blocks_rejects_non_numeric_end_slot() {
    let state = test_state();

    let response = handle_get_blocks(
        state,
        json!(1),
        Some(vec![json!(0u64), json!("not_a_slot")]),
    )
    .await
    .expect("response");

    let (parts, body) = response.into_parts();
    assert_eq!(parts.status, StatusCode::OK);

    let body_bytes = to_bytes(body, usize::MAX).await.expect("body bytes");
    let parsed: JsonRpcResponse = serde_json::from_slice(&body_bytes).expect("json parse");

    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid params: end_slot must be a number");
}

#[tokio::test]
async fn get_blocks_rejects_processed_commitment() {
    let state = test_state();

    let response = handle_get_blocks(
        state,
        json!(1),
        Some(vec![json!(0u64), json!({ "commitment": "processed" })]),
    )
    .await
    .expect("response");

    let (parts, body) = response.into_parts();
    assert_eq!(parts.status, StatusCode::OK);

    let body_bytes = to_bytes(body, usize::MAX).await.expect("body bytes");
    let parsed: JsonRpcResponse = serde_json::from_slice(&body_bytes).expect("json parse");

    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(
        err.message,
        "Only confirmed or finalized commitments are supported"
    );
}

#[tokio::test]
async fn get_blocks_rejects_end_before_start() {
    let state = test_state();

    let response = handle_get_blocks(state, json!(1), Some(vec![json!(10u64), json!(5u64)]))
        .await
        .expect("response");

    let (parts, body) = response.into_parts();
    assert_eq!(parts.status, StatusCode::OK);

    let body_bytes = to_bytes(body, usize::MAX).await.expect("body bytes");
    let parsed: JsonRpcResponse = serde_json::from_slice(&body_bytes).expect("json parse");

    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(
        err.message,
        "Invalid params: end_slot must be greater than or equal to start_slot"
    );
}

#[tokio::test]
async fn get_blocks_rejects_range_too_large() {
    let state = test_state();

    let response = handle_get_blocks(
        state,
        json!(1),
        Some(vec![json!(0u64), json!(MAX_GET_BLOCKS_RANGE + 1)]),
    )
    .await
    .expect("response");

    let (parts, body) = response.into_parts();
    assert_eq!(parts.status, StatusCode::OK);

    let body_bytes = to_bytes(body, usize::MAX).await.expect("body bytes");
    let parsed: JsonRpcResponse = serde_json::from_slice(&body_bytes).expect("json parse");

    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(
        err.message,
        format!(
            "Invalid params: end_slot must be no more than {} blocks higher than start_slot",
            MAX_GET_BLOCKS_RANGE
        )
    );
}

#[tokio::test]
async fn get_blocks_without_end_slot_uses_latest_slot_cache() {
    let state = test_state_with_clickhouse_url("http://127.0.0.1:1");
    state.latest_slot_cache.value.store(5, Ordering::Relaxed);
    state
        .latest_slot_cache
        .last_updated_ms
        .store(current_time_millis(), Ordering::Relaxed);

    let response = handle_get_blocks(state, json!(1), Some(vec![json!(10u64)]))
        .await
        .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    assert!(
        parsed.error.is_none(),
        "expected success: {:?}",
        parsed.error
    );
    assert_eq!(parsed.result, Some(json!([])));
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_blocks_without_end_slot_falls_back_to_head_cache_when_clickhouse_unreachable() {
    let cache = Arc::new(HeadCache::new(32, 1024));
    cache.note_slot_commitment(50, CommitmentLevel::Finalized);

    let state = test_state_with_head_cache_and_clickhouse_url(cache, "http://127.0.0.1:1");
    state
        .latest_slot_cache
        .last_updated_ms
        .store(0, Ordering::Relaxed);

    let response = handle_get_blocks(state, json!(1), Some(vec![json!(51u64)]))
        .await
        .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    assert!(
        parsed.error.is_none(),
        "expected success: {:?}",
        parsed.error
    );
    assert_eq!(parsed.result, Some(json!([])));
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_blocks_processed_served_from_head_cache_when_clickhouse_unreachable() {
    let cache = Arc::new(HeadCache::new(32, 1024));
    cache.note_slot_commitment(10, CommitmentLevel::Processed);
    cache.note_slot_commitment(11, CommitmentLevel::Processed);

    let state = test_state_with_head_cache_and_clickhouse_url(cache, "http://127.0.0.1:1");

    let response = handle_get_blocks(
        state,
        json!(1),
        Some(vec![
            json!(10u64),
            json!(11u64),
            json!({ "commitment": "processed" }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    assert!(
        parsed.error.is_none(),
        "expected success: {:?}",
        parsed.error
    );
    assert_eq!(parsed.result, Some(json!([10u64, 11u64])));
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_blocks_finalized_served_from_head_cache_when_clickhouse_unreachable() {
    let cache = Arc::new(HeadCache::new(32, 1024));
    cache.note_slot_commitment(20, CommitmentLevel::Finalized);
    cache.note_slot_commitment(21, CommitmentLevel::Finalized);

    let state = test_state_with_head_cache_and_clickhouse_url(cache, "http://127.0.0.1:1");

    let response = handle_get_blocks(
        state,
        json!(1),
        Some(vec![
            json!(20u64),
            json!(21u64),
            json!({ "commitment": "finalized" }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    assert!(
        parsed.error.is_none(),
        "expected success: {:?}",
        parsed.error
    );
    assert_eq!(parsed.result, Some(json!([20u64, 21u64])));
}

#[tokio::test]
async fn get_blocks_with_limit_rejects_missing_start_slot() {
    let state = test_state();

    let response = handle_get_blocks_with_limit(state, json!(1), None)
        .await
        .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid params: missing start_slot");
}

#[tokio::test]
async fn get_blocks_with_limit_rejects_non_numeric_start_slot() {
    let state = test_state();

    let response = handle_get_blocks_with_limit(state, json!(1), Some(vec![json!("not_a_slot")]))
        .await
        .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid params: start_slot must be a number");
}

#[tokio::test]
async fn get_blocks_with_limit_rejects_missing_limit() {
    let state = test_state();

    let response = handle_get_blocks_with_limit(state, json!(1), Some(vec![json!(0u64)]))
        .await
        .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid params: missing limit");
}

#[tokio::test]
async fn get_blocks_with_limit_rejects_non_numeric_limit() {
    let state = test_state();

    let response =
        handle_get_blocks_with_limit(state, json!(1), Some(vec![json!(0u64), json!("bad")]))
            .await
            .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid params: limit must be a number");
}

#[tokio::test]
async fn get_blocks_with_limit_rejects_processed_commitment() {
    let state = test_state();

    let response = handle_get_blocks_with_limit(
        state,
        json!(1),
        Some(vec![
            json!(0u64),
            json!(1u64),
            json!({ "commitment": "processed" }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(
        err.message,
        "Only confirmed or finalized commitments are supported"
    );
}

#[tokio::test]
async fn get_blocks_with_limit_rejects_limit_too_large() {
    let state = test_state();

    let response = handle_get_blocks_with_limit(
        state,
        json!(1),
        Some(vec![json!(0u64), json!(MAX_GET_BLOCKS_RANGE + 1)]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(
        err.message,
        format!(
            "Invalid params: limit must be no greater than {}",
            MAX_GET_BLOCKS_RANGE
        )
    );
}

#[tokio::test]
async fn get_blocks_with_limit_rejects_invalid_config() {
    let state = test_state();

    let response = handle_get_blocks_with_limit(
        state,
        json!(1),
        Some(vec![json!(0u64), json!(1u64), json!("bad")]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert!(err.message.contains("failed to parse config"));
}

#[tokio::test]
async fn get_blocks_with_limit_zero_limit_returns_empty() {
    let state = test_state();

    let response = handle_get_blocks_with_limit(state, json!(1), Some(vec![json!(0u64), json!(0)]))
        .await
        .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    assert!(parsed.error.is_none());
    assert_eq!(parsed.result, Some(json!([])));
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_blocks_with_limit_processed_served_from_head_cache_when_clickhouse_unreachable() {
    let cache = Arc::new(HeadCache::new(32, 1024));
    cache.note_slot_commitment(10, CommitmentLevel::Processed);
    cache.note_slot_commitment(11, CommitmentLevel::Processed);

    let state = test_state_with_head_cache_and_clickhouse_url(cache, "http://127.0.0.1:1");

    let response = handle_get_blocks_with_limit(
        state,
        json!(1),
        Some(vec![
            json!(10u64),
            json!(2u64),
            json!({ "commitment": "processed" }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    assert!(
        parsed.error.is_none(),
        "expected success: {:?}",
        parsed.error
    );
    assert_eq!(parsed.result, Some(json!([10u64, 11u64])));
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_blocks_with_limit_finalized_served_from_head_cache_when_clickhouse_unreachable() {
    let cache = Arc::new(HeadCache::new(32, 1024));
    cache.note_slot_commitment(30, CommitmentLevel::Finalized);
    cache.note_slot_commitment(31, CommitmentLevel::Finalized);

    let state = test_state_with_head_cache_and_clickhouse_url(cache, "http://127.0.0.1:1");

    let response = handle_get_blocks_with_limit(
        state,
        json!(1),
        Some(vec![
            json!(30u64),
            json!(2u64),
            json!({ "commitment": "finalized" }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    assert!(
        parsed.error.is_none(),
        "expected success: {:?}",
        parsed.error
    );
    assert_eq!(parsed.result, Some(json!([30u64, 31u64])));
}

#[tokio::test]
async fn get_first_available_block_rejects_params() {
    let state = test_state();

    let response = handle_get_first_available_block(state, json!(1), Some(vec![json!(1u64)]))
        .await
        .expect("response");

    let (parts, body) = response.into_parts();
    assert_eq!(parts.status, StatusCode::OK);

    let body_bytes = to_bytes(body, usize::MAX).await.expect("body bytes");
    let parsed: JsonRpcResponse = serde_json::from_slice(&body_bytes).expect("json parse");

    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid params: expected no parameters");
}

#[tokio::test]
async fn minimum_ledger_slot_rejects_params() {
    let state = test_state();

    let response = handle_minimum_ledger_slot(state, json!(1), Some(vec![json!(1u64)]))
        .await
        .expect("response");

    let (parts, body) = response.into_parts();
    assert_eq!(parts.status, StatusCode::OK);

    let body_bytes = to_bytes(body, usize::MAX).await.expect("body bytes");
    let parsed: JsonRpcResponse = serde_json::from_slice(&body_bytes).expect("json parse");

    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid params: expected no parameters");
}

#[tokio::test]
async fn minimum_ledger_slot_unreachable_backend_returns_internal_error() {
    let state = test_state_with_clickhouse_url("http://127.0.0.1:1");

    let response = handle_minimum_ledger_slot(state, json!(1), None)
        .await
        .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32603);
    assert_eq!(err.message, "Internal error");
    assert!(parsed.result.is_none());
}

#[test]
fn parse_transaction_error_display_instruction_custom() {
    let err = "Error processing Instruction 0: custom program error: 0x0";
    let parsed = parse_transaction_error_display(err).expect("parsed error");
    assert_eq!(
        parsed,
        TransactionError::InstructionError(0, InstructionError::Custom(0))
    );
}

#[test]
fn parse_transaction_error_display_fixed_variant() {
    let err = "Blockhash not found";
    let parsed = parse_transaction_error_display(err).expect("parsed error");
    assert_eq!(parsed, TransactionError::BlockhashNotFound);
}

#[test]
fn parse_instruction_error_display_borsh() {
    let err = "Failed to serialize or deserialize account data: boom";
    let parsed = parse_instruction_error_display(err).expect("parsed error");
    assert_eq!(parsed, InstructionError::BorshIoError);
}

#[test]
fn build_v0_transaction_allows_missing_lookup_flag_when_empty() {
    let record = StoredTransactionRecord {
        signature: [0u8; 64],
        slot: 0,
        slot_idx: 0,
        block_time: None,
        tx_version: Some(0),
        tx_signatures: vec![[0u8; 64]],
        tx_num_required_signatures: 1,
        tx_num_readonly_signed_accounts: 0,
        tx_num_readonly_unsigned_accounts: 0,
        tx_account_keys: vec![[0u8; 32]],
        tx_recent_blockhash: [0u8; 32],
        tx_instructions_program_id_index: Vec::new(),
        tx_instructions_accounts: Vec::new(),
        tx_instructions_data: Vec::new(),
        tx_address_table_lookups_present: false,
        tx_address_table_lookup_account_key: Vec::new(),
        tx_address_table_lookup_writable_indexes: Vec::new(),
        tx_address_table_lookup_readonly_indexes: Vec::new(),
        meta_status_ok: true,
        meta_err: None,
        meta_fee: 0,
        meta_pre_balances: Vec::new(),
        meta_post_balances: Vec::new(),
        meta_inner_instructions_present: false,
        meta_inner_instructions_index: Vec::new(),
        meta_inner_instructions_program_id_index: Vec::new(),
        meta_inner_instructions_accounts: Vec::new(),
        meta_inner_instructions_data: Vec::new(),
        meta_inner_instructions_stack_height: Vec::new(),
        meta_log_messages_present: false,
        meta_log_messages: Vec::new(),
        meta_pre_token_balances_present: false,
        meta_pre_token_account_index: Vec::new(),
        meta_pre_token_mint: Vec::new(),
        meta_pre_token_owner: Vec::new(),
        meta_pre_token_program_id: Vec::new(),
        meta_pre_token_amount: Vec::new(),
        meta_pre_token_decimals: Vec::new(),
        meta_pre_token_ui_amount: Vec::new(),
        meta_pre_token_ui_amount_string: Vec::new(),
        meta_post_token_balances_present: false,
        meta_post_token_account_index: Vec::new(),
        meta_post_token_mint: Vec::new(),
        meta_post_token_owner: Vec::new(),
        meta_post_token_program_id: Vec::new(),
        meta_post_token_amount: Vec::new(),
        meta_post_token_decimals: Vec::new(),
        meta_post_token_ui_amount: Vec::new(),
        meta_post_token_ui_amount_string: Vec::new(),
        meta_rewards_present: false,
        meta_reward_pubkey: Vec::new(),
        meta_reward_lamports: Vec::new(),
        meta_reward_post_balance: Vec::new(),
        meta_reward_type: Vec::new(),
        meta_reward_commission: Vec::new(),
        meta_loaded_addresses_writable: Vec::new(),
        meta_loaded_addresses_readonly: Vec::new(),
        meta_return_data_present: false,
        meta_return_data_program_id: None,
        meta_return_data_data: None,
        meta_compute_units_consumed: None,
        meta_cost_units: None,
    };

    let tx = build_versioned_transaction(&record).expect("build succeeds");
    match tx.message {
        VersionedMessage::V0(message) => {
            assert!(message.address_table_lookups.is_empty());
        }
        other => panic!("unexpected message variant: {:?}", other),
    }
}

#[tokio::test]
async fn handle_json_rpc_invalid_version_returns_json_rpc_error() {
    let state = test_state();

    let request = JsonRpcRequest {
        jsonrpc: "1.0".to_string(),
        id: json!(1),
        method: "getSignaturesForAddress".to_string(),
        params: None,
    };

    let response = handle_json_rpc_request(state, &request).await;

    assert_eq!(response.status(), StatusCode::OK);
    let parsed = parse_json_rpc_response(response).await;

    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32600);
    assert_eq!(err.message, "Invalid JSON-RPC version");
    assert!(parsed.result.is_none());
}

#[tokio::test]
async fn handle_json_rpc_method_not_found_returns_json_rpc_error() {
    let state = test_state();

    let request = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        id: json!(1),
        method: "unknownMethod".to_string(),
        params: None,
    };

    let response = handle_json_rpc_request(state, &request).await;

    assert_eq!(response.status(), StatusCode::OK);
    let parsed = parse_json_rpc_response(response).await;

    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32601);
    assert_eq!(err.message, "Method not found: unknownMethod");
    assert!(parsed.result.is_none());
}

#[tokio::test]
async fn handle_json_rpc_minimum_ledger_slot_routes_to_handler() {
    let state = test_state_with_clickhouse_url("http://127.0.0.1:1");

    let request = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        id: json!(1),
        method: "minimumLedgerSlot".to_string(),
        params: Some(vec![json!(1u64)]),
    };

    let response = handle_json_rpc_request(state, &request).await;

    assert_eq!(response.status(), StatusCode::OK);
    let parsed = parse_json_rpc_response(response).await;

    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid params: expected no parameters");
    assert!(parsed.result.is_none());
}

#[tokio::test]
async fn handle_json_rpc_get_health_routes_to_handler() {
    let state = test_state_with_clickhouse_latest_slot(Some(42));

    let request = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        id: json!(1),
        method: "getHealth".to_string(),
        params: None,
    };

    let response = handle_json_rpc_request(state, &request).await;

    assert_eq!(response.status(), StatusCode::OK);
    let parsed = parse_json_rpc_response(response).await;

    assert_eq!(parsed.result, Some(json!("ok")));
    assert!(parsed.error.is_none());
}

#[tokio::test]
async fn get_health_accepts_empty_params() {
    let state = test_state_with_clickhouse_latest_slot(Some(42));

    let response = handle_get_health(state, json!(1), Some(Vec::new()))
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    let parsed = parse_json_rpc_response(response).await;

    assert_eq!(parsed.result, Some(json!("ok")));
    assert!(parsed.error.is_none());
}

#[tokio::test]
async fn get_health_rejects_unexpected_params() {
    let state = test_state_with_clickhouse_url("http://127.0.0.1:1");

    let response = handle_get_health(state, json!(1), Some(vec![json!(1u64)]))
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    let parsed = parse_json_rpc_response(response).await;

    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid params: expected no parameters");
    assert!(parsed.result.is_none());
}

#[tokio::test]
async fn get_health_returns_unhealthy_when_clickhouse_has_no_latest_slot() {
    let state = test_state_with_clickhouse_latest_slot(None);

    let response = handle_get_health(state, json!(1), None)
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    let parsed = parse_json_rpc_response(response).await;

    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32005);
    assert_eq!(err.message, "Node is unhealthy");
    assert_eq!(err.data, Some(json!({ "numSlotsBehind": null })));
    assert!(parsed.result.is_none());
}

#[tokio::test]
async fn get_health_returns_unhealthy_when_clickhouse_query_fails() {
    let state = test_state_with_clickhouse_url("http://127.0.0.1:1");

    let response = handle_get_health(state, json!(1), None)
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    let parsed = parse_json_rpc_response(response).await;

    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32005);
    assert_eq!(err.message, "Node is unhealthy");
    assert_eq!(err.data, Some(json!({ "numSlotsBehind": null })));
    assert!(parsed.result.is_none());
}

#[tokio::test]
async fn handle_json_rpc_batch_preserves_input_order() {
    let state = test_state();

    let request = json!([
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSlot",
            "params": []
        },
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "getBlockHeight",
            "params": []
        }
    ]);

    let response = handle_json_rpc_value(state, &request).await;
    assert_eq!(response.status(), StatusCode::OK);

    let parsed = parse_json_value_response(response).await;
    let results = parsed.as_array().expect("batch response array");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id"), Some(&json!(1)));
    assert_eq!(results[1].get("id"), Some(&json!(2)));
}

#[tokio::test]
async fn handle_json_rpc_batch_includes_missing_id_item_with_null_id() {
    let state = test_state();

    let request = json!([
        {
            "jsonrpc": "2.0",
            "method": "getSlot",
            "params": []
        },
        {
            "jsonrpc": "2.0",
            "id": 9,
            "method": "getSlot",
            "params": []
        }
    ]);

    let response = handle_json_rpc_value(state, &request).await;
    assert_eq!(response.status(), StatusCode::OK);

    let parsed = parse_json_value_response(response).await;
    let results = parsed.as_array().expect("batch response array");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id"), Some(&Value::Null));
    assert_eq!(results[1].get("id"), Some(&json!(9)));
}

#[tokio::test]
async fn handle_json_rpc_notification_only_batch_returns_json_with_null_id() {
    let state = test_state();

    let request = json!([{
        "jsonrpc": "2.0",
        "method": "getSlot",
        "params": []
    }]);

    let response = handle_json_rpc_value(state, &request).await;
    assert_eq!(response.status(), StatusCode::OK);
    let parsed = parse_json_value_response(response).await;
    let results = parsed.as_array().expect("batch response array");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id"), Some(&Value::Null));
    assert!(results[0].get("result").is_some());
}

#[tokio::test]
async fn handle_json_rpc_single_missing_id_returns_json_with_null_id() {
    let state = test_state();

    let request = json!({
        "jsonrpc": "2.0",
        "method": "getSlot",
        "params": []
    });

    let response = handle_json_rpc_value(state, &request).await;
    assert_eq!(response.status(), StatusCode::OK);
    let parsed = parse_json_value_response(response).await;
    assert_eq!(parsed.get("id"), Some(&Value::Null));
    assert!(parsed.get("result").is_some());
}

#[tokio::test]
async fn handle_json_rpc_rejects_empty_batch() {
    let state = test_state();

    let request = json!([]);
    let response = handle_json_rpc_value(state, &request).await;

    assert_eq!(response.status(), StatusCode::OK);
    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32600);
    assert_eq!(err.message, "Invalid Request");
    assert_eq!(parsed.id, Value::Null);
}

#[tokio::test]
async fn handle_json_rpc_rejects_oversized_batch() {
    let state = test_state();

    let mut requests = Vec::new();
    for idx in 0..65 {
        requests.push(json!({
            "jsonrpc": "2.0",
            "id": idx,
            "method": "getSlot",
            "params": []
        }));
    }

    let response = handle_json_rpc_value(state, &Value::Array(requests)).await;
    assert_eq!(response.status(), StatusCode::OK);

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32600);
    assert!(err.message.contains("batch size"));
}

#[tokio::test]
async fn handle_json_rpc_invalid_json_returns_parse_error() {
    let state = test_state();

    let response = handle_json_rpc_body_with_headers(
        state,
        HeaderMap::new(),
        Bytes::from_static(br#"{"jsonrpc":"2.0""#),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32700);
    assert_eq!(err.message, "Parse error");
    assert_eq!(parsed.id, Value::Null);
}

#[tokio::test]
async fn handle_json_rpc_emits_header_labels_in_metrics() {
    let state = test_state_with_metrics_header_capture(MetricsHeaderCaptureConfig {
        capture_x_endpoint: true,
        capture_x_rpc_node: true,
        capture_x_subscription_id: true,
        capture_x_account_id: true,
    });
    let request = json!({
        "jsonrpc": "2.0",
        "id": 123,
        "method": "getSlot",
        "params": []
    });
    let mut headers = HeaderMap::new();
    headers.insert(
        "X-Endpoint",
        HeaderValue::from_static("metrics-endpoint-e2e"),
    );
    headers.insert("X-RPC-Node", HeaderValue::from_static("metrics-node-e2e"));
    headers.insert(
        "X-Subscription-ID",
        HeaderValue::from_static("metrics-subscription-e2e"),
    );
    headers.insert(
        "X-Account-ID",
        HeaderValue::from_static("metrics-account-e2e"),
    );

    let response = handle_json_rpc_value_with_headers(state, headers, &request).await;
    assert_eq!(response.status(), StatusCode::OK);

    let text = String::from_utf8(metrics::export_metrics().expect("metrics export"))
        .expect("metrics should be valid UTF-8");
    assert!(
        text.contains("x_endpoint=\"metrics-endpoint-e2e\""),
        "metrics output missing x_endpoint label: {text}"
    );
    assert!(
        text.contains("x_rpc_node=\"metrics-node-e2e\""),
        "metrics output missing x_rpc_node label: {text}"
    );
    assert!(
        text.contains("x_subscription_id=\"metrics-subscription-e2e\""),
        "metrics output missing x_subscription_id label: {text}"
    );
    assert!(
        text.contains("x_account_id=\"metrics-account-e2e\""),
        "metrics output missing x_account_id label: {text}"
    );
    let rpc_requests_with_headers = text.lines().any(|line| {
        line.starts_with("superbank_rpc_requests_total{")
            && line.contains("x_subscription_id=\"metrics-subscription-e2e\"")
            && line.contains("x_account_id=\"metrics-account-e2e\"")
    });
    assert!(
        rpc_requests_with_headers,
        "metrics output missing request-counter labels for subscription/account: {text}"
    );
    let route_total_with_headers = text.lines().any(|line| {
        line.starts_with("superbank_rpc_route_total_total{")
            && line.contains("x_subscription_id=\"metrics-subscription-e2e\"")
            && line.contains("x_account_id=\"metrics-account-e2e\"")
    });
    assert!(
        route_total_with_headers,
        "metrics output missing route labels for subscription/account: {text}"
    );
}

#[tokio::test]
async fn handle_json_rpc_omits_disabled_header_labels_when_capture_off() {
    let state = test_state();
    let request = json!({
        "jsonrpc": "2.0",
        "id": 124,
        "method": "getSlot",
        "params": []
    });
    let mut headers = HeaderMap::new();
    headers.insert(
        "X-Subscription-ID",
        HeaderValue::from_static("ignored-subscription"),
    );
    headers.insert("X-Account-ID", HeaderValue::from_static("ignored-account"));

    let response = handle_json_rpc_value_with_headers(state, headers, &request).await;
    assert_eq!(response.status(), StatusCode::OK);

    let text = String::from_utf8(metrics::export_metrics().expect("metrics export"))
        .expect("metrics should be valid UTF-8");
    let rpc_requests_without_header_labels = text.lines().any(|line| {
        line.starts_with("superbank_rpc_requests_total{")
            && line.contains("method=\"getSlot\"")
            && line.contains("status=\"200\"")
            && !line.contains("x_endpoint=")
            && !line.contains("x_rpc_node=")
            && !line.contains("x_subscription_id=")
            && !line.contains("x_account_id=")
    });
    assert!(
        rpc_requests_without_header_labels,
        "metrics output should omit disabled request header labels: {text}"
    );
    assert!(
        !text.contains("=\"disabled\""),
        "metrics output unexpectedly contains disabled label values: {text}"
    );
}

#[tokio::test]
async fn handle_json_rpc_batch_metrics_include_subscription_account_labels() {
    let state = test_state_with_metrics_header_capture(MetricsHeaderCaptureConfig {
        capture_x_endpoint: true,
        capture_x_rpc_node: true,
        capture_x_subscription_id: true,
        capture_x_account_id: true,
    });
    let request = json!([
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSlot",
            "params": []
        },
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "getBlockHeight",
            "params": []
        }
    ]);
    let mut headers = HeaderMap::new();
    headers.insert(
        "X-Subscription-ID",
        HeaderValue::from_static("batch-subscription-e2e"),
    );
    headers.insert(
        "X-Account-ID",
        HeaderValue::from_static("batch-account-e2e"),
    );
    headers.insert("X-Endpoint", HeaderValue::from_static("batch-endpoint-e2e"));
    headers.insert("X-RPC-Node", HeaderValue::from_static("batch-node-e2e"));

    let response = handle_json_rpc_value_with_headers(state, headers, &request).await;
    assert_eq!(response.status(), StatusCode::OK);

    let text = String::from_utf8(metrics::export_metrics().expect("metrics export"))
        .expect("metrics should be valid UTF-8");
    let batch_requests_with_headers = text.lines().any(|line| {
        line.starts_with("superbank_rpc_batch_requests_total{")
            && line.contains("x_subscription_id=\"batch-subscription-e2e\"")
            && line.contains("x_account_id=\"batch-account-e2e\"")
    });
    assert!(
        batch_requests_with_headers,
        "metrics output missing batch request labels for subscription/account: {text}"
    );
    let batch_size_with_headers = text.lines().any(|line| {
        line.starts_with("superbank_rpc_batch_size_bucket{")
            && line.contains("x_subscription_id=\"batch-subscription-e2e\"")
            && line.contains("x_account_id=\"batch-account-e2e\"")
    });
    assert!(
        batch_size_with_headers,
        "metrics output missing batch size labels for subscription/account: {text}"
    );
}

#[tokio::test]
async fn clickhouse_metrics_include_subscription_account_labels() {
    metrics::with_request_metric_labels(
        metrics::RequestHeaderMetricLabels {
            x_endpoint: Some("clickhouse-endpoint-e2e".to_string()),
            x_rpc_node: Some("clickhouse-node-e2e".to_string()),
            x_subscription_id: Some("clickhouse-subscription-e2e".to_string()),
            x_account_id: Some("clickhouse-account-e2e".to_string()),
        },
        async {
            metrics::clickhouse_timings("getSlot", 5, 10, 20);
        },
    )
    .await;

    let text = String::from_utf8(metrics::export_metrics().expect("metrics export"))
        .expect("metrics should be valid UTF-8");
    let clickhouse_duration_with_headers = text.lines().any(|line| {
        line.starts_with("superbank_rpc_clickhouse_duration_seconds_bucket{")
            && line.contains("x_subscription_id=\"clickhouse-subscription-e2e\"")
            && line.contains("x_account_id=\"clickhouse-account-e2e\"")
    });
    assert!(
        clickhouse_duration_with_headers,
        "metrics output missing clickhouse duration labels for subscription/account: {text}"
    );
}

#[cfg(feature = "grpc-head-cache")]
#[test]
fn head_cache_active_metric_tracks_x_rpc_node_labels() {
    metrics::force_init().expect("metrics init");

    metrics::head_cache_set_active(false);
    let disabled_text = String::from_utf8(metrics::export_metrics().expect("metrics export"))
        .expect("metrics should be valid UTF-8");
    assert!(
        disabled_text.contains("head_cache_active{x_rpc_node=\"none\"} 0"),
        "metrics output missing disabled head_cache_active label: {disabled_text}"
    );

    metrics::head_cache_set_active(true);
    let unknown_text = String::from_utf8(metrics::export_metrics().expect("metrics export"))
        .expect("metrics should be valid UTF-8");
    assert!(
        unknown_text.contains("head_cache_active{x_rpc_node=\"unknown\"} 1"),
        "metrics output missing unknown active head_cache_active label: {unknown_text}"
    );

    metrics::head_cache_set_active_node("lb-nyc21");
    metrics::head_cache_set_active_node("lb-sfo12");
    let switched_text = String::from_utf8(metrics::export_metrics().expect("metrics export"))
        .expect("metrics should be valid UTF-8");
    assert!(
        switched_text.contains("head_cache_active{x_rpc_node=\"lb-nyc21\"} 0"),
        "metrics output missing prior node deactivation label: {switched_text}"
    );
    assert!(
        switched_text.contains("head_cache_active{x_rpc_node=\"lb-sfo12\"} 1"),
        "metrics output missing active node label: {switched_text}"
    );

    metrics::head_cache_set_active_node("");
    let empty_node_text = String::from_utf8(metrics::export_metrics().expect("metrics export"))
        .expect("metrics should be valid UTF-8");
    assert!(
        empty_node_text.contains("head_cache_active{x_rpc_node=\"unknown\"} 1"),
        "metrics output missing empty-node unknown fallback label: {empty_node_text}"
    );

    metrics::head_cache_set_active(false);
    let final_disabled_text = String::from_utf8(metrics::export_metrics().expect("metrics export"))
        .expect("metrics should be valid UTF-8");
    assert!(
        final_disabled_text.contains("head_cache_active{x_rpc_node=\"unknown\"} 0"),
        "metrics output missing unknown deactivation label: {final_disabled_text}"
    );
    assert!(
        final_disabled_text.contains("head_cache_active{x_rpc_node=\"none\"} 0"),
        "metrics output missing final disabled label: {final_disabled_text}"
    );
}

#[test]
#[ignore = "run in isolation: tracing callsite cache can interfere under parallel test execution"]
fn handle_json_rpc_emits_header_labels_in_logs() {
    let state = test_state_with_metrics_header_capture(MetricsHeaderCaptureConfig {
        capture_x_endpoint: true,
        capture_x_rpc_node: true,
        capture_x_subscription_id: true,
        capture_x_account_id: true,
    });
    let request = json!({
        "jsonrpc": "2.0",
        "id": 456,
        "method": "unknownMethod",
        "params": []
    });
    let mut headers = HeaderMap::new();
    headers.insert("X-Endpoint", HeaderValue::from_static("logs-endpoint-e2e"));
    headers.insert("X-RPC-Node", HeaderValue::from_static("logs-node-e2e"));
    headers.insert(
        "X-Subscription-ID",
        HeaderValue::from_static("logs-subscription-e2e"),
    );
    headers.insert("X-Account-ID", HeaderValue::from_static("logs-account-e2e"));

    let log_buffer = SharedLogBuffer::default();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_max_level(tracing::Level::DEBUG)
        .with_ansi(false)
        .without_time()
        .with_writer(log_buffer.clone())
        .finish();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build current-thread runtime");

    tracing::dispatcher::with_default(&tracing::Dispatch::new(subscriber), || {
        tracing::callsite::rebuild_interest_cache();

        let response =
            runtime.block_on(handle_json_rpc_value_with_headers(state, headers, &request));
        assert_eq!(response.status(), StatusCode::OK);
    });

    let logs = log_buffer.snapshot();
    assert!(
        logs.contains("\"x_endpoint\":\"logs-endpoint-e2e\""),
        "log output missing x_endpoint field: {logs}"
    );
    assert!(
        logs.contains("\"x_rpc_node\":\"logs-node-e2e\""),
        "log output missing x_rpc_node field: {logs}"
    );
    assert!(
        logs.contains("\"x_subscription_id\":\"logs-subscription-e2e\""),
        "log output missing x_subscription_id field: {logs}"
    );
    assert!(
        logs.contains("\"x_account_id\":\"logs-account-e2e\""),
        "log output missing x_account_id field: {logs}"
    );
}

#[tokio::test]
async fn get_signature_statuses_rejects_missing_signatures() {
    let state = test_state();

    let response = handle_get_signature_statuses(state, json!(1), None)
        .await
        .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid params: missing signatures");
}

#[tokio::test]
async fn get_signature_statuses_rejects_non_array() {
    let state = test_state();

    let response = handle_get_signature_statuses(state, json!(1), Some(vec![json!("oops")]))
        .await
        .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid params: signatures must be an array");
}

#[tokio::test]
async fn get_signature_statuses_rejects_empty_array() {
    let state = test_state();

    let response = handle_get_signature_statuses(state, json!(1), Some(vec![json!([])]))
        .await
        .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(
        err.message,
        "Invalid params: signatures list must be non-empty"
    );
}

#[tokio::test]
async fn get_signature_statuses_rejects_invalid_config() {
    let state = test_state();

    let response = handle_get_signature_statuses(
        state,
        json!(1),
        Some(vec![
            json!(["not_base58"]),
            json!({ "searchTransactionHistory": "nope" }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid params: failed to parse config");
}

#[tokio::test]
async fn get_signature_statuses_ignores_unknown_config_fields() {
    for commitment in [
        json!("processed"),
        json!("confirmed"),
        json!("finalized"),
        json!("unexpected"),
        json!(123),
        json!({"nested": true}),
    ] {
        let state = test_state();

        let response = handle_get_signature_statuses(
            state,
            json!(1),
            Some(vec![
                json!(["not_base58"]),
                json!({
                    "searchTransactionHistory": false,
                    "commitment": commitment
                }),
            ]),
        )
        .await
        .expect("response");

        let parsed = parse_json_rpc_response(response).await;
        assert!(parsed.error.is_none());

        let result = parsed.result.expect("result present");
        let values = result
            .get("value")
            .and_then(|value| value.as_array())
            .expect("value array");
        assert_eq!(values.len(), 1);
        assert!(values[0].is_null());
    }
}

#[tokio::test]
async fn get_signatures_for_address_rejects_invalid_options() {
    let state = test_state();

    let response = handle_get_signatures_for_address(
        state,
        json!(1),
        Some(vec![
            json!("11111111111111111111111111111111"),
            json!({ "limit": "nope" }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert!(err.message.contains("failed to parse options"));
}

#[tokio::test]
async fn get_signatures_for_address_rejects_min_context_slot_not_reached() {
    let state = test_state();

    let response = handle_get_signatures_for_address(
        state,
        json!(1),
        Some(vec![
            json!("11111111111111111111111111111111"),
            json!({ "minContextSlot": 2 }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(
        err.code,
        JSON_RPC_SERVER_ERROR_MIN_CONTEXT_SLOT_NOT_REACHED as i32
    );
    assert_eq!(err.message, "Minimum context slot has not been reached");
    let context_slot = err
        .data
        .and_then(|data| data.get("contextSlot").cloned())
        .and_then(|slot| slot.as_u64())
        .expect("context slot in error data");
    assert_eq!(context_slot, 1);
}

#[tokio::test]
async fn get_transactions_for_address_rejects_invalid_transaction_details() {
    let state = test_state();

    let response = handle_get_transactions_for_address(
        state,
        json!(1),
        Some(vec![
            json!("11111111111111111111111111111111"),
            json!({ "transactionDetails": "nope" }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert!(err.message.contains("unsupported transactionDetails"));
}

#[tokio::test]
async fn get_transactions_for_address_rejects_invalid_sort_order() {
    let state = test_state();

    let response = handle_get_transactions_for_address(
        state,
        json!(1),
        Some(vec![
            json!("11111111111111111111111111111111"),
            json!({ "sortOrder": "sideways" }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert!(err.message.contains("unsupported sortOrder"));
}

#[tokio::test]
async fn get_transactions_for_address_rejects_invalid_commitment() {
    let state = test_state();

    let response = handle_get_transactions_for_address(
        state,
        json!(1),
        Some(vec![
            json!("11111111111111111111111111111111"),
            json!({ "commitment": "nope" }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert!(err.message.contains("unsupported commitment"));
}

#[tokio::test]
async fn get_transactions_for_address_rejects_empty_pagination_token() {
    let state = test_state();

    let response = handle_get_transactions_for_address(
        state,
        json!(1),
        Some(vec![
            json!("11111111111111111111111111111111"),
            json!({ "paginationToken": "   " }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid params: paginationToken is empty");
}

#[tokio::test]
async fn get_transactions_for_address_rejects_invalid_pagination_slot() {
    let state = test_state();

    let response = handle_get_transactions_for_address(
        state,
        json!(1),
        Some(vec![
            json!("11111111111111111111111111111111"),
            json!({ "paginationToken": "abc:1" }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(
        err.message,
        "Invalid params: paginationToken slot is not a number"
    );
}

#[tokio::test]
async fn get_transactions_for_address_rejects_invalid_pagination_position() {
    let state = test_state();

    let response = handle_get_transactions_for_address(
        state,
        json!(1),
        Some(vec![
            json!("11111111111111111111111111111111"),
            json!({ "paginationToken": "1:abc" }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(
        err.message,
        "Invalid params: paginationToken position is not a number"
    );
}

#[tokio::test]
async fn get_transactions_for_address_rejects_invalid_pagination_signature() {
    let state = test_state();

    let response = handle_get_transactions_for_address(
        state,
        json!(1),
        Some(vec![
            json!("11111111111111111111111111111111"),
            json!({ "paginationToken": "not_base58" }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(
        err.message,
        "Invalid params: paginationToken is not a valid signature"
    );
}

#[tokio::test]
async fn get_transactions_for_address_rejects_invalid_signature_filter() {
    let state = test_state();

    let response = handle_get_transactions_for_address(
        state,
        json!(1),
        Some(vec![
            json!("11111111111111111111111111111111"),
            json!({ "filters": { "signature": { "gt": "not_base58" } } }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(
        err.message,
        "Invalid params: filters.signature must be valid base58 signatures"
    );
}

#[tokio::test]
async fn get_transactions_for_address_rejects_slot_eq_filter() {
    let state = test_state();

    let response = handle_get_transactions_for_address(
        state,
        json!(1),
        Some(vec![
            json!("11111111111111111111111111111111"),
            json!({ "filters": { "slot": { "eq": 1 } } }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(
        err.message,
        "Invalid params: filters.slot.eq is not supported"
    );
}

#[tokio::test]
async fn get_transactions_for_address_rejects_invalid_token_accounts() {
    let state = test_state();

    let response = handle_get_transactions_for_address(
        state,
        json!(1),
        Some(vec![
            json!("11111111111111111111111111111111"),
            json!({ "filters": { "tokenAccounts": "maybe" } }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert!(err.message.contains("unsupported tokenAccounts"));
}

#[tokio::test]
async fn get_block_rejects_invalid_config() {
    let state = test_state();

    let response = handle_get_block(state, json!(1), Some(vec![json!(0u64), json!("bad")]))
        .await
        .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert!(err.message.contains("failed to parse config"));
}

#[tokio::test]
async fn get_block_rejects_unknown_config_fields() {
    let state = test_state();

    let response = handle_get_block(
        state,
        json!(1),
        Some(vec![json!(0u64), json!({ "unknownField": true })]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert!(err.message.contains("unknown field"));
    assert!(err.message.contains("unknownField"));
}

#[tokio::test]
async fn get_block_time_rejects_unexpected_config_value() {
    let state = test_state();

    let response = handle_get_block_time(state, json!(1), Some(vec![json!(0u64), json!("bad")]))
        .await
        .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid params: unexpected config");
}

#[tokio::test]
async fn get_blocks_rejects_invalid_config() {
    let state = test_state();

    let response = handle_get_blocks(
        state,
        json!(1),
        Some(vec![json!(0u64), json!(null), json!("bad")]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert!(err.message.contains("failed to parse config"));
}

#[tokio::test]
async fn get_transaction_rejects_non_string_signature() {
    let state = test_state();

    let response = crate::handlers::transactions::handle_get_transaction(
        state,
        json!(1),
        Some(vec![json!(12345)]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid params: signature must be a string");
}

#[tokio::test]
async fn get_transaction_rejects_invalid_signature() {
    let state = test_state();

    let response = crate::handlers::transactions::handle_get_transaction(
        state,
        json!(1),
        Some(vec![json!("not_base58")]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(err.message, "Invalid params: signature is not valid base58");
}

#[tokio::test]
async fn get_transaction_rejects_denylisted_signature_fail_fast() {
    let state = test_state_with_clickhouse_url("http://127.0.0.1:1");

    let response = crate::handlers::transactions::handle_get_transaction(
        state,
        json!(1),
        Some(vec![json!(
            "1111111111111111111111111111111111111111111111111111111111111111"
        )]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(
        err.message,
        "Invalid params: signature is not a valid transaction signature"
    );
}

#[tokio::test]
async fn get_transaction_rejects_unknown_config_fields() {
    let state = test_state();
    let signature = bs58::encode([1u8; 64]).into_string();

    let response = crate::handlers::transactions::handle_get_transaction(
        state,
        json!(1),
        Some(vec![json!(signature), json!({ "unknownField": true })]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert!(err.message.contains("unknown field"));
    assert!(err.message.contains("unknownField"));
}

#[tokio::test]
async fn get_transaction_rejects_processed_commitment() {
    let state = test_state();
    let signature = bs58::encode([1u8; 64]).into_string();

    let response = crate::handlers::transactions::handle_get_transaction(
        state,
        json!(1),
        Some(vec![json!(signature), json!({ "commitment": "processed" })]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(err.code, -32602);
    assert_eq!(
        err.message,
        "Only confirmed or finalized commitments are supported"
    );
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_transaction_processed_served_from_head_cache() {
    let address = Pubkey::new_from_array([0u8; 32]);

    let cache = Arc::new(HeadCache::new(32, TEST_MAX_LIMIT as usize));

    let signature_bytes = [7u8; 64];
    let signature = Signature::from(signature_bytes);
    let signature_str = bs58::encode(signature_bytes).into_string();

    let mut record = base_transaction_record();
    record.slot = 123;
    record.signature = signature_bytes;
    record.tx_signatures = vec![signature_bytes];
    record.tx_account_keys = vec![address.to_bytes()];

    cache.insert_for_tests(signature, record, 0, &[address], CommitmentLevel::Processed);

    let state = test_state_with_head_cache(cache);
    let response = crate::handlers::transactions::handle_get_transaction(
        state,
        json!(1),
        Some(vec![
            json!(signature_str),
            json!({ "commitment": "processed" }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    assert!(
        parsed.error.is_none(),
        "expected success: {:?}",
        parsed.error
    );
    let result = parsed.result.expect("result");
    assert_eq!(result.get("slot").and_then(|v| v.as_u64()), Some(123));
    assert_eq!(result.get("blockTime").and_then(|v| v.as_i64()), Some(123));
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_transaction_with_slot_returns_null_on_head_cache_slot_mismatch() {
    let address = Pubkey::new_from_array([0u8; 32]);

    let cache = Arc::new(HeadCache::new(32, TEST_MAX_LIMIT as usize));

    let signature_bytes = [8u8; 64];
    let signature = Signature::from(signature_bytes);
    let signature_str = bs58::encode(signature_bytes).into_string();

    let mut record = base_transaction_record();
    record.slot = 123;
    record.signature = signature_bytes;
    record.tx_signatures = vec![signature_bytes];
    record.tx_account_keys = vec![address.to_bytes()];

    cache.insert_for_tests(signature, record, 0, &[address], CommitmentLevel::Processed);

    let state = test_state_with_head_cache(cache);
    let response = crate::handlers::transactions::handle_get_transaction(
        state,
        json!(1),
        Some(vec![
            json!(signature_str),
            json!({ "commitment": "processed", "slot": 124 }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    assert!(parsed.error.is_none(), "expected null result, not error");
    assert_eq!(parsed.result, Some(Value::Null));
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_transaction_processed_head_cache_uses_slot_block_time_backfill() {
    let address = Pubkey::new_from_array([17u8; 32]);

    let cache = Arc::new(HeadCache::new(32, TEST_MAX_LIMIT as usize));

    let signature_bytes = [19u8; 64];
    let signature = Signature::from(signature_bytes);
    let signature_str = bs58::encode(signature_bytes).into_string();

    let mut record = base_transaction_record();
    record.slot = 456;
    record.block_time = None;
    record.signature = signature_bytes;
    record.tx_signatures = vec![signature_bytes];
    record.tx_account_keys = vec![address.to_bytes()];

    cache.insert_for_tests(signature, record, 0, &[address], CommitmentLevel::Processed);
    cache.note_block_time(456, 1_700_000_000);

    let state = test_state_with_head_cache(cache);
    let response = crate::handlers::transactions::handle_get_transaction(
        state,
        json!(1),
        Some(vec![
            json!(signature_str),
            json!({ "commitment": "processed" }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    assert!(
        parsed.error.is_none(),
        "expected success: {:?}",
        parsed.error
    );
    let result = parsed.result.expect("result");
    assert_eq!(result.get("slot").and_then(|v| v.as_u64()), Some(456));
    assert_eq!(
        result.get("blockTime").and_then(|v| v.as_i64()),
        Some(1_700_000_000)
    );
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_signatures_for_address_processed_served_from_head_cache() {
    let address_str = "11111111111111111111111111111111";
    let address = Pubkey::new_from_array([0u8; 32]);

    let cache = Arc::new(HeadCache::new(32, TEST_MAX_LIMIT as usize));

    let sig1_bytes = [1u8; 64];
    let sig1 = Signature::from(sig1_bytes);
    let sig1_str = bs58::encode(sig1_bytes).into_string();

    let mut rec1 = base_transaction_record();
    rec1.slot = 10;
    rec1.signature = sig1_bytes;
    rec1.tx_signatures = vec![sig1_bytes];
    rec1.tx_account_keys = vec![address.to_bytes()];

    cache.insert_for_tests(sig1, rec1, 7, &[address], CommitmentLevel::Processed);

    let sig2_bytes = [2u8; 64];
    let sig2 = Signature::from(sig2_bytes);
    let sig2_str = bs58::encode(sig2_bytes).into_string();

    let mut rec2 = base_transaction_record();
    rec2.slot = 11;
    rec2.signature = sig2_bytes;
    rec2.tx_signatures = vec![sig2_bytes];
    rec2.tx_account_keys = vec![address.to_bytes()];

    cache.insert_for_tests(sig2, rec2, 2, &[address], CommitmentLevel::Processed);

    let state = test_state_with_head_cache(cache);
    let response = handle_get_signatures_for_address(
        state,
        json!(1),
        Some(vec![
            json!(address_str),
            json!({ "limit": 2, "commitment": "processed" }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    assert!(
        parsed.error.is_none(),
        "expected success: {:?}",
        parsed.error
    );
    let result = parsed.result.expect("result");
    let arr = result.as_array().expect("array result");
    assert_eq!(arr.len(), 2);

    assert_eq!(
        arr[0].get("signature").and_then(|v| v.as_str()),
        Some(sig2_str.as_str())
    );
    assert_eq!(arr[0].get("slot").and_then(|v| v.as_u64()), Some(11));
    assert_eq!(
        arr[0].get("confirmationStatus").and_then(|v| v.as_str()),
        Some("processed")
    );

    assert_eq!(
        arr[1].get("signature").and_then(|v| v.as_str()),
        Some(sig1_str.as_str())
    );
    assert_eq!(arr[1].get("slot").and_then(|v| v.as_u64()), Some(10));
    assert_eq!(
        arr[1].get("confirmationStatus").and_then(|v| v.as_str()),
        Some("processed")
    );
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_signatures_for_address_processed_honors_before_slot() {
    let address_str = "11111111111111111111111111111111";
    let address = Pubkey::new_from_array([0u8; 32]);

    let cache = Arc::new(HeadCache::new(32, TEST_MAX_LIMIT as usize));

    let sig1_bytes = [5u8; 64];
    let sig1 = Signature::from(sig1_bytes);
    let sig1_str = bs58::encode(sig1_bytes).into_string();

    let mut rec1 = base_transaction_record();
    rec1.slot = 10;
    rec1.signature = sig1_bytes;
    rec1.tx_signatures = vec![sig1_bytes];
    rec1.tx_account_keys = vec![address.to_bytes()];
    cache.insert_for_tests(sig1, rec1, 7, &[address], CommitmentLevel::Processed);

    let sig2_bytes = [6u8; 64];
    let sig2 = Signature::from(sig2_bytes);

    let mut rec2 = base_transaction_record();
    rec2.slot = 11;
    rec2.signature = sig2_bytes;
    rec2.tx_signatures = vec![sig2_bytes];
    rec2.tx_account_keys = vec![address.to_bytes()];
    cache.insert_for_tests(sig2, rec2, 2, &[address], CommitmentLevel::Processed);

    let state = test_state_with_head_cache(cache);
    let response = handle_get_signatures_for_address(
        state,
        json!(1),
        Some(vec![
            json!(address_str),
            json!({ "limit": 1, "commitment": "processed", "beforeSlot": 11 }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    assert!(
        parsed.error.is_none(),
        "expected success: {:?}",
        parsed.error
    );
    let result = parsed.result.expect("result");
    let arr = result.as_array().expect("array result");
    assert_eq!(arr.len(), 1);
    assert_eq!(
        arr[0].get("signature").and_then(|v| v.as_str()),
        Some(sig1_str.as_str())
    );
    assert_eq!(arr[0].get("slot").and_then(|v| v.as_u64()), Some(10));
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_signatures_for_address_confirmed_partial_head_cache_returns_long_term_storage_error() {
    let address_str = "11111111111111111111111111111111";
    let address = Pubkey::new_from_array([0u8; 32]);

    let cache = Arc::new(HeadCache::new(32, TEST_MAX_LIMIT as usize));

    let signature_bytes = [12u8; 64];
    let signature = Signature::from(signature_bytes);
    let mut record = base_transaction_record();
    record.slot = 55;
    record.signature = signature_bytes;
    record.tx_signatures = vec![signature_bytes];
    record.tx_account_keys = vec![address.to_bytes()];

    cache.insert_for_tests(signature, record, 4, &[address], CommitmentLevel::Confirmed);

    let state = test_state_with_head_cache_and_clickhouse_url(cache, "http://127.0.0.1:1");
    let response = handle_get_signatures_for_address(
        state,
        json!(1),
        Some(vec![
            json!(address_str),
            json!({ "limit": 2, "commitment": "confirmed" }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(
        err.code,
        JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_UNREACHABLE as i32
    );
    assert_eq!(
        err.message,
        "Failed to query long-term storage; please try again"
    );
    assert_eq!(err.data, None);

    let wrapped = format!(
        "failed to get signatures for address: {}",
        err.message.as_str()
    );
    assert_eq!(
        wrapped,
        "failed to get signatures for address: Failed to query long-term storage; please try again"
    );
    assert_eq!(parsed.result, None);
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_signatures_for_address_head_cache_missing_before_returns_filter_transaction_not_found()
{
    let cache = Arc::new(HeadCache::new(32, TEST_MAX_LIMIT as usize));
    let missing_signature_bytes = [33u8; 64];
    let state = test_state_with_head_cache_and_cached_signature_slot(
        cache,
        "http://127.0.0.1:1",
        missing_signature_bytes,
        None,
    )
    .await;
    let missing_signature = bs58::encode(missing_signature_bytes).into_string();

    let response = handle_get_signatures_for_address(
        state,
        json!(1),
        Some(vec![
            json!("11111111111111111111111111111111"),
            json!({ "before": missing_signature, "commitment": "confirmed" }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(
        err.code,
        JSON_RPC_SERVER_ERROR_FILTER_TRANSACTION_NOT_FOUND as i32
    );
    assert_eq!(
        err.message,
        format!("Transaction {missing_signature} not found")
    );
    assert_eq!(err.data, None);
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_signatures_for_address_head_cache_missing_until_returns_filter_transaction_not_found()
{
    let cache = Arc::new(HeadCache::new(32, TEST_MAX_LIMIT as usize));
    let missing_signature_bytes = [34u8; 64];
    let state = test_state_with_head_cache_and_cached_signature_slot(
        cache,
        "http://127.0.0.1:1",
        missing_signature_bytes,
        None,
    )
    .await;
    let missing_signature = bs58::encode(missing_signature_bytes).into_string();

    let response = handle_get_signatures_for_address(
        state,
        json!(1),
        Some(vec![
            json!("11111111111111111111111111111111"),
            json!({ "until": missing_signature, "commitment": "confirmed" }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    let err = parsed.error.expect("error present");
    assert_eq!(
        err.code,
        JSON_RPC_SERVER_ERROR_FILTER_TRANSACTION_NOT_FOUND as i32
    );
    assert_eq!(
        err.message,
        format!("Transaction {missing_signature} not found")
    );
    assert_eq!(err.data, None);
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_signatures_for_address_head_cache_until_uses_global_signature_boundary() {
    let target_address_str = "11111111111111111111111111111111";
    let target_address = Pubkey::new_from_array([0u8; 32]);
    let other_address = Pubkey::new_from_array([9u8; 32]);

    let cache = Arc::new(HeadCache::new(32, TEST_MAX_LIMIT as usize));
    let cursor_signature_bytes = [35u8; 64];
    let cursor_signature = Signature::from(cursor_signature_bytes);
    let cursor_signature_str = bs58::encode(cursor_signature_bytes).into_string();

    let mut cursor_record = base_transaction_record();
    cursor_record.slot = 77;
    cursor_record.signature = cursor_signature_bytes;
    cursor_record.tx_signatures = vec![cursor_signature_bytes];
    cursor_record.tx_account_keys = vec![other_address.to_bytes()];

    cache.insert_for_tests(
        cursor_signature,
        cursor_record,
        1,
        &[other_address],
        CommitmentLevel::Processed,
    );

    let target_signature_bytes = [37u8; 64];
    let target_signature = Signature::from(target_signature_bytes);
    let target_signature_str = bs58::encode(target_signature_bytes).into_string();

    let mut target_record = base_transaction_record();
    target_record.slot = 78;
    target_record.signature = target_signature_bytes;
    target_record.tx_signatures = vec![target_signature_bytes];
    target_record.tx_account_keys = vec![target_address.to_bytes()];

    cache.insert_for_tests(
        target_signature,
        target_record,
        0,
        &[target_address],
        CommitmentLevel::Processed,
    );

    let state = test_state_with_head_cache(cache);
    let response = handle_get_signatures_for_address(
        state,
        json!(1),
        Some(vec![
            json!(target_address_str),
            json!({ "limit": 1, "until": cursor_signature_str, "commitment": "processed" }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    assert!(
        parsed.error.is_none(),
        "expected success: {:?}",
        parsed.error
    );
    let result = parsed.result.expect("result");
    let arr = result.as_array().expect("array result");
    assert_eq!(arr.len(), 1);
    assert_eq!(
        arr[0].get("signature").and_then(|v| v.as_str()),
        Some(target_signature_str.as_str())
    );
    assert_eq!(arr[0].get("slot").and_then(|v| v.as_u64()), Some(78));

    assert_ne!(target_address, other_address);
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_signatures_for_address_head_cache_before_uses_global_signature_boundary() {
    let target_address_str = "11111111111111111111111111111111";
    let target_address = Pubkey::new_from_array([0u8; 32]);
    let other_address = Pubkey::new_from_array([8u8; 32]);

    let cache = Arc::new(HeadCache::new(32, TEST_MAX_LIMIT as usize));
    let cursor_signature_bytes = [36u8; 64];
    let cursor_signature = Signature::from(cursor_signature_bytes);
    let cursor_signature_str = bs58::encode(cursor_signature_bytes).into_string();

    let mut cursor_record = base_transaction_record();
    cursor_record.slot = 88;
    cursor_record.signature = cursor_signature_bytes;
    cursor_record.tx_signatures = vec![cursor_signature_bytes];
    cursor_record.tx_account_keys = vec![other_address.to_bytes()];

    cache.insert_for_tests(
        cursor_signature,
        cursor_record,
        2,
        &[other_address],
        CommitmentLevel::Processed,
    );

    let target_signature_bytes = [38u8; 64];
    let target_signature = Signature::from(target_signature_bytes);
    let target_signature_str = bs58::encode(target_signature_bytes).into_string();

    let mut target_record = base_transaction_record();
    target_record.slot = 87;
    target_record.signature = target_signature_bytes;
    target_record.tx_signatures = vec![target_signature_bytes];
    target_record.tx_account_keys = vec![target_address.to_bytes()];

    cache.insert_for_tests(
        target_signature,
        target_record,
        9,
        &[target_address],
        CommitmentLevel::Processed,
    );

    let state = test_state_with_head_cache(cache);
    let response = handle_get_signatures_for_address(
        state,
        json!(1),
        Some(vec![
            json!(target_address_str),
            json!({ "limit": 1, "before": cursor_signature_str, "commitment": "processed" }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    assert!(
        parsed.error.is_none(),
        "expected success: {:?}",
        parsed.error
    );
    let result = parsed.result.expect("result");
    let arr = result.as_array().expect("array result");
    assert_eq!(arr.len(), 1);
    assert_eq!(
        arr[0].get("signature").and_then(|v| v.as_str()),
        Some(target_signature_str.as_str())
    );
    assert_eq!(arr[0].get("slot").and_then(|v| v.as_u64()), Some(87));
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_transactions_for_address_processed_head_only_pagination_token_is_slot_index() {
    let address_str = "11111111111111111111111111111111";
    let address = Pubkey::new_from_array([0u8; 32]);

    let cache = Arc::new(HeadCache::new(32, TEST_MAX_LIMIT as usize));

    let sig1_bytes = [3u8; 64];
    let sig1 = Signature::from(sig1_bytes);
    let sig1_str = bs58::encode(sig1_bytes).into_string();

    let mut rec1 = base_transaction_record();
    rec1.slot = 10;
    rec1.signature = sig1_bytes;
    rec1.tx_signatures = vec![sig1_bytes];
    rec1.tx_account_keys = vec![address.to_bytes()];

    cache.insert_for_tests(sig1, rec1, 3, &[address], CommitmentLevel::Processed);

    let sig2_bytes = [4u8; 64];
    let sig2 = Signature::from(sig2_bytes);
    let sig2_str = bs58::encode(sig2_bytes).into_string();

    let mut rec2 = base_transaction_record();
    rec2.slot = 11;
    rec2.signature = sig2_bytes;
    rec2.tx_signatures = vec![sig2_bytes];
    rec2.tx_account_keys = vec![address.to_bytes()];

    cache.insert_for_tests(sig2, rec2, 9, &[address], CommitmentLevel::Processed);

    let state = test_state_with_head_cache(cache);
    let response = handle_get_transactions_for_address(
        state,
        json!(1),
        Some(vec![
            json!(address_str),
            json!({
                "transactionDetails": "signatures",
                "limit": 2,
                "commitment": "processed"
            }),
        ]),
    )
    .await
    .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    assert!(
        parsed.error.is_none(),
        "expected success: {:?}",
        parsed.error
    );
    let result = parsed.result.expect("result");
    let data = result
        .get("data")
        .and_then(|v| v.as_array())
        .expect("data array");
    assert_eq!(data.len(), 2);

    assert_eq!(
        data[0].get("signature").and_then(|v| v.as_str()),
        Some(sig2_str.as_str())
    );
    assert_eq!(data[0].get("slot").and_then(|v| v.as_u64()), Some(11));
    assert_eq!(
        data[0].get("transactionIndex").and_then(|v| v.as_u64()),
        Some(9)
    );
    assert_eq!(
        data[0].get("confirmationStatus").and_then(|v| v.as_str()),
        Some("processed")
    );

    assert_eq!(
        data[1].get("signature").and_then(|v| v.as_str()),
        Some(sig1_str.as_str())
    );
    assert_eq!(data[1].get("slot").and_then(|v| v.as_u64()), Some(10));
    assert_eq!(
        data[1].get("transactionIndex").and_then(|v| v.as_u64()),
        Some(3)
    );
    assert_eq!(
        data[1].get("confirmationStatus").and_then(|v| v.as_str()),
        Some("processed")
    );

    assert_eq!(
        result
            .get("paginationToken")
            .and_then(|v| v.as_str())
            .map(str::to_string),
        Some("10:3".to_string())
    );
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_signature_statuses_served_from_head_cache() {
    let address = Pubkey::new_from_array([0u8; 32]);

    let cache = Arc::new(HeadCache::new(32, TEST_MAX_LIMIT as usize));

    let signature_bytes = [9u8; 64];
    let signature = Signature::from(signature_bytes);
    let signature_str = bs58::encode(signature_bytes).into_string();

    let mut record = base_transaction_record();
    record.slot = 42;
    record.signature = signature_bytes;
    record.tx_signatures = vec![signature_bytes];
    record.tx_account_keys = vec![address.to_bytes()];

    cache.insert_for_tests(signature, record, 0, &[address], CommitmentLevel::Processed);

    let state = test_state_with_head_cache(cache);
    let response =
        handle_get_signature_statuses(state, json!(1), Some(vec![json!([signature_str])]))
            .await
            .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    assert!(
        parsed.error.is_none(),
        "expected success: {:?}",
        parsed.error
    );

    let result = parsed.result.expect("result");
    assert_eq!(
        result
            .get("context")
            .and_then(|v| v.get("slot"))
            .and_then(|v| v.as_u64()),
        Some(42)
    );
    let value = result
        .get("value")
        .and_then(|v| v.as_array())
        .expect("value array");
    assert_eq!(value.len(), 1);
    let entry = value[0].as_object().expect("status object");
    assert_eq!(entry.get("slot").and_then(|v| v.as_u64()), Some(42));
    assert_eq!(entry.get("confirmations").and_then(|v| v.as_u64()), Some(0));
    assert_eq!(
        entry.get("confirmationStatus").and_then(|v| v.as_str()),
        Some("processed")
    );
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_signature_statuses_confirmed_head_cache_uses_non_null_confirmations() {
    let address = Pubkey::new_from_array([0u8; 32]);

    let cache = Arc::new(HeadCache::new(32, TEST_MAX_LIMIT as usize));

    let signature_bytes = [10u8; 64];
    let signature = Signature::from(signature_bytes);
    let signature_str = bs58::encode(signature_bytes).into_string();

    let mut record = base_transaction_record();
    record.slot = 84;
    record.signature = signature_bytes;
    record.tx_signatures = vec![signature_bytes];
    record.tx_account_keys = vec![address.to_bytes()];

    cache.insert_for_tests(signature, record, 0, &[address], CommitmentLevel::Confirmed);

    let state = test_state_with_head_cache(cache);
    let response =
        handle_get_signature_statuses(state, json!(1), Some(vec![json!([signature_str])]))
            .await
            .expect("response");

    let parsed = parse_json_rpc_response(response).await;
    assert!(
        parsed.error.is_none(),
        "expected success: {:?}",
        parsed.error
    );

    let result = parsed.result.expect("result");
    assert_eq!(
        result
            .get("context")
            .and_then(|v| v.get("slot"))
            .and_then(|v| v.as_u64()),
        Some(84)
    );
    let value = result
        .get("value")
        .and_then(|v| v.as_array())
        .expect("value array");
    assert_eq!(value.len(), 1);
    let entry = value[0].as_object().expect("status object");
    assert_eq!(entry.get("slot").and_then(|v| v.as_u64()), Some(84));
    assert_eq!(entry.get("confirmations").and_then(|v| v.as_u64()), Some(1));
    assert_eq!(
        entry.get("confirmationStatus").and_then(|v| v.as_str()),
        Some("confirmed")
    );
}

#[cfg(feature = "grpc-head-cache")]
#[test]
fn head_cache_eviction_removes_address_entries() {
    let address_a = Pubkey::new_from_array([1u8; 32]);
    let address_b = Pubkey::new_from_array([2u8; 32]);

    let cache = HeadCache::new(1, TEST_MAX_LIMIT as usize);

    let sig1_bytes = [1u8; 64];
    let sig1 = Signature::from(sig1_bytes);
    let mut rec1 = base_transaction_record();
    rec1.slot = 1;
    cache.insert_for_tests(sig1, rec1, 0, &[address_a], CommitmentLevel::Processed);

    let sig2_bytes = [2u8; 64];
    let sig2 = Signature::from(sig2_bytes);
    let mut rec2 = base_transaction_record();
    rec2.slot = 2;
    cache.insert_for_tests(sig2, rec2, 0, &[address_b], CommitmentLevel::Processed);

    // retain_slots=1 => inserting slot 2 should evict slot 1 and fully remove address_a.
    assert_eq!(cache.address_entries(), 1);
    let metas_a =
        cache.signatures_for_address(&address_a, None, None, 10, CommitmentLevel::Processed);
    assert!(metas_a.is_empty());
}

#[cfg(feature = "grpc-head-cache")]
#[test]
fn head_cache_remove_slot_prunes_exact_slot_only() {
    let address = Pubkey::new_from_array([3u8; 32]);

    let cache = HeadCache::new(32, TEST_MAX_LIMIT as usize);

    let sig1 = Signature::from([11u8; 64]);
    let mut rec1 = base_transaction_record();
    rec1.slot = 10;
    cache.insert_for_tests(sig1, rec1, 0, &[address], CommitmentLevel::Processed);

    let sig2 = Signature::from([12u8; 64]);
    let mut rec2 = base_transaction_record();
    rec2.slot = 11;
    cache.insert_for_tests(sig2, rec2, 0, &[address], CommitmentLevel::Processed);

    cache.remove_slot(11);

    let metas = cache.signatures_for_address(&address, None, None, 10, CommitmentLevel::Processed);
    assert_eq!(metas.len(), 1);
    assert_eq!(metas[0].pos.slot, 10);
}

#[cfg(feature = "grpc-head-cache")]
#[test]
fn head_cache_note_block_time_backfills_existing_entries() {
    let address = Pubkey::new_from_array([21u8; 32]);
    let cache = HeadCache::new(32, TEST_MAX_LIMIT as usize);
    let signature_bytes = [22u8; 64];
    let signature = Signature::from(signature_bytes);

    let mut record = base_transaction_record();
    record.slot = 77;
    record.block_time = None;
    record.signature = signature_bytes;
    record.tx_signatures = vec![signature_bytes];
    record.tx_account_keys = vec![address.to_bytes()];

    cache.insert_for_tests(signature, record, 0, &[address], CommitmentLevel::Processed);

    let before_tx = cache
        .get_tx(&signature, CommitmentLevel::Processed)
        .expect("head tx exists");
    assert!(before_tx.block_time.is_none());
    let before_meta = cache
        .get_meta(&signature, CommitmentLevel::Processed)
        .expect("head meta exists");
    assert!(before_meta.block_time.is_none());

    cache.note_block_time(77, 1_700_000_001);

    let after_tx = cache
        .get_tx(&signature, CommitmentLevel::Processed)
        .expect("head tx exists");
    assert_eq!(after_tx.block_time, Some(1_700_000_001));
    let after_meta = cache
        .get_meta(&signature, CommitmentLevel::Processed)
        .expect("head meta exists");
    assert_eq!(after_meta.block_time, Some(1_700_000_001));
}

#[cfg(feature = "grpc-head-cache")]
#[test]
fn head_cache_insert_for_tests_uses_known_slot_block_time() {
    let address = Pubkey::new_from_array([23u8; 32]);
    let cache = HeadCache::new(32, TEST_MAX_LIMIT as usize);
    let signature_bytes = [24u8; 64];
    let signature = Signature::from(signature_bytes);

    cache.note_block_time(88, 1_700_000_002);

    let mut record = base_transaction_record();
    record.slot = 88;
    record.block_time = None;
    record.signature = signature_bytes;
    record.tx_signatures = vec![signature_bytes];
    record.tx_account_keys = vec![address.to_bytes()];

    cache.insert_for_tests(signature, record, 0, &[address], CommitmentLevel::Processed);

    let tx = cache
        .get_tx(&signature, CommitmentLevel::Processed)
        .expect("head tx exists");
    assert_eq!(tx.block_time, Some(1_700_000_002));
    let meta = cache
        .get_meta(&signature, CommitmentLevel::Processed)
        .expect("head meta exists");
    assert_eq!(meta.block_time, Some(1_700_000_002));
}

#[test]
fn build_versioned_transaction_rejects_signature_count_mismatch() {
    let mut record = base_transaction_record();
    record.tx_num_required_signatures = 2;

    let err = build_versioned_transaction(&record).expect_err("expected error");
    match err {
        TransactionHydrationError::InvalidStoredTransaction(msg) => {
            assert!(msg.contains("signature count"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn build_versioned_transaction_rejects_account_key_mismatch() {
    let mut record = base_transaction_record();
    record.tx_account_keys.clear();

    let err = build_versioned_transaction(&record).expect_err("expected error");
    match err {
        TransactionHydrationError::InvalidStoredTransaction(msg) => {
            assert!(msg.contains("account_keys length"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn build_versioned_transaction_rejects_instruction_length_mismatch() {
    let mut record = base_transaction_record();
    record.tx_instructions_program_id_index = vec![0, 1];
    record.tx_instructions_accounts = vec![vec![0u8]];
    record.tx_instructions_data = vec![vec![0u8]];

    let err = build_versioned_transaction(&record).expect_err("expected error");
    match err {
        TransactionHydrationError::InvalidStoredTransaction(msg) => {
            assert!(msg.contains("instruction array length mismatch"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn build_versioned_transaction_rejects_legacy_address_table_lookups() {
    let mut record = base_transaction_record();
    record.tx_address_table_lookups_present = true;

    let err = build_versioned_transaction(&record).expect_err("expected error");
    match err {
        TransactionHydrationError::InvalidStoredTransaction(msg) => {
            assert!(msg.contains("legacy transaction contains address table lookups"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn build_versioned_transaction_rejects_lookup_length_mismatch() {
    let mut record = base_transaction_record();
    record.tx_version = Some(0);
    record.tx_address_table_lookup_account_key = vec![[0u8; 32]];
    record.tx_address_table_lookup_writable_indexes = Vec::new();
    record.tx_address_table_lookup_readonly_indexes = Vec::new();

    let err = build_versioned_transaction(&record).expect_err("expected error");
    match err {
        TransactionHydrationError::InvalidStoredTransaction(msg) => {
            assert!(msg.contains("address table lookup length mismatch"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn hydrate_block_record_filters_zero_time_and_height() {
    let record = base_block_record(10);

    let encoded = hydrate_block_record(
        record,
        UiTransactionEncoding::Json,
        TransactionDetails::Signatures,
        true,
        None,
    )
    .expect("hydrate block");

    assert!(encoded.block_time.is_none());
    assert!(encoded.block_height.is_none());
}

#[test]
fn hydrate_block_record_none_skips_transaction_validation() {
    let mut block = base_block_record(10);
    let mut record = base_transaction_record();
    record.tx_version = Some(9);
    record.tx_signatures = vec![[5u8; 64]];
    record.signature = [5u8; 64];
    block.metadata.executed_transaction_count = 1;
    block.transactions.push(record);

    let encoded = hydrate_block_record(
        block,
        UiTransactionEncoding::Json,
        TransactionDetails::None,
        true,
        None,
    )
    .expect("metadata-only block");

    assert!(encoded.transactions.is_none());
    assert!(encoded.signatures.is_none());
}

#[test]
fn hydrate_block_record_accounts_encodes_account_list() {
    let mut block = base_block_record(10);
    let mut record = base_transaction_record();
    record.signature = [6u8; 64];
    record.tx_signatures = vec![[6u8; 64]];
    record.tx_account_keys = vec![[1u8; 32], [2u8; 32]];
    record.tx_num_required_signatures = 1;
    record.tx_num_readonly_unsigned_accounts = 1;
    record.meta_pre_balances = vec![10, 20];
    record.meta_post_balances = vec![11, 19];
    block.metadata.executed_transaction_count = 1;
    block.transactions.push(record);

    let encoded = hydrate_block_record(
        block,
        UiTransactionEncoding::Base64,
        TransactionDetails::Accounts,
        false,
        Some(0),
    )
    .expect("accounts block");

    let transactions = encoded.transactions.expect("transactions");
    assert_eq!(transactions.len(), 1);
    match &transactions[0].transaction {
        EncodedTransaction::Accounts(accounts) => {
            assert_eq!(
                accounts.signatures,
                vec![bs58::encode([6u8; 64]).into_string()]
            );
            assert_eq!(accounts.account_keys.len(), 2);
            assert!(accounts.account_keys[0].signer);
            assert!(accounts.account_keys[0].writable);
            assert!(!accounts.account_keys[1].signer);
            assert!(!accounts.account_keys[1].writable);
        }
        other => panic!("unexpected transaction encoding: {other:?}"),
    }
}

#[test]
fn hydrate_block_record_accounts_demotes_invoked_program_account() {
    let mut block = base_block_record(10);
    let mut record = base_transaction_record();
    record.signature = [7u8; 64];
    record.tx_signatures = vec![[7u8; 64]];
    record.tx_account_keys = vec![[1u8; 32], [2u8; 32]];
    record.tx_num_required_signatures = 1;
    record.tx_num_readonly_unsigned_accounts = 0;
    record.tx_instructions_program_id_index = vec![1];
    record.meta_pre_balances = vec![10, 20];
    record.meta_post_balances = vec![10, 20];
    block.metadata.executed_transaction_count = 1;
    block.transactions.push(record);

    let encoded = hydrate_block_record(
        block,
        UiTransactionEncoding::Json,
        TransactionDetails::Accounts,
        false,
        Some(0),
    )
    .expect("accounts block");

    let transactions = encoded.transactions.expect("transactions");
    match &transactions[0].transaction {
        EncodedTransaction::Accounts(accounts) => {
            assert!(accounts.account_keys[0].writable);
            assert!(!accounts.account_keys[1].writable);
        }
        other => panic!("unexpected transaction encoding: {other:?}"),
    }
}

#[test]
fn hydrate_block_payload_signatures_rejects_metadata_projection() {
    let payload = StoredBlockPayload::Metadata(base_block_record(10).metadata);

    let err = hydrate_block_payload(
        payload,
        UiTransactionEncoding::Json,
        TransactionDetails::Signatures,
        true,
        None,
    )
    .expect_err("expected error");

    match err {
        BlockHydrationError::Transaction(TransactionHydrationError::InvalidStoredTransaction(
            msg,
        )) => {
            assert!(msg.contains("signatures block payload required"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn hydrate_block_payload_accounts_rejects_signatures_projection() {
    let metadata = base_block_record(10).metadata;
    let payload = StoredBlockPayload::Signatures {
        metadata,
        signatures: vec![bs58::encode([8u8; 64]).into_string()],
    };

    let err = hydrate_block_payload(
        payload,
        UiTransactionEncoding::Json,
        TransactionDetails::Accounts,
        true,
        None,
    )
    .expect_err("expected error");

    match err {
        BlockHydrationError::Transaction(TransactionHydrationError::InvalidStoredTransaction(
            msg,
        )) => {
            assert!(msg.contains("accounts block payload required"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn hydrate_block_payload_none_matches_legacy_full_encoding() {
    let record = projection_equivalence_block_record();
    let expected = encode_block_via_legacy_full_path(
        record.clone(),
        UiTransactionEncoding::Base64,
        TransactionDetails::None,
        Some(0),
    )
    .expect("legacy encoding");

    let actual = hydrate_block_payload(
        StoredBlockPayload::Metadata(record.metadata),
        UiTransactionEncoding::Base64,
        TransactionDetails::None,
        false,
        Some(0),
    )
    .expect("optimized encoding");

    assert_eq!(
        serde_json::to_value(actual).expect("serialize actual"),
        serde_json::to_value(expected).expect("serialize expected")
    );
}

#[test]
fn hydrate_block_payload_signatures_matches_legacy_full_encoding() {
    let record = projection_equivalence_block_record();
    let expected = encode_block_via_legacy_full_path(
        record.clone(),
        UiTransactionEncoding::Base64,
        TransactionDetails::Signatures,
        Some(0),
    )
    .expect("legacy encoding");
    let signatures = record
        .transactions
        .iter()
        .map(|tx| bs58::encode(tx.signature).into_string())
        .collect();

    let actual = hydrate_block_payload(
        StoredBlockPayload::Signatures {
            metadata: record.metadata,
            signatures,
        },
        UiTransactionEncoding::Base64,
        TransactionDetails::Signatures,
        false,
        Some(0),
    )
    .expect("optimized encoding");

    assert_eq!(
        serde_json::to_value(actual).expect("serialize actual"),
        serde_json::to_value(expected).expect("serialize expected")
    );
}

#[test]
fn hydrate_block_payload_accounts_matches_legacy_full_encoding() {
    let record = projection_equivalence_block_record();
    let expected = encode_block_via_legacy_full_path(
        record.clone(),
        UiTransactionEncoding::Base64,
        TransactionDetails::Accounts,
        Some(0),
    )
    .expect("legacy encoding");
    let transactions = record
        .transactions
        .clone()
        .into_iter()
        .map(Into::into)
        .collect();

    let actual = hydrate_block_payload(
        StoredBlockPayload::Accounts {
            metadata: record.metadata,
            transactions,
        },
        UiTransactionEncoding::Base64,
        TransactionDetails::Accounts,
        false,
        Some(0),
    )
    .expect("optimized encoding");

    assert_eq!(
        serde_json::to_value(actual).expect("serialize actual"),
        serde_json::to_value(expected).expect("serialize expected")
    );
}

#[test]
fn hydrate_transaction_record_filters_zero_time() {
    let mut record = base_transaction_record();
    record.block_time = Some(0);

    let encoded = hydrate_transaction_record(&record, UiTransactionEncoding::Json, None)
        .expect("hydrate transaction");

    assert!(encoded.block_time.is_none());
    assert_eq!(encoded.transaction_index, Some(0));
}

#[test]
fn build_transaction_status_meta_returns_none_when_missing() {
    let record = base_transaction_record();

    let meta = build_transaction_status_meta(&record).expect("build meta");

    assert!(meta.is_none());
}

#[test]
fn build_transaction_status_meta_emits_null_lists_when_absent_before_boundary() {
    let mut record = base_transaction_record();
    record.meta_pre_balances = vec![1];
    record.meta_post_balances = vec![1];

    let meta = build_transaction_status_meta(&record)
        .expect("build meta")
        .expect("meta present");

    assert!(meta.pre_token_balances.is_none());
    assert!(meta.post_token_balances.is_none());
    assert!(meta.rewards.is_none());
}

#[test]
fn build_transaction_status_meta_emits_null_lists_when_absent_after_boundary() {
    let mut record = base_transaction_record();
    record.slot = 40_000_000;
    record.meta_pre_balances = vec![1];
    record.meta_post_balances = vec![1];

    let meta = build_transaction_status_meta(&record)
        .expect("build meta")
        .expect("meta present");

    assert!(meta.pre_token_balances.is_none());
    assert!(meta.post_token_balances.is_none());
    assert!(meta.rewards.is_none());
}

#[test]
fn build_transaction_status_meta_emits_empty_lists_when_present() {
    let mut record = base_transaction_record();
    record.meta_pre_balances = vec![1];
    record.meta_post_balances = vec![1];
    record.meta_inner_instructions_present = true;
    record.meta_log_messages_present = true;
    record.meta_pre_token_balances_present = true;
    record.meta_post_token_balances_present = true;
    record.meta_rewards_present = true;

    let meta = build_transaction_status_meta(&record)
        .expect("build meta")
        .expect("meta present");

    assert!(meta.inner_instructions.as_ref().expect("inner").is_empty());
    assert!(meta.log_messages.as_ref().expect("logs").is_empty());
    assert!(
        meta.pre_token_balances
            .as_ref()
            .expect("pre tokens")
            .is_empty()
    );
    assert!(
        meta.post_token_balances
            .as_ref()
            .expect("post tokens")
            .is_empty()
    );
    assert!(meta.rewards.as_ref().expect("rewards").is_empty());
}

#[test]
fn hydrate_block_record_rejects_rewards_when_flag_false() {
    let mut record = base_block_record(10);
    record.metadata.rewards_pubkey.push([9u8; 32]);

    let err = hydrate_block_record(
        record,
        UiTransactionEncoding::Json,
        TransactionDetails::Signatures,
        true,
        None,
    )
    .expect_err("expected error");

    match err {
        BlockHydrationError::InvalidBlockMetadata(msg) => {
            assert!(msg.contains("rewards fields populated without rewards_present"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn hydrate_block_record_rejects_reward_length_mismatch() {
    let mut record = base_block_record(10);
    record.metadata.rewards_present = true;
    record.metadata.rewards_pubkey.push([9u8; 32]);
    record.metadata.rewards_lamports.push(1);
    record.metadata.rewards_post_balance.push(2);
    record.metadata.rewards_type.push(None);
    // Missing commission entry triggers mismatch.

    let err = hydrate_block_record(
        record,
        UiTransactionEncoding::Json,
        TransactionDetails::Signatures,
        true,
        None,
    )
    .expect_err("expected error");

    match err {
        BlockHydrationError::InvalidBlockMetadata(msg) => {
            assert!(msg.contains("reward length mismatch"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test]
async fn handle_json_rpc_single_response_emits_source_and_clickhouse_headers() {
    let state = test_state();
    let request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getBlocksWithLimit",
        "params": [123, 0]
    });
    let response = handle_json_rpc_value(state, &request).await;

    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response.headers().get("X-Downstream-Timings").is_none(),
        "legacy X-Downstream-Timings header should be absent"
    );

    let source_touched = response
        .headers()
        .get("X-Superbank-Sources")
        .expect("X-Superbank-Sources header present")
        .to_str()
        .expect("X-Superbank-Sources should be valid ASCII");
    assert_eq!(source_touched, "none");

    let clickhouse_metrics = response
        .headers()
        .get("X-Superbank-Metrics")
        .expect("X-Superbank-Metrics header present")
        .to_str()
        .expect("X-Superbank-Metrics should be valid ASCII");
    assert!(
        clickhouse_metrics.contains("rows_read=0"),
        "missing rows_read metric in header: {clickhouse_metrics}"
    );
    assert!(
        clickhouse_metrics.contains("rows_returned=0"),
        "missing rows_returned metric in header: {clickhouse_metrics}"
    );
    assert!(
        clickhouse_metrics.contains("data_read_bytes=0"),
        "missing data_read_bytes metric in header: {clickhouse_metrics}"
    );
}

#[tokio::test]
async fn handle_json_rpc_batch_response_aggregates_clickhouse_metrics_header() {
    let state = test_state();
    let request = json!([
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBlocksWithLimit",
            "params": [123, 0]
        },
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "getBlocksWithLimit",
            "params": [456, 0]
        }
    ]);
    let response = handle_json_rpc_value(state, &request).await;

    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response.headers().get("X-Downstream-Timings").is_none(),
        "legacy X-Downstream-Timings header should be absent"
    );

    let source_touched = response
        .headers()
        .get("X-Superbank-Sources")
        .expect("X-Superbank-Sources header present")
        .to_str()
        .expect("X-Superbank-Sources should be valid ASCII");
    assert_eq!(source_touched, "none");

    let clickhouse_metrics = response
        .headers()
        .get("X-Superbank-Metrics")
        .expect("X-Superbank-Metrics header present")
        .to_str()
        .expect("X-Superbank-Metrics should be valid ASCII");
    assert!(
        clickhouse_metrics.contains("rows_read=0"),
        "missing rows_read metric in batch header: {clickhouse_metrics}"
    );
    assert!(
        clickhouse_metrics.contains("rows_returned=0"),
        "missing rows_returned metric in batch header: {clickhouse_metrics}"
    );
    assert!(
        clickhouse_metrics.contains("data_read_bytes=0"),
        "missing data_read_bytes metric in batch header: {clickhouse_metrics}"
    );

    let body_bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body bytes");
    let value: Value = serde_json::from_slice(&body_bytes).expect("valid JSON body");
    let items = value.as_array().expect("batch response array");
    assert_eq!(items.len(), 2);
}

#[cfg(feature = "disk-cache")]
mod disk_cache_tier {
    //! Read-tier tests for the disk cache. The unreachable ClickHouse URL
    //! (`http://127.0.0.1:1`) proves a request was answered without ClickHouse:
    //! any fallthrough surfaces as an internal/storage error instead.

    use super::*;
    use crate::disk_cache::tests::{block_metadata, transaction};
    use crate::disk_cache::{DiskCache, DiskCacheConfig};
    use crate::handlers::signatures::handle_get_signature_statuses;
    use crate::handlers::transactions::handle_get_transaction;

    const UNREACHABLE_CLICKHOUSE: &str = "http://127.0.0.1:1";

    fn open_disk_cache(dir: &tempfile::TempDir) -> Arc<DiskCache> {
        Arc::new(
            DiskCache::open(DiskCacheConfig {
                path: dir.path().join("db"),
                retain_slots: 1_000_000,
                max_bytes: 0,
                block_cache_bytes: 8 << 20,
                read_concurrency: 4,
            })
            .expect("open disk cache"),
        )
    }

    fn state_with_disk_cache(disk: Arc<DiskCache>) -> Arc<AppState> {
        let mut state =
            match Arc::try_unwrap(test_state_with_clickhouse_url(UNREACHABLE_CLICKHOUSE)) {
                Ok(state) => state,
                Err(_) => panic!("test state should have a single Arc owner"),
            };
        state.disk_cache = Some(disk);
        Arc::new(state)
    }

    /// Block at `slot` with `tx_count` transactions; returns the signatures.
    fn write_block(disk: &DiskCache, slot: u64, parent: u64, tx_count: u32) -> Vec<String> {
        let meta = block_metadata(slot, parent, u64::from(tx_count));
        let txs: Vec<_> = (0..tx_count)
            .map(|idx| Arc::new(transaction(slot, idx)))
            .collect();
        let signatures = txs
            .iter()
            .map(|tx| bs58::encode(tx.signature).into_string())
            .collect();
        disk.write_finalized_slot(&meta, &txs, crate::disk_cache::schema::COVERAGE_SOURCE_LIVE)
            .expect("write block");
        signatures
    }

    #[tokio::test]
    async fn get_transaction_served_from_disk_without_clickhouse() {
        let dir = tempfile::tempdir().expect("tempdir");
        let disk = open_disk_cache(&dir);
        let signatures = write_block(&disk, 100, 99, 2);
        let state = state_with_disk_cache(disk);

        let response = handle_get_transaction(state, json!(1), Some(vec![json!(signatures[1])]))
            .await
            .expect("response");
        let parsed = parse_json_rpc_response(response).await;
        assert!(
            parsed.error.is_none(),
            "expected success: {:?}",
            parsed.error
        );
        let result = parsed.result.expect("result");
        assert_eq!(result.get("slot").and_then(Value::as_u64), Some(100));
    }

    #[tokio::test]
    async fn get_transaction_conclusive_null_for_covered_slot() {
        let dir = tempfile::tempdir().expect("tempdir");
        let disk = open_disk_cache(&dir);
        write_block(&disk, 100, 99, 1);
        let state = state_with_disk_cache(disk);

        // Signature unknown anywhere, but the request pins a slot the disk
        // fully covers: conclusively null, no ClickHouse.
        let unknown = bs58::encode([42u8; 64]).into_string();
        let response = handle_get_transaction(
            state,
            json!(1),
            Some(vec![json!(unknown), json!({ "slot": 100 })]),
        )
        .await
        .expect("response");
        let parsed = parse_json_rpc_response(response).await;
        assert!(parsed.error.is_none(), "expected null: {:?}", parsed.error);
        assert_eq!(parsed.result, Some(Value::Null));
    }

    #[tokio::test]
    async fn get_transaction_miss_still_consults_clickhouse() {
        let dir = tempfile::tempdir().expect("tempdir");
        let disk = open_disk_cache(&dir);
        write_block(&disk, 100, 99, 1);
        let state = state_with_disk_cache(disk);

        // No slot pin: a disk miss proves nothing, so the handler must try
        // ClickHouse — which is unreachable here.
        let unknown = bs58::encode([42u8; 64]).into_string();
        let response = handle_get_transaction(state, json!(1), Some(vec![json!(unknown)]))
            .await
            .expect("response");
        let parsed = parse_json_rpc_response(response).await;
        assert!(parsed.error.is_some(), "fallthrough should hit ClickHouse");
    }

    #[tokio::test]
    async fn get_block_served_from_disk_at_all_detail_levels() {
        let dir = tempfile::tempdir().expect("tempdir");
        let disk = open_disk_cache(&dir);
        let signatures = write_block(&disk, 100, 99, 2);
        let state = state_with_disk_cache(disk);

        for details in ["none", "signatures", "accounts", "full"] {
            let response = handle_get_block(
                state.clone(),
                json!(1),
                Some(vec![
                    json!(100),
                    json!({ "transactionDetails": details, "commitment": "finalized" }),
                ]),
            )
            .await
            .expect("response");
            let parsed = parse_json_rpc_response(response).await;
            assert!(
                parsed.error.is_none(),
                "details={details}: {:?}",
                parsed.error
            );
            let result = parsed.result.expect("result");
            match details {
                "none" => {
                    assert!(result.get("signatures").is_none());
                    assert!(result.get("transactions").is_none());
                }
                "signatures" => {
                    let listed = result
                        .get("signatures")
                        .and_then(Value::as_array)
                        .expect("signatures");
                    assert_eq!(listed.len(), 2);
                    assert_eq!(listed[0].as_str(), Some(signatures[0].as_str()));
                }
                _ => {
                    let transactions = result
                        .get("transactions")
                        .and_then(Value::as_array)
                        .expect("transactions");
                    assert_eq!(transactions.len(), 2);
                }
            }
        }
    }

    #[tokio::test]
    async fn get_block_skipped_slot_is_conclusive() {
        let dir = tempfile::tempdir().expect("tempdir");
        let disk = open_disk_cache(&dir);
        write_block(&disk, 100, 99, 1);
        // Parent link proves 101..=104 were skipped.
        write_block(&disk, 105, 100, 1);
        let state = state_with_disk_cache(disk);

        let response = handle_get_block(state, json!(1), Some(vec![json!(103)]))
            .await
            .expect("response");
        let parsed = parse_json_rpc_response(response).await;
        let error = parsed.error.expect("skipped-slot error");
        assert_eq!(
            error.code,
            solana_rpc_client_api::custom_error::JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED
                as i32
        );
    }

    #[tokio::test]
    async fn get_block_uncovered_slot_falls_to_clickhouse() {
        let dir = tempfile::tempdir().expect("tempdir");
        let disk = open_disk_cache(&dir);
        write_block(&disk, 100, 99, 1);
        let state = state_with_disk_cache(disk);

        let response = handle_get_block(state, json!(1), Some(vec![json!(50)]))
            .await
            .expect("response");
        let parsed = parse_json_rpc_response(response).await;
        let error = parsed
            .error
            .expect("uncovered slot must consult ClickHouse");
        assert_eq!(error.code, -32603);
    }

    #[tokio::test]
    async fn get_block_time_from_disk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let disk = open_disk_cache(&dir);
        write_block(&disk, 100, 99, 1);
        write_block(&disk, 105, 100, 1);
        let state = state_with_disk_cache(disk);

        let response = handle_get_block_time(state.clone(), json!(1), Some(vec![json!(100)]))
            .await
            .expect("response");
        let parsed = parse_json_rpc_response(response).await;
        assert!(parsed.error.is_none(), "{:?}", parsed.error);
        assert_eq!(parsed.result.and_then(|v| v.as_i64()), Some(1_700_000_100));

        // Skipped slot: conclusive error without ClickHouse.
        let response = handle_get_block_time(state, json!(1), Some(vec![json!(102)]))
            .await
            .expect("response");
        let parsed = parse_json_rpc_response(response).await;
        let error = parsed.error.expect("skipped-slot error");
        assert_eq!(error.code, -32009);
    }

    #[tokio::test]
    async fn get_blocks_range_within_disk_coverage() {
        let dir = tempfile::tempdir().expect("tempdir");
        let disk = open_disk_cache(&dir);
        for slot in 100..=105u64 {
            write_block(&disk, slot, slot - 1, 1);
        }
        let state = state_with_disk_cache(disk);

        let response = handle_get_blocks(state, json!(1), Some(vec![json!(100), json!(105)]))
            .await
            .expect("response");
        let parsed = parse_json_rpc_response(response).await;
        assert!(parsed.error.is_none(), "{:?}", parsed.error);
        let slots: Vec<u64> = parsed
            .result
            .and_then(|v| {
                v.as_array()
                    .map(|values| values.iter().filter_map(Value::as_u64).collect())
            })
            .expect("slot list");
        assert_eq!(slots, vec![100, 101, 102, 103, 104, 105]);
    }

    #[tokio::test]
    async fn get_signature_statuses_served_from_disk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let disk = open_disk_cache(&dir);
        let signatures = write_block(&disk, 100, 99, 2);
        let state = state_with_disk_cache(disk);

        let response = handle_get_signature_statuses(
            state,
            json!(1),
            Some(vec![json!([signatures[0], signatures[1]])]),
        )
        .await
        .expect("response");
        let parsed = parse_json_rpc_response(response).await;
        assert!(parsed.error.is_none(), "{:?}", parsed.error);
        let value = parsed
            .result
            .and_then(|mut result| result.get_mut("value").map(Value::take))
            .expect("statuses");
        let statuses = value.as_array().expect("array");
        assert_eq!(statuses.len(), 2);
        for status in statuses {
            assert_eq!(
                status.get("confirmationStatus").and_then(Value::as_str),
                Some("finalized")
            );
            assert_eq!(status.get("slot").and_then(Value::as_u64), Some(100));
        }
    }

    fn state_with_head_and_disk(head: Arc<HeadCache>, disk: Arc<DiskCache>) -> Arc<AppState> {
        let mut state = match Arc::try_unwrap(test_state_with_head_cache_and_clickhouse_url(
            head,
            UNREACHABLE_CLICKHOUSE,
        )) {
            Ok(state) => state,
            Err(_) => panic!("test state should have a single Arc owner"),
        };
        state.disk_cache = Some(disk);
        Arc::new(state)
    }

    /// Block at `slot` whose single transaction touches `address`; returns the
    /// signature string.
    fn write_block_for_address(
        disk: &DiskCache,
        slot: u64,
        parent: u64,
        address: [u8; 32],
        failed: bool,
    ) -> String {
        let meta = block_metadata(slot, parent, 1);
        let mut tx = transaction(slot, 0);
        tx.tx_account_keys = vec![address];
        if failed {
            tx.meta_status_ok = false;
            tx.meta_err = Some("\"AccountNotFound\"".to_string());
        }
        let signature = bs58::encode(tx.signature).into_string();
        disk.write_finalized_slot(
            &meta,
            &[Arc::new(tx)],
            crate::disk_cache::schema::COVERAGE_SOURCE_LIVE,
        )
        .expect("write block");
        signature
    }

    fn empty_head_cache() -> Arc<HeadCache> {
        Arc::new(HeadCache::new(64, TEST_MAX_LIMIT as usize))
    }

    #[tokio::test]
    async fn gsfa_disk_serves_within_coverage_without_clickhouse() {
        let dir = tempfile::tempdir().expect("tempdir");
        let disk = open_disk_cache(&dir);
        let address = [50u8; 32];
        let mut signatures = Vec::new();
        for slot in 100..=105u64 {
            signatures.push(write_block_for_address(
                &disk,
                slot,
                slot - 1,
                address,
                false,
            ));
        }
        let state = state_with_head_and_disk(empty_head_cache(), disk);
        let address_str = bs58::encode(address).into_string();

        // Limit satisfied from disk: no ClickHouse.
        let response = handle_get_signatures_for_address(
            state.clone(),
            json!(1),
            Some(vec![json!(address_str), json!({ "limit": 3 })]),
        )
        .await
        .expect("response");
        let parsed = parse_json_rpc_response(response).await;
        assert!(parsed.error.is_none(), "{:?}", parsed.error);
        let rows = parsed.result.expect("rows");
        let rows = rows.as_array().expect("array");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].get("slot").and_then(Value::as_u64), Some(105));
        assert_eq!(rows[2].get("slot").and_then(Value::as_u64), Some(103));
        assert_eq!(
            rows[0].get("confirmationStatus").and_then(Value::as_str),
            Some("finalized")
        );

        // until inside coverage: fully answered even though fewer than limit.
        let response = handle_get_signatures_for_address(
            state.clone(),
            json!(1),
            Some(vec![
                json!(address_str),
                json!({ "limit": 100, "untilSlot": 102 }),
            ]),
        )
        .await
        .expect("response");
        let parsed = parse_json_rpc_response(response).await;
        assert!(parsed.error.is_none(), "{:?}", parsed.error);
        let rows = parsed.result.expect("rows");
        assert_eq!(rows.as_array().map(Vec::len), Some(3)); // 105, 104, 103

        // before resolved by the disk signature index, until keeps the scan
        // inside coverage: still no ClickHouse.
        let response = handle_get_signatures_for_address(
            state.clone(),
            json!(1),
            Some(vec![
                json!(address_str),
                json!({ "limit": 100, "before": signatures[4], "untilSlot": 101 }),
            ]),
        )
        .await
        .expect("response");
        let parsed = parse_json_rpc_response(response).await;
        assert!(parsed.error.is_none(), "{:?}", parsed.error);
        let rows = parsed.result.expect("rows");
        let rows = rows.as_array().expect("array");
        assert_eq!(rows.len(), 2); // 103, 102
        assert_eq!(rows[0].get("slot").and_then(Value::as_u64), Some(103));

        // Limit straddles the coverage floor: ClickHouse must be consulted and
        // is unreachable here.
        let response = handle_get_signatures_for_address(
            state,
            json!(1),
            Some(vec![json!(address_str), json!({ "limit": 100 })]),
        )
        .await
        .expect("response");
        let parsed = parse_json_rpc_response(response).await;
        assert!(parsed.error.is_some(), "floor escape must hit ClickHouse");
    }

    #[tokio::test]
    async fn gsfa_deduplicates_head_and_disk_rows() {
        use solana_commitment_config::CommitmentLevel;
        use solana_sdk::signature::Signature as SdkSignature;

        let dir = tempfile::tempdir().expect("tempdir");
        let disk = open_disk_cache(&dir);
        let address = [51u8; 32];
        let signature = write_block_for_address(&disk, 100, 99, address, false);

        // The same transaction is still resident in the head cache.
        let head = empty_head_cache();
        let mut tx = transaction(100, 0);
        tx.tx_account_keys = vec![address];
        head.insert_for_tests(
            SdkSignature::from(tx.signature),
            tx,
            0,
            &[solana_sdk::pubkey::Pubkey::from(address)],
            CommitmentLevel::Finalized,
        );

        let state = state_with_head_and_disk(head, disk);
        let response = handle_get_signatures_for_address(
            state,
            json!(1),
            Some(vec![
                json!(bs58::encode(address).into_string()),
                json!({ "limit": 10, "untilSlot": 99 }),
            ]),
        )
        .await
        .expect("response");
        let parsed = parse_json_rpc_response(response).await;
        assert!(parsed.error.is_none(), "{:?}", parsed.error);
        let rows = parsed.result.expect("rows");
        let rows = rows.as_array().expect("array");
        assert_eq!(rows.len(), 1, "head+disk duplicate must collapse");
        assert_eq!(
            rows[0].get("signature").and_then(Value::as_str),
            Some(signature.as_str())
        );
    }

    #[tokio::test]
    async fn gtfa_desc_served_from_disk_with_filters() {
        let dir = tempfile::tempdir().expect("tempdir");
        let disk = open_disk_cache(&dir);
        let address = [52u8; 32];
        for slot in 100..=105u64 {
            // Odd slots fail.
            write_block_for_address(&disk, slot, slot - 1, address, slot % 2 == 1);
        }
        let state = state_with_disk_cache(disk);
        let address_str = bs58::encode(address).into_string();

        // slot.gte above the floor: fully covered, status filter on disk.
        let response = handle_get_transactions_for_address(
            state.clone(),
            json!(1),
            Some(vec![
                json!(address_str),
                json!({
                    "limit": 10,
                    "filters": { "slot": { "gte": 101 }, "status": "failed" }
                }),
            ]),
        )
        .await
        .expect("response");
        let parsed = parse_json_rpc_response(response).await;
        assert!(parsed.error.is_none(), "{:?}", parsed.error);
        let result = parsed.result.expect("result");
        let data = result.get("data").and_then(Value::as_array).expect("data");
        let slots: Vec<u64> = data
            .iter()
            .filter_map(|row| row.get("slot").and_then(Value::as_u64))
            .collect();
        assert_eq!(slots, vec![105, 103, 101]);
        // Disk-sourced pagination tokens are position-shaped.
        assert_eq!(
            result.get("paginationToken").and_then(Value::as_str),
            Some("101:0")
        );

        // Small limit fills from disk: no ClickHouse even without slot bounds.
        let response = handle_get_transactions_for_address(
            state.clone(),
            json!(1),
            Some(vec![json!(address_str), json!({ "limit": 2 })]),
        )
        .await
        .expect("response");
        let parsed = parse_json_rpc_response(response).await;
        assert!(parsed.error.is_none(), "{:?}", parsed.error);

        // Unbounded request larger than coverage: ClickHouse owes the
        // remainder and is unreachable.
        let response = handle_get_transactions_for_address(
            state,
            json!(1),
            Some(vec![json!(address_str), json!({ "limit": 100 })]),
        )
        .await
        .expect("response");
        let parsed = parse_json_rpc_response(response).await;
        assert!(parsed.error.is_some(), "floor escape must hit ClickHouse");
    }

    #[tokio::test]
    async fn gtfa_asc_pages_only_within_coverage() {
        let dir = tempfile::tempdir().expect("tempdir");
        let disk = open_disk_cache(&dir);
        let address = [53u8; 32];
        for slot in 100..=104u64 {
            write_block_for_address(&disk, slot, slot - 1, address, false);
        }
        let state = state_with_disk_cache(disk);
        let address_str = bs58::encode(address).into_string();

        // Ascending with a lower bound inside coverage: disk only.
        let response = handle_get_transactions_for_address(
            state.clone(),
            json!(1),
            Some(vec![
                json!(address_str),
                json!({
                    "limit": 10,
                    "sortOrder": "asc",
                    "filters": { "slot": { "gte": 101 } }
                }),
            ]),
        )
        .await
        .expect("response");
        let parsed = parse_json_rpc_response(response).await;
        assert!(parsed.error.is_none(), "{:?}", parsed.error);
        let result = parsed.result.expect("result");
        let slots: Vec<u64> = result
            .get("data")
            .and_then(Value::as_array)
            .expect("data")
            .iter()
            .filter_map(|row| row.get("slot").and_then(Value::as_u64))
            .collect();
        assert_eq!(slots, vec![101, 102, 103, 104]);

        // Ascending from below the floor: the oldest rows lead the page, so
        // ClickHouse must serve it (unreachable here).
        let response = handle_get_transactions_for_address(
            state,
            json!(1),
            Some(vec![
                json!(address_str),
                json!({ "limit": 10, "sortOrder": "asc" }),
            ]),
        )
        .await
        .expect("response");
        let parsed = parse_json_rpc_response(response).await;
        assert!(
            parsed.error.is_some(),
            "asc below floor must hit ClickHouse"
        );
    }

    #[tokio::test]
    async fn gtfa_token_accounts_filter_unions_owner_index() {
        let dir = tempfile::tempdir().expect("tempdir");
        let disk = open_disk_cache(&dir);
        let owner = [54u8; 32];

        // Slot 100: owner only via token-owner index, balance changed.
        let meta = block_metadata(100, 99, 1);
        let mut tx = transaction(100, 0);
        tx.tx_account_keys = vec![[1u8; 32], [2u8; 32]];
        tx.meta_post_token_account_index = vec![1];
        tx.meta_post_token_owner = vec![Some(owner)];
        tx.meta_post_token_amount = vec!["5".to_string()];
        disk.write_finalized_slot(
            &meta,
            &[Arc::new(tx)],
            crate::disk_cache::schema::COVERAGE_SOURCE_LIVE,
        )
        .expect("write");

        // Slot 101: owner only via token-owner index, balance unchanged.
        let meta = block_metadata(101, 100, 1);
        let mut tx = transaction(101, 0);
        tx.tx_account_keys = vec![[1u8; 32], [2u8; 32]];
        tx.meta_pre_token_account_index = vec![1];
        tx.meta_pre_token_owner = vec![Some(owner)];
        tx.meta_pre_token_amount = vec!["7".to_string()];
        tx.meta_post_token_account_index = vec![1];
        tx.meta_post_token_owner = vec![Some(owner)];
        tx.meta_post_token_amount = vec!["7".to_string()];
        disk.write_finalized_slot(
            &meta,
            &[Arc::new(tx)],
            crate::disk_cache::schema::COVERAGE_SOURCE_LIVE,
        )
        .expect("write");

        // Slot 102: owner directly in the account keys (gsfa side of the union).
        write_block_for_address(&disk, 102, 101, owner, false);

        let state = state_with_disk_cache(disk);
        let address_str = bs58::encode(owner).into_string();

        async fn fetch_slots(
            state: Arc<AppState>,
            address_str: &str,
            token_accounts: &str,
        ) -> Vec<u64> {
            let response = handle_get_transactions_for_address(
                state,
                json!(1),
                Some(vec![
                    json!(address_str),
                    json!({
                        "limit": 10,
                        "filters": { "slot": { "gte": 100 }, "tokenAccounts": token_accounts }
                    }),
                ]),
            )
            .await
            .expect("response");
            let parsed = parse_json_rpc_response(response).await;
            assert!(parsed.error.is_none(), "{:?}", parsed.error);
            parsed
                .result
                .expect("result")
                .get("data")
                .and_then(Value::as_array)
                .expect("data")
                .iter()
                .filter_map(|row| row.get("slot").and_then(Value::as_u64))
                .collect::<Vec<u64>>()
        }

        // All: both token rows plus the gsfa row.
        let slots = fetch_slots(state.clone(), &address_str, "all").await;
        assert_eq!(slots, vec![102, 101, 100]);

        // BalanceChanged: the unchanged-balance token row drops out; the gsfa
        // row stays (the union filters only the token side).
        let slots = fetch_slots(state.clone(), &address_str, "balanceChanged").await;
        assert_eq!(slots, vec![102, 100]);

        // None: only the gsfa row.
        let slots = fetch_slots(state, &address_str, "none").await;
        assert_eq!(slots, vec![102]);
    }

    #[tokio::test]
    async fn get_signature_statuses_unknown_signature_consults_clickhouse() {
        let dir = tempfile::tempdir().expect("tempdir");
        let disk = open_disk_cache(&dir);
        write_block(&disk, 100, 99, 1);
        let state = state_with_disk_cache(disk);

        let unknown = bs58::encode([7u8; 64]).into_string();
        let response = handle_get_signature_statuses(state, json!(1), Some(vec![json!([unknown])]))
            .await
            .expect("response");
        let parsed = parse_json_rpc_response(response).await;
        assert!(parsed.error.is_some(), "fallthrough should hit ClickHouse");
    }
}
