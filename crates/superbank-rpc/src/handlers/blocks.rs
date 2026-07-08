// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use axum::{http::StatusCode, response::Response};
use serde_json::{Value, json};
use solana_clock::{DEFAULT_SLOTS_PER_EPOCH, MAX_PROCESSING_AGE};
use solana_commitment_config::CommitmentConfig;
use solana_rpc_client_api::custom_error::JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE;
use solana_rpc_client_api::custom_error::JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED;
use solana_rpc_client_api::custom_error::JSON_RPC_SERVER_ERROR_MIN_CONTEXT_SLOT_NOT_REACHED;
use solana_rpc_client_api::custom_error::JSON_RPC_SERVER_ERROR_UNSUPPORTED_TRANSACTION_VERSION;
use solana_rpc_client_types::config::{RpcBlockConfig, RpcEncodingConfigWrapper};
use solana_sdk::hash::Hash;
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::{EncodeError, TransactionDetails, UiTransactionEncoding};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{error, warn};

use crate::clickhouse::{QueryTimings, StoredBlockPayload, StoredBlockRecord};
use crate::handlers::{
    RouteMetric,
    types::{
        GetBlockHeightConfig, GetBlocksConfig, GetInflationRewardConfig, GetLatestBlockhashConfig,
        GetLatestBlockhashResult, GetLatestBlockhashValue, GetSlotConfig, InflationRewardInfo,
        IsBlockhashValidResult, MAX_GET_BLOCKS_RANGE, RpcContextSlot, reject_unknown_fields,
    },
};
use crate::hydration::{BlockHydrationError, hydrate_block_payload};
use crate::metrics;
use crate::rpc::{
    json_rpc_error_response, json_rpc_internal_error_response, json_rpc_node_unhealthy_response,
    json_rpc_null_response, json_rpc_success_response,
};
use crate::state::{AppState, LatestSlotSource};
use crate::util::add_downstream_header;

const GET_BLOCK_ALLOWED_FIELDS: [&str; 5] = [
    "encoding",
    "transactionDetails",
    "rewards",
    "commitment",
    "maxSupportedTransactionVersion",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct GetBlockFetchPlan {
    encoding: UiTransactionEncoding,
    transaction_details: TransactionDetails,
    show_rewards: bool,
    max_supported_transaction_version: Option<u8>,
}

impl GetBlockFetchPlan {
    fn new(config: &RpcBlockConfig) -> Self {
        Self {
            encoding: config.encoding.unwrap_or(UiTransactionEncoding::Json),
            transaction_details: config.transaction_details.unwrap_or_default(),
            show_rewards: config.rewards.unwrap_or(true),
            max_supported_transaction_version: config.max_supported_transaction_version,
        }
    }

    fn needs_blocking_hydration(self) -> bool {
        matches!(
            self.transaction_details,
            TransactionDetails::Accounts | TransactionDetails::Full
        )
    }
}

fn block_payload_transaction_count(payload: &StoredBlockPayload) -> Option<usize> {
    payload.observed_transaction_count()
}

fn unsupported_transaction_version_message(version: u8) -> String {
    format!(
        "Transaction version ({version}) is not supported by the requesting client. Please try the request again with the following configuration parameter: \"maxSupportedTransactionVersion\": {version}"
    )
}

fn parse_get_blocks_config(config_value_opt: Option<Value>) -> Result<GetBlocksConfig, String> {
    if let Some(config_value) = config_value_opt {
        if config_value.is_null() {
            Ok(GetBlocksConfig::default())
        } else {
            match serde_json::from_value::<GetBlocksConfig>(config_value) {
                Ok(parsed) => Ok(parsed),
                Err(e) => Err(format!("Invalid params: failed to parse config ({e})")),
            }
        }
    } else {
        Ok(GetBlocksConfig::default())
    }
}

fn parse_get_inflation_reward_config(
    config_value_opt: Option<Value>,
) -> Result<GetInflationRewardConfig, String> {
    if let Some(config_value) = config_value_opt {
        if config_value.is_null() {
            Ok(GetInflationRewardConfig::default())
        } else if config_value.is_object() {
            serde_json::from_value::<GetInflationRewardConfig>(config_value)
                .map_err(|e| format!("Invalid params: failed to parse config ({e})"))
        } else {
            Err("Invalid params: config must be an object".to_string())
        }
    } else {
        Ok(GetInflationRewardConfig::default())
    }
}

fn reject_unsupported_blocks_commitment(
    state: &AppState,
    route: &mut RouteMetric,
    id: &Value,
    commitment: &CommitmentConfig,
) -> Option<Response> {
    #[cfg(not(feature = "grpc-head-cache"))]
    let _ = state;

    if commitment.is_processed() {
        #[cfg(feature = "grpc-head-cache")]
        {
            if state.head_cache.is_none() {
                route.invalid_params();
                return Some(json_rpc_error_response(
                    id.clone(),
                    -32602,
                    "Only confirmed or finalized commitments are supported",
                    Some(json!({ "requestedCommitment": commitment.commitment })),
                ));
            }
        }
        #[cfg(not(feature = "grpc-head-cache"))]
        {
            route.invalid_params();
            return Some(json_rpc_error_response(
                id.clone(),
                -32602,
                "Only confirmed or finalized commitments are supported",
                Some(json!({ "requestedCommitment": commitment.commitment })),
            ));
        }
    }

    None
}

fn merge_sorted_block_slots(clickhouse_slots: Vec<u64>, head_slots: Vec<u64>) -> Vec<u64> {
    if clickhouse_slots.is_empty() {
        return head_slots;
    }
    if head_slots.is_empty() {
        return clickhouse_slots;
    }

    let mut merged = Vec::with_capacity(clickhouse_slots.len() + head_slots.len());
    let mut clickhouse_idx = 0usize;
    let mut head_idx = 0usize;
    let mut last_slot: Option<u64> = None;

    while clickhouse_idx < clickhouse_slots.len() || head_idx < head_slots.len() {
        let next_slot = match (
            clickhouse_slots.get(clickhouse_idx),
            head_slots.get(head_idx),
        ) {
            (Some(&clickhouse_slot), Some(&head_slot)) => {
                if clickhouse_slot <= head_slot {
                    clickhouse_idx += 1;
                    clickhouse_slot
                } else {
                    head_idx += 1;
                    head_slot
                }
            }
            (Some(&clickhouse_slot), None) => {
                clickhouse_idx += 1;
                clickhouse_slot
            }
            (None, Some(&head_slot)) => {
                head_idx += 1;
                head_slot
            }
            (None, None) => break,
        };

        if last_slot != Some(next_slot) {
            merged.push(next_slot);
            last_slot = Some(next_slot);
        }
    }

    merged
}

// `getBlock` is served from ClickHouse, so classify misses against ClickHouse ingest state.
fn classify_get_block_miss(slot: u64, clickhouse_latest: u64) -> (i32, String) {
    if slot > clickhouse_latest {
        (
            JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE as i32,
            format!("Block not available for slot {slot}"),
        )
    } else {
        (
            JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED as i32,
            format!("Slot {slot} was skipped, or missing in long-term storage"),
        )
    }
}

async fn get_block_slots_response_for_range(
    state: &Arc<AppState>,
    route: &mut RouteMetric,
    id: Value,
    start_slot: u64,
    end_slot: u64,
    commitment: CommitmentConfig,
    mut timings: QueryTimings,
) -> Result<Response, StatusCode> {
    #[cfg(not(feature = "grpc-head-cache"))]
    let _ = commitment;

    // Disk tier: the contiguous covered span answers its part of the range, so
    // ClickHouse is only consulted below the coverage floor and above the
    // covered head (the latter matters when disk writes lag the chain tip).
    #[cfg(feature = "disk-cache")]
    let (disk_slots, disk_span): (Vec<u64>, Option<(u64, u64)>) =
        if let Some(disk) = state.disk_cache.as_ref() {
            route.disk_cache_read();
            match disk.tip_span() {
                Some((floor, head)) if end_slot >= floor && start_slot <= head => {
                    match disk
                        .covered_slots_in_range(start_slot.max(floor), end_slot.min(head))
                        .await
                    {
                        Some(slots) => (slots, Some((floor, head))),
                        None => (Vec::new(), None),
                    }
                }
                _ => (Vec::new(), None),
            }
        } else {
            (Vec::new(), None)
        };
    #[cfg(not(feature = "disk-cache"))]
    let (disk_slots, disk_span): (Vec<u64>, Option<(u64, u64)>) = (Vec::new(), None);

    let disk_contributed = disk_span.is_some();
    let mut clickhouse_ranges: Vec<(u64, u64)> = Vec::new();
    match disk_span {
        Some((floor, head)) => {
            if start_slot < floor {
                clickhouse_ranges.push((start_slot, end_slot.min(floor.saturating_sub(1))));
            }
            if end_slot > head {
                clickhouse_ranges.push((start_slot.max(head + 1), end_slot));
            }
        }
        None => clickhouse_ranges.push((start_slot, end_slot)),
    }

    let mut clickhouse_slots = Vec::new();
    let mut clickhouse_read = false;
    let mut clickhouse_error: Option<String> = None;

    for (range_start, range_end) in clickhouse_ranges {
        match state
            .clickhouse
            .get_block_slots_by_range(range_start, range_end)
            .await
        {
            Ok((slots, query_timings)) => {
                clickhouse_slots = merge_sorted_block_slots(clickhouse_slots, slots);
                clickhouse_read = true;
                timings.add(query_timings);
            }
            Err(e) => {
                metrics::backend_error("get_block_slots_by_range");
                clickhouse_error = Some(e.to_string());
            }
        }
    }

    #[cfg(feature = "grpc-head-cache")]
    let head_slots = if let Some(cache) = state.head_cache.as_ref() {
        route.head_cache_read();
        cache.slots_in_range_at_least(start_slot, end_slot, commitment.commitment)
    } else {
        Vec::new()
    };

    #[cfg(not(feature = "grpc-head-cache"))]
    let head_slots: Vec<u64> = Vec::new();

    let head_contributed = !head_slots.is_empty();

    if let Some(err) = clickhouse_error {
        if !head_contributed && !disk_contributed {
            error!(
                "Failed to query ClickHouse for block slots {}..={}: {}",
                start_slot, end_slot, err
            );
            return Ok(json_rpc_internal_error_response(id));
        }

        warn!(
            "Serving cache-only slots for range {}..={} after ClickHouse error: {}",
            start_slot, end_slot, err
        );
    }

    if clickhouse_read {
        route.source_clickhouse();
    } else if disk_contributed {
        #[cfg(feature = "disk-cache")]
        route.source_disk_cache();
    } else if head_contributed {
        #[cfg(feature = "grpc-head-cache")]
        route.source_head_cache();
    } else {
        route.source_none();
    }

    let slots = merge_sorted_block_slots(
        merge_sorted_block_slots(clickhouse_slots, disk_slots),
        head_slots,
    );
    metrics::blocks_slots_returned(route.method(), slots.len());
    route.success();
    let mut resp = json_rpc_success_response(id, slots);
    add_downstream_header(&mut resp, &timings);
    Ok(resp)
}

pub(crate) async fn handle_get_block_height(
    state: Arc<AppState>,
    id: Value,
    params: Option<Vec<Value>>,
) -> Result<Response, StatusCode> {
    let mut route = RouteMetric::for_state("getBlockHeight", state.as_ref());

    let config = match params.filter(|v| !v.is_empty()) {
        None => GetBlockHeightConfig::default(),
        Some(mut params) => {
            if params.len() != 1 {
                route.invalid_params();
                return Ok(json_rpc_error_response(
                    id,
                    -32602,
                    "Invalid params: expected a single config object",
                    None,
                ));
            }

            let value = params.remove(0);
            if value.is_null() {
                GetBlockHeightConfig::default()
            } else if value.is_object() {
                match serde_json::from_value::<GetBlockHeightConfig>(value) {
                    Ok(config) => config,
                    Err(e) => {
                        route.invalid_params();
                        return Ok(json_rpc_error_response(
                            id,
                            -32602,
                            format!("Invalid params: failed to parse config ({e})"),
                            None,
                        ));
                    }
                }
            } else {
                route.invalid_params();
                return Ok(json_rpc_error_response(
                    id,
                    -32602,
                    "Invalid params: config must be an object",
                    None,
                ));
            }
        }
    };

    let commitment = config.commitment.unwrap_or_default();

    if commitment.is_processed() {
        #[cfg(feature = "grpc-head-cache")]
        {
            if state.head_cache.is_none() {
                route.invalid_params();
                return Ok(json_rpc_error_response(
                    id,
                    -32602,
                    "Only confirmed or finalized commitments are supported",
                    Some(json!({ "requestedCommitment": commitment.commitment })),
                ));
            }
        }
        #[cfg(not(feature = "grpc-head-cache"))]
        {
            route.invalid_params();
            return Ok(json_rpc_error_response(
                id,
                -32602,
                "Only confirmed or finalized commitments are supported",
                Some(json!({ "requestedCommitment": commitment.commitment })),
            ));
        }
    }

    let mut clickhouse_context_slot_opt = None;
    if let Some(min_context_slot) = config.min_context_slot {
        let (context_slot, context_source) = match state
            .resolve_latest_slot_with_source("get_block_height_min_context", commitment.commitment)
            .await
        {
            Ok(result) => result,
            Err(e) => {
                metrics::backend_error("get_latest_finalized_slot");
                error!(
                    "Failed to fetch latest slot for minContextSlot check: {}",
                    e
                );
                return Ok(json_rpc_internal_error_response(id));
            }
        };

        match context_source {
            LatestSlotSource::ClickHouse => {
                clickhouse_context_slot_opt = Some(context_slot);
                route.source_clickhouse();
            }
            #[cfg(feature = "grpc-head-cache")]
            LatestSlotSource::HeadCache => route.source_head_cache(),
        }

        if context_slot < min_context_slot {
            route.rpc_error();
            return Ok(json_rpc_error_response(
                id,
                JSON_RPC_SERVER_ERROR_MIN_CONTEXT_SLOT_NOT_REACHED as i32,
                "Minimum context slot has not been reached",
                Some(json!({ "contextSlot": context_slot })),
            ));
        }
    }

    let head_height_opt: Option<u64> = {
        #[cfg(feature = "grpc-head-cache")]
        {
            if let Some(cache) = state.head_cache.as_ref() {
                route.head_cache_read();
                cache.latest_block_height_at_least(commitment.commitment)
            } else {
                None
            }
        }
        #[cfg(not(feature = "grpc-head-cache"))]
        {
            None
        }
    };

    if let Some(head_height) = head_height_opt {
        #[cfg(feature = "grpc-head-cache")]
        route.source_head_cache();
        route.success();
        return Ok(json_rpc_success_response(id, json!(head_height)));
    }

    let clickhouse_slot = if let Some(slot) = clickhouse_context_slot_opt {
        slot
    } else {
        match state
            .latest_slot_cache
            .get_or_refresh(&state.clickhouse)
            .await
        {
            Ok(slot) => slot,
            Err(e) => {
                metrics::backend_error("get_latest_finalized_slot");
                error!(
                    "Failed to fetch latest slot for getBlockHeight ClickHouse fallback: {}",
                    e
                );
                return Ok(json_rpc_internal_error_response(id));
            }
        }
    };

    let clickhouse_height_opt = match state
        .latest_block_height_cache
        .get_or_refresh(clickhouse_slot, &state.clickhouse)
        .await
    {
        Ok(height_opt) => height_opt,
        Err(e) => {
            metrics::backend_error("get_block_height_by_slot");
            error!(
                "Failed to query ClickHouse block height at slot {}: {}",
                clickhouse_slot, e
            );
            return Ok(json_rpc_internal_error_response(id));
        }
    };
    route.source_clickhouse();

    let Some(height) = clickhouse_height_opt else {
        route.source_none();
        error!(slot = clickhouse_slot, "Block height unavailable");
        return Ok(json_rpc_internal_error_response(id));
    };

    route.success();
    Ok(json_rpc_success_response(id, json!(height)))
}

pub(crate) async fn handle_get_slot(
    state: Arc<AppState>,
    id: Value,
    params: Option<Vec<Value>>,
) -> Result<Response, StatusCode> {
    let mut route = RouteMetric::for_state("getSlot", state.as_ref());

    let config = match params.filter(|v| !v.is_empty()) {
        None => GetSlotConfig::default(),
        Some(mut params) => {
            if params.len() != 1 {
                route.invalid_params();
                return Ok(json_rpc_error_response(
                    id,
                    -32602,
                    "Invalid params: expected a single config object",
                    None,
                ));
            }

            let value = params.remove(0);
            if value.is_null() {
                GetSlotConfig::default()
            } else if value.is_object() {
                match serde_json::from_value::<GetSlotConfig>(value) {
                    Ok(config) => config,
                    Err(e) => {
                        route.invalid_params();
                        return Ok(json_rpc_error_response(
                            id,
                            -32602,
                            format!("Invalid params: failed to parse config ({e})"),
                            None,
                        ));
                    }
                }
            } else {
                route.invalid_params();
                return Ok(json_rpc_error_response(
                    id,
                    -32602,
                    "Invalid params: config must be an object",
                    None,
                ));
            }
        }
    };

    let commitment = config.commitment.unwrap_or_default();

    if commitment.is_processed() {
        #[cfg(feature = "grpc-head-cache")]
        {
            if state.head_cache.is_none() {
                route.invalid_params();
                return Ok(json_rpc_error_response(
                    id,
                    -32602,
                    "Only confirmed or finalized commitments are supported",
                    Some(json!({ "requestedCommitment": commitment.commitment })),
                ));
            }
        }
        #[cfg(not(feature = "grpc-head-cache"))]
        {
            route.invalid_params();
            return Ok(json_rpc_error_response(
                id,
                -32602,
                "Only confirmed or finalized commitments are supported",
                Some(json!({ "requestedCommitment": commitment.commitment })),
            ));
        }
    }

    let (slot, slot_source) = match state
        .resolve_latest_slot_with_source("get_slot", commitment.commitment)
        .await
    {
        Ok(result) => result,
        Err(e) => {
            metrics::backend_error("get_latest_finalized_slot");
            error!("Failed to fetch latest slot for getSlot: {}", e);
            return Ok(json_rpc_internal_error_response(id));
        }
    };
    match slot_source {
        LatestSlotSource::ClickHouse => route.source_clickhouse(),
        #[cfg(feature = "grpc-head-cache")]
        LatestSlotSource::HeadCache => route.source_head_cache(),
    }

    if let Some(min_context_slot) = config.min_context_slot
        && slot < min_context_slot
    {
        route.rpc_error();
        return Ok(json_rpc_error_response(
            id,
            JSON_RPC_SERVER_ERROR_MIN_CONTEXT_SLOT_NOT_REACHED as i32,
            "Minimum context slot has not been reached",
            Some(json!({ "contextSlot": slot })),
        ));
    }

    route.success();
    Ok(json_rpc_success_response(id, json!(slot)))
}

enum TransactionCountPlan {
    ClickHouseThroughSlot {
        context_slot: u64,
    },
    #[cfg(feature = "grpc-head-cache")]
    ClickHouseBeforeSlotPlusHead {
        context_slot: u64,
        clickhouse_before_slot: u64,
        head_transaction_count: u64,
    },
}

impl TransactionCountPlan {
    fn context_slot(&self) -> u64 {
        match self {
            Self::ClickHouseThroughSlot { context_slot } => *context_slot,
            #[cfg(feature = "grpc-head-cache")]
            Self::ClickHouseBeforeSlotPlusHead { context_slot, .. } => *context_slot,
        }
    }
}

pub(crate) async fn handle_get_transaction_count(
    state: Arc<AppState>,
    id: Value,
    params: Option<Vec<Value>>,
) -> Result<Response, StatusCode> {
    let mut route = RouteMetric::for_state("getTransactionCount", state.as_ref());

    let config = match params.filter(|v| !v.is_empty()) {
        None => GetSlotConfig::default(),
        Some(mut params) => {
            if params.len() != 1 {
                route.invalid_params();
                return Ok(json_rpc_error_response(
                    id,
                    -32602,
                    "Invalid params: expected a single config object",
                    None,
                ));
            }

            let value = params.remove(0);
            if value.is_null() {
                GetSlotConfig::default()
            } else if value.is_object() {
                match serde_json::from_value::<GetSlotConfig>(value) {
                    Ok(config) => config,
                    Err(e) => {
                        route.invalid_params();
                        return Ok(json_rpc_error_response(
                            id,
                            -32602,
                            format!("Invalid params: failed to parse config ({e})"),
                            None,
                        ));
                    }
                }
            } else {
                route.invalid_params();
                return Ok(json_rpc_error_response(
                    id,
                    -32602,
                    "Invalid params: config must be an object",
                    None,
                ));
            }
        }
    };

    let commitment = config.commitment.unwrap_or_default();
    if commitment.is_processed() {
        #[cfg(feature = "grpc-head-cache")]
        {
            if state.head_cache.is_none() {
                route.invalid_params();
                return Ok(json_rpc_error_response(
                    id,
                    -32602,
                    "Only confirmed or finalized commitments are supported",
                    Some(json!({ "requestedCommitment": commitment.commitment })),
                ));
            }
        }
        #[cfg(not(feature = "grpc-head-cache"))]
        {
            route.invalid_params();
            return Ok(json_rpc_error_response(
                id,
                -32602,
                "Only confirmed or finalized commitments are supported",
                Some(json!({ "requestedCommitment": commitment.commitment })),
            ));
        }
    }

    let clickhouse_slot = match state
        .latest_slot_cache
        .get_or_refresh(&state.clickhouse)
        .await
    {
        Ok(slot) => slot,
        Err(e) => {
            metrics::backend_error("get_latest_finalized_slot");
            error!("Failed to fetch latest slot for getTransactionCount: {}", e);
            return Ok(json_rpc_internal_error_response(id));
        }
    };

    #[cfg(feature = "grpc-head-cache")]
    let count_plan = {
        let mut count_plan = TransactionCountPlan::ClickHouseThroughSlot {
            context_slot: clickhouse_slot,
        };
        if let Some(cache) = state.head_cache.as_ref()
            && let Some(overlay) =
                cache.transaction_count_overlay_at_least(commitment.commitment, clickhouse_slot)
            && overlay.context_slot > clickhouse_slot
        {
            count_plan = TransactionCountPlan::ClickHouseBeforeSlotPlusHead {
                context_slot: overlay.context_slot,
                clickhouse_before_slot: overlay.start_slot,
                head_transaction_count: overlay.transaction_count,
            };
        }
        count_plan
    };
    #[cfg(not(feature = "grpc-head-cache"))]
    let count_plan = TransactionCountPlan::ClickHouseThroughSlot {
        context_slot: clickhouse_slot,
    };

    let context_slot = count_plan.context_slot();
    match &count_plan {
        TransactionCountPlan::ClickHouseThroughSlot { .. } => route.source_clickhouse(),
        #[cfg(feature = "grpc-head-cache")]
        TransactionCountPlan::ClickHouseBeforeSlotPlusHead { .. } => {
            route.source_head_cache();
        }
    }

    if let Some(min_context_slot) = config.min_context_slot
        && context_slot < min_context_slot
    {
        route.rpc_error();
        return Ok(json_rpc_error_response(
            id,
            JSON_RPC_SERVER_ERROR_MIN_CONTEXT_SLOT_NOT_REACHED as i32,
            "Minimum context slot has not been reached",
            Some(json!({ "contextSlot": context_slot })),
        ));
    }

    let (transaction_count, timings) = match count_plan {
        TransactionCountPlan::ClickHouseThroughSlot { context_slot } => {
            match state
                .clickhouse
                .get_transaction_count_by_slot(context_slot)
                .await
            {
                Ok(result) => result,
                Err(e) => {
                    metrics::backend_error("get_transaction_count_by_slot");
                    error!(
                        "Failed to query ClickHouse transaction count at slot {}: {}",
                        context_slot, e
                    );
                    return Ok(json_rpc_internal_error_response(id));
                }
            }
        }
        #[cfg(feature = "grpc-head-cache")]
        TransactionCountPlan::ClickHouseBeforeSlotPlusHead {
            context_slot,
            clickhouse_before_slot,
            head_transaction_count,
        } => match state
            .clickhouse
            .get_transaction_count_before_slot(clickhouse_before_slot)
            .await
        {
            Ok((base_transaction_count, timings)) => (
                base_transaction_count.saturating_add(head_transaction_count),
                timings,
            ),
            Err(e) => {
                metrics::backend_error("get_transaction_count_before_slot");
                error!(
                    "Failed to query ClickHouse transaction count before slot {} for context slot {}: {}",
                    clickhouse_before_slot, context_slot, e
                );
                return Ok(json_rpc_internal_error_response(id));
            }
        },
    };

    route.success();
    let mut resp = json_rpc_success_response(id, json!(transaction_count));
    add_downstream_header(&mut resp, &timings);
    Ok(resp)
}

pub(crate) async fn handle_get_latest_blockhash(
    state: Arc<AppState>,
    id: Value,
    params: Option<Vec<Value>>,
) -> Result<Response, StatusCode> {
    let mut route = RouteMetric::for_state("getLatestBlockhash", state.as_ref());

    let config = match params.filter(|v| !v.is_empty()) {
        None => GetLatestBlockhashConfig::default(),
        Some(mut params) => {
            if params.len() != 1 {
                route.invalid_params();
                return Ok(json_rpc_error_response(
                    id,
                    -32602,
                    "Invalid params: expected a single config object",
                    None,
                ));
            }

            let value = params.remove(0);
            if value.is_null() {
                GetLatestBlockhashConfig::default()
            } else if value.is_object() {
                match serde_json::from_value::<GetLatestBlockhashConfig>(value) {
                    Ok(config) => config,
                    Err(e) => {
                        warn!(error = %e, "Invalid getLatestBlockhash config");
                        route.invalid_params();
                        return Ok(json_rpc_error_response(
                            id,
                            -32602,
                            "Invalid params: failed to parse config".to_string(),
                            None,
                        ));
                    }
                }
            } else {
                route.invalid_params();
                return Ok(json_rpc_error_response(
                    id,
                    -32602,
                    "Invalid params: config must be an object",
                    None,
                ));
            }
        }
    };

    let commitment = config.commitment.unwrap_or_default();

    if commitment.is_processed() {
        #[cfg(feature = "grpc-head-cache")]
        {
            if state.head_cache.is_none() {
                route.invalid_params();
                return Ok(json_rpc_error_response(
                    id,
                    -32602,
                    "Only confirmed or finalized commitments are supported",
                    Some(json!({ "requestedCommitment": commitment.commitment })),
                ));
            }
        }
        #[cfg(not(feature = "grpc-head-cache"))]
        {
            route.invalid_params();
            return Ok(json_rpc_error_response(
                id,
                -32602,
                "Only confirmed or finalized commitments are supported",
                Some(json!({ "requestedCommitment": commitment.commitment })),
            ));
        }
    }

    let head_candidate = {
        #[cfg(feature = "grpc-head-cache")]
        {
            if let Some(cache) = state.head_cache.as_ref() {
                route.head_cache_read();
                cache.latest_blockhash_info_at_least(commitment.commitment)
            } else {
                None
            }
        }
        #[cfg(not(feature = "grpc-head-cache"))]
        {
            None
        }
    };

    #[cfg(feature = "grpc-head-cache")]
    let head_context_source = LatestSlotSource::HeadCache;
    #[cfg(not(feature = "grpc-head-cache"))]
    let head_context_source = LatestSlotSource::ClickHouse;

    let (context_slot, use_head, context_source) = if let Some((head_slot, _, _)) = head_candidate {
        (head_slot, true, head_context_source)
    } else {
        let (slot, source) = match state
            .resolve_latest_slot_with_source("get_latest_blockhash", commitment.commitment)
            .await
        {
            Ok(result) => result,
            Err(e) => {
                metrics::backend_error("get_latest_finalized_slot");
                error!("Failed to fetch latest slot for getLatestBlockhash: {}", e);
                return Ok(json_rpc_internal_error_response(id));
            }
        };
        (slot, false, source)
    };
    if use_head {
        #[cfg(feature = "grpc-head-cache")]
        route.source_head_cache();
    } else {
        match context_source {
            LatestSlotSource::ClickHouse => route.source_clickhouse(),
            #[cfg(feature = "grpc-head-cache")]
            LatestSlotSource::HeadCache => route.source_head_cache(),
        }
    }

    if let Some(min_context_slot) = config.min_context_slot
        && context_slot < min_context_slot
    {
        route.rpc_error();
        return Ok(json_rpc_error_response(
            id,
            JSON_RPC_SERVER_ERROR_MIN_CONTEXT_SLOT_NOT_REACHED as i32,
            "Minimum context slot has not been reached",
            Some(json!({ "contextSlot": context_slot })),
        ));
    }

    let (blockhash_bytes, block_height, timings_opt): ([u8; 32], u64, Option<QueryTimings>) =
        if use_head {
            let (_slot, blockhash, block_height) =
                head_candidate.expect("use_head implies head_candidate");
            (blockhash, block_height, None)
        } else {
            route.source_clickhouse();
            let (row_opt, timings) = match state
                .clickhouse
                .get_blockhash_height_by_slot(context_slot)
                .await
            {
                Ok(result) => result,
                Err(e) => {
                    metrics::backend_error("get_blockhash_height_by_slot");
                    error!(
                        "Failed to query ClickHouse for blockhash/height at slot {}: {}",
                        context_slot, e
                    );
                    return Ok(json_rpc_internal_error_response(id));
                }
            };

            let Some((blockhash, block_height_opt)) = row_opt else {
                error!(slot = context_slot, "Latest blockhash unavailable");
                let mut resp = json_rpc_internal_error_response(id);
                add_downstream_header(&mut resp, &timings);
                return Ok(resp);
            };

            let Some(block_height) = block_height_opt else {
                error!(slot = context_slot, "Latest block height unavailable");
                let mut resp = json_rpc_internal_error_response(id);
                add_downstream_header(&mut resp, &timings);
                return Ok(resp);
            };

            (blockhash, block_height, Some(timings))
        };

    let blockhash = Hash::from(blockhash_bytes).to_string();
    let last_valid_block_height = block_height.saturating_add(MAX_PROCESSING_AGE as u64);

    let result = GetLatestBlockhashResult {
        context: RpcContextSlot { slot: context_slot },
        value: GetLatestBlockhashValue {
            blockhash,
            last_valid_block_height,
        },
    };

    let mut resp = json_rpc_success_response(id, result);
    if let Some(timings) = timings_opt {
        add_downstream_header(&mut resp, &timings);
    }
    route.success();
    Ok(resp)
}

pub(crate) async fn handle_is_blockhash_valid(
    state: Arc<AppState>,
    id: Value,
    params: Option<Vec<Value>>,
) -> Result<Response, StatusCode> {
    let mut route = RouteMetric::for_state("isBlockhashValid", state.as_ref());

    let Some(params) = params.filter(|v| !v.is_empty()) else {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            "Invalid params: expected a blockhash string",
            None,
        ));
    };
    let Some(blockhash_str) = params.first().and_then(Value::as_str) else {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            "Invalid params: blockhash must be a base-58 encoded string",
            None,
        ));
    };
    let Ok(blockhash) = Hash::from_str(blockhash_str) else {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            "Invalid params: invalid blockhash",
            None,
        ));
    };
    let blockhash_bytes = blockhash.to_bytes();

    let config = match params.get(1) {
        None => GetLatestBlockhashConfig::default(),
        Some(v) if v.is_null() => GetLatestBlockhashConfig::default(),
        Some(v) if v.is_object() => {
            match serde_json::from_value::<GetLatestBlockhashConfig>(v.clone()) {
                Ok(config) => config,
                Err(e) => {
                    warn!(error = %e, "Invalid isBlockhashValid config");
                    route.invalid_params();
                    return Ok(json_rpc_error_response(
                        id,
                        -32602,
                        "Invalid params: failed to parse config".to_string(),
                        None,
                    ));
                }
            }
        }
        Some(_) => {
            route.invalid_params();
            return Ok(json_rpc_error_response(
                id,
                -32602,
                "Invalid params: config must be an object",
                None,
            ));
        }
    };
    let commitment = config.commitment.unwrap_or_default();

    if commitment.is_processed() {
        #[cfg(feature = "grpc-head-cache")]
        {
            if state.head_cache.is_none() {
                route.invalid_params();
                return Ok(json_rpc_error_response(
                    id,
                    -32602,
                    "Only confirmed or finalized commitments are supported",
                    Some(json!({ "requestedCommitment": commitment.commitment })),
                ));
            }
        }
        #[cfg(not(feature = "grpc-head-cache"))]
        {
            route.invalid_params();
            return Ok(json_rpc_error_response(
                id,
                -32602,
                "Only confirmed or finalized commitments are supported",
                Some(json!({ "requestedCommitment": commitment.commitment })),
            ));
        }
    }

    let head_candidate = {
        #[cfg(feature = "grpc-head-cache")]
        {
            if let Some(cache) = state.head_cache.as_ref() {
                route.head_cache_read();
                cache.latest_blockhash_info_at_least(commitment.commitment)
            } else {
                None
            }
        }
        #[cfg(not(feature = "grpc-head-cache"))]
        {
            None
        }
    };
    #[cfg(feature = "grpc-head-cache")]
    let head_context_source = LatestSlotSource::HeadCache;
    #[cfg(not(feature = "grpc-head-cache"))]
    let head_context_source = LatestSlotSource::ClickHouse;

    let (context_slot, use_head, context_source) = if let Some((head_slot, _, _)) = head_candidate {
        (head_slot, true, head_context_source)
    } else {
        let (slot, source) = match state
            .resolve_latest_slot_with_source("is_blockhash_valid", commitment.commitment)
            .await
        {
            Ok(result) => result,
            Err(e) => {
                metrics::backend_error("get_latest_finalized_slot");
                error!("Failed to fetch latest slot for isBlockhashValid: {}", e);
                return Ok(json_rpc_internal_error_response(id));
            }
        };
        (slot, false, source)
    };
    if use_head {
        #[cfg(feature = "grpc-head-cache")]
        route.source_head_cache();
    } else {
        match context_source {
            LatestSlotSource::ClickHouse => route.source_clickhouse(),
            #[cfg(feature = "grpc-head-cache")]
            LatestSlotSource::HeadCache => route.source_head_cache(),
        }
    }

    if let Some(min_context_slot) = config.min_context_slot
        && context_slot < min_context_slot
    {
        route.rpc_error();
        return Ok(json_rpc_error_response(
            id,
            JSON_RPC_SERVER_ERROR_MIN_CONTEXT_SLOT_NOT_REACHED as i32,
            "Minimum context slot has not been reached",
            Some(json!({ "contextSlot": context_slot })),
        ));
    }

    let current_block_height = if use_head {
        let (_slot, _blockhash, block_height): (u64, [u8; 32], u64) =
            head_candidate.expect("use_head implies head_candidate");
        block_height
    } else {
        route.source_clickhouse();
        let (row_opt, timings) = match state
            .clickhouse
            .get_blockhash_height_by_slot(context_slot)
            .await
        {
            Ok(result) => result,
            Err(e) => {
                metrics::backend_error("get_blockhash_height_by_slot");
                error!(
                    "Failed to query ClickHouse for block height at slot {}: {}",
                    context_slot, e
                );
                return Ok(json_rpc_internal_error_response(id));
            }
        };
        let Some((_blockhash, height_opt)) = row_opt else {
            error!(
                slot = context_slot,
                "Block height unavailable for isBlockhashValid"
            );
            let mut resp = json_rpc_internal_error_response(id);
            add_downstream_header(&mut resp, &timings);
            return Ok(resp);
        };
        let Some(height) = height_opt else {
            error!(
                slot = context_slot,
                "Block height unavailable for isBlockhashValid"
            );
            let mut resp = json_rpc_internal_error_response(id);
            add_downstream_header(&mut resp, &timings);
            return Ok(resp);
        };
        height
    };

    let min_block_height = current_block_height.saturating_sub(MAX_PROCESSING_AGE as u64);

    #[cfg(feature = "grpc-head-cache")]
    if let Some(cache) = state.head_cache.as_ref() {
        route.head_cache_read();
        if let Some(valid) =
            cache.blockhash_valid_at_least(blockhash_bytes, min_block_height, commitment.commitment)
        {
            route.source_head_cache();
            route.success();
            let result = IsBlockhashValidResult {
                context: RpcContextSlot { slot: context_slot },
                value: valid,
            };
            return Ok(json_rpc_success_response(id, json!(result)));
        }
    }

    route.source_clickhouse();
    let (value, timings) = match state
        .clickhouse
        .is_blockhash_valid_in_window(&blockhash_bytes, context_slot, min_block_height)
        .await
    {
        Ok(result) => result,
        Err(e) => {
            metrics::backend_error("is_blockhash_valid");
            error!("Failed to query ClickHouse for isBlockhashValid: {}", e);
            return Ok(json_rpc_internal_error_response(id));
        }
    };

    route.success();
    let result = IsBlockhashValidResult {
        context: RpcContextSlot { slot: context_slot },
        value,
    };
    let mut resp = json_rpc_success_response(id, json!(result));
    add_downstream_header(&mut resp, &timings);
    Ok(resp)
}

/// Hydrate a block payload and build the JSON-RPC response; shared by the
/// head-cache, disk-cache, and ClickHouse branches of getBlock.
async fn respond_with_hydrated_block(
    state: &AppState,
    id: Value,
    route: &mut RouteMetric,
    slot: u64,
    payload: StoredBlockPayload,
    fetch_plan: GetBlockFetchPlan,
    timings: Option<QueryTimings>,
) -> Result<Response, StatusCode> {
    if payload.metadata().slot != slot {
        warn!(
            requested = slot,
            observed = payload.metadata().slot,
            "Block metadata slot mismatch"
        );
    }
    if let Some(observed) = block_payload_transaction_count(&payload)
        && payload.metadata().executed_transaction_count != observed as u64
    {
        warn!(
            slot,
            expected = payload.metadata().executed_transaction_count,
            observed = observed,
            entry_count = payload.metadata().entry_count,
            "Block transaction count mismatch"
        );
    }

    let attach_timings = |resp: &mut Response| {
        if let Some(timings) = timings.as_ref() {
            add_downstream_header(resp, timings);
        }
    };

    let hydrated = if fetch_plan.needs_blocking_hydration() {
        let permit = match state.hydration_sem.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => {
                error!(slot, "Hydration semaphore closed");
                let mut resp = json_rpc_internal_error_response(id);
                attach_timings(&mut resp);
                return Ok(resp);
            }
        };
        match tokio::task::spawn_blocking(move || {
            let _permit = permit;
            hydrate_block_payload(
                payload,
                fetch_plan.encoding,
                fetch_plan.transaction_details,
                fetch_plan.show_rewards,
                fetch_plan.max_supported_transaction_version,
            )
        })
        .await
        {
            Ok(result) => result,
            Err(join_err) => {
                error!(
                    "Failed to join hydration task for block {}: {}",
                    slot, join_err
                );
                let mut resp = json_rpc_internal_error_response(id);
                attach_timings(&mut resp);
                return Ok(resp);
            }
        }
    } else {
        hydrate_block_payload(
            payload,
            fetch_plan.encoding,
            fetch_plan.transaction_details,
            fetch_plan.show_rewards,
            fetch_plan.max_supported_transaction_version,
        )
    };

    match hydrated {
        Ok(encoded_block) => {
            route.success();
            let mut resp = json_rpc_success_response(id, encoded_block);
            attach_timings(&mut resp);
            Ok(resp)
        }
        Err(BlockHydrationError::Encode(EncodeError::UnsupportedTransactionVersion(version))) => {
            route.rpc_error();
            let code = JSON_RPC_SERVER_ERROR_UNSUPPORTED_TRANSACTION_VERSION as i32;
            let mut resp = json_rpc_error_response(
                id,
                code,
                unsupported_transaction_version_message(version),
                None,
            );
            attach_timings(&mut resp);
            Ok(resp)
        }
        Err(e) => {
            error!("Failed to hydrate block {}: {}", slot, e);
            let mut resp = json_rpc_internal_error_response(id);
            attach_timings(&mut resp);
            Ok(resp)
        }
    }
}

pub(crate) async fn handle_get_block(
    state: Arc<AppState>,
    id: Value,
    params: Option<Vec<Value>>,
) -> Result<Response, StatusCode> {
    let mut route = RouteMetric::for_state("getBlock", state.as_ref());

    let Some(mut params) = params.filter(|v| !v.is_empty()) else {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            "Invalid params: missing slot",
            None,
        ));
    };

    let slot_value = params.remove(0);
    let slot = match slot_value.as_u64() {
        Some(slot) => slot,
        None => {
            route.invalid_params();
            return Ok(json_rpc_error_response(
                id,
                -32602,
                "Invalid params: slot must be a number",
                None,
            ));
        }
    };

    let config_wrapper = match params.into_iter().next() {
        Some(config_value) => {
            if let Err(message) = reject_unknown_fields(&config_value, &GET_BLOCK_ALLOWED_FIELDS) {
                route.invalid_params();
                return Ok(json_rpc_error_response(id, -32602, message, None));
            }
            if config_value.is_null() {
                RpcEncodingConfigWrapper::Current(Some(RpcBlockConfig::default()))
            } else {
                match serde_json::from_value::<RpcEncodingConfigWrapper<RpcBlockConfig>>(
                    config_value,
                ) {
                    Ok(wrapper) => wrapper,
                    Err(e) => {
                        route.invalid_params();
                        return Ok(json_rpc_error_response(
                            id,
                            -32602,
                            format!("Invalid params: failed to parse config ({e})"),
                            None,
                        ));
                    }
                }
            }
        }
        None => RpcEncodingConfigWrapper::Current(Some(RpcBlockConfig::default())),
    };

    let config = config_wrapper.convert_to_current();
    let commitment = config.commitment.unwrap_or_default();
    let fetch_plan = GetBlockFetchPlan::new(&config);

    if commitment.is_processed() {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            "Only confirmed or finalized commitments are supported",
            Some(json!({ "requestedCommitment": commitment.commitment })),
        ));
    }

    #[cfg(feature = "grpc-head-cache")]
    if let Some(cache) = state.head_cache.as_ref() {
        route.head_cache_read();
        if let Some(payload) =
            cache.get_block(slot, commitment.commitment, fetch_plan.transaction_details)
        {
            if payload.metadata().slot != slot {
                warn!(
                    requested = slot,
                    observed = payload.metadata().slot,
                    "Head-cache block metadata slot mismatch"
                );
            }

            if let Some(observed) = block_payload_transaction_count(&payload)
                && payload.metadata().executed_transaction_count != observed as u64
            {
                warn!(
                    slot,
                    expected = payload.metadata().executed_transaction_count,
                    observed = observed,
                    entry_count = payload.metadata().entry_count,
                    "Head-cache block transaction count mismatch"
                );
            }

            route.source_head_cache();
            return respond_with_hydrated_block(
                state.as_ref(),
                id,
                &mut route,
                slot,
                payload,
                fetch_plan,
                None,
            )
            .await;
        }
    }

    // Disk tier: serves any individually covered slot (all stored data is
    // finalized, satisfying the confirmed/finalized commitments accepted
    // above). A Skipped marker is proof the slot has no block on the finalized
    // chain, so the miss is answered without ClickHouse.
    #[cfg(feature = "disk-cache")]
    if let Some(disk) = state.disk_cache.as_ref() {
        route.disk_cache_read();
        match disk.get_block(slot, fetch_plan.transaction_details).await {
            crate::disk_cache::DiskBlockResult::Found(payload) => {
                route.source_disk_cache();
                return respond_with_hydrated_block(
                    state.as_ref(),
                    id,
                    &mut route,
                    slot,
                    *payload,
                    fetch_plan,
                    None,
                )
                .await;
            }
            crate::disk_cache::DiskBlockResult::Skipped => {
                route.source_disk_cache();
                route.not_found();
                return Ok(json_rpc_error_response(
                    id,
                    JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED as i32,
                    format!("Slot {slot} was skipped, or missing in long-term storage"),
                    None,
                ));
            }
            crate::disk_cache::DiskBlockResult::NotCovered => {}
        }
    }

    route.source_clickhouse();
    let (block_payload_opt, timings) = match fetch_plan.transaction_details {
        TransactionDetails::None => match state
            .clickhouse
            .get_block_metadata_by_slot(slot, fetch_plan.show_rewards)
            .await
        {
            Ok((metadata_opt, timings)) => {
                (metadata_opt.map(StoredBlockPayload::Metadata), timings)
            }
            Err(e) => {
                metrics::backend_error("get_block_metadata_by_slot");
                error!(
                    "Failed to query ClickHouse for block {} metadata: {}",
                    slot, e
                );
                return Ok(json_rpc_internal_error_response(id));
            }
        },
        TransactionDetails::Signatures => match tokio::try_join!(
            state
                .clickhouse
                .get_block_metadata_by_slot(slot, fetch_plan.show_rewards),
            state.clickhouse.get_block_signatures_by_slot(slot)
        ) {
            Ok(((metadata_opt, mut timings), (signatures, signature_timings))) => {
                timings.add(signature_timings);
                let payload = metadata_opt.map(|metadata| StoredBlockPayload::Signatures {
                    metadata,
                    signatures,
                });
                (payload, timings)
            }
            Err(e) => {
                metrics::backend_error("get_block_signatures_by_slot");
                error!(
                    "Failed to query ClickHouse for block {} signatures: {}",
                    slot, e
                );
                return Ok(json_rpc_internal_error_response(id));
            }
        },
        TransactionDetails::Accounts => match tokio::try_join!(
            state
                .clickhouse
                .get_block_metadata_by_slot(slot, fetch_plan.show_rewards),
            state.clickhouse.get_block_accounts_by_slot(slot)
        ) {
            Ok(((metadata_opt, mut timings), (transactions, tx_timings))) => {
                timings.add(tx_timings);
                let payload = metadata_opt.map(|metadata| StoredBlockPayload::Accounts {
                    metadata,
                    transactions,
                });
                (payload, timings)
            }
            Err(e) => {
                metrics::backend_error("get_block_accounts_by_slot");
                error!(
                    "Failed to query ClickHouse for block {} accounts: {}",
                    slot, e
                );
                return Ok(json_rpc_internal_error_response(id));
            }
        },
        TransactionDetails::Full => match tokio::try_join!(
            state
                .clickhouse
                .get_block_metadata_by_slot(slot, fetch_plan.show_rewards),
            state.clickhouse.get_block_full_transactions_by_slot(slot)
        ) {
            Ok(((metadata_opt, mut timings), (transactions, tx_timings))) => {
                timings.add(tx_timings);
                let payload = metadata_opt.map(|metadata| {
                    StoredBlockPayload::Full(StoredBlockRecord {
                        metadata,
                        transactions,
                    })
                });
                (payload, timings)
            }
            Err(e) => {
                metrics::backend_error("get_block_full_transactions_by_slot");
                error!("Failed to query ClickHouse for block {}: {}", slot, e);
                return Ok(json_rpc_internal_error_response(id));
            }
        },
    };

    let Some(block_payload) = block_payload_opt else {
        let clickhouse_latest = match state
            .latest_slot_cache
            .get_or_refresh(&state.clickhouse)
            .await
        {
            Ok(latest) => latest,
            Err(e) => {
                metrics::backend_error("get_latest_finalized_slot");
                error!(
                    "Failed to refresh latest slot cache for getBlock miss classification at slot {}: {}",
                    slot, e
                );
                let mut resp = json_rpc_internal_error_response(id);
                add_downstream_header(&mut resp, &timings);
                return Ok(resp);
            }
        };

        route.not_found();
        let (code, message) = classify_get_block_miss(slot, clickhouse_latest);
        let mut resp = json_rpc_error_response(id, code, message, None);
        add_downstream_header(&mut resp, &timings);
        return Ok(resp);
    };

    respond_with_hydrated_block(
        state.as_ref(),
        id,
        &mut route,
        slot,
        block_payload,
        fetch_plan,
        Some(timings),
    )
    .await
}

pub(crate) async fn handle_get_block_time(
    state: Arc<AppState>,
    id: Value,
    params: Option<Vec<Value>>,
) -> Result<Response, StatusCode> {
    let mut route = RouteMetric::for_state("getBlockTime", state.as_ref());

    let Some(mut params) = params.filter(|v| !v.is_empty()) else {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            "Invalid params: missing slot",
            None,
        ));
    };

    let slot_value = params.remove(0);
    let slot = match slot_value.as_u64() {
        Some(slot) => slot,
        None => {
            route.invalid_params();
            return Ok(json_rpc_error_response(
                id,
                -32602,
                "Invalid params: slot must be a number",
                None,
            ));
        }
    };

    if !params.is_empty() {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            "Invalid params: unexpected config",
            None,
        ));
    }

    #[cfg(feature = "grpc-head-cache")]
    if let Some(cache) = state.head_cache.as_ref() {
        route.head_cache_read();

        let head_tip = cache.latest_slot();
        if head_tip > 0 && slot > head_tip {
            route.not_found();
            return Ok(json_rpc_error_response(
                id,
                JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE as i32,
                format!("Block not available for slot {slot}"),
                None,
            ));
        }

        if let Some(block_time) = cache.block_time_for_slot(slot) {
            route.source_head_cache();
            route.success();
            return Ok(json_rpc_success_response(id, json!(block_time)));
        }
    }

    // Disk tier: a covered slot answers conclusively (including a stored NULL
    // block time); a Skipped marker proves the slot has no block.
    #[cfg(feature = "disk-cache")]
    if let Some(disk) = state.disk_cache.as_ref() {
        route.disk_cache_read();
        match disk.block_time_for_slot(slot).await {
            crate::disk_cache::DiskBlockTime::Found(block_time) => {
                route.source_disk_cache();
                route.success();
                return Ok(match block_time {
                    Some(value) => json_rpc_success_response(id, json!(value)),
                    None => json_rpc_null_response(id),
                });
            }
            crate::disk_cache::DiskBlockTime::Skipped => {
                route.source_disk_cache();
                route.not_found();
                return Ok(json_rpc_error_response(
                    id,
                    -32009,
                    format!("Slot {slot} was skipped, or missing in long-term storage"),
                    None,
                ));
            }
            crate::disk_cache::DiskBlockTime::NotCovered => {}
        }
    }

    route.source_clickhouse();
    let (block_time_opt, timings) = match state.clickhouse.get_block_time_by_slot(slot).await {
        Ok(result) => result,
        Err(e) => {
            metrics::backend_error("get_block_time_by_slot");
            error!("Failed to query ClickHouse for block time {}: {}", slot, e);
            return Ok(json_rpc_internal_error_response(id));
        }
    };

    let mut resp = match block_time_opt {
        Some(block_time) => {
            route.success();
            match block_time {
                Some(value) => json_rpc_success_response(id, json!(value)),
                None => json_rpc_null_response(id),
            }
        }
        None => {
            route.not_found();
            json_rpc_error_response(
                id,
                -32009,
                format!("Slot {slot} was skipped, or missing in long-term storage"),
                None,
            )
        }
    };
    add_downstream_header(&mut resp, &timings);
    Ok(resp)
}

pub(crate) async fn handle_get_blocks(
    state: Arc<AppState>,
    id: Value,
    params: Option<Vec<Value>>,
) -> Result<Response, StatusCode> {
    let mut route = RouteMetric::for_state("getBlocks", state.as_ref());

    let Some(mut params) = params.filter(|v| !v.is_empty()) else {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            "Invalid params: missing start_slot",
            None,
        ));
    };

    let start_value = params.remove(0);
    let start_slot = match start_value.as_u64() {
        Some(slot) => slot,
        None => {
            route.invalid_params();
            return Ok(json_rpc_error_response(
                id,
                -32602,
                "Invalid params: start_slot must be a number",
                None,
            ));
        }
    };

    let mut end_slot_opt = None;
    let mut end_from_param = false;
    let mut config_value_opt: Option<Value> = None;

    if let Some(value) = params.first() {
        if value.is_null() {
            if let Some(config_value) = params.get(1) {
                config_value_opt = Some(config_value.clone());
            }
        } else if value.is_object() {
            config_value_opt = Some(value.clone());
        } else if value.is_number() {
            let end_slot = match value.as_u64() {
                Some(slot) => slot,
                None => {
                    route.invalid_params();
                    return Ok(json_rpc_error_response(
                        id,
                        -32602,
                        "Invalid params: end_slot must be a number",
                        None,
                    ));
                }
            };
            end_slot_opt = Some(end_slot);
            end_from_param = true;
            if let Some(config_value) = params.get(1) {
                config_value_opt = Some(config_value.clone());
            }
        } else {
            route.invalid_params();
            return Ok(json_rpc_error_response(
                id,
                -32602,
                "Invalid params: end_slot must be a number",
                None,
            ));
        }
    }

    let config = match parse_get_blocks_config(config_value_opt) {
        Ok(config) => config,
        Err(message) => {
            route.invalid_params();
            return Ok(json_rpc_error_response(id, -32602, message, None));
        }
    };

    let commitment = config.commitment.unwrap_or_default();
    if let Some(resp) =
        reject_unsupported_blocks_commitment(state.as_ref(), &mut route, &id, &commitment)
    {
        return Ok(resp);
    }

    let timings = QueryTimings {
        elapsed_ms: 0,
        received_bytes: 0,
        decoded_bytes: 0,
        rows_read: Some(0),
        rows_read_unknown: false,
        rows_returned: 0,
    };

    let end_slot = match end_slot_opt {
        Some(end_slot) => end_slot,
        None => {
            #[cfg(feature = "grpc-head-cache")]
            let head_latest_opt = if let Some(cache) = state.head_cache.as_ref() {
                route.head_cache_read();
                let latest = cache.latest_slot_at_least(commitment.commitment);
                (latest > 0).then_some(latest)
            } else {
                None
            };

            #[cfg(not(feature = "grpc-head-cache"))]
            let head_latest_opt: Option<u64> = None;

            let clickhouse_latest_opt = match state
                .latest_slot_cache
                .get_or_refresh(&state.clickhouse)
                .await
            {
                Ok(latest) => Some(latest),
                Err(e) => {
                    metrics::backend_error("get_latest_finalized_slot");
                    #[cfg(feature = "grpc-head-cache")]
                    {
                        if head_latest_opt.is_some() {
                            warn!(
                                "ClickHouse latest slot cache refresh failed for getBlocks; falling back to head cache latest slot: {:?}; error: {}",
                                head_latest_opt, e
                            );
                            None
                        } else {
                            error!("Failed to refresh latest slot cache for getBlocks: {}", e);
                            return Ok(json_rpc_internal_error_response(id));
                        }
                    }
                    #[cfg(not(feature = "grpc-head-cache"))]
                    {
                        error!("Failed to refresh latest slot cache for getBlocks: {}", e);
                        return Ok(json_rpc_internal_error_response(id));
                    }
                }
            };

            let latest_opt = match (clickhouse_latest_opt, head_latest_opt) {
                (Some(ch), Some(head)) => Some(ch.max(head)),
                (Some(ch), None) => Some(ch),
                (None, Some(head)) => Some(head),
                (None, None) => None,
            };

            let Some(latest) = latest_opt else {
                route.success();
                route.source_none();
                let mut resp = json_rpc_success_response(id, Vec::<u64>::new());
                add_downstream_header(&mut resp, &timings);
                return Ok(resp);
            };
            latest
        }
    };

    if end_slot < start_slot {
        if end_from_param {
            route.invalid_params();
            return Ok(json_rpc_error_response(
                id,
                -32602,
                "Invalid params: end_slot must be greater than or equal to start_slot",
                None,
            ));
        }

        route.success();
        let mut resp = json_rpc_success_response(id, Vec::<u64>::new());
        add_downstream_header(&mut resp, &timings);
        return Ok(resp);
    }

    if end_slot.saturating_sub(start_slot) > MAX_GET_BLOCKS_RANGE {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            format!(
                "Invalid params: end_slot must be no more than {} blocks higher than start_slot",
                MAX_GET_BLOCKS_RANGE
            ),
            None,
        ));
    }

    get_block_slots_response_for_range(
        &state, &mut route, id, start_slot, end_slot, commitment, timings,
    )
    .await
}

pub(crate) async fn handle_get_blocks_with_limit(
    state: Arc<AppState>,
    id: Value,
    params: Option<Vec<Value>>,
) -> Result<Response, StatusCode> {
    let mut route = RouteMetric::for_state("getBlocksWithLimit", state.as_ref());

    let Some(mut params) = params.filter(|v| !v.is_empty()) else {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            "Invalid params: missing start_slot",
            None,
        ));
    };

    let start_value = params.remove(0);
    let start_slot = match start_value.as_u64() {
        Some(slot) => slot,
        None => {
            route.invalid_params();
            return Ok(json_rpc_error_response(
                id,
                -32602,
                "Invalid params: start_slot must be a number",
                None,
            ));
        }
    };

    let Some(limit_value) = params.first() else {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            "Invalid params: missing limit",
            None,
        ));
    };
    let limit = match limit_value.as_u64() {
        Some(limit) => limit,
        None => {
            route.invalid_params();
            return Ok(json_rpc_error_response(
                id,
                -32602,
                "Invalid params: limit must be a number",
                None,
            ));
        }
    };

    let config = match parse_get_blocks_config(params.get(1).cloned()) {
        Ok(config) => config,
        Err(message) => {
            route.invalid_params();
            return Ok(json_rpc_error_response(id, -32602, message, None));
        }
    };

    let commitment = config.commitment.unwrap_or_default();
    if let Some(resp) =
        reject_unsupported_blocks_commitment(state.as_ref(), &mut route, &id, &commitment)
    {
        return Ok(resp);
    }

    if limit > MAX_GET_BLOCKS_RANGE {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            format!(
                "Invalid params: limit must be no greater than {}",
                MAX_GET_BLOCKS_RANGE
            ),
            None,
        ));
    }

    let timings = QueryTimings {
        elapsed_ms: 0,
        received_bytes: 0,
        decoded_bytes: 0,
        rows_read: Some(0),
        rows_read_unknown: false,
        rows_returned: 0,
    };

    if limit == 0 {
        route.success();
        route.source_none();
        let mut resp = json_rpc_success_response(id, Vec::<u64>::new());
        add_downstream_header(&mut resp, &timings);
        return Ok(resp);
    }

    let end_slot = start_slot.saturating_add(limit.saturating_sub(1));

    get_block_slots_response_for_range(
        &state, &mut route, id, start_slot, end_slot, commitment, timings,
    )
    .await
}

pub(crate) async fn handle_get_inflation_reward(
    state: Arc<AppState>,
    id: Value,
    params: Option<Vec<Value>>,
) -> Result<Response, StatusCode> {
    let mut route = RouteMetric::for_state("getInflationReward", state.as_ref());

    let Some(mut params) = params.filter(|v| !v.is_empty()) else {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            "Invalid params: missing addresses",
            None,
        ));
    };

    if params.len() > 2 {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            "Invalid params: expected addresses array and optional config object",
            None,
        ));
    }

    let addresses_value = params.remove(0);
    let Some(addresses) = addresses_value.as_array() else {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            "Invalid params: addresses must be an array",
            None,
        ));
    };

    let mut requested_pubkeys = Vec::with_capacity(addresses.len());
    for value in addresses {
        let Some(address) = value.as_str() else {
            route.invalid_params();
            return Ok(json_rpc_error_response(
                id,
                -32602,
                "Invalid params: addresses must be an array of strings",
                None,
            ));
        };

        let Ok(pubkey) = address.parse::<Pubkey>() else {
            route.invalid_params();
            return Ok(json_rpc_error_response(
                id,
                -32602,
                "Invalid param: Invalid",
                None,
            ));
        };

        requested_pubkeys.push(pubkey.to_bytes());
    }

    let config = match parse_get_inflation_reward_config(params.into_iter().next()) {
        Ok(config) => config,
        Err(message) => {
            route.invalid_params();
            return Ok(json_rpc_error_response(id, -32602, message, None));
        }
    };

    let commitment = config.commitment.unwrap_or_default();
    if commitment.is_processed() {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            "Only confirmed or finalized commitments are supported",
            Some(json!({ "requestedCommitment": commitment.commitment })),
        ));
    }

    let needs_context_slot = config.min_context_slot.is_some() || config.epoch.is_none();
    let mut context_slot_opt = None;
    if needs_context_slot {
        let (context_slot, context_source) = match state
            .resolve_latest_slot_with_source("get_inflation_reward_context", commitment.commitment)
            .await
        {
            Ok(result) => result,
            Err(e) => {
                metrics::backend_error("get_latest_finalized_slot");
                error!(
                    "Failed to resolve context slot for getInflationReward: {}",
                    e
                );
                return Ok(json_rpc_internal_error_response(id));
            }
        };
        context_slot_opt = Some(context_slot);

        match context_source {
            LatestSlotSource::ClickHouse => route.source_clickhouse(),
            #[cfg(feature = "grpc-head-cache")]
            LatestSlotSource::HeadCache => route.source_head_cache(),
        }
    }

    if let Some(min_context_slot) = config.min_context_slot {
        let context_slot = context_slot_opt.expect("context slot must be resolved when required");
        if context_slot < min_context_slot {
            route.rpc_error();
            return Ok(json_rpc_error_response(
                id,
                JSON_RPC_SERVER_ERROR_MIN_CONTEXT_SLOT_NOT_REACHED as i32,
                "Minimum context slot has not been reached",
                Some(json!({ "contextSlot": context_slot })),
            ));
        }
    }

    let epoch = match config.epoch {
        Some(epoch) => epoch,
        None => {
            let context_slot =
                context_slot_opt.expect("context slot must be resolved when epoch is omitted");
            (context_slot / DEFAULT_SLOTS_PER_EPOCH).saturating_sub(1)
        }
    };

    route.source_clickhouse();
    let (reward_rows, timings) = match state
        .clickhouse
        .get_inflation_rewards_for_epoch(&requested_pubkeys, epoch)
        .await
    {
        Ok(result) => result,
        Err(e) => {
            metrics::backend_error("get_inflation_rewards_for_epoch");
            error!(
                "Failed to query ClickHouse inflation rewards for epoch {}: {}",
                epoch, e
            );
            return Ok(json_rpc_internal_error_response(id));
        }
    };

    let mut rewards_by_pubkey = HashMap::with_capacity(reward_rows.len());
    for row in reward_rows {
        rewards_by_pubkey.insert(
            row.pubkey,
            InflationRewardInfo {
                epoch,
                effective_slot: row.effective_slot,
                amount: row.lamports.unsigned_abs(),
                post_balance: row.post_balance,
                commission: row.commission,
            },
        );
    }

    let rewards = requested_pubkeys
        .iter()
        .map(|pubkey| rewards_by_pubkey.get(pubkey).cloned())
        .collect::<Vec<_>>();

    route.success();
    let mut resp = json_rpc_success_response(id, json!(rewards));
    add_downstream_header(&mut resp, &timings);
    Ok(resp)
}

pub(crate) async fn handle_get_first_available_block(
    state: Arc<AppState>,
    id: Value,
    params: Option<Vec<Value>>,
) -> Result<Response, StatusCode> {
    let mut route = RouteMetric::for_state("getFirstAvailableBlock", state.as_ref());

    if params.filter(|v| !v.is_empty()).is_some() {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            "Invalid params: expected no parameters",
            None,
        ));
    }

    route.source_clickhouse();
    let (slot_opt, timings) = match state.clickhouse.get_first_available_block().await {
        Ok(result) => result,
        Err(e) => {
            metrics::backend_error("get_first_available_block");
            error!(
                "Failed to query ClickHouse for first available block: {}",
                e
            );
            return Ok(json_rpc_internal_error_response(id));
        }
    };

    let mut resp = match slot_opt {
        Some(slot) => {
            route.success();
            json_rpc_success_response(id, json!(slot))
        }
        None => {
            route.not_found();
            json_rpc_null_response(id)
        }
    };
    add_downstream_header(&mut resp, &timings);
    Ok(resp)
}

pub(crate) async fn handle_get_health(
    state: Arc<AppState>,
    id: Value,
    params: Option<Vec<Value>>,
) -> Result<Response, StatusCode> {
    let mut route = RouteMetric::for_state("getHealth", state.as_ref());

    if params.filter(|v| !v.is_empty()).is_some() {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            "Invalid params: expected no parameters",
            None,
        ));
    }

    route.source_clickhouse();
    match state.clickhouse.get_latest_finalized_slot().await {
        Ok(Some(_slot)) => {
            route.success();
            Ok(json_rpc_success_response(id, "ok"))
        }
        Ok(None) => {
            route.rpc_error();
            warn!("ClickHouse returned no latest finalized slot for getHealth");
            Ok(json_rpc_node_unhealthy_response(id))
        }
        Err(e) => {
            route.rpc_error();
            metrics::backend_error("get_latest_finalized_slot");
            error!("Failed to query ClickHouse latest finalized slot for getHealth: {e}");
            Ok(json_rpc_node_unhealthy_response(id))
        }
    }
}

pub(crate) async fn handle_minimum_ledger_slot(
    state: Arc<AppState>,
    id: Value,
    params: Option<Vec<Value>>,
) -> Result<Response, StatusCode> {
    let mut route = RouteMetric::for_state("minimumLedgerSlot", state.as_ref());

    if params.filter(|v| !v.is_empty()).is_some() {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            "Invalid params: expected no parameters",
            None,
        ));
    }

    route.source_clickhouse();
    let (slot_opt, timings) = match state.clickhouse.minimum_ledger_slot().await {
        Ok(result) => result,
        Err(e) => {
            metrics::backend_error("minimum_ledger_slot");
            error!("Failed to query ClickHouse for minimum ledger slot: {}", e);
            return Ok(json_rpc_internal_error_response(id));
        }
    };

    let mut resp = match slot_opt {
        Some(slot) => {
            route.success();
            json_rpc_success_response(id, json!(slot))
        }
        None => {
            route.not_found();
            json_rpc_null_response(id)
        }
    };
    add_downstream_header(&mut resp, &timings);
    Ok(resp)
}

#[cfg(test)]
mod tests {
    use super::{classify_get_block_miss, merge_sorted_block_slots};
    use solana_rpc_client_api::custom_error::JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE;
    use solana_rpc_client_api::custom_error::JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED;

    #[test]
    fn merge_sorted_block_slots_handles_empty_inputs() {
        assert_eq!(
            merge_sorted_block_slots(Vec::new(), Vec::new()),
            Vec::<u64>::new()
        );
        assert_eq!(merge_sorted_block_slots(vec![1, 2], Vec::new()), vec![1, 2]);
        assert_eq!(merge_sorted_block_slots(Vec::new(), vec![3, 4]), vec![3, 4]);
    }

    #[test]
    fn merge_sorted_block_slots_merges_sorted_inputs() {
        let merged = merge_sorted_block_slots(vec![1, 3, 5], vec![2, 4, 6]);
        assert_eq!(merged, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn merge_sorted_block_slots_deduplicates_overlap() {
        let merged = merge_sorted_block_slots(vec![1, 2, 2, 3, 6], vec![2, 3, 4, 6, 7]);
        assert_eq!(merged, vec![1, 2, 3, 4, 6, 7]);
    }

    #[test]
    fn classify_get_block_miss_returns_block_not_available_above_clickhouse_latest() {
        let (code, message) = classify_get_block_miss(11, 10);
        assert_eq!(code, JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE as i32);
        assert_eq!(message, "Block not available for slot 11");
    }

    #[test]
    fn classify_get_block_miss_returns_long_term_storage_error_at_clickhouse_latest() {
        let (code, message) = classify_get_block_miss(10, 10);
        assert_eq!(
            code,
            JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED as i32
        );
        assert_eq!(
            message,
            "Slot 10 was skipped, or missing in long-term storage"
        );
    }

    #[test]
    fn classify_get_block_miss_returns_long_term_storage_error_below_clickhouse_latest() {
        let (code, message) = classify_get_block_miss(9, 10);
        assert_eq!(
            code,
            JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED as i32
        );
        assert_eq!(
            message,
            "Slot 9 was skipped, or missing in long-term storage"
        );
    }

    #[test]
    fn classify_get_block_miss_treats_slots_above_zero_latest_as_not_available() {
        let (code, message) = classify_get_block_miss(1, 0);
        assert_eq!(code, JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE as i32);
        assert_eq!(message, "Block not available for slot 1");
    }
}
