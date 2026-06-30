// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use axum::{http::StatusCode, response::Response};
use serde_json::{Value, json};
use solana_commitment_config::CommitmentConfig;
use solana_commitment_config::CommitmentLevel;
use solana_rpc_client_api::custom_error::{
    JSON_RPC_SERVER_ERROR_MIN_CONTEXT_SLOT_NOT_REACHED,
    JSON_RPC_SERVER_ERROR_UNSUPPORTED_TRANSACTION_VERSION,
};
use solana_rpc_client_types::config::{RpcEncodingConfigWrapper, RpcTransactionConfig};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_transaction_status::{EncodeError, UiTransactionEncoding};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tracing::error;

use crate::clickhouse::{
    NumericFilter, PaginationToken, QueryTimings, ResolvedSignatureFilter, SignatureFilter,
    SignatureSlot, SortOrder, StoredTransactionRecord, TokenAccountsFilter,
    TransactionStatusFilter, TransactionsForAddressQuery,
};
use crate::handlers::{
    RouteMetric,
    types::{
        GetTransactionsForAddressFilters, GetTransactionsForAddressOptions,
        TransactionsForAddressDetails, TransactionsForAddressFullInfo,
        TransactionsForAddressResult, TransactionsForAddressSignaturesInfo, reject_unknown_fields,
    },
};
use crate::hydration::{TransactionHydrationError, hydrate_transaction_record};
use crate::metrics;
use crate::rpc::{
    json_rpc_error_response, json_rpc_internal_error_response, json_rpc_null_response,
    json_rpc_success_response,
};
use crate::state::{AppState, LatestSlotSource};
use crate::util::add_downstream_header;

#[cfg(feature = "grpc-head-cache")]
use crate::clickhouse::SlotBoundary;
#[cfg(feature = "grpc-head-cache")]
use std::collections::HashSet;

const GET_TRANSACTION_ALLOWED_FIELDS: [&str; 4] = [
    "encoding",
    "commitment",
    "maxSupportedTransactionVersion",
    "slot",
];
const GET_TRANSACTION_INVALID_SIGNATURE_MESSAGE: &str =
    "Invalid params: signature is not a valid transaction signature";
const GET_TRANSACTION_DENYLISTED_SIGNATURES: [&str; 1] =
    ["1111111111111111111111111111111111111111111111111111111111111111"];

#[derive(Debug)]
struct ParsedGetTransactionConfig {
    wrapper: RpcEncodingConfigWrapper<RpcTransactionConfig>,
    slot: Option<u64>,
}

fn extract_get_transaction_slot(config_value: &mut Value) -> Result<Option<u64>, String> {
    let Some(object) = config_value.as_object_mut() else {
        return Ok(None);
    };

    let Some(slot_value) = object.remove("slot") else {
        return Ok(None);
    };
    if slot_value.is_null() {
        return Ok(None);
    }

    slot_value
        .as_u64()
        .ok_or_else(|| "Invalid params: slot must be a number".to_string())
        .map(Some)
}

fn parse_get_transaction_config_value(
    mut config_value: Value,
) -> Result<ParsedGetTransactionConfig, String> {
    reject_unknown_fields(&config_value, &GET_TRANSACTION_ALLOWED_FIELDS)?;

    if config_value.is_null() {
        return Ok(ParsedGetTransactionConfig {
            wrapper: RpcEncodingConfigWrapper::Current(Some(RpcTransactionConfig::default())),
            slot: None,
        });
    }

    let slot = extract_get_transaction_slot(&mut config_value)?;
    let wrapper =
        serde_json::from_value::<RpcEncodingConfigWrapper<RpcTransactionConfig>>(config_value)
            .map_err(|e| format!("Invalid params: failed to parse config ({e})"))?;

    Ok(ParsedGetTransactionConfig { wrapper, slot })
}

fn apply_get_transactions_for_address_slot_aliases(
    filters: &mut GetTransactionsForAddressFilters,
    before_slot: Option<u64>,
    until_slot: Option<u64>,
) -> Result<(), &'static str> {
    if let Some(before_slot) = before_slot {
        let slot_filter = filters.slot.get_or_insert_with(Default::default);
        if slot_filter.lt.is_some() || slot_filter.lte.is_some() {
            return Err(
                "Invalid params: beforeSlot cannot be combined with filters.slot.lt or filters.slot.lte",
            );
        }
        slot_filter.lt = Some(before_slot);
    }

    if let Some(until_slot) = until_slot {
        let slot_filter = filters.slot.get_or_insert_with(Default::default);
        if slot_filter.gt.is_some() || slot_filter.gte.is_some() {
            return Err(
                "Invalid params: untilSlot cannot be combined with filters.slot.gt or filters.slot.gte",
            );
        }
        slot_filter.gt = Some(until_slot);
    }

    Ok(())
}

async fn resolve_signature_slot_for_bounds(
    state: &AppState,
    signature: &str,
) -> crate::processing::ProcessingResult<(Option<SignatureSlot>, QueryTimings)> {
    #[cfg(feature = "grpc-head-cache")]
    if let Some(cache) = state.head_cache.as_ref()
        && let Ok(parsed) = Signature::from_str(signature)
        && let Some(pos) = cache.signature_position(&parsed)
    {
        return Ok((
            Some(SignatureSlot {
                slot: pos.slot,
                slot_idx: pos.idx,
            }),
            QueryTimings::zero(),
        ));
    }

    #[cfg(feature = "disk-cache")]
    if let Some(disk) = state.disk_cache.as_ref()
        && let Ok(parsed) = Signature::from_str(signature)
        && let Some(position) = disk.signature_position(parsed).await
    {
        return Ok((Some(position), QueryTimings::zero()));
    }

    state.clickhouse.get_signature_slot(signature).await
}

fn unsupported_transaction_version_message(version: u8) -> String {
    format!(
        "Transaction version ({version}) is not supported by the requesting client. Please try the request again with the following configuration parameter: \"maxSupportedTransactionVersion\": {version}"
    )
}

/// Hydrate a stored record and build the JSON-RPC response; shared by the
/// head-cache, disk-cache, and ClickHouse branches of getTransaction.
#[allow(clippy::too_many_arguments)]
async fn respond_with_hydrated_transaction(
    state: &AppState,
    id: Value,
    route: &mut RouteMetric,
    signature_str: &str,
    record: Arc<StoredTransactionRecord>,
    encoding: UiTransactionEncoding,
    max_version: Option<u8>,
    timings: Option<QueryTimings>,
) -> Result<Response, StatusCode> {
    let attach_timings = |resp: &mut Response| {
        if let Some(timings) = timings.as_ref() {
            add_downstream_header(resp, timings);
        }
    };

    let permit = match state.hydration_sem.clone().acquire_owned().await {
        Ok(permit) => permit,
        Err(_) => {
            error!(signature = signature_str, "Hydration semaphore closed");
            let mut resp = json_rpc_internal_error_response(id);
            attach_timings(&mut resp);
            return Ok(resp);
        }
    };

    let record_slot = record.slot;
    match tokio::task::spawn_blocking(move || {
        let _permit = permit;
        hydrate_transaction_record(record.as_ref(), encoding, max_version)
    })
    .await
    {
        Ok(Ok(encoded_tx)) => {
            route.success();
            let mut resp = json_rpc_success_response(id, encoded_tx);
            attach_timings(&mut resp);
            Ok(resp)
        }
        Ok(Err(TransactionHydrationError::Encode(EncodeError::UnsupportedTransactionVersion(
            version,
        )))) => {
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
        Ok(Err(e)) => {
            error!(
                "Failed to hydrate transaction {} in slot {}: {}",
                signature_str, record_slot, e
            );
            let mut resp = json_rpc_internal_error_response(id);
            attach_timings(&mut resp);
            Ok(resp)
        }
        Err(join_err) => {
            error!(
                "Failed to join hydration task for transaction {} in slot {}: {}",
                signature_str, record_slot, join_err
            );
            let mut resp = json_rpc_internal_error_response(id);
            attach_timings(&mut resp);
            Ok(resp)
        }
    }
}

pub(crate) async fn handle_get_transaction(
    state: Arc<AppState>,
    id: Value,
    params: Option<Vec<Value>>,
) -> Result<Response, StatusCode> {
    let mut route = RouteMetric::for_state("getTransaction", state.as_ref());

    let Some(mut params) = params.filter(|v| !v.is_empty()) else {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            "Invalid params: missing signature",
            None,
        ));
    };

    let signature_value = params.remove(0);
    let signature_str = match signature_value.as_str() {
        Some(sig) => sig,
        None => {
            route.invalid_params();
            return Ok(json_rpc_error_response(
                id,
                -32602,
                "Invalid params: signature must be a string",
                None,
            ));
        }
    };

    if GET_TRANSACTION_DENYLISTED_SIGNATURES.contains(&signature_str) {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            GET_TRANSACTION_INVALID_SIGNATURE_MESSAGE,
            None,
        ));
    }

    let signature = match Signature::from_str(signature_str) {
        Ok(sig) => sig,
        Err(_) => {
            route.invalid_params();
            return Ok(json_rpc_error_response(
                id,
                -32602,
                "Invalid params: signature is not valid base58",
                None,
            ));
        }
    };

    if signature == Signature::default() {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            GET_TRANSACTION_INVALID_SIGNATURE_MESSAGE,
            None,
        ));
    }
    #[cfg(not(feature = "grpc-head-cache"))]
    let _ = signature;

    let parsed_config = match params.into_iter().next() {
        Some(config_value) => match parse_get_transaction_config_value(config_value) {
            Ok(parsed) => parsed,
            Err(message) => {
                route.invalid_params();
                return Ok(json_rpc_error_response(id, -32602, message, None));
            }
        },
        None => ParsedGetTransactionConfig {
            wrapper: RpcEncodingConfigWrapper::Current(Some(RpcTransactionConfig::default())),
            slot: None,
        },
    };

    let requested_slot = parsed_config.slot;
    let config = parsed_config.wrapper.convert_to_current();
    let encoding = config.encoding.unwrap_or(UiTransactionEncoding::Json);
    let commitment = config.commitment.unwrap_or(CommitmentConfig::default());

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

    #[cfg(feature = "grpc-head-cache")]
    if let Some(cache) = state.head_cache.as_ref()
        && let Some(record) = cache.get_tx(&signature, commitment.commitment)
    {
        if requested_slot.is_some_and(|slot| record.slot != slot) {
            route.source_head_cache();
            route.not_found();
            return Ok(json_rpc_null_response(id));
        }

        route.source_head_cache();
        return respond_with_hydrated_transaction(
            state.as_ref(),
            id,
            &mut route,
            signature_str,
            record,
            encoding,
            config.max_supported_transaction_version,
            None,
        )
        .await;
    }
    #[cfg(feature = "grpc-head-cache")]
    if state.head_cache.is_some() {
        route.head_cache_read();
    }

    // Disk tier: everything stored is finalized, which satisfies any commitment
    // accepted above. A hit answers without ClickHouse. A miss proves nothing by
    // itself (the signature may be older than the window or in a coverage hole) —
    // EXCEPT when the request pinned a slot that the disk fully covers: then the
    // transaction conclusively does not exist there.
    #[cfg(feature = "disk-cache")]
    if let Some(disk) = state.disk_cache.as_ref() {
        route.disk_cache_read();
        if let Some(record) = disk.get_tx(signature).await {
            if requested_slot.is_some_and(|slot| record.slot != slot) {
                route.source_disk_cache();
                route.not_found();
                return Ok(json_rpc_null_response(id));
            }
            route.source_disk_cache();
            return respond_with_hydrated_transaction(
                state.as_ref(),
                id,
                &mut route,
                signature_str,
                Arc::new(record),
                encoding,
                config.max_supported_transaction_version,
                None,
            )
            .await;
        }
        if let Some(slot) = requested_slot
            && matches!(
                disk.slot_status(slot).await,
                crate::disk_cache::SlotStatus::Covered { .. }
                    | crate::disk_cache::SlotStatus::Skipped
            )
        {
            route.source_disk_cache();
            route.not_found();
            return Ok(json_rpc_null_response(id));
        }
    }

    route.source_clickhouse();
    let transaction_result = if let Some(slot) = requested_slot {
        state
            .clickhouse
            .get_transaction_by_signature_and_slot(signature_str, slot)
            .await
    } else {
        state
            .clickhouse
            .get_transaction_by_signature(signature_str)
            .await
    };
    let (transaction_row, timings) = match transaction_result {
        Ok(row) => row,
        Err(e) => {
            let operation = if requested_slot.is_some() {
                "get_transaction_by_signature_and_slot"
            } else {
                "get_transaction_by_signature"
            };
            metrics::backend_error(operation);
            error!(
                "Failed to query ClickHouse for signature {signature_str}: {}",
                e
            );
            return Ok(json_rpc_internal_error_response(id));
        }
    };

    let Some(record) = transaction_row else {
        route.not_found();
        let mut resp = json_rpc_null_response(id);
        add_downstream_header(&mut resp, &timings);
        return Ok(resp);
    };

    respond_with_hydrated_transaction(
        state.as_ref(),
        id,
        &mut route,
        signature_str,
        Arc::new(record),
        encoding,
        config.max_supported_transaction_version,
        Some(timings),
    )
    .await
}

pub(crate) async fn handle_get_transactions_for_address(
    state: Arc<AppState>,
    id: Value,
    params: Option<Vec<Value>>,
) -> Result<Response, StatusCode> {
    let mut route = RouteMetric::for_state("getTransactionsForAddress", state.as_ref());

    let Some(mut params) = params.filter(|v| !v.is_empty()) else {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            "Invalid params: missing address",
            None,
        ));
    };

    let address_value = params.remove(0);
    let address = match address_value.as_str() {
        Some(value) => value,
        None => {
            route.invalid_params();
            return Ok(json_rpc_error_response(
                id,
                -32602,
                "Invalid params: address must be a string",
                None,
            ));
        }
    };

    if Pubkey::from_str(address).is_err() {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            "Invalid param: Invalid",
            None,
        ));
    }

    let options = match params.into_iter().next() {
        Some(options_value) => {
            if options_value.is_null() {
                GetTransactionsForAddressOptions::default()
            } else {
                match serde_json::from_value::<GetTransactionsForAddressOptions>(options_value) {
                    Ok(parsed) => parsed,
                    Err(e) => {
                        route.invalid_params();
                        return Ok(json_rpc_error_response(
                            id,
                            -32602,
                            format!("Invalid params: failed to parse options ({e})"),
                            None,
                        ));
                    }
                }
            }
        }
        None => GetTransactionsForAddressOptions::default(),
    };

    let transaction_details = match options
        .transaction_details
        .as_deref()
        .unwrap_or("signatures")
        .to_lowercase()
        .as_str()
    {
        "signatures" => TransactionsForAddressDetails::Signatures,
        "full" => TransactionsForAddressDetails::Full,
        other => {
            route.invalid_params();
            return Ok(json_rpc_error_response(
                id,
                -32602,
                format!("Invalid params: unsupported transactionDetails '{other}'"),
                None,
            ));
        }
    };

    let sort_order = match options
        .sort_order
        .as_deref()
        .unwrap_or("desc")
        .to_lowercase()
        .as_str()
    {
        "asc" => SortOrder::Asc,
        "desc" => SortOrder::Desc,
        other => {
            route.invalid_params();
            return Ok(json_rpc_error_response(
                id,
                -32602,
                format!("Invalid params: unsupported sortOrder '{other}'"),
                None,
            ));
        }
    };

    let spec_max_limit = match transaction_details {
        TransactionsForAddressDetails::Signatures => 1000,
        TransactionsForAddressDetails::Full => 100,
    };
    let max_limit = state.max_signatures_limit.min(spec_max_limit);
    let requested_limit = options.limit.unwrap_or(spec_max_limit);
    if requested_limit == 0 {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            format!("Invalid limit; max {}", max_limit),
            None,
        ));
    }
    let limit = requested_limit.min(max_limit);

    if let Some(commitment) = options.commitment.as_deref() {
        let commitment = commitment.to_ascii_lowercase();
        match commitment.as_str() {
            "finalized" | "confirmed" => {}
            "processed" => {
                #[cfg(feature = "grpc-head-cache")]
                {
                    if state.head_cache.is_none() {
                        route.invalid_params();
                        return Ok(json_rpc_error_response(
                            id,
                            -32602,
                            "Only confirmed or finalized commitments are supported",
                            Some(json!({ "requestedCommitment": commitment })),
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
                        Some(json!({ "requestedCommitment": commitment })),
                    ));
                }
            }
            other => {
                route.invalid_params();
                return Ok(json_rpc_error_response(
                    id,
                    -32602,
                    format!("Invalid params: unsupported commitment '{other}'"),
                    None,
                ));
            }
        }
    }

    if let Some(min_context_slot) = options.min_context_slot {
        let commitment = options
            .commitment
            .as_deref()
            .unwrap_or("finalized")
            .trim()
            .to_ascii_lowercase();
        let min_commitment = match commitment.as_str() {
            "processed" => CommitmentLevel::Processed,
            "confirmed" => CommitmentLevel::Confirmed,
            _ => CommitmentLevel::Finalized,
        };

        let (context_slot, context_source) = match state
            .resolve_latest_slot_with_source(
                "get_transactions_for_address_min_context",
                min_commitment,
            )
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
            LatestSlotSource::ClickHouse => route.source_clickhouse(),
            #[cfg(feature = "grpc-head-cache")]
            LatestSlotSource::HeadCache => route.source_head_cache(),
        }

        if context_slot < min_context_slot {
            let code = JSON_RPC_SERVER_ERROR_MIN_CONTEXT_SLOT_NOT_REACHED as i32;
            route.rpc_error();
            return Ok(json_rpc_error_response(
                id,
                code,
                "Minimum context slot has not been reached",
                Some(json!({ "contextSlot": context_slot })),
            ));
        }
    }

    let pagination = if let Some(token) = options.pagination_token.as_deref() {
        let token = token.trim();
        if token.is_empty() {
            route.invalid_params();
            return Ok(json_rpc_error_response(
                id,
                -32602,
                "Invalid params: paginationToken is empty",
                None,
            ));
        }

        if let Some((slot_str, idx_str)) = token.split_once(':') {
            let slot = match slot_str.parse::<u64>() {
                Ok(value) => value,
                Err(_) => {
                    route.invalid_params();
                    return Ok(json_rpc_error_response(
                        id,
                        -32602,
                        "Invalid params: paginationToken slot is not a number",
                        None,
                    ));
                }
            };
            let idx = match idx_str.parse::<u32>() {
                Ok(value) => value,
                Err(_) => {
                    route.invalid_params();
                    return Ok(json_rpc_error_response(
                        id,
                        -32602,
                        "Invalid params: paginationToken position is not a number",
                        None,
                    ));
                }
            };
            Some(PaginationToken::SlotIndex { slot, idx })
        } else {
            if Signature::from_str(token).is_err() {
                route.invalid_params();
                return Ok(json_rpc_error_response(
                    id,
                    -32602,
                    "Invalid params: paginationToken is not a valid signature",
                    None,
                ));
            }
            Some(PaginationToken::Signature(token.to_string()))
        }
    } else {
        None
    };

    #[cfg(feature = "grpc-head-cache")]
    let pagination = {
        if let Some(cache) = state.head_cache.as_ref() {
            match pagination {
                Some(PaginationToken::Signature(sig_str)) => {
                    route.head_cache_read();
                    if let Ok(sig) = Signature::from_str(&sig_str)
                        && let Some(pos) = cache.signature_position(&sig)
                    {
                        Some(PaginationToken::SlotIndex {
                            slot: pos.slot,
                            idx: pos.idx,
                        })
                    } else {
                        Some(PaginationToken::Signature(sig_str))
                    }
                }
                other => other,
            }
        } else {
            pagination
        }
    };

    #[cfg(feature = "disk-cache")]
    let pagination = {
        if let Some(disk) = state.disk_cache.as_ref() {
            match pagination {
                Some(PaginationToken::Signature(sig_str)) => {
                    route.disk_cache_read();
                    if let Ok(sig) = Signature::from_str(&sig_str)
                        && let Some(position) = disk.signature_position(sig).await
                    {
                        Some(PaginationToken::SlotIndex {
                            slot: position.slot,
                            idx: position.slot_idx,
                        })
                    } else {
                        Some(PaginationToken::Signature(sig_str))
                    }
                }
                other => other,
            }
        } else {
            pagination
        }
    };

    let mut filters = options.filters.unwrap_or_default();

    if let Err(message) = apply_get_transactions_for_address_slot_aliases(
        &mut filters,
        options.before_slot,
        options.until_slot,
    ) {
        route.invalid_params();
        return Ok(json_rpc_error_response(id, -32602, message, None));
    }

    if let Some(slot_filter) = filters.slot.as_ref()
        && slot_filter.eq.is_some()
    {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            "Invalid params: filters.slot.eq is not supported",
            None,
        ));
    }

    if let Some(signature_filter) = filters.signature.as_ref() {
        if signature_filter.eq.is_some() {
            route.invalid_params();
            return Ok(json_rpc_error_response(
                id,
                -32602,
                "Invalid params: filters.signature.eq is not supported",
                None,
            ));
        }

        for value in [
            signature_filter.gte.as_deref(),
            signature_filter.gt.as_deref(),
            signature_filter.lte.as_deref(),
            signature_filter.lt.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            if Signature::from_str(value).is_err() {
                route.invalid_params();
                return Ok(json_rpc_error_response(
                    id,
                    -32602,
                    "Invalid params: filters.signature must be valid base58 signatures",
                    None,
                ));
            }
        }
    }

    let status = match filters
        .status
        .as_deref()
        .unwrap_or("any")
        .to_lowercase()
        .as_str()
    {
        "any" | "all" => TransactionStatusFilter::Any,
        "succeeded" => TransactionStatusFilter::Succeeded,
        "failed" => TransactionStatusFilter::Failed,
        other => {
            route.invalid_params();
            return Ok(json_rpc_error_response(
                id,
                -32602,
                format!("Invalid params: unsupported status '{other}'"),
                None,
            ));
        }
    };

    let token_accounts = match filters
        .token_accounts
        .as_deref()
        .unwrap_or("none")
        .to_lowercase()
        .as_str()
    {
        "none" => TokenAccountsFilter::None,
        "balancechanged" => TokenAccountsFilter::BalanceChanged,
        "all" => TokenAccountsFilter::All,
        other => {
            route.invalid_params();
            return Ok(json_rpc_error_response(
                id,
                -32602,
                format!("Invalid params: unsupported tokenAccounts '{other}'"),
                None,
            ));
        }
    };

    if token_accounts != TokenAccountsFilter::None
        && !state.clickhouse.token_owner_activity_available()
    {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            "Invalid params: tokenAccounts filters require token owner activity data",
            None,
        ));
    }

    let signature_filter = filters.signature.map(|filter| SignatureFilter {
        gte: filter.gte,
        gt: filter.gt,
        lte: filter.lte,
        lt: filter.lt,
    });

    let slot_filter = filters.slot.map(|filter| NumericFilter {
        gte: filter.gte,
        gt: filter.gt,
        lte: filter.lte,
        lt: filter.lt,
        eq: None,
    });

    let block_time_filter = filters.block_time.map(|filter| NumericFilter {
        gte: filter.gte,
        gt: filter.gt,
        lte: filter.lte,
        lt: filter.lt,
        eq: filter.eq,
    });

    let address_pubkey = Pubkey::from_str(address).expect("validated address");
    let hot_fanout_eligible = state.clickhouse.is_gsfa_hot_address(&address_pubkey)
        && token_accounts == TokenAccountsFilter::None;

    let mut query = TransactionsForAddressQuery {
        address: address.to_string(),
        limit,
        sort_order,
        pagination,
        resolved_pagination: None,
        slot_filter,
        block_time_filter,
        signature_filter,
        resolved_signature_filter: None,
        status,
        token_accounts,
    };

    #[cfg(feature = "disk-cache")]
    let disk_candidate = state.disk_cache.is_some();
    #[cfg(not(feature = "disk-cache"))]
    let disk_candidate = false;

    let mut prequery_timings = QueryTimings::zero();
    if hot_fanout_eligible || disk_candidate {
        match query.pagination.as_ref() {
            Some(PaginationToken::SlotIndex { slot, idx }) => {
                query.resolved_pagination = Some(SignatureSlot {
                    slot: *slot,
                    slot_idx: *idx,
                });
            }
            Some(PaginationToken::Signature(signature)) => {
                let (position, timings) =
                    match resolve_signature_slot_for_bounds(state.as_ref(), signature).await {
                        Ok(result) => result,
                        Err(e) => {
                            metrics::backend_error("get_signature_slot");
                            error!(
                                "Failed to resolve hot pagination signature {signature}: {}",
                                e
                            );
                            return Ok(json_rpc_internal_error_response(id));
                        }
                    };
                prequery_timings.add(timings);
                query.resolved_pagination = position;
            }
            None => {}
        }

        if let Some(signature_filter) = query.signature_filter.as_ref() {
            let mut resolved = ResolvedSignatureFilter::default();
            for (label, maybe_signature, slot_ref) in [
                ("gte", signature_filter.gte.as_deref(), &mut resolved.gte),
                ("gt", signature_filter.gt.as_deref(), &mut resolved.gt),
                ("lte", signature_filter.lte.as_deref(), &mut resolved.lte),
                ("lt", signature_filter.lt.as_deref(), &mut resolved.lt),
            ] {
                let Some(signature) = maybe_signature else {
                    continue;
                };
                let (position, timings) =
                    match resolve_signature_slot_for_bounds(state.as_ref(), signature).await {
                        Ok(result) => result,
                        Err(e) => {
                            metrics::backend_error("get_signature_slot");
                            error!(
                                "Failed to resolve hot signature filter {label}={signature}: {}",
                                e
                            );
                            return Ok(json_rpc_internal_error_response(id));
                        }
                    };
                prequery_timings.add(timings);
                *slot_ref = position;
            }
            query.resolved_signature_filter = Some(resolved);
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum RecordSource {
        ClickHouse,
        #[cfg(feature = "grpc-head-cache")]
        Head,
        #[cfg(feature = "disk-cache")]
        Disk,
    }

    #[derive(Debug)]
    struct MergedRecord {
        source: RecordSource,
        signature: String,
        slot: u64,
        slot_idx: u32,
        err: Option<Value>,
        memo: Option<String>,
        block_time: Option<i64>,
        confirmation_status: String,
    }

    #[cfg(feature = "grpc-head-cache")]
    fn slot_filter_matches(slot: u64, filter: &NumericFilter<u64>) -> bool {
        if let Some(value) = filter.gte
            && slot < value
        {
            return false;
        }
        if let Some(value) = filter.gt
            && slot <= value
        {
            return false;
        }
        if let Some(value) = filter.lte
            && slot > value
        {
            return false;
        }
        if let Some(value) = filter.lt
            && slot >= value
        {
            return false;
        }
        true
    }

    #[cfg(feature = "grpc-head-cache")]
    let (head_cache, min_commitment) = {
        let cache = state.head_cache.as_ref();
        let commitment = options
            .commitment
            .as_deref()
            .unwrap_or("finalized")
            .trim()
            .to_ascii_lowercase();
        let min_commitment = match commitment.as_str() {
            "processed" => CommitmentLevel::Processed,
            "confirmed" => CommitmentLevel::Confirmed,
            _ => CommitmentLevel::Finalized,
        };
        (cache, min_commitment)
    };

    #[cfg(feature = "grpc-head-cache")]
    if let Some(cache) = head_cache {
        let head_only_candidate = query.pagination.is_none()
            && query.sort_order == SortOrder::Desc
            && query.token_accounts == TokenAccountsFilter::None
            && query.signature_filter.is_none()
            && query.block_time_filter.is_none()
            && query.slot_filter.is_none()
            && query.status == TransactionStatusFilter::Any;

        if head_only_candidate {
            route.head_cache_read();
            let head_metas = cache.signatures_for_address(
                &address_pubkey,
                None,
                None,
                limit as usize,
                min_commitment,
            );

            if head_metas.len() as u64 >= limit {
                if transaction_details == TransactionsForAddressDetails::Signatures {
                    route.source_head_cache();
                    route.success();
                    let data = head_metas
                        .iter()
                        .map(|meta| TransactionsForAddressSignaturesInfo {
                            signature: meta.signature_str.to_string(),
                            slot: meta.pos.slot,
                            transaction_index: meta.pos.idx,
                            err: meta.err.clone(),
                            memo: meta.memo.clone(),
                            block_time: meta.block_time,
                            confirmation_status: Some(
                                cache.confirmation_status_string(meta.pos.slot).to_string(),
                            ),
                        })
                        .collect::<Vec<_>>();

                    let pagination_token = head_metas
                        .last()
                        .map(|meta| format!("{}:{}", meta.pos.slot, meta.pos.idx));

                    let result = TransactionsForAddressResult {
                        data,
                        pagination_token,
                    };

                    return Ok(json_rpc_success_response(id, result));
                }

                let encoding = match options
                    .encoding
                    .as_deref()
                    .unwrap_or("json")
                    .to_lowercase()
                    .as_str()
                {
                    "json" => UiTransactionEncoding::Json,
                    "jsonparsed" => UiTransactionEncoding::JsonParsed,
                    "base58" => UiTransactionEncoding::Base58,
                    "base64" => UiTransactionEncoding::Base64,
                    other => {
                        route.invalid_params();
                        return Ok(json_rpc_error_response(
                            id,
                            -32602,
                            format!("Invalid params: unsupported encoding '{other}'"),
                            None,
                        ));
                    }
                };

                struct HeadHydrationInput {
                    slot: u64,
                    slot_idx: u32,
                    block_time: Option<i64>,
                    signature_str: String,
                    stored: Arc<crate::clickhouse::StoredTransactionRecord>,
                }

                let mut inputs = Vec::with_capacity(head_metas.len());
                for meta in head_metas {
                    route.head_cache_read();
                    let sig_str = meta.signature_str.to_string();
                    let Ok(sig) = Signature::from_str(&sig_str) else {
                        continue;
                    };
                    let Some(stored) = cache.get_tx(&sig, min_commitment) else {
                        continue;
                    };
                    inputs.push(HeadHydrationInput {
                        slot: meta.pos.slot,
                        slot_idx: meta.pos.idx,
                        block_time: meta.block_time,
                        signature_str: sig_str,
                        stored,
                    });
                }

                struct HeadHydrationFailure {
                    signature: String,
                    slot: u64,
                    error: TransactionHydrationError,
                }

                let permit = match state.hydration_sem.clone().acquire_owned().await {
                    Ok(permit) => permit,
                    Err(_) => {
                        error!(address, "Hydration semaphore closed");
                        return Ok(json_rpc_internal_error_response(id));
                    }
                };

                let max_supported_transaction_version = options.max_supported_transaction_version;

                let result = match tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    let mut data = Vec::with_capacity(inputs.len());
                    let mut pagination_token = None;

                    for input in inputs {
                        let encoded = match hydrate_transaction_record(
                            input.stored.as_ref(),
                            encoding,
                            max_supported_transaction_version,
                        ) {
                            Ok(encoded) => encoded,
                            Err(error) => {
                                return Err(HeadHydrationFailure {
                                    signature: input.signature_str,
                                    slot: input.slot,
                                    error,
                                });
                            }
                        };

                        data.push(TransactionsForAddressFullInfo {
                            slot: input.slot,
                            transaction_index: input.slot_idx,
                            block_time: input.block_time,
                            transaction: encoded.transaction.transaction,
                            meta: encoded.transaction.meta,
                            version: encoded.transaction.version,
                        });

                        pagination_token = Some(format!("{}:{}", input.slot, input.slot_idx));
                    }

                    Ok::<_, HeadHydrationFailure>(TransactionsForAddressResult {
                        data,
                        pagination_token,
                    })
                })
                .await
                {
                    Ok(Ok(result)) => result,
                    Ok(Err(failure)) => {
                        error!(
                            "Failed to hydrate head-cache transaction {} in slot {}: {}",
                            failure.signature, failure.slot, failure.error
                        );
                        return Ok(json_rpc_internal_error_response(id));
                    }
                    Err(join_err) => {
                        error!("Failed to join hydration task: {}", join_err);
                        return Ok(json_rpc_internal_error_response(id));
                    }
                };

                route.source_head_cache();
                route.success();
                return Ok(json_rpc_success_response(id, result));
            }
        }
    }

    // Disk tier: eligible only when every signature-shaped bound resolved to a
    // position (otherwise ClickHouse's NULL-tolerant SQL semantics must apply),
    // and — for ascending pages, which start from the oldest row — when the
    // whole eligible range lies inside the contiguous covered span.
    #[cfg(feature = "disk-cache")]
    let disk_page = 'disk: {
        let Some(disk) = state.disk_cache.as_ref() else {
            break 'disk None;
        };

        let pagination_position = match query.pagination.as_ref() {
            None => None,
            Some(PaginationToken::SlotIndex { slot, idx }) => Some(SignatureSlot {
                slot: *slot,
                slot_idx: *idx,
            }),
            Some(PaginationToken::Signature(_)) => match query.resolved_pagination {
                Some(position) => Some(position),
                None => break 'disk None,
            },
        };

        let resolved_filter = match query.signature_filter.as_ref() {
            None => None,
            Some(filter) => {
                let resolved = query.resolved_signature_filter.clone().unwrap_or_default();
                let fully_resolved = [
                    (filter.gte.is_some(), resolved.gte.is_some()),
                    (filter.gt.is_some(), resolved.gt.is_some()),
                    (filter.lte.is_some(), resolved.lte.is_some()),
                    (filter.lt.is_some(), resolved.lt.is_some()),
                ]
                .iter()
                .all(|(requested, resolved)| !requested || *resolved);
                if !fully_resolved {
                    break 'disk None;
                }
                Some(resolved)
            }
        };

        if query.sort_order == SortOrder::Asc {
            let Some((floor, _)) = disk.tip_span() else {
                break 'disk None;
            };
            let mut lower_bound: Option<u64> = pagination_position.map(|position| position.slot);
            if let Some(filter) = query.slot_filter.as_ref() {
                for bound in [filter.gt.map(|v| v.saturating_add(1)), filter.gte]
                    .into_iter()
                    .flatten()
                {
                    lower_bound = Some(lower_bound.map_or(bound, |low| low.max(bound)));
                }
            }
            if let Some(resolved) = resolved_filter.as_ref() {
                for position in [resolved.gt, resolved.gte].into_iter().flatten() {
                    lower_bound =
                        Some(lower_bound.map_or(position.slot, |low| low.max(position.slot)));
                }
            }
            // An ascending page that starts below the floor must come from
            // ClickHouse in full: its oldest rows lead the page.
            if lower_bound.is_none_or(|low| low < floor) {
                break 'disk None;
            }
        }

        route.disk_cache_read();
        disk.transactions_for_address(
            address_pubkey,
            crate::disk_cache::index::DiskTfaQuery {
                limit: limit as usize,
                sort_order: query.sort_order,
                pagination: pagination_position,
                slot_filter: query.slot_filter.clone(),
                block_time_filter: query.block_time_filter.clone(),
                signature_filter: resolved_filter,
                status: query.status,
                token_accounts: query.token_accounts,
            },
        )
        .await
    };

    #[cfg(feature = "disk-cache")]
    let (skip_clickhouse, clickhouse_query) = match disk_page.as_ref() {
        Some(page) if !page.reached_floor => (true, None),
        Some(page) => {
            // ClickHouse owes only the remainder strictly below the floor.
            let mut remainder = query.clone();
            let slot_filter = remainder.slot_filter.get_or_insert_with(Default::default);
            slot_filter.lt = Some(slot_filter.lt.map_or(page.floor, |lt| lt.min(page.floor)));
            remainder.limit = limit - page.records.len() as u64;
            (false, Some(remainder))
        }
        None => (false, None),
    };
    #[cfg(not(feature = "disk-cache"))]
    let (skip_clickhouse, clickhouse_query): (bool, Option<TransactionsForAddressQuery>) =
        (false, None);

    let (signature_records, mut timings) = if skip_clickhouse {
        (Vec::new(), QueryTimings::zero())
    } else {
        route.source_clickhouse();
        match state
            .clickhouse
            .get_transactions_for_address_signatures(clickhouse_query.as_ref().unwrap_or(&query))
            .await
        {
            Ok(result) => result,
            Err(e) => {
                metrics::backend_error("get_transactions_for_address_signatures");
                error!("Failed to query ClickHouse: {}", e);
                return Ok(json_rpc_internal_error_response(id));
            }
        }
    };
    timings.add(prequery_timings);
    #[cfg(feature = "grpc-head-cache")]
    let clickhouse_records_count = signature_records.len();

    let merged_records = signature_records
        .into_iter()
        .map(|record| MergedRecord {
            source: RecordSource::ClickHouse,
            signature: record.signature,
            slot: record.slot,
            slot_idx: record.slot_idx,
            err: record.err,
            memo: record.memo,
            block_time: record.block_time,
            confirmation_status: "finalized".to_string(),
        })
        .collect::<Vec<_>>();

    #[cfg(feature = "grpc-head-cache")]
    let mut merged_records = merged_records;
    #[cfg(feature = "grpc-head-cache")]
    let mut merged_has_head = false;

    #[cfg(feature = "disk-cache")]
    let mut merged_has_disk = false;
    #[cfg(feature = "disk-cache")]
    if let Some(page) = disk_page {
        let mut seen = HashSet::with_capacity(merged_records.len() + page.records.len());
        for record in merged_records.iter() {
            seen.insert(record.signature.clone());
        }
        for record in page.records {
            if !seen.insert(record.signature.clone()) {
                continue;
            }
            merged_has_disk = true;
            merged_records.push(MergedRecord {
                source: RecordSource::Disk,
                signature: record.signature,
                slot: record.slot,
                slot_idx: record.slot_idx,
                err: record.err,
                memo: record.memo,
                block_time: record.block_time,
                confirmation_status: "finalized".to_string(),
            });
        }
        if merged_has_disk {
            merged_records.sort_unstable_by(|a, b| match query.sort_order {
                SortOrder::Desc => b
                    .slot
                    .cmp(&a.slot)
                    .then_with(|| b.slot_idx.cmp(&a.slot_idx))
                    .then_with(|| b.signature.cmp(&a.signature)),
                SortOrder::Asc => a
                    .slot
                    .cmp(&b.slot)
                    .then_with(|| a.slot_idx.cmp(&b.slot_idx))
                    .then_with(|| a.signature.cmp(&b.signature)),
            });
            merged_records.truncate(limit as usize);
        }
    }
    #[cfg(all(feature = "grpc-head-cache", not(feature = "disk-cache")))]
    let merged_has_disk = false;

    #[cfg(feature = "grpc-head-cache")]
    if let Some(cache) = head_cache {
        let head_merge_eligible = query.token_accounts == TokenAccountsFilter::None
            && query.signature_filter.is_none()
            && query.block_time_filter.is_none();

        if head_merge_eligible {
            let pagination_is_signature = matches!(
                query.pagination.as_ref(),
                Some(PaginationToken::Signature(_))
            );

            let (head_before, head_until) = match query.pagination.as_ref() {
                None => (None, None),
                Some(PaginationToken::SlotIndex { slot, idx }) => match query.sort_order {
                    SortOrder::Desc => (
                        Some(SlotBoundary::Position(SignatureSlot {
                            slot: *slot,
                            slot_idx: *idx,
                        })),
                        None,
                    ),
                    SortOrder::Asc => (
                        None,
                        Some(SlotBoundary::Position(SignatureSlot {
                            slot: *slot,
                            slot_idx: *idx,
                        })),
                    ),
                },
                Some(PaginationToken::Signature(_)) => (None, None),
            };

            let mut head_metas = if pagination_is_signature {
                Vec::new()
            } else {
                route.head_cache_read();
                cache.signatures_for_address(
                    &address_pubkey,
                    head_before,
                    head_until,
                    limit as usize,
                    min_commitment,
                )
            };

            if let Some(filter) = query.slot_filter.as_ref() {
                head_metas.retain(|meta| slot_filter_matches(meta.pos.slot, filter));
            }
            match query.status {
                TransactionStatusFilter::Succeeded => {
                    head_metas.retain(|meta| meta.err.is_none());
                }
                TransactionStatusFilter::Failed => {
                    head_metas.retain(|meta| meta.err.is_some());
                }
                TransactionStatusFilter::Any => {}
            }

            let mut seen = HashSet::with_capacity(merged_records.len() + head_metas.len());
            for record in merged_records.iter() {
                seen.insert(record.signature.clone());
            }

            for meta in head_metas {
                let signature = meta.signature_str.to_string();
                if !seen.insert(signature.clone()) {
                    continue;
                }
                merged_has_head = true;
                merged_records.push(MergedRecord {
                    source: RecordSource::Head,
                    signature,
                    slot: meta.pos.slot,
                    slot_idx: meta.pos.idx,
                    err: meta.err.clone(),
                    memo: meta.memo.clone(),
                    block_time: meta.block_time,
                    confirmation_status: cache
                        .confirmation_status_string(meta.pos.slot)
                        .to_string(),
                });
            }

            merged_records.sort_unstable_by(|a, b| match query.sort_order {
                SortOrder::Desc => b
                    .slot
                    .cmp(&a.slot)
                    .then_with(|| b.slot_idx.cmp(&a.slot_idx))
                    .then_with(|| b.signature.cmp(&a.signature)),
                SortOrder::Asc => a
                    .slot
                    .cmp(&b.slot)
                    .then_with(|| a.slot_idx.cmp(&b.slot_idx))
                    .then_with(|| a.signature.cmp(&b.signature)),
            });
            merged_records.truncate(limit as usize);
        }
    }

    if transaction_details == TransactionsForAddressDetails::Signatures {
        #[cfg(feature = "grpc-head-cache")]
        let pagination_token = merged_records.last().map(|record| match record.source {
            RecordSource::ClickHouse => record.signature.clone(),
            RecordSource::Head => format!("{}:{}", record.slot, record.slot_idx),
            #[cfg(feature = "disk-cache")]
            RecordSource::Disk => format!("{}:{}", record.slot, record.slot_idx),
        });

        #[cfg(not(feature = "grpc-head-cache"))]
        let pagination_token = merged_records.last().map(|record| record.signature.clone());

        let data = merged_records
            .into_iter()
            .map(|record| TransactionsForAddressSignaturesInfo {
                signature: record.signature,
                slot: record.slot,
                transaction_index: record.slot_idx,
                err: record.err,
                memo: record.memo,
                block_time: record.block_time,
                confirmation_status: Some(record.confirmation_status),
            })
            .collect::<Vec<_>>();

        let result = TransactionsForAddressResult {
            data,
            pagination_token,
        };

        #[cfg(feature = "grpc-head-cache")]
        if clickhouse_records_count == 0 && merged_has_disk {
            #[cfg(feature = "disk-cache")]
            route.source_disk_cache();
        } else if clickhouse_records_count == 0 && merged_has_head {
            route.source_head_cache();
        } else {
            route.source_clickhouse();
        }
        #[cfg(not(feature = "grpc-head-cache"))]
        route.source_clickhouse();
        route.success();
        let mut resp = json_rpc_success_response(id, result);
        add_downstream_header(&mut resp, &timings);
        return Ok(resp);
    }

    let encoding = match options
        .encoding
        .as_deref()
        .unwrap_or("json")
        .to_lowercase()
        .as_str()
    {
        "json" => UiTransactionEncoding::Json,
        "jsonparsed" => UiTransactionEncoding::JsonParsed,
        "base58" => UiTransactionEncoding::Base58,
        "base64" => UiTransactionEncoding::Base64,
        other => {
            route.invalid_params();
            return Ok(json_rpc_error_response(
                id,
                -32602,
                format!("Invalid params: unsupported encoding '{other}'"),
                None,
            ));
        }
    };

    let max_supported_transaction_version = options.max_supported_transaction_version;

    #[cfg(feature = "disk-cache")]
    let mut disk_transaction_map: HashMap<
        String,
        Arc<crate::clickhouse::StoredTransactionRecord>,
    > = {
        let disk_records: Vec<(u64, u32, String)> = merged_records
            .iter()
            .filter(|record| record.source == RecordSource::Disk)
            .map(|record| (record.slot, record.slot_idx, record.signature.clone()))
            .collect();
        if disk_records.is_empty() {
            HashMap::new()
        } else {
            let disk = state
                .disk_cache
                .as_ref()
                .expect("disk records imply disk cache");
            let positions: Vec<(u64, u32)> = disk_records
                .iter()
                .map(|(slot, idx, _)| (*slot, *idx))
                .collect();
            let fetched = disk.get_txs_by_position(positions).await;
            disk_records
                .into_iter()
                .zip(fetched)
                .filter_map(|((_, _, signature), record)| {
                    record.map(|record| (signature, Arc::new(record)))
                })
                .collect()
        }
    };

    // Disk-sourced rows whose full record vanished (eviction race) are fetched
    // from ClickHouse with the rest, so the page never silently drops entries.
    let signature_pairs = merged_records
        .iter()
        .filter(|record| match record.source {
            RecordSource::ClickHouse => true,
            #[cfg(feature = "grpc-head-cache")]
            RecordSource::Head => false,
            #[cfg(feature = "disk-cache")]
            RecordSource::Disk => !disk_transaction_map.contains_key(&record.signature),
        })
        .map(|record| (record.slot, record.signature.clone()))
        .collect::<Vec<_>>();

    let (transactions, tx_timings) = if signature_pairs.is_empty() {
        (Vec::new(), crate::clickhouse::QueryTimings::zero())
    } else {
        route.source_clickhouse();
        match state
            .clickhouse
            .get_transactions_by_slot_signatures(
                &signature_pairs,
                max_supported_transaction_version,
            )
            .await
        {
            Ok(result) => result,
            Err(e) => {
                metrics::backend_error("get_transactions_by_slot_signatures");
                error!("Failed to query ClickHouse for full transactions: {}", e);
                let mut resp = json_rpc_internal_error_response(id);
                add_downstream_header(&mut resp, &timings);
                return Ok(resp);
            }
        }
    };

    let mut transaction_map = HashMap::new();
    for record in transactions {
        let signature = bs58::encode(record.signature).into_string();
        transaction_map.insert(signature, Arc::new(record));
    }

    struct HydrationInput {
        slot: u64,
        slot_idx: u32,
        block_time: Option<i64>,
        signature: String,
        /// Position-shaped pagination token (cache-sourced records have no
        /// ClickHouse signature cursor).
        #[cfg(feature = "grpc-head-cache")]
        position_token: bool,
        stored: Arc<crate::clickhouse::StoredTransactionRecord>,
    }

    let mut inputs = Vec::with_capacity(merged_records.len());
    for record in merged_records {
        #[cfg(feature = "grpc-head-cache")]
        let position_token = record.source != RecordSource::ClickHouse;
        let stored = match record.source {
            RecordSource::ClickHouse => {
                let Some(stored) = transaction_map.remove(&record.signature) else {
                    continue;
                };
                stored
            }
            #[cfg(feature = "grpc-head-cache")]
            RecordSource::Head => {
                route.head_cache_read();
                let Some(cache) = head_cache else {
                    continue;
                };
                let Ok(sig) = Signature::from_str(&record.signature) else {
                    continue;
                };
                let Some(stored) = cache.get_tx(&sig, min_commitment) else {
                    continue;
                };
                stored
            }
            #[cfg(feature = "disk-cache")]
            RecordSource::Disk => {
                match disk_transaction_map
                    .remove(&record.signature)
                    .or_else(|| transaction_map.remove(&record.signature))
                {
                    Some(stored) => stored,
                    None => continue,
                }
            }
        };

        inputs.push(HydrationInput {
            slot: record.slot,
            slot_idx: record.slot_idx,
            block_time: record.block_time,
            signature: record.signature,
            #[cfg(feature = "grpc-head-cache")]
            position_token,
            stored,
        });
    }

    struct HydrationFailure {
        signature: String,
        slot: u64,
        error: TransactionHydrationError,
    }

    let permit = match state.hydration_sem.clone().acquire_owned().await {
        Ok(permit) => permit,
        Err(_) => {
            error!(address, "Hydration semaphore closed");
            let mut resp = json_rpc_internal_error_response(id);
            add_downstream_header(&mut resp, &timings);
            return Ok(resp);
        }
    };

    let hydrated = match tokio::task::spawn_blocking(move || {
        let _permit = permit;

        let mut data = Vec::with_capacity(inputs.len());
        let mut pagination_token = None;
        for input in inputs {
            let encoded = match hydrate_transaction_record(
                input.stored.as_ref(),
                encoding,
                max_supported_transaction_version,
            ) {
                Ok(encoded) => encoded,
                Err(error) => {
                    return Err(HydrationFailure {
                        signature: input.signature,
                        slot: input.slot,
                        error,
                    });
                }
            };

            data.push(TransactionsForAddressFullInfo {
                slot: input.slot,
                transaction_index: input.slot_idx,
                block_time: input.block_time,
                transaction: encoded.transaction.transaction,
                meta: encoded.transaction.meta,
                version: encoded.transaction.version,
            });

            #[cfg(feature = "grpc-head-cache")]
            {
                if input.position_token {
                    pagination_token = Some(format!("{}:{}", input.slot, input.slot_idx));
                } else {
                    pagination_token = Some(input.signature);
                }
            }
            #[cfg(not(feature = "grpc-head-cache"))]
            {
                pagination_token = Some(input.signature);
            }
        }

        Ok::<_, HydrationFailure>((data, pagination_token))
    })
    .await
    {
        Ok(Ok(output)) => output,
        Ok(Err(failure)) => {
            error!(
                "Failed to hydrate transaction {} in slot {}: {}",
                failure.signature, failure.slot, failure.error
            );
            let mut resp = json_rpc_internal_error_response(id);
            add_downstream_header(&mut resp, &timings);
            return Ok(resp);
        }
        Err(join_err) => {
            error!("Failed to join hydration task: {}", join_err);
            let mut resp = json_rpc_internal_error_response(id);
            add_downstream_header(&mut resp, &timings);
            return Ok(resp);
        }
    };

    let (data, pagination_token) = hydrated;

    let result = TransactionsForAddressResult {
        data,
        pagination_token,
    };

    #[cfg(feature = "grpc-head-cache")]
    if signature_pairs.is_empty() && merged_has_disk {
        #[cfg(feature = "disk-cache")]
        route.source_disk_cache();
    } else if signature_pairs.is_empty() && merged_has_head {
        route.source_head_cache();
    } else {
        route.source_clickhouse();
    }
    #[cfg(not(feature = "grpc-head-cache"))]
    route.source_clickhouse();
    route.success();
    let mut combined_timings = timings;
    combined_timings.add(tx_timings);
    let mut resp = json_rpc_success_response(id, result);
    add_downstream_header(&mut resp, &combined_timings);
    Ok(resp)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_get_transaction_config_accepts_slot_extension() {
        let parsed = parse_get_transaction_config_value(json!({
            "encoding": "base64",
            "maxSupportedTransactionVersion": 0,
            "slot": 42
        }))
        .expect("config should parse");

        let config = parsed.wrapper.convert_to_current();
        assert_eq!(parsed.slot, Some(42));
        assert_eq!(config.encoding, Some(UiTransactionEncoding::Base64));
        assert_eq!(config.max_supported_transaction_version, Some(0));
    }

    #[test]
    fn parse_get_transaction_config_treats_null_slot_as_absent() {
        let parsed = parse_get_transaction_config_value(json!({
            "encoding": "json",
            "slot": null
        }))
        .expect("config should parse");

        assert_eq!(parsed.slot, None);
    }

    #[test]
    fn parse_get_transaction_config_rejects_invalid_slot_type() {
        let err = parse_get_transaction_config_value(json!({
            "encoding": "json",
            "slot": "42"
        }))
        .expect_err("slot string should fail");

        assert_eq!(err, "Invalid params: slot must be a number");
    }

    #[test]
    fn parse_get_transaction_config_still_rejects_unknown_fields() {
        let err = parse_get_transaction_config_value(json!({
            "slot": 42,
            "unexpected": true
        }))
        .expect_err("unknown field should fail");

        assert!(err.contains("unexpected"));
    }

    #[test]
    fn get_transactions_for_address_slot_aliases_populate_slot_filter() {
        let mut filters = GetTransactionsForAddressFilters::default();

        apply_get_transactions_for_address_slot_aliases(&mut filters, Some(100), Some(50))
            .expect("aliases should apply");

        let slot_filter = filters.slot.expect("slot filter");
        assert_eq!(slot_filter.lt, Some(100));
        assert_eq!(slot_filter.gt, Some(50));
    }

    #[test]
    fn get_transactions_for_address_before_slot_rejects_upper_bound_conflict() {
        let mut filters = GetTransactionsForAddressFilters {
            slot: Some(crate::handlers::types::ComparisonFilter {
                lte: Some(100),
                ..Default::default()
            }),
            ..Default::default()
        };

        let err = apply_get_transactions_for_address_slot_aliases(&mut filters, Some(100), None)
            .expect_err("conflicting upper bound should fail");

        assert_eq!(
            err,
            "Invalid params: beforeSlot cannot be combined with filters.slot.lt or filters.slot.lte"
        );
    }

    #[test]
    fn get_transactions_for_address_until_slot_rejects_lower_bound_conflict() {
        let mut filters = GetTransactionsForAddressFilters {
            slot: Some(crate::handlers::types::ComparisonFilter {
                gte: Some(50),
                ..Default::default()
            }),
            ..Default::default()
        };

        let err = apply_get_transactions_for_address_slot_aliases(&mut filters, None, Some(50))
            .expect_err("conflicting lower bound should fail");

        assert_eq!(
            err,
            "Invalid params: untilSlot cannot be combined with filters.slot.gt or filters.slot.gte"
        );
    }
}
