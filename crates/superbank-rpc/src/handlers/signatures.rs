// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use axum::{http::StatusCode, response::Response};
use serde_json::{Value, json};
use solana_commitment_config::CommitmentLevel;
use solana_sdk::{pubkey::Pubkey, signature::Signature};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use tracing::{error, warn};

use crate::clickhouse::{SignatureRecord, SignatureStatusRecord, SlotBoundary};
use crate::handlers::{
    RouteMetric,
    types::{
        GetSignatureStatusesConfig, GetSignaturesForAddressOptions, RpcContextSlot, SignatureInfo,
        SignatureStatusInfo, SignatureStatusesResult,
    },
};
use crate::metrics;
use crate::rpc::{
    json_rpc_error_response, json_rpc_filter_transaction_not_found_response,
    json_rpc_internal_error_response, json_rpc_long_term_storage_unreachable_response,
    json_rpc_node_unhealthy_response, json_rpc_success_response,
};
use crate::state::{AppState, LatestSlotSource};
use crate::util::add_downstream_header;

#[cfg(feature = "grpc-head-cache")]
use crate::clickhouse::SignatureSlot;

#[cfg(feature = "grpc-head-cache")]
fn head_status_confirmations(
    slot: u64,
    context_slot: u64,
    confirmation_status: &str,
) -> Option<u64> {
    match confirmation_status {
        "finalized" => None,
        "confirmed" => Some(context_slot.saturating_sub(slot).max(1)),
        "processed" => Some(0),
        _ => Some(context_slot.saturating_sub(slot)),
    }
}

pub(crate) async fn handle_get_signature_statuses(
    state: Arc<AppState>,
    id: Value,
    params: Option<Vec<Value>>,
) -> Result<Response, StatusCode> {
    let mut route = RouteMetric::for_state("getSignatureStatuses", state.as_ref());

    let Some(params) = params.filter(|v| !v.is_empty()) else {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            "Invalid params: missing signatures",
            None,
        ));
    };

    let signatures_value = match params.first() {
        Some(value) => value,
        None => {
            route.invalid_params();
            return Ok(json_rpc_error_response(
                id,
                -32602,
                "Invalid params: missing signatures",
                None,
            ));
        }
    };

    let signatures = match signatures_value.as_array() {
        Some(array) => array,
        None => {
            route.invalid_params();
            return Ok(json_rpc_error_response(
                id,
                -32602,
                "Invalid params: signatures must be an array",
                None,
            ));
        }
    };

    if signatures.is_empty() {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            "Invalid params: signatures list must be non-empty",
            None,
        ));
    }

    if signatures.len() > 256 {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            "Invalid params: too many signatures (max 256)",
            None,
        ));
    }

    if let Some(config_value) = params.get(1).filter(|value| !value.is_null())
        && serde_json::from_value::<GetSignatureStatusesConfig>(config_value.clone()).is_err()
    {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            "Invalid params: failed to parse config",
            None,
        ));
    }

    let mut inputs: Vec<Option<String>> = Vec::with_capacity(signatures.len());
    let mut unique_valid: Vec<String> = Vec::new();
    let mut seen = HashSet::new();

    for value in signatures {
        let Some(sig_str) = value.as_str() else {
            inputs.push(None);
            continue;
        };

        if Signature::from_str(sig_str).is_err() {
            inputs.push(None);
            continue;
        }

        let sig_string = sig_str.to_string();
        if seen.insert(sig_string.clone()) {
            unique_valid.push(sig_string.clone());
        }
        inputs.push(Some(sig_string));
    }

    let (context_slot, context_source) = match state
        .resolve_latest_slot_with_source(
            "get_signature_statuses_context",
            CommitmentLevel::Finalized,
        )
        .await
    {
        Ok(result) => result,
        Err(e) => {
            metrics::backend_error("get_latest_finalized_slot");
            error!("Failed to fetch latest slot for signature statuses: {}", e);
            return Ok(json_rpc_internal_error_response(id));
        }
    };
    match context_source {
        LatestSlotSource::ClickHouse => route.source_clickhouse(),
        #[cfg(feature = "grpc-head-cache")]
        LatestSlotSource::HeadCache => route.source_head_cache(),
    }

    #[cfg(feature = "grpc-head-cache")]
    let (head_statuses, context_slot) = {
        if let Some(cache) = state.head_cache.as_ref() {
            route.head_cache_read();
            let mut head = HashMap::new();
            for sig_str in unique_valid.iter() {
                let Ok(sig) = Signature::from_str(sig_str) else {
                    continue;
                };
                if let Some(meta) = cache.get_meta(&sig, CommitmentLevel::Processed) {
                    head.insert(
                        sig_str.clone(),
                        (
                            meta.pos.slot,
                            meta.err.clone(),
                            cache.confirmation_status_string(meta.pos.slot),
                        ),
                    );
                }
            }
            let ctx = context_slot.max(cache.latest_slot());
            (head, ctx)
        } else {
            (HashMap::new(), context_slot)
        }
    };

    // Disk tier: finalized statuses for signatures the head cache does not hold.
    // A disk miss proves nothing (the signature may predate the window), so the
    // remainder still goes to ClickHouse.
    #[cfg(feature = "disk-cache")]
    let disk_statuses: HashMap<String, (u64, Option<Value>)> =
        if let Some(disk) = state.disk_cache.as_ref() {
            let pending: Vec<(String, Signature)> = unique_valid
                .iter()
                .filter(|sig_str| !head_statuses.contains_key(*sig_str))
                .filter_map(|sig_str| {
                    Signature::from_str(sig_str)
                        .ok()
                        .map(|sig| (sig_str.clone(), sig))
                })
                .collect();
            if pending.is_empty() {
                HashMap::new()
            } else {
                route.disk_cache_read();
                let signatures: Vec<Signature> = pending.iter().map(|(_, sig)| *sig).collect();
                let statuses = disk.get_sig_statuses(signatures).await;
                pending
                    .into_iter()
                    .zip(statuses)
                    .filter_map(|((sig_str, _), status)| {
                        status.map(|status| {
                            let err = status
                                .err
                                .and_then(|raw| crate::clickhouse::parse_err_json(&sig_str, raw));
                            (sig_str, (status.slot, err))
                        })
                    })
                    .collect()
            }
        } else {
            HashMap::new()
        };

    #[cfg(feature = "disk-cache")]
    let in_disk = |sig: &String| disk_statuses.contains_key(sig);
    #[cfg(not(feature = "disk-cache"))]
    let in_disk = |_sig: &String| false;

    let mut timings: Option<crate::clickhouse::QueryTimings> = None;
    let status_map: HashMap<String, SignatureStatusRecord> = if unique_valid.is_empty() {
        HashMap::new()
    } else {
        #[cfg(feature = "grpc-head-cache")]
        let to_query = unique_valid
            .iter()
            .filter(|sig| !head_statuses.contains_key(*sig) && !in_disk(sig))
            .cloned()
            .collect::<Vec<_>>();
        #[cfg(not(feature = "grpc-head-cache"))]
        let to_query: Vec<String> = unique_valid
            .iter()
            .filter(|sig| !in_disk(sig))
            .cloned()
            .collect();

        if to_query.is_empty() {
            HashMap::new()
        } else {
            route.source_clickhouse();
            match state.clickhouse.get_signature_statuses(&to_query).await {
                Ok((records, query_timings)) => {
                    timings = Some(query_timings);
                    records
                        .into_iter()
                        .map(|record| (record.signature.clone(), record))
                        .collect()
                }
                Err(e) => {
                    metrics::backend_error("get_signature_statuses");
                    error!("Failed to query ClickHouse for signature statuses: {}", e);
                    return Ok(json_rpc_internal_error_response(id));
                }
            }
        }
    };

    let mut value = Vec::with_capacity(inputs.len());
    let mut has_clickhouse_match = false;
    #[cfg(feature = "disk-cache")]
    let mut has_disk_match = false;
    #[cfg(feature = "grpc-head-cache")]
    let mut has_head_match = false;
    for maybe_sig in inputs {
        let Some(sig) = maybe_sig else {
            value.push(None);
            continue;
        };

        if let Some(record) = status_map.get(&sig) {
            has_clickhouse_match = true;
            let err_value = record.err.clone();
            let status_value = match &err_value {
                Some(err) => json!({ "Err": err }),
                None => json!({ "Ok": Value::Null }),
            };

            value.push(Some(SignatureStatusInfo {
                slot: record.slot,
                confirmations: None,
                err: err_value,
                status: status_value,
                confirmation_status: "finalized".to_string(),
            }));
            continue;
        }

        #[cfg(feature = "disk-cache")]
        if let Some((slot, err)) = disk_statuses.get(&sig) {
            has_disk_match = true;
            let status_value = match err {
                Some(err) => json!({ "Err": err }),
                None => json!({ "Ok": Value::Null }),
            };

            value.push(Some(SignatureStatusInfo {
                slot: *slot,
                confirmations: None,
                err: err.clone(),
                status: status_value,
                confirmation_status: "finalized".to_string(),
            }));
            continue;
        }

        #[cfg(feature = "grpc-head-cache")]
        if let Some((slot, err, confirmation_status)) = head_statuses.get(&sig) {
            has_head_match = true;
            let status_value = match &err {
                Some(err) => json!({ "Err": err }),
                None => json!({ "Ok": Value::Null }),
            };

            value.push(Some(SignatureStatusInfo {
                slot: *slot,
                confirmations: head_status_confirmations(*slot, context_slot, confirmation_status),
                err: err.clone(),
                status: status_value,
                confirmation_status: confirmation_status.to_string(),
            }));
            continue;
        }

        value.push(None);
    }

    if has_clickhouse_match {
        route.source_clickhouse();
    }
    #[cfg(all(feature = "grpc-head-cache", feature = "disk-cache"))]
    let disk_matched = has_disk_match;
    #[cfg(all(feature = "grpc-head-cache", not(feature = "disk-cache")))]
    let disk_matched = false;
    #[cfg(feature = "disk-cache")]
    if !has_clickhouse_match && has_disk_match {
        route.source_disk_cache();
    }
    #[cfg(feature = "grpc-head-cache")]
    if !has_clickhouse_match && !disk_matched && has_head_match {
        route.source_head_cache();
    }

    let result = SignatureStatusesResult {
        context: RpcContextSlot { slot: context_slot },
        value,
    };

    let mut resp = json_rpc_success_response(id, result);
    if let Some(query_timings) = timings {
        add_downstream_header(&mut resp, &query_timings);
    }
    route.success();
    Ok(resp)
}

/// Tighter of the caller's `before` bound and the disk coverage floor: the
/// ClickHouse remainder must stay strictly below the floor (the disk page has
/// already evaluated everything at or above it).
#[cfg(feature = "disk-cache")]
fn clamp_before_to_floor(before: Option<SlotBoundary>, floor: u64) -> SlotBoundary {
    match before {
        None => SlotBoundary::Slot(floor),
        Some(SlotBoundary::Slot(slot)) => SlotBoundary::Slot(slot.min(floor)),
        Some(SlotBoundary::Position(position)) if position.slot < floor => {
            SlotBoundary::Position(position)
        }
        Some(SlotBoundary::Position(_)) => SlotBoundary::Slot(floor),
    }
}

pub(crate) async fn handle_get_signatures_for_address(
    state: Arc<AppState>,
    id: Value,
    params: Option<Vec<Value>>,
) -> Result<Response, StatusCode> {
    let mut route = RouteMetric::for_state("getSignaturesForAddress", state.as_ref());

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

    // Validate address format to align with Solana error semantics
    if Pubkey::from_str(address).is_err() {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            "Invalid param: Invalid",
            None,
        ));
    }

    // Parse options if provided
    let options = if let Some(options_value) = params.first() {
        if options_value.is_null() {
            GetSignaturesForAddressOptions::default()
        } else {
            match serde_json::from_value::<GetSignaturesForAddressOptions>(options_value.clone()) {
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
    } else {
        GetSignaturesForAddressOptions::default()
    };

    // Default limit to max_signatures_limit if not specified and reject zero requests
    let requested_limit = options.limit.unwrap_or(state.max_signatures_limit);

    if requested_limit == 0 {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            format!("Invalid limit; max {}", state.max_signatures_limit),
            None,
        ));
    }

    let limit = requested_limit.min(state.max_signatures_limit);

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

    if let Some(before) = options.before.as_deref()
        && Signature::from_str(before).is_err()
    {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            "Invalid params: before must be a valid signature",
            None,
        ));
    }
    if let Some(until) = options.until.as_deref()
        && Signature::from_str(until).is_err()
    {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            "Invalid params: until must be a valid signature",
            None,
        ));
    }
    if options.before.is_some() && options.before_slot.is_some() {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            "Invalid params: before and beforeSlot are mutually exclusive",
            None,
        ));
    }
    if options.until.is_some() && options.until_slot.is_some() {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            "Invalid params: until and untilSlot are mutually exclusive",
            None,
        ));
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
                "get_signatures_for_address_min_context",
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
                route.rpc_error();
                return Ok(json_rpc_node_unhealthy_response(id));
            }
        };
        match context_source {
            LatestSlotSource::ClickHouse => route.source_clickhouse(),
            #[cfg(feature = "grpc-head-cache")]
            LatestSlotSource::HeadCache => route.source_head_cache(),
        }

        if context_slot < min_context_slot {
            warn!(
                address = address,
                required_slot = min_context_slot,
                context_slot,
                "Minimum context slot has not been reached"
            );
            let code =
                solana_rpc_client_api::custom_error::JSON_RPC_SERVER_ERROR_MIN_CONTEXT_SLOT_NOT_REACHED
                    as i32;
            route.rpc_error();
            return Ok(json_rpc_error_response(
                id,
                code,
                "Minimum context slot has not been reached",
                Some(json!({ "contextSlot": context_slot })),
            ));
        }
    }

    #[cfg(feature = "grpc-head-cache")]
    if let Some(cache) = state.head_cache.as_ref() {
        route.head_cache_read();
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

        let address_pubkey = Pubkey::from_str(address).expect("validated address");

        let mut precheck_timings = crate::clickhouse::QueryTimings::zero();
        let mut before_boundary = options.before_slot.map(SlotBoundary::Slot);
        let mut until_boundary = options.until_slot.map(SlotBoundary::Slot);

        if let Some(sig_str) = options.before.as_deref() {
            let mut pos = None;
            if let Ok(sig) = Signature::from_str(sig_str)
                && let Some(head_pos) = cache.signature_position(&sig)
            {
                pos = Some(SignatureSlot {
                    slot: head_pos.slot,
                    slot_idx: head_pos.idx,
                });
            }

            #[cfg(feature = "disk-cache")]
            if pos.is_none()
                && let Some(disk) = state.disk_cache.as_ref()
                && let Ok(sig) = Signature::from_str(sig_str)
            {
                route.disk_cache_read();
                pos = disk.signature_position(sig).await;
            }

            if pos.is_none() {
                route.source_clickhouse();
                match state.clickhouse.get_signature_slot(sig_str).await {
                    Ok((pos_opt, timings)) => {
                        precheck_timings.add(timings);
                        pos = pos_opt;
                    }
                    Err(e) => {
                        metrics::backend_error("get_signature_slot");
                        error!("Failed to query ClickHouse for signature slot {sig_str}: {e}");
                        route.rpc_error();
                        return Ok(json_rpc_long_term_storage_unreachable_response(id));
                    }
                }
            }

            if pos.is_none() {
                route.rpc_error();
                return Ok(json_rpc_filter_transaction_not_found_response(id, sig_str));
            }
            before_boundary = pos.map(SlotBoundary::Position);
        }

        if let Some(sig_str) = options.until.as_deref() {
            if options.before.as_deref() == Some(sig_str) {
                until_boundary = before_boundary;
            } else {
                let mut pos = None;
                if let Ok(sig) = Signature::from_str(sig_str)
                    && let Some(head_pos) = cache.signature_position(&sig)
                {
                    pos = Some(SignatureSlot {
                        slot: head_pos.slot,
                        slot_idx: head_pos.idx,
                    });
                }

                #[cfg(feature = "disk-cache")]
                if pos.is_none()
                    && let Some(disk) = state.disk_cache.as_ref()
                    && let Ok(sig) = Signature::from_str(sig_str)
                {
                    route.disk_cache_read();
                    pos = disk.signature_position(sig).await;
                }

                if pos.is_none() {
                    route.source_clickhouse();
                    match state.clickhouse.get_signature_slot(sig_str).await {
                        Ok((pos_opt, timings)) => {
                            precheck_timings.add(timings);
                            pos = pos_opt;
                        }
                        Err(e) => {
                            metrics::backend_error("get_signature_slot");
                            error!("Failed to query ClickHouse for signature slot {sig_str}: {e}");
                            route.rpc_error();
                            return Ok(json_rpc_long_term_storage_unreachable_response(id));
                        }
                    }
                }

                if pos.is_none() {
                    route.rpc_error();
                    return Ok(json_rpc_filter_transaction_not_found_response(id, sig_str));
                }
                until_boundary = pos.map(SlotBoundary::Position);
            }
        }

        let head_metas = cache.signatures_for_address(
            &address_pubkey,
            before_boundary,
            until_boundary,
            limit as usize,
            min_commitment,
        );

        if head_metas.len() as u64 >= limit {
            route.source_head_cache();
            route.success();
            let signature_infos = head_metas
                .into_iter()
                .map(|meta| SignatureInfo {
                    signature: meta.signature_str.to_string(),
                    slot: meta.pos.slot,
                    err: meta.err.clone(),
                    memo: meta.memo.clone(),
                    block_time: meta.block_time,
                    confirmation_status: Some(
                        cache.confirmation_status_string(meta.pos.slot).to_string(),
                    ),
                })
                .collect::<Vec<_>>();
            return Ok(json_rpc_success_response(id, json!(signature_infos)));
        }

        #[derive(Debug)]
        struct MergedSignature {
            signature: String,
            slot: u64,
            slot_idx: u32,
            err: Option<Value>,
            memo: Option<String>,
            block_time: Option<i64>,
            confirmation_status: String,
        }

        // Disk tier: newest-first page over the contiguous covered span. The
        // page tells us whether ClickHouse still owes the remainder below the
        // coverage floor — and when it does, the ClickHouse bound is clamped
        // strictly below the floor so the tiers can never overlap.
        #[cfg(feature = "disk-cache")]
        let disk_page = match state.disk_cache.as_ref() {
            Some(disk) => {
                route.disk_cache_read();
                disk.signatures_for_address(
                    address_pubkey,
                    before_boundary,
                    until_boundary,
                    limit as usize,
                )
                .await
            }
            None => None,
        };

        #[cfg(feature = "disk-cache")]
        let (skip_clickhouse, clickhouse_before, clickhouse_limit) = match disk_page.as_ref() {
            Some(page) if !page.reached_floor => (true, before_boundary, 0),
            Some(page) => (
                false,
                Some(clamp_before_to_floor(before_boundary, page.floor)),
                limit - page.records.len() as u64,
            ),
            None => (false, before_boundary, limit),
        };
        #[cfg(not(feature = "disk-cache"))]
        let (skip_clickhouse, clickhouse_before, clickhouse_limit) =
            (false, before_boundary, limit);

        let (signatures, mut timings) = if skip_clickhouse {
            (Vec::new(), crate::clickhouse::QueryTimings::zero())
        } else {
            route.source_clickhouse();
            match state
                .clickhouse
                .get_signatures_for_address_with_positions(
                    address,
                    clickhouse_limit,
                    clickhouse_before,
                    until_boundary,
                )
                .await
            {
                Ok(result) => result,
                Err(e) => {
                    metrics::backend_error("get_signatures_for_address_with_positions");
                    error!("Failed to query ClickHouse: {}", e);
                    route.rpc_error();
                    return Ok(json_rpc_long_term_storage_unreachable_response(id));
                }
            }
        };

        timings.add(precheck_timings);

        let mut seen = HashSet::new();
        let mut merged = Vec::with_capacity(signatures.len() + head_metas.len());
        let clickhouse_contributed = !signatures.is_empty();
        for sig in signatures {
            seen.insert(sig.signature.clone());
            merged.push(MergedSignature {
                signature: sig.signature,
                slot: sig.slot,
                slot_idx: sig.slot_idx,
                err: sig.err,
                memo: sig.memo,
                block_time: sig.block_time,
                confirmation_status: "finalized".to_string(),
            });
        }

        #[cfg(feature = "disk-cache")]
        let disk_contributed = {
            let mut contributed = false;
            if let Some(page) = disk_page {
                for record in page.records {
                    if seen.insert(record.signature.clone()) {
                        contributed = true;
                        merged.push(MergedSignature {
                            signature: record.signature,
                            slot: record.slot,
                            slot_idx: record.slot_idx,
                            err: record.err,
                            memo: record.memo,
                            block_time: record.block_time,
                            confirmation_status: "finalized".to_string(),
                        });
                    }
                }
            }
            contributed
        };
        #[cfg(not(feature = "disk-cache"))]
        let disk_contributed = false;

        if !clickhouse_contributed {
            if disk_contributed {
                #[cfg(feature = "disk-cache")]
                route.source_disk_cache();
            } else if !head_metas.is_empty() {
                route.source_head_cache();
            }
        }

        for meta in head_metas {
            let sig = meta.signature_str.to_string();
            if seen.insert(sig.clone()) {
                merged.push(MergedSignature {
                    signature: sig,
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
        }

        merged.sort_unstable_by(|a, b| {
            b.slot
                .cmp(&a.slot)
                .then_with(|| b.slot_idx.cmp(&a.slot_idx))
                .then_with(|| b.signature.cmp(&a.signature))
        });
        merged.truncate(limit as usize);

        let signature_infos = merged
            .into_iter()
            .map(|sig| SignatureInfo {
                signature: sig.signature,
                slot: sig.slot,
                err: sig.err,
                memo: sig.memo,
                block_time: sig.block_time,
                confirmation_status: Some(sig.confirmation_status),
            })
            .collect::<Vec<_>>();

        route.success();
        let mut resp = json_rpc_success_response(id, signature_infos);
        add_downstream_header(&mut resp, &timings);
        return Ok(resp);
    }

    let mut before_boundary = options.before_slot.map(SlotBoundary::Slot);
    let mut until_boundary = options.until_slot.map(SlotBoundary::Slot);
    let mut precheck_timings = crate::clickhouse::QueryTimings::zero();

    if let Some(sig_str) = options.before.as_deref() {
        route.source_clickhouse();
        match state.clickhouse.get_signature_slot(sig_str).await {
            Ok((pos_opt, timings)) => {
                precheck_timings.add(timings);
                before_boundary = pos_opt.map(SlotBoundary::Position);
            }
            Err(e) => {
                metrics::backend_error("get_signature_slot");
                error!("Failed to query ClickHouse for signature slot {sig_str}: {e}");
                route.rpc_error();
                return Ok(json_rpc_long_term_storage_unreachable_response(id));
            }
        }

        if before_boundary.is_none() {
            route.rpc_error();
            return Ok(json_rpc_filter_transaction_not_found_response(id, sig_str));
        }
    }

    if let Some(sig_str) = options.until.as_deref() {
        if options.before.as_deref() == Some(sig_str) {
            until_boundary = before_boundary;
        } else {
            route.source_clickhouse();
            match state.clickhouse.get_signature_slot(sig_str).await {
                Ok((pos_opt, timings)) => {
                    precheck_timings.add(timings);
                    until_boundary = pos_opt.map(SlotBoundary::Position);
                }
                Err(e) => {
                    metrics::backend_error("get_signature_slot");
                    error!("Failed to query ClickHouse for signature slot {sig_str}: {e}");
                    route.rpc_error();
                    return Ok(json_rpc_long_term_storage_unreachable_response(id));
                }
            }

            if until_boundary.is_none() {
                route.rpc_error();
                return Ok(json_rpc_filter_transaction_not_found_response(id, sig_str));
            }
        }
    }

    // Query ClickHouse for signatures
    route.source_clickhouse();
    match state
        .clickhouse
        .get_signatures_for_address_with_positions(address, limit, before_boundary, until_boundary)
        .await
    {
        Ok((signatures, timings)) => {
            let mut timings = timings;
            timings.add(precheck_timings);
            let signature_infos: Vec<SignatureInfo> = signatures
                .into_iter()
                .map(|sig: SignatureRecord| SignatureInfo {
                    signature: sig.signature,
                    slot: sig.slot,
                    err: sig.err,
                    memo: sig.memo,
                    block_time: sig.block_time,
                    confirmation_status: Some("finalized".to_string()),
                })
                .collect();

            route.success();
            let mut resp = json_rpc_success_response(id, signature_infos);
            add_downstream_header(&mut resp, &timings);
            Ok(resp)
        }
        Err(e) => {
            metrics::backend_error("get_signatures_for_address_with_positions");
            error!("Failed to query ClickHouse: {}", e);
            route.rpc_error();
            Ok(json_rpc_long_term_storage_unreachable_response(id))
        }
    }
}
