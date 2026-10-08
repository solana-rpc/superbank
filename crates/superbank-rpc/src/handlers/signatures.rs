// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use crate::solana_sdk::{pubkey::Pubkey, signature::Signature};
use axum::{http::StatusCode, response::Response};
use serde_json::{Value, json};
use solana_commitment_config::CommitmentLevel;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use tracing::{error, warn};

use crate::clickhouse::{
    GsfaCursor, GsfaMissingCursor, InlineGsfaPage, SignatureRecord, SignatureSlot,
    SignatureStatusRecord, SlotBoundary,
};
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

    metrics::signature_status_batch_size("input", signatures.len());

    let search_transaction_history = match params.get(1).filter(|value| !value.is_null()) {
        Some(config_value) => {
            match serde_json::from_value::<GetSignatureStatusesConfig>(config_value.clone()) {
                Ok(config) => config.search_transaction_history.unwrap_or(false),
                Err(_) => {
                    route.invalid_params();
                    return Ok(json_rpc_error_response(
                        id,
                        -32602,
                        "Invalid params: failed to parse config",
                        None,
                    ));
                }
            }
        }
        None => false,
    };

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

    // The head's trusted finalized tip before its status lookup bounds what that lookup
    // can have seen (see `head_extends_disk`).
    #[cfg(all(feature = "disk-cache", feature = "grpc-head-cache"))]
    let head_tip_before_lookup = state
        .status_history_cache
        .enabled()
        .then(|| trusted_head_finalized_tip(&state))
        .flatten();
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
                            cache.confirmation_status_string(&meta),
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
    #[cfg(all(feature = "disk-cache", not(feature = "grpc-head-cache")))]
    let head_statuses: HashMap<String, (u64, Option<Value>, String)> = HashMap::new();

    // Disk tier: finalized recent statuses for signatures the head cache does not hold.
    // This is part of the default status-cache lookup; searchTransactionHistory
    // only controls the ClickHouse fallback below.
    #[cfg(feature = "disk-cache")]
    let mut history = HistoryAbsence::default();
    #[cfg(feature = "disk-cache")]
    let disk_statuses: HashMap<String, (u64, Option<Value>)> =
        if let Some(disk) = state.disk_cache() {
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
                let lookups = disk.get_sig_statuses_detailed(signatures).await;
                history.disk_span = lookups.span;
                pending
                    .into_iter()
                    .zip(lookups.statuses)
                    .filter_map(|((sig_str, sig), lookup)| {
                        if matches!(lookup, crate::disk_cache::DiskStatusLookup::Absent) {
                            history.disk_absent.insert(sig_str.clone(), sig);
                        }
                        lookup.found().map(|status| {
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
    let status_map: HashMap<String, SignatureStatusRecord> =
        if unique_valid.is_empty() || !search_transaction_history {
            HashMap::new()
        } else {
            #[cfg(feature = "grpc-head-cache")]
            let to_query = unique_valid
                .iter()
                .filter(|sig| !head_statuses.contains_key(*sig) && !in_disk(sig))
                .cloned()
                .collect::<Vec<_>>();
            // Drop signatures the primary recently had no row for, while the local
            // tiers still prove nothing has landed since.
            #[cfg(all(feature = "disk-cache", feature = "grpc-head-cache"))]
            let to_query = history
                .skip_known_absent(&state, head_tip_before_lookup, to_query)
                .await;
            #[cfg(not(feature = "grpc-head-cache"))]
            let to_query: Vec<String> = unique_valid
                .iter()
                .filter(|sig| !in_disk(sig))
                .cloned()
                .collect();

            metrics::signature_status_batch_size("primary_fallback", to_query.len());
            if to_query.is_empty() {
                HashMap::new()
            } else {
                route.source_clickhouse();
                match state.clickhouse.get_signature_statuses(&to_query).await {
                    Ok((records, query_timings)) => {
                        timings = Some(query_timings);
                        let records: HashMap<String, SignatureStatusRecord> = records
                            .into_iter()
                            .map(|record| (record.signature.clone(), record))
                            .collect();
                        #[cfg(all(feature = "disk-cache", feature = "grpc-head-cache"))]
                        history.remember_absent(&state, &to_query, &records).await;
                        records
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

/// What this request's local read proved absent, for the history absence cache.
#[cfg(feature = "disk-cache")]
#[cfg_attr(not(feature = "grpc-head-cache"), allow(dead_code))]
#[derive(Default)]
struct HistoryAbsence {
    /// Signatures the disk read proved absent from its covered span.
    disk_absent: HashMap<String, Signature>,
    /// The contiguous covered span `(floor, tip)` of a read that could prove absence.
    disk_span: Option<(u64, u64)>,
}

#[cfg(all(feature = "disk-cache", feature = "grpc-head-cache"))]
impl HistoryAbsence {
    /// Remove signatures whose empty primary answer still stands. Each must be absent
    /// from this request's contiguous disk span and from the head cache, whose verified
    /// chain must continue gaplessly from the disk tip to the trusted finalized tip, so
    /// any landing since the primary answered would have been seen locally.
    async fn skip_known_absent(
        &self,
        state: &AppState,
        head_tip_before_lookup: Option<u64>,
        to_query: Vec<String>,
    ) -> Vec<String> {
        let cache = &state.status_history_cache;
        if !cache.enabled() || to_query.is_empty() {
            return to_query;
        }
        let Some((floor, _)) = self
            .disk_span
            .filter(|&(_, tip)| head_extends_disk(state, head_tip_before_lookup, tip))
        else {
            metrics::signature_status_history_cache("bypass", to_query.len() as u64);
            return to_query;
        };
        let now = std::time::Instant::now();
        let queried = to_query.len();
        let mut remaining = Vec::with_capacity(queried);
        for sig in to_query {
            let absent = match self.disk_absent.get(&sig) {
                Some(signature) => cache.is_absent(signature, floor, now).await,
                None => false,
            };
            if !absent {
                remaining.push(sig);
            }
        }
        metrics::signature_status_history_cache("hit", (queried - remaining.len()) as u64);
        metrics::signature_status_history_cache("miss", remaining.len() as u64);
        remaining
    }

    /// Record every queried signature the primary had no row for and this request's
    /// disk read proved absent through its tip. Only a successful primary answer
    /// reaches here: errors return before any insertion.
    async fn remember_absent(
        &self,
        state: &AppState,
        queried: &[String],
        records: &HashMap<String, SignatureStatusRecord>,
    ) {
        let cache = &state.status_history_cache;
        let Some((_, tip)) = self.disk_span.filter(|_| cache.enabled()) else {
            return;
        };
        let now = std::time::Instant::now();
        let mut inserted = 0;
        for sig in queried {
            if records.contains_key(sig) {
                continue;
            }
            if let Some(signature) = self.disk_absent.get(sig) {
                cache.insert_absent(*signature, tip, now).await;
                inserted += 1;
            }
        }
        metrics::signature_status_history_cache("inserted", inserted);
    }
}

/// The head's finalized tip when its stream is connected, the tip moved recently, and
/// at least one parent edge below it is verified.
#[cfg(all(feature = "disk-cache", feature = "grpc-head-cache"))]
fn trusted_head_finalized_tip(state: &AppState) -> Option<u64> {
    let head = state.head_cache.as_ref()?;
    let proof = head.coverage.read().expect("head coverage lock").snapshot(
        0,
        None,
        CommitmentLevel::Finalized,
        std::time::Instant::now(),
    );
    proof.ok().map(|(tip, _)| tip)
}

/// Whether the head tier covers every slot after `disk_tip` that the primary can hold.
///
/// `tip_before_lookup` is the trusted finalized tip taken before this request's head
/// status lookup, so the lookup saw every block up to it. Now, after the lookup, the
/// head must still be connected with a fresh finalized tip, still retain the slot after
/// `disk_tip`, and its parent-verified chain must cover every slot from there to its
/// current tip. The skip then relies on the primary trailing the head stream: holding
/// nothing past `tip_before_lookup`. That is checked only against the last primary slot
/// this process happened to read (`latest_slot_cache`), a lower bound of the primary's
/// tip that may be stale or unset; a primary known to be further ahead disables the skip.
#[cfg(all(feature = "disk-cache", feature = "grpc-head-cache"))]
fn head_extends_disk(state: &AppState, tip_before_lookup: Option<u64>, disk_tip: u64) -> bool {
    let (Some(head), Some(lookup_tip)) = (state.head_cache.as_ref(), tip_before_lookup) else {
        return false;
    };
    let start = disk_tip.saturating_add(1);
    // Retention only rises, so the lookup also still held this slot.
    if start < head.min_retained_slot() {
        return false;
    }
    let proof = head.coverage.read().expect("head coverage lock").snapshot(
        start,
        None,
        CommitmentLevel::Finalized,
        std::time::Instant::now(),
    );
    // Primary slots are only ever added, so any value it once reported bounds its tip.
    let primary_floor = state
        .latest_slot_cache
        .value
        .load(std::sync::atomic::Ordering::Relaxed);
    matches!(proof, Ok((end, coverage))
        if primary_floor <= lookup_tip
            && lookup_tip <= end
            && coverage.gaps(start, end).is_empty())
}

/// The primary's getSignaturesForAddress page for the request's own bounds and limit.
#[cfg(feature = "disk-cache")]
pub(crate) type PrimaryGsfaPage = std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = crate::processing::ProcessingResult<(
                    Vec<SignatureRecord>,
                    crate::clickhouse::QueryTimings,
                )>,
            > + Send,
    >,
>;

#[cfg(feature = "disk-cache")]
fn primary_gsfa_page(
    state: &Arc<AppState>,
    address: &str,
    limit: u64,
    before: Option<SlotBoundary>,
    until: Option<SlotBoundary>,
) -> PrimaryGsfaPage {
    let state = state.clone();
    let address = address.to_string();
    Box::pin(async move {
        state
            .clickhouse
            .get_signatures_for_address_with_positions(&address, limit, before, until)
            .await
    })
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

/// `GSFA_RACE_PRIMARY=false`: the serial path from before the race. The local page is
/// awaited first; a complete page (`reached_floor` unset) replaces the primary, otherwise
/// the primary is asked only for the remainder strictly below the coverage floor. The
/// returned page may therefore be a short one whose rows the caller merges.
#[cfg(feature = "disk-cache")]
async fn serial_gsfa_page(
    state: &AppState,
    route: &mut RouteMetric,
    local: impl std::future::Future<Output = Option<crate::disk_cache::DiskGsfaPage>>,
    address: &str,
    limit: u64,
    before: Option<SlotBoundary>,
    until: Option<SlotBoundary>,
) -> crate::processing::ProcessingResult<(
    Vec<SignatureRecord>,
    crate::clickhouse::QueryTimings,
    Option<crate::disk_cache::DiskGsfaPage>,
)> {
    let disk_page = local.await;
    let (clickhouse_before, clickhouse_limit) = match disk_page.as_ref() {
        Some(page) if !page.reached_floor => {
            return Ok((
                Vec::new(),
                crate::clickhouse::QueryTimings::zero(),
                disk_page,
            ));
        }
        Some(page) => (
            Some(clamp_before_to_floor(before, page.floor)),
            limit - page.records.len() as u64,
        ),
        None => (before, limit),
    };
    route.source_clickhouse();
    let (records, timings) = state
        .clickhouse
        .get_signatures_for_address_with_positions(
            address,
            clickhouse_limit,
            clickhouse_before,
            until,
        )
        .await?;
    Ok((records, timings, disk_page))
}

/// Race a local page against the primary's full page for the same bounds, returning
/// the primary rows or a complete local page (`reached_floor` unset). The local future
/// yields `None` for a page that owes rows above its tip nothing merged will supply.
///
/// A short local page that reached the coverage floor still owes older history, which
/// the primary page already covers, so it is discarded (the cache is filled from the
/// primary, so its rows are a subset). A primary error waits for the local page: a
/// complete local page never needed the primary. The losing local read is dropped, which
/// releases its permit when its response closes. A losing primary read is left to drain
/// on its own task: dropping a submitted primary read would retain its admission until
/// the server confirms termination.
#[cfg(feature = "disk-cache")]
pub(crate) async fn race_gsfa_page(
    local: impl std::future::Future<Output = Option<crate::disk_cache::DiskGsfaPage>>,
    primary: PrimaryGsfaPage,
) -> crate::processing::ProcessingResult<(
    Vec<SignatureRecord>,
    crate::clickhouse::QueryTimings,
    Option<crate::disk_cache::DiskGsfaPage>,
)> {
    use futures_util::future::{Either, select};
    let local = std::pin::pin!(local);
    // Poll the primary first so its request is sent before the local page's
    // synchronous candidate filtering runs.
    match select(primary, local).await {
        Either::Left((Ok((records, timings)), _)) => Ok((records, timings, None)),
        Either::Left((Err(err), local)) => match local.await {
            Some(page) if !page.reached_floor => Ok((
                Vec::new(),
                crate::clickhouse::QueryTimings::zero(),
                Some(page),
            )),
            _ => Err(err),
        },
        Either::Right((Some(page), primary)) if !page.reached_floor => {
            let inherited_admission = crate::clickhouse::read_query::admission::current();
            tokio::spawn(crate::clickhouse::read_query::admission::scope_with(
                inherited_admission,
                async move {
                    let _ = primary.await;
                },
            ));
            Ok((
                Vec::new(),
                crate::clickhouse::QueryTimings::zero(),
                Some(page),
            ))
        }
        Either::Right((_, primary)) => primary
            .await
            .map(|(records, timings)| (records, timings, None)),
    }
}

/// The primary step of a getSignaturesForAddress cursor lookup, after the head and local
/// tiers missed.
enum PrimaryCursor {
    Found(SignatureSlot),
    Missing,
    /// Left to the page query (`CLICKHOUSE_GSFA_INLINE_CURSOR`).
    Deferred([u8; 64]),
}

/// With the inline cursor enabled, a signature-slot cache answer is used and anything else is
/// deferred to the page query; otherwise this is the separate primary lookup.
async fn resolve_cursor_on_primary(
    state: &AppState,
    sig_str: &str,
    timings: &mut crate::clickhouse::QueryTimings,
) -> crate::processing::ProcessingResult<PrimaryCursor> {
    if state.clickhouse.gsfa_inline_cursor_enabled()
        && let Some(bytes) = Signature::from_str(sig_str)
            .ok()
            .and_then(|signature| <[u8; 64]>::try_from(signature.as_ref()).ok())
    {
        return Ok(match state.clickhouse.cached_signature_slot(&bytes).await {
            Some(Some(pos)) => PrimaryCursor::Found(pos),
            Some(None) => PrimaryCursor::Missing,
            None => PrimaryCursor::Deferred(bytes),
        });
    }
    let (pos, lookup_timings) = state.clickhouse.get_signature_slot(sig_str).await?;
    timings.add(lookup_timings);
    Ok(pos.map_or(PrimaryCursor::Missing, PrimaryCursor::Found))
}

fn inline_cursor(boundary: Option<SlotBoundary>, deferred: Option<[u8; 64]>) -> GsfaCursor {
    deferred.map_or(GsfaCursor::Resolved(boundary), GsfaCursor::Signature)
}

/// The primary page with deferred cursors resolved inside it, `Ok(None)` when no cursor was
/// deferred, or `Err` with the -32020 response for a cursor with no `signatures` row. The page
/// comes with the positions its deferred `before`/`until` cursors resolved to.
#[allow(clippy::too_many_arguments)]
async fn inline_cursor_page(
    state: &AppState,
    route: &mut RouteMetric,
    id: &Value,
    address: &str,
    limit: u64,
    options: &GetSignaturesForAddressOptions,
    before: (Option<SlotBoundary>, Option<[u8; 64]>),
    until: (Option<SlotBoundary>, Option<[u8; 64]>),
) -> Result<
    Option<
        crate::processing::ProcessingResult<(
            Vec<SignatureRecord>,
            crate::clickhouse::QueryTimings,
            Option<SignatureSlot>,
            Option<SignatureSlot>,
        )>,
    >,
    Box<Response>,
> {
    if before.1.is_none() && until.1.is_none() {
        return Ok(None);
    }
    route.source_clickhouse();
    match state
        .clickhouse
        .get_signatures_for_address_inline_cursor(
            address,
            limit,
            inline_cursor(before.0, before.1),
            inline_cursor(until.0, until.1),
        )
        .await
    {
        Ok(InlineGsfaPage::Page {
            records,
            timings,
            before: before_pos,
            until: until_pos,
        }) => {
            // `until == before` shares the `before` scalar.
            let until_pos = if until.1.is_some() && until.1 == before.1 {
                before_pos
            } else {
                until_pos
            };
            Ok(Some(Ok((records, timings, before_pos, until_pos))))
        }
        Ok(InlineGsfaPage::Missing(missing)) => {
            let signature = match missing {
                GsfaMissingCursor::Before => options.before.as_deref(),
                GsfaMissingCursor::Until => options.until.as_deref(),
            };
            route.rpc_error();
            Err(Box::new(json_rpc_filter_transaction_not_found_response(
                id.clone(),
                signature.unwrap_or_default(),
            )))
        }
        Err(err) => Ok(Some(Err(err))),
    }
}

/// Whether the head cache proves every slot above `local_tip` up to its `commitment` tip, the
/// same chain-continuity proof getBlocks uses.
#[cfg(all(feature = "grpc-head-cache", feature = "disk-cache"))]
fn head_proves_above(
    cache: &crate::head_cache::HeadCache,
    local_tip: u64,
    commitment: CommitmentLevel,
) -> bool {
    let start = local_tip.saturating_add(1);
    if cache.min_retained_slot() > start {
        return false;
    }
    match cache.coverage.read().expect("head coverage lock").snapshot(
        start,
        None,
        commitment,
        std::time::Instant::now(),
    ) {
        Ok((end, coverage)) => coverage.gaps(start, end).is_empty(),
        Err(_) => false,
    }
}

/// Whether the current local span and head proof could serve a watermark page, checked before
/// the local read so an unprovable entry keeps the local/primary race instead of running the
/// local read and the primary one after another.
#[cfg(all(feature = "grpc-head-cache", feature = "disk-cache"))]
fn watermark_provable(
    cache: &crate::head_cache::HeadCache,
    disk: &crate::disk_cache::DiskCache,
    watermark: u64,
    commitment: CommitmentLevel,
) -> bool {
    use crate::disk_cache::gsfa_watermark::local_page_reaches_watermark;
    let outcome = match disk.tip_span() {
        None => "local_unavailable",
        Some((floor, _)) if !local_page_reaches_watermark(floor, watermark) => "floor_gap",
        Some((_, tip)) if !head_proves_above(cache, tip, commitment) => "head_unproven",
        Some(_) => return true,
    };
    metrics::disk_cache_read("gsfa_empty_watermark", outcome);
    false
}

/// A no-cursor page for an address with an empty-address watermark: the local page serves
/// when it is complete, or when it covers `(watermark, local tip]` and the head cache proves
/// everything above the local tip. Otherwise the primary serves alone, since the local page is
/// already known to be short.
#[cfg(all(feature = "grpc-head-cache", feature = "disk-cache"))]
#[allow(clippy::too_many_arguments)]
async fn watermark_gsfa_page(
    state: &Arc<AppState>,
    route: &mut RouteMetric,
    cache: &crate::head_cache::HeadCache,
    disk: &crate::disk_cache::DiskCache,
    address: &str,
    address_pubkey: Pubkey,
    limit: u64,
    deadline: tokio::time::Instant,
    watermark: u64,
    commitment: CommitmentLevel,
) -> crate::processing::ProcessingResult<(
    Vec<SignatureRecord>,
    crate::clickhouse::QueryTimings,
    Option<crate::disk_cache::DiskGsfaPage>,
)> {
    use crate::disk_cache::gsfa_watermark::local_page_reaches_watermark;
    let local = disk
        .signatures_for_address_until(address_pubkey, None, None, limit as usize, deadline)
        .await;
    let outcome = match &local {
        Some(page) if !page.reached_floor => "complete",
        Some(page)
            if local_page_reaches_watermark(page.floor, watermark)
                && head_proves_above(cache, page.tip, commitment) =>
        {
            "hit"
        }
        Some(page) if !local_page_reaches_watermark(page.floor, watermark) => "floor_gap",
        Some(_) => "head_unproven",
        None => "local_unavailable",
    };
    metrics::disk_cache_read("gsfa_empty_watermark", outcome);
    match local {
        Some(page) if outcome == "complete" || outcome == "hit" => Ok((
            Vec::new(),
            crate::clickhouse::QueryTimings::zero(),
            Some(page),
        )),
        _ => {
            route.source_clickhouse();
            primary_gsfa_page(state, address, limit, None, None)
                .await
                .map(|(records, timings)| (records, timings, None))
        }
    }
}

pub(crate) async fn handle_get_signatures_for_address(
    state: Arc<AppState>,
    id: Value,
    params: Option<Vec<Value>>,
) -> Result<Response, StatusCode> {
    let mut route = RouteMetric::for_state("getSignaturesForAddress", state.as_ref());
    #[cfg(feature = "disk-cache")]
    let disk_request = state
        .disk_cache()
        .map(|disk| (disk, disk.address_request_deadline()));

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
        let mut before_inline = None;
        let mut until_inline = None;

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
                && let Some((disk, deadline)) = disk_request
                && let Ok(sig) = Signature::from_str(sig_str)
            {
                route.disk_cache_read();
                pos = disk.signature_position_until(sig, deadline).await;
            }

            if pos.is_none() {
                route.source_clickhouse();
                match resolve_cursor_on_primary(&state, sig_str, &mut precheck_timings).await {
                    Ok(PrimaryCursor::Found(found)) => pos = Some(found),
                    Ok(PrimaryCursor::Missing) => {}
                    Ok(PrimaryCursor::Deferred(bytes)) => before_inline = Some(bytes),
                    Err(e) => {
                        metrics::backend_error("get_signature_slot");
                        error!("Failed to query ClickHouse for signature slot {sig_str}: {e}");
                        route.rpc_error();
                        return Ok(json_rpc_long_term_storage_unreachable_response(id));
                    }
                }
            }

            if pos.is_none() && before_inline.is_none() {
                route.rpc_error();
                return Ok(json_rpc_filter_transaction_not_found_response(id, sig_str));
            }
            before_boundary = pos.map(SlotBoundary::Position);
        }

        if let Some(sig_str) = options.until.as_deref() {
            if options.before.as_deref() == Some(sig_str) {
                until_boundary = before_boundary;
                until_inline = before_inline;
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
                    && let Some((disk, deadline)) = disk_request
                    && let Ok(sig) = Signature::from_str(sig_str)
                {
                    route.disk_cache_read();
                    pos = disk.signature_position_until(sig, deadline).await;
                }

                if pos.is_none() {
                    route.source_clickhouse();
                    match resolve_cursor_on_primary(&state, sig_str, &mut precheck_timings).await {
                        Ok(PrimaryCursor::Found(found)) => pos = Some(found),
                        Ok(PrimaryCursor::Missing) => {}
                        Ok(PrimaryCursor::Deferred(bytes)) => until_inline = Some(bytes),
                        Err(e) => {
                            metrics::backend_error("get_signature_slot");
                            error!("Failed to query ClickHouse for signature slot {sig_str}: {e}");
                            route.rpc_error();
                            return Ok(json_rpc_long_term_storage_unreachable_response(id));
                        }
                    }
                }

                if pos.is_none() && until_inline.is_none() {
                    route.rpc_error();
                    return Ok(json_rpc_filter_transaction_not_found_response(id, sig_str));
                }
                until_boundary = pos.map(SlotBoundary::Position);
            }
        }

        // The head's chain proof is taken before its rows are read: every slot it covers
        // was ingested before the proof published it, so the rows read next include them.
        #[cfg(feature = "disk-cache")]
        let head_chain_floor = disk_request.and_then(|_| cache.chain_floor(min_commitment));
        // A deferred cursor bounds the head rows only once the page query resolves it.
        let deferred = before_inline.is_some() || until_inline.is_some();
        let mut head_metas = if deferred {
            Vec::new()
        } else {
            cache.signatures_for_address(
                &address_pubkey,
                before_boundary,
                until_boundary,
                limit as usize,
                min_commitment,
            )
        };

        if head_metas.len() as u64 >= limit {
            route.source_head_cache();
            route.success();
            let signature_infos = head_metas
                .into_iter()
                .map(|meta| SignatureInfo {
                    signature: meta.signature_str.to_string(),
                    slot: meta.pos.slot,
                    transaction_index: Some(meta.pos.idx),
                    err: meta.err.clone(),
                    memo: meta.memo.clone(),
                    block_time: meta.block_time,
                    confirmation_status: Some(cache.confirmation_status_string(&meta).to_string()),
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

        // Disk tier: a complete newest-first page over the contiguous covered span
        // replaces the primary. It races the primary's full page for the same
        // bounds instead of preceding it: a short local page cannot prove there is
        // no older history, so it almost never replaces the primary.
        // A cursor only the primary knows is resolved inside the page query. The local page
        // cannot be bounded by it, so that request is not raced.
        let inline_page = match inline_cursor_page(
            &state,
            &mut route,
            &id,
            address,
            limit,
            &options,
            (before_boundary, before_inline),
            (until_boundary, until_inline),
        )
        .await
        {
            Ok(Some(Ok((records, timings, before_pos, until_pos)))) => {
                if let Some(pos) = before_pos {
                    before_boundary = Some(SlotBoundary::Position(pos));
                }
                if let Some(pos) = until_pos {
                    until_boundary = Some(SlotBoundary::Position(pos));
                }
                head_metas = cache.signatures_for_address(
                    &address_pubkey,
                    before_boundary,
                    until_boundary,
                    limit as usize,
                    min_commitment,
                );
                Some(Ok((records, timings)))
            }
            Ok(Some(Err(err))) => Some(Err(err)),
            Ok(None) => None,
            Err(response) => return Ok(*response),
        };

        // Empty-address watermarks apply only to a request without any cursor.
        #[cfg(feature = "disk-cache")]
        let watermark_request = options.before.is_none()
            && options.until.is_none()
            && options.before_slot.is_none()
            && options.until_slot.is_none();
        #[cfg(feature = "disk-cache")]
        let mut watermark_fill_tip = None;
        #[cfg(feature = "disk-cache")]
        let page = match (inline_page, disk_request) {
            (Some(result), _) => result.map(|(records, timings)| (records, timings, None)),
            (None, Some((disk, deadline))) if !disk.gsfa_race_primary() => {
                // Boxed: keeps this arm's temporaries out of the handler's poll frame
                // (debug builds allocate every arm's locals there).
                Box::pin(async {
                    route.disk_cache_read();
                    // Serial path (`GSFA_RACE_PRIMARY=false`), main's local-then-remainder order;
                    // the empty-address watermark rides on the race and is not consulted here.
                    // The tip-gap rule is a correctness fix, not part of the race, so it applies
                    // here too: a page that owes rows above its tip the head merge cannot prove
                    // is dropped, and the primary answers the full page instead of a below-floor
                    // remainder.
                    let local = async {
                        let page = disk
                            .signatures_for_address_until(
                                address_pubkey,
                                before_boundary,
                                until_boundary,
                                limit as usize,
                                deadline,
                            )
                            .await?;
                        crate::disk_cache::tip_gap_covered(
                            "signatures_for_address",
                            crate::disk_cache::gsfa_tip_gap(
                                page.tip,
                                before_boundary,
                                until_boundary,
                            ),
                            || cache.address_floor(&address_pubkey, head_chain_floor),
                        )
                        .then_some(page)
                    };
                    serial_gsfa_page(
                        &state,
                        &mut route,
                        local,
                        address,
                        limit,
                        before_boundary,
                        until_boundary,
                    )
                    .await
                })
                .await
            }
            (None, Some((disk, deadline))) => {
                // Boxed: keeps this arm's temporaries out of the handler's poll frame
                // (debug builds allocate every arm's locals there).
                Box::pin(async {
                    route.disk_cache_read();
                    let watermark =
                        if watermark_request && !cache.address_list_full(&address_pubkey) {
                            disk.gsfa_watermarks()
                                .get(&address_pubkey, std::time::Instant::now())
                                .filter(|&watermark| {
                                    watermark_provable(cache, disk, watermark, min_commitment)
                                })
                        } else {
                            None
                        };
                    match watermark {
                        Some(watermark) => {
                            watermark_gsfa_page(
                                &state,
                                &mut route,
                                cache,
                                disk,
                                address,
                                address_pubkey,
                                limit,
                                deadline,
                                watermark,
                                min_commitment,
                            )
                            .await
                        }
                        None => {
                            if watermark_request && disk.gsfa_watermarks().enabled() {
                                // Read before the primary: the filler copied every slot up to
                                // this tip from the primary, so an empty primary page proves them.
                                watermark_fill_tip = disk.tip_span().map(|(_, tip)| tip);
                            }
                            let primary = primary_gsfa_page(
                                &state,
                                address,
                                limit,
                                before_boundary,
                                until_boundary,
                            );
                            route.source_clickhouse();
                            let local = async {
                                let page = disk
                                    .signatures_for_address_until(
                                        address_pubkey,
                                        before_boundary,
                                        until_boundary,
                                        limit as usize,
                                        deadline,
                                    )
                                    .await?;
                                // Rows above the local tip come only from the head merge below.
                                let covered = page.reached_floor
                                    || crate::disk_cache::tip_gap_covered(
                                        "signatures_for_address",
                                        crate::disk_cache::gsfa_tip_gap(
                                            page.tip,
                                            before_boundary,
                                            until_boundary,
                                        ),
                                        || cache.address_floor(&address_pubkey, head_chain_floor),
                                    );
                                covered.then_some(page)
                            };
                            race_gsfa_page(local, primary).await
                        }
                    }
                })
                .await
            }
            (None, None) => {
                route.source_clickhouse();
                primary_gsfa_page(&state, address, limit, before_boundary, until_boundary)
                    .await
                    .map(|(records, timings)| (records, timings, None))
            }
        };
        #[cfg(not(feature = "disk-cache"))]
        let page = match inline_page {
            Some(result) => result.map(|(records, timings)| (records, timings, None::<()>)),
            None => {
                route.source_clickhouse();
                state
                    .clickhouse
                    .get_signatures_for_address_with_positions(
                        address,
                        limit,
                        before_boundary,
                        until_boundary,
                    )
                    .await
                    .map(|(records, timings)| (records, timings, None::<()>))
            }
        };

        let (signatures, mut timings, disk_page) = match page {
            Ok(page) => page,
            Err(e) => {
                metrics::backend_error("get_signatures_for_address_with_positions");
                error!("Failed to query ClickHouse: {}", e);
                route.rpc_error();
                return Ok(json_rpc_long_term_storage_unreachable_response(id));
            }
        };
        // Only a complete page replaced the primary; the serial path can also return a
        // short page whose remainder came from the primary.
        #[cfg(feature = "disk-cache")]
        let skip_clickhouse = disk_page.as_ref().is_some_and(|page| !page.reached_floor);
        #[cfg(not(feature = "disk-cache"))]
        let skip_clickhouse = disk_page.is_some();

        // Only an empty primary page (no local page) writes a watermark; rows remove it.
        #[cfg(feature = "disk-cache")]
        if watermark_request && let Some((disk, _)) = disk_request {
            if !signatures.is_empty() {
                disk.gsfa_watermarks().remove(&address_pubkey);
            } else if disk_page.is_none()
                && let Some(tip) = watermark_fill_tip
            {
                disk.gsfa_watermarks().insert(
                    address_pubkey,
                    crate::disk_cache::gsfa_watermark::fill_watermark(tip),
                    std::time::Instant::now(),
                );
            }
        }

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
            if skip_clickhouse || disk_contributed {
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
                    confirmation_status: cache.confirmation_status_string(&meta).to_string(),
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
                transaction_index: Some(sig.slot_idx),
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
    let mut before_inline = None;
    let mut until_inline = None;

    if let Some(sig_str) = options.before.as_deref() {
        #[cfg(feature = "disk-cache")]
        if let Some((disk, deadline)) = disk_request
            && let Ok(signature) = Signature::from_str(sig_str)
        {
            route.disk_cache_read();
            before_boundary = disk
                .signature_position_until(signature, deadline)
                .await
                .map(SlotBoundary::Position);
        }
        if before_boundary.is_none() {
            route.source_clickhouse();
            match resolve_cursor_on_primary(&state, sig_str, &mut precheck_timings).await {
                Ok(PrimaryCursor::Found(pos)) => {
                    before_boundary = Some(SlotBoundary::Position(pos))
                }
                Ok(PrimaryCursor::Missing) => {}
                Ok(PrimaryCursor::Deferred(bytes)) => before_inline = Some(bytes),
                Err(e) => {
                    metrics::backend_error("get_signature_slot");
                    error!("Failed to query ClickHouse for signature slot {sig_str}: {e}");
                    route.rpc_error();
                    return Ok(json_rpc_long_term_storage_unreachable_response(id));
                }
            }
        }

        if before_boundary.is_none() && before_inline.is_none() {
            route.rpc_error();
            return Ok(json_rpc_filter_transaction_not_found_response(id, sig_str));
        }
    }

    if let Some(sig_str) = options.until.as_deref() {
        if options.before.as_deref() == Some(sig_str) {
            until_boundary = before_boundary;
            until_inline = before_inline;
        } else {
            #[cfg(feature = "disk-cache")]
            if let Some((disk, deadline)) = disk_request
                && let Ok(signature) = Signature::from_str(sig_str)
            {
                route.disk_cache_read();
                until_boundary = disk
                    .signature_position_until(signature, deadline)
                    .await
                    .map(SlotBoundary::Position);
            }
            if until_boundary.is_none() {
                route.source_clickhouse();
                match resolve_cursor_on_primary(&state, sig_str, &mut precheck_timings).await {
                    Ok(PrimaryCursor::Found(pos)) => {
                        until_boundary = Some(SlotBoundary::Position(pos))
                    }
                    Ok(PrimaryCursor::Missing) => {}
                    Ok(PrimaryCursor::Deferred(bytes)) => until_inline = Some(bytes),
                    Err(e) => {
                        metrics::backend_error("get_signature_slot");
                        error!("Failed to query ClickHouse for signature slot {sig_str}: {e}");
                        route.rpc_error();
                        return Ok(json_rpc_long_term_storage_unreachable_response(id));
                    }
                }
            }

            if until_boundary.is_none() && until_inline.is_none() {
                route.rpc_error();
                return Ok(json_rpc_filter_transaction_not_found_response(id, sig_str));
            }
        }
    }

    let inline_page = match inline_cursor_page(
        &state,
        &mut route,
        &id,
        address,
        limit,
        &options,
        (before_boundary, before_inline),
        (until_boundary, until_inline),
    )
    .await
    {
        Ok(page) => page.map(|result| result.map(|(records, timings, _, _)| (records, timings))),
        Err(response) => return Ok(*response),
    };

    // A complete local page replaces the primary; see the head-cache path above.
    #[cfg(feature = "disk-cache")]
    let page = match (inline_page, disk_request) {
        (Some(result), _) => result.map(|(records, timings)| (records, timings, None)),
        (None, Some((disk, deadline))) if !disk.gsfa_race_primary() => {
            // Boxed: keeps this arm's temporaries out of the handler's poll frame
            // (debug builds allocate every arm's locals there).
            Box::pin(async {
                route.disk_cache_read();
                // Serial path: no head merge here, so any row owed above the local tip
                // sends the full page to the primary (tip-gap correctness rule).
                let local = async {
                    let page = disk
                        .signatures_for_address_until(
                            Pubkey::from_str(address).expect("validated address"),
                            before_boundary,
                            until_boundary,
                            limit as usize,
                            deadline,
                        )
                        .await?;
                    crate::disk_cache::tip_gap_covered(
                        "signatures_for_address",
                        crate::disk_cache::gsfa_tip_gap(page.tip, before_boundary, until_boundary),
                        || None,
                    )
                    .then_some(page)
                };
                serial_gsfa_page(
                    &state,
                    &mut route,
                    local,
                    address,
                    limit,
                    before_boundary,
                    until_boundary,
                )
                .await
            })
            .await
        }
        (None, disk_request) => {
            // Boxed: keeps this arm's temporaries out of the handler's poll frame
            // (debug builds allocate every arm's locals there).
            Box::pin(async {
                let primary =
                    primary_gsfa_page(&state, address, limit, before_boundary, until_boundary);
                route.source_clickhouse();
                match disk_request {
                    Some((disk, deadline)) => {
                        route.disk_cache_read();
                        let local = async {
                            let page = disk
                                .signatures_for_address_until(
                                    Pubkey::from_str(address).expect("validated address"),
                                    before_boundary,
                                    until_boundary,
                                    limit as usize,
                                    deadline,
                                )
                                .await?;
                            // No head merge on this path: any row owed above the local tip
                            // leaves the page incomplete.
                            let covered = page.reached_floor
                                || crate::disk_cache::tip_gap_covered(
                                    "signatures_for_address",
                                    crate::disk_cache::gsfa_tip_gap(
                                        page.tip,
                                        before_boundary,
                                        until_boundary,
                                    ),
                                    || None,
                                );
                            covered.then_some(page)
                        };
                        race_gsfa_page(local, primary).await
                    }
                    None => primary
                        .await
                        .map(|(records, timings)| (records, timings, None)),
                }
            })
            .await
        }
    };
    #[cfg(not(feature = "disk-cache"))]
    let page = if let Some(result) = inline_page {
        result.map(|(records, timings)| (records, timings, None::<()>))
    } else {
        route.source_clickhouse();
        state
            .clickhouse
            .get_signatures_for_address_with_positions(
                address,
                limit,
                before_boundary,
                until_boundary,
            )
            .await
            .map(|(records, timings)| (records, timings, None::<()>))
    };

    #[cfg_attr(not(feature = "disk-cache"), allow(unused_variables))]
    let (mut signatures, mut timings, disk_page) = match page {
        Ok(page) => page,
        Err(e) => {
            metrics::backend_error("get_signatures_for_address_with_positions");
            error!("Failed to query ClickHouse: {}", e);
            route.rpc_error();
            return Ok(json_rpc_long_term_storage_unreachable_response(id));
        }
    };
    #[cfg(feature = "disk-cache")]
    let skip_clickhouse = disk_page.as_ref().is_some_and(|page| !page.reached_floor);
    timings.add(precheck_timings);
    #[cfg(feature = "disk-cache")]
    let clickhouse_contributed = !signatures.is_empty();

    #[cfg(feature = "disk-cache")]
    let disk_contributed = if let Some(page) = disk_page {
        let mut seen: HashSet<String> = signatures
            .iter()
            .map(|record| record.signature.clone())
            .collect();
        let mut contributed = false;
        for record in page.records {
            if seen.insert(record.signature.clone()) {
                contributed = true;
                signatures.push(record);
            }
        }
        contributed
    } else {
        false
    };

    signatures.sort_unstable_by(|a, b| {
        b.slot
            .cmp(&a.slot)
            .then_with(|| b.slot_idx.cmp(&a.slot_idx))
            .then_with(|| b.signature.cmp(&a.signature))
    });
    signatures.truncate(limit as usize);
    let signature_infos: Vec<SignatureInfo> = signatures
        .into_iter()
        .map(|sig: SignatureRecord| SignatureInfo {
            signature: sig.signature,
            slot: sig.slot,
            transaction_index: Some(sig.slot_idx),
            err: sig.err,
            memo: sig.memo,
            block_time: sig.block_time,
            confirmation_status: Some("finalized".to_string()),
        })
        .collect();

    #[cfg(feature = "disk-cache")]
    if skip_clickhouse || (!clickhouse_contributed && disk_contributed) {
        route.source_disk_cache();
    }
    route.success();
    let mut resp = json_rpc_success_response(id, signature_infos);
    add_downstream_header(&mut resp, &timings);
    Ok(resp)
}

#[cfg(all(test, feature = "disk-cache", feature = "grpc-head-cache"))]
mod history_tests {
    use super::*;
    use crate::head_cache::HeadCache;
    use crate::head_cache::coverage::Link;

    fn finalize(head: &HeadCache, slots: std::ops::RangeInclusive<u64>, at: std::time::Instant) {
        let mut proof = head.coverage.write().unwrap();
        for slot in slots {
            proof.metadata(Link {
                slot,
                hash: [slot as u8; 32],
                parent: slot - 1,
                parent_hash: [(slot - 1) as u8; 32],
            });
            proof.observe(slot, CommitmentLevel::Finalized, at);
            proof.publish(slot, CommitmentLevel::Finalized);
        }
    }

    fn state_with_head(head: Option<Arc<HeadCache>>) -> Arc<AppState> {
        let mut state = crate::tests::test_state_with_clickhouse_url("http://127.0.0.1:1");
        Arc::get_mut(&mut state).unwrap().head_cache = head;
        state
    }

    fn extends(state: &AppState, disk_tip: u64) -> bool {
        head_extends_disk(state, trusted_head_finalized_tip(state), disk_tip)
    }

    #[test]
    fn head_must_continue_the_disk_tip_gaplessly() {
        assert!(!extends(&state_with_head(None), 109), "no head");

        let head = Arc::new(HeadCache::new(600, 8));
        let state = state_with_head(Some(head.clone()));
        finalize(&head, 110..=112, std::time::Instant::now());
        assert!(!extends(&state, 109), "never connected");

        head.coverage.write().unwrap().connect();
        finalize(&head, 110..=112, std::time::Instant::now());
        assert!(extends(&state, 109));
        assert!(extends(&state, 111));
        assert!(extends(&state, 115), "disk ahead of the head tip");
        assert!(!extends(&state, 108), "slot 109 is in neither tier");

        // The head no longer retains the slot after the disk tip.
        head.note_slot_commitment(1_000, CommitmentLevel::Processed);
        assert!(!extends(&state, 109));

        // A stale tip is untrusted.
        let head = Arc::new(HeadCache::new(600, 8));
        let state = state_with_head(Some(head.clone()));
        head.coverage.write().unwrap().connect();
        let stale = std::time::Instant::now() - std::time::Duration::from_secs(5);
        finalize(&head, 110..=112, stale);
        assert!(!extends(&state, 109));

        // Disconnection drops the proof.
        finalize(&head, 113..=113, std::time::Instant::now());
        assert!(extends(&state, 109));
        // A primary known to be ahead of the head's finalized tip could hold a landing
        // the head has not proven.
        let primary = &state.latest_slot_cache.value;
        primary.store(114, std::sync::atomic::Ordering::Relaxed);
        assert!(!extends(&state, 109), "primary ahead of the head");
        primary.store(113, std::sync::atomic::Ordering::Relaxed);
        assert!(extends(&state, 109));
        // Slot 114 finalizes after the lookup: the lookup could not have seen it, so a
        // primary holding it defeats the proof taken before the lookup.
        let before_lookup = trusted_head_finalized_tip(&state);
        assert_eq!(before_lookup, Some(113));
        finalize(&head, 114..=114, std::time::Instant::now());
        primary.store(114, std::sync::atomic::Ordering::Relaxed);
        assert!(extends(&state, 109), "a fresh tip would cover it");
        assert!(!head_extends_disk(&state, before_lookup, 109));
        primary.store(113, std::sync::atomic::Ordering::Relaxed);
        assert!(head_extends_disk(&state, before_lookup, 109));
        assert!(
            !head_extends_disk(&state, None, 109),
            "no trusted tip before the lookup"
        );
        head.coverage.write().unwrap().disconnect();
        assert!(!extends(&state, 109));
    }

    #[tokio::test]
    async fn disabled_cache_or_unproven_read_keeps_every_signature() {
        let head = Arc::new(HeadCache::new(600, 8));
        head.coverage.write().unwrap().connect();
        finalize(&head, 110..=112, std::time::Instant::now());
        let mut state = state_with_head(Some(head));
        let signature = Signature::from([7; 64]);
        let history = HistoryAbsence {
            disk_absent: HashMap::from([(signature.to_string(), signature)]),
            disk_span: Some((10, 109)),
        };
        let to_query = vec![signature.to_string()];
        // Disabled (the default): nothing is skipped or remembered.
        history
            .remember_absent(&state, &to_query, &HashMap::new())
            .await;
        assert_eq!(
            history
                .skip_known_absent(&state, trusted_head_finalized_tip(&state), to_query.clone())
                .await,
            to_query
        );

        Arc::get_mut(&mut state).unwrap().status_history_cache =
            crate::status_history_cache::StatusHistoryCache::new(
                10,
                1 << 20,
                std::time::Duration::from_secs(60),
            );
        // Not yet remembered.
        assert_eq!(
            history
                .skip_known_absent(&state, trusted_head_finalized_tip(&state), to_query.clone())
                .await,
            to_query
        );
        // A found primary record is never remembered as absent.
        let found = HashMap::from([(
            signature.to_string(),
            SignatureStatusRecord {
                signature: signature.to_string(),
                slot: 5,
                err: None,
            },
        )]);
        history.remember_absent(&state, &to_query, &found).await;
        assert_eq!(
            history
                .skip_known_absent(&state, trusted_head_finalized_tip(&state), to_query.clone())
                .await,
            to_query
        );
        // A read that proved nothing records nothing.
        let unproven = HistoryAbsence {
            disk_absent: HashMap::new(),
            disk_span: None,
        };
        unproven
            .remember_absent(&state, &to_query, &HashMap::new())
            .await;
        assert_eq!(
            history
                .skip_known_absent(&state, trusted_head_finalized_tip(&state), to_query.clone())
                .await,
            to_query
        );
        // An empty primary answer over a proven read is remembered and skipped.
        history
            .remember_absent(&state, &to_query, &HashMap::new())
            .await;
        assert!(
            history
                .skip_known_absent(&state, trusted_head_finalized_tip(&state), to_query.clone())
                .await
                .is_empty()
        );
        // ...but not for a request whose own read proved nothing.
        assert_eq!(
            unproven
                .skip_known_absent(&state, trusted_head_finalized_tip(&state), to_query.clone())
                .await,
            to_query
        );
    }
}
