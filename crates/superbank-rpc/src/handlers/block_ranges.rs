// SPDX-License-Identifier: AGPL-3.0-only
//! Resolve completeness before issuing any remote range reads.
use super::{RouteMetric, blocks::merge_sorted_block_slots};
use crate::{
    clickhouse::QueryTimings,
    metrics,
    rpc::{json_rpc_internal_error_response, json_rpc_success_response},
    slot_coverage::SlotCoverage,
    state::AppState,
    util::add_downstream_header,
};
use axum::{http::StatusCode, response::Response};
use serde_json::Value;
use solana_commitment_config::CommitmentConfig;
use std::{sync::Arc, time::Instant};

pub(super) struct RangeObservation {
    started: Instant,
    method: &'static str,
    primary: bool,
    local: bool,
    success: bool,
    pub(super) reason: &'static str,
}
impl RangeObservation {
    pub(super) fn new(method: &'static str) -> Self {
        Self {
            started: Instant::now(),
            method,
            primary: false,
            local: false,
            success: false,
            reason: "none",
        }
    }
    pub(super) fn success(&mut self) {
        self.success = true;
    }
}
impl Drop for RangeObservation {
    fn drop(&mut self) {
        let path = match (self.primary, self.local) {
            (false, _) => "local",
            (true, true) => "partial_cache",
            (true, false) => "primary",
        };
        metrics::blocks_range_observation(
            self.method,
            path,
            self.reason,
            self.success,
            self.started.elapsed().as_secs_f64(),
        );
    }
}

pub(super) struct BlockRange {
    start: u64,
    pub(super) end: u64,
    head: SlotCoverage,
    authoritative_head_tip: bool,
}

pub(super) async fn resolve_range(
    state: &AppState,
    route: &mut RouteMetric,
    start: u64,
    end: Option<u64>,
    commitment: CommitmentConfig,
    timings: &mut QueryTimings,
    observation: &mut RangeObservation,
) -> Result<BlockRange, &'static str> {
    #[cfg(feature = "grpc-head-cache")]
    if let Some(cache) = state.head_cache.as_ref() {
        route.head_cache_read();
        let authoritative_head_tip = end.is_none();
        let (end, head) = cache
            .coverage
            .read()
            .expect("head coverage lock")
            .snapshot(start, end, commitment.commitment, Instant::now())?;
        return Ok(BlockRange {
            start,
            end,
            head,
            authoritative_head_tip,
        });
    }
    let _ = commitment;
    let end = match end {
        Some(end) => end,
        None => latest_from_primary(state, route, timings, observation).await?,
    };
    Ok(BlockRange {
        start,
        end,
        head: SlotCoverage::default(),
        authoritative_head_tip: false,
    })
}

async fn latest_from_primary(
    state: &AppState,
    route: &mut RouteMetric,
    timings: &mut QueryTimings,
    observation: &mut RangeObservation,
) -> Result<u64, &'static str> {
    let started = Instant::now();
    let result = state
        .latest_slot_cache
        .get_or_refresh_with_source(&state.clickhouse)
        .await;
    timings.elapsed_ms = timings
        .elapsed_ms
        .saturating_add(started.elapsed().as_millis() as u64);
    match result {
        Ok((slot, primary)) => {
            observation.primary = primary;
            if primary {
                route.source_clickhouse();
                timings.rows_read_unknown = true;
                timings.rows_read = None;
            }
            Ok(slot)
        }
        Err(error) => {
            observation.primary = true;
            route.source_clickhouse();
            metrics::backend_error("get_latest_finalized_slot");
            tracing::warn!(%error, "getBlocks latest slot discovery failed");
            Err("untrusted_tip")
        }
    }
}

fn memory_coverage(
    state: &AppState,
    range: &BlockRange,
    observation: &mut RangeObservation,
) -> (SlotCoverage, bool) {
    let proof = range.head.clone();
    #[cfg(feature = "disk-cache")]
    if let Some(index) = state.disk_cache().and_then(|disk| disk.block_index()) {
        let mut proof = proof;
        let indexed = index.range_coverage(range.start, range.end);
        let contributed = !indexed.intervals.is_empty();
        if proof.merge_checked(indexed) {
            observation.reason = "cache_changed";
            // A contradiction can identify an incompatible chain, not merely one
            // bad slot. Never retain its non-overlapping suffix as a valid proof.
            return (SlotCoverage::default(), false);
        }
        return (proof, contributed);
    }
    let _ = (state, observation);
    (proof, false)
}

async fn disk_coverage(
    state: &AppState,
    route: &mut RouteMetric,
    range: &BlockRange,
    proof: &mut SlotCoverage,
) -> bool {
    #[cfg(feature = "disk-cache")]
    if let Some(disk) = state.disk_cache() {
        let mut contributed = false;
        for (start, end) in proof.gaps(range.start, range.end) {
            route.disk_cache_read();
            let disk_proof = disk.range_coverage(start, end).await;
            contributed |= !disk_proof.intervals.is_empty();
            proof.merge(disk_proof);
        }
        return contributed;
    }
    let _ = (state, route, range, proof);
    false
}

fn local_source(route: &mut RouteMetric, head: &SlotCoverage, disk: bool) {
    #[cfg(feature = "disk-cache")]
    if disk {
        route.source_disk_cache();
        return;
    }
    #[cfg(feature = "grpc-head-cache")]
    if !head.intervals.is_empty() {
        route.source_head_cache();
        return;
    }
    let _ = (head, disk);
    route.source_none();
}

pub(super) async fn get_block_slots_response_for_range(
    state: &Arc<AppState>,
    route: &mut RouteMetric,
    id: Value,
    range: BlockRange,
    mut timings: QueryTimings,
    observation: &mut RangeObservation,
) -> Result<Response, StatusCode> {
    let (mut proof, indexed) = memory_coverage(state, &range, observation);
    if range.authoritative_head_tip && observation.reason == "cache_changed" {
        let mut response = json_rpc_internal_error_response(id);
        add_downstream_header(&mut response, &timings);
        return Ok(response);
    }
    let disk_started = Instant::now();
    let disk = disk_coverage(state, route, &range, &mut proof).await;
    timings.elapsed_ms = timings
        .elapsed_ms
        .saturating_add(disk_started.elapsed().as_millis() as u64);
    observation.local = !proof.intervals.is_empty();
    let gaps = proof.gaps(range.start, range.end);
    if !observation.primary {
        local_source(route, &range.head, indexed || disk);
    }
    let mut slots = proof.slots;
    for (start, end) in gaps {
        observation.primary = true;
        if observation.reason == "none" {
            observation.reason = "coverage_gap";
        }
        route.source_clickhouse();
        let started = Instant::now();
        match state.clickhouse.get_block_slots_by_range(start, end).await {
            Ok((rows, query_timings)) => {
                slots = merge_sorted_block_slots(slots, rows);
                timings.add(query_timings);
            }
            Err(error) => {
                timings.elapsed_ms = timings
                    .elapsed_ms
                    .saturating_add(started.elapsed().as_millis() as u64);
                metrics::backend_error("get_block_slots_by_range");
                tracing::warn!(start, end, %error, "getBlocks unresolved range failed");
                let mut response = json_rpc_internal_error_response(id);
                add_downstream_header(&mut response, &timings);
                return Ok(response);
            }
        }
    }
    observation.success();
    route.success();
    metrics::blocks_slots_returned(route.method(), slots.len());
    let mut response = json_rpc_success_response(id, slots);
    add_downstream_header(&mut response, &timings);
    Ok(response)
}
