// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::borrow::Cow;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use once_cell::sync::OnceCell;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;
use prometheus_client_derive_encode::EncodeLabelSet;
use tracing::warn;

use crate::report::{FindingCode, SlotStatus};

const WINDOW_BUCKETS: [f64; 12] = [
    0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0,
];
const FETCH_BUCKETS: [f64; 11] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ModeInfoLabels {
    mode: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct StatusLabels {
    status: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct CodeLabels {
    code: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct TableLabels {
    table: String,
}

pub(crate) struct Metrics {
    registry: Registry,
    mode_info: Family<ModeInfoLabels, Gauge>,
    range_start_slot: Gauge,
    range_end_slot: Gauge,
    last_processed_slot: Gauge,
    slots_processed_total: Family<StatusLabels, Counter>,
    findings_total: Family<CodeLabels, Counter>,
    entries_verified_total: Counter,
    hashes_computed_total: Counter,
    window_duration_seconds: Histogram,
    fetch_duration_seconds: Family<TableLabels, Histogram>,
    last_window_completed_timestamp_seconds: Gauge,
}

impl Metrics {
    fn new(cluster_label: Option<&str>) -> Self {
        let mode_info = Family::default();
        let range_start_slot = Gauge::default();
        let range_end_slot = Gauge::default();
        let last_processed_slot = Gauge::default();
        let slots_processed_total = Family::default();
        let findings_total = Family::default();
        let entries_verified_total = Counter::default();
        let hashes_computed_total = Counter::default();
        let window_duration_seconds = Histogram::new(WINDOW_BUCKETS);
        let fetch_duration_seconds =
            Family::new_with_constructor((|| Histogram::new(FETCH_BUCKETS)) as fn() -> Histogram);
        let last_window_completed_timestamp_seconds = Gauge::default();

        let mut registry = Registry::with_prefix("superbank");
        let registry_for_metrics = match cluster_label
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            Some(value) => registry
                .sub_registry_with_label((Cow::Borrowed("cluster"), Cow::Owned(value.to_string()))),
            None => &mut registry,
        };
        registry_for_metrics.register(
            "verify_mode_info",
            "Static mode label for this superbank-verify process",
            mode_info.clone(),
        );
        registry_for_metrics.register(
            "verify_range_start_slot",
            "First slot of the requested verification range",
            range_start_slot.clone(),
        );
        registry_for_metrics.register(
            "verify_range_end_slot",
            "Last slot of the requested verification range",
            range_end_slot.clone(),
        );
        registry_for_metrics.register(
            "verify_last_processed_slot",
            "Highest slot whose verification window has completed",
            last_processed_slot.clone(),
        );
        registry_for_metrics.register(
            "verify_slots_processed_total",
            "Slots accounted by final status (ok, failed, unverifiable, skipped, missing)",
            slots_processed_total.clone(),
        );
        registry_for_metrics.register(
            "verify_findings_total",
            "Verification findings by code",
            findings_total.clone(),
        );
        registry_for_metrics.register(
            "verify_entries_verified_total",
            "PoH entries whose hash was recomputed and compared",
            entries_verified_total.clone(),
        );
        registry_for_metrics.register(
            "verify_hashes_computed_total",
            "SHA-256 hashes recomputed while verifying entries",
            hashes_computed_total.clone(),
        );
        registry_for_metrics.register(
            "verify_window_duration_seconds",
            "Wall-clock time to verify one slot window",
            window_duration_seconds.clone(),
        );
        registry_for_metrics.register(
            "verify_clickhouse_fetch_duration_seconds",
            "ClickHouse fetch latency by source table",
            fetch_duration_seconds.clone(),
        );
        registry_for_metrics.register(
            "verify_last_window_completed_timestamp_seconds",
            "Unix timestamp of the last completed verification window",
            last_window_completed_timestamp_seconds.clone(),
        );
        if let Err(err) = kubert_prometheus_process::register(
            registry_for_metrics.sub_registry_with_prefix("process"),
        ) {
            warn!("Failed to register process collector: {err}");
        }

        Self {
            registry,
            mode_info,
            range_start_slot,
            range_end_slot,
            last_processed_slot,
            slots_processed_total,
            findings_total,
            entries_verified_total,
            hashes_computed_total,
            window_duration_seconds,
            fetch_duration_seconds,
            last_window_completed_timestamp_seconds,
        }
    }

    fn export(&self) -> Result<Vec<u8>, String> {
        let mut buffer = String::new();
        encode(&mut buffer, &self.registry).map_err(|err| err.to_string())?;
        Ok(buffer.into_bytes())
    }
}

pub(crate) static METRICS: OnceCell<Metrics> = OnceCell::new();

fn metrics() -> &'static Metrics {
    METRICS
        .get()
        .expect("verify metrics must be initialized before use")
}

pub(crate) fn force_init(mode: &str, cluster_label: Option<&str>) {
    let metrics = METRICS.get_or_init(|| Metrics::new(cluster_label));
    metrics
        .mode_info
        .get_or_create(&ModeInfoLabels {
            mode: mode.to_string(),
        })
        .set(1);
}

pub(crate) fn set_range(start: u64, end: u64) {
    metrics().range_start_slot.set(clamp_i64(start));
    metrics().range_end_slot.set(clamp_i64(end));
}

pub(crate) fn set_last_processed_slot(slot: u64) {
    metrics().last_processed_slot.set(clamp_i64(slot));
}

pub(crate) fn observe_slot_status(status: SlotStatus, count: u64) {
    if count > 0 {
        metrics()
            .slots_processed_total
            .get_or_create(&StatusLabels {
                status: status.as_str().to_string(),
            })
            .inc_by(count);
    }
}

pub(crate) fn observe_finding(code: FindingCode) {
    metrics()
        .findings_total
        .get_or_create(&CodeLabels {
            code: code.as_str().to_string(),
        })
        .inc();
}

pub(crate) fn observe_verified(entries: u64, hashes: u64) {
    metrics().entries_verified_total.inc_by(entries);
    metrics().hashes_computed_total.inc_by(hashes);
}

pub(crate) fn observe_window_completed(duration_seconds: f64, end_slot: u64) {
    metrics().window_duration_seconds.observe(duration_seconds);
    metrics().last_processed_slot.set(clamp_i64(end_slot));
    metrics()
        .last_window_completed_timestamp_seconds
        .set(current_timestamp_seconds());
}

pub(crate) fn observe_fetch_duration(table: &str, elapsed_seconds: f64) {
    metrics()
        .fetch_duration_seconds
        .get_or_create(&TableLabels {
            table: table.to_string(),
        })
        .observe(elapsed_seconds);
}

pub(crate) async fn health_handler(stale_secs: u64) -> impl IntoResponse {
    let last_window = metrics().last_window_completed_timestamp_seconds.get();
    health_status(stale_secs, last_window, current_timestamp_seconds())
}

fn health_status(stale_secs: u64, last_window: i64, now: i64) -> StatusCode {
    if stale_secs == 0 {
        return StatusCode::OK;
    }
    if last_window <= 0 || now.saturating_sub(last_window) > stale_secs as i64 {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    }
}

pub(crate) async fn metrics_handler() -> impl IntoResponse {
    match metrics().export() {
        Ok(buffer) => (
            StatusCode::OK,
            [(
                header::CONTENT_TYPE,
                "application/openmetrics-text; version=1.0.0; charset=utf-8",
            )],
            buffer,
        )
            .into_response(),
        Err(err) => {
            warn!("Failed to scrape verify metrics: {err}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

fn current_timestamp_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| clamp_i64(duration.as_secs()))
        .unwrap_or_default()
}

fn clamp_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_is_unavailable_before_the_first_completed_window() {
        assert_eq!(
            health_status(300, 0, 1_000),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(health_status(0, 0, 1_000), StatusCode::OK);
    }

    #[test]
    fn health_tracks_the_completion_staleness_threshold() {
        assert_eq!(health_status(300, 700, 1_000), StatusCode::OK);
        assert_eq!(
            health_status(300, 699, 1_000),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
}
