// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::borrow::Cow;
use std::sync::Mutex;
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

const LATENCY_BUCKETS: [f64; 13] = [
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

fn latency_histogram() -> Histogram {
    Histogram::new(LATENCY_BUCKETS)
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct SourceInfoLabels {
    source: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct TableLabels {
    table: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct FlushFailureLabels {
    table: String,
    stage: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct SourceErrorLabels {
    stage: String,
    kind: String,
}

#[derive(Default)]
struct SlotState {
    last_processed_slot: u64,
    last_inserted_slot: u64,
    last_network_tip_slot: u64,
}

pub(crate) struct Metrics {
    registry: Registry,
    source_info: Family<SourceInfoLabels, Gauge>,
    last_processed_slot: Gauge,
    last_inserted_slot: Gauge,
    slot_lag: Gauge,
    chain_tip_lag: Gauge,
    inserted_slots_total: Counter,
    inserted_transactions_total: Counter,
    fumarole_data_channel_capacity: Gauge,
    fumarole_memory_soft_limit_bytes: Gauge,
    fumarole_buffered_bytes: Gauge,
    fumarole_pending_slots: Gauge,
    fumarole_rss_bytes: Gauge,
    fumarole_pressure_flushes_total: Counter,
    last_successful_flush_timestamp_seconds: Gauge,
    flush_duration_seconds: Family<TableLabels, Histogram>,
    flush_rows_total: Family<TableLabels, Counter>,
    flush_failures_total: Family<FlushFailureLabels, Counter>,
    source_errors_total: Family<SourceErrorLabels, Counter>,
    slot_state: Mutex<SlotState>,
}

impl Metrics {
    fn new(cluster_label: Option<&str>) -> Self {
        let source_info = Family::default();
        let last_processed_slot = Gauge::default();
        let last_inserted_slot = Gauge::default();
        let slot_lag = Gauge::default();
        let chain_tip_lag = Gauge::default();
        let inserted_slots_total = Counter::default();
        let inserted_transactions_total = Counter::default();
        let fumarole_data_channel_capacity = Gauge::default();
        let fumarole_memory_soft_limit_bytes = Gauge::default();
        let fumarole_buffered_bytes = Gauge::default();
        let fumarole_pending_slots = Gauge::default();
        let fumarole_rss_bytes = Gauge::default();
        let fumarole_pressure_flushes_total = Counter::default();
        let last_successful_flush_timestamp_seconds = Gauge::default();
        let flush_duration_seconds =
            Family::new_with_constructor(latency_histogram as fn() -> Histogram);
        let flush_rows_total = Family::default();
        let flush_failures_total = Family::default();
        let source_errors_total = Family::default();

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
            "ingest_source_info",
            "Static source label for this superbank ingest process",
            source_info.clone(),
        );
        registry_for_metrics.register(
            "ingest_last_processed_slot",
            "Highest source slot processed by superbank before durable insertion",
            last_processed_slot.clone(),
        );
        registry_for_metrics.register(
            "ingest_last_inserted_slot",
            "Highest slot durably inserted into ClickHouse blocks metadata",
            last_inserted_slot.clone(),
        );
        registry_for_metrics.register(
            "ingest_slot_lag",
            "Difference between the highest processed slot and highest durably inserted slot",
            slot_lag.clone(),
        );
        registry_for_metrics.register(
            "ingest_chain_tip_lag",
            "Slots between the current network tip (at configured commitment) and the last durably inserted slot; only populated for fumarole and grpc sources",
            chain_tip_lag.clone(),
        );
        registry_for_metrics.register(
            "ingest_inserted_slots_total",
            "Total slots durably inserted into ClickHouse blocks metadata",
            inserted_slots_total.clone(),
        );
        registry_for_metrics.register(
            "ingest_inserted_transactions_total",
            "Total transaction rows durably inserted into ClickHouse",
            inserted_transactions_total.clone(),
        );
        registry_for_metrics.register(
            "ingest_fumarole_data_channel_capacity",
            "Configured Fumarole stream output channel capacity",
            fumarole_data_channel_capacity.clone(),
        );
        registry_for_metrics.register(
            "ingest_fumarole_memory_soft_limit_bytes",
            "Configured Fumarole memory pressure soft limit in bytes; zero disables the guard",
            fumarole_memory_soft_limit_bytes.clone(),
        );
        registry_for_metrics.register(
            "ingest_fumarole_buffered_bytes",
            "Estimated bytes buffered by the Superbank Fumarole block assembler",
            fumarole_buffered_bytes.clone(),
        );
        registry_for_metrics.register(
            "ingest_fumarole_pending_slots",
            "Slots currently pending in the Superbank Fumarole block assembler",
            fumarole_pending_slots.clone(),
        );
        registry_for_metrics.register(
            "ingest_fumarole_rss_bytes",
            "Latest sampled process resident set size in bytes used by the Fumarole pressure guard",
            fumarole_rss_bytes.clone(),
        );
        registry_for_metrics.register(
            "ingest_fumarole_pressure_flushes_total",
            "Total ClickHouse flushes triggered by the Fumarole memory pressure guard",
            fumarole_pressure_flushes_total.clone(),
        );
        registry_for_metrics.register(
            "ingest_last_successful_flush_timestamp_seconds",
            "Unix timestamp of the last successful ClickHouse flush",
            last_successful_flush_timestamp_seconds.clone(),
        );
        registry_for_metrics.register(
            "ingest_clickhouse_flush_duration_seconds",
            "ClickHouse flush latency by target table",
            flush_duration_seconds.clone(),
        );
        registry_for_metrics.register(
            "ingest_clickhouse_flush_rows_total",
            "Total rows successfully flushed to ClickHouse by target table",
            flush_rows_total.clone(),
        );
        registry_for_metrics.register(
            "ingest_clickhouse_flush_failures_total",
            "Total ClickHouse flush failures by target table and stage",
            flush_failures_total.clone(),
        );
        registry_for_metrics.register(
            "ingest_source_errors_total",
            "Total source-side ingest errors by stage and kind",
            source_errors_total.clone(),
        );
        if let Err(err) = kubert_prometheus_process::register(
            registry_for_metrics.sub_registry_with_prefix("process"),
        ) {
            warn!("Failed to register process collector: {err}");
        }

        Self {
            registry,
            source_info,
            last_processed_slot,
            last_inserted_slot,
            slot_lag,
            chain_tip_lag,
            inserted_slots_total,
            inserted_transactions_total,
            fumarole_data_channel_capacity,
            fumarole_memory_soft_limit_bytes,
            fumarole_buffered_bytes,
            fumarole_pending_slots,
            fumarole_rss_bytes,
            fumarole_pressure_flushes_total,
            last_successful_flush_timestamp_seconds,
            flush_duration_seconds,
            flush_rows_total,
            flush_failures_total,
            source_errors_total,
            slot_state: Mutex::new(SlotState::default()),
        }
    }

    fn set_source(&self, source: &str) {
        self.source_info
            .get_or_create(&SourceInfoLabels {
                source: source.to_string(),
            })
            .set(1);
    }

    fn set_last_processed_slot(&self, slot: u64) {
        self.last_processed_slot.set(clamp_i64(slot));
        self.update_slot_state(|state| {
            state.last_processed_slot = state.last_processed_slot.max(slot);
        });
    }

    fn observe_block_insert(&self, max_slot: u64, inserted_slots: u64) {
        self.last_inserted_slot.set(clamp_i64(max_slot));
        self.inserted_slots_total.inc_by(inserted_slots);
        self.last_successful_flush_timestamp_seconds
            .set(current_timestamp_seconds());
        self.update_slot_state(|state| {
            state.last_inserted_slot = state.last_inserted_slot.max(max_slot);
        });
    }

    fn observe_transaction_insert(&self, rows: u64) {
        self.inserted_transactions_total.inc_by(rows);
        self.last_successful_flush_timestamp_seconds
            .set(current_timestamp_seconds());
    }

    fn set_fumarole_data_channel_capacity(&self, capacity: usize) {
        self.fumarole_data_channel_capacity
            .set(clamp_usize(capacity));
    }

    fn set_fumarole_memory_soft_limit_bytes(&self, bytes: u64) {
        self.fumarole_memory_soft_limit_bytes.set(clamp_i64(bytes));
    }

    fn set_fumarole_buffered_bytes(&self, bytes: u64) {
        self.fumarole_buffered_bytes.set(clamp_i64(bytes));
    }

    fn set_fumarole_pending_slots(&self, slots: usize) {
        self.fumarole_pending_slots.set(clamp_usize(slots));
    }

    fn set_fumarole_rss_bytes(&self, bytes: u64) {
        self.fumarole_rss_bytes.set(clamp_i64(bytes));
    }

    fn observe_fumarole_pressure_flush(&self) {
        self.fumarole_pressure_flushes_total.inc();
    }

    fn observe_flush_duration(&self, table: &str, elapsed_seconds: f64) {
        self.flush_duration_seconds
            .get_or_create(&TableLabels {
                table: table.to_string(),
            })
            .observe(elapsed_seconds);
    }

    fn observe_flush_rows(&self, table: &str, rows: u64) {
        self.flush_rows_total
            .get_or_create(&TableLabels {
                table: table.to_string(),
            })
            .inc_by(rows);
    }

    fn observe_flush_failure(&self, table: &str, stage: &str) {
        self.flush_failures_total
            .get_or_create(&FlushFailureLabels {
                table: table.to_string(),
                stage: stage.to_string(),
            })
            .inc();
    }

    fn observe_source_error(&self, stage: &str, kind: &str) {
        self.source_errors_total
            .get_or_create(&SourceErrorLabels {
                stage: stage.to_string(),
                kind: kind.to_string(),
            })
            .inc();
    }

    fn export(&self) -> Result<Vec<u8>, String> {
        let mut buffer = String::new();
        encode(&mut buffer, &self.registry).map_err(|err| err.to_string())?;
        Ok(buffer.into_bytes())
    }

    fn set_network_tip_slot(&self, slot: u64) {
        self.update_slot_state(|state| {
            state.last_network_tip_slot = state.last_network_tip_slot.max(slot);
        });
    }

    fn update_slot_state(&self, update: impl FnOnce(&mut SlotState)) {
        let guard = self.slot_state.lock();
        let mut guard = match guard {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        update(&mut guard);
        let lag = guard
            .last_processed_slot
            .saturating_sub(guard.last_inserted_slot);
        self.slot_lag.set(clamp_i64(lag));
        let chain_tip_lag = guard
            .last_network_tip_slot
            .saturating_sub(guard.last_inserted_slot);
        self.chain_tip_lag.set(clamp_i64(chain_tip_lag));
    }
}

pub static METRICS: OnceCell<Metrics> = OnceCell::new();

fn metrics() -> &'static Metrics {
    METRICS
        .get()
        .expect("ingest metrics must be initialized before use")
}

pub(crate) fn force_init(source: &str, cluster_label: Option<&str>) {
    let metrics = METRICS.get_or_init(|| Metrics::new(cluster_label));
    metrics.set_source(source);
}

pub(crate) fn set_last_processed_slot(slot: u64) {
    metrics().set_last_processed_slot(slot);
}

pub(crate) fn set_network_tip_slot(slot: u64) {
    metrics().set_network_tip_slot(slot);
}

pub(crate) fn observe_block_insert(table: &str, rows: usize, max_slot: Option<u64>) {
    metrics().observe_flush_rows(table, rows as u64);
    if let Some(max_slot) = max_slot {
        metrics().observe_block_insert(max_slot, rows as u64);
    }
}

pub(crate) fn observe_transaction_insert(table: &str, rows: usize) {
    metrics().observe_flush_rows(table, rows as u64);
    metrics().observe_transaction_insert(rows as u64);
}

pub(crate) fn set_fumarole_data_channel_capacity(capacity: usize) {
    metrics().set_fumarole_data_channel_capacity(capacity);
}

pub(crate) fn set_fumarole_memory_soft_limit_bytes(bytes: u64) {
    metrics().set_fumarole_memory_soft_limit_bytes(bytes);
}

pub(crate) fn set_fumarole_buffered_bytes(bytes: u64) {
    metrics().set_fumarole_buffered_bytes(bytes);
}

pub(crate) fn set_fumarole_pending_slots(slots: usize) {
    metrics().set_fumarole_pending_slots(slots);
}

pub(crate) fn set_fumarole_rss_bytes(bytes: u64) {
    metrics().set_fumarole_rss_bytes(bytes);
}

pub(crate) fn observe_fumarole_pressure_flush() {
    metrics().observe_fumarole_pressure_flush();
}

pub(crate) fn observe_flush_duration(table: &str, elapsed_seconds: f64) {
    metrics().observe_flush_duration(table, elapsed_seconds);
}

pub(crate) fn observe_flush_failure(table: &str, stage: &str) {
    metrics().observe_flush_failure(table, stage);
}

pub(crate) fn observe_source_error(stage: &str, kind: &str) {
    metrics().observe_source_error(stage, kind);
}

pub(crate) fn export_metrics() -> Result<Vec<u8>, String> {
    metrics().export()
}

pub async fn health_handler(stale_secs: u64) -> impl IntoResponse {
    let last_flush = metrics().last_successful_flush_timestamp_seconds.get();
    let stale = stale_secs > 0
        && last_flush > 0
        && current_timestamp_seconds().saturating_sub(last_flush) > stale_secs as i64;
    if stale {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    }
}

pub async fn metrics_handler() -> impl IntoResponse {
    match export_metrics() {
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
            warn!("Failed to scrape ingest metrics: {err}");
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

fn clamp_usize(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}
