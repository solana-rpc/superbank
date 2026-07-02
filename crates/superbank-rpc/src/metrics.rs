// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::fmt;
use std::future::Future;
#[cfg(feature = "grpc-head-cache")]
use std::sync::Mutex;
use std::time::Instant;

use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use once_cell::sync::Lazy;
use prometheus_client::encoding::text::encode;
use prometheus_client::encoding::{
    EncodeLabel, EncodeLabelSet as EncodeLabelSetTrait, LabelSetEncoder,
};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;
use prometheus_client_derive_encode::EncodeLabelSet;
use thiserror::Error;
use tracing::warn;

/// Global metrics registry and collectors used by the RPC service.
pub static METRICS: Lazy<Result<Metrics, MetricsInitError>> = Lazy::new(Metrics::try_new);

const LATENCY_BUCKETS: [f64; 13] = [
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

fn latency_histogram() -> Histogram {
    Histogram::new(LATENCY_BUCKETS)
}

const BATCH_SIZE_BUCKETS: [f64; 8] = [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0];

fn batch_size_histogram() -> Histogram {
    Histogram::new(BATCH_SIZE_BUCKETS)
}

const BLOCK_SLOT_COUNT_BUCKETS: [f64; 20] = [
    1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0, 2048.0, 4096.0, 8192.0,
    16384.0, 32768.0, 65536.0, 131072.0, 262144.0, 500000.0,
];

fn block_slot_count_histogram() -> Histogram {
    Histogram::new(BLOCK_SLOT_COUNT_BUCKETS)
}

#[cfg(feature = "grpc-head-cache")]
const HEAD_CACHE_ACTIVE_NODE_NONE: &str = "none";
#[cfg(feature = "grpc-head-cache")]
const HEAD_CACHE_ACTIVE_NODE_UNKNOWN: &str = "unknown";
const REQUEST_LABEL_DISABLED: &str = "disabled";

#[derive(Clone, Debug)]
pub(crate) struct RequestHeaderMetricLabels {
    pub(crate) x_endpoint: Option<String>,
    pub(crate) x_rpc_node: Option<String>,
    pub(crate) x_subscription_id: Option<String>,
    pub(crate) x_account_id: Option<String>,
}

impl RequestHeaderMetricLabels {
    pub(crate) fn disabled() -> Self {
        Self {
            x_endpoint: None,
            x_rpc_node: None,
            x_subscription_id: None,
            x_account_id: None,
        }
    }

    pub(crate) fn x_endpoint_for_logs(&self) -> &str {
        self.x_endpoint.as_deref().unwrap_or(REQUEST_LABEL_DISABLED)
    }

    pub(crate) fn x_rpc_node_for_logs(&self) -> &str {
        self.x_rpc_node.as_deref().unwrap_or(REQUEST_LABEL_DISABLED)
    }

    pub(crate) fn x_subscription_id_for_logs(&self) -> &str {
        self.x_subscription_id
            .as_deref()
            .unwrap_or(REQUEST_LABEL_DISABLED)
    }

    pub(crate) fn x_account_id_for_logs(&self) -> &str {
        self.x_account_id
            .as_deref()
            .unwrap_or(REQUEST_LABEL_DISABLED)
    }
}

tokio::task_local! {
    static REQUEST_HEADER_METRIC_LABELS: RequestHeaderMetricLabels;
}

pub(crate) async fn with_request_metric_labels<F, T>(labels: RequestHeaderMetricLabels, fut: F) -> T
where
    F: Future<Output = T>,
{
    REQUEST_HEADER_METRIC_LABELS.scope(labels, fut).await
}

pub(crate) fn current_request_metric_labels() -> RequestHeaderMetricLabels {
    REQUEST_HEADER_METRIC_LABELS
        .try_with(|labels| labels.clone())
        .unwrap_or_else(|_| RequestHeaderMetricLabels::disabled())
}

fn encode_required_label(
    encoder: &mut LabelSetEncoder<'_>,
    key: &str,
    value: &str,
) -> Result<(), fmt::Error> {
    (key, value).encode(encoder.encode_label())
}

fn encode_optional_label(
    encoder: &mut LabelSetEncoder<'_>,
    key: &str,
    value: Option<&str>,
) -> Result<(), fmt::Error> {
    if let Some(value) = value {
        encode_required_label(encoder, key, value)?;
    }
    Ok(())
}

fn encode_request_header_labels(
    encoder: &mut LabelSetEncoder<'_>,
    x_endpoint: Option<&str>,
    x_rpc_node: Option<&str>,
    x_subscription_id: Option<&str>,
    x_account_id: Option<&str>,
) -> Result<(), fmt::Error> {
    encode_optional_label(encoder, "x_endpoint", x_endpoint)?;
    encode_optional_label(encoder, "x_rpc_node", x_rpc_node)?;
    encode_optional_label(encoder, "x_subscription_id", x_subscription_id)?;
    encode_optional_label(encoder, "x_account_id", x_account_id)
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct MethodLabels {
    method: String,
    x_endpoint: Option<String>,
    x_rpc_node: Option<String>,
    x_subscription_id: Option<String>,
    x_account_id: Option<String>,
}

impl EncodeLabelSetTrait for MethodLabels {
    fn encode(&self, mut encoder: LabelSetEncoder<'_>) -> Result<(), fmt::Error> {
        encode_required_label(&mut encoder, "method", self.method.as_str())?;
        encode_request_header_labels(
            &mut encoder,
            self.x_endpoint.as_deref(),
            self.x_rpc_node.as_deref(),
            self.x_subscription_id.as_deref(),
            self.x_account_id.as_deref(),
        )
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct MethodStatusLabels {
    method: String,
    status: String,
    x_endpoint: Option<String>,
    x_rpc_node: Option<String>,
    x_subscription_id: Option<String>,
    x_account_id: Option<String>,
}

impl EncodeLabelSetTrait for MethodStatusLabels {
    fn encode(&self, mut encoder: LabelSetEncoder<'_>) -> Result<(), fmt::Error> {
        encode_required_label(&mut encoder, "method", self.method.as_str())?;
        encode_required_label(&mut encoder, "status", self.status.as_str())?;
        encode_request_header_labels(
            &mut encoder,
            self.x_endpoint.as_deref(),
            self.x_rpc_node.as_deref(),
            self.x_subscription_id.as_deref(),
            self.x_account_id.as_deref(),
        )
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct OperationLabels {
    operation: String,
    x_endpoint: Option<String>,
    x_rpc_node: Option<String>,
    x_subscription_id: Option<String>,
    x_account_id: Option<String>,
}

impl EncodeLabelSetTrait for OperationLabels {
    fn encode(&self, mut encoder: LabelSetEncoder<'_>) -> Result<(), fmt::Error> {
        encode_required_label(&mut encoder, "operation", self.operation.as_str())?;
        encode_request_header_labels(
            &mut encoder,
            self.x_endpoint.as_deref(),
            self.x_rpc_node.as_deref(),
            self.x_subscription_id.as_deref(),
            self.x_account_id.as_deref(),
        )
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct QueryCacheLabels {
    operation: String,
    cache: String,
    x_endpoint: Option<String>,
    x_rpc_node: Option<String>,
    x_subscription_id: Option<String>,
    x_account_id: Option<String>,
}

impl EncodeLabelSetTrait for QueryCacheLabels {
    fn encode(&self, mut encoder: LabelSetEncoder<'_>) -> Result<(), fmt::Error> {
        encode_required_label(&mut encoder, "operation", self.operation.as_str())?;
        encode_required_label(&mut encoder, "cache", self.cache.as_str())?;
        encode_request_header_labels(
            &mut encoder,
            self.x_endpoint.as_deref(),
            self.x_rpc_node.as_deref(),
            self.x_subscription_id.as_deref(),
            self.x_account_id.as_deref(),
        )
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct QueryCacheSettingsLabels {
    operation: String,
    reads: String,
    writes: String,
    ttl: String,
    x_endpoint: Option<String>,
    x_rpc_node: Option<String>,
    x_subscription_id: Option<String>,
    x_account_id: Option<String>,
}

impl EncodeLabelSetTrait for QueryCacheSettingsLabels {
    fn encode(&self, mut encoder: LabelSetEncoder<'_>) -> Result<(), fmt::Error> {
        encode_required_label(&mut encoder, "operation", self.operation.as_str())?;
        encode_required_label(&mut encoder, "reads", self.reads.as_str())?;
        encode_required_label(&mut encoder, "writes", self.writes.as_str())?;
        encode_required_label(&mut encoder, "ttl", self.ttl.as_str())?;
        encode_request_header_labels(
            &mut encoder,
            self.x_endpoint.as_deref(),
            self.x_rpc_node.as_deref(),
            self.x_subscription_id.as_deref(),
            self.x_account_id.as_deref(),
        )
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct OperationTransportReasonLabels {
    operation: &'static str,
    transport: &'static str,
    reason: &'static str,
    x_endpoint: Option<String>,
    x_rpc_node: Option<String>,
    x_subscription_id: Option<String>,
    x_account_id: Option<String>,
}

impl EncodeLabelSetTrait for OperationTransportReasonLabels {
    fn encode(&self, mut encoder: LabelSetEncoder<'_>) -> Result<(), fmt::Error> {
        encode_required_label(&mut encoder, "operation", self.operation)?;
        encode_required_label(&mut encoder, "transport", self.transport)?;
        encode_required_label(&mut encoder, "reason", self.reason)?;
        encode_request_header_labels(
            &mut encoder,
            self.x_endpoint.as_deref(),
            self.x_rpc_node.as_deref(),
            self.x_subscription_id.as_deref(),
            self.x_account_id.as_deref(),
        )
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct OperationOutcomeLabels {
    operation: &'static str,
    outcome: &'static str,
    x_endpoint: Option<String>,
    x_rpc_node: Option<String>,
    x_subscription_id: Option<String>,
    x_account_id: Option<String>,
}

impl EncodeLabelSetTrait for OperationOutcomeLabels {
    fn encode(&self, mut encoder: LabelSetEncoder<'_>) -> Result<(), fmt::Error> {
        encode_required_label(&mut encoder, "operation", self.operation)?;
        encode_required_label(&mut encoder, "outcome", self.outcome)?;
        encode_request_header_labels(
            &mut encoder,
            self.x_endpoint.as_deref(),
            self.x_rpc_node.as_deref(),
            self.x_subscription_id.as_deref(),
            self.x_account_id.as_deref(),
        )
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct TransportFallbackLabels {
    operation: &'static str,
    from: &'static str,
    to: &'static str,
    reason: &'static str,
    x_endpoint: Option<String>,
    x_rpc_node: Option<String>,
    x_subscription_id: Option<String>,
    x_account_id: Option<String>,
}

impl EncodeLabelSetTrait for TransportFallbackLabels {
    fn encode(&self, mut encoder: LabelSetEncoder<'_>) -> Result<(), fmt::Error> {
        encode_required_label(&mut encoder, "operation", self.operation)?;
        encode_required_label(&mut encoder, "from", self.from)?;
        encode_required_label(&mut encoder, "to", self.to)?;
        encode_required_label(&mut encoder, "reason", self.reason)?;
        encode_request_header_labels(
            &mut encoder,
            self.x_endpoint.as_deref(),
            self.x_rpc_node.as_deref(),
            self.x_subscription_id.as_deref(),
            self.x_account_id.as_deref(),
        )
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct RouteLabels {
    method: String,
    transport: String,
    scope: String,
    source: String,
    head_cache_read: String,
    disk_cache_read: String,
    outcome: String,
    x_endpoint: Option<String>,
    x_rpc_node: Option<String>,
    x_subscription_id: Option<String>,
    x_account_id: Option<String>,
}

impl EncodeLabelSetTrait for RouteLabels {
    fn encode(&self, mut encoder: LabelSetEncoder<'_>) -> Result<(), fmt::Error> {
        encode_required_label(&mut encoder, "method", self.method.as_str())?;
        encode_required_label(&mut encoder, "transport", self.transport.as_str())?;
        encode_required_label(&mut encoder, "scope", self.scope.as_str())?;
        encode_required_label(&mut encoder, "source", self.source.as_str())?;
        encode_required_label(
            &mut encoder,
            "head_cache_read",
            self.head_cache_read.as_str(),
        )?;
        encode_required_label(
            &mut encoder,
            "disk_cache_read",
            self.disk_cache_read.as_str(),
        )?;
        encode_required_label(&mut encoder, "outcome", self.outcome.as_str())?;
        encode_request_header_labels(
            &mut encoder,
            self.x_endpoint.as_deref(),
            self.x_rpc_node.as_deref(),
            self.x_subscription_id.as_deref(),
            self.x_account_id.as_deref(),
        )
    }
}

pub(crate) struct RouteMetricLabels<'a> {
    pub(crate) method: &'a str,
    pub(crate) transport: &'a str,
    pub(crate) scope: &'a str,
    pub(crate) source: &'a str,
    pub(crate) head_cache_read: bool,
    pub(crate) disk_cache_read: bool,
    pub(crate) outcome: &'a str,
    pub(crate) x_endpoint: Option<&'a str>,
    pub(crate) x_rpc_node: Option<&'a str>,
    pub(crate) x_subscription_id: Option<&'a str>,
    pub(crate) x_account_id: Option<&'a str>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct SlotSourceLabels {
    operation: String,
    source: String,
    commitment: String,
}

#[cfg(feature = "disk-cache")]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct DiskCacheReadLabels {
    operation: String,
    outcome: String,
}

#[cfg(feature = "disk-cache")]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct DiskCacheReasonLabels {
    reason: String,
}

#[cfg(feature = "disk-cache")]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct DiskCacheSourceLabels {
    source: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct BatchRejectLabels {
    reason: String,
    x_endpoint: Option<String>,
    x_rpc_node: Option<String>,
    x_subscription_id: Option<String>,
    x_account_id: Option<String>,
}

impl EncodeLabelSetTrait for BatchRejectLabels {
    fn encode(&self, mut encoder: LabelSetEncoder<'_>) -> Result<(), fmt::Error> {
        encode_required_label(&mut encoder, "reason", self.reason.as_str())?;
        encode_request_header_labels(
            &mut encoder,
            self.x_endpoint.as_deref(),
            self.x_rpc_node.as_deref(),
            self.x_subscription_id.as_deref(),
            self.x_account_id.as_deref(),
        )
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct BatchLabels {
    x_endpoint: Option<String>,
    x_rpc_node: Option<String>,
    x_subscription_id: Option<String>,
    x_account_id: Option<String>,
}

impl EncodeLabelSetTrait for BatchLabels {
    fn encode(&self, mut encoder: LabelSetEncoder<'_>) -> Result<(), fmt::Error> {
        encode_request_header_labels(
            &mut encoder,
            self.x_endpoint.as_deref(),
            self.x_rpc_node.as_deref(),
            self.x_subscription_id.as_deref(),
            self.x_account_id.as_deref(),
        )
    }
}

#[cfg(feature = "grpc-head-cache")]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct HeadCacheActiveLabels {
    x_rpc_node: String,
}

#[cfg(feature = "grpc-streaming")]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct SuperbankGrpcMethodLabels {
    method: String,
}

#[cfg(feature = "grpc-streaming")]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct SuperbankGrpcErrorLabels {
    method: String,
    stage: String,
}

#[derive(Debug, Error)]
pub enum MetricsInitError {
    #[allow(dead_code)]
    #[error("metrics initialization failed: {0}")]
    Init(String),
}

pub struct Metrics {
    registry: Registry,

    rpc_requests: Family<MethodStatusLabels, Counter>,
    rpc_latency_seconds: Family<MethodStatusLabels, Histogram>,

    rpc_inflight: Family<MethodLabels, Gauge>,
    rpc_timeouts: Family<MethodLabels, Counter>,
    rpc_batch_requests: Family<BatchLabels, Counter>,
    rpc_batch_items: Family<BatchLabels, Counter>,
    rpc_batch_size: Family<BatchLabels, Histogram>,
    rpc_batch_rejected: Family<BatchRejectLabels, Counter>,
    rpc_response_overhead_seconds: Family<MethodLabels, Histogram>,
    rpc_blocks_slots_returned: Family<MethodLabels, Histogram>,

    backend_errors: Family<OperationLabels, Counter>,

    route_total: Family<RouteLabels, Counter>,
    slot_source: Family<SlotSourceLabels, Counter>,

    clickhouse_latency_seconds: Family<MethodLabels, Histogram>,
    clickhouse_received_bytes: Family<MethodLabels, Counter>,
    clickhouse_decoded_bytes: Family<MethodLabels, Counter>,
    clickhouse_timeouts: Family<OperationLabels, Counter>,
    clickhouse_query_cache: Family<QueryCacheLabels, Counter>,
    clickhouse_query_cache_settings: Family<QueryCacheSettingsLabels, Counter>,
    clickhouse_shard_query_aborts: Family<OperationTransportReasonLabels, Counter>,
    clickhouse_shard_query_cleanup: Family<OperationOutcomeLabels, Counter>,
    clickhouse_transport_fallbacks: Family<TransportFallbackLabels, Counter>,

    #[cfg(feature = "grpc-head-cache")]
    head_cache_active: Family<HeadCacheActiveLabels, Gauge>,
    #[cfg(feature = "grpc-head-cache")]
    head_cache_active_current_node: Mutex<Option<String>>,
    #[cfg(feature = "grpc-head-cache")]
    head_cache_reconnects_total: Counter,
    #[cfg(feature = "grpc-head-cache")]
    head_cache_blocks_total: Counter,
    #[cfg(feature = "grpc-head-cache")]
    head_cache_transactions_total: Counter,
    #[cfg(feature = "grpc-head-cache")]
    head_cache_dropped_slots_total: Counter,
    #[cfg(feature = "grpc-head-cache")]
    head_cache_latest_slot: Gauge,
    #[cfg(feature = "grpc-head-cache")]
    head_cache_tx_entries: Gauge,
    #[cfg(feature = "grpc-head-cache")]
    head_cache_address_entries: Gauge,
    #[cfg(feature = "grpc-head-cache")]
    head_cache_slot_entries: Gauge,

    #[cfg(feature = "grpc-streaming")]
    superbank_grpc_stream_requests_total: Family<SuperbankGrpcMethodLabels, Counter>,
    #[cfg(feature = "grpc-streaming")]
    superbank_grpc_stream_chunks_total: Family<SuperbankGrpcMethodLabels, Counter>,
    #[cfg(feature = "grpc-streaming")]
    superbank_grpc_stream_messages_total: Family<SuperbankGrpcMethodLabels, Counter>,
    #[cfg(feature = "grpc-streaming")]
    superbank_grpc_stream_errors_total: Family<SuperbankGrpcErrorLabels, Counter>,

    #[cfg(feature = "disk-cache")]
    disk_cache_active: Gauge,
    #[cfg(feature = "disk-cache")]
    disk_cache_reads_total: Family<DiskCacheReadLabels, Counter>,
    #[cfg(feature = "disk-cache")]
    disk_cache_covered_min_slot: Gauge,
    #[cfg(feature = "disk-cache")]
    disk_cache_covered_max_slot: Gauge,
    #[cfg(feature = "disk-cache")]
    disk_cache_contiguous_floor_slot: Gauge,
    #[cfg(feature = "disk-cache")]
    disk_cache_size_bytes: Gauge,
    #[cfg(feature = "disk-cache")]
    disk_cache_writes_total: Family<DiskCacheSourceLabels, Counter>,
    #[cfg(feature = "disk-cache")]
    disk_cache_written_transactions_total: Counter,
    #[cfg(feature = "disk-cache")]
    disk_cache_write_latency_seconds: Histogram,
    #[cfg(feature = "disk-cache")]
    disk_cache_write_errors_total: Counter,
    #[cfg(feature = "disk-cache")]
    disk_cache_dropped_to_repair_total: Family<DiskCacheReasonLabels, Counter>,
    #[cfg(feature = "disk-cache")]
    disk_cache_evicted_slots_total: Family<DiskCacheReasonLabels, Counter>,
    #[cfg(feature = "disk-cache")]
    disk_cache_backfill_slots_remaining: Gauge,
    #[cfg(feature = "disk-cache")]
    disk_cache_fill_errors_total: Counter,
    #[cfg(feature = "disk-cache")]
    disk_cache_poisoned_slots_total: Counter,
    #[cfg(feature = "disk-cache")]
    disk_cache_wipes_total: Counter,
}

impl Metrics {
    fn try_new() -> Result<Self, MetricsInitError> {
        let rpc_requests = Family::default();
        let rpc_latency_seconds =
            Family::new_with_constructor(latency_histogram as fn() -> Histogram);

        let rpc_inflight = Family::default();
        let rpc_timeouts = Family::default();
        let rpc_batch_requests = Family::default();
        let rpc_batch_items = Family::default();
        let rpc_batch_size =
            Family::new_with_constructor(batch_size_histogram as fn() -> Histogram);
        let rpc_batch_rejected = Family::default();
        let rpc_response_overhead_seconds =
            Family::new_with_constructor(latency_histogram as fn() -> Histogram);
        let rpc_blocks_slots_returned =
            Family::new_with_constructor(block_slot_count_histogram as fn() -> Histogram);

        let backend_errors = Family::default();

        let route_total = Family::default();
        let slot_source = Family::default();

        let clickhouse_latency_seconds =
            Family::new_with_constructor(latency_histogram as fn() -> Histogram);
        let clickhouse_received_bytes = Family::default();
        let clickhouse_decoded_bytes = Family::default();
        let clickhouse_timeouts = Family::default();
        let clickhouse_query_cache = Family::default();
        let clickhouse_query_cache_settings = Family::default();
        let clickhouse_shard_query_aborts = Family::default();
        let clickhouse_shard_query_cleanup = Family::default();
        let clickhouse_transport_fallbacks = Family::default();

        #[cfg(feature = "grpc-head-cache")]
        let head_cache_active = Family::default();
        #[cfg(feature = "grpc-head-cache")]
        let head_cache_active_current_node = Mutex::new(None);
        #[cfg(feature = "grpc-head-cache")]
        let head_cache_reconnects_total = Counter::default();
        #[cfg(feature = "grpc-head-cache")]
        let head_cache_blocks_total = Counter::default();
        #[cfg(feature = "grpc-head-cache")]
        let head_cache_transactions_total = Counter::default();
        #[cfg(feature = "grpc-head-cache")]
        let head_cache_dropped_slots_total = Counter::default();
        #[cfg(feature = "grpc-head-cache")]
        let head_cache_latest_slot = Gauge::default();
        #[cfg(feature = "grpc-head-cache")]
        let head_cache_tx_entries = Gauge::default();
        #[cfg(feature = "grpc-head-cache")]
        let head_cache_address_entries = Gauge::default();
        #[cfg(feature = "grpc-head-cache")]
        let head_cache_slot_entries = Gauge::default();

        #[cfg(feature = "grpc-streaming")]
        let superbank_grpc_stream_requests_total = Family::default();
        #[cfg(feature = "grpc-streaming")]
        let superbank_grpc_stream_chunks_total = Family::default();
        #[cfg(feature = "grpc-streaming")]
        let superbank_grpc_stream_messages_total = Family::default();
        #[cfg(feature = "grpc-streaming")]
        let superbank_grpc_stream_errors_total = Family::default();

        #[cfg(feature = "disk-cache")]
        let disk_cache_active = Gauge::default();
        #[cfg(feature = "disk-cache")]
        let disk_cache_reads_total = Family::default();
        #[cfg(feature = "disk-cache")]
        let disk_cache_covered_min_slot = Gauge::default();
        #[cfg(feature = "disk-cache")]
        let disk_cache_covered_max_slot = Gauge::default();
        #[cfg(feature = "disk-cache")]
        let disk_cache_contiguous_floor_slot = Gauge::default();
        #[cfg(feature = "disk-cache")]
        let disk_cache_size_bytes = Gauge::default();
        #[cfg(feature = "disk-cache")]
        let disk_cache_writes_total = Family::default();
        #[cfg(feature = "disk-cache")]
        let disk_cache_written_transactions_total = Counter::default();
        #[cfg(feature = "disk-cache")]
        let disk_cache_write_latency_seconds = latency_histogram();
        #[cfg(feature = "disk-cache")]
        let disk_cache_write_errors_total = Counter::default();
        #[cfg(feature = "disk-cache")]
        let disk_cache_dropped_to_repair_total = Family::default();
        #[cfg(feature = "disk-cache")]
        let disk_cache_evicted_slots_total = Family::default();
        #[cfg(feature = "disk-cache")]
        let disk_cache_backfill_slots_remaining = Gauge::default();
        #[cfg(feature = "disk-cache")]
        let disk_cache_fill_errors_total = Counter::default();
        #[cfg(feature = "disk-cache")]
        let disk_cache_poisoned_slots_total = Counter::default();
        #[cfg(feature = "disk-cache")]
        let disk_cache_wipes_total = Counter::default();

        let mut registry = Registry::with_prefix("superbank");

        registry.register(
            "rpc_requests",
            "Total JSON-RPC requests handled by superbank-rpc",
            rpc_requests.clone(),
        );
        registry.register(
            "rpc_response_time_seconds",
            "JSON-RPC response latency in seconds",
            rpc_latency_seconds.clone(),
        );
        registry.register(
            "rpc_inflight_requests",
            "In-flight JSON-RPC requests",
            rpc_inflight.clone(),
        );
        registry.register(
            "rpc_timeouts",
            "Total JSON-RPC requests that exceeded the server timeout",
            rpc_timeouts.clone(),
        );
        registry.register(
            "rpc_batch_requests",
            "Total JSON-RPC batch request envelopes handled by superbank-rpc",
            rpc_batch_requests.clone(),
        );
        registry.register(
            "rpc_batch_items",
            "Total JSON-RPC request items observed inside batch envelopes",
            rpc_batch_items.clone(),
        );
        registry.register(
            "rpc_batch_size",
            "Batch size distribution for JSON-RPC envelopes",
            rpc_batch_size.clone(),
        );
        registry.register(
            "rpc_batch_rejected_total",
            "Total JSON-RPC batch request envelopes rejected by reason",
            rpc_batch_rejected.clone(),
        );
        registry.register(
            "rpc_response_overhead_seconds",
            "Estimated non-ClickHouse response overhead in seconds by method",
            rpc_response_overhead_seconds.clone(),
        );
        registry.register(
            "rpc_blocks_slots_returned",
            "Distribution of slot counts returned by getBlocks/getBlocksWithLimit",
            rpc_blocks_slots_returned.clone(),
        );
        registry.register(
            "rpc_backend_errors",
            "Backend (ClickHouse) errors encountered while serving RPC",
            backend_errors.clone(),
        );
        registry.register(
            "rpc_route_total",
            "Normalized route labels for RPC handler outcomes",
            route_total.clone(),
        );
        registry.register(
            "rpc_slot_source",
            "Source chosen for latest-slot resolution by operation and commitment",
            slot_source.clone(),
        );
        registry.register(
            "rpc_clickhouse_duration_seconds",
            "ClickHouse query duration in seconds for JSON-RPC requests",
            clickhouse_latency_seconds.clone(),
        );
        registry.register(
            "rpc_clickhouse_received_bytes",
            "Total bytes received from ClickHouse for JSON-RPC requests",
            clickhouse_received_bytes.clone(),
        );
        registry.register(
            "rpc_clickhouse_decoded_bytes",
            "Total bytes decoded from ClickHouse for JSON-RPC requests",
            clickhouse_decoded_bytes.clone(),
        );
        registry.register(
            "rpc_clickhouse_timeouts",
            "Total ClickHouse operations that exceeded the configured timeout",
            clickhouse_timeouts.clone(),
        );
        registry.register(
            "rpc_clickhouse_query_cache_total",
            "Total ClickHouse read operations classified as query-cache eligible or bypassed",
            clickhouse_query_cache.clone(),
        );
        registry.register(
            "rpc_clickhouse_query_cache_settings_total",
            "Total ClickHouse read operations where query-cache settings were applied",
            clickhouse_query_cache_settings.clone(),
        );
        registry.register(
            "rpc_clickhouse_shard_query_aborts_total",
            "Total shard-local ClickHouse queries abandoned before completion by transport and reason",
            clickhouse_shard_query_aborts.clone(),
        );
        registry.register(
            "rpc_clickhouse_shard_query_cleanup_total",
            "Total shard-local ClickHouse cleanup attempts and outcomes for abandoned queries",
            clickhouse_shard_query_cleanup.clone(),
        );
        registry.register(
            "rpc_clickhouse_transport_fallback_total",
            "Total ClickHouse transport or scope fallbacks by operation, source, destination, and reason",
            clickhouse_transport_fallbacks.clone(),
        );

        #[cfg(feature = "grpc-head-cache")]
        {
            registry.register(
                "head_cache_active",
                "Whether the gRPC head cache is enabled (1) or disabled (0), labeled by x_rpc_node",
                head_cache_active.clone(),
            );
            registry.register(
                "head_cache_reconnects",
                "Total reconnection attempts for the DragonsMouth gRPC head cache",
                head_cache_reconnects_total.clone(),
            );
            registry.register(
                "head_cache_blocks",
                "Total frozen blocks ingested into the gRPC head cache",
                head_cache_blocks_total.clone(),
            );
            registry.register(
                "head_cache_transactions",
                "Total transactions ingested into the gRPC head cache",
                head_cache_transactions_total.clone(),
            );
            registry.register(
                "head_cache_dropped_slots",
                "Total slots dropped from the gRPC head cache due to fork/dead block detection",
                head_cache_dropped_slots_total.clone(),
            );
            registry.register(
                "head_cache_latest_slot",
                "Latest slot observed by the gRPC head cache",
                head_cache_latest_slot.clone(),
            );
            registry.register(
                "head_cache_tx_entries",
                "Current number of transactions stored in the gRPC head cache",
                head_cache_tx_entries.clone(),
            );
            registry.register(
                "head_cache_address_entries",
                "Current number of address entries stored in the gRPC head cache",
                head_cache_address_entries.clone(),
            );
            registry.register(
                "head_cache_slot_entries",
                "Current number of slot entries tracked by the gRPC head cache",
                head_cache_slot_entries.clone(),
            );
        }

        #[cfg(feature = "grpc-streaming")]
        {
            registry.register(
                "grpc_stream_requests",
                "Total accepted Superbank gRPC stream requests by method",
                superbank_grpc_stream_requests_total.clone(),
            );
            registry.register(
                "grpc_stream_chunks",
                "Total ClickHouse slot-range chunks fetched by Superbank gRPC streams",
                superbank_grpc_stream_chunks_total.clone(),
            );
            registry.register(
                "grpc_stream_messages",
                "Total response messages emitted by Superbank gRPC streams",
                superbank_grpc_stream_messages_total.clone(),
            );
            registry.register(
                "grpc_stream_errors",
                "Total Superbank gRPC stream errors by method and stage",
                superbank_grpc_stream_errors_total.clone(),
            );
        }

        #[cfg(feature = "disk-cache")]
        {
            registry.register(
                "disk_cache_active",
                "Whether the disk cache is open and serving (1) or disabled (0)",
                disk_cache_active.clone(),
            );
            registry.register(
                "disk_cache_reads",
                "Disk-cache read outcomes per operation",
                disk_cache_reads_total.clone(),
            );
            registry.register(
                "disk_cache_covered_min_slot",
                "Oldest slot covered by the disk cache",
                disk_cache_covered_min_slot.clone(),
            );
            registry.register(
                "disk_cache_covered_max_slot",
                "Newest slot covered by the disk cache",
                disk_cache_covered_max_slot.clone(),
            );
            registry.register(
                "disk_cache_contiguous_floor_slot",
                "Floor of the contiguous tip span (the gSFA-safe boundary)",
                disk_cache_contiguous_floor_slot.clone(),
            );
            registry.register(
                "disk_cache_size_bytes",
                "Live SST bytes across all disk-cache column families",
                disk_cache_size_bytes.clone(),
            );
            registry.register(
                "disk_cache_writes",
                "Slots committed to the disk cache by source",
                disk_cache_writes_total.clone(),
            );
            registry.register(
                "disk_cache_written_transactions",
                "Transactions written to the disk cache",
                disk_cache_written_transactions_total.clone(),
            );
            registry.register(
                "disk_cache_write_latency_seconds",
                "Per slot-batch commit latency",
                disk_cache_write_latency_seconds.clone(),
            );
            registry.register(
                "disk_cache_write_errors",
                "RocksDB write failures",
                disk_cache_write_errors_total.clone(),
            );
            registry.register(
                "disk_cache_dropped_to_repair",
                "Live slots deferred to the repair queue by reason",
                disk_cache_dropped_to_repair_total.clone(),
            );
            registry.register(
                "disk_cache_evicted_slots",
                "Slots evicted below the retention floor by reason",
                disk_cache_evicted_slots_total.clone(),
            );
            registry.register(
                "disk_cache_backfill_slots_remaining",
                "Holes remaining inside the retention window",
                disk_cache_backfill_slots_remaining.clone(),
            );
            registry.register(
                "disk_cache_fill_errors",
                "ClickHouse fetch failures in the backfill/repair filler",
                disk_cache_fill_errors_total.clone(),
            );
            registry.register(
                "disk_cache_poisoned_slots",
                "Slots dropped from coverage after decode/consistency failures",
                disk_cache_poisoned_slots_total.clone(),
            );
            registry.register(
                "disk_cache_wipes",
                "Times the disk cache database was destroyed and rebuilt",
                disk_cache_wipes_total.clone(),
            );
        }

        // Register process metrics to expose basic runtime health info (CPU, memory).
        if let Err(err) =
            kubert_prometheus_process::register(registry.sub_registry_with_prefix("process"))
        {
            warn!("Failed to register process collector: {err}");
        }

        Ok(Self {
            registry,
            rpc_requests,
            rpc_latency_seconds,
            rpc_inflight,
            rpc_timeouts,
            rpc_batch_requests,
            rpc_batch_items,
            rpc_batch_size,
            rpc_batch_rejected,
            rpc_response_overhead_seconds,
            rpc_blocks_slots_returned,
            backend_errors,
            route_total,
            slot_source,
            clickhouse_latency_seconds,
            clickhouse_received_bytes,
            clickhouse_decoded_bytes,
            clickhouse_timeouts,
            clickhouse_query_cache,
            clickhouse_query_cache_settings,
            clickhouse_shard_query_aborts,
            clickhouse_shard_query_cleanup,
            clickhouse_transport_fallbacks,
            #[cfg(feature = "grpc-head-cache")]
            head_cache_active,
            #[cfg(feature = "grpc-head-cache")]
            head_cache_active_current_node,
            #[cfg(feature = "grpc-head-cache")]
            head_cache_reconnects_total,
            #[cfg(feature = "grpc-head-cache")]
            head_cache_blocks_total,
            #[cfg(feature = "grpc-head-cache")]
            head_cache_transactions_total,
            #[cfg(feature = "grpc-head-cache")]
            head_cache_dropped_slots_total,
            #[cfg(feature = "grpc-head-cache")]
            head_cache_latest_slot,
            #[cfg(feature = "grpc-head-cache")]
            head_cache_tx_entries,
            #[cfg(feature = "grpc-head-cache")]
            head_cache_address_entries,
            #[cfg(feature = "grpc-head-cache")]
            head_cache_slot_entries,
            #[cfg(feature = "grpc-streaming")]
            superbank_grpc_stream_requests_total,
            #[cfg(feature = "grpc-streaming")]
            superbank_grpc_stream_chunks_total,
            #[cfg(feature = "grpc-streaming")]
            superbank_grpc_stream_messages_total,
            #[cfg(feature = "grpc-streaming")]
            superbank_grpc_stream_errors_total,
            #[cfg(feature = "disk-cache")]
            disk_cache_active,
            #[cfg(feature = "disk-cache")]
            disk_cache_reads_total,
            #[cfg(feature = "disk-cache")]
            disk_cache_covered_min_slot,
            #[cfg(feature = "disk-cache")]
            disk_cache_covered_max_slot,
            #[cfg(feature = "disk-cache")]
            disk_cache_contiguous_floor_slot,
            #[cfg(feature = "disk-cache")]
            disk_cache_size_bytes,
            #[cfg(feature = "disk-cache")]
            disk_cache_writes_total,
            #[cfg(feature = "disk-cache")]
            disk_cache_written_transactions_total,
            #[cfg(feature = "disk-cache")]
            disk_cache_write_latency_seconds,
            #[cfg(feature = "disk-cache")]
            disk_cache_write_errors_total,
            #[cfg(feature = "disk-cache")]
            disk_cache_dropped_to_repair_total,
            #[cfg(feature = "disk-cache")]
            disk_cache_evicted_slots_total,
            #[cfg(feature = "disk-cache")]
            disk_cache_backfill_slots_remaining,
            #[cfg(feature = "disk-cache")]
            disk_cache_fill_errors_total,
            #[cfg(feature = "disk-cache")]
            disk_cache_poisoned_slots_total,
            #[cfg(feature = "disk-cache")]
            disk_cache_wipes_total,
        })
    }

    fn method_labels_from_request(
        method: &str,
        request_labels: &RequestHeaderMetricLabels,
    ) -> MethodLabels {
        MethodLabels {
            method: method.to_string(),
            x_endpoint: request_labels.x_endpoint.clone(),
            x_rpc_node: request_labels.x_rpc_node.clone(),
            x_subscription_id: request_labels.x_subscription_id.clone(),
            x_account_id: request_labels.x_account_id.clone(),
        }
    }

    fn current_method_labels(method: &str) -> MethodLabels {
        let request_labels = current_request_metric_labels();
        Self::method_labels_from_request(method, &request_labels)
    }

    fn method_status_labels_from_request(
        method: &str,
        status: StatusCode,
        request_labels: &RequestHeaderMetricLabels,
    ) -> MethodStatusLabels {
        MethodStatusLabels {
            method: method.to_string(),
            status: status.as_u16().to_string(),
            x_endpoint: request_labels.x_endpoint.clone(),
            x_rpc_node: request_labels.x_rpc_node.clone(),
            x_subscription_id: request_labels.x_subscription_id.clone(),
            x_account_id: request_labels.x_account_id.clone(),
        }
    }

    fn current_operation_labels(operation: &str) -> OperationLabels {
        let request_labels = current_request_metric_labels();
        OperationLabels {
            operation: operation.to_string(),
            x_endpoint: request_labels.x_endpoint,
            x_rpc_node: request_labels.x_rpc_node,
            x_subscription_id: request_labels.x_subscription_id,
            x_account_id: request_labels.x_account_id,
        }
    }

    fn current_query_cache_labels(operation: &str, cache: &str) -> QueryCacheLabels {
        let request_labels = current_request_metric_labels();
        QueryCacheLabels {
            operation: operation.to_string(),
            cache: cache.to_string(),
            x_endpoint: request_labels.x_endpoint,
            x_rpc_node: request_labels.x_rpc_node,
            x_subscription_id: request_labels.x_subscription_id,
            x_account_id: request_labels.x_account_id,
        }
    }

    fn current_query_cache_settings_labels(
        operation: &str,
        reads: bool,
        writes: bool,
        ttl_seconds: u64,
    ) -> QueryCacheSettingsLabels {
        let request_labels = current_request_metric_labels();
        QueryCacheSettingsLabels {
            operation: operation.to_string(),
            reads: if reads {
                "1".to_string()
            } else {
                "0".to_string()
            },
            writes: if writes {
                "1".to_string()
            } else {
                "0".to_string()
            },
            ttl: ttl_seconds.to_string(),
            x_endpoint: request_labels.x_endpoint,
            x_rpc_node: request_labels.x_rpc_node,
            x_subscription_id: request_labels.x_subscription_id,
            x_account_id: request_labels.x_account_id,
        }
    }

    fn current_operation_transport_reason_labels(
        operation: &'static str,
        transport: &'static str,
        reason: &'static str,
    ) -> OperationTransportReasonLabels {
        let request_labels = current_request_metric_labels();
        OperationTransportReasonLabels {
            operation,
            transport,
            reason,
            x_endpoint: request_labels.x_endpoint,
            x_rpc_node: request_labels.x_rpc_node,
            x_subscription_id: request_labels.x_subscription_id,
            x_account_id: request_labels.x_account_id,
        }
    }

    fn current_operation_outcome_labels(
        operation: &'static str,
        outcome: &'static str,
    ) -> OperationOutcomeLabels {
        let request_labels = current_request_metric_labels();
        OperationOutcomeLabels {
            operation,
            outcome,
            x_endpoint: request_labels.x_endpoint,
            x_rpc_node: request_labels.x_rpc_node,
            x_subscription_id: request_labels.x_subscription_id,
            x_account_id: request_labels.x_account_id,
        }
    }

    fn current_transport_fallback_labels(
        operation: &'static str,
        from: &'static str,
        to: &'static str,
        reason: &'static str,
    ) -> TransportFallbackLabels {
        let request_labels = current_request_metric_labels();
        TransportFallbackLabels {
            operation,
            from,
            to,
            reason,
            x_endpoint: request_labels.x_endpoint,
            x_rpc_node: request_labels.x_rpc_node,
            x_subscription_id: request_labels.x_subscription_id,
            x_account_id: request_labels.x_account_id,
        }
    }

    fn current_batch_labels() -> BatchLabels {
        let request_labels = current_request_metric_labels();
        BatchLabels {
            x_endpoint: request_labels.x_endpoint,
            x_rpc_node: request_labels.x_rpc_node,
            x_subscription_id: request_labels.x_subscription_id,
            x_account_id: request_labels.x_account_id,
        }
    }

    fn current_batch_reject_labels(reason: &str) -> BatchRejectLabels {
        let request_labels = current_request_metric_labels();
        BatchRejectLabels {
            reason: reason.to_string(),
            x_endpoint: request_labels.x_endpoint,
            x_rpc_node: request_labels.x_rpc_node,
            x_subscription_id: request_labels.x_subscription_id,
            x_account_id: request_labels.x_account_id,
        }
    }

    pub fn track_request(&self, method: impl Into<String>) -> RequestTracker<'_> {
        let method = method.into();
        let request_labels = current_request_metric_labels();
        let method_labels = Self::method_labels_from_request(method.as_str(), &request_labels);
        self.rpc_inflight.get_or_create(&method_labels).inc();
        RequestTracker {
            metrics: self,
            method,
            request_labels,
            start: Instant::now(),
        }
    }

    fn observe(
        &self,
        method: &str,
        request_labels: &RequestHeaderMetricLabels,
        status: StatusCode,
        elapsed: f64,
    ) {
        let labels = Self::method_status_labels_from_request(method, status, request_labels);
        self.rpc_requests.get_or_create(&labels).inc();
        self.rpc_latency_seconds
            .get_or_create(&labels)
            .observe(elapsed);
    }

    fn observe_clickhouse(
        &self,
        method: &str,
        elapsed_ms: u64,
        received_bytes: u64,
        decoded_bytes: u64,
    ) {
        let labels = Self::current_method_labels(method);
        let elapsed = elapsed_ms as f64 / 1000.0;
        self.clickhouse_latency_seconds
            .get_or_create(&labels)
            .observe(elapsed);
        self.clickhouse_received_bytes
            .get_or_create(&labels)
            .inc_by(received_bytes);
        self.clickhouse_decoded_bytes
            .get_or_create(&labels)
            .inc_by(decoded_bytes);
    }

    pub fn response_overhead(&self, method: &str, elapsed_ms: u64) {
        let labels = Self::current_method_labels(method);
        self.rpc_response_overhead_seconds
            .get_or_create(&labels)
            .observe(elapsed_ms as f64 / 1000.0);
    }

    pub fn blocks_slots_returned(&self, method: &str, slots: usize) {
        let labels = Self::current_method_labels(method);
        self.rpc_blocks_slots_returned
            .get_or_create(&labels)
            .observe(slots as f64);
    }

    pub fn backend_error(&self, operation: &str) {
        let labels = Self::current_operation_labels(operation);
        self.backend_errors.get_or_create(&labels).inc();
    }

    pub fn rpc_timeout(&self, method: &str) {
        let labels = Self::current_method_labels(method);
        self.rpc_timeouts.get_or_create(&labels).inc();
    }

    pub fn batch_observed(&self, items: u64) {
        let labels = Self::current_batch_labels();
        self.rpc_batch_requests.get_or_create(&labels).inc();
        self.rpc_batch_items.get_or_create(&labels).inc_by(items);
        self.rpc_batch_size
            .get_or_create(&labels)
            .observe(items as f64);
    }

    pub fn batch_rejected(&self, reason: &str) {
        let labels = Self::current_batch_reject_labels(reason);
        self.rpc_batch_rejected.get_or_create(&labels).inc();
    }

    pub fn clickhouse_timeout(&self, operation: &str) {
        let labels = Self::current_operation_labels(operation);
        self.clickhouse_timeouts.get_or_create(&labels).inc();
    }

    pub fn clickhouse_query_cache_classified(&self, operation: &str, eligible: bool) {
        let labels = Self::current_query_cache_labels(
            operation,
            if eligible { "eligible" } else { "bypassed" },
        );
        self.clickhouse_query_cache.get_or_create(&labels).inc();
    }

    pub fn clickhouse_query_cache_settings_applied(
        &self,
        operation: &str,
        reads: bool,
        writes: bool,
        ttl_seconds: u64,
    ) {
        let labels =
            Self::current_query_cache_settings_labels(operation, reads, writes, ttl_seconds);
        self.clickhouse_query_cache_settings
            .get_or_create(&labels)
            .inc();
    }

    pub fn clickhouse_shard_query_abort(
        &self,
        operation: &'static str,
        transport: &'static str,
        reason: &'static str,
    ) {
        let labels = Self::current_operation_transport_reason_labels(operation, transport, reason);
        self.clickhouse_shard_query_aborts
            .get_or_create(&labels)
            .inc();
    }

    pub fn clickhouse_shard_query_cleanup(&self, operation: &'static str, outcome: &'static str) {
        let labels = Self::current_operation_outcome_labels(operation, outcome);
        self.clickhouse_shard_query_cleanup
            .get_or_create(&labels)
            .inc();
    }

    pub fn clickhouse_transport_fallback(
        &self,
        operation: &'static str,
        from: &'static str,
        to: &'static str,
        reason: &'static str,
    ) {
        let labels = Self::current_transport_fallback_labels(operation, from, to, reason);
        self.clickhouse_transport_fallbacks
            .get_or_create(&labels)
            .inc();
    }

    pub fn route(&self, labels: RouteMetricLabels<'_>) {
        self.route_total
            .get_or_create(&RouteLabels {
                method: labels.method.to_string(),
                transport: labels.transport.to_string(),
                scope: labels.scope.to_string(),
                source: labels.source.to_string(),
                head_cache_read: if labels.head_cache_read {
                    "true".to_string()
                } else {
                    "false".to_string()
                },
                disk_cache_read: if labels.disk_cache_read {
                    "true".to_string()
                } else {
                    "false".to_string()
                },
                outcome: labels.outcome.to_string(),
                x_endpoint: labels.x_endpoint.map(str::to_string),
                x_rpc_node: labels.x_rpc_node.map(str::to_string),
                x_subscription_id: labels.x_subscription_id.map(str::to_string),
                x_account_id: labels.x_account_id.map(str::to_string),
            })
            .inc();
    }

    pub fn slot_source(&self, operation: &str, source: &str, commitment: &str) {
        self.slot_source
            .get_or_create(&SlotSourceLabels {
                operation: operation.to_string(),
                source: source.to_string(),
                commitment: commitment.to_string(),
            })
            .inc();
    }

    pub fn export(&self) -> Result<Vec<u8>, String> {
        let mut buffer = String::new();
        encode(&mut buffer, &self.registry)
            .map_err(|e| format!("failed to encode metrics: {e}"))?;
        Ok(buffer.into_bytes())
    }

    #[cfg(feature = "grpc-head-cache")]
    fn set_head_cache_active_value(&self, x_rpc_node: &str, value: i64) {
        self.head_cache_active
            .get_or_create(&HeadCacheActiveLabels {
                x_rpc_node: x_rpc_node.to_string(),
            })
            .set(value);
    }

    #[cfg(feature = "grpc-head-cache")]
    fn normalize_head_cache_node_label(node: &str) -> &str {
        let trimmed = node.trim();
        if trimmed.is_empty() {
            HEAD_CACHE_ACTIVE_NODE_UNKNOWN
        } else {
            trimmed
        }
    }

    #[cfg(feature = "grpc-head-cache")]
    fn head_cache_set_active(&self, active: bool) {
        if !active {
            let previous = match self.head_cache_active_current_node.lock() {
                Ok(mut guard) => guard.take(),
                Err(poisoned) => {
                    let mut guard = poisoned.into_inner();
                    guard.take()
                }
            };

            if let Some(previous) = previous {
                self.set_head_cache_active_value(previous.as_str(), 0);
            }
            self.set_head_cache_active_value(HEAD_CACHE_ACTIVE_NODE_NONE, 0);
            return;
        }

        let label = match self.head_cache_active_current_node.lock() {
            Ok(mut guard) => guard
                .get_or_insert_with(|| HEAD_CACHE_ACTIVE_NODE_UNKNOWN.to_string())
                .clone(),
            Err(poisoned) => {
                let mut guard = poisoned.into_inner();
                guard
                    .get_or_insert_with(|| HEAD_CACHE_ACTIVE_NODE_UNKNOWN.to_string())
                    .clone()
            }
        };

        self.set_head_cache_active_value(HEAD_CACHE_ACTIVE_NODE_NONE, 0);
        self.set_head_cache_active_value(label.as_str(), 1);
    }

    #[cfg(feature = "grpc-head-cache")]
    fn head_cache_set_active_node(&self, x_rpc_node: &str) {
        let normalized = Self::normalize_head_cache_node_label(x_rpc_node).to_string();
        let previous = match self.head_cache_active_current_node.lock() {
            Ok(mut guard) => {
                if guard.as_deref() == Some(normalized.as_str()) {
                    None
                } else {
                    guard.replace(normalized.clone())
                }
            }
            Err(poisoned) => {
                let mut guard = poisoned.into_inner();
                if guard.as_deref() == Some(normalized.as_str()) {
                    None
                } else {
                    guard.replace(normalized.clone())
                }
            }
        };

        if let Some(previous) = previous {
            self.set_head_cache_active_value(previous.as_str(), 0);
        }

        self.set_head_cache_active_value(HEAD_CACHE_ACTIVE_NODE_NONE, 0);
        self.set_head_cache_active_value(normalized.as_str(), 1);
    }
}

fn metrics() -> Option<&'static Metrics> {
    METRICS.as_ref().ok()
}

pub(crate) fn force_init() -> Result<(), &'static MetricsInitError> {
    match METRICS.as_ref() {
        Ok(_) => Ok(()),
        Err(err) => Err(err),
    }
}

pub(crate) fn track_request(method: &str) -> Option<RequestTracker<'static>> {
    metrics().map(|metrics| metrics.track_request(method))
}

pub(crate) fn backend_error(operation: &str) {
    if let Some(metrics) = metrics() {
        metrics.backend_error(operation);
    }
}

pub(crate) fn rpc_timeout(method: &str) {
    if let Some(metrics) = metrics() {
        metrics.rpc_timeout(method);
    }
}

pub(crate) fn batch_observed(items: u64) {
    if let Some(metrics) = metrics() {
        metrics.batch_observed(items);
    }
}

pub(crate) fn batch_rejected(reason: &str) {
    if let Some(metrics) = metrics() {
        metrics.batch_rejected(reason);
    }
}

pub(crate) fn clickhouse_timeout(operation: &str) {
    if let Some(metrics) = metrics() {
        metrics.clickhouse_timeout(operation);
    }
}

pub(crate) fn clickhouse_query_cache_classified(operation: &str, eligible: bool) {
    if let Some(metrics) = metrics() {
        metrics.clickhouse_query_cache_classified(operation, eligible);
    }
}

pub(crate) fn clickhouse_query_cache_settings_applied(
    operation: &str,
    reads: bool,
    writes: bool,
    ttl_seconds: u64,
) {
    if let Some(metrics) = metrics() {
        metrics.clickhouse_query_cache_settings_applied(operation, reads, writes, ttl_seconds);
    }
}

pub(crate) fn clickhouse_shard_query_abort(
    operation: &'static str,
    transport: &'static str,
    reason: &'static str,
) {
    if let Some(metrics) = metrics() {
        metrics.clickhouse_shard_query_abort(operation, transport, reason);
    }
}

pub(crate) fn clickhouse_shard_query_cleanup(operation: &'static str, outcome: &'static str) {
    if let Some(metrics) = metrics() {
        metrics.clickhouse_shard_query_cleanup(operation, outcome);
    }
}

pub(crate) fn clickhouse_transport_fallback(
    operation: &'static str,
    from: &'static str,
    to: &'static str,
    reason: &'static str,
) {
    if let Some(metrics) = metrics() {
        metrics.clickhouse_transport_fallback(operation, from, to, reason);
    }
}

pub(crate) fn route(labels: RouteMetricLabels<'_>) {
    if let Some(metrics) = metrics() {
        metrics.route(labels);
    }
}

pub(crate) fn slot_source(operation: &str, source: &str, commitment: &str) {
    if let Some(metrics) = metrics() {
        metrics.slot_source(operation, source, commitment);
    }
}

pub(crate) fn clickhouse_timings(
    method: &str,
    elapsed_ms: u64,
    received_bytes: u64,
    decoded_bytes: u64,
) {
    if let Some(metrics) = metrics() {
        metrics.observe_clickhouse(method, elapsed_ms, received_bytes, decoded_bytes);
    }
}

pub(crate) fn response_overhead(method: &str, elapsed_ms: u64) {
    if let Some(metrics) = metrics() {
        metrics.response_overhead(method, elapsed_ms);
    }
}

pub(crate) fn blocks_slots_returned(method: &str, slots: usize) {
    if let Some(metrics) = metrics() {
        metrics.blocks_slots_returned(method, slots);
    }
}

pub(crate) fn export_metrics() -> Result<Vec<u8>, String> {
    match metrics() {
        Some(metrics) => metrics.export(),
        None => Err("metrics are not initialized".to_string()),
    }
}

#[cfg(feature = "grpc-head-cache")]
fn clamp_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[cfg(feature = "grpc-head-cache")]
fn clamp_i64_usize(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[cfg(feature = "grpc-head-cache")]
pub(crate) fn head_cache_set_active(active: bool) {
    if let Some(metrics) = metrics() {
        metrics.head_cache_set_active(active);
    }
}

#[cfg(feature = "grpc-head-cache")]
pub(crate) fn head_cache_set_active_node(x_rpc_node: &str) {
    if let Some(metrics) = metrics() {
        metrics.head_cache_set_active_node(x_rpc_node);
    }
}

#[cfg(feature = "grpc-head-cache")]
pub(crate) fn head_cache_reconnect() {
    if let Some(metrics) = metrics() {
        metrics.head_cache_reconnects_total.inc();
    }
}

#[cfg(feature = "grpc-head-cache")]
pub(crate) fn head_cache_observe_block(
    latest_slot: u64,
    ingested_txs: u64,
    tx_entries: usize,
    address_entries: usize,
    slot_entries: usize,
) {
    if let Some(metrics) = metrics() {
        metrics.head_cache_blocks_total.inc();
        metrics.head_cache_transactions_total.inc_by(ingested_txs);
        metrics.head_cache_latest_slot.set(clamp_i64(latest_slot));
        metrics
            .head_cache_tx_entries
            .set(clamp_i64_usize(tx_entries));
        metrics
            .head_cache_address_entries
            .set(clamp_i64_usize(address_entries));
        metrics
            .head_cache_slot_entries
            .set(clamp_i64_usize(slot_entries));
    }
}

#[cfg(feature = "grpc-head-cache")]
pub(crate) fn head_cache_drop_slot(
    latest_slot: u64,
    tx_entries: usize,
    address_entries: usize,
    slot_entries: usize,
) {
    if let Some(metrics) = metrics() {
        metrics.head_cache_dropped_slots_total.inc();
        metrics.head_cache_latest_slot.set(clamp_i64(latest_slot));
        metrics
            .head_cache_tx_entries
            .set(clamp_i64_usize(tx_entries));
        metrics
            .head_cache_address_entries
            .set(clamp_i64_usize(address_entries));
        metrics
            .head_cache_slot_entries
            .set(clamp_i64_usize(slot_entries));
    }
}

#[cfg(feature = "grpc-streaming")]
fn superbank_grpc_method_labels(method: &'static str) -> SuperbankGrpcMethodLabels {
    SuperbankGrpcMethodLabels {
        method: method.to_string(),
    }
}

#[cfg(feature = "grpc-streaming")]
pub(crate) fn superbank_grpc_stream_started(method: &'static str) {
    if let Some(metrics) = metrics() {
        metrics
            .superbank_grpc_stream_requests_total
            .get_or_create(&superbank_grpc_method_labels(method))
            .inc();
    }
}

#[cfg(feature = "grpc-streaming")]
pub(crate) fn superbank_grpc_stream_chunk(method: &'static str) {
    if let Some(metrics) = metrics() {
        metrics
            .superbank_grpc_stream_chunks_total
            .get_or_create(&superbank_grpc_method_labels(method))
            .inc();
    }
}

#[cfg(feature = "grpc-streaming")]
pub(crate) fn superbank_grpc_stream_message(method: &'static str) {
    if let Some(metrics) = metrics() {
        metrics
            .superbank_grpc_stream_messages_total
            .get_or_create(&superbank_grpc_method_labels(method))
            .inc();
    }
}

#[cfg(feature = "grpc-streaming")]
pub(crate) fn superbank_grpc_stream_error(method: &'static str, stage: &'static str) {
    if let Some(metrics) = metrics() {
        metrics
            .superbank_grpc_stream_errors_total
            .get_or_create(&SuperbankGrpcErrorLabels {
                method: method.to_string(),
                stage: stage.to_string(),
            })
            .inc();
    }
}

#[cfg(feature = "disk-cache")]
pub(crate) fn disk_cache_set_active(active: bool) {
    if let Some(metrics) = metrics() {
        metrics.disk_cache_active.set(i64::from(active));
    }
}

#[cfg(feature = "disk-cache")]
pub(crate) fn disk_cache_read(operation: &'static str, outcome: &'static str) {
    if let Some(metrics) = metrics() {
        metrics
            .disk_cache_reads_total
            .get_or_create(&DiskCacheReadLabels {
                operation: operation.to_string(),
                outcome: outcome.to_string(),
            })
            .inc();
    }
}

#[cfg(feature = "disk-cache")]
pub(crate) fn disk_cache_coverage(min_covered: u64, max_covered: u64, contiguous_floor: u64) {
    if let Some(metrics) = metrics() {
        metrics
            .disk_cache_covered_min_slot
            .set(clamp_i64(min_covered));
        metrics
            .disk_cache_covered_max_slot
            .set(clamp_i64(max_covered));
        metrics
            .disk_cache_contiguous_floor_slot
            .set(clamp_i64(contiguous_floor));
    }
}

#[cfg(feature = "disk-cache")]
pub(crate) fn disk_cache_size_bytes(bytes: u64) {
    if let Some(metrics) = metrics() {
        metrics.disk_cache_size_bytes.set(clamp_i64(bytes));
    }
}

#[cfg(feature = "disk-cache")]
pub(crate) fn disk_cache_write(source: &'static str, transactions: u64, latency_seconds: f64) {
    if let Some(metrics) = metrics() {
        metrics
            .disk_cache_writes_total
            .get_or_create(&DiskCacheSourceLabels {
                source: source.to_string(),
            })
            .inc();
        metrics
            .disk_cache_written_transactions_total
            .inc_by(transactions);
        metrics
            .disk_cache_write_latency_seconds
            .observe(latency_seconds);
    }
}

#[cfg(feature = "disk-cache")]
pub(crate) fn disk_cache_write_error() {
    if let Some(metrics) = metrics() {
        metrics.disk_cache_write_errors_total.inc();
    }
}

#[cfg(feature = "disk-cache")]
pub(crate) fn disk_cache_dropped_to_repair(reason: &'static str) {
    if let Some(metrics) = metrics() {
        metrics
            .disk_cache_dropped_to_repair_total
            .get_or_create(&DiskCacheReasonLabels {
                reason: reason.to_string(),
            })
            .inc();
    }
}

#[cfg(feature = "disk-cache")]
pub(crate) fn disk_cache_evicted(reason: &'static str, slots: u64) {
    if let Some(metrics) = metrics() {
        metrics
            .disk_cache_evicted_slots_total
            .get_or_create(&DiskCacheReasonLabels {
                reason: reason.to_string(),
            })
            .inc_by(slots);
    }
}

#[cfg(feature = "disk-cache")]
pub(crate) fn disk_cache_backfill_remaining(slots: u64) {
    if let Some(metrics) = metrics() {
        metrics
            .disk_cache_backfill_slots_remaining
            .set(clamp_i64(slots));
    }
}

#[cfg(feature = "disk-cache")]
pub(crate) fn disk_cache_fill_error() {
    if let Some(metrics) = metrics() {
        metrics.disk_cache_fill_errors_total.inc();
    }
}

#[cfg(feature = "disk-cache")]
pub(crate) fn disk_cache_poisoned_slot() {
    if let Some(metrics) = metrics() {
        metrics.disk_cache_poisoned_slots_total.inc();
    }
}

#[cfg(feature = "disk-cache")]
pub(crate) fn disk_cache_wipe() {
    if let Some(metrics) = metrics() {
        metrics.disk_cache_wipes_total.inc();
    }
}

pub struct RequestTracker<'a> {
    metrics: &'a Metrics,
    method: String,
    request_labels: RequestHeaderMetricLabels,
    start: Instant,
}

impl<'a> RequestTracker<'a> {
    pub fn observe(self, status: StatusCode) {
        let elapsed = self.start.elapsed().as_secs_f64();
        self.metrics
            .observe(self.method.as_str(), &self.request_labels, status, elapsed);
        // Gauge decrement handled in Drop
    }
}

impl Drop for RequestTracker<'_> {
    fn drop(&mut self) {
        self.metrics
            .rpc_inflight
            .get_or_create(&Metrics::method_labels_from_request(
                self.method.as_str(),
                &self.request_labels,
            ))
            .dec();
    }
}

/// Axum handler that renders the current Prometheus metrics exposition.
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
            warn!("Failed to scrape metrics: {err}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}
