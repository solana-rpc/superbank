// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

pub(crate) mod blocks;
pub(crate) mod signatures;
pub(crate) mod transactions;
pub(crate) mod types;

use axum::{
    Json,
    body::{Bytes, to_bytes},
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::Value;
use std::future::Future;
use std::io::{self, Write};
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicU8, AtomicU64, Ordering},
};
use std::time::Instant;
use tokio::{sync::Semaphore, task::JoinSet};
use tracing::{Level, debug, error, info};

use crate::clickhouse::{QueryTimings, RoutingScope, RoutingTransport};
use crate::metrics;
use crate::rpc::{JsonRpcInboundRequest, JsonRpcRequest, json_rpc_error_response};
use crate::state::AppState;
use crate::util::{add_superbank_response_metrics_headers, extract_downstream_timings};

const METRICS_UNKNOWN_METHOD: &str = "unknown";
const ROUTE_TRANSPORT_TCP: &str = "tcp";
const ROUTE_TRANSPORT_HTTP: &str = "http";
const ROUTE_SCOPE_DISTRIBUTED: &str = "distributed";
const ROUTE_SCOPE_SHARD_DIRECT: &str = "shard_direct";
pub(crate) const ROUTE_SOURCE_NONE: &str = "none";
pub(crate) const ROUTE_SOURCE_CLICKHOUSE: &str = "clickhouse";
#[cfg(feature = "grpc-head-cache")]
pub(crate) const ROUTE_SOURCE_HEAD_CACHE: &str = "head_cache";
#[cfg(feature = "disk-cache")]
pub(crate) const ROUTE_SOURCE_DISK_CACHE: &str = "disk_cache";
pub(crate) const ROUTE_OUTCOME_SUCCESS: &str = "success";
pub(crate) const ROUTE_OUTCOME_NOT_FOUND: &str = "not_found";
pub(crate) const ROUTE_OUTCOME_INVALID_PARAMS: &str = "invalid_params";
pub(crate) const ROUTE_OUTCOME_RPC_ERROR: &str = "rpc_error";
pub(crate) const ROUTE_OUTCOME_BACKEND_ERROR: &str = "backend_error";
pub(crate) const ROUTE_OUTCOME_TIMEOUT: &str = "timeout";
const ROUTE_HEADER_LABEL_MISSING: &str = "missing";
const HEADER_X_ENDPOINT: &str = "X-Endpoint";
const HEADER_X_RPC_NODE: &str = "X-RPC-Node";
const HEADER_X_SUBSCRIPTION_ID: &str = "X-Subscription-ID";
const HEADER_X_ACCOUNT_ID: &str = "X-Account-ID";
const SOURCE_TOUCHED_CLICKHOUSE_BIT: u8 = 0b0000_0001;
const SOURCE_TOUCHED_HEAD_CACHE_BIT: u8 = 0b0000_0010;
#[cfg(feature = "disk-cache")]
const SOURCE_TOUCHED_DISK_CACHE_BIT: u8 = 0b0000_0100;

#[derive(Debug, Default)]
struct ResponseHeaderMetricsContext {
    source_touched_bits: AtomicU8,
    clickhouse_rows_read: AtomicU64,
    clickhouse_rows_read_unknown: AtomicU8,
    clickhouse_rows_returned: AtomicU64,
    clickhouse_data_read_bytes: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default)]
struct ResponseHeaderMetricsSnapshot {
    source_touched_bits: u8,
    clickhouse_rows_read: u64,
    clickhouse_rows_read_unknown: bool,
    clickhouse_rows_returned: u64,
    clickhouse_data_read_bytes: u64,
}

impl ResponseHeaderMetricsContext {
    fn mark_clickhouse_source_touched(&self) {
        self.source_touched_bits
            .fetch_or(SOURCE_TOUCHED_CLICKHOUSE_BIT, Ordering::Relaxed);
    }

    #[cfg(feature = "grpc-head-cache")]
    fn mark_head_cache_source_touched(&self) {
        self.source_touched_bits
            .fetch_or(SOURCE_TOUCHED_HEAD_CACHE_BIT, Ordering::Relaxed);
    }

    #[cfg(feature = "disk-cache")]
    fn mark_disk_cache_source_touched(&self) {
        self.source_touched_bits
            .fetch_or(SOURCE_TOUCHED_DISK_CACHE_BIT, Ordering::Relaxed);
    }

    fn observe_clickhouse_timings(&self, timings: &QueryTimings) {
        if let Some(rows_read) = timings.rows_read {
            atomic_saturating_add(&self.clickhouse_rows_read, rows_read);
        }

        if timings.rows_read_unknown || timings.rows_read.is_none() {
            self.clickhouse_rows_read_unknown
                .store(1, Ordering::Relaxed);
        }

        atomic_saturating_add(&self.clickhouse_rows_returned, timings.rows_returned);
        atomic_saturating_add(&self.clickhouse_data_read_bytes, timings.decoded_bytes);
    }

    fn snapshot(&self) -> ResponseHeaderMetricsSnapshot {
        ResponseHeaderMetricsSnapshot {
            source_touched_bits: self.source_touched_bits.load(Ordering::Relaxed),
            clickhouse_rows_read: self.clickhouse_rows_read.load(Ordering::Relaxed),
            clickhouse_rows_read_unknown: self.clickhouse_rows_read_unknown.load(Ordering::Relaxed)
                != 0,
            clickhouse_rows_returned: self.clickhouse_rows_returned.load(Ordering::Relaxed),
            clickhouse_data_read_bytes: self.clickhouse_data_read_bytes.load(Ordering::Relaxed),
        }
    }
}

fn atomic_saturating_add(counter: &AtomicU64, value: u64) {
    if value == 0 {
        return;
    }

    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(value))
    });
}

tokio::task_local! {
    static RESPONSE_HEADER_METRICS_CONTEXT: Arc<ResponseHeaderMetricsContext>;
}

async fn with_response_header_metrics_context<F, T>(
    context: Arc<ResponseHeaderMetricsContext>,
    fut: F,
) -> T
where
    F: Future<Output = T>,
{
    RESPONSE_HEADER_METRICS_CONTEXT.scope(context, fut).await
}

async fn with_optional_response_header_metrics_context<F, T>(
    context: Option<Arc<ResponseHeaderMetricsContext>>,
    fut: F,
) -> T
where
    F: Future<Output = T>,
{
    match context {
        Some(context) => with_response_header_metrics_context(context, fut).await,
        None => fut.await,
    }
}

fn current_response_header_metrics_context() -> Option<Arc<ResponseHeaderMetricsContext>> {
    RESPONSE_HEADER_METRICS_CONTEXT.try_with(Arc::clone).ok()
}

fn mark_clickhouse_source_touched() {
    if let Some(context) = current_response_header_metrics_context() {
        context.mark_clickhouse_source_touched();
    }
}

#[cfg(feature = "grpc-head-cache")]
fn mark_head_cache_source_touched() {
    if let Some(context) = current_response_header_metrics_context() {
        context.mark_head_cache_source_touched();
    }
}

#[cfg(feature = "disk-cache")]
fn mark_disk_cache_source_touched() {
    if let Some(context) = current_response_header_metrics_context() {
        context.mark_disk_cache_source_touched();
    }
}

fn observe_clickhouse_timings(timings: &QueryTimings) {
    if let Some(context) = current_response_header_metrics_context() {
        let has_clickhouse_signal = timings.elapsed_ms > 0
            || timings.received_bytes > 0
            || timings.decoded_bytes > 0
            || timings.rows_returned > 0
            || timings.rows_read_unknown
            || timings.rows_read.is_none()
            || timings.rows_read.unwrap_or(0) > 0;
        if has_clickhouse_signal {
            context.mark_clickhouse_source_touched();
        }
        context.observe_clickhouse_timings(timings);
    }
}

fn sources_touched_header_value(source_touched_bits: u8) -> &'static str {
    let clickhouse = (source_touched_bits & SOURCE_TOUCHED_CLICKHOUSE_BIT) != 0;
    let head_cache = (source_touched_bits & SOURCE_TOUCHED_HEAD_CACHE_BIT) != 0;
    #[cfg(feature = "disk-cache")]
    let disk_cache = (source_touched_bits & SOURCE_TOUCHED_DISK_CACHE_BIT) != 0;
    #[cfg(not(feature = "disk-cache"))]
    let disk_cache = false;
    match (head_cache, disk_cache, clickhouse) {
        (false, false, false) => "none",
        (true, false, false) => "head-cache",
        (false, false, true) => "clickhouse",
        // Legacy value, kept for header consumers that predate the disk cache.
        (true, false, true) => "both",
        (false, true, false) => "disk-cache",
        (false, true, true) => "disk-cache,clickhouse",
        (true, true, false) => "head-cache,disk-cache",
        (true, true, true) => "all",
    }
}

fn add_response_metrics_headers(resp: &mut Response, snapshot: ResponseHeaderMetricsSnapshot) {
    let has_clickhouse_metrics = snapshot.clickhouse_rows_read_unknown
        || snapshot.clickhouse_rows_read > 0
        || snapshot.clickhouse_rows_returned > 0
        || snapshot.clickhouse_data_read_bytes > 0;
    let rows_read = if !has_clickhouse_metrics {
        Some(0)
    } else if snapshot.clickhouse_rows_read_unknown {
        None
    } else {
        Some(snapshot.clickhouse_rows_read)
    };

    add_superbank_response_metrics_headers(
        resp,
        sources_touched_header_value(snapshot.source_touched_bits),
        rows_read,
        snapshot.clickhouse_rows_returned,
        snapshot.clickhouse_data_read_bytes,
    );
}

fn header_label_value(headers: &HeaderMap, header_name: &str) -> Option<String> {
    headers
        .get(header_name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn captured_header_metric_label(
    capture: bool,
    headers: &HeaderMap,
    header_name: &str,
) -> Option<String> {
    if !capture {
        return None;
    }

    Some(
        header_label_value(headers, header_name)
            .unwrap_or_else(|| ROUTE_HEADER_LABEL_MISSING.to_string()),
    )
}

fn request_header_metric_labels(
    headers: &HeaderMap,
    capture: &crate::state::MetricsHeaderCaptureConfig,
) -> metrics::RequestHeaderMetricLabels {
    metrics::RequestHeaderMetricLabels {
        x_endpoint: captured_header_metric_label(
            capture.capture_x_endpoint,
            headers,
            HEADER_X_ENDPOINT,
        ),
        x_rpc_node: captured_header_metric_label(
            capture.capture_x_rpc_node,
            headers,
            HEADER_X_RPC_NODE,
        ),
        x_subscription_id: captured_header_metric_label(
            capture.capture_x_subscription_id,
            headers,
            HEADER_X_SUBSCRIPTION_ID,
        ),
        x_account_id: captured_header_metric_label(
            capture.capture_x_account_id,
            headers,
            HEADER_X_ACCOUNT_ID,
        ),
    }
}

pub(crate) struct RouteMetric {
    method: &'static str,
    transport: &'static str,
    scope: &'static str,
    source: &'static str,
    head_cache_read: bool,
    disk_cache_read: bool,
    outcome: &'static str,
    started: Instant,
    timeout: std::time::Duration,
    x_endpoint: Option<String>,
    x_rpc_node: Option<String>,
    x_subscription_id: Option<String>,
    x_account_id: Option<String>,
}

impl RouteMetric {
    pub(crate) fn for_state(method: &'static str, state: &AppState) -> Self {
        let (transport, scope) = Self::transport_scope_for_state(state);
        let request_headers = metrics::current_request_metric_labels();
        Self {
            method,
            transport,
            scope,
            source: ROUTE_SOURCE_NONE,
            head_cache_read: false,
            disk_cache_read: false,
            outcome: ROUTE_OUTCOME_BACKEND_ERROR,
            started: Instant::now(),
            timeout: state.rpc_request_timeout,
            x_endpoint: request_headers.x_endpoint,
            x_rpc_node: request_headers.x_rpc_node,
            x_subscription_id: request_headers.x_subscription_id,
            x_account_id: request_headers.x_account_id,
        }
    }

    pub(crate) fn transport_scope_for_state(state: &AppState) -> (&'static str, &'static str) {
        let transport = match state.clickhouse.routing_policy.transport {
            RoutingTransport::Tcp => ROUTE_TRANSPORT_TCP,
            RoutingTransport::Http => ROUTE_TRANSPORT_HTTP,
        };
        let scope = match state.clickhouse.routing_policy.scope {
            RoutingScope::Distributed => ROUTE_SCOPE_DISTRIBUTED,
            RoutingScope::ShardDirect => ROUTE_SCOPE_SHARD_DIRECT,
        };
        (transport, scope)
    }

    pub(crate) fn source_clickhouse(&mut self) {
        mark_clickhouse_source_touched();
        self.source = ROUTE_SOURCE_CLICKHOUSE;
    }

    #[cfg(feature = "grpc-head-cache")]
    pub(crate) fn source_head_cache(&mut self) {
        mark_head_cache_source_touched();
        self.source = ROUTE_SOURCE_HEAD_CACHE;
        self.head_cache_read = true;
    }

    pub(crate) fn source_none(&mut self) {
        self.source = ROUTE_SOURCE_NONE;
    }

    #[cfg(feature = "grpc-head-cache")]
    pub(crate) fn head_cache_read(&mut self) {
        mark_head_cache_source_touched();
        self.head_cache_read = true;
    }

    #[cfg(feature = "disk-cache")]
    pub(crate) fn source_disk_cache(&mut self) {
        mark_disk_cache_source_touched();
        self.source = ROUTE_SOURCE_DISK_CACHE;
        self.disk_cache_read = true;
    }

    #[cfg(feature = "disk-cache")]
    pub(crate) fn disk_cache_read(&mut self) {
        mark_disk_cache_source_touched();
        self.disk_cache_read = true;
    }

    pub(crate) fn success(&mut self) {
        self.outcome = ROUTE_OUTCOME_SUCCESS;
    }

    pub(crate) fn not_found(&mut self) {
        self.outcome = ROUTE_OUTCOME_NOT_FOUND;
    }

    pub(crate) fn invalid_params(&mut self) {
        self.outcome = ROUTE_OUTCOME_INVALID_PARAMS;
    }

    pub(crate) fn rpc_error(&mut self) {
        self.outcome = ROUTE_OUTCOME_RPC_ERROR;
    }

    pub(crate) fn method(&self) -> &'static str {
        self.method
    }
}

impl Drop for RouteMetric {
    fn drop(&mut self) {
        let outcome = if self.outcome == ROUTE_OUTCOME_BACKEND_ERROR
            && self.started.elapsed() >= self.timeout
        {
            ROUTE_OUTCOME_TIMEOUT
        } else {
            self.outcome
        };
        metrics::route(metrics::RouteMetricLabels {
            method: self.method,
            transport: self.transport,
            scope: self.scope,
            source: self.source,
            head_cache_read: self.head_cache_read,
            disk_cache_read: self.disk_cache_read,
            outcome,
            x_endpoint: self.x_endpoint.as_deref(),
            x_rpc_node: self.x_rpc_node.as_deref(),
            x_subscription_id: self.x_subscription_id.as_deref(),
            x_account_id: self.x_account_id.as_deref(),
        });
    }
}

fn slow_rpc_threshold_ms() -> u64 {
    static THRESHOLD: OnceLock<u64> = OnceLock::new();
    *THRESHOLD.get_or_init(|| {
        std::env::var("SLOW_RPC_MS")
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(250)
    })
}

fn slow_rpc_log_request_max_bytes() -> usize {
    static MAX_BYTES: OnceLock<usize> = OnceLock::new();
    *MAX_BYTES.get_or_init(|| {
        std::env::var("SLOW_RPC_LOG_REQUEST_MAX_BYTES")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            // Keep slow logs readable by default; override by setting env var.
            .unwrap_or(4096)
    })
}

struct LimitedBytesWriter {
    buf: Vec<u8>,
    max_bytes: usize,
    truncated: bool,
}

impl LimitedBytesWriter {
    /// Creates a writer that buffers up to `max_bytes` bytes.
    ///
    /// When `max_bytes == 0`, no truncation is applied.
    fn new(max_bytes: usize) -> Self {
        Self {
            buf: Vec::new(),
            max_bytes,
            truncated: false,
        }
    }

    fn into_string(self) -> String {
        String::from_utf8_lossy(&self.buf).into_owned()
    }
}

impl Write for LimitedBytesWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        // Convention: `max_bytes == 0` means "no truncation".
        if self.max_bytes == 0 {
            self.buf.extend_from_slice(bytes);
            return Ok(bytes.len());
        }

        let remaining = self.max_bytes.saturating_sub(self.buf.len());
        if remaining == 0 {
            self.truncated = true;
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "max bytes reached",
            ));
        }

        if bytes.len() <= remaining {
            self.buf.extend_from_slice(bytes);
            Ok(bytes.len())
        } else {
            self.buf.extend_from_slice(&bytes[..remaining]);
            self.truncated = true;
            Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "max bytes reached",
            ))
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn json_rpc_request_body_for_slow_log(req: &JsonRpcRequest) -> (String, bool) {
    let max_bytes = slow_rpc_log_request_max_bytes();
    let mut writer = LimitedBytesWriter::new(max_bytes);

    match serde_json::to_writer(&mut writer, req) {
        Ok(()) => (writer.into_string(), false),
        Err(err) => {
            if writer.truncated {
                (writer.into_string(), true)
            } else {
                (
                    format!("<failed_to_serialize_json_rpc_request: {err}>"),
                    false,
                )
            }
        }
    }
}

fn metrics_method_label(method: &str) -> &'static str {
    match method {
        "getSignaturesForAddress" => "getSignaturesForAddress",
        "getTransactionsForAddress" => "getTransactionsForAddress",
        "getSignatureStatuses" => "getSignatureStatuses",
        "getBlock" => "getBlock",
        "getBlockHeight" => "getBlockHeight",
        "getSlot" => "getSlot",
        "getTransactionCount" => "getTransactionCount",
        "getLatestBlockhash" => "getLatestBlockhash",
        "getBlockTime" => "getBlockTime",
        "getBlocks" => "getBlocks",
        "getBlocksWithLimit" => "getBlocksWithLimit",
        "getHealth" => "getHealth",
        "getInflationReward" => "getInflationReward",
        "getFirstAvailableBlock" => "getFirstAvailableBlock",
        "minimumLedgerSlot" => "minimumLedgerSlot",
        "getTransaction" => "getTransaction",
        _ => METRICS_UNKNOWN_METHOD,
    }
}

#[derive(Debug)]
struct NormalizedJsonRpcRequest {
    id: Option<Value>,
    method: String,
    params: Option<Vec<Value>>,
}

impl NormalizedJsonRpcRequest {
    fn into_dispatch_request(self) -> JsonRpcRequest {
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: self.id.unwrap_or(Value::Null),
            method: self.method,
            params: self.params,
        }
    }
}

#[derive(Debug)]
struct RequestParseError {
    id: Value,
    message: &'static str,
}

type BatchItemResponse = Value;
type BatchItemTaskResult = Result<BatchItemResponse, StatusCode>;
type BatchTaskOutput = (usize, Value, BatchItemTaskResult);

fn parse_json_rpc_request(value: Value) -> Result<NormalizedJsonRpcRequest, RequestParseError> {
    let request_id = value
        .as_object()
        .and_then(|object| object.get("id").cloned())
        .unwrap_or(Value::Null);

    let request: JsonRpcInboundRequest =
        serde_json::from_value(value).map_err(|_| RequestParseError {
            id: request_id.clone(),
            message: "Invalid Request",
        })?;

    if request.jsonrpc.as_deref() != Some("2.0") {
        return Err(RequestParseError {
            id: request_id,
            message: "Invalid JSON-RPC version",
        });
    }

    let Some(method) = request.method else {
        return Err(RequestParseError {
            id: request_id,
            message: "Invalid Request",
        });
    };

    Ok(NormalizedJsonRpcRequest {
        id: request.id.map(|maybe_id| maybe_id.unwrap_or(Value::Null)),
        method,
        params: request.params,
    })
}

fn json_rpc_error_value(
    id: Value,
    code: i32,
    message: impl Into<String>,
    data: Option<Value>,
) -> Value {
    // Keep behavior aligned with json_rpc_error_response: never leak internals for -32603.
    let (message, data) = if code == -32603 {
        ("Internal error".to_string(), None)
    } else {
        (message.into(), data)
    };

    let mut error = serde_json::Map::new();
    error.insert("code".to_string(), Value::from(code));
    error.insert("message".to_string(), Value::String(message));
    if let Some(data) = data {
        error.insert("data".to_string(), data);
    }

    let mut response = serde_json::Map::new();
    response.insert("jsonrpc".to_string(), Value::String("2.0".to_string()));
    response.insert("id".to_string(), id);
    response.insert("error".to_string(), Value::Object(error));
    Value::Object(response)
}

fn validate_json_rpc_response_value(value: &Value) -> Result<(), &'static str> {
    let response = value
        .as_object()
        .ok_or("response body is not a JSON object")?;

    match response.get("jsonrpc") {
        Some(Value::String(version)) if version == "2.0" => {}
        _ => return Err("response jsonrpc field is missing or invalid"),
    }

    if !response.contains_key("id") {
        return Err("response id field is missing");
    }

    let has_result = response.contains_key("result");
    let has_error = response.contains_key("error");
    if has_result == has_error {
        return Err("response must contain exactly one of result or error");
    }
    if has_error {
        let error = response
            .get("error")
            .ok_or("response error field is missing")?
            .as_object()
            .ok_or("response error field is not an object")?;

        match error.get("code") {
            Some(Value::Number(code)) if code.as_i64().is_some() || code.as_u64().is_some() => {}
            _ => return Err("response error code field is missing or invalid"),
        }

        match error.get("message") {
            Some(Value::String(_)) => {}
            _ => return Err("response error message field is missing or invalid"),
        }
    }

    Ok(())
}

async fn response_to_json_value(response: Response) -> Result<Value, StatusCode> {
    let status = response.status();
    let body = response.into_body();
    let bytes = match to_bytes(body, usize::MAX).await {
        Ok(bytes) => bytes,
        Err(err) => {
            error!("Failed to read JSON-RPC response body: {err}");
            return Err(status);
        }
    };
    let value = match serde_json::from_slice::<Value>(&bytes) {
        Ok(value) => value,
        Err(err) => {
            error!("Failed to deserialize JSON-RPC response body: {err}");
            return Err(status);
        }
    };
    if let Err(reason) = validate_json_rpc_response_value(&value) {
        let preview_len = bytes.len().min(256);
        let preview = String::from_utf8_lossy(&bytes[..preview_len]);
        error!(
            reason,
            response_body_len = bytes.len(),
            response_body_preview = %preview,
            response_body_truncated = bytes.len() > preview_len,
            "Failed to validate JSON-RPC response body"
        );
        return Err(status);
    }
    Ok(value)
}

async fn dispatch_json_rpc_request(
    state: Arc<AppState>,
    req: JsonRpcRequest,
    timeout: Option<std::time::Duration>,
) -> Result<Response, StatusCode> {
    let method = req.method.clone();
    let method_label = metrics_method_label(method.as_str());
    let tracker = metrics::track_request(method_label);
    let start = Instant::now();
    let slow_log_request = tracing::enabled!(Level::INFO).then(|| req.clone());
    let request_metric_labels = metrics::current_request_metric_labels();

    if req.jsonrpc != "2.0" {
        let response = json_rpc_error_response(req.id, -32600, "Invalid JSON-RPC version", None);

        if let Some(tracker) = tracker {
            tracker.observe(response.status());
        }

        let elapsed_ms = start.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        debug!(
            method = method.as_str(),
            status = response.status().as_u16(),
            handler_elapsed_ms = elapsed_ms,
            downstream_elapsed_ms = Option::<u64>::None,
            response_overhead_ms = Option::<u64>::None,
            x_endpoint = request_metric_labels.x_endpoint_for_logs(),
            x_rpc_node = request_metric_labels.x_rpc_node_for_logs(),
            x_subscription_id = request_metric_labels.x_subscription_id_for_logs(),
            x_account_id = request_metric_labels.x_account_id_for_logs(),
        );

        if elapsed_ms >= slow_rpc_threshold_ms() {
            match slow_log_request.as_ref() {
                Some(request) => {
                    let (rpc_request_body, rpc_request_body_truncated) =
                        json_rpc_request_body_for_slow_log(request);
                    info!(
                        method = method.as_str(),
                        status = response.status().as_u16(),
                        handler_elapsed_ms = elapsed_ms,
                        downstream_elapsed_ms = Option::<u64>::None,
                        response_overhead_ms = Option::<u64>::None,
                        x_endpoint = request_metric_labels.x_endpoint_for_logs(),
                        x_rpc_node = request_metric_labels.x_rpc_node_for_logs(),
                        x_subscription_id = request_metric_labels.x_subscription_id_for_logs(),
                        x_account_id = request_metric_labels.x_account_id_for_logs(),
                        rpc_request_body = rpc_request_body.as_str(),
                        rpc_request_body_truncated = rpc_request_body_truncated,
                    );
                }
                None => {
                    info!(
                        method = method.as_str(),
                        status = response.status().as_u16(),
                        handler_elapsed_ms = elapsed_ms,
                        downstream_elapsed_ms = Option::<u64>::None,
                        response_overhead_ms = Option::<u64>::None,
                        x_endpoint = request_metric_labels.x_endpoint_for_logs(),
                        x_rpc_node = request_metric_labels.x_rpc_node_for_logs(),
                        x_subscription_id = request_metric_labels.x_subscription_id_for_logs(),
                        x_account_id = request_metric_labels.x_account_id_for_logs(),
                    );
                }
            }
        }

        return Ok(response);
    }

    let id = req.id;
    let id_for_dispatch = id.clone();
    let params = req.params;
    let method_str = method.as_str();
    let dispatch = Box::pin(async {
        match method_str {
            "getSignaturesForAddress" => {
                signatures::handle_get_signatures_for_address(state, id_for_dispatch, params).await
            }
            "getTransactionsForAddress" => {
                transactions::handle_get_transactions_for_address(state, id_for_dispatch, params)
                    .await
            }
            "getSignatureStatuses" => {
                signatures::handle_get_signature_statuses(state, id_for_dispatch, params).await
            }
            "getBlock" => blocks::handle_get_block(state, id_for_dispatch, params).await,
            "getBlockHeight" => {
                blocks::handle_get_block_height(state, id_for_dispatch, params).await
            }
            "getSlot" => blocks::handle_get_slot(state, id_for_dispatch, params).await,
            "getTransactionCount" => {
                blocks::handle_get_transaction_count(state, id_for_dispatch, params).await
            }
            "getLatestBlockhash" => {
                blocks::handle_get_latest_blockhash(state, id_for_dispatch, params).await
            }
            "getBlockTime" => blocks::handle_get_block_time(state, id_for_dispatch, params).await,
            "getBlocks" => blocks::handle_get_blocks(state, id_for_dispatch, params).await,
            "getBlocksWithLimit" => {
                blocks::handle_get_blocks_with_limit(state, id_for_dispatch, params).await
            }
            "getHealth" => blocks::handle_get_health(state, id_for_dispatch, params).await,
            "getInflationReward" => {
                blocks::handle_get_inflation_reward(state, id_for_dispatch, params).await
            }
            "getFirstAvailableBlock" => {
                blocks::handle_get_first_available_block(state, id_for_dispatch, params).await
            }
            "minimumLedgerSlot" => {
                blocks::handle_minimum_ledger_slot(state, id_for_dispatch, params).await
            }
            "getTransaction" => {
                transactions::handle_get_transaction(state, id_for_dispatch, params).await
            }
            _ => Ok(json_rpc_error_response(
                id_for_dispatch,
                -32601,
                format!("Method not found: {method_str}"),
                None,
            )),
        }
    });
    let result = if let Some(timeout) = timeout {
        match tokio::time::timeout(timeout, dispatch).await {
            Ok(result) => result,
            Err(_) => {
                metrics::rpc_timeout(method_label);
                Ok(json_rpc_error_response(
                    id,
                    -32000,
                    "Request timeout",
                    Some(serde_json::json!({
                        "timeoutMs": timeout.as_millis().min(u128::from(u64::MAX)) as u64,
                    })),
                ))
            }
        }
    } else {
        dispatch.await
    };

    let status = match &result {
        Ok(resp) => resp.status(),
        Err(code) => *code,
    };
    if let Some(tracker) = tracker {
        tracker.observe(status);
    }

    let elapsed_ms = start.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
    match &result {
        Ok(resp) => {
            let downstream_timings = extract_downstream_timings(resp);
            if let Some(timings) = downstream_timings.as_ref() {
                observe_clickhouse_timings(timings);
                metrics::clickhouse_timings(
                    method_label,
                    timings.elapsed_ms,
                    timings.received_bytes,
                    timings.decoded_bytes,
                );
            }
            let downstream_elapsed_ms = downstream_timings.as_ref().map(|t| t.elapsed_ms);
            let response_overhead_ms =
                downstream_elapsed_ms.map(|db_ms| elapsed_ms.saturating_sub(db_ms));
            if let Some(overhead_ms) = response_overhead_ms {
                metrics::response_overhead(method_label, overhead_ms);
            }
            debug!(
                method = method.as_str(),
                status = status.as_u16(),
                handler_elapsed_ms = elapsed_ms,
                downstream_elapsed_ms = downstream_elapsed_ms,
                response_overhead_ms = response_overhead_ms,
                x_endpoint = request_metric_labels.x_endpoint_for_logs(),
                x_rpc_node = request_metric_labels.x_rpc_node_for_logs(),
                x_subscription_id = request_metric_labels.x_subscription_id_for_logs(),
                x_account_id = request_metric_labels.x_account_id_for_logs(),
            );

            if elapsed_ms >= slow_rpc_threshold_ms() {
                match slow_log_request.as_ref() {
                    Some(request) => {
                        let (rpc_request_body, rpc_request_body_truncated) =
                            json_rpc_request_body_for_slow_log(request);
                        info!(
                            method = method.as_str(),
                            status = status.as_u16(),
                            handler_elapsed_ms = elapsed_ms,
                            downstream_elapsed_ms = downstream_elapsed_ms,
                            response_overhead_ms = response_overhead_ms,
                            x_endpoint = request_metric_labels.x_endpoint_for_logs(),
                            x_rpc_node = request_metric_labels.x_rpc_node_for_logs(),
                            x_subscription_id = request_metric_labels.x_subscription_id_for_logs(),
                            x_account_id = request_metric_labels.x_account_id_for_logs(),
                            rpc_request_body = rpc_request_body.as_str(),
                            rpc_request_body_truncated = rpc_request_body_truncated,
                        );
                    }
                    None => {
                        info!(
                            method = method.as_str(),
                            status = status.as_u16(),
                            handler_elapsed_ms = elapsed_ms,
                            downstream_elapsed_ms = downstream_elapsed_ms,
                            response_overhead_ms = response_overhead_ms,
                            x_endpoint = request_metric_labels.x_endpoint_for_logs(),
                            x_rpc_node = request_metric_labels.x_rpc_node_for_logs(),
                            x_subscription_id = request_metric_labels.x_subscription_id_for_logs(),
                            x_account_id = request_metric_labels.x_account_id_for_logs(),
                        );
                    }
                }
            }
        }
        Err(_) => {
            debug!(
                method = method.as_str(),
                status = status.as_u16(),
                handler_elapsed_ms = elapsed_ms,
                downstream_elapsed_ms = Option::<u64>::None,
                response_overhead_ms = Option::<u64>::None,
                x_endpoint = request_metric_labels.x_endpoint_for_logs(),
                x_rpc_node = request_metric_labels.x_rpc_node_for_logs(),
                x_subscription_id = request_metric_labels.x_subscription_id_for_logs(),
                x_account_id = request_metric_labels.x_account_id_for_logs(),
            );

            if elapsed_ms >= slow_rpc_threshold_ms() {
                match slow_log_request.as_ref() {
                    Some(request) => {
                        let (rpc_request_body, rpc_request_body_truncated) =
                            json_rpc_request_body_for_slow_log(request);
                        info!(
                            method = method.as_str(),
                            status = status.as_u16(),
                            handler_elapsed_ms = elapsed_ms,
                            downstream_elapsed_ms = Option::<u64>::None,
                            response_overhead_ms = Option::<u64>::None,
                            x_endpoint = request_metric_labels.x_endpoint_for_logs(),
                            x_rpc_node = request_metric_labels.x_rpc_node_for_logs(),
                            x_subscription_id = request_metric_labels.x_subscription_id_for_logs(),
                            x_account_id = request_metric_labels.x_account_id_for_logs(),
                            rpc_request_body = rpc_request_body.as_str(),
                            rpc_request_body_truncated = rpc_request_body_truncated,
                        );
                    }
                    None => {
                        info!(
                            method = method.as_str(),
                            status = status.as_u16(),
                            handler_elapsed_ms = elapsed_ms,
                            downstream_elapsed_ms = Option::<u64>::None,
                            response_overhead_ms = Option::<u64>::None,
                            x_endpoint = request_metric_labels.x_endpoint_for_logs(),
                            x_rpc_node = request_metric_labels.x_rpc_node_for_logs(),
                            x_subscription_id = request_metric_labels.x_subscription_id_for_logs(),
                            x_account_id = request_metric_labels.x_account_id_for_logs(),
                        );
                    }
                }
            }
        }
    }

    result
}

async fn handle_single_request(
    state: Arc<AppState>,
    request_metric_labels: metrics::RequestHeaderMetricLabels,
    request_value: Value,
) -> Result<Response, StatusCode> {
    let request = match parse_json_rpc_request(request_value) {
        Ok(request) => request,
        Err(err) => {
            return Ok(json_rpc_error_response(err.id, -32600, err.message, None));
        }
    };

    let timeout = state.rpc_request_timeout;
    let dispatch_request = request.into_dispatch_request();
    let response = metrics::with_request_metric_labels(
        request_metric_labels,
        Box::pin(dispatch_json_rpc_request(
            state,
            dispatch_request,
            Some(timeout),
        )),
    )
    .await?;
    Ok(response)
}

async fn execute_batch_requests(
    state: Arc<AppState>,
    request_metric_labels: metrics::RequestHeaderMetricLabels,
    requests: Vec<Value>,
) -> Result<Response, StatusCode> {
    let batch_len = requests.len();
    let parallelism = state.rpc_batch_concurrency_limit.max(1).min(batch_len);
    let semaphore = Arc::new(Semaphore::new(parallelism));

    let mut responses: Vec<Option<BatchItemResponse>> = (0..batch_len).map(|_| None).collect();
    let mut join_set: JoinSet<BatchTaskOutput> = JoinSet::new();

    for (idx, request_value) in requests.into_iter().enumerate() {
        match parse_json_rpc_request(request_value) {
            Ok(request) => {
                let response_id = request.id.clone().unwrap_or(Value::Null);
                let dispatch_request = request.into_dispatch_request();
                let state = state.clone();
                let semaphore = semaphore.clone();
                let request_metric_labels = request_metric_labels.clone();
                let response_header_metrics_context = current_response_header_metrics_context();
                join_set.spawn(async move {
                    let permit = match semaphore.acquire_owned().await {
                        Ok(permit) => permit,
                        Err(_) => {
                            return (idx, response_id, Err(StatusCode::INTERNAL_SERVER_ERROR));
                        }
                    };
                    let _permit = permit;

                    let response = match with_optional_response_header_metrics_context(
                        response_header_metrics_context,
                        metrics::with_request_metric_labels(
                            request_metric_labels,
                            Box::pin(dispatch_json_rpc_request(state, dispatch_request, None)),
                        ),
                    )
                    .await
                    {
                        Ok(response) => response,
                        Err(status) => return (idx, response_id, Err(status)),
                    };
                    let parsed = response_to_json_value(response).await;
                    (idx, response_id, parsed)
                });
            }
            Err(err) => {
                responses[idx] = Some(json_rpc_error_value(err.id, -32600, err.message, None));
            }
        }
    }

    while let Some(joined) = join_set.join_next().await {
        match joined {
            Ok((idx, _response_id, Ok(result))) => {
                responses[idx] = Some(result);
            }
            Ok((idx, response_id, Err(status))) => {
                error!(
                    status = status.as_u16(),
                    "Batch item returned invalid JSON-RPC response"
                );
                responses[idx] = Some(json_rpc_error_value(
                    response_id,
                    -32603,
                    "Internal error",
                    None,
                ));
            }
            Err(err) => {
                error!("Batch item task failed: {err}");
                return Ok(json_rpc_error_response(
                    Value::Null,
                    -32603,
                    "Internal error",
                    None,
                ));
            }
        }
    }

    let mut response_values = Vec::with_capacity(batch_len);
    for value in responses.into_iter().flatten() {
        response_values.push(value);
    }

    Ok(Json(Value::Array(response_values)).into_response())
}

async fn handle_batch_request(
    state: Arc<AppState>,
    request_metric_labels: metrics::RequestHeaderMetricLabels,
    requests: Vec<Value>,
) -> Result<Response, StatusCode> {
    metrics::with_request_metric_labels(request_metric_labels.clone(), async move {
        if requests.is_empty() {
            metrics::batch_rejected("empty");
            return Ok(json_rpc_error_response(
                Value::Null,
                -32600,
                "Invalid Request",
                None,
            ));
        }

        metrics::batch_observed(requests.len() as u64);
        if requests.len() > state.rpc_max_batch_size {
            metrics::batch_rejected("too_many_items");
            return Ok(json_rpc_error_response(
                Value::Null,
                -32600,
                format!(
                    "Invalid Request: batch size {} exceeds max {}",
                    requests.len(),
                    state.rpc_max_batch_size
                ),
                None,
            ));
        }

        let timeout = state.rpc_request_timeout;
        match tokio::time::timeout(
            timeout,
            execute_batch_requests(state, request_metric_labels, requests),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                metrics::batch_rejected("timeout");
                Ok(json_rpc_error_response(
                    Value::Null,
                    -32000,
                    "Request timeout",
                    Some(serde_json::json!({
                        "timeoutMs": timeout.as_millis().min(u128::from(u64::MAX)) as u64,
                    })),
                ))
            }
        }
    })
    .await
}

pub(crate) async fn handle_json_rpc_with_headers(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, StatusCode> {
    let response_header_metrics_context = Arc::new(ResponseHeaderMetricsContext::default());
    let mut response =
        with_response_header_metrics_context(response_header_metrics_context.clone(), async move {
            let request_metric_labels =
                request_header_metric_labels(&headers, &state.metrics_header_capture);
            let payload = match serde_json::from_slice::<Value>(&body) {
                Ok(payload) => payload,
                Err(_) => {
                    return Ok(json_rpc_error_response(
                        Value::Null,
                        -32700,
                        "Parse error",
                        None,
                    ));
                }
            };

            match payload {
                Value::Array(requests) => {
                    handle_batch_request(state, request_metric_labels, requests).await
                }
                Value::Object(_) => {
                    handle_single_request(state, request_metric_labels, payload).await
                }
                _ => Ok(json_rpc_error_response(
                    Value::Null,
                    -32600,
                    "Invalid Request",
                    None,
                )),
            }
        })
        .await?;

    add_response_metrics_headers(&mut response, response_header_metrics_context.snapshot());
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::{
        request_header_metric_labels, response_to_json_value, validate_json_rpc_response_value,
    };
    use axum::{
        Json,
        http::{HeaderMap, HeaderValue, StatusCode},
        response::IntoResponse,
    };
    use serde_json::json;

    use crate::state::MetricsHeaderCaptureConfig;

    #[test]
    fn request_metric_labels_disabled_when_capture_off() {
        let headers = HeaderMap::new();
        let capture = MetricsHeaderCaptureConfig::default();

        let labels = request_header_metric_labels(&headers, &capture);
        assert_eq!(labels.x_endpoint, None);
        assert_eq!(labels.x_rpc_node, None);
        assert_eq!(labels.x_subscription_id, None);
        assert_eq!(labels.x_account_id, None);
    }

    #[test]
    fn request_metric_labels_missing_when_capture_on_and_header_absent() {
        let headers = HeaderMap::new();
        let capture = MetricsHeaderCaptureConfig {
            capture_x_endpoint: true,
            capture_x_rpc_node: true,
            capture_x_subscription_id: true,
            capture_x_account_id: true,
        };

        let labels = request_header_metric_labels(&headers, &capture);
        assert_eq!(labels.x_endpoint.as_deref(), Some("missing"));
        assert_eq!(labels.x_rpc_node.as_deref(), Some("missing"));
        assert_eq!(labels.x_subscription_id.as_deref(), Some("missing"));
        assert_eq!(labels.x_account_id.as_deref(), Some("missing"));
    }

    #[test]
    fn request_metric_labels_captures_x_endpoint_when_configured() {
        let mut headers = HeaderMap::new();
        headers.insert("X-Endpoint", HeaderValue::from_static("abc"));
        headers.insert("X-RPC-Node", HeaderValue::from_static("node-1"));
        headers.insert("X-Subscription-ID", HeaderValue::from_static("sub-1"));
        headers.insert("X-Account-ID", HeaderValue::from_static("acct-1"));
        let capture = MetricsHeaderCaptureConfig {
            capture_x_endpoint: true,
            capture_x_rpc_node: true,
            capture_x_subscription_id: true,
            capture_x_account_id: true,
        };

        let labels = request_header_metric_labels(&headers, &capture);
        assert_eq!(labels.x_endpoint.as_deref(), Some("abc"));
        assert_eq!(labels.x_rpc_node.as_deref(), Some("node-1"));
        assert_eq!(labels.x_subscription_id.as_deref(), Some("sub-1"));
        assert_eq!(labels.x_account_id.as_deref(), Some("acct-1"));
    }

    #[test]
    fn request_metric_labels_treats_whitespace_headers_as_missing() {
        let mut headers = HeaderMap::new();
        headers.insert("X-Subscription-ID", HeaderValue::from_static("   "));
        headers.insert("X-Account-ID", HeaderValue::from_static("\t"));
        let capture = MetricsHeaderCaptureConfig {
            capture_x_endpoint: false,
            capture_x_rpc_node: false,
            capture_x_subscription_id: true,
            capture_x_account_id: true,
        };

        let labels = request_header_metric_labels(&headers, &capture);
        assert_eq!(labels.x_subscription_id.as_deref(), Some("missing"));
        assert_eq!(labels.x_account_id.as_deref(), Some("missing"));
    }

    #[test]
    fn validate_json_rpc_response_rejects_missing_result_and_error() {
        let value = json!({
            "jsonrpc": "2.0",
            "id": 0
        });

        let err = validate_json_rpc_response_value(&value).expect_err("expected invalid response");
        assert_eq!(err, "response must contain exactly one of result or error");
    }

    #[tokio::test]
    async fn response_to_json_value_rejects_missing_result_and_error() {
        let response = Json(json!({
            "jsonrpc": "2.0",
            "id": 0
        }))
        .into_response();

        let err = response_to_json_value(response)
            .await
            .expect_err("expected invalid response");
        assert_eq!(err, StatusCode::OK);
    }

    #[test]
    fn validate_json_rpc_response_accepts_null_result() {
        let value = json!({
            "jsonrpc": "2.0",
            "id": 0,
            "result": null
        });

        validate_json_rpc_response_value(&value).expect("null result should be valid");
    }

    #[test]
    fn validate_json_rpc_response_accepts_null_id() {
        let value = json!({
            "jsonrpc": "2.0",
            "id": null,
            "result": 413285640
        });

        validate_json_rpc_response_value(&value).expect("null id should be valid");
    }

    #[test]
    fn validate_json_rpc_response_rejects_null_error() {
        let value = json!({
            "jsonrpc": "2.0",
            "id": 0,
            "error": null
        });

        let err = validate_json_rpc_response_value(&value).expect_err("expected invalid response");
        assert_eq!(err, "response error field is not an object");
    }

    #[test]
    fn validate_json_rpc_response_rejects_error_without_code() {
        let value = json!({
            "jsonrpc": "2.0",
            "id": 0,
            "error": {
                "message": "Internal error"
            }
        });

        let err = validate_json_rpc_response_value(&value).expect_err("expected invalid response");
        assert_eq!(err, "response error code field is missing or invalid");
    }
}
