// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use axum::{
    Json,
    body::{Body, Bytes},
    http::{Response as HttpResponse, StatusCode, header},
    response::{IntoResponse, Response},
};
use futures_util::stream;
use jsonrpc_core::Error as SolanaJsonRpcError;
use serde::Serialize;
use serde_json::{Value, json};
use solana_rpc_client_api::custom_error::{
    JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_UNREACHABLE, JSON_RPC_SERVER_ERROR_NODE_UNHEALTHY,
    RpcCustomError,
};
use std::borrow::Cow;

use crate::rpc::types::{JsonRpcError, JsonRpcResponse};

pub(crate) const JSON_RPC_INTERNAL_ERROR_CODE: i64 = -32603;
pub(crate) const JSON_RPC_REQUEST_TIMEOUT_CODE: i64 = -32000;
pub(crate) const JSON_RPC_REQUEST_TIMEOUT_MESSAGE: &str = "Request timeout";

/// Whether a single JSON-RPC response carries an error that `--emit-http-errors` promotes to
/// HTTP 503. The builders below attach it, so promotion does not have to read the body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct JsonRpcHttp503(pub(crate) bool);

pub(crate) fn is_http_503_eligible_json_rpc_error(code: i64, message: Option<&str>) -> bool {
    if code == JSON_RPC_INTERNAL_ERROR_CODE {
        return true;
    }
    if code == JSON_RPC_REQUEST_TIMEOUT_CODE {
        return message == Some(JSON_RPC_REQUEST_TIMEOUT_MESSAGE);
    }
    if code == JSON_RPC_SERVER_ERROR_NODE_UNHEALTHY {
        return true;
    }
    if code == JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_UNREACHABLE {
        return true;
    }
    false
}

fn with_http_503_marker(mut response: Response, eligible: bool) -> Response {
    // A failed serialization (HTTP 500, text body) stays unmarked, so promotion scans it.
    if response.status() == StatusCode::OK {
        response.extensions_mut().insert(JsonRpcHttp503(eligible));
    }
    response
}

pub(crate) fn json_rpc_success_response<T>(id: Value, result: T) -> Response
where
    T: Serialize,
{
    let response = Json(JsonRpcResponse {
        jsonrpc: Cow::Borrowed("2.0"),
        id,
        result: Some(result),
        error: None,
    })
    .into_response();
    with_http_503_marker(response, false)
}

pub(crate) fn json_rpc_success_response_from_result_bytes(id: Value, result: Bytes) -> Response {
    let id = serde_json::to_vec(&id).expect("serde_json::Value must serialize");
    let mut prefix = Vec::with_capacity(32 + id.len());
    prefix.extend_from_slice(br#"{"jsonrpc":"2.0","id":"#);
    prefix.extend_from_slice(&id);
    prefix.extend_from_slice(b",\"result\":");

    let suffix = Bytes::from_static(b"}");
    let content_length = prefix.len() + result.len() + suffix.len();
    let chunks = [
        Ok::<Bytes, std::convert::Infallible>(Bytes::from(prefix)),
        Ok(result),
        Ok(suffix),
    ];
    HttpResponse::builder()
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CONTENT_LENGTH, content_length)
        .extension(JsonRpcHttp503(false))
        .body(Body::from_stream(stream::iter(chunks)))
        .expect("static JSON-RPC response headers must be valid")
}

pub(crate) fn json_rpc_null_response(id: Value) -> Response {
    let response = Json(JsonRpcResponse::<Value> {
        jsonrpc: Cow::Borrowed("2.0"),
        id,
        result: Some(Value::Null),
        error: None,
    })
    .into_response();
    with_http_503_marker(response, false)
}

pub(crate) fn json_rpc_error_response(
    id: Value,
    code: i32,
    message: impl Into<String>,
    data: Option<Value>,
) -> Response {
    // Never leak internal error details to the caller. All diagnostics should go to logs.
    //
    // JSON-RPC 2.0 reserves `-32603` for "Internal error". For Solana-compatible behavior,
    // always return a generic message with no data payload.
    let (message, data) = if code == -32603 {
        ("Internal error".to_string(), None)
    } else {
        (message.into(), data)
    };

    let eligible = is_http_503_eligible_json_rpc_error(i64::from(code), Some(&message));
    let response = Json(JsonRpcResponse::<Value> {
        jsonrpc: Cow::Borrowed("2.0"),
        id,
        result: None,
        error: Some(JsonRpcError {
            code,
            message,
            data,
        }),
    })
    .into_response();
    with_http_503_marker(response, eligible)
}

fn json_rpc_solana_custom_error_response(id: Value, error: RpcCustomError) -> Response {
    let error = SolanaJsonRpcError::from(error);
    let code = error.code.code() as i32;
    let eligible = is_http_503_eligible_json_rpc_error(i64::from(code), Some(&error.message));
    let response = Json(JsonRpcResponse::<Value> {
        jsonrpc: Cow::Borrowed("2.0"),
        id,
        result: None,
        error: Some(JsonRpcError {
            code,
            message: error.message,
            data: error.data,
        }),
    })
    .into_response();
    with_http_503_marker(response, eligible)
}

pub(crate) fn json_rpc_internal_error_response(id: Value) -> Response {
    json_rpc_error_response(id, -32603, "Internal error", None)
}

pub(crate) fn json_rpc_node_unhealthy_response(id: Value) -> Response {
    json_rpc_error_response(
        id,
        JSON_RPC_SERVER_ERROR_NODE_UNHEALTHY as i32,
        "Node is unhealthy",
        Some(json!({ "numSlotsBehind": Value::Null })),
    )
}

pub(crate) fn json_rpc_long_term_storage_unreachable_response(id: Value) -> Response {
    json_rpc_error_response(
        id,
        JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_UNREACHABLE as i32,
        "Failed to query long-term storage; please try again",
        None,
    )
}

pub(crate) fn json_rpc_filter_transaction_not_found_response(
    id: Value,
    signature: impl Into<String>,
) -> Response {
    json_rpc_solana_custom_error_response(
        id,
        RpcCustomError::FilterTransactionNotFound {
            signature: signature.into(),
        },
    )
}
