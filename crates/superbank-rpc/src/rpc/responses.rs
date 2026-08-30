// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use axum::{
    Json,
    body::{Body, Bytes},
    http::{Response as HttpResponse, header},
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

pub(crate) fn json_rpc_success_response<T>(id: Value, result: T) -> Response
where
    T: Serialize,
{
    Json(JsonRpcResponse {
        jsonrpc: Cow::Borrowed("2.0"),
        id,
        result: Some(result),
        error: None,
    })
    .into_response()
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
        .body(Body::from_stream(stream::iter(chunks)))
        .expect("static JSON-RPC response headers must be valid")
}

pub(crate) fn json_rpc_null_response(id: Value) -> Response {
    Json(JsonRpcResponse::<Value> {
        jsonrpc: Cow::Borrowed("2.0"),
        id,
        result: Some(Value::Null),
        error: None,
    })
    .into_response()
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

    Json(JsonRpcResponse::<Value> {
        jsonrpc: Cow::Borrowed("2.0"),
        id,
        result: None,
        error: Some(JsonRpcError {
            code,
            message,
            data,
        }),
    })
    .into_response()
}

fn json_rpc_solana_custom_error_response(id: Value, error: RpcCustomError) -> Response {
    let error = SolanaJsonRpcError::from(error);
    Json(JsonRpcResponse::<Value> {
        jsonrpc: Cow::Borrowed("2.0"),
        id,
        result: None,
        error: Some(JsonRpcError {
            code: error.code.code() as i32,
            message: error.message,
            data: error.data,
        }),
    })
    .into_response()
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
