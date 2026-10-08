// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::sync::Arc;

use axum::{http::StatusCode, response::Response};
use serde_json::Value;
use solana_rpc_client_api::custom_error::JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_UNREACHABLE;

use crate::handlers::RouteMetric;
use crate::rpc::{json_rpc_error_response, json_rpc_success_response};
use crate::state::AppState;

pub(crate) async fn handle_get_ag_genesis_cert(
    state: Arc<AppState>,
    id: Value,
    params: Option<Vec<Value>>,
) -> Result<Response, StatusCode> {
    let mut route = RouteMetric::for_state("getAgGenesisCert", &state);
    if params.is_some_and(|params| !params.is_empty()) {
        route.invalid_params();
        return Ok(json_rpc_error_response(
            id,
            -32602,
            "Invalid params: expected no parameters",
            None,
        ));
    }
    match state.ag_genesis_cert.get().await {
        Ok(certificate) => {
            route.success();
            Ok(json_rpc_success_response(id, certificate))
        }
        Err(error) => Ok(json_rpc_error_response(
            id,
            JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_UNREACHABLE as i32,
            "Alpenglow genesis certificate source unavailable",
            Some(error.data()),
        )),
    }
}
