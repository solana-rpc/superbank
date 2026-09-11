// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use super::*;

fn capture_response_logs(state: Arc<AppState>, request: &Value) -> (StatusCode, Vec<Value>) {
    let log_buffer = SharedLogBuffer::default();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_max_level(tracing::Level::DEBUG)
        .without_time()
        .with_writer(log_buffer.clone())
        .finish();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let status = tracing::dispatcher::with_default(&tracing::Dispatch::new(subscriber), || {
        tracing::callsite::rebuild_interest_cache();
        runtime
            .block_on(handle_json_rpc_value(state, request))
            .status()
    });
    let logs = log_buffer
        .snapshot()
        .lines()
        .map(|line| serde_json::from_str(line).expect("JSON log event"))
        .collect();
    (status, logs)
}

#[test]
#[ignore = "run in isolation: tracing callsite cache can interfere under parallel test execution"]
fn http_response_logs_match_final_status() {
    let success = json!({"jsonrpc": "2.0", "id": 1, "method": "getEpochSchedule"});
    let client_error = json!({"jsonrpc": "2.0", "id": 2, "method": "unknownMethod"});
    let server_error = json!({"jsonrpc": "2.0", "id": 3, "method": "getInflationReward",
        "params": [["11111111111111111111111111111111"], {"epoch": 43}]});
    let cases = [
        (false, server_error.clone(), StatusCode::OK),
        (true, server_error.clone(), StatusCode::SERVICE_UNAVAILABLE),
        (true, success.clone(), StatusCode::OK),
        (true, client_error.clone(), StatusCode::OK),
        (
            true,
            json!([success, server_error]),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        (true, json!([client_error]), StatusCode::OK),
        (true, json!([]), StatusCode::OK),
    ];
    for (enabled, request, expected) in cases {
        let mut state = test_state();
        Arc::get_mut(&mut state).unwrap().emit_http_errors = enabled;
        // Force a server-side rejection without relying on a database or a timeout race.
        let _permit = state
            .get_inflation_reward_sem
            .as_ref()
            .unwrap()
            .clone()
            .try_acquire_owned()
            .unwrap();
        let (status, events) = capture_response_logs(state, &request);
        assert_eq!(status, expected);
        let envelopes: Vec<_> = events
            .iter()
            .filter(|event| event["fields"]["message"] == "JSON-RPC HTTP response")
            .collect();
        assert_eq!(envelopes.len(), 1, "{events:?}");
        assert_eq!(envelopes[0]["fields"]["status"], expected.as_u16());
        for event in events
            .iter()
            .filter(|event| event["fields"].get("method").is_some())
        {
            assert!(event["fields"].get("status").is_none(), "{event}");
            assert!(event["fields"].get("handler_status").is_some(), "{event}");
        }
    }
}
