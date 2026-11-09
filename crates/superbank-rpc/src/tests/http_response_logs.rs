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

fn exported_count(exported: &str, family: &str, method: &str) -> u64 {
    let prefix = format!("{family}_count{{method=\"{method}\"}} ");
    exported
        .lines()
        .find_map(|line| line.strip_prefix(&prefix)?.parse().ok())
        .unwrap_or(0)
}

fn envelope_counts(method: &str) -> (u64, u64) {
    let exported = String::from_utf8(metrics::export_metrics().expect("metrics export"))
        .expect("metrics are UTF-8");
    (
        exported_count(&exported, "superbank_rpc_envelope_seconds", method),
        exported_count(&exported, "superbank_rpc_response_bytes", method),
    )
}

#[tokio::test]
async fn envelope_metrics_and_status_promotion_cover_single_and_batch() {
    let success = json!({"jsonrpc": "2.0", "id": 1, "method": "getEpochSchedule"});
    let server_error = json!({"jsonrpc": "2.0", "id": 3, "method": "getInflationReward",
        "params": [["11111111111111111111111111111111"], {"epoch": 43}]});
    let cases = [
        (
            false,
            server_error.clone(),
            StatusCode::OK,
            "getInflationReward",
        ),
        (
            true,
            server_error.clone(),
            StatusCode::SERVICE_UNAVAILABLE,
            "getInflationReward",
        ),
        (true, success.clone(), StatusCode::OK, "getEpochSchedule"),
        (
            true,
            json!([success, server_error.clone()]),
            StatusCode::SERVICE_UNAVAILABLE,
            "batch",
        ),
        (true, json!("not a request"), StatusCode::OK, "invalid"),
    ];
    let mut server_error_bodies = Vec::new();
    for (enabled, request, expected, method) in cases {
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
        let (envelopes_before, sizes_before) = envelope_counts(method);
        let response = handle_json_rpc_value(state, &request).await;
        assert_eq!(response.status(), expected, "{request}");
        let (envelopes_after, sizes_after) = envelope_counts(method);
        assert!(envelopes_after > envelopes_before, "{method}");
        assert!(sizes_after > sizes_before, "{method}");

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body");
        if request == server_error {
            server_error_bodies.push(body);
        }
    }
    assert_eq!(server_error_bodies.len(), 2);
    assert_eq!(server_error_bodies[0], server_error_bodies[1]);
}
