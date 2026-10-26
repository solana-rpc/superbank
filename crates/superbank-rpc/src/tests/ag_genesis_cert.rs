// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use super::*;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::{Json, Router, body::Body, http::StatusCode, response::Response, routing::post};
use clap::Parser;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

struct HttpServer {
    url: String,
    task: JoinHandle<()>,
}

impl Drop for HttpServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve(router: Router) -> HttpServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    HttpServer { url, task }
}

struct Reply {
    status: StatusCode,
    body: String,
    delay: Duration,
    body_delay: Duration,
    chunked: bool,
}

impl Reply {
    fn result(value: Value) -> Self {
        Self::body(json!({"jsonrpc": "2.0", "id": 1, "result": value}))
    }

    fn body(value: Value) -> Self {
        Self {
            status: StatusCode::OK,
            body: value.to_string(),
            delay: Duration::ZERO,
            body_delay: Duration::ZERO,
            chunked: false,
        }
    }
}

async fn upstream(replies: Vec<Reply>) -> (HttpServer, Arc<AtomicUsize>) {
    let replies = Arc::new(Mutex::new(VecDeque::from(replies)));
    let requests = Arc::new(AtomicUsize::new(0));
    let counter = requests.clone();
    let app = Router::new().route(
        "/",
        post(move |Json(request): Json<Value>| {
            let replies = replies.clone();
            let counter = counter.clone();
            async move {
                assert_eq!(
                    request,
                    json!({"jsonrpc": "2.0", "id": 1, "method": "getAgGenesisCert"})
                );
                counter.fetch_add(1, Ordering::Relaxed);
                let reply = replies
                    .lock()
                    .await
                    .pop_front()
                    .expect("unexpected upstream call");
                tokio::time::sleep(reply.delay).await;
                let body = if reply.chunked {
                    Body::from_stream(futures_util::stream::once(async move {
                        tokio::time::sleep(reply.body_delay).await;
                        Ok::<_, std::io::Error>(reply.body)
                    }))
                } else {
                    Body::from(reply.body)
                };
                Response::builder()
                    .status(reply.status)
                    .header(axum::http::header::LOCATION, "/")
                    .body(body)
                    .unwrap()
            }
        }),
    );
    (serve(app).await, requests)
}

fn source(url: &str) -> AgGenesisCertSource {
    let config =
        RpcConfig::try_parse_from(["superbank-rpc", "--ag-genesis-cert-rpc-url", url]).unwrap();
    AgGenesisCertSource::from_config(&config).unwrap()
}

async fn rpc_server(
    source: AgGenesisCertSource,
    emit_http_errors: bool,
) -> (HttpServer, Arc<crate::state::AppState>) {
    let mut state = Arc::try_unwrap(crate::tests::test_state()).ok().unwrap();
    state.ag_genesis_cert = source;
    state.emit_http_errors = emit_http_errors;
    let state = Arc::new(state);
    let app = Router::new()
        .route("/", post(crate::handlers::handle_json_rpc_with_headers))
        .with_state(state.clone());
    (serve(app).await, state)
}

async fn post_rpc(server: &HttpServer, request: Value) -> (StatusCode, Value) {
    let response = Client::new()
        .post(&server.url)
        .json(&request)
        .send()
        .await
        .unwrap();
    (response.status(), response.json().await.unwrap())
}

async fn call(server: &HttpServer) -> (StatusCode, Value) {
    post_rpc(
        server,
        json!({"jsonrpc": "2.0", "id": "client-id", "method": "getAgGenesisCert"}),
    )
    .await
}

fn certificate() -> Value {
    // Synthetic wire fixture, not a cryptographically verified certificate.
    json!({
        "block": {"slot": u64::MAX, "blockId": (0..32).collect::<Vec<u8>>()},
        "signature": {"signature": (0..192).collect::<Vec<u8>>(), "bitmap": [0, 1, 128, 255]}
    })
}

async fn expire(source: &AgGenesisCertSource) {
    source.cache.lock().await.as_mut().unwrap().expires_at = Some(Instant::now());
}

fn assert_source_error(body: &Value, reason: &str) {
    assert!(
        body.get("result").is_none(),
        "failure must not become null: {body}"
    );
    assert_eq!(body["error"]["code"], -32019);
    assert_eq!(body["error"]["data"]["reason"], reason);
}

#[tokio::test]
async fn get_ag_genesis_cert_http_null_and_migrated_serialization() {
    let expected = certificate();
    let (upstream, requests) = upstream(vec![
        Reply::result(Value::Null),
        Reply::result(expected.clone()),
    ])
    .await;
    let (rpc, state) = rpc_server(source(&upstream.url), false).await;
    let (status, body) = call(&rpc).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body,
        json!({"jsonrpc": "2.0", "id": "client-id", "result": null})
    );
    assert!(
        state
            .ag_genesis_cert
            .cache
            .lock()
            .await
            .as_ref()
            .unwrap()
            .expires_at
            .is_some()
    );
    assert_eq!(call(&rpc).await.1, body);
    assert_eq!(requests.load(Ordering::Relaxed), 1);

    expire(&state.ag_genesis_cert).await;
    let (status, body) = call(&rpc).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["id"], "client-id");
    assert_eq!(body["result"], expected);
    assert_eq!(body["result"]["block"]["slot"].as_u64(), Some(u64::MAX));
    assert_eq!(
        body["result"]["block"]["blockId"].as_array().unwrap().len(),
        32
    );
    assert_eq!(
        body["result"]["signature"]["signature"]
            .as_array()
            .unwrap()
            .len(),
        192
    );
    assert_eq!(requests.load(Ordering::Relaxed), 2);
    assert!(
        state
            .ag_genesis_cert
            .cache
            .lock()
            .await
            .as_ref()
            .unwrap()
            .expires_at
            .is_none()
    );
    drop(upstream);
    assert_eq!(
        call(&rpc).await.1,
        body,
        "immutable certificate survives source outages"
    );
}

#[tokio::test]
async fn get_ag_genesis_cert_parameters_and_batch_contract() {
    let (upstream, requests) = upstream(vec![Reply::result(Value::Null)]).await;
    let (rpc, _) = rpc_server(source(&upstream.url), true).await;
    for params in [
        json!([null]),
        json!([{}]),
        json!([{"commitment": "finalized"}]),
        json!({}),
        json!({"commitment": "finalized"}),
    ] {
        let (status, body) = post_rpc(
            &rpc,
            json!({"jsonrpc": "2.0", "id": 7, "method": "getAgGenesisCert", "params": params}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["id"], 7);
        assert_eq!(body["error"]["code"], -32602);
    }
    assert_eq!(requests.load(Ordering::Relaxed), 0);
    let (status, body) = post_rpc(
        &rpc,
        json!([
            {"jsonrpc": "2.0", "id": 1, "method": "getAgGenesisCert", "params": []},
            {"jsonrpc": "2.0", "id": 2, "method": "getAgGenesisCert", "params": null},
            {"jsonrpc": "2.0", "id": 3, "method": "getAgGenesisCert", "params": {}},
            {"jsonrpc": "2.0", "id": 4, "method": "getEpochSchedule", "params": {}}
        ]),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body[0], json!({"jsonrpc": "2.0", "id": 1, "result": null}));
    assert_eq!(body[1], json!({"jsonrpc": "2.0", "id": 2, "result": null}));
    assert_eq!(body[2]["error"]["code"], -32602);
    assert_eq!(
        body[3]["error"]["code"], -32600,
        "historical parsing is preserved"
    );
    assert_eq!(requests.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn get_ag_genesis_cert_unconfigured_source_is_not_tower_bft() {
    for emit_http_errors in [false, true] {
        let (rpc, _) = rpc_server(AgGenesisCertSource::default(), emit_http_errors).await;
        let (status, body) = call(&rpc).await;
        assert_eq!(
            status,
            if emit_http_errors {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::OK
            }
        );
        assert_source_error(&body, "source_not_configured");
    }
}

#[tokio::test]
async fn get_ag_genesis_cert_upstream_unsupported_and_errors_are_not_null() {
    for (code, reason) in [(-32601, "upstream_unsupported"), (-32005, "upstream_error")] {
        let (upstream, requests) = upstream(vec![Reply::body(json!({"jsonrpc": "2.0", "id": 1, "error": {"code": code, "message": "secret provider diagnostic", "data": "secret"}}))]).await;
        let (rpc, _) = rpc_server(source(&upstream.url), true).await;
        let (status, body) = call(&rpc).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_source_error(&body, reason);
        assert_eq!(body["error"]["data"]["upstreamCode"], code);
        assert!(!body.to_string().contains("secret"));
        assert_eq!(call(&rpc).await.1, body);
        assert_eq!(
            requests.load(Ordering::Relaxed),
            1,
            "failures are throttled"
        );
    }
}

#[tokio::test]
async fn get_ag_genesis_cert_expired_null_fails_closed_then_recovers() {
    let (upstream, requests) = upstream(vec![
        Reply::result(Value::Null),
        Reply::body(json!({"jsonrpc": "2.0", "id": 1})),
        Reply::result(certificate()),
    ])
    .await;
    let (rpc, state) = rpc_server(source(&upstream.url), false).await;
    assert_eq!(call(&rpc).await.1["result"], Value::Null);
    expire(&state.ag_genesis_cert).await;
    assert_source_error(&call(&rpc).await.1, "invalid_upstream_response");
    expire(&state.ag_genesis_cert).await;
    assert_eq!(call(&rpc).await.1["result"], certificate());
    assert_eq!(requests.load(Ordering::Relaxed), 3);
}

#[tokio::test]
async fn get_ag_genesis_cert_http_failure_and_redirect_are_not_null() {
    for status in [
        StatusCode::SERVICE_UNAVAILABLE,
        StatusCode::TEMPORARY_REDIRECT,
    ] {
        let mut reply = Reply::result(Value::Null);
        reply.status = status;
        let (upstream, requests) = upstream(vec![reply]).await;
        let (rpc, _) = rpc_server(source(&upstream.url), false).await;
        assert_source_error(&call(&rpc).await.1, "upstream_unavailable");
        assert_eq!(requests.load(Ordering::Relaxed), 1);
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let (rpc, _) = rpc_server(source(&url), false).await;
    assert_source_error(&call(&rpc).await.1, "upstream_unavailable");
}

#[tokio::test]
async fn get_ag_genesis_cert_fetch_and_admission_deadlines_are_bounded() {
    let mut reply = Reply::result(Value::Null);
    reply.delay = Duration::from_secs(10);
    let (upstream, requests) = upstream(vec![reply]).await;
    let mut source = source(&upstream.url);
    source.timeout = Duration::from_millis(100);
    let (rpc, state) = rpc_server(source, false).await;
    let started = Instant::now();
    assert_source_error(&call(&rpc).await.1, "source_timeout");
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_source_error(&call(&rpc).await.1, "source_timeout");
    assert_eq!(
        requests.load(Ordering::Relaxed),
        1,
        "timed-out fetches are throttled"
    );
    let guard = state.ag_genesis_cert.cache.lock().await;
    let started = Instant::now();
    assert_eq!(
        state.ag_genesis_cert.get().await,
        Err(CertificateSourceError::Timeout)
    );
    assert!(started.elapsed() < Duration::from_secs(2));
    drop(guard);
}

#[tokio::test]
async fn get_ag_genesis_cert_response_size_is_bounded_for_fixed_and_streamed_bodies() {
    for chunked in [false, true] {
        let mut reply = Reply::result(Value::Null);
        reply.body = " ".repeat(MAX_RESPONSE_BYTES + 1);
        reply.chunked = chunked;
        let (upstream, _) = upstream(vec![reply]).await;
        let (rpc, _) = rpc_server(source(&upstream.url), false).await;
        assert_source_error(&call(&rpc).await.1, "invalid_upstream_response");
    }
}

#[tokio::test]
async fn get_ag_genesis_cert_slow_response_body_is_bounded() {
    let mut reply = Reply::result(Value::Null);
    reply.chunked = true;
    reply.body_delay = Duration::from_secs(10);
    let (upstream, _) = upstream(vec![reply]).await;
    let mut source = source(&upstream.url);
    source.timeout = Duration::from_millis(100);
    let (rpc, _) = rpc_server(source, false).await;
    let started = Instant::now();
    assert_source_error(&call(&rpc).await.1, "source_timeout");
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[tokio::test]
async fn get_ag_genesis_cert_concurrent_fetches_coalesce() {
    let mut reply = Reply::result(certificate());
    reply.delay = Duration::from_millis(20);
    let (upstream, requests) = upstream(vec![reply]).await;
    let source = Arc::new(source(&upstream.url));
    let mut callers = Vec::new();
    for _ in 0..16 {
        let source = source.clone();
        callers.push(tokio::spawn(async move { source.get().await }));
    }
    for caller in callers {
        let value = caller.await.unwrap().unwrap().unwrap();
        assert_eq!(serde_json::to_value(value).unwrap(), certificate());
    }
    assert_eq!(requests.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn get_ag_genesis_cert_cancelled_fetch_does_not_strand_refresh() {
    let mut reply = Reply::result(Value::Null);
    reply.delay = Duration::from_secs(10);
    let (upstream, requests) = upstream(vec![reply, Reply::result(certificate())]).await;
    let source = Arc::new(source(&upstream.url));
    let first_source = source.clone();
    let first = tokio::spawn(async move { first_source.get().await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while requests.load(Ordering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    assert_eq!(
        serde_json::to_value(source.get().await.unwrap().unwrap()).unwrap(),
        certificate()
    );
    assert_eq!(requests.load(Ordering::Relaxed), 2);
}

#[test]
fn get_ag_genesis_cert_rejects_malformed_envelopes_and_wire_shapes() {
    let mut invalid_results = vec![json!({}), json!([]), json!(false)];
    for (field, replacement) in [
        ("slot", json!(-1)),
        ("slot", json!(1.5)),
        ("slot", json!("1")),
        ("blockId", json!(vec![0; 31])),
        ("blockId", json!("base58-block-id")),
    ] {
        let mut value = certificate();
        value["block"][field] = replacement;
        invalid_results.push(value);
    }
    for (field, replacement) in [
        ("signature", json!(vec![0; 191])),
        ("signature", json!("base64-signature")),
        ("bitmap", json!([256])),
        ("bitmap", json!([-1])),
    ] {
        let mut value = certificate();
        value["signature"][field] = replacement;
        invalid_results.push(value);
    }
    for result in invalid_results {
        assert_eq!(
            decode_response(&Reply::result(result).body.into_bytes()),
            Err(CertificateSourceError::InvalidResponse)
        );
    }
    for envelope in [
        json!({"jsonrpc": "2.0", "id": 1}),
        json!({"jsonrpc": "1.0", "id": 1, "result": null}),
        json!({"jsonrpc": "2.0", "id": 2, "result": null}),
        json!({"jsonrpc": "2.0", "id": 1, "result": null, "error": null}),
        json!({"jsonrpc": "2.0", "id": 1, "error": null}),
        json!({"jsonrpc": "2.0", "id": 1, "error": {"code": -32601}}),
    ] {
        assert_eq!(
            decode_response(&serde_json::to_vec(&envelope).unwrap()),
            Err(CertificateSourceError::InvalidResponse)
        );
    }
    assert_eq!(
        decode_response(b"not JSON"),
        Err(CertificateSourceError::InvalidResponse)
    );
}

#[test]
fn get_ag_genesis_cert_configuration_validates_source_and_bounds() {
    let mut config = RpcConfig::try_parse_from(["superbank-rpc"]).unwrap();
    config.ag_genesis_cert_rpc_url = None;
    assert!(
        AgGenesisCertSource::from_config(&config)
            .unwrap()
            .upstream
            .is_none()
    );
    for blank in ["", "  "] {
        config.ag_genesis_cert_rpc_url = Some(blank.to_string());
        assert!(
            AgGenesisCertSource::from_config(&config)
                .unwrap()
                .upstream
                .is_none()
        );
    }
    for url in [
        "not a URL",
        "ftp://example.com",
        "https://example.com/#fragment",
    ] {
        config.ag_genesis_cert_rpc_url = Some(url.to_string());
        assert!(AgGenesisCertSource::from_config(&config).is_err());
    }
    config.ag_genesis_cert_rpc_url = Some("https://rpc.example.com".to_string());
    config.ag_genesis_cert_rpc_timeout_ms = config.rpc_request_timeout_ms;
    assert!(AgGenesisCertSource::from_config(&config).is_err());
    for args in [
        ["--ag-genesis-cert-rpc-timeout-ms", "0"],
        ["--ag-genesis-cert-refresh-interval-secs", "0"],
        ["--ag-genesis-cert-refresh-interval-secs", "301"],
    ] {
        assert!(RpcConfig::try_parse_from(["superbank-rpc", args[0], args[1]]).is_err());
    }
    let config = RpcConfig::try_parse_from([
        "superbank-rpc",
        "--ag-genesis-cert-rpc-url",
        "https://rpc.example.com",
        "--ag-genesis-cert-rpc-timeout-ms",
        "1500",
        "--ag-genesis-cert-refresh-interval-secs",
        "3",
    ])
    .unwrap();
    let source = AgGenesisCertSource::from_config(&config).unwrap();
    assert_eq!(source.timeout, Duration::from_millis(1500));
    assert_eq!(source.refresh_interval, Duration::from_secs(3));
}
