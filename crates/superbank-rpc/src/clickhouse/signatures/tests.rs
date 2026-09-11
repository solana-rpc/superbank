// SPDX-License-Identifier: AGPL-3.0-only
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::{Router, body::Body, extract::Query, routing::post};
use futures_util::StreamExt;
use tokio::sync::mpsc;

use super::*;
use crate::clickhouse::{ClickHouseClientOptions, RoutingPolicy, RoutingScope, RoutingTransport};

struct MockClickHouse {
    client: ClickHouseClient,
    queries: mpsc::UnboundedReceiver<(String, String)>,
    server: tokio::task::JoinHandle<()>,
    body_started: Arc<tokio::sync::Notify>,
}

impl Drop for MockClickHouse {
    fn drop(&mut self) {
        self.server.abort();
    }
}

fn discovery_response(sql: &str) -> Option<Body> {
    if !sql.contains("AS coordinator") {
        return None;
    }
    let mut rows = vec![5];
    rows.extend_from_slice(b"node1");
    rows.extend_from_slice(&0_u64.to_le_bytes());
    rows.push(0);
    rows.push(5);
    rows.extend_from_slice(b"node1");
    rows.extend_from_slice(&1_u64.to_le_bytes());
    rows.push(1);
    Some(Body::from(rows))
}

fn verification_response(sql: &str) -> Option<Body> {
    discovery_response(sql).or_else(|| {
        sql.contains("system.processes")
            .then(|| Body::from(vec![5, b'n', b'o', b'd', b'e', b'1', 0]))
    })
}

fn record_query(
    send: &mpsc::UnboundedSender<(String, String)>,
    sql: &str,
    params: &HashMap<String, String>,
) {
    if !["AS coordinator", "status_disconnect_preflight"]
        .iter()
        .any(|marker| sql.contains(marker))
    {
        send.send((
            sql.into(),
            params.get("query_id").cloned().unwrap_or_default(),
        ))
        .unwrap();
    }
}

impl MockClickHouse {
    async fn new(response: Vec<u8>, pending_headers: bool, pending_body: bool) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (send, queries) = mpsc::unbounded_channel();
        let body_started = Arc::new(tokio::sync::Notify::new());
        let body_notify = body_started.clone();
        let app = Router::new().route(
            "/",
            post(
                move |Query(params): Query<HashMap<String, String>>, body: String| {
                    let send = send.clone();
                    let response = response.clone();
                    let body_notify = body_notify.clone();
                    async move {
                        let sql = params.get("query").cloned().unwrap_or(body);
                        assert_eq!(params.get("readonly").map(String::as_str), Some("2"));
                        assert_eq!(
                            params
                                .get("cancel_http_readonly_queries_on_client_close")
                                .map(String::as_str),
                            Some("1")
                        );
                        assert!(!sql.contains("KILL QUERY"));
                        record_query(&send, &sql, &params);
                        if let Some(body) = verification_response(&sql) {
                            return body;
                        }
                        if pending_headers {
                            std::future::pending::<()>().await;
                        }
                        let chunks = futures_util::stream::once(async move {
                            body_notify.notify_one();
                            Ok::<_, std::io::Error>(response)
                        });
                        if pending_body {
                            Body::from_stream(chunks.chain(futures_util::stream::pending()))
                        } else {
                            Body::from_stream(chunks)
                        }
                    }
                },
            ),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let mut client = ClickHouseClient::new(
            &format!("http://{address}"),
            "default",
            "default",
            "",
            ClickHouseClientOptions::new(
                RoutingPolicy {
                    transport: RoutingTransport::Http,
                    scope: RoutingScope::Distributed,
                },
                None,
                Vec::new(),
                "default.gsfa_hot".into(),
                "default.gsfa_hot_local".into(),
            )
            .with_query_cleanup_cluster("rbx2".into())
            .with_signature_status_limits(1, 2),
        );
        client.set_http_client_for_tests(
            client
                .client
                .clone()
                .with_validation(false)
                .with_compression(clickhouse::Compression::None),
        );
        client.allow_query_settings = false;
        Self {
            client,
            queries,
            server,
            body_started,
        }
    }

    async fn next_query(&mut self) -> (String, String) {
        tokio::time::timeout(Duration::from_secs(2), self.queries.recv())
            .await
            .unwrap()
            .unwrap()
    }
}

fn signatures(count: usize) -> Vec<String> {
    (0..count)
        .map(|i| {
            let mut bytes = [0u8; 64];
            bytes[..8].copy_from_slice(&(i as u64).to_le_bytes());
            bs58::encode(bytes).into_string()
        })
        .collect()
}

fn status_row() -> Vec<u8> {
    let mut bytes = vec![0; 64];
    bytes.extend_from_slice(&123u64.to_le_bytes());
    bytes.push(1); // Nullable(String): null
    bytes
}

#[tokio::test]
async fn primary_maximum_miss_batch_enforces_settings_with_optional_tuning_disabled() {
    let mut mock = MockClickHouse::new(Vec::new(), false, false).await;
    let (rows, _) = mock
        .client
        .get_signature_statuses(&signatures(256))
        .await
        .unwrap();
    assert!(rows.is_empty());
    let (sql, id) = mock.next_query().await;
    assert!(!id.is_empty());
    assert!(sql.contains("max_threads_for_indexes=2"));
    assert!(sql.contains("max_threads=2"));
    assert!(sql.contains("use_hedged_requests=0"));
    assert!(sql.contains("max_parallel_replicas=1"));
    assert!(sql.contains("max_execution_time_leaf="));
    assert_eq!(sql.matches("unhex(").count(), 256);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), mock.queries.recv())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn primary_consumes_successful_rows_and_disarms_cleanup() {
    let mut mock = MockClickHouse::new(status_row(), false, false).await;
    let mut input = signatures(2);
    input.push(input[0].clone());
    let (rows, timings) = mock.client.get_signature_statuses(&input).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].signature, input[0]);
    assert_eq!(rows[0].slot, 123);
    assert!(rows[0].err.is_none());
    assert_eq!(timings.rows_returned, 1);
    mock.next_query().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(30), mock.queries.recv())
            .await
            .is_err()
    );
}

async fn assert_two_absent_probes(mock: &mut MockClickHouse, id: &str) {
    for _ in 0..2 {
        let (probe, _) = mock.next_query().await;
        assert!(probe.contains("system.processes"));
        assert!(probe.contains(&format!("query_id IN ('{id}')")));
        assert!(probe.contains(&format!("initial_query_id IN ('{id}')")));
        assert!(!probe.contains("KILL"));
    }
}

async fn assert_cancelled_query_cleanup(pending_headers: bool, pending_body: bool) {
    let mut mock = MockClickHouse::new(status_row(), pending_headers, pending_body).await;
    let client = mock.client.clone();
    let request =
        tokio::spawn(async move { client.get_signature_statuses(&signatures(256)).await });
    let (_, id) = mock.next_query().await;
    if pending_body {
        tokio::time::timeout(Duration::from_secs(2), mock.body_started.notified())
            .await
            .unwrap();
    }
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    assert_two_absent_probes(&mut mock, &id).await;
    let _permits = tokio::time::timeout(
        Duration::from_secs(2),
        mock.client.acquire_signature_status_permits(),
    )
    .await
    .unwrap()
    .unwrap();
}

#[tokio::test]
async fn primary_cancel_during_headers_cleans_up() {
    assert_cancelled_query_cleanup(true, false).await;
}

#[tokio::test]
async fn primary_cancel_during_body_cleans_up() {
    assert_cancelled_query_cleanup(false, true).await;
}

#[tokio::test]
async fn primary_queued_cancel_and_empty_input_submit_nothing() {
    let mut mock = MockClickHouse::new(Vec::new(), false, false).await;
    assert!(
        mock.client
            .get_signature_statuses(&[])
            .await
            .unwrap()
            .0
            .is_empty()
    );
    let (source, http) = mock
        .client
        .acquire_signature_status_permits()
        .await
        .unwrap();
    drop(http);
    let client = mock.client.clone();
    let request = tokio::spawn(async move { client.get_signature_statuses(&signatures(1)).await });
    tokio::task::yield_now().await;
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    drop(source);
    assert!(mock.queries.try_recv().is_err());
}

#[test]
fn remaining_deadline_is_not_rounded_up_or_restarted() {
    let sql = primary_signature_status_settings("", 2, Duration::from_millis(125)).unwrap();
    assert!(sql.contains("max_execution_time=0.125,"));
    assert!(sql.contains("max_execution_time_leaf=0.125,"));
}

#[tokio::test]
async fn primary_timeout_while_queued_submits_nothing() {
    let mut mock = MockClickHouse::new(Vec::new(), false, false).await;
    mock.client.query_timeout = Duration::from_millis(30);
    let (_source, http) = mock
        .client
        .acquire_signature_status_permits()
        .await
        .unwrap();
    drop(http);
    assert!(
        mock.client
            .get_signature_statuses(&signatures(1))
            .await
            .is_err()
    );
    assert!(mock.queries.try_recv().is_err());
}

#[tokio::test]
async fn primary_timeout_during_headers_dispatches_cleanup() {
    let mut mock = MockClickHouse::new(Vec::new(), true, false).await;
    mock.client.query_timeout = Duration::from_millis(50);
    assert!(
        mock.client
            .get_signature_statuses(&signatures(1))
            .await
            .is_err()
    );
    let (_, id) = mock.next_query().await;
    let (probe, _) = mock.next_query().await;
    assert!(probe.contains(&format!("initial_query_id IN ('{id}')")));
    assert!(!probe.contains("KILL"));
}

#[tokio::test]
async fn primary_admission_wait_is_subtracted_from_server_budget() {
    let mut mock = MockClickHouse::new(Vec::new(), false, false).await;
    mock.client.query_timeout = Duration::from_secs(1);
    let (source, http) = mock
        .client
        .acquire_signature_status_permits()
        .await
        .unwrap();
    drop(http);
    let client = mock.client.clone();
    let (started, waiting) = tokio::sync::oneshot::channel();
    let request = tokio::spawn(async move {
        started.send(()).unwrap();
        client.get_signature_statuses(&signatures(1)).await
    });
    waiting.await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(source);
    request.await.unwrap().unwrap();
    let (sql, _) = mock.next_query().await;
    let remaining: f64 = sql
        .split_once("max_execution_time=")
        .unwrap()
        .1
        .split(',')
        .next()
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        remaining > 0.0 && remaining < 0.99,
        "remaining budget: {remaining}"
    );
}

#[test]
fn exhausted_or_submillisecond_budget_never_becomes_an_unlimited_query() {
    for remaining in [
        Duration::ZERO,
        Duration::from_nanos(1),
        Duration::from_micros(999),
    ] {
        assert!(primary_signature_status_settings("", 2, remaining).is_err());
    }
    let sql = primary_signature_status_settings("", 2, Duration::from_millis(1)).unwrap();
    assert!(sql.contains("max_execution_time=0.001,"));
    assert!(sql.contains("timeout_overflow_mode_leaf='throw'"));
    assert!(sql.contains("timeout_before_checking_execution_speed=0"));
}
