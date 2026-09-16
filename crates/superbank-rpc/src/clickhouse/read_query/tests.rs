// SPDX-License-Identifier: AGPL-3.0-only
//! HTTP protocol lifecycle tests; the mock controls when source work disappears.

use super::*;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};

use axum::body::{Body, Bytes};
use axum::extract::{ConnectInfo, Query, State};
use axum::response::{IntoResponse, Response};
use futures_util::StreamExt;

const STREAMING: u8 = 1;
const TRUNCATED: u8 = 2;

#[derive(Clone)]
struct Observed {
    sql: String,
    params: HashMap<String, String>,
    peer: SocketAddr,
}

#[derive(Default)]
struct MockState {
    requests: Mutex<Vec<Observed>>,
    mode: AtomicU8,
    active: AtomicBool,
    active_id: Mutex<String>,
    quiet_probes: AtomicUsize,
}

fn append_string(bytes: &mut Vec<u8>, value: &str) {
    let mut length = value.len();
    while length >= 128 {
        bytes.push((length as u8 & 127) | 128);
        length >>= 7;
    }
    bytes.push(length as u8);
    bytes.extend_from_slice(value.as_bytes());
}

fn observation_rows(state: &MockState) -> Vec<u8> {
    let mut bytes = Vec::new();
    append_string(&mut bytes, "node1");
    append_string(&mut bytes, "");
    if state.active.load(Ordering::SeqCst) {
        append_string(&mut bytes, "node1");
        append_string(&mut bytes, &state.active_id.lock().unwrap());
    } else {
        state.quiet_probes.fetch_add(1, Ordering::SeqCst);
    }
    bytes
}

async fn handle(
    State(state): State<Arc<MockState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Query(params): Query<HashMap<String, String>>,
    body: String,
) -> Response {
    let sql = if body.is_empty() {
        params.get("query").cloned().unwrap_or_default()
    } else {
        body
    };
    state.requests.lock().unwrap().push(Observed {
        sql: sql.clone(),
        params: params.clone(),
        peer,
    });
    if sql.contains("AS coordinator") {
        let mut bytes = Vec::new();
        append_string(&mut bytes, "node1");
        bytes.extend_from_slice(&1_u64.to_le_bytes());
        bytes.push(1);
        return bytes.into_response();
    }
    if sql.contains("system.processes") {
        return observation_rows(&state).into_response();
    }
    assert!(!sql.contains("KILL"), "read cleanup must never issue KILL");
    let mut bytes = 42_u64.to_le_bytes().to_vec();
    match state.mode.load(Ordering::SeqCst) {
        STREAMING => {
            *state.active_id.lock().unwrap() = params["query_id"].clone();
            state.active.store(true, Ordering::SeqCst);
            let stream = futures_util::stream::once(async move {
                Ok::<_, std::convert::Infallible>(Bytes::from(bytes))
            })
            .chain(futures_util::stream::pending());
            Body::from_stream(stream).into_response()
        }
        TRUNCATED => {
            bytes.push(1); // A complete first UInt64 followed by an incomplete second row.
            bytes.into_response()
        }
        _ => bytes.into_response(),
    }
}

struct Fixture {
    client: Client,
    endpoint: ReadEndpoint,
    state: Arc<MockState>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Fixture {
    async fn new(timeout: Duration) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(MockState::default());
        let app = axum::Router::new()
            .fallback(handle)
            .with_state(state.clone());
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });
        let client = Client::default()
            .with_url(url)
            .with_validation(false)
            .with_compression(clickhouse::Compression::None);
        let endpoint = ReadEndpoint::new(client.clone(), None, 1, timeout, "primary");
        Self {
            client,
            endpoint,
            state,
            server,
        }
    }

    async fn initialized() -> Self {
        let fixture = Self::new(Duration::from_secs(2)).await;
        fixture.endpoint.initialize().await.unwrap();
        assert_eq!(fixture.requests().len(), 2);
        fixture
    }

    fn requests(&self) -> Vec<Observed> {
        self.state.requests.lock().unwrap().clone()
    }

    fn probe_count(&self) -> usize {
        self.requests()
            .iter()
            .filter(|request| request.sql.contains("system.processes"))
            .count()
    }

    async fn wait_until(&self, condition: impl Fn() -> bool) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while !condition() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn requests_require_explicit_initialization_without_lazy_network_io() {
    let fixture = Fixture::new(Duration::from_secs(1)).await;
    assert!(
        fixture
            .endpoint
            .query(&fixture.client, "SELECT 42", "test")
            .await
            .is_err()
    );
    assert!(fixture.requests().is_empty());
    assert_eq!(fixture.endpoint.admission.available_permits(), 1);
}

#[tokio::test]
async fn successful_eof_releases_capacity_reuses_connection_and_adds_no_probes() {
    let fixture = Fixture::initialized().await;
    for _ in 0..2 {
        let row = fixture
            .endpoint
            .query(&fixture.client, "SELECT 42", "test")
            .await
            .unwrap()
            .with_setting("readonly", "0")
            .with_setting("cancel_http_readonly_queries_on_client_close", "0")
            .fetch_optional::<u64>()
            .await
            .unwrap();
        assert_eq!(row, Some(42));
        assert_eq!(fixture.endpoint.admission.available_permits(), 1);
    }
    let requests = fixture.requests();
    assert_eq!(requests.len(), 4);
    assert_eq!(fixture.probe_count(), 1); // Initialization capability probe only.
    assert_eq!(
        requests[2].peer, requests[3].peer,
        "successful EOF should permit HTTP reuse"
    );
    for request in &requests[2..] {
        assert_eq!(request.params["readonly"], "2");
        assert_eq!(
            request.params["cancel_http_readonly_queries_on_client_close"],
            "1"
        );
        assert!(!request.params["query_id"].is_empty());
    }
    assert_ne!(
        requests[2].params["query_id"],
        requests[3].params["query_id"]
    );
}

#[tokio::test]
async fn dropping_unsubmitted_query_releases_capacity_without_verification() {
    let fixture = Fixture::initialized().await;
    let query = fixture
        .endpoint
        .query(&fixture.client, "SELECT 42", "test")
        .await
        .unwrap();
    assert_eq!(fixture.endpoint.admission.available_permits(), 0);
    drop(query);
    assert_eq!(fixture.endpoint.admission.available_permits(), 1);
    assert_eq!(fixture.requests().len(), 2);
}

#[tokio::test]
async fn abandoned_stream_retains_capacity_until_source_disappears_without_kill() {
    let fixture = Fixture::initialized().await;
    fixture.state.mode.store(STREAMING, Ordering::SeqCst);
    let mut cursor = fixture
        .endpoint
        .query_with_id(
            &fixture.client,
            "SELECT 42",
            "test",
            Some("preserved-status-id".into()),
        )
        .await
        .unwrap()
        .fetch::<u64>()
        .unwrap();
    assert_eq!(cursor.next().await.unwrap(), Some(42));
    drop(cursor);
    fixture.wait_until(|| fixture.probe_count() >= 2).await;
    assert_eq!(fixture.endpoint.admission.available_permits(), 0);
    assert!(fixture.requests().iter().any(|request| {
        request.sql.contains("system.processes") && request.sql.contains("preserved-status-id")
    }));
    let quiet_before_absence = fixture.state.quiet_probes.load(Ordering::SeqCst);
    fixture.state.active.store(false, Ordering::SeqCst);
    fixture
        .wait_until(|| fixture.endpoint.admission.available_permits() == 1)
        .await;
    assert!(fixture.state.quiet_probes.load(Ordering::SeqCst) >= quiet_before_absence + 2);
    assert!(
        fixture
            .requests()
            .iter()
            .all(|request| !request.sql.contains("KILL"))
    );
}

#[tokio::test]
async fn trailing_decode_error_is_not_accepted_as_success() {
    let fixture = Fixture::initialized().await;
    fixture.state.mode.store(TRUNCATED, Ordering::SeqCst);
    let result = fixture
        .endpoint
        .query(&fixture.client, "SELECT 42", "test")
        .await
        .unwrap()
        .fetch_optional::<u64>()
        .await;
    assert!(
        result.is_err(),
        "the first valid row must not hide a truncated trailing row"
    );
    assert_eq!(fixture.endpoint.admission.available_permits(), 0);
    fixture
        .wait_until(|| fixture.endpoint.admission.available_permits() == 1)
        .await;
    assert!(fixture.probe_count() >= 3);
}

#[tokio::test]
async fn failed_cursor_closes_response_and_recovers_capacity_while_caller_retains_it() {
    let fixture = Fixture::initialized().await;
    fixture.state.mode.store(TRUNCATED, Ordering::SeqCst);
    let mut cursor = fixture
        .endpoint
        .query(&fixture.client, "SELECT 42", "test")
        .await
        .unwrap()
        .fetch::<u64>()
        .unwrap();
    assert_eq!(cursor.next().await.unwrap(), Some(42));
    assert!(cursor.next().await.is_err());
    assert!(
        cursor.cursor.is_none(),
        "failed response must close immediately"
    );
    assert!(
        cursor.guard.is_none(),
        "verification must start before caller drops cursor"
    );
    fixture
        .wait_until(|| fixture.endpoint.admission.available_permits() == 1)
        .await;
    assert!(fixture.probe_count() >= 3);
    // The failed wrapper deliberately stays alive throughout verification.
    assert!(cursor.next().await.is_err());
}

#[tokio::test]
async fn admission_timeout_submits_no_second_query_and_releases_on_owner_drop() {
    let fixture = Fixture::initialized().await;
    let endpoint = fixture.endpoint.with_timeout(Duration::from_millis(20));
    let held = endpoint
        .query(&fixture.client, "SELECT 42", "held")
        .await
        .unwrap();
    let result = endpoint
        .query(&fixture.client, "SELECT 43", "waiting")
        .await;
    assert!(result.is_err());
    assert_eq!(fixture.requests().len(), 2);
    assert_eq!(endpoint.admission.available_permits(), 0);
    drop(held);
    assert_eq!(endpoint.admission.available_permits(), 1);
    assert_eq!(fixture.requests().len(), 2);
}

#[tokio::test]
async fn background_capacity_is_independent_and_extended_deadline_is_preserved() {
    let fixture = Fixture::initialized().await;
    let request_endpoint = fixture.endpoint.with_timeout(Duration::from_millis(20));
    let background = request_endpoint
        .background(1)
        .with_timeout(Duration::from_secs(1));
    let held = request_endpoint
        .query(&fixture.client, "SELECT 42", "foreground")
        .await
        .unwrap();
    let query = background
        .query(&fixture.client, "SELECT 42", "background")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert_eq!(query.fetch_one::<u64>().await.unwrap(), 42);
    assert_eq!(request_endpoint.admission.available_permits(), 0);
    assert_eq!(background.admission.available_permits(), 1);
    drop(held);
}
