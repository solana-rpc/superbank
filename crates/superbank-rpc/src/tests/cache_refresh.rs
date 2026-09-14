// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use super::*;
use crate::processing::ProcessingError;
use axum::{Router, routing::post};
use std::sync::atomic::AtomicUsize;
use tokio::sync::{mpsc, oneshot};

const SLOT: u64 = 123;
const HEIGHT: u64 = 100;
const TEST_TIMEOUT: Duration = Duration::from_secs(3);

type BackendReply = (StatusCode, Vec<u8>);

struct PendingQuery {
    sql: String,
    reply: oneshot::Sender<BackendReply>,
}

struct Backend {
    url: String,
    requests: mpsc::UnboundedReceiver<PendingQuery>,
    calls: Arc<AtomicUsize>,
    server: tokio::task::JoinHandle<()>,
}

impl Backend {
    async fn start() -> Self {
        let (send, requests) = mpsc::unbounded_channel();
        let calls = Arc::new(AtomicUsize::new(0));
        let handler_calls = calls.clone();
        let app = Router::new().route(
            "/",
            post(move |body: Bytes| {
                let send = send.clone();
                let calls = handler_calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let (reply, response) = oneshot::channel();
                    send.send(PendingQuery {
                        sql: String::from_utf8(body.to_vec()).expect("SQL body"),
                        reply,
                    })
                    .expect("test receiver");
                    response
                        .await
                        .unwrap_or((StatusCode::SERVICE_UNAVAILABLE, Vec::new()))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self {
            url,
            requests,
            calls,
            server,
        }
    }

    async fn next(&mut self, kind: CacheKind) -> PendingQuery {
        let query = tokio::time::timeout(TEST_TIMEOUT, self.requests.recv())
            .await
            .expect("backend query must start")
            .expect("backend request");
        assert!(
            query.sql.contains(match kind {
                CacheKind::Slot => "maxOrNull(slot)",
                CacheKind::Height => "block_height",
            }),
            "unexpected query: {}",
            query.sql
        );
        query
    }

    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn state(&self) -> Arc<AppState> {
        let mut state = test_state_with_clickhouse_url(&self.url);
        let state_mut = Arc::get_mut(&mut state).unwrap();
        state_mut.latest_slot_cache = LatestSlotCache::new(Duration::from_secs(60));
        state_mut.latest_block_height_cache = LatestBlockHeightCache::new(Duration::from_secs(60));
        // The fixture returns plain RowBinary; exercise real queries and decoding without
        // duplicating ClickHouse's compression and schema-header protocols in the mock.
        state_mut.clickhouse.client = state_mut
            .clickhouse
            .client
            .clone()
            .with_compression(clickhouse::Compression::None)
            .with_validation(false);
        state
    }
}

impl Drop for Backend {
    fn drop(&mut self) {
        self.server.abort();
    }
}

#[derive(Clone, Copy, Debug)]
enum CacheKind {
    Slot,
    Height,
}

impl CacheKind {
    async fn refresh(self, state: Arc<AppState>) -> Result<Option<u64>, ProcessingError> {
        match self {
            Self::Slot => state
                .latest_slot_cache
                .get_or_refresh(&state.clickhouse)
                .await
                .map(Some),
            Self::Height => {
                state
                    .latest_block_height_cache
                    .get_or_refresh(SLOT, &state.clickhouse)
                    .await
            }
        }
    }

    fn value(self) -> u64 {
        match self {
            Self::Slot => SLOT,
            Self::Height => HEIGHT,
        }
    }

    fn method(self) -> &'static str {
        match self {
            Self::Slot => "getSlot",
            Self::Height => "getBlockHeight",
        }
    }

    fn respond(self, query: PendingQuery, value: Option<u64>) {
        // FixedString(32) blockhash followed by Nullable(UInt64), or just Nullable(UInt64).
        let mut bytes = if matches!(self, Self::Height) {
            vec![7; 32]
        } else {
            Vec::new()
        };
        bytes.push(u8::from(value.is_none()));
        if let Some(value) = value {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        query
            .reply
            .send((StatusCode::OK, bytes))
            .expect("live backend request");
    }
}

async fn assert_recovered(
    waiters: Vec<tokio::task::JoinHandle<Result<Option<u64>, ProcessingError>>>,
    kind: CacheKind,
) {
    let results = tokio::time::timeout(TEST_TIMEOUT, futures_util::future::join_all(waiters))
        .await
        .expect("all cache callers must complete");
    for result in results {
        assert_eq!(
            result.expect("caller task").expect("refresh success"),
            Some(kind.value())
        );
    }
}

#[tokio::test]
async fn cache_refresh_cancelled_leader_releases_existing_waiters() {
    for kind in [CacheKind::Slot, CacheKind::Height] {
        let mut backend = Backend::start().await;
        let state = backend.state();
        let leader = tokio::spawn(kind.refresh(state.clone()));
        let pending = backend.next(kind).await;
        let mut waiters = Vec::new();
        for _ in 0..3 {
            let mut waiter = Box::pin(kind.refresh(state.clone()));
            assert!(futures_util::poll!(waiter.as_mut()).is_pending());
            waiters.push(waiter);
        }
        assert_eq!(backend.call_count(), 1);
        leader.abort();
        assert!(leader.await.unwrap_err().is_cancelled());
        drop(pending);
        let mut tasks: Vec<_> = waiters.into_iter().map(tokio::spawn).collect();
        tasks.push(tokio::spawn(kind.refresh(state.clone())));
        kind.respond(backend.next(kind).await, Some(kind.value()));
        assert_recovered(tasks, kind).await;
        assert_eq!(kind.refresh(state).await.unwrap(), Some(kind.value()));
        assert_eq!(
            backend.call_count(),
            2,
            "one replacement leader for {kind:?}"
        );
    }
}

#[tokio::test]
async fn cache_refresh_singleflight_and_error_recovery() {
    for kind in [CacheKind::Slot, CacheKind::Height] {
        for fail in [false, true] {
            let mut backend = Backend::start().await;
            let state = backend.state();
            let leader = tokio::spawn(kind.refresh(state.clone()));
            let pending = backend.next(kind).await;
            let mut waiters = Vec::new();
            for _ in 0..3 {
                let mut waiter = Box::pin(kind.refresh(state.clone()));
                assert!(futures_util::poll!(waiter.as_mut()).is_pending());
                waiters.push(waiter);
            }
            assert_eq!(backend.call_count(), 1);
            if fail {
                pending
                    .reply
                    .send((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        b"backend unavailable".to_vec(),
                    ))
                    .unwrap();
                assert!(
                    tokio::time::timeout(TEST_TIMEOUT, leader)
                        .await
                        .unwrap()
                        .unwrap()
                        .is_err()
                );
            } else {
                kind.respond(pending, Some(kind.value()));
                assert_recovered(vec![leader], kind).await;
            }
            let tasks = waiters.into_iter().map(tokio::spawn).collect();
            if fail {
                kind.respond(backend.next(kind).await, Some(kind.value()));
            }
            assert_recovered(tasks, kind).await;
            assert_eq!(kind.refresh(state).await.unwrap(), Some(kind.value()));
            assert_eq!(backend.call_count(), if fail { 2 } else { 1 });
        }
    }
}

#[tokio::test]
async fn cache_refresh_empty_slot_retries_and_missing_height_is_cached_by_slot() {
    let mut backend = Backend::start().await;
    let state = backend.state();
    let leader = tokio::spawn(CacheKind::Slot.refresh(state.clone()));
    CacheKind::Slot.respond(backend.next(CacheKind::Slot).await, None);
    let error = tokio::time::timeout(TEST_TIMEOUT, leader)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("no finalized slot in ClickHouse")
    );
    let retry = tokio::spawn(CacheKind::Slot.refresh(state.clone()));
    CacheKind::Slot.respond(backend.next(CacheKind::Slot).await, Some(SLOT));
    assert_recovered(vec![retry], CacheKind::Slot).await;

    let leader = tokio::spawn(CacheKind::Height.refresh(state.clone()));
    CacheKind::Height.respond(backend.next(CacheKind::Height).await, None);
    assert_eq!(
        tokio::time::timeout(TEST_TIMEOUT, leader)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        None
    );
    assert_eq!(
        CacheKind::Height.refresh(state.clone()).await.unwrap(),
        None
    );
    assert_eq!(backend.call_count(), 3);
    let other_slot = tokio::spawn(async move {
        state
            .latest_block_height_cache
            .get_or_refresh(SLOT + 1, &state.clickhouse)
            .await
    });
    let query = backend.next(CacheKind::Height).await;
    assert!(query.sql.contains("slot = 124"));
    CacheKind::Height.respond(query, Some(HEIGHT));
    assert_recovered(vec![other_slot], CacheKind::Height).await;
    assert_eq!(backend.call_count(), 4);
}

#[tokio::test]
async fn cache_refresh_recovers_after_batch_envelope_deadline() {
    for kind in [CacheKind::Slot, CacheKind::Height] {
        let mut backend = Backend::start().await;
        let mut state = backend.state();
        let state_mut = Arc::get_mut(&mut state).unwrap();
        // Shorter than the client's 8s query budget: cancellation must come from the batch.
        state_mut.rpc_request_timeout = Duration::from_millis(250);
        if matches!(kind, CacheKind::Height) {
            state_mut
                .latest_slot_cache
                .value
                .store(SLOT, Ordering::Relaxed);
            state_mut
                .latest_slot_cache
                .last_updated_ms
                .store(current_time_millis(), Ordering::Relaxed);
        }
        let batch_state = state.clone();
        let batch = tokio::spawn(async move {
            handle_json_rpc_value(
                batch_state,
                &json!([
                    {"jsonrpc": "2.0", "id": 1, "method": kind.method()},
                    {"jsonrpc": "2.0", "id": 2, "method": kind.method()}
                ]),
            )
            .await
        });
        let pending = backend.next(kind).await;
        let mut external_waiter = Box::pin(kind.refresh(state.clone()));
        assert!(futures_util::poll!(external_waiter.as_mut()).is_pending());
        let response = tokio::time::timeout(TEST_TIMEOUT, batch)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let response = parse_json_value_response(response).await;
        assert_eq!(response["id"], Value::Null);
        assert_eq!(response["error"]["code"], -32000);
        assert_eq!(response["error"]["message"], "Request timeout");
        assert_eq!(response["error"]["data"]["timeoutMs"], 250);
        drop(pending);
        let waiter = tokio::spawn(external_waiter);
        kind.respond(backend.next(kind).await, Some(kind.value()));
        assert_recovered(vec![waiter], kind).await;
        let response = handle_json_rpc_value(
            state,
            &json!({
                "jsonrpc": "2.0", "id": 3, "method": kind.method()
            }),
        )
        .await;
        let response = parse_json_value_response(response).await;
        assert_eq!(response["result"], kind.value());
        assert_eq!(backend.call_count(), 2);
    }
}
