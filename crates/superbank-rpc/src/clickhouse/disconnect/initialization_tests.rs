// SPDX-License-Identifier: AGPL-3.0-only
//! Deterministic failure sequencing; real protocol coverage lives in integration_tests.
use super::*;
use std::sync::atomic::AtomicU8;
use tokio::sync::Semaphore;

struct Fixture {
    verifier: DisconnectVerifier,
    mode: Arc<AtomicU8>,
    requests: Arc<std::sync::atomic::AtomicUsize>,
    entered: Arc<Notify>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Fixture {
    async fn new(mode: u8) -> Self {
        use axum::response::IntoResponse;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let mode = Arc::new(AtomicU8::new(mode));
        let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let entered = Arc::new(Notify::new());
        let app = axum::Router::new().fallback({
            let (mode, requests, entered) = (mode.clone(), requests.clone(), entered.clone());
            move |body: String| {
                let (mode, requests, entered) = (mode.clone(), requests.clone(), entered.clone());
                async move {
                    requests.fetch_add(1, Ordering::SeqCst);
                    entered.notify_one();
                    if mode.load(Ordering::SeqCst) == 2 {
                        std::future::pending::<()>().await;
                    }
                    if mode.load(Ordering::SeqCst) == 0 && body.contains("system.processes") {
                        return (
                            axum::http::StatusCode::FORBIDDEN,
                            "process inspection denied",
                        )
                            .into_response();
                    }
                    let mut bytes = vec![5, b'n', b'o', b'd', b'e', b'1'];
                    if body.contains("AS coordinator") {
                        bytes.extend_from_slice(&1_u64.to_le_bytes());
                        bytes.push(1);
                    } else {
                        assert!(body.contains("system.processes"));
                        bytes.push(0);
                    }
                    bytes.into_response()
                }
            }
        });
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = HttpClient::default()
            .with_url(url)
            .with_validation(false)
            .with_compression(clickhouse::Compression::None);
        Self {
            verifier: DisconnectVerifier::new(client, None),
            mode,
            requests,
            entered,
            server,
        }
    }

    async fn assert_backoff(&self, permits: &Arc<Semaphore>, expected_requests: usize) {
        let mut tasks = tokio::task::JoinSet::new();
        for i in 0..32 {
            let verifier = self.verifier.clone();
            let permit = permits.clone().try_acquire_owned().unwrap();
            tasks.spawn(async move { verifier.arm(format!("retry-{i}"), permit).await.is_err() });
        }
        while let Some(result) = tasks.join_next().await {
            assert!(result.unwrap());
        }
        assert_eq!(self.requests.load(Ordering::SeqCst), expected_requests);
        assert_eq!(permits.available_permits(), 64);
        assert!(self.verifier.0.topology.get().is_none());
        assert!(self.verifier.0.pending.lock().unwrap().is_empty());
    }

    async fn assert_recovery(&self, permits: &Arc<Semaphore>, expected_requests: usize) {
        self.mode.store(1, Ordering::SeqCst);
        tokio::time::sleep(INITIALIZATION_BACKOFF + Duration::from_millis(20)).await;
        let mut guard = self
            .verifier
            .arm(
                "recovered".into(),
                permits.clone().try_acquire_owned().unwrap(),
            )
            .await
            .unwrap();
        assert!(self.verifier.0.topology.get().is_some());
        guard.disarm();
        assert_eq!(self.requests.load(Ordering::SeqCst), expected_requests);
        assert_eq!(permits.available_permits(), 64);
    }
}

#[tokio::test]
async fn permission_failure_blocks_initialization_and_shared_retries_then_recovers() {
    let fixture = Fixture::new(0).await;
    let permits = Arc::new(Semaphore::new(64));
    assert!(
        fixture
            .verifier
            .arm("first".into(), permits.clone().try_acquire_owned().unwrap())
            .await
            .is_err()
    );
    fixture.assert_backoff(&permits, 2).await;
    fixture.assert_recovery(&permits, 4).await;
}

#[tokio::test]
async fn cancelled_initialization_backs_off_and_releases_admission() {
    let fixture = Fixture::new(2).await;
    let permits = Arc::new(Semaphore::new(64));
    let verifier = fixture.verifier.clone();
    let permit = permits.clone().try_acquire_owned().unwrap();
    let task = tokio::spawn(async move { verifier.arm("cancelled".into(), permit).await });
    tokio::time::timeout(PROBE_TIMEOUT, fixture.entered.notified())
        .await
        .unwrap();
    task.abort();
    assert!(matches!(task.await, Err(error) if error.is_cancelled()));
    fixture.assert_backoff(&permits, 1).await;
    fixture.assert_recovery(&permits, 3).await;
}
