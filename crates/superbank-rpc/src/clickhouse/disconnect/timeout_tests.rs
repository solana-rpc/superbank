// SPDX-License-Identifier: AGPL-3.0-only
use super::*;
use std::sync::atomic::AtomicU64;
use tokio::sync::Semaphore;

type Requests = Arc<Mutex<Vec<(usize, BTreeMap<String, String>)>>>;

struct Fixture {
    verifier: DisconnectVerifier,
    delays: Arc<[AtomicU64; 4]>,
    requests: Requests,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Fixture {
    async fn new(timeouts: VerificationTimeouts, delays: [u64; 4]) -> Self {
        use axum::extract::OriginalUri;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let delays = Arc::new(delays.map(AtomicU64::new));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let app = axum::Router::new().fallback({
            let delays = delays.clone();
            let requests = requests.clone();
            move |OriginalUri(uri): OriginalUri, body: String| {
                let delays = delays.clone();
                let requests = requests.clone();
                async move {
                    let phase = if body.contains("getMacro") {
                        0
                    } else if body.contains("AS coordinator") {
                        1
                    } else if body.contains("status_disconnect_preflight") {
                        2
                    } else {
                        3
                    };
                    let url = reqwest::Url::parse(&format!("http://localhost{uri}")).unwrap();
                    requests
                        .lock()
                        .unwrap()
                        .push((phase, url.query_pairs().into_owned().collect()));
                    tokio::time::sleep(Duration::from_millis(delays[phase].load(Ordering::SeqCst)))
                        .await;
                    let mut bytes = Vec::new();
                    if phase == 0 {
                        super::tests::string(&mut bytes, "cluster1");
                    } else {
                        super::tests::string(&mut bytes, "node1");
                        if phase == 1 {
                            bytes.extend_from_slice(&0_u64.to_le_bytes());
                            bytes.push(0);
                            super::tests::string(&mut bytes, "node1");
                            bytes.extend_from_slice(&1_u64.to_le_bytes());
                            bytes.push(1);
                        } else {
                            bytes.push(0);
                        }
                    }
                    bytes
                }
            }
        });
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = HttpClient::default()
            .with_url(url)
            .with_validation(false)
            .with_compression(clickhouse::Compression::None);
        Self {
            verifier: DisconnectVerifier::new(client, Some("{cluster}".into()), timeouts),
            delays,
            requests,
            server,
        }
    }
}

#[tokio::test]
async fn startup_budget_is_per_query_and_independent_of_runtime() {
    let fixture = Fixture::new(
        VerificationTimeouts {
            startup: Duration::from_millis(300),
            runtime: Duration::from_millis(30),
        },
        [150, 150, 150, 0],
    )
    .await;
    let started = Instant::now();
    fixture.verifier.initialize_ready().await.unwrap();
    assert!(started.elapsed() > Duration::from_millis(300));
    assert_eq!(fixture.requests.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn every_startup_phase_times_out_and_retries_with_the_startup_budget() {
    for phase in 0..3 {
        let mut delays = [0; 4];
        delays[phase] = 200;
        let fixture = Fixture::new(
            VerificationTimeouts {
                startup: Duration::from_millis(40),
                runtime: Duration::from_secs(10),
            },
            delays,
        )
        .await;
        let started = Instant::now();
        assert!(matches!(
            fixture.verifier.initialize_ready().await,
            Err(ProcessingError::Timeout { .. })
        ));
        assert!(started.elapsed() < Duration::from_millis(200));
        assert_eq!(fixture.requests.lock().unwrap().len(), phase + 1);
        assert!(fixture.verifier.0.topology.get().is_none());
        assert!(fixture.verifier.initialize_ready().await.is_err());
        assert_eq!(fixture.requests.lock().unwrap().len(), phase + 1);
        tokio::time::sleep(INITIALIZATION_BACKOFF + Duration::from_millis(10)).await;
        assert!(matches!(
            fixture.verifier.initialize_ready().await,
            Err(ProcessingError::Timeout { .. })
        ));
        fixture.delays[phase].store(0, Ordering::SeqCst);
        tokio::time::sleep(INITIALIZATION_BACKOFF + Duration::from_millis(10)).await;
        fixture.verifier.initialize_ready().await.unwrap();
    }
}

#[tokio::test]
async fn default_budgets_allow_startup_and_runtime_probes_longer_than_one_second() {
    let fixture = Fixture::new(Default::default(), [1100, 1100, 1100, 1100]).await;
    fixture.verifier.initialize_ready().await.unwrap();
    probe(&fixture.verifier.0, &["q".into()]).await.unwrap();
    for (_, settings) in fixture.requests.lock().unwrap().iter() {
        assert_eq!(settings["max_execution_time"], "10");
        assert_eq!(settings["max_execution_time_leaf"], "10");
    }
}

#[tokio::test]
async fn runtime_budget_is_independent_and_server_limits_round_up() {
    let fixture = Fixture::new(
        VerificationTimeouts {
            startup: Duration::from_millis(1001),
            runtime: Duration::from_millis(2500),
        },
        [0, 0, 0, 1200],
    )
    .await;
    fixture.verifier.initialize_ready().await.unwrap();
    probe(&fixture.verifier.0, &["q".into()]).await.unwrap();
    for (phase, settings) in fixture.requests.lock().unwrap().iter() {
        let seconds = if *phase == 3 { "3" } else { "2" };
        assert_eq!(settings["max_execution_time"], seconds);
        assert_eq!(settings["max_execution_time_leaf"], seconds);
    }
}

#[tokio::test]
async fn runtime_timeout_resets_evidence_retains_admission_and_recovers() {
    let fixture = Fixture::new(
        VerificationTimeouts {
            startup: Duration::from_secs(10),
            runtime: Duration::from_millis(80),
        },
        [0; 4],
    )
    .await;
    fixture.verifier.initialize_ready().await.unwrap();
    let permits = Arc::new(Semaphore::new(1));
    super::tests::pending(&fixture.verifier, &permits, "q");
    let ids = ["q".into()];
    assert_eq!(
        probe_batches(&fixture.verifier.0, &ids).await,
        Duration::from_millis(250)
    );
    assert_eq!(
        fixture.verifier.0.pending.lock().unwrap()["q"].quiet_observations,
        1
    );
    fixture
        .verifier
        .0
        .pending
        .lock()
        .unwrap()
        .get_mut("q")
        .unwrap()
        .abandoned_at = Instant::now() - UNCONFIRMED_AFTER;
    fixture.delays[3].store(200, Ordering::SeqCst);
    let started = Instant::now();
    assert_eq!(
        probe_batches(&fixture.verifier.0, &ids).await,
        Duration::from_secs(1)
    );
    assert!(started.elapsed() >= Duration::from_millis(80));
    assert!(started.elapsed() < Duration::from_millis(200));
    assert_eq!(permits.available_permits(), 0);
    {
        let pending = fixture.verifier.0.pending.lock().unwrap();
        assert_eq!(pending["q"].quiet_observations, 0);
        assert!(pending["q"].unconfirmed);
    }
    fixture.delays[3].store(0, Ordering::SeqCst);
    probe_batches(&fixture.verifier.0, &ids).await;
    assert_eq!(permits.available_permits(), 0);
    probe_batches(&fixture.verifier.0, &ids).await;
    assert_eq!(permits.available_permits(), 1);
}

#[tokio::test]
async fn execution_settings_round_milliseconds_up_at_boundaries() {
    let fixture = Fixture::new(Default::default(), [0; 4]).await;
    for (millis, seconds) in [
        (999, "1"),
        (1000, "1"),
        (1001, "2"),
        (10_000, "10"),
        (10_001, "11"),
    ] {
        fetch::<ProbeRow>(
            &fixture.verifier.0.client,
            "SELECT node, active_id",
            "test",
            Duration::from_millis(millis),
        )
        .await
        .unwrap();
        let requests = fixture.requests.lock().unwrap();
        let settings = &requests.last().unwrap().1;
        assert_eq!(settings["max_execution_time"], seconds);
        assert_eq!(settings["max_execution_time_leaf"], seconds);
    }
}

#[tokio::test]
async fn slow_runtime_timeout_evaluates_unconfirmed_only_after_probe_finishes() {
    let fixture = Fixture::new(
        VerificationTimeouts {
            startup: Duration::from_secs(10),
            runtime: Duration::from_millis(5500),
        },
        [0, 0, 0, 10_000],
    )
    .await;
    fixture.verifier.initialize_ready().await.unwrap();
    let permits = Arc::new(Semaphore::new(1));
    super::tests::pending(&fixture.verifier, &permits, "q");
    let verifier = fixture.verifier.clone();
    let task = tokio::spawn(async move { probe_batches(&verifier.0, &["q".into()]).await });
    tokio::time::sleep(Duration::from_millis(5100)).await;
    assert!(!task.is_finished());
    assert!(!fixture.verifier.0.pending.lock().unwrap()["q"].unconfirmed);
    assert_eq!(permits.available_permits(), 0);
    assert_eq!(task.await.unwrap(), Duration::from_secs(1));
    assert!(fixture.verifier.0.pending.lock().unwrap()["q"].unconfirmed);
    assert_eq!(permits.available_permits(), 0);
}
