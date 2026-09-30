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

/// The owner-shard layout check's `system.tables` row for the mock's `default.signatures`: a
/// materialized view over `Distributed('my_cluster', 'default', 'signatures_local', ...)`.
fn owner_shard_layout_response(sql: &str) -> Option<Body> {
    if !(sql.contains("FROM system.tables") && sql.contains("engine_full")) {
        return None;
    }
    let mut row = Vec::new();
    for value in [
        "MaterializedView",
        "CREATE MATERIALIZED VIEW default.signatures (`signature` FixedString(64)) ENGINE = Distributed('my_cluster', 'default', 'signatures_local', cityHash64(signature)) AS SELECT 1",
        "",
    ] {
        // RowBinary strings: LEB128 length, then the bytes.
        let mut len = value.len();
        while len >= 0x80 {
            row.push(u8::try_from(len & 0x7f).unwrap() | 0x80);
            len >>= 7;
        }
        row.push(u8::try_from(len).unwrap());
        row.extend_from_slice(value.as_bytes());
    }
    Some(Body::from(row))
}

fn verification_response(sql: &str) -> Option<Body> {
    discovery_response(sql)
        .or_else(|| owner_shard_layout_response(sql))
        .or_else(|| {
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
        client.initialize_read_cancellation().await.unwrap();
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

const OWNER_SOURCE: &str = "cluster('my_cluster', default.signatures_local, cityHash64(signature))";

#[tokio::test]
async fn owner_shard_routing_is_off_by_default() {
    let mut mock = MockClickHouse::new(Vec::new(), false, false).await;
    assert_eq!(mock.client.signature_lookup_source(), "default.signatures");
    mock.client
        .get_signature_statuses(&signatures(2))
        .await
        .unwrap();
    let (sql, _) = mock.next_query().await;
    assert!(sql.contains("FROM default.signatures\n"));
    assert!(!sql.contains("cluster("));
    assert!(!sql.contains("force_optimize_skip_unused_shards"));
}

#[tokio::test]
async fn owner_shard_routing_rewrites_primary_status_batch() {
    let mut mock = MockClickHouse::new(Vec::new(), false, false).await;
    mock.client.allow_query_settings = true;
    mock.client
        .set_signatures_owner_shard_routing(true, "my_cluster", None)
        .unwrap();
    assert_eq!(mock.client.signature_lookup_source(), OWNER_SOURCE);
    mock.client
        .get_signature_statuses(&signatures(3))
        .await
        .unwrap();
    let (sql, _) = mock.next_query().await;
    assert!(sql.contains(&format!("FROM {OWNER_SOURCE}")), "{sql}");
    // The tuple filter stays for the primary key; `signature IN` lets the shards be pruned.
    assert!(sql.contains("(sig_bucket, signature) IN ("), "{sql}");
    assert!(
        sql.contains(") AND signature IN (toFixedString(unhex("),
        "{sql}"
    );
    assert_eq!(sql.matches("unhex(").count(), 6);
    assert!(sql.contains("optimize_skip_unused_shards=1"), "{sql}");
    assert!(sql.contains("force_optimize_skip_unused_shards=0"), "{sql}");
    assert!(sql.contains("max_execution_time_leaf="), "{sql}");
}

#[tokio::test]
async fn owner_shard_routing_rewrites_primary_signature_slot_lookup() {
    let mut mock = MockClickHouse::new(Vec::new(), false, false).await;
    mock.client.allow_query_settings = true;
    mock.client
        .set_signatures_owner_shard_routing(true, "{cluster}", Some("db.sigs_local"))
        .unwrap();
    let slot = mock
        .client
        .get_signature_slot(&signatures(1)[0])
        .await
        .unwrap();
    assert!(slot.0.is_none());
    let (sql, _) = mock.next_query().await;
    assert!(
        sql.contains("FROM cluster('{cluster}', db.sigs_local, cityHash64(signature))"),
        "{sql}"
    );
    assert!(sql.contains("PREWHERE sig_bucket = "), "{sql}");
    assert!(sql.contains("LIMIT 1"), "{sql}");
    assert_eq!(
        sql.matches("optimize_skip_unused_shards=1").count(),
        1,
        "{sql}"
    );
    assert!(sql.contains("force_optimize_skip_unused_shards=0"), "{sql}");
}

/// The inline gSFA cursor scalar is embedded in the address page query, whose settings
/// and source are the gSFA read's, so owner-shard routing deliberately leaves it on the view.
#[tokio::test]
async fn owner_shard_routing_leaves_gsfa_inline_cursor_on_the_view() {
    let mut mock = MockClickHouse::new(Vec::new(), false, false).await;
    mock.client.allow_query_settings = true;
    mock.client.set_gsfa_inline_cursor(true);
    mock.client
        .set_signatures_owner_shard_routing(true, "{cluster}", Some("db.sigs_local"))
        .unwrap();
    assert!(mock.client.signatures_owner_shard_routed());
    let mut cursor = [0u8; 64];
    cursor[0] = 7;
    let _ = mock
        .client
        .get_signatures_for_address_inline_cursor(
            "11111111111111111111111111111111",
            10,
            crate::clickhouse::GsfaCursor::Signature(cursor),
            crate::clickhouse::GsfaCursor::Resolved(None),
        )
        .await;
    let (sql, _) = mock.next_query().await;
    assert!(sql.contains("FROM default.signatures"), "{sql}");
    assert!(sql.contains(&signature_literal(&cursor)), "{sql}");
    assert!(!sql.contains("cluster("), "{sql}");
    assert!(!sql.contains("force_optimize_skip_unused_shards"), "{sql}");
}

#[tokio::test]
async fn owner_shard_routing_never_applies_to_local_cache_clients() {
    let mut mock = MockClickHouse::new(Vec::new(), false, false).await;
    mock.client.allow_query_settings = true;
    mock.client
        .set_signatures_owner_shard_routing(true, "my_cluster", None)
        .unwrap();
    assert_eq!(mock.client.signature_lookup_source(), OWNER_SOURCE);
    mock.client.cache_partition = Some((10, 1));
    assert_eq!(mock.client.signature_lookup_source(), "default.signatures");
    // Without query SETTINGS pruning cannot be requested, so the view is read.
    mock.client.cache_partition = None;
    mock.client.allow_query_settings = false;
    assert_eq!(mock.client.signature_lookup_source(), "default.signatures");
    mock.client.allow_query_settings = true;
    // A disabled flag clears any earlier source.
    mock.client.cache_partition = None;
    mock.client
        .set_signatures_owner_shard_routing(false, "my_cluster", None)
        .unwrap();
    assert_eq!(mock.client.signature_lookup_source(), "default.signatures");
}

#[tokio::test]
async fn owner_shard_routing_rejects_bad_configuration() {
    let mut mock = MockClickHouse::new(Vec::new(), false, false).await;
    mock.client.allow_query_settings = true;
    assert!(
        mock.client
            .set_signatures_owner_shard_routing(true, "", None)
            .is_err()
    );
    assert!(
        mock.client
            .set_signatures_owner_shard_routing(true, "my_cluster", Some("default.`.inner_id.x`"))
            .is_err()
    );
    assert_eq!(mock.client.signature_lookup_source(), "default.signatures");
}

#[tokio::test]
async fn owner_shard_startup_check_forces_pruning_or_disables_without_settings() {
    let mut mock = MockClickHouse::new(Vec::new(), false, false).await;
    mock.client
        .set_signatures_owner_shard_routing(true, "my_cluster", None)
        .unwrap();
    // Query SETTINGS unavailable: routing turns itself off rather than fanning out.
    mock.client
        .verify_signatures_owner_shard_routing()
        .await
        .unwrap();
    assert_eq!(mock.client.signature_lookup_source(), "default.signatures");

    mock.client
        .set_signatures_owner_shard_routing(true, "my_cluster", None)
        .unwrap();
    mock.client.allow_query_settings = true;
    // The layout check reads the view's storage (the mock answers it with a matching
    // `Distributed('my_cluster', 'default', 'signatures_local', ...)`) and the macros (empty body, no
    // macros); the cluster names match, so system.clusters is not read. The mock then answers
    // the probe with an empty body, which fails to decode a count.
    let err = mock
        .client
        .verify_signatures_owner_shard_routing()
        .await
        .unwrap_err();
    assert!(err.to_string().contains("startup check failed"), "{err}");
    let (sql, _) = mock.next_query().await;
    assert!(sql.contains("FROM system.tables WHERE database = 'default' AND name = 'signatures'"));
    let (sql, _) = mock.next_query().await;
    assert!(sql.contains("FROM system.macros"), "{sql}");
    let (sql, _) = mock.next_query().await;
    assert!(sql.starts_with(&format!("SELECT count() FROM {OWNER_SOURCE} PREWHERE")));
    assert!(sql.contains("force_optimize_skip_unused_shards=1"), "{sql}");
    // The probe uses the same literal shape as real lookups, so it keeps checking pruning of it.
    assert!(
        sql.contains(&format!("signature = {}", signature_literal(&[0u8; 64]))),
        "{sql}"
    );

    // A local table the view's storage does not read fails the layout check before the probe.
    mock.client
        .set_signatures_owner_shard_routing(true, "my_cluster", Some("default.other_local"))
        .unwrap();
    let err = mock
        .client
        .verify_signatures_owner_shard_routing()
        .await
        .unwrap_err();
    assert!(err.to_string().contains("layout check failed"), "{err}");
    let (sql, _) = mock.next_query().await;
    assert!(sql.contains("FROM system.tables"), "{sql}");
    let (sql, _) = mock.next_query().await;
    assert!(sql.contains("FROM system.macros"), "{sql}");
    assert!(
        mock.queries.try_recv().is_err(),
        "no probe after a failed layout check"
    );
}

/// Live check on a disposable 3-shard cluster (one server per shard, the `ddl/replicated`
/// database layout, `prefer_localhost_replica=0` so every shard read is a logged sub-query). Builds the
/// `signatures` materialized view over a `Distributed` table exactly like
/// `ddl/replicated/signatures.sql` (non-replicated engines), then compares the view with
/// owner-shard routing: identical rows, and one shard sub-query per signature instead of three.
///
/// `SUPERBANK_OWNER_SHARD_CLUSTER_URLS` lists the shard HTTP URLs in shard order; the first is
/// the coordinator. `SUPERBANK_OWNER_SHARD_CLUSTER` names the cluster (default `{cluster}`).
/// `SUPERBANK_OWNER_SHARD_WRONG_CLUSTER` optionally names a cluster with the same hosts in
/// another shard order, which the startup layout check must refuse.
#[tokio::test]
#[ignore = "requires SUPERBANK_OWNER_SHARD_CLUSTER_URLS: a disposable 3-shard ClickHouse cluster"]
async fn owner_shard_routing_prunes_and_matches_view_clickhouse() {
    let urls = std::env::var("SUPERBANK_OWNER_SHARD_CLUSTER_URLS")
        .expect("set SUPERBANK_OWNER_SHARD_CLUSTER_URLS");
    let urls = urls.split(',').map(str::trim).collect::<Vec<_>>();
    assert_eq!(urls.len(), 3, "expected three shard URLs");
    let cluster =
        std::env::var("SUPERBANK_OWNER_SHARD_CLUSTER").unwrap_or_else(|_| "{cluster}".into());
    let http = reqwest::Client::new();
    async fn execute(http: &reqwest::Client, url: &str, sql: String) -> String {
        let response = http.post(url).body(sql).send().await.expect("request");
        let status = response.status();
        let body = response.text().await.unwrap();
        assert!(status.is_success(), "ClickHouse {status}: {body}");
        body
    }
    let db = format!(
        "owner_shard_{}_{}",
        std::process::id(),
        crate::util::current_time_millis()
    );
    for url in &urls {
        for sql in [
            format!("CREATE DATABASE {db}"),
            format!(
                "CREATE TABLE {db}.transactions_local (slot UInt64, slot_idx UInt32, \
                 tx_signatures Array(FixedString(64)), meta_status_ok UInt8, meta_err Nullable(String)) \
                 ENGINE = MergeTree ORDER BY (slot, slot_idx)"
            ),
            format!(
                "CREATE TABLE {db}.signatures_local (sig_bucket UInt8 MATERIALIZED cityHash64(signature) % 32, \
                 signature FixedString(64), slot UInt64, slot_idx UInt32, err Nullable(String), \
                 INDEX bf_signature signature TYPE bloom_filter(0.01) GRANULARITY 64) \
                 ENGINE = ReplacingMergeTree(slot) PARTITION BY sig_bucket \
                 PRIMARY KEY (sig_bucket, signature, slot, slot_idx) \
                 ORDER BY (sig_bucket, signature, slot DESC, slot_idx) \
                 SETTINGS allow_experimental_reverse_key = 1, index_granularity = 512"
            ),
            format!(
                "CREATE MATERIALIZED VIEW {db}.signatures (sig_bucket UInt8 MATERIALIZED cityHash64(signature) % 32, \
                 signature FixedString(64), slot UInt64, slot_idx UInt32, err Nullable(String)) \
                 ENGINE = Distributed('{cluster}', '{db}', 'signatures_local', cityHash64(signature)) \
                 AS SELECT signature, slot, slot_idx, if(meta_status_ok = 1, NULL, meta_err) AS err \
                 FROM {db}.transactions_local ARRAY JOIN tx_signatures AS signature"
            ),
        ] {
            execute(&http, url, sql).await;
        }
    }
    let signature_bytes = |i: u8| {
        let mut bytes = [0u8; 64];
        bytes[0] = i;
        bytes[63] = 0xA5;
        bytes
    };
    let hex = |bytes: &[u8; 64]| hex::encode(bytes).to_uppercase();
    // 30 signatures at slot 1000+i; the first 10 again at a later slot with an error, so the
    // status read must pick the latest row across both.
    let mut values = Vec::new();
    for i in 0..30u8 {
        values.push(format!(
            "({}, {}, [toFixedString(unhex('{}'), 64)], 1, NULL)",
            1000 + u64::from(i),
            i % 7,
            hex(&signature_bytes(i))
        ));
    }
    for i in 0..10u8 {
        values.push(format!(
            "({}, 3, [toFixedString(unhex('{}'), 64)], 0, '{{\"InstructionError\":[0,\"Custom\"]}}')",
            90_000 + u64::from(i),
            hex(&signature_bytes(i))
        ));
    }
    execute(
        &http,
        urls[0],
        format!(
            "INSERT INTO {db}.transactions_local SETTINGS distributed_foreground_insert = 1 VALUES {}",
            values.join(",")
        ),
    )
    .await;
    // Rows landed only on their owner shard.
    for (index, url) in urls.iter().enumerate() {
        let misplaced = execute(
            &http,
            url,
            format!(
                "SELECT count() FROM {db}.signatures_local WHERE cityHash64(signature) % 3 != {index}"
            ),
        )
        .await;
        assert_eq!(misplaced.trim(), "0");
    }

    let client = |routing: bool| {
        let mut client = ClickHouseClient::new(
            urls[0],
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
                format!("{db}.gsfa_hot"),
                format!("{db}.gsfa_hot_local"),
            ),
        );
        client.allow_query_settings = true;
        client.signature_statuses_table = format!("{db}.signatures");
        client
            .set_signatures_owner_shard_routing(routing, &cluster, None)
            .unwrap();
        client
    };
    let view = client(false);
    let mut owner = client(true);
    for client in [&view, &owner] {
        client
            .initialize_read_cancellation()
            .await
            .expect("cancellation preflight");
    }
    // The startup check runs with force_optimize_skip_unused_shards=1.
    owner
        .verify_signatures_owner_shard_routing()
        .await
        .expect("owner-shard startup check");
    assert_eq!(
        owner.signature_lookup_source(),
        format!("cluster('{cluster}', {db}.signatures_local, cityHash64(signature))")
    );
    // A cluster that exists but lays the shards out differently (e.g. `my_cluster_rev`: the same hosts
    // in reverse order) passes the force=1 probe but would miss about two thirds of the rows;
    // the layout check refuses it at startup.
    if let Ok(wrong_cluster) = std::env::var("SUPERBANK_OWNER_SHARD_WRONG_CLUSTER") {
        let mut wrong = client(false);
        wrong
            .set_signatures_owner_shard_routing(true, &wrong_cluster, None)
            .unwrap();
        wrong
            .initialize_read_cancellation()
            .await
            .expect("cancellation preflight");
        let err = wrong
            .verify_signatures_owner_shard_routing()
            .await
            .expect_err("a differently laid out cluster must fail the layout check");
        assert!(err.to_string().contains("different shard layout"), "{err}");
    }

    // Shard sub-queries that touched this database, summed over every shard.
    let sub_queries = || {
        let http = http.clone();
        let urls = urls.clone();
        let db = db.clone();
        async move {
            let mut total = 0u64;
            for url in &urls {
                execute(&http, url, "SYSTEM FLUSH LOGS".into()).await;
            }
            for url in &urls {
                let count = execute(
                    &http,
                    url,
                    format!(
                        "SELECT count() FROM system.query_log WHERE type = 'QueryFinish' \
                         AND is_initial_query = 0 AND query LIKE '%`{db}`.`signatures_local`%'"
                    ),
                )
                .await;
                total += count.trim().parse::<u64>().unwrap();
            }
            total
        }
    };

    let mut lookups = (0..30u8)
        .map(|i| bs58::encode(signature_bytes(i)).into_string())
        .collect::<Vec<_>>();
    lookups.push(bs58::encode(signature_bytes(200)).into_string()); // unknown
    let n = lookups.len() as u64;

    let before = sub_queries().await;
    let mut view_slots = Vec::new();
    for signature in &lookups {
        view_slots.push(view.get_signature_slot(signature).await.unwrap().0);
    }
    let after_view = sub_queries().await;
    let mut owner_slots = Vec::new();
    for signature in &lookups {
        owner_slots.push(owner.get_signature_slot(signature).await.unwrap().0);
    }
    let after_owner = sub_queries().await;
    let slot_key = |slot: &Option<SignatureSlot>| slot.as_ref().map(|s| (s.slot, s.slot_idx));
    assert_eq!(
        view_slots.iter().map(slot_key).collect::<Vec<_>>(),
        owner_slots.iter().map(slot_key).collect::<Vec<_>>()
    );
    assert_eq!(slot_key(&owner_slots[0]), Some((90_000, 3)));
    assert_eq!(slot_key(&owner_slots[29]), Some((1029, 29 % 7)));
    assert!(owner_slots[30].is_none());
    assert_eq!(after_view - before, 3 * n, "the view queries every shard");
    assert_eq!(
        after_owner - after_view,
        n,
        "owner routing queries one shard"
    );

    // Status batches: all signatures (every shard owns some), and two owned by one shard.
    let shard_of = |i: u8| ch_cityhash102::cityhash64(&signature_bytes(i)) % 3;
    let same_shard = (1..30u8)
        .find(|i| shard_of(*i) == shard_of(0))
        .expect("two signatures on one shard");
    let pair = vec![lookups[0].clone(), lookups[usize::from(same_shard)].clone()];
    let status_key = |mut rows: Vec<SignatureStatusRecord>| {
        rows.sort_by(|a, b| a.signature.cmp(&b.signature));
        rows.into_iter()
            .map(|row| (row.signature, row.slot, row.err.is_some()))
            .collect::<Vec<_>>()
    };
    let before = sub_queries().await;
    let view_all = view.get_signature_statuses(&lookups).await.unwrap().0;
    let view_pair = view.get_signature_statuses(&pair).await.unwrap().0;
    let after_view = sub_queries().await;
    let owner_all = owner.get_signature_statuses(&lookups).await.unwrap().0;
    let owner_pair = owner.get_signature_statuses(&pair).await.unwrap().0;
    let after_owner = sub_queries().await;
    let owner_all = status_key(owner_all);
    assert_eq!(status_key(view_all), owner_all);
    assert_eq!(status_key(view_pair), status_key(owner_pair));
    assert_eq!(owner_all.len(), 30);
    assert_eq!(
        owner_all.iter().filter(|(_, _, err)| *err).count(),
        10,
        "latest rows carry the error"
    );
    assert_eq!(
        after_view - before,
        6,
        "the view queries every shard per batch"
    );
    assert_eq!(
        after_owner - after_view,
        3 + 1,
        "all-shard batch reaches 3 shards, same-shard pair reaches 1"
    );

    for url in &urls {
        execute(&http, url, format!("DROP DATABASE {db} SYNC")).await;
    }
}
