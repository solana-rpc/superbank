// SPDX-License-Identifier: AGPL-3.0-only
//! Real protocol tests run by test-clickhouse-http-disconnect.py, never against production.

use super::*;
use crate::clickhouse::{
    ClickHouseClient, ClickHouseClientOptions, RoutingPolicy, RoutingScope, RoutingTransport,
};
use tokio::sync::Semaphore;

struct Fixture {
    url: String,
    control: String,
    client: HttpClient,
}

impl Fixture {
    fn new() -> Self {
        let url = fixture_url("SUPERBANK_DISCONNECT_TEST_URL");
        let control = fixture_url("SUPERBANK_DISCONNECT_TEST_CONTROL_URL");
        let client = super::super::client::build_clickhouse_http_client(
            &url,
            "default",
            "",
            "",
            Duration::from_secs(2),
        );
        // Keep production validation and compression. Do not replace this with
        // an unvalidated synthetic RowBinary response or a JSON-only client.
        Self {
            url,
            control,
            client,
        }
    }

    fn verifier(&self) -> DisconnectVerifier {
        DisconnectVerifier::new(self.client.clone(), Some("{cluster}".into()))
    }

    async fn control(&self, action: &str) {
        reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .post(format!("{}/{}", self.control, action))
            .timeout(Duration::from_secs(15))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    async fn active_nodes(&self, id: &str) -> HashSet<String> {
        let sql = format!(
            "SELECT DISTINCT hostName() AS node FROM \
            clusterAllReplicas('fixture',system.processes) WHERE \
            query_id={} OR initial_query_id={}",
            quoted(id),
            quoted(id)
        );
        self.client
            .query(&sql)
            .fetch_all::<NodeRow>()
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.node)
            .collect()
    }

    async fn wait_active(&self, id: &str) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while self.active_nodes(id).await.len() != 3 {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("query must run on coordinator and both remote nodes");
    }

    async fn terminals(&self, id: &str) -> Vec<TerminalRow> {
        let sql = format!(
            "SELECT hostName() AS node,toString(type) AS kind,exception_code AS code,exception,\
            is_initial_query AS initial,toUInt8(Settings['readonly']) AS readonly,\
            toUInt8(Settings['cancel_http_readonly_queries_on_client_close']) AS cancel \
            FROM clusterAllReplicas('fixture',system.query_log) WHERE \
            initial_query_id={} AND type!='QueryStart'",
            quoted(id)
        );
        self.client
            .query(&sql)
            .fetch_all::<TerminalRow>()
            .await
            .unwrap()
    }

    async fn assert_cancelled(&self, id: &str) {
        let rows = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let rows = self.terminals(id).await;
                if rows.len() == 3 {
                    break rows;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("all participating executions must have terminal evidence");
        assert_eq!(
            rows.iter()
                .map(|row| &row.node)
                .collect::<HashSet<_>>()
                .len(),
            3
        );
        assert_eq!(rows.iter().filter(|row| row.initial == 1).count(), 1);
        for row in rows {
            assert_ne!(
                row.kind, "QueryFinish",
                "slow fixture must cancel, not finish"
            );
            assert!(
                row.cancelled_by_disconnect(),
                "unexpected terminal: {row:?}"
            );
            assert_eq!((row.readonly, row.cancel), (2, 1));
        }
    }
}

fn fixture_url(name: &str) -> String {
    let value = std::env::var(name).expect("run through the isolated Python fixture");
    let url = reqwest::Url::parse(&value).unwrap();
    assert_eq!(url.scheme(), "http");
    assert_eq!(
        url.host_str(),
        Some("127.0.0.1"),
        "fixture must be loopback-only"
    );
    value.trim_end_matches('/').to_owned()
}

#[derive(clickhouse::Row, Deserialize)]
struct NodeRow {
    node: String,
}

#[derive(clickhouse::Row, Deserialize, Debug)]
struct TerminalRow {
    node: String,
    kind: String,
    code: i32,
    exception: String,
    initial: u8,
    readonly: u8,
    cancel: u8,
}

impl TerminalRow {
    fn cancelled_by_disconnect(&self) -> bool {
        if matches!(self.code, 394 | 735) {
            return true;
        }
        // A streaming leaf can observe the coordinator closing its native
        // socket before it reads the cancellation packet. Restrict this race
        // to explicit socket-close errors on leaves; the coordinator must
        // still report cancellation, and all executions must end promptly.
        self.initial == 0
            && self.code == 210
            && self.exception.contains("while writing to socket")
            && (self.exception.contains("Connection reset by peer")
                || self.exception.contains("Broken pipe"))
    }
}

#[derive(clickhouse::Row, Deserialize)]
struct SumRow {
    total: u64,
}

#[derive(clickhouse::Row, Deserialize)]
struct StreamRow {
    number: u64,
    delay: u8,
    padding: String,
}

async fn wait_released(semaphore: &Semaphore) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while semaphore.available_permits() != 1 {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("verified termination must release admission within five seconds");
}

fn slow_query(client: &HttpClient, sql: &str, id: &str) -> clickhouse::query::Query {
    client
        .query(sql)
        .with_setting("query_id", id)
        .with_setting("readonly", "2")
        .with_setting("cancel_http_readonly_queries_on_client_close", "1")
        .with_setting("max_threads", "1")
        .with_setting("max_block_size", "1")
        .with_setting("max_parallel_replicas", "1")
        .with_setting("use_hedged_requests", "0")
        .with_setting("max_execution_time", "35")
        .with_setting("max_execution_time_leaf", "35")
        .with_setting("wait_end_of_query", "0")
        .with_setting("buffer_size", "1")
        .with_setting("log_query_settings", "1")
}

#[tokio::test]
#[ignore = "requires isolated pinned three-node ClickHouse fixture"]
async fn real_protocol_discovery_probe_and_status_results() {
    let fixture = Fixture::new();
    let verifier = fixture.verifier();
    let semaphore = Arc::new(Semaphore::new(1));
    let id = next_required_query_id("native_integration_discovery");
    let mut guard = verifier
        .arm(id.clone(), semaphore.clone().acquire_owned().await.unwrap())
        .await
        .expect("real discovery, macro, and capability rows must deserialize");
    assert_eq!(verifier.0.topology.get().unwrap().nodes.len(), 3);
    assert!(probe(&verifier.0, &[id]).await.unwrap().is_empty());
    guard.disarm();
    assert_eq!(semaphore.available_permits(), 1);

    let options = ClickHouseClientOptions::new(
        RoutingPolicy {
            transport: RoutingTransport::Http,
            scope: RoutingScope::Distributed,
        },
        None,
        Vec::new(),
        String::new(),
        String::new(),
    )
    .with_query_cleanup_cluster("{cluster}".into());
    let mut client = ClickHouseClient::new(&fixture.url, "default", "", "", options);
    client.signature_statuses_table = "default.fixture_signatures".into();
    client.bucket_moduli.signatures = 32;
    let known = bs58::encode([0x11; 64]).into_string();
    let missing = bs58::encode([0x22; 64]).into_string();
    let failed = bs58::encode([0x33; 64]).into_string();
    let (rows, _) = client
        .get_signature_statuses(&[known.clone(), missing.clone(), failed.clone()])
        .await
        .expect("real StatusRow schema and native settings must work");
    assert_eq!(rows.len(), 2);
    let hit = rows.iter().find(|row| row.signature == known).unwrap();
    assert_eq!(hit.slot, 17);
    assert!(hit.err.is_none());
    let failed = rows.iter().find(|row| row.signature == failed).unwrap();
    assert_eq!(failed.slot, 19);
    assert_eq!(failed.err, Some(serde_json::json!("BlockhashNotFound")));
    assert!(
        client
            .get_signature_statuses(&[missing])
            .await
            .unwrap()
            .0
            .is_empty()
    );
    let mut held = Vec::new();
    for _ in 0..4 {
        held.push(
            tokio::time::timeout(
                Duration::from_secs(1),
                client.acquire_signature_status_permits(),
            )
            .await
            .expect("successful status responses must release all source admission")
            .unwrap(),
        );
    }
    assert_eq!(held.len(), 4);
}

async fn cancel_real_query(fixture: &Fixture, streaming: bool) {
    let verifier = fixture.verifier();
    let semaphore = Arc::new(Semaphore::new(1));
    let id = next_required_query_id("native_integration_cancel");
    let guard = verifier
        .arm(id.clone(), semaphore.clone().acquire_owned().await.unwrap())
        .await
        .unwrap();
    if streaming {
        // Repeated bytes remain buffered after LZ4 compression. Deterministic
        // hash text produces enough wire bytes to decode a row while work runs.
        let sql = concat!(
            "SELECT number,delay,arrayStringConcat(arrayMap(x -> ",
            "hex(SHA512(concat(toString(number), ':', toString(x)))), ",
            "range(2048))) AS padding FROM slow_all"
        );
        let mut cursor = slow_query(&fixture.client, sql, &id)
            .fetch::<StreamRow>()
            .unwrap();
        let row = tokio::time::timeout(Duration::from_secs(5), cursor.next())
            .await
            .expect("a compressed, validated row must arrive before cancellation")
            .unwrap()
            .unwrap();
        assert!(row.number < 600);
        assert_eq!(row.delay, 0);
        assert_eq!(row.padding.len(), 262144);
        fixture.wait_active(&id).await;
        drop(cursor);
    } else {
        let query = slow_query(
            &fixture.client,
            "SELECT sum(delay) AS total FROM slow_all",
            &id,
        )
        // Hold the complete aggregate response, including validated schema
        // headers, so this exercises dropping before HTTP response headers.
        .with_setting("wait_end_of_query", "1")
        .with_setting("send_progress_in_http_headers", "0");
        let task = tokio::spawn(async move {
            let rows = query.fetch_all::<SumRow>().await?;
            Ok::<_, clickhouse::error::Error>(rows.into_iter().map(|row| row.total).sum::<u64>())
        });
        fixture.wait_active(&id).await;
        assert!(
            !task.is_finished(),
            "query must still be waiting for headers"
        );
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
    }
    drop(guard);
    wait_released(&semaphore).await;
    assert!(fixture.active_nodes(&id).await.is_empty());
    fixture.assert_cancelled(&id).await;
}

#[tokio::test]
#[ignore = "requires isolated pinned three-node ClickHouse fixture"]
async fn real_protocol_cancellation_before_headers_and_streaming() {
    let fixture = Fixture::new();
    cancel_real_query(&fixture, false).await;
    cancel_real_query(&fixture, true).await;
    fixture.control("block-ddl").await;
    cancel_real_query(&fixture, false).await;
    cancel_real_query(&fixture, true).await;
    fixture.control("assert-ddl-blocked").await;
}

#[tokio::test]
#[ignore = "requires isolated pinned three-node ClickHouse fixture"]
async fn real_protocol_replica_outage_retains_admission_until_recovery() {
    let fixture = Fixture::new();
    let verifier = fixture.verifier();
    let semaphore = Arc::new(Semaphore::new(1));
    let id = next_required_query_id("native_integration_outage");
    let guard = verifier
        .arm(id, semaphore.clone().acquire_owned().await.unwrap())
        .await
        .unwrap();
    fixture.control("pause-replica").await;
    drop(guard);
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(
        semaphore.available_permits(),
        0,
        "unreachable replica must retain admission"
    );
    fixture.control("resume-replica").await;
    wait_released(&semaphore).await;
}
