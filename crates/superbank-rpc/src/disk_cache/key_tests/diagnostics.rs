// SPDX-License-Identifier: AGPL-3.0-only
//! Synthetic local cache diagnosis. Timings describe this fixture, not production capacity.
use super::*;
use serde_json::{Value, json};

fn disposable_loopback_url(value: &str) -> bool {
    reqwest::Url::parse(value).is_ok_and(|url| {
        url.scheme() == "http"
            && url.host_str() == Some("127.0.0.1")
            && url.port().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.path() == "/"
            && url.query().is_none()
            && url.fragment().is_none()
    })
}

#[test]
fn diagnostic_url_rejects_remote_targets_and_ambiguous_authority() {
    assert!(disposable_loopback_url("http://127.0.0.1:18193"));
    for url in [
        "http://127.0.0.1:8123@remote-host:8123",
        "http://remote-host:8123",
        "http://user@127.0.0.1:8123",
        "https://127.0.0.1:8123",
        "http://127.0.0.1",
        "http://127.0.0.1:8123/path",
        "http://127.0.0.1:8123/?query=DROP",
        "http://127.0.0.1:8123/#fragment",
    ] {
        assert!(!disposable_loopback_url(url), "accepted {url}");
    }
}

async fn sample(cache: &DiskCache, state: &str, slot: u64, pinned: bool) -> Value {
    let start = std::time::Instant::now();
    let candidates = cache.inner.key_index.signature_candidates(
        1,
        4,
        key_index::SignatureHash::new(signature(slot).as_ref()),
    );
    let membership_ms = start.elapsed().as_secs_f64() * 1000.0;
    let start = std::time::Instant::now();
    let result = cache.get_tx(signature(slot), pinned.then_some(slot)).await;
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    let outcome = match result {
        DiskTransactionResult::Found(record) => {
            assert_eq!(record.slot, slot);
            assert_eq!(record.tx_version, if slot == 25 { Some(0) } else { None });
            "hit"
        }
        DiskTransactionResult::Unavailable if slot == 500 => "miss",
        other => panic!("unexpected result: {other:?}"),
    };
    json!({"membership":state,"slot":slot,"slot_pinned":pinned,"outcome":outcome,
        "candidate_partitions":candidates.partitions.len(),
        "membership_ms":membership_ms,"elapsed_ms":elapsed_ms})
}

async fn run(client: &clickhouse::Client, cache: &DiskCache, source: &ClickHouseClient) -> Value {
    assert!(cache.ready());
    assert!(!cache.signature_indexes_ready());
    let mut samples = Vec::new();
    for state in ["unknown", "ready"] {
        if state == "ready" {
            cache.build_signature_indexes().await;
            assert!(cache.signature_indexes_ready());
        }
        for _ in 0..20 {
            for slot in [15, 25, 500] {
                for pinned in [false, true] {
                    samples.push(sample(cache, state, slot, pinned).await);
                }
            }
        }
    }
    let mut phases = Vec::new();
    for _ in 0..20 {
        for slot in [15, 25] {
            let mut scoped = cache.query_client();
            scoped.cache_partition = Some((10, slot / 10));
            let phase = crate::clickhouse::measure_position_reads(
                &scoped,
                &signature(slot).to_string(),
                crate::clickhouse::SignatureSlot { slot, slot_idx: 0 },
            )
            .await;
            phases.push(json!({"slot":slot,"phases":phase}));
        }
    }
    let mut handlers = Vec::new();
    for slot in [15, 25] {
        for encoding in ["json", "base64"] {
            let config = json!({"encoding":encoding,"maxSupportedTransactionVersion":0,"commitment":"finalized"});
            let expected =
                transaction_response(source, None, signature(slot), config.clone()).await;
            assert!(!expected["result"].is_null(), "{expected}");
            let start = std::time::Instant::now();
            let actual = transaction_response(source, Some(cache), signature(slot), config).await;
            handlers.push(json!({"slot":slot,"encoding":encoding,"handler_and_body_ms":start.elapsed().as_secs_f64()*1000.0}));
            assert_eq!(actual, expected);
        }
    }
    assert_transaction_position_fallback(cache).await;
    assert_transaction_invalidation(cache).await;
    let db = &cache.inner.cfg.database;
    execute(
        client,
        &format!("RENAME TABLE {db}.transactions TO {db}.transactions_unavailable"),
    )
    .await;
    assert!(matches!(
        cache.get_tx(signature(45), Some(45)).await,
        DiskTransactionResult::Unavailable
    ));
    assert_transaction_fallback(source, cache).await;
    execute(
        client,
        &format!("RENAME TABLE {db}.transactions_unavailable TO {db}.transactions"),
    )
    .await;
    let version = client
        .query("SELECT version()")
        .fetch_one::<String>()
        .await
        .unwrap();
    json!({"clickhouse_version":version,"samples":samples,"phases":phases,"handler_samples":handlers,
        "checks":{"legacy_v0_parity":true,"stale_position":true,"invalidated_read":true,"unavailable_fallback":true}})
}

#[tokio::test]
#[ignore = "requires disposable loopback ClickHouse and GETTX_DIAGNOSTIC_OUTPUT"]
async fn get_transaction_local_diagnostics() {
    let url = std::env::var("DISK_CACHE_TEST_URL").expect("explicit disposable ClickHouse URL");
    assert!(
        disposable_loopback_url(&url),
        "explicit loopback HTTP URL required"
    );
    let output = std::env::var("GETTX_DIAGNOSTIC_OUTPUT").expect("output JSON artifact path");
    let database = format!("test_gettx_diagnostic_{}", now_version());
    let cache_database = format!("{database}_cache");
    let client = clickhouse::Client::default()
        .with_url(&url)
        .with_user("default");
    fixture(&client, &database).await;
    insert_transactions(&client, &database).await;
    let mut source = ClickHouseClient::new(
        &url,
        &database,
        "default",
        "",
        ClickHouseClientOptions::new(
            RoutingPolicy {
                transport: RoutingTransport::Http,
                scope: RoutingScope::Distributed,
            },
            None,
            vec![],
            format!("{database}.gsfa_hot"),
            format!("{database}.gsfa_hot"),
        ),
    );
    source.use_table_names(ClickHouseTableNames::in_database(&database));
    source.initialize_read_cancellation().await.unwrap();
    let cache = DiskCache::open(config(url, cache_database.clone()), &source)
        .await
        .unwrap();
    insert_transactions(&client, &cache_database).await;
    for db in [&database, &cache_database] {
        execute(&client,&format!("ALTER TABLE {db}.transactions UPDATE tx_version=0 WHERE slot=25 SETTINGS mutations_sync=2")).await;
    }
    cache
        .publish_range_coverage(
            (10..50)
                .map(|slot| (slot, SlotStatus::Covered { tx_count: 1 }))
                .collect(),
        )
        .await
        .unwrap();
    let result = run(&client, &cache, &source).await;
    execute(&client, &format!("DROP DATABASE {cache_database}")).await;
    execute(&client, &format!("DROP DATABASE {database}")).await;
    std::fs::write(output, serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}
