// SPDX-License-Identifier: AGPL-3.0-only
//! Agave contracts exercised through real Native cache fills and HTTP queries.
use super::*;
use serde_json::{Value, json};

fn source_client(url: &str, database: &str) -> ClickHouseClient {
    let mut source = ClickHouseClient::new(
        url,
        database,
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
    source.use_table_names(ClickHouseTableNames::in_database(database));
    source
}

async fn setup() -> (
    clickhouse::Client,
    ClickHouseClient,
    DiskCacheConfig,
    DiskCache,
) {
    let url = std::env::var("DISK_CACHE_TEST_URL").expect("explicit disposable ClickHouse URL");
    assert!(url.starts_with("http://127.0.0.1:"));
    let database = format!("test_agave43_{}", now_version());
    let client = clickhouse::Client::default()
        .with_url(&url)
        .with_user("default");
    fixture(&client, &database).await;
    insert_transactions(&client, &database).await;
    let source = source_client(&url, &database);
    let cfg = config(url, format!("{database}_cache"));
    let cache = DiskCache::open(cfg.clone(), &source).await.unwrap();
    (client, source, cfg, cache)
}

async fn fill(cache: &DiskCache, source: &ClickHouseClient) {
    filler::fill_range(
        cache,
        source,
        filler::SlotRange { start: 10, end: 14 },
        &filler::FillerConfig::default(),
    )
    .await
    .unwrap();
    cache.build_key_indexes().await;
    cache.build_signature_indexes().await;
}

async fn cleanup(client: &clickhouse::Client, cfg: &DiskCacheConfig) {
    execute(client, &format!("DROP DATABASE {} SYNC", cfg.database)).await;
    execute(
        client,
        &format!(
            "DROP DATABASE {} SYNC",
            cfg.database.trim_end_matches("_cache")
        ),
    )
    .await;
}

fn expected_error(version: Option<u8>, encoding: &str, maximum: Option<u8>) -> Option<i64> {
    if matches!(encoding, "base58" | "binary") && maximum.is_some_and(|v| v >= 1) {
        return Some(-32602);
    }
    match (version, maximum) {
        (Some(version), Some(maximum)) if version <= maximum => None,
        (Some(_), _) => Some(-32015),
        (None, _) => None,
    }
}

fn assert_contract(body: &Value, expected: Option<i64>) {
    assert_eq!(body["error"]["code"].as_i64(), expected, "{body}");
    if expected == Some(-32602) {
        assert_eq!(
            body["error"]["message"],
            "base58 encoding is not supported with maxSupportedTransactionVersion >= 1"
        );
        assert!(body["error"].get("data").is_none());
    } else if expected.is_none() {
        assert!(!body["result"].is_null(), "{body}");
    }
}

#[tokio::test]
#[ignore = "requires disposable ClickHouse: DISK_CACHE_TEST_URL"]
async fn agave43_disk_cached_encoding_matrix() {
    let (client, source, cfg, cache) = setup().await;
    let database = cfg.database.trim_end_matches("_cache");
    execute(&client, &format!("ALTER TABLE {database}.transactions UPDATE tx_version=0 WHERE slot=11 SETTINGS mutations_sync=2")).await;
    execute(&client, &format!("ALTER TABLE {database}.transactions UPDATE tx_version=1 WHERE slot IN (12,13) SETTINGS mutations_sync=2")).await;
    execute(&client, &format!("ALTER TABLE {database}.transactions UPDATE tx_config_priority_fee=42 WHERE slot=13 SETTINGS mutations_sync=2")).await;
    fill(&cache, &source).await;
    let unavailable = source_client("http://127.0.0.1:1", database);
    for (slot, version) in [(10, None), (11, Some(0)), (12, Some(1)), (13, Some(1))] {
        assert_encoding_matrix(&unavailable, &cache, slot, version).await;
        assert_block_encoding_matrix(&unavailable, &cache, slot, version).await;
    }
    cleanup(&client, &cfg).await;
}

async fn assert_encoding_matrix(
    source: &ClickHouseClient,
    cache: &DiskCache,
    slot: u64,
    version: Option<u8>,
) {
    for encoding in ["base58", "binary", "base64", "json", "jsonParsed"] {
        for maximum in [None, Some(0), Some(1), Some(255)] {
            let config =
                json!({"slot":slot,"encoding":encoding,"maxSupportedTransactionVersion":maximum});
            let body = transaction_response(source, Some(cache), signature(slot), config).await;
            assert_contract(&body, expected_error(version, encoding, maximum));
        }
    }
}

async fn block_response(
    source: &ClickHouseClient,
    cache: &DiskCache,
    slot: u64,
    config: Value,
) -> Value {
    let mut state = Arc::try_unwrap(crate::tests::test_state()).ok().unwrap();
    state.clickhouse = source.clone();
    state.disk_cache = Some(Arc::new(tokio::sync::OnceCell::new_with(Some(Arc::new(
        cache.clone(),
    )))));
    let response = crate::handlers::blocks::handle_get_block(
        Arc::new(state),
        json!(1),
        Some(vec![json!(slot), config]),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap()
}

async fn assert_block_encoding_matrix(
    source: &ClickHouseClient,
    cache: &DiskCache,
    slot: u64,
    version: Option<u8>,
) {
    for details in ["full", "accounts", "signatures", "none"] {
        for encoding in ["base58", "binary", "base64", "json", "jsonParsed"] {
            for maximum in [None, Some(0), Some(1), Some(255)] {
                let config = json!({"encoding":encoding,"maxSupportedTransactionVersion":maximum,"transactionDetails":details});
                let body = block_response(source, cache, slot, config).await;
                let projected = if matches!(details, "none" | "signatures") {
                    None
                } else {
                    version
                };
                assert_contract(&body, expected_error(projected, encoding, maximum));
            }
        }
    }
}

