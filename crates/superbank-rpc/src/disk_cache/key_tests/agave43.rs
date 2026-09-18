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

async fn insert_vat_rewards(
    client: &clickhouse::Client,
    database: &str,
    slot: u64,
    spelling: &str,
) {
    let columns = format!(
        "rewards_present=1,rewards_pubkey=[toFixedString('vat',32),toFixedString('stake',32)],rewards_lamports=[-10,100],rewards_post_balance=[90,200],rewards_type=['{spelling}','Staking'],rewards_commission=[NULL,7],rewards_commission_bps=[NULL,725]"
    );
    execute(client,&format!("ALTER TABLE {database}.blocks_metadata UPDATE {columns} WHERE slot={slot} SETTINGS mutations_sync=2")).await;
    let tx_columns = columns
        .replace("rewards_present", "meta_rewards_present")
        .replace("rewards_", "meta_reward_")
        .replace("meta_meta_reward_present", "meta_rewards_present");
    execute(client,&format!("ALTER TABLE {database}.transactions UPDATE {tx_columns} WHERE slot={slot} SETTINGS mutations_sync=2")).await;
}

fn assert_vat_json(rewards: &Value) {
    assert_eq!(rewards[0]["rewardType"], "VATDebit");
    assert_eq!(rewards[0]["lamports"], -10);
    assert_eq!(rewards[0]["postBalance"], 90);
    assert_eq!(rewards[1]["commission"], 7);
    assert_eq!(rewards[1]["commissionBps"], 725);
}

async fn assert_vat_cache(source: &ClickHouseClient, cache: &DiskCache, slot: u64, spelling: &str) {
    let stored = found_transaction(cache.get_tx(signature(slot), Some(slot)).await);
    assert_eq!(
        stored.meta_reward_type,
        vec![Some(spelling.to_owned()), Some("Staking".to_owned())]
    );
    assert_eq!(stored.meta_reward_lamports, vec![-10, 100]);
    assert_eq!(stored.meta_reward_post_balance, vec![90, 200]);
    assert_eq!(stored.meta_reward_commission, vec![None, Some(7)]);
    assert_eq!(stored.meta_reward_commission_bps, vec![None, Some(725)]);
    let offline = source_client("http://127.0.0.1:1", "unavailable");
    let config = json!({"slot":slot,"encoding":"json","maxSupportedTransactionVersion":1});
    let expected = transaction_response(source, None, signature(slot), config.clone()).await;
    let actual = transaction_response(&offline, Some(cache), signature(slot), config).await;
    assert_eq!(actual, expected);
    assert_vat_json(&actual["result"]["meta"]["rewards"]);
    let DiskBlockResult::Found(block) = cache.get_block(slot, TransactionDetails::Full, true).await
    else {
        panic!("cached block missing")
    };
    assert_eq!(block.metadata().rewards_type, stored.meta_reward_type);
    assert_eq!(
        block.metadata().rewards_lamports,
        stored.meta_reward_lamports
    );
    let config = json!({"encoding":"json","maxSupportedTransactionVersion":1});
    let block = block_response(&offline, cache, slot, config).await;
    assert_vat_json(&block["result"]["rewards"]);
}

#[tokio::test]
#[ignore = "requires disposable ClickHouse: DISK_CACHE_TEST_URL"]
async fn agave43_vat_native_fill_and_restart() {
    let (client, source, cfg, cache) = setup().await;
    let database = cfg.database.trim_end_matches("_cache");
    for (slot, spelling) in [(10, "VATDebit"), (11, "validator-admission-ticket-debit")] {
        insert_vat_rewards(&client, database, slot, spelling).await;
    }
    fill(&cache, &source).await;
    for (slot, spelling) in [(10, "VATDebit"), (11, "validator-admission-ticket-debit")] {
        assert_vat_cache(&source, &cache, slot, spelling).await;
    }
    drop(cache);
    let reopened = DiskCache::open(cfg.clone(), &source).await.unwrap();
    for (slot, spelling) in [(10, "VATDebit"), (11, "validator-admission-ticket-debit")] {
        assert_vat_cache(&source, &reopened, slot, spelling).await;
    }
    cleanup(&client, &cfg).await;
}
