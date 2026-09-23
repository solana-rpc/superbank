// SPDX-License-Identifier: AGPL-3.0-only
//! Local-only regressions for address budgets, bounds and exact hydration.
use super::*;
use std::str::FromStr;

pub(super) async fn setup(
    address_timeout: Duration,
) -> (clickhouse::Client, ClickHouseClient, DiskCache) {
    let url = std::env::var("DISK_CACHE_TEST_URL").expect("explicit disposable ClickHouse URL");
    assert!(url.starts_with("http://127.0.0.1:"));
    let database = format!("test_address_latency_{}", now_version());
    let client = clickhouse::Client::default()
        .with_url(&url)
        .with_user("default");
    fixture(&client, &database).await;
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
    let mut cfg = config(url, format!("{database}_cache"));
    cfg.retain_slots = 100;
    // Functional assertions need room for debug-build hydration. Budget assertions
    // pass an explicit short deadline independently of machine speed.
    cfg.address_query_timeout = address_timeout;
    let cache = DiskCache::open(cfg, &source).await.unwrap();
    for db in [&database, &cache.inner.cfg.database] {
        execute(&client, &format!("INSERT INTO {db}.transactions (signature,slot,slot_idx,tx_signatures,tx_account_keys,tx_num_required_signatures,meta_status_ok,meta_pre_balances,meta_post_balances,tx_version) SELECT toFixedString(concat('sig-',toString(number)),64),number,0,[toFixedString(concat('sig-',toString(number)),64)],[toFixedString('address',32)],1,1,[10000],[10000],if(number=109,toNullable(toUInt8(0)),NULL) FROM numbers(10,100)")).await;
        execute(&client, &format!("INSERT INTO {db}.transactions (signature,slot,slot_idx,tx_signatures,tx_account_keys,tx_num_required_signatures,meta_status_ok) SELECT toFixedString(concat('same-',toString(number)),64),55,number,[toFixedString(concat('same-',toString(number)),64)],[toFixedString('address',32)],1,1 FROM numbers(1,20)")).await;
        execute(&client, &format!("INSERT INTO {db}.blocks_metadata (slot,parent_slot,blockhash,parent_blockhash,block_height,executed_transaction_count) SELECT number,number-1,toFixedString('hash',32),toFixedString('hash',32),number,1 FROM numbers(50,60)")).await;
    }
    cache
        .publish_range_coverage(
            (10..110)
                .map(|slot| {
                    (
                        slot,
                        SlotStatus::Covered {
                            tx_count: if slot == 55 { 21 } else { 1 },
                        },
                    )
                })
                .collect(),
        )
        .await
        .unwrap();
    cache.build_signature_indexes().await;
    cache.build_key_indexes().await;
    (client, source, cache)
}

fn positions() -> Vec<(u64, u32, String)> {
    (10..110)
        .map(|slot| (slot, 0, signature(slot).to_string()))
        .collect()
}

async fn assert_exact_batches(source: &ClickHouseClient, cache: &DiskCache) {
    let mut requested = positions();
    // Wrong historical index and a duplicate must not lose or reorder a row.
    requested[30].1 = 999;
    requested.push(requested[0].clone());
    requested.push((55, 0, signature(999).to_string()));
    let found = cache
        .get_txs_by_position(&requested, cache.address_request_deadline())
        .await;
    assert_eq!(found.len(), requested.len());
    for (expected, actual) in requested[..101].iter().zip(&found) {
        let actual = actual.as_ref().unwrap();
        assert_eq!(
            (actual.slot, actual.signature),
            (
                expected.0,
                *Signature::from_str(&expected.2).unwrap().as_array()
            )
        );
    }
    assert!(found[101].is_none());
    let (primary, _) = source
        .get_transactions_by_positions(&requested)
        .await
        .unwrap();
    assert_eq!(primary.len(), 100);
    assert!(
        primary
            .iter()
            .any(|row| row.slot == 109 && row.tx_version == Some(0))
    );
    let same: Vec<_> = (1..21)
        .map(|idx| (55, idx as u32, named_signature("same", idx).to_string()))
        .collect();
    let found = cache
        .get_txs_by_position(&same, cache.address_request_deadline())
        .await;
    assert_eq!(found.iter().filter(|row| row.is_some()).count(), 20);
    assert_eq!(
        source
            .get_transactions_by_positions(&same)
            .await
            .unwrap()
            .0
            .len(),
        20
    );
}

async fn address_response(
    source: &ClickHouseClient,
    cache: Option<&DiskCache>,
    options: serde_json::Value,
) -> serde_json::Value {
    let mut state = Arc::try_unwrap(crate::tests::test_state()).ok().unwrap();
    state.clickhouse = source.clone();
    state.disk_cache = cache.map(|cache| {
        Arc::new(tokio::sync::OnceCell::new_with(Some(Arc::new(
            cache.clone(),
        ))))
    });
    let response = Box::pin(
        crate::handlers::transactions::handle_get_transactions_for_address(
            Arc::new(state),
            serde_json::json!("address-regression"),
            Some(vec![
                serde_json::json!(address("address").to_string()),
                options,
            ]),
        ),
    )
    .await
    .unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}

async fn assert_handler_parity(source: &ClickHouseClient, cache: &DiskCache) {
    for encoding in ["json", "jsonParsed", "base58", "base64"] {
        for order in ["asc", "desc"] {
            let options = serde_json::json!({"transactionDetails":"full", "encoding":encoding, "sortOrder":order, "limit":100, "maxSupportedTransactionVersion":0});
            let expected = address_response(source, None, options.clone()).await;
            assert!(expected.get("error").is_none(), "{expected}");
            let actual = address_response(source, Some(cache), options).await;
            assert!(actual.get("error").is_none(), "{actual}");
            assert_eq!(actual["result"]["data"], expected["result"]["data"]);
            assert_eq!(actual["result"]["data"].as_array().unwrap().len(), 100);
        }
    }
    let options = serde_json::json!({"transactionDetails":"full", "sortOrder":"desc", "limit":10});
    for tier in [None, Some(cache)] {
        let response = address_response(source, tier, options.clone()).await;
        assert_eq!(response["error"]["code"], -32015, "{response}");
    }
}

async fn assert_resolved_bounds(source: &ClickHouseClient, cache: &DiskCache) {
    for order in ["asc", "desc"] {
        let bound = named_signature("same", 10).to_string();
        let options = serde_json::json!({"transactionDetails":"signatures", "sortOrder":order, "limit":100, "filters":{"signature":{"gte":bound,"lte":bound}}});
        let expected = address_response(source, None, options.clone()).await;
        assert!(expected.get("error").is_none(), "{expected}");
        assert_eq!(
            expected["result"]["data"].as_array().unwrap().len(),
            1,
            "{expected}"
        );
        let actual = address_response(source, Some(cache), options).await;
        assert_eq!(actual["result"]["data"], expected["result"]["data"]);
    }
}

async fn assert_missing_and_failed_bounds(
    client: &clickhouse::Client,
    source: &ClickHouseClient,
    cache: &DiskCache,
) {
    let missing = signature(998).to_string();
    let options = serde_json::json!({"transactionDetails":"signatures", "sortOrder":"desc", "limit":5, "paginationToken":missing, "filters":{"signature":{"gte":missing,"gt":missing,"lte":missing,"lt":missing}}});
    for tier in [None, Some(cache)] {
        let response = address_response(source, tier, options.clone()).await;
        assert!(response.get("error").is_none(), "{response}");
        let slots: Vec<_> = response["result"]["data"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["slot"].as_u64().unwrap())
            .collect();
        assert_eq!(slots, [109, 108, 107, 106, 105]);
    }
    let database = cache.inner.cfg.database.trim_end_matches("_cache");
    execute(
        client,
        &format!("RENAME TABLE {database}.signatures TO {database}.signatures_unavailable"),
    )
    .await;
    let response = address_response(source, None, serde_json::json!({"transactionDetails":"signatures", "paginationToken":signature(997).to_string()})).await;
    execute(
        client,
        &format!("RENAME TABLE {database}.signatures_unavailable TO {database}.signatures"),
    )
    .await;
    assert_eq!(
        response["error"]["code"], -32603,
        "lookup failure must not become unbounded: {response}"
    );
}

async fn assert_unknown_pages(cache: &DiskCache) {
    drop(cache.inner.key_index.mutation(10, 109));
    let recent = cache
        .signatures_for_address_until(
            address("address"),
            None,
            None,
            3,
            cache.address_request_deadline(),
        )
        .await
        .unwrap();
    assert_eq!(
        recent
            .records
            .iter()
            .map(|row| row.slot)
            .collect::<Vec<_>>(),
        [109, 108, 107]
    );
    // More than the unknown probe limit: never return the incomplete prefix.
    assert!(
        cache
            .signatures_for_address_until(
                address("address"),
                None,
                None,
                100,
                cache.address_request_deadline()
            )
            .await
            .is_none()
    );
    let mut query = tfa(SortOrder::Desc, TokenAccountsFilter::None);
    query.limit = 100;
    assert!(
        cache
            .transactions_for_address_until(
                address("address"),
                query,
                cache.address_request_deadline()
            )
            .await
            .is_none()
    );
    cache.build_signature_indexes().await;
    cache.build_key_indexes().await;
    // Both edge partitions are intentionally unknown under partial coverage.
    cache.inner.coverage.write().unwrap().remove_below(15);
    drop(cache.inner.key_index.mutation(10, 19));
    let page = cache
        .signatures_for_address_until(
            address("address"),
            None,
            None,
            200,
            cache.address_request_deadline(),
        )
        .await
        .unwrap();
    assert_eq!(page.records.last().unwrap().slot, 15);
    assert!(page.reached_floor);
}

async fn assert_hydration_invalidation(cache: &DiskCache) {
    let permits = cache
        .inner
        .local
        .http_query_sem
        .acquire_many(2)
        .await
        .unwrap();
    let requested = [(45, 0, signature(45).to_string())];
    let read = cache.get_txs_by_position(&requested, cache.address_request_deadline());
    tokio::pin!(read);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut read)
            .await
            .is_err()
    );
    cache.inner.key_index.invalidate_reads();
    drop(permits);
    assert!(
        read.await[0].is_none(),
        "repair invalidation must reject fetched payload"
    );
}

async fn assert_shared_deadline(cache: &DiskCache) {
    let _permits = cache
        .inner
        .local
        .http_query_sem
        .acquire_many(2)
        .await
        .unwrap();
    let start = tokio::time::Instant::now();
    let deadline = start + Duration::from_millis(75);
    assert!(
        cache
            .signature_position_until(signature(109), deadline)
            .await
            .is_none()
    );
    assert!(
        cache
            .signature_position_until(signature(100), deadline)
            .await
            .is_none()
    );
    assert!(
        cache
            .signatures_for_address_until(address("address"), None, None, 100, deadline)
            .await
            .is_none()
    );
    assert!(
        cache
            .get_txs_by_position(&positions(), deadline)
            .await
            .iter()
            .all(Option::is_none)
    );
    assert!(
        start.elapsed() < Duration::from_millis(150),
        "each stage must share one 75ms budget: {:?}",
        start.elapsed()
    );
}

async fn assert_eviction_fallback(
    client: &clickhouse::Client,
    source: &ClickHouseClient,
    cache: &DiskCache,
) {
    execute(
        client,
        &format!(
            "ALTER TABLE {}.transactions DELETE WHERE slot=109 SETTINGS mutations_sync=2",
            cache.inner.cfg.database
        ),
    )
    .await;
    assert!(
        cache
            .get_txs_by_position(
                &[(109, 0, signature(109).to_string())],
                cache.address_request_deadline()
            )
            .await[0]
            .is_none()
    );
    let options = serde_json::json!({"transactionDetails":"full", "sortOrder":"desc", "limit":10, "maxSupportedTransactionVersion":0});
    let expected = address_response(source, None, options.clone()).await;
    let actual = address_response(source, Some(cache), options).await;
    assert_eq!(actual["result"]["data"], expected["result"]["data"]);
    assert_eq!(actual["result"]["data"].as_array().unwrap().len(), 10);
}

async fn assert_hydration_query_shapes(client: &clickhouse::Client, cache: &DiskCache) {
    execute(client, "SYSTEM FLUSH LOGS").await;
    let database = cache.inner.cfg.database.trim_end_matches("_cache");
    let queries = client.query(&format!("SELECT query FROM system.query_log WHERE type='QueryFinish' AND (has(databases,'{database}') OR has(databases,'{database}_cache')) AND query LIKE '%PREWHERE%' AND query LIKE '%slot, %signature) IN%'"))
        .fetch_all::<String>().await.unwrap();
    let exact: Vec<_> = queries
        .iter()
        .filter(|query| query.contains("(slot, slot_idx, signature) IN"))
        .collect();
    assert!(
        exact
            .iter()
            .any(|query| query.matches("toFixedString(unhex(").count() == 100),
        "100 slots must share a batch"
    );
    let fallback: Vec<_> = queries
        .iter()
        .filter(|query| query.contains("(slot, signature) IN"))
        .collect();
    assert!(!fallback.is_empty(), "historical mismatch must retry");
    for query in fallback {
        assert_eq!(
            query.matches("toFixedString(unhex(").count(),
            1,
            "retry only the unresolved identity: {query}"
        );
    }
}

#[tokio::test]
#[ignore = "requires disposable ClickHouse: DISK_CACHE_TEST_URL=http://127.0.0.1:18195"]
async fn address_latency_clickhouse_integration() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("error")
        .try_init();
    let (client, source, cache) = setup(Duration::from_secs(2)).await;
    assert_exact_batches(&source, &cache).await;
    assert_handler_parity(&source, &cache).await;
    assert_resolved_bounds(&source, &cache).await;
    assert_missing_and_failed_bounds(&client, &source, &cache).await;
    assert_shared_deadline(&cache).await;
    assert_hydration_invalidation(&cache).await;
    assert_unknown_pages(&cache).await;
    assert_eviction_fallback(&client, &source, &cache).await;
    assert_hydration_query_shapes(&client, &cache).await;
    let source_database = cache.inner.cfg.database.trim_end_matches("_cache");
    if std::env::var_os("DISK_CACHE_TEST_KEEP").is_some() {
        eprintln!(
            "Kept source database {source_database} and cache {}",
            cache.inner.cfg.database
        );
        return;
    }
    execute(
        &client,
        &format!("DROP DATABASE {} SYNC", cache.inner.cfg.database),
    )
    .await;
    execute(&client, &format!("DROP DATABASE {source_database} SYNC")).await;
}
