// SPDX-License-Identifier: AGPL-3.0-only
//! Local-only regressions for address budgets, bounds and exact hydration.
use super::*;
use std::str::FromStr;

pub(super) async fn setup(
    address_timeout: Duration,
) -> (clickhouse::Client, ClickHouseClient, DiskCache) {
    setup_with(address_timeout, |_| {}).await
}

pub(super) async fn setup_with(
    address_timeout: Duration,
    adjust: impl FnOnce(&mut DiskCacheConfig),
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
    adjust(&mut cfg);
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

/// Page through the whole address from ClickHouse, returning every row and token.
async fn address_pages(
    source: &ClickHouseClient,
    mut options: serde_json::Value,
    token_transform: impl Fn(&str) -> String,
) -> (Vec<serde_json::Value>, Vec<String>) {
    let (mut rows, mut tokens) = (Vec::new(), Vec::new());
    for _ in 0..64 {
        let response = address_response(source, None, options.clone()).await;
        assert!(response.get("error").is_none(), "{response}");
        rows.extend(
            response["result"]["data"]
                .as_array()
                .unwrap()
                .iter()
                .cloned(),
        );
        let Some(token) = response["result"]["paginationToken"].as_str() else {
            return (rows, tokens);
        };
        tokens.push(token.to_string());
        options["paginationToken"] = serde_json::json!(token_transform(token));
    }
    panic!("pagination did not terminate");
}

async fn assert_position_token_pagination(source: &ClickHouseClient) {
    let mut positioned = source.clone();
    positioned.set_transactions_for_address_position_tokens(true);
    let is_position = |token: &str| {
        token
            .split_once(':')
            .is_some_and(|(slot, idx)| slot.parse::<u64>().is_ok() && idx.parse::<u32>().is_ok())
    };
    for details in ["signatures", "full"] {
        for order in ["asc", "desc"] {
            // Limit 7 lands page boundaries inside slot 55 (21 rows).
            let options = serde_json::json!({"transactionDetails":details, "sortOrder":order, "limit":7, "maxSupportedTransactionVersion":0});
            let (expected, signature_tokens) =
                address_pages(source, options.clone(), str::to_string).await;
            assert_eq!(expected.len(), 120, "{details} {order}");
            assert!(signature_tokens.iter().all(|t| !is_position(t)));
            let (actual, position_tokens) =
                address_pages(&positioned, options.clone(), str::to_string).await;
            assert_eq!(actual, expected, "{details} {order}");
            assert_eq!(position_tokens.len(), signature_tokens.len());
            assert!(
                position_tokens.iter().all(|t| is_position(t)),
                "{position_tokens:?}"
            );
            // Old signature tokens stay valid on a position-token server.
            let mut first = options.clone();
            first["paginationToken"] = serde_json::json!(signature_tokens[0]);
            let old_token_page = address_response(&positioned, None, first).await;
            let mut second = options;
            second["paginationToken"] = serde_json::json!(position_tokens[0]);
            let new_token_page = address_response(&positioned, None, second).await;
            assert_eq!(
                old_token_page["result"]["data"],
                new_token_page["result"]["data"]
            );
            assert_eq!(
                old_token_page["result"]["paginationToken"],
                new_token_page["result"]["paginationToken"]
            );
        }
    }
}

async fn assert_cursor_cache(
    client: &clickhouse::Client,
    source: &ClickHouseClient,
    cache: &DiskCache,
) {
    let mut cursors = source.clone();
    cursors.set_transactions_for_address_cursor_cache(true);
    for details in ["signatures", "full"] {
        for order in ["asc", "desc"] {
            let options = serde_json::json!({"transactionDetails":details, "sortOrder":order, "limit":7, "maxSupportedTransactionVersion":0});
            let expected = address_pages(source, options.clone(), str::to_string).await;
            // Tokens stay signatures; only the next page's lookup changes.
            assert_eq!(
                address_pages(&cursors, options, str::to_string).await,
                expected
            );
        }
    }
    // A primed cursor needs no primary lookup: the page succeeds with `signatures` gone.
    // Limit 5 ends at a signature no earlier step resolved, so the shared
    // signature-slot cache cannot answer for either client.
    let options =
        serde_json::json!({"transactionDetails":"signatures", "sortOrder":"desc", "limit":5});
    let first = address_response(&cursors, None, options.clone()).await;
    let token = first["result"]["paginationToken"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(token, signature(105).to_string());
    let mut next = options;
    next["paginationToken"] = serde_json::json!(token);
    let database = cache.inner.cfg.database.trim_end_matches("_cache");
    execute(
        client,
        &format!("RENAME TABLE {database}.signatures TO {database}.signatures_unavailable"),
    )
    .await;
    let cached = address_response(&cursors, None, next.clone()).await;
    let uncached = address_response(source, None, next.clone()).await;
    // Filter bounds never use the cursor cache: the same remembered signature
    // as a filter (alone, or sharing one lookup with the cursor) still needs
    // `signatures`. Checked inside the rename window, before any successful
    // lookup could put this signature in the shared signature-slot cache.
    let mut as_filter = next.clone();
    as_filter.as_object_mut().unwrap().remove("paginationToken");
    as_filter["filters"] = serde_json::json!({"signature":{"lt":token}});
    let mut as_both = next.clone();
    as_both["filters"] = serde_json::json!({"signature":{"lt":token}});
    let filtered = address_response(&cursors, None, as_filter).await;
    let shared = address_response(&cursors, None, as_both).await;
    execute(
        client,
        &format!("RENAME TABLE {database}.signatures_unavailable TO {database}.signatures"),
    )
    .await;
    let expected = address_response(source, None, next).await;
    assert!(expected.get("error").is_none(), "{expected}");
    assert_eq!(cached["result"], expected["result"], "{cached}");
    assert_eq!(uncached["error"]["code"], -32603, "{uncached}");
    assert_eq!(filtered["error"]["code"], -32603, "{filtered}");
    assert_eq!(shared["error"]["code"], -32603, "{shared}");
}

/// Seed `token_owner_activity` for the address: 1-3 token accounts on every
/// gsfa transaction (same row data, so the dedup winner is irrelevant) and
/// token-only transactions at positions no gsfa row holds, some failed, with
/// mixed `balance_changed`. Only tokenAccounts queries read this table.
async fn seed_token_owner_activity(client: &clickhouse::Client, database: &str) {
    let owner = format!(
        "CAST(base58Decode('{}') AS FixedString(32))",
        address("address")
    );
    execute(client, &format!("INSERT INTO {database}.token_owner_activity (owner, token_account, signature, slot, slot_idx, memo, err, block_time, balance_changed) SELECT address, toFixedString(concat('acct-', toString(k)), 32), signature, slot, slot_idx, memo, err, block_time, (slot + k) % 2 FROM {database}.gsfa ARRAY JOIN range(1 + (slot + slot_idx) % 3) AS k WHERE address = {owner}")).await;
    // One per third slot at idx 1 (slot 55's idx 1 is a gsfa row), plus three
    // in the slot-55 group at idx 21-23.
    execute(client, &format!("INSERT INTO {database}.token_owner_activity (owner, token_account, signature, slot, slot_idx, memo, err, block_time, balance_changed) SELECT {owner}, toFixedString(concat('acct-', toString(k)), 32), toFixedString(concat('tok-', toString(slot), '-', toString(slot_idx)), 64), slot, slot_idx, NULL, if(slot % 4 = 0, 'failed', NULL), NULL, (slot + k) % 2 FROM (SELECT arrayJoin(arrayConcat(arrayMap(n -> (toUInt64(n), toUInt32(1)), arrayFilter(n -> n != 55, range(10, 110, 3))), [(toUInt64(55), toUInt32(21)), (toUInt64(55), toUInt32(22)), (toUInt64(55), toUInt32(23))])) AS position, position.1 AS slot, position.2 AS slot_idx) ARRAY JOIN range(1 + slot % 3) AS k")).await;
    // Page cursors on token-only rows resolve through `signatures`, as with the real schema.
    execute(client, &format!("INSERT INTO {database}.signatures (signature, slot, slot_idx, err) SELECT DISTINCT signature, slot, slot_idx, err FROM {database}.token_owner_activity WHERE startsWith(signature, 'tok-')")).await;
}

/// The expected unique rows of a tokenAccounts request, without any LIMIT:
/// the old-shape union, sorted by the request order and deduplicated by
/// signature in Rust, independently of the query builder.
async fn token_union_oracle(
    client: &clickhouse::Client,
    database: &str,
    balance_changed: bool,
    filter: &str,
    descending: bool,
) -> Vec<(u64, u32, String)> {
    let owner = format!(
        "CAST(base58Decode('{}') AS FixedString(32))",
        address("address")
    );
    let balance = if balance_changed {
        " AND balance_changed = 1"
    } else {
        ""
    };
    let rows = client
        .query(&format!("SELECT concat(toString(slot), ':', toString(slot_idx), ':', base58Encode(signature)) FROM (SELECT signature, slot, slot_idx, err FROM {database}.gsfa WHERE address = {owner} UNION ALL SELECT signature, slot, slot_idx, err FROM {database}.token_owner_activity WHERE owner = {owner}{balance}) WHERE {filter}"))
        .fetch_all::<String>()
        .await
        .unwrap();
    let mut rows: Vec<(u64, u32, String)> = rows
        .iter()
        .map(|row| {
            let mut parts = row.splitn(3, ':');
            (
                parts.next().unwrap().parse().unwrap(),
                parts.next().unwrap().parse().unwrap(),
                parts.next().unwrap().to_string(),
            )
        })
        .collect();
    rows.sort();
    if descending {
        rows.reverse();
    }
    let mut seen = std::collections::HashSet::new();
    rows.retain(|row| seen.insert(row.2.clone()));
    rows
}

/// The token-accounts UNION pushes filter + ORDER BY + LIMIT into each branch;
/// paging with a small limit (internal batches of 64 rows, boundaries inside
/// the slot-55 group, cross-table duplicates) must return exactly the oracle's
/// rows and tokens.
async fn assert_token_union_pagination(
    client: &clickhouse::Client,
    source: &ClickHouseClient,
    cache: &DiskCache,
) {
    let database = cache.inner.cfg.database.trim_end_matches("_cache");
    seed_token_owner_activity(client, database).await;
    let token_rows = client
        .query(&format!(
            "SELECT count() FROM {database}.token_owner_activity WHERE owner = CAST(base58Decode('{}') AS FixedString(32))",
            address("address")
        ))
        .fetch_one::<u64>()
        .await
        .unwrap();
    assert!(
        token_rows > 200,
        "token branch must not be empty: {token_rows}"
    );
    let limit = 7;
    for (filters, sql_filter) in [
        (serde_json::json!({}), "1"),
        (
            serde_json::json!({"slot":{"gte":20,"lt":100}, "status":"succeeded"}),
            "slot >= 20 AND slot < 100 AND err IS NULL",
        ),
        (serde_json::json!({"status":"failed"}), "err IS NOT NULL"),
    ] {
        for order in ["asc", "desc"] {
            let base = serde_json::json!({"transactionDetails":"signatures", "sortOrder":order, "limit":limit, "filters":filters});
            let (plain, _) = address_pages(source, base.clone(), str::to_string).await;
            for token_accounts in ["all", "balanceChanged"] {
                let label = format!("{order} {token_accounts} {filters}");
                let expected = token_union_oracle(
                    client,
                    database,
                    token_accounts == "balanceChanged",
                    sql_filter,
                    order == "desc",
                )
                .await;
                assert!(expected.len() > plain.len(), "token-only rows: {label}");
                let mut options = base.clone();
                options["filters"]["tokenAccounts"] = serde_json::json!(token_accounts);
                let (rows, tokens) = address_pages(source, options, str::to_string).await;
                let actual: Vec<_> = rows
                    .iter()
                    .map(|row| {
                        (
                            row["slot"].as_u64().unwrap(),
                            row["signature"].as_str().unwrap().to_string(),
                        )
                    })
                    .collect();
                let wanted: Vec<_> = expected
                    .iter()
                    .map(|(slot, _, signature)| (*slot, signature.clone()))
                    .collect();
                assert_eq!(actual, wanted, "{label}");
                // Every non-empty page names its last row.
                assert_eq!(tokens.len(), expected.len().div_ceil(limit), "{label}");
                for (page, token) in tokens.iter().enumerate() {
                    let last = ((page + 1) * limit).min(expected.len()) - 1;
                    assert_eq!(token, &expected[last].2, "{label} page {page}");
                }
            }
        }
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
    assert_position_token_pagination(&source).await;
    assert_cursor_cache(&client, &source, &cache).await;
    // After the cursor-cache check: its many page lookups fill the shared
    // signature-slot cache, which that check needs cold.
    assert_token_union_pagination(&client, &source, &cache).await;
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
