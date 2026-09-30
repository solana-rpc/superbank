// SPDX-License-Identifier: AGPL-3.0-only
//! Handler-level regression: cursor and page cache admission share one budget.
use super::*;
use crate::state::AppState;
use serde_json::{Value, json};

pub(super) fn request_state(source: &ClickHouseClient, cache: &DiskCache) -> AppState {
    let mut state = Arc::try_unwrap(crate::tests::test_state()).ok().unwrap();
    state.clickhouse = source.clone();
    state.disk_cache = Some(Arc::new(tokio::sync::OnceCell::new_with(Some(Arc::new(
        cache.clone(),
    )))));
    state
}

async fn signatures_response(state: Arc<AppState>, options: Value) -> Value {
    let response = Box::pin(
        crate::handlers::signatures::handle_get_signatures_for_address(
            state,
            json!("signature-budget"),
            Some(vec![json!(address("address").to_string()), options]),
        ),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}

pub(super) async fn signatures_body(state: Arc<AppState>, options: Value) -> axum::body::Bytes {
    let response = Box::pin(
        crate::handlers::signatures::handle_get_signatures_for_address(
            state,
            json!("signature-race"),
            Some(vec![json!(address("address").to_string()), options]),
        ),
    )
    .await
    .unwrap();
    axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
}

/// The local page races the primary's full page: whichever tier answers, the body
/// matches a primary-only server; a complete local page answers while the primary
/// is blocked or failing, and anything short of that keeps today's error.
async fn assert_handler_race(source: &ClickHouseClient, cache: &DiskCache) {
    let state = Arc::new(request_state(source, cache));
    let mut primary_only = request_state(source, cache);
    primary_only.disk_cache = None;
    let primary_only = Arc::new(primary_only);
    let pages = [
        json!({"limit": 5}),
        json!({"limit": 1000}),
        json!({"before": signature(60).to_string(), "limit": 30}),
        json!({"until": signature(100).to_string(), "limit": 100}),
        json!({"beforeSlot": 56, "untilSlot": 20, "limit": 25}),
        json!({"before": signature(56).to_string(), "until": signature(40).to_string()}),
    ];
    for options in &pages {
        assert_eq!(
            signatures_body(state.clone(), options.clone()).await,
            signatures_body(primary_only.clone(), options.clone()).await,
            "{options}"
        );
    }

    // Primary blocked: the complete local page answers, and the primary read drains.
    let total = source.http_query_sem.available_permits();
    let blocked = source
        .http_query_sem
        .clone()
        .acquire_many_owned(total as u32)
        .await
        .unwrap();
    // `beforeSlot` bounds the page at the local tip (109): without a head cache, a page
    // open above the tip owes rows the source may have ingested since.
    let complete = json!({"beforeSlot": 110, "until": signature(100).to_string(), "limit": 100});
    let response = tokio::time::timeout(
        Duration::from_millis(500),
        signatures_response(state.clone(), complete.clone()),
    )
    .await
    .expect("a complete local page must not wait for the primary");
    let slots: Vec<_> = response["result"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["slot"].as_u64().unwrap())
        .collect();
    assert_eq!(slots, (101..110).rev().collect::<Vec<_>>());
    drop(blocked);
    tokio::time::timeout(Duration::from_secs(5), async {
        while source.http_query_sem.available_permits() != total {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the losing primary read must drain and release its permit");

    // Primary failing: a complete local page still answers; a short one is an error.
    let mut failing = source.clone();
    failing.use_table_names(ClickHouseTableNames::in_database("gsfa_race_missing"));
    let failing = Arc::new(request_state(&failing, cache));
    let response = signatures_response(failing.clone(), complete).await;
    assert!(response.get("error").is_none(), "{response}");
    assert_eq!(response["result"].as_array().unwrap().len(), 9);
    let response = signatures_response(failing, json!({"limit": 1000})).await;
    assert_eq!(response["error"]["code"], -32019, "{response}");
}

/// Source-database queries the query log has seen so far.
async fn source_queries(client: &clickhouse::Client, database: &str) -> u64 {
    execute(client, "SYSTEM FLUSH LOGS").await;
    client
        .query("SELECT count() FROM system.query_log WHERE type='QueryStart' AND has(databases, ?)")
        .bind(database)
        .fetch_one::<u64>()
        .await
        .unwrap()
}

/// GSFA_RACE_PRIMARY=false restores the serial path: every page matches a primary-only
/// server, and a complete local page sends no primary query at all (the race submits one
/// and lets it drain).
async fn assert_handler_serial(
    client: &clickhouse::Client,
    source: &ClickHouseClient,
    cache: &DiskCache,
) {
    let mut cfg = cache.inner.cfg.clone();
    cfg.gsfa_race_primary = false;
    let serial = Box::pin(DiskCache::open(cfg, source)).await.unwrap();
    serial.build_signature_indexes().await;
    serial.build_key_indexes().await;
    let state = Arc::new(request_state(source, &serial));
    let mut primary_only = request_state(source, &serial);
    primary_only.disk_cache = None;
    let primary_only = Arc::new(primary_only);
    for options in [
        json!({"limit": 5}),
        json!({"limit": 1000}),
        json!({"before": signature(60).to_string(), "limit": 30}),
        json!({"until": signature(100).to_string(), "limit": 100}),
        json!({"beforeSlot": 56, "untilSlot": 20, "limit": 25}),
        json!({"before": signature(56).to_string(), "until": signature(40).to_string()}),
    ] {
        assert_eq!(
            signatures_body(state.clone(), options.clone()).await,
            signatures_body(primary_only.clone(), options.clone()).await,
            "{options}"
        );
    }
    let source_database = cache.inner.cfg.database.trim_end_matches("_cache");
    // `beforeSlot` bounds the page at the local tip (109), as in the race test: without a
    // head cache, a page open above the tip owes rows only the source has.
    let complete = json!({"beforeSlot": 110, "until": signature(100).to_string(), "limit": 100});
    let before = source_queries(client, source_database).await;
    let response = signatures_response(state.clone(), complete).await;
    assert_eq!(response["result"].as_array().unwrap().len(), 9);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(source_queries(client, source_database).await, before);
    // The tip-gap rule is not part of the race: the same page open above the tip still
    // asks the source on the serial path.
    let open = json!({"until": signature(100).to_string(), "limit": 100});
    let before = source_queries(client, source_database).await;
    let response = signatures_response(state.clone(), open).await;
    assert_eq!(response["result"].as_array().unwrap().len(), 9);
    assert!(source_queries(client, source_database).await > before);
}

pub(super) async fn assert_handler_budget(state: Arc<AppState>, cache: &DiskCache) {
    // Both cursor signatures exist. The local tier blocks on the first one;
    // primary resolves both bounds and the entire page after the cache expires.
    let permits = cache
        .inner
        .local
        .http_query_sem
        .acquire_many(2)
        .await
        .unwrap();
    let options = json!({
        "before": signature(109).to_string(),
        "until": signature(100).to_string(),
        "limit": 100,
    });
    let response = tokio::time::timeout(
        Duration::from_millis(500),
        signatures_response(state.clone(), options),
    )
    .await
    .expect("both cursors and page must share one 250ms cache budget");
    assert!(response.get("error").is_none(), "{response}");
    let slots: Vec<_> = response["result"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["slot"].as_u64().unwrap())
        .collect();
    assert_eq!(slots, (101..109).rev().collect::<Vec<_>>());
    drop(permits);

    for bound in ["before", "until"] {
        let response = signatures_response(
            state.clone(),
            json!({(bound): signature(999).to_string(), "limit": 100}),
        )
        .await;
        assert_eq!(response["error"]["code"], -32020, "{response}");
    }
}

#[tokio::test]
#[ignore = "requires disposable ClickHouse: DISK_CACHE_TEST_URL=http://127.0.0.1:18195"]
async fn gsfa_handler_cursor_budget_and_missing_bounds() {
    let (client, source, cache) = super::address_latency::setup(Duration::from_millis(250)).await;
    assert_handler_budget(Arc::new(request_state(&source, &cache)), &cache).await;
    #[cfg(feature = "grpc-head-cache")]
    {
        let mut state = request_state(&source, &cache);
        state.head_cache = Some(Arc::new(crate::head_cache::HeadCache::new(32, 1000)));
        assert_handler_budget(Arc::new(state), &cache).await;
    }
    let source_database = cache.inner.cfg.database.trim_end_matches("_cache");
    execute(
        &client,
        &format!("DROP DATABASE {} SYNC", cache.inner.cfg.database),
    )
    .await;
    execute(&client, &format!("DROP DATABASE {source_database} SYNC")).await;
}

#[tokio::test]
#[ignore = "requires disposable ClickHouse: DISK_CACHE_TEST_URL=http://127.0.0.1:18195"]
async fn gsfa_handler_races_local_page_against_primary() {
    let (client, source, cache) = super::address_latency::setup(Duration::from_millis(250)).await;
    assert_handler_race(&source, &cache).await;
    Box::pin(assert_handler_serial(&client, &source, &cache)).await;
    let source_database = cache.inner.cfg.database.trim_end_matches("_cache");
    execute(
        &client,
        &format!("DROP DATABASE {} SYNC", cache.inner.cfg.database),
    )
    .await;
    execute(&client, &format!("DROP DATABASE {source_database} SYNC")).await;
}

async fn address_rpc(state: Arc<AppState>, method: &'static str, options: Value) -> Value {
    let params = Some(vec![json!(address("address").to_string()), options]);
    let response = match method {
        "gsfa" => Box::pin(
            crate::handlers::signatures::handle_get_signatures_for_address(
                state,
                json!("tip-gap"),
                params,
            ),
        )
        .await
        .unwrap(),
        _ => Box::pin(
            crate::handlers::transactions::handle_get_transactions_for_address(
                state,
                json!("tip-gap"),
                params,
            ),
        )
        .await
        .unwrap(),
    };
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}

fn row_slots(response: &Value) -> Vec<u64> {
    let rows = response["result"]
        .get("data")
        .unwrap_or(&response["result"])
        .as_array()
        .unwrap_or_else(|| panic!("{response}"));
    rows.iter()
        .map(|row| row["slot"].as_u64().unwrap())
        .collect()
}

/// With every primary permit held, `local` shapes must answer within 500 ms and
/// `waits` shapes must not; either way the rows equal a primary-only server's.
async fn assert_tip_gap_tiers(
    state: Arc<AppState>,
    primary_only: Arc<AppState>,
    source: &ClickHouseClient,
    local: &[(&'static str, Value)],
    waits: &[(&'static str, Value)],
    refresh: &(dyn Fn() + Sync),
) {
    let total = source.http_query_sem.available_permits();
    for (method, options) in local.iter().chain(waits) {
        refresh();
        let expected = row_slots(&address_rpc(primary_only.clone(), method, options.clone()).await);
        let blocked = source
            .http_query_sem
            .clone()
            .acquire_many_owned(total as u32)
            .await
            .unwrap();
        refresh();
        let answer = tokio::spawn(address_rpc(state.clone(), method, options.clone()));
        let is_local = local.iter().any(|(m, o)| m == method && o == options);
        if is_local {
            let response = tokio::time::timeout(Duration::from_millis(500), answer)
                .await
                .unwrap_or_else(|_| panic!("{method} {options}: local answer expected"))
                .unwrap();
            assert_eq!(row_slots(&response), expected, "{method} {options}");
            drop(blocked);
        } else {
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert!(
                !answer.is_finished(),
                "{method} {options}: a page owing rows above the local tip must wait"
            );
            drop(blocked);
            let response = answer.await.unwrap();
            assert_eq!(row_slots(&response), expected, "{method} {options}");
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while source.http_query_sem.available_permits() != total {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("primary reads drain");
    }
}

/// The source holds slots 10..=114 while the local cache tip is 109 (the forwarder
/// lag). A local page may answer only when the request owes nothing above 109, or a
/// fresh head cache that holds every row of the address from 110 up is merged.
async fn assert_tip_gap(client: &clickhouse::Client, source: &ClickHouseClient, cache: &DiskCache) {
    let source_database = cache.inner.cfg.database.trim_end_matches("_cache");
    execute(client, &format!("INSERT INTO {source_database}.transactions (signature,slot,slot_idx,tx_signatures,tx_account_keys,tx_num_required_signatures,meta_status_ok,meta_pre_balances,meta_post_balances) SELECT toFixedString(concat('sig-',toString(number)),64),number,0,[toFixedString(concat('sig-',toString(number)),64)],[toFixedString('address',32)],1,1,[10000],[10000] FROM numbers(110,5)")).await;
    assert_eq!(cache.tip_span(), Some((10, 109)));
    let state = Arc::new(request_state(source, cache));
    let mut primary_only = request_state(source, cache);
    primary_only.disk_cache = None;
    let primary_only = Arc::new(primary_only);
    let gtfa = |extra: Value| {
        let mut options = json!({"transactionDetails": "signatures", "sortOrder": "desc"});
        options
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        options
    };
    let no_gap = [
        (
            "gsfa",
            json!({"before": signature(60).to_string(), "limit": 30}),
        ),
        ("gsfa", json!({"beforeSlot": 110, "limit": 5})),
        (
            "gtfa",
            gtfa(json!({"limit": 3, "paginationToken": "105:0"})),
        ),
    ];
    let gap = [
        // `until` at or above the local tip; the local page is empty.
        (
            "gsfa",
            json!({"limit": 1, "until": signature(108).to_string()}),
        ),
        ("gsfa", json!({"limit": 3, "untilSlot": 109})),
        ("gsfa", json!({"limit": 5})),
        ("gsfa", json!({"limit": 5, "beforeSlot": 113})),
        // A descending first page starts at the local tip.
        ("gtfa", gtfa(json!({"limit": 8}))),
    ];
    assert_tip_gap_tiers(
        state.clone(),
        primary_only.clone(),
        source,
        &no_gap,
        &gap,
        &|| {},
    )
    .await;
    // A short descending page used to send only the `slot < floor` remainder.
    let options = gtfa(json!({"limit": 1000}));
    assert_eq!(
        row_slots(&address_rpc(state.clone(), "gtfa", options.clone()).await),
        row_slots(&address_rpc(primary_only.clone(), "gtfa", options).await)
    );

    #[cfg(feature = "grpc-head-cache")]
    {
        use crate::head_cache::{
            HeadCache,
            coverage::{HeadCoverage, Link},
        };
        use solana_commitment_config::CommitmentLevel;
        let head_from = |first: u64, cap: usize| {
            let head = Arc::new(HeadCache::new(600, cap));
            for slot in first..115 {
                let mut record = crate::tests::base_transaction_record();
                record.signature = *signature(slot).as_array();
                record.slot = slot;
                head.insert_for_tests(
                    signature(slot),
                    record,
                    0,
                    &[address("address")],
                    CommitmentLevel::Finalized,
                );
            }
            head
        };
        let head_with = |cap: usize| head_from(110, cap);
        // A connected session whose chain proof holds `first..=114` at a fresh tip.
        let fresh = |head: &HeadCache, first: u64| {
            let mut coverage = HeadCoverage::default();
            coverage.connect();
            for slot in first..115 {
                coverage.metadata(Link {
                    slot,
                    hash: [slot as u8; 32],
                    parent: slot - 1,
                    parent_hash: [(slot - 1) as u8; 32],
                });
                coverage.publish(slot, CommitmentLevel::Finalized);
                coverage.observe(slot, CommitmentLevel::Finalized, std::time::Instant::now());
            }
            *head.coverage.write().unwrap() = coverage;
        };
        let with_head = |head: &Arc<HeadCache>| {
            let mut state = request_state(source, cache);
            state.head_cache = Some(head.clone());
            Arc::new(state)
        };

        // A fresh head holding the address from 110 up covers the gap. gTFA uses
        // limit 8 so the head alone (5 rows) cannot answer the first page.
        let covering = head_with(1000);
        // Limits above the head's 5 rows keep the head-only early answer out of the way.
        let covered = [
            (
                "gsfa",
                json!({"limit": 8, "until": signature(108).to_string()}),
            ),
            ("gsfa", json!({"limit": 8})),
            ("gsfa", json!({"limit": 8, "beforeSlot": 113})),
            ("gtfa", gtfa(json!({"limit": 8}))),
        ];
        let refresh = || fresh(&covering, 110);
        assert_tip_gap_tiers(
            with_head(&covering),
            primary_only.clone(),
            source,
            &covered,
            &[],
            &refresh,
        )
        .await;

        // A descending page that reached the floor queries ClickHouse anyway: it sends
        // the full query, not a remainder that would leave a head check stale by then.
        assert_tip_gap_tiers(
            with_head(&covering),
            primary_only.clone(),
            source,
            &[],
            &[("gtfa", gtfa(json!({"limit": 1000})))],
            &refresh,
        )
        .await;

        // A session that started at 112 (a restart or reconnect) holds 112..=114 at a
        // fresh tip; nothing proves it saw 110..=111, so the same shapes wait.
        let late = head_from(112, 1000);
        let refresh = || fresh(&late, 112);
        assert_tip_gap_tiers(
            with_head(&late),
            primary_only.clone(),
            source,
            &[],
            &covered,
            &refresh,
        )
        .await;

        // Disconnected head: the same shapes wait for the primary.
        let disconnected = head_with(1000);
        assert_tip_gap_tiers(
            with_head(&disconnected),
            primary_only.clone(),
            source,
            &[],
            &covered,
            &|| {},
        )
        .await;

        // Per-address cap: keys of 110..=112 were dropped, so 110 is not provably held.
        let capped = head_with(2);
        let refresh = || fresh(&capped, 110);
        assert_tip_gap_tiers(
            with_head(&capped),
            primary_only.clone(),
            source,
            &[],
            &[
                (
                    "gsfa",
                    json!({"limit": 8, "until": signature(108).to_string()}),
                ),
                ("gsfa", json!({"limit": 8, "beforeSlot": 113})),
                ("gtfa", gtfa(json!({"limit": 8}))),
            ],
            &refresh,
        )
        .await;
    }
}

#[tokio::test]
#[ignore = "requires disposable ClickHouse: DISK_CACHE_TEST_URL=http://127.0.0.1:18195"]
async fn address_pages_never_skip_rows_above_the_local_tip() {
    let (client, source, cache) = super::address_latency::setup(Duration::from_secs(5)).await;
    assert_tip_gap(&client, &source, &cache).await;
    let source_database = cache.inner.cfg.database.trim_end_matches("_cache");
    execute(
        &client,
        &format!("DROP DATABASE {} SYNC", cache.inner.cfg.database),
    )
    .await;
    execute(&client, &format!("DROP DATABASE {source_database} SYNC")).await;
}
