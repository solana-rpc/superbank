// SPDX-License-Identifier: AGPL-3.0-only
use super::*;
use axum::{Router, extract::Query};
use std::collections::HashMap;

const ADDRESS: &str = "So11111111111111111111111111111111111111112";

/// A ClickHouse stand-in that records each query and answers with no rows.
async fn recording_state() -> (Arc<AppState>, Arc<Mutex<Vec<String>>>) {
    let queries = Arc::new(Mutex::new(Vec::new()));
    let observed = queries.clone();
    let app = Router::new().fallback(
        move |Query(params): Query<HashMap<String, String>>, body: Bytes| {
            let queries = observed.clone();
            async move {
                let sql = if body.is_empty() {
                    params.get("query").cloned().unwrap_or_default()
                } else {
                    String::from_utf8(body.to_vec()).unwrap()
                };
                if let Some(reply) = super::latest_slot::cancellation_response(&sql, false) {
                    return (StatusCode::OK, reply);
                }
                queries.lock().unwrap().push(sql);
                (StatusCode::OK, Vec::new())
            }
        },
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut state = super::test_state_with_clickhouse_url(&url);
    let client = &mut Arc::get_mut(&mut state).unwrap().clickhouse;
    client.set_http_client_for_tests(
        client
            .client
            .clone()
            .with_compression(clickhouse::Compression::None)
            .with_validation(false),
    );
    // The test blocks sit at low slots, so no floor unless a test sets one.
    client.set_transactions_for_address_block_time_slot_bounds_min_slot(0);
    client.initialize_read_cancellation().await.unwrap();
    (state, queries)
}

/// Blocks at slots 1_000..=1_999 in one covered range, one second apart from 50_000.
fn with_block_index(state: &mut Arc<AppState>) {
    let blocks = (1_000..=1_999)
        .map(|slot| (slot, 49_000 + slot as i64))
        .collect::<Vec<_>>();
    let index =
        crate::disk_cache::block_index::BlockIndex::for_block_time_tests(1_000, 1_999, &blocks);
    let disk =
        crate::disk_cache::DiskCache::for_range_tests(state.clickhouse.clone(), Some(index), &[]);
    Arc::get_mut(state).unwrap().disk_cache = Some(Arc::new(tokio::sync::OnceCell::new_with(
        Some(Arc::new(disk)),
    )));
}

async fn address_queries(state: Arc<AppState>, queries: &Mutex<Vec<String>>, block_time: Value) {
    let request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransactionsForAddress",
        "params": [ADDRESS, {
            "transactionDetails": "signatures",
            "sortOrder": "asc",
            "filters": {"blockTime": block_time},
        }],
    });
    let response = parse_json_rpc_response(handle_json_rpc_value(state, &request).await).await;
    assert!(response.error.is_none(), "{:?}", response.error);
    queries
        .lock()
        .unwrap()
        .retain(|sql| sql.contains("FROM default.gsfa"));
}

fn single_address_query(queries: &Mutex<Vec<String>>) -> String {
    let queries = queries.lock().unwrap();
    assert_eq!(queries.len(), 1, "{queries:?}");
    queries[0].split_whitespace().collect::<Vec<_>>().join(" ")
}

#[tokio::test]
async fn block_time_filter_adds_the_block_index_slot_range_with_margin() {
    let (mut state, queries) = recording_state().await;
    with_block_index(&mut state);
    address_queries(state, &queries, json!({"gte": 50_200, "lt": 50_300})).await;
    // Blocks 1_200..=1_299 hold the window; 1_500 slots of margin pad each side.
    assert!(
        single_address_query(&queries)
            .contains("block_time >= 50200 AND block_time < 50300 AND slot >= 0 AND slot <= 2799"),
        "{queries:?}"
    );
}

#[tokio::test]
async fn block_time_filter_past_the_index_keeps_only_the_lower_slot_bound() {
    let (mut state, queries) = recording_state().await;
    with_block_index(&mut state);
    address_queries(state, &queries, json!({"gte": 50_900, "lte": 60_000})).await;
    let sql = single_address_query(&queries);
    assert!(
        sql.contains("block_time >= 50900 AND block_time <= 60000 AND slot >= 400"),
        "{sql}"
    );
    assert!(!sql.contains("slot <="), "{sql}");
}

#[tokio::test]
async fn block_time_filter_without_index_or_when_disabled_leaves_slots_unbounded() {
    let (state, queries) = recording_state().await;
    address_queries(state, &queries, json!({"gte": 50_200, "lt": 50_300})).await;
    assert!(!single_address_query(&queries).contains("slot >="));

    let (mut state, queries) = recording_state().await;
    Arc::get_mut(&mut state)
        .unwrap()
        .clickhouse
        .set_transactions_for_address_block_time_slot_bounds(false);
    with_block_index(&mut state);
    address_queries(state, &queries, json!({"gte": 50_200, "lt": 50_300})).await;
    assert!(!single_address_query(&queries).contains("slot >="));
}

#[tokio::test]
async fn block_time_edge_below_the_floor_leaves_that_side_unbounded() {
    let (mut state, queries) = recording_state().await;
    Arc::get_mut(&mut state)
        .unwrap()
        .clickhouse
        .set_transactions_for_address_block_time_slot_bounds_min_slot(1_250);
    with_block_index(&mut state);
    address_queries(state, &queries, json!({"gte": 50_200, "lt": 50_300})).await;
    // The edge before the window is block 1_199, below the floor; the edge after it is 1_300.
    let sql = single_address_query(&queries);
    assert!(
        sql.contains("block_time >= 50200 AND block_time < 50300 AND slot <= 2799"),
        "{sql}"
    );
    assert!(!sql.contains("slot >="), "{sql}");
}
