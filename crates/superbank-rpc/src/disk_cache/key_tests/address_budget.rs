// SPDX-License-Identifier: AGPL-3.0-only
//! Handler-level regression: cursor and page cache admission share one budget.
use super::*;
use crate::state::AppState;
use serde_json::{Value, json};

fn request_state(source: &ClickHouseClient, cache: &DiskCache) -> AppState {
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

async fn assert_handler_budget(state: Arc<AppState>, cache: &DiskCache) {
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
