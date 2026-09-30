// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::collections::HashMap;

use axum::{Router, body::Bytes, extract::Query, http::StatusCode};

use super::test_state_with_clickhouse_url;
use crate::clickhouse::QueryCacheConfig;
use crate::processing::ProcessingResult;

async fn latest_slot_response(status: StatusCode, body: Vec<u8>) -> ProcessingResult<Option<u64>> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ClickHouse test listener");
    let url = format!(
        "http://{}",
        listener.local_addr().expect("listener address")
    );
    let (queries_tx, mut queries_rx) = tokio::sync::mpsc::unbounded_channel();
    let app = Router::new().fallback(
        move |Query(params): Query<HashMap<String, String>>, query: Bytes| {
            let body = body.clone();
            let queries_tx = queries_tx.clone();
            async move { fixture_response(params, query, status, body, queries_tx) }
        },
    );
    let server = tokio::spawn(async move { axum::serve(listener, app).await });

    let mut client = test_state_with_clickhouse_url(&url).clickhouse.clone();
    client.set_http_client_for_tests(
        client
            .client
            .clone()
            .with_compression(clickhouse::Compression::None),
    );
    client
        .initialize_read_cancellation()
        .await
        .expect("cancellation preflight");
    // Enable the global result cache so this test detects a loss of tip freshness.
    client.query_cache = QueryCacheConfig::new(true, 60, false, true);
    let result = client.get_latest_finalized_slot_since(None).await;
    server.abort();

    let query = queries_rx.try_recv().expect("latest-slot query was sent");
    assert!(query.contains("SELECT slot FROM default.blocks_metadata ORDER BY slot DESC LIMIT 1"));
    assert!(!query.contains("use_query_cache=1"));
    result
}

fn fixture_response(
    params: HashMap<String, String>,
    query: Bytes,
    status: StatusCode,
    body: Vec<u8>,
    queries_tx: tokio::sync::mpsc::UnboundedSender<String>,
) -> (StatusCode, Vec<u8>) {
    let sql = if query.is_empty() {
        params.get("query").cloned().unwrap_or_default()
    } else {
        String::from_utf8(query.to_vec()).expect("SQL is UTF-8")
    };
    if let Some(control) = cancellation_response(
        &sql,
        params
            .get("default_format")
            .is_some_and(|format| format == "RowBinaryWithNamesAndTypes"),
    ) {
        return (StatusCode::OK, control);
    }
    queries_tx.send(sql).expect("record ClickHouse query");
    (status, body)
}

pub(super) fn cancellation_response(sql: &str, validation: bool) -> Option<Vec<u8>> {
    let (columns, row): (&[(&str, &str)], Vec<u8>) = if sql.contains("AS coordinator") {
        let mut row = b"\x05node1".to_vec();
        row.extend_from_slice(&1_u64.to_le_bytes());
        row.push(1);
        (
            &[
                ("node", "String"),
                ("expected", "UInt64"),
                ("coordinator", "UInt8"),
            ],
            row,
        )
    } else if sql.contains("system.processes") {
        (
            &[("node", "String"), ("active_id", "String")],
            b"\x05node1\x00".to_vec(),
        )
    } else {
        return None;
    };
    let mut body = Vec::new();
    if validation {
        body.push(columns.len() as u8);
        for value in columns
            .iter()
            .map(|(name, _)| name)
            .chain(columns.iter().map(|(_, kind)| kind))
        {
            body.push(value.len() as u8);
            body.extend_from_slice(value.as_bytes());
        }
    }
    body.extend(row);
    Some(body)
}

fn slot_rows(slot: Option<u64>) -> Vec<u8> {
    // RowBinaryWithNamesAndTypes: one non-null UInt64 column named `slot`.
    let mut body = b"\x01\x04slot\x06UInt64".to_vec();
    if let Some(slot) = slot {
        body.extend_from_slice(&slot.to_le_bytes());
    }
    body
}

#[tokio::test]
async fn latest_slot_empty_result_stays_none() {
    assert_eq!(
        latest_slot_response(StatusCode::OK, slot_rows(None))
            .await
            .expect("empty metadata is not a query error"),
        None
    );
}

#[tokio::test]
async fn latest_slot_preserves_zero_and_full_u64_range() {
    for slot in [0, 432_001, u64::MAX] {
        assert_eq!(
            latest_slot_response(StatusCode::OK, slot_rows(Some(slot)))
                .await
                .expect("decode latest slot"),
            Some(slot)
        );
    }
}

#[tokio::test]
async fn latest_slot_backend_error_is_not_an_empty_result() {
    let error = latest_slot_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        b"latest-slot backend unavailable".to_vec(),
    )
    .await
    .expect_err("backend failures must propagate");
    assert!(
        error
            .to_string()
            .contains("latest-slot backend unavailable")
    );
}

/// Sends one latest-slot request with `hint`, replying to successive queries with `replies`.
async fn hinted_latest_slot(
    hint: Option<u64>,
    replies: Vec<Vec<u8>>,
) -> (ProcessingResult<Option<u64>>, Vec<String>) {
    latest_slot_query(hint, replies, true).await
}

/// As `hinted_latest_slot`, with `CLICKHOUSE_LATEST_SLOT_HINT` set to `hint_enabled`.
async fn latest_slot_query(
    hint: Option<u64>,
    replies: Vec<Vec<u8>>,
    hint_enabled: bool,
) -> (ProcessingResult<Option<u64>>, Vec<String>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ClickHouse test listener");
    let url = format!(
        "http://{}",
        listener.local_addr().expect("listener address")
    );
    let (queries_tx, mut queries_rx) = tokio::sync::mpsc::unbounded_channel();
    let replies = std::sync::Arc::new(std::sync::Mutex::new(
        replies
            .into_iter()
            .collect::<std::collections::VecDeque<_>>(),
    ));
    let app = Router::new().fallback(
        move |Query(params): Query<HashMap<String, String>>, query: Bytes| {
            let queries_tx = queries_tx.clone();
            let replies = replies.clone();
            async move {
                let sql = if query.is_empty() {
                    params.get("query").cloned().unwrap_or_default()
                } else {
                    String::from_utf8_lossy(&query).into_owned()
                };
                // Control queries are answered by `fixture_response`; only data queries consume a reply.
                let body = if cancellation_response(&sql, false).is_some() {
                    Vec::new()
                } else {
                    replies.lock().unwrap().pop_front().unwrap_or_default()
                };
                fixture_response(params, query, StatusCode::OK, body, queries_tx)
            }
        },
    );
    let server = tokio::spawn(async move { axum::serve(listener, app).await });
    let mut client = test_state_with_clickhouse_url(&url).clickhouse.clone();
    client.set_http_client_for_tests(
        client
            .client
            .clone()
            .with_compression(clickhouse::Compression::None),
    );
    client.set_latest_slot_hint(hint_enabled);
    client
        .initialize_read_cancellation()
        .await
        .expect("cancellation preflight");
    let result = client.get_latest_finalized_slot_since(hint).await;
    server.abort();
    let mut queries = Vec::new();
    while let Ok(query) = queries_rx.try_recv() {
        queries.push(query);
    }
    (result, queries)
}

const LEGACY_LATEST_SQL: &str =
    "SELECT slot FROM default.blocks_metadata ORDER BY slot DESC LIMIT 1";

#[tokio::test]
async fn latest_slot_hint_bounds_the_scan() {
    let (result, queries) = hinted_latest_slot(Some(500_000), vec![slot_rows(Some(500_007))]).await;
    assert_eq!(result.expect("hinted latest slot"), Some(500_007));
    assert_eq!(queries.len(), 1, "{queries:?}");
    assert!(
        queries[0].contains(
            "SELECT slot FROM default.blocks_metadata WHERE slot >= 490000 ORDER BY slot DESC LIMIT 1"
        ),
        "{queries:?}"
    );
}

/// CLICKHOUSE_LATEST_SLOT_HINT=false: a caller's hint is ignored and the query is the
/// unbounded one main sends.
#[tokio::test]
async fn latest_slot_hint_switch_off_sends_the_unbounded_query() {
    let (result, queries) =
        latest_slot_query(Some(500_000), vec![slot_rows(Some(500_007))], false).await;
    assert_eq!(result.expect("unhinted latest slot"), Some(500_007));
    assert_eq!(queries.len(), 1, "{queries:?}");
    assert!(queries[0].starts_with(LEGACY_LATEST_SQL), "{queries:?}");
    assert!(!queries[0].contains("WHERE"), "{queries:?}");
}

#[tokio::test]
async fn latest_slot_empty_hinted_result_falls_back_to_unbounded() {
    let (result, queries) = hinted_latest_slot(
        Some(900_000),
        vec![slot_rows(None), slot_rows(Some(432_001))],
    )
    .await;
    assert_eq!(result.expect("fallback latest slot"), Some(432_001));
    assert_eq!(queries.len(), 2, "{queries:?}");
    assert!(queries[0].contains("WHERE slot >= 890000 ORDER BY slot DESC LIMIT 1"));
    assert!(queries[1].starts_with(LEGACY_LATEST_SQL), "{queries:?}");
    assert!(!queries[1].contains("WHERE"));
}

#[tokio::test]
async fn latest_slot_fallback_keeps_empty_source_none() {
    let (result, queries) =
        hinted_latest_slot(Some(900_000), vec![slot_rows(None), slot_rows(None)]).await;
    assert_eq!(result.expect("empty source is not an error"), None);
    assert_eq!(queries.len(), 2);
}

#[tokio::test]
async fn latest_slot_hinted_error_does_not_fall_back() {
    let (result, queries) = hinted_latest_slot(Some(900_000), vec![b"garbage".to_vec()]).await;
    assert!(result.is_err());
    assert_eq!(
        queries.len(),
        1,
        "a failed bounded query must not be retried unbounded"
    );
}

#[tokio::test]
async fn latest_slot_without_useful_hint_sends_legacy_sql() {
    let (_, baseline) = hinted_latest_slot(None, vec![slot_rows(Some(7))]).await;
    assert_eq!(baseline.len(), 1);
    assert!(baseline[0].starts_with(LEGACY_LATEST_SQL), "{baseline:?}");
    // A floor of 0 (hint <= margin) prunes nothing: byte-identical legacy SQL.
    for hint in [Some(0), Some(1), Some(10_000)] {
        let (result, queries) = hinted_latest_slot(hint, vec![slot_rows(Some(7))]).await;
        assert_eq!(result.expect("latest slot"), Some(7));
        assert_eq!(queries, baseline, "hint {hint:?}");
    }
    let (_, queries) = hinted_latest_slot(Some(10_001), vec![slot_rows(Some(10_001))]).await;
    assert!(
        queries[0].contains("WHERE slot >= 1 ORDER BY"),
        "{queries:?}"
    );
}
