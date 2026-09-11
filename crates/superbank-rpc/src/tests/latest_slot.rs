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
    let result = client.get_latest_finalized_slot().await;
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

fn cancellation_response(sql: &str, validation: bool) -> Option<Vec<u8>> {
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
