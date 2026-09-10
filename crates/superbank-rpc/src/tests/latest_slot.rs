// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use axum::{Router, body::Bytes, http::StatusCode};

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
    let app = Router::new().fallback(move |query: Bytes| {
        let body = body.clone();
        let queries_tx = queries_tx.clone();
        async move {
            queries_tx.send(query).expect("record ClickHouse query");
            (status, body)
        }
    });
    let server = tokio::spawn(async move { axum::serve(listener, app).await });

    let mut client = test_state_with_clickhouse_url(&url).clickhouse.clone();
    client.client = client
        .client
        .clone()
        .with_compression(clickhouse::Compression::None);
    // Enable the global result cache so this test detects a loss of tip freshness.
    client.query_cache = QueryCacheConfig::new(true, 60, false, true);
    let result = client.get_latest_finalized_slot().await;
    server.abort();

    let query = queries_rx.try_recv().expect("latest-slot query was sent");
    let query = std::str::from_utf8(&query).expect("SQL is UTF-8");
    assert!(query.contains("SELECT slot FROM default.blocks_metadata ORDER BY slot DESC LIMIT 1"));
    assert!(!query.contains("use_query_cache=1"));
    result
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
