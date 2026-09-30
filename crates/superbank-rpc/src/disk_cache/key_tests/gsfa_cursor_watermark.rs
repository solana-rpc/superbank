// SPDX-License-Identifier: AGPL-3.0-only
//! getSignaturesForAddress inline cursor and empty-address watermark, handler level.
use super::address_budget::request_state;
use super::*;
use crate::state::AppState;
use serde_json::{Value, json};

async fn body_for(state: Arc<AppState>, account: &str, options: Value) -> Value {
    let response = Box::pin(
        crate::handlers::signatures::handle_get_signatures_for_address(
            state,
            json!("gsfa-inline-cursor"),
            Some(vec![json!(address(account).to_string()), options]),
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

fn signature_bytes(slot: u64) -> [u8; 64] {
    <[u8; 64]>::try_from(signature(slot).as_ref()).unwrap()
}

/// Primary-only states (no local tier, so every cursor reaches the primary step), with and
/// without the head-cache path.
fn primary_states(source: &ClickHouseClient, cache: &DiskCache) -> Vec<Arc<AppState>> {
    let mut plain = request_state(source, cache);
    plain.disk_cache = None;
    #[cfg_attr(not(feature = "grpc-head-cache"), allow(unused_mut))]
    let mut states = vec![Arc::new(plain)];
    #[cfg(feature = "grpc-head-cache")]
    {
        // Head rows above every fixture row: a deferred `before` must exclude them and a
        // deferred `until` must keep them, exactly as with the separate lookup.
        use solana_commitment_config::CommitmentLevel;
        let head_cache = crate::head_cache::HeadCache::new(1_000, 1000);
        for slot in 200..205u64 {
            let signature = named_signature("head", slot);
            let bytes = <[u8; 64]>::try_from(signature.as_ref()).unwrap();
            let mut record = crate::tests::base_transaction_record();
            record.slot = slot;
            record.signature = bytes;
            record.tx_signatures = vec![bytes];
            record.tx_account_keys = vec![address("address").to_bytes()];
            head_cache.insert_for_tests(
                signature,
                record,
                0,
                &[address("address")],
                CommitmentLevel::Finalized,
            );
        }
        let mut head = request_state(source, cache);
        head.disk_cache = None;
        head.head_cache = Some(Arc::new(head_cache));
        states.push(Arc::new(head));
    }
    states
}

#[tokio::test]
#[ignore = "requires disposable ClickHouse: DISK_CACHE_TEST_URL=http://127.0.0.1:18195"]
async fn gsfa_inline_cursor_matches_the_separate_lookup() {
    let (client, source, cache) = super::address_latency::setup(Duration::from_secs(2)).await;
    let mut inline = source.clone();
    inline.set_gsfa_inline_cursor(true);
    assert!(inline.gsfa_inline_cursor_enabled());
    let missing = signature(999).to_string();
    let shapes = [
        json!({"before": signature(60).to_string(), "limit": 30}),
        json!({"limit": 8}),
        json!({"until": signature(105).to_string(), "limit": 8}),
        json!({"before": named_signature("head", 203).to_string(), "until": signature(100).to_string()}),
        json!({"until": signature(100).to_string(), "limit": 100}),
        json!({"before": signature(56).to_string(), "until": signature(40).to_string()}),
        json!({"before": signature(56).to_string(), "until": signature(56).to_string()}),
        json!({"beforeSlot": 90, "until": signature(40).to_string(), "limit": 5}),
        // Same-slot cursor: rows at slot 55 differ only by slot_idx.
        json!({"before": named_signature("same", 7).to_string(), "limit": 10}),
        json!({"until": named_signature("same", 7).to_string(), "beforeSlot": 56}),
        json!({"before": missing, "limit": 10}),
        json!({"until": missing, "limit": 10}),
        json!({"before": missing, "until": signature(40).to_string()}),
        json!({"before": signature(60).to_string(), "until": missing}),
        json!({"before": missing, "until": signature(998).to_string()}),
    ];
    for (inline_state, separate_state) in primary_states(&inline, &cache)
        .into_iter()
        .zip(primary_states(&source, &cache))
    {
        for options in &shapes {
            // Inline first: it must not populate the shared signature-slot cache.
            let inline_body = body_for(inline_state.clone(), "address", options.clone()).await;
            for key in ["before", "until"] {
                if let Some(sig) = options.get(key).and_then(Value::as_str) {
                    let bytes = <[u8; 64]>::try_from(
                        <Signature as std::str::FromStr>::from_str(sig)
                            .unwrap()
                            .as_ref(),
                    )
                    .unwrap();
                    assert_eq!(
                        inline.cached_signature_slot(&bytes).await,
                        None,
                        "inline path resolved {key} with a separate lookup: {options}"
                    );
                }
            }
            let separate_body = body_for(separate_state.clone(), "address", options.clone()).await;
            assert_eq!(inline_body, separate_body, "{options}");
            // Reset the separate path's cache entries so the next inline request is inline.
            source.signature_slot_cache.clear_for_tests().await;
        }
    }
    let error = body_for(
        primary_states(&inline, &cache)[0].clone(),
        "address",
        json!({"before": missing, "until": signature(998).to_string()}),
    )
    .await;
    assert_eq!(error["error"]["code"], -32020, "{error}");
    assert!(
        error["error"]["message"]
            .as_str()
            .unwrap()
            .contains(&missing),
        "before wins when both cursors are missing: {error}"
    );

    // A cached position is used without deferring (and without a query).
    let _ = source
        .get_signature_slot(&signature(60).to_string())
        .await
        .unwrap();
    assert!(
        inline
            .cached_signature_slot(&signature_bytes(60))
            .await
            .is_some()
    );

    // The existing budget and race suites still pass with the flag on.
    super::address_budget::assert_handler_budget(Arc::new(request_state(&inline, &cache)), &cache)
        .await;
    let source_database = cache.inner.cfg.database.trim_end_matches("_cache");
    execute(
        &client,
        &format!("DROP DATABASE {} SYNC", cache.inner.cfg.database),
    )
    .await;
    execute(&client, &format!("DROP DATABASE {source_database} SYNC")).await;
}

#[cfg(feature = "grpc-head-cache")]
fn proven_head(from: u64, to: u64) -> Arc<crate::head_cache::HeadCache> {
    use crate::head_cache::coverage::Link;
    use solana_commitment_config::CommitmentLevel;
    let head = Arc::new(crate::head_cache::HeadCache::new(1_000_000, 1000));
    {
        let mut proof = head.coverage.write().unwrap();
        proof.connect();
        let now = std::time::Instant::now();
        for slot in from..=to {
            proof.metadata(Link {
                slot,
                hash: [slot as u8; 32],
                parent: slot - 1,
                parent_hash: [(slot - 1) as u8; 32],
            });
            proof.observe(slot, CommitmentLevel::Finalized, now);
            proof.publish(slot, CommitmentLevel::Finalized);
        }
    }
    head
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
#[ignore = "requires disposable ClickHouse: DISK_CACHE_TEST_URL=http://127.0.0.1:18195"]
async fn gsfa_empty_watermark_skips_the_primary_only_when_proven() {
    let (client, source, cache) =
        super::address_latency::setup_with(Duration::from_secs(2), |cfg| {
            cfg.gsfa_empty_watermark_ttl = Duration::from_secs(60);
            cfg.gsfa_empty_watermark_max_entries = 16;
        })
        .await;
    let (floor, tip) = cache.tip_span().unwrap();
    assert_eq!((floor, tip), (10, 109));
    let empty = address("nobody");
    let watermarks = cache.gsfa_watermarks();
    let with_head = |head: Arc<crate::head_cache::HeadCache>| {
        let mut state = request_state(&source, &cache);
        state.head_cache = Some(head);
        Arc::new(state)
    };
    let mut primary_only = request_state(&source, &cache);
    primary_only.disk_cache = None;
    let primary_only = Arc::new(primary_only);

    // Miss: an empty primary page fills the watermark below the local tip.
    let state = with_head(proven_head(tip + 1, tip + 5));
    let response = body_for(state.clone(), "nobody", json!({"limit": 1})).await;
    assert_eq!(response["result"], json!([]), "{response}");
    let now = std::time::Instant::now();
    assert_eq!(
        watermarks.get(&empty, now),
        Some(crate::disk_cache::gsfa_watermark::fill_watermark(tip))
    );
    // A non-empty primary page never fills; rows drop a stale entry.
    watermarks.insert(address("address"), 5, now);
    let _ = body_for(state.clone(), "address", json!({"limit": 1000})).await;
    assert_eq!(watermarks.get(&address("address"), now), None);

    let total = source.http_query_sem.available_permits();
    let blocked = source
        .http_query_sem
        .clone()
        .acquire_many_owned(total as u32)
        .await
        .unwrap();
    let answers_without_primary = |state: Arc<AppState>, account: &'static str, options: Value| async move {
        tokio::time::timeout(
            Duration::from_millis(500),
            body_for(state, account, options),
        )
        .await
        .ok()
    };

    // Hit: local covers (W, tip] and the head proves (tip, head tip].
    watermarks.insert(empty, tip, std::time::Instant::now());
    let state = with_head(proven_head(tip + 1, tip + 5));
    let hit = answers_without_primary(state.clone(), "nobody", json!({"limit": 1}))
        .await
        .expect("a proven watermark must not wait for the primary");
    assert_eq!(hit["result"], json!([]), "{hit}");

    // Rows above W are read: W just below the address's oldest row serves its full
    // history from the local page, identical to the primary-only answer.
    watermarks.insert(address("address"), floor - 1, std::time::Instant::now());
    let local = answers_without_primary(
        with_head(proven_head(tip + 1, tip + 5)),
        "address",
        json!({"limit": 1000}),
    )
    .await
    .expect("rows above the watermark come from the local page");

    // Not proven: every case below must reach the (blocked) primary.
    let cases: Vec<(&str, Arc<AppState>, Value)> = vec![
        (
            "head gap above the local tip",
            with_head(proven_head(tip + 3, tip + 5)),
            json!({"limit": 1}),
        ),
        (
            "head without coverage",
            with_head(Arc::new(crate::head_cache::HeadCache::new(32, 1000))),
            json!({"limit": 1}),
        ),
        (
            "cursor request",
            with_head(proven_head(tip + 1, tip + 5)),
            json!({"limit": 1, "beforeSlot": 100}),
        ),
    ];
    for (name, state, options) in cases {
        watermarks.insert(empty, tip, std::time::Instant::now());
        assert!(
            answers_without_primary(state, "nobody", options)
                .await
                .is_none(),
            "{name}"
        );
    }
    // W below the local floor leaves slots the local page never read.
    watermarks.insert(empty, floor - 2, std::time::Instant::now());
    assert!(
        answers_without_primary(
            with_head(proven_head(tip + 1, tip + 5)),
            "nobody",
            json!({"limit": 1})
        )
        .await
        .is_none(),
        "floor gap"
    );
    drop(blocked);
    tokio::time::timeout(Duration::from_secs(5), async {
        while source.http_query_sem.available_permits() != total {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("primary reads drain");
    let expected = body_for(primary_only.clone(), "address", json!({"limit": 1000})).await;
    assert_eq!(local, expected);

    // TTL: an expired entry is gone.
    let short = crate::disk_cache::gsfa_watermark::GsfaWatermarks::new(Duration::from_millis(1), 4);
    let then = std::time::Instant::now();
    short.insert(empty, tip, then);
    assert_eq!(short.get(&empty, then + Duration::from_millis(2)), None);

    let source_database = cache.inner.cfg.database.trim_end_matches("_cache");
    execute(
        &client,
        &format!("DROP DATABASE {} SYNC", cache.inner.cfg.database),
    )
    .await;
    execute(&client, &format!("DROP DATABASE {source_database} SYNC")).await;
}
