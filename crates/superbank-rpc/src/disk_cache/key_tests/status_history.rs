// SPDX-License-Identifier: AGPL-3.0-only
//! Local-only regressions for the getSignatureStatuses history absence cache: the
//! tri-state local read and the handler's primary skip, with the query log as an
//! independent count of primary status queries.
use super::*;
use crate::head_cache::HeadCache;
use crate::head_cache::coverage::Link;
use crate::state::AppState;
use crate::status_history_cache::StatusHistoryCache;
use serde_json::{Value, json};
use solana_commitment_config::CommitmentLevel;

/// Finished status queries against one database so far.
async fn status_queries(client: &clickhouse::Client, database: &str) -> u64 {
    execute(client, "SYSTEM FLUSH LOGS").await;
    client
        .query("SELECT count() FROM system.query_log WHERE type='QueryFinish' AND has(databases, ?) AND query LIKE '%argMax(tuple(slot, err)%'")
        .bind(database)
        .fetch_one::<u64>()
        .await
        .unwrap()
}

fn link(slot: u64) -> Link {
    Link {
        slot,
        hash: [slot as u8; 32],
        parent: slot - 1,
        parent_hash: [(slot - 1) as u8; 32],
    }
}

/// Extend the head's verified finalized chain to `tip` and refresh its tip age.
fn advance_head(head: &HeadCache, from: u64, tip: u64) {
    let mut proof = head.coverage.write().unwrap();
    for slot in from..=tip {
        proof.metadata(link(slot));
        proof.observe(slot, CommitmentLevel::Finalized, std::time::Instant::now());
        proof.publish(slot, CommitmentLevel::Finalized);
    }
}

fn detailed(lookups: &DiskStatusLookups) -> Vec<Option<u64>> {
    lookups
        .statuses
        .iter()
        .map(|lookup| match lookup {
            DiskStatusLookup::Found(status) => Some(status.slot),
            DiskStatusLookup::Absent => Some(0),
            DiskStatusLookup::Unknown => None,
        })
        .collect()
}

/// Hold every local query permit so a local read that must query waits out its deadline.
async fn hold_local_queries(cache: &DiskCache) -> tokio::sync::SemaphorePermit<'_> {
    let sem = &cache.inner.local.http_query_sem;
    sem.acquire_many(sem.available_permits() as u32)
        .await
        .unwrap()
}

/// A read that timed out, or whose index epoch moved while it ran, proves nothing:
/// no span, and even a row it did find is Unknown rather than Absent. Requires every
/// signature to be a query candidate (a Bloom negative completes without querying).
async fn assert_incomplete_reads_prove_nothing(cache: &DiskCache, absent: Signature) {
    let batch = vec![signature(30), absent];
    let permits = hold_local_queries(cache).await;
    let lookups = cache.get_sig_statuses_detailed(batch.clone()).await;
    drop(permits);
    assert_eq!(lookups.span, None, "a timed-out read has no provable span");
    assert_eq!(detailed(&lookups), [None, None]);

    let permits = hold_local_queries(cache).await;
    let read = cache.get_sig_statuses_detailed(batch.clone());
    tokio::pin!(read);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut read)
            .await
            .is_err()
    );
    // The read captured its epoch and waits for admission; eviction invalidates it.
    cache.inner.key_index.invalidate_reads();
    drop(permits);
    let lookups = read.await;
    assert_eq!(
        lookups.span, None,
        "an invalidated read has no provable span"
    );
    assert_eq!(detailed(&lookups), [None, None]);

    // The same read, undisturbed, proves both.
    let lookups = cache.get_sig_statuses_detailed(batch).await;
    assert!(lookups.span.is_some());
    assert_eq!(detailed(&lookups), [Some(30), Some(0)]);
}

struct Harness {
    client: clickhouse::Client,
    state: Arc<AppState>,
    head: Arc<HeadCache>,
    cache: Arc<DiskCache>,
    source_database: String,
    head_tip: u64,
}

impl Harness {
    /// One request's `value` array and the primary status queries it issued. The head
    /// tip advances first so its age stays within the trusted bound.
    async fn statuses(&mut self, signatures: &[Signature]) -> (Value, u64) {
        self.head_tip += 1;
        advance_head(&self.head, self.head_tip, self.head_tip);
        let before = status_queries(&self.client, &self.source_database).await;
        let response = crate::handlers::signatures::handle_get_signature_statuses(
            self.state.clone(),
            json!(1),
            Some(vec![
                json!(
                    signatures
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                ),
                json!({ "searchTransactionHistory": true }),
            ]),
        )
        .await
        .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap();
        let after = status_queries(&self.client, &self.source_database).await;
        (body["result"]["value"].clone(), after - before)
    }

    fn with_history_cache(&mut self, cache: StatusHistoryCache) {
        // Only this test holds the state; handler futures have completed.
        Arc::get_mut(&mut self.state).unwrap().status_history_cache = cache;
    }
}

#[tokio::test]
#[ignore = "requires disposable ClickHouse: DISK_CACHE_TEST_URL=http://127.0.0.1:18195"]
async fn status_history_cache_clickhouse_integration() {
    let (client, source, cache) = address_latency::setup(Duration::from_secs(2)).await;
    let cache = Arc::new(cache);
    let cache_database = cache.inner.cfg.database.clone();
    let source_database = cache_database.trim_end_matches("_cache").to_string();
    let absent = named_signature("absent", 1);
    let other_absent = named_signature("absent", 2);

    // Tri-state: found, Bloom-negative absent, and a row outside coverage.
    let lookups = cache
        .get_sig_statuses_detailed(vec![signature(15), absent, signature(109)])
        .await;
    assert_eq!(lookups.span, Some((10, 109)));
    assert_eq!(detailed(&lookups), [Some(15), Some(0), Some(109)]);
    // A row in a searched partition but below the coverage floor proves nothing; a
    // signature only in an unsearched older partition is absent from the span.
    cache.inner.coverage.write().unwrap().remove_below(25);
    let lookups = cache
        .get_sig_statuses_detailed(vec![signature(22), absent, signature(15)])
        .await;
    assert_eq!(lookups.span, Some((25, 109)));
    assert_eq!(detailed(&lookups), [None, Some(0), Some(0)]);
    // A hole leaves no contiguous span: nothing is provably absent.
    cache.inner.coverage.write().unwrap().remove(60);
    let lookups = cache
        .get_sig_statuses_detailed(vec![signature(30), absent])
        .await;
    assert_eq!(lookups.span, None);
    assert_eq!(detailed(&lookups), [Some(30), None]);
    // Unknown index partitions are candidates, never proof of absence by omission.
    cache.inner.coverage.write().unwrap().insert(60);
    cache.inner.key_index.clear_signatures();
    let lookups = cache
        .get_sig_statuses_detailed(vec![signature(30), absent])
        .await;
    assert_eq!(detailed(&lookups), [Some(30), Some(0)]);
    // Every signature is now a candidate, so each read below must query.
    assert_incomplete_reads_prove_nothing(&cache, absent).await;
    cache.build_signature_indexes().await;

    // Handler: primary = source database, disk = cache (coverage 25..=109), head
    // verified from 110.
    let mut state = crate::tests::test_state_with_clickhouse_url(&source.url);
    {
        let mutable = Arc::get_mut(&mut state).unwrap();
        mutable.clickhouse = source.clone();
        mutable.disk_cache = Some(Arc::new(tokio::sync::OnceCell::new_with(Some(
            cache.clone(),
        ))));
        let head = Arc::new(HeadCache::new(600, 64));
        head.coverage.write().unwrap().connect();
        advance_head(&head, 110, 112);
        mutable.head_cache = Some(head);
    }
    let head = state.head_cache.clone().unwrap();
    let mut h = Harness {
        client: client.clone(),
        state,
        head,
        cache: cache.clone(),
        source_database: source_database.clone(),
        head_tip: 112,
    };
    let batch = [absent, signature(30), other_absent, signature(15)];
    let expected = json!([
        null,
        { "slot": 30, "confirmations": null, "err": null, "status": { "Ok": null }, "confirmationStatus": "finalized" },
        null,
        { "slot": 15, "confirmations": null, "err": null, "status": { "Ok": null }, "confirmationStatus": "finalized" },
    ]);

    // Disabled (the default): every poll asks the primary.
    for _ in 0..2 {
        let (value, primary) = h.statuses(&batch).await;
        assert_eq!(value, expected);
        assert_eq!(primary, 1);
    }

    h.with_history_cache(StatusHistoryCache::new(
        1000,
        1 << 20,
        Duration::from_secs(60),
    ));
    // The first poll learns the absences; sig-15 (absent from the disk span, below its
    // floor) is found by the primary and is never cached.
    let (value, primary) = h.statuses(&batch).await;
    assert_eq!((value, primary), (expected.clone(), 1));
    assert_eq!(h.state.status_history_cache.entry_count().await, 2);
    // Repeat poll: only sig-15 still needs the primary.
    let (value, primary) = h.statuses(&batch).await;
    assert_eq!((value, primary), (expected.clone(), 1));
    let only_absent = [absent, other_absent];
    let (value, primary) = h.statuses(&only_absent).await;
    assert_eq!((value, primary), (json!([null, null]), 0));

    // An unhealthy head (disconnected, or its tip too old) proves nothing newer.
    h.head.coverage.write().unwrap().disconnect();
    let before = status_queries(&h.client, &source_database).await;
    let response = crate::handlers::signatures::handle_get_signature_statuses(
        h.state.clone(),
        json!(1),
        Some(vec![
            json!([absent.to_string()]),
            json!({ "searchTransactionHistory": true }),
        ]),
    )
    .await
    .unwrap();
    assert!(response.status().is_success());
    assert_eq!(
        status_queries(&h.client, &source_database).await - before,
        1
    );
    h.head.coverage.write().unwrap().connect();
    advance_head(&h.head, 110, h.head_tip);
    let (_, primary) = h.statuses(&only_absent).await;
    assert_eq!(primary, 0);

    // A gap between the disk tip and the head chain: the primary is asked.
    h.head.coverage.write().unwrap().connect();
    advance_head(&h.head, 112, h.head_tip);
    let (value, primary) = h.statuses(&only_absent).await;
    assert_eq!((value, primary), (json!([null, null]), 1));
    advance_head(&h.head, 110, h.head_tip);

    // A hole in disk coverage: nothing is provably absent, so the primary is asked
    // and no entry is created from that read.
    cache.inner.coverage.write().unwrap().remove(60);
    let fresh = named_signature("absent", 3);
    let (value, primary) = h.statuses(&[fresh]).await;
    assert_eq!((value, primary), (json!([null]), 1));
    cache.inner.coverage.write().unwrap().insert(60);
    let (_, primary) = h.statuses(&[fresh]).await;
    assert_eq!(primary, 1, "an unproven read must not create an entry");
    let (_, primary) = h.statuses(&[fresh]).await;
    assert_eq!(primary, 0);

    // A primary error creates nothing.
    let errored = named_signature("absent", 4);
    let saved = h.state.clickhouse.signature_statuses_table.clone();
    Arc::get_mut(&mut h.state)
        .unwrap()
        .clickhouse
        .signature_statuses_table = format!("{source_database}.missing_table");
    let response = crate::handlers::signatures::handle_get_signature_statuses(
        h.state.clone(),
        json!(1),
        Some(vec![
            json!([errored.to_string()]),
            json!({ "searchTransactionHistory": true }),
        ]),
    )
    .await
    .unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert!(body.get("error").is_some(), "{body}");
    Arc::get_mut(&mut h.state)
        .unwrap()
        .clickhouse
        .signature_statuses_table = saved;
    let (_, primary) = h.statuses(&[errored]).await;
    assert_eq!(
        primary, 1,
        "an errored primary answer must not create an entry"
    );

    // A local read that timed out proves nothing, even when the primary then finds
    // nothing: no entry, and the next poll still asks.
    let slow = named_signature("absent", 5);
    cache.inner.key_index.clear_signatures();
    let entries = h.state.status_history_cache.entry_count().await;
    let permits = hold_local_queries(&cache).await;
    let (value, primary) = h.statuses(&[slow]).await;
    drop(permits);
    assert_eq!((value, primary), (json!([null]), 1));
    assert_eq!(h.state.status_history_cache.entry_count().await, entries);
    cache.build_signature_indexes().await;
    let (_, primary) = h.statuses(&[slow]).await;
    assert_eq!(primary, 1, "a timed-out read must not create an entry");
    let (_, primary) = h.statuses(&[slow]).await;
    assert_eq!(primary, 0);

    // Eviction past the tip the primary answered at: a landing there could hide.
    cache.inner.coverage.write().unwrap().remove_below(111);
    cache.inner.coverage.write().unwrap().insert_range(111, 120);
    advance_head(&h.head, 121, h.head_tip.max(121));
    h.head_tip = h.head_tip.max(121);
    let (_, primary) = h.statuses(&only_absent).await;
    assert_eq!(primary, 1);
    cache.inner.coverage.write().unwrap().insert_range(25, 110);

    // TTL expiry.
    h.with_history_cache(StatusHistoryCache::new(
        1000,
        1 << 20,
        Duration::from_secs(1),
    ));
    let (_, primary) = h.statuses(&only_absent).await;
    assert_eq!(primary, 1);
    let (_, primary) = h.statuses(&only_absent).await;
    assert_eq!(primary, 0);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let (value, primary) = h.statuses(&only_absent).await;
    assert_eq!((value, primary), (json!([null, null]), 1));

    let _ = &h.cache;
    if std::env::var_os("DISK_CACHE_TEST_KEEP").is_some() {
        eprintln!("Kept cache {cache_database}");
        return;
    }
    execute(&client, &format!("DROP DATABASE {cache_database} SYNC")).await;
    execute(&client, &format!("DROP DATABASE {source_database} SYNC")).await;
}

/// Handler latency of a repeat poll for unlanded signatures with the cache disabled
/// (a loopback "primary" query) and enabled (skipped). A remote primary adds its network
/// round trip to the disabled case.
#[tokio::test]
#[ignore = "benchmark; requires disposable ClickHouse: DISK_CACHE_TEST_URL=http://127.0.0.1:18195"]
async fn status_history_cache_latency_benchmark() {
    let (client, source, cache) = address_latency::setup(Duration::from_secs(2)).await;
    let cache = Arc::new(cache);
    let cache_database = cache.inner.cfg.database.clone();
    let source_database = cache_database.trim_end_matches("_cache").to_string();
    let mut state = crate::tests::test_state_with_clickhouse_url(&source.url);
    let head = Arc::new(HeadCache::new(600, 64));
    head.coverage.write().unwrap().connect();
    advance_head(&head, 110, 112);
    {
        let mutable = Arc::get_mut(&mut state).unwrap();
        mutable.clickhouse = source.clone();
        mutable.disk_cache = Some(Arc::new(tokio::sync::OnceCell::new_with(Some(
            cache.clone(),
        ))));
        mutable.head_cache = Some(head.clone());
    }
    let mut tip = 112;
    for (label, enabled) in [("disabled", false), ("enabled", true), ("disabled", false)] {
        Arc::get_mut(&mut state).unwrap().status_history_cache = if enabled {
            StatusHistoryCache::new(100_000, 1 << 26, Duration::from_secs(300))
        } else {
            StatusHistoryCache::new(0, 0, Duration::ZERO)
        };
        for batch_size in [1usize, 8] {
            let batch: Vec<String> = (0..batch_size)
                .map(|idx| named_signature("bench-absent", idx as u64).to_string())
                .collect();
            let mut samples = Vec::new();
            for round in 0..201 {
                tip += 1;
                advance_head(&head, tip, tip);
                let started = std::time::Instant::now();
                let response = crate::handlers::signatures::handle_get_signature_statuses(
                    state.clone(),
                    json!(1),
                    Some(vec![
                        json!(batch),
                        json!({ "searchTransactionHistory": true }),
                    ]),
                )
                .await
                .unwrap();
                let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                    .await
                    .unwrap();
                let elapsed = started.elapsed();
                let body: Value = serde_json::from_slice(&body).unwrap();
                assert_eq!(
                    body["result"]["value"],
                    json!(vec![Value::Null; batch_size])
                );
                if round > 0 {
                    samples.push(elapsed.as_secs_f64() * 1e3);
                }
            }
            samples.sort_by(f64::total_cmp);
            eprintln!(
                "{label} batch={batch_size}: p50 {:.3} ms p90 {:.3} ms p99 {:.3} ms",
                samples[samples.len() / 2],
                samples[samples.len() * 9 / 10],
                samples[samples.len() * 99 / 100]
            );
        }
    }
    execute(&client, &format!("DROP DATABASE {cache_database} SYNC")).await;
    execute(&client, &format!("DROP DATABASE {source_database} SYNC")).await;
}
