// SPDX-License-Identifier: AGPL-3.0-only
//! Local-only regressions for single-query signature-status reads.
use super::*;

/// Finished local status queries so far (the query log is an independent observer).
async fn status_queries(client: &clickhouse::Client, cache: &DiskCache) -> u64 {
    execute(client, "SYSTEM FLUSH LOGS").await;
    client
        .query("SELECT count() FROM system.query_log WHERE type='QueryFinish' AND has(databases, ?) AND query LIKE '%argMax(tuple(slot, err)%'")
        .bind(&cache.inner.cfg.database)
        .fetch_one::<u64>()
        .await
        .unwrap()
}

/// Asserts the batch resolves exactly as the per-partition queries do, and returns the
/// statuses with the local queries each path issued.
async fn same_statuses(
    client: &clickhouse::Client,
    cache: &DiskCache,
    batch: &[Signature],
) -> (Vec<Option<DiskSigStatus>>, u64, u64) {
    let before = status_queries(client, cache).await;
    let single = cache.get_sig_statuses(batch.to_vec()).await;
    let middle = status_queries(client, cache).await;
    let per_partition = cache.get_sig_statuses_per_partition(batch.to_vec()).await;
    let after = status_queries(client, cache).await;
    assert_eq!(single, per_partition, "batch {batch:?}");
    (single, middle - before, after - middle)
}

fn slots(statuses: &[Option<DiskSigStatus>]) -> Vec<Option<u64>> {
    statuses
        .iter()
        .map(|status| status.as_ref().map(|status| status.slot))
        .collect()
}

#[tokio::test]
#[ignore = "requires disposable ClickHouse: DISK_CACHE_TEST_URL=http://127.0.0.1:18195"]
async fn status_span_clickhouse_integration() {
    let (client, source, cache) = address_latency::setup(Duration::from_secs(2)).await;
    let database = cache.inner.cfg.database.clone();
    // The same signature again, newer and failed, in another partition: both paths
    // must resolve it to the newest row.
    execute(&client, &format!("INSERT INTO {database}.transactions (signature,slot,slot_idx,tx_signatures,tx_account_keys,tx_num_required_signatures,meta_status_ok,meta_err) VALUES (toFixedString('sig-15',64),95,1,[toFixedString('sig-15',64)],[toFixedString('address',32)],1,0,'\"AccountInUse\"')")).await;
    cache.build_signature_indexes().await;

    // Restart-like: every signature partition is unknown, so each is a candidate.
    cache.inner.key_index.clear_signatures();
    let unknown = cache.inner.key_index.signature_candidates(
        1,
        10,
        key_index::SignatureHash::new(signature(12).as_ref()),
    );
    assert_eq!(unknown.unknown_partitions.len(), 10);
    let mut batch: Vec<_> = (10..110).step_by(7).map(signature).collect();
    batch.extend([
        named_signature("absent", 1),
        named_signature("same", 3),
        signature(15),
    ]);
    let (statuses, single, per_partition) = same_statuses(&client, &cache, &batch).await;
    assert_eq!((single, per_partition), (1, 10));
    let mut expected: Vec<_> = (10..110).step_by(7).map(Some).collect();
    expected.extend([None, Some(55), Some(95)]);
    assert_eq!(slots(&statuses), expected);
    let newest = statuses.last().unwrap().as_ref().unwrap();
    assert!(newest.err.as_deref().unwrap().contains("AccountInUse"));

    // DISK_CACHE_STATUS_SPAN_QUERY=false: the same restart-like batch takes the
    // per-partition queries, as before the span query.
    let mut off_cfg = cache.inner.cfg.clone();
    off_cfg.status_span_query = false;
    let off = Box::pin(DiskCache::open(off_cfg, &source)).await.unwrap();
    off.build_signature_indexes().await;
    off.inner.key_index.clear_signatures();
    let before = status_queries(&client, &off).await;
    let off_statuses = off.get_sig_statuses(batch.clone()).await;
    assert_eq!(status_queries(&client, &off).await - before, 10);
    assert_eq!(off_statuses, statuses);
    drop(off);

    // Known and sparse: skipped partitions inside the span are index negatives.
    cache.build_signature_indexes().await;
    let batch = [signature(12), signature(45), signature(101), signature(15)];
    let (statuses, single, per_partition) = same_statuses(&client, &cache, &batch).await;
    assert_eq!(single, 1);
    assert!(per_partition >= 4, "{per_partition}");
    assert_eq!(slots(&statuses), [Some(12), Some(45), Some(101), Some(95)]);

    // Beyond the lookup cap a sparse batch keeps the per-partition queries.
    let mut batch: Vec<_> = (10..110).map(signature).collect();
    batch.extend((1..21).map(|idx| named_signature("same", idx)));
    let (statuses, single, per_partition) = same_statuses(&client, &cache, &batch).await;
    assert_eq!(single, per_partition);
    assert!(single >= 10, "{single}");
    assert!(statuses.iter().all(Option::is_some));

    // Rows outside coverage stay unavailable on both paths.
    cache.inner.coverage.write().unwrap().remove_below(15);
    let batch = [signature(12), signature(16), signature(15)];
    let (statuses, single, _) = same_statuses(&client, &cache, &batch).await;
    assert_eq!(single, 1);
    assert_eq!(slots(&statuses), [None, Some(16), Some(95)]);

    if std::env::var_os("DISK_CACHE_TEST_KEEP").is_some() {
        eprintln!("Kept cache {database}");
        return;
    }
    execute(&client, &format!("DROP DATABASE {database} SYNC")).await;
    execute(
        &client,
        &format!("DROP DATABASE {} SYNC", database.trim_end_matches("_cache")),
    )
    .await;
}

fn bench_signature(slot: u64, idx: u64) -> Signature {
    named_signature(&format!("b-{slot}"), idx)
}

/// Median and p90 wall time of `rounds` calls, after one warm-up call.
async fn timed<F: std::future::Future<Output = Vec<Option<DiskSigStatus>>>>(
    rounds: usize,
    mut call: impl FnMut() -> F,
) -> (Duration, Duration, usize) {
    let found = call().await.iter().filter(|s| s.is_some()).count();
    let mut samples = Vec::with_capacity(rounds);
    for _ in 0..rounds {
        let started = std::time::Instant::now();
        assert_eq!(call().await.iter().filter(|s| s.is_some()).count(), found);
        samples.push(started.elapsed());
    }
    samples.sort();
    (samples[rounds / 2], samples[rounds * 9 / 10], found)
}

#[tokio::test]
#[ignore = "benchmark: DISK_CACHE_TEST_URL=http://127.0.0.1:18195, run with --nocapture"]
async fn status_span_latency_benchmark() {
    let url = std::env::var("DISK_CACHE_TEST_URL").expect("explicit disposable ClickHouse URL");
    assert!(url.starts_with("http://127.0.0.1:"));
    let rows_per_slot: u64 =
        std::env::var("STATUS_BENCH_ROWS_PER_SLOT").map_or(2000, |v| v.parse().unwrap());
    let database = format!("test_status_bench_{}", now_version());
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
    // 88 partitions of 10 slots: a scaled-down retention window with many partitions.
    let mut cfg = config(url, format!("{database}_cache"));
    cfg.retain_slots = 880;
    let cache = DiskCache::open(cfg, &source).await.unwrap();
    let cache_db = &cache.inner.cfg.database;
    execute(&client, &format!("INSERT INTO {cache_db}.transactions (signature,slot,slot_idx,tx_signatures,tx_account_keys,tx_num_required_signatures,meta_status_ok) SELECT toFixedString(concat('b-',toString(10 + intDiv(number,{rows_per_slot})),'-',toString(number % {rows_per_slot})),64),10 + intDiv(number,{rows_per_slot}),number % {rows_per_slot},[toFixedString(concat('b-',toString(10 + intDiv(number,{rows_per_slot})),'-',toString(number % {rows_per_slot})),64)],[toFixedString('address',32)],1,1 FROM numbers({}) SETTINGS max_partitions_per_insert_block=200", 880 * rows_per_slot)).await;
    execute(
        &client,
        &format!("OPTIMIZE TABLE {cache_db}.signatures FINAL"),
    )
    .await;
    cache
        .publish_range_coverage(
            (10..890)
                .map(|slot| {
                    (
                        slot,
                        SlotStatus::Covered {
                            tx_count: rows_per_slot as u32,
                        },
                    )
                })
                .collect(),
        )
        .await
        .unwrap();
    cache.build_signature_indexes().await;
    let parts = client
        .query(&format!("SELECT count() FROM system.parts WHERE active AND database='{cache_db}' AND table='signatures'"))
        .fetch_one::<u64>()
        .await
        .unwrap();
    eprintln!(
        "fixture: 88 partitions x {} rows, {parts} active signature parts",
        10 * rows_per_slot
    );

    // Batches: `n` signatures spread over every partition, plus one clustered batch.
    let spread = |n: u64| -> Vec<Signature> {
        (0..n)
            .map(|i| bench_signature(10 + (i * 37 + i / 88) % 880, i * 7919 % rows_per_slot))
            .collect()
    };
    let clustered: Vec<_> = (0..8).map(|i| bench_signature(850 + i * 5, i)).collect();
    let rounds = 15;
    for unknown in [true, false] {
        if unknown {
            cache.inner.key_index.clear_signatures();
        } else {
            cache.build_signature_indexes().await;
        }
        for (name, batch) in [
            ("1 spread", spread(1)),
            ("8 spread", spread(8)),
            ("8 clustered", clustered.clone()),
            ("64 spread", spread(64)),
            ("256 spread", spread(256)),
        ] {
            let (old50, old90, old_found) = timed(rounds, || {
                cache.get_sig_statuses_per_partition(batch.clone())
            })
            .await;
            let (new50, new90, new_found) =
                timed(rounds, || cache.get_sig_statuses(batch.clone())).await;
            assert_eq!(old_found, new_found);
            eprintln!(
                "unknown={unknown} {name:>12}: found {new_found}/{} per-partition p50 {old50:?} p90 {old90:?} | span p50 {new50:?} p90 {new90:?}",
                batch.len()
            );
        }
    }
    // Raw single range queries, bypassing the lookup cap: cost per signature-partition lookup.
    for partitions in [1u64, 10, 88] {
        for n in [1u64, 8, 64, 256] {
            let pending: Vec<_> = (0..n)
                .map(|i| {
                    bench_signature(890 - partitions * 10 + (i * 37) % (partitions * 10), i)
                        .to_string()
                })
                .collect();
            let mut client = cache.query_client();
            client.cache_partition = Some((10, 88));
            client.cache_slot_range = Some((890 - partitions * 10, 889));
            let mut samples = Vec::new();
            for _ in 0..=rounds {
                let started = std::time::Instant::now();
                let (rows, _) = client.get_signature_statuses(&pending).await.unwrap();
                assert_eq!(rows.len() as u64, n);
                samples.push(started.elapsed());
            }
            samples.remove(0);
            samples.sort();
            eprintln!(
                "raw span {partitions:>2} partitions x {n:>3} signatures ({:>5} lookups): p50 {:?} p90 {:?}",
                partitions * n,
                samples[rounds / 2],
                samples[rounds * 9 / 10]
            );
        }
    }
    if std::env::var_os("DISK_CACHE_TEST_KEEP").is_none() {
        execute(&client, &format!("DROP DATABASE {cache_db} SYNC")).await;
        execute(&client, &format!("DROP DATABASE {database} SYNC")).await;
    }
}
