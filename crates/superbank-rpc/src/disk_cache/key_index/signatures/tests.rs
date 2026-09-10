// SPDX-License-Identifier: AGPL-3.0-only
use super::*;
use std::sync::{Arc, Mutex};

// Scale key count and bytes together. Full-retention memory and RPC behavior
// still require the separate deployment benchmark in tests/k6/README.md.
#[tokio::test]
#[ignore = "run explicitly with --release --ignored --nocapture"]
async fn signature_membership_latency_and_false_positives() {
    let index = Arc::new(KeyIndex {
        state: Mutex::default(),
        allocation: tokio::sync::Mutex::default(),
        width: 50_000,
        quota: 225_000,
        max_entries: 80,
    });
    for partition in 0..80 {
        let token = index
            .ensure_signature_partition(partition, || true)
            .await
            .unwrap();
        let tokens = BTreeMap::from([(partition, token)]);
        for first in (0u64..50_000).step_by(UPDATE_BATCH) {
            let batch: Vec<_> = (first..(first + UPDATE_BATCH as u64).min(50_000))
                .map(|offset| {
                    let key = partition * 50_000 + offset;
                    (key, SignatureHash::new(&key.to_le_bytes()))
                })
                .collect();
            index.insert_signature_hashes(&tokens, &batch).unwrap();
        }
        for offset in 0..1000 {
            let key = partition * 50_000 + offset;
            assert_eq!(
                index
                    .signature_candidates(
                        partition,
                        partition,
                        SignatureHash::new(&key.to_le_bytes())
                    )
                    .outcome(),
                "possible"
            );
        }
    }
    let (p99, false_positives) = measure(&index);
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = concurrent_updates(index.clone(), stop.clone());
    let (contended_p99, _) = measure(&index);
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    writer.join().unwrap();
    eprintln!(
        "80 partitions, 4M keys, 24 bits/key: p99={p99:?}, p99 with continuous 128-key updates={contended_p99:?}, aggregate false positives={false_positives}/20000"
    );
    assert!(
        false_positives <= 200,
        "aggregate false positives exceeded 1%"
    );
    assert!(p99 < Duration::from_micros(100));
    assert!(contended_p99 < Duration::from_micros(100));
}

fn measure(index: &KeyIndex) -> (Duration, usize) {
    let mut times = Vec::with_capacity(20_000);
    let mut false_positives = 0;
    for key in 4_000_000u64..4_020_000 {
        let started = std::time::Instant::now();
        let result = index.signature_candidates(0, 79, SignatureHash::new(&key.to_le_bytes()));
        times.push(started.elapsed());
        false_positives += usize::from(!result.partitions.is_empty());
    }
    times.sort_unstable();
    (times[19_799], false_positives)
}

fn concurrent_updates(
    index: Arc<KeyIndex>,
    stop: Arc<std::sync::atomic::AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let token = index
            .state
            .lock()
            .unwrap()
            .signatures
            .get(&79)
            .unwrap()
            .token;
        let tokens = BTreeMap::from([(79, token)]);
        let batch: Vec<_> = (3_950_000u64..3_950_128)
            .map(|key| (key, SignatureHash::new(&key.to_le_bytes())))
            .collect();
        while !stop.load(std::sync::atomic::Ordering::Relaxed) {
            index.insert_signature_hashes(&tokens, &batch).unwrap();
            std::thread::yield_now();
        }
    })
}

fn index() -> Arc<KeyIndex> {
    Arc::new(KeyIndex {
        state: Mutex::default(),
        allocation: tokio::sync::Mutex::default(),
        width: 10,
        quota: 1536,
        max_entries: 4,
    })
}
fn candidates(index: &KeyIndex, key: &[u8]) -> SignatureCandidates {
    index.signature_candidates(1, 1, SignatureHash::new(key))
}
async fn insert(index: &KeyIndex, key: &[u8]) -> u64 {
    let token = index.ensure_signature_partition(1, || true).await.unwrap();
    index
        .insert_signature_hashes(
            &BTreeMap::from([(1, token)]),
            &[(15, SignatureHash::new(key))],
        )
        .unwrap();
    token
}

#[tokio::test]
async fn appends_preserve_complete_filters_and_partial_eviction_keeps_bits() {
    let index = index();
    insert(&index, b"old").await;
    let mut fill = index.mutation(16, 18).signature_fill(false);
    assert_eq!(candidates(&index, b"absent").outcome(), "absent");
    insert(&index, b"new").await;
    fill.finish_signatures(true);
    drop(fill);
    index.evict_signatures(17);
    assert_eq!(candidates(&index, b"new").outcome(), "possible");
    assert_eq!(candidates(&index, b"old").outcome(), "possible");
    assert_eq!(candidates(&index, b"absent").outcome(), "absent");
    index.evict_signatures(20);
    assert_eq!(candidates(&index, b"old").outcome(), "unknown");
}

#[tokio::test]
async fn repairs_are_unknown_until_their_delta_is_applied() {
    let index = index();
    let token = insert(&index, b"old").await;
    let mut repair = index.mutation(15, 15).signature_fill(true);
    index.finish_signature_build(1, token);
    assert_eq!(candidates(&index, b"new").outcome(), "unknown");
    assert_eq!(index.signature_completeness(1, 1), (0, 1));
    insert(&index, b"new").await;
    repair.finish_signatures(true);
    assert_eq!(candidates(&index, b"new").outcome(), "possible");
    assert_eq!(candidates(&index, b"absent").outcome(), "absent");
}

#[tokio::test]
async fn cancelled_or_failed_fills_reject_stale_builds_even_for_empty_results() {
    let index = index();
    let token = insert(&index, b"old").await;
    drop(index.mutation(15, 15).signature_fill(false));
    index.finish_signature_build(1, token);
    assert_eq!(candidates(&index, b"absent").outcome(), "unknown");
    assert!(
        index
            .insert_signature_hashes(&BTreeMap::from([(1, token)]), &[])
            .is_err()
    );
    let token = insert(&index, b"new").await;
    let mut fill = index.mutation(16, 16).signature_fill(false);
    fill.finish_signatures(false);
    index.finish_signature_build(1, token);
    assert_eq!(candidates(&index, b"new").outcome(), "unknown");
    let token = insert(&index, b"new").await;
    index.finish_signature_build(1, token);
    assert_eq!(candidates(&index, b"old").outcome(), "possible");
}

#[tokio::test]
async fn reset_rejects_allocations_and_builds_from_the_old_generation() {
    let index = index();
    assert!(
        index
            .ensure_signature_partition(1, || {
                index.clear_signatures();
                true
            })
            .await
            .is_none()
    );
    let token = insert(&index, b"old").await;
    let reset = index.mutation(0, u64::MAX);
    index.clear_signatures();
    assert!(index.ensure_signature_partition(1, || true).await.is_none());
    drop(reset);
    index.ensure_signature_partition(1, || false).await.unwrap();
    index.finish_signature_build(1, token);
    assert_eq!(candidates(&index, b"old").outcome(), "unknown");
}

#[tokio::test]
async fn edges_holes_and_budget_exhaustion_remain_conservative() {
    let index = index();
    for partition in 1..=4 {
        index
            .ensure_signature_partition(partition, || true)
            .await
            .unwrap();
    }
    assert_eq!(
        index
            .signature_candidates(1, 4, SignatureHash::new(b"absent"))
            .outcome(),
        "absent"
    );
    assert!(index.ensure_signature_partition(5, || true).await.is_none());
    assert_eq!(
        index
            .signature_candidates(1, 5, SignatureHash::new(b"absent"))
            .partitions,
        [5]
    );
    let guards: Vec<_> = (0..129)
        .map(|_| index.mutation(10, 19).signature_fill(false))
        .collect();
    assert_eq!(candidates(&index, b"absent").outcome(), "unknown");
    assert!(index.ensure_signature_partition(1, || true).await.is_none());
    drop(guards);
}

#[tokio::test]
async fn signature_retries_progress_while_address_build_is_stalled() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::{Notify, broadcast};

    let index = index();
    insert(&index, b"old").await;
    let entered = Arc::new(Notify::new());
    let released = Arc::new(Notify::new());
    let recovered = Arc::new(Notify::new());
    let invalidated = Arc::new(AtomicBool::new(false));
    let (shutdown, receiver) = broadcast::channel(1);
    let worker = {
        let (index, entered, released, recovered, invalidated) = (
            index.clone(),
            entered.clone(),
            released.clone(),
            recovered.clone(),
            invalidated.clone(),
        );
        tokio::spawn(async move {
            super::super::run_workers(
                || async {
                    if invalidated.load(Ordering::Relaxed) {
                        let token = insert(&index, b"new").await;
                        assert!(index.finish_signature_build(1, token));
                        recovered.notify_one();
                    }
                },
                || async {
                    entered.notify_one();
                    released.notified().await;
                },
                receiver,
            )
            .await;
        })
    };
    entered.notified().await;
    drop(index.mutation(15, 15).signature_fill(false));
    assert_eq!(candidates(&index, b"absent").outcome(), "unknown");
    invalidated.store(true, Ordering::Relaxed);
    tokio::time::timeout(Duration::from_secs(8), recovered.notified())
        .await
        .expect("signature retry must not wait for the stalled address scan");
    assert_eq!(candidates(&index, b"absent").outcome(), "absent");
    assert_eq!(candidates(&index, b"new").outcome(), "possible");
    // Leave the address future stalled: shutdown must cancel it, too.
    shutdown.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), worker)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn failed_build_retries_without_publishing_partial_keys() {
    let index = index();
    let old = index.ensure_signature_partition(1, || false).await.unwrap();
    index
        .insert_signature_hashes(
            &BTreeMap::from([(1, old)]),
            &[(15, SignatureHash::new(b"first"))],
        )
        .unwrap();
    assert_eq!(candidates(&index, b"missing").outcome(), "unknown");
    // A failed scan leaves its partial bits conservative and reusable.
    let retry = index.ensure_signature_partition(1, || false).await.unwrap();
    index
        .insert_signature_hashes(
            &BTreeMap::from([(1, retry)]),
            &[(16, SignatureHash::new(b"second"))],
        )
        .unwrap();
    assert!(index.finish_signature_build(1, retry));
    assert_eq!(candidates(&index, b"first").outcome(), "possible");
    assert_eq!(candidates(&index, b"second").outcome(), "possible");
    drop(index.mutation(15, 15).signature_fill(false));
    assert!(!index.finish_signature_build(1, old));
    assert!(!index.signature_generation_matches(1, old));
}

#[test]
fn concurrent_builds_fit_the_existing_spare_partition_budget() {
    let cfg = crate::disk_cache::key_tests::config(String::new(), String::new());
    let index = KeyIndex::new(&cfg);
    // Address builds are reserved by entries; one signature allocation can be
    // outside the map. Both shares together fit the existing spare allowance.
    let partitions = index.max_entries as u64;
    let bitmaps = (partitions + 1) * (index.address_quota() + index.signature_quota()) as u64;
    let metadata = (partitions + 1) * super::super::ENTRY_OVERHEAD;
    assert!(bitmaps + metadata + super::super::RESERVE <= cfg.key_index_max_memory_bytes);
}
