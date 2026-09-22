// SPDX-License-Identifier: AGPL-3.0-only
use super::*;
use axum::{Router, extract::Query};
use std::collections::HashMap;

struct Backend {
    state: Arc<AppState>,
    queries: Arc<Mutex<Vec<String>>>,
    server: tokio::task::JoinHandle<()>,
}
impl Backend {
    async fn new(slots: Vec<u64>, fail: bool) -> Self {
        Self::with_options(slots, fail.then_some(0), Duration::ZERO).await
    }
    async fn with_options(slots: Vec<u64>, fail_after: Option<usize>, delay: Duration) -> Self {
        let queries = Arc::new(Mutex::new(Vec::new()));
        let observed = queries.clone();
        let app = Router::new().fallback(
            move |Query(params): Query<HashMap<String, String>>, body: Bytes| {
                let queries = observed.clone();
                let slots = slots.clone();
                async move {
                    let sql = if body.is_empty() {
                        params.get("query").cloned().unwrap_or_default()
                    } else {
                        String::from_utf8(body.to_vec()).unwrap()
                    };
                    if let Some(reply) = super::latest_slot::cancellation_response(&sql, false) {
                        return (StatusCode::OK, reply);
                    }
                    let count = {
                        let mut queries = queries.lock().unwrap();
                        queries.push(sql.clone());
                        queries.len()
                    };
                    tokio::time::sleep(delay).await;
                    if fail_after.is_some_and(|allowed| count > allowed) {
                        return (
                            StatusCode::SERVICE_UNAVAILABLE,
                            b"injected failure".to_vec(),
                        );
                    }
                    (StatusCode::OK, reply_rows(&sql, &slots))
                }
            },
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut state = super::test_state_with_clickhouse_url(&url);
        let client = &mut Arc::get_mut(&mut state).unwrap().clickhouse;
        client.set_http_client_for_tests(
            client
                .client
                .clone()
                .with_compression(clickhouse::Compression::None)
                .with_validation(false),
        );
        client.initialize_read_cancellation().await.unwrap();
        Self {
            state,
            queries,
            server,
        }
    }
    async fn request(&self, params: Vec<Value>) -> super::JsonRpcResponse {
        let response = handle_get_blocks(self.state.clone(), json!(1), Some(params))
            .await
            .unwrap();
        parse_json_rpc_response(response).await
    }
    fn queries(&self) -> Vec<String> {
        self.queries.lock().unwrap().clone()
    }
}
impl Drop for Backend {
    fn drop(&mut self) {
        self.server.abort();
    }
}

fn reply_rows(sql: &str, slots: &[u64]) -> Vec<u8> {
    if sql.contains("ORDER BY slot DESC LIMIT 1") {
        return slots.last().unwrap_or(&0).to_le_bytes().to_vec();
    }
    let after = sql.split("slot BETWEEN ").nth(1).expect("range query");
    let mut bounds = after.split_whitespace();
    let start = bounds.next().unwrap().parse::<u64>().unwrap();
    assert_eq!(bounds.next(), Some("AND"));
    let end = bounds.next().unwrap().parse::<u64>().unwrap();
    let selected = slots
        .iter()
        .copied()
        .filter(|slot| (start..=end).contains(slot))
        .collect::<Vec<_>>();
    let mut body = Vec::new();
    if sql.contains("SELECT slot, status") {
        for slot in start..=end {
            body.extend(slot.to_le_bytes());
            body.push(if selected.contains(&slot) { 1 } else { 2 });
        }
    } else {
        assert!(selected.len() < 128);
        body.push(selected.len() as u8);
        for slot in selected {
            body.extend(slot.to_le_bytes());
        }
    }
    body
}

#[cfg(feature = "grpc-head-cache")]
pub(super) fn seed_head(cache: &HeadCache, slots: &[u64], commitment: CommitmentLevel) {
    use crate::head_cache::coverage::Link;
    let mut proof = cache.coverage.write().unwrap();
    proof.connect();
    let mut parent = slots[0] - 1;
    for &slot in slots {
        proof.metadata(Link {
            slot,
            parent,
            hash: [slot as u8; 32],
            parent_hash: [parent as u8; 32],
        });
        proof.observe(slot, commitment, std::time::Instant::now());
        proof.publish(slot, commitment);
        parent = slot;
    }
}
#[cfg(feature = "grpc-head-cache")]
fn with_head(backend: &mut Backend, slots: &[u64], commitment: CommitmentLevel) -> Arc<HeadCache> {
    let cache = Arc::new(HeadCache::new(32, 1024));
    seed_head(&cache, slots, commitment);
    Arc::get_mut(&mut backend.state).unwrap().head_cache = Some(cache.clone());
    cache
}
#[cfg(feature = "disk-cache")]
fn with_disk(backend: &mut Backend, index: Option<(u64, u64, &[u64])>, intervals: &[(u64, u64)]) {
    let index = index.map(|(a, b, slots)| {
        crate::disk_cache::block_index::BlockIndex::for_range_tests(a, b, slots)
    });
    let disk = crate::disk_cache::DiskCache::for_range_tests(
        backend.state.clickhouse.clone(),
        index,
        intervals,
    );
    Arc::get_mut(&mut backend.state).unwrap().disk_cache = Some(Arc::new(
        tokio::sync::OnceCell::new_with(Some(Arc::new(disk))),
    ));
}

#[tokio::test]
async fn primary_range_failure_is_an_error() {
    let backend = Backend::new(vec![], true).await;
    assert_eq!(
        backend
            .request(vec![json!(10), json!(20)])
            .await
            .error
            .unwrap()
            .code,
        -32603
    );
    assert_eq!(backend.queries().len(), 1);
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn head_only_and_omitted_end_never_query_primary() {
    let mut backend = Backend::new(vec![], true).await;
    with_head(&mut backend, &[10, 12, 15], CommitmentLevel::Finalized);
    for params in [
        vec![json!(10), json!(15)],
        vec![json!(10)],
        vec![json!(11), json!(11)],
    ] {
        let response = backend.request(params.clone()).await;
        assert!(response.error.is_none());
        let expected = if params == vec![json!(11), json!(11)] {
            json!([])
        } else {
            json!([10, 12, 15])
        };
        assert_eq!(response.result, Some(expected));
    }
    assert!(backend.queries().is_empty());
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn disconnected_and_missing_commitment_never_refresh_latest() {
    let mut backend = Backend::new(vec![99], false).await;
    let head = with_head(&mut backend, &[10, 11], CommitmentLevel::Confirmed);
    assert!(backend.request(vec![json!(10)]).await.error.is_some());
    head.coverage.write().unwrap().disconnect();
    assert!(
        backend
            .request(vec![json!(10), json!({"commitment":"confirmed"})])
            .await
            .error
            .is_some()
    );
    assert!(backend.queries().is_empty());
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn head_tail_does_not_hide_missing_middle_or_primary_failure() {
    let mut backend = Backend::new(vec![], true).await;
    let head = with_head(&mut backend, &[10], CommitmentLevel::Finalized);
    {
        let mut proof = head.coverage.write().unwrap();
        proof.metadata(crate::head_cache::coverage::Link {
            slot: 12,
            parent: 11,
            hash: [12; 32],
            parent_hash: [11; 32],
        });
        proof.observe(12, CommitmentLevel::Finalized, std::time::Instant::now());
        proof.publish(12, CommitmentLevel::Finalized);
    }
    assert!(
        backend
            .request(vec![json!(10), json!(12)])
            .await
            .error
            .is_some()
    );
    let queries = backend.queries();
    assert_eq!(queries.len(), 1);
    assert!(queries[0].contains("slot BETWEEN 10 AND 11"));
}

#[cfg(feature = "disk-cache")]
#[tokio::test]
async fn index_only_skips_disk_and_primary() {
    let mut backend = Backend::new(vec![], true).await;
    with_disk(&mut backend, Some((10, 15, &[10, 12, 15])), &[(10, 15)]);
    let response = backend.request(vec![json!(10), json!(15)]).await;
    assert_eq!(response.result, Some(json!([10, 12, 15])));
    assert!(backend.queries().is_empty());
}

#[cfg(all(feature = "disk-cache", feature = "grpc-head-cache"))]
#[tokio::test]
async fn combined_index_head_covers_range_without_any_queries() {
    let mut backend = Backend::new(vec![], true).await;
    with_disk(&mut backend, Some((1, 10, &[1, 5, 10])), &[(1, 10)]);
    with_head(&mut backend, &[10, 12, 15], CommitmentLevel::Finalized);
    assert_eq!(
        backend.request(vec![json!(1)]).await.result,
        Some(json!([1, 5, 10, 12, 15]))
    );
    assert!(backend.queries().is_empty());
}

#[cfg(all(feature = "disk-cache", feature = "grpc-head-cache"))]
#[tokio::test]
async fn disjoint_proofs_query_only_the_gap() {
    let mut backend = Backend::new(vec![6, 8, 9], false).await;
    with_disk(&mut backend, Some((1, 5, &[1, 5])), &[]);
    with_head(&mut backend, &[10, 12], CommitmentLevel::Finalized);
    assert_eq!(
        backend.request(vec![json!(1), json!(12)]).await.result,
        Some(json!([1, 5, 6, 8, 9, 10, 12]))
    );
    let queries = backend.queries();
    assert_eq!(queries.len(), 1);
    assert!(queries[0].contains("slot BETWEEN 6 AND 9"));
}

#[cfg(feature = "disk-cache")]
#[tokio::test]
async fn disk_coverage_reads_statuses_and_skips_primary() {
    let mut backend = Backend::new(vec![10, 13], false).await;
    with_disk(&mut backend, None, &[(10, 13)]);
    assert_eq!(
        backend.request(vec![json!(10), json!(13)]).await.result,
        Some(json!([10, 13]))
    );
    let queries = backend.queries();
    assert_eq!(queries.len(), 1);
    assert!(queries[0].starts_with("SELECT slot, status"));
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_blocks_stale_tip_errors_without_querying_primary() {
    let mut backend = Backend::new(vec![999], false).await;
    let head = Arc::new(HeadCache::new(32, 1024));
    {
        let mut proof = head.coverage.write().unwrap();
        proof.connect();
        let old = std::time::Instant::now() - Duration::from_secs(2);
        for slot in [10, 11] {
            proof.metadata(crate::head_cache::coverage::Link {
                slot,
                parent: slot - 1,
                hash: [slot as u8; 32],
                parent_hash: [(slot - 1) as u8; 32],
            });
            proof.observe(slot, CommitmentLevel::Finalized, old);
            proof.publish(slot, CommitmentLevel::Finalized);
        }
    }
    Arc::get_mut(&mut backend.state).unwrap().head_cache = Some(head);
    assert!(backend.request(vec![json!(10)]).await.error.is_some());
    // Historical proof remains usable while the connection is live, even if tip discovery is stale.
    assert_eq!(
        backend.request(vec![json!(10), json!(11)]).await.result,
        Some(json!([10, 11]))
    );
    assert!(backend.queries().is_empty());
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_blocks_with_limit_preserves_skipped_slot_window() {
    let mut backend = Backend::new(vec![], true).await;
    with_head(&mut backend, &[10, 12, 15], CommitmentLevel::Finalized);
    let response = handle_get_blocks_with_limit(
        backend.state.clone(),
        json!(1),
        Some(vec![json!(10), json!(3)]),
    )
    .await
    .unwrap();
    assert_eq!(
        parse_json_rpc_response(response).await.result,
        Some(json!([10, 12]))
    );
    assert!(backend.queries().is_empty());
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_blocks_both_primary_gaps_merge_with_local_slots() {
    let mut backend = Backend::new(vec![1, 5, 15, 20], false).await;
    with_head(&mut backend, &[10, 12], CommitmentLevel::Finalized);
    assert_eq!(
        backend.request(vec![json!(1), json!(20)]).await.result,
        Some(json!([1, 5, 10, 12, 15, 20]))
    );
    let queries = backend.queries();
    assert_eq!(queries.len(), 2);
    assert!(queries[0].contains("slot BETWEEN 1 AND 9"));
    assert!(queries[1].contains("slot BETWEEN 13 AND 20"));
}

#[tokio::test]
async fn get_blocks_disabled_head_uses_primary_latest_cache() {
    let backend = Backend::new(vec![10, 11], false).await;
    backend
        .state
        .latest_slot_cache
        .last_updated_ms
        .store(0, Ordering::Relaxed);
    assert_eq!(
        backend.request(vec![json!(10)]).await.result,
        Some(json!([10, 11]))
    );
    let queries = backend.queries();
    assert_eq!(queries.len(), 2);
    assert!(queries[0].contains("ORDER BY slot DESC LIMIT 1"));
}

#[cfg(feature = "grpc-head-cache")]
#[tokio::test]
async fn get_blocks_second_gap_failure_cannot_return_partial_success() {
    let mut backend = Backend::with_options(vec![1, 20], Some(1), Duration::ZERO).await;
    with_head(&mut backend, &[10, 12], CommitmentLevel::Finalized);
    let response = backend.request(vec![json!(1), json!(20)]).await;
    assert!(response.result.is_none());
    assert_eq!(response.error.unwrap().code, -32603);
    assert_eq!(backend.queries().len(), 2);
}

#[tokio::test]
async fn get_blocks_downstream_time_includes_latest_and_range_reads() {
    let backend = Backend::with_options(vec![10, 11], None, Duration::from_millis(25)).await;
    backend
        .state
        .latest_slot_cache
        .last_updated_ms
        .store(0, Ordering::Relaxed);
    let response = handle_get_blocks(backend.state.clone(), json!(1), Some(vec![json!(10)]))
        .await
        .unwrap();
    let timings = crate::util::extract_downstream_timings(&response).unwrap();
    assert!(
        timings.elapsed_ms >= 50,
        "both delayed reads must be counted: {timings:?}"
    );
    assert!(parse_json_rpc_response(response).await.error.is_none());
    assert_eq!(backend.queries().len(), 2);
}

#[cfg(all(feature = "disk-cache", feature = "grpc-head-cache"))]
#[tokio::test]
async fn conflicting_index_and_head_invalidate_the_whole_chain() {
    let mut backend = Backend::new(vec![10, 11, 12], false).await;
    with_disk(&mut backend, Some((10, 11, &[10, 11])), &[]);
    with_head(&mut backend, &[10, 12], CommitmentLevel::Finalized);
    assert!(backend.request(vec![json!(10)]).await.error.is_some());
    assert!(
        backend.queries().is_empty(),
        "an untrusted authoritative tip must not refresh primary"
    );
    assert_eq!(
        backend.request(vec![json!(10), json!(12)]).await.result,
        Some(json!([10, 11, 12]))
    );
    let queries = backend.queries();
    assert_eq!(queries.len(), 1);
    assert!(queries[0].contains("slot BETWEEN 10 AND 12"));
}
