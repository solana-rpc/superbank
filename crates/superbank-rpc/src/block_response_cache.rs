// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::body::Bytes;
use moka::future::Cache;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(crate) struct BlockResponseCacheKey {
    pub(crate) slot: u64,
    pub(crate) encoding: u8,
    pub(crate) transaction_details: u8,
    pub(crate) show_rewards: bool,
    pub(crate) max_supported_transaction_version: Option<u8>,
}

#[derive(Clone)]
pub(crate) struct BlockResponseCache {
    inner: Option<Cache<BlockResponseCacheKey, Bytes>>,
    max_bytes: u64,
}

impl BlockResponseCache {
    pub(crate) fn new(max_bytes: u64) -> Self {
        let inner = (max_bytes > 0).then(|| {
            Cache::builder()
                .max_capacity(max_bytes)
                .weigher(|_key: &BlockResponseCacheKey, value: &Bytes| {
                    value.len().try_into().unwrap_or(u32::MAX)
                })
                .build()
        });
        let cache = Self { inner, max_bytes };
        cache.publish_metrics();
        cache
    }

    pub(crate) fn enabled(&self) -> bool {
        self.inner.is_some()
    }

    pub(crate) fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    pub(crate) async fn get(&self, key: &BlockResponseCacheKey) -> Option<Bytes> {
        let cache = self.inner.as_ref()?;
        let value = cache.get(key).await;
        crate::metrics::get_block_response_cache_access(if value.is_some() {
            "hit"
        } else {
            "miss"
        });
        self.publish_metrics();
        value
    }

    pub(crate) async fn get_or_try_insert_with<F, E>(
        &self,
        key: BlockResponseCacheKey,
        init: F,
    ) -> Result<(Bytes, bool), Arc<E>>
    where
        F: Future<Output = Result<Bytes, E>>,
        E: Send + Sync + 'static,
    {
        let Some(cache) = self.inner.as_ref() else {
            return init.await.map(|value| (value, true)).map_err(Arc::new);
        };
        let evaluated = Arc::new(AtomicBool::new(false));
        let evaluated_by_this_request = evaluated.clone();
        let value = cache
            .try_get_with(key, async move {
                evaluated_by_this_request.store(true, Ordering::Relaxed);
                init.await
            })
            .await?;
        let evaluated = evaluated.load(Ordering::Relaxed);
        crate::metrics::get_block_response_cache_access(if evaluated {
            "insert"
        } else {
            "coalesced"
        });
        if evaluated {
            cache.run_pending_tasks().await;
        }
        self.publish_metrics();
        Ok((value, evaluated))
    }

    pub(crate) fn entry_count(&self) -> u64 {
        self.inner.as_ref().map_or(0, Cache::entry_count)
    }

    pub(crate) fn weighted_size(&self) -> u64 {
        self.inner.as_ref().map_or(0, Cache::weighted_size)
    }

    fn publish_metrics(&self) {
        crate::metrics::get_block_response_cache_state(
            self.entry_count(),
            self.weighted_size(),
            self.max_bytes(),
        );
    }

    #[cfg(test)]
    pub(crate) async fn run_pending_tasks(&self) {
        if let Some(cache) = self.inner.as_ref() {
            cache.run_pending_tasks().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    fn key(slot: u64) -> BlockResponseCacheKey {
        BlockResponseCacheKey {
            slot,
            encoding: 0,
            transaction_details: 3,
            show_rewards: false,
            max_supported_transaction_version: Some(0),
        }
    }

    #[tokio::test]
    async fn disabled_cache_evaluates_every_request() {
        let cache = BlockResponseCache::new(0);
        let (value, evaluated) = cache
            .get_or_try_insert_with(key(1), async { Ok::<_, ()>(Bytes::from_static(b"one")) })
            .await
            .expect("value");
        assert_eq!(value, Bytes::from_static(b"one"));
        assert!(evaluated);
        assert!(cache.get(&key(1)).await.is_none());
    }

    #[tokio::test]
    async fn enabled_cache_reuses_serialized_result() {
        let cache = BlockResponseCache::new(1024);
        let (_, first_evaluated) = cache
            .get_or_try_insert_with(key(1), async { Ok::<_, ()>(Bytes::from_static(b"one")) })
            .await
            .expect("first value");
        let (_, second_evaluated) = cache
            .get_or_try_insert_with(key(1), async { Ok::<_, ()>(Bytes::from_static(b"two")) })
            .await
            .expect("second value");
        assert!(first_evaluated);
        assert!(!second_evaluated);
        assert_eq!(cache.get(&key(1)).await, Some(Bytes::from_static(b"one")));
    }

    #[tokio::test]
    async fn concurrent_misses_are_coalesced() {
        let cache = BlockResponseCache::new(1024);
        let evaluations = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let cache = cache.clone();
            let evaluations = evaluations.clone();
            tasks.push(tokio::spawn(async move {
                cache
                    .get_or_try_insert_with(key(2), async move {
                        evaluations.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        Ok::<_, ()>(Bytes::from_static(b"shared"))
                    })
                    .await
                    .expect("value")
            }));
        }

        let mut evaluated_count = 0;
        for task in tasks {
            let (value, evaluated) = task.await.expect("task");
            assert_eq!(value, Bytes::from_static(b"shared"));
            evaluated_count += usize::from(evaluated);
        }
        assert_eq!(evaluations.load(Ordering::Relaxed), 1);
        assert_eq!(evaluated_count, 1);
    }

    #[tokio::test]
    async fn failed_values_are_not_cached() {
        let cache = BlockResponseCache::new(1024);
        let result = cache
            .get_or_try_insert_with(key(3), async { Err::<Bytes, _>("failed") })
            .await;
        assert!(result.is_err());
        assert!(cache.get(&key(3)).await.is_none());
    }
}
