// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::futures::OwnedNotified;
use tokio::sync::{Mutex, Notify};

use super::types::SignatureSlot;

pub(crate) type SignatureBytes = [u8; 64];

const MAX_CACHE_CAPACITY: usize = 1_000_000;

#[derive(Debug)]
pub(crate) enum CacheStart<T> {
    Hit(T),
    Wait(OwnedNotified),
    Leader(SignatureSlotCacheLeader),
}

#[derive(Debug)]
enum Entry<T> {
    Ready { value: T, expires_at: Instant },
    InFlight { notify: Arc<Notify> },
}

#[derive(Debug)]
pub(crate) struct SignatureSlotCache {
    ttl_found: Duration,
    ttl_missing: Duration,
    capacity: usize,
    inner: Mutex<HashMap<SignatureBytes, Entry<Option<SignatureSlot>>>>,
}

#[derive(Debug)]
pub(crate) struct SignatureSlotCacheLeader {
    cache: Arc<SignatureSlotCache>,
    key: SignatureBytes,
    notify: Arc<Notify>,
    armed: bool,
}

impl SignatureSlotCacheLeader {
    pub(crate) async fn finish(mut self, value: Option<SignatureSlot>) {
        self.cache
            .finish_leader(self.key, self.notify.clone(), value)
            .await;
        self.armed = false;
    }

    pub(crate) async fn fail(mut self) {
        self.cache
            .remove_in_flight(self.key, self.notify.clone())
            .await;
        self.armed = false;
    }
}

impl Drop for SignatureSlotCacheLeader {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }

        let cache = self.cache.clone();
        let key = self.key;
        let notify = self.notify.clone();

        // Cancellation normally happens while polling on a Tokio runtime. Avoid scheduling when
        // the cache lock is immediately available, but fall back to an async cleanup task if a
        // concurrent cache operation owns it.
        if let Ok(mut guard) = cache.inner.try_lock() {
            SignatureSlotCache::remove_matching_entry(&mut guard, key, &notify);
            drop(guard);
            notify.notify_waiters();
            return;
        }

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                cache.remove_in_flight(key, notify).await;
            });
        } else {
            // There can be no active async waiter without a runtime. Wake any retained waiter so
            // runtime teardown does not leave it blocked on this notification.
            notify.notify_waiters();
        }
    }
}

impl SignatureSlotCache {
    pub(crate) fn new(ttl_found: Duration, ttl_missing: Duration, capacity: usize) -> Self {
        // Ensure the cache is always bounded even if misconfigured.
        let capacity = capacity.clamp(1, MAX_CACHE_CAPACITY);
        Self {
            ttl_found,
            ttl_missing,
            capacity,
            inner: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn from_env() -> Self {
        fn env_usize(key: &str) -> Option<usize> {
            std::env::var(key)
                .ok()
                .and_then(|raw| raw.trim().parse().ok())
        }

        fn env_u64(key: &str) -> Option<u64> {
            std::env::var(key)
                .ok()
                .and_then(|raw| raw.trim().parse().ok())
        }

        // Defaults are intentionally conservative: high hit rate for hot keys, but bounded memory.
        let capacity = env_usize("SIGNATURE_SLOT_CACHE_SIZE").unwrap_or(50_000);
        let ttl_found_secs = env_u64("SIGNATURE_SLOT_CACHE_TTL_FOUND_SECS").unwrap_or(6 * 60 * 60);
        let ttl_missing_secs = env_u64("SIGNATURE_SLOT_CACHE_TTL_MISSING_SECS").unwrap_or(1);

        let ttl_found = Duration::from_secs(ttl_found_secs);
        let ttl_missing = Duration::from_secs(ttl_missing_secs);
        Self::new(ttl_found, ttl_missing, capacity)
    }

    pub(crate) async fn get_or_start(
        self: &Arc<Self>,
        key: SignatureBytes,
    ) -> CacheStart<Option<SignatureSlot>> {
        let now = Instant::now();

        let mut guard = self.inner.lock().await;
        if let Some(entry) = guard.get(&key) {
            match entry {
                Entry::Ready { value, expires_at } => {
                    if now < *expires_at {
                        // NOTE: `SignatureSlot` is `Copy`, so `Option<SignatureSlot>` is `Copy` too.
                        return CacheStart::Hit(*value);
                    }
                }
                Entry::InFlight { notify } => {
                    // Create the wait future while holding the lock so we can't miss a `notify_waiters()`.
                    return CacheStart::Wait(notify.clone().notified_owned());
                }
            }
        }

        // Remove expired value (if any) and start a new in-flight leader.
        guard.remove(&key);
        let notify = Arc::new(Notify::new());
        guard.insert(
            key,
            Entry::InFlight {
                notify: notify.clone(),
            },
        );
        CacheStart::Leader(SignatureSlotCacheLeader {
            cache: self.clone(),
            key,
            notify,
            armed: true,
        })
    }

    async fn finish_leader(
        &self,
        key: SignatureBytes,
        notify: Arc<Notify>,
        value: Option<SignatureSlot>,
    ) {
        let ttl = if value.is_some() {
            self.ttl_found
        } else {
            self.ttl_missing
        };
        let expires_at = Instant::now() + ttl;

        let mut guard = self.inner.lock().await;
        if matches!(
            guard.get(&key),
            Some(Entry::InFlight { notify: current }) if Arc::ptr_eq(current, &notify)
        ) {
            guard.insert(key, Entry::Ready { value, expires_at });
            self.evict_if_needed(&mut guard);
        }
        drop(guard);

        notify.notify_waiters();
    }

    async fn remove_in_flight(&self, key: SignatureBytes, notify: Arc<Notify>) {
        let mut guard = self.inner.lock().await;
        Self::remove_matching_entry(&mut guard, key, &notify);
        drop(guard);

        notify.notify_waiters();
    }

    fn remove_matching_entry(
        guard: &mut HashMap<SignatureBytes, Entry<Option<SignatureSlot>>>,
        key: SignatureBytes,
        notify: &Arc<Notify>,
    ) {
        if let Some(Entry::InFlight { notify: current }) = guard.get(&key)
            && Arc::ptr_eq(current, notify)
        {
            guard.remove(&key);
        }
    }

    fn evict_if_needed(&self, guard: &mut HashMap<SignatureBytes, Entry<Option<SignatureSlot>>>) {
        if guard.len() <= self.capacity {
            return;
        }

        // First drop expired entries.
        let now = Instant::now();
        guard.retain(|_, entry| match entry {
            Entry::Ready { expires_at, .. } => *expires_at > now,
            Entry::InFlight { .. } => true,
        });

        if guard.len() <= self.capacity {
            return;
        }

        // Still over capacity: drop arbitrary ready entries (never in-flight), until bounded.
        let mut over_by = guard.len() - self.capacity;
        if over_by == 0 {
            return;
        }

        let keys_to_remove: Vec<SignatureBytes> = guard
            .iter()
            .filter_map(|(k, v)| match v {
                Entry::Ready { .. } => Some(*k),
                Entry::InFlight { .. } => None,
            })
            .take(over_by)
            .collect();

        for key in keys_to_remove {
            guard.remove(&key);
            over_by = over_by.saturating_sub(1);
            if over_by == 0 {
                break;
            }
        }
    }

    #[cfg(test)]
    pub(crate) async fn prime_for_tests(
        self: &Arc<Self>,
        key: SignatureBytes,
        value: Option<SignatureSlot>,
    ) {
        let leader = match self.get_or_start(key).await {
            CacheStart::Leader(leader) => leader,
            other => panic!("expected cache leader, got {other:?}"),
        };
        leader.finish(value).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn signature_slot_cache_singleflight() {
        let cache = Arc::new(SignatureSlotCache::new(
            Duration::from_secs(60),
            Duration::from_secs(1),
            128,
        ));
        let key: SignatureBytes = [9u8; 64];

        let leader = match cache.get_or_start(key).await {
            CacheStart::Leader(leader) => leader,
            other => panic!("expected leader, got {other:?}"),
        };
        let wait = match cache.get_or_start(key).await {
            CacheStart::Wait(wait) => wait,
            other => panic!("expected wait, got {other:?}"),
        };

        let cache_waiter = cache.clone();
        let waiter = tokio::spawn(async move {
            wait.await;
            match cache_waiter.get_or_start(key).await {
                CacheStart::Hit(value) => value,
                other => panic!("expected hit, got {other:?}"),
            }
        });

        leader
            .finish(Some(SignatureSlot {
                slot: 42,
                slot_idx: 7,
            }))
            .await;

        let value = waiter.await.expect("waiter join");
        let slot = value.expect("slot present");
        assert_eq!(slot.slot, 42);
        assert_eq!(slot.slot_idx, 7);
    }

    #[tokio::test]
    async fn dropped_signature_slot_leader_wakes_waiters_and_allows_retry() {
        let cache = Arc::new(SignatureSlotCache::new(
            Duration::from_secs(60),
            Duration::from_secs(1),
            128,
        ));
        let key: SignatureBytes = [7u8; 64];

        let leader = match cache.get_or_start(key).await {
            CacheStart::Leader(leader) => leader,
            other => panic!("expected cache leader, got {other:?}"),
        };
        let wait = match cache.get_or_start(key).await {
            CacheStart::Wait(wait) => wait,
            other => panic!("expected cache waiter, got {other:?}"),
        };

        // Force the cancellation guard down its contended-lock cleanup path. This is the case that
        // needs an async cleanup task instead of removing the in-flight entry directly in `Drop`.
        let cache_lock = cache.inner.lock().await;
        drop(leader);
        drop(cache_lock);
        tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .expect("cancelled leader should wake existing waiters");

        let retry_leader = match cache.get_or_start(key).await {
            CacheStart::Leader(leader) => leader,
            other => panic!("expected a new leader after cancellation, got {other:?}"),
        };
        retry_leader.fail().await;
    }

    #[tokio::test]
    async fn signature_slot_cache_expires() {
        let cache = Arc::new(SignatureSlotCache::new(
            Duration::from_millis(50),
            Duration::from_millis(50),
            128,
        ));
        let key: SignatureBytes = [1u8; 64];

        let leader = match cache.get_or_start(key).await {
            CacheStart::Leader(leader) => leader,
            other => panic!("expected leader, got {other:?}"),
        };
        leader.finish(None).await;

        match cache.get_or_start(key).await {
            CacheStart::Hit(None) => {}
            other => panic!("expected hit, got {other:?}"),
        }

        tokio::time::sleep(Duration::from_millis(80)).await;
        match cache.get_or_start(key).await {
            CacheStart::Leader(_) => {}
            other => panic!("expected leader after expiry, got {other:?}"),
        }
    }
}
