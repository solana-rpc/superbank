// SPDX-License-Identifier: AGPL-3.0-only
//! Short-lived proofs that the primary held no status for a signature.
//!
//! getSignatureStatuses with `searchTransactionHistory` asks the primary for every
//! signature that neither the head cache nor the local disk cache holds, and clients
//! poll the same unlanded signatures repeatedly. The primary search spans all history,
//! so an empty answer at time T proves the signature is absent from every slot the
//! primary held at T. Older history is immutable; a later landing can only appear in
//! newer slots, which the local tiers cover as long as they stay gapless. The handler
//! therefore trusts an entry only while the disk cache still holds every slot after
//! the tip it covered at T and the head cache extends that coverage to the chain tip.

use crate::solana_sdk::signature::Signature;
use moka::future::Cache;
use std::time::{Duration, Instant};

/// Budgeted resident bytes per entry (64-byte key, 16-byte value, and the cache's
/// per-entry bookkeeping, measured near 330 bytes with a TTL), used to turn the byte
/// bound into an entry bound.
pub(crate) const ENTRY_BYTES: u64 = 384;
/// Absences older than this are never served, whatever the configured TTL.
pub(crate) const MAX_TTL: Duration = Duration::from_secs(300);

#[derive(Clone, Copy, Debug)]
struct Absent {
    at: Instant,
    /// Newest slot of the contiguous disk coverage that proved the signature absent
    /// when the primary answered: the primary held every slot up to here (the disk
    /// cache is filled from it, and older history is immutable).
    disk_tip: u64,
}

#[derive(Clone)]
pub(crate) struct StatusHistoryCache {
    inner: Option<Cache<Signature, Absent>>,
    ttl: Duration,
}

// Only the handler with both local tiers consults the cache.
#[cfg_attr(
    not(all(feature = "disk-cache", feature = "grpc-head-cache")),
    allow(dead_code)
)]
impl StatusHistoryCache {
    /// Zero entries, a byte bound below one entry, or a zero TTL disables the cache.
    pub(crate) fn new(max_entries: u64, max_bytes: u64, ttl: Duration) -> Self {
        let capacity = max_entries.min(max_bytes / ENTRY_BYTES);
        let ttl = ttl.min(MAX_TTL);
        let inner = (capacity > 0 && !ttl.is_zero()).then(|| {
            Cache::builder()
                .max_capacity(capacity)
                .time_to_live(ttl)
                .build()
        });
        Self { inner, ttl }
    }

    #[cfg(test)]
    pub(crate) fn disabled() -> Self {
        Self::new(0, 0, Duration::ZERO)
    }

    pub(crate) fn enabled(&self) -> bool {
        self.inner.is_some()
    }

    /// Record that the primary returned no row for `signature` while the local cache
    /// proved it absent through `disk_tip`. Callers must only pass a complete, successful
    /// primary answer.
    pub(crate) async fn insert_absent(&self, signature: Signature, disk_tip: u64, now: Instant) {
        if let Some(cache) = self.inner.as_ref() {
            cache.insert(signature, Absent { at: now, disk_tip }).await;
        }
    }

    /// Whether a recorded primary absence still stands at `now` for a local read whose
    /// contiguous coverage starts at `disk_floor`: the entry is younger than the TTL and
    /// the disk cache still holds every slot newer than the primary held back then.
    pub(crate) async fn is_absent(
        &self,
        signature: &Signature,
        disk_floor: u64,
        now: Instant,
    ) -> bool {
        let Some(cache) = self.inner.as_ref() else {
            return false;
        };
        cache.get(signature).await.is_some_and(|entry| {
            now.saturating_duration_since(entry.at) < self.ttl
                && disk_floor <= entry.disk_tip.saturating_add(1)
        })
    }

    #[cfg(test)]
    pub(crate) async fn entry_count(&self) -> u64 {
        match self.inner.as_ref() {
            Some(cache) => {
                cache.run_pending_tasks().await;
                cache.entry_count()
            }
            None => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TTL: Duration = Duration::from_secs(60);

    fn unique_signature() -> Signature {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let mut bytes = [0; 64];
        bytes[..8].copy_from_slice(&NEXT.fetch_add(1, Ordering::Relaxed).to_le_bytes());
        Signature::from(bytes)
    }

    #[tokio::test]
    async fn disabled_by_any_zero_bound() {
        for cache in [
            StatusHistoryCache::disabled(),
            StatusHistoryCache::new(0, 1 << 20, TTL),
            StatusHistoryCache::new(1000, ENTRY_BYTES - 1, TTL),
            StatusHistoryCache::new(1000, 1 << 20, Duration::ZERO),
        ] {
            assert!(!cache.enabled());
            let signature = unique_signature();
            let now = Instant::now();
            cache.insert_absent(signature, 100, now).await;
            assert!(!cache.is_absent(&signature, 50, now).await);
        }
    }

    #[tokio::test]
    async fn fresh_entry_hits_until_the_ttl() {
        let cache = StatusHistoryCache::new(1000, 1 << 20, TTL);
        let signature = unique_signature();
        let now = Instant::now();
        assert!(
            !cache.is_absent(&signature, 50, now).await,
            "never inserted"
        );
        cache.insert_absent(signature, 100, now).await;
        assert!(cache.is_absent(&signature, 50, now).await);
        assert!(
            cache
                .is_absent(&signature, 50, now + TTL - Duration::from_millis(1))
                .await
        );
        assert!(!cache.is_absent(&signature, 50, now + TTL).await, "expired");
        assert!(!cache.is_absent(&unique_signature(), 50, now).await);
        // A newer primary answer renews the proof.
        cache.insert_absent(signature, 120, now + TTL).await;
        assert!(cache.is_absent(&signature, 50, now + TTL).await);
    }

    #[tokio::test]
    async fn eviction_past_the_recorded_tip_invalidates() {
        let cache = StatusHistoryCache::new(1000, 1 << 20, TTL);
        let signature = unique_signature();
        let now = Instant::now();
        cache.insert_absent(signature, 100, now).await;
        // Slot 101 onward is still local: nothing the primary lacked can hide.
        assert!(cache.is_absent(&signature, 101, now).await);
        // Slot 101 left the disk cache: a landing there would be invisible.
        assert!(!cache.is_absent(&signature, 102, now).await);
    }

    #[tokio::test]
    async fn ttl_is_capped() {
        let cache = StatusHistoryCache::new(1000, 1 << 20, Duration::from_secs(3600));
        let signature = unique_signature();
        let now = Instant::now();
        cache.insert_absent(signature, 100, now).await;
        assert!(!cache.is_absent(&signature, 50, now + MAX_TTL).await);
    }

    #[tokio::test]
    async fn entries_are_bounded_by_count_and_bytes() {
        let now = Instant::now();
        for cache in [
            StatusHistoryCache::new(8, 1 << 20, TTL),
            StatusHistoryCache::new(1 << 20, 8 * ENTRY_BYTES, TTL),
        ] {
            for _ in 0..64 {
                cache.insert_absent(unique_signature(), 1, now).await;
            }
            let entries = cache.entry_count().await;
            assert!(entries <= 8, "{entries}");
        }
    }
}
