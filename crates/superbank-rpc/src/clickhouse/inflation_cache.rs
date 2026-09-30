// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! In-process cache of validated `getInflationReward` epoch metadata.
//!
//! Only facts that cannot change once observed are stored:
//! - the payout epoch boundary block (first block of the payout epoch, whose parent lies in an
//!   earlier epoch), after the same validation the uncached lookup applies;
//! - the complete partition block-height -> slot map, and only when every declared partition
//!   block exists with a unique slot.
//!
//! Lookup outcomes (`-32004` boundary unavailable, `-32017` rewards period active), errors, and
//! reward rows are never cached. Entries expire after [`INFLATION_EPOCH_CACHE_TTL`] so a metadata
//! repair on the primary is picked up without a restart.

use std::sync::Arc;
use std::time::Duration;

use moka::future::Cache;

/// Upper bound on how long a cached epoch entry is served before it is re-read from the primary.
pub(crate) const INFLATION_EPOCH_CACHE_TTL: Duration = Duration::from_secs(3_600);

/// Fixed per-entry weight covering the key, the boundary fields, and the allocations around them.
const ENTRY_OVERHEAD_BYTES: u64 = 256;

/// `(first slot of the payout epoch, first slot of the following epoch)`.
pub(crate) type InflationEpochCacheKey = (u64, u64);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct InflationBoundary {
    pub(crate) slot: u64,
    pub(crate) parent_blockhash: [u8; 32],
    pub(crate) block_height: Option<u64>,
    pub(crate) num_partitions: Option<usize>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct InflationEpochMetadata {
    pub(crate) boundary: InflationBoundary,
    /// `partition_slots[i]` is the slot of block height `boundary.block_height + 1 + i`. Present
    /// only when all `num_partitions` partition blocks were found.
    pub(crate) partition_slots: Option<Arc<[u64]>>,
}

impl InflationEpochMetadata {
    fn weight(&self) -> u32 {
        let slots = self
            .partition_slots
            .as_ref()
            .map_or(0, |slots| slots.len() as u64);
        ENTRY_OVERHEAD_BYTES
            .saturating_add(slots.saturating_mul(8))
            .try_into()
            .unwrap_or(u32::MAX)
    }
}

#[derive(Clone)]
pub(crate) struct InflationEpochCache {
    inner: Option<Cache<InflationEpochCacheKey, Arc<InflationEpochMetadata>>>,
}

impl InflationEpochCache {
    pub(crate) fn new(max_bytes: u64) -> Self {
        Self::with_ttl(max_bytes, INFLATION_EPOCH_CACHE_TTL)
    }

    pub(crate) fn with_ttl(max_bytes: u64, ttl: Duration) -> Self {
        let inner = (max_bytes > 0).then(|| {
            Cache::builder()
                .max_capacity(max_bytes)
                .time_to_live(ttl)
                .weigher(
                    |_key: &InflationEpochCacheKey, value: &Arc<InflationEpochMetadata>| {
                        value.weight()
                    },
                )
                .build()
        });
        Self { inner }
    }

    pub(crate) async fn get(
        &self,
        key: InflationEpochCacheKey,
    ) -> Option<Arc<InflationEpochMetadata>> {
        let cache = self.inner.as_ref()?;
        let value = cache.get(&key).await;
        crate::metrics::inflation_reward_epoch_cache(
            "epoch",
            match &value {
                Some(meta) if meta.partition_slots.is_some() => "hit_partitions",
                Some(_) => "hit_boundary",
                None => "miss",
            },
        );
        value
    }

    /// Whether the cache holds anything (`GET_INFLATION_REWARD_EPOCH_CACHE_MAX_BYTES > 0`).
    pub(crate) fn enabled(&self) -> bool {
        self.inner.is_some()
    }

    /// Caches a validated boundary unless an entry (possibly carrying a partition map) exists.
    pub(crate) async fn insert_boundary(
        &self,
        key: InflationEpochCacheKey,
        boundary: InflationBoundary,
    ) {
        let Some(cache) = self.inner.as_ref() else {
            return;
        };
        let entry = cache
            .entry(key)
            .or_insert(Arc::new(InflationEpochMetadata {
                boundary,
                partition_slots: None,
            }))
            .await;
        if entry.is_fresh() {
            crate::metrics::inflation_reward_epoch_cache("boundary", "insert");
        }
    }

    /// Caches a boundary together with its complete partition slot map.
    pub(crate) async fn insert_complete(
        &self,
        key: InflationEpochCacheKey,
        boundary: InflationBoundary,
        partition_slots: Arc<[u64]>,
    ) {
        let Some(cache) = self.inner.as_ref() else {
            return;
        };
        cache
            .insert(
                key,
                Arc::new(InflationEpochMetadata {
                    boundary,
                    partition_slots: Some(partition_slots),
                }),
            )
            .await;
        crate::metrics::inflation_reward_epoch_cache("partitions", "insert");
    }

    #[cfg(test)]
    pub(crate) async fn run_pending_tasks(&self) {
        if let Some(cache) = self.inner.as_ref() {
            cache.run_pending_tasks().await;
        }
    }

    #[cfg(test)]
    pub(crate) fn weighted_size(&self) -> u64 {
        self.inner.as_ref().map_or(0, Cache::weighted_size)
    }
}

/// Builds the complete partition slot map from `(block_height, slot)` rows.
///
/// Returns `None` unless every height in `boundary_block_height + 1 ..= boundary_block_height +
/// num_partitions` maps to exactly one slot inside `(boundary_slot, end_slot_exclusive)` and slots
/// strictly increase with height. Rows outside that height range make the map incomplete.
pub(crate) fn complete_partition_slots(
    rows: &[(u64, u64)],
    boundary_slot: u64,
    end_slot_exclusive: u64,
    boundary_block_height: u64,
    num_partitions: usize,
) -> Option<Arc<[u64]>> {
    let first_height = boundary_block_height.checked_add(1)?;
    let mut slots: Vec<Option<u64>> = vec![None; num_partitions];
    for &(block_height, slot) in rows {
        let offset = usize::try_from(block_height.checked_sub(first_height)?).ok()?;
        if offset >= num_partitions || slot <= boundary_slot || slot >= end_slot_exclusive {
            return None;
        }
        match slots[offset] {
            Some(previous) if previous != slot => return None,
            _ => slots[offset] = Some(slot),
        }
    }
    let slots = slots.into_iter().collect::<Option<Vec<u64>>>()?;
    if slots.windows(2).any(|pair| pair[0] >= pair[1]) {
        return None;
    }
    Some(slots.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn boundary(slot: u64) -> InflationBoundary {
        InflationBoundary {
            slot,
            parent_blockhash: [1; 32],
            block_height: Some(100),
            num_partitions: Some(3),
        }
    }

    #[test]
    fn complete_partition_slots_requires_every_height_once() {
        let rows = [(101, 11), (102, 12), (103, 13)];
        assert_eq!(
            complete_partition_slots(&rows, 10, 100, 100, 3).as_deref(),
            Some(&[11, 12, 13][..])
        );
        // Duplicate identical rows (unmerged parts) are accepted.
        let rows = [(101, 11), (101, 11), (102, 12), (103, 13)];
        assert!(complete_partition_slots(&rows, 10, 100, 100, 3).is_some());
        // A missing partition block leaves the map incomplete.
        assert!(complete_partition_slots(&[(101, 11), (103, 13)], 10, 100, 100, 3).is_none());
        // A height mapped to two slots is rejected.
        let rows = [(101, 11), (101, 14), (102, 12), (103, 13)];
        assert!(complete_partition_slots(&rows, 10, 100, 100, 3).is_none());
        // Heights outside the partition range are rejected.
        let rows = [(100, 10), (101, 11), (102, 12), (103, 13)];
        assert!(complete_partition_slots(&rows, 9, 100, 100, 3).is_none());
        let rows = [(101, 11), (102, 12), (103, 13), (104, 14)];
        assert!(complete_partition_slots(&rows, 10, 100, 100, 3).is_none());
        // Slots must lie after the boundary and before the next epoch.
        assert!(
            complete_partition_slots(&[(101, 10), (102, 12), (103, 13)], 10, 100, 100, 3).is_none()
        );
        assert!(
            complete_partition_slots(&[(101, 11), (102, 12), (103, 100)], 10, 100, 100, 3)
                .is_none()
        );
        // Slots must increase with height.
        assert!(
            complete_partition_slots(&[(101, 12), (102, 11), (103, 13)], 10, 100, 100, 3).is_none()
        );
    }

    #[tokio::test]
    async fn disabled_cache_stores_nothing() {
        let cache = InflationEpochCache::new(0);
        cache.insert_boundary((1, 2), boundary(1)).await;
        assert!(cache.get((1, 2)).await.is_none());
    }

    #[tokio::test]
    async fn boundary_insert_does_not_replace_partition_map() {
        let cache = InflationEpochCache::new(1 << 20);
        cache
            .insert_complete((1, 2), boundary(1), Arc::from(vec![2, 3, 4]))
            .await;
        cache.insert_boundary((1, 2), boundary(1)).await;
        let meta = cache.get((1, 2)).await.expect("cached");
        assert_eq!(meta.partition_slots.as_deref(), Some(&[2, 3, 4][..]));
    }

    #[tokio::test]
    async fn cache_is_bounded_by_bytes() {
        let cache = InflationEpochCache::new(4 * ENTRY_OVERHEAD_BYTES);
        for epoch in 0..64u64 {
            cache
                .insert_boundary((epoch * 10, epoch * 10 + 10), boundary(epoch * 10))
                .await;
        }
        cache.run_pending_tasks().await;
        assert!(cache.weighted_size() <= 4 * ENTRY_OVERHEAD_BYTES);
    }

    #[tokio::test]
    async fn entries_expire_after_ttl() {
        let cache = InflationEpochCache::with_ttl(1 << 20, Duration::from_millis(50));
        cache.insert_boundary((1, 2), boundary(1)).await;
        assert!(cache.get((1, 2)).await.is_some());
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(cache.get((1, 2)).await.is_none());
    }
}
