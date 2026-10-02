// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! In-process cache of getTransaction records served by the primary ClickHouse.
//!
//! The primary holds finalized history and getTransaction reads it without regard to the
//! requested commitment, so a cached record is served exactly where the primary read would run:
//! after the head-cache and disk-cache misses. Values are decoded records, not encoded JSON, so
//! every encoding and `maxSupportedTransactionVersion` check (including
//! `UnsupportedTransactionVersion`) runs as for a primary-served record. Not-found results are
//! never cached. Entries expire a fixed time after insertion; the byte budget bounds memory.

use std::mem::size_of;
use std::sync::Arc;
use std::time::Duration;

use moka::future::Cache;
use moka::policy::EvictionPolicy;

use crate::clickhouse::StoredTransactionRecord;

/// Pinned-slot and unpinned reads use different primary queries, so they never share entries:
/// a pinned request only ever sees a record that a pinned read at the same slot returned.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub(crate) struct PrimaryTransactionCacheKey {
    pub(crate) signature: [u8; 64],
    pub(crate) pinned_slot: Option<u64>,
}

#[derive(Clone)]
pub(crate) struct PrimaryTransactionCache {
    inner: Option<Cache<PrimaryTransactionCacheKey, Arc<StoredTransactionRecord>>>,
    max_bytes: u64,
}

impl PrimaryTransactionCache {
    /// `max_bytes == 0` disables the cache. `ttl` is time since insertion, not idle time, so a
    /// retry storm cannot keep an entry alive.
    pub(crate) fn new(max_bytes: u64, ttl: Duration) -> Self {
        let inner = (max_bytes > 0).then(|| {
            Cache::builder()
                .max_capacity(max_bytes)
                .time_to_live(ttl)
                // LRU, not the default TinyLFU: a common reuse pattern is one fetch and one re-fetch
                // minutes later, and TinyLFU admission would reject such first-seen keys once full.
                .eviction_policy(EvictionPolicy::lru())
                .weigher(
                    |_key: &PrimaryTransactionCacheKey, value: &Arc<StoredTransactionRecord>| {
                        record_weight(value)
                    },
                )
                .build()
        });
        let cache = Self { inner, max_bytes };
        cache.publish_metrics();
        cache
    }

    #[cfg(test)]
    pub(crate) fn disabled() -> Self {
        Self::new(0, Duration::from_secs(1))
    }

    pub(crate) async fn get(
        &self,
        key: &PrimaryTransactionCacheKey,
    ) -> Option<Arc<StoredTransactionRecord>> {
        let cache = self.inner.as_ref()?;
        let value = cache.get(key).await;
        crate::metrics::get_transaction_primary_cache_access(if value.is_some() {
            "hit"
        } else {
            "miss"
        });
        self.publish_metrics();
        value
    }

    /// Caches a record the primary returned for `key`. A record for another signature (which the
    /// primary query cannot return) is ignored rather than cached under the wrong key.
    pub(crate) async fn insert(
        &self,
        key: PrimaryTransactionCacheKey,
        record: Arc<StoredTransactionRecord>,
    ) {
        let Some(cache) = self.inner.as_ref() else {
            return;
        };
        if record.signature != key.signature
            || key.pinned_slot.is_some_and(|slot| slot != record.slot)
        {
            return;
        }
        cache.insert(key, record).await;
        crate::metrics::get_transaction_primary_cache_access("insert");
        self.publish_metrics();
    }

    pub(crate) fn entry_count(&self) -> u64 {
        self.inner.as_ref().map_or(0, Cache::entry_count)
    }

    pub(crate) fn weighted_size(&self) -> u64 {
        self.inner.as_ref().map_or(0, Cache::weighted_size)
    }

    fn publish_metrics(&self) {
        crate::metrics::get_transaction_primary_cache_state(
            self.entry_count(),
            self.weighted_size(),
            self.max_bytes,
        );
    }

    #[cfg(test)]
    pub(crate) async fn run_pending_tasks(&self) {
        if let Some(cache) = self.inner.as_ref() {
            cache.run_pending_tasks().await;
        }
    }
}

/// Fixed per-entry cost on top of the record: the key, the `Arc` header and moka's entry
/// bookkeeping (an estimate; moka does not expose it).
const ENTRY_OVERHEAD_BYTES: usize = size_of::<PrimaryTransactionCacheKey>() + 128;

fn record_weight(record: &StoredTransactionRecord) -> u32 {
    u32::try_from(ENTRY_OVERHEAD_BYTES.saturating_add(record_heap_bytes(record)))
        .unwrap_or(u32::MAX)
}

#[allow(clippy::ptr_arg)] // capacity, not length, is what is resident
fn vec_bytes<T>(values: &Vec<T>) -> usize {
    values.capacity().saturating_mul(size_of::<T>())
}

#[allow(clippy::ptr_arg)] // capacity, not length, is what is resident
fn nested_bytes<T>(values: &Vec<Vec<T>>) -> usize {
    values.iter().fold(vec_bytes(values), |sum, inner| {
        sum.saturating_add(vec_bytes(inner))
    })
}

#[allow(clippy::ptr_arg)] // capacity, not length, is what is resident
fn strings_bytes(values: &Vec<String>) -> usize {
    values.iter().fold(vec_bytes(values), |sum, value| {
        sum.saturating_add(value.capacity())
    })
}

#[allow(clippy::ptr_arg)] // capacity, not length, is what is resident
fn optional_strings_bytes(values: &Vec<Option<String>>) -> usize {
    values.iter().fold(vec_bytes(values), |sum, value| {
        sum.saturating_add(value.as_ref().map_or(0, String::capacity))
    })
}

/// Approximate resident bytes of a record: the struct plus the capacity of every heap buffer it
/// owns. Allocator rounding is not counted.
pub(crate) fn record_heap_bytes(record: &StoredTransactionRecord) -> usize {
    let inner_instruction_accounts = record.meta_inner_instructions_accounts.iter().fold(
        vec_bytes(&record.meta_inner_instructions_accounts),
        |sum, v| sum.saturating_add(nested_bytes(v)),
    );
    let inner_instruction_data = record
        .meta_inner_instructions_data
        .iter()
        .fold(vec_bytes(&record.meta_inner_instructions_data), |sum, v| {
            sum.saturating_add(nested_bytes(v))
        });
    [
        size_of::<StoredTransactionRecord>(),
        record.meta_err.as_ref().map_or(0, String::capacity),
        vec_bytes(&record.tx_signatures),
        vec_bytes(&record.tx_account_keys),
        vec_bytes(&record.tx_instructions_program_id_index),
        nested_bytes(&record.tx_instructions_accounts),
        nested_bytes(&record.tx_instructions_data),
        vec_bytes(&record.tx_address_table_lookup_account_key),
        nested_bytes(&record.tx_address_table_lookup_writable_indexes),
        nested_bytes(&record.tx_address_table_lookup_readonly_indexes),
        vec_bytes(&record.meta_pre_balances),
        vec_bytes(&record.meta_post_balances),
        vec_bytes(&record.meta_inner_instructions_index),
        nested_bytes(&record.meta_inner_instructions_program_id_index),
        inner_instruction_accounts,
        inner_instruction_data,
        nested_bytes(&record.meta_inner_instructions_stack_height),
        strings_bytes(&record.meta_log_messages),
        vec_bytes(&record.meta_pre_token_account_index),
        vec_bytes(&record.meta_pre_token_mint),
        vec_bytes(&record.meta_pre_token_owner),
        vec_bytes(&record.meta_pre_token_program_id),
        strings_bytes(&record.meta_pre_token_amount),
        vec_bytes(&record.meta_pre_token_decimals),
        vec_bytes(&record.meta_pre_token_ui_amount),
        strings_bytes(&record.meta_pre_token_ui_amount_string),
        vec_bytes(&record.meta_post_token_account_index),
        vec_bytes(&record.meta_post_token_mint),
        vec_bytes(&record.meta_post_token_owner),
        vec_bytes(&record.meta_post_token_program_id),
        strings_bytes(&record.meta_post_token_amount),
        vec_bytes(&record.meta_post_token_decimals),
        vec_bytes(&record.meta_post_token_ui_amount),
        strings_bytes(&record.meta_post_token_ui_amount_string),
        strings_bytes(&record.meta_reward_pubkey),
        vec_bytes(&record.meta_reward_lamports),
        vec_bytes(&record.meta_reward_post_balance),
        optional_strings_bytes(&record.meta_reward_type),
        vec_bytes(&record.meta_reward_commission),
        vec_bytes(&record.meta_reward_commission_bps),
        vec_bytes(&record.meta_loaded_addresses_writable),
        vec_bytes(&record.meta_loaded_addresses_readonly),
        record
            .meta_return_data_data
            .as_ref()
            .map_or(0, Vec::capacity),
    ]
    .into_iter()
    .fold(0usize, usize::saturating_add)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(signature_byte: u8, slot: u64) -> Arc<StoredTransactionRecord> {
        let mut record = crate::tests::base_transaction_record();
        record.signature = [signature_byte; 64];
        record.tx_signatures = vec![[signature_byte; 64]];
        record.slot = slot;
        record.meta_log_messages = vec!["x".repeat(1_000); 4];
        Arc::new(record)
    }

    fn key(signature_byte: u8, pinned_slot: Option<u64>) -> PrimaryTransactionCacheKey {
        PrimaryTransactionCacheKey {
            signature: [signature_byte; 64],
            pinned_slot,
        }
    }

    fn access_count(outcome: &'static str) -> u64 {
        crate::metrics::get_transaction_primary_cache_access_count_for_tests(outcome)
    }

    #[tokio::test]
    async fn disabled_by_default_and_never_stores() {
        let cache = PrimaryTransactionCache::new(0, Duration::from_secs(600));
        cache.insert(key(1, None), record(1, 5)).await;
        assert!(cache.get(&key(1, None)).await.is_none());
        assert_eq!(cache.entry_count(), 0);
        assert_eq!(cache.weighted_size(), 0);
    }

    #[tokio::test]
    async fn miss_insert_hit_are_counted() {
        let cache = PrimaryTransactionCache::new(1 << 20, Duration::from_secs(600));
        let (miss, insert, hit) = (
            access_count("miss"),
            access_count("insert"),
            access_count("hit"),
        );
        assert!(cache.get(&key(2, None)).await.is_none());
        let stored = record(2, 5);
        cache.insert(key(2, None), stored.clone()).await;
        let cached = cache.get(&key(2, None)).await.expect("hit");
        assert!(Arc::ptr_eq(&cached, &stored));
        // Counters are process-wide; other tests only ever add to them.
        assert!(access_count("miss") > miss);
        assert!(access_count("insert") > insert);
        assert!(access_count("hit") > hit);
        cache.run_pending_tasks().await;
        assert_eq!(cache.entry_count(), 1);
        assert!(cache.weighted_size() >= record_heap_bytes(&stored) as u64);
    }

    #[tokio::test]
    async fn pinned_and_unpinned_reads_do_not_share_entries() {
        let cache = PrimaryTransactionCache::new(1 << 20, Duration::from_secs(600));
        cache.insert(key(3, None), record(3, 5)).await;
        assert!(cache.get(&key(3, Some(5))).await.is_none());

        cache.insert(key(4, Some(5)), record(4, 5)).await;
        assert!(cache.get(&key(4, None)).await.is_none());
        assert!(cache.get(&key(4, Some(6))).await.is_none());
        assert_eq!(cache.get(&key(4, Some(5))).await.map(|r| r.slot), Some(5));
    }

    #[tokio::test]
    async fn mismatched_records_are_not_cached() {
        let cache = PrimaryTransactionCache::new(1 << 20, Duration::from_secs(600));
        // Another signature, and a pinned read whose record sits at another slot.
        cache.insert(key(5, None), record(6, 5)).await;
        cache.insert(key(7, Some(8)), record(7, 9)).await;
        assert!(cache.get(&key(5, None)).await.is_none());
        assert!(cache.get(&key(6, None)).await.is_none());
        assert!(cache.get(&key(7, Some(8))).await.is_none());
        assert!(cache.get(&key(7, Some(9))).await.is_none());
    }

    #[tokio::test]
    async fn entries_expire_after_ttl_even_when_read() {
        let cache = PrimaryTransactionCache::new(1 << 20, Duration::from_millis(600));
        cache.insert(key(8, None), record(8, 5)).await;
        for _ in 0..2 {
            tokio::time::sleep(Duration::from_millis(150)).await;
            assert!(cache.get(&key(8, None)).await.is_some());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(cache.get(&key(8, None)).await.is_none());
    }

    #[tokio::test]
    async fn byte_budget_bounds_the_cache() {
        let one = record_weight(&record(9, 5)) as u64;
        let cache = PrimaryTransactionCache::new(one * 4, Duration::from_secs(600));
        for byte in 10..60u8 {
            cache.insert(key(byte, None), record(byte, 5)).await;
        }
        cache.run_pending_tasks().await;
        assert!(
            cache.weighted_size() <= one * 4,
            "{}",
            cache.weighted_size()
        );
        assert!(cache.entry_count() <= 4);
    }

    #[tokio::test]
    async fn full_cache_admits_new_keys_over_frequently_read_ones() {
        let one = record_weight(&record(60, 5)) as u64;
        let cache = PrimaryTransactionCache::new(one * 4, Duration::from_secs(600));
        for byte in 60..64u8 {
            cache.insert(key(byte, None), record(byte, 5)).await;
        }
        for _ in 0..20 {
            for byte in 60..64u8 {
                assert!(cache.get(&key(byte, None)).await.is_some());
            }
        }
        cache.run_pending_tasks().await;
        cache.insert(key(64, None), record(64, 5)).await;
        cache.run_pending_tasks().await;
        assert!(cache.get(&key(64, None)).await.is_some());
        assert!(cache.entry_count() <= 4);
    }

    #[test]
    fn weight_counts_heap_buffers() {
        let empty = crate::tests::base_transaction_record();
        let base = record_heap_bytes(&empty);
        assert!(base >= size_of::<StoredTransactionRecord>());
        let mut large = empty.clone();
        large.meta_log_messages = vec!["y".repeat(10_000)];
        large.tx_instructions_data = vec![vec![0; 5_000]];
        large.meta_inner_instructions_data = vec![vec![vec![0; 3_000]]];
        assert!(record_heap_bytes(&large) >= base + 18_000);
    }

    /// Hit-path latency with a warm entry (debug builds overstate it).
    #[tokio::test]
    async fn hit_path_latency() {
        let cache = PrimaryTransactionCache::new(1 << 26, Duration::from_secs(600));
        for byte in 0..=255u8 {
            cache.insert(key(byte, None), record(byte, 5)).await;
        }
        let iterations = 20_000u32;
        let started = std::time::Instant::now();
        for i in 0..iterations {
            assert!(cache.get(&key((i % 256) as u8, None)).await.is_some());
        }
        let per_hit = started.elapsed() / iterations;
        eprintln!("primary cache hit: {per_hit:?} per get");
        assert!(per_hit < Duration::from_millis(1), "{per_hit:?}");
    }
}
