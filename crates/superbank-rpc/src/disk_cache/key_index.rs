// SPDX-License-Identifier: AGPL-3.0-only
//! Ephemeral partition membership. Missing or changing filters always admit a query.

use super::{DiskCache, DiskCacheConfig, DiskCacheError, schema::CacheTableKind};
use clickhouse::Row;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

mod signatures;
pub(super) use signatures::{SignatureCandidates, SignatureHash};

const RESERVE: u64 = 64 * 1024 * 1024;
const ENTRY_OVERHEAD: u64 = 1024;

#[derive(Clone, Copy, Debug)]
pub(super) enum Family {
    Address,
    HotAddress,
    Owner,
}
impl Family {
    fn table(self) -> (CacheTableKind, &'static str) {
        match self {
            Self::Address => (CacheTableKind::Gsfa, "address"),
            Self::HotAddress => (CacheTableKind::GsfaHot, "address"),
            Self::Owner => (CacheTableKind::TokenOwnerActivity, "owner"),
        }
    }
}
const FAMILIES: [Family; 3] = [Family::Address, Family::HotAddress, Family::Owner];

struct Bloom {
    bits: Vec<u8>,
    hashes: u32,
}
impl Bloom {
    fn new(bytes: usize, cardinality: u64) -> Option<Self> {
        let hashes = ((bytes as f64 * 8.0 / cardinality.max(1) as f64) * std::f64::consts::LN_2)
            .round()
            .clamp(1.0, 16.0) as u32;
        Self::with_hashes(bytes, hashes)
    }
    fn with_hashes(bytes: usize, hashes: u32) -> Option<Self> {
        if bytes == 0 {
            return None;
        }
        let mut bits = Vec::new();
        bits.try_reserve_exact(bytes).ok()?;
        bits.resize(bytes, 0);
        Some(Self { bits, hashes })
    }
    fn positions(&self, key: &[u8]) -> impl Iterator<Item = usize> + use<> {
        self.hash_positions(SignatureHash::new(key))
    }
    fn hash_positions(
        &self,
        SignatureHash(a, b): SignatureHash,
    ) -> impl Iterator<Item = usize> + use<> {
        let bits = self.bits.len() as u64 * 8;
        (0..self.hashes)
            .map(move |i| (a.wrapping_add(u64::from(i).wrapping_mul(b)) % bits) as usize)
    }
    fn insert(&mut self, key: &[u8]) {
        for bit in self.positions(key) {
            self.bits[bit / 8] |= 1 << (bit % 8);
        }
    }
    fn contains(&self, key: &[u8]) -> bool {
        self.positions(key)
            .all(|bit| self.bits[bit / 8] & (1 << (bit % 8)) != 0)
    }
}
struct Entry {
    token: u64,
    filters: Option<[Option<Bloom>; 3]>,
}
#[derive(Default)]
struct State {
    entries: BTreeMap<u64, Entry>,
    writers: BTreeMap<u64, signatures::Writer>,
    signatures: BTreeMap<u64, signatures::SignatureEntry>,
    serial: u64,
    epoch: u64,
    untracked_writers: usize,
}

pub(super) struct KeyIndex {
    state: Mutex<State>,
    allocation: tokio::sync::Mutex<()>,
    width: u64,
    quota: usize,
    max_entries: usize,
}
pub(super) struct Mutation {
    index: Arc<KeyIndex>,
    id: Option<u64>,
}
impl Drop for Mutation {
    fn drop(&mut self) {
        let mut state = self.index.state.lock().expect("key index lock");
        match self.id {
            Some(id) => {
                if let Some(writer) = state.writers.remove(&id) {
                    signatures::abandon_update(&mut state, &writer);
                }
            }
            None => state.untracked_writers -= 1,
        }
    }
}
impl KeyIndex {
    pub(super) fn new(cfg: &DiskCacheConfig) -> Self {
        // One extra edge partition and one build/replacement slot. Reserve includes
        // streaming buffers and bounded mutation metadata, not ClickHouse's memory.
        let slots = cfg
            .retain_slots
            .div_ceil(cfg.partition_slots)
            .saturating_add(2);
        let quota = cfg.key_index_max_memory_bytes.saturating_sub(RESERVE) / slots;
        Self {
            state: Mutex::default(),
            allocation: tokio::sync::Mutex::default(),
            width: cfg.partition_slots,
            quota: usize::try_from(quota.saturating_sub(ENTRY_OVERHEAD)).unwrap_or(0),
            max_entries: usize::try_from(slots.saturating_sub(1)).unwrap_or(0),
        }
    }
    pub(super) fn mutation(self: &Arc<Self>, start: u64, end: u64) -> Mutation {
        let mut state = self.state.lock().expect("key index lock");
        // Bound mutation metadata even if many callers concurrently poison slots.
        if state.writers.len() >= 128 {
            // Reject allocations already in flight even if the untracked writer finishes first.
            state.serial += 1;
            state.entries.clear();
            state.signatures.clear();
            state.epoch += 1;
            state.untracked_writers += 1;
            return Mutation {
                index: self.clone(),
                id: None,
            };
        }
        state.serial += 1;
        let id = state.serial;
        let range = (start / self.width, end / self.width);
        let count = state.entries.len();
        state.entries.retain(|p, _| *p < range.0 || *p > range.1);
        if count != state.entries.len() {
            state.epoch += 1;
        }
        state.writers.insert(id, signatures::Writer::new(range));
        Mutation {
            index: self.clone(),
            id: Some(id),
        }
    }
    pub(super) fn invalidate_reads(&self) {
        self.state.lock().expect("key index lock").epoch += 1;
    }
    pub(super) fn epoch(&self) -> u64 {
        self.state.lock().expect("key index lock").epoch
    }
    pub(super) fn may_contain(&self, partition: u64, families: &[Family], key: &[u8]) -> bool {
        let state = self.state.lock().expect("key index lock");
        let Some(filters) = state
            .entries
            .get(&partition)
            .and_then(|e| e.filters.as_ref())
        else {
            return true;
        };
        families.iter().any(|f| {
            filters[*f as usize]
                .as_ref()
                .is_none_or(|b| b.contains(key))
        })
    }
    fn begin(&self, partition: u64) -> Option<u64> {
        let mut state = self.state.lock().expect("key index lock");
        if state.untracked_writers > 0
            || self.quota < 4
            || state.entries.len() >= self.max_entries
            || state.entries.contains_key(&partition)
        {
            return None;
        }
        if state
            .writers
            .values()
            .any(|writer| (writer.range.0..=writer.range.1).contains(&partition))
        {
            return None;
        }
        state.serial += 1;
        let token = state.serial;
        state.entries.insert(
            partition,
            Entry {
                token,
                filters: None,
            },
        );
        Some(token)
    }
    fn finish(&self, partition: u64, token: u64, filters: Option<[Option<Bloom>; 3]>) {
        let mut state = self.state.lock().expect("key index lock");
        if state
            .entries
            .get(&partition)
            .is_none_or(|e| e.token != token)
        {
            return;
        }
        match filters {
            Some(filters) => {
                state.entries.insert(
                    partition,
                    Entry {
                        token,
                        filters: Some(filters),
                    },
                );
            }
            None => {
                state.entries.remove(&partition);
            }
        }
    }
}

#[derive(Deserialize, Row)]
struct Cardinality {
    n: u64,
}
#[derive(Deserialize, Row)]
struct KeyRow<const N: usize> {
    key: serde_big_array::Array<u8, N>,
}

impl DiskCache {
    pub(crate) async fn run_key_index(
        self: Arc<Self>,
        shutdown: tokio::sync::broadcast::Receiver<()>,
    ) {
        run_workers(
            || self.build_signature_indexes(),
            || self.build_key_indexes(),
            shutdown,
        )
        .await;
    }
    pub(super) async fn build_key_indexes(&self) {
        self.publish_index_metrics();
        let Some((floor, tip)) = self.tip_span() else {
            return;
        };
        let width = self.inner.cfg.partition_slots;
        for partition in (floor.div_ceil(width)..tip / width).rev() {
            // Check live state before each build. An already-running address scan
            // may finish, but cannot delay the independent signature worker.
            if !self.signature_indexes_ready() {
                break;
            }
            let Some(token) = self.inner.key_index.begin(partition) else {
                continue;
            };
            self.publish_index_metrics();
            self.build_address_partition(partition, token).await;
            self.publish_index_metrics();
        }
    }
    async fn build_address_partition(&self, partition: u64, token: u64) {
        let started = std::time::Instant::now();
        let built = tokio::time::timeout(
            Duration::from_secs(300),
            self.build_partition_filters(partition),
        )
        .await;
        let outcome = if matches!(&built, Ok(Ok(_))) {
            "success"
        } else {
            "error"
        };
        let filters = match built {
            Ok(Ok(filters)) => Some(filters),
            _ => {
                crate::metrics::disk_cache_read("key_index_build", "error");
                None
            }
        };
        self.inner.key_index.finish(partition, token, filters);
        crate::metrics::disk_cache_key_seconds(
            "index_build",
            outcome,
            started.elapsed().as_secs_f64(),
        );
        tracing::debug!(
            partition,
            elapsed_ms = started.elapsed().as_millis(),
            "disk cache: key index build finished"
        );
    }
    fn publish_index_metrics(&self) {
        self.publish_signature_index_metrics();
        self.publish_key_index_metrics();
    }
    fn publish_key_index_metrics(&self) {
        let total = self.key_span().map_or(0, |(floor, tip)| {
            tip / self.inner.cfg.partition_slots - floor / self.inner.cfg.partition_slots + 1
        });
        let state = self.inner.key_index.state.lock().expect("key index lock");
        let indexed = state
            .entries
            .values()
            .filter(|entry| entry.filters.is_some())
            .count() as u64;
        let allocated = state
            .entries
            .values()
            .filter_map(|entry| entry.filters.as_ref())
            .flat_map(|filters| filters.iter().flatten())
            .map(|bloom| bloom.bits.capacity() as u64)
            .sum::<u64>();

        let building = state
            .entries
            .values()
            .filter(|entry| entry.filters.is_none())
            .count() as u64;
        // Include the reserved builder/metadata allowance to report the upper bound.
        let bytes = allocated
            + building * self.inner.key_index.address_quota() as u64
            + state
                .signatures
                .values()
                .map(|entry| entry.bloom.bits.capacity() as u64)
                .sum::<u64>()
            + RESERVE
            + (state.entries.len() + state.signatures.len()) as u64 * (ENTRY_OVERHEAD / 2);
        drop(state);
        crate::metrics::disk_cache_key_index(bytes, indexed, total.saturating_sub(indexed));
    }
    async fn build_partition_filters(
        &self,
        partition: u64,
    ) -> Result<[Option<Bloom>; 3], DiskCacheError> {
        // The persistent address-builder lane is independent of interactive reads.
        let mut builder = self.inner.address_index_reader.clone();
        builder.client = builder
            .client
            .clone()
            .with_setting("max_threads", "1")
            .with_setting("max_memory_usage", "67108864")
            .with_setting("max_execution_time", "300")
            .with_setting("max_block_size", "8192")
            .with_setting("preferred_block_size_bytes", "1048576");
        builder.cache_partition = Some((self.inner.cfg.partition_slots, partition));
        builder.query_timeout = Duration::from_secs(300);
        let snapshot = self.source_schema();
        let mut cardinalities = [0u64; 3];
        for family in FAMILIES {
            let (kind, key) = family.table();
            if !snapshot.has_table(kind) {
                continue;
            }
            let sql = format!(
                "SELECT uniqCombined64({key}) AS n FROM `{}`.{} WHERE intDiv(slot, {}) = {partition}",
                self.inner.cfg.database,
                kind.local_name(),
                self.inner.cfg.partition_slots
            );
            cardinalities[family as usize] = cardinality(&builder, &sql).await?.max(1);
        }
        let total: u128 = cardinalities.iter().map(|n| u128::from(*n)).sum();
        let mut filters: [Option<Bloom>; 3] = std::array::from_fn(|_| None);
        for family in FAMILIES {
            let n = cardinalities[family as usize];
            if n == 0 {
                continue;
            }
            let share = (self.inner.key_index.address_quota() as u128 * u128::from(n)
                / total.max(1)) as usize;
            let target = (n as f64 * 1.2).ceil() as usize;
            let Some(mut bloom) = Bloom::new(share.min(target), n) else {
                continue;
            };
            let (kind, key) = family.table();
            let sql = format!(
                "SELECT {key} AS key FROM `{}`.{} WHERE intDiv(slot, {}) = {partition}",
                self.inner.cfg.database,
                kind.local_name(),
                self.inner.cfg.partition_slots
            );
            read_keys::<32>(&builder, &sql, &mut bloom).await?;
            filters[family as usize] = Some(bloom);
        }
        Ok(filters)
    }
}

// Both loops are owned by the supervisor's existing task. Dropping this future
// also drops active query cleanup guards; no maintenance task can outlive it.
async fn run_workers<S, A, SF, AF>(
    signatures: S,
    addresses: A,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) where
    S: Fn() -> SF,
    A: Fn() -> AF,
    SF: std::future::Future<Output = ()>,
    AF: std::future::Future<Output = ()>,
{
    tokio::select! {
        _ = shutdown.recv() => {},
        _ = async { tokio::join!(repeat(signatures), repeat(addresses)); } => {},
    }
}

async fn repeat<F: std::future::Future<Output = ()>>(work: impl Fn() -> F) {
    loop {
        work().await;
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn cardinality(
    client: &crate::clickhouse::ClickHouseClient,
    sql: &str,
) -> Result<u64, DiskCacheError> {
    let row = client
        .read_one::<Cardinality>(sql, "key_index_cardinality")
        .await
        .map_err(|e| DiskCacheError::ClickHouse(e.to_string()))?;
    Ok(row.n)
}

async fn read_keys<const N: usize>(
    client: &crate::clickhouse::ClickHouseClient,
    sql: &str,
    bloom: &mut Bloom,
) -> Result<(), DiskCacheError> {
    let mut count = 0u64;
    let mut rows = client
        .read::<KeyRow<N>>(sql, "key_index_keys")
        .await
        .map_err(|e| DiskCacheError::ClickHouse(e.to_string()))?;
    while let Some(row) = rows
        .next()
        .await
        .map_err(|e| DiskCacheError::ClickHouse(e.to_string()))?
    {
        bloom.insert(&row.key.0);
        count += 1;
        if count.is_multiple_of(8192) {
            tokio::task::yield_now().await;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn mutation_metadata_is_bounded_under_overload() {
        let index = Arc::new(KeyIndex {
            state: Mutex::default(),
            allocation: tokio::sync::Mutex::default(),
            width: 10,
            quota: 1024,
            max_entries: 2,
        });
        let guards: Vec<_> = (0..140).map(|_| index.mutation(10, 19)).collect();
        assert_eq!(index.state.lock().unwrap().writers.len(), 128);
        assert!(index.begin(2).is_none());
        drop(guards);
        assert!(index.begin(2).is_some());
    }
    #[test]
    fn quota_accounts_for_builds_and_metadata() {
        let mut cfg = super::super::key_tests::config(String::new(), String::new());
        for width in [1, 10, 40, 1000] {
            cfg.partition_slots = width;
            let index = KeyIndex::new(&cfg);
            assert!(
                (index.quota as u64 + ENTRY_OVERHEAD) * index.max_entries as u64 + RESERVE
                    <= cfg.key_index_max_memory_bytes
            );
        }
        cfg.key_index_max_memory_bytes = RESERVE;
        assert!(KeyIndex::new(&cfg).begin(1).is_none());
    }
    #[test]
    fn family_isolation_and_failed_builds_remain_conservative() {
        let index = KeyIndex {
            state: Mutex::default(),
            allocation: tokio::sync::Mutex::default(),
            width: 10,
            quota: 1024,
            max_entries: 1,
        };
        let token = index.begin(1).unwrap();
        assert!(index.begin(2).is_none());
        index.finish(1, token, None);
        let token = index.begin(1).unwrap();
        let mut filters = std::array::from_fn(|_| Bloom::new(1024, 1));
        filters[Family::Owner as usize]
            .as_mut()
            .unwrap()
            .insert(b"owner");
        index.finish(1, token, Some(filters));
        assert!(!index.may_contain(1, &[Family::Address], b"owner"));
        assert!(index.may_contain(1, &[Family::Address, Family::Owner], b"owner"));
        assert!(index.may_contain(2, &[Family::Address], b"unknown"));
    }
    #[test]
    fn old_build_cannot_replace_new_generation() {
        let index = Arc::new(KeyIndex {
            state: Mutex::default(),
            allocation: tokio::sync::Mutex::default(),
            width: 10,
            quota: 1024,
            max_entries: 2,
        });
        let old = index.begin(1).unwrap();
        drop(index.mutation(10, 19));
        let new = index.begin(1).unwrap();
        index.finish(1, old, Some(std::array::from_fn(|_| Bloom::new(32, 10))));
        assert!(index.may_contain(1, &[Family::Address], b"missing"));
        index.finish(1, new, Some(std::array::from_fn(|_| Bloom::new(32, 10))));
        assert!(!index.may_contain(1, &[Family::Address], b"missing"));
    }

    #[test]
    fn saturated_bloom_never_loses_inserted_keys() {
        let mut bloom = Bloom::new(1, 10_000).unwrap();
        for n in 0u64..10_000 {
            bloom.insert(&n.to_le_bytes());
        }
        for n in 0u64..10_000 {
            assert!(bloom.contains(&n.to_le_bytes()));
        }
        assert!(bloom.contains(b"false positive is safe"));
    }
    #[test]
    fn invalidation_rejects_in_flight_build_and_admits_unknown_keys() {
        let index = Arc::new(KeyIndex {
            state: Mutex::default(),
            allocation: tokio::sync::Mutex::default(),
            width: 10,
            quota: 1024,
            max_entries: 4,
        });
        let token = index.begin(1).unwrap();
        let mutation = index.mutation(10, 19);
        index.finish(1, token, Some(std::array::from_fn(|_| Bloom::new(32, 10))));
        assert!(index.may_contain(1, &[Family::Address], b"missing"));
        assert!(index.begin(1).is_none());
        drop(mutation);
        assert!(index.begin(1).is_some());
    }
}
