// SPDX-License-Identifier: AGPL-3.0-only
//! Signature membership shares the index budget, but survives ordinary appends.
use super::{Bloom, KeyIndex, Mutation, State};
use crate::disk_cache::{DiskCache, DiskCacheError};
use clickhouse::Row;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::time::Duration;

const UPDATE_BATCH: usize = 128;
const SIGNATURE_HASHES: u32 = 7;

#[derive(Clone, Copy)]
pub(in crate::disk_cache) struct SignatureHash(pub(super) u64, pub(super) u64);
impl SignatureHash {
    pub(in crate::disk_cache) fn new(key: &[u8]) -> Self {
        let digest = blake3::hash(key);
        let mut a = [0; 8];
        let mut b = [0; 8];
        a.copy_from_slice(&digest.as_bytes()[..8]);
        b.copy_from_slice(&digest.as_bytes()[8..16]);
        Self(u64::from_le_bytes(a), u64::from_le_bytes(b) | 1)
    }
}

#[cfg(test)]
mod tests;

pub(super) struct SignatureEntry {
    token: u64,
    pub(super) bloom: Bloom,
    complete: bool,
}

pub(super) struct Writer {
    pub(super) range: (u64, u64),
    signature_update: bool,
    repair: bool,
    updated: bool,
}
impl Writer {
    pub(super) fn new(range: (u64, u64)) -> Self {
        Self {
            range,
            signature_update: false,
            repair: false,
            updated: false,
        }
    }
    fn repairing(&self, partition: u64) -> bool {
        self.repair && !self.updated && (self.range.0..=self.range.1).contains(&partition)
    }
}

pub(in crate::disk_cache) struct SignatureCandidates {
    pub(in crate::disk_cache) partitions: Vec<u64>,
    pub(in crate::disk_cache) total: u64,
    unknown: bool,
}
impl SignatureCandidates {
    pub(in crate::disk_cache) fn outcome(&self) -> &'static str {
        if self.unknown {
            "unknown"
        } else if self.partitions.is_empty() {
            "absent"
        } else {
            "possible"
        }
    }
}

fn invalidate(state: &mut State, range: (u64, u64)) {
    state.serial += 1;
    for (_, entry) in state.signatures.range_mut(range.0..=range.1) {
        entry.complete = false;
        entry.token = state.serial;
    }
}

pub(super) fn abandon_update(state: &mut State, writer: &Writer) {
    if writer.signature_update && !writer.updated {
        invalidate(state, writer.range);
    }
}

impl Mutation {
    pub(in crate::disk_cache) fn signature_fill(self, repair: bool) -> Self {
        if let Some(writer) = self
            .index
            .state
            .lock()
            .expect("key index lock")
            .writers
            .get_mut(&self.id.unwrap_or(0))
        {
            writer.signature_update = true;
            writer.repair = repair;
        }
        self
    }
    fn finish_signatures(&mut self, success: bool) {
        let mut state = self.index.state.lock().expect("key index lock");
        let Some(writer) = state.writers.get_mut(&self.id.unwrap_or(0)) else {
            return;
        };
        writer.updated = success;
        // Failed fills may still publish coverage. Keep the partition unknown
        // until guard drop, which also invalidates entries allocated meanwhile.
        writer.repair |= !success;
        let range = writer.range;
        if !success {
            invalidate(&mut state, range);
        }
    }
}

impl KeyIndex {
    pub(super) fn address_quota(&self) -> usize {
        self.quota - self.signature_quota()
    }
    fn signature_quota(&self) -> usize {
        self.quota / 3 * 2
    }

    pub(in crate::disk_cache) fn clear_signatures(&self) {
        let mut state = self.state.lock().expect("key index lock");
        state.serial += 1;
        state.signatures.clear();
    }
    pub(in crate::disk_cache) fn evict_signatures(&self, floor: u64) {
        self.state
            .lock()
            .expect("key index lock")
            .signatures
            .retain(|p, _| *p >= floor / self.width);
    }

    async fn ensure_signature_partition(
        &self,
        partition: u64,
        empty: impl FnOnce() -> bool,
    ) -> Option<u64> {
        // Only one bitmap allocation may be outside the map at a time. The existing
        // spare partition allowance accounts for it, even across a concurrent reset.
        let _allocation = self.allocation.lock().await;
        let serial = {
            let state = self.state.lock().expect("key index lock");
            if state.writers.values().any(|writer| {
                !writer.signature_update && (writer.range.0..=writer.range.1).contains(&partition)
            }) {
                return None;
            }
            if let Some(entry) = state.signatures.get(&partition) {
                return Some(entry.token);
            }
            if state.signatures.len() >= self.max_entries || state.untracked_writers > 0 {
                return None;
            }
            state.serial
        };
        let bloom = Bloom::with_hashes(self.signature_quota(), SIGNATURE_HASHES)?;
        let complete = empty();
        let mut state = self.state.lock().expect("key index lock");
        if state.serial != serial {
            return None;
        }
        state.serial += 1;
        let token = state.serial;
        state.signatures.insert(
            partition,
            SignatureEntry {
                token,
                bloom,
                complete,
            },
        );
        Some(token)
    }

    pub(in crate::disk_cache) fn signature_candidates(
        &self,
        floor: u64,
        tip: u64,
        hash: SignatureHash,
    ) -> SignatureCandidates {
        let state = self.state.lock().expect("key index lock");
        let mut result = SignatureCandidates {
            partitions: Vec::new(),
            total: 0,
            unknown: false,
        };
        for partition in (floor..=tip).rev() {
            result.total += 1;
            let entry = state
                .signatures
                .get(&partition)
                .filter(|entry| entry.complete);
            let repairing = state.writers.values().any(|w| w.repairing(partition));
            if let Some(entry) = entry.filter(|_| !repairing) {
                if entry
                    .bloom
                    .hash_positions(hash)
                    .all(|bit| entry.bloom.bits[bit / 8] & (1 << (bit % 8)) != 0)
                {
                    result.partitions.push(partition);
                }
            } else {
                result.unknown = true;
                result.partitions.push(partition);
            }
        }
        result
    }

    fn insert_signature_hashes(
        &self,
        tokens: &BTreeMap<u64, u64>,
        batch: &[(u64, SignatureHash)],
    ) -> Result<(), DiskCacheError> {
        let mut state = self.state.lock().expect("key index lock");
        // Check even an empty result: a reset or failed overlapping fill must
        // not turn an obsolete scan into a successful membership update.
        if !tokens.iter().all(|(partition, token)| {
            state
                .signatures
                .get(partition)
                .is_some_and(|entry| entry.token == *token)
        }) {
            return Err(DiskCacheError::ClickHouse(
                "signature index generation changed".into(),
            ));
        }
        for &(slot, hash) in batch {
            let partition = slot / self.width;
            let entry = state
                .signatures
                .get_mut(&partition)
                .filter(|entry| tokens.get(&partition) == Some(&entry.token))
                .ok_or_else(|| {
                    DiskCacheError::ClickHouse("signature index generation changed".into())
                })?;
            for bit in entry.bloom.hash_positions(hash) {
                entry.bloom.bits[bit / 8] |= 1 << (bit % 8);
            }
        }
        Ok(())
    }
    fn finish_signature_build(&self, partition: u64, token: u64) -> bool {
        let mut state = self.state.lock().expect("key index lock");
        if let Some(entry) = state
            .signatures
            .get_mut(&partition)
            .filter(|entry| entry.token == token)
        {
            entry.complete = true;
            return true;
        }
        false
    }
    fn signature_generation_matches(&self, partition: u64, token: u64) -> bool {
        self.state
            .lock()
            .expect("key index lock")
            .signatures
            .get(&partition)
            .is_some_and(|entry| entry.token == token)
    }
    fn signature_complete(&self, partition: u64) -> bool {
        self.state
            .lock()
            .expect("key index lock")
            .signatures
            .get(&partition)
            .is_some_and(|entry| entry.complete)
    }
    fn signature_completeness(&self, floor: u64, tip: u64) -> (u64, u64) {
        let state = self.state.lock().expect("key index lock");
        let ready = state
            .signatures
            .range(floor..=tip)
            .filter(|(partition, entry)| {
                entry.complete && !state.writers.values().any(|w| w.repairing(**partition))
            })
            .count() as u64;
        (ready, (tip - floor + 1).saturating_sub(ready))
    }
}

#[derive(Deserialize, Row)]
struct SignatureRow {
    slot: u64,
    key: serde_big_array::Array<u8, 64>,
}

impl DiskCache {
    async fn signature_partition(&self, partition: u64) -> Option<u64> {
        let width = self.inner.cfg.partition_slots;
        self.inner
            .key_index
            .ensure_signature_partition(partition, || {
                let floor = partition.saturating_mul(width);
                !self
                    .inner
                    .coverage
                    .read()
                    .expect("coverage lock")
                    .intersects(floor, floor.saturating_add(width - 1))
            })
            .await
    }

    pub(in crate::disk_cache) async fn update_signature_membership(
        &self,
        mutation: &mut Mutation,
        start: u64,
        end: u64,
    ) {
        let result = tokio::time::timeout(
            self.inner.cfg.query_timeout,
            self.update_signature_range(start, end),
        )
        .await;
        let success = matches!(result, Ok(Ok(())));
        mutation.finish_signatures(success);
        if !success {
            crate::metrics::disk_cache_read("signature_index_update", "error");
            tracing::warn!(
                ?result,
                "disk cache: signature membership update failed; affected filters remain unknown"
            );
        }
    }

    async fn update_signature_range(&self, start: u64, end: u64) -> Result<(), DiskCacheError> {
        let width = self.inner.cfg.partition_slots;
        let mut tokens = BTreeMap::new();
        for partition in start / width..=end / width {
            let token = self.signature_partition(partition).await.ok_or_else(|| {
                DiskCacheError::ClickHouse("signature index memory unavailable".into())
            })?;
            tokens.insert(partition, token);
        }
        let sql = self.source_schema().signature_keys_query(
            &self.inner.cfg.database,
            width,
            start,
            end,
        )?;
        self.stream_signature_hashes(&sql, &tokens, self.inner.cfg.query_timeout)
            .await
    }

    pub(in crate::disk_cache) fn signature_indexes_ready(&self) -> bool {
        let width = self.inner.cfg.partition_slots;
        self.ready()
            && self.key_span().is_some_and(|(floor, tip)| {
                self.inner
                    .key_index
                    .signature_completeness(floor / width, tip / width)
                    .1
                    == 0
            })
    }

    pub(in crate::disk_cache) async fn build_signature_indexes(&self) {
        self.publish_index_metrics();
        if !self.ready() {
            return;
        }
        let Some((floor, tip)) = self.key_span() else {
            return;
        };
        let width = self.inner.cfg.partition_slots;
        for partition in (floor / width..=tip / width).rev() {
            if self.inner.key_index.signature_complete(partition) {
                continue;
            }
            self.build_signature_partition(partition).await;
            self.publish_index_metrics();
        }
    }

    async fn build_signature_partition(&self, partition: u64) {
        let Some(token) = self.signature_partition(partition).await else {
            crate::metrics::disk_cache_read("signature_index_build", "deferred");
            tracing::debug!(partition, "disk cache: signature index allocation deferred");
            return;
        };
        self.publish_index_metrics();
        let width = self.inner.cfg.partition_slots;
        let sql = format!(
            "SELECT slot, signature AS key FROM `{}`.signatures WHERE intDiv(slot, {width}) = {partition}",
            self.inner.cfg.database
        );
        let tokens = BTreeMap::from([(partition, token)]);
        let started = std::time::Instant::now();
        let built = tokio::time::timeout(
            Duration::from_secs(300),
            self.stream_signature_hashes(&sql, &tokens, Duration::from_secs(300)),
        )
        .await;
        let outcome = match &built {
            Ok(Ok(()))
                if self
                    .inner
                    .key_index
                    .finish_signature_build(partition, token) =>
            {
                "success"
            }
            _ if !self
                .inner
                .key_index
                .signature_generation_matches(partition, token) =>
            {
                "superseded"
            }
            Err(_) => "timeout",
            _ => "error",
        };
        crate::metrics::disk_cache_key_seconds(
            "signature_index_build",
            outcome,
            started.elapsed().as_secs_f64(),
        );
        if matches!(outcome, "error" | "timeout") {
            tracing::warn!(
                partition,
                outcome,
                ?built,
                "disk cache: signature index build failed"
            );
        } else {
            tracing::debug!(
                partition,
                outcome,
                "disk cache: signature index build finished"
            );
        }
    }

    pub(in crate::disk_cache) fn publish_signature_index_metrics(&self) {
        if let Some((floor, tip)) = self.key_span() {
            let width = self.inner.cfg.partition_slots;
            let (ready, unknown) = self
                .inner
                .key_index
                .signature_completeness(floor / width, tip / width);
            crate::metrics::disk_cache_signature_index(ready, unknown);
        } else {
            crate::metrics::disk_cache_signature_index(0, 0);
        }
    }

    async fn stream_signature_hashes(
        &self,
        sql: &str,
        tokens: &BTreeMap<u64, u64>,
        timeout: Duration,
    ) -> Result<(), DiskCacheError> {
        let mut builder = self.inner.local.clone();
        builder.cache_partition = Some((
            self.inner.cfg.partition_slots,
            *tokens
                .first_key_value()
                .expect("nonempty signature range")
                .0,
        ));
        builder.query_timeout = timeout;
        let (sql, id, mut cleanup) =
            builder.annotate_lookup_query(sql.into(), "signature_index_keys");
        let client = builder
            .client
            .clone()
            .with_setting("query_id", id.unwrap_or_default())
            .with_setting("max_threads", "1")
            .with_setting("max_memory_usage", "67108864")
            .with_setting("max_block_size", "8192")
            .with_setting("preferred_block_size_bytes", "1048576")
            .with_setting("max_execution_time", timeout.as_secs_f64().to_string());
        let mut cursor = client
            .query(&sql)
            .fetch::<SignatureRow>()
            .map_err(|e| DiskCacheError::ClickHouse(e.to_string()))?;
        let mut batch = Vec::with_capacity(UPDATE_BATCH);
        while let Some(row) = cursor
            .next()
            .await
            .map_err(|e| DiskCacheError::ClickHouse(e.to_string()))?
        {
            batch.push((row.slot, SignatureHash::new(&row.key.0)));
            if batch.len() == UPDATE_BATCH {
                self.inner
                    .key_index
                    .insert_signature_hashes(tokens, &batch)?;
                batch.clear();
                tokio::task::yield_now().await;
            }
        }
        self.inner
            .key_index
            .insert_signature_hashes(tokens, &batch)?;
        if let Some(cleanup) = &mut cleanup {
            cleanup.disarm();
        }
        Ok(())
    }
}
