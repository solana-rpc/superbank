// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! In-process cache of complete `getInflationReward` answers, keyed by payout bounds, the
//! epoch-cache boundary the answer used, and the sorted, deduplicated address set.
//! Errors, `-32004` and `-32017` are never stored; see the crate README for the settle gate.

use std::future::Future;
use std::mem::size_of;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use moka::future::Cache;
use solana_epoch_schedule::EpochSchedule;
use tokio::time::Instant;

use crate::processing::{ProcessingError, ProcessingResult};

use super::blocks::{InflationRewardLookup, inflation_epoch_bounds_or_error};
use super::client::{ClickHouseClient, InflationRewardQueryLimits};
use super::inflation_cache::{InflationBoundary, InflationEpochCache};
use super::types::{InflationRewardLookupOutcome, InflationRewardRecord, QueryTimings};

pub(crate) const DEFAULT_TTL: Duration = Duration::from_secs(3_600);

/// 30 minutes of slots; matches the replica-lag margin of the getSignaturesForAddress watermark.
pub(crate) const DEFAULT_SETTLE_SLOTS: u64 = 4_500;

/// A lookup that a request takes over or retries starts only with this much of the request's
/// budget left, or half the budget when that is shorter.
const MIN_LOOKUP_BUDGET: Duration = Duration::from_millis(100);

/// Inline key and value, their two `Arc` headers, and an estimate of moka's per-entry bookkeeping.
const ENTRY_OVERHEAD_BYTES: u64 = (size_of::<InflationResponseKey>()
    + size_of::<InflationResponseValue>()
    + 2 * 2 * size_of::<usize>()
    + 128) as u64;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct InflationResponseKey {
    start_slot: u64,
    end_slot_exclusive: u64,
    /// `None` coalesces a lookup made without an epoch-cache entry; nothing is stored under it.
    boundary: Option<InflationBoundary>,
    addresses: Arc<[[u8; 32]]>,
}

impl InflationResponseKey {
    fn new(
        (start_slot, end_slot_exclusive): (u64, u64),
        boundary: Option<InflationBoundary>,
        addresses: &[[u8; 32]],
    ) -> Self {
        let mut sorted = addresses.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        Self {
            start_slot,
            end_slot_exclusive,
            boundary,
            addresses: sorted.into(),
        }
    }
}

type InflationResponseValue = Arc<[InflationRewardRecord]>;

fn entry_weight(key: &InflationResponseKey, value: &InflationResponseValue) -> u32 {
    let key_bytes = (key.addresses.len() as u64).saturating_mul(32);
    let record_bytes = size_of::<InflationRewardRecord>() as u64;
    let value_bytes = (value.len() as u64).saturating_mul(record_bytes);
    ENTRY_OVERHEAD_BYTES
        .saturating_add(key_bytes)
        .saturating_add(value_bytes)
        .try_into()
        .unwrap_or(u32::MAX)
}

/// How a cached `getInflationReward` lookup was answered.
#[derive(Debug)]
pub(crate) enum InflationRewardAnswer {
    /// This request ran the ClickHouse lookup.
    ClickHouse {
        outcome: InflationRewardLookupOutcome,
        timings: QueryTimings,
    },
    /// A stored answer, or one a concurrent identical request fetched.
    ResponseCache(InflationRewardLookupOutcome),
}

/// A leader's result that concurrent waiters share but that is not stored under its key.
enum Unstored {
    Answer {
        outcome: InflationRewardLookupOutcome,
        /// Boundary to store a settled answer under after a lookup without an epoch-cache entry.
        store_under: Option<InflationBoundary>,
    },
    Failed(ProcessingError),
}

#[derive(Clone)]
pub(crate) struct InflationResponseCache {
    inner: Option<Cache<InflationResponseKey, InflationResponseValue>>,
    max_bytes: u64,
    settle_slots: u64,
}

impl InflationResponseCache {
    pub(crate) fn disabled() -> Self {
        Self {
            inner: None,
            max_bytes: 0,
            settle_slots: DEFAULT_SETTLE_SLOTS,
        }
    }

    /// `GET_INFLATION_REWARD_EPOCH_CACHE_MAX_BYTES=0` turns this cache off too.
    pub(crate) fn from_limits(limits: &InflationRewardQueryLimits) -> Self {
        if limits.epoch_cache_max_bytes == 0 {
            return Self::disabled();
        }
        Self::new(
            limits.response_cache_max_bytes,
            limits.response_cache_ttl,
            limits.response_cache_settle_slots,
        )
    }

    pub(crate) fn new(max_bytes: u64, ttl: Duration, settle_slots: u64) -> Self {
        if max_bytes == 0 {
            return Self::disabled();
        }
        let inner = Cache::builder()
            .max_capacity(max_bytes)
            .time_to_live(ttl)
            .weigher(entry_weight)
            .build();
        let cache = Self {
            inner: Some(inner),
            max_bytes,
            settle_slots,
        };
        cache.publish_metrics();
        cache
    }

    pub(crate) fn enabled(&self) -> bool {
        self.inner.is_some()
    }

    pub(crate) fn entry_count(&self) -> u64 {
        self.inner.as_ref().map_or(0, Cache::entry_count)
    }

    pub(crate) fn weighted_size(&self) -> u64 {
        self.inner.as_ref().map_or(0, Cache::weighted_size)
    }

    fn publish_metrics(&self) {
        crate::metrics::inflation_reward_response_cache_state(
            self.entry_count(),
            self.weighted_size(),
            self.max_bytes,
        );
    }

    async fn insert(
        &self,
        cache: &Cache<InflationResponseKey, InflationResponseValue>,
        key: InflationResponseKey,
        records: InflationResponseValue,
    ) {
        cache.insert(key, records).await;
        self.inserted(cache).await;
    }

    async fn inserted(&self, cache: &Cache<InflationResponseKey, InflationResponseValue>) {
        crate::metrics::inflation_reward_response_cache("insert");
        cache.run_pending_tasks().await;
        self.publish_metrics();
    }

    /// Returns the boundary to store a lookup under, or the metric outcome that rules it out.
    async fn admit<R, RF>(
        &self,
        lookup: &InflationRewardLookup,
        unique_addresses: usize,
        tip_reached: R,
    ) -> Result<InflationBoundary, &'static str>
    where
        R: FnOnce(u64) -> RF,
        RF: Future<Output = bool>,
    {
        let (InflationRewardLookupOutcome::Complete(records), Some(provenance)) =
            (&lookup.outcome, &lookup.provenance)
        else {
            return Err("uncacheable");
        };
        // A row proves its block was read; a null can also mean a replica lacked the block.
        if records.len() >= unique_addresses
            || tip_reached(provenance.settle_slot.saturating_add(self.settle_slots)).await
        {
            Ok(provenance.boundary.clone())
        } else {
            Err("unsettled")
        }
    }

    /// Stores a cold lookup's answer only under the boundary the epoch cache now serves.
    async fn store_cold(
        &self,
        cache: &Cache<InflationResponseKey, InflationResponseValue>,
        epoch_cache: &InflationEpochCache,
        key: &InflationResponseKey,
        boundary: &InflationBoundary,
        records: &[InflationRewardRecord],
    ) {
        let bounds = (key.start_slot, key.end_slot_exclusive);
        if epoch_cache.peek_boundary(bounds).await.as_ref() != Some(boundary) {
            crate::metrics::inflation_reward_response_cache("uncacheable");
            return;
        }
        let key = InflationResponseKey {
            boundary: Some(boundary.clone()),
            ..key.clone()
        };
        self.insert(cache, key, records.into()).await;
    }

    #[cfg(test)]
    pub(crate) async fn run_pending_tasks(&self) {
        if let Some(cache) = self.inner.as_ref() {
            cache.run_pending_tasks().await;
        }
    }
}

/// Copies an error a concurrent leader produced; the source chain stays with the leader.
fn copy_error(err: &ProcessingError) -> ProcessingError {
    match err {
        ProcessingError::Database { context, .. } => ProcessingError::database_msg(context.clone()),
        ProcessingError::Deserialization { context, .. } => {
            ProcessingError::deserialization_msg(context.clone())
        }
        ProcessingError::Timeout { context, .. } => ProcessingError::timeout_msg(context.clone()),
    }
}

/// What is left of a request's lookup budget, or `None` when too little is left to start one.
fn budget_left(deadline: Instant, query_timeout: Duration) -> Option<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    (remaining >= MIN_LOOKUP_BUDGET.min(query_timeout / 2)).then_some(remaining)
}

fn budget_spent(query_timeout: Duration) -> ProcessingError {
    ProcessingError::timeout_msg(format!(
        "getInflationReward lookup budget of {query_timeout:?} ran out before the lookup started"
    ))
}

impl ClickHouseClient {
    /// `getInflationReward` through the response cache. `on_lookup` runs before each lookup this
    /// call starts; `tip_reached(slot)` reports whether the finalized tip is at least `slot`.
    pub(crate) async fn get_inflation_rewards_cached<L, R, RF>(
        &self,
        addresses: &[[u8; 32]],
        epoch: u64,
        schedule: &EpochSchedule,
        on_lookup: L,
        tip_reached: R,
    ) -> ProcessingResult<InflationRewardAnswer>
    where
        L: Fn(),
        R: FnOnce(u64) -> RF,
        RF: Future<Output = bool>,
    {
        let response_cache = &self.inflation_response_cache;
        let Some(cache) = response_cache
            .inner
            .as_ref()
            .filter(|_| !addresses.is_empty())
        else {
            let (outcome, timings) = self
                .get_inflation_rewards_for_epoch(addresses, epoch, schedule)
                .await?;
            return Ok(InflationRewardAnswer::ClickHouse { outcome, timings });
        };

        let bounds = inflation_epoch_bounds_or_error(epoch, schedule)?;
        let cached_epoch = self.inflation_epoch_cache.get(bounds).await;
        let key = InflationResponseKey::new(
            bounds,
            cached_epoch.as_ref().map(|meta| meta.boundary.clone()),
            addresses,
        );
        if key.boundary.is_some()
            && let Some(records) = cache.get(&key).await
        {
            crate::metrics::inflation_reward_response_cache("hit");
            response_cache.publish_metrics();
            return Ok(InflationRewardAnswer::ResponseCache(
                InflationRewardLookupOutcome::Complete(records.to_vec()),
            ));
        }
        crate::metrics::inflation_reward_response_cache("miss");

        let query_timeout = self.inflation_reward_limits.query_timeout;
        let deadline = Instant::now() + query_timeout;
        // Set only when this request runs the lookup: `Some(timings)` on success, `None` on error.
        let evaluated = Arc::new(OnceLock::<Option<QueryTimings>>::new());
        let on_lookup = &on_lookup;
        let init = {
            let evaluated = evaluated.clone();
            let cached_epoch = cached_epoch.clone();
            let key_boundary = key.boundary.clone();
            let unique_addresses = key.addresses.len();
            async move {
                // A request that takes over from an aborted leader keeps its own end-to-end budget.
                let Some(budget) = budget_left(deadline, query_timeout) else {
                    let _ = evaluated.set(None);
                    crate::metrics::inflation_reward_response_cache("uncacheable");
                    return Err(Unstored::Failed(budget_spent(query_timeout)));
                };
                on_lookup();
                let lookup = match self
                    .lookup_inflation_rewards(addresses, epoch, bounds, cached_epoch, budget)
                    .await
                {
                    Ok(lookup) => lookup,
                    Err(err) => {
                        let _ = evaluated.set(None);
                        crate::metrics::inflation_reward_response_cache("uncacheable");
                        return Err(Unstored::Failed(err));
                    }
                };
                let _ = evaluated.set(Some(lookup.timings.clone()));
                // The tip read shares the lookup's budget; running out leaves the answer unstored.
                let admission = tokio::time::timeout_at(
                    deadline,
                    response_cache.admit(&lookup, unique_addresses, tip_reached),
                )
                .await
                .unwrap_or(Err("unsettled"));
                match (admission, lookup.outcome) {
                    (Ok(boundary), InflationRewardLookupOutcome::Complete(records))
                        if key_boundary.as_ref() == Some(&boundary) =>
                    {
                        Ok(InflationResponseValue::from(records))
                    }
                    (Ok(boundary), outcome) if key_boundary.is_none() => Err(Unstored::Answer {
                        outcome,
                        store_under: Some(boundary),
                    }),
                    (admission, outcome) => {
                        crate::metrics::inflation_reward_response_cache(
                            admission.err().unwrap_or("uncacheable"),
                        );
                        Err(Unstored::Answer {
                            outcome,
                            store_under: None,
                        })
                    }
                }
            }
        };

        let result = cache.try_get_with(key.clone(), init).await;
        let evaluated = evaluated.get().cloned();
        let unstored = match result {
            Ok(records) => {
                let outcome = InflationRewardLookupOutcome::Complete(records.to_vec());
                return Ok(match evaluated.flatten() {
                    Some(timings) => {
                        response_cache.inserted(cache).await;
                        InflationRewardAnswer::ClickHouse { outcome, timings }
                    }
                    None => {
                        crate::metrics::inflation_reward_response_cache("coalesced");
                        InflationRewardAnswer::ResponseCache(outcome)
                    }
                });
            }
            Err(unstored) => unstored,
        };

        match (&*unstored, evaluated) {
            (
                Unstored::Answer {
                    outcome,
                    store_under,
                },
                Some(Some(timings)),
            ) => {
                if let (Some(boundary), InflationRewardLookupOutcome::Complete(records)) =
                    (store_under, outcome)
                {
                    response_cache
                        .store_cold(cache, &self.inflation_epoch_cache, &key, boundary, records)
                        .await;
                }
                Ok(InflationRewardAnswer::ClickHouse {
                    outcome: outcome.clone(),
                    timings,
                })
            }
            (Unstored::Answer { outcome, .. }, _) => {
                crate::metrics::inflation_reward_response_cache("coalesced");
                Ok(InflationRewardAnswer::ResponseCache(outcome.clone()))
            }
            (Unstored::Failed(err), Some(_)) => Err(copy_error(err)),
            (Unstored::Failed(err), None) => {
                // A leader's failure, such as its timeout, is not this request's answer.
                crate::metrics::inflation_reward_response_cache("coalesced");
                let Some(budget) = budget_left(deadline, query_timeout) else {
                    return Err(copy_error(err));
                };
                crate::metrics::inflation_reward_response_cache("retry");
                on_lookup();
                let lookup = self
                    .lookup_inflation_rewards(addresses, epoch, bounds, cached_epoch, budget)
                    .await?;
                Ok(InflationRewardAnswer::ClickHouse {
                    outcome: lookup.outcome,
                    timings: lookup.timings,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(pubkey: u8) -> InflationRewardRecord {
        InflationRewardRecord {
            pubkey: [pubkey; 32],
            effective_slot: 10,
            lamports: 1,
            post_balance: 2,
            commission: None,
            commission_bps: None,
        }
    }

    fn boundary() -> InflationBoundary {
        InflationBoundary {
            slot: 10,
            parent_blockhash: [1; 32],
            block_height: Some(5),
            num_partitions: None,
        }
    }

    #[test]
    fn key_is_the_sorted_deduplicated_address_set() {
        let forward = InflationResponseKey::new((10, 20), Some(boundary()), &[[1; 32], [2; 32]]);
        let shuffled = InflationResponseKey::new(
            (10, 20),
            Some(boundary()),
            &[[2; 32], [1; 32], [2; 32], [1; 32]],
        );
        assert_eq!(forward, shuffled);
        assert_eq!(&*forward.addresses, &[[1; 32], [2; 32]]);
        let other_boundary = InflationResponseKey::new(
            (10, 20),
            Some(InflationBoundary {
                parent_blockhash: [2; 32],
                ..boundary()
            }),
            &[[1; 32], [2; 32]],
        );
        assert_ne!(forward, other_boundary);
    }

    #[test]
    fn weight_counts_the_key_value_addresses_and_records() {
        let key = InflationResponseKey::new((10, 20), Some(boundary()), &[[1; 32], [2; 32]]);
        let empty: InflationResponseValue = Vec::new().into();
        let one: InflationResponseValue = vec![record(1)].into();
        let inline =
            (size_of::<InflationResponseKey>() + size_of::<InflationResponseValue>()) as u64;
        assert!(ENTRY_OVERHEAD_BYTES >= inline + 128);
        assert_eq!(
            u64::from(entry_weight(&key, &empty)),
            ENTRY_OVERHEAD_BYTES + 64
        );
        assert_eq!(
            u64::from(entry_weight(&key, &one)),
            ENTRY_OVERHEAD_BYTES + 64 + size_of::<InflationRewardRecord>() as u64
        );
    }

    #[tokio::test]
    async fn late_lookups_get_only_the_budget_left() {
        let budget = Duration::from_secs(5);
        let now = Instant::now();
        let left = budget_left(now + budget, budget).expect("full budget");
        assert!(left <= budget && left > budget - Duration::from_secs(1));
        assert!(budget_left(now + MIN_LOOKUP_BUDGET / 2, budget).is_none());
        assert!(budget_left(now, budget).is_none());
        // A budget shorter than the floor still lets a fresh leader start.
        let short = Duration::from_millis(40);
        assert!(budget_left(Instant::now() + short, short).is_some());
    }

    #[test]
    fn epoch_cache_off_forces_the_response_cache_off() {
        let limits = InflationRewardQueryLimits {
            epoch_cache_max_bytes: 0,
            response_cache_max_bytes: 1 << 20,
            ..InflationRewardQueryLimits::default()
        };
        assert!(InflationResponseCache::from_limits(&limits).inner.is_none());
        let limits = InflationRewardQueryLimits {
            response_cache_max_bytes: 1 << 20,
            ..InflationRewardQueryLimits::default()
        };
        assert!(InflationResponseCache::from_limits(&limits).inner.is_some());
        assert!(
            InflationResponseCache::from_limits(&InflationRewardQueryLimits::default())
                .inner
                .is_none()
        );
    }
}
