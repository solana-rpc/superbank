// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use solana_commitment_config::CommitmentLevel;
use tokio::sync::futures::OwnedNotified;
use tokio::sync::{Mutex, Notify, Semaphore};

use crate::clickhouse::ClickHouseClient;
use crate::metrics;
use crate::processing::ProcessingError;
use crate::util::{current_time_millis, ttl_millis};

#[cfg(feature = "disk-cache")]
use crate::disk_cache::DiskCache;
#[cfg(feature = "grpc-head-cache")]
use crate::head_cache::HeadCache;

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct MetricsHeaderCaptureConfig {
    pub(crate) capture_x_endpoint: bool,
    pub(crate) capture_x_rpc_node: bool,
    pub(crate) capture_x_subscription_id: bool,
    pub(crate) capture_x_account_id: bool,
}

pub(crate) struct AppState {
    pub(crate) clickhouse: ClickHouseClient,
    pub(crate) max_signatures_limit: u64,
    pub(crate) rpc_max_batch_size: usize,
    pub(crate) rpc_batch_concurrency_limit: usize,
    pub(crate) latest_slot_cache: LatestSlotCache,
    pub(crate) latest_block_height_cache: LatestBlockHeightCache,
    pub(crate) rpc_request_timeout: Duration,
    pub(crate) metrics_header_capture: MetricsHeaderCaptureConfig,
    pub(crate) hydration_sem: Arc<Semaphore>,
    #[cfg(feature = "grpc-head-cache")]
    pub(crate) head_cache: Option<Arc<HeadCache>>,
    /// Finalized recent-slot cache on local disk; consulted between the head
    /// cache and ClickHouse. Intentionally not part of latest-slot resolution:
    /// its tip never leads the head cache, so it adds nothing there.
    #[cfg(feature = "disk-cache")]
    pub(crate) disk_cache: Option<Arc<DiskCache>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LatestSlotSource {
    ClickHouse,
    #[cfg(feature = "grpc-head-cache")]
    HeadCache,
}

impl AppState {
    pub(crate) async fn resolve_latest_slot_with_source(
        &self,
        operation: &'static str,
        min_commitment: CommitmentLevel,
    ) -> Result<(u64, LatestSlotSource), ProcessingError> {
        #[cfg(feature = "grpc-head-cache")]
        if let Some(cache) = self.head_cache.as_ref() {
            let head_slot = cache.latest_slot_at_least(min_commitment);
            if head_slot > 0 {
                metrics::slot_source(operation, "head", commitment_label(min_commitment));
                return Ok((head_slot, LatestSlotSource::HeadCache));
            }
        }

        let slot = self
            .latest_slot_cache
            .get_or_refresh(&self.clickhouse)
            .await?;
        metrics::slot_source(operation, "clickhouse", commitment_label(min_commitment));
        Ok((slot, LatestSlotSource::ClickHouse))
    }
}

fn commitment_label(commitment: CommitmentLevel) -> &'static str {
    match commitment {
        CommitmentLevel::Processed => "processed",
        CommitmentLevel::Confirmed => "confirmed",
        CommitmentLevel::Finalized => "finalized",
    }
}

pub(crate) struct LatestSlotCache {
    ttl: Duration,
    pub(crate) value: AtomicU64,
    pub(crate) last_updated_ms: AtomicU64,
    refresh_lock: Mutex<Option<Arc<Notify>>>,
}

impl LatestSlotCache {
    pub(crate) fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            value: AtomicU64::new(0),
            last_updated_ms: AtomicU64::new(0),
            refresh_lock: Mutex::new(None),
        }
    }

    pub(crate) async fn get_or_refresh(
        &self,
        clickhouse: &ClickHouseClient,
    ) -> Result<u64, ProcessingError> {
        loop {
            let now_ms = current_time_millis();
            let last_ms = self.last_updated_ms.load(Ordering::Relaxed);
            let cached = self.value.load(Ordering::Relaxed);
            if last_ms != 0 && now_ms.saturating_sub(last_ms) <= ttl_millis(self.ttl) {
                return Ok(cached);
            }

            let (is_leader, leader_notify, wait_notified): (
                bool,
                Option<Arc<Notify>>,
                Option<OwnedNotified>,
            ) = {
                let mut guard = self.refresh_lock.lock().await;
                let now_ms = current_time_millis();
                let last_ms = self.last_updated_ms.load(Ordering::Relaxed);
                let cached = self.value.load(Ordering::Relaxed);
                if last_ms != 0 && now_ms.saturating_sub(last_ms) <= ttl_millis(self.ttl) {
                    return Ok(cached);
                }

                if let Some(inflight) = guard.as_ref() {
                    (
                        false,
                        None,
                        // Create the wait future while holding the lock so we can't miss a notify.
                        Some(inflight.clone().notified_owned()),
                    )
                } else {
                    let inflight = Arc::new(Notify::new());
                    *guard = Some(inflight.clone());
                    (true, Some(inflight), None)
                }
            };

            if is_leader {
                let notify = leader_notify.expect("leader must have notify");
                let result = clickhouse.get_latest_finalized_slot().await;
                match result {
                    Ok(slot_opt) => {
                        let latest = slot_opt.unwrap_or(0);
                        self.value.store(latest, Ordering::Relaxed);
                        self.last_updated_ms
                            .store(current_time_millis(), Ordering::Relaxed);
                    }
                    Err(err) => {
                        let mut guard = self.refresh_lock.lock().await;
                        *guard = None;
                        notify.notify_waiters();
                        return Err(err);
                    }
                }

                let mut guard = self.refresh_lock.lock().await;
                *guard = None;
                notify.notify_waiters();
                let cached = self.value.load(Ordering::Relaxed);
                return Ok(cached);
            }

            let notified = wait_notified.expect("waiters must have notified future");
            notified.await;
        }
    }
}

pub(crate) struct LatestBlockHeightCache {
    ttl: Duration,
    // u64::MAX represents "no value" (ClickHouse returned NULL / empty table).
    pub(crate) value: AtomicU64,
    // u64::MAX means cache seeded without a slot (tests/back-compat).
    pub(crate) slot: AtomicU64,
    pub(crate) last_updated_ms: AtomicU64,
    refresh_lock: Mutex<Option<Arc<Notify>>>,
}

impl LatestBlockHeightCache {
    const NONE_SENTINEL: u64 = u64::MAX;
    const UNKNOWN_SLOT_SENTINEL: u64 = u64::MAX;

    pub(crate) fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            value: AtomicU64::new(Self::NONE_SENTINEL),
            slot: AtomicU64::new(Self::UNKNOWN_SLOT_SENTINEL),
            last_updated_ms: AtomicU64::new(0),
            refresh_lock: Mutex::new(None),
        }
    }

    pub(crate) async fn get_or_refresh(
        &self,
        slot: u64,
        clickhouse: &ClickHouseClient,
    ) -> Result<Option<u64>, ProcessingError> {
        loop {
            let now_ms = current_time_millis();
            let last_ms = self.last_updated_ms.load(Ordering::Acquire);
            let cached = self.value.load(Ordering::Relaxed);
            let cached_slot = self.slot.load(Ordering::Relaxed);
            if last_ms != 0
                && now_ms.saturating_sub(last_ms) <= ttl_millis(self.ttl)
                && (cached_slot == slot || cached_slot == Self::UNKNOWN_SLOT_SENTINEL)
            {
                return Ok((cached != Self::NONE_SENTINEL).then_some(cached));
            }

            let (is_leader, leader_notify, wait_notified): (
                bool,
                Option<Arc<Notify>>,
                Option<OwnedNotified>,
            ) = {
                let mut guard = self.refresh_lock.lock().await;
                let now_ms = current_time_millis();
                let last_ms = self.last_updated_ms.load(Ordering::Acquire);
                let cached = self.value.load(Ordering::Relaxed);
                let cached_slot = self.slot.load(Ordering::Relaxed);
                if last_ms != 0
                    && now_ms.saturating_sub(last_ms) <= ttl_millis(self.ttl)
                    && (cached_slot == slot || cached_slot == Self::UNKNOWN_SLOT_SENTINEL)
                {
                    return Ok((cached != Self::NONE_SENTINEL).then_some(cached));
                }

                if let Some(inflight) = guard.as_ref() {
                    (
                        false,
                        None,
                        // Create the wait future while holding the lock so we can't miss a notify.
                        Some(inflight.clone().notified_owned()),
                    )
                } else {
                    let inflight = Arc::new(Notify::new());
                    *guard = Some(inflight.clone());
                    (true, Some(inflight), None)
                }
            };

            if is_leader {
                let notify = leader_notify.expect("leader must have notify");
                let result = clickhouse.get_blockhash_height_by_slot(slot).await.map(
                    |(row_opt, timings)| (row_opt.and_then(|(_blockhash, height)| height), timings),
                );
                match result {
                    Ok((height_opt, _timings)) => {
                        let stored = height_opt.unwrap_or(Self::NONE_SENTINEL);
                        self.value.store(stored, Ordering::Relaxed);
                        self.slot.store(slot, Ordering::Relaxed);
                        self.last_updated_ms
                            .store(current_time_millis(), Ordering::Release);
                    }
                    Err(err) => {
                        let mut guard = self.refresh_lock.lock().await;
                        *guard = None;
                        notify.notify_waiters();
                        return Err(err);
                    }
                }

                let mut guard = self.refresh_lock.lock().await;
                *guard = None;
                notify.notify_waiters();
                let cached = self.value.load(Ordering::Relaxed);
                return Ok((cached != Self::NONE_SENTINEL).then_some(cached));
            }

            let notified = wait_notified.expect("waiters must have notified future");
            notified.await;
        }
    }
}
