// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Empty-address watermarks for getSignaturesForAddress.
//!
//! An entry `address -> W` means "the primary returned no row for this address at a slot
//! `<= W`". It is written only from an empty primary page for a request without any cursor, and
//! `W` sits below the local contiguous tip the primary is known to have held (see
//! [`fill_watermark`]). A repeat request may then skip the primary when the local page covers
//! `(W, local tip]` and the head cache proves everything above the local tip. The TTL bounds how
//! long a primary backfill below `W` stays invisible; rows above `W` are always read.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::solana_sdk::pubkey::Pubkey;

/// Slots kept below the local contiguous tip when filling: a replica serving the primary page
/// may lag the replica the filler copied from, and recent slots are the ones repairs touch.
/// The local page reads `(W, tip]` anyway, so a lower `W` costs nothing but floor margin.
pub(crate) const GSFA_WATERMARK_TIP_MARGIN_SLOTS: u64 = 4_500;

/// The watermark for an empty primary page read while the local contiguous tip was `local_tip`.
pub(crate) fn fill_watermark(local_tip: u64) -> u64 {
    local_tip.saturating_sub(GSFA_WATERMARK_TIP_MARGIN_SLOTS)
}

/// Whether a local page over `[floor, ..]` leaves no gap above watermark `watermark`.
pub(crate) fn local_page_reaches_watermark(floor: u64, watermark: u64) -> bool {
    floor <= watermark.saturating_add(1)
}

struct Inner {
    entries: HashMap<Pubkey, (u64, Instant)>,
    /// Insertion order for eviction; an item whose instant no longer matches its entry is stale.
    order: VecDeque<(Pubkey, Instant)>,
}

pub(crate) struct GsfaWatermarks {
    ttl: Duration,
    max_entries: usize,
    inner: Mutex<Inner>,
}

impl GsfaWatermarks {
    pub(crate) fn new(ttl: Duration, max_entries: usize) -> Self {
        Self {
            ttl,
            max_entries: max_entries.max(1),
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                order: VecDeque::new(),
            }),
        }
    }

    pub(crate) fn enabled(&self) -> bool {
        !self.ttl.is_zero()
    }

    pub(crate) fn get(&self, address: &Pubkey, now: Instant) -> Option<u64> {
        if !self.enabled() {
            return None;
        }
        let mut inner = self.inner.lock().expect("gsfa watermark lock");
        let &(watermark, inserted) = inner.entries.get(address)?;
        if now.saturating_duration_since(inserted) < self.ttl {
            return Some(watermark);
        }
        inner.entries.remove(address);
        None
    }

    pub(crate) fn insert(&self, address: Pubkey, watermark: u64, now: Instant) {
        if !self.enabled() {
            return;
        }
        let mut inner = self.inner.lock().expect("gsfa watermark lock");
        inner.entries.insert(address, (watermark, now));
        inner.order.push_back((address, now));
        while inner.entries.len() > self.max_entries {
            let Some((oldest, at)) = inner.order.pop_front() else {
                break;
            };
            if inner.entries.get(&oldest).is_some_and(|&(_, t)| t == at) {
                inner.entries.remove(&oldest);
            }
        }
        // Re-inserted keys leave stale order items; compact before they outgrow the map.
        if inner.order.len() > self.max_entries.saturating_mul(2) {
            let Inner { entries, order } = &mut *inner;
            order.retain(|(key, at)| entries.get(key).is_some_and(|&(_, t)| t == *at));
        }
    }

    /// Drops an address that has rows again, so a stale entry never outlives its evidence.
    pub(crate) fn remove(&self, address: &Pubkey) {
        if !self.enabled() {
            return;
        }
        self.inner
            .lock()
            .expect("gsfa watermark lock")
            .entries
            .remove(address);
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner
            .lock()
            .expect("gsfa watermark lock")
            .entries
            .len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(byte: u8) -> Pubkey {
        Pubkey::new_from_array([byte; 32])
    }

    #[test]
    fn disabled_cache_never_stores() {
        let cache = GsfaWatermarks::new(Duration::ZERO, 10);
        let now = Instant::now();
        cache.insert(key(1), 100, now);
        assert_eq!(cache.get(&key(1), now), None);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn hit_until_ttl_then_miss() {
        let cache = GsfaWatermarks::new(Duration::from_secs(60), 10);
        let now = Instant::now();
        cache.insert(key(1), 100, now);
        assert_eq!(cache.get(&key(1), now + Duration::from_secs(59)), Some(100));
        assert_eq!(cache.get(&key(1), now + Duration::from_secs(60)), None);
        assert_eq!(cache.len(), 0, "an expired entry is dropped on read");
        assert_eq!(cache.get(&key(2), now), None);
    }

    #[test]
    fn evicts_oldest_insert_at_capacity() {
        let cache = GsfaWatermarks::new(Duration::from_secs(60), 2);
        let now = Instant::now();
        cache.insert(key(1), 1, now);
        cache.insert(key(2), 2, now + Duration::from_millis(1));
        // Refreshing key 1 makes key 2 the oldest live insert.
        cache.insert(key(1), 11, now + Duration::from_millis(2));
        cache.insert(key(3), 3, now + Duration::from_millis(3));
        let later = now + Duration::from_millis(4);
        assert_eq!(cache.get(&key(1), later), Some(11));
        assert_eq!(cache.get(&key(2), later), None);
        assert_eq!(cache.get(&key(3), later), Some(3));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn order_queue_stays_bounded_under_reinserts() {
        let cache = GsfaWatermarks::new(Duration::from_secs(60), 4);
        let now = Instant::now();
        for i in 0..1_000u64 {
            cache.insert(key((i % 3) as u8), i, now + Duration::from_micros(i));
        }
        let inner = cache.inner.lock().unwrap();
        assert!(inner.order.len() <= 8, "order len {}", inner.order.len());
        assert_eq!(inner.entries.len(), 3);
    }

    #[test]
    fn remove_drops_the_entry() {
        let cache = GsfaWatermarks::new(Duration::from_secs(60), 4);
        let now = Instant::now();
        cache.insert(key(1), 5, now);
        cache.remove(&key(1));
        assert_eq!(cache.get(&key(1), now), None);
    }

    #[test]
    fn fill_keeps_a_margin_below_the_local_tip() {
        assert_eq!(
            fill_watermark(10_000),
            10_000 - GSFA_WATERMARK_TIP_MARGIN_SLOTS
        );
        assert_eq!(fill_watermark(10), 0);
    }

    #[test]
    fn local_page_must_start_at_or_below_the_slot_after_the_watermark() {
        assert!(local_page_reaches_watermark(100, 99));
        assert!(local_page_reaches_watermark(50, 99));
        assert!(
            !local_page_reaches_watermark(101, 99),
            "slot 100 would be unread"
        );
        assert!(local_page_reaches_watermark(0, u64::MAX));
    }
}
