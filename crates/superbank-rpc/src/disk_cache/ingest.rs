// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Live ingestion hook: copies finalized slots out of the head cache into the
//! disk-cache write queue.
//!
//! The disk cache deliberately has no gRPC stream of its own — a second
//! finalized block subscription would double the firehose. Instead, when the
//! head cache observes a slot reaching finalized commitment, the sink snapshots
//! the complete block (Arc refs, no deep clone) and hands it to the writer.
//! Anything that cannot be snapshotted — incomplete head data, an evicted slot,
//! a full write queue — lands in the [`RepairQueue`] to be refilled from
//! ClickHouse.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use tracing::warn;

use crate::head_cache::HeadCache;

use super::DiskCache;
use super::writer::DiskWriteSender;

/// Slots that must be (re)fetched from ClickHouse. Bounded: on overflow the
/// newest slots win, because dropped holes are re-discovered by the periodic
/// coverage scan anyway.
pub(crate) struct RepairQueue {
    slots: Mutex<BTreeSet<u64>>,
    notify: tokio::sync::Notify,
    capacity: usize,
}

impl RepairQueue {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            slots: Mutex::new(BTreeSet::new()),
            notify: tokio::sync::Notify::new(),
            capacity: capacity.max(1),
        }
    }

    pub(crate) fn push(&self, slot: u64) {
        let mut slots = self.slots.lock().expect("repair queue lock");
        if slots.len() >= self.capacity {
            // Keep the newest slots: they serve the most traffic, and the
            // periodic hole scan re-finds anything dropped here.
            if slots.first().is_some_and(|&oldest| oldest < slot) {
                slots.pop_first();
            } else {
                return;
            }
        }
        slots.insert(slot);
        drop(slots);
        self.notify.notify_one();
    }

    /// Take up to `max` slots, newest first.
    pub(crate) fn drain(&self, max: usize) -> Vec<u64> {
        let mut slots = self.slots.lock().expect("repair queue lock");
        let mut drained = Vec::with_capacity(max.min(slots.len()));
        while drained.len() < max {
            match slots.pop_last() {
                Some(slot) => drained.push(slot),
                None => break,
            }
        }
        drained
    }

    pub(crate) fn len(&self) -> usize {
        self.slots.lock().expect("repair queue lock").len()
    }

    /// Wait until at least one slot is queued.
    pub(crate) async fn notified(&self) {
        self.notify.notified().await;
    }
}

pub(crate) struct DiskIngestSink {
    cache: Arc<DiskCache>,
    sender: DiskWriteSender,
    repair: Arc<RepairQueue>,
}

impl std::fmt::Debug for DiskIngestSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DiskIngestSink")
    }
}

impl DiskIngestSink {
    pub(crate) fn new(
        cache: Arc<DiskCache>,
        sender: DiskWriteSender,
        repair: Arc<RepairQueue>,
    ) -> Self {
        Self {
            cache,
            sender,
            repair,
        }
    }

    /// Called from the DragonsMouth stream task on every finalized commitment
    /// update. Must never block.
    pub(crate) fn on_slot_finalized(&self, slot: u64, head: &HeadCache) {
        if slot < self.cache.min_retained_slot() || self.cache.covers_slot(slot) {
            return;
        }

        match head.finalized_block_snapshot(slot) {
            Some((meta, txs)) => self.sender.send_live(meta, txs),
            None => {
                warn!(
                    slot,
                    "disk cache: head cache view of finalized slot is incomplete; deferring to repair"
                );
                crate::metrics::disk_cache_dropped_to_repair("incomplete_head");
                self.repair.push(slot);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repair_queue_caps_and_keeps_newest() {
        let queue = RepairQueue::new(3);
        queue.push(10);
        queue.push(30);
        queue.push(20);
        assert_eq!(queue.len(), 3);

        // Over capacity: newer slot displaces the oldest.
        queue.push(40);
        assert_eq!(queue.len(), 3);
        assert_eq!(queue.drain(10), vec![40, 30, 20]);
        assert_eq!(queue.len(), 0);

        // Over capacity with an older slot: dropped.
        queue.push(10);
        queue.push(20);
        queue.push(30);
        queue.push(5);
        assert_eq!(queue.drain(2), vec![30, 20]);
        assert_eq!(queue.drain(10), vec![10]);
    }
}
