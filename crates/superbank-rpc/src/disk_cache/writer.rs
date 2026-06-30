// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Dedicated writer thread for the disk cache.
//!
//! All RocksDB writes flow through one OS thread (not `spawn_blocking`, so the
//! shared blocking pool stays free for hydration work and watermark updates have
//! a single owner). Two bounded channels feed it: `live` (finalized slots from
//! the DragonsMouth stream; producers only ever `try_send` so the gRPC task can
//! never block on disk) and `bulk` (backfill/repair batches). The loop drains
//! live jobs fully before taking one bulk job, so the tip always wins.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, TrySendError, sync_channel};
use std::time::{Duration, Instant};

use tracing::{info, warn};

use crate::clickhouse::{BlockMetadataRecord, StoredTransactionRecord};

use super::ingest::RepairQueue;
use super::{DiskCache, DiskCacheInner, schema};

const EVICTION_TICK: Duration = Duration::from_secs(30);
const RECV_TIMEOUT: Duration = Duration::from_millis(100);

pub(crate) enum DiskWriteJob {
    LiveSlot {
        meta: Box<BlockMetadataRecord>,
        txs: Vec<Arc<StoredTransactionRecord>>,
    },
    /// Backfill/repair batch; written in order, oldest within the batch first.
    FillBatch {
        source: u8,
        blocks: Vec<(BlockMetadataRecord, Vec<Arc<StoredTransactionRecord>>)>,
    },
    Shutdown,
}

#[derive(Clone)]
pub(crate) struct DiskWriteSender {
    live: SyncSender<DiskWriteJob>,
    repair: Arc<RepairQueue>,
}

impl DiskWriteSender {
    /// Enqueue a finalized slot without ever blocking. A full queue routes the
    /// slot to the repair queue (it will be refilled from ClickHouse).
    pub(crate) fn send_live(
        &self,
        meta: BlockMetadataRecord,
        txs: Vec<Arc<StoredTransactionRecord>>,
    ) {
        let slot = meta.slot;
        match self.live.try_send(DiskWriteJob::LiveSlot {
            meta: Box::new(meta),
            txs,
        }) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                warn!(
                    slot,
                    "disk cache: live write queue full; deferring slot to repair"
                );
                crate::metrics::disk_cache_dropped_to_repair("queue_full");
                self.repair.push(slot);
            }
            Err(TrySendError::Disconnected(_)) => {
                warn!(slot, "disk cache: writer is gone; dropping live slot");
            }
        }
    }

    pub(crate) fn send_shutdown(&self) {
        let _ = self.live.try_send(DiskWriteJob::Shutdown);
    }
}

pub(crate) struct DiskWriterHandle {
    pub(crate) sender: DiskWriteSender,
    pub(crate) bulk: SyncSender<DiskWriteJob>,
    stop: Arc<AtomicBool>,
    join: Option<std::thread::JoinHandle<()>>,
}

impl DiskWriterHandle {
    /// Signal the writer and wait for it to drain and flush the WAL.
    pub(crate) fn shutdown(mut self) {
        self.stop.store(true, Ordering::Release);
        self.sender.send_shutdown();
        if let Some(join) = self.join.take()
            && join.join().is_err()
        {
            warn!("disk cache: writer thread panicked during shutdown");
        }
    }
}

impl DiskCache {
    pub(crate) fn spawn_writer(
        &self,
        repair: Arc<RepairQueue>,
        live_queue_slots: usize,
    ) -> DiskWriterHandle {
        let (live_tx, live_rx) = sync_channel(live_queue_slots.max(1));
        let (bulk_tx, bulk_rx) = sync_channel(2);
        let stop = Arc::new(AtomicBool::new(false));

        let inner = self.inner_arc();
        let thread_stop = stop.clone();
        let thread_repair = repair.clone();
        let join = std::thread::Builder::new()
            .name("superbank-disk-writer".to_string())
            .spawn(move || writer_loop(inner, live_rx, bulk_rx, thread_repair, thread_stop))
            .expect("spawn disk-cache writer thread");

        DiskWriterHandle {
            sender: DiskWriteSender {
                live: live_tx,
                repair,
            },
            bulk: bulk_tx,
            stop,
            join: Some(join),
        }
    }
}

fn writer_loop(
    inner: Arc<DiskCacheInner>,
    live_rx: Receiver<DiskWriteJob>,
    bulk_rx: Receiver<DiskWriteJob>,
    repair: Arc<RepairQueue>,
    stop: Arc<AtomicBool>,
) {
    info!("disk cache: writer thread started");
    let mut last_eviction = Instant::now();

    'outer: loop {
        if stop.load(Ordering::Acquire) {
            break;
        }

        // Drain everything queued on the live channel first.
        loop {
            match live_rx.try_recv() {
                Ok(DiskWriteJob::Shutdown) => break 'outer,
                Ok(job) => handle_job(&inner, job, &repair),
                Err(_) => break,
            }
        }

        // At most one bulk job per round, so live slots never wait long.
        match bulk_rx.try_recv() {
            Ok(DiskWriteJob::Shutdown) => break 'outer,
            Ok(job) => {
                handle_job(&inner, job, &repair);
                maybe_evict_tick(&inner, &mut last_eviction);
                continue;
            }
            Err(_) => {}
        }

        match live_rx.recv_timeout(RECV_TIMEOUT) {
            Ok(DiskWriteJob::Shutdown) => break,
            Ok(job) => handle_job(&inner, job, &repair),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }

        maybe_evict_tick(&inner, &mut last_eviction);
    }

    if let Err(err) = inner.flush_wal() {
        warn!("disk cache: WAL flush on shutdown failed: {err}");
    }
    info!("disk cache: writer thread stopped");
}

fn handle_job(inner: &DiskCacheInner, job: DiskWriteJob, repair: &RepairQueue) {
    match job {
        DiskWriteJob::LiveSlot { meta, txs } => {
            write_one(inner, *meta, txs, schema::COVERAGE_SOURCE_LIVE, repair);
        }
        DiskWriteJob::FillBatch { source, blocks } => {
            for (meta, txs) in blocks {
                write_one(inner, meta, txs, source, repair);
            }
        }
        DiskWriteJob::Shutdown => {}
    }
}

fn write_one(
    inner: &DiskCacheInner,
    meta: BlockMetadataRecord,
    txs: Vec<Arc<StoredTransactionRecord>>,
    source: u8,
    repair: &RepairQueue,
) {
    let slot = meta.slot;
    // Reconnects replay finalized updates; coverage makes the write idempotent.
    if inner.covers_slot(slot) {
        return;
    }
    let start = Instant::now();
    match inner.write_finalized_slot(&meta, &txs, source) {
        Ok(()) => {
            crate::metrics::disk_cache_write(
                schema::coverage_source_label(source),
                txs.len() as u64,
                start.elapsed().as_secs_f64(),
            );
        }
        Err(err) => {
            warn!(
                slot,
                "disk cache: slot write failed ({err}); deferring to repair"
            );
            crate::metrics::disk_cache_write_error();
            crate::metrics::disk_cache_dropped_to_repair("write_error");
            repair.push(slot);
        }
    }
}

fn maybe_evict_tick(inner: &DiskCacheInner, last_eviction: &mut Instant) {
    if last_eviction.elapsed() < EVICTION_TICK {
        return;
    }
    *last_eviction = Instant::now();
    match inner.maybe_evict() {
        Ok(Some(stats)) => {
            info!(
                old_floor = stats.old_floor,
                new_floor = stats.new_floor,
                byte_budget_bound = stats.byte_budget_bound,
                "disk cache: evicted slots below floor"
            );
        }
        Ok(None) => {}
        Err(err) => warn!("disk cache: eviction failed: {err}"),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use solana_commitment_config::CommitmentLevel;
    use solana_sdk::{pubkey::Pubkey, signature::Signature};
    use solana_transaction_status::TransactionDetails;

    use crate::disk_cache::ingest::{DiskIngestSink, RepairQueue};
    use crate::disk_cache::tests::{block_metadata, test_config, transaction};
    use crate::disk_cache::{DiskBlockResult, DiskCache};
    use crate::head_cache::HeadCache;

    fn finalized_head_with_block(slot: u64, tx_count: u32) -> HeadCache {
        let head = HeadCache::new(64, 64);
        head.note_block_metadata(block_metadata(slot, slot - 1, u64::from(tx_count)));
        for idx in 0..tx_count {
            let record = transaction(slot, idx);
            head.insert_for_tests(
                Signature::from(record.signature),
                record,
                idx,
                &[Pubkey::new_unique()],
                CommitmentLevel::Finalized,
            );
        }
        head
    }

    async fn wait_until(deadline: Duration, mut condition: impl FnMut() -> bool) -> bool {
        let start = std::time::Instant::now();
        while start.elapsed() < deadline {
            if condition() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        condition()
    }

    #[tokio::test]
    async fn sink_writes_finalized_slot_through_writer() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = Arc::new(DiskCache::open(test_config(&dir)).expect("open"));
        let repair = Arc::new(RepairQueue::new(100));
        let handle = cache.spawn_writer(repair.clone(), 64);
        let sink = DiskIngestSink::new(cache.clone(), handle.sender.clone(), repair.clone());

        let head = finalized_head_with_block(100, 2);
        sink.on_slot_finalized(100, &head);

        assert!(
            wait_until(Duration::from_secs(5), || cache.covers_slot(100)).await,
            "writer should cover the slot"
        );
        match cache.get_block(100, TransactionDetails::Full).await {
            DiskBlockResult::Found(payload) => {
                assert_eq!(payload.observed_transaction_count(), Some(2));
            }
            other => panic!("unexpected: {other:?}"),
        }

        // Replayed finalized updates are no-ops.
        sink.on_slot_finalized(100, &head);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(repair.len(), 0);

        handle.shutdown();
    }

    #[tokio::test]
    async fn incomplete_head_view_defers_to_repair() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = Arc::new(DiskCache::open(test_config(&dir)).expect("open"));
        let repair = Arc::new(RepairQueue::new(100));
        let handle = cache.spawn_writer(repair.clone(), 64);
        let sink = DiskIngestSink::new(cache.clone(), handle.sender.clone(), repair.clone());

        // Block meta claims 2 transactions but only 1 reached the head cache.
        let head = HeadCache::new(64, 64);
        head.note_block_metadata(block_metadata(200, 199, 2));
        let record = transaction(200, 0);
        head.insert_for_tests(
            Signature::from(record.signature),
            record,
            0,
            &[Pubkey::new_unique()],
            CommitmentLevel::Finalized,
        );

        sink.on_slot_finalized(200, &head);
        assert_eq!(repair.drain(10), vec![200]);
        assert!(!cache.covers_slot(200));
        handle.shutdown();
    }

    #[tokio::test]
    async fn bulk_fill_batch_is_written() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = Arc::new(DiskCache::open(test_config(&dir)).expect("open"));
        let repair = Arc::new(RepairQueue::new(100));
        let handle = cache.spawn_writer(repair.clone(), 64);

        let blocks = (300..303u64)
            .map(|slot| {
                let txs = vec![Arc::new(transaction(slot, 0))];
                (block_metadata(slot, slot - 1, 1), txs)
            })
            .collect();
        handle
            .bulk
            .send(super::DiskWriteJob::FillBatch {
                source: crate::disk_cache::schema::COVERAGE_SOURCE_BACKFILL,
                blocks,
            })
            .expect("send bulk");

        assert!(
            wait_until(Duration::from_secs(5), || cache.covers_slot(302)).await,
            "bulk batch should land"
        );
        assert!(cache.covers_slot(300) && cache.covers_slot(301));
        handle.shutdown();
    }

    #[test]
    fn finalized_block_snapshot_orders_and_validates() {
        let head = HeadCache::new(64, 64);
        head.note_block_metadata(block_metadata(300, 299, 2));
        let tx_late = transaction(300, 5);
        let tx_early = transaction(300, 2);
        head.insert_for_tests(
            Signature::from(tx_late.signature),
            tx_late,
            5,
            &[Pubkey::new_unique()],
            CommitmentLevel::Finalized,
        );
        head.insert_for_tests(
            Signature::from(tx_early.signature),
            tx_early,
            2,
            &[Pubkey::new_unique()],
            CommitmentLevel::Finalized,
        );

        let (meta, records) = head.finalized_block_snapshot(300).expect("snapshot");
        assert_eq!(meta.slot, 300);
        let indexes: Vec<u32> = records.iter().map(|record| record.slot_idx).collect();
        assert_eq!(indexes, vec![2, 5]);

        // Unknown slot and incomplete blocks are not snapshottable.
        assert!(head.finalized_block_snapshot(301).is_none());
        head.note_block_metadata(block_metadata(302, 301, 3));
        assert!(head.finalized_block_snapshot(302).is_none());

        // Zero-transaction blocks are.
        head.note_block_metadata(block_metadata(303, 302, 0));
        let (_, records) = head.finalized_block_snapshot(303).expect("snapshot");
        assert!(records.is_empty());
    }
}
