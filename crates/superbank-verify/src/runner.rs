// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Windowed fetch -> verify -> account pipeline. A fetcher task streams slot
//! windows from ClickHouse a few windows ahead; verification fans out across
//! a rayon pool (block-level parallelism — each block starts from its
//! recorded parent_blockhash); accounting, chain linkage, reporting, and
//! checkpointing stay sequential so windows complete strictly in order.

use std::collections::{BTreeMap, HashMap};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use rayon::prelude::*;
use tracing::{info, warn};

use crate::chain::{ChainWalk, ObserveResult, span_len};
use crate::chdb::{ChDb, TxSignaturesBySlot};
use crate::checkpoint::{self, Checkpoint, JobDescriptor};
use crate::cli::Args;
use crate::epoch::EpochSlots;
use crate::metrics;
use crate::range::RangeSpec;
use crate::report::{
    Finding, FindingClass, FindingCode, ReportWriter, RunCounters, SlotStatus, log_finding,
};
use crate::verify::{BlockInfo, EntryInfo, VerifyMode, VerifyOutcome, verify_block};

struct WindowData {
    end: u64,
    blocks: Vec<BlockInfo>,
    entries: BTreeMap<u64, Vec<EntryInfo>>,
    tx_signatures: TxSignaturesBySlot,
    duplicate_findings: Vec<Finding>,
    /// Parent block of the first in-range block, fetched alongside the first
    /// window that contains any block so the chain walk can check linkage
    /// across the range start.
    parent_seed: Option<BlockInfo>,
}

pub(crate) async fn run(args: &Args) -> Result<RunCounters> {
    let db = ChDb::new(args);
    let epochs = EpochSlots::new(args.slots_per_epoch, args.epoch_warmup);
    let (range_start, range_end) = resolve_range(args, &db, &epochs).await?;
    let mode = args.mode.to_verify_mode();
    info!(
        range_start,
        range_end,
        mode = args.mode.as_str(),
        "verification range resolved"
    );
    metrics::set_range(range_start, range_end);

    if args.expected_genesis_hash.is_none() && range_start == 0 {
        warn!(
            "no expected genesis hash configured; slot 0 will anchor on its recorded \
             parent_blockhash without external verification"
        );
    }

    let descriptor = JobDescriptor {
        range_start,
        range_end,
        mode: args.mode.as_str().to_string(),
        blocks_table: args.blocks_table.clone(),
        entries_table: args.entries_table.clone(),
        transactions_table: args.transactions_table.clone(),
        ticks_per_slot: args.ticks_per_slot,
        hashes_per_tick_schedule: args.hashes_per_tick_schedule.to_spec(),
        expected_genesis_hash: args.expected_genesis_hash,
        anchors: canonical_anchors(&args.anchors),
    };

    let mut counters = RunCounters::default();
    let mut cursor = range_start;
    let mut resumed = false;
    let mut checked_anchors = Default::default();
    let mut genesis_checked = false;
    if args.resume
        && let Some(path) = args.checkpoint_file.as_deref()
        && let Some(saved) = checkpoint::load_for_resume(path, &descriptor, args.full)?
    {
        cursor = saved.next_start;
        counters = saved.counters;
        checked_anchors = saved.checked_anchors;
        genesis_checked = saved.genesis_checked;
        resumed = true;
        info!(
            next_start = cursor,
            slots_ok = counters.slots_ok,
            slots_failed = counters.slots_failed,
            "resuming from checkpoint"
        );
        if cursor > 0 {
            metrics::set_last_processed_slot(cursor - 1);
        }
    }

    if cursor > range_end {
        info!("checkpoint already covers the requested range");
        log_summary(&counters);
        return Ok(counters);
    }

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(args.verify_threads)
        .thread_name(|index| format!("verify-{index}"))
        .build()
        .context("build verification thread pool")?;

    let (sender, mut receiver) = tokio::sync::mpsc::channel::<Result<WindowData>>(args.fetch_ahead);
    let fetch_db = db.clone();
    let window_slots = args.window_slots;
    let fetch_transactions = mode == VerifyMode::Full;
    let fetch_cursor = cursor;
    let fetcher = tokio::spawn(async move {
        let mut start = fetch_cursor;
        let mut need_parent_seed = true;
        while start <= range_end {
            let end = range_end.min(start.saturating_add(window_slots - 1));
            let window =
                fetch_window(&fetch_db, start, end, fetch_transactions, need_parent_seed).await;
            if let Ok(window) = &window
                && !window.blocks.is_empty()
            {
                need_parent_seed = false;
            }
            let failed = window.is_err();
            if sender.send(window).await.is_err() || failed {
                return;
            }
            start = end + 1;
        }
    });

    let anchors: HashMap<u64, [u8; 32]> = args.anchors.iter().copied().collect();
    let mut walk = ChainWalk::from_checkpoint(
        cursor,
        args.expected_genesis_hash,
        anchors,
        checked_anchors,
        genesis_checked,
    );
    let mut report = ReportWriter::new(args.report_file.as_deref(), resumed)?;
    let mut progress = ProgressTracker::new(cursor, range_end, args.progress_every_slots);
    let mut aborted = false;

    'windows: while let Some(window) = receiver.recv().await {
        let window = window?;
        let window_started = Instant::now();

        if let Some(seed) = &window.parent_seed {
            walk.seed(seed.slot, seed.blockhash);
        }

        let outcomes = tokio::task::block_in_place(|| {
            verify_window(
                &window,
                &pool,
                mode,
                args.ticks_per_slot,
                &args.hashes_per_tick_schedule,
            )
        });

        let mut duplicates_by_slot: BTreeMap<u64, Vec<Finding>> = BTreeMap::new();
        for finding in window.duplicate_findings {
            duplicates_by_slot
                .entry(finding.slot)
                .or_default()
                .push(finding);
        }

        for (block, outcome) in window.blocks.iter().zip(outcomes) {
            let chain_result = walk.observe_block(block);
            account_spans(&chain_result, &mut counters);

            let mut findings = chain_result.findings;
            findings.extend(outcome.findings);
            if let Some(duplicates) = duplicates_by_slot.remove(&block.slot) {
                findings.extend(duplicates);
            }

            let status = slot_status(&findings);
            counters.record_slot(status);
            metrics::observe_slot_status(status, 1);
            counters.entries_verified += outcome.entries_verified;
            counters.hashes_computed += outcome.hashes_computed;
            metrics::observe_verified(outcome.entries_verified, outcome.hashes_computed);

            for finding in &findings {
                counters.record_finding(finding.code);
                metrics::observe_finding(finding.code);
                log_finding(finding);
                report.write_finding(finding)?;
            }

            if args.max_failures > 0 && counters.failure_count() >= args.max_failures {
                tracing::error!(
                    failed_slots = counters.slots_failed,
                    orphan_failures = counters.orphan_failures,
                    max_failures = args.max_failures,
                    "aborting: failure budget exhausted"
                );
                aborted = true;
            }
        }

        // Duplicate conflicts on slots without a blocks_metadata row in this
        // window (entries/transactions orphans): they carry no slot status,
        // but Fail-class findings must still drive the exit code.
        for (_, findings) in duplicates_by_slot {
            for finding in findings {
                if finding.class == FindingClass::Fail {
                    counters.orphan_failures += 1;
                }
                counters.record_finding(finding.code);
                metrics::observe_finding(finding.code);
                log_finding(&finding);
                report.write_finding(&finding)?;
            }
        }

        report.flush()?;
        metrics::observe_window_completed(window_started.elapsed().as_secs_f64(), window.end);
        if let Some(path) = args.checkpoint_file.as_deref() {
            // Slots after the last present block are not yet classified —
            // they await the next block's parent-link claim (or the tail
            // pass) — so the resume cursor must not run past them.
            let accounted_through = walk
                .last_present()
                .map(|slot| (slot + 1).max(cursor))
                .unwrap_or(cursor);
            checkpoint::save(
                path,
                &checkpoint_state(&descriptor, accounted_through, &counters, &walk),
            )?;
        }
        progress.observe(window.end, &counters);

        if aborted {
            break 'windows;
        }
    }
    drop(receiver);
    fetcher.await.context("window fetcher panicked")?;

    if !aborted {
        // Classify the tail of the range after the last present block using
        // the first block beyond the range (its parent link claims which tail
        // slots were skipped).
        let next_block = db.fetch_first_block_after(range_end).await?;
        let tail = walk.observe_tail(range_end, next_block.as_ref());
        account_spans(&tail, &mut counters);
        for finding in &tail.findings {
            counters.record_finding(finding.code);
            metrics::observe_finding(finding.code);
            log_finding(finding);
            report.write_finding(finding)?;
        }

        // An anchor that never met a block proves nothing: surface it rather
        // than letting the operator believe it was compared.
        for (slot, _) in walk.unchecked_anchors() {
            let finding = Finding::new(
                slot,
                FindingCode::AnchorNotChecked,
                "anchor was never compared: slot has no block in the verified range \
                 (skipped, missing, or outside the range)",
            );
            counters.anchors_unchecked += 1;
            counters.record_finding(finding.code);
            metrics::observe_finding(finding.code);
            log_finding(&finding);
            report.write_finding(&finding)?;
        }
        report.flush()?;
        if let Some(path) = args.checkpoint_file.as_deref() {
            checkpoint::save(
                path,
                &checkpoint_state(&descriptor, range_end + 1, &counters, &walk),
            )?;
        }
    }

    log_summary(&counters);
    Ok(counters)
}

async fn fetch_window(
    db: &ChDb,
    start: u64,
    end: u64,
    fetch_transactions: bool,
    need_parent_seed: bool,
) -> Result<WindowData> {
    let include_transactions = fetch_transactions;
    let fetch_transactions = async {
        if include_transactions {
            db.fetch_tx_signatures(start, end).await
        } else {
            Ok(TxSignaturesBySlot::new())
        }
    };
    let (blocks, entries, tx_signatures, duplicate_findings) = tokio::try_join!(
        db.fetch_blocks(start, end),
        db.fetch_entries(start, end),
        fetch_transactions,
        db.fetch_duplicate_conflicts(start, end, include_transactions),
    )?;

    let parent_seed = match blocks.first() {
        Some(block) if need_parent_seed && block.slot != 0 && block.parent_slot < start => {
            db.fetch_block_at(block.parent_slot).await?
        }
        _ => None,
    };

    Ok(WindowData {
        end,
        blocks,
        entries,
        tx_signatures,
        duplicate_findings,
        parent_seed,
    })
}

fn canonical_anchors(anchors: &[(u64, [u8; 32])]) -> Vec<(u64, [u8; 32])> {
    let mut anchors = anchors.to_vec();
    anchors.sort_unstable();
    anchors
}

fn verify_window(
    window: &WindowData,
    pool: &rayon::ThreadPool,
    mode: VerifyMode,
    ticks_per_slot: u64,
    schedule: &crate::eras::HashesPerTickSchedule,
) -> Vec<VerifyOutcome> {
    let empty_tx: BTreeMap<u32, Vec<[u8; 64]>> = BTreeMap::new();
    pool.install(|| {
        window
            .blocks
            .par_iter()
            .map(|block| {
                let entries = window.entries.get(&block.slot);
                match entries.filter(|entries| !entries.is_empty()) {
                    Some(entries) => verify_block(
                        mode,
                        block,
                        entries,
                        window.tx_signatures.get(&block.slot).unwrap_or(&empty_tx),
                        ticks_per_slot,
                        Some(schedule.value_at(block.slot)),
                    ),
                    None => VerifyOutcome {
                        findings: vec![Finding::new(
                            block.slot,
                            FindingCode::MissingEntries,
                            "block has no entries rows; PoH unverifiable \
                             (range ingested without entry data?)",
                        )],
                        ..VerifyOutcome::default()
                    },
                }
            })
            .collect()
    })
}

fn checkpoint_state(
    descriptor: &JobDescriptor,
    next_start: u64,
    counters: &RunCounters,
    walk: &ChainWalk,
) -> Checkpoint {
    Checkpoint {
        descriptor: descriptor.clone(),
        next_start,
        counters: counters.clone(),
        checked_anchors: walk.checked_anchors(),
        genesis_checked: walk.genesis_checked(),
        updated_unix: checkpoint::now_unix(),
    }
}

async fn resolve_range(args: &Args, db: &ChDb, epochs: &EpochSlots) -> Result<(u64, u64)> {
    if args.full {
        let Some((min_slot, max_slot)) = db.fetch_bounds().await? else {
            bail!("no blocks found in {}", db.blocks_table);
        };
        if min_slot > 0 {
            warn!(
                first_present_slot = min_slot,
                "blocks_metadata does not reach back to genesis; verification anchors on the \
                 recorded parent_blockhash at the first present slot"
            );
        }
        return Ok((min_slot, max_slot));
    }

    match args.range {
        Some(RangeSpec::Slots { start, end }) => Ok((start, end)),
        Some(RangeSpec::Epochs { start, end }) => Ok((
            epochs.first_slot_in_epoch(start),
            epochs.last_slot_in_epoch(end),
        )),
        None => bail!("no range specified"),
    }
}

fn account_spans(result: &ObserveResult, counters: &mut RunCounters) {
    let skipped = span_len(result.skipped_span);
    if skipped > 0 {
        counters.slots_skipped += skipped;
        metrics::observe_slot_status(SlotStatus::Skipped, skipped);
    }
    let missing = span_len(result.missing_span);
    if missing > 0 {
        counters.slots_missing += missing;
        metrics::observe_slot_status(SlotStatus::Missing, missing);
    }
}

fn slot_status(findings: &[Finding]) -> SlotStatus {
    let mut status = SlotStatus::Ok;
    for finding in findings {
        match finding.class {
            FindingClass::Fail => return SlotStatus::Failed,
            FindingClass::Unverifiable => status = SlotStatus::Unverifiable,
            FindingClass::Warn => {}
        }
    }
    status
}

fn log_summary(counters: &RunCounters) {
    info!(
        slots_ok = counters.slots_ok,
        slots_failed = counters.slots_failed,
        slots_unverifiable = counters.slots_unverifiable,
        slots_skipped = counters.slots_skipped,
        slots_missing = counters.slots_missing,
        orphan_failures = counters.orphan_failures,
        anchors_unchecked = counters.anchors_unchecked,
        entries_verified = counters.entries_verified,
        hashes_computed = counters.hashes_computed,
        "verification finished"
    );
    for (code, count) in &counters.findings {
        info!(code = code.as_str(), count, "finding totals");
    }
}

struct ProgressTracker {
    started: Instant,
    range_start: u64,
    range_end: u64,
    log_every: u64,
    next_log_at: u64,
}

impl ProgressTracker {
    fn new(range_start: u64, range_end: u64, log_every: u64) -> Self {
        Self {
            started: Instant::now(),
            range_start,
            range_end,
            log_every: log_every.max(1),
            next_log_at: range_start.saturating_add(log_every.max(1)),
        }
    }

    fn observe(&mut self, processed_through: u64, counters: &RunCounters) {
        if processed_through < self.next_log_at && processed_through < self.range_end {
            return;
        }
        self.next_log_at = processed_through.saturating_add(self.log_every);

        let total = self.range_end - self.range_start + 1;
        let processed = processed_through - self.range_start + 1;
        let percent = processed as f64 / total as f64 * 100.0;
        let elapsed = self.started.elapsed().as_secs_f64();
        let rate = processed as f64 / elapsed.max(f64::EPSILON);
        let eta_seconds = ((total - processed) as f64 / rate.max(f64::EPSILON)) as u64;
        info!(
            processed,
            total,
            percent = format!("{percent:.2}"),
            slots_per_second = format!("{rate:.0}"),
            eta_seconds,
            slots_ok = counters.slots_ok,
            slots_failed = counters.slots_failed,
            slots_unverifiable = counters.slots_unverifiable,
            "verification progress"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::verify::test_support::build_block;

    #[test]
    fn full_window_execution_preserves_clean_poh_and_chain_results() {
        let (block, entries, signatures) = build_block(
            9,
            8,
            [1; 32],
            &[(25, 0), (10, 2), (15, 0), (25, 0), (5, 1), (20, 0)],
        );
        let blockhash = block.blockhash;
        let mut entries_by_slot = BTreeMap::new();
        entries_by_slot.insert(block.slot, entries);
        let mut signatures_by_slot = TxSignaturesBySlot::new();
        signatures_by_slot.insert(block.slot, signatures);
        let window = WindowData {
            end: block.slot,
            blocks: vec![block],
            entries: entries_by_slot,
            tx_signatures: signatures_by_slot,
            duplicate_findings: Vec::new(),
            parent_seed: None,
        };
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .unwrap();

        let outcomes = verify_window(
            &window,
            &pool,
            VerifyMode::Full,
            4,
            &crate::eras::HashesPerTickSchedule::parse("0:25").unwrap(),
        );
        assert_eq!(outcomes.len(), 1);
        assert!(
            outcomes[0].findings.is_empty(),
            "{:?}",
            outcomes[0].findings
        );
        assert_eq!(outcomes[0].entries_verified, 6);

        let mut walk = ChainWalk::new(9, None, HashMap::new());
        walk.seed(8, [1; 32]);
        let chain = walk.observe_block(&window.blocks[0]);
        assert!(chain.findings.is_empty());
        assert_eq!(slot_status(&outcomes[0].findings), SlotStatus::Ok);
        assert_eq!(walk.last_present(), Some(9));
        assert_eq!(blockhash, window.blocks[0].blockhash);
    }

    #[test]
    fn missing_entries_are_classified_unverifiable_by_window_execution() {
        let window = WindowData {
            end: 10,
            blocks: vec![BlockInfo {
                slot: 10,
                parent_slot: 9,
                blockhash: [2; 32],
                parent_blockhash: [1; 32],
                executed_transaction_count: 0,
                entry_count: 1,
            }],
            entries: BTreeMap::new(),
            tx_signatures: TxSignaturesBySlot::new(),
            duplicate_findings: Vec::new(),
            parent_seed: None,
        };
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .unwrap();

        let outcomes = verify_window(
            &window,
            &pool,
            VerifyMode::Structural,
            64,
            &crate::eras::HashesPerTickSchedule::mainnet(),
        );

        assert_eq!(outcomes.len(), 1);
        assert_eq!(slot_status(&outcomes[0].findings), SlotStatus::Unverifiable);
        assert_eq!(outcomes[0].findings[0].code, FindingCode::MissingEntries);
    }
}
