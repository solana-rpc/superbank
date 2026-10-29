// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Library entry point: verify one inclusive slot range with the same runner
//! as the CLI, without checkpoints, a findings file, or a metrics server.
//! Cluster parameters are mainnet's (64 ticks per slot, the built-in
//! hashes_per_tick history, mainnet genesis hash).

use std::collections::BTreeMap;

use anyhow::{Result, bail};
use tokio::runtime::{Handle, RuntimeFlavor};

use crate::cli::{
    Args, DEFAULT_FETCH_AHEAD, DEFAULT_WINDOW_SLOTS, MAINNET_GENESIS_HASH, ModeArg, parse_hash32,
    validate_args,
};
use crate::eras::HashesPerTickSchedule;
use crate::range::RangeSpec;
use crate::{metrics, runner};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotRangeMode {
    /// Tick, entry and transaction-index structure plus blockhash linkage.
    Structural,
    /// Structural checks plus recomputing every entry hash.
    Full,
}

impl SlotRangeMode {
    fn to_mode_arg(self) -> ModeArg {
        match self {
            SlotRangeMode::Structural => ModeArg::Structural,
            SlotRangeMode::Full => ModeArg::Full,
        }
    }
}

/// One range to verify and where to read it from.
#[derive(Debug, Clone)]
pub struct SlotRangeCheck {
    pub start_slot: u64,
    /// Inclusive.
    pub end_slot: u64,
    pub mode: SlotRangeMode,
    pub clickhouse_url: String,
    pub clickhouse_database: String,
    pub clickhouse_user: String,
    pub clickhouse_password: String,
    pub blocks_table: String,
    pub entries_table: String,
    pub transactions_table: String,
    /// Trusted Alpenglow genesis certificate slot; later slots use Alpenglow rules.
    pub alpenglow_genesis_slot: Option<u64>,
    /// Hash recomputation threads (0 = all available cores).
    pub verify_threads: usize,
}

/// Outcome of one [`verify_slot_range`] call. Findings are also logged as they
/// are found, one line per finding.
#[derive(Debug, Clone, Default)]
pub struct SlotRangeVerification {
    pub slots_ok: u64,
    pub slots_failed: u64,
    pub slots_unverifiable: u64,
    pub slots_skipped: u64,
    pub slots_missing: u64,
    /// Fail-class findings not attached to a counted slot.
    pub orphan_failures: u64,
    pub entries_verified: u64,
    pub hashes_computed: u64,
    /// Finding code -> count.
    pub findings: BTreeMap<String, u64>,
    pub failed_slots: Vec<u64>,
    pub unverifiable_slots: Vec<u64>,
    /// Inclusive spans neither present nor claimed skipped by the chain.
    pub missing_spans: Vec<(u64, u64)>,
}

impl SlotRangeVerification {
    /// A PoH, tick or chain-linkage check failed (CLI exit code 2).
    pub fn has_failures(&self) -> bool {
        self.slots_failed > 0 || self.orphan_failures > 0
    }

    /// Some slots could not be checked: no entry rows, or a gap in the chain
    /// (CLI exit code 3).
    pub fn has_unverifiable(&self) -> bool {
        self.slots_unverifiable > 0 || self.slots_missing > 0
    }
}

/// Verify `[start_slot, end_slot]`. The chain is seeded from the parent of the
/// first block and the range tail is classified using the first block after
/// it, as in a CLI `--range` run. Must run on a multi-thread Tokio runtime.
pub async fn verify_slot_range(check: &SlotRangeCheck) -> Result<SlotRangeVerification> {
    if Handle::current().runtime_flavor() != RuntimeFlavor::MultiThread {
        bail!("verify_slot_range needs a multi-thread Tokio runtime");
    }
    let args = slot_range_args(check)?;
    metrics::force_init(args.mode.as_str(), None);
    let counters = runner::run(&args).await?;
    let problems = counters.problem_slots.unwrap_or_default();
    Ok(SlotRangeVerification {
        slots_ok: counters.slots_ok,
        slots_failed: counters.slots_failed,
        slots_unverifiable: counters.slots_unverifiable,
        slots_skipped: counters.slots_skipped,
        slots_missing: counters.slots_missing,
        orphan_failures: counters.orphan_failures,
        entries_verified: counters.entries_verified,
        hashes_computed: counters.hashes_computed,
        findings: counters.findings,
        failed_slots: problems.failed,
        unverifiable_slots: problems.unverifiable,
        missing_spans: problems.missing,
    })
}

fn slot_range_args(check: &SlotRangeCheck) -> Result<Args> {
    if check.end_slot < check.start_slot {
        bail!(
            "invalid slot range {}:{}: end before start",
            check.start_slot,
            check.end_slot
        );
    }
    let args = Args {
        range: Some(RangeSpec::Slots {
            start: check.start_slot,
            end: check.end_slot,
        }),
        full: false,
        mode: check.mode.to_mode_arg(),
        slots_per_epoch: 432_000,
        epoch_warmup: true,
        ticks_per_slot: 64,
        expected_genesis_hash: Some(parse_hash32(MAINNET_GENESIS_HASH)?),
        hashes_per_tick_schedule: HashesPerTickSchedule::mainnet(),
        alpenglow_rpc_url: None,
        // Only the slot drives verification; the block id is recorded in
        // checkpoints, which this entry point never writes.
        alpenglow_genesis_block: check.alpenglow_genesis_slot.map(|slot| (slot, [0; 32])),
        window_slots: DEFAULT_WINDOW_SLOTS,
        fetch_ahead: DEFAULT_FETCH_AHEAD,
        audit_duplicate_conflicts: false,
        verify_threads: check.verify_threads,
        checkpoint_file: None,
        resume: false,
        report_file: None,
        max_failures: 0,
        allow_unverifiable: false,
        anchors: Vec::new(),
        clickhouse_url: check.clickhouse_url.clone(),
        clickhouse_database: check.clickhouse_database.clone(),
        clickhouse_user: check.clickhouse_user.clone(),
        clickhouse_password: check.clickhouse_password.clone(),
        blocks_table: check.blocks_table.clone(),
        entries_table: check.entries_table.clone(),
        transactions_table: check.transactions_table.clone(),
        metrics_host: String::new(),
        metrics_port: 0,
        health_stale_secs: 0,
        metrics_cluster_label: None,
        // One progress line per archive window is plenty.
        progress_every_slots: u64::MAX,
        export_fixture: None,
        collect_problem_slots: true,
    };
    validate_args(&args)?;
    Ok(args)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(start_slot: u64, end_slot: u64) -> SlotRangeCheck {
        SlotRangeCheck {
            start_slot,
            end_slot,
            mode: SlotRangeMode::Full,
            clickhouse_url: "http://localhost:8123".to_string(),
            clickhouse_database: "default".to_string(),
            clickhouse_user: "default".to_string(),
            clickhouse_password: String::new(),
            blocks_table: "blocks_metadata".to_string(),
            entries_table: "entries".to_string(),
            transactions_table: "transactions".to_string(),
            alpenglow_genesis_slot: Some(100),
            verify_threads: 2,
        }
    }

    #[test]
    fn slot_range_args_are_a_valid_inclusive_range_job() {
        let args = slot_range_args(&check(10, 20)).unwrap();
        assert!(matches!(
            args.range,
            Some(RangeSpec::Slots { start: 10, end: 20 })
        ));
        assert_eq!(args.mode, ModeArg::Full);
        assert_eq!(
            args.alpenglow_genesis_block.map(|(slot, _)| slot),
            Some(100)
        );
        assert!(args.collect_problem_slots);
        assert!(args.checkpoint_file.is_none() && args.report_file.is_none());
    }

    #[test]
    fn single_slot_is_valid_and_reversed_range_is_rejected() {
        assert!(slot_range_args(&check(10, 10)).is_ok());
        assert!(slot_range_args(&check(11, 10)).is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn current_thread_runtime_is_rejected_before_touching_clickhouse() {
        let err = verify_slot_range(&check(10, 20)).await.unwrap_err();
        assert!(err.to_string().contains("multi-thread"));
    }

    #[test]
    fn unverifiable_and_failure_verdicts_are_distinct() {
        let mut verification = SlotRangeVerification {
            slots_missing: 1,
            ..SlotRangeVerification::default()
        };
        assert!(verification.has_unverifiable() && !verification.has_failures());
        verification.orphan_failures = 1;
        assert!(verification.has_failures());
    }
}
