// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Pre-archive gap backfill.
//!
//! `superbank-solparq` already detects missing blocks in an archive's slot
//! range during validation (`ValidationReport::missing_blocks`). This module
//! turns that detection into a repair: it writes the missing slots to a temp
//! slot-list file and invokes the `superbank` RPC ingestor
//! (`--source rpc --rpc-slot-list`) as a subprocess to fetch and insert exactly
//! those slots. The archiver re-validates afterward; because the ClickHouse
//! tables are `ReplacingMergeTree(slot)`, re-ingesting is idempotent.

use std::collections::BTreeSet;
use std::process::Stdio;

use anyhow::{Context, Result, anyhow};
use tokio::process::Command;
use tracing::info;

use crate::clickhouse::{DbTables, ValidationReport};
use crate::config::Config;

/// Slots to backfill for a validation report: blocks produced on-chain but
/// absent locally (`missing_blocks`), plus — when `include_undercounts` — slots
/// whose archived transaction count is short of the declared count (re-ingest
/// re-inserts the missing rows and dedups). Sorted and de-duplicated.
pub fn backfill_target_slots(validation: &ValidationReport, include_undercounts: bool) -> Vec<u64> {
    let mut slots: BTreeSet<u64> = validation.missing_blocks.iter().copied().collect();
    if include_undercounts {
        for range in &validation.transaction_undercount_ranges {
            for slot in range.start_slot..=range.end_slot {
                slots.insert(slot);
            }
        }
    }
    slots.into_iter().collect()
}

/// Fetch `slots` via the `superbank` RPC ingestor and insert them into
/// ClickHouse. No-op when `slots` is empty. Returns an error if the subprocess
/// cannot be spawned or exits non-zero; callers should treat that as "gaps not
/// repaired" and fall back to the normal validation gate.
pub async fn run_backfill(config: &Config, tables: &DbTables, slots: &[u64]) -> Result<()> {
    if slots.is_empty() {
        return Ok(());
    }

    let slot_list_path = std::env::temp_dir().join(format!(
        "solparq-backfill-{}-{}-{}.txt",
        std::process::id(),
        slots.first().copied().unwrap_or_default(),
        slots.last().copied().unwrap_or_default(),
    ));
    let mut contents = slots
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    contents.push('\n');
    tokio::fs::write(&slot_list_path, contents)
        .await
        .with_context(|| format!("write backfill slot list {}", slot_list_path.display()))?;

    info!(
        superbank_bin = %config.backfill_superbank_bin,
        slot_count = slots.len(),
        first_slot = slots.first().copied(),
        last_slot = slots.last().copied(),
        slot_list = %slot_list_path.display(),
        "backfilling missing blocks via superbank RPC ingestor"
    );

    // Config flows through the environment so the ClickHouse password never
    // appears in the child's argv. Env var names match superbank's CLI.
    let mut command = Command::new(&config.backfill_superbank_bin);
    command
        .env("SUPERBANK_SOURCE", "rpc")
        .env("RPC_SLOT_LIST", &slot_list_path)
        .env("RPC_URL", &config.solana_rpc_url)
        .env("CLICKHOUSE_URL", config.clickhouse_url())
        .env("CLICKHOUSE_DATABASE", &config.db_database)
        .env("CLICKHOUSE_USER", &config.db_user)
        .env("CLICKHOUSE_PASSWORD", &config.db_password)
        .env("CLICKHOUSE_TRANSACTIONS_TABLE", &tables.transactions_table)
        .env("CLICKHOUSE_BLOCKS_TABLE", &tables.blocks_table)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());

    let status = command.status().await.with_context(|| {
        format!(
            "spawn superbank backfill ({}); is the binary on PATH or --backfill-superbank-bin set?",
            config.backfill_superbank_bin
        )
    });

    // Best-effort cleanup regardless of outcome.
    let _ = tokio::fs::remove_file(&slot_list_path).await;

    let status = status?;
    if !status.success() {
        return Err(anyhow!(
            "superbank backfill exited with {status} (slot list {})",
            slot_list_path.display()
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clickhouse::{SlotRange, TransactionMismatch};

    fn report_with(
        missing_blocks: Vec<u64>,
        undercount_ranges: Vec<SlotRange>,
    ) -> ValidationReport {
        ValidationReport {
            start_slot: 0,
            end_slot: 1_000,
            db_block_slots: 0,
            rpc_produced_slots: 0,
            missing_blocks,
            missing_block_ranges: Vec::new(),
            not_produced_slot_ranges: Vec::new(),
            transaction_mismatches: Vec::<TransactionMismatch>::new(),
            transaction_mismatch_ranges: Vec::new(),
            transaction_undercount_ranges: undercount_ranges,
            transaction_overcount_ranges: Vec::new(),
            rpc_check_error: None,
        }
    }

    #[test]
    fn target_slots_are_missing_blocks_when_undercounts_excluded() {
        let report = report_with(
            vec![10, 12, 11],
            vec![SlotRange {
                start_slot: 100,
                end_slot: 102,
                slot_count: 3,
            }],
        );
        assert_eq!(backfill_target_slots(&report, false), vec![10, 11, 12]);
    }

    #[test]
    fn target_slots_include_and_dedup_undercount_ranges() {
        let report = report_with(
            vec![101],
            vec![SlotRange {
                start_slot: 100,
                end_slot: 102,
                slot_count: 3,
            }],
        );
        // 101 appears in both missing_blocks and the undercount range.
        assert_eq!(backfill_target_slots(&report, true), vec![100, 101, 102]);
    }

    #[test]
    fn target_slots_empty_report_is_empty() {
        let report = report_with(Vec::new(), Vec::new());
        assert!(backfill_target_slots(&report, true).is_empty());
    }
}
