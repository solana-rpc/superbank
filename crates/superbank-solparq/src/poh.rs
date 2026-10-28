//! Optional Proof-of-History gate (`--poh-verify-mode`): runs superbank-verify
//! over the archive's slot range in ClickHouse before anything is written.
//! The archive is a `SELECT ... WHERE slot BETWEEN start AND end` over the same
//! tables, so a clean run covers the data that goes into the Parquet files.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde::Serialize;
use superbank_verify::{SlotRangeCheck, SlotRangeMode, SlotRangeVerification, verify_slot_range};

use crate::{
    clickhouse::{SlotRange, slot_ranges},
    config::{Config, PohVerifyMode},
};

/// PoH result for one archive range, carried in the validation report.
#[derive(Debug, Clone, Serialize)]
pub struct PohReport {
    pub mode: &'static str,
    pub slots_ok: u64,
    pub slots_failed: u64,
    pub slots_unverifiable: u64,
    pub slots_skipped: u64,
    pub slots_missing: u64,
    /// Fail-class findings not attached to a counted slot.
    pub orphan_failures: u64,
    pub failed_ranges: Vec<SlotRange>,
    pub unverifiable_ranges: Vec<SlotRange>,
    pub missing_ranges: Vec<SlotRange>,
    /// superbank-verify finding code -> count.
    pub findings: BTreeMap<String, u64>,
    /// `--poh-verify-allow-unverifiable`: only failures block.
    pub unverifiable_allowed: bool,
}

impl PohReport {
    pub fn from_verification(
        mode: &'static str,
        verification: SlotRangeVerification,
        unverifiable_allowed: bool,
    ) -> Self {
        Self {
            mode,
            slots_ok: verification.slots_ok,
            slots_failed: verification.slots_failed,
            slots_unverifiable: verification.slots_unverifiable,
            slots_skipped: verification.slots_skipped,
            slots_missing: verification.slots_missing,
            orphan_failures: verification.orphan_failures,
            failed_ranges: slot_ranges(&verification.failed_slots),
            unverifiable_ranges: slot_ranges(&verification.unverifiable_slots),
            missing_ranges: verification
                .missing_spans
                .iter()
                .map(|&(start, end)| SlotRange::new(start, end))
                .collect(),
            findings: verification.findings,
            unverifiable_allowed,
        }
    }

    pub fn has_failures(&self) -> bool {
        self.slots_failed > 0 || self.orphan_failures > 0
    }

    pub fn has_unverifiable(&self) -> bool {
        self.slots_unverifiable > 0 || self.slots_missing > 0
    }

    pub fn blocks_archive(&self) -> bool {
        self.has_failures() || (self.has_unverifiable() && !self.unverifiable_allowed)
    }
}

/// Run the configured PoH check over `[start_slot, end_slot]`; `None` when off.
pub async fn verify_archive_range(
    config: &Config,
    start_slot: u64,
    end_slot: u64,
) -> Result<Option<PohReport>> {
    let (mode, label) = match config.poh_verify_mode {
        PohVerifyMode::Off => return Ok(None),
        PohVerifyMode::Structural => (SlotRangeMode::Structural, "structural"),
        PohVerifyMode::Full => (SlotRangeMode::Full, "full"),
    };
    let check = SlotRangeCheck {
        start_slot,
        end_slot,
        mode,
        clickhouse_url: config.clickhouse_url(),
        clickhouse_database: config.db_database.clone(),
        clickhouse_user: config.db_user.clone(),
        clickhouse_password: config.db_password.clone(),
        blocks_table: config.blocks_table.clone(),
        entries_table: config.entries_table.clone(),
        transactions_table: config.transactions_table.clone(),
        alpenglow_genesis_slot: config.alpenglow_genesis_slot,
        verify_threads: config.poh_verify_threads,
    };
    let verification = verify_slot_range(&check)
        .await
        .with_context(|| format!("PoH verification of slots {start_slot}-{end_slot}"))?;
    Ok(Some(PohReport::from_verification(
        label,
        verification,
        config.poh_verify_allow_unverifiable,
    )))
}
