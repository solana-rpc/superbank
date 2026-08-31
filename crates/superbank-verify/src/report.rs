// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Severity class of a finding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FindingClass {
    /// Verification failure: the data contradicts Proof-of-History.
    Fail,
    /// The data needed to verify is absent; nothing can be proven either way.
    Unverifiable,
    /// Suspicious but non-fatal observation.
    Warn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FindingCode {
    EntryHashMismatch,
    BlockhashMismatch,
    GenesisHashMismatch,
    ChainBreak,
    ParentSlotMismatch,
    EntryCountMismatch,
    EntryIndexGap,
    TickCountMismatch,
    TrailingEntry,
    TickHashCountMismatch,
    TxIndexMismatch,
    DuplicateConflict,
    AnchorMismatch,
    NumHashesOutOfRange,
    MissingEntries,
    MissingTransactions,
    MissingParent,
    MissingBlock,
    AnchorNotChecked,
    ZeroNumHashesEntry,
}

impl FindingCode {
    pub(crate) fn class(self) -> FindingClass {
        match self {
            FindingCode::EntryHashMismatch
            | FindingCode::BlockhashMismatch
            | FindingCode::GenesisHashMismatch
            | FindingCode::ChainBreak
            | FindingCode::ParentSlotMismatch
            | FindingCode::EntryCountMismatch
            | FindingCode::EntryIndexGap
            | FindingCode::TickCountMismatch
            | FindingCode::TrailingEntry
            | FindingCode::TickHashCountMismatch
            | FindingCode::TxIndexMismatch
            | FindingCode::DuplicateConflict
            | FindingCode::AnchorMismatch
            | FindingCode::NumHashesOutOfRange => FindingClass::Fail,
            FindingCode::MissingEntries
            | FindingCode::MissingTransactions
            | FindingCode::MissingParent
            | FindingCode::MissingBlock
            | FindingCode::AnchorNotChecked => FindingClass::Unverifiable,
            FindingCode::ZeroNumHashesEntry => FindingClass::Warn,
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            FindingCode::EntryHashMismatch => "entry_hash_mismatch",
            FindingCode::BlockhashMismatch => "blockhash_mismatch",
            FindingCode::GenesisHashMismatch => "genesis_hash_mismatch",
            FindingCode::ChainBreak => "chain_break",
            FindingCode::ParentSlotMismatch => "parent_slot_mismatch",
            FindingCode::EntryCountMismatch => "entry_count_mismatch",
            FindingCode::EntryIndexGap => "entry_index_gap",
            FindingCode::TickCountMismatch => "tick_count_mismatch",
            FindingCode::TrailingEntry => "trailing_entry",
            FindingCode::TickHashCountMismatch => "tick_hash_count_mismatch",
            FindingCode::TxIndexMismatch => "tx_index_mismatch",
            FindingCode::DuplicateConflict => "duplicate_conflict",
            FindingCode::AnchorMismatch => "anchor_mismatch",
            FindingCode::NumHashesOutOfRange => "num_hashes_out_of_range",
            FindingCode::MissingEntries => "missing_entries",
            FindingCode::MissingTransactions => "missing_transactions",
            FindingCode::MissingParent => "missing_parent",
            FindingCode::MissingBlock => "missing_block",
            FindingCode::AnchorNotChecked => "anchor_not_checked",
            FindingCode::ZeroNumHashesEntry => "zero_num_hashes_entry",
        }
    }
}

/// A single verification finding, emitted as one JSONL line in the report file.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct Finding {
    pub(crate) slot: u64,
    pub(crate) code: FindingCode,
    pub(crate) class: FindingClass,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) entry_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) expected: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) actual: Option<String>,
    pub(crate) detail: String,
}

impl Finding {
    pub(crate) fn new(slot: u64, code: FindingCode, detail: impl Into<String>) -> Self {
        Self {
            slot,
            code,
            class: code.class(),
            entry_index: None,
            expected: None,
            actual: None,
            detail: detail.into(),
        }
    }

    pub(crate) fn with_entry_index(mut self, entry_index: u32) -> Self {
        self.entry_index = Some(entry_index);
        self
    }

    pub(crate) fn with_expected_actual(
        mut self,
        expected: impl Into<String>,
        actual: impl Into<String>,
    ) -> Self {
        self.expected = Some(expected.into());
        self.actual = Some(actual.into());
        self
    }
}

/// Final status of a slot inside the requested range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SlotStatus {
    /// Block present and every applicable check passed.
    Ok,
    /// At least one Fail-class finding.
    Failed,
    /// Block present but required data is missing (entries/transactions/parent).
    Unverifiable,
    /// The chain skipped this slot (covered by a later block's parent link).
    Skipped,
    /// Neither present in our data nor claimed skipped by the chain: a data gap.
    Missing,
}

impl SlotStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            SlotStatus::Ok => "ok",
            SlotStatus::Failed => "failed",
            SlotStatus::Unverifiable => "unverifiable",
            SlotStatus::Skipped => "skipped",
            SlotStatus::Missing => "missing",
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(crate) struct RunCounters {
    pub(crate) slots_ok: u64,
    pub(crate) slots_failed: u64,
    pub(crate) slots_unverifiable: u64,
    pub(crate) slots_skipped: u64,
    pub(crate) slots_missing: u64,
    /// Fail-class findings not attributable to a counted slot (e.g. duplicate
    /// conflicts on slots with no blocks_metadata row).
    #[serde(default)]
    pub(crate) orphan_failures: u64,
    /// Anchors that were requested but never compared against a block.
    #[serde(default)]
    pub(crate) anchors_unchecked: u64,
    pub(crate) entries_verified: u64,
    pub(crate) hashes_computed: u64,
    pub(crate) findings: BTreeMap<String, u64>,
}

impl RunCounters {
    pub(crate) fn record_slot(&mut self, status: SlotStatus) {
        match status {
            SlotStatus::Ok => self.slots_ok += 1,
            SlotStatus::Failed => self.slots_failed += 1,
            SlotStatus::Unverifiable => self.slots_unverifiable += 1,
            SlotStatus::Skipped => self.slots_skipped += 1,
            SlotStatus::Missing => self.slots_missing += 1,
        }
    }

    pub(crate) fn record_finding(&mut self, code: FindingCode) {
        *self.findings.entry(code.as_str().to_string()).or_default() += 1;
    }

    pub(crate) fn has_failures(&self) -> bool {
        self.slots_failed > 0 || self.orphan_failures > 0
    }

    pub(crate) fn has_unverifiable(&self) -> bool {
        self.slots_unverifiable > 0 || self.slots_missing > 0 || self.anchors_unchecked > 0
    }

    pub(crate) fn failure_count(&self) -> u64 {
        self.slots_failed + self.orphan_failures
    }
}

/// Exit-code contract, documented in the crate README:
/// 0 = clean, 1 = operational error, 2 = verification failures,
/// 3 = no failures but unverifiable/missing slots (0 with --allow-unverifiable).
pub(crate) const EXIT_OK: i32 = 0;
pub(crate) const EXIT_OPERATIONAL_ERROR: i32 = 1;
pub(crate) const EXIT_VERIFICATION_FAILED: i32 = 2;
pub(crate) const EXIT_UNVERIFIABLE: i32 = 3;

pub(crate) fn exit_code(counters: &RunCounters, allow_unverifiable: bool) -> i32 {
    if counters.has_failures() {
        EXIT_VERIFICATION_FAILED
    } else if counters.has_unverifiable() && !allow_unverifiable {
        EXIT_UNVERIFIABLE
    } else {
        EXIT_OK
    }
}

pub(crate) struct ReportWriter {
    writer: Option<BufWriter<File>>,
}

impl ReportWriter {
    pub(crate) fn new(path: Option<&Path>, append: bool) -> Result<Self> {
        let writer = match path {
            Some(path) => {
                let file = OpenOptions::new()
                    .create(true)
                    .append(append)
                    .write(true)
                    .truncate(!append)
                    .open(path)
                    .with_context(|| format!("open report file {}", path.display()))?;
                Some(BufWriter::new(file))
            }
            None => None,
        };
        Ok(Self { writer })
    }

    pub(crate) fn write_finding(&mut self, finding: &Finding) -> Result<()> {
        if let Some(writer) = self.writer.as_mut() {
            serde_json::to_writer(&mut *writer, finding).context("serialize finding")?;
            writer.write_all(b"\n").context("write finding")?;
        }
        Ok(())
    }

    pub(crate) fn flush(&mut self) -> Result<()> {
        if let Some(writer) = self.writer.as_mut() {
            writer.flush().context("flush report file")?;
        }
        Ok(())
    }
}

pub(crate) fn log_finding(finding: &Finding) {
    match finding.class {
        FindingClass::Fail => tracing::error!(
            slot = finding.slot,
            code = finding.code.as_str(),
            entry_index = finding.entry_index,
            expected = finding.expected.as_deref(),
            actual = finding.actual.as_deref(),
            detail = %finding.detail,
            "verification failure"
        ),
        FindingClass::Unverifiable => tracing::warn!(
            slot = finding.slot,
            code = finding.code.as_str(),
            entry_index = finding.entry_index,
            detail = %finding.detail,
            "unverifiable"
        ),
        FindingClass::Warn => tracing::warn!(
            slot = finding.slot,
            code = finding.code.as_str(),
            entry_index = finding.entry_index,
            detail = %finding.detail,
            "suspicious data"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_codes_follow_contract() {
        let mut counters = RunCounters::default();
        assert_eq!(exit_code(&counters, false), EXIT_OK);

        counters.slots_unverifiable = 1;
        assert_eq!(exit_code(&counters, false), EXIT_UNVERIFIABLE);
        assert_eq!(exit_code(&counters, true), EXIT_OK);

        counters.slots_failed = 1;
        assert_eq!(exit_code(&counters, false), EXIT_VERIFICATION_FAILED);
        assert_eq!(exit_code(&counters, true), EXIT_VERIFICATION_FAILED);
    }

    #[test]
    fn finding_serializes_snake_case() {
        let finding = Finding::new(5, FindingCode::EntryHashMismatch, "boom")
            .with_entry_index(3)
            .with_expected_actual("a", "b");
        let json = serde_json::to_value(&finding).unwrap();
        assert_eq!(json["code"], "entry_hash_mismatch");
        assert_eq!(json["class"], "fail");
        assert_eq!(json["entry_index"], 3);
    }

    #[test]
    fn missing_data_is_unverifiable_class() {
        assert_eq!(
            FindingCode::MissingEntries.class(),
            FindingClass::Unverifiable
        );
        assert_eq!(FindingCode::ChainBreak.class(), FindingClass::Fail);
        assert_eq!(FindingCode::ZeroNumHashesEntry.class(), FindingClass::Warn);
    }
}
