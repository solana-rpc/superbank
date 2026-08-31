// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Structural PoH invariants that need no hash recomputation, mirroring the
//! checks Agave's `blockstore_processor::verify_ticks` performs during replay.

use crate::report::{Finding, FindingCode};
use crate::verify::{BlockInfo, EntryInfo, b58};

/// The number of ticks a block must carry: its own slot's ticks plus one full
/// set per skipped slot since the parent (`tick_height` must advance from
/// `(parent_slot + 1) * ticks_per_slot` to `(slot + 1) * ticks_per_slot`).
/// Slot 0 has no parent and carries exactly one slot of ticks.
pub(crate) fn expected_tick_count(slot: u64, parent_slot: u64, ticks_per_slot: u64) -> u64 {
    if slot == 0 {
        ticks_per_slot
    } else {
        slot.saturating_sub(parent_slot)
            .saturating_mul(ticks_per_slot)
    }
}

pub(crate) fn check_structure(
    block: &BlockInfo,
    entries: &[EntryInfo],
    ticks_per_slot: u64,
    hashes_per_tick: Option<u64>,
) -> Vec<Finding> {
    let mut findings = Vec::new();
    let slot = block.slot;

    // entries table rows must be indexed 0..n-1 with no gaps; rows arrive
    // sorted and deduplicated.
    let mut index_reliable = true;
    for (position, entry) in entries.iter().enumerate() {
        if entry.entry_index != position as u32 {
            findings.push(
                Finding::new(
                    slot,
                    FindingCode::EntryIndexGap,
                    format!(
                        "entry_index {} at position {position}; expected contiguous indices from 0",
                        entry.entry_index
                    ),
                )
                .with_entry_index(entry.entry_index),
            );
            index_reliable = false;
            break;
        }
    }

    if block.entry_count > 0 && entries.len() as u64 != block.entry_count {
        findings.push(
            Finding::new(
                slot,
                FindingCode::EntryCountMismatch,
                "entries row count does not match blocks_metadata.entry_count",
            )
            .with_expected_actual(block.entry_count.to_string(), entries.len().to_string()),
        );
    }

    // Everything below assumes the entry sequence is the block's real,
    // complete entry stream.
    if !index_reliable {
        return findings;
    }

    let tick_count = entries
        .iter()
        .filter(|entry| entry.transaction_count == 0)
        .count() as u64;
    let expected_ticks = expected_tick_count(slot, block.parent_slot, ticks_per_slot);
    if tick_count != expected_ticks {
        findings.push(
            Finding::new(
                slot,
                FindingCode::TickCountMismatch,
                format!(
                    "block covers slots {}..={slot} and must carry {expected_ticks} ticks",
                    block.parent_slot.saturating_add(1)
                ),
            )
            .with_expected_actual(expected_ticks.to_string(), tick_count.to_string()),
        );
    }

    if let Some(last) = entries.last() {
        if last.transaction_count != 0 {
            findings.push(
                Finding::new(
                    slot,
                    FindingCode::TrailingEntry,
                    "last entry of a block must be a tick",
                )
                .with_entry_index(last.entry_index),
            );
        }
        // A block's blockhash is by definition its last entry's hash; this
        // needs no recomputation, so it is enforced in both modes.
        if last.hash != block.blockhash {
            findings.push(
                Finding::new(
                    slot,
                    FindingCode::BlockhashMismatch,
                    "last entry hash does not match recorded blockhash",
                )
                .with_entry_index(last.entry_index)
                .with_expected_actual(b58(&block.blockhash), b58(&last.hash)),
            );
        }
    }

    // Replica of Agave `verify_tick_hash_count`: the running num_hashes sum
    // between consecutive ticks must equal hashes_per_tick exactly, and no
    // tick may claim zero hashes. Skipped when hashes_per_tick is unknown for
    // the era or hashing is disabled (value 0).
    if let Some(hashes_per_tick) = hashes_per_tick.filter(|value| *value > 0) {
        let mut tick_hash_count = 0u64;
        for entry in entries {
            tick_hash_count = tick_hash_count.saturating_add(entry.num_hashes);
            if entry.transaction_count == 0 {
                if entry.num_hashes == 0 || tick_hash_count != hashes_per_tick {
                    findings.push(
                        Finding::new(
                            slot,
                            FindingCode::TickHashCountMismatch,
                            "hashes accumulated since the previous tick must equal hashes_per_tick",
                        )
                        .with_entry_index(entry.entry_index)
                        .with_expected_actual(
                            hashes_per_tick.to_string(),
                            tick_hash_count.to_string(),
                        ),
                    );
                    break;
                }
                tick_hash_count = 0;
            }
        }
    }

    // Entry transaction ranges must exactly tile [0, executed_transaction_count).
    let mut expected_next = 0u32;
    let mut tiling_ok = true;
    for entry in entries {
        if entry.starting_transaction_index != expected_next {
            findings.push(
                Finding::new(
                    slot,
                    FindingCode::TxIndexMismatch,
                    "entry starting_transaction_index does not continue the previous entry's range",
                )
                .with_entry_index(entry.entry_index)
                .with_expected_actual(
                    expected_next.to_string(),
                    entry.starting_transaction_index.to_string(),
                ),
            );
            tiling_ok = false;
            break;
        }
        expected_next = expected_next.saturating_add(entry.transaction_count);
    }
    if tiling_ok && u64::from(expected_next) != block.executed_transaction_count {
        findings.push(
            Finding::new(
                slot,
                FindingCode::TxIndexMismatch,
                "entry transaction ranges do not sum to executed_transaction_count",
            )
            .with_expected_actual(
                block.executed_transaction_count.to_string(),
                expected_next.to_string(),
            ),
        );
    }

    findings
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::verify::test_support::build_block;

    fn layout() -> Vec<(u64, usize)> {
        // 4 ticks at hashes_per_tick = 25, with tx entries stealing hashes
        // from their enclosing tick window.
        vec![(25, 0), (10, 2), (15, 0), (25, 0), (5, 1), (20, 0)]
    }

    #[test]
    fn clean_block_has_no_findings() {
        let (block, entries, _) = build_block(9, 8, [1u8; 32], &layout());
        assert!(check_structure(&block, &entries, 4, Some(25)).is_empty());
    }

    #[test]
    fn skipped_slots_require_extra_ticks() {
        // Parent is 5, slot is 9: 4 slots of ticks expected at 4 ticks/slot.
        let (block, entries, _) = build_block(9, 5, [1u8; 32], &layout());
        let findings = check_structure(&block, &entries, 4, Some(25));
        assert!(
            findings
                .iter()
                .any(|finding| finding.code == FindingCode::TickCountMismatch)
        );

        // A block genuinely carrying 16 ticks passes.
        let mut big_layout = Vec::new();
        for _ in 0..16 {
            big_layout.push((25u64, 0usize));
        }
        let (block, entries, _) = build_block(9, 5, [1u8; 32], &big_layout);
        assert!(check_structure(&block, &entries, 4, Some(25)).is_empty());
    }

    #[test]
    fn slot_zero_expects_one_slot_of_ticks() {
        assert_eq!(expected_tick_count(0, 0, 64), 64);
        assert_eq!(expected_tick_count(1, 0, 64), 64);
        assert_eq!(expected_tick_count(5, 2, 64), 192);
    }

    #[test]
    fn entry_index_gap_short_circuits_dependent_checks() {
        let (block, mut entries, _) = build_block(9, 8, [1u8; 32], &layout());
        entries.remove(2);
        let findings = check_structure(&block, &entries, 4, Some(25));
        assert!(
            findings
                .iter()
                .any(|finding| finding.code == FindingCode::EntryIndexGap)
        );
        // Tick-count and tiling checks are suppressed on unreliable indices.
        assert!(
            !findings
                .iter()
                .any(|finding| finding.code == FindingCode::TickCountMismatch)
        );
    }

    #[test]
    fn entry_count_mismatch_detected() {
        let (mut block, entries, _) = build_block(9, 8, [1u8; 32], &layout());
        block.entry_count += 1;
        let findings = check_structure(&block, &entries, 4, Some(25));
        assert!(
            findings
                .iter()
                .any(|finding| finding.code == FindingCode::EntryCountMismatch)
        );
    }

    #[test]
    fn unknown_entry_count_skips_count_check() {
        let (mut block, entries, _) = build_block(9, 8, [1u8; 32], &layout());
        block.entry_count = 0;
        let findings = check_structure(&block, &entries, 4, Some(25));
        assert!(
            !findings
                .iter()
                .any(|finding| finding.code == FindingCode::EntryCountMismatch)
        );
    }

    #[test]
    fn blockhash_mismatch_detected_without_hashing() {
        let (mut block, entries, _) = build_block(9, 8, [1u8; 32], &layout());
        block.blockhash[0] ^= 0xff;
        let findings = check_structure(&block, &entries, 4, Some(25));
        assert!(
            findings
                .iter()
                .any(|finding| finding.code == FindingCode::BlockhashMismatch)
        );
    }

    #[test]
    fn trailing_transaction_entry_detected() {
        let mut with_trailing_tx = layout();
        with_trailing_tx.push((7, 1));
        let (block, entries, _) = build_block(9, 8, [1u8; 32], &with_trailing_tx);
        let findings = check_structure(&block, &entries, 4, Some(25));
        assert!(
            findings
                .iter()
                .any(|finding| finding.code == FindingCode::TrailingEntry)
        );
    }

    #[test]
    fn tick_hash_count_replica_detects_window_drift() {
        let (block, mut entries, _) = build_block(9, 8, [1u8; 32], &layout());
        entries[1].num_hashes += 1;
        let findings = check_structure(&block, &entries, 4, Some(25));
        let hits: Vec<_> = findings
            .iter()
            .filter(|finding| finding.code == FindingCode::TickHashCountMismatch)
            .collect();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].entry_index, Some(2));
    }

    #[test]
    fn zero_hash_tick_rejected() {
        let (block, mut entries, _) = build_block(9, 8, [1u8; 32], &layout());
        entries[0].num_hashes = 0;
        let findings = check_structure(&block, &entries, 4, Some(25));
        assert!(
            findings
                .iter()
                .any(|finding| finding.code == FindingCode::TickHashCountMismatch)
        );
    }

    #[test]
    fn tick_hash_count_skipped_when_era_unknown_or_disabled() {
        let (block, mut entries, _) = build_block(9, 8, [1u8; 32], &layout());
        entries[1].num_hashes += 1;
        assert!(
            !check_structure(&block, &entries, 4, None)
                .iter()
                .any(|finding| finding.code == FindingCode::TickHashCountMismatch)
        );
        assert!(
            !check_structure(&block, &entries, 4, Some(0))
                .iter()
                .any(|finding| finding.code == FindingCode::TickHashCountMismatch)
        );
    }

    #[test]
    fn tx_tiling_gap_detected() {
        let (block, mut entries, _) = build_block(9, 8, [1u8; 32], &layout());
        entries[4].starting_transaction_index += 1;
        let findings = check_structure(&block, &entries, 4, Some(25));
        assert!(
            findings
                .iter()
                .any(|finding| finding.code == FindingCode::TxIndexMismatch)
        );
    }

    #[test]
    fn tx_total_mismatch_detected() {
        let (mut block, entries, _) = build_block(9, 8, [1u8; 32], &layout());
        block.executed_transaction_count += 1;
        let findings = check_structure(&block, &entries, 4, Some(25));
        assert!(
            findings
                .iter()
                .any(|finding| finding.code == FindingCode::TxIndexMismatch)
        );
    }
}
