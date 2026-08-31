// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Cross-block chain-linkage walk. Blocks are observed in ascending slot
//! order; the walk carries the previous present block and classifies every
//! slot between consecutive present blocks as skipped (claimed by the chain
//! via `parent_slot`) or missing (absent from our data with no skip claim).

use std::collections::{BTreeSet, HashMap};

use crate::report::{Finding, FindingCode};
use crate::verify::poh::Hash32;
use crate::verify::{BlockInfo, b58};

#[derive(Debug, Default)]
pub(crate) struct ObserveResult {
    pub(crate) findings: Vec<Finding>,
    /// Inclusive span of slots the chain skipped (no block exists on chain).
    pub(crate) skipped_span: Option<(u64, u64)>,
    /// Inclusive span of slots absent from our data with unknown chain state.
    pub(crate) missing_span: Option<(u64, u64)>,
}

pub(crate) struct ChainWalk {
    /// Last present block observed: (slot, blockhash).
    prev: Option<(u64, Hash32)>,
    /// Slots strictly below this are already accounted (range start, or the
    /// checkpoint cursor after a resume).
    floor: u64,
    expected_genesis_hash: Option<Hash32>,
    anchors: HashMap<u64, Hash32>,
    checked_anchors: BTreeSet<u64>,
    genesis_checked: bool,
}

impl ChainWalk {
    #[cfg(test)]
    pub(crate) fn new(
        floor: u64,
        expected_genesis_hash: Option<Hash32>,
        anchors: HashMap<u64, Hash32>,
    ) -> Self {
        Self::from_checkpoint(
            floor,
            expected_genesis_hash,
            anchors,
            BTreeSet::new(),
            false,
        )
    }

    /// Restore externally pinned checks completed before a resume cursor.
    pub(crate) fn from_checkpoint(
        floor: u64,
        expected_genesis_hash: Option<Hash32>,
        anchors: HashMap<u64, Hash32>,
        checked_anchors: BTreeSet<u64>,
        genesis_checked: bool,
    ) -> Self {
        Self {
            prev: None,
            floor,
            expected_genesis_hash,
            anchors,
            checked_anchors,
            genesis_checked,
        }
    }

    /// Slot of the last present block observed (everything at or below it,
    /// down to the floor, is fully accounted).
    pub(crate) fn last_present(&self) -> Option<u64> {
        self.prev.map(|(slot, _)| slot)
    }

    /// Anchors that were requested but never compared against a block.
    pub(crate) fn unchecked_anchors(&self) -> Vec<(u64, Hash32)> {
        let mut unchecked: Vec<(u64, Hash32)> = self
            .anchors
            .iter()
            .filter(|(slot, _)| !self.checked_anchors.contains(slot))
            .map(|(slot, hash)| (*slot, *hash))
            .collect();
        unchecked.sort_unstable_by_key(|(slot, _)| *slot);
        unchecked
    }

    pub(crate) fn checked_anchors(&self) -> BTreeSet<u64> {
        self.checked_anchors.clone()
    }

    pub(crate) fn genesis_checked(&self) -> bool {
        self.genesis_checked
    }

    /// Seed the carry with the parent block of the first in-range block
    /// (fetched separately when the range does not start at slot 0).
    pub(crate) fn seed(&mut self, slot: u64, blockhash: Hash32) {
        if self.prev.is_none() {
            self.prev = Some((slot, blockhash));
        }
    }

    pub(crate) fn observe_block(&mut self, block: &BlockInfo) -> ObserveResult {
        let mut result = ObserveResult::default();

        if block.slot == 0 {
            if block.parent_slot != 0 {
                result.findings.push(
                    Finding::new(
                        0,
                        FindingCode::ParentSlotMismatch,
                        "slot 0 must be its own parent",
                    )
                    .with_expected_actual("0", block.parent_slot.to_string()),
                );
            }
            if let Some(genesis) = self.expected_genesis_hash {
                self.genesis_checked = true;
                if block.parent_blockhash != genesis {
                    result.findings.push(
                        Finding::new(
                            0,
                            FindingCode::GenesisHashMismatch,
                            "slot 0 parent_blockhash does not match the expected genesis hash",
                        )
                        .with_expected_actual(b58(&genesis), b58(&block.parent_blockhash)),
                    );
                }
            }
        } else if block.parent_slot >= block.slot {
            // A block claiming itself or a future slot as parent is
            // structurally impossible; interval classification and linkage
            // are meaningless against such a claim.
            result.findings.push(
                Finding::new(
                    block.slot,
                    FindingCode::ParentSlotMismatch,
                    "parent_slot must be strictly below the block's own slot",
                )
                .with_expected_actual(format!("< {}", block.slot), block.parent_slot.to_string()),
            );
        } else {
            self.classify_interval(block, &mut result);
            self.check_linkage(block, &mut result);
        }

        if let Some(anchor) = self.anchors.get(&block.slot) {
            self.checked_anchors.insert(block.slot);
            if *anchor != block.blockhash {
                result.findings.push(
                    Finding::new(
                        block.slot,
                        FindingCode::AnchorMismatch,
                        "recorded blockhash does not match the externally supplied anchor",
                    )
                    .with_expected_actual(b58(anchor), b58(&block.blockhash)),
                );
            }
        }

        self.prev = Some((block.slot, block.blockhash));
        result
    }

    /// Account for the tail of the range after the last present block.
    /// `next_block` is the first present block beyond the range end, if any.
    pub(crate) fn observe_tail(
        &self,
        range_end: u64,
        next_block: Option<&BlockInfo>,
    ) -> ObserveResult {
        let mut result = ObserveResult::default();
        let lower = match self.prev {
            Some((slot, _)) => slot.saturating_add(1).max(self.floor),
            None => self.floor,
        };
        if lower > range_end {
            return result;
        }

        match next_block {
            Some(next) => {
                let claim = next.parent_slot;
                let missing_hi = claim.min(range_end);
                if missing_hi >= lower {
                    result.missing_span = Some((lower, missing_hi));
                }
                let skipped_lo = claim.saturating_add(1).max(lower);
                if range_end >= skipped_lo {
                    result.skipped_span = Some((skipped_lo, range_end));
                }
            }
            None => {
                result.missing_span = Some((lower, range_end));
            }
        }

        if let Some((lo, hi)) = result.missing_span {
            result.findings.push(missing_span_finding(lo, hi));
        }
        result
    }

    /// Classify the slots strictly between the previous present block (or the
    /// accounting floor) and `block.slot`. The chain claims everything after
    /// `parent_slot` was skipped; anything at or below it that we do not hold
    /// is a data gap.
    fn classify_interval(&self, block: &BlockInfo, result: &mut ObserveResult) {
        let lower = match self.prev {
            Some((slot, _)) => slot.saturating_add(1).max(self.floor),
            None => self.floor,
        };
        if lower >= block.slot {
            return;
        }
        let upper = block.slot - 1;
        let claim = block.parent_slot;

        let missing_hi = claim.min(upper);
        if missing_hi >= lower {
            result.missing_span = Some((lower, missing_hi));
            result
                .findings
                .push(missing_span_finding(lower, missing_hi));
        }

        let skipped_lo = claim.saturating_add(1).max(lower);
        if upper >= skipped_lo {
            result.skipped_span = Some((skipped_lo, upper));
        }
    }

    fn check_linkage(&self, block: &BlockInfo, result: &mut ObserveResult) {
        match self.prev {
            Some((prev_slot, prev_hash)) => {
                if block.parent_slot == prev_slot {
                    if block.parent_blockhash != prev_hash {
                        result.findings.push(
                            Finding::new(
                                block.slot,
                                FindingCode::ChainBreak,
                                format!(
                                    "parent_blockhash does not match blockhash of parent slot {prev_slot}"
                                ),
                            )
                            .with_expected_actual(b58(&prev_hash), b58(&block.parent_blockhash)),
                        );
                    }
                } else if block.parent_slot > prev_slot {
                    result.findings.push(Finding::new(
                        block.slot,
                        FindingCode::MissingParent,
                        format!(
                            "parent slot {} absent from blocks_metadata; linkage unverifiable",
                            block.parent_slot
                        ),
                    ));
                } else {
                    result.findings.push(
                        Finding::new(
                            block.slot,
                            FindingCode::ParentSlotMismatch,
                            format!(
                                "chain claims parent {} but our data holds a later block at slot {prev_slot}",
                                block.parent_slot
                            ),
                        )
                        .with_expected_actual(prev_slot.to_string(), block.parent_slot.to_string()),
                    );
                }
            }
            None => {
                // No carry: the runner seeds the walk with the fetched parent
                // when it exists; reaching here means the parent is absent.
                result.findings.push(Finding::new(
                    block.slot,
                    FindingCode::MissingParent,
                    format!(
                        "parent slot {} absent from blocks_metadata; linkage unverifiable",
                        block.parent_slot
                    ),
                ));
            }
        }
    }
}

fn missing_span_finding(lo: u64, hi: u64) -> Finding {
    Finding::new(
        lo,
        FindingCode::MissingBlock,
        if lo == hi {
            format!("slot {lo} absent from blocks_metadata with no skip claim covering it")
        } else {
            format!(
                "slots {lo}..={hi} absent from blocks_metadata with no skip claim covering them"
            )
        },
    )
}

pub(crate) fn span_len(span: Option<(u64, u64)>) -> u64 {
    span.map(|(lo, hi)| hi.saturating_sub(lo) + 1).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(slot: u64, parent_slot: u64, blockhash: u8, parent_blockhash: u8) -> BlockInfo {
        BlockInfo {
            slot,
            parent_slot,
            blockhash: [blockhash; 32],
            parent_blockhash: [parent_blockhash; 32],
            executed_transaction_count: 0,
            entry_count: 0,
        }
    }

    #[test]
    fn contiguous_chain_links_cleanly() {
        let mut walk = ChainWalk::new(10, None, HashMap::new());
        walk.seed(9, [1; 32]);
        let result = walk.observe_block(&block(10, 9, 2, 1));
        assert!(result.findings.is_empty());
        let result = walk.observe_block(&block(11, 10, 3, 2));
        assert!(result.findings.is_empty());
        assert_eq!(result.skipped_span, None);
        assert_eq!(result.missing_span, None);
    }

    #[test]
    fn skipped_slots_are_claimed_by_parent_link() {
        let mut walk = ChainWalk::new(10, None, HashMap::new());
        walk.seed(9, [1; 32]);
        walk.observe_block(&block(10, 9, 2, 1));
        let result = walk.observe_block(&block(14, 10, 3, 2));
        assert!(result.findings.is_empty(), "{:?}", result.findings);
        assert_eq!(result.skipped_span, Some((11, 13)));
        assert_eq!(result.missing_span, None);
    }

    #[test]
    fn broken_hash_linkage_detected() {
        let mut walk = ChainWalk::new(10, None, HashMap::new());
        walk.seed(9, [1; 32]);
        walk.observe_block(&block(10, 9, 2, 1));
        let result = walk.observe_block(&block(11, 10, 3, 9));
        assert_eq!(result.findings.len(), 1);
        assert_eq!(result.findings[0].code, FindingCode::ChainBreak);
    }

    #[test]
    fn data_gap_counts_missing_and_skipped() {
        let mut walk = ChainWalk::new(10, None, HashMap::new());
        walk.seed(9, [1; 32]);
        walk.observe_block(&block(10, 9, 2, 1));
        // Next present block claims parent 15: slots 11..=15 are a data gap
        // (15 itself should exist on chain), 16..=19 were skipped.
        let result = walk.observe_block(&block(20, 15, 4, 3));
        assert_eq!(result.missing_span, Some((11, 15)));
        assert_eq!(result.skipped_span, Some((16, 19)));
        assert!(
            result
                .findings
                .iter()
                .any(|finding| finding.code == FindingCode::MissingParent)
        );
        assert!(
            result
                .findings
                .iter()
                .any(|finding| finding.code == FindingCode::MissingBlock)
        );
    }

    #[test]
    fn parent_older_than_previous_block_is_a_mismatch() {
        let mut walk = ChainWalk::new(10, None, HashMap::new());
        walk.seed(9, [1; 32]);
        walk.observe_block(&block(10, 9, 2, 1));
        let result = walk.observe_block(&block(12, 8, 3, 2));
        assert!(
            result
                .findings
                .iter()
                .any(|finding| finding.code == FindingCode::ParentSlotMismatch)
        );
        assert_eq!(result.missing_span, None);
    }

    #[test]
    fn unseeded_walk_reports_missing_parent_once() {
        let mut walk = ChainWalk::new(100, None, HashMap::new());
        let result = walk.observe_block(&block(105, 99, 2, 1));
        assert!(
            result
                .findings
                .iter()
                .any(|finding| finding.code == FindingCode::MissingParent)
        );
        // Slots 100..=104: parent claim is 99 (< floor), so all are skipped.
        assert_eq!(result.skipped_span, Some((100, 104)));
        assert_eq!(result.missing_span, None);
    }

    #[test]
    fn self_or_future_parent_is_a_hard_failure() {
        let mut walk = ChainWalk::new(10, None, HashMap::new());
        walk.seed(9, [1; 32]);
        walk.observe_block(&block(10, 9, 2, 1));

        let result = walk.observe_block(&block(12, 12, 3, 2));
        assert!(
            result
                .findings
                .iter()
                .any(|finding| finding.code == FindingCode::ParentSlotMismatch)
        );

        let result = walk.observe_block(&block(14, 9000, 4, 3));
        assert!(
            result
                .findings
                .iter()
                .any(|finding| finding.code == FindingCode::ParentSlotMismatch)
        );
    }

    #[test]
    fn unchecked_anchors_are_surfaced() {
        let mut anchors = HashMap::new();
        anchors.insert(10u64, [2u8; 32]); // will be checked
        anchors.insert(11u64, [9u8; 32]); // slot skipped by the chain
        anchors.insert(9999u64, [9u8; 32]); // outside the walked range
        let mut walk = ChainWalk::new(10, None, anchors);
        walk.seed(9, [1; 32]);
        walk.observe_block(&block(10, 9, 2, 1));
        walk.observe_block(&block(12, 10, 3, 2));

        let unchecked = walk.unchecked_anchors();
        let slots: Vec<u64> = unchecked.iter().map(|(slot, _)| *slot).collect();
        assert_eq!(slots, vec![11, 9999]);
    }

    #[test]
    fn genesis_block_checks_hash_and_self_parent() {
        let genesis = [7u8; 32];
        let mut walk = ChainWalk::new(0, Some(genesis), HashMap::new());
        let result = walk.observe_block(&block(0, 0, 2, 7));
        assert!(result.findings.is_empty(), "{:?}", result.findings);

        let mut walk = ChainWalk::new(0, Some(genesis), HashMap::new());
        let result = walk.observe_block(&block(0, 0, 2, 8));
        assert!(
            result
                .findings
                .iter()
                .any(|finding| finding.code == FindingCode::GenesisHashMismatch)
        );
    }

    #[test]
    fn anchors_checked_on_matching_slot() {
        let mut anchors = HashMap::new();
        anchors.insert(10u64, [2u8; 32]);
        anchors.insert(11u64, [9u8; 32]);
        let mut walk = ChainWalk::new(10, None, anchors);
        walk.seed(9, [1; 32]);
        let result = walk.observe_block(&block(10, 9, 2, 1));
        assert!(result.findings.is_empty());
        let result = walk.observe_block(&block(11, 10, 3, 2));
        assert!(
            result
                .findings
                .iter()
                .any(|finding| finding.code == FindingCode::AnchorMismatch)
        );
    }

    #[test]
    fn resume_restores_checked_anchor_and_genesis_pin_state() {
        let mut anchors = HashMap::new();
        anchors.insert(0, [2; 32]);
        anchors.insert(10, [3; 32]);
        let mut walk =
            ChainWalk::from_checkpoint(11, Some([7; 32]), anchors, BTreeSet::from([0, 10]), true);
        walk.seed(10, [3; 32]);
        let result = walk.observe_block(&block(11, 10, 4, 3));

        assert!(result.findings.is_empty());
        assert!(walk.unchecked_anchors().is_empty());
        assert_eq!(walk.checked_anchors(), BTreeSet::from([0, 10]));
        assert!(walk.genesis_checked());
    }

    #[test]
    fn tail_with_next_block_classifies_claim() {
        let mut walk = ChainWalk::new(10, None, HashMap::new());
        walk.seed(9, [1; 32]);
        walk.observe_block(&block(10, 9, 2, 1));
        // Range ends at 20; the next present block (25) claims parent 15.
        let next = block(25, 15, 5, 4);
        let result = walk.observe_tail(20, Some(&next));
        assert_eq!(result.missing_span, Some((11, 15)));
        assert_eq!(result.skipped_span, Some((16, 20)));

        // Next block links directly to our last present block: all skipped.
        let next = block(25, 10, 5, 2);
        let result = walk.observe_tail(20, Some(&next));
        assert_eq!(result.missing_span, None);
        assert_eq!(result.skipped_span, Some((11, 20)));
    }

    #[test]
    fn tail_without_next_block_is_missing() {
        let mut walk = ChainWalk::new(10, None, HashMap::new());
        walk.seed(9, [1; 32]);
        walk.observe_block(&block(10, 9, 2, 1));
        let result = walk.observe_tail(20, None);
        assert_eq!(result.missing_span, Some((11, 20)));
        assert_eq!(result.skipped_span, None);
    }

    #[test]
    fn empty_range_tail_accounts_from_floor() {
        let walk = ChainWalk::new(10, None, HashMap::new());
        let result = walk.observe_tail(20, None);
        assert_eq!(result.missing_span, Some((10, 20)));
    }

    #[test]
    fn span_len_counts_inclusive() {
        assert_eq!(span_len(None), 0);
        assert_eq!(span_len(Some((5, 5))), 1);
        assert_eq!(span_len(Some((5, 9))), 5);
    }
}
