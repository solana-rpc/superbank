// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

pub(crate) mod poh;
pub(crate) mod structural;

use std::collections::BTreeMap;

use crate::report::{Finding, FindingCode};
use poh::{Hash32, Signature64, next_hash, signature_merkle_root};

/// Block metadata needed for verification (from `blocks_metadata`).
#[derive(Debug, Clone)]
pub(crate) struct BlockInfo {
    pub(crate) slot: u64,
    pub(crate) parent_slot: u64,
    pub(crate) blockhash: Hash32,
    pub(crate) parent_blockhash: Hash32,
    pub(crate) executed_transaction_count: u64,
    pub(crate) entry_count: u64,
}

/// One PoH entry (from `entries`), sorted by `entry_index`.
#[derive(Debug, Clone)]
pub(crate) struct EntryInfo {
    pub(crate) entry_index: u32,
    pub(crate) num_hashes: u64,
    pub(crate) hash: Hash32,
    pub(crate) starting_transaction_index: u32,
    pub(crate) transaction_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VerifyMode {
    Structural,
    Full,
}

/// Hard ceiling on per-entry `num_hashes` before recomputation. The largest
/// legitimate value is the era's hashes_per_tick (62,500 on current mainnet);
/// anything near this bound is corrupt data, and hashing it would stall a
/// verify worker for an arbitrarily long time instead of producing a finding.
pub(crate) const MAX_ENTRY_NUM_HASHES: u64 = 10_000_000;

#[derive(Debug, Default)]
pub(crate) struct VerifyOutcome {
    pub(crate) findings: Vec<Finding>,
    pub(crate) entries_verified: u64,
    pub(crate) hashes_computed: u64,
}

pub(crate) fn b58(bytes: &[u8]) -> String {
    bs58::encode(bytes).into_string()
}

/// Verify a single block. `tx_signatures` maps `slot_idx` (transaction index
/// within the block) to that transaction's complete signature list; it is only
/// consulted in full mode. Chain linkage against the parent block is checked
/// separately by the chain walk, which owns cross-block state.
pub(crate) fn verify_block(
    mode: VerifyMode,
    block: &BlockInfo,
    entries: &[EntryInfo],
    tx_signatures: &BTreeMap<u32, Vec<Signature64>>,
    ticks_per_slot: u64,
    hashes_per_tick: Option<u64>,
) -> VerifyOutcome {
    let mut outcome = VerifyOutcome {
        findings: structural::check_structure(block, entries, ticks_per_slot, hashes_per_tick),
        ..VerifyOutcome::default()
    };

    let index_reliable = !outcome
        .findings
        .iter()
        .any(|finding| finding.code == FindingCode::EntryIndexGap);

    if mode == VerifyMode::Full && index_reliable {
        verify_hash_chain(block, entries, tx_signatures, &mut outcome);
    }

    outcome
}

fn verify_hash_chain(
    block: &BlockInfo,
    entries: &[EntryInfo],
    tx_signatures: &BTreeMap<u32, Vec<Signature64>>,
    outcome: &mut VerifyOutcome,
) {
    let mut prev = block.parent_blockhash;

    for entry in entries {
        if entry.num_hashes == 0 {
            outcome.findings.push(
                Finding::new(
                    block.slot,
                    FindingCode::ZeroNumHashesEntry,
                    "entry with num_hashes = 0; never produced by Agave's recorder",
                )
                .with_entry_index(entry.entry_index),
            );
        }

        if entry.num_hashes > MAX_ENTRY_NUM_HASHES {
            // Corrupt row: recomputing would stall the worker for an
            // arbitrary time. Fail the entry and re-anchor at the recorded
            // hash so later entries stay checkable.
            outcome.findings.push(
                Finding::new(
                    block.slot,
                    FindingCode::NumHashesOutOfRange,
                    format!(
                        "entry claims {} hashes (ceiling {MAX_ENTRY_NUM_HASHES}); refusing to recompute",
                        entry.num_hashes
                    ),
                )
                .with_entry_index(entry.entry_index),
            );
            prev = entry.hash;
            continue;
        }

        match entry_mixin(block.slot, entry, tx_signatures) {
            Err(finding) => {
                // The mixin cannot be computed; re-anchor the chain at the
                // recorded hash so later entries stay checkable.
                outcome.findings.push(finding);
            }
            Ok(mixin) => {
                let computed = next_hash(&prev, entry.num_hashes, mixin.as_ref());
                outcome.entries_verified += 1;
                outcome.hashes_computed += entry.num_hashes;
                if computed != entry.hash {
                    outcome.findings.push(
                        Finding::new(
                            block.slot,
                            FindingCode::EntryHashMismatch,
                            "recomputed PoH hash does not match recorded entry hash",
                        )
                        .with_entry_index(entry.entry_index)
                        .with_expected_actual(b58(&entry.hash), b58(&computed)),
                    );
                }
            }
        }

        prev = entry.hash;
    }
    // The last entry hash == blockhash equality needs no hashing and is
    // checked in structural::check_structure for both modes.
    let _ = prev;
}

fn entry_mixin(
    slot: u64,
    entry: &EntryInfo,
    tx_signatures: &BTreeMap<u32, Vec<Signature64>>,
) -> Result<Option<Hash32>, Finding> {
    if entry.transaction_count == 0 {
        return Ok(None);
    }

    let start = entry.starting_transaction_index;
    let end = start.saturating_add(entry.transaction_count);
    let mut signatures: Vec<Signature64> = Vec::new();
    for slot_idx in start..end {
        match tx_signatures.get(&slot_idx) {
            Some(sigs) => signatures.extend_from_slice(sigs),
            None => {
                return Err(Finding::new(
                    slot,
                    FindingCode::MissingTransactions,
                    format!(
                        "transaction at slot_idx {slot_idx} not found; mixin uncomputable for entry"
                    ),
                )
                .with_entry_index(entry.entry_index));
            }
        }
    }

    Ok(Some(signature_merkle_root(&signatures)))
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use solana_entry::entry::Entry;
    use solana_hash::Hash;
    use solana_message::Message;
    use solana_signature::Signature;
    use solana_transaction::Transaction;

    /// Build a valid block from agave entries: `layout` gives the number of
    /// transactions per entry (0 = tick); returns block info, entry rows, and
    /// the slot_idx -> signatures map.
    pub(crate) fn build_block(
        slot: u64,
        parent_slot: u64,
        start_hash: [u8; 32],
        layout: &[(u64, usize)],
    ) -> (BlockInfo, Vec<EntryInfo>, BTreeMap<u32, Vec<Signature64>>) {
        let mut entries = Vec::new();
        let mut tx_signatures = BTreeMap::new();
        let mut prev = Hash::new_from_array(start_hash);
        let mut next_tx_index = 0u32;
        let mut seed = 1u8;

        for (index, (num_hashes, tx_count)) in layout.iter().enumerate() {
            let mut txs = Vec::new();
            for _ in 0..*tx_count {
                let mut sig_bytes = [0u8; 64];
                for (i, byte) in sig_bytes.iter_mut().enumerate() {
                    *byte = seed.wrapping_mul(37).wrapping_add(i as u8);
                }
                seed = seed.wrapping_add(1);
                let signature = Signature::from(sig_bytes);
                tx_signatures
                    .entry(next_tx_index + txs.len() as u32)
                    .or_insert_with(Vec::new)
                    .push(sig_bytes);
                txs.push(Transaction {
                    signatures: vec![signature],
                    message: Message::default(),
                });
            }

            let entry = Entry::new(&prev, *num_hashes, txs);
            prev = entry.hash;
            entries.push(EntryInfo {
                entry_index: index as u32,
                num_hashes: *num_hashes,
                hash: entry.hash.to_bytes(),
                starting_transaction_index: next_tx_index,
                transaction_count: *tx_count as u32,
            });
            next_tx_index += *tx_count as u32;
        }

        let block = BlockInfo {
            slot,
            parent_slot,
            blockhash: prev.to_bytes(),
            parent_blockhash: start_hash,
            executed_transaction_count: next_tx_index as u64,
            entry_count: entries.len() as u64,
        };

        (block, entries, tx_signatures)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_support::build_block;

    /// A minimal valid single-slot block: 4 ticks of 25 hashes each with two
    /// transaction entries interleaved, at hashes_per_tick = 25.
    fn sample_layout() -> Vec<(u64, usize)> {
        vec![(25, 0), (10, 2), (15, 0), (25, 0), (5, 1), (20, 0)]
    }

    #[test]
    fn valid_block_verifies_clean_in_full_mode() {
        let (block, entries, sigs) = build_block(9, 8, [3u8; 32], &sample_layout());
        let outcome = verify_block(VerifyMode::Full, &block, &entries, &sigs, 4, Some(25));
        assert!(
            outcome.findings.is_empty(),
            "unexpected findings: {:?}",
            outcome.findings
        );
        assert_eq!(outcome.entries_verified, 6);
    }

    #[test]
    fn corrupted_entry_hash_is_localized() {
        let (block, mut entries, sigs) = build_block(9, 8, [3u8; 32], &sample_layout());
        entries[1].hash[0] ^= 0xff;
        let outcome = verify_block(VerifyMode::Full, &block, &entries, &sigs, 4, Some(25));
        // The corrupted recorded hash breaks exactly the two PoH links that
        // reference it (entry 1's own hash and entry 2's starting point);
        // entries further down re-anchor on recorded hashes and stay clean.
        let mismatched: Vec<_> = outcome
            .findings
            .iter()
            .filter(|finding| finding.code == FindingCode::EntryHashMismatch)
            .map(|finding| finding.entry_index)
            .collect();
        assert_eq!(mismatched, vec![Some(1), Some(2)]);
    }

    #[test]
    fn corrupted_blockhash_is_reported() {
        let (mut block, entries, sigs) = build_block(9, 8, [3u8; 32], &sample_layout());
        block.blockhash[0] ^= 0xff;
        let outcome = verify_block(VerifyMode::Full, &block, &entries, &sigs, 4, Some(25));
        assert!(
            outcome
                .findings
                .iter()
                .any(|finding| finding.code == FindingCode::BlockhashMismatch)
        );
    }

    #[test]
    fn missing_transaction_marks_entry_unverifiable_but_chain_continues() {
        let (block, entries, mut sigs) = build_block(9, 8, [3u8; 32], &sample_layout());
        sigs.remove(&0);
        let outcome = verify_block(VerifyMode::Full, &block, &entries, &sigs, 4, Some(25));
        assert!(
            outcome
                .findings
                .iter()
                .any(|finding| finding.code == FindingCode::MissingTransactions)
        );
        // All other entries still verified.
        assert_eq!(outcome.entries_verified, 5);
        assert!(
            !outcome
                .findings
                .iter()
                .any(|finding| finding.code == FindingCode::EntryHashMismatch)
        );
    }

    #[test]
    fn oversized_num_hashes_fails_without_hashing() {
        let (block, mut entries, sigs) = build_block(9, 8, [3u8; 32], &sample_layout());
        entries[2].num_hashes = u64::MAX; // corrupt row; must not be recomputed
        let outcome = verify_block(VerifyMode::Full, &block, &entries, &sigs, 4, None);
        let capped: Vec<_> = outcome
            .findings
            .iter()
            .filter(|finding| finding.code == FindingCode::NumHashesOutOfRange)
            .collect();
        assert_eq!(capped.len(), 1);
        assert_eq!(capped[0].entry_index, Some(2));
        // The chain re-anchors on the recorded hash: every other entry still
        // verifies, and no hash-mismatch cascade follows.
        assert_eq!(outcome.entries_verified, 5);
        assert!(
            !outcome
                .findings
                .iter()
                .any(|finding| finding.code == FindingCode::EntryHashMismatch)
        );
    }

    #[test]
    fn structural_mode_computes_no_hashes() {
        let (block, entries, sigs) = build_block(9, 8, [3u8; 32], &sample_layout());
        let outcome = verify_block(VerifyMode::Structural, &block, &entries, &sigs, 4, Some(25));
        assert!(outcome.findings.is_empty());
        assert_eq!(outcome.hashes_computed, 0);
    }

    #[test]
    fn wrong_mixin_signature_fails_entry_hash() {
        let (block, entries, mut sigs) = build_block(9, 8, [3u8; 32], &sample_layout());
        sigs.get_mut(&0).unwrap()[0][0] ^= 0x01;
        let outcome = verify_block(VerifyMode::Full, &block, &entries, &sigs, 4, Some(25));
        let mismatches: Vec<_> = outcome
            .findings
            .iter()
            .filter(|finding| finding.code == FindingCode::EntryHashMismatch)
            .collect();
        assert_eq!(mismatches.len(), 1);
        assert_eq!(mismatches[0].entry_index, Some(1));
    }
}
