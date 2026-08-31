// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Pure Proof-of-History math, ported from Agave's `solana-entry`
//! (`entry/src/entry.rs` `next_hash` / `hash_transactions`) and
//! `solana-merkle-tree`. Operates on raw byte arrays so it stays independent
//! of any particular agave crate version; differential tests against
//! `solana-entry` pin the semantics.

use sha2::{Digest, Sha256};

pub(crate) type Hash32 = [u8; 32];
pub(crate) type Signature64 = [u8; 64];

const MERKLE_LEAF_PREFIX: &[u8] = &[0x00];
const MERKLE_INTERMEDIATE_PREFIX: &[u8] = &[0x01];

#[inline]
fn sha256_chain(hash: &Hash32) -> Hash32 {
    let mut hasher = Sha256::new();
    hasher.update(hash);
    hasher.finalize().into()
}

#[inline]
fn sha256_mixin(hash: &Hash32, mixin: &Hash32) -> Hash32 {
    let mut hasher = Sha256::new();
    hasher.update(hash);
    hasher.update(mixin);
    hasher.finalize().into()
}

/// Compute the PoH hash of an entry, given the previous entry's hash (or the
/// parent block's blockhash for the first entry of a block).
///
/// Matches Agave `next_hash`: `num_hashes == 0` with no transactions leaves
/// the hash unchanged; otherwise `num_hashes - 1` chained hashes followed by a
/// final tick hash (no transactions) or a record hash of `cur || mixin`.
pub(crate) fn next_hash(start: &Hash32, num_hashes: u64, mixin: Option<&Hash32>) -> Hash32 {
    if num_hashes == 0 && mixin.is_none() {
        return *start;
    }
    let mut hash = *start;
    for _ in 1..num_hashes {
        hash = sha256_chain(&hash);
    }
    match mixin {
        None => sha256_chain(&hash),
        Some(mixin) => sha256_mixin(&hash, mixin),
    }
}

/// Merkle root over all transaction signatures of an entry, matching Agave
/// `hash_transactions`: leaves are the 64-byte signatures flat-mapped across
/// the entry's transactions in order; leaf nodes are hashed with a `0x00`
/// prefix, intermediate nodes with `0x01`, and an odd node at any level is
/// paired with itself. An empty signature list yields the all-zero hash
/// (`Hash::default()` in Agave).
pub(crate) fn signature_merkle_root(signatures: &[Signature64]) -> Hash32 {
    if signatures.is_empty() {
        return [0u8; 32];
    }

    let mut level: Vec<Hash32> = signatures
        .iter()
        .map(|signature| {
            let mut hasher = Sha256::new();
            hasher.update(MERKLE_LEAF_PREFIX);
            hasher.update(signature);
            hasher.finalize().into()
        })
        .collect();

    while level.len() > 1 {
        let mut next = Vec::with_capacity(level.len().div_ceil(2));
        for pair in level.chunks(2) {
            let left = &pair[0];
            let right = pair.get(1).unwrap_or(left);
            let mut hasher = Sha256::new();
            hasher.update(MERKLE_INTERMEDIATE_PREFIX);
            hasher.update(left);
            hasher.update(right);
            next.push(hasher.finalize().into());
        }
        level = next;
    }

    level[0]
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_entry::entry::{Entry, hash_transactions, next_hash as agave_next_hash};
    use solana_hash::Hash;
    use solana_message::{Message, VersionedMessage};
    use solana_signature::Signature;
    use solana_transaction::Transaction;
    use solana_transaction::versioned::VersionedTransaction;

    fn tx_with_signatures(signatures: Vec<Signature>) -> VersionedTransaction {
        VersionedTransaction {
            signatures,
            message: VersionedMessage::Legacy(Message::default()),
        }
    }

    fn legacy_tx(signatures: Vec<Signature>) -> Transaction {
        Transaction {
            signatures,
            message: Message::default(),
        }
    }

    fn deterministic_signature(seed: u8) -> ([u8; 64], Signature) {
        let mut bytes = [0u8; 64];
        for (i, byte) in bytes.iter_mut().enumerate() {
            *byte = seed.wrapping_add(i as u8).wrapping_mul(31);
        }
        (bytes, Signature::from(bytes))
    }

    #[test]
    fn merkle_root_matches_agave_for_all_small_sizes() {
        for count in 0..=9usize {
            let mut raw = Vec::new();
            let mut sigs = Vec::new();
            for seed in 0..count {
                let (bytes, sig) = deterministic_signature(seed as u8 + 1);
                raw.push(bytes);
                sigs.push(sig);
            }
            let ours = signature_merkle_root(&raw);
            let theirs = hash_transactions(&[tx_with_signatures(sigs)]);
            assert_eq!(
                ours,
                theirs.to_bytes(),
                "merkle root diverges for {count} signatures"
            );
        }
    }

    #[test]
    fn merkle_root_flattens_multi_signature_transactions() {
        let (raw_a, sig_a) = deterministic_signature(10);
        let (raw_b, sig_b) = deterministic_signature(20);
        let (raw_c, sig_c) = deterministic_signature(30);
        let ours = signature_merkle_root(&[raw_a, raw_b, raw_c]);
        let theirs = hash_transactions(&[
            tx_with_signatures(vec![sig_a, sig_b]),
            tx_with_signatures(vec![sig_c]),
        ]);
        assert_eq!(ours, theirs.to_bytes());
    }

    #[test]
    fn next_hash_matches_agave_for_ticks() {
        let start = Hash::new_unique();
        for num_hashes in [0u64, 1, 2, 3, 100] {
            let ours = next_hash(&start.to_bytes(), num_hashes, None);
            let theirs = agave_next_hash(&start, num_hashes, &[]);
            assert_eq!(ours, theirs.to_bytes(), "tick diverges at {num_hashes}");
        }
    }

    #[test]
    fn next_hash_matches_agave_with_transactions() {
        let start = Hash::new_unique();
        let (raw_a, sig_a) = deterministic_signature(42);
        let (raw_b, sig_b) = deterministic_signature(43);
        let txs = vec![tx_with_signatures(vec![sig_a, sig_b])];
        for num_hashes in [0u64, 1, 2, 500] {
            let mixin = signature_merkle_root(&[raw_a, raw_b]);
            let ours = next_hash(&start.to_bytes(), num_hashes, Some(&mixin));
            let theirs = agave_next_hash(&start, num_hashes, &txs);
            assert_eq!(ours, theirs.to_bytes(), "record diverges at {num_hashes}");
        }
    }

    #[test]
    fn entry_chain_matches_agave_entry_verify() {
        // Build a small chain of entries with agave and verify our recomputed
        // hashes agree entry by entry.
        let start = Hash::new_unique();
        let (raw_a, sig_a) = deterministic_signature(7);
        let (_raw_b, sig_b) = deterministic_signature(9);

        let tick = Entry::new(&start, 3, vec![]);
        let record = Entry::new(&tick.hash, 5, vec![legacy_tx(vec![sig_a])]);
        let multi = Entry::new(&record.hash, 4, vec![legacy_tx(vec![sig_b, sig_a])]);

        let computed_tick = next_hash(&start.to_bytes(), 3, None);
        assert_eq!(computed_tick, tick.hash.to_bytes());

        let mixin = signature_merkle_root(&[raw_a]);
        let computed_record = next_hash(&tick.hash.to_bytes(), 5, Some(&mixin));
        assert_eq!(computed_record, record.hash.to_bytes());

        let mixin_multi = signature_merkle_root(&[sig_b.into(), sig_a.into()]);
        let computed_multi = next_hash(&record.hash.to_bytes(), 4, Some(&mixin_multi));
        assert_eq!(computed_multi, multi.hash.to_bytes());
    }

    #[test]
    fn merkle_matches_solana_merkle_tree_directly() {
        let items: Vec<[u8; 64]> = (1..=5u8)
            .map(|seed| deterministic_signature(seed).0)
            .collect();
        let refs: Vec<&[u8]> = items.iter().map(|item| item.as_slice()).collect();
        let tree = solana_merkle_tree::MerkleTree::new(&refs);
        let root = tree.get_root().expect("root");
        assert_eq!(signature_merkle_root(&items), root.to_bytes());
    }

    #[test]
    fn zero_hashes_without_mixin_is_identity() {
        let start = [7u8; 32];
        assert_eq!(next_hash(&start, 0, None), start);
    }
}
