// SPDX-License-Identifier: AGPL-3.0-only
//! Range proofs use one subscription generation, never independent DashMap reads.
use super::commitment_meets;
use crate::slot_coverage::SlotCoverage;
use solana_commitment_config::CommitmentLevel;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

pub(crate) const TIP_MAX_AGE: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Link {
    pub(crate) slot: u64,
    pub(crate) hash: [u8; 32],
    pub(crate) parent: u64,
    pub(crate) parent_hash: [u8; 32],
}

/// What the trusted finalized parent-link chain proves about one slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SlotStatus {
    /// A verified link of the finalized chain.
    Present,
    /// Strictly between two consecutive verified links of the finalized chain.
    ProvenSkipped,
    /// Above the trusted finalized tip.
    AboveFinalizedTip,
    /// Not provable: below the chain, no session, a stale tip or no verified parent edge.
    Unknown,
}

#[derive(Default)]
struct Node {
    link: Option<Link>,
    commitment: Option<CommitmentLevel>,
    invalid: bool,
}

#[derive(Clone, Copy)]
struct Tip {
    slot: u64,
    advanced: Instant,
}

#[derive(Default)]
pub(crate) struct HeadCoverage {
    connected: bool,
    nodes: BTreeMap<u64, Node>,
    tips: [Option<Tip>; 3],
    highest: u64,
}

fn rank(commitment: CommitmentLevel) -> usize {
    match commitment {
        CommitmentLevel::Processed => 0,
        CommitmentLevel::Confirmed => 1,
        CommitmentLevel::Finalized => 2,
    }
}

impl HeadCoverage {
    pub(crate) fn connect(&mut self) {
        *self = Self {
            connected: true,
            ..Self::default()
        };
    }
    pub(crate) fn disconnect(&mut self) {
        *self = Self::default();
    }

    #[cfg(test)]
    pub(crate) fn metadata(&mut self, link: Link) {
        if self
            .nodes
            .get(&link.slot)
            .and_then(|node| node.link)
            .is_some_and(|old| old != link)
        {
            self.invalidate_branch(link.slot);
        }
        self.nodes.entry(link.slot).or_default().link = Some(link);
    }

    /// Replace the projected bank only after bank-aware reconstruction/selection.
    pub(super) fn select_bank(&mut self, link: Link) {
        if self
            .nodes
            .get(&link.slot)
            .and_then(|node| node.link)
            .is_some_and(|old| old != link)
        {
            self.invalidate_branch(link.slot);
        }
        self.nodes.insert(
            link.slot,
            Node {
                link: Some(link),
                ..Default::default()
            },
        );
    }

    pub(crate) fn invalidate_branch(&mut self, slot: u64) {
        // Only children that name the invalidated bank's hash descend from it.
        let old_hash = self
            .nodes
            .get(&slot)
            .and_then(|node| node.link)
            .map(|link| link.hash);
        self.invalidate(slot);
        let mut invalid = BTreeMap::from([(slot, old_hash)]);
        for (&slot, node) in self.nodes.range_mut(slot..) {
            let Some(link) = node.link else { continue };
            if invalid
                .get(&link.parent)
                .is_some_and(|hash| hash.is_none_or(|hash| hash == link.parent_hash))
            {
                node.invalid = true;
                invalid.insert(slot, Some(link.hash));
            }
        }
    }

    pub(crate) fn validate_parent(&mut self, slot: u64, parent: Option<u64>) {
        if let Some(link) = self.nodes.get(&slot).and_then(|node| node.link)
            && parent.is_some_and(|parent| parent != link.parent)
        {
            self.invalidate_branch(slot);
        }
    }

    pub(crate) fn observe(&mut self, slot: u64, commitment: CommitmentLevel, now: Instant) {
        for tip in &mut self.tips[..=rank(commitment)] {
            if tip.is_none_or(|old| slot > old.slot) {
                *tip = Some(Tip {
                    slot,
                    advanced: now,
                });
            }
        }
    }

    pub(crate) fn publish(&mut self, slot: u64, commitment: CommitmentLevel) {
        let node = self.nodes.entry(slot).or_default();
        if node
            .commitment
            .is_none_or(|old| commitment_meets(commitment, old))
        {
            node.commitment = Some(commitment);
        }
    }

    pub(crate) fn invalidate(&mut self, slot: u64) {
        // Keep the tombstone until eviction: delayed metadata must not resurrect a fork.
        self.nodes.entry(slot).or_default().invalid = true;
    }

    pub(crate) fn retain(&mut self, slot: u64, retain: u64) {
        self.highest = self.highest.max(slot);
        let floor = self.highest.saturating_sub(retain.saturating_sub(1));
        self.nodes = self.nodes.split_off(&floor);
    }

    fn link(&self, slot: u64, commitment: CommitmentLevel) -> Option<Link> {
        let node = self.nodes.get(&slot)?;
        if node.invalid || !commitment_meets(node.commitment?, commitment) {
            return None;
        }
        node.link
    }

    fn chain(&self, tip: u64, commitment: CommitmentLevel) -> Vec<Link> {
        let mut chain = Vec::new();
        let mut current = self.link(tip, commitment);
        while let Some(link) = current {
            if link.slot == 0 {
                if link.parent != 0 {
                    return Vec::new();
                }
                chain.push(link);
                break;
            }
            if link.parent >= link.slot {
                return Vec::new();
            }
            chain.push(link);
            if self.parent_conflicts(link) {
                return Vec::new();
            }
            current = self.link(link.parent, commitment);
        }
        chain
    }

    fn parent_conflicts(&self, link: Link) -> bool {
        self.nodes.get(&link.parent).is_some_and(|node| {
            node.invalid
                || node
                    .link
                    .is_some_and(|parent| parent.hash != link.parent_hash)
        })
    }

    /// The trusted newest tip at `commitment` and its verified chain: a connected
    /// subscription, a tip advanced within [`TIP_MAX_AGE`], and at least one verified parent
    /// edge (or the genesis root). This is the single definition of a trusted tip.
    fn trusted_chain(
        &self,
        commitment: CommitmentLevel,
        now: Instant,
    ) -> Result<(u64, Vec<Link>), &'static str> {
        if !self.connected {
            return Err("untrusted_tip");
        }
        let Some(tip) = self.tips[rank(commitment)] else {
            return Err("commitment_unavailable");
        };
        if now.saturating_duration_since(tip.advanced) > TIP_MAX_AGE {
            return Err("untrusted_tip");
        }
        let chain = self.chain(tip.slot, commitment);
        // A trusted latest bound requires at least one verified parent edge.
        let rooted = chain.first().is_some_and(|link| link.slot == 0);
        if chain.len() < 2 && !rooted {
            return Err("untrusted_tip");
        }
        Ok((tip.slot, chain))
    }

    /// Classifies `slot` against the trusted finalized chain only. A real block that never
    /// sealed has no link, so it ends the chain and everything below it stays `Unknown`.
    pub(crate) fn slot_status(&self, slot: u64, now: Instant) -> SlotStatus {
        // Every chain link is a retained node, so an older slot is never provable.
        if self
            .nodes
            .first_key_value()
            .is_none_or(|(&oldest, _)| slot < oldest)
        {
            return SlotStatus::Unknown;
        }
        let Ok((tip, chain)) = self.trusted_chain(CommitmentLevel::Finalized, now) else {
            return SlotStatus::Unknown;
        };
        if slot > tip {
            return SlotStatus::AboveFinalizedTip;
        }
        if chain.iter().any(|link| link.slot == slot) {
            return SlotStatus::Present;
        }
        // Newest first: each pair is a child and the parent its verified link names.
        if chain
            .windows(2)
            .any(|pair| pair[1].slot < slot && slot < pair[0].slot)
        {
            return SlotStatus::ProvenSkipped;
        }
        SlotStatus::Unknown
    }

    pub(crate) fn snapshot(
        &self,
        start: u64,
        end: Option<u64>,
        commitment: CommitmentLevel,
        now: Instant,
    ) -> Result<(u64, SlotCoverage), &'static str> {
        let Some(end) = end else {
            let (tip, chain) = self.trusted_chain(commitment, now)?;
            return Ok((tip, chain_coverage(&chain, start, tip)));
        };
        Ok((end, self.historical_coverage(start, end, commitment)))
    }

    /// Coverage for an explicit end: historical proof stays usable while connected, even
    /// when tip discovery is stale.
    fn historical_coverage(
        &self,
        start: u64,
        end: u64,
        commitment: CommitmentLevel,
    ) -> SlotCoverage {
        if !self.connected {
            return SlotCoverage::default();
        }
        let Some(tip) = self.tips[rank(commitment)] else {
            return SlotCoverage::default();
        };
        chain_coverage(&self.chain(tip.slot, commitment), start, end)
    }

    /// Explicit-end snapshot that clamps `end` to the trusted tip, as Agave clamps to its
    /// bank slot. Slots above a trusted tip are not yet at `commitment`, so no source can
    /// list them. Without a trusted tip the explicit end is kept unchanged (never an error).
    /// Returns `(end, coverage, clamped)`; the clamped end may be below `start`.
    pub(crate) fn snapshot_clamped(
        &self,
        start: u64,
        end: u64,
        commitment: CommitmentLevel,
        now: Instant,
    ) -> (u64, SlotCoverage, bool) {
        match self.trusted_chain(commitment, now) {
            Ok((tip, chain)) => {
                let clamped = tip < end;
                let end = end.min(tip);
                (end, chain_coverage(&chain, start, end), clamped)
            }
            Err(_) => (end, self.historical_coverage(start, end, commitment), false),
        }
    }
}

fn chain_coverage(chain: &[Link], start: u64, end: u64) -> SlotCoverage {
    let (Some(first), Some(last)) = (chain.first(), chain.last()) else {
        return SlotCoverage::default();
    };
    let floor = start.max(last.slot);
    let ceiling = end.min(first.slot);
    if floor > ceiling {
        return SlotCoverage::default();
    }
    let slots = chain
        .iter()
        .rev()
        .map(|link| link.slot)
        .filter(|slot| (floor..=ceiling).contains(slot))
        .collect();
    SlotCoverage::new(slots, floor, ceiling)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn link(slot: u64, parent: u64) -> Link {
        Link {
            slot,
            parent,
            hash: [slot as u8; 32],
            parent_hash: [parent as u8; 32],
        }
    }
    fn add(state: &mut HeadCoverage, slot: u64, parent: u64, now: Instant) {
        state.metadata(link(slot, parent));
        state.observe(slot, CommitmentLevel::Finalized, now);
        state.publish(slot, CommitmentLevel::Finalized);
    }
    #[test]
    fn genesis_and_maximum_slot_are_valid_inclusive_bounds() {
        let now = Instant::now();
        let mut state = HeadCoverage::default();
        state.connect();
        add(&mut state, 0, 0, now);
        assert_eq!(
            state
                .snapshot(0, None, CommitmentLevel::Finalized, now)
                .unwrap()
                .1
                .slots,
            vec![0]
        );
        state.connect();
        add(&mut state, u64::MAX - 1, u64::MAX - 2, now);
        add(&mut state, u64::MAX, u64::MAX - 1, now);
        let (_, proof) = state
            .snapshot(u64::MAX, Some(u64::MAX), CommitmentLevel::Finalized, now)
            .unwrap();
        assert_eq!(proof.slots, vec![u64::MAX]);
        assert!(proof.gaps(u64::MAX, u64::MAX).is_empty());
    }

    #[test]
    fn concurrent_publication_never_mixes_chain_generations() {
        use std::sync::{Arc, Barrier, RwLock};
        let state = Arc::new(RwLock::new(HeadCoverage::default()));
        let barrier = Arc::new(Barrier::new(2));
        std::thread::scope(|scope| {
            let writer = state.clone();
            let ready = barrier.clone();
            scope.spawn(move || {
                ready.wait();
                for generation in 0..100 {
                    writer.write().unwrap().connect();
                    let slots = if generation % 2 == 0 {
                        vec![10, 12]
                    } else {
                        vec![10, 11, 12]
                    };
                    let mut parent = 9;
                    for slot in slots {
                        writer.write().unwrap().metadata(link(slot, parent));
                        writer.write().unwrap().observe(
                            slot,
                            CommitmentLevel::Finalized,
                            Instant::now(),
                        );
                        writer
                            .write()
                            .unwrap()
                            .publish(slot, CommitmentLevel::Finalized);
                        parent = slot;
                    }
                }
            });
            barrier.wait();
            for _ in 0..1000 {
                let (_, proof) = state
                    .read()
                    .unwrap()
                    .snapshot(10, Some(12), CommitmentLevel::Finalized, Instant::now())
                    .unwrap();
                if proof.gaps(10, 12).is_empty() {
                    assert!(proof.slots == vec![10, 12] || proof.slots == vec![10, 11, 12]);
                }
            }
        });
    }

    #[test]
    fn skipped_slots_and_missing_middle_are_distinct() {
        let now = Instant::now();
        let mut state = HeadCoverage::default();
        state.connect();
        add(&mut state, 10, 9, now);
        add(&mut state, 13, 10, now);
        let (_, proof) = state
            .snapshot(11, Some(12), CommitmentLevel::Finalized, now)
            .unwrap();
        assert!(proof.slots.is_empty());
        assert!(proof.gaps(11, 12).is_empty());
        add(&mut state, 15, 14, now);
        let (_, proof) = state
            .snapshot(10, Some(15), CommitmentLevel::Finalized, now)
            .unwrap();
        assert_eq!(proof.gaps(10, 15), vec![(10, 14)]);
        assert!(
            state
                .snapshot(10, None, CommitmentLevel::Finalized, now)
                .is_err()
        );
    }
    #[test]
    fn freshness_is_commitment_specific_and_duplicates_do_not_renew_it() {
        let now = Instant::now();
        let mut state = HeadCoverage::default();
        state.connect();
        add(&mut state, 10, 9, now);
        add(&mut state, 11, 10, now);
        let later = now + TIP_MAX_AGE + Duration::from_nanos(1);
        state.observe(11, CommitmentLevel::Finalized, later);
        state.observe(12, CommitmentLevel::Processed, later);
        assert!(
            state
                .snapshot(10, None, CommitmentLevel::Finalized, now + TIP_MAX_AGE)
                .is_ok()
        );
        assert!(
            state
                .snapshot(10, None, CommitmentLevel::Finalized, later)
                .is_err()
        );
        state.disconnect();
        assert!(
            state
                .snapshot(10, None, CommitmentLevel::Finalized, now)
                .is_err()
        );
        state.connect();
        assert!(
            state
                .snapshot(10, None, CommitmentLevel::Finalized, now)
                .is_err()
        );
    }
    #[test]
    fn snapshots_survive_eviction_but_new_reads_cannot_reuse_them() {
        let now = Instant::now();
        let mut state = HeadCoverage::default();
        state.connect();
        add(&mut state, 10, 9, now);
        add(&mut state, 11, 10, now);
        let (_, proof) = state
            .snapshot(10, Some(11), CommitmentLevel::Finalized, now)
            .unwrap();
        state.retain(12, 2);
        assert!(proof.gaps(10, 11).is_empty());
        assert_eq!(
            state
                .snapshot(10, Some(11), CommitmentLevel::Finalized, now)
                .unwrap()
                .1
                .gaps(10, 11),
            vec![(10, 10)]
        );
    }
    #[test]
    fn forks_and_commitment_mismatch_cannot_prove_continuity() {
        let now = Instant::now();
        let mut state = HeadCoverage::default();
        state.connect();
        add(&mut state, 10, 9, now);
        add(&mut state, 11, 10, now);
        state.metadata(Link {
            hash: [99; 32],
            ..link(10, 9)
        });
        assert!(
            state
                .snapshot(10, None, CommitmentLevel::Finalized, now)
                .is_err()
        );
        state.invalidate(11);
        state.metadata(link(11, 10));
        assert!(
            state
                .snapshot(10, Some(11), CommitmentLevel::Finalized, now)
                .unwrap()
                .1
                .intervals
                .is_empty()
        );
        state.connect();
        state.metadata(link(10, 9));
        state.publish(10, CommitmentLevel::Processed);
        state.observe(10, CommitmentLevel::Finalized, now);
        assert!(
            state
                .snapshot(10, None, CommitmentLevel::Finalized, now)
                .is_err()
        );
    }
    #[test]
    fn explicit_end_clamps_only_to_the_trusted_tip() {
        let now = Instant::now();
        let finalized = CommitmentLevel::Finalized;
        let mut state = HeadCoverage::default();
        state.connect();
        add(&mut state, 10, 9, now);
        add(&mut state, 12, 10, now);
        // Trusted: the clamp matches the omitted-end tip and its proof.
        let (tip, trusted) = state.snapshot(5, None, finalized, now).unwrap();
        let (end, proof, clamped) = state.snapshot_clamped(5, 20, finalized, now);
        assert_eq!((end, clamped), (tip, true));
        assert_eq!(proof.slots, trusted.slots);
        assert_eq!(proof.gaps(5, end), vec![(5, 9)]);
        // An end at or below the tip is unchanged.
        let (end, proof, clamped) = state.snapshot_clamped(10, 12, finalized, now);
        assert_eq!((end, clamped), (12, false));
        assert!(proof.gaps(10, 12).is_empty());
        // Start above the tip clamps below start: an empty range, not an error.
        let (end, proof, clamped) = state.snapshot_clamped(15, 20, finalized, now);
        assert_eq!((end, clamped), (12, true));
        assert!(proof.slots.is_empty());
        // Stale tip: keep the raw end and the historical proof.
        let later = now + TIP_MAX_AGE + Duration::from_nanos(1);
        assert!(state.snapshot(5, None, finalized, later).is_err());
        let (end, proof, clamped) = state.snapshot_clamped(10, 20, finalized, later);
        assert_eq!((end, clamped), (20, false));
        assert_eq!(proof.gaps(10, 20), vec![(13, 20)]);
        // No tip yet after a reconnect: raw end.
        state.connect();
        assert_eq!(state.snapshot_clamped(10, 20, finalized, now).0, 20);
        // Unverified single-link chain: raw end.
        add(&mut state, 30, 29, now);
        assert!(state.snapshot(30, None, finalized, now).is_err());
        assert_eq!(state.snapshot_clamped(30, 40, finalized, now).0, 40);
        // Disconnected: raw end, no proof.
        state.disconnect();
        let (end, proof, clamped) = state.snapshot_clamped(10, 20, finalized, now);
        assert_eq!((end, clamped), (20, false));
        assert!(proof.intervals.is_empty());
    }

    #[test]
    fn slot_status_proves_skips_only_between_finalized_links() {
        let now = Instant::now();
        let mut state = HeadCoverage::default();
        state.connect();
        add(&mut state, 10, 9, now);
        add(&mut state, 13, 10, now);
        add(&mut state, 14, 13, now);
        // Confirmed links above the finalized tip prove nothing about their gaps yet.
        for (slot, parent) in [(16, 14), (18, 16)] {
            state.metadata(link(slot, parent));
            state.observe(slot, CommitmentLevel::Confirmed, now);
            state.publish(slot, CommitmentLevel::Confirmed);
        }
        let status = |slot| state.slot_status(slot, now);
        assert_eq!(status(8), SlotStatus::Unknown);
        assert_eq!(status(9), SlotStatus::Unknown, "parent of the oldest link");
        for slot in [10, 13, 14] {
            assert_eq!(status(slot), SlotStatus::Present, "{slot}");
        }
        for slot in [11, 12] {
            assert_eq!(status(slot), SlotStatus::ProvenSkipped, "{slot}");
        }
        for slot in [15, 16, 17, 18, 19, u64::MAX] {
            assert_eq!(status(slot), SlotStatus::AboveFinalizedTip, "{slot}");
        }
    }

    #[test]
    fn slot_status_treats_an_unsealed_block_as_a_gap() {
        let now = Instant::now();
        let mut state = HeadCoverage::default();
        state.connect();
        add(&mut state, 10, 9, now);
        add(&mut state, 12, 10, now);
        // Slot 13 was a real block whose bank never sealed: finalized, but no link.
        state.observe(13, CommitmentLevel::Finalized, now);
        assert_eq!(state.slot_status(11, now), SlotStatus::Unknown);
        assert_eq!(state.slot_status(14, now), SlotStatus::Unknown);
        add(&mut state, 14, 13, now);
        add(&mut state, 16, 14, now);
        let status = |slot| state.slot_status(slot, now);
        for slot in [9, 10, 11, 12, 13] {
            assert_eq!(status(slot), SlotStatus::Unknown, "{slot}");
        }
        assert_eq!(status(14), SlotStatus::Present);
        assert_eq!(status(15), SlotStatus::ProvenSkipped);
        assert_eq!(status(16), SlotStatus::Present);
        assert_eq!(status(17), SlotStatus::AboveFinalizedTip);
    }

    #[test]
    fn slot_status_is_unknown_without_a_trusted_tip() {
        let now = Instant::now();
        let mut state = HeadCoverage::default();
        state.connect();
        add(&mut state, 10, 9, now);
        // A single unverified link is not a trusted chain.
        assert_eq!(state.slot_status(11, now), SlotStatus::Unknown);
        add(&mut state, 12, 10, now);
        assert_eq!(state.slot_status(11, now), SlotStatus::ProvenSkipped);
        let stale = now + TIP_MAX_AGE + Duration::from_nanos(1);
        for slot in [10, 11, 13] {
            assert_eq!(
                state.slot_status(slot, stale),
                SlotStatus::Unknown,
                "{slot}"
            );
        }
        // A replaced bank breaks the verified parent edge.
        state.metadata(Link {
            hash: [99; 32],
            ..link(10, 9)
        });
        assert_eq!(state.slot_status(11, now), SlotStatus::Unknown);
        assert_eq!(state.slot_status(13, now), SlotStatus::Unknown);
        state.disconnect();
        for slot in [10, 11, 13] {
            assert_eq!(state.slot_status(slot, now), SlotStatus::Unknown, "{slot}");
        }
    }

    #[test]
    fn replacing_a_bank_keeps_children_built_on_the_winner() {
        let now = Instant::now();
        let mut state = HeadCoverage::default();
        state.connect();
        add(&mut state, 10, 9, now);
        add(&mut state, 11, 10, now);
        let winner = Link {
            hash: [99; 32],
            ..link(11, 10)
        };
        let child = Link {
            parent_hash: [99; 32],
            ..link(12, 11)
        };
        let orphan_child = link(13, 12);
        state.metadata(child);
        state.metadata(orphan_child);
        state.observe(13, CommitmentLevel::Finalized, now);
        state.publish(12, CommitmentLevel::Finalized);
        state.publish(13, CommitmentLevel::Finalized);
        state.invalidate_branch(11);
        state.select_bank(winner);
        state.publish(11, CommitmentLevel::Finalized);
        let (_, proof) = state
            .snapshot(10, None, CommitmentLevel::Finalized, now)
            .unwrap();
        assert_eq!(proof.slots, vec![10, 11, 12, 13]);
    }
}
