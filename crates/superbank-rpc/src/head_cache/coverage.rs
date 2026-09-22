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

    pub(crate) fn invalidate_branch(&mut self, slot: u64) {
        self.invalidate(slot);
        let mut invalid = std::collections::BTreeSet::from([slot]);
        for (&slot, node) in self.nodes.range_mut(slot..) {
            if node.link.is_some_and(|link| invalid.contains(&link.parent)) {
                node.invalid = true;
                invalid.insert(slot);
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

    pub(crate) fn snapshot(
        &self,
        start: u64,
        end: Option<u64>,
        commitment: CommitmentLevel,
        now: Instant,
    ) -> Result<(u64, SlotCoverage), &'static str> {
        let unavailable = || {
            end.map(|end| (end, SlotCoverage::default()))
                .ok_or("untrusted_tip")
        };
        if !self.connected {
            return unavailable();
        }
        let Some(tip) = self.tips[rank(commitment)] else {
            return end
                .map(|end| (end, SlotCoverage::default()))
                .ok_or("commitment_unavailable");
        };
        if end.is_none() && now.saturating_duration_since(tip.advanced) > TIP_MAX_AGE {
            return Err("untrusted_tip");
        }
        let chain = self.chain(tip.slot, commitment);
        // A trusted latest bound requires at least one verified parent edge.
        let rooted = chain.first().is_some_and(|link| link.slot == 0);
        if end.is_none() && chain.len() < 2 && !rooted {
            return Err("untrusted_tip");
        }
        let end = end.unwrap_or(tip.slot);
        Ok((end, chain_coverage(&chain, start, end)))
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
}
