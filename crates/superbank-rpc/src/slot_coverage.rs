// SPDX-License-Identifier: AGPL-3.0-only
//! Owned answers and their exact inclusive completeness intervals.

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct SlotCoverage {
    pub(crate) slots: Vec<u64>,
    pub(crate) intervals: Vec<(u64, u64)>,
}

impl SlotCoverage {
    #[cfg(any(feature = "disk-cache", feature = "grpc-head-cache", test))]
    pub(crate) fn new(slots: Vec<u64>, start: u64, end: u64) -> Self {
        Self {
            slots,
            intervals: vec![(start, end)],
        }
    }

    #[cfg(any(feature = "disk-cache", test))]
    pub(crate) fn merge(&mut self, other: Self) {
        self.slots.extend(other.slots);
        self.slots.sort_unstable();
        self.slots.dedup();
        self.intervals.extend(other.intervals);
        self.intervals.sort_unstable();
    }

    #[cfg(any(feature = "disk-cache", test))]
    pub(crate) fn merge_checked(&mut self, mut other: Self) -> bool {
        let mut conflicts = Vec::new();
        for &(a, b) in &self.intervals {
            for &(c, d) in &other.intervals {
                let (start, end) = (a.max(c), b.min(d));
                if start <= end
                    && !self
                        .slots
                        .iter()
                        .filter(|s| (start..=end).contains(s))
                        .eq(other.slots.iter().filter(|s| (start..=end).contains(s)))
                {
                    conflicts.push((start, end));
                }
            }
        }
        let changed = !conflicts.is_empty();
        conflicts.sort_unstable();
        let exclusions = Self {
            intervals: conflicts,
            slots: Vec::new(),
        };
        self.exclude(&exclusions);
        other.exclude(&exclusions);
        self.merge(other);
        changed
    }

    #[cfg(any(feature = "disk-cache", test))]
    fn exclude(&mut self, exclusions: &Self) {
        self.intervals = self
            .intervals
            .iter()
            .flat_map(|&(a, b)| exclusions.gaps(a, b))
            .collect();
        self.slots
            .retain(|slot| self.intervals.iter().any(|&(a, b)| (a..=b).contains(slot)));
    }

    pub(crate) fn gaps(&self, start: u64, end: u64) -> Vec<(u64, u64)> {
        let mut gaps = Vec::new();
        if start > end {
            return gaps;
        }
        let mut cursor = start;
        for &(floor, tip) in &self.intervals {
            if tip < cursor {
                continue;
            }
            if floor > end {
                break;
            }
            if floor > cursor {
                gaps.push((cursor, floor - 1));
            }
            if tip >= end {
                return gaps;
            }
            cursor = tip + 1;
        }
        gaps.push((cursor, end));
        gaps
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contradictory_overlap_becomes_a_gap() {
        let mut proof = SlotCoverage::new(vec![10, 12], 10, 12);
        assert!(proof.merge_checked(SlotCoverage::new(vec![11, 12, 13], 11, 13)));
        assert_eq!(proof.gaps(10, 13), vec![(11, 12)]);
        assert_eq!(proof.slots, vec![10, 13]);
    }

    #[test]
    fn union_preserves_holes_and_handles_extreme_bounds() {
        let mut coverage = SlotCoverage::new(vec![1, 3], 0, 3);
        coverage.merge(SlotCoverage::new(vec![3, 5], 3, 5));
        coverage.merge(SlotCoverage::new(vec![9], 8, u64::MAX));
        assert_eq!(coverage.gaps(0, u64::MAX), vec![(6, 7)]);
        assert_eq!(coverage.slots, vec![1, 3, 5, 9]);
        assert!(coverage.gaps(u64::MAX, u64::MAX).is_empty());
        assert!(coverage.gaps(5, 4).is_empty());
    }
}
