// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! In-memory coverage tracking for the disk cache.
//!
//! `CoverageMap` mirrors the persisted `slot_coverage` CF as a set of inclusive,
//! merged slot ranges. A slot is "covered" when the cache holds complete knowledge
//! of it: either the full block landed atomically or the slot is proven skipped.
//! Address-index scans may only be served from the contiguous range ending at the
//! tip; slot-keyed reads may be served from any covered slot.

use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub(crate) struct CoverageMap {
    /// start -> end (inclusive); ranges are disjoint and non-adjacent.
    ranges: BTreeMap<u64, u64>,
}

impl CoverageMap {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn insert(&mut self, slot: u64) {
        self.insert_range(slot, slot);
    }

    pub(crate) fn insert_range(&mut self, start: u64, end: u64) {
        if end < start {
            return;
        }
        let mut new_start = start;
        let mut new_end = end;

        // Absorb a range that starts before us and overlaps or touches `start`.
        if let Some((&prev_start, &prev_end)) = self.ranges.range(..=start).next_back()
            && prev_end.saturating_add(1) >= start
        {
            new_start = prev_start;
            new_end = new_end.max(prev_end);
            self.ranges.remove(&prev_start);
        }

        // Absorb every range that starts within or adjacent to the new range.
        let absorbed: Vec<u64> = self
            .ranges
            .range(new_start..=new_end.saturating_add(1))
            .map(|(&range_start, _)| range_start)
            .collect();
        for range_start in absorbed {
            if let Some(range_end) = self.ranges.remove(&range_start) {
                new_end = new_end.max(range_end);
            }
        }

        self.ranges.insert(new_start, new_end);
    }

    /// Remove a single slot, splitting its range (poisoned slot).
    pub(crate) fn remove(&mut self, slot: u64) {
        let Some((&start, &end)) = self.ranges.range(..=slot).next_back() else {
            return;
        };
        if slot > end {
            return;
        }
        self.ranges.remove(&start);
        if start < slot {
            self.ranges.insert(start, slot - 1);
        }
        if slot < end {
            self.ranges.insert(slot + 1, end);
        }
    }

    /// Drop all coverage below `floor` (eviction).
    pub(crate) fn remove_below(&mut self, floor: u64) {
        let starts: Vec<u64> = self
            .ranges
            .range(..floor)
            .map(|(&start, _)| start)
            .collect();
        for start in starts {
            if let Some(end) = self.ranges.remove(&start)
                && end >= floor
            {
                self.ranges.insert(floor, end);
            }
        }
    }

    pub(crate) fn contains(&self, slot: u64) -> bool {
        self.ranges
            .range(..=slot)
            .next_back()
            .is_some_and(|(_, &end)| slot <= end)
    }

    /// `(min covered, max covered)` across all ranges.
    pub(crate) fn covered_span(&self) -> Option<(u64, u64)> {
        let (&first_start, _) = self.ranges.first_key_value()?;
        let (_, &last_end) = self.ranges.last_key_value()?;
        Some((first_start, last_end))
    }

    /// The contiguous range ending at the maximum covered slot — the only span
    /// address-index scans may be answered from.
    pub(crate) fn contiguous_tip_span(&self) -> Option<(u64, u64)> {
        self.ranges
            .last_key_value()
            .map(|(&start, &end)| (start, end))
    }

    /// Inclusive sub-ranges of `[start, end]` not covered by the map.
    pub(crate) fn holes_in(&self, start: u64, end: u64) -> Vec<(u64, u64)> {
        if end < start {
            return Vec::new();
        }
        let mut holes = Vec::new();
        let mut cursor = start;

        // A range beginning before `start` may swallow the beginning of the window.
        if let Some((_, &prev_end)) = self.ranges.range(..=start).next_back()
            && prev_end >= cursor
        {
            if prev_end >= end {
                return Vec::new();
            }
            cursor = prev_end + 1;
        }

        for (&range_start, &range_end) in self.ranges.range(cursor..=end) {
            if range_start > cursor {
                holes.push((cursor, range_start - 1));
            }
            if range_end >= end {
                return holes;
            }
            cursor = range_end + 1;
        }

        if cursor <= end {
            holes.push((cursor, end));
        }
        holes
    }

    pub(crate) fn covered_slot_count(&self) -> u64 {
        self.ranges
            .iter()
            .map(|(&start, &end)| end - start + 1)
            .sum()
    }

    #[cfg(test)]
    pub(crate) fn range_count(&self) -> usize {
        self.ranges.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_merges_overlapping_and_adjacent_ranges() {
        let mut map = CoverageMap::new();
        map.insert_range(10, 20);
        map.insert_range(30, 40);
        assert_eq!(map.range_count(), 2);

        // Adjacent on the left edge.
        map.insert_range(21, 29);
        assert_eq!(map.range_count(), 1);
        assert_eq!(map.covered_span(), Some((10, 40)));

        // Overlapping both sides.
        map.insert_range(5, 45);
        assert_eq!(map.covered_span(), Some((5, 45)));
        assert_eq!(map.range_count(), 1);

        // Contained: no change.
        map.insert_range(7, 9);
        assert_eq!(map.range_count(), 1);

        // Single-slot adjacency.
        map.insert(46);
        assert_eq!(map.covered_span(), Some((5, 46)));
        assert_eq!(map.range_count(), 1);
    }

    #[test]
    fn contains_and_span_accessors() {
        let mut map = CoverageMap::new();
        assert_eq!(map.covered_span(), None);
        assert_eq!(map.contiguous_tip_span(), None);
        assert!(!map.contains(5));

        map.insert_range(10, 20);
        map.insert_range(40, 50);
        assert!(map.contains(10) && map.contains(15) && map.contains(20));
        assert!(!map.contains(9) && !map.contains(21) && !map.contains(39));
        assert_eq!(map.covered_span(), Some((10, 50)));
        assert_eq!(map.contiguous_tip_span(), Some((40, 50)));
        assert_eq!(map.covered_slot_count(), 22);
    }

    #[test]
    fn holes_in_reports_uncovered_subranges() {
        let mut map = CoverageMap::new();
        map.insert_range(10, 20);
        map.insert_range(30, 40);

        assert_eq!(map.holes_in(0, 50), vec![(0, 9), (21, 29), (41, 50)]);
        assert_eq!(map.holes_in(10, 40), vec![(21, 29)]);
        assert_eq!(map.holes_in(15, 18), Vec::<(u64, u64)>::new());
        assert_eq!(map.holes_in(21, 29), vec![(21, 29)]);
        assert_eq!(map.holes_in(20, 30), vec![(21, 29)]);
        assert_eq!(map.holes_in(50, 40), Vec::<(u64, u64)>::new());
        assert_eq!(CoverageMap::new().holes_in(1, 3), vec![(1, 3)]);
    }

    #[test]
    fn remove_splits_ranges() {
        let mut map = CoverageMap::new();
        map.insert_range(10, 20);

        map.remove(15);
        assert!(!map.contains(15));
        assert!(map.contains(14) && map.contains(16));
        assert_eq!(map.contiguous_tip_span(), Some((16, 20)));

        // Edges.
        map.remove(10);
        assert!(!map.contains(10));
        map.remove(20);
        assert!(!map.contains(20));
        assert_eq!(map.covered_span(), Some((11, 19)));

        // No-ops.
        map.remove(5);
        map.remove(25);
        assert_eq!(map.covered_span(), Some((11, 19)));

        // Single-slot range removal empties the map.
        let mut single = CoverageMap::new();
        single.insert(7);
        single.remove(7);
        assert_eq!(single.covered_span(), None);
    }

    #[test]
    fn remove_below_truncates_ranges() {
        let mut map = CoverageMap::new();
        map.insert_range(10, 20);
        map.insert_range(30, 40);

        map.remove_below(15);
        assert_eq!(map.covered_span(), Some((15, 40)));
        assert!(!map.contains(14));

        map.remove_below(25);
        assert_eq!(map.covered_span(), Some((30, 40)));

        map.remove_below(41);
        assert_eq!(map.covered_span(), None);
    }
}
