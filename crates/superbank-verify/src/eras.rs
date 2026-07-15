// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Historical `hashes_per_tick` values by slot. The value changed on
//! mainnet-beta through the `update_hashes_per_tick{2..6}` feature gates;
//! features apply on the first block of the activation epoch, so the exact
//! boundary is that feature account's `activated_at` slot (no block exists
//! earlier in that epoch).
//!
//! The built-in table below was verified against the live mainnet feature
//! accounts on 2026-07-15 (activated_at read from
//! `Feature111111111111111111111111111111111111`-owned accounts) with target
//! values from `solana-sdk` v1.18.26 `clock.rs` (`UPDATED_HASHES_PER_TICK2..6`):
//!
//! | feature                 | account                                      | activated_at |
//! |-------------------------|----------------------------------------------|--------------|
//! | update_hashes_per_tick2 | EWme9uFqfy1ikK1jhJs8fM5hxWnK336QJpbscNtizkTU | 253584001    |
//! | update_hashes_per_tick3 | 8C8MCtsab5SsfammbzvYz65HHauuUYdbY2DZ4sznH6h5 | 255312004    |
//! | update_hashes_per_tick4 | 8We4E7DPwF2WfAN8tRTtWQNhi98B99Qpuj7JoZ3Aikgg | 255744008    |
//! | update_hashes_per_tick5 | BsKLKAn1WM4HVhPRDsjosmqSg2J8Tq5xP2s2daDS6Ni4 | 257040000    |
//! | update_hashes_per_tick6 | FKu1qYwLQSiehz644H6Si65U5ZQ2cp9GxsyFUfYcuADv | 257904000    |
//!
//! (`update_hashes_per_tick`, activated at 232848000, set the value to 12500 —
//! a no-op on mainnet — and is omitted.)

use anyhow::{Result, anyhow};

const MAINNET_HASHES_PER_TICK: &[(u64, u64)] = &[
    (0, 12_500),
    (253_584_001, 17_500),
    (255_312_004, 27_500),
    (255_744_008, 47_500),
    (257_040_000, 57_500),
    (257_904_000, 62_500),
];

/// Piecewise-constant `hashes_per_tick` schedule keyed by first effective
/// slot. A value of 0 means PoH hash-count enforcement is disabled for that
/// era (matching Agave's `hashes_per_tick == 0` semantics).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HashesPerTickSchedule {
    eras: Vec<(u64, u64)>,
}

impl HashesPerTickSchedule {
    pub(crate) fn mainnet() -> Self {
        Self {
            eras: MAINNET_HASHES_PER_TICK.to_vec(),
        }
    }

    /// Parse a `from_slot:value,from_slot:value,...` override. The first era
    /// must start at slot 0 and eras must be strictly ascending.
    pub(crate) fn parse(spec: &str) -> Result<Self> {
        let mut eras = Vec::new();
        for part in spec.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            let (from_slot, value) = part.split_once(':').ok_or_else(|| {
                anyhow!("hashes-per-tick era must be <from_slot>:<value>, got {part:?}")
            })?;
            let from_slot: u64 = from_slot
                .trim()
                .parse()
                .map_err(|_| anyhow!("invalid era from_slot in {part:?}"))?;
            let value: u64 = value
                .trim()
                .parse()
                .map_err(|_| anyhow!("invalid era value in {part:?}"))?;
            eras.push((from_slot, value));
        }
        if eras.is_empty() {
            return Err(anyhow!("hashes-per-tick schedule cannot be empty"));
        }
        if eras[0].0 != 0 {
            return Err(anyhow!("hashes-per-tick schedule must start at slot 0"));
        }
        if !eras.is_sorted_by_key(|(from_slot, _)| *from_slot)
            || eras.windows(2).any(|pair| pair[0].0 == pair[1].0)
        {
            return Err(anyhow!(
                "hashes-per-tick schedule slots must be strictly ascending"
            ));
        }
        Ok(Self { eras })
    }

    /// A canonical string form, used to fingerprint checkpoints.
    pub(crate) fn to_spec(&self) -> String {
        self.eras
            .iter()
            .map(|(from_slot, value)| format!("{from_slot}:{value}"))
            .collect::<Vec<_>>()
            .join(",")
    }

    pub(crate) fn value_at(&self, slot: u64) -> u64 {
        match self
            .eras
            .partition_point(|(from_slot, _)| *from_slot <= slot)
        {
            0 => 0,
            index => self.eras[index - 1].1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mainnet_era_boundaries_are_exact() {
        let schedule = HashesPerTickSchedule::mainnet();
        assert_eq!(schedule.value_at(0), 12_500);
        assert_eq!(schedule.value_at(253_584_000), 12_500);
        assert_eq!(schedule.value_at(253_584_001), 17_500);
        assert_eq!(schedule.value_at(255_312_003), 17_500);
        assert_eq!(schedule.value_at(255_312_004), 27_500);
        assert_eq!(schedule.value_at(255_744_008), 47_500);
        assert_eq!(schedule.value_at(257_039_999), 47_500);
        assert_eq!(schedule.value_at(257_040_000), 57_500);
        assert_eq!(schedule.value_at(257_904_000), 62_500);
        assert_eq!(schedule.value_at(u64::MAX), 62_500);
    }

    #[test]
    fn parse_round_trips() {
        let schedule = HashesPerTickSchedule::parse("0:12500,100:0,200:62500").unwrap();
        assert_eq!(schedule.value_at(50), 12_500);
        assert_eq!(schedule.value_at(100), 0);
        assert_eq!(schedule.value_at(400), 62_500);
        assert_eq!(schedule.to_spec(), "0:12500,100:0,200:62500");
        assert_eq!(
            HashesPerTickSchedule::parse(&HashesPerTickSchedule::mainnet().to_spec()).unwrap(),
            HashesPerTickSchedule::mainnet()
        );
    }

    #[test]
    fn parse_rejects_bad_specs() {
        assert!(HashesPerTickSchedule::parse("").is_err());
        assert!(HashesPerTickSchedule::parse("5:100").is_err());
        assert!(HashesPerTickSchedule::parse("0:100,50:1,50:2").is_err());
        assert!(HashesPerTickSchedule::parse("0:100,60:1,50:2").is_err());
        assert!(HashesPerTickSchedule::parse("0:abc").is_err());
    }
}
