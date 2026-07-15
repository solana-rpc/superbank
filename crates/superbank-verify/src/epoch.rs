// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Epoch <-> slot mapping. Mainnet-beta uses a warmup schedule: epochs 0..=13
//! are 32, 64, ..., 262144 slots long and normal 432000-slot epochs begin at
//! epoch 14 / slot 524256. (`superbank-rpc`'s getEpochSchedule currently
//! ignores warmup; this crate deliberately does not share that code.)

use solana_epoch_schedule::EpochSchedule;

#[derive(Debug, Clone)]
pub(crate) struct EpochSlots {
    schedule: EpochSchedule,
}

impl EpochSlots {
    pub(crate) fn new(slots_per_epoch: u64, warmup: bool) -> Self {
        Self {
            schedule: EpochSchedule::custom(slots_per_epoch, slots_per_epoch, warmup),
        }
    }

    pub(crate) fn first_slot_in_epoch(&self, epoch: u64) -> u64 {
        self.schedule.get_first_slot_in_epoch(epoch)
    }

    pub(crate) fn last_slot_in_epoch(&self, epoch: u64) -> u64 {
        self.schedule.get_last_slot_in_epoch(epoch)
    }

    #[cfg(test)]
    fn epoch_of_slot(&self, slot: u64) -> u64 {
        self.schedule.get_epoch(slot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mainnet_warmup_constants() {
        let epochs = EpochSlots::new(432_000, true);
        assert_eq!(epochs.schedule.first_normal_epoch, 14);
        assert_eq!(epochs.schedule.first_normal_slot, 524_256);
        // Epoch 0 is the 32-slot minimum epoch.
        assert_eq!(epochs.first_slot_in_epoch(0), 0);
        assert_eq!(epochs.last_slot_in_epoch(0), 31);
        assert_eq!(epochs.first_slot_in_epoch(1), 32);
        assert_eq!(epochs.last_slot_in_epoch(1), 95);
        // First normal epoch.
        assert_eq!(epochs.first_slot_in_epoch(14), 524_256);
        assert_eq!(epochs.last_slot_in_epoch(14), 524_256 + 432_000 - 1);
        // A modern epoch.
        assert_eq!(
            epochs.first_slot_in_epoch(700),
            524_256 + (700 - 14) * 432_000
        );
        assert_eq!(epochs.epoch_of_slot(0), 0);
        assert_eq!(epochs.epoch_of_slot(31), 0);
        assert_eq!(epochs.epoch_of_slot(32), 1);
        assert_eq!(epochs.epoch_of_slot(524_255), 13);
        assert_eq!(epochs.epoch_of_slot(524_256), 14);
    }

    #[test]
    fn no_warmup_is_uniform() {
        let epochs = EpochSlots::new(432_000, false);
        assert_eq!(epochs.first_slot_in_epoch(0), 0);
        assert_eq!(epochs.first_slot_in_epoch(2), 864_000);
        assert_eq!(epochs.epoch_of_slot(431_999), 0);
        assert_eq!(epochs.epoch_of_slot(432_000), 1);
    }
}
