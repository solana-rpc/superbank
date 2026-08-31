// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Local resume state for long verification runs. Windows complete strictly
//! in ascending slot order, so a single high-water mark (`next_start`) plus
//! the accumulated counters captures the full run state. The file is written
//! atomically (temp file + rename) after every window.

use std::collections::BTreeSet;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::report::RunCounters;
use crate::verify::poh::Hash32;

/// Identifies a verification job; a checkpoint only resumes a job with an
/// identical descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct JobDescriptor {
    pub(crate) range_start: u64,
    pub(crate) range_end: u64,
    pub(crate) mode: String,
    pub(crate) blocks_table: String,
    pub(crate) entries_table: String,
    pub(crate) transactions_table: String,
    pub(crate) ticks_per_slot: u64,
    pub(crate) hashes_per_tick_schedule: String,
    /// The genesis pin is part of the job identity even after slot 0 is below
    /// the resume cursor.
    #[serde(default)]
    pub(crate) expected_genesis_hash: Option<Hash32>,
    /// Canonically sorted external pins, also part of the job identity.
    #[serde(default)]
    pub(crate) anchors: Vec<(u64, Hash32)>,
}

impl JobDescriptor {
    fn matches_resume(&self, current: &Self, moving_tip: bool) -> bool {
        if self == current {
            return true;
        }
        if !moving_tip || current.range_end < self.range_end {
            return false;
        }
        let mut saved = self.clone();
        saved.range_end = current.range_end;
        saved == *current
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Checkpoint {
    pub(crate) descriptor: JobDescriptor,
    /// All slots strictly below this are fully accounted in `counters`.
    pub(crate) next_start: u64,
    pub(crate) counters: RunCounters,
    /// Anchors already compared in windows below `next_start`.
    #[serde(default)]
    pub(crate) checked_anchors: BTreeSet<u64>,
    /// Whether the expected genesis pin has been compared at slot 0.
    #[serde(default)]
    pub(crate) genesis_checked: bool,
    pub(crate) updated_unix: u64,
}

pub(crate) fn save(path: &Path, checkpoint: &Checkpoint) -> Result<()> {
    let payload = serde_json::to_vec_pretty(checkpoint).context("serialize checkpoint")?;
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, payload)
        .with_context(|| format!("write checkpoint temp file {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("rename checkpoint into place at {}", path.display()))?;
    Ok(())
}

pub(crate) fn load(path: &Path) -> Result<Option<Checkpoint>> {
    let contents = match std::fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(err).with_context(|| format!("read checkpoint {}", path.display()));
        }
    };
    let checkpoint = serde_json::from_str::<Checkpoint>(&contents)
        .with_context(|| format!("parse checkpoint {}", path.display()))?;
    Ok(Some(checkpoint))
}

/// Load a checkpoint for resuming. `--full` jobs identify the range's lower
/// bound and invariant settings, but deliberately allow a monotonically
/// advancing live tip.
pub(crate) fn load_for_resume(
    path: &Path,
    descriptor: &JobDescriptor,
    moving_tip: bool,
) -> Result<Option<Checkpoint>> {
    let Some(checkpoint) = load(path)? else {
        return Ok(None);
    };
    if !checkpoint.descriptor.matches_resume(descriptor, moving_tip) {
        bail!(
            "checkpoint {} was written by a different job (stored: {:?}, current: {:?}); \
             delete the file or use a different --checkpoint-file to start fresh",
            path.display(),
            checkpoint.descriptor,
            descriptor
        );
    }
    Ok(Some(checkpoint))
}

pub(crate) fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn descriptor() -> JobDescriptor {
        JobDescriptor {
            range_start: 0,
            range_end: 1000,
            mode: "full".to_string(),
            blocks_table: "default.blocks_metadata".to_string(),
            entries_table: "default.entries".to_string(),
            transactions_table: "default.transactions".to_string(),
            ticks_per_slot: 64,
            hashes_per_tick_schedule: "0:12500".to_string(),
            expected_genesis_hash: Some([7; 32]),
            anchors: vec![(7, [9; 32])],
        }
    }

    #[test]
    fn round_trips_through_disk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("checkpoint.json");

        assert!(load(&path).unwrap().is_none());

        let checkpoint = Checkpoint {
            descriptor: descriptor(),
            next_start: 512,
            counters: RunCounters {
                slots_ok: 42,
                ..RunCounters::default()
            },
            checked_anchors: BTreeSet::from([7]),
            genesis_checked: true,
            updated_unix: now_unix(),
        };
        save(&path, &checkpoint).unwrap();

        let loaded = load_for_resume(&path, &descriptor(), false)
            .unwrap()
            .unwrap();
        assert_eq!(loaded.next_start, 512);
        assert_eq!(loaded.counters.slots_ok, 42);
        assert_eq!(loaded.checked_anchors, BTreeSet::from([7]));
        assert!(loaded.genesis_checked);
    }

    #[test]
    fn resume_rejects_descriptor_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("checkpoint.json");
        let checkpoint = Checkpoint {
            descriptor: descriptor(),
            next_start: 512,
            counters: RunCounters::default(),
            checked_anchors: BTreeSet::new(),
            genesis_checked: false,
            updated_unix: now_unix(),
        };
        save(&path, &checkpoint).unwrap();

        let mut other = descriptor();
        other.mode = "structural".to_string();
        assert!(load_for_resume(&path, &other, false).is_err());
    }

    #[test]
    fn full_resume_accepts_a_later_tip_but_fixed_ranges_do_not() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("checkpoint.json");
        let checkpoint = Checkpoint {
            descriptor: descriptor(),
            next_start: 512,
            counters: RunCounters::default(),
            checked_anchors: BTreeSet::from([7]),
            genesis_checked: true,
            updated_unix: now_unix(),
        };
        save(&path, &checkpoint).unwrap();

        let mut live_tip = descriptor();
        live_tip.range_end = 2_000;
        assert!(load_for_resume(&path, &live_tip, true).unwrap().is_some());
        assert!(load_for_resume(&path, &live_tip, false).is_err());

        let mut regressed_tip = descriptor();
        regressed_tip.range_end = 999;
        assert!(load_for_resume(&path, &regressed_tip, true).is_err());
    }
}
