// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Local resume state for long verification runs. Windows complete strictly
//! in ascending slot order, so a single high-water mark (`next_start`) plus
//! the accumulated counters captures the full run state. The file is written
//! atomically (temp file + rename) after every window.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::report::RunCounters;

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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Checkpoint {
    pub(crate) descriptor: JobDescriptor,
    /// All slots strictly below this are fully accounted in `counters`.
    pub(crate) next_start: u64,
    pub(crate) counters: RunCounters,
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

/// Load a checkpoint for resuming: the descriptor must match exactly.
pub(crate) fn load_for_resume(
    path: &Path,
    descriptor: &JobDescriptor,
) -> Result<Option<Checkpoint>> {
    let Some(checkpoint) = load(path)? else {
        return Ok(None);
    };
    if checkpoint.descriptor != *descriptor {
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
            updated_unix: now_unix(),
        };
        save(&path, &checkpoint).unwrap();

        let loaded = load_for_resume(&path, &descriptor()).unwrap().unwrap();
        assert_eq!(loaded.next_start, 512);
        assert_eq!(loaded.counters.slots_ok, 42);
    }

    #[test]
    fn resume_rejects_descriptor_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("checkpoint.json");
        let checkpoint = Checkpoint {
            descriptor: descriptor(),
            next_start: 512,
            counters: RunCounters::default(),
            updated_unix: now_unix(),
        };
        save(&path, &checkpoint).unwrap();

        let mut other = descriptor();
        other.mode = "structural".to_string();
        assert!(load_for_resume(&path, &other).is_err());
    }
}
