// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::io;
use std::path::Path;

use solana_epoch_schedule::EpochSchedule;
use solana_genesis_config::GenesisConfig;

/// Decode a [`GenesisConfig`] from raw `genesis.bin` bytes and return its epoch
/// schedule. Pure: identical bytes always produce the same schedule.
fn epoch_schedule_from_bytes(bytes: &[u8]) -> io::Result<EpochSchedule> {
    let config: GenesisConfig = bincode::deserialize(bytes)
        .map_err(|err| io::Error::other(format!("failed to decode genesis.bin: {err}")))?;
    Ok(config.epoch_schedule)
}

/// Read `genesis.bin` at `path` and return the cluster's epoch schedule.
pub(crate) fn load_epoch_schedule(path: &Path) -> io::Result<EpochSchedule> {
    let bytes = std::fs::read(path)?;
    epoch_schedule_from_bytes(&bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_epoch_schedule_from_genesis_bytes() {
        let mut config = GenesisConfig::new(&[], &[]);
        config.epoch_schedule = EpochSchedule::custom(8192, 8192, true);
        let bytes = bincode::serialize(&config).expect("serialize genesis");

        let schedule = epoch_schedule_from_bytes(&bytes).expect("decode schedule");
        assert_eq!(schedule, EpochSchedule::custom(8192, 8192, true));
    }

    #[test]
    fn rejects_non_genesis_bytes() {
        assert!(epoch_schedule_from_bytes(b"not a genesis file").is_err());
    }

    #[test]
    fn loads_epoch_schedule_from_file() {
        use std::io::Write;

        let mut config = GenesisConfig::new(&[], &[]);
        config.epoch_schedule = EpochSchedule::without_warmup();
        let bytes = bincode::serialize(&config).expect("serialize genesis");

        let mut file = tempfile::NamedTempFile::new().expect("temp file");
        file.write_all(&bytes).expect("write genesis");

        let schedule = load_epoch_schedule(file.path()).expect("load schedule");
        assert_eq!(schedule, EpochSchedule::without_warmup());
    }

    #[test]
    fn load_missing_file_errors() {
        assert!(load_epoch_schedule(Path::new("/nonexistent/genesis.bin")).is_err());
    }
}
