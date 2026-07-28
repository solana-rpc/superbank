// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::path::Path;

use anyhow::{Context, Result, anyhow};
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, BufReader},
};

/// Parse a whitespace-separated slot list file (`#` starts a line comment).
/// Returns a sorted, de-duplicated list of slot numbers. Shared by the
/// Bigtable (`--bigtable-slot-file`) and RPC (`--rpc-slot-list`) sources.
pub(crate) async fn load_slot_list(path: &Path) -> Result<Vec<u64>> {
    let file = File::open(path)
        .await
        .with_context(|| format!("open slot list {}", path.display()))?;
    let mut lines = BufReader::new(file).lines();
    let mut slots = Vec::new();
    let mut line_number = 0usize;

    while let Some(line) = lines
        .next_line()
        .await
        .with_context(|| format!("read slot list {}", path.display()))?
    {
        line_number = line_number.saturating_add(1);
        for token in line.split_whitespace() {
            if token.starts_with('#') {
                break;
            }
            let slot = token.parse::<u64>().with_context(|| {
                format!(
                    "invalid slot '{}' on line {} in {}",
                    token,
                    line_number,
                    path.display()
                )
            })?;
            slots.push(slot);
        }
    }

    if slots.is_empty() {
        return Err(anyhow!("slot list {} was empty", path.display()));
    }

    slots.sort_unstable();
    slots.dedup();
    Ok(slots)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RangeSpec {
    Slots { start: u64, end: u64 },
    Epochs { start: u64, end: u64 },
}

impl RangeSpec {
    pub(crate) fn is_epoch(self) -> bool {
        matches!(self, RangeSpec::Epochs { .. })
    }
}

pub(crate) fn parse_range_spec(value: &str) -> Result<RangeSpec> {
    let normalized = value.trim();
    if normalized.is_empty() {
        return Err(anyhow!("range cannot be empty"));
    }

    if normalized.contains(':') {
        let parts: Vec<_> = normalized.split(':').collect();
        if parts.len() != 2 {
            return Err(anyhow!("slot range must be formatted as <start>:<end>"));
        }
        let start = parse_part("slot start", parts[0])?;
        let end = parse_part("slot end", parts[1])?;
        if start > end {
            return Err(anyhow!(
                "slot range start {} must be less than or equal to end {}",
                start,
                end
            ));
        }
        return Ok(RangeSpec::Slots { start, end });
    }

    if normalized.contains('-') {
        let parts: Vec<_> = normalized.split('-').collect();
        if parts.len() != 2 {
            return Err(anyhow!("epoch range must be formatted as <start>-<end>"));
        }
        let start = parse_part("epoch start", parts[0])?;
        let end = parse_part("epoch end", parts[1])?;
        if start > end {
            return Err(anyhow!(
                "epoch range start {} must be less than or equal to end {}",
                start,
                end
            ));
        }
        return Ok(RangeSpec::Epochs { start, end });
    }

    let epoch = parse_part("epoch", normalized)?;
    Ok(RangeSpec::Epochs {
        start: epoch,
        end: epoch,
    })
}

fn parse_part(label: &str, value: &str) -> Result<u64> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("{label} cannot be empty"));
    }
    trimmed
        .parse::<u64>()
        .map_err(|_| anyhow!("{label} must be an unsigned integer"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_slot_range() {
        assert_eq!(
            parse_range_spec("123:456").unwrap(),
            RangeSpec::Slots {
                start: 123,
                end: 456
            }
        );
    }

    #[test]
    fn parses_epoch_range() {
        assert_eq!(
            parse_range_spec("1-10").unwrap(),
            RangeSpec::Epochs { start: 1, end: 10 }
        );
    }

    #[test]
    fn parses_single_epoch() {
        assert_eq!(
            parse_range_spec("5").unwrap(),
            RangeSpec::Epochs { start: 5, end: 5 }
        );
    }

    #[test]
    fn rejects_empty() {
        assert!(parse_range_spec("").is_err());
    }

    #[test]
    fn rejects_invalid_slot_range() {
        assert!(parse_range_spec("1:0").is_err());
    }
}
