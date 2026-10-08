// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use jetstreamer::JetstreamerRunner;
use jetstreamer_clickhouse_plugin::{ClickhouseIngestConfig, ClickhouseIngestPlugin};
use jetstreamer_firehose::epochs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() || args.len() > 1 || args[0] == "-h" || args[0] == "--help" {
        eprintln!("usage: jetstreamer-clickhouse <epoch|start:end>");
        std::process::exit(1);
    }

    let (start_slot, end_inclusive) = if let Some((start, end)) = args[0].split_once(':') {
        let start_slot: u64 = start.parse().map_err(|_| "invalid start slot")?;
        let end_slot: u64 = end.parse().map_err(|_| "invalid end slot")?;
        if start_slot > end_slot {
            return Err("start slot must be <= end slot".into());
        }
        (start_slot, end_slot)
    } else {
        let epoch: u64 = args[0].parse().map_err(|_| "invalid epoch")?;
        checked_epoch_range(epoch)?
    };

    let threads = std::env::var("JETSTREAMER_THREADS")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or_else(jetstreamer_firehose::system::optimal_firehose_thread_count);

    let plugin = ClickhouseIngestPlugin::new(ClickhouseIngestConfig::default(), threads);

    let range = plugin.historical_slot_range(start_slot, end_inclusive)?;
    if range.end - 1 < end_inclusive {
        eprintln!(
            "clamped requested range to qualified historical slots {}:{}",
            range.start,
            range.end - 1
        );
    }

    JetstreamerRunner::default()
        .with_log_level("info")
        .with_threads(threads)
        .with_slot_range(range)
        .with_plugin(Box::new(plugin))
        .run()
        .map_err(|err| -> Box<dyn std::error::Error> { Box::new(err) })
}

fn checked_epoch_range(epoch: u64) -> Result<(u64, u64), &'static str> {
    if epoch > (u64::MAX - 431_999) / 432_000 {
        return Err("epoch range overflows u64");
    }
    Ok(epochs::epoch_to_slot_range(epoch))
}

#[cfg(test)]
mod tests {
    #[test]
    fn epoch_range_validates_both_multiplication_and_final_slot_addition() {
        let last = (u64::MAX - 431_999) / 432_000;
        assert!(super::checked_epoch_range(last).is_ok());
        assert!(super::checked_epoch_range(last + 1).is_err());
        assert!(super::checked_epoch_range(u64::MAX).is_err());
        assert_eq!(
            super::checked_epoch_range(800).unwrap(),
            (345_600_000, 346_031_999)
        );
    }
}
