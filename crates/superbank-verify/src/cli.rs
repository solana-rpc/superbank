// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Configuration resolution: CLI flags > environment variables > YAML config
//! file > defaults, following the same three-layer merge as the ingestor's
//! `cli.rs`.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use clap::parser::ValueSource;
use clap::{ArgMatches, CommandFactory, FromArgMatches, Parser};
use serde::{Deserialize, Serialize};

use crate::eras::HashesPerTickSchedule;
use crate::range::{RangeSpec, parse_range_spec};
use crate::verify::VerifyMode;
use crate::verify::poh::Hash32;

pub(crate) const MAINNET_GENESIS_HASH: &str = "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d";

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum ModeArg {
    Structural,
    Full,
}

impl ModeArg {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            ModeArg::Structural => "structural",
            ModeArg::Full => "full",
        }
    }

    pub(crate) fn to_verify_mode(self) -> VerifyMode {
        match self {
            ModeArg::Structural => VerifyMode::Structural,
            ModeArg::Full => VerifyMode::Full,
        }
    }
}

#[derive(Parser, Debug)]
#[command(about = "Superbank Proof-of-History validator")]
struct CliArgs {
    /// Path to YAML config file
    #[arg(long, env = "SUPERBANK_VERIFY_CONFIG", value_name = "PATH")]
    config: Option<PathBuf>,

    /// Range to verify: "<start>:<end>" slots, "<a>-<b>" epochs, or "<e>" one epoch
    #[arg(long, env = "SUPERBANK_VERIFY_RANGE")]
    range: Option<String>,

    /// Verify everything present in blocks_metadata (genesis-to-tip)
    #[arg(
        long,
        env = "SUPERBANK_VERIFY_FULL",
        default_value_t = false,
        default_missing_value = "true",
        num_args = 0..=1,
        require_equals = true,
        action = clap::ArgAction::Set
    )]
    full: bool,

    /// Verification mode: structural (no hash recomputation) | full (recompute every hash)
    #[arg(
        long,
        env = "SUPERBANK_VERIFY_MODE",
        value_enum,
        default_value = "structural"
    )]
    mode: ModeArg,

    /// Slots per normal epoch (used to resolve epoch ranges)
    #[arg(
        long,
        env = "SUPERBANK_VERIFY_SLOTS_PER_EPOCH",
        default_value_t = 432_000
    )]
    slots_per_epoch: u64,

    /// Whether the cluster uses warmup epochs (mainnet-beta: true)
    #[arg(
        long,
        env = "SUPERBANK_VERIFY_EPOCH_WARMUP",
        default_value_t = true,
        default_missing_value = "true",
        num_args = 0..=1,
        require_equals = true,
        action = clap::ArgAction::Set
    )]
    epoch_warmup: bool,

    /// Ticks per slot (64 for all of mainnet history)
    #[arg(long, env = "SUPERBANK_VERIFY_TICKS_PER_SLOT", default_value_t = 64)]
    ticks_per_slot: u64,

    /// Expected genesis blockhash, checked against slot 0's parent_blockhash
    /// (base58; empty string disables the check)
    #[arg(
        long,
        env = "SUPERBANK_VERIFY_EXPECTED_GENESIS_HASH",
        default_value = MAINNET_GENESIS_HASH
    )]
    expected_genesis_hash: String,

    /// hashes_per_tick eras as "<from_slot>:<value>,..."; 0 disables the
    /// tick-hash-count check for that era (default: built-in mainnet history)
    #[arg(long, env = "SUPERBANK_VERIFY_HASHES_PER_TICK_SCHEDULE")]
    hashes_per_tick_schedule: Option<String>,

    /// Slots fetched and verified per window
    #[arg(long, env = "SUPERBANK_VERIFY_WINDOW_SLOTS", default_value_t = 256)]
    window_slots: u64,

    /// Windows prefetched ahead of verification
    #[arg(long, env = "SUPERBANK_VERIFY_FETCH_AHEAD", default_value_t = 3)]
    fetch_ahead: usize,

    /// Worker threads for hash recomputation (0 = all available cores)
    #[arg(long, env = "SUPERBANK_VERIFY_VERIFY_THREADS", default_value_t = 0)]
    verify_threads: usize,

    /// Checkpoint file for resumable runs
    #[arg(long, env = "SUPERBANK_VERIFY_CHECKPOINT_FILE", value_name = "PATH")]
    checkpoint_file: Option<PathBuf>,

    /// Resume from the checkpoint file instead of starting over
    #[arg(
        long,
        env = "SUPERBANK_VERIFY_RESUME",
        default_value_t = false,
        default_missing_value = "true",
        num_args = 0..=1,
        require_equals = true,
        action = clap::ArgAction::Set
    )]
    resume: bool,

    /// Write findings as JSONL to this file
    #[arg(long, env = "SUPERBANK_VERIFY_REPORT_FILE", value_name = "PATH")]
    report_file: Option<PathBuf>,

    /// Abort after this many failed slots (0 = unlimited)
    #[arg(long, env = "SUPERBANK_VERIFY_MAX_FAILURES", default_value_t = 0)]
    max_failures: u64,

    /// Exit 0 even when unverifiable or missing slots remain
    #[arg(
        long,
        env = "SUPERBANK_VERIFY_ALLOW_UNVERIFIABLE",
        default_value_t = false,
        default_missing_value = "true",
        num_args = 0..=1,
        require_equals = true,
        action = clap::ArgAction::Set
    )]
    allow_unverifiable: bool,

    /// External trust anchor "<slot>:<base58-blockhash>" (repeatable /
    /// comma-separated) checked against the recorded blockhash
    #[arg(
        long = "anchor",
        env = "SUPERBANK_VERIFY_ANCHOR",
        value_delimiter = ','
    )]
    anchor: Vec<String>,

    /// ClickHouse HTTP URL
    #[arg(long, env = "CLICKHOUSE_URL", default_value = "http://localhost:8123")]
    clickhouse_url: String,

    /// ClickHouse database
    #[arg(long, env = "CLICKHOUSE_DATABASE", default_value = "default")]
    clickhouse_database: String,

    /// ClickHouse user
    #[arg(long, env = "CLICKHOUSE_USER", default_value = "default")]
    clickhouse_user: String,

    /// ClickHouse password
    #[arg(long, env = "CLICKHOUSE_PASSWORD", default_value = "")]
    clickhouse_password: String,

    /// Blocks metadata table
    #[arg(
        long,
        env = "SUPERBANK_VERIFY_BLOCKS_TABLE",
        default_value = "default.blocks_metadata"
    )]
    blocks_table: String,

    /// Entries table
    #[arg(
        long,
        env = "SUPERBANK_VERIFY_ENTRIES_TABLE",
        default_value = "default.entries"
    )]
    entries_table: String,

    /// Transactions table
    #[arg(
        long,
        env = "SUPERBANK_VERIFY_TRANSACTIONS_TABLE",
        default_value = "default.transactions"
    )]
    transactions_table: String,

    /// Metrics listen host
    #[arg(long, env = "METRICS_HOST", default_value = "0.0.0.0")]
    metrics_host: String,

    /// Metrics listen port
    #[arg(long, env = "METRICS_PORT", default_value_t = 9902)]
    metrics_port: u16,

    /// /health returns 503 when no window completed for this many seconds (0 disables)
    #[arg(
        long,
        env = "SUPERBANK_VERIFY_HEALTH_STALE_SECS",
        default_value_t = 300
    )]
    health_stale_secs: u64,

    /// Optional cluster label added to all metrics
    #[arg(long, env = "SUPERBANK_VERIFY_METRICS_CLUSTER_LABEL")]
    metrics_cluster_label: Option<String>,

    /// Emit a progress log line roughly every N slots
    #[arg(
        long,
        env = "SUPERBANK_VERIFY_PROGRESS_EVERY_SLOTS",
        default_value_t = 10_000
    )]
    progress_every_slots: u64,

    /// Export one slot's block/entries/signatures as a JSON test fixture and exit
    #[arg(
        long,
        env = "SUPERBANK_VERIFY_EXPORT_FIXTURE_SLOT",
        value_name = "SLOT"
    )]
    export_fixture_slot: Option<u64>,

    /// Output path for --export-fixture-slot
    #[arg(long, env = "SUPERBANK_VERIFY_EXPORT_FIXTURE_OUT", value_name = "PATH")]
    export_fixture_out: Option<PathBuf>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    #[serde(alias = "range")]
    range: Option<String>,
    #[serde(rename = "full", alias = "full")]
    full: Option<bool>,
    #[serde(alias = "mode")]
    mode: Option<ModeArg>,
    #[serde(rename = "slots-per-epoch", alias = "slots_per_epoch")]
    slots_per_epoch: Option<u64>,
    #[serde(rename = "epoch-warmup", alias = "epoch_warmup")]
    epoch_warmup: Option<bool>,
    #[serde(rename = "ticks-per-slot", alias = "ticks_per_slot")]
    ticks_per_slot: Option<u64>,
    #[serde(rename = "expected-genesis-hash", alias = "expected_genesis_hash")]
    expected_genesis_hash: Option<String>,
    #[serde(
        rename = "hashes-per-tick-schedule",
        alias = "hashes_per_tick_schedule"
    )]
    hashes_per_tick_schedule: Option<String>,
    #[serde(rename = "window-slots", alias = "window_slots")]
    window_slots: Option<u64>,
    #[serde(rename = "fetch-ahead", alias = "fetch_ahead")]
    fetch_ahead: Option<usize>,
    #[serde(rename = "verify-threads", alias = "verify_threads")]
    verify_threads: Option<usize>,
    #[serde(rename = "checkpoint-file", alias = "checkpoint_file")]
    checkpoint_file: Option<PathBuf>,
    #[serde(alias = "resume")]
    resume: Option<bool>,
    #[serde(rename = "report-file", alias = "report_file")]
    report_file: Option<PathBuf>,
    #[serde(rename = "max-failures", alias = "max_failures")]
    max_failures: Option<u64>,
    #[serde(rename = "allow-unverifiable", alias = "allow_unverifiable")]
    allow_unverifiable: Option<bool>,
    #[serde(alias = "anchors", rename = "anchor")]
    anchor: Option<Vec<String>>,
    #[serde(rename = "clickhouse-url", alias = "clickhouse_url")]
    clickhouse_url: Option<String>,
    #[serde(rename = "clickhouse-database", alias = "clickhouse_database")]
    clickhouse_database: Option<String>,
    #[serde(rename = "clickhouse-user", alias = "clickhouse_user")]
    clickhouse_user: Option<String>,
    #[serde(rename = "clickhouse-password", alias = "clickhouse_password")]
    clickhouse_password: Option<String>,
    #[serde(rename = "blocks-table", alias = "blocks_table")]
    blocks_table: Option<String>,
    #[serde(rename = "entries-table", alias = "entries_table")]
    entries_table: Option<String>,
    #[serde(rename = "transactions-table", alias = "transactions_table")]
    transactions_table: Option<String>,
    #[serde(rename = "metrics-host", alias = "metrics_host")]
    metrics_host: Option<String>,
    #[serde(rename = "metrics-port", alias = "metrics_port")]
    metrics_port: Option<u16>,
    #[serde(rename = "health-stale-secs", alias = "health_stale_secs")]
    health_stale_secs: Option<u64>,
    #[serde(rename = "metrics-cluster-label", alias = "metrics_cluster_label")]
    metrics_cluster_label: Option<String>,
    #[serde(rename = "progress-every-slots", alias = "progress_every_slots")]
    progress_every_slots: Option<u64>,
}

#[derive(Debug, Clone)]
pub(crate) struct Args {
    pub(crate) range: Option<RangeSpec>,
    pub(crate) full: bool,
    pub(crate) mode: ModeArg,
    pub(crate) slots_per_epoch: u64,
    pub(crate) epoch_warmup: bool,
    pub(crate) ticks_per_slot: u64,
    pub(crate) expected_genesis_hash: Option<Hash32>,
    pub(crate) hashes_per_tick_schedule: HashesPerTickSchedule,
    pub(crate) window_slots: u64,
    pub(crate) fetch_ahead: usize,
    pub(crate) verify_threads: usize,
    pub(crate) checkpoint_file: Option<PathBuf>,
    pub(crate) resume: bool,
    pub(crate) report_file: Option<PathBuf>,
    pub(crate) max_failures: u64,
    pub(crate) allow_unverifiable: bool,
    pub(crate) anchors: Vec<(u64, Hash32)>,
    pub(crate) clickhouse_url: String,
    pub(crate) clickhouse_database: String,
    pub(crate) clickhouse_user: String,
    pub(crate) clickhouse_password: String,
    pub(crate) blocks_table: String,
    pub(crate) entries_table: String,
    pub(crate) transactions_table: String,
    pub(crate) metrics_host: String,
    pub(crate) metrics_port: u16,
    pub(crate) health_stale_secs: u64,
    pub(crate) metrics_cluster_label: Option<String>,
    pub(crate) progress_every_slots: u64,
    pub(crate) export_fixture: Option<(u64, PathBuf)>,
}

pub(crate) fn resolve_args() -> Result<Args> {
    let matches = CliArgs::command().get_matches();
    let cli = CliArgs::from_arg_matches(&matches).context("parse CLI arguments")?;
    resolve_from(&matches, cli)
}

fn resolve_from(matches: &ArgMatches, cli: CliArgs) -> Result<Args> {
    let config = load_config(cli.config.as_deref())?.unwrap_or_default();

    // --range / --full form ONE logical selector: when either is set
    // explicitly (CLI or env), the config file's pair is ignored entirely —
    // otherwise a config with `full: true` could never be overridden by
    // `--range` (and vice versa).
    let selector_explicit =
        !should_use_config(matches, "range") || !should_use_config(matches, "full");
    let (range_spec, full) = if selector_explicit {
        (cli.range, cli.full)
    } else {
        (cli.range.or(config.range), config.full.unwrap_or(cli.full))
    };
    let range = range_spec
        .as_deref()
        .map(parse_range_spec)
        .transpose()
        .context("parse --range")?;

    let expected_genesis_hash = merge_value(
        matches,
        "expected_genesis_hash",
        cli.expected_genesis_hash,
        config.expected_genesis_hash,
    );
    let expected_genesis_hash = match expected_genesis_hash.trim() {
        "" => None,
        value => Some(parse_hash32(value).context("parse --expected-genesis-hash")?),
    };

    let hashes_per_tick_schedule = match merge_option(
        matches,
        "hashes_per_tick_schedule",
        cli.hashes_per_tick_schedule,
        config.hashes_per_tick_schedule,
    ) {
        Some(spec) => {
            HashesPerTickSchedule::parse(&spec).context("parse --hashes-per-tick-schedule")?
        }
        None => HashesPerTickSchedule::mainnet(),
    };

    let anchor_specs = if matches
        .value_source("anchor")
        .is_none_or(|source| matches!(source, ValueSource::DefaultValue))
    {
        config.anchor.unwrap_or(cli.anchor)
    } else {
        cli.anchor
    };
    let anchors = anchor_specs
        .iter()
        .map(|spec| parse_anchor(spec))
        .collect::<Result<Vec<_>>>()?;

    let export_fixture = match (cli.export_fixture_slot, cli.export_fixture_out) {
        (Some(slot), Some(path)) => Some((slot, path)),
        (Some(_), None) => bail!("--export-fixture-slot requires --export-fixture-out"),
        (None, Some(_)) => bail!("--export-fixture-out requires --export-fixture-slot"),
        (None, None) => None,
    };

    let args = Args {
        range,
        full,
        mode: merge_value(matches, "mode", cli.mode, config.mode),
        slots_per_epoch: merge_value(
            matches,
            "slots_per_epoch",
            cli.slots_per_epoch,
            config.slots_per_epoch,
        ),
        epoch_warmup: merge_value(
            matches,
            "epoch_warmup",
            cli.epoch_warmup,
            config.epoch_warmup,
        ),
        ticks_per_slot: merge_value(
            matches,
            "ticks_per_slot",
            cli.ticks_per_slot,
            config.ticks_per_slot,
        ),
        expected_genesis_hash,
        hashes_per_tick_schedule,
        window_slots: merge_value(
            matches,
            "window_slots",
            cli.window_slots,
            config.window_slots,
        ),
        fetch_ahead: merge_value(matches, "fetch_ahead", cli.fetch_ahead, config.fetch_ahead),
        verify_threads: merge_value(
            matches,
            "verify_threads",
            cli.verify_threads,
            config.verify_threads,
        ),
        checkpoint_file: merge_option(
            matches,
            "checkpoint_file",
            cli.checkpoint_file,
            config.checkpoint_file,
        ),
        resume: merge_value(matches, "resume", cli.resume, config.resume),
        report_file: merge_option(matches, "report_file", cli.report_file, config.report_file),
        max_failures: merge_value(
            matches,
            "max_failures",
            cli.max_failures,
            config.max_failures,
        ),
        allow_unverifiable: merge_value(
            matches,
            "allow_unverifiable",
            cli.allow_unverifiable,
            config.allow_unverifiable,
        ),
        anchors,
        clickhouse_url: merge_value(
            matches,
            "clickhouse_url",
            cli.clickhouse_url,
            config.clickhouse_url,
        ),
        clickhouse_database: merge_value(
            matches,
            "clickhouse_database",
            cli.clickhouse_database,
            config.clickhouse_database,
        ),
        clickhouse_user: merge_value(
            matches,
            "clickhouse_user",
            cli.clickhouse_user,
            config.clickhouse_user,
        ),
        clickhouse_password: merge_value(
            matches,
            "clickhouse_password",
            cli.clickhouse_password,
            config.clickhouse_password,
        ),
        blocks_table: merge_value(
            matches,
            "blocks_table",
            cli.blocks_table,
            config.blocks_table,
        ),
        entries_table: merge_value(
            matches,
            "entries_table",
            cli.entries_table,
            config.entries_table,
        ),
        transactions_table: merge_value(
            matches,
            "transactions_table",
            cli.transactions_table,
            config.transactions_table,
        ),
        metrics_host: merge_value(
            matches,
            "metrics_host",
            cli.metrics_host,
            config.metrics_host,
        ),
        metrics_port: merge_value(
            matches,
            "metrics_port",
            cli.metrics_port,
            config.metrics_port,
        ),
        health_stale_secs: merge_value(
            matches,
            "health_stale_secs",
            cli.health_stale_secs,
            config.health_stale_secs,
        ),
        metrics_cluster_label: merge_option(
            matches,
            "metrics_cluster_label",
            cli.metrics_cluster_label,
            config.metrics_cluster_label,
        ),
        progress_every_slots: merge_value(
            matches,
            "progress_every_slots",
            cli.progress_every_slots,
            config.progress_every_slots,
        ),
        export_fixture,
    };

    validate_args(&args)?;
    Ok(args)
}

fn validate_args(args: &Args) -> Result<()> {
    if args.export_fixture.is_none() {
        match (args.range.is_some(), args.full) {
            (true, true) => bail!("--range and --full are mutually exclusive"),
            (false, false) => bail!("one of --range or --full is required"),
            _ => {}
        }
    }
    if args.resume && args.checkpoint_file.is_none() {
        bail!("--resume requires --checkpoint-file");
    }
    if args.window_slots == 0 {
        bail!("--window-slots must be at least 1");
    }
    if args.fetch_ahead == 0 {
        bail!("--fetch-ahead must be at least 1");
    }
    if args.ticks_per_slot == 0 {
        bail!("--ticks-per-slot must be at least 1");
    }
    Ok(())
}

pub(crate) fn parse_hash32(value: &str) -> Result<Hash32> {
    let bytes = bs58::decode(value.trim())
        .into_vec()
        .map_err(|err| anyhow!("invalid base58 hash {value:?}: {err}"))?;
    let hash: Hash32 = bytes.as_slice().try_into().map_err(|_| {
        anyhow!(
            "hash {value:?} must decode to 32 bytes, got {}",
            bytes.len()
        )
    })?;
    Ok(hash)
}

fn parse_anchor(spec: &str) -> Result<(u64, Hash32)> {
    let (slot, hash) = spec
        .split_once(':')
        .ok_or_else(|| anyhow!("anchor must be <slot>:<base58-blockhash>, got {spec:?}"))?;
    let slot: u64 = slot
        .trim()
        .parse()
        .map_err(|_| anyhow!("invalid anchor slot in {spec:?}"))?;
    let hash = parse_hash32(hash).with_context(|| format!("parse anchor {spec:?}"))?;
    Ok((slot, hash))
}

fn load_config(path: Option<&Path>) -> Result<Option<FileConfig>> {
    let Some(path) = path else {
        return Ok(None);
    };

    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("read config file {}", path.display()))?;
    let config = serde_yaml::from_str::<FileConfig>(&contents)
        .with_context(|| format!("parse config file {}", path.display()))?;
    Ok(Some(config))
}

fn should_use_config(matches: &ArgMatches, name: &str) -> bool {
    matches
        .value_source(name)
        .is_none_or(|source| matches!(source, ValueSource::DefaultValue))
}

fn merge_value<T>(matches: &ArgMatches, name: &str, cli: T, config: Option<T>) -> T {
    if should_use_config(matches, name) {
        config.unwrap_or(cli)
    } else {
        cli
    }
}

fn merge_option<T>(
    matches: &ArgMatches,
    name: &str,
    cli: Option<T>,
    config: Option<T>,
) -> Option<T> {
    if should_use_config(matches, name) {
        cli.or(config)
    } else {
        cli
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolve(argv: &[&str]) -> Result<Args> {
        let matches = CliArgs::command().get_matches_from(argv);
        let cli = CliArgs::from_arg_matches(&matches).unwrap();
        resolve_from(&matches, cli)
    }

    #[test]
    fn requires_range_or_full() {
        assert!(resolve(&["superbank-verify"]).is_err());
        assert!(resolve(&["superbank-verify", "--range", "1:2", "--full"]).is_err());
        assert!(resolve(&["superbank-verify", "--range", "1:2"]).is_ok());
        assert!(resolve(&["superbank-verify", "--full"]).is_ok());
    }

    #[test]
    fn defaults_are_mainnet_shaped() {
        let args = resolve(&["superbank-verify", "--full"]).unwrap();
        assert_eq!(args.mode, ModeArg::Structural);
        assert_eq!(args.slots_per_epoch, 432_000);
        assert!(args.epoch_warmup);
        assert_eq!(args.ticks_per_slot, 64);
        assert_eq!(
            args.expected_genesis_hash,
            Some(parse_hash32(MAINNET_GENESIS_HASH).unwrap())
        );
        assert_eq!(
            args.hashes_per_tick_schedule,
            HashesPerTickSchedule::mainnet()
        );
        assert_eq!(args.metrics_port, 9902);
    }

    #[test]
    fn empty_genesis_hash_disables_check() {
        let args = resolve(&["superbank-verify", "--full", "--expected-genesis-hash", ""]).unwrap();
        assert_eq!(args.expected_genesis_hash, None);
    }

    #[test]
    fn parses_anchors() {
        let args = resolve(&[
            "superbank-verify",
            "--full",
            "--anchor",
            &format!("5:{MAINNET_GENESIS_HASH}"),
            "--anchor",
            &format!("9:{MAINNET_GENESIS_HASH}"),
        ])
        .unwrap();
        assert_eq!(args.anchors.len(), 2);
        assert_eq!(args.anchors[0].0, 5);
        assert!(resolve(&["superbank-verify", "--full", "--anchor", "nope"]).is_err());
    }

    #[test]
    fn resume_requires_checkpoint_file() {
        assert!(resolve(&["superbank-verify", "--full", "--resume"]).is_err());
        assert!(
            resolve(&[
                "superbank-verify",
                "--full",
                "--resume",
                "--checkpoint-file",
                "/tmp/cp.json"
            ])
            .is_ok()
        );
    }

    #[test]
    fn fixture_export_needs_both_flags() {
        assert!(resolve(&["superbank-verify", "--export-fixture-slot", "5"]).is_err());
        let args = resolve(&[
            "superbank-verify",
            "--export-fixture-slot",
            "5",
            "--export-fixture-out",
            "/tmp/fixture.json",
        ])
        .unwrap();
        assert_eq!(args.export_fixture.as_ref().unwrap().0, 5);
    }

    #[test]
    fn cli_range_overrides_config_full() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(&path, "full: true\n").unwrap();
        let path = path.to_str().unwrap();

        // Config alone selects --full.
        let args = resolve(&["superbank-verify", "--config", path]).unwrap();
        assert!(args.full);
        assert!(args.range.is_none());

        // An explicit --range must win over the config's `full: true`.
        let args = resolve(&["superbank-verify", "--config", path, "--range", "1:2"]).unwrap();
        assert!(!args.full);
        assert!(args.range.is_some());
    }

    #[test]
    fn cli_full_overrides_config_range() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(&path, "range: \"5:9\"\n").unwrap();
        let path = path.to_str().unwrap();

        let args = resolve(&["superbank-verify", "--config", path]).unwrap();
        assert!(args.range.is_some());

        let args = resolve(&["superbank-verify", "--config", path, "--full"]).unwrap();
        assert!(args.full);
        assert!(args.range.is_none());

        // A config supplying both is still rejected.
        std::fs::write(dir.path().join("both.yaml"), "range: \"5:9\"\nfull: true\n").unwrap();
        let both = dir.path().join("both.yaml");
        assert!(resolve(&["superbank-verify", "--config", both.to_str().unwrap()]).is_err());
    }

    #[test]
    fn file_config_parses_kebab_and_snake_case() {
        let config: FileConfig = serde_yaml::from_str(
            "window-slots: 512\nticks_per_slot: 32\nmode: full\nanchor:\n  - \"5:abc\"\n",
        )
        .expect("parse config");
        assert_eq!(config.window_slots, Some(512));
        assert_eq!(config.ticks_per_slot, Some(32));
        assert_eq!(config.mode, Some(ModeArg::Full));
    }
}
