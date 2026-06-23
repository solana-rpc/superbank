use std::{
    fmt,
    io::{self, IsTerminal, Write},
    path::PathBuf,
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Result, anyhow};
use serde::Serialize;
use tracing::{debug, info};

use crate::{
    clickhouse::{ClickHouseClient, DbTables, ValidationReport},
    config::Config,
    storage::{self, ArchiveDestination},
};

pub const HOURLY_SLOTS: u64 = 9_000;
pub const EPOCH_SLOTS: u64 = 432_000;
pub const DEFAULT_CUSTOM_SLOTS: u64 = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum ArchiveKind {
    Hourly,
    Epoch,
    Custom { slots: u64 },
}

impl ArchiveKind {
    pub fn label(self) -> &'static str {
        match self {
            ArchiveKind::Hourly => "hourly",
            ArchiveKind::Epoch => "epoch",
            ArchiveKind::Custom { .. } => "custom",
        }
    }

    pub fn slot_count(self) -> u64 {
        match self {
            ArchiveKind::Hourly => HOURLY_SLOTS,
            ArchiveKind::Epoch => EPOCH_SLOTS,
            ArchiveKind::Custom { slots } => slots,
        }
    }

    fn first_start_slot(self, earliest_slot: u64) -> u64 {
        match self {
            ArchiveKind::Epoch => align_up(earliest_slot, EPOCH_SLOTS),
            ArchiveKind::Hourly | ArchiveKind::Custom { .. } => earliest_slot,
        }
    }
}

impl FromStr for ArchiveKind {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        let normalized = value.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "hourly" => Ok(ArchiveKind::Hourly),
            "epoch" => Ok(ArchiveKind::Epoch),
            "custom" => Ok(ArchiveKind::Custom {
                slots: DEFAULT_CUSTOM_SLOTS,
            }),
            _ if normalized.starts_with("custom:") => {
                let slots = normalized
                    .trim_start_matches("custom:")
                    .parse::<u64>()
                    .map_err(|_| format!("invalid custom archive slot count in '{value}'"))?;
                if slots == 0 {
                    return Err("custom archive slot count must be greater than zero".to_string());
                }
                Ok(ArchiveKind::Custom { slots })
            }
            _ => Err(format!(
                "unsupported archive range type '{value}' (valid: hourly, epoch, custom, custom:<slots>)"
            )),
        }
    }
}

impl fmt::Display for ArchiveKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ArchiveKind::Hourly => formatter.write_str("hourly"),
            ArchiveKind::Epoch => formatter.write_str("epoch"),
            ArchiveKind::Custom { slots } => write!(formatter, "custom:{slots}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ClickHouseBounds {
    pub earliest_slot: u64,
    pub latest_slot: u64,
}

impl ClickHouseBounds {
    pub fn slots_available(self) -> u64 {
        self.latest_slot
            .saturating_sub(self.earliest_slot)
            .saturating_add(1)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArchivePlan {
    pub kind: ArchiveKind,
    pub epoch: u64,
    pub start_slot: u64,
    pub end_slot: u64,
}

impl ArchivePlan {
    pub fn file_name(&self) -> String {
        format!(
            "{}_{}_{}-{}.parquet",
            self.kind.label(),
            self.epoch,
            self.start_slot,
            self.end_slot
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedArchiveName {
    pub kind_label: String,
    pub epoch: u64,
    pub start_slot: u64,
    pub end_slot: u64,
}

pub fn parse_archive_name(file_name: &str) -> Option<ParsedArchiveName> {
    let stem = file_name.strip_suffix(".parquet")?;
    let mut parts = stem.split('_');
    let kind_label = parts.next()?.to_string();
    let epoch = parts.next()?.parse().ok()?;
    let range = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    let (start, end) = range.split_once('-')?;
    Some(ParsedArchiveName {
        kind_label,
        epoch,
        start_slot: start.parse().ok()?,
        end_slot: end.parse().ok()?,
    })
}

pub fn plan_next_archive(
    kind: ArchiveKind,
    bounds: ClickHouseBounds,
    last_archive_name: Option<&str>,
    continue_from_last_archive: bool,
) -> Result<Option<ArchivePlan>> {
    if bounds.latest_slot < bounds.earliest_slot {
        return Ok(None);
    }

    let start_slot = match last_archive_name.filter(|_| continue_from_last_archive) {
        Some(file_name) => {
            let parsed = parse_archive_name(file_name)
                .ok_or_else(|| anyhow!("unable to parse last archive name '{file_name}'"))?;
            if parsed.kind_label != kind.label() {
                return Err(anyhow!(
                    "last archive name '{file_name}' does not match archive type '{}'",
                    kind.label()
                ));
            }
            if parsed.end_slot < bounds.earliest_slot {
                kind.first_start_slot(bounds.earliest_slot)
            } else {
                parsed
                    .end_slot
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("last archive end slot overflowed"))?
            }
        }
        None => kind.first_start_slot(bounds.earliest_slot),
    };

    let slot_count = kind.slot_count();
    let end_slot = start_slot
        .checked_add(slot_count - 1)
        .ok_or_else(|| anyhow!("archive end slot overflowed"))?;

    if end_slot > bounds.latest_slot {
        return Ok(None);
    }

    Ok(Some(ArchivePlan {
        kind,
        epoch: start_slot / EPOCH_SLOTS,
        start_slot,
        end_slot,
    }))
}

pub fn should_delete_archived_data_range(config: &Config, kind: ArchiveKind) -> bool {
    config.delete_archived_data_range
        && config
            .archive_kinds
            .iter()
            .map(|archive_kind| archive_kind.slot_count())
            .max()
            .map(|max_slots| kind.slot_count() == max_slots)
            .unwrap_or(false)
}

#[derive(Debug, Clone, Serialize)]
pub struct ArchiveRunReport {
    pub timestamp_unix: u64,
    pub archive_created: bool,
    pub archive_skipped_reason: Option<String>,
    pub archive_name: Option<String>,
    pub archive_kind: ArchiveKind,
    pub archive_epoch: Option<u64>,
    pub archive_slot_start: Option<u64>,
    pub archive_slot_end: Option<u64>,
    pub db_bounds: Option<ClickHouseBounds>,
    pub destination: String,
    pub validation: Option<ValidationReport>,
    pub deleted_clickhouse_range: bool,
    pub cleaned_archives: Vec<String>,
}

impl ArchiveRunReport {
    pub fn skipped(kind: ArchiveKind, destination: String, reason: impl Into<String>) -> Self {
        Self {
            timestamp_unix: unix_timestamp(),
            archive_created: false,
            archive_skipped_reason: Some(reason.into()),
            archive_name: None,
            archive_kind: kind,
            archive_epoch: None,
            archive_slot_start: None,
            archive_slot_end: None,
            db_bounds: None,
            destination,
            validation: None,
            deleted_clickhouse_range: false,
            cleaned_archives: Vec::new(),
        }
    }

    pub fn to_text(&self) -> String {
        let mut lines = Vec::new();
        lines.push(format!("timestamp_unix: {}", self.timestamp_unix));
        lines.push(format!("archive_created: {}", self.archive_created));
        if let Some(reason) = &self.archive_skipped_reason {
            lines.push(format!("archive_skipped_reason: {reason}"));
        }
        if let Some(name) = &self.archive_name {
            lines.push(format!("archive_name: {name}"));
        }
        lines.push(format!("archive_kind: {}", self.archive_kind));
        if let Some(epoch) = self.archive_epoch {
            lines.push(format!("archive_epoch: {epoch}"));
        }
        if let Some(start) = self.archive_slot_start {
            lines.push(format!("archive_slot_start: {start}"));
        }
        if let Some(end) = self.archive_slot_end {
            lines.push(format!("archive_slot_end: {end}"));
        }
        if let Some(bounds) = self.db_bounds {
            lines.push(format!("db_earliest_slot: {}", bounds.earliest_slot));
            lines.push(format!("db_latest_slot: {}", bounds.latest_slot));
            lines.push(format!("db_slots_available: {}", bounds.slots_available()));
        }
        lines.push(format!("destination: {}", self.destination));
        lines.push(format!(
            "deleted_clickhouse_range: {}",
            self.deleted_clickhouse_range
        ));
        if !self.cleaned_archives.is_empty() {
            lines.push(format!(
                "cleaned_archives: {}",
                self.cleaned_archives.join(",")
            ));
        }
        if let Some(validation) = &self.validation {
            lines.push(format!(
                "validation_missing_blocks: {}",
                validation.missing_blocks.len()
            ));
            lines.push(format!(
                "validation_missing_block_ranges: {}",
                ValidationReport::format_ranges(&validation.missing_block_ranges)
            ));
            lines.push(format!(
                "validation_not_produced_slot_ranges: {}",
                ValidationReport::format_ranges(&validation.not_produced_slot_ranges)
            ));
            lines.push(format!(
                "validation_transaction_mismatches: {}",
                validation.transaction_mismatches.len()
            ));
            lines.push(format!(
                "validation_transaction_mismatch_ranges: {}",
                ValidationReport::format_ranges(&validation.transaction_mismatch_ranges)
            ));
            if let Some(error) = &validation.rpc_check_error {
                lines.push(format!("validation_rpc_check_error: {error}"));
            }
        }
        lines.join("\n")
    }
}

pub async fn run_once(config: &Config) -> Result<ArchiveRunReport> {
    run_once_for_kind(config, config.archive_kinds[0]).await
}

pub async fn run_once_for_kind(config: &Config, kind: ArchiveKind) -> Result<ArchiveRunReport> {
    let destination = storage::destination(config, kind).await?;
    debug!(
        archive_kind = kind.to_string(),
        destination = destination.describe(),
        "resolved archive destination"
    );
    let client = ClickHouseClient::from_config(config)?;
    let tables = DbTables::from_config(config);
    info!(
        archive_kind = kind.to_string(),
        transactions_table = tables.transactions_table,
        blocks_table = tables.blocks_table,
        "checking ClickHouse archive source"
    );
    client
        .check_tables(&tables, should_delete_archived_data_range(config, kind))
        .await?;

    let Some(bounds) = client.fetch_bounds(&tables.transactions_table).await? else {
        return Ok(ArchiveRunReport::skipped(
            kind,
            destination.describe(),
            "no transactions found in ClickHouse",
        ));
    };
    info!(
        archive_kind = kind.to_string(),
        earliest_slot = bounds.earliest_slot,
        latest_slot = bounds.latest_slot,
        slots_available = bounds.slots_available(),
        "ClickHouse archive source has slots available"
    );

    let last_archive = storage::latest_archive_name(config, kind).await?;
    debug!(
        archive_kind = kind.to_string(),
        last_archive = last_archive.as_deref().unwrap_or("none"),
        continue_from_last_archive = config.continue_from_last_archive,
        "loaded latest archive state"
    );
    if !config.continue_from_last_archive {
        info!(
            archive_kind = kind.to_string(),
            last_archive = last_archive.as_deref().unwrap_or("none"),
            "archive continuation disabled; planning from oldest ClickHouse slot"
        );
    }
    let Some(plan) = plan_next_archive(
        kind,
        bounds,
        last_archive.as_deref(),
        config.continue_from_last_archive,
    )?
    else {
        let mut report = ArchiveRunReport::skipped(
            kind,
            destination.describe(),
            "not enough ClickHouse slots available for the next archive",
        );
        report.db_bounds = Some(bounds);
        return Ok(report);
    };
    info!(
        archive_kind = kind.to_string(),
        archive_name = plan.file_name(),
        start_slot = plan.start_slot,
        end_slot = plan.end_slot,
        "archive task is ready to run"
    );

    let validation = client
        .validate_archive_range(
            &tables,
            &config.solana_rpc_url,
            plan.start_slot,
            plan.end_slot,
        )
        .await?;
    debug!(
        archive_kind = kind.to_string(),
        missing_blocks = validation.missing_blocks.len(),
        transaction_mismatches = validation.transaction_mismatches.len(),
        rpc_check_error = validation.rpc_check_error.as_deref().unwrap_or("none"),
        "archive validation completed"
    );
    log_validation_gaps(kind, &validation);
    if validation.has_warnings() && !config.force_archive {
        if config.server_mode {
            let mut report = ArchiveRunReport::skipped(
                kind,
                destination.describe(),
                "validation warnings require --force-archive in server mode",
            );
            report.db_bounds = Some(bounds);
            report.validation = Some(validation);
            return Ok(report);
        }
        if !confirm_validation_warnings(&plan, &validation)? {
            let mut report = ArchiveRunReport::skipped(
                kind,
                destination.describe(),
                "user declined archive after validation warnings",
            );
            report.db_bounds = Some(bounds);
            report.validation = Some(validation);
            return Ok(report);
        }
    }

    let archive_name = plan.file_name();
    info!(
        archive_kind = kind.to_string(),
        archive_name,
        start_slot = plan.start_slot,
        end_slot = plan.end_slot,
        "creating archive"
    );
    match &destination {
        ArchiveDestination::Local { directory } => {
            let path = directory.join(&archive_name);
            client
                .stream_local_parquet(
                    &tables.transactions_table,
                    plan.start_slot,
                    plan.end_slot,
                    &path,
                )
                .await?;
        }
        ArchiveDestination::S3 { prefix, .. } => {
            let s3 = config
                .s3
                .as_ref()
                .ok_or_else(|| anyhow!("S3 destination requested without S3 config"))?;
            let sql = crate::clickhouse::build_s3_archive_sql(crate::clickhouse::S3ArchiveSql {
                transactions_table: &tables.transactions_table,
                start_slot: plan.start_slot,
                end_slot: plan.end_slot,
                endpoint: &s3.endpoint,
                bucket: &s3.bucket_name,
                bucket_path: prefix,
                archive_name: &archive_name,
                access_key: &s3.auth_key,
                secret_key: &s3.auth_secret_key,
            });
            client.execute(&sql).await?;
        }
    }

    let mut deleted_clickhouse_range = false;
    if should_delete_archived_data_range(config, kind) {
        info!(
            archive_kind = kind.to_string(),
            start_slot = plan.start_slot,
            end_slot = plan.end_slot,
            "deleting archived ClickHouse data range"
        );
        client
            .delete_archived_range(&tables, plan.start_slot, plan.end_slot)
            .await?;
        deleted_clickhouse_range = true;
    } else if config.delete_archived_data_range {
        info!(
            archive_kind = kind.to_string(),
            archive_slots = kind.slot_count(),
            max_configured_archive_slots = config
                .archive_kinds
                .iter()
                .map(|archive_kind| archive_kind.slot_count())
                .max()
                .unwrap_or(kind.slot_count()),
            "deferring ClickHouse data cleanup until largest configured archive type completes"
        );
    }

    let cleaned_archives = storage::cleanup_archives(config, kind).await?;
    let report = ArchiveRunReport {
        timestamp_unix: unix_timestamp(),
        archive_created: true,
        archive_skipped_reason: None,
        archive_name: Some(archive_name.clone()),
        archive_kind: kind,
        archive_epoch: Some(plan.epoch),
        archive_slot_start: Some(plan.start_slot),
        archive_slot_end: Some(plan.end_slot),
        db_bounds: Some(bounds),
        destination: destination.describe(),
        validation: Some(validation),
        deleted_clickhouse_range,
        cleaned_archives,
    };
    storage::write_report(config, kind, &archive_name, &report.to_text()).await?;
    info!(
        archive_kind = kind.to_string(),
        archive_name,
        cleaned_archives = report.cleaned_archives.len(),
        "archive task completed"
    );
    Ok(report)
}

fn log_validation_gaps(kind: ArchiveKind, validation: &ValidationReport) {
    info!(
        archive_kind = kind.to_string(),
        missing_block_gap_count = validation.missing_block_ranges.len(),
        missing_block_ranges = ValidationReport::format_ranges(&validation.missing_block_ranges),
        not_produced_gap_count = validation.not_produced_slot_ranges.len(),
        not_produced_slot_ranges =
            ValidationReport::format_ranges(&validation.not_produced_slot_ranges),
        transaction_mismatch_gap_count = validation.transaction_mismatch_ranges.len(),
        transaction_mismatch_ranges =
            ValidationReport::format_ranges(&validation.transaction_mismatch_ranges),
        rpc_check_error = validation.rpc_check_error.as_deref().unwrap_or("none"),
        "archive validation gap summary"
    );
}

fn confirm_validation_warnings(plan: &ArchivePlan, validation: &ValidationReport) -> Result<bool> {
    eprintln!(
        "WARNING: validation found issues for {} (missing_blocks={}, transaction_mismatches={}, rpc_check_error={}).",
        plan.file_name(),
        validation.missing_blocks.len(),
        validation.transaction_mismatches.len(),
        validation.rpc_check_error.as_deref().unwrap_or("none")
    );

    if !io::stdin().is_terminal() {
        eprintln!("stdin is not interactive; rerun with --force-archive to archive anyway.");
        return Ok(false);
    }

    eprint!("Create the archive anyway? Type 'yes' to continue: ");
    io::stderr().flush()?;
    let mut answer = String::new();
    io::stdin().read_line(&mut answer)?;
    Ok(answer.trim().eq_ignore_ascii_case("yes"))
}

fn align_up(value: u64, width: u64) -> u64 {
    if value.is_multiple_of(width) {
        value
    } else {
        (value / width + 1) * width
    }
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
}

pub fn report_path_for_local_archive(directory: PathBuf, archive_name: &str) -> PathBuf {
    directory.join(format!(".{archive_name}.report.txt"))
}
