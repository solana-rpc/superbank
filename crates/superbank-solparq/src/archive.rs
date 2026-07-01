use std::{
    collections::HashSet,
    fmt,
    io::{self, IsTerminal, Write},
    path::{Path, PathBuf},
    str::FromStr,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio::fs;
use tracing::{debug, info};

use crate::{
    clickhouse::{ArchiveDbTable, ArchiveTableKind, ClickHouseClient, DbTables, ValidationReport},
    config::Config,
    manifest::{ArchiveManifest, ArchiveManifestTable, MANIFEST_FILE_NAME, SkippedArchiveTable},
    storage::{self, ArchiveDestination},
};

pub const HOURLY_SLOTS: u64 = 9_000;
pub const EPOCH_SLOTS: u64 = 432_000;
pub const DEFAULT_CUSTOM_SLOTS: u64 = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ArchiveSlotRange {
    pub start_slot: u64,
    pub end_slot: u64,
}

impl ArchiveSlotRange {
    pub fn new(start_slot: u64, end_slot: u64) -> Result<Self> {
        if start_slot > end_slot {
            return Err(anyhow!(
                "archive slot range start must be less than or equal to end"
            ));
        }
        Ok(Self {
            start_slot,
            end_slot,
        })
    }
}

impl FromStr for ArchiveSlotRange {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        let (start, end) = value
            .trim()
            .split_once('-')
            .ok_or_else(|| "archive slot range must use START-END format".to_string())?;
        let start_slot = start
            .trim()
            .parse::<u64>()
            .map_err(|_| format!("invalid archive slot range start in '{value}'"))?;
        let end_slot = end
            .trim()
            .parse::<u64>()
            .map_err(|_| format!("invalid archive slot range end in '{value}'"))?;
        Self::new(start_slot, end_slot).map_err(|err| err.to_string())
    }
}

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
    pub fn archive_id(&self) -> String {
        format!(
            "{}_{}_{}-{}",
            self.kind.label(),
            self.epoch,
            self.start_slot,
            self.end_slot
        )
    }

    pub fn file_name(&self) -> String {
        format!("{}.parquet", self.archive_id())
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
    let stem = file_name.strip_suffix(".parquet").unwrap_or(file_name);
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

pub fn plan_archive_slot_range(
    kind: ArchiveKind,
    bounds: ClickHouseBounds,
    slot_range: ArchiveSlotRange,
) -> Result<Option<ArchivePlan>> {
    if bounds.latest_slot < bounds.earliest_slot {
        return Ok(None);
    }
    if slot_range.start_slot > slot_range.end_slot {
        return Err(anyhow!(
            "archive slot range start must be less than or equal to end"
        ));
    }
    if slot_range.start_slot < bounds.earliest_slot || slot_range.end_slot > bounds.latest_slot {
        return Ok(None);
    }

    Ok(Some(ArchivePlan {
        kind,
        epoch: slot_range.start_slot / EPOCH_SLOTS,
        start_slot: slot_range.start_slot,
        end_slot: slot_range.end_slot,
    }))
}

pub fn safe_delete_archived_data_range(
    config: &Config,
    current_start_slot: u64,
    current_end_slot: u64,
    latest_archive_names: &[(ArchiveKind, Option<String>)],
) -> Result<Option<crate::clickhouse::SlotRange>> {
    if !config.delete_archived_data_range {
        return Ok(None);
    }

    let mut safe_end_slot = current_end_slot;
    for kind in config.archive_kinds.iter().copied() {
        let Some(latest_archive_name) = latest_archive_names
            .iter()
            .find(|(latest_kind, _)| *latest_kind == kind)
            .and_then(|(_, latest_archive_name)| latest_archive_name.as_deref())
        else {
            return Ok(None);
        };
        let parsed = parse_archive_name(latest_archive_name).ok_or_else(|| {
            anyhow!("unable to parse latest archive name '{latest_archive_name}'")
        })?;
        if parsed.kind_label != kind.label() {
            return Err(anyhow!(
                "latest archive name '{latest_archive_name}' does not match archive type '{}'",
                kind.label()
            ));
        }
        safe_end_slot = safe_end_slot.min(parsed.end_slot);
    }

    if safe_end_slot < current_start_slot {
        return Ok(None);
    }

    Ok(Some(crate::clickhouse::SlotRange::new(
        current_start_slot,
        safe_end_slot,
    )))
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
    pub archive_tables: Vec<ArchiveRunTable>,
    pub validation: Option<ValidationReport>,
    pub deleted_clickhouse_range: bool,
    pub cleaned_archives: Vec<String>,
    /// Operational measurements captured during the run, surfaced as Prometheus
    /// metrics by [`crate::metrics::AppState::record_report`]. Empty/`None` for
    /// runs that skip archive creation.
    pub run_metrics: ArchiveRunMetrics,
}

/// Per-run operational measurements consumed by the metrics layer.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ArchiveRunMetrics {
    /// Wall-clock duration of individual archive phases (validate, write, …).
    pub phase_durations: Vec<PhaseDuration>,
    /// Row counts written per archive table.
    pub archived_table_rows: Vec<ArchivedTableRows>,
    /// Total bytes written for the archive bundle. Only populated for local
    /// destinations; ClickHouse-driven S3 exports do not report bytes written.
    pub archived_bytes_total: Option<u64>,
    /// Current Solana network tip (finalized) observed for this run, if the RPC
    /// probe succeeded. Enables the chain-tip lag metric.
    pub chain_tip_slot: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PhaseDuration {
    pub phase: String,
    pub seconds: f64,
}

impl PhaseDuration {
    fn new(phase: impl Into<String>, elapsed: Duration) -> Self {
        Self {
            phase: phase.into(),
            seconds: elapsed.as_secs_f64(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArchivedTableRows {
    pub table: String,
    pub rows: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArchiveRunTable {
    pub kind: ArchiveTableKind,
    pub table_name: String,
    pub required: bool,
}

impl ArchiveRunTable {
    fn from_db_table(table: &ArchiveDbTable) -> Self {
        Self {
            kind: table.kind,
            table_name: table.table_name.clone(),
            required: table.required,
        }
    }
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
            archive_tables: Vec::new(),
            validation: None,
            deleted_clickhouse_range: false,
            cleaned_archives: Vec::new(),
            run_metrics: ArchiveRunMetrics::default(),
        }
    }

    pub fn to_text(&self) -> String {
        let mut lines = Vec::new();
        lines.push(format!("timestamp_unix: {}", self.timestamp_unix));
        lines.push(format!(
            "timestamp_utc: {}",
            format_unix_timestamp_utc(self.timestamp_unix)
        ));
        lines.push(format!("hostname: {}", node_hostname()));
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
        if !self.archive_tables.is_empty() {
            lines.push(format!(
                "archive_tables: {}",
                self.archive_tables
                    .iter()
                    .map(|table| format!(
                        "{}:{}:{}",
                        table.kind.as_str(),
                        table.table_name,
                        if table.required {
                            "required"
                        } else {
                            "optional"
                        }
                    ))
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }
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
        if !self.run_metrics.archived_table_rows.is_empty() {
            lines.push(format!(
                "archived_rows: {}",
                self.run_metrics
                    .archived_table_rows
                    .iter()
                    .map(|entry| format!("{}:{}", entry.table, entry.rows))
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }
        if let Some(bytes) = self.run_metrics.archived_bytes_total {
            lines.push(format!("archived_bytes_total: {bytes}"));
        }
        if !self.run_metrics.phase_durations.is_empty() {
            lines.push(format!(
                "phase_durations_seconds: {}",
                self.run_metrics
                    .phase_durations
                    .iter()
                    .map(|entry| format!("{}:{:.3}", entry.phase, entry.seconds))
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }
        if let Some(chain_tip_slot) = self.run_metrics.chain_tip_slot {
            lines.push(format!("chain_tip_slot: {chain_tip_slot}"));
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
    // Best-effort network tip probe up front so every report (created or
    // skipped) can expose chain-tip lag. A failing probe leaves the tip
    // unknown rather than aborting the archive run.
    let chain_tip_slot =
        match crate::clickhouse::fetch_solana_tip_slot(&config.solana_rpc_url).await {
            Ok(slot) => Some(slot),
            Err(err) => {
                debug!(
                    archive_kind = kind.to_string(),
                    %err, "failed to probe Solana network tip"
                );
                None
            }
        };
    let started = Instant::now();
    let mut report = run_once_for_kind_inner(config, kind).await?;
    report.run_metrics.chain_tip_slot = chain_tip_slot;
    report
        .run_metrics
        .phase_durations
        .push(PhaseDuration::new("total", started.elapsed()));
    Ok(report)
}

async fn run_once_for_kind_inner(config: &Config, kind: ArchiveKind) -> Result<ArchiveRunReport> {
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
    let available_archive_tables = client.check_tables(&tables).await?;
    let archive_tables = available_archive_tables
        .iter()
        .map(ArchiveRunTable::from_db_table)
        .collect::<Vec<_>>();
    let skipped_archive_tables = skipped_archive_tables(&tables, &available_archive_tables);
    info!(
        archive_kind = kind.to_string(),
        archive_tables = available_archive_tables.len(),
        skipped_optional_archive_tables = skipped_archive_tables.len(),
        "resolved DB archive table set"
    );

    let Some(bounds) = client.fetch_bounds(&tables.transactions_table).await? else {
        let mut report = ArchiveRunReport::skipped(
            kind,
            destination.describe(),
            "no transactions found in ClickHouse",
        );
        report.archive_tables = archive_tables;
        return Ok(report);
    };
    info!(
        archive_kind = kind.to_string(),
        earliest_slot = bounds.earliest_slot,
        latest_slot = bounds.latest_slot,
        slots_available = bounds.slots_available(),
        "ClickHouse archive source has slots available"
    );

    let (plan, skipped_reason) = if let Some(slot_range) = config.archive_slot_range {
        info!(
            archive_kind = kind.to_string(),
            start_slot = slot_range.start_slot,
            end_slot = slot_range.end_slot,
            "using explicit one-shot archive slot range"
        );
        (
            plan_archive_slot_range(kind, bounds, slot_range)?,
            "requested archive slot range is not fully available in ClickHouse",
        )
    } else {
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
        (
            plan_next_archive(
                kind,
                bounds,
                last_archive.as_deref(),
                config.continue_from_last_archive,
            )?,
            "not enough ClickHouse slots available for the next archive",
        )
    };
    let Some(plan) = plan else {
        let mut report = ArchiveRunReport::skipped(kind, destination.describe(), skipped_reason);
        report.db_bounds = Some(bounds);
        report.archive_tables = archive_tables;
        return Ok(report);
    };
    let archive_name = plan.archive_id();
    info!(
        archive_kind = kind.to_string(),
        archive_name = plan.file_name(),
        start_slot = plan.start_slot,
        end_slot = plan.end_slot,
        "archive task is ready to run"
    );

    if storage::archive_has_done_marker(config, kind, &archive_name).await? {
        info!(
            archive_kind = kind.to_string(),
            archive_name, "archive already has done marker; treating as successful"
        );
        let deleted_clickhouse_range = maybe_delete_archived_data_range(
            config,
            kind,
            &client,
            &available_archive_tables,
            &plan,
        )
        .await?;
        let cleaned_archives = storage::cleanup_archives(config, kind).await?;
        let report = ArchiveRunReport {
            timestamp_unix: unix_timestamp(),
            archive_created: true,
            archive_skipped_reason: Some("archive already has done marker".to_string()),
            archive_name: Some(archive_name.clone()),
            archive_kind: kind,
            archive_epoch: Some(plan.epoch),
            archive_slot_start: Some(plan.start_slot),
            archive_slot_end: Some(plan.end_slot),
            db_bounds: Some(bounds),
            destination: destination.describe(),
            archive_tables,
            validation: None,
            deleted_clickhouse_range,
            cleaned_archives,
            run_metrics: ArchiveRunMetrics::default(),
        };
        storage::write_report(config, kind, &archive_name, &report.to_text()).await?;
        return Ok(report);
    }
    let mut phase_durations = Vec::new();
    let validate_started = Instant::now();
    let validation = client
        .validate_archive_range(
            &tables,
            &config.solana_rpc_url,
            plan.start_slot,
            plan.end_slot,
        )
        .await?;
    phase_durations.push(PhaseDuration::new("validate", validate_started.elapsed()));
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
            report.archive_tables = archive_tables;
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
            report.archive_tables = archive_tables;
            report.validation = Some(validation);
            return Ok(report);
        }
    }

    if storage::remove_archive_bundle(config, kind, &archive_name).await? {
        info!(
            archive_kind = kind.to_string(),
            archive_name, "removed existing archive bundle without done marker before recreating"
        );
    }

    info!(
        archive_kind = kind.to_string(),
        archive_name,
        start_slot = plan.start_slot,
        end_slot = plan.end_slot,
        "creating archive"
    );
    let mut manifest_tables = Vec::new();
    let mut archived_bytes_total: Option<u64> = None;
    let write_started = Instant::now();
    match &destination {
        ArchiveDestination::Local { directory } => {
            let staging_dir = storage::local_staging_bundle_dir(directory, &archive_name);
            if staging_dir.exists() {
                fs::remove_dir_all(&staging_dir).await?;
            }
            fs::create_dir_all(&staging_dir).await?;
            for table in &available_archive_tables {
                let path = staging_dir.join(table.file_name());
                info!(
                    archive_kind = kind.to_string(),
                    archive_name,
                    archive_table = table.kind.as_str(),
                    table_name = table.table_name.as_str(),
                    path = path.display().to_string(),
                    "creating archive table parquet"
                );
                client
                    .stream_local_table_parquet(table, plan.start_slot, plan.end_slot, &path)
                    .await?;
                let row_count = client
                    .count_table_rows(table, plan.start_slot, plan.end_slot)
                    .await?;
                manifest_tables.push(ArchiveManifestTable::from_table(table, row_count));
            }
            let checksum_file_names = manifest_tables
                .iter()
                .map(|table| table.file_name.clone())
                .collect::<Vec<_>>();
            storage::write_local_sha256sums(&staging_dir, &checksum_file_names).await?;
            let manifest = ArchiveManifest::new(
                archive_name.clone(),
                kind,
                plan.epoch,
                plan.start_slot,
                plan.end_slot,
                manifest_tables.clone(),
                skipped_archive_tables.clone(),
            );
            let manifest_json = serde_json::to_vec_pretty(&manifest).map_err(|err| anyhow!(err))?;
            fs::write(staging_dir.join(MANIFEST_FILE_NAME), manifest_json).await?;
            archived_bytes_total = Some(staging_bundle_bytes(&staging_dir).await);
            fs::rename(
                &staging_dir,
                storage::local_bundle_dir(directory, &archive_name),
            )
            .await?;
        }
        ArchiveDestination::S3 { prefix, .. } => {
            let s3 = config
                .s3
                .as_ref()
                .ok_or_else(|| anyhow!("S3 destination requested without S3 config"))?;
            for table in &available_archive_tables {
                info!(
                    archive_kind = kind.to_string(),
                    archive_name,
                    archive_table = table.kind.as_str(),
                    table_name = table.table_name.as_str(),
                    "creating S3 archive table parquet"
                );
                let sql =
                    crate::clickhouse::build_s3_archive_sql(crate::clickhouse::S3ArchiveSql {
                        table,
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
                let row_count = client
                    .count_table_rows(table, plan.start_slot, plan.end_slot)
                    .await?;
                manifest_tables.push(ArchiveManifestTable::from_table(table, row_count));
            }
            let checksum_file_names = manifest_tables
                .iter()
                .map(|table| table.file_name.clone())
                .collect::<Vec<_>>();
            storage::write_s3_sha256sums(config, kind, &archive_name, &checksum_file_names).await?;
            let manifest = ArchiveManifest::new(
                archive_name.clone(),
                kind,
                plan.epoch,
                plan.start_slot,
                plan.end_slot,
                manifest_tables.clone(),
                skipped_archive_tables.clone(),
            );
            storage::write_manifest(config, kind, &archive_name, &manifest).await?;
        }
    }
    phase_durations.push(PhaseDuration::new("write", write_started.elapsed()));

    let report_timestamp = unix_timestamp();
    storage::write_done_marker(
        config,
        kind,
        &archive_name,
        &node_hostname(),
        &format_unix_timestamp_utc(report_timestamp),
    )
    .await?;

    let delete_started = Instant::now();
    let deleted_clickhouse_range =
        maybe_delete_archived_data_range(config, kind, &client, &available_archive_tables, &plan)
            .await?;
    phase_durations.push(PhaseDuration::new("delete_range", delete_started.elapsed()));

    let cleanup_started = Instant::now();
    let cleaned_archives = storage::cleanup_archives(config, kind).await?;
    phase_durations.push(PhaseDuration::new("cleanup", cleanup_started.elapsed()));

    let archived_table_rows = manifest_tables
        .iter()
        .map(|table| ArchivedTableRows {
            table: table.table_name.clone(),
            rows: table.row_count,
        })
        .collect::<Vec<_>>();
    let report = ArchiveRunReport {
        timestamp_unix: report_timestamp,
        archive_created: true,
        archive_skipped_reason: None,
        archive_name: Some(archive_name.clone()),
        archive_kind: kind,
        archive_epoch: Some(plan.epoch),
        archive_slot_start: Some(plan.start_slot),
        archive_slot_end: Some(plan.end_slot),
        db_bounds: Some(bounds),
        destination: destination.describe(),
        archive_tables,
        validation: Some(validation),
        deleted_clickhouse_range,
        cleaned_archives,
        run_metrics: ArchiveRunMetrics {
            phase_durations,
            archived_table_rows,
            archived_bytes_total,
            chain_tip_slot: None,
        },
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

async fn maybe_delete_archived_data_range(
    config: &Config,
    kind: ArchiveKind,
    client: &ClickHouseClient,
    available_archive_tables: &[ArchiveDbTable],
    plan: &ArchivePlan,
) -> Result<bool> {
    if !config.delete_archived_data_range {
        return Ok(false);
    }

    let latest_archive_names = storage::latest_archive_names(config).await?;
    let safe_delete_range = safe_delete_archived_data_range(
        config,
        plan.start_slot,
        plan.end_slot,
        &latest_archive_names,
    )?;
    if let Some(range) = safe_delete_range {
        info!(
            archive_kind = kind.to_string(),
            start_slot = range.start_slot,
            end_slot = range.end_slot,
            current_archive_start_slot = plan.start_slot,
            current_archive_end_slot = plan.end_slot,
            "deleting archived ClickHouse data range covered by all configured archive types"
        );
        client
            .delete_archived_range_for_tables(
                available_archive_tables,
                range.start_slot,
                range.end_slot,
            )
            .await?;
        Ok(true)
    } else {
        info!(
            archive_kind = kind.to_string(),
            start_slot = plan.start_slot,
            end_slot = plan.end_slot,
            "deferring ClickHouse data cleanup until all configured archive types cover this range"
        );
        Ok(false)
    }
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

fn skipped_archive_tables(
    tables: &DbTables,
    available_archive_tables: &[ArchiveDbTable],
) -> Vec<SkippedArchiveTable> {
    let available = available_archive_tables
        .iter()
        .map(|table| (table.kind, table.table_name.as_str()))
        .collect::<HashSet<_>>();
    tables
        .archive_tables()
        .into_iter()
        .filter(|table| !available.contains(&(table.kind, table.table_name.as_str())))
        .map(|table| SkippedArchiveTable::unavailable(&table))
        .collect()
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

/// Sum the byte sizes of the files in a staged local archive bundle.
///
/// Best-effort: unreadable entries are skipped so byte accounting never fails
/// an otherwise-successful archive.
async fn staging_bundle_bytes(staging_dir: &Path) -> u64 {
    let mut total = 0u64;
    let Ok(mut entries) = fs::read_dir(staging_dir).await else {
        return 0;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        if let Ok(metadata) = entry.metadata().await
            && metadata.is_file()
        {
            total = total.saturating_add(metadata.len());
        }
    }
    total
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

fn format_unix_timestamp_utc(timestamp_unix: u64) -> String {
    DateTime::<Utc>::from_timestamp(timestamp_unix as i64, 0)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| "invalid timestamp".to_string())
}

fn node_hostname() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|hostname| !hostname.trim().is_empty())
        .or_else(system_hostname)
        .unwrap_or_else(|| "unknown".to_string())
}

fn system_hostname() -> Option<String> {
    let output = std::process::Command::new("hostname").output().ok()?;
    if !output.status.success() {
        return None;
    }
    let hostname = String::from_utf8(output.stdout).ok()?;
    let hostname = hostname.trim();
    if hostname.is_empty() {
        None
    } else {
        Some(hostname.to_string())
    }
}

pub fn report_path_for_local_archive(directory: PathBuf, archive_name: &str) -> PathBuf {
    directory.join(format!(".{archive_name}.report.txt"))
}
