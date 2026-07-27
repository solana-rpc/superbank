use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use prometheus_client::{
    encoding::text::encode,
    metrics::{counter::Counter, family::Family, gauge::Gauge, histogram::Histogram},
    registry::Registry,
};
use prometheus_client_derive_encode::EncodeLabelSet;
use serde::Serialize;
use tracing::warn;

use crate::{
    archive::{ArchiveKind, ArchiveRunReport, ClickHouseBounds},
    clickhouse::{DiskUsage, MismatchDirection, SlotRange, TableSize},
};

const MAX_RECENT_EVENTS: usize = 60;

/// Duration buckets for archive phase timings. Archive phases range from
/// sub-second (cleanup) to tens of minutes (writing an epoch of Parquet), so
/// the buckets span 0.1s to 1h.
const PHASE_BUCKETS: [f64; 12] = [
    0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1_800.0, 3_600.0,
];

/// Classification for slots Solana never produced (leader-skipped). Expected,
/// not an archiving problem — the ops dashboard filters these out of the gaps
/// table. Kept as a shared const so the recorder and the dashboard agree.
pub const LEGIT_NOT_PRODUCED_CLASSIFICATION: &str = "Legit not-produced";

/// Data-gap classifications surfaced as `solparq_known_gaps` label values.
/// Kept in sync with the classifications produced by [`AppState::record_known_gaps`].
const GAP_CLASSIFICATIONS: [&str; 4] = [
    "Needs backfill",
    LEGIT_NOT_PRODUCED_CLASSIFICATION,
    "Transaction mismatch (undercount)",
    "Transaction mismatch (overcount)",
];

fn phase_histogram() -> Histogram {
    Histogram::new(PHASE_BUCKETS)
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct KindLabels {
    archive_kind: String,
}

impl KindLabels {
    fn new(label: &str) -> Self {
        Self {
            archive_kind: label.to_string(),
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct SkipLabels {
    archive_kind: String,
    reason: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct TableLabels {
    archive_kind: String,
    table: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct PhaseLabels {
    archive_kind: String,
    phase: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct GapLabels {
    archive_kind: String,
    classification: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ValidationLabels {
    archive_kind: String,
    category: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct MismatchLabels {
    archive_kind: String,
    direction: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct RepairLabels {
    archive_kind: String,
    outcome: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct DiskLabels {
    disk: String,
    path: String,
}

impl DiskLabels {
    fn new(disk: &DiskUsage) -> Self {
        Self {
            disk: disk.name.clone(),
            path: disk.path.clone(),
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct DbTableLabels {
    table_kind: String,
    table: String,
}

/// Labels for the `solparq_build_info` gauge. Follows the Prometheus
/// `*_build_info` convention: a constant `1` gauge whose labels carry the build
/// identity, so a dashboard can display the running version/commit and alert on
/// unexpected version churn.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct BuildInfoLabels {
    name: String,
    version: String,
    git_sha: String,
}

impl DbTableLabels {
    fn new(size: &TableSize) -> Self {
        Self {
            table_kind: size.kind.as_str().to_string(),
            table: size.table_name.clone(),
        }
    }
}

/// Validation issue categories, kept distinct so a dashboard can separate
/// actionable gaps from expected leader gaps and other data problems.
#[derive(Clone, Copy)]
enum ValidationCategory {
    /// Solana produced the block but it is missing from ClickHouse — needs backfill.
    MissingBlock,
    /// Slot was not produced on-chain (expected leader gap) — informational.
    NotProduced,
    /// Block transaction count does not match the archived transaction rows.
    TransactionMismatch,
}

impl ValidationCategory {
    fn as_str(self) -> &'static str {
        match self {
            ValidationCategory::MissingBlock => "missing_block",
            ValidationCategory::NotProduced => "not_produced",
            ValidationCategory::TransactionMismatch => "transaction_mismatch",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PublicStatus {
    pub build: crate::manifest::ManifestProducer,
    pub started_at_unix: u64,
    pub last_run_at_unix: Option<u64>,
    pub last_success_at_unix: Option<u64>,
    pub healthy: bool,
    pub last_error: Option<String>,
    pub last_error_at_unix: Option<u64>,
    pub last_report: Option<ArchiveRunReport>,
    pub db_slots: Option<DbSlotStatus>,
    pub disk_usage: Vec<DiskUsage>,
    pub table_sizes: Vec<TableSize>,
    pub recent_events: Vec<ArchiveEvent>,
    pub known_gaps: Vec<KnownDataGap>,
    pub gap_repairs: Vec<GapRepairEvent>,
    pub archives_created: u64,
    pub archives_skipped: u64,
    pub archive_errors: u64,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct DbSlotStatus {
    pub earliest_slot: u64,
    pub latest_slot: u64,
    pub slots_available: u64,
}

impl From<ClickHouseBounds> for DbSlotStatus {
    fn from(bounds: ClickHouseBounds) -> Self {
        Self {
            earliest_slot: bounds.earliest_slot,
            latest_slot: bounds.latest_slot,
            slots_available: bounds.slots_available(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ArchiveEvent {
    pub timestamp_unix: u64,
    pub archive_kind: String,
    pub outcome: String,
    pub archive_name: Option<String>,
    pub reason: Option<String>,
    pub skip_reason_code: Option<String>,
    pub start_slot: Option<u64>,
    pub end_slot: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct KnownDataGap {
    pub timestamp_unix: u64,
    pub archive_kind: String,
    pub classification: String,
    pub start_slot: u64,
    pub end_slot: u64,
    pub slot_count: u64,
    pub detail: String,
}

/// A gap-repair attempt (RPC backfill or dedup) recorded from a run report, for
/// the ops dashboard's repaired-gaps section.
#[derive(Debug, Clone, Serialize)]
pub struct GapRepairEvent {
    pub timestamp_unix: u64,
    pub archive_kind: String,
    /// Human label for the repair mechanism, e.g. "RPC backfill" or "Dedup".
    pub kind: String,
    pub succeeded: bool,
    pub start_slot: Option<u64>,
    pub end_slot: Option<u64>,
    pub detail: String,
}

/// Prometheus metric handles for the solparq archiver, exposed on `/metrics`
/// (default `SOLPARQ_METRICS_PORT`, 31313).
///
/// Follows the same `prometheus_client` pattern as the `superbank` and
/// `superbank-rpc` crates: a single [`Registry`] plus typed handles, all
/// interior-mutable so they can be updated behind a shared `&self`. Per-archive
/// series carry an `archive_kind` label (`hourly`/`epoch`/`custom`).
struct Metrics {
    registry: Registry,
    // Process-global gauges.
    health: Gauge,
    check_interval_seconds: Gauge,
    db_earliest_slot: Gauge,
    db_latest_slot: Gauge,
    db_slots_available: Gauge,
    chain_tip_slot: Gauge,
    chain_tip_lag_slots: Gauge,
    // Per-archive-kind gauges.
    last_run_at_unix: Family<KindLabels, Gauge>,
    last_success_at_unix: Family<KindLabels, Gauge>,
    last_archived_start_slot: Family<KindLabels, Gauge>,
    last_archived_end_slot: Family<KindLabels, Gauge>,
    last_archived_epoch: Family<KindLabels, Gauge>,
    last_archive_rows: Family<KindLabels, Gauge>,
    last_archive_bytes: Family<KindLabels, Gauge>,
    db_lag_slots: Family<KindLabels, Gauge>,
    archive_in_flight: Family<KindLabels, Gauge>,
    validation_slots: Family<ValidationLabels, Gauge>,
    validation_ranges: Family<ValidationLabels, Gauge>,
    validation_mismatch_slots: Family<MismatchLabels, Gauge>,
    validation_range_start_slot: Family<KindLabels, Gauge>,
    validation_range_end_slot: Family<KindLabels, Gauge>,
    validation_db_block_slots: Family<KindLabels, Gauge>,
    validation_rpc_produced_slots: Family<KindLabels, Gauge>,
    known_gaps: Family<GapLabels, Gauge>,
    disk_free_bytes: Family<DiskLabels, Gauge>,
    disk_used_bytes: Family<DiskLabels, Gauge>,
    disk_total_bytes: Family<DiskLabels, Gauge>,
    db_table_bytes: Family<DbTableLabels, Gauge>,
    db_table_rows: Family<DbTableLabels, Gauge>,
    // Counters.
    archives_created_total: Family<KindLabels, Counter>,
    archives_skipped_total: Family<SkipLabels, Counter>,
    archive_errors_total: Family<KindLabels, Counter>,
    archives_cleaned_total: Family<KindLabels, Counter>,
    clickhouse_range_deleted_total: Family<KindLabels, Counter>,
    validation_rpc_errors_total: Family<KindLabels, Counter>,
    validation_slots_total: Family<ValidationLabels, Counter>,
    mismatch_repairs_total: Family<RepairLabels, Counter>,
    gap_backfills_total: Family<RepairLabels, Counter>,
    archive_rows_total: Family<TableLabels, Counter>,
    // Histograms.
    phase_duration_seconds: Family<PhaseLabels, Histogram>,
    // Cached inputs for the derived chain-tip lag gauge.
    last_db_latest_slot: AtomicU64,
    last_chain_tip_slot: AtomicU64,
}

impl Metrics {
    fn new(started_at_unix: u64) -> Self {
        let started_at: Gauge = Gauge::default();
        started_at.set(clamp_i64(started_at_unix));
        let build_info = Family::<BuildInfoLabels, Gauge>::default();
        let producer = crate::manifest::ManifestProducer::current();
        build_info
            .get_or_create(&BuildInfoLabels {
                name: producer.name,
                version: producer.version,
                git_sha: producer.git_sha,
            })
            .set(1);
        let health = Gauge::default();
        health.set(1);
        let check_interval_seconds = Gauge::default();
        let db_earliest_slot = Gauge::default();
        let db_latest_slot = Gauge::default();
        let db_slots_available = Gauge::default();
        let chain_tip_slot = Gauge::default();
        let chain_tip_lag_slots = Gauge::default();
        let last_run_at_unix = Family::<KindLabels, Gauge>::default();
        let last_success_at_unix = Family::<KindLabels, Gauge>::default();
        let last_archived_start_slot = Family::<KindLabels, Gauge>::default();
        let last_archived_end_slot = Family::<KindLabels, Gauge>::default();
        let last_archived_epoch = Family::<KindLabels, Gauge>::default();
        let last_archive_rows = Family::<KindLabels, Gauge>::default();
        let last_archive_bytes = Family::<KindLabels, Gauge>::default();
        let db_lag_slots = Family::<KindLabels, Gauge>::default();
        let archive_in_flight = Family::<KindLabels, Gauge>::default();
        let validation_slots = Family::<ValidationLabels, Gauge>::default();
        let validation_ranges = Family::<ValidationLabels, Gauge>::default();
        let validation_mismatch_slots = Family::<MismatchLabels, Gauge>::default();
        let validation_range_start_slot = Family::<KindLabels, Gauge>::default();
        let validation_range_end_slot = Family::<KindLabels, Gauge>::default();
        let validation_db_block_slots = Family::<KindLabels, Gauge>::default();
        let validation_rpc_produced_slots = Family::<KindLabels, Gauge>::default();
        let known_gaps = Family::<GapLabels, Gauge>::default();
        let disk_free_bytes = Family::<DiskLabels, Gauge>::default();
        let disk_used_bytes = Family::<DiskLabels, Gauge>::default();
        let disk_total_bytes = Family::<DiskLabels, Gauge>::default();
        let db_table_bytes = Family::<DbTableLabels, Gauge>::default();
        let db_table_rows = Family::<DbTableLabels, Gauge>::default();
        let archives_created_total = Family::<KindLabels, Counter>::default();
        let archives_skipped_total = Family::<SkipLabels, Counter>::default();
        let archive_errors_total = Family::<KindLabels, Counter>::default();
        let archives_cleaned_total = Family::<KindLabels, Counter>::default();
        let clickhouse_range_deleted_total = Family::<KindLabels, Counter>::default();
        let validation_rpc_errors_total = Family::<KindLabels, Counter>::default();
        let validation_slots_total = Family::<ValidationLabels, Counter>::default();
        let mismatch_repairs_total = Family::<RepairLabels, Counter>::default();
        let gap_backfills_total = Family::<RepairLabels, Counter>::default();
        let archive_rows_total = Family::<TableLabels, Counter>::default();
        let phase_duration_seconds = Family::<PhaseLabels, Histogram>::new_with_constructor(
            phase_histogram as fn() -> Histogram,
        );

        let mut registry = Registry::with_prefix("solparq");
        registry.register(
            "build_info",
            "Build identity of the running binary (constant 1; see name/version/git_sha labels)",
            build_info.clone(),
        );
        registry.register(
            "started_at_unix",
            "Process start time as a Unix timestamp",
            started_at.clone(),
        );
        registry.register(
            "health",
            "1 when the last archive loop completed without error, 0 otherwise",
            health.clone(),
        );
        registry.register(
            "check_interval_seconds",
            "Configured interval between archive planning checks",
            check_interval_seconds.clone(),
        );
        registry.register(
            "db_earliest_slot",
            "Earliest transaction slot visible in ClickHouse",
            db_earliest_slot.clone(),
        );
        registry.register(
            "db_latest_slot",
            "Latest transaction slot visible in ClickHouse",
            db_latest_slot.clone(),
        );
        registry.register(
            "db_slots_available",
            "Number of distinct transaction slots visible in ClickHouse",
            db_slots_available.clone(),
        );
        registry.register(
            "chain_tip_slot",
            "Latest Solana network slot (finalized) observed via getSlot",
            chain_tip_slot.clone(),
        );
        registry.register(
            "chain_tip_lag_slots",
            "Slots between the Solana network tip and the latest slot in ClickHouse",
            chain_tip_lag_slots.clone(),
        );
        registry.register(
            "last_run_at_unix",
            "Unix timestamp of the last archive check by archive kind",
            last_run_at_unix.clone(),
        );
        registry.register(
            "last_success_at_unix",
            "Unix timestamp of the last successful archive by archive kind",
            last_success_at_unix.clone(),
        );
        registry.register(
            "last_archived_start_slot",
            "Start slot of the most recently created archive by archive kind",
            last_archived_start_slot.clone(),
        );
        registry.register(
            "last_archived_end_slot",
            "End slot of the most recently created archive by archive kind",
            last_archived_end_slot.clone(),
        );
        registry.register(
            "last_archived_epoch",
            "Epoch of the most recently created archive by archive kind",
            last_archived_epoch.clone(),
        );
        registry.register(
            "last_archive_rows",
            "Total rows written in the most recent archive by archive kind",
            last_archive_rows.clone(),
        );
        registry.register(
            "last_archive_bytes",
            "Total bytes written in the most recent archive by archive kind (local destinations only)",
            last_archive_bytes.clone(),
        );
        registry.register(
            "db_lag_slots",
            "Slots between the latest ClickHouse slot and the latest archived slot by archive kind",
            db_lag_slots.clone(),
        );
        registry.register(
            "archive_in_flight",
            "1 while an archive task is running for the archive kind, 0 otherwise",
            archive_in_flight.clone(),
        );
        registry.register(
            "validation_slots",
            "Slots flagged in the last validated range by archive kind and category (missing_block = needs backfill, not_produced = expected leader gap, transaction_mismatch = count mismatch)",
            validation_slots.clone(),
        );
        registry.register(
            "validation_ranges",
            "Contiguous slot ranges flagged in the last validated range by archive kind and category",
            validation_ranges.clone(),
        );
        registry.register(
            "validation_mismatch_slots",
            "Transaction-count mismatch slots in the last validated range by archive kind and direction (undercount = missing rows, overcount = duplicate rows)",
            validation_mismatch_slots.clone(),
        );
        registry.register(
            "validation_range_start_slot",
            "Start slot of the last validated range by archive kind",
            validation_range_start_slot.clone(),
        );
        registry.register(
            "validation_range_end_slot",
            "End slot of the last validated range by archive kind",
            validation_range_end_slot.clone(),
        );
        registry.register(
            "validation_db_block_slots",
            "Block slots present in ClickHouse for the last validated range by archive kind",
            validation_db_block_slots.clone(),
        );
        registry.register(
            "validation_rpc_produced_slots",
            "Slots the Solana RPC reports as produced for the last validated range by archive kind",
            validation_rpc_produced_slots.clone(),
        );
        registry.register(
            "known_gaps",
            "Currently tracked known data gaps by archive kind and classification",
            known_gaps.clone(),
        );
        registry.register(
            "disk_free_bytes",
            "Free space on a ClickHouse-managed disk, from system.disks",
            disk_free_bytes.clone(),
        );
        registry.register(
            "disk_used_bytes",
            "Used space on a ClickHouse-managed disk, from system.disks",
            disk_used_bytes.clone(),
        );
        registry.register(
            "disk_total_bytes",
            "Total space on a ClickHouse-managed disk, from system.disks",
            disk_total_bytes.clone(),
        );
        registry.register(
            "db_table_bytes",
            "Disk bytes used by a ClickHouse source table, from system.tables, by table_kind and table",
            db_table_bytes.clone(),
        );
        registry.register(
            "db_table_rows",
            "Row count of a ClickHouse source table, from system.tables, by table_kind and table",
            db_table_rows.clone(),
        );
        // Counters are registered without the `_total` suffix; the OpenMetrics
        // encoder appends it, yielding e.g. `solparq_archives_created_total`.
        registry.register(
            "archives_created",
            "Archives created successfully by archive kind",
            archives_created_total.clone(),
        );
        registry.register(
            "archives_skipped",
            "Archive planning runs skipped by archive kind and skip reason",
            archives_skipped_total.clone(),
        );
        registry.register(
            "archive_errors",
            "Archive loop errors by archive kind",
            archive_errors_total.clone(),
        );
        registry.register(
            "archives_cleaned",
            "Old archive bundles pruned during retention cleanup by archive kind",
            archives_cleaned_total.clone(),
        );
        registry.register(
            "clickhouse_range_deleted",
            "Reserved compatibility metric for ClickHouse archive-range deletion by archive kind",
            clickhouse_range_deleted_total.clone(),
        );
        registry.register(
            "validation_rpc_errors",
            "Validation runs where the Solana RPC cross-check failed by archive kind",
            validation_rpc_errors_total.clone(),
        );
        registry.register(
            "validation_flagged_slots",
            "Cumulative slots flagged during validation by archive kind and category",
            validation_slots_total.clone(),
        );
        registry.register(
            "mismatch_repairs",
            "Transaction-mismatch repair attempts by archive kind and outcome (repaired = clean after dedup, still_dirty = mismatch remains)",
            mismatch_repairs_total.clone(),
        );
        registry.register(
            "gap_backfills",
            "Pre-archive RPC gap backfill attempts by archive kind and outcome (filled = no missing blocks remain, partial = some blocks still missing, failed = backfill subprocess errored)",
            gap_backfills_total.clone(),
        );
        registry.register(
            "archive_rows",
            "Total rows archived by archive kind and source table",
            archive_rows_total.clone(),
        );
        registry.register(
            "phase_duration_seconds",
            "Duration of individual archive phases by archive kind and phase",
            phase_duration_seconds.clone(),
        );
        if let Err(err) =
            kubert_prometheus_process::register(registry.sub_registry_with_prefix("process"))
        {
            warn!("failed to register process metrics collector: {err}");
        }

        Self {
            registry,
            health,
            check_interval_seconds,
            db_earliest_slot,
            db_latest_slot,
            db_slots_available,
            chain_tip_slot,
            chain_tip_lag_slots,
            last_run_at_unix,
            last_success_at_unix,
            last_archived_start_slot,
            last_archived_end_slot,
            last_archived_epoch,
            last_archive_rows,
            last_archive_bytes,
            db_lag_slots,
            archive_in_flight,
            validation_slots,
            validation_ranges,
            validation_mismatch_slots,
            validation_range_start_slot,
            validation_range_end_slot,
            validation_db_block_slots,
            validation_rpc_produced_slots,
            known_gaps,
            disk_free_bytes,
            disk_used_bytes,
            disk_total_bytes,
            db_table_bytes,
            db_table_rows,
            archives_created_total,
            archives_skipped_total,
            archive_errors_total,
            archives_cleaned_total,
            clickhouse_range_deleted_total,
            validation_rpc_errors_total,
            validation_slots_total,
            mismatch_repairs_total,
            gap_backfills_total,
            archive_rows_total,
            phase_duration_seconds,
            last_db_latest_slot: AtomicU64::new(0),
            last_chain_tip_slot: AtomicU64::new(0),
        }
    }

    fn kind_gauge(family: &Family<KindLabels, Gauge>, label: &str) -> Gauge {
        family.get_or_create(&KindLabels::new(label)).clone()
    }

    fn observe_db_bounds(&self, bounds: DbSlotStatus) {
        self.db_earliest_slot.set(clamp_i64(bounds.earliest_slot));
        self.db_latest_slot.set(clamp_i64(bounds.latest_slot));
        self.db_slots_available
            .set(clamp_i64(bounds.slots_available));
        self.last_db_latest_slot
            .store(bounds.latest_slot, Ordering::Relaxed);
        self.recompute_chain_tip_lag();
    }

    fn observe_disk_usage(&self, disks: &[DiskUsage]) {
        for disk in disks {
            let labels = DiskLabels::new(disk);
            self.disk_free_bytes
                .get_or_create(&labels)
                .set(clamp_i64(disk.free_bytes));
            self.disk_used_bytes
                .get_or_create(&labels)
                .set(clamp_i64(disk.used_bytes()));
            self.disk_total_bytes
                .get_or_create(&labels)
                .set(clamp_i64(disk.total_bytes));
        }
    }

    fn observe_table_sizes(&self, sizes: &[TableSize]) {
        for size in sizes {
            let labels = DbTableLabels::new(size);
            self.db_table_bytes
                .get_or_create(&labels)
                .set(clamp_i64(size.bytes));
            self.db_table_rows
                .get_or_create(&labels)
                .set(clamp_i64(size.rows));
        }
    }

    fn observe_chain_tip(&self, tip_slot: u64) {
        self.chain_tip_slot.set(clamp_i64(tip_slot));
        self.last_chain_tip_slot.store(tip_slot, Ordering::Relaxed);
        self.recompute_chain_tip_lag();
    }

    fn recompute_chain_tip_lag(&self) {
        let tip = self.last_chain_tip_slot.load(Ordering::Relaxed);
        let db_latest = self.last_db_latest_slot.load(Ordering::Relaxed);
        if tip == 0 {
            return;
        }
        self.chain_tip_lag_slots
            .set(clamp_i64(tip.saturating_sub(db_latest)));
    }

    fn export(&self) -> Result<String, String> {
        let mut buffer = String::new();
        encode(&mut buffer, &self.registry).map_err(|err| err.to_string())?;
        Ok(buffer)
    }
}

#[derive(Debug)]
pub struct AppState {
    started_at_unix: u64,
    last_run_at_unix: AtomicU64,
    last_success_at_unix: AtomicU64,
    archives_created: AtomicU64,
    archives_skipped: AtomicU64,
    archive_errors: AtomicU64,
    /// Whether the most recent archive check completed without erroring. Tracked
    /// separately from `last_error` so routine skipped checks flip health back to
    /// healthy while the last error stays visible until a real archive succeeds.
    healthy: AtomicBool,
    last_error: Mutex<Option<String>>,
    last_error_at_unix: AtomicU64,
    last_report: Mutex<Option<ArchiveRunReport>>,
    db_slots: Mutex<Option<DbSlotStatus>>,
    disk_usage: Mutex<Vec<DiskUsage>>,
    table_sizes: Mutex<Vec<TableSize>>,
    recent_events: Mutex<Vec<ArchiveEvent>>,
    known_gaps: Mutex<Vec<KnownDataGap>>,
    gap_repairs: Mutex<Vec<GapRepairEvent>>,
    metrics: Metrics,
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Metrics { .. }")
    }
}

impl AppState {
    pub fn new() -> Arc<Self> {
        let started_at_unix = unix_timestamp();
        Arc::new(Self {
            started_at_unix,
            last_run_at_unix: AtomicU64::new(0),
            last_success_at_unix: AtomicU64::new(0),
            archives_created: AtomicU64::new(0),
            archives_skipped: AtomicU64::new(0),
            archive_errors: AtomicU64::new(0),
            healthy: AtomicBool::new(true),
            last_error: Mutex::new(None),
            last_error_at_unix: AtomicU64::new(0),
            last_report: Mutex::new(None),
            db_slots: Mutex::new(None),
            disk_usage: Mutex::new(Vec::new()),
            table_sizes: Mutex::new(Vec::new()),
            recent_events: Mutex::new(Vec::new()),
            known_gaps: Mutex::new(Vec::new()),
            gap_repairs: Mutex::new(Vec::new()),
            metrics: Metrics::new(started_at_unix),
        })
    }

    /// Record configuration-derived gauges that do not change at runtime.
    pub fn set_check_interval_secs(&self, interval_secs: u64) {
        self.metrics
            .check_interval_seconds
            .set(clamp_i64(interval_secs));
    }

    /// Toggle the in-flight gauge for an archive kind as tasks start and finish.
    pub fn set_archive_in_flight(&self, kind: ArchiveKind, in_flight: bool) {
        Metrics::kind_gauge(&self.metrics.archive_in_flight, kind.label())
            .set(i64::from(in_flight));
    }

    pub fn record_check_started(&self, kind: ArchiveKind, bounds: Option<ClickHouseBounds>) {
        let timestamp_unix = unix_timestamp();
        self.last_run_at_unix
            .store(timestamp_unix, Ordering::Relaxed);
        Metrics::kind_gauge(&self.metrics.last_run_at_unix, kind.label())
            .set(clamp_i64(timestamp_unix));
        if let Some(bounds) = bounds {
            let db_slots = DbSlotStatus::from(bounds);
            *self.db_slots.lock().expect("db_slots poisoned") = Some(db_slots);
            self.metrics.observe_db_bounds(db_slots);
        }
        self.push_event(ArchiveEvent {
            timestamp_unix,
            archive_kind: kind.label().to_string(),
            outcome: "checking".to_string(),
            archive_name: None,
            reason: None,
            skip_reason_code: None,
            start_slot: None,
            end_slot: None,
        });
    }

    pub fn record_report(&self, report: ArchiveRunReport) {
        let kind_label = report.archive_kind.label();
        self.last_run_at_unix
            .store(report.timestamp_unix, Ordering::Relaxed);
        Metrics::kind_gauge(&self.metrics.last_run_at_unix, kind_label)
            .set(clamp_i64(report.timestamp_unix));
        if let Some(bounds) = report.db_bounds {
            let db_slots = DbSlotStatus::from(bounds);
            *self.db_slots.lock().expect("db_slots poisoned") = Some(db_slots);
            self.metrics.observe_db_bounds(db_slots);
        }
        if let Some(tip_slot) = report.run_metrics.chain_tip_slot {
            self.metrics.observe_chain_tip(tip_slot);
        }
        if !report.run_metrics.disk_usage.is_empty() {
            self.metrics
                .observe_disk_usage(&report.run_metrics.disk_usage);
            *self.disk_usage.lock().expect("disk_usage poisoned") =
                report.run_metrics.disk_usage.clone();
        }
        if !report.run_metrics.table_sizes.is_empty() {
            self.metrics
                .observe_table_sizes(&report.run_metrics.table_sizes);
            *self.table_sizes.lock().expect("table_sizes poisoned") =
                report.run_metrics.table_sizes.clone();
        }
        if report.archive_created {
            self.archives_created.fetch_add(1, Ordering::Relaxed);
            self.metrics
                .archives_created_total
                .get_or_create(&KindLabels::new(kind_label))
                .inc();
            self.last_success_at_unix
                .store(report.timestamp_unix, Ordering::Relaxed);
            Metrics::kind_gauge(&self.metrics.last_success_at_unix, kind_label)
                .set(clamp_i64(report.timestamp_unix));
        } else {
            self.archives_skipped.fetch_add(1, Ordering::Relaxed);
            let reason = classify_skip_reason(&report).unwrap_or_else(|| "skipped".to_string());
            self.metrics
                .archives_skipped_total
                .get_or_create(&SkipLabels {
                    archive_kind: kind_label.to_string(),
                    reason,
                })
                .inc();
        }
        self.record_archive_metrics(&report);
        self.record_known_gaps(&report);
        self.record_gap_repairs(&report);
        self.update_known_gap_metrics(kind_label);
        // The check completed without erroring, so the service is healthy again.
        // Only a genuine archive creation clears the last error — routine skipped
        // checks must not wipe it, otherwise the last error is never visible.
        self.healthy.store(true, Ordering::Relaxed);
        if report.archive_created {
            *self.last_error.lock().expect("last_error poisoned") = None;
            self.last_error_at_unix.store(0, Ordering::Relaxed);
        }
        self.metrics.health.set(1);
        let skip_reason_code = classify_skip_reason(&report);
        self.push_event(ArchiveEvent {
            timestamp_unix: report.timestamp_unix,
            archive_kind: kind_label.to_string(),
            outcome: if report.archive_created {
                "created".to_string()
            } else {
                "skipped".to_string()
            },
            archive_name: report.archive_name.clone(),
            reason: report.archive_skipped_reason.clone(),
            skip_reason_code,
            start_slot: report.archive_slot_start,
            end_slot: report.archive_slot_end,
        });
        *self.last_report.lock().expect("last_report poisoned") = Some(report);
    }

    pub fn record_task_error(&self, kind: ArchiveKind, error: String) {
        let timestamp_unix = unix_timestamp();
        self.last_run_at_unix
            .store(timestamp_unix, Ordering::Relaxed);
        Metrics::kind_gauge(&self.metrics.last_run_at_unix, kind.label())
            .set(clamp_i64(timestamp_unix));
        self.archive_errors.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .archive_errors_total
            .get_or_create(&KindLabels::new(kind.label()))
            .inc();
        self.healthy.store(false, Ordering::Relaxed);
        self.metrics.health.set(0);
        *self.last_error.lock().expect("last_error poisoned") = Some(error.clone());
        self.last_error_at_unix
            .store(timestamp_unix, Ordering::Relaxed);
        self.push_event(ArchiveEvent {
            timestamp_unix,
            archive_kind: kind.label().to_string(),
            outcome: "error".to_string(),
            archive_name: None,
            reason: Some(error),
            skip_reason_code: None,
            start_slot: None,
            end_slot: None,
        });
    }

    pub fn public_status(&self) -> PublicStatus {
        PublicStatus {
            build: crate::manifest::ManifestProducer::current(),
            started_at_unix: self.started_at_unix,
            last_run_at_unix: nonzero(self.last_run_at_unix.load(Ordering::Relaxed)),
            last_success_at_unix: nonzero(self.last_success_at_unix.load(Ordering::Relaxed)),
            healthy: self.healthy.load(Ordering::Relaxed),
            last_error: self.last_error.lock().expect("last_error poisoned").clone(),
            last_error_at_unix: nonzero(self.last_error_at_unix.load(Ordering::Relaxed)),
            last_report: self
                .last_report
                .lock()
                .expect("last_report poisoned")
                .clone(),
            db_slots: *self.db_slots.lock().expect("db_slots poisoned"),
            disk_usage: self.disk_usage.lock().expect("disk_usage poisoned").clone(),
            table_sizes: self
                .table_sizes
                .lock()
                .expect("table_sizes poisoned")
                .clone(),
            recent_events: self
                .recent_events
                .lock()
                .expect("recent_events poisoned")
                .clone(),
            known_gaps: self.known_gaps.lock().expect("known_gaps poisoned").clone(),
            gap_repairs: self
                .gap_repairs
                .lock()
                .expect("gap_repairs poisoned")
                .clone(),
            archives_created: self.archives_created.load(Ordering::Relaxed),
            archives_skipped: self.archives_skipped.load(Ordering::Relaxed),
            archive_errors: self.archive_errors.load(Ordering::Relaxed),
        }
    }

    pub fn prometheus_text(&self) -> String {
        match self.metrics.export() {
            Ok(buffer) => buffer,
            Err(err) => {
                warn!("failed to encode solparq metrics: {err}");
                format!("# failed to encode metrics: {err}\n")
            }
        }
    }

    fn record_archive_metrics(&self, report: &ArchiveRunReport) {
        let kind_label = report.archive_kind.label();
        let run = &report.run_metrics;
        for phase in &run.phase_durations {
            self.metrics
                .phase_duration_seconds
                .get_or_create(&PhaseLabels {
                    archive_kind: kind_label.to_string(),
                    phase: phase.phase.clone(),
                })
                .observe(phase.seconds);
        }
        // Row counts on a non-created report (e.g. a dry run previewing what
        // would be archived) must never feed the cumulative "rows archived"
        // counter or gauge; only a real archive advances them.
        if report.archive_created {
            let mut total_rows = 0u64;
            for table in &run.archived_table_rows {
                total_rows = total_rows.saturating_add(table.rows);
                self.metrics
                    .archive_rows_total
                    .get_or_create(&TableLabels {
                        archive_kind: kind_label.to_string(),
                        table: table.table.clone(),
                    })
                    .inc_by(table.rows);
            }
            if !run.archived_table_rows.is_empty() {
                Metrics::kind_gauge(&self.metrics.last_archive_rows, kind_label)
                    .set(clamp_i64(total_rows));
            }
        }
        if let Some(bytes) = run.archived_bytes_total {
            Metrics::kind_gauge(&self.metrics.last_archive_bytes, kind_label).set(clamp_i64(bytes));
        }
        if report.archive_created {
            if let (Some(start), Some(end)) = (report.archive_slot_start, report.archive_slot_end) {
                Metrics::kind_gauge(&self.metrics.last_archived_start_slot, kind_label)
                    .set(clamp_i64(start));
                Metrics::kind_gauge(&self.metrics.last_archived_end_slot, kind_label)
                    .set(clamp_i64(end));
            }
            if let Some(epoch) = report.archive_epoch {
                Metrics::kind_gauge(&self.metrics.last_archived_epoch, kind_label)
                    .set(clamp_i64(epoch));
            }
            if report.deleted_clickhouse_range {
                self.metrics
                    .clickhouse_range_deleted_total
                    .get_or_create(&KindLabels::new(kind_label))
                    .inc();
            }
        }
        if !report.cleaned_archives.is_empty() {
            self.metrics
                .archives_cleaned_total
                .get_or_create(&KindLabels::new(kind_label))
                .inc_by(report.cleaned_archives.len() as u64);
        }
        // db_lag = latest ClickHouse slot - latest archived slot for this kind.
        let last_archived_end =
            Metrics::kind_gauge(&self.metrics.last_archived_end_slot, kind_label)
                .get()
                .max(0) as u64;
        if last_archived_end > 0 {
            let db_latest = self.metrics.last_db_latest_slot.load(Ordering::Relaxed);
            Metrics::kind_gauge(&self.metrics.db_lag_slots, kind_label)
                .set(clamp_i64(db_latest.saturating_sub(last_archived_end)));
        }
        if let Some(validation) = &report.validation {
            // Actual gaps that need backfill: Solana produced the block but it is
            // missing from ClickHouse.
            self.observe_validation_category(
                kind_label,
                ValidationCategory::MissingBlock,
                validation.missing_blocks.len() as u64,
                validation.missing_block_ranges.len() as u64,
            );
            // Expected leader gaps: the slot was never produced on-chain.
            let not_produced_slots: u64 = validation
                .not_produced_slot_ranges
                .iter()
                .map(|range| range.slot_count)
                .sum();
            self.observe_validation_category(
                kind_label,
                ValidationCategory::NotProduced,
                not_produced_slots,
                validation.not_produced_slot_ranges.len() as u64,
            );
            // Other data issues: block transaction count vs archived rows.
            self.observe_validation_category(
                kind_label,
                ValidationCategory::TransactionMismatch,
                validation.transaction_mismatches.len() as u64,
                validation.transaction_mismatch_ranges.len() as u64,
            );
            // Split mismatches by direction: undercount needs re-ingestion,
            // overcount is dedup-fixable.
            let undercount_slots: u64 = validation
                .transaction_undercount_ranges
                .iter()
                .map(|range| range.slot_count)
                .sum();
            let overcount_slots: u64 = validation
                .transaction_overcount_ranges
                .iter()
                .map(|range| range.slot_count)
                .sum();
            self.set_mismatch_slots(kind_label, MismatchDirection::Undercount, undercount_slots);
            self.set_mismatch_slots(kind_label, MismatchDirection::Overcount, overcount_slots);
            Metrics::kind_gauge(&self.metrics.validation_range_start_slot, kind_label)
                .set(clamp_i64(validation.start_slot));
            Metrics::kind_gauge(&self.metrics.validation_range_end_slot, kind_label)
                .set(clamp_i64(validation.end_slot));
            Metrics::kind_gauge(&self.metrics.validation_db_block_slots, kind_label)
                .set(clamp_i64(validation.db_block_slots));
            Metrics::kind_gauge(&self.metrics.validation_rpc_produced_slots, kind_label)
                .set(clamp_i64(validation.rpc_produced_slots));
            if validation.rpc_check_error.is_some() {
                self.metrics
                    .validation_rpc_errors_total
                    .get_or_create(&KindLabels::new(kind_label))
                    .inc();
            }
        }
        if let Some(repair) = &report.run_metrics.mismatch_repair {
            let outcome = if repair.overcount_slots_after == 0 {
                "repaired"
            } else {
                "still_dirty"
            };
            self.record_mismatch_repair(kind_label, outcome);
        }
        if let Some(backfill) = &report.run_metrics.gap_backfill {
            let outcome = if !backfill.succeeded {
                "failed"
            } else if backfill.missing_blocks_after == 0 {
                "filled"
            } else {
                "partial"
            };
            self.record_gap_backfill(kind_label, outcome);
        }
    }

    /// Record the last-validated-range gauges and cumulative counter for one
    /// validation category.
    fn observe_validation_category(
        &self,
        kind_label: &str,
        category: ValidationCategory,
        slots: u64,
        ranges: u64,
    ) {
        let labels = ValidationLabels {
            archive_kind: kind_label.to_string(),
            category: category.as_str().to_string(),
        };
        self.metrics
            .validation_slots
            .get_or_create(&labels)
            .set(clamp_i64(slots));
        self.metrics
            .validation_ranges
            .get_or_create(&labels)
            .set(clamp_i64(ranges));
        if slots > 0 {
            self.metrics
                .validation_slots_total
                .get_or_create(&labels)
                .inc_by(slots);
        }
    }

    fn set_mismatch_slots(&self, kind_label: &str, direction: MismatchDirection, slots: u64) {
        self.metrics
            .validation_mismatch_slots
            .get_or_create(&MismatchLabels {
                archive_kind: kind_label.to_string(),
                direction: direction.as_str().to_string(),
            })
            .set(clamp_i64(slots));
    }

    fn record_mismatch_repair(&self, kind_label: &str, outcome: &str) {
        self.metrics
            .mismatch_repairs_total
            .get_or_create(&RepairLabels {
                archive_kind: kind_label.to_string(),
                outcome: outcome.to_string(),
            })
            .inc();
    }

    fn record_gap_backfill(&self, kind_label: &str, outcome: &str) {
        self.metrics
            .gap_backfills_total
            .get_or_create(&RepairLabels {
                archive_kind: kind_label.to_string(),
                outcome: outcome.to_string(),
            })
            .inc();
    }

    fn update_known_gap_metrics(&self, kind_label: &str) {
        let gaps = self.known_gaps.lock().expect("known_gaps poisoned");
        for classification in GAP_CLASSIFICATIONS {
            let count = gaps
                .iter()
                .filter(|gap| {
                    gap.archive_kind == kind_label && gap.classification == classification
                })
                .count();
            self.metrics
                .known_gaps
                .get_or_create(&GapLabels {
                    archive_kind: kind_label.to_string(),
                    classification: classification.to_string(),
                })
                .set(clamp_i64(count as u64));
        }
    }

    fn push_event(&self, event: ArchiveEvent) {
        let mut events = self.recent_events.lock().expect("recent_events poisoned");
        events.push(event);
        let overflow = events.len().saturating_sub(MAX_RECENT_EVENTS);
        if overflow > 0 {
            events.drain(0..overflow);
        }
    }

    fn record_known_gaps(&self, report: &ArchiveRunReport) {
        let Some(validation) = &report.validation else {
            return;
        };
        let mut gaps = self.known_gaps.lock().expect("known_gaps poisoned");
        for range in &validation.missing_block_ranges {
            gaps.push(known_gap(
                report,
                "Needs backfill",
                *range,
                "Produced block missing from blocks_metadata",
            ));
        }
        for range in &validation.not_produced_slot_ranges {
            gaps.push(known_gap(
                report,
                LEGIT_NOT_PRODUCED_CLASSIFICATION,
                *range,
                "Slot not returned by Solana getBlocks",
            ));
        }
        for range in &validation.transaction_undercount_ranges {
            gaps.push(known_gap(
                report,
                "Transaction mismatch (undercount)",
                *range,
                "Fewer archived rows than the block declares; needs re-ingestion",
            ));
        }
        for range in &validation.transaction_overcount_ranges {
            gaps.push(known_gap(
                report,
                "Transaction mismatch (overcount)",
                *range,
                "More archived rows than the block declares; fixable by ClickHouse dedup",
            ));
        }
        let overflow = gaps.len().saturating_sub(MAX_RECENT_EVENTS);
        if overflow > 0 {
            gaps.drain(0..overflow);
        }
    }

    fn record_gap_repairs(&self, report: &ArchiveRunReport) {
        let kind_label = report.archive_kind.label().to_string();
        let mut repairs = self.gap_repairs.lock().expect("gap_repairs poisoned");
        if let Some(backfill) = &report.run_metrics.gap_backfill {
            repairs.push(GapRepairEvent {
                timestamp_unix: report.timestamp_unix,
                archive_kind: kind_label.clone(),
                kind: "RPC backfill".to_string(),
                // A backfill is only a success if it ran cleanly AND no produced
                // block is still missing afterward.
                succeeded: backfill.succeeded && backfill.missing_blocks_after == 0,
                start_slot: report.archive_slot_start,
                end_slot: report.archive_slot_end,
                detail: format!(
                    "targeted {} slot(s); {} still missing after",
                    backfill.slots_targeted, backfill.missing_blocks_after
                ),
            });
        }
        if let Some(repair) = &report.run_metrics.mismatch_repair {
            repairs.push(GapRepairEvent {
                timestamp_unix: report.timestamp_unix,
                archive_kind: kind_label,
                kind: "Dedup repair".to_string(),
                succeeded: repair.overcount_slots_after == 0,
                start_slot: report.archive_slot_start,
                end_slot: report.archive_slot_end,
                detail: format!(
                    "{} partition(s); overcount {}→{}",
                    repair.partitions_optimized,
                    repair.overcount_slots_before,
                    repair.overcount_slots_after
                ),
            });
        }
        let overflow = repairs.len().saturating_sub(MAX_RECENT_EVENTS);
        if overflow > 0 {
            repairs.drain(0..overflow);
        }
    }
}

fn known_gap(
    report: &ArchiveRunReport,
    classification: impl Into<String>,
    range: SlotRange,
    detail: impl Into<String>,
) -> KnownDataGap {
    KnownDataGap {
        timestamp_unix: report.timestamp_unix,
        archive_kind: report.archive_kind.label().to_string(),
        classification: classification.into(),
        start_slot: range.start_slot,
        end_slot: range.end_slot,
        slot_count: range.slot_count,
        detail: detail.into(),
    }
}

fn classify_skip_reason(report: &ArchiveRunReport) -> Option<String> {
    if report.archive_created {
        return None;
    }
    let reason = report.archive_skipped_reason.as_deref().unwrap_or_default();
    if reason.contains("not enough ClickHouse slots") {
        return Some("not-enough-slots".to_string());
    }
    if reason.contains("no transactions") {
        return Some("no-data".to_string());
    }
    if reason.contains("user declined") {
        return Some("user-declined".to_string());
    }
    if reason.contains("validation warnings") {
        if report
            .validation
            .as_ref()
            .map(|validation| {
                !validation.missing_block_ranges.is_empty()
                    || !validation.transaction_mismatch_ranges.is_empty()
            })
            .unwrap_or(false)
        {
            return Some("data-gap".to_string());
        }
        return Some("validation-warning".to_string());
    }
    if reason.contains("dry-run") {
        return Some("dry-run".to_string());
    }
    Some("skipped".to_string())
}

fn nonzero(value: u64) -> Option<u64> {
    if value == 0 { None } else { Some(value) }
}

fn clamp_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
}
