use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::Serialize;

use crate::{
    archive::{ArchiveKind, ArchiveRunReport, ClickHouseBounds},
    clickhouse::SlotRange,
};

const MAX_RECENT_EVENTS: usize = 50;

#[derive(Debug, Clone, Serialize)]
pub struct PublicStatus {
    pub started_at_unix: u64,
    pub last_run_at_unix: Option<u64>,
    pub last_success_at_unix: Option<u64>,
    pub last_error: Option<String>,
    pub last_report: Option<ArchiveRunReport>,
    pub db_slots: Option<DbSlotStatus>,
    pub recent_events: Vec<ArchiveEvent>,
    pub known_gaps: Vec<KnownDataGap>,
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

#[derive(Debug)]
pub struct AppState {
    started_at_unix: u64,
    last_run_at_unix: AtomicU64,
    last_success_at_unix: AtomicU64,
    archives_created: AtomicU64,
    archives_skipped: AtomicU64,
    archive_errors: AtomicU64,
    last_error: Mutex<Option<String>>,
    last_report: Mutex<Option<ArchiveRunReport>>,
    db_slots: Mutex<Option<DbSlotStatus>>,
    recent_events: Mutex<Vec<ArchiveEvent>>,
    known_gaps: Mutex<Vec<KnownDataGap>>,
}

impl AppState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            started_at_unix: unix_timestamp(),
            last_run_at_unix: AtomicU64::new(0),
            last_success_at_unix: AtomicU64::new(0),
            archives_created: AtomicU64::new(0),
            archives_skipped: AtomicU64::new(0),
            archive_errors: AtomicU64::new(0),
            last_error: Mutex::new(None),
            last_report: Mutex::new(None),
            db_slots: Mutex::new(None),
            recent_events: Mutex::new(Vec::new()),
            known_gaps: Mutex::new(Vec::new()),
        })
    }

    pub fn record_check_started(&self, kind: ArchiveKind, bounds: Option<ClickHouseBounds>) {
        let timestamp_unix = unix_timestamp();
        self.last_run_at_unix
            .store(timestamp_unix, Ordering::Relaxed);
        if let Some(bounds) = bounds {
            *self.db_slots.lock().expect("db_slots poisoned") = Some(bounds.into());
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
        self.last_run_at_unix
            .store(report.timestamp_unix, Ordering::Relaxed);
        if let Some(bounds) = report.db_bounds {
            *self.db_slots.lock().expect("db_slots poisoned") = Some(bounds.into());
        }
        if report.archive_created {
            self.archives_created.fetch_add(1, Ordering::Relaxed);
            self.last_success_at_unix
                .store(report.timestamp_unix, Ordering::Relaxed);
        } else {
            self.archives_skipped.fetch_add(1, Ordering::Relaxed);
        }
        self.record_known_gaps(&report);
        *self.last_error.lock().expect("last_error poisoned") = None;
        let skip_reason_code = classify_skip_reason(&report);
        self.push_event(ArchiveEvent {
            timestamp_unix: report.timestamp_unix,
            archive_kind: report.archive_kind.label().to_string(),
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

    pub fn record_error(&self, error: String) {
        self.last_run_at_unix
            .store(unix_timestamp(), Ordering::Relaxed);
        self.archive_errors.fetch_add(1, Ordering::Relaxed);
        *self.last_error.lock().expect("last_error poisoned") = Some(error);
    }

    pub fn record_task_error(&self, kind: ArchiveKind, error: String) {
        let timestamp_unix = unix_timestamp();
        self.last_run_at_unix
            .store(timestamp_unix, Ordering::Relaxed);
        self.archive_errors.fetch_add(1, Ordering::Relaxed);
        *self.last_error.lock().expect("last_error poisoned") = Some(error.clone());
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
            started_at_unix: self.started_at_unix,
            last_run_at_unix: nonzero(self.last_run_at_unix.load(Ordering::Relaxed)),
            last_success_at_unix: nonzero(self.last_success_at_unix.load(Ordering::Relaxed)),
            last_error: self.last_error.lock().expect("last_error poisoned").clone(),
            last_report: self
                .last_report
                .lock()
                .expect("last_report poisoned")
                .clone(),
            db_slots: *self.db_slots.lock().expect("db_slots poisoned"),
            recent_events: self
                .recent_events
                .lock()
                .expect("recent_events poisoned")
                .clone(),
            known_gaps: self.known_gaps.lock().expect("known_gaps poisoned").clone(),
            archives_created: self.archives_created.load(Ordering::Relaxed),
            archives_skipped: self.archives_skipped.load(Ordering::Relaxed),
            archive_errors: self.archive_errors.load(Ordering::Relaxed),
        }
    }

    pub fn prometheus_text(&self) -> String {
        let status = self.public_status();
        let (earliest_slot, latest_slot, slots_available) = status
            .db_slots
            .map(|slots| {
                (
                    slots.earliest_slot.to_string(),
                    slots.latest_slot.to_string(),
                    slots.slots_available.to_string(),
                )
            })
            .unwrap_or_else(|| ("0".to_string(), "0".to_string(), "0".to_string()));
        format!(
            "# HELP solparq_archives_created_total Archives created successfully\n\
             # TYPE solparq_archives_created_total counter\n\
             solparq_archives_created_total {}\n\
             # HELP solparq_archives_skipped_total Archive planning runs skipped without creating an archive\n\
             # TYPE solparq_archives_skipped_total counter\n\
             solparq_archives_skipped_total {}\n\
             # HELP solparq_archive_errors_total Archive loop errors\n\
             # TYPE solparq_archive_errors_total counter\n\
             solparq_archive_errors_total {}\n\
             # HELP solparq_started_at_unix Process start time\n\
             # TYPE solparq_started_at_unix gauge\n\
             solparq_started_at_unix {}\n\
             # HELP solparq_last_run_at_unix Last archive run time\n\
             # TYPE solparq_last_run_at_unix gauge\n\
             solparq_last_run_at_unix {}\n\
             # HELP solparq_last_success_at_unix Last successful archive time\n\
             # TYPE solparq_last_success_at_unix gauge\n\
             solparq_last_success_at_unix {}\n\
             # HELP solparq_health Healthy when the last loop has no error\n\
             # TYPE solparq_health gauge\n\
             solparq_health {}\n\
             # HELP solparq_db_earliest_slot Earliest transaction slot visible in ClickHouse\n\
             # TYPE solparq_db_earliest_slot gauge\n\
             solparq_db_earliest_slot {}\n\
             # HELP solparq_db_latest_slot Latest transaction slot visible in ClickHouse\n\
             # TYPE solparq_db_latest_slot gauge\n\
             solparq_db_latest_slot {}\n\
             # HELP solparq_db_slots_available Number of transaction slots visible in ClickHouse\n\
             # TYPE solparq_db_slots_available gauge\n\
             solparq_db_slots_available {}\n",
            status.archives_created,
            status.archives_skipped,
            status.archive_errors,
            status.started_at_unix,
            status.last_run_at_unix.unwrap_or(0),
            status.last_success_at_unix.unwrap_or(0),
            u8::from(status.last_error.is_none()),
            earliest_slot,
            latest_slot,
            slots_available
        )
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
                "Legit not-produced",
                *range,
                "Slot not returned by Solana getBlocks",
            ));
        }
        for range in &validation.transaction_mismatch_ranges {
            gaps.push(known_gap(
                report,
                "Transaction mismatch",
                *range,
                "Block transaction count does not match transaction rows",
            ));
        }
        let overflow = gaps.len().saturating_sub(MAX_RECENT_EVENTS);
        if overflow > 0 {
            gaps.drain(0..overflow);
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
    Some("skipped".to_string())
}

fn nonzero(value: u64) -> Option<u64> {
    if value == 0 { None } else { Some(value) }
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
}
