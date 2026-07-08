use serde::{Deserialize, Serialize};

use crate::{
    archive::ArchiveKind,
    clickhouse::{ArchiveDbTable, ArchiveTableKind},
};

pub const MANIFEST_FILE_NAME: &str = "manifest.json";
pub const REPORT_FILE_NAME: &str = "report.json";
pub const SHA256SUMS_FILE_NAME: &str = "SHA256SUMS.txt";
pub const DONE_FILE_PREFIX: &str = ".done";

/// Bump when the manifest schema changes. A reader can compare this against
/// the highest version it understands to detect archives written by a newer
/// producer.
pub const MANIFEST_FORMAT_VERSION: u32 = 2;

/// Bump when the `report.json` schema changes.
pub const REPORT_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveManifest {
    pub format_version: u32,
    /// Build identity of the `superbank-solparq` binary that wrote this archive.
    pub producer: ManifestProducer,
    pub archive_id: String,
    pub archive_kind: String,
    pub epoch: u64,
    pub start_slot: u64,
    pub end_slot: u64,
    pub tables: Vec<ArchiveManifestTable>,
    pub skipped_tables: Vec<SkippedArchiveTable>,
    pub poh_tool_ready: bool,
}

impl ArchiveManifest {
    pub fn new(
        archive_id: String,
        archive_kind: ArchiveKind,
        epoch: u64,
        start_slot: u64,
        end_slot: u64,
        tables: Vec<ArchiveManifestTable>,
        skipped_tables: Vec<SkippedArchiveTable>,
    ) -> Self {
        let poh_tool_ready = tables
            .iter()
            .any(|table| table.kind == ArchiveTableKind::Entries.as_str());
        Self {
            format_version: MANIFEST_FORMAT_VERSION,
            producer: ManifestProducer::current(),
            archive_id,
            archive_kind: archive_kind.to_string(),
            epoch,
            start_slot,
            end_slot,
            tables,
            skipped_tables,
            poh_tool_ready,
        }
    }
}

/// Identifies the exact build that produced an archive, so a mismatch or bug
/// found later in a bundle can be traced back to the commit that wrote it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestProducer {
    pub name: String,
    pub version: String,
    pub git_sha: String,
}

impl ManifestProducer {
    pub fn current() -> Self {
        Self {
            name: env!("CARGO_PKG_NAME").to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            git_sha: option_env!("SUPERBANK_GIT_SHA")
                .map(str::trim)
                .filter(|sha| !sha.is_empty())
                .unwrap_or("unknown")
                .to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveManifestTable {
    pub kind: String,
    pub table_name: String,
    pub file_name: String,
    pub row_count: u64,
    pub required: bool,
}

impl ArchiveManifestTable {
    pub fn from_table(table: &ArchiveDbTable, row_count: u64) -> Self {
        Self {
            kind: table.kind.as_str().to_string(),
            table_name: table.table_name.clone(),
            file_name: table.file_name().to_string(),
            row_count,
            required: table.required,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkippedArchiveTable {
    pub kind: String,
    pub table_name: String,
    pub required: bool,
    pub reason: String,
}

impl SkippedArchiveTable {
    pub fn unavailable(table: &ArchiveDbTable) -> Self {
        Self {
            kind: table.kind.as_str().to_string(),
            table_name: table.table_name.clone(),
            required: table.required,
            reason: "table not available".to_string(),
        }
    }
}
