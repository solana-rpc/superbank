use serde::{Deserialize, Serialize};

use crate::{
    archive::ArchiveKind,
    clickhouse::{ArchiveDbTable, ArchiveTableKind},
};

pub const MANIFEST_FILE_NAME: &str = "manifest.json";
pub const REPORT_FILE_NAME: &str = "report.txt";
pub const SHA256SUMS_FILE_NAME: &str = "SHA256SUMS.txt";
pub const DONE_FILE_PREFIX: &str = ".done";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveManifest {
    pub format_version: u32,
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
            format_version: 1,
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
