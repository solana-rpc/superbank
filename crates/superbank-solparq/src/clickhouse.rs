use std::{collections::HashSet, fmt, path::Path};

use anyhow::{Context, Result, anyhow};
use futures_util::StreamExt;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use tokio::{fs, io::AsyncWriteExt};

use crate::{archive::ClickHouseBounds, config::Config};

#[derive(Debug, Clone)]
pub struct DbTables {
    pub transactions_table: String,
    pub blocks_table: String,
    pub entries_table: String,
    pub gsfa_table: String,
    pub gsfa_hot_table: String,
    pub signatures_table: String,
    pub token_owner_activity_table: String,
}

impl DbTables {
    pub fn from_config(config: &Config) -> Self {
        Self {
            transactions_table: config.transactions_table.clone(),
            blocks_table: config.blocks_table.clone(),
            entries_table: config.entries_table.clone(),
            gsfa_table: config.gsfa_table.clone(),
            gsfa_hot_table: config.gsfa_hot_table.clone(),
            signatures_table: config.signatures_table.clone(),
            token_owner_activity_table: config.token_owner_activity_table.clone(),
        }
    }

    pub fn archive_tables(&self) -> Vec<ArchiveDbTable> {
        vec![
            ArchiveDbTable::new(
                ArchiveTableKind::Transactions,
                self.transactions_table.clone(),
                "slot, slot_idx, signature",
                true,
            ),
            ArchiveDbTable::new(
                ArchiveTableKind::BlocksMetadata,
                self.blocks_table.clone(),
                "slot",
                true,
            ),
            ArchiveDbTable::new(
                ArchiveTableKind::Entries,
                self.entries_table.clone(),
                "slot, entry_index",
                false,
            ),
            ArchiveDbTable::new(
                ArchiveTableKind::Gsfa,
                self.gsfa_table.clone(),
                "slot, slot_idx, signature",
                false,
            ),
            ArchiveDbTable::new(
                ArchiveTableKind::GsfaHot,
                self.gsfa_hot_table.clone(),
                "slot, slot_idx, signature",
                false,
            ),
            ArchiveDbTable::new(
                ArchiveTableKind::Signatures,
                self.signatures_table.clone(),
                "slot, slot_idx, signature",
                false,
            ),
            ArchiveDbTable::new(
                ArchiveTableKind::TokenOwnerActivity,
                self.token_owner_activity_table.clone(),
                "slot, slot_idx, signature",
                false,
            ),
        ]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArchiveTableKind {
    Transactions,
    BlocksMetadata,
    Entries,
    Gsfa,
    GsfaHot,
    Signatures,
    TokenOwnerActivity,
}

impl ArchiveTableKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ArchiveTableKind::Transactions => "transactions",
            ArchiveTableKind::BlocksMetadata => "blocks_metadata",
            ArchiveTableKind::Entries => "entries",
            ArchiveTableKind::Gsfa => "gsfa",
            ArchiveTableKind::GsfaHot => "gsfa_hot",
            ArchiveTableKind::Signatures => "signatures",
            ArchiveTableKind::TokenOwnerActivity => "token_owner_activity",
        }
    }

    pub fn file_name(self) -> &'static str {
        match self {
            ArchiveTableKind::Transactions => "transactions.parquet",
            ArchiveTableKind::BlocksMetadata => "blocks_metadata.parquet",
            ArchiveTableKind::Entries => "entries.parquet",
            ArchiveTableKind::Gsfa => "gsfa.parquet",
            ArchiveTableKind::GsfaHot => "gsfa_hot.parquet",
            ArchiveTableKind::Signatures => "signatures.parquet",
            ArchiveTableKind::TokenOwnerActivity => "token_owner_activity.parquet",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArchiveDbTable {
    pub kind: ArchiveTableKind,
    pub table_name: String,
    pub order_by: String,
    pub required: bool,
}

impl ArchiveDbTable {
    fn new(
        kind: ArchiveTableKind,
        table_name: String,
        order_by: impl Into<String>,
        required: bool,
    ) -> Self {
        Self {
            kind,
            table_name,
            order_by: order_by.into(),
            required,
        }
    }

    pub fn file_name(&self) -> &'static str {
        self.kind.file_name()
    }
}

#[derive(Debug, Clone)]
pub struct ClickHouseClient {
    http: reqwest::Client,
    url: String,
    database: String,
    user: String,
    password: String,
    archive_settings: String,
    archive_dedup_export: bool,
}

impl ClickHouseClient {
    pub fn from_config(config: &Config) -> Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder()
                .build()
                .context("build ClickHouse HTTP client")?,
            url: config.clickhouse_url(),
            database: config.db_database.clone(),
            user: config.db_user.clone(),
            password: config.db_password.clone(),
            archive_settings: config.clickhouse_archive_settings.clone(),
            archive_dedup_export: config.archive_dedup_export,
        })
    }

    pub async fn check_tables(&self, tables: &DbTables) -> Result<Vec<ArchiveDbTable>> {
        let mut available = Vec::new();
        for table in tables.archive_tables() {
            let sql = format!("SELECT count() FROM {} WHERE 0", table.table_name);
            match self.execute(&sql).await {
                Ok(()) => available.push(table),
                Err(err) if table.required => {
                    return Err(err)
                        .with_context(|| format!("check ClickHouse table {}", table.table_name));
                }
                Err(_) => {
                    tracing::debug!(
                        archive_table = table.kind.as_str(),
                        table_name = table.table_name,
                        "optional ClickHouse archive table is not available"
                    );
                }
            }
        }
        Ok(available)
    }

    pub async fn fetch_bounds(&self, transactions_table: &str) -> Result<Option<ClickHouseBounds>> {
        #[derive(Debug, Deserialize)]
        struct BoundsRow {
            earliest_slot: Option<u64>,
            latest_slot: Option<u64>,
            distinct_slots: u64,
        }

        let sql = format!(
            "SELECT minOrNull(slot) AS earliest_slot, maxOrNull(slot) AS latest_slot, uniqExact(slot) AS distinct_slots FROM {transactions_table}"
        );
        let rows = self.query_json_rows::<BoundsRow>(&sql).await?;
        let Some(row) = rows.into_iter().next() else {
            return Ok(None);
        };
        match (row.earliest_slot, row.latest_slot) {
            (Some(earliest_slot), Some(latest_slot)) => Ok(Some(ClickHouseBounds {
                earliest_slot,
                latest_slot,
                distinct_slots: row.distinct_slots,
            })),
            _ => Ok(None),
        }
    }

    pub async fn validate_archive_range(
        &self,
        tables: &DbTables,
        solana_rpc_url: &str,
        start_slot: u64,
        end_slot: u64,
    ) -> Result<ValidationReport> {
        let db_block_slots = self
            .fetch_block_slots(&tables.blocks_table, start_slot, end_slot)
            .await?;
        let (produced_slots, rpc_check_error) =
            match fetch_solana_produced_slots(solana_rpc_url, start_slot, end_slot).await {
                Ok(slots) => (slots, None),
                Err(err) => (Vec::new(), Some(err.to_string())),
            };

        let transaction_mismatches = self
            .fetch_transaction_mismatches(tables, start_slot, end_slot)
            .await?;

        Ok(ValidationReport::from_observed_slots(
            start_slot,
            end_slot,
            db_block_slots,
            produced_slots,
            transaction_mismatches,
            rpc_check_error,
        ))
    }

    pub async fn stream_local_parquet(
        &self,
        transactions_table: &str,
        start_slot: u64,
        end_slot: u64,
        path: &Path,
    ) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create archive directory {}", parent.display()))?;
        }

        let tmp_path = path.with_extension("parquet.tmp");
        let query = build_local_parquet_query(
            transactions_table,
            start_slot,
            end_slot,
            &self.archive_settings,
            self.archive_dedup_export,
        );
        let response = self.post_sql(&query).await?;
        let mut stream = response.bytes_stream();
        let mut file = fs::File::create(&tmp_path)
            .await
            .with_context(|| format!("create temporary archive {}", tmp_path.display()))?;

        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context("read ClickHouse parquet response chunk")?;
            file.write_all(&chunk)
                .await
                .context("write parquet archive chunk")?;
        }
        file.flush().await.context("flush parquet archive")?;
        drop(file);
        fs::rename(&tmp_path, path)
            .await
            .with_context(|| format!("move archive into place {}", path.display()))?;
        Ok(())
    }

    pub async fn stream_local_table_parquet(
        &self,
        table: &ArchiveDbTable,
        start_slot: u64,
        end_slot: u64,
        path: &Path,
    ) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create archive directory {}", parent.display()))?;
        }

        let tmp_path = path.with_extension("parquet.tmp");
        let query = build_local_table_parquet_query(
            table,
            start_slot,
            end_slot,
            &self.archive_settings,
            self.archive_dedup_export,
        );
        let response = self.post_sql(&query).await?;
        let mut stream = response.bytes_stream();
        let mut file = fs::File::create(&tmp_path)
            .await
            .with_context(|| format!("create temporary archive {}", tmp_path.display()))?;

        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context("read ClickHouse parquet response chunk")?;
            file.write_all(&chunk)
                .await
                .context("write parquet archive chunk")?;
        }
        file.flush().await.context("flush parquet archive")?;
        drop(file);
        fs::rename(&tmp_path, path)
            .await
            .with_context(|| format!("move archive into place {}", path.display()))?;
        Ok(())
    }

    pub async fn execute(&self, sql: &str) -> Result<()> {
        self.post_sql(sql).await?;
        Ok(())
    }

    pub async fn delete_archived_range(
        &self,
        tables: &DbTables,
        start_slot: u64,
        end_slot: u64,
    ) -> Result<()> {
        for sql in build_delete_sql(tables, start_slot, end_slot) {
            self.execute(&sql).await?;
        }
        Ok(())
    }

    pub async fn delete_archived_range_for_tables(
        &self,
        tables: &[ArchiveDbTable],
        start_slot: u64,
        end_slot: u64,
    ) -> Result<()> {
        for table in tables {
            self.execute(&build_delete_table_sql(
                &table.table_name,
                start_slot,
                end_slot,
            ))
            .await?;
        }
        Ok(())
    }

    /// Force ClickHouse to merge and deduplicate a single epoch partition of the
    /// transactions table, collapsing duplicate `(slot, slot_idx, signature)`
    /// rows that inflate transaction-count validation. Must target the shard-local
    /// table (OPTIMIZE is unsupported on Distributed tables); pass `cluster` to
    /// fan the OPTIMIZE out across a clustered/replicated deployment.
    pub async fn deduplicate_partition(
        &self,
        table: &str,
        cluster: Option<&str>,
        partition: u64,
    ) -> Result<()> {
        let on_cluster = match cluster {
            Some(cluster) => format!(" ON CLUSTER `{cluster}`"),
            None => String::new(),
        };
        let sql =
            format!("OPTIMIZE TABLE {table}{on_cluster} PARTITION {partition} FINAL DEDUPLICATE");
        self.execute(&sql).await
    }

    /// Deduplicated row count for an archive table over `[start_slot, end_slot]`.
    ///
    /// Uses `FINAL` so the count reflects logically-distinct rows rather than a
    /// raw `count()` that varies with ReplacingMergeTree merge timing. See
    /// [`build_count_query`] for why this is stable and reproducible across
    /// independent deployments archiving the same slot range.
    pub async fn count_table_rows(
        &self,
        table: &ArchiveDbTable,
        start_slot: u64,
        end_slot: u64,
    ) -> Result<u64> {
        #[derive(Debug, Deserialize)]
        struct CountRow {
            rows: u64,
        }

        let sql = build_count_query(
            &table.table_name,
            start_slot,
            end_slot,
            &self.archive_settings,
        );
        let rows = self.query_json_rows::<CountRow>(&sql).await?;
        Ok(rows.into_iter().next().map(|row| row.rows).unwrap_or(0))
    }

    async fn fetch_block_slots(
        &self,
        blocks_table: &str,
        start_slot: u64,
        end_slot: u64,
    ) -> Result<Vec<u64>> {
        #[derive(Debug, Deserialize)]
        struct SlotRow {
            slot: u64,
        }

        let sql = format!(
            "SELECT slot FROM {blocks_table} WHERE slot BETWEEN {start_slot} AND {end_slot} ORDER BY slot"
        );
        Ok(self
            .query_json_rows::<SlotRow>(&sql)
            .await?
            .into_iter()
            .map(|row| row.slot)
            .collect())
    }

    async fn fetch_transaction_mismatches(
        &self,
        tables: &DbTables,
        start_slot: u64,
        end_slot: u64,
    ) -> Result<Vec<TransactionMismatch>> {
        // Count logically-distinct transaction rows per slot, matching the
        // ReplacingMergeTree dedup key (slot, slot_idx, signature). A raw count()
        // would over-report while duplicate rows from retries/re-ingest wait for a
        // background merge, producing false-positive overcounts. Transactions are
        // deduped in a subquery so the LEFT JOIN never turns "no rows" into 1, and
        // `max`/`any` collapse any duplicate block-metadata rows.
        let sql = format!(
            "SELECT b.slot AS slot, \
                    max(b.executed_transaction_count) AS expected, \
                    ifNull(any(tx.cnt), 0) AS actual \
             FROM {blocks} AS b \
             LEFT JOIN ( \
                 SELECT slot, uniqExact(slot_idx, signature) AS cnt \
                 FROM {transactions} \
                 WHERE slot BETWEEN {start_slot} AND {end_slot} \
                 GROUP BY slot \
             ) AS tx ON tx.slot = b.slot \
             WHERE b.slot BETWEEN {start_slot} AND {end_slot} \
             GROUP BY b.slot \
             HAVING expected != actual \
             ORDER BY slot \
             LIMIT 1000",
            blocks = tables.blocks_table,
            transactions = tables.transactions_table
        );
        self.query_json_rows::<TransactionMismatch>(&sql).await
    }

    /// Disk space ClickHouse reports for its own configured disks (`system.disks`).
    /// Reflects whichever node serves this HTTP request, so on a cluster this is
    /// per-node, not an aggregate across shards.
    pub async fn fetch_disk_usage(&self) -> Result<Vec<DiskUsage>> {
        #[derive(Debug, Deserialize)]
        struct DiskRow {
            name: String,
            path: String,
            free_space: u64,
            total_space: u64,
        }

        let rows = self
            .query_json_rows::<DiskRow>(
                "SELECT name, path, free_space, total_space FROM system.disks ORDER BY name",
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| DiskUsage {
                name: row.name,
                path: row.path,
                free_bytes: row.free_space,
                total_bytes: row.total_space,
            })
            .collect())
    }

    /// Disk-space footprint of each archive-relevant table (`system.tables`
    /// `total_bytes`/`total_rows`). Only returns entries for tables that exist;
    /// missing optional tables are silently omitted, matching `check_tables`.
    pub async fn fetch_table_sizes(&self, tables: &[ArchiveDbTable]) -> Result<Vec<TableSize>> {
        #[derive(Debug, Deserialize)]
        struct SizeRow {
            name: String,
            total_bytes: Option<u64>,
            total_rows: Option<u64>,
        }

        let rows = self
            .query_json_rows::<SizeRow>(
                "SELECT name, total_bytes, total_rows FROM system.tables WHERE database = currentDatabase()",
            )
            .await?;
        Ok(tables
            .iter()
            .filter_map(|table| {
                rows.iter()
                    .find(|row| row.name == table.table_name)
                    .map(|row| TableSize {
                        kind: table.kind,
                        table_name: table.table_name.clone(),
                        bytes: row.total_bytes.unwrap_or(0),
                        rows: row.total_rows.unwrap_or(0),
                    })
            })
            .collect())
    }

    async fn query_json_rows<T>(&self, sql: &str) -> Result<Vec<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        let response = self.post_sql(&format!("{sql} FORMAT JSONEachRow")).await?;
        let text = response.text().await.context("read ClickHouse response")?;
        text.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                serde_json::from_str::<T>(line).with_context(|| format!("parse row {line}"))
            })
            .collect()
    }

    async fn post_sql(&self, sql: &str) -> Result<reqwest::Response> {
        let mut request = self
            .http
            .post(&self.url)
            .query(&[("database", self.database.as_str())])
            .body(sql.to_string());
        if !self.user.is_empty() {
            request = request.basic_auth(&self.user, Some(&self.password));
        }
        let response = request
            .send()
            .await
            .with_context(|| format!("send ClickHouse query to {}", self.url))?;
        if response.status().is_success() {
            Ok(response)
        } else {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<failed to read response body>".to_string());
            Err(clickhouse_status_error(status, body, sql))
        }
    }
}

/// Disk space for a single ClickHouse-managed disk, as reported by `system.disks`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DiskUsage {
    pub name: String,
    pub path: String,
    pub free_bytes: u64,
    pub total_bytes: u64,
}

impl DiskUsage {
    pub fn used_bytes(&self) -> u64 {
        self.total_bytes.saturating_sub(self.free_bytes)
    }
}

/// Disk-space footprint of a single archive-relevant table, as reported by
/// `system.tables`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TableSize {
    pub kind: ArchiveTableKind,
    pub table_name: String,
    pub bytes: u64,
    pub rows: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SlotRange {
    pub start_slot: u64,
    pub end_slot: u64,
    pub slot_count: u64,
}

impl SlotRange {
    pub fn new(start_slot: u64, end_slot: u64) -> Self {
        Self {
            start_slot,
            end_slot,
            slot_count: end_slot.saturating_sub(start_slot).saturating_add(1),
        }
    }
}

impl fmt::Display for SlotRange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.start_slot == self.end_slot {
            write!(formatter, "{}", self.start_slot)
        } else {
            write!(formatter, "{}-{}", self.start_slot, self.end_slot)
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ValidationReport {
    pub start_slot: u64,
    pub end_slot: u64,
    pub db_block_slots: u64,
    pub rpc_produced_slots: u64,
    pub missing_blocks: Vec<u64>,
    pub missing_block_ranges: Vec<SlotRange>,
    pub not_produced_slot_ranges: Vec<SlotRange>,
    pub transaction_mismatches: Vec<TransactionMismatch>,
    pub transaction_mismatch_ranges: Vec<SlotRange>,
    /// Slots with fewer archived rows than declared — need re-ingestion.
    pub transaction_undercount_ranges: Vec<SlotRange>,
    /// Slots with more archived rows than declared — fixable by ClickHouse dedup.
    pub transaction_overcount_ranges: Vec<SlotRange>,
    pub rpc_check_error: Option<String>,
}

impl ValidationReport {
    pub fn from_observed_slots(
        start_slot: u64,
        end_slot: u64,
        db_block_slots: Vec<u64>,
        produced_slots: Vec<u64>,
        transaction_mismatches: Vec<TransactionMismatch>,
        rpc_check_error: Option<String>,
    ) -> Self {
        let db_block_slot_set: HashSet<u64> = db_block_slots.iter().copied().collect();
        let mut produced_slots = produced_slots;
        produced_slots.sort_unstable();
        produced_slots.dedup();
        let missing_blocks = produced_slots
            .iter()
            .copied()
            .filter(|slot| !db_block_slot_set.contains(slot))
            .collect::<Vec<_>>();
        let mismatch_slots = transaction_mismatches
            .iter()
            .map(|mismatch| mismatch.slot)
            .collect::<Vec<_>>();
        let undercount_slots = transaction_mismatches
            .iter()
            .filter(|mismatch| mismatch.direction() == MismatchDirection::Undercount)
            .map(|mismatch| mismatch.slot)
            .collect::<Vec<_>>();
        let overcount_slots = transaction_mismatches
            .iter()
            .filter(|mismatch| mismatch.direction() == MismatchDirection::Overcount)
            .map(|mismatch| mismatch.slot)
            .collect::<Vec<_>>();
        let not_produced_slot_ranges = if rpc_check_error.is_none() {
            missing_ranges(start_slot, end_slot, &produced_slots)
        } else {
            Vec::new()
        };

        Self {
            start_slot,
            end_slot,
            db_block_slots: db_block_slots.len() as u64,
            rpc_produced_slots: produced_slots.len() as u64,
            missing_block_ranges: slot_ranges(&missing_blocks),
            not_produced_slot_ranges,
            transaction_mismatch_ranges: slot_ranges(&mismatch_slots),
            transaction_undercount_ranges: slot_ranges(&undercount_slots),
            transaction_overcount_ranges: slot_ranges(&overcount_slots),
            missing_blocks,
            transaction_mismatches,
            rpc_check_error,
        }
    }

    pub fn has_warnings(&self) -> bool {
        !self.missing_blocks.is_empty()
            || !self.transaction_mismatches.is_empty()
            || self.rpc_check_error.is_some()
    }

    /// Verified data problems that always block archiving: blocks Solana
    /// produced but we are missing, or transaction-count mismatches. Excludes
    /// the RPC cross-check *failure*, which is a "couldn't verify" condition
    /// rather than a detected gap.
    pub fn has_data_warnings(&self) -> bool {
        !self.missing_blocks.is_empty() || !self.transaction_mismatches.is_empty()
    }

    /// Whether validation results should block archiving. When
    /// `allow_rpc_failure` is set, an RPC cross-check failure alone does not
    /// block — only verified data problems do.
    pub fn blocks_archive(&self, allow_rpc_failure: bool) -> bool {
        if allow_rpc_failure {
            self.has_data_warnings()
        } else {
            self.has_warnings()
        }
    }

    pub fn has_known_data_gaps(&self) -> bool {
        !self.missing_block_ranges.is_empty()
            || !self.not_produced_slot_ranges.is_empty()
            || !self.transaction_mismatch_ranges.is_empty()
    }

    pub fn format_ranges(ranges: &[SlotRange]) -> String {
        if ranges.is_empty() {
            "none".to_string()
        } else {
            ranges
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",")
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TransactionMismatch {
    pub slot: u64,
    pub expected: u64,
    pub actual: u64,
}

/// Direction of a transaction-count mismatch, which decides how it can be
/// repaired: an overcount is duplicate rows (fixable by ClickHouse dedup),
/// an undercount is missing rows (needs re-ingestion from a source of truth).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum MismatchDirection {
    Undercount,
    Overcount,
}

impl MismatchDirection {
    pub fn as_str(self) -> &'static str {
        match self {
            MismatchDirection::Undercount => "undercount",
            MismatchDirection::Overcount => "overcount",
        }
    }
}

impl TransactionMismatch {
    /// Fewer archived rows than the block declares is an undercount; more is an
    /// overcount. The detection query only reports slots where the two differ.
    pub fn direction(&self) -> MismatchDirection {
        if self.actual < self.expected {
            MismatchDirection::Undercount
        } else {
            MismatchDirection::Overcount
        }
    }

    /// Signed row delta (`actual - expected`).
    pub fn delta(&self) -> i64 {
        i64::try_from(self.actual).unwrap_or(i64::MAX)
            - i64::try_from(self.expected).unwrap_or(i64::MAX)
    }
}

fn slot_ranges(slots: &[u64]) -> Vec<SlotRange> {
    if slots.is_empty() {
        return Vec::new();
    }
    let mut slots = slots.to_vec();
    slots.sort_unstable();
    slots.dedup();

    let mut ranges = Vec::new();
    let mut start = slots[0];
    let mut prev = slots[0];
    for slot in slots.into_iter().skip(1) {
        if slot == prev.saturating_add(1) {
            prev = slot;
        } else {
            ranges.push(SlotRange::new(start, prev));
            start = slot;
            prev = slot;
        }
    }
    ranges.push(SlotRange::new(start, prev));
    ranges
}

fn missing_ranges(start_slot: u64, end_slot: u64, present_slots: &[u64]) -> Vec<SlotRange> {
    if end_slot < start_slot {
        return Vec::new();
    }
    let present = present_slots.iter().copied().collect::<HashSet<_>>();
    let mut missing = Vec::new();
    let mut cursor = start_slot;
    while cursor <= end_slot {
        if !present.contains(&cursor) {
            missing.push(cursor);
        }
        if cursor == u64::MAX {
            break;
        }
        cursor += 1;
    }
    slot_ranges(&missing)
}

pub fn build_local_parquet_query(
    transactions_table: &str,
    start_slot: u64,
    end_slot: u64,
    settings: &str,
    dedup: bool,
) -> String {
    format!(
        "SELECT * FROM {transactions_table}{dedup} WHERE slot BETWEEN {start_slot} AND {end_slot} ORDER BY slot, slot_idx, signature{settings} FORMAT Parquet",
        dedup = final_suffix(dedup),
        settings = settings_suffix(settings, " ")
    )
}

/// Build a deduplicated row-count query for a ReplacingMergeTree archive table.
///
/// The archive tables are all `ReplacingMergeTree`, so retries and re-ingestion
/// leave duplicate physical rows that only collapse once an asynchronous
/// background merge runs. A raw `count()` therefore over-reports by however many
/// duplicates happen to be un-merged at query time — a value that drifts with
/// merge scheduling, so two independent deployments archiving the identical slot
/// range can report different counts (e.g. the `gsfa` mismatch between the US and
/// EU epoch_1001 archives). `FINAL` applies the ReplacingMergeTree collapse at
/// query time via a streaming merge on each table's sorting key, yielding the
/// logically-distinct count. It is memory-light (unlike a large `uniqExact` over
/// billions of rows) and needs no per-table key config. On clustered
/// deployments the archive reads the `Distributed` surface; every table's shard
/// key is a prefix-compatible function of its sorting key (epoch for
/// slot-keyed tables, `cityHash64(address|signature)` for the bucketed indexes),
/// so rows sharing a dedup key co-locate on one shard and per-shard `FINAL` is
/// globally correct.
///
/// Note: when the export query ([`build_local_table_parquet_query`] /
/// [`build_s3_table_archive_sql`]) runs without `FINAL` (dedup export disabled),
/// it writes raw rows, so this count can be below the Parquet file's physical
/// row count; the restore side treats `row_count` as informational and never
/// gates on it. With dedup export enabled the file and this count agree.
pub fn build_count_query(
    table_name: &str,
    start_slot: u64,
    end_slot: u64,
    settings: &str,
) -> String {
    format!(
        "SELECT count() AS rows FROM {table_name} FINAL WHERE slot BETWEEN {start_slot} AND {end_slot}{settings}",
        settings = settings_suffix(settings, " ")
    )
}

pub fn build_local_table_parquet_query(
    table: &ArchiveDbTable,
    start_slot: u64,
    end_slot: u64,
    settings: &str,
    dedup: bool,
) -> String {
    format!(
        "SELECT * FROM {table_name}{dedup} WHERE slot BETWEEN {start_slot} AND {end_slot} ORDER BY {order_by}{settings} FORMAT Parquet",
        table_name = table.table_name,
        dedup = final_suffix(dedup),
        order_by = table.order_by,
        settings = settings_suffix(settings, " ")
    )
}

#[derive(Debug, Clone, Copy)]
pub struct S3ArchiveSql<'a> {
    pub table: &'a ArchiveDbTable,
    pub start_slot: u64,
    pub end_slot: u64,
    pub endpoint: &'a str,
    pub bucket: &'a str,
    pub bucket_path: &'a str,
    pub archive_name: &'a str,
    pub access_key: &'a str,
    pub secret_key: &'a str,
    /// Raw ClickHouse `SETTINGS` clause body appended to the export query to
    /// bound memory (see [`Config::clickhouse_archive_settings`]). Empty omits
    /// the clause.
    pub settings: &'a str,
    /// Export logically-distinct rows by adding `FINAL` to the `SELECT`, so the
    /// Parquet file matches the deduplicated manifest `row_count`. See
    /// [`build_count_query`] for the dedup semantics.
    pub dedup: bool,
}

pub fn build_s3_archive_sql(params: S3ArchiveSql<'_>) -> String {
    build_s3_table_archive_sql(params)
}

pub fn build_s3_table_archive_sql(params: S3ArchiveSql<'_>) -> String {
    let url = join_s3_url(
        params.endpoint,
        params.bucket,
        params.bucket_path,
        &format!("{}/{}", params.archive_name, params.table.file_name()),
    );
    format!(
        "INSERT INTO FUNCTION s3(\n  '{}',\n  '{}',\n  '{}',\n  'Parquet'\n)\nSELECT *\nFROM {table_name}{dedup}\nWHERE slot BETWEEN {start_slot} AND {end_slot}\nORDER BY {order_by}{settings}",
        escape_sql_string(&url),
        escape_sql_string(params.access_key),
        escape_sql_string(params.secret_key),
        table_name = params.table.table_name,
        dedup = final_suffix(params.dedup),
        start_slot = params.start_slot,
        end_slot = params.end_slot,
        order_by = params.table.order_by,
        settings = settings_suffix(params.settings, "\n"),
    )
}

/// Render the ` FINAL` modifier when a deduplicated export is requested, or an
/// empty string otherwise. `FINAL` makes ClickHouse collapse ReplacingMergeTree
/// duplicates at query time so the exported Parquet holds logically-distinct
/// rows (see [`build_count_query`]).
fn final_suffix(dedup: bool) -> &'static str {
    if dedup { " FINAL" } else { "" }
}

/// Render a `SETTINGS` clause suffix, or an empty string when no settings are
/// configured. `separator` precedes the clause (a space for single-line local
/// queries, a newline for the multi-line S3 statement).
fn settings_suffix(settings: &str, separator: &str) -> String {
    let settings = settings.trim();
    if settings.is_empty() {
        String::new()
    } else {
        format!("{separator}SETTINGS {settings}")
    }
}

pub fn build_delete_sql(tables: &DbTables, start_slot: u64, end_slot: u64) -> Vec<String> {
    tables
        .archive_tables()
        .into_iter()
        .map(|table| build_delete_table_sql(&table.table_name, start_slot, end_slot))
        .collect()
}

fn build_delete_table_sql(table: &str, start_slot: u64, end_slot: u64) -> String {
    format!("ALTER TABLE {table} DELETE WHERE slot BETWEEN {start_slot} AND {end_slot}")
}

async fn fetch_solana_produced_slots(
    rpc_url: &str,
    start_slot: u64,
    end_slot: u64,
) -> Result<Vec<u64>> {
    #[derive(Serialize)]
    struct RpcRequest<'a> {
        jsonrpc: &'a str,
        id: u64,
        method: &'a str,
        params: [u64; 2],
    }
    #[derive(Deserialize)]
    struct RpcResponse {
        result: Option<Vec<u64>>,
        error: Option<serde_json::Value>,
    }

    let client = reqwest::Client::new();
    let mut slots = Vec::new();
    let mut cursor = start_slot;
    while cursor <= end_slot {
        let chunk_end = end_slot.min(cursor.saturating_add(499_999));
        let response = client
            .post(rpc_url)
            .json(&RpcRequest {
                jsonrpc: "2.0",
                id: 1,
                method: "getBlocks",
                params: [cursor, chunk_end],
            })
            .send()
            .await
            .with_context(|| format!("query Solana RPC getBlocks {cursor}-{chunk_end}"))?;
        let parsed = response
            .json::<RpcResponse>()
            .await
            .context("parse Solana RPC getBlocks response")?;
        if let Some(error) = parsed.error {
            return Err(anyhow!("Solana RPC getBlocks error: {error}"));
        }
        slots.extend(parsed.result.unwrap_or_default());
        if chunk_end == u64::MAX {
            break;
        }
        cursor = chunk_end + 1;
    }
    Ok(slots)
}

/// Query the current Solana network tip via `getSlot` at the given commitment.
///
/// Used to expose the end-to-end chain-tip lag metric (how far the archived
/// ClickHouse data trails the live chain). Errors are the caller's to handle;
/// solparq treats a missing tip as "unknown" rather than failing the run.
pub async fn fetch_solana_tip_slot(rpc_url: &str) -> Result<u64> {
    #[derive(Serialize)]
    struct RpcRequest<'a> {
        jsonrpc: &'a str,
        id: u64,
        method: &'a str,
        params: [serde_json::Value; 1],
    }
    #[derive(Deserialize)]
    struct RpcResponse {
        result: Option<u64>,
        error: Option<serde_json::Value>,
    }

    let client = reqwest::Client::new();
    let response = client
        .post(rpc_url)
        .json(&RpcRequest {
            jsonrpc: "2.0",
            id: 1,
            method: "getSlot",
            params: [serde_json::json!({ "commitment": "finalized" })],
        })
        .send()
        .await
        .context("query Solana RPC getSlot")?;
    let parsed = response
        .json::<RpcResponse>()
        .await
        .context("parse Solana RPC getSlot response")?;
    if let Some(error) = parsed.error {
        return Err(anyhow!("Solana RPC getSlot error: {error}"));
    }
    parsed
        .result
        .ok_or_else(|| anyhow!("Solana RPC getSlot returned no result"))
}

fn join_s3_url(endpoint: &str, bucket: &str, bucket_path: &str, archive_name: &str) -> String {
    let endpoint = endpoint.trim_end_matches('/');
    let bucket = bucket.trim_matches('/');
    let bucket_path = bucket_path.trim_matches('/');
    if bucket_path.is_empty() {
        format!("{endpoint}/{bucket}/{archive_name}")
    } else {
        format!("{endpoint}/{bucket}/{bucket_path}/{archive_name}")
    }
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "\\'")
}

fn clickhouse_status_error(status: StatusCode, body: String, sql: &str) -> anyhow::Error {
    // Redact S3 credentials *before* truncating: the export SQL built by
    // `build_s3_table_archive_sql` embeds the access key and secret as the second
    // and third `s3(...)` arguments, and a failed export would otherwise leak both
    // in plaintext through this error (and any log line carrying it).
    let redacted = redact_s3_credentials(sql);
    let preview = if redacted.len() > 500 {
        // Slice on a char boundary — `redacted` may contain multi-byte UTF-8.
        let mut end = 500;
        while !redacted.is_char_boundary(end) {
            end -= 1;
        }
        &redacted[..end]
    } else {
        &redacted
    };
    anyhow!("ClickHouse query failed with status {status}: {body}; query: {preview}")
}

/// Replace the credential arguments of every ClickHouse `s3(...)` / `s3Cluster(...)`
/// table-function call in `sql` with `'[REDACTED]'`.
///
/// The archive export statement embeds the S3 access key and secret as the second
/// and third string arguments of the `s3(...)` function (see
/// [`build_s3_table_archive_sql`]); the URL (first arg) and format (fourth arg)
/// are non-sensitive and left intact for debugging. Literals are parsed with the
/// same `\'` / `\\` escaping that [`escape_sql_string`] emits, so an embedded
/// quote in a credential cannot prematurely end the redacted span.
fn redact_s3_credentials(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    while i < bytes.len() {
        if let Some(call_len) = match_s3_call(bytes, i) {
            out.push_str(&sql[i..i + call_len]);
            i = redact_s3_arg_list(sql, bytes, i + call_len, &mut out);
        } else {
            // Copy verbatim up to the next byte that could begin an `s3(` call.
            // `s`/`S` are single-byte ASCII, so this never splits a UTF-8 char.
            let start = i;
            i += 1;
            while i < bytes.len() && bytes[i] != b's' && bytes[i] != b'S' {
                i += 1;
            }
            out.push_str(&sql[start..i]);
        }
    }
    out
}

/// If `bytes[i..]` begins an `s3(` or `s3Cluster(` call at a word boundary, return
/// the length of the matched `<name>(` prefix; otherwise `None`. Matching is
/// case-insensitive on the function name.
fn match_s3_call(bytes: &[u8], i: usize) -> Option<usize> {
    if i > 0 {
        let prev = bytes[i - 1];
        if prev == b'_' || prev.is_ascii_alphanumeric() {
            return None;
        }
    }
    for keyword in [b"s3cluster(".as_slice(), b"s3(".as_slice()] {
        let end = i + keyword.len();
        if end <= bytes.len() && bytes[i..end].eq_ignore_ascii_case(keyword) {
            return Some(keyword.len());
        }
    }
    None
}

/// Walk the argument list of an `s3(...)` call starting at `i` (just past the
/// opening paren), appending each argument to `out` and replacing the second and
/// third string literals (access key, secret key) with `'[REDACTED]'`. Returns the
/// index just past the closing paren (or end of input if unterminated).
fn redact_s3_arg_list(sql: &str, bytes: &[u8], mut i: usize, out: &mut String) -> usize {
    let mut literal_index = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b')' => {
                out.push(')');
                return i + 1;
            }
            b'\'' => {
                let start = i;
                i += 1;
                while i < bytes.len() {
                    match bytes[i] {
                        b'\\' if i + 1 < bytes.len() => i += 2,
                        b'\'' => {
                            i += 1;
                            break;
                        }
                        _ => i += 1,
                    }
                }
                if literal_index == 1 || literal_index == 2 {
                    out.push_str("'[REDACTED]'");
                } else {
                    out.push_str(&sql[start..i]);
                }
                literal_index += 1;
            }
            _ => {
                // Non-literal argument text (whitespace, commas). Copy up to the
                // next quote or closing paren; neither is a UTF-8 continuation
                // byte, so slicing stays on char boundaries.
                let start = i;
                i += 1;
                while i < bytes.len() && bytes[i] != b'\'' && bytes[i] != b')' {
                    i += 1;
                }
                out.push_str(&sql[start..i]);
            }
        }
    }
    i
}

#[cfg(test)]
mod redaction_tests {
    use super::*;

    fn entries_table() -> ArchiveDbTable {
        DbTables {
            transactions_table: "transactions".to_string(),
            blocks_table: "blocks_metadata".to_string(),
            entries_table: "entries".to_string(),
            gsfa_table: "gsfa".to_string(),
            gsfa_hot_table: "gsfa_hot".to_string(),
            signatures_table: "signatures".to_string(),
            token_owner_activity_table: "token_owner_activity".to_string(),
        }
        .archive_tables()
        .into_iter()
        .find(|table| table.kind.as_str() == "entries")
        .expect("entries table")
    }

    #[test]
    fn redacts_credentials_in_generated_archive_sql() {
        let table = entries_table();
        let sql = build_s3_table_archive_sql(S3ArchiveSql {
            table: &table,
            start_slot: 42,
            end_slot: 84,
            endpoint: "https://s3.us-west.example",
            bucket: "bucket",
            bucket_path: "prefix/hourly",
            archive_name: "hourly_0_42-84",
            access_key: "AKIAIOSFODNN7EXAMPLE",
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            settings: "max_threads=4",
            dedup: true,
        });

        let redacted = redact_s3_credentials(&sql);

        assert!(
            !redacted.contains("AKIAIOSFODNN7EXAMPLE"),
            "access key leaked: {redacted}"
        );
        assert!(
            !redacted.contains("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"),
            "secret key leaked: {redacted}"
        );
        assert_eq!(redacted.matches("'[REDACTED]'").count(), 2);
        // URL (first arg) and format (fourth arg) stay intact for debugging.
        assert!(redacted.contains(
            "'https://s3.us-west.example/bucket/prefix/hourly/hourly_0_42-84/entries.parquet'"
        ));
        assert!(redacted.contains("'Parquet'"));
        assert!(redacted.contains("WHERE slot BETWEEN 42 AND 84"));
    }

    #[test]
    fn status_error_preview_never_contains_credentials() {
        let table = entries_table();
        let sql = build_s3_table_archive_sql(S3ArchiveSql {
            table: &table,
            start_slot: 42,
            end_slot: 84,
            endpoint: "https://s3.us-west.example",
            bucket: "bucket",
            bucket_path: "prefix/hourly",
            archive_name: "hourly_0_42-84",
            access_key: "AKIAIOSFODNN7EXAMPLE",
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            settings: "",
            dedup: false,
        });

        let err = clickhouse_status_error(StatusCode::BAD_REQUEST, "bad bucket".to_string(), &sql)
            .to_string();

        assert!(
            !err.contains("AKIAIOSFODNN7EXAMPLE"),
            "access key leaked: {err}"
        );
        assert!(
            !err.contains("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"),
            "secret key leaked: {err}"
        );
        assert!(err.contains("[REDACTED]"));
    }

    #[test]
    fn redacts_single_line_restore_style_call() {
        // Mirrors the restore statement form used by the ingestor.
        let sql = "INSERT INTO db.transactions SELECT * FROM \
             s3('https://s3.example/bucket/x.parquet', 'AKIA_KEY', 'SUPER_SECRET', 'Parquet') \
             WHERE slot BETWEEN 1 AND 2";

        let redacted = redact_s3_credentials(sql);

        assert!(!redacted.contains("AKIA_KEY"));
        assert!(!redacted.contains("SUPER_SECRET"));
        assert_eq!(redacted.matches("'[REDACTED]'").count(), 2);
        assert!(redacted.contains("'https://s3.example/bucket/x.parquet'"));
        assert!(redacted.contains("'Parquet'"));
    }

    #[test]
    fn redaction_respects_escaped_quotes_in_secret() {
        // A secret containing an escaped quote must not let the closing quote of
        // the redacted span land early and expose trailing credential bytes.
        let sql = "s3('url', 'ke\\'y', 'sec\\'ret', 'Parquet')";

        let redacted = redact_s3_credentials(sql);

        assert!(!redacted.contains("ke\\'y"));
        assert!(!redacted.contains("sec\\'ret"));
        assert_eq!(redacted, "s3('url', '[REDACTED]', '[REDACTED]', 'Parquet')");
    }

    #[test]
    fn leaves_non_s3_sql_untouched() {
        let sql = "SELECT count() FROM transactions WHERE slot BETWEEN 1 AND 2";
        assert_eq!(redact_s3_credentials(sql), sql);
    }

    #[test]
    fn does_not_match_s3_inside_identifiers() {
        // A column/table whose name merely contains `s3(`-like text must not be
        // treated as an s3() call.
        let sql = "SELECT hs3(col) FROM t";
        assert_eq!(redact_s3_credentials(sql), sql);
    }
}
