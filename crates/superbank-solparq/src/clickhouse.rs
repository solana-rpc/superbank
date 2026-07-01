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
        }

        let sql = format!(
            "SELECT minOrNull(slot) AS earliest_slot, maxOrNull(slot) AS latest_slot FROM {transactions_table}"
        );
        let rows = self.query_json_rows::<BoundsRow>(&sql).await?;
        let Some(row) = rows.into_iter().next() else {
            return Ok(None);
        };
        match (row.earliest_slot, row.latest_slot) {
            (Some(earliest_slot), Some(latest_slot)) => Ok(Some(ClickHouseBounds {
                earliest_slot,
                latest_slot,
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
        let query = build_local_parquet_query(transactions_table, start_slot, end_slot);
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
        let query = build_local_table_parquet_query(table, start_slot, end_slot);
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

        let sql = format!(
            "SELECT count() AS rows FROM {} WHERE slot BETWEEN {start_slot} AND {end_slot}",
            table.table_name
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
        let sql = format!(
            "SELECT b.slot AS slot, b.executed_transaction_count AS expected, count(t.slot_idx) AS actual \
             FROM {blocks} AS b \
             LEFT JOIN {transactions} AS t ON t.slot = b.slot \
             WHERE b.slot BETWEEN {start_slot} AND {end_slot} \
             GROUP BY b.slot, b.executed_transaction_count \
             HAVING expected != actual \
             ORDER BY b.slot \
             LIMIT 1000",
            blocks = tables.blocks_table,
            transactions = tables.transactions_table
        );
        self.query_json_rows::<TransactionMismatch>(&sql).await
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
) -> String {
    format!(
        "SELECT * FROM {transactions_table} WHERE slot BETWEEN {start_slot} AND {end_slot} ORDER BY slot, slot_idx, signature FORMAT Parquet"
    )
}

pub fn build_local_table_parquet_query(
    table: &ArchiveDbTable,
    start_slot: u64,
    end_slot: u64,
) -> String {
    format!(
        "SELECT * FROM {table_name} WHERE slot BETWEEN {start_slot} AND {end_slot} ORDER BY {order_by} FORMAT Parquet",
        table_name = table.table_name,
        order_by = table.order_by
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
        "INSERT INTO FUNCTION s3(\n  '{}',\n  '{}',\n  '{}',\n  'Parquet'\n)\nSELECT *\nFROM {table_name}\nWHERE slot BETWEEN {start_slot} AND {end_slot}\nORDER BY {order_by}",
        escape_sql_string(&url),
        escape_sql_string(params.access_key),
        escape_sql_string(params.secret_key),
        table_name = params.table.table_name,
        start_slot = params.start_slot,
        end_slot = params.end_slot,
        order_by = params.table.order_by,
    )
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
    let preview = if sql.len() > 500 { &sql[..500] } else { sql };
    anyhow!("ClickHouse query failed with status {status}: {body}; query: {preview}")
}
