use std::{collections::HashSet, path::Path};

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
    pub gsfa_table: String,
    pub signatures_table: String,
}

impl DbTables {
    pub fn from_config(config: &Config) -> Self {
        Self {
            transactions_table: config.transactions_table.clone(),
            blocks_table: config.blocks_table.clone(),
            gsfa_table: config.gsfa_table.clone(),
            signatures_table: config.signatures_table.clone(),
        }
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

    pub async fn check_tables(
        &self,
        tables: &DbTables,
        require_cleanup_tables: bool,
    ) -> Result<()> {
        let mut required_tables = vec![&tables.transactions_table, &tables.blocks_table];
        if require_cleanup_tables {
            required_tables.push(&tables.gsfa_table);
            required_tables.push(&tables.signatures_table);
        }
        for table in required_tables {
            let sql = format!("SELECT count() FROM {table} WHERE 0");
            self.execute(&sql)
                .await
                .with_context(|| format!("check ClickHouse table {table}"))?;
        }
        Ok(())
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
        let db_block_slot_set: HashSet<u64> = db_block_slots.iter().copied().collect();
        let (produced_slots, rpc_check_error) =
            match fetch_solana_produced_slots(solana_rpc_url, start_slot, end_slot).await {
                Ok(slots) => (slots, None),
                Err(err) => (Vec::new(), Some(err.to_string())),
            };
        let missing_blocks = produced_slots
            .iter()
            .copied()
            .filter(|slot| !db_block_slot_set.contains(slot))
            .collect();

        let transaction_mismatches = self
            .fetch_transaction_mismatches(tables, start_slot, end_slot)
            .await?;

        Ok(ValidationReport {
            start_slot,
            end_slot,
            db_block_slots: db_block_slots.len() as u64,
            rpc_produced_slots: produced_slots.len() as u64,
            missing_blocks,
            transaction_mismatches,
            rpc_check_error,
        })
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
        for sql in build_delete_sql(
            &tables.transactions_table,
            &tables.blocks_table,
            &tables.gsfa_table,
            &tables.signatures_table,
            start_slot,
            end_slot,
        ) {
            self.execute(&sql).await?;
        }
        Ok(())
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

#[derive(Debug, Clone, Serialize)]
pub struct ValidationReport {
    pub start_slot: u64,
    pub end_slot: u64,
    pub db_block_slots: u64,
    pub rpc_produced_slots: u64,
    pub missing_blocks: Vec<u64>,
    pub transaction_mismatches: Vec<TransactionMismatch>,
    pub rpc_check_error: Option<String>,
}

impl ValidationReport {
    pub fn has_warnings(&self) -> bool {
        !self.missing_blocks.is_empty()
            || !self.transaction_mismatches.is_empty()
            || self.rpc_check_error.is_some()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TransactionMismatch {
    pub slot: u64,
    pub expected: u64,
    pub actual: u64,
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

#[derive(Debug, Clone, Copy)]
pub struct S3ArchiveSql<'a> {
    pub transactions_table: &'a str,
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
    let url = join_s3_url(
        params.endpoint,
        params.bucket,
        params.bucket_path,
        params.archive_name,
    );
    format!(
        "INSERT INTO FUNCTION s3(\n  '{}',\n  '{}',\n  '{}',\n  'Parquet'\n)\nSELECT *\nFROM {transactions_table}\nWHERE slot BETWEEN {start_slot} AND {end_slot}\nORDER BY slot, slot_idx, signature",
        escape_sql_string(&url),
        escape_sql_string(params.access_key),
        escape_sql_string(params.secret_key),
        transactions_table = params.transactions_table,
        start_slot = params.start_slot,
        end_slot = params.end_slot
    )
}

pub fn build_delete_sql(
    transactions_table: &str,
    blocks_table: &str,
    gsfa_table: &str,
    signatures_table: &str,
    start_slot: u64,
    end_slot: u64,
) -> Vec<String> {
    [
        transactions_table,
        blocks_table,
        gsfa_table,
        signatures_table,
    ]
    .into_iter()
    .map(|table| {
        format!("ALTER TABLE {table} DELETE WHERE slot BETWEEN {start_slot} AND {end_slot}")
    })
    .collect()
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
