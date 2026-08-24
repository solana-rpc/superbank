// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Source-schema inspection and local ClickHouse cache DDL.

use std::fmt::Write as _;

use clickhouse::Row;
use serde::Deserialize;

use crate::clickhouse::ClickHouseClient;

pub(crate) const CACHE_FORMAT_VERSION: u32 = 4;
pub(crate) const CACHE_MAGIC: &str = "superbank-clickhouse-forward-cache";
pub(crate) const META_TABLE: &str = "_cache_meta";
pub(crate) const COVERAGE_TABLE: &str = "_cache_coverage";
pub(crate) const RUNTIME_TABLE: &str = "_cache_runtime";

#[derive(Debug, thiserror::Error)]
pub(crate) enum SchemaError {
    #[error("clickhouse schema query failed: {0}")]
    Query(String),
    #[error("invalid cache schema: {0}")]
    Invalid(String),
    #[error("cache database ownership check failed: {0}")]
    Ownership(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CacheTableKind {
    Transactions,
    BlocksMetadata,
    Signatures,
    Gsfa,
    GsfaHot,
    TokenOwnerActivity,
}

impl CacheTableKind {
    pub(crate) fn local_name(self) -> &'static str {
        match self {
            Self::Transactions => "transactions",
            Self::BlocksMetadata => "blocks_metadata",
            Self::Signatures => "signatures",
            Self::Gsfa => "gsfa",
            Self::GsfaHot => "gsfa_hot",
            Self::TokenOwnerActivity => "token_owner_activity",
        }
    }

    fn is_derived(self) -> bool {
        !matches!(self, Self::Transactions | Self::BlocksMetadata)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CacheSchemaConfig {
    pub(crate) database: String,
    pub(crate) partition_slots: u64,
    pub(crate) memory_blocks_metadata: bool,
    pub(crate) memory_retain_slots: Option<u64>,
    pub(crate) memory_max_bytes: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Row)]
struct ColumnRow {
    name: String,
    #[serde(rename = "type")]
    type_name: String,
    #[serde(default)]
    default_kind: String,
    #[serde(default)]
    default_expression: String,
    #[serde(default)]
    compression_codec: String,
}

#[derive(Debug, Clone, Deserialize, Row)]
struct TableRow {
    uuid: String,
    engine: String,
    #[serde(default)]
    partition_key: String,
    #[serde(default)]
    sorting_key: String,
    #[serde(default)]
    primary_key: String,
    #[serde(default)]
    create_table_query: String,
    #[serde(default)]
    as_select: String,
}

#[derive(Debug, Clone, Deserialize, Row)]
struct NameRow {
    name: String,
}

#[derive(Debug, Clone, Deserialize, Row)]
struct MetaRow {
    key: String,
    value: String,
}

#[derive(Debug, Clone, Deserialize, Row)]
struct CountRow {
    count: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct SourceTableSchema {
    pub(crate) kind: CacheTableKind,
    pub(crate) logical_name: String,
    pub(crate) storage_name: String,
    columns: Vec<ColumnRow>,
    storage: TableRow,
    view_select: Option<String>,
    indexes: Vec<String>,
}

impl SourceTableSchema {
    pub(crate) fn insert_columns(&self) -> Vec<String> {
        self.columns
            .iter()
            .filter(|column| {
                !matches!(
                    column.default_kind.to_ascii_uppercase().as_str(),
                    "MATERIALIZED" | "ALIAS" | "EPHEMERAL"
                )
            })
            .map(|column| column.name.clone())
            .collect()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SourceSchemaSnapshot {
    pub(crate) tables: Vec<SourceTableSchema>,
    pub(crate) fingerprint: String,
}

impl SourceSchemaSnapshot {
    pub(crate) fn table(&self, kind: CacheTableKind) -> Option<&SourceTableSchema> {
        self.tables.iter().find(|table| table.kind == kind)
    }

    pub(crate) fn has_table(&self, kind: CacheTableKind) -> bool {
        self.table(kind).is_some()
    }
}

fn split_table_reference<'a>(default_database: &'a str, table: &'a str) -> (&'a str, &'a str) {
    let mut parts = table.splitn(2, '.');
    let first = parts.next().unwrap_or(table).trim_matches('`');
    match parts.next() {
        Some(second) => (first, second.trim_matches('`')),
        None => (default_database, first),
    }
}

fn local_table_candidate(table: &str) -> String {
    match table.rsplit_once('.') {
        Some((database, name)) => format!("{database}.{name}_local"),
        None => format!("{table}_local"),
    }
}

fn validate_identifier(value: &str, label: &str) -> Result<(), SchemaError> {
    let mut chars = value.chars();
    let valid_first = chars
        .next()
        .is_some_and(|ch| ch == '_' || ch.is_ascii_alphabetic());
    if !valid_first || !chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric()) {
        return Err(SchemaError::Invalid(format!(
            "{label} must contain only ASCII letters, digits, and underscores and must not start with a digit"
        )));
    }
    Ok(())
}

fn quote_identifier(value: &str) -> String {
    format!("`{value}`")
}

fn quote_table(database: &str, table: &str) -> String {
    format!("{}.`{table}`", quote_identifier(database))
}

async fn fetch_table(
    source: &ClickHouseClient,
    table: &str,
) -> Result<Option<(TableRow, Vec<ColumnRow>)>, SchemaError> {
    let (database, name) = split_table_reference(&source.database, table);
    let database = database.replace('\'', "''");
    let name = name.replace('\'', "''");
    let table_query = format!(
        "SELECT toString(uuid) AS uuid, engine, partition_key, sorting_key, primary_key, create_table_query, as_select \
         FROM system.tables WHERE database = '{database}' AND name = '{name}' LIMIT 1"
    );
    let row = source
        .client
        .query(&table_query)
        .fetch_optional::<TableRow>()
        .await
        .map_err(|err| SchemaError::Query(err.to_string()))?;
    let Some(row) = row else {
        return Ok(None);
    };

    let columns_query = format!(
        "SELECT name, type, default_kind, default_expression, compression_codec \
         FROM system.columns WHERE database = '{database}' AND table = '{name}' ORDER BY position"
    );
    let columns = source
        .client
        .query(&columns_query)
        .fetch_all::<ColumnRow>()
        .await
        .map_err(|err| SchemaError::Query(err.to_string()))?;
    if columns.is_empty() {
        return Err(SchemaError::Invalid(format!(
            "source table {table} has no visible columns"
        )));
    }
    Ok(Some((row, columns)))
}

async fn inspect_table(
    source: &ClickHouseClient,
    kind: CacheTableKind,
    logical_name: String,
    storage_candidate: Option<String>,
) -> Result<SourceTableSchema, SchemaError> {
    let (logical, logical_columns) =
        fetch_table(source, &logical_name).await?.ok_or_else(|| {
            SchemaError::Invalid(format!("source table {logical_name} does not exist"))
        })?;

    let view_select = kind
        .is_derived()
        .then(|| logical.as_select.trim().trim_end_matches(';').to_string())
        .filter(|query| !query.is_empty());

    let needs_backing_table = logical.sorting_key.trim().is_empty()
        || logical.engine.eq_ignore_ascii_case("Distributed")
        || logical.engine.eq_ignore_ascii_case("MaterializedView");
    let candidate = storage_candidate.unwrap_or_else(|| local_table_candidate(&logical_name));
    let (storage_name, storage, columns) = if needs_backing_table {
        let explicit = fetch_table(source, &candidate).await?;
        if let Some((storage, columns)) = explicit
            && !storage.sorting_key.trim().is_empty()
        {
            (candidate, storage, columns)
        } else if logical.engine.eq_ignore_ascii_case("MaterializedView")
            && !logical.uuid.is_empty()
        {
            let (database, _) = split_table_reference(&source.database, &logical_name);
            let inner = format!("{database}.`.inner_id.{}`", logical.uuid);
            match fetch_table(source, &inner).await? {
                Some((storage, columns)) if !storage.sorting_key.trim().is_empty() => {
                    (inner, storage, columns)
                }
                _ => {
                    return Err(SchemaError::Invalid(format!(
                        "cannot resolve MergeTree backing table for {logical_name}; tried {candidate} and the materialized-view inner table"
                    )));
                }
            }
        } else if !logical.sorting_key.trim().is_empty() {
            (logical_name.clone(), logical.clone(), logical_columns)
        } else {
            return Err(SchemaError::Invalid(format!(
                "cannot resolve MergeTree backing table for {logical_name}; tried {candidate}"
            )));
        }
    } else {
        (logical_name.clone(), logical.clone(), logical_columns)
    };

    if !columns.iter().any(|column| column.name == "slot") {
        return Err(SchemaError::Invalid(format!(
            "source table {storage_name} has no slot column"
        )));
    }
    if kind.is_derived() && view_select.is_none() {
        return Err(SchemaError::Invalid(format!(
            "source query surface {logical_name} is not an inspectable materialized view"
        )));
    }

    let indexes = extract_index_definitions(&storage.create_table_query);
    Ok(SourceTableSchema {
        kind,
        logical_name,
        storage_name,
        columns,
        storage,
        view_select,
        indexes,
    })
}

pub(crate) async fn inspect_source_schema(
    source: &ClickHouseClient,
    config: &CacheSchemaConfig,
) -> Result<SourceSchemaSnapshot, SchemaError> {
    validate_identifier(&config.database, "cache database")?;

    let mut tables = Vec::new();
    tables.push(
        inspect_table(
            source,
            CacheTableKind::Transactions,
            source.transaction_table.clone(),
            source.transactions_local_table.clone(),
        )
        .await?,
    );
    tables.push(
        inspect_table(
            source,
            CacheTableKind::BlocksMetadata,
            source.blocks_metadata_table.clone(),
            source.blocks_metadata_local_table.clone(),
        )
        .await?,
    );
    tables.push(
        inspect_table(
            source,
            CacheTableKind::Signatures,
            source.signature_statuses_table.clone(),
            source.signatures_local_table.clone(),
        )
        .await?,
    );
    tables.push(
        inspect_table(
            source,
            CacheTableKind::Gsfa,
            source.gsfa_table.clone(),
            Some(local_table_candidate(&source.gsfa_table)),
        )
        .await?,
    );
    if source.token_owner_activity_available() {
        tables.push(
            inspect_table(
                source,
                CacheTableKind::TokenOwnerActivity,
                source.token_owner_activity_table.clone(),
                source.token_owner_activity_local_table.clone(),
            )
            .await?,
        );
    }
    if source.gsfa_hot_routing_configured() {
        tables.push(
            inspect_table(
                source,
                CacheTableKind::GsfaHot,
                source.gsfa_hot_table.clone(),
                Some(source.gsfa_hot_local_table.clone()),
            )
            .await?,
        );
    }

    let fingerprint = schema_fingerprint(&tables, config);
    Ok(SourceSchemaSnapshot {
        tables,
        fingerprint,
    })
}

fn schema_fingerprint(tables: &[SourceTableSchema], config: &CacheSchemaConfig) -> String {
    let mut input = format!(
        "format={CACHE_FORMAT_VERSION}\ndatabase={}\npartition_slots={}\nmemory_blocks={}\nmemory_retain={:?}\nmemory_bytes={:?}\n",
        config.database,
        config.partition_slots,
        config.memory_blocks_metadata,
        config.memory_retain_slots,
        config.memory_max_bytes
    );
    for table in tables {
        let _ = writeln!(
            input,
            "table={:?}|logical={}|storage={}|engine={}|partition={}|primary={}|sorting={}|view={}",
            table.kind,
            table.logical_name,
            table.storage_name,
            table.storage.engine,
            table.storage.partition_key,
            table.storage.primary_key,
            table.storage.sorting_key,
            table.view_select.as_deref().unwrap_or_default()
        );
        for column in &table.columns {
            let _ = writeln!(
                input,
                "column={}|{}|{}|{}|{}",
                column.name,
                column.type_name,
                column.default_kind,
                column.default_expression,
                column.compression_codec
            );
        }
        for index in &table.indexes {
            let _ = writeln!(input, "index={index}");
        }
    }
    blake3::hash(input.as_bytes()).to_hex().to_string()
}

fn column_definition(column: &ColumnRow) -> String {
    let mut definition = format!("{} {}", quote_identifier(&column.name), column.type_name);
    if !column.default_kind.is_empty() {
        let _ = write!(
            definition,
            " {} {}",
            column.default_kind, column.default_expression
        );
    }
    if !column.compression_codec.is_empty()
        && !column.compression_codec.eq_ignore_ascii_case("NONE")
    {
        definition.push(' ');
        definition.push_str(&column.compression_codec);
    }
    definition
}

fn create_cache_table_sql(
    table: &SourceTableSchema,
    config: &CacheSchemaConfig,
) -> Result<String, SchemaError> {
    let target = quote_table(&config.database, table.kind.local_name());
    let mut entries: Vec<String> = table.columns.iter().map(column_definition).collect();
    entries.extend(table.indexes.iter().cloned());
    let columns = entries.join(",\n    ");

    if table.kind == CacheTableKind::BlocksMetadata && config.memory_blocks_metadata {
        let retain = config.memory_retain_slots.ok_or_else(|| {
            SchemaError::Invalid(
                "Memory blocks_metadata requires DISK_CACHE_MEMORY_RETAIN_SLOTS".to_string(),
            )
        })?;
        let bytes = config.memory_max_bytes.ok_or_else(|| {
            SchemaError::Invalid(
                "Memory blocks_metadata requires DISK_CACHE_MEMORY_MAX_BYTES".to_string(),
            )
        })?;
        return Ok(format!(
            "CREATE TABLE IF NOT EXISTS {target} (\n    {columns}\n) \
             ENGINE = Memory SETTINGS max_rows_to_keep = {retain}, min_rows_to_keep = {}, \
             max_bytes_to_keep = {bytes}, min_bytes_to_keep = {}",
            retain.saturating_mul(9) / 10,
            bytes.saturating_mul(9) / 10
        ));
    }

    let sorting_key = table.storage.sorting_key.trim();
    if sorting_key.is_empty() {
        return Err(SchemaError::Invalid(format!(
            "source table {} has no sorting key",
            table.storage_name
        )));
    }
    let primary_key = if table.storage.primary_key.trim().is_empty() {
        sorting_key
    } else {
        table.storage.primary_key.trim()
    };
    let reverse_setting = if sorting_key.to_ascii_uppercase().contains(" DESC") {
        " SETTINGS allow_experimental_reverse_key = 1"
    } else {
        ""
    };
    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {target} (\n    {columns}\n) \
         ENGINE = ReplacingMergeTree(slot) \
         PARTITION BY intDiv(slot, {}) PRIMARY KEY ({primary_key}) ORDER BY ({sorting_key}){reverse_setting}",
        config.partition_slots
    ))
}

fn replace_table_reference(
    select: &str,
    source_table: &str,
    target_table: &str,
) -> Result<String, SchemaError> {
    let (source_db, source_name) = split_table_reference("default", source_table);
    let variants = [
        source_table.to_string(),
        format!("`{source_db}`.`{source_name}`"),
        format!("{source_db}.`{source_name}`"),
    ];
    for variant in variants {
        if select.contains(&variant) {
            return Ok(select.replace(&variant, target_table));
        }
    }
    Err(SchemaError::Invalid(format!(
        "materialized view does not reference expected source table {source_table}"
    )))
}

fn create_cache_view_sql(
    table: &SourceTableSchema,
    transaction_storage: &str,
    config: &CacheSchemaConfig,
) -> Result<String, SchemaError> {
    let select = table
        .view_select
        .as_deref()
        .ok_or_else(|| SchemaError::Invalid("derived table has no view query".to_string()))?;
    let local_transactions = quote_table(&config.database, "transactions");
    let rewritten = replace_table_reference(select, transaction_storage, &local_transactions)?;
    let view_name = format!("{}__mv", table.kind.local_name());
    let view = quote_table(&config.database, &view_name);
    let target = quote_table(&config.database, table.kind.local_name());
    Ok(format!(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS {view} TO {target} AS {rewritten}"
    ))
}

async fn execute(client: &ClickHouseClient, sql: &str) -> Result<(), SchemaError> {
    client
        .client
        .query(sql)
        .execute()
        .await
        .map_err(|err| SchemaError::Query(err.to_string()))
}

async fn database_tables(
    local: &ClickHouseClient,
    database: &str,
) -> Result<Vec<String>, SchemaError> {
    let database = database.replace('\'', "''");
    local
        .client
        .query(&format!(
            "SELECT name FROM system.tables WHERE database = '{database}' ORDER BY name"
        ))
        .fetch_all::<NameRow>()
        .await
        .map(|rows| rows.into_iter().map(|row| row.name).collect())
        .map_err(|err| SchemaError::Query(err.to_string()))
}

async fn read_meta(
    local: &ClickHouseClient,
    config: &CacheSchemaConfig,
) -> Result<Vec<MetaRow>, SchemaError> {
    let table = quote_table(&config.database, META_TABLE);
    local
        .client
        .query(&format!(
            "SELECT key, argMax(value, updated_at) AS value FROM {table} GROUP BY key"
        ))
        .fetch_all::<MetaRow>()
        .await
        .map_err(|err| SchemaError::Query(err.to_string()))
}

async fn create_control_tables(
    local: &ClickHouseClient,
    config: &CacheSchemaConfig,
) -> Result<(), SchemaError> {
    let meta = quote_table(&config.database, META_TABLE);
    execute(
        local,
        &format!(
            "CREATE TABLE IF NOT EXISTS {meta} (key LowCardinality(String), value String, updated_at DateTime64(3)) \
             ENGINE = ReplacingMergeTree(updated_at) ORDER BY key"
        ),
    )
    .await?;

    let coverage = quote_table(&config.database, COVERAGE_TABLE);
    execute(
        local,
        &format!(
            "CREATE TABLE IF NOT EXISTS {coverage} (slot UInt64, status Int8, tx_count UInt32, version UInt64) \
             ENGINE = ReplacingMergeTree(version) PARTITION BY intDiv(slot, {}) ORDER BY slot",
            config.partition_slots
        ),
    )
    .await?;

    let runtime = quote_table(&config.database, RUNTIME_TABLE);
    execute(
        local,
        &format!("CREATE TABLE IF NOT EXISTS {runtime} (key String, value String) ENGINE = Memory"),
    )
    .await
}

async fn write_meta(
    local: &ClickHouseClient,
    snapshot: &SourceSchemaSnapshot,
    config: &CacheSchemaConfig,
) -> Result<(), SchemaError> {
    let table = quote_table(&config.database, META_TABLE);
    let fingerprint = snapshot.fingerprint.replace('\'', "''");
    execute(
        local,
        &format!(
            "INSERT INTO {table} (key, value, updated_at) VALUES \
             ('magic', '{CACHE_MAGIC}', now64(3)), \
             ('format_version', '{CACHE_FORMAT_VERSION}', now64(3)), \
             ('fingerprint', '{fingerprint}', now64(3))"
        ),
    )
    .await
}

/// The Memory engine survives an RPC process restart but not a ClickHouse
/// restart. A marker in another Memory table distinguishes those cases. When
/// ClickHouse has restarted, persisted coverage can no longer prove that the
/// in-memory block rows exist, so the forwarder must rebuild the window.
async fn reset_coverage_after_memory_restart(
    local: &ClickHouseClient,
    config: &CacheSchemaConfig,
) -> Result<bool, SchemaError> {
    if !config.memory_blocks_metadata {
        return Ok(false);
    }
    let runtime = quote_table(&config.database, RUNTIME_TABLE);
    let marker = local
        .client
        .query(&format!(
            "SELECT count() AS count FROM {runtime} WHERE key = 'clickhouse_generation'"
        ))
        .fetch_one::<CountRow>()
        .await
        .map_err(|err| SchemaError::Query(err.to_string()))?;
    if marker.count > 0 {
        return Ok(false);
    }

    execute(
        local,
        &format!(
            "TRUNCATE TABLE {}",
            quote_table(&config.database, COVERAGE_TABLE)
        ),
    )
    .await?;
    execute(
        local,
        &format!(
            "INSERT INTO {runtime} (key, value) VALUES ('clickhouse_generation', generateUUIDv4())"
        ),
    )
    .await?;
    Ok(true)
}

pub(crate) async fn initialize_cache_schema(
    local: &ClickHouseClient,
    snapshot: &SourceSchemaSnapshot,
    config: &CacheSchemaConfig,
) -> Result<bool, SchemaError> {
    validate_identifier(&config.database, "cache database")?;
    let mut rebuilt = false;
    let existing = database_tables(local, &config.database).await?;
    if !existing.is_empty() && !existing.iter().any(|name| name == META_TABLE) {
        return Err(SchemaError::Ownership(format!(
            "database {} contains tables but no {META_TABLE} ownership marker",
            config.database
        )));
    }
    if existing.iter().any(|name| name == META_TABLE) {
        let meta = read_meta(local, config).await?;
        let value = |key: &str| {
            meta.iter()
                .find(|row| row.key == key)
                .map(|row| row.value.as_str())
        };
        let expected_version = CACHE_FORMAT_VERSION.to_string();
        if value("magic") != Some(CACHE_MAGIC) {
            return Err(SchemaError::Ownership(format!(
                "database {} has an invalid cache marker",
                config.database
            )));
        }
        if value("format_version") != Some(expected_version.as_str())
            || value("fingerprint") != Some(snapshot.fingerprint.as_str())
        {
            execute(
                local,
                &format!("DROP DATABASE {} SYNC", quote_identifier(&config.database)),
            )
            .await?;
            rebuilt = true;
        }
    }

    execute(
        local,
        &format!(
            "CREATE DATABASE IF NOT EXISTS {} ENGINE = Atomic",
            quote_identifier(&config.database)
        ),
    )
    .await?;
    create_control_tables(local, config).await?;
    rebuilt |= reset_coverage_after_memory_restart(local, config).await?;

    for table in &snapshot.tables {
        execute(local, &create_cache_table_sql(table, config)?).await?;
    }

    let transaction = snapshot
        .table(CacheTableKind::Transactions)
        .ok_or_else(|| SchemaError::Invalid("transactions schema missing".to_string()))?;
    for table in snapshot
        .tables
        .iter()
        .filter(|table| table.kind.is_derived())
    {
        execute(
            local,
            &create_cache_view_sql(table, &transaction.storage_name, config)?,
        )
        .await?;
    }
    write_meta(local, snapshot, config).await?;
    Ok(rebuilt)
}

/// Extract top-level `INDEX ...` entries from a CREATE TABLE column list.
fn extract_index_definitions(create: &str) -> Vec<String> {
    let Some(open) = create.find('(') else {
        return Vec::new();
    };
    let mut depth = 0usize;
    let mut quote = None;
    let mut escaped = false;
    let mut close = None;
    for (offset, ch) in create[open + 1..].char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if ch == '\\' && quote.is_some() {
            escaped = true;
            continue;
        }
        if let Some(active) = quote {
            if ch == active {
                quote = None;
            }
            continue;
        }
        if matches!(ch, '\'' | '"' | '`') {
            quote = Some(ch);
            continue;
        }
        match ch {
            '(' => depth += 1,
            ')' if depth == 0 => {
                close = Some(open + 1 + offset);
                break;
            }
            ')' => depth -= 1,
            _ => {}
        }
    }
    let Some(close) = close else {
        return Vec::new();
    };
    split_top_level(&create[open + 1..close])
        .into_iter()
        .filter(|entry| {
            entry
                .trim_start()
                .to_ascii_uppercase()
                .starts_with("INDEX ")
        })
        .map(|entry| entry.trim().to_string())
        .collect()
}

fn split_top_level(input: &str) -> Vec<&str> {
    let mut entries = Vec::new();
    let mut start = 0usize;
    let mut depth = 0usize;
    let mut quote = None;
    let mut escaped = false;
    for (offset, ch) in input.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if ch == '\\' && quote.is_some() {
            escaped = true;
            continue;
        }
        if let Some(active) = quote {
            if ch == active {
                quote = None;
            }
            continue;
        }
        if matches!(ch, '\'' | '"' | '`') {
            quote = Some(ch);
            continue;
        }
        match ch {
            '(' | '[' => depth += 1,
            ')' | ']' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                entries.push(&input[start..offset]);
                start = offset + ch.len_utf8();
            }
            _ => {}
        }
    }
    entries.push(&input[start..]);
    entries
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_only_top_level_indexes() {
        let ddl = "CREATE TABLE x (a Array(Tuple(String, UInt64)), b Nullable(String), INDEX bf b TYPE bloom_filter(0.01) GRANULARITY 64) ENGINE=MergeTree ORDER BY a";
        assert_eq!(
            extract_index_definitions(ddl),
            vec!["INDEX bf b TYPE bloom_filter(0.01) GRANULARITY 64"]
        );
    }

    #[test]
    fn rewrites_qualified_view_source() {
        let rewritten = replace_table_reference(
            "SELECT * FROM default.transactions_local",
            "default.transactions_local",
            "`superbank_disk_cache`.`transactions`",
        )
        .expect("rewrite");
        assert_eq!(
            rewritten,
            "SELECT * FROM `superbank_disk_cache`.`transactions`"
        );
    }

    #[test]
    fn partition_width_is_part_of_fingerprint() {
        let table = SourceTableSchema {
            kind: CacheTableKind::Transactions,
            logical_name: "default.transactions".into(),
            storage_name: "default.transactions_local".into(),
            columns: vec![ColumnRow {
                name: "slot".into(),
                type_name: "UInt64".into(),
                default_kind: String::new(),
                default_expression: String::new(),
                compression_codec: String::new(),
            }],
            storage: TableRow {
                uuid: String::new(),
                engine: "ReplacingMergeTree".into(),
                partition_key: "intDiv(slot, 432000)".into(),
                sorting_key: "slot".into(),
                primary_key: "slot".into(),
                create_table_query: String::new(),
                as_select: String::new(),
            },
            view_select: None,
            indexes: Vec::new(),
        };
        let config = |partition_slots| CacheSchemaConfig {
            database: "cache".into(),
            partition_slots,
            memory_blocks_metadata: false,
            memory_retain_slots: None,
            memory_max_bytes: None,
        };
        assert_ne!(
            schema_fingerprint(std::slice::from_ref(&table), &config(10_000)),
            schema_fingerprint(&[table], &config(20_000))
        );
    }
}
