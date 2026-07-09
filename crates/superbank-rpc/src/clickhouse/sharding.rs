// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use clickhouse::Client as HttpClient;
use clickhouse_rs::{Options as TcpOptions, Pool as TcpPool};
use serde::Deserialize;

use crate::processing::{ProcessingError, ProcessingResult};

use super::constants::MAX_BUCKET_MODULUS;
use super::util::{
    QueryCacheConfig, QueryFreshnessClass, append_max_execution_time_setting,
    build_select_settings_clause, build_select_settings_clause_with_overrides,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RoutingTransport {
    Tcp,
    Http,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RoutingScope {
    Distributed,
    ShardDirect,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RoutingPolicy {
    pub(crate) transport: RoutingTransport,
    pub(crate) scope: RoutingScope,
}

#[derive(Clone)]
pub(crate) struct ShardRoutingConfig {
    pub(crate) cluster: String,
    pub(crate) topology_config_path: Option<String>,
    pub(crate) shard_http_port: Option<u16>,
    pub(crate) gsfa_local_table: Option<String>,
    pub(crate) signatures_local_table: Option<String>,
    pub(crate) token_owner_activity_local_table: Option<String>,
    pub(crate) transfers_local_table: Option<String>,
    pub(crate) transactions_local_table: Option<String>,
    pub(crate) blocks_metadata_local_table: Option<String>,
}

#[derive(Deserialize, clickhouse::Row, Clone)]
pub(crate) struct ClusterRow {
    pub(crate) shard_num: u32,
    pub(crate) shard_weight: u32,
    pub(crate) replica_num: u32,
    pub(crate) host_name: String,
    pub(crate) host_address: String,
    pub(crate) port: u16,
    pub(crate) is_local: u8,
}

#[derive(Deserialize, Clone, Debug, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ClickHouseTopologyConfig {
    pub(crate) nodes: Vec<ClickHouseTopologyNode>,
}

#[derive(Deserialize, Clone, Debug, Eq, PartialEq)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub(crate) struct ClickHouseTopologyNode {
    #[serde(alias = "shard_id")]
    pub(crate) shard_id: u32,
    pub(crate) hostname: String,
    #[serde(alias = "ip_address")]
    pub(crate) ip_address: IpAddr,
    #[serde(alias = "tcp_port")]
    pub(crate) tcp_port: u16,
    #[serde(alias = "shard_weight")]
    pub(crate) shard_weight: u32,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct TopologyNodeKey {
    shard_id: u32,
    hostname: String,
    tcp_port: u16,
    shard_weight: u32,
}

impl TopologyNodeKey {
    fn from_configured(node: &ClickHouseTopologyNode) -> Self {
        Self {
            shard_id: node.shard_id,
            hostname: node.hostname.clone(),
            tcp_port: node.tcp_port,
            shard_weight: node.shard_weight,
        }
    }
}

impl fmt::Display for TopologyNodeKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "shard-id={} hostname='{}' tcp-port={} shard-weight={}",
            self.shard_id, self.hostname, self.tcp_port, self.shard_weight
        )
    }
}

#[derive(Deserialize, clickhouse::Row, Clone, Debug, Default)]
pub(crate) struct DescribeTableRow {
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) default_type: String,
    #[serde(default)]
    pub(crate) default_expression: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BucketColumn {
    Address,
    Signature,
    Owner,
}

impl BucketColumn {
    fn bucket_column_name(self) -> &'static str {
        match self {
            BucketColumn::Address => "addr_bucket",
            BucketColumn::Signature => "sig_bucket",
            BucketColumn::Owner => "owner_bucket",
        }
    }

    fn source_column_name(self) -> &'static str {
        match self {
            BucketColumn::Address => "address",
            BucketColumn::Signature => "signature",
            BucketColumn::Owner => "owner",
        }
    }
}

#[derive(Clone)]
pub(crate) struct ShardTarget {
    pub(crate) shard_num: u32,
    pub(crate) tcp_pool: Arc<TcpPool>,
    pub(crate) http_client: HttpClient,
    pub(crate) host: String,
    pub(crate) tcp_port: u16,
}

#[derive(Clone)]
pub(crate) struct ShardTopology {
    pub(crate) total_weight: u64,
    pub(crate) shards: Vec<ShardTarget>,
    pub(crate) weights: Vec<u64>,
    pub(crate) allow_query_settings: bool,
    pub(crate) query_cache: QueryCacheConfig,
    // Caps server-side execution for shard-direct HTTP reads built from this topology, matching the
    // client-side `query_timeout` so ClickHouse abandons a query once superbank stops awaiting it.
    pub(crate) query_timeout: Duration,
}

impl ShardTopology {
    pub(crate) fn settings_clause(
        &self,
        operation: &'static str,
        freshness: QueryFreshnessClass,
    ) -> String {
        append_max_execution_time_setting(
            &build_select_settings_clause(
                self.allow_query_settings,
                freshness,
                &self.query_cache,
                false,
                operation,
            ),
            self.query_timeout,
        )
    }

    pub(crate) fn settings_clause_with_condition_cache(
        &self,
        operation: &'static str,
        freshness: QueryFreshnessClass,
    ) -> String {
        append_max_execution_time_setting(
            &build_select_settings_clause(
                self.allow_query_settings,
                freshness,
                &self.query_cache,
                true,
                operation,
            ),
            self.query_timeout,
        )
    }

    pub(crate) fn get_transaction_settings_clause(
        &self,
        operation: &'static str,
        freshness: QueryFreshnessClass,
    ) -> String {
        append_max_execution_time_setting(
            &build_select_settings_clause_with_overrides(
                self.allow_query_settings,
                freshness,
                &self.query_cache,
                self.query_cache.get_transaction_overrides(),
                false,
                operation,
            ),
            self.query_timeout,
        )
    }

    pub(crate) fn shard_index_for_hash(&self, hash: u64) -> usize {
        select_weighted_index(&self.weights, self.total_weight, hash)
    }

    pub(crate) fn shard_for_hash(&self, hash: u64) -> &ShardTarget {
        &self.shards[self.shard_index_for_hash(hash)]
    }
}

pub(crate) async fn load_clickhouse_topology_config(
    path: &str,
) -> ProcessingResult<ClickHouseTopologyConfig> {
    let contents = tokio::fs::read_to_string(path).await.map_err(|e| {
        ProcessingError::deserialization(
            format!("failed to read ClickHouse topology config '{path}'"),
            e,
        )
    })?;
    parse_clickhouse_topology_config_with_context(&contents, path)
}

#[cfg(test)]
fn parse_clickhouse_topology_config(contents: &str) -> ProcessingResult<ClickHouseTopologyConfig> {
    parse_clickhouse_topology_config_with_context(contents, "inline")
}

fn parse_clickhouse_topology_config_with_context(
    contents: &str,
    source: &str,
) -> ProcessingResult<ClickHouseTopologyConfig> {
    let config = serde_yaml::from_str::<ClickHouseTopologyConfig>(contents).map_err(|e| {
        ProcessingError::deserialization(
            format!("failed to parse ClickHouse topology config '{source}'"),
            e,
        )
    })?;
    validate_clickhouse_topology_config(&config).map_err(|e| {
        ProcessingError::deserialization_msg(format!(
            "invalid ClickHouse topology config '{source}': {e}"
        ))
    })?;

    Ok(config)
}

fn validate_clickhouse_topology_config(config: &ClickHouseTopologyConfig) -> ProcessingResult<()> {
    if config.nodes.is_empty() {
        return Err(ProcessingError::deserialization_msg(
            "topology config must contain at least one node",
        ));
    }

    let mut seen = BTreeSet::new();
    for (idx, node) in config.nodes.iter().enumerate() {
        if node.hostname.trim().is_empty() {
            return Err(ProcessingError::deserialization_msg(format!(
                "topology node {idx} has blank hostname"
            )));
        }

        let key = TopologyNodeKey::from_configured(node);
        if !seen.insert(key.clone()) {
            return Err(ProcessingError::deserialization_msg(format!(
                "duplicate topology node identity: {key}"
            )));
        }
    }

    Ok(())
}

pub(crate) fn selected_configured_shard_nodes(
    config: &ClickHouseTopologyConfig,
) -> Vec<ClickHouseTopologyNode> {
    let mut by_shard = BTreeMap::new();
    for node in &config.nodes {
        by_shard
            .entry(node.shard_id)
            .or_insert_with(|| node.clone());
    }
    by_shard.into_values().collect()
}

pub(crate) fn derive_local_table_name(
    distributed: &str,
    override_table: Option<String>,
) -> Option<String> {
    if let Some(value) = override_table {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }

    let trimmed = distributed.trim();
    if trimmed.is_empty() {
        return None;
    }

    let (db, table) = match trimmed.rsplit_once('.') {
        Some((db, table)) => (Some(db), table),
        None => (None, trimmed),
    };

    if table.ends_with("_local") {
        return Some(trimmed.to_string());
    }

    Some(match db {
        Some(db) => format!("{db}.{table}_local"),
        None => format!("{table}_local"),
    })
}

pub(crate) fn resolve_host(row: &ClusterRow) -> Option<String> {
    if !row.host_name.is_empty() {
        Some(row.host_name.to_string())
    } else if !row.host_address.is_empty() {
        Some(row.host_address.to_string())
    } else {
        None
    }
}

pub(crate) fn pick_replica(replicas: &[ClusterRow]) -> Option<&ClusterRow> {
    if replicas.is_empty() {
        return None;
    }
    replicas
        .iter()
        .find(|row| row.is_local != 0)
        .or_else(|| replicas.iter().min_by_key(|row| row.replica_num))
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct TcpPoolSizing {
    pub(crate) min: usize,
    pub(crate) max: usize,
}

pub(crate) fn build_tcp_pool(
    database: &str,
    username: &str,
    password: &str,
    host: &str,
    port: u16,
    query_timeout: Duration,
    sizing: TcpPoolSizing,
) -> ProcessingResult<TcpPool> {
    let host_for_tcp = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_string()
    };
    let addr = format!("tcp://{host_for_tcp}:{port}");
    let mut options = TcpOptions::from_str(&addr).map_err(|e| {
        ProcessingError::database(format!("invalid ClickHouse TCP url '{addr}'"), e)
    })?;

    if !username.is_empty() {
        options = options.username(username);
    }
    if !password.is_empty() {
        options = options.password(password);
    }
    if !database.is_empty() {
        options = options.database(database);
    }
    // clickhouse_rs defaults to pool_min=10/pool_max=20 per shard; make these explicit and
    // configurable so the per-instance native connection ceiling (pool_max x shards) is bounded
    // by operator config rather than the driver default.
    let pool_max = sizing.max.max(1);
    let pool_min = sizing.min.min(pool_max);
    options = options
        .query_timeout(query_timeout)
        .execute_timeout(Some(query_timeout))
        .pool_min(pool_min)
        .pool_max(pool_max);

    Ok(TcpPool::new(options))
}

pub(crate) async fn validate_table_schema(
    pool: &TcpPool,
    table: &str,
    shard_num: u32,
    host: &str,
    port: u16,
    required_columns: &[&str],
    timeout: Duration,
) -> ProcessingResult<()> {
    match tokio::time::timeout(timeout, async {
        let rows = describe_table_schema(pool, table, shard_num, host, port).await?;
        validate_required_columns(&rows, table, required_columns, shard_num, host, port)?;

        Ok(())
    })
    .await
    {
        Ok(result) => result,
        Err(_) => Err(ProcessingError::timeout_msg(format!(
            "Shard {shard_num} {host}:{port} schema query timed out after {timeout:?}"
        ))),
    }
}

async fn describe_table_schema(
    pool: &TcpPool,
    table: &str,
    shard_num: u32,
    host: &str,
    port: u16,
) -> ProcessingResult<Vec<DescribeTableRow>> {
    let query = format!("DESCRIBE TABLE {}", table);
    let mut client = pool.get_handle().await.map_err(|e| {
        ProcessingError::database(format!("Shard {shard_num} {host}:{port} handle error"), e)
    })?;

    let block = client
        .query(query.as_str())
        .fetch_all()
        .await
        .map_err(|e| {
            ProcessingError::database(
                format!("Shard {shard_num} {host}:{port} schema query failed"),
                e,
            )
        })?;

    if block.row_count() == 0 {
        return Err(ProcessingError::database_msg(format!(
            "Shard {shard_num} {host}:{port} returned no columns for {table}"
        )));
    }

    let mut rows = Vec::with_capacity(block.row_count());
    for row in block.rows() {
        let name: String = row
            .get("name")
            .map_err(|e| ProcessingError::database(e.to_string(), e))?;
        let default_type: String = row
            .get("default_type")
            .map_err(|e| ProcessingError::database(e.to_string(), e))?;
        let default_expression: String = row
            .get("default_expression")
            .map_err(|e| ProcessingError::database(e.to_string(), e))?;
        rows.push(DescribeTableRow {
            name,
            default_type,
            default_expression,
        });
    }

    Ok(rows)
}

fn validate_required_columns(
    rows: &[DescribeTableRow],
    table: &str,
    required_columns: &[&str],
    shard_num: u32,
    host: &str,
    port: u16,
) -> ProcessingResult<()> {
    let mut missing = Vec::new();
    for required in required_columns {
        if !rows.iter().any(|row| row.name == *required) {
            missing.push(required.to_string());
        }
    }

    if !missing.is_empty() {
        return Err(ProcessingError::database_msg(format!(
            "Shard {shard_num} {host}:{port} table {table} missing columns: {}",
            missing.join(", ")
        )));
    }

    Ok(())
}

fn strip_wrapping_parens(mut value: &str) -> &str {
    while value.len() >= 2 && value.starts_with('(') && value.ends_with(')') {
        let mut depth = 0_i32;
        let mut wraps = true;
        for (idx, ch) in value.char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 && idx != value.len() - 1 {
                        wraps = false;
                        break;
                    }
                }
                _ => {}
            }
        }

        if !wraps || depth != 0 {
            break;
        }

        value = &value[1..value.len() - 1];
    }

    value
}

fn normalize_bucket_expression(value: &str) -> String {
    let compact: String = value
        .chars()
        .filter(|ch| !ch.is_ascii_whitespace())
        .collect();
    strip_wrapping_parens(compact.as_str()).to_string()
}

pub(crate) fn bucket_modulus_from_describe_rows(
    rows: &[DescribeTableRow],
    table: &str,
    bucket_column: BucketColumn,
) -> ProcessingResult<u64> {
    let bucket_name = bucket_column.bucket_column_name();
    let Some(row) = rows.iter().find(|row| row.name == bucket_name) else {
        return Err(ProcessingError::database_msg(format!(
            "table {table} missing bucket column {bucket_name}"
        )));
    };

    let default_type = row.default_type.trim();
    if !default_type.eq_ignore_ascii_case("materialized") {
        return Err(ProcessingError::database_msg(format!(
            "table {table} bucket column {bucket_name} must be MATERIALIZED, found '{}'",
            row.default_type
        )));
    }

    let normalized = normalize_bucket_expression(&row.default_expression);
    let normalized_lower = normalized.to_ascii_lowercase();
    let prefix = format!("cityhash64({})%", bucket_column.source_column_name());
    let Some(modulus_str) = normalized_lower.strip_prefix(prefix.as_str()) else {
        return Err(ProcessingError::database_msg(format!(
            "table {table} bucket column {bucket_name} has unsupported expression '{}'",
            row.default_expression
        )));
    };

    if modulus_str.is_empty() || !modulus_str.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(ProcessingError::database_msg(format!(
            "table {table} bucket column {bucket_name} has invalid modulus expression '{}'",
            row.default_expression
        )));
    }

    let modulus = modulus_str.parse::<u64>().map_err(|e| {
        ProcessingError::database(
            format!(
                "table {table} bucket column {bucket_name} has invalid modulus '{}'",
                modulus_str
            ),
            e,
        )
    })?;

    if modulus == 0 || modulus > MAX_BUCKET_MODULUS {
        return Err(ProcessingError::database_msg(format!(
            "table {table} bucket column {bucket_name} modulus {modulus} out of supported range 1..={MAX_BUCKET_MODULUS}"
        )));
    }

    Ok(modulus)
}

pub(crate) async fn validate_table_schema_on_shards(
    topology: &ShardTopology,
    table: &str,
    required_columns: &[&str],
    timeout: Duration,
) -> ProcessingResult<()> {
    for shard in &topology.shards {
        validate_table_schema(
            shard.tcp_pool.as_ref(),
            table,
            shard.shard_num,
            &shard.host,
            shard.tcp_port,
            required_columns,
            timeout,
        )
        .await?;
    }
    Ok(())
}

pub(crate) async fn detect_bucket_modulus_on_shards(
    topology: &ShardTopology,
    table: &str,
    bucket_column: BucketColumn,
    timeout: Duration,
) -> ProcessingResult<u64> {
    let mut discovered = None;
    for shard in &topology.shards {
        let modulus = match tokio::time::timeout(timeout, async {
            let rows = describe_table_schema(
                shard.tcp_pool.as_ref(),
                table,
                shard.shard_num,
                &shard.host,
                shard.tcp_port,
            )
            .await?;
            bucket_modulus_from_describe_rows(&rows, table, bucket_column)
        })
        .await
        {
            Ok(result) => result?,
            Err(_) => {
                return Err(ProcessingError::timeout_msg(format!(
                    "Shard {} {}:{} bucket discovery timed out after {timeout:?}",
                    shard.shard_num, shard.host, shard.tcp_port
                )));
            }
        };

        if let Some(prev) = discovered {
            if prev != modulus {
                return Err(ProcessingError::database_msg(format!(
                    "Shard {} {}:{} table {} modulus {} does not match previously discovered modulus {}",
                    shard.shard_num, shard.host, shard.tcp_port, table, modulus, prev
                )));
            }
        } else {
            discovered = Some(modulus);
        }
    }

    discovered.ok_or_else(|| {
        ProcessingError::database_msg(format!(
            "No shards available to discover bucket modulus for {table}"
        ))
    })
}

pub(crate) fn escape_clickhouse_string(value: &str) -> String {
    value.replace('\'', "''")
}

fn select_weighted_index(weights: &[u64], total_weight: u64, hash: u64) -> usize {
    if weights.is_empty() || total_weight == 0 {
        return 0;
    }
    let mut remaining = hash % total_weight;
    for (idx, weight) in weights.iter().enumerate() {
        if remaining < *weight {
            return idx;
        }
        remaining = remaining.saturating_sub(*weight);
    }
    weights.len().saturating_sub(1)
}

#[cfg(test)]
mod tests {
    use super::{
        BucketColumn, ClusterRow, DescribeTableRow, QueryCacheConfig, QueryFreshnessClass,
        ShardTopology, bucket_modulus_from_describe_rows, parse_clickhouse_topology_config,
        resolve_host, select_weighted_index, selected_configured_shard_nodes,
    };

    fn topology_yaml() -> &'static str {
        "\
nodes:
  - shard-id: 1
    hostname: ch-bhs1
    ip-address: 10.43.86.5
    tcp-port: 9000
    shard-weight: 1
  - shard-id: 2
    hostname: ch-bhs2
    ip-address: 10.43.86.6
    tcp-port: 9000
    shard-weight: 2
"
    }

    #[test]
    fn weighted_selection_respects_shard_weights() {
        let weights = vec![1_u64, 2, 1];
        let total = 4_u64;

        assert_eq!(select_weighted_index(&weights, total, 0), 0);
        assert_eq!(select_weighted_index(&weights, total, 1), 1);
        assert_eq!(select_weighted_index(&weights, total, 2), 1);
        assert_eq!(select_weighted_index(&weights, total, 3), 2);

        assert_eq!(select_weighted_index(&weights, total, 4), 0);
        assert_eq!(select_weighted_index(&weights, total, 5), 1);
        assert_eq!(select_weighted_index(&weights, total, 6), 1);
        assert_eq!(select_weighted_index(&weights, total, 7), 2);
    }

    #[test]
    fn resolve_host_prefers_host_name_over_host_address() {
        let row = ClusterRow {
            shard_num: 1,
            shard_weight: 1,
            replica_num: 1,
            host_name: "ch-1.internal".to_string(),
            host_address: "10.0.0.1".to_string(),
            port: 9000,
            is_local: 0,
        };

        assert_eq!(resolve_host(&row).as_deref(), Some("ch-1.internal"));
    }

    #[test]
    fn resolve_host_falls_back_to_host_address_when_host_name_missing() {
        let row = ClusterRow {
            shard_num: 1,
            shard_weight: 1,
            replica_num: 1,
            host_name: "".to_string(),
            host_address: "10.0.0.1".to_string(),
            port: 9000,
            is_local: 0,
        };

        assert_eq!(resolve_host(&row).as_deref(), Some("10.0.0.1"));
    }

    #[test]
    fn resolve_host_returns_none_when_both_fields_empty() {
        let row = ClusterRow {
            shard_num: 1,
            shard_weight: 1,
            replica_num: 1,
            host_name: "".to_string(),
            host_address: "".to_string(),
            port: 9000,
            is_local: 0,
        };

        assert_eq!(resolve_host(&row), None);
    }

    #[test]
    fn topology_config_parses_kebab_case_keys() {
        let config =
            parse_clickhouse_topology_config(topology_yaml()).expect("topology config parses");

        assert_eq!(config.nodes.len(), 2);
        assert_eq!(config.nodes[0].shard_id, 1);
        assert_eq!(config.nodes[0].hostname, "ch-bhs1");
        assert_eq!(config.nodes[0].ip_address.to_string(), "10.43.86.5");
        assert_eq!(config.nodes[0].tcp_port, 9000);
        assert_eq!(config.nodes[0].shard_weight, 1);
    }

    #[test]
    fn topology_config_parses_snake_case_aliases() {
        let config = parse_clickhouse_topology_config(
            "\
nodes:
  - shard_id: 1
    hostname: ch-bhs1
    ip_address: 10.43.86.5
    tcp_port: 9000
    shard_weight: 1
",
        )
        .expect("topology config parses");

        assert_eq!(config.nodes[0].shard_id, 1);
        assert_eq!(config.nodes[0].ip_address.to_string(), "10.43.86.5");
    }

    #[test]
    fn topology_config_rejects_invalid_ip_addresses() {
        let err = parse_clickhouse_topology_config(
            "\
nodes:
  - shard-id: 1
    hostname: ch-bhs1
    ip-address: not-an-ip
    tcp-port: 9000
    shard-weight: 1
",
        )
        .expect_err("invalid IP should fail");

        assert!(err.to_string().contains("failed to parse"));
    }

    #[test]
    fn topology_config_rejects_empty_files() {
        let err = parse_clickhouse_topology_config("").expect_err("empty file should fail");

        assert!(err.to_string().contains("failed to parse"));
    }

    #[test]
    fn topology_config_rejects_empty_node_list() {
        let err =
            parse_clickhouse_topology_config("nodes: []").expect_err("empty nodes should fail");

        assert!(err.to_string().contains("at least one node"));
    }

    #[test]
    fn topology_config_rejects_blank_hostnames() {
        let err = parse_clickhouse_topology_config(
            "\
nodes:
  - shard-id: 1
    hostname: '   '
    ip-address: 10.43.86.5
    tcp-port: 9000
    shard-weight: 1
",
        )
        .expect_err("blank hostname should fail");

        assert!(err.to_string().contains("blank hostname"));
    }

    #[test]
    fn topology_config_rejects_duplicate_nodes() {
        let err = parse_clickhouse_topology_config(
            "\
nodes:
  - shard-id: 1
    hostname: ch-bhs1
    ip-address: 10.43.86.5
    tcp-port: 9000
    shard-weight: 1
  - shard-id: 1
    hostname: ch-bhs1
    ip-address: 10.43.86.5
    tcp-port: 9000
    shard-weight: 1
",
        )
        .expect_err("duplicate node should fail");

        assert!(err.to_string().contains("duplicate topology node identity"));
    }

    #[test]
    fn topology_config_rejects_duplicate_nodes_that_only_differ_by_ip_address() {
        let err = parse_clickhouse_topology_config(
            "\
nodes:
  - shard-id: 1
    hostname: ch-bhs1
    ip-address: 10.43.86.5
    tcp-port: 9000
    shard-weight: 1
  - shard-id: 1
    hostname: ch-bhs1
    ip-address: 10.43.86.99
    tcp-port: 9000
    shard-weight: 1
",
        )
        .expect_err("same topology identity with different connection IP should fail");

        assert!(err.to_string().contains("duplicate topology node identity"));
    }

    #[test]
    fn topology_config_rejects_unknown_fields() {
        let err = parse_clickhouse_topology_config(
            "\
nodes:
  - shard-id: 1
    hostname: ch-bhs1
    ip-address: 10.43.86.5
    tcp-port: 9000
    shard-weight: 1
    rack: bhs-a
",
        )
        .expect_err("unknown field should fail");

        assert!(err.to_string().contains("failed to parse"));
    }

    #[test]
    fn configured_shard_selection_uses_first_yaml_node_per_shard() {
        let config = parse_clickhouse_topology_config(
            "\
nodes:
  - shard-id: 2
    hostname: ch-bhs2-a
    ip-address: 10.43.86.6
    tcp-port: 9000
    shard-weight: 2
  - shard-id: 1
    hostname: ch-bhs1
    ip-address: 10.43.86.5
    tcp-port: 9000
    shard-weight: 1
  - shard-id: 2
    hostname: ch-bhs2-b
    ip-address: 10.43.86.7
    tcp-port: 9000
    shard-weight: 2
",
        )
        .expect("topology config parses");

        let selected = selected_configured_shard_nodes(&config);

        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0].shard_id, 1);
        assert_eq!(selected[0].hostname, "ch-bhs1");
        assert_eq!(selected[1].shard_id, 2);
        assert_eq!(selected[1].hostname, "ch-bhs2-a");
    }

    #[test]
    fn bucket_modulus_parses_current_expression_shape() {
        let rows = vec![DescribeTableRow {
            name: "addr_bucket".to_string(),
            default_type: "MATERIALIZED".to_string(),
            default_expression: "cityHash64(address) % 128".to_string(),
        }];

        let modulus =
            bucket_modulus_from_describe_rows(&rows, "default.gsfa", BucketColumn::Address)
                .expect("bucket modulus");

        assert_eq!(modulus, 128);
    }

    #[test]
    fn bucket_modulus_parses_expression_with_whitespace_and_parens() {
        let rows = vec![DescribeTableRow {
            name: "sig_bucket".to_string(),
            default_type: "MATERIALIZED".to_string(),
            default_expression: " ( cityHash64(signature) % 32 ) ".to_string(),
        }];

        let modulus =
            bucket_modulus_from_describe_rows(&rows, "default.signatures", BucketColumn::Signature)
                .expect("bucket modulus");

        assert_eq!(modulus, 32);
    }

    #[test]
    fn bucket_modulus_rejects_wrong_source_column() {
        let rows = vec![DescribeTableRow {
            name: "owner_bucket".to_string(),
            default_type: "MATERIALIZED".to_string(),
            default_expression: "cityHash64(address) % 32".to_string(),
        }];

        let err = bucket_modulus_from_describe_rows(
            &rows,
            "default.token_owner_activity",
            BucketColumn::Owner,
        )
        .expect_err("wrong source column should fail");

        assert!(err.to_string().contains("unsupported expression"));
    }

    #[test]
    fn bucket_modulus_rejects_out_of_range_modulus() {
        let rows = vec![DescribeTableRow {
            name: "addr_bucket".to_string(),
            default_type: "MATERIALIZED".to_string(),
            default_expression: "cityHash64(address) % 300".to_string(),
        }];

        let err = bucket_modulus_from_describe_rows(&rows, "default.gsfa", BucketColumn::Address)
            .expect_err("out of range modulus should fail");

        assert!(err.to_string().contains("out of supported range"));
    }

    #[test]
    fn shard_topology_get_transaction_settings_clause_uses_overrides_only_for_that_path() {
        let topology = ShardTopology {
            total_weight: 1,
            shards: Vec::new(),
            weights: vec![1],
            allow_query_settings: true,
            query_cache: QueryCacheConfig::new(true, 10, false, false)
                .with_get_transaction_overrides(300, 2),
            query_timeout: std::time::Duration::from_millis(8_000),
        };

        let tx_clause = topology.get_transaction_settings_clause(
            "get_transaction_by_signature_local_http",
            QueryFreshnessClass::Historical,
        );
        let generic_clause = topology.settings_clause(
            "get_block_time_by_slot_local_http",
            QueryFreshnessClass::Historical,
        );

        assert!(tx_clause.contains("query_cache_ttl=300"));
        assert!(tx_clause.contains("query_cache_min_query_runs=2"));
        assert!(generic_clause.contains("query_cache_ttl=10"));
        assert!(!generic_clause.contains("query_cache_min_query_runs"));
        // Shard-direct HTTP reads carry a server-side execution cap aligned with query_timeout.
        assert!(tx_clause.contains("max_execution_time=8"));
        assert!(generic_clause.contains("max_execution_time=8"));
    }
}
