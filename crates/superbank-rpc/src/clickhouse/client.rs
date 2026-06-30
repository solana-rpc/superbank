// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use clickhouse::Client as HttpClient;
use clickhouse_rs::{
    Block as TcpBlock,
    types::{Complex as TcpComplex, Query as TcpQuery},
};
use hyper_util::client::legacy::{Client as HyperClient, connect::HttpConnector};
use hyper_util::rt::TokioExecutor;
use reqwest::Url;
use serde::Deserialize;
use solana_sdk::pubkey::Pubkey;
use tokio::sync::Semaphore;

use crate::config::{ClickHouseStartupTableCheck, has_usable_gsfa_hot_addresses};
use crate::processing::{ProcessingError, ProcessingResult};

use super::cache::SignatureSlotCache;
use super::constants::DEFAULT_BUCKET_MODULUS;
use super::gsfa::GsfaShardRouter;
use super::queries::{
    GSFA_REQUIRED_COLUMNS, SIGNATURES_REQUIRED_COLUMNS, TOKEN_OWNER_REQUIRED_COLUMNS,
};
use super::sharding::{
    BucketColumn, ClusterRow, DescribeTableRow, RoutingPolicy, RoutingScope, RoutingTransport,
    ShardRoutingConfig, ShardTarget, ShardTopology, TcpPoolSizing,
    bucket_modulus_from_describe_rows, build_tcp_pool, derive_local_table_name,
    detect_bucket_modulus_on_shards, escape_clickhouse_string, load_clickhouse_topology_config,
    pick_replica, resolve_host, selected_configured_shard_nodes, validate_table_schema_on_shards,
};
use super::types::QueryTimings;
use super::util::{
    QueryCacheConfig, QueryFreshnessClass, annotate_tcp_query, append_max_execution_time_setting,
    build_select_settings_clause, build_select_settings_clause_with_overrides, env_truthy,
    kill_query_semaphore, next_required_query_id, transient_shard_local_error_reason,
};

const SHARD_TCP_TIMEOUT_RESERVE_MIN_MS: u128 = 500;
const SHARD_TCP_TIMEOUT_RESERVE_MAX_MS: u128 = 5_000;
const SHARD_TCP_TIMEOUT_RESERVE_DIVISOR: u128 = 4;
const SHARD_TCP_TIMEOUT_MIN_MS: u128 = 1;

pub(crate) fn shard_tcp_query_timeout_for(query_timeout: Duration) -> Duration {
    let timeout_ms = query_timeout.as_millis();
    if timeout_ms <= SHARD_TCP_TIMEOUT_MIN_MS {
        return query_timeout;
    }

    let max_reserve_ms = timeout_ms.saturating_sub(SHARD_TCP_TIMEOUT_MIN_MS);
    let reserve_ms = if timeout_ms <= SHARD_TCP_TIMEOUT_RESERVE_MIN_MS * 2 {
        timeout_ms / 2
    } else {
        (timeout_ms / SHARD_TCP_TIMEOUT_RESERVE_DIVISOR).clamp(
            SHARD_TCP_TIMEOUT_RESERVE_MIN_MS,
            SHARD_TCP_TIMEOUT_RESERVE_MAX_MS,
        )
    }
    .min(max_reserve_ms);

    let tcp_timeout_ms = timeout_ms.saturating_sub(reserve_ms);
    Duration::from_millis(tcp_timeout_ms.min(u128::from(u64::MAX)) as u64)
}

struct ShardTcpQueryCleanup {
    shard: ShardTarget,
    operation: &'static str,
    query_timeout: Duration,
    query_id: Option<String>,
}

impl ShardTcpQueryCleanup {
    fn new(
        shard: ShardTarget,
        operation: &'static str,
        query_timeout: Duration,
        query_id: String,
    ) -> Self {
        Self {
            shard,
            operation,
            query_timeout,
            query_id: Some(query_id),
        }
    }

    fn disarm(&mut self) {
        self.query_id = None;
    }

    fn spawn_cleanup(&mut self, reason: &'static str) {
        let Some(query_id) = self.query_id.take() else {
            return;
        };

        crate::metrics::clickhouse_shard_query_abort(self.operation, "tcp", reason);

        let shard = self.shard.clone();
        let operation = self.operation;
        let query_timeout = self.query_timeout;
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    kill_shard_tcp_query(shard, operation, query_timeout, query_id).await;
                });
            }
            Err(err) => {
                crate::metrics::clickhouse_shard_query_cleanup(operation, "no_runtime");
                tracing::warn!(
                    "Unable to schedule shard-local TCP cleanup for {}:{} query {}: {}",
                    shard.host,
                    shard.tcp_port,
                    query_id,
                    err
                );
            }
        }
    }
}

impl Drop for ShardTcpQueryCleanup {
    fn drop(&mut self) {
        self.spawn_cleanup("future_dropped");
    }
}

pub(crate) struct HttpQueryCleanup {
    client: HttpClient,
    cluster: Option<String>,
    operation: &'static str,
    query_timeout: Duration,
    query_id: Option<String>,
}

impl HttpQueryCleanup {
    fn new(
        client: HttpClient,
        cluster: Option<String>,
        operation: &'static str,
        query_timeout: Duration,
        query_id: String,
    ) -> Self {
        Self {
            client,
            cluster,
            operation,
            query_timeout,
            query_id: Some(query_id),
        }
    }

    pub(crate) fn disarm(&mut self) {
        self.query_id = None;
    }

    pub(crate) fn spawn_cleanup(&mut self, reason: &'static str) {
        let Some(query_id) = self.query_id.take() else {
            return;
        };

        crate::metrics::clickhouse_shard_query_abort(self.operation, "http", reason);

        let client = self.client.clone();
        let cluster = self.cluster.clone();
        let operation = self.operation;
        let query_timeout = self.query_timeout;
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    kill_http_query(client, cluster, operation, query_timeout, query_id).await;
                });
            }
            Err(err) => {
                crate::metrics::clickhouse_shard_query_cleanup(operation, "no_runtime");
                tracing::warn!(
                    "Unable to schedule HTTP ClickHouse cleanup for query {}: {}",
                    query_id,
                    err
                );
            }
        }
    }
}

impl Drop for HttpQueryCleanup {
    fn drop(&mut self) {
        self.spawn_cleanup("future_dropped");
    }
}

fn kill_query_sql(cluster: Option<&str>, query_id: &str) -> String {
    let escaped_query_id = escape_clickhouse_string(query_id);
    let predicate =
        format!("(query_id = '{escaped_query_id}' OR initial_query_id = '{escaped_query_id}')");

    match cluster {
        Some(cluster) => {
            let escaped_cluster = escape_clickhouse_string(cluster);
            format!("KILL QUERY ON CLUSTER '{escaped_cluster}' WHERE {predicate} ASYNC")
        }
        None => format!("KILL QUERY WHERE {predicate} ASYNC"),
    }
}

async fn kill_http_query(
    client: HttpClient,
    cluster: Option<String>,
    operation: &'static str,
    query_timeout: Duration,
    query_id: String,
) {
    // Hard-cap concurrent KILL cleanups; drop this one if the budget is exhausted rather than
    // opening yet another connection while ClickHouse is already overloaded.
    let _permit = match kill_query_semaphore().clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            crate::metrics::clickhouse_shard_query_cleanup(operation, "kill_throttled");
            return;
        }
    };
    let cleanup_query_id = next_required_query_id("http_query_cleanup");
    let kill_sql = kill_query_sql(cluster.as_deref(), &query_id);
    let cleanup_timeout = query_timeout.min(Duration::from_secs(2));

    match tokio::time::timeout(
        cleanup_timeout,
        client
            .query(&kill_sql)
            .with_option("query_id", cleanup_query_id)
            .execute(),
    )
    .await
    {
        Ok(Ok(())) => crate::metrics::clickhouse_shard_query_cleanup(operation, "kill_dispatched"),
        Ok(Err(err)) => {
            crate::metrics::clickhouse_shard_query_cleanup(operation, "kill_failed");
            tracing::warn!(
                "Failed to dispatch HTTP ClickHouse cleanup query for {}: {}",
                query_id,
                err
            );
        }
        Err(_) => {
            crate::metrics::clickhouse_shard_query_cleanup(operation, "kill_timeout");
            tracing::warn!(
                "Timed out dispatching HTTP ClickHouse cleanup query for {} after {:?}",
                query_id,
                cleanup_timeout
            );
        }
    }
}

async fn kill_shard_tcp_query(
    shard: ShardTarget,
    operation: &'static str,
    query_timeout: Duration,
    query_id: String,
) {
    // Hard-cap concurrent KILL cleanups; drop this one if the budget is exhausted rather than
    // opening yet another connection while ClickHouse is already overloaded.
    let _permit = match kill_query_semaphore().clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            crate::metrics::clickhouse_shard_query_cleanup(operation, "kill_throttled");
            return;
        }
    };
    let escaped_query_id = escape_clickhouse_string(&query_id);
    let cleanup_query_id = next_required_query_id("shard_query_cleanup");
    let kill_sql = format!("KILL QUERY WHERE query_id = '{escaped_query_id}' ASYNC");
    let cleanup_timeout = query_timeout.min(Duration::from_secs(2));

    match tokio::time::timeout(
        cleanup_timeout,
        shard
            .http_client
            .query(&kill_sql)
            .with_option("query_id", cleanup_query_id)
            .execute(),
    )
    .await
    {
        Ok(Ok(())) => crate::metrics::clickhouse_shard_query_cleanup(operation, "kill_dispatched"),
        Ok(Err(err)) => {
            crate::metrics::clickhouse_shard_query_cleanup(operation, "kill_failed");
            tracing::warn!(
                "Failed to dispatch shard-local ClickHouse cleanup query on {}:{} for {}: {}",
                shard.host,
                shard.tcp_port,
                query_id,
                err
            );
        }
        Err(_) => {
            crate::metrics::clickhouse_shard_query_cleanup(operation, "kill_timeout");
            tracing::warn!(
                "Timed out dispatching shard-local ClickHouse cleanup query on {}:{} for {} after {:?}",
                shard.host,
                shard.tcp_port,
                query_id,
                cleanup_timeout
            );
        }
    }
}

pub(crate) async fn execute_shard_tcp_query_block(
    shard: ShardTarget,
    query_timeout: Duration,
    operation: &'static str,
    query_label: &'static str,
    sql: String,
) -> ProcessingResult<(TcpBlock<TcpComplex>, QueryTimings)> {
    let start = std::time::Instant::now();
    let mut client = match tokio::time::timeout(query_timeout, shard.tcp_pool.get_handle()).await {
        Ok(Ok(handle)) => handle,
        Ok(Err(e)) => {
            return Err(ProcessingError::database(
                format!("Shard {}:{} tcp handle error", shard.host, shard.tcp_port),
                e,
            ));
        }
        Err(_) => {
            crate::metrics::clickhouse_timeout(operation);
            return Err(ProcessingError::timeout_msg(format!(
                "Shard {}:{} tcp handle timed out after {:?}",
                shard.host, shard.tcp_port, query_timeout
            )));
        }
    };

    let (sql, query_id) = annotate_tcp_query(sql, query_label);
    let mut cleanup =
        ShardTcpQueryCleanup::new(shard.clone(), operation, query_timeout, query_id.clone());

    match client
        .query(TcpQuery::new(sql).id(query_id))
        .fetch_all()
        .await
    {
        Ok(block) => {
            cleanup.disarm();
            let row_count = block.row_count() as u64;
            Ok((
                block,
                QueryTimings {
                    elapsed_ms: start.elapsed().as_millis() as u64,
                    received_bytes: 0,
                    decoded_bytes: 0,
                    rows_read: Some(0),
                    rows_read_unknown: true,
                    rows_returned: row_count,
                },
            ))
        }
        Err(err) => {
            let error = ProcessingError::database(err.to_string(), err);
            match transient_shard_local_error_reason(&error) {
                Some("timeout") => {
                    crate::metrics::clickhouse_timeout(operation);
                    cleanup.spawn_cleanup("timeout");
                    return Err(ProcessingError::timeout_msg(format!(
                        "ClickHouse operation '{operation}' timed out after {:?}",
                        query_timeout
                    )));
                }
                Some(reason) => cleanup.spawn_cleanup(reason),
                None => cleanup.disarm(),
            }

            Err(error)
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct BucketModuli {
    pub(crate) gsfa: u64,
    pub(crate) gsfa_hot: u64,
    pub(crate) signatures: u64,
    pub(crate) token_owner_activity: u64,
}

impl Default for BucketModuli {
    fn default() -> Self {
        Self {
            gsfa: DEFAULT_BUCKET_MODULUS,
            gsfa_hot: DEFAULT_BUCKET_MODULUS,
            signatures: DEFAULT_BUCKET_MODULUS,
            token_owner_activity: DEFAULT_BUCKET_MODULUS,
        }
    }
}

#[derive(Clone)]
pub struct ClickHouseClient {
    pub(crate) client: HttpClient,
    pub(crate) url: String,
    pub(crate) database: String,
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) signature_slot_cache: Arc<SignatureSlotCache>,
    pub(crate) transaction_table: String,
    pub(crate) blocks_metadata_table: String,
    pub(crate) gsfa_table: String,
    pub(crate) gsfa_hot_table: String,
    pub(crate) gsfa_hot_local_table: String,
    pub(crate) gsfa_hot_addresses: Vec<String>,
    pub(crate) gsfa_hot_pubkeys: HashSet<Pubkey>,
    pub(crate) signature_statuses_table: String,
    pub(crate) token_owner_activity_table: String,
    pub(crate) signatures_local_table: Option<String>,
    pub(crate) token_owner_activity_local_table: Option<String>,
    pub(crate) transactions_local_table: Option<String>,
    pub(crate) blocks_metadata_local_table: Option<String>,
    pub(crate) token_owner_activity_available: bool,
    pub(crate) bucket_moduli: BucketModuli,
    pub(crate) allow_query_settings: bool,
    pub(crate) query_cache: QueryCacheConfig,
    pub(crate) gsfa_router: Option<GsfaShardRouter>,
    pub(crate) shard_topology: Option<Arc<ShardTopology>>,
    pub(crate) shard_routing: Option<ShardRoutingConfig>,
    pub(crate) routing_policy: RoutingPolicy,

    pub(crate) query_timeout: Duration,
    pub(crate) tcp_access_check_timeout: Duration,
    pub(crate) http_connect_timeout: Duration,
    pub(crate) fanout_sem: Arc<Semaphore>,
    // Bounds concurrent direct (scalar/lookup) ClickHouse HTTP queries server-wide so HTTP
    // connection demand does not track raw request/batch concurrency. Acquired in `with_timeout`.
    pub(crate) http_query_sem: Arc<Semaphore>,
    pub(crate) tcp_pool_min: usize,
    pub(crate) tcp_pool_max: usize,
    pub(crate) in_clause_chunk: usize,
    pub(crate) startup_table_check: ClickHouseStartupTableCheck,
    #[cfg(test)]
    pub(crate) latest_finalized_slot_for_tests: Option<Option<u64>>,
}

#[derive(Clone)]
pub struct ClickHouseClientOptions {
    pub routing_policy: RoutingPolicy,
    pub shard_routing: Option<ShardRoutingConfig>,
    pub gsfa_hot_addresses: Vec<String>,
    pub gsfa_hot_table: String,
    pub gsfa_hot_local_table: String,
    pub query_timeout: Duration,
    pub tcp_access_check_timeout: Duration,
    pub query_cache: QueryCacheConfig,
    pub fanout_concurrency: usize,
    pub http_concurrency: usize,
    pub http_connect_timeout: Duration,
    pub tcp_pool_min: usize,
    pub tcp_pool_max: usize,
    pub in_clause_chunk: usize,
    pub startup_table_check: ClickHouseStartupTableCheck,
}

#[derive(Deserialize, clickhouse::Row)]
struct TableDefinitionRow {
    #[serde(default)]
    engine: String,
    #[serde(default)]
    create_table_query: String,
}

// ClickHouse closes idle keep-alive connections after its `keep_alive_timeout` (3s by default),
// so the pool idle timeout must stay below that or the client would try to reuse a connection the
// server already closed (broken pipe). The `clickhouse` crate's default client uses 2s for this
// reason; hyper's own default is 90s, so we must set it explicitly on our custom client.
const HTTP_POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(2);
const HTTP_TCP_KEEPALIVE: Duration = Duration::from_secs(60);

/// Builds a ClickHouse HTTP client over an explicitly-configured hyper connection pool.
///
/// The `clickhouse` crate's default HTTP client sets no connect timeout, so a new connection
/// attempt can hang during ClickHouse backpressure. Build the underlying hyper client explicitly
/// so connects fail fast, while preserving the crate's idle-timeout behavior.
///
/// NB: this mirrors the crate default's plain-HTTP connector. superbank builds `clickhouse`
/// without a TLS feature, so ClickHouse connections are HTTP-only, the same as before.
fn build_clickhouse_http_client(
    url: &str,
    database: &str,
    username: &str,
    password: &str,
    connect_timeout: Duration,
) -> HttpClient {
    let mut connector = HttpConnector::new();
    connector.set_keepalive(Some(HTTP_TCP_KEEPALIVE));
    connector.set_connect_timeout(Some(connect_timeout));
    connector.enforce_http(true);

    let mut client = HttpClient::with_http_client(
        HyperClient::builder(TokioExecutor::new())
            .pool_idle_timeout(HTTP_POOL_IDLE_TIMEOUT)
            .build(connector),
    )
    .with_url(url)
    .with_database(database);

    if !username.is_empty() {
        client = client.with_user(username);
    }
    if !password.is_empty() {
        client = client.with_password(password);
    }

    client
}

impl ClickHouseClientOptions {
    pub fn new(
        routing_policy: RoutingPolicy,
        shard_routing: Option<ShardRoutingConfig>,
        gsfa_hot_addresses: Vec<String>,
        gsfa_hot_table: String,
        gsfa_hot_local_table: String,
    ) -> Self {
        Self {
            routing_policy,
            shard_routing,
            gsfa_hot_addresses,
            gsfa_hot_table,
            gsfa_hot_local_table,
            query_timeout: Duration::from_millis(8_000),
            tcp_access_check_timeout: Duration::from_secs(2),
            query_cache: QueryCacheConfig::default(),
            fanout_concurrency: 8,
            http_concurrency: 512,
            http_connect_timeout: Duration::from_secs(2),
            tcp_pool_min: 10,
            tcp_pool_max: 20,
            in_clause_chunk: 512,
            startup_table_check: ClickHouseStartupTableCheck::Exists,
        }
    }

    pub fn with_query_timeout(mut self, timeout: Duration) -> Self {
        self.query_timeout = timeout;
        self
    }

    pub fn with_tcp_access_check_timeout(mut self, timeout: Duration) -> Self {
        self.tcp_access_check_timeout = timeout;
        self
    }

    pub fn with_query_cache_config(mut self, query_cache: QueryCacheConfig) -> Self {
        self.query_cache = query_cache;
        self
    }

    pub fn with_fanout_concurrency(mut self, concurrency: usize) -> Self {
        self.fanout_concurrency = concurrency.max(1);
        self
    }

    pub fn with_http_concurrency(mut self, concurrency: usize) -> Self {
        self.http_concurrency = concurrency.max(1);
        self
    }

    pub fn with_http_connect_timeout(mut self, timeout: Duration) -> Self {
        self.http_connect_timeout = timeout;
        self
    }

    pub fn with_tcp_pool_sizing(mut self, pool_min: usize, pool_max: usize) -> Self {
        self.tcp_pool_max = pool_max.max(1);
        self.tcp_pool_min = pool_min.min(self.tcp_pool_max);
        self
    }

    pub fn with_in_clause_chunk(mut self, chunk: usize) -> Self {
        self.in_clause_chunk = chunk.max(1);
        self
    }

    pub fn with_startup_table_check(mut self, mode: ClickHouseStartupTableCheck) -> Self {
        self.startup_table_check = mode;
        self
    }
}

impl ClickHouseClient {
    pub fn new(
        url: &str,
        database: &str,
        username: &str,
        password: &str,
        options: ClickHouseClientOptions,
    ) -> Self {
        let ClickHouseClientOptions {
            routing_policy,
            shard_routing,
            gsfa_hot_addresses,
            gsfa_hot_table,
            gsfa_hot_local_table,
            query_timeout,
            tcp_access_check_timeout,
            query_cache,
            fanout_concurrency,
            http_concurrency,
            http_connect_timeout,
            tcp_pool_min,
            tcp_pool_max,
            in_clause_chunk,
            startup_table_check,
            ..
        } = options;
        let client =
            build_clickhouse_http_client(url, database, username, password, http_connect_timeout);

        let transaction_table = std::env::var("CLICKHOUSE_TRANSACTION_TABLE")
            .or_else(|_| std::env::var("CLICKHOUSE_SIGNATURE_TABLE"))
            .unwrap_or_else(|_| "default.transactions".to_string());
        let blocks_metadata_table = std::env::var("CLICKHOUSE_BLOCKS_METADATA_TABLE")
            .unwrap_or_else(|_| "default.blocks_metadata".to_string());
        let gsfa_table =
            std::env::var("CLICKHOUSE_GSFA_TABLE").unwrap_or_else(|_| "default.gsfa".to_string());
        let signature_statuses_table = std::env::var("CLICKHOUSE_SIGNATURE_STATUSES_TABLE")
            .unwrap_or_else(|_| "default.signatures".to_string());
        let token_owner_activity_table = std::env::var("CLICKHOUSE_TOKEN_OWNER_ACTIVITY_TABLE")
            .unwrap_or_else(|_| "default.token_owner_activity".to_string());

        let signatures_local_table = shard_routing
            .as_ref()
            .and_then(|config| config.signatures_local_table.clone())
            .or_else(|| derive_local_table_name(&signature_statuses_table, None));
        let token_owner_activity_local_table = shard_routing
            .as_ref()
            .and_then(|config| config.token_owner_activity_local_table.clone())
            .or_else(|| derive_local_table_name(&token_owner_activity_table, None));
        let transactions_local_table = shard_routing
            .as_ref()
            .and_then(|config| config.transactions_local_table.clone())
            .or_else(|| derive_local_table_name(&transaction_table, None));
        let blocks_metadata_local_table = shard_routing
            .as_ref()
            .and_then(|config| config.blocks_metadata_local_table.clone())
            .or_else(|| derive_local_table_name(&blocks_metadata_table, None));

        let gsfa_local_table = shard_routing
            .as_ref()
            .and_then(|config| config.gsfa_local_table.clone())
            .or_else(|| derive_local_table_name(&gsfa_table, None));

        let shard_routing = shard_routing.map(|mut config| {
            if config.gsfa_local_table.is_none() {
                config.gsfa_local_table = gsfa_local_table.clone();
            }
            if config.signatures_local_table.is_none() {
                config.signatures_local_table = signatures_local_table.clone();
            }
            if config.token_owner_activity_local_table.is_none() {
                config.token_owner_activity_local_table = token_owner_activity_local_table.clone();
            }
            if config.transactions_local_table.is_none() {
                config.transactions_local_table = transactions_local_table.clone();
            }
            if config.blocks_metadata_local_table.is_none() {
                config.blocks_metadata_local_table = blocks_metadata_local_table.clone();
            }
            config
        });

        Self {
            client,
            url: url.to_string(),
            database: database.to_string(),
            username: username.to_string(),
            password: password.to_string(),
            signature_slot_cache: Arc::new(SignatureSlotCache::from_env()),
            transaction_table,
            blocks_metadata_table,
            gsfa_table,
            gsfa_hot_table,
            gsfa_hot_local_table,
            gsfa_hot_addresses,
            gsfa_hot_pubkeys: HashSet::new(),
            signature_statuses_table,
            token_owner_activity_table,
            signatures_local_table,
            token_owner_activity_local_table,
            transactions_local_table,
            blocks_metadata_local_table,
            token_owner_activity_available: true,
            bucket_moduli: BucketModuli::default(),
            allow_query_settings: !std::env::var("CLICKHOUSE_DISABLE_QUERY_SETTINGS")
                .map(|value| env_truthy(&value))
                .unwrap_or(false),
            query_cache,
            gsfa_router: None,
            shard_topology: None,
            shard_routing,
            routing_policy,

            query_timeout,
            tcp_access_check_timeout,
            http_connect_timeout,
            fanout_sem: Arc::new(Semaphore::new(fanout_concurrency.max(1))),
            http_query_sem: Arc::new(Semaphore::new(http_concurrency.max(1))),
            tcp_pool_max: tcp_pool_max.max(1),
            tcp_pool_min: tcp_pool_min.min(tcp_pool_max.max(1)),
            in_clause_chunk: in_clause_chunk.max(1),
            startup_table_check,
            #[cfg(test)]
            latest_finalized_slot_for_tests: None,
        }
    }

    pub fn token_owner_activity_available(&self) -> bool {
        self.token_owner_activity_available
    }

    #[cfg(test)]
    pub fn set_token_owner_activity_available_for_tests(&mut self, available: bool) {
        self.token_owner_activity_available = available;
    }

    #[cfg(test)]
    pub fn set_latest_finalized_slot_for_tests(&mut self, latest_slot: Option<u64>) {
        self.latest_finalized_slot_for_tests = Some(latest_slot);
    }

    pub(crate) fn scope_shard_direct(&self) -> bool {
        self.routing_policy.scope == RoutingScope::ShardDirect
    }

    pub(crate) fn gsfa_hot_routing_configured(&self) -> bool {
        has_usable_gsfa_hot_addresses(&self.gsfa_hot_addresses)
    }

    pub(crate) fn transport_tcp(&self) -> bool {
        self.routing_policy.transport == RoutingTransport::Tcp
    }

    pub(crate) fn transport_http(&self) -> bool {
        self.routing_policy.transport == RoutingTransport::Http
    }

    pub(crate) fn shard_tcp_query_timeout(&self) -> Duration {
        shard_tcp_query_timeout_for(self.query_timeout)
    }

    pub(crate) fn http_query_cleanup(
        &self,
        operation: &'static str,
        query_id: String,
    ) -> HttpQueryCleanup {
        HttpQueryCleanup::new(
            self.client.clone(),
            self.shard_routing
                .as_ref()
                .map(|config| config.cluster.clone()),
            operation,
            self.query_timeout,
            query_id,
        )
    }

    pub(crate) fn signatures_bucket_modulus(&self) -> u64 {
        self.bucket_moduli.signatures
    }

    pub(crate) fn token_owner_bucket_modulus(&self) -> u64 {
        self.bucket_moduli.token_owner_activity
    }

    pub(crate) fn gsfa_bucket_modulus_for_address(&self, pubkey: &Pubkey) -> u64 {
        if self.gsfa_hot_pubkeys.contains(pubkey) {
            self.bucket_moduli.gsfa_hot
        } else {
            self.bucket_moduli.gsfa
        }
    }

    pub(crate) fn hot_shard_topology(&self) -> ProcessingResult<&Arc<ShardTopology>> {
        self.shard_topology.as_ref().ok_or_else(|| {
            ProcessingError::database_msg(
                "GSFA hot routing requires shard topology discovery and validated shard-local hot tables"
                    .to_string(),
            )
        })
    }

    pub(crate) fn select_settings_clause(
        &self,
        operation: &'static str,
        freshness: QueryFreshnessClass,
    ) -> String {
        self.select_settings_clause_with_timeout(operation, freshness, self.query_timeout)
    }

    pub(crate) fn select_settings_clause_with_timeout(
        &self,
        operation: &'static str,
        freshness: QueryFreshnessClass,
        timeout: Duration,
    ) -> String {
        // Bound the server-side query lifetime so a query ClickHouse keeps running after
        // `with_timeout` drops the HTTP future does not linger and hold a connection.
        append_max_execution_time_setting(
            &build_select_settings_clause(
                self.allow_query_settings,
                freshness,
                &self.query_cache,
                false,
                operation,
            ),
            timeout,
        )
    }

    pub(crate) fn select_settings_clause_with_condition_cache(
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

    pub(crate) fn select_get_transaction_settings_clause(
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

    pub(crate) async fn with_timeout<T>(
        &self,
        operation: &'static str,
        fut: impl std::future::Future<Output = ProcessingResult<T>>,
    ) -> ProcessingResult<T> {
        self.with_timeout_duration(operation, self.query_timeout, fut)
            .await
    }

    /// [`Self::with_timeout`] with an explicit deadline, for operations whose
    /// budget differs from the interactive query timeout (e.g. disk-cache
    /// backfill range scans).
    pub(crate) async fn with_timeout_duration<T>(
        &self,
        operation: &'static str,
        timeout: std::time::Duration,
        fut: impl std::future::Future<Output = ProcessingResult<T>>,
    ) -> ProcessingResult<T> {
        // Gate every direct (non-fanout) ClickHouse HTTP query on a global permit so concurrent
        // HTTP connections do not track raw request/batch concurrency. Fanout paths use
        // `fanout_sem` and are not gated here; the surrounding request timeout bounds the wait for
        // a permit under saturation.
        let _permit = self.http_query_sem.acquire().await.ok();
        // Box the query future onto the heap. `fut` (a ClickHouse query state machine) is large in
        // debug builds, and `with_timeout` is composed deeply on some request paths (a JSON-RPC
        // batch sub-request chains several queries plus hydration, and `dispatch_json_rpc_request`
        // is sized to its largest method arm). Keeping `fut` inline lets those sizes compound up
        // the call tree and overflow the (2 MiB) worker/test thread stack; boxing keeps each
        // `with_timeout` future pointer-sized in its caller.
        let fut = Box::pin(fut);
        match tokio::time::timeout(timeout, fut).await {
            Ok(result) => result,
            Err(_) => {
                crate::metrics::clickhouse_timeout(operation);
                Err(ProcessingError::timeout_msg(format!(
                    "ClickHouse operation '{operation}' timed out after {timeout:?}"
                )))
            }
        }
    }

    async fn describe_table_http(&self, table: &str) -> ProcessingResult<Vec<DescribeTableRow>> {
        self.with_timeout("describe_table_http", async {
            let (database, table_name) = split_table_reference(&self.database, table);
            let database = escape_clickhouse_string(database);
            let table_name = escape_clickhouse_string(table_name);
            let query = format!(
                "SELECT name, default_kind AS default_type, default_expression \
                 FROM system.columns \
                 WHERE database = '{database}' AND table = '{table_name}' \
                 ORDER BY position"
            );
            let rows = self
                .client
                .query(&query)
                .fetch_all::<DescribeTableRow>()
                .await
                .map_err(|e| ProcessingError::database(e.to_string(), e))?;

            if rows.is_empty() {
                return Err(ProcessingError::database_msg(format!(
                    "table {table} returned no columns from system.columns"
                )));
            }

            Ok(rows)
        })
        .await
    }

    async fn detect_bucket_modulus_http(
        &self,
        table: &str,
        bucket_column: BucketColumn,
    ) -> ProcessingResult<u64> {
        let rows = self.describe_table_http(table).await?;
        bucket_modulus_from_describe_rows(&rows, table, bucket_column)
    }

    async fn detect_optional_bucket_modulus_http(
        &self,
        dataset: &'static str,
        table: &str,
        bucket_column: BucketColumn,
    ) -> Option<u64> {
        match self.detect_bucket_modulus_http(table, bucket_column).await {
            Ok(modulus) => Some(modulus),
            Err(err) => {
                tracing::warn!(
                    "Failed to detect bucket modulus for optional {dataset} table '{}'; hot routing may stay disabled (error: {})",
                    table,
                    err
                );
                None
            }
        }
    }

    async fn fetch_table_definition_http(
        &self,
        table: &str,
    ) -> ProcessingResult<TableDefinitionRow> {
        self.with_timeout("fetch_table_definition_http", async {
            let (database, table_name) = split_table_reference(&self.database, table);
            let database = escape_clickhouse_string(database);
            let table_name = escape_clickhouse_string(table_name);
            let query = format!(
                "SELECT engine, create_table_query \
                 FROM system.tables \
                 WHERE database = '{database}' AND name = '{table_name}' \
                 LIMIT 1"
            );
            let rows = self
                .client
                .query(&query)
                .fetch_all::<TableDefinitionRow>()
                .await
                .map_err(|e| ProcessingError::database(e.to_string(), e))?;

            rows.into_iter().next().ok_or_else(|| {
                ProcessingError::database_msg(format!(
                    "table {table} returned no definition from system.tables"
                ))
            })
        })
        .await
    }

    async fn validate_gsfa_shard_layout(&self, gsfa_local_table: &str) -> ProcessingResult<()> {
        let gsfa_definition = self.fetch_table_definition_http(&self.gsfa_table).await?;

        validate_gsfa_shard_layout_query(
            &self.database,
            &self.gsfa_table,
            gsfa_local_table,
            &gsfa_definition.engine,
            &gsfa_definition.create_table_query,
        )
    }

    fn ensure_matching_bucket_modulus(
        dataset: &str,
        distributed_table: &str,
        distributed_modulus: u64,
        local_table: &str,
        local_modulus: u64,
    ) -> ProcessingResult<()> {
        if distributed_modulus == local_modulus {
            return Ok(());
        }

        Err(ProcessingError::database_msg(format!(
            "{dataset} bucket modulus mismatch: {distributed_table} uses {distributed_modulus}, {local_table} uses {local_modulus}"
        )))
    }

    async fn discover_bucket_moduli(&mut self) -> ProcessingResult<()> {
        let gsfa_modulus = self
            .detect_bucket_modulus_http(&self.gsfa_table, BucketColumn::Address)
            .await?;
        let signatures_modulus = self
            .detect_bucket_modulus_http(&self.signature_statuses_table, BucketColumn::Signature)
            .await?;

        let token_owner_modulus = if self.token_owner_activity_available {
            Some(
                self.detect_bucket_modulus_http(
                    &self.token_owner_activity_table,
                    BucketColumn::Owner,
                )
                .await?,
            )
        } else {
            None
        };

        let hot_modulus = if self.gsfa_hot_routing_configured() {
            self.detect_optional_bucket_modulus_http(
                "gsfa_hot",
                &self.gsfa_hot_table,
                BucketColumn::Address,
            )
            .await
        } else {
            None
        };

        if self.scope_shard_direct()
            && let Some(topology) = &self.shard_topology
        {
            if let Some(router) = &self.gsfa_router {
                let local_modulus = detect_bucket_modulus_on_shards(
                    topology,
                    &router.local_table,
                    BucketColumn::Address,
                    self.query_timeout,
                )
                .await?;
                Self::ensure_matching_bucket_modulus(
                    "gsfa",
                    &self.gsfa_table,
                    gsfa_modulus,
                    &router.local_table,
                    local_modulus,
                )?;
            }

            if let Some(local_table) = &self.signatures_local_table {
                let local_modulus = detect_bucket_modulus_on_shards(
                    topology,
                    local_table,
                    BucketColumn::Signature,
                    self.query_timeout,
                )
                .await?;
                Self::ensure_matching_bucket_modulus(
                    "signatures",
                    &self.signature_statuses_table,
                    signatures_modulus,
                    local_table,
                    local_modulus,
                )?;
            }

            if let (Some(distributed_modulus), Some(local_table)) =
                (token_owner_modulus, &self.token_owner_activity_local_table)
            {
                let local_modulus = detect_bucket_modulus_on_shards(
                    topology,
                    local_table,
                    BucketColumn::Owner,
                    self.query_timeout,
                )
                .await?;
                Self::ensure_matching_bucket_modulus(
                    "token_owner_activity",
                    &self.token_owner_activity_table,
                    distributed_modulus,
                    local_table,
                    local_modulus,
                )?;
            }
        }

        if let (Some(topology), Some(distributed_modulus)) = (&self.shard_topology, hot_modulus) {
            let local_modulus = detect_bucket_modulus_on_shards(
                topology,
                &self.gsfa_hot_local_table,
                BucketColumn::Address,
                self.query_timeout,
            )
            .await?;
            Self::ensure_matching_bucket_modulus(
                "gsfa_hot",
                &self.gsfa_hot_table,
                distributed_modulus,
                &self.gsfa_hot_local_table,
                local_modulus,
            )?;
        }

        self.bucket_moduli.gsfa = gsfa_modulus;
        self.bucket_moduli.signatures = signatures_modulus;
        if let Some(modulus) = token_owner_modulus {
            self.bucket_moduli.token_owner_activity = modulus;
        }
        if let Some(modulus) = hot_modulus {
            self.bucket_moduli.gsfa_hot = modulus;
        }

        if let Some(gsfa_hot) = hot_modulus {
            tracing::info!(
                gsfa = self.bucket_moduli.gsfa,
                gsfa_hot,
                signatures = self.bucket_moduli.signatures,
                token_owner_activity = self.bucket_moduli.token_owner_activity,
                "Discovered ClickHouse bucket moduli"
            );
        } else {
            tracing::info!(
                gsfa = self.bucket_moduli.gsfa,
                signatures = self.bucket_moduli.signatures,
                token_owner_activity = self.bucket_moduli.token_owner_activity,
                "Discovered ClickHouse bucket moduli"
            );
        }

        Ok(())
    }

    async fn detect_readonly_setting(&self) -> ProcessingResult<u8> {
        #[derive(Deserialize, clickhouse::Row)]
        struct ReadonlyRow {
            readonly: u8,
        }

        let row = self
            .with_timeout("detect_readonly_setting", async {
                self.client
                    .query("SELECT toUInt8(getSetting('readonly')) AS readonly")
                    .fetch_one::<ReadonlyRow>()
                    .await
                    .map_err(|e| ProcessingError::database(e.to_string(), e))
            })
            .await?;

        Ok(row.readonly)
    }

    pub async fn create_tables(&mut self) -> ProcessingResult<()> {
        let table_access_context = |table: &str| {
            format!(
                "Cannot access ClickHouse table '{table}'. Ensure ClickHouse is running, credentials are correct, the table exists, and network connectivity is available"
            )
        };

        let gsfa_table = &self.gsfa_table;
        match self.startup_table_check {
            ClickHouseStartupTableCheck::Count => {
                let row_count = self
                    .with_timeout("startup_gsfa_count", async {
                        self.client
                            .query(&format!("SELECT COUNT(*) FROM {}", gsfa_table))
                            .fetch_one::<u64>()
                            .await
                            .map_err(|e| {
                                ProcessingError::database(table_access_context(gsfa_table), e)
                            })
                    })
                    .await?;

                tracing::info!(
                    "📊 Database initialized - {} table: {} rows",
                    gsfa_table,
                    row_count
                );
            }
            ClickHouseStartupTableCheck::Exists => {
                self.with_timeout("startup_gsfa_exists", async {
                    self.client
                        .query(&format!("SELECT count() FROM {} WHERE 0", gsfa_table))
                        .fetch_one::<u64>()
                        .await
                        .map(|_| ())
                        .map_err(|e| ProcessingError::database(table_access_context(gsfa_table), e))
                })
                .await?;
                tracing::info!("📊 Database initialized - {} table accessible", gsfa_table);
            }
        }

        let signature_statuses_table = &self.signature_statuses_table;
        match self.startup_table_check {
            ClickHouseStartupTableCheck::Count => {
                let signature_row_count = self
                    .with_timeout("startup_signatures_count", async {
                        self.client
                            .query(&format!(
                                "SELECT COUNT(*) FROM {}",
                                signature_statuses_table
                            ))
                            .fetch_one::<u64>()
                            .await
                            .map_err(|e| {
                                ProcessingError::database(
                                    table_access_context(signature_statuses_table),
                                    e,
                                )
                            })
                    })
                    .await?;

                tracing::info!(
                    "📊 Database initialized - {} table: {} rows",
                    signature_statuses_table,
                    signature_row_count
                );
            }
            ClickHouseStartupTableCheck::Exists => {
                self.with_timeout("startup_signatures_exists", async {
                    self.client
                        .query(&format!(
                            "SELECT count() FROM {} WHERE 0",
                            signature_statuses_table
                        ))
                        .fetch_one::<u64>()
                        .await
                        .map(|_| ())
                        .map_err(|e| {
                            ProcessingError::database(
                                table_access_context(signature_statuses_table),
                                e,
                            )
                        })
                })
                .await?;
                tracing::info!(
                    "📊 Database initialized - {} table accessible",
                    signature_statuses_table
                );
            }
        }

        let token_owner_activity_table = &self.token_owner_activity_table;
        match self.startup_table_check {
            ClickHouseStartupTableCheck::Count => {
                match self
                    .with_timeout("startup_token_owner_activity_count", async {
                        self.client
                            .query(&format!(
                                "SELECT COUNT(*) FROM {}",
                                token_owner_activity_table
                            ))
                            .fetch_one::<u64>()
                            .await
                            .map_err(|e| ProcessingError::database(e.to_string(), e))
                    })
                    .await
                {
                    Ok(count) => {
                        self.token_owner_activity_available = true;
                        tracing::info!(
                            "📊 Database initialized - {} table: {} rows",
                            token_owner_activity_table,
                            count
                        );
                    }
                    Err(e) => {
                        self.token_owner_activity_available = false;
                        tracing::warn!(
                            "Token owner activity table '{}' unavailable; tokenAccounts filters disabled. Error: {}",
                            token_owner_activity_table,
                            e
                        );
                    }
                }
            }
            ClickHouseStartupTableCheck::Exists => {
                match self
                    .with_timeout("startup_token_owner_activity_exists", async {
                        self.client
                            .query(&format!(
                                "SELECT count() FROM {} WHERE 0",
                                token_owner_activity_table
                            ))
                            .fetch_one::<u64>()
                            .await
                            .map(|_| ())
                            .map_err(|e| ProcessingError::database(e.to_string(), e))
                    })
                    .await
                {
                    Ok(()) => {
                        self.token_owner_activity_available = true;
                        tracing::info!(
                            "📊 Database initialized - {} table accessible",
                            token_owner_activity_table
                        );
                    }
                    Err(e) => {
                        self.token_owner_activity_available = false;
                        tracing::warn!(
                            "Token owner activity table '{}' unavailable; tokenAccounts filters disabled. Error: {}",
                            token_owner_activity_table,
                            e
                        );
                    }
                }
            }
        }

        let blocks_metadata_table = &self.blocks_metadata_table;
        match self.startup_table_check {
            ClickHouseStartupTableCheck::Count => {
                let blocks_row_count = self
                    .with_timeout("startup_blocks_metadata_count", async {
                        self.client
                            .query(&format!("SELECT COUNT(*) FROM {}", blocks_metadata_table))
                            .fetch_one::<u64>()
                            .await
                            .map_err(|e| {
                                ProcessingError::database(
                                    table_access_context(blocks_metadata_table),
                                    e,
                                )
                            })
                    })
                    .await?;

                tracing::info!(
                    "📊 Database initialized - {} table: {} rows",
                    blocks_metadata_table,
                    blocks_row_count
                );
            }
            ClickHouseStartupTableCheck::Exists => {
                self.with_timeout("startup_blocks_metadata_exists", async {
                    self.client
                        .query(&format!(
                            "SELECT count() FROM {} WHERE 0",
                            blocks_metadata_table
                        ))
                        .fetch_one::<u64>()
                        .await
                        .map(|_| ())
                        .map_err(|e| {
                            ProcessingError::database(
                                table_access_context(blocks_metadata_table),
                                e,
                            )
                        })
                })
                .await?;
                tracing::info!(
                    "📊 Database initialized - {} table accessible",
                    blocks_metadata_table
                );
            }
        }

        let transaction_table = &self.transaction_table;
        match self.startup_table_check {
            ClickHouseStartupTableCheck::Count => {
                let tx_row_count = self
                    .with_timeout("startup_transactions_count", async {
                        self.client
                            .query(&format!("SELECT COUNT(*) FROM {}", transaction_table))
                            .fetch_one::<u64>()
                            .await
                            .map_err(|e| {
                                ProcessingError::database(
                                    table_access_context(transaction_table),
                                    e,
                                )
                            })
                    })
                    .await?;

                tracing::info!(
                    "📊 Database initialized - {} table: {} rows",
                    transaction_table,
                    tx_row_count
                );
            }
            ClickHouseStartupTableCheck::Exists => {
                self.with_timeout("startup_transactions_exists", async {
                    self.client
                        .query(&format!(
                            "SELECT count() FROM {} WHERE 0",
                            transaction_table
                        ))
                        .fetch_one::<u64>()
                        .await
                        .map(|_| ())
                        .map_err(|e| {
                            ProcessingError::database(table_access_context(transaction_table), e)
                        })
                })
                .await?;
                tracing::info!(
                    "📊 Database initialized - {} table accessible",
                    transaction_table
                );
            }
        }

        if self.allow_query_settings {
            match self.detect_readonly_setting().await {
                Ok(readonly) if readonly > 0 => {
                    self.allow_query_settings = false;
                    tracing::info!(
                        "ClickHouse readonly mode detected (readonly={}); disabling query SETTINGS overrides",
                        readonly
                    );
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::debug!(
                        "Failed to detect ClickHouse readonly setting; keeping query SETTINGS enabled: {}",
                        e
                    );
                }
            }
        }

        if self.scope_shard_direct() && self.shard_routing.is_none() {
            return Err(ProcessingError::database_msg(
                "CLICKHOUSE_SCOPE=shard-direct requires shard routing configuration",
            ));
        }

        let hot_routing_configured = self.gsfa_hot_routing_configured();

        if let Some(config) = self.shard_routing.clone() {
            match self.build_shard_topology(&config).await {
                Ok(topology) => {
                    if self.transport_tcp() {
                        self.check_tcp_access(&topology).await?;
                    }

                    let topology = Arc::new(topology);
                    self.shard_topology = Some(topology.clone());

                    if self.scope_shard_direct() {
                        if let Some(local_table) = config.gsfa_local_table.clone() {
                            if let Err(e) = validate_table_schema_on_shards(
                                topology.as_ref(),
                                &local_table,
                                &GSFA_REQUIRED_COLUMNS,
                                self.query_timeout,
                            )
                            .await
                            {
                                tracing::warn!(
                                    "GSFA shard routing disabled; local table validation failed: {}",
                                    e
                                );
                            } else {
                                self.validate_gsfa_shard_layout(&local_table).await?;
                                self.gsfa_router = Some(GsfaShardRouter {
                                    local_table,
                                    topology: topology.clone(),
                                    query_timeout: self.shard_tcp_query_timeout(),
                                });
                            }
                        } else {
                            tracing::warn!(
                                "GSFA shard routing disabled; local table not configured"
                            );
                        }

                        if let Some(local_table) = config.signatures_local_table.clone()
                            && let Err(e) = validate_table_schema_on_shards(
                                topology.as_ref(),
                                &local_table,
                                &SIGNATURES_REQUIRED_COLUMNS,
                                self.query_timeout,
                            )
                            .await
                        {
                            tracing::warn!(
                                "Signature shard routing disabled; local table validation failed: {}",
                                e
                            );
                            self.signatures_local_table = None;
                        }

                        if let Some(local_table) = config.token_owner_activity_local_table.clone()
                            && let Err(e) = validate_table_schema_on_shards(
                                topology.as_ref(),
                                &local_table,
                                &TOKEN_OWNER_REQUIRED_COLUMNS,
                                self.query_timeout,
                            )
                            .await
                        {
                            tracing::warn!(
                                "Token owner shard routing disabled; local table validation failed: {}",
                                e
                            );
                            self.token_owner_activity_local_table = None;
                        }
                    }

                    if hot_routing_configured {
                        validate_table_schema_on_shards(
                            topology.as_ref(),
                            &self.gsfa_hot_local_table,
                            &GSFA_REQUIRED_COLUMNS,
                            self.query_timeout,
                        )
                        .await
                        .map_err(|e| {
                            ProcessingError::database_msg(format!(
                                "GSFA hot routing is enabled but shard-local hot table validation failed for '{}': {}",
                                self.gsfa_hot_local_table, e
                            ))
                        })?;
                    }
                }
                Err(e) => {
                    if self.scope_shard_direct()
                        || hot_routing_configured
                        || config.topology_config_path.is_some()
                    {
                        return Err(ProcessingError::database_msg(format!(
                            "ClickHouse shard routing is required but topology initialization failed: {}",
                            e
                        )));
                    }
                }
            }
        }

        self.discover_bucket_moduli().await?;
        self.initialize_gsfa_hot_addresses().await;

        if !self.gsfa_hot_pubkeys.is_empty() {
            tracing::info!(
                "GSFA hot addresses use shard-local fanout over '{}' while '{}' remains the distributed hot table",
                self.gsfa_hot_local_table,
                self.gsfa_hot_table,
            );
        }

        Ok(())
    }

    fn build_http_client(&self, url: &str) -> HttpClient {
        build_clickhouse_http_client(
            url,
            &self.database,
            &self.username,
            &self.password,
            self.http_connect_timeout,
        )
    }

    async fn build_shard_topology(
        &self,
        config: &ShardRoutingConfig,
    ) -> ProcessingResult<ShardTopology> {
        let base_url = Url::parse(&self.url).map_err(|e| {
            ProcessingError::database(format!("Invalid CLICKHOUSE_URL '{}'", self.url), e)
        })?;

        let http_port = config.shard_http_port.or(base_url.port()).ok_or_else(|| {
            ProcessingError::database_msg(
                "CLICKHOUSE_URL has no port and CLICKHOUSE_SHARD_HTTP_PORT is not set",
            )
        })?;

        if let Some(path) = config.topology_config_path.as_deref() {
            tracing::debug!(
                cluster = %config.cluster,
                topology_config = %path,
                http_port,
                "ClickHouse shard routing enabled; loading topology config"
            );

            let topology_config = load_clickhouse_topology_config(path).await?;
            let selected_nodes = selected_configured_shard_nodes(&topology_config);
            let configured_lines = topology_config
                .nodes
                .iter()
                .enumerate()
                .map(|(idx, node)| {
                    format!(
                        "order={idx} shard={} weight={} hostname='{}' ip_address={} tcp_port={}",
                        node.shard_id,
                        node.shard_weight,
                        node.hostname,
                        node.ip_address,
                        node.tcp_port
                    )
                })
                .collect::<Vec<_>>();
            tracing::debug!(
                cluster = %config.cluster,
                topology_config = %path,
                shard_count = selected_nodes.len(),
                node_count = topology_config.nodes.len(),
                topology = %configured_lines.join(" | "),
                "ClickHouse shard routing topology config loaded"
            );

            let mut shards = Vec::with_capacity(selected_nodes.len());
            let mut weights = Vec::with_capacity(selected_nodes.len());
            let mut total_weight = 0_u64;

            for node in selected_nodes {
                if node.shard_weight == 0 {
                    return Err(ProcessingError::database_msg(format!(
                        "Shard {} has zero weight in ClickHouse topology config '{}'",
                        node.shard_id, path
                    )));
                }

                let host = node.ip_address.to_string();
                let tcp_pool = Arc::new(build_tcp_pool(
                    &self.database,
                    &self.username,
                    &self.password,
                    &host,
                    node.tcp_port,
                    self.shard_tcp_query_timeout(),
                    TcpPoolSizing {
                        min: self.tcp_pool_min,
                        max: self.tcp_pool_max,
                    },
                )?);

                let mut shard_url = base_url.clone();
                shard_url.set_host(Some(&host)).map_err(|_| {
                    ProcessingError::database_msg(format!(
                        "Invalid shard ip-address '{host}' in ClickHouse topology config '{path}'"
                    ))
                })?;
                shard_url.set_port(Some(http_port)).map_err(|_| {
                    ProcessingError::database_msg(format!(
                        "Invalid HTTP port '{http_port}' for '{}'",
                        config.cluster
                    ))
                })?;

                let http_client = self.build_http_client(shard_url.as_str());

                total_weight = total_weight
                    .checked_add(u64::from(node.shard_weight))
                    .ok_or_else(|| {
                        ProcessingError::database_msg("Total shard weight overflowed u64")
                    })?;

                shards.push(ShardTarget {
                    shard_num: node.shard_id,
                    tcp_pool,
                    http_client,
                    host,
                    tcp_port: node.tcp_port,
                });
                weights.push(u64::from(node.shard_weight));
            }

            if total_weight == 0 {
                return Err(ProcessingError::database_msg("Total shard weight is zero"));
            }

            return Ok(ShardTopology {
                total_weight,
                shards,
                weights,
                allow_query_settings: self.allow_query_settings,
                query_cache: self.query_cache.clone(),
                query_timeout: self.query_timeout,
            });
        }

        tracing::debug!(
            cluster = %config.cluster,
            http_port,
            "ClickHouse shard routing enabled; discovering cluster topology"
        );

        let cluster_name = escape_clickhouse_string(&config.cluster);
        let cluster_query = format!(
            "SELECT shard_num, shard_weight, replica_num, host_name, host_address, port, is_local\n             FROM system.clusters\n             WHERE cluster = '{cluster_name}'\n             ORDER BY shard_num, replica_num",
        );

        let rows: Vec<ClusterRow> = self
            .with_timeout("build_shard_topology", async {
                self.client
                    .query(&cluster_query)
                    .fetch_all()
                    .await
                    .map_err(|e| {
                        ProcessingError::database(
                            format!("Failed to query system.clusters for '{}'", config.cluster),
                            e,
                        )
                    })
            })
            .await?;

        if rows.is_empty() {
            return Err(ProcessingError::database_msg(format!(
                "No shards found in system.clusters for '{}'",
                config.cluster
            )));
        }

        let mut by_shard: BTreeMap<u32, Vec<ClusterRow>> = BTreeMap::new();
        for row in rows {
            by_shard.entry(row.shard_num).or_default().push(row);
        }

        let mut cluster_lines = Vec::new();
        for (shard_num, replicas) in &by_shard {
            for replica in replicas {
                let resolved_host = resolve_host(replica).unwrap_or_else(|| "<empty>".to_string());
                cluster_lines.push(format!(
                    "shard={shard_num} weight={} replica={} host_name='{}' host_address='{}' resolved_host='{}' tcp_port={} is_local={}",
                    replica.shard_weight,
                    replica.replica_num,
                    replica.host_name,
                    replica.host_address,
                    resolved_host,
                    replica.port,
                    replica.is_local
                ));
            }
        }

        tracing::debug!(
            cluster = %config.cluster,
            shard_count = by_shard.len(),
            replica_count = cluster_lines.len(),
            topology = %cluster_lines.join(" | "),
            "ClickHouse shard routing cluster config discovered"
        );

        let mut shards = Vec::with_capacity(by_shard.len());
        let mut weights = Vec::with_capacity(by_shard.len());
        let mut total_weight = 0_u64;

        for (shard_num, replicas) in by_shard {
            let chosen = pick_replica(&replicas).ok_or_else(|| {
                ProcessingError::database_msg(format!(
                    "No replicas available for shard {shard_num} in '{}'",
                    config.cluster
                ))
            })?;

            if chosen.shard_weight == 0 {
                return Err(ProcessingError::database_msg(format!(
                    "Shard {shard_num} has zero weight in '{}'",
                    config.cluster
                )));
            }

            let host = resolve_host(chosen).ok_or_else(|| {
                ProcessingError::database_msg(format!(
                    "Shard {shard_num} has empty host_name/host_address in '{}'",
                    config.cluster
                ))
            })?;

            let tcp_pool = Arc::new(build_tcp_pool(
                &self.database,
                &self.username,
                &self.password,
                &host,
                chosen.port,
                self.shard_tcp_query_timeout(),
                TcpPoolSizing {
                    min: self.tcp_pool_min,
                    max: self.tcp_pool_max,
                },
            )?);

            let mut shard_url = base_url.clone();
            shard_url.set_host(Some(&host)).map_err(|_| {
                ProcessingError::database_msg(format!(
                    "Invalid shard host '{host}' for '{}'",
                    config.cluster
                ))
            })?;
            shard_url.set_port(Some(http_port)).map_err(|_| {
                ProcessingError::database_msg(format!(
                    "Invalid HTTP port '{http_port}' for '{}'",
                    config.cluster
                ))
            })?;

            let http_client = self.build_http_client(shard_url.as_str());

            total_weight = total_weight
                .checked_add(u64::from(chosen.shard_weight))
                .ok_or_else(|| {
                    ProcessingError::database_msg("Total shard weight overflowed u64")
                })?;

            shards.push(ShardTarget {
                shard_num,
                tcp_pool,
                http_client,
                host,
                tcp_port: chosen.port,
            });
            weights.push(u64::from(chosen.shard_weight));
        }

        if total_weight == 0 {
            return Err(ProcessingError::database_msg("Total shard weight is zero"));
        }

        Ok(ShardTopology {
            total_weight,
            shards,
            weights,
            allow_query_settings: self.allow_query_settings,
            query_cache: self.query_cache.clone(),
            query_timeout: self.query_timeout,
        })
    }

    async fn check_tcp_access(&self, topology: &ShardTopology) -> ProcessingResult<()> {
        let tcp_connect_timeout = self.tcp_access_check_timeout;
        let mut failures = Vec::new();

        for shard in &topology.shards {
            let mut client = match tokio::time::timeout(
                tcp_connect_timeout,
                shard.tcp_pool.get_handle(),
            )
            .await
            {
                Ok(Ok(handle)) => handle,
                Ok(Err(e)) => {
                    failures.push((shard.host.clone(), shard.tcp_port, e.to_string()));
                    continue;
                }
                Err(_) => {
                    failures.push((shard.host.clone(), shard.tcp_port, "timeout".to_string()));
                    continue;
                }
            };

            match tokio::time::timeout(tcp_connect_timeout, client.query("SELECT 1").fetch_all())
                .await
            {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => failures.push((shard.host.clone(), shard.tcp_port, e.to_string())),
                Err(_) => {
                    failures.push((shard.host.clone(), shard.tcp_port, "timeout".to_string()))
                }
            }
        }

        if failures.is_empty() {
            return Ok(());
        }

        let sample = failures
            .iter()
            .take(3)
            .map(|(host, port, err)| format!("{host}:{port} ({err})"))
            .collect::<Vec<_>>()
            .join(", ");
        Err(ProcessingError::database_msg(format!(
            "ClickHouse TCP access check failed for {} of {} shards: {}",
            failures.len(),
            topology.shards.len(),
            sample
        )))
    }
}

fn normalize_clickhouse_ddl(value: &str) -> String {
    value
        .chars()
        .filter(|ch| !ch.is_ascii_whitespace() && *ch != '`')
        .collect::<String>()
        .to_ascii_lowercase()
}

fn materialized_view_uses_distributed_engine_for_local_table_by_address(
    default_database: &str,
    create_table_query: &str,
    local_table: &str,
) -> bool {
    let normalized = normalize_clickhouse_ddl(create_table_query);
    if !normalized.contains("engine=distributed(") {
        return false;
    }

    let (local_database, local_table_name) = split_table_reference(default_database, local_table);
    let local_database = local_database.to_ascii_lowercase();
    let local_table_name = local_table_name.to_ascii_lowercase();
    let expected_suffix =
        format!(",'{local_database}','{local_table_name}',cityhash64(address))as");

    normalized.contains(expected_suffix.as_str())
}

fn validate_gsfa_shard_layout_query(
    default_database: &str,
    gsfa_table: &str,
    gsfa_local_table: &str,
    gsfa_engine: &str,
    gsfa_create_table_query: &str,
) -> ProcessingResult<()> {
    if !gsfa_engine.eq_ignore_ascii_case("materializedview") {
        return Err(ProcessingError::database_msg(format!(
            "incompatible GSFA shard-direct layout: configured GSFA table '{}' must itself be a materialized view with ENGINE = Distributed(..., '{}', cityHash64(address))",
            gsfa_table, gsfa_local_table
        )));
    }

    if !materialized_view_uses_distributed_engine_for_local_table_by_address(
        default_database,
        gsfa_create_table_query,
        gsfa_local_table,
    ) {
        return Err(ProcessingError::database_msg(format!(
            "incompatible GSFA shard-direct layout: configured GSFA table '{}' must itself be a materialized view with ENGINE = Distributed(..., '{}', cityHash64(address)); legacy split layouts with a separate 'gsfa_mv' object are not supported in shard-direct mode",
            gsfa_table, gsfa_local_table
        )));
    }

    Ok(())
}

fn split_table_reference<'a>(default_database: &'a str, table: &'a str) -> (&'a str, &'a str) {
    match table.rsplit_once('.') {
        Some((database, table_name)) if !database.is_empty() && !table_name.is_empty() => {
            (database.trim_matches('`'), table_name.trim_matches('`'))
        }
        _ => (default_database, table.trim_matches('`')),
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use super::{
        ClickHouseClient, ClickHouseClientOptions, kill_query_sql, shard_tcp_query_timeout_for,
        split_table_reference, validate_gsfa_shard_layout_query,
    };
    use crate::clickhouse::{
        QueryCacheConfig, QueryFreshnessClass, RoutingPolicy, RoutingScope, RoutingTransport,
        ShardRoutingConfig,
    };

    struct TempTopologyConfig {
        path: PathBuf,
    }

    impl TempTopologyConfig {
        fn new(contents: &str) -> Self {
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock should be after unix epoch")
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "superbank-rpc-topology-{}-{unique}.yaml",
                std::process::id()
            ));
            std::fs::write(&path, contents).expect("topology config should be writable");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempTopologyConfig {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    fn test_client_with_hot_addresses(hot_addresses: Vec<String>) -> ClickHouseClient {
        ClickHouseClient::new(
            "http://localhost:8123",
            "default",
            "default",
            "",
            ClickHouseClientOptions::new(
                RoutingPolicy {
                    transport: RoutingTransport::Http,
                    scope: RoutingScope::Distributed,
                },
                None,
                hot_addresses,
                "default.gsfa_hot".to_string(),
                "default.gsfa_hot_local".to_string(),
            ),
        )
    }

    fn test_client_with_query_cache() -> ClickHouseClient {
        ClickHouseClient::new(
            "http://localhost:8123",
            "default",
            "default",
            "",
            ClickHouseClientOptions::new(
                RoutingPolicy {
                    transport: RoutingTransport::Http,
                    scope: RoutingScope::Distributed,
                },
                None,
                Vec::new(),
                "default.gsfa_hot".to_string(),
                "default.gsfa_hot_local".to_string(),
            )
            .with_query_cache_config(
                QueryCacheConfig::new(true, 10, false, true).with_get_transaction_overrides(300, 2),
            ),
        )
    }

    #[test]
    fn gsfa_hot_routing_is_disabled_for_blank_addresses() {
        let client = test_client_with_hot_addresses(vec!["   ".to_string()]);

        assert!(!client.gsfa_hot_routing_configured());
    }

    #[test]
    fn gsfa_hot_routing_is_disabled_for_invalid_addresses() {
        let client = test_client_with_hot_addresses(vec!["not-a-pubkey".to_string()]);

        assert!(!client.gsfa_hot_routing_configured());
    }

    #[test]
    fn gsfa_hot_routing_is_enabled_for_valid_addresses() {
        let client = test_client_with_hot_addresses(vec![
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".to_string(),
        ]);

        assert!(client.gsfa_hot_routing_configured());
    }

    #[test]
    fn get_transaction_settings_clause_uses_override_ttl_and_min_runs() {
        let client = test_client_with_query_cache();

        let clause = client.select_get_transaction_settings_clause(
            "get_transaction_by_signature_distributed",
            QueryFreshnessClass::Historical,
        );

        assert!(clause.contains("query_cache_ttl=300"));
        assert!(clause.contains("query_cache_min_query_runs=2"));
        assert!(!clause.contains("use_query_condition_cache=1"));
        assert!(clause.contains("max_execution_time=8"));
    }

    #[test]
    fn generic_settings_clause_keeps_global_cache_defaults() {
        let client = test_client_with_query_cache();

        let clause = client.select_settings_clause(
            "get_block_time_by_slot_distributed",
            QueryFreshnessClass::Historical,
        );

        assert!(clause.contains("query_cache_ttl=10"));
        assert!(!clause.contains("query_cache_min_query_runs"));
        assert!(clause.contains("max_execution_time=8"));
    }

    #[test]
    fn settings_clause_can_use_explicit_timeout() {
        let client = test_client_with_query_cache();

        let clause = client.select_settings_clause_with_timeout(
            "get_block_full_transactions_by_slot_range",
            QueryFreshnessClass::Historical,
            Duration::from_millis(30_000),
        );

        assert!(clause.contains("query_cache_ttl=10"));
        assert!(!clause.contains("query_cache_min_query_runs"));
        assert!(clause.contains("max_execution_time=30"));
        assert!(!clause.contains("max_execution_time=8"));
    }

    #[tokio::test]
    async fn build_shard_topology_uses_yaml_config_without_cluster_discovery() {
        let topology_config = TempTopologyConfig::new(
            "\
nodes:
  - shard-id: 1
    hostname: ch-rbx1
    ip-address: 10.43.128.230
    tcp-port: 9000
    shard-weight: 1
  - shard-id: 2
    hostname: ch-rbx2
    ip-address: 10.43.128.231
    tcp-port: 9000
    shard-weight: 1
",
        );
        let routing = ShardRoutingConfig {
            cluster: "cluster-that-does-not-need-to-exist".to_string(),
            topology_config_path: Some(topology_config.path().display().to_string()),
            shard_http_port: Some(8123),
            gsfa_local_table: None,
            signatures_local_table: None,
            token_owner_activity_local_table: None,
            transactions_local_table: None,
            blocks_metadata_local_table: None,
        };
        let client = ClickHouseClient::new(
            "http://localhost:8123",
            "default",
            "default",
            "",
            ClickHouseClientOptions::new(
                RoutingPolicy {
                    transport: RoutingTransport::Tcp,
                    scope: RoutingScope::ShardDirect,
                },
                Some(routing.clone()),
                Vec::new(),
                "default.gsfa_hot".to_string(),
                "default.gsfa_hot_local".to_string(),
            ),
        );

        let topology = client
            .build_shard_topology(&routing)
            .await
            .expect("YAML topology should load without querying system.clusters");

        assert_eq!(topology.shards.len(), 2);
        assert_eq!(topology.total_weight, 2);
        assert_eq!(topology.weights, vec![1, 1]);
        assert_eq!(topology.shards[0].shard_num, 1);
        assert_eq!(topology.shards[0].host, "10.43.128.230");
        assert_eq!(topology.shards[0].tcp_port, 9000);
        assert_eq!(topology.shards[1].shard_num, 2);
        assert_eq!(topology.shards[1].host, "10.43.128.231");
        assert_eq!(topology.shards[1].tcp_port, 9000);
    }

    #[test]
    fn shard_tcp_query_timeout_leaves_room_for_fallback() {
        assert_eq!(
            shard_tcp_query_timeout_for(std::time::Duration::from_secs(8)),
            std::time::Duration::from_secs(6)
        );
        assert_eq!(
            shard_tcp_query_timeout_for(std::time::Duration::from_secs(30)),
            std::time::Duration::from_secs(25)
        );
    }

    #[test]
    fn shard_tcp_query_timeout_handles_small_parent_budget() {
        assert_eq!(
            shard_tcp_query_timeout_for(std::time::Duration::from_millis(200)),
            std::time::Duration::from_millis(100)
        );
        assert_eq!(
            shard_tcp_query_timeout_for(std::time::Duration::from_millis(1)),
            std::time::Duration::from_millis(1)
        );
    }

    #[test]
    fn kill_query_sql_targets_query_and_initial_query_id() {
        assert_eq!(
            kill_query_sql(None, "superbank:get_inflation_rewards_for_epoch:7"),
            "KILL QUERY WHERE (query_id = 'superbank:get_inflation_rewards_for_epoch:7' OR initial_query_id = 'superbank:get_inflation_rewards_for_epoch:7') ASYNC"
        );
    }

    #[test]
    fn kill_query_sql_can_target_cluster() {
        assert_eq!(
            kill_query_sql(Some("bhs"), "superbank:get_inflation_rewards_for_epoch:7"),
            "KILL QUERY ON CLUSTER 'bhs' WHERE (query_id = 'superbank:get_inflation_rewards_for_epoch:7' OR initial_query_id = 'superbank:get_inflation_rewards_for_epoch:7') ASYNC"
        );
    }

    #[test]
    fn split_table_reference_uses_explicit_database() {
        assert_eq!(
            split_table_reference("default", "analytics.gsfa"),
            ("analytics", "gsfa")
        );
    }

    #[test]
    fn split_table_reference_falls_back_to_default_database() {
        assert_eq!(
            split_table_reference("default", "gsfa"),
            ("default", "gsfa")
        );
    }

    #[test]
    fn split_table_reference_strips_backticks() {
        assert_eq!(
            split_table_reference("default", "`analytics`.`gsfa_hot`"),
            ("analytics", "gsfa_hot")
        );
    }

    #[test]
    fn gsfa_shard_layout_accepts_materialized_view_shape() {
        let result = validate_gsfa_shard_layout_query(
            "default",
            "default.gsfa",
            "default.gsfa_local",
            "MaterializedView",
            "CREATE MATERIALIZED VIEW default.gsfa ENGINE = Distributed('{cluster}', 'default', 'gsfa_local', cityHash64(address)) AS SELECT 1",
        );

        assert!(result.is_ok());
    }

    #[test]
    fn gsfa_shard_layout_rejects_legacy_split_layout() {
        let err = validate_gsfa_shard_layout_query(
            "default",
            "default.gsfa",
            "default.gsfa_local",
            "Distributed",
            "CREATE TABLE default.gsfa ENGINE = Distributed('{cluster}', 'default', 'gsfa_local', cityHash64(address))",
        )
        .expect_err("legacy split layout should fail");

        assert!(
            err.to_string()
                .contains("must itself be a materialized view")
        );
    }

    #[test]
    fn gsfa_shard_layout_rejects_wrong_local_table_target() {
        let err = validate_gsfa_shard_layout_query(
            "default",
            "default.gsfa",
            "default.gsfa_local",
            "MaterializedView",
            "CREATE MATERIALIZED VIEW default.gsfa ENGINE = Distributed('{cluster}', 'default', 'other_local', cityHash64(address)) AS SELECT 1",
        )
        .expect_err("wrong local table should fail");

        assert!(err.to_string().contains("default.gsfa_local"));
    }

    #[test]
    fn gsfa_shard_layout_rejects_wrong_sharding_key() {
        let err = validate_gsfa_shard_layout_query(
            "default",
            "default.gsfa",
            "default.gsfa_local",
            "MaterializedView",
            "CREATE MATERIALIZED VIEW default.gsfa ENGINE = Distributed('{cluster}', 'default', 'gsfa_local', cityHash64(signature)) AS SELECT 1",
        )
        .expect_err("wrong sharding key should fail");

        assert!(err.to_string().contains("cityHash64(address)"));
    }

    #[test]
    fn gsfa_shard_layout_accepts_backticked_qualified_tables() {
        let result = validate_gsfa_shard_layout_query(
            "default",
            "`analytics`.`gsfa`",
            "`analytics`.`gsfa_local`",
            "MaterializedView",
            "CREATE MATERIALIZED VIEW `analytics`.`gsfa` ENGINE = Distributed('{cluster}', 'analytics', 'gsfa_local', cityHash64(address)) AS SELECT 1",
        );

        assert!(result.is_ok());
    }
}
