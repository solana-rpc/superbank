// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::sync::{
    OnceLock,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use clickhouse::Client as HttpClient;
use clickhouse_rs::errors::{DriverError as TcpDriverError, Error as TcpError};
use solana_sdk::pubkey::Pubkey;

#[cfg(feature = "grpc-head-cache")]
use crate::clickhouse::StoredTransactionRecord;
use crate::hydration::parse_transaction_error_display;
use crate::processing::ProcessingError;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum QueryFreshnessClass {
    TipSensitive,
    Historical,
}

#[derive(Clone, Debug)]
pub(crate) struct QueryCacheConfig {
    pub(crate) enabled: bool,
    pub(crate) ttl_seconds: u64,
    pub(crate) share_between_users: bool,
    pub(crate) condition_cache_enabled: bool,
    pub(crate) get_transaction_ttl_seconds: u64,
    pub(crate) get_transaction_min_query_runs: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct QueryCacheSettingsOverrides {
    pub(crate) ttl_seconds: Option<u64>,
    pub(crate) min_query_runs: Option<u64>,
}

impl QueryCacheConfig {
    pub(crate) fn new(
        enabled: bool,
        ttl_seconds: u64,
        share_between_users: bool,
        condition_cache_enabled: bool,
    ) -> Self {
        Self {
            enabled,
            ttl_seconds: ttl_seconds.max(1),
            share_between_users,
            condition_cache_enabled,
            get_transaction_ttl_seconds: 300,
            get_transaction_min_query_runs: 2,
        }
    }

    pub(crate) fn with_get_transaction_overrides(
        mut self,
        ttl_seconds: u64,
        min_query_runs: u64,
    ) -> Self {
        self.get_transaction_ttl_seconds = ttl_seconds.max(1);
        self.get_transaction_min_query_runs = min_query_runs.max(1);
        self
    }

    pub(crate) fn get_transaction_overrides(&self) -> QueryCacheSettingsOverrides {
        QueryCacheSettingsOverrides {
            ttl_seconds: Some(self.get_transaction_ttl_seconds),
            min_query_runs: Some(self.get_transaction_min_query_runs),
        }
    }
}

impl Default for QueryCacheConfig {
    fn default() -> Self {
        Self::new(false, 1, false, false)
    }
}

pub(crate) fn pubkey_literal(pubkey: &Pubkey) -> String {
    // Used to avoid base58Decode() work in ClickHouse for hot paths.
    let hex = hex::encode(pubkey.as_ref()).to_uppercase();
    format!("toFixedString(unhex('{hex}'), 32)")
}

pub(crate) fn env_truthy(value: &str) -> bool {
    let value = value.trim();
    value == "1"
        || value.eq_ignore_ascii_case("true")
        || value.eq_ignore_ascii_case("yes")
        || value.eq_ignore_ascii_case("on")
}

fn query_id_prefix() -> Option<&'static str> {
    static PREFIX: OnceLock<Option<String>> = OnceLock::new();
    PREFIX
        .get_or_init(|| {
            let raw = std::env::var("CLICKHOUSE_QUERY_ID_PREFIX").ok();
            let value = raw.as_deref().unwrap_or("superbank").trim();
            if value.is_empty()
                || value.eq_ignore_ascii_case("0")
                || value.eq_ignore_ascii_case("false")
                || value.eq_ignore_ascii_case("off")
                || value.eq_ignore_ascii_case("no")
                || value.eq_ignore_ascii_case("disabled")
            {
                return None;
            }
            if value.eq_ignore_ascii_case("auto") {
                return Some(format!("superbank-rpc-{}", std::process::id()));
            }
            Some(value.to_string())
        })
        .as_deref()
}

fn internal_query_id_prefix() -> &'static str {
    static PREFIX: OnceLock<String> = OnceLock::new();
    PREFIX
        .get_or_init(|| format!("superbank-rpc-{}", std::process::id()))
        .as_str()
}

fn next_query_id_with_prefix(prefix: &str, label: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let suffix = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}:{label}:{suffix}")
}

fn next_query_id(label: &str) -> Option<String> {
    query_id_prefix().map(|prefix| next_query_id_with_prefix(prefix, label))
}

pub(crate) fn next_required_query_id(label: &str) -> String {
    let prefix = query_id_prefix().unwrap_or_else(internal_query_id_prefix);
    next_query_id_with_prefix(prefix, label)
}

/// Bounds the number of concurrent best-effort `KILL QUERY` cleanup connections opened when a
/// ClickHouse read times out, errors transiently, or has its future dropped. These cleanups are
/// detached tasks that are otherwise ungated, so a burst of timeouts/cancellations (exactly when
/// ClickHouse is overloaded) would spawn an unbounded number of extra HTTP connections and amplify
/// the overload. Configured via `CLICKHOUSE_KILL_QUERY_MAX_CONCURRENCY` (default 16).
pub(crate) fn kill_query_semaphore() -> &'static std::sync::Arc<tokio::sync::Semaphore> {
    static SEM: OnceLock<std::sync::Arc<tokio::sync::Semaphore>> = OnceLock::new();
    SEM.get_or_init(|| {
        let limit = std::env::var("CLICKHOUSE_KILL_QUERY_MAX_CONCURRENCY")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .unwrap_or(16)
            .max(1);
        std::sync::Arc::new(tokio::sync::Semaphore::new(limit))
    })
}

pub(crate) fn annotate_query(sql: String, label: &str) -> (String, Option<String>) {
    let Some(query_id) = next_query_id(label) else {
        return (sql, None);
    };

    let mut annotated = String::with_capacity(sql.len() + query_id.len() + 20);
    annotated.push_str("/*superbank_qid=");
    annotated.push_str(&query_id);
    annotated.push_str("*/ ");
    annotated.push_str(&sql);
    (annotated, Some(query_id))
}

pub(crate) fn annotate_tcp_query(sql: String, label: &str) -> (String, String) {
    annotate_required_query(sql, label)
}

pub(crate) fn annotate_required_query(sql: String, label: &str) -> (String, String) {
    let query_id = next_required_query_id(label);
    if query_id_prefix().is_none() {
        return (sql, query_id);
    }

    let mut annotated = String::with_capacity(sql.len() + query_id.len() + 20);
    annotated.push_str("/*superbank_qid=");
    annotated.push_str(&query_id);
    annotated.push_str("*/ ");
    annotated.push_str(&sql);
    (annotated, query_id)
}

pub(crate) fn http_query_with_id(
    client: &HttpClient,
    sql: &str,
    query_id: Option<String>,
) -> clickhouse::query::Query {
    let mut query = client.query(sql);
    if let Some(query_id) = query_id {
        query = query.with_option("query_id", query_id);
    }
    query
}

pub(crate) fn append_max_execution_time_setting(
    settings_clause: &str,
    timeout: Duration,
) -> String {
    let settings_clause = settings_clause.trim();
    if settings_clause.is_empty() || settings_clause.contains("max_execution_time") {
        return settings_clause.to_string();
    }

    let timeout_secs = (timeout.as_millis().saturating_add(999) / 1000).max(1);
    let timeout_secs = timeout_secs.min(u128::from(u64::MAX)) as u64;

    format!("{settings_clause}, max_execution_time={timeout_secs}")
}

pub(crate) fn build_select_settings_clause(
    allow_query_settings: bool,
    freshness: QueryFreshnessClass,
    query_cache: &QueryCacheConfig,
    use_condition_cache: bool,
    operation: &'static str,
) -> String {
    build_select_settings_clause_with_overrides(
        allow_query_settings,
        freshness,
        query_cache,
        QueryCacheSettingsOverrides::default(),
        use_condition_cache,
        operation,
    )
}

pub(crate) fn build_select_settings_clause_with_overrides(
    allow_query_settings: bool,
    freshness: QueryFreshnessClass,
    query_cache: &QueryCacheConfig,
    query_cache_overrides: QueryCacheSettingsOverrides,
    use_condition_cache: bool,
    operation: &'static str,
) -> String {
    if !allow_query_settings {
        crate::metrics::clickhouse_query_cache_classified(operation, false);
        return String::new();
    }

    let query_cache_eligible =
        query_cache.enabled && matches!(freshness, QueryFreshnessClass::Historical);
    crate::metrics::clickhouse_query_cache_classified(operation, query_cache_eligible);

    let condition_cache_eligible = query_cache.condition_cache_enabled
        && use_condition_cache
        && matches!(freshness, QueryFreshnessClass::Historical);

    let mut settings = vec!["optimize_skip_unused_shards=1".to_string()];
    if query_cache_eligible {
        let effective_ttl_seconds = query_cache_overrides
            .ttl_seconds
            .unwrap_or(query_cache.ttl_seconds)
            .max(1);
        crate::metrics::clickhouse_query_cache_settings_applied(
            operation,
            true,
            true,
            effective_ttl_seconds,
        );

        let share_between_users = if query_cache.share_between_users {
            1
        } else {
            0
        };
        settings.extend([
            "use_query_cache=1".to_string(),
            "enable_reads_from_query_cache=1".to_string(),
            "enable_writes_to_query_cache=1".to_string(),
            format!("query_cache_ttl={effective_ttl_seconds}"),
            format!("query_cache_share_between_users={share_between_users}"),
        ]);
        if let Some(min_query_runs) = query_cache_overrides.min_query_runs {
            settings.push(format!(
                "query_cache_min_query_runs={}",
                min_query_runs.max(1)
            ));
        }
    }

    if condition_cache_eligible {
        settings.push("use_query_condition_cache=1".to_string());
    }

    format!("SETTINGS {}", settings.join(", "))
}

pub(crate) fn parse_err_json(signature: &str, err_str: String) -> Option<serde_json::Value> {
    let trimmed = err_str.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Ok(value) = serde_json::from_str::<serde_json::Value>(trimmed) {
        if let Some(as_string) = value.as_str()
            && let Some(parsed) = parse_transaction_error_display(as_string)
            && let Ok(converted) = serde_json::to_value(parsed)
        {
            return Some(converted);
        }
        return Some(value);
    }

    if let Some(parsed) = parse_transaction_error_display(trimmed)
        && let Ok(converted) = serde_json::to_value(parsed)
    {
        return Some(converted);
    }

    tracing::debug!(
        signature = %signature,
        "Failed to parse err JSON stored in gSFA table; returning raw string"
    );
    Some(serde_json::Value::String(trimmed.to_string()))
}

/// Extract gSFA-style memos from a stored transaction: every memo-program
/// instruction's UTF-8 data, formatted `"[len] text"` and joined by `"; "`.
///
/// This matches what [`format_gsfa_memo`] produces from the raw memos stored by
/// the gsfa/token_owner_activity materialized views, so values built here and
/// values read from ClickHouse render identically.
#[cfg(feature = "grpc-head-cache")]
pub(crate) fn extract_memo(record: &StoredTransactionRecord) -> Option<String> {
    static MEMO_PROGRAM_IDS: once_cell::sync::Lazy<[Pubkey; 2]> =
        once_cell::sync::Lazy::new(|| {
            use std::str::FromStr;
            [
                Pubkey::from_str("Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo")
                    .expect("memo program id"),
                Pubkey::from_str("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr")
                    .expect("memo program id"),
            ]
        });

    let mut memos = Vec::new();
    for (program_id_index, data) in record
        .tx_instructions_program_id_index
        .iter()
        .zip(record.tx_instructions_data.iter())
    {
        let idx = usize::from(*program_id_index);
        let Some(program_id) = account_key_at(record, idx) else {
            continue;
        };
        if !MEMO_PROGRAM_IDS.contains(&program_id) {
            continue;
        }
        let Ok(text) = std::str::from_utf8(data) else {
            continue;
        };
        if text.is_empty() {
            continue;
        }
        memos.push(format!("[{}] {}", text.len(), text));
    }

    if memos.is_empty() {
        None
    } else {
        Some(memos.join("; "))
    }
}

/// Resolve an instruction account index against the transaction's combined key
/// list: static keys, then loaded writable, then loaded readonly addresses.
#[cfg(feature = "grpc-head-cache")]
pub(crate) fn account_key_at(record: &StoredTransactionRecord, idx: usize) -> Option<Pubkey> {
    let static_len = record.tx_account_keys.len();
    if idx < static_len {
        return Some(Pubkey::from(record.tx_account_keys[idx]));
    }
    let idx = idx - static_len;
    let w_len = record.meta_loaded_addresses_writable.len();
    if idx < w_len {
        return Some(Pubkey::from(record.meta_loaded_addresses_writable[idx]));
    }
    let idx = idx - w_len;
    if idx < record.meta_loaded_addresses_readonly.len() {
        return Some(Pubkey::from(record.meta_loaded_addresses_readonly[idx]));
    }
    None
}

pub(crate) fn format_gsfa_memo(memo: Option<String>) -> Option<String> {
    memo.map(|raw| {
        if raw.is_empty() {
            return raw;
        }

        let parts = raw.split("; ").collect::<Vec<_>>();
        parts
            .into_iter()
            .map(|part| {
                if memo_has_length_prefix(part) {
                    part.to_string()
                } else {
                    format!("[{}] {}", part.len(), part)
                }
            })
            .collect::<Vec<_>>()
            .join("; ")
    })
}

fn memo_has_length_prefix(memo: &str) -> bool {
    let Some(rest) = memo.strip_prefix('[') else {
        return false;
    };
    let Some((len_part, remaining)) = rest.split_once("] ") else {
        return false;
    };
    !len_part.is_empty() && len_part.chars().all(|c| c.is_ascii_digit()) && !remaining.is_empty()
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub(crate) enum GsfaFallbackMode {
    Disabled,
    EmptyOnly,
    Incomplete,
}

pub(crate) fn gsfa_fallback_mode() -> GsfaFallbackMode {
    static MODE: OnceLock<GsfaFallbackMode> = OnceLock::new();
    *MODE.get_or_init(|| {
        let value = match std::env::var("CLICKHOUSE_GSFA_FALLBACK_TRANSACTIONS") {
            Ok(value) => value.to_lowercase(),
            Err(_) => return GsfaFallbackMode::Disabled,
        };

        match value.as_str() {
            "1" | "true" | "yes" | "on" | "empty" => GsfaFallbackMode::EmptyOnly,
            "force" | "always" | "full" | "incomplete" => GsfaFallbackMode::Incomplete,
            _ => GsfaFallbackMode::Disabled,
        }
    })
}

pub(crate) fn transient_shard_local_error_reason(err: &ProcessingError) -> Option<&'static str> {
    match err {
        ProcessingError::Timeout { .. } => Some("timeout"),
        ProcessingError::Database { context, source } => {
            if context.contains("tcp handle error") {
                return Some("tcp_handle");
            }

            let source = source.as_deref()?;
            if let Some(source) = source.downcast_ref::<TcpError>() {
                return match source {
                    TcpError::Driver(TcpDriverError::Timeout) => Some("timeout"),
                    TcpError::Io(_) | TcpError::Connection(_) => Some("network"),
                    TcpError::Server(server) => match server.code {
                        209 => Some("socket_timeout"),
                        210 => Some("network"),
                        236 => Some("aborted"),
                        279 => Some("all_connection_tries_failed"),
                        735 => Some("client_cancelled"),
                        _ => None,
                    },
                    _ => None,
                };
            }

            let message = source.to_string();
            if message.contains("(SOCKET_TIMEOUT)") || message.contains("ERROR 209") {
                Some("socket_timeout")
            } else if message.contains("(NETWORK_ERROR)")
                || message.contains("Broken pipe")
                || message.contains("ERROR 210")
            {
                Some("network")
            } else if message.contains("(ABORTED)") || message.contains("ERROR 236") {
                Some("aborted")
            } else if message.contains("(ALL_CONNECTION_TRIES_FAILED)")
                || message.contains("ERROR 279")
            {
                Some("all_connection_tries_failed")
            } else if message.contains("(QUERY_WAS_CANCELLED_BY_CLIENT)")
                || message.contains("ERROR 735")
            {
                Some("client_cancelled")
            } else {
                None
            }
        }
        ProcessingError::Deserialization { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        QueryCacheConfig, QueryCacheSettingsOverrides, QueryFreshnessClass,
        append_max_execution_time_setting, build_select_settings_clause,
        build_select_settings_clause_with_overrides, transient_shard_local_error_reason,
    };
    use crate::processing::ProcessingError;
    use clickhouse_rs::errors::{DriverError as TcpDriverError, Error as TcpError, ServerError};
    use std::time::Duration;

    #[test]
    fn settings_clause_empty_when_query_settings_disabled() {
        let cfg = QueryCacheConfig::new(true, 5, false, true);
        let clause = build_select_settings_clause(
            false,
            QueryFreshnessClass::Historical,
            &cfg,
            true,
            "test_operation",
        );
        assert_eq!(clause, "");
    }

    #[test]
    fn settings_clause_has_optimize_only_when_cache_disabled() {
        let cfg = QueryCacheConfig::new(false, 5, false, false);
        let clause = build_select_settings_clause(
            true,
            QueryFreshnessClass::Historical,
            &cfg,
            false,
            "test_operation",
        );
        assert_eq!(clause, "SETTINGS optimize_skip_unused_shards=1");
    }

    #[test]
    fn settings_clause_has_query_cache_for_historical_reads() {
        let cfg = QueryCacheConfig::new(true, 5, true, false);
        let clause = build_select_settings_clause(
            true,
            QueryFreshnessClass::Historical,
            &cfg,
            false,
            "test_operation",
        );
        assert!(clause.contains("SETTINGS optimize_skip_unused_shards=1"));
        assert!(clause.contains("use_query_cache=1"));
        assert!(clause.contains("enable_reads_from_query_cache=1"));
        assert!(clause.contains("enable_writes_to_query_cache=1"));
        assert!(clause.contains("query_cache_ttl=5"));
        assert!(clause.contains("query_cache_share_between_users=1"));
    }

    #[test]
    fn settings_clause_omits_query_cache_for_tip_sensitive_reads() {
        let cfg = QueryCacheConfig::new(true, 5, true, true);
        let clause = build_select_settings_clause(
            true,
            QueryFreshnessClass::TipSensitive,
            &cfg,
            true,
            "test_operation",
        );
        assert_eq!(clause, "SETTINGS optimize_skip_unused_shards=1");
    }

    #[test]
    fn query_cache_config_normalizes_ttl_to_minimum_one() {
        let cfg = QueryCacheConfig::new(true, 0, false, false);
        assert_eq!(cfg.ttl_seconds, 1);
    }

    #[test]
    fn settings_clause_has_condition_cache_for_eligible_historical_reads() {
        let cfg = QueryCacheConfig::new(false, 5, false, true);
        let clause = build_select_settings_clause(
            true,
            QueryFreshnessClass::Historical,
            &cfg,
            true,
            "test_operation",
        );
        assert_eq!(
            clause,
            "SETTINGS optimize_skip_unused_shards=1, use_query_condition_cache=1"
        );
    }

    #[test]
    fn settings_clause_omits_condition_cache_for_ineligible_queries() {
        let cfg = QueryCacheConfig::new(false, 5, false, true);
        let clause = build_select_settings_clause(
            true,
            QueryFreshnessClass::Historical,
            &cfg,
            false,
            "test_operation",
        );
        assert_eq!(clause, "SETTINGS optimize_skip_unused_shards=1");
    }

    #[test]
    fn settings_clause_can_override_query_cache_ttl_and_min_runs() {
        let cfg = QueryCacheConfig::new(true, 5, false, false);
        let clause = build_select_settings_clause_with_overrides(
            true,
            QueryFreshnessClass::Historical,
            &cfg,
            QueryCacheSettingsOverrides {
                ttl_seconds: Some(300),
                min_query_runs: Some(2),
            },
            false,
            "test_operation",
        );
        assert!(clause.contains("query_cache_ttl=300"));
        assert!(clause.contains("query_cache_min_query_runs=2"));
        assert!(clause.contains("query_cache_share_between_users=0"));
    }

    #[test]
    fn settings_clause_omits_query_cache_overrides_when_cache_disabled() {
        let cfg = QueryCacheConfig::new(false, 5, false, false);
        let clause = build_select_settings_clause_with_overrides(
            true,
            QueryFreshnessClass::Historical,
            &cfg,
            QueryCacheSettingsOverrides {
                ttl_seconds: Some(300),
                min_query_runs: Some(2),
            },
            false,
            "test_operation",
        );
        assert_eq!(clause, "SETTINGS optimize_skip_unused_shards=1");
    }

    #[test]
    fn query_cache_config_defaults_get_transaction_overrides() {
        let cfg = QueryCacheConfig::new(true, 5, false, false);
        let overrides = cfg.get_transaction_overrides();

        assert_eq!(overrides.ttl_seconds, Some(300));
        assert_eq!(overrides.min_query_runs, Some(2));
    }

    #[test]
    fn max_execution_time_setting_respects_disabled_query_settings() {
        let clause = append_max_execution_time_setting("", Duration::from_millis(8_000));
        assert_eq!(clause, "");
    }

    #[test]
    fn max_execution_time_setting_is_appended_to_existing_clause() {
        let clause = append_max_execution_time_setting(
            "SETTINGS optimize_skip_unused_shards=1",
            Duration::from_millis(8_000),
        );
        assert_eq!(
            clause,
            "SETTINGS optimize_skip_unused_shards=1, max_execution_time=8"
        );
    }

    #[test]
    fn max_execution_time_setting_is_not_duplicated() {
        let clause = append_max_execution_time_setting(
            "SETTINGS optimize_skip_unused_shards=1, max_execution_time=8",
            Duration::from_millis(8_000),
        );
        assert_eq!(
            clause,
            "SETTINGS optimize_skip_unused_shards=1, max_execution_time=8"
        );
    }

    #[test]
    fn transient_reason_detects_tcp_driver_timeout() {
        let err = ProcessingError::database("timed out", TcpError::Driver(TcpDriverError::Timeout));
        assert_eq!(transient_shard_local_error_reason(&err), Some("timeout"));
    }

    #[test]
    fn transient_reason_detects_clickhouse_network_server_error() {
        let err = ProcessingError::database(
            "network",
            TcpError::Server(ServerError {
                code: 210,
                name: "NETWORK_ERROR".to_string(),
                message: "broken pipe".to_string(),
                stack_trace: String::new(),
            }),
        );
        assert_eq!(transient_shard_local_error_reason(&err), Some("network"));
    }

    #[test]
    fn transient_reason_detects_tcp_handle_error_context() {
        let err = ProcessingError::database_msg("Shard x tcp handle error");
        assert_eq!(transient_shard_local_error_reason(&err), Some("tcp_handle"));
    }
}
