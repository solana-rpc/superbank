// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Owner-shard routing for primary signature lookups
//! (`CLICKHOUSE_SIGNATURES_OWNER_SHARD_ROUTING`).
//!
//! `default.signatures` is a materialized view whose storage is a `Distributed` table sharded by
//! `cityHash64(signature)`. Reading through the view does not prune shards, so every lookup
//! queries all shards. The view's inner table does prune, but its `.inner_id.<uuid>` name differs
//! between replicas (unless the view was created with one UUID cluster-wide), so it cannot be named
//! from a load-balanced endpoint. The `cluster()` table function builds the same `Distributed`
//! read over the same local table and sharding key on whichever host receives the query, and
//! `optimize_skip_unused_shards=1` then sends it only to the owner shard.
//!
//! The rewrite is only equivalent to the view while the cluster layouts match and every row sits
//! on its `cityHash64(signature)` owner shard. Startup checks the layout on the answering host
//! ([`check_owner_shard_layout`], [`same_cluster_layout`]); row placement is an operator check
//! (README, "Signature owner-shard routing").

use crate::processing::{ProcessingError, ProcessingResult};

use super::sharding::ClusterRow;

/// Settings appended to owner-routed reads. `optimize_skip_unused_shards=1` enables pruning;
/// `force_optimize_skip_unused_shards=0` keeps a filter that cannot be pruned working (it fans
/// out like the view) instead of failing.
const OWNER_SHARD_SETTINGS: [(&str, &str); 2] = [
    ("optimize_skip_unused_shards", "1"),
    ("force_optimize_skip_unused_shards", "0"),
];

/// Owner-shard routing configuration: the cluster and local table the `cluster()` source reads,
/// and the source text itself.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OwnerShardSource {
    pub(crate) cluster: String,
    pub(crate) local_table: String,
    pub(crate) source: String,
}

impl OwnerShardSource {
    pub(crate) fn new(cluster: &str, local_table: &str) -> ProcessingResult<Self> {
        Ok(Self {
            cluster: cluster.trim().to_string(),
            local_table: local_table.to_string(),
            source: signatures_owner_shard_source(cluster, local_table)?,
        })
    }
}

/// The literal every signature lookup uses for a 64-byte signature. Shard pruning needs
/// ClickHouse to constant-fold it (this shape folds; e.g. `unhex(lpad(hex(...)))` does not), so
/// the startup probe builds its literal with this helper too.
pub(crate) fn signature_literal(signature: &[u8]) -> String {
    format!(
        "toFixedString(unhex('{}'), 64)",
        hex::encode(signature).to_uppercase()
    )
}

/// Builds `cluster('<cluster>', <local_table>, cityHash64(signature))`.
///
/// `cluster` may be a macro such as `{cluster}`; ClickHouse expands macros in the `cluster()`
/// table function. `local_table` must be a plain `[db.]table` identifier.
pub(crate) fn signatures_owner_shard_source(
    cluster: &str,
    local_table: &str,
) -> ProcessingResult<String> {
    let cluster = cluster.trim();
    if cluster.is_empty() {
        return Err(ProcessingError::database_msg(
            "CLICKHOUSE_SIGNATURES_OWNER_SHARD_ROUTING requires CLICKHOUSE_CLUSTER (a cluster name or macro such as {cluster})",
        ));
    }
    if cluster
        .chars()
        .any(|c| c.is_control() || c == '\'' || c == '\\')
    {
        return Err(ProcessingError::database_msg(format!(
            "CLICKHOUSE_CLUSTER {cluster:?} contains characters not allowed in a cluster name"
        )));
    }
    if !is_plain_table_identifier(local_table) {
        return Err(ProcessingError::database_msg(format!(
            "Signatures local table {local_table:?} must be a plain [database.]table identifier for owner-shard routing"
        )));
    }
    Ok(format!(
        "cluster('{cluster}', {local_table}, cityHash64(signature))"
    ))
}

fn is_plain_table_identifier(name: &str) -> bool {
    let mut parts = name.split('.');
    let valid_part = |part: &str| {
        !part.is_empty()
            && part.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            && !part.starts_with(|c: char| c.is_ascii_digit())
    };
    match (parts.next(), parts.next(), parts.next()) {
        (Some(table), None, None) => valid_part(table),
        (Some(db), Some(table), None) => valid_part(db) && valid_part(table),
        _ => false,
    }
}

/// Adds the owner-shard settings to a `SETTINGS ...` clause (or starts one), replacing nothing
/// the clause already sets.
pub(crate) fn with_owner_shard_settings(clause: &str) -> String {
    let trimmed = clause.trim_end();
    let mut out = if trimmed.trim().is_empty() {
        String::from("SETTINGS ")
    } else {
        format!("{trimmed}, ")
    };
    let missing = OWNER_SHARD_SETTINGS
        .iter()
        .filter(|(name, _)| !clause_sets(trimmed, name))
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return trimmed.to_string();
    }
    out.push_str(&missing.join(", "));
    out
}

fn clause_sets(clause: &str, name: &str) -> bool {
    clause
        .split([',', ' ', '\n', '\t'])
        .any(|token| token.split_once('=').is_some_and(|(key, _)| key == name))
}

/// Status filter for owner-routed reads. The primary batch filter
/// `(sig_bucket, signature) IN (...)` does not prune shards (verified on ClickHouse 26.8.11.7);
/// adding the redundant `signature IN (...)` conjunct lets ClickHouse compute the owner shards.
pub(crate) fn owner_shard_status_filter(primary_filter: &str, literals: &[&str]) -> String {
    if literals.len() <= 1 {
        return primary_filter.to_string();
    }
    format!(
        "({primary_filter}) AND signature IN ({})",
        literals.join(",")
    )
}

/// `Distributed(cluster, database, table, sharding_key[, ...])` arguments, unquoted.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct DistributedStorage {
    pub(crate) cluster: String,
    pub(crate) database: String,
    pub(crate) table: String,
    pub(crate) sharding_key: String,
}

/// The `Distributed` storage behind the signatures table, from its `system.tables` row: a
/// materialized view's `ENGINE = Distributed(...)` (its inner table) or a `Distributed` table's
/// own engine. Anything else (a view `TO` another table, a plain local table) is rejected,
/// because routing could not be shown to read the same rows.
pub(crate) fn signatures_storage(
    engine: &str,
    create_table_query: &str,
    engine_full: &str,
) -> Result<DistributedStorage, String> {
    let text = if engine.eq_ignore_ascii_case("MaterializedView") {
        create_table_query
            .find("ENGINE = Distributed(")
            .map(|at| &create_table_query[at + "ENGINE = ".len()..])
    } else if engine.eq_ignore_ascii_case("Distributed") {
        Some(engine_full)
    } else {
        None
    };
    text.and_then(parse_distributed_engine).ok_or_else(|| {
        format!(
            "its engine is {engine}, not a materialized view with ENGINE = Distributed(...) or a Distributed table"
        )
    })
}

fn parse_distributed_engine(text: &str) -> Option<DistributedStorage> {
    let args = split_top_level_args(text.trim_start().strip_prefix("Distributed(")?)?;
    if args.len() < 4 {
        return None;
    }
    Some(DistributedStorage {
        cluster: unquote_argument(&args[0])?,
        database: unquote_argument(&args[1])?,
        table: unquote_argument(&args[2])?,
        sharding_key: args[3].trim().to_string(),
    })
}

/// Splits `a, 'b,c', f(x, y)) tail` into `["a", "'b,c'", "f(x, y)"]`, stopping at the closing
/// parenthesis. `None` when it is missing.
fn split_top_level_args(text: &str) -> Option<Vec<String>> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut depth = 0usize;
    let mut quote: Option<char> = None;
    let mut escaped = false;
    for c in text.chars() {
        if let Some(q) = quote {
            current.push(c);
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == q {
                quote = None;
            }
            continue;
        }
        match c {
            '\'' | '`' | '"' => {
                quote = Some(c);
                current.push(c);
            }
            '(' => {
                depth += 1;
                current.push(c);
            }
            ')' if depth == 0 => {
                args.push(current.trim().to_string());
                return Some(args);
            }
            ')' => {
                depth -= 1;
                current.push(c);
            }
            ',' if depth == 0 => args.push(std::mem::take(&mut current).trim().to_string()),
            _ => current.push(c),
        }
    }
    None
}

fn unquote_argument(arg: &str) -> Option<String> {
    let arg = arg.trim();
    for q in ['\'', '`', '"'] {
        if let Some(inner) = arg.strip_prefix(q).and_then(|rest| rest.strip_suffix(q)) {
            return Some(inner.replace(&format!("\\{q}"), &q.to_string()));
        }
    }
    (!arg.is_empty() && is_plain_table_identifier(arg)).then(|| arg.to_string())
}

/// Replaces every `{name}` with its `system.macros` substitution.
pub(crate) fn expand_macros(value: &str, macros: &[(String, String)]) -> Result<String, String> {
    let mut out = String::new();
    let mut rest = value;
    while let Some(open) = rest.find('{') {
        let close = rest[open..]
            .find('}')
            .map(|at| open + at)
            .ok_or_else(|| format!("unterminated macro in {value:?}"))?;
        let name = &rest[open + 1..close];
        let substitution = macros
            .iter()
            .find(|(macro_name, _)| macro_name == name)
            .map(|(_, substitution)| substitution)
            .ok_or_else(|| format!("macro {{{name}}} in {value:?} is not defined on this host"))?;
        out.push_str(&rest[..open]);
        out.push_str(substitution);
        rest = &rest[close + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

/// What remains to check after [`check_owner_shard_layout`].
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum LayoutVerdict {
    /// The routed source and the view use the same cluster.
    SameCluster,
    /// Different cluster names: their `system.clusters` rows must match
    /// ([`same_cluster_layout`]).
    CompareClusters { configured: String, storage: String },
}

/// Checks that the routed source reads the same local table with the same sharding key as the
/// view's `Distributed` storage, and says whether the clusters still need comparing.
/// `local_database`/`local_table` are the configured local table, already qualified.
pub(crate) fn check_owner_shard_layout(
    storage: &DistributedStorage,
    configured_cluster: &str,
    local_database: &str,
    local_table: &str,
    macros: &[(String, String)],
) -> Result<LayoutVerdict, String> {
    let storage_database = expand_macros(&storage.database, macros)?;
    let storage_table = expand_macros(&storage.table, macros)?;
    if storage_database != local_database || storage_table != local_table {
        return Err(format!(
            "the view's Distributed storage reads {storage_database}.{storage_table}, but routing would read {local_database}.{local_table}"
        ));
    }
    let key = storage
        .sharding_key
        .chars()
        .filter(|c| !c.is_ascii_whitespace() && *c != '`')
        .collect::<String>()
        .to_ascii_lowercase();
    if key != "cityhash64(signature)" {
        return Err(format!(
            "the view's Distributed storage is sharded by {}, not cityHash64(signature)",
            storage.sharding_key
        ));
    }
    let configured_cluster = configured_cluster.trim();
    if configured_cluster == storage.cluster {
        return Ok(LayoutVerdict::SameCluster);
    }
    let configured = expand_macros(configured_cluster, macros)?;
    let storage_cluster = expand_macros(&storage.cluster, macros)?;
    if configured == storage_cluster {
        return Ok(LayoutVerdict::SameCluster);
    }
    Ok(LayoutVerdict::CompareClusters {
        configured,
        storage: storage_cluster,
    })
}

/// Two clusters route `cityHash64(signature)` to the same hosts only if they list the same
/// shards, in the same order, with the same weights and replicas.
pub(crate) fn same_cluster_layout(a: &[ClusterRow], b: &[ClusterRow]) -> bool {
    let key = |rows: &[ClusterRow]| {
        let mut rows = rows
            .iter()
            .map(|row| {
                (
                    row.shard_num,
                    row.shard_weight,
                    row.replica_num,
                    row.host_name.clone(),
                    row.port,
                )
            })
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    !a.is_empty() && key(a) == key(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_uses_cluster_function_with_signature_sharding_key() {
        assert_eq!(
            signatures_owner_shard_source("{cluster}", "default.signatures_local").unwrap(),
            "cluster('{cluster}', default.signatures_local, cityHash64(signature))"
        );
        assert_eq!(
            signatures_owner_shard_source(" cluster_a ", "signatures_local").unwrap(),
            "cluster('cluster_a', signatures_local, cityHash64(signature))"
        );
    }

    #[test]
    fn source_rejects_empty_cluster_and_injection() {
        assert!(signatures_owner_shard_source("", "default.signatures_local").is_err());
        assert!(signatures_owner_shard_source("  ", "default.signatures_local").is_err());
        assert!(signatures_owner_shard_source("a'b", "default.signatures_local").is_err());
        assert!(signatures_owner_shard_source("a\\b", "default.signatures_local").is_err());
        assert!(signatures_owner_shard_source("a\nb", "default.signatures_local").is_err());
        for table in [
            "",
            "default.",
            ".signatures_local",
            "a.b.c",
            "default.signatures_local; DROP TABLE x",
            "default.`.inner_id.x`",
            "default.1abc",
        ] {
            assert!(
                signatures_owner_shard_source("cluster_a", table).is_err(),
                "{table:?} should be rejected"
            );
        }
    }

    #[test]
    fn settings_are_appended_once() {
        assert_eq!(
            with_owner_shard_settings(""),
            "SETTINGS optimize_skip_unused_shards=1, force_optimize_skip_unused_shards=0"
        );
        assert_eq!(
            with_owner_shard_settings(
                "SETTINGS optimize_skip_unused_shards=1, max_execution_time=30"
            ),
            "SETTINGS optimize_skip_unused_shards=1, max_execution_time=30, force_optimize_skip_unused_shards=0"
        );
        let full = "SETTINGS optimize_skip_unused_shards=1, force_optimize_skip_unused_shards=0";
        assert_eq!(with_owner_shard_settings(full), full);
        // `force_optimize_skip_unused_shards` must not be mistaken for the plain setting.
        assert_eq!(
            with_owner_shard_settings("SETTINGS force_optimize_skip_unused_shards=0"),
            "SETTINGS force_optimize_skip_unused_shards=0, optimize_skip_unused_shards=1"
        );
    }

    const MV_DDL: &str = "CREATE MATERIALIZED VIEW default.signatures (`sig_bucket` UInt8 MATERIALIZED cityHash64(signature) % 32, `signature` FixedString(64)) ENGINE = Distributed('{cluster}', 'default', 'signatures_local', cityHash64(signature)) AS SELECT signature FROM default.transactions_local";

    fn macros() -> Vec<(String, String)> {
        vec![
            ("cluster".into(), "cluster_a".into()),
            ("shard".into(), "1".into()),
        ]
    }

    fn row(shard_num: u32, host: &str) -> ClusterRow {
        ClusterRow {
            shard_num,
            shard_weight: 1,
            replica_num: 1,
            host_name: host.into(),
            host_address: "192.0.2.1".into(),
            port: 9000,
            is_local: 0,
        }
    }

    #[test]
    fn signature_literal_is_the_foldable_fixed_string_shape() {
        assert_eq!(
            signature_literal(&[0xab, 0x01]),
            "toFixedString(unhex('AB01'), 64)"
        );
    }

    #[test]
    fn storage_is_parsed_from_view_ddl_or_distributed_engine() {
        let expected = DistributedStorage {
            cluster: "{cluster}".into(),
            database: "default".into(),
            table: "signatures_local".into(),
            sharding_key: "cityHash64(signature)".into(),
        };
        assert_eq!(
            signatures_storage("MaterializedView", MV_DDL, "").unwrap(),
            expected
        );
        assert_eq!(
            signatures_storage(
                "Distributed",
                "",
                "Distributed('{cluster}', 'default', 'signatures_local', cityHash64(signature), 'policy') SETTINGS x = 1"
            )
            .unwrap(),
            expected
        );
        assert_eq!(
            signatures_storage(
                "Distributed",
                "",
                "Distributed(cluster_a, default, signatures_local, cityHash64(`signature`))"
            )
            .unwrap()
            .cluster,
            "cluster_a"
        );
        // A view `TO` another table, a local table, or a malformed engine is rejected.
        for (engine, ddl, full) in [
            (
                "MaterializedView",
                "CREATE MATERIALIZED VIEW default.signatures TO default.signatures_dist AS SELECT 1",
                "",
            ),
            ("ReplacingMergeTree", "", "ReplacingMergeTree(slot)"),
            (
                "Distributed",
                "",
                "Distributed('cluster_a', 'default', 'signatures_local'",
            ),
            ("Distributed", "", "Distributed('cluster_a', 'default')"),
        ] {
            assert!(
                signatures_storage(engine, ddl, full).is_err(),
                "{full}{ddl}"
            );
        }
    }

    #[test]
    fn macros_expand_or_fail() {
        assert_eq!(expand_macros("{cluster}", &macros()).unwrap(), "cluster_a");
        assert_eq!(
            expand_macros("all_{cluster}_{shard}", &macros()).unwrap(),
            "all_cluster_a_1"
        );
        assert_eq!(expand_macros("cluster_a", &[]).unwrap(), "cluster_a");
        assert!(expand_macros("{missing}", &macros()).is_err());
        assert!(expand_macros("{cluster", &macros()).is_err());
    }

    #[test]
    fn layout_check_accepts_the_views_cluster_and_compares_other_names() {
        let storage = signatures_storage("MaterializedView", MV_DDL, "").unwrap();
        let check = |cluster: &str| {
            check_owner_shard_layout(&storage, cluster, "default", "signatures_local", &macros())
        };
        assert_eq!(check("{cluster}").unwrap(), LayoutVerdict::SameCluster);
        assert_eq!(check(" cluster_a ").unwrap(), LayoutVerdict::SameCluster);
        // Another cluster name (e.g. an all-replicas cluster reused from query cleanup) is only
        // accepted if its system.clusters rows match.
        assert_eq!(
            check("cluster_a_all").unwrap(),
            LayoutVerdict::CompareClusters {
                configured: "cluster_a_all".into(),
                storage: "cluster_a".into(),
            }
        );
        assert!(
            check_owner_shard_layout(&storage, "cluster_a", "default", "sigs_local", &macros())
                .is_err()
        );
        assert!(
            check_owner_shard_layout(
                &storage,
                "cluster_a",
                "other",
                "signatures_local",
                &macros()
            )
            .is_err()
        );
        let by_slot = DistributedStorage {
            sharding_key: "cityHash64(slot)".into(),
            ..signatures_storage("MaterializedView", MV_DDL, "").unwrap()
        };
        assert!(
            check_owner_shard_layout(
                &by_slot,
                "cluster_a",
                "default",
                "signatures_local",
                &macros()
            )
            .is_err()
        );
        assert!(
            check_owner_shard_layout(&storage, "cluster_a", "default", "signatures_local", &[])
                .is_err(),
            "the view's {{cluster}} cannot be expanded"
        );
    }

    #[test]
    fn cluster_layouts_must_match_in_order_weight_and_hosts() {
        let layout = vec![row(1, "a"), row(2, "b"), row(3, "c")];
        let shuffled_rows = vec![row(3, "c"), row(1, "a"), row(2, "b")];
        assert!(same_cluster_layout(&layout, &shuffled_rows));
        let reversed = vec![row(1, "c"), row(2, "b"), row(3, "a")];
        assert!(!same_cluster_layout(&layout, &reversed));
        let mut weighted = layout.clone();
        weighted[0].shard_weight = 2;
        assert!(!same_cluster_layout(&layout, &weighted));
        let mut replicas = layout.clone();
        replicas.push(ClusterRow {
            replica_num: 2,
            ..row(1, "a2")
        });
        assert!(!same_cluster_layout(&layout, &replicas));
        assert!(!same_cluster_layout(&[], &[]));
        assert!(!same_cluster_layout(&layout, &layout[..2]));
    }

    #[test]
    fn status_filter_adds_signature_in_only_for_batches() {
        let single = "sig_bucket = 3 AND signature = X";
        assert_eq!(owner_shard_status_filter(single, &["X"]), single);
        assert_eq!(
            owner_shard_status_filter("(sig_bucket, signature) IN ((1, A),(2, B))", &["A", "B"]),
            "((sig_bucket, signature) IN ((1, A),(2, B))) AND signature IN (A,B)"
        );
    }
}
