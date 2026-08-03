// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Default, Deserialize)]
#[serde(default, rename_all = "kebab-case")]
struct RpcFileConfig {
    rpc_parameter_filters: Vec<Vec<Value>>,
}

/// Startup-compiled RPC request filters.
///
/// The first lookup excludes unfiltered methods. The arity lookup then excludes
/// differently-shaped calls before any structural JSON comparisons are needed.
/// Single-address filters for address-history methods also match requests with
/// trailing configuration parameters.
#[derive(Debug, Default)]
pub(crate) struct RpcParameterFilterSet {
    by_method_and_arity: HashMap<String, HashMap<usize, Vec<Vec<Value>>>>,
    len: usize,
}

impl RpcParameterFilterSet {
    pub(crate) fn load(path: Option<&Path>) -> Result<Self, String> {
        let Some(path) = path else {
            return Ok(Self::default());
        };

        let contents = fs::read_to_string(path)
            .map_err(|err| format!("failed to read config file '{}': {err}", path.display()))?;
        let config: RpcFileConfig = serde_yaml::from_str(&contents)
            .map_err(|err| format!("failed to parse config file '{}': {err}", path.display()))?;
        Self::from_entries(config.rpc_parameter_filters, Some(path))
    }

    pub(crate) fn matches(&self, method: &str, params: Option<&[Value]>) -> bool {
        if self.len == 0 {
            return false;
        }
        let Some(params) = params else {
            return false;
        };
        let Some(by_arity) = self.by_method_and_arity.get(method) else {
            return false;
        };
        if by_arity
            .get(&params.len())
            .is_some_and(|candidates| candidates.iter().any(|candidate| candidate == params))
        {
            return true;
        }

        Self::matches_address_wide_filter(method, params, by_arity)
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn from_entries(
        entries: Vec<Vec<Value>>,
        path: Option<&Path>,
    ) -> Result<Self, String> {
        let mut filters = Self::default();
        for (index, mut entry) in entries.into_iter().enumerate() {
            if entry.is_empty() {
                return Err(Self::entry_error(path, index, "entry must not be empty"));
            }
            let method = entry.remove(0);
            let Value::String(method) = method else {
                return Err(Self::entry_error(
                    path,
                    index,
                    "first value must be a method string",
                ));
            };
            if method.trim().is_empty() {
                return Err(Self::entry_error(path, index, "method must not be empty"));
            }

            let candidates = filters
                .by_method_and_arity
                .entry(method)
                .or_default()
                .entry(entry.len())
                .or_default();
            if !candidates.contains(&entry) {
                candidates.push(entry);
                filters.len += 1;
            }
        }
        Ok(filters)
    }

    fn entry_error(path: Option<&Path>, index: usize, message: &str) -> String {
        let source = path
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("<inline>"));
        format!(
            "invalid rpc-parameter-filters entry {} in '{}': {message}",
            index + 1,
            source.display()
        )
    }

    fn matches_address_wide_filter(
        method: &str,
        params: &[Value],
        by_arity: &HashMap<usize, Vec<Vec<Value>>>,
    ) -> bool {
        if !matches!(
            method,
            "getSignaturesForAddress" | "getTransactionsForAddress"
        ) {
            return false;
        }
        let Some(address) = params.first().and_then(Value::as_str) else {
            return false;
        };

        by_arity.get(&1).is_some_and(|candidates| {
            candidates
                .iter()
                .any(|candidate| candidate.first().and_then(Value::as_str) == Some(address))
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::RpcParameterFilterSet;

    fn filters(entries: Vec<Vec<Value>>) -> RpcParameterFilterSet {
        RpcParameterFilterSet::from_entries(entries, None).expect("valid filters")
    }

    use serde_json::Value;

    #[test]
    fn exact_match_is_indexed_by_method_and_arity() {
        let filters = filters(vec![
            vec![json!("getThing"), json!("address")],
            vec![json!("getThing"), json!("address"), json!({"mode": "full"})],
        ]);

        assert!(filters.matches("getThing", Some(&[json!("address")])));
        assert!(filters.matches(
            "getThing",
            Some(&[json!("address"), json!({"mode": "full"})])
        ));
        assert!(!filters.matches("getthing", Some(&[json!("address")])));
        assert!(!filters.matches(
            "getThing",
            Some(&[json!("address"), json!({"mode": "other"})])
        ));
        assert!(!filters.matches(
            "getThing",
            Some(&[json!("address"), json!({"mode": "full"}), json!(true)])
        ));
        assert!(!filters.matches("getThing", None));
    }

    #[test]
    fn object_key_order_does_not_affect_equality() {
        let filters = filters(vec![vec![
            json!("getThing"),
            json!({"first": 1, "second": 2}),
        ]]);

        assert!(filters.matches("getThing", Some(&[json!({"second": 2, "first": 1})])));
    }

    #[test]
    fn address_only_filters_match_trailing_configs_for_address_history_methods() {
        let address = "ComputeBudget111111111111111111111111111111";
        let filters = filters(vec![
            vec![json!("getSignaturesForAddress"), json!(address)],
            vec![json!("getTransactionsForAddress"), json!(address)],
        ]);

        for method in ["getSignaturesForAddress", "getTransactionsForAddress"] {
            assert!(filters.matches(method, Some(&[json!(address)])));
            assert!(filters.matches(method, Some(&[json!(address), json!({"limit": 25})])));
            assert!(!filters.matches(method, Some(&[json!("other-address"), json!({})])));
        }
    }

    #[test]
    fn address_wide_matching_requires_a_string_address() {
        let filters = filters(vec![vec![json!("getSignaturesForAddress"), json!(42)]]);

        assert!(filters.matches("getSignaturesForAddress", Some(&[json!(42)])));
        assert!(!filters.matches(
            "getSignaturesForAddress",
            Some(&[json!(42), json!({"limit": 10})])
        ));
    }

    #[test]
    fn duplicate_entries_are_removed() {
        let filters = filters(vec![
            vec![json!("getThing"), json!("address")],
            vec![json!("getThing"), json!("address")],
        ]);

        assert_eq!(filters.len(), 1);
    }

    #[test]
    fn invalid_entries_are_rejected_with_index() {
        let err = RpcParameterFilterSet::from_entries(
            vec![vec![json!("getThing"), json!(1)], vec![json!(42)]],
            None,
        )
        .expect_err("non-string method must fail");

        assert!(err.contains("entry 2"));
        assert!(err.contains("first value must be a method string"));
    }

    #[test]
    fn shared_yaml_ignores_ingestor_keys_and_loads_filters() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("superbank.yaml");
        std::fs::write(
            &path,
            r#"
source: grpc
endpoint: https://example.invalid
rpc-parameter-filters:
  - [getThing, address]
"#,
        )
        .expect("write config");

        let filters = RpcParameterFilterSet::load(Some(&path)).expect("load config");
        assert!(filters.matches("getThing", Some(&[json!("address")])));
    }
}
