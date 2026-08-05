// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
};

use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Default, Deserialize)]
#[serde(default, rename_all = "kebab-case")]
struct RpcFileConfig {
    rpc_method_filters: Vec<String>,
    rpc_parameter_filters: Vec<Vec<Value>>,
}

/// Startup-compiled RPC request filters.
///
/// The first lookup excludes unfiltered methods. The arity lookup then excludes
/// differently-shaped calls before any structural JSON comparisons are needed.
/// Single-address filters for address-history methods also match requests with
/// trailing configuration parameters.
/// Cursor-conditional address filters are indexed separately so request config
/// fields other than cursor presence do not require structural comparisons.
#[derive(Debug, Default)]
pub(crate) struct RpcParameterFilterSet {
    blocked_methods: HashSet<String>,
    by_method_and_arity: HashMap<String, HashMap<usize, Vec<Vec<Value>>>>,
    cursor_filtered_addresses: HashMap<String, HashSet<String>>,
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
        Self::from_config(
            config.rpc_method_filters,
            config.rpc_parameter_filters,
            Some(path),
        )
    }

    pub(crate) fn matches(&self, method: &str, params: Option<&[Value]>) -> bool {
        if self.len == 0 {
            return false;
        }
        if self.blocked_methods.contains(method) {
            return true;
        }
        let Some(params) = params else {
            return false;
        };
        if self.matches_cursor_filtered_address(method, params) {
            return true;
        }
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

    #[cfg(test)]
    pub(crate) fn from_entries(
        entries: Vec<Vec<Value>>,
        path: Option<&Path>,
    ) -> Result<Self, String> {
        Self::from_config(Vec::new(), entries, path)
    }

    pub(crate) fn from_config(
        methods: Vec<String>,
        entries: Vec<Vec<Value>>,
        path: Option<&Path>,
    ) -> Result<Self, String> {
        let mut filters = Self::default();
        for (index, method) in methods.into_iter().enumerate() {
            if method.trim().is_empty() {
                return Err(Self::method_entry_error(
                    path,
                    index,
                    "method must not be empty",
                ));
            }
            if filters.blocked_methods.insert(method) {
                filters.len += 1;
            }
        }

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

            if let Some(address) = Self::cursor_filter_address(&method, &entry) {
                if filters
                    .cursor_filtered_addresses
                    .entry(method)
                    .or_default()
                    .insert(address.to_owned())
                {
                    filters.len += 1;
                }
                continue;
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

    fn method_entry_error(path: Option<&Path>, index: usize, message: &str) -> String {
        let source = path
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("<inline>"));
        format!(
            "invalid rpc-method-filters entry {} in '{}': {message}",
            index + 1,
            source.display()
        )
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

    fn cursor_filter_address<'a>(method: &str, params: &'a [Value]) -> Option<&'a str> {
        let [Value::String(address), Value::Object(config)] = params else {
            return None;
        };
        Self::cursor_keys(method)
            .iter()
            .any(|key| config.contains_key(*key))
            .then_some(address.as_str())
    }

    fn matches_cursor_filtered_address(&self, method: &str, params: &[Value]) -> bool {
        let Some(addresses) = self.cursor_filtered_addresses.get(method) else {
            return false;
        };
        let Some(address) = params.first().and_then(Value::as_str) else {
            return false;
        };
        if !addresses.contains(address) {
            return false;
        }
        let Some(config) = params.get(1).and_then(Value::as_object) else {
            return false;
        };

        Self::cursor_keys(method)
            .iter()
            .any(|key| config.get(*key).is_some_and(|value| !value.is_null()))
    }

    fn cursor_keys(method: &str) -> &'static [&'static str] {
        match method {
            "getSignaturesForAddress" => &["before", "until"],
            "getTransactionsForAddress" => &["paginationToken"],
            _ => &[],
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::RpcParameterFilterSet;

    fn filters(entries: Vec<Vec<Value>>) -> RpcParameterFilterSet {
        RpcParameterFilterSet::from_entries(entries, None).expect("valid filters")
    }

    fn filters_with_methods(methods: Vec<&str>, entries: Vec<Vec<Value>>) -> RpcParameterFilterSet {
        RpcParameterFilterSet::from_config(
            methods.into_iter().map(str::to_owned).collect(),
            entries,
            None,
        )
        .expect("valid filters")
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
    fn method_filters_match_any_parameter_shape() {
        let filters = filters_with_methods(vec!["getTransactionsForAddress"], Vec::new());

        assert!(filters.matches("getTransactionsForAddress", None));
        assert!(filters.matches("getTransactionsForAddress", Some(&[])));
        assert!(filters.matches(
            "getTransactionsForAddress",
            Some(&[json!("address"), json!({"limit": 25})])
        ));
        assert!(!filters.matches("gettransactionsforaddress", None));
        assert!(!filters.matches("getSignaturesForAddress", None));
    }

    #[test]
    fn method_filters_are_deduplicated_and_reject_blank_names() {
        let filters = filters_with_methods(
            vec!["getTransactionsForAddress", "getTransactionsForAddress"],
            Vec::new(),
        );
        assert_eq!(filters.len(), 1);

        let err = RpcParameterFilterSet::from_config(vec!["  ".to_string()], Vec::new(), None)
            .expect_err("blank method must fail");
        assert!(err.contains("rpc-method-filters entry 1"));
        assert!(err.contains("method must not be empty"));
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
    fn cursor_filters_ignore_values_and_other_config_fields() {
        let gsfa_address = "ComputeBudget111111111111111111111111111111";
        let gtfa_address = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";
        let filters = filters(vec![
            vec![
                json!("getSignaturesForAddress"),
                json!(gsfa_address),
                json!({"commitment": "confirmed", "limit": 10, "before": "ignored"}),
            ],
            vec![
                json!("getTransactionsForAddress"),
                json!(gtfa_address),
                json!({"limit": 25, "paginationToken": "ignored"}),
            ],
        ]);

        assert!(filters.matches(
            "getSignaturesForAddress",
            Some(&[json!(gsfa_address), json!({"before": "different"})])
        ));
        assert!(filters.matches(
            "getSignaturesForAddress",
            Some(&[
                json!(gsfa_address),
                json!({"commitment": "finalized", "limit": 100, "until": "different"})
            ])
        ));
        assert!(filters.matches(
            "getTransactionsForAddress",
            Some(&[
                json!(gtfa_address),
                json!({"limit": 1, "paginationToken": "different", "sortOrder": "asc"})
            ])
        ));
    }

    #[test]
    fn cursor_filters_require_a_non_null_cursor_for_the_same_address() {
        let address = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";
        let filters = filters(vec![vec![
            json!("getTransactionsForAddress"),
            json!(address),
            json!({"paginationToken": "ignored"}),
        ]]);

        assert!(!filters.matches(
            "getTransactionsForAddress",
            Some(&[json!(address), json!({"paginationToken": null})])
        ));
        assert!(!filters.matches(
            "getTransactionsForAddress",
            Some(&[json!(address), json!({"limit": 25})])
        ));
        assert!(!filters.matches(
            "getTransactionsForAddress",
            Some(&[
                json!("different-address"),
                json!({"paginationToken": "present"})
            ])
        ));
    }

    #[test]
    fn gsfa_cursor_filter_treats_null_before_and_until_as_absent() {
        let address = "ComputeBudget111111111111111111111111111111";
        let filters = filters(vec![vec![
            json!("getSignaturesForAddress"),
            json!(address),
            json!({"before": "ignored"}),
        ]]);

        assert!(!filters.matches(
            "getSignaturesForAddress",
            Some(&[json!(address), json!({"before": null, "until": null})])
        ));
        assert!(filters.matches(
            "getSignaturesForAddress",
            Some(&[json!(address), json!({"before": null, "until": "present"})])
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
rpc-method-filters:
  - getTransactionsForAddress
"#,
        )
        .expect("write config");

        let filters = RpcParameterFilterSet::load(Some(&path)).expect("load config");
        assert!(filters.matches("getThing", Some(&[json!("address")])));
        assert!(filters.matches("getTransactionsForAddress", None));
        assert_eq!(filters.len(), 2);
    }
}
