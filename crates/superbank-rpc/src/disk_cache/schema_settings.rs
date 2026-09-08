// SPDX-License-Identifier: AGPL-3.0-only
//! Copy only portable, numeric MergeTree tuning from the source schema.

use super::schema::SchemaError;
use std::collections::BTreeMap;

const ALLOWED: &[&str] = &[
    "index_granularity",
    "index_granularity_bytes",
    "min_bytes_for_wide_part",
    "compress_primary_key",
    "compress_marks",
    "allow_experimental_reverse_key",
];

#[derive(Default)]
struct Scanner {
    depth: usize,
    quote: Option<char>,
    escaped: bool,
}

impl Scanner {
    fn consume(&mut self, ch: char) {
        if self.escaped {
            self.escaped = false;
            return;
        }
        if let Some(quote) = self.quote {
            if ch == '\\' {
                self.escaped = true;
            }
            if ch == quote {
                self.quote = None;
            }
            return;
        }
        match ch {
            '\'' | '"' | '`' => self.quote = Some(ch),
            '(' | '[' => self.depth += 1,
            ')' | ']' => self.depth = self.depth.saturating_sub(1),
            _ => {}
        }
    }
}

fn top_level_keyword(sql: &str, keyword: &str) -> Option<usize> {
    let mut scan = Scanner::default();
    for (i, ch) in sql.char_indices() {
        if scan.depth == 0
            && scan.quote.is_none()
            && sql[i..]
                .get(..keyword.len())
                .is_some_and(|s| s.eq_ignore_ascii_case(keyword))
        {
            return Some(i);
        }
        scan.consume(ch);
    }
    None
}

pub(super) fn extract(sql: &str) -> Result<BTreeMap<String, u64>, SchemaError> {
    let mut result = BTreeMap::new();
    let Some(start) = top_level_keyword(sql, " SETTINGS ") else {
        return Ok(result);
    };
    let tail = &sql[start + 10..];
    let end = top_level_keyword(tail, " AS ").unwrap_or(tail.len());
    for entry in super::schema::split_top_level(&tail[..end]) {
        let (name, value) = entry.trim().split_once('=').unwrap_or((entry.trim(), ""));
        let name = name.trim().trim_matches('`').to_ascii_lowercase();
        if !ALLOWED.contains(&name.as_str()) {
            continue;
        }
        let value = value
            .trim()
            .trim_end_matches(';')
            .parse::<u64>()
            .map_err(|_| SchemaError::Invalid(format!("invalid source setting {name}")))?;
        validate(&name, value)?;
        result.insert(name, value);
    }
    Ok(result)
}

pub(super) fn sorting_key(sql: &str) -> Option<String> {
    let start = top_level_keyword(sql, " ORDER BY ")? + 10;
    let tail = &sql[start..];
    let end = [" SETTINGS ", " TTL ", " COMMENT ", " AS ", " SAMPLE BY "]
        .iter()
        .filter_map(|keyword| top_level_keyword(tail, keyword))
        .min()
        .unwrap_or(tail.len());
    let key = tail[..end].trim().trim_end_matches(';');
    let key = key
        .strip_prefix("tuple(")
        .and_then(|s| s.strip_suffix(')'))
        .unwrap_or(key);
    Some(key.to_string())
}

fn validate(name: &str, value: u64) -> Result<(), SchemaError> {
    let invalid = match name {
        "compress_primary_key" | "compress_marks" | "allow_experimental_reverse_key" => value > 1,
        "index_granularity" => value == 0,
        _ => false,
    };
    if invalid {
        return Err(SchemaError::Invalid(format!(
            "invalid source setting {name}={value}"
        )));
    }
    Ok(())
}

pub(super) fn clause(settings: &BTreeMap<String, u64>) -> String {
    if settings.is_empty() {
        return String::new();
    }
    format!(
        " SETTINGS {}",
        settings
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn copies_only_top_level_portable_settings() {
        let sql = "CREATE TABLE t (s String DEFAULT ' SETTINGS compress_marks=0') ENGINE=MergeTree ORDER BY s SETTINGS storage_policy='x, y', index_granularity=512, compress_marks=1 AS SELECT s FROM source";
        assert_eq!(
            clause(&extract(sql).unwrap()),
            " SETTINGS compress_marks=1, index_granularity=512"
        );
    }
    #[test]
    fn retains_reverse_sort_directions_from_canonical_ddl() {
        assert_eq!(
            sorting_key(
                "CREATE TABLE t (a UInt64) ENGINE=MergeTree PRIMARY KEY a ORDER BY tuple(a DESC, b) SETTINGS allow_experimental_reverse_key=1"
            ),
            Some("a DESC, b".into())
        );
    }
    #[test]
    fn rejects_invalid_supported_values() {
        for value in ["-1", "0", "oops"] {
            assert!(
                extract(&format!(
                    "CREATE TABLE t SETTINGS index_granularity={value}"
                ))
                .is_err()
            );
        }
        assert!(extract("CREATE TABLE t SETTINGS compress_marks=2").is_err());
    }
}
