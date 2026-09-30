// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Local disk-cache getTransaction: signature position and payload in one query.

use ch_cityhash102::cityhash64;

use crate::processing::{ProcessingError, ProcessingResult};
use crate::solana_sdk::signature::Signature;

use super::QueryFreshnessClass;
use super::client::ClickHouseClient;
use super::queries::TRANSACTION_SELECT_COLUMNS;
use super::rows::{fetch_single_transaction_row, map_transaction_row};
use super::types::{QueryTimings, StoredTransactionRecord};

/// The inner lookup mirrors the per-partition signature lookup: over disjoint
/// partitions, its newest position is the one a newest-first loop finds first.
/// The outer read keeps the exact `(slot, slot_idx, signature)` identity; a
/// stale position or a secondary signature therefore returns no row.
pub(crate) fn build_fused_transaction_query(
    transaction_table: &str,
    signature_statuses_table: &str,
    sig_bucket: u64,
    signature_literal: &str,
    (low, high): (u64, u64),
    settings_clause: &str,
) -> String {
    format!(
        "SELECT
            {columns}
         FROM {transaction_table}
         PREWHERE slot BETWEEN {low} AND {high} AND signature = {signature_literal}
             AND (slot, slot_idx) IN (
                 SELECT slot, slot_idx
                 FROM {signature_statuses_table}
                 PREWHERE sig_bucket = {sig_bucket} AND signature = {signature_literal}
                     AND slot BETWEEN {low} AND {high}
                 ORDER BY slot DESC, slot_idx DESC, signature
                 LIMIT 1)
         LIMIT 1
         {settings_clause}",
        columns = TRANSACTION_SELECT_COLUMNS,
    )
}

impl ClickHouseClient {
    /// Resolve the newest indexed position within `cache_slot_range` and read its
    /// payload under one admission. No row is not absence: the caller must fall back
    /// to the separate position and payload reads, which alone decide a miss.
    pub(crate) async fn get_transaction_fused(
        &self,
        signature: &Signature,
    ) -> ProcessingResult<(Option<StoredTransactionRecord>, QueryTimings)> {
        let (Some(_), Some(range)) = (self.cache_partition, self.cache_slot_range) else {
            return Err(ProcessingError::database_msg(
                "fused transaction read requires a local-cache slot range".to_string(),
            ));
        };
        self.with_http_query_timeout("get_transaction_fused", async {
            let bytes = signature.as_ref();
            let sig_bucket = cityhash64(bytes) % self.signatures_bucket_modulus();
            let literal = format!(
                "toFixedString(unhex('{}'), 64)",
                hex::encode(bytes).to_uppercase()
            );
            let settings_clause = self.select_get_transaction_settings_clause(
                "get_transaction_fused",
                QueryFreshnessClass::Historical,
            );
            let query = build_fused_transaction_query(
                &self.transaction_table,
                &self.signature_statuses_table,
                sig_bucket,
                &literal,
                range,
                &settings_clause,
            );
            let (row, timings) =
                fetch_single_transaction_row(&self.client, &self.read_endpoint, &query).await?;
            Ok((row.map(map_transaction_row), timings))
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn normalize_sql(sql: &str) -> String {
        sql.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    #[test]
    fn fused_query_bounds_both_reads_and_keeps_payload_identity() {
        let query = normalize_sql(&build_fused_transaction_query(
            "cache.transactions",
            "cache.signatures",
            7,
            "toFixedString(unhex('AB'), 64)",
            (20, 79),
            "SETTINGS max_execution_time=1",
        ));
        assert!(query.contains(
            "FROM cache.transactions PREWHERE slot BETWEEN 20 AND 79 \
             AND signature = toFixedString(unhex('AB'), 64) AND (slot, slot_idx) IN ("
        ));
        assert!(query.contains(
            "SELECT slot, slot_idx FROM cache.signatures PREWHERE sig_bucket = 7 \
             AND signature = toFixedString(unhex('AB'), 64) AND slot BETWEEN 20 AND 79 \
             ORDER BY slot DESC, slot_idx DESC, signature LIMIT 1)"
        ));
        assert!(query.ends_with("LIMIT 1) LIMIT 1 SETTINGS max_execution_time=1"));
    }

    #[tokio::test]
    async fn fused_read_requires_a_bounded_local_cache_client() {
        let mut client = crate::tests::test_state().clickhouse.clone();
        let signature = Signature::from([1; 64]);
        for (partition, range) in [(None, Some((0, 9))), (Some((10, 0)), None)] {
            client.cache_partition = partition;
            client.cache_slot_range = range;
            assert!(matches!(
                client.get_transaction_fused(&signature).await,
                Err(ProcessingError::Database { .. })
            ));
        }
    }
}
