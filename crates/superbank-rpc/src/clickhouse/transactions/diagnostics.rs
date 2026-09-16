// SPDX-License-Identifier: AGPL-3.0-only
//! Opt-in observations of guarded local reads; never linked into server binaries.
use super::*;
use clickhouse::{RowOwned, RowRead};
use serde_json::{Value, json};

async fn measure<T: RowOwned + RowRead>(client: &ClickHouseClient, sql: &str) -> (Value, T) {
    let start = Instant::now();
    let lease = client.acquire_http_query_permit().await.unwrap();
    let admission_ms = start.elapsed().as_secs_f64() * 1000.0;
    lease
        .scope(async {
            let start = Instant::now();
            let query = client.read_query(sql, "gettx_diagnostic").await.unwrap();
            let query_id = query.query_id().to_owned();
            let submitted_ms = start.elapsed().as_secs_f64() * 1000.0;
            let mut cursor = query.fetch::<T>().unwrap();
            let row = cursor.next().await.unwrap().expect("fixture row");
            let first_row_ms = start.elapsed().as_secs_f64() * 1000.0;
            assert!(cursor.next().await.unwrap().is_none());
            let complete_ms = start.elapsed().as_secs_f64() * 1000.0;
            (
                json!({"query_id":query_id,"admission_ms":admission_ms,
            "read_endpoint_admission_and_setup_ms":submitted_ms,
            "first_row_ms":first_row_ms,"complete_ms":complete_ms,
            "first_row_to_eof_ms":complete_ms-first_row_ms}),
                row,
            )
        })
        .await
}

pub(crate) async fn measure_position_reads(
    client: &ClickHouseClient,
    signature: &str,
    position: SignatureSlot,
) -> Value {
    #[derive(Deserialize, clickhouse::Row)]
    struct Position {
        slot: u64,
        slot_idx: u32,
    }
    let (bytes, literal) = decode_transaction_signature(signature).unwrap();
    let bucket = cityhash64(bytes.as_ref()) % client.signatures_bucket_modulus();
    let settings =
        client.select_settings_clause("get_signature_slot", QueryFreshnessClass::Historical);
    let signature_sql = format!(
        "SELECT slot,slot_idx FROM {} PREWHERE sig_bucket={bucket} AND signature={literal}{} ORDER BY slot DESC,slot_idx DESC,signature LIMIT 1 {settings}",
        client.signature_statuses_table,
        client.cache_slot_predicate()
    );
    let payload_sql = build_get_transaction_by_signature_query(
        &client.transaction_table,
        &literal,
        position.slot,
        Some(position.slot_idx),
        &client.select_get_transaction_settings_clause(
            "get_transaction_by_signature_distributed",
            QueryFreshnessClass::Historical,
        ),
    );
    let (signature, found) = measure::<Position>(client, &signature_sql).await;
    assert_eq!(
        (found.slot, found.slot_idx),
        (position.slot, position.slot_idx)
    );
    let (payload, found) = measure::<TransactionRow>(client, &payload_sql).await;
    assert_eq!(
        (found.slot, found.slot_idx),
        (position.slot, position.slot_idx)
    );
    json!({"signature":signature,"payload":payload})
}

pub(crate) async fn measure_layout_sample(
    client: &ClickHouseClient,
    signature_text: &str,
    position: SignatureSlot,
    two_step: bool,
) -> Value {
    #[derive(Deserialize, clickhouse::Row)]
    struct Position {
        slot: u64,
        slot_idx: u32,
    }
    let started = Instant::now();
    let (bytes, literal) = decode_transaction_signature(signature_text).unwrap();
    let signature = if two_step {
        let bucket = cityhash64(bytes.as_ref()) % client.signatures_bucket_modulus();
        let settings =
            client.select_settings_clause("get_signature_slot", QueryFreshnessClass::Historical);
        let sql = format!(
            "SELECT slot,slot_idx FROM {} PREWHERE sig_bucket={bucket} AND signature={literal}{} ORDER BY slot DESC,slot_idx DESC,signature LIMIT 1 {settings}",
            client.signature_statuses_table,
            client.cache_slot_predicate()
        );
        let (timings, found) = measure::<Position>(client, &sql).await;
        assert_eq!(
            (found.slot, found.slot_idx),
            (position.slot, position.slot_idx)
        );
        Some(timings)
    } else {
        None
    };
    let sql = build_get_transaction_by_signature_query(
        &client.transaction_table,
        &literal,
        position.slot,
        Some(position.slot_idx),
        &client.select_get_transaction_settings_clause(
            "get_transaction_by_signature_distributed",
            QueryFreshnessClass::Historical,
        ),
    );
    let (payload, found) = measure::<TransactionRow>(client, &sql).await;
    let total_ms = started.elapsed().as_secs_f64() * 1000.0;
    assert_eq!(
        (found.slot, found.slot_idx),
        (position.slot, position.slot_idx)
    );
    let record = map_transaction_row(found);
    assert_eq!(record.signature.as_slice(), bytes.as_ref());
    let serialized = serde_json::to_vec(&record).unwrap();
    let digest = blake3::hash(&serialized).to_hex().to_string();
    for encoding in [
        solana_transaction_status::UiTransactionEncoding::Json,
        solana_transaction_status::UiTransactionEncoding::Base64,
    ] {
        crate::hydration::hydrate_transaction_record(&record, encoding, Some(0)).unwrap();
    }
    json!({"signature":signature,"payload":payload,"total_ms":total_ms,
        "payload_digest":digest,"mapped_payload_bytes":serialized.len(),"tx_version":record.tx_version})
}
