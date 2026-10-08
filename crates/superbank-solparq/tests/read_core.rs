use std::{fs::File, path::Path, sync::Arc};

use arrow_array::{
    ArrayRef, FixedSizeBinaryArray, RecordBatch, StringArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use parquet::{
    arrow::ArrowWriter,
    data_type::{ByteArray, ByteArrayType, Int64Type},
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::parser::parse_message_type,
};
use serde_json::Value;
use serde_json::json;
use superbank_solparq::read::{
    archive_name::{ParsedArchiveName, parse_archive_name},
    config::{Cli, ScanSelection},
    query::{ArchiveInput, ScanOptions, scan_archive, summarize_archive},
    render,
};
use tempfile::TempDir;

#[test]
fn parse_archive_name_accepts_solparq_file_names() {
    assert_eq!(
        parse_archive_name("custom_989_427299625-427300124.parquet"),
        Some(ParsedArchiveName {
            kind_label: "custom".to_string(),
            epoch: 989,
            start_slot: 427_299_625,
            end_slot: 427_300_124,
        })
    );

    assert_eq!(parse_archive_name("not-an-archive.parquet"), None);
}

#[tokio::test]
async fn summary_counts_rows_slots_blocks_and_file_metadata() {
    let (_dir, archive_path) = write_test_archive();

    let summary = summarize_archive(ArchiveInput::LocalFile(archive_path))
        .await
        .expect("summarize archive");

    assert_eq!(summary.transaction_rows, 5);
    assert_eq!(summary.actual_min_slot, Some(10));
    assert_eq!(summary.actual_max_slot, Some(13));
    assert_eq!(summary.observed_slots, 4);
    assert_eq!(summary.observed_blocks, 4);
    assert_eq!(summary.row_groups, 1);
    assert_eq!(summary.columns, 4);
}

#[tokio::test]
async fn summary_projects_slot_column_and_ignores_invalid_utf8_columns() {
    let (_dir, archive_path) = write_archive_with_invalid_utf8_column();

    let summary = summarize_archive(ArchiveInput::LocalFile(archive_path))
        .await
        .expect("summarize archive with invalid utf8 payload column");

    assert_eq!(summary.transaction_rows, 3);
    assert_eq!(summary.actual_min_slot, Some(10));
    assert_eq!(summary.actual_max_slot, Some(12));
    assert_eq!(summary.observed_slots, 3);
    assert_eq!(summary.observed_blocks, 3);
    assert_eq!(summary.row_groups, 1);
    assert_eq!(summary.columns, 2);
}

#[tokio::test]
async fn summary_reads_db_archive_bundle_manifest_and_table_counts() {
    let (_dir, bundle_path) = write_test_bundle();
    let cli = Cli::try_parse_from([
        "superbank-solparq-read",
        "summary",
        "--archive",
        bundle_path.to_str().expect("bundle path"),
    ])
    .expect("valid summary command");

    let output = render(cli).await.expect("render summary");

    assert!(output.contains("archive_format: bundle"));
    assert!(output.contains("archive: custom_0_10-13"));
    assert!(output.contains("table_transactions_rows: 5"));
    assert!(output.contains("table_blocks_metadata_rows: 4"));
}

#[tokio::test]
async fn reader_reads_hourly_manifests_for_both_cadences() {
    use superbank_solparq::{
        archive::{ArchiveKind, ClickHouseBounds, plan_next_archive_with_hourly_slot_duration},
        manifest::{ArchiveManifest, MANIFEST_FORMAT_VERSION},
        read::config::ArchiveTable,
    };

    for duration in [400, 200] {
        let (dir, old_bundle_path) = write_test_bundle();
        let plan = plan_next_archive_with_hourly_slot_duration(
            ArchiveKind::Hourly,
            ClickHouseBounds {
                earliest_slot: 10,
                latest_slot: 18_009,
                distinct_slots: 4,
            },
            None,
            true,
            false,
            duration,
        )
        .unwrap()
        .unwrap();
        let bundle_path = dir.path().join(plan.archive_id());
        std::fs::rename(old_bundle_path, &bundle_path).unwrap();
        let manifest_path = bundle_path.join("manifest.json");
        let legacy: Value =
            serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
        let manifest = ArchiveManifest::new(
            plan.archive_id(),
            plan.kind,
            plan.epoch,
            plan.start_slot,
            plan.end_slot,
            serde_json::from_value(legacy["tables"].clone()).unwrap(),
            vec![],
        );
        assert_eq!(manifest.archive_kind, "hourly");
        std::fs::write(manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();

        let summary = summarize_archive(ArchiveInput::LocalBundle {
            dir: bundle_path,
            table: ArchiveTable::Transactions,
        })
        .await
        .unwrap();
        assert_eq!(summary.archive_name, plan.archive_id());
        assert_eq!(summary.format_version, Some(MANIFEST_FORMAT_VERSION));
        assert_eq!(summary.transaction_rows, 5);
        assert_eq!(summary.actual_min_slot, Some(10));
        assert_eq!(summary.actual_max_slot, Some(13));
    }
}

#[tokio::test]
async fn scan_filters_transactions_by_inclusive_slot_range() {
    let (_dir, archive_path) = write_test_archive();

    let rows = scan_archive(
        ArchiveInput::LocalFile(archive_path),
        ScanOptions {
            selection: ScanSelection::SlotRange {
                start_slot: 11,
                end_slot: 12,
            },
            columns: vec!["slot".to_string(), "signature".to_string()],
            limit: None,
        },
    )
    .await
    .expect("scan archive");

    assert_eq!(
        rows,
        vec![
            json!({"slot": 11, "signature": "sig-b"}),
            json!({"slot": 12, "signature": "sig-c"}),
            json!({"slot": 12, "signature": "sig-d"}),
        ]
    );
}

#[tokio::test]
async fn scan_reads_transactions_from_db_archive_bundle_by_default() {
    let (_dir, bundle_path) = write_test_bundle();
    let cli = Cli::try_parse_from([
        "superbank-solparq-read",
        "scan",
        "--archive",
        bundle_path.to_str().expect("bundle path"),
        "--slot-range",
        "11-12",
        "--columns",
        "slot,signature",
        "--format",
        "json",
    ])
    .expect("valid scan command");

    let output = render(cli).await.expect("render scan");
    let rows: Vec<Value> = serde_json::from_str(&output).expect("json rows");

    assert_eq!(
        rows,
        vec![
            json!({"slot": 11, "signature": "sig-b"}),
            json!({"slot": 12, "signature": "sig-c"}),
            json!({"slot": 12, "signature": "sig-d"}),
        ]
    );
}

#[tokio::test]
async fn scan_filters_before_applying_output_columns() {
    let (_dir, archive_path) = write_test_archive();

    let rows = scan_archive(
        ArchiveInput::LocalFile(archive_path),
        ScanOptions {
            selection: ScanSelection::SlotRange {
                start_slot: 12,
                end_slot: 12,
            },
            columns: vec!["signature".to_string()],
            limit: None,
        },
    )
    .await
    .expect("scan archive");

    assert_eq!(
        rows,
        vec![json!({"signature": "sig-c"}), json!({"signature": "sig-d"})]
    );
}

#[tokio::test]
async fn scan_without_columns_explains_invalid_utf8_projection_failure() {
    let (_dir, archive_path) = write_archive_with_invalid_utf8_column();

    let err = scan_archive(
        ArchiveInput::LocalFile(archive_path),
        ScanOptions {
            selection: ScanSelection::SlotRange {
                start_slot: 10,
                end_slot: 10,
            },
            columns: Vec::new(),
            limit: Some(1),
        },
    )
    .await
    .expect_err("unprojected scan should fail on invalid utf8 payload");

    assert!(
        err.to_string().contains("--columns"),
        "error should point users to projection, got: {err:?}"
    );
}

#[test]
fn cli_rejects_scan_without_range_or_all() {
    let err = Cli::try_parse_from([
        "superbank-solparq-read",
        "scan",
        "--archive",
        "./archive.parquet",
        "--format",
        "jsonl",
    ])
    .expect_err("scan without --slot-range or --all should fail");

    assert!(
        err.to_string()
            .contains("exactly one of --slot-range or --all is required"),
        "{err}"
    );
}

#[test]
fn cli_accepts_scan_with_slot_range() {
    let cli = Cli::try_parse_from([
        "superbank-solparq-read",
        "scan",
        "--archive",
        "./archive.parquet",
        "--slot-range",
        "10-20",
        "--columns",
        "slot,signature",
    ])
    .expect("valid scan arguments");

    let command = cli.command;
    let scan = command.scan_args().expect("scan command");
    assert_eq!(
        scan.selection,
        ScanSelection::SlotRange {
            start_slot: 10,
            end_slot: 20
        }
    );
    assert_eq!(scan.columns, vec!["slot", "signature"]);
}

fn write_test_archive() -> (TempDir, std::path::PathBuf) {
    let dir = TempDir::new().expect("temp dir");
    let archive_path = dir.path().join("custom_0_10-13.parquet");
    write_parquet(&archive_path);
    (dir, archive_path)
}

fn write_test_bundle() -> (TempDir, std::path::PathBuf) {
    let dir = TempDir::new().expect("temp dir");
    let bundle_path = dir.path().join("custom_0_10-13");
    std::fs::create_dir(&bundle_path).expect("create bundle");
    write_parquet(&bundle_path.join("transactions.parquet"));
    write_blocks_parquet(&bundle_path.join("blocks_metadata.parquet"));
    std::fs::write(
        bundle_path.join("manifest.json"),
        r#"{
          "format_version": 1,
          "archive_id": "custom_0_10-13",
          "archive_kind": "custom:4",
          "start_slot": 10,
          "end_slot": 13,
          "tables": [
            {
              "kind": "transactions",
              "table_name": "transactions",
              "file_name": "transactions.parquet",
              "row_count": 5,
              "required": true
            },
            {
              "kind": "blocks_metadata",
              "table_name": "blocks_metadata",
              "file_name": "blocks_metadata.parquet",
              "row_count": 4,
              "required": true
            }
          ],
          "skipped_tables": [],
          "poh_tool_ready": true
        }"#,
    )
    .expect("write manifest");
    (dir, bundle_path)
}

fn write_parquet(path: &Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("slot_idx", DataType::UInt32, false),
        Field::new("signature", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![10, 11, 12, 12, 13])) as ArrayRef,
            Arc::new(UInt32Array::from(vec![0, 0, 0, 1, 0])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                "sig-a", "sig-b", "sig-c", "sig-d", "sig-e",
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("ok"),
                Some("ok"),
                Some("err"),
                Some("ok"),
                None,
            ])) as ArrayRef,
        ],
    )
    .expect("record batch");

    let file = File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("parquet writer");
    writer.write(&batch).expect("write parquet batch");
    writer.close().expect("close parquet writer");
}

fn write_blocks_parquet(path: &Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("executed_transaction_count", DataType::UInt64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![10, 11, 12, 13])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![1, 1, 2, 1])) as ArrayRef,
        ],
    )
    .expect("record batch");

    let file = File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("parquet writer");
    writer.write(&batch).expect("write parquet batch");
    writer.close().expect("close parquet writer");
}

fn write_archive_with_invalid_utf8_column() -> (TempDir, std::path::PathBuf) {
    let dir = TempDir::new().expect("temp dir");
    let archive_path = dir.path().join("custom_0_10-12.parquet");
    let schema = Arc::new(
        parse_message_type(
            "
            message archive {
                REQUIRED INT64 slot;
                REQUIRED BYTE_ARRAY payload (UTF8);
            }
            ",
        )
        .expect("parse parquet schema"),
    );
    let file = File::create(&archive_path).expect("create parquet");
    let props = Arc::new(WriterProperties::builder().build());
    let mut writer = SerializedFileWriter::new(file, schema, props).expect("parquet writer");
    let mut row_group = writer.next_row_group().expect("row group writer");

    let mut slot_column = row_group
        .next_column()
        .expect("slot column result")
        .expect("slot column");
    slot_column
        .typed::<Int64Type>()
        .write_batch(&[10, 11, 12], None, None)
        .expect("write slot column");
    slot_column.close().expect("close slot column");

    let invalid_utf8 = vec![
        ByteArray::from(vec![0xff, 0xfe]),
        ByteArray::from("valid"),
        ByteArray::from(vec![0x80]),
    ];
    let mut payload_column = row_group
        .next_column()
        .expect("payload column result")
        .expect("payload column");
    payload_column
        .typed::<ByteArrayType>()
        .write_batch(&invalid_utf8, None, None)
        .expect("write payload column");
    payload_column.close().expect("close payload column");

    row_group.close().expect("close row group");
    writer.close().expect("close parquet writer");
    (dir, archive_path)
}

#[tokio::test]
async fn blocks_metadata_footer_columns_scan_with_nulls() {
    let (_dir, bundle) = write_test_bundle();
    let schema = Arc::new(Schema::new(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("bank_id", DataType::UInt64, true),
        Field::new("bank_hash", DataType::FixedSizeBinary(32), true),
        Field::new("block_producer_time_nanos", DataType::UInt64, true),
        Field::new("block_user_agent", DataType::Utf8, true),
    ]));
    let hashes = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
        vec![None, Some([7u8; 32]), Some([8u8; 32])].into_iter(),
        32,
    )
    .unwrap();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![10, 11, 12])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![None, Some(7), Some(9)])) as ArrayRef,
            Arc::new(hashes) as ArrayRef,
            Arc::new(UInt64Array::from(vec![None, Some(110), Some(120)])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                None,
                Some("agave/4.3"),
                Some("agave/4.3"),
            ])) as ArrayRef,
        ],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(
        File::create(bundle.join("blocks_metadata.parquet")).unwrap(),
        schema,
        None,
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let cli = Cli::try_parse_from([
        "reader",
        "schema",
        "--archive",
        bundle.to_str().unwrap(),
        "--table",
        "blocks_metadata",
    ])
    .unwrap();
    let schema_output = render(cli).await.unwrap();
    assert!(schema_output.contains("block_producer_time_nanos"));
    assert!(schema_output.contains("block_user_agent"));
    let cli = Cli::try_parse_from([
        "reader",
        "scan",
        "--archive",
        bundle.to_str().unwrap(),
        "--table",
        "blocks_metadata",
        "--slot-range",
        "10-11",
        "--columns",
        "slot,bank_id,block_user_agent",
        "--format",
        "json",
    ])
    .unwrap();
    let rows: Value = serde_json::from_str(&render(cli).await.unwrap()).unwrap();
    assert_eq!(
        rows,
        json!([
            {"slot": 10},
            {"slot": 11, "bank_id": 7, "block_user_agent": "agave/4.3"}
        ])
    );
}

#[tokio::test]
async fn blocks_metadata_archive_without_footer_columns_still_scans() {
    let (_dir, bundle) = write_test_bundle();
    let cli = Cli::try_parse_from([
        "reader",
        "scan",
        "--archive",
        bundle.to_str().unwrap(),
        "--table",
        "blocks_metadata",
        "--slot-range",
        "10-11",
        "--columns",
        "slot,executed_transaction_count",
        "--format",
        "json",
    ])
    .unwrap();
    let rows: Value = serde_json::from_str(&render(cli).await.unwrap()).unwrap();
    assert_eq!(
        rows,
        json!([
            {"slot": 10, "executed_transaction_count": 1},
            {"slot": 11, "executed_transaction_count": 1}
        ])
    );
}
