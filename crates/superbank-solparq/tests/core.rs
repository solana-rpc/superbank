use std::{fs, process::Command, str::FromStr};

use superbank_solparq::{
    archive::{
        ArchiveKind, ArchiveRunReport, ArchiveSlotRange, ClickHouseBounds, plan_archive_slot_range,
        plan_next_archive, safe_delete_archived_data_range,
    },
    clickhouse::{
        DbTables, S3ArchiveSql, SlotRange, TransactionMismatch, ValidationReport, build_delete_sql,
        build_local_parquet_query, build_s3_archive_sql, build_s3_table_archive_sql,
    },
    config::{ArchiveLocation, Config},
    metrics::AppState,
    server::{format_utc_timestamp, render_dashboard},
    storage::{archive_has_done_marker, local_archives_to_delete, write_local_sha256sums},
};

#[test]
fn hourly_plan_uses_prompt_filename_convention() {
    let plan = plan_next_archive(
        ArchiveKind::Hourly,
        ClickHouseBounds {
            earliest_slot: 427_236_024,
            latest_slot: 427_245_023,
        },
        None,
        true,
    )
    .expect("plan should succeed")
    .expect("enough slots are available");

    assert_eq!(plan.kind, ArchiveKind::Hourly);
    assert_eq!(plan.epoch, 988);
    assert_eq!(plan.start_slot, 427_236_024);
    assert_eq!(plan.end_slot, 427_245_023);
    assert_eq!(plan.file_name(), "hourly_988_427236024-427245023.parquet");
}

#[test]
fn epoch_plan_aligns_to_epoch_boundary_after_earliest_available_slot() {
    let plan = plan_next_archive(
        ArchiveKind::Epoch,
        ClickHouseBounds {
            earliest_slot: 426_800_000,
            latest_slot: 427_247_999,
        },
        None,
        true,
    )
    .expect("plan should succeed")
    .expect("enough slots are available");

    assert_eq!(plan.epoch, 988);
    assert_eq!(plan.start_slot, 426_816_000);
    assert_eq!(plan.end_slot, 427_247_999);
    assert_eq!(plan.file_name(), "epoch_988_426816000-427247999.parquet");
}

#[test]
fn custom_plan_defaults_to_one_thousand_slots_and_continues_after_last_archive() {
    let kind = ArchiveKind::Custom { slots: 1_000 };
    let plan = plan_next_archive(
        kind,
        ClickHouseBounds {
            earliest_slot: 10_000,
            latest_slot: 12_999,
        },
        Some("custom_0_10000-10999.parquet"),
        true,
    )
    .expect("plan should succeed")
    .expect("enough slots are available");

    assert_eq!(plan.kind, kind);
    assert_eq!(plan.start_slot, 11_000);
    assert_eq!(plan.end_slot, 11_999);
    assert_eq!(plan.file_name(), "custom_0_11000-11999.parquet");
}

#[test]
fn plan_can_start_from_oldest_slot_when_continuation_is_disabled() {
    let kind = ArchiveKind::Custom { slots: 1_000 };
    let plan = plan_next_archive(
        kind,
        ClickHouseBounds {
            earliest_slot: 10_000,
            latest_slot: 12_999,
        },
        Some("custom_0_10000-10999.parquet"),
        false,
    )
    .expect("plan should succeed")
    .expect("enough slots are available");

    assert_eq!(plan.kind, kind);
    assert_eq!(plan.start_slot, 10_000);
    assert_eq!(plan.end_slot, 10_999);
    assert_eq!(plan.file_name(), "custom_0_10000-10999.parquet");
}

#[test]
fn explicit_slot_range_plan_uses_requested_inclusive_range() {
    let plan = plan_archive_slot_range(
        ArchiveKind::Hourly,
        ClickHouseBounds {
            earliest_slot: 500,
            latest_slot: 4_000,
        },
        ArchiveSlotRange::new(1_000, 3_222).expect("valid slot range"),
    )
    .expect("plan should succeed")
    .expect("requested range is available");

    assert_eq!(plan.kind, ArchiveKind::Hourly);
    assert_eq!(plan.epoch, 0);
    assert_eq!(plan.start_slot, 1_000);
    assert_eq!(plan.end_slot, 3_222);
    assert_eq!(plan.file_name(), "hourly_0_1000-3222.parquet");
}

#[test]
fn explicit_slot_range_plan_requires_clickhouse_to_cover_range() {
    let plan = plan_archive_slot_range(
        ArchiveKind::Custom { slots: 1_000 },
        ClickHouseBounds {
            earliest_slot: 1_500,
            latest_slot: 3_000,
        },
        ArchiveSlotRange::new(1_000, 3_222).expect("valid slot range"),
    )
    .expect("plan should succeed");

    assert_eq!(plan, None);
}

#[test]
fn config_rejects_multiple_archive_types_outside_server_mode() {
    let err = Config::try_parse_from([
        "superbank-solparq",
        "--db-server",
        "127.0.0.1",
        "--db-user",
        "admin",
        "--db-password",
        "secret",
        "--archive-range-type",
        "hourly",
        "--archive-range-type",
        "epoch",
    ])
    .expect_err("multiple archive types require server mode");

    assert!(
        err.to_string()
            .contains("multiple archive range types require --server-mode"),
        "{err}"
    );
}

#[test]
fn config_accepts_one_shot_archive_slot_range() {
    let config = Config::try_parse_from([
        "superbank-solparq",
        "--db-server",
        "127.0.0.1",
        "--db-user",
        "admin",
        "--db-password",
        "secret",
        "--archive-range-type",
        "custom",
        "--archive-slot-range",
        "1000-3222",
    ])
    .expect("valid config");

    assert_eq!(
        config.archive_slot_range,
        Some(ArchiveSlotRange::new(1_000, 3_222).expect("valid slot range"))
    );
}

#[test]
fn config_rejects_archive_slot_range_in_server_mode() {
    let err = Config::try_parse_from([
        "superbank-solparq",
        "--db-server",
        "127.0.0.1",
        "--db-user",
        "admin",
        "--db-password",
        "secret",
        "--archive-range-type",
        "custom",
        "--server-mode",
        "--archive-slot-range",
        "1000-3222",
    ])
    .expect_err("explicit archive slot range should be one-shot only");

    assert!(
        err.to_string()
            .contains("--archive-slot-range is only supported for one-shot archives"),
        "{err}"
    );
}

#[test]
fn config_rejects_invalid_archive_slot_range() {
    let err = Config::try_parse_from([
        "superbank-solparq",
        "--db-server",
        "127.0.0.1",
        "--db-user",
        "admin",
        "--db-password",
        "secret",
        "--archive-range-type",
        "custom",
        "--archive-slot-range",
        "3222-1000",
    ])
    .expect_err("reversed archive slot range should fail");

    assert!(
        err.to_string()
            .contains("archive slot range start must be less than or equal to end"),
        "{err}"
    );
}

#[test]
fn config_rejects_multiple_custom_archive_sizes() {
    let err = Config::try_parse_from([
        "superbank-solparq",
        "--db-server",
        "127.0.0.1",
        "--db-user",
        "admin",
        "--db-password",
        "secret",
        "--archive-range-type",
        "custom:500",
        "--archive-range-type",
        "custom:1000",
        "--server-mode",
    ])
    .expect_err("multiple custom archive sizes should be rejected");

    assert!(
        err.to_string()
            .contains("only one custom archive range size can be configured"),
        "{err}"
    );
}

#[test]
fn config_rejects_duplicate_archive_types() {
    let err = Config::try_parse_from([
        "superbank-solparq",
        "--db-server",
        "127.0.0.1",
        "--db-user",
        "admin",
        "--db-password",
        "secret",
        "--archive-range-type",
        "hourly",
        "--archive-range-type",
        "hourly",
        "--server-mode",
    ])
    .expect_err("duplicate archive types should be rejected");

    assert!(
        err.to_string()
            .contains("duplicate archive range type 'hourly'"),
        "{err}"
    );
}

#[test]
fn binary_reports_crate_version() {
    let output = Command::new(env!("CARGO_BIN_EXE_superbank-solparq"))
        .arg("--version")
        .output()
        .expect("run superbank-solparq --version");

    assert!(
        output.status.success(),
        "superbank-solparq --version failed with status {:?}: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        concat!("superbank-solparq ", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn config_applies_required_defaults_and_s3_validation() {
    let config = Config::try_parse_from([
        "superbank-solparq",
        "--db-server",
        "192.168.0.184",
        "--db-user",
        "admin",
        "--db-password",
        "secret",
        "--archive-range-type",
        "custom",
        "--archive-location-type",
        "s3",
        "--archive-s3-bucket-name",
        "archives",
        "--archive-s3-auth-key",
        "key",
        "--archive-s3-auth-secret-key",
        "secret",
        "--archive-s3-endpoint",
        "https://s3.example.test",
    ])
    .expect("valid config");

    assert_eq!(config.clickhouse_url(), "http://192.168.0.184:8123");
    assert_eq!(config.transactions_table, "transactions");
    assert_eq!(config.blocks_table, "blocks_metadata");
    assert_eq!(config.entries_table, "entries");
    assert_eq!(config.gsfa_table, "gsfa");
    assert_eq!(config.gsfa_hot_table, "gsfa_hot");
    assert_eq!(config.signatures_table, "signatures");
    assert_eq!(config.token_owner_activity_table, "token_owner_activity");
    assert_eq!(
        config.archive_kinds,
        vec![ArchiveKind::Custom { slots: 1_000 }]
    );
    assert_eq!(config.archive_location, ArchiveLocation::S3);
    assert_eq!(config.ops_port, 30_303);
    assert_eq!(config.metrics_port, 31_313);
    assert_eq!(config.archives_to_keep, 5);
    assert_eq!(config.solana_rpc_url, "https://api.mainnet-beta.solana.com");
}

#[test]
fn db_archive_tables_cover_superbank_base_and_index_tables() {
    let config = Config::try_parse_from([
        "superbank-solparq",
        "--db-server",
        "127.0.0.1",
        "--db-user",
        "admin",
        "--db-password",
        "secret",
        "--archive-range-type",
        "hourly",
    ])
    .expect("valid config");

    let tables = DbTables::from_config(&config).archive_tables();
    let table_specs = tables
        .iter()
        .map(|table| {
            (
                table.kind.as_str(),
                table.table_name.as_str(),
                table.file_name(),
                table.order_by.as_str(),
                table.required,
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(
        table_specs,
        vec![
            (
                "transactions",
                "transactions",
                "transactions.parquet",
                "slot, slot_idx, signature",
                true
            ),
            (
                "blocks_metadata",
                "blocks_metadata",
                "blocks_metadata.parquet",
                "slot",
                true
            ),
            (
                "entries",
                "entries",
                "entries.parquet",
                "slot, entry_index",
                false
            ),
            (
                "gsfa",
                "gsfa",
                "gsfa.parquet",
                "slot, slot_idx, signature",
                false
            ),
            (
                "gsfa_hot",
                "gsfa_hot",
                "gsfa_hot.parquet",
                "slot, slot_idx, signature",
                false
            ),
            (
                "signatures",
                "signatures",
                "signatures.parquet",
                "slot, slot_idx, signature",
                false
            ),
            (
                "token_owner_activity",
                "token_owner_activity",
                "token_owner_activity.parquet",
                "slot, slot_idx, signature",
                false
            ),
        ]
    );
}

#[test]
fn s3_archive_sql_uses_clickhouse_s3_function_and_stable_order() {
    let table = DbTables {
        transactions_table: "transactions".to_string(),
        blocks_table: "blocks_metadata".to_string(),
        entries_table: "entries".to_string(),
        gsfa_table: "gsfa".to_string(),
        gsfa_hot_table: "gsfa_hot".to_string(),
        signatures_table: "signatures".to_string(),
        token_owner_activity_table: "token_owner_activity".to_string(),
    }
    .archive_tables()
    .into_iter()
    .find(|table| table.kind.as_str() == "transactions")
    .expect("transactions table");

    let sql = build_s3_archive_sql(S3ArchiveSql {
        table: &table,
        start_slot: 42,
        end_slot: 84,
        endpoint: "https://s3.us-west.example",
        bucket: "bucket",
        bucket_path: "prefix/hourly",
        archive_name: "hourly_0_42-84",
        access_key: "access",
        secret_key: "secret",
    });

    assert!(sql.contains("INSERT INTO FUNCTION s3("));
    assert!(sql.contains(
        "'https://s3.us-west.example/bucket/prefix/hourly/hourly_0_42-84/transactions.parquet'"
    ));
    assert!(sql.contains("'access'"));
    assert!(sql.contains("'secret'"));
    assert!(sql.contains("'Parquet'"));
    assert!(sql.contains("FROM transactions"));
    assert!(sql.contains("WHERE slot BETWEEN 42 AND 84"));
    assert!(sql.contains("ORDER BY slot, slot_idx, signature"));
}

#[test]
fn s3_table_archive_sql_writes_bundle_table_object() {
    let table = DbTables {
        transactions_table: "transactions".to_string(),
        blocks_table: "blocks_metadata".to_string(),
        entries_table: "entries".to_string(),
        gsfa_table: "gsfa".to_string(),
        gsfa_hot_table: "gsfa_hot".to_string(),
        signatures_table: "signatures".to_string(),
        token_owner_activity_table: "token_owner_activity".to_string(),
    }
    .archive_tables()
    .into_iter()
    .find(|table| table.kind.as_str() == "entries")
    .expect("entries table");

    let sql = build_s3_table_archive_sql(S3ArchiveSql {
        table: &table,
        start_slot: 42,
        end_slot: 84,
        endpoint: "https://s3.us-west.example",
        bucket: "bucket",
        bucket_path: "prefix/hourly",
        archive_name: "hourly_0_42-84",
        access_key: "access",
        secret_key: "secret",
    });

    assert!(sql.contains(
        "'https://s3.us-west.example/bucket/prefix/hourly/hourly_0_42-84/entries.parquet'"
    ));
    assert!(sql.contains("FROM entries"));
    assert!(sql.contains("WHERE slot BETWEEN 42 AND 84"));
    assert!(sql.contains("ORDER BY slot, entry_index"));
}

#[test]
fn local_parquet_query_streams_parquet_with_stable_order() {
    let sql = build_local_parquet_query("default.transactions", 1, 9);

    assert_eq!(
        sql,
        "SELECT * FROM default.transactions WHERE slot BETWEEN 1 AND 9 ORDER BY slot, slot_idx, signature FORMAT Parquet"
    );
}

#[test]
fn delete_sql_covers_all_configured_tables() {
    let tables = DbTables {
        transactions_table: "transactions".to_string(),
        blocks_table: "blocks_metadata".to_string(),
        entries_table: "entries".to_string(),
        gsfa_table: "gsfa".to_string(),
        gsfa_hot_table: "gsfa_hot".to_string(),
        signatures_table: "signatures".to_string(),
        token_owner_activity_table: "token_owner_activity".to_string(),
    };
    let statements = build_delete_sql(&tables, 100, 123);

    assert_eq!(
        statements,
        vec![
            "ALTER TABLE transactions DELETE WHERE slot BETWEEN 100 AND 123".to_string(),
            "ALTER TABLE blocks_metadata DELETE WHERE slot BETWEEN 100 AND 123".to_string(),
            "ALTER TABLE entries DELETE WHERE slot BETWEEN 100 AND 123".to_string(),
            "ALTER TABLE gsfa DELETE WHERE slot BETWEEN 100 AND 123".to_string(),
            "ALTER TABLE gsfa_hot DELETE WHERE slot BETWEEN 100 AND 123".to_string(),
            "ALTER TABLE signatures DELETE WHERE slot BETWEEN 100 AND 123".to_string(),
            "ALTER TABLE token_owner_activity DELETE WHERE slot BETWEEN 100 AND 123".to_string(),
        ]
    );
}

#[test]
fn validation_report_groups_legit_and_backfill_gap_ranges() {
    let report = ValidationReport::from_observed_slots(
        10,
        20,
        vec![10, 15, 20],
        vec![10, 11, 15, 20],
        vec![TransactionMismatch {
            slot: 20,
            expected: 2,
            actual: 1,
        }],
        None,
    );

    assert_eq!(
        report.not_produced_slot_ranges,
        vec![SlotRange::new(12, 14), SlotRange::new(16, 19),]
    );
    assert_eq!(report.missing_block_ranges, vec![SlotRange::new(11, 11)]);
    assert_eq!(
        report.transaction_mismatch_ranges,
        vec![SlotRange::new(20, 20)]
    );
    assert!(report.has_warnings());
}

#[test]
fn clickhouse_cleanup_waits_until_all_archive_types_cover_range() {
    let config = Config::try_parse_from([
        "superbank-solparq",
        "--db-server",
        "127.0.0.1",
        "--db-user",
        "admin",
        "--db-password",
        "secret",
        "--archive-range-type",
        "custom:500",
        "--archive-range-type",
        "hourly",
        "--server-mode",
        "--delete-archived-data-range",
    ])
    .expect("valid config");

    let delete_range = safe_delete_archived_data_range(
        &config,
        0,
        499,
        &[
            (
                ArchiveKind::Custom { slots: 500 },
                Some("custom_0_0-499".to_string()),
            ),
            (ArchiveKind::Hourly, None),
        ],
    )
    .expect("safe delete check");

    assert_eq!(delete_range, None);
}

#[test]
fn clickhouse_cleanup_allows_smaller_kind_after_larger_kind_covers_range() {
    let config = Config::try_parse_from([
        "superbank-solparq",
        "--db-server",
        "127.0.0.1",
        "--db-user",
        "admin",
        "--db-password",
        "secret",
        "--archive-range-type",
        "custom:500",
        "--archive-range-type",
        "hourly",
        "--server-mode",
        "--delete-archived-data-range",
    ])
    .expect("valid config");

    let delete_range = safe_delete_archived_data_range(
        &config,
        0,
        499,
        &[
            (
                ArchiveKind::Custom { slots: 500 },
                Some("custom_0_0-499".to_string()),
            ),
            (ArchiveKind::Hourly, Some("hourly_0_0-8999".to_string())),
        ],
    )
    .expect("safe delete check");

    assert_eq!(delete_range, Some(SlotRange::new(0, 499)));
}

#[test]
fn clickhouse_cleanup_deletes_only_safe_prefix_when_other_kinds_lag() {
    let config = Config::try_parse_from([
        "superbank-solparq",
        "--db-server",
        "127.0.0.1",
        "--db-user",
        "admin",
        "--db-password",
        "secret",
        "--archive-range-type",
        "custom:500",
        "--archive-range-type",
        "hourly",
        "--archive-range-type",
        "epoch",
        "--server-mode",
        "--delete-archived-data-range",
    ])
    .expect("valid config");

    let delete_range = safe_delete_archived_data_range(
        &config,
        0,
        431_999,
        &[
            (
                ArchiveKind::Custom { slots: 500 },
                Some("custom_0_0-999".to_string()),
            ),
            (ArchiveKind::Hourly, Some("hourly_0_0-8999".to_string())),
            (ArchiveKind::Epoch, Some("epoch_0_0-431999".to_string())),
        ],
    )
    .expect("safe delete check");

    assert_eq!(delete_range, Some(SlotRange::new(0, 999)));
}

#[test]
fn local_retention_deletes_oldest_archives_for_each_kind() {
    let dir = tempfile::tempdir().expect("tempdir");
    for file in [
        "hourly_0_1000-9999.parquet",
        "hourly_0_10000-18999.parquet",
        "hourly_0_19000-27999.parquet",
        "epoch_0_0-431999.parquet",
    ] {
        fs::write(dir.path().join(file), b"archive").expect("write archive");
    }

    let to_delete = local_archives_to_delete(dir.path(), ArchiveKind::Hourly, 2)
        .expect("retention should scan");
    let names: Vec<_> = to_delete
        .iter()
        .map(|path| path.file_name().unwrap().to_string_lossy().into_owned())
        .collect();

    assert_eq!(names, vec!["hourly_0_1000-9999.parquet"]);
}

#[test]
fn local_retention_deletes_oldest_bundle_directories_for_each_kind() {
    let dir = tempfile::tempdir().expect("tempdir");
    for bundle in [
        "custom_0_10-19",
        "custom_0_20-29",
        "custom_0_30-39",
        "hourly_0_10-9009",
    ] {
        let bundle_dir = dir.path().join(bundle);
        fs::create_dir(&bundle_dir).expect("create bundle");
        fs::write(bundle_dir.join("manifest.json"), b"{}").expect("write manifest");
    }

    let to_delete = local_archives_to_delete(dir.path(), ArchiveKind::Custom { slots: 10 }, 2)
        .expect("retention should scan");
    let names: Vec<_> = to_delete
        .iter()
        .map(|path| path.file_name().unwrap().to_string_lossy().into_owned())
        .collect();

    assert_eq!(names, vec!["custom_0_10-19"]);
}

#[tokio::test]
async fn local_archive_done_marker_is_detected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let archive_id = "custom_0_10-19";
    let bundle_dir = dir.path().join(archive_id);
    fs::create_dir(&bundle_dir).expect("create bundle");
    fs::write(bundle_dir.join(".done.test-node"), b"done").expect("write done marker");
    let output_location = dir.path().to_string_lossy().into_owned();
    let config = Config::try_parse_from([
        "superbank-solparq",
        "--db-server",
        "127.0.0.1",
        "--db-user",
        "admin",
        "--db-password",
        "secret",
        "--archive-range-type",
        "custom:10",
        "--archive-file-output-location",
        &output_location,
    ])
    .expect("valid config");

    assert!(
        archive_has_done_marker(&config, ArchiveKind::Custom { slots: 10 }, archive_id)
            .await
            .expect("done marker check")
    );
}

#[tokio::test]
async fn local_sha256sums_include_all_parquet_files() {
    let dir = tempfile::tempdir().expect("tempdir");
    fs::write(dir.path().join("transactions.parquet"), b"abc").expect("write parquet");
    fs::write(dir.path().join("blocks_metadata.parquet"), b"").expect("write parquet");

    write_local_sha256sums(
        dir.path(),
        &[
            "transactions.parquet".to_string(),
            "blocks_metadata.parquet".to_string(),
        ],
    )
    .await
    .expect("write checksums");

    let contents = fs::read_to_string(dir.path().join("SHA256SUMS.txt")).expect("read checksums");
    assert!(contents.contains(
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad  transactions.parquet"
    ));
    assert!(contents.contains(
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855  blocks_metadata.parquet"
    ));
}

#[test]
fn archive_kind_from_str_parses_custom_slot_override() {
    assert_eq!(
        ArchiveKind::from_str("hourly").unwrap(),
        ArchiveKind::Hourly
    );
    assert_eq!(ArchiveKind::from_str("epoch").unwrap(), ArchiveKind::Epoch);
    assert_eq!(
        ArchiveKind::from_str("custom:2500").unwrap(),
        ArchiveKind::Custom { slots: 2_500 }
    );
}

#[test]
fn config_rejects_s3_without_required_fields() {
    let err = Config::try_parse_from([
        "superbank-solparq",
        "--db-server",
        "127.0.0.1",
        "--db-user",
        "admin",
        "--db-password",
        "secret",
        "--archive-range-type",
        "hourly",
        "--archive-location-type",
        "s3",
    ])
    .expect_err("missing S3 fields should fail");

    assert!(err.to_string().contains("archive-s3-bucket-name"), "{err}");
}

#[test]
fn config_accepts_optional_log_file() {
    let config = Config::try_parse_from([
        "superbank-solparq",
        "--db-server",
        "127.0.0.1",
        "--db-user",
        "admin",
        "--db-password",
        "secret",
        "--archive-range-type",
        "hourly",
        "--log-file",
        "solparq.log",
    ])
    .expect("valid config");

    assert_eq!(
        config.log_file.as_deref(),
        Some(std::path::Path::new("solparq.log"))
    );
}

#[test]
fn config_continues_from_last_archive_by_default_and_can_disable_it() {
    let default_config = Config::try_parse_from([
        "superbank-solparq",
        "--db-server",
        "127.0.0.1",
        "--db-user",
        "admin",
        "--db-password",
        "secret",
        "--archive-range-type",
        "hourly",
    ])
    .expect("valid config");

    let disabled_config = Config::try_parse_from([
        "superbank-solparq",
        "--db-server",
        "127.0.0.1",
        "--db-user",
        "admin",
        "--db-password",
        "secret",
        "--archive-range-type",
        "hourly",
        "--no-continue-from-last-archive",
    ])
    .expect("valid config");

    assert!(default_config.continue_from_last_archive);
    assert!(!disabled_config.continue_from_last_archive);
}

#[test]
fn utc_timestamp_formatter_is_human_readable() {
    assert_eq!(format_utc_timestamp(Some(0)), "1970-01-01 00:00:00 UTC");
    assert_eq!(format_utc_timestamp(None), "never");
}

#[test]
fn app_state_tracks_db_slots_and_archive_timeline() {
    let state = AppState::new();
    state.record_check_started(
        ArchiveKind::Custom { slots: 1_000 },
        Some(ClickHouseBounds {
            earliest_slot: 427_270_662,
            latest_slot: 427_271_661,
        }),
    );
    state.record_report(ArchiveRunReport {
        timestamp_unix: 1_700_000_000,
        archive_created: true,
        archive_skipped_reason: None,
        archive_name: Some("custom_989_427270662-427271661.parquet".to_string()),
        archive_kind: ArchiveKind::Custom { slots: 1_000 },
        archive_epoch: Some(989),
        archive_slot_start: Some(427_270_662),
        archive_slot_end: Some(427_271_661),
        db_bounds: Some(ClickHouseBounds {
            earliest_slot: 427_270_662,
            latest_slot: 427_271_661,
        }),
        destination: "./archives".to_string(),
        validation: None,
        deleted_clickhouse_range: false,
        cleaned_archives: Vec::new(),
    });

    let status = state.public_status();
    let db_slots = status.db_slots.expect("db slot status");
    assert_eq!(db_slots.earliest_slot, 427_270_662);
    assert_eq!(db_slots.latest_slot, 427_271_661);
    assert_eq!(db_slots.slots_available, 1_000);
    assert_eq!(status.recent_events.len(), 2);
    assert_eq!(status.recent_events[1].outcome, "created");
    assert_eq!(
        status.recent_events[1].archive_name.as_deref(),
        Some("custom_989_427270662-427271661.parquet")
    );
}

#[test]
fn dashboard_renders_refresh_slot_status_human_times_and_timeline() {
    let config = Config::try_parse_from([
        "superbank-solparq",
        "--db-server",
        "127.0.0.1",
        "--db-user",
        "admin",
        "--db-password",
        "secret",
        "--archive-range-type",
        "custom",
        "--server-mode",
    ])
    .expect("valid config");
    let state = AppState::new();
    state.record_check_started(
        ArchiveKind::Custom { slots: 1_000 },
        Some(ClickHouseBounds {
            earliest_slot: 10,
            latest_slot: 1_009,
        }),
    );
    state.record_report(ArchiveRunReport {
        timestamp_unix: 1_700_000_000,
        archive_created: true,
        archive_skipped_reason: None,
        archive_name: Some("custom_0_10-1009.parquet".to_string()),
        archive_kind: ArchiveKind::Custom { slots: 1_000 },
        archive_epoch: Some(0),
        archive_slot_start: Some(10),
        archive_slot_end: Some(1_009),
        db_bounds: Some(ClickHouseBounds {
            earliest_slot: 10,
            latest_slot: 1_009,
        }),
        destination: "./".to_string(),
        validation: None,
        deleted_clickhouse_range: false,
        cleaned_archives: Vec::new(),
    });

    let html = render_dashboard(&config, &state.public_status());

    assert!(html.contains("http-equiv=\"refresh\" content=\"30\""));
    assert!(html.contains("Slots available"));
    assert!(html.contains("1,000"));
    assert!(html.contains("2023-11-14 22:13:20 UTC"));
    assert!(html.contains("Archive timeline"));
    assert!(html.contains("Continue from last archive"));
    assert!(html.contains("main { max-width: 1534px"));
    assert!(html.contains(".gaps table { min-width: 988px; }"));
    assert!(html.contains("<svg"));
    assert!(html.contains("custom_0_10-1009.parquet"));
}

#[test]
fn archive_report_includes_human_timestamp_and_hostname() {
    let report = ArchiveRunReport {
        timestamp_unix: 1_700_000_000,
        archive_created: true,
        archive_skipped_reason: None,
        archive_name: Some("custom_0_10-1009.parquet".to_string()),
        archive_kind: ArchiveKind::Custom { slots: 1_000 },
        archive_epoch: Some(0),
        archive_slot_start: Some(10),
        archive_slot_end: Some(1_009),
        db_bounds: None,
        destination: "./".to_string(),
        validation: None,
        deleted_clickhouse_range: false,
        cleaned_archives: Vec::new(),
    };

    let text = report.to_text();
    assert!(text.contains("timestamp_utc: 2023-11-14 22:13:20 UTC"));
    assert!(text.contains("hostname: "));
}

#[test]
fn dashboard_renders_skip_reasons_and_known_gap_tables() {
    let config = Config::try_parse_from([
        "superbank-solparq",
        "--db-server",
        "127.0.0.1",
        "--db-user",
        "admin",
        "--db-password",
        "secret",
        "--archive-range-type",
        "custom",
        "--server-mode",
    ])
    .expect("valid config");
    let state = AppState::new();
    let validation = ValidationReport::from_observed_slots(
        100,
        110,
        vec![100, 105, 110],
        vec![100, 101, 105, 110],
        Vec::new(),
        None,
    );
    state.record_report(ArchiveRunReport {
        timestamp_unix: 1_700_000_100,
        archive_created: false,
        archive_skipped_reason: Some(
            "validation warnings require --force-archive in server mode".to_string(),
        ),
        archive_name: None,
        archive_kind: ArchiveKind::Custom { slots: 1_000 },
        archive_epoch: Some(0),
        archive_slot_start: Some(100),
        archive_slot_end: Some(110),
        db_bounds: Some(ClickHouseBounds {
            earliest_slot: 100,
            latest_slot: 110,
        }),
        destination: "./".to_string(),
        validation: Some(validation),
        deleted_clickhouse_range: false,
        cleaned_archives: Vec::new(),
    });

    let status = state.public_status();
    assert_eq!(
        status
            .recent_events
            .last()
            .and_then(|event| event.skip_reason_code.as_deref()),
        Some("data-gap")
    );
    assert_eq!(status.known_gaps.len(), 3);

    let html = render_dashboard(&config, &status);

    assert!(html.contains("Known data gaps"));
    assert!(html.contains("Needs backfill"));
    assert!(html.contains("Legit not-produced"));
    assert!(html.contains("101"));
    assert!(html.contains("102-104"));
    assert!(html.contains("skipped: data-gap"));
}
