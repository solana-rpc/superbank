use std::{fs, process::Command, str::FromStr};

use superbank_solparq::{
    archive::{
        ArchiveKind, ArchiveReportDocument, ArchiveRunMetrics, ArchiveRunReport, ArchiveRunTable,
        ArchiveSlotRange, ArchivedTableRows, ClickHouseBounds, GapBackfill, MismatchRepair,
        PhaseDuration, epoch_partitions, plan_archive_slot_range, plan_next_archive,
        safe_delete_archived_data_range,
    },
    clickhouse::{
        ArchiveTableKind, DbTables, DiskUsage, MismatchDirection, S3ArchiveSql, SlotRange,
        TableSize, TransactionMismatch, ValidationReport, build_delete_sql,
        build_local_parquet_query, build_s3_archive_sql, build_s3_table_archive_sql,
    },
    config::{ArchiveLocation, Config},
    metrics::AppState,
    server::{format_utc_timestamp, render_dashboard},
    storage::{
        archive_has_done_marker, local_archives_to_delete, remove_archive_bundle,
        write_local_sha256sums,
    },
};

#[test]
fn hourly_plan_uses_prompt_filename_convention() {
    let plan = plan_next_archive(
        ArchiveKind::Hourly,
        ClickHouseBounds {
            earliest_slot: 427_236_024,
            latest_slot: 427_245_023,
            distinct_slots: 9_000,
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
            distinct_slots: 448_000,
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
            distinct_slots: 3_000,
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
            distinct_slots: 3_000,
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
            distinct_slots: 3_501,
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
            distinct_slots: 1_501,
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
    // The version line starts with "superbank-solparq <semver>" and, when the
    // build embedded a git SHA (SUPERBANK_LONG_VERSION), is followed by
    // " (<short-sha>)". Assert the stable prefix so both forms pass.
    let stdout = String::from_utf8_lossy(&output.stdout);
    let version_line = stdout.trim();
    assert!(
        version_line.starts_with(concat!("superbank-solparq ", env!("CARGO_PKG_VERSION"))),
        "unexpected version line: {version_line}"
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
    assert!(!config.dry_run);
}

#[test]
fn config_accepts_dry_run_flag_in_one_shot_and_server_mode() {
    let one_shot = Config::try_parse_from([
        "superbank-solparq",
        "--db-server",
        "127.0.0.1",
        "--db-user",
        "admin",
        "--db-password",
        "secret",
        "--archive-range-type",
        "custom",
        "--dry-run",
    ])
    .expect("valid config");
    assert!(one_shot.dry_run);

    let server_mode = Config::try_parse_from([
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
        "--dry-run",
    ])
    .expect("valid config");
    assert!(server_mode.dry_run);
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
        settings: "max_bytes_before_external_sort=1073741824, max_threads=4",
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
    // Memory-bounding settings are appended after ORDER BY so a full-epoch
    // export does not trip the ClickHouse server memory limit.
    assert!(sql.contains(
        "ORDER BY slot, slot_idx, signature\nSETTINGS max_bytes_before_external_sort=1073741824, max_threads=4"
    ));
}

#[test]
fn s3_archive_sql_omits_settings_clause_when_unset() {
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
        settings: "  ",
    });

    assert!(
        sql.trim_end()
            .ends_with("ORDER BY slot, slot_idx, signature")
    );
    assert!(!sql.contains("SETTINGS"));
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
        settings: "max_threads=4",
    });

    assert!(sql.contains(
        "'https://s3.us-west.example/bucket/prefix/hourly/hourly_0_42-84/entries.parquet'"
    ));
    assert!(sql.contains("FROM entries"));
    assert!(sql.contains("WHERE slot BETWEEN 42 AND 84"));
    assert!(sql.contains("ORDER BY slot, entry_index\nSETTINGS max_threads=4"));
}

#[test]
fn local_parquet_query_streams_parquet_with_stable_order() {
    let sql = build_local_parquet_query(
        "default.transactions",
        1,
        9,
        "max_bytes_before_external_sort=1073741824",
    );

    assert_eq!(
        sql,
        "SELECT * FROM default.transactions WHERE slot BETWEEN 1 AND 9 ORDER BY slot, slot_idx, signature SETTINGS max_bytes_before_external_sort=1073741824 FORMAT Parquet"
    );
}

#[test]
fn local_parquet_query_omits_settings_clause_when_unset() {
    let sql = build_local_parquet_query("default.transactions", 1, 9, "");

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
fn clickhouse_cleanup_removes_slots_before_current_archive_start() {
    let config = Config::try_parse_from([
        "superbank-solparq",
        "--db-server",
        "127.0.0.1",
        "--db-user",
        "admin",
        "--db-password",
        "secret",
        "--archive-range-type",
        "epoch",
        "--server-mode",
        "--delete-archived-data-range",
    ])
    .expect("valid config");

    // A continuation archive covering the second epoch (432000-863999). Older
    // slots from the first epoch (0-431999) must still be swept even though this
    // archive does not start at slot 0.
    let delete_range = safe_delete_archived_data_range(
        &config,
        863_999,
        &[(
            ArchiveKind::Epoch,
            Some("epoch_1_432000-863999".to_string()),
        )],
    )
    .expect("safe delete check");

    assert_eq!(delete_range, Some(SlotRange::new(0, 863_999)));
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
    fs::write(bundle_dir.join(".done.test-node.txt"), b"done").expect("write done marker");
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
async fn local_archive_bundle_without_done_marker_is_removed_for_overwrite() {
    let dir = tempfile::tempdir().expect("tempdir");
    let archive_id = "custom_0_10-19";
    let bundle_dir = dir.path().join(archive_id);
    fs::create_dir(&bundle_dir).expect("create bundle");
    fs::write(bundle_dir.join("manifest.json"), b"partial").expect("write partial archive");
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

    let removed = remove_archive_bundle(&config, ArchiveKind::Custom { slots: 10 }, archive_id)
        .await
        .expect("remove bundle");

    assert!(removed);
    assert!(!bundle_dir.exists());
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
            distinct_slots: 1_000,
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
            distinct_slots: 1_000,
        }),
        destination: "./archives".to_string(),
        archive_tables: Vec::new(),
        validation: None,
        deleted_clickhouse_range: false,
        cleaned_archives: Vec::new(),
        run_metrics: Default::default(),
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
            distinct_slots: 1_000,
        }),
    );
    // Timeline shows only the last 2 hours, so use a recent timestamp.
    let recent_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs()
        - 60;
    state.record_report(ArchiveRunReport {
        timestamp_unix: recent_unix,
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
            distinct_slots: 1_000,
        }),
        destination: "./".to_string(),
        archive_tables: vec![
            ArchiveRunTable {
                kind: ArchiveTableKind::Transactions,
                table_name: "transactions".to_string(),
                required: true,
            },
            ArchiveRunTable {
                kind: ArchiveTableKind::BlocksMetadata,
                table_name: "blocks_metadata".to_string(),
                required: true,
            },
        ],
        validation: None,
        deleted_clickhouse_range: false,
        cleaned_archives: Vec::new(),
        run_metrics: Default::default(),
    });

    let html = render_dashboard(&config, &state.public_status());

    assert!(html.contains("http-equiv=\"refresh\" content=\"30\""));
    assert!(html.contains("Slots available"));
    assert!(html.contains("1,000"));
    assert!(html.contains("Archive timeline"));
    assert!(html.contains("Continue from last archive"));
    assert!(html.contains("Archive tables"));
    assert!(html.contains("<code>transactions</code>"));
    assert!(html.contains("<code>blocks_metadata</code>"));
    assert!(!html.contains("<code>entries</code>"));
    assert!(html.contains("main { max-width: 1534px"));
    assert!(html.contains(".gaps table { min-width: 988px; }"));
    assert!(html.contains("<svg"));
    assert!(html.contains("custom_0_10-1009.parquet"));
}

#[test]
fn dashboard_and_metrics_expose_clickhouse_disk_usage() {
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
    state.record_report(ArchiveRunReport {
        timestamp_unix: 1_700_000_000,
        archive_created: false,
        archive_skipped_reason: Some(
            "not enough ClickHouse slots available for the next archive".to_string(),
        ),
        archive_name: None,
        archive_kind: ArchiveKind::Custom { slots: 1_000 },
        archive_epoch: None,
        archive_slot_start: None,
        archive_slot_end: None,
        db_bounds: None,
        destination: "./".to_string(),
        archive_tables: Vec::new(),
        validation: None,
        deleted_clickhouse_range: false,
        cleaned_archives: Vec::new(),
        run_metrics: ArchiveRunMetrics {
            disk_usage: vec![DiskUsage {
                name: "default".to_string(),
                path: "/var/lib/clickhouse/".to_string(),
                free_bytes: 25 * 1024 * 1024 * 1024,
                total_bytes: 100 * 1024 * 1024 * 1024,
            }],
            ..Default::default()
        },
    });

    let status = state.public_status();
    let html = render_dashboard(&config, &status);
    assert!(html.contains("ClickHouse disk usage"));
    assert!(html.contains("<code>default</code>"));
    assert!(html.contains("<code>/var/lib/clickhouse/</code>"));
    assert!(html.contains("75.0 GB"));
    assert!(html.contains("25.0 GB"));
    assert!(html.contains("100.0 GB"));
    assert!(html.contains("75.0%"));

    let text = state.prometheus_text();
    assert!(text.contains(
        "solparq_disk_free_bytes{disk=\"default\",path=\"/var/lib/clickhouse/\"} 26843545600"
    ));
    assert!(text.contains(
        "solparq_disk_used_bytes{disk=\"default\",path=\"/var/lib/clickhouse/\"} 80530636800"
    ));
    assert!(text.contains(
        "solparq_disk_total_bytes{disk=\"default\",path=\"/var/lib/clickhouse/\"} 107374182400"
    ));
}

#[test]
fn dashboard_and_metrics_expose_db_table_sizes() {
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
    state.record_report(ArchiveRunReport {
        timestamp_unix: 1_700_000_000,
        archive_created: false,
        archive_skipped_reason: Some(
            "not enough ClickHouse slots available for the next archive".to_string(),
        ),
        archive_name: None,
        archive_kind: ArchiveKind::Custom { slots: 1_000 },
        archive_epoch: None,
        archive_slot_start: None,
        archive_slot_end: None,
        db_bounds: None,
        destination: "./".to_string(),
        archive_tables: Vec::new(),
        validation: None,
        deleted_clickhouse_range: false,
        cleaned_archives: Vec::new(),
        run_metrics: ArchiveRunMetrics {
            table_sizes: vec![
                TableSize {
                    kind: ArchiveTableKind::Transactions,
                    table_name: "transactions".to_string(),
                    bytes: 50 * 1024 * 1024 * 1024,
                    rows: 123_456_789,
                },
                TableSize {
                    kind: ArchiveTableKind::BlocksMetadata,
                    table_name: "blocks_metadata".to_string(),
                    bytes: 1024 * 1024 * 1024,
                    rows: 1_000_000,
                },
            ],
            ..Default::default()
        },
    });

    let status = state.public_status();
    let html = render_dashboard(&config, &status);
    assert!(html.contains("ClickHouse table sizes"));
    assert!(html.contains("<code>transactions</code>"));
    assert!(html.contains("<code>blocks_metadata</code>"));
    assert!(html.contains("123,456,789"));
    assert!(html.contains("50.0 GB"));
    assert!(html.contains("1.0 GB"));
    assert!(html.contains("51.0 GB"));

    let text = state.prometheus_text();
    assert!(text.contains(
        "solparq_db_table_bytes{table_kind=\"transactions\",table=\"transactions\"} 53687091200"
    ));
    assert!(text.contains(
        "solparq_db_table_rows{table_kind=\"transactions\",table=\"transactions\"} 123456789"
    ));
    assert!(text.contains(
        "solparq_db_table_bytes{table_kind=\"blocks_metadata\",table=\"blocks_metadata\"} 1073741824"
    ));
}

#[test]
fn dashboard_renders_s3_output_location() {
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
        "--archive-location-type",
        "s3",
        "--archive-s3-endpoint",
        "https://s3.example.test",
        "--archive-s3-bucket-name",
        "superbank-archives",
        "--archive-s3-bucket-path",
        "archives/prod",
        "--archive-s3-auth-key",
        "key",
        "--archive-s3-auth-secret-key",
        "secret-key",
    ])
    .expect("valid config");

    let html = render_dashboard(&config, &AppState::new().public_status());

    assert!(html.contains("<code>s3://superbank-archives/archives/prod</code>"));
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
        archive_tables: Vec::new(),
        validation: None,
        deleted_clickhouse_range: false,
        cleaned_archives: Vec::new(),
        run_metrics: Default::default(),
    };

    let text = report.to_text();
    assert!(text.contains("timestamp_utc: 2023-11-14 22:13:20 UTC"));
    assert!(text.contains("hostname: "));
}

#[test]
fn rpc_check_failure_blocks_only_without_allow_flag() {
    // getBlocks failed: rpc_check_error set, no produced slots, no data problems.
    let rpc_failed = ValidationReport::from_observed_slots(
        1_000,
        1_010,
        vec![1_000, 1_002],
        Vec::new(),
        Vec::new(),
        Some("Solana RPC getBlocks error: {\"code\":405,\"message\":\"Bad method\"}".to_string()),
    );
    assert!(rpc_failed.has_warnings());
    assert!(!rpc_failed.has_data_warnings());
    // Blocks under strict default, but not when the RPC failure is allowed.
    assert!(rpc_failed.blocks_archive(false));
    assert!(!rpc_failed.blocks_archive(true));

    // A real missing block blocks regardless of the allow flag.
    let missing_block = ValidationReport::from_observed_slots(
        1_000,
        1_010,
        vec![1_000],
        vec![1_000, 1_001],
        Vec::new(),
        None,
    );
    assert!(missing_block.has_data_warnings());
    assert!(missing_block.blocks_archive(true));
    assert!(missing_block.blocks_archive(false));
}

#[test]
fn archive_report_document_is_json_with_producer_and_run_facts() {
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
        archive_tables: Vec::new(),
        validation: None,
        deleted_clickhouse_range: false,
        cleaned_archives: Vec::new(),
        run_metrics: ArchiveRunMetrics {
            gap_backfill: Some(GapBackfill {
                slots_targeted: 2,
                missing_blocks_after: 0,
                succeeded: true,
            }),
            ..Default::default()
        },
    };

    let json = ArchiveReportDocument::new(report)
        .to_json()
        .expect("to_json");
    let value: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");

    // Manifest-style build identity.
    assert_eq!(value["report_version"], 1);
    assert_eq!(value["producer"]["name"], "superbank-solparq");
    assert!(value["producer"]["version"].is_string());
    assert!(value["producer"]["git_sha"].is_string());
    assert!(value["hostname"].is_string());
    // Timestamps: raw unix (flattened from the report) plus human UTC.
    assert_eq!(value["timestamp_unix"], 1_700_000_000u64);
    assert_eq!(value["timestamp_utc"], "2023-11-14 22:13:20 UTC");
    // Core archive facts are flattened to the top level.
    assert_eq!(value["archive_created"], true);
    assert_eq!(value["archive_name"], "custom_0_10-1009.parquet");
    // New run facts (gap backfill) are included via run_metrics.
    assert_eq!(value["run_metrics"]["gap_backfill"]["slots_targeted"], 2);
    assert_eq!(value["run_metrics"]["gap_backfill"]["succeeded"], true);
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
    // Recent timestamp so the skip event shows in the 2-hour timeline table.
    let recent_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs()
        - 60;
    state.record_report(ArchiveRunReport {
        timestamp_unix: recent_unix,
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
            distinct_slots: 11,
        }),
        destination: "./".to_string(),
        archive_tables: Vec::new(),
        validation: Some(validation),
        deleted_clickhouse_range: false,
        cleaned_archives: Vec::new(),
        run_metrics: Default::default(),
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
    // Actual gap (missing block at slot 101) is shown; leader-skipped
    // ("Legit not-produced") ranges like 102-104 are filtered out.
    assert!(html.contains("101"));
    assert!(!html.contains("Legit not-produced"));
    assert!(!html.contains("102-104"));
    assert!(html.contains("skipped: data-gap"));
}

#[test]
fn dashboard_renders_gap_repairs_section_with_outcomes() {
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
    state.record_report(ArchiveRunReport {
        timestamp_unix: 1_700_000_000,
        archive_created: true,
        archive_skipped_reason: None,
        archive_name: Some("custom_0_100-110.parquet".to_string()),
        archive_kind: ArchiveKind::Custom { slots: 1_000 },
        archive_epoch: Some(0),
        archive_slot_start: Some(100),
        archive_slot_end: Some(110),
        db_bounds: None,
        destination: "./".to_string(),
        archive_tables: Vec::new(),
        validation: None,
        deleted_clickhouse_range: false,
        cleaned_archives: Vec::new(),
        run_metrics: ArchiveRunMetrics {
            // Backfill fully repaired -> success.
            gap_backfill: Some(GapBackfill {
                slots_targeted: 3,
                missing_blocks_after: 0,
                succeeded: true,
            }),
            // Dedup left overcounts behind -> failed.
            mismatch_repair: Some(MismatchRepair {
                partitions_optimized: 1,
                overcount_slots_before: 2,
                overcount_slots_after: 2,
            }),
            ..Default::default()
        },
    });

    let html = render_dashboard(&config, &state.public_status());

    assert!(html.contains("Gap repairs"));
    assert!(html.contains("RPC backfill"));
    assert!(html.contains("Dedup repair"));
    // Distinct outcomes render as success/failed badges.
    assert!(html.contains(">success<"));
    assert!(html.contains(">failed<"));
    assert!(html.contains("100-110"));
}

#[test]
fn dry_run_report_is_classified_as_dry_run_and_does_not_inflate_row_counters() {
    let state = AppState::new();
    state.record_report(ArchiveRunReport {
        timestamp_unix: 1_700_000_200,
        archive_created: false,
        archive_skipped_reason: Some(
            "dry-run: archive would be created; no files written and no ClickHouse changes made"
                .to_string(),
        ),
        archive_name: Some("custom_0_10-1009.parquet".to_string()),
        archive_kind: ArchiveKind::Custom { slots: 1_000 },
        archive_epoch: Some(0),
        archive_slot_start: Some(10),
        archive_slot_end: Some(1_009),
        db_bounds: Some(ClickHouseBounds {
            earliest_slot: 10,
            latest_slot: 1_009,
            distinct_slots: 1_000,
        }),
        destination: "./".to_string(),
        archive_tables: Vec::new(),
        validation: None,
        deleted_clickhouse_range: false,
        cleaned_archives: Vec::new(),
        run_metrics: ArchiveRunMetrics {
            archived_table_rows: vec![ArchivedTableRows {
                table: "transactions".to_string(),
                rows: 1_000,
            }],
            ..Default::default()
        },
    });

    let status = state.public_status();
    assert_eq!(
        status
            .recent_events
            .last()
            .and_then(|event| event.skip_reason_code.as_deref()),
        Some("dry-run")
    );

    let text = state.prometheus_text();
    assert!(
        text.contains(
            r#"solparq_archives_skipped_total{archive_kind="custom",reason="dry-run"} 1"#
        )
    );
    // The dry-run row count must not create a data point for this archive_kind/table
    // pair on either the cumulative counter or the last-archive gauge; only a real
    // archive creation may do that.
    assert!(!text.contains(r#"archive_kind="custom",table="transactions""#));
    assert!(!text.contains(r#"solparq_last_archive_rows{archive_kind="custom"}"#));
}

#[test]
fn build_info_is_exposed_on_metrics_and_status() {
    let state = AppState::new();

    // The build identity is available to the ops dashboard / status JSON.
    let status = state.public_status();
    assert_eq!(status.build.name, env!("CARGO_PKG_NAME"));
    assert_eq!(status.build.version, env!("CARGO_PKG_VERSION"));
    assert!(!status.build.git_sha.is_empty());

    // ...and exposed as a constant-1 build_info gauge for Grafana. Labels are
    // emitted in field-declaration order: name, version, git_sha.
    let text = state.prometheus_text();
    let expected = format!(
        r#"solparq_build_info{{name="{}",version="{}",git_sha="{}"}} 1"#,
        status.build.name, status.build.version, status.build.git_sha
    );
    assert!(
        text.contains(&expected),
        "missing build_info series:\n{text}"
    );
}

#[test]
fn dashboard_shows_binary_version() {
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
    let status = state.public_status();
    let html = render_dashboard(&config, &status);

    assert!(html.contains(&format!("v{}", env!("CARGO_PKG_VERSION"))));
    assert!(html.contains(&status.build.git_sha));
}

#[test]
fn metrics_endpoint_exposes_labeled_series() {
    let state = AppState::new();
    state.set_check_interval_secs(60);
    state.set_archive_in_flight(ArchiveKind::Epoch, true);
    // Produced 1000,1001,1002,1005,1006 but ClickHouse only has 1000,1002,1005,1006:
    // slot 1001 is an actual gap (missing block). Slots the RPC did not produce in
    // 1000-1010 are legit not-produced. Slot 1002 has a transaction-count mismatch.
    let validation = ValidationReport::from_observed_slots(
        1_000,
        1_010,
        vec![1_000, 1_002, 1_005, 1_006],
        vec![1_000, 1_001, 1_002, 1_005, 1_006],
        vec![TransactionMismatch {
            slot: 1_002,
            expected: 5,
            actual: 4,
        }],
        None,
    );
    state.record_report(ArchiveRunReport {
        timestamp_unix: 1_700_000_000,
        archive_created: true,
        archive_skipped_reason: None,
        archive_name: Some("epoch_1000_432000000-432431999.parquet".to_string()),
        archive_kind: ArchiveKind::Epoch,
        archive_epoch: Some(1_000),
        archive_slot_start: Some(432_000_000),
        archive_slot_end: Some(432_431_999),
        db_bounds: Some(ClickHouseBounds {
            earliest_slot: 432_000_000,
            latest_slot: 432_432_049,
            distinct_slots: 432_050,
        }),
        destination: "./archives".to_string(),
        archive_tables: Vec::new(),
        validation: Some(validation),
        deleted_clickhouse_range: true,
        cleaned_archives: vec!["epoch_999_431568000-431999999.parquet".to_string()],
        run_metrics: ArchiveRunMetrics {
            phase_durations: vec![
                PhaseDuration {
                    phase: "validate".to_string(),
                    seconds: 1.0,
                },
                PhaseDuration {
                    phase: "write".to_string(),
                    seconds: 2.0,
                },
            ],
            archived_table_rows: vec![ArchivedTableRows {
                table: "transactions".to_string(),
                rows: 12_345,
            }],
            archived_bytes_total: Some(4_096),
            chain_tip_slot: Some(432_432_099),
            mismatch_repair: None,
            gap_backfill: None,
            disk_usage: Vec::new(),
            table_sizes: Vec::new(),
        },
    });

    let text = state.prometheus_text();

    // Global gauges.
    assert!(text.contains("solparq_check_interval_seconds 60"));
    assert!(text.contains("solparq_db_latest_slot 432432049"));
    assert!(text.contains("solparq_chain_tip_slot 432432099"));
    // chain_tip_lag = tip (432432099) - db_latest (432432049) = 50.
    assert!(text.contains("solparq_chain_tip_lag_slots 50"));
    // Per-kind labelled series.
    assert!(text.contains("solparq_archive_in_flight{archive_kind=\"epoch\"} 1"));
    assert!(text.contains("solparq_last_archive_bytes{archive_kind=\"epoch\"} 4096"));
    assert!(text.contains("solparq_last_archived_end_slot{archive_kind=\"epoch\"} 432431999"));
    // db_lag = db_latest (432432049) - last archived end (432431999) = 50.
    assert!(text.contains("solparq_db_lag_slots{archive_kind=\"epoch\"} 50"));
    // Counters (suffix handling left to the encoder; assert base name + labels).
    assert!(text.contains("solparq_archives_created") && text.contains("archive_kind=\"epoch\""));
    assert!(text.contains("solparq_clickhouse_range_deleted"));
    assert!(text.contains("solparq_archives_cleaned"));
    assert!(text.contains("solparq_archive_rows") && text.contains("table=\"transactions\""));
    // Phase-duration histogram.
    assert!(text.contains("solparq_phase_duration_seconds_bucket"));
    assert!(text.contains("phase=\"validate\""));
    // Categorized validation metrics: actual gap vs legit not-produced vs mismatch.
    assert!(
        text.contains(
            "solparq_validation_slots{archive_kind=\"epoch\",category=\"missing_block\"} 1"
        )
    );
    assert!(text.contains(
        "solparq_validation_slots{archive_kind=\"epoch\",category=\"transaction_mismatch\"} 1"
    ));
    assert!(text.contains("category=\"not_produced\""));
    assert!(text.contains(
        "solparq_validation_ranges{archive_kind=\"epoch\",category=\"missing_block\"} 1"
    ));
    assert!(text.contains("solparq_validation_range_start_slot{archive_kind=\"epoch\"} 1000"));
    assert!(text.contains("solparq_validation_range_end_slot{archive_kind=\"epoch\"} 1010"));
    assert!(text.contains("solparq_validation_db_block_slots{archive_kind=\"epoch\"} 4"));
    assert!(text.contains("solparq_validation_rpc_produced_slots{archive_kind=\"epoch\"} 5"));
    // Cumulative counter across runs, split by category. The encoder appends the
    // single `_total` suffix (no `_total_total` double suffix).
    assert!(text.contains("solparq_validation_flagged_slots_total"));
    assert!(text.contains(
        "solparq_validation_flagged_slots_total{archive_kind=\"epoch\",category=\"transaction_mismatch\"} 1"
    ));
    assert!(!text.contains("_total_total"));
    // Existing counters keep the single-suffix idiomatic name.
    assert!(text.contains("solparq_archives_created_total{archive_kind=\"epoch\"} 1"));
}

#[test]
fn last_error_persists_across_skips_until_archive_created() {
    let report = |archive_created: bool, reason: Option<&str>| ArchiveRunReport {
        timestamp_unix: 1_700_000_000,
        archive_created,
        archive_skipped_reason: reason.map(ToString::to_string),
        archive_name: archive_created.then(|| "epoch_1_0-431999.parquet".to_string()),
        archive_kind: ArchiveKind::Epoch,
        archive_epoch: Some(1),
        archive_slot_start: Some(0),
        archive_slot_end: Some(431_999),
        db_bounds: None,
        destination: "./archives".to_string(),
        archive_tables: Vec::new(),
        validation: None,
        deleted_clickhouse_range: false,
        cleaned_archives: Vec::new(),
        run_metrics: Default::default(),
    };

    let state = AppState::new();
    // A hard error marks the service unhealthy and records the error.
    state.record_task_error(ArchiveKind::Epoch, "clickhouse unavailable".to_string());
    let status = state.public_status();
    assert!(!status.healthy);
    assert_eq!(status.last_error.as_deref(), Some("clickhouse unavailable"));
    assert!(status.last_error_at_unix.is_some());

    // A routine skipped check restores health but must NOT wipe the last error.
    state.record_report(report(
        false,
        Some("not enough ClickHouse slots available for the next archive"),
    ));
    let status = state.public_status();
    assert!(status.healthy, "a successful check should restore health");
    assert_eq!(
        status.last_error.as_deref(),
        Some("clickhouse unavailable"),
        "a skipped check must not clear the last error"
    );
    assert!(status.last_error_at_unix.is_some());

    // A genuine archive creation clears the last error.
    state.record_report(report(true, None));
    let status = state.public_status();
    assert!(status.healthy);
    assert_eq!(status.last_error, None);
    assert!(status.last_error_at_unix.is_none());
}

#[test]
fn transaction_mismatch_direction_and_delta() {
    let under = TransactionMismatch {
        slot: 1,
        expected: 10,
        actual: 8,
    };
    let over = TransactionMismatch {
        slot: 2,
        expected: 5,
        actual: 7,
    };
    assert_eq!(under.direction(), MismatchDirection::Undercount);
    assert_eq!(under.delta(), -2);
    assert_eq!(over.direction(), MismatchDirection::Overcount);
    assert_eq!(over.delta(), 2);
}

#[test]
fn validation_splits_transaction_mismatches_by_direction() {
    let mismatches = vec![
        TransactionMismatch {
            slot: 100,
            expected: 10,
            actual: 8,
        },
        TransactionMismatch {
            slot: 101,
            expected: 10,
            actual: 9,
        },
        TransactionMismatch {
            slot: 200,
            expected: 5,
            actual: 7,
        },
    ];
    let report = ValidationReport::from_observed_slots(
        100,
        200,
        vec![100, 101, 200],
        vec![100, 101, 200],
        mismatches,
        None,
    );

    // Contiguous undercount slots collapse into one range; the overcount is separate.
    assert_eq!(report.transaction_undercount_ranges.len(), 1);
    assert_eq!(report.transaction_undercount_ranges[0].start_slot, 100);
    assert_eq!(report.transaction_undercount_ranges[0].end_slot, 101);
    assert_eq!(report.transaction_overcount_ranges.len(), 1);
    assert_eq!(report.transaction_overcount_ranges[0].start_slot, 200);
    assert_eq!(report.transaction_overcount_ranges[0].end_slot, 200);
    // The combined range set still covers all mismatched slots.
    assert_eq!(report.transaction_mismatch_ranges.len(), 2);
}

#[test]
fn epoch_partitions_cover_all_epochs_touched_by_ranges() {
    let ranges = vec![
        SlotRange::new(0, 10),
        SlotRange::new(431_999, 432_002),
        SlotRange::new(864_000, 864_000),
    ];
    // Epoch = intDiv(slot, 432000): epoch 0, boundary 0->1, and epoch 2.
    assert_eq!(epoch_partitions(&ranges), vec![0, 1, 2]);
}

#[test]
fn metrics_expose_mismatch_direction_and_repair_outcome() {
    let state = AppState::new();
    let validation = ValidationReport::from_observed_slots(
        100,
        105,
        vec![100, 101, 104],
        vec![100, 101, 104],
        vec![
            TransactionMismatch {
                slot: 100,
                expected: 10,
                actual: 8,
            },
            TransactionMismatch {
                slot: 104,
                expected: 5,
                actual: 6,
            },
        ],
        None,
    );
    state.record_report(ArchiveRunReport {
        timestamp_unix: 1_700_000_000,
        archive_created: true,
        archive_skipped_reason: None,
        archive_name: Some("epoch_1_0-431999.parquet".to_string()),
        archive_kind: ArchiveKind::Epoch,
        archive_epoch: Some(1),
        archive_slot_start: Some(0),
        archive_slot_end: Some(431_999),
        db_bounds: None,
        destination: "./archives".to_string(),
        archive_tables: Vec::new(),
        validation: Some(validation),
        deleted_clickhouse_range: false,
        cleaned_archives: Vec::new(),
        run_metrics: ArchiveRunMetrics {
            mismatch_repair: Some(MismatchRepair {
                partitions_optimized: 1,
                overcount_slots_before: 1,
                overcount_slots_after: 0,
            }),
            ..Default::default()
        },
    });

    let text = state.prometheus_text();
    assert!(text.contains(
        "solparq_validation_mismatch_slots{archive_kind=\"epoch\",direction=\"undercount\"} 1"
    ));
    assert!(text.contains(
        "solparq_validation_mismatch_slots{archive_kind=\"epoch\",direction=\"overcount\"} 1"
    ));
    // Overcount cleared after dedup -> repaired.
    assert!(
        text.contains(
            "solparq_mismatch_repairs_total{archive_kind=\"epoch\",outcome=\"repaired\"} 1"
        )
    );
    // Direction-split known gaps.
    assert!(
        state
            .public_status()
            .known_gaps
            .iter()
            .any(|gap| gap.classification == "Transaction mismatch (undercount)")
    );
}

#[test]
fn metrics_expose_gap_backfill_outcome() {
    let state = AppState::new();
    state.record_report(ArchiveRunReport {
        timestamp_unix: 1_700_000_000,
        archive_created: true,
        archive_skipped_reason: None,
        archive_name: Some("epoch_1_0-431999.parquet".to_string()),
        archive_kind: ArchiveKind::Epoch,
        archive_epoch: Some(1),
        archive_slot_start: Some(0),
        archive_slot_end: Some(431_999),
        db_bounds: None,
        destination: "./archives".to_string(),
        archive_tables: Vec::new(),
        validation: None,
        deleted_clickhouse_range: false,
        cleaned_archives: Vec::new(),
        run_metrics: ArchiveRunMetrics {
            gap_backfill: Some(GapBackfill {
                slots_targeted: 3,
                missing_blocks_after: 1,
                succeeded: true,
            }),
            ..Default::default()
        },
    });

    let text = state.prometheus_text();
    // Subprocess succeeded but a block is still missing -> partial.
    assert!(
        text.contains("solparq_gap_backfills_total{archive_kind=\"epoch\",outcome=\"partial\"} 1")
    );
}
