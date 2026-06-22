use std::{fs, str::FromStr};

use solparq::{
    archive::{ArchiveKind, ArchiveRunReport, ClickHouseBounds, plan_next_archive},
    clickhouse::{S3ArchiveSql, build_delete_sql, build_local_parquet_query, build_s3_archive_sql},
    config::{ArchiveLocation, Config},
    metrics::AppState,
    server::{format_utc_timestamp, render_dashboard},
    storage::local_archives_to_delete,
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
    )
    .expect("plan should succeed")
    .expect("enough slots are available");

    assert_eq!(plan.kind, kind);
    assert_eq!(plan.start_slot, 11_000);
    assert_eq!(plan.end_slot, 11_999);
    assert_eq!(plan.file_name(), "custom_0_11000-11999.parquet");
}

#[test]
fn config_rejects_multiple_archive_types_outside_server_mode() {
    let err = Config::try_parse_from([
        "solparq",
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
fn config_applies_required_defaults_and_s3_validation() {
    let config = Config::try_parse_from([
        "solparq",
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
    assert_eq!(config.gsfa_table, "gsfa");
    assert_eq!(config.signatures_table, "signatures");
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
fn s3_archive_sql_uses_clickhouse_s3_function_and_stable_order() {
    let sql = build_s3_archive_sql(S3ArchiveSql {
        transactions_table: "transactions",
        start_slot: 42,
        end_slot: 84,
        endpoint: "https://s3.us-west.example",
        bucket: "bucket",
        bucket_path: "prefix/hourly",
        archive_name: "hourly_0_42-84.parquet",
        access_key: "access",
        secret_key: "secret",
    });

    assert!(sql.contains("INSERT INTO FUNCTION s3("));
    assert!(
        sql.contains("'https://s3.us-west.example/bucket/prefix/hourly/hourly_0_42-84.parquet'")
    );
    assert!(sql.contains("'access'"));
    assert!(sql.contains("'secret'"));
    assert!(sql.contains("'Parquet'"));
    assert!(sql.contains("FROM transactions"));
    assert!(sql.contains("WHERE slot BETWEEN 42 AND 84"));
    assert!(sql.contains("ORDER BY slot, slot_idx, signature"));
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
    let statements = build_delete_sql(
        "transactions",
        "blocks_metadata",
        "gsfa",
        "signatures",
        100,
        123,
    );

    assert_eq!(
        statements,
        vec![
            "ALTER TABLE transactions DELETE WHERE slot BETWEEN 100 AND 123".to_string(),
            "ALTER TABLE blocks_metadata DELETE WHERE slot BETWEEN 100 AND 123".to_string(),
            "ALTER TABLE gsfa DELETE WHERE slot BETWEEN 100 AND 123".to_string(),
            "ALTER TABLE signatures DELETE WHERE slot BETWEEN 100 AND 123".to_string(),
        ]
    );
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
        "solparq",
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
        "solparq",
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
        "solparq",
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
    assert!(html.contains("<svg"));
    assert!(html.contains("custom_0_10-1009.parquet"));
}
