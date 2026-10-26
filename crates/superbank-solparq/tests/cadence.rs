use std::{fs, process::Command};

use superbank_solparq::{
    archive::{
        ArchiveKind, ArchivePlan, ArchiveSlotRange, ClickHouseBounds, EPOCH_SLOTS, HOURLY_SLOTS,
        hourly_slot_count, plan_archive_slot_range, plan_next_archive,
        plan_next_archive_with_hourly_slot_duration, safe_delete_archived_data_range,
    },
    clickhouse::SlotRange,
    config::Config,
    storage::{latest_archive_name, local_archives_to_delete},
};

fn config(extra: &[&str]) -> anyhow::Result<Config> {
    let mut args = vec![
        "superbank-solparq",
        "--db-server",
        "127.0.0.1",
        "--db-user",
        "default",
        "--db-password",
        "",
        "--archive-range-type",
        "hourly",
    ];
    args.extend_from_slice(extra);
    Config::try_parse_from(args)
}

#[test]
fn default_cadence_preserves_historical_hourly_plan() {
    let config = config(&[]).expect("default config");
    assert_eq!(config.hourly_slot_duration_ms, 400);
    assert_eq!(
        hourly_slot_count(config.hourly_slot_duration_ms).unwrap(),
        HOURLY_SLOTS
    );
    let bounds = ClickHouseBounds {
        earliest_slot: 427_236_024,
        latest_slot: 427_245_023,
        distinct_slots: 9_000,
    };
    let historical = plan_next_archive(ArchiveKind::Hourly, bounds, None, true, false)
        .expect("historical planning");
    let configured = plan_next_archive_with_hourly_slot_duration(
        ArchiveKind::Hourly,
        bounds,
        None,
        true,
        false,
        config.hourly_slot_duration_ms,
    )
    .expect("configured planning");
    assert_eq!(configured, historical);
    assert_eq!(
        configured.unwrap().file_name(),
        "hourly_988_427236024-427245023.parquet"
    );
}

#[test]
fn two_hundred_ms_hour_waits_for_inclusive_end_despite_skipped_slots() {
    let config = config(&["--hourly-slot-duration-ms", "200"]).expect("200 ms config");
    let mut bounds = ClickHouseBounds {
        earliest_slot: 427_236_024,
        latest_slot: 427_254_022,
        distinct_slots: 17_000,
    };
    let plan = |bounds| {
        plan_next_archive_with_hourly_slot_duration(
            ArchiveKind::Hourly,
            bounds,
            None,
            true,
            false,
            config.hourly_slot_duration_ms,
        )
        .expect("hourly planning")
    };
    assert_eq!(plan(bounds), None, "one slot short of a nominal hour");
    bounds.latest_slot += 1;
    let plan = plan(bounds).expect("inclusive end is now available");
    assert_eq!(plan.end_slot - plan.start_slot + 1, 18_000);
    assert_eq!(
        plan.epoch, 988,
        "epoch numbering stays independent of cadence"
    );
    assert_eq!(plan.file_name(), "hourly_988_427236024-427254023.parquet");
}

#[test]
fn cadence_changes_continue_from_recorded_end_without_gaps_or_overlap() {
    let bounds = ClickHouseBounds {
        earliest_slot: 1_000,
        latest_slot: 100_000,
        distinct_slots: 99_001,
    };
    let faster = plan_next_archive_with_hourly_slot_duration(
        ArchiveKind::Hourly,
        bounds,
        Some("hourly_0_1000-9999.parquet"),
        true,
        false,
        200,
    )
    .unwrap()
    .unwrap();
    assert_eq!((faster.start_slot, faster.end_slot), (10_000, 27_999));
    let historical = plan_next_archive_with_hourly_slot_duration(
        ArchiveKind::Hourly,
        bounds,
        Some(&faster.archive_id()),
        true,
        false,
        400,
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        (historical.start_slot, historical.end_slot),
        (28_000, 36_999)
    );
}

#[test]
fn hourly_cadence_does_not_change_epoch_or_custom_planning() {
    let bounds = ClickHouseBounds {
        earliest_slot: 1_000,
        latest_slot: 2 * EPOCH_SLOTS - 1,
        distinct_slots: 800_000,
    };
    for kind in [ArchiveKind::Epoch, ArchiveKind::Custom { slots: 1_000 }] {
        assert_eq!(
            plan_next_archive_with_hourly_slot_duration(kind, bounds, None, true, false, 200)
                .unwrap(),
            plan_next_archive(kind, bounds, None, true, false).unwrap(),
        );
    }
}

#[test]
fn explicit_hourly_range_keeps_operator_boundaries_at_faster_cadence() {
    let config = config(&[
        "--hourly-slot-duration-ms",
        "200",
        "--archive-slot-range",
        "1000-3222",
    ])
    .unwrap();
    assert_eq!(
        config.archive_slot_range,
        Some(ArchiveSlotRange::new(1_000, 3_222).unwrap())
    );
    let plan = plan_archive_slot_range(
        config.archive_kinds[0],
        ClickHouseBounds {
            earliest_slot: 1_000,
            latest_slot: 3_222,
            distinct_slots: 2_223,
        },
        config.archive_slot_range.unwrap(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(plan.file_name(), "hourly_0_1000-3222.parquet");
}

#[test]
fn cadence_env_is_validated_and_cli_takes_precedence() {
    // Child processes avoid mutating this test process's shared environment.
    // An invalid table name stops after cadence validation, before network I/O.
    for (extra, expected_error) in [
        (vec![], "hourly-slot-duration-ms must be positive"),
        (
            vec!["--hourly-slot-duration-ms", "200"],
            "invalid ClickHouse table name",
        ),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_superbank-solparq"))
            .env("SOLPARQ_HOURLY_SLOT_DURATION_MS", "0")
            .args([
                "--db-server",
                "127.0.0.1",
                "--db-user",
                "default",
                "--db-password",
                "",
                "--archive-range-type",
                "hourly",
                "--db-transactions-table-name",
                "invalid-name",
            ])
            .args(extra)
            .output()
            .expect("run archiver config validation");
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains(expected_error));
    }
}

#[test]
fn config_validates_exact_positive_hourly_cadence() {
    for duration in ["0", "333", "3600001", "18446744073709551615"] {
        let err = config(&["--hourly-slot-duration-ms", duration]).expect_err("invalid cadence");
        assert!(
            err.to_string()
                .contains("hourly-slot-duration-ms must be positive and divide"),
            "{err}"
        );
    }
    for duration in ["1", "200", "400", "3600000"] {
        config(&["--hourly-slot-duration-ms", duration]).expect("exact one-hour divisor");
    }
    for duration in ["-200", "garbage"] {
        assert!(config(&["--hourly-slot-duration-ms", duration]).is_err());
    }
}

#[test]
fn configured_hourly_end_detects_slot_overflow() {
    let err = plan_next_archive_with_hourly_slot_duration(
        ArchiveKind::Hourly,
        ClickHouseBounds {
            earliest_slot: u64::MAX - 100,
            latest_slot: u64::MAX,
            distinct_slots: 101,
        },
        None,
        true,
        false,
        200,
    )
    .expect_err("end cannot wrap around");
    assert!(err.to_string().contains("archive end slot overflowed"));
}

#[tokio::test]
async fn mixed_cadence_history_shares_checkpoint_and_count_retention() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("hourly_0_1000-9999.parquet"), b"legacy").unwrap();
    for name in [
        "hourly_0_10000-27999",
        "hourly_0_28000-45999",
        "epoch_0_0-431999",
    ] {
        fs::create_dir(dir.path().join(name)).unwrap();
    }
    let mut config = config(&["--hourly-slot-duration-ms", "200"]).unwrap();
    config.output_location = dir.path().to_owned();
    assert_eq!(
        latest_archive_name(&config, ArchiveKind::Hourly)
            .await
            .unwrap()
            .as_deref(),
        Some("hourly_0_28000-45999")
    );
    let deleted = local_archives_to_delete(dir.path(), ArchiveKind::Hourly, 2).unwrap();
    assert_eq!(deleted, vec![dir.path().join("hourly_0_1000-9999.parquet")]);
    assert!(
        local_archives_to_delete(dir.path(), ArchiveKind::Hourly, 0)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn faster_hourly_cleanup_still_waits_for_every_kind_and_preserves_prefix() {
    let config = config(&[
        "--hourly-slot-duration-ms",
        "200",
        "--archive-range-type",
        "custom:500",
        "--server-mode",
        "--delete-archived-data-range",
    ])
    .unwrap();
    let hourly = (ArchiveKind::Hourly, Some("hourly_0_1000-18999".to_owned()));
    assert_eq!(
        safe_delete_archived_data_range(
            &config,
            18_999,
            &[hourly.clone(), (ArchiveKind::Custom { slots: 500 }, None)]
        )
        .unwrap(),
        None
    );
    let custom = (
        ArchiveKind::Custom { slots: 500 },
        Some("custom_0_1000-1499".to_owned()),
    );
    assert_eq!(
        safe_delete_archived_data_range(&config, 18_999, &[hourly, custom]).unwrap(),
        Some(SlotRange::new(1_000, 1_499))
    );
}

#[test]
fn hourly_names_keep_reader_compatibility_at_both_cadences() {
    for slots in [9_000, 18_000] {
        let plan = ArchivePlan {
            kind: ArchiveKind::Hourly,
            epoch: 0,
            start_slot: 1_000,
            end_slot: 1_000 + slots - 1,
        };
        let parsed =
            superbank_solparq::read::archive_name::parse_archive_name(&plan.file_name()).unwrap();
        assert_eq!(parsed.kind_label, "hourly");
        assert_eq!(
            (parsed.start_slot, parsed.end_slot),
            (plan.start_slot, plan.end_slot)
        );
    }
}
