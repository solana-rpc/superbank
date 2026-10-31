use superbank_solparq::{
    archive::{ArchiveKind, ArchiveReportDocument, ArchiveRunReport},
    clickhouse::{FooterCheck, ValidationReport, build_footer_gap_query},
    config::{Config, FooterCoveragePolicy},
    metrics::AppState,
};

/// Mainnet `getAgGenesisCert` `block.slot`: the last block before Alpenglow consensus.
const MAINNET_GENESIS_SLOT: u64 = 459_652_999;

fn footer_check(required: bool) -> FooterCheck {
    FooterCheck {
        genesis_slot: Some(MAINNET_GENESIS_SLOT),
        required,
    }
}

fn clean_report() -> ValidationReport {
    ValidationReport::from_observed_slots(
        459_652_990,
        459_653_010,
        vec![459_652_990, 459_653_001, 459_653_002],
        vec![459_652_990, 459_653_001, 459_653_002],
        Vec::new(),
        None,
    )
}

fn parse_config(extra: &[&str]) -> anyhow::Result<Config> {
    let mut args = vec![
        "superbank-solparq",
        "--db-server",
        "127.0.0.1",
        "--db-user",
        "admin",
        "--db-password",
        "secret",
        "--archive-range-type",
        "custom",
    ];
    args.extend_from_slice(extra);
    Config::try_parse_from(args)
}

#[test]
fn footer_check_only_covers_slots_after_genesis() {
    let check = footer_check(false);
    // Pre-Alpenglow blocks never carry a footer, so a range ending at G has nothing to check.
    assert_eq!(check.checked_range(459_648_000, MAINNET_GENESIS_SLOT), None);
    assert_eq!(
        check.checked_range(459_648_000, 459_660_000),
        Some((459_653_000, 459_660_000))
    );
    assert_eq!(
        check.checked_range(459_653_000, 459_660_000),
        Some((459_653_000, 459_660_000))
    );
    assert_eq!(
        check.checked_range(459_700_000, 459_709_999),
        Some((459_700_000, 459_709_999))
    );
    assert_eq!(
        check.checked_range(459_653_000, 459_653_000),
        Some((459_653_000, 459_653_000))
    );
}

#[test]
fn footer_check_is_off_without_a_genesis_slot() {
    let check = FooterCheck {
        genesis_slot: None,
        required: true,
    };
    assert_eq!(check.checked_range(0, u64::MAX), None);
    let saturated = FooterCheck {
        genesis_slot: Some(u64::MAX),
        required: true,
    };
    assert_eq!(saturated.checked_range(0, u64::MAX), None);
}

#[test]
fn footer_gap_query_flags_missing_and_partial_footers() {
    let sql = build_footer_gap_query("default.blocks_metadata", 459_653_000, 459_660_000);
    // FINAL reads the row a replay left last, so an overwritten NULL footer counts as a gap.
    assert!(sql.contains("FROM default.blocks_metadata FINAL"));
    assert!(sql.contains("slot BETWEEN 459653000 AND 459660000"));
    // A footer missing any field is incomplete, matching how getBlock treats it.
    for column in ["bank_hash", "block_producer_time_nanos", "block_user_agent"] {
        assert!(
            sql.contains(&format!("{column} IS NULL")),
            "{column}: {sql}"
        );
    }
    assert!(sql.contains(" OR "));
}

#[test]
fn footer_gaps_are_reported_but_do_not_block_under_report_policy() {
    let report = clean_report().with_footer_gaps(&[459_653_001, 459_653_002, 459_653_007], false);
    assert_eq!(report.footer_missing_ranges.len(), 2);
    assert_eq!(report.footer_missing_ranges[0].start_slot, 459_653_001);
    assert_eq!(report.footer_missing_ranges[0].end_slot, 459_653_002);
    assert_eq!(report.footer_missing_slots(), 3);
    // An upgrade with the default policy must not stop archiving.
    assert!(!report.has_data_warnings());
    assert!(!report.blocks_archive(false));
}

#[test]
fn footer_gaps_block_the_archive_under_require_policy() {
    let report = clean_report().with_footer_gaps(&[459_653_001], true);
    // A post-genesis block without a footer is a data problem, so the RPC escape hatch does not help.
    assert!(report.has_data_warnings());
    assert!(report.blocks_archive(true));
    assert!(report.blocks_archive(false));

    let complete = clean_report().with_footer_gaps(&[], true);
    assert!(!complete.has_data_warnings());
    assert!(!complete.blocks_archive(false));
}

#[test]
fn config_parses_footer_settings_and_rejects_require_without_genesis() {
    let config = parse_config(&[]).expect("defaults");
    assert_eq!(config.alpenglow_genesis_slot, None);
    assert_eq!(config.footer_coverage_policy, FooterCoveragePolicy::Report);
    assert_eq!(FooterCheck::from_config(&config), FooterCheck::default());

    let config = parse_config(&[
        "--alpenglow-genesis-slot",
        "459652999",
        "--footer-coverage-policy",
        "require",
    ])
    .expect("require with genesis");
    assert_eq!(FooterCheck::from_config(&config), footer_check(true));

    // Requiring footers with no boundary would silently check nothing.
    let err = parse_config(&["--footer-coverage-policy", "require"]).unwrap_err();
    assert!(
        err.to_string().contains("--alpenglow-genesis-slot"),
        "{err}"
    );
}

#[test]
fn footer_gaps_reach_metrics_report_json_and_text() {
    let validation = clean_report().with_footer_gaps(&[459_653_001, 459_653_002], false);
    let mut report = ArchiveRunReport::skipped(ArchiveKind::Hourly, "local".to_string(), "test");
    report.validation = Some(validation);

    let text = report.to_text();
    assert!(text.contains("validation_footer_missing_ranges: 459653001-459653002"));

    let json: serde_json::Value = serde_json::from_str(
        &ArchiveReportDocument::new(report.clone())
            .to_json()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(json["report_version"], 2);
    assert_eq!(
        json["validation"]["footer_missing_ranges"][0]["slot_count"],
        2
    );
    assert_eq!(json["validation"]["footer_coverage_required"], false);

    let state = AppState::new();
    state.record_report(report);
    let metrics = state.prometheus_text();
    assert!(
        metrics.contains(
            "solparq_validation_slots{archive_kind=\"hourly\",category=\"footer_missing\"} 2"
        ),
        "{metrics}"
    );
    assert!(metrics.contains(
        "solparq_validation_ranges{archive_kind=\"hourly\",category=\"footer_missing\"} 1"
    ));
}
