use std::collections::BTreeMap;

use superbank_solparq::{
    archive::{ArchiveKind, ArchiveReportDocument, ArchiveRunReport},
    clickhouse::ValidationReport,
    config::{Config, PohVerifyMode},
    metrics::AppState,
    poh::PohReport,
};
use superbank_verify::SlotRangeVerification;

fn clean_report() -> ValidationReport {
    ValidationReport::from_observed_slots(
        100,
        110,
        vec![100, 101, 102],
        vec![100, 101, 102],
        Vec::new(),
        None,
    )
}

fn poh(verification: SlotRangeVerification, unverifiable_allowed: bool) -> PohReport {
    PohReport::from_verification("full", verification, unverifiable_allowed)
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
fn config_defaults_poh_gate_off_and_parses_settings() {
    let config = parse_config(&[]).expect("defaults");
    assert_eq!(config.poh_verify_mode, PohVerifyMode::Off);
    assert!(!config.poh_verify_allow_unverifiable);
    assert_eq!(config.poh_verify_threads, 0);

    let config = parse_config(&[
        "--poh-verify-mode",
        "full",
        "--poh-verify-allow-unverifiable",
        "--poh-verify-threads",
        "4",
    ])
    .expect("poh settings");
    assert_eq!(config.poh_verify_mode, PohVerifyMode::Full);
    assert!(config.poh_verify_allow_unverifiable);
    assert_eq!(config.poh_verify_threads, 4);

    assert!(parse_config(&["--poh-verify-mode", "fast"]).is_err());
}

#[test]
fn poh_failures_always_block_and_unverifiable_slots_block_unless_allowed() {
    let clean = poh(
        SlotRangeVerification {
            slots_ok: 3,
            slots_skipped: 8,
            ..Default::default()
        },
        false,
    );
    assert!(!clean.blocks_archive());

    let failed = SlotRangeVerification {
        slots_ok: 2,
        slots_failed: 1,
        failed_slots: vec![101],
        ..Default::default()
    };
    assert!(poh(failed.clone(), false).blocks_archive());
    assert!(poh(failed, true).blocks_archive());

    let unverifiable = SlotRangeVerification {
        slots_ok: 2,
        slots_unverifiable: 1,
        unverifiable_slots: vec![102],
        ..Default::default()
    };
    assert!(poh(unverifiable.clone(), false).blocks_archive());
    assert!(!poh(unverifiable, true).blocks_archive());

    let missing = SlotRangeVerification {
        slots_missing: 2,
        missing_spans: vec![(103, 104)],
        ..Default::default()
    };
    assert!(poh(missing, false).blocks_archive());
}

#[test]
fn blocking_poh_result_is_a_data_warning_even_with_rpc_escape_hatch() {
    let mut validation = clean_report();
    assert!(!validation.blocks_archive(true));
    validation.poh = Some(poh(
        SlotRangeVerification {
            slots_failed: 1,
            failed_slots: vec![101],
            ..Default::default()
        },
        false,
    ));
    assert!(validation.has_data_warnings());
    assert!(validation.blocks_archive(true));
    assert!(validation.blocks_archive(false));
}

#[test]
fn poh_result_reaches_report_text_json_metrics_and_skip_reason() {
    let mut validation = clean_report();
    validation.poh = Some(poh(
        SlotRangeVerification {
            slots_ok: 5,
            slots_failed: 2,
            slots_unverifiable: 1,
            failed_slots: vec![102, 101],
            unverifiable_slots: vec![105],
            findings: BTreeMap::from([("entry_hash_mismatch".to_string(), 2)]),
            ..Default::default()
        },
        false,
    ));
    let mut report = ArchiveRunReport::skipped(
        ArchiveKind::Hourly,
        "local".to_string(),
        "validation warnings require --force-archive in server mode",
    );
    report.validation = Some(validation);

    let text = report.to_text();
    assert!(text.contains("validation_poh_mode: full"), "{text}");
    assert!(
        text.contains("validation_poh_failed_ranges: 101-102"),
        "{text}"
    );
    assert!(
        text.contains("validation_poh_unverifiable_ranges: 105"),
        "{text}"
    );

    let json: serde_json::Value = serde_json::from_str(
        &ArchiveReportDocument::new(report.clone())
            .to_json()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(json["validation"]["poh"]["slots_failed"], 2);
    assert_eq!(
        json["validation"]["poh"]["failed_ranges"][0]["slot_count"],
        2
    );
    assert_eq!(
        json["validation"]["poh"]["findings"]["entry_hash_mismatch"],
        2
    );

    let state = AppState::new();
    state.record_report(report);
    let metrics = state.prometheus_text();
    assert!(
        metrics.contains(
            "solparq_validation_slots{archive_kind=\"hourly\",category=\"poh_failed\"} 2"
        ),
        "{metrics}"
    );
    assert!(metrics.contains(
        "solparq_validation_ranges{archive_kind=\"hourly\",category=\"poh_unverifiable\"} 1"
    ));
    assert!(metrics.contains("poh-verification"), "{metrics}");
}
