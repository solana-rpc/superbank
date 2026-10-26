// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::process::{Command, Output};

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_superbank-verify"))
        .args(args)
        .output()
        .expect("run superbank-verify")
}

#[test]
fn help_is_a_successful_cli_exit() {
    let output = run(&["--help"]);

    assert_eq!(output.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&output.stdout).contains("Proof-of-History validator"));
}

#[test]
fn clap_usage_errors_are_operational_failures_not_verification_failures() {
    let output = run(&["--full", "--mode", "not-a-mode"]);

    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&output.stderr).contains("invalid value"));
}

#[test]
fn semantic_cli_errors_are_operational_failures() {
    let output = run(&["--full", "--resume"]);

    assert_eq!(output.status.code(), Some(1));
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("--resume requires --checkpoint-file")
    );
}

#[test]
fn migration_pair_obeys_cli_env_yaml_precedence_without_connecting() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("verify.yaml");
    let valid = format!("42:{}", bs58::encode([7; 32]).into_string());
    for (yaml, env, cli, expected) in [
        ("invalid", valid.as_str(), None, "window-slots"),
        (
            valid.as_str(),
            "invalid",
            Some(valid.as_str()),
            "window-slots",
        ),
        (valid.as_str(), "invalid", None, "alpenglow-genesis-block"),
    ] {
        std::fs::write(
            &path,
            format!("full: true\nwindow-slots: 129\nalpenglow-genesis-block: '{yaml}'\n"),
        )
        .unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_superbank-verify"));
        command
            .args(["--config", path.to_str().unwrap()])
            .env("SUPERBANK_VERIFY_ALPENGLOW_GENESIS_BLOCK", env);
        if let Some(pair) = cli {
            command.args(["--alpenglow-genesis-block", pair]);
        }
        let result = command.output().unwrap();
        assert_eq!(result.status.code(), Some(1));
        assert!(
            String::from_utf8_lossy(&result.stderr).contains(expected),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
    }
}
