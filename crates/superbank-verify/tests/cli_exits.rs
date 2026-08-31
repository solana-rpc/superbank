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
