// SPDX-License-Identifier: AGPL-3.0-only
use std::time::Duration;

pub const DEFAULT_STARTUP_VERIFICATION_TIMEOUT_MS: u64 = 10_000;
pub const DEFAULT_RUNTIME_VERIFICATION_TIMEOUT_MS: u64 = 10_000;

/// Independent per-query startup and per-batch termination verification budgets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VerificationTimeouts {
    pub startup: Duration,
    pub runtime: Duration,
}

impl Default for VerificationTimeouts {
    fn default() -> Self {
        Self {
            startup: Duration::from_millis(DEFAULT_STARTUP_VERIFICATION_TIMEOUT_MS),
            runtime: Duration::from_millis(DEFAULT_RUNTIME_VERIFICATION_TIMEOUT_MS),
        }
    }
}
