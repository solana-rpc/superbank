// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

mod block;
mod errors;
mod meta;
mod transaction;

pub(crate) use block::hydrate_block_payload;
pub(crate) use errors::{BlockHydrationError, TransactionHydrationError};
#[cfg(any(test, feature = "grpc-streaming"))]
pub(crate) use meta::build_transaction_status_meta;
#[cfg(test)]
pub(crate) use meta::build_transaction_status_meta_for_accounts;
pub(crate) use meta::parse_transaction_error_display;
#[cfg(any(test, feature = "grpc-streaming"))]
pub(crate) use transaction::build_versioned_transaction;
pub(crate) use transaction::hydrate_transaction_record;

#[cfg(test)]
pub(crate) use block::hydrate_block_record;
#[cfg(test)]
pub(crate) use meta::parse_instruction_error_display;
