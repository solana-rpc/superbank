// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Cache-facing index query types. ClickHouse materialized views now maintain
//! the indexes; this module only carries the already-resolved handler bounds.

use crate::clickhouse::{
    NumericFilter, ResolvedSignatureFilter, SignatureSlot, SortOrder, TokenAccountsFilter,
    TransactionStatusFilter,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DiskSigStatus {
    pub(crate) slot: u64,
    pub(crate) err: Option<String>,
}

/// Disk-cache getTransactionsForAddress query. Signature-shaped bounds have
/// already been resolved by the handler, so the local query never consults the
/// primary cluster while executing the cache tier.
#[derive(Debug, Clone)]
pub(crate) struct DiskTfaQuery {
    pub(crate) limit: usize,
    pub(crate) sort_order: SortOrder,
    pub(crate) pagination: Option<SignatureSlot>,
    pub(crate) slot_filter: Option<NumericFilter<u64>>,
    pub(crate) block_time_filter: Option<NumericFilter<i64>>,
    pub(crate) signature_filter: Option<ResolvedSignatureFilter>,
    pub(crate) status: TransactionStatusFilter,
    pub(crate) token_accounts: TokenAccountsFilter,
}
