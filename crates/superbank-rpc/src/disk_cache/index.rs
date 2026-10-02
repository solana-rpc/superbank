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

/// One signature's local status, separating a proven absence from a read that proved
/// nothing (timeout, invalidation, no coverage, or a row outside coverage).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DiskStatusLookup {
    Found(DiskSigStatus),
    /// No row in any covered slot: the read completed under an unchanged index epoch
    /// over one contiguous covered span.
    Absent,
    Unknown,
}

impl DiskStatusLookup {
    pub(crate) fn found(self) -> Option<DiskSigStatus> {
        match self {
            Self::Found(status) => Some(status),
            Self::Absent | Self::Unknown => None,
        }
    }
}

/// Per-signature local statuses and, when any absence was provable, the covered span
/// `(floor, tip)` the read searched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DiskStatusLookups {
    pub(crate) statuses: Vec<DiskStatusLookup>,
    pub(crate) span: Option<(u64, u64)>,
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
