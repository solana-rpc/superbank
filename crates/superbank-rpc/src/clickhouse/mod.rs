// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

mod blocks;
mod cache;
mod client;
mod constants;
mod gsfa;
mod queries;
mod rows;
mod sharding;
mod signatures;
mod transactions;
mod types;
mod util;

pub use client::{ClickHouseClient, ClickHouseClientOptions};
#[allow(unused_imports)]
pub use types::TransactionsForAddressRecord;
pub use types::{
    BlockMetadataRecord, NumericFilter, PaginationToken, QueryTimings, SignatureFilter,
    SignatureRecord, SignatureStatusRecord, SortOrder, StoredAccountsTransactionRecord,
    StoredBlockPayload, StoredBlockRecord, StoredTransactionRecord, TokenAccountsFilter,
    TransactionStatusFilter, TransactionsForAddressQuery,
};

pub(crate) use types::{ResolvedSignatureFilter, SignatureSlot, SlotBoundary};

pub(crate) use sharding::{RoutingPolicy, RoutingScope, RoutingTransport, ShardRoutingConfig};
pub(crate) use util::{QueryCacheConfig, QueryFreshnessClass};

#[cfg(feature = "grpc-streaming")]
pub(crate) use util::transient_shard_local_error_reason;

#[cfg(feature = "grpc-head-cache")]
pub(crate) use util::extract_memo;
#[cfg(feature = "disk-cache")]
pub(crate) use util::parse_err_json;
