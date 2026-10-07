// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

mod blocks;
mod cache;
mod client;
mod constants;
mod disconnect;
mod gsfa;
mod inflation_cache;
#[cfg(feature = "disk-cache")]
mod local_transaction;
mod owner_shard;
mod queries;
pub(crate) mod read_query;
mod rows;
mod sharding;
mod signatures;
mod transactions;
mod types;
mod util;
pub mod verification;

#[cfg(feature = "disk-cache")]
pub(crate) use client::{CacheAdmissionBusy, ClickHouseTableNames};
pub use client::{ClickHouseClient, ClickHouseClientOptions, InflationRewardQueryLimits};
#[cfg(feature = "disk-cache")]
pub(crate) use rows::BlockTimeRangeRow;
#[allow(unused_imports)]
pub use types::TransactionsForAddressRecord;
pub use types::{
    BlockFooterRecord, BlockMetadataRecord, NumericFilter, PaginationToken, QueryTimings,
    SignatureFilter, SignatureRecord, SignatureStatusRecord, SortOrder,
    StoredAccountsTransactionRecord, StoredBlockPayload, StoredBlockRecord,
    StoredTransactionRecord, TokenAccountsFilter, TransactionStatusFilter,
    TransactionsForAddressQuery,
};

pub(crate) use types::{
    InflationRewardLookupOutcome, InflationRewardRecord, ResolvedSignatureFilter, SignatureSlot,
    SlotBoundary,
};

pub(crate) use gsfa::{GsfaCursor, GsfaMissingCursor, InlineGsfaPage};
pub(crate) use sharding::{RoutingPolicy, RoutingScope, RoutingTransport, ShardRoutingConfig};
pub(crate) use util::{QueryCacheConfig, QueryFreshnessClass};

#[cfg(feature = "grpc-streaming")]
pub(crate) use util::transient_shard_local_error_reason;

#[cfg(feature = "grpc-head-cache")]
pub(crate) use util::extract_memo;
#[cfg(feature = "disk-cache")]
pub(crate) use util::parse_err_json;

#[cfg(all(test, feature = "disk-cache"))]
pub(crate) use local_transaction::build_fused_transaction_query;
#[cfg(all(test, feature = "disk-cache"))]
pub(crate) use transactions::diagnostics::{measure_layout_sample, measure_position_reads};
