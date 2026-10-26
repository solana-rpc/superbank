// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::fmt::Display;

use ch_cityhash102::cityhash64;

use crate::processing::{ProcessingError, ProcessingResult};

#[cfg(feature = "grpc-streaming")]
use super::constants::SLOT_SHARD_DIVISOR;
use super::types::{
    NumericFilter, PaginationToken, ResolvedSignatureFilter, SignatureFilter, SignatureSlot,
    SlotBoundary, SortOrder, TokenAccountsFilter, TransactionStatusFilter,
    TransactionsForAddressQuery,
};

pub(crate) const TRANSACTION_SELECT_COLUMNS: &str = "signature,
                slot,
                slot_idx,
                block_time,
                is_vote,
                tx_version,
                tx_config_priority_fee,
                tx_config_compute_unit_limit,
                tx_config_loaded_accounts_data_size_limit,
                tx_config_heap_size,
                tx_signatures,
                tx_num_required_signatures,
                tx_num_readonly_signed_accounts,
                tx_num_readonly_unsigned_accounts,
                tx_account_keys,
                tx_recent_blockhash,
                tx_instructions_program_id_index,
                tx_instructions_accounts,
                tx_instructions_data,
                tx_address_table_lookups_present,
                tx_address_table_lookup_account_key,
                tx_address_table_lookup_writable_indexes,
                tx_address_table_lookup_readonly_indexes,
                meta_status_ok,
                meta_err,
                meta_fee,
                meta_pre_balances,
                meta_post_balances,
                meta_inner_instructions_present,
                meta_inner_instructions_index,
                meta_inner_instructions_program_id_index,
                meta_inner_instructions_accounts,
                meta_inner_instructions_data,
                meta_inner_instructions_stack_height,
                meta_log_messages_present,
                meta_log_messages,
                meta_pre_token_balances_present,
                meta_pre_token_account_index,
                meta_pre_token_mint,
                meta_pre_token_owner,
                meta_pre_token_program_id,
                meta_pre_token_amount,
                meta_pre_token_decimals,
                meta_pre_token_ui_amount,
                meta_pre_token_ui_amount_string,
                meta_post_token_balances_present,
                meta_post_token_account_index,
                meta_post_token_mint,
                meta_post_token_owner,
                meta_post_token_program_id,
                meta_post_token_amount,
                meta_post_token_decimals,
                meta_post_token_ui_amount,
                meta_post_token_ui_amount_string,
                meta_rewards_present,
                meta_reward_pubkey,
                meta_reward_lamports,
                meta_reward_post_balance,
                meta_reward_type,
                meta_reward_commission,
                meta_reward_commission_bps,
                meta_loaded_addresses_writable,
                meta_loaded_addresses_readonly,
                meta_return_data_present,
                meta_return_data_program_id,
                meta_return_data_data,
                meta_compute_units_consumed,
                meta_cost_units";

pub(crate) const BLOCK_METADATA_BASE_COLUMNS: &[&str] = &[
    "slot",
    "parent_slot",
    "blockhash",
    "parent_blockhash",
    "block_time",
    "block_height",
    "executed_transaction_count",
    "entry_count",
    "rewards_num_partitions",
    "block_producer_time_nanos",
    "block_user_agent",
];

pub(crate) const BLOCK_METADATA_REWARD_COLUMNS: &[&str] = &[
    "rewards_present",
    "rewards_pubkey",
    "rewards_lamports",
    "rewards_post_balance",
    "rewards_type",
    "rewards_commission",
    "rewards_commission_bps",
];

pub(crate) const BLOCK_SIGNATURE_COLUMNS: &[&str] = &["signature"];

pub(crate) const BLOCK_ACCOUNTS_BASE_COLUMNS: &[&str] = &[
    "tx_version",
    "tx_signatures",
    "tx_num_required_signatures",
    "tx_num_readonly_signed_accounts",
    "tx_num_readonly_unsigned_accounts",
    "tx_account_keys",
    "tx_instructions_program_id_index",
    "meta_status_ok",
    "meta_err",
    "meta_fee",
    "meta_pre_balances",
    "meta_post_balances",
    "meta_pre_token_balances_present",
    "meta_pre_token_account_index",
    "meta_pre_token_mint",
    "meta_pre_token_owner",
    "meta_pre_token_program_id",
    "meta_pre_token_amount",
    "meta_pre_token_decimals",
    "meta_pre_token_ui_amount",
    "meta_pre_token_ui_amount_string",
    "meta_post_token_balances_present",
    "meta_post_token_account_index",
    "meta_post_token_mint",
    "meta_post_token_owner",
    "meta_post_token_program_id",
    "meta_post_token_amount",
    "meta_post_token_decimals",
    "meta_post_token_ui_amount",
    "meta_post_token_ui_amount_string",
    "meta_loaded_addresses_writable",
    "meta_loaded_addresses_readonly",
];

pub(crate) const BLOCK_TRANSACTION_REWARD_COLUMNS: &[&str] = &[
    "meta_rewards_present",
    "meta_reward_pubkey",
    "meta_reward_lamports",
    "meta_reward_post_balance",
    "meta_reward_type",
    "meta_reward_commission",
    "meta_reward_commission_bps",
];

pub(crate) const BLOCK_FULL_BASE_COLUMNS: &[&str] = &[
    "slot_idx",
    "is_vote",
    "tx_version",
    "tx_config_priority_fee",
    "tx_config_compute_unit_limit",
    "tx_config_loaded_accounts_data_size_limit",
    "tx_config_heap_size",
    "tx_signatures",
    "tx_num_required_signatures",
    "tx_num_readonly_signed_accounts",
    "tx_num_readonly_unsigned_accounts",
    "tx_account_keys",
    "tx_recent_blockhash",
    "tx_instructions_program_id_index",
    "tx_instructions_accounts",
    "tx_instructions_data",
    "tx_address_table_lookups_present",
    "tx_address_table_lookup_account_key",
    "tx_address_table_lookup_writable_indexes",
    "tx_address_table_lookup_readonly_indexes",
    "meta_status_ok",
    "meta_err",
    "meta_fee",
    "meta_pre_balances",
    "meta_post_balances",
    "meta_inner_instructions_present",
    "meta_inner_instructions_index",
    "meta_inner_instructions_program_id_index",
    "meta_inner_instructions_accounts",
    "meta_inner_instructions_data",
    "meta_inner_instructions_stack_height",
    "meta_log_messages_present",
    "meta_log_messages",
    "meta_pre_token_balances_present",
    "meta_pre_token_account_index",
    "meta_pre_token_mint",
    "meta_pre_token_owner",
    "meta_pre_token_program_id",
    "meta_pre_token_amount",
    "meta_pre_token_decimals",
    "meta_pre_token_ui_amount",
    "meta_pre_token_ui_amount_string",
    "meta_post_token_balances_present",
    "meta_post_token_account_index",
    "meta_post_token_mint",
    "meta_post_token_owner",
    "meta_post_token_program_id",
    "meta_post_token_amount",
    "meta_post_token_decimals",
    "meta_post_token_ui_amount",
    "meta_post_token_ui_amount_string",
    "meta_loaded_addresses_writable",
    "meta_loaded_addresses_readonly",
    "meta_return_data_present",
    "meta_return_data_program_id",
    "meta_return_data_data",
    "meta_compute_units_consumed",
    "meta_cost_units",
];

pub(crate) fn format_select_columns(columns: &[&str]) -> String {
    columns.join(",\n                ")
}

pub(crate) const GSFA_REQUIRED_COLUMNS: [&str; 8] = [
    "addr_bucket",
    "address",
    "signature",
    "slot",
    "slot_idx",
    "memo",
    "err",
    "block_time",
];

pub(crate) const SIGNATURES_REQUIRED_COLUMNS: [&str; 5] =
    ["sig_bucket", "signature", "slot", "slot_idx", "err"];

pub(crate) const TOKEN_OWNER_REQUIRED_COLUMNS: [&str; 9] = [
    "owner_bucket",
    "owner",
    "signature",
    "slot",
    "slot_idx",
    "memo",
    "err",
    "block_time",
    "balance_changed",
];

pub(crate) struct TransactionsForAddressTables<'a> {
    pub(crate) cache_partition: Option<(u64, u64)>,
    pub(crate) gsfa_table: &'a str,
    pub(crate) gsfa_bucket_modulus: u64,
    pub(crate) token_owner_table: &'a str,
    pub(crate) token_owner_bucket_modulus: u64,
    pub(crate) signatures_table: &'a str,
    pub(crate) signature_bucket_modulus: u64,
    /// Token-accounts UNION: push the filter, ORDER BY and LIMIT into each branch
    /// (`CLICKHOUSE_TRANSACTIONS_FOR_ADDRESS_UNION_PUSHDOWN`); `false` keeps the filter
    /// outside the union, as before.
    pub(crate) union_pushdown: bool,
}

#[derive(Clone, Copy)]
enum SlotIdxComparison {
    Lt,
    Lte,
    Gt,
    Gte,
}

struct SignaturePositionExpr {
    slot_expr: String,
    idx_expr: String,
    nullable: bool,
    with_parts: Vec<String>,
}

fn append_numeric_filter_conditions<T: Display>(
    column: &str,
    filter: &NumericFilter<T>,
    conditions: &mut Vec<String>,
) {
    if let Some(value) = &filter.eq {
        conditions.push(format!("{column} = {value}"));
    }
    if let Some(value) = &filter.gte {
        conditions.push(format!("{column} >= {value}"));
    }
    if let Some(value) = &filter.gt {
        conditions.push(format!("{column} > {value}"));
    }
    if let Some(value) = &filter.lte {
        conditions.push(format!("{column} <= {value}"));
    }
    if let Some(value) = &filter.lt {
        conditions.push(format!("{column} < {value}"));
    }
}

fn append_transactions_for_address_slot_filter_conditions(
    filter: &NumericFilter<u64>,
    conditions: &mut Vec<String>,
) {
    // Qualification build: requires a ClickHouse version with corrected reverse-key
    // pruning. Bare key predicates avoid per-query primary-index column transforms.
    let slot_expr = "slot";
    if let Some(value) = filter.eq {
        conditions.push(format!("{slot_expr} = {value}"));
    }
    if let Some(value) = filter.gte {
        conditions.push(format!("{slot_expr} >= {value}"));
    }
    if let Some(value) = filter.gt {
        conditions.push(format!("{slot_expr} > {value}"));
    }
    if let Some(value) = filter.lte {
        conditions.push(format!("{slot_expr} <= {value}"));
    }
    if let Some(value) = filter.lt {
        conditions.push(format!("{slot_expr} < {value}"));
    }
}

fn signature_position_for_signature(
    signatures_table: &str,
    signature_bucket_modulus: u64,
    signature: &str,
    prefix: &str,
) -> ProcessingResult<SignaturePositionExpr> {
    let signature_bytes = bs58::decode(signature)
        .into_vec()
        .map_err(|e| ProcessingError::deserialization("Invalid signature", e))?;
    if signature_bytes.len() != 64 {
        return Err(ProcessingError::deserialization_msg(format!(
            "Invalid signature length {} (expected 64 bytes)",
            signature_bytes.len()
        )));
    }

    let signature_hex = hex::encode(&signature_bytes).to_uppercase();
    let signature_literal = format!("toFixedString(unhex('{signature_hex}'), 64)");
    let signature_bucket = cityhash64(signature_bytes.as_slice()) % signature_bucket_modulus;

    let sig_alias = format!("{prefix}_sig");
    let bucket_alias = format!("{prefix}_bucket");
    let slot_alias = format!("{prefix}_slot");
    let idx_alias = format!("{prefix}_idx");

    let with_parts = vec![
        format!("{signature_literal} AS {sig_alias}"),
        format!("{signature_bucket} AS {bucket_alias}"),
        format!(
            "(SELECT CAST(slot AS Nullable(UInt64)) \
             FROM {signatures_table} \
             PREWHERE sig_bucket = {bucket_alias} AND signature = {sig_alias} \
             ORDER BY slot DESC, slot_idx DESC \
             LIMIT 1) AS {slot_alias}",
            signatures_table = signatures_table,
            bucket_alias = bucket_alias,
            sig_alias = sig_alias,
            slot_alias = slot_alias
        ),
        format!(
            "(SELECT CAST(slot_idx AS Nullable(UInt32)) \
             FROM {signatures_table} \
             PREWHERE sig_bucket = {bucket_alias} AND signature = {sig_alias} \
             ORDER BY slot DESC, slot_idx DESC \
             LIMIT 1) AS {idx_alias}",
            signatures_table = signatures_table,
            bucket_alias = bucket_alias,
            sig_alias = sig_alias,
            idx_alias = idx_alias
        ),
    ];

    Ok(SignaturePositionExpr {
        slot_expr: slot_alias,
        idx_expr: idx_alias,
        nullable: true,
        with_parts,
    })
}

fn slot_idx_condition(expr: &SignaturePositionExpr, comparison: SlotIdxComparison) -> String {
    let (slot_expr, idx_expr) = (&expr.slot_expr, &expr.idx_expr);
    let slot_col = "slot";
    let idx_col = "slot_idx";
    let condition = match comparison {
        SlotIdxComparison::Lt => {
            format!(
                "{slot_col} < {slot_expr} OR ({slot_col} = {slot_expr} AND {idx_col} < {idx_expr})"
            )
        }
        SlotIdxComparison::Lte => {
            format!(
                "{slot_col} < {slot_expr} OR ({slot_col} = {slot_expr} AND {idx_col} <= {idx_expr})"
            )
        }
        SlotIdxComparison::Gt => {
            format!(
                "{slot_col} > {slot_expr} OR ({slot_col} = {slot_expr} AND {idx_col} > {idx_expr})"
            )
        }
        SlotIdxComparison::Gte => {
            format!(
                "{slot_col} > {slot_expr} OR ({slot_col} = {slot_expr} AND {idx_col} >= {idx_expr})"
            )
        }
    };

    if expr.nullable {
        format!("(isNull({slot_expr}) OR {condition})")
    } else {
        format!("({condition})")
    }
}

fn slot_idx_condition_for_position(
    position: SignatureSlot,
    comparison: SlotIdxComparison,
) -> String {
    let slot = position.slot;
    let idx = position.slot_idx;
    match comparison {
        SlotIdxComparison::Lt => {
            format!("(slot < {slot} OR (slot = {slot} AND slot_idx < {idx}))")
        }
        SlotIdxComparison::Lte => {
            format!("(slot < {slot} OR (slot = {slot} AND slot_idx <= {idx}))")
        }
        SlotIdxComparison::Gt => {
            format!("(slot > {slot} OR (slot = {slot} AND slot_idx > {idx}))")
        }
        SlotIdxComparison::Gte => {
            format!("(slot > {slot} OR (slot = {slot} AND slot_idx >= {idx}))")
        }
    }
}

fn apply_signature_filter(
    signatures_table: &str,
    signature_bucket_modulus: u64,
    filter: &SignatureFilter,
    with_parts: &mut Vec<String>,
    conditions: &mut Vec<String>,
) -> ProcessingResult<()> {
    if let Some(sig) = filter.gte.as_deref() {
        let expr = signature_position_for_signature(
            signatures_table,
            signature_bucket_modulus,
            sig,
            "sig_gte",
        )?;
        let condition = slot_idx_condition(&expr, SlotIdxComparison::Gte);
        with_parts.extend(expr.with_parts);
        conditions.push(condition);
    }
    if let Some(sig) = filter.gt.as_deref() {
        let expr = signature_position_for_signature(
            signatures_table,
            signature_bucket_modulus,
            sig,
            "sig_gt",
        )?;
        let condition = slot_idx_condition(&expr, SlotIdxComparison::Gt);
        with_parts.extend(expr.with_parts);
        conditions.push(condition);
    }
    if let Some(sig) = filter.lte.as_deref() {
        let expr = signature_position_for_signature(
            signatures_table,
            signature_bucket_modulus,
            sig,
            "sig_lte",
        )?;
        let condition = slot_idx_condition(&expr, SlotIdxComparison::Lte);
        with_parts.extend(expr.with_parts);
        conditions.push(condition);
    }
    if let Some(sig) = filter.lt.as_deref() {
        let expr = signature_position_for_signature(
            signatures_table,
            signature_bucket_modulus,
            sig,
            "sig_lt",
        )?;
        let condition = slot_idx_condition(&expr, SlotIdxComparison::Lt);
        with_parts.extend(expr.with_parts);
        conditions.push(condition);
    }

    Ok(())
}

fn apply_pagination_token(
    signatures_table: &str,
    signature_bucket_modulus: u64,
    pagination: &PaginationToken,
    sort_order: SortOrder,
    with_parts: &mut Vec<String>,
    conditions: &mut Vec<String>,
) -> ProcessingResult<()> {
    let comparison = match sort_order {
        SortOrder::Desc => SlotIdxComparison::Lt,
        SortOrder::Asc => SlotIdxComparison::Gt,
    };

    match pagination {
        PaginationToken::SlotIndex { slot, idx } => {
            conditions.push(slot_idx_condition(
                &SignaturePositionExpr {
                    slot_expr: slot.to_string(),
                    idx_expr: idx.to_string(),
                    nullable: false,
                    with_parts: Vec::new(),
                },
                comparison,
            ));
        }
        PaginationToken::Signature(sig) => {
            let expr = signature_position_for_signature(
                signatures_table,
                signature_bucket_modulus,
                sig,
                "page",
            )?;
            let condition = slot_idx_condition(&expr, comparison);
            with_parts.extend(expr.with_parts);
            conditions.push(condition);
        }
    }

    Ok(())
}

fn apply_resolved_signature_filter(filter: &ResolvedSignatureFilter, conditions: &mut Vec<String>) {
    for (position, comparison) in [
        (filter.gte, SlotIdxComparison::Gte),
        (filter.gt, SlotIdxComparison::Gt),
        (filter.lte, SlotIdxComparison::Lte),
        (filter.lt, SlotIdxComparison::Lt),
    ] {
        if let Some(position) = position {
            conditions.push(slot_idx_condition(
                &SignaturePositionExpr {
                    slot_expr: position.slot.to_string(),
                    idx_expr: position.slot_idx.to_string(),
                    nullable: false,
                    with_parts: Vec::new(),
                },
                comparison,
            ));
        }
    }
}

fn apply_hot_resolved_signature_filter(
    filter: &ResolvedSignatureFilter,
    conditions: &mut Vec<String>,
) {
    if let Some(position) = filter.gte {
        conditions.push(slot_idx_condition_for_position(
            position,
            SlotIdxComparison::Gte,
        ));
    }
    if let Some(position) = filter.gt {
        conditions.push(slot_idx_condition_for_position(
            position,
            SlotIdxComparison::Gt,
        ));
    }
    if let Some(position) = filter.lte {
        conditions.push(slot_idx_condition_for_position(
            position,
            SlotIdxComparison::Lte,
        ));
    }
    if let Some(position) = filter.lt {
        conditions.push(slot_idx_condition_for_position(
            position,
            SlotIdxComparison::Lt,
        ));
    }
}

fn apply_hot_resolved_pagination_token(
    pagination: SignatureSlot,
    sort_order: SortOrder,
    conditions: &mut Vec<String>,
) {
    let comparison = match sort_order {
        SortOrder::Desc => SlotIdxComparison::Lt,
        SortOrder::Asc => SlotIdxComparison::Gt,
    };
    conditions.push(slot_idx_condition_for_position(pagination, comparison));
}

pub(crate) fn build_pagination_clauses(
    before: Option<SlotBoundary>,
    until: Option<SlotBoundary>,
) -> (String, String) {
    let mut conditions = Vec::new();
    // Preserve same-slot boundaries while comparing the indexed columns directly.
    // This qualification build requires corrected ClickHouse reverse-key pruning.

    if let Some(before) = before {
        let condition = match before {
            SlotBoundary::Position(before_pos) => {
                format!(
                    "(slot < {slot} OR (slot = {slot} AND slot_idx < {idx}))",
                    slot = before_pos.slot,
                    idx = before_pos.slot_idx
                )
            }
            SlotBoundary::Slot(slot) => format!("slot < {slot}"),
        };
        conditions.push(condition);
    }

    if let Some(until) = until {
        let condition = match until {
            SlotBoundary::Position(until_pos) => {
                format!(
                    "(slot > {slot} OR (slot = {slot} AND slot_idx > {idx}))",
                    slot = until_pos.slot,
                    idx = until_pos.slot_idx
                )
            }
            SlotBoundary::Slot(slot) => format!("slot > {slot}"),
        };
        conditions.push(condition);
    }

    let where_clause = if conditions.is_empty() {
        "1".to_string()
    } else {
        conditions.join(" AND ")
    };

    (String::new(), where_clause)
}

pub(crate) fn build_hot_position_pagination_clauses(
    before: Option<SlotBoundary>,
    until: Option<SlotBoundary>,
) -> (String, String) {
    let mut conditions = Vec::new();

    if let Some(before) = before {
        let condition = match before {
            SlotBoundary::Position(position) => {
                slot_idx_condition_for_position(position, SlotIdxComparison::Lt)
            }
            SlotBoundary::Slot(slot) => format!("slot < {slot}"),
        };
        conditions.push(condition);
    }

    if let Some(until) = until {
        let condition = match until {
            SlotBoundary::Position(position) => {
                slot_idx_condition_for_position(position, SlotIdxComparison::Gt)
            }
            SlotBoundary::Slot(slot) => format!("slot > {slot}"),
        };
        conditions.push(condition);
    }

    let where_clause = if conditions.is_empty() {
        "1".to_string()
    } else {
        conditions.join(" AND ")
    };

    (String::new(), where_clause)
}

pub(crate) fn build_transactions_for_address_query(
    tables: &TransactionsForAddressTables<'_>,
    query: &TransactionsForAddressQuery,
    settings_clause: &str,
) -> ProcessingResult<String> {
    let address_literal = format!("CAST(base58Decode('{}') AS FixedString(32))", query.address);
    let gsfa_addr_bucket = format!(
        "cityHash64({}) % {}",
        address_literal, tables.gsfa_bucket_modulus
    );
    let token_owner_bucket = format!(
        "cityHash64({}) % {}",
        address_literal, tables.token_owner_bucket_modulus
    );

    let mut with_parts = Vec::new();
    let mut conditions = Vec::new();

    match query.status {
        TransactionStatusFilter::Succeeded => conditions.push("err IS NULL".to_string()),
        TransactionStatusFilter::Failed => conditions.push("err IS NOT NULL".to_string()),
        TransactionStatusFilter::Any => {}
    }

    if let Some(slot_filter) = &query.slot_filter {
        append_transactions_for_address_slot_filter_conditions(slot_filter, &mut conditions);
    }

    if let Some(block_time_filter) = &query.block_time_filter {
        append_numeric_filter_conditions("block_time", block_time_filter, &mut conditions);
    }

    if let Some(signature_filter) = query.resolved_signature_filter.as_ref() {
        // Compare pre-resolved bounds directly, without scalar lookups or
        // per-query transformations of primary-index columns.
        apply_resolved_signature_filter(signature_filter, &mut conditions);
    } else if let Some(signature_filter) = &query.signature_filter {
        apply_signature_filter(
            tables.signatures_table,
            tables.signature_bucket_modulus,
            signature_filter,
            &mut with_parts,
            &mut conditions,
        )?;
    }

    if let Some(pagination) = &query.pagination {
        apply_pagination_token(
            tables.signatures_table,
            tables.signature_bucket_modulus,
            pagination,
            query.sort_order,
            &mut with_parts,
            &mut conditions,
        )?;
    }

    let with_clause = if with_parts.is_empty() {
        String::new()
    } else {
        format!("WITH {} ", with_parts.join(", "))
    };
    let where_clause = if conditions.is_empty() {
        "1".to_string()
    } else {
        conditions.join(" AND ")
    };

    let slot_scope = tables
        .cache_partition
        .map_or_else(String::new, |(width, partition)| {
            format!(" AND intDiv(slot, {width}) = {partition}")
        });

    let order_dir = match query.sort_order {
        SortOrder::Asc => "ASC",
        SortOrder::Desc => "DESC",
    };

    let select = if query.token_accounts == TokenAccountsFilter::None {
        format!(
            "{with_clause}SELECT
            base58Encode(signature) AS signature,
            slot,
            slot_idx,
            err,
            memo,
            block_time
         FROM {gsfa_table}
         PREWHERE addr_bucket = {addr_bucket} AND address = {address_literal}{slot_scope}
         WHERE {where_clause}",
            with_clause = with_clause,
            gsfa_table = tables.gsfa_table,
            addr_bucket = gsfa_addr_bucket,
            address_literal = address_literal,
            where_clause = where_clause,
        )
    } else {
        let balance_condition = if query.token_accounts == TokenAccountsFilter::BalanceChanged {
            "balance_changed = 1 AND "
        } else {
            ""
        };
        let gsfa_branch = format!(
            "SELECT signature, slot, slot_idx, err, memo, block_time
             FROM {gsfa_table}
             PREWHERE addr_bucket = {addr_bucket} AND address = {address_literal}{slot_scope}",
            gsfa_table = tables.gsfa_table,
            addr_bucket = gsfa_addr_bucket,
            address_literal = address_literal
        );
        let token_branch = format!(
            "SELECT signature, slot, slot_idx, err, memo, block_time
             FROM {token_owner_table}
             PREWHERE owner_bucket = {addr_bucket} AND owner = {address_literal}{slot_scope}",
            token_owner_table = tables.token_owner_table,
            addr_bucket = token_owner_bucket,
            address_literal = address_literal
        );
        if with_parts.is_empty() && tables.union_pushdown {
            // Every bound is a literal, so each branch can apply the filter and
            // its own top-`limit` before the union. The outer ORDER BY/LIMIT
            // below still picks the global top-`limit`, which lies within the
            // union of each branch's top-`limit`. Branches order by the same
            // key as the outer query (base58 tiebreak, which the outer alias
            // `signature` resolves to), so ties at a branch boundary resolve
            // identically. That alias resolution needs the server default
            // prefer_column_name_to_alias=0; with 1 the outer query would sort
            // raw bytes and distinct signatures tied on (slot, slot_idx) across
            // a branch LIMIT could differ. Without the pushdown every matching
            // branch row is shipped from each shard to the initiator.
            let branch_order = format!(
                "ORDER BY slot {order_dir}, slot_idx {order_dir}, base58Encode(signature) {order_dir}
             LIMIT {limit}",
                limit = query.limit
            );
            format!(
                "SELECT
                base58Encode(signature) AS signature,
                slot,
                slot_idx,
                err,
                memo,
                block_time
             FROM (
                {gsfa_branch}
             WHERE {where_clause}
             {branch_order}
                UNION ALL
                {token_branch}
             WHERE {balance_condition}({where_clause})
             {branch_order}
             )"
            )
        } else {
            // Unresolved signature bounds are WITH scalar subqueries; keep the
            // filter outside the union so the aliases stay in scope.
            let balance_clause = if balance_condition.is_empty() {
                ""
            } else {
                " WHERE balance_changed = 1"
            };
            format!(
                "{with_clause}SELECT
                base58Encode(signature) AS signature,
                slot,
                slot_idx,
                err,
                memo,
                block_time
             FROM (
                {gsfa_branch}
UNION ALL
                {token_branch}{balance_clause}
             )
             WHERE {where_clause}"
            )
        }
    };

    Ok(format!(
        "{select}
         ORDER BY slot {order_dir}, slot_idx {order_dir}, signature {order_dir}
         LIMIT {limit}
         {settings_clause}",
        select = select,
        order_dir = order_dir,
        limit = query.limit,
        settings_clause = settings_clause
    ))
}

pub(crate) fn build_transactions_for_address_hot_query(
    gsfa_table: &str,
    gsfa_bucket_modulus: u64,
    query: &TransactionsForAddressQuery,
    settings_clause: &str,
) -> ProcessingResult<String> {
    if query.token_accounts != TokenAccountsFilter::None {
        return Err(ProcessingError::database_msg(
            "hot GSFA fanout only supports tokenAccounts=none".to_string(),
        ));
    }

    let address_literal = format!("CAST(base58Decode('{}') AS FixedString(32))", query.address);
    let gsfa_addr_bucket = format!("cityHash64({}) % {}", address_literal, gsfa_bucket_modulus);

    let mut conditions = Vec::new();

    match query.status {
        TransactionStatusFilter::Succeeded => conditions.push("err IS NULL".to_string()),
        TransactionStatusFilter::Failed => conditions.push("err IS NOT NULL".to_string()),
        TransactionStatusFilter::Any => {}
    }

    if let Some(slot_filter) = &query.slot_filter {
        append_numeric_filter_conditions("slot", slot_filter, &mut conditions);
    }

    if let Some(block_time_filter) = &query.block_time_filter {
        append_numeric_filter_conditions("block_time", block_time_filter, &mut conditions);
    }

    if let Some(signature_filter) = query.resolved_signature_filter.as_ref() {
        apply_hot_resolved_signature_filter(signature_filter, &mut conditions);
    }

    if let Some(pagination) = query.resolved_pagination {
        apply_hot_resolved_pagination_token(pagination, query.sort_order, &mut conditions);
    }

    let where_clause = if conditions.is_empty() {
        "1".to_string()
    } else {
        conditions.join(" AND ")
    };

    let order_dir = match query.sort_order {
        SortOrder::Asc => "ASC",
        SortOrder::Desc => "DESC",
    };

    Ok(format!(
        "SELECT
            base58Encode(signature) AS signature,
            slot,
            slot_idx,
            err,
            memo,
            block_time
         FROM {gsfa_table}
         PREWHERE addr_bucket = {addr_bucket} AND address = {address_literal}
         WHERE {where_clause}
         ORDER BY slot {order_dir}, slot_idx {order_dir}, signature {order_dir}
         LIMIT {limit}
         {settings_clause}",
        gsfa_table = gsfa_table,
        addr_bucket = gsfa_addr_bucket,
        address_literal = address_literal,
        where_clause = where_clause,
        order_dir = order_dir,
        limit = query.limit,
        settings_clause = settings_clause
    ))
}

fn transaction_tuple_predicates(chunk: &[(u64, Option<u32>, String)]) -> [Option<String>; 2] {
    let exact = chunk
        .iter()
        .filter_map(|(slot, idx, literal)| idx.map(|idx| format!("({slot}, {idx}, {literal})")))
        .collect::<Vec<_>>();
    let fallback = chunk
        .iter()
        .filter(|(_, idx, _)| idx.is_none())
        .map(|(slot, _, literal)| format!("({slot}, {literal})"))
        .collect::<Vec<_>>();
    [
        (!exact.is_empty())
            .then(|| format!("(slot, slot_idx, signature) IN ({})", exact.join(", "))),
        (!fallback.is_empty()).then(|| format!("(slot, signature) IN ({})", fallback.join(", "))),
    ]
}

pub(crate) fn build_transactions_by_slot_signatures_query(
    transaction_table: &str,
    pairs: &[(u64, Option<u32>, String)],
    settings_clause: &str,
    in_clause_chunk: usize,
) -> String {
    let chunk_size = in_clause_chunk.max(1);
    let mut prewhere_parts = Vec::new();
    for chunk in pairs.chunks(chunk_size) {
        prewhere_parts.extend(transaction_tuple_predicates(chunk).into_iter().flatten());
    }
    let prewhere_clause = if prewhere_parts.len() == 1 {
        prewhere_parts[0].clone()
    } else {
        format!("({})", prewhere_parts.join(" OR "))
    };
    format!(
        "SELECT
            {columns}
         FROM {transaction_table}
         PREWHERE {prewhere_clause}
         {settings_clause}",
        columns = TRANSACTION_SELECT_COLUMNS,
        transaction_table = transaction_table,
        prewhere_clause = prewhere_clause,
        settings_clause = settings_clause
    )
}

/// Range scan for gRPC streaming: every transaction in
/// `[start_slot, end_slot]` in `(slot, slot_idx)` order. `LIMIT 1 BY signature`
/// mirrors the per-slot block query's ReplacingMergeTree dedup.
#[cfg(feature = "grpc-streaming")]
pub(crate) fn build_transactions_by_slot_range_query(
    transaction_table: &str,
    start_slot: u64,
    end_slot: u64,
    settings_clause: &str,
) -> String {
    let start_bucket = start_slot / SLOT_SHARD_DIVISOR;
    let end_bucket = end_slot / SLOT_SHARD_DIVISOR;

    format!(
        "SELECT
            {columns}
         FROM {transaction_table}
         PREWHERE
            intDiv(slot, {slot_shard_divisor}) BETWEEN {start_bucket} AND {end_bucket}
            AND slot BETWEEN {start_slot} AND {end_slot}
         ORDER BY slot ASC, slot_idx ASC, signature ASC
         LIMIT 1 BY signature
         {settings_clause}",
        columns = TRANSACTION_SELECT_COLUMNS,
        transaction_table = transaction_table,
        slot_shard_divisor = SLOT_SHARD_DIVISOR,
        start_bucket = start_bucket,
        end_bucket = end_bucket,
        start_slot = start_slot,
        end_slot = end_slot,
        settings_clause = settings_clause
    )
}

#[cfg(test)]
mod tests {
    use ch_cityhash102::cityhash64;

    #[cfg(feature = "grpc-streaming")]
    use super::build_transactions_by_slot_range_query;
    use super::{
        TransactionsForAddressTables, build_hot_position_pagination_clauses,
        build_transactions_for_address_hot_query, build_transactions_for_address_query,
    };
    #[cfg(feature = "grpc-streaming")]
    use crate::clickhouse::constants::SLOT_SHARD_DIVISOR;
    use crate::clickhouse::types::{
        NumericFilter, ResolvedSignatureFilter, SignatureFilter, SignatureSlot, SlotBoundary,
        SortOrder, TokenAccountsFilter, TransactionStatusFilter, TransactionsForAddressQuery,
    };

    fn normalize_sql(sql: &str) -> String {
        sql.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    #[test]
    fn transactions_for_address_query_uses_distinct_bucket_moduli() {
        let address = bs58::encode([9_u8; 32]).into_string();
        let signature_bytes = [7_u8; 64];
        let signature = bs58::encode(signature_bytes).into_string();
        let expected_signature_bucket = cityhash64(signature_bytes.as_slice()) % 64;

        let query = TransactionsForAddressQuery {
            address: address.clone(),
            limit: 25,
            sort_order: SortOrder::Desc,
            pagination: None,
            resolved_pagination: None,
            slot_filter: None,
            block_time_filter: None,
            signature_filter: Some(SignatureFilter {
                gte: Some(signature),
                ..SignatureFilter::default()
            }),
            resolved_signature_filter: None,
            status: TransactionStatusFilter::Any,
            token_accounts: TokenAccountsFilter::All,
        };

        let tables = TransactionsForAddressTables {
            cache_partition: None,
            gsfa_table: "default.gsfa",
            gsfa_bucket_modulus: 128,
            token_owner_table: "default.token_owner_activity",
            token_owner_bucket_modulus: 32,
            signatures_table: "default.signatures",
            signature_bucket_modulus: 64,
            union_pushdown: true,
        };

        let sql = build_transactions_for_address_query(&tables, &query, "").expect("query");
        let sql = normalize_sql(&sql);

        assert!(
            sql.contains("FROM default.gsfa PREWHERE addr_bucket = cityHash64(CAST(base58Decode('")
        );
        assert!(sql.contains("AS FixedString(32))) % 128 AND address = CAST(base58Decode('"));
        assert!(sql.contains(
            "FROM default.token_owner_activity PREWHERE owner_bucket = cityHash64(CAST(base58Decode('"
        ));
        assert!(sql.contains("AS FixedString(32))) % 32 AND owner = CAST(base58Decode('"));
        assert!(sql.contains(&format!("{expected_signature_bucket} AS sig_gte_bucket")));
        assert!(
            sql.contains(
                "slot > sig_gte_slot OR (slot = sig_gte_slot AND slot_idx >= sig_gte_idx)"
            )
        );
    }

    fn token_union_query(
        token_accounts: TokenAccountsFilter,
        signature_filter: Option<SignatureFilter>,
        resolved_signature_filter: Option<ResolvedSignatureFilter>,
    ) -> String {
        token_union_query_with(
            true,
            token_accounts,
            signature_filter,
            resolved_signature_filter,
        )
    }

    fn token_union_query_with(
        union_pushdown: bool,
        token_accounts: TokenAccountsFilter,
        signature_filter: Option<SignatureFilter>,
        resolved_signature_filter: Option<ResolvedSignatureFilter>,
    ) -> String {
        let query = TransactionsForAddressQuery {
            address: bs58::encode([9_u8; 32]).into_string(),
            limit: 40,
            sort_order: SortOrder::Asc,
            pagination: None,
            resolved_pagination: None,
            slot_filter: Some(NumericFilter {
                gte: Some(100),
                ..NumericFilter::default()
            }),
            block_time_filter: None,
            signature_filter,
            resolved_signature_filter,
            status: TransactionStatusFilter::Failed,
            token_accounts,
        };
        let tables = TransactionsForAddressTables {
            cache_partition: None,
            gsfa_table: "default.gsfa",
            gsfa_bucket_modulus: 128,
            token_owner_table: "default.token_owner_activity",
            token_owner_bucket_modulus: 32,
            signatures_table: "default.signatures",
            signature_bucket_modulus: 64,
            union_pushdown,
        };
        normalize_sql(&build_transactions_for_address_query(&tables, &query, "").expect("query"))
    }

    #[test]
    fn token_accounts_union_pushdown_switch_off_keeps_the_outer_filter() {
        // CLICKHOUSE_TRANSACTIONS_FOR_ADDRESS_UNION_PUSHDOWN=false: literal bounds get the
        // pre-pushdown shape, filter outside the union and no per-branch ORDER BY/LIMIT.
        let resolved = || {
            Some(ResolvedSignatureFilter {
                lt: Some(SignatureSlot {
                    slot: 500,
                    slot_idx: 7,
                }),
                ..ResolvedSignatureFilter::default()
            })
        };
        let signature_filter = || {
            Some(SignatureFilter {
                lt: Some("unused".into()),
                ..SignatureFilter::default()
            })
        };
        let sql = token_union_query_with(
            false,
            TokenAccountsFilter::BalanceChanged,
            signature_filter(),
            resolved(),
        );
        assert!(!sql.starts_with("WITH"), "{sql}");
        assert!(!sql.contains("base58Encode(signature) ASC LIMIT"), "{sql}");
        assert!(
            sql.contains("AS FixedString(32)) WHERE balance_changed = 1 ) WHERE err IS NOT NULL"),
            "{sql}"
        );
        assert_eq!(
            sql.matches("(slot < 500 OR (slot = 500 AND slot_idx < 7))")
                .count(),
            1,
            "{sql}"
        );
        assert!(sql.ends_with("ORDER BY slot ASC, slot_idx ASC, signature ASC LIMIT 40"));
        // The switch changes nothing for unresolved bounds (already the outer shape)
        // or for single-table requests.
        let signature = bs58::encode([7_u8; 64]).into_string();
        let unresolved = |pushdown| {
            token_union_query_with(
                pushdown,
                TokenAccountsFilter::All,
                Some(SignatureFilter {
                    gte: Some(signature.clone()),
                    ..SignatureFilter::default()
                }),
                None,
            )
        };
        assert_eq!(unresolved(false), unresolved(true));
        assert_eq!(
            token_union_query_with(false, TokenAccountsFilter::None, None, None),
            token_union_query_with(true, TokenAccountsFilter::None, None, None)
        );
    }

    #[test]
    fn token_accounts_union_pushes_filter_order_and_limit_into_each_branch() {
        let branch_tail = "ORDER BY slot ASC, slot_idx ASC, base58Encode(signature) ASC LIMIT 40";
        let sql = token_union_query(TokenAccountsFilter::BalanceChanged, None, None);

        assert!(sql.contains(&format!(
            "AS FixedString(32)) WHERE err IS NOT NULL AND slot >= 100 {branch_tail} UNION ALL"
        )));
        assert!(sql.contains(&format!(
            "WHERE balance_changed = 1 AND (err IS NOT NULL AND slot >= 100) {branch_tail} )"
        )));
        // The outer query still picks the global top-n from both branches.
        assert!(sql.ends_with(") ORDER BY slot ASC, slot_idx ASC, signature ASC LIMIT 40"));
        assert!(!sql.starts_with("WITH"));

        let sql = token_union_query(TokenAccountsFilter::All, None, None);
        assert!(sql.contains(&format!(
            "WHERE (err IS NOT NULL AND slot >= 100) {branch_tail} )"
        )));
        assert!(!sql.contains("balance_changed"));
        assert_eq!(sql.matches(branch_tail).count(), 2);

        // Resolved signature bounds are literals and push down too.
        let sql = token_union_query(
            TokenAccountsFilter::All,
            Some(SignatureFilter {
                lt: Some("unused".into()),
                ..SignatureFilter::default()
            }),
            Some(ResolvedSignatureFilter {
                lt: Some(SignatureSlot {
                    slot: 500,
                    slot_idx: 7,
                }),
                ..ResolvedSignatureFilter::default()
            }),
        );
        assert_eq!(sql.matches(branch_tail).count(), 2);
        assert_eq!(
            sql.matches("(slot < 500 OR (slot = 500 AND slot_idx < 7))")
                .count(),
            2
        );
    }

    #[test]
    fn token_accounts_union_keeps_outer_filter_for_unresolved_signature_bounds() {
        let signature = bs58::encode([7_u8; 64]).into_string();
        let sql = token_union_query(
            TokenAccountsFilter::BalanceChanged,
            Some(SignatureFilter {
                gte: Some(signature),
                ..SignatureFilter::default()
            }),
            None,
        );

        assert!(sql.starts_with("WITH "));
        assert!(!sql.contains("base58Encode(signature) ASC LIMIT"));
        assert!(sql.contains("AND owner = CAST(base58Decode('"));
        assert!(
            sql.contains("AS FixedString(32)) WHERE balance_changed = 1 ) WHERE err IS NOT NULL")
        );
        assert!(sql.contains("slot > sig_gte_slot OR"));
        assert!(sql.ends_with("ORDER BY slot ASC, slot_idx ASC, signature ASC LIMIT 40"));
    }

    #[test]
    #[cfg(feature = "grpc-streaming")]
    fn transactions_by_slot_range_query_includes_bucket_predicate_for_shard_pruning() {
        let query = build_transactions_by_slot_range_query(
            "default.transactions",
            SLOT_SHARD_DIVISOR + 1,
            SLOT_SHARD_DIVISOR + 10,
            "",
        );
        let sql = normalize_sql(&query);

        assert!(sql.contains("intDiv(slot, 432000) BETWEEN 1 AND 1"));
        assert!(sql.contains("AND slot BETWEEN 432001 AND 432010"));
    }

    #[test]
    fn transactions_for_address_query_uses_bare_slot_filter_bounds() {
        let query = TransactionsForAddressQuery {
            address: bs58::encode([9_u8; 32]).into_string(),
            limit: 100,
            sort_order: SortOrder::Asc,
            pagination: None,
            resolved_pagination: None,
            slot_filter: Some(NumericFilter {
                gte: Some(420_050_657),
                lte: Some(420_051_754),
                ..NumericFilter::default()
            }),
            block_time_filter: Some(NumericFilter {
                gte: Some(1_778_907_063),
                lte: Some(1_778_907_499),
                ..NumericFilter::default()
            }),
            signature_filter: None,
            resolved_signature_filter: None,
            status: TransactionStatusFilter::Any,
            token_accounts: TokenAccountsFilter::None,
        };

        let tables = TransactionsForAddressTables {
            cache_partition: None,
            gsfa_table: "default.gsfa",
            gsfa_bucket_modulus: 32,
            token_owner_table: "default.token_owner_activity",
            token_owner_bucket_modulus: 32,
            signatures_table: "default.signatures",
            signature_bucket_modulus: 32,
            union_pushdown: true,
        };

        let sql = build_transactions_for_address_query(&tables, &query, "").expect("query");
        let sql = normalize_sql(&sql);

        assert!(sql.contains(
            "WHERE slot >= 420050657 AND slot <= 420051754 AND block_time >= 1778907063 AND block_time <= 1778907499"
        ));
        assert!(!sql.contains("slot + toUInt64(0)"));
    }

    #[test]
    fn transactions_for_address_query_uses_raw_cursor_and_no_limit_by() {
        let query = TransactionsForAddressQuery {
            address: bs58::encode([9_u8; 32]).into_string(),
            limit: 64,
            sort_order: SortOrder::Desc,
            pagination: Some(crate::clickhouse::PaginationToken::SlotIndex {
                slot: 436_663_495,
                idx: 1_387,
            }),
            resolved_pagination: Some(SignatureSlot {
                slot: 436_663_495,
                slot_idx: 1_387,
            }),
            slot_filter: None,
            block_time_filter: None,
            signature_filter: None,
            resolved_signature_filter: None,
            status: TransactionStatusFilter::Any,
            token_accounts: TokenAccountsFilter::None,
        };
        let tables = TransactionsForAddressTables {
            cache_partition: None,
            gsfa_table: "default.gsfa_local",
            gsfa_bucket_modulus: 32,
            token_owner_table: "default.token_owner_activity_local",
            token_owner_bucket_modulus: 32,
            signatures_table: "default.signatures",
            signature_bucket_modulus: 32,
            union_pushdown: true,
        };

        let sql = normalize_sql(
            &build_transactions_for_address_query(&tables, &query, "").expect("query"),
        );

        assert!(sql.contains("WHERE (slot < 436663495 OR (slot = 436663495 AND slot_idx < 1387))"));
        assert!(!sql.contains("LIMIT BY"));
        assert!(sql.contains("ORDER BY slot DESC, slot_idx DESC, signature DESC LIMIT 64"));
        assert!(!sql.contains("FROM ( SELECT signature"));
    }

    #[test]
    fn resolved_bounds_take_precedence_and_refills_never_repeat_scalar_lookups() {
        let position = SignatureSlot {
            slot: 42,
            slot_idx: 7,
        };
        let mut query = TransactionsForAddressQuery {
            address: bs58::encode([9_u8; 32]).into_string(),
            limit: 64,
            sort_order: SortOrder::Desc,
            pagination: Some(super::PaginationToken::SlotIndex { slot: 42, idx: 9 }),
            resolved_pagination: Some(SignatureSlot {
                slot: 42,
                slot_idx: 9,
            }),
            slot_filter: None,
            block_time_filter: None,
            // Invalid text deliberately verifies no signature decoding or scalar SQL remains.
            signature_filter: Some(SignatureFilter {
                gte: Some("already resolved".into()),
                gt: Some("already resolved".into()),
                lte: Some("already resolved".into()),
                lt: Some("already resolved".into()),
            }),
            resolved_signature_filter: Some(ResolvedSignatureFilter {
                gte: Some(position),
                gt: Some(position),
                lte: Some(position),
                lt: Some(position),
            }),
            status: TransactionStatusFilter::Any,
            token_accounts: TokenAccountsFilter::None,
        };
        let tables = TransactionsForAddressTables {
            cache_partition: None,
            gsfa_table: "default.gsfa_local",
            gsfa_bucket_modulus: 32,
            token_owner_table: "default.token_owner_activity_local",
            token_owner_bucket_modulus: 32,
            signatures_table: "default.signatures",
            signature_bucket_modulus: 32,
            union_pushdown: true,
        };
        for sort_order in [SortOrder::Asc, SortOrder::Desc] {
            query.sort_order = sort_order;
            for idx in [9, 8] {
                query.pagination = Some(super::PaginationToken::SlotIndex { slot: 42, idx });
                let sql = build_transactions_for_address_query(&tables, &query, "")
                    .expect("resolved query");
                assert!(!sql.contains("FROM default.signatures"));
                assert!(!sql.contains("SELECT CAST"));
                for operator in [">=", ">", "<=", "<"] {
                    assert!(sql.contains(&format!("slot_idx {operator} 7")));
                }
                assert!(sql.contains("slot = 42"));
            }
        }
    }

    #[test]
    fn transactions_for_address_query_applies_pre_resolved_signature_bounds() {
        let query = TransactionsForAddressQuery {
            address: bs58::encode([9_u8; 32]).into_string(),
            limit: 64,
            sort_order: SortOrder::Desc,
            pagination: None,
            resolved_pagination: None,
            slot_filter: None,
            block_time_filter: None,
            signature_filter: None,
            resolved_signature_filter: Some(ResolvedSignatureFilter {
                gte: Some(SignatureSlot {
                    slot: 436_663_495,
                    slot_idx: 1_387,
                }),
                ..ResolvedSignatureFilter::default()
            }),
            status: TransactionStatusFilter::Any,
            token_accounts: TokenAccountsFilter::None,
        };
        let tables = TransactionsForAddressTables {
            cache_partition: None,
            gsfa_table: "cache.gsfa",
            gsfa_bucket_modulus: 32,
            token_owner_table: "cache.token_owner_activity",
            token_owner_bucket_modulus: 32,
            signatures_table: "cache.signatures",
            signature_bucket_modulus: 32,
            union_pushdown: true,
        };

        let sql = normalize_sql(
            &build_transactions_for_address_query(&tables, &query, "").expect("query"),
        );

        assert!(
            sql.contains("WHERE (slot > 436663495 OR (slot = 436663495 AND slot_idx >= 1387))")
        );
        assert!(!sql.contains("FROM cache.signatures"));
    }

    #[test]
    fn transactions_for_address_ascending_cursor_preserves_bare_key_predicate() {
        let query = TransactionsForAddressQuery {
            address: bs58::encode([9_u8; 32]).into_string(),
            limit: 64,
            sort_order: SortOrder::Asc,
            pagination: Some(crate::clickhouse::PaginationToken::SlotIndex {
                slot: 436_663_495,
                idx: 1_387,
            }),
            resolved_pagination: Some(SignatureSlot {
                slot: 436_663_495,
                slot_idx: 1_387,
            }),
            slot_filter: None,
            block_time_filter: None,
            signature_filter: None,
            resolved_signature_filter: None,
            status: TransactionStatusFilter::Any,
            token_accounts: TokenAccountsFilter::None,
        };
        let tables = TransactionsForAddressTables {
            cache_partition: None,
            gsfa_table: "default.gsfa_local",
            gsfa_bucket_modulus: 32,
            token_owner_table: "default.token_owner_activity_local",
            token_owner_bucket_modulus: 32,
            signatures_table: "default.signatures",
            signature_bucket_modulus: 32,
            union_pushdown: true,
        };

        let sql = normalize_sql(
            &build_transactions_for_address_query(&tables, &query, "").expect("query"),
        );

        assert!(sql.contains("WHERE (slot > 436663495 OR (slot = 436663495 AND slot_idx > 1387))"));
        assert!(sql.contains("ORDER BY slot ASC, slot_idx ASC, signature ASC LIMIT 64"));
    }

    #[test]
    fn hot_transactions_for_address_query_uses_resolved_numeric_bounds() {
        let query = TransactionsForAddressQuery {
            address: bs58::encode([9_u8; 32]).into_string(),
            limit: 50,
            sort_order: SortOrder::Desc,
            pagination: Some(crate::clickhouse::PaginationToken::Signature(
                "ignored".to_string(),
            )),
            resolved_pagination: Some(SignatureSlot {
                slot: 400_179_920,
                slot_idx: 678,
            }),
            slot_filter: None,
            block_time_filter: None,
            signature_filter: Some(SignatureFilter {
                gte: Some("ignored".to_string()),
                ..SignatureFilter::default()
            }),
            resolved_signature_filter: Some(ResolvedSignatureFilter {
                gte: Some(SignatureSlot {
                    slot: 400_179_100,
                    slot_idx: 42,
                }),
                ..ResolvedSignatureFilter::default()
            }),
            status: TransactionStatusFilter::Any,
            token_accounts: TokenAccountsFilter::None,
        };

        let sql = build_transactions_for_address_hot_query(
            "default.gsfa_hot_local",
            32,
            &query,
            "SETTINGS use_query_cache=1",
        )
        .expect("query");
        let sql = normalize_sql(&sql);

        assert!(sql.contains("FROM default.gsfa_hot_local"));
        assert!(sql.contains("(slot > 400179100 OR (slot = 400179100 AND slot_idx >= 42))"));
        assert!(sql.contains("(slot < 400179920 OR (slot = 400179920 AND slot_idx < 678))"));
        assert!(!sql.contains("slot + toUInt64(0) > 400179100"));
        assert!(!sql.contains("slot_idx + toUInt32(0) < 678"));
        assert!(!sql.contains("FROM default.signatures"));
        assert!(!sql.contains("ignored"));
        assert!(!sql.contains("LIMIT BY"));
    }

    #[test]
    fn hot_position_pagination_clauses_use_raw_reverse_key_predicates() {
        let (_, where_clause) = build_hot_position_pagination_clauses(
            Some(SlotBoundary::Position(SignatureSlot {
                slot: 400_179_920,
                slot_idx: 678,
            })),
            Some(SlotBoundary::Position(SignatureSlot {
                slot: 400_179_100,
                slot_idx: 42,
            })),
        );

        assert_eq!(
            where_clause,
            "(slot < 400179920 OR (slot = 400179920 AND slot_idx < 678)) AND (slot > 400179100 OR (slot = 400179100 AND slot_idx > 42))"
        );
    }

    #[test]
    fn hot_transactions_for_address_query_uses_raw_slot_filter_bounds() {
        let query = TransactionsForAddressQuery {
            address: bs58::encode([9_u8; 32]).into_string(),
            limit: 100,
            sort_order: SortOrder::Asc,
            pagination: None,
            resolved_pagination: None,
            slot_filter: Some(NumericFilter {
                lte: Some(420_051_754),
                ..NumericFilter::default()
            }),
            block_time_filter: None,
            signature_filter: None,
            resolved_signature_filter: None,
            status: TransactionStatusFilter::Any,
            token_accounts: TokenAccountsFilter::None,
        };

        let sql =
            build_transactions_for_address_hot_query("default.gsfa_hot_local", 32, &query, "")
                .expect("query");
        let sql = normalize_sql(&sql);

        assert!(sql.contains("WHERE slot <= 420051754"));
        assert!(!sql.contains("slot + toUInt64(0) <= 420051754"));
    }
}
