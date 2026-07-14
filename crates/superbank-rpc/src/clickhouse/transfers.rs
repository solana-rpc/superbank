// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::str::FromStr;
use std::time::Instant;

use serde::Deserialize;
use solana_sdk::pubkey::Pubkey;

use crate::processing::{ProcessingError, ProcessingResult};

use super::QueryFreshnessClass;
use super::client::ClickHouseClient;
use super::types::{
    NumericFilter, QueryTimings, SolMode, SortOrder, TransferDirectionFilter, TransferRecord,
    TransfersByAddressQuery,
};
use super::util::pubkey_literal;

const NATIVE_SOL_MINT: &str = "So11111111111111111111111111111111111111111";
const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";

#[derive(Deserialize, clickhouse::Row)]
struct TransferQueryRow {
    signature: String,
    slot: u64,
    slot_idx: u32,
    transfer_idx: u32,
    inner_instruction_idx: u32,
    block_time: Option<i64>,
    transfer_type: String,
    amount: String,
    mint: Option<String>,
    decimals: Option<u8>,
    from_user_account: Option<String>,
    to_user_account: Option<String>,
    from_token_account: Option<String>,
    to_token_account: Option<String>,
}

fn map_transfer_row(row: TransferQueryRow) -> TransferRecord {
    TransferRecord {
        signature: row.signature,
        slot: row.slot,
        slot_idx: row.slot_idx,
        transfer_idx: row.transfer_idx,
        inner_instruction_idx: row.inner_instruction_idx,
        block_time: row.block_time,
        transfer_type: row.transfer_type,
        amount: row.amount,
        mint: row.mint,
        decimals: row.decimals,
        from_user_account: row.from_user_account,
        to_user_account: row.to_user_account,
        from_token_account: row.from_token_account,
        to_token_account: row.to_token_account,
    }
}

fn append_numeric_filter_conditions<T: std::fmt::Display>(
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

fn append_pubkey_filter(
    column: &str,
    value: &str,
    conditions: &mut Vec<String>,
) -> ProcessingResult<()> {
    let pubkey = Pubkey::from_str(value)
        .map_err(|e| ProcessingError::deserialization("Invalid pubkey filter", e))?;
    conditions.push(format!("{column} = {}", pubkey_literal(&pubkey)));
    Ok(())
}

fn pubkey_literal_from_str(value: &str) -> ProcessingResult<String> {
    let pubkey = Pubkey::from_str(value)
        .map_err(|e| ProcessingError::deserialization("Invalid pubkey filter", e))?;
    Ok(pubkey_literal(&pubkey))
}

fn append_pagination_condition(query: &TransfersByAddressQuery, conditions: &mut Vec<String>) {
    let Some(token) = &query.pagination else {
        return;
    };
    let op = match query.sort_order {
        SortOrder::Desc => "<",
        SortOrder::Asc => ">",
    };
    conditions.push(format!(
        "(slot, slot_idx, transfer_idx, inner_instruction_idx, transfer_type) {op} ({}, {}, {}, {}, '{}')",
        token.slot,
        token.slot_idx,
        token.transfer_idx,
        token.inner_instruction_idx,
        <&'static str>::from(token.transfer_type)
    ));
}

fn transfer_select_list(table_alias: &str) -> String {
    format!(
        "base58Encode({table_alias}.signature) AS signature,
            {table_alias}.slot,
            {table_alias}.slot_idx,
            {table_alias}.transfer_idx,
            {table_alias}.inner_instruction_idx,
            {table_alias}.block_time,
            {table_alias}.transfer_type,
            {table_alias}.amount,
            if(isNull({table_alias}.mint), NULL, base58Encode(assumeNotNull({table_alias}.mint))) AS mint,
            {table_alias}.decimals,
            if(isNull({table_alias}.from_user_account), NULL, base58Encode(assumeNotNull({table_alias}.from_user_account))) AS from_user_account,
            if(isNull({table_alias}.to_user_account), NULL, base58Encode(assumeNotNull({table_alias}.to_user_account))) AS to_user_account,
            if(isNull({table_alias}.from_token_account), NULL, base58Encode(assumeNotNull({table_alias}.from_token_account))) AS from_token_account,
            if(isNull({table_alias}.to_token_account), NULL, base58Encode(assumeNotNull({table_alias}.to_token_account))) AS to_token_account"
    )
}

fn append_shared_filter_conditions(
    table_alias: &str,
    query: &TransfersByAddressQuery,
    conditions: &mut Vec<String>,
) -> ProcessingResult<()> {
    append_pagination_condition(query, conditions);
    if let Some(filter) = &query.amount_filter {
        append_numeric_filter_conditions(
            &format!("toFloat64OrZero({table_alias}.amount)"),
            filter,
            conditions,
        );
    }
    if let Some(filter) = &query.slot_filter {
        append_numeric_filter_conditions(&format!("{table_alias}.slot"), filter, conditions);
    }
    if let Some(filter) = &query.block_time_filter {
        append_numeric_filter_conditions(&format!("{table_alias}.block_time"), filter, conditions);
    }
    if query.sol_mode == SolMode::Merged {
        let native_sol_mint = pubkey_literal_from_str(NATIVE_SOL_MINT)?;
        conditions.push(format!(
            "({table_alias}.transfer_type != 'wrap' AND ({table_alias}.transfer_type != 'closeAccount' OR {table_alias}.mint = {native_sol_mint}))"
        ));
    }
    if let Some(mint) = &query.mint {
        if query.sol_mode == SolMode::Merged && (mint == NATIVE_SOL_MINT || mint == WSOL_MINT) {
            conditions.push(format!(
                "{table_alias}.mint IN ({}, {})",
                pubkey_literal_from_str(NATIVE_SOL_MINT)?,
                pubkey_literal_from_str(WSOL_MINT)?
            ));
        } else {
            append_pubkey_filter(&format!("{table_alias}.mint"), mint, conditions)?;
        }
    }
    Ok(())
}

fn build_transfer_branch(
    table: &str,
    table_alias: &str,
    address_literal: &str,
    user_column: &str,
    counterparty_user_column: Option<&str>,
    query: &TransfersByAddressQuery,
) -> ProcessingResult<String> {
    let mut conditions = vec!["1".to_string()];
    conditions.push(format!("{table_alias}.{user_column} = {address_literal}"));
    if let (Some(counterparty_column), Some(with_account)) =
        (counterparty_user_column, query.with_account.as_deref())
    {
        append_pubkey_filter(
            &format!("{table_alias}.{counterparty_column}"),
            with_account,
            &mut conditions,
        )?;
    }
    append_shared_filter_conditions(table_alias, query, &mut conditions)?;
    let where_clause = conditions.join(" AND ");
    Ok(format!(
        "SELECT
            {select_list}
         FROM {table} AS {table_alias}
         WHERE {where_clause}",
        select_list = transfer_select_list(table_alias),
        table = table,
        table_alias = table_alias,
        where_clause = where_clause,
    ))
}

fn build_transfers_by_address_query(
    table: &str,
    query: &TransfersByAddressQuery,
    settings_clause: &str,
) -> ProcessingResult<String> {
    let address = Pubkey::from_str(&query.address)
        .map_err(|e| ProcessingError::deserialization("Invalid address", e))?;
    let address_literal = pubkey_literal(&address);

    let from_branch = build_transfer_branch(
        table,
        "from_t",
        &address_literal,
        "from_user_account",
        Some("to_user_account"),
        query,
    )?;
    let to_branch = build_transfer_branch(
        table,
        "to_t",
        &address_literal,
        "to_user_account",
        Some("from_user_account"),
        query,
    )?;

    let body = match query.direction {
        TransferDirectionFilter::Out => from_branch,
        TransferDirectionFilter::In => to_branch,
        TransferDirectionFilter::Any => format!("{from_branch}\nUNION DISTINCT\n{to_branch}"),
    };

    let order = match query.sort_order {
        SortOrder::Desc => "DESC",
        SortOrder::Asc => "ASC",
    };

    Ok(format!(
        "SELECT *
         FROM (
            {body}
         )
         ORDER BY slot {order}, slot_idx {order}, transfer_idx {order}, inner_instruction_idx {order}, transfer_type {order}
         LIMIT {limit}
         {settings_clause}",
        body = body,
        order = order,
        limit = query.limit.saturating_add(1),
        settings_clause = settings_clause,
    ))
}

impl ClickHouseClient {
    pub async fn get_transfers_by_address(
        &self,
        query: &TransfersByAddressQuery,
    ) -> ProcessingResult<(Vec<TransferRecord>, QueryTimings)> {
        self.with_timeout("get_transfers_by_address", async {
            if !self.transfers_available {
                return Err(ProcessingError::database_msg(format!(
                    "getTransfersByAddress requires transfers table '{}'",
                    self.transfers_table
                )));
            }

            let settings_clause = self.select_settings_clause_with_condition_cache(
                "get_transfers_by_address",
                QueryFreshnessClass::Historical,
            );
            let query_sql =
                build_transfers_by_address_query(&self.transfers_table, query, &settings_clause)?;

            let start = Instant::now();
            let mut cursor = self
                .client
                .query(&query_sql)
                .fetch::<TransferQueryRow>()
                .map_err(|e| ProcessingError::database(e.to_string(), e))?;

            let mut results = Vec::new();
            while let Some(row) = cursor
                .next()
                .await
                .map_err(|e| ProcessingError::database(e.to_string(), e))?
            {
                results.push(row);
            }

            let records = results
                .into_iter()
                .map(map_transfer_row)
                .collect::<Vec<_>>();
            let timings = QueryTimings {
                elapsed_ms: start.elapsed().as_millis() as u64,
                received_bytes: cursor.received_bytes(),
                decoded_bytes: cursor.decoded_bytes(),
                rows_read: Some(0),
                rows_read_unknown: true,
                rows_returned: records.len() as u64,
            };

            Ok((records, timings))
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_query(sol_mode: SolMode) -> TransfersByAddressQuery {
        TransfersByAddressQuery {
            address: "11111111111111111111111111111111".to_string(),
            limit: 100,
            sort_order: SortOrder::Desc,
            sol_mode,
            pagination: None,
            amount_filter: None,
            slot_filter: None,
            block_time_filter: None,
            direction: TransferDirectionFilter::Any,
            mint: None,
            with_account: None,
        }
    }

    #[test]
    fn with_account_filter_applies_to_both_union_branches() {
        let mut query = base_query(SolMode::Merged);
        query.with_account = Some("9vR6ssB1BdzhAgVEoQoeZdbsqknBA2PkFYDsjvbAS5jP".to_string());

        let sql = build_transfers_by_address_query("default.transfers", &query, "")
            .expect("query should build");

        assert!(sql.contains("from_t.to_user_account = "));
        assert!(sql.contains("to_t.from_user_account = "));
        assert!(sql.contains("UNION DISTINCT"));
        assert!(sql.contains("FROM default.transfers AS from_t"));
        assert!(sql.contains("FROM default.transfers AS to_t"));
    }

    #[test]
    fn out_direction_uses_from_branch_only() {
        let mut query = base_query(SolMode::Merged);
        query.direction = TransferDirectionFilter::Out;

        let sql = build_transfers_by_address_query("default.transfers", &query, "")
            .expect("query should build");

        assert!(sql.contains("from_t.from_user_account = "));
        assert!(!sql.contains("UNION ALL"));
        assert!(!sql.contains("to_t."));
    }

    #[test]
    fn in_direction_uses_to_branch_only() {
        let mut query = base_query(SolMode::Merged);
        query.direction = TransferDirectionFilter::In;

        let sql = build_transfers_by_address_query("default.transfers", &query, "")
            .expect("query should build");

        assert!(sql.contains("to_t.to_user_account = "));
        assert!(!sql.contains("UNION ALL"));
        assert!(!sql.contains("from_t."));
    }

    #[test]
    fn merged_sol_mode_excludes_wrap_and_wsol_close_account_only() {
        let query = base_query(SolMode::Merged);
        let sql = build_transfers_by_address_query("default.transfers", &query, "")
            .expect("query should build");

        assert!(sql.contains("transfer_type != 'wrap'"));
        assert!(sql.contains("transfer_type != 'closeAccount' OR"));
        assert!(sql.contains(&pubkey_literal_from_str(NATIVE_SOL_MINT).unwrap()));
        assert!(!sql.contains("transfer_type NOT IN ('wrap', 'closeAccount')"));
    }

    #[test]
    fn separate_sol_mode_keeps_lifecycle_rows() {
        let query = base_query(SolMode::Separate);
        let sql = build_transfers_by_address_query("default.transfers", &query, "")
            .expect("query should build");

        assert!(!sql.contains("transfer_type NOT IN ('wrap', 'closeAccount')"));
    }

    #[test]
    fn merged_sol_mode_matches_native_and_wrapped_sol_mints() {
        let mut query = base_query(SolMode::Merged);
        query.mint = Some(NATIVE_SOL_MINT.to_string());

        let sql = build_transfers_by_address_query("default.transfers", &query, "")
            .expect("query should build");

        assert!(sql.contains(".mint IN ("));
        assert!(sql.contains(&pubkey_literal_from_str(NATIVE_SOL_MINT).unwrap()));
        assert!(sql.contains(&pubkey_literal_from_str(WSOL_MINT).unwrap()));
    }

    #[test]
    fn pagination_condition_uses_helius_cursor_shape() {
        let mut query = base_query(SolMode::Separate);
        query.pagination = Some(super::super::TransferPositionFilter {
            slot: 315073428,
            slot_idx: 35,
            transfer_idx: 1,
            inner_instruction_idx: 0,
            transfer_type: super::super::TokenTransferTypes::Transfer,
        });

        let sql = build_transfers_by_address_query("default.transfers", &query, "")
            .expect("query should build");

        assert!(sql.contains(
            "(slot, slot_idx, transfer_idx, inner_instruction_idx, transfer_type) < (315073428, 35, 1, 0, 'transfer')"
        ));
        assert!(sql.contains(
            "ORDER BY slot DESC, slot_idx DESC, transfer_idx DESC, inner_instruction_idx DESC, transfer_type DESC"
        ));
    }

    #[test]
    fn query_fetches_one_extra_row_to_detect_next_page() {
        let mut query = base_query(SolMode::Separate);
        query.limit = 7;

        let sql = build_transfers_by_address_query("default.transfers", &query, "")
            .expect("query should build");

        assert!(sql.contains("LIMIT 8"));
    }
}
