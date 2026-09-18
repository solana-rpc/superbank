// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::handlers::transactions::handle_get_transaction;

const ENCODING_ERROR: &str =
    "base58 encoding is not supported with maxSupportedTransactionVersion >= 1";

async fn assert_invalid_params(response: Response, message: &str) {
    assert_eq!(response.status(), StatusCode::OK);
    let body = parse_json_rpc_response(response).await;
    let error = body.error.expect("invalid params");
    assert_eq!(error.code, -32602);
    assert_eq!(error.message, message);
    assert!(error.data.is_none());
}

#[tokio::test]
async fn encoding_version_is_rejected_before_missing_transaction_lookup() {
    for encoding in ["binary", "base58"] {
        for version in [1, 255] {
            let response = handle_get_transaction(
                test_state(),
                json!(1),
                Some(vec![
                    json!(solana_sdk::signature::Signature::from([7; 64]).to_string()),
                    json!({"encoding": encoding, "maxSupportedTransactionVersion": version}),
                ]),
            )
            .await
            .unwrap();
            assert_invalid_params(response, ENCODING_ERROR).await;
        }
    }
}

#[tokio::test]
async fn encoding_version_is_rejected_before_every_block_projection() {
    for details in ["full", "accounts", "signatures", "none"] {
        for encoding in ["binary", "base58"] {
            let response = handle_get_block(
                test_state(),
                json!(1),
                Some(vec![
                    json!(42),
                    json!({"encoding": encoding, "maxSupportedTransactionVersion": 1,
                    "transactionDetails": details}),
                ]),
            )
            .await
            .unwrap();
            assert_invalid_params(response, ENCODING_ERROR).await;
        }
    }
}

#[tokio::test]
async fn custom_address_method_rejects_version_encoding_before_lookup() {
    for details in ["signatures", "full"] {
        let response = handle_get_transactions_for_address(
            test_state(),
            json!(1),
            Some(vec![
                json!("11111111111111111111111111111111"),
                json!({"encoding": "base58", "maxSupportedTransactionVersion": 1,
                "transactionDetails": details}),
            ]),
        )
        .await
        .unwrap();
        assert_invalid_params(response, ENCODING_ERROR).await;
    }
}

#[tokio::test]
async fn inflation_limit_uses_configured_agave_error_before_address_parsing() {
    for limit in [32, 100] {
        let mut state = test_state();
        Arc::get_mut(&mut state)
            .unwrap()
            .get_inflation_reward_max_addresses = Some(limit);
        let response = handle_get_inflation_reward(
            state,
            json!(1),
            Some(vec![json!(vec!["invalid"; limit + 1])]),
        )
        .await
        .unwrap();
        assert_invalid_params(response, &format!("Too many inputs provided; max {limit}")).await;
    }
}

#[tokio::test]
async fn inflation_boundary_and_disabled_limit_reach_address_validation() {
    for (limit, count) in [
        (Some(32), 31),
        (Some(32), 32),
        (Some(100), 100),
        (None, 101),
    ] {
        let mut state = test_state();
        Arc::get_mut(&mut state)
            .unwrap()
            .get_inflation_reward_max_addresses = limit;
        let response =
            handle_get_inflation_reward(state, json!(1), Some(vec![json!(vec!["invalid"; count])]))
                .await
                .unwrap();
        assert_invalid_params(response, "Invalid param: Invalid").await;
    }
}

#[test]
fn vat_debit_hydrates_both_producer_spellings_without_losing_negative_lamports() {
    for name in ["VATDebit", "validator-admission-ticket-debit"] {
        let mut block = base_block_record(77);
        block.metadata.rewards_present = true;
        block.metadata.rewards_pubkey = vec![[1; 32]];
        block.metadata.rewards_lamports = vec![-10];
        block.metadata.rewards_post_balance = vec![90];
        block.metadata.rewards_type = vec![Some(name.to_owned())];
        block.metadata.rewards_commission = vec![None];
        block.metadata.rewards_commission_bps = vec![None];
        let result = hydrate_block_record(
            block,
            UiTransactionEncoding::Json,
            TransactionDetails::None,
            true,
            Some(1),
        )
        .unwrap();
        let value = serde_json::to_value(result).unwrap();
        assert_eq!(value["rewards"][0]["rewardType"], "VATDebit");
        assert_eq!(value["rewards"][0]["lamports"], -10);
        assert_eq!(value["rewards"][0]["postBalance"], 90);
    }
}

