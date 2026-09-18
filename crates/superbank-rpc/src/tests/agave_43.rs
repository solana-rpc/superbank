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

#[test]
fn confidential_supply_rotation_uses_context_account_key() {
    use solana_message::{AccountKeys, compiled_instruction::CompiledInstruction};
    use solana_transaction_status::parse_token::parse_token;
    let keys: Vec<_> = (1..=4)
        .map(|byte| solana_sdk::pubkey::Pubkey::from([byte; 32]))
        .collect();
    // Token-2022 extension 42, RotateSupplyElGamalPubkey (1), a 32-byte
    // ElGamal key, and a zero proof offset selecting the context-state account.
    let mut data = vec![42, 1];
    data.extend([0; 33]);
    let instruction = CompiledInstruction {
        program_id_index: 3,
        accounts: vec![0, 1, 2],
        data,
    };
    let parsed = parse_token(&instruction, &AccountKeys::new(&keys, None)).unwrap();
    assert_eq!(parsed.info["proofContextStateAccount"], keys[1].to_string());
    assert!(parsed.info.get("proofAccount").is_none());
}

#[test]
fn confidential_empty_account_does_not_mistake_multisig_owner_for_record() {
    use solana_message::{AccountKeys, compiled_instruction::CompiledInstruction};
    use solana_transaction_status::parse_token::parse_token;
    let keys: Vec<_> = (1..=5)
        .map(|byte| solana_sdk::pubkey::Pubkey::from([byte; 32]))
        .collect();
    // ConfidentialTransfer (27), EmptyAccount (4), in-transaction proof (1).
    let instruction = CompiledInstruction {
        program_id_index: 4,
        accounts: vec![0, 1, 2, 3],
        data: vec![27, 4, 1],
    };
    let parsed = parse_token(&instruction, &AccountKeys::new(&keys, None)).unwrap();
    assert_eq!(parsed.info["instructionsSysvar"], keys[1].to_string());
    assert_eq!(parsed.info["multisigOwner"], keys[2].to_string());
    assert_eq!(parsed.info["signers"], json!([keys[3].to_string()]));
    assert!(parsed.info.get("recordAccount").is_none());
}

#[test]
fn confidential_fee_withdrawal_preserves_multisig_authority() {
    use solana_message::{AccountKeys, compiled_instruction::CompiledInstruction};
    use solana_transaction_status::parse_token::parse_token;
    let keys: Vec<_> = (1..=6)
        .map(|byte| solana_sdk::pubkey::Pubkey::from([byte; 32]))
        .collect();
    // ConfidentialTransferFee (37), WithdrawWithheldTokensFromMint (1),
    // in-transaction proof (1), and a 36-byte decryptable balance.
    let mut data = vec![37, 1, 1];
    data.extend([0; 36]);
    let instruction = CompiledInstruction {
        program_id_index: 5,
        accounts: vec![0, 1, 2, 3, 4],
        data,
    };
    let parsed = parse_token(&instruction, &AccountKeys::new(&keys, None)).unwrap();
    assert_eq!(parsed.info["instructionsSysvar"], keys[2].to_string());
    assert_eq!(
        parsed.info["multisigWithdrawWithheldAuthority"],
        keys[3].to_string()
    );
    assert_eq!(parsed.info["signers"], json!([keys[4].to_string()]));
    assert!(parsed.info.get("recordAccount").is_none());
}

#[test]
fn permissioned_burn_mixed_proofs_preserve_trailing_authorities() {
    use solana_message::{AccountKeys, compiled_instruction::CompiledInstruction};
    use solana_transaction_status::parse_token::parse_token;
    let keys: Vec<_> = (1..=7)
        .map(|byte| solana_sdk::pubkey::Pubkey::from([byte; 32]))
        .collect();
    // PermissionedBurn (46), ConfidentialBurn (3), decryptable balance (36),
    // two ElGamal ciphertexts (64 each), then equality/validity/range offsets.
    let mut data = vec![46, 3];
    data.extend([0; 164]);
    data.extend([1, 1, 0]);
    let instruction = CompiledInstruction {
        program_id_index: 6,
        accounts: vec![0, 1, 2, 3, 4, 5],
        data,
    };
    let parsed = parse_token(&instruction, &AccountKeys::new(&keys, None)).unwrap();
    assert_eq!(parsed.info["instructionsSysvar"], keys[2].to_string());
    assert_eq!(
        parsed.info["rangeProofContextStateAccount"],
        keys[3].to_string()
    );
    assert_eq!(
        parsed.info["permissionedBurnAuthority"],
        keys[4].to_string()
    );
    assert_eq!(parsed.info["authority"], keys[5].to_string());
    assert!(
        parsed
            .info
            .get("equalityProofContextStateAccount")
            .is_none()
    );
}
