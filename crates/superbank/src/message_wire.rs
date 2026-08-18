// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use solana_message::VersionedMessage;
#[cfg(test)]
use solana_transaction::versioned;
use wincode::WriteResult;

pub(crate) fn serialize_versioned_message(message: &VersionedMessage) -> WriteResult<Vec<u8>> {
    Ok(message.serialize())
}

#[cfg(test)]
pub(crate) fn serialize_versioned_transaction(
    transaction: &versioned::VersionedTransaction,
) -> wincode05::WriteResult<Vec<u8>> {
    wincode05::serialize(transaction)
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_message::{
        Address, Hash, MESSAGE_VERSION_PREFIX, MessageHeader, VersionedMessage,
        compiled_instruction::CompiledInstruction,
        legacy::Message,
        v0::{Message as V0Message, MessageAddressTableLookup},
    };

    #[test]
    fn serializes_legacy_message_with_solana_short_vec_lengths() {
        let message = VersionedMessage::Legacy(Message {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![Address::from([1; 32]), Address::from([2; 32])],
            recent_blockhash: Hash::default(),
            instructions: vec![CompiledInstruction {
                program_id_index: 1,
                accounts: vec![0],
                data: vec![1, 2, 3],
            }],
        });

        let mut expected = vec![1, 0, 0, 2];
        expected.extend([1; 32]);
        expected.extend([2; 32]);
        expected.extend([0; 32]);
        expected.extend([1, 1, 1, 0, 3, 1, 2, 3]);

        assert_eq!(serialize_versioned_message(&message).unwrap(), expected);
    }

    #[test]
    fn serializes_v0_message_with_version_prefix_and_lookup_tables() {
        let message = VersionedMessage::V0(V0Message {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![Address::from([1; 32]), Address::from([2; 32])],
            recent_blockhash: Hash::new_from_array([9; 32]),
            instructions: vec![CompiledInstruction {
                program_id_index: 1,
                accounts: vec![0],
                data: vec![7, 8],
            }],
            address_table_lookups: vec![MessageAddressTableLookup {
                account_key: Address::from([3; 32]),
                writable_indexes: vec![4, 5],
                readonly_indexes: vec![6],
            }],
        });

        let mut expected = vec![MESSAGE_VERSION_PREFIX, 1, 0, 1, 2];
        expected.extend([1; 32]);
        expected.extend([2; 32]);
        expected.extend([9; 32]);
        expected.extend([1, 1, 1, 0, 2, 7, 8, 1]);
        expected.extend([3; 32]);
        expected.extend([2, 4, 5, 1, 6]);

        assert_eq!(serialize_versioned_message(&message).unwrap(), expected);
    }

    #[test]
    fn serializes_v1_message_with_version_prefix_and_all_config_fields() {
        let message = VersionedMessage::V1(solana_message::v1::Message {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            config: solana_message::v1::TransactionConfig {
                priority_fee: Some(42),
                compute_unit_limit: Some(1_000_000),
                loaded_accounts_data_size_limit: Some(65_536),
                heap_size: Some(32_768),
            },
            lifetime_specifier: Hash::new_from_array([9; 32]),
            account_keys: vec![Address::from([1; 32])],
            instructions: Vec::new(),
        });

        let bytes = serialize_versioned_message(&message).unwrap();
        assert_eq!(bytes[0], solana_message::v1::V1_PREFIX);
        assert_eq!(bytes, message.serialize());
    }
}
