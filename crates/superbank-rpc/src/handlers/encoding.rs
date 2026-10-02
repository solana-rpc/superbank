// SPDX-License-Identifier: AGPL-3.0-only

use solana_transaction_status::UiTransactionEncoding;

/// Agave validates this combination before looking up any block or transaction.
pub(crate) fn validate_transaction_encoding(
    encoding: UiTransactionEncoding,
    max_version: Option<u8>,
) -> Result<(), &'static str> {
    if matches!(
        encoding,
        UiTransactionEncoding::Binary | UiTransactionEncoding::Base58
    ) && max_version.is_some_and(|version| version >= 1)
    {
        return Err("base58 encoding is not supported with maxSupportedTransactionVersion >= 1");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agave_encoding_version_matrix() {
        for encoding in [UiTransactionEncoding::Binary, UiTransactionEncoding::Base58] {
            for version in [None, Some(0)] {
                assert!(validate_transaction_encoding(encoding, version).is_ok());
            }
            for version in [1, u8::MAX] {
                assert_eq!(
                    validate_transaction_encoding(encoding, Some(version)),
                    Err(
                        "base58 encoding is not supported with maxSupportedTransactionVersion >= 1"
                    )
                );
            }
        }
        for encoding in [
            UiTransactionEncoding::Json,
            UiTransactionEncoding::JsonParsed,
            UiTransactionEncoding::Base64,
        ] {
            assert!(validate_transaction_encoding(encoding, Some(1)).is_ok());
        }
    }
}
