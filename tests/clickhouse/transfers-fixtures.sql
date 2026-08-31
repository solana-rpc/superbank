-- SPDX-License-Identifier: AGPL-3.0-only
--
-- Copyright 2025-2026 Triton One Limited. All rights reserved.
--

-- Fixture coverage for ddl/local/transfers.sql:
-- - exact amount larger than JavaScript's integer-safe range
-- - inner and outer instructions
-- - Token-2022 TransferCheckedWithFee
-- - WSOL CloseAccount
-- - failed transactions (which must not emit transfers)
-- - Token transfer metadata omitted by the upstream source

INSERT INTO default.transactions (
    signature,
    slot,
    slot_idx,
    message_hash,
    is_vote,
    tx_account_keys,
    tx_instructions_program_id_index,
    tx_instructions_accounts,
    tx_instructions_data,
    meta_status_ok
)
SELECT
    'fixture-outer-large',
    100,
    0,
    'fixture',
    0,
    [
        base58Decode('11111111111111111111111111111111'),
        base58Decode('HLnpSz9h2S4hiLQ43rnSD9XkcUThA7B8hQMKmDaiTLcC'),
        base58Decode('9vR6ssB1BdzhAgVEoQoeZdbsqknBA2PkFYDsjvbAS5jP')
    ],
    [toUInt8(0)],
    [[toUInt8(1), toUInt8(2)]],
    [concat(unhex('02000000'), toFixedString(reinterpretAsString(toUInt64(9007199254740993)), 8))],
    1;

INSERT INTO default.transactions (
    signature,
    slot,
    slot_idx,
    message_hash,
    is_vote,
    tx_account_keys,
    tx_instructions_program_id_index,
    tx_instructions_accounts,
    tx_instructions_data,
    meta_status_ok,
    meta_inner_instructions_present,
    meta_inner_instructions_index,
    meta_inner_instructions_program_id_index,
    meta_inner_instructions_accounts,
    meta_inner_instructions_data
)
SELECT
    'fixture-inner',
    101,
    0,
    'fixture',
    0,
    [
        base58Decode('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'),
        base58Decode('HLnpSz9h2S4hiLQ43rnSD9XkcUThA7B8hQMKmDaiTLcC'),
        base58Decode('9vR6ssB1BdzhAgVEoQoeZdbsqknBA2PkFYDsjvbAS5jP'),
        base58Decode('11111111111111111111111111111111')
    ],
    [toUInt8(0)],
    [[toUInt8(1), toUInt8(2)]],
    [unhex('00')],
    1,
    1,
    [toUInt8(0)],
    [[toUInt8(3)]],
    [[[toUInt8(1), toUInt8(2)]]],
    [[concat(unhex('02000000'), toFixedString(reinterpretAsString(toUInt64(77)), 8))]];

INSERT INTO default.transactions (
    signature,
    slot,
    slot_idx,
    message_hash,
    is_vote,
    tx_account_keys,
    tx_instructions_program_id_index,
    tx_instructions_accounts,
    tx_instructions_data,
    meta_status_ok,
    meta_pre_token_balances_present,
    meta_pre_token_account_index,
    meta_pre_token_mint,
    meta_pre_token_owner,
    meta_pre_token_program_id,
    meta_pre_token_amount,
    meta_pre_token_decimals,
    meta_post_token_balances_present,
    meta_post_token_account_index,
    meta_post_token_mint,
    meta_post_token_owner,
    meta_post_token_program_id,
    meta_post_token_amount,
    meta_post_token_decimals
)
SELECT
    'fixture-token-2022-fee',
    102,
    0,
    'fixture',
    0,
    [
        base58Decode('TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb'),
        base58Decode('CrExqHvA6Nws8ZRH7kGoTurbsb6Rm7RKFSKbVe6dA1m7'),
        base58Decode('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'),
        base58Decode('HcvK3EJ74iM9g11cUgsaPvLSrhCvCwcrWxBNd87LsC1x'),
        base58Decode('HLnpSz9h2S4hiLQ43rnSD9XkcUThA7B8hQMKmDaiTLcC')
    ],
    [toUInt8(0)],
    [[toUInt8(1), toUInt8(2), toUInt8(3), toUInt8(4)]],
    [concat(
        unhex('1A01'),
        toFixedString(reinterpretAsString(toUInt64(1000)), 8),
        unhex('06'),
        toFixedString(reinterpretAsString(toUInt64(25)), 8)
    )],
    1,
    1,
    [toUInt8(1), toUInt8(3)],
    [
        base58Decode('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'),
        base58Decode('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v')
    ],
    [
        base58Decode('HLnpSz9h2S4hiLQ43rnSD9XkcUThA7B8hQMKmDaiTLcC'),
        base58Decode('9vR6ssB1BdzhAgVEoQoeZdbsqknBA2PkFYDsjvbAS5jP')
    ],
    [
        base58Decode('TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb'),
        base58Decode('TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb')
    ],
    ['2000', '0'],
    [toUInt8(6), toUInt8(6)],
    1,
    [toUInt8(1), toUInt8(3)],
    [
        base58Decode('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'),
        base58Decode('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v')
    ],
    [
        base58Decode('HLnpSz9h2S4hiLQ43rnSD9XkcUThA7B8hQMKmDaiTLcC'),
        base58Decode('9vR6ssB1BdzhAgVEoQoeZdbsqknBA2PkFYDsjvbAS5jP')
    ],
    [
        base58Decode('TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb'),
        base58Decode('TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb')
    ],
    ['1000', '975'],
    [toUInt8(6), toUInt8(6)];

INSERT INTO default.transactions (
    signature,
    slot,
    slot_idx,
    message_hash,
    is_vote,
    tx_account_keys,
    tx_instructions_program_id_index,
    tx_instructions_accounts,
    tx_instructions_data,
    meta_status_ok,
    meta_pre_token_balances_present,
    meta_pre_token_account_index,
    meta_pre_token_mint,
    meta_pre_token_owner,
    meta_pre_token_amount,
    meta_pre_token_decimals
)
SELECT
    'fixture-wsol-close',
    103,
    0,
    'fixture',
    0,
    [
        base58Decode('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'),
        base58Decode('CrExqHvA6Nws8ZRH7kGoTurbsb6Rm7RKFSKbVe6dA1m7'),
        base58Decode('9vR6ssB1BdzhAgVEoQoeZdbsqknBA2PkFYDsjvbAS5jP'),
        base58Decode('HLnpSz9h2S4hiLQ43rnSD9XkcUThA7B8hQMKmDaiTLcC')
    ],
    [toUInt8(0)],
    [[toUInt8(1), toUInt8(2), toUInt8(3)]],
    [unhex('09')],
    1,
    1,
    [toUInt8(1)],
    [base58Decode('So11111111111111111111111111111111111111112')],
    [base58Decode('HLnpSz9h2S4hiLQ43rnSD9XkcUThA7B8hQMKmDaiTLcC')],
    ['4200000000'],
    [toUInt8(9)];

INSERT INTO default.transactions (
    signature,
    slot,
    slot_idx,
    message_hash,
    is_vote,
    tx_account_keys,
    tx_instructions_program_id_index,
    tx_instructions_accounts,
    tx_instructions_data,
    meta_status_ok
)
SELECT
    'fixture-failed',
    104,
    0,
    'fixture',
    0,
    [
        base58Decode('11111111111111111111111111111111'),
        base58Decode('HLnpSz9h2S4hiLQ43rnSD9XkcUThA7B8hQMKmDaiTLcC'),
        base58Decode('9vR6ssB1BdzhAgVEoQoeZdbsqknBA2PkFYDsjvbAS5jP')
    ],
    [toUInt8(0)],
    [[toUInt8(1), toUInt8(2)]],
    [concat(unhex('02000000'), reinterpretAsString(toUInt64(99)))],
    0;

INSERT INTO default.transactions (
    signature,
    slot,
    slot_idx,
    message_hash,
    is_vote,
    tx_account_keys,
    tx_instructions_program_id_index,
    tx_instructions_accounts,
    tx_instructions_data,
    meta_status_ok
)
SELECT
    'fixture-missing-token-metadata',
    105,
    0,
    'fixture',
    0,
    [
        base58Decode('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'),
        base58Decode('CrExqHvA6Nws8ZRH7kGoTurbsb6Rm7RKFSKbVe6dA1m7'),
        base58Decode('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'),
        base58Decode('HcvK3EJ74iM9g11cUgsaPvLSrhCvCwcrWxBNd87LsC1x'),
        base58Decode('HLnpSz9h2S4hiLQ43rnSD9XkcUThA7B8hQMKmDaiTLcC')
    ],
    [toUInt8(0)],
    [[toUInt8(1), toUInt8(2), toUInt8(3), toUInt8(4)]],
    [concat(unhex('0C'), toFixedString(reinterpretAsString(toUInt64(55)), 8), unhex('06'))],
    1;

SELECT throwIf(
    (SELECT amount FROM default.transfers WHERE slot = 100) != '9007199254740993',
    'large outer transfer amount was not preserved exactly'
);
SELECT throwIf(
    (SELECT inner_instruction_idx FROM default.transfers WHERE slot = 101) != 1,
    'inner transfer was not indexed with its inner instruction position'
);
SELECT throwIf(
    (SELECT amount FROM default.transfers WHERE slot = 102) != '975',
    'Token-2022 fee transfer amount must be the destination credit'
);
SELECT throwIf(
    (SELECT fee_amount FROM default.transfers WHERE slot = 102) != '25',
    'Token-2022 fee transfer must retain fee_amount'
);
SELECT throwIf(
    (SELECT amount FROM default.transfers WHERE slot = 103) != '4200000000',
    'WSOL close amount was not recovered from pre-token metadata'
);
SELECT throwIf(
    (SELECT count() FROM default.transfers WHERE slot = 104) != 0,
    'failed transaction emitted a transfer row'
);
SELECT throwIf(
    (SELECT count() FROM default.transfers WHERE slot = 105 AND from_user_account IS NULL AND to_user_account IS NULL) != 1,
    'transfer with missing token metadata was not indexed safely'
);
SELECT throwIf(
    (SELECT count() FROM default.transfers) != 5,
    'unexpected fixture transfer count'
);

SELECT slot, transfer_type, amount, fee_amount, inner_instruction_idx
FROM default.transfers
ORDER BY slot;
