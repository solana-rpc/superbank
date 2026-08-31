-- SPDX-License-Identifier: AGPL-3.0-only
--
-- Copyright 2025-2026 Triton One Limited. All rights reserved.
--

-- Local-only canonical transfer ledger used by getTransfersByAddress (single-node deployments).
-- Parses outer and inner instructions from successful transactions only.
-- Query addresses must be wallet owners (not ATAs); token owners come from meta token balance metadata.
CREATE MATERIALIZED VIEW IF NOT EXISTS default.transfers
(
    signature FixedString(64),
    slot UInt64,
    slot_idx UInt32,
    transfer_idx UInt32,
    inner_instruction_idx UInt32,
    block_time Nullable(Int64),
    transfer_type LowCardinality(String),
    amount String,
    mint Nullable(FixedString(32)),
    decimals Nullable(UInt8),
    from_user_account Nullable(FixedString(32)),
    to_user_account Nullable(FixedString(32)),
    from_token_account Nullable(FixedString(32)),
    to_token_account Nullable(FixedString(32)),
    fee_amount Nullable(String),
    INDEX bf_signature signature TYPE bloom_filter(0.01) GRANULARITY 64
)
ENGINE = ReplacingMergeTree(slot)
PARTITION BY intDiv(slot, 3024000)
PRIMARY KEY (slot, slot_idx, transfer_idx, inner_instruction_idx, transfer_type, signature)
ORDER BY (slot DESC, slot_idx DESC, transfer_idx DESC, inner_instruction_idx, transfer_type, signature)
SETTINGS
    allow_experimental_reverse_key = 1,
    index_granularity = 512,
    index_granularity_bytes = 67108864,
    min_bytes_for_wide_part = 10485760,
    compress_primary_key = 1,
    compress_marks = 1
AS
SELECT
    signature,
    slot,
    slot_idx,
    entry.2 AS transfer_idx,
    entry.11 AS inner_instruction_idx,
    block_time,
    entry.3 AS transfer_type,
    entry.8 AS amount,
    entry.9 AS mint,
    entry.10 AS decimals,
    entry.4 AS from_user_account,
    entry.5 AS to_user_account,
    entry.6 AS from_token_account,
    entry.7 AS to_token_account,
    entry.12 AS fee_amount
FROM
(
    WITH
        arrayConcat(tx_account_keys, meta_loaded_addresses_writable, meta_loaded_addresses_readonly) AS account_keys_all,
        CAST(base58Decode('11111111111111111111111111111111') AS FixedString(32)) AS system_program,
        CAST(base58Decode('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA') AS FixedString(32)) AS token_program,
        CAST(base58Decode('TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb') AS FixedString(32)) AS token_2022_program,
        CAST(base58Decode('So11111111111111111111111111111111111111111') AS FixedString(32)) AS native_sol_mint,
        CAST(base58Decode('So11111111111111111111111111111111111111112') AS FixedString(32)) AS wsol_mint,
        arrayMap(
            (ix_idx, prog_idx, accts, data) ->
                (
                    toUInt32(ix_idx - 1),
                    toUInt32(0),
                    arrayElement(account_keys_all, prog_idx + 1),
                    accts,
                    data
                ),
            arrayEnumerate(tx_instructions_program_id_index),
            tx_instructions_program_id_index,
            tx_instructions_accounts,
            tx_instructions_data
        ) AS outer_ixs,
        -- Build a normalized inner-instruction list only when metadata includes it.
        if(
            meta_inner_instructions_present = 1,
            -- Collapse each parent instruction's inner instruction array into one flat list.
            arrayFlatten(
                arrayMap(
                    -- Keep each inner instruction tied to its outer parent index.
                    (parent_ix, prog_ids, accts_arr, data_arr) ->
                        arrayMap(
                            -- Shape inner instructions to match outer_ixs: parent index, inner index, program id, accounts, data.
                            (inner_idx, prog_idx, accts, data) ->
                                (
                                    toUInt32(parent_ix),
                                    toUInt32(inner_idx),
                                    arrayElement(account_keys_all, prog_idx + 1),
                                    accts,
                                    data
                                ),
                            arrayEnumerate(prog_ids),
                            prog_ids,
                            accts_arr,
                            data_arr
                        ),
                    meta_inner_instructions_index,
                    meta_inner_instructions_program_id_index,
                    meta_inner_instructions_accounts,
                    meta_inner_instructions_data
                )
            ),
            -- Preserve the tuple type when no inner instructions are present.
            CAST([] AS Array(Tuple(
                UInt32,
                UInt32,
                FixedString(32),
                Array(UInt8),
                String
            )))
        ) AS inner_ixs,
        -- Treat top-level and inner instructions uniformly for transfer extraction.
        arrayConcat(outer_ixs, inner_ixs) AS all_ixs,
        -- Convert each normalized instruction into the transfer row fields used below, then keep only
        -- successfully parsed transfer-like entries.
        arrayFilter(
            entry -> entry.1 = 1,
            arrayMap(
                ix ->
                    (
                        -- filter for valid instruction type
                        multiIf(
                            ix.3 = system_program,
                                multiIf(
                                    -- 0: CreateAccount
                                    -- 5: WithdrawNonceAccount
                                    -- 3: CreateAccountWithSeed
                                    -- 13: CreateAccountAllowPrefund
                                    reinterpretAsUInt32(substring(ix.5, 1, 4)) IN (0, 3, 5, 13),
                                    toUInt8(1),
                                    -- 2: Transfer
                                    -- 11: TransferWithSeed
                                    reinterpretAsUInt32(substring(ix.5, 1, 4)) IN (2, 11)
                                        -- Ignore zero-amount transfers.
                                        AND reinterpretAsUInt64(substring(ix.5, 5, 8)) > 0,
                                    toUInt8(1),
                                    toUInt8(0)
                                ),
                            ix.3 IN (token_program, token_2022_program),
                                multiIf(
                                    -- 3: Transfer
                                    -- 7: MintTo
                                    -- 8: Burn
                                    -- 12: TransferChecked
                                    -- 14: MintToChecked
                                    -- 15: BurnChecked
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) IN (3, 7, 8, 12, 14, 15)
                                        -- Ignore zero-amount transfers.
                                        AND reinterpretAsUInt64(substring(ix.5, 2, 8)) > 0
                                    -- 26: TransferFee
                                        -- 1: TransferCheckedWithFee
                                        OR (
                                            reinterpretAsUInt8(substring(ix.5, 1, 1)) = 26
                                            AND reinterpretAsUInt8(substring(ix.5, 2, 1)) = 1
                                            -- Ignore zero-amount transfers.
                                            AND reinterpretAsUInt64(substring(ix.5, 3, 8)) > 0
                                        ),
                                    toUInt8(1),
                                    -- 9: CloseAccount
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) = 9,
                                    toUInt8(1),
                                    -- 6: SetAuthority
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) = 6
                                        AND reinterpretAsUInt8(substring(ix.5, 2, 1)) = 2,
                                    toUInt8(1),
                                    -- 26: TransferFee
                                        -- 2: WithdrawWithheldTokensFromMint
                                        -- 3: WithdrawWithheldTokensFromAccounts
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) = 26
                                        AND reinterpretAsUInt8(substring(ix.5, 2, 1)) IN (2, 3),
                                    toUInt8(1),
                                    -- 38: WithdrawExcessLamports
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) = 38
                                        -- Ignore zero-amount transfers.
                                        AND toUInt64(arrayElement(meta_pre_balances, arrayElement(ix.4, 1) + 1))
                                            > toUInt64(arrayElement(meta_post_balances, arrayElement(ix.4, 1) + 1)),
                                    toUInt8(1),
                                    -- 45: UnwrapLamports
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) = 45
                                        AND multiIf(
                                            reinterpretAsUInt8(substring(ix.5, 2, 1)) = 1,
                                            reinterpretAsUInt64(substring(ix.5, 3, 8)),
                                            toUInt64OrZero(
                                                arrayElement(
                                                    meta_pre_token_amount,
                                                    indexOf(meta_pre_token_account_index, arrayElement(ix.4, 1))
                                                )
                                            )
                                        ) > 0,
                                    toUInt8(1),
                                    toUInt8(0)
                                ),
                            toUInt8(0)
                        ),
                        ix.1,
                        -- transfer type
                        multiIf(
                            ix.3 = system_program,
                                'transfer',
                            ix.3 IN (token_program, token_2022_program),
                                multiIf(
                                    -- 3: Transfer
                                    -- 12: TransferChecked
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) IN (3, 12)
                                    -- 26: TransferFee
                                        -- 1: TransferCheckedWithFee
                                        OR (
                                            reinterpretAsUInt8(substring(ix.5, 1, 1)) = 26
                                            AND reinterpretAsUInt8(substring(ix.5, 2, 1)) = 1
                                        ),
                                    'transfer',
                                    -- 7: MintTo
                                    -- 14: MintToChecked
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) IN (7, 14),
                                    'mint',
                                    -- 8: Burn
                                    -- 15: BurnChecked
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) IN (8, 15),
                                    'burn',
                                    -- 9: CloseAccount
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) = 9,
                                    'closeAccount',
                                    -- 6: SetAuthority
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) = 6
                                        AND reinterpretAsUInt8(substring(ix.5, 2, 1)) = 2,
                                    'changeOwner',
                                    -- 26: TransferFee
                                        -- 2: WithdrawWithheldTokensFromMint
                                        -- 3: WithdrawWithheldTokensFromAccounts
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) = 26
                                        AND reinterpretAsUInt8(substring(ix.5, 2, 1)) IN (2, 3),
                                    'withdrawWithheldFee',
                                    -- 38: WithdrawExcessLamports
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) = 38,
                                    'transfer',
                                    -- 45: UnwrapLamports with explicit amount.
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) = 45
                                        AND reinterpretAsUInt8(substring(ix.5, 2, 1)) = 1,
                                    'transfer',
                                    -- 45: UnwrapLamports without explicit amount closes the token account.
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) = 45
                                        AND reinterpretAsUInt8(substring(ix.5, 2, 1)) = 0,
                                    'closeAccount',
                                    -- Unreachable
                                    ''
                                ),
                            -- Unreachable
                            ''
                        ),
                        -- fromUserAccount
                        multiIf(
                            ix.3 = system_program,
                                -- 0: CreateAccount
                                -- 2: Transfer
                                -- 3: CreateAccountWithSeed
                                -- 5: WithdrawNonceAccount
                                -- 11: TransferWithSeed
                                multiIf(
                                    -- 13: CreateAccountAllowPrefund
                                    reinterpretAsUInt32(substring(ix.5, 1, 4)) = 13,
                                    if(
                                        length(ix.4) >= 2,
                                        CAST(arrayElement(account_keys_all, arrayElement(ix.4, 2) + 1) AS Nullable(FixedString(32))),
                                        CAST(NULL AS Nullable(FixedString(32)))
                                    ),
                                    CAST(arrayElement(account_keys_all, arrayElement(ix.4, 1) + 1) AS Nullable(FixedString(32)))
                                ),
                            ix.3 IN (token_program, token_2022_program),
                                multiIf(
                                    -- 7: MintTo
                                    -- 14: MintToChecked
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) IN (7, 14)
                                    -- 26: TransferFee
                                        -- 2: WithdrawWithheldTokensFromMint
                                        OR (
                                            reinterpretAsUInt8(substring(ix.5, 1, 1)) = 26
                                            AND reinterpretAsUInt8(substring(ix.5, 2, 1)) = 2
                                        ),
                                    CAST(NULL AS Nullable(FixedString(32))),
                                    -- 26: TransferFee
                                        -- 3: WithdrawWithheldTokensFromAccounts
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) = 26
                                        AND reinterpretAsUInt8(substring(ix.5, 2, 1)) = 3,
                                    CAST(arrayElement(account_keys_all, arrayElement(ix.4, 3) + 1) AS Nullable(FixedString(32))),
                                    -- 3: Transfer
                                    -- 8: Burn
                                    -- 12: TransferChecked
                                    -- 15: BurnChecked
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) IN (3, 8, 12, 15)
                                    -- 26: TransferFee
                                        -- 1: TransferCheckedWithFee
                                        OR (
                                            reinterpretAsUInt8(substring(ix.5, 1, 1)) = 26
                                            AND reinterpretAsUInt8(substring(ix.5, 2, 1)) = 1
                                        ),
                                    CAST(
                                        -- Infer from transaction metadata token balances
                                        nullIf(
                                            arrayElement(
                                                meta_pre_token_owner,
                                                indexOf(meta_pre_token_account_index, arrayElement(ix.4, 1))
                                            ),
                                            defaultValueOfTypeName('FixedString(32)')
                                        ) AS Nullable(FixedString(32))
                                    ),
                                    -- 9: CloseAccount
                                    -- 38: WithdrawExcessLamports
                                    -- 45: UnwrapLamports
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) IN (9, 38, 45),
                                    CAST(arrayElement(account_keys_all, arrayElement(ix.4, 3) + 1) AS Nullable(FixedString(32))),
                                    -- 6: SetAuthority
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) = 6
                                        AND reinterpretAsUInt8(substring(ix.5, 2, 1)) = 2,
                                    CAST(arrayElement(account_keys_all, arrayElement(ix.4, 2) + 1) AS Nullable(FixedString(32))),
                                    CAST(NULL AS Nullable(FixedString(32)))
                                ),
                            CAST(NULL AS Nullable(FixedString(32)))
                        ),
                        -- toUserAccount
                        multiIf(
                            ix.3 = system_program,
                                multiIf(
                                    -- 0: CreateAccount
                                    -- 2: Transfer
                                    -- 3: CreateAccountWithSeed
                                    -- 5: WithdrawNonceAccount
                                    reinterpretAsUInt32(substring(ix.5, 1, 4)) IN (0, 2, 3, 5),
                                    CAST(arrayElement(account_keys_all, arrayElement(ix.4, 2) + 1) AS Nullable(FixedString(32))),
                                    -- 11: TransferWithSeed
                                    reinterpretAsUInt32(substring(ix.5, 1, 4)) = 11,
                                    CAST(arrayElement(account_keys_all, arrayElement(ix.4, 3) + 1) AS Nullable(FixedString(32))),
                                    -- 13: CreateAccountAllowPrefund
                                    reinterpretAsUInt32(substring(ix.5, 1, 4)) = 13,
                                    CAST(arrayElement(account_keys_all, arrayElement(ix.4, 1) + 1) AS Nullable(FixedString(32))),
                                    CAST(NULL AS Nullable(FixedString(32)))
                                ),
                            ix.3 IN (token_program, token_2022_program),
                                multiIf(
                                    -- 3: Transfer
                                    -- 7: MintTo
                                    -- 14: MintToChecked
                                    -- 26: TransferFee
                                        -- 2: WithdrawWithheldTokensFromMint
                                        -- 3: WithdrawWithheldTokensFromAccounts
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) IN (3, 7, 14)
                                        OR (
                                            reinterpretAsUInt8(substring(ix.5, 1, 1)) = 26
                                            AND reinterpretAsUInt8(substring(ix.5, 2, 1)) IN (2, 3)
                                        ),
                                    CAST(
                                        -- Infer from transaction metadata token balances
                                        nullIf(
                                            arrayElement(
                                                meta_post_token_owner,
                                                indexOf(meta_post_token_account_index, arrayElement(ix.4, 2))
                                            ),
                                            defaultValueOfTypeName('FixedString(32)')
                                        ) AS Nullable(FixedString(32))
                                    ),
                                    -- 12: TransferChecked
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) = 12
                                    -- 26: TransferFee
                                        -- 1: TransferCheckedWithFee
                                        OR (
                                            reinterpretAsUInt8(substring(ix.5, 1, 1)) = 26
                                            AND reinterpretAsUInt8(substring(ix.5, 2, 1)) = 1
                                        ),
                                    CAST(
                                        -- Infer from transaction metadata token balances
                                        nullIf(
                                            arrayElement(
                                                meta_post_token_owner,
                                                indexOf(meta_post_token_account_index, arrayElement(ix.4, 3))
                                            ),
                                            defaultValueOfTypeName('FixedString(32)')
                                        ) AS Nullable(FixedString(32))
                                    ),
                                    -- 8: Burn
                                    -- 15: BurnChecked
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) IN (8, 15),
                                    CAST(NULL AS Nullable(FixedString(32))),
                                    -- 9: CloseAccount
                                    -- 38: WithdrawExcessLamports
                                    -- 45: UnwrapLamports
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) IN (9, 38, 45),
                                    CAST(arrayElement(account_keys_all, arrayElement(ix.4, 2) + 1) AS Nullable(FixedString(32))),
                                    -- 6: SetAuthority
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) = 6
                                        AND reinterpretAsUInt8(substring(ix.5, 2, 1)) = 2,
                                    if(
                                        reinterpretAsUInt8(substring(ix.5, 3, 1)) = 1,
                                        CAST(substring(ix.5, 4, 32) AS Nullable(FixedString(32))),
                                        CAST(NULL AS Nullable(FixedString(32)))
                                    ),
                                    CAST(NULL AS Nullable(FixedString(32)))
                                ),
                            CAST(NULL AS Nullable(FixedString(32)))
                        ),
                        -- fromTokenAccount
                        multiIf(
                            ix.3 = system_program,
                                CAST(NULL AS Nullable(FixedString(32))),
                            ix.3 IN (token_program, token_2022_program),
                                multiIf(
                                    -- 3: Transfer
                                    -- 8: Burn
                                    -- 9: CloseAccount
                                    -- 12: TransferChecked
                                    -- 15: BurnChecked
                                    -- 38: WithdrawExcessLamports
                                    -- 45: UnwrapLamports
                                    -- 26: TransferFee
                                        -- 1: TransferCheckedWithFee
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) IN (3, 8, 9, 12, 15, 38, 45)
                                        OR (
                                            reinterpretAsUInt8(substring(ix.5, 1, 1)) = 26
                                            AND reinterpretAsUInt8(substring(ix.5, 2, 1)) = 1
                                        ),
                                    CAST(arrayElement(account_keys_all, arrayElement(ix.4, 1) + 1) AS Nullable(FixedString(32))),
                                    CAST(NULL AS Nullable(FixedString(32)))
                                ),
                            CAST(NULL AS Nullable(FixedString(32)))
                        ),
                        -- toTokenAccount
                        multiIf(
                            ix.3 = system_program,
                                CAST(NULL AS Nullable(FixedString(32))),
                            ix.3 IN (token_program, token_2022_program),
                                multiIf(
                                    -- 3: Transfer
                                    -- 7: MintTo
                                    -- 14: MintToChecked
                                    -- 26: TransferFee
                                        -- 2: WithdrawWithheldTokensFromMint
                                        -- 3: WithdrawWithheldTokensFromAccounts
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) IN (3, 7, 14)
                                        OR (
                                            reinterpretAsUInt8(substring(ix.5, 1, 1)) = 26
                                            AND reinterpretAsUInt8(substring(ix.5, 2, 1)) IN (2, 3)
                                        ),
                                    CAST(arrayElement(account_keys_all, arrayElement(ix.4, 2) + 1) AS Nullable(FixedString(32))),
                                    -- 12: TransferChecked
                                    -- 26: TransferFee
                                        -- 1: TransferCheckedWithFee
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) = 12
                                        OR (
                                            reinterpretAsUInt8(substring(ix.5, 1, 1)) = 26
                                            AND reinterpretAsUInt8(substring(ix.5, 2, 1)) = 1
                                        ),
                                    CAST(arrayElement(account_keys_all, arrayElement(ix.4, 3) + 1) AS Nullable(FixedString(32))),
                                    -- 8: Burn
                                    -- 15: BurnChecked
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) IN (8, 15),
                                    CAST(NULL AS Nullable(FixedString(32))),
                                    CAST(NULL AS Nullable(FixedString(32)))
                                ),
                            CAST(NULL AS Nullable(FixedString(32)))
                        ),
                        -- raw transfer amount
                        multiIf(
                            ix.3 = system_program,
                                multiIf(
                                    -- 3: CreateAccountWithSeed
                                    -- [0..4] discriminator, [4..36] base, [36..44] u64 seed length,
                                    -- [44..] seed, then u64 amount / u64 space / owner.
                                    reinterpretAsUInt32(substring(ix.5, 1, 4)) = 3,
                                    toString(reinterpretAsUInt64(substring(
                                        ix.5,
                                        45 + reinterpretAsUInt64(substring(ix.5, 37, 8)),
                                        8
                                    ))),
                                    -- 0: CreateAccount
                                    -- 2: Transfer
                                    -- 5: WithdrawNonceAccount
                                    -- 11: TransferWithSeed
                                    -- 13: CreateAccountAllowPrefund
                                    reinterpretAsUInt32(substring(ix.5, 1, 4)) IN (0, 2, 5, 11, 13),
                                    toString(reinterpretAsUInt64(substring(ix.5, 5, 8))),
                                    '0'
                                ),
                            ix.3 IN (token_program, token_2022_program),
                                multiIf(
                                    -- 3: Transfer
                                    -- 7: MintTo
                                    -- 8: Burn
                                    -- 12: TransferChecked
                                    -- 14: MintToChecked
                                    -- 15: BurnChecked
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) IN (3, 7, 8, 12, 14, 15),
                                    toString(reinterpretAsUInt64(substring(ix.5, 2, 8))),
                                    -- 26: TransferFee
                                        -- 1: TransferCheckedWithFee
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) = 26
                                        AND reinterpretAsUInt8(substring(ix.5, 2, 1)) = 1,
                                    toString(
                                        reinterpretAsUInt64(substring(ix.5, 3, 8))
                                        - reinterpretAsUInt64(substring(ix.5, 12, 8))
                                    ),
                                    -- 9: CloseAccount
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) = 9,
                                    multiIf(
                                        nullIf(
                                            arrayElement(
                                                meta_pre_token_mint,
                                                indexOf(meta_pre_token_account_index, arrayElement(ix.4, 1))
                                            ),
                                            defaultValueOfTypeName('FixedString(32)')
                                        ) = wsol_mint,
                                        toString(
                                            toUInt64OrZero(
                                                arrayElement(
                                                    meta_pre_token_amount,
                                                    indexOf(meta_pre_token_account_index, arrayElement(ix.4, 1))
                                                )
                                            )
                                        ),
                                        toString(
                                            toUInt64(arrayElement(meta_post_balances, arrayElement(ix.4, 2) + 1))
                                            - toUInt64(arrayElement(meta_pre_balances, arrayElement(ix.4, 2) + 1))
                                        )
                                    ),
                                    -- 26: TransferFee
                                        -- 2: WithdrawWithheldTokensFromMint
                                        -- 3: WithdrawWithheldTokensFromAccounts
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) = 26
                                        AND reinterpretAsUInt8(substring(ix.5, 2, 1)) IN (2, 3),
                                    toString(
                                        abs(
                                            toUInt64OrZero(
                                                arrayElement(
                                                    meta_post_token_amount,
                                                    indexOf(meta_post_token_account_index, arrayElement(ix.4, 2))
                                                )
                                            )
                                            - toUInt64OrZero(
                                                arrayElement(
                                                    meta_pre_token_amount,
                                                    indexOf(meta_pre_token_account_index, arrayElement(ix.4, 2))
                                                )
                                            )
                                        )
                                    ),
                                    -- 38: WithdrawExcessLamports
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) = 38,
                                    toString(
                                        toUInt64(arrayElement(meta_pre_balances, arrayElement(ix.4, 1) + 1))
                                        - toUInt64(arrayElement(meta_post_balances, arrayElement(ix.4, 1) + 1))
                                    ),
                                    -- 45: UnwrapLamports
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) = 45,
                                    toString(
                                        multiIf(
                                            reinterpretAsUInt8(substring(ix.5, 2, 1)) = 1,
                                            reinterpretAsUInt64(substring(ix.5, 3, 8)),
                                            toUInt64OrZero(
                                                arrayElement(
                                                    meta_pre_token_amount,
                                                    indexOf(meta_pre_token_account_index, arrayElement(ix.4, 1))
                                                )
                                            )
                                        )
                                    ),
                                    '0'
                                ),
                            '0'
                        ),
                        -- mint address
                        multiIf(
                            ix.3 = system_program,
                                CAST(native_sol_mint AS Nullable(FixedString(32))),
                            ix.3 IN (token_program, token_2022_program),
                                multiIf(
                                    -- 3: Transfer
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) = 3,
                                    CAST(
                                        arrayElement(
                                            meta_post_token_mint,
                                            indexOf(meta_post_token_account_index, arrayElement(ix.4, 1))
                                        ) AS Nullable(FixedString(32))
                                    ),
                                    -- 7: MintTo
                                    -- 14: MintToChecked
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) IN (7, 14),
                                    CAST(arrayElement(account_keys_all, arrayElement(ix.4, 1) + 1) AS Nullable(FixedString(32))),
                                    -- 8: Burn
                                    -- 12: TransferChecked
                                    -- 15: BurnChecked
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) IN (8, 12, 15)
                                    -- 26: TransferFee
                                        -- 1: TransferCheckedWithFee
                                        OR (
                                            reinterpretAsUInt8(substring(ix.5, 1, 1)) = 26
                                            AND reinterpretAsUInt8(substring(ix.5, 2, 1)) = 1
                                        ),
                                    CAST(arrayElement(account_keys_all, arrayElement(ix.4, 2) + 1) AS Nullable(FixedString(32))),
                                    -- 9: CloseAccount
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) = 9,
                                    CAST(native_sol_mint AS Nullable(FixedString(32))),
                                    -- 26: TransferFee
                                        -- 2: WithdrawWithheldTokensFromMint
                                        -- 3: WithdrawWithheldTokensFromAccounts
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) = 26
                                        AND reinterpretAsUInt8(substring(ix.5, 2, 1)) IN (2, 3),
                                    CAST(arrayElement(account_keys_all, arrayElement(ix.4, 1) + 1) AS Nullable(FixedString(32))),
                                    -- 38: WithdrawExcessLamports
                                    -- 45: UnwrapLamports
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) IN (38, 45),
                                    CAST(native_sol_mint AS Nullable(FixedString(32))),
                                    CAST(NULL AS Nullable(FixedString(32)))
                                ),
                            CAST(NULL AS Nullable(FixedString(32)))
                        ),
                        -- mint decimals
                        multiIf(
                            ix.3 = system_program,
                                CAST(9 AS Nullable(UInt8)),
                            ix.3 IN (token_program, token_2022_program),
                                multiIf(
                                    -- 3: Transfer
                                    -- 7: MintTo
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) IN (3, 7),
                                    CAST(
                                        arrayElement(
                                            meta_pre_token_decimals,
                                            indexOf(meta_pre_token_account_index, arrayElement(ix.4, 2))
                                        ) AS Nullable(UInt8)
                                    ),
                                    -- 9: CloseAccount
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) = 9,
                                    CAST(9 AS Nullable(UInt8)),
                                    -- 12: TransferChecked
                                    -- 14: MintToChecked
                                    -- 15: BurnChecked
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) IN (12, 14, 15),
                                    CAST(reinterpretAsUInt8(substring(ix.5, 10, 1)) AS Nullable(UInt8)),
                                    -- 26: TransferFee
                                        -- 1: TransferCheckedWithFee
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) = 26
                                        AND reinterpretAsUInt8(substring(ix.5, 2, 1)) = 1,
                                    CAST(reinterpretAsUInt8(substring(ix.5, 11, 1)) AS Nullable(UInt8)),
                                    -- 8: Burn
                                    -- 26: TransferFee
                                        -- 2: WithdrawWithheldTokensFromMint
                                        -- 3: WithdrawWithheldTokensFromAccounts
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) = 8
                                        OR (
                                            reinterpretAsUInt8(substring(ix.5, 1, 1)) = 26
                                            AND reinterpretAsUInt8(substring(ix.5, 2, 1)) IN (2, 3)
                                        ),
                                    CAST(
                                        nullIf(
                                            arrayElement(
                                                meta_pre_token_decimals,
                                                indexOf(meta_pre_token_account_index, arrayElement(ix.4, 1))
                                            ),
                                            defaultValueOfTypeName('UInt8')
                                        ) AS Nullable(UInt8)
                                    ),
                                    -- 38: WithdrawExcessLamports
                                    -- 45: UnwrapLamports
                                    reinterpretAsUInt8(substring(ix.5, 1, 1)) IN (38, 45),
                                    CAST(9 AS Nullable(UInt8)),
                                    CAST(NULL AS Nullable(UInt8))
                                ),
                            CAST(NULL AS Nullable(UInt8))
                        ),
                        ix.2,
                        multiIf(
                            ix.3 IN (token_program, token_2022_program)
                                -- 26: TransferFee
                                    -- 1: TransferCheckedWithFee
                                AND reinterpretAsUInt8(substring(ix.5, 1, 1)) = 26
                                AND reinterpretAsUInt8(substring(ix.5, 2, 1)) = 1,
                            CAST(toString(reinterpretAsUInt64(substring(ix.5, 12, 8))) AS Nullable(String)),
                            CAST(NULL AS Nullable(String))
                        )
                    ),
                all_ixs
            )
        ) AS parsed_entries
    SELECT
        signature,
        slot,
        slot_idx,
        block_time,
        parsed_entries AS transfer_entries
    FROM default.transactions
    WHERE meta_status_ok = 1
)
ARRAY JOIN transfer_entries AS entry;
