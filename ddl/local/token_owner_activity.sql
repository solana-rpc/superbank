-- SPDX-License-Identifier: AGPL-3.0-only
--
-- Copyright 2025-2026 Triton One Limited. All rights reserved.
--

-- Local-only token owner activity materialized view (single-node deployments).
CREATE MATERIALIZED VIEW IF NOT EXISTS default.token_owner_activity
(
    owner_bucket UInt8 materialized cityHash64(owner) % 32,
    owner FixedString(32),
    token_account FixedString(32),
    signature FixedString(64),
    slot UInt64,
    slot_idx UInt32,
    memo Nullable(String),
    err Nullable(String),
    block_time Nullable(Int64),
    balance_changed UInt8,
    INDEX bf_signature signature TYPE bloom_filter(0.01) GRANULARITY 64
)
ENGINE = ReplacingMergeTree(slot)
PARTITION BY owner_bucket
PRIMARY KEY (owner_bucket, owner, slot, slot_idx, signature, token_account)
ORDER BY (owner_bucket, owner, slot DESC, slot_idx DESC, signature, token_account)
SETTINGS
    allow_experimental_reverse_key = 1,
    index_granularity = 512,
    index_granularity_bytes = 67108864,
    min_bytes_for_wide_part = 10485760,
    compress_primary_key = 1,
    compress_marks = 1
AS
WITH
    [
        CAST(base58Decode('Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo') AS FixedString(32)),
        CAST(base58Decode('MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr') AS FixedString(32)),
        CAST(base58Decode('Memo4c2pN8afCj432Lb7RMVKi9PbQnnW7ewFFaV3oAH') AS FixedString(32))
    ] AS memo_program_ids
SELECT
    assumeNotNull(entry.1) AS owner,
    entry.2 AS token_account,
    signature,
    slot,
    slot_idx,
    memo,
    if(meta_status_ok = 1, NULL, meta_err) AS err,
    block_time,
    entry.3 AS balance_changed
FROM
(
    WITH arrayConcat(tx_account_keys, meta_loaded_addresses_writable, meta_loaded_addresses_readonly) AS account_keys_all
    SELECT
        signature,
        slot,
        slot_idx,
        block_time,
        meta_status_ok,
        meta_err,
        tx_instructions_program_id_index,
        tx_instructions_data,
        nullIf(
            arrayStringConcat(
                arrayMap(x -> x.2,
                    arrayFilter(
                        x -> has(memo_program_ids, x.1) AND isValidUTF8(x.2),
                        arrayZip(
                            arrayMap(idx -> arrayElement(account_keys_all, idx + 1), tx_instructions_program_id_index),
                            tx_instructions_data
                        )
                    )
                ),
                '; '
            ),
            ''
        ) AS memo,
        arrayDistinct(arrayConcat(meta_pre_token_account_index, meta_post_token_account_index)) AS token_indices,
        arrayMap(idx -> indexOf(meta_pre_token_account_index, idx), token_indices) AS pre_pos,
        arrayMap(idx -> indexOf(meta_post_token_account_index, idx), token_indices) AS post_pos,
        arrayFilter(
            x -> isNotNull(x.1) AND x.4,
            arrayMap(
                (idx, pre_idx, post_idx) ->
                    (
                        coalesce(
                            arrayElement(meta_post_token_owner, post_idx),
                            arrayElement(meta_pre_token_owner, pre_idx)
                        ),
                        arrayElement(account_keys_all, idx + 1),
                        multiIf(
                            pre_idx = 0 OR post_idx = 0, 1,
                            arrayElement(meta_pre_token_amount, pre_idx)
                                != arrayElement(meta_post_token_amount, post_idx),
                            1,
                            0
                        ),
                        idx < length(account_keys_all)
                    ),
                token_indices,
                pre_pos,
                post_pos
            )
        ) AS token_entries
    FROM default.transactions
)
ARRAY JOIN token_entries AS entry;

-- Update the query of an existing materialized view without replacing its stored data.
ALTER TABLE default.token_owner_activity MODIFY QUERY
WITH
    [
        CAST(base58Decode('Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo') AS FixedString(32)),
        CAST(base58Decode('MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr') AS FixedString(32)),
        CAST(base58Decode('Memo4c2pN8afCj432Lb7RMVKi9PbQnnW7ewFFaV3oAH') AS FixedString(32))
    ] AS memo_program_ids
SELECT
    assumeNotNull(entry.1) AS owner,
    entry.2 AS token_account,
    signature,
    slot,
    slot_idx,
    memo,
    if(meta_status_ok = 1, NULL, meta_err) AS err,
    block_time,
    entry.3 AS balance_changed
FROM
(
    WITH arrayConcat(tx_account_keys, meta_loaded_addresses_writable, meta_loaded_addresses_readonly) AS account_keys_all
    SELECT
        signature,
        slot,
        slot_idx,
        block_time,
        meta_status_ok,
        meta_err,
        tx_instructions_program_id_index,
        tx_instructions_data,
        nullIf(
            arrayStringConcat(
                arrayMap(x -> x.2,
                    arrayFilter(
                        x -> has(memo_program_ids, x.1) AND isValidUTF8(x.2),
                        arrayZip(
                            arrayMap(idx -> arrayElement(account_keys_all, idx + 1), tx_instructions_program_id_index),
                            tx_instructions_data
                        )
                    )
                ),
                '; '
            ),
            ''
        ) AS memo,
        arrayDistinct(arrayConcat(meta_pre_token_account_index, meta_post_token_account_index)) AS token_indices,
        arrayMap(idx -> indexOf(meta_pre_token_account_index, idx), token_indices) AS pre_pos,
        arrayMap(idx -> indexOf(meta_post_token_account_index, idx), token_indices) AS post_pos,
        arrayFilter(
            x -> isNotNull(x.1) AND x.4,
            arrayMap(
                (idx, pre_idx, post_idx) ->
                    (
                        coalesce(
                            arrayElement(meta_post_token_owner, post_idx),
                            arrayElement(meta_pre_token_owner, pre_idx)
                        ),
                        arrayElement(account_keys_all, idx + 1),
                        multiIf(
                            pre_idx = 0 OR post_idx = 0, 1,
                            arrayElement(meta_pre_token_amount, pre_idx)
                                != arrayElement(meta_post_token_amount, post_idx),
                            1,
                            0
                        ),
                        idx < length(account_keys_all)
                    ),
                token_indices,
                pre_pos,
                post_pos
            )
        ) AS token_entries
    FROM default.transactions
)
ARRAY JOIN token_entries AS entry;
