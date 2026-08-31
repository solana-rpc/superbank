-- SPDX-License-Identifier: AGPL-3.0-only
--
-- Copyright 2025-2026 Triton One Limited. All rights reserved.
--

-- GSFA hot tables for frequently queried addresses.
-- Use 14-day slot-window partitions to reduce hot-address part and merge pressure.
-- Keep the hot address list in sync with RPC hot address configuration.
CREATE TABLE IF NOT EXISTS default.gsfa_hot_local ON CLUSTER '{cluster}'
(
    addr_bucket UInt8 materialized cityHash64(address) % 32,
    address     FixedString(32),
    signature   FixedString(64),
    slot        UInt64,
    slot_idx    UInt32,
    memo        Nullable(String),
    err         Nullable(String),
    block_time  Nullable(Int64)
)
ENGINE = ReplacingMergeTree(slot)
PARTITION BY (addr_bucket, intDiv(slot, 3024000))
PRIMARY KEY (addr_bucket, address, slot, slot_idx, signature)
ORDER BY (addr_bucket, address, slot DESC, slot_idx DESC, signature)
SETTINGS
    allow_experimental_reverse_key = 1,
    index_granularity = 512,
    index_granularity_bytes = 67108864,
    min_bytes_for_wide_part = 10485760,
    compress_primary_key = 1,
    compress_marks = 1;

-- Derive hot GSFA rows directly from transactions for hot addresses.
CREATE MATERIALIZED VIEW IF NOT EXISTS default.gsfa_hot ON CLUSTER '{cluster}'
(
    addr_bucket UInt8 materialized cityHash64(address) % 32,
    address     FixedString(32),
    signature   FixedString(64),
    slot        UInt64,
    slot_idx    UInt32,
    memo        Nullable(String),
    err         Nullable(String),
    block_time  Nullable(Int64)
)
-- Shard hot-address rows by signature so one address spreads across the cluster.
ENGINE = Distributed('{cluster}', 'default', 'gsfa_hot_local', cityHash64(signature))
AS
WITH
    [
        CAST(base58Decode('Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo') AS FixedString(32)),
        CAST(base58Decode('MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr') AS FixedString(32)),
        CAST(base58Decode('Memo4c2pN8afCj432Lb7RMVKi9PbQnnW7ewFFaV3oAH') AS FixedString(32))
    ] AS memo_program_ids,
    [
        CAST(base58Decode('11111111111111111111111111111111') AS FixedString(32)),
        CAST(base58Decode('Vote111111111111111111111111111111111111111') AS FixedString(32)),
        CAST(base58Decode('SysvarC1ock11111111111111111111111111111111') AS FixedString(32)),
        CAST(base58Decode('SysvarS1otHashes111111111111111111111111111') AS FixedString(32))
    ] AS gsfa_ignored_addresses,
    [
        CAST(base58Decode('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v') AS FixedString(32))
    ] AS gsfa_hot_addresses
SELECT
    address,
    signature,
    slot,
    slot_idx,
    memo,
    if(meta_status_ok = 1, NULL, meta_err) AS err,
    block_time
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
        arrayDistinct(account_keys_all) AS addresses,
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
        ) AS memo
    FROM default.transactions_local
)
ARRAY JOIN arrayFilter(addr -> NOT has(gsfa_ignored_addresses, addr) AND has(gsfa_hot_addresses, addr), addresses) AS address;

-- Update the query of an existing materialized view without replacing its stored data.
ALTER TABLE default.gsfa_hot ON CLUSTER '{cluster}' MODIFY QUERY
WITH
    [
        CAST(base58Decode('Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo') AS FixedString(32)),
        CAST(base58Decode('MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr') AS FixedString(32)),
        CAST(base58Decode('Memo4c2pN8afCj432Lb7RMVKi9PbQnnW7ewFFaV3oAH') AS FixedString(32))
    ] AS memo_program_ids,
    [
        CAST(base58Decode('11111111111111111111111111111111') AS FixedString(32)),
        CAST(base58Decode('Vote111111111111111111111111111111111111111') AS FixedString(32)),
        CAST(base58Decode('SysvarC1ock11111111111111111111111111111111') AS FixedString(32)),
        CAST(base58Decode('SysvarS1otHashes111111111111111111111111111') AS FixedString(32))
    ] AS gsfa_ignored_addresses,
    [
        CAST(base58Decode('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v') AS FixedString(32))
    ] AS gsfa_hot_addresses
SELECT
    address,
    signature,
    slot,
    slot_idx,
    memo,
    if(meta_status_ok = 1, NULL, meta_err) AS err,
    block_time
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
        arrayDistinct(account_keys_all) AS addresses,
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
        ) AS memo
    FROM default.transactions_local
)
ARRAY JOIN arrayFilter(addr -> NOT has(gsfa_ignored_addresses, addr) AND has(gsfa_hot_addresses, addr), addresses) AS address;
