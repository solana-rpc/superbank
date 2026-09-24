-- SPDX-License-Identifier: AGPL-3.0-only
-- Alpenglow footer fields are internal data, keyed by the producer's bank instance.
CREATE TABLE IF NOT EXISTS default.block_footers
(
    slot UInt64,
    bank_id UInt64,
    bank_hash FixedString(32),
    block_producer_time_nanos UInt64,
    block_user_agent String
)
ENGINE = ReplacingMergeTree
PARTITION BY intDiv(slot, 432000)
ORDER BY (slot, bank_id);
