-- SPDX-License-Identifier: AGPL-3.0-only
CREATE TABLE IF NOT EXISTS default.block_footers_local ON CLUSTER '{cluster}'
(
    slot UInt64,
    bank_id UInt64,
    bank_hash FixedString(32),
    block_producer_time_nanos UInt64,
    block_user_agent String
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{cluster}/{database}/{table}/{shard}', '{replica}')
PARTITION BY intDiv(slot, 432000)
ORDER BY (slot, bank_id);

CREATE TABLE IF NOT EXISTS default.block_footers ON CLUSTER '{cluster}'
AS default.block_footers_local
ENGINE = Distributed('{cluster}', 'default', 'block_footers_local', intDiv(slot, 432000));
