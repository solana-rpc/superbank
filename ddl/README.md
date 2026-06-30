## DDL Layout

The schema files are organized by deployment mode:

- `local/`: single-node ClickHouse schemas for local development.
- `cluster/`: clustered schemas with non-replicated shard-local `ReplacingMergeTree` tables.
- `replicated/`: clustered schemas with replicated shard-local `ReplicatedReplacingMergeTree` tables.

Each folder contains the same file basenames:

- `transactions.sql`
- `blocks_metadata.sql`
- `entries.sql`
- `gsfa.sql`
- `gsfa_nohot.sql`
- `gsfa_hot.sql`
- `signatures.sql`
- `token_owner_activity.sql`

Pick one folder and apply the matching schema set consistently.
Apply `transactions.sql` before materialized-view files such as `gsfa*.sql`, `signatures.sql`, and
`token_owner_activity.sql`; those views select from the transactions table and will fail if it does
not exist yet.
`gsfa_nohot.sql` is an alternative to `gsfa.sql`; do not apply both for the same schema set.
`entries.sql` is required for Superbank Fumarole/gRPC source defaults and for PoH entry ingestion
from Old Faithful / Jetstreamer. RPC and Bigtable sources do not populate `entries`.

GSFA note:
- Current GSFA DDL defines `default.gsfa` as the materialized view and query surface.
- In clustered deployments, `default.gsfa` uses `ENGINE = Distributed(..., 'gsfa_local',
  cityHash64(address))`, so derived rows are routed to the correct shard-local `gsfa_local`
  storage table.
