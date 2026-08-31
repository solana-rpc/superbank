# Architecture

Superbank ingests Solana ledger data into ClickHouse and serves a subset of Solana-compatible
JSON-RPC endpoints, plus an optional Superbank gRPC streaming API, backed by that data.

At a high level:
- `superbank` (ingestor) reads blocks/transactions from Fumarole, gRPC, RPC, or Bigtable sources
  and writes them to ClickHouse.
- ClickHouse stores base tables and derived index tables/materialized views (defined in `ddl/`).
- `superbank-rpc` reads ClickHouse to serve supported JSON-RPC methods and, when enabled, historical
  gRPC block/transaction streams.

This document describes the logical architecture and data model contracts in this repository. It
does not guarantee any specific deployment topology or production readiness.

## System Overview

- Ingest: `crates/superbank/` writes the base tables:
  - `transactions`
  - `blocks_metadata`
  - `entries` for Fumarole/gRPC source PoH entry data
- Store: ClickHouse holds those base tables plus derived tables used as RPC indexes.
- Serve: `crates/superbank-rpc/` serves JSON-RPC, and optional Superbank gRPC streams, backed by
  those tables.

See:
- Ingestor: [`crates/superbank/README.md`](../crates/superbank/README.md)
- RPC server: [`crates/superbank-rpc/README.md`](../crates/superbank-rpc/README.md)
- Schemas: [`ddl/`](../ddl/)

## Data Flow

```mermaid
flowchart LR
  subgraph Sources
    FUM[Yellowstone Fumarole]
    DM[Yellowstone gRPC (DragonsMouth)]
    SRPC[Solana JSON-RPC (getBlock)]
    BT[Solana Bigtable]
  end

  Ingest[superbank]

  subgraph CH[ClickHouse]
    TX[transactions]
    BM[blocks_metadata]
    GSFA[gsfa (MV)]
    SIG[signatures (MV)]
    EN[entries (Fumarole/gRPC sources)]
    TOA[token_owner_activity (MV, optional)]
    HOT[gsfa_hot (MV, optional)]
  end

  subgraph Serve
    RPC[superbank-rpc]
    HC[gRPC head cache (optional)]
    DC[local ClickHouse forward cache (optional)]
    SGRPC[Superbank gRPC streaming (optional)]
  end

  RPCClients[JSON-RPC clients]
  GRPCClients[gRPC clients]

  FUM --> Ingest
  DM --> Ingest
  SRPC --> Ingest
  BT --> Ingest

  Ingest --> TX
  Ingest --> BM
  Ingest --> EN

  TX --> GSFA
  TX --> SIG
  TX --> TOA
  TX --> HOT

  TX --> RPC
  BM --> RPC
  GSFA --> RPC
  SIG --> RPC
  TOA --> RPC
  HOT --> RPC

  DM -. optional .-> HC
  HC -. merges recent slots .-> RPC
  CH -. schema and recent finalized rows .-> DC
  DC -. recent finalized reads .-> RPC
  CH --> SGRPC

  RPC --> RPCClients
  SGRPC --> GRPCClients
```

## Optional gRPC Head Cache

`superbank-rpc` can optionally subscribe to a Yellowstone gRPC stream and keep a small in-memory
cache of the most recent slots ("head"). This is intended to reduce perceived ingestion lag and to
optionally expose `processed` commitment for a subset of RPC methods.

This feature is gated in two ways:
- Build-time: compile with `--features grpc-head-cache`.
- Runtime: enable with `HEAD_CACHE_ENABLED=true` (and provide the gRPC endpoint/config).

License note: enabling `grpc-head-cache` pulls in an AGPL-3.0 dependency. See
[`crates/superbank-rpc/README.md`](../crates/superbank-rpc/README.md) for details, including supported
methods and configuration.

## Optional Local ClickHouse Forward Cache

With `--features disk-cache`, `superbank-rpc` can forward recent finalized slots from the source ClickHouse cluster to a separate ClickHouse instance on localhost. This feature is independent from the optional gRPC head cache. The local instance is a bounded near cache and never becomes a source of truth.

At startup, the RPC server inspects source metadata and creates an equivalent cache-specific schema. The forwarder streams bounded ranges concurrently in ClickHouse Native format through one global rate limiter. Each range lets local materialized views build the query indexes, validates transaction counts, and only then marks its slots covered. A failed range remains a hole without invalidating successful sibling ranges. Reads use a contiguous covered range and fall through to the source cluster for misses, holes, and older history. MergeTree tables use whole-slot-range partitions for retention. The `blocks_metadata` table can use a bounded `Memory` engine when explicitly configured.

## Optional Superbank gRPC Streaming

With `--features grpc-streaming` and `SUPERBANK_GRPC_ENABLED=true`, `superbank-rpc` serves a tonic
gRPC endpoint alongside JSON-RPC. The current service exposes historical `StreamBlocks` and
`StreamTransactions` over bounded inclusive slot ranges from ClickHouse. It is separate from
Yellowstone gRPC ingestion and from the optional head cache.

## ClickHouse Data Model (DDL -> RPC Features)

Superbank treats ClickHouse schemas under [`ddl/`](../ddl/) as the contract between ingestion and
query serving:

- Base tables are written by the ingestor.
- Derived tables are materialized from `transactions` inside ClickHouse and act as query indexes for
  RPC methods.

### Local vs Cluster DDL

- `ddl/local/*.sql` is intended for single-node ClickHouse (local development). Objects are created
  directly in `default.*`.
- `ddl/cluster/*.sql` is intended for ClickHouse clusters. These files typically create:
  - a shard-local `*_local` table (MergeTree family), and
  - a distributed table/materialized view that fans out across the cluster via the `{cluster}`
    macro.
- `ddl/replicated/*.sql` is intended for ClickHouse clusters whose shard-local tables should be
  replicated with `ReplicatedReplacingMergeTree`. These files keep the same distributed object
  names as `ddl/cluster/*.sql`, but require Keeper/ZooKeeper, `{cluster}`, `{shard}`, and
  `{replica}` macros, and a cluster definition with `internal_replication=1`.

### Schema Map

| DDL file(s) | Creates | What it stores | RPC features that depend on it |
| --- | --- | --- | --- |
| `ddl/{local,cluster,replicated}/transactions.sql` | `transactions` (base table) | Source-of-truth transaction rows (message + status meta + balances + logs, etc.). | `getTransaction`, transaction payloads for `getBlock`, and full hydration in `getTransactionsForAddress`. Also the source for `gsfa`, `signatures`, and `token_owner_activity`. |
| `ddl/{local,cluster,replicated}/blocks_metadata.sql` | `blocks_metadata` (base table) | Per-slot block metadata (blockhash, parent, block_time, heights, rewards summary). | `getBlock`, `getBlockTime`, `getBlocks`, `getBlocksWithLimit`, `getFirstAvailableBlock` (and context slot calculations). |
| `ddl/{local,cluster,replicated}/entries.sql` | `entries` (base table) | PoH entries emitted by Fumarole/gRPC sources and the Jetstreamer ClickHouse plugin. | Ingestion/debugging data; not required by current RPC handlers. |
| `ddl/{local,cluster,replicated}/gsfa.sql` or `ddl/{local,cluster,replicated}/gsfa_nohot.sql` | `gsfa` (materialized view / index table) | Bucketed address -> (signature, slot, slot_idx, memo, err, block_time) rows derived from `transactions`. `gsfa_nohot.sql` excludes the configured hot-address set so those rows live only in `gsfa_hot`. | `getSignaturesForAddress` and the signature-side scan for `getTransactionsForAddress`. |
| `ddl/{local,cluster,replicated}/gsfa_hot.sql` | `gsfa_hot` (optional materialized view / index table) | Same shape as `gsfa`, but restricted to a configured set of "hot" addresses for faster lookups. | Optional performance optimization for `getSignaturesForAddress` when hot routing is configured in `superbank-rpc`. |
| `ddl/{local,cluster,replicated}/signatures.sql` | `signatures` (materialized view / index table) | Bucketed signature -> (slot, slot_idx, err) rows derived from `transactions` (includes all signatures, not just the primary). | `getSignatureStatuses`, plus internal "signature position" lookups used by `getTransaction` and pagination in address-based methods. |
| `ddl/{local,cluster,replicated}/token_owner_activity.sql` | `token_owner_activity` (optional materialized view / index table) | Bucketed (owner, token_account) -> (signature, slot, slot_idx, memo, err, block_time, balance_changed) rows derived from token balance diffs in `transactions`. | Enables `tokenAccounts` filters in `getTransactionsForAddress`. If this table is missing/unavailable, those filters are rejected. |
