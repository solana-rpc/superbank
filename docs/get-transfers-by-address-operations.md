# `getTransfersByAddress` operations and historical coverage

## Current contract

The transfers materialized view processes transaction inserts after the view is installed. It does
not populate rows for transactions already in ClickHouse. Until verified coverage exists, the
endpoint is a live-forward indexed-transfer endpoint, not an historical-completeness guarantee.

`amount` is a raw unsigned base-unit quantity. The RPC accepts a JSON unsigned integer or a
base-10 string and rejects fractional and floating-point values. `feeAmount` is a raw Token-2022
`TransferCheckedWithFee` fee when known and `null` otherwise; it is not UI-denominated.

## Required resumable backfill design

Before claiming historical coverage, introduce an operator-owned, versioned coverage record and
backfill procedure. A coverage record must include at least the schema/extraction version, an
inclusive non-overlapping slot range, source and emitted-row counts, a deterministic validation
digest, run identifier, timestamps, and state (`planned`, `running`, `verified`, or `failed`).

The worker should claim one bounded slot range at a time, derive rows with the same extraction
logic as the live view, and validate the destination before changing the record to `verified`.
Retries reuse the same range and run identifier or create a new attempt linked to it; a failed or
interrupted attempt is never coverage. The destination insert must be idempotent for an identical
range, and a retry must fail closed if its extraction version or validation digest differs from an
already verified range.

The public endpoint must remain conservative while ranges are missing, overlapping, failed, or
derived by a different extraction version. A future API coverage signal should report either a
verified interval for the requested query or `unknown`; it must not infer completeness from table
row counts or the oldest transfer slot.

## Address-oriented layout evaluation

The current transfer order begins with `slot`, while this endpoint filters on either
`from_user_account` or `to_user_account`. Evaluate these alternatives on a production-like clone:

1. Two projections ordered by `(from_user_account, slot DESC, ...)` and
   `(to_user_account, slot DESC, ...)`.
2. A denormalized address-leg table with one row per participating address, ordered by
   `(address, slot DESC, ...)`.

For a representative set of cold and active addresses, run `EXPLAIN indexes = 1` and capture
`read_rows`, `read_bytes`, disk cost, ingest cost, and p95 latency for both directions and mixed
filters. Select neither layout until those measurements are reviewed; projections alter storage
and merge behavior, while a leg table changes write amplification and migration semantics.

## Decisions required from the submitter

1. Approve the coverage-record schema and its durable owner (ClickHouse, control-plane database,
   or another authoritative store).
2. Choose the client-visible coverage contract: response field, header, separate capability method,
   or intentionally no historical claim until a later API version.
3. Choose projections or an address-leg table only after the representative benchmark evidence is
   available, including acceptable storage and ingest overhead.
