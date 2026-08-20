# Analyst-friendly views

Plain (non-materialized) SQL views over `default.transactions`
(the `view_*.sql` files under `ddl/additional/`) that make the
raw schema directly usable for ad-hoc analytics, without knowing its storage
conventions. They encapsulate three pitfalls of the raw table:

1. **Binary keys** — pubkeys and signatures are stored as raw bytes
   (`FixedString(32/64)`); the views re-encode everything to base58.
2. **Address lookup tables** — accounts loaded through ALTs live in separate
   columns (`meta_loaded_addresses_*`); the views merge them, so filtering by
   account does not silently miss v0 transactions.
3. **Votes** — vote transactions dominate row counts and skew every
   aggregate; the analytical views pre-filter `is_vote = 0`
   (`transactions_decoded` keeps them, being strictly 1:1).

## Views

| View | Grain | Purpose |
|---|---|---|
| `tx_summary` | one row per non-vote transaction | Readable transaction feed: fee payer, accounts, programs, fees, CU, per-account SOL deltas |
| `sol_transfers` | one row per (tx, account) with a SOL balance change | SOL flow analysis; failed txs kept (fee debit), filter with `success` |
| `token_transfers` | one row per (tx, token account) with an SPL balance change | Token flow analysis; `delta` is scaled by mint decimals |
| `transactions_decoded` | strictly 1:1 with `transactions` (votes included) | Full-width readable projection for inspecting individual transactions; instruction payloads are hex-encoded |

## Applying

These views are **opt-in**. They live under `ddl/additional/`
(`local/`, `cluster/`, `replicated/`) and are **not** applied by Docker
Compose, Tilt, or the k8s DDL job. Clustered variants add
`ON CLUSTER '{cluster}'`. Apply them after `transactions.sql` — the only
object they require.

To apply (single-node example):

```bash
for f in ddl/additional/local/view_*.sql; do
  clickhouse-client --multiquery < "$f"
done
```

## Semantics notes

- `token_transfers` records **balance changes**, not strict transfers: a
  wSOL wrap/unwrap or a token mint/burn appears as a delta without a
  counterparty. `sum(delta_raw) GROUP BY signature, mint HAVING sum != 0`
  isolates exactly those events (pure transfers always net to zero).
- The same operation can appear in both `sol_transfers` (lamports moving in
  or out of a wSOL token account) and `token_transfers` (the wSOL delta).
  When summing "SOL volume" across both views, exclude wSOL token accounts
  on one side or you count wraps twice.
- In `sol_transfers`, a delta of ~2,039,280 lamports on a token account is
  usually the rent-exempt deposit of its creation, not a payment.

  
## Consistency checks

Both must return `ok = 1`; useful after a backfill or an ingestor change:

```sql
-- lamports are conserved per transaction: deltas sum to -fee
SELECT countIf(arraySum(c -> c.2, balance_changes) != -toInt64(fee_lamports)) = 0 AS ok
FROM tx_summary;

-- flat and packed SOL deltas agree across views
SELECT (SELECT sum(delta_lamports) FROM sol_transfers)
     = (SELECT -sum(fee_lamports) FROM tx_summary) AS ok;
```

## Performance notes

- Views are computed at query time: base58 encoding is cheap, but predicates
  on `accounts` still scan the underlying table. For selective per-address
  lookups, resolve `(slot, signature)` through the `gsfa` index first (see
  example 3 below).
- The base table is partitioned by epoch (`intDiv(slot, 432000)`). Bound
  `slot` (or `block_time`) in analytical queries to get partition pruning.
- If query-time encoding ever becomes a bottleneck for a hot access path,
  promote the relevant view to a materialized view; the SELECTs are designed
  to be reusable as-is.

## Example queries

### 1. Transaction sample

```sql
SELECT signature, block_time, success, fee_payer, programs, fee_lamports
FROM tx_summary
ORDER BY slot DESC, slot_idx DESC
LIMIT 10;
```

### 2. Filter by account

```sql
SELECT count()
FROM tx_summary
WHERE has(accounts, 'A7FMMgue4aZmPLLoutVtbC7gJcyqkHybUieiaDg9aaVE');
```

### 3. Filter by account, fast (gsfa index)

Same result as example 2, but resolves matching transactions through the
`gsfa` materialized view instead of scanning:

```sql
SELECT count()
FROM tx_summary
WHERE (slot, signature) IN
(
    SELECT slot, base58Encode(signature)
    FROM gsfa
    WHERE address = CAST(base58Decode('A7FMMgue4aZmPLLoutVtbC7gJcyqkHybUieiaDg9aaVE') AS FixedString(32))
);
```

### 4. SOL delta per account over time (filter + group by)

```sql
SELECT toStartOfHour(block_time) AS h,
       count() AS txs,
       round(sum(delta_sol), 4) AS sol_delta
FROM sol_transfers
WHERE account = 'A7FMMgue4aZmPLLoutVtbC7gJcyqkHybUieiaDg9aaVE'
GROUP BY h
ORDER BY h;
```

### 5. Token flows by mint

```sql
SELECT mint,
       count() AS transfers,
       uniq(owner) AS owners,
       round(sum(abs(delta)), 2) AS gross_volume
FROM token_transfers
GROUP BY mint
ORDER BY transfers DESC
LIMIT 10;
```

### 6. Inspect a single transaction (all fields, readable)

```sql
SELECT *
FROM transactions_decoded
WHERE signature = 'WxktzjjGjXeydZ6CiKMUpeAK2B3VHA1Eqp6naLw1wBFBLgAK4zDT8pdmrN1UYsYp5LBvqSzC25e7gzDoaAMmidU'
FORMAT Vertical;
```

Note: filtering by base58 signature through this view cannot use the bloom
filter index on the raw column. For an indexed lookup, filter the base table
on `signature = base58Decode('...')` (add a slot bound if known), or go
through `gsfa`.

### 7. Program activity

```sql
SELECT program,
       count() AS txs,
       round(avg(compute_units)) AS avg_cu
FROM tx_summary
ARRAY JOIN programs AS program
GROUP BY program
ORDER BY txs DESC
LIMIT 10;
```
