# getTransaction follow-up: correctness and local measurements

These changes preserve the existing ClickHouse schemas, cache format, and retention
settings. Measurements below use isolated synthetic fixtures.

## Implemented

- Disk transaction reads distinguish found, definitive absence, and unavailable.
  A timeout or database failure cannot become null merely because a separate coverage
  query succeeds. Coverage checks share the read deadline and generation validation.
- A slot must already be covered when the signature search begins before a successful
  miss can prove absence there. Ordinary appends need not change the epoch, so this
  separately prevents a coverage-publication race.
- An index entry with an unavailable payload falls back to primary. Successful
  pinned-slot misses and skipped slots retain null semantics; unpinned misses fall back.
- Disk payload reads retain `slot_idx`, sharing the existing primary position lookup
  and its slot-only stale/legacy fallback. One admission permit covers payload retries.
  Payload ownership passes directly into the existing hydration path.

No new persistent index, retention extension, cache format, or production setting is
introduced. The existing primary two-request route remains enabled.

## Benchmark method

Run `python3 scripts/test/benchmark-get-transaction.py --output /tmp/gettx-benchmark`.
The script creates and removes its own three-node ClickHouse 26.2.3.2 Docker cluster,
with a transparent local TCP gateway, two CPUs and 8 GiB per node. The initial 2 GiB
fixture limit failed during ingestion and is not a performance result.

Each payload copy contains three million deterministic synthetic transactions across
six slot partitions, with signature distribution independent of payload distribution.
The repository schema and complete transaction projection are used. Payload logs vary
in size; this is not a sample of production payload distributions. The copies differ
only in `index_granularity` (8192/1024); `index_granularity_bytes=10485760` and codecs
are held constant. Ingestion uses 50,000-row batches; both copies maintain equivalent signature materialized
views. Both payload copies are merged
before lookup measurements; merge logs and part footprints are retained.

Five runs alternate variant order over the same 50 seeded signatures. Each variant
has 250 warm and 250 ClickHouse-cache-cleared observations. Cache-cleared means mark
and uncompressed caches were dropped before each lookup; filesystem caches are
uncontrolled. Query-result caching is disabled. Full RowBinary payload digests must
match; the two-request baseline decodes the returned position before its payload query.

HTTP measurements include local gateway/client overhead but exclude Rust admission,
hydration, and serialization. Logical bytes are not physical I/O. Coordinator CPU
counters are recorded separately and do not establish total cluster CPU. The optional
per-request delay and the 100 ms RTT arithmetic model are not measurements of a deployed network.

## Gateway decision

The tested scalar-array SQL candidate resolves a signature and fetches the positioned
payload in one request. Its result contract fails the stale-position prerequisite:
an absent signature and an existing signature with a stale position both return an
empty body, while a slot-only payload lookup still finds the latter transaction.
Integrating this query as written would either lose the existing fallback or repeat
signature resolution on empty results. It is therefore **not integrated**.

This rejects the tested candidate, not every possible single-query design. A future
candidate must return enough position/absence information to preserve fallback and
then pass pruning, repeated-resolution, cancellation, admission, and performance gates.
Candidate-specific cancellation and activation work stops at this failed correctness
gate. The existing cancellation protocol suite was run as a regression check;
it is not proof of getTransaction cancellation through the deployed gateway.

## Results

[Machine-readable results](docs/benchmarks/gettransaction-2026-09-11.json) include
plans, settings, merge/part measurements and the raw-report checksum. The raw report is retained locally; rerun the script to generate the complete query telemetry. All 2,500 observations have terminal
query telemetry, no query errors, and matching full payload digests.

Client times are milliseconds. Read counts are per logical lookup; the two-request
baseline includes both signature and payload queries.

| Variant | Warm p50 | Warm p95 | Cache-cleared p95 | Mean logical rows | Mean logical MiB |
|---|---:|---:|---:|---:|---:|
| Slot only, 8192 | 17.5 | 22.6 | 26.6 | 16,417 | 5.39 |
| Full position, 8192 | 16.4 | 20.2 | 25.3 | 8,684 | 5.45 |
| Full position, 1024 | 13.6 | 17.2 | 19.9 | 1,597 | 0.66 |
| Two gateway requests | 22.5 | 30.7 | 27.4 | 9,209 | 5.49 |
| Combined SQL candidate | 20.8 | 25.5 | 26.9 | 9,207 | 5.49 |

Full-position lookup reduced logical row accounting but did not materially reduce
payload bytes at granularity 8192. Its local tail-latency improvement is modest and
was sensitive to run conditions in exploratory measurements. This does not establish
a deployed p99 improvement.

Granularity 1024 reduced logical payload bytes by **87.9%** relative to the 8192
full-position query; warm p95 fell from **20.2 to 17.2 ms**. The local EXPLAIN plans
include the owning shard and prune to one of its two payload parts. This is useful
query-shape evidence, not a production sizing or rollout gate.

Across all three nodes, active payload parts occupied **1,011,149,892 bytes** at 8192
and **1,011,903,737 bytes** at 1024: **+0.075%** in this synthetic dataset. Mark bytes
rose from **138,365 to 405,580** (~2.9x); reported primary-key memory rose from
**3,408 to 23,568 bytes** (~6.9x). These small, compressed indexes do not establish
full-retention memory requirements.

Both ingestion paths maintained equivalent signature views. Three million payload
rows took **19.4 s** at 8192 versus **22.8 s** at 1024, including per-batch footprint
sampling. This is one sequential ingestion trial, not a steady-state throughput
conclusion. Successful payload merge durations summed across nodes were **12.15 s**
and **11.07 s** respectively; background scheduling prevents a causal merge-speed claim.

Parts awaiting deletion matter for the 2 TB budget: each payload copy still occupied
about **2.63 GB including inactive parts**, versus about **1.01 GB active**. Sampling
during ingestion observed **6.19 GB** across both payload and signature copies.
These are observed footprints, not a guaranteed peak bound. Keep merge/deletion
headroom; the small active-size delta does not justify filling production disks.

The combined candidate returned zero bytes for both the missing-signature and stale
position fixtures; slot-only fallback returned the existing **804-byte** payload.
It therefore fails correctness before performance/cancellation acceptance. Its warm
p95 was 25.5 ms versus 30.7 ms for two requests, but coordinator CPU counters increased
~6.6% in cache-cleared measurements and there is no production gateway validation.
The saved-round-trip sensitivity model is not an adoption result.

**Decision:** retain the two cache fixes. Keep gateway consolidation and granularity
changes experimental; change neither production routing nor production table settings.


## Validation

- `cargo fmt --all -- --check` and both workspace/all-feature RPC Clippy commands passed.
- `cargo test --workspace --locked`: 666 passed; four opt-in tests ignored.
- `cargo test -p superbank-rpc --all-features --locked`: 551 passed; six opt-in tests ignored.
- Explicit disk-cache integration passed, including covered-slot admission timeout,
  unavailable payload table, in-flight invalidation, concurrent append/coverage,
  skipped/uncovered slots, pinned mismatch, and stale position fallback.
- ClickHouse protocol fixture passed, including three production Rust integrations,
  positive disconnect cases and negative gateway controls.
- Basic k6 on the final build: 1,244 requests, no failures, over ten seconds.
- HTTP transaction parity on the final build: 1,041 full-envelope comparisons and
  1,122 passing checks over 2,122 HTTP requests. The corpus included legacy/v0/v1,
  a 32 KiB metadata log, all encodings, version errors and request-ID variants.
  RPC metrics confirmed disk-cache serving of present transactions.
- The scoped complexity gate passed for all 25 changed/new Rust functions against
  `89d47aac`: new functions <=10 McCabe; existing functions did not increase.

The benchmark and RPC fixtures are local. Deployed RPC latency, deployed-gateway
cancellation, full-retention performance and production granularity sizing remain
unvalidated. These local checks do not validate a deployment.
