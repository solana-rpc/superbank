# Codex Agent Guide (Superbank)

This repo uses `AGENTS.md` as the "agent contract": repo-specific instructions that should
shape how Codex plans, edits, runs checks, and reports results.

## What `AGENTS.md` Is (And How To Use It)

`AGENTS.md` exists to keep changes consistent and low-friction across contributors and
agents. It should be the first file you read at the start of any task.

Use it to:
- Confirm the repo layout and which crates exist in the Cargo workspace.
- Copy/paste the right build, run, lint, and test commands.
- Follow the PR hygiene expectations (scope control, testing notes, no secrets).

If `AGENTS.md` conflicts with the repository (missing paths, crates that do not exist, stale
commands), prefer repo reality:
- Confirm workspace members in `Cargo.toml`.
- Prefer crate docs: `crates/superbank/README.md` and `crates/superbank-rpc/README.md`.
- Prefer shipped scripts under `scripts/` and tests under `tests/`.
- If scope allows, update `AGENTS.md` in the same PR; otherwise, call out the mismatch.

Repo anchors you will use often:
- Rust workspace crates: `crates/superbank/` (ingestor) and `crates/superbank-rpc/` (JSON-RPC server)
- ClickHouse DDL: `ddl/`
- Load tests: `tests/k6/` (start at `tests/k6/README.md`)
- Helper scripts: `scripts/dev/` and `scripts/test/`

## Planning vs Execution

### Planning (before editing)

Do enough upfront work to make the implementation mechanical:
- Restate the goal and constraints in 1-3 sentences (scope, files allowed, ASCII-only, etc.).
- Identify the smallest set of files to read to be correct.
- Decide what "done" means (acceptance criteria) and how you will validate it.
- List the exact commands you will run (and which ones are optional).
- If the task changes behavior, identify the tests that guard it (unit/integration and k6).

### Execution (after the plan is stable)

Keep the change tight and verifiable:
- Edit only what the plan requires; avoid drive-by refactors.
- Prefer existing repo patterns and helpers (scripts, existing config parsing, existing error shapes).
- Run the checks that match the change and report results.
- If you discover the task needs more scope, stop and get alignment before expanding.

## Copy/Paste Prompt Templates

### 1) Doc-only change (no code)

```text
You are Codex working in {REPO_ROOT}.

Task:
- <describe the doc change>

Scope:
- Only edit/add: <list the doc files>
- Do not change Rust code, scripts, or config defaults.

Constraints:
- ASCII only.
- Keep it repo-specific and copy/paste runnable.

Required context:
- Read `AGENTS.md` and any referenced docs before writing.

Validation:
- Ensure every referenced path exists in this repo.
- If you mention commands, they must match this repo's actual workspace/crates/scripts.

Deliverable:
- Provide the final doc content and a brief change summary.
```

### 2) Code change + required tests

```text
You are Codex working in {REPO_ROOT}.

Task:
- <describe the code change>

Scope:
- Only edit: <paths or crates>
- Add or update tests as needed.

Constraints:
- Keep changes minimal; avoid refactors unrelated to the task.

Plan first:
- List files you will inspect and the exact tests/commands you will run.

Implementation requirements:
- If you change CLI flags, env vars, scripts, or response formats, update the relevant docs in the same PR.

Validation (run and report results):
- cargo fmt --all -- --check
  (If it fails: cargo fmt --all)
- cargo clippy --workspace --all-targets --locked -- -D warnings
- cargo clippy -p superbank-rpc --all-targets --all-features --locked -- -D warnings
- cargo test --workspace --locked

Optional (stricter; matches `.pre-commit-config.yaml`):
- cargo clippy --all-targets --all-features -- -D warnings
- cargo test --all --all-features

Deliverable:
- A concise summary of what changed and why, plus the commands run and results.
```

### 3) RPC behavior change + k6 validation

```text
You are Codex working in {REPO_ROOT}.

Task:
- <describe the RPC behavior change (method, params, response shape, ClickHouse query)>

Scope:
- Limit changes to `crates/superbank-rpc/` (and tests/docs if needed).

Constraints:
- Keep behavior changes explicit and tested.
- Avoid drive-by performance refactors unless requested.

Plan first:
- Identify the handler/query path you will touch and how you will validate correctness.

Local run:
- Ensure ClickHouse is available and has schemas from `ddl/`.
- Run superbank-rpc (example):
  RPC_HOST=0.0.0.0 RPC_PORT=8899 CLICKHOUSE_URL=http://localhost:8123 CLICKHOUSE_DATABASE=default \
    cargo run -p superbank-rpc --
  - Alternatively use the helper script and override defaults as needed:
    CLICKHOUSE_URL=http://localhost:8123 CLICKHOUSE_DATABASE=default CLICKHOUSE_USER=default CLICKHOUSE_PASSWORD= scripts/dev/run-local-rpc.sh

k6 validation (run and report results):
- Minimum:
  k6 run tests/k6/scenarios/basic/superbank-rpc-get-signatures.js -e RPC_URL=http://localhost:8899
- Or run the suite script:
  scripts/test/run-k6.sh

Also run and report:
- cargo fmt --all -- --check
  (If it fails: cargo fmt --all)
- cargo clippy --workspace --all-targets --locked -- -D warnings
- cargo test --workspace --locked
- If your change affects optional superbank-rpc features, run:
  cargo clippy -p superbank-rpc --all-targets --all-features --locked -- -D warnings
  cargo test -p superbank-rpc --all-features --locked

Deliverable:
- Summary of the RPC change, plus the exact commands run and their key results (k6 pass/fail and any threshold failures).
```

## PR / Final Output Expectations

When you open a PR (or when you report work back), include:

1. What changed
- 1-2 lines describing behavior changes, not just file names.

2. Commands run
- A copy/paste list, exactly as executed.

3. Results
- For Rust: whether fmt/clippy/tests passed.
- For k6: scenario(s) run, pass/fail, and any notable latency/threshold outcomes.

4. Scope control
- Avoid mixing unrelated cleanup with functional changes.
- If you intentionally changed more than requested, say why.
