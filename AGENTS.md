# Repository Guidelines

## Project Structure & Module Organization
- Rust workspace (root `Cargo.toml`): `superbank-workspace`, `crates/superbank/` (ingestor), and `crates/superbank-rpc/` (JSON-RPC server; bin: `superbank-rpc`).
- ClickHouse DDL: `ddl/`
- Load tests: `tests/k6/`
- Helper scripts: `scripts/`
- Deploy manifests/images: `deploy/`
- Tilt local k8s dev: `Tiltfile`, `scripts/dev/setup-tilt.sh`
- Standalone ingestion plugin workspace (not in the root Cargo workspace): `ingest/jetstreamer-clickhouse-plugin/`

Config lives in `superbank.example.yaml`; copy to `superbank.yaml` for local runs. Build outputs land in `target/`.

## Build, Test, and Development Commands
- Build (release):
  `cargo build --release -p superbank -p superbank-rpc`
- Run ingestor with local config:
  `cargo run -p superbank -- --config superbank.yaml`
- Run RPC server (local ClickHouse):
  `RPC_HOST=0.0.0.0 RPC_PORT=8899 CLICKHOUSE_URL=http://localhost:8123 CLICKHOUSE_DATABASE=default cargo run -p superbank-rpc --`
  (scripted helper: `scripts/dev/run-local-rpc.sh`)
- Checks (CI-style; see `.github/workflows/ci.yml`):
  `cargo fmt --all -- --check`
  `cargo clippy --workspace --all-targets --locked -- -D warnings`
  `cargo clippy -p superbank-rpc --all-targets --all-features --locked -- -D warnings`
  `cargo test --workspace --locked`
  `cargo test -p superbank-rpc --all-features --locked`
- Load tests:
  `scripts/test/run-k6.sh` (or run a single scenario under `tests/k6/scenarios/`)

## Coding Style & Naming Conventions
- Use `rustfmt` (stable toolchain with `rustfmt`/`clippy`) for formatting; do not hand-format.
- Follow Rust naming: `snake_case` for modules/functions, `PascalCase` for types, `SCREAMING_SNAKE_CASE` for constants.

## Testing Guidelines
- RPC performance and correctness checks live in `tests/k6/` (see `tests/k6/README.md` for scenarios).
- If changing RPC behavior, ClickHouse queries, or response formats, run at least the basic k6 test and note results in the PR.

## Agent Workflow (Codex)
- Read `README.md`, `AGENTS.md`, and the relevant crate README(s) before making changes.
- Keep diffs scoped; avoid drive-by refactors.
- Prefer non-destructive git operations; do not rewrite history unless explicitly asked.
- Run the CI-style checks appropriate to the change; for doc-only changes, verify referenced paths/commands exist.
- When changing CLI flags, env vars, or scripts, update the docs in the same change.

## Commit & Pull Request Guidelines
- Commit messages are short and imperative; optional Conventional Commit prefixes (`feat:`, `fix:`, `chore:`) are common in history.
- PRs should include a clear description, testing notes, and any config/script changes. Update `README.md` and `superbank.example.yaml` when adding or changing pipeline config or plugins.

## Configuration & Secrets
- Keep secrets (API tokens, ClickHouse credentials) out of git; use `superbank.yaml` or environment variables locally.
- Document any new required env vars or config fields in `README.md` and `superbank.example.yaml`.
