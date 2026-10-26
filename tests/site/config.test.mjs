// Checks of the config reference data (site/js/config-data/*.js) against
// itself and against the Rust sources it documents. Run from the repo root:
//   node --test tests/site/config.test.mjs
//
// The drift checks read the clap structs, the YAML FileConfig structs and the
// direct std::env reads, and fail when the code gains or loses an env var,
// flag or YAML key that the page does not list (or lists but the code lacks).
// They do not check types, defaults or descriptions; review those by hand.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, readFileSync, readdirSync, statSync } from 'node:fs';
import { join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  COMPONENTS,
  COMPONENT_IDS,
  FEATURES,
  PRIMARY,
  RELATIONS,
  REQUIRE_KINDS,
  SOURCES,
  STATUSES,
  SUBCOMMANDS,
  buildModel,
  parseRequire,
  resolveRef,
} from '../../site/js/config-model.js';

const REPO_ROOT = fileURLToPath(new URL('../../', import.meta.url));
const read = (path) => readFileSync(join(REPO_ROOT, path), 'utf8');
const model = buildModel();

// --- Rust source readers -----------------------------------------------------

// Where each component's configuration is defined. `clap` files hold the
// derive structs, `yaml` the serde FileConfig, `roots` are scanned for direct
// env reads (std::env::var, var_os and the crates' small env_* helpers).
const CODE = {
  superbank: { clap: ['crates/superbank/src/cli.rs'], yaml: 'crates/superbank/src/cli.rs', roots: ['crates/superbank/src'] },
  rpc: { clap: ['crates/superbank-rpc/src/config.rs'], roots: ['crates/superbank-rpc/src'] },
  solparq: {
    clap: ['crates/superbank-solparq/src/config.rs'],
    roots: ['crates/superbank-solparq/src'],
    exclude: ['crates/superbank-solparq/src/read', 'crates/superbank-solparq/src/bin/superbank-solparq-read.rs'],
  },
  'solparq-read': {
    clap: ['crates/superbank-solparq/src/read/config.rs'],
    roots: ['crates/superbank-solparq/src/read', 'crates/superbank-solparq/src/bin/superbank-solparq-read.rs'],
  },
  verify: { clap: ['crates/superbank-verify/src/cli.rs'], yaml: 'crates/superbank-verify/src/cli.rs', roots: ['crates/superbank-verify/src'] },
  jetstreamer: { clap: [], roots: ['ingest/jetstreamer-clickhouse-plugin/src'] },
};

// Env names read by code that is not operator configuration.
const IGNORED_ENV = [
  /_TEST_URL$/, // integration tests against a live ClickHouse
  /^DISK_CACHE_TEST_/, // disk cache integration tests
  /_BENCH_/, // benchmarks
  /^PAYLOAD_LAYOUT_/, // payload layout benchmark output
  /^GETTX_DIAGNOSTIC_OUTPUT$/, // getTransaction diagnostics test output
  /^HOSTNAME$/, // solparq manifest producer metadata, set by the OS
];
const ignoredEnv = (name) => IGNORED_ENV.some((re) => re.test(name));

// Read by tracing's EnvFilter, not by a literal env::var call.
const IMPLICIT_ENV = new Set(['RUST_LOG']);

function rustFiles(path) {
  const abs = join(REPO_ROOT, path);
  if (!existsSync(abs)) return [];
  if (statSync(abs).isFile()) return path.endsWith('.rs') ? [path] : [];
  return readdirSync(abs).flatMap((name) => rustFiles(join(path, name)));
}

// Test-only files: `tests/` directories, `tests.rs`, `*_tests.rs`, `key_tests*`.
const isTestFile = (path) => /(^|\/)tests(\/|\.rs$)|_tests\.rs$|key_tests/.test(path);

// Also matches clap `env = "X"` attributes, so a clap struct in a file not
// listed under `clap` still has its env vars checked. (Flags are only checked
// in the listed clap files.)
const ENV_READ = /(?:(?:env::var(?:_os)?|\benv_(?:u64|usize|bool))\(\s*|\benv\s*=\s*)"([A-Z][A-Z0-9_]*)"/g;

function directEnvReads(id) {
  const { roots, exclude = [] } = CODE[id];
  const names = new Set();
  for (const file of roots.flatMap(rustFiles)) {
    if (isTestFile(file) || exclude.some((ex) => file === ex || file.startsWith(`${ex}/`))) continue;
    // Drop // comments (not `://` inside strings) so commented-out reads don't count.
    const code = read(file).replace(/(^|\s)\/\/.*$/gm, '$1');
    for (const [, name] of code.matchAll(ENV_READ)) names.add(name);
  }
  return names;
}

const kebab = (snake) => snake.replaceAll('_', '-');
const FIELD = /^(?:pub(?:\([a-z]+\))?\s+)?([a-z_][a-z0-9_]*)\s*:\s*\S/;

// Walks Rust source and yields each struct field with the attribute text that
// precedes it (attributes may span lines). Fields without attributes are only
// yielded with `bare`, for walking a known struct body.
function* fields(source, { bare = false } = {}) {
  let attrs = [];
  let pending = '';
  let depth = 0;
  for (const raw of source.split('\n')) {
    const line = raw.trim();
    if (depth > 0) {
      pending += ` ${line}`;
      depth += (line.match(/\[/g) ?? []).length - (line.match(/\]/g) ?? []).length;
      if (depth <= 0) {
        attrs.push(pending);
        depth = 0;
      }
      continue;
    }
    if (line.startsWith('//')) continue;
    if (line.startsWith('#[')) {
      const open = (line.match(/\[/g) ?? []).length - (line.match(/\]/g) ?? []).length;
      if (open > 0) {
        pending = line;
        depth = open;
      } else attrs.push(line);
      continue;
    }
    const m = FIELD.exec(line);
    if (m && (bare || attrs.length)) yield { name: m[1], attrs: attrs.join(' ') };
    if (line) attrs = [];
  }
}

// clap fields: env, flag, cfg features and hide, from #[arg(...)].
function clapFields(file) {
  const out = [];
  for (const { name, attrs } of fields(read(file))) {
    if (!/#\[(?:arg|clap)\(/.test(attrs) || /\bskip\b/.test(attrs)) continue;
    const env = /\benv\s*=\s*"([^"]+)"/.exec(attrs)?.[1];
    const long = /\blong\s*=\s*"([^"]+)"/.exec(attrs)?.[1] ?? (/\blong\b/.test(attrs) ? kebab(name) : undefined);
    out.push({
      field: name,
      env,
      flag: long && `--${long}`,
      features: [...attrs.matchAll(/cfg\(feature\s*=\s*"([^"]+)"\)/g)].map((m) => m[1]),
      hidden: /\bhide\s*=\s*true\b/.test(attrs),
    });
  }
  return out;
}

// YAML keys of `struct FileConfig`, honouring rename and rename_all.
function yamlKeys(file) {
  const source = read(file);
  const start = source.indexOf('struct FileConfig {');
  assert.ok(start !== -1, `${file}: struct FileConfig not found`);
  const header = source.slice(source.lastIndexOf('#[derive', start), start);
  const kebabCase = /rename_all\s*=\s*"kebab-case"/.test(header);
  const body = source.slice(start, source.indexOf('\n}\n', start));
  const keys = new Set();
  for (const { name, attrs } of fields(body.split('\n').slice(1).join('\n'), { bare: true })) {
    keys.add(/\brename\s*=\s*"([^"]+)"/.exec(attrs)?.[1] ?? (kebabCase ? kebab(name) : name));
  }
  return keys;
}

const entriesOf = (id) => model.entries.filter((e) => e.component === id);
const sorted = (set) => [...set].sort();

// --- Shape ---------------------------------------------------------------------

const ITEM_FIELDS = new Set(['key', 'env', 'flag', 'yaml', 'type', 'default', 'required', 'text', 'requires', 'relations', 'status', 'secret', 'source']);
const nonEmpty = (v) => typeof v === 'string' && v.trim().length > 0;

function proseProblems(where, text) {
  const problems = [];
  if (typeof text !== 'string') return problems;
  if (text.includes('<')) problems.push(`${where}: contains "<": ${text}`);
  if ((text.match(/`/g) ?? []).length % 2) problems.push(`${where}: unbalanced backticks: ${text}`);
  return problems;
}

test('components are the six documented binaries, each with groups and metadata', () => {
  assert.deepEqual(COMPONENT_IDS, ['superbank', 'rpc', 'solparq', 'solparq-read', 'verify', 'jetstreamer']);
  const problems = [];
  for (const c of COMPONENTS) {
    for (const key of ['label', 'summary', 'source', 'readme', 'intro']) if (!nonEmpty(c[key])) problems.push(`${c.id}: missing ${key}`);
    if (!PRIMARY.includes(c.primary)) problems.push(`${c.id}: primary must be one of ${PRIMARY.join(', ')}`);
    problems.push(...proseProblems(`${c.id} intro`, c.intro), ...proseProblems(`${c.id} summary`, c.summary));
    if (!Array.isArray(c.groups) || c.groups.length === 0) problems.push(`${c.id}: no groups`);
    const groupIds = new Set();
    for (const g of c.groups ?? []) {
      if (!/^[a-z0-9-]+$/.test(g.id ?? '')) problems.push(`${c.id}: bad group id ${g.id}`);
      if (groupIds.has(g.id)) problems.push(`${c.id}: duplicate group ${g.id}`);
      groupIds.add(g.id);
      if (!nonEmpty(g.title)) problems.push(`${c.id}/${g.id}: missing title`);
      if (!Array.isArray(g.items) || g.items.length === 0) problems.push(`${c.id}/${g.id}: no items`);
      problems.push(...proseProblems(`${c.id}/${g.id} intro`, g.intro));
    }
  }
  assert.deepEqual(problems, []);
});

test('every item is well formed', () => {
  const problems = [];
  for (const c of COMPONENTS) {
    for (const g of c.groups) {
      for (const item of g.items) {
        const where = `${c.id}/${g.id}/${item.env ?? item.yaml ?? item.flag ?? item.key}`;
        for (const field of Object.keys(item)) if (!ITEM_FIELDS.has(field)) problems.push(`${where}: unknown field ${field}`);
        if (!item.env && !item.flag && !item.yaml) problems.push(`${where}: needs env, flag or yaml`);
        if (item.env !== undefined && !/^[A-Z][A-Z0-9_]*$/.test(item.env)) problems.push(`${where}: bad env ${item.env}`);
        if (item.flag !== undefined && !/^--[a-z0-9][a-z0-9-]*$/.test(item.flag)) problems.push(`${where}: bad flag ${item.flag}`);
        if (item.yaml !== undefined && !/^[a-z0-9][a-z0-9-]*$/.test(item.yaml)) problems.push(`${where}: bad yaml key ${item.yaml}`);
        if (!nonEmpty(item.type)) problems.push(`${where}: missing type`);
        if (!nonEmpty(item.text)) problems.push(`${where}: missing text`);
        if (item.default !== undefined && typeof item.default !== 'string') problems.push(`${where}: default must be a string`);
        if (item.required !== undefined && item.required !== true && !nonEmpty(item.required)) problems.push(`${where}: required must be true or text`);
        if (item.status !== undefined && !STATUSES.includes(item.status)) problems.push(`${where}: unknown status ${item.status}`);
        if (item.secret !== undefined && typeof item.secret !== 'boolean') problems.push(`${where}: secret must be boolean`);
        for (const text of [item.text, item.type, item.default, item.required]) problems.push(...proseProblems(where, text));
      }
    }
  }
  assert.deepEqual(problems, []);
});

test('item ids are unique and anchor-safe', () => {
  assert.deepEqual(model.duplicates, []);
  for (const e of model.entries) assert.match(e.id, /^[a-z-]+\.[A-Za-z0-9_.-]+$/, e.id);
});

test('requires entries use known kinds and values', () => {
  const problems = [];
  const allowed = { feature: FEATURES, source: SOURCES, subcommand: SUBCOMMANDS };
  for (const c of COMPONENTS) {
    for (const g of c.groups) {
      for (const item of [{ requires: g.requires, env: `(group ${g.id})` }, ...g.items]) {
        for (const raw of item.requires ?? []) {
          const req = parseRequire(raw);
          const where = `${c.id}/${item.env ?? item.yaml ?? item.flag}: ${raw}`;
          if (!REQUIRE_KINDS.includes(req.kind)) problems.push(`${where}: unknown kind`);
          else if (req.kind === 'when') {
            if (!nonEmpty(req.equals)) problems.push(`${where}: when needs KEY=value`);
            if (!model.byId.has(`${c.id}.${req.key}`)) problems.push(`${where}: ${req.key} is not an item of ${c.id}`);
            problems.push(...proseProblems(where, req.equals));
          } else if (!allowed[req.kind].includes(req.value)) problems.push(`${where}: unknown ${req.kind}`);
        }
      }
    }
  }
  assert.deepEqual(problems, []);
  for (const e of model.entries) {
    if (e.features.length) assert.equal(e.component, 'rpc', `${e.id}: only superbank-rpc has Cargo features`);
    if (e.sources.length) assert.equal(e.component, 'superbank', `${e.id}: only superbank has --source`);
  }
});

test('relations point at existing items with known types', () => {
  const problems = [];
  for (const e of model.entries) {
    for (const rel of e.relations) {
      if (!(rel.type in RELATIONS)) problems.push(`${e.id}: unknown relation ${rel.type}`);
      const to = resolveRef(e.component, rel.to);
      if (!model.byId.has(to)) problems.push(`${e.id}: ${rel.type} target ${rel.to} does not exist`);
      if (to === e.id) problems.push(`${e.id}: relates to itself`);
    }
  }
  assert.deepEqual(problems, []);
});

test('every source and readme path exists in the repository', () => {
  const paths = new Set();
  for (const c of COMPONENTS) paths.add(c.source).add(c.readme);
  for (const e of model.entries) paths.add(e.source);
  for (const path of paths) {
    assert.ok(nonEmpty(path) && !path.startsWith('/') && !path.includes('..') && !path.includes('://'), `not repo-relative: ${path}`);
    assert.ok(existsSync(join(REPO_ROOT, path)), `missing: ${path}`);
  }
});

// --- Drift against the code ---------------------------------------------------

for (const id of COMPONENT_IDS) {
  test(`${id}: env vars match the code`, () => {
    const code = directEnvReads(id);
    for (const file of CODE[id].clap) for (const f of clapFields(file)) if (f.env) code.add(f.env);
    const documented = new Set(entriesOf(id).map((e) => e.env).filter(Boolean));
    const undocumented = sorted(code).filter((name) => !documented.has(name) && !ignoredEnv(name));
    const stale = sorted(documented).filter((name) => !code.has(name) && !IMPLICIT_ENV.has(name));
    assert.deepEqual(undocumented, [], `env vars read by ${id} but missing from site/js/config-data`);
    assert.deepEqual(stale, [], `env vars listed for ${id} that the code no longer reads`);
  });

  test(`${id}: flags match the code`, () => {
    const code = CODE[id].clap.flatMap(clapFields).filter((f) => f.flag);
    const documented = new Set(entriesOf(id).map((e) => e.flag).filter(Boolean));
    const documentedEnv = new Set(entriesOf(id).map((e) => e.env).filter(Boolean));
    // Hidden flags may be documented by their env var alone.
    const undocumented = code
      .filter((f) => !documented.has(f.flag) && !(f.hidden && f.env && documentedEnv.has(f.env)))
      .map((f) => f.flag);
    const known = new Set(code.map((f) => f.flag));
    const stale = sorted(documented).filter((flag) => !known.has(flag));
    assert.deepEqual(sorted(undocumented), [], `flags of ${id} missing from site/js/config-data`);
    assert.deepEqual(stale, [], `flags listed for ${id} that the code does not define`);
  });

  test(`${id}: env and flag belong to the same clap field`, () => {
    const byEnv = new Map(CODE[id].clap.flatMap(clapFields).filter((f) => f.env).map((f) => [f.env, f]));
    const mismatched = entriesOf(id)
      .filter((e) => e.env && e.flag && byEnv.has(e.env) && byEnv.get(e.env).flag !== e.flag)
      .map((e) => `${e.env}: page says ${e.flag}, code says ${byEnv.get(e.env).flag}`);
    assert.deepEqual(mismatched, []);
  });

  test(`${id}: hidden clap fields are marked deprecated`, () => {
    const hidden = CODE[id].clap.flatMap(clapFields).filter((f) => f.hidden);
    const notDeprecated = hidden
      .map((f) => entriesOf(id).find((e) => (f.env && e.env === f.env) || (f.flag && e.flag === f.flag)))
      .filter((e) => e && e.status !== 'deprecated')
      .map((e) => e.id);
    assert.deepEqual(notDeprecated, []);
  });
}

for (const id of ['superbank', 'verify']) {
  test(`${id}: YAML keys match FileConfig`, () => {
    const code = yamlKeys(CODE[id].yaml);
    const documented = new Set(entriesOf(id).map((e) => e.yaml).filter(Boolean));
    assert.deepEqual(sorted(code).filter((k) => !documented.has(k)), [], `FileConfig keys of ${id} missing from the page`);
    assert.deepEqual(sorted(documented).filter((k) => !code.has(k)), [], `YAML keys listed for ${id} that FileConfig lacks`);
  });
}

test('rpc: the only YAML key is the request filter list', () => {
  const keys = entriesOf('rpc').map((e) => e.yaml).filter(Boolean);
  assert.deepEqual(keys, ['rpc-parameter-filters']);
  assert.match(read('crates/superbank-rpc/src/request_filter.rs'), /rpc_parameter_filters: Vec</);
});

test('rpc: every #[cfg(feature)] gate on a config field is labelled', () => {
  const gated = clapFields('crates/superbank-rpc/src/config.rs').filter((f) => f.features.length);
  // Guard against the parser silently matching nothing.
  assert.ok(gated.length > 50, `expected many feature-gated fields, parsed ${gated.length}`);
  const missing = [];
  for (const f of gated) {
    const entry = model.byId.get(`rpc.${f.env}`);
    for (const feature of f.features) if (!entry?.features.includes(feature)) missing.push(`${f.env} needs feature:${feature}`);
  }
  assert.deepEqual(missing, []);
});

// --- Derived links ---------------------------------------------------------------

test('"also in" links shared names across components', () => {
  const alsoIn = (id) => model.alsoIn.get(id) ?? [];
  for (const [id, others] of [
    ['rpc.CLICKHOUSE_URL', ['superbank.CLICKHOUSE_URL', 'verify.CLICKHOUSE_URL']],
    ['rpc.METRICS_PORT', ['superbank.METRICS_PORT', 'verify.METRICS_PORT']],
    ['solparq.SOLPARQ_ARCHIVE_S3_ENDPOINT', ['superbank.SOLPARQ_ARCHIVE_S3_ENDPOINT', 'solparq-read.SOLPARQ_READ_ARCHIVE_S3_ENDPOINT']],
    ['rpc.DRAGONSMOUTH_ENDPOINT', ['superbank.DRAGONSMOUTH_ENDPOINT']],
  ]) {
    for (const other of others) assert.ok(alsoIn(id).includes(other), `${id} should link to ${other}`);
    for (const other of others) assert.ok(alsoIn(other).includes(id), `${other} should link back to ${id}`);
  }
  for (const [id, others] of model.alsoIn) {
    for (const other of others) assert.notEqual(model.byId.get(other).component, model.byId.get(id).component, `${id}: also-in stays across components`);
  }
});

test('relations are mirrored on their targets', () => {
  const links = model.links.get('rpc.DISK_CACHE_QUERY_TIMEOUT_MS');
  assert.ok(links.some((l) => l.label === 'Caps' && l.to === 'rpc.DISK_CACHE_GET_TX_TIMEOUT_MS' && l.inverse));
  assert.ok(model.links.get('rpc.DISK_CACHE_GET_TX_TIMEOUT_MS').some((l) => l.label === 'Capped by' && l.to === 'rpc.DISK_CACHE_QUERY_TIMEOUT_MS'));
  for (const [id, list] of model.links) {
    const seen = new Set(list.map((l) => `${l.label}|${l.to}`));
    assert.equal(seen.size, list.length, `${id}: repeated link`);
  }
});

test('a toggle never requires itself, and its group condition reaches its siblings', () => {
  const toggle = model.byId.get('rpc.DISK_CACHE_ENABLED');
  assert.ok(!toggle.requires.some((r) => r.kind === 'when' && r.key === 'DISK_CACHE_ENABLED'));
  assert.deepEqual(toggle.features, ['disk-cache']);
  const sibling = model.byId.get('rpc.DISK_CACHE_RETAIN_SLOTS');
  assert.ok(sibling.requires.some((r) => r.kind === 'when' && r.value === 'DISK_CACHE_ENABLED=true'));
});

// --- Self-checks of the source readers --------------------------------------------

test('source readers see the shapes they rely on', () => {
  const rpc = clapFields('crates/superbank-rpc/src/config.rs');
  const host = rpc.find((f) => f.env === 'RPC_HOST');
  assert.equal(host?.flag, '--host', 'explicit long = "host"');
  const retain = rpc.find((f) => f.env === 'DISK_CACHE_RETAIN_SLOTS');
  assert.deepEqual(retain?.features, ['disk-cache'], 'cfg on its own line before the doc comment');
  assert.ok(rpc.find((f) => f.env === 'DISK_CACHE_PATH')?.hidden, 'hide = true');
  assert.ok(directEnvReads('rpc').has('SIGNATURE_SLOT_CACHE_SIZE'), 'env_usize helper');
  assert.ok(!directEnvReads('rpc').has('SUPERBANK_OWNER_SHARD_CLUSTER_URLS'), 'tests.rs is skipped');
  assert.ok(yamlKeys('crates/superbank/src/cli.rs').has('from-slot'), 'serde rename');
  assert.ok(yamlKeys('crates/superbank/src/cli.rs').has('fumarole-x-token'), 'rename_all kebab-case');
  assert.ok(yamlKeys('crates/superbank-verify/src/cli.rs').has('range'), 'no rename_all');
  assert.equal(relative(REPO_ROOT, join(REPO_ROOT, 'site')), 'site');
});
