// Checks of scripts/site/build-info.mjs, which writes site/build-info.json in
// the Pages build. Run from the repo root:
//   node --test tests/site/build-info.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { fileURLToPath } from 'node:url';
import {
  VERSIONS,
  cargoLockVersions,
  collectBuildInfo,
  composeImageTags,
  gitlinkCommit,
  importmapVersions,
  workspaceField,
} from '../../scripts/site/build-info.mjs';
import { parseBuildInfo } from '../../site/js/build-info.js';

const REPO_ROOT = fileURLToPath(new URL('../../', import.meta.url));

const LOCK = `version = 4

[[package]]
name = "clickhouse"
version = "0.14.3"
source = "registry+https://github.com/rust-lang/crates.io-index"

[[package]]
name = "clickhouse-rs"
version = "1.1.0-alpha.1"

[[package]]
name = "clickhouse"
version = "0.15.1"
dependencies = [
 "name = \\"not-a-package\\"",
]
`;

test('cargoLockVersions finds every version of exactly that crate', () => {
  assert.deepEqual(cargoLockVersions(LOCK, 'clickhouse'), ['0.14.3', '0.15.1']);
  assert.deepEqual(cargoLockVersions(LOCK, 'clickhouse-rs'), ['1.1.0-alpha.1']);
  assert.deepEqual(cargoLockVersions(LOCK, 'click'), [], 'no prefix matches');
  assert.deepEqual(cargoLockVersions(LOCK, 'not-a-package'), [], 'dependency strings are not packages');
  assert.deepEqual(cargoLockVersions('', 'clickhouse'), []);
});

test('workspaceField reads only the [workspace.package] table', () => {
  const toml = `[package]\nversion = "9.9.9"\n\n[workspace.package]\nversion = "0.6.0"\nrust-version = "1.98.1"\n\n[workspace.dependencies]\nversion = "1.0"\n`;
  assert.equal(workspaceField(toml, 'version'), '0.6.0');
  assert.equal(workspaceField(toml, 'rust-version'), '1.98.1');
  assert.equal(workspaceField(toml, 'edition'), undefined);
  assert.equal(workspaceField('[workspace.package]\nversion = "1.2.3"', 'version'), '1.2.3', 'last table in the file');
});

test('composeImageTags and importmapVersions collect distinct values', () => {
  const yaml = `services:\n  a:\n    image: clickhouse/clickhouse-server:26.1.2.11\n  b:\n    image: "clickhouse/clickhouse-server:26.1.2.11"\n  c:\n    image: other/clickhouse/clickhouse-server:1\n`;
  assert.deepEqual(composeImageTags(yaml, 'clickhouse/clickhouse-server'), ['26.1.2.11']);
  const html = `"three": "https://cdn.jsdelivr.net/npm/three@0.186.1/build/three.module.js",\n"three/addons/": "https://cdn.jsdelivr.net/npm/three@0.186.1/examples/jsm/"\n"x": "https://cdn.jsdelivr.net/npm/threejs-extra@2.0.0/x.js"`;
  assert.deepEqual(importmapVersions(html, 'three'), ['0.186.1']);
});

test('gitlinkCommit reads a submodule entry from git ls-tree', () => {
  const sha = '048d2cd25af949679c56347440b7cf5a9a900063';
  assert.equal(gitlinkCommit(`160000 commit ${sha}\tingest/jetstreamer\n`), sha);
  assert.equal(gitlinkCommit(`100644 blob ${sha}\tREADME.md\n`), undefined, 'a file is not a gitlink');
  assert.equal(gitlinkCommit(''), undefined);
});

test('every declared version resolves in this repository', () => {
  const info = collectBuildInfo(REPO_ROOT, { env: {}, now: new Date('2026-01-01T00:00:00Z') });
  assert.deepEqual(info.versions.map((v) => v.id), VERSIONS.map((v) => v.id));
  for (const v of info.versions) assert.ok(v.version.trim(), `${v.id} has a version`);
  assert.match(info.versions.find((v) => v.id === 'jetstreamer').version, /^[0-9a-f]{40}$/);
});

test('the script output satisfies the page contract', () => {
  const env = { GITHUB_SERVER_URL: 'https://github.com', GITHUB_REPOSITORY: 'solana-rpc/superbank', GITHUB_RUN_ID: '123' };
  const info = collectBuildInfo(REPO_ROOT, { env, now: new Date('2026-01-01T00:00:00Z') });
  const parsed = parseBuildInfo(JSON.parse(JSON.stringify(info)));
  assert.ok(parsed, 'parseBuildInfo accepts what the script writes');
  assert.equal(parsed.runUrl, 'https://github.com/solana-rpc/superbank/actions/runs/123');
  assert.equal(parsed.builtAt, '2026-01-01T00:00:00.000Z');
  assert.equal(collectBuildInfo(REPO_ROOT, { env: {} }).runUrl, null, 'no run link outside Actions');
});

test('an unresolvable version fails loudly instead of publishing a blank row', () => {
  const versions = [...VERSIONS, { id: 'gone', label: 'Renamed crate', file: 'Cargo.lock', read: (t) => cargoLockVersions(t, 'no-such-crate').join(', ') || undefined }];
  assert.throws(() => collectBuildInfo(REPO_ROOT, { env: {}, versions }), /gone: no version found in Cargo\.lock/);
  const missing = [{ id: 'missing', label: 'Missing file', file: 'no/such/file.toml', read: () => '1' }];
  assert.throws(() => collectBuildInfo(REPO_ROOT, { env: {}, versions: missing }), /missing: cannot read no\/such\/file\.toml/);
});
