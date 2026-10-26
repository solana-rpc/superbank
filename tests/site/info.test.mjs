// Checks of the Info page model, the build-info.json contract it reads, and
// the site nav shared by every page. Run from the repo root:
//   node --test tests/site/info.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { pageSources, upstreamSources } from '../../site/js/info-model.js';
import { commitUrl, isRepoPath, parseBuildInfo, sourceUrl } from '../../site/js/build-info.js';

const REPO_ROOT = fileURLToPath(new URL('../../', import.meta.url));
const SHA = '9d9526572fc99b0909dc68762566585f3c2d5f1c';

const valid = () => ({
  schema: 1,
  repository: 'solana-rpc/superbank',
  commit: SHA,
  builtAt: '2026-10-08T04:13:40.773Z',
  runUrl: 'https://github.com/solana-rpc/superbank/actions/runs/42',
  versions: [
    { id: 'superbank', label: 'Superbank', version: '0.6.0', file: 'Cargo.toml' },
    { id: 'jetstreamer', label: 'Jetstreamer (submodule commit)', version: SHA, file: 'ingest/jetstreamer' },
  ],
});

test('pageSources lists every page with existing, repo-relative files', () => {
  const groups = pageSources();
  assert.deepEqual(groups.map((g) => g.id), ['architecture', 'configuration', 'alpenglow']);
  for (const g of groups) {
    assert.ok(g.paths.length > 0, `${g.id} has sources`);
    assert.deepEqual(g.paths, [...new Set(g.paths)].sort((a, b) => a.localeCompare(b)), `${g.id} is sorted and unique`);
    for (const path of g.paths) {
      assert.ok(isRepoPath(path), `${g.id}: not repo-relative: ${path}`);
      assert.ok(existsSync(join(REPO_ROOT, path)), `${g.id}: missing ${path}`);
    }
  }
  assert.ok(groups[1].paths.includes('crates/superbank-rpc/src/config.rs'), 'config sources come from the config data');
});

test('upstream citations are pinned https links', () => {
  const upstream = upstreamSources();
  assert.ok(upstream.length > 0);
  for (const u of upstream) {
    assert.match(u.url, /^https:\/\//, u.id);
    assert.ok(!/\/blob\/(main|master)\//.test(u.url), `${u.id} is pinned, not a moving branch`);
    assert.ok(u.label.trim() && !u.label.includes('<'), u.id);
  }
});

test('parseBuildInfo accepts what the build writes', () => {
  const info = parseBuildInfo(valid());
  assert.ok(info);
  assert.equal(info.versions.length, 2);
  assert.ok(Object.isFrozen(info) && Object.isFrozen(info.versions[0]));
  assert.ok(parseBuildInfo({ ...valid(), runUrl: null }), 'a local build has no run link');
  assert.ok(parseBuildInfo({ ...valid(), extra: 'ignored' }), 'unknown keys are ignored');
  assert.equal(parseBuildInfo({ ...valid(), extra: 'ignored' }).extra, undefined);
});

test('parseBuildInfo rejects anything that could become a bad link or markup', () => {
  const bad = [
    null,
    [],
    'string',
    { ...valid(), schema: 2 },
    { ...valid(), commit: 'main' },
    { ...valid(), commit: SHA.toUpperCase() },
    { ...valid(), commit: `${SHA}0` },
    { ...valid(), repository: 'solana-rpc/superbank/../evil' },
    { ...valid(), repository: 'javascript:alert(1)//x' },
    { ...valid(), runUrl: 'javascript:alert(1)' },
    { ...valid(), runUrl: 'https://github.com.evil.example/a/b/actions/runs/1' },
    { ...valid(), runUrl: 'https://github.com/a/b/actions/runs/1?x=<y>' },
    { ...valid(), builtAt: 'yesterday' },
    { ...valid(), versions: [] },
    { ...valid(), versions: 'Cargo.toml' },
    { ...valid(), versions: [{ id: 'x', label: '<img src=x>', version: '1', file: 'Cargo.toml' }] },
    { ...valid(), versions: [{ id: 'x', label: 'X', version: '1', file: '../etc/passwd' }] },
    { ...valid(), versions: [{ id: 'x', label: 'X', version: '1', file: '/etc/passwd' }] },
    { ...valid(), versions: [{ id: 'x', label: 'X', version: '1', file: 'https://evil.example/' }] },
    { ...valid(), versions: [{ id: 'X Y', label: 'X', version: '1', file: 'Cargo.toml' }] },
    { ...valid(), versions: [{ id: 'x', label: 'X', version: '', file: 'Cargo.toml' }] },
    { ...valid(), versions: [{ id: 'x', label: 'X', version: '1', file: 'a' }, { id: 'x', label: 'Y', version: '2', file: 'b' }] },
  ];
  for (const input of bad) assert.equal(parseBuildInfo(input), null, JSON.stringify(input));
});

test('links pin to the built commit and fall back to main', () => {
  const info = parseBuildInfo(valid());
  assert.equal(sourceUrl(info, 'Cargo.toml'), `https://github.com/solana-rpc/superbank/blob/${SHA}/Cargo.toml`);
  assert.equal(sourceUrl(null, 'Cargo.toml'), 'https://github.com/solana-rpc/superbank/blob/main/Cargo.toml');
  assert.equal(commitUrl(info), `https://github.com/solana-rpc/superbank/commit/${SHA}`);
});

// --- Site nav -----------------------------------------------------------------

const PAGES = ['index.html', 'config.html', 'alpenglow.html', 'info.html'];
const NAV_HREFS = ['./', 'config.html', 'alpenglow.html', 'info.html'];

function navOf(file) {
  const html = readFileSync(join(REPO_ROOT, 'site', file), 'utf8');
  const nav = /<nav class="site-nav"[^>]*>([\s\S]*?)<\/nav>/.exec(html)?.[1];
  assert.ok(nav, `${file}: no .site-nav`);
  return [...nav.matchAll(/<a\b([^>]*)>/g)].map(([, attrs]) => ({
    href: /\bhref="([^"]*)"/.exec(attrs)?.[1],
    current: /\baria-current="page"/.test(attrs),
  }));
}

test('every page has the same nav, with itself marked current', () => {
  for (const file of PAGES) {
    const links = navOf(file);
    assert.deepEqual(links.map((l) => l.href), NAV_HREFS, `${file}: nav links`);
    const current = links.filter((l) => l.current);
    assert.equal(current.length, 1, `${file}: exactly one aria-current`);
    assert.equal(current[0].href, file === 'index.html' ? './' : file, `${file}: aria-current points at itself`);
  }
});
