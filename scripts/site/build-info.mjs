#!/usr/bin/env node
// Writes site/build-info.json for the Info page: the commit the site was built
// from and the versions of what the pages describe, read from the repository.
// The Pages workflow runs it before uploading the site; locally:
//   node scripts/site/build-info.mjs site/build-info.json
//
// Exits non-zero when a declared version cannot be read, so a renamed crate or
// image fails the deploy instead of publishing a blank row.

import { execFileSync } from 'node:child_process';
import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { DEFAULT_REPOSITORY, SCHEMA, parseBuildInfo } from '../../site/js/build-info.js';

const escape = (text) => text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

// Every `version` of `[[package]] name = "<name>"` in Cargo.lock (a crate can
// appear more than once).
export function cargoLockVersions(lock, name) {
  const versions = [];
  for (const block of String(lock).split(/^\[\[package\]\]$/m)) {
    if (new RegExp(`^name = "${escape(name)}"$`, 'm').test(block)) {
      const version = /^version = "([^"]+)"$/m.exec(block)?.[1];
      if (version && !versions.includes(version)) versions.push(version);
    }
  }
  return versions;
}

// A `key = "value"` from the `[workspace.package]` table of Cargo.toml.
export function workspaceField(toml, key) {
  const table = /^\[workspace\.package\]$([\s\S]*?)(?=^\[)/m.exec(`${toml}\n[`)?.[1] ?? '';
  return new RegExp(`^${escape(key)}\\s*=\\s*"([^"]+)"`, 'm').exec(table)?.[1];
}

// Every tag of `image: <image>:<tag>` in a compose file.
export function composeImageTags(yaml, image) {
  const tags = [];
  for (const [, tag] of String(yaml).matchAll(new RegExp(`^\\s*image:\\s*["']?${escape(image)}:([^\\s"']+)`, 'gm'))) {
    if (!tags.includes(tag)) tags.push(tag);
  }
  return tags;
}

// Every version of an npm package in a page's import map / CDN URLs.
export function importmapVersions(html, pkg) {
  const versions = [];
  for (const [, version] of String(html).matchAll(new RegExp(`/npm/${escape(pkg)}@([0-9][^/"']*)/`, 'g'))) {
    if (!versions.includes(version)) versions.push(version);
  }
  return versions;
}

// The commit a submodule path points at, from the parent repo's tree.
export function gitlinkCommit(lsTree) {
  return /^160000 commit ([0-9a-f]{40})\t/m.exec(String(lsTree))?.[1];
}

const list = (values) => (values.length ? values.join(', ') : undefined);

// What the Info page lists. `read` gets the file's text (or `git ls-tree`
// output for a gitlink) and returns the version string or undefined.
export const VERSIONS = Object.freeze([
  { id: 'superbank', label: 'Superbank', file: 'Cargo.toml', read: (t) => workspaceField(t, 'version') },
  { id: 'rust', label: 'Rust (minimum)', file: 'Cargo.toml', read: (t) => workspaceField(t, 'rust-version') },
  { id: 'clickhouse', label: 'ClickHouse server', file: 'docker-compose.yaml', read: (t) => list(composeImageTags(t, 'clickhouse/clickhouse-server')) },
  { id: 'solana', label: 'Agave / Solana crates', file: 'Cargo.lock', read: (t) => list(cargoLockVersions(t, 'solana-rpc-client')) },
  { id: 'yellowstone-grpc-proto', label: 'Yellowstone gRPC proto', file: 'Cargo.lock', read: (t) => list(cargoLockVersions(t, 'yellowstone-grpc-proto')) },
  { id: 'yellowstone-grpc-client', label: 'Yellowstone gRPC client', file: 'Cargo.lock', read: (t) => list(cargoLockVersions(t, 'yellowstone-grpc-client')) },
  { id: 'fumarole', label: 'Yellowstone Fumarole client', file: 'Cargo.lock', read: (t) => list(cargoLockVersions(t, 'yellowstone-fumarole-client')) },
  { id: 'block-machine', label: 'Yellowstone block machine', file: 'Cargo.lock', read: (t) => list(cargoLockVersions(t, 'yellowstone-block-machine')) },
  { id: 'jetstreamer', label: 'Jetstreamer (submodule commit)', file: 'ingest/jetstreamer', gitlink: true, read: gitlinkCommit },
  { id: 'three', label: 'Three.js', file: 'site/index.html', read: (t) => list(importmapVersions(t, 'three')) },
]);

const git = (root, args) => execFileSync('git', args, { cwd: root, encoding: 'utf8' });

// Builds the build-info object. Throws, listing every problem, when a version
// or the commit cannot be read.
export function collectBuildInfo(root, { env = process.env, now = new Date(), versions = VERSIONS } = {}) {
  const problems = [];
  const resolved = [];
  for (const spec of versions) {
    let version;
    try {
      const text = spec.gitlink ? git(root, ['ls-tree', 'HEAD', spec.file]) : readFileSync(join(root, spec.file), 'utf8');
      version = spec.read(text);
    } catch (err) {
      problems.push(`${spec.id}: cannot read ${spec.file} (${err.message.split('\n')[0]})`);
      continue;
    }
    if (!version) problems.push(`${spec.id}: no version found in ${spec.file}`);
    else resolved.push({ id: spec.id, label: spec.label, version, file: spec.file });
  }
  let commit;
  try {
    commit = git(root, ['rev-parse', 'HEAD']).trim();
  } catch (err) {
    problems.push(`commit: git rev-parse failed (${err.message.split('\n')[0]})`);
  }
  if (problems.length) throw new Error(`build-info: ${problems.join('; ')}`);

  const repository = env.GITHUB_REPOSITORY || DEFAULT_REPOSITORY;
  const runUrl =
    env.GITHUB_SERVER_URL === 'https://github.com' && env.GITHUB_REPOSITORY && /^[0-9]+$/.test(env.GITHUB_RUN_ID ?? '')
      ? `https://github.com/${env.GITHUB_REPOSITORY}/actions/runs/${env.GITHUB_RUN_ID}`
      : null;
  const info = { schema: SCHEMA, repository, commit, builtAt: now.toISOString(), runUrl, versions: resolved };
  // The page drops anything that fails its own check; fail here instead.
  if (!parseBuildInfo(info)) throw new Error(`build-info: output does not satisfy site/js/build-info.js: ${JSON.stringify(info)}`);
  return info;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const out = process.argv[2];
  if (!out) {
    console.error('usage: node scripts/site/build-info.mjs <output.json>');
    process.exit(2);
  }
  const root = fileURLToPath(new URL('../../', import.meta.url));
  try {
    const info = collectBuildInfo(root);
    writeFileSync(out, `${JSON.stringify(info, null, 2)}\n`);
    console.log(`wrote ${out}: ${info.commit.slice(0, 12)}, ${info.versions.map((v) => `${v.id} ${v.version}`).join(', ')}`);
  } catch (err) {
    console.error(err.message);
    process.exit(1);
  }
}
