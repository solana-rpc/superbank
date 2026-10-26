// The build-info.json contract, shared by scripts/site/build-info.mjs (which
// writes the file in the Pages build) and info-page.js (which reads it). Pure;
// no DOM and no data imports.
//
// The file is produced by our own CI, but the page still treats it as
// untrusted: parseBuildInfo() returns null for any unexpected shape, and only
// a validated repository, 40-hex commit and https://github.com/ run URL are
// ever turned into links.

export const SCHEMA = 1;
export const DEFAULT_REPOSITORY = 'solana-rpc/superbank';

const REPOSITORY = /^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/;
const COMMIT = /^[0-9a-f]{40}$/;
const RUN_URL = /^https:\/\/github\.com\/[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+\/actions\/runs\/[0-9]+$/;
const VERSION_ID = /^[a-z0-9-]+$/;
const REPO_PATH = /^[A-Za-z0-9_][A-Za-z0-9_./-]*$/;

const isText = (value, max) => typeof value === 'string' && value.trim().length > 0 && value.length <= max && !value.includes('<');
export const isRepoPath = (value) => typeof value === 'string' && REPO_PATH.test(value) && !value.split('/').includes('..');

// Returns a normalized, frozen copy, or null when anything is off.
export function parseBuildInfo(input) {
  if (!input || typeof input !== 'object' || Array.isArray(input)) return null;
  const { schema, repository, commit, builtAt, runUrl, versions } = input;
  if (schema !== SCHEMA) return null;
  if (typeof repository !== 'string' || !REPOSITORY.test(repository)) return null;
  if (typeof commit !== 'string' || !COMMIT.test(commit)) return null;
  if (typeof builtAt !== 'string' || builtAt.length > 40 || Number.isNaN(Date.parse(builtAt))) return null;
  if (runUrl !== null && (typeof runUrl !== 'string' || !RUN_URL.test(runUrl))) return null;
  if (!Array.isArray(versions) || versions.length === 0 || versions.length > 50) return null;
  const parsed = [];
  for (const v of versions) {
    if (!v || typeof v !== 'object') return null;
    if (typeof v.id !== 'string' || !VERSION_ID.test(v.id)) return null;
    if (!isText(v.label, 80) || !isText(v.version, 200) || !isRepoPath(v.file)) return null;
    parsed.push(Object.freeze({ id: v.id, label: v.label, version: v.version, file: v.file }));
  }
  if (new Set(parsed.map((v) => v.id)).size !== parsed.length) return null;
  return Object.freeze({ schema, repository, commit, builtAt, runUrl, versions: Object.freeze(parsed) });
}

// GitHub links pinned to the built commit; `main` when there is no build info.
export function sourceUrl(info, path) {
  return `https://github.com/${info?.repository ?? DEFAULT_REPOSITORY}/blob/${info?.commit ?? 'main'}/${path}`;
}

export function commitUrl(info) {
  return `https://github.com/${info.repository}/commit/${info.commit}`;
}

export function repoUrl(info, suffix = '') {
  return `https://github.com/${info?.repository ?? DEFAULT_REPOSITORY}${suffix}`;
}
