// Info page wiring: build details and versions from build-info.json (written
// by CI; see scripts/site/build-info.mjs), and the sources each page cites.
//
// build-info.json is untrusted input: parseBuildInfo() validates it and the
// page falls back to a note when it is missing or malformed. All text goes
// through textContent (dom.js); links are built only from validated values.

import { el, rich } from './dom.js';
import { commitUrl, parseBuildInfo, sourceUrl } from './build-info.js';
import { pageSources, upstreamSources } from './info-model.js';

const $ = (id) => document.getElementById(id);

const LOCAL_NOTE =
  'Written by CI when the site is deployed. In a local preview, run `node scripts/site/build-info.mjs site/build-info.json` to fill this in.';

const ext = (href, children, props = {}) => el('a', { href, target: '_blank', rel: 'noopener noreferrer', ...props }, children);
const isSha = (value) => /^[0-9a-f]{40}$/.test(value);
const shortSha = (sha) => sha.slice(0, 12);

async function loadBuildInfo() {
  try {
    const response = await fetch('build-info.json', { cache: 'no-store' });
    return response.ok ? parseBuildInfo(await response.json()) : null;
  } catch {
    return null;
  }
}

function formatTime(iso) {
  const date = new Date(iso);
  return `${date.toISOString().slice(0, 16).replace('T', ' ')} UTC`;
}

function renderBuilt(info) {
  if (!info) return rich('p', LOCAL_NOTE, { class: 'notice' });
  const superbank = info.versions.find((v) => v.id === 'superbank');
  const rows = [
    ['Commit', ext(commitUrl(info), [el('code', { text: shortSha(info.commit), title: info.commit })])],
    ['Built', el('time', { datetime: info.builtAt, text: formatTime(info.builtAt) })],
    ['Workflow run', info.runUrl ? ext(info.runUrl, 'View the run') : el('span', { class: 'info-muted', text: 'local build' })],
    superbank ? ['Superbank', el('code', { text: superbank.version })] : null,
  ];
  return el(
    'dl',
    { class: 'info-dl' },
    rows.filter(Boolean).map(([label, value]) => el('div', {}, [el('dt', { text: label }), el('dd', {}, [value])])),
  );
}

function renderVersions(info) {
  if (!info) return el('p', { class: 'notice', text: 'Versions appear here once CI has built the site.' });
  const body = info.versions.map((v) =>
    el('tr', {}, [
      el('th', { scope: 'row', text: v.label }),
      el('td', {}, [el('code', { text: isSha(v.version) ? shortSha(v.version) : v.version, title: v.version })]),
      el('td', {}, [ext(sourceUrl(info, v.file), [el('code', { text: v.file })])]),
    ]),
  );
  return el('div', { class: 'info-table-wrap' }, [
    el('table', { class: 'info-table' }, [
      el('thead', {}, [el('tr', {}, [el('th', { scope: 'col', text: 'Component' }), el('th', { scope: 'col', text: 'Version' }), el('th', { scope: 'col', text: 'Read from' })])]),
      el('tbody', {}, body),
    ]),
  ]);
}

function renderSources(info) {
  return pageSources().map((group) =>
    el('details', { class: 'info-group' }, [
      el('summary', { class: 'info-group__summary' }, [
        el('span', { class: 'info-group__title', text: group.title }),
        el('span', { class: 'info-group__count', text: `${group.paths.length} files` }),
      ]),
      el('p', { class: 'info-group__page' }, [el('a', { href: group.href, text: `Open the ${group.title.toLowerCase()}` })]),
      el(
        'ul',
        { class: 'info-paths' },
        group.paths.map((path) => el('li', {}, [ext(sourceUrl(info, path), [el('code', { text: path })])])),
      ),
    ]),
  );
}

function renderUpstream() {
  return upstreamSources().map((source) => el('li', {}, [ext(source.url, source.label)]));
}

async function start() {
  // Sources come from the page data and render without build info.
  $('info-sources').replaceChildren(...renderSources(null));
  $('info-upstream').replaceChildren(...renderUpstream());

  const info = await loadBuildInfo();
  $('info-built').replaceChildren(renderBuilt(info));
  $('info-versions').replaceChildren(renderVersions(info));
  // Re-render so source links pin to the built commit.
  if (info) $('info-sources').replaceChildren(...renderSources(info));
}

start();
