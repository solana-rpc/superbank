// Page wiring for the Alpenglow lifecycle: selector bar, URL hash, step
// navigation, timeline, summary and info drawer. Modelled on main.js.
// alpenglow-content.js is loaded lazily and the page keeps working (timeline,
// steps, summary) if it fails.
//
// All dynamic text goes through textContent / createElement. The URL hash is
// untrusted input but only ever reaches the DOM after parseHash() has reduced
// it to enumerated values.

import { DEFAULT_STATE, ENUMS, normalizeState, parseHash, serializeHash } from './alpenglow-state.js';
import { LANES, SOURCES, buildLifecycle } from './alpenglow-model.js';
import { createTimeline } from './alpenglow-timeline.js';
import { el, rich } from './dom.js';

const FALLBACK_REPO_BLOB = 'https://github.com/solana-rpc/superbank/blob/main/';
const PLAY_INTERVAL_MS = 2600;

// --- Selector definitions ---------------------------------------------------
const OPTIONS = {
  era: {
    alpenglow: ['Alpenglow', 'Bank-tagged Yellowstone stream with footers; confirmed and finalized arrive together'],
    tower: ['Tower BFT', 'What mainnet runs until Alpenglow activates: bank_id = slot, no footers'],
  },
  min: {
    processed: ['Processed', 'superbank-rpc HEAD_CACHE_MIN_COMMITMENT=processed (default)'],
    confirmed: ['Confirmed', 'superbank-rpc HEAD_CACHE_MIN_COMMITMENT=confirmed'],
    finalized: ['Finalized', 'superbank-rpc HEAD_CACHE_MIN_COMMITMENT=finalized'],
  },
  scenario: {
    clean: ['One bank', 'A single bank for slot N, start to finish'],
    fork: ['Fork', 'Alpenglow: two banks compete for slot N. Tower BFT: slot N lands on an abandoned fork'],
    retry: ['Retried tx', 'A transaction lands on a losing bank or slot and is retried in N+1'],
    reconnect: ['Reconnect', 'The head-cache stream drops right after slot N is published'],
  },
  method: {
    getTransaction: ['getTransaction', 'Probe a transaction T from slot N'],
    getBlock: ['getBlock', 'Probe the block for slot N, with footer: true'],
  },
  read: {
    processed: ['processed', 'Probe at processed commitment'],
    confirmed: ['confirmed', 'Probe at confirmed commitment'],
    finalized: ['finalized', 'Probe at finalized commitment'],
  },
};

// Explicit order; ENUMS iteration order is not the reading order we want.
const GROUPS = [
  { label: 'Consensus', enum: 'era' },
  { label: 'Head minimum', enum: 'min' },
  { label: 'Scenario', enum: 'scenario' },
  { label: 'Probe', enum: 'method' },
  { label: 'At', enum: 'read' },
];

// --- DOM helpers ------------------------------------------------------------
const $ = (id) => document.getElementById(id);

const labelFor = (key, value) => OPTIONS[key]?.[value]?.[0] ?? String(value);
const reducedMotion = () => matchMedia('(prefers-reduced-motion: reduce)').matches;

// --- State ------------------------------------------------------------------
let state = parseHash(location.hash);
let life = buildLifecycle(state);
let stepIndex = 0;
// null (drawer closed), { kind: 'step' } (follows the current step) or { kind: 'lane', id }.
let selection = null;
let playTimer = null;
let timeline = null;

let contentMod = null;
const contentReady = import('./alpenglow-content.js')
  .then((mod) => {
    contentMod = mod;
  })
  .catch((err) => console.warn('alpenglow-content.js unavailable; the drawer will show step summaries only', err));

const frame = () => life.steps[stepIndex];

// --- Controls ---------------------------------------------------------------
function buildControls() {
  const host = $('controls-groups');
  for (const group of GROUPS) {
    const labelId = `ctl-label-${group.enum}`;
    const body = el('div', { class: 'segmented', role: 'radiogroup', 'aria-labelledby': labelId });
    for (const value of ENUMS[group.enum]) {
      const [text, hint] = OPTIONS[group.enum][value] ?? [value, ''];
      const id = `ctl-${group.enum}-${value}`;
      body.append(
        el('input', { type: 'radio', class: 'ctl-input', id, name: group.enum, value, title: hint }),
        el('label', { class: 'ctl-option', for: id, text, title: hint }),
      );
    }
    host.append(el('div', { class: 'ctl-group' }, [el('span', { class: 'ctl-group__label', id: labelId, text: group.label }), body]));
  }
  host.append($('reset-config'));

  host.addEventListener('change', (event) => {
    const input = event.target;
    if (!(input instanceof HTMLInputElement)) return;
    setState({ ...state, [input.name]: input.value });
  });

  $('reset-config').addEventListener('click', () => {
    setState({ ...DEFAULT_STATE });
    syncControls();
  });
}

function syncControls() {
  for (const input of document.querySelectorAll('#controls .ctl-input')) input.checked = state[input.name] === input.value;
}

function describeConfig(s) {
  return [
    labelFor('era', s.era),
    `Head minimum: ${labelFor('min', s.min)}`,
    labelFor('scenario', s.scenario),
    `${labelFor('method', s.method)} @ ${labelFor('read', s.read)}`,
  ].join(' · ');
}

// The selector bar is a collapsible "Configure" disclosure on narrow screens
// and an always-open bar on wide ones.
function setupDisclosure() {
  const details = $('controls-details');
  const wide = matchMedia('(min-width: 900px)');
  details.open = wide.matches;
  wide.addEventListener('change', () => {
    if (wide.matches) details.open = true;
  });
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && !wide.matches && details.open && details.contains(document.activeElement)) {
      details.open = false;
      details.querySelector('summary')?.focus();
      event.preventDefault();
    }
  });
}

// A new configuration keeps the current step if it still exists, otherwise
// the same position (clamped).
function setState(next) {
  const id = frame()?.id;
  state = normalizeState(next);
  life = buildLifecycle(state);
  const same = life.steps.findIndex((s) => s.id === id);
  stepIndex = same >= 0 ? same : Math.min(stepIndex, life.steps.length - 1);
  render();
}

// --- Summary ----------------------------------------------------------------
function renderSummary() {
  $('summary-config').textContent = describeConfig(state);
  $('controls-state').textContent = describeConfig(state);
  $('summary-body').replaceChildren(
    ...life.summary.map((step, i) => {
      const button = el('button', { type: 'button', class: 'ag-steps__jump' }, [
        el('span', { class: 'ag-steps__title', text: step.title }),
        rich('span', step.text, { class: 'ag-steps__text' }),
      ]);
      button.addEventListener('click', () => {
        goToStep(i);
        $('stage').scrollIntoView({ block: 'start', behavior: reducedMotion() ? 'auto' : 'smooth' });
      });
      return el('li', { class: 'ag-steps__item', 'aria-current': i === stepIndex ? 'step' : null }, [button]);
    }),
  );
}

// --- Info panel -------------------------------------------------------------
function setDrawer(open) {
  $('stage').classList.toggle('is-drawer-open', open);
  $('walkthrough').setAttribute('aria-pressed', String(selection?.kind === 'step'));
}

function panelEmpty() {
  const panel = $('panel');
  panel.replaceChildren();
  panel.classList.remove('has-content');
  setDrawer(false);
}

function sourceLinks(ids) {
  return el(
    'ul',
    { class: 'panel__refs ag-sources' },
    ids
      .filter((id) => SOURCES[id])
      .map((id) => el('li', {}, [el('a', { href: SOURCES[id].url, target: '_blank', rel: 'noopener noreferrer', text: SOURCES[id].label })])),
  );
}

function renderPanel() {
  const panel = $('panel');
  if (!selection) return panelEmpty();

  let title;
  let subtitle;
  let lead = null;
  let content = null;
  let note = null;
  try {
    if (selection.kind === 'step') {
      const f = frame();
      title = f.title;
      subtitle = `Step ${stepIndex + 1} of ${life.steps.length}`;
      lead = f.summary;
      note = f.note;
      content = contentMod?.stepContent?.(f.id, state) ?? null;
    } else {
      content = contentMod?.laneContent?.(selection.id, state) ?? null;
      title = content?.title ?? LANES.find((l) => l.id === selection.id)?.label ?? selection.id;
      subtitle = content?.subtitle;
    }
  } catch (err) {
    console.warn('drawer content failed for', selection, err);
  }

  const scrollTop = panel.scrollTop;
  const close = el('button', { type: 'button', class: 'panel__close', 'aria-label': 'Close details', text: '×' });
  close.addEventListener('click', () => {
    const lane = selection?.kind === 'lane' ? selection.id : null;
    clearSelection();
    (lane ? document.querySelector(`.ag-lane__label[data-lane="${CSS.escape(lane)}"]`) : $('walkthrough'))?.focus({ preventScroll: true });
  });
  const children = [el('div', { class: 'panel__head' }, [el('h2', { class: 'panel__title', id: 'panel-title', tabindex: '-1', text: title }), close])];
  if (subtitle) children.push(rich('p', subtitle, { class: 'panel__subtitle' }));
  if (lead) children.push(rich('p', lead, { class: 'ag-lead' }));

  if (content) {
    for (const paragraph of content.body ?? []) children.push(rich('p', paragraph));
    if (note) children.push(rich('p', note, { class: 'ag-note' }));
    if (content.config?.length) {
      const rows = content.config.map((row) =>
        el('tr', {}, [
          el('th', { scope: 'row' }, [el('code', { text: row.key })]),
          rich('td', row.value, { class: 'config__value' }),
          row.note ? rich('td', row.note, { class: 'config__note' }) : null,
        ]),
      );
      children.push(
        el('h3', { class: 'panel__h', text: 'Key config' }),
        el('table', { class: 'config' }, [
          el('thead', { class: 'sr-only' }, [el('tr', {}, ['Key', 'Value', 'Note'].map((text) => el('th', { scope: 'col', text })))]),
          el('tbody', {}, rows),
        ]),
      );
    }
    if (content.refs?.length) {
      const base = contentMod?.REPO_BLOB ?? FALLBACK_REPO_BLOB;
      children.push(
        el('h3', { class: 'panel__h', text: 'Source' }),
        el(
          'ul',
          { class: 'panel__refs' },
          content.refs.map((path) =>
            el('li', {}, [el('a', { href: base + path, target: '_blank', rel: 'noopener noreferrer' }, [el('code', { text: path })])]),
          ),
        ),
      );
    }
    if (content.sources?.length) children.push(el('h3', { class: 'panel__h', text: 'Upstream sources' }), sourceLinks(content.sources));
  } else if (note) {
    children.push(rich('p', note, { class: 'ag-note' }));
  }

  panel.replaceChildren(...children);
  panel.classList.add('has-content');
  panel.scrollTop = scrollTop;
  setDrawer(true);
}

function select(next) {
  selection = next;
  timeline?.select(selection?.kind === 'lane' ? selection.id : null);
  renderPanel();
  if (selection && !matchMedia('(min-width: 900px)').matches) {
    $('panel').scrollIntoView({ block: 'nearest', behavior: reducedMotion() ? 'auto' : 'smooth' });
  }
  if (selection && !contentMod) contentReady.then(() => selection && renderPanel());
}

function clearSelection() {
  select(null);
}

// --- Steps ------------------------------------------------------------------
function goToStep(index, { announce = true } = {}) {
  stepIndex = Math.min(Math.max(index, 0), life.steps.length - 1);
  render({ announce });
}

function step(delta) {
  stopPlay();
  goToStep(stepIndex + delta);
}

function renderWalkBar() {
  const last = stepIndex === life.steps.length - 1;
  $('walk-status').textContent = `${stepIndex + 1} / ${life.steps.length} · ${frame().title}`;
  const prev = $('walk-prev');
  const next = $('walk-next');
  // A disabled button drops focus; keep it in the bar.
  if (stepIndex === 0 && document.activeElement === prev) next.focus();
  if (last && document.activeElement === next) prev.focus();
  prev.disabled = stepIndex === 0;
  next.disabled = last;
}

function stopPlay() {
  if (!playTimer) return;
  clearInterval(playTimer);
  playTimer = null;
  $('play').setAttribute('aria-pressed', 'false');
  $('play').textContent = 'Play';
}

function startPlay() {
  if (stepIndex === life.steps.length - 1) goToStep(0);
  $('play').setAttribute('aria-pressed', 'true');
  $('play').textContent = 'Pause';
  playTimer = setInterval(() => {
    if (stepIndex >= life.steps.length - 1) return stopPlay();
    goToStep(stepIndex + 1, { announce: false });
  }, PLAY_INTERVAL_MS);
}

function setupSteps() {
  $('walk-prev').addEventListener('click', () => step(-1));
  $('walk-next').addEventListener('click', () => step(1));
  $('restart').addEventListener('click', () => {
    stopPlay();
    goToStep(0);
  });
  $('play').addEventListener('click', () => (playTimer ? stopPlay() : startPlay()));
  $('walkthrough').addEventListener('click', () => select(selection?.kind === 'step' ? null : { kind: 'step' }));
  // Arrow keys step, unless a control that uses them (the selector radios) has focus.
  document.addEventListener('keydown', (event) => {
    if (event.defaultPrevented || event.altKey || event.ctrlKey || event.metaKey || event.shiftKey) return;
    if (event.key !== 'ArrowLeft' && event.key !== 'ArrowRight') return;
    const target = event.target;
    if (target instanceof HTMLElement && (target.isContentEditable || target.closest('input, select, textarea'))) return;
    event.preventDefault();
    step(event.key === 'ArrowRight' ? 1 : -1);
  });
}

// --- Render -----------------------------------------------------------------
function render({ announce = false } = {}) {
  const f = frame();
  timeline.update(f, state);
  renderWalkBar();
  renderSummary();
  if (selection) renderPanel();
  if (announce) $('panel-status').textContent = `Step ${stepIndex + 1} of ${life.steps.length}: ${f.title}`;
  const hash = serializeHash(state);
  const url = hash ? `#${hash}` : location.pathname + location.search;
  if (location.hash.replace(/^#/, '') !== hash) history.replaceState(null, '', url);
}

// --- Boot -------------------------------------------------------------------
timeline = createTimeline($('timeline'), {
  onLane: (id) => select(selection?.kind === 'lane' && selection.id === id ? null : { kind: 'lane', id }),
});
buildControls();
setupDisclosure();
setupSteps();
syncControls();
panelEmpty();
render();

window.addEventListener('hashchange', () => {
  setState(parseHash(location.hash));
  syncControls();
});

document.addEventListener('keydown', (event) => {
  if (event.key === 'Escape' && selection && !event.defaultPrevented) clearSelection();
});

$('skip-summary').addEventListener('click', () => {
  const target = $('summary');
  target.focus({ preventScroll: true });
  target.scrollIntoView({ block: 'start', behavior: reducedMotion() ? 'auto' : 'smooth' });
});
