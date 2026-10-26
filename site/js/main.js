// Page wiring: selector bar, URL hash, data-path summary and info panel.
// None of this depends on Three.js. scene.js and content.js are loaded lazily
// and the page keeps working (selectors, summary, panel) if either fails.
//
// All dynamic text goes through textContent / createElement. The URL hash is
// untrusted input but only ever reaches the DOM after parseHash() has reduced
// it to enumerated values.

import { DEFAULT_STATE, ENUMS, FLAGS, normalizeState, parseHash, serializeHash } from './state.js';
import { buildTopology, walkthroughSteps } from './topology.js';
import { appendRich, el, rich } from './dom.js';

const FALLBACK_REPO_BLOB = 'https://github.com/solana-rpc/superbank/blob/main/';
const FALLBACK_NOTICE =
  'The 3D view needs WebGL and could not start here. The data path below still updates as you change the selectors.';

// --- Selector definitions ---------------------------------------------------
// Human labels and hints. Hints cite the flag or env var that turns the
// component on so the bar doubles as a quick reference.
const OPTIONS = {
  source: {
    grpc: ['Yellowstone gRPC', 'superbank --source grpc: live blocks over DragonsMouth'],
    fumarole: ['Fumarole', 'superbank --source fumarole: live blocks with a durable consumer-group cursor'],
    rpc: ['JSON-RPC backfill', 'superbank --source rpc: bounded getBlocks / getBlock backfill'],
    bigtable: ['Bigtable backfill', 'superbank --source bigtable: bounded backfill over a slot or epoch range'],
    solparq: ['Parquet restore', 'superbank --source solparq: restore archived Parquet bundles into ClickHouse'],
    jetstreamer: ['Jetstreamer (Old Faithful)', 'jetstreamer-clickhouse plugin (separate binary): replays Old Faithful epochs'],
  },
  ch: {
    single: ['Single node', 'ddl/local: one ClickHouse node'],
    cluster: ['Cluster', 'ddl/cluster: 3 shards behind Distributed tables'],
    replicated: ['Replicated', 'ddl/replicated: 3 shards x 2 replicas, coordinated by ClickHouse Keeper'],
  },
  archive: {
    off: ['Off', 'No Parquet archiving'],
    local: ['Local disk', 'superbank-solparq streams Parquet from ClickHouse to local disk'],
    s3: ['S3', 'ClickHouse writes Parquet straight to S3 (INSERT INTO FUNCTION s3)'],
  },
  flow: {
    blocks: ['Blocks', 'Animate the write path'],
    requests: ['Requests', 'Animate JSON-RPC request journeys through the cache tiers'],
    both: ['Both', 'Animate the write path and request journeys together'],
  },
};

const FLAG_OPTIONS = {
  head: ['Head cache', 'superbank-rpc --features grpc-head-cache + HEAD_CACHE_ENABLED=true'],
  disk: ['Disk cache', 'superbank-rpc --features disk-cache + DISK_CACHE_ENABLED=true'],
  stream: ['gRPC streaming', 'superbank-rpc --features grpc-streaming + SUPERBANK_GRPC_ENABLED=true'],
  verify: ['superbank-verify', 'superbank-verify re-checks Proof of History from ClickHouse'],
};

// Explicit order; ENUMS/FLAGS iteration order is not the reading order we want.
const GROUPS = [
  { label: 'Source', enum: 'source' },
  { label: 'ClickHouse', enum: 'ch' },
  { label: 'Features', flags: ['head', 'disk', 'stream'] },
  { label: 'Archive', enum: 'archive' },
  { label: 'Tools', flags: ['verify'] },
  { label: 'Animate', enum: 'flow' },
];

// --- DOM helpers ------------------------------------------------------------
const $ = (id) => document.getElementById(id);

const labelFor = (key, value) => OPTIONS[key]?.[value]?.[0] ?? String(value);

// --- State ------------------------------------------------------------------
let state = parseHash(location.hash);
let topology = buildTopology(state);
let scene = null;
let selectedId = null;
let syncingScene = false;
// { index } while the walkthrough runs. The current step is the selection.
let walk = null;
// Read-path order for the walkthrough ('data' or 'request'); not in the hash.
let readOrder = 'data';

// content.js is imported dynamically so a missing or broken module cannot stop
// the page from booting. The promise is cached; panel rendering degrades to the
// topology label while it is pending or if it failed.
let contentMod = null;
const contentReady = import('./content.js')
  .then((mod) => {
    contentMod = mod;
  })
  .catch((err) => console.warn('content.js unavailable; info panel will show labels only', err));

// --- Controls ---------------------------------------------------------------
function buildControls() {
  const host = $('controls-groups');
  const grouped = new Set(GROUPS.flatMap((g) => g.flags ?? []));
  const groups = [...GROUPS];
  const leftover = FLAGS.filter((flag) => !grouped.has(flag));
  if (leftover.length) groups.push({ label: 'Options', flags: leftover });

  for (const group of groups) {
    const labelId = `ctl-label-${group.label.toLowerCase()}`;
    const body = el('div', { class: group.enum ? 'segmented' : 'toggles', role: group.enum ? 'radiogroup' : 'group', 'aria-labelledby': labelId });
    if (group.enum) {
      for (const value of ENUMS[group.enum]) {
        const [text, hint] = OPTIONS[group.enum][value] ?? [value, ''];
        const id = `ctl-${group.enum}-${value}`;
        body.append(
          el('input', { type: 'radio', class: 'ctl-input', id, name: group.enum, value, title: hint }),
          el('label', { class: 'ctl-option', for: id, text, title: hint }),
        );
      }
    } else {
      for (const flag of group.flags) {
        const [text, hint] = FLAG_OPTIONS[flag] ?? [flag, ''];
        const id = `ctl-${flag}`;
        body.append(
          el('span', { class: 'toggle' }, [
            el('input', { type: 'checkbox', role: 'switch', class: 'ctl-input', id, name: flag, title: hint }),
            el('label', { class: `ctl-toggle ctl-toggle--${flag}`, for: id, title: hint }, [el('span', { class: 'ctl-toggle__sq', 'aria-hidden': 'true' }), el('span', { text })]),
          ]),
        );
      }
    }
    host.append(el('div', { class: 'ctl-group' }, [el('span', { class: 'ctl-group__label', id: labelId, text: group.label }), body]));
  }

  // Reset sits after the last group so it shares the final row on wide screens.
  host.append($('reset-config'));

  host.addEventListener('change', (event) => {
    const input = event.target;
    if (!(input instanceof HTMLInputElement)) return;
    const next = { ...state };
    next[input.name] = input.type === 'checkbox' ? input.checked : input.value;
    state = normalizeState(next);
    render();
  });

  $('reset-config').addEventListener('click', () => {
    state = { ...DEFAULT_STATE };
    syncControls();
    render();
  });
}

function syncControls() {
  for (const input of document.querySelectorAll('#controls .ctl-input')) {
    if (input.type === 'checkbox') input.checked = state[input.name] === true;
    else input.checked = state[input.name] === input.value;
  }
}

// One-line description of the current selection, shown in the collapsed
// mobile bar and above the summary.
function describeConfig(s) {
  const parts = [labelFor('source', s.source), labelFor('ch', s.ch)];
  const on = FLAGS.filter((flag) => s[flag]).map((flag) => FLAG_OPTIONS[flag]?.[0] ?? flag);
  if (on.length) parts.push(on.join(' + '));
  parts.push(`Archive: ${labelFor('archive', s.archive)}`);
  return parts.join(' · ');
}

// The selector bar is a collapsible "Configure" disclosure on narrow screens
// and an always-open bar on wide ones.
function setupDisclosure() {
  const details = $('controls-details');
  const wide = matchMedia('(min-width: 900px)');
  // Open by default in the HTML so it still works if this script never runs;
  // collapsed on first paint for narrow screens, forced open when widened.
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

// --- Summary ----------------------------------------------------------------
function renderSummary() {
  $('summary-config').textContent = describeConfig(state);
  $('controls-state').textContent = describeConfig(state);
  const grid = $('summary-body');
  grid.replaceChildren(
    ...topology.summary.map((section, i) => {
      const headingId = `summary-${section.id}`;
      return el('section', { class: 'cell summary__section', 'aria-labelledby': headingId }, [
        el('span', { class: 'cell__index', 'aria-hidden': 'true', text: String(i + 1).padStart(2, '0') }),
        el('h3', { id: headingId, class: 'cell__title', text: section.title }),
        el('ol', {}, section.steps.map((step) => rich('li', step))),
      ]);
    }),
  );
}

// --- Info panel -------------------------------------------------------------
function panelEmpty() {
  const panel = $('panel');
  panel.replaceChildren();
  panel.classList.remove('has-content');
  setDrawer(null);
}

// Open/closed state and side live on #stage so the CSS can move the overlay
// buttons. The drawer opens on the side away from the selected node, so the
// highlighted node stays visible at any width (u runs about -17 .. 17.6).
// During the walkthrough it stays on the right and the scene frames each step
// in the space left of it, so the drawer does not jump from side to side.
function setDrawer(node) {
  const stage = $('stage');
  stage.classList.toggle('is-drawer-open', Boolean(node));
  if (node) stage.dataset.side = !walk && (node.pos?.[0] ?? 0) > 0 ? 'left' : 'right';
  else delete stage.dataset.side;
  const status = node ? walkStatus(node) ?? `Details for ${node.label}` : '';
  if ($('panel-status').textContent !== status) $('panel-status').textContent = status;
}

function renderPanel() {
  const panel = $('panel');
  const node = topology.nodes.find((n) => n.id === selectedId);
  if (!node) return panelEmpty();

  let content = null;
  try {
    content = contentMod?.contentFor?.(node.id, state) ?? null;
  } catch (err) {
    console.warn('contentFor failed for', node.id, err);
  }

  const scrollTop = panel.scrollTop;
  const title = el('h2', { class: 'panel__title', id: 'panel-title', tabindex: '-1', text: content?.title ?? node.label });
  const close = el('button', { type: 'button', class: 'panel__close', 'aria-label': 'Close details', text: '×' });
  close.addEventListener('click', () => {
    const id = selectedId;
    clearSelection();
    // Return focus to the node's label; the hidden drawer cannot hold it.
    document.querySelector(`.node-label[data-node-id="${CSS.escape(id)}"]`)?.focus({ preventScroll: true });
  });
  const children = [el('div', { class: 'panel__head' }, [title, close])];

  const subtitle = content?.subtitle ?? node.sublabel;
  if (subtitle) children.push(rich('p', subtitle, { class: 'panel__subtitle' }));

  if (!content) {
    children.push(el('p', { text: 'Detailed notes for this component are not available right now.' }));
  } else {
    for (const paragraph of content.body ?? []) children.push(rich('p', paragraph));

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

    if (content.action) {
      const action = el('button', { type: 'button', class: 'btn btn--primary panel__action', text: content.action.label });
      action.addEventListener('click', () => {
        state = normalizeState({ ...state, ...content.action.patch });
        syncControls();
        render();
        $('panel-title')?.focus({ preventScroll: true });
      });
      children.push(action);
    }
  }

  panel.replaceChildren(...children);
  panel.classList.add('has-content');
  panel.scrollTop = scrollTop;
  setDrawer(node);
}

function selectNode(id) {
  if (!id || !topology.nodes.some((n) => n.id === id)) return clearSelection();
  if (walk) {
    const index = walkSteps().findIndex((step) => step.id === id);
    if (index >= 0) return goToStep(index);
    endWalk();
  }
  const changed = id !== selectedId;
  selectedId = id;
  renderPanel();
  syncSceneSelection(id);
  // The panel sits below the canvas on narrow screens; make sure the click
  // visibly did something.
  if (changed && !matchMedia('(min-width: 900px)').matches) {
    $('panel').scrollIntoView({ block: 'nearest', behavior: reducedMotion() ? 'auto' : 'smooth' });
  }
  // content.js may still be loading; repaint once it settles, unless the user
  // has moved on.
  if (!contentMod) contentReady.then(() => selectedId === id && renderPanel());
}

function clearSelection() {
  endWalk();
  selectedId = null;
  panelEmpty();
  syncSceneSelection(null);
}

// scene.select() must not echo back into onSelect; the flag breaks that loop
// if an implementation reports programmatic selection.
function syncSceneSelection(id) {
  if (!scene) return;
  syncingScene = true;
  try {
    scene.select(id);
  } finally {
    syncingScene = false;
  }
}

// --- Walkthrough ------------------------------------------------------------
// Steps through every component drawn for this configuration in flow order
// (walkthroughSteps in topology.js), opening its details and framing it in the
// scene. Selecting another node jumps to its step; clearing the selection
// (close, Escape, a click on empty canvas) ends the walkthrough.
const READ_ORDER_BUTTONS = { data: 'walk-order-data', request: 'walk-order-request' };

const walkSteps = () => walkthroughSteps(topology, { readOrder });

function startWalk() {
  walk = { index: 0 };
  $('walk-bar').hidden = false;
  $('walkthrough').setAttribute('aria-pressed', 'true');
  goToStep(0);
  // On wide screens the bar sits over the bottom of the canvas; make sure it is
  // on screen. On phones it is pinned to the bottom of the screen already.
  if (matchMedia('(min-width: 900px)').matches) {
    $('walk-bar').scrollIntoView({ block: 'nearest', behavior: reducedMotion() ? 'auto' : 'smooth' });
  }
}

function goToStep(index) {
  const steps = walkSteps();
  if (!walk || !steps.length) return;
  walk.index = Math.min(Math.max(index, 0), steps.length - 1);
  const { id } = steps[walk.index];
  selectedId = id;
  renderWalkBar(steps);
  // Render the drawer before focusing so the scene measures it open.
  renderPanel();
  syncSceneSelection(id);
  scene?.focus(id);
  if (!contentMod) contentReady.then(() => walk && selectedId === id && renderPanel());
}

// Past either end does nothing (Finish is a deliberate click).
function stepWalk(delta) {
  if (!walk) return;
  const index = walk.index + delta;
  if (index >= 0 && index < walkSteps().length) goToStep(index);
}

// Tears down the walkthrough UI and zooms back out. The selection is left to
// the caller (clearSelection calls this first).
function endWalk() {
  if (!walk) return;
  walk = null;
  // Hiding the bar would drop focus on the page; hand it to the toggle first.
  if ($('walk-bar').contains(document.activeElement)) $('walkthrough').focus({ preventScroll: true });
  $('walk-bar').hidden = true;
  $('walkthrough').setAttribute('aria-pressed', 'false');
  scene?.focus(null);
}

function renderWalkBar(steps) {
  const { index } = walk;
  const last = index === steps.length - 1;
  const section = topology.summary.find((s) => s.id === steps[index].section)?.title ?? '';
  $('walk-status').textContent = `${section} · ${index + 1} / ${steps.length}`;
  const prev = $('walk-prev');
  const next = $('walk-next');
  // A disabled button drops focus; keep it in the bar.
  if (index === 0 && document.activeElement === prev) next.focus();
  prev.disabled = index === 0;
  next.replaceChildren(last ? 'Finish' : 'Next', last ? '' : el('span', { 'aria-hidden': 'true', text: ' ›' }));
  for (const [order, id] of Object.entries(READ_ORDER_BUTTONS)) $(id).setAttribute('aria-pressed', String(order === readOrder));
}

// Announced through #panel-status in place of "Details for …".
function walkStatus(node) {
  if (!walk) return null;
  const steps = walkSteps();
  const index = steps.findIndex((step) => step.id === node.id);
  if (index < 0) return null;
  const section = topology.summary.find((s) => s.id === steps[index].section)?.title ?? '';
  return `Step ${index + 1} of ${steps.length}, ${section}: ${node.label}`;
}

// Canvas px covered by the drawer, the walkthrough bar and the overlay buttons,
// so the scene can frame a focused node in what is left. On phones these sit
// outside the canvas and measure as no overlap.
function focusInset() {
  const view = $('scene').getBoundingClientRect();
  const inset = { top: 0, right: 0, bottom: 0, left: 0 };
  const over = (node) => {
    if (!node || node.hidden) return null;
    const r = node.getBoundingClientRect();
    const hit = r.width > 0 && r.height > 0 && r.left < view.right && r.right > view.left && r.top < view.bottom && r.bottom > view.top;
    return hit ? r : null;
  };
  const panel = $('panel').classList.contains('has-content') ? over($('panel')) : null;
  if (panel) {
    if (panel.left - view.left < view.right - panel.right) inset.left = panel.right - view.left;
    else inset.right = view.right - panel.left;
  }
  const bar = over($('walk-bar'));
  if (bar) inset.bottom = view.bottom - bar.top;
  const actions = over($('scene-actions'));
  if (actions) inset.top = actions.bottom - view.top;
  return inset;
}

function setupWalkthrough() {
  // Phones pin the bar to the bottom of the screen and styles.css pads the page
  // by its height, which wraps with the width and is 0 while hidden.
  const bar = $('walk-bar');
  new ResizeObserver(() => document.documentElement.style.setProperty('--walk-bar-h', `${bar.offsetHeight}px`)).observe(bar);
  $('walkthrough').addEventListener('click', () => (walk ? clearSelection() : startWalk()));
  $('walk-prev').addEventListener('click', () => stepWalk(-1));
  $('walk-next').addEventListener('click', () => {
    if (walk && walk.index < walkSteps().length - 1) stepWalk(1);
    else clearSelection();
  });
  $('walk-exit').addEventListener('click', () => clearSelection());
  for (const [order, id] of Object.entries(READ_ORDER_BUTTONS)) {
    $(id).addEventListener('click', () => {
      readOrder = order;
      if (!walk) return;
      // Same node, new position in the order; the camera stays where it is.
      const steps = walkSteps();
      walk.index = Math.max(0, steps.findIndex((step) => step.id === selectedId));
      renderWalkBar(steps);
      const node = topology.nodes.find((n) => n.id === selectedId);
      if (node) $('panel-status').textContent = walkStatus(node) ?? '';
    });
  }
  // Arrow keys step, unless a control that uses them (the selector radios) has focus.
  document.addEventListener('keydown', (event) => {
    if (!walk || event.defaultPrevented || event.altKey || event.ctrlKey || event.metaKey || event.shiftKey) return;
    if (event.key !== 'ArrowLeft' && event.key !== 'ArrowRight') return;
    const target = event.target;
    if (target instanceof HTMLElement && (target.isContentEditable || target.closest('input, select, textarea'))) return;
    event.preventDefault();
    stepWalk(event.key === 'ArrowRight' ? 1 : -1);
  });
}

// --- Idle nudge -------------------------------------------------------------
// Pulses the Walkthrough button once the stage has been on screen for NUDGE_MS
// with nothing clicked, changed, wheeled or pinched. Any of those cancels it
// for the rest of the visit; scrolling the page past the stage does not.
const NUDGE_MS = 6000;

function setupNudge() {
  const view = $('stage-view');
  const button = $('walkthrough');
  let timer = null;
  const observer = new IntersectionObserver(
    (entries) => {
      clearTimeout(timer);
      if (!entries[entries.length - 1].isIntersecting) return;
      timer = setTimeout(() => {
        button.classList.add('is-nudging');
        observer.disconnect();
      }, NUDGE_MS);
    },
    { threshold: 0.5 },
  );
  // One finger on a phone scrolls the page; only a second finger is a gesture.
  const onPress = (event) => (event.pointerType !== 'touch' || !event.isPrimary) && cancel();
  const listeners = [
    [document, 'click', cancel],
    [document, 'change', cancel],
    [view, 'wheel', cancel],
    [view, 'pointerdown', onPress],
  ];
  function cancel() {
    clearTimeout(timer);
    observer.disconnect();
    button.classList.remove('is-nudging');
    for (const [target, type, fn] of listeners) target.removeEventListener(type, fn, true);
  }
  for (const [target, type, fn] of listeners) target.addEventListener(type, fn, { capture: true, passive: true });
  observer.observe(view);
}

// --- Render -----------------------------------------------------------------
function render() {
  const walkId = walk ? selectedId : null;
  topology = buildTopology(state);
  renderSummary();

  const selectionGone = selectedId && !topology.nodes.some((n) => n.id === selectedId);
  if (selectionGone) {
    selectedId = null;
    panelEmpty();
  } else if (selectedId) {
    renderPanel();
  }

  try {
    scene?.update(topology);
    if (selectionGone) syncSceneSelection(null);
  } catch (err) {
    console.warn('scene update failed; falling back to text view', err);
    showFallback();
  }

  // Stay on the same component if it is still drawn, else on the same step number.
  if (walk) {
    const index = walkSteps().findIndex((step) => step.id === walkId);
    goToStep(index >= 0 ? index : walk.index);
  }

  const serialized = serializeHash(state);
  history.replaceState(null, '', serialized ? `#${serialized}` : location.pathname + location.search);
}

// --- Scene ------------------------------------------------------------------
// Reduced motion is read once, when the scene is created. A later OS-level
// change takes effect on reload.
const reducedMotion = () => matchMedia('(prefers-reduced-motion: reduce)').matches;

function showFallback() {
  try {
    scene?.dispose();
  } catch (err) {
    console.warn('scene dispose failed', err);
  }
  scene = null;
  endWalk();
  $('scene-loading').hidden = true;
  $('scene').hidden = true;
  $('scene-actions').hidden = true;
  $('scene-fallback').hidden = false;
  $('scene-fallback').textContent = FALLBACK_NOTICE;
  // Without the canvas there is nothing to click or explain: the panel and legend go away.
  $('stage-view').closest('.stage').classList.add('is-fallback');
}

async function startScene() {
  const container = $('scene');
  try {
    const { createScene } = await import('./scene.js');
    const reduced = reducedMotion();
    scene = createScene(container, {
      onSelect: (id) => {
        if (syncingScene) return;
        if (id) selectNode(id);
        else clearSelection();
      },
      focusInset,
      reducedMotion: reduced,
      debug: new URLSearchParams(location.search).has('debug'),
    });
    scene.update(topology);
    if (selectedId) syncSceneSelection(selectedId);
    if (walk) scene.focus(selectedId);
    $('scene-loading').hidden = true;
    // With reduced motion there are no particles to pause.
    $('pause').hidden = reduced;
    if (new URLSearchParams(location.search).has('debug')) window.__superbankScene = scene;
  } catch (err) {
    console.warn('3D scene unavailable; showing text view only', err);
    showFallback();
  }
}

function setupSceneButtons() {
  const pause = $('pause');
  pause.addEventListener('click', () => {
    const paused = pause.getAttribute('aria-pressed') !== 'true';
    pause.setAttribute('aria-pressed', String(paused));
    scene?.setPaused(paused);
  });
  $('reset-view').addEventListener('click', () => scene?.resetView());
}

// --- Boot -------------------------------------------------------------------
buildControls();
setupDisclosure();
setupSceneButtons();
setupWalkthrough();
setupNudge();
syncControls();
panelEmpty();
render();

window.addEventListener('hashchange', () => {
  state = parseHash(location.hash);
  syncControls();
  render();
});

document.addEventListener('keydown', (event) => {
  if (event.key === 'Escape' && selectedId && !event.defaultPrevented) clearSelection();
});

$('skip-summary').addEventListener('click', () => {
  const target = $('summary');
  target.focus({ preventScroll: true });
  target.scrollIntoView({ block: 'start', behavior: reducedMotion() ? 'auto' : 'smooth' });
});

startScene();
