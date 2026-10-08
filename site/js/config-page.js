// Config reference page wiring: renders every option once, then search and
// filters only toggle `hidden`, update counts and redraw name highlights.
//
// All dynamic text goes through textContent / createElement (dom.js). The
// query string is untrusted: parseParams() reduces filters to enumerated
// values and the query only ever reaches input.value and text nodes. The hash
// names an item; it is looked up with getElementById, never parsed as HTML or
// used as a selector.

import { appendRich, el, rich } from './dom.js';
import { COMPONENTS, COMPONENT_IDS, FEATURES, SOURCES, buildModel } from './config-model.js';
import { EMPTY_PARAMS, applyFilters, highlightRanges, parseParams, search, serializeParams } from './config-search.js';

const REPO_BLOB = 'https://github.com/solana-rpc/superbank/blob/main/';
const ALLOWED = { components: COMPONENT_IDS, features: FEATURES, sources: SOURCES };

const FILTERS = [
  { key: 'c', label: 'Component', values: COMPONENTS.map((c) => [c.id, c.label, c.summary]) },
  { key: 'f', label: 'Cargo feature', values: FEATURES.map((f) => [f, f, `superbank-rpc built with --features ${f}`]) },
  { key: 's', label: 'Ingest source', values: SOURCES.map((s) => [s, s, `superbank --source ${s}`]) },
];

const $ = (id) => document.getElementById(id);
const model = buildModel();
const views = new Map(); // entry id -> { article, names: [[code element, text]] }
const groupViews = []; // { section, count, tocCount, tocItem, entries }
const componentViews = []; // { section, tocItem, groups }

let state = parseParams(location.search, ALLOWED);

// --- Rendering ---------------------------------------------------------------

const chipClass = (kind, value) => `chip chip--${kind} chip--${kind[0]}-${value}`;

function filterChip(key, value, label, title) {
  const kind = { c: 'comp', f: 'feature', s: 'source' }[key];
  const prefix = { f: 'feature', s: 'source' }[key];
  return el('button', { type: 'button', class: chipClass(kind, value), 'data-filter': key, 'data-value': value, 'aria-pressed': 'false', title }, [
    prefix ? el('span', { class: 'chip__kind', text: prefix }) : null,
    el('span', { text: label }),
  ]);
}

function itemLink(id) {
  const target = model.byId.get(id);
  return target ? el('a', { href: `#${id}`, class: 'cfg-link' }, [el('code', { text: target.name })]) : null;
}

function renderChips(entry) {
  const chips = [filterChip('c', entry.component, entry.componentLabel, `Filter to ${entry.componentLabel}`)];
  for (const req of entry.requires) {
    if (req.kind === 'feature') chips.push(filterChip('f', req.value, req.value, `Needs superbank-rpc built with --features ${req.value}. Click to filter.`));
    else if (req.kind === 'source') chips.push(filterChip('s', req.value, req.value, `Only used with superbank --source ${req.value}. Click to filter.`));
    else if (req.kind === 'subcommand') chips.push(el('span', { class: 'chip chip--cond' }, [el('span', { class: 'chip__kind', text: 'subcommand' }), req.value]));
    else if (req.kind === 'when') {
      const id = `${entry.component}.${req.key}`;
      const target = model.byId.get(id);
      chips.push(
        el('a', { class: 'chip chip--when', href: `#${id}`, title: 'Only takes effect when this is set' }, [
          el('span', { class: 'chip__kind', text: 'when' }),
          `${target?.name ?? req.key}=${req.equals}`,
        ]),
      );
    }
  }
  if (entry.required === true) chips.push(el('span', { class: 'chip chip--required', text: 'required' }));
  else if (entry.required) chips.push(appendRich(el('span', { class: 'chip chip--required' }), `required ${entry.required}`));
  if (entry.status === 'deprecated') chips.push(el('span', { class: 'chip chip--deprecated', text: 'deprecated' }));
  if (entry.secret) chips.push(el('span', { class: 'chip chip--secret', text: 'secret' }));
  return el('div', { class: 'chips' }, chips);
}

function renderItem(entry) {
  const nameCode = el('code', { class: 'cfg-item__name-text', text: entry.name });
  const names = [[nameCode, entry.name]];
  const forms = [];
  for (const [label, value] of [
    ['Env', entry.env],
    ['Flag', entry.flag],
    ['YAML', entry.yaml],
  ]) {
    if (!value) continue;
    const code = el('code', { text: value });
    names.push([code, value]);
    forms.push(el('div', { class: 'cfg-form' }, [el('dt', { text: label }), el('dd', {}, [code])]));
  }

  const meta = [el('span', {}, [el('span', { class: 'cfg-meta__label', text: 'Type ' }), entry.type])];
  if (entry.default !== undefined) {
    meta.push(el('span', {}, [el('span', { class: 'cfg-meta__label', text: 'Default ' }), entry.default === '' ? el('span', { class: 'cfg-meta__empty', text: 'empty' }) : el('code', { text: entry.default })]));
  }

  const linkItems = [];
  const byLabel = new Map();
  for (const link of model.links.get(entry.id)) {
    if (!link.resolved) continue;
    if (!byLabel.has(link.label)) byLabel.set(link.label, []);
    byLabel.get(link.label).push(link.to);
  }
  for (const [label, ids] of byLabel) {
    linkItems.push(el('li', {}, [el('span', { class: 'cfg-links__label', text: label }), ...ids.map((id) => itemLink(id))]));
  }
  const also = model.alsoIn.get(entry.id);
  if (also.length) {
    linkItems.push(
      el('li', {}, [
        el('span', { class: 'cfg-links__label', text: 'Also in' }),
        ...also.map((id) => {
          const other = model.byId.get(id);
          return el('a', { href: `#${id}`, class: `cfg-link cfg-link--comp chip--c-${other.component}` }, [el('span', { class: 'cfg-link__comp', text: other.componentLabel }), el('code', { text: other.name })]);
        }),
      ]),
    );
  }

  const article = el('article', { class: `cfg-item${entry.status === 'deprecated' ? ' is-deprecated' : ''}`, id: entry.id, 'aria-labelledby': `${entry.id}--name` }, [
    el('div', { class: 'cfg-item__head' }, [
      el('h4', { class: 'cfg-item__name', id: `${entry.id}--name` }, [nameCode]),
      el('a', { class: 'cfg-item__anchor', href: `#${entry.id}`, 'aria-label': `Link to ${entry.name}`, text: '#' }),
    ]),
    renderChips(entry),
    rich('p', entry.text, { class: 'cfg-item__text' }),
    el('dl', { class: 'cfg-forms' }, forms),
    el('p', { class: 'cfg-meta' }, meta),
    linkItems.length ? el('ul', { class: 'cfg-links' }, linkItems) : null,
  ]);
  views.set(entry.id, { article, names });
  return article;
}

function render() {
  const sections = $('cfg-sections');
  const toc = $('cfg-toc-list');
  for (const component of COMPONENTS) {
    const entries = model.entries.filter((e) => e.component === component.id);
    const headingId = `c-${component.id}`;
    const tocGroups = el('ol', { class: 'cfg-toc__groups' });
    const tocCount = el('span', { class: 'cfg-toc__count' });
    const tocItem = el('li', { class: 'cfg-toc__comp' }, [
      el('a', { href: `#${headingId}`, class: `cfg-toc__link chip--c-${component.id}` }, [el('span', { class: 'cfg-toc__swatch', 'aria-hidden': 'true' }), el('span', { text: component.label }), tocCount]),
      tocGroups,
    ]);
    toc.append(tocItem);

    const section = el('section', { class: `cfg-comp chip--c-${component.id}`, 'aria-labelledby': `${headingId}-title` });
    section.append(
      el('header', { class: 'cfg-comp__head', id: headingId, tabindex: '-1' }, [
        el('h2', { class: 'cfg-comp__title', id: `${headingId}-title` }, [el('span', { class: 'cfg-comp__swatch', 'aria-hidden': 'true' }), component.label]),
        rich('p', component.summary, { class: 'cfg-comp__summary' }),
        rich('p', component.intro, { class: 'cfg-comp__intro' }),
        el('p', { class: 'cfg-comp__refs' }, [
          el('a', { href: `${REPO_BLOB}${component.source}`, target: '_blank', rel: 'noopener noreferrer' }, [el('code', { text: component.source })]),
          el('a', { href: `${REPO_BLOB}${component.readme}`, target: '_blank', rel: 'noopener noreferrer' }, [el('code', { text: component.readme })]),
        ]),
      ]),
    );

    const groups = [];
    for (const group of component.groups) {
      const groupEntries = entries.filter((e) => e.group === group.id);
      const groupId = `g-${component.id}-${group.id}`;
      const count = el('span', { class: 'cfg-group__count' });
      const groupTocCount = el('span', { class: 'cfg-toc__count' });
      const groupTocItem = el('li', {}, [el('a', { href: `#${groupId}`, class: 'cfg-toc__link' }, [el('span', { text: group.title }), groupTocCount])]);
      tocGroups.append(groupTocItem);
      const groupSection = el('section', { class: 'cfg-group', 'aria-labelledby': groupId }, [
        el('h3', { class: 'cfg-group__title', id: groupId, tabindex: '-1' }, [group.title, count]),
        group.intro ? rich('p', group.intro, { class: 'cfg-group__intro' }) : null,
        el('div', { class: 'cfg-items' }, groupEntries.map(renderItem)),
      ]);
      section.append(groupSection);
      const view = { section: groupSection, count, tocCount: groupTocCount, tocItem: groupTocItem, entries: groupEntries };
      groups.push(view);
      groupViews.push(view);
    }
    sections.append(section);
    componentViews.push({ section, tocItem, tocCount, groups });
  }

  for (const filter of FILTERS) {
    const labelId = `cfg-filter-${filter.key}`;
    $('cfg-filter-groups').append(
      el('div', { class: 'ctl-group', role: 'group', 'aria-labelledby': labelId }, [
        el('span', { class: 'ctl-group__label', id: labelId, text: filter.label }),
        el('div', { class: 'chips' }, filter.values.map(([value, label, title]) => filterChip(filter.key, value, label, title))),
      ]),
    );
  }
  $('cfg-loading').remove();
}

// --- Applying state -------------------------------------------------------------

function setHighlight(code, text, query) {
  const ranges = query ? highlightRanges(text, query) : [];
  if (!ranges.length) {
    if (code.childElementCount) code.textContent = text;
    return;
  }
  const parts = [];
  let at = 0;
  for (const [start, end] of ranges) {
    if (start > at) parts.push(text.slice(at, start));
    parts.push(el('mark', { text: text.slice(start, end) }));
    at = end;
  }
  if (at < text.length) parts.push(text.slice(at));
  code.replaceChildren(...parts);
}

const hasFilters = (s) => s.c.length + s.f.length + s.s.length > 0;
const isActive = (s) => hasFilters(s) || s.q.trim() !== '';

function apply() {
  const filtered = applyFilters(model.entries, state);
  const tiers = search(filtered, state.q);
  const visible = new Set(tiers ? tiers.keys() : filtered);

  for (const entry of model.entries) {
    const view = views.get(entry.id);
    const shown = visible.has(entry);
    view.article.hidden = !shown;
    view.article.classList.toggle('is-exact', shown && tiers?.get(entry) === 3);
    for (const [code, text] of view.names) setHighlight(code, text, shown ? state.q : '');
  }
  for (const group of groupViews) {
    const n = group.entries.filter((e) => visible.has(e)).length;
    group.section.hidden = n === 0;
    group.count.textContent = String(n);
    group.tocCount.textContent = String(n);
    group.tocItem.classList.toggle('is-empty', n === 0);
  }
  for (const comp of componentViews) {
    const n = comp.groups.reduce((sum, g) => sum + g.entries.filter((e) => visible.has(e)).length, 0);
    comp.section.hidden = n === 0;
    comp.tocCount.textContent = String(n);
    comp.tocItem.classList.toggle('is-empty', n === 0);
  }

  const total = model.entries.length;
  $('cfg-count').textContent = isActive(state) ? `${visible.size} of ${total} options` : `${total} options`;
  $('cfg-empty').hidden = visible.size > 0;

  for (const button of document.querySelectorAll('button[data-filter]')) {
    button.setAttribute('aria-pressed', String(state[button.dataset.filter].includes(button.dataset.value)));
  }
  const active = FILTERS.flatMap((f) => state[f.key].map((v) => f.values.find(([value]) => value === v)?.[1] ?? v));
  $('cfg-filter-state').textContent = active.join(' · ');
  $('cfg-reset').hidden = !hasFilters(state);
}

// Mirrors the state into the query string. Typing goes through a debounce:
// Safari throws once a page makes 100 replaceState calls in 30 seconds.
let urlTimer = 0;
function writeUrl() {
  clearTimeout(urlTimer);
  const query = serializeParams(state);
  try {
    history.replaceState(history.state, '', `${location.pathname}${query ? `?${query}` : ''}${location.hash}`);
  } catch {
    // The URL is a convenience; the page state is already applied.
  }
}

let frame = 0;
function scheduleApply() {
  if (!frame) frame = requestAnimationFrame(() => {
    frame = 0;
    apply();
  });
  clearTimeout(urlTimer);
  urlTimer = setTimeout(writeUrl, 300);
}

function setState(next) {
  state = { ...state, ...next };
  apply();
  writeUrl();
}

function resetAll() {
  $('cfg-q').value = '';
  setState({ ...EMPTY_PARAMS });
}

// --- Navigation -------------------------------------------------------------------

function idFromHash(hash) {
  try {
    return decodeURIComponent(String(hash).replace(/^#/, ''));
  } catch {
    return '';
  }
}

// Scrolls to the item, group or component named by the hash. A target hidden
// by the current search or filters clears them first.
function reveal(id, { focus = true } = {}) {
  const target = id ? document.getElementById(id) : null;
  if (!target || !$('cfg-results').contains(target)) return;
  if (target.closest('[hidden]')) resetAll();
  target.scrollIntoView({ block: 'start' });
  if (target.matches('.cfg-item')) {
    target.classList.remove('is-flash');
    void target.offsetWidth; // restart the animation
    target.classList.add('is-flash');
  }
  if (focus) (target.matches('.cfg-item') ? target.querySelector('.cfg-item__anchor') : target).focus({ preventScroll: true });
}

document.addEventListener('click', (event) => {
  const link = event.target.closest('a[href^="#"]');
  if (link && !event.defaultPrevented && event.button === 0 && !event.metaKey && !event.ctrlKey && !event.shiftKey && !event.altKey) {
    const id = idFromHash(link.getAttribute('href'));
    if (!document.getElementById(id)) return;
    event.preventDefault();
    if (idFromHash(location.hash) !== id) history.pushState(null, '', `#${encodeURIComponent(id)}`);
    reveal(id);
    // Close the mobile table of contents after a jump.
    if (link.closest('.cfg-toc') && matchMedia('(max-width: 999px)').matches) $('cfg-toc-details').open = false;
    return;
  }

  const chip = event.target.closest('button[data-filter]');
  if (chip) {
    const { filter: key, value } = chip.dataset;
    const values = state[key].includes(value) ? state[key].filter((v) => v !== value) : [...state[key], value];
    // Keep the clicked chip's item where it is on screen while the list reflows.
    const anchor = chip.closest('.cfg-item');
    const before = anchor?.getBoundingClientRect().top;
    setState({ [key]: ALLOWED[{ c: 'components', f: 'features', s: 'sources' }[key]].filter((v) => values.includes(v)) });
    if (anchor && !anchor.hidden) window.scrollBy(0, anchor.getBoundingClientRect().top - before);
  }
});

window.addEventListener('popstate', () => reveal(idFromHash(location.hash), { focus: false }));

// --- Search input -------------------------------------------------------------------

const input = $('cfg-q');
input.addEventListener('input', () => {
  state = { ...state, q: input.value };
  scheduleApply();
});
input.addEventListener('keydown', (event) => {
  if (event.key === 'Escape' && input.value) {
    event.preventDefault();
    input.value = '';
    setState({ q: '' });
  }
});
document.addEventListener('keydown', (event) => {
  if (event.key !== '/' || event.metaKey || event.ctrlKey || event.altKey) return;
  const active = document.activeElement;
  if (active && (active.isContentEditable || /^(INPUT|TEXTAREA|SELECT)$/.test(active.tagName))) return;
  event.preventDefault();
  input.focus();
  input.select();
});

$('cfg-reset').addEventListener('click', () => setState({ c: [], f: [], s: [] }));
$('cfg-empty-reset').addEventListener('click', resetAll);

// The filter dropdown closes on a click outside it or on Escape.
const filters = $('cfg-filters');
document.addEventListener('click', (event) => {
  if (filters.open && !filters.contains(event.target)) filters.open = false;
});
filters.addEventListener('keydown', (event) => {
  if (event.key === 'Escape' && filters.open) {
    filters.open = false;
    filters.querySelector('summary').focus();
  }
});

// The sticky bar's height feeds scroll-padding-top and the sticky TOC offset.
// Set once synchronously so a deep link on load lands below the bar.
const bar = $('cfg-bar');
const syncBarHeight = () => document.documentElement.style.setProperty('--bar-h', `${bar.offsetHeight}px`);
new ResizeObserver(syncBarHeight).observe(bar);

// --- Start -------------------------------------------------------------------------

render();
input.value = state.q;
$('cfg-toc-details').open = matchMedia('(min-width: 1000px)').matches;
apply();
writeUrl(); // drops junk params from a shared link
syncBarHeight();
if (location.hash) reveal(idFromHash(location.hash), { focus: false });
