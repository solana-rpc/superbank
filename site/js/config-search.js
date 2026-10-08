// Search, filters and URL params for the config reference. Pure: no DOM and
// no data imports, so the tests drive it with fixtures.
//
// Matching works on "compacted" identifiers: lowercase with everything but
// [a-z0-9] removed, so `disk cache retain`, `DISK_CACHE_RETAIN` and
// `--disk-cache-retain` all compare equal to part of DISK_CACHE_RETAIN_SLOTS.
// Each query token must match (AND). An entry's tier is the weakest of its
// tokens' tiers:
//   3  the whole query equals one of the entry's names
//   2  every token is a substring of a name, or a word prefix in its prose
//   1  some token only matches a name fuzzily (word-prefix chunks such as
//      `dcqt` for DISK_CACHE_QUERY_TIMEOUT_MS, or a tight subsequence that
//      survives a dropped letter)
// When anything reaches tier 2, tier-1 entries are dropped, so fuzzy matching
// only widens the results when nothing matches more precisely.

export const MAX_QUERY = 100;

// Subsequence matches shorter than this are too permissive to be useful.
const MIN_SUBSEQUENCE = 3;

export const compact = (value) => String(value ?? '').toLowerCase().replace(/[^a-z0-9]/g, '');

export function tokenize(query) {
  return String(query ?? '')
    .slice(0, MAX_QUERY)
    .split(/\s+/)
    .map(compact)
    .filter(Boolean);
}

// A name compacted, with the source index of each kept character and the
// compacted indices where a word starts.
function prepareName(raw) {
  const str = String(raw ?? '');
  const chars = [];
  const map = [];
  for (let i = 0; i < str.length; i++) {
    const ch = str[i].toLowerCase();
    if (/[a-z0-9]/.test(ch)) {
      chars.push(ch);
      map.push(i);
    }
  }
  const starts = [];
  for (let k = 0; k < map.length; k++) if (k === 0 || map[k] !== map[k - 1] + 1) starts.push(k);
  return { text: chars.join(''), map, starts };
}

// Matches `token` as a run of word-prefix chunks, in order, skipping words as
// needed: `dcqt` is d(isk) c(ache) q(uery) t(imeout). Returns compacted
// indices or null. Memoized on (pos, word), so a hostile query stays cheap.
function chunkMatch(token, name) {
  const { text, starts } = name;
  const wordEnd = (w) => (w + 1 < starts.length ? starts[w + 1] : text.length);
  const memo = new Map();
  const walk = (pos, word) => {
    const key = pos * (starts.length + 1) + word;
    if (!memo.has(key)) memo.set(key, step(pos, word));
    return memo.get(key);
  };
  const step = (pos, word) => {
    if (pos === token.length) return [];
    for (let w = word; w < starts.length; w++) {
      const begin = starts[w];
      let len = 0;
      while (pos + len < token.length && begin + len < wordEnd(w) && text[begin + len] === token[pos + len]) len++;
      for (let take = len; take >= 1; take--) {
        const rest = walk(pos + take, w + 1);
        if (rest) return [...Array.from({ length: take }, (_, i) => begin + i), ...rest];
      }
    }
    return null;
  };
  return walk(0, 0);
}

// Tightest subsequence of `token` in the name, accepted only if its span is
// at most twice the token length (a dropped or swapped letter, not a scatter).
function subsequenceMatch(token, name) {
  if (token.length < MIN_SUBSEQUENCE) return null;
  const { text } = name;
  let best = null;
  for (let start = text.indexOf(token[0]); start !== -1; start = text.indexOf(token[0], start + 1)) {
    const hits = [start];
    for (let i = 1, at = start + 1; i < token.length; i++, at++) {
      at = text.indexOf(token[i], at);
      if (at === -1) return best;
      hits.push(at);
    }
    const span = hits[hits.length - 1] - start + 1;
    if (span <= token.length * 2 && (!best || span < best[best.length - 1] - best[0] + 1)) best = hits;
  }
  return best;
}

function fuzzyIndices(token, name) {
  return chunkMatch(token, name) ?? subsequenceMatch(token, name);
}

// Prepared search data per entry, built on first use.
const prepared = new WeakMap();

function prepareEntry(entry) {
  let p = prepared.get(entry);
  if (!p) {
    p = {
      names: [...new Set(entry.names ?? [])].filter(Boolean).map(prepareName),
      words: String(entry.prose ?? '')
        .toLowerCase()
        .split(/[^a-z0-9]+/)
        .filter(Boolean),
    };
    prepared.set(entry, p);
  }
  return p;
}

function tokenTier(token, p) {
  if (p.names.some((n) => n.text.includes(token))) return 2;
  if (p.words.some((w) => w.startsWith(token))) return 2;
  if (p.names.some((n) => fuzzyIndices(token, n))) return 1;
  return 0;
}

// Tier (0-3) of one entry for already-tokenized input.
export function entryTier(entry, tokens) {
  if (tokens.length === 0) return 0;
  const p = prepareEntry(entry);
  const whole = tokens.join('');
  if (p.names.some((n) => n.text === whole)) return 3;
  let tier = 2;
  for (const token of tokens) {
    tier = Math.min(tier, tokenTier(token, p));
    if (tier === 0) return 0;
  }
  return tier;
}

// Returns null for an empty query (show everything), otherwise a Map of the
// entries to show -> their tier.
export function search(entries, query) {
  const tokens = tokenize(query);
  if (tokens.length === 0) return null;
  const tiers = new Map();
  let best = 0;
  for (const entry of entries) {
    const tier = entryTier(entry, tokens);
    if (tier > 0) {
      tiers.set(entry, tier);
      best = Math.max(best, tier);
    }
  }
  const floor = best >= 2 ? 2 : 1;
  for (const [entry, tier] of tiers) if (tier < floor) tiers.delete(entry);
  return tiers;
}

// [start, end) ranges of `text` to highlight for `query`. Separator-only gaps
// between matched characters are bridged so DISK_CACHE reads as one mark.
export function highlightRanges(text, query) {
  const tokens = tokenize(query);
  const str = String(text ?? '');
  if (tokens.length === 0 || !str) return [];
  const name = prepareName(str);
  const hit = new Set();
  if (name.text === tokens.join('')) name.map.forEach((_, k) => hit.add(k));
  else {
    for (const token of tokens) {
      const at = name.text.indexOf(token);
      const indices = at !== -1 ? Array.from({ length: token.length }, (_, i) => at + i) : fuzzyIndices(token, name) ?? [];
      for (const k of indices) hit.add(k);
    }
  }
  const sorted = [...hit].sort((a, b) => a - b).map((k) => name.map[k]);
  const ranges = [];
  for (const i of sorted) {
    const last = ranges[ranges.length - 1];
    if (last && /^[^a-z0-9]*$/i.test(str.slice(last[1], i))) last[1] = i + 1;
    else ranges.push([i, i + 1]);
  }
  return ranges;
}

// --- Filters ----------------------------------------------------------------
// OR within a category, AND across categories. `s` (ingest source) only
// applies to source-scoped entries: those with no source restriction apply to
// every source, entries of other components never match.
export function applyFilters(entries, { c = [], f = [], s = [] } = {}) {
  return entries.filter(
    (e) =>
      (c.length === 0 || c.includes(e.component)) &&
      (f.length === 0 || e.features.some((x) => f.includes(x))) &&
      (s.length === 0 || (e.sourceScoped && (e.sources.length === 0 || e.sources.some((x) => s.includes(x))))),
  );
}

// --- URL params --------------------------------------------------------------
// The query string is untrusted: list params are reduced to the allowed values
// (in canonical order, deduplicated) and q is cut to MAX_QUERY characters.
export const EMPTY_PARAMS = Object.freeze({ q: '', c: Object.freeze([]), f: Object.freeze([]), s: Object.freeze([]) });

export function parseParams(search, allowed) {
  const params = new URLSearchParams(String(search ?? '').replace(/^\?/, ''));
  const pick = (key, values) => {
    const wanted = new Set(params.getAll(key).flatMap((raw) => raw.split(',')));
    return values.filter((v) => wanted.has(v));
  };
  return {
    q: (params.get('q') ?? '').slice(0, MAX_QUERY),
    c: pick('c', allowed.components),
    f: pick('f', allowed.features),
    s: pick('s', allowed.sources),
  };
}

export function serializeParams({ q = '', c = [], f = [], s = [] } = {}) {
  const params = new URLSearchParams();
  if (String(q).trim()) params.set('q', String(q).slice(0, MAX_QUERY));
  for (const [key, values] of [['c', c], ['f', f], ['s', s]]) for (const v of values) params.append(key, v);
  return params.toString();
}
