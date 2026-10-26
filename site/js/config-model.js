// Config reference model: the per-component data modules flattened into
// entries, plus the indexes the page needs: relations in both directions and
// same-name "also in" links across components. Pure; no DOM.
//
// Data contract (one module per component under ./config-data/):
//   { id, label, summary, source, readme, primary: 'env' | 'flag' | 'yaml', intro,
//     groups: [{ id, title, intro?, requires?, items: [item] }] }
//   item: at least one of env / flag / yaml, plus type, text and optionally
//     key, default, required, requires, relations, status, secret, source.
// `requires` entries are `kind:value` strings (see REQUIRE_KINDS). A group's
// `requires` applies to all its items; `when:` entries that name the item
// itself are dropped, so a toggle never requires itself.

import { compact } from './config-search.js';
import superbank from './config-data/superbank.js';
import rpc from './config-data/rpc.js';
import solparq from './config-data/solparq.js';
import solparqRead from './config-data/solparq-read.js';
import verify from './config-data/verify.js';
import jetstreamer from './config-data/jetstreamer.js';

export const COMPONENTS = Object.freeze([superbank, rpc, solparq, solparqRead, verify, jetstreamer]);
export const COMPONENT_IDS = Object.freeze(COMPONENTS.map((c) => c.id));

// Cargo features of superbank-rpc, in the order the explorer lists them.
export const FEATURES = Object.freeze(['grpc-head-cache', 'disk-cache', 'grpc-streaming', 'pyroscope']);

// `superbank --source` values (the jetstreamer plugin is a separate binary).
export const SOURCES = Object.freeze(['grpc', 'fumarole', 'rpc', 'bigtable', 'solparq']);

export const SUBCOMMANDS = Object.freeze(['list', 'summary', 'schema', 'scan']);

// feature: compile-time Cargo feature; source: ingest source; subcommand:
// solparq-read subcommand; when: KEY=value where KEY is an item of the same
// component (rendered as a link).
export const REQUIRE_KINDS = Object.freeze(['feature', 'source', 'subcommand', 'when']);

// Relation type -> [label on the item that declares it, label on the target].
export const RELATIONS = Object.freeze({
  requires: Object.freeze(['Requires', 'Required by']),
  conflicts: Object.freeze(['Conflicts with', 'Conflicts with']),
  'capped-by': Object.freeze(['Capped by', 'Caps']),
  'alias-of': Object.freeze(['Alias of', 'Has alias']),
  see: Object.freeze(['See also', 'See also']),
});

export const STATUSES = Object.freeze(['deprecated']);

export const itemKey = (item) => item.key ?? item.env ?? item.yaml ?? item.flag?.replace(/^--/, '');

// `superbank:CLICKHOUSE_URL` names another component's item; a bare key names
// one in the same component.
export function resolveRef(componentId, ref) {
  const at = String(ref).indexOf(':');
  return at === -1 ? `${componentId}.${ref}` : `${ref.slice(0, at)}.${ref.slice(at + 1)}`;
}

export function parseRequire(raw) {
  const text = String(raw);
  const at = text.indexOf(':');
  const kind = at === -1 ? '' : text.slice(0, at);
  const value = at === -1 ? text : text.slice(at + 1);
  if (kind !== 'when') return { kind, value };
  const eq = value.indexOf('=');
  return { kind, value, key: eq === -1 ? value : value.slice(0, eq), equals: eq === -1 ? '' : value.slice(eq + 1) };
}

// The name an item is listed under: the form operators of that component use
// most, falling back to whichever forms the item has.
export const PRIMARY = Object.freeze(['env', 'flag', 'yaml']);

export function displayName(component, item) {
  if (component.primary === 'flag') return item.flag ?? item.env ?? item.yaml;
  if (component.primary === 'yaml') return item.yaml ?? item.flag ?? item.env;
  return item.env ?? item.flag ?? item.yaml;
}

export function buildModel(components = COMPONENTS) {
  const entries = [];
  const byId = new Map();
  const duplicates = [];

  for (const component of components) {
    for (const group of component.groups) {
      for (const item of group.items) {
        const key = itemKey(item);
        const id = `${component.id}.${key}`;
        const requires = [];
        for (const raw of [...(group.requires ?? []), ...(item.requires ?? [])]) {
          const req = parseRequire(raw);
          if (req.kind === 'when' && req.key === key) continue;
          if (!requires.some((r) => r.kind === req.kind && r.value === req.value)) requires.push(req);
        }
        const name = displayName(component, item);
        const entry = {
          id,
          key,
          component: component.id,
          componentLabel: component.label,
          group: group.id,
          groupTitle: group.title,
          name,
          env: item.env,
          flag: item.flag,
          yaml: item.yaml,
          type: item.type,
          default: item.default,
          required: item.required,
          text: item.text,
          status: item.status,
          secret: Boolean(item.secret),
          source: item.source ?? component.source,
          requires,
          features: requires.filter((r) => r.kind === 'feature').map((r) => r.value),
          sources: requires.filter((r) => r.kind === 'source').map((r) => r.value),
          sourceScoped: component.id === 'superbank',
          relations: item.relations ?? [],
          // Search fields (config-search.js): names compare as identifiers,
          // prose by word prefix.
          names: [name, item.env, item.flag, item.yaml].filter(Boolean),
          prose: [item.text, group.title, component.label, ...requires.map((r) => r.value)].join(' '),
        };
        if (byId.has(id)) duplicates.push(id);
        entries.push(entry);
        byId.set(id, entry);
      }
    }
  }

  // Relations, both directions. Unresolvable targets are kept (marked) so the
  // tests can report them; the page skips them.
  // A link already shown under the same label is not repeated (two items that
  // both say "see" each other).
  const links = new Map(entries.map((e) => [e.id, []]));
  const addLink = (from, link) => {
    const list = links.get(from);
    if (!list.some((l) => l.label === link.label && l.to === link.to)) list.push(link);
  };
  for (const entry of entries) {
    for (const rel of entry.relations) {
      const to = resolveRef(entry.component, rel.to);
      const labels = RELATIONS[rel.type];
      addLink(entry.id, { type: rel.type, label: labels?.[0], to, resolved: byId.has(to) });
      if (byId.has(to) && to !== entry.id) addLink(to, { type: rel.type, label: labels?.[1], to: entry.id, resolved: true, inverse: true });
    }
  }

  // Same name (env, flag or YAML key, compared compacted) in another component.
  const byName = new Map();
  for (const entry of entries) {
    for (const name of new Set([entry.env, entry.flag, entry.yaml].filter(Boolean).map(compact))) {
      if (!byName.has(name)) byName.set(name, []);
      byName.get(name).push(entry);
    }
  }
  const alsoIn = new Map(entries.map((e) => [e.id, []]));
  for (const group of byName.values()) {
    for (const a of group) {
      for (const b of group) {
        if (a.component !== b.component && !alsoIn.get(a.id).includes(b.id)) alsoIn.get(a.id).push(b.id);
      }
    }
  }

  return { components, entries, byId, links, alsoIn, duplicates };
}
