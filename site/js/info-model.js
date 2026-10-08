// Sources for the Info page, taken from the data each page already carries,
// so the list cannot drift from what the pages actually cite. Pure; no DOM.

import { allRefs as explorerRefs } from './content.js';
import { allRefs as alpenglowRefs } from './alpenglow-content.js';
import { SOURCES as ALPENGLOW_SOURCES } from './alpenglow-model.js';
import { COMPONENTS, buildModel } from './config-model.js';

const sortedUnique = (paths) => [...new Set(paths)].sort((a, b) => a.localeCompare(b));

function configRefs() {
  const paths = [];
  for (const c of COMPONENTS) paths.push(c.source, c.readme);
  for (const e of buildModel().entries) paths.push(e.source);
  return paths;
}

// One group per page, in nav order: { id, title, href, paths }.
export function pageSources() {
  return [
    { id: 'architecture', title: 'Architecture explorer', href: './', paths: sortedUnique(explorerRefs()) },
    { id: 'configuration', title: 'Configuration reference', href: 'config.html', paths: sortedUnique(configRefs()) },
    { id: 'alpenglow', title: 'Alpenglow', href: 'alpenglow.html', paths: sortedUnique(alpenglowRefs()) },
  ];
}

// Upstream citations (Agave, SIMDs, docs.rs), pinned to a tag, commit or crate
// version so their line anchors keep pointing at the quoted text. Only the
// Alpenglow page cites outside the repository.
export function upstreamSources() {
  return Object.entries(ALPENGLOW_SOURCES).map(([id, { label, url }]) => ({ id, page: 'alpenglow', label, url }));
}
