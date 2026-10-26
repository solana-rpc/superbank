// DOM helpers shared by the explorer (main.js) and the config reference
// (config-page.js). All dynamic text goes through textContent / createElement;
// nothing here parses HTML.

export function el(tag, props = {}, children = []) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(props)) {
    if (value == null || value === false) continue;
    if (key === 'text') node.textContent = value;
    else if (key === 'class') node.className = value;
    else node.setAttribute(key, value === true ? '' : value);
  }
  for (const child of [].concat(children)) if (child) node.append(child);
  return node;
}

// Renders `code` spans without ever parsing HTML. An unbalanced backtick
// (even segment count) is left as plain text.
export function appendRich(parent, text) {
  const parts = String(text).split('`');
  if (parts.length % 2 === 0) {
    parent.append(document.createTextNode(String(text)));
    return parent;
  }
  parts.forEach((part, i) => {
    if (!part) return;
    // Short single tokens (flags, env vars) stay on one line instead of
    // splitting at a hyphen; longer ones (paths) may still wrap to fit the panel.
    const token = part.length <= 40 && !/\s/.test(part);
    parent.append(i % 2 ? el('code', { text: part, class: token ? 'is-token' : null }) : document.createTextNode(part));
  });
  return parent;
}

export function rich(tag, text, props = {}) {
  return appendRich(el(tag, props), text);
}
