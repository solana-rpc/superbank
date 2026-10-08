// Selector state for the architecture explorer, plus URL-hash (de)serialization.
// The hash is untrusted input: only enumerated values are accepted, everything
// else silently falls back to the default.

export const ENUMS = Object.freeze({
  source: Object.freeze(['grpc', 'fumarole', 'rpc', 'bigtable', 'solparq', 'jetstreamer']),
  ch: Object.freeze(['single', 'cluster', 'replicated']),
  archive: Object.freeze(['off', 'local', 's3']),
  flow: Object.freeze(['blocks', 'requests', 'both']),
});

export const FLAGS = Object.freeze(['head', 'disk', 'stream', 'verify']);

export const DEFAULT_STATE = Object.freeze({
  source: 'grpc',
  ch: 'single',
  head: false,
  disk: false,
  stream: false,
  archive: 'local',
  verify: false,
  flow: 'blocks',
});

// Returns a complete, valid state. Unknown keys are dropped; invalid values
// fall back to DEFAULT_STATE.
export function normalizeState(input) {
  const state = { ...DEFAULT_STATE };
  if (!input || typeof input !== 'object') return state;
  for (const [key, values] of Object.entries(ENUMS)) {
    if (values.includes(input[key])) state[key] = input[key];
  }
  for (const key of FLAGS) {
    if (typeof input[key] === 'boolean') state[key] = input[key];
  }
  return state;
}

export function parseHash(hash) {
  const state = { ...DEFAULT_STATE };
  const params = new URLSearchParams(String(hash ?? '').replace(/^#/, ''));
  for (const [key, values] of Object.entries(ENUMS)) {
    const value = params.get(key);
    if (value !== null && values.includes(value)) state[key] = value;
  }
  for (const key of FLAGS) {
    const value = params.get(key);
    if (value === '1') state[key] = true;
    else if (value === '0') state[key] = false;
  }
  return state;
}

// Only non-default values are written so shared links stay short. The default
// state serializes to the empty string.
export function serializeHash(state) {
  const normalized = normalizeState(state);
  const params = new URLSearchParams();
  for (const key of Object.keys(ENUMS)) {
    if (normalized[key] !== DEFAULT_STATE[key]) params.set(key, normalized[key]);
  }
  for (const key of FLAGS) {
    if (normalized[key] !== DEFAULT_STATE[key]) params.set(key, normalized[key] ? '1' : '0');
  }
  return params.toString();
}

// Every valid state, for exhaustive tests (6*3*3*3*2^4 = 2592 states).
export function* allStates() {
  for (const source of ENUMS.source)
    for (const ch of ENUMS.ch)
      for (const archive of ENUMS.archive)
        for (const flow of ENUMS.flow)
          for (let bits = 0; bits < 1 << FLAGS.length; bits++) {
            const state = { source, ch, archive, flow };
            FLAGS.forEach((key, i) => {
              state[key] = Boolean(bits & (1 << i));
            });
            yield state;
          }
}
