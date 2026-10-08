// Selector state for the Alpenglow block-lifecycle page, plus URL-hash
// (de)serialization. The hash is untrusted input: only enumerated values are
// accepted, everything else silently falls back to the default.

export const ENUMS = Object.freeze({
  era: Object.freeze(['alpenglow', 'tower']),
  // HEAD_CACHE_MIN_COMMITMENT on superbank-rpc.
  min: Object.freeze(['processed', 'confirmed', 'finalized']),
  scenario: Object.freeze(['clean', 'fork', 'retry', 'reconnect']),
  // The read probe: which JSON-RPC call, at which commitment.
  method: Object.freeze(['getTransaction', 'getBlock']),
  read: Object.freeze(['processed', 'confirmed', 'finalized']),
});

export const DEFAULT_STATE = Object.freeze({
  era: 'alpenglow',
  min: 'processed',
  scenario: 'clean',
  method: 'getTransaction',
  read: 'confirmed',
});

// Returns a complete, valid state. Unknown keys are dropped; invalid values
// fall back to DEFAULT_STATE.
export function normalizeState(input) {
  const state = { ...DEFAULT_STATE };
  if (!input || typeof input !== 'object') return state;
  for (const [key, values] of Object.entries(ENUMS)) {
    if (values.includes(input[key])) state[key] = input[key];
  }
  return state;
}

export function parseHash(hash) {
  const params = new URLSearchParams(String(hash ?? '').replace(/^#/, ''));
  const input = {};
  for (const key of Object.keys(ENUMS)) input[key] = params.get(key);
  return normalizeState(input);
}

// Only non-default values are written so shared links stay short. The default
// state serializes to the empty string.
export function serializeHash(state) {
  const normalized = normalizeState(state);
  const params = new URLSearchParams();
  for (const key of Object.keys(ENUMS)) {
    if (normalized[key] !== DEFAULT_STATE[key]) params.set(key, normalized[key]);
  }
  return params.toString();
}

// Every valid state, for exhaustive tests (2*3*4*2*3 = 144 states).
export function* allStates() {
  for (const era of ENUMS.era)
    for (const min of ENUMS.min)
      for (const scenario of ENUMS.scenario)
        for (const method of ENUMS.method)
          for (const read of ENUMS.read) yield { era, min, scenario, method, read };
}
