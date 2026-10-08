// Pure model of one block's trip through Superbank, for the Alpenglow page.
// No DOM. buildLifecycle(state) returns every step as a full frame, so the
// renderer draws a frame without diffing and tests can check each frame alone.
//
// Slot positions are relative to the featured slot N. Positions the code fixes
// (the 32-slot head window, the 75-slot disk lag) are exact; positions set by
// consensus (when a slot confirms or finalizes) are approximate and say so.
//
// Code facts carry repo refs; upstream facts carry pinned source URLs. The
// numbers in CONSTANTS are re-read from the Rust source by the tests.

import { normalizeState } from './alpenglow-state.js';

export const RANK = Object.freeze({ processed: 0, confirmed: 1, finalized: 2 });
export const meets = (token, level) => token != null && RANK[token] >= RANK[level];

export const CONSTANTS = Object.freeze({
  headRetainSlots: { value: 32, name: 'HEAD_CACHE_RETAIN_SLOTS', ref: 'crates/superbank-rpc/src/config.rs' },
  statusHistoryMinRetainSlots: { value: 256, name: 'STATUS_HISTORY_MIN_HEAD_RETAIN_SLOTS', ref: 'crates/superbank-rpc/src/server.rs' },
  pendingCommitmentSlots: { value: 64, name: 'PENDING_COMMITMENT_SLOTS', ref: 'crates/superbank-rpc/src/head_cache/banks.rs' },
  tipMaxAgeSecs: { value: 1, name: 'TIP_MAX_AGE', ref: 'crates/superbank-rpc/src/head_cache/coverage.rs' },
  backoffStartMs: { value: 250, name: 'reconnect backoff (first)', ref: 'crates/superbank-rpc/src/head_cache/dragonsmouth.rs' },
  backoffMaxSecs: { value: 5, name: 'reconnect backoff (cap)', ref: 'crates/superbank-rpc/src/head_cache/dragonsmouth.rs' },
  diskMinLagSlots: { value: 75, name: 'DISK_CACHE_REPAIR_MIN_LAG_SLOTS', ref: 'crates/superbank-rpc/src/config.rs' },
  footerWaitSecs: { value: 2, name: 'FOOTER_WAIT', ref: 'crates/superbank/src/ingest/grpc.rs' },
  flushIntervalSecs: { value: 5, name: 'FLUSH_INTERVAL_SECS', ref: 'crates/superbank/src/cli.rs' },
});

const AGAVE = 'https://github.com/anza-xyz/agave/blob/v4.3.0/';
const SIMD =
  'https://github.com/solana-foundation/solana-improvement-documents/blob/f1f6c8b05dc205552d3c290a854a438392701107/proposals/';

// Upstream (Agave, SIMDs, Yellowstone) citations. Pinned to a tag, commit or
// crate version so the line anchors keep pointing at the quoted text.
export const SOURCES = Object.freeze({
  simdFinalization: { label: 'SIMD-0326: fast and slow finalization', url: `${SIMD}0326-alpenglow.md?plain=1#L84-L90` },
  simdCertificates: { label: 'SIMD-0326: certificates', url: `${SIMD}0326-alpenglow.md?plain=1#L120-L140` },
  simdTowerFinality: { label: 'SIMD-0326: TowerBFT finality time', url: `${SIMD}0326-alpenglow.md?plain=1#L36-L39` },
  simdUpdateParent: { label: 'SIMD-0337: UpdateParent marker', url: `${SIMD}0337-parent-ready-update-marker.md?plain=1#L185-L190` },
  simdMigration: { label: 'SIMD-0384: Alpenglow migration', url: `${SIMD}0384-alpenglow-migration.md?plain=1#L52-L86` },
  simdFooter: { label: 'SIMD-0307: block footer', url: `${SIMD}0307-add-block-footer.md?plain=1#L120-L124` },
  agaveCertificates: { label: 'Agave votor-messages: certificate thresholds', url: `${AGAVE}votor-messages/src/certificate.rs#L101-L110` },
  agaveCommitment: {
    label: 'Agave commitment service: confirmed, root and finalized set together',
    url: `${AGAVE}core/src/commitment_service.rs#L190-L202`,
  },
  agaveSyntheticOc: { label: 'Agave votor: confirmed notification sent at root', url: `${AGAVE}votor/src/root_utils.rs#L77-L91` },
  agaveRootable: { label: 'Agave votor: when a block can be rooted', url: `${AGAVE}votor/src/event_handler.rs#L935-L946` },
  agaveLeaderWindow: { label: 'Agave: 4 consecutive leader slots', url: `${AGAVE}leader-schedule/src/lib.rs#L20` },
  agaveTowerCommitment: { label: 'Agave runtime: TowerBFT commitment levels', url: `${AGAVE}runtime/src/commitment.rs#L9-L10` },
  voteLockout: {
    label: 'solana-vote-interface: MAX_LOCKOUT_HISTORY = 31',
    url: 'https://docs.rs/solana-vote-interface/6.1.0/src/solana_vote_interface/state/mod.rs.html#37',
  },
  yellowstoneProto: {
    label: 'yellowstone-grpc-proto 14.0.1: geyser.proto (footer, UpdateParent, bank_id)',
    url: 'https://docs.rs/crate/yellowstone-grpc-proto/14.0.1/source/proto/geyser.proto',
  },
  blockMachine: {
    label: 'yellowstone-block-machine 0.11.0-rc2: ForkDetected vs BankDiscarded',
    url: 'https://docs.rs/crate/yellowstone-block-machine/0.11.0-rc2/source/src/state_machine.rs',
  },
});

export const UPSTREAM = Object.freeze({
  leaderWindowSlots: { value: 4, sources: ['agaveLeaderWindow'] },
  notarizationPct: { value: 60, sources: ['simdCertificates', 'agaveCertificates'] },
  skipPct: { value: 60, sources: ['simdCertificates', 'agaveCertificates'] },
  fastFinalizationPct: { value: 80, sources: ['simdFinalization', 'agaveCertificates'] },
  slowFinalizationPct: { value: 60, sources: ['simdFinalization', 'agaveCertificates'] },
  towerConfirmFraction: { value: '2/3', sources: ['agaveTowerCommitment'] },
  // Approximate: 31 lockouts deep, and SIMD-0326's 12.8 s at 400 ms slots.
  towerRootDepthSlots: { value: 32, approx: true, sources: ['voteLockout', 'simdTowerFinality'] },
});

// Every step id buildLifecycle can emit, in reading order.
export const STEP_IDS = Object.freeze([
  'created',
  'staged',
  'frozen',
  'published',
  'held',
  'second-bank',
  'retried',
  'confirmed',
  'finalized',
  'edge',
  'forked',
  'dropped',
  'ingested',
  'evicted',
  'disk',
]);

export const LANES = Object.freeze([
  { id: 'upstream', label: 'Upstream (Agave)' },
  { id: 'stream', label: 'Yellowstone stream' },
  { id: 'head', label: 'Head cache' },
  { id: 'ingest', label: 'Ingestor → ClickHouse' },
  { id: 'disk', label: 'Disk cache' },
  { id: 'probe', label: 'Read probe' },
]);

// The code default (32) is a development size. Production installs keep
// several hundred slots or more, sized to the RAM superbank-rpc has; features
// that join the head and disk tiers need the window to reach the disk cache's
// tip (superbank-rpc warns below 256). Illustrative, not read from code.
export const PRODUCTION_RETAIN_EXAMPLE = 512;

export const retainSlots = (state) => (state.retain === 'default' ? CONSTANTS.headRetainSlots.value : PRODUCTION_RETAIN_EXAMPLE);

const DISK_LAG = CONSTANTS.diskMinLagSlots.value;
const ROOT_DEPTH = UPSTREAM.towerRootDepthSlots.value;

export function slotLabel(rel, approx = false) {
  const base = rel === 0 ? 'N' : rel > 0 ? `N+${rel}` : `N−${-rel}`;
  return approx ? `≈ ${base}` : base;
}

// --- World: mutable while steps are authored, frozen into frames by snap() ---

class World {
  constructor(state) {
    this.state = state;
    this.tower = state.era === 'tower';
    this.retain = retainSlots(state);
    this.tip = 0;
    this.fin = this.tower ? -ROOT_DEPTH : -1;
    this.tipApprox = false;
    this.finApprox = true;
    this.banks = [];
    this.tx = null; // head signature index for the probed transaction: { slot, bank }
    this.stored = {}; // slot -> { bank, footer: 'set' | 'null' }
    this.pending = false; // the ingestor's write of the featured slot is in flight
    this.disk = {}; // slot -> true
    this.dropTip = null; // tip when the head-cache stream dropped (reconnect)
    this.featured = 0; // the slot whose journey the frames follow (N+1 once a retry lands)
    this.frames = [];
  }

  bank(id) {
    return this.banks.find((b) => b.id === id);
  }

  addBank(id, slot, { hasTx = true } = {}) {
    const name = this.tower ? `bank_id = ${slotLabel(slot)}` : `bank_id ${{ A: 7, B: 9, C: 12 }[id]}`;
    const bank = { id, slot, name, status: 'staged', token: null, footer: false, hasTx };
    this.banks.push(bank);
    return bank;
  }

  visible(slot) {
    return this.banks.find((b) => b.slot === slot && b.status === 'visible') ?? null;
  }

  // banks.rs publish_bank: hold below the session minimum; a processed bank
  // cannot replace the bank already shown for its slot.
  publish(bank) {
    if (bank.status === 'discarded' || bank.status === 'cleared') return;
    if (this.dropTip != null) return; // the new session never saw this bank
    if (!meets(bank.token, this.state.min)) {
      bank.status = 'held';
      return;
    }
    const current = this.visible(bank.slot);
    if (current && current !== bank) {
      if (bank.token === 'processed') {
        bank.status = 'held';
        return;
      }
      current.status = 'discarded';
    }
    bank.status = 'visible';
    if (bank.hasTx) this.project(bank);
  }

  // mod.rs projection_wins: a projection moves to another slot only when the
  // new bank is committed (confirmed/finalized) and the old one is processed.
  project(bank) {
    if (!this.tx || this.tx.slot === bank.slot) {
      this.tx = { slot: bank.slot, bank: bank.id };
      return;
    }
    const old = this.bank(this.tx.bank);
    if (!old || old.status !== 'visible' || (RANK[bank.token] > 0 && RANK[old.token] === 0)) {
      this.tx = { slot: bank.slot, bank: bank.id };
    }
  }

  // banks.rs commit_bank: a confirmed/finalized status names the winner and
  // discards every other bank of that slot.
  commit(bank, token) {
    for (const other of this.banks) {
      if (other.slot === bank.slot && other !== bank && other.status !== 'cleared') other.status = 'discarded';
    }
    if (this.tx && this.bank(this.tx.bank)?.status === 'discarded') this.tx = null;
    bank.token = token;
    this.publish(bank);
    // A visible winner re-projects its transactions (retry reclaim).
    if (bank.status === 'visible' && bank.hasTx) this.project(bank);
  }

  discardSlot(slot) {
    for (const b of this.banks) if (b.slot === slot && b.status !== 'cleared') b.status = 'discarded';
    if (this.tx?.slot === slot) this.tx = null;
  }

  // The newest slot the head cache has published: the window counts back from it.
  headHi() {
    const { min } = this.state;
    if (min === 'processed') return this.tip;
    if (min === 'confirmed' && this.tower) return this.tip - 1;
    return this.fin;
  }

  headWindow() {
    const hi = this.headHi();
    let lo = hi - (this.retain - 1);
    if (this.dropTip != null) lo = Math.max(lo, this.dropTip + 1);
    return { lo, hi };
  }

  // mod.rs evict_old_slots / min_retained_slot = latest − (retain − 1).
  evict() {
    const { lo } = this.headWindow();
    for (const b of this.banks) if (b.status === 'visible' && b.slot < lo) b.status = 'evicted';
    if (this.tx && this.tx.slot < lo) this.tx = null;
  }

  ingest(slot) {
    const bank = this.banks.find((b) => b.slot === slot && b.token === 'finalized');
    this.stored[slot] = { bank: bank.id, footer: this.tower ? 'null' : 'set' };
    this.pending = false;
  }

  at(tip, fin, { tipApprox = false, finApprox = false } = {}) {
    this.tip = tip;
    this.fin = fin;
    this.tipApprox = tipApprox;
    this.finApprox = finApprox;
  }

  snap(id, title, summary, { upstream = null, stream = [], note = null } = {}) {
    this.evict();
    this.frames.push(
      structuredClone({
        id,
        title,
        summary,
        upstream,
        stream,
        note,
        tip: this.tip,
        fin: this.fin,
        tipLabel: slotLabel(this.tip, this.tipApprox),
        finLabel: slotLabel(this.fin, this.finApprox),
        banks: this.banks,
        headWindow: this.headWindow(),
        headCleared: this.dropTip != null,
        retain: this.retain,
        featured: this.featured,
        tx: this.tx,
        stored: this.stored,
        pending: this.pending,
        disk: this.disk,
      }),
    );
  }
}

// --- Step authoring ---------------------------------------------------------

function intro(w) {
  const tower = w.tower;
  const a = w.addBank('A', 0);
  w.snap(
    'created',
    'Bank created',
    tower
      ? 'Yellowstone reports `CreatedBank` for slot N without a bank ID, so Superbank uses `bank_id = slot`.'
      : 'Yellowstone reports `CreatedBank` for slot N with a node-local `bank_id`, and the head cache opens a buffer for `(slot, bank_id)`.',
    {
      upstream: tower ? 'The leader for slot N starts its block.' : 'The leader starts slot N, one of its 4 consecutive leader slots.',
      stream: [tower ? 'CreatedBank · N (no bank_id)' : 'CreatedBank · N · bank_id 7'],
    },
  );
  if (!tower) a.footer = true;
  w.snap(
    'staged',
    'Staged per bank',
    tower
      ? 'Transactions, entries and block metadata are buffered for the bank. Tower blocks have no footer.'
      : 'Transactions, entries, block metadata and the footer are buffered for this bank. The footer can come before or after the block freezes; the first one per bank wins.',
    {
      upstream: 'Replay executes the block’s entries.',
      stream: tower ? ['Transaction ×n', 'Entry ×m', 'BlockMeta'] : ['Transaction ×n', 'Entry ×m', 'BlockMeta', 'BlockFooter'],
    },
  );
  a.status = 'frozen';
  a.token = 'processed';
  w.snap(
    'frozen',
    'Frozen and checked',
    'The bank froze. The head cache seals it only if the blockhash matches and the transaction and entry counts, indexes and entry ranges are complete. Its token is `processed`.',
    { upstream: 'The bank freezes, and geyser reports `SLOT_PROCESSED`.', stream: ['FrozenBlock', 'SlotStatus · processed'] },
  );
  w.publish(a);
  if (a.status === 'visible') {
    w.snap(
      'published',
      'Published to the head cache',
      'The token meets `HEAD_CACHE_MIN_COMMITMENT=processed`, so the bank is published: `processed` readers can see it.',
    );
  } else {
    w.snap(
      'held',
      'Held below the minimum',
      `The bank is complete but its token is below \`HEAD_CACHE_MIN_COMMITMENT=${w.state.min}\`, so nothing is published yet, not even to \`processed\` readers. A minimum above processed keeps buffered banks ${CONSTANTS.pendingCommitmentSlots.value} extra slots while they wait.`,
    );
  }
  return a;
}

function secondBank(w, { hasTx }) {
  const b = w.addBank('B', 0, { hasTx });
  b.footer = true;
  b.status = 'frozen';
  b.token = 'processed';
  w.publish(b);
  w.snap(
    'second-bank',
    'A second bank for slot N',
    w.state.min === 'processed'
      ? 'Another bank for slot N freezes (bank_id 9). A `processed` bank cannot replace the one already shown, so bank_id 9 waits in the buffer and `processed` readers still see bank_id 7.'
      : 'Another bank for slot N freezes (bank_id 9). Both banks are held: neither has reached the minimum.',
    {
      upstream: 'Slot N has two versions, for example a duplicate block that replay switches to the version the cluster votes for.',
      stream: ['CreatedBank · N · bank_id 9', 'FrozenBlock', 'SlotStatus · processed'],
    },
  );
  return b;
}

function retried(w, parentNote) {
  const c = w.addBank('C', 1);
  if (!w.tower) c.footer = true;
  c.status = 'frozen';
  c.token = 'processed';
  w.featured = 1;
  w.at(1, w.fin, { finApprox: true });
  w.publish(c);
  w.snap(
    'retried',
    'Transaction retried in N+1',
    w.tx?.slot === 0
      ? 'The client retries the transaction and it lands in slot N+1. The head cache keeps pointing at slot N: a `processed` bank cannot take a signature over from another `processed` slot.'
      : 'The client retries the transaction and it lands in slot N+1, which is still held below the minimum.',
    { upstream: parentNote, stream: [w.tower ? 'CreatedBank · N+1' : 'CreatedBank · N+1 · bank_id 12', 'FrozenBlock', 'SlotStatus · processed'] },
  );
  return c;
}

function alpenglowFinalize(w, winners) {
  const { min } = w.state;
  const retry = winners.length > 1;
  w.at(retry ? 2 : 1, retry ? 1 : 0, { tipApprox: true });
  for (const bank of winners) w.commit(bank, 'finalized');
  w.pending = true;
  const named = winners.map((b) => `${slotLabel(b.slot)} · ${b.name}`).join(', ');
  const discarded = w.banks.filter((b) => b.status === 'discarded');
  let head;
  if (w.dropTip != null) head = ' The new head-cache session never saw slot N, so it has nothing to mark.';
  else {
    head = ` The head cache marks it canonical${discarded.length ? ` and discards ${discarded.map((b) => b.name).join(', ')}` : ''}${
      min === 'processed' ? '' : `, and publishes it now that it meets \`${min}\``
    }.`;
    if (retry && w.tx?.slot === 1) head += ' The retried transaction now resolves to slot N+1.';
  }
  w.snap(
    'finalized',
    'Finalized (confirmed at the same moment)',
    `Votor roots the block, and Yellowstone reports \`confirmed\` and \`finalized\` together for ${named}.${head}`,
    {
      upstream:
        'A fast-finalization (80% notarize) or slow-finalization (60% finalize, plus notarization) certificate lets votor root the block.',
      stream: [
        ...winners.flatMap((b) => [`SlotStatus · confirmed · ${slotLabel(b.slot)}`, `SlotStatus · finalized · ${slotLabel(b.slot)}`]),
        ...discarded.map((b) => `BankDiscarded · ${b.name}`),
      ],
    },
  );
}

function ingested(w, slots) {
  for (const slot of slots) w.ingest(slot);
  w.at(w.tip + 1, w.fin + 1, { tipApprox: true, finApprox: true });
  w.snap(
    'ingested',
    'Written to ClickHouse',
    w.tower
      ? 'The ingestor’s own finalized subscription delivers the block. It writes `transactions`, then `blocks_metadata` (footer columns `NULL`), then `entries`.'
      : `The ingestor’s own finalized subscription delivers the block and its footer. It waits up to ${CONSTANTS.footerWaitSecs.value} s for the footer, then writes \`transactions\`, then \`blocks_metadata\` with \`bank_id\`, \`bank_hash\` and the footer fields, then \`entries\`.`,
    {
      stream: ['(ingestor) finalized block'],
      note: `Writes land on the next flush: every \`FLUSH_INTERVAL_SECS\` (${CONSTANTS.flushIntervalSecs.value} s) or sooner when a row threshold fills.`,
    },
  );
}

// Where the tip and finalized tip stand when `slot` leaves the head window:
// the newest published slot reaches slot + retain.
function evictPosition(w, slot) {
  const { min } = w.state;
  const edge = slot + w.retain;
  if (min === 'processed') return [edge, edge - (w.tower ? ROOT_DEPTH : 1), { finApprox: true }];
  if (min === 'confirmed' && w.tower) return [edge + 1, edge + 1 - ROOT_DEPTH, { tipApprox: true, finApprox: true }];
  return [edge + (w.tower ? ROOT_DEPTH : 1), edge, { tipApprox: true }];
}

// ...and when the disk cache copies it: the finalized tip is DISK_LAG past it.
const diskPosition = (w, slot) => [DISK_LAG + slot + (w.tower ? ROOT_DEPTH : 1), DISK_LAG + slot, { tipApprox: true }];

function evicted(w, slot) {
  const { min } = w.state;
  w.at(...evictPosition(w, slot));
  const counted = min === 'processed' ? 'processed' : min === 'confirmed' && w.tower ? 'confirmed' : 'finalized';
  const next = w.disk[slot] ? 'the disk cache' : 'ClickHouse';
  w.snap(
    'evicted',
    'Dropped from the head cache',
    `The head cache keeps the newest ${w.retain} slots it has published, counted from its newest \`${counted}\` slot. Slot ${slotLabel(slot)} falls out of that window, and reads move to ${next}.`,
    { note: w.disk[slot] ? null : 'This assumes the ingestor has flushed the slot by now, which it normally has.' },
  );
}

function copiedToDisk(w, slot) {
  w.disk[slot] = true;
  w.at(...diskPosition(w, slot));
  const stillHead = w.dropTip == null && w.visible(slot) && w.headWindow().lo <= slot;
  w.snap(
    'disk',
    'Copied to the disk cache',
    `Once the source ClickHouse finalized tip is at least ${DISK_LAG} slots past the block, the disk cache filler copies its rows from ClickHouse (never from the head cache), checks the transaction counts and only then marks the range covered.${
      stillHead ? ' The head cache still holds the slot, so reads keep going there first.' : ''
    }`,
  );
}

// Eviction and the disk copy, in the order their slot positions put them in.
function tail(w, slot, { evict = true } = {}) {
  const events = [['disk', diskPosition(w, slot)[0]]];
  if (evict) events.push(['evict', evictPosition(w, slot)[0]]);
  events.sort((a, b) => a[1] - b[1]);
  for (const [kind] of events) (kind === 'disk' ? copiedToDisk : evicted)(w, slot);
}

function towerConfirm(w, bank) {
  w.at(bank.slot + 1, w.fin + 1, { tipApprox: true, finApprox: true });
  const before = w.tx?.slot;
  w.commit(bank, 'confirmed');
  let head;
  if (bank.status === 'visible') {
    head = ' The head cache can now answer `confirmed` reads from it.';
    if (before != null && before !== bank.slot && w.tx?.slot === bank.slot) {
      head += ` A committed bank takes the retried transaction over from the \`processed\` slot N, so it now resolves to ${slotLabel(bank.slot)}.`;
    }
  } else if (bank.status === 'cleared') head = ' The new head-cache session never saw this slot.';
  else head = ' It is still below the minimum and stays held.';
  w.snap(
    'confirmed',
    'Optimistically confirmed',
    `More than 2/3 of stake voted for ${slotLabel(bank.slot)}, so Yellowstone reports \`confirmed\`.${head}`,
    { upstream: 'Votes from more than 2/3 of stake: optimistic confirmation.', stream: [`SlotStatus · confirmed · ${slotLabel(bank.slot)}`] },
  );
}

// Tower: the root lands about where the slot leaves a window counted from the
// processed (or confirmed) tip. Which comes first is not fixed, so this frame
// shows both done and the ingestor's write still in flight.
const atTowerEdge = (w) => w.tower && w.state.min !== 'finalized' && w.dropTip == null && w.retain <= ROOT_DEPTH;

function towerRoot(w, bank) {
  const atEdge = atTowerEdge(w);
  const wasVisible = bank.status === 'visible';
  w.at(bank.slot + ROOT_DEPTH, bank.slot, { tipApprox: true });
  w.commit(bank, 'finalized');
  w.pending = true;
  if (atEdge) {
    w.snap(
      'edge',
      'Rooted at the edge of the head window',
      `Slot ${slotLabel(bank.slot)} reaches max lockout and is rooted about ${ROOT_DEPTH} slots behind the tip, which is where it leaves the ${w.retain}-slot head window. Whichever happens first, the head cache serves it as \`finalized\` for at most a moment, and the ingestor writes it within seconds.`,
      {
        upstream: 'The block reaches maximum vote lockout and becomes the root.',
        stream: [`SlotStatus · finalized · ${slotLabel(bank.slot)}`],
        note: 'Reads for this slot can briefly miss both the head cache and ClickHouse here.',
      },
    );
  } else {
    w.snap(
      'finalized',
      'Rooted (finalized)',
      w.dropTip != null
        ? `Slot ${slotLabel(bank.slot)} is rooted about ${ROOT_DEPTH} slots behind the tip. The new head-cache session never saw it.`
        : wasVisible
          ? `Slot ${slotLabel(bank.slot)} is rooted about ${ROOT_DEPTH} slots behind the tip, well inside the ${w.retain}-slot head window, so the head cache now answers \`finalized\` reads for it too.`
          : `Slot ${slotLabel(bank.slot)} is rooted about ${ROOT_DEPTH} slots behind the tip and is published now that it meets \`finalized\`. The window counts from the newest finalized slot, so it stays for about ${w.retain} more slots.`,
      {
        upstream: 'The block reaches maximum vote lockout and becomes the root.',
        stream: [`SlotStatus · finalized · ${slotLabel(bank.slot)}`],
      },
    );
  }
}

function dropped(w) {
  w.at(1, w.fin, { finApprox: true });
  w.dropTip = w.tip;
  for (const b of w.banks) if (b.status !== 'discarded') b.status = 'cleared';
  w.tx = null;
  w.snap(
    'dropped',
    'Head-cache stream reconnects',
    `The Dragon’s Mouth stream drops. The head cache retries with backoff (${CONSTANTS.backoffStartMs.value} ms, doubling to ${CONSTANTS.backoffMaxSecs.value} s) and starts a new session that clears every cached slot, bank and proof. The new subscription starts at the tip and does not replay slot N.`,
    { stream: ['stream error → resubscribe'], note: 'Until ingestion passes the slots the head held, tip methods regress to the finalized tip.' },
  );
}

function buildSteps(state) {
  const w = new World(state);
  const { scenario } = state;

  if (scenario === 'fork' && w.tower) {
    intro(w);
    w.at(3, w.fin + 3, { tipApprox: true, finApprox: true });
    w.discardSlot(0);
    w.snap(
      'forked',
      'Slot dropped from the canonical chain',
      'The cluster builds on a different fork. The block machine reports `ForkDetected` for slot N, and the head cache removes the whole slot: with `bank_id = slot` there is only one bank to drop. Slot N is never finalized, so the ingestor never writes it.',
      { upstream: 'Slot N is not an ancestor of the chain being rooted.', stream: ['ForkDetected · N'] },
    );
    return w.frames;
  }

  if (scenario === 'retry' && w.tower) {
    intro(w);
    const c = retried(w, 'Slot N+1 builds on N−1: slot N is on a minority fork.');
    towerConfirm(w, c);
    w.at(3, w.fin + 1, { tipApprox: true, finApprox: true });
    w.discardSlot(0);
    w.snap(
      'forked',
      'Slot N dropped from the canonical chain',
      'The block machine reports `ForkDetected` for slot N, and the head cache removes it. The retried transaction lives on in N+1.',
      { upstream: 'Slot N is not an ancestor of the chain being rooted.', stream: ['ForkDetected · N'] },
    );
    towerRoot(w, c);
    const edge = atTowerEdge(w);
    ingested(w, [1]);
    tail(w, 1, { evict: !edge });
    return w.frames;
  }

  const a = intro(w);

  if (scenario === 'reconnect') {
    dropped(w);
    if (w.tower) {
      towerConfirm(w, a);
      towerRoot(w, a);
    } else {
      alpenglowFinalize(w, [a]);
    }
    ingested(w, [0]);
    copiedToDisk(w, 0);
    return w.frames;
  }

  if (w.tower) {
    // clean
    towerConfirm(w, a);
    towerRoot(w, a);
    const edge = atTowerEdge(w);
    ingested(w, [0]);
    tail(w, 0, { evict: !edge });
    return w.frames;
  }

  // Alpenglow
  if (scenario === 'clean') {
    alpenglowFinalize(w, [a]);
  } else {
    // In the retry scenario the transaction landed only on bank_id 7.
    const b = secondBank(w, { hasTx: scenario !== 'retry' });
    if (scenario === 'retry') {
      const c = retried(w, 'Slot N+1 builds on bank_id 9’s version of slot N.');
      alpenglowFinalize(w, [b, c]);
    } else {
      alpenglowFinalize(w, [b]);
    }
  }
  const featured = scenario === 'retry' ? 1 : 0;
  ingested(w, scenario === 'retry' ? [0, 1] : [0]);
  tail(w, featured);
  return w.frames;
}

// --- Read probe ---------------------------------------------------------------

// Which tier answers getBlock(N) or getTransaction(T) at `read` commitment in
// this frame. Mirrors the handler order: head cache, then the disk cache, then
// ClickHouse; the last two hold finalized data only and answer `confirmed`
// requests with it.
export function answerRead(frame, method, read) {
  if (method === 'getBlock' && read === 'processed') {
    return { tier: null, error: '-32602', text: '`getBlock` rejects `processed`, even with the head cache on.' };
  }
  const bankOf = (id) => frame.banks.find((b) => b.id === id);
  let headBank = null;
  let slot = 0;
  if (method === 'getBlock') {
    headBank = frame.banks.find((b) => b.slot === 0 && b.status === 'visible') ?? null;
  } else if (frame.tx) {
    headBank = bankOf(frame.tx.bank);
    slot = frame.tx.slot;
  }
  if (headBank && meets(headBank.token, read)) {
    return { tier: 'head', slot, bank: headBank.name, token: headBank.token, footer: method === 'getBlock' ? headBank.footer : undefined };
  }

  // Finalized tiers: where does the target live in ClickHouse?
  let storedSlot = null;
  if (method === 'getBlock') {
    if (frame.stored[0]) storedSlot = 0;
  } else {
    for (const [s, row] of Object.entries(frame.stored)) if (bankOf(row.bank)?.hasTx) storedSlot = Number(s);
  }
  if (storedSlot != null) {
    const row = frame.stored[storedSlot];
    const tier = frame.disk[storedSlot] ? 'disk' : 'clickhouse';
    return {
      tier,
      slot: storedSlot,
      bank: bankOf(row.bank).name,
      token: 'finalized',
      footer: method === 'getBlock' ? row.footer === 'set' : undefined,
    };
  }
  if (frame.pending) return { tier: null, pending: true, text: 'Not in the head cache, and the ingestor’s write has not landed yet.' };
  return { tier: null, text: method === 'getBlock' ? 'Not available yet at this commitment.' : 'Not found at this commitment yet (`null`).' };
}

// --- Public entry point -----------------------------------------------------

export function buildLifecycle(input) {
  const state = normalizeState(input);
  const steps = buildSteps(state);
  return {
    state,
    steps,
    summary: steps.map((step) => ({ id: step.id, title: step.title, text: step.summary })),
  };
}
