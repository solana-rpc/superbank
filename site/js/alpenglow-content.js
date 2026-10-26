// Copy for the Alpenglow page's info drawer. Code claims were checked against
// the Rust source they cite; upstream claims cite pinned SIMD, Agave and
// Yellowstone sources (SOURCES in alpenglow-model.js).
//
// Plain text only: `code` spans use backticks and callers render with
// textContent. Refs are repo-relative paths with no line numbers (they drift).

import { normalizeState } from './alpenglow-state.js';
import { CONSTANTS, LANES, PRODUCTION_RETAIN_EXAMPLE, SOURCES, STEP_IDS, UPSTREAM, retainSlots } from './alpenglow-model.js';

export const REPO_BLOB = 'https://github.com/solana-rpc/superbank/blob/main/';

const REFS = {
  compat: 'docs/agave-4.3-compatibility.md',
  rpcReadme: 'crates/superbank-rpc/README.md',
  rpcConfig: 'crates/superbank-rpc/src/config.rs',
  rpcServer: 'crates/superbank-rpc/src/server.rs',
  headBanks: 'crates/superbank-rpc/src/head_cache/banks.rs',
  headMod: 'crates/superbank-rpc/src/head_cache/mod.rs',
  headStream: 'crates/superbank-rpc/src/head_cache/dragonsmouth.rs',
  headProtocol: 'crates/superbank-rpc/src/head_cache/protocol.rs',
  headCoverage: 'crates/superbank-rpc/src/head_cache/coverage.rs',
  diskFiller: 'crates/superbank-rpc/src/disk_cache/filler.rs',
  blocks: 'crates/superbank-rpc/src/handlers/blocks.rs',
  transactions: 'crates/superbank-rpc/src/handlers/transactions.rs',
  signatures: 'crates/superbank-rpc/src/handlers/signatures.rs',
  ingestReadme: 'crates/superbank/README.md',
  ingestCli: 'crates/superbank/src/cli.rs',
  ingestGrpc: 'crates/superbank/src/ingest/grpc.rs',
  ingestFumarole: 'crates/superbank/src/ingest/fumarole.rs',
  ingestCommitment: 'crates/superbank/src/commitment.rs',
  ingestWriter: 'crates/superbank/src/clickhouse.rs',
  blocksMetadata: 'ddl/local/blocks_metadata.sql',
  jetstreamerReadme: 'ingest/jetstreamer-clickhouse-plugin/README.md',
};

export function allRefs() {
  return [...new Set(Object.values(REFS))];
}

export function allSources() {
  return Object.keys(SOURCES);
}

const cfg = (key, value, note) => (note ? { key, value, note } : { key, value });
const C = (key) => CONSTANTS[key].value;
const U = (key) => UPSTREAM[key].value;

// --- Lanes --------------------------------------------------------------------

const LANE = {
  upstream: (s) =>
    s.era === 'alpenglow'
      ? {
          title: 'Upstream: Alpenglow (Agave)',
          subtitle: 'Votes and certificates decide commitment before Superbank sees anything.',
          body: [
            `Each leader produces ${U('leaderWindowSlots')} consecutive slots. Validators vote to notarize (or skip) each block, and those votes aggregate into certificates.`,
            `A block is finalized by a fast-finalization certificate (${U('fastFinalizationPct')}% of stake notarizing) or by a slow-finalization certificate (${U('slowFinalizationPct')}% voting finalize) together with a notarization certificate.`,
            'Once a block is frozen, this node has voted on it and it holds a finalization certificate, votor roots it. Agave then sets `confirmed`, the root and `finalized` together: there is no separate optimistic-confirmation step, and a notarization certificate on its own does not make a block `confirmed`.',
            'Superbank never sees votes or certificates. Yellowstone can attach the final and skip-reward certificates to a block footer when a filter asks for them; every Superbank subscription sets `include_certificates: false`.',
          ],
          config: [
            cfg('Notarization', `${U('notarizationPct')}% notarize`),
            cfg('Skip', `${U('skipPct')}% skip`),
            cfg('Fast finalization', `${U('fastFinalizationPct')}% notarize`, 'one round'),
            cfg('Slow finalization', `${U('slowFinalizationPct')}% finalize`, 'plus a notarization certificate'),
          ],
          refs: [REFS.headStream, REFS.ingestGrpc],
          sources: ['simdCertificates', 'simdFinalization', 'agaveCertificates', 'agaveRootable', 'agaveCommitment', 'agaveSyntheticOc', 'agaveLeaderWindow', 'yellowstoneProto'],
        }
      : {
          title: 'Upstream: Tower BFT (Agave)',
          subtitle: 'What mainnet runs until Alpenglow activates.',
          body: [
            `A slot is \`confirmed\` once more than ${U('towerConfirmFraction')} of stake has voted for it (optimistic confirmation).`,
            `It is \`finalized\` when it becomes the root, at maximum vote lockout (the vote tower holds 31 votes). That puts the finalized tip roughly ${U('towerRootDepthSlots')} slots behind the processed tip. SIMD-0326 puts Tower BFT finality at 12.8 seconds, which is 32 slots of 400 ms.`,
          ],
          config: [cfg('Confirmed', `more than ${U('towerConfirmFraction')} of stake`), cfg('Finalized', 'root at max lockout', `≈ ${U('towerRootDepthSlots')} slots behind the tip`)],
          refs: [REFS.headStream],
          sources: ['agaveTowerCommitment', 'voteLockout', 'simdTowerFinality'],
        },

  stream: (s) => ({
    title: 'Yellowstone stream',
    subtitle: 'One Dragon’s Mouth subscription at `processed` feeds the head cache.',
    body: [
      'The head cache subscribes to transactions, entries, block metadata, slot statuses (including the inter-slot ones) and sysvar accounts on a single subscription, so every piece of evidence about a bank comes from the same session.',
      s.era === 'alpenglow'
        ? 'Alpenglow streams tag every update with a node-local `bank_id`, so two versions of one slot stay apart. They also carry the block footer, and `EntryUpdateParent` names a `cleared_bank_id` when a leader switches parent mid-block (the `UpdateParent` marker); the head cache discards that bank.'
        : 'A Tower stream sends `CreatedBank` without a bank ID. The head cache then treats the stream as legacy and uses `bank_id = slot`, so a slot has one bank. A footer or `EntryUpdateParent` on such a stream is a protocol error.',
      'The block machine turns slot statuses into events: `ForkDetected` when a slot falls out of the chain being rooted, `BankDiscarded` when a bank loses its slot to a sibling, `DeadBlockDetected` for a dead slot. Each one discards cached data.',
      'Bank IDs only mean something inside one subscription. Nothing carries them across a reconnect, and the ingestor’s separate subscription has its own.',
    ],
    refs: [REFS.headStream, REFS.headProtocol, REFS.compat],
    sources: s.era === 'alpenglow' ? ['yellowstoneProto', 'blockMachine', 'simdUpdateParent', 'simdFooter'] : ['yellowstoneProto', 'blockMachine'],
  }),

  head: (s) => ({
    title: 'Head cache',
    subtitle: 'In-memory, speculative: the only tier that holds data below `finalized`.',
    body: [
      'Banks are buffered per `(slot, bank_id)` and sealed only when complete. A frozen bank is published once its commitment token reaches `HEAD_CACHE_MIN_COMMITMENT`, and that gate applies to concurrent `processed` readers too.',
      'A `processed` bank never replaces the bank already shown for its slot. A `confirmed` or `finalized` status names the winner: the head cache shows it, drops the losers and re-indexes signatures that first landed on an abandoned bank.',
      `The window is the newest \`HEAD_CACHE_RETAIN_SLOTS\` slots it has published, counted from its newest published slot. With a minimum above \`processed\`, that is the newest \`confirmed\` or \`finalized\` slot, and buffered banks are kept ${C('pendingCommitmentSlots')} extra slots while they wait.`,
      `The code default of ${C('headRetainSlots')} slots is a development size. Production installations keep several hundred slots or more, sized to the RAM available to \`superbank-rpc\` (this page uses ${PRODUCTION_RETAIN_EXAMPLE} as an example). Features that combine the head and disk tiers need the window to reach down to the disk cache’s tip, and \`superbank-rpc\` warns at startup when it is below ${C('statusHistoryMinRetainSlots')}.`,
      s.era === 'alpenglow'
        ? 'Under Alpenglow a slot is finalized soon after it freezes, so for almost the whole window the head cache holds finalized data, and a `confirmed` minimum behaves exactly like `finalized`.'
        : retainSlots(s) <= U('towerRootDepthSlots')
          ? `Under Tower BFT a slot is rooted about ${U('towerRootDepthSlots')} slots behind the tip, which is where it leaves a ${retainSlots(s)}-slot window counted from the processed tip. With the default minimum the head cache serves \`processed\` and \`confirmed\` reads, and \`finalized\` ones for at most a moment.`
          : `Under Tower BFT a slot is rooted about ${U('towerRootDepthSlots')} slots behind the tip, well inside a ${retainSlots(s)}-slot window, so the head cache serves it at every commitment until it ages out.`,
      `A reconnect starts a new session that clears every slot, bank and proof (backoff ${C('backoffStartMs')} ms doubling to ${C('backoffMaxSecs')} s). The latest-slot tip is trusted only if it advanced within ${C('tipMaxAgeSecs')} s.`,
    ],
    config: [
      cfg('HEAD_CACHE_ENABLED', 'false', 'needs `--features grpc-head-cache`'),
      cfg('HEAD_CACHE_RETAIN_SLOTS', String(C('headRetainSlots')), `code default; this page: ${retainSlots(s)}`),
      cfg('HEAD_CACHE_MIN_COMMITMENT', 'processed', `this page: \`${s.min}\``),
    ],
    refs: [REFS.headBanks, REFS.headMod, REFS.headCoverage, REFS.rpcConfig, REFS.rpcServer, REFS.rpcReadme],
    sources: [],
  }),

  ingest: (s) => ({
    title: 'Primary ingestor → ClickHouse',
    subtitle: 'The source of truth: finalized blocks only.',
    body: [
      '`superbank` reads its own stream (Yellowstone gRPC or Fumarole), separate from the head cache. Durable writers reject any `commitment` other than `finalized`, so forks and losing banks never reach ClickHouse; a second finalized bank for the same slot is a hard error.',
      s.era === 'alpenglow'
        ? `Footers arrive on the same subscription. Only the finalized winner’s footer is used: an identified block waits up to ${C('footerWaitSecs')} s for it, then \`blocks_metadata\` gets \`bank_id\`, \`bank_hash\`, \`block_producer_time_nanos\` and \`block_user_agent\`. A footer that misses the wait leaves those columns \`NULL\` for good. Footer problems never stop block ingestion.`
        : 'Tower blocks have no footer, so the footer columns of `blocks_metadata` stay `NULL`.',
      `Rows are batched: \`transactions\`, then \`blocks_metadata\`, then \`entries\`, flushed every \`FLUSH_INTERVAL_SECS\` (${C('flushIntervalSecs')} s) or sooner when a row threshold fills.`,
      'Live sources after activation are Yellowstone gRPC and Fumarole without a historical bound (it needs a server with the `block_footer` filter, or footers stay `NULL`). Jetstreamer, and Fumarole with a bound, stop at the Alpenglow genesis slot or an attested preactivation slot.',
    ],
    config: [
      cfg('commitment', 'finalized', '`DRAGONSMOUTH_COMMITMENT`; nothing else is accepted'),
      cfg('FLUSH_INTERVAL_SECS', String(C('flushIntervalSecs'))),
    ],
    refs: [REFS.ingestCommitment, REFS.ingestGrpc, REFS.ingestWriter, REFS.ingestCli, REFS.blocksMetadata, REFS.ingestFumarole, REFS.jetstreamerReadme, REFS.ingestReadme],
    sources: s.era === 'alpenglow' ? ['simdFooter', 'simdMigration'] : ['simdMigration'],
  }),

  disk: (s) => ({
    title: 'Disk cache',
    subtitle: 'A loopback ClickHouse holding recent finalized slots, copied from the source ClickHouse.',
    body: [
      `The filler copies finalized rows from the source ClickHouse, never from the head cache, and stays at least ${C('diskMinLagSlots')} slots behind the source’s finalized tip. A range counts as covered only after its transaction counts check out.`,
      s.retain === 'default'
        ? `With the ${C('headRetainSlots')}-slot default head window, the head cache drops a slot before the disk cache copies it, so ClickHouse answers in between.`
        : 'With a production-sized head window the two tiers overlap: the head cache still holds a slot when the disk cache copies it and keeps answering first until the slot ages out, then the disk cache takes over.',
      'It mirrors the source table’s columns, footer columns included, so it can answer `getBlock` with `footer: true` once those columns exist in the source.',
      'It is a near cache, never a source of truth: misses, holes and errors fall through to the source ClickHouse.',
    ],
    config: [
      cfg('DISK_CACHE_ENABLED', 'false', 'needs `--features disk-cache`'),
      cfg('DISK_CACHE_REPAIR_MIN_LAG_SLOTS', String(C('diskMinLagSlots'))),
      cfg('DISK_CACHE_RETAIN_SLOTS', 'required', 'no default'),
    ],
    refs: [REFS.diskFiller, REFS.rpcConfig, REFS.rpcServer, REFS.rpcReadme],
    sources: [],
  }),

  probe: (s) => ({
    title: 'Read probe',
    subtitle: `\`${s.method}\` at \`${s.read}\`, asked of each tier in turn.`,
    body: [
      'Handlers try the head cache, then the disk cache, then the source ClickHouse, falling through on a miss. The disk cache and ClickHouse hold finalized data only and use it for `confirmed` requests too.',
      '`processed` is accepted only with the head cache on, and only by a subset of methods. `getBlock` always rejects it with `-32602`.',
      '`getBlock` can also be answered first by an in-process response cache (`GET_BLOCK_RESPONSE_CACHE_MAX_BYTES`, off by default). It stores `finalized` responses only, and not head-cache responses that carry a footer, because the footer can still arrive later.',
      s.era === 'alpenglow'
        ? '`getBlock` with `footer: true` returns `blockProducerTimeNanos` and `blockUserAgent`, from any tier. A transaction’s `confirmationStatus` comes from its own bank’s token, not the slot’s.'
        : '`getBlock` with `footer: true` returns `footer: null` for Tower slots.',
    ],
    refs: [REFS.blocks, REFS.transactions, REFS.signatures, REFS.rpcReadme],
    sources: s.era === 'alpenglow' ? ['simdFooter'] : [],
  }),
};

// --- Steps --------------------------------------------------------------------

const STEP = {
  created: (s) => ({
    body: [
      s.era === 'alpenglow'
        ? 'The bank ID is node-local: a different validator, or the ingestor’s own subscription, numbers the same block differently. The head cache holds updates that arrive before `CreatedBank` (up to 1024 events) so it can tell an Alpenglow stream from a legacy one.'
        : 'Superbank recognises a legacy stream by a `CreatedBank` with no bank ID, and from then on keys each bank by its slot.',
    ],
    refs: [REFS.headProtocol, REFS.headBanks],
    sources: s.era === 'alpenglow' ? ['yellowstoneProto'] : [],
  }),
  staged: (s) => ({
    body: [
      'Nothing is visible yet. The buffer belongs to one bank of one subscription session, and updates for banks that were already discarded are refused.',
      s.era === 'alpenglow'
        ? 'The footer (SIMD-0307) is streamed on its own and joined on `(slot, bank_id)`. It carries the bank hash, the producer’s timestamp and its user agent.'
        : 'There is no footer before Alpenglow.',
    ],
    refs: [REFS.headBanks, REFS.headStream],
    sources: s.era === 'alpenglow' ? ['simdFooter', 'yellowstoneProto'] : [],
  }),
  frozen: () => ({
    body: [
      'Geyser reports `processed` when the bank freezes. It is a single node’s view: another bank for the same slot can still win.',
      'A bank that fails the completeness check is never published; the rejection is logged with the expected and received transaction counts.',
    ],
    refs: [REFS.headBanks],
    sources: [],
  }),
  published: () => ({
    body: [
      'From here `getTransaction`, `getSignatureStatuses` and the other head-aware methods can answer `processed` requests for this block. `getBlock` still needs `confirmed` or `finalized`.',
    ],
    refs: [REFS.headBanks, REFS.transactions],
    sources: [],
  }),
  held: (s) => ({
    body: [
      `With \`HEAD_CACHE_MIN_COMMITMENT=${s.min}\`, frozen banks wait for their status before anything is exposed. The buffer floor is widened by ${C('pendingCommitmentSlots')} slots so a bank survives the wait.`,
      s.era === 'alpenglow'
        ? 'Under Alpenglow the wait is short, and `confirmed` and `finalized` publish at the same moment.'
        : s.min === 'finalized'
          ? `Under Tower BFT the wait is about ${U('towerRootDepthSlots')} slots; the extra ${C('pendingCommitmentSlots')} slots of buffer keep the bank alive through it.`
          : 'Under Tower BFT optimistic confirmation follows shortly.',
    ],
    refs: [REFS.headBanks, REFS.rpcConfig],
    sources: [],
  }),
  'second-bank': () => ({
    body: [
      'Yellowstone gives every version of a slot its own bank buffer. A `processed` sighting never decides between them: only a `confirmed` or `finalized` status does, and at most one bank per slot can reach it.',
      'The same discard path also handles `EntryUpdateParent`: when a leader switches parent mid-block (SIMD-0337), the stream names the `cleared_bank_id`, and the replacement arrives as a new `CreatedBank`.',
    ],
    refs: [REFS.headBanks, REFS.headStream],
    sources: ['yellowstoneProto', 'blockMachine', 'simdUpdateParent'],
  }),
  retried: () => ({
    body: [
      'A projection moves to another slot only when the new bank is committed and the old one is still `processed`. Until then the signature keeps resolving to its first landing.',
    ],
    refs: [REFS.headMod],
    sources: [],
  }),
  confirmed: () => ({
    body: [
      'Superbank does not count votes: it applies the status Yellowstone reports. Each transaction’s `confirmationStatus` follows its own bank’s token.',
    ],
    refs: [REFS.headBanks, REFS.signatures],
    sources: ['agaveTowerCommitment'],
  }),
  finalized: (s) =>
    s.era === 'alpenglow'
      ? {
          body: [
            'Agave sets `confirmed` and `finalized` in the same step once votor roots the block, so Yellowstone’s `confirmed` status is sent at root time for compatibility. There is no window where a block is confirmed but not yet finalized.',
            'When the winning bank replaces another, only descendants whose parent hash names the replaced bank lose their coverage proof; descendants built on the winner keep it.',
          ],
          refs: [REFS.headBanks, REFS.headCoverage],
          sources: ['agaveCommitment', 'agaveSyntheticOc', 'agaveRootable', 'simdFinalization'],
        }
      : {
          body: [
            `Rooting happens about ${U('towerRootDepthSlots')} slots behind the tip. With a \`finalized\` minimum the head cache counts its window from the newest finalized slot, so the block stays cached for about ${C('headRetainSlots')} more slots.`,
          ],
          refs: [REFS.headBanks, REFS.headMod],
          sources: ['voteLockout', 'simdTowerFinality'],
        },
  edge: () => ({
    body: [
      'Two things happen at about the same slot and their order is not fixed: the root arrives, and the slot passes the edge of a window counted from the processed (or confirmed) tip. If the root comes first, the head cache briefly serves the block as `finalized`.',
      'The ingestor then needs up to the footer wait plus one flush to land the rows, so a read in this gap can find nothing. This race only exists at the development-sized default: a production window of several hundred slots keeps the slot long after its root.',
    ],
    refs: [REFS.headMod, REFS.ingestCli],
    sources: ['voteLockout', 'simdTowerFinality'],
  }),
  forked: () => ({
    body: [
      '`ForkDetected` is slot-level: the slot is not on the chain being rooted. A bank losing its slot to a sibling is reported separately as `BankDiscarded`.',
      'Signatures indexed on the dropped slot are removed with it, unless they already resolve to a different slot.',
    ],
    refs: [REFS.headStream, REFS.headMod],
    sources: ['blockMachine'],
  }),
  dropped: () => ({
    body: [
      'The full reset is deliberate: bank IDs and proofs from the old subscription cannot be trusted on the new one. Yellowstone’s reconnect-and-replay API is not used.',
      'Until ClickHouse ingestion passes the slots the head held, `getSlot`, `getBlockHeight` and `getLatestBlockhash` regress to the finalized tip, and `minContextSlot` callers can get `-32016`.',
    ],
    refs: [REFS.headStream, REFS.rpcReadme],
    sources: [],
  }),
  ingested: (s) => ({
    body: [
      s.era === 'alpenglow'
        ? 'Rows that were replayed after a restart get any footer fields already stored for them, so a restart does not erase footers.'
        : 'Footer columns exist on the table but stay `NULL` for Tower slots.',
      'From now on ClickHouse can answer this block at `confirmed` and `finalized`, whatever the head cache does.',
    ],
    refs: [REFS.ingestGrpc, REFS.ingestWriter, REFS.blocksMetadata],
    sources: s.era === 'alpenglow' ? ['simdFooter'] : [],
  }),
  evicted: () => ({
    body: [
      'Eviction is triggered when a newer slot is published, so it follows whatever minimum the cache publishes at. From here until the disk cache copies the slot, reads come from the source ClickHouse.',
    ],
    refs: [REFS.headMod],
    sources: [],
  }),
  disk: () => ({
    body: [
      'Newer ranges are copied first. Slots age out past `DISK_CACHE_RETAIN_SLOTS`, and whole partitions are evicted when the cache passes `DISK_CACHE_MAX_BYTES`. The source ClickHouse keeps the block for good.',
    ],
    refs: [REFS.diskFiller, REFS.rpcReadme],
    sources: [],
  }),
};

// Ids come from the page's own model, but guard against prototype keys anyway.
export function laneContent(id, input) {
  return Object.hasOwn(LANE, id) ? LANE[id](normalizeState(input)) : null;
}

// Step drawer content. The title and summary come from the frame; this adds
// detail, refs and sources.
export function stepContent(id, input) {
  return Object.hasOwn(STEP, id) ? STEP[id](normalizeState(input)) : null;
}

export const LANE_IDS = Object.freeze(LANES.map((l) => l.id));
export { STEP_IDS };
