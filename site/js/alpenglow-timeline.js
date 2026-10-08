// DOM renderer for one lifecycle frame: a slot ruler plus one row per lane.
// Plain elements, no canvas or SVG, so every part wraps, scales with the page
// and is reachable by keyboard and screen readers. All text goes through
// textContent (el/rich), never innerHTML.
//
// The ruler is linear in slots up to N+112, so switching eras moves the
// finalized-tip marker by the real ~32-slot difference; beyond that a
// compressed segment reaches a production-sized head window. Elements persist
// across updates so CSS can animate their positions.

import { appendRich, el, rich } from './dom.js';
import { CONSTANTS, LANES, answerRead, slotLabel } from './alpenglow-model.js';

const DOMAIN = [-36, 560];
const BREAK = 112; // slots past N where the ruler starts compressing
const BREAK_PCT = 76; // ...and how much of its width the linear part takes
const DISK_LAG = CONSTANTS.diskMinLagSlots.value;

const clamp = (x) => Math.min(DOMAIN[1], Math.max(DOMAIN[0], x));
function pct(rel) {
  const x = clamp(rel);
  if (x <= BREAK) return ((x - DOMAIN[0]) / (BREAK - DOMAIN[0])) * BREAK_PCT;
  return BREAK_PCT + ((x - BREAK) / (DOMAIN[1] - BREAK)) * (100 - BREAK_PCT);
}

const STATUS_TEXT = {
  staged: 'buffering',
  frozen: 'frozen',
  held: 'held',
  visible: 'served',
  discarded: 'discarded',
  cleared: 'cleared by reconnect',
  evicted: 'evicted',
};

const TIER_TEXT = { head: 'Head cache', disk: 'Disk cache', clickhouse: 'ClickHouse' };

export function createTimeline(host, { onLane } = {}) {
  // --- Ruler ---------------------------------------------------------------
  const band = (tier, label) => {
    const fill = el('span', { class: `ag-band__fill ag-band__fill--${tier}` });
    const gap = el('span', { class: 'ag-band__break', style: `left:${BREAK_PCT}%` });
    const row = el('div', { class: 'ag-band' }, [el('span', { class: 'ag-band__label', text: label }), el('span', { class: 'ag-band__track' }, [fill, gap])]);
    return { row, fill };
  };
  const bands = {
    head: band('head', 'Head window'),
    clickhouse: band('clickhouse', 'ClickHouse'),
    disk: band('disk', 'Disk cache'),
  };
  const marker = (kind) => {
    const label = el('span', { class: 'ag-marker__label' });
    const node = el('span', { class: `ag-marker ag-marker--${kind}` }, [label]);
    return { node, label };
  };
  const tip = marker('tip');
  const fin = marker('fin');
  const slotMark = el('span', { class: 'ag-slotmark' }, [el('span', { class: 'ag-slotmark__label', text: 'N' })]);
  // The head-window tick follows the selected HEAD_CACHE_RETAIN_SLOTS.
  const windowTick = el('span', { class: 'ag-tick' });
  const ticks = el('div', { class: 'ag-ticks', 'aria-hidden': 'true' }, [
    ...[
      [-32, 'N−32'],
      [32, 'N+32'],
      [DISK_LAG, `N+${DISK_LAG}`],
    ].map(([rel, text]) => el('span', { class: 'ag-tick', style: `left:${pct(rel)}%`, text })),
    el('span', { class: 'ag-tick ag-tick--break', style: `left:${BREAK_PCT}%`, text: '⋯' }),
    windowTick,
  ]);
  const markers = el('div', { class: 'ag-markers', 'aria-hidden': 'true' }, [slotMark, fin.node, tip.node]);
  const ruler = el('div', { class: 'ag-ruler', role: 'img' }, [
    markers,
    el('div', { class: 'ag-bands', 'aria-hidden': 'true' }, [bands.head.row, bands.clickhouse.row, bands.disk.row]),
    ticks,
  ]);

  // --- Lanes ---------------------------------------------------------------
  const lanes = {};
  const list = el(
    'ol',
    { class: 'ag-lanes' },
    LANES.map((lane) => {
      const button = el('button', { type: 'button', class: 'ag-lane__label', 'aria-pressed': 'false', 'data-lane': lane.id, text: lane.label });
      button.addEventListener('click', () => onLane?.(lane.id));
      const body = el('div', { class: 'ag-lane__body' });
      const row = el('li', { class: `ag-lane ag-lane--${lane.id}` }, [button, body]);
      lanes[lane.id] = { row, button, body };
      return row;
    }),
  );

  host.replaceChildren(ruler, list);

  const place = (fill, lo, hi) => {
    if (hi < lo || hi < DOMAIN[0]) {
      fill.style.left = `${pct(DOMAIN[0])}%`;
      fill.style.width = '0%';
      return;
    }
    fill.style.left = `${pct(lo)}%`;
    fill.style.width = `${Math.max(0.4, pct(hi) - pct(lo))}%`;
  };

  const muted = (text) => el('p', { class: 'ag-muted', text });

  function bankChip(b) {
    return el('li', { class: `ag-bank ag-bank--${b.status}${b.token ? ` ag-token--${b.token}` : ''}` }, [
      el('span', { class: 'ag-bank__slot', text: slotLabel(b.slot) }),
      el('span', { class: 'ag-bank__name', text: b.name }),
      el('span', { class: 'ag-bank__state', text: [STATUS_TEXT[b.status], b.token].filter(Boolean).join(' · ') }),
    ]);
  }

  function update(frame, state) {
    const { featured } = frame;

    // Ruler
    const w = frame.headWindow;
    place(bands.head.fill, w.lo, w.hi);
    const storedFeatured = Boolean(frame.stored[featured]);
    place(bands.clickhouse.fill, DOMAIN[0], storedFeatured ? frame.fin : Math.min(frame.fin, featured - 1));
    place(bands.disk.fill, DOMAIN[0], frame.disk[featured] ? frame.fin - DISK_LAG : Math.min(frame.fin - DISK_LAG, featured - 1));
    for (const [m, rel, text] of [
      [tip, frame.tip, `tip ${frame.tipLabel}`],
      [fin, frame.fin, `finalized ${frame.finLabel}`],
    ]) {
      m.node.style.left = `${pct(rel)}%`;
      m.label.textContent = text;
      // Flip the label to the left of its line near the right edge.
      m.label.classList.toggle('is-right', pct(rel) > 60);
    }
    windowTick.hidden = frame.retain <= BREAK;
    windowTick.style.left = `${pct(featured + frame.retain)}%`;
    windowTick.textContent = slotLabel(featured + frame.retain);
    slotMark.style.left = `${pct(featured)}%`;
    slotMark.firstChild.textContent = slotLabel(featured);
    ruler.setAttribute(
      'aria-label',
      `Slot ruler. Tip ${frame.tipLabel}, finalized tip ${frame.finLabel}. Head cache window ${slotLabel(w.lo)} to ${slotLabel(w.hi)}${
        frame.headCleared ? ' (new session)' : ''
      }.`,
    );

    // Upstream
    lanes.upstream.body.replaceChildren(frame.upstream ? rich('p', frame.upstream) : muted('No upstream event in this step.'));

    // Stream
    lanes.stream.body.replaceChildren(
      frame.stream.length ? el('ul', { class: 'ag-msgs' }, frame.stream.map((m) => el('li', { class: 'ag-msg', text: m }))) : muted('No new messages.'),
    );

    // Head cache
    const banks = frame.banks.length ? el('ul', { class: 'ag-banks' }, frame.banks.map(bankChip)) : muted('Nothing buffered yet.');
    const windowText = frame.headCleared
      ? `New session after a reconnect: only slots after ${slotLabel(w.lo - 1)}.`
      : `Window ${slotLabel(w.lo)} … ${slotLabel(w.hi)} (newest ${frame.retain} published slots).`;
    lanes.head.body.replaceChildren(banks, el('p', { class: 'ag-meta', text: windowText }));

    // Ingest
    const ingestRows = [...new Set(frame.banks.map((b) => b.slot))].map((slot) => {
      const row = frame.stored[slot];
      const dropped = frame.banks.filter((b) => b.slot === slot).every((b) => b.status === 'discarded');
      let text;
      let cls = 'ag-store';
      if (row) {
        text = `${slotLabel(slot)} · written · footer ${row.footer === 'set' ? 'columns set' : 'columns NULL'}`;
        cls += ' ag-store--done ag-store--clickhouse';
      } else if (dropped) {
        text = `${slotLabel(slot)} · never written: not on the finalized chain`;
        cls += ' ag-store--never';
      } else if (frame.pending && slot === featured) {
        text = `${slotLabel(slot)} · finalized, write in flight`;
        cls += ' ag-store--pending';
      } else {
        text = `${slotLabel(slot)} · waiting for finalized`;
      }
      return el('li', { class: cls, text });
    });
    lanes.ingest.body.replaceChildren(el('ul', { class: 'ag-stores' }, ingestRows));

    // Disk
    const copied = frame.disk[featured];
    lanes.disk.body.replaceChildren(
      el('ul', { class: 'ag-stores' }, [
        el('li', {
          class: `ag-store${copied ? ' ag-store--done ag-store--disk' : ''}`,
          text: copied
            ? `${slotLabel(featured)} · copied from ClickHouse`
            : `${slotLabel(featured)} · waits until the finalized tip reaches ${slotLabel(featured + DISK_LAG)}`,
        }),
      ]),
    );

    // Probe
    const answer = answerRead(frame, state.method, state.read);
    const target = state.method === 'getBlock' ? 'getBlock(N)' : 'getTransaction(T)';
    const result = el('p', { class: `ag-probe__result${answer.tier ? ` ag-probe__result--${answer.tier}` : ''}` });
    if (answer.tier) {
      result.append(el('span', { class: 'ag-probe__tier', text: TIER_TEXT[answer.tier] }));
      appendRich(result, ` → ${slotLabel(answer.slot)} · ${answer.bank} · \`${answer.token}\``);
      if (answer.footer !== undefined) appendRich(result, answer.footer ? ' · `footer: {…}`' : ' · `footer: null`');
    } else {
      appendRich(result, answer.error ? `\`${answer.error}\`: ${answer.text}` : answer.text);
    }
    lanes.probe.body.replaceChildren(el('p', { class: 'ag-probe__ask' }, [el('code', { text: `${target} @ ${state.read}` })]), result);
  }

  function select(laneId) {
    for (const [id, lane] of Object.entries(lanes)) {
      const on = id === laneId;
      lane.button.setAttribute('aria-pressed', String(on));
      lane.row.classList.toggle('is-selected', on);
    }
  }

  return { update, select };
}
