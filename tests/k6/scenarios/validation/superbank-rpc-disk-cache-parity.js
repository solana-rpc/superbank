// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

// Disk-cache parity validation for superbank-rpc.
//
// Purpose: prove that a disk-cache-enabled superbank-rpc returns byte-identical
// results to a reference target (the same build with the disk cache disabled,
// or any trusted RPC) for every method the disk tier serves: getBlock (all
// transactionDetails levels), getTransaction, getSignatureStatuses, getBlocks,
// getBlockTime, getSignaturesForAddress, and getTransactionsForAddress —
// including pagination walks that cross the disk coverage floor, which is
// where tier-boundary duplicates or gaps would show up.
//
// Pagination tokens themselves are NOT compared for getTransactionsForAddress:
// disk- and head-sourced rows intentionally emit position-shaped tokens while
// ClickHouse rows emit signature tokens. Each walk continues with its own
// target's token; the concatenated data pages must still match exactly.
//
// Usage:
//   k6 run tests/k6/scenarios/validation/superbank-rpc-disk-cache-parity.js \
//     -e RPC_URL=http://disk-enabled:8899 \
//     -e REFERENCE_RPC_URL=http://reference:8899 \
//     -e ADDRESS_FILE=tests/k6/data/pools/addresses.txt
//
// Tuning:
//   PARITY_SLOT_SPAN      how far below the shared tip to sample blocks (default 256)
//   PARITY_BLOCK_SAMPLES  how many slots to sample in that span (default 12)
//   PARITY_ADDRESSES      how many pool addresses to walk (default 5)
//   PARITY_PAGE_SIZE      gSFA/gTFA page size (default 100)
//   PARITY_MAX_PAGES      page cap per address walk (default 10)

import http from 'k6/http';
import { check, fail } from 'k6';
import { config } from '../../lib/config.js';
import { deepEqual } from '../../lib/compare.js';
import { initAddressPool, getAddressPool } from '../../lib/addresses.js';

const rpcUrl = config.rpcUrl;
const referenceUrl = config.referenceRpcUrl;
const slotSpan = Number(__ENV.PARITY_SLOT_SPAN || 256);
const blockSamples = Number(__ENV.PARITY_BLOCK_SAMPLES || 12);
const addressCount = Number(__ENV.PARITY_ADDRESSES || 5);
const pageSize = Number(__ENV.PARITY_PAGE_SIZE || 100);
const maxPages = Number(__ENV.PARITY_MAX_PAGES || 10);

export const options = {
  vus: 1,
  iterations: 1,
  thresholds: {
    checks: ['rate==1'],
  },
};

let requestId = 0;

function rpc(url, method, params) {
  requestId += 1;
  const response = http.post(
    url,
    JSON.stringify({ jsonrpc: '2.0', id: requestId, method, params }),
    { headers: { 'Content-Type': 'application/json' }, timeout: '30s' },
  );
  if (response.status !== 200) {
    return { transport_error: `${method}: HTTP ${response.status}` };
  }
  try {
    return JSON.parse(response.body);
  } catch (_) {
    return { transport_error: `${method}: unparsable body` };
  }
}

/**
 * Issue the same call against both targets and require identical outcomes:
 * equal results, or equal error codes.
 */
function compareCall(label, method, params) {
  const target = rpc(rpcUrl, method, params);
  const reference = rpc(referenceUrl, method, params);

  const ok = check(null, {
    [`${label}: no transport errors`]: () =>
      !target.transport_error && !reference.transport_error,
    [`${label}: error parity`]: () =>
      Boolean(target.error) === Boolean(reference.error) &&
      (!target.error || target.error.code === reference.error.code),
    [`${label}: result parity`]: () =>
      Boolean(target.error) || deepEqual(target.result ?? null, reference.result ?? null),
  });
  if (!ok) {
    console.error(
      `${label} mismatch\n  params: ${JSON.stringify(params)}\n` +
        `  target: ${JSON.stringify(target).slice(0, 2000)}\n` +
        `  reference: ${JSON.stringify(reference).slice(0, 2000)}`,
    );
  }
  return target;
}

export function setup() {
  if (!referenceUrl) {
    fail('REFERENCE_RPC_URL is required for parity validation');
  }
  initAddressPool();

  // Anchor on a finalized tip both targets have passed.
  const target = rpc(rpcUrl, 'getSlot', [{ commitment: 'finalized' }]);
  const reference = rpc(referenceUrl, 'getSlot', [{ commitment: 'finalized' }]);
  if (target.error || reference.error || !target.result || !reference.result) {
    fail('could not resolve finalized tips for both targets');
  }
  const tip = Math.min(target.result, reference.result) - 32;
  return { tip, addresses: getAddressPool().slice(0, addressCount) };
}

export default function (data) {
  const { tip, addresses } = data;
  const span = Math.min(slotSpan, tip);
  const harvestedSignatures = [];

  // --- Block-shaped methods over sampled recent slots -----------------------
  const step = Math.max(1, Math.floor(span / blockSamples));
  for (let slot = tip; slot > tip - span; slot -= step) {
    const full = compareCall('getBlock full', 'getBlock', [
      slot,
      {
        transactionDetails: 'full',
        rewards: true,
        maxSupportedTransactionVersion: 0,
        commitment: 'finalized',
      },
    ]);
    for (const details of ['none', 'signatures', 'accounts']) {
      compareCall(`getBlock ${details}`, 'getBlock', [
        slot,
        {
          transactionDetails: details,
          rewards: details !== 'none',
          maxSupportedTransactionVersion: 0,
          commitment: 'finalized',
        },
      ]);
    }
    compareCall('getBlockTime', 'getBlockTime', [slot]);

    if (!full.error && full.result && Array.isArray(full.result.transactions)) {
      for (const tx of full.result.transactions.slice(0, 3)) {
        const signature = tx?.transaction?.signatures?.[0];
        if (signature) {
          harvestedSignatures.push(signature);
        }
      }
    }
  }

  // Range listing straddling the sampled span.
  compareCall('getBlocks', 'getBlocks', [tip - span, tip]);
  compareCall('getBlocksWithLimit', 'getBlocksWithLimit', [tip - span, 50]);

  // --- Signature-shaped methods over harvested signatures -------------------
  for (const signature of harvestedSignatures.slice(0, 25)) {
    compareCall('getTransaction', 'getTransaction', [
      signature,
      { maxSupportedTransactionVersion: 0, commitment: 'finalized' },
    ]);
  }
  if (harvestedSignatures.length > 0) {
    compareCall('getSignatureStatuses', 'getSignatureStatuses', [
      harvestedSignatures.slice(0, 50),
    ]);
  }

  // --- Address walks across the coverage floor ------------------------------
  for (const address of addresses) {
    // getSignaturesForAddress: walk with before=<last signature>, identical
    // cursors on both sides (signatures are target-independent).
    let before = null;
    for (let page = 0; page < maxPages; page += 1) {
      const params = [address, before ? { limit: pageSize, before } : { limit: pageSize }];
      const target = compareCall('gSFA walk', 'getSignaturesForAddress', params);
      const rows = !target.error && Array.isArray(target.result) ? target.result : [];
      if (rows.length < pageSize) {
        break;
      }
      before = rows[rows.length - 1].signature;
    }

    // getTransactionsForAddress: each target walks with its own pagination
    // token; the data pages must match page-for-page.
    let targetToken = null;
    let referenceToken = null;
    for (let page = 0; page < maxPages; page += 1) {
      const optionsFor = (token) => {
        const opts = { limit: pageSize, transactionDetails: 'signatures' };
        if (token) {
          opts.paginationToken = token;
        }
        return opts;
      };
      const target = rpc(rpcUrl, 'getTransactionsForAddress', [
        address,
        optionsFor(targetToken),
      ]);
      const reference = rpc(referenceUrl, 'getTransactionsForAddress', [
        address,
        optionsFor(referenceToken),
      ]);

      const ok = check(null, {
        'gTFA walk: no transport errors': () =>
          !target.transport_error && !reference.transport_error,
        'gTFA walk: error parity': () =>
          Boolean(target.error) === Boolean(reference.error) &&
          (!target.error || target.error.code === reference.error.code),
        'gTFA walk: page data parity': () =>
          Boolean(target.error) ||
          deepEqual(target.result?.data ?? null, reference.result?.data ?? null),
      });
      if (!ok) {
        console.error(
          `gTFA page ${page} mismatch for ${address}\n` +
            `  target: ${JSON.stringify(target).slice(0, 2000)}\n` +
            `  reference: ${JSON.stringify(reference).slice(0, 2000)}`,
        );
      }
      if (target.error || !target.result) {
        break;
      }

      const rows = Array.isArray(target.result.data) ? target.result.data : [];
      targetToken = target.result.paginationToken ?? null;
      referenceToken = reference.result?.paginationToken ?? null;
      if (rows.length < pageSize || !targetToken || !referenceToken) {
        break;
      }
    }
  }
}
