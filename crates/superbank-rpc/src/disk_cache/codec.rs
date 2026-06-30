// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Value encodings for the disk cache.
//!
//! Record values (`block_meta`, `tx` CFs) are bincode wrapped in a 1-byte version
//! envelope. Index values (`sig`, `addr_sig`, `token_owner` CFs) are hand-rolled so
//! the compaction filters and hot lookups never run serde. Any decode failure is a
//! cache miss, never an error surfaced to a request.

use serde::Serialize;
use serde::de::DeserializeOwned;

pub(crate) const VALUE_VERSION_V1: u8 = 1;

pub(crate) fn encode_record<T: Serialize>(value: &T) -> Result<Vec<u8>, bincode::Error> {
    let mut out = Vec::with_capacity(128);
    out.push(VALUE_VERSION_V1);
    bincode::serialize_into(&mut out, value)?;
    Ok(out)
}

pub(crate) fn decode_record<T: DeserializeOwned>(bytes: &[u8]) -> Option<T> {
    let (&version, payload) = bytes.split_first()?;
    if version != VALUE_VERSION_V1 {
        return None;
    }
    bincode::deserialize(payload).ok()
}

/// `sig` CF value: `[ver][slot u64 BE][idx u32 BE][err_len u32 BE][err bytes]`.
/// `err_len == 0` means the transaction succeeded (matches the gsfa.sql
/// `if(meta_status_ok = 1, NULL, meta_err)` convention).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SigValue {
    pub(crate) slot: u64,
    pub(crate) idx: u32,
    pub(crate) err: Option<String>,
}

pub(crate) fn encode_sig_value(slot: u64, idx: u32, err: Option<&str>) -> Vec<u8> {
    let err_bytes = err.map(str::as_bytes).unwrap_or_default();
    let mut out = Vec::with_capacity(1 + 8 + 4 + 4 + err_bytes.len());
    out.push(VALUE_VERSION_V1);
    out.extend_from_slice(&slot.to_be_bytes());
    out.extend_from_slice(&idx.to_be_bytes());
    out.extend_from_slice(&(err_bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(err_bytes);
    out
}

pub(crate) fn decode_sig_value(bytes: &[u8]) -> Option<SigValue> {
    if bytes.len() < 17 || bytes[0] != VALUE_VERSION_V1 {
        return None;
    }
    let slot = u64::from_be_bytes(bytes[1..9].try_into().ok()?);
    let idx = u32::from_be_bytes(bytes[9..13].try_into().ok()?);
    let err_len = u32::from_be_bytes(bytes[13..17].try_into().ok()?) as usize;
    let err_bytes = bytes.get(17..17 + err_len)?;
    let err = if err_len == 0 {
        None
    } else {
        Some(String::from_utf8(err_bytes.to_vec()).ok()?)
    };
    Some(SigValue { slot, idx, err })
}

/// Slot extraction for the `sig` CF compaction filter — no allocation, no serde.
pub(crate) fn sig_value_slot(bytes: &[u8]) -> Option<u64> {
    if bytes.first() != Some(&VALUE_VERSION_V1) {
        return None;
    }
    Some(u64::from_be_bytes(bytes.get(1..9)?.try_into().ok()?))
}

/// `slot_coverage` CF value: `[ver][tag]` plus, for Covered, `[tx_count u32 BE][source]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CoverageValue {
    Covered { tx_count: u32, source: u8 },
    Skipped,
}

pub(crate) fn encode_coverage_value(value: CoverageValue) -> Vec<u8> {
    match value {
        CoverageValue::Covered { tx_count, source } => {
            let mut out = Vec::with_capacity(7);
            out.push(VALUE_VERSION_V1);
            out.push(super::schema::COVERAGE_TAG_COVERED);
            out.extend_from_slice(&tx_count.to_be_bytes());
            out.push(source);
            out
        }
        CoverageValue::Skipped => vec![VALUE_VERSION_V1, super::schema::COVERAGE_TAG_SKIPPED],
    }
}

pub(crate) fn decode_coverage_value(bytes: &[u8]) -> Option<CoverageValue> {
    if bytes.len() < 2 || bytes[0] != VALUE_VERSION_V1 {
        return None;
    }
    match bytes[1] {
        tag if tag == super::schema::COVERAGE_TAG_COVERED => {
            let tx_count = u32::from_be_bytes(bytes.get(2..6)?.try_into().ok()?);
            let source = *bytes.get(6)?;
            Some(CoverageValue::Covered { tx_count, source })
        }
        tag if tag == super::schema::COVERAGE_TAG_SKIPPED => Some(CoverageValue::Skipped),
        _ => None,
    }
}

const FLAG_ERR: u8 = 1 << 0;
const FLAG_MEMO: u8 = 1 << 1;
const FLAG_BLOCK_TIME: u8 = 1 << 2;
const FLAG_BALANCE_CHANGED: u8 = 1 << 3;

/// `addr_sig` / `token_owner` CF value: `[ver][signature 64][flags]` followed by the
/// optional fields in flag order: err (`u32 BE` length + bytes), memo (same), and
/// block_time (`i64 BE`). `token_owner` additionally uses the balance-changed flag bit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AddrSigValue {
    pub(crate) signature: [u8; 64],
    pub(crate) err: Option<String>,
    pub(crate) memo: Option<String>,
    pub(crate) block_time: Option<i64>,
    pub(crate) balance_changed: bool,
}

pub(crate) fn encode_addr_sig_value(value: &AddrSigValue) -> Vec<u8> {
    let err_bytes = value.err.as_deref().map(str::as_bytes).unwrap_or_default();
    let memo_bytes = value.memo.as_deref().map(str::as_bytes).unwrap_or_default();
    let mut out = Vec::with_capacity(1 + 64 + 1 + 8 + err_bytes.len() + memo_bytes.len() + 8 + 8);

    let mut flags = 0u8;
    if value.err.is_some() {
        flags |= FLAG_ERR;
    }
    if value.memo.is_some() {
        flags |= FLAG_MEMO;
    }
    if value.block_time.is_some() {
        flags |= FLAG_BLOCK_TIME;
    }
    if value.balance_changed {
        flags |= FLAG_BALANCE_CHANGED;
    }

    out.push(VALUE_VERSION_V1);
    out.extend_from_slice(&value.signature);
    out.push(flags);
    if value.err.is_some() {
        out.extend_from_slice(&(err_bytes.len() as u32).to_be_bytes());
        out.extend_from_slice(err_bytes);
    }
    if value.memo.is_some() {
        out.extend_from_slice(&(memo_bytes.len() as u32).to_be_bytes());
        out.extend_from_slice(memo_bytes);
    }
    if let Some(block_time) = value.block_time {
        out.extend_from_slice(&block_time.to_be_bytes());
    }
    out
}

pub(crate) fn decode_addr_sig_value(bytes: &[u8]) -> Option<AddrSigValue> {
    if bytes.len() < 66 || bytes[0] != VALUE_VERSION_V1 {
        return None;
    }
    let signature: [u8; 64] = bytes[1..65].try_into().ok()?;
    let flags = bytes[65];
    let mut cursor = 66usize;

    let read_string = |bytes: &[u8], cursor: &mut usize| -> Option<String> {
        let len = u32::from_be_bytes(bytes.get(*cursor..*cursor + 4)?.try_into().ok()?) as usize;
        *cursor += 4;
        let raw = bytes.get(*cursor..*cursor + len)?;
        *cursor += len;
        String::from_utf8(raw.to_vec()).ok()
    };

    let err = if flags & FLAG_ERR != 0 {
        Some(read_string(bytes, &mut cursor)?)
    } else {
        None
    };
    let memo = if flags & FLAG_MEMO != 0 {
        Some(read_string(bytes, &mut cursor)?)
    } else {
        None
    };
    let block_time = if flags & FLAG_BLOCK_TIME != 0 {
        let raw = bytes.get(cursor..cursor + 8)?;
        cursor += 8;
        Some(i64::from_be_bytes(raw.try_into().ok()?))
    } else {
        None
    };
    let _ = cursor;

    Some(AddrSigValue {
        signature,
        err,
        memo,
        block_time,
        balance_changed: flags & FLAG_BALANCE_CHANGED != 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sig_value_round_trip() {
        for err in [None, Some("{\"InstructionError\":[0,\"Custom\"]}")] {
            let encoded = encode_sig_value(123_456_789, 42, err);
            let decoded = decode_sig_value(&encoded).expect("decode");
            assert_eq!(decoded.slot, 123_456_789);
            assert_eq!(decoded.idx, 42);
            assert_eq!(decoded.err.as_deref(), err);
            assert_eq!(sig_value_slot(&encoded), Some(123_456_789));
        }
    }

    #[test]
    fn sig_value_rejects_unknown_version_and_truncation() {
        let mut encoded = encode_sig_value(7, 1, Some("err"));
        encoded[0] = 99;
        assert!(decode_sig_value(&encoded).is_none());
        assert!(sig_value_slot(&encoded).is_none());

        let encoded = encode_sig_value(7, 1, Some("err"));
        assert!(decode_sig_value(&encoded[..encoded.len() - 1]).is_none());
        assert!(decode_sig_value(&[]).is_none());
    }

    #[test]
    fn addr_sig_value_round_trip_all_field_combinations() {
        let mut signature = [0u8; 64];
        signature[0] = 0xAB;
        signature[63] = 0xCD;

        for err in [None, Some("{\"err\":1}".to_string())] {
            for memo in [None, Some("[5] hello".to_string())] {
                for block_time in [None, Some(1_700_000_000i64), Some(-1i64)] {
                    for balance_changed in [false, true] {
                        let value = AddrSigValue {
                            signature,
                            err: err.clone(),
                            memo: memo.clone(),
                            block_time,
                            balance_changed,
                        };
                        let encoded = encode_addr_sig_value(&value);
                        let decoded = decode_addr_sig_value(&encoded).expect("decode");
                        assert_eq!(decoded, value);
                    }
                }
            }
        }
    }

    #[test]
    fn addr_sig_value_rejects_truncation() {
        let value = AddrSigValue {
            signature: [7u8; 64],
            err: Some("e".to_string()),
            memo: Some("m".to_string()),
            block_time: Some(5),
            balance_changed: true,
        };
        let encoded = encode_addr_sig_value(&value);
        for len in 0..encoded.len() {
            assert!(
                decode_addr_sig_value(&encoded[..len]).is_none(),
                "truncated to {len} bytes should not decode"
            );
        }
    }

    #[test]
    fn coverage_value_round_trip() {
        let covered = CoverageValue::Covered {
            tx_count: 3210,
            source: crate::disk_cache::schema::COVERAGE_SOURCE_LIVE,
        };
        assert_eq!(
            decode_coverage_value(&encode_coverage_value(covered)),
            Some(covered)
        );
        assert_eq!(
            decode_coverage_value(&encode_coverage_value(CoverageValue::Skipped)),
            Some(CoverageValue::Skipped)
        );
        assert_eq!(decode_coverage_value(&[VALUE_VERSION_V1, 9]), None);
        assert_eq!(decode_coverage_value(&[]), None);
    }

    #[test]
    fn record_envelope_round_trip() {
        let value = vec![1u64, 2, 3];
        let encoded = encode_record(&value).expect("encode");
        assert_eq!(encoded[0], VALUE_VERSION_V1);
        assert_eq!(decode_record::<Vec<u64>>(&encoded), Some(value));

        let mut wrong_version = encoded.clone();
        wrong_version[0] = 2;
        assert_eq!(decode_record::<Vec<u64>>(&wrong_version), None);
        assert_eq!(decode_record::<Vec<u64>>(&[]), None);
    }
}
