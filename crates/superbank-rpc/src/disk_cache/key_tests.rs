// SPDX-License-Identifier: AGPL-3.0-only
//! Opt-in integration against a disposable, loopback ClickHouse server.
//! Raw queries below are fixture setup and independent observation probes;
//! cancellation assertions exercise the guarded application read path.
use super::*;
use crate::clickhouse::{NumericFilter, SortOrder, TransactionStatusFilter};
use crate::solana_sdk::{pubkey::Pubkey, signature::Signature};

fn found_transaction(result: DiskTransactionResult) -> Arc<StoredTransactionRecord> {
    match result {
        DiskTransactionResult::Found(record) => record,
        other => panic!("expected transaction, got {other:?}"),
    }
}

fn signature(slot: u64) -> Signature {
    named_signature("sig", slot)
}
fn named_signature(prefix: &str, slot: u64) -> Signature {
    let mut key = [0; 64];
    let value = format!("{prefix}-{slot}");
    key[..value.len()].copy_from_slice(value.as_bytes());
    Signature::from(key)
}
fn signature_candidate(cache: &DiskCache, partition: u64, signature: Signature) -> bool {
    !cache
        .inner
        .key_index
        .signature_candidates(
            partition,
            partition,
            key_index::SignatureHash::new(signature.as_ref()),
        )
        .partitions
        .is_empty()
}

async fn transaction_response(
    source: &ClickHouseClient,
    cache: Option<&DiskCache>,
    signature: Signature,
    config: serde_json::Value,
) -> serde_json::Value {
    let mut state = Arc::try_unwrap(crate::tests::test_state()).ok().unwrap();
    state.clickhouse = source.clone();
    state.disk_cache = cache.map(|cache| {
        Arc::new(tokio::sync::OnceCell::new_with(Some(Arc::new(
            cache.clone(),
        ))))
    });
    let response = Box::pin(crate::handlers::transactions::handle_get_transaction(
        Arc::new(state),
        serde_json::json!("transaction-regression"),
        Some(vec![serde_json::json!(signature.to_string()), config]),
    ))
    .await
    .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

async fn assert_transaction_fallback(source: &ClickHouseClient, cache: &DiskCache) {
    for encoding in ["json", "jsonParsed", "base58", "base64"] {
        let config = serde_json::json!({"slot": 45, "encoding": encoding, "maxSupportedTransactionVersion": 1});
        let expected = transaction_response(source, None, signature(45), config.clone()).await;
        assert!(expected.get("error").is_none(), "{expected}");
        assert_eq!(expected["result"]["slot"], 45);
        let actual = transaction_response(source, Some(cache), signature(45), config).await;
        assert_eq!(actual, expected);
    }
}

async fn assert_transaction_invalidation(cache: &DiskCache) {
    let permits = cache
        .inner
        .local
        .http_query_sem
        .acquire_many(2)
        .await
        .unwrap();
    let read = cache.get_tx(signature(45), Some(45));
    tokio::pin!(read);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut read)
            .await
            .is_err()
    );
    cache.inner.key_index.invalidate_reads();
    drop(permits);
    assert!(matches!(read.await, DiskTransactionResult::Unavailable));
}

/// Starts a getTransaction behind held admission permits, runs `event` while it is
/// in flight, then releases the permits and returns what the read served.
async fn get_tx_across<F: Future<Output = ()>>(
    cache: &DiskCache,
    slot: u64,
    event: impl FnOnce() -> F,
) -> DiskTransactionResult {
    let permits = cache
        .inner
        .local
        .http_query_sem
        .acquire_many(2)
        .await
        .unwrap();
    // Boxed: debug-build read futures are large and this fixture runs deep.
    let mut read = Box::pin(cache.get_tx(signature(slot), None));
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut read)
            .await
            .is_err()
    );
    Box::pin(event()).await;
    drop(permits);
    read.await
}

/// Eviction moves only the floor: a found row whose slot is still covered is served.
/// Data-invalidating events (poison) and evicted slots still fall back.
async fn assert_transaction_read_revalidation(cache: &DiskCache) {
    let found = get_tx_across(cache, 45, || async {
        assert!(cache.evict_below(12, "window").await.unwrap());
    })
    .await;
    assert_eq!(found_transaction(found).slot, 45);
    // Poison of the row's own slot, and of any other slot, rejects the in-flight row.
    for poisoned in [43, 44] {
        let result = get_tx_across(cache, 44, || cache.poison_slot(poisoned)).await;
        assert!(matches!(result, DiskTransactionResult::Unavailable));
    }
    let result = get_tx_across(cache, 25, || async {
        assert!(cache.evict_below(30, "window").await.unwrap());
    })
    .await;
    assert!(matches!(result, DiskTransactionResult::Unavailable));
    assert!(!cache.covers_slot(25));
    assert_eq!(
        found_transaction(Box::pin(cache.get_tx(signature(45), None)).await).slot,
        45
    );
}

/// A stalled local server (every interactive permit held): getTransaction gives up at
/// its own budget while a status read on the same cache still waits the full timeout.
async fn assert_get_tx_budget(source: &ClickHouseClient, cfg: &DiskCacheConfig) {
    let mut cfg = cfg.clone();
    cfg.query_timeout = Duration::from_millis(1_500);
    cfg.get_tx_timeout = Duration::from_millis(200);
    let cache = Box::pin(DiskCache::open(cfg, source)).await.unwrap();
    let _permits = cache
        .inner
        .local
        .http_query_sem
        .acquire_many(2)
        .await
        .unwrap();
    let started = std::time::Instant::now();
    let (tx, statuses) = tokio::join!(
        async {
            let result = Box::pin(cache.get_tx(signature(45), None)).await;
            (result, started.elapsed())
        },
        async {
            let result = Box::pin(cache.get_sig_statuses(vec![signature(45)])).await;
            (result, started.elapsed())
        },
    );
    assert!(matches!(tx.0, DiskTransactionResult::Unavailable));
    assert!(
        tx.1 >= Duration::from_millis(200) && tx.1 < Duration::from_millis(700),
        "get_tx gave up after {:?}",
        tx.1
    );
    assert!(statuses.0[0].is_none());
    assert!(
        statuses.1 >= Duration::from_millis(1_500),
        "status read gave up after {:?}",
        statuses.1
    );
}

/// Times one getTransaction against a stalled local server (every interactive permit held).
async fn stalled_get_tx(cache: &DiskCache, slot: u64, requested_slot: Option<u64>) -> Duration {
    let _permits = cache
        .inner
        .local
        .http_query_sem
        .acquire_many(2)
        .await
        .unwrap();
    let started = std::time::Instant::now();
    let result = Box::pin(cache.get_tx(signature(slot), requested_slot)).await;
    let elapsed = started.elapsed();
    // An expired budget is never proof of absence, even for a covered requested slot.
    assert!(
        matches!(result, DiskTransactionResult::Unavailable),
        "{result:?}"
    );
    elapsed
}

/// After a restart or a signature-index reset every partition is unknown. With more
/// than `GET_TX_UNKNOWN_PARTITION_LIMIT` of them a stalled getTransaction gives up at
/// the unknown-membership budget while a fast hit is still served; a built index, or
/// one partition left unknown by a repair, keeps the normal budget.
async fn assert_get_tx_unknown_budget(
    client: &clickhouse::Client,
    source: &ClickHouseClient,
    cfg: &DiskCacheConfig,
) {
    let mut cfg = cfg.clone();
    cfg.database = format!("{}_unknown", cfg.database);
    // Eight partitions (2..=9) over slots 10..49, twice the gate's limit.
    cfg.partition_slots = 5;
    cfg.query_timeout = Duration::from_millis(2_000);
    cfg.get_tx_timeout = Duration::from_millis(800);
    cfg.get_tx_unknown_timeout = Duration::from_millis(100);
    let (normal, short) = (cfg.get_tx_timeout, cfg.get_tx_unknown_timeout);
    let cache = Box::pin(DiskCache::open(cfg.clone(), source))
        .await
        .unwrap();
    insert_transactions(client, &cfg.database).await;
    cache
        .publish_range_coverage(
            (10..50)
                .map(|slot| (slot, SlotStatus::Covered { tx_count: 1 }))
                .collect(),
        )
        .await
        .unwrap();
    assert_eq!(cache.unknown_signature_partitions(), 8);
    // Gated, but a responsive local server still serves the hit.
    assert_eq!(
        found_transaction(Box::pin(cache.get_tx(signature(45), None)).await).slot,
        45
    );
    // A gated miss that completes probes every unknown partition and is still no proof.
    let started = std::time::Instant::now();
    let miss = Box::pin(cache.get_tx(signature(500), None)).await;
    eprintln!("unknown x8 miss took {:?}", started.elapsed());
    assert!(
        matches!(miss, DiskTransactionResult::Unavailable),
        "{miss:?}"
    );
    for (slot, requested_slot) in [(45, None), (45, Some(45)), (500, Some(45))] {
        let elapsed = stalled_get_tx(&cache, slot, requested_slot).await;
        eprintln!("unknown x8 stalled get_tx gave up after {elapsed:?}");
        assert!(
            elapsed >= short && elapsed < normal / 2,
            "gated get_tx gave up after {elapsed:?}"
        );
    }
    cache.build_signature_indexes().await;
    assert_eq!(cache.unknown_signature_partitions(), 0);
    let elapsed = stalled_get_tx(&cache, 45, None).await;
    eprintln!("known stalled get_tx gave up after {elapsed:?}");
    assert!(elapsed >= normal, "known get_tx gave up after {elapsed:?}");
    // A repair leaves one partition unknown: below the limit, the normal budget.
    let repair = cache.begin_fill(25, 25);
    assert_eq!(cache.unknown_signature_partitions(), 1);
    let elapsed = stalled_get_tx(&cache, 45, None).await;
    assert!(elapsed >= normal, "repair get_tx gave up after {elapsed:?}");
    drop(repair);
    // A signature-index reset makes every partition unknown again.
    cache.inner.key_index.clear_signatures();
    assert_eq!(cache.unknown_signature_partitions(), 8);
    let elapsed = stalled_get_tx(&cache, 45, None).await;
    assert!(
        elapsed >= short && elapsed < normal / 2,
        "reset get_tx gave up after {elapsed:?}"
    );
    execute(client, &format!("DROP DATABASE {} SYNC", cfg.database)).await;
}

async fn assert_transaction_append_race(client: &clickhouse::Client, cache: &DiskCache) {
    cache.inner.key_index.clear_signatures();
    let permits = cache
        .inner
        .local
        .http_query_sem
        .acquire_many(2)
        .await
        .unwrap();
    let read = cache.get_tx(signature(50), Some(50));
    tokio::pin!(read);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut read)
            .await
            .is_err()
    );
    execute(client, &format!("INSERT INTO {}.transactions (signature,slot,slot_idx,tx_signatures) VALUES (toFixedString('sig-50',64),50,7,[toFixedString('sig-50',64)])", cache.inner.cfg.database)).await;
    cache
        .publish_range_coverage(vec![(50, SlotStatus::Covered { tx_count: 1 })])
        .await
        .unwrap();
    drop(permits);
    assert!(matches!(read.await, DiskTransactionResult::Unavailable));
    let found = found_transaction(cache.get_tx(signature(50), Some(50)).await);
    assert_eq!(found.slot, 50);
    cache
        .publish_range_coverage(vec![(51, SlotStatus::Skipped)])
        .await
        .unwrap();
    assert!(matches!(
        cache.get_tx(signature(500), Some(51)).await,
        DiskTransactionResult::Absent
    ));
    cache.build_signature_indexes().await;
}

async fn assert_transaction_positions(cache: &DiskCache) {
    let mut client = cache.query_client();
    client.cache_partition = Some((
        cache.inner.cfg.partition_slots,
        45 / cache.inner.cfg.partition_slots,
    ));
    for slot_idx in [0, 999] {
        let (record, _) = client
            .get_transaction_by_signature_and_position(
                &signature(45).to_string(),
                crate::clickhouse::SignatureSlot { slot: 45, slot_idx },
            )
            .await
            .unwrap();
        assert_eq!(record.unwrap().slot, 45);
    }
    assert!(
        client
            .get_transaction_by_signature_and_position(
                &signature(500).to_string(),
                crate::clickhouse::SignatureSlot {
                    slot: 45,
                    slot_idx: 0
                },
            )
            .await
            .unwrap()
            .0
            .is_none()
    );
}

async fn assert_transaction_reads(
    client: &clickhouse::Client,
    source: &ClickHouseClient,
    cache: &DiskCache,
) {
    assert_transaction_positions(cache).await;
    assert_transaction_invalidation(cache).await;
    assert_transaction_append_race(client, cache).await;
    assert!(matches!(
        cache.get_tx(signature(500), Some(45)).await,
        DiskTransactionResult::Absent
    ));
    assert!(matches!(
        cache.get_tx(signature(500), Some(500)).await,
        DiskTransactionResult::Unavailable
    ));
    let mismatch = transaction_response(
        source,
        Some(cache),
        signature(45),
        serde_json::json!({"slot": 44}),
    )
    .await;
    assert!(mismatch["result"].is_null());
    assert!(mismatch.get("error").is_none());
    let database = &cache.inner.cfg.database;
    execute(
        client,
        &format!("RENAME TABLE {database}.transactions TO {database}.transactions_unavailable"),
    )
    .await;
    assert_transaction_fallback(source, cache).await;
    execute(
        client,
        &format!("RENAME TABLE {database}.transactions_unavailable TO {database}.transactions"),
    )
    .await;
}

async fn assert_absent_without_admission(cache: &DiskCache) {
    let client = cache.query_client();
    let _permits = client.http_query_sem.acquire_many(2).await.unwrap();
    tokio::time::timeout(Duration::from_millis(50), async {
        assert!(matches!(
            cache.get_tx(signature(500), None).await,
            DiskTransactionResult::Unavailable
        ));
        assert!(cache.signature_position(signature(500)).await.is_none());
        assert!(cache.get_sig_statuses(vec![signature(500)]).await[0].is_none());
    })
    .await
    .expect("complete negative must not wait for admission");
}
async fn assert_address_priority_recovery(cache: &DiskCache) {
    let mut repair = cache.begin_fill(25, 25);
    assert!(!cache.signature_indexes_ready());
    cache.build_key_indexes().await;
    assert!(
        cache
            .inner
            .key_index
            .may_contain(2, &[key_index::Family::Address], b"absent")
    );
    cache.update_signature_membership(&mut repair, 25, 25).await;
    drop(repair);
    assert!(cache.signature_indexes_ready());
    cache.build_key_indexes().await;
    assert!(
        !cache
            .inner
            .key_index
            .may_contain(2, &[key_index::Family::Address], b"absent")
    );
    assert_absent_without_admission(cache).await;
}

fn address(value: &str) -> Pubkey {
    let mut key = [0; 32];
    key[..value.len()].copy_from_slice(value.as_bytes());
    Pubkey::from(key)
}
pub(super) fn config(url: String, database: String) -> DiskCacheConfig {
    DiskCacheConfig {
        url,
        database,
        username: "default".into(),
        password: String::new(),
        required: false,
        retain_slots: 40,
        max_bytes: 0,
        partition_slots: 10,
        query_timeout: Duration::from_secs(2),
        get_tx_timeout: Duration::from_secs(2),
        fused_get_tx: true,
        get_tx_span_check: true,
        eviction_safe_hits: true,
        status_span_query: true,
        compact_transactions_parts: false,
        gsfa_race_primary: true,
        // Equal to get_tx_timeout: the unknown-membership gate is a no-op unless a test sets it.
        get_tx_unknown_timeout: Duration::from_secs(2),
        address_query_timeout: Duration::from_millis(100),
        gsfa_empty_watermark_ttl: Duration::ZERO,
        gsfa_empty_watermark_max_entries: 1,
        key_index_max_memory_bytes: 128 * 1024 * 1024,
        query_concurrency: 2,
        background_query_concurrency: 2,
        query_max_threads: 2,
        schema_check_interval: Duration::from_secs(300),
        memory_blocks_metadata: false,
        memory_retain_slots: None,
        memory_max_bytes: None,
        block_index: None,
    }
}
async fn execute(client: &clickhouse::Client, sql: &str) {
    client
        .query(sql)
        .execute()
        .await
        .unwrap_or_else(|e| panic!("{e}: {sql}"));
}
fn statements(sql: &str) -> Vec<&str> {
    let mut quote = false;
    let mut escaped = false;
    let mut start = 0;
    let mut result = Vec::new();
    for (offset, ch) in sql.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match ch {
            '\\' if quote => escaped = true,
            '\'' => quote = !quote,
            ';' if !quote => {
                result.push(&sql[start..offset]);
                start = offset + 1;
            }
            _ => {}
        }
    }
    if !sql[start..].trim().is_empty() {
        result.push(&sql[start..]);
    }
    result
}
async fn fixture(client: &clickhouse::Client, database: &str) {
    execute(client, &format!("CREATE DATABASE {database}")).await;
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../ddl/local");
    for table in [
        "transactions",
        "blocks_metadata",
        "signatures",
        "gsfa",
        "gsfa_hot",
        "token_owner_activity",
    ] {
        let sql = std::fs::read_to_string(root.join(format!("{table}.sql")))
            .unwrap()
            .replace("default.", &format!("{database}."));
        let sql = if table == "gsfa_hot" {
            sql.replace("% 32", "% 64").replace(
                "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                &address("address").to_string(),
            )
        } else {
            sql
        };
        for statement in statements(&sql) {
            execute(client, statement).await;
        }
    }
    execute(client, &format!("INSERT INTO {database}.blocks_metadata (slot,parent_slot,blockhash,parent_blockhash,block_time,block_height,executed_transaction_count) SELECT number,number-1,toFixedString('hash',32),toFixedString('hash',32),1700000000+number,number,1 FROM numbers(10,40)")).await;
}
async fn insert_transactions(client: &clickhouse::Client, database: &str) {
    execute(client, &format!("INSERT INTO {database}.transactions (signature, slot, slot_idx, tx_signatures, tx_account_keys, tx_num_required_signatures, meta_status_ok, meta_post_token_balances_present, meta_post_token_account_index, meta_post_token_mint, meta_post_token_owner, meta_post_token_program_id, meta_post_token_amount, meta_post_token_decimals, meta_post_token_ui_amount, meta_post_token_ui_amount_string) SELECT toFixedString(concat('sig-', toString(number)),64), number, 0, [toFixedString(concat('sig-', toString(number)),64),toFixedString(concat('secondary-', toString(number)),64)], [toFixedString('address',32),toFixedString('signer',32)], 2, 1, 1, [0], [toFixedString('mint',32)], [toFixedString('owner',32)], [toFixedString('program',32)], ['1'], [0], [1.0], ['1'] FROM numbers(10,40)")).await;
}
fn tfa(order: SortOrder, tokens: TokenAccountsFilter) -> index::DiskTfaQuery {
    index::DiskTfaQuery {
        limit: 100,
        sort_order: order,
        pagination: None,
        slot_filter: Some(NumericFilter::default()),
        block_time_filter: None,
        signature_filter: None,
        status: TransactionStatusFilter::Any,
        token_accounts: tokens,
    }
}

async fn assert_pagination(cache: &DiskCache) {
    for order in [SortOrder::Asc, SortOrder::Desc] {
        let mut query = tfa(order, TokenAccountsFilter::All);
        query.limit = 7;
        let mut slots = Vec::new();
        loop {
            let page = cache
                .transactions_for_address(address("owner"), query.clone())
                .await
                .unwrap();
            let Some(last) = page.records.last() else {
                break;
            };
            query.pagination = Some(crate::clickhouse::SignatureSlot {
                slot: last.slot,
                slot_idx: last.slot_idx,
            });
            slots.extend(page.records.iter().map(|r| r.slot));
            if page.records.len() < query.limit {
                break;
            }
        }
        let mut expected: Vec<_> = (10..50).collect();
        if order == SortOrder::Desc {
            expected.reverse();
        }
        assert_eq!(slots, expected);
    }
}

/// Bounded gsfa pages over several partitions, as one local query, must equal the
/// unbounded page filtered by the same bounds: order, limit and exclusive edges.
async fn assert_gsfa_bounds_across_partitions(cache: &DiskCache) {
    use crate::clickhouse::{SignatureSlot, SlotBoundary};
    let all = cache
        .signatures_for_address(address("address"), None, None, 1000)
        .await
        .unwrap()
        .records;
    let position = |slot| {
        let row = all.iter().find(|row| row.slot == slot).unwrap();
        SlotBoundary::Position(SignatureSlot {
            slot,
            slot_idx: row.slot_idx,
        })
    };
    let key = |row: &crate::clickhouse::SignatureRecord| (row.slot, row.slot_idx);
    let newer_than = |row, bound: Option<SlotBoundary>| match bound {
        None => true,
        Some(SlotBoundary::Slot(slot)) => key(row).0 > slot,
        Some(SlotBoundary::Position(p)) => key(row) > (p.slot, p.slot_idx),
    };
    let older_than = |row, bound: Option<SlotBoundary>| match bound {
        None => true,
        Some(SlotBoundary::Slot(slot)) => key(row).0 < slot,
        Some(SlotBoundary::Position(p)) => key(row) < (p.slot, p.slot_idx),
    };
    let befores = [None, Some(position(37)), Some(SlotBoundary::Slot(33))];
    let untils = [None, Some(position(12)), Some(SlotBoundary::Slot(21))];
    for before in befores {
        for until in untils {
            for limit in [1, 5, 15, 100] {
                let expected: Vec<_> = all
                    .iter()
                    .filter(|row| older_than(row, before) && newer_than(row, until))
                    .take(limit)
                    .map(|row| (row.slot, row.slot_idx, row.signature.clone()))
                    .collect();
                let page = cache
                    .signatures_for_address(address("address"), before, until, limit)
                    .await
                    .unwrap_or_else(|| panic!("no page for {before:?} {until:?} {limit}"));
                let actual: Vec<_> = page
                    .records
                    .iter()
                    .map(|row| (row.slot, row.slot_idx, row.signature.clone()))
                    .collect();
                assert_eq!(
                    actual, expected,
                    "before={before:?} until={until:?} limit={limit}"
                );
                // Both `until` bounds lie inside coverage, so only unbounded pages reach it.
                assert_eq!(
                    page.reached_floor,
                    expected.len() < limit && until.is_none()
                );
            }
        }
    }
}

async fn assert_transaction_position_fallback(cache: &DiskCache) {
    let mut client = cache.query_client();
    client.cache_partition = Some((10, 1));
    // The fallback must reuse the first query's permit instead of deadlocking at capacity one.
    client.http_query_sem = Arc::new(tokio::sync::Semaphore::new(1));
    let position = crate::clickhouse::SignatureSlot {
        slot: 15,
        slot_idx: 99,
    };
    let (record, _) = client
        .get_transaction_by_signature_and_position(&signature(15).to_string(), position)
        .await
        .unwrap();
    let record = record.unwrap();
    assert_eq!((record.slot, record.slot_idx), (15, 0));
    let (missing, _) = client
        .get_transaction_by_signature_and_position(&signature(500).to_string(), position)
        .await
        .unwrap();
    assert!(missing.is_none());
    let (wrong_slot, _) = client
        .get_transaction_by_signature_and_position(&signature(25).to_string(), position)
        .await
        .unwrap();
    assert!(wrong_slot.is_none());
}
async fn assert_cross_partition_duplicates(client: &clickhouse::Client, cache: &DiskCache) {
    let _mutation = cache.begin_fill(39, 39);
    execute(client, &format!("INSERT INTO {}.transactions (signature,slot,slot_idx,tx_account_keys,meta_status_ok) SELECT signature,39,0,tx_account_keys,1 FROM {}.transactions WHERE slot=49 LIMIT 1", cache.inner.cfg.database,cache.inner.cfg.database)).await;
    cache
        .publish_range_coverage(vec![(39, SlotStatus::Covered { tx_count: 2 })])
        .await
        .unwrap();
    drop(_mutation);
    let mut query = tfa(SortOrder::Desc, TokenAccountsFilter::None);
    query.limit = 15;
    let page = cache
        .transactions_for_address(address("address"), query)
        .await
        .unwrap();
    assert_eq!(
        page.records.iter().map(|r| r.slot).collect::<Vec<_>>(),
        (35..50).rev().collect::<Vec<_>>()
    );
}

async fn wait_for_query(client: &clickhouse::Client, id: &str, running: bool) {
    for _ in 0..50 {
        let count = client
            .query("SELECT count() FROM system.processes WHERE query_id=?")
            .bind(id)
            .fetch_one::<u64>()
            .await
            .unwrap();
        if (count > 0) == running {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("query running state did not become {running}");
}
async fn assert_cancellation(client: &clickhouse::Client, cache: &DiskCache) {
    let mut lookup = cache.query_client();
    lookup.cache_partition = Some((10, 1));
    let query = lookup.read_query(
        "SELECT sum(cityHash64(number)) FROM numbers(1000000000000) SETTINGS max_execution_time=2,max_threads=1",
        "test_cache_cancel",
    ).await.unwrap();
    let id = query.query_id().to_owned();
    let task = tokio::spawn(query.fetch_one::<u64>());
    wait_for_query(client, &id, true).await;
    task.abort();
    let _ = task.await;
    wait_for_query(client, &id, false).await;
}

async fn assert_signature_fill_updates(cache: &DiskCache, source: &ClickHouseClient) {
    // First fill, append within the same partition, then repair covered slots.
    for (start, end) in [(10, 14), (15, 19), (15, 19)] {
        filler::fill_range(
            cache,
            source,
            filler::SlotRange { start, end },
            &filler::FillerConfig::default(),
        )
        .await
        .unwrap();
        assert!(signature_candidate(cache, 1, signature(10)));
        assert!(signature_candidate(
            cache,
            1,
            named_signature("secondary", end)
        ));
        assert_eq!(
            cache
                .signature_position(named_signature("secondary", end))
                .await
                .unwrap()
                .slot,
            end
        );
        assert_eq!(
            cache
                .get_sig_statuses(vec![named_signature("secondary", end)])
                .await[0]
                .as_ref()
                .unwrap()
                .slot,
            end
        );
        assert_absent_without_admission(cache).await;
    }
}

async fn assert_migration(
    client: &clickhouse::Client,
    source: &ClickHouseClient,
    cfg: &DiskCacheConfig,
) {
    let mut cfg = cfg.clone();
    cfg.database.push_str("_migration");
    let cache = DiskCache::open(cfg.clone(), source).await.unwrap();
    let views = client
        .query(&format!("SELECT name FROM system.tables WHERE database='{}' AND endsWith(name, '__mv') ORDER BY name", cfg.database))
        .fetch_all::<String>().await.unwrap();
    let mut view_ddl = Vec::new();
    for name in &views {
        view_ddl.push(
            client
                .query(&format!("SHOW CREATE TABLE {}.{name}", cfg.database))
                .fetch_one::<String>()
                .await
                .unwrap(),
        );
        execute(client, &format!("DROP TABLE {}.{name} SYNC", cfg.database)).await;
    }
    let old_ddl = client
        .query(&format!("SHOW CREATE TABLE {}.transactions", cfg.database))
        .fetch_one::<String>()
        .await
        .unwrap()
        .replace("index_granularity = 64", "index_granularity = 8192")
        .replace(
            "min_compress_block_size = 16384",
            "min_compress_block_size = 65536",
        )
        .replace(
            "max_compress_block_size = 65536",
            "max_compress_block_size = 1048576",
        );
    execute(
        client,
        &format!("DROP TABLE {}.transactions SYNC", cfg.database),
    )
    .await;
    execute(client, &old_ddl).await;
    for ddl in view_ddl {
        execute(client, &ddl).await;
    }
    insert_transactions(client, &cfg.database).await;
    cache
        .publish_range_coverage(vec![(15, SlotStatus::Covered { tx_count: 1 })])
        .await
        .unwrap();
    assert!(cache.covers_slot(15));
    execute(
        client,
        &format!(
            "ALTER TABLE {}._cache_meta UPDATE value='previous-payload-layout' WHERE key='fingerprint' SETTINGS mutations_sync=2",
            cfg.database
        ),
    )
    .await;
    let reopened = DiskCache::open(cfg.clone(), source).await.unwrap();
    let ddl = client
        .query(&format!("SHOW CREATE TABLE {}.transactions", cfg.database))
        .fetch_one::<String>()
        .await
        .unwrap();
    for setting in [
        "index_granularity = 64",
        "index_granularity_bytes = 10485760",
        "min_compress_block_size = 16384",
        "max_compress_block_size = 65536",
    ] {
        assert!(ddl.contains(setting), "{ddl}");
    }
    let count = reopened
        .inner
        .local
        .client
        .query("SELECT count() AS count FROM signatures")
        .fetch_one::<u64>()
        .await
        .unwrap();
    assert_eq!(count, 0);
    assert!(reopened.tip_span().is_none());
    assert_signature_fill_updates(&reopened, source).await;
    assert!(reopened.covers_slot(15));
    assert_eq!(
        found_transaction(reopened.get_tx(signature(15), None).await).slot,
        15
    );
    let restarted = DiskCache::open(cfg.clone(), source).await.unwrap();
    assert_eq!(
        found_transaction(restarted.get_tx(signature(15), None).await).slot,
        15
    );
    execute(
        client,
        &format!(
            "ALTER TABLE {}._cache_meta UPDATE value='4' WHERE key='format_version' SETTINGS mutations_sync=2",
            cfg.database
        ),
    )
    .await;
    assert_resumable_rebuild(&restarted, &cfg).await;
    let restarted = DiskCache::open(cfg.clone(), source).await.unwrap();
    assert_signature_fill_updates(&restarted, source).await;
    execute(
        client,
        &format!("DROP TABLE {}._cache_meta SYNC", cfg.database),
    )
    .await;
    let error = DiskCache::open(cfg.clone(), source)
        .await
        .err()
        .expect("missing marker must fail closed");
    assert!(error.to_string().contains("ownership check failed"));
    assert_eq!(
        found_transaction(restarted.get_tx(signature(15), None).await).slot,
        15
    );
    drop(cache);
    execute(client, &format!("DROP DATABASE {} SYNC", cfg.database)).await;
}
async fn assert_resumable_rebuild(cache: &DiskCache, cfg: &DiskCacheConfig) {
    let mut admin = cache.inner.admin.clone();
    admin.client = admin.client.with_setting("max_table_size_to_drop", "1");
    let table = format!("{}.gsfa", cfg.database);
    let error = admin
        .client
        .query(&format!("DROP TABLE {table} SYNC"))
        .execute()
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT")
    );
    let marker_query = format!(
        "SELECT toString(uuid) FROM system.tables WHERE database='{}' AND name='_cache_meta'",
        cfg.database
    );
    let marker = admin
        .client
        .query(&marker_query)
        .fetch_one::<String>()
        .await
        .unwrap();
    // Fail replacement DDL after deletion, then retry with the real schema.
    let mut broken = (*cache.source_schema()).clone();
    broken.tables.clear();
    assert!(
        schema::initialize_cache_schema(&admin, &broken, &cfg.schema_config())
            .await
            .is_err()
    );
    assert_eq!(
        admin
            .client
            .query(&marker_query)
            .fetch_one::<String>()
            .await
            .unwrap(),
        marker
    );
    assert_eq!(
        schema::initialize_cache_schema(&admin, &cache.source_schema(), &cfg.schema_config())
            .await
            .unwrap(),
        schema::SchemaBootstrap::Rebuilt
    );
    assert_eq!(
        schema::initialize_cache_schema(&admin, &cache.source_schema(), &cfg.schema_config())
            .await
            .unwrap(),
        schema::SchemaBootstrap::Reused
    );
    assert_eq!(
        admin
            .client
            .query(&marker_query)
            .fetch_one::<String>()
            .await
            .unwrap(),
        marker
    );
}

async fn select_queries(client: &clickhouse::Client) -> u64 {
    client
        .query("SELECT value FROM system.events WHERE event = 'SelectQuery'")
        .fetch_one::<u64>()
        .await
        .unwrap()
}

/// Local SELECTs issued by `read`, excluding the counter probes themselves.
async fn local_selects<T>(client: &clickhouse::Client, read: impl Future<Output = T>) -> (T, u64) {
    let probe = select_queries(client).await;
    let before = select_queries(client).await;
    let value = Box::pin(read).await;
    let after = select_queries(client).await;
    (value, (after - before) - (before - probe))
}

async fn explain(client: &clickhouse::Client, query: &str) -> String {
    let explain = client
        .query(&format!("EXPLAIN indexes=1 {query}"))
        .fetch_all::<String>()
        .await
        .unwrap()
        .join("\n");
    eprintln!("EXPLAIN indexes=1 {query}\n{explain}");
    // Plan indentation and tree glyphs differ across server versions.
    explain.split_whitespace().collect::<Vec<_>>().join(" ")
}

async fn assert_fused_transaction(client: &clickhouse::Client, cache: &DiskCache) {
    let database = &cache.inner.cfg.database;
    let bytes = signature(15);
    let literal = format!(
        "toFixedString(unhex('{}'), 64)",
        hex::encode(bytes.as_ref()).to_uppercase()
    );
    let bucket = ch_cityhash102::cityhash64(bytes.as_ref())
        % cache.query_client().signatures_bucket_modulus();
    let query = crate::clickhouse::build_fused_transaction_query(
        &format!("{database}.transactions"),
        &format!("{database}.signatures"),
        bucket,
        &literal,
        (10, 49),
        "",
    );
    // The IN set drives payload primary-key analysis; the position lookup is keyed.
    let outer = explain(client, &query).await;
    assert!(
        outer.contains("(slot, slot_idx) in 1-element set"),
        "{outer}"
    );
    assert!(outer.contains("Search Algorithm: binary search"), "{outer}");
    let (_, inner) = query.split_once("IN (").unwrap();
    let (inner, _) = inner.rsplit_once(')').unwrap();
    let inner = explain(client, inner).await;
    assert!(
        inner.contains("PrimaryKey Keys: sig_bucket signature"),
        "{inner}"
    );
    assert!(inner.contains("Search Algorithm: binary search"), "{inner}");

    // A hit is one local query.
    let (found, selects) = local_selects(client, cache.get_tx(signature(25), None)).await;
    assert_eq!(found_transaction(found).slot, 25);
    assert_eq!(selects, 1);

    // A stale newest position yields no fused row; the legacy retry still finds it.
    execute(client, &format!("INSERT INTO {database}.signatures (signature,slot,slot_idx) VALUES (toFixedString('sig-35',64),35,99)")).await;
    let (found, selects) = local_selects(client, cache.get_tx(signature(35), Some(35))).await;
    let found = found_transaction(found);
    assert_eq!((found.slot, found.slot_idx), (35, 0));
    assert_eq!(selects, 4, "fused, position, exact payload, legacy payload");
    execute(client, &format!("ALTER TABLE {database}.signatures DELETE WHERE slot_idx = 99 SETTINGS mutations_sync = 2")).await;

    // The newest row sits in a skipped partition inside the span: the per-partition
    // search never reads it, so neither may the fused read.
    execute(client, &format!("INSERT INTO {database}.transactions (signature,slot,slot_idx,tx_signatures) VALUES (toFixedString('sig-15',64),35,5,[toFixedString('sig-15',64)])")).await;
    assert!(!signature_candidate(cache, 3, signature(15)));
    let repair = cache.begin_fill(45, 45);
    let (found, selects) = local_selects(client, cache.get_tx(signature(15), None)).await;
    assert_eq!(found_transaction(found).slot, 15);
    assert!(selects > 1, "rejected fused row must fall back");
    drop(repair);
    for table in ["transactions", "signatures"] {
        execute(client, &format!("ALTER TABLE {database}.{table} DELETE WHERE slot = 35 AND slot_idx = 5 SETTINGS mutations_sync = 2")).await;
    }
    cache.build_signature_indexes().await;
    assert!(cache.signature_indexes_ready());

    // Candidates without rows (as for a Bloom false positive) are not absence by
    // themselves: the pinned slot decides, as after the per-partition probes. With
    // no position anywhere in the span, one span lookup stands in for those probes.
    cache.inner.key_index.clear_signatures();
    let (absent, selects) = local_selects(client, cache.get_tx(signature(500), Some(45))).await;
    assert!(matches!(absent, DiskTransactionResult::Absent));
    assert_eq!(
        selects, 3,
        "fused, span position lookup, pinned slot status"
    );
    for requested in [None, Some(500)] {
        let (miss, selects) = local_selects(client, cache.get_tx(signature(500), requested)).await;
        assert!(matches!(miss, DiskTransactionResult::Unavailable));
        assert_eq!(selects, 2, "fused, span position lookup");
    }
    // A position in the span whose payload does not match (here stale) still takes
    // the per-partition probes, newest first: partition 4 is empty, 3 holds it.
    execute(client, &format!("INSERT INTO {database}.signatures (signature,slot,slot_idx) VALUES (toFixedString('sig-35',64),35,99)")).await;
    let (found, selects) = local_selects(client, cache.get_tx(signature(35), None)).await;
    let found = found_transaction(found);
    assert_eq!((found.slot, found.slot_idx), (35, 0));
    assert_eq!(
        selects, 6,
        "fused, span, two positions, exact payload, legacy payload"
    );
    execute(client, &format!("ALTER TABLE {database}.signatures DELETE WHERE slot_idx = 99 SETTINGS mutations_sync = 2")).await;
    cache.build_signature_indexes().await;
    assert!(cache.signature_indexes_ready());
}

/// DISK_CACHE_FUSED_GET_TX=false: a hit is the two-step lookup (position, then payload)
/// that ran before the fused read.
async fn assert_two_step_transaction(
    client: &clickhouse::Client,
    source: &ClickHouseClient,
    cfg: &DiskCacheConfig,
) {
    let mut cfg = cfg.clone();
    cfg.fused_get_tx = false;
    let cache = Box::pin(DiskCache::open(cfg, source)).await.unwrap();
    Box::pin(cache.build_key_indexes()).await;
    Box::pin(cache.build_signature_indexes()).await;
    assert!(cache.signature_indexes_ready());
    let (found, selects) = local_selects(client, cache.get_tx(signature(25), None)).await;
    assert_eq!(found_transaction(found).slot, 25);
    assert_eq!(selects, 2, "position, then payload");
}

/// DISK_CACHE_GET_TX_SPAN_CHECK=false: an empty fused read over unknown-membership
/// partitions takes the per-partition probes again instead of one span lookup.
async fn assert_span_check_off(
    client: &clickhouse::Client,
    source: &ClickHouseClient,
    cfg: &DiskCacheConfig,
) {
    let mut cfg = cfg.clone();
    cfg.get_tx_span_check = false;
    let cache = Box::pin(DiskCache::open(cfg, source)).await.unwrap();
    Box::pin(cache.build_key_indexes()).await;
    Box::pin(cache.build_signature_indexes()).await;
    assert!(cache.signature_indexes_ready());
    cache.inner.key_index.clear_signatures();
    let (miss, selects) = local_selects(client, cache.get_tx(signature(500), None)).await;
    assert!(matches!(miss, DiskTransactionResult::Unavailable));
    // With the switch on this is 2 (fused, span lookup); off it is the fused read plus
    // one position probe per candidate partition, as before the span check.
    eprintln!("span check off: {selects} local selects");
    assert!(
        selects > 2,
        "fused read plus per-partition probes, got {selects}"
    );
    Box::pin(cache.build_signature_indexes()).await;
    assert!(cache.signature_indexes_ready());
}

async fn assert_pruning(client: &clickhouse::Client, database: &str) {
    let query = format!(
        "EXPLAIN indexes=1 SELECT slot FROM {database}.signatures PREWHERE signature=toFixedString('sig-15',64) AND intDiv(slot,10)=1"
    );
    let explain = client
        .query(&query)
        .fetch_all::<String>()
        .await
        .unwrap()
        .join("\n");
    assert!(explain.contains("Parts: 1/4"), "{explain}");
    let ddl = client
        .query(&format!("SHOW CREATE TABLE {database}.signatures"))
        .fetch_one::<String>()
        .await
        .unwrap();
    assert!(ddl.contains("slot DESC"), "{ddl}");
    assert!(ddl.contains("index_granularity = 512"), "{ddl}");
}

#[tokio::test]
#[ignore = "requires disposable ClickHouse: DISK_CACHE_TEST_URL=http://127.0.0.1:18123"]
async fn key_routing_clickhouse_integration() {
    let url = std::env::var("DISK_CACHE_TEST_URL").expect("explicit disposable ClickHouse URL");
    assert!(url.starts_with("http://127.0.0.1:"));
    let database = format!("test_key_router_{}", now_version());
    let cache_database = format!("{database}_cache");
    let client = clickhouse::Client::default()
        .with_url(&url)
        .with_user("default");
    fixture(&client, &database).await;
    insert_transactions(&client, &database).await;
    let mut source = ClickHouseClient::new(
        &url,
        &database,
        "default",
        "",
        ClickHouseClientOptions::new(
            RoutingPolicy {
                transport: RoutingTransport::Http,
                scope: RoutingScope::Distributed,
            },
            None,
            vec![address("address").to_string()],
            format!("{database}.gsfa_hot"),
            format!("{database}.gsfa_hot"),
        ),
    );
    source.verification_timeouts = crate::clickhouse::verification::VerificationTimeouts {
        startup: Duration::from_millis(1234),
        runtime: Duration::from_millis(2345),
    };
    source.use_table_names(ClickHouseTableNames::in_database(&database));
    let cfg = config(url, cache_database.clone());
    let cache = DiskCache::open(cfg.clone(), &source).await.unwrap();
    assert_eq!(
        cache.inner.admin.read_endpoint.verification_timeouts(),
        source.verification_timeouts
    );
    assert_eq!(
        cache.inner.local.read_endpoint.verification_timeouts(),
        source.verification_timeouts
    );

    for reader in [
        &cache.inner.maintenance_reader,
        &cache.inner.address_index_reader,
        &cache.inner.signature_index_reader,
    ] {
        assert_eq!(
            reader.read_endpoint.verification_timeouts(),
            source.verification_timeouts
        );
    }
    let lanes_database = format!("{cache_database}_lanes");
    {
        // Raising serving concurrency leaves the background lane at its own size.
        let mut lanes_cfg = cfg.clone();
        lanes_cfg.database = lanes_database.clone();
        lanes_cfg.query_concurrency = 16;
        lanes_cfg.background_query_concurrency = 8;
        let lanes = DiskCache::open(lanes_cfg, &source).await.unwrap();
        assert_eq!(lanes.inner.local.read_endpoint.available_permits(), 16);
        assert_eq!(
            lanes
                .inner
                .maintenance_reader
                .read_endpoint
                .available_permits(),
            8
        );
        assert_eq!(
            lanes
                .inner
                .address_index_reader
                .read_endpoint
                .available_permits(),
            1
        );
        assert_eq!(
            lanes
                .inner
                .signature_index_reader
                .read_endpoint
                .available_permits(),
            1
        );
        drop(lanes);
    }
    insert_transactions(&client, &cache_database).await;
    cache
        .publish_range_coverage(
            (10..50)
                .map(|slot| (slot, SlotStatus::Covered { tx_count: 1 }))
                .collect(),
        )
        .await
        .unwrap();
    cache.build_key_indexes().await;
    assert!(
        cache
            .inner
            .key_index
            .may_contain(2, &[key_index::Family::Address], b"absent")
    );
    assert!(!cache.signature_indexes_ready());
    cache.build_signature_indexes().await;
    assert!(cache.signature_indexes_ready());
    cache.build_key_indexes().await;
    assert!(
        !cache
            .inner
            .key_index
            .may_contain(2, &[key_index::Family::Address], b"absent")
    );
    assert_address_priority_recovery(&cache).await;
    assert_absent_without_admission(&cache).await;
    assert!(!signature_candidate(&cache, 1, signature(35)));
    assert!(signature_candidate(&cache, 1, signature(15)));
    assert_eq!(
        cache.signature_position(signature(15)).await.unwrap().slot,
        15
    );
    assert_eq!(
        found_transaction(cache.get_tx(signature(15), None).await).slot,
        15
    );
    assert!(cache.signature_position(signature(500)).await.is_none());
    let statuses = cache
        .get_sig_statuses(vec![
            signature(15),
            signature(500),
            signature(15),
            signature(45),
        ])
        .await;
    assert_eq!(
        statuses
            .iter()
            .map(|s| s.as_ref().map(|s| s.slot))
            .collect::<Vec<_>>(),
        [Some(15), None, Some(15), Some(45)]
    );
    let page = cache
        .signatures_for_address(address("address"), None, None, 100)
        .await
        .unwrap();
    assert_eq!(
        page.records.iter().map(|r| r.slot).collect::<Vec<_>>(),
        (10..50).rev().collect::<Vec<_>>()
    );
    assert_gsfa_bounds_across_partitions(&cache).await;
    for (key, tokens) in [
        ("address", TokenAccountsFilter::None),
        ("owner", TokenAccountsFilter::All),
        ("owner", TokenAccountsFilter::BalanceChanged),
    ] {
        let page = cache
            .transactions_for_address(address(key), tfa(SortOrder::Asc, tokens))
            .await
            .unwrap();
        assert_eq!(
            page.records.iter().map(|r| r.slot).collect::<Vec<_>>(),
            (10..50).collect::<Vec<_>>()
        );
    }
    let mut scoped = cache.query_client();
    scoped.cache_partition = Some((10, 2));
    assert!(
        scoped
            .get_signature_slot(&signature(15).to_string())
            .await
            .unwrap()
            .0
            .is_none()
    );
    assert_eq!(
        cache
            .query_client()
            .get_signature_slot(&signature(15).to_string())
            .await
            .unwrap()
            .0
            .unwrap()
            .slot,
        15
    );
    assert_transaction_position_fallback(&cache).await;
    assert_pagination(&cache).await;
    assert_pruning(&client, &cache_database).await;
    let fused_client = client.clone();
    let fused_cache = cache.clone();
    let (two_step_source, two_step_cfg) = (source.clone(), cfg.clone());
    tokio::spawn(async move {
        // Boxed so the test body stays under the 2 MiB debug test-thread stack.
        Box::pin(assert_fused_transaction(&fused_client, &fused_cache)).await;
        Box::pin(assert_two_step_transaction(
            &fused_client,
            &two_step_source,
            &two_step_cfg,
        ))
        .await;
        Box::pin(assert_span_check_off(
            &fused_client,
            &two_step_source,
            &two_step_cfg,
        ))
        .await;
    })
    .await
    .unwrap();
    // Poll migration separately so nested debug lookup futures do not consume
    // the fixture task's stack as well as their own.
    let migration_client = client.clone();
    let migration_source = source.clone();
    let migration_cfg = cfg.clone();
    tokio::spawn(async move {
        Box::pin(assert_migration(
            &migration_client,
            &migration_source,
            &migration_cfg,
        ))
        .await;
    })
    .await
    .unwrap();
    assert_cancellation(&client, &cache).await;
    assert_cross_partition_duplicates(&client, &cache).await;
    let tx_client = client.clone();
    let tx_source = source.clone();
    let tx_cache = cache.clone();
    tokio::spawn(async move {
        Box::pin(assert_transaction_reads(&tx_client, &tx_source, &tx_cache)).await;
    })
    .await
    .unwrap();
    cache.poison_slot(15).await;
    assert!(matches!(
        cache.get_tx(signature(15), None).await,
        DiskTransactionResult::Unavailable
    ));
    assert!(!signature_candidate(&cache, 1, signature(500)));
    let (budget_client, budget_source, budget_cfg, revalidation_cache) =
        (client.clone(), source.clone(), cfg.clone(), cache.clone());
    // Boxed so the spawned block stays small inside this already large test body.
    tokio::spawn(async move {
        Box::pin(assert_get_tx_budget(&budget_source, &budget_cfg)).await;
        Box::pin(assert_get_tx_unknown_budget(
            &budget_client,
            &budget_source,
            &budget_cfg,
        ))
        .await;
        Box::pin(assert_transaction_read_revalidation(&revalidation_cache)).await;
    })
    .await
    .unwrap();
    let mut reopened_cfg = cfg;
    reopened_cfg.query_timeout = Duration::from_millis(50);
    let reopened = DiskCache::open(reopened_cfg, &source).await.unwrap();
    assert!(
        reopened
            .query_client()
            .is_gsfa_hot_address(&address("address"))
    );
    let hot_page = reopened
        .signatures_for_address(address("address"), None, None, 3)
        .await
        .unwrap();
    assert_eq!(
        hot_page
            .records
            .iter()
            .map(|row| row.slot)
            .collect::<Vec<_>>(),
        [49, 48, 47]
    );
    let _permits = reopened
        .inner
        .local
        .http_query_sem
        .acquire_many(2)
        .await
        .unwrap();
    let started = std::time::Instant::now();
    assert!(matches!(
        reopened.get_tx(signature(45), None).await,
        DiskTransactionResult::Unavailable
    ));
    assert!(started.elapsed() < Duration::from_millis(200));
    assert!(matches!(
        reopened.slot_status(45).await,
        SlotStatus::Covered { .. }
    ));
    let tx_source = source.clone();
    let tx_cache = reopened.clone();
    tokio::spawn(async move {
        Box::pin(assert_transaction_fallback(&tx_source, &tx_cache)).await;
    })
    .await
    .unwrap();
    drop(_permits);
    if std::env::var_os("DISK_CACHE_TEST_KEEP").is_some() {
        eprintln!("Kept source database {database} and caches {cache_database}, {lanes_database}");
        return;
    }
    execute(&client, &format!("DROP DATABASE {lanes_database} SYNC")).await;
    execute(&client, &format!("DROP DATABASE {cache_database} SYNC")).await;
    execute(&client, &format!("DROP DATABASE {database} SYNC")).await;
}

mod diagnostics;

mod address_latency;

mod address_budget;

mod gsfa_cursor_watermark;

mod part_layout;

mod status_span;

#[cfg(feature = "grpc-head-cache")]
mod status_history;
