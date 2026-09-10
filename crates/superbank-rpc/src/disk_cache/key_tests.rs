// SPDX-License-Identifier: AGPL-3.0-only
//! Opt-in integration against a disposable, loopback ClickHouse server.
use super::*;
use crate::clickhouse::{NumericFilter, SortOrder, TransactionStatusFilter};
use crate::solana_sdk::{pubkey::Pubkey, signature::Signature};

fn signature(slot: u64) -> Signature {
    let mut key = [0; 64];
    let value = format!("sig-{slot}");
    key[..value.len()].copy_from_slice(value.as_bytes());
    Signature::from(key)
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
        key_index_max_memory_bytes: 128 * 1024 * 1024,
        query_concurrency: 2,
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
    execute(client, &format!("INSERT INTO {database}.transactions (signature, slot, slot_idx, tx_signatures, tx_account_keys, tx_num_required_signatures, meta_status_ok, meta_post_token_balances_present, meta_post_token_account_index, meta_post_token_mint, meta_post_token_owner, meta_post_token_program_id, meta_post_token_amount, meta_post_token_decimals, meta_post_token_ui_amount, meta_post_token_ui_amount_string) SELECT toFixedString(concat('sig-', toString(number)),64), number, 0, [toFixedString(concat('sig-', toString(number)),64)], [toFixedString('address',32)], 1, 1, 1, [0], [toFixedString('mint',32)], [toFixedString('owner',32)], [toFixedString('program',32)], ['1'], [0], [1.0], ['1'] FROM numbers(10,40)")).await;
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
        .get_cached_transaction_by_position(&signature(15).to_string(), position)
        .await
        .unwrap();
    let record = record.unwrap();
    assert_eq!((record.slot, record.slot_idx), (15, 0));
    let (missing, _) = client
        .get_cached_transaction_by_position(&signature(500).to_string(), position)
        .await
        .unwrap();
    assert!(missing.is_none());
    let (wrong_slot, _) = client
        .get_cached_transaction_by_position(&signature(25).to_string(), position)
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
    let (sql,id,cleanup)=lookup.annotate_lookup_query("SELECT sum(cityHash64(number)) FROM numbers(1000000000000) SETTINGS max_execution_time=2,max_threads=1".into(),"test_cache_cancel");
    let id = id.unwrap();
    let query_client = lookup.client.clone().with_setting("query_id", id.clone());
    let task = tokio::spawn(async move {
        let _cleanup = cleanup;
        query_client.query(&sql).execute().await
    });
    wait_for_query(client, &id, true).await;
    task.abort();
    let _ = task.await;
    wait_for_query(client, &id, false).await;
}

async fn assert_migration(
    client: &clickhouse::Client,
    source: &ClickHouseClient,
    cfg: &DiskCacheConfig,
) {
    let mut cfg = cfg.clone();
    cfg.database.push_str("_migration");
    let cache = DiskCache::open(cfg.clone(), source).await.unwrap();
    insert_transactions(client, &cfg.database).await;
    execute(
        client,
        &format!(
            "ALTER TABLE {}._cache_meta UPDATE value='4' WHERE key='format_version' SETTINGS mutations_sync=2",
            cfg.database
        ),
    )
    .await;
    assert_resumable_rebuild(&cache, &cfg).await;
    let reopened = DiskCache::open(cfg.clone(), source).await.unwrap();
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
    filler::fill_range(
        &reopened,
        source,
        filler::SlotRange { start: 10, end: 19 },
        &filler::FillerConfig::default(),
    )
    .await
    .unwrap();
    assert!(reopened.covers_slot(15));
    assert_eq!(reopened.get_tx(signature(15)).await.unwrap().slot, 15);
    let restarted = DiskCache::open(cfg.clone(), source).await.unwrap();
    assert_eq!(restarted.get_tx(signature(15)).await.unwrap().slot, 15);
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
    assert_eq!(restarted.get_tx(signature(15)).await.unwrap().slot, 15);
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
    assert!(
        schema::initialize_cache_schema(&admin, &cache.source_schema(), &cfg.schema_config())
            .await
            .unwrap()
    );
    assert!(
        !schema::initialize_cache_schema(&admin, &cache.source_schema(), &cfg.schema_config())
            .await
            .unwrap()
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
    source.use_table_names(ClickHouseTableNames::in_database(&database));
    let cfg = config(url, cache_database.clone());
    let cache = DiskCache::open(cfg.clone(), &source).await.unwrap();
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
    assert!(!cache.inner.key_index.may_contain(
        1,
        &[key_index::Family::Signature],
        signature(35).as_ref()
    ));
    assert!(cache.inner.key_index.may_contain(
        1,
        &[key_index::Family::Signature],
        signature(15).as_ref()
    ));
    assert_eq!(
        cache.signature_position(signature(15)).await.unwrap().slot,
        15
    );
    assert_eq!(cache.get_tx(signature(15)).await.unwrap().slot, 15);
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
    assert_migration(&client, &source, &cfg).await;
    assert_cancellation(&client, &cache).await;
    assert_cross_partition_duplicates(&client, &cache).await;
    cache.poison_slot(15).await;
    assert!(cache.get_tx(signature(15)).await.is_none());
    assert!(cache.inner.key_index.may_contain(
        1,
        &[key_index::Family::Signature],
        signature(500).as_ref()
    ));
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
    assert!(reopened.get_tx(signature(45)).await.is_none());
    assert!(started.elapsed() < Duration::from_millis(200));
    drop(_permits);
    if std::env::var_os("DISK_CACHE_TEST_KEEP").is_some() {
        eprintln!("Kept source database {database} and cache {cache_database}");
        return;
    }
    execute(&client, &format!("DROP DATABASE {cache_database} SYNC")).await;
    execute(&client, &format!("DROP DATABASE {database} SYNC")).await;
}
