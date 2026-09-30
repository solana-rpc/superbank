// SPDX-License-Identifier: AGPL-3.0-only
//! Compact transactions parts: applied on open without a rebuild, and point reads
//! agree across Wide, Compact and merged parts.
use super::*;

const COMPACT: &str = "min_bytes_for_wide_part = 1099511627776";

async fn insert_rows(client: &clickhouse::Client, database: &str, first: u64, count: u64) {
    execute(client, &format!("INSERT INTO {database}.transactions (signature,slot,slot_idx,block_time,tx_version,tx_signatures,tx_account_keys,tx_num_required_signatures,meta_status_ok,meta_err,meta_pre_balances,meta_post_balances,meta_log_messages_present,meta_log_messages) SELECT toFixedString(concat('sig-',toString(number)),64),number,0,toInt64(1700000000+number),if(number%2=0,NULL,toUInt8(0)),[toFixedString(concat('sig-',toString(number)),64)],[toFixedString('address',32),toFixedString(concat('key-',toString(number)),32)],1,number%3!=0,if(number%3=0,'failed',NULL),[number,1],[number+1,1],1,arrayMap(i->concat('log-',toString(number),'-',toString(i)),range(number%6)) FROM numbers({first},{count})")).await;
}

async fn part_types(client: &clickhouse::Client, database: &str) -> Vec<String> {
    client
        .query(&format!("SELECT part_type FROM system.parts WHERE database='{database}' AND table='transactions' AND active ORDER BY min_block_number"))
        .fetch_all::<String>()
        .await
        .unwrap()
}

async fn point_row(client: &clickhouse::Client, database: &str, slot: u64) -> String {
    client
        .query(&format!("SELECT formatRow('JSONEachRow', *) FROM {database}.transactions PREWHERE slot = {slot} AND slot_idx = 0 AND signature = toFixedString('sig-{slot}',64) LIMIT 1"))
        .fetch_one::<String>()
        .await
        .unwrap()
}

async fn cached_records(cache: &DiskCache) -> Vec<String> {
    let mut records = Vec::new();
    for slot in 10..20 {
        let record = found_transaction(cache.get_tx(signature(slot), None).await);
        assert_eq!(record.slot, slot);
        records.push(format!("{record:?}"));
    }
    records
}

async fn assert_point_reads(client: &clickhouse::Client, source: &str, cache: &str) {
    for slot in 10..20 {
        assert_eq!(
            point_row(client, cache, slot).await,
            point_row(client, source, slot).await,
            "slot {slot}"
        );
    }
}

#[tokio::test]
#[ignore = "requires disposable ClickHouse: DISK_CACHE_TEST_URL=http://127.0.0.1:18195"]
async fn transactions_compact_parts_read_like_wide_parts() {
    let url = std::env::var("DISK_CACHE_TEST_URL").expect("explicit disposable ClickHouse URL");
    assert!(url.starts_with("http://127.0.0.1:"));
    let database = format!("test_part_layout_{}", now_version());
    let cache_database = format!("{database}_cache");
    let client = clickhouse::Client::default()
        .with_url(&url)
        .with_user("default");
    fixture(&client, &database).await;
    insert_rows(&client, &database, 10, 10).await;
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
            vec![],
            format!("{database}.gsfa_hot"),
            format!("{database}.gsfa_hot"),
        ),
    );
    source.use_table_names(ClickHouseTableNames::in_database(&database));
    let mut cfg = config(url, cache_database.clone());
    cfg.retain_slots = 100;
    cfg.compact_transactions_parts = true;
    let cache = DiskCache::open(cfg.clone(), &source).await.unwrap();
    let show_create = format!("SHOW CREATE TABLE {cache_database}.transactions");
    let ddl = client
        .query(&show_create)
        .fetch_one::<String>()
        .await
        .unwrap();
    assert!(ddl.contains(COMPACT), "{ddl}");
    assert!(
        ddl.contains("write_marks_for_substreams_in_compact_parts = 0"),
        "{ddl}"
    );

    // One partition (slots 10..19) holding every layout an upgraded cache can contain.
    let table = format!("{cache_database}.transactions");
    execute(&client, &format!("SYSTEM STOP MERGES {table}")).await;
    execute(
        &client,
        &format!("ALTER TABLE {table} MODIFY SETTING min_bytes_for_wide_part = 0"),
    )
    .await;
    insert_rows(&client, &cache_database, 10, 4).await;
    execute(&client, &format!("ALTER TABLE {table} MODIFY SETTING {COMPACT}, write_marks_for_substreams_in_compact_parts = 1")).await;
    insert_rows(&client, &cache_database, 14, 3).await;
    execute(
        &client,
        &format!("ALTER TABLE {table} MODIFY SETTING min_bytes_for_wide_part = 0"),
    )
    .await;
    drop(cache);

    // Reopening re-applies the layout without a fingerprint rebuild.
    let cache = DiskCache::open(cfg.clone(), &source).await.unwrap();
    let ddl = client
        .query(&show_create)
        .fetch_one::<String>()
        .await
        .unwrap();
    assert!(ddl.contains(COMPACT), "{ddl}");
    assert_eq!(
        schema::initialize_cache_schema(
            &cache.inner.admin,
            &cache.source_schema(),
            &cfg.schema_config()
        )
        .await
        .unwrap(),
        schema::SchemaBootstrap::Reused
    );
    let count = client
        .query(&format!("SELECT count() FROM {table}"))
        .fetch_one::<u64>()
        .await
        .unwrap();
    assert_eq!(count, 7, "reopen must keep existing rows");
    insert_rows(&client, &cache_database, 17, 3).await;
    assert_eq!(
        part_types(&client, &cache_database).await,
        ["Wide", "Compact", "Compact"]
    );

    assert_point_reads(&client, &database, &cache_database).await;
    cache
        .publish_range_coverage(
            (10..20)
                .map(|slot| (slot, SlotStatus::Covered { tx_count: 1 }))
                .collect(),
        )
        .await
        .unwrap();
    cache.build_key_indexes().await;
    cache.build_signature_indexes().await;
    let mixed = cached_records(&cache).await;

    // A rejected layout ALTER only logs; the cache keeps serving.
    let mut missing = cfg.schema_config();
    missing.database = format!("{cache_database}_missing");
    schema::apply_layout_settings(&cache.inner.admin, &cache.source_schema(), &missing).await;
    assert_eq!(cached_records(&cache).await, mixed);

    // Merging Wide with Compact parts keeps the rows. ClickHouse (26.1, 26.8) writes the
    // result Wide whenever a source part is Wide, so upgraded partitions stay Wide until
    // evicted; partitions filled after the upgrade are Compact throughout.
    execute(&client, &format!("SYSTEM START MERGES {table}")).await;
    execute(&client, &format!("OPTIMIZE TABLE {table} FINAL")).await;
    assert_eq!(part_types(&client, &cache_database).await.len(), 1);
    insert_rows(&client, &cache_database, 20, 3).await;
    insert_rows(&client, &cache_database, 23, 3).await;
    execute(
        &client,
        &format!("OPTIMIZE TABLE {table} PARTITION 2 FINAL"),
    )
    .await;
    let merged = client
        .query(&format!("SELECT part_type FROM system.parts WHERE database='{cache_database}' AND table='transactions' AND active AND partition_id='2'"))
        .fetch_all::<String>()
        .await
        .unwrap();
    assert_eq!(merged, ["Compact"]);
    assert_point_reads(&client, &database, &cache_database).await;
    assert_eq!(cached_records(&cache).await, mixed);

    // Turning the flag off resets both settings on the next open (the rollback for new
    // parts) without a rebuild; Compact parts already written keep serving.
    drop(cache);
    let mut off = cfg.clone();
    off.compact_transactions_parts = false;
    let cache = DiskCache::open(off.clone(), &source).await.unwrap();
    let ddl = client
        .query(&show_create)
        .fetch_one::<String>()
        .await
        .unwrap();
    assert!(!ddl.contains("min_bytes_for_wide_part"), "{ddl}");
    assert!(
        !ddl.contains("write_marks_for_substreams_in_compact_parts"),
        "{ddl}"
    );
    assert_eq!(
        schema::initialize_cache_schema(
            &cache.inner.admin,
            &cache.source_schema(),
            &off.schema_config()
        )
        .await
        .unwrap(),
        schema::SchemaBootstrap::Reused
    );
    assert_point_reads(&client, &database, &cache_database).await;
    drop(cache);

    if std::env::var_os("DISK_CACHE_TEST_KEEP").is_some() {
        eprintln!("Kept source database {database} and cache {cache_database}");
        return;
    }
    execute(&client, &format!("DROP DATABASE {cache_database} SYNC")).await;
    execute(&client, &format!("DROP DATABASE {database} SYNC")).await;
}
