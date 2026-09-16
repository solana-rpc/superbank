// SPDX-License-Identifier: AGPL-3.0-only
//! Read-only benchmark over a separately provisioned synthetic local fixture.
use super::*;
use serde::Deserialize;
use std::collections::HashSet;

#[derive(Deserialize)]
struct Manifest {
    database: String,
    partition_slots: u64,
    #[serde(default = "query_threads")]
    query_max_threads: u32,
    samples: Vec<Sample>,
}

fn query_threads() -> u32 {
    2
}

#[derive(Deserialize)]
struct Sample {
    signature: String,
    slot: u64,
    slot_idx: u32,
    batch: u32,
    phase: String,
    mode: String,
}

fn validate(manifest: &Manifest) {
    assert!(!manifest.database.is_empty());
    assert!(
        manifest
            .database
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
    );
    assert!(manifest.partition_slots > 0 && manifest.query_max_threads > 0);
    let mut payload = HashSet::new();
    let mut two_step = HashSet::new();
    let mut first = HashSet::new();
    let mut repeated = false;
    for sample in &manifest.samples {
        assert!(matches!(sample.mode.as_str(), "payload" | "two_step"));
        match sample.phase.as_str() {
            "first_touch" => {
                assert!(
                    !repeated,
                    "first-touch requests must precede repeated requests"
                );
                assert!(
                    first.insert((&sample.mode, &sample.signature)),
                    "duplicate first-touch signature"
                );
            }
            "repeated" => {
                repeated = true;
                assert!(
                    first.contains(&(&sample.mode, &sample.signature)),
                    "repeat without a first touch"
                );
            }
            _ => panic!("unknown benchmark phase"),
        }
        if sample.mode == "payload" {
            payload.insert(&sample.signature);
        } else {
            two_step.insert(&sample.signature);
        }
    }
    assert!(
        payload.is_disjoint(&two_step),
        "payload and two-step corpora overlap"
    );
}

#[test]
fn layout_manifest_rejects_prewarmed_or_overlapping_corpora() {
    let sample = |mode, phase| {
        json!({"signature":"example","slot":1,"slot_idx":0,
        "batch":0,"mode":mode,"phase":phase})
    };
    for samples in [
        vec![
            sample("payload", "first_touch"),
            sample("two_step", "first_touch"),
        ],
        vec![
            sample("payload", "first_touch"),
            sample("payload", "first_touch"),
        ],
        vec![sample("payload", "repeated")],
    ] {
        let manifest: Manifest = serde_json::from_value(json!({"database":"fixture",
            "partition_slots":10000,"samples":samples}))
        .unwrap();
        assert!(std::panic::catch_unwind(|| validate(&manifest)).is_err());
    }
}

#[tokio::test]
#[ignore = "requires provisioned disposable ClickHouse, PAYLOAD_LAYOUT_MANIFEST and PAYLOAD_LAYOUT_OUTPUT"]
async fn payload_layout_benchmark() {
    let url = std::env::var("DISK_CACHE_TEST_URL").expect("explicit local ClickHouse URL");
    assert!(disposable_loopback_url(&url));
    let manifest: Manifest = serde_json::from_slice(
        &std::fs::read(std::env::var("PAYLOAD_LAYOUT_MANIFEST").expect("sample manifest path"))
            .unwrap(),
    )
    .unwrap();
    validate(&manifest);
    let output = std::env::var("PAYLOAD_LAYOUT_OUTPUT").expect("output artifact path");
    let mut client = ClickHouseClient::new(
        &url,
        &manifest.database,
        "default",
        "",
        ClickHouseClientOptions::new(
            RoutingPolicy {
                transport: RoutingTransport::Http,
                scope: RoutingScope::Distributed,
            },
            None,
            vec![],
            format!("{}.gsfa_hot", manifest.database),
            format!("{}.gsfa_hot", manifest.database),
        )
        .with_query_timeout(Duration::from_secs(2))
        .with_http_concurrency(2),
    );
    client.use_table_names(ClickHouseTableNames::in_database(&manifest.database));
    client.client = client
        .client
        .clone()
        .with_setting("max_threads", manifest.query_max_threads.to_string());
    client.initialize_read_cancellation().await.unwrap();
    let mut results = Vec::new();
    for sample in manifest.samples {
        client.cache_partition = Some((
            manifest.partition_slots,
            sample.slot / manifest.partition_slots,
        ));
        let result = client
            .with_operation_timeout("payload_layout_benchmark", async {
                Ok(crate::clickhouse::measure_layout_sample(
                    &client,
                    &sample.signature,
                    crate::clickhouse::SignatureSlot {
                        slot: sample.slot,
                        slot_idx: sample.slot_idx,
                    },
                    sample.mode == "two_step",
                )
                .await)
            })
            .await
            .unwrap();
        results.push(
            json!({"signature":sample.signature,"slot":sample.slot,"slot_idx":sample.slot_idx,
            "batch":sample.batch,"phase":sample.phase,"mode":sample.mode,"measurements":result}),
        );
    }
    std::fs::write(
        output,
        serde_json::to_vec_pretty(&json!({"database":manifest.database,"samples":results}))
            .unwrap(),
    )
    .unwrap();
}
