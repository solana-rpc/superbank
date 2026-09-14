// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::HashMap;
use std::time::Duration;

use axum::{Router, extract::Query, routing::post};
use tokio::sync::mpsc;

use crate::clickhouse::{
    ClickHouseClient, ClickHouseClientOptions, RoutingPolicy, RoutingScope, RoutingTransport,
};

struct SettingsServer {
    client: ClickHouseClient,
    requests: mpsc::UnboundedReceiver<HashMap<String, String>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for SettingsServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl SettingsServer {
    async fn new() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (sender, requests) = mpsc::unbounded_channel();
        let app = Router::new().route(
            "/",
            post(move |Query(params): Query<HashMap<String, String>>| {
                let sender = sender.clone();
                async move {
                    sender.send(params).unwrap();
                    axum::http::StatusCode::OK
                }
            }),
        );
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut client = ClickHouseClient::new(
            &url,
            "default",
            "default",
            "",
            ClickHouseClientOptions::new(
                RoutingPolicy {
                    transport: RoutingTransport::Http,
                    scope: RoutingScope::Distributed,
                },
                None,
                Vec::new(),
                "default.gsfa_hot".into(),
                "default.gsfa_hot_local".into(),
            ),
        );
        client.set_http_client_for_tests(
            client
                .client
                .clone()
                .with_validation(false)
                .with_compression(clickhouse::Compression::None),
        );
        Self {
            client,
            requests,
            task,
        }
    }

    async fn capture(&mut self, query: clickhouse::query::Query) -> HashMap<String, String> {
        tokio::time::timeout(Duration::from_secs(2), query.execute())
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), self.requests.recv())
            .await
            .unwrap()
            .unwrap()
    }
}

#[tokio::test]
async fn disconnect_http_settings_are_exclusive_to_primary_distributed_http_status_reads() {
    let mut server = SettingsServer::new().await;
    let protected = server
        .client
        .signature_status_http_query("SELECT 1", Some("protected".into()));
    let params = server.capture(protected).await;
    assert_eq!(params["readonly"], "2");
    assert_eq!(params["cancel_http_readonly_queries_on_client_close"], "1");
    assert_eq!(params["query_id"], "protected");

    let mut local_cache = server.client.clone();
    local_cache.cache_partition = Some((1000, 1));
    let mut shard_direct = server.client.clone();
    shard_direct.routing_policy.scope = RoutingScope::ShardDirect;
    let mut tcp_policy = server.client.clone();
    tcp_policy.routing_policy.transport = RoutingTransport::Tcp;

    for (label, client) in [
        ("local_cache", local_cache),
        ("shard_direct", shard_direct),
        ("tcp_policy", tcp_policy),
    ] {
        let query = client.signature_status_http_query("SELECT 1", Some(label.into()));
        let params = server.capture(query).await;
        assert!(!params.contains_key("readonly"), "{label}");
        assert!(
            !params.contains_key("cancel_http_readonly_queries_on_client_close"),
            "{label}"
        );
        assert_eq!(params["query_id"], label);
    }

    let unrelated = server.client.client.query("SELECT 1");
    let params = server.capture(unrelated).await;
    assert!(!params.contains_key("readonly"));
    assert!(!params.contains_key("cancel_http_readonly_queries_on_client_close"));
}

#[tokio::test]
async fn protected_status_read_does_not_mutate_shared_http_client_settings() {
    let mut server = SettingsServer::new().await;
    server.client.set_http_client_for_tests(
        server
            .client
            .client
            .clone()
            .with_setting("readonly", "1")
            .with_setting("max_threads", "7"),
    );
    let before = server.capture(server.client.client.query("SELECT 1")).await;
    let query = server
        .client
        .signature_status_http_query("SELECT 1", Some("protected".into()));
    let protected = server.capture(query).await;
    let after = server.capture(server.client.client.query("SELECT 1")).await;

    assert_eq!(protected["readonly"], "2");
    assert_eq!(
        protected["cancel_http_readonly_queries_on_client_close"],
        "1"
    );
    assert_eq!(protected["max_threads"], "7");
    assert_eq!(before, after);
    assert_eq!(after["readonly"], "1");
    assert!(!after.contains_key("cancel_http_readonly_queries_on_client_close"));
}
