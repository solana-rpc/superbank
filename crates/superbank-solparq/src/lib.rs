pub mod archive;
pub mod backfill;
pub mod clickhouse;
pub mod config;
pub mod manifest;
pub mod metrics;
pub mod read;
pub mod server;
pub mod storage;

use anyhow::Result;
use config::Config;
use manifest::ManifestProducer;

pub async fn run(config: Config) -> Result<()> {
    let producer = ManifestProducer::current();
    tracing::info!(
        name = producer.name,
        version = producer.version,
        git_sha = producer.git_sha,
        "starting"
    );

    if config.server_mode {
        server::run(config).await
    } else {
        let report = archive::run_once(&config).await?;
        println!("{}", report.to_text());
        Ok(())
    }
}
