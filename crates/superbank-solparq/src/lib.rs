pub mod archive;
pub mod clickhouse;
pub mod config;
pub mod manifest;
pub mod metrics;
pub mod read;
pub mod server;
pub mod storage;

use anyhow::Result;
use config::Config;

pub async fn run(config: Config) -> Result<()> {
    if config.server_mode {
        server::run(config).await
    } else {
        let report = archive::run_once(&config).await?;
        println!("{}", report.to_text());
        Ok(())
    }
}
