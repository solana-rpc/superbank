use std::path::Path;

use anyhow::{Context, Result};
use solparq::config::Config;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::writer::MakeWriterExt;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::try_parse()?;
    let _log_guard = init_tracing(config.verbose, config.log_file.as_deref())?;
    solparq::run(config).await
}

fn init_tracing(verbose: u8, log_file: Option<&Path>) -> Result<Option<WorkerGuard>> {
    let default_level = match verbose {
        0 => "info",
        1 => "debug",
        _ => "trace",
    };

    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_level));
    if let Some(path) = log_file {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create log directory {}", parent.display()))?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("open log file {}", path.display()))?;
        let (file_writer, guard) = tracing_appender::non_blocking(file);
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(std::io::stderr.and(file_writer))
            .init();
        Ok(Some(guard))
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
        Ok(None)
    }
}
