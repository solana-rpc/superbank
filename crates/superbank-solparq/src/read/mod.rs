pub mod archive_name;
pub mod config;
pub mod output;
pub mod query;
pub mod source;

use anyhow::Result;

use crate::read::{
    config::{Cli, Command},
    output::{format_archive_list, format_rows, format_schema, format_summary},
    query::{ArchiveInput, ScanOptions, scan_archive, schema_archive, summarize_archive},
    source::{archive_input_from_args, list_archives},
};

pub async fn render(cli: Cli) -> Result<String> {
    match cli.command {
        Command::List(args) => {
            let archives = list_archives(&args).await?;
            Ok(format_archive_list(&archives))
        }
        Command::Summary(args) => {
            let summary = summarize_archive(archive_input_from_args(&args)?).await?;
            Ok(format_summary(&summary))
        }
        Command::Schema(args) => {
            let schema = schema_archive(archive_input_from_args(&args)?).await?;
            Ok(format_schema(&schema))
        }
        Command::Scan(args) => {
            let input = archive_input_from_args(&args.archive)?;
            let format = args.format;
            let rows = scan_archive(
                input,
                ScanOptions {
                    selection: args.selection,
                    columns: args.columns,
                    limit: args.limit,
                },
            )
            .await?;
            Ok(format_rows(&rows, format)?)
        }
    }
}

pub async fn run(cli: Cli) -> Result<()> {
    let rendered = render(cli).await?;
    print!("{rendered}");
    Ok(())
}

impl From<std::path::PathBuf> for ArchiveInput {
    fn from(path: std::path::PathBuf) -> Self {
        Self::LocalFile(path)
    }
}
