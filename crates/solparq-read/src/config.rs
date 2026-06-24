use std::{ffi::OsString, path::PathBuf};

use clap::{Args, CommandFactory, Parser, Subcommand, ValueEnum, error::ErrorKind};
use serde::Serialize;

#[derive(Debug, Parser)]
#[command(
    name = "solparq-read",
    version,
    about = "Inspect and read solparq Parquet archives"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

impl Cli {
    pub fn try_parse_from<I, T>(itr: I) -> Result<Self, clap::Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        let cli = <Self as Parser>::try_parse_from(itr)?;
        cli.finalize()
    }

    pub fn parse_args() -> Self {
        match <Self as Parser>::try_parse().and_then(Self::finalize) {
            Ok(cli) => cli,
            Err(err) => err.exit(),
        }
    }

    fn finalize(mut self) -> Result<Self, clap::Error> {
        if let Command::Scan(scan) = &mut self.command {
            scan.selection = match (scan.slot_range, scan.all) {
                (Some(range), false) => ScanSelection::SlotRange {
                    start_slot: range.start_slot,
                    end_slot: range.end_slot,
                },
                (None, true) => ScanSelection::All,
                _ => {
                    return Err(Cli::command().error(
                        ErrorKind::MissingRequiredArgument,
                        "exactly one of --slot-range or --all is required",
                    ));
                }
            };
        }
        Ok(self)
    }
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// List archives under a local directory or S3 prefix.
    List(ListArgs),
    /// Show row, slot, block, and metadata counts for one archive.
    Summary(ArchiveArgs),
    /// Show the Arrow schema stored in one archive.
    Schema(ArchiveArgs),
    /// Output transaction rows from one archive.
    Scan(ScanArgs),
}

impl Command {
    pub fn scan_args(&self) -> Option<&ScanArgs> {
        match self {
            Command::Scan(args) => Some(args),
            Command::List(_) | Command::Summary(_) | Command::Schema(_) => None,
        }
    }
}

#[derive(Debug, Args)]
pub struct ListArgs {
    #[command(flatten)]
    pub source: SourceArgs,

    /// Local archive directory. Required when --archive-location-type=local.
    #[arg(long = "archive-dir")]
    pub archive_dir: Option<PathBuf>,

    /// Optional archive type filter such as hourly, epoch, or custom.
    #[arg(long = "archive-kind")]
    pub archive_kind: Option<String>,
}

#[derive(Debug, Args)]
pub struct ArchiveArgs {
    #[command(flatten)]
    pub source: SourceArgs,

    /// Local archive file path, or S3 object key relative to --archive-s3-bucket-path.
    #[arg(long = "archive")]
    pub archive: String,

    /// Table to read when --archive points at a DB archive bundle.
    #[arg(long = "table", value_enum, default_value_t = ArchiveTable::Transactions)]
    pub table: ArchiveTable,
}

#[derive(Debug, Args)]
pub struct ScanArgs {
    #[command(flatten)]
    pub archive: ArchiveArgs,

    /// Inclusive slot range to read, formatted START-END.
    #[arg(long = "slot-range", value_parser = parse_slot_range)]
    pub slot_range: Option<SlotRange>,

    /// Read all transaction rows in the archive.
    #[arg(long = "all")]
    pub all: bool,

    /// Comma-separated output columns. Defaults to every column.
    #[arg(long = "columns", value_delimiter = ',')]
    pub columns: Vec<String>,

    /// Maximum number of rows to output.
    #[arg(long = "limit")]
    pub limit: Option<usize>,

    /// Transaction output format.
    #[arg(long = "format", value_enum, default_value_t = OutputFormat::Jsonl)]
    pub format: OutputFormat,

    #[arg(skip = ScanSelection::All)]
    pub selection: ScanSelection,
}

#[derive(Debug, Clone, Args)]
pub struct SourceArgs {
    /// Archive source type.
    #[arg(
        long = "archive-location-type",
        value_enum,
        default_value_t = ArchiveLocationType::Local
    )]
    pub archive_location_type: ArchiveLocationType,

    /// S3-compatible endpoint, for example https://s3.eu-central-003.backblazeb2.com.
    #[arg(long = "archive-s3-endpoint", env = "SOLPARQ_READ_ARCHIVE_S3_ENDPOINT")]
    pub archive_s3_endpoint: Option<String>,

    /// S3 bucket name.
    #[arg(
        long = "archive-s3-bucket-name",
        env = "SOLPARQ_READ_ARCHIVE_S3_BUCKET_NAME"
    )]
    pub archive_s3_bucket_name: Option<String>,

    /// S3 key prefix containing solparq archives.
    #[arg(
        long = "archive-s3-bucket-path",
        env = "SOLPARQ_READ_ARCHIVE_S3_BUCKET_PATH",
        default_value = ""
    )]
    pub archive_s3_bucket_path: String,

    /// S3 access key.
    #[arg(long = "archive-s3-auth-key", env = "SOLPARQ_READ_ARCHIVE_S3_AUTH_KEY")]
    pub archive_s3_auth_key: Option<String>,

    /// S3 secret access key.
    #[arg(
        long = "archive-s3-auth-secret-key",
        env = "SOLPARQ_READ_ARCHIVE_S3_AUTH_SECRET_KEY"
    )]
    pub archive_s3_auth_secret_key: Option<String>,

    /// S3 region used for request signing.
    #[arg(
        long = "archive-s3-region",
        env = "SOLPARQ_READ_ARCHIVE_S3_REGION",
        default_value = "us-east-1"
    )]
    pub archive_s3_region: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ArchiveLocationType {
    Local,
    S3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "snake_case")]
pub enum ArchiveTable {
    Transactions,
    BlocksMetadata,
    Entries,
    Gsfa,
    GsfaHot,
    Signatures,
    TokenOwnerActivity,
}

impl ArchiveTable {
    pub fn as_str(self) -> &'static str {
        match self {
            ArchiveTable::Transactions => "transactions",
            ArchiveTable::BlocksMetadata => "blocks_metadata",
            ArchiveTable::Entries => "entries",
            ArchiveTable::Gsfa => "gsfa",
            ArchiveTable::GsfaHot => "gsfa_hot",
            ArchiveTable::Signatures => "signatures",
            ArchiveTable::TokenOwnerActivity => "token_owner_activity",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ScanSelection {
    All,
    SlotRange { start_slot: u64, end_slot: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotRange {
    pub start_slot: u64,
    pub end_slot: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
    Jsonl,
    Json,
    Csv,
}

fn parse_slot_range(value: &str) -> Result<SlotRange, String> {
    let (start, end) = value
        .split_once('-')
        .ok_or_else(|| "slot range must be formatted START-END".to_string())?;
    let start_slot = start
        .parse::<u64>()
        .map_err(|_| "slot range start must be an unsigned integer".to_string())?;
    let end_slot = end
        .parse::<u64>()
        .map_err(|_| "slot range end must be an unsigned integer".to_string())?;
    if end_slot < start_slot {
        return Err("slot range end must be greater than or equal to start".to_string());
    }
    Ok(SlotRange {
        start_slot,
        end_slot,
    })
}
