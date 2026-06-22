use std::{ffi::OsString, path::PathBuf, str::FromStr};

use anyhow::{Result, anyhow};
use clap::{ArgAction, Parser, ValueEnum, error::ErrorKind};

use crate::archive::{ArchiveKind, DEFAULT_CUSTOM_SLOTS};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub enum ArchiveLocation {
    Local,
    S3,
}

#[derive(Debug, Clone)]
pub struct S3Config {
    pub bucket_name: String,
    pub bucket_path: String,
    pub auth_key: String,
    pub auth_secret_key: String,
    pub endpoint: String,
    pub region: String,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub db_server: String,
    pub db_server_port: u16,
    pub db_database: String,
    pub db_user: String,
    pub db_password: String,
    pub transactions_table: String,
    pub blocks_table: String,
    pub gsfa_table: String,
    pub signatures_table: String,
    pub archive_kinds: Vec<ArchiveKind>,
    pub archive_location: ArchiveLocation,
    pub output_location: PathBuf,
    pub s3: Option<S3Config>,
    pub force_archive: bool,
    pub delete_archived_data_range: bool,
    pub server_mode: bool,
    pub ops_port: u16,
    pub metrics_port: u16,
    pub archives_to_keep: usize,
    pub solana_rpc_url: String,
    pub archive_check_interval_secs: u64,
    pub log_file: Option<PathBuf>,
    pub verbose: u8,
}

impl Config {
    pub fn try_parse() -> Result<Self> {
        let cli = match Cli::try_parse() {
            Ok(cli) => cli,
            Err(err)
                if matches!(
                    err.kind(),
                    ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
                ) =>
            {
                err.print()?;
                std::process::exit(0);
            }
            Err(err) => return Err(anyhow!(err.to_string())),
        };
        Self::from_cli(cli)
    }

    pub fn try_parse_from<I, T>(args: I) -> Result<Self>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        let cli = Cli::try_parse_from(args).map_err(|err| anyhow!(err.to_string()))?;
        Self::from_cli(cli)
    }

    fn from_cli(cli: Cli) -> Result<Self> {
        let archive_kinds = parse_archive_kinds(&cli.archive_range_type, cli.custom_slot_range)?;
        if !cli.server_mode && archive_kinds.len() > 1 {
            return Err(anyhow!(
                "multiple archive range types require --server-mode"
            ));
        }

        for table in [
            &cli.transactions_table,
            &cli.blocks_table,
            &cli.gsfa_table,
            &cli.signatures_table,
        ] {
            validate_table_name(table)?;
        }

        if cli.archive_check_interval_secs == 0 {
            return Err(anyhow!(
                "archive-check-interval-secs must be greater than zero"
            ));
        }

        let s3 = match cli.archive_location {
            ArchiveLocation::Local => None,
            ArchiveLocation::S3 => Some(S3Config {
                bucket_name: require_s3_value(
                    cli.archive_s3_bucket_name,
                    "archive-s3-bucket-name",
                )?,
                bucket_path: cli.archive_s3_bucket_path.unwrap_or_default(),
                auth_key: require_s3_value(cli.archive_s3_auth_key, "archive-s3-auth-key")?,
                auth_secret_key: require_s3_value(
                    cli.archive_s3_auth_secret_key,
                    "archive-s3-auth-secret-key",
                )?,
                endpoint: require_s3_value(cli.archive_s3_endpoint, "archive-s3-endpoint")?,
                region: cli.archive_s3_region,
            }),
        };

        Ok(Self {
            db_server: cli.db_server,
            db_server_port: cli.db_server_port,
            db_database: cli.db_database,
            db_user: cli.db_user,
            db_password: cli.db_password,
            transactions_table: cli.transactions_table,
            blocks_table: cli.blocks_table,
            gsfa_table: cli.gsfa_table,
            signatures_table: cli.signatures_table,
            archive_kinds,
            archive_location: cli.archive_location,
            output_location: cli.archive_file_output_location,
            s3,
            force_archive: cli.force_archive,
            delete_archived_data_range: cli.delete_archived_data_range,
            server_mode: cli.server_mode,
            ops_port: cli.ops_port,
            metrics_port: cli.metrics_port,
            archives_to_keep: cli.archives_to_keep,
            solana_rpc_url: cli.solana_rpc_url,
            archive_check_interval_secs: cli.archive_check_interval_secs,
            log_file: cli.log_file,
            verbose: cli.verbose,
        })
    }

    pub fn clickhouse_url(&self) -> String {
        let server = self.db_server.trim_end_matches('/');
        if server.starts_with("http://") || server.starts_with("https://") {
            return server.to_string();
        }
        if server
            .rsplit_once(':')
            .and_then(|(_, port)| port.parse::<u16>().ok())
            .is_some()
        {
            format!("http://{server}")
        } else {
            format!("http://{}:{}", server, self.db_server_port)
        }
    }
}

#[derive(Parser, Debug)]
#[command(about = "Archive Superbank ClickHouse transactions to Parquet")]
struct Cli {
    #[arg(long, env = "SOLPARQ_DB_SERVER")]
    db_server: String,

    #[arg(long, env = "SOLPARQ_DB_SERVER_PORT", default_value_t = 8123)]
    db_server_port: u16,

    #[arg(long, env = "SOLPARQ_DB_DATABASE", default_value = "default")]
    db_database: String,

    #[arg(long, env = "SOLPARQ_DB_USER")]
    db_user: String,

    #[arg(long, env = "SOLPARQ_DB_PASSWORD")]
    db_password: String,

    #[arg(
        long = "db-transactions-table-name",
        alias = "transactions-table",
        env = "SOLPARQ_DB_TRANSACTIONS_TABLE_NAME",
        default_value = "transactions"
    )]
    transactions_table: String,

    #[arg(
        long = "db-blocks-table-name",
        alias = "blocks-table",
        env = "SOLPARQ_DB_BLOCKS_TABLE_NAME",
        default_value = "blocks_metadata"
    )]
    blocks_table: String,

    #[arg(
        long = "db-gsfa-table-name",
        alias = "gsfa-table",
        env = "SOLPARQ_DB_GSFA_TABLE_NAME",
        default_value = "gsfa"
    )]
    gsfa_table: String,

    #[arg(
        long = "db-signatures-table-name",
        alias = "signatures-table",
        env = "SOLPARQ_DB_SIGNATURES_TABLE_NAME",
        default_value = "signatures"
    )]
    signatures_table: String,

    #[arg(
        long = "archive-range-type",
        env = "SOLPARQ_ARCHIVE_RANGE_TYPE",
        value_delimiter = ',',
        action = ArgAction::Append,
        required = true
    )]
    archive_range_type: Vec<String>,

    #[arg(long, env = "SOLPARQ_CUSTOM_SLOT_RANGE", default_value_t = DEFAULT_CUSTOM_SLOTS)]
    custom_slot_range: u64,

    #[arg(
        long = "archive-location-type",
        env = "SOLPARQ_ARCHIVE_LOCATION_TYPE",
        value_enum,
        default_value = "local"
    )]
    archive_location: ArchiveLocation,

    #[arg(
        long = "archive-file-output-location",
        env = "SOLPARQ_ARCHIVE_FILE_OUTPUT_LOCATION",
        default_value = "./"
    )]
    archive_file_output_location: PathBuf,

    #[arg(
        long = "archive-s3-bucket-name",
        env = "SOLPARQ_ARCHIVE_S3_BUCKET_NAME"
    )]
    archive_s3_bucket_name: Option<String>,

    #[arg(
        long = "archive-s3-bucket-path",
        env = "SOLPARQ_ARCHIVE_S3_BUCKET_PATH"
    )]
    archive_s3_bucket_path: Option<String>,

    #[arg(long = "archive-s3-auth-key", env = "SOLPARQ_ARCHIVE_S3_AUTH_KEY")]
    archive_s3_auth_key: Option<String>,

    #[arg(
        long = "archive-s3-auth-secret-key",
        env = "SOLPARQ_ARCHIVE_S3_AUTH_SECRET_KEY"
    )]
    archive_s3_auth_secret_key: Option<String>,

    #[arg(long = "archive-s3-endpoint", env = "SOLPARQ_ARCHIVE_S3_ENDPOINT")]
    archive_s3_endpoint: Option<String>,

    #[arg(
        long = "archive-s3-region",
        env = "SOLPARQ_ARCHIVE_S3_REGION",
        default_value = "us-east-1"
    )]
    archive_s3_region: String,

    #[arg(
        long = "force-archive",
        env = "SOLPARQ_FORCE_ARCHIVE",
        default_value_t = false
    )]
    force_archive: bool,

    #[arg(
        long = "delete-archived-data-range",
        env = "SOLPARQ_DELETE_ARCHIVED_DATA_RANGE",
        default_value_t = false
    )]
    delete_archived_data_range: bool,

    #[arg(
        long = "server-mode",
        env = "SOLPARQ_SERVER_MODE",
        default_value_t = false
    )]
    server_mode: bool,

    #[arg(long = "ops-port", env = "SOLPARQ_OPS_PORT", default_value_t = 30_303)]
    ops_port: u16,

    #[arg(
        long = "metrics-port",
        env = "SOLPARQ_METRICS_PORT",
        default_value_t = 31_313
    )]
    metrics_port: u16,

    #[arg(
        long = "archives-to-keep",
        env = "SOLPARQ_ARCHIVES_TO_KEEP",
        default_value_t = 5
    )]
    archives_to_keep: usize,

    #[arg(
        long = "solana-rpc-url",
        env = "SOLPARQ_SOLANA_RPC_URL",
        default_value = "https://api.mainnet-beta.solana.com"
    )]
    solana_rpc_url: String,

    #[arg(
        long = "archive-check-interval-secs",
        env = "SOLPARQ_ARCHIVE_CHECK_INTERVAL_SECS",
        default_value_t = 60
    )]
    archive_check_interval_secs: u64,

    #[arg(long = "log-file", env = "SOLPARQ_LOG_FILE", value_name = "PATH")]
    log_file: Option<PathBuf>,

    #[arg(short, long, action = ArgAction::Count)]
    verbose: u8,
}

fn parse_archive_kinds(values: &[String], custom_slot_range: u64) -> Result<Vec<ArchiveKind>> {
    if custom_slot_range == 0 {
        return Err(anyhow!("custom-slot-range must be greater than zero"));
    }
    let mut kinds = Vec::new();
    for value in values {
        let kind = if value.eq_ignore_ascii_case("custom") {
            ArchiveKind::Custom {
                slots: custom_slot_range,
            }
        } else {
            ArchiveKind::from_str(value).map_err(|err| anyhow!(err))?
        };
        kinds.push(kind);
    }
    Ok(kinds)
}

fn require_s3_value(value: Option<String>, name: &str) -> Result<String> {
    let value = value.ok_or_else(|| anyhow!("{name} is required when archive-location-type=s3"))?;
    if value.trim().is_empty() {
        return Err(anyhow!("{name} is required when archive-location-type=s3"));
    }
    Ok(value)
}

fn validate_table_name(value: &str) -> Result<()> {
    if value.is_empty()
        || !value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '.')
    {
        return Err(anyhow!(
            "invalid ClickHouse table name '{value}'; use only ASCII letters, numbers, '_' and '.'"
        ));
    }
    Ok(())
}
