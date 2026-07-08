use std::{collections::HashSet, ffi::OsString, path::PathBuf, str::FromStr};

use anyhow::{Result, anyhow};
use clap::{ArgAction, Parser, ValueEnum, error::ErrorKind};

use crate::archive::{ArchiveKind, ArchiveSlotRange, DEFAULT_CUSTOM_SLOTS};

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
    pub transactions_local_table: String,
    pub clickhouse_cluster: Option<String>,
    /// Raw `SETTINGS` clause appended to the archive `SELECT`/`INSERT ... SELECT`
    /// queries that export Parquet. Bounds per-query memory so a full-epoch
    /// export does not trip the server memory limit. Empty disables the clause.
    pub clickhouse_archive_settings: String,
    pub blocks_table: String,
    pub entries_table: String,
    pub gsfa_table: String,
    pub gsfa_hot_table: String,
    pub signatures_table: String,
    pub token_owner_activity_table: String,
    pub repair_mismatches: bool,
    pub archive_kinds: Vec<ArchiveKind>,
    pub archive_location: ArchiveLocation,
    pub output_location: PathBuf,
    pub s3: Option<S3Config>,
    /// Write `SHA256SUMS.txt` for S3 archives. Disabled by default: ClickHouse
    /// writes the Parquet objects directly to S3, so computing checksums forces
    /// superbank to download every object back, which is API-rate-limit
    /// intensive on large archives.
    pub s3_write_checksums: bool,
    pub force_archive: bool,
    /// Backfill blocks missing from the archive's slot range (detected by
    /// validation) from Solana RPC before archiving, by invoking the `superbank`
    /// RPC ingestor as a subprocess. Off by default.
    pub backfill_gaps: bool,
    /// Path to the `superbank` binary used for gap backfill (resolved from
    /// `PATH` when a bare name).
    pub backfill_superbank_bin: String,
    /// Also backfill slots flagged as transaction undercounts (block present but
    /// fewer archived rows than declared); re-ingest re-inserts and dedups.
    pub backfill_include_undercounts: bool,
    pub delete_archived_data_range: bool,
    pub server_mode: bool,
    pub ops_port: u16,
    pub metrics_port: u16,
    pub archives_to_keep: usize,
    pub solana_rpc_url: String,
    pub archive_check_interval_secs: u64,
    pub continue_from_last_archive: bool,
    pub archive_slot_range: Option<ArchiveSlotRange>,
    pub log_file: Option<PathBuf>,
    pub verbose: u8,
    pub dry_run: bool,
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
        if cli.server_mode && cli.archive_slot_range.is_some() {
            return Err(anyhow!(
                "--archive-slot-range is only supported for one-shot archives and cannot be used with --server-mode"
            ));
        }

        for table in [
            &cli.transactions_table,
            &cli.blocks_table,
            &cli.entries_table,
            &cli.gsfa_table,
            &cli.gsfa_hot_table,
            &cli.signatures_table,
            &cli.token_owner_activity_table,
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
            transactions_local_table: cli
                .transactions_local_table
                .clone()
                .unwrap_or_else(|| cli.transactions_table.clone()),
            transactions_table: cli.transactions_table,
            clickhouse_cluster: cli
                .clickhouse_cluster
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            clickhouse_archive_settings: cli.clickhouse_archive_settings.trim().to_string(),
            blocks_table: cli.blocks_table,
            entries_table: cli.entries_table,
            gsfa_table: cli.gsfa_table,
            gsfa_hot_table: cli.gsfa_hot_table,
            signatures_table: cli.signatures_table,
            token_owner_activity_table: cli.token_owner_activity_table,
            repair_mismatches: cli.repair_mismatches,
            archive_kinds,
            archive_location: cli.archive_location,
            output_location: cli.archive_file_output_location,
            s3,
            s3_write_checksums: cli.archive_s3_write_checksums,
            force_archive: cli.force_archive,
            backfill_gaps: cli.backfill_gaps,
            backfill_superbank_bin: cli.backfill_superbank_bin,
            backfill_include_undercounts: cli.backfill_include_undercounts,
            delete_archived_data_range: cli.delete_archived_data_range,
            server_mode: cli.server_mode,
            ops_port: cli.ops_port,
            metrics_port: cli.metrics_port,
            archives_to_keep: cli.archives_to_keep,
            solana_rpc_url: cli.solana_rpc_url,
            archive_check_interval_secs: cli.archive_check_interval_secs,
            continue_from_last_archive: !cli.no_continue_from_last_archive,
            archive_slot_range: cli.archive_slot_range,
            log_file: cli.log_file,
            verbose: cli.verbose,
            dry_run: cli.dry_run,
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
#[command(
    version = env!("SUPERBANK_LONG_VERSION"),
    about = "Archive Superbank ClickHouse tables to Parquet"
)]
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

    /// Shard-local transactions table targeted by mismatch-repair OPTIMIZE
    /// statements. Defaults to the transactions table name; set this to the
    /// `*_local` table in clustered/replicated deployments (OPTIMIZE cannot run
    /// on a Distributed table).
    #[arg(
        long = "db-transactions-local-table-name",
        env = "SOLPARQ_DB_TRANSACTIONS_LOCAL_TABLE_NAME"
    )]
    transactions_local_table: Option<String>,

    /// ClickHouse cluster name for `ON CLUSTER` mismatch-repair OPTIMIZE
    /// statements. Leave unset for single-node deployments.
    #[arg(long = "clickhouse-cluster", env = "SOLPARQ_CLICKHOUSE_CLUSTER")]
    clickhouse_cluster: Option<String>,

    /// Raw ClickHouse `SETTINGS` appended to the Parquet-export queries (both S3
    /// and local). Keeps a full-epoch export from exceeding the server memory
    /// limit by spilling the sort to disk, capping read parallelism, and
    /// shrinking the Parquet writer's in-memory row group. Pass an empty string
    /// to omit the clause entirely, or override with your own tuning.
    #[arg(
        long = "clickhouse-archive-settings",
        env = "SOLPARQ_CLICKHOUSE_ARCHIVE_SETTINGS",
        default_value = "max_bytes_before_external_sort=1073741824, max_threads=4, output_format_parquet_row_group_size=100000"
    )]
    clickhouse_archive_settings: String,

    #[arg(
        long = "db-blocks-table-name",
        alias = "blocks-table",
        env = "SOLPARQ_DB_BLOCKS_TABLE_NAME",
        default_value = "blocks_metadata"
    )]
    blocks_table: String,

    #[arg(
        long = "db-entries-table-name",
        alias = "entries-table",
        env = "SOLPARQ_DB_ENTRIES_TABLE_NAME",
        default_value = "entries"
    )]
    entries_table: String,

    #[arg(
        long = "db-gsfa-table-name",
        alias = "gsfa-table",
        env = "SOLPARQ_DB_GSFA_TABLE_NAME",
        default_value = "gsfa"
    )]
    gsfa_table: String,

    #[arg(
        long = "db-gsfa-hot-table-name",
        alias = "gsfa-hot-table",
        env = "SOLPARQ_DB_GSFA_HOT_TABLE_NAME",
        default_value = "gsfa_hot"
    )]
    gsfa_hot_table: String,

    #[arg(
        long = "db-signatures-table-name",
        alias = "signatures-table",
        env = "SOLPARQ_DB_SIGNATURES_TABLE_NAME",
        default_value = "signatures"
    )]
    signatures_table: String,

    #[arg(
        long = "db-token-owner-activity-table-name",
        alias = "token-owner-activity-table",
        env = "SOLPARQ_DB_TOKEN_OWNER_ACTIVITY_TABLE_NAME",
        default_value = "token_owner_activity"
    )]
    token_owner_activity_table: String,

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

    /// Write `SHA256SUMS.txt` for S3 archives. Off by default: ClickHouse writes
    /// the Parquet objects straight to S3, so checksumming them requires
    /// downloading every object back, which is API-rate-limit intensive on large
    /// archives. Enable only when integrity manifests are worth that cost.
    #[arg(
        long = "archive-s3-write-checksums",
        env = "SOLPARQ_ARCHIVE_S3_WRITE_CHECKSUMS",
        default_value_t = false
    )]
    archive_s3_write_checksums: bool,

    #[arg(
        long = "force-archive",
        env = "SOLPARQ_FORCE_ARCHIVE",
        default_value_t = false
    )]
    force_archive: bool,

    /// Backfill blocks missing from the archive's slot range from Solana RPC
    /// before archiving, by invoking the `superbank` RPC ingestor as a
    /// subprocess. Uses `--solana-rpc-url` and the configured ClickHouse
    /// connection. Off by default.
    #[arg(
        long = "backfill-gaps",
        env = "SOLPARQ_BACKFILL_GAPS",
        default_value_t = false
    )]
    backfill_gaps: bool,

    /// Path to the `superbank` binary used for gap backfill.
    #[arg(
        long = "backfill-superbank-bin",
        env = "SOLPARQ_BACKFILL_SUPERBANK_BIN",
        default_value = "superbank"
    )]
    backfill_superbank_bin: String,

    /// Also backfill slots flagged as transaction undercounts (block present but
    /// fewer archived rows than declared).
    #[arg(
        long = "backfill-include-undercounts",
        env = "SOLPARQ_BACKFILL_INCLUDE_UNDERCOUNTS",
        default_value_t = true
    )]
    backfill_include_undercounts: bool,

    #[arg(
        long = "delete-archived-data-range",
        env = "SOLPARQ_DELETE_ARCHIVED_DATA_RANGE",
        default_value_t = false
    )]
    delete_archived_data_range: bool,

    /// Attempt to repair overcount transaction mismatches before archiving by
    /// running `OPTIMIZE ... FINAL DEDUPLICATE` on affected epoch partitions,
    /// then re-validating. Undercount mismatches (missing rows) still require
    /// re-ingestion and are left for an operator.
    #[arg(
        long = "repair-mismatches",
        env = "SOLPARQ_REPAIR_MISMATCHES",
        default_value_t = false
    )]
    repair_mismatches: bool,

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

    #[arg(
        long = "archive-slot-range",
        env = "SOLPARQ_ARCHIVE_SLOT_RANGE",
        value_name = "START-END"
    )]
    archive_slot_range: Option<ArchiveSlotRange>,

    #[arg(
        long = "no-continue-from-last-archive",
        env = "SOLPARQ_NO_CONTINUE_FROM_LAST_ARCHIVE",
        default_value_t = false
    )]
    no_continue_from_last_archive: bool,

    #[arg(long = "log-file", env = "SOLPARQ_LOG_FILE", value_name = "PATH")]
    log_file: Option<PathBuf>,

    #[arg(short, long, action = ArgAction::Count)]
    verbose: u8,

    /// Plan and validate archives without writing any files, deleting
    /// ClickHouse data, or creating done markers. Works in both one-shot and
    /// `--server-mode`; validation still runs read-only ClickHouse/RPC
    /// queries, and reports show what would have been archived.
    #[arg(long = "dry-run", env = "SOLPARQ_DRY_RUN", default_value_t = false)]
    dry_run: bool,
}

fn parse_archive_kinds(values: &[String], custom_slot_range: u64) -> Result<Vec<ArchiveKind>> {
    if custom_slot_range == 0 {
        return Err(anyhow!("custom-slot-range must be greater than zero"));
    }
    let mut kinds = Vec::new();
    let mut seen = HashSet::new();
    let mut custom_slots = None;
    for value in values {
        let kind = if value.eq_ignore_ascii_case("custom") {
            ArchiveKind::Custom {
                slots: custom_slot_range,
            }
        } else {
            ArchiveKind::from_str(value).map_err(|err| anyhow!(err))?
        };
        if !seen.insert(kind) {
            return Err(anyhow!("duplicate archive range type '{kind}'"));
        }
        if let ArchiveKind::Custom { slots } = kind {
            if let Some(previous_slots) = custom_slots {
                if previous_slots != slots {
                    return Err(anyhow!(
                        "only one custom archive range size can be configured at a time; custom archives share the custom_* storage namespace"
                    ));
                }
            } else {
                custom_slots = Some(slots);
            }
        }
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
