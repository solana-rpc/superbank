// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Restore source: read `superbank-solparq` Parquet archive bundles (local FS or
//! S3) and load every archived table back into ClickHouse.
//!
//! This is the inverse of the `superbank-solparq` archiver, which exports each
//! ClickHouse table with `SELECT * ... FORMAT Parquet`. Loading is done
//! ClickHouse-natively so it is schema-symmetric by construction and needs no
//! in-process Arrow decoding. Both paths match columns by *name* so a
//! destination whose column order has drifted from the archive still restores
//! correctly (rather than silently landing in the wrong columns):
//!
//! * **S3** — `INSERT INTO <table> (<cols>) SELECT <cols> FROM s3(<url>, <key>,
//!   <secret>, 'Parquet')`, where `<cols>` is read from the archived file's own
//!   schema, letting ClickHouse pull the object directly.
//! * **Local** — `INSERT INTO <table> FORMAT Parquet` with the bundle's parquet
//!   file streamed as the HTTP request body (name-matched by ClickHouse's
//!   Parquet reader).
//!
//! Every bundle is version-gated against [`MANIFEST_FORMAT_VERSION`]: archives
//! written by a newer producer are refused rather than misread.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail};
use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, path::Path as ObjectPath};
use serde::Deserialize;
use tokio::sync::watch;
use tokio_util::io::ReaderStream;

use superbank_solparq::manifest::{MANIFEST_FILE_NAME, MANIFEST_FORMAT_VERSION};
use superbank_solparq::read::source::{S3ArchiveConfig, build_s3_store};

use crate::cli::{Args, SolparqArchiveLocation};
use crate::metrics;
use crate::shutdown::spawn_shutdown_watch;

const TRANSACTIONS_KIND: &str = "transactions";
const BLOCKS_KIND: &str = "blocks_metadata";

/// Lenient view of a bundle's `manifest.json`. `format_version` and `producer`
/// are optional so legacy v1 bundles (written before those fields existed) still
/// deserialize; the version gate keys off `format_version` being absent or `<=`
/// [`MANIFEST_FORMAT_VERSION`].
#[derive(Debug, Clone, Deserialize)]
struct RestoreManifest {
    #[serde(default)]
    format_version: Option<u32>,
    #[serde(default)]
    producer: Option<ManifestProducer>,
    #[serde(default)]
    archive_id: Option<String>,
    #[serde(default)]
    start_slot: Option<u64>,
    #[serde(default)]
    end_slot: Option<u64>,
    #[serde(default)]
    tables: Vec<ManifestTable>,
}

#[derive(Debug, Clone, Deserialize)]
struct ManifestProducer {
    #[serde(default)]
    name: String,
    #[serde(default)]
    version: String,
    #[serde(default)]
    git_sha: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ManifestTable {
    kind: String,
    table_name: String,
    file_name: String,
    #[serde(default)]
    row_count: u64,
    #[serde(default)]
    required: bool,
}

#[derive(Debug, Default, Clone, Copy)]
struct RestoreStats {
    bundles: u64,
    tables: u64,
    rows: u64,
}

pub(crate) async fn run_solparq_ingest(args: &Args) -> Result<()> {
    let location = args
        .solparq_archive_location
        .ok_or_else(|| anyhow!("solparq source requires --solparq-archive-location"))?;
    let started = Instant::now();
    let shutdown_rx = spawn_shutdown_watch();
    let client = ChRestClient::from_args(args)?;

    eprintln!(
        "solparq restore -> ClickHouse {} (database: {})",
        args.clickhouse_url, args.clickhouse_database
    );
    print_active_filters(args);

    // A restore failure prints an inline "FAILED" marker on the offending table
    // and propagates a contextual error; the success summary is only reached
    // when every selected table loaded.
    let stats = match location {
        SolparqArchiveLocation::Local => restore_local(args, &client, &shutdown_rx).await?,
        SolparqArchiveLocation::S3 => restore_s3(args, &client, &shutdown_rx).await?,
    };

    eprintln!(
        "\nrestore complete: {} bundle(s), {} table(s), {} rows in {}",
        stats.bundles,
        stats.tables,
        fmt_thousands(stats.rows),
        fmt_duration(started.elapsed())
    );
    Ok(())
}

/// Print the optional slot-window / table filters that narrow the restore, so
/// the operator can see up front that not everything in the archive will land.
fn print_active_filters(args: &Args) {
    match (args.solparq_from_slot, args.solparq_to_slot) {
        (Some(from), Some(to)) => eprintln!("  slot window: {from} - {to}"),
        (Some(from), None) => eprintln!("  slot window: >= {from}"),
        (None, Some(to)) => eprintln!("  slot window: <= {to}"),
        (None, None) => {}
    }
    if !args.solparq_tables.is_empty() {
        eprintln!("  tables: {}", args.solparq_tables.join(", "));
    }
    if !args.solparq_clickhouse_settings.trim().is_empty() {
        eprintln!(
            "  clickhouse settings: {}",
            args.solparq_clickhouse_settings
        );
    }
}

// --- local restore -------------------------------------------------------

struct LocalBundle {
    dir: PathBuf,
    id: String,
}

async fn restore_local(
    args: &Args,
    client: &ChRestClient,
    shutdown_rx: &watch::Receiver<u64>,
) -> Result<RestoreStats> {
    let root = args
        .solparq_archive_path
        .as_ref()
        .ok_or_else(|| anyhow!("solparq local archive requires --solparq-archive-path"))?;
    eprintln!("  source: local {}", root.display());
    eprintln!("\nscanning for bundles...");
    let bundles = discover_local_bundles(root)
        .with_context(|| format!("discover solparq bundles in {}", root.display()))?;
    let total = bundles.len();
    if total == 0 {
        eprintln!("no solparq bundles found under {}", root.display());
        return Ok(RestoreStats::default());
    }
    eprintln!("found {total} bundle(s)");

    let mut stats = RestoreStats::default();
    for (index, bundle) in bundles.into_iter().enumerate() {
        if is_shutdown(shutdown_rx) {
            eprintln!(
                "\nshutdown requested; stopping after {} bundle(s)",
                stats.bundles
            );
            break;
        }
        let manifest = read_local_manifest(&bundle.dir)?;
        let id = manifest.archive_id.clone().unwrap_or(bundle.id.clone());
        gate_manifest_version(&manifest, &id)?;
        print_bundle_header(index + 1, total, &id, &manifest);
        if !bundle_in_range(&manifest, args.solparq_from_slot, args.solparq_to_slot) {
            eprintln!("  (outside requested slot window; skipping)");
            continue;
        }
        stats.bundles += 1;

        for table in &manifest.tables {
            if !table_selected(&table.kind, &args.solparq_tables) {
                eprintln!("  - {:<20} skipped (not in --solparq-tables)", table.kind);
                continue;
            }
            let file = bundle.dir.join(&table.file_name);
            if !file.is_file() {
                if table.required {
                    eprintln!("  - {:<20} FAILED (required file missing)", table.kind);
                    bail!(
                        "bundle {id} is missing required table file {} ({})",
                        table.file_name,
                        table.kind
                    );
                }
                eprintln!("  - {:<20} skipped (file missing)", table.kind);
                continue;
            }
            let size = std::fs::metadata(&file).map(|meta| meta.len()).unwrap_or(0);
            eprint!(
                "  - {:<20} {} rows, {} ... ",
                table.kind,
                fmt_thousands(table.row_count),
                fmt_bytes(size)
            );
            let dest = resolve_dest_table(&args.clickhouse_database, &table.table_name);
            let started = Instant::now();
            match client
                .insert_parquet_file(&dest, &args.solparq_clickhouse_settings, &file)
                .await
            {
                Ok(()) => eprintln!("done ({})", fmt_duration(started.elapsed())),
                Err(err) => {
                    eprintln!("FAILED");
                    return Err(err).with_context(|| {
                        format!("restore {} from {}", table.kind, file.display())
                    });
                }
            }
            observe_restored(&table.kind, &dest, table.row_count, manifest.end_slot);
            stats.tables += 1;
            stats.rows += table.row_count;
        }
    }
    Ok(stats)
}

/// A bundle is either the path itself (when it directly holds a `manifest.json`)
/// or each immediate child directory that does. Local bundles are laid out flat
/// as `<root>/<archive-id>/` by the archiver.
fn discover_local_bundles(root: &Path) -> Result<Vec<LocalBundle>> {
    if root.join(MANIFEST_FILE_NAME).is_file() {
        let id = root
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();
        return Ok(vec![LocalBundle {
            dir: root.to_path_buf(),
            id,
        }]);
    }

    let mut bundles = Vec::new();
    for entry in std::fs::read_dir(root)
        .with_context(|| format!("read solparq archive directory {}", root.display()))?
    {
        let entry = entry?;
        let dir = entry.path();
        if dir.is_dir() && dir.join(MANIFEST_FILE_NAME).is_file() {
            let id = entry.file_name().to_string_lossy().into_owned();
            bundles.push(LocalBundle { dir, id });
        }
    }
    bundles.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(bundles)
}

fn read_local_manifest(dir: &Path) -> Result<RestoreManifest> {
    let path = dir.join(MANIFEST_FILE_NAME);
    let bytes = std::fs::read(&path)
        .with_context(|| format!("read solparq manifest {}", path.display()))?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("parse solparq manifest {}", path.display()))
}

// --- S3 restore ----------------------------------------------------------

struct S3Bundle {
    prefix: ObjectPath,
    id: String,
}

async fn restore_s3(
    args: &Args,
    client: &ChRestClient,
    shutdown_rx: &watch::Receiver<u64>,
) -> Result<RestoreStats> {
    let config = s3_config_from_args(args)?;
    let store = build_s3_store(&config).context("build solparq S3 store")?;
    let source = if config.bucket_path.is_empty() {
        config.bucket_name.clone()
    } else {
        format!("{}/{}", config.bucket_name, config.bucket_path)
    };
    eprintln!("  source: s3 {source} (endpoint {})", config.endpoint);
    eprintln!("\nscanning for bundles...");
    let bundles = discover_s3_bundles(&store, &config).await?;
    let total = bundles.len();
    if total == 0 {
        eprintln!("no solparq bundles found under s3 {source}");
        return Ok(RestoreStats::default());
    }
    eprintln!("found {total} bundle(s)");

    let mut stats = RestoreStats::default();
    for (index, bundle) in bundles.into_iter().enumerate() {
        if is_shutdown(shutdown_rx) {
            eprintln!(
                "\nshutdown requested; stopping after {} bundle(s)",
                stats.bundles
            );
            break;
        }
        let manifest = read_s3_manifest(&store, &bundle.prefix).await?;
        let id = manifest.archive_id.clone().unwrap_or(bundle.id.clone());
        gate_manifest_version(&manifest, &id)?;
        print_bundle_header(index + 1, total, &id, &manifest);
        if !bundle_in_range(&manifest, args.solparq_from_slot, args.solparq_to_slot) {
            eprintln!("  (outside requested slot window; skipping)");
            continue;
        }
        stats.bundles += 1;

        for table in &manifest.tables {
            if !table_selected(&table.kind, &args.solparq_tables) {
                eprintln!("  - {:<20} skipped (not in --solparq-tables)", table.kind);
                continue;
            }
            let object = object_join(&bundle.prefix, &table.file_name);
            let url = s3_object_url(&config.endpoint, &config.bucket_name, object.as_ref());
            let s3_object = S3Object {
                url: &url,
                access_key: &config.auth_key,
                secret_key: &config.auth_secret_key,
            };
            let dest = resolve_dest_table(&args.clickhouse_database, &table.table_name);
            eprint!(
                "  - {:<20} {} rows ... ",
                table.kind,
                fmt_thousands(table.row_count)
            );
            let started = Instant::now();
            // Read the archive's own column order so the INSERT matches by name,
            // not position — a destination whose column order has drifted from
            // the archive would otherwise be silently mis-columned.
            let columns = match client.describe_s3_columns(&s3_object).await {
                Ok(columns) => columns,
                Err(err) => {
                    eprintln!("FAILED");
                    return Err(err)
                        .with_context(|| format!("describe {} schema from {url}", table.kind));
                }
            };
            let sql = build_s3_restore_sql(
                &dest,
                &columns,
                &s3_object,
                args.solparq_from_slot,
                args.solparq_to_slot,
                &args.solparq_clickhouse_settings,
            );
            match client.execute_sql(&sql).await {
                Ok(()) => eprintln!("done ({})", fmt_duration(started.elapsed())),
                Err(err) => {
                    eprintln!("FAILED");
                    return Err(err).with_context(|| format!("restore {} from {url}", table.kind));
                }
            }
            observe_restored(&table.kind, &dest, table.row_count, manifest.end_slot);
            stats.tables += 1;
            stats.rows += table.row_count;
        }
    }
    Ok(stats)
}

fn s3_config_from_args(args: &Args) -> Result<S3ArchiveConfig> {
    Ok(S3ArchiveConfig {
        endpoint: require_s3(
            &args.solparq_archive_s3_endpoint,
            "solparq-archive-s3-endpoint",
        )?,
        bucket_name: require_s3(
            &args.solparq_archive_s3_bucket_name,
            "solparq-archive-s3-bucket-name",
        )?,
        bucket_path: args
            .solparq_archive_s3_bucket_path
            .clone()
            .unwrap_or_default(),
        auth_key: require_s3(
            &args.solparq_archive_s3_auth_key,
            "solparq-archive-s3-auth-key",
        )?,
        auth_secret_key: require_s3(
            &args.solparq_archive_s3_auth_secret_key,
            "solparq-archive-s3-auth-secret-key",
        )?,
        region: args.solparq_archive_s3_region.clone(),
    })
}

fn require_s3(value: &Option<String>, name: &str) -> Result<String> {
    match value.as_deref().map(str::trim) {
        Some(value) if !value.is_empty() => Ok(value.to_string()),
        _ => Err(anyhow!("solparq s3 archive requires --{name}")),
    }
}

/// List every bundle under the configured bucket path. A bundle is any prefix
/// that contains a `manifest.json` object; the archiver writes them at
/// `<bucket-path>/<kind>/<archive-id>/`.
async fn discover_s3_bundles(
    store: &Arc<dyn ObjectStore>,
    config: &S3ArchiveConfig,
) -> Result<Vec<S3Bundle>> {
    let prefix = config.prefix();
    let mut stream = store.list(prefix.as_ref());
    let mut bundles = Vec::new();
    let manifest_suffix = format!("/{MANIFEST_FILE_NAME}");
    while let Some(meta) = stream
        .try_next()
        .await
        .context("list solparq S3 archive objects")?
    {
        let location = meta.location.as_ref();
        if let Some(prefix_str) = location.strip_suffix(&manifest_suffix) {
            let id = prefix_str
                .rsplit('/')
                .next()
                .unwrap_or(prefix_str)
                .to_string();
            bundles.push(S3Bundle {
                prefix: ObjectPath::from(prefix_str),
                id,
            });
        }
    }
    bundles.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(bundles)
}

async fn read_s3_manifest(
    store: &Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
) -> Result<RestoreManifest> {
    let path = object_join(prefix, MANIFEST_FILE_NAME);
    let bytes = store
        .get(&path)
        .await
        .with_context(|| format!("read solparq S3 manifest {path}"))?
        .bytes()
        .await
        .with_context(|| format!("read solparq S3 manifest bytes {path}"))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parse solparq S3 manifest {path}"))
}

// --- ClickHouse HTTP client ---------------------------------------------

/// Minimal reqwest-based ClickHouse HTTP client. The typed `clickhouse` crate
/// client cannot stream a raw Parquet request body, so restore talks to the
/// HTTP interface directly (mirroring the archiver's own client).
struct ChRestClient {
    http: reqwest::Client,
    url: String,
    database: String,
    user: String,
    password: String,
}

impl ChRestClient {
    fn from_args(args: &Args) -> Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder()
                .build()
                .context("build ClickHouse HTTP client")?,
            url: args.clickhouse_url.clone(),
            database: args.clickhouse_database.clone(),
            user: args.clickhouse_user.clone(),
            password: args.clickhouse_password.clone(),
        })
    }

    /// Run a statement that carries no input data (e.g. `INSERT INTO ... (cols)
    /// SELECT cols FROM s3(...)`). The SQL travels in the request body.
    async fn execute_sql(&self, sql: &str) -> Result<()> {
        let mut request = self
            .http
            .post(&self.url)
            .query(&[("database", self.database.as_str())])
            .body(sql.to_string());
        if !self.user.is_empty() {
            request = request.basic_auth(&self.user, Some(&self.password));
        }
        let response = request
            .send()
            .await
            .with_context(|| format!("send ClickHouse query to {}", self.url))?;
        ensure_success(response).await
    }

    /// Read the ordered column names of an archived Parquet object from its own
    /// schema via `DESCRIBE TABLE s3(...)`. ClickHouse only reads the Parquet
    /// footer for this, so it is cheap. The names drive a name-matched restore
    /// INSERT (see [`build_s3_restore_sql`]).
    async fn describe_s3_columns(&self, object: &S3Object<'_>) -> Result<Vec<String>> {
        let sql = format!(
            "DESCRIBE TABLE {table_fn} FORMAT TabSeparated",
            table_fn = object.table_function(),
        );
        let body = self.query_text(&sql).await?;
        let columns = parse_describe_columns(&body);
        if columns.is_empty() {
            bail!(
                "DESCRIBE returned no columns for archived object {}",
                object.url
            );
        }
        Ok(columns)
    }

    /// Run a statement that returns a result set and collect its response body as
    /// text (mirrors [`Self::execute_sql`], but keeps the body).
    async fn query_text(&self, sql: &str) -> Result<String> {
        let mut request = self
            .http
            .post(&self.url)
            .query(&[("database", self.database.as_str())])
            .body(sql.to_string());
        if !self.user.is_empty() {
            request = request.basic_auth(&self.user, Some(&self.password));
        }
        let response = request
            .send()
            .await
            .with_context(|| format!("send ClickHouse query to {}", self.url))?;
        if !response.status().is_success() {
            return ensure_success(response).await.map(|()| String::new());
        }
        response
            .text()
            .await
            .with_context(|| format!("read ClickHouse response from {}", self.url))
    }

    /// Stream a local Parquet file into `dest` via `INSERT INTO <dest> FORMAT
    /// Parquet`. The statement is passed as the `query` parameter and the file
    /// bytes stream as the request body so large files are never buffered whole.
    async fn insert_parquet_file(&self, dest: &str, settings: &str, path: &Path) -> Result<()> {
        let insert_sql = build_local_insert_sql(dest, settings);
        let file = tokio::fs::File::open(path)
            .await
            .with_context(|| format!("open parquet file {}", path.display()))?;
        let body = reqwest::Body::wrap_stream(ReaderStream::new(file));
        let mut request = self
            .http
            .post(&self.url)
            .query(&[
                ("database", self.database.as_str()),
                ("query", insert_sql.as_str()),
            ])
            .body(body);
        if !self.user.is_empty() {
            request = request.basic_auth(&self.user, Some(&self.password));
        }
        let response = request
            .send()
            .await
            .with_context(|| format!("send ClickHouse parquet insert to {}", self.url))?;
        ensure_success(response).await
    }
}

async fn ensure_success(response: reqwest::Response) -> Result<()> {
    if response.status().is_success() {
        return Ok(());
    }
    let status = response.status();
    let body = response
        .text()
        .await
        .unwrap_or_else(|_| "<failed to read response body>".to_string());
    Err(anyhow!(
        "ClickHouse request failed with status {status}: {body}"
    ))
}

// --- shared helpers ------------------------------------------------------

/// Refuse archives whose manifest declares a `format_version` newer than this
/// build understands. Absent (`None`, legacy v1) or `<=` current is accepted.
fn gate_manifest_version(manifest: &RestoreManifest, archive_id: &str) -> Result<()> {
    match manifest.format_version {
        None => Ok(()),
        Some(version) if version <= MANIFEST_FORMAT_VERSION => Ok(()),
        Some(version) => Err(anyhow!(
            "solparq archive {archive_id} manifest format_version {version} is newer than supported {MANIFEST_FORMAT_VERSION}; upgrade superbank to restore it"
        )),
    }
}

/// Print the per-bundle header line: position, id, slot span, and (when present)
/// the producer build that wrote the archive.
fn print_bundle_header(index: usize, total: usize, archive_id: &str, manifest: &RestoreManifest) {
    eprintln!(
        "\n[{index}/{total}] {archive_id}  ({}{})",
        slot_span(manifest),
        producer_suffix(manifest)
    );
}

fn slot_span(manifest: &RestoreManifest) -> String {
    match (manifest.start_slot, manifest.end_slot) {
        (Some(start), Some(end)) => format!("slots {start} - {end}"),
        (Some(start), None) => format!("from slot {start}"),
        (None, Some(end)) => format!("to slot {end}"),
        (None, None) => "slot range unknown".to_string(),
    }
}

fn producer_suffix(manifest: &RestoreManifest) -> String {
    match &manifest.producer {
        Some(producer) => {
            let sha = if producer.git_sha.is_empty() || producer.git_sha == "unknown" {
                String::new()
            } else {
                format!(" {}", &producer.git_sha[..producer.git_sha.len().min(12)])
            };
            format!(", producer {} {}{sha}", producer.name, producer.version)
        }
        None => String::new(),
    }
}

/// Group digits with commas, e.g. `1234567` -> `1,234,567`.
fn fmt_thousands(value: u64) -> String {
    let digits = value.to_string();
    let bytes = digits.as_bytes();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (offset, digit) in bytes.iter().enumerate() {
        if offset > 0 && (bytes.len() - offset).is_multiple_of(3) {
            out.push(',');
        }
        out.push(*digit as char);
    }
    out
}

/// Human-readable byte size, e.g. `47448064` -> `45.2 MiB`.
fn fmt_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}

/// Compact elapsed time, e.g. `412ms`, `3.1s`, `2m05s`.
fn fmt_duration(elapsed: Duration) -> String {
    let secs = elapsed.as_secs_f64();
    if secs < 1.0 {
        format!("{}ms", elapsed.as_millis())
    } else if secs < 60.0 {
        format!("{secs:.1}s")
    } else {
        let minutes = elapsed.as_secs() / 60;
        let seconds = elapsed.as_secs() % 60;
        format!("{minutes}m{seconds:02}s")
    }
}

/// Keep only bundles that overlap the requested slot window. Uses the manifest's
/// own slot bounds when present; when absent (or no window requested) the bundle
/// is kept.
fn bundle_in_range(manifest: &RestoreManifest, from: Option<u64>, to: Option<u64>) -> bool {
    if let (Some(from), Some(end)) = (from, manifest.end_slot)
        && end < from
    {
        return false;
    }
    if let (Some(to), Some(start)) = (to, manifest.start_slot)
        && start > to
    {
        return false;
    }
    true
}

fn table_selected(kind: &str, allow: &[String]) -> bool {
    allow.is_empty() || allow.iter().any(|allowed| allowed == kind)
}

/// Destination table: always the configured ClickHouse database with the bare
/// table name recorded in the manifest, so a restore lands in this ingestor's
/// database regardless of where the archive was produced.
fn resolve_dest_table(database: &str, manifest_table_name: &str) -> String {
    let table = manifest_table_name
        .rsplit('.')
        .next()
        .unwrap_or(manifest_table_name);
    format!("{database}.{table}")
}

fn observe_restored(kind: &str, dest: &str, rows: u64, end_slot: Option<u64>) {
    match kind {
        TRANSACTIONS_KIND => metrics::observe_transaction_insert(dest, rows as usize),
        BLOCKS_KIND => metrics::observe_block_insert(dest, rows as usize, end_slot),
        _ => metrics::observe_restored_rows(dest, rows),
    }
}

fn is_shutdown(shutdown_rx: &watch::Receiver<u64>) -> bool {
    *shutdown_rx.borrow() > 0
}

fn object_join(prefix: &ObjectPath, file_name: &str) -> ObjectPath {
    ObjectPath::from(format!(
        "{}/{}",
        prefix.as_ref().trim_end_matches('/'),
        file_name
    ))
}

fn s3_object_url(endpoint: &str, bucket: &str, key: &str) -> String {
    format!(
        "{}/{}/{}",
        endpoint.trim_end_matches('/'),
        bucket.trim_matches('/'),
        key.trim_start_matches('/')
    )
}

fn escape_sql_literal(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "\\'")
}

fn settings_suffix(settings: &str) -> String {
    let settings = settings.trim();
    if settings.is_empty() {
        String::new()
    } else {
        format!(" SETTINGS {settings}")
    }
}

fn slot_where_clause(from: Option<u64>, to: Option<u64>) -> String {
    match (from, to) {
        (Some(from), Some(to)) => format!(" WHERE slot BETWEEN {from} AND {to}"),
        (Some(from), None) => format!(" WHERE slot >= {from}"),
        (None, Some(to)) => format!(" WHERE slot <= {to}"),
        (None, None) => String::new(),
    }
}

/// An archived Parquet object addressed by ClickHouse's `s3` table function:
/// the object URL plus the credentials to read it. Grouped so restore helpers
/// pass one value instead of three parallel strings.
struct S3Object<'a> {
    url: &'a str,
    access_key: &'a str,
    secret_key: &'a str,
}

impl S3Object<'_> {
    /// Render the `s3('url', 'key', 'secret', 'Parquet')` table-function call,
    /// SQL-escaping each argument.
    fn table_function(&self) -> String {
        format!(
            "s3('{url}', '{key}', '{secret}', 'Parquet')",
            url = escape_sql_literal(self.url),
            key = escape_sql_literal(self.access_key),
            secret = escape_sql_literal(self.secret_key),
        )
    }
}

/// Inverse of `superbank-solparq`'s `build_s3_table_archive_sql`: pull the
/// archived object back into `dest` with ClickHouse's `s3` table function.
///
/// `columns` is the ordered column list read from the archived file's own schema
/// (see [`ChRestClient::describe_s3_columns`]). Emitting it explicitly on both
/// sides — `INSERT INTO dest (cols) SELECT cols FROM s3(...)` — makes the S3 path
/// match columns by *name*, mirroring the local `FORMAT Parquet` path. A bare
/// `SELECT *` matches by position, so if the destination's column order ever
/// drifts from the archive's, a restore would silently write into the wrong
/// columns (or error on a type mismatch); the explicit list prevents that.
fn build_s3_restore_sql(
    dest: &str,
    columns: &[String],
    object: &S3Object,
    from: Option<u64>,
    to: Option<u64>,
    settings: &str,
) -> String {
    let column_list = columns
        .iter()
        .map(|name| quote_identifier(name))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "INSERT INTO {dest} ({column_list}) SELECT {column_list} FROM {table_fn}{where_clause}{settings}",
        table_fn = object.table_function(),
        where_clause = slot_where_clause(from, to),
        settings = settings_suffix(settings),
    )
}

/// Backtick-quote a ClickHouse identifier, escaping embedded backticks, so
/// archived column names survive intact in an explicit column list.
fn quote_identifier(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Extract column names from a `DESCRIBE ... FORMAT TabSeparated` response. Each
/// row is `name<TAB>type<TAB>...`; only the leading name field is kept, in the
/// file's declared order.
fn parse_describe_columns(body: &str) -> Vec<String> {
    body.lines()
        .filter_map(|line| line.split('\t').next())
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .collect()
}

fn build_local_insert_sql(dest: &str, settings: &str) -> String {
    format!(
        "INSERT INTO {dest}{settings} FORMAT Parquet",
        settings = settings_suffix(settings)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest_with_version(version: Option<u32>) -> RestoreManifest {
        RestoreManifest {
            format_version: version,
            producer: None,
            archive_id: Some("hourly_1_0-8999".to_string()),
            start_slot: Some(0),
            end_slot: Some(8999),
            tables: Vec::new(),
        }
    }

    #[test]
    fn version_gate_accepts_legacy_and_current() {
        for version in [None, Some(0), Some(1), Some(MANIFEST_FORMAT_VERSION)] {
            let manifest = manifest_with_version(version);
            assert!(
                gate_manifest_version(&manifest, "archive").is_ok(),
                "version {version:?} should be accepted"
            );
        }
    }

    #[test]
    fn version_gate_rejects_newer() {
        let manifest = manifest_with_version(Some(MANIFEST_FORMAT_VERSION + 1));
        let err = gate_manifest_version(&manifest, "archive").unwrap_err();
        assert!(err.to_string().contains("newer than supported"));
    }

    #[test]
    fn resolve_dest_table_forces_configured_database() {
        assert_eq!(
            resolve_dest_table("mydb", "default.transactions"),
            "mydb.transactions"
        );
        assert_eq!(
            resolve_dest_table("mydb", "transactions"),
            "mydb.transactions"
        );
    }

    #[test]
    fn table_selection_defaults_to_all() {
        assert!(table_selected("gsfa", &[]));
        assert!(table_selected(
            "transactions",
            &["transactions".to_string(), "blocks_metadata".to_string()]
        ));
        assert!(!table_selected("gsfa", &["transactions".to_string()]));
    }

    #[test]
    fn bundle_range_skips_out_of_window() {
        let manifest = manifest_with_version(Some(2)); // slots 0..=8999
        assert!(bundle_in_range(&manifest, None, None));
        assert!(bundle_in_range(&manifest, Some(5000), Some(20000)));
        // Overlaps the requested window at its lower edge.
        assert!(bundle_in_range(&manifest, None, Some(0)));
        // Entirely before the requested start (end 8999 < from 9000).
        assert!(!bundle_in_range(&manifest, Some(9000), None));

        // A later bundle (slots 10000..=18999) is skipped when the window ends
        // before it starts.
        let mut later = manifest_with_version(Some(2));
        later.start_slot = Some(10000);
        later.end_slot = Some(18999);
        assert!(!bundle_in_range(&later, None, Some(5000)));
        assert!(bundle_in_range(&later, None, Some(12000)));
    }

    #[test]
    fn s3_url_joins_cleanly() {
        assert_eq!(
            s3_object_url("http://minio:9000/", "bucket", "/path/to/x.parquet"),
            "http://minio:9000/bucket/path/to/x.parquet"
        );
    }

    #[test]
    fn s3_restore_sql_is_inverse_of_export() {
        let columns = vec!["slot".to_string(), "signature".to_string()];
        let object = S3Object {
            url: "http://minio:9000/bucket/hourly/id/transactions.parquet",
            access_key: "ak",
            secret_key: "sk",
        };
        let sql = build_s3_restore_sql(
            "db.transactions",
            &columns,
            &object,
            Some(10),
            Some(20),
            "max_insert_threads = 4",
        );
        // Explicit, name-matched column list on both the INSERT and SELECT so a
        // drifted destination column order can't silently mis-column the data.
        assert!(sql.starts_with(
            "INSERT INTO db.transactions (`slot`, `signature`) SELECT `slot`, `signature` FROM s3("
        ));
        assert!(!sql.contains("SELECT * FROM"));
        assert!(sql.contains("'Parquet')"));
        assert!(sql.contains("WHERE slot BETWEEN 10 AND 20"));
        assert!(sql.ends_with("SETTINGS max_insert_threads = 4"));
    }

    #[test]
    fn s3_restore_sql_escapes_quotes() {
        let columns = vec!["slot".to_string()];
        let object = S3Object {
            url: "u",
            access_key: "a'b",
            secret_key: "s",
        };
        let sql = build_s3_restore_sql("db.t", &columns, &object, None, None, "");
        assert!(sql.contains("'a\\'b'"));
        assert!(!sql.contains("WHERE"));
        assert!(!sql.contains("SETTINGS"));
    }

    #[test]
    fn quote_identifier_backticks_and_escapes() {
        assert_eq!(quote_identifier("slot"), "`slot`");
        assert_eq!(quote_identifier("odd`name"), "`odd``name`");
    }

    #[test]
    fn parse_describe_columns_keeps_names_in_order() {
        // `DESCRIBE ... FORMAT TabSeparated` rows: name<TAB>type<TAB>...
        let body = "slot\tUInt64\t\t\t\t\t\nsignature\tString\t\t\t\t\t\ndata\tString\t\t\t\t\t\n";
        assert_eq!(
            parse_describe_columns(body),
            vec![
                "slot".to_string(),
                "signature".to_string(),
                "data".to_string()
            ]
        );
        assert!(parse_describe_columns("").is_empty());
    }

    #[test]
    fn formatters_render_human_readable_values() {
        assert_eq!(fmt_thousands(0), "0");
        assert_eq!(fmt_thousands(999), "999");
        assert_eq!(fmt_thousands(1_234_567), "1,234,567");

        assert_eq!(fmt_bytes(512), "512 B");
        assert_eq!(fmt_bytes(1024), "1.0 KiB");
        assert_eq!(fmt_bytes(47_448_064), "45.2 MiB");

        assert_eq!(fmt_duration(Duration::from_millis(412)), "412ms");
        assert_eq!(fmt_duration(Duration::from_millis(3100)), "3.1s");
        assert_eq!(fmt_duration(Duration::from_secs(125)), "2m05s");
    }

    #[test]
    fn local_insert_sql_appends_format_and_settings() {
        assert_eq!(
            build_local_insert_sql("db.t", ""),
            "INSERT INTO db.t FORMAT Parquet"
        );
        assert_eq!(
            build_local_insert_sql("db.t", "max_threads = 2"),
            "INSERT INTO db.t SETTINGS max_threads = 2 FORMAT Parquet"
        );
    }

    #[test]
    fn manifest_parses_v2_json() {
        let json = r#"{
            "format_version": 2,
            "producer": {"name": "superbank-solparq", "version": "0.5.0", "git_sha": "abc"},
            "archive_id": "hourly_988_427236024-427245023",
            "archive_kind": "hourly",
            "epoch": 988,
            "start_slot": 427236024,
            "end_slot": 427245023,
            "tables": [
                {"kind": "transactions", "table_name": "default.transactions", "file_name": "transactions.parquet", "row_count": 42, "required": true}
            ],
            "skipped_tables": [],
            "poh_tool_ready": false
        }"#;
        let manifest: RestoreManifest = serde_json::from_str(json).expect("parse manifest");
        assert_eq!(manifest.format_version, Some(2));
        assert_eq!(manifest.start_slot, Some(427236024));
        assert_eq!(manifest.tables.len(), 1);
        assert_eq!(manifest.tables[0].kind, "transactions");
        assert_eq!(manifest.tables[0].table_name, "default.transactions");
        assert_eq!(manifest.tables[0].row_count, 42);
    }

    #[test]
    fn manifest_parses_legacy_v1_json() {
        let json = r#"{
            "archive_id": "hourly_1_0-8999",
            "start_slot": 0,
            "end_slot": 8999,
            "tables": [
                {"kind": "transactions", "table_name": "transactions", "file_name": "transactions.parquet", "row_count": 1, "required": true}
            ]
        }"#;
        let manifest: RestoreManifest = serde_json::from_str(json).expect("parse legacy manifest");
        assert_eq!(manifest.format_version, None);
        assert!(manifest.producer.is_none());
        assert!(gate_manifest_version(&manifest, "legacy").is_ok());
    }
}
