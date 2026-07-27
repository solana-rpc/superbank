use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, anyhow};
use futures_util::TryStreamExt;
use object_store::{
    ObjectStore, ObjectStoreExt, PutPayload, aws::AmazonS3Builder, path::Path as ObjectPath,
};
use sha2::{Digest, Sha256};
use tokio::fs;
use tokio::io::AsyncReadExt;

use crate::{
    archive::{ArchiveKind, parse_archive_name, report_path_for_local_archive},
    config::{ArchiveLocation, Config, S3Config},
    manifest::{
        ArchiveManifest, DONE_FILE_PREFIX, MANIFEST_FILE_NAME, REPORT_FILE_NAME,
        SHA256SUMS_FILE_NAME,
    },
};

#[derive(Debug, Clone)]
pub enum ArchiveDestination {
    Local { directory: PathBuf },
    S3 { bucket: String, prefix: String },
}

impl ArchiveDestination {
    pub fn describe(&self) -> String {
        match self {
            ArchiveDestination::Local { directory } => directory.display().to_string(),
            ArchiveDestination::S3 { bucket, prefix } => format!("s3://{bucket}/{prefix}"),
        }
    }
}

pub async fn destination(config: &Config, kind: ArchiveKind) -> Result<ArchiveDestination> {
    match config.archive_location {
        ArchiveLocation::Local => Ok(ArchiveDestination::Local {
            directory: config.output_location.clone(),
        }),
        ArchiveLocation::S3 => {
            let s3 = config
                .s3
                .as_ref()
                .ok_or_else(|| anyhow!("archive-location-type=s3 requires S3 config"))?;
            Ok(ArchiveDestination::S3 {
                bucket: s3.bucket_name.clone(),
                prefix: s3_prefix_for_kind(s3, kind),
            })
        }
    }
}

pub async fn latest_archive_name(config: &Config, kind: ArchiveKind) -> Result<Option<String>> {
    let mut archives = match config.archive_location {
        ArchiveLocation::Local => list_local_archive_names(&config.output_location, kind).await?,
        ArchiveLocation::S3 => list_s3_archive_names(config, kind).await?,
    };
    archives.sort_by_key(|name| {
        parse_archive_name(name)
            .map(|parsed| parsed.end_slot)
            .unwrap_or_default()
    });
    Ok(archives.pop())
}

pub async fn cleanup_archives(config: &Config, kind: ArchiveKind) -> Result<Vec<String>> {
    if config.archives_to_keep == 0 {
        return Ok(Vec::new());
    }
    match config.archive_location {
        ArchiveLocation::Local => {
            let paths =
                local_archives_to_delete(&config.output_location, kind, config.archives_to_keep)?;
            let mut deleted = Vec::new();
            for path in paths {
                let name = path
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned())
                    .unwrap_or_else(|| path.display().to_string());
                if path.is_dir() {
                    fs::remove_dir_all(&path).await.with_context(|| {
                        format!("delete local archive bundle {}", path.display())
                    })?;
                } else {
                    fs::remove_file(&path)
                        .await
                        .with_context(|| format!("delete local archive {}", path.display()))?;
                }
                let report_path =
                    report_path_for_local_archive(config.output_location.clone(), &name);
                let _ = fs::remove_file(report_path).await;
                deleted.push(name);
            }
            Ok(deleted)
        }
        ArchiveLocation::S3 => cleanup_s3_archives(config, kind).await,
    }
}

pub async fn archive_has_done_marker(
    config: &Config,
    kind: ArchiveKind,
    archive_id: &str,
) -> Result<bool> {
    match config.archive_location {
        ArchiveLocation::Local => {
            let bundle_dir = local_bundle_dir(&config.output_location, archive_id);
            if !bundle_dir.exists() {
                return Ok(false);
            }
            let mut entries = fs::read_dir(&bundle_dir)
                .await
                .with_context(|| format!("scan archive bundle {}", bundle_dir.display()))?;
            while let Some(entry) = entries.next_entry().await? {
                if entry
                    .file_name()
                    .to_str()
                    .map(is_done_file_name)
                    .unwrap_or(false)
                {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        ArchiveLocation::S3 => {
            let s3 = config
                .s3
                .as_ref()
                .ok_or_else(|| anyhow!("archive-location-type=s3 requires S3 config"))?;
            let store = build_s3_store(s3)?;
            let bundle_prefix =
                ObjectPath::from(format!("{}/{}/", s3_prefix_for_kind(s3, kind), archive_id));
            let objects = store
                .list(Some(&bundle_prefix))
                .try_collect::<Vec<_>>()
                .await
                .with_context(|| format!("list S3 archive bundle {}", bundle_prefix.as_ref()))?;
            Ok(objects.iter().any(|object| {
                object
                    .location
                    .filename()
                    .map(is_done_file_name)
                    .unwrap_or(false)
            }))
        }
    }
}

pub async fn remove_archive_bundle(
    config: &Config,
    kind: ArchiveKind,
    archive_id: &str,
) -> Result<bool> {
    match config.archive_location {
        ArchiveLocation::Local => {
            let bundle_dir = local_bundle_dir(&config.output_location, archive_id);
            if !bundle_dir.exists() {
                return Ok(false);
            }
            if bundle_dir.is_dir() {
                fs::remove_dir_all(&bundle_dir)
                    .await
                    .with_context(|| format!("remove archive bundle {}", bundle_dir.display()))?;
            } else {
                fs::remove_file(&bundle_dir)
                    .await
                    .with_context(|| format!("remove archive bundle {}", bundle_dir.display()))?;
            }
            Ok(true)
        }
        ArchiveLocation::S3 => {
            let s3 = config
                .s3
                .as_ref()
                .ok_or_else(|| anyhow!("archive-location-type=s3 requires S3 config"))?;
            let store = build_s3_store(s3)?;
            let bundle_prefix =
                ObjectPath::from(format!("{}/{}/", s3_prefix_for_kind(s3, kind), archive_id));
            let objects = store
                .list(Some(&bundle_prefix))
                .try_collect::<Vec<_>>()
                .await
                .with_context(|| format!("list S3 archive bundle {}", bundle_prefix.as_ref()))?;
            let removed = !objects.is_empty();
            for object in objects {
                store.delete(&object.location).await.with_context(|| {
                    format!(
                        "remove S3 archive bundle object {}",
                        object.location.as_ref()
                    )
                })?;
            }
            Ok(removed)
        }
    }
}

pub async fn write_done_marker(
    config: &Config,
    kind: ArchiveKind,
    archive_id: &str,
    hostname: &str,
    timestamp_utc: &str,
) -> Result<()> {
    let file_name = done_file_name(hostname);
    let contents = format!("hostname: {hostname}\ntimestamp_utc: {timestamp_utc}\n");
    match config.archive_location {
        ArchiveLocation::Local => {
            let bundle_dir = local_bundle_dir(&config.output_location, archive_id);
            fs::create_dir_all(&bundle_dir).await?;
            let path = bundle_dir.join(file_name);
            fs::write(&path, contents)
                .await
                .with_context(|| format!("write archive done marker {}", path.display()))?;
        }
        ArchiveLocation::S3 => {
            let s3 = config
                .s3
                .as_ref()
                .ok_or_else(|| anyhow!("archive-location-type=s3 requires S3 config"))?;
            let store = build_s3_store(s3)?;
            let key = object_path_for_bundle_file(s3, kind, archive_id, &file_name);
            store
                .put(&key, PutPayload::from(contents.into_bytes()))
                .await
                .with_context(|| format!("write S3 done marker {}", key.as_ref()))?;
        }
    }
    Ok(())
}

pub async fn write_local_sha256sums(bundle_dir: &Path, file_names: &[String]) -> Result<()> {
    let mut lines = Vec::with_capacity(file_names.len());
    for file_name in file_names {
        let path = bundle_dir.join(file_name);
        let sha256 = sha256_local_file(&path).await?;
        lines.push(format!("{sha256}  {file_name}"));
    }
    let mut contents = lines.join("\n");
    contents.push('\n');
    let path = bundle_dir.join(SHA256SUMS_FILE_NAME);
    fs::write(&path, contents)
        .await
        .with_context(|| format!("write archive checksums {}", path.display()))?;
    Ok(())
}

pub async fn write_s3_sha256sums(
    config: &Config,
    kind: ArchiveKind,
    archive_id: &str,
    file_names: &[String],
) -> Result<()> {
    let s3 = config
        .s3
        .as_ref()
        .ok_or_else(|| anyhow!("archive-location-type=s3 requires S3 config"))?;
    let store = build_s3_store(s3)?;
    let mut lines = Vec::with_capacity(file_names.len());
    for file_name in file_names {
        let key = object_path_for_bundle_file(s3, kind, archive_id, file_name);
        let bytes = store
            .get(&key)
            .await
            .with_context(|| format!("read S3 archive object {}", key.as_ref()))?
            .bytes()
            .await
            .with_context(|| format!("read S3 archive object bytes {}", key.as_ref()))?;
        let sha256 = sha256_bytes(&bytes);
        lines.push(format!("{sha256}  {file_name}"));
    }
    let mut contents = lines.join("\n");
    contents.push('\n');
    let key = object_path_for_bundle_file(s3, kind, archive_id, SHA256SUMS_FILE_NAME);
    store
        .put(&key, PutPayload::from(contents.into_bytes()))
        .await
        .with_context(|| format!("write S3 checksums {}", key.as_ref()))?;
    Ok(())
}

pub async fn write_report(
    config: &Config,
    kind: ArchiveKind,
    archive_name: &str,
    report: &str,
) -> Result<()> {
    match config.archive_location {
        ArchiveLocation::Local => {
            let path = if archive_name.ends_with(".parquet") {
                fs::create_dir_all(&config.output_location).await?;
                report_path_for_local_archive(config.output_location.clone(), archive_name)
            } else {
                let bundle_dir = local_bundle_dir(&config.output_location, archive_name);
                fs::create_dir_all(&bundle_dir).await?;
                bundle_dir.join(REPORT_FILE_NAME)
            };
            fs::write(&path, report)
                .await
                .with_context(|| format!("write archive report {}", path.display()))?;
        }
        ArchiveLocation::S3 => {
            let s3 = config
                .s3
                .as_ref()
                .ok_or_else(|| anyhow!("archive-location-type=s3 requires S3 config"))?;
            let store = build_s3_store(s3)?;
            let key = if archive_name.ends_with(".parquet") {
                object_path_for_report(s3, kind, archive_name)
            } else {
                object_path_for_bundle_file(s3, kind, archive_name, REPORT_FILE_NAME)
            };
            store
                .put(&key, PutPayload::from(report.as_bytes().to_vec()))
                .await
                .with_context(|| format!("write S3 report {}", key.as_ref()))?;
        }
    }
    Ok(())
}

pub async fn write_manifest(
    config: &Config,
    kind: ArchiveKind,
    archive_id: &str,
    manifest: &ArchiveManifest,
) -> Result<()> {
    let json = serde_json::to_vec_pretty(manifest).context("serialize archive manifest")?;
    match config.archive_location {
        ArchiveLocation::Local => {
            let bundle_dir = local_bundle_dir(&config.output_location, archive_id);
            fs::create_dir_all(&bundle_dir).await?;
            let path = bundle_dir.join(MANIFEST_FILE_NAME);
            fs::write(&path, json)
                .await
                .with_context(|| format!("write archive manifest {}", path.display()))?;
        }
        ArchiveLocation::S3 => {
            let s3 = config
                .s3
                .as_ref()
                .ok_or_else(|| anyhow!("archive-location-type=s3 requires S3 config"))?;
            let store = build_s3_store(s3)?;
            let key = object_path_for_bundle_file(s3, kind, archive_id, MANIFEST_FILE_NAME);
            store
                .put(&key, PutPayload::from(json))
                .await
                .with_context(|| format!("write S3 manifest {}", key.as_ref()))?;
        }
    }
    Ok(())
}

pub fn local_archives_to_delete(
    directory: &Path,
    kind: ArchiveKind,
    archives_to_keep: usize,
) -> Result<Vec<PathBuf>> {
    if archives_to_keep == 0 || !directory.exists() {
        return Ok(Vec::new());
    }
    let mut archives = Vec::new();
    for entry in std::fs::read_dir(directory)
        .with_context(|| format!("scan archive directory {}", directory.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() && !path.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(parsed) = parse_archive_name(name) else {
            continue;
        };
        if parsed.kind_label == kind.label() {
            archives.push((parsed.end_slot, path));
        }
    }
    archives.sort_by_key(|(end_slot, _)| *end_slot);
    let delete_count = archives.len().saturating_sub(archives_to_keep);
    Ok(archives
        .into_iter()
        .take(delete_count)
        .map(|(_, path)| path)
        .collect())
}

async fn list_local_archive_names(directory: &Path, kind: ArchiveKind) -> Result<Vec<String>> {
    if !directory.exists() {
        return Ok(Vec::new());
    }
    let mut names = Vec::new();
    let mut entries = fs::read_dir(directory)
        .await
        .with_context(|| format!("scan archive directory {}", directory.display()))?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if !path.is_file() && !path.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if parse_archive_name(name)
            .map(|parsed| parsed.kind_label == kind.label())
            .unwrap_or(false)
        {
            names.push(name.to_string());
        }
    }
    Ok(names)
}

async fn list_s3_archive_names(config: &Config, kind: ArchiveKind) -> Result<Vec<String>> {
    let s3 = config
        .s3
        .as_ref()
        .ok_or_else(|| anyhow!("archive-location-type=s3 requires S3 config"))?;
    let store = build_s3_store(s3)?;
    let prefix = ObjectPath::from(s3_prefix_for_kind(s3, kind));
    let objects = store
        .list(Some(&prefix))
        .try_collect::<Vec<_>>()
        .await
        .context("list S3 archives")?;
    let prefix_text = s3_prefix_for_kind(s3, kind);
    let prefix_with_slash = format!("{prefix_text}/");
    let mut names = HashSet::new();
    for meta in objects {
        let location = meta.location.as_ref();
        if location.ends_with(".parquet") {
            if let Some(name) = meta.location.filename().map(ToOwned::to_owned)
                && parse_archive_name(&name)
                    .filter(|parsed| parsed.kind_label == kind.label())
                    .is_some()
            {
                names.insert(name);
            }
            continue;
        }
        let Some(relative) = location.strip_prefix(&prefix_with_slash) else {
            continue;
        };
        let Some((archive_id, file_name)) = relative.split_once('/') else {
            continue;
        };
        if file_name == MANIFEST_FILE_NAME
            && parse_archive_name(archive_id)
                .filter(|parsed| parsed.kind_label == kind.label())
                .is_some()
        {
            names.insert(archive_id.to_string());
        }
    }
    Ok(names.into_iter().collect())
}

async fn cleanup_s3_archives(config: &Config, kind: ArchiveKind) -> Result<Vec<String>> {
    let s3 = config
        .s3
        .as_ref()
        .ok_or_else(|| anyhow!("archive-location-type=s3 requires S3 config"))?;
    let store = build_s3_store(s3)?;
    let mut archives = list_s3_archive_names(config, kind)
        .await?
        .into_iter()
        .filter_map(|name| parse_archive_name(&name).map(|parsed| (parsed.end_slot, name)))
        .collect::<Vec<_>>();
    archives.sort_by_key(|(end_slot, _)| *end_slot);
    let delete_count = archives.len().saturating_sub(config.archives_to_keep);
    let mut deleted = Vec::new();
    for (_, name) in archives.into_iter().take(delete_count) {
        if name.ends_with(".parquet") {
            let archive_key = object_path_for_archive(s3, kind, &name);
            store
                .delete(&archive_key)
                .await
                .with_context(|| format!("delete S3 archive {}", archive_key.as_ref()))?;
            let report_key = object_path_for_report(s3, kind, &name);
            let _ = store.delete(&report_key).await;
        } else {
            let bundle_prefix =
                ObjectPath::from(format!("{}/{}/", s3_prefix_for_kind(s3, kind), name));
            let objects = store
                .list(Some(&bundle_prefix))
                .try_collect::<Vec<_>>()
                .await
                .with_context(|| format!("list S3 archive bundle {}", bundle_prefix.as_ref()))?;
            for object in objects {
                store.delete(&object.location).await.with_context(|| {
                    format!(
                        "delete S3 archive bundle object {}",
                        object.location.as_ref()
                    )
                })?;
            }
        }
        deleted.push(name);
    }
    Ok(deleted)
}

fn build_s3_store(config: &S3Config) -> Result<Arc<dyn ObjectStore>> {
    let store = AmazonS3Builder::new()
        .with_bucket_name(&config.bucket_name)
        .with_access_key_id(&config.auth_key)
        .with_secret_access_key(&config.auth_secret_key)
        .with_endpoint(&config.endpoint)
        .with_region(&config.region)
        .with_allow_http(config.endpoint.starts_with("http://"))
        .build()
        .context("build S3 object store")?;
    Ok(Arc::new(store))
}

fn s3_prefix_for_kind(config: &S3Config, kind: ArchiveKind) -> String {
    let base = config.bucket_path.trim_matches('/');
    if base.is_empty() {
        kind.label().to_string()
    } else {
        format!("{base}/{}", kind.label())
    }
}

fn object_path_for_archive(config: &S3Config, kind: ArchiveKind, archive_name: &str) -> ObjectPath {
    ObjectPath::from(format!(
        "{}/{}",
        s3_prefix_for_kind(config, kind),
        archive_name
    ))
}

fn object_path_for_report(config: &S3Config, kind: ArchiveKind, archive_name: &str) -> ObjectPath {
    ObjectPath::from(format!(
        "{}/.{}.report.json",
        s3_prefix_for_kind(config, kind),
        archive_name
    ))
}

pub fn local_bundle_dir(directory: &Path, archive_id: &str) -> PathBuf {
    directory.join(archive_id)
}

pub fn local_staging_bundle_dir(directory: &Path, archive_id: &str) -> PathBuf {
    directory.join(format!(".{archive_id}.tmp"))
}

fn done_file_name(hostname: &str) -> String {
    let sanitized = hostname
        .chars()
        .map(|ch| match ch {
            '/' | '\\' => '_',
            _ => ch,
        })
        .collect::<String>();
    format!("{DONE_FILE_PREFIX}.{sanitized}.txt")
}

fn is_done_file_name(file_name: &str) -> bool {
    file_name.starts_with(DONE_FILE_PREFIX)
}

async fn sha256_local_file(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path)
        .await
        .with_context(|| format!("open parquet file for checksum {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let bytes_read = file
            .read(&mut buffer)
            .await
            .with_context(|| format!("read parquet file for checksum {}", path.display()))?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn sha256_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn object_path_for_bundle_file(
    config: &S3Config,
    kind: ArchiveKind,
    archive_id: &str,
    file_name: &str,
) -> ObjectPath {
    ObjectPath::from(format!(
        "{}/{}/{}",
        s3_prefix_for_kind(config, kind),
        archive_id,
        file_name
    ))
}
