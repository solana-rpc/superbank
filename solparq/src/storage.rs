use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, anyhow};
use futures_util::TryStreamExt;
use object_store::{ObjectStore, PutPayload, aws::AmazonS3Builder, path::Path as ObjectPath};
use tokio::fs;

use crate::{
    archive::{ArchiveKind, parse_archive_name, report_path_for_local_archive},
    config::{ArchiveLocation, Config, S3Config},
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
                fs::remove_file(&path)
                    .await
                    .with_context(|| format!("delete local archive {}", path.display()))?;
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

pub async fn write_report(
    config: &Config,
    kind: ArchiveKind,
    archive_name: &str,
    report: &str,
) -> Result<()> {
    match config.archive_location {
        ArchiveLocation::Local => {
            fs::create_dir_all(&config.output_location).await?;
            let path = report_path_for_local_archive(config.output_location.clone(), archive_name);
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
            let key = object_path_for_report(s3, kind, archive_name);
            store
                .put(&key, PutPayload::from(report.as_bytes().to_vec()))
                .await
                .with_context(|| format!("write S3 report {}", key.as_ref()))?;
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
        if !path.is_file() {
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
        if !path.is_file() {
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
    Ok(objects
        .into_iter()
        .filter_map(|meta| {
            let name = meta.location.filename()?.to_string();
            parse_archive_name(&name)
                .filter(|parsed| parsed.kind_label == kind.label())
                .map(|_| name)
        })
        .collect())
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
        let archive_key = object_path_for_archive(s3, kind, &name);
        store
            .delete(&archive_key)
            .await
            .with_context(|| format!("delete S3 archive {}", archive_key.as_ref()))?;
        let report_key = object_path_for_report(s3, kind, &name);
        let _ = store.delete(&report_key).await;
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
        "{}/.{}.report.txt",
        s3_prefix_for_kind(config, kind),
        archive_name
    ))
}
