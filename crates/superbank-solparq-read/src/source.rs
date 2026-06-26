use std::{fs, path::PathBuf, sync::Arc};

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use futures_util::TryStreamExt;
use object_store::{ObjectStore, aws::AmazonS3Builder, path::Path as ObjectPath};

use crate::{
    archive_name::{ParsedArchiveName, parse_archive_name},
    config::{ArchiveArgs, ArchiveLocationType, ListArgs, SourceArgs},
    query::ArchiveInput,
};

#[derive(Debug, Clone)]
pub struct ArchiveObject {
    pub path: String,
    pub name: String,
    pub size_bytes: u64,
    pub modified_utc: Option<DateTime<Utc>>,
    pub parsed_name: Option<ParsedArchiveName>,
}

#[derive(Debug, Clone)]
pub struct S3ArchiveConfig {
    pub endpoint: String,
    pub bucket_name: String,
    pub bucket_path: String,
    pub auth_key: String,
    pub auth_secret_key: String,
    pub region: String,
}

impl S3ArchiveConfig {
    pub fn object_key(&self, archive: &str) -> ObjectPath {
        let archive = archive.trim_start_matches('/');
        let bucket_path = self.bucket_path.trim_matches('/');
        if bucket_path.is_empty() || archive.starts_with(&format!("{bucket_path}/")) {
            ObjectPath::from(archive)
        } else {
            ObjectPath::from(format!("{bucket_path}/{archive}"))
        }
    }

    pub fn prefix(&self) -> Option<ObjectPath> {
        let prefix = self.bucket_path.trim_matches('/');
        (!prefix.is_empty()).then(|| ObjectPath::from(prefix))
    }
}

pub fn archive_input_from_args(args: &ArchiveArgs) -> Result<ArchiveInput> {
    match args.source.archive_location_type {
        ArchiveLocationType::Local => {
            let path = PathBuf::from(&args.archive);
            if path.is_dir() {
                Ok(ArchiveInput::LocalBundle {
                    dir: path,
                    table: args.table,
                })
            } else {
                Ok(ArchiveInput::LocalFile(path))
            }
        }
        ArchiveLocationType::S3 => {
            let config = s3_config_from_source(&args.source)?;
            let store = build_s3_store(&config)?;
            let path = config.object_key(&args.archive);
            let display_path = format!("s3://{}/{}", config.bucket_name, path);
            if args.archive.ends_with(".parquet") {
                Ok(ArchiveInput::S3Object {
                    store,
                    path,
                    display_path,
                })
            } else {
                Ok(ArchiveInput::S3Bundle {
                    store,
                    prefix: path,
                    table: args.table,
                    display_path,
                })
            }
        }
    }
}

pub async fn list_archives(args: &ListArgs) -> Result<Vec<ArchiveObject>> {
    let mut archives = match args.source.archive_location_type {
        ArchiveLocationType::Local => {
            let dir = args.archive_dir.as_ref().ok_or_else(|| {
                anyhow!("--archive-dir is required when --archive-location-type=local")
            })?;
            list_local_archives(dir)
                .with_context(|| format!("list archives in {}", dir.display()))?
        }
        ArchiveLocationType::S3 => {
            let config = s3_config_from_source(&args.source)?;
            let store = build_s3_store(&config)?;
            list_s3_archives(store, config.prefix()).await?
        }
    };

    if let Some(kind) = &args.archive_kind {
        archives.retain(|archive| {
            archive
                .parsed_name
                .as_ref()
                .map(|parsed| parsed.kind_label == kind.as_str())
                .unwrap_or(false)
        });
    }

    archives.sort_by_key(|archive| {
        archive.parsed_name.as_ref().map(|parsed| {
            (
                parsed.kind_label.clone(),
                parsed.start_slot,
                parsed.end_slot,
            )
        })
    });
    Ok(archives)
}

fn list_local_archives(dir: &PathBuf) -> Result<Vec<ArchiveObject>> {
    let mut archives = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let is_bundle = path.is_dir() && path.join("manifest.json").is_file();
        let is_parquet =
            path.extension().and_then(|extension| extension.to_str()) == Some("parquet");
        if !is_parquet && !is_bundle {
            continue;
        }
        let metadata = entry.metadata()?;
        let name = entry.file_name().to_string_lossy().into_owned();
        archives.push(ArchiveObject {
            path: path.display().to_string(),
            parsed_name: parse_archive_name(&name),
            name,
            size_bytes: metadata.len(),
            modified_utc: metadata.modified().ok().map(DateTime::<Utc>::from),
        });
    }
    Ok(archives)
}

async fn list_s3_archives(
    store: Arc<dyn ObjectStore>,
    prefix: Option<ObjectPath>,
) -> Result<Vec<ArchiveObject>> {
    let mut stream = store.list(prefix.as_ref());
    let mut archives = Vec::new();
    while let Some(meta) = stream.try_next().await? {
        let location = meta.location.as_ref();
        let is_bundle_manifest = location.ends_with("/manifest.json");
        if !location.ends_with(".parquet") && !is_bundle_manifest {
            continue;
        }
        let name = if is_bundle_manifest {
            location
                .rsplit_once('/')
                .and_then(|(parent, _)| parent.rsplit_once('/').map(|(_, name)| name))
                .unwrap_or(location)
                .to_string()
        } else {
            meta.location
                .filename()
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| meta.location.to_string())
        };
        archives.push(ArchiveObject {
            path: meta.location.to_string(),
            parsed_name: parse_archive_name(&name),
            name,
            size_bytes: meta.size,
            modified_utc: Some(meta.last_modified),
        });
    }
    Ok(archives)
}

fn s3_config_from_source(source: &SourceArgs) -> Result<S3ArchiveConfig> {
    Ok(S3ArchiveConfig {
        endpoint: required_s3_value(&source.archive_s3_endpoint, "archive-s3-endpoint")?,
        bucket_name: required_s3_value(&source.archive_s3_bucket_name, "archive-s3-bucket-name")?,
        bucket_path: source.archive_s3_bucket_path.clone(),
        auth_key: required_s3_value(&source.archive_s3_auth_key, "archive-s3-auth-key")?,
        auth_secret_key: required_s3_value(
            &source.archive_s3_auth_secret_key,
            "archive-s3-auth-secret-key",
        )?,
        region: source.archive_s3_region.clone(),
    })
}

fn required_s3_value(value: &Option<String>, name: &str) -> Result<String> {
    let value = value
        .as_ref()
        .ok_or_else(|| anyhow!("{name} is required when --archive-location-type=s3"))?;
    if value.trim().is_empty() {
        return Err(anyhow!(
            "{name} is required when --archive-location-type=s3"
        ));
    }
    Ok(value.clone())
}

fn build_s3_store(config: &S3ArchiveConfig) -> Result<Arc<dyn ObjectStore>> {
    let allow_http = config.endpoint.starts_with("http://");
    let store = AmazonS3Builder::new()
        .with_endpoint(config.endpoint.clone())
        .with_bucket_name(config.bucket_name.clone())
        .with_access_key_id(config.auth_key.clone())
        .with_secret_access_key(config.auth_secret_key.clone())
        .with_region(config.region.clone())
        .with_allow_http(allow_http)
        .with_virtual_hosted_style_request(false)
        .build()
        .context("build S3 archive store")?;
    Ok(Arc::new(store))
}
