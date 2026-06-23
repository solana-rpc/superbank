use std::{collections::HashSet, fs::File, path::PathBuf, sync::Arc};

use anyhow::{Context, Result, anyhow};
use arrow_array::{Array, ArrayRef, Int32Array, Int64Array, RecordBatch, UInt32Array, UInt64Array};
use arrow_json::ArrayWriter;
use arrow_schema::Schema;
use futures_util::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, path::Path as ObjectPath};
use parquet::arrow::{
    ProjectionMask,
    arrow_reader::ParquetRecordBatchReaderBuilder,
    async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder},
};
use serde::Serialize;
use serde_json::{Map, Value};

use crate::{
    archive_name::{ParsedArchiveName, parse_archive_name},
    config::ScanSelection,
};

#[derive(Clone)]
pub enum ArchiveInput {
    LocalFile(PathBuf),
    S3Object {
        store: Arc<dyn ObjectStore>,
        path: ObjectPath,
        display_path: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanOptions {
    pub selection: ScanSelection,
    pub columns: Vec<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArchiveSummary {
    pub archive_path: String,
    pub archive_name: String,
    pub parsed_name: Option<ParsedArchiveName>,
    pub transaction_rows: u64,
    pub actual_min_slot: Option<u64>,
    pub actual_max_slot: Option<u64>,
    pub observed_slots: u64,
    pub observed_blocks: u64,
    pub row_groups: usize,
    pub columns: usize,
    pub size_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArchiveSchema {
    pub archive_path: String,
    pub fields: Vec<SchemaField>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SchemaField {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

struct OpenArchive {
    path: String,
    name: String,
    parsed_name: Option<ParsedArchiveName>,
    row_groups: usize,
    transaction_rows: u64,
    size_bytes: Option<u64>,
    schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
}

pub async fn summarize_archive(input: ArchiveInput) -> Result<ArchiveSummary> {
    let archive = read_archive(input, Projection::Columns(vec!["slot".to_string()])).await?;
    let mut distinct_slots = HashSet::new();
    let mut min_slot = None;
    let mut max_slot = None;

    for batch in &archive.batches {
        let slot_index = batch
            .schema()
            .index_of("slot")
            .context("archive does not contain a slot column")?;
        let slot_array = batch.column(slot_index);
        for row in 0..batch.num_rows() {
            if let Some(slot) = slot_value(slot_array, row)? {
                min_slot = Some(min_slot.map_or(slot, |current: u64| current.min(slot)));
                max_slot = Some(max_slot.map_or(slot, |current: u64| current.max(slot)));
                distinct_slots.insert(slot);
            }
        }
    }

    Ok(ArchiveSummary {
        archive_path: archive.path,
        archive_name: archive.name,
        parsed_name: archive.parsed_name,
        transaction_rows: archive.transaction_rows,
        actual_min_slot: min_slot,
        actual_max_slot: max_slot,
        observed_slots: distinct_slots.len() as u64,
        observed_blocks: distinct_slots.len() as u64,
        row_groups: archive.row_groups,
        columns: archive.schema.fields().len(),
        size_bytes: archive.size_bytes,
    })
}

pub async fn schema_archive(input: ArchiveInput) -> Result<ArchiveSchema> {
    let archive = read_archive(input, Projection::Columns(Vec::new())).await?;
    Ok(ArchiveSchema {
        archive_path: archive.path,
        fields: archive
            .schema
            .fields()
            .iter()
            .map(|field| SchemaField {
                name: field.name().to_string(),
                data_type: field.data_type().to_string(),
                nullable: field.is_nullable(),
            })
            .collect(),
    })
}

pub async fn scan_archive(input: ArchiveInput, options: ScanOptions) -> Result<Vec<Value>> {
    let projection = scan_projection(&options);
    let archive = read_archive(input, projection).await?;
    let mut rows = Vec::new();

    for batch in archive.batches {
        validate_columns(&batch, &options.columns)?;
        let json_rows = batch_to_json_rows(&batch)?;
        for row in json_rows {
            if row_matches_selection(&row, options.selection)? {
                rows.push(Value::Object(project_json_row(row, &options.columns)));
                if options.limit.is_some_and(|limit| rows.len() >= limit) {
                    return Ok(rows);
                }
            }
        }
    }

    Ok(rows)
}

#[derive(Debug, Clone)]
enum Projection {
    All,
    Columns(Vec<String>),
}

async fn read_archive(input: ArchiveInput, projection: Projection) -> Result<OpenArchive> {
    match input {
        ArchiveInput::LocalFile(path) => read_local_archive(path, projection).await,
        ArchiveInput::S3Object {
            store,
            path,
            display_path,
        } => read_s3_archive(store, path, display_path, projection).await,
    }
}

async fn read_local_archive(path: PathBuf, projection: Projection) -> Result<OpenArchive> {
    let file = File::open(&path).with_context(|| format!("open archive {}", path.display()))?;
    let size_bytes = file.metadata().ok().map(|metadata| metadata.len());
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("read parquet metadata {}", path.display()))?;
    let row_groups = builder.metadata().num_row_groups();
    let transaction_rows = builder.metadata().file_metadata().num_rows().max(0) as u64;
    let schema = builder.schema().clone();
    let projection = projection_mask(builder.parquet_schema(), &schema, projection)?;
    let mut builder = builder.with_batch_size(8192);
    if let Some(mask) = projection {
        builder = builder.with_projection(mask);
    }
    let reader = builder
        .build()
        .with_context(|| format!("build parquet reader {}", path.display()))?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.map_err(|err| {
            enrich_batch_error(format!("read parquet batch {}", path.display()), err)
        })?);
    }
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_string();
    Ok(OpenArchive {
        path: path.display().to_string(),
        parsed_name: parse_archive_name(&name),
        name,
        row_groups,
        transaction_rows,
        size_bytes,
        schema,
        batches,
    })
}

async fn read_s3_archive(
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    display_path: String,
    projection: Projection,
) -> Result<OpenArchive> {
    let meta = store
        .head(&path)
        .await
        .with_context(|| format!("stat S3 archive {display_path}"))?;
    let reader = ParquetObjectReader::new(store, path.clone()).with_file_size(meta.size);
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .with_context(|| format!("read parquet metadata {display_path}"))?;
    let row_groups = builder.metadata().num_row_groups();
    let transaction_rows = builder.metadata().file_metadata().num_rows().max(0) as u64;
    let schema = builder.schema().clone();
    let projection = projection_mask(builder.parquet_schema(), &schema, projection)?;
    let mut builder = builder.with_batch_size(8192);
    if let Some(mask) = projection {
        builder = builder.with_projection(mask);
    }
    let mut stream = builder
        .build()
        .with_context(|| format!("build parquet stream {display_path}"))?;
    let mut batches = Vec::new();
    while let Some(batch) = stream
        .try_next()
        .await
        .map_err(|err| enrich_batch_error(format!("read parquet batch {display_path}"), err))?
    {
        batches.push(batch);
    }
    let name = path
        .filename()
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| path.to_string());
    Ok(OpenArchive {
        path: display_path,
        parsed_name: parse_archive_name(&name),
        name,
        row_groups,
        transaction_rows,
        size_bytes: Some(meta.size),
        schema,
        batches,
    })
}

fn scan_projection(options: &ScanOptions) -> Projection {
    if options.columns.is_empty() {
        return Projection::All;
    }

    let mut columns = options.columns.clone();
    if matches!(options.selection, ScanSelection::SlotRange { .. })
        && !columns.iter().any(|column| column == "slot")
    {
        columns.push("slot".to_string());
    }
    Projection::Columns(columns)
}

fn projection_mask(
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
    schema: &Schema,
    projection: Projection,
) -> Result<Option<ProjectionMask>> {
    match projection {
        Projection::All => Ok(None),
        Projection::Columns(columns) => {
            if columns.is_empty() {
                return Ok(Some(ProjectionMask::none(parquet_schema.num_columns())));
            }
            for column in &columns {
                schema.index_of(column).with_context(|| {
                    format!("archive does not contain requested column '{column}'")
                })?;
            }
            Ok(Some(ProjectionMask::columns(
                parquet_schema,
                columns.iter().map(String::as_str),
            )))
        }
    }
}

fn enrich_batch_error<E>(context: String, err: E) -> anyhow::Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    let err = anyhow!(err);
    let detail = err.to_string();
    if detail.contains("non UTF-8") || detail.contains("non-UTF8") {
        err.context(format!(
            "{context}; archive contains a UTF8-typed column with non-UTF8 bytes. \
             Use --columns to read only the fields you need, for example --columns slot,signature"
        ))
    } else {
        err.context(context)
    }
}

fn validate_columns(batch: &RecordBatch, columns: &[String]) -> Result<()> {
    if columns.is_empty() {
        return Ok(());
    }

    let schema = batch.schema();
    for column in columns {
        schema
            .index_of(column)
            .with_context(|| format!("archive does not contain requested column '{column}'"))?;
    }
    Ok(())
}

fn project_json_row(mut row: Map<String, Value>, columns: &[String]) -> Map<String, Value> {
    if columns.is_empty() {
        return row;
    }

    let mut projected = Map::new();
    for column in columns {
        if let Some(value) = row.remove(column) {
            projected.insert(column.clone(), value);
        }
    }
    projected
}

fn batch_to_json_rows(batch: &RecordBatch) -> Result<Vec<Map<String, Value>>> {
    let mut writer = ArrayWriter::new(Vec::new());
    writer
        .write_batches(&[batch])
        .context("encode archive rows as JSON")?;
    writer.finish().context("finish JSON archive rows")?;
    let bytes = writer.into_inner();
    serde_json::from_slice(&bytes).context("decode JSON archive rows")
}

fn row_matches_selection(row: &Map<String, Value>, selection: ScanSelection) -> Result<bool> {
    match selection {
        ScanSelection::All => Ok(true),
        ScanSelection::SlotRange {
            start_slot,
            end_slot,
        } => {
            let slot = row
                .get("slot")
                .and_then(Value::as_u64)
                .ok_or_else(|| anyhow!("archive row does not contain an unsigned slot value"))?;
            Ok((start_slot..=end_slot).contains(&slot))
        }
    }
}

fn slot_value(array: &ArrayRef, row: usize) -> Result<Option<u64>> {
    if array.is_null(row) {
        return Ok(None);
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt64Array>() {
        return Ok(Some(array.value(row)));
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt32Array>() {
        return Ok(Some(u64::from(array.value(row))));
    }
    if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
        let value = array.value(row);
        if value < 0 {
            return Err(anyhow!("slot column contains a negative value"));
        }
        return Ok(Some(value as u64));
    }
    if let Some(array) = array.as_any().downcast_ref::<Int32Array>() {
        let value = array.value(row);
        if value < 0 {
            return Err(anyhow!("slot column contains a negative value"));
        }
        return Ok(Some(value as u64));
    }
    Err(anyhow!(
        "slot column must be UInt64, UInt32, Int64, or Int32"
    ))
}
