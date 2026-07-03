use anyhow::Result;
use serde_json::Value;

use crate::read::{
    config::OutputFormat,
    query::{ArchiveSchema, ArchiveSummary},
    source::ArchiveObject,
};

pub fn format_archive_list(archives: &[ArchiveObject]) -> String {
    if archives.is_empty() {
        return "No archives found.\n".to_string();
    }

    let mut output =
        String::from("kind\tepoch\tstart_slot\tend_slot\tsize_bytes\tmodified_utc\tpath\n");
    for archive in archives {
        let (kind, epoch, start_slot, end_slot) = archive
            .parsed_name
            .as_ref()
            .map(|parsed| {
                (
                    parsed.kind_label.clone(),
                    parsed.epoch.to_string(),
                    parsed.start_slot.to_string(),
                    parsed.end_slot.to_string(),
                )
            })
            .unwrap_or_else(|| {
                (
                    "unknown".to_string(),
                    "-".to_string(),
                    "-".to_string(),
                    "-".to_string(),
                )
            });
        let modified = archive
            .modified_utc
            .map(|modified| modified.to_rfc3339())
            .unwrap_or_else(|| "-".to_string());
        output.push_str(&format!(
            "{kind}\t{epoch}\t{start_slot}\t{end_slot}\t{}\t{modified}\t{}\n",
            archive.size_bytes, archive.path
        ));
    }
    output
}

pub fn format_summary(summary: &ArchiveSummary) -> String {
    let parsed = summary
        .parsed_name
        .as_ref()
        .map(|parsed| {
            format!(
                "{} epoch {} {}-{}",
                parsed.kind_label, parsed.epoch, parsed.start_slot, parsed.end_slot
            )
        })
        .unwrap_or_else(|| "unparsed".to_string());
    format!(
        "archive: {}\npath: {}\narchive_format: {}\nparsed: {}\nformat_version: {}\nproducer: {}\ntransaction_rows: {}\nactual_min_slot: {}\nactual_max_slot: {}\nobserved_slots: {}\nobserved_blocks: {}\nrow_groups: {}\ncolumns: {}\nsize_bytes: {}\n{}",
        summary.archive_name,
        summary.archive_path,
        summary.archive_format,
        parsed,
        option_u32(summary.format_version),
        format_producer(summary),
        summary.transaction_rows,
        option_u64(summary.actual_min_slot),
        option_u64(summary.actual_max_slot),
        summary.observed_slots,
        summary.observed_blocks,
        summary.row_groups,
        summary.columns,
        option_u64(summary.size_bytes),
        format_table_summaries(summary),
    )
}

fn format_producer(summary: &ArchiveSummary) -> String {
    summary
        .producer
        .as_ref()
        .map(|producer| {
            format!(
                "{} {} ({})",
                producer.name, producer.version, producer.git_sha
            )
        })
        .unwrap_or_else(|| "-".to_string())
}

fn format_table_summaries(summary: &ArchiveSummary) -> String {
    let mut output = String::new();
    for table in &summary.tables {
        output.push_str(&format!("table_{}_rows: {}\n", table.kind, table.row_count));
    }
    output
}

pub fn format_schema(schema: &ArchiveSchema) -> String {
    let mut output = format!("archive: {}\nname\ttype\tnullable\n", schema.archive_path);
    for field in &schema.fields {
        output.push_str(&format!(
            "{}\t{}\t{}\n",
            field.name, field.data_type, field.nullable
        ));
    }
    output
}

pub fn format_rows(rows: &[Value], format: OutputFormat) -> Result<String> {
    match format {
        OutputFormat::Jsonl => Ok(format_jsonl(rows)),
        OutputFormat::Json => serde_json::to_string_pretty(rows)
            .map(|json| format!("{json}\n"))
            .map_err(Into::into),
        OutputFormat::Csv => Ok(format_csv(rows)),
    }
}

fn format_jsonl(rows: &[Value]) -> String {
    let mut output = String::new();
    for row in rows {
        output.push_str(&row.to_string());
        output.push('\n');
    }
    output
}

fn format_csv(rows: &[Value]) -> String {
    let Some(first) = rows.first().and_then(Value::as_object) else {
        return String::new();
    };
    let headers = first.keys().cloned().collect::<Vec<_>>();
    let mut output = String::new();
    output.push_str(&headers.join(","));
    output.push('\n');
    for row in rows {
        let Some(object) = row.as_object() else {
            continue;
        };
        for (index, header) in headers.iter().enumerate() {
            if index > 0 {
                output.push(',');
            }
            let value = object.get(header).map(csv_value).unwrap_or_default();
            output.push_str(&escape_csv(&value));
        }
        output.push('\n');
    }
    output
}

fn csv_value(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Array(_) | Value::Object(_) => value.to_string(),
    }
}

fn escape_csv(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn option_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn option_u32(value: Option<u32>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}
