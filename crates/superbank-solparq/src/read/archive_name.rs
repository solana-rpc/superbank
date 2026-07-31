use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ParsedArchiveName {
    pub kind_label: String,
    pub epoch: u64,
    pub start_slot: u64,
    pub end_slot: u64,
}

pub fn parse_archive_name(file_name: &str) -> Option<ParsedArchiveName> {
    let stem = file_name.strip_suffix(".parquet")?;
    let mut parts = stem.split('_');
    let kind_label = parts.next()?.to_string();
    let epoch = parts.next()?.parse().ok()?;
    let range = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    let (start, end) = range.split_once('-')?;
    Some(ParsedArchiveName {
        kind_label,
        epoch,
        start_slot: start.parse().ok()?,
        end_slot: end.parse().ok()?,
    })
}
