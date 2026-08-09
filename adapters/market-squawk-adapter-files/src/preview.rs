//! Path-free, bounded previews over the exact production file parsers.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use market_squawk_domain::Timestamp;
use tokio_util::sync::CancellationToken;

use crate::clock::{ExtractionClock, RequestDeadline, SystemExtractionClock};
use crate::contracts::{CellValue, ParseBudget, ParsedRow, SourceRowLimit};
use crate::manifest::FileFormat;
use crate::parse::{parse_decimal_lexeme, parse_rows};
use crate::{ExtractionLimits, FileAdapterError, ParserLimit};

const MAXIMUM_PREVIEW_ROWS: usize = 100;
const MAXIMUM_PREVIEW_COLUMNS: usize = 1_024;
const MAXIMUM_PREVIEW_CELL_BYTES: usize = 4 * 1_024;
const MAXIMUM_PREVIEW_COLUMN_NAME_BYTES: usize = 256;
const MAXIMUM_PREVIEW_LOGICAL_BYTES: usize = 16 * 1_024 * 1_024;
const PREVIEW_CELL_STRUCTURAL_BYTES: usize = 64;

/// Closed set of formats admitted by the guided owned-file preview flow.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilePreviewFormat {
    /// UTF-8 delimiter-separated rows with one header row.
    Csv {
        /// One nonzero delimiter other than quote or line terminators.
        delimiter: u8,
    },
    /// One flat JSON row object or array of flat row objects.
    Json,
    /// One flat JSON object per nonempty line.
    Ndjson,
    /// One flat, bounded Parquet file.
    Parquet,
}

/// Independent result bounds for one path-free preview.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FilePreviewLimits {
    maximum_sample_rows: usize,
    maximum_columns: usize,
    maximum_cell_bytes: usize,
}

impl FilePreviewLimits {
    /// Returns conservative interactive-preview defaults.
    pub const fn standard() -> Self {
        Self {
            maximum_sample_rows: 20,
            maximum_columns: 256,
            maximum_cell_bytes: 256,
        }
    }

    /// Validates positive preview bounds under a fixed aggregate output ceiling.
    ///
    /// # Errors
    ///
    /// Rejects zero or excessive row, column, cell, or aggregate logical bounds.
    pub fn try_new(
        maximum_sample_rows: usize,
        maximum_columns: usize,
        maximum_cell_bytes: usize,
    ) -> Result<Self, FileAdapterError> {
        let logical_bytes = maximum_sample_rows
            .checked_mul(maximum_columns)
            .and_then(|cells| {
                maximum_cell_bytes
                    .checked_add(PREVIEW_CELL_STRUCTURAL_BYTES)
                    .and_then(|bytes| cells.checked_mul(bytes))
            })
            .and_then(|bytes| {
                maximum_columns
                    .checked_mul(MAXIMUM_PREVIEW_COLUMN_NAME_BYTES)
                    .and_then(|names| bytes.checked_add(names))
            })
            .ok_or(FileAdapterError::InvalidLimits)?;
        if maximum_sample_rows == 0
            || maximum_sample_rows > MAXIMUM_PREVIEW_ROWS
            || maximum_columns == 0
            || maximum_columns > MAXIMUM_PREVIEW_COLUMNS
            || maximum_cell_bytes == 0
            || maximum_cell_bytes > MAXIMUM_PREVIEW_CELL_BYTES
            || logical_bytes > MAXIMUM_PREVIEW_LOGICAL_BYTES
        {
            return Err(FileAdapterError::InvalidLimits);
        }
        Ok(Self {
            maximum_sample_rows,
            maximum_columns,
            maximum_cell_bytes,
        })
    }

    /// Returns the maximum sampled rows.
    pub const fn maximum_sample_rows(self) -> usize {
        self.maximum_sample_rows
    }

    /// Returns the maximum distinct columns.
    pub const fn maximum_columns(self) -> usize {
        self.maximum_columns
    }

    /// Returns the maximum retained UTF-8 bytes in one displayed cell.
    pub const fn maximum_cell_bytes(self) -> usize {
        self.maximum_cell_bytes
    }
}

/// Conservative type summary derived from exact normalized parser values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilePreviewColumnKind {
    /// Every present non-null value is an exact supported decimal lexeme.
    ExactDecimal,
    /// Every present non-null value is text that is not uniformly an exact decimal.
    Text,
    /// Values contain incompatible supported kinds.
    Mixed,
    /// Present values use a source type that canonical file ingestion does not map.
    Unsupported,
    /// The column contains no present non-null value.
    Null,
}

/// One bounded path-free column summary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FilePreviewColumn {
    name: String,
    kind: FilePreviewColumnKind,
    nullable: bool,
}

impl FilePreviewColumn {
    /// Returns the exact source field name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the conservative type summary.
    pub const fn kind(&self) -> FilePreviewColumnKind {
        self.kind
    }

    /// Returns whether any parsed row omitted the field or supplied null.
    pub const fn nullable(&self) -> bool {
        self.nullable
    }
}

/// One displayed cell retaining missing, null, and unsupported distinctions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FilePreviewCell {
    /// Bounded UTF-8 source text.
    Text {
        /// Display value, possibly shortened at a UTF-8 boundary.
        value: String,
        /// Whether the source text exceeded the configured display bound.
        truncated: bool,
    },
    /// The source explicitly supplied null.
    Null,
    /// The source supplied a type unsupported by canonical numeric mapping.
    Unsupported,
    /// This row did not contain the unioned source field.
    Missing,
}

/// One sample row whose cells align exactly with [`FilePreview::columns`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FilePreviewRow {
    cells: Box<[FilePreviewCell]>,
}

impl FilePreviewRow {
    /// Returns cells in the same order as the preview columns.
    pub fn cells(&self) -> &[FilePreviewCell] {
        &self.cells
    }
}

/// Bounded path-free result from one exact production parser.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FilePreview {
    format: FilePreviewFormat,
    row_count: u64,
    columns: Box<[FilePreviewColumn]>,
    sample_rows: Box<[FilePreviewRow]>,
}

impl FilePreview {
    /// Returns the exact closed format used by the parser.
    pub const fn format(&self) -> FilePreviewFormat {
        self.format
    }

    /// Returns the complete parsed row count, not merely the sample size.
    pub const fn row_count(&self) -> u64 {
        self.row_count
    }

    /// Returns bounded source-column summaries in lexical field order.
    pub fn columns(&self) -> &[FilePreviewColumn] {
        &self.columns
    }

    /// Returns the bounded leading-row sample.
    pub fn sample_rows(&self) -> &[FilePreviewRow] {
        &self.sample_rows
    }
}

/// Parses exact staged bytes through the production CSV, JSON, NDJSON, or Parquet parser.
///
/// This function accepts no path or filesystem authority. It parses the complete bounded input to
/// report an exact row count, while retaining only a separately bounded display sample.
///
/// # Errors
///
/// Rejects empty or oversized inputs, unsupported delimiter policies, malformed source content,
/// parser resource-limit breaches, cancellation, deadline expiry, or an excessive preview shape.
pub fn preview_bytes(
    format: FilePreviewFormat,
    bytes: &[u8],
    extraction_limits: ExtractionLimits,
    preview_limits: FilePreviewLimits,
    deadline: Timestamp,
    cancellation: &CancellationToken,
) -> Result<FilePreview, FileAdapterError> {
    let byte_count = u64::try_from(bytes.len())
        .map_err(|_| FileAdapterError::LimitExceeded(ParserLimit::SourceBytes))?;
    if bytes.is_empty() || byte_count > extraction_limits.source_bytes() {
        return Err(if byte_count > extraction_limits.source_bytes() {
            FileAdapterError::LimitExceeded(ParserLimit::SourceBytes)
        } else {
            FileAdapterError::InvalidRecord
        });
    }
    let manifest_format = manifest_format(format);
    manifest_format.validate()?;
    let admission_expiry = Instant::now()
        .checked_add(extraction_limits.input.max_elapsed)
        .ok_or(FileAdapterError::ClockFailure)?;
    let clock: Arc<dyn ExtractionClock> = Arc::new(SystemExtractionClock);
    let sealed = RequestDeadline::seal(clock.as_ref(), deadline, admission_expiry)?;
    let mut budget = ParseBudget::new(
        extraction_limits,
        cancellation,
        clock,
        sealed,
        SourceRowLimit::from_adapter_limit(extraction_limits.input.max_records),
    );
    let rows = parse_rows(&manifest_format, bytes, &mut budget)?;
    if rows.is_empty() {
        return Err(FileAdapterError::InvalidRecord);
    }
    build_preview(format, &rows, preview_limits)
}

fn manifest_format(format: FilePreviewFormat) -> FileFormat {
    match format {
        FilePreviewFormat::Csv { delimiter } => FileFormat::Csv { delimiter },
        FilePreviewFormat::Json => FileFormat::Json {},
        FilePreviewFormat::Ndjson => FileFormat::Ndjson {},
        FilePreviewFormat::Parquet => FileFormat::Parquet {},
    }
}

#[derive(Default)]
struct ColumnStats {
    present_rows: usize,
    saw_null: bool,
    saw_decimal: bool,
    saw_text: bool,
    saw_unsupported: bool,
}

impl ColumnStats {
    fn observe(&mut self, value: &CellValue) -> Result<(), FileAdapterError> {
        self.present_rows = self
            .present_rows
            .checked_add(1)
            .ok_or(FileAdapterError::LimitExceeded(ParserLimit::Records))?;
        match value {
            CellValue::Text(value) if parse_decimal_lexeme(value).is_ok() => {
                self.saw_decimal = true;
            }
            CellValue::Text(_) => self.saw_text = true,
            CellValue::Null => self.saw_null = true,
            CellValue::Unsupported => self.saw_unsupported = true,
        }
        Ok(())
    }

    const fn kind(&self) -> FilePreviewColumnKind {
        match (self.saw_decimal, self.saw_text, self.saw_unsupported) {
            (false, false, false) => FilePreviewColumnKind::Null,
            (true, false, false) => FilePreviewColumnKind::ExactDecimal,
            (false, true, false) => FilePreviewColumnKind::Text,
            (false, false, true) => FilePreviewColumnKind::Unsupported,
            _ => FilePreviewColumnKind::Mixed,
        }
    }
}

fn build_preview(
    format: FilePreviewFormat,
    rows: &[ParsedRow],
    limits: FilePreviewLimits,
) -> Result<FilePreview, FileAdapterError> {
    let mut stats = BTreeMap::<String, ColumnStats>::new();
    for row in rows {
        for (name, value) in &row.fields {
            if name.len() > MAXIMUM_PREVIEW_COLUMN_NAME_BYTES {
                return Err(FileAdapterError::LimitExceeded(ParserLimit::TextBytes));
            }
            if !stats.contains_key(name) && stats.len() >= limits.maximum_columns {
                return Err(FileAdapterError::LimitExceeded(ParserLimit::Columns));
            }
            stats.entry(name.clone()).or_default().observe(value)?;
        }
    }
    let row_count = rows.len();
    let mut columns = Vec::new();
    columns
        .try_reserve_exact(stats.len())
        .map_err(|_| FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
    for (name, stats) in stats {
        columns.push(FilePreviewColumn {
            name,
            kind: stats.kind(),
            nullable: stats.saw_null || stats.present_rows != row_count,
        });
    }
    let sample_count = row_count.min(limits.maximum_sample_rows);
    let mut sample_rows = Vec::new();
    sample_rows
        .try_reserve_exact(sample_count)
        .map_err(|_| FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
    for row in rows.iter().take(sample_count) {
        let mut cells = Vec::new();
        cells
            .try_reserve_exact(columns.len())
            .map_err(|_| FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
        for column in &columns {
            cells.push(preview_cell(
                row.fields.get(&column.name),
                limits.maximum_cell_bytes,
            )?);
        }
        sample_rows.push(FilePreviewRow {
            cells: cells.into_boxed_slice(),
        });
    }
    Ok(FilePreview {
        format,
        row_count: u64::try_from(row_count)
            .map_err(|_| FileAdapterError::LimitExceeded(ParserLimit::Records))?,
        columns: columns.into_boxed_slice(),
        sample_rows: sample_rows.into_boxed_slice(),
    })
}

fn preview_cell(
    value: Option<&CellValue>,
    maximum_bytes: usize,
) -> Result<FilePreviewCell, FileAdapterError> {
    match value {
        Some(CellValue::Text(value)) => {
            let end = utf8_prefix_end(value, maximum_bytes);
            let retained = value.get(..end).ok_or(FileAdapterError::InvalidRecord)?;
            let mut display = String::new();
            display
                .try_reserve_exact(retained.len())
                .map_err(|_| FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
            display.push_str(retained);
            Ok(FilePreviewCell::Text {
                value: display,
                truncated: end < value.len(),
            })
        }
        Some(CellValue::Null) => Ok(FilePreviewCell::Null),
        Some(CellValue::Unsupported) => Ok(FilePreviewCell::Unsupported),
        None => Ok(FilePreviewCell::Missing),
    }
}

fn utf8_prefix_end(value: &str, maximum_bytes: usize) -> usize {
    if value.len() <= maximum_bytes {
        return value.len();
    }
    let mut end = maximum_bytes;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    end
}
