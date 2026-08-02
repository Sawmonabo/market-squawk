//! Resource-bounded entry point for `cargo-fuzz` to exercise the production file parsers.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use market_squawk_domain::Timestamp;
use tokio_util::sync::CancellationToken;

use crate::clock::RequestDeadline;
use crate::contracts::{ParseBudget, SourceRowLimit};
use crate::manifest::FormulaPolicy;
use crate::{
    ExtractionClock, ExtractionLimits, ExtractionLimitsInput, FileAdapterError,
    SystemExtractionClock,
};

const FUZZ_MAXIMUM_SOURCE_BYTES: u64 = 1024 * 1024;
const FUZZ_MAXIMUM_RECORDS: usize = 4_096;
const FUZZ_DURATION: Duration = Duration::from_millis(250);

/// Closed parser selection used by the grouped financial-file fuzz target.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FuzzFileFormat {
    /// Comma-separated records.
    Csv,
    /// Tab-separated records.
    Tsv,
    /// One JSON array/object document.
    Json,
    /// Newline-delimited JSON objects.
    Ndjson,
    /// Flat XML records named `record`.
    Xml,
    /// XLSX archive with cached formula values only.
    Excel,
    /// Parquet with flat supported Arrow values.
    Parquet,
    /// OFX/QFX statement bytes.
    Ofx,
}

/// Exercises one production parser under fixed, conservative resource and time limits.
///
/// This entry point exists only with the `fuzzing` feature. It does not bypass parser validation,
/// expose normalized records, or participate in production ingestion authority.
pub fn fuzz_parse_bytes(format: FuzzFileFormat, bytes: &[u8]) -> Result<(), FileAdapterError> {
    if u64::try_from(bytes.len())
        .ok()
        .is_none_or(|length| length > FUZZ_MAXIMUM_SOURCE_BYTES)
    {
        return Ok(());
    }
    let limits = ExtractionLimits::try_new(fuzz_limits())?;
    let cancellation = CancellationToken::new();
    let clock: Arc<dyn ExtractionClock> = Arc::new(SystemExtractionClock);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| FileAdapterError::ClockFailure)?;
    let now_nanos = i128::from(now.as_secs())
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(i128::from(now.subsec_nanos())))
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(FileAdapterError::ClockFailure)?;
    let wall_deadline = now_nanos
        .checked_add(
            i64::try_from(FUZZ_DURATION.as_nanos()).map_err(|_| FileAdapterError::ClockFailure)?,
        )
        .map(Timestamp::from_unix_nanos)
        .ok_or(FileAdapterError::ClockFailure)?;
    let admission_expiry = Instant::now()
        .checked_add(FUZZ_DURATION)
        .ok_or(FileAdapterError::ClockFailure)?;
    let deadline = RequestDeadline::seal(clock.as_ref(), wall_deadline, admission_expiry)?;
    let request_limit =
        u32::try_from(FUZZ_MAXIMUM_RECORDS).map_err(|_| FileAdapterError::InvalidLimits)?;
    let row_limit = SourceRowLimit::from_output_limit(request_limit, 1, FUZZ_MAXIMUM_RECORDS)?;
    let mut budget = ParseBudget::new(limits, &cancellation, clock, deadline, row_limit);

    let rows = match format {
        FuzzFileFormat::Csv => crate::csv::parse(bytes, b',', &mut budget),
        FuzzFileFormat::Tsv => crate::csv::parse(bytes, b'\t', &mut budget),
        FuzzFileFormat::Json => crate::json::parse_json(bytes, &mut budget),
        FuzzFileFormat::Ndjson => crate::json::parse_ndjson(bytes, &mut budget),
        FuzzFileFormat::Xml => crate::xml::parse(bytes, "record", &mut budget),
        FuzzFileFormat::Excel => {
            crate::excel::parse(bytes, FormulaPolicy::CachedValues, &mut budget)
        }
        FuzzFileFormat::Parquet => crate::parquet::parse(bytes, &mut budget),
        FuzzFileFormat::Ofx => crate::ofx::parse(bytes, "fuzz-account", "USD", &mut budget),
    }?;
    drop(rows);
    Ok(())
}

const fn fuzz_limits() -> ExtractionLimitsInput {
    ExtractionLimitsInput {
        max_manifest_bytes: 64 * 1024,
        max_manifest_nesting_depth: 32,
        max_manifest_objects: 16,
        max_manifest_mappings: 1_024,
        max_manifest_string_bytes: 16 * 1024,
        max_manifest_retained_bytes: 1024 * 1024,
        max_source_bytes: FUZZ_MAXIMUM_SOURCE_BYTES,
        max_decompressed_bytes: 4 * 1024 * 1024,
        max_retained_bytes: 16 * 1024 * 1024,
        max_records: FUZZ_MAXIMUM_RECORDS,
        max_fields_per_record: 256,
        max_nesting_depth: 64,
        max_text_bytes: 256 * 1024,
        max_sheets: 32,
        max_cells: 65_536,
        max_row_groups: 512,
        max_columns: 512,
        max_archive_entries: 256,
        max_compression_ratio: 100,
        max_elapsed: FUZZ_DURATION,
    }
}
