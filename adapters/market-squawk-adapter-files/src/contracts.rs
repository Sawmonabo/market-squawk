//! Shared bounded-parser contracts and normalized intermediate rows.

use std::collections::BTreeMap;
use std::fmt;
use std::marker::PhantomData;
use std::mem::size_of;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::clock::{ExtractionClock, RequestDeadline};
use market_squawk_sources::ExtractionError;

/// Input fields used to construct one cohesive bounded extraction policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExtractionLimitsInput {
    /// Maximum exact bytes in the source manifest.
    pub max_manifest_bytes: u64,
    /// Maximum manifest container nesting depth before typed deserialization.
    pub max_manifest_nesting_depth: usize,
    /// Maximum declared source objects in one manifest.
    pub max_manifest_objects: usize,
    /// Maximum cumulative row-field mappings in one manifest.
    pub max_manifest_mappings: usize,
    /// Maximum encoded bytes in one manifest string.
    pub max_manifest_string_bytes: usize,
    /// Maximum conservatively retained bytes while admitting one manifest.
    pub max_manifest_retained_bytes: u64,
    /// Maximum exact bytes read from one source file.
    pub max_source_bytes: u64,
    /// Maximum decompressed bytes across a container.
    pub max_decompressed_bytes: u64,
    /// Maximum cumulative parser-owned retained and temporary allocation bytes.
    pub max_retained_bytes: u64,
    /// Maximum normalized output records.
    pub max_records: usize,
    /// Maximum fields in one source record.
    pub max_fields_per_record: usize,
    /// Maximum parser nesting depth.
    pub max_nesting_depth: usize,
    /// Maximum UTF-8 bytes in one text value.
    pub max_text_bytes: usize,
    /// Maximum spreadsheet sheets.
    pub max_sheets: usize,
    /// Maximum spreadsheet cells.
    pub max_cells: usize,
    /// Maximum Parquet row groups.
    pub max_row_groups: usize,
    /// Maximum Parquet or tabular columns.
    pub max_columns: usize,
    /// Maximum archive entries.
    pub max_archive_entries: usize,
    /// Maximum uncompressed-to-compressed ratio for one archive entry.
    pub max_compression_ratio: u64,
    /// Maximum elapsed parser time.
    pub max_elapsed: Duration,
}

impl ExtractionLimitsInput {
    /// Returns conservative local defaults below global extraction ceilings.
    pub const fn standard() -> Self {
        Self {
            max_manifest_bytes: 4 * 1024 * 1024,
            max_manifest_nesting_depth: 32,
            max_manifest_objects: 4_096,
            max_manifest_mappings: 65_536,
            max_manifest_string_bytes: 256 * 1024,
            max_manifest_retained_bytes: 16 * 1024 * 1024,
            max_source_bytes: 64 * 1024 * 1024,
            max_decompressed_bytes: 256 * 1024 * 1024,
            max_retained_bytes: 256 * 1024 * 1024,
            max_records: 100_000,
            max_fields_per_record: 1_024,
            max_nesting_depth: 64,
            max_text_bytes: 1024 * 1024,
            max_sheets: 256,
            max_cells: 1_000_000,
            max_row_groups: 8_192,
            max_columns: 4_096,
            max_archive_entries: 10_000,
            max_compression_ratio: 100,
            max_elapsed: Duration::from_secs(60),
        }
    }
}

/// Validated fixed resource policy shared by every local extraction format.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExtractionLimits {
    pub(crate) input: ExtractionLimitsInput,
}

impl ExtractionLimits {
    /// Validates nonzero limits against fixed process-safe ceilings.
    ///
    /// # Errors
    ///
    /// Rejects zero, address-space-incompatible, or excessive limits.
    pub fn try_new(input: ExtractionLimitsInput) -> Result<Self, FileAdapterError> {
        let valid = input.max_manifest_bytes > 0
            && input.max_manifest_bytes <= 64 * 1024 * 1024
            && usize::try_from(input.max_manifest_bytes).is_ok()
            && input.max_manifest_nesting_depth > 0
            && input.max_manifest_nesting_depth <= 256
            && input.max_manifest_objects > 0
            && input.max_manifest_objects <= 4_096
            && input.max_manifest_mappings > 0
            && input.max_manifest_mappings <= 1_048_576
            && input.max_manifest_string_bytes > 0
            && input.max_manifest_string_bytes <= 4 * 1024 * 1024
            && input.max_manifest_retained_bytes > 0
            && input.max_manifest_retained_bytes <= 256 * 1024 * 1024
            && usize::try_from(input.max_manifest_retained_bytes).is_ok()
            && input.max_source_bytes > 0
            && input.max_source_bytes <= 1024 * 1024 * 1024
            && usize::try_from(input.max_source_bytes).is_ok()
            && input.max_decompressed_bytes >= input.max_source_bytes
            && input.max_decompressed_bytes <= 4 * 1024 * 1024 * 1024
            && input.max_retained_bytes > 0
            && input.max_retained_bytes <= 4 * 1024 * 1024 * 1024
            && usize::try_from(input.max_retained_bytes).is_ok()
            && input.max_records > 0
            && input.max_records <= market_squawk_sources::MAX_EXTRACTION_RECORDS
            && input.max_fields_per_record > 0
            && input.max_fields_per_record <= 4_096
            && input.max_nesting_depth > 0
            && input.max_nesting_depth <= 256
            && input.max_text_bytes > 0
            && input.max_text_bytes <= market_squawk_sources::MAX_EXTRACTION_RECORD_BYTES
            && input.max_sheets > 0
            && input.max_sheets <= 4_096
            && input.max_cells > 0
            && input.max_cells <= 10_000_000
            && input.max_row_groups > 0
            && input.max_row_groups <= 32_768
            && input.max_columns > 0
            && input.max_columns <= 16_384
            && input.max_archive_entries > 0
            && input.max_archive_entries <= 100_000
            && input.max_compression_ratio > 0
            && input.max_compression_ratio <= 10_000
            && !input.max_elapsed.is_zero()
            && input.max_elapsed <= Duration::from_secs(3_600);
        if !valid {
            return Err(FileAdapterError::InvalidLimits);
        }
        Ok(Self { input })
    }

    pub(crate) fn source_bytes(self) -> u64 {
        self.input.max_source_bytes
    }
}

/// Independently reportable parser ceiling.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParserLimit {
    /// Exact source-manifest bytes.
    ManifestBytes,
    /// Manifest container nesting depth.
    ManifestNestingDepth,
    /// Manifest source-object declarations.
    ManifestObjects,
    /// Manifest row-field mappings.
    ManifestMappings,
    /// Entries in one manifest format string sequence.
    ManifestFormatSequenceEntries,
    /// Encoded bytes in one manifest string.
    ManifestStringBytes,
    /// Conservative retained manifest bytes.
    ManifestRetainedBytes,
    /// Exact source bytes.
    SourceBytes,
    /// Decompressed container bytes.
    DecompressedBytes,
    /// Actual decoded buffers and retained text allocations.
    DecodedBytes,
    /// Output records.
    Records,
    /// Fields in one record.
    Fields,
    /// Nested containers or XML elements.
    NestingDepth,
    /// UTF-8 bytes in one text value.
    TextBytes,
    /// Spreadsheet sheets.
    Sheets,
    /// Spreadsheet cells.
    Cells,
    /// Encoded columnar metadata bytes.
    MetadataBytes,
    /// Parquet row groups.
    RowGroups,
    /// Tabular or Parquet columns.
    Columns,
    /// Archive entries.
    ArchiveEntries,
    /// Archive compression ratio.
    CompressionRatio,
    /// Elapsed parser time.
    Elapsed,
}

/// Bounded local extraction failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum FileAdapterError {
    /// Limits were zero, excessive, or relationally invalid.
    #[error("local extraction limits are invalid")]
    InvalidLimits,
    /// The manifest was malformed or violated a closed schema.
    #[error("local extraction manifest is invalid")]
    InvalidManifest,
    /// Exact manifest bytes do not match the source-metadata revision evidence.
    #[error("local extraction manifest evidence does not match source metadata")]
    ManifestEvidenceMismatch,
    /// The authority root overlaps the user-authorized input capability.
    #[error("local extraction representation authority overlaps the input root")]
    RepresentationAuthorityScope,
    /// Another source instance owns the representation authority lifetime lock.
    #[error("local extraction representation authority is already locked")]
    RepresentationAuthorityLocked,
    /// Durable representation authority is corrupt, ambiguous, or namespace-mismatched.
    #[error("local extraction representation authority is invalid")]
    RepresentationAuthorityInvalid,
    /// Durable representation authority could not be read or committed safely.
    #[error("local extraction representation authority is unavailable")]
    RepresentationAuthorityUnavailable,
    /// Durable exact-object representation state reached its fixed record ceiling.
    #[error("local extraction representation authority is full")]
    RepresentationAuthorityExhausted,
    /// Source metadata is not a user-owned, network-denied extraction source.
    #[error("local extraction source metadata is incompatible")]
    MetadataPolicyMismatch,
    /// Registry authority belongs to another source metadata revision.
    #[error("local extraction authority does not match this source")]
    AuthorityMismatch,
    /// Registry authority was replaced, revoked, expired, or otherwise rejected.
    #[error("local extraction authority was rejected: {0}")]
    Authority(#[from] market_squawk_sources::ExtractionAuthorityError),
    /// A requested object is absent from the manifest.
    #[error("local extraction object is not declared")]
    ObjectNotFound,
    /// A discovered object was transplanted across source, revision, or manifest lineage.
    #[error("local extraction object lineage does not match the source manifest")]
    ObjectLineageMismatch,
    /// Re-read source bytes do not match discovery evidence.
    #[error("local extraction object bytes changed after discovery")]
    ObjectEvidenceMismatch,
    /// Object availability does not match the evidence retained at exact-byte discovery.
    #[error("local extraction object availability does not match discovery")]
    ObjectAvailabilityMismatch,
    /// A SQLite file, schema object, query plan, or value violates the closed read policy.
    #[error("local extraction database violates safety policy")]
    UnsafeDatabase,
    /// The selected parser is not supported by this build.
    #[error("local extraction format is unsupported")]
    UnsupportedFormat,
    /// A record or object contains a duplicate field or identifier.
    #[error("local extraction record contains a duplicate field")]
    DuplicateField,
    /// A record is malformed or violates its explicit row policy.
    #[error("local extraction record is invalid")]
    InvalidRecord,
    /// A financial value was not an exact decimal string.
    #[error("local extraction decimal is invalid")]
    InvalidDecimal,
    /// A financial decimal did not match the configured source scale.
    #[error("local extraction decimal scale does not match policy")]
    DecimalScaleMismatch,
    /// XML contains a DTD, entity, processing instruction, or unsupported structure.
    #[error("local extraction XML contains unsafe markup")]
    UnsafeXml,
    /// A container is encrypted, overlapping, path-unsafe, or violates archive policy.
    #[error("local extraction archive violates safety policy")]
    UnsafeArchive,
    /// A workbook contains active content, external relationships, or disallowed formulas.
    #[error("local extraction spreadsheet violates safety policy")]
    UnsafeSpreadsheet,
    /// A Parquet file has an invalid footer, schema, metadata, or value encoding.
    #[error("local extraction Parquet input violates safety policy")]
    UnsafeParquet,
    /// An OFX/QFX header, document, statement, transaction, or total violates policy.
    #[error("local extraction OFX/QFX input violates safety policy")]
    UnsafeOfx,
    /// A parser-specific resource ceiling was reached.
    #[error("local extraction exceeded the {0:?} limit")]
    LimitExceeded(ParserLimit),
    /// Caller cancelled extraction.
    #[error("local extraction was cancelled")]
    Cancelled,
    /// Request or local elapsed deadline was exceeded.
    #[error("local extraction deadline was exceeded")]
    DeadlineExceeded,
    /// Paired wall/monotonic time was unavailable, regressed, or overflowed.
    #[error("local extraction clock failed")]
    ClockFailure,
    /// Tokio could not execute or join the bounded blocking operation.
    #[error("local extraction blocking operation failed")]
    BlockingTaskFailed,
    /// The controlled input capability rejected the operation.
    #[error("local extraction input capability rejected the operation")]
    InputCapability,
    /// A canonical domain or extraction contract rejected output.
    #[error("local extraction output contract was rejected")]
    Contract,
    /// The shared extraction batch contract rejected incremental output construction.
    #[error("local extraction output contract was rejected: {0}")]
    ExtractionContract(ExtractionError),
}

#[derive(Debug)]
pub(crate) struct ParsedRow {
    pub(crate) fields: BTreeMap<String, CellValue>,
    pub(crate) canonical_row_sha256: [u8; 32],
}

impl ParsedRow {
    pub(crate) fn try_new(
        fields: BTreeMap<String, CellValue>,
        budget: &mut ParseBudget<'_>,
    ) -> Result<Self, FileAdapterError> {
        let canonical_bound = fields.iter().try_fold(2_usize, |total, (key, value)| {
            let value_bytes = match value {
                CellValue::Text(value) => value
                    .len()
                    .checked_mul(6)
                    .and_then(|bytes| bytes.checked_add(2)),
                CellValue::Null | CellValue::Unsupported => Some(4),
            }
            .ok_or(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
            total
                .checked_add(
                    key.len()
                        .checked_mul(6)
                        .ok_or(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?,
                )
                .and_then(|bytes| bytes.checked_add(value_bytes))
                .and_then(|bytes| bytes.checked_add(4))
                .ok_or(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))
        })?;
        budget.allocation_bytes(
            canonical_bound
                .checked_mul(2)
                .ok_or(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?,
        )?;
        let canonical = serde_json::to_vec(&fields).map_err(|_| FileAdapterError::InvalidRecord)?;
        Ok(Self {
            fields,
            canonical_row_sha256: Sha256::digest(canonical).into(),
        })
    }
}

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(untagged)]
pub(crate) enum CellValue {
    Text(String),
    Null,
    Unsupported,
}

impl CellValue {
    pub(crate) fn as_text(&self) -> Result<&str, FileAdapterError> {
        match self {
            Self::Text(value) => Ok(value),
            Self::Null | Self::Unsupported => Err(FileAdapterError::InvalidRecord),
        }
    }
}

pub(crate) struct ParseBudget<'a> {
    pub(crate) limits: ExtractionLimits,
    control: ParseControl,
    lifetime: PhantomData<&'a ()>,
    row_limit: SourceRowLimit,
    records: usize,
    cells: usize,
    decompressed_bytes: u64,
    allocated_bytes: u64,
}

#[derive(Clone)]
pub(crate) struct ParseControl {
    cancellation: CancellationToken,
    clock: Arc<dyn ExtractionClock>,
    deadline: RequestDeadline,
}

impl ParseControl {
    pub(crate) fn checkpoint(&self) -> Result<(), FileAdapterError> {
        if self.cancellation.is_cancelled() {
            return Err(FileAdapterError::Cancelled);
        }
        self.deadline.checkpoint(self.clock.as_ref())
    }
}

struct BoundedFormatter<'a> {
    output: &'a mut String,
    maximum: usize,
}

impl fmt::Write for BoundedFormatter<'_> {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        let required = self
            .output
            .len()
            .checked_add(value.len())
            .ok_or(fmt::Error)?;
        if required > self.maximum {
            return Err(fmt::Error);
        }
        self.output.write_str(value)
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct SourceRowLimit {
    maximum: usize,
    request_maximum: Option<u32>,
}

impl SourceRowLimit {
    pub(crate) fn from_output_limit(
        request_maximum: u32,
        outputs_per_row: usize,
        adapter_maximum: usize,
    ) -> Result<Self, FileAdapterError> {
        let request_maximum_usize = usize::try_from(request_maximum)
            .map_err(|_| FileAdapterError::LimitExceeded(ParserLimit::Records))?;
        let request_rows = request_maximum_usize
            .checked_div(outputs_per_row)
            .ok_or(FileAdapterError::InvalidManifest)?;
        let request_binds = request_rows <= adapter_maximum;
        Ok(Self {
            maximum: request_rows.min(adapter_maximum),
            request_maximum: request_binds.then_some(request_maximum),
        })
    }

    pub(crate) const fn maximum(self) -> usize {
        self.maximum
    }

    fn exceeded(self) -> FileAdapterError {
        if let Some(requested) = self.request_maximum {
            FileAdapterError::ExtractionContract(ExtractionError::RecordLimitExceeded { requested })
        } else {
            FileAdapterError::LimitExceeded(ParserLimit::Records)
        }
    }
}

impl<'a> ParseBudget<'a> {
    pub(crate) fn new(
        limits: ExtractionLimits,
        cancellation: &'a CancellationToken,
        clock: Arc<dyn ExtractionClock>,
        deadline: RequestDeadline,
        row_limit: SourceRowLimit,
    ) -> Self {
        Self {
            limits,
            control: ParseControl {
                cancellation: cancellation.clone(),
                clock,
                deadline,
            },
            lifetime: PhantomData,
            row_limit,
            records: 0,
            cells: 0,
            decompressed_bytes: 0,
            allocated_bytes: 0,
        }
    }

    pub(crate) fn checkpoint(&self) -> Result<(), FileAdapterError> {
        self.control.checkpoint()
    }

    pub(crate) fn control(&self) -> ParseControl {
        self.control.clone()
    }

    pub(crate) fn record(&mut self) -> Result<(), FileAdapterError> {
        if self.records >= self.row_limit.maximum() {
            return Err(self.row_limit.exceeded());
        }
        self.records = self
            .records
            .checked_add(1)
            .ok_or(FileAdapterError::LimitExceeded(ParserLimit::Records))?;
        self.checkpoint()
    }

    pub(crate) const fn row_limit(&self) -> usize {
        self.row_limit.maximum()
    }

    pub(crate) fn row_limit_error(&self) -> FileAdapterError {
        self.row_limit.exceeded()
    }

    pub(crate) fn fields(&self, fields: usize) -> Result<(), FileAdapterError> {
        if fields > self.limits.input.max_fields_per_record {
            return Err(FileAdapterError::LimitExceeded(ParserLimit::Fields));
        }
        Ok(())
    }

    pub(crate) fn columns(&self, columns: usize) -> Result<(), FileAdapterError> {
        if columns > self.limits.input.max_columns {
            return Err(FileAdapterError::LimitExceeded(ParserLimit::Columns));
        }
        Ok(())
    }

    pub(crate) fn cell(&mut self) -> Result<(), FileAdapterError> {
        self.cells = self
            .cells
            .checked_add(1)
            .ok_or(FileAdapterError::LimitExceeded(ParserLimit::Cells))?;
        if self.cells > self.limits.input.max_cells {
            return Err(FileAdapterError::LimitExceeded(ParserLimit::Cells));
        }
        self.checkpoint()
    }

    pub(crate) fn text(&self, bytes: usize) -> Result<(), FileAdapterError> {
        if bytes > self.limits.input.max_text_bytes {
            return Err(FileAdapterError::LimitExceeded(ParserLimit::TextBytes));
        }
        Ok(())
    }

    pub(crate) fn depth(&self, depth: usize) -> Result<(), FileAdapterError> {
        if depth > self.limits.input.max_nesting_depth {
            return Err(FileAdapterError::LimitExceeded(ParserLimit::NestingDepth));
        }
        Ok(())
    }

    pub(crate) fn decompressed(&mut self, bytes: u64) -> Result<(), FileAdapterError> {
        // Container expansion is bounded separately from parser-owned decoded copies.
        self.decompressed_bytes =
            self.decompressed_bytes
                .checked_add(bytes)
                .ok_or(FileAdapterError::LimitExceeded(
                    ParserLimit::DecompressedBytes,
                ))?;
        if self.decompressed_bytes > self.limits.input.max_decompressed_bytes {
            return Err(FileAdapterError::LimitExceeded(
                ParserLimit::DecompressedBytes,
            ));
        }
        Ok(())
    }

    pub(crate) fn allocation_bytes(&mut self, bytes: usize) -> Result<(), FileAdapterError> {
        // Decompressed input and parser-owned allocation are independent resource dimensions.
        let bytes = u64::try_from(bytes)
            .map_err(|_| FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
        self.allocated_bytes = self
            .allocated_bytes
            .checked_add(bytes)
            .ok_or(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
        if self.allocated_bytes > self.limits.input.max_retained_bytes {
            return Err(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes));
        }
        Ok(())
    }

    pub(crate) fn string_allocation(&mut self, value: &String) -> Result<(), FileAdapterError> {
        self.allocation_bytes(value.capacity())
    }

    pub(crate) fn pre_admit_dynamic_bytes(
        &mut self,
        maximum: usize,
    ) -> Result<(), FileAdapterError> {
        let admitted = maximum
            .checked_mul(2)
            .ok_or(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
        self.allocation_bytes(admitted)
    }

    pub(crate) fn ensure_dynamic_bytes(&self, maximum: usize) -> Result<(), FileAdapterError> {
        let admitted = u64::try_from(
            maximum
                .checked_mul(2)
                .ok_or(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?,
        )
        .map_err(|_| FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
        if self
            .allocated_bytes
            .checked_add(admitted)
            .is_none_or(|bytes| bytes > self.limits.input.max_retained_bytes)
        {
            Err(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))
        } else {
            Ok(())
        }
    }

    pub(crate) fn remaining_retained_bytes(&self) -> Result<usize, FileAdapterError> {
        let remaining = self
            .limits
            .input
            .max_retained_bytes
            .checked_sub(self.allocated_bytes)
            .ok_or(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
        usize::try_from(remaining)
            .map_err(|_| FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))
    }

    pub(crate) fn string_with_capacity(
        &mut self,
        capacity: usize,
    ) -> Result<String, FileAdapterError> {
        self.pre_admit_dynamic_bytes(capacity)?;
        let mut value = String::new();
        value
            .try_reserve_exact(capacity)
            .map_err(|_| FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
        Ok(value)
    }

    pub(crate) fn owned_text(&mut self, value: &str) -> Result<String, FileAdapterError> {
        self.text(value.len())?;
        let mut owned = self.string_with_capacity(value.len())?;
        owned.push_str(value);
        Ok(owned)
    }

    pub(crate) fn formatted_text(
        &mut self,
        maximum: usize,
        arguments: fmt::Arguments<'_>,
    ) -> Result<String, FileAdapterError> {
        let mut output = self.string_with_capacity(maximum)?;
        fmt::write(
            &mut BoundedFormatter {
                output: &mut output,
                maximum,
            },
            arguments,
        )
        .map_err(|_| FileAdapterError::InvalidRecord)?;
        self.text(output.len())?;
        Ok(output)
    }

    pub(crate) fn vec_with_capacity<T>(
        &mut self,
        capacity: usize,
    ) -> Result<Vec<T>, FileAdapterError> {
        let bytes = capacity
            .checked_mul(size_of::<T>())
            .and_then(|bytes| bytes.checked_mul(2))
            .ok_or(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
        self.allocation_bytes(bytes)?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(capacity)
            .map_err(|_| FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
        Ok(values)
    }

    pub(crate) fn reserve_vec_slot<T>(
        &mut self,
        values: &mut Vec<T>,
    ) -> Result<(), FileAdapterError> {
        if values.len() == values.capacity() {
            let next_capacity = values
                .capacity()
                .max(1)
                .checked_mul(2)
                .ok_or(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
            let next_capacity = if values.capacity() == 0 {
                1
            } else {
                next_capacity
            };
            let admitted = next_capacity
                .checked_mul(size_of::<T>())
                .and_then(|bytes| bytes.checked_mul(2))
                .ok_or(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
            self.allocation_bytes(admitted)?;
            let additional = next_capacity
                .checked_sub(values.len())
                .ok_or(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
            values
                .try_reserve_exact(additional)
                .map_err(|_| FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
        }
        Ok(())
    }

    pub(crate) fn append_string(
        &mut self,
        target: &mut String,
        value: &str,
    ) -> Result<(), FileAdapterError> {
        let required = target
            .len()
            .checked_add(value.len())
            .ok_or(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
        if required > target.capacity() {
            let next_capacity = target
                .capacity()
                .max(1)
                .checked_mul(2)
                .map(|capacity| capacity.max(required))
                .ok_or(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
            self.pre_admit_dynamic_bytes(next_capacity)?;
            let additional = next_capacity
                .checked_sub(target.len())
                .ok_or(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
            target
                .try_reserve_exact(additional)
                .map_err(|_| FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
        }
        target.push_str(value);
        Ok(())
    }

    pub(crate) fn map_entry<K, V>(&mut self) -> Result<(), FileAdapterError> {
        let tree_overhead = size_of::<usize>()
            .checked_mul(4)
            .ok_or(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
        let bytes = size_of::<K>()
            .checked_add(size_of::<V>())
            .and_then(|bytes| bytes.checked_add(tree_overhead))
            .ok_or(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
        self.allocation_bytes(bytes)
    }

    pub(crate) fn set_entry<T>(&mut self) -> Result<(), FileAdapterError> {
        let tree_overhead = size_of::<usize>()
            .checked_mul(4)
            .ok_or(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
        let bytes = size_of::<T>()
            .checked_add(tree_overhead)
            .ok_or(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
        self.allocation_bytes(bytes)
    }

    pub(crate) fn remaining_decompressed(&self) -> Result<u64, FileAdapterError> {
        self.limits
            .input
            .max_decompressed_bytes
            .checked_sub(self.decompressed_bytes)
            .ok_or(FileAdapterError::LimitExceeded(
                ParserLimit::DecompressedBytes,
            ))
    }
}
