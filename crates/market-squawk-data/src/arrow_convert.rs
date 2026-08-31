//! Exact canonical-observation conversion to and from Arrow.

use std::cell::Cell;
use std::io::{BufReader, Read, Write};
use std::mem::size_of;
use std::sync::Arc;

use arrow::array::{
    Array as _, ArrayRef, BinaryArray, Date32Array, Decimal128Array, PrimitiveArray, StringArray,
    TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array,
    builder::{BinaryBuilder, PrimitiveBuilder, StringBuilder},
};
use arrow::compute::concat_batches;
use arrow::datatypes::{
    ArrowPrimitiveType, Date32Type, Decimal128Type, TimestampNanosecondType, UInt8Type, UInt16Type,
    UInt32Type,
};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use market_squawk_domain::{
    AvailabilityEvidence, Currency, DataQuality, DigestAlgorithm, EvidenceDigest,
    ExactPayloadEvidence, MetadataRevision, ResearchContext, ResearchObservation,
    ResearchTemporalCoordinate, RevisionNumber, SchemaVersion, SourceId, SourceIdentifier,
    Timestamp,
};
use market_squawk_platform::{
    ResearchObjectControl, ResearchObjectControlError, ResearchObjectControlPoint,
};
use market_squawk_sources::{
    AvailabilityEvidence as SourceAvailabilityEvidence, CanonicalObservationPayload,
    DiscoveryRequestId, ExtractionBatch, ExtractionRequestId, SourceObjectCaptureIdentity,
    payload_matches_exact_evidence,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Error as JsonError;
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::catalog::PreparedProviderCaptureBinding;
use crate::schema::{
    DATASET_KEY, REQUEST_DIGEST_KEY, RESEARCH_RECORD_SCHEMA, RESEARCH_SCHEMA_NAME,
    RESEARCH_SCHEMA_VERSION, SCHEMA_VERSION_KEY, decode_hex, research_payload_contract_for,
    research_schema,
};
pub use crate::schema::{
    DatasetSchemaError, DatasetSchemaRef, DatasetSchemaRegistry, FeatureLabelBatchBindings,
};

const CONTROLLED_ARROW_WORK_CHUNK_BYTES: usize = 64 * 1024;
const CONTROLLED_ARROW_ROW_INTERVAL: usize = 64;

struct ArrowOperationControl<'control> {
    control: &'control dyn ResearchObjectControl,
    offset_bytes: u64,
    bytes_until_checkpoint: usize,
}

impl<'control> ArrowOperationControl<'control> {
    const fn new(control: &'control dyn ResearchObjectControl) -> Self {
        Self {
            control,
            offset_bytes: 0,
            bytes_until_checkpoint: 0,
        }
    }

    fn checkpoint_now(&self) -> Result<(), ArrowConversionError> {
        self.control
            .checkpoint(ResearchObjectControlPoint::BeforeVerificationChunk {
                offset_bytes: self.offset_bytes,
            })?;
        Ok(())
    }

    fn checkpoint_row(&self, ordinal: usize) -> Result<(), ArrowConversionError> {
        if ordinal % CONTROLLED_ARROW_ROW_INTERVAL == 0 {
            self.checkpoint_now()?;
        }
        Ok(())
    }

    fn byte_permit(&mut self, requested: usize) -> Result<usize, ArrowConversionError> {
        if requested == 0 {
            self.checkpoint_now()?;
            return Ok(0);
        }
        if self.bytes_until_checkpoint == 0 {
            self.checkpoint_now()?;
            self.bytes_until_checkpoint = CONTROLLED_ARROW_WORK_CHUNK_BYTES;
        }
        Ok(requested.min(self.bytes_until_checkpoint))
    }

    fn complete_bytes(&mut self, completed: usize) -> Result<(), ArrowConversionError> {
        self.offset_bytes = self
            .offset_bytes
            .checked_add(
                u64::try_from(completed).map_err(|_| ArrowConversionError::RetainedSizeOverflow)?,
            )
            .ok_or(ArrowConversionError::RetainedSizeOverflow)?;
        self.bytes_until_checkpoint = self
            .bytes_until_checkpoint
            .checked_sub(completed)
            .ok_or(ArrowConversionError::RetainedSizeOverflow)?;
        Ok(())
    }
}

struct ControlledJsonReader<'bytes, 'operation, 'control> {
    bytes: &'bytes [u8],
    position: usize,
    operation: &'operation mut ArrowOperationControl<'control>,
    failure: &'operation Cell<Option<ResearchObjectControlError>>,
    eof_checked: bool,
}

impl Read for ControlledJsonReader<'_, '_, '_> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if self.position == self.bytes.len() {
            if !self.eof_checked {
                self.eof_checked = true;
                if let Err(error) = self.operation.checkpoint_now() {
                    return record_control_io_failure(error, self.failure);
                }
            }
            return Ok(0);
        }
        let remaining = self.bytes.len() - self.position;
        let requested = remaining.min(buffer.len());
        let permitted = match self.operation.byte_permit(requested) {
            Ok(permitted) => permitted,
            Err(error) => return record_control_io_failure(error, self.failure),
        };
        let end = self.position + permitted;
        buffer[..permitted].copy_from_slice(&self.bytes[self.position..end]);
        self.position = end;
        if let Err(error) = self.operation.complete_bytes(permitted) {
            return record_control_io_failure(error, self.failure);
        }
        Ok(permitted)
    }
}

struct ControlledJsonWriter<'writer, 'operation, 'control, W> {
    inner: &'writer mut W,
    operation: &'operation mut ArrowOperationControl<'control>,
    failure: &'operation Cell<Option<ResearchObjectControlError>>,
}

impl<W: Write> Write for ControlledJsonWriter<'_, '_, '_, W> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        let permitted = match self.operation.byte_permit(buffer.len()) {
            Ok(permitted) => permitted,
            Err(error) => return record_control_io_failure(error, self.failure),
        };
        let written = self.inner.write(&buffer[..permitted])?;
        if let Err(error) = self.operation.complete_bytes(written) {
            return record_control_io_failure(error, self.failure);
        }
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

fn record_control_io_failure<T>(
    error: ArrowConversionError,
    failure: &Cell<Option<ResearchObjectControlError>>,
) -> std::io::Result<T> {
    if let ArrowConversionError::ObjectControl(control) = error {
        failure.set(Some(control));
    }
    Err(std::io::Error::other(
        "controlled Arrow JSON work was stopped",
    ))
}

fn decode_json<T: DeserializeOwned>(
    bytes: &[u8],
    control: Option<&mut ArrowOperationControl<'_>>,
) -> Result<T, ArrowConversionError> {
    let Some(control) = control else {
        return Ok(serde_json::from_slice(bytes)?);
    };
    let failure = Cell::new(None);
    let result = {
        let reader = ControlledJsonReader {
            bytes,
            position: 0,
            operation: control,
            failure: &failure,
            eof_checked: false,
        };
        serde_json::from_reader(BufReader::with_capacity(
            CONTROLLED_ARROW_WORK_CHUNK_BYTES,
            reader,
        ))
    };
    if let Some(error) = failure.get() {
        return Err(error.into());
    }
    Ok(result?)
}

fn encode_json<T: Serialize>(
    value: &T,
    control: Option<&mut ArrowOperationControl<'_>>,
) -> Result<Vec<u8>, ArrowConversionError> {
    let Some(control) = control else {
        return Ok(serde_json::to_vec(value)?);
    };
    let failure = Cell::new(None);
    let mut bytes = Vec::new();
    let result = {
        let mut writer = ControlledJsonWriter {
            inner: &mut bytes,
            operation: control,
            failure: &failure,
        };
        serde_json::to_writer(&mut writer, value)
    };
    if let Some(error) = failure.get() {
        return Err(error.into());
    }
    result?;
    control.checkpoint_now()?;
    Ok(bytes)
}

fn update_hash_bytes(
    hash: &mut Sha256,
    bytes: &[u8],
    control: Option<&mut ArrowOperationControl<'_>>,
) -> Result<(), ArrowConversionError> {
    let Some(control) = control else {
        hash.update(bytes);
        return Ok(());
    };
    let mut position = 0_usize;
    while position < bytes.len() {
        let permitted = control.byte_permit(bytes.len() - position)?;
        let end = position
            .checked_add(permitted)
            .ok_or(ArrowConversionError::RetainedSizeOverflow)?;
        hash.update(&bytes[position..end]);
        control.complete_bytes(permitted)?;
        position = end;
    }
    if bytes.is_empty() {
        control.checkpoint_now()?;
    }
    Ok(())
}

fn bytes_equal_controlled(
    left: &[u8],
    right: &[u8],
    control: &mut ArrowOperationControl<'_>,
) -> Result<bool, ArrowConversionError> {
    if left.len() != right.len() {
        return Ok(false);
    }
    let mut position = 0_usize;
    while position < left.len() {
        let permitted = control.byte_permit(left.len() - position)?;
        let end = position
            .checked_add(permitted)
            .ok_or(ArrowConversionError::RetainedSizeOverflow)?;
        if left[position..end] != right[position..end] {
            return Ok(false);
        }
        control.complete_bytes(permitted)?;
        position = end;
    }
    Ok(true)
}

fn record_batches_equal_controlled(
    left: &RecordBatch,
    right: &RecordBatch,
    control: &mut ArrowOperationControl<'_>,
) -> Result<bool, ArrowConversionError> {
    if left.schema() != right.schema()
        || left.num_rows() != right.num_rows()
        || left.num_columns() != right.num_columns()
    {
        return Ok(false);
    }
    for (column_ordinal, (left, right)) in left.columns().iter().zip(right.columns()).enumerate() {
        control.checkpoint_row(column_ordinal)?;
        if let (Some(left), Some(right)) = (
            left.as_any().downcast_ref::<BinaryArray>(),
            right.as_any().downcast_ref::<BinaryArray>(),
        ) {
            for row in 0..left.len() {
                control.checkpoint_row(row)?;
                if left.is_null(row) != right.is_null(row)
                    || (!left.is_null(row)
                        && !bytes_equal_controlled(left.value(row), right.value(row), control)?)
                {
                    return Ok(false);
                }
            }
            continue;
        }
        if let (Some(left), Some(right)) = (
            left.as_any().downcast_ref::<StringArray>(),
            right.as_any().downcast_ref::<StringArray>(),
        ) {
            for row in 0..left.len() {
                control.checkpoint_row(row)?;
                if left.is_null(row) != right.is_null(row)
                    || (!left.is_null(row)
                        && !bytes_equal_controlled(
                            left.value(row).as_bytes(),
                            right.value(row).as_bytes(),
                            control,
                        )?)
                {
                    return Ok(false);
                }
            }
            continue;
        }
        let mut offset = 0_usize;
        while offset < left.len() {
            control.checkpoint_row(offset)?;
            let count = (left.len() - offset).min(CONTROLLED_ARROW_ROW_INTERVAL);
            if left.slice(offset, count) != right.slice(offset, count) {
                return Ok(false);
            }
            offset += count;
        }
    }
    control.checkpoint_now()?;
    Ok(true)
}

fn write_controlled_bytes(
    retained: &mut BinaryBuilder,
    bytes: &[u8],
    control: &mut ArrowOperationControl<'_>,
) -> Result<(), ArrowConversionError> {
    let mut position = 0_usize;
    while position < bytes.len() {
        let permitted = control.byte_permit(bytes.len() - position)?;
        let end = position
            .checked_add(permitted)
            .ok_or(ArrowConversionError::RetainedSizeOverflow)?;
        retained
            .write_all(&bytes[position..end])
            .map_err(|error| ArrowError::ExternalError(Box::new(error)))?;
        control.complete_bytes(permitted)?;
        position = end;
    }
    Ok(())
}

trait ControlledByteArrayValue {
    fn controlled_bytes(&self) -> Option<&[u8]>;
}

trait ControlledStringArrayValue {
    fn controlled_str(&self) -> Option<&str>;
}

impl ControlledByteArrayValue for [u8; 32] {
    fn controlled_bytes(&self) -> Option<&[u8]> {
        Some(self.as_slice())
    }
}

impl ControlledByteArrayValue for Vec<u8> {
    fn controlled_bytes(&self) -> Option<&[u8]> {
        Some(self.as_slice())
    }
}

impl ControlledStringArrayValue for String {
    fn controlled_str(&self) -> Option<&str> {
        Some(self.as_str())
    }
}

impl ControlledStringArrayValue for &str {
    fn controlled_str(&self) -> Option<&str> {
        Some(self)
    }
}

impl<T: ControlledByteArrayValue> ControlledByteArrayValue for Option<T> {
    fn controlled_bytes(&self) -> Option<&[u8]> {
        self.as_ref()
            .and_then(ControlledByteArrayValue::controlled_bytes)
    }
}

impl<T: ControlledStringArrayValue> ControlledStringArrayValue for Option<T> {
    fn controlled_str(&self) -> Option<&str> {
        self.as_ref()
            .and_then(ControlledStringArrayValue::controlled_str)
    }
}

fn controlled_value_bytes_len<V: ControlledByteArrayValue>(
    values: &[V],
    control: &mut ArrowOperationControl<'_>,
) -> Result<usize, ArrowConversionError> {
    let mut value_bytes_len = 0_usize;
    for (ordinal, value) in values.iter().enumerate() {
        control.checkpoint_row(ordinal)?;
        if let Some(bytes) = value.controlled_bytes() {
            value_bytes_len = value_bytes_len
                .checked_add(bytes.len())
                .ok_or(ArrowConversionError::RetainedSizeOverflow)?;
        }
    }
    if value_bytes_len > i32::MAX as usize {
        return Err(ArrowError::OffsetOverflowError(value_bytes_len).into());
    }
    Ok(value_bytes_len)
}

fn controlled_string_bytes_len<V: ControlledStringArrayValue>(
    values: &[V],
    control: &mut ArrowOperationControl<'_>,
) -> Result<usize, ArrowConversionError> {
    let mut value_bytes_len = 0_usize;
    for (ordinal, value) in values.iter().enumerate() {
        control.checkpoint_row(ordinal)?;
        if let Some(value) = value.controlled_str() {
            value_bytes_len = value_bytes_len
                .checked_add(value.len())
                .ok_or(ArrowConversionError::RetainedSizeOverflow)?;
        }
    }
    if value_bytes_len > i32::MAX as usize {
        return Err(ArrowError::OffsetOverflowError(value_bytes_len).into());
    }
    Ok(value_bytes_len)
}

fn controlled_binary_array<V: ControlledByteArrayValue>(
    values: Vec<V>,
    control: &mut ArrowOperationControl<'_>,
) -> Result<BinaryArray, ArrowConversionError> {
    let row_count = values.len();
    row_count
        .checked_add(1)
        .ok_or(ArrowConversionError::RetainedSizeOverflow)?;
    let value_bytes_len = controlled_value_bytes_len(&values, control)?;
    control.checkpoint_now()?;
    let mut builder = BinaryBuilder::with_capacity(row_count, value_bytes_len);
    control.checkpoint_now()?;
    for (ordinal, value) in values.into_iter().enumerate() {
        control.checkpoint_row(ordinal)?;
        if let Some(bytes) = value.controlled_bytes() {
            write_controlled_bytes(&mut builder, bytes, control)?;
            builder.append_value([]);
        } else {
            builder.append_null();
        }
    }
    control.checkpoint_now()?;
    let array = builder.finish();
    control.checkpoint_now()?;
    Ok(array)
}

fn write_controlled_str(
    retained: &mut StringBuilder,
    value: &str,
    control: &mut ArrowOperationControl<'_>,
) -> Result<(), ArrowConversionError> {
    let mut position = 0_usize;
    while position < value.len() {
        let permitted = control.byte_permit(value.len() - position)?;
        let mut end = position
            .checked_add(permitted)
            .ok_or(ArrowConversionError::RetainedSizeOverflow)?;
        while end > position && !value.is_char_boundary(end) {
            end -= 1;
        }
        if end == position {
            control.complete_bytes(permitted)?;
            continue;
        }
        std::fmt::Write::write_str(retained, &value[position..end])
            .map_err(|error| ArrowError::ExternalError(Box::new(error)))?;
        let completed = end - position;
        control.complete_bytes(completed)?;
        position = end;
    }
    Ok(())
}

fn controlled_string_array<V: ControlledStringArrayValue>(
    values: Vec<V>,
    control: &mut ArrowOperationControl<'_>,
) -> Result<StringArray, ArrowConversionError> {
    let row_count = values.len();
    row_count
        .checked_add(1)
        .ok_or(ArrowConversionError::RetainedSizeOverflow)?;
    let value_bytes_len = controlled_string_bytes_len(&values, control)?;
    control.checkpoint_now()?;
    let mut builder = StringBuilder::with_capacity(row_count, value_bytes_len);
    control.checkpoint_now()?;
    for (ordinal, value) in values.into_iter().enumerate() {
        control.checkpoint_row(ordinal)?;
        if let Some(value) = value.controlled_str() {
            write_controlled_str(&mut builder, value, control)?;
            builder.append_value("");
        } else {
            builder.append_null();
        }
    }
    control.checkpoint_now()?;
    let array = builder.finish();
    control.checkpoint_now()?;
    Ok(array)
}

fn controlled_primitive_array<I, T, F>(
    values: I,
    mut value: F,
    control: &mut ArrowOperationControl<'_>,
) -> Result<PrimitiveArray<T>, ArrowConversionError>
where
    I: IntoIterator,
    I::IntoIter: ExactSizeIterator,
    T: ArrowPrimitiveType,
    F: FnMut(I::Item) -> Option<T::Native>,
{
    let values = values.into_iter();
    let row_count = values.len();
    control.checkpoint_now()?;
    let mut builder = PrimitiveBuilder::<T>::with_capacity(row_count);
    control.checkpoint_now()?;
    for (ordinal, source) in values.enumerate() {
        control.checkpoint_row(ordinal)?;
        builder.append_option(value(source));
    }
    control.checkpoint_now()?;
    let array = builder.finish();
    control.checkpoint_now()?;
    Ok(array)
}

#[path = "arrow_convert/dataset.rs"]
mod dataset;
pub use dataset::DatasetArrowBatch;

/// A request- and dataset-bound canonical Arrow record batch.
#[derive(Clone, Debug)]
pub struct ResearchArrowBatch {
    schema_ref: DatasetSchemaRef,
    batch: RecordBatch,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "binding", rename_all = "snake_case")]
enum RowLineage {
    Extraction(Box<ExtractionRowLineage>),
    CanonicalObservation {
        schema_version: u16,
        source_id: SourceId,
        dataset: SourceIdentifier,
        request_digest: EvidenceDigest,
    },
}

impl RowLineage {
    fn dataset(&self) -> &SourceIdentifier {
        match self {
            Self::Extraction(lineage) => &lineage.dataset,
            Self::CanonicalObservation { dataset, .. } => dataset,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ExtractionRowLineage {
    schema_version: u16,
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    dataset: SourceIdentifier,
    discovery_request_id: DiscoveryRequestId,
    extraction_request_id: ExtractionRequestId,
    request_digest: EvidenceDigest,
    object_id: SourceIdentifier,
    object_evidence: ExactPayloadEvidence,
    record_schema: SourceIdentifier,
    record_evidence: ExactPayloadEvidence,
    effective_time: ResearchTemporalCoordinate,
    published_time: Option<ResearchTemporalCoordinate>,
    availability: SourceAvailabilityEvidence,
    revision: SourceIdentifier,
    superseded_time: Option<ResearchTemporalCoordinate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    revision_assignment: Option<RevisionAssignmentLineage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    provider_capture: Option<ProviderCaptureRowLineage>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct RevisionAssignmentLineage {
    assigned_revision: RevisionNumber,
    semantic_payload_identity: EvidenceDigest,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ProviderCaptureRowLineage {
    binding_digest: EvidenceDigest,
    capture_observation_digest: EvidenceDigest,
    canonical_row_ordinal: u32,
    native_semantic_digest: EvidenceDigest,
    capture_page_ordinal: u16,
    segment_ordinal: u16,
    physical_frame_ordinal: u32,
    page_body_digest: EvidenceDigest,
}

/// Already-validated durable coordinates tying one canonical Arrow row to provider capture.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProviderCaptureRowCoordinate {
    pub(crate) binding_digest: EvidenceDigest,
    pub(crate) capture_observation_digest: EvidenceDigest,
    pub(crate) canonical_row_ordinal: u32,
    pub(crate) canonical_row_digest: EvidenceDigest,
    pub(crate) observation_digest: EvidenceDigest,
    pub(crate) native_semantic_digest: EvidenceDigest,
    pub(crate) capture_page_ordinal: u16,
    pub(crate) segment_ordinal: u16,
    pub(crate) physical_frame_ordinal: u32,
    pub(crate) page_body_digest: EvidenceDigest,
}

/// One canonical batch decoded and capture-validated without repeating payload or lineage work.
pub(crate) struct DecodedProviderCaptureBatch {
    pub(crate) schema_ref: DatasetSchemaRef,
    pub(crate) observations: Vec<ResearchObservation>,
    pub(crate) coordinates: Vec<ProviderCaptureRowCoordinate>,
    pub(crate) retained_bytes: usize,
}

/// Streaming form of the canonical ordered row-lineage digest across Parquet batches.
pub(crate) struct ResearchLineageDigestAccumulator {
    hash: Sha256,
}

impl ResearchLineageDigestAccumulator {
    pub(crate) fn new() -> Self {
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/research-row-lineage/v3");
        Self { hash }
    }

    pub(crate) fn finish(self) -> EvidenceDigest {
        EvidenceDigest::new(DigestAlgorithm::Sha256, self.hash.finalize().into())
    }

    fn update(
        &mut self,
        batch: &ResearchArrowBatch,
        mut control: Option<&mut ArrowOperationControl<'_>>,
    ) -> Result<(), ArrowConversionError> {
        let request_digests = batch
            .batch
            .column_by_name("request_sha256")
            .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
            .ok_or(ArrowConversionError::InvalidSchema)?;
        let lineages = batch
            .batch
            .column_by_name("extraction_lineage_json")
            .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
            .ok_or(ArrowConversionError::InvalidSchema)?;
        let payload_digests = batch
            .batch
            .column_by_name("payload_sha256")
            .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
            .ok_or(ArrowConversionError::InvalidSchema)?;
        if request_digests.len() != lineages.len() || lineages.len() != payload_digests.len() {
            return Err(ArrowConversionError::InvalidSchema);
        }
        for (ordinal, ((request_digest, lineage), payload_digest)) in request_digests
            .iter()
            .zip(lineages)
            .zip(payload_digests)
            .enumerate()
        {
            if let Some(control) = control.as_deref_mut() {
                control.checkpoint_row(ordinal)?;
            }
            let request_digest = request_digest.ok_or(ArrowConversionError::InvalidSchema)?;
            let lineage = lineage.ok_or(ArrowConversionError::InvalidSchema)?;
            let payload_digest = payload_digest.ok_or(ArrowConversionError::InvalidSchema)?;
            if request_digest.len() != 32 || payload_digest.len() != 32 {
                return Err(ArrowConversionError::InvalidSchema);
            }
            let lineage_bytes = u64::try_from(lineage.len())
                .map_err(|_| ArrowConversionError::InvalidSchema)?
                .to_be_bytes();
            self.hash.update(request_digest);
            self.hash.update(lineage_bytes);
            update_hash_bytes(&mut self.hash, lineage, control.as_deref_mut())?;
            self.hash.update(payload_digest);
        }
        if let Some(control) = control {
            control.checkpoint_now()?;
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct ResearchObservationEnvelopeTag {
    observation: String,
}

const CAPTURED_EXTRACTION_LINEAGE_SCHEMA_VERSION: u16 = 5;
const EXTRACTION_LINEAGE_SCHEMA_VERSION: u16 = 4;
const LEGACY_EXTRACTION_LINEAGE_SCHEMA_VERSION: u16 = 3;

impl ResearchArrowBatch {
    /// Converts only canonical observations retained by one exact extraction request.
    pub fn try_from_extraction_batch(
        extraction: &ExtractionBatch,
    ) -> Result<Self, ArrowConversionError> {
        Self::try_from_extraction_batch_with_revisions(extraction, None, None)
    }

    /// Returns source-validated canonical observations before durable revision rebinding.
    pub(crate) fn validated_extraction_observations(
        extraction: &ExtractionBatch,
    ) -> Result<Vec<ResearchObservation>, ArrowConversionError> {
        if extraction.records().is_empty() {
            return Err(ArrowConversionError::EmptyBatch);
        }
        let request = serde_json::to_vec(extraction.request())?;
        let request_digest =
            EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(request).into());
        let mut observations = Vec::with_capacity(extraction.records().len());
        for record in extraction.records() {
            let observation: ResearchObservation = serde_json::from_slice(record.payload())?;
            let lineage = RowLineage::Extraction(Box::new(ExtractionRowLineage {
                schema_version: LEGACY_EXTRACTION_LINEAGE_SCHEMA_VERSION,
                source_id: record.source_id().clone(),
                metadata_revision: record.metadata_revision().clone(),
                dataset: record.dataset().clone(),
                discovery_request_id: record.discovery_request_id(),
                extraction_request_id: record.extraction_request_id(),
                request_digest,
                object_id: record.object_id().clone(),
                object_evidence: record.object_evidence().clone(),
                record_schema: record.schema().clone(),
                record_evidence: record.evidence().clone(),
                effective_time: record.effective_time().clone(),
                published_time: record.published_time().cloned(),
                availability: record.availability().clone(),
                revision: record.revision().clone(),
                superseded_time: record.superseded_time().cloned(),
                revision_assignment: None,
                provider_capture: None,
            }));
            validate_row_lineage(
                &lineage,
                extraction.request().object().dataset(),
                request_digest.bytes(),
                &observation,
                record.payload(),
                None,
            )?;
            observations.push(observation);
        }
        Ok(observations)
    }

    pub(crate) fn try_from_extraction_batch_with_assigned_revisions(
        extraction: &ExtractionBatch,
        revisions: &[RevisionNumber],
    ) -> Result<Self, ArrowConversionError> {
        Self::try_from_extraction_batch_with_revisions(extraction, Some(revisions), None)
    }

    pub(crate) fn try_from_extraction_batch_with_assigned_revisions_and_provider_binding(
        extraction: &ExtractionBatch,
        revisions: &[RevisionNumber],
        binding: &PreparedProviderCaptureBinding,
    ) -> Result<Self, ArrowConversionError> {
        Self::try_from_extraction_batch_with_revisions(extraction, Some(revisions), Some(binding))
    }

    fn try_from_extraction_batch_with_revisions(
        extraction: &ExtractionBatch,
        revisions: Option<&[RevisionNumber]>,
        binding: Option<&PreparedProviderCaptureBinding>,
    ) -> Result<Self, ArrowConversionError> {
        let original_observations = Self::validated_extraction_observations(extraction)?;
        if revisions.is_some_and(|values| values.len() != original_observations.len()) {
            return Err(ArrowConversionError::RevisionAssignmentMismatch);
        }
        if matches!(
            extraction.request().object().capture_identity(),
            SourceObjectCaptureIdentity::Paged { .. }
        ) != binding.is_some()
        {
            return Err(ArrowConversionError::ProviderCaptureRequired);
        }
        let request = serde_json::to_vec(extraction.request())?;
        let request_digest =
            EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(request).into());
        let mut observations = Vec::with_capacity(original_observations.len());
        let mut lineages = Vec::with_capacity(original_observations.len());
        for (index, (record, original)) in extraction
            .records()
            .iter()
            .zip(original_observations)
            .enumerate()
        {
            let assignment = revisions
                .map(
                    |values| -> Result<RevisionAssignmentLineage, ArrowConversionError> {
                        let assigned_revision = values
                            .get(index)
                            .copied()
                            .ok_or(ArrowConversionError::RevisionAssignmentMismatch)?;
                        let payload = CanonicalObservationPayload::try_from_observation(&original)
                            .map_err(ArrowConversionError::RevisionAuthority)?;
                        Ok(RevisionAssignmentLineage {
                            assigned_revision,
                            semantic_payload_identity: payload.identity(),
                        })
                    },
                )
                .transpose()?;
            let observation = match &assignment {
                Some(assignment) => original.with_revision(assignment.assigned_revision)?,
                None => original,
            };
            lineages.push(RowLineage::Extraction(Box::new(ExtractionRowLineage {
                schema_version: if binding.is_some() {
                    CAPTURED_EXTRACTION_LINEAGE_SCHEMA_VERSION
                } else if assignment.is_some() {
                    EXTRACTION_LINEAGE_SCHEMA_VERSION
                } else {
                    LEGACY_EXTRACTION_LINEAGE_SCHEMA_VERSION
                },
                source_id: record.source_id().clone(),
                metadata_revision: record.metadata_revision().clone(),
                dataset: record.dataset().clone(),
                discovery_request_id: record.discovery_request_id(),
                extraction_request_id: record.extraction_request_id(),
                request_digest,
                object_id: record.object_id().clone(),
                object_evidence: record.object_evidence().clone(),
                record_schema: record.schema().clone(),
                record_evidence: record.evidence().clone(),
                effective_time: record.effective_time().clone(),
                published_time: record.published_time().cloned(),
                availability: record.availability().clone(),
                revision: record.revision().clone(),
                superseded_time: record.superseded_time().cloned(),
                revision_assignment: assignment,
                provider_capture: binding
                    .map(|binding| provider_capture_lineage(binding, index))
                    .transpose()?,
            })));
            observations.push(observation);
        }
        let request_digests = vec![request_digest.bytes(); observations.len()];
        Self::try_from_observations_with_requests(
            extraction.request().object().dataset().clone(),
            request_digest,
            request_digests,
            lineages,
            &observations,
            None,
        )
    }

    /// Converts validated canonical observations without floating-point or decimal rescaling.
    pub fn try_from_observations(
        dataset: SourceIdentifier,
        request_digest: EvidenceDigest,
        observations: Vec<ResearchObservation>,
    ) -> Result<Self, ArrowConversionError> {
        let request_digests = vec![request_digest.bytes(); observations.len()];
        let lineages = observations
            .iter()
            .map(|observation| RowLineage::CanonicalObservation {
                schema_version: RESEARCH_SCHEMA_VERSION,
                source_id: observation_context(observation)
                    .provenance()
                    .source_id()
                    .clone(),
                dataset: dataset.clone(),
                request_digest,
            })
            .collect();
        Self::try_from_observations_with_requests(
            dataset,
            request_digest,
            request_digests,
            lineages,
            &observations,
            None,
        )
    }

    /// Rewrites already validated objects into one compaction batch while retaining each row's
    /// exact extraction-request identity.
    pub(crate) fn try_from_compaction_batches(
        dataset: SourceIdentifier,
        compaction_digest: EvidenceDigest,
        batches: Vec<RecordBatch>,
    ) -> Result<Self, ArrowConversionError> {
        if batches.is_empty() {
            return Err(ArrowConversionError::EmptyBatch);
        }
        let target_schema = research_schema(&dataset, compaction_digest)?;
        let mut normalized = Vec::with_capacity(batches.len());
        for batch in batches {
            let validated = Self::try_from_record_batch(batch)?;
            let retained_dataset = validated
                .batch
                .schema()
                .metadata()
                .get(DATASET_KEY)
                .cloned()
                .ok_or(ArrowConversionError::InvalidSchemaMetadata)?;
            if retained_dataset != dataset.as_str() {
                return Err(ArrowConversionError::ExtractionBindingMismatch);
            }
            normalized.push(RecordBatch::try_new(
                Arc::clone(&target_schema),
                validated.batch.columns().to_vec(),
            )?);
        }
        let compacted = concat_batches(&target_schema, &normalized)?;
        Self::try_from_record_batch(compacted)
    }

    /// Groups already validated captured-extraction batches under one analytical dataset while
    /// retaining every original row and extraction-lineage column unchanged.
    pub(crate) fn try_rebind_provider_capture_batches(
        provider_dataset: &SourceIdentifier,
        analytical_dataset: SourceIdentifier,
        publication_digest: EvidenceDigest,
        batches: Vec<RecordBatch>,
        max_additional_bytes_per_batch: usize,
        control: &dyn ResearchObjectControl,
    ) -> Result<Self, ArrowConversionError> {
        if batches.is_empty() || max_additional_bytes_per_batch == 0 {
            return Err(ArrowConversionError::EmptyBatch);
        }
        let target_schema = research_schema(&analytical_dataset, publication_digest)?;
        let mut normalized = Vec::new();
        normalized
            .try_reserve_exact(batches.len())
            .map_err(|_| ArrowConversionError::AllocationFailure)?;
        let mut lineage = ResearchLineageDigestAccumulator::new();
        for batch in batches {
            let retained_dataset = batch
                .schema()
                .metadata()
                .get(DATASET_KEY)
                .cloned()
                .ok_or(ArrowConversionError::InvalidSchemaMetadata)?;
            if retained_dataset != provider_dataset.as_str() {
                return Err(ArrowConversionError::ExtractionBindingMismatch);
            }
            let decoded = Self::decode_provider_capture_record_batch_bounded(
                batch.clone(),
                max_additional_bytes_per_batch,
                &mut lineage,
                control,
            )?;
            if decoded.observations.len() != batch.num_rows()
                || decoded.coordinates.len() != batch.num_rows()
            {
                return Err(ArrowConversionError::ExtractionBindingMismatch);
            }
            normalized.push(RecordBatch::try_new(
                Arc::clone(&target_schema),
                batch.columns().to_vec(),
            )?);
        }
        let source_lineage_digest = lineage.finish();
        let grouped = concat_batches(&target_schema, &normalized)?;
        let rebound = Self::try_from_record_batch(grouped)?;
        if rebound.lineage_digest()? != source_lineage_digest {
            return Err(ArrowConversionError::ExtractionBindingMismatch);
        }
        Ok(rebound)
    }

    fn try_from_observations_with_requests(
        dataset: SourceIdentifier,
        batch_digest: EvidenceDigest,
        request_digests: Vec<[u8; 32]>,
        row_lineages: Vec<RowLineage>,
        observations: &[ResearchObservation],
        mut control: Option<&mut ArrowOperationControl<'_>>,
    ) -> Result<Self, ArrowConversionError> {
        if !matches!(batch_digest.algorithm(), DigestAlgorithm::Sha256) {
            return Err(ArrowConversionError::RequestDigestNotSha256);
        }
        if request_digests.len() != observations.len() || row_lineages.len() != observations.len() {
            return Err(ArrowConversionError::InvalidSchema);
        }
        for (ordinal, ((lineage, observation), request_digest)) in row_lineages
            .iter()
            .zip(observations)
            .zip(&request_digests)
            .enumerate()
        {
            if let Some(control) = control.as_deref_mut() {
                control.checkpoint_row(ordinal)?;
            }
            let payload = encode_json(observation, control.as_deref_mut())?;
            validate_row_lineage(
                lineage,
                &dataset,
                *request_digest,
                observation,
                &payload,
                control.as_deref_mut(),
            )?;
        }
        let mut encoded_lineages = Vec::new();
        encoded_lineages
            .try_reserve_exact(row_lineages.len())
            .map_err(|_| ArrowConversionError::AllocationFailure)?;
        for (ordinal, lineage) in row_lineages.iter().enumerate() {
            if let Some(control) = control.as_deref_mut() {
                control.checkpoint_row(ordinal)?;
            }
            encoded_lineages.push(encode_json(lineage, control.as_deref_mut())?);
        }
        let row_lineages = encoded_lineages;
        let mut payloads = Vec::with_capacity(observations.len());
        let mut payload_digests = Vec::with_capacity(observations.len());
        let mut kinds = Vec::with_capacity(observations.len());
        let mut source_ids = Vec::with_capacity(observations.len());
        let mut instrument_ids = Vec::with_capacity(observations.len());
        let mut venue_ids = Vec::with_capacity(observations.len());
        let mut source_identifiers = Vec::with_capacity(observations.len());
        let mut macro_series = Vec::with_capacity(observations.len());
        let mut source_timestamps = Vec::with_capacity(observations.len());
        let mut received_at = Vec::with_capacity(observations.len());
        let mut available_at = Vec::with_capacity(observations.len());
        let mut availability_reported_or_inferred_at = Vec::with_capacity(observations.len());
        let mut availability_kinds = Vec::with_capacity(observations.len());
        let mut availability_evidence = Vec::with_capacity(observations.len());
        let mut availability_methods = Vec::with_capacity(observations.len());
        let mut ingested_at = Vec::with_capacity(observations.len());
        let mut effective_precision = Vec::with_capacity(observations.len());
        let mut effective_at = Vec::with_capacity(observations.len());
        let mut effective_date = Vec::with_capacity(observations.len());
        let mut effective_period_scheme = Vec::with_capacity(observations.len());
        let mut effective_period_year = Vec::with_capacity(observations.len());
        let mut effective_period_ordinal = Vec::with_capacity(observations.len());
        let mut effective_period_code = Vec::with_capacity(observations.len());
        let mut published_precision = Vec::with_capacity(observations.len());
        let mut published_at = Vec::with_capacity(observations.len());
        let mut published_date = Vec::with_capacity(observations.len());
        let mut published_period_scheme = Vec::with_capacity(observations.len());
        let mut published_period_year = Vec::with_capacity(observations.len());
        let mut published_period_ordinal = Vec::with_capacity(observations.len());
        let mut published_period_code = Vec::with_capacity(observations.len());
        let mut revisions = Vec::with_capacity(observations.len());
        let mut superseded_precision = Vec::with_capacity(observations.len());
        let mut superseded_at = Vec::with_capacity(observations.len());
        let mut superseded_date = Vec::with_capacity(observations.len());
        let mut superseded_period_scheme = Vec::with_capacity(observations.len());
        let mut superseded_period_year = Vec::with_capacity(observations.len());
        let mut superseded_period_ordinal = Vec::with_capacity(observations.len());
        let mut superseded_period_code = Vec::with_capacity(observations.len());
        let mut qualities = Vec::with_capacity(observations.len());
        let mut value_states = Vec::with_capacity(observations.len());
        let mut missing_markers = Vec::with_capacity(observations.len());
        let mut missing_reasons = Vec::with_capacity(observations.len());
        let mut mantissas = Vec::with_capacity(observations.len());
        let mut scales = Vec::with_capacity(observations.len());
        let mut units = Vec::with_capacity(observations.len());
        let mut currencies = Vec::with_capacity(observations.len());

        for (ordinal, observation) in observations.iter().enumerate() {
            if let Some(control) = control.as_deref_mut() {
                control.checkpoint_row(ordinal)?;
            }
            let payload = encode_json(observation, control.as_deref_mut())?;
            let mut payload_hash = Sha256::new();
            update_hash_bytes(&mut payload_hash, &payload, control.as_deref_mut())?;
            payload_digests.push(payload_hash.finalize().to_vec());
            payloads.push(payload);
            let context = observation_context(observation);
            let provenance = context.provenance();
            let time = context.time();
            kinds.push(observation_kind(observation));
            source_ids.push(provenance.source_id().as_str().to_owned());
            instrument_ids.push(provenance.instrument_id().map(|id| id.to_string()));
            venue_ids.push(provenance.venue_id().map(|id| id.as_str().to_owned()));
            source_identifiers.push(provenance.source_identifier().as_str().to_owned());
            macro_series.push(match observation {
                ResearchObservation::Macro(macro_observation) => {
                    Some(macro_observation.series().as_str().to_owned())
                }
                _ => None,
            });
            source_timestamps.push(
                provenance
                    .source_timestamp()
                    .map(|value| value.unix_nanos()),
            );
            received_at.push(provenance.received_at().unix_nanos());
            let availability = provenance.availability();
            available_at.push(
                availability
                    .conservative_available_at()
                    .map(|value| value.unix_nanos()),
            );
            availability_reported_or_inferred_at
                .push(availability.reported_at().map(|value| value.unix_nanos()));
            let (kind, evidence, method) = availability_projection(availability);
            availability_kinds.push(kind);
            availability_evidence.push(evidence);
            availability_methods.push(method);
            ingested_at.push(provenance.ingested_at().unix_nanos());
            let effective = temporal_projection(Some(time.effective()));
            effective_precision.push(effective.precision);
            effective_at.push(effective.timestamp);
            effective_date.push(effective.date);
            effective_period_scheme.push(effective.period_scheme);
            effective_period_year.push(effective.period_year);
            effective_period_ordinal.push(effective.period_ordinal);
            effective_period_code.push(effective.period_code);
            let published = temporal_projection(time.published());
            published_precision.push(published.precision);
            published_at.push(published.timestamp);
            published_date.push(published.date);
            published_period_scheme.push(published.period_scheme);
            published_period_year.push(published.period_year);
            published_period_ordinal.push(published.period_ordinal);
            published_period_code.push(published.period_code);
            revisions.push(time.revision().get());
            let superseded = temporal_projection(time.superseded());
            superseded_precision.push(superseded.precision);
            superseded_at.push(superseded.timestamp);
            superseded_date.push(superseded.date);
            superseded_period_scheme.push(superseded.period_scheme);
            superseded_period_year.push(superseded.period_year);
            superseded_period_ordinal.push(superseded.period_ordinal);
            superseded_period_code.push(superseded.period_code);
            qualities.push(quality_name(provenance.quality()));
            let value = analytical_value(observation);
            value_states.push(value.state);
            missing_markers.push(value.missing_marker.map(str::to_owned));
            missing_reasons.push(value.missing_reason.map(str::to_owned));
            mantissas.push(value.decimal.map(|decimal| decimal.mantissa()));
            scales.push(
                value
                    .decimal
                    .map(|decimal| u8::try_from(decimal.scale()))
                    .transpose()?,
            );
            units.push(value.unit.map(str::to_owned));
            currencies.push(
                value
                    .currency
                    .map(|currency| currency.as_str().to_owned())
                    .or_else(|| {
                        value
                            .unit
                            .filter(|unit| is_currency(unit))
                            .map(|unit| (*unit).to_owned())
                    }),
            );
        }

        if let Some(control) = control.as_deref_mut() {
            control.checkpoint_now()?;
        }
        let arrays: Vec<ArrayRef> = match control.as_deref_mut() {
            Some(control) => {
                let schema_versions = controlled_primitive_array::<_, UInt16Type, _>(
                    std::iter::repeat_n(RESEARCH_SCHEMA_VERSION, observations.len()),
                    Some,
                    control,
                )?;
                let source_timestamps =
                    controlled_primitive_array::<_, TimestampNanosecondType, _>(
                        source_timestamps,
                        |value| value,
                        control,
                    )?
                    .with_timezone_utc();
                let received_at = controlled_primitive_array::<_, TimestampNanosecondType, _>(
                    received_at,
                    Some,
                    control,
                )?
                .with_timezone_utc();
                let available_at = controlled_primitive_array::<_, TimestampNanosecondType, _>(
                    available_at,
                    |value| value,
                    control,
                )?
                .with_timezone_utc();
                let availability_reported_or_inferred_at =
                    controlled_primitive_array::<_, TimestampNanosecondType, _>(
                        availability_reported_or_inferred_at,
                        |value| value,
                        control,
                    )?
                    .with_timezone_utc();
                let ingested_at = controlled_primitive_array::<_, TimestampNanosecondType, _>(
                    ingested_at,
                    Some,
                    control,
                )?
                .with_timezone_utc();
                let effective_at = controlled_primitive_array::<_, TimestampNanosecondType, _>(
                    effective_at,
                    |value| value,
                    control,
                )?
                .with_timezone_utc();
                let effective_date = controlled_primitive_array::<_, Date32Type, _>(
                    effective_date,
                    |value| value,
                    control,
                )?;
                let effective_period_year = controlled_primitive_array::<_, UInt16Type, _>(
                    effective_period_year,
                    |value| value,
                    control,
                )?;
                let effective_period_ordinal = controlled_primitive_array::<_, UInt16Type, _>(
                    effective_period_ordinal,
                    |value| value,
                    control,
                )?;
                let published_at = controlled_primitive_array::<_, TimestampNanosecondType, _>(
                    published_at,
                    |value| value,
                    control,
                )?
                .with_timezone_utc();
                let published_date = controlled_primitive_array::<_, Date32Type, _>(
                    published_date,
                    |value| value,
                    control,
                )?;
                let published_period_year = controlled_primitive_array::<_, UInt16Type, _>(
                    published_period_year,
                    |value| value,
                    control,
                )?;
                let published_period_ordinal = controlled_primitive_array::<_, UInt16Type, _>(
                    published_period_ordinal,
                    |value| value,
                    control,
                )?;
                let revisions =
                    controlled_primitive_array::<_, UInt32Type, _>(revisions, Some, control)?;
                let superseded_at = controlled_primitive_array::<_, TimestampNanosecondType, _>(
                    superseded_at,
                    |value| value,
                    control,
                )?
                .with_timezone_utc();
                let superseded_date = controlled_primitive_array::<_, Date32Type, _>(
                    superseded_date,
                    |value| value,
                    control,
                )?;
                let superseded_period_year = controlled_primitive_array::<_, UInt16Type, _>(
                    superseded_period_year,
                    |value| value,
                    control,
                )?;
                let superseded_period_ordinal = controlled_primitive_array::<_, UInt16Type, _>(
                    superseded_period_ordinal,
                    |value| value,
                    control,
                )?;
                let decimal = controlled_primitive_array::<_, Decimal128Type, _>(
                    mantissas,
                    |value| value,
                    control,
                )?
                .with_precision_and_scale(38, 0)?;
                let scales =
                    controlled_primitive_array::<_, UInt8Type, _>(scales, |value| value, control)?;

                control.checkpoint_now()?;
                let arrays: Vec<ArrayRef> = vec![
                    Arc::new(schema_versions),
                    Arc::new(controlled_binary_array(request_digests, control)?),
                    Arc::new(controlled_binary_array(row_lineages, control)?),
                    Arc::new(controlled_string_array(kinds, control)?),
                    Arc::new(controlled_string_array(source_ids, control)?),
                    Arc::new(controlled_string_array(instrument_ids, control)?),
                    Arc::new(controlled_string_array(venue_ids, control)?),
                    Arc::new(controlled_string_array(source_identifiers, control)?),
                    Arc::new(controlled_string_array(macro_series, control)?),
                    Arc::new(source_timestamps),
                    Arc::new(received_at),
                    Arc::new(available_at),
                    Arc::new(availability_reported_or_inferred_at),
                    Arc::new(controlled_string_array(availability_kinds, control)?),
                    Arc::new(controlled_string_array(availability_evidence, control)?),
                    Arc::new(controlled_string_array(availability_methods, control)?),
                    Arc::new(ingested_at),
                    Arc::new(controlled_string_array(effective_precision, control)?),
                    Arc::new(effective_at),
                    Arc::new(effective_date),
                    Arc::new(controlled_string_array(effective_period_scheme, control)?),
                    Arc::new(effective_period_year),
                    Arc::new(effective_period_ordinal),
                    Arc::new(controlled_string_array(effective_period_code, control)?),
                    Arc::new(controlled_string_array(published_precision, control)?),
                    Arc::new(published_at),
                    Arc::new(published_date),
                    Arc::new(controlled_string_array(published_period_scheme, control)?),
                    Arc::new(published_period_year),
                    Arc::new(published_period_ordinal),
                    Arc::new(controlled_string_array(published_period_code, control)?),
                    Arc::new(revisions),
                    Arc::new(controlled_string_array(superseded_precision, control)?),
                    Arc::new(superseded_at),
                    Arc::new(superseded_date),
                    Arc::new(controlled_string_array(superseded_period_scheme, control)?),
                    Arc::new(superseded_period_year),
                    Arc::new(superseded_period_ordinal),
                    Arc::new(controlled_string_array(superseded_period_code, control)?),
                    Arc::new(controlled_string_array(qualities, control)?),
                    Arc::new(controlled_string_array(value_states, control)?),
                    Arc::new(controlled_string_array(missing_markers, control)?),
                    Arc::new(controlled_string_array(missing_reasons, control)?),
                    Arc::new(decimal),
                    Arc::new(scales),
                    Arc::new(controlled_string_array(units, control)?),
                    Arc::new(controlled_string_array(currencies, control)?),
                    Arc::new(controlled_binary_array(payload_digests, control)?),
                    Arc::new(controlled_binary_array(payloads, control)?),
                ];
                control.checkpoint_now()?;
                arrays
            }
            None => {
                let decimal = Decimal128Array::from(mantissas).with_precision_and_scale(38, 0)?;
                let utc = |values: Vec<Option<i64>>| {
                    TimestampNanosecondArray::from(values).with_timezone_utc()
                };
                vec![
                    Arc::new(UInt16Array::from_value(
                        RESEARCH_SCHEMA_VERSION,
                        observations.len(),
                    )),
                    Arc::new(BinaryArray::from_iter_values(request_digests)),
                    Arc::new(BinaryArray::from_iter_values(row_lineages)),
                    Arc::new(StringArray::from(kinds)),
                    Arc::new(StringArray::from(source_ids)),
                    Arc::new(StringArray::from(instrument_ids)),
                    Arc::new(StringArray::from(venue_ids)),
                    Arc::new(StringArray::from(source_identifiers)),
                    Arc::new(StringArray::from(macro_series)),
                    Arc::new(utc(source_timestamps)),
                    Arc::new(TimestampNanosecondArray::from(received_at).with_timezone_utc()),
                    Arc::new(utc(available_at)),
                    Arc::new(utc(availability_reported_or_inferred_at)),
                    Arc::new(StringArray::from(availability_kinds)),
                    Arc::new(StringArray::from(availability_evidence)),
                    Arc::new(StringArray::from(availability_methods)),
                    Arc::new(TimestampNanosecondArray::from(ingested_at).with_timezone_utc()),
                    Arc::new(StringArray::from(effective_precision)),
                    Arc::new(utc(effective_at)),
                    Arc::new(Date32Array::from(effective_date)),
                    Arc::new(StringArray::from(effective_period_scheme)),
                    Arc::new(UInt16Array::from(effective_period_year)),
                    Arc::new(UInt16Array::from(effective_period_ordinal)),
                    Arc::new(StringArray::from(effective_period_code)),
                    Arc::new(StringArray::from(published_precision)),
                    Arc::new(utc(published_at)),
                    Arc::new(Date32Array::from(published_date)),
                    Arc::new(StringArray::from(published_period_scheme)),
                    Arc::new(UInt16Array::from(published_period_year)),
                    Arc::new(UInt16Array::from(published_period_ordinal)),
                    Arc::new(StringArray::from(published_period_code)),
                    Arc::new(UInt32Array::from(revisions)),
                    Arc::new(StringArray::from(superseded_precision)),
                    Arc::new(utc(superseded_at)),
                    Arc::new(Date32Array::from(superseded_date)),
                    Arc::new(StringArray::from(superseded_period_scheme)),
                    Arc::new(UInt16Array::from(superseded_period_year)),
                    Arc::new(UInt16Array::from(superseded_period_ordinal)),
                    Arc::new(StringArray::from(superseded_period_code)),
                    Arc::new(StringArray::from(qualities)),
                    Arc::new(StringArray::from(value_states)),
                    Arc::new(StringArray::from(missing_markers)),
                    Arc::new(StringArray::from(missing_reasons)),
                    Arc::new(decimal),
                    Arc::new(UInt8Array::from(scales)),
                    Arc::new(StringArray::from(units)),
                    Arc::new(StringArray::from(currencies)),
                    Arc::new(BinaryArray::from_iter_values(payload_digests)),
                    Arc::new(BinaryArray::from_iter_values(payloads)),
                ]
            }
        };
        let schema_ref = DatasetSchemaRegistry::local().canonical_research_observations()?;
        let batch = RecordBatch::try_new(research_schema(&dataset, batch_digest)?, arrays)?;
        let validated = DatasetArrowBatch::try_new(schema_ref, batch)?;
        if let Some(control) = control {
            control.checkpoint_now()?;
        }
        Ok(Self {
            schema_ref: validated.schema_ref,
            batch: validated.batch,
        })
    }

    /// Validates a persisted batch against the complete current schema and every projected value.
    pub fn try_from_record_batch(batch: RecordBatch) -> Result<Self, ArrowConversionError> {
        Self::validate_and_decode_record_batch(batch, usize::MAX).map(|(candidate, _, _)| candidate)
    }

    /// Decodes one canonical research-observation batch under an exact retained-memory ceiling.
    ///
    /// The batch schema, per-row payload digest, and canonical observation invariants are verified
    /// before any observation is returned. This is the only supported application boundary for
    /// interpreting an inline canonical research query result.
    pub fn decode_record_batch_bounded(
        batch: RecordBatch,
        max_additional_bytes: usize,
    ) -> Result<(Vec<ResearchObservation>, usize), ArrowConversionError> {
        Self::validate_and_decode_record_batch(batch, max_additional_bytes)
            .map(|(_, observations, retained)| (observations, retained))
    }

    /// Validates and decodes one provider-capture batch in one bounded lineage/payload pass.
    pub(crate) fn decode_provider_capture_record_batch_bounded(
        batch: RecordBatch,
        max_additional_bytes: usize,
        lineage: &mut ResearchLineageDigestAccumulator,
        control: &dyn ResearchObjectControl,
    ) -> Result<DecodedProviderCaptureBatch, ArrowConversionError> {
        let mut operation = ArrowOperationControl::new(control);
        let (candidate, observations, observation_bytes, coordinates) =
            Self::validate_and_decode_record_batch_inner(
                batch,
                max_additional_bytes,
                true,
                Some(&mut operation),
            )?;
        let coordinates = coordinates.ok_or(ArrowConversionError::ProviderCaptureRequired)?;
        let retained_bytes = observation_bytes
            .checked_add(
                coordinates
                    .len()
                    .checked_mul(size_of::<ProviderCaptureRowCoordinate>())
                    .ok_or(ArrowConversionError::RetainedSizeOverflow)?,
            )
            .ok_or(ArrowConversionError::RetainedSizeOverflow)?;
        lineage.update(&candidate, Some(&mut operation))?;
        operation.checkpoint_now()?;
        Ok(DecodedProviderCaptureBatch {
            schema_ref: candidate.schema_ref.clone(),
            observations,
            coordinates,
            retained_bytes,
        })
    }

    /// Decodes a canonical research projection whose query engine discarded schema metadata.
    ///
    /// The registered fields, common producer-dataset identity retained by every row lineage,
    /// payload digests, and every projected value are revalidated. The producer dataset is not the
    /// same identity as a derived publication manifest, so it is recovered from the exact row
    /// lineage instead of being inferred from that manifest. This does not itself establish
    /// publication-generation authority; callers must retain the non-forgeable pinned-query
    /// receipt that supplied the batch.
    ///
    /// # Errors
    ///
    /// Returns [`ArrowConversionError::InvalidSchema`] when the projection is not the complete
    /// canonical row shape, [`ArrowConversionError::ExtractionBindingMismatch`] when rows do not
    /// share one exact producer dataset or disagree with their lineage, or
    /// [`ArrowConversionError::RetainedLimitExceeded`] when the caller's memory ceiling cannot
    /// admit validation.
    pub fn decode_query_projection_bounded(
        batch: RecordBatch,
        max_additional_bytes: usize,
    ) -> Result<(Vec<ResearchObservation>, usize), ArrowConversionError> {
        let registry = DatasetSchemaRegistry::local();
        let schema_ref = registry.canonical_research_observations()?;
        let comparison_schema = registry.resolve(&schema_ref)?;
        if batch.schema().fields() != comparison_schema.fields() {
            return Err(ArrowConversionError::InvalidSchema);
        }
        if batch.num_rows() == 0 {
            return Ok((Vec::new(), 0));
        }
        let candidate = Self { schema_ref, batch };
        let (working_bytes, observation_bytes) = candidate.decode_admission(None)?;
        if working_bytes > max_additional_bytes {
            return Err(ArrowConversionError::RetainedLimitExceeded);
        }
        let observations = candidate.decode_payloads(None)?;
        let request_digests = candidate.decode_request_digests(None)?;
        let row_lineages = candidate.decode_row_lineages(None)?;
        let producer_dataset = row_lineages
            .first()
            .map(RowLineage::dataset)
            .cloned()
            .ok_or(ArrowConversionError::InvalidSchema)?;
        if row_lineages
            .iter()
            .any(|lineage| lineage.dataset() != &producer_dataset)
        {
            return Err(ArrowConversionError::ExtractionBindingMismatch);
        }
        let batch_digest = request_digests
            .first()
            .copied()
            .ok_or(ArrowConversionError::InvalidSchema)?;
        let rebuilt = Self::try_from_observations_with_requests(
            producer_dataset,
            EvidenceDigest::new(DigestAlgorithm::Sha256, batch_digest),
            request_digests,
            row_lineages,
            &observations,
            None,
        )?;
        if rebuilt.batch.columns() != candidate.batch.columns() {
            return Err(ArrowConversionError::ProjectionMismatch);
        }
        Ok((observations, observation_bytes))
    }

    fn validate_and_decode_record_batch(
        batch: RecordBatch,
        max_additional_bytes: usize,
    ) -> Result<(Self, Vec<ResearchObservation>, usize), ArrowConversionError> {
        Self::validate_and_decode_record_batch_inner(batch, max_additional_bytes, false, None)
            .map(|(candidate, observations, retained, _)| (candidate, observations, retained))
    }

    fn validate_and_decode_record_batch_inner(
        batch: RecordBatch,
        max_additional_bytes: usize,
        require_provider_capture: bool,
        mut control: Option<&mut ArrowOperationControl<'_>>,
    ) -> Result<
        (
            Self,
            Vec<ResearchObservation>,
            usize,
            Option<Vec<ProviderCaptureRowCoordinate>>,
        ),
        ArrowConversionError,
    > {
        if let Some(control) = control.as_deref_mut() {
            control.checkpoint_now()?;
        }
        let metadata = batch.schema().metadata().clone();
        let version = metadata
            .get(SCHEMA_VERSION_KEY)
            .and_then(|value| value.parse::<u16>().ok())
            .ok_or(ArrowConversionError::InvalidSchemaMetadata)?;
        if version != RESEARCH_SCHEMA_VERSION {
            return Err(ArrowConversionError::UnsupportedSchemaVersion { found: version });
        }
        let validated = DatasetArrowBatch::try_from_record_batch(batch)?;
        if let Some(control) = control.as_deref_mut() {
            control.checkpoint_now()?;
        }
        if validated.schema_ref.name() != RESEARCH_SCHEMA_NAME {
            return Err(ArrowConversionError::UnexpectedDatasetSchema);
        }
        let DatasetArrowBatch { schema_ref, batch } = validated;
        let dataset = metadata
            .get(DATASET_KEY)
            .ok_or(ArrowConversionError::InvalidSchemaMetadata)
            .and_then(|value| {
                SourceIdentifier::try_from(value.as_str())
                    .map_err(|_| ArrowConversionError::InvalidSchemaMetadata)
            })?;
        let request_digest = metadata
            .get(REQUEST_DIGEST_KEY)
            .and_then(|value| decode_hex(value))
            .ok_or(ArrowConversionError::InvalidSchemaMetadata)?;
        if batch.schema().fields()
            != research_schema(
                &dataset,
                EvidenceDigest::new(DigestAlgorithm::Sha256, request_digest),
            )?
            .fields()
        {
            return Err(ArrowConversionError::InvalidSchema);
        }
        let candidate = Self { schema_ref, batch };
        let (working_bytes, observation_bytes) =
            candidate.decode_admission(control.as_deref_mut())?;
        let coordinate_bytes = if require_provider_capture {
            candidate
                .batch
                .num_rows()
                .checked_mul(size_of::<ProviderCaptureRowCoordinate>())
                .ok_or(ArrowConversionError::RetainedSizeOverflow)?
        } else {
            0
        };
        if working_bytes
            .checked_add(coordinate_bytes)
            .ok_or(ArrowConversionError::RetainedSizeOverflow)?
            > max_additional_bytes
        {
            return Err(ArrowConversionError::RetainedLimitExceeded);
        }
        let observations = candidate.decode_payloads(control.as_deref_mut())?;
        let request_digests = candidate.decode_request_digests(control.as_deref_mut())?;
        let row_lineages = candidate.decode_row_lineages(control.as_deref_mut())?;
        let coordinates = require_provider_capture
            .then(|| {
                candidate
                    .provider_capture_row_coordinates_from(&row_lineages, control.as_deref_mut())
            })
            .transpose()?;
        let rebuilt = Self::try_from_observations_with_requests(
            dataset,
            EvidenceDigest::new(DigestAlgorithm::Sha256, request_digest),
            request_digests,
            row_lineages,
            &observations,
            control.as_deref_mut(),
        )?;
        let projection_matches = match control.as_deref_mut() {
            Some(control) => {
                record_batches_equal_controlled(&rebuilt.batch, &candidate.batch, control)?
            }
            None => rebuilt.batch == candidate.batch,
        };
        if !projection_matches {
            return Err(ArrowConversionError::ProjectionMismatch);
        }
        if let Some(control) = control {
            control.checkpoint_now()?;
        }
        Ok((candidate, observations, observation_bytes, coordinates))
    }

    /// Returns the immutable Arrow batch.
    pub const fn record_batch(&self) -> &RecordBatch {
        &self.batch
    }

    /// Returns the complete canonical research dataset-schema identity.
    pub const fn schema_ref(&self) -> &DatasetSchemaRef {
        &self.schema_ref
    }

    /// Returns the generic registered-dataset publication view.
    pub fn dataset_batch(&self) -> DatasetArrowBatch {
        DatasetArrowBatch {
            schema_ref: self.schema_ref.clone(),
            batch: self.batch.clone(),
        }
    }

    /// Returns the exact analytical row-schema version retained in this batch.
    ///
    /// # Errors
    ///
    /// Returns [`ArrowConversionError::InvalidSchemaMetadata`] when the mandatory version is
    /// absent, malformed, or zero.
    pub fn schema_version(&self) -> Result<SchemaVersion, ArrowConversionError> {
        Ok(self.schema_ref.version())
    }

    /// Reconstructs canonical observations after validating every projected column.
    pub fn observations(&self) -> Result<Vec<ResearchObservation>, ArrowConversionError> {
        Self::validate_and_decode_record_batch(self.batch.clone(), usize::MAX)
            .map(|(_, observations, _)| observations)
    }

    fn provider_capture_row_coordinates_from(
        &self,
        lineages: &[RowLineage],
        mut control: Option<&mut ArrowOperationControl<'_>>,
    ) -> Result<Vec<ProviderCaptureRowCoordinate>, ArrowConversionError> {
        let payload_digests = self
            .batch
            .column_by_name("payload_sha256")
            .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
            .ok_or(ArrowConversionError::InvalidSchema)?;
        if payload_digests.len() != lineages.len() {
            return Err(ArrowConversionError::InvalidSchema);
        }
        let mut coordinates = Vec::new();
        coordinates
            .try_reserve_exact(lineages.len())
            .map_err(|_| ArrowConversionError::AllocationFailure)?;
        for (ordinal, (lineage, observation_digest)) in
            lineages.iter().zip(payload_digests).enumerate()
        {
            if let Some(control) = control.as_deref_mut() {
                control.checkpoint_row(ordinal)?;
            }
            let RowLineage::Extraction(lineage) = lineage else {
                return Err(ArrowConversionError::ProviderCaptureRequired);
            };
            let capture = lineage
                .provider_capture
                .as_ref()
                .ok_or(ArrowConversionError::ProviderCaptureRequired)?;
            let observation_digest = observation_digest
                .ok_or(ArrowConversionError::InvalidSchema)?
                .try_into()
                .map_err(|_| ArrowConversionError::InvalidSchema)?;
            coordinates.push(ProviderCaptureRowCoordinate {
                binding_digest: capture.binding_digest,
                capture_observation_digest: capture.capture_observation_digest,
                canonical_row_ordinal: capture.canonical_row_ordinal,
                canonical_row_digest: lineage.record_evidence.content_digest(),
                observation_digest: EvidenceDigest::new(
                    DigestAlgorithm::Sha256,
                    observation_digest,
                ),
                native_semantic_digest: capture.native_semantic_digest,
                capture_page_ordinal: capture.capture_page_ordinal,
                segment_ordinal: capture.segment_ordinal,
                physical_frame_ordinal: capture.physical_frame_ordinal,
                page_body_digest: capture.page_body_digest,
            });
        }
        Ok(coordinates)
    }

    /// Hashes the ordered canonical row identities independently of Parquet layout.
    pub fn lineage_digest(&self) -> Result<EvidenceDigest, ArrowConversionError> {
        let mut lineage = ResearchLineageDigestAccumulator::new();
        lineage.update(self, None)?;
        Ok(lineage.finish())
    }

    fn decode_payloads(
        &self,
        mut control: Option<&mut ArrowOperationControl<'_>>,
    ) -> Result<Vec<ResearchObservation>, ArrowConversionError> {
        let payloads = self
            .batch
            .column_by_name("payload_json")
            .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
            .ok_or(ArrowConversionError::InvalidSchema)?;
        let mut decoded = Vec::new();
        decoded
            .try_reserve_exact(payloads.len())
            .map_err(|_| ArrowConversionError::AllocationFailure)?;
        for (ordinal, payload) in payloads.iter().enumerate() {
            if let Some(control) = control.as_deref_mut() {
                control.checkpoint_row(ordinal)?;
            }
            let payload = payload.ok_or(ArrowConversionError::InvalidSchema)?;
            decoded.push(decode_json(payload, control.as_deref_mut())?);
        }
        Ok(decoded)
    }

    fn decode_request_digests(
        &self,
        mut control: Option<&mut ArrowOperationControl<'_>>,
    ) -> Result<Vec<[u8; 32]>, ArrowConversionError> {
        let digests = self
            .batch
            .column_by_name("request_sha256")
            .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
            .ok_or(ArrowConversionError::InvalidSchema)?;
        let mut decoded = Vec::new();
        decoded
            .try_reserve_exact(digests.len())
            .map_err(|_| ArrowConversionError::AllocationFailure)?;
        for (ordinal, digest) in digests.iter().enumerate() {
            if let Some(control) = control.as_deref_mut() {
                control.checkpoint_row(ordinal)?;
            }
            decoded.push(
                digest
                    .ok_or(ArrowConversionError::InvalidSchema)?
                    .try_into()
                    .map_err(|_| ArrowConversionError::InvalidSchema)?,
            );
        }
        Ok(decoded)
    }

    fn decode_row_lineages(
        &self,
        mut control: Option<&mut ArrowOperationControl<'_>>,
    ) -> Result<Vec<RowLineage>, ArrowConversionError> {
        let lineages = self
            .batch
            .column_by_name("extraction_lineage_json")
            .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
            .ok_or(ArrowConversionError::InvalidSchema)?;
        let mut decoded = Vec::new();
        decoded
            .try_reserve_exact(lineages.len())
            .map_err(|_| ArrowConversionError::AllocationFailure)?;
        for (ordinal, lineage) in lineages.iter().enumerate() {
            if let Some(control) = control.as_deref_mut() {
                control.checkpoint_row(ordinal)?;
            }
            let lineage = lineage.ok_or(ArrowConversionError::InvalidSchema)?;
            decoded.push(decode_json(lineage, control.as_deref_mut())?);
        }
        Ok(decoded)
    }

    fn decode_admission(
        &self,
        mut control: Option<&mut ArrowOperationControl<'_>>,
    ) -> Result<(usize, usize), ArrowConversionError> {
        let payloads = self
            .batch
            .column_by_name("payload_json")
            .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
            .ok_or(ArrowConversionError::InvalidSchema)?;
        let lineages = self
            .batch
            .column_by_name("extraction_lineage_json")
            .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
            .ok_or(ArrowConversionError::InvalidSchema)?;
        let mut payload_bytes = 0_usize;
        for (ordinal, payload) in payloads.iter().enumerate() {
            if let Some(control) = control.as_deref_mut() {
                control.checkpoint_row(ordinal)?;
            }
            payload_bytes = payload_bytes
                .checked_add(payload.ok_or(ArrowConversionError::InvalidSchema)?.len())
                .ok_or(ArrowConversionError::RetainedSizeOverflow)?;
        }
        let mut lineage_bytes = 0_usize;
        for (ordinal, lineage) in lineages.iter().enumerate() {
            if let Some(control) = control.as_deref_mut() {
                control.checkpoint_row(ordinal)?;
            }
            lineage_bytes = lineage_bytes
                .checked_add(lineage.ok_or(ArrowConversionError::InvalidSchema)?.len())
                .ok_or(ArrowConversionError::RetainedSizeOverflow)?;
        }
        let row_count = self.batch.num_rows();
        let observation_bytes = row_count
            .checked_mul(size_of::<ResearchObservation>())
            .and_then(|bytes| bytes.checked_add(payload_bytes.checked_mul(2)?))
            .ok_or(ArrowConversionError::RetainedSizeOverflow)?;
        let lineage_retained = row_count
            .checked_mul(size_of::<RowLineage>())
            .and_then(|bytes| bytes.checked_add(lineage_bytes.checked_mul(2)?))
            .ok_or(ArrowConversionError::RetainedSizeOverflow)?;
        let request_digest_retained = row_count
            .checked_mul(size_of::<[u8; 32]>())
            .ok_or(ArrowConversionError::RetainedSizeOverflow)?;
        let projection_slots = projection_vector_slot_bytes(row_count)?;
        let projection_dynamic = projection_dynamic_bytes(&self.batch, control.as_deref_mut())?
            .checked_mul(2)
            .ok_or(ArrowConversionError::RetainedSizeOverflow)?;
        let rebuilt_arrow = self
            .batch
            .get_array_memory_size()
            .checked_mul(2)
            .ok_or(ArrowConversionError::RetainedSizeOverflow)?;
        // Projection validation retains decoded observations, decoded and reserialized lineage,
        // request digests, every row-oriented projection vector, and the Arrow arrays already
        // converted from earlier vectors at the same time. The doubled dynamic/Arrow charges
        // conservatively admit allocator capacity slack and the current-column conversion.
        let working_bytes = self
            .batch
            .num_columns()
            .checked_mul(size_of::<ArrayRef>() + (2 * size_of::<usize>()))
            .and_then(|bytes| bytes.checked_add(64 * 1024))
            .and_then(|bytes| bytes.checked_add(observation_bytes))
            .and_then(|bytes| bytes.checked_add(lineage_retained))
            .and_then(|bytes| bytes.checked_add(request_digest_retained))
            .and_then(|bytes| bytes.checked_add(projection_slots))
            .and_then(|bytes| bytes.checked_add(projection_dynamic))
            .and_then(|bytes| bytes.checked_add(rebuilt_arrow))
            .ok_or(ArrowConversionError::RetainedSizeOverflow)?;
        Ok((working_bytes, observation_bytes))
    }
}

fn projection_dynamic_bytes(
    batch: &RecordBatch,
    mut control: Option<&mut ArrowOperationControl<'_>>,
) -> Result<usize, ArrowConversionError> {
    let mut total = 0_usize;
    for column in batch.columns() {
        if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
            for (ordinal, value) in values.iter().enumerate() {
                if let Some(control) = control.as_deref_mut() {
                    control.checkpoint_row(ordinal)?;
                }
                total = total
                    .checked_add(value.map_or(0, str::len))
                    .ok_or(ArrowConversionError::RetainedSizeOverflow)?;
            }
            continue;
        }
        if let Some(values) = column.as_any().downcast_ref::<BinaryArray>() {
            for (ordinal, value) in values.iter().enumerate() {
                if let Some(control) = control.as_deref_mut() {
                    control.checkpoint_row(ordinal)?;
                }
                total = total
                    .checked_add(value.map_or(0, <[u8]>::len))
                    .ok_or(ArrowConversionError::RetainedSizeOverflow)?;
            }
        }
    }
    Ok(total)
}

fn projection_vector_slot_bytes(row_count: usize) -> Result<usize, ArrowConversionError> {
    let string = size_of::<String>();
    let optional_string = size_of::<Option<String>>();
    let borrowed_string = size_of::<&'static str>();
    let optional_i64 = size_of::<Option<i64>>();
    let optional_i32 = size_of::<Option<i32>>();
    let optional_u16 = size_of::<Option<u16>>();
    let optional_i128 = size_of::<Option<i128>>();
    let optional_u8 = size_of::<Option<u8>>();
    let bytes = size_of::<Vec<u8>>();

    let slots_per_row = (3 * bytes) // serialized lineage, payload digest, and payload JSON
        .checked_add(7 * borrowed_string) // kind, availability/precision, quality, value state
        .and_then(|value| value.checked_add(2 * string)) // source and source identifier
        .and_then(|value| value.checked_add(15 * optional_string))
        .and_then(|value| value.checked_add(8 * optional_i64))
        .and_then(|value| value.checked_add(2 * size_of::<i64>()))
        .and_then(|value| value.checked_add(3 * optional_i32))
        .and_then(|value| value.checked_add(6 * optional_u16))
        .and_then(|value| value.checked_add(size_of::<u32>()))
        .and_then(|value| value.checked_add(optional_i128))
        .and_then(|value| value.checked_add(optional_u8))
        .ok_or(ArrowConversionError::RetainedSizeOverflow)?;
    row_count
        .checked_mul(slots_per_row)
        .ok_or(ArrowConversionError::RetainedSizeOverflow)
}

impl From<ResearchArrowBatch> for DatasetArrowBatch {
    fn from(value: ResearchArrowBatch) -> Self {
        Self {
            schema_ref: value.schema_ref,
            batch: value.batch,
        }
    }
}

fn provider_capture_lineage(
    binding: &PreparedProviderCaptureBinding,
    index: usize,
) -> Result<ProviderCaptureRowLineage, ArrowConversionError> {
    let row = binding
        .rows()
        .get(index)
        .ok_or(ArrowConversionError::ExtractionBindingMismatch)?;
    Ok(ProviderCaptureRowLineage {
        binding_digest: binding.binding_digest(),
        capture_observation_digest: binding.capture_observation_digest(),
        canonical_row_ordinal: row.canonical_row_ordinal(),
        native_semantic_digest: row.native_semantic_digest(),
        capture_page_ordinal: row.capture_page_ordinal(),
        segment_ordinal: row.segment_ordinal(),
        physical_frame_ordinal: row.physical_frame_ordinal(),
        page_body_digest: row.page_body_digest(),
    })
}

fn valid_provider_capture_lineage(lineage: &ProviderCaptureRowLineage) -> bool {
    lineage.canonical_row_ordinal < 100_000
        && lineage.capture_page_ordinal < 64
        && lineage.segment_ordinal < 64
        && lineage.physical_frame_ordinal < 64
        && [
            lineage.binding_digest,
            lineage.capture_observation_digest,
            lineage.native_semantic_digest,
            lineage.page_body_digest,
        ]
        .into_iter()
        .all(|digest| digest.algorithm() == DigestAlgorithm::Sha256 && digest.bytes() != [0; 32])
}

fn validate_row_lineage(
    lineage: &RowLineage,
    dataset: &SourceIdentifier,
    request_digest: [u8; 32],
    observation: &ResearchObservation,
    payload: &[u8],
    control: Option<&mut ArrowOperationControl<'_>>,
) -> Result<(), ArrowConversionError> {
    validate_research_payload_contract(observation, payload, control)?;
    let context = observation_context(observation);
    let provenance = context.provenance();
    let time = context.time();
    let matches = match lineage {
        RowLineage::Extraction(lineage) => {
            matches!(
                lineage.schema_version,
                LEGACY_EXTRACTION_LINEAGE_SCHEMA_VERSION
                    | EXTRACTION_LINEAGE_SCHEMA_VERSION
                    | CAPTURED_EXTRACTION_LINEAGE_SCHEMA_VERSION
            ) && lineage.source_id == *provenance.source_id()
                && (lineage.dataset == *dataset
                    || lineage.schema_version == CAPTURED_EXTRACTION_LINEAGE_SCHEMA_VERSION)
                && lineage.request_digest.algorithm() == DigestAlgorithm::Sha256
                && lineage.request_digest.bytes() == request_digest
                && lineage.record_schema.as_str() == RESEARCH_RECORD_SCHEMA
                && &lineage.effective_time == time.effective()
                && lineage.published_time.as_ref() == time.published()
                && availability_basis_matches(&lineage.availability, provenance.availability())
                && lineage.superseded_time.as_ref() == time.superseded()
                && match &lineage.provider_capture {
                    Some(capture) => {
                        lineage.schema_version == CAPTURED_EXTRACTION_LINEAGE_SCHEMA_VERSION
                            && valid_provider_capture_lineage(capture)
                    }
                    None => lineage.schema_version != CAPTURED_EXTRACTION_LINEAGE_SCHEMA_VERSION,
                }
                && match &lineage.revision_assignment {
                    Some(assignment) => {
                        matches!(
                            lineage.schema_version,
                            EXTRACTION_LINEAGE_SCHEMA_VERSION
                                | CAPTURED_EXTRACTION_LINEAGE_SCHEMA_VERSION
                        ) && assignment.assigned_revision == time.revision()
                            && CanonicalObservationPayload::try_from_observation(observation)
                                .is_ok_and(|semantic| {
                                    semantic.identity() == assignment.semantic_payload_identity
                                })
                    }
                    None => {
                        lineage.schema_version == LEGACY_EXTRACTION_LINEAGE_SCHEMA_VERSION
                            && payload_matches_exact_evidence(payload, &lineage.record_evidence)
                    }
                }
        }
        RowLineage::CanonicalObservation {
            schema_version,
            source_id,
            dataset: lineage_dataset,
            request_digest: lineage_request_digest,
        } => {
            *schema_version == RESEARCH_SCHEMA_VERSION
                && source_id == provenance.source_id()
                && lineage_dataset == dataset
                && lineage_request_digest.algorithm() == DigestAlgorithm::Sha256
                && lineage_request_digest.bytes() == request_digest
        }
    };
    if matches {
        Ok(())
    } else {
        Err(ArrowConversionError::ExtractionBindingMismatch)
    }
}

fn validate_research_payload_contract(
    observation: &ResearchObservation,
    payload: &[u8],
    control: Option<&mut ArrowOperationControl<'_>>,
) -> Result<(), ArrowConversionError> {
    let contract = research_payload_contract_for(observation);
    let envelope: ResearchObservationEnvelopeTag = decode_json(payload, control)?;
    if envelope.observation != contract.json_tag() {
        return Err(ArrowConversionError::PayloadContractMismatch);
    }
    let semantic_payload = CanonicalObservationPayload::try_from_observation(observation)
        .map_err(ArrowConversionError::PayloadContractEncoding)?;
    if semantic_payload_tag(semantic_payload.exact_bytes()) != Some(contract.semantic_tag()) {
        return Err(ArrowConversionError::PayloadContractMismatch);
    }
    Ok(())
}

fn semantic_payload_tag(payload: &[u8]) -> Option<u8> {
    const MAGIC: &[u8] = b"MSQPIT";
    const IDENTITY_SCHEMA_VERSION: u16 = 2;
    const DOMAIN: &[u8] = b"market-squawk/pit/payload";

    let version_start = MAGIC.len();
    let domain_length_start = version_start.checked_add(size_of::<u16>())?;
    let domain_start = domain_length_start.checked_add(size_of::<u64>())?;
    if payload.get(..version_start)? != MAGIC
        || u16::from_le_bytes(
            payload
                .get(version_start..domain_length_start)?
                .try_into()
                .ok()?,
        ) != IDENTITY_SCHEMA_VERSION
    {
        return None;
    }
    let domain_length = usize::try_from(u64::from_le_bytes(
        payload
            .get(domain_length_start..domain_start)?
            .try_into()
            .ok()?,
    ))
    .ok()?;
    let tag_index = domain_start.checked_add(domain_length)?;
    if payload.get(domain_start..tag_index)? != DOMAIN {
        return None;
    }
    payload.get(tag_index).copied()
}

#[derive(Debug)]
struct TemporalProjection {
    precision: Option<&'static str>,
    timestamp: Option<i64>,
    date: Option<i32>,
    period_scheme: Option<String>,
    period_year: Option<u16>,
    period_ordinal: Option<u16>,
    period_code: Option<String>,
}

fn temporal_projection(value: Option<&ResearchTemporalCoordinate>) -> TemporalProjection {
    match value {
        Some(value) => {
            let period = value.source_period_value();
            TemporalProjection {
                precision: Some(value.precision().as_str()),
                timestamp: value.exact_timestamp().map(Timestamp::unix_nanos),
                date: value
                    .calendar_date_value()
                    .map(|date| date.days_since_unix_epoch()),
                period_scheme: period.map(|period| period.scheme().as_str().to_owned()),
                period_year: period.map(|period| period.year()),
                period_ordinal: period.map(|period| period.ordinal().get()),
                period_code: period.map(|period| period.code().as_str().to_owned()),
            }
        }
        None => TemporalProjection {
            precision: None,
            timestamp: None,
            date: None,
            period_scheme: None,
            period_year: None,
            period_ordinal: None,
            period_code: None,
        },
    }
}

fn availability_projection(
    availability: &AvailabilityEvidence,
) -> (&'static str, Option<String>, Option<String>) {
    match availability {
        AvailabilityEvidence::Evidenced { evidence, .. } => {
            ("evidenced", Some(evidence.as_str().to_owned()), None)
        }
        AvailabilityEvidence::LocalFirstObserved { .. } => ("local_first_observed", None, None),
        AvailabilityEvidence::Inferred { method, .. } => {
            ("inferred", None, Some(method.as_str().to_owned()))
        }
        AvailabilityEvidence::Unknown => ("unknown", None, None),
    }
}

fn availability_basis_matches(
    source: &SourceAvailabilityEvidence,
    canonical: &AvailabilityEvidence,
) -> bool {
    match (source, canonical) {
        (
            SourceAvailabilityEvidence::Observed {
                available_at: source_time,
                evidence: source_evidence,
            },
            AvailabilityEvidence::Evidenced {
                available_at,
                evidence,
            },
        ) => source_time == available_at && source_evidence == evidence,
        (
            SourceAvailabilityEvidence::LocalFirstObserved {
                observed_at: source_time,
            },
            AvailabilityEvidence::LocalFirstObserved { observed_at },
        ) => source_time == observed_at,
        (
            SourceAvailabilityEvidence::Inferred {
                inferred_at: source_time,
                method: source_method,
            },
            AvailabilityEvidence::Inferred {
                inferred_at,
                method,
            },
        ) => source_time == inferred_at && source_method == method,
        (SourceAvailabilityEvidence::Unknown, AvailabilityEvidence::Unknown) => true,
        _ => false,
    }
}

/// Exact Arrow conversion failure.
#[derive(Debug, Error)]
pub enum ArrowConversionError {
    /// Caller cancellation, deadline, or trusted control stopped controlled Arrow work.
    #[error(transparent)]
    ObjectControl(#[from] ResearchObjectControlError),
    /// An ingest cannot publish an empty object.
    #[error("research Arrow batch must contain at least one observation")]
    EmptyBatch,
    /// Canonical provenance or time disagrees with exact extraction lineage.
    #[error("canonical observation does not match its extraction request and record lineage")]
    ExtractionBindingMismatch,
    /// Durable assignments were not aligned one-for-one with normalized extraction records.
    #[error("durable revision assignments do not match the extraction batch")]
    RevisionAssignmentMismatch,
    /// A paged provider extraction omitted its exact verified sealed-capture receipt.
    #[error("paged provider extraction requires a verified sealed-capture receipt")]
    ProviderCaptureRequired,
    /// Exact observed-revision evidence could not be constructed.
    #[error("observed revision authority rejected canonical evidence")]
    RevisionAuthority(market_squawk_sources::ObservedRevisionError),
    /// An observation could not be represented by its registered semantic payload encoder.
    #[error("canonical research payload encoding does not match the registered contract")]
    PayloadContractEncoding(#[source] market_squawk_sources::ObservedRevisionError),
    /// The JSON discriminator or PIT semantic tag disagreed with the closed payload contract.
    #[error("canonical research payload tag does not match the registered contract")]
    PayloadContractMismatch,
    /// Rebinding a retained canonical observation exposed invalid source state.
    #[error("canonical observation revision rebinding failed")]
    Research(#[from] market_squawk_domain::ResearchError),
    /// The request binding was not SHA-256.
    #[error("Arrow request binding must use SHA-256")]
    RequestDigestNotSha256,
    /// Schema metadata is missing or malformed.
    #[error("Arrow schema metadata is invalid")]
    InvalidSchemaMetadata,
    /// This reader cannot interpret the retained schema version.
    #[error("unsupported Arrow schema version {found}")]
    UnsupportedSchemaVersion { found: u16 },
    /// Fields, nullability, stable metadata, or mandatory values violate the registered schema.
    #[error("Arrow batch does not match its registered dataset schema")]
    InvalidSchema,
    /// A generic batch used a registered non-research schema where research rows were required.
    #[error("Arrow batch does not use the canonical research-observation schema")]
    UnexpectedDatasetSchema,
    /// A typed feature/label component violates its closed row-level contract.
    #[error("Arrow feature/label component row is invalid")]
    InvalidFeatureLabelRow,
    /// A typed canonical market-event row violates its retained publication evidence.
    #[error("Arrow market-event row does not match its exact provider publication evidence")]
    InvalidMarketEventRow,
    /// A typed canonical option row or batch header violates retained publication evidence.
    #[error("Arrow option-market batch does not match its exact provider publication evidence")]
    InvalidOptionMarketRow,
    /// Canonical payload and analytical projections disagree.
    #[error("Arrow analytical projection does not match its canonical payload")]
    ProjectionMismatch,
    /// A caller-selected retained-memory ceiling cannot admit decoding and projection validation.
    #[error("research Arrow decoding exceeds the retained-memory ceiling")]
    RetainedLimitExceeded,
    /// Fallible vector reservation failed before decoding.
    #[error("research Arrow decoding allocation reservation failed")]
    AllocationFailure,
    /// Checked retained-memory accounting overflowed.
    #[error("research Arrow retained-memory accounting overflowed")]
    RetainedSizeOverflow,
    /// A decimal scale cannot be represented in the explicit scale column.
    #[error("decimal scale is outside the supported exact range")]
    DecimalScale(#[from] std::num::TryFromIntError),
    /// Arrow rejected an array or record-batch invariant.
    #[error("Arrow conversion failed")]
    Arrow(#[from] ArrowError),
    /// Canonical JSON encoding or decoding failed.
    #[error("canonical observation serialization failed")]
    Json(#[from] JsonError),
    /// Dataset schema identity is unknown, malformed, or fingerprint-inconsistent.
    #[error("Arrow dataset schema identity is invalid")]
    DatasetSchema(#[from] DatasetSchemaError),
}

fn observation_context(observation: &ResearchObservation) -> &ResearchContext {
    match observation {
        ResearchObservation::Filing(value) => value.context(),
        ResearchObservation::Fundamental(value) => value.context(),
        ResearchObservation::Macro(value) => value.context(),
        ResearchObservation::MarketBar(value) => value.context(),
        ResearchObservation::FundNav(value) => value.context(),
        ResearchObservation::PortfolioPosition(value) => value.context(),
        ResearchObservation::Transaction(value) => value.context(),
        ResearchObservation::CorporateAction(value) => value.context(),
        ResearchObservation::UniverseMembership(value) => value.context(),
        ResearchObservation::AlternativeData(value) => value.context(),
    }
}

const fn observation_kind(observation: &ResearchObservation) -> &'static str {
    research_payload_contract_for(observation).json_tag()
}

struct AnalyticalValue<'a> {
    state: &'static str,
    decimal: Option<Decimal>,
    unit: Option<&'a str>,
    currency: Option<Currency>,
    missing_marker: Option<&'a str>,
    missing_reason: Option<&'a str>,
}

fn analytical_value(observation: &ResearchObservation) -> AnalyticalValue<'_> {
    match observation {
        ResearchObservation::Fundamental(value) => AnalyticalValue {
            state: "observed",
            decimal: Some(value.value()),
            unit: Some(value.unit().as_str()),
            currency: None,
            missing_marker: None,
            missing_reason: None,
        },
        ResearchObservation::Macro(value) => match value.value().missing_value() {
            Some(missing) => AnalyticalValue {
                state: "missing",
                decimal: None,
                unit: Some(value.unit().as_str()),
                currency: None,
                missing_marker: Some(missing.marker().as_str()),
                missing_reason: missing.reason().map(SourceIdentifier::as_str),
            },
            None => AnalyticalValue {
                state: "observed",
                decimal: value.value().observed_value(),
                unit: Some(value.unit().as_str()),
                currency: None,
                missing_marker: None,
                missing_reason: None,
            },
        },
        ResearchObservation::AlternativeData(value) => AnalyticalValue {
            state: "observed",
            decimal: Some(value.value()),
            unit: value.unit().map(SourceIdentifier::as_str),
            currency: None,
            missing_marker: None,
            missing_reason: None,
        },
        ResearchObservation::MarketBar(value) => AnalyticalValue {
            state: "observed",
            decimal: Some(value.close().amount()),
            unit: None,
            currency: Some(value.currency()),
            missing_marker: None,
            missing_reason: None,
        },
        ResearchObservation::FundNav(value) => match value.value() {
            market_squawk_domain::FundNavValue::Observed(money) => AnalyticalValue {
                state: "observed",
                decimal: Some(money.amount()),
                unit: Some("per_share"),
                currency: Some(money.currency()),
                missing_marker: None,
                missing_reason: None,
            },
            market_squawk_domain::FundNavValue::Missing(missing) => AnalyticalValue {
                state: "missing",
                decimal: None,
                unit: Some("per_share"),
                currency: Some(value.currency()),
                missing_marker: Some(fund_nav_missing_name(missing)),
                missing_reason: None,
            },
        },
        _ => AnalyticalValue {
            state: "not_applicable",
            decimal: None,
            unit: None,
            currency: None,
            missing_marker: None,
            missing_reason: None,
        },
    }
}

const fn fund_nav_missing_name(missing: market_squawk_domain::FundNavMissingState) -> &'static str {
    match missing {
        market_squawk_domain::FundNavMissingState::NotYetPublished => "not_yet_published",
        market_squawk_domain::FundNavMissingState::Unsupported => "unsupported",
        market_squawk_domain::FundNavMissingState::SourceMissing => "source_missing",
        market_squawk_domain::FundNavMissingState::Invalid => "invalid",
        market_squawk_domain::FundNavMissingState::Unavailable => "unavailable",
    }
}

const fn quality_name(quality: DataQuality) -> &'static str {
    match quality {
        DataQuality::DirectVerified => "direct_verified",
        DataQuality::DirectUnverified => "direct_unverified",
        DataQuality::OfficialDelayed => "official_delayed",
        DataQuality::Aggregated => "aggregated",
        DataQuality::Indicative => "indicative",
        DataQuality::Modeled => "modeled",
        DataQuality::Estimated => "estimated",
        DataQuality::Stale => "stale",
        DataQuality::Quarantined => "quarantined",
    }
}

fn is_currency(unit: &str) -> bool {
    unit.len() == 3 && unit.bytes().all(|byte| byte.is_ascii_uppercase())
}
