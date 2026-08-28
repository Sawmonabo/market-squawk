//! Provider-neutral option-market Arrow publication, restart validation, and whole-batch PIT reads.

use std::num::NonZeroU16;
use std::sync::Arc;

use arrow::array::{
    Array as _, ArrayRef, BinaryArray, FixedSizeBinaryArray, StringArray, TimestampNanosecondArray,
    UInt16Array, UInt32Array,
};
use arrow::record_batch::RecordBatch;
use market_squawk_domain::{
    CalendarDate, DigestAlgorithm, EvidenceDigest, InstrumentId, MetadataRevision, Money,
    OccOptionIdentity, OptionComponent, OptionComponentState, OptionContractTerms,
    OptionContractTermsInput, OptionExerciseStyle, OptionExpirationClass,
    OptionExpirationObservation, OptionExpirationObservationInput, OptionKind,
    OptionSettlementKind, OptionSnapshotObservation, OptionSnapshotObservationInput,
    OptionUnderlyingObservation, ProviderChannel, ProviderInstrumentId, ProviderProduct,
    QuantityLots, SourceId, SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_sources::{
    MAX_PROVIDER_OPTION_MARKET_BATCH_ROWS, OptionExpirationRange, OptionMarketBatchDisposition,
    OptionMarketBatchKind, OptionMarketCompleteness, OptionMarketCompletenessInput,
    OptionMarketCursorState, OptionMarketRequestFilter, OptionMarketRequestScope,
    OptionMarketRequestScopeInput, OptionStrikeRange, PROVIDER_OPTION_MARKET_SCHEMA_VERSION,
    SealedProviderOptionMarketBinding,
};
use rust_decimal::Decimal;
use serde::Deserialize;
use sha2::{Digest as _, Sha256};

use crate::arrow_convert::{ArrowConversionError, DatasetArrowBatch};
use crate::catalog::{
    PersistedProviderOptionMarketBindingEvidence, PersistedProviderOptionMarketBindingRow,
};
use crate::schema::{
    DatasetSchemaRef, OPTION_MARKET_SCHEMA_NAME, PROVIDER_PUBLICATION_DIGEST_KEY,
    PROVIDER_PUBLICATION_KIND_KEY, decode_hex, option_market_schema,
};
use crate::{DatasetId, DatasetManifestRef};

const HEADER_ROW: &str = "batch_header";
const SNAPSHOT_ROW: &str = "snapshot";
const EXPIRATION_ROW: &str = "expiration";
const OPTION_CONTENT_DOMAIN: &[u8] = b"market-squawk/provider-option-market/content/v1";
const OPTION_LINEAGE_DOMAIN: &[u8] = b"market-squawk/option-market-row-lineage/v1";
const OPTION_SELECTION_DOMAIN: &[u8] = b"market-squawk/option-market-pit-selection/v1";
const MAX_OPTION_RESTART_BYTES: usize = 192 * 1024 * 1024;

/// One nonempty registered Parquet batch containing a mandatory batch header and zero or more
/// canonical option rows.
#[derive(Clone, Debug)]
pub struct ProviderOptionMarketArrowBatch {
    dataset: DatasetArrowBatch,
    scope: OptionMarketRequestScope,
    completeness: OptionMarketCompleteness,
    snapshots: Box<[OptionSnapshotObservation]>,
    expirations: Box<[OptionExpirationObservation]>,
    publication_digest: EvidenceDigest,
    publication_kind: OptionMarketBatchKind,
}

impl ProviderOptionMarketArrowBatch {
    pub(crate) fn try_from_publication(
        binding: &SealedProviderOptionMarketBinding,
    ) -> Result<Self, ArrowConversionError> {
        binding
            .validate()
            .map_err(|_| ArrowConversionError::InvalidOptionMarketRow)?;
        let batch = binding.batch();
        let scope_json = serde_json::to_vec(batch.scope())?;
        let completeness_json = serde_json::to_vec(&batch.completeness())?;
        let header_json = compose_header_json(&scope_json, &completeness_json)?;
        let rows = match batch.kind() {
            OptionMarketBatchKind::Snapshots => batch
                .snapshots()
                .ok_or(ArrowConversionError::InvalidOptionMarketRow)?
                .iter()
                .map(serde_json::to_vec)
                .collect::<Result<Vec<_>, _>>()?,
            OptionMarketBatchKind::Expirations => batch
                .expirations()
                .ok_or(ArrowConversionError::InvalidOptionMarketRow)?
                .iter()
                .map(serde_json::to_vec)
                .collect::<Result<Vec<_>, _>>()?,
        };
        let evidence_rows = binding
            .row_frames()
            .iter()
            .enumerate()
            .map(|(ordinal, frame)| {
                Ok(ArrowOptionRowEvidence {
                    canonical_digest: batch
                        .canonical_row_digest(ordinal)
                        .ok_or(ArrowConversionError::InvalidOptionMarketRow)?,
                    native_digest: binding
                        .native_lineage()
                        .row_digest(ordinal)
                        .ok_or(ArrowConversionError::InvalidOptionMarketRow)?,
                    capture_page_ordinal: frame.capture_page_ordinal(),
                    physical_frame_ordinal: frame.physical_frame_ordinal(),
                    payload_digest: frame.page_body_digest(),
                    source_sequence: frame.source_sequence(),
                })
            })
            .collect::<Result<Vec<_>, ArrowConversionError>>()?;
        build_option_batch(
            batch.scope().clone(),
            batch.completeness(),
            batch.kind(),
            binding.evidence_digest().evidence(),
            header_json,
            rows,
            evidence_rows,
            Some(batch.content_identity().content_digest()),
        )
    }

    pub fn try_from_record_batch_with_publication_evidence(
        batch: RecordBatch,
        evidence: &PersistedProviderOptionMarketBindingEvidence,
        maximum_retained_bytes: usize,
    ) -> Result<Self, ArrowConversionError> {
        evidence
            .verify_integrity()
            .map_err(|_| ArrowConversionError::InvalidOptionMarketRow)?;
        if maximum_retained_bytes == 0 || maximum_retained_bytes > MAX_OPTION_RESTART_BYTES {
            return Err(ArrowConversionError::RetainedLimitExceeded);
        }
        let dataset = DatasetArrowBatch::try_from_record_batch(batch)?;
        if dataset.schema_ref().name() != OPTION_MARKET_SCHEMA_NAME {
            return Err(ArrowConversionError::UnexpectedDatasetSchema);
        }
        if dataset.record_batch().get_array_memory_size() > maximum_retained_bytes {
            return Err(ArrowConversionError::RetainedLimitExceeded);
        }
        let metadata = dataset.record_batch().schema().metadata().clone();
        let publication_digest = metadata
            .get(PROVIDER_PUBLICATION_DIGEST_KEY)
            .and_then(|value| decode_hex(value))
            .map(|bytes| EvidenceDigest::new(DigestAlgorithm::Sha256, bytes))
            .ok_or(ArrowConversionError::InvalidSchemaMetadata)?;
        let publication_kind = metadata
            .get(PROVIDER_PUBLICATION_KIND_KEY)
            .ok_or(ArrowConversionError::InvalidSchemaMetadata)?;
        if publication_digest != evidence.binding_digest()
            || publication_kind != evidence.publication_kind_name()
            || dataset.record_batch().num_rows()
                != evidence
                    .canonical_row_count()
                    .checked_add(1)
                    .ok_or(ArrowConversionError::RetainedSizeOverflow)?
        {
            return Err(ArrowConversionError::InvalidOptionMarketRow);
        }
        let columns = OptionColumns::try_from_batch(dataset.record_batch())?;
        let scope = decode_scope(evidence.scope_json())?;
        let completeness = decode_completeness(evidence.completeness_json())?;
        if serde_json::to_vec(scope.filter())?.as_slice() != evidence.filter_json() {
            return Err(ArrowConversionError::InvalidOptionMarketRow);
        }
        validate_header_columns(&columns, evidence, &scope, completeness)?;
        let expected_header =
            compose_header_json(evidence.scope_json(), evidence.completeness_json())?;
        if columns.payload_json.value(0) != expected_header {
            return Err(ArrowConversionError::InvalidOptionMarketRow);
        }
        let mut rows = Vec::new();
        rows.try_reserve_exact(evidence.canonical_row_count())
            .map_err(|_| ArrowConversionError::AllocationFailure)?;
        let mut row_evidence = Vec::new();
        row_evidence
            .try_reserve_exact(evidence.canonical_row_count())
            .map_err(|_| ArrowConversionError::AllocationFailure)?;
        for ordinal in 0..evidence.canonical_row_count() {
            let physical_row = ordinal + 1;
            let expected = evidence
                .rows()
                .get(ordinal)
                .ok_or(ArrowConversionError::InvalidOptionMarketRow)?;
            validate_option_columns(
                &columns,
                physical_row,
                ordinal,
                expected,
                evidence,
                &scope,
                completeness,
            )?;
            rows.push(columns.payload_json.value(physical_row).to_vec());
            row_evidence.push(ArrowOptionRowEvidence {
                canonical_digest: expected.canonical_row_digest(),
                native_digest: expected.native_semantic_digest(),
                capture_page_ordinal: expected.capture_page_ordinal(),
                physical_frame_ordinal: expected.physical_frame_ordinal(),
                payload_digest: expected.payload_digest(),
                source_sequence: expected.source_sequence(),
            });
        }
        build_option_batch(
            scope,
            completeness,
            evidence.publication_kind(),
            evidence.binding_digest(),
            expected_header,
            rows,
            row_evidence,
            Some(evidence.canonical_content_digest()),
        )
    }

    pub const fn dataset_batch(&self) -> &DatasetArrowBatch {
        &self.dataset
    }
    pub const fn schema_ref(&self) -> &DatasetSchemaRef {
        self.dataset.schema_ref()
    }
    pub const fn scope(&self) -> &OptionMarketRequestScope {
        &self.scope
    }
    pub const fn completeness(&self) -> OptionMarketCompleteness {
        self.completeness
    }
    pub fn snapshots(&self) -> Option<&[OptionSnapshotObservation]> {
        (self.publication_kind == OptionMarketBatchKind::Snapshots)
            .then_some(self.snapshots.as_ref())
    }
    pub fn expirations(&self) -> Option<&[OptionExpirationObservation]> {
        (self.publication_kind == OptionMarketBatchKind::Expirations)
            .then_some(self.expirations.as_ref())
    }
    pub const fn publication_digest(&self) -> EvidenceDigest {
        self.publication_digest
    }
    pub const fn publication_kind(&self) -> OptionMarketBatchKind {
        self.publication_kind
    }

    pub fn lineage_digest(&self) -> Result<EvidenceDigest, ArrowConversionError> {
        let columns = OptionColumns::try_from_batch(self.dataset.record_batch())?;
        let mut digest = Sha256::new();
        digest.update(OPTION_LINEAGE_DOMAIN);
        digest.update(self.publication_digest.bytes());
        digest.update(
            u64::try_from(self.dataset.record_batch().num_rows())
                .map_err(|_| ArrowConversionError::RetainedSizeOverflow)?
                .to_be_bytes(),
        );
        for row in 0..self.dataset.record_batch().num_rows() {
            digest.update(columns.scope_digests.value(row));
            digest.update(columns.completeness_digests.value(row));
            if row > 0 {
                digest.update(columns.canonical_digests.value(row));
                digest.update(columns.native_digests.value(row));
                digest.update(columns.raw_digests.value(row));
                digest.update(columns.capture_pages.value(row).to_be_bytes());
                digest.update(columns.physical_frames.value(row).to_be_bytes());
            }
        }
        Ok(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            digest.finalize().into(),
        ))
    }
}

/// Fixed bounded whole-batch option-market selection request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptionMarketPointInTimeRequest {
    dataset: DatasetId,
    underlying_instrument_id: InstrumentId,
    publication_kind: OptionMarketBatchKind,
    filter_digest: EvidenceDigest,
    knowledge_cutoff: Timestamp,
    maximum_canonical_rows: usize,
    exact_manifest: Option<DatasetManifestRef>,
}

impl OptionMarketPointInTimeRequest {
    pub fn try_latest(
        dataset: DatasetId,
        underlying_instrument_id: InstrumentId,
        publication_kind: OptionMarketBatchKind,
        filter: &OptionMarketRequestFilter,
        knowledge_cutoff: Timestamp,
        maximum_canonical_rows: usize,
    ) -> Result<Self, ArrowConversionError> {
        Self::try_new(
            dataset,
            underlying_instrument_id,
            publication_kind,
            filter,
            knowledge_cutoff,
            maximum_canonical_rows,
            None,
        )
    }

    pub fn try_exact(
        dataset: DatasetId,
        underlying_instrument_id: InstrumentId,
        publication_kind: OptionMarketBatchKind,
        filter: &OptionMarketRequestFilter,
        knowledge_cutoff: Timestamp,
        maximum_canonical_rows: usize,
        manifest: DatasetManifestRef,
    ) -> Result<Self, ArrowConversionError> {
        Self::try_new(
            dataset,
            underlying_instrument_id,
            publication_kind,
            filter,
            knowledge_cutoff,
            maximum_canonical_rows,
            Some(manifest),
        )
    }

    fn try_new(
        dataset: DatasetId,
        underlying_instrument_id: InstrumentId,
        publication_kind: OptionMarketBatchKind,
        filter: &OptionMarketRequestFilter,
        knowledge_cutoff: Timestamp,
        maximum_canonical_rows: usize,
        exact_manifest: Option<DatasetManifestRef>,
    ) -> Result<Self, ArrowConversionError> {
        if maximum_canonical_rows == 0
            || maximum_canonical_rows > MAX_PROVIDER_OPTION_MARKET_BATCH_ROWS
            || exact_manifest
                .as_ref()
                .is_some_and(|manifest| manifest.dataset_id() != &dataset)
        {
            return Err(ArrowConversionError::RetainedLimitExceeded);
        }
        Ok(Self {
            dataset,
            underlying_instrument_id,
            publication_kind,
            filter_digest: sha256_evidence(&serde_json::to_vec(filter)?),
            knowledge_cutoff,
            maximum_canonical_rows,
            exact_manifest,
        })
    }

    pub const fn dataset(&self) -> &DatasetId {
        &self.dataset
    }
    pub const fn underlying_instrument_id(&self) -> InstrumentId {
        self.underlying_instrument_id
    }
    pub const fn publication_kind(&self) -> OptionMarketBatchKind {
        self.publication_kind
    }
    pub const fn filter_digest(&self) -> EvidenceDigest {
        self.filter_digest
    }
    pub const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }
    pub const fn maximum_canonical_rows(&self) -> usize {
        self.maximum_canonical_rows
    }
    pub const fn exact_manifest(&self) -> Option<&DatasetManifestRef> {
        self.exact_manifest.as_ref()
    }
}

/// One exact manifest and coherent option batch selected at a fixed knowledge cutoff.
#[derive(Clone, Debug)]
pub struct OptionMarketPointInTimeSelection {
    manifest: DatasetManifestRef,
    batch: ProviderOptionMarketArrowBatch,
    selection_digest: EvidenceDigest,
}

impl OptionMarketPointInTimeSelection {
    pub(crate) fn try_new(
        request: &OptionMarketPointInTimeRequest,
        manifest: DatasetManifestRef,
        batch: ProviderOptionMarketArrowBatch,
    ) -> Result<Self, ArrowConversionError> {
        if batch.scope().underlying_instrument_id() != request.underlying_instrument_id
            || batch.publication_kind() != request.publication_kind
            || batch.scope().available_at() > request.knowledge_cutoff
            || batch.scope().ingested_at() > request.knowledge_cutoff
            || batch.completeness().returned_records()
                > u64::try_from(request.maximum_canonical_rows)
                    .map_err(|_| ArrowConversionError::RetainedSizeOverflow)?
        {
            return Err(ArrowConversionError::InvalidOptionMarketRow);
        }
        let mut digest = Sha256::new();
        digest.update(OPTION_SELECTION_DOMAIN);
        digest.update(request.dataset.as_str().as_bytes());
        digest.update(request.underlying_instrument_id.as_uuid().as_bytes());
        digest.update(option_kind_tag(request.publication_kind));
        digest.update(request.filter_digest.bytes());
        digest.update(request.knowledge_cutoff.unix_nanos().to_be_bytes());
        digest.update(manifest.content_hash().bytes());
        digest.update(batch.publication_digest().bytes());
        Ok(Self {
            manifest,
            batch,
            selection_digest: EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                digest.finalize().into(),
            ),
        })
    }

    pub const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }
    pub const fn batch(&self) -> &ProviderOptionMarketArrowBatch {
        &self.batch
    }
    pub const fn selection_digest(&self) -> EvidenceDigest {
        self.selection_digest
    }
}

#[derive(Clone, Copy)]
struct ArrowOptionRowEvidence {
    canonical_digest: EvidenceDigest,
    native_digest: EvidenceDigest,
    capture_page_ordinal: u16,
    physical_frame_ordinal: u32,
    payload_digest: EvidenceDigest,
    source_sequence: Option<u64>,
}

#[allow(clippy::too_many_arguments)]
fn build_option_batch(
    scope: OptionMarketRequestScope,
    completeness: OptionMarketCompleteness,
    kind: OptionMarketBatchKind,
    publication_digest: EvidenceDigest,
    header_json: Vec<u8>,
    row_json: Vec<Vec<u8>>,
    row_evidence: Vec<ArrowOptionRowEvidence>,
    expected_content_digest: Option<EvidenceDigest>,
) -> Result<ProviderOptionMarketArrowBatch, ArrowConversionError> {
    if row_json.len() != row_evidence.len()
        || row_json.len() > MAX_PROVIDER_OPTION_MARKET_BATCH_ROWS
        || u64::try_from(row_json.len()).ok() != Some(completeness.returned_records())
    {
        return Err(ArrowConversionError::InvalidOptionMarketRow);
    }
    let scope_json = serde_json::to_vec(&scope)?;
    let completeness_json = serde_json::to_vec(&completeness)?;
    if header_json != compose_header_json(&scope_json, &completeness_json)? {
        return Err(ArrowConversionError::InvalidOptionMarketRow);
    }
    let content_digest = option_content_digest(kind, &header_json, &row_json)?;
    if expected_content_digest.is_some_and(|expected| expected != content_digest) {
        return Err(ArrowConversionError::InvalidOptionMarketRow);
    }
    let count = row_json
        .len()
        .checked_add(1)
        .ok_or(ArrowConversionError::RetainedSizeOverflow)?;
    let scope_digest = sha256_evidence(&scope_json);
    let completeness_digest = sha256_evidence(&completeness_json);
    let publication_kind = option_publication_kind(kind);
    let row_kind = match kind {
        OptionMarketBatchKind::Snapshots => SNAPSHOT_ROW,
        OptionMarketBatchKind::Expirations => EXPIRATION_ROW,
    };
    let mut payloads = Vec::with_capacity(count);
    payloads.push(header_json);
    payloads.extend(row_json.iter().cloned());
    for (ordinal, (payload, evidence)) in row_json.iter().zip(&row_evidence).enumerate() {
        if sha256_evidence(payload) != evidence.canonical_digest || u32::try_from(ordinal).is_err()
        {
            return Err(ArrowConversionError::InvalidOptionMarketRow);
        }
    }
    let row_ordinals = std::iter::once(None)
        .chain((0..row_json.len()).map(|ordinal| u32::try_from(ordinal).ok()))
        .collect::<Vec<_>>();
    let canonical_digests = std::iter::once(None)
        .chain(
            row_evidence
                .iter()
                .map(|row| Some(row.canonical_digest.bytes().to_vec())),
        )
        .collect::<Vec<_>>();
    let native_digests = std::iter::once(None)
        .chain(
            row_evidence
                .iter()
                .map(|row| Some(row.native_digest.bytes().to_vec())),
        )
        .collect::<Vec<_>>();
    let capture_pages = std::iter::once(None)
        .chain(
            row_evidence
                .iter()
                .map(|row| Some(row.capture_page_ordinal)),
        )
        .collect::<Vec<_>>();
    let physical_frames = std::iter::once(None)
        .chain(
            row_evidence
                .iter()
                .map(|row| Some(row.physical_frame_ordinal)),
        )
        .collect::<Vec<_>>();
    let raw_digests = std::iter::once(None)
        .chain(
            row_evidence
                .iter()
                .map(|row| Some(row.payload_digest.bytes().to_vec())),
        )
        .collect::<Vec<_>>();
    let source_sequences = std::iter::once(None)
        .chain(row_evidence.iter().map(|row| {
            row.source_sequence
                .map(|value| value.to_be_bytes().to_vec())
        }))
        .collect::<Vec<_>>();
    let schema = option_market_schema(scope.dataset(), publication_digest, publication_kind)?;
    let fields: Vec<ArrayRef> = vec![
        Arc::new(UInt16Array::from_value(
            PROVIDER_OPTION_MARKET_SCHEMA_VERSION,
            count,
        )),
        Arc::new(StringArray::from_iter_values(
            std::iter::once(HEADER_ROW).chain(std::iter::repeat_n(row_kind, row_json.len())),
        )),
        Arc::new(UInt32Array::from(row_ordinals)),
        Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
            scope.source_id().as_str(),
            count,
        ))),
        Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
            option_kind_name(kind),
            count,
        ))),
        Arc::new(FixedSizeBinaryArray::try_from_iter(std::iter::repeat_n(
            scope
                .underlying_instrument_id()
                .as_uuid()
                .as_bytes()
                .as_slice(),
            count,
        ))?),
        Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
            scope.provider_instrument_id().as_str(),
            count,
        ))),
        Arc::new(
            TimestampNanosecondArray::from_value(scope.available_at().unix_nanos(), count)
                .with_timezone_utc(),
        ),
        Arc::new(
            TimestampNanosecondArray::from_value(scope.received_at().unix_nanos(), count)
                .with_timezone_utc(),
        ),
        Arc::new(
            TimestampNanosecondArray::from_value(scope.ingested_at().unix_nanos(), count)
                .with_timezone_utc(),
        ),
        Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
            disposition_name(completeness.disposition()),
            count,
        ))),
        Arc::new(FixedSizeBinaryArray::try_from_iter(std::iter::repeat_n(
            scope_digest.bytes().as_slice(),
            count,
        ))?),
        Arc::new(FixedSizeBinaryArray::try_from_iter(std::iter::repeat_n(
            completeness_digest.bytes().as_slice(),
            count,
        ))?),
        Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            canonical_digests.iter().map(|value| value.as_deref()),
            32,
        )?),
        Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            native_digests.iter().map(|value| value.as_deref()),
            32,
        )?),
        Arc::new(UInt16Array::from(capture_pages)),
        Arc::new(UInt32Array::from(physical_frames)),
        Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            raw_digests.iter().map(|value| value.as_deref()),
            32,
        )?),
        Arc::new(BinaryArray::from_iter(
            source_sequences.iter().map(|value| value.as_deref()),
        )),
        Arc::new(BinaryArray::from_iter_values(
            payloads.iter().map(Vec::as_slice),
        )),
    ];
    let record_batch = RecordBatch::try_new(schema, fields)?;
    let dataset = DatasetArrowBatch::try_from_record_batch(record_batch)?;
    let (snapshots, expirations): (
        Box<[OptionSnapshotObservation]>,
        Box<[OptionExpirationObservation]>,
    ) = match kind {
        OptionMarketBatchKind::Snapshots => (
            row_json
                .iter()
                .map(|row| decode_snapshot(row))
                .collect::<Result<Vec<_>, _>>()?
                .into_boxed_slice(),
            Box::new([]),
        ),
        OptionMarketBatchKind::Expirations => (
            Box::new([]),
            row_json
                .iter()
                .map(|row| decode_expiration(row))
                .collect::<Result<Vec<_>, _>>()?
                .into_boxed_slice(),
        ),
    };
    Ok(ProviderOptionMarketArrowBatch {
        dataset,
        scope,
        completeness,
        snapshots,
        expirations,
        publication_digest,
        publication_kind: kind,
    })
}

struct OptionColumns<'a> {
    versions: &'a UInt16Array,
    row_kinds: &'a StringArray,
    ordinals: &'a UInt32Array,
    source_ids: &'a StringArray,
    batch_kinds: &'a StringArray,
    underlyings: &'a FixedSizeBinaryArray,
    provider_ids: &'a StringArray,
    available: &'a TimestampNanosecondArray,
    received: &'a TimestampNanosecondArray,
    ingested: &'a TimestampNanosecondArray,
    dispositions: &'a StringArray,
    scope_digests: &'a FixedSizeBinaryArray,
    completeness_digests: &'a FixedSizeBinaryArray,
    canonical_digests: &'a FixedSizeBinaryArray,
    native_digests: &'a FixedSizeBinaryArray,
    capture_pages: &'a UInt16Array,
    physical_frames: &'a UInt32Array,
    raw_digests: &'a FixedSizeBinaryArray,
    source_sequences: &'a BinaryArray,
    payload_json: &'a BinaryArray,
}

impl<'a> OptionColumns<'a> {
    fn try_from_batch(batch: &'a RecordBatch) -> Result<Self, ArrowConversionError> {
        macro_rules! column {
            ($name:literal, $ty:ty) => {
                batch
                    .column_by_name($name)
                    .and_then(|column| column.as_any().downcast_ref::<$ty>())
                    .ok_or(ArrowConversionError::InvalidOptionMarketRow)?
            };
        }
        Ok(Self {
            versions: column!("schema_version", UInt16Array),
            row_kinds: column!("row_kind", StringArray),
            ordinals: column!("canonical_row_ordinal", UInt32Array),
            source_ids: column!("source_id", StringArray),
            batch_kinds: column!("batch_kind", StringArray),
            underlyings: column!("underlying_instrument_id", FixedSizeBinaryArray),
            provider_ids: column!("provider_instrument_id", StringArray),
            available: column!("available_at", TimestampNanosecondArray),
            received: column!("received_at", TimestampNanosecondArray),
            ingested: column!("ingested_at", TimestampNanosecondArray),
            dispositions: column!("disposition", StringArray),
            scope_digests: column!("scope_sha256", FixedSizeBinaryArray),
            completeness_digests: column!("completeness_sha256", FixedSizeBinaryArray),
            canonical_digests: column!("canonical_row_sha256", FixedSizeBinaryArray),
            native_digests: column!("native_semantic_sha256", FixedSizeBinaryArray),
            capture_pages: column!("capture_page_ordinal", UInt16Array),
            physical_frames: column!("physical_frame_ordinal", UInt32Array),
            raw_digests: column!("raw_payload_sha256", FixedSizeBinaryArray),
            source_sequences: column!("source_sequence_be", BinaryArray),
            payload_json: column!("payload_json", BinaryArray),
        })
    }
}

fn validate_header_columns(
    columns: &OptionColumns<'_>,
    evidence: &PersistedProviderOptionMarketBindingEvidence,
    scope: &OptionMarketRequestScope,
    completeness: OptionMarketCompleteness,
) -> Result<(), ArrowConversionError> {
    validate_common_columns(columns, 0, evidence, scope, completeness)?;
    if columns.row_kinds.value(0) != HEADER_ROW
        || !columns.ordinals.is_null(0)
        || !columns.canonical_digests.is_null(0)
        || !columns.native_digests.is_null(0)
        || !columns.capture_pages.is_null(0)
        || !columns.physical_frames.is_null(0)
        || !columns.raw_digests.is_null(0)
        || !columns.source_sequences.is_null(0)
    {
        return Err(ArrowConversionError::InvalidOptionMarketRow);
    }
    Ok(())
}

fn validate_option_columns(
    columns: &OptionColumns<'_>,
    physical_row: usize,
    ordinal: usize,
    expected: &PersistedProviderOptionMarketBindingRow,
    evidence: &PersistedProviderOptionMarketBindingEvidence,
    scope: &OptionMarketRequestScope,
    completeness: OptionMarketCompleteness,
) -> Result<(), ArrowConversionError> {
    validate_common_columns(columns, physical_row, evidence, scope, completeness)?;
    let expected_kind = match evidence.publication_kind() {
        OptionMarketBatchKind::Snapshots => SNAPSHOT_ROW,
        OptionMarketBatchKind::Expirations => EXPIRATION_ROW,
    };
    let expected_sequence = expected.source_sequence().map(u64::to_be_bytes);
    if columns.row_kinds.value(physical_row) != expected_kind
        || columns.ordinals.value(physical_row)
            != u32::try_from(ordinal).map_err(|_| ArrowConversionError::InvalidOptionMarketRow)?
        || columns.canonical_digests.value(physical_row) != expected.canonical_row_digest().bytes()
        || columns.native_digests.value(physical_row) != expected.native_semantic_digest().bytes()
        || columns.capture_pages.value(physical_row) != expected.capture_page_ordinal()
        || columns.physical_frames.value(physical_row) != expected.physical_frame_ordinal()
        || columns.raw_digests.value(physical_row) != expected.payload_digest().bytes()
        || columns.source_sequences.is_null(physical_row) != expected_sequence.is_none()
        || expected_sequence.is_some_and(|sequence| {
            columns.source_sequences.value(physical_row) != sequence.as_slice()
        })
        || sha256_evidence(columns.payload_json.value(physical_row))
            != expected.canonical_row_digest()
    {
        return Err(ArrowConversionError::InvalidOptionMarketRow);
    }
    Ok(())
}

fn validate_common_columns(
    columns: &OptionColumns<'_>,
    row: usize,
    evidence: &PersistedProviderOptionMarketBindingEvidence,
    scope: &OptionMarketRequestScope,
    completeness: OptionMarketCompleteness,
) -> Result<(), ArrowConversionError> {
    let (available, received, ingested) = evidence.knowledge_clocks();
    if scope.source_id() != evidence.capture().source_id()
        || scope.underlying_instrument_id() != evidence.underlying_instrument_id()
        || scope.available_at() != available
        || scope.received_at() != received
        || scope.ingested_at() != ingested
        || completeness.disposition() != evidence.disposition()
        || columns.versions.value(row) != PROVIDER_OPTION_MARKET_SCHEMA_VERSION
        || columns.source_ids.value(row) != scope.source_id().as_str()
        || columns.batch_kinds.value(row) != option_kind_name(evidence.publication_kind())
        || columns.underlyings.value(row) != scope.underlying_instrument_id().as_uuid().as_bytes()
        || columns.provider_ids.value(row) != scope.provider_instrument_id().as_str()
        || columns.available.value(row) != available.unix_nanos()
        || columns.received.value(row) != received.unix_nanos()
        || columns.ingested.value(row) != ingested.unix_nanos()
        || columns.dispositions.value(row) != disposition_name(evidence.disposition())
        || columns.scope_digests.value(row) != evidence.scope_digest().bytes()
        || columns.completeness_digests.value(row) != evidence.completeness_digest().bytes()
    {
        return Err(ArrowConversionError::InvalidOptionMarketRow);
    }
    Ok(())
}

fn option_content_digest(
    kind: OptionMarketBatchKind,
    header_json: &[u8],
    rows: &[Vec<u8>],
) -> Result<EvidenceDigest, ArrowConversionError> {
    let schema_fingerprint = provider_option_schema_fingerprint();
    let mut digest = Sha256::new();
    hash_field(&mut digest, OPTION_CONTENT_DOMAIN)?;
    hash_evidence(&mut digest, schema_fingerprint);
    hash_field(&mut digest, option_kind_tag(kind))?;
    hash_field(&mut digest, header_json)?;
    hash_length(&mut digest, rows.len())?;
    for row in rows {
        hash_field(&mut digest, row)?;
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn provider_option_schema_fingerprint() -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/provider-option-market/schema/v1");
    digest.update(PROVIDER_OPTION_MARKET_SCHEMA_VERSION.to_be_bytes());
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

fn compose_header_json(scope: &[u8], completeness: &[u8]) -> Result<Vec<u8>, ArrowConversionError> {
    let capacity = scope
        .len()
        .checked_add(completeness.len())
        .and_then(|value| value.checked_add(3))
        .ok_or(ArrowConversionError::RetainedSizeOverflow)?;
    let mut header = Vec::new();
    header
        .try_reserve_exact(capacity)
        .map_err(|_| ArrowConversionError::AllocationFailure)?;
    header.push(b'[');
    header.extend_from_slice(scope);
    header.push(b',');
    header.extend_from_slice(completeness);
    header.push(b']');
    Ok(header)
}

const fn option_publication_kind(kind: OptionMarketBatchKind) -> &'static str {
    match kind {
        OptionMarketBatchKind::Snapshots => "option_snapshots",
        OptionMarketBatchKind::Expirations => "option_expirations",
    }
}

const fn option_kind_name(kind: OptionMarketBatchKind) -> &'static str {
    match kind {
        OptionMarketBatchKind::Snapshots => "snapshots",
        OptionMarketBatchKind::Expirations => "expirations",
    }
}

const fn option_kind_tag(kind: OptionMarketBatchKind) -> &'static [u8] {
    match kind {
        OptionMarketBatchKind::Snapshots => b"snapshots",
        OptionMarketBatchKind::Expirations => b"expirations",
    }
}

const fn disposition_name(disposition: OptionMarketBatchDisposition) -> &'static str {
    match disposition {
        OptionMarketBatchDisposition::Complete => "complete",
        OptionMarketBatchDisposition::Unavailable => "unavailable",
    }
}

fn sha256_evidence(bytes: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(bytes).into())
}

fn hash_evidence(digest: &mut Sha256, evidence: EvidenceDigest) {
    digest.update(match evidence.algorithm() {
        DigestAlgorithm::Sha256 => [1],
        DigestAlgorithm::Blake3 => [2],
    });
    digest.update(evidence.bytes());
}

fn hash_field(digest: &mut Sha256, value: &[u8]) -> Result<(), ArrowConversionError> {
    hash_length(digest, value.len())?;
    digest.update(value);
    Ok(())
}

fn hash_length(digest: &mut Sha256, value: usize) -> Result<(), ArrowConversionError> {
    digest.update(
        u64::try_from(value)
            .map_err(|_| ArrowConversionError::RetainedSizeOverflow)?
            .to_be_bytes(),
    );
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopeWire {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    dataset: SourceIdentifier,
    provider_product: ProviderProduct,
    provider_channel: ProviderChannel,
    venue_id: Option<VenueId>,
    underlying_instrument_id: InstrumentId,
    underlying_definition_revision: EvidenceDigest,
    provider_instrument_id: ProviderInstrumentId,
    request_identity: EvidenceDigest,
    observation_identity: EvidenceDigest,
    entitlement_evidence: EvidenceDigest,
    capability_evidence: EvidenceDigest,
    available_at: Timestamp,
    received_at: Timestamp,
    ingested_at: Timestamp,
    filter: FilterWire,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FilterWire {
    expiration_range: Option<ExpirationRangeWire>,
    strike_range: Option<StrikeRangeWire>,
    kind: Option<OptionKind>,
    contracts: Box<[ProviderInstrumentId]>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExpirationRangeWire {
    start: CalendarDate,
    end: CalendarDate,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrikeRangeWire {
    minimum: Money,
    maximum: Money,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CompletenessWire {
    expected_records: Option<u64>,
    returned_records: u64,
    missing_records: u64,
    unexpected_records: u64,
    provider_reported_records: Option<u64>,
    page_count: NonZeroU16,
    cursor: CursorWire,
    disposition: DispositionWire,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CursorWire {
    NotApplicable,
    Exhausted,
    Incomplete,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DispositionWire {
    Complete,
    Unavailable,
}

fn decode_scope(payload: &[u8]) -> Result<OptionMarketRequestScope, ArrowConversionError> {
    let wire: ScopeWire = serde_json::from_slice(payload)?;
    let expiration_range = wire
        .filter
        .expiration_range
        .map(|range| OptionExpirationRange::try_new(range.start, range.end))
        .transpose()
        .map_err(|_| ArrowConversionError::InvalidOptionMarketRow)?;
    let strike_range = wire
        .filter
        .strike_range
        .map(|range| OptionStrikeRange::try_new(range.minimum, range.maximum))
        .transpose()
        .map_err(|_| ArrowConversionError::InvalidOptionMarketRow)?;
    let filter = OptionMarketRequestFilter::try_new(
        expiration_range,
        strike_range,
        wire.filter.kind,
        wire.filter.contracts.into_vec(),
    )
    .map_err(|_| ArrowConversionError::InvalidOptionMarketRow)?;
    OptionMarketRequestScope::try_new(OptionMarketRequestScopeInput {
        source_id: wire.source_id,
        metadata_revision: wire.metadata_revision,
        dataset: wire.dataset,
        provider_product: wire.provider_product,
        provider_channel: wire.provider_channel,
        venue_id: wire.venue_id,
        underlying_instrument_id: wire.underlying_instrument_id,
        underlying_definition_revision: wire.underlying_definition_revision,
        provider_instrument_id: wire.provider_instrument_id,
        request_identity: wire.request_identity,
        observation_identity: wire.observation_identity,
        entitlement_evidence: wire.entitlement_evidence,
        capability_evidence: wire.capability_evidence,
        available_at: wire.available_at,
        received_at: wire.received_at,
        ingested_at: wire.ingested_at,
        filter,
    })
    .map_err(|_| ArrowConversionError::InvalidOptionMarketRow)
}

fn decode_completeness(payload: &[u8]) -> Result<OptionMarketCompleteness, ArrowConversionError> {
    let wire: CompletenessWire = serde_json::from_slice(payload)?;
    OptionMarketCompleteness::try_new(OptionMarketCompletenessInput {
        expected_records: wire.expected_records,
        returned_records: wire.returned_records,
        missing_records: wire.missing_records,
        unexpected_records: wire.unexpected_records,
        provider_reported_records: wire.provider_reported_records,
        page_count: wire.page_count,
        cursor: match wire.cursor {
            CursorWire::NotApplicable => OptionMarketCursorState::NotApplicable,
            CursorWire::Exhausted => OptionMarketCursorState::Exhausted,
            CursorWire::Incomplete => OptionMarketCursorState::Incomplete,
        },
        disposition: match wire.disposition {
            DispositionWire::Complete => OptionMarketBatchDisposition::Complete,
            DispositionWire::Unavailable => OptionMarketBatchDisposition::Unavailable,
        },
    })
    .map_err(|_| ArrowConversionError::InvalidOptionMarketRow)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotWire {
    terms: TermsWire,
    bid_price: ComponentWire<Money>,
    bid_size: ComponentWire<QuantityLots>,
    ask_price: ComponentWire<Money>,
    ask_size: ComponentWire<QuantityLots>,
    last_price: ComponentWire<Money>,
    last_size: ComponentWire<QuantityLots>,
    mark_price: ComponentWire<Money>,
    trade_conditions: ComponentWire<Box<[SourceIdentifier]>>,
    volume: ComponentWire<u64>,
    open_interest: ComponentWire<u64>,
    implied_volatility: ComponentWire<Decimal>,
    delta: ComponentWire<Decimal>,
    gamma: ComponentWire<Decimal>,
    theta: ComponentWire<Decimal>,
    vega: ComponentWire<Decimal>,
    rho: ComponentWire<Decimal>,
    underlying: UnderlyingWire,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TermsWire {
    option_instrument_id: InstrumentId,
    underlying_instrument_id: InstrumentId,
    option_definition_revision: EvidenceDigest,
    underlying_definition_revision: EvidenceDigest,
    provider_instrument_id: ProviderInstrumentId,
    occ_identity: Option<OccOptionIdentity>,
    expiration: CalendarDate,
    strike: Money,
    kind: OptionKind,
    multiplier: Decimal,
    exercise_style: ComponentWire<ExerciseWire>,
    settlement: ComponentWire<SettlementWire>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UnderlyingWire {
    price: ComponentWire<Money>,
    evidence: EvidenceDigest,
}

#[derive(Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum ComponentWire<T> {
    Observed {
        value: T,
        source_at: Option<Timestamp>,
    },
    Unavailable {
        reason: ComponentStateWire,
        source_at: Option<Timestamp>,
    },
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ComponentStateWire {
    ProviderAbsent,
    ProviderNull,
    NotApplicable,
    Omitted,
    Invalid,
    Unresolved,
}

#[derive(Deserialize)]
#[serde(tag = "kind", content = "source_value", rename_all = "snake_case")]
enum ExerciseWire {
    American,
    European,
    Bermudan,
    Other(SourceIdentifier),
}

#[derive(Deserialize)]
#[serde(tag = "kind", content = "source_value", rename_all = "snake_case")]
enum SettlementWire {
    Physical,
    Cash,
    Other(SourceIdentifier),
}

#[derive(Deserialize)]
#[serde(tag = "kind", content = "source_value", rename_all = "snake_case")]
enum ExpirationClassWire {
    Standard,
    Weekly,
    Monthly,
    Quarterly,
    EndOfMonth,
    Other(SourceIdentifier),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExpirationWire {
    underlying_instrument_id: InstrumentId,
    underlying_definition_revision: EvidenceDigest,
    provider_instrument_id: ProviderInstrumentId,
    expiration: CalendarDate,
    class: ComponentWire<ExpirationClassWire>,
    standard: ComponentWire<bool>,
}

fn decode_snapshot(payload: &[u8]) -> Result<OptionSnapshotObservation, ArrowConversionError> {
    let wire: SnapshotWire = serde_json::from_slice(payload)?;
    let terms = OptionContractTerms::try_new(OptionContractTermsInput {
        option_instrument_id: wire.terms.option_instrument_id,
        underlying_instrument_id: wire.terms.underlying_instrument_id,
        option_definition_revision: wire.terms.option_definition_revision,
        underlying_definition_revision: wire.terms.underlying_definition_revision,
        provider_instrument_id: wire.terms.provider_instrument_id,
        occ_identity: wire.terms.occ_identity,
        expiration: wire.terms.expiration,
        strike: wire.terms.strike,
        kind: wire.terms.kind,
        multiplier: wire.terms.multiplier,
        exercise_style: wire.terms.exercise_style.map(map_exercise),
        settlement: wire.terms.settlement.map(map_settlement),
    })
    .map_err(|_| ArrowConversionError::InvalidOptionMarketRow)?;
    let underlying = OptionUnderlyingObservation::try_new(
        wire.underlying.price.map(std::convert::identity),
        wire.underlying.evidence,
    )
    .map_err(|_| ArrowConversionError::InvalidOptionMarketRow)?;
    OptionSnapshotObservation::try_new(OptionSnapshotObservationInput {
        terms,
        bid_price: wire.bid_price.map(std::convert::identity),
        bid_size: wire.bid_size.map(std::convert::identity),
        ask_price: wire.ask_price.map(std::convert::identity),
        ask_size: wire.ask_size.map(std::convert::identity),
        last_price: wire.last_price.map(std::convert::identity),
        last_size: wire.last_size.map(std::convert::identity),
        mark_price: wire.mark_price.map(std::convert::identity),
        trade_conditions: wire.trade_conditions.map(std::convert::identity),
        volume: wire.volume.map(std::convert::identity),
        open_interest: wire.open_interest.map(std::convert::identity),
        implied_volatility: wire.implied_volatility.map(std::convert::identity),
        delta: wire.delta.map(std::convert::identity),
        gamma: wire.gamma.map(std::convert::identity),
        theta: wire.theta.map(std::convert::identity),
        vega: wire.vega.map(std::convert::identity),
        rho: wire.rho.map(std::convert::identity),
        underlying,
    })
    .map_err(|_| ArrowConversionError::InvalidOptionMarketRow)
}

fn decode_expiration(payload: &[u8]) -> Result<OptionExpirationObservation, ArrowConversionError> {
    let wire: ExpirationWire = serde_json::from_slice(payload)?;
    OptionExpirationObservation::try_new(OptionExpirationObservationInput {
        underlying_instrument_id: wire.underlying_instrument_id,
        underlying_definition_revision: wire.underlying_definition_revision,
        provider_instrument_id: wire.provider_instrument_id,
        expiration: wire.expiration,
        class: wire.class.map(map_expiration_class),
        standard: wire.standard.map(std::convert::identity),
    })
    .map_err(|_| ArrowConversionError::InvalidOptionMarketRow)
}

impl<T> ComponentWire<T> {
    fn map<U>(self, map: impl FnOnce(T) -> U) -> OptionComponent<U> {
        match self {
            Self::Observed { value, source_at } => OptionComponent::observed(map(value), source_at),
            Self::Unavailable { reason, source_at } => {
                OptionComponent::unavailable(reason.into_domain(), source_at)
            }
        }
    }
}

impl ComponentStateWire {
    const fn into_domain(self) -> OptionComponentState {
        match self {
            Self::ProviderAbsent => OptionComponentState::ProviderAbsent,
            Self::ProviderNull => OptionComponentState::ProviderNull,
            Self::NotApplicable => OptionComponentState::NotApplicable,
            Self::Omitted => OptionComponentState::Omitted,
            Self::Invalid => OptionComponentState::Invalid,
            Self::Unresolved => OptionComponentState::Unresolved,
        }
    }
}

fn map_exercise(value: ExerciseWire) -> OptionExerciseStyle {
    match value {
        ExerciseWire::American => OptionExerciseStyle::American,
        ExerciseWire::European => OptionExerciseStyle::European,
        ExerciseWire::Bermudan => OptionExerciseStyle::Bermudan,
        ExerciseWire::Other(value) => OptionExerciseStyle::Other(value),
    }
}

fn map_settlement(value: SettlementWire) -> OptionSettlementKind {
    match value {
        SettlementWire::Physical => OptionSettlementKind::Physical,
        SettlementWire::Cash => OptionSettlementKind::Cash,
        SettlementWire::Other(value) => OptionSettlementKind::Other(value),
    }
}

fn map_expiration_class(value: ExpirationClassWire) -> OptionExpirationClass {
    match value {
        ExpirationClassWire::Standard => OptionExpirationClass::Standard,
        ExpirationClassWire::Weekly => OptionExpirationClass::Weekly,
        ExpirationClassWire::Monthly => OptionExpirationClass::Monthly,
        ExpirationClassWire::Quarterly => OptionExpirationClass::Quarterly,
        ExpirationClassWire::EndOfMonth => OptionExpirationClass::EndOfMonth,
        ExpirationClassWire::Other(value) => OptionExpirationClass::Other(value),
    }
}
