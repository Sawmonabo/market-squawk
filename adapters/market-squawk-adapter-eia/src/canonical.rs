//! Closed normalization and non-authoritative publication-candidate handoff.

use std::collections::BTreeMap;
use std::mem::size_of;
use std::num::NonZeroU16;
use std::sync::Arc;

use bytes::Bytes;
use market_squawk_domain::{
    AvailabilityEvidence, DataQuality, DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence,
    MacroMissingValue, MacroObservation, PayloadHash, PayloadReference, ResearchContext,
    ResearchObservation, ResearchPeriod, ResearchProvenance, ResearchProvenanceInput,
    ResearchTemporalCoordinate, ResearchTime, RevisionNumber, SchemaVersion, SourceIdentifier,
    Timestamp,
};
use market_squawk_sources::{
    CURRENT_RESEARCH_RECORD_SCHEMA, CanonicalObservationFamily, CanonicalObservationPayload,
    ExtractionBatch, ExtractionContentIdentity, ExtractionRecord, ExtractionRequest,
    ExtractionRevisionEvidence, ExtractionRevisionPlan, MAX_OBSERVED_REVISION_BATCH_BYTES,
    ObservedRevisionRecord, ObservedSemanticPayload, ProviderCaptureSetReceipt,
    ProviderCaptureTerminalDisposition, ProviderNativeLineageBatch,
    ProviderNativeLineageBatchBuilder, ProviderNativeLineageImplementation,
    ProviderOrderedCaptureSegments, SealedProviderCaptureBinding, SealedProviderCaptureSetReceipt,
    SourceMetadata, SourceObjectCaptureIdentity,
};
use serde::Deserialize;

use crate::types::{digest_bytes, digest_parts};
use crate::{
    EiaAcquisition, EiaAcquisitionReceipt, EiaActivatedProvider, EiaDataRetrievalSealRejoin,
    EiaDigest, EiaDoctorReport, EiaError, EiaNativeValue, EiaObservation, EiaObservationClocks,
    EiaPeriod, EiaPeriodKind, EiaPublicationMode, EiaRootPageJournalRejoin, EiaSeriesIdentity,
    eia_data_dataset_identifier,
};

/// Exact coordinates the shared raw/revision/manifest authority must rejoin before publication.
///
/// This is intentionally neither serializable nor a publication receipt. It contains the actual
/// sealed capture receipt and no generation, manifest, checkpoint, PIT, or query authority.
#[derive(Debug)]
pub struct EiaPublicationRejoin {
    source_metadata: Arc<SourceMetadata>,
    doctor_report: EiaDoctorReport,
    sealed_doctor_captures: Box<[SealedProviderCaptureSetReceipt]>,
    provider_dataset: SourceIdentifier,
    query_digest: EiaDigest,
    contract_schema_digest: EiaDigest,
    api_version: crate::EiaApiVersion,
    acquisition_receipt: EiaAcquisitionReceipt,
    acquisition_digest: EiaDigest,
    root_page_rejoins: Box<[EiaRootPageJournalRejoin]>,
    capture_content_digest: EvidenceDigest,
    capture_observation_digest: EvidenceDigest,
    ordered_capture: ProviderOrderedCaptureSegments,
    canonical_schema: SourceIdentifier,
    canonical_schema_version: SchemaVersion,
    canonical_record_count: u32,
    publication_retained_bytes: usize,
    normalization_admitted_at: Timestamp,
    rejoin_digest: EiaDigest,
}

#[derive(serde::Serialize)]
struct EiaPublicationRejoinDigestInput<'a> {
    source_metadata: &'a SourceMetadata,
    doctor_report_digest: EiaDigest,
    sealed_doctor_captures: &'a [SealedProviderCaptureSetReceipt],
    provider_dataset: &'a SourceIdentifier,
    query_digest: EiaDigest,
    contract_schema_digest: EiaDigest,
    api_version: &'a crate::EiaApiVersion,
    acquisition_total: u64,
    acquisition_page_count: u32,
    acquisition_returned_rows: u64,
    acquisition_observation_count: u64,
    acquisition_missing_observation_count: u64,
    acquisition_response_bytes: u64,
    acquisition_page_digests: &'a [EiaDigest],
    acquisition_digest: EiaDigest,
    root_page_rejoins: &'a [EiaRootPageRejoinDigestInput<'a>],
    sealed_page_captures: &'a [&'a SealedProviderCaptureSetReceipt],
    capture_content_digest: EvidenceDigest,
    capture_observation_digest: EvidenceDigest,
    root_capture: &'a ProviderCaptureSetReceipt,
    ordered_capture_receipt_digest: EvidenceDigest,
    canonical_schema: &'a SourceIdentifier,
    canonical_schema_version: SchemaVersion,
    canonical_record_count: u32,
    publication_retained_bytes: usize,
    normalization_admitted_at: Timestamp,
}

#[derive(serde::Serialize)]
struct EiaRootPageRejoinDigestInput<'a> {
    source_metadata: &'a SourceMetadata,
    provider_dataset: &'a SourceIdentifier,
    query_digest: EiaDigest,
    contract_schema_digest: EiaDigest,
    api_version: &'a crate::EiaApiVersion,
    page_ordinal: u16,
    offset: u64,
    next_offset: Option<u64>,
    provider_total: u64,
    capture_receipt: &'a market_squawk_sources::ProviderCapturePageReceipt,
}

impl EiaPublicationRejoin {
    /// Returns the stable provider-content object identity, excluding local physical rejoin facts.
    pub fn source_object_id(&self) -> Result<SourceIdentifier, EiaError> {
        if self.capture_content_digest.algorithm() != DigestAlgorithm::Sha256 {
            return Err(EiaError::CaptureBinding);
        }
        let query_digest = self.query_digest.bytes();
        let contract_schema_digest = self.contract_schema_digest.bytes();
        let capture_content_digest = self.capture_content_digest.bytes();
        let object_digest = digest_parts(
            b"market-squawk/eia-source-object/v1",
            [
                self.source_metadata.source_id().as_str().as_bytes(),
                self.source_metadata
                    .revision()
                    .as_source_identifier()
                    .as_str()
                    .as_bytes(),
                self.provider_dataset.as_str().as_bytes(),
                query_digest.as_slice(),
                contract_schema_digest.as_slice(),
                capture_content_digest.as_slice(),
            ],
        );
        source_identifier_from_digest("eia-object", object_digest)
    }

    /// Returns the exact source metadata generation root must compare to its current registry.
    pub fn source_metadata(&self) -> &SourceMetadata {
        self.source_metadata.as_ref()
    }

    /// Returns the exact redacted doctor evidence identity.
    pub const fn doctor_report(&self) -> &EiaDoctorReport {
        &self.doctor_report
    }

    /// Returns the exclusive finite doctor deadline that root admission must precede.
    pub const fn doctor_expires_at(&self) -> market_squawk_domain::Timestamp {
        self.doctor_report.expires_at()
    }

    /// Returns the actual immutable doctor receipts root must retain in activation lineage.
    pub fn sealed_doctor_captures(&self) -> &[SealedProviderCaptureSetReceipt] {
        &self.sealed_doctor_captures
    }

    /// Returns the provider-query raw dataset identity.
    pub const fn provider_dataset(&self) -> &SourceIdentifier {
        &self.provider_dataset
    }

    /// Returns the exact secret-free query identity.
    pub const fn query_digest(&self) -> EiaDigest {
        self.query_digest
    }

    /// Returns the route/query/native-schema identity.
    pub const fn contract_schema_digest(&self) -> EiaDigest {
        self.contract_schema_digest
    }

    /// Returns the exact provider API version observed by metadata and every data page.
    pub const fn api_version(&self) -> &crate::EiaApiVersion {
        &self.api_version
    }

    /// Returns the complete typed acquisition receipt without granting read/publication authority.
    pub const fn acquisition_receipt(&self) -> &EiaAcquisitionReceipt {
        &self.acquisition_receipt
    }

    /// Returns the complete ordered offset-chain identity.
    pub const fn acquisition_digest(&self) -> EiaDigest {
        self.acquisition_digest
    }

    /// Returns every exact root-journal page coordinate in immutable chain order.
    pub fn root_page_rejoins(&self) -> &[EiaRootPageJournalRejoin] {
        &self.root_page_rejoins
    }

    /// Returns the exact number of standalone page seals in acquisition order.
    pub fn sealed_page_capture_count(&self) -> usize {
        self.ordered_capture.segment_count()
    }

    /// Returns persisted evidence for one exact standalone page seal.
    pub fn sealed_page_capture(&self, ordinal: usize) -> Option<&SealedProviderCaptureSetReceipt> {
        self.ordered_capture.persisted_segment_receipt(ordinal)
    }

    /// Returns the source-neutral provider-content identity.
    pub const fn capture_content_digest(&self) -> EvidenceDigest {
        self.capture_content_digest
    }

    /// Returns the provider observation identity including exact receipt clocks.
    pub const fn capture_observation_digest(&self) -> EvidenceDigest {
        self.capture_observation_digest
    }

    /// Returns the complete logical provider response capture.
    pub const fn capture_receipt(&self) -> &ProviderCaptureSetReceipt {
        self.ordered_capture.root_capture()
    }

    /// Returns the identity joining the logical capture to every ordered physical page seal.
    pub const fn ordered_capture_receipt_digest(&self) -> EvidenceDigest {
        self.ordered_capture.receipt_digest()
    }

    /// Returns the shared canonical research schema expected at root ingest.
    pub const fn canonical_schema(&self) -> &SourceIdentifier {
        &self.canonical_schema
    }

    /// Returns the shared canonical schema version expected at root ingest.
    pub const fn canonical_schema_version(&self) -> SchemaVersion {
        self.canonical_schema_version
    }

    /// Returns exact canonical record cardinality root must reconcile.
    pub const fn canonical_record_count(&self) -> u32 {
        self.canonical_record_count
    }

    /// Returns the checked simultaneous native/canonical/root/revision working-set charge.
    pub const fn publication_retained_bytes(&self) -> usize {
        self.publication_retained_bytes
    }

    /// Returns the trusted local instant at which this sealed chain entered normalization.
    pub const fn normalization_admitted_at(&self) -> Timestamp {
        self.normalization_admitted_at
    }

    /// Returns a non-authoritative integrity identity for these rejoin coordinates.
    pub const fn rejoin_digest(&self) -> EiaDigest {
        self.rejoin_digest
    }

    /// Reopens the exact source and complete ordered physical-seal binding before root ingest.
    pub fn validate(&self, current_source: &SourceMetadata) -> Result<(), EiaError> {
        self.doctor_report
            .validate()
            .map_err(|_| EiaError::CaptureBinding)?;
        let page_count = usize::try_from(self.acquisition_receipt.page_count())
            .map_err(|_| EiaError::CaptureBinding)?;
        let capture = self.ordered_capture.root_capture();
        if current_source != self.source_metadata.as_ref()
            || self.doctor_report.source_id() != self.source_metadata.source_id()
            || self.doctor_report.metadata_revision() != self.source_metadata.revision()
            || self.doctor_report.source_metadata_payload_digest()
                != self
                    .source_metadata
                    .revision_evidence()
                    .payload_evidence()
                    .content_digest()
            || self.doctor_report.authorization_subject()
                != self
                    .source_metadata
                    .authorization()
                    .basis()
                    .as_source_identifier()
            || self.doctor_report.authorization_evidence()
                != self
                    .source_metadata
                    .authorization()
                    .evidence()
                    .content_digest()
            || self.doctor_report.authorization_starts_at()
                != self
                    .source_metadata
                    .authorization()
                    .effective_interval()
                    .starts_at()
            || self.doctor_report.authorization_ends_at()
                != self
                    .source_metadata
                    .authorization()
                    .effective_interval()
                    .ends_at()
            || self.sealed_doctor_captures.len()
                != self.doctor_report.doctor_capture_receipts().len()
            || self
                .sealed_doctor_captures
                .iter()
                .zip(self.doctor_report.doctor_capture_receipts())
                .any(|(sealed, expected)| {
                    sealed.capture() != expected
                        || sealed.receipt_digest().bytes() == [0; 32]
                        || sealed.segment().physical_receipt_digest().bytes() == [0; 32]
                })
            || self.acquisition_receipt.query_digest() != self.query_digest
            || self.acquisition_receipt.contract_schema_digest() != self.contract_schema_digest
            || self.acquisition_receipt.api_version() != &self.api_version
            || self.acquisition_receipt.content_digest() != self.acquisition_digest
            || page_count == 0
            || self.root_page_rejoins.len() != page_count
            || self.ordered_capture.segment_count() != page_count
            || capture.pages().len() != page_count
            || capture.content_digest() != self.capture_content_digest
            || capture.observation_digest() != self.capture_observation_digest
            || self.ordered_capture.receipt_digest().bytes() == [0; 32]
            || u64::from(self.canonical_record_count)
                != self.acquisition_receipt.observation_count()
            || self.publication_retained_bytes
                < self.acquisition_receipt.publication_retained_bytes()
            || self.publication_retained_bytes > MAX_OBSERVED_REVISION_BATCH_BYTES
            || self.normalization_admitted_at < self.doctor_report.observed_at()
            || self.normalization_admitted_at >= self.doctor_report.expires_at()
            || !self
                .source_metadata
                .authorization()
                .is_effective_at(self.normalization_admitted_at)
            || self.compute_digest()? != self.rejoin_digest
        {
            return Err(EiaError::CaptureBinding);
        }
        validate_capture_receipt(
            self.source_metadata.as_ref(),
            &self.provider_dataset,
            self.query_digest,
            self.contract_schema_digest,
            &self.api_version,
            &self.acquisition_receipt,
            capture,
        )?;
        let mut previous_received_at = None;
        for (index, (page_rejoin, full_page)) in self
            .root_page_rejoins
            .iter()
            .zip(capture.pages())
            .enumerate()
        {
            let ordinal = u16::try_from(index).map_err(|_| EiaError::CaptureBinding)?;
            let sealed_page = self
                .ordered_capture
                .persisted_segment_receipt(index)
                .ok_or(EiaError::CaptureBinding)?;
            let expected_next_offset = self
                .root_page_rejoins
                .get(index + 1)
                .map(EiaRootPageJournalRejoin::offset);
            if page_rejoin.source_metadata() != self.source_metadata.as_ref()
                || page_rejoin.provider_dataset() != &self.provider_dataset
                || page_rejoin.query_digest() != self.query_digest
                || page_rejoin.contract_schema_digest() != self.contract_schema_digest
                || page_rejoin.api_version() != &self.api_version
                || page_rejoin.page_ordinal() != ordinal
                || (index == 0) != (page_rejoin.offset() == 0)
                || page_rejoin.next_offset() != expected_next_offset
                || page_rejoin.provider_total() != self.acquisition_receipt.total()
                || page_rejoin.capture_receipt() != full_page
                || full_page.ordinal() != ordinal
                || self
                    .acquisition_receipt
                    .page_digests()
                    .get(index)
                    .is_none_or(|digest| digest.bytes() != full_page.body_digest().bytes())
                || !self
                    .source_metadata
                    .authorization()
                    .is_effective_at(full_page.received_at())
                || full_page.received_at() < self.doctor_report.observed_at()
                || full_page.received_at() >= self.doctor_report.expires_at()
                || full_page.received_at() > self.normalization_admitted_at
                || previous_received_at
                    .is_some_and(|received_at| received_at > full_page.received_at())
                || (index == 0
                    && full_page.received_at() != self.acquisition_receipt.first_received_at())
                || (index + 1 == page_count
                    && full_page.received_at() != self.acquisition_receipt.last_received_at())
            {
                return Err(EiaError::CaptureBinding);
            }
            crate::transport::validate_root_page_rejoin_seal(page_rejoin, sealed_page)
                .map_err(|_| EiaError::CaptureBinding)?;
            previous_received_at = Some(full_page.received_at());
        }
        Ok(())
    }

    fn compute_digest(&self) -> Result<EiaDigest, EiaError> {
        let mut root_page_rejoins = Vec::new();
        root_page_rejoins
            .try_reserve_exact(self.root_page_rejoins.len())
            .map_err(|_| EiaError::AllocationFailure)?;
        root_page_rejoins.extend(self.root_page_rejoins.iter().map(|rejoin| {
            EiaRootPageRejoinDigestInput {
                source_metadata: rejoin.source_metadata(),
                provider_dataset: rejoin.provider_dataset(),
                query_digest: rejoin.query_digest(),
                contract_schema_digest: rejoin.contract_schema_digest(),
                api_version: rejoin.api_version(),
                page_ordinal: rejoin.page_ordinal(),
                offset: rejoin.offset(),
                next_offset: rejoin.next_offset(),
                provider_total: rejoin.provider_total(),
                capture_receipt: rejoin.capture_receipt(),
            }
        }));
        let mut sealed_page_captures = Vec::new();
        sealed_page_captures
            .try_reserve_exact(self.ordered_capture.segment_count())
            .map_err(|_| EiaError::AllocationFailure)?;
        for ordinal in 0..self.ordered_capture.segment_count() {
            sealed_page_captures.push(
                self.ordered_capture
                    .persisted_segment_receipt(ordinal)
                    .ok_or(EiaError::CaptureBinding)?,
            );
        }
        let semantic = serde_json::to_vec(&EiaPublicationRejoinDigestInput {
            source_metadata: self.source_metadata.as_ref(),
            doctor_report_digest: self.doctor_report.report_digest(),
            sealed_doctor_captures: &self.sealed_doctor_captures,
            provider_dataset: &self.provider_dataset,
            query_digest: self.query_digest,
            contract_schema_digest: self.contract_schema_digest,
            api_version: &self.api_version,
            acquisition_total: self.acquisition_receipt.total(),
            acquisition_page_count: self.acquisition_receipt.page_count(),
            acquisition_returned_rows: self.acquisition_receipt.returned_rows(),
            acquisition_observation_count: self.acquisition_receipt.observation_count(),
            acquisition_missing_observation_count: self
                .acquisition_receipt
                .missing_observation_count(),
            acquisition_response_bytes: self.acquisition_receipt.response_bytes(),
            acquisition_page_digests: self.acquisition_receipt.page_digests(),
            acquisition_digest: self.acquisition_digest,
            root_page_rejoins: &root_page_rejoins,
            sealed_page_captures: &sealed_page_captures,
            capture_content_digest: self.capture_content_digest,
            capture_observation_digest: self.capture_observation_digest,
            root_capture: self.ordered_capture.root_capture(),
            ordered_capture_receipt_digest: self.ordered_capture.receipt_digest(),
            canonical_schema: &self.canonical_schema,
            canonical_schema_version: self.canonical_schema_version,
            canonical_record_count: self.canonical_record_count,
            publication_retained_bytes: self.publication_retained_bytes,
            normalization_admitted_at: self.normalization_admitted_at,
        })
        .map_err(|_| EiaError::Canonicalization)?;
        Ok(digest_parts(
            b"market-squawk/eia-publication-rejoin/v6",
            [semantic.as_slice()],
        ))
    }
}

/// Canonical macro observation plus exact provider-native evidence retained for root publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EiaCanonicalObservation {
    observation: MacroObservation,
    page_ordinal: u16,
    native_series: Arc<EiaSeriesIdentity>,
    native_period: EiaPeriod,
    native_value: EiaNativeValue,
    native_clocks: EiaObservationClocks,
    native_semantic_digest: EiaDigest,
    native_row_digest: EiaDigest,
    native_schema_digest: EiaDigest,
    series_digest: EiaDigest,
    raw_page_digest: EiaDigest,
}

impl EiaCanonicalObservation {
    fn try_from_native(
        native: EiaObservation,
        observation_ordinal: usize,
        context: &EiaCanonicalContext<'_>,
    ) -> Result<Self, EiaError> {
        let page_ordinal = context
            .observation_page_ordinals
            .get(observation_ordinal)
            .copied()
            .ok_or(EiaError::CaptureBinding)?;
        let observation = canonical_macro(&native, observation_ordinal, context)?;
        let native_semantic_digest = native.semantic_digest();
        let native_row_digest = native.row_digest();
        let native_schema_digest = native.row_schema_digest();
        let series_digest = native.series().digest();
        let raw_page_digest = native.page_payload_digest();
        let native_clocks = native.clocks().clone();
        let (native_series, native_period, native_value) = native.into_canonical_lineage();
        Ok(Self {
            observation,
            page_ordinal,
            native_series: Arc::new(native_series),
            native_period,
            native_value,
            native_clocks,
            native_semantic_digest,
            native_row_digest,
            native_schema_digest,
            series_digest,
            raw_page_digest,
        })
    }

    /// Returns the canonical macro observation root will place into its extraction batch.
    pub const fn observation(&self) -> &MacroObservation {
        &self.observation
    }

    /// Returns complete route, field, frequency, facet, descriptor, and unit identity.
    pub fn native_series(&self) -> &EiaSeriesIdentity {
        self.native_series.as_ref()
    }

    /// Returns the exact lexical and precision-preserving provider period.
    pub const fn native_period(&self) -> &EiaPeriod {
        &self.native_period
    }

    /// Returns the exact provider-native value, including any lexical missing marker.
    pub const fn native_value(&self) -> &EiaNativeValue {
        &self.native_value
    }

    /// Returns every provider publication/update/availability clock plus local receipt time.
    pub const fn native_clocks(&self) -> &EiaObservationClocks {
        &self.native_clocks
    }

    /// Returns native row content identity.
    pub const fn native_row_digest(&self) -> EiaDigest {
        self.native_row_digest
    }

    /// Returns native row schema identity.
    pub const fn native_schema_digest(&self) -> EiaDigest {
        self.native_schema_digest
    }

    /// Returns stable provider series identity.
    pub const fn series_digest(&self) -> EiaDigest {
        self.series_digest
    }

    /// Returns exact raw-page content identity.
    pub const fn raw_page_digest(&self) -> EiaDigest {
        self.raw_page_digest
    }
}

fn canonical_macro(
    native: &EiaObservation,
    observation_ordinal: usize,
    context: &EiaCanonicalContext<'_>,
) -> Result<MacroObservation, EiaError> {
    let page_ordinal = context
        .observation_page_ordinals
        .get(observation_ordinal)
        .copied()
        .ok_or(EiaError::CaptureBinding)?;
    if context
        .page_digests
        .get(usize::from(page_ordinal))
        .is_none_or(|digest| *digest != native.page_payload_digest())
    {
        return Err(EiaError::CaptureBinding);
    }
    let clocks = native.clocks();
    if context.normalization_admitted_at < clocks.received_at() {
        return Err(EiaError::CaptureBinding);
    }
    let series = source_identifier_from_digest("eia-series", native.series().digest())?;
    let unit =
        source_identifier_from_digest("eia-unit", digest_bytes(native.series().unit().as_bytes()))?;
    let source_identifier = SourceIdentifier::try_from(format!(
        "eia-row:{}:{}",
        lower_hex(native.series().digest().bytes()),
        lower_hex(native.row_digest().bytes())
    ))
    .map_err(|_| EiaError::Canonicalization)?;
    let provenance = ResearchProvenance::try_new(ResearchProvenanceInput {
        source_id: context.source.source_id().clone(),
        instrument_id: None,
        venue_id: None,
        source_identifier,
        source_timestamp: clocks
            .available_at()
            .or(clocks.updated_at())
            .or(clocks.released_at()),
        received_at: clocks.received_at(),
        // This adapter clock is the normalization admission coordinate. Root publication records
        // its distinct durable commit clock.
        ingested_at: context.normalization_admitted_at,
        quality: DataQuality::OfficialDelayed,
        payload_reference: PayloadReference::ContentHash(PayloadHash::new(
            DigestAlgorithm::Sha256,
            native.page_payload_digest().bytes(),
        )),
        availability: AvailabilityEvidence::local_first_observed(
            clocks.conservative_available_at(),
        ),
    })
    .map_err(|_| EiaError::Canonicalization)?;
    let time = ResearchTime::try_new_with_coordinates(
        canonical_effective(native)?,
        clocks.released_at().map(ResearchTemporalCoordinate::exact),
        RevisionNumber::new(1).map_err(|_| EiaError::Canonicalization)?,
        None,
    )
    .map_err(|_| EiaError::Canonicalization)?;
    let research_context =
        ResearchContext::new(provenance, time).map_err(|_| EiaError::Canonicalization)?;
    match native.value() {
        EiaNativeValue::Decimal { value, .. } => Ok(MacroObservation::new(
            research_context,
            series,
            *value,
            unit,
        )),
        EiaNativeValue::Missing(missing) => Ok(MacroObservation::missing(
            research_context,
            series,
            MacroMissingValue::new(
                source_identifier_from_digest(
                    "eia-missing",
                    digest_bytes(missing.lexical().unwrap_or("json-null").as_bytes()),
                )?,
                None,
            ),
            unit,
        )),
        EiaNativeValue::String(_) => Err(EiaError::Canonicalization),
    }
}

/// One unique provider-native series descriptor required to interpret opaque canonical series IDs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EiaPublishedSeries {
    canonical_series: SourceIdentifier,
    native: Arc<EiaSeriesIdentity>,
}

impl EiaPublishedSeries {
    /// Returns the canonical macro-series identity.
    pub const fn canonical_series(&self) -> &SourceIdentifier {
        &self.canonical_series
    }

    /// Returns lossless route, dimensions, frequency, field, and unit coordinates.
    pub fn native(&self) -> &EiaSeriesIdentity {
        self.native.as_ref()
    }
}

/// Actual-sealed-capture-bound canonical input for the shared publication authority.
///
/// The adapter deliberately stops here. Only root composition may assign durable revisions,
/// ingest Arrow, commit immutable Parquet/manifests, advance restart state, or mint PIT/read
/// authority.
#[derive(Debug)]
pub struct EiaPublicationCandidate {
    rejoin: EiaPublicationRejoin,
    observations: Box<[EiaCanonicalObservation]>,
    series: Box<[EiaPublishedSeries]>,
    revision_plan: ExtractionRevisionPlan,
}

/// Current provider-policy evidence retained separately from decoded analytical payloads.
///
/// This is neither raw-capture authority nor durable publication authority. Shared composition
/// retains it to recheck that the exact source/authorization generation and bounded doctor window
/// used by the adapter are still current when it consumes the capture-bound publication input.
#[derive(Debug, Eq, PartialEq)]
pub struct EiaPublicationPolicyEvidence {
    source_metadata: Arc<SourceMetadata>,
    doctor_report: EiaDoctorReport,
    sealed_doctor_captures: Box<[SealedProviderCaptureSetReceipt]>,
    normalization_admitted_at: Timestamp,
}

impl EiaPublicationPolicyEvidence {
    /// Returns the exact source/authorization generation admitted by the adapter.
    pub fn source_metadata(&self) -> &SourceMetadata {
        self.source_metadata.as_ref()
    }

    /// Returns the exact activation/doctor report admitted by the adapter.
    pub const fn doctor_report(&self) -> &EiaDoctorReport {
        &self.doctor_report
    }

    /// Returns persisted doctor response evidence; it is not live capture authority.
    pub fn sealed_doctor_captures(&self) -> &[SealedProviderCaptureSetReceipt] {
        &self.sealed_doctor_captures
    }

    /// Returns the trusted normalization admission clock.
    pub const fn normalization_admitted_at(&self) -> Timestamp {
        self.normalization_admitted_at
    }

    /// Rechecks the exact source generation, authorization, doctor receipts, and bounded expiry.
    pub fn validate(&self, current_source: &SourceMetadata) -> Result<(), EiaError> {
        let operation_at =
            crate::transport::system_timestamp().map_err(|_| EiaError::CaptureBinding)?;
        self.doctor_report
            .validate()
            .map_err(|_| EiaError::CaptureBinding)?;
        if current_source != self.source_metadata.as_ref()
            || self.doctor_report.source_id() != current_source.source_id()
            || self.doctor_report.metadata_revision() != current_source.revision()
            || self.doctor_report.source_metadata_payload_digest()
                != current_source
                    .revision_evidence()
                    .payload_evidence()
                    .content_digest()
            || self.doctor_report.authorization_subject()
                != current_source
                    .authorization()
                    .basis()
                    .as_source_identifier()
            || self.doctor_report.authorization_evidence()
                != current_source.authorization().evidence().content_digest()
            || self.doctor_report.authorization_starts_at()
                != current_source
                    .authorization()
                    .effective_interval()
                    .starts_at()
            || self.doctor_report.authorization_ends_at()
                != current_source
                    .authorization()
                    .effective_interval()
                    .ends_at()
            || self.sealed_doctor_captures.len()
                != self.doctor_report.doctor_capture_receipts().len()
            || self
                .sealed_doctor_captures
                .iter()
                .zip(self.doctor_report.doctor_capture_receipts())
                .any(|(sealed, expected)| {
                    sealed.capture() != expected
                        || sealed.receipt_digest().bytes() == [0; 32]
                        || sealed.segment().physical_receipt_digest().bytes() == [0; 32]
                })
            || self.normalization_admitted_at < self.doctor_report.observed_at()
            || self.normalization_admitted_at >= self.doctor_report.expires_at()
            || operation_at < self.normalization_admitted_at
            || operation_at >= self.doctor_report.expires_at()
            || !current_source
                .authorization()
                .is_effective_at(self.normalization_admitted_at)
            || !current_source.authorization().is_effective_at(operation_at)
        {
            return Err(EiaError::CaptureBinding);
        }
        Ok(())
    }
}

/// Owned, capture-bound inputs for the shared extraction and publication spine.
#[derive(Debug)]
pub struct EiaSharedPublicationParts {
    policy_evidence: EiaPublicationPolicyEvidence,
    revision_plan: ExtractionRevisionPlan,
    sealed_capture_binding: SealedProviderCaptureBinding,
}

impl EiaSharedPublicationParts {
    /// Returns the standard source-neutral canonical extraction batch.
    pub const fn batch(&self) -> &ExtractionBatch {
        self.sealed_capture_binding.batch()
    }

    /// Returns the standard semantic identity recomputed from the capture-bound batch.
    pub const fn extraction_content_identity(&self) -> ExtractionContentIdentity {
        self.sealed_capture_binding.content_identity()
    }

    /// Returns current provider-policy evidence kept separate from decoded event payloads.
    pub const fn policy_evidence(&self) -> &EiaPublicationPolicyEvidence {
        &self.policy_evidence
    }

    /// Returns the local-content revision evidence aligned with the canonical batch.
    pub const fn revision_plan(&self) -> &ExtractionRevisionPlan {
        &self.revision_plan
    }

    /// Returns exact bounded EIA-native semantics aligned one-for-one with the canonical batch.
    pub const fn native_lineage(&self) -> &ProviderNativeLineageBatch {
        self.sealed_capture_binding.native_lineage()
    }

    /// Returns the validated whole-chain physical capture binding.
    pub const fn sealed_capture_binding(&self) -> &SealedProviderCaptureBinding {
        &self.sealed_capture_binding
    }

    /// Consumes the handoff into the exact owned inputs required by root publication.
    pub fn into_parts(
        self,
    ) -> (
        EiaPublicationPolicyEvidence,
        ExtractionRevisionPlan,
        SealedProviderCaptureBinding,
    ) {
        (
            self.policy_evidence,
            self.revision_plan,
            self.sealed_capture_binding,
        )
    }
}

impl EiaPublicationCandidate {
    /// Normalizes one complete acquisition only after its exact response chain is physically sealed.
    pub(crate) fn try_new(
        provider: &EiaActivatedProvider,
        retrieval: EiaDataRetrievalSealRejoin,
        normalization_admitted_at: Timestamp,
        max_publication_bytes: usize,
    ) -> Result<Self, EiaError> {
        if max_publication_bytes == 0 || max_publication_bytes > MAX_OBSERVED_REVISION_BATCH_BYTES {
            return Err(EiaError::InvalidLimit);
        }
        if provider.publication_mode() != EiaPublicationMode::CanonicalMacro {
            return Err(EiaError::Canonicalization);
        }
        let current_source = provider.source_metadata();
        let provider_dataset = eia_data_dataset_identifier(provider.contract())
            .map_err(|_| EiaError::CaptureBinding)?;
        crate::transport::validate_terminal_data_rejoin(
            current_source,
            provider.contract(),
            &retrieval,
        )
        .map_err(|_| EiaError::CaptureBinding)?;
        let (retrieval_dataset, acquisition, pages, _transport_receipt, ordered_capture) =
            retrieval.into_parts();
        if retrieval_dataset != provider_dataset || acquisition.observations().is_empty() {
            return Err(EiaError::Canonicalization);
        }
        let source = pages
            .first()
            .map(|page| Arc::clone(page.root_journal_rejoin().source_metadata_arc()))
            .ok_or(EiaError::CaptureBinding)?;
        if source.as_ref() != current_source {
            return Err(EiaError::CaptureBinding);
        }
        validate_capture(
            source.as_ref(),
            &provider_dataset,
            provider.contract().query().identity(),
            provider.contract().schema_digest(),
            provider.contract().metadata().api_version(),
            &acquisition,
            ordered_capture.root_capture(),
        )?;
        let (native_observations, acquisition_receipt) = acquisition.into_parts();
        let observation_page_ordinals =
            observation_page_ordinals(&pages, native_observations.len())?;
        let context = EiaCanonicalContext {
            source: source.as_ref(),
            page_digests: acquisition_receipt.page_digests(),
            observation_page_ordinals: &observation_page_ordinals,
            normalization_admitted_at,
        };
        let publication_retained_bytes = publication_working_set_bytes(
            &native_observations,
            &context,
            acquisition_receipt.publication_retained_bytes(),
            pages.len(),
            provider,
            &ordered_capture,
        )?;
        if publication_retained_bytes > max_publication_bytes {
            return Err(EiaError::InvalidLimit);
        }
        let mut root_page_rejoins = Vec::new();
        root_page_rejoins
            .try_reserve_exact(pages.len())
            .map_err(|_| EiaError::AllocationFailure)?;
        root_page_rejoins.extend(
            pages
                .into_vec()
                .into_iter()
                .map(crate::EiaDataPageMaterial::into_root_journal_rejoin),
        );
        let mut observations = Vec::new();
        observations
            .try_reserve_exact(native_observations.len())
            .map_err(|_| EiaError::AllocationFailure)?;
        for (observation_ordinal, native) in native_observations.into_iter().enumerate() {
            observations.push(EiaCanonicalObservation::try_from_native(
                native,
                observation_ordinal,
                &context,
            )?);
        }
        let mut series_by_digest: BTreeMap<EiaDigest, EiaPublishedSeries> = BTreeMap::new();
        for observation in &observations {
            let candidate = EiaPublishedSeries {
                canonical_series: source_identifier_from_digest(
                    "eia-series",
                    observation.native_series.digest(),
                )?,
                native: Arc::clone(&observation.native_series),
            };
            match series_by_digest.entry(observation.native_series.digest()) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(candidate);
                }
                std::collections::btree_map::Entry::Occupied(entry) => {
                    if entry.get() != &candidate {
                        return Err(EiaError::Canonicalization);
                    }
                }
            }
        }
        let mut series = Vec::new();
        series
            .try_reserve_exact(series_by_digest.len())
            .map_err(|_| EiaError::AllocationFailure)?;
        for (digest, candidate) in series_by_digest {
            if candidate.native.digest() != digest {
                return Err(EiaError::Canonicalization);
            }
            series.push(candidate);
        }
        let canonical_record_count =
            u32::try_from(observations.len()).map_err(|_| EiaError::InvalidLimit)?;
        let canonical_schema = SourceIdentifier::try_from("market-squawk-research-v3")
            .map_err(|_| EiaError::Canonicalization)?;
        let capture_content_digest = ordered_capture.root_capture().content_digest();
        let capture_observation_digest = ordered_capture.root_capture().observation_digest();
        let mut sealed_doctor_captures = Vec::new();
        sealed_doctor_captures
            .try_reserve_exact(provider.doctor_capture_count())
            .map_err(|_| EiaError::AllocationFailure)?;
        for ordinal in 0..provider.doctor_capture_count() {
            sealed_doctor_captures.push(
                provider
                    .sealed_doctor_capture(ordinal)
                    .ok_or(EiaError::CaptureBinding)?
                    .clone(),
            );
        }
        let mut rejoin = EiaPublicationRejoin {
            source_metadata: source,
            doctor_report: provider.doctor_report().clone(),
            sealed_doctor_captures: sealed_doctor_captures.into_boxed_slice(),
            provider_dataset,
            query_digest: provider.contract().query().identity(),
            contract_schema_digest: provider.contract().schema_digest(),
            api_version: provider.contract().metadata().api_version().clone(),
            acquisition_digest: acquisition_receipt.content_digest(),
            acquisition_receipt,
            root_page_rejoins: root_page_rejoins.into_boxed_slice(),
            capture_content_digest,
            capture_observation_digest,
            ordered_capture,
            canonical_schema,
            canonical_schema_version: SchemaVersion::CURRENT,
            canonical_record_count,
            publication_retained_bytes,
            normalization_admitted_at,
            rejoin_digest: EiaDigest::new([0; 32]),
        };
        rejoin.rejoin_digest = rejoin.compute_digest()?;
        rejoin.validate(provider.source_metadata())?;
        let revision_plan =
            ExtractionRevisionPlan::locally_observed_with_native_lineage(observations.len())
                .map_err(|_| EiaError::InvalidLimit)?;
        Ok(Self {
            rejoin,
            observations: observations.into_boxed_slice(),
            series: series.into_boxed_slice(),
            revision_plan,
        })
    }

    /// Returns the exact root-publication rejoin coordinates and actual physical seal.
    pub const fn rejoin(&self) -> &EiaPublicationRejoin {
        &self.rejoin
    }

    /// Returns all canonical rows with provider-native lineage.
    pub fn observations(&self) -> &[EiaCanonicalObservation] {
        &self.observations
    }

    /// Returns one lossless descriptor per exact canonical macro series.
    pub fn series(&self) -> &[EiaPublishedSeries] {
        &self.series
    }

    /// Returns the bounded aligned local-content revision plan root must submit to shared authority.
    pub const fn revision_plan(&self) -> &ExtractionRevisionPlan {
        &self.revision_plan
    }

    /// Clones the closed canonical payloads for construction of the shared extraction batch.
    pub fn research_observations(&self) -> impl ExactSizeIterator<Item = ResearchObservation> + '_ {
        self.observations
            .iter()
            .map(|observation| ResearchObservation::Macro(observation.observation.clone()))
    }

    /// Consumes this one-shot candidate into the shared capture-bound extraction handoff.
    pub fn try_into_shared_publication(
        self,
        request: ExtractionRequest,
    ) -> Result<EiaSharedPublicationParts, EiaError> {
        self.validate_shared_request(&request)?;
        self.validate_canonical_alignment()?;
        let schema = SourceIdentifier::try_from(CURRENT_RESEARCH_RECORD_SCHEMA)
            .map_err(|_| EiaError::Canonicalization)?;
        let mut records = Vec::new();
        records
            .try_reserve_exact(self.observations.len())
            .map_err(|_| EiaError::AllocationFailure)?;
        for observation in &self.observations {
            let context = observation.observation.context();
            let page_ordinal = usize::from(observation.page_ordinal);
            let page = self
                .rejoin
                .capture_receipt()
                .pages()
                .get(page_ordinal)
                .ok_or(EiaError::CaptureBinding)?;
            let page_rejoin = self
                .rejoin
                .root_page_rejoins()
                .get(page_ordinal)
                .ok_or(EiaError::CaptureBinding)?;
            let sealed_page = self
                .rejoin
                .sealed_page_capture(page_ordinal)
                .ok_or(EiaError::CaptureBinding)?;
            if page.ordinal() != observation.page_ordinal
                || page.body_digest().bytes() != observation.raw_page_digest.bytes()
                || page.received_at() != observation.native_clocks.received_at()
                || page_rejoin.page_ordinal() != observation.page_ordinal
                || page_rejoin.capture_receipt() != page
                || crate::transport::validate_root_page_rejoin_seal(page_rejoin, sealed_page)
                    .is_err()
                || context.provenance().received_at() != observation.native_clocks.received_at()
                || context.provenance().ingested_at() != self.rejoin.normalization_admitted_at
                || context.time().published()
                    != observation
                        .native_clocks
                        .released_at()
                        .map(ResearchTemporalCoordinate::exact)
                        .as_ref()
            {
                return Err(EiaError::CaptureBinding);
            }
            let research = ResearchObservation::Macro(observation.observation.clone());
            let canonical_payload = CanonicalObservationPayload::try_from_observation(&research)
                .map_err(|_| EiaError::Canonicalization)?;
            let observed_payload =
                ObservedSemanticPayload::try_from_bytes(canonical_payload.exact_bytes())
                    .map_err(|_| EiaError::Canonicalization)?;
            let revision =
                source_identifier_from_evidence("eia-local-content", observed_payload.identity())?;
            let payload = serde_json::to_vec(&research).map_err(|_| EiaError::Canonicalization)?;
            let evidence = ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                digest_bytes(&payload).bytes(),
            ));
            records.push(
                ExtractionRecord::try_new_with_time(
                    &request,
                    schema.clone(),
                    evidence,
                    context.time().effective().clone(),
                    context.time().published().cloned(),
                    market_squawk_sources::AvailabilityEvidence::LocalFirstObserved {
                        observed_at: observation.native_clocks.received_at(),
                    },
                    revision,
                    context.time().superseded().cloned(),
                    Bytes::from(payload),
                )
                .map_err(|_| EiaError::Canonicalization)?,
            );
        }
        let batch = ExtractionBatch::try_new(&request, records)
            .and_then(|batch| batch.try_bind_provider_capture(self.rejoin.capture_receipt()))
            .map_err(|_| EiaError::CaptureBinding)?;
        if batch.records().len() != self.observations.len()
            || batch.request().object().capture_identity()
                != SourceObjectCaptureIdentity::try_from_capture(self.rejoin.capture_receipt())
                    .map_err(|_| EiaError::CaptureBinding)?
        {
            return Err(EiaError::CaptureBinding);
        }
        let native_lineage = eia_native_lineage(&self.observations, &batch)?;
        let mut row_capture_page_ordinals = Vec::new();
        row_capture_page_ordinals
            .try_reserve_exact(self.observations.len())
            .map_err(|_| EiaError::AllocationFailure)?;
        row_capture_page_ordinals.extend(
            self.observations
                .iter()
                .map(|observation| observation.page_ordinal),
        );
        let EiaPublicationRejoin {
            source_metadata,
            doctor_report,
            sealed_doctor_captures,
            ordered_capture,
            normalization_admitted_at,
            ..
        } = self.rejoin;
        let policy_evidence = EiaPublicationPolicyEvidence {
            source_metadata,
            doctor_report,
            sealed_doctor_captures,
            normalization_admitted_at,
        };
        let sealed_capture_binding = SealedProviderCaptureBinding::try_ordered_segments(
            ordered_capture,
            batch,
            native_lineage,
            row_capture_page_ordinals,
        )
        .map_err(|_| EiaError::CaptureBinding)?;
        Ok(EiaSharedPublicationParts {
            policy_evidence,
            revision_plan: self.revision_plan,
            sealed_capture_binding,
        })
    }

    fn validate_shared_request(&self, request: &ExtractionRequest) -> Result<(), EiaError> {
        self.rejoin.validate(self.rejoin.source_metadata())?;
        let object = request.object();
        let receipt = self.rejoin.acquisition_receipt();
        let capture = self.rejoin.capture_receipt();
        let effective =
            market_squawk_domain::EffectiveInterval::new(receipt.first_received_at(), None)
                .map_err(|_| EiaError::CaptureBinding)?;
        let availability = market_squawk_sources::AvailabilityEvidence::LocalFirstObserved {
            observed_at: receipt.last_received_at(),
        };
        if object.source_id() != self.rejoin.source_metadata.source_id()
            || object.metadata_revision() != self.rejoin.source_metadata.revision()
            || object.dataset() != &self.rejoin.provider_dataset
            || object.object_id() != &self.rejoin.source_object_id()?
            || object.media_type().as_str() != "application/json"
            || object.evidence()
                != &ExactPayloadEvidence::from_content_digest(self.rejoin.capture_content_digest)
            || object.capture_identity() != SourceObjectCaptureIdentity::Standalone
            || object.effective_interval() != effective
            || object.published_at().is_some()
            || object.availability() != &availability
            || object.expected_bytes() != Some(capture.total_body_bytes())
            || request.max_records() < self.rejoin.canonical_record_count
        {
            return Err(EiaError::CaptureBinding);
        }
        Ok(())
    }

    fn validate_canonical_alignment(&self) -> Result<(), EiaError> {
        if self.observations.is_empty()
            || self.observations.len()
                != usize::try_from(self.rejoin.canonical_record_count)
                    .map_err(|_| EiaError::CaptureBinding)?
            || self.revision_plan.len() != self.observations.len()
            || !self.revision_plan.is_locally_observed()
            || !self.revision_plan.native_lineage_required()
            || self.series.is_empty()
            || self.series.len() > self.observations.len()
        {
            return Err(EiaError::Canonicalization);
        }
        let mut expected = BTreeMap::new();
        for observation in &self.observations {
            expected.insert(
                observation.native_series.digest(),
                EiaPublishedSeries {
                    canonical_series: source_identifier_from_digest(
                        "eia-series",
                        observation.native_series.digest(),
                    )?,
                    native: Arc::clone(&observation.native_series),
                },
            );
        }
        if expected.into_values().eq(self.series.iter().cloned()) {
            Ok(())
        } else {
            Err(EiaError::Canonicalization)
        }
    }
}

/// Adapter-owned precision recovered from one exact EIA native-lineage row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EiaNativePublishedSeriesPrecision {
    /// The provider supplied a civil calendar date.
    CalendarDate,
    /// The provider supplied a coarser source period with this exact series-bound scheme.
    SourcePeriod { scheme: SourceIdentifier },
}

/// Adapter-owned canonical series/time coordinate decoded from native-lineage schema v1.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EiaNativePublishedSeriesCoordinate {
    canonical_series: SourceIdentifier,
    precision: EiaNativePublishedSeriesPrecision,
}

impl EiaNativePublishedSeriesCoordinate {
    /// Returns the canonical series derived by the EIA adapter's own identity rule.
    pub const fn canonical_series(&self) -> &SourceIdentifier {
        &self.canonical_series
    }

    /// Returns the exact effective-time precision retained by the native row.
    pub const fn precision(&self) -> &EiaNativePublishedSeriesPrecision {
        &self.precision
    }
}

#[derive(serde::Serialize)]
#[serde(deny_unknown_fields)]
struct EiaNativeLineageRowV1<'a> {
    native_series: &'a EiaSeriesIdentity,
    native_period: &'a EiaPeriod,
    native_value: &'a EiaNativeValue,
    released_at: Option<Timestamp>,
    updated_at: Option<Timestamp>,
    available_at: Option<Timestamp>,
    series_digest: EiaDigest,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EiaNativeLineageCoordinateV1 {
    native_series: EiaNativeSeriesCoordinateV1,
    native_period: EiaNativePeriodCoordinateV1,
    native_value: serde_json::Value,
    released_at: Option<serde_json::Value>,
    updated_at: Option<serde_json::Value>,
    available_at: Option<serde_json::Value>,
    series_digest: EiaDigest,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EiaNativeSeriesCoordinateV1 {
    route: serde_json::Value,
    data_field: serde_json::Value,
    frequency: serde_json::Value,
    facets: Vec<serde_json::Value>,
    descriptors: Vec<serde_json::Value>,
    unit: String,
    digest: EiaDigest,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EiaNativePeriodCoordinateV1 {
    raw: String,
    format: String,
    frequency: serde_json::Value,
    kind: EiaNativePeriodKindCoordinateV1,
}

#[derive(Deserialize)]
enum EiaNativePeriodKindCoordinateV1 {
    CalendarDate(serde_json::Value),
    Month { year: u16, month: u8 },
    Quarter { year: u16, quarter: u8 },
    Year(u16),
    Provider(String),
}

/// Decodes one exact EIA native-lineage v1 row using the adapter-owned wire schema.
///
/// The returned identities use the same canonical series and provider-period scheme algorithms
/// as live normalization. Application restart code therefore never parses private JSON paths or
/// recreates EIA digest formatting.
pub fn decode_eia_native_published_series_coordinate(
    payload: &[u8],
) -> Result<EiaNativePublishedSeriesCoordinate, EiaError> {
    let decoded: EiaNativeLineageCoordinateV1 =
        serde_json::from_slice(payload).map_err(|_| EiaError::Canonicalization)?;
    let EiaNativeLineageCoordinateV1 {
        native_series,
        native_period,
        native_value,
        released_at,
        updated_at,
        available_at,
        series_digest,
    } = decoded;
    let EiaNativeSeriesCoordinateV1 {
        route,
        data_field,
        frequency: series_frequency,
        facets,
        descriptors,
        unit,
        digest,
    } = native_series;
    let EiaNativePeriodCoordinateV1 {
        raw,
        format,
        frequency: period_frequency,
        kind,
    } = native_period;
    if digest != series_digest
        || raw.is_empty()
        || format.is_empty()
        || unit.is_empty()
        || route.is_null()
        || data_field.is_null()
        || series_frequency.is_null()
        || period_frequency.is_null()
        || facets.iter().any(serde_json::Value::is_null)
        || descriptors.iter().any(serde_json::Value::is_null)
        || native_value.is_null()
        || released_at.as_ref().is_some_and(serde_json::Value::is_null)
        || updated_at.as_ref().is_some_and(serde_json::Value::is_null)
        || available_at
            .as_ref()
            .is_some_and(serde_json::Value::is_null)
    {
        return Err(EiaError::Canonicalization);
    }
    let canonical_series = source_identifier_from_digest("eia-series", series_digest)?;
    let precision = match kind {
        EiaNativePeriodKindCoordinateV1::CalendarDate(value) if !value.is_null() => {
            EiaNativePublishedSeriesPrecision::CalendarDate
        }
        EiaNativePeriodKindCoordinateV1::Month { year, month }
            if year != 0 && (1..=12).contains(&month) =>
        {
            EiaNativePublishedSeriesPrecision::SourcePeriod {
                scheme: source_identifier_from_digest("eia-period-scheme", series_digest)?,
            }
        }
        EiaNativePeriodKindCoordinateV1::Quarter { year, quarter }
            if year != 0 && (1..=4).contains(&quarter) =>
        {
            EiaNativePublishedSeriesPrecision::SourcePeriod {
                scheme: source_identifier_from_digest("eia-period-scheme", series_digest)?,
            }
        }
        EiaNativePeriodKindCoordinateV1::Year(year) if year != 0 => {
            EiaNativePublishedSeriesPrecision::SourcePeriod {
                scheme: source_identifier_from_digest("eia-period-scheme", series_digest)?,
            }
        }
        EiaNativePeriodKindCoordinateV1::Provider(value) => {
            let _ = value;
            return Err(EiaError::Canonicalization);
        }
        EiaNativePeriodKindCoordinateV1::CalendarDate(_)
        | EiaNativePeriodKindCoordinateV1::Month { .. }
        | EiaNativePeriodKindCoordinateV1::Quarter { .. }
        | EiaNativePeriodKindCoordinateV1::Year(_) => {
            return Err(EiaError::Canonicalization);
        }
    };
    Ok(EiaNativePublishedSeriesCoordinate {
        canonical_series,
        precision,
    })
}

fn eia_native_lineage(
    observations: &[EiaCanonicalObservation],
    batch: &ExtractionBatch,
) -> Result<ProviderNativeLineageBatch, EiaError> {
    if observations.is_empty() || observations.len() != batch.records().len() {
        return Err(EiaError::Canonicalization);
    }
    let mut native_lineage = ProviderNativeLineageBatchBuilder::try_new(
        ProviderNativeLineageImplementation::EiaSeriesV1,
        batch,
    )
    .map_err(|_| EiaError::AllocationFailure)?;
    for observation in observations {
        native_lineage
            .try_push(&EiaNativeLineageRowV1 {
                native_series: observation.native_series.as_ref(),
                native_period: &observation.native_period,
                native_value: &observation.native_value,
                released_at: observation.native_clocks.released_at(),
                updated_at: observation.native_clocks.updated_at(),
                available_at: observation.native_clocks.available_at(),
                series_digest: observation.series_digest,
            })
            .map_err(|_| EiaError::Canonicalization)?;
    }
    native_lineage
        .finish()
        .map_err(|_| EiaError::Canonicalization)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct EiaCanonicalContext<'a> {
    source: &'a SourceMetadata,
    page_digests: &'a [EiaDigest],
    observation_page_ordinals: &'a [u16],
    normalization_admitted_at: Timestamp,
}

fn observation_page_ordinals(
    pages: &[crate::EiaDataPageMaterial],
    expected_observations: usize,
) -> Result<Box<[u16]>, EiaError> {
    let mut ordinals = Vec::new();
    ordinals
        .try_reserve_exact(expected_observations)
        .map_err(|_| EiaError::AllocationFailure)?;
    for (ordinal, page) in pages.iter().enumerate() {
        let ordinal = u16::try_from(ordinal).map_err(|_| EiaError::InvalidLimit)?;
        let data = page.data_receipt();
        let raw = page.raw_page();
        let capture = raw.capture_receipt();
        if capture.ordinal() != ordinal
            || page.root_journal_rejoin().page_ordinal() != ordinal
            || page.root_journal_rejoin().capture_receipt() != capture
            || data.retained_payload_digest().bytes() != capture.body_digest().bytes()
            || data.retained_payload_digest() != digest_bytes(raw.payload())
            || data.received_at() != capture.received_at()
        {
            return Err(EiaError::CaptureBinding);
        }
        let count =
            usize::try_from(data.observation_count()).map_err(|_| EiaError::InvalidLimit)?;
        if ordinals
            .len()
            .checked_add(count)
            .is_none_or(|count| count > expected_observations)
        {
            return Err(EiaError::CaptureBinding);
        }
        ordinals.extend(std::iter::repeat_n(ordinal, count));
    }
    if ordinals.len() != expected_observations {
        return Err(EiaError::CaptureBinding);
    }
    Ok(ordinals.into_boxed_slice())
}

fn publication_working_set_bytes(
    native: &[EiaObservation],
    context: &EiaCanonicalContext<'_>,
    acquisition_retained_bytes: usize,
    page_count: usize,
    provider: &EiaActivatedProvider,
    ordered_capture: &ProviderOrderedCaptureSegments,
) -> Result<usize, EiaError> {
    let record_scratch = size_of::<ObservedRevisionRecord>()
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(size_of::<Vec<usize>>()))
        .and_then(|bytes| bytes.checked_add(size_of::<usize>().checked_mul(2)?))
        .ok_or(EiaError::InvalidLimit)?;
    let fixed_per_record = size_of::<EiaCanonicalObservation>()
        .checked_add(size_of::<EiaPublishedSeries>())
        .and_then(|bytes| bytes.checked_add(size_of::<EiaDigest>()))
        .and_then(|bytes| bytes.checked_add(4_usize.checked_mul(size_of::<usize>())?))
        .and_then(|bytes| bytes.checked_add(size_of::<EiaSeriesIdentity>()))
        .and_then(|bytes| bytes.checked_add(size_of::<ResearchObservation>()))
        .and_then(|bytes| bytes.checked_add(size_of::<ExtractionRevisionEvidence>()))
        .and_then(|bytes| bytes.checked_add(record_scratch))
        .ok_or(EiaError::InvalidLimit)?;
    let lineage_retained_bytes =
        publication_lineage_retained_bytes(page_count, provider, ordered_capture)?;
    let mut retained = acquisition_retained_bytes
        .checked_add(lineage_retained_bytes)
        .and_then(|bytes| bytes.checked_add(native.len().checked_mul(fixed_per_record)?))
        .and_then(|bytes| bytes.checked_add(context.source.source_id().retained_bytes()))
        .filter(|bytes| *bytes <= MAX_OBSERVED_REVISION_BATCH_BYTES)
        .ok_or(EiaError::InvalidLimit)?;
    for (observation_ordinal, observation) in native.iter().enumerate() {
        let macro_observation = canonical_macro(observation, observation_ordinal, context)?;
        let serialized =
            serde_json::to_vec(&macro_observation).map_err(|_| EiaError::Canonicalization)?;
        let research = ResearchObservation::Macro(macro_observation);
        let family = CanonicalObservationFamily::try_from_observation(&research)
            .map_err(|_| EiaError::InvalidLimit)?;
        let payload = CanonicalObservationPayload::try_from_observation(&research)
            .map_err(|_| EiaError::InvalidLimit)?;
        let dynamic = serialized
            .len()
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(family.source_id().retained_bytes()))
            .and_then(|bytes| bytes.checked_add(family.exact_bytes().len()))
            // Locally observed revision evidence retains the exact payload twice: as version
            // evidence and as semantic payload.
            .and_then(|bytes| bytes.checked_add(payload.exact_bytes().len().checked_mul(2)?))
            .ok_or(EiaError::InvalidLimit)?;
        retained = retained
            .checked_add(dynamic)
            .filter(|bytes| *bytes <= MAX_OBSERVED_REVISION_BATCH_BYTES)
            .ok_or(EiaError::InvalidLimit)?;
    }
    Ok(retained)
}

fn publication_lineage_retained_bytes(
    page_count: usize,
    provider: &EiaActivatedProvider,
    ordered_capture: &ProviderOrderedCaptureSegments,
) -> Result<usize, EiaError> {
    if ordered_capture.segment_count() != page_count {
        return Err(EiaError::CaptureBinding);
    }
    let root_capture = serde_json::to_vec(ordered_capture.root_capture())
        .map_err(|_| EiaError::Canonicalization)?;
    let mut page_seals = 0_usize;
    for ordinal in 0..ordered_capture.segment_count() {
        page_seals = page_seals
            .checked_add(
                serde_json::to_vec(
                    ordered_capture
                        .persisted_segment_receipt(ordinal)
                        .ok_or(EiaError::CaptureBinding)?,
                )
                .map_err(|_| EiaError::Canonicalization)?
                .len(),
            )
            .ok_or(EiaError::InvalidLimit)?;
    }
    let mut doctor_seals = 0_usize;
    for ordinal in 0..provider.doctor_capture_count() {
        let sealed = provider
            .sealed_doctor_capture(ordinal)
            .ok_or(EiaError::CaptureBinding)?;
        doctor_seals = doctor_seals
            .checked_add(
                serde_json::to_vec(sealed)
                    .map_err(|_| EiaError::Canonicalization)?
                    .len(),
            )
            .ok_or(EiaError::InvalidLimit)?;
    }
    size_of::<EiaPublicationCandidate>()
        .checked_add(size_of::<EiaPublicationRejoin>())
        .and_then(|bytes| bytes.checked_add(size_of::<Arc<SourceMetadata>>()))
        .and_then(|bytes| {
            bytes.checked_add(page_count.checked_mul(size_of::<EiaRootPageJournalRejoin>())?)
        })
        .and_then(|bytes| {
            bytes.checked_add(page_count.checked_mul(size_of::<EiaRootPageRejoinDigestInput>())?)
        })
        .and_then(|bytes| {
            bytes.checked_add(
                provider
                    .doctor_capture_count()
                    .checked_mul(size_of::<SealedProviderCaptureSetReceipt>())?,
            )
        })
        .and_then(|bytes| bytes.checked_add(root_capture.len()))
        .and_then(|bytes| bytes.checked_add(page_seals))
        .and_then(|bytes| bytes.checked_add(size_of::<ProviderOrderedCaptureSegments>()))
        .and_then(|bytes| bytes.checked_add(doctor_seals))
        .and_then(|bytes| bytes.checked_add(size_of::<EiaDoctorReport>()))
        .and_then(|bytes| bytes.checked_add(provider.doctor_report().route().as_str().len()))
        .and_then(|bytes| bytes.checked_add(provider.doctor_report().api_version().as_str().len()))
        .ok_or(EiaError::InvalidLimit)
}

fn validate_capture(
    source: &SourceMetadata,
    dataset: &SourceIdentifier,
    query_digest: EiaDigest,
    contract_schema_digest: EiaDigest,
    api_version: &crate::EiaApiVersion,
    acquisition: &EiaAcquisition,
    capture: &ProviderCaptureSetReceipt,
) -> Result<(), EiaError> {
    validate_capture_receipt(
        source,
        dataset,
        query_digest,
        contract_schema_digest,
        api_version,
        acquisition.receipt(),
        capture,
    )?;
    let mut observation_ordinal = 0_usize;
    for page in capture.pages() {
        let page_digest = EiaDigest::new(page.body_digest().bytes());
        let expected = acquisition
            .observations()
            .iter()
            .skip(observation_ordinal)
            .take_while(|observation| observation.page_payload_digest() == page_digest)
            .count();
        observation_ordinal = observation_ordinal
            .checked_add(expected)
            .ok_or(EiaError::CaptureBinding)?;
    }
    if observation_ordinal != acquisition.observations().len() {
        return Err(EiaError::CaptureBinding);
    }
    Ok(())
}

fn validate_capture_receipt(
    source: &SourceMetadata,
    dataset: &SourceIdentifier,
    query_digest: EiaDigest,
    contract_schema_digest: EiaDigest,
    api_version: &crate::EiaApiVersion,
    receipt: &EiaAcquisitionReceipt,
    capture: &ProviderCaptureSetReceipt,
) -> Result<(), EiaError> {
    if capture.source_id() != source.source_id()
        || capture.metadata_revision() != source.revision()
        || capture.dataset() != dataset
        || capture.request_set_identity().bytes() != query_digest.bytes()
        || capture.terminal() != ProviderCaptureTerminalDisposition::ExhaustedWithoutNextPage
        || receipt.query_digest() != query_digest
        || receipt.contract_schema_digest() != contract_schema_digest
        || receipt.api_version() != api_version
        || capture.pages().len()
            != usize::try_from(receipt.page_count()).map_err(|_| EiaError::CaptureBinding)?
        || capture
            .pages()
            .iter()
            .zip(receipt.page_digests())
            .any(|(page, digest)| page.body_digest().bytes() != digest.bytes())
    {
        return Err(EiaError::CaptureBinding);
    }
    Ok(())
}

fn canonical_effective(native: &EiaObservation) -> Result<ResearchTemporalCoordinate, EiaError> {
    match native.period().kind() {
        EiaPeriodKind::CalendarDate(date) => Ok(ResearchTemporalCoordinate::calendar_date(*date)),
        EiaPeriodKind::Year(year) => period_coordinate(native, *year, 1),
        EiaPeriodKind::Month { year, month } => period_coordinate(native, *year, u16::from(*month)),
        EiaPeriodKind::Quarter { year, quarter } => {
            period_coordinate(native, *year, u16::from(*quarter))
        }
        EiaPeriodKind::Provider(_) => Err(EiaError::Canonicalization),
    }
}

fn period_coordinate(
    native: &EiaObservation,
    year: u16,
    ordinal: u16,
) -> Result<ResearchTemporalCoordinate, EiaError> {
    let scheme = source_identifier_from_digest("eia-period-scheme", native.series().digest())?;
    let code = SourceIdentifier::try_from(native.period().raw())
        .map_err(|_| EiaError::Canonicalization)?;
    let ordinal = NonZeroU16::new(ordinal).ok_or(EiaError::Canonicalization)?;
    let period = ResearchPeriod::try_new(scheme, year, ordinal, code)
        .map_err(|_| EiaError::Canonicalization)?;
    Ok(ResearchTemporalCoordinate::source_period(period))
}

fn source_identifier_from_digest(
    prefix: &str,
    digest: EiaDigest,
) -> Result<SourceIdentifier, EiaError> {
    SourceIdentifier::try_from(format!("{prefix}:{}", lower_hex(digest.bytes())))
        .map_err(|_| EiaError::Canonicalization)
}

fn source_identifier_from_evidence(
    prefix: &str,
    digest: EvidenceDigest,
) -> Result<SourceIdentifier, EiaError> {
    if digest.algorithm() != DigestAlgorithm::Sha256 {
        return Err(EiaError::Canonicalization);
    }
    SourceIdentifier::try_from(format!("{prefix}:{}", lower_hex(digest.bytes())))
        .map_err(|_| EiaError::Canonicalization)
}

fn lower_hex(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}
