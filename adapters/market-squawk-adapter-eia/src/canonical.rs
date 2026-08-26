//! Closed normalization and non-authoritative publication-candidate handoff.

use std::collections::BTreeMap;
use std::mem::size_of;
use std::num::NonZeroU16;
use std::sync::Arc;

use market_squawk_domain::{
    AvailabilityEvidence, DataQuality, DigestAlgorithm, EvidenceDigest, MacroMissingValue,
    MacroObservation, PayloadHash, PayloadReference, ResearchContext, ResearchObservation,
    ResearchPeriod, ResearchProvenance, ResearchProvenanceInput, ResearchTemporalCoordinate,
    ResearchTime, RevisionNumber, SchemaVersion, SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    CanonicalObservationFamily, CanonicalObservationPayload, ExtractionRevisionEvidence,
    ExtractionRevisionPlan, MAX_OBSERVED_REVISION_BATCH_BYTES, ObservedRevisionRecord,
    ProviderCaptureTerminalDisposition, SealedProviderCaptureSetReceipt, SourceMetadata,
};

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
#[derive(Debug, Eq, PartialEq)]
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
    sealed_page_captures: Box<[SealedProviderCaptureSetReceipt]>,
    capture_content_digest: EvidenceDigest,
    capture_observation_digest: EvidenceDigest,
    sealed_capture: SealedProviderCaptureSetReceipt,
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
    sealed_page_captures: &'a [SealedProviderCaptureSetReceipt],
    capture_content_digest: EvidenceDigest,
    capture_observation_digest: EvidenceDigest,
    sealed_capture: &'a SealedProviderCaptureSetReceipt,
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
    /// Returns the exact source metadata generation root must compare to its current registry.
    pub fn source_metadata(&self) -> &SourceMetadata {
        self.source_metadata.as_ref()
    }

    /// Returns the non-authoritative fixed policy matrix root must rejoin to its rights decision.
    pub const fn private_use_policy_digest(&self) -> EiaDigest {
        self.doctor_report.private_use_policy_digest()
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

    /// Returns every actual standalone page seal in exact acquisition order.
    pub fn sealed_page_captures(&self) -> &[SealedProviderCaptureSetReceipt] {
        &self.sealed_page_captures
    }

    /// Returns the source-neutral provider-content identity.
    pub const fn capture_content_digest(&self) -> EvidenceDigest {
        self.capture_content_digest
    }

    /// Returns the provider observation identity including exact receipt clocks.
    pub const fn capture_observation_digest(&self) -> EvidenceDigest {
        self.capture_observation_digest
    }

    /// Returns the actual shared immutable-journal receipt root publication must consume.
    pub const fn sealed_capture(&self) -> &SealedProviderCaptureSetReceipt {
        &self.sealed_capture
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
            || self.private_use_policy_digest().bytes() == [0; 32]
            || self.acquisition_receipt.query_digest() != self.query_digest
            || self.acquisition_receipt.contract_schema_digest() != self.contract_schema_digest
            || self.acquisition_receipt.api_version() != &self.api_version
            || self.acquisition_receipt.content_digest() != self.acquisition_digest
            || page_count == 0
            || self.root_page_rejoins.len() != page_count
            || self.sealed_page_captures.len() != page_count
            || self.sealed_capture.capture().pages().len() != page_count
            || self.sealed_capture.capture().content_digest() != self.capture_content_digest
            || self.sealed_capture.capture().observation_digest() != self.capture_observation_digest
            || self.sealed_capture.receipt_digest().bytes() == [0; 32]
            || self
                .sealed_capture
                .segment()
                .physical_receipt_digest()
                .bytes()
                == [0; 32]
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
        validate_sealed_capture_receipt(
            self.source_metadata.as_ref(),
            &self.provider_dataset,
            self.query_digest,
            self.contract_schema_digest,
            &self.api_version,
            &self.acquisition_receipt,
            &self.sealed_capture,
        )?;
        let mut previous_received_at = None;
        for (index, ((page_rejoin, sealed_page), full_page)) in self
            .root_page_rejoins
            .iter()
            .zip(&self.sealed_page_captures)
            .zip(self.sealed_capture.capture().pages())
            .enumerate()
        {
            let ordinal = u16::try_from(index).map_err(|_| EiaError::CaptureBinding)?;
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
            sealed_page_captures: &self.sealed_page_captures,
            capture_content_digest: self.capture_content_digest,
            capture_observation_digest: self.capture_observation_digest,
            sealed_capture: &self.sealed_capture,
            canonical_schema: &self.canonical_schema,
            canonical_schema_version: self.canonical_schema_version,
            canonical_record_count: self.canonical_record_count,
            publication_retained_bytes: self.publication_retained_bytes,
            normalization_admitted_at: self.normalization_admitted_at,
        })
        .map_err(|_| EiaError::Canonicalization)?;
        Ok(digest_parts(
            b"market-squawk/eia-publication-rejoin/v4",
            [semantic.as_slice()],
        ))
    }
}

/// Canonical macro observation plus exact provider-native evidence retained for root publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EiaCanonicalObservation {
    observation: MacroObservation,
    native_series: Arc<EiaSeriesIdentity>,
    native_period: EiaPeriod,
    native_value: EiaNativeValue,
    native_clocks: EiaObservationClocks,
    native_row_digest: EiaDigest,
    native_schema_digest: EiaDigest,
    series_digest: EiaDigest,
    raw_page_digest: EiaDigest,
}

impl EiaCanonicalObservation {
    fn try_from_native(
        native: EiaObservation,
        context: &EiaCanonicalContext<'_>,
    ) -> Result<Self, EiaError> {
        let observation = canonical_macro(&native, context)?;
        let native_row_digest = native.row_digest();
        let native_schema_digest = native.row_schema_digest();
        let series_digest = native.series().digest();
        let raw_page_digest = native.page_payload_digest();
        let native_clocks = native.clocks().clone();
        let (native_series, native_period, native_value) = native.into_canonical_lineage();
        Ok(Self {
            observation,
            native_series: Arc::new(native_series),
            native_period,
            native_value,
            native_clocks,
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
    context: &EiaCanonicalContext<'_>,
) -> Result<MacroObservation, EiaError> {
    if !context.page_digests.contains(&native.page_payload_digest()) {
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
#[derive(Debug, Eq, PartialEq)]
pub struct EiaPublicationCandidate {
    rejoin: EiaPublicationRejoin,
    observations: Box<[EiaCanonicalObservation]>,
    series: Box<[EiaPublishedSeries]>,
    revision_plan: ExtractionRevisionPlan,
}

impl EiaPublicationCandidate {
    /// Normalizes one complete acquisition only after its exact response chain is physically sealed.
    pub(crate) fn try_new(
        provider: &EiaActivatedProvider,
        retrieval: EiaDataRetrievalSealRejoin,
        sealed_capture: SealedProviderCaptureSetReceipt,
        normalization_admitted_at: Timestamp,
    ) -> Result<Self, EiaError> {
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
            &sealed_capture,
        )
        .map_err(|_| EiaError::CaptureBinding)?;
        let (
            retrieval_dataset,
            acquisition,
            pages,
            _transport_receipt,
            sealed_page_captures,
            _combined_capture_receipt,
        ) = retrieval.into_parts();
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
        validate_sealed_capture(
            source.as_ref(),
            &provider_dataset,
            provider.contract().query().identity(),
            provider.contract().schema_digest(),
            provider.contract().metadata().api_version(),
            &acquisition,
            &sealed_capture,
        )?;
        let (native_observations, acquisition_receipt) = acquisition.into_parts();
        let context = EiaCanonicalContext {
            source: source.as_ref(),
            page_digests: acquisition_receipt.page_digests(),
            normalization_admitted_at,
        };
        let publication_retained_bytes = publication_working_set_bytes(
            &native_observations,
            &context,
            acquisition_receipt.publication_retained_bytes(),
            pages.len(),
            provider,
            &sealed_capture,
        )?;
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
        for native in native_observations {
            observations.push(EiaCanonicalObservation::try_from_native(native, &context)?);
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
        let capture_content_digest = sealed_capture.capture().content_digest();
        let capture_observation_digest = sealed_capture.capture().observation_digest();
        let mut sealed_doctor_captures = Vec::new();
        sealed_doctor_captures
            .try_reserve_exact(provider.sealed_doctor_captures().len())
            .map_err(|_| EiaError::AllocationFailure)?;
        sealed_doctor_captures.extend_from_slice(provider.sealed_doctor_captures());
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
            sealed_page_captures,
            capture_content_digest,
            capture_observation_digest,
            sealed_capture,
            canonical_schema,
            canonical_schema_version: SchemaVersion::CURRENT,
            canonical_record_count,
            publication_retained_bytes,
            normalization_admitted_at,
            rejoin_digest: EiaDigest::new([0; 32]),
        };
        rejoin.rejoin_digest = rejoin.compute_digest()?;
        rejoin.validate(provider.source_metadata())?;
        let revision_plan = ExtractionRevisionPlan::locally_observed(observations.len())
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

    /// Consumes the one-shot adapter handoff without minting any publication authority.
    pub fn into_parts(
        self,
    ) -> (
        EiaPublicationRejoin,
        Box<[EiaCanonicalObservation]>,
        Box<[EiaPublishedSeries]>,
        ExtractionRevisionPlan,
    ) {
        (
            self.rejoin,
            self.observations,
            self.series,
            self.revision_plan,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct EiaCanonicalContext<'a> {
    source: &'a SourceMetadata,
    page_digests: &'a [EiaDigest],
    normalization_admitted_at: Timestamp,
}

fn publication_working_set_bytes(
    native: &[EiaObservation],
    context: &EiaCanonicalContext<'_>,
    acquisition_retained_bytes: usize,
    page_count: usize,
    provider: &EiaActivatedProvider,
    sealed_capture: &SealedProviderCaptureSetReceipt,
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
        publication_lineage_retained_bytes(page_count, provider, sealed_capture)?;
    let mut retained = acquisition_retained_bytes
        .checked_add(lineage_retained_bytes)
        .and_then(|bytes| bytes.checked_add(native.len().checked_mul(fixed_per_record)?))
        .and_then(|bytes| bytes.checked_add(context.source.source_id().retained_bytes()))
        .filter(|bytes| *bytes <= MAX_OBSERVED_REVISION_BATCH_BYTES)
        .ok_or(EiaError::InvalidLimit)?;
    for observation in native {
        let macro_observation = canonical_macro(observation, context)?;
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
    sealed_capture: &SealedProviderCaptureSetReceipt,
) -> Result<usize, EiaError> {
    let combined_seal =
        serde_json::to_vec(sealed_capture).map_err(|_| EiaError::Canonicalization)?;
    let mut doctor_seals = 0_usize;
    for sealed in provider.sealed_doctor_captures() {
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
                    .sealed_doctor_captures()
                    .len()
                    .checked_mul(size_of::<SealedProviderCaptureSetReceipt>())?,
            )
        })
        .and_then(|bytes| bytes.checked_add(combined_seal.len()))
        .and_then(|bytes| bytes.checked_add(doctor_seals))
        .and_then(|bytes| bytes.checked_add(size_of::<EiaDoctorReport>()))
        .and_then(|bytes| bytes.checked_add(provider.doctor_report().route().as_str().len()))
        .and_then(|bytes| bytes.checked_add(provider.doctor_report().api_version().as_str().len()))
        .ok_or(EiaError::InvalidLimit)
}

fn validate_sealed_capture(
    source: &SourceMetadata,
    dataset: &SourceIdentifier,
    query_digest: EiaDigest,
    contract_schema_digest: EiaDigest,
    api_version: &crate::EiaApiVersion,
    acquisition: &EiaAcquisition,
    sealed_capture: &SealedProviderCaptureSetReceipt,
) -> Result<(), EiaError> {
    validate_sealed_capture_receipt(
        source,
        dataset,
        query_digest,
        contract_schema_digest,
        api_version,
        acquisition.receipt(),
        sealed_capture,
    )?;
    if acquisition.observations().iter().any(|observation| {
        !acquisition
            .receipt()
            .page_digests()
            .contains(&observation.page_payload_digest())
    }) {
        return Err(EiaError::CaptureBinding);
    }
    Ok(())
}

fn validate_sealed_capture_receipt(
    source: &SourceMetadata,
    dataset: &SourceIdentifier,
    query_digest: EiaDigest,
    contract_schema_digest: EiaDigest,
    api_version: &crate::EiaApiVersion,
    receipt: &EiaAcquisitionReceipt,
    sealed_capture: &SealedProviderCaptureSetReceipt,
) -> Result<(), EiaError> {
    let capture = sealed_capture.capture();
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
        || sealed_capture.receipt_digest().bytes() == [0; 32]
        || sealed_capture.segment().physical_receipt_digest().bytes() == [0; 32]
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

fn lower_hex(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}
