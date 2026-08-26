//! Non-authoritative BLS publication handoff bound to physically sealed provider evidence.

use std::sync::Arc;

use market_squawk_domain::{
    DataQuality, DigestAlgorithm, EvidenceDigest, MetadataRevision, PayloadReference,
    ResearchObservation, ResearchTemporalCoordinate, SchemaVersion, SourceId, SourceIdentifier,
    Timestamp,
};
use market_squawk_sources::{
    CURRENT_RESEARCH_RECORD_SCHEMA, DiscoveryRequestId, ExtractionBatch, ExtractionContentIdentity,
    ExtractionRevisionPlan, ProviderCaptureTerminalDisposition, SealedProviderCaptureSetReceipt,
    SourceMetadata, SourceObjectCaptureIdentity,
};
use rust_decimal::Decimal;
use serde::Serialize;
use sha2::{Digest as _, Sha256};

use crate::contract::BlsRuntimeInstanceCapability;
use crate::source::BlsExtractionOutput;
use crate::{
    BlsActivationCandidate, BlsCredentialRejoin, BlsProviderRateDeclaration, BlsResponse,
    BlsSource, BlsSourceConfig, BlsSourceError,
};

const PUBLICATION_CANDIDATE_SCHEMA_VERSION: u16 = 1;
const BLS_PROVIDER_SEMANTICS_SCHEMA: &str = "market-squawk-bls-provider-semantics-v1";

/// Explicit root schema-extension rejoin required to preserve BLS-native semantics.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BlsRootSchemaExtensionRequirement {
    base_schema: SourceIdentifier,
    base_schema_version: SchemaVersion,
    provider_semantics_schema: SourceIdentifier,
    co_persistence_required: bool,
    point_in_time_join_required: bool,
    typed_desktop_mcp_join_required: bool,
    requirement_digest: EvidenceDigest,
}

impl BlsRootSchemaExtensionRequirement {
    fn new() -> Result<Self, BlsSourceError> {
        let base_schema = SourceIdentifier::try_from(CURRENT_RESEARCH_RECORD_SCHEMA)
            .map_err(|_| BlsSourceError::InvalidPublication)?;
        let base_schema_version = SchemaVersion::CURRENT;
        let provider_semantics_schema = SourceIdentifier::try_from(BLS_PROVIDER_SEMANTICS_SCHEMA)
            .map_err(|_| BlsSourceError::InvalidPublication)?;
        let mut requirement = Self {
            base_schema,
            base_schema_version,
            provider_semantics_schema,
            co_persistence_required: true,
            point_in_time_join_required: true,
            typed_desktop_mcp_join_required: true,
            requirement_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
        };
        requirement.requirement_digest = digest_serialized(
            b"market-squawk/bls-root-schema-extension/v1",
            &(
                &requirement.base_schema,
                requirement.base_schema_version,
                &requirement.provider_semantics_schema,
                requirement.co_persistence_required,
                requirement.point_in_time_join_required,
                requirement.typed_desktop_mcp_join_required,
            ),
        )?;
        Ok(requirement)
    }

    /// Returns the shared canonical record schema carrying the MacroObservation payload.
    pub const fn base_schema(&self) -> &SourceIdentifier {
        &self.base_schema
    }

    /// Returns the exact companion provider-semantics schema root must persist and query.
    pub const fn provider_semantics_schema(&self) -> &SourceIdentifier {
        &self.provider_semantics_schema
    }

    /// Returns the complete root integration requirement identity.
    pub const fn requirement_digest(&self) -> EvidenceDigest {
        self.requirement_digest
    }

    /// Reconstructs the exact schema join instead of trusting serialized booleans.
    pub fn validate(&self) -> Result<(), BlsSourceError> {
        if self == &Self::new()? {
            Ok(())
        } else {
            Err(BlsSourceError::InvalidPublication)
        }
    }
}

/// Exact BLS footnote semantics retained beside the shared canonical macro row.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BlsCanonicalFootnote {
    code: Option<Box<str>>,
    text: Option<Box<str>>,
}

impl BlsCanonicalFootnote {
    /// Returns the exact provider code, including `P` for preliminary values.
    pub fn code(&self) -> Option<&str> {
        self.code.as_deref()
    }

    /// Returns exact provider explanatory text when supplied.
    pub fn text(&self) -> Option<&str> {
        self.text.as_deref()
    }
}

/// Full configured semantics for one BLS series in a publication chunk.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BlsCanonicalSeriesManifest {
    series_id: SourceIdentifier,
    title: Box<str>,
    unit: SourceIdentifier,
    frequency: SourceIdentifier,
    seasonal_adjustment: SourceIdentifier,
    measure: SourceIdentifier,
    metadata_content_digest: EvidenceDigest,
    authorization_reference: SourceIdentifier,
}

impl BlsCanonicalSeriesManifest {
    /// Returns the exact BLS series identifier.
    pub const fn series_id(&self) -> &SourceIdentifier {
        &self.series_id
    }

    /// Returns the user-verified provider title.
    pub fn title(&self) -> &str {
        &self.title
    }

    /// Returns the exact verified frequency.
    pub const fn frequency(&self) -> &SourceIdentifier {
        &self.frequency
    }

    /// Returns the exact verified seasonal-adjustment semantic.
    pub const fn seasonal_adjustment(&self) -> &SourceIdentifier {
        &self.seasonal_adjustment
    }

    /// Returns the exact verified measure semantic.
    pub const fn measure(&self) -> &SourceIdentifier {
        &self.measure
    }
}

/// BLS-native semantics and exact clocks aligned one-for-one with a canonical extraction row.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BlsCanonicalObservationSemantics {
    record_ordinal: u32,
    series_id: SourceIdentifier,
    year: u16,
    period: SourceIdentifier,
    period_label: Box<str>,
    raw_value: Box<str>,
    value: Option<Decimal>,
    latest: bool,
    preliminary: bool,
    footnotes: Box<[BlsCanonicalFootnote]>,
    missing_explanations: Box<[Box<str>]>,
    effective_time: ResearchTemporalCoordinate,
    first_observed_at: Timestamp,
    response_received_at: Timestamp,
    canonical_ingested_at: Timestamp,
    canonical_revision: SourceIdentifier,
    canonical_payload_digest: EvidenceDigest,
}

impl BlsCanonicalObservationSemantics {
    /// Returns the exact provider period label without reducing it to a month number.
    pub fn period_label(&self) -> &str {
        &self.period_label
    }

    /// Returns whether BLS marked this row latest.
    pub const fn is_latest(&self) -> bool {
        self.latest
    }

    /// Returns whether exact footnote code `P` marked this row preliminary.
    pub const fn is_preliminary(&self) -> bool {
        self.preliminary
    }

    /// Returns every exact provider footnote.
    pub fn footnotes(&self) -> &[BlsCanonicalFootnote] {
        &self.footnotes
    }

    /// Returns exact provider footnote text explaining a missing marker, without interpretation.
    pub fn missing_explanations(&self) -> &[Box<str>] {
        &self.missing_explanations
    }
}

/// Typed companion payload retaining BLS information the shared MacroObservation cannot express.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BlsCanonicalProviderSemantics {
    schema_requirement: BlsRootSchemaExtensionRequirement,
    series: Box<[BlsCanonicalSeriesManifest]>,
    observations: Box<[BlsCanonicalObservationSemantics]>,
    semantics_digest: EvidenceDigest,
}

impl BlsCanonicalProviderSemantics {
    pub(crate) fn try_new(
        config: &BlsSourceConfig,
        response: &BlsResponse,
        batch: &ExtractionBatch,
        first_observed_at: Timestamp,
        response_received_at: Timestamp,
        canonical_ingested_at: Timestamp,
    ) -> Result<Self, BlsSourceError> {
        let mut series_manifests = Vec::new();
        series_manifests
            .try_reserve_exact(response.series().len())
            .map_err(|_| BlsSourceError::InvalidPublication)?;
        let mut observations = Vec::new();
        observations
            .try_reserve_exact(batch.records().len())
            .map_err(|_| BlsSourceError::InvalidPublication)?;
        let mut record_ordinal = 0_usize;
        for series in response.series() {
            let metadata = config
                .series_metadata(series.series_id())
                .ok_or(BlsSourceError::InvalidSeriesMetadata)?;
            series_manifests.push(BlsCanonicalSeriesManifest {
                series_id: SourceIdentifier::try_from(series.series_id())
                    .map_err(|_| BlsSourceError::InvalidPublication)?,
                title: metadata.title().into(),
                unit: metadata.unit().clone(),
                frequency: metadata.frequency().clone(),
                seasonal_adjustment: metadata.seasonal_adjustment().clone(),
                measure: metadata.measure().clone(),
                metadata_content_digest: metadata.evidence().content_digest(),
                authorization_reference: metadata.authorization_reference().clone(),
            });
            for observation in series.observations() {
                let record = batch
                    .records()
                    .get(record_ordinal)
                    .ok_or(BlsSourceError::InvalidPublication)?;
                let ResearchObservation::Macro(canonical): ResearchObservation =
                    serde_json::from_slice(record.payload())
                        .map_err(|_| BlsSourceError::InvalidPublication)?
                else {
                    return Err(BlsSourceError::InvalidPublication);
                };
                let footnotes = observation
                    .footnotes()
                    .iter()
                    .map(|footnote| BlsCanonicalFootnote {
                        code: footnote.code().map(Into::into),
                        text: footnote.text().map(Into::into),
                    })
                    .collect::<Vec<_>>()
                    .into_boxed_slice();
                let missing_explanations = if observation.value().is_none() {
                    observation
                        .footnotes()
                        .iter()
                        .filter_map(|footnote| footnote.text().map(Into::into))
                        .collect::<Vec<Box<str>>>()
                        .into_boxed_slice()
                } else {
                    Box::default()
                };
                if record.schema().as_str() != CURRENT_RESEARCH_RECORD_SCHEMA
                    || record.available_at() != Some(first_observed_at)
                    || record.effective_time() != canonical.context().time().effective()
                    || record.revision() != canonical.context().provenance().source_identifier()
                    || canonical.series().as_str() != series.series_id()
                    || canonical.unit() != metadata.unit()
                    || canonical.context().provenance().received_at() != response_received_at
                    || canonical.context().provenance().ingested_at() != canonical_ingested_at
                    || canonical.value().observed_value() != observation.value()
                {
                    return Err(BlsSourceError::InvalidPublication);
                }
                observations.push(BlsCanonicalObservationSemantics {
                    record_ordinal: u32::try_from(record_ordinal)
                        .map_err(|_| BlsSourceError::InvalidPublication)?,
                    series_id: SourceIdentifier::try_from(series.series_id())
                        .map_err(|_| BlsSourceError::InvalidPublication)?,
                    year: observation.year(),
                    period: SourceIdentifier::try_from(observation.period())
                        .map_err(|_| BlsSourceError::InvalidPublication)?,
                    period_label: observation.period_name().into(),
                    raw_value: observation.raw_value().into(),
                    value: observation.value(),
                    latest: observation.is_latest(),
                    preliminary: observation.is_preliminary(),
                    footnotes,
                    missing_explanations,
                    effective_time: record.effective_time().clone(),
                    first_observed_at,
                    response_received_at,
                    canonical_ingested_at,
                    canonical_revision: record.revision().clone(),
                    canonical_payload_digest: record.evidence().content_digest(),
                });
                record_ordinal = record_ordinal
                    .checked_add(1)
                    .ok_or(BlsSourceError::InvalidPublication)?;
            }
        }
        if record_ordinal != batch.records().len() {
            return Err(BlsSourceError::InvalidPublication);
        }
        let schema_requirement = BlsRootSchemaExtensionRequirement::new()?;
        let mut semantics = Self {
            schema_requirement,
            series: series_manifests.into_boxed_slice(),
            observations: observations.into_boxed_slice(),
            semantics_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
        };
        semantics.semantics_digest = semantics.compute_digest()?;
        semantics.validate(batch)?;
        Ok(semantics)
    }

    /// Returns configured provider series semantics in response order.
    pub fn series(&self) -> &[BlsCanonicalSeriesManifest] {
        &self.series
    }

    /// Returns provider observations aligned one-for-one with canonical rows.
    pub fn observations(&self) -> &[BlsCanonicalObservationSemantics] {
        &self.observations
    }

    /// Returns the explicit root storage/query/presentation schema join.
    pub const fn schema_requirement(&self) -> &BlsRootSchemaExtensionRequirement {
        &self.schema_requirement
    }

    /// Returns the exact companion semantic payload identity.
    pub const fn semantics_digest(&self) -> EvidenceDigest {
        self.semantics_digest
    }

    pub(crate) fn validate(&self, batch: &ExtractionBatch) -> Result<(), BlsSourceError> {
        self.schema_requirement.validate()?;
        if self.series.is_empty()
            || self.observations.len() != batch.records().len()
            || self
                .observations
                .iter()
                .enumerate()
                .any(|(index, observation)| {
                    usize::try_from(observation.record_ordinal).ok() != Some(index)
                        || batch.records().get(index).is_none_or(|record| {
                            record.revision() != &observation.canonical_revision
                                || record.effective_time() != &observation.effective_time
                                || record.evidence().content_digest()
                                    != observation.canonical_payload_digest
                                || record.available_at() != Some(observation.first_observed_at)
                        })
                })
            || self.semantics_digest != self.compute_digest()?
        {
            return Err(BlsSourceError::InvalidPublication);
        }
        Ok(())
    }

    fn compute_digest(&self) -> Result<EvidenceDigest, BlsSourceError> {
        digest_serialized(
            b"market-squawk/bls-canonical-provider-semantics/v1",
            &(&self.schema_requirement, &self.series, &self.observations),
        )
    }
}

/// Canonical BLS input whose exact provider response has already been physically sealed.
///
/// This value is deliberately only a root-ingest handoff. It carries no manifest, generation,
/// committed timestamp, checkpoint, restore admission, query pin, or point-in-time receipt. Only
/// the shared application/data authority may assign locally observed revisions, commit an
/// immutable dataset, advance a job checkpoint, and mint a typed point-in-time read.
#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BlsPublicationCandidate {
    schema_version: u16,
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    discovery_request_id: DiscoveryRequestId,
    provider_dataset: SourceIdentifier,
    analytical_dataset: SourceIdentifier,
    object_id: SourceIdentifier,
    canonical_schema: SourceIdentifier,
    canonical_schema_version: SchemaVersion,
    schema_extension_requirement_digest: EvidenceDigest,
    chunk_index: u16,
    total_chunks: u16,
    canonical_record_count: u32,
    first_observed_at: Timestamp,
    response_received_at: Timestamp,
    canonical_ingested_at: Timestamp,
    discovery_request_set_identity: EvidenceDigest,
    discovery_capture_content_digest: EvidenceDigest,
    discovery_capture_observation_digest: EvidenceDigest,
    component_request_identity: EvidenceDigest,
    component_content_digest: EvidenceDigest,
    component_observation_digest: EvidenceDigest,
    source_generation_digest: EvidenceDigest,
    sealed_discovery_capture: SealedProviderCaptureSetReceipt,
    extraction_content_digest: EvidenceDigest,
    canonical_content_digest: EvidenceDigest,
    provider_semantics_digest: EvidenceDigest,
    credential_rejoin: BlsCredentialRejoin,
    provider_rate_declaration_digest: EvidenceDigest,
    doctor_report_digest: EvidenceDigest,
    sealed_doctor_capture_receipt_digest: EvidenceDigest,
    activation_expires_at: Timestamp,
    activation_candidate_digest: EvidenceDigest,
    batch: ExtractionBatch,
    provider_semantics: BlsCanonicalProviderSemantics,
    candidate_digest: EvidenceDigest,
    #[serde(skip_serializing)]
    runtime_instance: Arc<BlsRuntimeInstanceCapability>,
}

impl BlsPublicationCandidate {
    pub(crate) fn try_new(
        metadata: &SourceMetadata,
        config: &BlsSourceConfig,
        rate: &BlsProviderRateDeclaration,
        output: BlsExtractionOutput,
        activation: &BlsActivationCandidate,
        expected_runtime_instance: &Arc<BlsRuntimeInstanceCapability>,
    ) -> Result<Self, BlsSourceError> {
        let (batch, provider_semantics, discovery_admission) = output.into_parts();
        discovery_admission.validate_for_extraction(
            batch.request(),
            expected_runtime_instance,
            activation,
        )?;
        let object = batch.request().object();
        let chunk_index = discovery_admission.chunk_index();
        let sealed_discovery_capture = discovery_admission.sealed_discovery_capture().clone();
        let capture = sealed_discovery_capture.capture();
        let page = capture
            .pages()
            .get(chunk_index)
            .ok_or(BlsSourceError::InvalidPublication)?;
        let chunk_index =
            u16::try_from(chunk_index).map_err(|_| BlsSourceError::InvalidPublication)?;
        let total_chunks =
            u16::try_from(config.chunk_count()).map_err(|_| BlsSourceError::InvalidPublication)?;
        let canonical_record_count =
            u32::try_from(batch.records().len()).map_err(|_| BlsSourceError::InvalidPublication)?;
        let first_observed_at = object.effective_interval().starts_at();
        let response_received_at = page.received_at();
        let canonical_schema = SourceIdentifier::try_from(CURRENT_RESEARCH_RECORD_SCHEMA)
            .map_err(|_| BlsSourceError::InvalidPublication)?;
        let canonical_schema_version = SchemaVersion::CURRENT;
        let canonical_ingested_at = validate_canonical_batch(
            metadata,
            config,
            &batch,
            page.body_digest(),
            first_observed_at,
            response_received_at,
            &canonical_schema,
        )?;
        provider_semantics.validate(&batch)?;
        let schema_extension_requirement_digest =
            provider_semantics.schema_requirement().requirement_digest();
        let extraction_content = ExtractionContentIdentity::try_from_batch(&batch)
            .map_err(|_| BlsSourceError::InvalidPublication)?;

        if object.source_id() != metadata.source_id()
            || object.metadata_revision() != metadata.revision()
            || object.dataset() != config.dataset()
            || capture.source_id() != metadata.source_id()
            || capture.metadata_revision() != metadata.revision()
            || capture.dataset() != config.dataset()
            || !validate_discovery_component(
                capture,
                usize::from(chunk_index),
                object.capture_identity(),
                discovery_admission.component_request_identity(),
                discovery_admission.component_content_digest(),
                discovery_admission.component_observation_digest(),
            )
            || object.evidence().content_digest() != page.body_digest()
            || object.expected_bytes() != Some(page.body_bytes())
            || batch.request().deadline() >= activation.expires_at()
            || first_observed_at > response_received_at
            || response_received_at > canonical_ingested_at
            || usize::from(chunk_index) >= config.chunk_count()
            || canonical_record_count == 0
            || extraction_content.record_count() != batch.records().len()
            || sealed_discovery_capture.receipt_digest().bytes() == [0; 32]
            || activation.candidate_digest().bytes() == [0; 32]
            || discovery_admission.activation_candidate_digest() != activation.candidate_digest()
            || !Arc::ptr_eq(
                discovery_admission.runtime_instance(),
                expected_runtime_instance,
            )
        {
            return Err(BlsSourceError::InvalidPublication);
        }

        let mut candidate = Self {
            schema_version: PUBLICATION_CANDIDATE_SCHEMA_VERSION,
            source_id: metadata.source_id().clone(),
            metadata_revision: metadata.revision().clone(),
            discovery_request_id: object.discovery_request_id(),
            provider_dataset: config.dataset().clone(),
            analytical_dataset: BlsSource::analytical_dataset_identifier(config.dataset())?,
            object_id: object.object_id().clone(),
            canonical_schema,
            canonical_schema_version,
            schema_extension_requirement_digest,
            chunk_index,
            total_chunks,
            canonical_record_count,
            first_observed_at,
            response_received_at,
            canonical_ingested_at,
            discovery_request_set_identity: capture.request_set_identity(),
            discovery_capture_content_digest: capture.content_digest(),
            discovery_capture_observation_digest: capture.observation_digest(),
            component_request_identity: discovery_admission.component_request_identity(),
            component_content_digest: discovery_admission.component_content_digest(),
            component_observation_digest: discovery_admission.component_observation_digest(),
            source_generation_digest: discovery_admission.source_generation_digest(),
            sealed_discovery_capture,
            extraction_content_digest: extraction_content.digest(),
            canonical_content_digest: canonical_content_digest(&batch)?,
            provider_semantics_digest: provider_semantics.semantics_digest(),
            credential_rejoin: config.credential_rejoin(),
            provider_rate_declaration_digest: rate.declaration_digest(),
            doctor_report_digest: activation.doctor_report().report_digest(),
            sealed_doctor_capture_receipt_digest: activation
                .sealed_doctor_capture()
                .receipt_digest(),
            activation_expires_at: activation.expires_at(),
            activation_candidate_digest: activation.candidate_digest(),
            batch,
            provider_semantics,
            candidate_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
            runtime_instance: Arc::clone(expected_runtime_instance),
        };
        candidate.candidate_digest = candidate_digest(&candidate)?;
        Ok(candidate)
    }

    /// Reopens every provider-local invariant against the exact batch and physical receipt.
    ///
    /// Root should call this immediately before it reserves the shared ingest run. Successful
    /// validation does not mean that any dataset has been committed.
    pub(crate) fn validate(
        &self,
        metadata: &SourceMetadata,
        config: &BlsSourceConfig,
        rate: &BlsProviderRateDeclaration,
        activation: &BlsActivationCandidate,
        expected_runtime_instance: &Arc<BlsRuntimeInstanceCapability>,
    ) -> Result<(), BlsSourceError> {
        activation.validate(
            &crate::BlsActivationPlan::try_new(
                metadata.source_id().clone(),
                metadata.revision().clone(),
                config.dataset().clone(),
                BlsSource::analytical_dataset_identifier(config.dataset())?,
                config.credential_rejoin(),
                rate.clone(),
            )?,
            crate::client::system_timestamp()?,
            expected_runtime_instance,
        )?;
        let object = self.batch.request().object();
        let capture = self.sealed_discovery_capture.capture();
        let page = capture
            .pages()
            .get(usize::from(self.chunk_index))
            .ok_or(BlsSourceError::InvalidPublication)?;
        let extraction = ExtractionContentIdentity::try_from_batch(&self.batch)
            .map_err(|_| BlsSourceError::InvalidPublication)?;
        let canonical_ingested_at = validate_canonical_batch(
            metadata,
            config,
            &self.batch,
            page.body_digest(),
            self.first_observed_at,
            self.response_received_at,
            &self.canonical_schema,
        )?;
        self.provider_semantics.validate(&self.batch)?;
        if self.schema_version != PUBLICATION_CANDIDATE_SCHEMA_VERSION
            || self.source_id != *metadata.source_id()
            || self.metadata_revision != *metadata.revision()
            || self.provider_dataset != *config.dataset()
            || self.analytical_dataset
                != BlsSource::analytical_dataset_identifier(config.dataset())?
            || self.discovery_request_id != object.discovery_request_id()
            || self.object_id != *object.object_id()
            || self.first_observed_at != object.effective_interval().starts_at()
            || self.response_received_at != page.received_at()
            || self.response_received_at > self.canonical_ingested_at
            || self.batch.request().deadline() >= activation.expires_at()
            || self.canonical_schema.as_str() != CURRENT_RESEARCH_RECORD_SCHEMA
            || self.canonical_schema_version != SchemaVersion::CURRENT
            || self.schema_extension_requirement_digest
                != self
                    .provider_semantics
                    .schema_requirement()
                    .requirement_digest()
            || usize::from(self.total_chunks) != config.chunk_count()
            || usize::try_from(self.canonical_record_count).ok() != Some(self.batch.records().len())
            || self.canonical_record_count == 0
            || self.canonical_ingested_at != canonical_ingested_at
            || self.discovery_request_set_identity != capture.request_set_identity()
            || self.discovery_capture_content_digest != capture.content_digest()
            || self.discovery_capture_observation_digest != capture.observation_digest()
            || self.sealed_discovery_capture.receipt_digest().bytes() == [0; 32]
            || self.source_generation_digest != activation.plan().plan_digest()
            || !validate_discovery_component(
                capture,
                usize::from(self.chunk_index),
                object.capture_identity(),
                self.component_request_identity,
                self.component_content_digest,
                self.component_observation_digest,
            )
            || extraction.digest() != self.extraction_content_digest
            || canonical_content_digest(&self.batch)? != self.canonical_content_digest
            || self.provider_semantics.semantics_digest() != self.provider_semantics_digest
            || self.credential_rejoin != config.credential_rejoin()
            || self.provider_rate_declaration_digest != rate.declaration_digest()
            || self.doctor_report_digest != activation.doctor_report().report_digest()
            || self.sealed_doctor_capture_receipt_digest
                != activation.sealed_doctor_capture().receipt_digest()
            || self.activation_expires_at != activation.expires_at()
            || self.activation_candidate_digest != activation.candidate_digest()
            || !Arc::ptr_eq(&self.runtime_instance, expected_runtime_instance)
            || self.candidate_digest != candidate_digest(self)?
        {
            return Err(BlsSourceError::InvalidPublication);
        }
        Ok(())
    }

    /// Returns the exact source authority root ingest must rejoin.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact source-metadata revision root ingest must rejoin.
    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    /// Returns the discovery operation that produced the exact source object.
    pub const fn discovery_request_id(&self) -> DiscoveryRequestId {
        self.discovery_request_id
    }

    /// Returns the provider request-plan dataset retained as source provenance.
    pub const fn provider_dataset(&self) -> &SourceIdentifier {
        &self.provider_dataset
    }

    /// Returns the analytical dataset root publication must reserve.
    pub const fn analytical_dataset(&self) -> &SourceIdentifier {
        &self.analytical_dataset
    }

    /// Returns the exact discovered source object identity.
    pub const fn object_id(&self) -> &SourceIdentifier {
        &self.object_id
    }

    /// Returns the canonical extraction-record schema identity.
    pub const fn canonical_schema(&self) -> &SourceIdentifier {
        &self.canonical_schema
    }

    /// Returns the current shared canonical record schema version.
    pub const fn canonical_schema_version(&self) -> SchemaVersion {
        self.canonical_schema_version
    }

    /// Returns this request's zero-based deterministic chunk position.
    pub const fn chunk_index(&self) -> u16 {
        self.chunk_index
    }

    /// Returns the exact number of deterministic chunks in the request plan.
    pub const fn total_chunks(&self) -> u16 {
        self.total_chunks
    }

    /// Returns the exact number of canonical macro records root must ingest.
    pub const fn canonical_record_count(&self) -> u32 {
        self.canonical_record_count
    }

    /// Returns the first time this process observed the exact provider content.
    pub const fn first_observed_at(&self) -> Timestamp {
        self.first_observed_at
    }

    /// Returns the socket-boundary receipt time of the physically sealed response.
    pub const fn response_received_at(&self) -> Timestamp {
        self.response_received_at
    }

    /// Returns when canonical normalization completed locally.
    pub const fn canonical_ingested_at(&self) -> Timestamp {
        self.canonical_ingested_at
    }

    /// Returns the exact provider-request identity used for the sealed response.
    pub const fn request_set_identity(&self) -> EvidenceDigest {
        self.discovery_request_set_identity
    }

    /// Returns the stable provider-content identity excluding local receipt time.
    pub const fn capture_content_digest(&self) -> EvidenceDigest {
        self.discovery_capture_content_digest
    }

    /// Returns the receive-time-bound provider-capture identity.
    pub const fn capture_observation_digest(&self) -> EvidenceDigest {
        self.discovery_capture_observation_digest
    }

    /// Returns the stable source/configuration/rights/credential generation of this receipt.
    pub const fn source_generation_digest(&self) -> EvidenceDigest {
        self.source_generation_digest
    }

    /// Returns the actual immutable raw-journal receipt root ingest must attach.
    pub const fn sealed_discovery_capture(&self) -> &SealedProviderCaptureSetReceipt {
        &self.sealed_discovery_capture
    }

    /// Returns the physical receipt identity of the owned immutable raw-journal seal.
    pub const fn sealed_discovery_capture_receipt_digest(&self) -> EvidenceDigest {
        self.sealed_discovery_capture.receipt_digest()
    }

    /// Returns the capture-bound semantic extraction identity root must recompute.
    pub const fn extraction_content_digest(&self) -> EvidenceDigest {
        self.extraction_content_digest
    }

    /// Returns the ordered canonical payload identity for this chunk.
    pub const fn canonical_content_digest(&self) -> EvidenceDigest {
        self.canonical_content_digest
    }

    /// Returns the explicit public marker or registered credential-generation coordinate.
    pub const fn credential_rejoin(&self) -> BlsCredentialRejoin {
        self.credential_rejoin
    }

    /// Returns the exact shared provider-rate declaration identity.
    pub const fn provider_rate_declaration_digest(&self) -> EvidenceDigest {
        self.provider_rate_declaration_digest
    }

    /// Returns the redacted successful doctor evidence bound into activation.
    pub const fn doctor_report_digest(&self) -> EvidenceDigest {
        self.doctor_report_digest
    }

    /// Returns the actual physical doctor-seal identity bound into activation.
    pub const fn sealed_doctor_capture_receipt_digest(&self) -> EvidenceDigest {
        self.sealed_doctor_capture_receipt_digest
    }

    /// Returns the exclusive end of the doctor-backed admission used for this candidate.
    pub const fn activation_expires_at(&self) -> Timestamp {
        self.activation_expires_at
    }

    /// Returns the current sealed-doctor admission root scheduling must rejoin.
    pub const fn activation_candidate_digest(&self) -> EvidenceDigest {
        self.activation_candidate_digest
    }

    /// Returns the non-authoritative identity of this sealed ingest handoff.
    pub const fn candidate_digest(&self) -> EvidenceDigest {
        self.candidate_digest
    }

    /// Returns the exact canonical extraction batch root ingest must publish.
    pub const fn batch(&self) -> &ExtractionBatch {
        &self.batch
    }

    /// Returns BLS-native semantics root must co-persist and join on typed reads.
    pub const fn provider_semantics(&self) -> &BlsCanonicalProviderSemantics {
        &self.provider_semantics
    }

    /// Consumes the candidate into the three root-owned publication inputs.
    pub fn into_root_publication_parts(
        self,
    ) -> (
        ExtractionBatch,
        BlsCanonicalProviderSemantics,
        SealedProviderCaptureSetReceipt,
    ) {
        (
            self.batch,
            self.provider_semantics,
            self.sealed_discovery_capture,
        )
    }

    /// Returns the exact input-aligned local-content revision plan root must durably assign.
    ///
    /// This plan contains evidence only; it cannot allocate revision numbers or publish data.
    pub fn revision_plan(&self) -> Result<ExtractionRevisionPlan, BlsSourceError> {
        self.provider_semantics.validate(&self.batch)?;
        ExtractionRevisionPlan::locally_observed(self.batch.records().len()).map_err(Into::into)
    }
}

fn validate_discovery_component(
    capture: &market_squawk_sources::ProviderCaptureSetReceipt,
    chunk_index: usize,
    object_capture_identity: SourceObjectCaptureIdentity,
    component_request_identity: EvidenceDigest,
    component_content_digest: EvidenceDigest,
    component_observation_digest: EvidenceDigest,
) -> bool {
    let Some(page) = capture.pages().get(chunk_index) else {
        return false;
    };
    let (
        expected_request_identity,
        expected_content_digest,
        expected_observation_digest,
        expected_terminal,
    ) = match capture.terminal() {
        ProviderCaptureTerminalDisposition::StandaloneResponse
            if capture.pages().len() == 1
                && chunk_index == 0
                && capture.request_graph_components().is_empty() =>
        {
            (
                capture.request_set_identity(),
                capture.content_digest(),
                capture.observation_digest(),
                capture.terminal(),
            )
        }
        ProviderCaptureTerminalDisposition::CompleteRequestGraph => {
            let Some(component) = capture.request_graph_components().get(chunk_index) else {
                return false;
            };
            if usize::from(component.ordinal()) != chunk_index
                || usize::from(component.first_page_ordinal()) != chunk_index
                || component.page_count().get() != 1
                || component.total_body_bytes() != page.body_bytes()
                || component.terminal() != ProviderCaptureTerminalDisposition::StandaloneResponse
            {
                return false;
            }
            (
                component.request_set_identity(),
                component.content_digest(),
                component.observation_digest(),
                component.terminal(),
            )
        }
        ProviderCaptureTerminalDisposition::ExhaustedWithoutNextPage
        | ProviderCaptureTerminalDisposition::StandaloneResponse => return false,
    };
    let Some(page_count) = std::num::NonZeroU16::new(1) else {
        return false;
    };
    object_capture_identity
        == (SourceObjectCaptureIdentity::Paged {
            content_digest: expected_content_digest,
            page_count,
            terminal: expected_terminal,
        })
        && component_request_identity == expected_request_identity
        && component_content_digest == expected_content_digest
        && component_observation_digest == expected_observation_digest
        && page.request_identity() == expected_request_identity
}

fn validate_canonical_batch(
    metadata: &SourceMetadata,
    config: &BlsSourceConfig,
    batch: &ExtractionBatch,
    raw_body_digest: EvidenceDigest,
    first_observed_at: Timestamp,
    response_received_at: Timestamp,
    canonical_schema: &SourceIdentifier,
) -> Result<Timestamp, BlsSourceError> {
    let mut canonical_ingested_at = None;
    for record in batch.records() {
        let ResearchObservation::Macro(observation) = serde_json::from_slice(record.payload())
            .map_err(|_| BlsSourceError::InvalidPublication)?
        else {
            return Err(BlsSourceError::InvalidPublication);
        };
        let context = observation.context();
        let provenance = context.provenance();
        let ingested_at = provenance.ingested_at();
        let raw_reference_matches = matches!(
            provenance.payload_reference(),
            PayloadReference::ContentHash(value)
                if value.algorithm() == raw_body_digest.algorithm()
                    && value.digest() == raw_body_digest.bytes()
        );
        let metadata_matches = config
            .series_metadata(observation.series().as_str())
            .is_some_and(|series| series.unit() == observation.unit());
        if record.schema() != canonical_schema
            || record.source_id() != metadata.source_id()
            || record.metadata_revision() != metadata.revision()
            || record.dataset() != config.dataset()
            || record.available_at() != Some(first_observed_at)
            || record.published_time().is_some()
            || record.superseded_time().is_some()
            || provenance.source_id() != metadata.source_id()
            || provenance.instrument_id().is_some()
            || provenance.venue_id().is_some()
            || provenance.source_identifier() != record.revision()
            || provenance.source_timestamp().is_some()
            || provenance.received_at() != response_received_at
            || ingested_at < response_received_at
            || provenance.quality() != DataQuality::OfficialDelayed
            || provenance.availability().conservative_available_at() != Some(first_observed_at)
            || !raw_reference_matches
            || context.time().effective() != record.effective_time()
            || context.time().published().is_some()
            || context.time().revision().get() != 1
            || context.time().superseded().is_some()
            || !metadata_matches
        {
            return Err(BlsSourceError::InvalidPublication);
        }
        match canonical_ingested_at {
            None => canonical_ingested_at = Some(ingested_at),
            Some(expected) if expected == ingested_at => {}
            Some(_) => return Err(BlsSourceError::InvalidPublication),
        }
    }
    canonical_ingested_at.ok_or(BlsSourceError::InvalidPublication)
}

fn canonical_content_digest(batch: &ExtractionBatch) -> Result<EvidenceDigest, BlsSourceError> {
    let records = batch.records();
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/bls-canonical-publication-content/v2\0");
    digest.update(
        u32::try_from(records.len())
            .map_err(|_| BlsSourceError::InvalidPublication)?
            .to_be_bytes(),
    );
    for record in records {
        hash_field(&mut digest, record.schema().as_str().as_bytes())?;
        hash_field(&mut digest, record.revision().as_str().as_bytes())?;
        hash_evidence_digest(&mut digest, record.evidence().content_digest());
        hash_field(&mut digest, record.payload())?;
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn candidate_digest(candidate: &BlsPublicationCandidate) -> Result<EvidenceDigest, BlsSourceError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/bls-publication-candidate/v3\0");
    digest.update(candidate.schema_version.to_be_bytes());
    hash_field(&mut digest, candidate.source_id.as_str().as_bytes())?;
    hash_field(
        &mut digest,
        candidate
            .metadata_revision
            .as_source_identifier()
            .as_str()
            .as_bytes(),
    )?;
    let discovery_request_id = serde_json::to_vec(&candidate.discovery_request_id)
        .map_err(|_| BlsSourceError::InvalidPublication)?;
    hash_field(&mut digest, &discovery_request_id)?;
    for value in [
        &candidate.provider_dataset,
        &candidate.analytical_dataset,
        &candidate.object_id,
        &candidate.canonical_schema,
    ] {
        hash_field(&mut digest, value.as_str().as_bytes())?;
    }
    let canonical_schema_version = serde_json::to_vec(&candidate.canonical_schema_version)
        .map_err(|_| BlsSourceError::InvalidPublication)?;
    hash_field(&mut digest, &canonical_schema_version)?;
    digest.update(candidate.chunk_index.to_be_bytes());
    digest.update(candidate.total_chunks.to_be_bytes());
    digest.update(candidate.canonical_record_count.to_be_bytes());
    digest.update(candidate.first_observed_at.unix_nanos().to_be_bytes());
    digest.update(candidate.response_received_at.unix_nanos().to_be_bytes());
    digest.update(candidate.canonical_ingested_at.unix_nanos().to_be_bytes());
    for value in [
        candidate.schema_extension_requirement_digest,
        candidate.discovery_request_set_identity,
        candidate.discovery_capture_content_digest,
        candidate.discovery_capture_observation_digest,
        candidate.component_request_identity,
        candidate.component_content_digest,
        candidate.component_observation_digest,
        candidate.source_generation_digest,
        candidate.sealed_discovery_capture.receipt_digest(),
        candidate.extraction_content_digest,
        candidate.canonical_content_digest,
        candidate.provider_semantics_digest,
        candidate.provider_rate_declaration_digest,
        candidate.doctor_report_digest,
        candidate.sealed_doctor_capture_receipt_digest,
        candidate.activation_candidate_digest,
    ] {
        hash_evidence_digest(&mut digest, value);
    }
    hash_credential_rejoin(&mut digest, candidate.credential_rejoin);
    digest.update(candidate.activation_expires_at.unix_nanos().to_be_bytes());
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn hash_credential_rejoin(digest: &mut Sha256, value: BlsCredentialRejoin) {
    match value {
        BlsCredentialRejoin::PublicNoCredential => digest.update(b"public-no-credential"),
        BlsCredentialRejoin::RegisteredGeneration(generation) => {
            digest.update(b"registered-generation");
            hash_evidence_digest(digest, generation);
        }
    }
}

fn digest_serialized<T: Serialize + ?Sized>(
    domain: &[u8],
    value: &T,
) -> Result<EvidenceDigest, BlsSourceError> {
    let wire = serde_json::to_vec(value).map_err(|_| BlsSourceError::InvalidPublication)?;
    let mut digest = Sha256::new();
    hash_field(&mut digest, domain)?;
    hash_field(&mut digest, &wire)?;
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn hash_evidence_digest(digest: &mut Sha256, value: EvidenceDigest) {
    digest.update(match value.algorithm() {
        DigestAlgorithm::Sha256 => [1],
        DigestAlgorithm::Blake3 => [2],
    });
    digest.update(value.bytes());
}

fn hash_field(digest: &mut Sha256, value: &[u8]) -> Result<(), BlsSourceError> {
    digest.update(
        u64::try_from(value.len())
            .map_err(|_| BlsSourceError::InvalidPublication)?
            .to_be_bytes(),
    );
    digest.update(value);
    Ok(())
}
