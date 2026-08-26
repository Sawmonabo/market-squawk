use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, ResearchTemporalCoordinate, SchemaVersion, SourceId,
    SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    CURRENT_RESEARCH_RECORD_SCHEMA, ExtractionBatch, ExtractionContentIdentity,
    ExtractionRevisionPlan, ProviderCaptureMaterial, ProviderCaptureTerminalDisposition,
    SealedProviderCaptureSetReceipt, SourceMetadata,
};
use serde::Serialize;
use sha2::{Digest as _, Sha256};

use crate::{
    CensusClocks, CensusDataset, CensusDatasetAcquisition, CensusDatasetContract,
    CensusDoctorReadiness, CensusDoctorReport, CensusDoctorScope, CensusGeographyValue,
    CensusPredicateValue, CensusReportedTime, CensusSourceConfig, CensusSourceError,
    CensusValueState, census_provider_rate_declaration, update_digest_component,
};

const CENSUS_RUNTIME_SCHEMA_VERSION: u16 = 4;
const CENSUS_DOCTOR_ACTIVATION_TTL_NANOS: i64 = 86_400_000_000_000;

/// One exact configured dataset admitted by a Census activation plan.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CensusActivatedDataset {
    provider_dataset: SourceIdentifier,
    analytical_dataset: SourceIdentifier,
    query_digest: EvidenceDigest,
    metadata_request_digests: Box<[EvidenceDigest]>,
}

impl CensusActivatedDataset {
    /// Returns the exact provider-query dataset identity.
    pub const fn provider_dataset(&self) -> &SourceIdentifier {
        &self.provider_dataset
    }

    /// Returns the storage-safe analytical dataset identity.
    pub const fn analytical_dataset(&self) -> &SourceIdentifier {
        &self.analytical_dataset
    }

    /// Returns the key-free data-query digest.
    pub const fn query_digest(&self) -> EvidenceDigest {
        self.query_digest
    }

    /// Returns public metadata request identities in required acquisition order.
    pub fn metadata_request_digests(&self) -> &[EvidenceDigest] {
        &self.metadata_request_digests
    }
}

/// Provider-local activation recipe consumed by root application composition.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CensusActivationPlan {
    schema_version: u16,
    source_id: SourceId,
    metadata_revision: SourceIdentifier,
    configuration_digest: EvidenceDigest,
    provider_rate_declaration_digest: EvidenceDigest,
    datasets: Box<[CensusActivatedDataset]>,
    activation_digest: EvidenceDigest,
}

impl CensusActivationPlan {
    /// Returns the internal source identity.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact source-metadata revision.
    pub const fn metadata_revision(&self) -> &SourceIdentifier {
        &self.metadata_revision
    }

    /// Returns the complete provider configuration digest.
    pub const fn configuration_digest(&self) -> EvidenceDigest {
        self.configuration_digest
    }

    /// Returns the exact declaration the root must register with durable rate authority.
    pub const fn provider_rate_declaration_digest(&self) -> EvidenceDigest {
        self.provider_rate_declaration_digest
    }

    /// Returns admitted provider datasets in stable identity order.
    pub fn datasets(&self) -> &[CensusActivatedDataset] {
        &self.datasets
    }

    /// Returns the identity of the complete activation recipe.
    pub const fn activation_digest(&self) -> EvidenceDigest {
        self.activation_digest
    }

    /// Recomputes all provider-local activation evidence.
    pub fn validate(&self) -> Result<(), CensusSourceError> {
        if self.schema_version != CENSUS_RUNTIME_SCHEMA_VERSION
            || self.datasets.is_empty()
            || self.configuration_digest.bytes() == [0; 32]
            || self.provider_rate_declaration_digest.bytes() == [0; 32]
            || self.activation_digest.bytes() == [0; 32]
            || self.datasets.windows(2).any(|pair| {
                pair[0].provider_dataset >= pair[1].provider_dataset
                    || pair[0].analytical_dataset == pair[1].analytical_dataset
            })
            || self.datasets.iter().any(|dataset| {
                dataset.query_digest.bytes() == [0; 32]
                    || dataset.metadata_request_digests.is_empty()
                    || dataset
                        .metadata_request_digests
                        .iter()
                        .any(|digest| digest.bytes() == [0; 32])
            })
        {
            return Err(CensusSourceError::InvalidConfiguration);
        }
        let expected = digest_serialized(
            b"market-squawk/census-activation/v4",
            &(
                self.schema_version,
                &self.source_id,
                &self.metadata_revision,
                self.configuration_digest,
                self.provider_rate_declaration_digest,
                &self.datasets,
            ),
        )?;
        if expected != self.activation_digest {
            return Err(CensusSourceError::InvalidConfiguration);
        }
        Ok(())
    }

    fn dataset(&self, provider_dataset: &SourceIdentifier) -> Option<&CensusActivatedDataset> {
        self.datasets
            .binary_search_by(|candidate| candidate.provider_dataset.cmp(provider_dataset))
            .ok()
            .map(|index| &self.datasets[index])
    }
}

/// Provider readiness candidate bound to the exact doctor capture.
///
/// This is activation evidence only. It grants no manifest, restart, or query authority.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CensusActivationCandidate {
    plan: CensusActivationPlan,
    doctor_report_digest: EvidenceDigest,
    doctor_capture_receipt_digest: EvidenceDigest,
    activated_at: Timestamp,
    expires_at: Timestamp,
    candidate_digest: EvidenceDigest,
}

impl CensusActivationCandidate {
    /// Admits one current doctor report only after its exact raw response was sealed.
    pub(crate) fn try_new(
        plan: CensusActivationPlan,
        doctor: &CensusDoctorReport,
        sealed_capture: &SealedProviderCaptureSetReceipt,
        activated_at: Timestamp,
    ) -> Result<Self, CensusSourceError> {
        plan.validate()?;
        validate_doctor_capture(&plan, doctor, sealed_capture)?;
        let expires_at = doctor
            .checked_at()
            .checked_add_nanos(CENSUS_DOCTOR_ACTIVATION_TTL_NANOS)
            .map_err(|_| CensusSourceError::InvalidConfiguration)?;
        if activated_at < doctor.checked_at() || activated_at >= expires_at {
            return Err(CensusSourceError::InvalidConfiguration);
        }
        let doctor_report_digest = doctor.report_digest();
        let doctor_capture_receipt_digest = sealed_capture.receipt_digest();
        let candidate_digest = digest_serialized(
            b"market-squawk/census-activation-candidate/v4",
            &(
                plan.activation_digest,
                doctor_report_digest,
                doctor_capture_receipt_digest,
                activated_at,
                expires_at,
            ),
        )?;
        Ok(Self {
            plan,
            doctor_report_digest,
            doctor_capture_receipt_digest,
            activated_at,
            expires_at,
            candidate_digest,
        })
    }

    /// Reopens doctor and sealed-capture evidence at the exact operation time.
    pub fn validate(
        &self,
        doctor: &CensusDoctorReport,
        sealed_capture: &SealedProviderCaptureSetReceipt,
        operation_at: Timestamp,
    ) -> Result<(), CensusSourceError> {
        self.plan.validate()?;
        validate_doctor_capture(&self.plan, doctor, sealed_capture)?;
        let expected = digest_serialized(
            b"market-squawk/census-activation-candidate/v4",
            &(
                self.plan.activation_digest,
                self.doctor_report_digest,
                self.doctor_capture_receipt_digest,
                self.activated_at,
                self.expires_at,
            ),
        )?;
        if doctor.report_digest() != self.doctor_report_digest
            || sealed_capture.receipt_digest() != self.doctor_capture_receipt_digest
            || operation_at < self.activated_at
            || operation_at >= self.expires_at
            || expected != self.candidate_digest
        {
            return Err(CensusSourceError::InvalidConfiguration);
        }
        Ok(())
    }

    /// Returns the exact provider activation recipe.
    pub const fn plan(&self) -> &CensusActivationPlan {
        &self.plan
    }

    /// Returns the activation-evidence identity root scheduling must retain.
    pub const fn candidate_digest(&self) -> EvidenceDigest {
        self.candidate_digest
    }

    /// Returns the exact sealed doctor receipt identity used for admission.
    pub const fn doctor_capture_receipt_digest(&self) -> EvidenceDigest {
        self.doctor_capture_receipt_digest
    }

    /// Returns the exact redacted doctor report identity used for admission.
    pub const fn doctor_report_digest(&self) -> EvidenceDigest {
        self.doctor_report_digest
    }

    /// Returns when this in-process activation was admitted.
    pub const fn activated_at(&self) -> Timestamp {
        self.activated_at
    }

    /// Returns the end of the bounded doctor admission window.
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }
}

fn validate_doctor_capture(
    plan: &CensusActivationPlan,
    doctor: &CensusDoctorReport,
    sealed_capture: &SealedProviderCaptureSetReceipt,
) -> Result<(), CensusSourceError> {
    let capture = sealed_capture.capture();
    let page = capture.pages().first().ok_or(CensusSourceError::Protocol)?;
    let doctor_dataset = crate::doctor::doctor_dataset_identity()?;
    if doctor.readiness() != CensusDoctorReadiness::Available
        || doctor.scope() != CensusDoctorScope::Acs2024OneYearUnitedStatesPopulation
        || doctor.source_id() != &plan.source_id
        || doctor.metadata_revision() != &plan.metadata_revision
        || doctor.configuration_digest() != plan.configuration_digest
        || doctor.provider_rate_declaration_digest() != plan.provider_rate_declaration_digest
        || doctor.native_schema().as_str() != "census-json-matrix-acs1-us-population"
        || doctor.native_schema_version() == 0
        || doctor.native_schema_fingerprint().bytes() == [0; 32]
        || doctor.response_status() != 200
        || doctor.response_bytes() == 0
        || doctor.report_digest().bytes() == [0; 32]
        || sealed_capture.receipt_digest().bytes() == [0; 32]
        || capture.source_id() != &plan.source_id
        || capture.metadata_revision().as_source_identifier() != &plan.metadata_revision
        || capture.dataset() != &doctor_dataset
        || capture.terminal() != ProviderCaptureTerminalDisposition::StandaloneResponse
        || capture.request_set_identity() != doctor.request_digest()
        || capture.observation_digest() != doctor.capture_observation_digest()
        || capture.pages().len() != 1
        || page.request_identity() != doctor.request_digest()
        || page.http_status() != doctor.response_status()
        || page.body_digest() != doctor.response_digest()
        || page.body_bytes() != doctor.response_bytes()
        || page.received_at() != doctor.checked_at()
    {
        return Err(CensusSourceError::Protocol);
    }
    Ok(())
}

/// Exact role and order of one raw response needed by a Census publication.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "ordinal")]
pub enum CensusCaptureRole {
    /// One complete ordered graph: public metadata responses followed by credentialed data.
    CompleteMetadataAndDataGraph,
}

/// One exact raw-capture dependency that must be sealed before canonical publication.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CensusCaptureBinding {
    ordinal: u32,
    role: CensusCaptureRole,
    request_digest: EvidenceDigest,
    content_digest: EvidenceDigest,
    observation_digest: EvidenceDigest,
    response_bytes: u64,
    received_at: Timestamp,
    component_count: u32,
}

impl CensusCaptureBinding {
    /// Returns the zero-based capture order.
    pub const fn ordinal(&self) -> u32 {
        self.ordinal
    }

    /// Returns metadata/data role.
    pub const fn role(&self) -> CensusCaptureRole {
        self.role
    }

    /// Returns the exact key-free request digest.
    pub const fn request_digest(&self) -> EvidenceDigest {
        self.request_digest
    }

    /// Returns the response-set content digest.
    pub const fn content_digest(&self) -> EvidenceDigest {
        self.content_digest
    }

    /// Returns the complete response receipt identity.
    pub const fn observation_digest(&self) -> EvidenceDigest {
        self.observation_digest
    }

    /// Returns exact retained response bytes.
    pub const fn response_bytes(&self) -> u64 {
        self.response_bytes
    }

    /// Returns local receipt time.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns exact metadata-plus-data request-graph component count.
    pub const fn component_count(&self) -> u32 {
        self.component_count
    }
}

/// Full provider identity bound to one ordered canonical macro observation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CensusCanonicalObservationBinding {
    canonical_ordinal: u64,
    dataset: CensusDataset,
    provider_variable: SourceIdentifier,
    geography: CensusGeographyValue,
    predicates: Box<[CensusPredicateValue]>,
    reported_time: Option<CensusReportedTime>,
    effective_time: ResearchTemporalCoordinate,
    published_time: Option<ResearchTemporalCoordinate>,
    value_state: CensusValueState,
    clocks: CensusClocks,
    family_digest: EvidenceDigest,
    content_digest: EvidenceDigest,
    row_digest: EvidenceDigest,
    request_digest: EvidenceDigest,
    response_digest: EvidenceDigest,
    metadata_digest: EvidenceDigest,
    canonical_series: SourceIdentifier,
    canonical_source_identifier: SourceIdentifier,
    canonical_unit: SourceIdentifier,
}

impl CensusCanonicalObservationBinding {
    #[allow(
        clippy::too_many_arguments,
        reason = "provider identity, clocks, revision evidence, and canonical binding stay explicit"
    )]
    pub(crate) fn new(
        canonical_ordinal: u64,
        observation: &crate::CensusObservation,
        effective_time: ResearchTemporalCoordinate,
        canonical_series: SourceIdentifier,
        canonical_source_identifier: SourceIdentifier,
        canonical_unit: SourceIdentifier,
    ) -> Self {
        Self {
            canonical_ordinal,
            dataset: observation.dataset().clone(),
            provider_variable: observation.variable().clone(),
            geography: observation.geography().clone(),
            predicates: observation.predicates().to_vec().into_boxed_slice(),
            reported_time: observation.reported_time().cloned(),
            effective_time,
            published_time: None,
            value_state: observation.value().clone(),
            clocks: observation.clocks().clone(),
            family_digest: evidence_digest(observation.revision_candidate().family_digest()),
            content_digest: evidence_digest(observation.revision_candidate().content_digest()),
            row_digest: evidence_digest(observation.row_digest()),
            request_digest: evidence_digest(observation.request_digest()),
            response_digest: evidence_digest(observation.response_payload_digest()),
            metadata_digest: evidence_digest(observation.metadata_payload_digest()),
            canonical_series,
            canonical_source_identifier,
            canonical_unit,
        }
    }

    /// Returns the canonical record ordinal in the extraction batch.
    pub const fn canonical_ordinal(&self) -> u64 {
        self.canonical_ordinal
    }

    /// Returns exact Census dataset vintage and path.
    pub const fn dataset(&self) -> &CensusDataset {
        &self.dataset
    }

    /// Returns the exact provider variable.
    pub const fn provider_variable(&self) -> &SourceIdentifier {
        &self.provider_variable
    }

    /// Returns the exact row geography.
    pub const fn geography(&self) -> &CensusGeographyValue {
        &self.geography
    }

    /// Returns the exact non-geographic request predicates.
    pub fn predicates(&self) -> &[CensusPredicateValue] {
        &self.predicates
    }

    /// Returns the source-reported period without invented precision.
    pub const fn reported_time(&self) -> Option<&CensusReportedTime> {
        self.reported_time.as_ref()
    }

    /// Returns the full family identity over dataset, vintage, variable, geography, predicates,
    /// and time.
    pub const fn family_digest(&self) -> EvidenceDigest {
        self.family_digest
    }

    /// Returns the locally observed content candidate for shared revision assignment.
    pub const fn content_digest(&self) -> EvidenceDigest {
        self.content_digest
    }

    /// Returns the exact canonical effective coordinate.
    pub const fn effective_time(&self) -> &ResearchTemporalCoordinate {
        &self.effective_time
    }

    /// Returns provider publication time when explicitly supplied; Census rows currently do not.
    pub const fn published_time(&self) -> Option<&ResearchTemporalCoordinate> {
        self.published_time.as_ref()
    }

    /// Returns closed observed, missing, annotated, or invalid provider state.
    pub const fn value_state(&self) -> &CensusValueState {
        &self.value_state
    }

    /// Returns local receipt, decode, ingestion, and availability clocks.
    pub const fn clocks(&self) -> &CensusClocks {
        &self.clocks
    }

    /// Returns exact closed provider-native row identity.
    pub const fn row_digest(&self) -> EvidenceDigest {
        self.row_digest
    }

    /// Returns exact key-free request identity.
    pub const fn request_digest(&self) -> EvidenceDigest {
        self.request_digest
    }

    /// Returns exact response payload identity.
    pub const fn response_digest(&self) -> EvidenceDigest {
        self.response_digest
    }

    /// Returns exact variable-metadata identity.
    pub const fn metadata_digest(&self) -> EvidenceDigest {
        self.metadata_digest
    }

    /// Returns the canonical series identity.
    pub const fn canonical_series(&self) -> &SourceIdentifier {
        &self.canonical_series
    }

    /// Returns the canonical provenance source identifier.
    pub const fn canonical_source_identifier(&self) -> &SourceIdentifier {
        &self.canonical_source_identifier
    }

    /// Returns the reviewed canonical unit identity.
    pub const fn canonical_unit(&self) -> &SourceIdentifier {
        &self.canonical_unit
    }
}

/// All exact raw, native-identity, canonical, and shared-quota evidence required before
/// publishing one immutable Census generation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CensusPublicationPlan {
    schema_version: u16,
    source_id: SourceId,
    metadata_revision: SourceIdentifier,
    provider_dataset: SourceIdentifier,
    analytical_dataset: SourceIdentifier,
    configuration_digest: EvidenceDigest,
    provider_rate_declaration_digest: EvidenceDigest,
    query_digest: EvidenceDigest,
    metadata_bundle_digest: EvidenceDigest,
    data_response_digest: EvidenceDigest,
    extraction_content_digest: EvidenceDigest,
    prepared_at: Timestamp,
    captures: Box<[CensusCaptureBinding]>,
    observations: Box<[CensusCanonicalObservationBinding]>,
    publication_identity: EvidenceDigest,
}

impl CensusPublicationPlan {
    /// Returns source identity.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns provider-query dataset identity.
    pub const fn provider_dataset(&self) -> &SourceIdentifier {
        &self.provider_dataset
    }

    /// Returns analytical storage dataset identity.
    pub const fn analytical_dataset(&self) -> &SourceIdentifier {
        &self.analytical_dataset
    }

    /// Returns exact configuration identity.
    pub const fn configuration_digest(&self) -> EvidenceDigest {
        self.configuration_digest
    }

    /// Returns exact shared quota declaration identity.
    pub const fn provider_rate_declaration_digest(&self) -> EvidenceDigest {
        self.provider_rate_declaration_digest
    }

    /// Returns the key-free provider data-query digest.
    pub const fn query_digest(&self) -> EvidenceDigest {
        self.query_digest
    }

    /// Returns the complete public metadata bundle identity.
    pub const fn metadata_bundle_digest(&self) -> EvidenceDigest {
        self.metadata_bundle_digest
    }

    /// Returns the exact credential-bearing data response identity.
    pub const fn data_response_digest(&self) -> EvidenceDigest {
        self.data_response_digest
    }

    /// Returns exact raw response dependencies in sealing order.
    pub fn captures(&self) -> &[CensusCaptureBinding] {
        &self.captures
    }

    /// Returns exact provider-to-canonical observation bindings in record order.
    pub fn observations(&self) -> &[CensusCanonicalObservationBinding] {
        &self.observations
    }

    /// Returns when canonical preparation completed locally.
    pub const fn prepared_at(&self) -> Timestamp {
        self.prepared_at
    }

    /// Returns stable semantic extraction identity.
    pub const fn extraction_content_digest(&self) -> EvidenceDigest {
        self.extraction_content_digest
    }

    /// Returns the complete publication-plan identity.
    pub const fn publication_identity(&self) -> EvidenceDigest {
        self.publication_identity
    }

    /// Recomputes the complete plan identity and structural ordering.
    pub fn validate(&self) -> Result<(), CensusSourceError> {
        if self.schema_version != CENSUS_RUNTIME_SCHEMA_VERSION
            || self.captures.len() != 1
            || self.observations.is_empty()
            || self.captures[0].role != CensusCaptureRole::CompleteMetadataAndDataGraph
            || self.captures[0].component_count < 2
            || self.captures[0].response_bytes == 0
            || self.configuration_digest.bytes() == [0; 32]
            || self.provider_rate_declaration_digest.bytes() == [0; 32]
            || self.query_digest.bytes() == [0; 32]
            || self.metadata_bundle_digest.bytes() == [0; 32]
            || self.data_response_digest.bytes() == [0; 32]
            || self.extraction_content_digest.bytes() == [0; 32]
            || self.publication_identity.bytes() == [0; 32]
            || self.observations.iter().any(|binding| {
                binding.family_digest.bytes() == [0; 32]
                    || binding.content_digest.bytes() == [0; 32]
                    || binding.row_digest.bytes() == [0; 32]
                    || binding.request_digest != self.query_digest
                    || binding.response_digest != self.data_response_digest
                    || binding.clocks.ingested_at() > self.prepared_at
            })
            || self
                .captures
                .iter()
                .enumerate()
                .any(|(index, capture)| capture.ordinal as usize != index)
            || self
                .observations
                .iter()
                .enumerate()
                .any(|(index, binding)| binding.canonical_ordinal as usize != index)
        {
            return Err(CensusSourceError::Protocol);
        }
        let expected = publication_identity(self)?;
        if expected != self.publication_identity {
            return Err(CensusSourceError::Protocol);
        }
        Ok(())
    }
}

/// Canonical publication input whose exact provider request graph has already been sealed.
///
/// This value deliberately carries no generation, manifest, revision assignment, checkpoint, or
/// query authority. Only the application/data publication authority may consume it, commit an
/// immutable manifest, and mint restart or point-in-time read receipts.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CensusPublicationCandidate {
    plan: CensusPublicationPlan,
    activation_candidate_digest: EvidenceDigest,
    canonical_schema: SourceIdentifier,
    canonical_schema_version: SchemaVersion,
    canonical_schema_fingerprint: EvidenceDigest,
    sealed_capture: SealedProviderCaptureSetReceipt,
    candidate_digest: EvidenceDigest,
}

impl CensusPublicationCandidate {
    /// Binds canonical evidence to the actual shared sealed-journal receipt.
    pub fn try_new(
        plan: CensusPublicationPlan,
        sealed_capture: &SealedProviderCaptureSetReceipt,
        activation: &CensusActivationCandidate,
    ) -> Result<Self, CensusSourceError> {
        plan.validate()?;
        validate_sealed_capture(&plan, sealed_capture)?;
        validate_publication_activation(&plan, activation, plan.prepared_at)?;
        let canonical_schema = SourceIdentifier::try_from(CURRENT_RESEARCH_RECORD_SCHEMA)
            .map_err(|_| CensusSourceError::Protocol)?;
        let canonical_schema_version = SchemaVersion::CURRENT;
        let canonical_schema_fingerprint =
            canonical_schema_fingerprint(&canonical_schema, canonical_schema_version)?;
        let sealed_capture_receipt_digest = sealed_capture.receipt_digest();
        let candidate_digest = digest_serialized(
            b"market-squawk/census-publication-candidate/v4",
            &(
                plan.publication_identity,
                activation.candidate_digest,
                &canonical_schema,
                canonical_schema_version,
                canonical_schema_fingerprint,
                sealed_capture_receipt_digest,
            ),
        )?;
        Ok(Self {
            plan,
            activation_candidate_digest: activation.candidate_digest,
            canonical_schema,
            canonical_schema_version,
            canonical_schema_fingerprint,
            sealed_capture: sealed_capture.clone(),
            candidate_digest,
        })
    }

    /// Reopens every provider-local invariant against the owned shared sealed receipt before
    /// ingest.
    pub fn validate(
        &self,
        activation: &CensusActivationCandidate,
        operation_at: Timestamp,
    ) -> Result<(), CensusSourceError> {
        self.plan.validate()?;
        validate_sealed_capture(&self.plan, &self.sealed_capture)?;
        validate_publication_activation(&self.plan, activation, operation_at)?;
        let expected = digest_serialized(
            b"market-squawk/census-publication-candidate/v4",
            &(
                self.plan.publication_identity,
                self.activation_candidate_digest,
                &self.canonical_schema,
                self.canonical_schema_version,
                self.canonical_schema_fingerprint,
                self.sealed_capture.receipt_digest(),
            ),
        )?;
        if self.canonical_schema.as_str() != CURRENT_RESEARCH_RECORD_SCHEMA
            || self.canonical_schema_version != SchemaVersion::CURRENT
            || self.canonical_schema_fingerprint
                != canonical_schema_fingerprint(
                    &self.canonical_schema,
                    self.canonical_schema_version,
                )?
            || self.activation_candidate_digest != activation.candidate_digest
            || self.sealed_capture.receipt_digest().bytes() == [0; 32]
            || expected != self.candidate_digest
        {
            return Err(CensusSourceError::Protocol);
        }
        Ok(())
    }

    /// Rejoins this candidate to the exact graph-bound extraction batch root will ingest.
    pub fn validate_batch(&self, batch: &ExtractionBatch) -> Result<(), CensusSourceError> {
        let identity = ExtractionContentIdentity::try_from_batch(batch)
            .map_err(|_| CensusSourceError::Protocol)?;
        if identity.digest() != self.plan.extraction_content_digest
            || identity.record_count() != self.plan.observations.len()
            || batch.records().iter().any(|record| {
                record.source_id() != &self.plan.source_id
                    || record.metadata_revision().as_source_identifier()
                        != &self.plan.metadata_revision
                    || record.dataset() != &self.plan.provider_dataset
                    || record.schema() != &self.canonical_schema
            })
        {
            return Err(CensusSourceError::Protocol);
        }
        Ok(())
    }

    /// Builds input-aligned locally observed revision evidence for root's durable authority.
    ///
    /// This is not an assignment receipt: root remains the only component that may assign or
    /// persist canonical revision numbers.
    pub fn revision_plan(
        &self,
        batch: &ExtractionBatch,
    ) -> Result<ExtractionRevisionPlan, CensusSourceError> {
        self.validate_batch(batch)?;
        ExtractionRevisionPlan::locally_observed(batch.records().len()).map_err(Into::into)
    }

    /// Returns the full canonical/native publication plan.
    pub const fn plan(&self) -> &CensusPublicationPlan {
        &self.plan
    }

    /// Returns the exact source identity root ingest must rejoin.
    pub const fn source_id(&self) -> &SourceId {
        &self.plan.source_id
    }

    /// Returns the exact source-metadata revision root ingest must rejoin.
    pub const fn metadata_revision(&self) -> &SourceIdentifier {
        &self.plan.metadata_revision
    }

    /// Returns the provider request-graph dataset identity.
    pub const fn provider_dataset(&self) -> &SourceIdentifier {
        &self.plan.provider_dataset
    }

    /// Returns the analytical dataset root publication must reserve.
    pub const fn analytical_dataset(&self) -> &SourceIdentifier {
        &self.plan.analytical_dataset
    }

    /// Returns the exact canonical schema name.
    pub const fn canonical_schema(&self) -> &SourceIdentifier {
        &self.canonical_schema
    }

    /// Returns the exact canonical schema version.
    pub const fn canonical_schema_version(&self) -> SchemaVersion {
        self.canonical_schema_version
    }

    /// Returns the exact canonical payload contract fingerprint.
    pub const fn canonical_schema_fingerprint(&self) -> EvidenceDigest {
        self.canonical_schema_fingerprint
    }

    /// Returns the graph-bound extraction content identity root ingest must recompute.
    pub const fn extraction_content_digest(&self) -> EvidenceDigest {
        self.plan.extraction_content_digest
    }

    /// Returns the exact shared provider-rate declaration identity.
    pub const fn provider_rate_declaration_digest(&self) -> EvidenceDigest {
        self.plan.provider_rate_declaration_digest
    }

    /// Returns the actual sealed request graph root ingest must attach to publication.
    pub const fn sealed_capture(&self) -> &SealedProviderCaptureSetReceipt {
        &self.sealed_capture
    }

    /// Returns the actual sealed request-graph receipt identity.
    pub const fn sealed_capture_receipt_digest(&self) -> EvidenceDigest {
        self.sealed_capture.receipt_digest()
    }

    /// Returns the non-authoritative candidate identity.
    pub const fn candidate_digest(&self) -> EvidenceDigest {
        self.candidate_digest
    }

    /// Returns the exact doctor-backed activation candidate root scheduling must rejoin.
    pub const fn activation_candidate_digest(&self) -> EvidenceDigest {
        self.activation_candidate_digest
    }

    /// Returns the exact number of canonical records expected at root ingest.
    pub fn canonical_record_count(&self) -> usize {
        self.plan.observations.len()
    }
}

fn canonical_schema_fingerprint(
    schema: &SourceIdentifier,
    version: SchemaVersion,
) -> Result<EvidenceDigest, CensusSourceError> {
    digest_serialized(
        b"market-squawk/census-canonical-schema/v1",
        &(schema, version, "research_observation.macro"),
    )
}

fn validate_publication_activation(
    plan: &CensusPublicationPlan,
    activation: &CensusActivationCandidate,
    operation_at: Timestamp,
) -> Result<(), CensusSourceError> {
    activation.plan.validate()?;
    let dataset = activation
        .plan
        .dataset(&plan.provider_dataset)
        .ok_or(CensusSourceError::InvalidConfiguration)?;
    if activation.candidate_digest.bytes() == [0; 32]
        || plan.source_id != activation.plan.source_id
        || plan.metadata_revision != activation.plan.metadata_revision
        || plan.configuration_digest != activation.plan.configuration_digest
        || plan.provider_rate_declaration_digest != activation.plan.provider_rate_declaration_digest
        || plan.analytical_dataset != dataset.analytical_dataset
        || plan.query_digest != dataset.query_digest
        || plan.prepared_at < activation.activated_at
        || plan.prepared_at >= activation.expires_at
        || operation_at < plan.prepared_at
        || operation_at >= activation.expires_at
    {
        return Err(CensusSourceError::InvalidConfiguration);
    }
    Ok(())
}

fn validate_sealed_capture(
    plan: &CensusPublicationPlan,
    sealed_capture: &SealedProviderCaptureSetReceipt,
) -> Result<(), CensusSourceError> {
    let expected = plan.captures.first().ok_or(CensusSourceError::Protocol)?;
    let capture = sealed_capture.capture();
    if plan.captures.len() != 1
        || capture.source_id() != &plan.source_id
        || capture.metadata_revision().as_source_identifier() != &plan.metadata_revision
        || capture.dataset() != &plan.provider_dataset
        || capture.terminal() != ProviderCaptureTerminalDisposition::CompleteRequestGraph
        || capture.request_set_identity() != expected.request_digest
        || capture.content_digest() != expected.content_digest
        || capture.observation_digest() != expected.observation_digest
        || capture.total_body_bytes() != expected.response_bytes
        || capture.request_graph_components().len() != expected.component_count as usize
        || sealed_capture.receipt_digest().bytes() == [0; 32]
    {
        return Err(CensusSourceError::Protocol);
    }
    Ok(())
}

pub(crate) fn build_activation_plan(
    metadata: &SourceMetadata,
    config: &CensusSourceConfig,
) -> Result<CensusActivationPlan, CensusSourceError> {
    let budget = metadata
        .budget_policy()
        .ok_or(CensusSourceError::InvalidMetadata)?;
    let subject = budget
        .scope()
        .authorization_account()
        .ok_or(CensusSourceError::InvalidMetadata)?;
    let rate = census_provider_rate_declaration(subject)?;
    if rate.policy() != budget {
        return Err(CensusSourceError::InvalidMetadata);
    }
    let datasets = config
        .contracts()
        .iter()
        .map(|contract| CensusActivatedDataset {
            provider_dataset: contract.dataset_id().clone(),
            analytical_dataset: contract.analytical_dataset_id().clone(),
            query_digest: evidence_digest(contract.query().request_digest()),
            metadata_request_digests: contract
                .metadata_requests()
                .iter()
                .map(|request| evidence_digest(request.request_digest()))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let schema_version = CENSUS_RUNTIME_SCHEMA_VERSION;
    let source_id = metadata.source_id().clone();
    let metadata_revision = metadata.revision().as_source_identifier().clone();
    let configuration_digest = config.configuration_digest();
    let provider_rate_declaration_digest = rate.declaration_digest();
    let activation_digest = digest_serialized(
        b"market-squawk/census-activation/v4",
        &(
            schema_version,
            &source_id,
            &metadata_revision,
            configuration_digest,
            provider_rate_declaration_digest,
            &datasets,
        ),
    )?;
    Ok(CensusActivationPlan {
        schema_version,
        source_id,
        metadata_revision,
        configuration_digest,
        provider_rate_declaration_digest,
        datasets,
        activation_digest,
    })
}

pub(crate) fn build_publication_plan(
    metadata: &SourceMetadata,
    config: &CensusSourceConfig,
    contract: &CensusDatasetContract,
    acquisition: &CensusDatasetAcquisition,
    batch: &ExtractionBatch,
    capture_material: &ProviderCaptureMaterial,
    observations: Box<[CensusCanonicalObservationBinding]>,
) -> Result<CensusPublicationPlan, CensusSourceError> {
    let activation = build_activation_plan(metadata, config)?;
    let extraction = ExtractionContentIdentity::try_from_batch(batch)
        .map_err(|_| CensusSourceError::Protocol)?;
    let receipt = capture_material.receipt();
    if extraction.record_count() != observations.len()
        || receipt.source_id() != metadata.source_id()
        || receipt.metadata_revision() != metadata.revision()
        || receipt.dataset() != contract.dataset_id()
        || receipt.terminal() != ProviderCaptureTerminalDisposition::CompleteRequestGraph
        || receipt.request_graph_components().len()
            != acquisition.metadata().documents().len().saturating_add(1)
    {
        return Err(CensusSourceError::Protocol);
    }
    let received_at = receipt
        .pages()
        .last()
        .map(|page| page.received_at())
        .ok_or(CensusSourceError::Protocol)?;
    let capture_bindings = vec![CensusCaptureBinding {
        ordinal: 0,
        role: CensusCaptureRole::CompleteMetadataAndDataGraph,
        request_digest: receipt.request_set_identity(),
        content_digest: receipt.content_digest(),
        observation_digest: receipt.observation_digest(),
        response_bytes: receipt.total_body_bytes(),
        received_at,
        component_count: u32::try_from(receipt.request_graph_components().len())
            .map_err(|_| CensusSourceError::Protocol)?,
    }]
    .into_boxed_slice();
    let mut plan = CensusPublicationPlan {
        schema_version: CENSUS_RUNTIME_SCHEMA_VERSION,
        source_id: metadata.source_id().clone(),
        metadata_revision: metadata.revision().as_source_identifier().clone(),
        provider_dataset: contract.dataset_id().clone(),
        analytical_dataset: contract.analytical_dataset_id().clone(),
        configuration_digest: config.configuration_digest(),
        provider_rate_declaration_digest: activation.provider_rate_declaration_digest,
        query_digest: evidence_digest(contract.query().request_digest()),
        metadata_bundle_digest: evidence_digest(acquisition.metadata().content_digest()),
        data_response_digest: evidence_digest(acquisition.data().page().response_payload_digest()),
        extraction_content_digest: extraction.digest(),
        prepared_at: acquisition.data().page().clocks().ingested_at(),
        captures: capture_bindings,
        observations,
        publication_identity: evidence_digest([0; 32]),
    };
    plan.publication_identity = publication_identity(&plan)?;
    plan.validate()?;
    Ok(plan)
}

fn publication_identity(plan: &CensusPublicationPlan) -> Result<EvidenceDigest, CensusSourceError> {
    #[derive(Serialize)]
    #[serde(deny_unknown_fields)]
    struct PublicationIdentityWire<'a> {
        schema_version: u16,
        source_id: &'a SourceId,
        metadata_revision: &'a SourceIdentifier,
        provider_dataset: &'a SourceIdentifier,
        analytical_dataset: &'a SourceIdentifier,
        configuration_digest: EvidenceDigest,
        provider_rate_declaration_digest: EvidenceDigest,
        query_digest: EvidenceDigest,
        metadata_bundle_digest: EvidenceDigest,
        data_response_digest: EvidenceDigest,
        extraction_content_digest: EvidenceDigest,
        prepared_at: Timestamp,
        captures: &'a [CensusCaptureBinding],
        observations: &'a [CensusCanonicalObservationBinding],
    }
    digest_serialized(
        b"market-squawk/census-publication-plan/v4",
        &PublicationIdentityWire {
            schema_version: plan.schema_version,
            source_id: &plan.source_id,
            metadata_revision: &plan.metadata_revision,
            provider_dataset: &plan.provider_dataset,
            analytical_dataset: &plan.analytical_dataset,
            configuration_digest: plan.configuration_digest,
            provider_rate_declaration_digest: plan.provider_rate_declaration_digest,
            query_digest: plan.query_digest,
            metadata_bundle_digest: plan.metadata_bundle_digest,
            data_response_digest: plan.data_response_digest,
            extraction_content_digest: plan.extraction_content_digest,
            prepared_at: plan.prepared_at,
            captures: &plan.captures,
            observations: &plan.observations,
        },
    )
}

fn digest_serialized<T: Serialize + ?Sized>(
    domain: &[u8],
    value: &T,
) -> Result<EvidenceDigest, CensusSourceError> {
    let wire = serde_json::to_vec(value).map_err(|_| CensusSourceError::Protocol)?;
    let mut digest = Sha256::new();
    update_digest_component(&mut digest, domain);
    update_digest_component(&mut digest, &wire);
    Ok(evidence_digest(digest.finalize().into()))
}

fn evidence_digest(bytes: [u8; 32]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, bytes)
}
