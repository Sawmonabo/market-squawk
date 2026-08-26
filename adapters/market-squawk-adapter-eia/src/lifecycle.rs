//! Capture-first doctor, fixed private-use policy, and finite exact-generation activation.

use market_squawk_domain::{
    EvidenceDigest, MetadataRevision, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    ExtractionAuthority, ProviderCaptureMaterial, ProviderCaptureSetReceipt,
    SealedProviderCaptureSetReceipt, SourceMetadata, SourceMetadataProvider,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::types::digest_parts;
use crate::{
    EiaApiVersion, EiaClockField, EiaDataAcquisitionCursor, EiaDataFieldContract,
    EiaDataPageSealRejoin, EiaDataPageTransition, EiaDataQuery, EiaDataRetrievalSealRejoin,
    EiaDatasetContract, EiaDatasetContractInput, EiaDigest, EiaError, EiaFieldId,
    EiaMetadataRequest, EiaPendingDataPage, EiaSourceTransport, EiaSourceTransportError,
    EiaValueKind,
};

const EIA_DOCTOR_MAX_AGE_NANOS: i64 = 86_400_000_000_000;

fn required_data_pages(total: u64, page_length: u16) -> Result<u64, EiaLifecycleError> {
    let page_length = u64::from(page_length);
    total
        .checked_add(
            page_length
                .checked_sub(1)
                .ok_or(EiaLifecycleError::InvalidEvidence)?,
        )
        .ok_or(EiaLifecycleError::InvalidEvidence)
        .map(|rounded| rounded / page_length)
}

/// Closed operations in the adapter's fixed private-research matrix.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EiaResearchOperation {
    /// Retrieve official provider responses for private research.
    Retrieve,
    /// Display results inside the owner's private application.
    PrivateDisplay,
    /// Persist exact responses and derived data inside owner-controlled storage.
    PrivatePersist,
    /// Transform data into private features or derived research artifacts.
    PrivateTransform,
    /// Run private point-in-time backtests.
    PrivateBacktest,
    /// Run private forecasts.
    PrivateForecast,
    /// Train private models.
    PrivateModelTraining,
    /// Operate private models and signals.
    PrivateModelOperation,
    /// Sell EIA data or a data substitute.
    Sale,
    /// Redistribute EIA data outside the owner's private research boundary.
    Redistribution,
}

/// Immutable provider-local policy matrix; this is not evidence that root granted those rights.
///
/// This type intentionally has no configurable commerce flags: retrieval, private display,
/// persistence, transformation, backtesting, forecasting, and model work are admitted; sale and
/// redistribution are permanently denied at this adapter boundary.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EiaPrivateResearchPolicy {}

impl EiaPrivateResearchPolicy {
    /// Returns the sole provider-local usage policy.
    pub const fn personal_research() -> Self {
        Self {}
    }

    /// Admits an exact operation or fails closed for sale/redistribution.
    pub const fn authorize(self, operation: EiaResearchOperation) -> Result<(), EiaLifecycleError> {
        match operation {
            EiaResearchOperation::Retrieve
            | EiaResearchOperation::PrivateDisplay
            | EiaResearchOperation::PrivatePersist
            | EiaResearchOperation::PrivateTransform
            | EiaResearchOperation::PrivateBacktest
            | EiaResearchOperation::PrivateForecast
            | EiaResearchOperation::PrivateModelTraining
            | EiaResearchOperation::PrivateModelOperation => Ok(()),
            EiaResearchOperation::Sale | EiaResearchOperation::Redistribution => {
                Err(EiaLifecycleError::UseDenied)
            }
        }
    }

    /// Returns whether sale is ever admitted by this policy.
    pub const fn sale_allowed(self) -> bool {
        false
    }

    /// Returns whether redistribution is ever admitted by this policy.
    pub const fn redistribution_allowed(self) -> bool {
        false
    }

    /// Returns the deterministic non-authoritative operation matrix root must rejoin to rights.
    pub fn operation_matrix_digest(self) -> Result<EiaDigest, EiaLifecycleError> {
        let operations = [
            EiaResearchOperation::Retrieve,
            EiaResearchOperation::PrivateDisplay,
            EiaResearchOperation::PrivatePersist,
            EiaResearchOperation::PrivateTransform,
            EiaResearchOperation::PrivateBacktest,
            EiaResearchOperation::PrivateForecast,
            EiaResearchOperation::PrivateModelTraining,
            EiaResearchOperation::PrivateModelOperation,
            EiaResearchOperation::Sale,
            EiaResearchOperation::Redistribution,
        ];
        let matrix = operations.map(|operation| (operation, self.authorize(operation).is_ok()));
        let semantic =
            serde_json::to_vec(&matrix).map_err(|_| EiaLifecycleError::InvalidEvidence)?;
        Ok(digest_parts(
            b"market-squawk/eia-private-use-operation-matrix/v1",
            [semantic.as_slice()],
        ))
    }
}

/// Adapter-local declaration of durable services that composition must bind before activation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct EiaActivationRequirements {
    shared_provider_rate_authority: bool,
    root_page_journal_rejoin: bool,
    sealed_raw_before_publish: bool,
    shared_publication_authority: bool,
    root_rights_decision_rejoin: bool,
}

impl EiaActivationRequirements {
    /// Returns the fixed production activation requirements.
    pub const fn production() -> Self {
        Self {
            shared_provider_rate_authority: true,
            root_page_journal_rejoin: true,
            sealed_raw_before_publish: true,
            shared_publication_authority: true,
            root_rights_decision_rejoin: true,
        }
    }

    /// EIA uses the shared crash-safe provider-rate authority; no duplicate local quota store exists.
    pub const fn shared_provider_rate_authority_required(self) -> bool {
        self.shared_provider_rate_authority
    }

    /// Returns whether partial offset progress must be owned by the root page journal.
    pub const fn root_page_journal_rejoin_required(self) -> bool {
        self.root_page_journal_rejoin
    }

    /// Returns whether exact raw capture must be immutable before normalized publication.
    pub const fn sealed_raw_before_publish(self) -> bool {
        self.sealed_raw_before_publish
    }

    /// Returns whether only the shared data plane may publish manifests/generations and PIT reads.
    pub const fn shared_publication_authority_required(self) -> bool {
        self.shared_publication_authority
    }

    /// Returns whether root must rejoin its nonforgeable rights decision before use/publication.
    pub const fn root_rights_decision_rejoin_required(self) -> bool {
        self.root_rights_decision_rejoin
    }
}

/// Static selected dataset input frozen against freshly discovered route metadata.
#[derive(Clone, Debug)]
pub struct EiaDatasetProfile {
    query: EiaDataQuery,
    fields: Vec<EiaDataFieldContract>,
    descriptor_fields: Vec<EiaFieldId>,
    clock_fields: Vec<EiaClockField>,
    publication: EiaPublicationMode,
}

/// Closed downstream publication capability for one selected route contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum EiaPublicationMode {
    /// Retain typed provider-native values only; no canonical numeric macro publication is claimed.
    NativeOnly,
    /// Every selected field is an exact decimal/missing macro series.
    CanonicalMacro,
}

impl EiaDatasetProfile {
    /// Constructs a canonical macro activation profile and rejects string-valued fields.
    pub fn try_for_macro(
        query: EiaDataQuery,
        fields: Vec<EiaDataFieldContract>,
        descriptor_fields: Vec<EiaFieldId>,
        clock_fields: Vec<EiaClockField>,
    ) -> Result<Self, EiaError> {
        if fields.is_empty()
            || fields
                .iter()
                .any(|field| field.value_kind() != EiaValueKind::Decimal)
        {
            return Err(EiaError::Canonicalization);
        }
        Ok(Self {
            query,
            fields,
            descriptor_fields,
            clock_fields,
            publication: EiaPublicationMode::CanonicalMacro,
        })
    }

    /// Constructs a typed provider-native activation profile without claiming macro publication.
    pub fn native_only(
        query: EiaDataQuery,
        fields: Vec<EiaDataFieldContract>,
        descriptor_fields: Vec<EiaFieldId>,
        clock_fields: Vec<EiaClockField>,
    ) -> Result<Self, EiaError> {
        if fields.is_empty() {
            return Err(EiaError::SchemaDrift);
        }
        Ok(Self {
            query,
            fields,
            descriptor_fields,
            clock_fields,
            publication: EiaPublicationMode::NativeOnly,
        })
    }

    /// Returns the exact immutable provider query.
    pub const fn query(&self) -> &EiaDataQuery {
        &self.query
    }

    /// Returns the exact publication capability validated by this profile.
    pub const fn publication_mode(&self) -> EiaPublicationMode {
        self.publication
    }

    fn freeze(
        &self,
        metadata: crate::EiaRouteMetadata,
        facet_catalogs: Vec<crate::EiaFacetCatalog>,
    ) -> Result<EiaDatasetContract, EiaError> {
        if self.publication == EiaPublicationMode::CanonicalMacro
            && metadata
                .frequency(self.query.frequency())
                .is_none_or(|frequency| {
                    !matches!(
                        frequency.format(),
                        "YYYY" | "YYYY-MM" | "YYYY-Q" | "YYYY-Q#" | "YYYY-\"Q\"Q" | "YYYY-MM-DD"
                    )
                })
        {
            return Err(EiaError::Canonicalization);
        }
        EiaDatasetContract::try_new(EiaDatasetContractInput {
            metadata,
            query: self.query.clone(),
            fields: self.fields.clone(),
            facet_catalogs,
            descriptor_fields: self.descriptor_fields.clone(),
            clock_fields: self.clock_fields.clone(),
        })
    }
}

/// Redacted successful activation doctor evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EiaDoctorReport {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    source_metadata_payload_digest: EvidenceDigest,
    authorization_subject: SourceIdentifier,
    authorization_evidence: EvidenceDigest,
    authorization_starts_at: Timestamp,
    authorization_ends_at: Option<Timestamp>,
    private_use_policy_digest: EiaDigest,
    route: crate::EiaRoute,
    api_version: EiaApiVersion,
    query_digest: EiaDigest,
    contract_schema_digest: EiaDigest,
    metadata_request_digest: EiaDigest,
    data_request_digest: EiaDigest,
    data_envelope_schema_digest: EiaDigest,
    data_row_schema_digest: EiaDigest,
    facet_catalog_digests: Box<[EiaDigest]>,
    provider_total: u64,
    probe_rows: u64,
    probe_observations: u64,
    probe_missing_observations: u64,
    response_bytes: u64,
    retained_bytes: u64,
    latency_nanos: u64,
    observed_at: Timestamp,
    expires_at: Timestamp,
    requirements: EiaActivationRequirements,
    publication: EiaPublicationMode,
    report_digest: EiaDigest,
    doctor_capture_receipts: Box<[ProviderCaptureSetReceipt]>,
}

#[derive(Serialize)]
struct EiaDoctorReportDigestInput<'a> {
    source_id: &'a SourceId,
    metadata_revision: &'a MetadataRevision,
    source_metadata_payload_digest: EvidenceDigest,
    authorization_subject: &'a SourceIdentifier,
    authorization_evidence: EvidenceDigest,
    authorization_starts_at: Timestamp,
    authorization_ends_at: Option<Timestamp>,
    private_use_policy_digest: EiaDigest,
    route: &'a crate::EiaRoute,
    api_version: &'a EiaApiVersion,
    query_digest: EiaDigest,
    contract_schema_digest: EiaDigest,
    metadata_request_digest: EiaDigest,
    data_request_digest: EiaDigest,
    data_envelope_schema_digest: EiaDigest,
    data_row_schema_digest: EiaDigest,
    facet_catalog_digests: &'a [EiaDigest],
    provider_total: u64,
    probe_rows: u64,
    probe_observations: u64,
    probe_missing_observations: u64,
    response_bytes: u64,
    retained_bytes: u64,
    latency_nanos: u64,
    observed_at: Timestamp,
    expires_at: Timestamp,
    requirements: EiaActivationRequirements,
    publication: EiaPublicationMode,
    doctor_capture_receipts: &'a [ProviderCaptureSetReceipt],
}

impl EiaDoctorReport {
    /// Returns the registered source identity.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact source metadata revision used by the doctor.
    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    /// Returns the exact content evidence bound to the current source-metadata revision.
    pub const fn source_metadata_payload_digest(&self) -> EvidenceDigest {
        self.source_metadata_payload_digest
    }

    /// Returns the exact authorization/account generation subject.
    pub const fn authorization_subject(&self) -> &SourceIdentifier {
        &self.authorization_subject
    }

    /// Returns exact authorization/credential-generation evidence.
    pub const fn authorization_evidence(&self) -> EvidenceDigest {
        self.authorization_evidence
    }

    /// Returns the inclusive start of the authorization generation.
    pub const fn authorization_starts_at(&self) -> Timestamp {
        self.authorization_starts_at
    }

    /// Returns the exclusive end of the authorization generation, when finite.
    pub const fn authorization_ends_at(&self) -> Option<Timestamp> {
        self.authorization_ends_at
    }

    /// Returns the non-authoritative fixed policy digest root must rejoin to its rights decision.
    pub const fn private_use_policy_digest(&self) -> EiaDigest {
        self.private_use_policy_digest
    }

    /// Returns the exact provider route.
    pub const fn route(&self) -> &crate::EiaRoute {
        &self.route
    }

    /// Returns the provider-serving API v2 version.
    pub const fn api_version(&self) -> &EiaApiVersion {
        &self.api_version
    }

    /// Returns the selected secret-free query identity.
    pub const fn query_digest(&self) -> EiaDigest {
        self.query_digest
    }

    /// Returns the freshly frozen route-native schema identity.
    pub const fn contract_schema_digest(&self) -> EiaDigest {
        self.contract_schema_digest
    }

    /// Returns the secret-free route-metadata request identity.
    pub const fn metadata_request_digest(&self) -> EiaDigest {
        self.metadata_request_digest
    }

    /// Returns the secret-free data-probe request identity.
    pub const fn data_request_digest(&self) -> EiaDigest {
        self.data_request_digest
    }

    /// Returns the real data-probe envelope shape identity.
    pub const fn data_envelope_schema_digest(&self) -> EiaDigest {
        self.data_envelope_schema_digest
    }

    /// Returns the real provider row-shape identity.
    pub const fn data_row_schema_digest(&self) -> EiaDigest {
        self.data_row_schema_digest
    }

    /// Returns exact sealed-catalog schema identities aligned to the query facets.
    pub fn facet_catalog_digests(&self) -> &[EiaDigest] {
        &self.facet_catalog_digests
    }

    /// Returns provider matching-row total observed by the offset-zero probe.
    pub const fn provider_total(&self) -> u64 {
        self.provider_total
    }

    /// Returns rows validated by the bounded probe.
    pub const fn probe_rows(&self) -> u64 {
        self.probe_rows
    }

    /// Returns actual field observations emitted by the bounded probe.
    pub const fn probe_observations(&self) -> u64 {
        self.probe_observations
    }

    /// Returns explicit provider-missing observations in the data probe.
    pub const fn probe_missing_observations(&self) -> u64 {
        self.probe_missing_observations
    }

    /// Returns exact transport bytes across metadata and data-probe responses.
    pub const fn response_bytes(&self) -> u64 {
        self.response_bytes
    }

    /// Returns exact sanitized bytes eligible for raw sealing.
    pub const fn retained_bytes(&self) -> u64 {
        self.retained_bytes
    }

    /// Returns checked aggregate request latency in nanoseconds.
    pub const fn latency_nanos(&self) -> u64 {
        self.latency_nanos
    }

    /// Returns when all route, facet-catalog, and data-probe responses had been fully validated.
    pub const fn observed_at(&self) -> Timestamp {
        self.observed_at
    }

    /// Returns the exclusive finite deadline at which this doctor can no longer be used.
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    /// Returns fixed durable activation requirements.
    pub const fn requirements(&self) -> EiaActivationRequirements {
        self.requirements
    }

    /// Returns whether this activation can enter canonical macro publication.
    pub const fn publication_mode(&self) -> EiaPublicationMode {
        self.publication
    }

    /// Returns the complete redacted doctor evidence identity.
    pub const fn report_digest(&self) -> EiaDigest {
        self.report_digest
    }

    /// Returns the complete logical doctor captures that activation must physically seal.
    pub fn doctor_capture_receipts(&self) -> &[ProviderCaptureSetReceipt] {
        &self.doctor_capture_receipts
    }

    fn compute_digest(&self) -> Result<EiaDigest, EiaLifecycleError> {
        let semantic = serde_json::to_vec(&EiaDoctorReportDigestInput {
            source_id: &self.source_id,
            metadata_revision: &self.metadata_revision,
            source_metadata_payload_digest: self.source_metadata_payload_digest,
            authorization_subject: &self.authorization_subject,
            authorization_evidence: self.authorization_evidence,
            authorization_starts_at: self.authorization_starts_at,
            authorization_ends_at: self.authorization_ends_at,
            private_use_policy_digest: self.private_use_policy_digest,
            route: &self.route,
            api_version: &self.api_version,
            query_digest: self.query_digest,
            contract_schema_digest: self.contract_schema_digest,
            metadata_request_digest: self.metadata_request_digest,
            data_request_digest: self.data_request_digest,
            data_envelope_schema_digest: self.data_envelope_schema_digest,
            data_row_schema_digest: self.data_row_schema_digest,
            facet_catalog_digests: &self.facet_catalog_digests,
            provider_total: self.provider_total,
            probe_rows: self.probe_rows,
            probe_observations: self.probe_observations,
            probe_missing_observations: self.probe_missing_observations,
            response_bytes: self.response_bytes,
            retained_bytes: self.retained_bytes,
            latency_nanos: self.latency_nanos,
            observed_at: self.observed_at,
            expires_at: self.expires_at,
            requirements: self.requirements,
            publication: self.publication,
            doctor_capture_receipts: &self.doctor_capture_receipts,
        })
        .map_err(|_| EiaLifecycleError::InvalidEvidence)?;
        Ok(digest_parts(
            b"market-squawk/eia-doctor-report/v2",
            [semantic.as_slice()],
        ))
    }

    pub(crate) fn validate(&self) -> Result<(), EiaLifecycleError> {
        if self.source_metadata_payload_digest.bytes() == [0; 32]
            || self.authorization_evidence.bytes() == [0; 32]
            || self.private_use_policy_digest.bytes() == [0; 32]
            || self.query_digest.bytes() == [0; 32]
            || self.contract_schema_digest.bytes() == [0; 32]
            || self
                .facet_catalog_digests
                .iter()
                .any(|digest| digest.bytes() == [0; 32])
            || self.probe_rows == 0
            || self.probe_observations == 0
            || self.observed_at >= self.expires_at
            || self.authorization_starts_at > self.observed_at
            || self
                .authorization_ends_at
                .is_some_and(|ends_at| self.observed_at >= ends_at || self.expires_at > ends_at)
            || self.doctor_capture_receipts.is_empty()
            || self.doctor_capture_receipts.iter().any(|receipt| {
                receipt.content_digest().bytes() == [0; 32]
                    || receipt.observation_digest().bytes() == [0; 32]
            })
            || self.compute_digest()? != self.report_digest
        {
            return Err(EiaLifecycleError::InvalidEvidence);
        }
        Ok(())
    }
}

/// Capture-first doctor output. Every exact official response must be sealed before activation.
#[derive(Debug)]
pub struct EiaDoctorOutput {
    candidate: EiaActivationCandidate,
    captures: Box<[ProviderCaptureMaterial]>,
}

impl EiaDoctorOutput {
    /// Returns redacted doctor evidence before consuming capture material.
    pub const fn report(&self) -> &EiaDoctorReport {
        &self.candidate.report
    }

    /// Consumes the indivisible activation handoff.
    pub fn into_parts(self) -> (EiaActivationCandidate, Box<[ProviderCaptureMaterial]>) {
        (self.candidate, self.captures)
    }
}

/// Non-serializable candidate bound to freshly acquired metadata and a real data probe.
#[derive(Debug)]
pub struct EiaActivationCandidate {
    transport: EiaSourceTransport,
    contract: EiaDatasetContract,
    policy: EiaPrivateResearchPolicy,
    report: EiaDoctorReport,
}

/// Runs metadata discovery and one real offset-zero request against the exact selected query.
pub async fn run_eia_doctor(
    transport: EiaSourceTransport,
    authority: &ExtractionAuthority,
    profile: EiaDatasetProfile,
    deadline: Timestamp,
    cancellation: CancellationToken,
) -> Result<EiaDoctorOutput, EiaLifecycleError> {
    let policy = EiaPrivateResearchPolicy::personal_research();
    policy.authorize(EiaResearchOperation::Retrieve)?;
    let private_use_policy_digest = policy.operation_matrix_digest()?;
    let source_metadata = transport.metadata();
    let metadata_request = EiaMetadataRequest::route(profile.query().route().clone());
    let metadata = transport
        .discover_route_metadata(authority, &metadata_request, deadline, cancellation.clone())
        .await?;
    let (route_metadata, metadata_raw, metadata_capture) = metadata.into_parts();
    let mut captures = Vec::new();
    captures
        .try_reserve_exact(profile.query().facets().len().saturating_add(2))
        .map_err(|_| EiaLifecycleError::InvalidEvidence)?;
    captures.push(metadata_capture);
    let mut facet_catalogs = Vec::new();
    facet_catalogs
        .try_reserve_exact(profile.query().facets().len())
        .map_err(|_| EiaLifecycleError::InvalidEvidence)?;
    let mut facet_catalog_digests = Vec::new();
    facet_catalog_digests
        .try_reserve_exact(profile.query().facets().len())
        .map_err(|_| EiaLifecycleError::InvalidEvidence)?;
    let mut response_bytes = metadata_raw.http_receipt().response_bytes();
    let mut retained_bytes = metadata_raw.http_receipt().retained_bytes();
    let mut latency = metadata_raw.http_receipt().latency();
    let mut observed_at = metadata_raw.http_receipt().received_at();
    for facet in profile.query().facets() {
        let request =
            EiaMetadataRequest::facet(profile.query().route().clone(), facet.facet().clone());
        let retrieval = transport
            .discover_facet_metadata(authority, &request, deadline, cancellation.clone())
            .await?;
        let (catalog, raw, capture) = retrieval.into_parts();
        response_bytes = response_bytes
            .checked_add(raw.http_receipt().response_bytes())
            .ok_or(EiaLifecycleError::InvalidEvidence)?;
        retained_bytes = retained_bytes
            .checked_add(raw.http_receipt().retained_bytes())
            .ok_or(EiaLifecycleError::InvalidEvidence)?;
        latency = latency
            .checked_add(raw.http_receipt().latency())
            .ok_or(EiaLifecycleError::InvalidEvidence)?;
        observed_at = observed_at.max(raw.http_receipt().received_at());
        facet_catalog_digests.push(catalog.schema_digest());
        facet_catalogs.push(catalog);
        captures.push(capture);
    }
    let publication = profile.publication_mode();
    let contract = profile.freeze(route_metadata, facet_catalogs)?;
    let probe = transport
        .probe_data(authority, &contract, deadline, cancellation)
        .await?;
    let (_dataset, page, probe_raw, probe_capture) = probe.into_parts();
    response_bytes = response_bytes
        .checked_add(probe_raw.http_receipt().response_bytes())
        .ok_or(EiaLifecycleError::InvalidEvidence)?;
    retained_bytes = retained_bytes
        .checked_add(probe_raw.http_receipt().retained_bytes())
        .ok_or(EiaLifecycleError::InvalidEvidence)?;
    latency = latency
        .checked_add(probe_raw.http_receipt().latency())
        .ok_or(EiaLifecycleError::InvalidEvidence)?;
    observed_at = observed_at.max(probe_raw.http_receipt().received_at());
    captures.push(probe_capture);
    let authorization = source_metadata.authorization();
    if !authorization.is_effective_at(observed_at) {
        return Err(EiaLifecycleError::InvalidEvidence);
    }
    if publication == EiaPublicationMode::CanonicalMacro
        && (page.observations().is_empty()
            || page.observations().iter().any(|observation| {
                matches!(observation.value(), crate::EiaNativeValue::String(_))
                    || matches!(
                        observation.period().kind(),
                        crate::EiaPeriodKind::Provider(_)
                    )
            }))
    {
        return Err(EiaLifecycleError::InvalidEvidence);
    }
    let page_receipt = page.receipt();
    let required_pages = required_data_pages(page_receipt.total(), contract.query().length())?;
    if page_receipt.returned_rows() == 0
        || page_receipt.observation_count() == 0
        || required_pages == 0
        || required_pages > u64::from(transport.max_pages())
        || crate::data::validate_publication_cardinality(
            page_receipt.total(),
            contract.fields().len(),
        )? == 0
    {
        return Err(EiaLifecycleError::InvalidEvidence);
    }
    let latency_nanos =
        u64::try_from(latency.as_nanos()).map_err(|_| EiaLifecycleError::InvalidEvidence)?;
    let mut expires_at = observed_at
        .checked_add_nanos(EIA_DOCTOR_MAX_AGE_NANOS)
        .map_err(|_| EiaLifecycleError::InvalidEvidence)?;
    if let Some(authorization_end) = authorization.effective_interval().ends_at() {
        expires_at = expires_at.min(authorization_end);
    }
    if expires_at <= observed_at {
        return Err(EiaLifecycleError::InvalidEvidence);
    }
    let effective = authorization.effective_interval();
    let doctor_capture_receipts = captures
        .iter()
        .map(|capture| capture.receipt().clone())
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let mut report = EiaDoctorReport {
        source_id: source_metadata.source_id().clone(),
        metadata_revision: source_metadata.revision().clone(),
        source_metadata_payload_digest: source_metadata
            .revision_evidence()
            .payload_evidence()
            .content_digest(),
        authorization_subject: authorization.basis().as_source_identifier().clone(),
        authorization_evidence: authorization.evidence().content_digest(),
        authorization_starts_at: effective.starts_at(),
        authorization_ends_at: effective.ends_at(),
        private_use_policy_digest,
        route: contract.query().route().clone(),
        api_version: contract.metadata().api_version().clone(),
        query_digest: contract.query().identity(),
        contract_schema_digest: contract.schema_digest(),
        metadata_request_digest: metadata_raw.http_receipt().request_digest(),
        data_request_digest: page_receipt.request_digest(),
        data_envelope_schema_digest: page_receipt.envelope_schema_digest(),
        data_row_schema_digest: page_receipt.row_schema_digest(),
        facet_catalog_digests: facet_catalog_digests.into_boxed_slice(),
        provider_total: page_receipt.total(),
        probe_rows: page_receipt.returned_rows(),
        probe_observations: page_receipt.observation_count(),
        probe_missing_observations: page_receipt.missing_observation_count(),
        response_bytes,
        retained_bytes,
        latency_nanos,
        observed_at,
        expires_at,
        requirements: EiaActivationRequirements::production(),
        publication,
        report_digest: EiaDigest::new([0; 32]),
        doctor_capture_receipts,
    };
    report.report_digest = report.compute_digest()?;
    let candidate = EiaActivationCandidate {
        transport,
        contract,
        policy,
        report,
    };
    Ok(EiaDoctorOutput {
        candidate,
        captures: captures.into_boxed_slice(),
    })
}

/// Activated, credential-bearing provider boundary. Secrets remain solely inside its transport.
pub struct EiaActivatedProvider {
    transport: EiaSourceTransport,
    contract: EiaDatasetContract,
    policy: EiaPrivateResearchPolicy,
    report: EiaDoctorReport,
    sealed_doctor_captures: Box<[SealedProviderCaptureSetReceipt]>,
}

impl std::fmt::Debug for EiaActivatedProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EiaActivatedProvider")
            .field("source_id", self.transport.metadata().source_id())
            .field("metadata_revision", self.transport.metadata().revision())
            .field("query_digest", &self.contract.query().identity())
            .field(
                "private_use_policy_digest",
                &self.report.private_use_policy_digest(),
            )
            .finish_non_exhaustive()
    }
}

impl EiaActivatedProvider {
    /// Activates only after every doctor response has an immutable physical receipt.
    pub fn try_activate(
        candidate: EiaActivationCandidate,
        sealed_doctor_captures: &[SealedProviderCaptureSetReceipt],
    ) -> Result<Self, EiaLifecycleError> {
        let activated_at = crate::transport::system_timestamp()?;
        candidate.report.validate()?;
        if sealed_doctor_captures.len() != candidate.report.doctor_capture_receipts().len()
            || sealed_doctor_captures
                .iter()
                .zip(candidate.report.doctor_capture_receipts())
                .any(|(sealed, expected)| {
                    sealed.capture() != expected
                        || sealed.receipt_digest().bytes() == [0; 32]
                        || sealed.segment().physical_receipt_digest().bytes() == [0; 32]
                })
            || candidate.transport.metadata().source_id() != candidate.report.source_id()
            || candidate.transport.metadata().revision() != candidate.report.metadata_revision()
            || candidate
                .transport
                .metadata()
                .revision_evidence()
                .payload_evidence()
                .content_digest()
                != candidate.report.source_metadata_payload_digest()
            || candidate
                .transport
                .metadata()
                .authorization()
                .basis()
                .as_source_identifier()
                != candidate.report.authorization_subject()
            || candidate
                .transport
                .metadata()
                .authorization()
                .evidence()
                .content_digest()
                != candidate.report.authorization_evidence()
            || candidate
                .transport
                .metadata()
                .authorization()
                .effective_interval()
                .starts_at()
                != candidate.report.authorization_starts_at()
            || candidate
                .transport
                .metadata()
                .authorization()
                .effective_interval()
                .ends_at()
                != candidate.report.authorization_ends_at()
            || !candidate
                .transport
                .metadata()
                .authorization()
                .is_effective_at(activated_at)
            || candidate.policy.operation_matrix_digest()?
                != candidate.report.private_use_policy_digest()
            || required_data_pages(
                candidate.report.provider_total(),
                candidate.contract.query().length(),
            )? > u64::from(candidate.transport.max_pages())
            || activated_at < candidate.report.observed_at()
            || activated_at >= candidate.report.expires_at()
        {
            return Err(EiaLifecycleError::InvalidEvidence);
        }
        candidate
            .policy
            .authorize(EiaResearchOperation::PrivatePersist)?;
        let mut retained_doctor_seals = Vec::new();
        retained_doctor_seals
            .try_reserve_exact(sealed_doctor_captures.len())
            .map_err(|_| EiaLifecycleError::InvalidEvidence)?;
        retained_doctor_seals.extend_from_slice(sealed_doctor_captures);
        let activated = Self {
            transport: candidate.transport,
            contract: candidate.contract,
            policy: candidate.policy,
            report: candidate.report,
            sealed_doctor_captures: retained_doctor_seals.into_boxed_slice(),
        };
        activated.ensure_current_at(crate::transport::system_timestamp()?)?;
        Ok(activated)
    }

    /// Returns the exact frozen route/query contract.
    pub const fn contract(&self) -> &EiaDatasetContract {
        &self.contract
    }

    /// Returns the exact source metadata and credential/authorization generation.
    pub fn source_metadata(&self) -> &SourceMetadata {
        self.transport.metadata()
    }

    /// Returns redacted doctor evidence used for this activation.
    pub const fn doctor_report(&self) -> &EiaDoctorReport {
        &self.report
    }

    /// Returns the actual immutable doctor receipts retained by the active provider.
    pub fn sealed_doctor_captures(&self) -> &[SealedProviderCaptureSetReceipt] {
        &self.sealed_doctor_captures
    }

    /// Returns the fixed usage policy.
    pub const fn usage_policy(&self) -> EiaPrivateResearchPolicy {
        self.policy
    }

    /// Returns whether the selected fields were admitted for canonical macro publication.
    pub const fn publication_mode(&self) -> EiaPublicationMode {
        self.report.publication_mode()
    }

    /// Starts a linear root-controlled acquisition at offset zero.
    ///
    /// The deadline must fit wholly inside the finite activated doctor window. No network request
    /// occurs here and the returned cursor has no public constructor or durable authority.
    pub fn begin_retrieval(
        &self,
        authority: &ExtractionAuthority,
        deadline: Timestamp,
    ) -> Result<EiaDataAcquisitionCursor, EiaLifecycleError> {
        self.policy
            .authorize(EiaResearchOperation::PrivatePersist)?;
        let observed_at = crate::transport::system_timestamp()?;
        self.ensure_transition_deadline(observed_at, deadline)?;
        Ok(self
            .transport
            .begin_data_retrieval(authority, &self.contract)?)
    }

    /// Fetches exactly one bounded page and withholds all continuation until root seals it.
    pub async fn fetch_next_retrieval_page(
        &self,
        authority: &ExtractionAuthority,
        cursor: EiaDataAcquisitionCursor,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<EiaPendingDataPage, EiaLifecycleError> {
        self.policy
            .authorize(EiaResearchOperation::PrivatePersist)?;
        self.ensure_transition_deadline(crate::transport::system_timestamp()?, deadline)?;
        let pending = self
            .transport
            .fetch_next_data_page(authority, &self.contract, cursor, deadline, cancellation)
            .await?;
        let received_at = pending
            .page_material()
            .raw_page()
            .http_receipt()
            .received_at();
        self.ensure_current_at(received_at)?;
        self.ensure_transition_deadline(crate::transport::system_timestamp()?, deadline)?;
        Ok(pending)
    }

    /// Rejoins one root-journaled actual page seal and only then exposes `More` or `Complete`.
    pub fn rejoin_retrieval_page(
        &self,
        authority: &ExtractionAuthority,
        rejoin: EiaDataPageSealRejoin,
        sealed_page: SealedProviderCaptureSetReceipt,
        deadline: Timestamp,
    ) -> Result<EiaDataPageTransition, EiaLifecycleError> {
        self.policy
            .authorize(EiaResearchOperation::PrivatePersist)?;
        let observed_at = crate::transport::system_timestamp()?;
        self.ensure_transition_deadline(observed_at, deadline)?;
        self.ensure_current_at(rejoin.root_journal_rejoin().capture_receipt().received_at())?;
        let transition =
            self.transport
                .rejoin_data_page(authority, &self.contract, rejoin, sealed_page)?;
        self.ensure_transition_deadline(crate::transport::system_timestamp()?, deadline)?;
        Ok(transition)
    }

    /// Validates an exact operation against the frozen non-sale/non-redistribution policy.
    pub fn authorize_use(&self, operation: EiaResearchOperation) -> Result<(), EiaLifecycleError> {
        self.policy.authorize(operation)?;
        self.ensure_current_at(crate::transport::system_timestamp()?)
    }

    /// Consumes the complete terminal retrieval and its actual combined shared-journal seal.
    pub fn publication_candidate(
        &self,
        retrieval: EiaDataRetrievalSealRejoin,
        sealed_capture: SealedProviderCaptureSetReceipt,
    ) -> Result<crate::EiaPublicationCandidate, EiaLifecycleError> {
        self.policy
            .authorize(EiaResearchOperation::PrivatePersist)?;
        let normalization_admitted_at = crate::transport::system_timestamp()?;
        self.ensure_current_at(normalization_admitted_at)?;
        let capture_pages = sealed_capture.capture().pages();
        if capture_pages.is_empty()
            || capture_pages
                .iter()
                .any(|page| page.received_at() > normalization_admitted_at)
        {
            return Err(EiaLifecycleError::InvalidEvidence);
        }
        for page in capture_pages {
            self.ensure_current_at(page.received_at())?;
        }
        let candidate = crate::EiaPublicationCandidate::try_new(
            self,
            retrieval,
            sealed_capture,
            normalization_admitted_at,
        )?;
        self.ensure_current_at(crate::transport::system_timestamp()?)?;
        Ok(candidate)
    }

    fn ensure_transition_deadline(
        &self,
        observed_at: Timestamp,
        deadline: Timestamp,
    ) -> Result<(), EiaLifecycleError> {
        self.ensure_current_at(observed_at)?;
        if deadline <= observed_at {
            return Err(EiaLifecycleError::InvalidEvidence);
        }
        if deadline >= self.report.expires_at() {
            return Err(EiaLifecycleError::StaleActivation);
        }
        Ok(())
    }

    pub(crate) fn ensure_current_at(
        &self,
        observed_at: Timestamp,
    ) -> Result<(), EiaLifecycleError> {
        self.report.validate()?;
        if self.transport.metadata().source_id() != self.report.source_id()
            || self.transport.metadata().revision() != self.report.metadata_revision()
            || self
                .transport
                .metadata()
                .revision_evidence()
                .payload_evidence()
                .content_digest()
                != self.report.source_metadata_payload_digest()
            || self
                .transport
                .metadata()
                .authorization()
                .basis()
                .as_source_identifier()
                != self.report.authorization_subject()
            || self
                .transport
                .metadata()
                .authorization()
                .evidence()
                .content_digest()
                != self.report.authorization_evidence()
            || self
                .transport
                .metadata()
                .authorization()
                .effective_interval()
                .starts_at()
                != self.report.authorization_starts_at()
            || self
                .transport
                .metadata()
                .authorization()
                .effective_interval()
                .ends_at()
                != self.report.authorization_ends_at()
            || !self
                .transport
                .metadata()
                .authorization()
                .is_effective_at(observed_at)
            || observed_at < self.report.observed_at()
            || observed_at >= self.report.expires_at()
            || self.policy.operation_matrix_digest()? != self.report.private_use_policy_digest()
            || required_data_pages(self.report.provider_total(), self.contract.query().length())?
                > u64::from(self.transport.max_pages())
            || self.sealed_doctor_captures.len() != self.report.doctor_capture_receipts().len()
            || self
                .sealed_doctor_captures
                .iter()
                .zip(self.report.doctor_capture_receipts())
                .any(|(sealed, expected)| {
                    sealed.capture() != expected
                        || sealed.receipt_digest().bytes() == [0; 32]
                        || sealed.segment().physical_receipt_digest().bytes() == [0; 32]
                })
        {
            return Err(EiaLifecycleError::StaleActivation);
        }
        Ok(())
    }
}

impl SourceMetadataProvider for EiaActivatedProvider {
    fn metadata(&self) -> &SourceMetadata {
        self.transport.metadata()
    }
}

/// Provider-local activation failure with no credential-bearing context.
#[derive(Debug, Error)]
pub enum EiaLifecycleError {
    /// The fixed private-use policy rejected sale or redistribution.
    #[error("EIA use is outside the frozen private/personal research authorization")]
    UseDenied,
    /// Doctor, capture, or activation evidence did not bind exactly.
    #[error("invalid EIA activation evidence")]
    InvalidEvidence,
    /// Doctor, policy, authorization, source metadata, or actual doctor seals are no longer current.
    #[error("EIA activation is no longer current")]
    StaleActivation,
    /// Provider request/response or capture transport failed.
    #[error(transparent)]
    Transport(#[from] EiaSourceTransportError),
    /// Frozen provider-native contract failed.
    #[error(transparent)]
    Protocol(#[from] EiaError),
}
