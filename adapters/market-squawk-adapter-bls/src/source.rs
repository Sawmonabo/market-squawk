use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;
use market_squawk_domain::{
    DataQuality, DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence,
    SourceIdentifier, Timestamp,
};
use market_squawk_platform::RawCaptureRecord;
#[cfg(test)]
use market_squawk_sources::AuthoritativeSourceRegistry;
use market_squawk_sources::{
    AuthorizationMode, AvailabilityEvidence, CoverageDomain, DiscoveryBatch, DiscoveryRequest,
    ExtractionAuthority, ExtractionBatch, ExtractionRequest, ExtractionRevisionPlan,
    ExtractionSource, ExtractionSourceError, HistoricalCapability,
    ProviderCaptureMaterial, ProviderCapturePageReceipt, ProviderCaptureSetReceipt,
    ProviderCaptureTerminalDisposition, SealedProviderCaptureSetReceipt, SourceClass, SourceError,
    SourceMetadata, SourceMetadataProvider, SourceObject, SourceObjectCaptureIdentity,
    SourceProtocolProfile, CURRENT_RESEARCH_RECORD_SCHEMA, MAX_PROVIDER_CAPTURE_PAGES,
    payload_matches_exact_evidence,
};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::client::{BlsHttpClient, RetrievedBlsPage, ensure_deadline_open, system_timestamp};
use crate::contract::BlsRuntimeInstanceCapability;
use crate::discovery::{
    BlsDiscoveryAdmission, BlsDiscoveryObjectAdmission, BlsDiscoveryOutput,
    BlsPendingDiscovery,
};
use crate::{
    BlsAccessTier, BlsActivationCandidate, BlsActivationPlan, BlsAuthorization, BlsDoctorOutput,
    BlsDoctorReadiness, BlsDoctorReport, BlsProviderRateDeclaration, BlsPublicationCandidate,
    BlsCanonicalProviderSemantics, BlsRequestLimits, BlsRequestPlan, BlsRootRightsRejoin,
    BlsSeriesMetadata, BlsSourceError, BlsUsagePolicy,
};

mod normalize;
mod state;

use normalize::canonical_records;
use state::PageCache;
pub use state::{BlsNormalizedPage, BlsSourceHealth};

/// Exact, deterministic BLS source configuration bound into its dataset identity.
#[derive(Debug)]
pub struct BlsSourceConfig {
    authorization: BlsAuthorization,
    usage_policy: BlsUsagePolicy,
    root_rights_rejoin: BlsRootRightsRejoin,
    plan: BlsRequestPlan,
    series_metadata: BTreeMap<String, BlsSeriesMetadata>,
    dataset: SourceIdentifier,
}

/// Indivisible BLS extraction handoff containing canonical rows and their exact response bytes.
///
/// Application composition must seal [`Self::capture_material`] before publishing
/// [`Self::batch`] as a durable canonical generation.
#[derive(Debug)]
pub struct BlsExtractionOutput {
    batch: ExtractionBatch,
    provider_semantics: BlsCanonicalProviderSemantics,
    discovery_admission: BlsDiscoveryObjectAdmission,
}

impl BlsExtractionOutput {
    /// Returns the source-neutral canonical extraction batch.
    pub const fn batch(&self) -> &ExtractionBatch {
        &self.batch
    }

    /// Returns BLS-native semantics aligned one-for-one with canonical records.
    pub const fn provider_semantics(&self) -> &BlsCanonicalProviderSemantics {
        &self.provider_semantics
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        ExtractionBatch,
        BlsCanonicalProviderSemantics,
        BlsDiscoveryObjectAdmission,
    ) {
        (
            self.batch,
            self.provider_semantics,
            self.discovery_admission,
        )
    }
}

/// Registered, allowlisted, budget-coordinated BLS extraction producer.
pub struct BlsSource {
    metadata: SourceMetadata,
    config: BlsSourceConfig,
    rate_declaration: BlsProviderRateDeclaration,
    http: BlsHttpClient,
    runtime_instance: Arc<BlsRuntimeInstanceCapability>,
    cache: Mutex<PageCache>,
    health: Mutex<BlsSourceHealth>,
    #[cfg(test)]
    publication_actions: Mutex<VecDeque<Option<AuthoritativeSourceRegistry>>>,
}

impl std::fmt::Debug for BlsSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BlsSource")
            .field("source_id", self.metadata.source_id())
            .field("revision", self.metadata.revision())
            .field("dataset", self.config.dataset())
            .field("authorization", self.config.authorization())
            .finish_non_exhaustive()
    }
}

impl BlsSource {
    /// Derives the storage-safe analytical identity for one exact BLS request-plan dataset.
    ///
    /// The colon-delimited input remains the provider request and provenance identity. The
    /// returned dotted identity is a separate local analytical namespace that preserves the
    /// provider tier and complete request-plan digest.
    ///
    /// # Errors
    ///
    /// Returns [`BlsSourceError::InvalidConfiguration`] when the input is not an exact bounded BLS
    /// timeseries request-plan identity.
    pub fn analytical_dataset_identifier(
        provider_dataset: &SourceIdentifier,
    ) -> Result<SourceIdentifier, BlsSourceError> {
        let mut fields = provider_dataset.as_str().split(':');
        if fields.next() != Some("bls") || fields.next() != Some("timeseries") {
            return Err(BlsSourceError::InvalidConfiguration);
        }
        let tier = fields
            .next()
            .filter(|value| matches!(*value, "public-v1" | "registered-v2"))
            .ok_or(BlsSourceError::InvalidConfiguration)?;
        let digest = fields
            .next()
            .filter(|value| {
                value.len() == 64
                    && value
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            })
            .ok_or(BlsSourceError::InvalidConfiguration)?;
        if fields.next().is_some() {
            return Err(BlsSourceError::InvalidConfiguration);
        }
        SourceIdentifier::try_from(format!("bls.timeseries.{tier}.{digest}"))
            .map_err(|_| BlsSourceError::InvalidConfiguration)
    }

    /// Builds honest local-observation revision authority for a normalized BLS batch.
    ///
    /// The BLS timeseries response does not publish a per-observation version or publication
    /// coordinate. Market Squawk therefore binds revisions to exact canonical content and local
    /// observation order instead of fabricating provider chronology.
    ///
    /// # Errors
    ///
    /// Returns [`BlsSourceError::InvalidMetadata`] when the batch belongs to another source
    /// registration and [`BlsSourceError::RevisionAuthority`] when bounded evidence construction
    /// fails.
    pub fn revision_plan(
        &self,
        batch: &ExtractionBatch,
    ) -> Result<ExtractionRevisionPlan, BlsSourceError> {
        if batch.request().object().source_id() != self.metadata.source_id()
            || batch.request().object().metadata_revision() != self.metadata.revision()
        {
            return Err(BlsSourceError::InvalidMetadata);
        }
        ExtractionRevisionPlan::locally_observed(batch.records().len()).map_err(Into::into)
    }

    /// Binds a provider configuration to exact immutable metadata.
    ///
    /// # Errors
    ///
    /// Fails closed unless metadata declares an official-agency, macroeconomic, historical,
    /// extraction-only source with an allowlisted endpoint and the matching authorization mode.
    pub fn try_new(
        metadata: SourceMetadata,
        config: BlsSourceConfig,
    ) -> Result<Self, BlsSourceError> {
        let rate_declaration = Self::validate_metadata(&metadata, &config)?;
        let http = BlsHttpClient::try_new(&metadata, config.authorization())?;
        Ok(Self {
            metadata,
            config,
            rate_declaration,
            http,
            runtime_instance: BlsRuntimeInstanceCapability::new(),
            cache: Mutex::new(PageCache::new()),
            health: Mutex::new(BlsSourceHealth::new()),
            #[cfg(test)]
            publication_actions: Mutex::new(VecDeque::new()),
        })
    }

    #[cfg(test)]
    fn try_new_with_transport(
        metadata: SourceMetadata,
        config: BlsSourceConfig,
        transport: Arc<dyn crate::client::BlsTransport>,
    ) -> Result<Self, BlsSourceError> {
        let rate_declaration = Self::validate_metadata(&metadata, &config)?;
        let http = BlsHttpClient::try_new_with_transport(
            &metadata,
            config.authorization(),
            transport,
        )?;
        Ok(Self {
            metadata,
            config,
            rate_declaration,
            http,
            runtime_instance: BlsRuntimeInstanceCapability::new(),
            cache: Mutex::new(PageCache::new()),
            health: Mutex::new(BlsSourceHealth::new()),
            publication_actions: Mutex::new(VecDeque::new()),
        })
    }

    fn validate_metadata(
        metadata: &SourceMetadata,
        config: &BlsSourceConfig,
    ) -> Result<BlsProviderRateDeclaration, BlsSourceError> {
        let expected_mode = match config.tier() {
            BlsAccessTier::PublicV1 => AuthorizationMode::PublicInterface,
            BlsAccessTier::RegisteredV2 => AuthorizationMode::UserAuthorized,
        };
        if metadata.source_class() != SourceClass::OfficialAgency
            || metadata.provider().as_str() != "us-bls"
            || metadata.coverage().domain() != CoverageDomain::Macroeconomic
            || metadata.quality_ceiling() != DataQuality::OfficialDelayed
            || metadata.authorization().mode() != expected_mode
            || metadata.capabilities().live()
            || !metadata.capabilities().extraction()
            || metadata.capabilities().historical() != HistoricalCapability::Historical
            || !matches!(metadata.protocol_profile(), SourceProtocolProfile::NotLive)
        {
            return Err(BlsSourceError::InvalidMetadata);
        }
        metadata
            .network_policy()
            .authorize(config.authorization().endpoint())
            .map_err(|_| BlsSourceError::InvalidMetadata)?;
        BlsProviderRateDeclaration::try_from_metadata(metadata, config.tier(), config.limits())
    }

    /// Returns the exact request-plan-bound dataset callers use for discovery.
    pub const fn dataset(&self) -> &SourceIdentifier {
        self.config.dataset()
    }

    /// Returns the exact provider-local activation contract for shared composition.
    pub fn activation_plan(&self) -> Result<BlsActivationPlan, BlsSourceError> {
        BlsActivationPlan::try_new(
            self.metadata.source_id().clone(),
            self.metadata.revision().clone(),
            self.config.dataset().clone(),
            Self::analytical_dataset_identifier(self.config.dataset())?,
            self.config.usage_policy(),
            self.config.root_rights_rejoin(),
            self.config.authorization().credential_rejoin(),
            self.rate_declaration.clone(),
        )
    }

    /// Admits one freshly sealed successful doctor for a bounded in-process production window.
    pub fn activation_candidate(
        &self,
        doctor: BlsDoctorReport,
        sealed_doctor_capture: SealedProviderCaptureSetReceipt,
    ) -> Result<BlsActivationCandidate, BlsSourceError> {
        let activated_at = system_timestamp()?;
        BlsActivationCandidate::try_new(
            self.activation_plan()?,
            doctor,
            sealed_doctor_capture,
            activated_at,
            Arc::clone(&self.runtime_instance),
        )
    }

    /// Reopens a doctor-backed in-process admission against this exact source configuration.
    pub fn validate_activation_candidate(
        &self,
        activation: &BlsActivationCandidate,
    ) -> Result<(), BlsSourceError> {
        let operation_at = system_timestamp()?;
        self.validate_activation_candidate_at(activation, operation_at)
    }

    fn validate_activation_candidate_at(
        &self,
        activation: &BlsActivationCandidate,
        operation_at: Timestamp,
    ) -> Result<(), BlsSourceError> {
        activation.validate(
            &self.activation_plan()?,
            operation_at,
            &self.runtime_instance,
        )
    }

    fn validate_activation_operation_at(
        &self,
        activation: &BlsActivationCandidate,
        operation_at: Timestamp,
        deadline: Timestamp,
    ) -> Result<(), BlsSourceError> {
        self.validate_activation_candidate_at(activation, operation_at)?;
        if deadline >= activation.expires_at() {
            return Err(BlsSourceError::InvalidPublication);
        }
        Ok(())
    }

    /// Returns the exact declaration the application-owned durable authority must register.
    pub const fn rate_declaration(&self) -> &BlsProviderRateDeclaration {
        &self.rate_declaration
    }

    /// Builds a non-authoritative root-ingest handoff that owns the sealed discovery graph.
    pub fn publication_candidate(
        &self,
        output: BlsExtractionOutput,
        activation: &BlsActivationCandidate,
    ) -> Result<BlsPublicationCandidate, BlsSourceError> {
        self.validate_activation_candidate(activation)?;
        BlsPublicationCandidate::try_new(
            &self.metadata,
            &self.config,
            &self.rate_declaration,
            output,
            activation,
            &self.runtime_instance,
        )
    }

    /// Reopens a candidate immediately before root reserves the shared ingest transaction.
    pub fn validate_publication_candidate(
        &self,
        candidate: &BlsPublicationCandidate,
        activation: &BlsActivationCandidate,
    ) -> Result<(), BlsSourceError> {
        self.validate_activation_candidate(activation)?;
        candidate.validate(
            &self.metadata,
            &self.config,
            &self.rate_declaration,
            activation,
            &self.runtime_instance,
        )
    }

    /// Returns a bounded copy of local producer health.
    ///
    /// # Errors
    ///
    /// Fails closed if health synchronization was poisoned.
    pub fn health(&self) -> Result<BlsSourceHealth, BlsSourceError> {
        self.health
            .lock()
            .map(|health| *health)
            .map_err(|_| BlsSourceError::HealthUnavailable)
    }

    /// Runs one bounded real-request provider doctor without exposing credential material.
    ///
    /// The probe uses the first configured series and only the final year in the exact request
    /// plan. It therefore never broadens provider work for diagnosis and never invents a second
    /// series contract. Dynamic remaining quota remains owned by the durable provider-rate
    /// authority and must be joined by application composition rather than guessed here.
    pub async fn doctor(
        &self,
        authority: ExtractionAuthority,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<BlsDoctorOutput, ExtractionSourceError> {
        self.validate_authority(&authority)?;
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled);
        }
        ensure_deadline_open(deadline)?;
        let series_id = self
            .config
            .series_metadata
            .keys()
            .next()
            .cloned()
            .ok_or(SourceError::InvalidProtocolState)?;
        let year = self
            .config
            .plan()
            .chunks()
            .last()
            .map(crate::BlsRequestChunk::end_year)
            .ok_or(SourceError::InvalidProtocolState)?;
        let doctor_plan =
            BlsRequestPlan::try_new(self.config.tier(), vec![series_id.clone()], year, year)
                .map_err(|_| SourceError::InvalidProtocolState)?;
        let chunk = doctor_plan
            .chunks()
            .first()
            .ok_or(SourceError::InvalidProtocolState)?;
        let page = self
            .fetch_page(&authority, chunk, deadline, &cancellation)
            .await?;
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled);
        }
        ensure_deadline_open(deadline)?;
        authority.validate_current()?;

        let returned_series = u16::try_from(page.response.series().len())
            .map_err(|_| SourceError::InvalidProtocolState)?;
        let mut returned_observations = 0_u32;
        let mut observed_values = 0_u32;
        let mut missing_values = 0_u32;
        let mut preliminary_values = 0_u32;
        let mut footnotes = 0_u32;
        for series in page.response.series() {
            for observation in series.observations() {
                returned_observations = returned_observations
                    .checked_add(1)
                    .ok_or(SourceError::InvalidProtocolState)?;
                if observation.value().is_some() {
                    observed_values = observed_values
                        .checked_add(1)
                        .ok_or(SourceError::InvalidProtocolState)?;
                } else {
                    missing_values = missing_values
                        .checked_add(1)
                        .ok_or(SourceError::InvalidProtocolState)?;
                }
                if observation.is_preliminary() {
                    preliminary_values = preliminary_values
                        .checked_add(1)
                        .ok_or(SourceError::InvalidProtocolState)?;
                }
                footnotes = footnotes
                    .checked_add(
                        u32::try_from(observation.footnotes().len())
                            .map_err(|_| SourceError::InvalidProtocolState)?,
                    )
                    .ok_or(SourceError::InvalidProtocolState)?;
            }
        }
        let provider_messages = u16::try_from(page.response.messages().len())
            .map_err(|_| SourceError::InvalidProtocolState)?;
        let readiness = if returned_observations == 0 || observed_values == 0 {
            BlsDoctorReadiness::Unavailable
        } else if page.response.is_partial() || missing_values > 0 {
            BlsDoctorReadiness::Degraded
        } else {
            BlsDoctorReadiness::Available
        };
        let request_identity = doctor_capture_request_identity(
            &self.metadata,
            &self.config,
            &series_id,
            year,
        )?;
        let capture_material = self.capture_material_for_request(request_identity, &page)?;
        let capture = capture_material.receipt();
        let response_bytes = u64::try_from(page.bytes.len())
            .map_err(|_| SourceError::InvalidProtocolState)?;
        let response_content_digest = EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            Sha256::digest(&page.bytes).into(),
        );
        let report = BlsDoctorReport::new(
            self.metadata.source_id().clone(),
            self.metadata.revision().clone(),
            self.config.tier(),
            self.config.dataset().clone(),
            SourceIdentifier::try_from(series_id.as_str())
                .map_err(|_| SourceError::InvalidProtocolState)?,
            year,
            readiness,
            returned_series,
            returned_observations,
            observed_values,
            missing_values,
            preliminary_values,
            footnotes,
            provider_messages,
            page.response.response_time_millis(),
            page.received_at,
            response_bytes,
            response_content_digest,
            capture.content_digest(),
            capture.observation_digest(),
            capture.request_set_identity(),
            self.config.usage_policy().policy_digest(),
            self.config.root_rights_rejoin(),
            self.config.authorization().credential_rejoin(),
            self.config
                .usage_policy()
                .presentation_obligation_digest(),
            self.rate_declaration.declaration_digest(),
            self.config.limits(),
            Arc::clone(&self.runtime_instance),
        )
        .map_err(|_| SourceError::InvalidProtocolState)?;
        BlsDoctorOutput::try_new(report, capture_material)
            .map_err(|_| SourceError::InvalidProtocolState.into())
    }

    /// Fetches and discovers exact response objects only through current doctor-backed admission.
    pub async fn discover_with_activation(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        activation: &BlsActivationCandidate,
        cancellation: CancellationToken,
    ) -> Result<BlsDiscoveryOutput, ExtractionSourceError> {
        let started_at = system_timestamp().map_err(|_| SourceError::TrustedTimeUnavailable)?;
        self.validate_activation_operation_at(activation, started_at, request.deadline())
            .map_err(|_| SourceError::InvalidProtocolState)?;
        let discovered = self
            .discover_pages_impl(authority, request, activation, cancellation)
            .await?;
        let completed_at = system_timestamp().map_err(|_| SourceError::TrustedTimeUnavailable)?;
        self.validate_activation_candidate_at(activation, completed_at)
            .map_err(|_| SourceError::InvalidProtocolState)?;
        Ok(discovered)
    }

    async fn discover_pages_impl(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        activation: &BlsActivationCandidate,
        cancellation: CancellationToken,
    ) -> Result<BlsDiscoveryOutput, ExtractionSourceError> {
        self.validate_authority(&authority)?;
        if request.dataset() != self.config.dataset()
            || request.effective_at().is_some()
            || self.config.plan().chunks().len() > usize::from(request.max_results())
        {
            return Err(SourceError::InvalidProtocolState.into());
        }
        let mut discovered = Vec::with_capacity(self.config.plan().chunks().len());
        let mut capture_components = Vec::with_capacity(self.config.plan().chunks().len());
        for (index, chunk) in self.config.plan().chunks().iter().enumerate() {
            let page = self
                .fetch_page(&authority, chunk, request.deadline(), &cancellation)
                .await?;
            if page.response.is_partial() {
                return Err(SourceError::GenerationResynchronizationRequired.into());
            }
            let evidence = exact_evidence(&page.bytes);
            let object_id = SourceIdentifier::try_from(format!("bls:{index}:{}", page.sha256_hex))
                .map_err(|_| SourceError::InvalidProtocolState)?;
            let effective = EffectiveInterval::new(page.received_at, None)
                .map_err(|_| SourceError::InvalidProtocolState)?;
            let expected_bytes =
                u64::try_from(page.bytes.len()).map_err(|_| SourceError::InvalidProtocolState)?;
            let request_identity =
                capture_request_identity(&self.metadata, &self.config, index, chunk)?;
            let capture_material = self.capture_material_for_request(request_identity, &page)?;
            let capture_identity =
                SourceObjectCaptureIdentity::try_from_capture(capture_material.receipt())
                    .map_err(|_| SourceError::InvalidProtocolState)?;
            discovered.push(SourceObject::try_new_with_capture_identity(
                self.metadata.source_id().clone(),
                self.metadata.revision().clone(),
                &request,
                object_id.clone(),
                SourceIdentifier::try_from("application/json")
                    .map_err(|_| SourceError::InvalidProtocolState)?,
                evidence,
                capture_identity,
                effective,
                None,
                AvailabilityEvidence::LocalFirstObserved {
                    observed_at: page.received_at,
                },
                Some(expected_bytes),
            )?);
            capture_components.push(capture_material);
            let retained = self.cache
                .lock()
                .map_err(|_| SourceError::InvalidProtocolState)?
                .insert(&object_id, &page)?;
            if !retained {
                return Err(SourceError::FrameTooLarge {
                    max: market_squawk_sources::MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES as usize,
                }
                .into());
            }
        }
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled);
        }
        ensure_deadline_open(request.deadline())?;
        #[cfg(test)]
        self.apply_test_publication_action()?;
        authority.validate_current()?;
        let batch = DiscoveryBatch::try_new(&request, discovered)?;
        let capture_material = if capture_components.len() == 1 {
            capture_components
                .pop()
                .ok_or(SourceError::InvalidProtocolState)?
        } else {
            let graph_identity =
                discovery_capture_graph_identity(&self.metadata, &self.config, &request)?;
            ProviderCaptureMaterial::try_combine_request_graph(
                self.config.dataset().clone(),
                graph_identity,
                capture_components,
            )
            .map_err(|_| SourceError::InvalidProtocolState)?
        };
        let output = BlsDiscoveryOutput::new(
            batch,
            capture_material,
            Arc::clone(&self.runtime_instance),
            activation.candidate_digest(),
        );
        validate_discovery_output(&self.metadata, &self.config, &output)?;
        Ok(output)
    }

    /// Admits a discovery graph only after root physically sealed its exact raw responses.
    pub fn admit_sealed_discovery(
        &self,
        pending: BlsPendingDiscovery,
        sealed_capture: SealedProviderCaptureSetReceipt,
        activation: &BlsActivationCandidate,
    ) -> Result<BlsDiscoveryAdmission, BlsSourceError> {
        self.validate_activation_candidate(activation)?;
        BlsDiscoveryAdmission::try_new(
            pending,
            sealed_capture,
            &self.runtime_instance,
            activation,
        )
    }

    async fn normalized_admitted_page(
        &self,
        authority: &ExtractionAuthority,
        request: &ExtractionRequest,
        discovery_admission: &BlsDiscoveryObjectAdmission,
        activation: &BlsActivationCandidate,
        cancellation: CancellationToken,
    ) -> Result<BlsNormalizedPage, ExtractionSourceError> {
        self.validate_authority(authority)?;
        discovery_admission
            .validate_for_extraction(
                request,
                &self.runtime_instance,
                activation,
            )
            .map_err(|_| SourceError::InvalidProtocolState)?;
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled);
        }
        ensure_deadline_open(request.deadline())?;
        if request.object().source_id() != self.metadata.source_id()
            || request.object().metadata_revision() != self.metadata.revision()
            || request.object().dataset() != self.config.dataset()
        {
            return Err(SourceError::InvalidProtocolState.into());
        }
        let (chunk_index, object_digest) = parse_object_id(request.object().object_id())?;
        let chunk = self
            .config
            .plan()
            .chunks()
            .get(chunk_index)
            .ok_or(SourceError::InvalidProtocolState)?;
        if chunk_index != discovery_admission.chunk_index() {
            return Err(SourceError::InvalidProtocolState.into());
        }
        let cached = self
            .cache
            .lock()
            .map_err(|_| SourceError::InvalidProtocolState)?
            .pages
            .get(request.object().object_id().as_str())
            .cloned()
            .ok_or(SourceError::GenerationResynchronizationRequired)?;
        let bytes = Bytes::from_owner(cached.bytes);
        let requested = chunk
            .series()
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let response = crate::BlsResponse::parse_for_request(
            &bytes,
            self.config.tier(),
            &requested,
            chunk.start_year(),
            chunk.end_year(),
        )
        .map_err(|_| SourceError::InvalidProtocolState)?;
        let page = RetrievedBlsPage {
            bytes,
            response,
            received_at: cached.received_at,
            sha256_hex: cached.sha256_hex,
        };
        if page.response.is_partial()
            || page.sha256_hex != object_digest
            || !payload_matches_exact_evidence(&page.bytes, request.object().evidence())
            || page.received_at != discovery_admission.response_received_at()
        {
            return Err(SourceError::GenerationResynchronizationRequired.into());
        }
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled);
        }
        ensure_deadline_open(request.deadline())?;
        let observed_at = request.object().effective_interval().starts_at();
        let ingested_at = system_timestamp().map_err(|_| SourceError::TrustedTimeUnavailable)?;
        let records = canonical_records(
            &self.metadata,
            &self.config,
            &page.response,
            &page.bytes,
            observed_at,
            page.received_at,
            ingested_at,
        )
        .map_err(|_| SourceError::InvalidProtocolState)?;
        if records.len() > request.max_records() as usize {
            return Err(
                market_squawk_sources::ExtractionError::RecordLimitExceeded {
                    requested: request.max_records(),
                }
                .into(),
            );
        }
        let payload_bytes = records.iter().try_fold(0_u64, |total, record| {
            u64::try_from(record.payload.len())
                .ok()
                .and_then(|length| total.checked_add(length))
                .ok_or(SourceError::InvalidProtocolState)
        })?;
        if payload_bytes > request.max_bytes() {
            return Err(market_squawk_sources::ExtractionError::ByteLimitExceeded {
                requested: request.max_bytes(),
            }
            .into());
        }
        let payloads = records
            .iter()
            .map(|record| record.payload.clone())
            .collect();
        #[cfg(test)]
        self.apply_test_publication_action()?;
        authority.validate_current()?;
        Ok(BlsNormalizedPage {
            received_at: observed_at,
            response_received_at: page.received_at,
            source_payload_sha256: page.sha256_hex,
            exact_payload: page.bytes,
            payloads,
            records,
            response: page.response,
            canonical_ingested_at: ingested_at,
        })
    }

    /// Consumes one physically sealed discovery-object admission into canonical BLS records.
    ///
    /// Extraction performs no provider request and never substitutes a byte-identical refetch for
    /// the earlier first-observed response. Loss of the bounded in-process response cache fails
    /// closed; restart rejoin belongs to root's sealed-journal reader and a fresh admission.
    pub async fn extract_sealed_discovery(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        discovery_admission: BlsDiscoveryObjectAdmission,
        activation: &BlsActivationCandidate,
        cancellation: CancellationToken,
    ) -> Result<BlsExtractionOutput, ExtractionSourceError> {
        let started_at = system_timestamp().map_err(|_| SourceError::TrustedTimeUnavailable)?;
        self.validate_activation_operation_at(activation, started_at, request.deadline())
            .map_err(|_| SourceError::InvalidProtocolState)?;
        let page = self
            .normalized_admitted_page(
                &authority,
                &request,
                &discovery_admission,
                activation,
                cancellation,
            )
            .await?;
        let BlsNormalizedPage {
            received_at,
            response_received_at,
            response,
            canonical_ingested_at,
            records,
            ..
        } = page;
        let schema = SourceIdentifier::try_from(CURRENT_RESEARCH_RECORD_SCHEMA)
            .map_err(|_| SourceError::InvalidProtocolState)?;
        let records = records
            .into_iter()
            .map(|record| {
                market_squawk_sources::ExtractionRecord::try_new_with_time(
                    &request,
                    schema.clone(),
                    record.evidence,
                    record.effective,
                    None,
                    record.availability,
                    record.revision,
                    None,
                    record.payload,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let batch = ExtractionBatch::try_new(&request, records)?;
        let provider_semantics = BlsCanonicalProviderSemantics::try_new(
            &self.config,
            &response,
            &batch,
            received_at,
            response_received_at,
            canonical_ingested_at,
        )
        .map_err(|_| SourceError::InvalidProtocolState)?;
        #[cfg(test)]
        self.apply_test_publication_action()?;
        authority.validate_current()?;
        let completed_at = system_timestamp().map_err(|_| SourceError::TrustedTimeUnavailable)?;
        self.validate_activation_candidate_at(activation, completed_at)
            .map_err(|_| SourceError::InvalidProtocolState)?;
        Ok(BlsExtractionOutput {
            batch,
            provider_semantics,
            discovery_admission,
        })
    }

    fn capture_material_for_request(
        &self,
        request_identity: EvidenceDigest,
        page: &RetrievedBlsPage,
    ) -> Result<ProviderCaptureMaterial, ExtractionSourceError> {
        let body_bytes =
            u64::try_from(page.bytes.len()).map_err(|_| SourceError::InvalidProtocolState)?;
        let body_digest =
            EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(&page.bytes).into());
        let page_receipt = ProviderCapturePageReceipt::try_new(
            0,
            request_identity,
            None,
            None,
            200,
            body_bytes,
            body_digest,
            page.received_at,
        )
        .map_err(|_| SourceError::InvalidProtocolState)?;
        let receipt = ProviderCaptureSetReceipt::try_new(
            self.metadata.source_id().clone(),
            self.metadata.revision().clone(),
            self.config.dataset().clone(),
            request_identity,
            ProviderCaptureTerminalDisposition::StandaloneResponse,
            vec![page_receipt],
        )
        .map_err(|_| SourceError::InvalidProtocolState)?;
        let connection_id =
            Uuid::new_v5(&Uuid::NAMESPACE_URL, &receipt.observation_digest().bytes());
        let mut event_identity = Sha256::new();
        event_identity.update(b"market-squawk/bls-provider-capture-event/v1");
        event_identity.update(receipt.observation_digest().bytes());
        event_identity.update(body_digest.bytes());
        let event_id = Uuid::new_v5(&connection_id, &event_identity.finalize());
        let record = RawCaptureRecord::try_new_live(
            event_id,
            Arc::from(self.metadata.source_id().as_str()),
            connection_id,
            Some(0),
            None,
            DateTime::<Utc>::from_timestamp_nanos(page.received_at.unix_nanos()),
            page.bytes.clone(),
        )
        .map_err(|_| SourceError::InvalidProtocolState)?;
        ProviderCaptureMaterial::try_new(receipt, vec![record])
            .map_err(|_| SourceError::InvalidProtocolState.into())
    }

    fn validate_authority(
        &self,
        authority: &ExtractionAuthority,
    ) -> Result<(), ExtractionSourceError> {
        authority.validate_current()?;
        if authority.metadata() != &self.metadata {
            return Err(SourceError::InvalidProtocolState.into());
        }
        Ok(())
    }

    async fn fetch_page(
        &self,
        authority: &ExtractionAuthority,
        chunk: &crate::BlsRequestChunk,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<RetrievedBlsPage, ExtractionSourceError> {
        self.record_attempt()
            .map_err(|_| SourceError::InvalidProtocolState)?;
        let result = self
            .http
            .fetch(
                &self.metadata,
                self.config.authorization(),
                authority,
                chunk,
                deadline,
                cancellation,
            )
            .await;
        self.record_result(&result)?;
        result
    }

    fn record_attempt(&self) -> Result<(), BlsSourceError> {
        let now = system_timestamp()?;
        let mut health = self
            .health
            .lock()
            .map_err(|_| BlsSourceError::HealthUnavailable)?;
        health.last_attempt_at = Some(now);
        Ok(())
    }

    fn record_result(
        &self,
        result: &Result<RetrievedBlsPage, ExtractionSourceError>,
    ) -> Result<(), ExtractionSourceError> {
        let mut health = self
            .health
            .lock()
            .map_err(|_| SourceError::InvalidProtocolState)?;
        match result {
            Ok(page) => {
                health.last_success_at = Some(page.received_at);
                health.last_payload_digest = Some(Sha256::digest(&page.bytes).into());
                health.consecutive_failures = 0;
            }
            Err(_) => {
                health.consecutive_failures = health.consecutive_failures.saturating_add(1);
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn queue_test_publication_action(
        &self,
        registry_to_drop: Option<AuthoritativeSourceRegistry>,
    ) -> Result<(), BlsSourceError> {
        self.publication_actions
            .lock()
            .map_err(|_| BlsSourceError::HealthUnavailable)?
            .push_back(registry_to_drop);
        Ok(())
    }

    #[cfg(test)]
    fn apply_test_publication_action(&self) -> Result<(), ExtractionSourceError> {
        let registry_to_drop = self
            .publication_actions
            .lock()
            .map_err(|_| SourceError::InvalidProtocolState)?
            .pop_front()
            .flatten();
        drop(registry_to_drop);
        Ok(())
    }
}

impl SourceMetadataProvider for BlsSource {
    fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }
}

impl ExtractionSource for BlsSource {
    fn discover(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<DiscoveryBatch, ExtractionSourceError>> {
        let _ = (authority, request, cancellation);
        Box::pin(async { Err(SourceError::InvalidProtocolState.into()) })
    }

    fn extract(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<ExtractionBatch, ExtractionSourceError>> {
        let _ = (authority, request, cancellation);
        Box::pin(async { Err(SourceError::InvalidProtocolState.into()) })
    }
}

fn exact_evidence(payload: &[u8]) -> ExactPayloadEvidence {
    let digest = Sha256::digest(payload);
    ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.into(),
    ))
}

fn capture_request_identity(
    metadata: &SourceMetadata,
    config: &BlsSourceConfig,
    chunk_index: usize,
    chunk: &crate::BlsRequestChunk,
) -> Result<EvidenceDigest, ExtractionSourceError> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/bls-provider-capture-request/v1");
    hash_capture_field(&mut hash, metadata.source_id().as_str().as_bytes())?;
    hash_capture_field(
        &mut hash,
        metadata
            .revision()
            .as_source_identifier()
            .as_str()
            .as_bytes(),
    )?;
    hash_capture_field(&mut hash, config.dataset().as_str().as_bytes())?;
    hash_capture_field(&mut hash, config.authorization().endpoint().as_bytes())?;
    hash.update(
        u16::try_from(chunk_index)
            .map_err(|_| SourceError::InvalidProtocolState)?
            .to_be_bytes(),
    );
    hash.update(chunk.start_year().to_be_bytes());
    hash.update(chunk.end_year().to_be_bytes());
    hash.update(
        u16::try_from(chunk.series().len())
            .map_err(|_| SourceError::InvalidProtocolState)?
            .to_be_bytes(),
    );
    for series_id in chunk.series() {
        hash_capture_field(&mut hash, series_id.as_bytes())?;
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hash.finalize().into(),
    ))
}

fn discovery_capture_graph_identity(
    metadata: &SourceMetadata,
    config: &BlsSourceConfig,
    request: &DiscoveryRequest,
) -> Result<EvidenceDigest, ExtractionSourceError> {
    if config.plan().chunks().len() < 2 {
        return Err(SourceError::InvalidProtocolState.into());
    }
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/bls-discovery-request-graph/v1\0");
    hash_capture_field(&mut hash, metadata.source_id().as_str().as_bytes())?;
    hash_capture_field(
        &mut hash,
        metadata
            .revision()
            .as_source_identifier()
            .as_str()
            .as_bytes(),
    )?;
    hash_capture_field(&mut hash, config.dataset().as_str().as_bytes())?;
    let discovery_request_id = serde_json::to_vec(&request.request_id())
        .map_err(|_| SourceError::InvalidProtocolState)?;
    hash_capture_field(&mut hash, &discovery_request_id)?;
    hash.update(
        u16::try_from(config.plan().chunks().len())
            .map_err(|_| SourceError::InvalidProtocolState)?
            .to_be_bytes(),
    );
    for (index, chunk) in config.plan().chunks().iter().enumerate() {
        let component = capture_request_identity(metadata, config, index, chunk)?;
        hash.update(component.bytes());
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hash.finalize().into(),
    ))
}

fn validate_discovery_output(
    metadata: &SourceMetadata,
    config: &BlsSourceConfig,
    output: &BlsDiscoveryOutput,
) -> Result<(), ExtractionSourceError> {
    let objects = output.batch().objects();
    let capture = output.capture_material().receipt();
    let pages = capture.pages();
    let components = capture.request_graph_components();
    let chunk_count = config.plan().chunks().len();
    if output.batch().request().dataset() != config.dataset()
        || objects.len() != chunk_count
        || pages.len() != chunk_count
        || output.capture_material().records().len() != chunk_count
        || capture.source_id() != metadata.source_id()
        || capture.metadata_revision() != metadata.revision()
        || capture.dataset() != config.dataset()
        || (chunk_count == 1
            && (capture.terminal()
                != ProviderCaptureTerminalDisposition::StandaloneResponse
                || !components.is_empty()))
        || (chunk_count > 1
            && (capture.terminal()
                != ProviderCaptureTerminalDisposition::CompleteRequestGraph
                || components.len() != chunk_count
                || capture.request_set_identity()
                    != discovery_capture_graph_identity(metadata, config, output.batch().request())?))
    {
        return Err(SourceError::InvalidProtocolState.into());
    }

    let mut total_body_bytes = 0_u64;
    for (index, ((object, page), chunk)) in objects
        .iter()
        .zip(pages)
        .zip(config.plan().chunks())
        .enumerate()
    {
        let expected_request_identity = capture_request_identity(metadata, config, index, chunk)?;
        let (object_index, _) = parse_object_id(object.object_id())?;
        let SourceObjectCaptureIdentity::Paged {
            content_digest: object_capture_content_digest,
            page_count: object_capture_page_count,
            terminal: object_capture_terminal,
        } = object.capture_identity()
        else {
            return Err(SourceError::InvalidProtocolState.into());
        };
        total_body_bytes = total_body_bytes
            .checked_add(page.body_bytes())
            .ok_or(SourceError::InvalidProtocolState)?;
        if object_index != index
            || object.source_id() != metadata.source_id()
            || object.metadata_revision() != metadata.revision()
            || object.dataset() != config.dataset()
            || object.discovery_request_id() != output.batch().request().request_id()
            || object.evidence().content_digest() != page.body_digest()
            || object.expected_bytes() != Some(page.body_bytes())
            || object.effective_interval().starts_at() != page.received_at()
            || object.published_at().is_some()
            || object.availability()
                != &AvailabilityEvidence::LocalFirstObserved {
                    observed_at: page.received_at(),
                }
            || page.request_identity() != expected_request_identity
            || page.request_page_token_digest().is_some()
            || page.response_next_page_token_digest().is_some()
            || page.http_status() != 200
        {
            return Err(SourceError::InvalidProtocolState.into());
        }
        if chunk_count == 1 {
            if capture.request_set_identity() != expected_request_identity
                || object_capture_content_digest != capture.content_digest()
                || object_capture_page_count.get() != 1
                || object_capture_terminal != capture.terminal()
            {
                return Err(SourceError::InvalidProtocolState.into());
            }
        } else {
            let component = components
                .get(index)
                .ok_or(SourceError::InvalidProtocolState)?;
            if usize::from(component.ordinal()) != index
                || component.dataset() != config.dataset()
                || component.request_set_identity() != expected_request_identity
                || component.terminal()
                    != ProviderCaptureTerminalDisposition::StandaloneResponse
                || usize::from(component.first_page_ordinal()) != index
                || component.page_count().get() != 1
                || component.total_body_bytes() != page.body_bytes()
                || component.content_digest().bytes() == [0; 32]
                || component.observation_digest().bytes() == [0; 32]
                || object_capture_content_digest != component.content_digest()
                || object_capture_page_count != component.page_count()
                || object_capture_terminal != component.terminal()
            {
                return Err(SourceError::InvalidProtocolState.into());
            }
        }
    }
    if total_body_bytes != capture.total_body_bytes() {
        return Err(SourceError::InvalidProtocolState.into());
    }
    Ok(())
}

fn doctor_capture_request_identity(
    metadata: &SourceMetadata,
    config: &BlsSourceConfig,
    series_id: &str,
    year: u16,
) -> Result<EvidenceDigest, ExtractionSourceError> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/bls-provider-doctor-request/v1");
    hash_capture_field(&mut hash, metadata.source_id().as_str().as_bytes())?;
    hash_capture_field(
        &mut hash,
        metadata
            .revision()
            .as_source_identifier()
            .as_str()
            .as_bytes(),
    )?;
    hash_capture_field(&mut hash, config.dataset().as_str().as_bytes())?;
    hash_capture_field(&mut hash, config.authorization().endpoint().as_bytes())?;
    hash_capture_field(&mut hash, b"doctor")?;
    hash_capture_field(&mut hash, series_id.as_bytes())?;
    hash.update(year.to_be_bytes());
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hash.finalize().into(),
    ))
}

fn hash_capture_field(hash: &mut Sha256, value: &[u8]) -> Result<(), ExtractionSourceError> {
    let length = u16::try_from(value.len()).map_err(|_| SourceError::InvalidProtocolState)?;
    hash.update(length.to_be_bytes());
    hash.update(value);
    Ok(())
}

pub(crate) fn parse_object_id(
    object_id: &SourceIdentifier,
) -> Result<(usize, &str), ExtractionSourceError> {
    let mut fields = object_id.as_str().split(':');
    if fields.next() != Some("bls") {
        return Err(SourceError::InvalidProtocolState.into());
    }
    let index = fields
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .ok_or(SourceError::InvalidProtocolState)?;
    let digest = fields.next().ok_or(SourceError::InvalidProtocolState)?;
    if fields.next().is_some()
        || digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(SourceError::InvalidProtocolState.into());
    }
    Ok((index, digest))
}

impl BlsSourceConfig {
    /// Builds the bounded request plan and a dataset identity that binds every request semantic.
    ///
    /// # Errors
    ///
    /// Rejects malformed series/year plans or a dataset identity outside domain bounds.
    pub fn try_new(
        authorization: BlsAuthorization,
        root_rights_rejoin: BlsRootRightsRejoin,
        series_metadata: Vec<BlsSeriesMetadata>,
        start_year: u16,
        end_year: u16,
    ) -> Result<Self, BlsSourceError> {
        root_rights_rejoin.validate()?;
        let usage_policy = BlsUsagePolicy::private_personal_research_no_distribution()?;
        usage_policy.validate()?;
        let tier = authorization.tier();
        let mut metadata_by_series = BTreeMap::new();
        for metadata in series_metadata {
            let series_id = metadata.series_id().to_owned();
            if metadata_by_series.insert(series_id, metadata).is_some() {
                return Err(BlsSourceError::InvalidSeriesMetadata);
            }
        }
        let series = metadata_by_series.keys().cloned().collect::<Vec<_>>();
        let plan = BlsRequestPlan::try_new(tier, series, start_year, end_year)
            .map_err(|_| BlsSourceError::InvalidConfiguration)?;
        if plan
            .chunks()
            .len()
            .checked_add(1)
            .is_none_or(|attempts| attempts > usize::from(plan.limits().daily_queries()))
            || plan.chunks().len() > MAX_PROVIDER_CAPTURE_PAGES
        {
            return Err(BlsSourceError::InvalidConfiguration);
        }
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/bls-request-plan/v4");
        hash_field(&mut hash, BlsUsagePolicy::policy_id().as_bytes())?;
        let policy_digest = usage_policy.policy_digest();
        hash.update(match policy_digest.algorithm() {
            DigestAlgorithm::Sha256 => b"sha256".as_slice(),
            DigestAlgorithm::Blake3 => b"blake3".as_slice(),
        });
        hash.update(policy_digest.bytes());
        for rejoin_digest in [
            root_rights_rejoin.root_decision_digest(),
            root_rights_rejoin.provider_policy_digest(),
        ] {
            hash.update(match rejoin_digest.algorithm() {
                DigestAlgorithm::Sha256 => b"sha256".as_slice(),
                DigestAlgorithm::Blake3 => b"blake3".as_slice(),
            });
            hash.update(rejoin_digest.bytes());
        }
        let presentation_obligation = usage_policy.presentation_obligation()?;
        presentation_obligation.validate()?;
        let presentation_obligation_digest = presentation_obligation.obligation_digest();
        hash.update(match presentation_obligation_digest.algorithm() {
            DigestAlgorithm::Sha256 => b"sha256".as_slice(),
            DigestAlgorithm::Blake3 => b"blake3".as_slice(),
        });
        hash.update(presentation_obligation_digest.bytes());
        hash.update(match tier {
            BlsAccessTier::PublicV1 => b"public-v1".as_slice(),
            BlsAccessTier::RegisteredV2 => b"registered-v2".as_slice(),
        });
        let chunk_count =
            u16::try_from(plan.chunks().len()).map_err(|_| BlsSourceError::InvalidConfiguration)?;
        hash.update(chunk_count.to_be_bytes());
        for chunk in plan.chunks() {
            hash.update(chunk.start_year().to_be_bytes());
            hash.update(chunk.end_year().to_be_bytes());
            let series_count = u16::try_from(chunk.series().len())
                .map_err(|_| BlsSourceError::InvalidConfiguration)?;
            hash.update(series_count.to_be_bytes());
            for identifier in chunk.series() {
                let length = u16::try_from(identifier.len())
                    .map_err(|_| BlsSourceError::InvalidConfiguration)?;
                hash.update(length.to_be_bytes());
                hash.update(identifier.as_bytes());
            }
        }
        let metadata_count = u16::try_from(metadata_by_series.len())
            .map_err(|_| BlsSourceError::InvalidSeriesMetadata)?;
        hash.update(metadata_count.to_be_bytes());
        for metadata in metadata_by_series.values() {
            hash_field(&mut hash, metadata.series_id().as_bytes())?;
            hash_field(
                &mut hash,
                metadata.authorization_reference().as_str().as_bytes(),
            )?;
            let content_digest = metadata.evidence().content_digest();
            hash.update(match content_digest.algorithm() {
                DigestAlgorithm::Sha256 => b"sha256".as_slice(),
                DigestAlgorithm::Blake3 => b"blake3".as_slice(),
            });
            hash.update(content_digest.bytes());
        }
        let digest = hash.finalize();
        let tier_label = match tier {
            BlsAccessTier::PublicV1 => "public-v1",
            BlsAccessTier::RegisteredV2 => "registered-v2",
        };
        let dataset = SourceIdentifier::try_from(format!("bls:timeseries:{tier_label}:{digest:x}"))
            .map_err(|_| BlsSourceError::InvalidConfiguration)?;
        Ok(Self {
            authorization,
            usage_policy,
            root_rights_rejoin,
            plan,
            series_metadata: metadata_by_series,
            dataset,
        })
    }

    /// Returns the exact request-plan-bound dataset identity.
    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }

    /// Returns the exact provider tier used by this request plan.
    pub const fn tier(&self) -> BlsAccessTier {
        self.plan.tier()
    }

    /// Returns documented and enforced limits used by metadata and request composition.
    pub const fn limits(&self) -> BlsRequestLimits {
        self.plan.limits()
    }

    /// Returns the fixed provider-local private-use/no-distribution policy.
    pub const fn usage_policy(&self) -> BlsUsagePolicy {
        self.usage_policy
    }

    /// Returns the non-authoritative root rights coordinate bound into every source rejoin.
    pub const fn root_rights_rejoin(&self) -> BlsRootRightsRejoin {
        self.root_rights_rejoin
    }

    /// Returns the explicit public marker or protected registered credential generation.
    pub const fn credential_rejoin(&self) -> crate::BlsCredentialRejoin {
        self.authorization.credential_rejoin()
    }

    /// Returns the number of provider requests required for one complete discovery.
    pub fn chunk_count(&self) -> usize {
        self.plan.chunks().len()
    }

    /// Returns exact user-authorized semantic metadata for a configured series.
    pub fn series_metadata(&self, series_id: &str) -> Option<&BlsSeriesMetadata> {
        self.series_metadata.get(series_id)
    }

    pub(crate) const fn authorization(&self) -> &BlsAuthorization {
        &self.authorization
    }

    pub(crate) const fn plan(&self) -> &BlsRequestPlan {
        &self.plan
    }
}

fn hash_field(hash: &mut Sha256, value: &[u8]) -> Result<(), BlsSourceError> {
    let length = u16::try_from(value.len()).map_err(|_| BlsSourceError::InvalidSeriesMetadata)?;
    hash.update(length.to_be_bytes());
    hash.update(value);
    Ok(())
}

#[cfg(test)]
#[path = "source/tests.rs"]
mod tests;
