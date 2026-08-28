//! Application-owned composition for research ingestion and immutable analytical generations.

use std::sync::Arc;
use std::time::Instant;

use market_squawk_data::{
    AnalyticalDataService, AnalyticalManifestCatalog, AnalyticalReadCapability, CatalogAuthority,
    CatalogConfig, CatalogLimit, CommittedDataset, CompanyIdentityReadCapability,
    CompanySecurityLinkPublicationCapability, DatasetBuildError, DatasetBuildPrecommitAuthority,
    DatasetBuildRequest, DatasetBuilder, DatasetId, FairValueCatalogCapability,
    FeatureLabelDataset, IngestError, IngestIdentity, IngestPrecommitAuthority,
    InstrumentDefinitionReadCapability, ManifestCatalogError, MarketDataInstrumentReadCapability,
    MarketDataInstrumentSynchronizationCapability, ObjectStoreConfig, OnboardingCatalogCapability,
    ProviderPublicationInput, ResearchIngestService, RightsDecisionInput, RightsError,
    SourceOperation, extraction_provider_payload_digest,
};
use market_squawk_domain::{
    CompanyIdentityObservation, DigestAlgorithm, ExactPayloadEvidence, InstrumentDefinition,
    Timestamp,
};
use market_squawk_platform::{
    LocalPaths, PathError, SealedResearchJournalStore, SealedResearchJournalStoreError, SecretStore,
};
use market_squawk_sources::{
    ExtractionBatch, ExtractionRevisionPlan, ProviderCaptureMaterialSealError,
    ProviderCaptureSealRequest, ProviderRateAuthority, SealedProviderCaptureBinding,
    SealedProviderCaptureMaterial, SourceClass, SourceMetadata, SourceObjectCaptureIdentity,
};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::{ProviderOnboardingError, ProviderOnboardingService};

/// One rights-reserved normalized extraction, with provider revision evidence when required.
#[derive(Debug)]
pub struct ResearchIngestRequest {
    source: SourceMetadata,
    registered_at: market_squawk_domain::Timestamp,
    rights: RightsDecisionInput,
    identity: IngestIdentity,
    analytical_dataset: DatasetId,
    payload: ResearchIngestPayload,
    company_identity: Option<CompanyIdentityObservation>,
    precommit_authority: Option<Arc<dyn IngestPrecommitAuthority>>,
}

#[derive(Debug)]
enum ResearchIngestPayload {
    Local(ExtractionBatch),
    Provider {
        sealed_capture: SealedProviderCaptureBinding,
        revisions: ExtractionRevisionPlan,
    },
}

impl ResearchIngestRequest {
    /// Constructs a local-file or portfolio ingest whose revisions are locally observed.
    pub fn locally_observed(
        source: SourceMetadata,
        rights: RightsDecisionInput,
        analytical_dataset: DatasetId,
        batch: ExtractionBatch,
    ) -> Result<Self, ResearchServiceError> {
        Self::try_new_local(source, rights, analytical_dataset, batch)
    }

    /// Constructs a provider ingest from one adapter-produced canonical/native/physical binding.
    pub fn with_provider_publication(
        source: SourceMetadata,
        rights: RightsDecisionInput,
        analytical_dataset: DatasetId,
        sealed_capture: SealedProviderCaptureBinding,
        revisions: ExtractionRevisionPlan,
    ) -> Result<Self, ResearchServiceError> {
        sealed_capture.validate().map_err(IngestError::from)?;
        let batch = sealed_capture.batch();
        if matches!(
            source.source_class(),
            SourceClass::LocalFile | SourceClass::PortfolioExport
        ) || revisions.len() != batch.records().len()
        {
            return Err(ResearchServiceError::IngestAuthorityMismatch);
        }
        let (registered_at, identity) =
            validate_ingest_authority(&source, &rights, &analytical_dataset, batch, true)?;
        Ok(Self {
            source,
            registered_at,
            rights,
            identity,
            analytical_dataset,
            payload: ResearchIngestPayload::Provider {
                sealed_capture,
                revisions,
            },
            company_identity: None,
            precommit_authority: None,
        })
    }

    fn try_new_local(
        source: SourceMetadata,
        rights: RightsDecisionInput,
        analytical_dataset: DatasetId,
        batch: ExtractionBatch,
    ) -> Result<Self, ResearchServiceError> {
        if !matches!(
            source.source_class(),
            SourceClass::LocalFile | SourceClass::PortfolioExport
        ) {
            return Err(ResearchServiceError::IngestAuthorityMismatch);
        }
        let (registered_at, identity) =
            validate_ingest_authority(&source, &rights, &analytical_dataset, &batch, false)?;
        if !matches!(
            batch.request().object().capture_identity(),
            SourceObjectCaptureIdentity::Standalone
        ) {
            return Err(ResearchServiceError::IngestAuthorityMismatch);
        }
        Ok(Self {
            source,
            registered_at,
            rights,
            identity,
            analytical_dataset,
            payload: ResearchIngestPayload::Local(batch),
            company_identity: None,
            precommit_authority: None,
        })
    }

    pub(crate) fn with_precommit_authority(
        mut self,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
    ) -> Self {
        self.precommit_authority = Some(precommit_authority);
        self
    }

    pub(crate) fn with_company_identity(
        mut self,
        company_identity: CompanyIdentityObservation,
    ) -> Result<Self, ResearchServiceError> {
        if !matches!(&self.payload, ResearchIngestPayload::Provider { .. })
            || company_identity.source_id() != self.source.source_id()
            || company_identity
                .parent_ingest_payload_evidence()
                .content_digest()
                != self.identity.payload_digest()
        {
            return Err(ResearchServiceError::IngestAuthorityMismatch);
        }
        self.company_identity = Some(company_identity);
        Ok(self)
    }
}

fn validate_ingest_authority(
    source: &SourceMetadata,
    rights: &RightsDecisionInput,
    analytical_dataset: &DatasetId,
    batch: &ExtractionBatch,
    provider_publication: bool,
) -> Result<(Timestamp, IngestIdentity), ResearchServiceError> {
    let object = batch.request().object();
    let payload_digest = extraction_provider_payload_digest(batch);
    let source_id = object.source_id();
    if source.source_id() != source_id
        || source.revision() != object.metadata_revision()
        || &rights.source_id != source_id
        || rights.payload_digest != payload_digest
        || provider_publication
            == matches!(
                object.capture_identity(),
                SourceObjectCaptureIdentity::Standalone
            )
    {
        return Err(ResearchServiceError::IngestAuthorityMismatch);
    }
    let idempotency_key = provider_object_ingest_key(source, analytical_dataset, batch)?;
    let identity = IngestIdentity::try_new(
        source_id.clone(),
        payload_digest,
        SourceOperation::Persist,
        idempotency_key,
    )?;
    Ok((rights.retrieved_at, identity))
}

fn provider_object_ingest_key(
    source: &SourceMetadata,
    analytical_dataset: &DatasetId,
    batch: &ExtractionBatch,
) -> Result<String, ResearchServiceError> {
    let object = batch.request().object();
    if source.source_id() != object.source_id() || source.revision() != object.metadata_revision() {
        return Err(ResearchServiceError::IngestAuthorityMismatch);
    }
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/provider-object-ingest/v4");
    update_identity(&mut digest, object.source_id().as_str())?;
    update_identity(
        &mut digest,
        object.metadata_revision().as_source_identifier().as_str(),
    )?;
    update_evidence(&mut digest, source.revision_evidence().payload_evidence())?;
    update_identity(&mut digest, object.dataset().as_str())?;
    update_identity(&mut digest, analytical_dataset.as_str())?;
    update_identity(&mut digest, object.object_id().as_str())?;
    update_identity(&mut digest, object.media_type().as_str())?;
    update_evidence(&mut digest, object.evidence())?;
    match object.expected_bytes() {
        Some(bytes) => {
            digest.update([1]);
            digest.update(bytes.to_be_bytes());
        }
        None => digest.update([0]),
    }
    match object.capture_identity() {
        market_squawk_sources::SourceObjectCaptureIdentity::Standalone => digest.update([0]),
        market_squawk_sources::SourceObjectCaptureIdentity::Paged {
            content_digest,
            page_count,
            terminal,
        } => {
            digest.update([1]);
            digest.update(content_digest.bytes());
            digest.update(page_count.get().to_be_bytes());
            digest.update(match terminal {
                market_squawk_sources::ProviderCaptureTerminalDisposition::StandaloneResponse => {
                    b"standalone_response".as_slice()
                }
                market_squawk_sources::ProviderCaptureTerminalDisposition::ExhaustedWithoutNextPage => {
                    b"exhausted_without_next_page".as_slice()
                }
                market_squawk_sources::ProviderCaptureTerminalDisposition::CompleteRequestGraph => {
                    b"complete_request_graph".as_slice()
                }
            });
        }
    }
    Ok(format!(
        "provider-object-v4-{}",
        encode_lower_hex(digest.finalize().into())
    ))
}

fn update_evidence(
    digest: &mut Sha256,
    evidence: &ExactPayloadEvidence,
) -> Result<(), ResearchServiceError> {
    let content = evidence.content_digest();
    digest.update([match content.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    digest.update(content.bytes());
    match evidence.version_pinned_locator() {
        Some(locator) => {
            digest.update([1]);
            update_identity(digest, locator.reference().as_str())?;
            update_identity(digest, locator.version().as_str())?;
        }
        None => digest.update([0]),
    }
    Ok(())
}

fn update_identity(digest: &mut Sha256, value: &str) -> Result<(), ResearchServiceError> {
    let length =
        u64::try_from(value.len()).map_err(|_error| ResearchServiceError::IdentityOverflow)?;
    digest.update(length.to_be_bytes());
    digest.update(value.as_bytes());
    Ok(())
}

fn encode_lower_hex(bytes: [u8; 32]) -> String {
    use std::fmt::Write as _;

    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

/// Single application authority for local analytical storage and dataset construction.
#[derive(Debug)]
pub struct ResearchService {
    analytical: AnalyticalDataService,
    provider_captures: Arc<SealedResearchJournalStore>,
    provider_capture_seal_gate: Arc<Semaphore>,
}

impl ResearchService {
    /// Opens the existing analytical authority or initializes a genuinely fresh local root.
    ///
    /// This method never treats corruption, composition drift, or incomplete recovery as a fresh
    /// installation. Initialization is attempted only when the catalog explicitly reports that
    /// the artifact-root authority has never been established.
    pub fn open_or_initialize(
        paths: &LocalPaths,
        catalog: CatalogConfig,
        max_objects_per_generation: usize,
        objects: ObjectStoreConfig,
    ) -> Result<Self, ResearchServiceError> {
        match Self::open(paths, catalog.clone(), max_objects_per_generation, objects) {
            Ok(service) => Ok(service),
            Err(ResearchServiceError::Ingest(IngestError::Catalog(
                market_squawk_data::CatalogError::ArtifactRootAuthorityInitializationRequired,
            ))) => Self::initialize(paths, catalog, max_objects_per_generation, objects),
            Err(error) => Err(error),
        }
    }

    /// Creates and durably binds a fresh catalog and controlled artifact root.
    pub fn initialize(
        paths: &LocalPaths,
        catalog: CatalogConfig,
        max_objects_per_generation: usize,
        objects: ObjectStoreConfig,
    ) -> Result<Self, ResearchServiceError> {
        let authority = CatalogAuthority::open(catalog)?;
        let manifests =
            AnalyticalManifestCatalog::open(paths.catalog()?, max_objects_per_generation)?;
        let analytical = AnalyticalDataService::initialize(
            authority,
            manifests,
            paths.artifacts()?.clone(),
            objects,
        )?;
        Self::from_analytical(paths, analytical)
    }

    /// Opens or initializes a safe research and provider-onboarding service composition.
    ///
    /// The catalog writer is consumed inside this boundary and never returned to the caller.
    pub fn open_or_initialize_with_provider_onboarding_service<S>(
        paths: &LocalPaths,
        catalog: CatalogConfig,
        max_objects_per_generation: usize,
        objects: ObjectStoreConfig,
        secrets: Arc<S>,
        provider_rate: ProviderRateAuthority,
    ) -> Result<(Self, ProviderOnboardingService), ResearchServiceError>
    where
        S: SecretStore + 'static,
    {
        let (research, onboarding_catalog) = Self::open_or_initialize_with_provider_onboarding(
            paths,
            catalog,
            max_objects_per_generation,
            objects,
        )?;
        let onboarding = ProviderOnboardingService::try_new_with_provider_rate(
            onboarding_catalog,
            secrets,
            provider_rate,
        )?;
        Ok((research, onboarding))
    }

    /// Internal installed-composition boundary for the restricted onboarding facade.
    ///
    /// The [`ResearchService`] retains no onboarding writer, and its ordinary constructors do not
    /// compose the facade.
    pub(crate) fn open_or_initialize_with_provider_onboarding(
        paths: &LocalPaths,
        catalog: CatalogConfig,
        max_objects_per_generation: usize,
        objects: ObjectStoreConfig,
    ) -> Result<(Self, OnboardingCatalogCapability), ResearchServiceError> {
        match Self::open_provider_onboarding_composition(
            paths,
            catalog.clone(),
            max_objects_per_generation,
            objects,
        ) {
            Ok(composition) => Ok(composition),
            Err(ResearchServiceError::Ingest(IngestError::Catalog(
                market_squawk_data::CatalogError::ArtifactRootAuthorityInitializationRequired,
            ))) => Self::initialize_provider_onboarding_composition(
                paths,
                catalog,
                max_objects_per_generation,
                objects,
            ),
            Err(error) => Err(error),
        }
    }

    fn initialize_provider_onboarding_composition(
        paths: &LocalPaths,
        catalog: CatalogConfig,
        max_objects_per_generation: usize,
        objects: ObjectStoreConfig,
    ) -> Result<(Self, OnboardingCatalogCapability), ResearchServiceError> {
        let authority = CatalogAuthority::open(catalog)?;
        let manifests =
            AnalyticalManifestCatalog::open(paths.catalog()?, max_objects_per_generation)?;
        let (analytical, onboarding_catalog) =
            AnalyticalDataService::initialize_with_provider_onboarding(
                authority,
                manifests,
                paths.artifacts()?.clone(),
                objects,
            )?;
        let service = Self::from_analytical(paths, analytical)?;
        Ok((service, onboarding_catalog))
    }

    /// Reopens an already bound catalog and artifact root without implicit migration.
    pub fn open(
        paths: &LocalPaths,
        catalog: CatalogConfig,
        max_objects_per_generation: usize,
        objects: ObjectStoreConfig,
    ) -> Result<Self, ResearchServiceError> {
        let authority = CatalogAuthority::open(catalog)?;
        let manifests =
            AnalyticalManifestCatalog::open(paths.catalog()?, max_objects_per_generation)?;
        let analytical =
            AnalyticalDataService::open(authority, manifests, paths.artifacts()?.clone(), objects)?;
        Self::from_analytical(paths, analytical)
    }

    fn open_provider_onboarding_composition(
        paths: &LocalPaths,
        catalog: CatalogConfig,
        max_objects_per_generation: usize,
        objects: ObjectStoreConfig,
    ) -> Result<(Self, OnboardingCatalogCapability), ResearchServiceError> {
        let authority = CatalogAuthority::open(catalog)?;
        let manifests =
            AnalyticalManifestCatalog::open(paths.catalog()?, max_objects_per_generation)?;
        let (analytical, onboarding_catalog) =
            AnalyticalDataService::open_with_provider_onboarding(
                authority,
                manifests,
                paths.artifacts()?.clone(),
                objects,
            )?;
        let service = Self::from_analytical(paths, analytical)?;
        Ok((service, onboarding_catalog))
    }

    fn from_analytical(
        paths: &LocalPaths,
        analytical: AnalyticalDataService,
    ) -> Result<Self, ResearchServiceError> {
        Ok(Self {
            analytical,
            provider_captures: Arc::new(paths.sealed_research_journal_store()?),
            provider_capture_seal_gate: Arc::new(Semaphore::new(1)),
        })
    }

    /// Verifies every catalog-retained provider capture before a provider runtime is published.
    ///
    /// Incomplete stages and unreferenced final objects are quarantined by the sole sealed-store
    /// owner. A retained claim is never trusted from SQLite alone: its exact MSJ1 bytes are opened,
    /// hashed, and replay-validated during this recovery boundary.
    pub async fn recover_provider_capture_store(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<market_squawk_platform::SealedResearchJournalRecoveryReport, ResearchServiceError>
    {
        self.analytical
            .recover_provider_capture_store(Arc::clone(&self.provider_captures), cancellation)
            .await
            .map_err(Into::into)
    }

    /// Consumes and seals one already validated provider capture without exposing store authority.
    ///
    /// The synchronous filesystem work runs on one application-owned blocking lane. Cancellation
    /// and the monotonic deadline race both lane admission and completion; a late unreferenced
    /// segment remains recoverable by the startup quarantine pass.
    pub(crate) async fn seal_provider_capture(
        &self,
        request: ProviderCaptureSealRequest,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<SealedProviderCaptureMaterial, ResearchServiceError> {
        let deadline = tokio::time::Instant::from_std(deadline);
        let permit = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                return Err(ResearchServiceError::Ingest(IngestError::Cancelled));
            }
            () = tokio::time::sleep_until(deadline) => {
                return Err(ResearchServiceError::Ingest(IngestError::DeadlineExceeded));
            }
            permit = Arc::clone(&self.provider_capture_seal_gate).acquire_owned() => {
                permit.map_err(|_error| ResearchServiceError::ProviderCaptureSealWorkerUnavailable)?
            }
        };
        let store = Arc::clone(&self.provider_captures);
        let mut worker = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            request.seal(store.as_ref())
        });
        tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                reap_provider_capture_seal(worker);
                Err(ResearchServiceError::Ingest(IngestError::Cancelled))
            }
            () = tokio::time::sleep_until(deadline) => {
                reap_provider_capture_seal(worker);
                Err(ResearchServiceError::Ingest(IngestError::DeadlineExceeded))
            }
            result = &mut worker => {
                result
                    .map_err(|_error| ResearchServiceError::ProviderCaptureSealWorkerUnavailable)?
                    .map_err(map_provider_capture_seal_error)
            }
        }
    }

    /// Executes one rights-reserved ingest through durable revision and publication authority.
    pub async fn ingest(
        &self,
        request: ResearchIngestRequest,
        cancellation: CancellationToken,
    ) -> Result<CommittedDataset, ResearchServiceError> {
        let ResearchIngestRequest {
            source,
            registered_at,
            rights,
            identity,
            analytical_dataset,
            payload,
            company_identity,
            precommit_authority,
        } = request;
        let reservation = self
            .analytical
            .reserve_source_ingest(&source, registered_at, rights, &identity, &cancellation)
            .await?;
        match payload {
            ResearchIngestPayload::Provider {
                sealed_capture,
                revisions,
            } => {
                let mut publication = ProviderPublicationInput::try_new(sealed_capture, revisions)?;
                if let Some(company_identity) = company_identity {
                    publication = publication.with_company_identity(company_identity);
                }
                if let Some(precommit_authority) = precommit_authority {
                    publication = publication.with_precommit_authority(precommit_authority);
                }
                self.analytical
                    .ingest_provider_publication(
                        reservation,
                        analytical_dataset,
                        publication,
                        cancellation,
                    )
                    .await
                    .map_err(Into::into)
            }
            ResearchIngestPayload::Local(batch) => match (company_identity, precommit_authority) {
                (None, Some(precommit_authority)) => self
                    .analytical
                    .ingest_with_precommit_authority(
                        reservation,
                        analytical_dataset,
                        batch,
                        cancellation,
                        precommit_authority,
                    )
                    .await
                    .map_err(Into::into),
                (None, None) => self
                    .analytical
                    .ingest(reservation, analytical_dataset, batch, cancellation)
                    .await
                    .map_err(Into::into),
                (Some(_), _) => Err(ResearchServiceError::IngestAuthorityMismatch),
            },
        }
    }

    /// Builds one authorized phase-one, point-in-time derived generation.
    ///
    /// The returned generation is immutable and restart-queryable by its exact manifest. It does
    /// not carry product admission, model admission, or execution authority.
    pub async fn build_phase_one_derived_generation(
        &self,
        request: DatasetBuildRequest,
        cancellation: CancellationToken,
    ) -> Result<FeatureLabelDataset, ResearchServiceError> {
        self.analytical
            .dataset_builder()
            .build(request, cancellation)
            .await
            .map_err(Into::into)
    }

    /// Builds phase one while retaining exact caller authority through generation publication.
    ///
    /// The precommit authority is consumed only for this immutable analytical generation; no
    /// product receipt or issuer authority is minted by this service boundary.
    pub async fn build_phase_one_derived_generation_with_precommit_authority(
        &self,
        request: DatasetBuildRequest,
        cancellation: CancellationToken,
        precommit_authority: Arc<dyn DatasetBuildPrecommitAuthority>,
    ) -> Result<FeatureLabelDataset, ResearchServiceError> {
        self.analytical
            .dataset_builder()
            .build_with_precommit_authority(request, cancellation, precommit_authority)
            .await
            .map_err(Into::into)
    }

    /// Returns the manifest-pinned analytical service for bounded query composition.
    pub const fn analytical(&self) -> &AnalyticalDataService {
        &self.analytical
    }

    /// Returns immutable bounded analytical metadata and fixed-template observation reads.
    pub fn analytical_reader(&self) -> AnalyticalReadCapability {
        self.analytical.analytical_reader()
    }

    /// Returns fair-value persistence authority over this service's sole catalog writer.
    pub fn fair_value_catalog(&self) -> FairValueCatalogCapability {
        self.analytical.fair_value_catalog()
    }

    /// Returns bounded point-in-time definition reads over this service's sole catalog session.
    pub fn instrument_definitions(&self) -> InstrumentDefinitionReadCapability {
        self.analytical.instrument_definitions()
    }

    /// Returns bounded reads over FIGI-backed, explicitly non-executable market identities.
    pub fn market_data_instruments(&self) -> MarketDataInstrumentReadCapability {
        self.analytical.market_data_instruments()
    }

    /// Returns the sole atomic publisher for FIGI-backed market-data identities.
    pub fn market_data_instrument_synchronization(
        &self,
    ) -> MarketDataInstrumentSynchronizationCapability {
        self.analytical.market_data_instrument_synchronization()
    }

    /// Returns bounded company-identity reads over the canonical research catalog.
    pub fn company_identities(&self) -> CompanyIdentityReadCapability {
        self.analytical.company_identities()
    }

    /// Returns the pure, narrow publisher for a fully evidenced company/security link.
    ///
    /// Desktop preview and confirmation workflow state is deliberately not owned here.
    pub fn company_security_link_publication(&self) -> CompanySecurityLinkPublicationCapability {
        self.analytical.company_security_link_publication()
    }

    /// Atomically reconciles validated code/config-owned definitions before product publication.
    pub(crate) fn synchronize_configured_instruments(
        &self,
        instruments: &[InstrumentDefinition],
        observed_at: Timestamp,
        limit: CatalogLimit,
    ) -> Result<usize, ResearchServiceError> {
        self.analytical
            .instrument_catalog()
            .synchronize(instruments, observed_at, limit)
            .map_err(Into::into)
    }
}

fn map_provider_capture_seal_error(
    error: ProviderCaptureMaterialSealError,
) -> ResearchServiceError {
    match error {
        ProviderCaptureMaterialSealError::Store(error) => {
            ResearchServiceError::ProviderCaptureStore(error)
        }
        ProviderCaptureMaterialSealError::Capture(error) => {
            ResearchServiceError::Ingest(IngestError::ProviderCapture(error))
        }
    }
}

fn reap_provider_capture_seal(
    worker: tokio::task::JoinHandle<
        Result<SealedProviderCaptureMaterial, ProviderCaptureMaterialSealError>,
    >,
) {
    tokio::spawn(async move {
        let _late_result = worker.await;
    });
}

/// Research composition, storage, ingestion, or analytical-generation failure.
#[derive(Debug, Error)]
pub enum ResearchServiceError {
    /// Local controlled paths could not be resolved.
    #[error("research service local path is unavailable: {0}")]
    Path(#[from] PathError),
    /// The durable source/rights catalog could not be opened.
    #[error("research service catalog failed: {0}")]
    Catalog(#[from] market_squawk_data::CatalogError),
    /// The immutable generation catalog could not be opened.
    #[error("research service manifest catalog failed: {0}")]
    Manifest(#[from] ManifestCatalogError),
    /// The sealed exact-provider-response authority could not be opened or verified.
    #[error("research service provider-capture store failed: {0}")]
    ProviderCaptureStore(#[from] SealedResearchJournalStoreError),
    /// The bounded provider-capture sealing worker could not be admitted or joined.
    #[error("research service provider-capture sealing worker is unavailable")]
    ProviderCaptureSealWorkerUnavailable,
    /// Analytical authority composition or ingestion failed.
    #[error("research service ingestion failed: {0}")]
    Ingest(#[from] IngestError),
    /// The fully composed provider-onboarding service could not be constructed.
    #[error("research provider-onboarding composition failed: {0}")]
    ProviderOnboarding(#[from] ProviderOnboardingError),
    /// Phase-one point-in-time derived-generation construction failed.
    #[error("research service phase-one derived-generation build failed: {0}")]
    Dataset(#[from] DatasetBuildError),
    /// The composed source, rights, and exact extraction payload do not agree.
    #[error("research ingest source, rights, and batch evidence do not agree")]
    IngestAuthorityMismatch,
    /// The idempotency identity is invalid.
    #[error("research ingest identity failed: {0}")]
    Rights(#[from] RightsError),
    /// A provider-object identity field could not be represented in the canonical hash framing.
    #[error("research ingest identity length overflow")]
    IdentityOverflow,
}
