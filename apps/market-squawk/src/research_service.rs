//! Application-owned composition for research ingestion and immutable analytical generations.

mod worker;

use worker::ResearchIoWorker;

use std::sync::Arc;
use std::time::Instant;

use market_squawk_data::{
    AnalyticalDataService, AnalyticalManifestCatalog, AnalyticalReadCapability, CatalogAuthority,
    CatalogConfig, CatalogLimit, CommittedDataset, CompanyIdentityReadCapability,
    CompanySecurityIdentityReadCapability, CompanySecurityLinkPublicationCapability,
    DatasetBuildError, DatasetBuildPrecommitAuthority, DatasetBuildRequest, DatasetBuilder,
    DatasetId, FairValueCatalogCapability, FeatureDatasetProductionPublisher, FeatureLabelDataset,
    IngestError, IngestIdentity, IngestPrecommitAuthority, InstrumentDefinitionReadCapability,
    ManifestCatalogError, MarketDataInstrumentReadCapability,
    MarketDataInstrumentSynchronizationCapability, ObjectStoreConfig, OnboardingCatalogCapability,
    PersistedProviderCaptureBindingEvidence, ProviderPublicationInput, ResearchIngestService,
    RightsDecisionInput, RightsError, SourceOperation, extraction_provider_payload_digest,
};
use market_squawk_domain::{
    CompanyIdentityObservation, DigestAlgorithm, ExactPayloadEvidence, InstrumentDefinition,
    ResearchObservation, Timestamp,
};
use market_squawk_platform::{
    LocalPaths, PathError, SealedResearchJournalStore, SealedResearchJournalStoreError, SecretStore,
};
use market_squawk_sources::{
    ExtractionBatch, ExtractionRevisionPlan, ProviderCaptureMaterialSealError,
    ProviderCaptureSealRequest, ProviderNativeLineageImplementation, ProviderRateAuthority,
    SealedProviderCaptureBinding, SealedProviderCaptureMaterial, SourceClass, SourceMetadata,
    SourceObjectCaptureIdentity,
};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
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

fn validate_census_reobservation(
    original: &PersistedProviderCaptureBindingEvidence,
    fresh: &PersistedProviderCaptureBindingEvidence,
    fresh_rows: &[ResearchObservation],
) -> Result<(), IngestError> {
    let reopen = |evidence: &PersistedProviderCaptureBindingEvidence| {
        let native = evidence.native_lineage();
        if native.implementation() != "census_tabular_v1"
            || evidence.scope() != "whole"
            || evidence.component_ordinal().is_some()
            || evidence.record_count() == 0
            || evidence.rows().len() != evidence.record_count()
            || native.row_count() != evidence.record_count()
        {
            return Err(IngestError::ReplayConflict);
        }
        let plan = market_squawk_adapter_census::CensusPublicationPlan::try_from_retained_payload(
            native
                .batch_sidecar_semantic_payload()
                .ok_or(IngestError::ReplayConflict)?,
            evidence.capture(),
            evidence.extraction_content_identity(),
        )
        .map_err(|_| IngestError::ReplayConflict)?;
        if plan.observations().len() != evidence.record_count() {
            return Err(IngestError::ReplayConflict);
        }
        for (ordinal, row) in evidence.rows().iter().enumerate() {
            if usize::try_from(row.canonical_row_ordinal()).ok() != Some(ordinal) {
                return Err(IngestError::ReplayConflict);
            }
            plan.validate_native_row(ordinal, row.native_semantic_payload())
                .map_err(|_| IngestError::ReplayConflict)?;
        }
        Ok(plan)
    };
    if original.native_lineage().version() != fresh.native_lineage().version()
        || original.native_lineage().fingerprint() != fresh.native_lineage().fingerprint()
        || original.record_count() != fresh.record_count()
        || fresh_rows.len() != fresh.record_count()
    {
        return Err(IngestError::ReplayConflict);
    }
    let original_plan = reopen(original)?;
    let fresh_plan = reopen(fresh)?;
    for (ordinal, observation) in fresh_rows.iter().enumerate() {
        let ResearchObservation::Macro(observation) = observation else {
            return Err(IngestError::ReplayConflict);
        };
        fresh_plan
            .validate_canonical_observation(ordinal, observation)
            .map_err(|_| IngestError::ReplayConflict)?;
    }
    fresh_plan
        .validate_reobservation_of(&original_plan)
        .map_err(|_| IngestError::ReplayConflict)
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
    analytical: Arc<AnalyticalDataService>,
    provider_captures: Arc<SealedResearchJournalStore>,
    provider_capture_worker: ResearchIoWorker,
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
        Self::from_analytical(paths, Arc::new(analytical))
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
    ) -> Result<
        (
            Self,
            ProviderOnboardingService,
            FeatureDatasetProductionPublisher,
        ),
        ResearchServiceError,
    >
    where
        S: SecretStore + 'static,
    {
        let (research, onboarding_catalog, publisher) =
            Self::open_or_initialize_with_provider_onboarding(
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
        Ok((research, onboarding, publisher))
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
    ) -> Result<
        (
            Self,
            OnboardingCatalogCapability,
            FeatureDatasetProductionPublisher,
        ),
        ResearchServiceError,
    > {
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
    ) -> Result<
        (
            Self,
            OnboardingCatalogCapability,
            FeatureDatasetProductionPublisher,
        ),
        ResearchServiceError,
    > {
        let authority = CatalogAuthority::open(catalog)?;
        let manifests =
            AnalyticalManifestCatalog::open(paths.catalog()?, max_objects_per_generation)?;
        let (analytical_composition, onboarding_catalog) =
            AnalyticalDataService::initialize_with_provider_onboarding(
                authority,
                manifests,
                paths.artifacts()?.clone(),
                objects,
            )?;
        let (analytical, publisher) = analytical_composition.into_parts();
        let service = Self::from_analytical(paths, Arc::new(analytical))?;
        Ok((service, onboarding_catalog, publisher))
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
        Self::from_analytical(paths, Arc::new(analytical))
    }

    fn open_provider_onboarding_composition(
        paths: &LocalPaths,
        catalog: CatalogConfig,
        max_objects_per_generation: usize,
        objects: ObjectStoreConfig,
    ) -> Result<
        (
            Self,
            OnboardingCatalogCapability,
            FeatureDatasetProductionPublisher,
        ),
        ResearchServiceError,
    > {
        let authority = CatalogAuthority::open(catalog)?;
        let manifests =
            AnalyticalManifestCatalog::open(paths.catalog()?, max_objects_per_generation)?;
        let (analytical_composition, onboarding_catalog) =
            AnalyticalDataService::open_with_provider_onboarding(
                authority,
                manifests,
                paths.artifacts()?.clone(),
                objects,
            )?;
        let (analytical, publisher) = analytical_composition.into_parts();
        let service = Self::from_analytical(paths, Arc::new(analytical))?;
        Ok((service, onboarding_catalog, publisher))
    }

    /// Shares the catalog owner without opening another catalog writer.
    pub(crate) fn from_analytical(
        paths: &LocalPaths,
        analytical: Arc<AnalyticalDataService>,
    ) -> Result<Self, ResearchServiceError> {
        Ok(Self {
            analytical,
            provider_captures: Arc::new(paths.sealed_research_journal_store()?),
            provider_capture_worker: ResearchIoWorker::new(),
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
        self.seal_provider_capture_with_job_context(None, request, cancellation, deadline)
            .await
    }

    /// Seals source input under this job's cancellation without claiming a job result publication.
    pub(crate) async fn seal_provider_capture_for_job(
        &self,
        job: &market_squawk_jobs::JobRunContext,
        request: ProviderCaptureSealRequest,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<SealedProviderCaptureMaterial, ResearchServiceError> {
        self.seal_provider_capture_with_job_context(Some(job), request, cancellation, deadline)
            .await
    }

    async fn seal_provider_capture_with_job_context(
        &self,
        job: Option<&market_squawk_jobs::JobRunContext>,
        request: ProviderCaptureSealRequest,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<SealedProviderCaptureMaterial, ResearchServiceError> {
        let store = Arc::clone(&self.provider_captures);
        self.provider_capture_worker
            .run_with_job_context(
                job.map(|job| job.cancellation()),
                deadline,
                cancellation,
                move |cancellation| {
                    if cancellation.is_cancelled() {
                        return Err(IngestError::Cancelled.into());
                    }
                    if Instant::now() >= deadline {
                        return Err(IngestError::DeadlineExceeded.into());
                    }
                    request
                        .seal(store.as_ref())
                        .map_err(map_provider_capture_seal_error)
                },
            )
            .await?
    }

    /// Physically verifies an original metadata seal on the existing supervised I/O owner.
    pub(crate) async fn verify_provider_macro_metadata_capture(
        &self,
        receipt: market_squawk_sources::SealedProviderCaptureSetReceipt,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<market_squawk_data::ProviderMacroMetadataCapture, ResearchServiceError> {
        let analytical = Arc::clone(&self.analytical);
        let store = Arc::clone(&self.provider_captures);
        self.provider_capture_worker.run(deadline, cancellation, move |worker_cancellation| {
            analytical.verify_provider_macro_metadata_capture(receipt, store.as_ref(), deadline, &worker_cancellation)
                .map_err(ResearchServiceError::from)
        }).await?
    }

    /// Runs synchronous capture, verification or source-preparation work on the existing lane.
    ///
    /// The closure must retain only the exact data capabilities it needs, never an Arc to this
    /// service, and must check the supplied cancellation token and original deadline. Its typed
    /// result is returned only after the original blocking handle joins. Domain errors carried
    /// by `T` are operation results, separate from failure to join the worker itself.
    pub(crate) async fn run_owned_research_io<T, F>(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
        operation: F,
    ) -> Result<T, ResearchServiceError>
    where
        T: Send + 'static,
        F: FnOnce(CancellationToken) -> T + Send + 'static,
    {
        self.provider_capture_worker
            .run(deadline, cancellation, operation)
            .await
    }

    /// Admits exact lineage on the original retained synchronous I/O lane.
    /// The worker owns only the existing analytical service, never this worker's owner.
    pub(crate) async fn authorize_research_use(
        &self,
        request: market_squawk_data::ResearchUseRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<
        Result<
            market_squawk_data::AuthorizedResearchUse,
            market_squawk_data::ResearchUseCatalogError,
        >,
        ResearchServiceError,
    > {
        let analytical = Arc::clone(&self.analytical);
        let result = self
            .run_owned_research_io(deadline, cancellation, move |worker_cancellation| {
                analytical.authorize_research_use(request, &worker_cancellation)
            })
            .await?;
        if cancellation.is_cancelled() {
            return Err(market_squawk_data::IngestError::Cancelled.into());
        }
        if Instant::now() >= deadline {
            return Err(market_squawk_data::IngestError::DeadlineExceeded.into());
        }
        Ok(result)
    }

    /// Closes admission and cancels original reads without discarding their blocking handles.
    pub(crate) fn begin_owned_io_shutdown(&self) {
        self.provider_capture_worker.begin_shutdown();
    }

    /// Joins the original capture/read worker; a timed-out caller can retry the same owner.
    pub(crate) async fn finish_owned_io_shutdown(
        &self,
        deadline: Instant,
    ) -> Result<(), ResearchServiceError> {
        self.provider_capture_worker.finish_shutdown(deadline).await
    }

    /// Rejoins the fixed analytical selection to bounded original native/physical custody in the
    /// same supervised worker used for retained provider reads. No independent task is detached.
    pub(crate) async fn read_selected_provider_capture_evidence(
        &self,
        selection: market_squawk_data::SelectedProviderCaptureRows,
        maximum_bytes: usize,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<market_squawk_data::SelectedProviderCaptureEvidence, ResearchServiceError> {
        let analytical = Arc::clone(&self.analytical);
        let store = Arc::clone(&self.provider_captures);
        self.provider_capture_worker
            .run_with_job_context(None, deadline, cancellation, move |worker_cancellation| {
                analytical
                    .selected_provider_capture_evidence_bounded(
                        selection,
                        maximum_bytes,
                        store.as_ref(),
                        deadline,
                        &worker_cancellation,
                    )
                    .map_err(Into::into)
            })
            .await?
    }

    /// Reopens one original generation and performs a bounded typed read in the existing raw
    /// worker lane. The callback cannot mint publication authority or use another object store.
    pub(crate) async fn read_provider_capture_generation<T, F>(
        &self,
        manifest: market_squawk_data::DatasetManifestRef,
        deadline: Instant,
        cancellation: &CancellationToken,
        read: F,
    ) -> Result<T, ResearchServiceError>
    where
        T: Send + 'static,
        F: FnOnce(
                market_squawk_data::GenerationOwnedProviderCaptureEvidence,
                &SealedResearchJournalStore,
                &dyn market_squawk_platform::ResearchObjectControl,
                &AnalyticalDataService,
                &CancellationToken,
            ) -> Result<T, ResearchServiceError>
            + Send
            + 'static,
    {
        self.read_provider_capture_generation_with_job_context(
            None,
            manifest,
            deadline,
            cancellation,
            read,
        )
        .await
    }

    /// Reopens the original generation in the same owned lane and joins its worker on job cancel.
    pub(crate) async fn read_provider_capture_generation_with_job_context<T, F>(
        &self,
        job: Option<&market_squawk_jobs::JobRunContext>,
        manifest: market_squawk_data::DatasetManifestRef,
        deadline: Instant,
        cancellation: &CancellationToken,
        read: F,
    ) -> Result<T, ResearchServiceError>
    where
        T: Send + 'static,
        F: FnOnce(
                market_squawk_data::GenerationOwnedProviderCaptureEvidence,
                &SealedResearchJournalStore,
                &dyn market_squawk_platform::ResearchObjectControl,
                &AnalyticalDataService,
                &CancellationToken,
            ) -> Result<T, ResearchServiceError>
            + Send
            + 'static,
    {
        use market_squawk_platform::{ResearchObjectControl as _, ResearchObjectControlPoint};
        let analytical = Arc::clone(&self.analytical);
        let store = Arc::clone(&self.provider_captures);
        self.provider_capture_worker
            .run_with_job_context(
                job.map(|job| job.cancellation()),
                deadline,
                cancellation,
                move |worker_cancellation| {
                    let control = ProviderCaptureReadControl {
                        deadline,
                        cancellation: worker_cancellation,
                    };
                    control
                        .checkpoint(ResearchObjectControlPoint::BeforeVerification)
                        .map_err(SealedResearchJournalStoreError::ObjectControl)?;
                    let evidence = analytical.generation_owned_provider_capture_evidence_bounded(
                        &manifest,
                        store.as_ref(),
                        deadline,
                        &control.cancellation,
                    )?;
                    control
                        .checkpoint(ResearchObjectControlPoint::BeforeVerification)
                        .map_err(SealedResearchJournalStoreError::ObjectControl)?;
                    let result = read(
                        evidence,
                        store.as_ref(),
                        &control,
                        &analytical,
                        &control.cancellation,
                    )?;
                    control
                        .checkpoint(ResearchObjectControlPoint::BeforeCommit)
                        .map_err(SealedResearchJournalStoreError::ObjectControl)?;
                    Ok(result)
                },
            )
            .await?
    }

    /// Executes one rights-reserved ingest through durable revision and publication authority.
    pub async fn ingest(
        &self,
        request: ResearchIngestRequest,
        cancellation: CancellationToken,
    ) -> Result<CommittedDataset, ResearchServiceError> {
        Self::ingest_on(&self.analytical, request, cancellation).await
    }

    /// Publishes an intermediate source generation, never the job's terminal result.
    ///
    /// The existing I/O slot owns the original future through runner cancellation or abort. Its
    /// blocking worker uses the originating executor handle; no runtime, detached task, or service
    /// ownership cycle is created. Only the analytical owner and original request cross the lane.
    pub(crate) async fn ingest_for_job(
        &self,
        job: &market_squawk_jobs::JobRunContext,
        mut request: ResearchIngestRequest,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<CommittedDataset, ResearchServiceError> {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| ResearchServiceError::ProviderCaptureSealWorkerUnavailable)?;
        let analytical = Arc::clone(&self.analytical);
        let job_cancellation = job.cancellation().clone();
        let caller_cancellation = cancellation.clone();
        self.provider_capture_worker.run_with_job_context(
            Some(job.cancellation()), deadline, &cancellation, move |operation_cancellation| {
                let authority = Arc::new(JobInputPrecommit {
                    original: request.precommit_authority.take(),
                    job_cancellation,
                    caller_cancellation,
                    operation_cancellation: operation_cancellation.clone(),
                    deadline,
                });
                if let Err(error) = authority.validate_precommit() { return Err(error.into()); }
                request.precommit_authority = Some(authority.clone());
                runtime.block_on(async move {
                    let operation = Self::ingest_on(&analytical, request, operation_cancellation.clone());
                    tokio::pin!(operation);
                    // The owning lane's original deadline cancels operation_cancellation.
                    // No timer/IO driver is needed by this retained storage continuation.
                    let interrupted = tokio::select! {
                        biased;
                        () = authority.job_cancellation.cancelled() => IngestError::Cancelled,
                        () = authority.caller_cancellation.cancelled() => IngestError::Cancelled,
                        () = operation_cancellation.cancelled() => IngestError::Cancelled,
                        result = &mut operation => {
                            authority.validate_precommit()?;
                            return result;
                        }
                    };
                    operation_cancellation.cancel();
                    // Keep polling the exact original ingest. Its existing native publication
                    // path observes cancellation and joins its own Parquet worker before return.
                    let _drained = operation.await;
                    Err(interrupted.into())
                })
            },
        ).await?
    }

    async fn ingest_on(
        analytical: &AnalyticalDataService,
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
        let reservation = analytical
            .reserve_source_ingest(&source, registered_at, rights.clone(), &identity, &cancellation)
            .await?;
        match payload {
            ResearchIngestPayload::Provider {
                sealed_capture,
                revisions,
            } => {
                let implementation = sealed_capture.native_lineage().schema().implementation();
                let mut publication = ProviderPublicationInput::try_new(sealed_capture, revisions)?
                    .with_reobservation_rights(rights);
                if implementation == ProviderNativeLineageImplementation::CensusTabularV1 {
                    publication = publication
                        .with_native_reobservation_validator(validate_census_reobservation);
                }
                if let Some(company_identity) = company_identity {
                    publication = publication.with_company_identity(company_identity);
                }
                if let Some(precommit_authority) = precommit_authority {
                    publication = publication.with_precommit_authority(precommit_authority);
                }
                analytical
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
                (None, Some(precommit_authority)) => analytical
                    .ingest_with_precommit_authority(
                        reservation,
                        analytical_dataset,
                        batch,
                        cancellation,
                        precommit_authority,
                    )
                    .await
                    .map_err(Into::into),
                (None, None) => analytical
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
    pub(crate) fn analytical_service(&self) -> Arc<AnalyticalDataService> {
        Arc::clone(&self.analytical)
    }

    pub fn analytical(&self) -> &AnalyticalDataService {
        &self.analytical
    }

    /// Shares the sole application-owned sealed store with typed restart-verification closures.
    ///
    /// This does not expose physical sealing to adapters; callers can only verify claims already
    /// retained by an immutable provider generation.
    pub(crate) fn provider_capture_store(&self) -> Arc<SealedResearchJournalStore> {
        Arc::clone(&self.provider_captures)
    }

    /// Returns immutable bounded analytical metadata and fixed-template observation reads.
    pub fn analytical_reader(&self) -> AnalyticalReadCapability {
        self.analytical.analytical_reader()
    }

    /// Publishes an authenticated source reference through the existing analytical owner.
    pub(crate) async fn publish_market_data_source_reference(
        &self,
        input: market_squawk_data::MarketDataInstrumentSourceReferenceInput,
        precommit: Arc<dyn market_squawk_data::IngestPrecommitAuthority>,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<
        market_squawk_data::MarketDataInstrumentRecord,
        market_squawk_data::MarketDataInstrumentCatalogError,
    > {
        self.analytical
            .publish_market_data_source_reference(input, precommit, deadline, cancellation)
            .await
    }

    /// Admits complete source-owned option references through the sole canonical identity writer.
    pub(crate) async fn publish_alpaca_option_references(
        &self,
        input: market_squawk_data::AlpacaOptionReferenceAdmission,
        precommit: Arc<dyn market_squawk_data::IngestPrecommitAuthority>,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<
        market_squawk_data::MarketDataInstrumentSynchronizationReceipt,
        market_squawk_data::MarketDataInstrumentCatalogError,
    > {
        self.analytical
            .publish_alpaca_option_references(input, precommit, deadline, cancellation)
            .await
    }

    /// Publishes one authenticated Alpaca asset reference through the existing catalog owner.
    pub(crate) async fn publish_alpaca_asset_reference(
        &self,
        input: market_squawk_data::AlpacaAssetReferenceAdmission,
        precommit: Arc<dyn market_squawk_data::IngestPrecommitAuthority>,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<
        market_squawk_data::MarketDataInstrumentRecord,
        market_squawk_data::MarketDataInstrumentCatalogError,
    > {
        self.analytical
            .publish_alpaca_asset_reference(input, precommit, deadline, cancellation)
            .await
    }

    /// Admits original issuer evidence through the analytical service's supervised catalog owner.
    pub(crate) async fn publish_market_data_issuer_reference(
        &self,
        issuer: market_squawk_data::OfficialIssuerInstrumentReference,
        listing: market_squawk_data::ListingReferenceRecord,
        expected_current: Option<market_squawk_data::MarketDataInstrumentRecord>,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<market_squawk_data::MarketDataInstrumentRecord, market_squawk_data::MarketDataInstrumentCatalogError> {
        self.analytical.publish_market_data_issuer_reference(
            issuer, listing, expected_current, deadline, cancellation,
        ).await
    }

    /// Returns fair-value persistence authority over this service's sole catalog writer.
    pub fn fair_value_catalog(&self) -> FairValueCatalogCapability {
        self.analytical.fair_value_catalog()
    }

    /// Returns bounded point-in-time definition reads over this service's sole catalog session.
    pub fn instrument_definitions(&self) -> InstrumentDefinitionReadCapability {
        self.analytical.instrument_definitions()
    }

    /// Returns bounded reads over repository-owned, explicitly non-executable market identities.
    pub fn market_data_instruments(&self) -> MarketDataInstrumentReadCapability {
        self.analytical.market_data_instruments()
    }

    /// Returns the sole atomic publisher for repository-owned market-data identities.
    pub fn market_data_instrument_synchronization(
        &self,
    ) -> MarketDataInstrumentSynchronizationCapability {
        self.analytical.market_data_instrument_synchronization()
    }

    /// Returns bounded company-identity reads over the canonical research catalog.
    pub fn company_identities(&self) -> CompanyIdentityReadCapability {
        self.analytical.company_identities()
    }

    /// Returns bounded authoritative company/security relationship reads.
    ///
    /// This capability owns no publication, identity inference, review, or execution authority.
    pub fn company_security_identities(&self) -> CompanySecurityIdentityReadCapability {
        self.analytical
            .company_identities()
            .security_relationships()
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

/// Adds job input controls without replacing the exact account/source authority or claiming a
/// terminal JobRunContext permit. Catalog validation delegates using the already-held writer.
#[derive(Debug)]
struct JobInputPrecommit {
    original: Option<Arc<dyn IngestPrecommitAuthority>>,
    job_cancellation: CancellationToken,
    caller_cancellation: CancellationToken,
    operation_cancellation: CancellationToken,
    deadline: Instant,
}

impl JobInputPrecommit {
    fn check(&self) -> Result<(), IngestError> {
        if self.job_cancellation.is_cancelled()
            || self.caller_cancellation.is_cancelled()
            || self.operation_cancellation.is_cancelled()
        {
            Err(IngestError::Cancelled)
        } else if Instant::now() >= self.deadline {
            Err(IngestError::DeadlineExceeded)
        } else {
            Ok(())
        }
    }
}

impl IngestPrecommitAuthority for JobInputPrecommit {
    fn validate_precommit(&self) -> Result<(), IngestError> {
        self.check()?;
        if let Some(original) = &self.original {
            original.validate_precommit()?;
        }
        self.check()
    }

    fn validate_catalog_precommit(&self, catalog: &CatalogAuthority) -> Result<(), IngestError> {
        self.check()?;
        if let Some(original) = &self.original {
            original.validate_catalog_precommit(catalog)?;
        }
        self.check()
    }

    fn claim_sec_fund_job_commit(
        &self,
        binding_digest: market_squawk_domain::EvidenceDigest,
    ) -> Result<Option<market_squawk_data::SecFundJobCommit>, IngestError> {
        self.check()?;
        match &self.original {
            Some(original) => original.claim_sec_fund_job_commit(binding_digest),
            None => Ok(None),
        }
    }
}

struct ProviderCaptureReadControl {
    deadline: Instant,
    cancellation: CancellationToken,
}

impl market_squawk_platform::ResearchObjectControl for ProviderCaptureReadControl {
    fn checkpoint(
        &self,
        _point: market_squawk_platform::ResearchObjectControlPoint,
    ) -> Result<(), market_squawk_platform::ResearchObjectControlError> {
        if self.cancellation.is_cancelled() {
            Err(market_squawk_platform::ResearchObjectControlError::Cancelled)
        } else if Instant::now() >= self.deadline {
            Err(market_squawk_platform::ResearchObjectControlError::DeadlineExceeded)
        } else {
            Ok(())
        }
    }
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
