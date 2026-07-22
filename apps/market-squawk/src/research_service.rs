//! Application-owned composition for local research ingestion and point-in-time datasets.

use market_squawk_data::{
    AnalyticalDataService, AnalyticalManifestCatalog, CatalogAuthority, CatalogConfig,
    CommittedDataset, DatasetBuildError, DatasetBuildRequest, DatasetBuilder, FeatureLabelDataset,
    IngestError, IngestIdentity, ManifestCatalogError, ObjectStoreConfig, ResearchIngestService,
    RightsDecisionInput, RightsError, SourceOperation, extraction_batch_digest,
};
use market_squawk_platform::{LocalPaths, PathError};
use market_squawk_sources::{ExtractionBatch, ExtractionRevisionPlan, SourceMetadata};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

/// One rights-reserved normalized extraction, with provider revision evidence when required.
#[derive(Clone, Debug)]
pub struct ResearchIngestRequest {
    source: SourceMetadata,
    registered_at: market_squawk_domain::Timestamp,
    rights: RightsDecisionInput,
    identity: IngestIdentity,
    batch: ExtractionBatch,
    revisions: Option<ExtractionRevisionPlan>,
}

impl ResearchIngestRequest {
    /// Constructs a local-file or portfolio ingest whose revisions are locally observed.
    pub fn locally_observed(
        source: SourceMetadata,
        rights: RightsDecisionInput,
        idempotency_key: impl Into<String>,
        batch: ExtractionBatch,
    ) -> Result<Self, ResearchServiceError> {
        Self::try_new(source, rights, idempotency_key, batch, None)
    }

    /// Constructs an ingest with one explicit provider revision decision per normalized record.
    pub fn with_provider_revisions(
        source: SourceMetadata,
        rights: RightsDecisionInput,
        idempotency_key: impl Into<String>,
        batch: ExtractionBatch,
        revisions: ExtractionRevisionPlan,
    ) -> Result<Self, ResearchServiceError> {
        Self::try_new(source, rights, idempotency_key, batch, Some(revisions))
    }

    fn try_new(
        source: SourceMetadata,
        rights: RightsDecisionInput,
        idempotency_key: impl Into<String>,
        batch: ExtractionBatch,
        revisions: Option<ExtractionRevisionPlan>,
    ) -> Result<Self, ResearchServiceError> {
        let payload_digest = extraction_batch_digest(&batch)?;
        let source_id = batch.request().object().source_id();
        if source.source_id() != source_id
            || &rights.source_id != source_id
            || rights.payload_digest != payload_digest
        {
            return Err(ResearchServiceError::IngestAuthorityMismatch);
        }
        let registered_at = rights.retrieved_at;
        let identity = IngestIdentity::try_new(
            source_id.clone(),
            payload_digest,
            SourceOperation::Persist,
            idempotency_key,
        )?;
        Ok(Self {
            source,
            registered_at,
            rights,
            identity,
            batch,
            revisions,
        })
    }
}

/// Single application authority for local analytical storage and dataset construction.
#[derive(Debug)]
pub struct ResearchService {
    analytical: AnalyticalDataService,
}

impl ResearchService {
    /// Creates and durably binds a fresh catalog and controlled artifact root.
    pub fn initialize(
        paths: &LocalPaths,
        catalog: CatalogConfig,
        max_generations: usize,
        objects: ObjectStoreConfig,
    ) -> Result<Self, ResearchServiceError> {
        let authority = CatalogAuthority::open(catalog)?;
        let manifests = AnalyticalManifestCatalog::open(paths.catalog()?, max_generations)?;
        let analytical = AnalyticalDataService::initialize(
            authority,
            manifests,
            paths.artifacts()?.clone(),
            objects,
        )?;
        Ok(Self { analytical })
    }

    /// Reopens an already bound catalog and artifact root without implicit migration.
    pub fn open(
        paths: &LocalPaths,
        catalog: CatalogConfig,
        max_generations: usize,
        objects: ObjectStoreConfig,
    ) -> Result<Self, ResearchServiceError> {
        let authority = CatalogAuthority::open(catalog)?;
        let manifests = AnalyticalManifestCatalog::open(paths.catalog()?, max_generations)?;
        let analytical =
            AnalyticalDataService::open(authority, manifests, paths.artifacts()?.clone(), objects)?;
        Ok(Self { analytical })
    }

    /// Executes one rights-reserved ingest through durable revision and publication authority.
    pub async fn ingest(
        &self,
        request: ResearchIngestRequest,
        cancellation: CancellationToken,
    ) -> Result<CommittedDataset, ResearchServiceError> {
        let reservation = self
            .analytical
            .reserve_source_ingest(
                &request.source,
                request.registered_at,
                request.rights,
                &request.identity,
                &cancellation,
            )
            .await?;
        match request.revisions {
            Some(revisions) => self
                .analytical
                .ingest_with_revision_plan(reservation, request.batch, revisions, cancellation)
                .await
                .map_err(Into::into),
            None => self
                .analytical
                .ingest(reservation, request.batch, cancellation)
                .await
                .map_err(Into::into),
        }
    }

    /// Builds one authorized, point-in-time feature/label generation.
    pub async fn build_dataset(
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

    /// Returns the manifest-pinned analytical service for bounded query composition.
    pub const fn analytical(&self) -> &AnalyticalDataService {
        &self.analytical
    }
}

/// Research composition, storage, ingestion, or dataset-construction failure.
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
    /// Analytical authority composition or ingestion failed.
    #[error("research service ingestion failed: {0}")]
    Ingest(#[from] IngestError),
    /// Point-in-time dataset construction failed.
    #[error("research service dataset build failed: {0}")]
    Dataset(#[from] DatasetBuildError),
    /// The composed source, rights, and exact extraction payload do not agree.
    #[error("research ingest source, rights, and batch evidence do not agree")]
    IngestAuthorityMismatch,
    /// The idempotency identity is invalid.
    #[error("research ingest identity failed: {0}")]
    Rights(#[from] RightsError),
}
