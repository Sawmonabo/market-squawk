//! Rights-bound analytical ingestion, immutable generation commit, and compaction.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::record_batch::RecordBatch;
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, SchemaVersion, SourceIdentifier, Timestamp,
};
use market_squawk_sources::ExtractionBatch;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::catalog::QueryArtifactPublisher;
use crate::parquet_store::ArtifactRootIdentity;
use crate::{
    AnalyticalManifestCatalog, ArrowConversionError, ArtifactRecord, CatalogAuthority,
    CatalogError, ContractCompletion, DatasetId, DatasetManifestRecord, DatasetManifestRef,
    GenerationKind, IngestReservation, IngestRunState, ManifestCatalogError, ManifestObject,
    ManifestPlan, ManifestPlanError, ObjectStoreConfig, OrphanRecoveryReport, ParquetObjectStore,
    ParquetStoreError, PinnedDataset, PublishedObject, QueryArtifactReservation,
    QueryArtifactReservationInput, ResearchArrowBatch, Sha256Digest, SourceOperation,
};

/// Exact immutable generation returned after successful reconciliation or commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommittedDataset {
    pinned: PinnedDataset,
}

impl CommittedDataset {
    /// Returns the exact immutable generation pin.
    pub const fn manifest(&self) -> &DatasetManifestRef {
        self.pinned.manifest()
    }

    /// Returns the complete pinned object set.
    pub const fn pinned(&self) -> &PinnedDataset {
        &self.pinned
    }

    fn new(pinned: PinnedDataset) -> Self {
        Self { pinned }
    }
}

/// Exact compaction request identity that callers must bind into the Task 3 reservation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionRequest {
    source: DatasetManifestRef,
    payload_digest: EvidenceDigest,
}

impl CompactionRequest {
    /// Constructs a request bound to one exact immutable source generation.
    pub fn new(source: DatasetManifestRef) -> Self {
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/analytical-compaction/v1");
        digest.update((source.dataset_id().as_str().len() as u64).to_be_bytes());
        digest.update(source.dataset_id().as_str().as_bytes());
        digest.update(source.manifest_version().to_be_bytes());
        digest.update(source.content_hash().bytes());
        Self {
            source,
            payload_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into()),
        }
    }

    /// Returns the immutable source generation.
    pub const fn source(&self) -> &DatasetManifestRef {
        &self.source
    }

    /// Returns the digest required by [`crate::IngestIdentity`].
    pub const fn payload_digest(&self) -> EvidenceDigest {
        self.payload_digest
    }
}

/// Returns the exact canonical digest a Task 3 ingest reservation must carry for this batch.
pub fn extraction_batch_digest(batch: &ExtractionBatch) -> Result<EvidenceDigest, IngestError> {
    let encoded = serde_json::to_vec(batch)?;
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(encoded).into(),
    ))
}

/// Rights-bound research ingestion service.
#[allow(
    async_fn_in_trait,
    reason = "the canonical local service contract intentionally retains native async cancellation"
)]
pub trait ResearchIngestService {
    /// Converts, publishes, and commits one request-bound extraction batch.
    async fn ingest(
        &self,
        reservation: IngestReservation,
        batch: ExtractionBatch,
        cancellation: CancellationToken,
    ) -> Result<CommittedDataset, IngestError>;
}

/// Local composition of Task 3 authority, immutable generations, and controlled Parquet objects.
#[derive(Debug)]
pub struct AnalyticalDataService {
    authority: Arc<Mutex<CatalogAuthority>>,
    catalog_id: uuid::Uuid,
    manifests: AnalyticalManifestCatalog,
    objects: Arc<ParquetObjectStore>,
}

/// Non-separable query-result publication authority for one catalog and artifact root.
pub struct QueryArtifactPublication {
    objects: Arc<ParquetObjectStore>,
    publisher: QueryArtifactPublisher,
    catalog_id: uuid::Uuid,
    root_identity: ArtifactRootIdentity,
}

impl fmt::Debug for QueryArtifactPublication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueryArtifactPublication")
            .field("objects", &"[SEALED ROOT CAPABILITY]")
            .field("publisher", &self.publisher)
            .field("catalog", &"[SEALED CATALOG SESSION]")
            .finish()
    }
}

impl QueryArtifactPublication {
    pub(crate) fn root_identity(&self) -> &ArtifactRootIdentity {
        &self.root_identity
    }

    pub(crate) async fn publish_and_bind(
        &self,
        batch: &RecordBatch,
        cancellation: &CancellationToken,
        reservation: &QueryArtifactReservation,
    ) -> Result<(PublishedObject, ArtifactRecord, crate::QueryArtifactResult), crate::QueryError>
    {
        if reservation.catalog_id() != self.catalog_id {
            return Err(crate::QueryError::Catalog(
                CatalogError::InvalidReservationCapability,
            ));
        }
        let lease = self
            .objects
            .begin_publication(cancellation)
            .await
            .map_err(map_query_store_error)?;
        let object = self
            .objects
            .publish_under_lease(batch, cancellation, &lease)
            .await
            .map_err(map_query_store_error)?;
        if !self
            .objects
            .verify(&object)
            .map_err(map_query_store_error)?
        {
            return Err(crate::QueryError::Artifact(
                ParquetStoreError::ObjectMetadataMismatch,
            ));
        }
        let artifact = ArtifactRecord::try_new(
            object.relative_reference(),
            object.content_hash().evidence(),
            object.size_bytes(),
            object.created_at(),
        )
        .map_err(crate::QueryError::Catalog)?;
        let ownership = self
            .publisher
            .bind(reservation, &artifact)
            .map_err(crate::QueryError::Catalog)?;
        Ok((object, artifact, ownership))
    }
}

fn map_query_store_error(error: ParquetStoreError) -> crate::QueryError {
    if matches!(error, ParquetStoreError::Cancelled) {
        crate::QueryError::Cancelled
    } else {
        crate::QueryError::Artifact(error)
    }
}

impl AnalyticalDataService {
    /// Opens immutable storage from controlled capabilities and a validated generation catalog.
    pub fn open(
        authority: CatalogAuthority,
        manifests: AnalyticalManifestCatalog,
        artifact_root: market_squawk_platform::ArtifactRoot,
        object_config: ObjectStoreConfig,
    ) -> Result<Self, IngestError> {
        authority.integrity_check()?;
        if authority.catalog_path() != manifests.catalog_path() {
            return Err(IngestError::CatalogCompositionMismatch);
        }
        let objects = ParquetObjectStore::open(
            artifact_root,
            object_config,
            authority.catalog_path(),
            authority.artifact_root_binding(),
        )?;
        let catalog_id = authority.session_id();
        Ok(Self {
            authority: Arc::new(Mutex::new(authority)),
            catalog_id,
            manifests,
            objects: Arc::new(objects),
        })
    }

    /// Returns the controlled object capability for manifest-pinned query construction.
    pub fn object_store(&self) -> Arc<ParquetObjectStore> {
        Arc::clone(&self.objects)
    }

    /// Persists query-result ownership and expiry before artifact publication begins.
    pub fn reserve_query_artifact(
        &self,
        input: QueryArtifactReservationInput,
    ) -> Result<QueryArtifactReservation, IngestError> {
        Ok(self.lock_authority()?.reserve_query_artifact(input)?)
    }

    /// Issues one sealed publisher that cannot be paired with another root or catalog writer.
    pub fn query_artifact_publication(&self) -> Arc<QueryArtifactPublication> {
        Arc::new(QueryArtifactPublication {
            objects: Arc::clone(&self.objects),
            publisher: QueryArtifactPublisher::new(Arc::clone(&self.authority)),
            catalog_id: self.catalog_id,
            root_identity: self.objects.authority_identity().clone(),
        })
    }

    /// Resolves one exact immutable generation.
    pub fn pinned(&self, manifest: &DatasetManifestRef) -> Result<PinnedDataset, IngestError> {
        Ok(self.manifests.pinned(manifest)?)
    }

    /// Rewrites the current pinned generation into one immutable object without changing rows.
    pub async fn compact(
        &self,
        reservation: IngestReservation,
        request: CompactionRequest,
        cancellation: CancellationToken,
    ) -> Result<CommittedDataset, IngestError> {
        let pinned = self.manifests.pinned(request.source())?;
        let source_id = self.manifests.source_id(request.source())?;
        {
            let authority = self.lock_authority()?;
            let run = self.validate_run(
                &authority,
                &reservation,
                request.payload_digest(),
                Some(&source_id),
            )?;
            if run.state() == IngestRunState::Failed {
                return Err(IngestError::TerminalRun);
            }
        }
        let batches = self
            .objects
            .read_pinned_async(&pinned, &cancellation)
            .await?;
        if cancellation.is_cancelled() {
            return Err(IngestError::Cancelled);
        }
        let dataset_name = SourceIdentifier::try_from(request.source().dataset_id().as_str())
            .map_err(|_| IngestError::InvalidDataset)?;
        let compacted = ResearchArrowBatch::try_from_compaction_batches(
            dataset_name.clone(),
            request.payload_digest(),
            batches,
        )?;
        let publication = self.objects.begin_publication(&cancellation).await?;
        let published = self
            .objects
            .publish_under_lease(compacted.record_batch(), &cancellation, &publication)
            .await?;
        if cancellation.is_cancelled() {
            return Err(IngestError::Cancelled);
        }
        let object = ManifestObject::try_new(
            published.content_hash(),
            published.row_count(),
            published.size_bytes(),
            pinned.plan().lineage_digest(),
        )?;

        let authority = self.lock_authority()?;
        let run = self.validate_run(
            &authority,
            &reservation,
            request.payload_digest(),
            Some(&source_id),
        )?;
        if let Some(committed) = self.reconcile_existing(
            &authority,
            &reservation,
            run.state(),
            request.source().dataset_id(),
            &object,
        )? {
            return Ok(committed);
        }
        let plan = self
            .manifests
            .preview_compaction(request.source(), object)?;
        self.commit_plan(
            &authority,
            &reservation,
            run.state(),
            dataset_name,
            plan,
            published,
            GenerationKind::Compaction,
        )
    }

    /// Quarantines only content-addressed objects absent from every retained generation.
    pub async fn recover_orphans(
        &self,
        now: Timestamp,
    ) -> Result<OrphanRecoveryReport, IngestError> {
        let mut recovery = self.objects.begin_recovery(now).await?;
        let _authority = self.lock_authority()?;
        let referenced: BTreeSet<_> = self.manifests.referenced_hashes(now)?.into_iter().collect();
        for object in recovery.candidates().to_vec() {
            if referenced.contains(&object.content_hash()) {
                continue;
            }
            if self.manifests.is_referenced(object.content_hash(), now)? {
                continue;
            }
            recovery.quarantine(&object)?;
        }
        Ok(recovery.finish()?)
    }

    async fn ingest_batch(
        &self,
        reservation: IngestReservation,
        batch: ExtractionBatch,
        cancellation: CancellationToken,
    ) -> Result<CommittedDataset, IngestError> {
        let payload_digest = extraction_batch_digest(&batch)?;
        let source_id = batch.request().object().source_id().clone();
        let dataset_name = batch.request().object().dataset().clone();
        let dataset_id = DatasetId::try_from(dataset_name.as_str())?;
        {
            let authority = self.lock_authority()?;
            let run =
                self.validate_run(&authority, &reservation, payload_digest, Some(&source_id))?;
            if run.state() == IngestRunState::Failed {
                return Err(IngestError::TerminalRun);
            }
        }
        let converted = ResearchArrowBatch::try_from_extraction_batch(&batch)?;
        let lineage = converted.lineage_digest()?;
        let publication = self.objects.begin_publication(&cancellation).await?;
        let published = self
            .objects
            .publish_under_lease(converted.record_batch(), &cancellation, &publication)
            .await?;
        if cancellation.is_cancelled() {
            return Err(IngestError::Cancelled);
        }
        let object = ManifestObject::try_new(
            published.content_hash(),
            published.row_count(),
            published.size_bytes(),
            Sha256Digest::new(lineage.bytes()),
        )?;

        let authority = self.lock_authority()?;
        let run = self.validate_run(&authority, &reservation, payload_digest, Some(&source_id))?;
        if let Some(committed) =
            self.reconcile_existing(&authority, &reservation, run.state(), &dataset_id, &object)?
        {
            return Ok(committed);
        }
        let plan = self.manifests.preview_append(dataset_id, object)?;
        self.commit_plan(
            &authority,
            &reservation,
            run.state(),
            dataset_name,
            plan,
            published,
            GenerationKind::Ingest,
        )
    }

    fn validate_run(
        &self,
        authority: &CatalogAuthority,
        reservation: &IngestReservation,
        payload_digest: EvidenceDigest,
        source_id: Option<&market_squawk_domain::SourceId>,
    ) -> Result<crate::IngestRunRecord, IngestError> {
        let run = authority
            .ingest_run(reservation.run_id())?
            .ok_or(IngestError::UnknownReservation)?;
        if run.payload_digest() != payload_digest
            || source_id.is_some_and(|source| run.source_id() != source)
        {
            return Err(IngestError::ReservationPayloadMismatch);
        }
        if run.operation() != SourceOperation::Persist {
            return Err(IngestError::PersistRightsRequired);
        }
        Ok(run)
    }

    fn reconcile_existing(
        &self,
        authority: &CatalogAuthority,
        reservation: &IngestReservation,
        state: IngestRunState,
        dataset_id: &DatasetId,
        object: &ManifestObject,
    ) -> Result<Option<CommittedDataset>, IngestError> {
        let Some(existing) = self.manifests.for_run(reservation.run_id())? else {
            return match state {
                IngestRunState::Reserved => Ok(None),
                IngestRunState::Succeeded => Err(IngestError::IncompleteSuccessfulRun),
                IngestRunState::Failed => Err(IngestError::TerminalRun),
            };
        };
        if existing.manifest().dataset_id() != dataset_id
            || existing.plan().objects().last() != Some(object)
        {
            return Err(IngestError::ReplayConflict);
        }
        match state {
            IngestRunState::Reserved => {
                authority.complete_ingest(reservation, ContractCompletion::Succeeded)?;
            }
            IngestRunState::Succeeded => {}
            IngestRunState::Failed => return Err(IngestError::TerminalRun),
        }
        Ok(Some(CommittedDataset::new(existing)))
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "publication keeps each independently verified authority input explicit"
    )]
    fn commit_plan(
        &self,
        authority: &CatalogAuthority,
        reservation: &IngestReservation,
        state: IngestRunState,
        dataset_name: SourceIdentifier,
        plan: ManifestPlan,
        published: PublishedObject,
        kind: GenerationKind,
    ) -> Result<CommittedDataset, IngestError> {
        if state != IngestRunState::Reserved {
            return Err(IngestError::TerminalRun);
        }
        let created_at = published.created_at().max(reservation.requested_at());
        let artifact = ArtifactRecord::try_new(
            published.relative_reference(),
            published.content_hash().evidence(),
            published.size_bytes(),
            created_at,
        )?;
        let anchor = DatasetManifestRecord::try_new(
            dataset_name,
            SchemaVersion::CURRENT,
            artifact.artifact_id(),
            plan.content_hash().evidence(),
            created_at,
        );
        let publication = authority.publish_artifact_manifest(reservation, &artifact, &anchor)?;
        let manifest = self.manifests.commit_generation(
            &plan,
            publication.artifact(),
            publication.manifest(),
            kind,
        )?;
        authority.complete_ingest(reservation, ContractCompletion::Succeeded)?;
        Ok(CommittedDataset::new(self.manifests.pinned(&manifest)?))
    }

    fn lock_authority(&self) -> Result<MutexGuard<'_, CatalogAuthority>, IngestError> {
        self.authority
            .lock()
            .map_err(|_| IngestError::AuthorityLockPoisoned)
    }
}

impl ResearchIngestService for AnalyticalDataService {
    async fn ingest(
        &self,
        reservation: IngestReservation,
        batch: ExtractionBatch,
        cancellation: CancellationToken,
    ) -> Result<CommittedDataset, IngestError> {
        self.ingest_batch(reservation, batch, cancellation).await
    }
}

/// Analytical ingestion, publication, reconciliation, or compaction failure.
#[derive(Debug, Error)]
pub enum IngestError {
    /// Canonical observation conversion failed.
    #[error("research observation conversion failed")]
    Arrow(#[from] ArrowConversionError),
    /// Parquet publication, verification, or recovery failed.
    #[error("analytical Parquet object operation failed")]
    Parquet(#[from] ParquetStoreError),
    /// Immutable manifest planning failed.
    #[error("analytical manifest plan is invalid")]
    Plan(#[from] ManifestPlanError),
    /// Immutable manifest persistence failed.
    #[error("analytical manifest catalog operation failed")]
    Manifest(#[from] ManifestCatalogError),
    /// Task 3 rejected a reservation, publication, or transition.
    #[error("analytical catalog authority rejected the operation")]
    Catalog(#[from] CatalogError),
    /// Catalog and manifest capabilities do not identify the same prepared catalog path.
    #[error("analytical service capabilities name different catalogs")]
    CatalogCompositionMismatch,
    /// Exact extraction-batch serialization failed.
    #[error("extraction batch identity serialization failed")]
    Serialization(#[from] serde_json::Error),
    /// The reservation does not exist in this authority.
    #[error("analytical ingest reservation is unknown")]
    UnknownReservation,
    /// The reservation's source or exact payload does not match the requested operation.
    #[error("analytical ingest reservation payload does not match")]
    ReservationPayloadMismatch,
    /// Persist rights are mandatory for immutable analytical publication.
    #[error("analytical publication requires admitted persist rights")]
    PersistRightsRequired,
    /// The reservation was already completed unsuccessfully or cannot transition again.
    #[error("analytical ingest run is already terminal")]
    TerminalRun,
    /// A successful run has no complete analytical generation.
    #[error("successful analytical ingest has no immutable generation")]
    IncompleteSuccessfulRun,
    /// A replay differs from the generation already anchored to its run.
    #[error("analytical ingest replay conflicts with its immutable generation")]
    ReplayConflict,
    /// Dataset identity is not valid for immutable storage.
    #[error("analytical dataset identity is invalid")]
    InvalidDataset,
    /// Cancellation was observed before commit.
    #[error("analytical operation was cancelled")]
    Cancelled,
    /// The process-owned Task 3 authority lock was poisoned.
    #[error("analytical catalog authority is unavailable")]
    AuthorityLockPoisoned,
}
