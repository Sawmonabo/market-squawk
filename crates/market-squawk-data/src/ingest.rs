//! Rights-bound analytical ingestion, immutable generation commit, and compaction.

use std::fmt;
use std::sync::atomic::AtomicBool;
#[cfg(test)]
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::record_batch::RecordBatch;
use market_squawk_domain::{
    CompanyIdentityObservation, DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    ExtractionBatch, ExtractionContentIdentity, ExtractionError, ExtractionRevisionPlan,
    ObservedRevisionAuthority, ObservedRevisionError, SourceClass, SourceMetadata,
};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::analytical_backup::AnalyticalOperationGate;
use crate::authority_transition::{AuthorityTransitionError, AuthorityTransitionService};
use crate::blocking_supervisor::BlockingIoSupervisor;
use crate::catalog::CatalogObservedRevisionAuthority;
#[cfg(test)]
use crate::catalog::QueryArtifactBindCheckpoint;
use crate::catalog::QueryArtifactPublisher;
use crate::parquet_store::MAX_SCAN_OBJECTS;
use crate::parquet_store::{ArtifactRootIdentity, QueryArtifactWriterAdmission};
use crate::query::QueryArtifactMemoryLease;
use crate::{
    AnalyticalManifestCatalog, ArrowConversionError, ArtifactRecord, CatalogAuthority,
    CatalogError, ContractCompletion, DatasetArrowBatch, DatasetId, DatasetManifestRecord,
    DatasetManifestRef, DatasetSchemaRef, GenerationKind, IngestIdentity, IngestReservation,
    IngestRunState, ManifestCatalogError, ManifestObject, ManifestPlan, ManifestPlanError,
    ObjectStoreConfig, OrphanRecoveryReport, ParquetObjectStore, ParquetStoreError, PinnedDataset,
    PinnedQueryOutput, PublishedObject, QueryArtifactReservation, QueryArtifactReservationInput,
    QueryError, QueryLimits, QueryRequest, ResearchArrowBatch, ResearchQueryEngine,
    RightsDecisionInput, Sha256Digest, SourceOperation,
};

const ORPHAN_RECOVERY_DEADLINE: Duration = Duration::from_secs(30);
const REVISION_ASSIGNMENT_DEADLINE: Duration = Duration::from_secs(30);

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
        digest.update(b"market-squawk/analytical-compaction/v2");
        digest.update((source.dataset_id().as_str().len() as u64).to_be_bytes());
        digest.update(source.dataset_id().as_str().as_bytes());
        digest.update(source.manifest_version().to_be_bytes());
        digest.update((source.schema().name().len() as u64).to_be_bytes());
        digest.update(source.schema().name().as_bytes());
        digest.update(source.schema_version().get().to_be_bytes());
        digest.update(source.schema().fingerprint());
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
    ExtractionContentIdentity::try_from_batch(batch)
        .map(ExtractionContentIdentity::digest)
        .map_err(IngestError::ContentIdentity)
}

/// Returns the exact provider payload digest that owns one normalized extraction.
///
/// This identity deliberately comes from the discovered source object rather than normalized
/// record bytes. Normalized rows retain receive, ingestion, and locally observed availability
/// times, which are truthful attempt provenance but must not turn a retry of the same immutable
/// provider object into a second ingest. The source-object contract already binds the exact
/// provider bytes and extraction rejects a refetch whose bytes do not match that evidence.
pub fn extraction_provider_payload_digest(batch: &ExtractionBatch) -> EvidenceDigest {
    batch.request().object().evidence().content_digest()
}

/// Process-local authority that must remain live through the durable ingest commit boundary.
pub trait IngestPrecommitAuthority: fmt::Debug + Send + Sync {
    /// Revalidates the exact caller authority immediately before catalog and manifest commit.
    fn validate_precommit(&self) -> Result<(), IngestError>;
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
        analytical_dataset: DatasetId,
        batch: ExtractionBatch,
        cancellation: CancellationToken,
    ) -> Result<CommittedDataset, IngestError>;

    /// Assigns durable revisions from explicit source-specific evidence before publication.
    async fn ingest_with_revision_plan(
        &self,
        reservation: IngestReservation,
        analytical_dataset: DatasetId,
        batch: ExtractionBatch,
        revisions: ExtractionRevisionPlan,
        cancellation: CancellationToken,
    ) -> Result<CommittedDataset, IngestError>;
}

/// Local composition of Task 3 authority, immutable generations, and controlled Parquet objects.
#[derive(Debug)]
pub struct AnalyticalDataService {
    authority: Arc<Mutex<CatalogAuthority>>,
    catalog_id: uuid::Uuid,
    manifests: Arc<AnalyticalManifestCatalog>,
    objects: Arc<ParquetObjectStore>,
    operation_gate: AnalyticalOperationGate,
}

/// Complete immutable operation input for one bounded pinned query with durable overflow.
///
/// Fields stay private so callers cannot detach the query, owner, expiry, cancellation, or
/// absolute deadline from the immutable generation they authorize.
pub struct PinnedArtifactQueryRequest {
    pinned: PinnedDataset,
    table_name: String,
    query: QueryRequest,
    limits: QueryLimits,
    owner: SourceIdentifier,
    artifact_ttl: Duration,
    cancellation: CancellationToken,
    operation_deadline: tokio::time::Instant,
}

impl PinnedArtifactQueryRequest {
    /// Binds every execution and publication input under one absolute operation deadline.
    pub fn try_new(
        pinned: PinnedDataset,
        table_name: impl Into<String>,
        query: QueryRequest,
        limits: QueryLimits,
        owner: SourceIdentifier,
        artifact_ttl: Duration,
        cancellation: CancellationToken,
    ) -> Result<Self, QueryError> {
        let operation_deadline = tokio::time::Instant::now()
            .checked_add(limits.deadline())
            .ok_or(QueryError::InvalidLimits)?;
        Ok(Self {
            pinned,
            table_name: table_name.into(),
            query,
            limits,
            owner,
            artifact_ttl,
            cancellation,
            operation_deadline,
        })
    }
}

impl fmt::Debug for PinnedArtifactQueryRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PinnedArtifactQueryRequest")
            .field("pinned", &"[IMMUTABLE PIN]")
            .field("table_name", &self.table_name)
            .field("query", &"[VALIDATED QUERY]")
            .field("limits", &self.limits)
            .field("owner", &"[PUBLICATION OWNER]")
            .field("artifact_ttl", &self.artifact_ttl)
            .field("cancellation", &"[CANCELLATION CAPABILITY]")
            .field("operation_deadline", &self.operation_deadline)
            .finish()
    }
}

/// Non-separable query-result publication authority for one catalog and artifact root.
pub struct QueryArtifactPublication {
    objects: Arc<ParquetObjectStore>,
    publisher: QueryArtifactPublisher,
    catalog_id: uuid::Uuid,
    root_identity: ArtifactRootIdentity,
    operation_gate: AnalyticalOperationGate,
    #[cfg(test)]
    bind_barrier: Mutex<Option<QueryArtifactBindWorkerBarrier>>,
    #[cfg(test)]
    writer_barrier: Mutex<Option<QueryArtifactWriterWorkerBarrier>>,
}

#[cfg(test)]
#[derive(Debug)]
struct QueryArtifactBindWorkerBarrier {
    checkpoint: QueryArtifactBindCheckpoint,
    entered_sender: std::sync::mpsc::SyncSender<()>,
    release_receiver: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct QueryArtifactBindTestBarrier {
    entered_receiver: Option<std::sync::mpsc::Receiver<()>>,
    release_sender: std::sync::mpsc::SyncSender<()>,
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct QueryArtifactWriterWorkerBarrier {
    entered_sender: std::sync::mpsc::SyncSender<()>,
    release_receiver: std::sync::mpsc::Receiver<()>,
    memory_retained: Arc<AtomicBool>,
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct QueryArtifactWriterTestBarrier {
    entered_receiver: Option<std::sync::mpsc::Receiver<()>>,
    release_sender: std::sync::mpsc::SyncSender<()>,
    memory_retained: Arc<AtomicBool>,
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

    pub(crate) fn writer_admission(
        &self,
        batch: &RecordBatch,
    ) -> Result<QueryArtifactWriterAdmission, ParquetStoreError> {
        self.objects.query_artifact_writer_admission(batch)
    }

    /// Reads one exact query object only while its durable ownership receipt remains live.
    ///
    /// All four independently retained identities—object, catalog artifact, durable ownership,
    /// and this sealed root/catalog publication capability—must agree before bytes are returned.
    pub async fn read_verified_bytes(
        &self,
        object: &PublishedObject,
        artifact: &ArtifactRecord,
        ownership: &crate::QueryArtifactResult,
        maximum_bytes: usize,
        deadline: tokio::time::Instant,
        cancellation: &CancellationToken,
    ) -> Result<Vec<u8>, QueryError> {
        let now = system_timestamp().map_err(|_| QueryError::ArtifactAuthorityRequired)?;
        if ownership.artifact_id() != artifact.artifact_id()
            || ownership.expires_at() <= now
            || artifact.relative_reference() != object.relative_reference()
            || artifact.content_digest().algorithm() != DigestAlgorithm::Sha256
            || artifact.content_digest().bytes() != object.content_hash().bytes()
            || artifact.size_bytes() != object.size_bytes()
            || artifact.created_at() < object.created_at()
            || usize::try_from(object.size_bytes())
                .ok()
                .is_none_or(|size| size > maximum_bytes)
        {
            return Err(QueryError::Artifact(
                ParquetStoreError::ObjectMetadataMismatch,
            ));
        }
        self.objects
            .read_published_bytes_async(object, maximum_bytes, deadline, cancellation)
            .await
            .map_err(map_query_store_error)
    }

    #[cfg(test)]
    pub(crate) fn install_test_bind_barrier(
        &self,
        checkpoint: QueryArtifactBindCheckpoint,
    ) -> Result<QueryArtifactBindTestBarrier, &'static str> {
        let (entered_sender, entered_receiver) = std::sync::mpsc::sync_channel(1);
        let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
        *self
            .bind_barrier
            .lock()
            .map_err(|_| "test query-bind barrier mutex was poisoned")? =
            Some(QueryArtifactBindWorkerBarrier {
                checkpoint,
                entered_sender,
                release_receiver,
            });
        Ok(QueryArtifactBindTestBarrier {
            entered_receiver: Some(entered_receiver),
            release_sender,
        })
    }

    #[cfg(test)]
    pub(crate) fn install_test_writer_barrier(
        &self,
    ) -> Result<QueryArtifactWriterTestBarrier, &'static str> {
        let (entered_sender, entered_receiver) = std::sync::mpsc::sync_channel(1);
        let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
        let memory_retained = Arc::new(AtomicBool::new(false));
        *self
            .writer_barrier
            .lock()
            .map_err(|_| "test query-writer barrier mutex was poisoned")? =
            Some(QueryArtifactWriterWorkerBarrier {
                entered_sender,
                release_receiver,
                memory_retained: Arc::clone(&memory_retained),
            });
        Ok(QueryArtifactWriterTestBarrier {
            entered_receiver: Some(entered_receiver),
            release_sender,
            memory_retained,
        })
    }

    #[cfg(test)]
    pub(crate) fn test_writer_memory_witness(&self) -> Option<Arc<AtomicBool>> {
        self.writer_barrier.lock().ok().and_then(|barrier| {
            barrier
                .as_ref()
                .map(|barrier| Arc::clone(&barrier.memory_retained))
        })
    }

    #[cfg(test)]
    fn take_test_writer_barrier(&self) -> Option<QueryArtifactWriterWorkerBarrier> {
        self.writer_barrier
            .lock()
            .ok()
            .and_then(|mut barrier| barrier.take())
    }

    #[cfg(test)]
    fn wait_at_test_bind_barrier(&self, checkpoint: QueryArtifactBindCheckpoint) {
        let barrier = self.bind_barrier.lock().ok().and_then(|mut barrier| {
            if barrier
                .as_ref()
                .is_some_and(|barrier| barrier.checkpoint == checkpoint)
            {
                barrier.take()
            } else {
                None
            }
        });
        if let Some(barrier) = barrier {
            let _ignored = barrier.entered_sender.send(());
            let _ignored = barrier.release_receiver.recv();
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "publication ownership, admission, deadline, and durability capabilities stay explicit"
    )]
    pub(crate) async fn publish_and_bind(
        &self,
        batch: RecordBatch,
        cancellation: &CancellationToken,
        reservation: &QueryArtifactReservation,
        writer_admission: QueryArtifactWriterAdmission,
        memory_lease: QueryArtifactMemoryLease,
        supervisor: &BlockingIoSupervisor,
        deadline: tokio::time::Instant,
        #[cfg(test)] bind_precommit_deadline: Option<tokio::time::Instant>,
        durable_bound: &AtomicBool,
    ) -> Result<(PublishedObject, ArtifactRecord, crate::QueryArtifactResult), crate::QueryError>
    {
        let _operation = self
            .operation_gate
            .acquire(cancellation)
            .await
            .ok_or(crate::QueryError::Cancelled)?;
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
            .publish_query_artifact_under_lease(
                batch,
                cancellation,
                &lease,
                writer_admission,
                memory_lease,
                supervisor,
                #[cfg(test)]
                self.take_test_writer_barrier(),
            )
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
        // Content-addressed publication may return an older, already verified immutable object.
        // The catalog timestamp records this reservation's publication, while the object retains
        // its original filesystem creation time.
        let created_at = object.created_at().max(reservation.requested_at());
        let artifact = ArtifactRecord::try_new(
            object.relative_reference(),
            object.content_hash().evidence(),
            object.size_bytes(),
            created_at,
        )
        .map_err(crate::QueryError::Catalog)?;
        let mut checkpoint = |checkpoint| {
            #[cfg(test)]
            self.wait_at_test_bind_barrier(checkpoint);
            #[cfg(not(test))]
            let _ = checkpoint;
        };
        let ownership = self
            .publisher
            .bind(
                reservation,
                &artifact,
                cancellation,
                deadline,
                #[cfg(test)]
                bind_precommit_deadline,
                durable_bound,
                &mut checkpoint,
            )
            .map_err(map_query_catalog_error)?;
        Ok((object, artifact, ownership))
    }
}

fn map_query_store_error(error: ParquetStoreError) -> crate::QueryError {
    match error {
        ParquetStoreError::Cancelled => crate::QueryError::Cancelled,
        ParquetStoreError::ReadDeadlineExceeded => crate::QueryError::DeadlineExceeded,
        ParquetStoreError::BlockingTaskLimitExceeded => {
            crate::QueryError::BlockingTaskLimitExceeded
        }
        error => crate::QueryError::Artifact(error),
    }
}

fn map_query_catalog_error(error: CatalogError) -> crate::QueryError {
    match error {
        CatalogError::QueryArtifactCancelled => crate::QueryError::Cancelled,
        CatalogError::QueryArtifactDeadlineExceeded => crate::QueryError::DeadlineExceeded,
        error => crate::QueryError::Catalog(error),
    }
}

fn map_query_reservation_error(error: IngestError) -> QueryError {
    match error {
        IngestError::Cancelled => QueryError::Cancelled,
        IngestError::DeadlineExceeded => QueryError::DeadlineExceeded,
        IngestError::Catalog(error) => QueryError::Catalog(error),
        IngestError::Parquet(error) => map_query_store_error(error),
        _ => QueryError::ArtifactAuthorityRequired,
    }
}

fn system_timestamp() -> Result<Timestamp, ()> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ())?;
    let nanos = i64::try_from(elapsed.as_nanos()).map_err(|_| ())?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn map_recovery_store_error(error: ParquetStoreError) -> IngestError {
    match error {
        ParquetStoreError::Cancelled => IngestError::Cancelled,
        ParquetStoreError::RecoveryDeadlineExceeded => IngestError::DeadlineExceeded,
        error => IngestError::Parquet(error),
    }
}

fn map_recovery_manifest_error(error: ManifestCatalogError) -> IngestError {
    match error {
        ManifestCatalogError::Cancelled => IngestError::Cancelled,
        ManifestCatalogError::DeadlineExceeded => IngestError::DeadlineExceeded,
        error => IngestError::Manifest(error),
    }
}

#[cfg(test)]
impl QueryArtifactBindTestBarrier {
    pub(crate) async fn wait_until_entered(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let receiver = self
            .entered_receiver
            .take()
            .ok_or("test query-bind barrier was already entered")?;
        tokio::task::spawn_blocking(move || receiver.recv()).await??;
        Ok(())
    }

    pub(crate) fn release(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.release_sender.send(())?;
        Ok(())
    }
}

#[cfg(test)]
impl QueryArtifactWriterWorkerBarrier {
    pub(crate) fn wait(self) {
        let _ignored = self.entered_sender.send(());
        let _ignored = self.release_receiver.recv();
    }
}

#[cfg(test)]
impl QueryArtifactWriterTestBarrier {
    pub(crate) async fn wait_until_entered(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let receiver = self
            .entered_receiver
            .take()
            .ok_or("test query-writer barrier was already entered")?;
        tokio::task::spawn_blocking(move || receiver.recv()).await??;
        Ok(())
    }

    pub(crate) fn memory_retained(&self) -> bool {
        self.memory_retained.load(Ordering::Acquire)
    }

    pub(crate) fn release(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.release_sender.send(())?;
        Ok(())
    }
}

impl AnalyticalDataService {
    /// Explicitly prepares, durably binds, and activates a fresh analytical catalog/root pair.
    pub fn initialize(
        authority: CatalogAuthority,
        manifests: AnalyticalManifestCatalog,
        artifact_root: market_squawk_platform::ArtifactRoot,
        object_config: ObjectStoreConfig,
    ) -> Result<Self, IngestError> {
        if authority.artifact_root_binding() != manifests.catalog_binding() {
            return Err(IngestError::CatalogCompositionMismatch);
        }
        let (authority, objects) =
            AuthorityTransitionService::initialize(authority, artifact_root, object_config)
                .map_err(map_authority_transition_error)?;
        Ok(Self::from_active_parts(authority, manifests, objects))
    }

    /// Explicitly verifies and migrates an exact version-3/version-4 catalog and v1 root pair.
    pub fn migrate_legacy(
        authority: CatalogAuthority,
        manifests: AnalyticalManifestCatalog,
        artifact_root: market_squawk_platform::ArtifactRoot,
        object_config: ObjectStoreConfig,
    ) -> Result<Self, IngestError> {
        if authority.artifact_root_binding() != manifests.catalog_binding() {
            return Err(IngestError::CatalogCompositionMismatch);
        }
        let (authority, objects) =
            AuthorityTransitionService::migrate_legacy(authority, artifact_root, object_config)
                .map_err(map_authority_transition_error)?;
        Ok(Self::from_active_parts(authority, manifests, objects))
    }

    /// Opens immutable storage from controlled capabilities and a validated generation catalog.
    pub fn open(
        authority: CatalogAuthority,
        manifests: AnalyticalManifestCatalog,
        artifact_root: market_squawk_platform::ArtifactRoot,
        object_config: ObjectStoreConfig,
    ) -> Result<Self, IngestError> {
        if authority.artifact_root_binding() != manifests.catalog_binding() {
            return Err(IngestError::CatalogCompositionMismatch);
        }
        let (authority, objects) =
            AuthorityTransitionService::open_bound(authority, artifact_root, object_config)
                .map_err(map_authority_transition_error)?;
        Ok(Self::from_active_parts(authority, manifests, objects))
    }

    pub(crate) fn from_active_parts(
        authority: CatalogAuthority,
        manifests: AnalyticalManifestCatalog,
        objects: ParquetObjectStore,
    ) -> Self {
        let catalog_id = authority.session_id();
        Self {
            authority: Arc::new(Mutex::new(authority)),
            catalog_id,
            manifests: Arc::new(manifests),
            objects: Arc::new(objects),
            operation_gate: AnalyticalOperationGate::default(),
        }
    }

    /// Returns the controlled object capability for manifest-pinned query construction.
    pub fn object_store(&self) -> Arc<ParquetObjectStore> {
        Arc::clone(&self.objects)
    }

    /// Returns a cloneable immutable manifest and fixed-template observation read capability.
    pub fn analytical_reader(&self) -> crate::AnalyticalReadCapability {
        crate::AnalyticalReadCapability::new(Arc::clone(&self.manifests), Arc::clone(&self.objects))
    }

    /// Executes one exact pinned query, reserving durable artifact authority only after the first
    /// bounded execution proves that the result crossed the inline threshold.
    ///
    /// The retry uses the same parsed request, manifest, complete execution limits, and immutable
    /// object graph. This avoids leaving unused durable reservations for ordinary inline results.
    pub async fn query_pinned_with_artifact_publication(
        &self,
        operation: PinnedArtifactQueryRequest,
    ) -> Result<PinnedQueryOutput, QueryError> {
        let PinnedArtifactQueryRequest {
            pinned,
            table_name,
            query,
            limits,
            owner,
            artifact_ttl,
            cancellation,
            operation_deadline,
        } = operation;
        let retry_query = query.retry_without_artifact()?;
        let engine = tokio::time::timeout_at(
            operation_deadline,
            ResearchQueryEngine::from_pinned_dataset(
                pinned,
                table_name,
                Arc::clone(&self.objects),
                cancellation.clone(),
            ),
        )
        .await
        .map_err(|_| QueryError::DeadlineExceeded)??;
        let limits = remaining_query_limits(limits, operation_deadline)?;
        match engine
            .query_pinned(query, limits, cancellation.clone())
            .await
        {
            Ok(output) => Ok(output),
            Err(QueryError::ArtifactStoreRequired) => {
                let engine = engine.with_artifact_publication(self.query_artifact_publication())?;
                let limits = remaining_query_limits(limits, operation_deadline)?;
                let expires_at = system_timestamp()
                    .ok()
                    .zip(i64::try_from(artifact_ttl.as_nanos()).ok())
                    .and_then(|(now, ttl)| now.checked_add_nanos(ttl).ok())
                    .ok_or(QueryError::ArtifactAuthorityRequired)?;
                let reservation_input = QueryArtifactReservationInput::try_new(
                    owner,
                    retry_query.artifact_identity(&limits),
                    limits.max_bytes(),
                    expires_at,
                )
                .map_err(QueryError::Catalog)?;
                let reservation = tokio::time::timeout_at(
                    operation_deadline,
                    self.reserve_query_artifact(reservation_input, &cancellation),
                )
                .await
                .map_err(|_| QueryError::DeadlineExceeded)?
                .map_err(map_query_reservation_error)?;
                let query = retry_query.with_artifact_reservation(reservation);
                engine.query_pinned(query, limits, cancellation).await
            }
            Err(error) => Err(error),
        }
    }

    /// Returns fair-value persistence authority over this service's sole catalog writer.
    pub fn fair_value_catalog(&self) -> crate::FairValueCatalogCapability {
        crate::FairValueCatalogCapability::new(Arc::clone(&self.authority))
    }

    /// Returns provider-onboarding authority over this service's sole catalog writer.
    pub fn onboarding_catalog(&self) -> crate::OnboardingCatalogCapability {
        crate::OnboardingCatalogCapability::new(Arc::clone(&self.authority))
    }

    /// Returns bounded point-in-time definition reads over this service's sole catalog session.
    pub fn instrument_definitions(&self) -> crate::InstrumentDefinitionReadCapability {
        crate::InstrumentDefinitionReadCapability::new(Arc::clone(&self.authority))
    }

    /// Returns bounded company-identity reads over this service's sole catalog session.
    pub fn company_identities(&self) -> crate::CompanyIdentityReadCapability {
        crate::CompanyIdentityReadCapability::new(Arc::clone(&self.authority))
    }

    /// Returns bounded canonical-instrument publication authority over the sole catalog writer.
    pub fn instrument_catalog(&self) -> crate::InstrumentCatalogCapability {
        crate::InstrumentCatalogCapability::new(Arc::clone(&self.authority))
    }

    /// Returns the rights-bound point-in-time dataset builder for this exact catalog/root pair.
    pub fn dataset_builder(&self) -> crate::DatasetBuilderService<'_> {
        crate::DatasetBuilderService::new(
            self,
            Arc::clone(&self.authority),
            self.operation_gate.clone(),
        )
    }

    /// Returns object-safe observed-revision authority over this exact shared catalog writer.
    pub fn observed_revision_authority(&self) -> Arc<dyn ObservedRevisionAuthority> {
        Arc::new(CatalogObservedRevisionAuthority::new(Arc::clone(
            &self.authority,
        )))
    }

    /// Registers a source and atomically admits and reserves one exact persist operation through
    /// this service's sole catalog authority.
    pub async fn reserve_source_ingest(
        &self,
        source: &SourceMetadata,
        registered_at: Timestamp,
        rights: RightsDecisionInput,
        identity: &IngestIdentity,
        cancellation: &CancellationToken,
    ) -> Result<IngestReservation, IngestError> {
        if source.source_id() != identity.source_id()
            || rights.source_id != *identity.source_id()
            || rights.payload_digest != identity.payload_digest()
            || identity.operation() != SourceOperation::Persist
        {
            return Err(IngestError::ReservationPayloadMismatch);
        }
        let _operation = self
            .operation_gate
            .acquire(cancellation)
            .await
            .ok_or(IngestError::Cancelled)?;
        let authority = self.lock_authority()?;
        if authority
            .source(source.source_id())?
            .as_ref()
            .is_none_or(|registered| registered != source)
        {
            authority.register_source(source, registered_at)?;
        }
        let grant = authority.admit_source_rights(rights)?;
        authority
            .reserve_ingest(identity, &grant)
            .map_err(Into::into)
    }

    /// Returns sealed backup authority for this exact active catalog and artifact root.
    pub fn backup_service(&self) -> crate::AnalyticalBackupService {
        crate::AnalyticalBackupService::new(
            self.operation_gate.clone(),
            Arc::clone(&self.authority),
            Arc::clone(&self.objects),
        )
    }

    /// Persists query-result ownership and expiry before artifact publication begins.
    pub async fn reserve_query_artifact(
        &self,
        input: QueryArtifactReservationInput,
        cancellation: &CancellationToken,
    ) -> Result<QueryArtifactReservation, IngestError> {
        let _operation = self
            .operation_gate
            .acquire(cancellation)
            .await
            .ok_or(IngestError::Cancelled)?;
        Ok(self.lock_authority()?.reserve_query_artifact(input)?)
    }

    /// Issues one sealed publisher that cannot be paired with another root or catalog writer.
    pub fn query_artifact_publication(&self) -> Arc<QueryArtifactPublication> {
        Arc::new(QueryArtifactPublication {
            objects: Arc::clone(&self.objects),
            publisher: QueryArtifactPublisher::new(Arc::clone(&self.authority)),
            catalog_id: self.catalog_id,
            root_identity: self.objects.authority_identity().clone(),
            operation_gate: self.operation_gate.clone(),
            #[cfg(test)]
            bind_barrier: Mutex::new(None),
            #[cfg(test)]
            writer_barrier: Mutex::new(None),
        })
    }

    /// Resolves one exact immutable generation.
    pub fn pinned(&self, manifest: &DatasetManifestRef) -> Result<PinnedDataset, IngestError> {
        Ok(self.manifests.pinned(manifest)?)
    }

    /// Resolves a unique derived generation by its complete immutable build identity.
    pub(crate) fn matching_derived_build(
        &self,
        dataset_id: &DatasetId,
        build_spec_digest: crate::DatasetBuildSpecDigest,
    ) -> Result<Option<PinnedDataset>, IngestError> {
        self.manifests
            .matching_derived_build(dataset_id, build_spec_digest)
            .map_err(Into::into)
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
        let schema = compacted.schema_ref().clone();
        let compacted = DatasetArrowBatch::from(compacted);
        let _operation = self
            .operation_gate
            .acquire(&cancellation)
            .await
            .ok_or(IngestError::Cancelled)?;
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
        let publication = self.objects.begin_publication(&cancellation).await?;
        let published = self
            .objects
            .publish_dataset_under_lease(&compacted, &cancellation, &publication)
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
            &schema,
            &object,
            None,
        )? {
            return Ok(committed);
        }
        let plan = self
            .manifests
            .preview_compaction(request.source(), object)?;
        self.commit_plan(
            &authority,
            &reservation,
            &run,
            dataset_name,
            schema,
            plan,
            published,
            GenerationKind::Compaction,
            None,
            None,
        )
    }

    /// Quarantines only content-addressed objects absent from every retained generation.
    ///
    /// # Errors
    ///
    /// Returns [`IngestError::Cancelled`] when cancellation wins admission or is observed during
    /// bounded recovery. Returns [`IngestError::DeadlineExceeded`] when recovery exceeds its fixed
    /// elapsed-time ceiling. Catalog, object-store, or authority failures retain their typed
    /// [`IngestError`] variants.
    pub async fn recover_orphans(
        &self,
        now: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<OrphanRecoveryReport, IngestError> {
        let deadline = Instant::now()
            .checked_add(ORPHAN_RECOVERY_DEADLINE)
            .ok_or(IngestError::DeadlineExceeded)?;
        let _operation = self
            .operation_gate
            .acquire(&cancellation)
            .await
            .ok_or(IngestError::Cancelled)?;
        let mut recovery = self
            .objects
            .begin_recovery(now, &cancellation, deadline)
            .await
            .map_err(map_recovery_store_error)?;
        let _authority = self.lock_authority()?;
        let referenced = self
            .manifests
            .referenced_candidates(
                recovery
                    .candidates()
                    .iter()
                    .map(PublishedObject::content_hash),
                now,
                MAX_SCAN_OBJECTS,
                deadline,
                &cancellation,
            )
            .map_err(map_recovery_manifest_error)?;
        for (index, is_referenced) in referenced.into_iter().enumerate() {
            if is_referenced {
                continue;
            }
            recovery
                .quarantine_candidate(index)
                .map_err(map_recovery_store_error)?;
        }
        recovery.finish().map_err(map_recovery_store_error)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "ingestion retains revision, identity, cancellation, and precommit authority explicitly"
    )]
    async fn ingest_batch(
        &self,
        reservation: IngestReservation,
        analytical_dataset: DatasetId,
        batch: ExtractionBatch,
        revision_plan: Option<ExtractionRevisionPlan>,
        company_identity: Option<CompanyIdentityObservation>,
        cancellation: CancellationToken,
        precommit_authority: Option<Arc<dyn IngestPrecommitAuthority>>,
    ) -> Result<CommittedDataset, IngestError> {
        let payload_digest = extraction_provider_payload_digest(&batch);
        let source_id = batch.request().object().source_id().clone();
        if company_identity.as_ref().is_some_and(|identity| {
            identity.source_id() != &source_id
                || identity.parent_ingest_payload_evidence().content_digest() != payload_digest
        }) {
            return Err(IngestError::ReservationPayloadMismatch);
        }
        let dataset_name = SourceIdentifier::try_from(analytical_dataset.as_str())
            .map_err(|_| IngestError::InvalidDataset)?;
        {
            let authority = self.lock_authority()?;
            let run =
                self.validate_run(&authority, &reservation, payload_digest, Some(&source_id))?;
            if run.state() == IngestRunState::Failed {
                return Err(IngestError::TerminalRun);
            }
            if let Some(committed) = self.reconcile_committed_run(
                &authority,
                &reservation,
                run.state(),
                &analytical_dataset,
                company_identity.as_ref(),
            )? {
                return Ok(committed);
            }
        }
        let observations = ResearchArrowBatch::validated_extraction_observations(&batch)?;
        let revision_plan = match revision_plan {
            Some(plan) => plan,
            None => {
                let authority = self.lock_authority()?;
                let source = authority
                    .source(&source_id)?
                    .ok_or(IngestError::UnknownSource)?;
                if !matches!(
                    source.source_class(),
                    SourceClass::LocalFile | SourceClass::PortfolioExport
                ) {
                    return Err(IngestError::RevisionEvidenceRequired);
                }
                ExtractionRevisionPlan::locally_observed(observations.len())
                    .map_err(map_revision_error)?
            }
        };
        if revision_plan.len() != observations.len() {
            return Err(IngestError::RevisionEvidenceMismatch);
        }
        let observed_batch = revision_plan
            .into_observed_batch(source_id.clone(), &observations)
            .map_err(map_revision_error)?;
        let deadline = Instant::now()
            .checked_add(REVISION_ASSIGNMENT_DEADLINE)
            .ok_or(IngestError::DeadlineExceeded)?;
        let assignments = self
            .observed_revision_authority()
            .assign(observed_batch, deadline, cancellation.clone())
            .await
            .map_err(map_revision_error)?;
        let converted = ResearchArrowBatch::try_from_extraction_batch_with_assigned_revisions(
            &batch,
            assignments.as_slice(),
        )?;
        let schema = converted.schema_ref().clone();
        let lineage = converted.lineage_digest()?;
        let converted = DatasetArrowBatch::from(converted);
        self.manifests
            .validate_append_schema(&analytical_dataset, &schema)?;
        let _operation = self
            .operation_gate
            .acquire(&cancellation)
            .await
            .ok_or(IngestError::Cancelled)?;
        {
            let authority = self.lock_authority()?;
            let run =
                self.validate_run(&authority, &reservation, payload_digest, Some(&source_id))?;
            if run.state() == IngestRunState::Failed {
                return Err(IngestError::TerminalRun);
            }
        }
        let publication = self.objects.begin_publication(&cancellation).await?;
        let published = self
            .objects
            .publish_dataset_under_lease(&converted, &cancellation, &publication)
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
        if let Some(committed) = self.reconcile_existing(
            &authority,
            &reservation,
            run.state(),
            &analytical_dataset,
            &schema,
            &object,
            company_identity.as_ref(),
        )? {
            return Ok(committed);
        }
        let plan = self
            .manifests
            .preview_append(analytical_dataset, &schema, object)?;
        self.commit_plan(
            &authority,
            &reservation,
            &run,
            dataset_name,
            schema,
            plan,
            published,
            GenerationKind::Ingest,
            precommit_authority.as_deref(),
            company_identity.as_ref(),
        )
    }

    /// Ingests a locally observed batch while retaining exact caller authority through commit.
    pub async fn ingest_with_precommit_authority(
        &self,
        reservation: IngestReservation,
        analytical_dataset: DatasetId,
        batch: ExtractionBatch,
        cancellation: CancellationToken,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
    ) -> Result<CommittedDataset, IngestError> {
        self.ingest_batch(
            reservation,
            analytical_dataset,
            batch,
            None,
            None,
            cancellation,
            Some(precommit_authority),
        )
        .await
    }

    /// Ingests provider revisions while retaining exact caller authority through commit.
    pub async fn ingest_with_revision_plan_and_precommit_authority(
        &self,
        reservation: IngestReservation,
        analytical_dataset: DatasetId,
        batch: ExtractionBatch,
        revisions: ExtractionRevisionPlan,
        cancellation: CancellationToken,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
    ) -> Result<CommittedDataset, IngestError> {
        self.ingest_batch(
            reservation,
            analytical_dataset,
            batch,
            Some(revisions),
            None,
            cancellation,
            Some(precommit_authority),
        )
        .await
    }

    /// Ingests provider revisions and atomically publishes source-authored company identity.
    pub async fn ingest_with_revision_plan_and_company_identity(
        &self,
        reservation: IngestReservation,
        analytical_dataset: DatasetId,
        batch: ExtractionBatch,
        revisions: ExtractionRevisionPlan,
        company_identity: CompanyIdentityObservation,
        cancellation: CancellationToken,
    ) -> Result<CommittedDataset, IngestError> {
        self.ingest_batch(
            reservation,
            analytical_dataset,
            batch,
            Some(revisions),
            Some(company_identity),
            cancellation,
            None,
        )
        .await
    }

    /// Ingests provider revisions and atomically publishes source-authored company identity.
    #[allow(
        clippy::too_many_arguments,
        reason = "provider revision, identity, cancellation, and publication authority stay explicit"
    )]
    pub async fn ingest_with_revision_plan_company_identity_and_precommit_authority(
        &self,
        reservation: IngestReservation,
        analytical_dataset: DatasetId,
        batch: ExtractionBatch,
        revisions: ExtractionRevisionPlan,
        company_identity: CompanyIdentityObservation,
        cancellation: CancellationToken,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
    ) -> Result<CommittedDataset, IngestError> {
        self.ingest_batch(
            reservation,
            analytical_dataset,
            batch,
            Some(revisions),
            Some(company_identity),
            cancellation,
            Some(precommit_authority),
        )
        .await
    }

    fn validate_run(
        &self,
        authority: &CatalogAuthority,
        reservation: &IngestReservation,
        payload_digest: EvidenceDigest,
        source_id: Option<&market_squawk_domain::SourceId>,
    ) -> Result<crate::IngestRunRecord, IngestError> {
        authority.validate_ingest_reservation(reservation)?;
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

    fn reconcile_committed_run(
        &self,
        authority: &CatalogAuthority,
        reservation: &IngestReservation,
        state: IngestRunState,
        dataset_id: &DatasetId,
        company_identity: Option<&CompanyIdentityObservation>,
    ) -> Result<Option<CommittedDataset>, IngestError> {
        let Some(existing) = self.manifests.for_run(reservation.run_id())? else {
            return match state {
                IngestRunState::Reserved => Ok(None),
                IngestRunState::Succeeded => Err(IngestError::IncompleteSuccessfulRun),
                IngestRunState::Failed => Err(IngestError::TerminalRun),
            };
        };
        if existing.manifest().dataset_id() != dataset_id {
            return Err(IngestError::ReplayConflict);
        }
        match state {
            IngestRunState::Reserved => {
                authority.complete_ingest_with_company_identity(
                    reservation,
                    ContractCompletion::Succeeded,
                    company_identity,
                )?;
            }
            IngestRunState::Succeeded => {
                if let Some(company_identity) = company_identity {
                    authority.reconcile_company_identity(reservation, company_identity)?;
                }
            }
            IngestRunState::Failed => return Err(IngestError::TerminalRun),
        }
        Ok(Some(CommittedDataset::new(existing)))
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "recovery compares the exact run, generation, schema, object, and identity evidence"
    )]
    fn reconcile_existing(
        &self,
        authority: &CatalogAuthority,
        reservation: &IngestReservation,
        state: IngestRunState,
        dataset_id: &DatasetId,
        schema: &DatasetSchemaRef,
        object: &ManifestObject,
        company_identity: Option<&CompanyIdentityObservation>,
    ) -> Result<Option<CommittedDataset>, IngestError> {
        let Some(existing) = self.manifests.for_run(reservation.run_id())? else {
            return match state {
                IngestRunState::Reserved => Ok(None),
                IngestRunState::Succeeded => Err(IngestError::IncompleteSuccessfulRun),
                IngestRunState::Failed => Err(IngestError::TerminalRun),
            };
        };
        if existing.manifest().dataset_id() != dataset_id
            || existing.manifest().schema() != schema
            || existing.plan().objects().last() != Some(object)
        {
            return Err(IngestError::ReplayConflict);
        }
        match state {
            IngestRunState::Reserved => {
                authority.complete_ingest_with_company_identity(
                    reservation,
                    ContractCompletion::Succeeded,
                    company_identity,
                )?;
            }
            IngestRunState::Succeeded => {
                if let Some(company_identity) = company_identity {
                    authority.reconcile_company_identity(reservation, company_identity)?;
                }
            }
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
        run: &crate::IngestRunRecord,
        dataset_name: SourceIdentifier,
        schema: DatasetSchemaRef,
        plan: ManifestPlan,
        published: PublishedObject,
        kind: GenerationKind,
        precommit_authority: Option<&dyn IngestPrecommitAuthority>,
        company_identity: Option<&CompanyIdentityObservation>,
    ) -> Result<CommittedDataset, IngestError> {
        if run.state() != IngestRunState::Reserved {
            return Err(IngestError::TerminalRun);
        }
        if let Some(precommit_authority) = precommit_authority {
            precommit_authority.validate_precommit()?;
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
            schema.version(),
            artifact.artifact_id(),
            plan.content_hash().evidence(),
            created_at,
        );
        let publication = authority.publish_artifact_manifest(reservation, &artifact, &anchor)?;
        let manifest = self.manifests.commit_generation(
            &plan,
            publication.artifact(),
            publication.manifest(),
            &schema,
            kind,
            match kind {
                GenerationKind::Ingest => Some(run),
                GenerationKind::Compaction | GenerationKind::Derived => None,
            },
        )?;
        authority.complete_ingest_with_company_identity(
            reservation,
            ContractCompletion::Succeeded,
            company_identity,
        )?;
        Ok(CommittedDataset::new(self.manifests.pinned(&manifest)?))
    }

    fn lock_authority(&self) -> Result<MutexGuard<'_, CatalogAuthority>, IngestError> {
        self.authority
            .lock()
            .map_err(|_| IngestError::AuthorityLockPoisoned)
    }
}

fn remaining_query_limits(
    limits: QueryLimits,
    deadline: tokio::time::Instant,
) -> Result<QueryLimits, QueryError> {
    limits.with_operation_deadline(deadline)
}

fn map_root_authority_catalog_error(error: CatalogError) -> IngestError {
    if matches!(error, CatalogError::ArtifactRootAuthorityMismatch) {
        IngestError::Parquet(ParquetStoreError::RootCatalogMismatch)
    } else {
        IngestError::Catalog(error)
    }
}

fn map_authority_transition_error(error: AuthorityTransitionError) -> IngestError {
    match error {
        AuthorityTransitionError::Catalog(error) => map_root_authority_catalog_error(error),
        AuthorityTransitionError::Root(error) => IngestError::Parquet(error),
        AuthorityTransitionError::Restore(_) => IngestError::AuthorityTransitionRejected,
        AuthorityTransitionError::InvalidIdentity
        | AuthorityTransitionError::TransitionConflict
        | AuthorityTransitionError::LegacyEvidenceMismatch
        | AuthorityTransitionError::NotBound => IngestError::AuthorityTransitionRejected,
    }
}

fn map_revision_error(error: ObservedRevisionError) -> IngestError {
    match error {
        ObservedRevisionError::Cancelled => IngestError::Cancelled,
        ObservedRevisionError::DeadlineExceeded => IngestError::DeadlineExceeded,
        error => IngestError::RevisionAuthority(error),
    }
}

impl ResearchIngestService for AnalyticalDataService {
    async fn ingest(
        &self,
        reservation: IngestReservation,
        analytical_dataset: DatasetId,
        batch: ExtractionBatch,
        cancellation: CancellationToken,
    ) -> Result<CommittedDataset, IngestError> {
        self.ingest_batch(
            reservation,
            analytical_dataset,
            batch,
            None,
            None,
            cancellation,
            None,
        )
        .await
    }

    async fn ingest_with_revision_plan(
        &self,
        reservation: IngestReservation,
        analytical_dataset: DatasetId,
        batch: ExtractionBatch,
        revisions: ExtractionRevisionPlan,
        cancellation: CancellationToken,
    ) -> Result<CommittedDataset, IngestError> {
        self.ingest_batch(
            reservation,
            analytical_dataset,
            batch,
            Some(revisions),
            None,
            cancellation,
            None,
        )
        .await
    }
}

/// Analytical ingestion, publication, reconciliation, or compaction failure.
#[derive(Debug, Error)]
pub enum IngestError {
    /// Exact process-local publication authority was revoked before durable commit.
    #[error("research publication authority was revoked before durable commit")]
    PublicationAuthorityRevoked,
    /// The explicit analytical catalog/root authority transition was rejected.
    #[error("analytical artifact-root authority transition was rejected")]
    AuthorityTransitionRejected,
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
    /// Canonical extraction content identity could not be constructed.
    #[error("extraction batch semantic identity construction failed")]
    ContentIdentity(#[source] ExtractionError),
    /// Source-specific revision evidence or durable assignment failed.
    #[error("observed revision assignment failed")]
    RevisionAuthority(#[source] ObservedRevisionError),
    /// A non-local source omitted mandatory provider version and ordering evidence.
    #[error("provider extraction requires explicit revision evidence")]
    RevisionEvidenceRequired,
    /// Revision evidence did not align one-for-one with normalized extraction records.
    #[error("revision evidence does not match the extraction batch")]
    RevisionEvidenceMismatch,
    /// The extraction source was not registered in the retained catalog.
    #[error("analytical ingest source is unknown")]
    UnknownSource,
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
    /// Recovery exceeded its fixed elapsed-time deadline.
    #[error("analytical operation deadline exceeded")]
    DeadlineExceeded,
    /// The process-owned Task 3 authority lock was poisoned.
    #[error("analytical catalog authority is unavailable")]
    AuthorityLockPoisoned,
}

#[cfg(test)]
mod tests;
