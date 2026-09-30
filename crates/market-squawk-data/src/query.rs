//! Bounded read-only DataFusion execution over one immutable manifest pin.

#[cfg(test)]
use std::mem::size_of;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::ExecutionPlanProperties as _;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::parser::DFParserBuilder;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use futures_util::StreamExt as _;
use futures_util::future::BoxFuture;
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest};
use sha2::Digest as _;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

#[path = "query/budget.rs"]
mod budget;
#[path = "query/feature_receipt.rs"]
mod feature_receipt;
#[path = "query/receipt.rs"]
mod receipt;
#[path = "query/source.rs"]
mod source;
#[path = "query/spool.rs"]
mod spool;
#[cfg(test)]
#[path = "query/tests.rs"]
mod tests;
#[path = "query/validation.rs"]
mod validation;

use self::budget::{
    CountingWriter, PlanningReceipt, map_datafusion, record_batch_retained_bytes, reserve_memory,
    resize_memory, schema_retained_bytes, valid_table_name,
};
pub use self::feature_receipt::PinnedFeatureMonetaryValue;
pub use self::receipt::{PinnedMonetaryValue, PinnedQueryOutput};
use self::receipt::{RESEARCH_MONETARY_COLUMNS, pinned_object_graph_digest};
use self::source::{PinnedObjectStoreRegistry, QuerySource, RetainedSourceReceipt};
pub use self::spool::{SealedQueryBatchCursor, SealedQueryBatchStore};
use self::validation::{validate_read_only_statement, validate_relations};
use crate::blocking_supervisor::BlockingIoSupervisor;
use crate::schema::DatasetSchemaRegistry;
use crate::{
    ArrowConversionError, ArtifactRecord, CatalogError, DatasetManifestRef, ParquetObjectStore,
    ParquetStoreError, PinnedDataset, PublishedObject, QueryArtifactPublication,
    QueryArtifactReservation, QueryArtifactResult,
};

const MAX_SQL_BYTES: usize = 64 * 1024;
const MAX_ROWS: u64 = 1_000_000;
const MAX_RESULT_BYTES: u64 = 256 * 1024 * 1024;
const MAX_MEMORY_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_PARTITIONS: usize = 64;
const MAX_AST_NODES: usize = 10_000;
const MAX_PLAN_NODES: usize = 10_000;
const MAX_DEADLINE: Duration = Duration::from_secs(60);
const INLINE_RESULT_BYTES: u64 = 256 * 1024;
const DEFAULT_SPILL_BYTES: u64 = 8 * 1024 * 1024 * 1024;

#[cfg(test)]
struct QueryArtifactMemoryTestWitness {
    retained: Arc<AtomicBool>,
}

#[cfg(test)]
impl QueryArtifactMemoryTestWitness {
    fn new(retained: Arc<AtomicBool>) -> Self {
        retained.store(true, Ordering::Release);
        Self { retained }
    }
}

#[cfg(test)]
impl Drop for QueryArtifactMemoryTestWitness {
    fn drop(&mut self) {
        self.retained.store(false, Ordering::Release);
    }
}

/// Complete caller limits for one analytical query.
#[derive(Clone, Copy, Debug)]
pub struct QueryLimits {
    max_rows: u64,
    max_inline_bytes: u64,
    max_bytes: u64,
    max_memory_bytes: u64,
    max_spill_bytes: u64,
    max_partitions: usize,
    max_ast_nodes: usize,
    max_plan_nodes: usize,
    deadline: Duration,
    operation_deadline: Option<tokio::time::Instant>,
    #[cfg(test)]
    bind_precommit_deadline: Option<tokio::time::Instant>,
    #[cfg(test)]
    require_spill: bool,
}

impl QueryLimits {
    /// Constructs nonzero limits within process-wide ceilings.
    #[allow(
        clippy::too_many_arguments,
        reason = "all independent query bounds remain explicit"
    )]
    pub fn try_new(
        max_rows: u64,
        max_bytes: u64,
        max_memory_bytes: u64,
        max_partitions: usize,
        max_ast_nodes: usize,
        max_plan_nodes: usize,
        deadline: Duration,
    ) -> Result<Self, QueryError> {
        Self::try_new_with_inline_bytes(
            max_rows,
            max_bytes.min(INLINE_RESULT_BYTES),
            max_bytes,
            max_memory_bytes,
            max_partitions,
            max_ast_nodes,
            max_plan_nodes,
            deadline,
        )
    }

    /// Constructs limits with an explicit inline-result threshold independent of the complete
    /// result-byte ceiling.
    #[allow(
        clippy::too_many_arguments,
        reason = "all independent query bounds remain explicit"
    )]
    pub fn try_new_with_inline_bytes(
        max_rows: u64,
        max_inline_bytes: u64,
        max_bytes: u64,
        max_memory_bytes: u64,
        max_partitions: usize,
        max_ast_nodes: usize,
        max_plan_nodes: usize,
        deadline: Duration,
    ) -> Result<Self, QueryError> {
        if max_rows == 0
            || max_rows > MAX_ROWS
            || max_inline_bytes == 0
            || max_inline_bytes > max_bytes
            || max_bytes == 0
            || max_bytes > MAX_RESULT_BYTES
            || max_memory_bytes == 0
            || max_memory_bytes > MAX_MEMORY_BYTES
            || max_partitions == 0
            || max_partitions > MAX_PARTITIONS
            || max_ast_nodes == 0
            || max_ast_nodes > MAX_AST_NODES
            || max_plan_nodes == 0
            || max_plan_nodes > MAX_PLAN_NODES
            || deadline.is_zero()
            || deadline > MAX_DEADLINE
        {
            return Err(QueryError::InvalidLimits);
        }
        Ok(Self {
            max_rows,
            max_inline_bytes,
            max_bytes,
            max_memory_bytes,
            max_spill_bytes: DEFAULT_SPILL_BYTES,
            max_partitions,
            max_ast_nodes,
            max_plan_nodes,
            deadline,
            operation_deadline: None,
            #[cfg(test)]
            bind_precommit_deadline: None,
            #[cfg(test)]
            require_spill: false,
        })
    }

    /// Sets the operation-local temporary-disk budget independently of RAM and result bytes.
    pub fn with_spill_bytes(mut self, max_spill_bytes: u64) -> Result<Self, QueryError> {
        if max_spill_bytes == 0 || max_spill_bytes > i64::MAX as u64 {
            return Err(QueryError::InvalidLimits);
        }
        self.max_spill_bytes = max_spill_bytes;
        Ok(self)
    }

    /// Returns the independent temporary-disk budget.
    pub const fn max_spill_bytes(self) -> u64 {
        self.max_spill_bytes
    }

    /// Returns the result-byte ceiling also used by durable artifact authority.
    pub const fn max_bytes(self) -> u64 {
        self.max_bytes
    }

    /// Returns the working-RAM budget independently of complete result and temporary-disk bytes.
    pub const fn max_memory_bytes(self) -> u64 {
        self.max_memory_bytes
    }

    /// Returns the largest Arrow IPC result that may remain inline.
    pub const fn max_inline_bytes(self) -> u64 {
        self.max_inline_bytes
    }

    pub(crate) fn with_operation_deadline(
        mut self,
        operation_deadline: tokio::time::Instant,
    ) -> Result<Self, QueryError> {
        let deadline = operation_deadline.saturating_duration_since(tokio::time::Instant::now());
        if deadline.is_zero() || deadline > self.deadline {
            return Err(QueryError::DeadlineExceeded);
        }
        self.deadline = deadline;
        self.operation_deadline = Some(operation_deadline);
        Ok(self)
    }

    pub(crate) const fn deadline(self) -> Duration {
        self.deadline
    }

    #[cfg(test)]
    fn with_test_required_spill(mut self) -> Self {
        self.require_spill = true;
        self
    }

    #[cfg(test)]
    fn with_test_bind_precommit_deadline(mut self, deadline: tokio::time::Instant) -> Self {
        self.bind_precommit_deadline = Some(deadline);
        self
    }
}

pub(crate) struct QueryArtifactMemoryLease {
    _reservation: MemoryReservation,
    #[cfg(test)]
    _witness: Option<QueryArtifactMemoryTestWitness>,
}

impl QueryArtifactMemoryLease {
    pub(crate) fn try_new(
        reservation: MemoryReservation,
        expected: usize,
    ) -> Result<Self, QueryError> {
        if reservation.size() != expected {
            return Err(QueryError::DependencyAllocationContract);
        }
        Ok(Self {
            _reservation: reservation,
            #[cfg(test)]
            _witness: None,
        })
    }

    pub(crate) fn resize(&self, bytes: usize, limit: u64) -> Result<(), ParquetStoreError> {
        self._reservation
            .try_resize(bytes)
            .map_err(|_| ParquetStoreError::WriterMemoryLimitExceeded { limit })
    }

    #[cfg(test)]
    fn with_test_witness(mut self, retained: Option<Arc<AtomicBool>>) -> Self {
        self._witness = retained.map(QueryArtifactMemoryTestWitness::new);
        self
    }
}

/// Validated single-statement read-only query bound to one exact manifest generation.
#[derive(Debug)]
pub struct QueryRequest {
    manifest: DatasetManifestRef,
    sql: String,
    ast_nodes: usize,
    artifact_reservation: Option<QueryArtifactReservation>,
}

impl QueryRequest {
    /// Parses and rejects every statement outside SELECT/CTE/subquery/EXPLAIN.
    pub fn try_new(
        manifest: DatasetManifestRef,
        sql: impl Into<String>,
    ) -> Result<Self, QueryError> {
        DatasetSchemaRegistry::local()
            .resolve(manifest.schema())
            .map_err(|_| QueryError::InvalidSource)?;
        let sql = sql.into();
        if sql.is_empty() || sql.len() > MAX_SQL_BYTES || sql.bytes().any(|byte| byte == 0) {
            return Err(QueryError::InvalidSql);
        }
        let dialect = GenericDialect;
        let mut parser = DFParserBuilder::new(sql.as_str())
            .with_dialect(&dialect)
            .with_recursion_limit(64)
            .build()
            .map_err(|error| QueryError::Parse(error.to_string()))?;
        let mut statements = parser
            .parse_statements()
            .map_err(|error| QueryError::Parse(error.to_string()))?;
        if statements.len() != 1 {
            return Err(QueryError::ForbiddenStatement);
        }
        let statement = statements
            .pop_front()
            .ok_or(QueryError::ForbiddenStatement)?;
        let ast_nodes = validate_read_only_statement(&statement)?;
        Ok(Self {
            manifest,
            sql,
            ast_nodes,
            artifact_reservation: None,
        })
    }

    /// Identifies the exact manifest, row schema, and SQL independently of execution admission.
    pub fn semantic_identity(&self) -> EvidenceDigest {
        let mut identity = sha2::Sha256::new();
        identity.update(b"market-squawk/query-semantics/v1");
        self.hash_manifest_and_sql(&mut identity);
        EvidenceDigest::new(DigestAlgorithm::Sha256, identity.finalize().into())
    }

    fn hash_manifest_and_sql(&self, identity: &mut sha2::Sha256) {
        identity.update(
            u64::try_from(self.manifest.dataset_id().as_str().len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        identity.update(self.manifest.dataset_id().as_str().as_bytes());
        identity.update(self.manifest.manifest_version().to_be_bytes());
        identity.update(
            u64::try_from(self.manifest.schema().name().len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        identity.update(self.manifest.schema().name().as_bytes());
        identity.update(self.manifest.schema_version().get().to_be_bytes());
        identity.update(self.manifest.schema().fingerprint());
        identity.update(self.manifest.content_hash().bytes());
        identity.update(
            u64::try_from(self.sql.len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        identity.update(self.sql.as_bytes());
    }

    /// Computes the exact SHA-256 identity of manifest, SQL, and every execution limit.
    pub fn artifact_identity(&self, limits: &QueryLimits) -> EvidenceDigest {
        let mut identity = sha2::Sha256::new();
        identity.update(b"market-squawk/query-artifact-request/v4");
        self.hash_manifest_and_sql(&mut identity);
        identity.update(limits.max_rows.to_be_bytes());
        identity.update(limits.max_inline_bytes.to_be_bytes());
        identity.update(limits.max_bytes.to_be_bytes());
        identity.update(limits.max_memory_bytes.to_be_bytes());
        identity.update(limits.max_spill_bytes.to_be_bytes());
        identity.update(
            u64::try_from(limits.max_partitions)
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        identity.update(
            u64::try_from(limits.max_ast_nodes)
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        identity.update(
            u64::try_from(limits.max_plan_nodes)
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        identity.update(
            u64::try_from(limits.deadline.as_nanos())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        EvidenceDigest::new(DigestAlgorithm::Sha256, identity.finalize().into())
    }

    pub(crate) fn retry_without_artifact(&self) -> Result<Self, QueryError> {
        if self.artifact_reservation.is_some() {
            return Err(QueryError::ArtifactReservationMismatch);
        }
        Self::try_new(self.manifest.clone(), self.sql.clone())
    }

    /// Attaches the non-cloneable durable authority receipt required for artifact mode.
    pub fn with_artifact_reservation(mut self, reservation: QueryArtifactReservation) -> Self {
        self.artifact_reservation = Some(reservation);
        self
    }
}

/// Bounded query result or controlled artifact reference.
#[derive(Debug)]
pub enum QueryResult {
    /// Small Arrow batches returned in process.
    Inline {
        /// Returned batches.
        batches: Vec<RecordBatch>,
        /// Exact Arrow IPC stream size used for the result bound.
        byte_count: u64,
    },
    /// Complete result consumed incrementally by a data-owned operation; the receipt retains
    /// counts and digest while the consumer owns its bounded calculation or durable staging.
    Consumed {
        /// Complete streamed row count.
        row_count: u64,
        /// Exact IPC byte count included in the result digest.
        byte_count: u64,
    },
    /// Complete operation-owned IPC batches authenticated by this query receipt.
    Spooled {
        /// Sealed batches retained until the last cursor is released.
        batches: SealedQueryBatchStore,
        /// Complete streamed row count.
        row_count: u64,
        /// Exact IPC result byte count.
        byte_count: u64,
    },
    /// Larger result published through the controlled content-addressed artifact boundary.
    Artifact {
        /// Immutable Parquet object receipt.
        object: PublishedObject,
        /// Task 3 controlled-artifact metadata for the exact object bytes.
        artifact: Box<ArtifactRecord>,
        /// Durable owner and expiry binding committed before this result crossed the boundary.
        ownership: QueryArtifactResult,
    },
}

#[derive(Debug)]
struct ExecutedQuery {
    result: QueryResult,
    result_digest: EvidenceDigest,
}

/// Bounded read-only analytical query service boundary.
#[allow(
    async_fn_in_trait,
    reason = "the canonical local service contract intentionally retains native async cancellation"
)]
pub trait ResearchQueryService {
    /// Executes one manifest-pinned query under explicit caller limits.
    async fn query(
        &self,
        request: QueryRequest,
        limits: QueryLimits,
        cancellation: CancellationToken,
    ) -> Result<QueryResult, QueryError>;
}

/// DataFusion query engine over an exact immutable input snapshot.
///
/// Arbitrary in-memory batches cannot be attached to a fabricated or real durable manifest:
///
/// ```compile_fail
/// use arrow::record_batch::RecordBatch;
/// use market_squawk_data::{DatasetManifestRef, ResearchQueryEngine};
///
/// fn fabricate(manifest: DatasetManifestRef, batches: Vec<RecordBatch>) {
///     let _ = ResearchQueryEngine::from_pinned_batches(manifest, "observations", batches);
/// }
/// ```
#[derive(Debug)]
pub struct ResearchQueryEngine {
    manifest: DatasetManifestRef,
    table_name: String,
    source: QuerySource,
    artifact_publication: Option<Arc<QueryArtifactPublication>>,
}

impl ResearchQueryEngine {
    /// Retains only the exact immutable pin and object-store capability; opening happens in query.
    pub async fn from_pinned_dataset(
        dataset: PinnedDataset,
        table_name: impl Into<String>,
        store: Arc<ParquetObjectStore>,
        cancellation: CancellationToken,
    ) -> Result<Self, QueryError> {
        if cancellation.is_cancelled() {
            return Err(QueryError::Cancelled);
        }
        let table_name = table_name.into();
        if !valid_table_name(&table_name) || dataset.objects().is_empty() {
            return Err(QueryError::InvalidSource);
        }
        let schema = DatasetSchemaRegistry::local()
            .resolve(dataset.manifest().schema())
            .map_err(|_| QueryError::InvalidSource)?;
        let retained_bytes = dataset
            .retained_bytes()
            .checked_add(schema_retained_bytes(&schema)?)
            .and_then(|value| value.checked_add(dataset.manifest().dataset_id().as_str().len()))
            .and_then(|value| value.checked_add(table_name.capacity()))
            .ok_or(QueryError::SizeOverflow)?;
        Ok(Self {
            manifest: dataset.manifest().clone(),
            table_name,
            source: QuerySource::Pinned {
                dataset: Box::new(dataset),
                store: Arc::clone(&store),
                schema,
                receipt: RetainedSourceReceipt::new(retained_bytes),
            },
            artifact_publication: None,
        })
    }

    /// Test-only in-memory source with no durable provenance authority.
    #[cfg(test)]
    pub(crate) fn from_pinned_batches(
        manifest: DatasetManifestRef,
        table_name: impl Into<String>,
        batches: Vec<RecordBatch>,
    ) -> Result<Self, QueryError> {
        let table_name = table_name.into();
        if !valid_table_name(&table_name) || batches.is_empty() {
            return Err(QueryError::InvalidSource);
        }
        let schema = batches[0].schema();
        if batches.iter().any(|batch| batch.schema() != schema) {
            return Err(QueryError::InvalidSource);
        }
        if batches.capacity() != batches.len() {
            return Err(QueryError::DependencyAllocationContract);
        }
        let batch_allocation = batches.as_ptr();
        let batches = batches.into_boxed_slice();
        if batches.as_ptr() != batch_allocation {
            return Err(QueryError::DependencyAllocationContract);
        }
        let retained_bytes = batches.iter().try_fold(
            schema_retained_bytes(&schema)?
                .checked_add(size_of::<[usize; 2]>())
                .and_then(|value| value.checked_add(size_of::<Box<[RecordBatch]>>()))
                .and_then(|value| value.checked_add(manifest.dataset_id().as_str().len()))
                .and_then(|value| value.checked_add(table_name.capacity()))
                .ok_or(QueryError::SizeOverflow)?,
            |total, batch| {
                total
                    .checked_add(record_batch_retained_bytes(batch)?)
                    .ok_or(QueryError::SizeOverflow)
            },
        )?;
        let batches = Arc::new(batches);
        Ok(Self {
            manifest,
            table_name,
            source: QuerySource::Batches {
                schema: batches[0].schema(),
                batches,
                receipt: RetainedSourceReceipt::new(retained_bytes),
            },
            artifact_publication: None,
        })
    }

    /// Attaches one service-issued root/catalog publication capability.
    pub fn with_artifact_publication(
        mut self,
        publication: Arc<QueryArtifactPublication>,
    ) -> Result<Self, QueryError> {
        let source_root = self
            .source
            .root_identity()
            .ok_or(QueryError::InvalidSource)?;
        if source_root != publication.root_identity() {
            return Err(QueryError::ArtifactRootMismatch);
        }
        self.artifact_publication = Some(publication);
        Ok(self)
    }

    /// Plans and executes one bounded, cancellation-aware query.
    pub async fn query(
        &self,
        request: QueryRequest,
        limits: QueryLimits,
        cancellation: CancellationToken,
    ) -> Result<QueryResult, QueryError> {
        self.execute(request, limits, cancellation, None)
            .await
            .map(|executed| executed.result)
    }

    /// Executes against a catalog-resolved pinned dataset and returns a non-forgeable result
    /// receipt binding the complete object graph, query, limits, and exact Arrow IPC output.
    pub async fn query_pinned(
        &self,
        request: QueryRequest,
        limits: QueryLimits,
        cancellation: CancellationToken,
    ) -> Result<PinnedQueryOutput, QueryError> {
        let dataset = self
            .source
            .pinned_dataset()
            .ok_or(QueryError::PinnedQuerySourceRequired)?;
        let manifest = dataset.manifest().clone();
        let object_graph_digest = pinned_object_graph_digest(dataset);
        let query_identity = request.artifact_identity(&limits);
        let semantic_query_identity = request.semantic_identity();
        let executed = self.execute(request, limits, cancellation, None).await?;
        Ok(PinnedQueryOutput::new(
            manifest,
            object_graph_digest,
            query_identity,
            semantic_query_identity,
            executed.result_digest,
            executed.result,
        ))
    }

    /// Consumes bounded batches without artifact-write authority and issues the same immutable
    /// receipt only after the complete query succeeds. Consumers must keep partial work private.
    pub(crate) async fn query_pinned_consume(
        &self,
        request: QueryRequest,
        limits: QueryLimits,
        cancellation: CancellationToken,
        mut consume: impl FnMut(RecordBatch) -> Result<(), QueryError> + Send,
    ) -> Result<PinnedQueryOutput, QueryError> {
        let dataset = self
            .source
            .pinned_dataset()
            .ok_or(QueryError::PinnedQuerySourceRequired)?;
        let manifest = dataset.manifest().clone();
        let object_graph_digest = pinned_object_graph_digest(dataset);
        let query_identity = request.artifact_identity(&limits);
        let semantic_query_identity = request.semantic_identity();
        let executed = self
            .execute(request, limits, cancellation, Some(&mut consume))
            .await?;
        Ok(PinnedQueryOutput::new(
            manifest,
            object_graph_digest,
            query_identity,
            semantic_query_identity,
            executed.result_digest,
            executed.result,
        ))
    }

    /// Streams a complete pinned result into private scratch for bounded synchronous consumers.
    pub async fn query_pinned_spooled(
        &self,
        request: QueryRequest,
        limits: QueryLimits,
        cancellation: CancellationToken,
    ) -> Result<PinnedQueryOutput, QueryError> {
        let scratch = match &self.source {
            QuerySource::Pinned { store, .. } => store.operation_scratch()?,
            #[cfg(test)]
            QuerySource::Batches { .. } => return Err(QueryError::PinnedQuerySourceRequired),
        };
        let deadline = match limits.operation_deadline {
            Some(deadline) => deadline,
            None => tokio::time::Instant::now()
                .checked_add(limits.deadline)
                .ok_or(QueryError::InvalidLimits)?,
        };
        let mut pending =
            spool::PendingQueryBatchStore::new(scratch, deadline, cancellation.clone())?;
        let receipt = self
            .query_pinned_consume(request, limits, cancellation, |batch| pending.write(batch))
            .await?;
        let (row_count, byte_count) = match receipt.result() {
            QueryResult::Consumed {
                row_count,
                byte_count,
            } => (*row_count, *byte_count),
            _ => return Err(QueryError::InvalidSource),
        };
        let batches = pending.finish()?;
        Ok(receipt.with_result(QueryResult::Spooled {
            batches,
            row_count,
            byte_count,
        }))
    }

    /// Reads one stable row from the canonical research-observation schema using an engine-owned
    /// direct base-column projection and returns a producer-issued monetary evidence receipt.
    ///
    /// Caller SQL and caller-selected semantic columns are deliberately absent from this API.
    ///
    /// # Errors
    ///
    /// Rejects noncanonical schemas, excessive row selectors, missing rows, nonmonetary rows, or
    /// any ordinary bounded query failure.
    pub async fn canonical_research_monetary_value(
        &self,
        row: usize,
        limits: QueryLimits,
        cancellation: CancellationToken,
    ) -> Result<PinnedMonetaryValue, QueryError> {
        let canonical = DatasetSchemaRegistry::local()
            .canonical_research_observations()
            .map_err(|_| QueryError::InvalidSource)?;
        if self.manifest.schema() != &canonical || row >= MAX_ROWS as usize {
            return Err(QueryError::PinnedQuerySourceRequired);
        }
        let sql = format!(
            "SELECT value_mantissa, value_scale, currency, source_id, instrument_id, \
             venue_id, source_identifier, source_timestamp, received_at, available_at, \
             ingested_at, effective_at, published_at, revision, quality, payload_sha256 \
             FROM {} ORDER BY source_id, source_identifier, revision, payload_sha256 \
             LIMIT 1 OFFSET {row}",
            self.table_name
        );
        let output = self
            .query_pinned(
                QueryRequest::try_new(self.manifest.clone(), sql)?,
                limits,
                cancellation,
            )
            .await?;
        output.monetary_value(0, row, RESEARCH_MONETARY_COLUMNS)
    }

    /// Reads one exact monetary feature from the canonical feature/label dataset using an
    /// engine-owned direct base-column projection.
    ///
    /// The selector is an offset within canonical, non-null decimal feature rows. Caller SQL,
    /// labels, floating-point outputs, and caller-selected semantic columns cannot enter the
    /// receipt-producing path.
    ///
    /// # Errors
    ///
    /// Rejects noncanonical schemas, excessive selectors, missing rows, nonmonetary feature rows,
    /// or any ordinary bounded query failure.
    pub async fn canonical_feature_monetary_value(
        &self,
        row: usize,
        limits: QueryLimits,
        cancellation: CancellationToken,
    ) -> Result<PinnedFeatureMonetaryValue, QueryError> {
        let canonical = DatasetSchemaRegistry::local()
            .canonical_feature_labels()
            .map_err(|_| QueryError::InvalidSource)?;
        if self.manifest.schema() != &canonical || row >= MAX_ROWS as usize {
            return Err(QueryError::PinnedQuerySourceRequired);
        }
        let sql = feature_monetary_sql(&self.table_name, row);
        let output = self
            .query_pinned(
                QueryRequest::try_new(self.manifest.clone(), sql)?,
                limits,
                cancellation,
            )
            .await?;
        PinnedFeatureMonetaryValue::try_from_output(&output, row)
    }

    async fn execute(
        &self,
        request: QueryRequest,
        limits: QueryLimits,
        cancellation: CancellationToken,
        mut consumer: Option<&mut (dyn FnMut(RecordBatch) -> Result<(), QueryError> + Send)>,
    ) -> Result<ExecutedQuery, QueryError> {
        if request.manifest != self.manifest {
            return Err(QueryError::ManifestPinMismatch);
        }
        if let Some(reservation) = request.artifact_reservation.as_ref()
            && (reservation.request_identity() != request.artifact_identity(&limits)
                || reservation.max_bytes() != limits.max_bytes)
        {
            return Err(QueryError::ArtifactReservationMismatch);
        }
        if cancellation.is_cancelled() {
            return Err(QueryError::Cancelled);
        }
        if request.ast_nodes > limits.max_ast_nodes {
            return Err(QueryError::AstLimitExceeded);
        }
        let deadline_at = match limits.operation_deadline {
            Some(deadline) => deadline,
            None => tokio::time::Instant::now()
                .checked_add(limits.deadline)
                .ok_or(QueryError::InvalidLimits)?,
        };
        validate_relations(&request.sql, &self.table_name, limits.max_ast_nodes)?;
        let planning_receipt = PlanningReceipt::try_new(
            request.sql.len(),
            request.ast_nodes,
            schema_retained_bytes(self.source.schema())?,
            limits.max_plan_nodes,
            limits.max_memory_bytes,
        )?;
        let operation_cancellation = cancellation.child_token();
        let execution_cancellation = operation_cancellation.clone();
        let durable_bound = Arc::new(AtomicBool::new(false));
        let execution_durable_bound = Arc::clone(&durable_bound);
        let io_supervisor = BlockingIoSupervisor::new(operation_cancellation.clone());
        let execution_io_supervisor = io_supervisor.clone();
        // Check the recursive planner state at its owner, before composing caller futures.
        let mut execution: BoxFuture<'_, Result<ExecutedQuery, QueryError>> = Box::pin(async {
            let _planning_admission = planning_receipt.acquire(&execution_cancellation).await?;
            let memory = planning_receipt.execution_bytes(limits.max_memory_bytes)?;
            let object_store_registry = Arc::new(PinnedObjectStoreRegistry::default());
            let scratch = match &self.source {
                QuerySource::Pinned { store, .. } => store.operation_scratch()?,
                #[cfg(test)]
                QuerySource::Batches { .. } => {
                    crate::parquet_store::OperationScratchDirectory::for_test()?
                }
            };
            let runtime = RuntimeEnvBuilder::new()
                .with_memory_limit(memory, 1.0)
                .with_object_store_registry(object_store_registry.clone())
                .with_disk_manager_builder(
                    DiskManagerBuilder::default()
                        .with_mode(DiskManagerMode::Directories(vec![
                            scratch.path().to_path_buf(),
                        ]))
                        .with_max_temp_directory_size(limits.max_spill_bytes),
                )
                .build_arc()
                .map_err(|error| map_datafusion(error, limits.max_memory_bytes))?;
            let input_memory =
                MemoryConsumer::new("market-squawk-query-input").register(&runtime.memory_pool);
            reserve_memory(
                &input_memory,
                self.source.retained_bytes()?,
                limits.max_memory_bytes,
            )?;
            let output_memory =
                MemoryConsumer::new("market-squawk-query-output").register(&runtime.memory_pool);
            let ipc_memory =
                MemoryConsumer::new("market-squawk-query-ipc").register(&runtime.memory_pool);
            let mut artifact_memory = Some(
                MemoryConsumer::new("market-squawk-query-artifact").register(&runtime.memory_pool),
            );
            let mut config = SessionConfig::new()
                .with_target_partitions(limits.max_partitions)
                .with_batch_size(8_192)
                .with_information_schema(false)
                .with_repartition_joins(false)
                .with_repartition_aggregations(false)
                .with_repartition_file_scans(false);
            config.options_mut().execution.sort_spill_reservation_bytes =
                (memory / 8).min(8 * 1024 * 1024);
            config.options_mut().execution.sort_in_place_threshold_bytes =
                (memory / 16).min(1024 * 1024);
            let context = SessionContext::new_with_config_rt(config, runtime);
            self.source
                .register(
                    &context,
                    &self.table_name,
                    &execution_io_supervisor,
                    &input_memory,
                    &object_store_registry,
                    limits.max_memory_bytes,
                )
                .await?;
            let dataframe = context
                .sql(&request.sql)
                .await
                .map_err(|error| map_datafusion(error, limits.max_memory_bytes))?;
            let mut logical_nodes = 0_usize;
            dataframe
                .logical_plan()
                .apply_with_subqueries(|_| {
                    logical_nodes += 1;
                    Ok(if logical_nodes > limits.max_plan_nodes {
                        TreeNodeRecursion::Stop
                    } else {
                        TreeNodeRecursion::Continue
                    })
                })
                .map_err(|error| map_datafusion(error, limits.max_memory_bytes))?;
            if logical_nodes > limits.max_plan_nodes {
                return Err(QueryError::PlanLimitExceeded);
            }
            let physical = dataframe
                .create_physical_plan()
                .await
                .map_err(|error| map_datafusion(error, limits.max_memory_bytes))?;
            if physical.output_partitioning().partition_count() > limits.max_partitions {
                return Err(QueryError::PartitionLimitExceeded);
            }
            let requested_rows = usize::try_from(
                limits
                    .max_rows
                    .checked_add(1)
                    .ok_or(QueryError::InvalidLimits)?,
            )
            .map_err(|_| QueryError::InvalidLimits)?;
            // Enforce the service's safety limit above the already optimized physical plan.
            // A logical LIMIT is pushed into ORDER BY as TopK, whose full K-row heap cannot
            // spill; it would turn an otherwise external sort into a memory-only operation.
            let physical: Arc<dyn datafusion::physical_plan::ExecutionPlan> =
                if physical.output_partitioning().partition_count() > 1 {
                    Arc::new(
                        datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(
                            physical,
                        ),
                    )
                } else {
                    physical
                };
            let limited: Arc<dyn datafusion::physical_plan::ExecutionPlan> =
                Arc::new(datafusion::physical_plan::limit::GlobalLimitExec::new(
                    physical,
                    0,
                    Some(requested_rows),
                ));
            #[cfg(test)]
            let metrics_plan = Arc::clone(&limited);
            let mut stream = datafusion::physical_plan::execute_stream(limited, context.task_ctx())
                .map_err(|error| map_datafusion(error, limits.max_memory_bytes))?;
            let result_schema = stream.schema();
            let schema_memory = schema_retained_bytes(&result_schema)?;
            resize_memory(&ipc_memory, schema_memory, limits.max_memory_bytes)?;
            let mut ipc = arrow::ipc::writer::StreamWriter::try_new(
                CountingWriter::default(),
                &result_schema,
            )?;
            resize_memory(&ipc_memory, 0, limits.max_memory_bytes)?;
            let mut rows = 0_u64;
            let mut batches = Vec::new();
            let mut staging: Option<crate::ingest::QueryArtifactStaging> = None;
            while let Some(batch) = stream.next().await {
                if execution_cancellation.is_cancelled() {
                    return Err(QueryError::Cancelled);
                }
                let batch =
                    batch.map_err(|error| map_datafusion(error, limits.max_memory_bytes))?;
                rows = rows
                    .checked_add(
                        u64::try_from(batch.num_rows()).map_err(|_| QueryError::SizeOverflow)?,
                    )
                    .ok_or(QueryError::SizeOverflow)?;
                if rows > limits.max_rows {
                    return Err(QueryError::RowLimitExceeded {
                        limit: limits.max_rows,
                    });
                }
                let batch_memory = record_batch_retained_bytes(&batch)?;
                reserve_memory(&output_memory, batch_memory, limits.max_memory_bytes)?;
                let ipc_work = batch_memory
                    .checked_add(schema_memory)
                    .ok_or(QueryError::SizeOverflow)?;
                resize_memory(&ipc_memory, ipc_work, limits.max_memory_bytes)?;
                ipc.write(&batch)?;
                if ipc.get_ref().byte_count > limits.max_bytes {
                    return Err(QueryError::ByteLimitExceeded {
                        limit: limits.max_bytes,
                    });
                }
                if let Some(consume) = consumer.as_mut() {
                    // Keep the encoding receipt through a consumer that may write a second
                    // bounded IPC stream into its private operation store.
                    consume(batch)?;
                    resize_memory(&ipc_memory, 0, limits.max_memory_bytes)?;
                    output_memory.shrink(batch_memory);
                    continue;
                }
                resize_memory(&ipc_memory, 0, limits.max_memory_bytes)?;
                if staging.is_none() && ipc.get_ref().byte_count > limits.max_inline_bytes {
                    let publication = self
                        .artifact_publication
                        .as_ref()
                        .ok_or(QueryError::ArtifactStoreRequired)?;
                    let reservation = request
                        .artifact_reservation
                        .as_ref()
                        .ok_or(QueryError::ArtifactAuthorityRequired)?;
                    let memory = artifact_memory
                        .take()
                        .ok_or(QueryError::DependencyAllocationContract)?;
                    let initial = schema_memory
                        .checked_mul(16)
                        .and_then(|bytes| bytes.checked_add(128 * 1024))
                        .ok_or(QueryError::SizeOverflow)?;
                    resize_memory(&memory, initial, limits.max_memory_bytes)?;
                    let memory = QueryArtifactMemoryLease::try_new(memory, initial)?;
                    #[cfg(test)]
                    let memory = memory.with_test_witness(publication.test_writer_memory_witness());
                    let mut writer = publication
                        .begin_streaming(
                            result_schema.clone(),
                            &execution_cancellation,
                            reservation,
                            memory,
                            limits.max_memory_bytes,
                        )
                        .await?;
                    for retained in batches.drain(..) {
                        let retained_bytes = record_batch_retained_bytes(&retained)?;
                        writer
                            .writer
                            .write_query_batch(retained, output_memory.split(retained_bytes))
                            .await
                            .map_err(map_streaming_writer)?;
                    }
                    staging = Some(writer);
                }
                if let Some(staging) = staging.as_mut() {
                    staging
                        .writer
                        .write_query_batch(batch, output_memory.split(batch_memory))
                        .await
                        .map_err(map_streaming_writer)?;
                } else {
                    batches
                        .try_reserve_exact(1)
                        .map_err(|_| QueryError::MemoryLimitExceeded {
                            limit: limits.max_memory_bytes,
                        })?;
                    batches.push(batch);
                }
            }
            #[cfg(test)]
            if limits.require_spill {
                fn spill_count(plan: &dyn datafusion::physical_plan::ExecutionPlan) -> usize {
                    plan.metrics()
                        .and_then(|metrics| metrics.spill_count())
                        .unwrap_or(0)
                        + plan
                            .children()
                            .iter()
                            .map(|child| spill_count(child.as_ref()))
                            .sum::<usize>()
                }
                assert!(
                    spill_count(metrics_plan.as_ref()) > 0,
                    "complete query must exercise native disk spill"
                );
            }
            resize_memory(&ipc_memory, schema_memory, limits.max_memory_bytes)?;
            ipc.finish()?;
            resize_memory(&ipc_memory, 0, limits.max_memory_bytes)?;
            let byte_count = ipc.get_ref().byte_count;
            if byte_count > limits.max_bytes {
                return Err(QueryError::ByteLimitExceeded {
                    limit: limits.max_bytes,
                });
            }
            if consumer.is_some() {
                if execution_cancellation.is_cancelled() {
                    return Err(QueryError::Cancelled);
                }
                return Ok(ExecutedQuery {
                    result: QueryResult::Consumed {
                        row_count: rows,
                        byte_count,
                    },
                    result_digest: EvidenceDigest::new(
                        DigestAlgorithm::Sha256,
                        ipc.get_ref().digest(),
                    ),
                });
            }
            // The IPC end marker can itself cross the threshold for a small result.
            if staging.is_none() && byte_count > limits.max_inline_bytes {
                let publication = self
                    .artifact_publication
                    .as_ref()
                    .ok_or(QueryError::ArtifactStoreRequired)?;
                let reservation = request
                    .artifact_reservation
                    .as_ref()
                    .ok_or(QueryError::ArtifactAuthorityRequired)?;
                let memory = artifact_memory
                    .take()
                    .ok_or(QueryError::DependencyAllocationContract)?;
                let initial = schema_memory
                    .checked_mul(16)
                    .and_then(|bytes| bytes.checked_add(128 * 1024))
                    .ok_or(QueryError::SizeOverflow)?;
                resize_memory(&memory, initial, limits.max_memory_bytes)?;
                let memory = QueryArtifactMemoryLease::try_new(memory, initial)?;
                #[cfg(test)]
                let memory = memory.with_test_witness(publication.test_writer_memory_witness());
                let mut writer = publication
                    .begin_streaming(
                        result_schema.clone(),
                        &execution_cancellation,
                        reservation,
                        memory,
                        limits.max_memory_bytes,
                    )
                    .await?;
                for batch in batches.drain(..) {
                    let retained = record_batch_retained_bytes(&batch)?;
                    writer
                        .writer
                        .write_query_batch(batch, output_memory.split(retained))
                        .await
                        .map_err(map_streaming_writer)?;
                }
                staging = Some(writer);
            }
            let result_digest =
                EvidenceDigest::new(DigestAlgorithm::Sha256, ipc.get_ref().digest());
            let Some(staging) = staging else {
                if execution_cancellation.is_cancelled() {
                    return Err(QueryError::Cancelled);
                }
                return Ok(ExecutedQuery {
                    result: QueryResult::Inline {
                        batches,
                        byte_count,
                    },
                    result_digest,
                });
            };
            let publication = self
                .artifact_publication
                .as_ref()
                .ok_or(QueryError::ArtifactStoreRequired)?;
            let reservation = request
                .artifact_reservation
                .as_ref()
                .ok_or(QueryError::ArtifactAuthorityRequired)?;
            let (object, artifact, ownership) = publication
                .finish_and_bind(
                    staging,
                    &execution_cancellation,
                    reservation,
                    &execution_io_supervisor,
                    deadline_at,
                    #[cfg(test)]
                    limits.bind_precommit_deadline,
                    &execution_durable_bound,
                )
                .await?;
            Ok(ExecutedQuery {
                result: QueryResult::Artifact {
                    object,
                    artifact: Box::new(artifact),
                    ownership,
                },
                result_digest,
            })
        });
        let deadline = tokio::time::sleep_until(deadline_at);
        tokio::pin!(deadline);
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                if durable_bound.load(Ordering::Acquire) {
                    execution.as_mut().await
                } else {
                    operation_cancellation.cancel();
                    Err(QueryError::Cancelled)
                }
            },
            _ = deadline.as_mut() => {
                if durable_bound.load(Ordering::Acquire) {
                    execution.as_mut().await
                } else {
                    operation_cancellation.cancel();
                    Err(QueryError::DeadlineExceeded)
                }
            },
            result = execution.as_mut() => result,
        };
        io_supervisor.cancel();
        result
    }
}

fn map_streaming_writer(error: ParquetStoreError) -> QueryError {
    match error {
        ParquetStoreError::Cancelled => QueryError::Cancelled,
        ParquetStoreError::WriterMemoryLimitExceeded { limit } => {
            QueryError::MemoryLimitExceeded { limit }
        }
        error => QueryError::Artifact(error),
    }
}

fn feature_monetary_sql(table_name: &str, row: usize) -> String {
    format!(
        "SELECT example_id, instrument_id, source_selection_as_of, component_kind, component_name, \
         component_version, value_decimal_mantissa, value_decimal_scale, unit, currency, \
         lineage_sha256 FROM {table_name} WHERE component_kind = 1 \
         AND value_decimal_mantissa IS NOT NULL AND value_decimal_scale IS NOT NULL \
         AND currency IS NOT NULL ORDER BY example_id, instrument_id, source_selection_as_of, \
         component_name, component_version, lineage_sha256 LIMIT 1 OFFSET {row}"
    )
}

/// Query validation, resource, execution, or artifact failure.
#[derive(Debug, Error)]
pub enum QueryError {
    /// Limits are zero, inconsistent, or exceed process ceilings.
    #[error("query limits are invalid")]
    InvalidLimits,
    /// SQL is empty, oversized, or contains a NUL byte.
    #[error("query SQL is invalid")]
    InvalidSql,
    /// SQL parsing failed without exposing source data.
    #[error("query SQL parse failed: {0}")]
    Parse(String),
    /// Only one SELECT/CTE/subquery/EXPLAIN statement is allowed.
    #[error("query statement is forbidden")]
    ForbiddenStatement,
    /// Table-valued and external-access functions are forbidden.
    #[error("query table function is forbidden")]
    ForbiddenTableFunction,
    /// An unregistered scalar or aggregate function was requested.
    #[error("query function is not allowlisted")]
    ForbiddenFunction,
    /// A relation was not the pinned table or a query-local CTE.
    #[error("query relation is not allowlisted")]
    ForbiddenRelation,
    /// Input batches or table identity are invalid.
    #[error("query source is invalid")]
    InvalidSource,
    /// Request and engine manifest pins differ.
    #[error("query manifest pin mismatch")]
    ManifestPinMismatch,
    /// The receipt-producing query path requires a catalog-resolved immutable dataset.
    #[error("pinned query output requires a catalog-resolved dataset source")]
    PinnedQuerySourceRequired,
    /// Monetary cells can only be derived from retained inline Arrow output.
    #[error("monetary value extraction requires an inline query result")]
    MonetaryValueRequiresInlineResult,
    /// A requested monetary row or column coordinate is outside the bounded result.
    #[error("monetary cell coordinate is outside the query result")]
    MonetaryCellOutOfBounds,
    /// Monetary cell types, nullability, physical scale, currency, or numeric range are invalid.
    #[error("query result does not contain a valid exact monetary cell")]
    InvalidMonetaryCell,
    /// The semantic monetary scale exceeds the exact analytical decimal representation.
    #[error("query monetary scale is unsupported")]
    UnsupportedMonetaryScale,
    /// SQL AST exceeded its configured node cap.
    #[error("query AST limit exceeded")]
    AstLimitExceeded,
    /// Logical plan exceeded its configured node cap.
    #[error("query plan limit exceeded")]
    PlanLimitExceeded,
    /// Physical plan exceeded the configured partition cap.
    #[error("query partition limit exceeded")]
    PartitionLimitExceeded,
    /// Result exceeded its row limit.
    #[error("query row limit {limit} exceeded")]
    RowLimitExceeded { limit: u64 },
    /// Result exceeded its serialized byte limit.
    #[error("query byte limit {limit} exceeded")]
    ByteLimitExceeded { limit: u64 },
    /// Native DataFusion spill exhausted the operation's temporary disk allocation.
    #[error("query temporary disk allocation exhausted; retry with available temporary storage")]
    SpillStorageExhausted,
    /// Retained input, execution, output, or serialization work exceeded one memory budget.
    #[error("query memory limit {limit} exceeded")]
    MemoryLimitExceeded { limit: u64 },
    /// A retained byte count could not be represented safely.
    #[error("query retained byte count overflow")]
    SizeOverflow,
    /// A pinned Rust or DataFusion allocation assumption no longer matches the locked dependency.
    #[error("query dependency allocation contract changed")]
    DependencyAllocationContract,
    /// Process-wide admission for query blocking workers is saturated.
    #[error("query blocking-worker limit exceeded")]
    BlockingTaskLimitExceeded,
    /// A source schema has nested, dictionary, or variable-width shapes without a proved bound.
    #[error("query source schema has no supported bounded reader representation")]
    UnsupportedSourceSchema,
    /// Verified Parquet metadata requires more than the compiled active-reader ceiling.
    #[error("query source exceeds the compiled active-reader memory bound")]
    ReaderMemoryBoundExceeded,
    /// Cancellation was observed before a result crossed the service boundary.
    #[error("query was cancelled")]
    Cancelled,
    /// Wall-time deadline expired.
    #[error("query deadline exceeded")]
    DeadlineExceeded,
    /// A non-inline result had no controlled artifact capability.
    #[error("query requires a controlled artifact store")]
    ArtifactStoreRequired,
    /// A non-inline result lacked a durable least-authority publisher or reservation.
    #[error("query requires authorized artifact publication authority")]
    ArtifactAuthorityRequired,
    /// The attached reservation was issued for different request or limit bytes.
    #[error("query artifact reservation identity does not match this request")]
    ArtifactReservationMismatch,
    /// A publication capability belongs to another pinned dataset root.
    #[error("query artifact publication root does not match the pinned dataset root")]
    ArtifactRootMismatch,
    /// DataFusion planning or execution failed.
    #[error("DataFusion query failed")]
    DataFusion(#[from] datafusion::error::DataFusionError),
    /// Arrow result assembly failed.
    #[error("Arrow query result failed")]
    Arrow(#[from] arrow::error::ArrowError),
    /// Controlled artifact publication failed.
    #[error("query artifact publication failed")]
    Artifact(#[from] ParquetStoreError),
    /// Canonical persisted Arrow metadata failed revalidation.
    #[error("manifest-pinned Arrow data failed validation")]
    ArrowConversion(#[from] ArrowConversionError),
    /// Task 3 rejected controlled artifact metadata.
    #[error("query artifact metadata is invalid")]
    Catalog(#[from] CatalogError),
    /// Arrow IPC serialization failed.
    #[error("query IPC serialization failed")]
    Io(#[from] std::io::Error),
    /// Prefix-confined object-store construction failed.
    #[error("query pinned object store failed")]
    ObjectStore(#[from] datafusion::object_store::Error),
}

impl ResearchQueryService for ResearchQueryEngine {
    async fn query(
        &self,
        request: QueryRequest,
        limits: QueryLimits,
        cancellation: CancellationToken,
    ) -> Result<QueryResult, QueryError> {
        ResearchQueryEngine::query(self, request, limits, cancellation).await
    }
}
