//! Bounded read-only DataFusion execution over one immutable manifest pin.

use std::mem::size_of;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use arrow::compute::concat_batches;
use arrow::datatypes::Schema;
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
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier};
use sha2::Digest as _;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

#[path = "query/budget.rs"]
mod budget;
#[path = "query/source.rs"]
mod source;
#[path = "query/validation.rs"]
mod validation;

use self::budget::{
    CountingWriter, PlanningReceipt, map_datafusion, record_batch_retained_bytes, reserve_memory,
    resize_memory, schema_retained_bytes, valid_table_name,
};
use self::source::{PinnedObjectStoreRegistry, QuerySource, RetainedSourceReceipt};
use self::validation::{validate_read_only_statement, validate_relations};
use crate::blocking_supervisor::BlockingIoSupervisor;
use crate::schema::research_schema;
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
    max_bytes: u64,
    max_memory_bytes: u64,
    max_partitions: usize,
    max_ast_nodes: usize,
    max_plan_nodes: usize,
    deadline: Duration,
    #[cfg(test)]
    bind_precommit_deadline: Option<tokio::time::Instant>,
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
        if max_rows == 0
            || max_rows > MAX_ROWS
            || max_bytes == 0
            || max_bytes > MAX_RESULT_BYTES
            || max_memory_bytes < max_bytes
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
            max_bytes,
            max_memory_bytes,
            max_partitions,
            max_ast_nodes,
            max_plan_nodes,
            deadline,
            #[cfg(test)]
            bind_precommit_deadline: None,
        })
    }

    /// Returns the result-byte ceiling also used by durable artifact authority.
    pub const fn max_bytes(self) -> u64 {
        self.max_bytes
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
    fn try_new(reservation: MemoryReservation, expected: usize) -> Result<Self, QueryError> {
        if reservation.size() != expected {
            return Err(QueryError::DependencyAllocationContract);
        }
        Ok(Self {
            _reservation: reservation,
            #[cfg(test)]
            _witness: None,
        })
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

    /// Computes the exact SHA-256 identity of manifest, SQL, and every execution limit.
    pub fn artifact_identity(&self, limits: &QueryLimits) -> EvidenceDigest {
        let mut identity = sha2::Sha256::new();
        identity.update(b"market-squawk/query-artifact-request/v1");
        identity.update(
            u64::try_from(self.manifest.dataset_id().as_str().len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        identity.update(self.manifest.dataset_id().as_str().as_bytes());
        identity.update(self.manifest.manifest_version().to_be_bytes());
        identity.update(self.manifest.content_hash().bytes());
        identity.update(
            u64::try_from(self.sql.len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        identity.update(self.sql.as_bytes());
        identity.update(limits.max_rows.to_be_bytes());
        identity.update(limits.max_bytes.to_be_bytes());
        identity.update(limits.max_memory_bytes.to_be_bytes());
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
        let dataset_name = SourceIdentifier::try_from(dataset.manifest().dataset_id().as_str())
            .map_err(|_| QueryError::InvalidSource)?;
        let schema = Arc::new(Schema::new(
            research_schema(
                &dataset_name,
                EvidenceDigest::new(
                    DigestAlgorithm::Sha256,
                    dataset.manifest().content_hash().bytes(),
                ),
            )
            .fields()
            .clone(),
        ));
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
                dataset,
                store: Arc::clone(&store),
                schema,
                receipt: RetainedSourceReceipt::new(retained_bytes),
            },
            artifact_publication: None,
        })
    }

    /// Registers only caller-supplied batches already bound to one immutable manifest.
    pub fn from_pinned_batches(
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
        if self
            .source
            .root_identity()
            .is_some_and(|identity| identity != publication.root_identity())
        {
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
        let deadline_at = tokio::time::Instant::now()
            .checked_add(limits.deadline)
            .ok_or(QueryError::InvalidLimits)?;
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
        let execution = async {
            let _planning_admission = planning_receipt.acquire(&execution_cancellation).await?;
            let memory = planning_receipt.execution_bytes(limits.max_memory_bytes)?;
            let object_store_registry = Arc::new(PinnedObjectStoreRegistry::default());
            let runtime = RuntimeEnvBuilder::new()
                .with_memory_limit(memory, 1.0)
                .with_object_store_registry(object_store_registry.clone())
                .with_disk_manager_builder(
                    DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
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
            let artifact_memory =
                MemoryConsumer::new("market-squawk-query-artifact").register(&runtime.memory_pool);
            let config = SessionConfig::new()
                .with_target_partitions(limits.max_partitions)
                .with_batch_size(8_192)
                .with_information_schema(false)
                .with_repartition_joins(false)
                .with_repartition_aggregations(false)
                .with_repartition_file_scans(false);
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
            let limited = dataframe
                .limit(0, Some(requested_rows))
                .map_err(|error| map_datafusion(error, limits.max_memory_bytes))?;
            let mut stream = limited
                .execute_stream()
                .await
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
            while let Some(batch) = stream.next().await {
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
                batches
                    .try_reserve_exact(1)
                    .map_err(|_| QueryError::MemoryLimitExceeded {
                        limit: limits.max_memory_bytes,
                    })?;
                let ipc_work = batch_memory
                    .checked_add(schema_memory)
                    .ok_or(QueryError::SizeOverflow)?;
                resize_memory(&ipc_memory, ipc_work, limits.max_memory_bytes)?;
                ipc.write(&batch)?;
                resize_memory(&ipc_memory, 0, limits.max_memory_bytes)?;
                if ipc.get_ref().byte_count > limits.max_bytes {
                    return Err(QueryError::ByteLimitExceeded {
                        limit: limits.max_bytes,
                    });
                }
                batches.push(batch);
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
            if byte_count <= INLINE_RESULT_BYTES {
                if execution_cancellation.is_cancelled() {
                    return Err(QueryError::Cancelled);
                }
                return Ok(QueryResult::Inline {
                    batches,
                    byte_count,
                });
            }
            let publication = self
                .artifact_publication
                .as_ref()
                .ok_or(QueryError::ArtifactStoreRequired)?;
            let reservation = request
                .artifact_reservation
                .as_ref()
                .ok_or(QueryError::ArtifactAuthorityRequired)?;
            let retained_output = batches.iter().try_fold(0_usize, |total, batch| {
                total
                    .checked_add(record_batch_retained_bytes(batch)?)
                    .ok_or(QueryError::SizeOverflow)
            })?;
            resize_memory(&artifact_memory, retained_output, limits.max_memory_bytes)?;
            let compact = concat_batches(&result_schema, &batches)?;
            drop(batches);
            output_memory.free();
            let compact_memory = record_batch_retained_bytes(&compact)?;
            let writer_admission = publication.writer_admission(&compact)?;
            let publication_work = compact_memory
                .checked_add(writer_admission.bytes())
                .ok_or(QueryError::SizeOverflow)?;
            resize_memory(&artifact_memory, publication_work, limits.max_memory_bytes)?;
            let artifact_memory =
                QueryArtifactMemoryLease::try_new(artifact_memory, publication_work)?;
            #[cfg(test)]
            let artifact_memory =
                artifact_memory.with_test_witness(publication.test_writer_memory_witness());
            let (object, artifact, ownership) = publication
                .publish_and_bind(
                    compact,
                    &execution_cancellation,
                    reservation,
                    writer_admission,
                    artifact_memory,
                    &execution_io_supervisor,
                    deadline_at,
                    #[cfg(test)]
                    limits.bind_precommit_deadline,
                    &execution_durable_bound,
                )
                .await?;
            Ok(QueryResult::Artifact {
                object,
                artifact: Box::new(artifact),
                ownership,
            })
        };
        tokio::pin!(execution);
        let deadline = tokio::time::sleep_until(deadline_at);
        tokio::pin!(deadline);
        let result = tokio::select! {
            biased;
            result = execution.as_mut() => result,
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
        };
        io_supervisor.cancel();
        result
    }
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
