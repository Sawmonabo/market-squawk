//! Bounded read-only DataFusion execution over one immutable manifest pin.

use std::collections::BTreeSet;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;

use arrow::compute::concat_batches;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::ExecutionPlanProperties as _;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::parser::{DFParserBuilder, Statement as DataFusionStatement};
use datafusion::sql::sqlparser::ast::{
    Expr, ObjectName, Query, Statement, TableFactor, Visit, Visitor,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::tokenizer::Tokenizer;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::schema::DATASET_KEY;
use crate::{
    ArrowConversionError, ArtifactRecord, CatalogError, DatasetManifestRef, ParquetObjectStore,
    ParquetStoreError, PinnedDataset, PublishedObject, ResearchArrowBatch,
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
        })
    }
}

/// Validated single-statement read-only query bound to one exact manifest generation.
#[derive(Clone, Debug)]
pub struct QueryRequest {
    manifest: DatasetManifestRef,
    sql: String,
    ast_nodes: usize,
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
        })
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
        artifact: ArtifactRecord,
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
    batches: Vec<RecordBatch>,
    artifact_store: Option<Arc<ParquetObjectStore>>,
}

impl ResearchQueryEngine {
    /// Verifies and loads only objects referenced by one exact immutable manifest generation.
    pub async fn from_pinned_dataset(
        dataset: PinnedDataset,
        table_name: impl Into<String>,
        store: Arc<ParquetObjectStore>,
        cancellation: CancellationToken,
    ) -> Result<Self, QueryError> {
        let batches = store.read_pinned_async(&dataset, &cancellation).await?;
        if cancellation.is_cancelled() {
            return Err(QueryError::Cancelled);
        }
        let schema = Arc::new(Schema::new(batches[0].schema().fields().clone()));
        let mut normalized = Vec::with_capacity(batches.len());
        for batch in batches {
            let validated = ResearchArrowBatch::try_from_record_batch(batch)?;
            let retained_dataset = validated
                .record_batch()
                .schema()
                .metadata()
                .get(DATASET_KEY)
                .cloned()
                .ok_or(QueryError::InvalidSource)?;
            if retained_dataset != dataset.manifest().dataset_id().as_str() {
                return Err(QueryError::InvalidSource);
            }
            normalized.push(RecordBatch::try_new(
                Arc::clone(&schema),
                validated.record_batch().columns().to_vec(),
            )?);
        }
        Ok(
            Self::from_pinned_batches(dataset.manifest().clone(), table_name, normalized)?
                .with_artifact_store(store),
        )
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
        Ok(Self {
            manifest,
            table_name,
            batches,
            artifact_store: None,
        })
    }

    /// Enables controlled Parquet publication for non-inline results.
    pub fn with_artifact_store(mut self, store: Arc<ParquetObjectStore>) -> Self {
        self.artifact_store = Some(store);
        self
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
        if cancellation.is_cancelled() {
            return Err(QueryError::Cancelled);
        }
        if request.ast_nodes > limits.max_ast_nodes {
            return Err(QueryError::AstLimitExceeded);
        }
        validate_relations(&request.sql, &self.table_name, limits.max_ast_nodes)?;
        let operation_cancellation = cancellation.child_token();
        let execution_cancellation = operation_cancellation.clone();
        let execution = async {
            let memory =
                usize::try_from(limits.max_memory_bytes).map_err(|_| QueryError::InvalidLimits)?;
            let runtime = RuntimeEnvBuilder::new()
                .with_memory_limit(memory, 1.0)
                .with_disk_manager_builder(
                    DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
                )
                .build_arc()?;
            let config = SessionConfig::new()
                .with_target_partitions(limits.max_partitions)
                .with_batch_size(8_192)
                .with_information_schema(false)
                .with_repartition_joins(false)
                .with_repartition_aggregations(false)
                .with_repartition_file_scans(false);
            let context = SessionContext::new_with_config_rt(config, runtime);
            let table = MemTable::try_new(self.batches[0].schema(), vec![self.batches.clone()])?;
            context.register_table(self.table_name.clone(), Arc::new(table))?;
            let dataframe = context.sql(&request.sql).await?;
            let logical_nodes = dataframe
                .logical_plan()
                .display_indent()
                .to_string()
                .lines()
                .count();
            if logical_nodes > limits.max_plan_nodes {
                return Err(QueryError::PlanLimitExceeded);
            }
            let physical = dataframe.create_physical_plan().await?;
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
            let batches = dataframe.limit(0, Some(requested_rows))?.collect().await?;
            let rows = batches.iter().try_fold(0_u64, |total, batch| {
                total
                    .checked_add(
                        u64::try_from(batch.num_rows()).map_err(|_| QueryError::InvalidLimits)?,
                    )
                    .ok_or(QueryError::InvalidLimits)
            })?;
            if rows > limits.max_rows {
                return Err(QueryError::RowLimitExceeded {
                    limit: limits.max_rows,
                });
            }
            let byte_count = ipc_size(&batches)?;
            if byte_count > limits.max_bytes {
                return Err(QueryError::ByteLimitExceeded {
                    limit: limits.max_bytes,
                });
            }
            if byte_count <= INLINE_RESULT_BYTES {
                return Ok(QueryResult::Inline {
                    batches,
                    byte_count,
                });
            }
            let store = self
                .artifact_store
                .as_ref()
                .ok_or(QueryError::ArtifactStoreRequired)?;
            let schema = batches
                .first()
                .map(RecordBatch::schema)
                .unwrap_or_else(|| self.batches[0].schema());
            let compact = concat_batches(&schema, &batches)?;
            let object = store.publish(&compact, &execution_cancellation).await?;
            let artifact = ArtifactRecord::try_new(
                object.relative_reference(),
                object.content_hash().evidence(),
                object.size_bytes(),
                object.created_at(),
            )?;
            Ok(QueryResult::Artifact { object, artifact })
        };
        tokio::select! {
            _ = cancellation.cancelled() => {
                operation_cancellation.cancel();
                Err(QueryError::Cancelled)
            },
            result = tokio::time::timeout(limits.deadline, execution) => {
                match result {
                    Ok(result) => result,
                    Err(_) => {
                        operation_cancellation.cancel();
                        Err(QueryError::DeadlineExceeded)
                    }
                }
            }
        }
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
    /// Cancellation was observed before a result crossed the service boundary.
    #[error("query was cancelled")]
    Cancelled,
    /// Wall-time deadline expired.
    #[error("query deadline exceeded")]
    DeadlineExceeded,
    /// A non-inline result had no controlled artifact capability.
    #[error("query requires a controlled artifact store")]
    ArtifactStoreRequired,
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

fn validate_read_only_statement(statement: &DataFusionStatement) -> Result<usize, QueryError> {
    match statement {
        DataFusionStatement::Statement(statement) => match statement.as_ref() {
            Statement::Query(query) => validate_query(query),
            _ => Err(QueryError::ForbiddenStatement),
        },
        DataFusionStatement::Explain(explain) => validate_read_only_statement(&explain.statement),
        DataFusionStatement::CreateExternalTable(_)
        | DataFusionStatement::CopyTo(_)
        | DataFusionStatement::Reset(_) => Err(QueryError::ForbiddenStatement),
    }
}

fn validate_query(query: &Query) -> Result<usize, QueryError> {
    let mut visitor = ConfinementVisitor::default();
    match query.visit(&mut visitor) {
        ControlFlow::Continue(()) => Ok(visitor.nodes),
        ControlFlow::Break(error) => Err(error),
    }
}

fn validate_relations(sql: &str, table_name: &str, max_nodes: usize) -> Result<(), QueryError> {
    let dialect = GenericDialect;
    let token_count = Tokenizer::new(&dialect, sql)
        .tokenize()
        .map_err(|error| QueryError::Parse(error.to_string()))?
        .len();
    if token_count > max_nodes {
        return Err(QueryError::AstLimitExceeded);
    }
    let mut parser = DFParserBuilder::new(sql)
        .with_dialect(&dialect)
        .with_recursion_limit(64)
        .build()
        .map_err(|error| QueryError::Parse(error.to_string()))?;
    let statement = parser
        .parse_statements()
        .map_err(|error| QueryError::Parse(error.to_string()))?
        .pop_front()
        .ok_or(QueryError::ForbiddenStatement)?;
    let mut visitor = RelationVisitor::new(table_name);
    match statement {
        DataFusionStatement::Statement(statement) => match statement.as_ref() {
            Statement::Query(query) => match query.visit(&mut visitor) {
                ControlFlow::Continue(()) => Ok(()),
                ControlFlow::Break(error) => Err(error),
            },
            _ => Err(QueryError::ForbiddenStatement),
        },
        DataFusionStatement::Explain(explain) => {
            validate_relations(&explain.statement.to_string(), table_name, max_nodes)
        }
        _ => Err(QueryError::ForbiddenStatement),
    }
}

#[derive(Default)]
struct ConfinementVisitor {
    nodes: usize,
}

impl Visitor for ConfinementVisitor {
    type Break = QueryError;

    fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
        self.nodes = self.nodes.saturating_add(1);
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<Self::Break> {
        self.nodes = self.nodes.saturating_add(1);
        match factor {
            TableFactor::Table { args: None, .. } | TableFactor::Derived { .. } => {
                ControlFlow::Continue(())
            }
            _ => ControlFlow::Break(QueryError::ForbiddenTableFunction),
        }
    }

    fn pre_visit_expr(&mut self, expression: &Expr) -> ControlFlow<Self::Break> {
        self.nodes = self.nodes.saturating_add(1);
        if let Expr::Function(function) = expression {
            let name = function.name.to_string().to_ascii_lowercase();
            if !matches!(
                name.as_str(),
                "abs"
                    | "avg"
                    | "coalesce"
                    | "count"
                    | "date_trunc"
                    | "lower"
                    | "max"
                    | "min"
                    | "round"
                    | "sum"
                    | "upper"
            ) {
                return ControlFlow::Break(QueryError::ForbiddenFunction);
            }
        }
        ControlFlow::Continue(())
    }
}

struct RelationVisitor {
    allowed: BTreeSet<String>,
}

impl RelationVisitor {
    fn new(table_name: &str) -> Self {
        Self {
            allowed: BTreeSet::from([table_name.to_ascii_lowercase()]),
        }
    }
}

impl Visitor for RelationVisitor {
    type Break = QueryError;

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                self.allowed
                    .insert(cte.alias.name.value.to_ascii_lowercase());
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
        let relation = relation.to_string().to_ascii_lowercase();
        if self.allowed.contains(&relation) {
            ControlFlow::Continue(())
        } else {
            ControlFlow::Break(QueryError::ForbiddenRelation)
        }
    }
}

fn ipc_size(batches: &[RecordBatch]) -> Result<u64, QueryError> {
    let Some(first) = batches.first() else {
        return Ok(0);
    };
    let mut bytes = Vec::new();
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut bytes, &first.schema())?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish()?;
        writer.flush()?;
    }
    u64::try_from(bytes.len()).map_err(|_| QueryError::InvalidLimits)
}

fn valid_table_name(value: &str) -> bool {
    let mut bytes = value.bytes();
    bytes.next().is_some_and(|first| first.is_ascii_lowercase())
        && value.len() <= 64
        && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}
