//! Purpose-built immutable DataFusion table providers and leaf plans.

use std::fmt;
use std::mem::size_of;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::ArrayRef;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, SchedulingType};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};

use super::RetainedPinnedMetadata;
use super::reader;
use crate::QueryError;
use crate::blocking_supervisor::BlockingIoSupervisor;
use crate::parquet_store::VerifiedPinnedObject;

const MAX_SCAN_NODES: usize = 10_000;
const PLAN_FIXED_RECEIPT: usize = 64 * 1024;

/// Immutable source storage shared by every logical occurrence and physical execution.
#[derive(Debug)]
pub(super) enum ImmutableSourceStorage {
    Pinned {
        files: Arc<Box<[VerifiedPinnedObject]>>,
        _receipt: Arc<RetainedPinnedMetadata>,
    },
    Batches {
        batches: Arc<Box<[RecordBatch]>>,
    },
}

impl ImmutableSourceStorage {
    pub(super) fn pinned(
        files: Arc<Box<[VerifiedPinnedObject]>>,
        receipt: Arc<RetainedPinnedMetadata>,
    ) -> Self {
        Self::Pinned {
            files,
            _receipt: receipt,
        }
    }

    pub(super) fn batches(batches: Arc<Box<[RecordBatch]>>) -> Self {
        Self::Batches { batches }
    }
}

/// A registered immutable table. `scan` admits its bounded plan representation before allocation.
#[derive(Debug)]
pub(super) struct ImmutableSourceTable {
    schema: SchemaRef,
    storage: Arc<ImmutableSourceStorage>,
    memory_pool: Arc<dyn MemoryPool>,
    supervisor: BlockingIoSupervisor,
    active_scans: Arc<AtomicUsize>,
}

impl ImmutableSourceTable {
    pub(super) fn try_new(
        schema: SchemaRef,
        storage: Arc<ImmutableSourceStorage>,
        memory_pool: Arc<dyn MemoryPool>,
        supervisor: BlockingIoSupervisor,
    ) -> Result<Self, QueryError> {
        if let ImmutableSourceStorage::Pinned { files, .. } = storage.as_ref() {
            reader::validate_supported_schema(&schema)?;
            if files.is_empty() {
                return Err(QueryError::InvalidSource);
            }
            for file in files.iter() {
                reader::validate_file_schema(&schema, file)?;
                let _ = reader::active_file_receipt(&schema, None, file)?;
            }
        }
        Ok(Self {
            schema,
            storage,
            memory_pool,
            supervisor,
            active_scans: Arc::new(AtomicUsize::new(0)),
        })
    }
}

#[async_trait]
impl TableProvider for ImmutableSourceTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let scan_lease = ScanLease::acquire(Arc::clone(&self.active_scans))?;
        let plan_receipt =
            MemoryConsumer::new("market-squawk-immutable-scan-plan").register(&self.memory_pool);
        let plan_bytes = plan_receipt_bytes(&self.storage, projection)?;
        plan_receipt.try_grow(plan_bytes).map_err(|_| {
            DataFusionError::ResourcesExhausted(
                "immutable scan plan exceeded the query memory pool".into(),
            )
        })?;
        let scan_lease = Arc::new(scan_lease);
        let projection = ScanProjection::try_new(&self.schema, projection)?;
        let batch_projection = match self.storage.as_ref() {
            ImmutableSourceStorage::Batches { batches } => match projection.indices.as_deref() {
                Some(indices) => Some(Arc::new(project_batches(batches, indices)?)),
                None => Some(Arc::clone(batches)),
            },
            ImmutableSourceStorage::Pinned { .. } => None,
        };
        let plan = ImmutableSourcePlan::try_new(
            Arc::clone(&self.storage),
            Arc::new(projection),
            batch_projection,
            Arc::clone(&self.memory_pool),
            self.supervisor.clone(),
            plan_receipt,
            scan_lease,
        )
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
        Ok(Arc::new(plan))
    }
}

#[derive(Debug)]
pub(super) struct ScanProjection {
    pub(super) schema: SchemaRef,
    pub(super) indices: Option<Box<[usize]>>,
    pub(super) decode_indices: Option<Box<[usize]>>,
    pub(super) output_remap: Option<Box<[usize]>>,
}

impl ScanProjection {
    fn try_new(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> DataFusionResult<Self> {
        let Some(indices) = projection else {
            return Ok(Self {
                schema: Arc::clone(schema),
                indices: None,
                decode_indices: None,
                output_remap: None,
            });
        };
        if indices.iter().any(|index| *index >= schema.fields().len()) {
            return Err(DataFusionError::Plan(
                "immutable source projection is outside the table schema".into(),
            ));
        }
        let projected = Arc::new(schema.project(indices)?);
        let mut decode_indices = indices.clone();
        decode_indices.sort_unstable();
        decode_indices.dedup();
        let output_remap = indices
            .iter()
            .map(|requested| {
                decode_indices.binary_search(requested).map_err(|_| {
                    DataFusionError::Internal("projection remap construction failed".into())
                })
            })
            .collect::<DataFusionResult<Vec<_>>>()?;
        let identity = output_remap
            .iter()
            .enumerate()
            .all(|(index, mapped)| index == *mapped)
            && output_remap.len() == decode_indices.len();
        Ok(Self {
            schema: projected,
            indices: Some(indices.clone().into_boxed_slice()),
            decode_indices: Some(decode_indices.into_boxed_slice()),
            output_remap: (!identity).then(|| output_remap.into_boxed_slice()),
        })
    }
}

#[derive(Debug)]
struct ScanLease {
    active: Arc<AtomicUsize>,
}

impl ScanLease {
    fn acquire(active: Arc<AtomicUsize>) -> DataFusionResult<Self> {
        let acquired = active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                (value < MAX_SCAN_NODES).then_some(value + 1)
            })
            .is_ok();
        if !acquired {
            return Err(DataFusionError::ResourcesExhausted(
                "immutable source exceeded the hard scan-node cap".into(),
            ));
        }
        Ok(Self { active })
    }
}

impl Drop for ScanLease {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Custom immutable leaf plan. No DataFusion file or memory scan exists below this node.
#[derive(Debug)]
pub(super) struct ImmutableSourcePlan {
    storage: Arc<ImmutableSourceStorage>,
    projection: Arc<ScanProjection>,
    projected_batches: Option<Arc<Box<[RecordBatch]>>>,
    memory_pool: Arc<dyn MemoryPool>,
    supervisor: BlockingIoSupervisor,
    properties: Arc<PlanProperties>,
    _plan_receipt: MemoryReservation,
    _scan_lease: Arc<ScanLease>,
}

impl ImmutableSourcePlan {
    #[allow(clippy::too_many_arguments)]
    fn try_new(
        storage: Arc<ImmutableSourceStorage>,
        projection: Arc<ScanProjection>,
        projected_batches: Option<Arc<Box<[RecordBatch]>>>,
        memory_pool: Arc<dyn MemoryPool>,
        supervisor: BlockingIoSupervisor,
        plan_receipt: MemoryReservation,
        scan_lease: Arc<ScanLease>,
    ) -> Result<Self, QueryError> {
        let properties = Arc::new(
            PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(&projection.schema)),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )
            .with_scheduling_type(SchedulingType::Cooperative),
        );
        Ok(Self {
            storage,
            projection,
            projected_batches,
            memory_pool,
            supervisor,
            properties,
            _plan_receipt: plan_receipt,
            _scan_lease: scan_lease,
        })
    }

    #[cfg(test)]
    pub(super) fn storage_identity(&self) -> usize {
        Arc::as_ptr(&self.storage).cast::<()>() as usize
    }
}

impl DisplayAs for ImmutableSourcePlan {
    fn fmt_as(
        &self,
        _display: DisplayFormatType,
        formatter: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        formatter.write_str("ImmutableSourcePlan")
    }
}

impl ExecutionPlan for ImmutableSourcePlan {
    fn name(&self) -> &'static str {
        "ImmutableSourcePlan"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(
                "immutable source leaf cannot accept children".into(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "immutable source has exactly one partition".into(),
            ));
        }
        match self.storage.as_ref() {
            ImmutableSourceStorage::Batches { .. } => {
                let batches = self.projected_batches.as_ref().ok_or_else(|| {
                    DataFusionError::Internal("immutable batches were not projected".into())
                })?;
                reader::shared_batch_stream(
                    Arc::clone(&self.projection.schema),
                    Arc::clone(batches),
                    &self.memory_pool,
                )
            }
            ImmutableSourceStorage::Pinned { files, .. } => reader::execute_pinned(
                files,
                &self.projection,
                Arc::clone(&self.memory_pool),
                self.supervisor.clone(),
            ),
        }
    }
}

fn project_batches(
    batches: &[RecordBatch],
    projection: &[usize],
) -> DataFusionResult<Box<[RecordBatch]>> {
    batches
        .iter()
        .map(|batch| batch.project(projection).map_err(DataFusionError::from))
        .collect::<DataFusionResult<Vec<_>>>()
        .map(Vec::into_boxed_slice)
}

fn plan_receipt_bytes(
    storage: &ImmutableSourceStorage,
    projection: Option<&Vec<usize>>,
) -> DataFusionResult<usize> {
    let fields = projection.map_or(0, Vec::len);
    let projection_bytes = fields
        .checked_mul(size_of::<usize>() + size_of::<ArrayRef>())
        .ok_or_else(|| DataFusionError::ResourcesExhausted("scan receipt overflow".into()))?;
    let batches = match (storage, projection) {
        (ImmutableSourceStorage::Batches { batches }, Some(_)) => batches.len(),
        (ImmutableSourceStorage::Pinned { .. }, _) => 0,
        (ImmutableSourceStorage::Batches { .. }, None) => 0,
    };
    let batch_bytes = batches
        .checked_mul(
            size_of::<RecordBatch>()
                .checked_add(projection_bytes)
                .ok_or_else(|| {
                    DataFusionError::ResourcesExhausted("scan receipt overflow".into())
                })?,
        )
        .ok_or_else(|| DataFusionError::ResourcesExhausted("scan receipt overflow".into()))?;
    PLAN_FIXED_RECEIPT
        .checked_add(projection_bytes)
        .and_then(|value| value.checked_add(batch_bytes))
        .ok_or_else(|| DataFusionError::ResourcesExhausted("scan receipt overflow".into()))
}
