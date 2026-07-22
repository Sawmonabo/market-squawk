//! Checked retained-memory and serialized-byte accounting helpers.

use std::error::Error as StdError;
use std::io::Write;
use std::mem::size_of;
use std::sync::{Arc, LazyLock};

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::MemoryReservation;
use sha2::{Digest as _, Sha256};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use super::QueryError;
use super::source::{PinnedIoAdmissionError, PinnedIoCancelledError, PinnedRangeMemoryError};

const PLANNING_FIXED_RECEIPT: usize = 128 * 1024;
const PLANNING_SQL_EXPANSION: usize = 32;
const PLANNING_AST_NODE_RECEIPT: usize = 1024;
const PLANNING_PLAN_NODE_RECEIPT: usize = 4 * 1024;
const PLANNING_SCHEMA_EXPANSION: usize = 16;
const GLOBAL_PLANNING_BYTES: usize = 1024 * 1024 * 1024;
const _: () = assert!(GLOBAL_PLANNING_BYTES <= Semaphore::MAX_PERMITS);
const _: () = assert!(GLOBAL_PLANNING_BYTES <= u32::MAX as usize);
static PLANNING_BYTES: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(GLOBAL_PLANNING_BYTES)));

#[derive(Clone, Copy, Debug)]
pub(super) struct PlanningReceipt {
    bytes: u32,
}

#[derive(Debug)]
pub(super) struct PlanningAdmission {
    _permit: OwnedSemaphorePermit,
}

impl PlanningReceipt {
    pub(super) fn try_new(
        sql_bytes: usize,
        ast_nodes: usize,
        schema_bytes: usize,
        max_plan_nodes: usize,
        limit: u64,
    ) -> Result<Self, QueryError> {
        let bytes = sql_bytes
            .checked_mul(PLANNING_SQL_EXPANSION)
            .and_then(|value| {
                ast_nodes
                    .checked_mul(PLANNING_AST_NODE_RECEIPT)
                    .and_then(|ast| value.checked_add(ast))
            })
            .and_then(|value| {
                schema_bytes
                    .checked_mul(PLANNING_SCHEMA_EXPANSION)
                    .and_then(|schema| value.checked_add(schema))
            })
            .and_then(|value| {
                max_plan_nodes
                    .checked_mul(PLANNING_PLAN_NODE_RECEIPT)
                    .and_then(|plan| value.checked_add(plan))
            })
            .and_then(|value| value.checked_add(PLANNING_FIXED_RECEIPT))
            .ok_or(QueryError::SizeOverflow)?;
        if u64::try_from(bytes).map_err(|_| QueryError::SizeOverflow)? >= limit {
            return Err(QueryError::MemoryLimitExceeded { limit });
        }
        Ok(Self {
            bytes: u32::try_from(bytes).map_err(|_| QueryError::SizeOverflow)?,
        })
    }

    pub(super) async fn acquire(
        self,
        cancellation: &CancellationToken,
    ) -> Result<PlanningAdmission, QueryError> {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(QueryError::Cancelled),
            permit = Arc::clone(&PLANNING_BYTES).acquire_many_owned(self.bytes) => {
                Ok(PlanningAdmission {
                    _permit: permit.map_err(|_| QueryError::DependencyAllocationContract)?,
                })
            }
        }
    }

    pub(super) fn execution_bytes(self, limit: u64) -> Result<usize, QueryError> {
        let available = limit
            .checked_sub(u64::from(self.bytes))
            .ok_or(QueryError::MemoryLimitExceeded { limit })?;
        usize::try_from(available).map_err(|_| QueryError::InvalidLimits)
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct CountingWriter {
    pub(super) byte_count: u64,
    digest: Sha256,
}

impl CountingWriter {
    pub(super) fn digest(&self) -> [u8; 32] {
        self.digest.clone().finalize().into()
    }
}

impl Write for CountingWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let bytes = u64::try_from(buffer.len()).map_err(std::io::Error::other)?;
        self.byte_count = self
            .byte_count
            .checked_add(bytes)
            .ok_or_else(|| std::io::Error::other("IPC byte count overflow"))?;
        self.digest.update(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(super) fn schema_retained_bytes(schema: &SchemaRef) -> Result<usize, QueryError> {
    let fields = schema.fields().iter().try_fold(0_usize, |total, field| {
        // Arrow's locked `Field::size` includes the complete recursive field, owned string
        // capacities, data-type backing, and occupied metadata buckets. Doubling that public
        // measurement conservatively covers hash-table control bytes, allocator rounding, and
        // Arc-backed data-type nodes whose allocation headers are not exposed by Arrow.
        let field_graph = field
            .size()
            .checked_mul(2)
            .ok_or(QueryError::SizeOverflow)?;
        total
            .checked_add(size_of::<[usize; 2]>())
            .and_then(|value| value.checked_add(field_graph))
            .ok_or(QueryError::SizeOverflow)
    })?;
    let metadata_buckets = schema
        .metadata()
        .capacity()
        .checked_mul(size_of::<(String, String)>())
        .and_then(|value| value.checked_mul(2))
        .ok_or(QueryError::SizeOverflow)?;
    let metadata_backing =
        schema
            .metadata()
            .iter()
            .try_fold(metadata_buckets, |total, (key, value)| {
                key.capacity()
                    .checked_add(value.capacity())
                    .and_then(|backing| backing.checked_mul(2))
                    .and_then(|backing| total.checked_add(backing))
                    .ok_or(QueryError::SizeOverflow)
            })?;
    // One Arc allocation owns Schema, and a second Arc allocation owns Fields' FieldRef slice.
    size_of::<Schema>()
        .checked_add(size_of::<[usize; 2]>())
        .and_then(|value| value.checked_add(size_of::<[usize; 2]>()))
        .and_then(|value| {
            schema
                .fields()
                .len()
                .checked_mul(size_of::<arrow::datatypes::FieldRef>())
                .and_then(|field_refs| value.checked_add(field_refs))
        })
        .and_then(|value| value.checked_add(fields))
        .and_then(|value| value.checked_add(metadata_backing))
        .ok_or(QueryError::SizeOverflow)
}

pub(super) fn record_batch_retained_bytes(batch: &RecordBatch) -> Result<usize, QueryError> {
    batch
        .num_columns()
        .checked_mul(size_of::<arrow::array::ArrayRef>())
        .and_then(|columns| columns.checked_add(size_of::<RecordBatch>()))
        .and_then(|inline| inline.checked_add(batch.get_array_memory_size()))
        .ok_or(QueryError::SizeOverflow)
}

pub(super) fn reserve_memory(
    reservation: &MemoryReservation,
    additional: usize,
    limit: u64,
) -> Result<(), QueryError> {
    reservation
        .try_grow(additional)
        .map_err(|_| QueryError::MemoryLimitExceeded { limit })
}

pub(super) fn resize_memory(
    reservation: &MemoryReservation,
    size: usize,
    limit: u64,
) -> Result<(), QueryError> {
    reservation
        .try_resize(size)
        .map_err(|_| QueryError::MemoryLimitExceeded { limit })
}

pub(super) fn map_datafusion(error: DataFusionError, limit: u64) -> QueryError {
    if error_chain_has_marker::<PinnedIoCancelledError>(&error) {
        QueryError::Cancelled
    } else if error_chain_has_marker::<PinnedIoAdmissionError>(&error) {
        QueryError::BlockingTaskLimitExceeded
    } else if datafusion_memory_error(&error) {
        QueryError::MemoryLimitExceeded { limit }
    } else {
        QueryError::DataFusion(error)
    }
}

fn datafusion_memory_error(error: &DataFusionError) -> bool {
    if error_chain_has_marker::<PinnedRangeMemoryError>(error) {
        return true;
    }
    match error {
        DataFusionError::ResourcesExhausted(_) => true,
        DataFusionError::Context(_, source) => datafusion_memory_error(source),
        _ => false,
    }
}

fn error_chain_has_marker<T: StdError + 'static>(error: &(dyn StdError + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(source) = current {
        if source.downcast_ref::<T>().is_some() {
            return true;
        }
        current = source.source();
    }
    false
}

pub(super) fn valid_table_name(value: &str) -> bool {
    let mut bytes = value.bytes();
    bytes.next().is_some_and(|first| first.is_ascii_lowercase())
        && value.len() <= 64
        && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}
