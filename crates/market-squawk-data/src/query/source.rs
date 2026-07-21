//! Exact-file DataFusion Parquet source over verified immutable handles.

use std::fmt;
use std::io::{Read, Seek, SeekFrom};
use std::ops::Range;
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::datatypes::SchemaRef;
#[cfg(test)]
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::execution::object_store::ObjectStoreRegistry;
use datafusion::object_store::path::Path as ObjectPath;
use datafusion::object_store::{
    Attributes, CopyOptions, Error as ObjectStoreError, GetOptions, GetResult, GetResultPayload,
    ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, Result as ObjectStoreResult,
};
use datafusion::prelude::SessionContext;
use futures_util::stream::{self, BoxStream, StreamExt};
use thiserror::Error;
use url::Url;

#[cfg(test)]
use crate::ParquetStoreError;
use crate::blocking_supervisor::{BlockingIoAdmissionError, BlockingIoSupervisor};
use crate::parquet_store::{ArtifactRootIdentity, VerifiedPinnedObject};
use crate::{ParquetObjectStore, PinnedDataset, QueryError};

#[path = "source/allocation.rs"]
mod allocation;
#[path = "source/reader.rs"]
mod reader;
#[path = "source/scan.rs"]
mod scan;

use allocation::{PinnedRegistrationAdmission, PinnedRegistrationBundle, RetainedPinnedMetadata};

const PINNED_STORE_URL: &str = "market-squawk://analytical/";
const STORE_NAME: &str = "PinnedReadOnlyObjectStore";

/// Construction-checked receipt for the complete immutable dataset/batch and schema graph.
#[derive(Debug)]
pub(super) struct RetainedSourceReceipt {
    bytes: usize,
}

impl RetainedSourceReceipt {
    pub(super) const fn new(bytes: usize) -> Self {
        Self { bytes }
    }

    const fn bytes(&self) -> usize {
        self.bytes
    }
}

#[derive(Debug, Error)]
#[error("pinned Parquet range exceeded the query memory pool")]
pub(super) struct PinnedRangeMemoryError;

#[derive(Debug, Error)]
#[error("pinned Parquet I/O was cancelled")]
pub(super) struct PinnedIoCancelledError;

#[derive(Debug, Error)]
#[error("global pinned Parquet blocking-worker admission is saturated")]
pub(super) struct PinnedIoAdmissionError;

/// Query-local fixed single-slot registry. Its fixed runtime allocation is outside the pinned
/// source R/P model; the only variable ownership is the already-admitted store `Arc` in the slot.
#[derive(Debug, Default)]
pub(super) struct PinnedObjectStoreRegistry {
    state: Mutex<PinnedRegistryState>,
}

impl PinnedObjectStoreRegistry {
    fn reserve_install(
        &self,
        store: Arc<dyn ObjectStore>,
    ) -> Result<PinnedRegistryInstall<'_>, QueryError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| QueryError::DependencyAllocationContract)?;
        if !matches!(*state, PinnedRegistryState::Empty) {
            return Err(QueryError::DependencyAllocationContract);
        }
        *state = PinnedRegistryState::Reserved;
        Ok(PinnedRegistryInstall {
            state,
            store,
            committed: false,
        })
    }
}

#[derive(Debug, Default)]
enum PinnedRegistryState {
    #[default]
    Empty,
    Reserved,
    Installed(Arc<dyn ObjectStore>),
}

struct PinnedRegistryInstall<'a> {
    state: MutexGuard<'a, PinnedRegistryState>,
    store: Arc<dyn ObjectStore>,
    committed: bool,
}

impl PinnedRegistryInstall<'_> {
    fn commit(mut self) {
        *self.state = PinnedRegistryState::Installed(Arc::clone(&self.store));
        self.committed = true;
    }
}

impl Drop for PinnedRegistryInstall<'_> {
    fn drop(&mut self) {
        if !self.committed {
            *self.state = PinnedRegistryState::Empty;
        }
    }
}

impl ObjectStoreRegistry for PinnedObjectStoreRegistry {
    fn register_store(
        &self,
        _url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        Some(store)
    }

    fn deregister_store(&self, _url: &Url) -> datafusion::common::Result<Arc<dyn ObjectStore>> {
        Err(DataFusionError::Execution(
            "the fixed pinned object-store registry refuses public mutation".to_owned(),
        ))
    }

    fn get_store(&self, url: &Url) -> datafusion::common::Result<Arc<dyn ObjectStore>> {
        if url.as_str() != PINNED_STORE_URL {
            return Err(DataFusionError::Execution(
                "object-store URL is outside the fixed pinned slot".to_owned(),
            ));
        }
        let state = self.state.lock().map_err(|_| {
            DataFusionError::Execution("pinned object-store registry poisoned".into())
        })?;
        match &*state {
            PinnedRegistryState::Installed(store) => Ok(Arc::clone(store)),
            PinnedRegistryState::Empty | PinnedRegistryState::Reserved => Err(
                DataFusionError::Execution("pinned object-store slot is unavailable".to_owned()),
            ),
        }
    }
}

#[derive(Debug)]
pub(super) enum QuerySource {
    Pinned {
        dataset: Box<PinnedDataset>,
        store: Arc<ParquetObjectStore>,
        schema: SchemaRef,
        receipt: RetainedSourceReceipt,
    },
    #[cfg(test)]
    Batches {
        schema: SchemaRef,
        batches: Arc<Box<[RecordBatch]>>,
        receipt: RetainedSourceReceipt,
    },
}

impl QuerySource {
    pub(super) fn schema(&self) -> &SchemaRef {
        match self {
            Self::Pinned { schema, .. } => schema,
            #[cfg(test)]
            Self::Batches { schema, .. } => schema,
        }
    }

    pub(super) fn root_identity(&self) -> Option<&ArtifactRootIdentity> {
        match self {
            Self::Pinned { store, .. } => Some(store.authority_identity()),
            #[cfg(test)]
            Self::Batches { .. } => None,
        }
    }

    pub(super) fn retained_bytes(&self) -> Result<usize, QueryError> {
        match self {
            Self::Pinned { receipt, .. } => Ok(receipt.bytes()),
            #[cfg(test)]
            Self::Batches { receipt, .. } => Ok(receipt.bytes()),
        }
    }

    pub(super) async fn register(
        &self,
        context: &SessionContext,
        table_name: &str,
        supervisor: &BlockingIoSupervisor,
        input_memory: &MemoryReservation,
        registry: &PinnedObjectStoreRegistry,
        max_memory_bytes: u64,
    ) -> Result<(), QueryError> {
        match self {
            Self::Pinned {
                dataset,
                store,
                schema,
                ..
            } => {
                let admission = PinnedRegistrationAdmission::reserve_for_dataset(
                    schema,
                    dataset,
                    input_memory.new_empty(),
                    max_memory_bytes,
                )?;
                let memory_pool = Arc::clone(&context.runtime_env().memory_pool);
                let verified = store.capture_pinned_async(dataset, supervisor).await?;
                PinnedRegistrationBundle::complete(
                    Arc::clone(schema),
                    verified,
                    memory_pool,
                    supervisor.clone(),
                    admission,
                )?
                .publish(context, table_name, registry)
            }
            #[cfg(test)]
            Self::Batches {
                schema, batches, ..
            } => {
                let storage = Arc::new(scan::ImmutableSourceStorage::batches(Arc::clone(batches)));
                let table = scan::ImmutableSourceTable::try_new(
                    Arc::clone(schema),
                    storage,
                    Arc::clone(&context.runtime_env().memory_pool),
                    supervisor.clone(),
                )?;
                context.register_table(table_name, Arc::new(table))?;
                Ok(())
            }
        }
    }
}

#[cfg(test)]
fn map_capture_error(error: ParquetStoreError) -> QueryError {
    if matches!(error, ParquetStoreError::Cancelled) {
        QueryError::Cancelled
    } else {
        QueryError::Artifact(error)
    }
}

/// A query-scoped store that can only read the files captured from an exact manifest.
#[derive(Debug)]
struct PinnedReadOnlyObjectStore {
    files: Arc<Box<[VerifiedPinnedObject]>>,
    memory_pool: Arc<dyn MemoryPool>,
    supervisor: BlockingIoSupervisor,
    _retained_metadata: Arc<RetainedPinnedMetadata>,
}

impl PinnedReadOnlyObjectStore {
    fn new_exact(
        files: Arc<Box<[VerifiedPinnedObject]>>,
        memory_pool: Arc<dyn MemoryPool>,
        supervisor: BlockingIoSupervisor,
        retained_metadata: Arc<RetainedPinnedMetadata>,
    ) -> Self {
        Self {
            files,
            memory_pool,
            supervisor,
            _retained_metadata: retained_metadata,
        }
    }

    fn unsupported(operation: &str) -> ObjectStoreError {
        ObjectStoreError::NotImplemented {
            operation: operation.to_owned(),
            implementer: STORE_NAME.to_owned(),
        }
    }

    fn not_found(location: &ObjectPath) -> ObjectStoreError {
        ObjectStoreError::NotFound {
            path: location.to_string(),
            source: "path is not present in the exact pinned manifest".into(),
        }
    }
}

impl fmt::Display for PinnedReadOnlyObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(STORE_NAME)
    }
}

#[async_trait]
impl ObjectStore for PinnedReadOnlyObjectStore {
    async fn put_opts(
        &self,
        _location: &ObjectPath,
        _payload: PutPayload,
        _options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        Err(Self::unsupported("put_opts"))
    }

    async fn put_multipart_opts(
        &self,
        _location: &ObjectPath,
        _options: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        Err(Self::unsupported("put_multipart_opts"))
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        let pinned = self
            .files
            .binary_search_by(|candidate| candidate.relative_reference().cmp(location.as_ref()))
            .ok()
            .and_then(|index| self.files.get(index))
            .ok_or_else(|| Self::not_found(location))?;
        let meta = pinned.object_meta().clone();
        options.check_preconditions(&meta)?;
        if options.version.is_some() {
            return Err(Self::unsupported("get_opts with version"));
        }
        let range = match options.range {
            Some(requested) => {
                requested
                    .as_range(meta.size)
                    .map_err(|source| ObjectStoreError::Generic {
                        store: STORE_NAME,
                        source: Box::new(source),
                    })?
            }
            None => 0..meta.size,
        };
        let payload = if options.head {
            Bytes::new()
        } else {
            read_exact_range(
                pinned.file(),
                range.clone(),
                Arc::clone(&self.memory_pool),
                self.supervisor.clone(),
            )
            .await?
        };
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(async move { Ok(payload) }).boxed()),
            meta,
            range,
            attributes: Attributes::default(),
        })
    }

    fn delete_stream(
        &self,
        _locations: BoxStream<'static, ObjectStoreResult<ObjectPath>>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectPath>> {
        stream::once(async { Err(Self::unsupported("delete_stream")) }).boxed()
    }

    fn list(
        &self,
        _prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        stream::once(async { Err(Self::unsupported("list")) }).boxed()
    }

    async fn list_with_delimiter(
        &self,
        _prefix: Option<&ObjectPath>,
    ) -> ObjectStoreResult<ListResult> {
        Err(Self::unsupported("list_with_delimiter"))
    }

    async fn copy_opts(
        &self,
        _from: &ObjectPath,
        _to: &ObjectPath,
        _options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        Err(Self::unsupported("copy_opts"))
    }
}

async fn read_exact_range(
    file: Arc<Mutex<std::fs::File>>,
    range: Range<u64>,
    memory_pool: Arc<dyn MemoryPool>,
    supervisor: BlockingIoSupervisor,
) -> ObjectStoreResult<Bytes> {
    let cancellation = supervisor.cancellation().clone();
    #[cfg(test)]
    let worker_supervisor = supervisor.clone();
    let mut worker = supervisor
        .spawn_blocking(move || {
            if cancellation.is_cancelled() {
                return Err(cancelled_object_store());
            }
            #[cfg(test)]
            worker_supervisor
                .wait_at_test_range_barrier()
                .map_err(|source| ObjectStoreError::Generic {
                    store: STORE_NAME,
                    source: source.into(),
                })?;
            if cancellation.is_cancelled() {
                return Err(cancelled_object_store());
            }
            let length =
                usize::try_from(range.end.saturating_sub(range.start)).map_err(|error| {
                    ObjectStoreError::Generic {
                        store: STORE_NAME,
                        source: Box::new(error),
                    }
                })?;
            let reservation =
                MemoryConsumer::new("market-squawk-pinned-parquet-range").register(&memory_pool);
            reservation
                .try_grow(length)
                .map_err(|_| ObjectStoreError::Generic {
                    store: STORE_NAME,
                    source: Box::new(PinnedRangeMemoryError),
                })?;
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(length)
                .map_err(|error| ObjectStoreError::Generic {
                    store: STORE_NAME,
                    source: Box::new(error),
                })?;
            bytes.resize(length, 0);
            if cancellation.is_cancelled() {
                return Err(cancelled_object_store());
            }
            let mut file = file.lock().map_err(|_| ObjectStoreError::Generic {
                store: STORE_NAME,
                source: "verified file mutex was poisoned".into(),
            })?;
            if cancellation.is_cancelled() {
                return Err(cancelled_object_store());
            }
            file.seek(SeekFrom::Start(range.start))
                .and_then(|_| file.read_exact(&mut bytes))
                .map_err(|source| ObjectStoreError::Generic {
                    store: STORE_NAME,
                    source: Box::new(source),
                })?;
            if cancellation.is_cancelled() {
                return Err(cancelled_object_store());
            }
            Ok(Bytes::from_owner(ReservedBytes {
                bytes,
                _reservation: reservation,
            }))
        })
        .map_err(blocking_admission_object_store)?;
    tokio::select! {
        result = &mut worker => result.map_err(|source| ObjectStoreError::Generic {
            store: STORE_NAME,
            source: Box::new(source),
        })?,
        _ = supervisor.cancellation().cancelled() => Err(cancelled_object_store()),
    }
}

fn blocking_admission_object_store(error: BlockingIoAdmissionError) -> ObjectStoreError {
    ObjectStoreError::Generic {
        store: STORE_NAME,
        source: match error {
            BlockingIoAdmissionError::Cancelled => Box::new(PinnedIoCancelledError),
            BlockingIoAdmissionError::Saturated | BlockingIoAdmissionError::ReaperUnavailable => {
                Box::new(PinnedIoAdmissionError)
            }
        },
    }
}

fn cancelled_object_store() -> ObjectStoreError {
    ObjectStoreError::Generic {
        store: STORE_NAME,
        source: Box::new(PinnedIoCancelledError),
    }
}

struct ReservedBytes {
    bytes: Vec<u8>,
    _reservation: MemoryReservation,
}

impl AsRef<[u8]> for ReservedBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

#[cfg(test)]
#[path = "source/tests.rs"]
mod tests;
