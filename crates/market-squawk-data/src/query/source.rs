//! Exact-file DataFusion Parquet source over verified immutable handles.

use std::collections::BTreeMap;
use std::fmt;
use std::io::{Read, Seek, SeekFrom};
use std::mem::{size_of, size_of_val};
use std::ops::Range;
use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::MemTable;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::object_store::path::Path as ObjectPath;
use datafusion::object_store::{
    Attributes, CopyOptions, Error as ObjectStoreError, GetOptions, GetResult, GetResultPayload,
    ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, Result as ObjectStoreResult,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use futures_util::stream::{self, BoxStream, StreamExt};
use thiserror::Error;

use super::budget::{reserve_memory, schema_retained_bytes};
use crate::blocking_supervisor::BlockingIoSupervisor;
use crate::parquet_store::{ArtifactRootIdentity, VerifiedPinnedObject};
use crate::{ParquetObjectStore, ParquetStoreError, PinnedDataset, QueryError};

const PINNED_STORE_URL: &str = "market-squawk://analytical";
const STORE_NAME: &str = "PinnedReadOnlyObjectStore";

#[derive(Debug, Error)]
#[error("pinned Parquet range exceeded the query memory pool")]
pub(super) struct PinnedRangeMemoryError;

#[derive(Debug, Error)]
#[error("pinned Parquet I/O was cancelled")]
pub(super) struct PinnedIoCancelledError;

#[derive(Debug)]
pub(super) enum QuerySource {
    Pinned {
        dataset: PinnedDataset,
        store: Arc<ParquetObjectStore>,
        schema: SchemaRef,
    },
    Batches {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    },
}

impl QuerySource {
    pub(super) fn root_identity(&self) -> Option<&ArtifactRootIdentity> {
        match self {
            Self::Pinned { store, .. } => Some(store.authority_identity()),
            Self::Batches { .. } => None,
        }
    }

    pub(super) fn retained_bytes(&self) -> Result<usize, QueryError> {
        match self {
            Self::Pinned {
                dataset, schema, ..
            } => dataset.objects().iter().try_fold(
                size_of::<PinnedDataset>()
                    .checked_add(schema_retained_bytes(schema)?)
                    .and_then(|value| {
                        value.checked_add(dataset.manifest().dataset_id().as_str().len())
                    })
                    .ok_or(QueryError::SizeOverflow)?,
                |total, pinned| {
                    total
                        .checked_add(size_of_val(pinned))
                        .and_then(|value| value.checked_add(pinned.relative_reference().len()))
                        .ok_or(QueryError::SizeOverflow)
                },
            ),
            Self::Batches { schema, batches } => batches.iter().try_fold(
                size_of::<Vec<RecordBatch>>()
                    .checked_add(schema_retained_bytes(schema)?)
                    .ok_or(QueryError::SizeOverflow)?,
                |total, batch| {
                    total
                        .checked_add(size_of::<RecordBatch>())
                        .and_then(|value| value.checked_add(batch.get_array_memory_size()))
                        .ok_or(QueryError::SizeOverflow)
                },
            ),
        }
    }

    pub(super) async fn register(
        &self,
        context: &SessionContext,
        table_name: &str,
        supervisor: &BlockingIoSupervisor,
        max_memory_bytes: u64,
    ) -> Result<(), QueryError> {
        match self {
            Self::Pinned {
                dataset,
                store,
                schema,
            } => {
                let metadata_bytes = pinned_registration_retained_bytes_for_manifest(dataset)?;
                let memory_pool = Arc::clone(&context.runtime_env().memory_pool);
                let metadata_reservation =
                    MemoryConsumer::new("market-squawk-pinned-parquet-metadata")
                        .register(&memory_pool);
                reserve_memory(&metadata_reservation, metadata_bytes, max_memory_bytes)?;
                let retained_metadata = Arc::new(RetainedPinnedMetadata {
                    _reservation: metadata_reservation,
                });
                let worker_metadata: Arc<dyn Send + Sync> = retained_metadata.clone();
                let verified = store
                    .capture_pinned_async(dataset, supervisor, worker_metadata)
                    .await
                    .map_err(map_capture_error)?;
                ManifestParquetTable::register(
                    context,
                    table_name,
                    Arc::clone(schema),
                    dataset,
                    verified,
                    PinnedRegistrationResources {
                        supervisor: supervisor.clone(),
                        retained_metadata,
                        max_memory_bytes,
                    },
                )
            }
            Self::Batches { schema, batches } => {
                let table = MemTable::try_new(Arc::clone(schema), vec![batches.clone()])?;
                context.register_table(table_name, Arc::new(table))?;
                Ok(())
            }
        }
    }
}

fn map_capture_error(error: ParquetStoreError) -> QueryError {
    if matches!(error, ParquetStoreError::Cancelled) {
        QueryError::Cancelled
    } else {
        QueryError::Artifact(error)
    }
}

#[derive(Debug)]
struct RetainedPinnedMetadata {
    _reservation: MemoryReservation,
}

struct PinnedRegistrationResources {
    supervisor: BlockingIoSupervisor,
    retained_metadata: Arc<RetainedPinnedMetadata>,
    max_memory_bytes: u64,
}

fn pinned_registration_retained_bytes_for_manifest(
    dataset: &PinnedDataset,
) -> Result<usize, QueryError> {
    pinned_registration_retained_bytes_for_shapes(
        dataset
            .objects()
            .iter()
            .map(|object| (object.relative_reference().len(), 64)),
    )
}

#[cfg(test)]
fn pinned_registration_retained_bytes(
    verified: &[VerifiedPinnedObject],
) -> Result<usize, QueryError> {
    pinned_registration_retained_bytes_for_shapes(
        verified
            .iter()
            .map(|object| (object.relative_reference().len(), object.etag().len())),
    )
}

fn pinned_registration_retained_bytes_for_shapes(
    mut shapes: impl Iterator<Item = (usize, usize)>,
) -> Result<usize, QueryError> {
    let fixed = size_of::<Vec<VerifiedPinnedObject>>()
        .checked_add(size_of::<BTreeMap<ObjectPath, PinnedFile>>())
        .and_then(|value| value.checked_add(size_of::<FileGroup>()))
        .and_then(|value| value.checked_add(size_of::<RetainedPinnedMetadata>()))
        .and_then(|value| value.checked_add(size_of::<usize>() * 2))
        .ok_or(QueryError::SizeOverflow)?;
    shapes.try_fold(fixed, |total, (reference, etag)| {
        let verified = size_of::<VerifiedPinnedObject>()
            .checked_add(reference)
            .and_then(|value| value.checked_add(etag))
            .and_then(|value| value.checked_add(size_of::<Mutex<std::fs::File>>()))
            .and_then(|value| value.checked_add(size_of::<usize>() * 2))
            .ok_or(QueryError::SizeOverflow)?;
        let map_node = size_of::<ObjectPath>()
            .checked_add(size_of::<PinnedFile>())
            .and_then(|value| value.checked_add(size_of::<usize>() * 3))
            .and_then(|value| value.checked_add(reference.checked_mul(2)?))
            .and_then(|value| value.checked_add(etag))
            .ok_or(QueryError::SizeOverflow)?;
        let file_group = size_of::<PartitionedFile>()
            .checked_add(reference)
            .and_then(|value| value.checked_add(etag))
            .ok_or(QueryError::SizeOverflow)?;
        total
            .checked_add(verified)
            .and_then(|value| value.checked_add(map_node))
            .and_then(|value| value.checked_add(file_group))
            .ok_or(QueryError::SizeOverflow)
    })
}

#[derive(Debug)]
struct PinnedFile {
    file: Arc<Mutex<std::fs::File>>,
    meta: ObjectMeta,
}

/// A query-scoped store that can only read the files captured from an exact manifest.
#[derive(Debug)]
struct PinnedReadOnlyObjectStore {
    files: BTreeMap<ObjectPath, PinnedFile>,
    memory_pool: Arc<dyn MemoryPool>,
    supervisor: BlockingIoSupervisor,
    _retained_metadata: Arc<RetainedPinnedMetadata>,
}

impl PinnedReadOnlyObjectStore {
    fn new_reserved(
        files: Vec<VerifiedPinnedObject>,
        memory_pool: Arc<dyn MemoryPool>,
        supervisor: BlockingIoSupervisor,
        retained_metadata: Arc<RetainedPinnedMetadata>,
    ) -> Result<Self, QueryError> {
        let mut exact = BTreeMap::new();
        for file in files {
            let location = ObjectPath::parse(file.relative_reference())
                .map_err(|_| QueryError::InvalidSource)?;
            let meta = ObjectMeta {
                location: location.clone(),
                last_modified: DateTime::<Utc>::from(file.modified_at()),
                size: file.size_bytes(),
                e_tag: Some(file.etag().to_owned()),
                version: None,
            };
            if exact
                .insert(
                    location,
                    PinnedFile {
                        file: file.file(),
                        meta,
                    },
                )
                .is_some()
            {
                return Err(QueryError::InvalidSource);
            }
        }
        Ok(Self {
            files: exact,
            memory_pool,
            supervisor,
            _retained_metadata: retained_metadata,
        })
    }

    #[cfg(test)]
    fn new(
        files: Vec<VerifiedPinnedObject>,
        memory_pool: Arc<dyn MemoryPool>,
        supervisor: BlockingIoSupervisor,
        memory_limit: u64,
    ) -> Result<Self, QueryError> {
        let retained_bytes = pinned_registration_retained_bytes(&files)?;
        let reservation =
            MemoryConsumer::new("market-squawk-pinned-parquet-metadata").register(&memory_pool);
        reserve_memory(&reservation, retained_bytes, memory_limit)?;
        Self::new_reserved(
            files,
            memory_pool,
            supervisor,
            Arc::new(RetainedPinnedMetadata {
                _reservation: reservation,
            }),
        )
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
            .get(location)
            .ok_or_else(|| Self::not_found(location))?;
        options.check_preconditions(&pinned.meta)?;
        if options.version.is_some() {
            return Err(Self::unsupported("get_opts with version"));
        }
        let range = match options.range {
            Some(requested) => requested.as_range(pinned.meta.size).map_err(|source| {
                ObjectStoreError::Generic {
                    store: STORE_NAME,
                    source: Box::new(source),
                }
            })?,
            None => 0..pinned.meta.size,
        };
        let payload = if options.head {
            Bytes::new()
        } else {
            read_exact_range(
                Arc::clone(&pinned.file),
                range.clone(),
                Arc::clone(&self.memory_pool),
                self.supervisor.clone(),
            )
            .await?
        };
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(async move { Ok(payload) }).boxed()),
            meta: pinned.meta.clone(),
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
    let supervision = supervisor.start().ok_or_else(cancelled_object_store)?;
    let cancellation = supervisor.cancellation().clone();
    #[cfg(test)]
    let worker_supervisor = supervisor.clone();
    let mut worker = tokio::task::spawn_blocking(move || {
        let _supervision = supervision;
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
        let length = usize::try_from(range.end.saturating_sub(range.start)).map_err(|error| {
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
    });
    tokio::select! {
        result = &mut worker => result.map_err(ObjectStoreError::from)?,
        _ = supervisor.cancellation().cancelled() => {
            let _ = worker.await.map_err(ObjectStoreError::from)?;
            Err(cancelled_object_store())
        }
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

#[derive(Debug)]
pub(super) struct ManifestParquetTable {
    schema: SchemaRef,
    object_store_url: ObjectStoreUrl,
    files: FileGroup,
    _retained_metadata: Arc<RetainedPinnedMetadata>,
}

impl ManifestParquetTable {
    fn register(
        context: &SessionContext,
        table_name: &str,
        schema: SchemaRef,
        dataset: &PinnedDataset,
        verified: Vec<VerifiedPinnedObject>,
        resources: PinnedRegistrationResources,
    ) -> Result<(), QueryError> {
        let PinnedRegistrationResources {
            supervisor,
            retained_metadata,
            max_memory_bytes,
        } = resources;
        let object_store_url = ObjectStoreUrl::parse(PINNED_STORE_URL)?;
        let memory_pool = Arc::clone(&context.runtime_env().memory_pool);
        context.register_object_store(
            object_store_url.as_ref(),
            Arc::new(PinnedReadOnlyObjectStore::new_reserved(
                verified,
                memory_pool,
                supervisor,
                Arc::clone(&retained_metadata),
            )?),
        );
        let mut files = Vec::new();
        files
            .try_reserve_exact(dataset.objects().len())
            .map_err(|_| QueryError::MemoryLimitExceeded {
                limit: max_memory_bytes,
            })?;
        files.extend(dataset.objects().iter().map(|pinned| {
            PartitionedFile::new(pinned.relative_reference(), pinned.object().size_bytes())
        }));
        context.register_table(
            table_name,
            Arc::new(Self {
                schema,
                object_store_url,
                files: FileGroup::new(files),
                _retained_metadata: retained_metadata,
            }),
        )?;
        Ok(())
    }
}

#[async_trait]
impl TableProvider for ManifestParquetTable {
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
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let source = Arc::new(ParquetSource::new(Arc::clone(&self.schema)));
        let config = FileScanConfigBuilder::new(self.object_store_url.clone(), source)
            .with_file_group(self.files.clone())
            .with_projection_indices(projection.cloned())?
            .with_limit(limit)
            .build();
        Ok(DataSourceExec::from_data_source(config))
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::ParquetStoreError;

    type TestResult = Result<(), Box<dyn Error>>;

    #[tokio::test]
    async fn pinned_io_cancellation_is_joined_and_many_object_metadata_is_admitted() -> TestResult {
        assert!(matches!(
            map_capture_error(ParquetStoreError::Cancelled),
            QueryError::Cancelled
        ));

        let mut range_file = tempfile::tempfile()?;
        std::io::Write::write_all(&mut range_file, &[7_u8; 4096])?;
        let cancellation = CancellationToken::new();
        let supervisor = BlockingIoSupervisor::new(cancellation.clone());
        let mut barrier = supervisor.install_test_range_barrier()?;
        let range_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(8192));
        let read_supervisor = supervisor.clone();
        let read = tokio::spawn(read_exact_range(
            Arc::new(Mutex::new(range_file)),
            0..4096,
            range_pool,
            read_supervisor,
        ));
        barrier.wait_until_entered().await?;
        cancellation.cancel();
        barrier.release()?;
        assert!(read.await?.is_err());
        supervisor.cancel_and_drain().await;
        assert_eq!(supervisor.active(), 0);

        let seed = tempfile::tempfile()?;
        let verified = (0..64)
            .map(|index| {
                VerifiedPinnedObject::test_fixture(
                    format!("objects/sha256/aa/{index:064x}.parquet"),
                    seed.try_clone()?,
                    1,
                    format!("{index:064x}"),
                )
            })
            .collect::<Result<Vec<_>, std::io::Error>>()?;
        let required = pinned_registration_retained_bytes(&verified)?;
        let tight_limit = required.checked_sub(1).ok_or("nonzero retained charge")?;
        let tight_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(tight_limit));
        assert!(matches!(
            PinnedReadOnlyObjectStore::new(
                verified,
                tight_pool,
                BlockingIoSupervisor::new(CancellationToken::new()),
                u64::try_from(tight_limit)?,
            ),
            Err(QueryError::MemoryLimitExceeded { limit }) if limit == u64::try_from(tight_limit)?
        ));
        Ok(())
    }
}
