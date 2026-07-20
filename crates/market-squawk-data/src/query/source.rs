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
use tokio_util::sync::CancellationToken;

use super::budget::schema_retained_bytes;
use crate::parquet_store::VerifiedPinnedObject;
use crate::{ParquetObjectStore, PinnedDataset, QueryError};

const PINNED_STORE_URL: &str = "market-squawk://analytical";
const STORE_NAME: &str = "PinnedReadOnlyObjectStore";

#[derive(Debug, Error)]
#[error("pinned Parquet range exceeded the query memory pool")]
pub(super) struct PinnedRangeMemoryError;

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
        cancellation: &CancellationToken,
    ) -> Result<(), QueryError> {
        match self {
            Self::Pinned {
                dataset,
                store,
                schema,
            } => {
                let verified = store.capture_pinned_async(dataset, cancellation).await?;
                ManifestParquetTable::register(
                    context,
                    table_name,
                    Arc::clone(schema),
                    dataset,
                    verified,
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
}

impl PinnedReadOnlyObjectStore {
    fn new(
        files: Vec<VerifiedPinnedObject>,
        memory_pool: Arc<dyn MemoryPool>,
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
        })
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
) -> ObjectStoreResult<Bytes> {
    tokio::task::spawn_blocking(move || {
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
        let mut file = file.lock().map_err(|_| ObjectStoreError::Generic {
            store: STORE_NAME,
            source: "verified file mutex was poisoned".into(),
        })?;
        file.seek(SeekFrom::Start(range.start))
            .and_then(|_| file.read_exact(&mut bytes))
            .map_err(|source| ObjectStoreError::Generic {
                store: STORE_NAME,
                source: Box::new(source),
            })?;
        Ok(Bytes::from_owner(ReservedBytes {
            bytes,
            _reservation: reservation,
        }))
    })
    .await
    .map_err(ObjectStoreError::from)?
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
}

impl ManifestParquetTable {
    pub(super) fn register(
        context: &SessionContext,
        table_name: &str,
        schema: SchemaRef,
        dataset: &PinnedDataset,
        verified: Vec<VerifiedPinnedObject>,
    ) -> Result<(), QueryError> {
        let object_store_url = ObjectStoreUrl::parse(PINNED_STORE_URL)?;
        let memory_pool = Arc::clone(&context.runtime_env().memory_pool);
        context.register_object_store(
            object_store_url.as_ref(),
            Arc::new(PinnedReadOnlyObjectStore::new(verified, memory_pool)?),
        );
        let files = dataset
            .objects()
            .iter()
            .map(|pinned| {
                PartitionedFile::new(pinned.relative_reference(), pinned.object().size_bytes())
            })
            .collect();
        context.register_table(
            table_name,
            Arc::new(Self {
                schema,
                object_store_url,
                files: FileGroup::new(files),
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
