//! Query-scoped capture of verified, no-follow immutable file handles.

use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt};
use cap_std::fs::OpenOptions;
use chrono::{DateTime, Utc};
use datafusion::object_store::ObjectMeta;
use datafusion::object_store::path::Path as ObjectPath;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use tokio_util::sync::CancellationToken;

use super::{OBJECTS, ParquetObjectStore, ParquetStoreError, hash_file};
use crate::blocking_supervisor::{BlockingIoAdmissionError, BlockingIoSupervisor};
use crate::{PinnedDataset, QueryError};

#[derive(Debug)]
pub(crate) struct VerifiedPinnedObject {
    object_meta: ObjectMeta,
    file: Arc<Mutex<File>>,
    reader_metadata: ArrowReaderMetadata,
}

impl VerifiedPinnedObject {
    pub(crate) fn relative_reference(&self) -> &str {
        self.object_meta.location.as_ref()
    }

    pub(crate) const fn object_meta(&self) -> &ObjectMeta {
        &self.object_meta
    }

    pub(crate) fn file(&self) -> Arc<Mutex<File>> {
        Arc::clone(&self.file)
    }

    pub(crate) const fn reader_metadata(&self) -> &ArrowReaderMetadata {
        &self.reader_metadata
    }

    pub(crate) fn bind_reader_schema(&mut self, schema: SchemaRef) -> Result<(), QueryError> {
        self.reader_metadata = ArrowReaderMetadata::try_new(
            Arc::clone(self.reader_metadata.metadata()),
            ArrowReaderOptions::new().with_schema(schema),
        )
        .map_err(|_| QueryError::InvalidSource)?;
        Ok(())
    }
}

struct PinnedCaptureObject {
    relative_reference: Box<str>,
    content_hash: [u8; 32],
    size_bytes: u64,
    row_count: u64,
}

struct PinnedCapturePlan {
    objects: Box<[PinnedCaptureObject]>,
}

impl PinnedCapturePlan {
    fn try_new(dataset: &PinnedDataset) -> Result<Self, QueryError> {
        let object_count = dataset.objects().len();
        if object_count == 0 {
            return Err(QueryError::InvalidSource);
        }
        let mut objects = Vec::new();
        objects
            .try_reserve_exact(object_count)
            .map_err(|_| QueryError::SizeOverflow)?;
        if objects.capacity() != object_count {
            return Err(QueryError::DependencyAllocationContract);
        }
        for pinned in dataset.objects() {
            objects.push(PinnedCaptureObject {
                relative_reference: Box::from(pinned.relative_reference()),
                content_hash: pinned.object().content_hash().bytes(),
                size_bytes: pinned.object().size_bytes(),
                row_count: pinned.object().row_count(),
            });
        }
        let allocation = objects.as_ptr();
        let objects = objects.into_boxed_slice();
        if objects.len() != object_count || objects.as_ptr() != allocation {
            return Err(QueryError::DependencyAllocationContract);
        }
        Ok(Self { objects })
    }
}

impl ParquetObjectStore {
    pub(crate) async fn capture_pinned_async(
        &self,
        dataset: &PinnedDataset,
        supervisor: &BlockingIoSupervisor,
    ) -> Result<Vec<VerifiedPinnedObject>, QueryError> {
        let plan = PinnedCapturePlan::try_new(dataset)?;
        let mut verified = Vec::new();
        verified
            .try_reserve_exact(plan.objects.len())
            .map_err(|_| QueryError::SizeOverflow)?;
        if verified.capacity() != plan.objects.len() {
            return Err(QueryError::DependencyAllocationContract);
        }
        let captured = async {
            let cancellation = supervisor.cancellation();
            let directory = self.directory.try_clone()?;
            let config = self.config;
            let permit = self.acquire_blocking_permit(cancellation).await?;
            let operation_cancellation = cancellation.child_token();
            let worker_cancellation = operation_cancellation.clone();
            let mut worker = supervisor
                .spawn_blocking(move || {
                    let _permit = permit;
                    Self::capture_pinned_files(
                        &directory,
                        config,
                        &plan,
                        verified,
                        &worker_cancellation,
                    )
                })
                .map_err(|error| match error {
                    BlockingIoAdmissionError::Cancelled => ParquetStoreError::Cancelled,
                    BlockingIoAdmissionError::Saturated => ParquetStoreError::BlockingTaskFailed,
                })?;
            tokio::select! {
                result = &mut worker => {
                    result.map_err(|_| ParquetStoreError::BlockingTaskFailed)?
                }
                _ = cancellation.cancelled() => {
                    operation_cancellation.cancel();
                    Err(ParquetStoreError::Cancelled)
                }
            }
        }
        .await;
        captured.map_err(capture_query_error)
    }

    fn capture_pinned_files(
        directory: &cap_std::fs::Dir,
        config: super::ObjectStoreConfig,
        plan: &PinnedCapturePlan,
        mut verified: Vec<VerifiedPinnedObject>,
        cancellation: &CancellationToken,
    ) -> Result<Vec<VerifiedPinnedObject>, ParquetStoreError> {
        for pinned in &plan.objects {
            if cancellation.is_cancelled() {
                return Err(ParquetStoreError::Cancelled);
            }
            if !reference_matches_digest(&pinned.relative_reference, pinned.content_hash) {
                return Err(ParquetStoreError::ObjectMetadataMismatch);
            }
            let mut options = OpenOptions::new();
            options.read(true).follow(FollowSymlinks::No);
            let mut file = directory
                .open_with(pinned.relative_reference.as_ref(), &options)?
                .into_std();
            let metadata = file.metadata()?;
            if metadata.len() != pinned.size_bytes
                || metadata.len() > config.max_staging_bytes
                || hash_file(&mut file, Some(cancellation))?.bytes() != pinned.content_hash
            {
                return Err(ParquetStoreError::ObjectMetadataMismatch);
            }
            file.seek(SeekFrom::Start(0))?;
            let reader_metadata =
                ArrowReaderMetadata::load(&file.try_clone()?, ArrowReaderOptions::default())?;
            let metadata_rows =
                u64::try_from(reader_metadata.metadata().file_metadata().num_rows())
                    .map_err(|_| ParquetStoreError::ObjectMetadataMismatch)?;
            if metadata_rows != pinned.row_count {
                return Err(ParquetStoreError::ObjectMetadataMismatch);
            }
            let location = ObjectPath::parse(&pinned.relative_reference)
                .map_err(|_| ParquetStoreError::ObjectMetadataMismatch)?;
            if location.as_ref() != pinned.relative_reference.as_ref() {
                return Err(ParquetStoreError::ObjectMetadataMismatch);
            }
            verified.push(VerifiedPinnedObject {
                object_meta: ObjectMeta {
                    location,
                    last_modified: DateTime::<Utc>::from(metadata.modified()?),
                    size: metadata.len(),
                    e_tag: None,
                    version: None,
                },
                file: Arc::new(Mutex::new(file)),
                reader_metadata,
            });
        }
        Ok(verified)
    }
}

fn capture_query_error(error: ParquetStoreError) -> QueryError {
    if matches!(error, ParquetStoreError::Cancelled) {
        QueryError::Cancelled
    } else {
        QueryError::Artifact(error)
    }
}

fn reference_matches_digest(reference: &str, digest: [u8; 32]) -> bool {
    let Some(relative) = reference
        .strip_prefix(OBJECTS)
        .and_then(|value| value.strip_prefix('/'))
    else {
        return false;
    };
    let Some((shard, filename)) = relative.split_once('/') else {
        return false;
    };
    let Some(encoded) = filename.strip_suffix(".parquet") else {
        return false;
    };
    if shard.len() != 2 || encoded.len() != digest.len() * 2 {
        return false;
    }
    let shard = shard.as_bytes();
    let encoded = encoded.as_bytes();
    if shard[0] != hex_digit(digest[0] >> 4) || shard[1] != hex_digit(digest[0] & 0x0f) {
        return false;
    }
    digest.iter().enumerate().all(|(index, byte)| {
        encoded[index * 2] == hex_digit(byte >> 4)
            && encoded[index * 2 + 1] == hex_digit(byte & 0x0f)
    })
}

const fn hex_digit(nibble: u8) -> u8 {
    if nibble < 10 {
        b'0' + nibble
    } else if nibble < 16 {
        b'a' + (nibble - 10)
    } else {
        u8::MAX
    }
}
