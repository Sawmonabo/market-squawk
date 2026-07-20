//! Query-scoped capture of verified, no-follow immutable file handles.

use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt};
use cap_std::fs::OpenOptions;
use tokio_util::sync::CancellationToken;

use super::{OBJECTS, ParquetObjectStore, ParquetStoreError, hash_file};
use crate::PinnedDataset;
use crate::blocking_supervisor::BlockingIoSupervisor;
use crate::schema::encode_hex;

#[derive(Debug)]
pub(crate) struct VerifiedPinnedObject {
    relative_reference: String,
    file: Arc<Mutex<File>>,
    size_bytes: u64,
    modified_at: SystemTime,
    etag: String,
}

impl VerifiedPinnedObject {
    pub(crate) fn relative_reference(&self) -> &str {
        &self.relative_reference
    }

    pub(crate) fn file(&self) -> Arc<Mutex<File>> {
        Arc::clone(&self.file)
    }

    pub(crate) const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub(crate) const fn modified_at(&self) -> SystemTime {
        self.modified_at
    }

    pub(crate) fn etag(&self) -> &str {
        &self.etag
    }

    #[cfg(test)]
    pub(crate) fn test_fixture(
        relative_reference: String,
        file: File,
        size_bytes: u64,
        etag: String,
    ) -> Result<Self, std::io::Error> {
        Ok(Self {
            relative_reference,
            file: Arc::new(Mutex::new(file)),
            size_bytes,
            modified_at: SystemTime::now(),
            etag,
        })
    }
}

impl ParquetObjectStore {
    pub(crate) async fn capture_pinned_async(
        &self,
        dataset: &PinnedDataset,
        supervisor: &BlockingIoSupervisor,
        retained_metadata: Arc<dyn Send + Sync>,
    ) -> Result<Vec<VerifiedPinnedObject>, ParquetStoreError> {
        let cancellation = supervisor.cancellation();
        let store = Self {
            root: self.root.clone(),
            directory: self.directory.try_clone()?,
            config: self.config,
            blocking_tasks: Arc::clone(&self.blocking_tasks),
            authority: Arc::clone(&self.authority),
        };
        let dataset = dataset.clone();
        let permit = self.acquire_blocking_permit(cancellation).await?;
        let supervision = supervisor.start().ok_or(ParquetStoreError::Cancelled)?;
        let operation_cancellation = cancellation.child_token();
        let worker_cancellation = operation_cancellation.clone();
        let mut worker = tokio::task::spawn_blocking(move || {
            let _supervision = supervision;
            let _retained_metadata = retained_metadata;
            let _permit = permit;
            store.capture_pinned_files(&dataset, &worker_cancellation)
        });
        tokio::select! {
            result = &mut worker => {
                result.map_err(|_| ParquetStoreError::BlockingTaskFailed)?
            }
            _ = cancellation.cancelled() => {
                operation_cancellation.cancel();
                worker.await.map_err(|_| ParquetStoreError::BlockingTaskFailed)??;
                Err(ParquetStoreError::Cancelled)
            }
        }
    }

    fn capture_pinned_files(
        &self,
        dataset: &PinnedDataset,
        cancellation: &CancellationToken,
    ) -> Result<Vec<VerifiedPinnedObject>, ParquetStoreError> {
        if dataset.objects().is_empty() {
            return Err(ParquetStoreError::ObjectMetadataMismatch);
        }
        let mut verified = Vec::new();
        verified
            .try_reserve_exact(dataset.objects().len())
            .map_err(|_| ParquetStoreError::SizeOverflow)?;
        for pinned in dataset.objects() {
            if cancellation.is_cancelled() {
                return Err(ParquetStoreError::Cancelled);
            }
            let object = pinned.object();
            let digest = encode_hex(object.content_hash().bytes());
            let expected_reference = format!("{OBJECTS}/{}/{}.parquet", &digest[..2], digest);
            if pinned.relative_reference() != expected_reference {
                return Err(ParquetStoreError::ObjectMetadataMismatch);
            }
            self.root.resolve(pinned.relative_reference())?;
            let mut options = OpenOptions::new();
            options.read(true).follow(FollowSymlinks::No);
            let mut file = self
                .directory
                .open_with(pinned.relative_reference(), &options)?
                .into_std();
            let metadata = file.metadata()?;
            if metadata.len() != object.size_bytes()
                || metadata.len() > self.config.max_staging_bytes
                || hash_file(&mut file, Some(cancellation))? != object.content_hash()
            {
                return Err(ParquetStoreError::ObjectMetadataMismatch);
            }
            file.seek(SeekFrom::Start(0))?;
            verified.push(VerifiedPinnedObject {
                relative_reference: pinned.relative_reference().to_owned(),
                file: Arc::new(Mutex::new(file)),
                size_bytes: metadata.len(),
                modified_at: metadata.modified()?,
                etag: digest,
            });
        }
        Ok(verified)
    }
}
