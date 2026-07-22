//! Capability-confined, immutable, content-addressed Parquet publication.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt as _};
use cap_std::fs::{Dir, OpenOptions};
use market_squawk_domain::Timestamp;
use market_squawk_platform::{ArtifactRoot, PathError};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, ZstdLevel};
use parquet::errors::ParquetError;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::arrow_convert::DatasetArrowBatch;
use crate::blocking_supervisor::{BlockingIoAdmissionError, BlockingIoSupervisor};
use crate::manifest::{PinnedDataset, Sha256Digest};
use crate::publication_coordinator::PublicationLease;
use crate::query::QueryArtifactMemoryLease;
use crate::schema::{decode_hex, encode_hex};

const OBJECTS: &str = "objects/sha256";
const STAGING: &str = "staging/parquet";
const QUARANTINE: &str = "quarantine/parquet";
pub(crate) const MAX_SCAN_OBJECTS: usize = 100_000;
const MAX_BLOCKING_TASKS: usize = 4;
const QUERY_WRITER_FIXED_RECEIPT: usize = 128 * 1024;
const QUERY_WRITER_INPUT_EXPANSION: usize = 3;
const QUERY_WRITER_SCHEMA_EXPANSION: usize = 16;
const QUERY_WRITER_COLUMN_METADATA: usize = 8 * 1024;
const QUERY_WRITER_ROW_GROUP_METADATA: usize = 4 * 1024;
const QUERY_WRITER_PAGE_BYTES: usize = 64 * 1024;
#[path = "parquet_store/authority.rs"]
mod authority;
#[path = "parquet_store/pinned.rs"]
mod pinned;
#[path = "parquet_store/recovery.rs"]
mod recovery;
#[cfg(test)]
#[path = "parquet_store/test_support.rs"]
pub(crate) mod test_support;

use authority::RootAuthority;
#[cfg(test)]
use authority::acquire_root_authority;
pub(crate) use authority::{
    ActivatedRootAuthority, ArtifactRootIdentity, PreparedRootAuthority,
    RootBindingCheckpointInternal, VerifiedLegacyRootAuthority, VerifiedRestoreControlSubset,
};
pub(crate) use pinned::VerifiedPinnedObject;
pub use recovery::OrphanRecoveryReport;

/// Fixed resource policy for local Parquet publication.
#[derive(Clone, Copy, Debug)]
pub struct ObjectStoreConfig {
    max_staging_bytes: u64,
    max_row_group_rows: usize,
    orphan_grace: Duration,
}

impl ObjectStoreConfig {
    /// Constructs bounded writer and recovery limits.
    pub fn try_new(
        max_staging_bytes: u64,
        max_row_group_rows: usize,
        orphan_grace: Duration,
    ) -> Result<Self, ParquetStoreError> {
        if max_staging_bytes == 0
            || max_staging_bytes > 1024 * 1024 * 1024
            || max_row_group_rows == 0
            || max_row_group_rows > 1_000_000
            || orphan_grace.is_zero()
            || orphan_grace > Duration::from_secs(31 * 24 * 60 * 60)
        {
            return Err(ParquetStoreError::InvalidConfiguration);
        }
        Ok(Self {
            max_staging_bytes,
            max_row_group_rows,
            orphan_grace,
        })
    }
}

/// Exact immutable object publication receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishedObject {
    relative_reference: String,
    content_hash: Sha256Digest,
    size_bytes: u64,
    row_count: u64,
    created_at: Timestamp,
}

/// Exact encoded object retained only in the controlled staging namespace.
#[derive(Debug)]
pub(crate) struct StagedObject {
    cleanup: OwnedStagingCleanup,
    content_hash: Sha256Digest,
    size_bytes: u64,
    row_count: u64,
    created_at: Timestamp,
}

impl StagedObject {
    pub(crate) const fn content_hash(&self) -> Sha256Digest {
        self.content_hash
    }

    pub(crate) const fn created_at(&self) -> Timestamp {
        self.created_at
    }
}

#[derive(Debug)]
struct OwnedStagingCleanup {
    directory: Dir,
    reference: Option<String>,
}

impl OwnedStagingCleanup {
    fn reference(&self) -> Result<&str, ParquetStoreError> {
        self.reference
            .as_deref()
            .ok_or(ParquetStoreError::InvalidStagedObject)
    }

    fn remove(&mut self) -> Result<(), ParquetStoreError> {
        let reference = self
            .reference
            .take()
            .ok_or(ParquetStoreError::InvalidStagedObject)?;
        self.directory.remove_file(reference)?;
        Ok(())
    }

    fn disarm(&mut self) -> Result<(), ParquetStoreError> {
        self.reference
            .take()
            .map(|_| ())
            .ok_or(ParquetStoreError::InvalidStagedObject)
    }
}

impl Drop for OwnedStagingCleanup {
    fn drop(&mut self) {
        if let Some(reference) = self.reference.take() {
            let _ignored = self.directory.remove_file(reference);
        }
    }
}

/// Pre-construction receipt for the bounded uncompressed query-artifact writer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct QueryArtifactWriterAdmission {
    active_writer_bytes: usize,
    metadata_bytes: usize,
    row_groups: usize,
    total_bytes: usize,
}

impl QueryArtifactWriterAdmission {
    pub(crate) const fn bytes(self) -> usize {
        self.total_bytes
    }
}

impl PublishedObject {
    /// Returns the portable path below the controlled artifact root.
    pub fn relative_reference(&self) -> &str {
        &self.relative_reference
    }

    /// Returns exact Parquet bytes identity.
    pub const fn content_hash(&self) -> Sha256Digest {
        self.content_hash
    }

    /// Returns exact object size.
    pub const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    /// Returns object rows.
    pub const fn row_count(&self) -> u64 {
        self.row_count
    }

    /// Returns publication time.
    pub const fn created_at(&self) -> Timestamp {
        self.created_at
    }
}

/// Retained directory capability for immutable Parquet objects.
#[derive(Debug)]
pub struct ParquetObjectStore {
    root: ArtifactRoot,
    directory: Dir,
    config: ObjectStoreConfig,
    blocking_tasks: Arc<Semaphore>,
    authority: Arc<RootAuthority>,
}

impl ParquetObjectStore {
    /// Revalidates and clones the exact artifact-root capability owned by this store.
    pub(crate) fn try_clone_artifact_root(&self) -> Result<ArtifactRoot, ParquetStoreError> {
        let retained = self
            .root
            .try_clone_directory()
            .map_err(map_artifact_root_clone_error)?;
        drop(retained);
        Ok(self.root.clone())
    }

    pub(crate) fn acquire_prepared_root_authority(
        root: ArtifactRoot,
        create_lock: bool,
    ) -> Result<PreparedRootAuthority, ParquetStoreError> {
        authority::acquire_prepared_root_authority(root, create_lock)
    }

    pub(crate) fn restore_root_endpoint(
        root: &ArtifactRoot,
    ) -> Result<crate::authority_transition::RootEndpointIdentity, ParquetStoreError> {
        authority::restore_root_endpoint(root)
    }

    pub(crate) fn validate_restore_control_subset(
        directory: &Dir,
        prepared: &crate::authority_transition::PreparedAuthorityTransition,
        catalog_bound: bool,
    ) -> Result<VerifiedRestoreControlSubset, ParquetStoreError> {
        authority::validate_restore_control_subset(directory, prepared, catalog_bound)
    }

    pub(crate) fn from_activated_root(
        activated: ActivatedRootAuthority,
        config: ObjectStoreConfig,
    ) -> Result<Self, ParquetStoreError> {
        for path in [OBJECTS, STAGING, QUARANTINE] {
            activated.directory.create_dir_all(path)?;
        }
        for path in [
            OBJECTS,
            "objects",
            STAGING,
            "staging",
            QUARANTINE,
            "quarantine",
        ] {
            sync_directory(&activated.directory, path)?;
        }
        sync_directory(&activated.directory, ".")?;
        Ok(Self {
            root: activated.root,
            directory: activated.directory,
            config,
            blocking_tasks: Arc::new(Semaphore::new(MAX_BLOCKING_TASKS)),
            authority: Arc::new(activated.authority),
        })
    }

    /// Opens the already controlled artifact root and prepares fixed internal namespaces.
    #[cfg(test)]
    pub(crate) fn open(
        root: ArtifactRoot,
        config: ObjectStoreConfig,
        catalog_binding: [u8; 32],
        catalog_root_identity: Option<[u8; 32]>,
    ) -> Result<Self, ParquetStoreError> {
        Self::open_inner(root, config, catalog_binding, catalog_root_identity, |_| {
            Ok(())
        })
    }

    #[cfg(test)]
    fn open_inner<F>(
        root: ArtifactRoot,
        config: ObjectStoreConfig,
        catalog_binding: [u8; 32],
        catalog_root_identity: Option<[u8; 32]>,
        mut checkpoint: F,
    ) -> Result<Self, ParquetStoreError>
    where
        F: FnMut(RootBindingCheckpointInternal) -> Result<(), ParquetStoreError>,
    {
        let directory = root
            .try_clone_directory()
            .map_err(map_artifact_root_clone_error)?;
        let authority = Arc::new(acquire_root_authority(
            &directory,
            &root,
            catalog_binding,
            catalog_root_identity,
            &mut checkpoint,
        )?);
        for path in [OBJECTS, STAGING, QUARANTINE] {
            directory.create_dir_all(path)?;
        }
        for path in [
            OBJECTS,
            "objects",
            STAGING,
            "staging",
            QUARANTINE,
            "quarantine",
        ] {
            sync_directory(&directory, path)?;
        }
        sync_directory(&directory, ".")?;
        Ok(Self {
            root,
            directory,
            config,
            blocking_tasks: Arc::new(Semaphore::new(MAX_BLOCKING_TASKS)),
            authority,
        })
    }

    pub(crate) fn authority_identity(&self) -> &ArtifactRootIdentity {
        &self.authority.identity
    }

    pub(crate) fn stable_root_identity(&self) -> [u8; 32] {
        self.authority.identity.stable_root
    }

    /// Writes, closes, fsyncs, hashes, and no-replace publishes one Parquet object.
    #[cfg(test)]
    pub(crate) async fn publish(
        &self,
        batch: &RecordBatch,
        cancellation: &CancellationToken,
    ) -> Result<PublishedObject, ParquetStoreError> {
        let lease = self.begin_publication(cancellation).await?;
        self.publish_under_lease(batch, cancellation, &lease).await
    }

    /// Acquires exclusive final-object publication ownership with cancellation.
    pub(crate) async fn begin_publication(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<PublicationLease, ParquetStoreError> {
        self.authority
            .publication
            .acquire(cancellation)
            .await
            .ok_or(ParquetStoreError::Cancelled)
    }

    /// Pre-admits every variable allocation of the query-only uncompressed writer path.
    pub(crate) fn query_artifact_writer_admission(
        &self,
        batch: &RecordBatch,
    ) -> Result<QueryArtifactWriterAdmission, ParquetStoreError> {
        let batch_bytes = batch
            .get_array_memory_size()
            .checked_add(
                batch
                    .num_columns()
                    .checked_mul(std::mem::size_of::<arrow::array::ArrayRef>())
                    .ok_or(ParquetStoreError::SizeOverflow)?,
            )
            .and_then(|value| value.checked_add(std::mem::size_of::<RecordBatch>()))
            .ok_or(ParquetStoreError::SizeOverflow)?;
        let schema_bytes = batch
            .schema()
            .fields()
            .iter()
            .try_fold(0_usize, |total, field| {
                total
                    .checked_add(field.size())
                    .ok_or(ParquetStoreError::SizeOverflow)
            })?;
        let row_groups = batch
            .num_rows()
            .checked_add(self.config.max_row_group_rows - 1)
            .ok_or(ParquetStoreError::SizeOverflow)?
            / self.config.max_row_group_rows;
        let metadata_units = row_groups
            .checked_mul(batch.num_columns())
            .ok_or(ParquetStoreError::SizeOverflow)?;
        let metadata_bytes = metadata_units
            .checked_mul(QUERY_WRITER_COLUMN_METADATA)
            .and_then(|value| {
                row_groups
                    .checked_mul(QUERY_WRITER_ROW_GROUP_METADATA)
                    .and_then(|rows| value.checked_add(rows))
            })
            .and_then(|value| {
                schema_bytes
                    .checked_mul(QUERY_WRITER_SCHEMA_EXPANSION)
                    .and_then(|schema| value.checked_add(schema))
            })
            .ok_or(ParquetStoreError::SizeOverflow)?;
        let active_writer_bytes = batch_bytes
            .checked_mul(QUERY_WRITER_INPUT_EXPANSION)
            .and_then(|value| value.checked_add(QUERY_WRITER_FIXED_RECEIPT))
            .and_then(|value| {
                batch
                    .num_columns()
                    .checked_mul(QUERY_WRITER_PAGE_BYTES)
                    .and_then(|pages| value.checked_add(pages))
            })
            .ok_or(ParquetStoreError::SizeOverflow)?;
        let total_bytes = active_writer_bytes
            .checked_add(metadata_bytes)
            .ok_or(ParquetStoreError::SizeOverflow)?;
        if u64::try_from(total_bytes).map_err(|_| ParquetStoreError::SizeOverflow)?
            > self.config.max_staging_bytes
        {
            return Err(ParquetStoreError::StagingLimitExceeded);
        }
        Ok(QueryArtifactWriterAdmission {
            active_writer_bytes,
            metadata_bytes,
            row_groups,
            total_bytes,
        })
    }

    /// Publishes while the caller retains exclusion through its durable reference commit.
    #[cfg(test)]
    pub(crate) async fn publish_under_lease(
        &self,
        batch: &RecordBatch,
        cancellation: &CancellationToken,
        lease: &PublicationLease,
    ) -> Result<PublishedObject, ParquetStoreError> {
        self.publish_under_lease_inner(batch, cancellation, lease)
            .await
    }

    /// Publishes only a batch that retained and validated one registered dataset schema identity.
    pub(crate) async fn publish_dataset_under_lease(
        &self,
        batch: &DatasetArrowBatch,
        cancellation: &CancellationToken,
        lease: &PublicationLease,
    ) -> Result<PublishedObject, ParquetStoreError> {
        self.publish_under_lease_inner(batch.record_batch(), cancellation, lease)
            .await
    }

    /// Encodes and hashes a registered dataset while retaining it outside the final namespace.
    pub(crate) async fn stage_dataset_under_lease(
        &self,
        batch: &DatasetArrowBatch,
        cancellation: &CancellationToken,
        lease: &PublicationLease,
    ) -> Result<StagedObject, ParquetStoreError> {
        if !self.authority.publication.owns(lease) {
            return Err(ParquetStoreError::InvalidPublicationLease);
        }
        let store = Self {
            root: self.root.clone(),
            directory: self.directory.try_clone()?,
            config: self.config,
            blocking_tasks: Arc::clone(&self.blocking_tasks),
            authority: Arc::clone(&self.authority),
        };
        let batch = batch.record_batch().clone();
        let permit = self.acquire_blocking_permit(cancellation).await?;
        let operation_cancellation = cancellation.child_token();
        let worker_cancellation = operation_cancellation.clone();
        let mut worker = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            store.stage_blocking(&batch, &worker_cancellation, None)
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

    /// Atomically moves an exact staged object into the immutable content-addressed namespace.
    pub(crate) fn finalize_staged_under_lease(
        &self,
        staged: StagedObject,
        lease: &PublicationLease,
    ) -> Result<PublishedObject, ParquetStoreError> {
        if !self.authority.publication.owns(lease) {
            return Err(ParquetStoreError::InvalidPublicationLease);
        }
        self.finalize_staged(staged)
    }

    /// Publishes a query artifact only after a checked uncompressed-writer admission.
    #[allow(
        clippy::too_many_arguments,
        reason = "query publication keeps every independently owned capability explicit"
    )]
    pub(crate) async fn publish_query_artifact_under_lease(
        &self,
        batch: RecordBatch,
        cancellation: &CancellationToken,
        lease: &PublicationLease,
        admission: QueryArtifactWriterAdmission,
        memory_lease: QueryArtifactMemoryLease,
        supervisor: &BlockingIoSupervisor,
        #[cfg(test)] writer_barrier: Option<crate::ingest::QueryArtifactWriterWorkerBarrier>,
    ) -> Result<PublishedObject, ParquetStoreError> {
        if !self.authority.publication.owns(lease)
            || self.query_artifact_writer_admission(&batch)? != admission
        {
            return Err(ParquetStoreError::InvalidPublicationLease);
        }
        let store = Self {
            root: self.root.clone(),
            directory: self.directory.try_clone()?,
            config: self.config,
            blocking_tasks: Arc::clone(&self.blocking_tasks),
            authority: Arc::clone(&self.authority),
        };
        let permit = self.acquire_blocking_permit(cancellation).await?;
        let worker_cancellation = supervisor.cancellation().clone();
        let mut worker = supervisor
            .spawn_blocking(move || {
                let _permit = permit;
                let _memory_lease = memory_lease;
                #[cfg(test)]
                if let Some(barrier) = writer_barrier {
                    barrier.wait();
                }
                store.publish_blocking(&batch, &worker_cancellation, Some(admission))
            })
            .map_err(|error| match error {
                BlockingIoAdmissionError::Cancelled => ParquetStoreError::Cancelled,
                BlockingIoAdmissionError::Saturated => ParquetStoreError::BlockingTaskLimitExceeded,
                BlockingIoAdmissionError::ReaperUnavailable => {
                    ParquetStoreError::BlockingTaskFailed
                }
            })?;
        tokio::select! {
            result = &mut worker => {
                result.map_err(|_| ParquetStoreError::BlockingTaskFailed)?
            }
            _ = cancellation.cancelled() => Err(ParquetStoreError::Cancelled),
        }
    }

    async fn publish_under_lease_inner(
        &self,
        batch: &RecordBatch,
        cancellation: &CancellationToken,
        lease: &PublicationLease,
    ) -> Result<PublishedObject, ParquetStoreError> {
        if !self.authority.publication.owns(lease) {
            return Err(ParquetStoreError::InvalidPublicationLease);
        }
        let store = Self {
            root: self.root.clone(),
            directory: self.directory.try_clone()?,
            config: self.config,
            blocking_tasks: Arc::clone(&self.blocking_tasks),
            authority: Arc::clone(&self.authority),
        };
        let batch = batch.clone();
        let permit = self.acquire_blocking_permit(cancellation).await?;
        let operation_cancellation = cancellation.child_token();
        let worker_cancellation = operation_cancellation.clone();
        let mut worker = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            store.publish_blocking(&batch, &worker_cancellation, None)
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

    fn publish_blocking(
        &self,
        batch: &RecordBatch,
        cancellation: &CancellationToken,
        query_admission: Option<QueryArtifactWriterAdmission>,
    ) -> Result<PublishedObject, ParquetStoreError> {
        let staged = self.stage_blocking(batch, cancellation, query_admission)?;
        self.finalize_staged(staged)
    }

    fn stage_blocking(
        &self,
        batch: &RecordBatch,
        cancellation: &CancellationToken,
        query_admission: Option<QueryArtifactWriterAdmission>,
    ) -> Result<StagedObject, ParquetStoreError> {
        if cancellation.is_cancelled() {
            return Err(ParquetStoreError::Cancelled);
        }
        let estimated = u64::try_from(batch.get_array_memory_size())
            .map_err(|_| ParquetStoreError::SizeOverflow)?;
        if estimated > self.config.max_staging_bytes {
            return Err(ParquetStoreError::StagingLimitExceeded);
        }
        let stage = format!("{STAGING}/{}.tmp", Uuid::new_v4());
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        configure_private_staging(&mut options);
        let staged = self.directory.open_with(&stage, &options)?.into_std();
        let mut cleanup = StagingCleanup {
            directory: &self.directory,
            reference: &stage,
            active: true,
        };
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(self.config.max_row_group_rows))
            .set_write_page_header_statistics(false);
        let properties = match query_admission {
            Some(admission) => properties
                .set_compression(Compression::UNCOMPRESSED)
                .set_dictionary_enabled(false)
                .set_statistics_enabled(EnabledStatistics::None)
                .set_data_page_size_limit(QUERY_WRITER_PAGE_BYTES)
                .set_write_batch_size(1024)
                .set_max_row_group_bytes(Some(admission.active_writer_bytes)),
            None => properties
                .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
                .set_statistics_enabled(EnabledStatistics::Chunk),
        }
        .build();
        let mut writer = ArrowWriter::try_new(staged, batch.schema(), Some(properties))?;
        let mut offset = 0_usize;
        while offset < batch.num_rows() {
            if cancellation.is_cancelled() {
                let _ignored = self.directory.remove_file(&stage);
                return Err(ParquetStoreError::Cancelled);
            }
            let length = self
                .config
                .max_row_group_rows
                .min(batch.num_rows() - offset);
            if let Err(error) = writer.write(&batch.slice(offset, length)) {
                let _ignored = self.directory.remove_file(&stage);
                return Err(error.into());
            }
            if query_admission.is_some_and(|admission| {
                writer.memory_size() > admission.active_writer_bytes
                    || writer.flushed_row_groups().len() > admission.row_groups
            }) {
                let _ignored = self.directory.remove_file(&stage);
                return Err(ParquetStoreError::StagingLimitExceeded);
            }
            offset = offset
                .checked_add(length)
                .ok_or(ParquetStoreError::SizeOverflow)?;
        }
        if cancellation.is_cancelled() {
            let _ignored = self.directory.remove_file(&stage);
            return Err(ParquetStoreError::Cancelled);
        }
        if query_admission.is_some_and(|admission| {
            writer.memory_size() > admission.active_writer_bytes
                || writer.flushed_row_groups().len() > admission.row_groups
        }) {
            let _ignored = self.directory.remove_file(&stage);
            return Err(ParquetStoreError::StagingLimitExceeded);
        }
        let mut staged = match writer.into_inner() {
            Ok(staged) => staged,
            Err(error) => {
                let _ignored = self.directory.remove_file(&stage);
                return Err(error.into());
            }
        };
        staged.sync_all()?;
        let size_bytes = staged.metadata()?.len();
        if size_bytes == 0 || size_bytes > self.config.max_staging_bytes {
            drop(staged);
            let _ignored = self.directory.remove_file(&stage);
            return Err(ParquetStoreError::StagingLimitExceeded);
        }
        let content_hash = hash_file(&mut staged, Some(cancellation))?;
        drop(staged);
        if cancellation.is_cancelled() {
            self.directory.remove_file(&stage)?;
            return Err(ParquetStoreError::Cancelled);
        }
        let created_at = timestamp_from_system_time(
            self.directory
                .open(&stage)?
                .into_std()
                .metadata()?
                .modified()?,
        )?;
        let cleanup_directory = self.directory.try_clone()?;
        cleanup.disarm();
        drop(cleanup);
        let owned_cleanup = OwnedStagingCleanup {
            directory: cleanup_directory,
            reference: Some(stage),
        };
        Ok(StagedObject {
            cleanup: owned_cleanup,
            content_hash,
            size_bytes,
            row_count: u64::try_from(batch.num_rows())
                .map_err(|_| ParquetStoreError::SizeOverflow)?,
            created_at,
        })
    }

    fn finalize_staged(
        &self,
        mut staged: StagedObject,
    ) -> Result<PublishedObject, ParquetStoreError> {
        let digest = encode_hex(staged.content_hash.bytes());
        let destination = format!("{OBJECTS}/{}/{}.parquet", &digest[..2], digest);
        self.directory
            .create_dir_all(format!("{OBJECTS}/{}", &digest[..2]))?;
        match self.publish_no_replace(staged.cleanup.reference()?, &destination) {
            Ok(()) => {
                staged.cleanup.disarm()?;
                sync_directory(&self.directory, &format!("{OBJECTS}/{}", &digest[..2]))?;
                sync_directory(&self.directory, OBJECTS)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                staged.cleanup.remove()?;
                let existing = object_from_reference(
                    &self.directory,
                    &destination,
                    usize::try_from(staged.row_count)
                        .map_err(|_| ParquetStoreError::SizeOverflow)?,
                )?;
                if existing.content_hash != staged.content_hash
                    || existing.size_bytes != staged.size_bytes
                {
                    return Err(ParquetStoreError::ContentAddressConflict);
                }
                return Ok(existing);
            }
            Err(error) => return Err(error.into()),
        }
        let published = object_from_reference(
            &self.directory,
            &destination,
            usize::try_from(staged.row_count).map_err(|_| ParquetStoreError::SizeOverflow)?,
        )?;
        if published.content_hash != staged.content_hash
            || published.size_bytes != staged.size_bytes
            || published.created_at != staged.created_at
        {
            return Err(ParquetStoreError::ObjectMetadataMismatch);
        }
        Ok(published)
    }

    /// Re-hashes a published object through the retained root capability.
    pub fn verify(&self, object: &PublishedObject) -> Result<bool, ParquetStoreError> {
        let verified = object_from_reference(
            &self.directory,
            &object.relative_reference,
            usize::try_from(object.row_count).map_err(|_| ParquetStoreError::SizeOverflow)?,
        )?;
        Ok(
            verified.content_hash == object.content_hash
                && verified.size_bytes == object.size_bytes,
        )
    }

    /// Reads and verifies every object in one immutable generation through the retained root.
    ///
    /// The catalog reference, byte length, content digest, and row count must all agree before any
    /// batch crosses the storage boundary. Directory listings are never consulted.
    pub fn read_pinned(
        &self,
        dataset: &PinnedDataset,
        cancellation: &CancellationToken,
    ) -> Result<Vec<RecordBatch>, ParquetStoreError> {
        self.read_pinned_with_limits(dataset, u64::MAX, usize::MAX, cancellation)
    }

    fn read_pinned_with_limits(
        &self,
        dataset: &PinnedDataset,
        max_rows: u64,
        max_retained_bytes: usize,
        cancellation: &CancellationToken,
    ) -> Result<Vec<RecordBatch>, ParquetStoreError> {
        let mut batches = Vec::new();
        let mut generation_rows = 0_u64;
        let mut retained_bytes = 0_usize;
        for pinned in dataset.objects() {
            if cancellation.is_cancelled() {
                return Err(ParquetStoreError::Cancelled);
            }
            let object = pinned.object();
            if generation_rows
                .checked_add(object.row_count())
                .is_none_or(|rows| rows > max_rows)
            {
                return Err(ParquetStoreError::ReadLimitExceeded);
            }
            let digest = encode_hex(object.content_hash().bytes());
            let expected_reference = format!("{OBJECTS}/{}/{}.parquet", &digest[..2], digest);
            if pinned.relative_reference() != expected_reference {
                return Err(ParquetStoreError::ObjectMetadataMismatch);
            }
            self.root.resolve(pinned.relative_reference())?;
            let mut file = self.directory.open(pinned.relative_reference())?.into_std();
            let metadata = file.metadata()?;
            if metadata.len() != object.size_bytes()
                || metadata.len() > self.config.max_staging_bytes
                || hash_file(&mut file, Some(cancellation))? != object.content_hash()
            {
                return Err(ParquetStoreError::ObjectMetadataMismatch);
            }
            file.seek(SeekFrom::Start(0))?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
            let row_groups = builder.metadata().row_groups();
            let object_estimate = row_groups.iter().try_fold(0_usize, |total, row_group| {
                row_group.columns().iter().try_fold(total, |total, column| {
                    total
                        .checked_add(
                            usize::try_from(column.uncompressed_size())
                                .map_err(|_| ParquetStoreError::SizeOverflow)?,
                        )
                        .ok_or(ParquetStoreError::SizeOverflow)
                })
            })?;
            let structural = usize::try_from(object.row_count())
                .ok()
                .and_then(|rows| rows.checked_mul(builder.schema().fields().len()))
                .and_then(|cells| cells.checked_mul(std::mem::size_of::<u64>()))
                .ok_or(ParquetStoreError::SizeOverflow)?;
            let estimated_peak = retained_bytes
                .checked_add(object_estimate)
                .and_then(|bytes| bytes.checked_add(structural))
                .ok_or(ParquetStoreError::SizeOverflow)?;
            if estimated_peak > max_retained_bytes {
                return Err(ParquetStoreError::ReadLimitExceeded);
            }
            batches
                .try_reserve(row_groups.len())
                .map_err(|_| ParquetStoreError::ReadLimitExceeded)?;
            let schema = Arc::clone(builder.schema());
            let reader = builder
                .with_batch_size(self.config.max_row_group_rows)
                .build()?;
            let first_index = batches.len();
            for batch in reader {
                if cancellation.is_cancelled() {
                    return Err(ParquetStoreError::Cancelled);
                }
                let batch = batch?;
                retained_bytes = retained_bytes
                    .checked_add(batch.get_array_memory_size())
                    .ok_or(ParquetStoreError::SizeOverflow)?;
                if retained_bytes > max_retained_bytes {
                    return Err(ParquetStoreError::ReadLimitExceeded);
                }
                let mut columns = Vec::new();
                columns
                    .try_reserve_exact(batch.num_columns())
                    .map_err(|_| ParquetStoreError::ReadLimitExceeded)?;
                columns.extend(batch.columns().iter().cloned());
                batches.push(RecordBatch::try_new(Arc::clone(&schema), columns)?);
            }
            let object_rows = batches[first_index..]
                .iter()
                .try_fold(0_u64, |total, batch| {
                    total
                        .checked_add(
                            u64::try_from(batch.num_rows())
                                .map_err(|_| ParquetStoreError::SizeOverflow)?,
                        )
                        .ok_or(ParquetStoreError::SizeOverflow)
                })?;
            if object_rows != object.row_count() {
                return Err(ParquetStoreError::ObjectMetadataMismatch);
            }
            generation_rows = generation_rows
                .checked_add(object_rows)
                .ok_or(ParquetStoreError::SizeOverflow)?;
        }
        if batches.is_empty() || generation_rows != dataset.plan().row_count() {
            return Err(ParquetStoreError::ObjectMetadataMismatch);
        }
        Ok(batches)
    }

    /// Reads one immutable generation through bounded blocking-task admission.
    pub async fn read_pinned_async(
        &self,
        dataset: &PinnedDataset,
        cancellation: &CancellationToken,
    ) -> Result<Vec<RecordBatch>, ParquetStoreError> {
        let store = Self {
            root: self.root.clone(),
            directory: self.directory.try_clone()?,
            config: self.config,
            blocking_tasks: Arc::clone(&self.blocking_tasks),
            authority: Arc::clone(&self.authority),
        };
        let dataset = dataset.clone();
        let permit = self.acquire_blocking_permit(cancellation).await?;
        let operation_cancellation = cancellation.child_token();
        let worker_cancellation = operation_cancellation.clone();
        let mut worker = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            store.read_pinned(&dataset, &worker_cancellation)
        });
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

    /// Reads one immutable generation only after caller-selected row and Arrow-memory admission.
    pub(crate) async fn read_pinned_bounded_async(
        &self,
        dataset: &PinnedDataset,
        max_rows: usize,
        max_retained_bytes: usize,
        cancellation: &CancellationToken,
    ) -> Result<Vec<RecordBatch>, ParquetStoreError> {
        if max_rows == 0 || max_retained_bytes == 0 {
            return Err(ParquetStoreError::ReadLimitExceeded);
        }
        let store = Self {
            root: self.root.clone(),
            directory: self.directory.try_clone()?,
            config: self.config,
            blocking_tasks: Arc::clone(&self.blocking_tasks),
            authority: Arc::clone(&self.authority),
        };
        let dataset = dataset.clone();
        let permit = self.acquire_blocking_permit(cancellation).await?;
        let operation_cancellation = cancellation.child_token();
        let worker_cancellation = operation_cancellation.clone();
        let max_rows = u64::try_from(max_rows).map_err(|_| ParquetStoreError::SizeOverflow)?;
        let mut worker = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            store.read_pinned_with_limits(
                &dataset,
                max_rows,
                max_retained_bytes,
                &worker_cancellation,
            )
        });
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

    async fn acquire_blocking_permit(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<OwnedSemaphorePermit, ParquetStoreError> {
        tokio::select! {
            permit = Arc::clone(&self.blocking_tasks).acquire_owned() => {
                permit.map_err(|_| ParquetStoreError::BlockingTaskFailed)
            }
            _ = cancellation.cancelled() => Err(ParquetStoreError::Cancelled),
        }
    }

    #[cfg(unix)]
    fn publish_no_replace(&self, source: &str, destination: &str) -> std::io::Result<()> {
        rustix::fs::renameat_with(
            &self.directory,
            source,
            &self.directory,
            destination,
            rustix::fs::RenameFlags::NOREPLACE,
        )
        .map_err(std::io::Error::from)
    }

    #[cfg(windows)]
    fn publish_no_replace(&self, source: &str, destination: &str) -> std::io::Result<()> {
        self.root.resolve(source).map_err(std::io::Error::other)?;
        self.root
            .resolve(destination)
            .map_err(std::io::Error::other)?;
        let source = self.root.root().join(source);
        let destination = self.root.root().join(destination);
        // MoveFileExW WRITE_THROUGH is used without replacement or cross-volume copying. Windows
        // durability is requested, but atomic visibility is not claimed.
        atomicwrites::move_atomic(&source, &destination)?;
        self.root
            .resolve(
                destination
                    .strip_prefix(self.root.root())
                    .map_err(std::io::Error::other)?,
            )
            .map_err(std::io::Error::other)?;
        if self
            .directory
            .symlink_metadata(
                source
                    .strip_prefix(self.root.root())
                    .map_err(std::io::Error::other)?,
            )
            .is_ok()
            || self
                .directory
                .open(
                    destination
                        .strip_prefix(self.root.root())
                        .map_err(std::io::Error::other)?,
                )
                .is_err()
        {
            return Err(std::io::Error::other(
                "Windows publication requires recovery",
            ));
        }
        Ok(())
    }

    #[cfg(not(any(unix, windows)))]
    fn publish_no_replace(&self, _source: &str, _destination: &str) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "no-replace publication is unsupported",
        ))
    }
}

struct StagingCleanup<'a> {
    directory: &'a Dir,
    reference: &'a str,
    active: bool,
}

impl StagingCleanup<'_> {
    fn disarm(&mut self) {
        self.active = false;
    }
}

fn map_artifact_root_clone_error(error: PathError) -> ParquetStoreError {
    match error {
        PathError::Io { source, .. } => ParquetStoreError::Io(source),
        PathError::PreparedRootChanged => ParquetStoreError::RootCatalogMismatch,
        PathError::ReadOnly
        | PathError::ControlRootUnavailable
        | PathError::ArtifactRootUnavailable
        | PathError::CatalogLocationUnavailable
        | PathError::CatalogAlreadyLocked => ParquetStoreError::RootCatalogMismatch,
        PathError::CatalogRestoreConflict => ParquetStoreError::CatalogRestoreConflict,
        PathError::CatalogRestoreIndeterminate => ParquetStoreError::CatalogRestoreIndeterminate,
    }
}

impl Drop for StagingCleanup<'_> {
    fn drop(&mut self) {
        if self.active {
            let _ignored = self.directory.remove_file(self.reference);
        }
    }
}

/// Immutable object storage failure.
#[derive(Debug, Error)]
pub enum ParquetStoreError {
    /// Writer limits are zero or excessive.
    #[error("Parquet object-store configuration is invalid")]
    InvalidConfiguration,
    /// Another live service or process owns final-object publication and recovery for this root.
    #[error("analytical artifact root already has an active authority")]
    RootAuthorityAlreadyOwned,
    /// The artifact root is durably bound to a different catalog path identity.
    #[error("analytical artifact root belongs to a different catalog")]
    RootCatalogMismatch,
    /// A retained catalog restore target contains different immutable bytes.
    #[error("analytical catalog restore conflicts with retained destination state")]
    CatalogRestoreConflict,
    /// Catalog restore publication requires exact receipt-based retry before reuse.
    #[error("analytical catalog restore publication is indeterminate")]
    CatalogRestoreIndeterminate,
    /// The process-local artifact-root authority registry was poisoned.
    #[error("analytical artifact-root authority registry is unavailable")]
    RootAuthorityRegistryUnavailable,
    /// A test-only interruption stopped first binding at one durable boundary.
    #[cfg(test)]
    #[error("analytical artifact-root first-bind fault was injected")]
    FirstBindFaultInjected,
    /// The operation was cancelled before publication.
    #[error("Parquet publication was cancelled")]
    Cancelled,
    /// A lease belongs to a different publication/recovery coordinator.
    #[error("Parquet publication lease does not own this object store")]
    InvalidPublicationLease,
    /// A staged receipt was already consumed or does not retain its controlled temporary object.
    #[error("Parquet staged-object receipt is invalid")]
    InvalidStagedObject,
    /// A batch or object exceeds the configured bounded staging area.
    #[error("Parquet staging byte limit exceeded")]
    StagingLimitExceeded,
    /// A bounded reader would exceed its caller-selected row or retained-memory ceiling.
    #[error("Parquet reader resource limit exceeded")]
    ReadLimitExceeded,
    /// A byte or row count could not be represented.
    #[error("Parquet size conversion overflow")]
    SizeOverflow,
    /// A bounded blocking worker or its admission semaphore failed.
    #[error("Parquet blocking worker failed")]
    BlockingTaskFailed,
    /// Process-global admission for query blocking workers is saturated.
    #[error("query Parquet blocking-worker limit exceeded")]
    BlockingTaskLimitExceeded,
    /// An existing content address contains different bytes.
    #[error("content-addressed Parquet object conflicts with existing bytes")]
    ContentAddressConflict,
    /// Catalog metadata, the content-addressed reference, and exact object bytes disagree.
    #[error("manifest-pinned Parquet object metadata does not match its bytes")]
    ObjectMetadataMismatch,
    /// Recovery enumeration exceeded its defensive ceiling.
    #[error("Parquet recovery object scan exceeded its ceiling")]
    RecoveryScanLimit,
    /// Recovery exceeded its elapsed-time deadline.
    #[error("Parquet recovery deadline exceeded")]
    RecoveryDeadlineExceeded,
    /// Artifact reference validation failed.
    #[error("controlled artifact reference is invalid")]
    ArtifactPath(#[from] market_squawk_platform::ArtifactPathError),
    /// Local filesystem operation failed.
    #[error("Parquet object filesystem operation failed")]
    Io(#[from] std::io::Error),
    /// Parquet encoding failed.
    #[error("Parquet encoding failed")]
    Parquet(#[from] ParquetError),
    /// Arrow decoding failed while reading a pinned Parquet object.
    #[error("Parquet Arrow decoding failed")]
    Arrow(#[from] ArrowError),
}

fn object_from_reference(
    directory: &Dir,
    reference: &str,
    rows: usize,
) -> Result<PublishedObject, ParquetStoreError> {
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    let mut file = directory.open_with(reference, &options)?.into_std();
    let metadata = file.metadata()?;
    let size_bytes = metadata.len();
    let content_hash = hash_file(&mut file, None)?;
    Ok(PublishedObject {
        relative_reference: reference.to_owned(),
        content_hash,
        size_bytes,
        row_count: u64::try_from(rows).map_err(|_| ParquetStoreError::SizeOverflow)?,
        created_at: timestamp_from_system_time(metadata.modified()?)?,
    })
}

fn hash_file(
    file: &mut File,
    cancellation: Option<&CancellationToken>,
) -> Result<Sha256Digest, ParquetStoreError> {
    file.seek(SeekFrom::Start(0))?;
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        if cancellation.is_some_and(CancellationToken::is_cancelled) {
            return Err(ParquetStoreError::Cancelled);
        }
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hash.update(&buffer[..read]);
    }
    Ok(Sha256Digest::new(hash.finalize().into()))
}

fn timestamp_from_system_time(value: SystemTime) -> Result<Timestamp, ParquetStoreError> {
    let duration = value
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ParquetStoreError::SizeOverflow)?;
    let nanos = i64::try_from(duration.as_nanos()).map_err(|_| ParquetStoreError::SizeOverflow)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

#[cfg(unix)]
fn configure_private_staging(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;

    options.mode(0o600);
}

#[cfg(not(unix))]
fn configure_private_staging(_options: &mut OpenOptions) {}

#[cfg(unix)]
fn sync_directory(directory: &Dir, path: &str) -> std::io::Result<()> {
    use cap_std::fs::OpenOptionsExt as _;

    let target = directory.open_dir(path)?;
    let mut options = OpenOptions::new();
    options.read(true);
    options.custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC);
    target.open_with(".", &options)?.sync_all()
}

#[cfg(windows)]
fn sync_directory(_directory: &Dir, _path: &str) -> std::io::Result<()> {
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn sync_directory(_directory: &Dir, _path: &str) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "directory durability is unsupported",
    ))
}
