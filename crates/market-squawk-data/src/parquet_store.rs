//! Capability-confined, immutable, content-addressed Parquet publication.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use cap_std::ambient_authority;
use cap_std::fs::{Dir, OpenOptions};
use market_squawk_domain::Timestamp;
use market_squawk_platform::ArtifactRoot;
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

use crate::manifest::{PinnedDataset, Sha256Digest};
use crate::publication_coordinator::{PublicationCoordinator, PublicationLease};
use crate::schema::{decode_hex, encode_hex};

const OBJECTS: &str = "objects/sha256";
const STAGING: &str = "staging/parquet";
const QUARANTINE: &str = "quarantine/parquet";
const MAX_SCAN_OBJECTS: usize = 100_000;
const MAX_BLOCKING_TASKS: usize = 4;

#[path = "parquet_store/pinned.rs"]
mod pinned;
#[path = "parquet_store/recovery.rs"]
mod recovery;

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
    publication: PublicationCoordinator,
}

impl ParquetObjectStore {
    /// Opens the already controlled artifact root and prepares fixed internal namespaces.
    pub fn open(root: ArtifactRoot, config: ObjectStoreConfig) -> Result<Self, ParquetStoreError> {
        let directory = Dir::open_ambient_dir(root.root(), ambient_authority())?;
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
            publication: PublicationCoordinator::default(),
        })
    }

    /// Writes, closes, fsyncs, hashes, and no-replace publishes one Parquet object.
    pub async fn publish(
        &self,
        batch: &RecordBatch,
        cancellation: &CancellationToken,
    ) -> Result<PublishedObject, ParquetStoreError> {
        let lease = self.begin_publication(cancellation).await?;
        self.publish_under_lease(batch, cancellation, &lease).await
    }

    /// Acquires exclusive final-object publication ownership with cancellation.
    pub async fn begin_publication(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<PublicationLease, ParquetStoreError> {
        self.publication
            .acquire(cancellation)
            .await
            .ok_or(ParquetStoreError::Cancelled)
    }

    /// Publishes while the caller retains exclusion through its durable reference commit.
    pub async fn publish_under_lease(
        &self,
        batch: &RecordBatch,
        cancellation: &CancellationToken,
        lease: &PublicationLease,
    ) -> Result<PublishedObject, ParquetStoreError> {
        if !self.publication.owns(lease) {
            return Err(ParquetStoreError::InvalidPublicationLease);
        }
        let store = Self {
            root: self.root.clone(),
            directory: self.directory.try_clone()?,
            config: self.config,
            blocking_tasks: Arc::clone(&self.blocking_tasks),
            publication: self.publication.clone(),
        };
        let batch = batch.clone();
        let permit = self.acquire_blocking_permit(cancellation).await?;
        let operation_cancellation = cancellation.child_token();
        let worker_cancellation = operation_cancellation.clone();
        let mut worker = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            store.publish_blocking(&batch, &worker_cancellation)
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
    ) -> Result<PublishedObject, ParquetStoreError> {
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
        let _cleanup = StagingCleanup {
            directory: &self.directory,
            reference: &stage,
        };
        let properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
            .set_max_row_group_row_count(Some(self.config.max_row_group_rows))
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_write_page_header_statistics(false)
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
            offset = offset
                .checked_add(length)
                .ok_or(ParquetStoreError::SizeOverflow)?;
        }
        if cancellation.is_cancelled() {
            let _ignored = self.directory.remove_file(&stage);
            return Err(ParquetStoreError::Cancelled);
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
        let digest = encode_hex(content_hash.bytes());
        let destination = format!("{OBJECTS}/{}/{}.parquet", &digest[..2], digest);
        self.directory
            .create_dir_all(format!("{OBJECTS}/{}", &digest[..2]))?;
        match self.publish_no_replace(&stage, &destination) {
            Ok(()) => {
                sync_directory(&self.directory, &format!("{OBJECTS}/{}", &digest[..2]))?;
                sync_directory(&self.directory, OBJECTS)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                self.directory.remove_file(&stage)?;
                let existing =
                    object_from_reference(&self.directory, &destination, batch.num_rows())?;
                if existing.content_hash != content_hash || existing.size_bytes != size_bytes {
                    return Err(ParquetStoreError::ContentAddressConflict);
                }
                return Ok(existing);
            }
            Err(error) => {
                let _ignored = self.directory.remove_file(&stage);
                return Err(error.into());
            }
        }
        object_from_reference(&self.directory, &destination, batch.num_rows())
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
        let mut batches = Vec::new();
        let mut generation_rows = 0_u64;
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
                batches.push(RecordBatch::try_new(
                    Arc::clone(&schema),
                    batch.columns().to_vec(),
                )?);
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
            publication: self.publication.clone(),
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
}

impl Drop for StagingCleanup<'_> {
    fn drop(&mut self) {
        let _ignored = self.directory.remove_file(self.reference);
    }
}

/// Immutable object storage failure.
#[derive(Debug, Error)]
pub enum ParquetStoreError {
    /// Writer limits are zero or excessive.
    #[error("Parquet object-store configuration is invalid")]
    InvalidConfiguration,
    /// The operation was cancelled before publication.
    #[error("Parquet publication was cancelled")]
    Cancelled,
    /// A lease belongs to a different publication/recovery coordinator.
    #[error("Parquet publication lease does not own this object store")]
    InvalidPublicationLease,
    /// A batch or object exceeds the configured bounded staging area.
    #[error("Parquet staging byte limit exceeded")]
    StagingLimitExceeded,
    /// A byte or row count could not be represented.
    #[error("Parquet size conversion overflow")]
    SizeOverflow,
    /// A bounded blocking worker or its admission semaphore failed.
    #[error("Parquet blocking worker failed")]
    BlockingTaskFailed,
    /// An existing content address contains different bytes.
    #[error("content-addressed Parquet object conflicts with existing bytes")]
    ContentAddressConflict,
    /// Catalog metadata, the content-addressed reference, and exact object bytes disagree.
    #[error("manifest-pinned Parquet object metadata does not match its bytes")]
    ObjectMetadataMismatch,
    /// Recovery enumeration exceeded its defensive ceiling.
    #[error("Parquet recovery object scan exceeded its ceiling")]
    RecoveryScanLimit,
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
    let mut file = directory.open(reference)?.into_std();
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
