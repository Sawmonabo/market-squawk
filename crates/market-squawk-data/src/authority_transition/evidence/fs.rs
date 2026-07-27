//! Capability-relative immutable artifact verification.

use std::collections::BTreeMap;
use std::fmt;
use std::fs::File;
use std::io::{Read as _, Seek as _, SeekFrom};
use std::path::{Component, Path};

use cap_fs_ext::{DirExt as _, FollowSymlinks, OpenOptionsFollowExt as _};
use cap_std::fs::{Dir, Metadata, OpenOptions};
use market_squawk_platform::ArtifactRoot;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{CatalogEvidenceSnapshot, EvidenceError, MAX_PARQUET_METADATA_BYTES};
use crate::Sha256Digest;
use crate::authority_transition::ArtifactInventoryDigest;

const HASH_BUFFER_BYTES: usize = 64 * 1024;
const PARQUET_FOOTER_BYTES: u64 = 8;
const PARQUET_MAGIC: &[u8; 4] = b"PAR1";

mod materialize;

pub(crate) use materialize::MaterializedArtifactRoot;

pub(crate) struct VerifiedArtifactInventory {
    artifacts: Vec<VerifiedArtifact>,
    total_bytes: u64,
    digest: ArtifactInventoryDigest,
    source_directory_identity: FileIdentity,
}

impl VerifiedArtifactInventory {
    pub(crate) fn artifacts(&self) -> &[VerifiedArtifact] {
        &self.artifacts
    }

    pub(crate) const fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    pub(crate) const fn digest(&self) -> ArtifactInventoryDigest {
        self.digest
    }
}

impl fmt::Debug for VerifiedArtifactInventory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VerifiedArtifactInventory")
            .field("artifact_count", &self.artifacts.len())
            .field("total_bytes", &self.total_bytes)
            .field("digest", &self.digest)
            .finish()
    }
}

pub(crate) struct VerifiedArtifact {
    relative_reference: Box<str>,
    content_hash: Sha256Digest,
    size_bytes: u64,
    row_count: u64,
    file: File,
}

#[derive(Clone, Copy)]
struct ExpectedPhysicalArtifact<'a> {
    artifact_id: Uuid,
    relative_reference: &'a str,
    content_hash: Sha256Digest,
    size_bytes: u64,
}

impl VerifiedArtifact {
    pub(crate) fn try_clone_file(&self) -> Result<File, EvidenceError> {
        self.file.try_clone().map_err(Into::into)
    }
}

impl fmt::Debug for VerifiedArtifact {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VerifiedArtifact")
            .field("relative_reference", &"[CAPABILITY-RELATIVE REFERENCE]")
            .field("content_hash", &self.content_hash)
            .field("size_bytes", &self.size_bytes)
            .field("row_count", &self.row_count)
            .field("file", &"[VERIFIED FILE CAPABILITY]")
            .finish()
    }
}

pub(crate) fn verify_artifact_inventory(
    root: &ArtifactRoot,
    snapshot: &CatalogEvidenceSnapshot,
    cancellation: &CancellationToken,
) -> Result<VerifiedArtifactInventory, EvidenceError> {
    snapshot.check_cancellation(cancellation)?;
    let directory = root
        .try_clone_directory()
        .map_err(|_| EvidenceError::UnsafeArtifact)?;
    let source_directory_identity = FileIdentity::from_metadata(&directory.dir_metadata()?);
    let expected_rows = expected_rows(snapshot)?;
    let mut artifacts = Vec::new();
    artifacts
        .try_reserve_exact(snapshot.physical_artifact_count())
        .map_err(|_| EvidenceError::ResourceLimitExceeded)?;
    let mut ordered = expected_physical_artifacts(snapshot)?;
    ordered.sort_unstable_by(|left, right| left.relative_reference.cmp(right.relative_reference));
    for artifact in ordered {
        snapshot.check_cancellation(cancellation)?;
        root.resolve(artifact.relative_reference)?;
        let (parent, name) = open_parent_nofollow(&directory, artifact.relative_reference)?;
        let named_before = parent.symlink_metadata(name)?;
        validate_private_regular_file(&named_before, artifact.size_bytes)?;
        let identity = FileIdentity::from_metadata(&named_before);
        let mut options = OpenOptions::new();
        options.read(true).follow(FollowSymlinks::No);
        configure_nonblocking_read(&mut options);
        let opened = parent.open_with(name, &options)?;
        let opened_metadata = opened.metadata()?;
        validate_private_regular_file(&opened_metadata, artifact.size_bytes)?;
        if FileIdentity::from_metadata(&opened_metadata) != identity {
            return Err(EvidenceError::UnsafeArtifact);
        }
        let mut file = opened.into_std();
        let content_hash = hash_file(&mut file, cancellation)?;
        if content_hash != artifact.content_hash {
            return Err(EvidenceError::ArtifactMetadataMismatch);
        }
        let row_count = validate_parquet(
            &mut file,
            artifact.size_bytes,
            snapshot.request().limits().max_parquet_metadata_bytes(),
        )?;
        if expected_rows
            .get(&artifact.artifact_id)
            .is_some_and(|expected| *expected != row_count)
        {
            return Err(EvidenceError::ArtifactMetadataMismatch);
        }
        let named_after =
            named_identity(&directory, artifact.relative_reference, artifact.size_bytes)?;
        let opened_after = opened_file_metadata(&file)?;
        validate_private_regular_file(&opened_after, artifact.size_bytes)?;
        if named_after != identity || FileIdentity::from_metadata(&opened_after) != identity {
            return Err(EvidenceError::UnsafeArtifact);
        }
        file.seek(SeekFrom::Start(0))?;
        artifacts.push(VerifiedArtifact {
            relative_reference: artifact.relative_reference.into(),
            content_hash,
            size_bytes: artifact.size_bytes,
            row_count,
            file,
        });
    }
    root.try_clone_directory()
        .map_err(|_| EvidenceError::UnsafeArtifact)?;
    let total_bytes = artifacts.iter().try_fold(0_u64, |total, artifact| {
        total
            .checked_add(artifact.size_bytes)
            .ok_or(EvidenceError::ResourceLimitExceeded)
    })?;
    let digest = inventory_digest(&artifacts)?;
    Ok(VerifiedArtifactInventory {
        artifacts,
        total_bytes,
        digest,
        source_directory_identity,
    })
}

fn expected_physical_artifacts(
    snapshot: &CatalogEvidenceSnapshot,
) -> Result<Vec<ExpectedPhysicalArtifact<'_>>, EvidenceError> {
    let mut expected = Vec::new();
    expected
        .try_reserve_exact(snapshot.physical_artifact_count())
        .map_err(|_| EvidenceError::ResourceLimitExceeded)?;
    expected.extend(
        snapshot
            .artifacts()
            .iter()
            .map(|artifact| ExpectedPhysicalArtifact {
                artifact_id: artifact.artifact_id(),
                relative_reference: artifact.relative_reference(),
                content_hash: artifact.content_hash(),
                size_bytes: artifact.size_bytes(),
            }),
    );
    expected.extend(
        snapshot
            .query_artifacts()
            .iter()
            .map(|artifact| ExpectedPhysicalArtifact {
                artifact_id: artifact.artifact_id(),
                relative_reference: artifact.relative_reference(),
                content_hash: artifact.content_hash(),
                size_bytes: artifact.size_bytes(),
            }),
    );
    Ok(expected)
}

fn expected_rows(snapshot: &CatalogEvidenceSnapshot) -> Result<BTreeMap<Uuid, u64>, EvidenceError> {
    let mut expected = BTreeMap::new();
    for generation in snapshot.generations() {
        for object in generation.objects() {
            if expected
                .insert(object.artifact_id(), object.row_count())
                .is_some_and(|previous| previous != object.row_count())
            {
                return Err(EvidenceError::GenerationSemanticMismatch);
            }
        }
    }
    Ok(expected)
}

fn open_parent_nofollow<'a>(
    root: &Dir,
    reference: &'a str,
) -> Result<(Dir, &'a str), EvidenceError> {
    let path = Path::new(reference);
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or(EvidenceError::UnsafeArtifact)?;
    let mut directory = root.try_clone()?;
    if let Some(parent) = path.parent() {
        for component in parent.components() {
            let Component::Normal(component) = component else {
                return Err(EvidenceError::UnsafeArtifact);
            };
            directory = directory.open_dir_nofollow(component)?;
        }
    }
    Ok((directory, name))
}

fn named_identity(
    root: &Dir,
    reference: &str,
    expected_size: u64,
) -> Result<FileIdentity, EvidenceError> {
    let (parent, name) = open_parent_nofollow(root, reference)?;
    let metadata = parent.symlink_metadata(name)?;
    validate_private_regular_file(&metadata, expected_size)?;
    Ok(FileIdentity::from_metadata(&metadata))
}

fn validate_private_regular_file(metadata: &Metadata, size: u64) -> Result<(), EvidenceError> {
    if !metadata.is_file()
        || metadata.len() != size
        || cap_fs_ext::MetadataExt::nlink(metadata) != 1
    {
        return Err(EvidenceError::UnsafeArtifact);
    }
    validate_private_permissions(metadata)
}

#[cfg(unix)]
fn validate_private_permissions(metadata: &Metadata) -> Result<(), EvidenceError> {
    if cap_fs_ext::OsMetadataExt::mode(metadata) & 0o077 == 0 {
        Ok(())
    } else {
        Err(EvidenceError::UnsafeArtifact)
    }
}

#[cfg(windows)]
fn validate_private_permissions(metadata: &Metadata) -> Result<(), EvidenceError> {
    use cap_std::fs::MetadataExt as _;

    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;

    if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT == 0 {
        Ok(())
    } else {
        Err(EvidenceError::UnsafeArtifact)
    }
}

#[cfg(not(any(unix, windows)))]
fn validate_private_permissions(_metadata: &Metadata) -> Result<(), EvidenceError> {
    Err(EvidenceError::UnsafeArtifact)
}

fn opened_file_metadata(file: &File) -> Result<Metadata, EvidenceError> {
    cap_std::fs::File::from_std(file.try_clone()?)
        .metadata()
        .map_err(Into::into)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

impl FileIdentity {
    fn from_metadata(metadata: &Metadata) -> Self {
        Self {
            device: cap_fs_ext::MetadataExt::dev(metadata),
            inode: cap_fs_ext::MetadataExt::ino(metadata),
        }
    }
}

fn hash_file(
    file: &mut File,
    cancellation: &CancellationToken,
) -> Result<Sha256Digest, EvidenceError> {
    file.seek(SeekFrom::Start(0))?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; HASH_BUFFER_BYTES];
    loop {
        if cancellation.is_cancelled() {
            return Err(EvidenceError::Cancelled);
        }
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(Sha256Digest::new(digest.finalize().into()))
}

fn validate_parquet(
    file: &mut File,
    size_bytes: u64,
    max_metadata_bytes: u64,
) -> Result<u64, EvidenceError> {
    if size_bytes < PARQUET_FOOTER_BYTES {
        return Err(EvidenceError::ArtifactMetadataMismatch);
    }
    let footer_offset =
        i64::try_from(PARQUET_FOOTER_BYTES).map_err(|_| EvidenceError::ResourceLimitExceeded)?;
    file.seek(SeekFrom::End(-footer_offset))?;
    let mut footer = [0_u8; PARQUET_FOOTER_BYTES as usize];
    file.read_exact(&mut footer)?;
    if &footer[4..] != PARQUET_MAGIC {
        return Err(EvidenceError::ArtifactMetadataMismatch);
    }
    let metadata_bytes = u64::from(u32::from_le_bytes(
        footer[..4]
            .try_into()
            .map_err(|_| EvidenceError::ArtifactMetadataMismatch)?,
    ));
    if metadata_bytes == 0
        || metadata_bytes > max_metadata_bytes
        || metadata_bytes
            .checked_add(PARQUET_FOOTER_BYTES)
            .is_none_or(|required| required > size_bytes)
    {
        return Err(EvidenceError::ArtifactMetadataMismatch);
    }
    file.seek(SeekFrom::Start(0))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file.try_clone()?)?;
    let metadata = reader.metadata();
    let rows = u64::try_from(metadata.file_metadata().num_rows())
        .map_err(|_| EvidenceError::ArtifactMetadataMismatch)?;
    let row_group_rows = metadata
        .row_groups()
        .iter()
        .try_fold(0_u64, |total, group| {
            let rows = u64::try_from(group.num_rows())
                .map_err(|_| EvidenceError::ArtifactMetadataMismatch)?;
            total
                .checked_add(rows)
                .ok_or(EvidenceError::ResourceLimitExceeded)
        })?;
    if rows == 0 || row_group_rows != rows {
        return Err(EvidenceError::ArtifactMetadataMismatch);
    }
    Ok(rows)
}

fn inventory_digest(
    artifacts: &[VerifiedArtifact],
) -> Result<ArtifactInventoryDigest, EvidenceError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/analytical-artifact-inventory/v1");
    digest.update(
        u64::try_from(artifacts.len())
            .map_err(|_| EvidenceError::ResourceLimitExceeded)?
            .to_be_bytes(),
    );
    for artifact in artifacts {
        digest.update(
            u64::try_from(artifact.relative_reference.len())
                .map_err(|_| EvidenceError::ResourceLimitExceeded)?
                .to_be_bytes(),
        );
        digest.update(artifact.relative_reference.as_bytes());
        digest.update(artifact.content_hash.bytes());
        digest.update(artifact.size_bytes.to_be_bytes());
        digest.update(artifact.row_count.to_be_bytes());
    }
    ArtifactInventoryDigest::try_new(digest.finalize().into())
        .ok_or(EvidenceError::ArtifactMetadataMismatch)
}

#[cfg(unix)]
fn configure_nonblocking_read(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;

    options.custom_flags(libc::O_NONBLOCK | libc::O_CLOEXEC);
}

#[cfg(not(unix))]
fn configure_nonblocking_read(_options: &mut OpenOptions) {}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    #[test]
    fn final_open_metadata_requires_exact_size_and_one_link()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::fs::{OpenOptions as StdOpenOptions, hard_link};
        use std::os::unix::fs::OpenOptionsExt as _;

        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("artifact.parquet");
        let linked = temporary.path().join("artifact-link.parquet");
        let file = StdOpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)?;
        file.set_len(4)?;
        assert!(
            super::validate_private_regular_file(&super::opened_file_metadata(&file)?, 4).is_ok()
        );

        file.set_len(5)?;
        assert!(matches!(
            super::validate_private_regular_file(&super::opened_file_metadata(&file)?, 4),
            Err(super::EvidenceError::UnsafeArtifact)
        ));
        file.set_len(4)?;
        hard_link(&path, &linked)?;
        assert!(matches!(
            super::validate_private_regular_file(&super::opened_file_metadata(&file)?, 4),
            Err(super::EvidenceError::UnsafeArtifact)
        ));
        Ok(())
    }
}
