//! Fresh and exact-subset retry materialization for verified analytical objects.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
use std::path::{Component, Path};

use cap_fs_ext::{DirExt as _, FollowSymlinks, OpenOptionsFollowExt as _};
use cap_std::fs::{Dir, OpenOptions};
use market_squawk_platform::ArtifactRoot;
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use super::{
    EvidenceError, FileIdentity, HASH_BUFFER_BYTES, MAX_PARQUET_METADATA_BYTES, VerifiedArtifact,
    VerifiedArtifactInventory, validate_parquet, validate_private_regular_file,
    validate_private_std_regular_file,
};
use crate::Sha256Digest;
use crate::parquet_store::VerifiedRestoreControlSubset;

pub(crate) struct MaterializedArtifactRoot {
    root: ArtifactRoot,
    directory: Dir,
    identity: FileIdentity,
}

impl MaterializedArtifactRoot {
    pub(crate) fn into_retained_capabilities(self) -> (ArtifactRoot, Dir) {
        (self.root, self.directory)
    }
}

impl std::fmt::Debug for MaterializedArtifactRoot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MaterializedArtifactRoot")
            .field("root", &"[RETAINED ARTIFACT ROOT]")
            .field("directory", &"[RETAINED DIRECTORY CAPABILITY]")
            .field("identity", &self.identity)
            .finish()
    }
}

impl VerifiedArtifactInventory {
    pub(crate) fn materialize_no_replace(
        &self,
        destination: &ArtifactRoot,
        cancellation: &CancellationToken,
    ) -> Result<MaterializedArtifactRoot, EvidenceError> {
        let (directory, identity) = self.prepare_destination(destination, cancellation)?;
        if directory.read_dir(".")?.next().transpose()?.is_some() {
            return Err(EvidenceError::DestinationNotFresh);
        }
        self.materialize_verified_subset(destination, directory, identity, cancellation, None)
    }

    /// Resumes only an exact subset left by this already receipt-verified bundle.
    ///
    /// The restore coordinator is the sole caller and must first reverify the catalog receipt,
    /// relationship digest, physical inventory digest, and destination catalog identity. This
    /// method never deletes, overwrites, or accepts an unexpected entry.
    pub(crate) fn resume_exact_subset_no_replace(
        &self,
        destination: &ArtifactRoot,
        cancellation: &CancellationToken,
        controls: &VerifiedRestoreControlSubset,
    ) -> Result<MaterializedArtifactRoot, EvidenceError> {
        let (directory, identity) = self.prepare_destination(destination, cancellation)?;
        self.validate_exact_subset(&directory, false, cancellation, Some(controls))?;
        self.materialize_verified_subset(
            destination,
            directory,
            identity,
            cancellation,
            Some(controls),
        )
    }

    fn prepare_destination(
        &self,
        destination: &ArtifactRoot,
        cancellation: &CancellationToken,
    ) -> Result<(Dir, FileIdentity), EvidenceError> {
        if cancellation.is_cancelled() {
            return Err(EvidenceError::Cancelled);
        }
        let directory = destination
            .try_clone_directory()
            .map_err(|_| EvidenceError::DestinationNotFresh)?;
        let metadata = directory.dir_metadata()?;
        if !metadata.is_dir() {
            return Err(EvidenceError::DestinationNotFresh);
        }
        let identity = FileIdentity::from_metadata(&metadata);
        if identity == self.source_directory_identity {
            return Err(EvidenceError::SameRootRestore);
        }
        Ok((directory, identity))
    }

    fn materialize_verified_subset(
        &self,
        destination: &ArtifactRoot,
        directory: Dir,
        identity: FileIdentity,
        cancellation: &CancellationToken,
        controls: Option<&VerifiedRestoreControlSubset>,
    ) -> Result<MaterializedArtifactRoot, EvidenceError> {
        if self
            .materialize_missing(&directory, cancellation)
            .and_then(|()| self.validate_exact_subset(&directory, true, cancellation, controls))
            .and_then(|()| synchronize_layout(&directory, &self.artifacts))
            .is_err()
        {
            return Err(EvidenceError::DestinationMaterializationIndeterminate);
        }
        let revalidated = destination
            .try_clone_directory()
            .map_err(|_| EvidenceError::DestinationMaterializationIndeterminate)?;
        if FileIdentity::from_metadata(&revalidated.dir_metadata()?) != identity {
            return Err(EvidenceError::DestinationMaterializationIndeterminate);
        }
        Ok(MaterializedArtifactRoot {
            root: destination.clone(),
            directory,
            identity,
        })
    }

    fn materialize_missing(
        &self,
        directory: &Dir,
        cancellation: &CancellationToken,
    ) -> Result<(), EvidenceError> {
        let objects = ensure_directory(directory, "objects")?;
        let sha256 = ensure_directory(&objects, "sha256")?;
        let mut current_shard: Option<(&str, Dir)> = None;
        for artifact in &self.artifacts {
            if cancellation.is_cancelled() {
                return Err(EvidenceError::Cancelled);
            }
            let (shard, filename) = object_components(&artifact.relative_reference)?;
            if current_shard
                .as_ref()
                .is_none_or(|(current, _)| *current != shard)
            {
                current_shard = Some((shard, ensure_directory(&sha256, shard)?));
            }
            let shard_directory = current_shard
                .as_ref()
                .map(|(_, directory)| directory)
                .ok_or(EvidenceError::ArtifactMetadataMismatch)?;
            match shard_directory.symlink_metadata(filename) {
                Ok(_) => verify_existing(shard_directory, filename, artifact, cancellation)?,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    materialize_one(shard_directory, filename, artifact, cancellation)?;
                }
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    fn validate_exact_subset(
        &self,
        directory: &Dir,
        require_complete: bool,
        cancellation: &CancellationToken,
        controls: Option<&VerifiedRestoreControlSubset>,
    ) -> Result<(), EvidenceError> {
        if cancellation.is_cancelled() {
            return Err(EvidenceError::Cancelled);
        }
        let expected = expected_layout(&self.artifacts)?;
        let mut objects_present = false;
        let mut any_entry = false;
        for root_entry in directory.read_dir(".")? {
            let name = root_entry?.file_name();
            any_entry = true;
            if name == "objects" {
                if objects_present {
                    return Err(EvidenceError::DestinationConflict);
                }
                objects_present = true;
            } else if !name
                .to_str()
                .is_some_and(|name| controls.is_some_and(|controls| controls.contains(name)))
            {
                return Err(EvidenceError::DestinationConflict);
            }
        }
        if !any_entry {
            return if require_complete || !self.artifacts.is_empty() {
                if require_complete {
                    Err(EvidenceError::DestinationConflict)
                } else {
                    Ok(())
                }
            } else {
                Ok(())
            };
        }
        if !objects_present {
            return if require_complete {
                Err(EvidenceError::DestinationConflict)
            } else {
                Ok(())
            };
        }
        if controls.is_none() && directory.read_dir(".")?.count() != 1 {
            return Err(EvidenceError::DestinationConflict);
        }
        let objects = directory
            .open_dir_nofollow("objects")
            .map_err(|_| EvidenceError::DestinationConflict)?;
        let mut object_entries = objects.read_dir(".")?;
        let Some(sha_entry) = object_entries.next().transpose()? else {
            return if require_complete {
                Err(EvidenceError::DestinationConflict)
            } else {
                Ok(())
            };
        };
        if sha_entry.file_name() != "sha256" || object_entries.next().transpose()?.is_some() {
            return Err(EvidenceError::DestinationConflict);
        }
        let sha256 = objects
            .open_dir_nofollow("sha256")
            .map_err(|_| EvidenceError::DestinationConflict)?;
        let mut observed = 0_usize;
        for shard_entry in sha256.read_dir(".")? {
            if cancellation.is_cancelled() {
                return Err(EvidenceError::Cancelled);
            }
            let shard_entry = shard_entry?;
            let shard = shard_entry
                .file_name()
                .into_string()
                .map_err(|_| EvidenceError::DestinationConflict)?;
            let expected_files = expected
                .get(shard.as_str())
                .ok_or(EvidenceError::DestinationConflict)?;
            let shard_directory = sha256
                .open_dir_nofollow(&shard)
                .map_err(|_| EvidenceError::DestinationConflict)?;
            for file_entry in shard_directory.read_dir(".")? {
                if cancellation.is_cancelled() {
                    return Err(EvidenceError::Cancelled);
                }
                let filename = file_entry?
                    .file_name()
                    .into_string()
                    .map_err(|_| EvidenceError::DestinationConflict)?;
                let artifact = expected_files
                    .get(filename.as_str())
                    .ok_or(EvidenceError::DestinationConflict)?;
                verify_existing(&shard_directory, &filename, artifact, cancellation)?;
                observed = observed
                    .checked_add(1)
                    .ok_or(EvidenceError::ResourceLimitExceeded)?;
            }
        }
        if require_complete && observed != self.artifacts.len() {
            return Err(EvidenceError::DestinationConflict);
        }
        Ok(())
    }
}

fn expected_layout(
    artifacts: &[VerifiedArtifact],
) -> Result<BTreeMap<&str, BTreeMap<&str, &VerifiedArtifact>>, EvidenceError> {
    let mut layout = BTreeMap::new();
    for artifact in artifacts {
        let (shard, filename) = object_components(&artifact.relative_reference)?;
        if layout
            .entry(shard)
            .or_insert_with(BTreeMap::new)
            .insert(filename, artifact)
            .is_some()
        {
            return Err(EvidenceError::DestinationConflict);
        }
    }
    Ok(layout)
}

fn object_components(reference: &str) -> Result<(&str, &str), EvidenceError> {
    reference
        .strip_prefix("objects/sha256/")
        .and_then(|relative| relative.split_once('/'))
        .ok_or(EvidenceError::ArtifactMetadataMismatch)
}

fn ensure_directory(parent: &Dir, name: &str) -> Result<Dir, EvidenceError> {
    match parent.open_dir_nofollow(name) {
        Ok(directory) => Ok(directory),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            parent.create_dir(name)?;
            parent.open_dir_nofollow(name).map_err(Into::into)
        }
        Err(_) => Err(EvidenceError::DestinationConflict),
    }
}

fn materialize_one(
    parent: &Dir,
    filename: &str,
    artifact: &VerifiedArtifact,
    cancellation: &CancellationToken,
) -> Result<(), EvidenceError> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    options.follow(FollowSymlinks::No);
    configure_private_creation(&mut options);
    let target = parent.open_with(filename, &options)?;
    let target_metadata = target.metadata()?;
    validate_private_regular_file(&target_metadata, 0)?;
    let target_identity = FileIdentity::from_metadata(&target_metadata);
    let mut target = target.into_std();
    let mut source = artifact.try_clone_file()?;
    source.seek(SeekFrom::Start(0))?;
    let mut copied = 0_u64;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; HASH_BUFFER_BYTES];
    loop {
        if cancellation.is_cancelled() {
            return Err(EvidenceError::Cancelled);
        }
        let read = source.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        copied = copied
            .checked_add(u64::try_from(read).map_err(|_| EvidenceError::ResourceLimitExceeded)?)
            .ok_or(EvidenceError::ResourceLimitExceeded)?;
        if copied > artifact.size_bytes {
            return Err(EvidenceError::ArtifactMetadataMismatch);
        }
        target.write_all(&buffer[..read])?;
        digest.update(&buffer[..read]);
    }
    if copied != artifact.size_bytes
        || Sha256Digest::new(digest.finalize().into()) != artifact.content_hash
    {
        return Err(EvidenceError::ArtifactMetadataMismatch);
    }
    target.sync_all()?;
    let named = parent.symlink_metadata(filename)?;
    validate_private_regular_file(&named, artifact.size_bytes)?;
    let opened_metadata = target.metadata()?;
    validate_private_std_regular_file(&opened_metadata, artifact.size_bytes)?;
    if FileIdentity::from_metadata(&named) != target_identity
        || !target_identity.matches_std(&opened_metadata)
        || hash_file(&mut target, cancellation)? != artifact.content_hash
        || validate_parquet(&mut target, artifact.size_bytes, MAX_PARQUET_METADATA_BYTES)?
            != artifact.row_count
    {
        return Err(EvidenceError::ArtifactMetadataMismatch);
    }
    let named_after = parent.symlink_metadata(filename)?;
    validate_private_regular_file(&named_after, artifact.size_bytes)?;
    let opened_after = target.metadata()?;
    validate_private_std_regular_file(&opened_after, artifact.size_bytes)?;
    if FileIdentity::from_metadata(&named_after) != target_identity
        || !target_identity.matches_std(&opened_after)
    {
        return Err(EvidenceError::ArtifactMetadataMismatch);
    }
    Ok(())
}

fn verify_existing(
    parent: &Dir,
    filename: &str,
    artifact: &VerifiedArtifact,
    cancellation: &CancellationToken,
) -> Result<(), EvidenceError> {
    if cancellation.is_cancelled() {
        return Err(EvidenceError::Cancelled);
    }
    let named_before = parent.symlink_metadata(filename)?;
    validate_private_regular_file(&named_before, artifact.size_bytes)?;
    let identity = FileIdentity::from_metadata(&named_before);
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    configure_nonblocking_read(&mut options);
    let opened = parent.open_with(filename, &options)?;
    if FileIdentity::from_metadata(&opened.metadata()?) != identity {
        return Err(EvidenceError::DestinationConflict);
    }
    let mut file = opened.into_std();
    let digest = hash_file(&mut file, cancellation)?;
    if digest != artifact.content_hash
        || validate_parquet(&mut file, artifact.size_bytes, MAX_PARQUET_METADATA_BYTES)?
            != artifact.row_count
    {
        return Err(EvidenceError::DestinationConflict);
    }
    let named_after = parent.symlink_metadata(filename)?;
    validate_private_regular_file(&named_after, artifact.size_bytes)?;
    let opened_after = file.metadata()?;
    validate_private_std_regular_file(&opened_after, artifact.size_bytes)?;
    if FileIdentity::from_metadata(&named_after) != identity || !identity.matches_std(&opened_after)
    {
        return Err(EvidenceError::DestinationConflict);
    }
    Ok(())
}

fn hash_file(
    file: &mut std::fs::File,
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

fn synchronize_layout(
    directory: &Dir,
    artifacts: &[VerifiedArtifact],
) -> Result<(), EvidenceError> {
    let shards: BTreeSet<_> = artifacts
        .iter()
        .map(|artifact| object_components(&artifact.relative_reference).map(|(shard, _)| shard))
        .collect::<Result<_, _>>()?;
    for shard in shards {
        sync_directory_at(directory, &format!("objects/sha256/{shard}"))?;
    }
    sync_directory_at(directory, "objects/sha256")?;
    sync_directory_at(directory, "objects")?;
    sync_directory_at(directory, ".")
}

#[cfg(unix)]
fn configure_private_creation(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;

    options.mode(0o600);
    options.custom_flags(libc::O_NONBLOCK | libc::O_CLOEXEC);
}

#[cfg(not(unix))]
fn configure_private_creation(_options: &mut OpenOptions) {}

#[cfg(unix)]
fn configure_nonblocking_read(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;

    options.custom_flags(libc::O_NONBLOCK | libc::O_CLOEXEC);
}

#[cfg(not(unix))]
fn configure_nonblocking_read(_options: &mut OpenOptions) {}

#[cfg(unix)]
fn sync_directory_at(directory: &Dir, path: &str) -> Result<(), EvidenceError> {
    use cap_std::fs::OpenOptionsExt as _;

    let target = if path == "." {
        directory.try_clone()?
    } else {
        let mut target = directory.try_clone()?;
        for component in Path::new(path).components() {
            let Component::Normal(component) = component else {
                return Err(EvidenceError::UnsafeArtifact);
            };
            target = target.open_dir_nofollow(component)?;
        }
        target
    };
    let mut options = OpenOptions::new();
    options.read(true);
    options.custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC);
    target.open_with(".", &options)?.sync_all()?;
    Ok(())
}

#[cfg(windows)]
fn sync_directory_at(_directory: &Dir, _path: &str) -> Result<(), EvidenceError> {
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn sync_directory_at(_directory: &Dir, _path: &str) -> Result<(), EvidenceError> {
    Err(EvidenceError::DestinationMaterializationIndeterminate)
}

#[cfg(test)]
mod tests {
    use market_squawk_platform::LocalPaths;
    use tokio_util::sync::CancellationToken;

    use super::super::{FileIdentity, VerifiedArtifactInventory};
    use crate::authority_transition::ArtifactInventoryDigest;
    use crate::authority_transition::evidence::EvidenceError;

    #[test]
    fn materialization_rejects_same_retained_root_before_mutation()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(temporary.path().join("market-squawk"))?;
        let root = paths.artifacts()?;
        let directory = root.try_clone_directory()?;
        let metadata = directory.dir_metadata()?;
        let inventory = VerifiedArtifactInventory {
            artifacts: Vec::new(),
            total_bytes: 0,
            digest: ArtifactInventoryDigest::try_new([7; 32]).ok_or("invalid inventory digest")?,
            source_directory_identity: FileIdentity::from_metadata(&metadata),
        };

        let result = inventory.materialize_no_replace(root, &CancellationToken::new());

        assert!(matches!(result, Err(EvidenceError::SameRootRestore)));
        assert_eq!(directory.read_dir(".")?.count(), 0);
        Ok(())
    }
}
