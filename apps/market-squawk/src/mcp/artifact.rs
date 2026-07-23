//! Capability-confined immutable MCP artifact publication.

use std::{
    io::{Read as _, Write as _},
    num::NonZeroUsize,
    path::Path,
    sync::Arc,
};

use async_trait::async_trait;
use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt as _};
use cap_std::fs::{Dir, OpenOptions};
use market_squawk_mcp::{
    ArtifactError, ArtifactPublication, ArtifactPublicationContext, ArtifactRead,
    ArtifactReadContext, ArtifactReadRequest, ArtifactReference, ArtifactRepository,
};
use market_squawk_platform::ArtifactRoot;
use sha2::{Digest, Sha256};

const ARTIFACT_NAMESPACE: &str = "mcp/v1";
const READ_CHECKPOINT_BYTES: usize = 64 * 1024;

#[allow(
    dead_code,
    reason = "the integration-owned shared artifact domain consumes this factory"
)]
pub(crate) fn controlled_artifact_repository(
    root: ArtifactRoot,
    maximum_bytes: NonZeroUsize,
) -> Result<Arc<dyn ArtifactRepository>, ArtifactError> {
    ControlledArtifactRepository::try_new(root, maximum_bytes)
        .map(|repository| Arc::new(repository) as Arc<dyn ArtifactRepository>)
}

/// Bounded content-addressed repository under the configured artifact capability.
#[derive(Debug)]
pub(super) struct ControlledArtifactRepository {
    root: ArtifactRoot,
    maximum_bytes: NonZeroUsize,
}

impl ControlledArtifactRepository {
    pub(super) fn try_new(
        root: ArtifactRoot,
        maximum_bytes: NonZeroUsize,
    ) -> Result<Self, ArtifactError> {
        let directory = root
            .try_clone_directory()
            .map_err(|_| ArtifactError::Unavailable)?;
        directory
            .create_dir_all(ARTIFACT_NAMESPACE)
            .map_err(|_| ArtifactError::Unavailable)?;
        Ok(Self {
            root,
            maximum_bytes,
        })
    }

    fn publish_bounded(
        &self,
        publication: &ArtifactPublication,
        context: &ArtifactPublicationContext,
    ) -> Result<ArtifactReference, ArtifactError> {
        context.ensure_live()?;
        if publication.byte_count() == 0 || publication.byte_count() > self.maximum_bytes.get() {
            return Err(ArtifactError::InvalidPublication);
        }
        let digest = publication.sha256_hex();
        let prefix = digest.get(..2).ok_or(ArtifactError::InvalidPublication)?;
        let parent = format!("{ARTIFACT_NAMESPACE}/{prefix}");
        let artifact_reference = format!("{parent}/{digest}.json");
        let mut nonce = [0_u8; 16];
        getrandom::fill(&mut nonce).map_err(|_| ArtifactError::Unavailable)?;
        let staging_reference = format!("{parent}/stage-{}.tmp", hex_bytes(&nonce));
        let artifact_path = Path::new(&artifact_reference);
        let staging_path = Path::new(&staging_reference);
        drop(
            self.root
                .resolve(artifact_path)
                .map_err(|_| ArtifactError::Unavailable)?,
        );
        drop(
            self.root
                .resolve(staging_path)
                .map_err(|_| ArtifactError::Unavailable)?,
        );

        let directory = self
            .root
            .try_clone_directory()
            .map_err(|_| ArtifactError::Unavailable)?;
        directory
            .create_dir_all(&parent)
            .map_err(|_| ArtifactError::Unavailable)?;
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        options.follow(FollowSymlinks::No);
        configure_private_creation(&mut options);
        let mut staging = directory
            .open_with(staging_path, &options)
            .map_err(|_| ArtifactError::Unavailable)?;
        let mut guard = StagingGuard::new(&directory, staging_path);
        staging
            .write_all(publication.content())
            .map_err(|_| ArtifactError::Unavailable)?;
        staging.sync_all().map_err(|_| ArtifactError::Unavailable)?;
        drop(staging);
        context.ensure_live()?;

        match directory.hard_link(staging_path, &directory, artifact_path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(_error) => return Err(ArtifactError::Unavailable),
        }
        guard.remove()?;
        synchronize_publication_directories(&directory, artifact_path)?;
        context.ensure_live()?;
        let persisted = read_bounded_regular(&directory, artifact_path, self.maximum_bytes.get())?;
        if persisted.as_slice() != publication.content()
            || format!("{:x}", Sha256::digest(&persisted)) != publication.sha256_hex()
        {
            return Err(ArtifactError::Unavailable);
        }
        ArtifactReference::try_new(
            format!("mcp-{digest}"),
            digest,
            publication.byte_count(),
            publication.media_type(),
        )
    }
}

#[async_trait]
impl ArtifactRepository for ControlledArtifactRepository {
    async fn publish(
        &self,
        publication: ArtifactPublication,
        context: ArtifactPublicationContext,
    ) -> Result<ArtifactReference, ArtifactError> {
        self.publish_bounded(&publication, &context)
    }

    async fn read(
        &self,
        request: ArtifactReadRequest,
        context: ArtifactReadContext,
    ) -> Result<ArtifactRead, ArtifactError> {
        context.ensure_live()?;
        let root = self.root.clone();
        let maximum_bytes = self.maximum_bytes;
        tokio::task::spawn_blocking(move || {
            read_verified_artifact(&root, maximum_bytes, request, &context)
        })
        .await
        .map_err(|_| ArtifactError::Unavailable)?
    }
}

#[derive(Debug)]
struct StagingGuard<'directory> {
    directory: &'directory Dir,
    path: &'directory Path,
    armed: bool,
}

impl<'directory> StagingGuard<'directory> {
    const fn new(directory: &'directory Dir, path: &'directory Path) -> Self {
        Self {
            directory,
            path,
            armed: true,
        }
    }

    fn remove(&mut self) -> Result<(), ArtifactError> {
        self.directory
            .remove_file(self.path)
            .map_err(|_| ArtifactError::Unavailable)?;
        self.armed = false;
        Ok(())
    }
}

impl Drop for StagingGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            let _ignored = self.directory.remove_file(self.path);
            self.armed = false;
        }
    }
}

fn read_bounded_regular(
    directory: &Dir,
    path: &Path,
    maximum_bytes: usize,
) -> Result<Vec<u8>, ArtifactError> {
    let mut options = OpenOptions::new();
    options.read(true);
    options.follow(FollowSymlinks::No);
    configure_nonblocking_read(&mut options);
    let mut file = directory
        .open_with(path, &options)
        .map_err(|_| ArtifactError::Unavailable)?;
    let metadata = file.metadata().map_err(|_| ArtifactError::Unavailable)?;
    if !metadata.is_file() {
        return Err(ArtifactError::Unavailable);
    }
    let size = usize::try_from(metadata.len()).map_err(|_| ArtifactError::Unavailable)?;
    if size == 0 || size > maximum_bytes {
        return Err(ArtifactError::Unavailable);
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(size)
        .map_err(|_| ArtifactError::Unavailable)?;
    bytes.resize(size, 0);
    file.read_exact(&mut bytes)
        .map_err(|_| ArtifactError::Unavailable)?;
    let mut trailing = [0_u8; 1];
    if file
        .read(&mut trailing)
        .map_err(|_| ArtifactError::Unavailable)?
        != 0
    {
        return Err(ArtifactError::Unavailable);
    }
    Ok(bytes)
}

fn read_verified_artifact(
    root: &ArtifactRoot,
    repository_maximum: NonZeroUsize,
    request: ArtifactReadRequest,
    context: &ArtifactReadContext,
) -> Result<ArtifactRead, ArtifactError> {
    context.ensure_live()?;
    let reference = request.reference();
    if reference.id() != format!("mcp-{}", reference.sha256())
        || reference.media_type() != "application/json"
    {
        return Err(ArtifactError::InvalidReference);
    }
    if reference.byte_count() > repository_maximum.get()
        || reference.byte_count() > request.maximum_bytes().get()
    {
        return Err(ArtifactError::ReadLimitExceeded);
    }
    let prefix = reference
        .sha256()
        .get(..2)
        .ok_or(ArtifactError::InvalidReference)?;
    let artifact_reference = format!("{ARTIFACT_NAMESPACE}/{prefix}/{}.json", reference.sha256());
    let artifact_path = Path::new(&artifact_reference);
    drop(
        root.resolve(artifact_path)
            .map_err(|_| ArtifactError::InvalidReference)?,
    );
    let directory = root
        .try_clone_directory()
        .map_err(|_| ArtifactError::Unavailable)?;
    let mut options = OpenOptions::new();
    options.read(true);
    options.follow(FollowSymlinks::No);
    configure_nonblocking_read(&mut options);
    let mut file = directory
        .open_with(artifact_path, &options)
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                ArtifactError::NotFound
            } else {
                ArtifactError::Unavailable
            }
        })?;
    let metadata = file.metadata().map_err(|_| ArtifactError::Unavailable)?;
    let size = usize::try_from(metadata.len()).map_err(|_| ArtifactError::Unavailable)?;
    if !metadata.is_file()
        || size != reference.byte_count()
        || size > repository_maximum.get()
        || size > request.maximum_bytes().get()
    {
        return Err(ArtifactError::Unavailable);
    }
    let mut content = Vec::new();
    content
        .try_reserve_exact(size)
        .map_err(|_| ArtifactError::ReadLimitExceeded)?;
    content.resize(size, 0);
    let mut offset = 0_usize;
    let mut digest = Sha256::new();
    while offset < size {
        context.ensure_live()?;
        let end = offset
            .checked_add(READ_CHECKPOINT_BYTES)
            .map(|end| end.min(size))
            .ok_or(ArtifactError::ReadLimitExceeded)?;
        let read = file
            .read(&mut content[offset..end])
            .map_err(|_| ArtifactError::Unavailable)?;
        if read == 0 {
            return Err(ArtifactError::Unavailable);
        }
        let read_end = offset
            .checked_add(read)
            .ok_or(ArtifactError::ReadLimitExceeded)?;
        digest.update(&content[offset..read_end]);
        offset = read_end;
    }
    let mut trailing = [0_u8; 1];
    if file
        .read(&mut trailing)
        .map_err(|_| ArtifactError::Unavailable)?
        != 0
        || format!("{:x}", digest.finalize()) != reference.sha256()
    {
        return Err(ArtifactError::Unavailable);
    }
    context.ensure_live()?;
    ArtifactRead::try_new(request.into_reference(), content)
}

fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

#[cfg(unix)]
fn configure_private_creation(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;

    options.mode(0o600);
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
fn synchronize_publication_directories(
    directory: &Dir,
    artifact_path: &Path,
) -> Result<(), ArtifactError> {
    let parent = artifact_path.parent().ok_or(ArtifactError::Unavailable)?;
    for path in [
        parent,
        Path::new(ARTIFACT_NAMESPACE),
        Path::new("mcp"),
        Path::new("."),
    ] {
        directory
            .open_dir(path)
            .map(Dir::into_std_file)
            .and_then(|file| file.sync_all())
            .map_err(|_| ArtifactError::Unavailable)?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn synchronize_publication_directories(
    _directory: &Dir,
    _artifact_path: &Path,
) -> Result<(), ArtifactError> {
    Err(ArtifactError::Unavailable)
}
