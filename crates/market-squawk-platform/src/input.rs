//! One-shot, no-follow capabilities for explicitly user-authorized local input roots.

use std::convert::Infallible;
use std::ffi::OsString;
use std::fmt;
use std::io::{Read as _, Seek as _, SeekFrom};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use cap_fs_ext::{DirExt as _, FollowSymlinks, MetadataExt as _, OpenOptionsFollowExt as _};
use cap_std::ambient_authority;
use cap_std::fs::{Dir, OpenOptions};
use cap_std::time::SystemTime;
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

#[path = "input/ownership.rs"]
mod ownership;

pub use ownership::{
    UserOwnedInputAuthority, UserOwnedInputEvidence, UserOwnedInputRootIdentityDigest,
};

const MAX_INPUT_DEPTH: usize = 64;
const MAX_COMPONENT_BYTES: usize = 255;
const INPUT_READ_CHUNK_BYTES: usize = 64 * 1024;

/// A no-follow capability rooted at one directory explicitly selected by the local user.
#[derive(Clone)]
pub struct UserAuthorizedInputRoot {
    inner: Arc<InputRootInner>,
}

struct InputRootInner {
    display_root: PathBuf,
    directory: Dir,
    identity: FileSystemIdentity,
    ownership_binding: Arc<InputRootOwnershipBinding>,
}

struct InputRootOwnershipBinding {
    identity_digest: UserOwnedInputRootIdentityDigest,
}

/// A one-shot capability for one regular file below a user-authorized root.
pub struct InputFileCapability {
    root: Arc<InputRootInner>,
    components: Vec<OsString>,
    parent_identities: Vec<FileSystemIdentity>,
    identity: InputFileIdentity,
}

/// An opened one-shot file whose root, path chain, and handle identity were validated.
pub struct VerifiedInputFile {
    root: Arc<InputRootInner>,
    components: Vec<OsString>,
    parent_identities: Vec<FileSystemIdentity>,
    identity: InputFileIdentity,
    file: std::fs::File,
    maximum_bytes: u64,
}

/// Opaque stable file identity retained across one admitted read.
#[derive(Clone, Eq, PartialEq)]
pub struct InputFileIdentity {
    filesystem: FileSystemIdentity,
    size_bytes: u64,
    modified: Option<SystemTime>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FileSystemIdentity {
    device: u64,
    inode: u64,
}

/// Exact bytes released only after the one-shot file capability was revalidated.
pub struct BoundedInput {
    bytes: Box<[u8]>,
    identity: InputFileIdentity,
    digest: EvidenceDigest,
    root_ownership_binding: Arc<InputRootOwnershipBinding>,
}

/// One of the two exact passes over a controlled input handle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InputReadPass {
    /// Exact bytes retained for publication and their incremental digest.
    Primary,
    /// Independent digest and length verification over the same handle.
    Verification,
}

/// A bounded filesystem-read control point.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InputReadCheckpoint {
    /// Before the stability lock, fixed allocation, or filesystem reads begin.
    BeforeRead,
    /// Immediately before one read of at most 64 KiB.
    BeforeReadChunk {
        /// Exact pass being read.
        pass: InputReadPass,
        /// Bytes already read in this pass.
        offset_bytes: u64,
    },
    /// Immediately after one bounded read attempt.
    AfterReadChunk {
        /// Exact pass being read.
        pass: InputReadPass,
        /// Bytes read in this pass after the attempt.
        offset_bytes: u64,
    },
    /// Immediately before the one-byte file-growth probe.
    BeforeGrowthProbe,
    /// Immediately after the one-byte file-growth probe.
    AfterGrowthProbe,
    /// Immediately before final path and handle identity revalidation.
    BeforeIdentityRevalidation,
    /// Immediately before verified bytes are released to the caller.
    BeforeRelease,
}

/// A caller-owned cooperative controller for bounded input reads.
pub trait InputReadControl {
    /// Checks whether the operation may continue at the supplied bounded control point.
    ///
    /// # Errors
    ///
    /// Returns the exact caller control state that stopped the operation.
    fn checkpoint(&self, checkpoint: InputReadCheckpoint) -> Result<(), InputReadControlError>;
}

/// Caller control failure independent of filesystem and capability errors.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum InputReadControlError {
    /// The caller cancelled the read.
    #[error("controlled input read was cancelled")]
    Cancelled,
    /// The caller's monotonic deadline expired.
    #[error("controlled input read deadline was exceeded")]
    DeadlineExceeded,
    /// The caller could not establish trusted control state.
    #[error("controlled input read control state is unavailable")]
    Unavailable,
}

/// Failure from either the input capability or its caller-owned control contract.
#[derive(Debug, Error)]
pub enum ControlledInputFileError {
    /// Filesystem, stability, identity, allocation, or byte-limit failure.
    #[error(transparent)]
    Input(#[from] InputFileError),
    /// Exact caller cancellation, deadline, or trusted-control failure.
    #[error(transparent)]
    Control(#[from] InputReadControlError),
}

enum BoundedReadError<E> {
    Input(InputFileError),
    Control(E),
}

/// Failure to prepare, resolve, open, or consume a local input capability.
#[derive(Debug, Error)]
pub enum InputFileError {
    /// Input roots must be explicit absolute local paths.
    #[error("user-authorized input root must be an absolute local path")]
    RootNotAbsolute,
    /// A controlled state/output root is equal to, contains, or is contained by the input root.
    #[error("controlled state root overlaps the user-authorized input root")]
    RootOverlap,
    /// Root paths or relative input references contain unsafe components.
    #[error("input path contains an unsafe component")]
    UnsafeComponent,
    /// The platform cannot enforce the required no-follow semantics.
    #[error("platform does not support hardened local input capabilities")]
    UnsupportedPlatform,
    /// A root, intermediate directory, or file is a symlink or reparse point.
    #[error("input path contains a symlink or reparse point")]
    SymlinkOrReparsePoint,
    /// The resolved input does not name a regular file.
    #[error("input capability does not name a regular file")]
    NotRegularFile,
    /// A retained root, directory, name, or file identity changed.
    #[error("input capability identity changed")]
    IdentityChanged,
    /// A nonzero byte limit is required and must fit in local address space.
    #[error("input byte limit is invalid")]
    InvalidByteLimit,
    /// The file exceeded its admitted byte ceiling.
    #[error("input exceeds the admitted byte limit of {max} bytes")]
    ByteLimitExceeded {
        /// Exact configured ceiling.
        max: u64,
    },
    /// The exact admitted input buffer could not be reserved without exceeding its ceiling.
    #[error("input buffer allocation failed within the admitted byte limit")]
    AllocationFailed,
    /// Another process holds an incompatible cooperative file lock.
    #[error("input file is busy")]
    FileBusy,
    /// The platform could not establish the required shared-read lock.
    #[error("input stability lock is unavailable: {source}")]
    StabilityLockUnavailable {
        /// Underlying path-redacted lock failure.
        #[source]
        source: std::io::Error,
    },
    /// Two bounded passes over the retained handle produced different bytes.
    #[error("input changed during its admitted read")]
    ContentChanged,
    /// A capability-relative filesystem operation failed.
    #[error("local input operation failed: {source}")]
    Io {
        /// Underlying path-redacted operating-system failure.
        #[source]
        source: std::io::Error,
    },
}

impl UserAuthorizedInputRoot {
    /// Opens an existing directory selected by the user without following any path component.
    ///
    /// The ambient path is accepted only at this explicit composition boundary. It must be an
    /// absolute local path without parent traversal. Every component after the filesystem root is
    /// opened relative to the preceding directory with no-follow semantics.
    ///
    /// # Errors
    ///
    /// Rejects relative, parent-traversing, symlinked, reparsed, non-directory, or unsupported
    /// roots and path-redacted I/O failures.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, InputFileError> {
        let display_root = path.as_ref().to_path_buf();
        let directory = open_absolute_directory(&display_root)?;
        let metadata = directory.dir_metadata().map_err(InputFileError::io)?;
        if !metadata.is_dir() || is_windows_reparse(&metadata) {
            return Err(InputFileError::SymlinkOrReparsePoint);
        }
        let identity = filesystem_identity(&metadata);
        Ok(Self {
            inner: Arc::new(InputRootInner {
                display_root,
                identity,
                directory,
                ownership_binding: Arc::new(InputRootOwnershipBinding {
                    identity_digest: ownership::root_identity_digest(identity),
                }),
            }),
        })
    }

    /// Resolves one existing regular file below this root without following any component.
    ///
    /// The returned capability is deliberately non-cloneable and must be consumed to open the
    /// file. Repeated reads require a fresh resolution and therefore a fresh path-chain check.
    ///
    /// # Errors
    ///
    /// Rejects absolute, empty, parent-traversing, non-portable, over-deep, symlinked, reparsed,
    /// replaced, or non-regular inputs.
    pub fn resolve(
        &self,
        relative: impl AsRef<Path>,
    ) -> Result<InputFileCapability, InputFileError> {
        self.inner.validate_root()?;
        let components = validate_relative_reference(relative.as_ref())?;
        let (parent, parent_identities) =
            walk_relative_parents(&self.inner.directory, &components)?;
        let file_name = components.last().ok_or(InputFileError::UnsafeComponent)?;
        let metadata = nofollow_regular_metadata(&parent, file_name)?;
        let identity = InputFileIdentity::from_metadata(&metadata)?;
        self.inner.validate_root()?;
        Ok(InputFileCapability {
            root: Arc::clone(&self.inner),
            components,
            parent_identities,
            identity,
        })
    }

    /// Verifies that a controlled writable root is disjoint from this read-only input capability.
    ///
    /// The candidate may name a not-yet-created final directory, but its nearest existing parent
    /// must be resolvable without lexical parent traversal. Both containment directions are
    /// rejected so a manifest can never name authority files as input and authority publication
    /// can never occur through an ancestor of the input capability.
    ///
    /// # Errors
    ///
    /// Rejects relative, parent-traversing, unresolvable, or overlapping roots.
    pub fn ensure_disjoint_root(&self, candidate: impl AsRef<Path>) -> Result<(), InputFileError> {
        self.inner.validate_root()?;
        let input = canonical_candidate(&self.inner.display_root)?;
        let candidate = canonical_candidate(candidate.as_ref())?;
        if candidate.starts_with(&input) || input.starts_with(&candidate) {
            return Err(InputFileError::RootOverlap);
        }
        Ok(())
    }
}

fn canonical_candidate(path: &Path) -> Result<PathBuf, InputFileError> {
    if !path.is_absolute()
        || path
            .components()
            .any(|component| component == Component::ParentDir)
    {
        return Err(InputFileError::RootNotAbsolute);
    }
    let mut missing = Vec::new();
    let mut existing = path;
    while !existing.exists() {
        let name = existing
            .file_name()
            .ok_or(InputFileError::UnsafeComponent)?;
        missing.push(name.to_os_string());
        existing = existing.parent().ok_or(InputFileError::UnsafeComponent)?;
    }
    let mut canonical = existing.canonicalize().map_err(InputFileError::io)?;
    for component in missing.into_iter().rev() {
        canonical.push(component);
    }
    Ok(canonical)
}

impl InputRootInner {
    fn validate_root(&self) -> Result<(), InputFileError> {
        let retained = self.directory.dir_metadata().map_err(InputFileError::io)?;
        if !retained.is_dir()
            || filesystem_identity(&retained) != self.identity
            || is_windows_reparse(&retained)
        {
            return Err(InputFileError::IdentityChanged);
        }
        let reopened = open_absolute_directory(&self.display_root)
            .map_err(|_| InputFileError::IdentityChanged)?;
        let displayed = reopened.dir_metadata().map_err(InputFileError::io)?;
        if !displayed.is_dir()
            || filesystem_identity(&displayed) != self.identity
            || is_windows_reparse(&displayed)
        {
            return Err(InputFileError::IdentityChanged);
        }
        Ok(())
    }
}

impl InputFileCapability {
    /// Opens this exact file once under a nonzero byte ceiling.
    ///
    /// This consumes the resolved capability. The path chain, named file, and opened handle must
    /// all still match the identities retained at resolution time.
    ///
    /// # Errors
    ///
    /// Rejects invalid limits, growth beyond the limit, replacement, symlinks/reparse points,
    /// non-regular files, and path-redacted I/O failures.
    pub fn open_bounded(self, maximum_bytes: u64) -> Result<VerifiedInputFile, InputFileError> {
        if maximum_bytes == 0
            || maximum_bytes == u64::MAX
            || usize::try_from(maximum_bytes).is_err()
        {
            return Err(InputFileError::InvalidByteLimit);
        }
        self.root.validate_root()?;
        let (parent, identities) = walk_relative_parents(&self.root.directory, &self.components)?;
        if identities != self.parent_identities {
            return Err(InputFileError::IdentityChanged);
        }
        let file_name = self
            .components
            .last()
            .ok_or(InputFileError::UnsafeComponent)?;
        let named = nofollow_regular_metadata(&parent, file_name)?;
        if InputFileIdentity::from_metadata(&named)? != self.identity {
            return Err(InputFileError::IdentityChanged);
        }
        if named.len() > maximum_bytes {
            return Err(InputFileError::ByteLimitExceeded { max: maximum_bytes });
        }
        let mut options = OpenOptions::new();
        options.read(true).follow(FollowSymlinks::No);
        configure_windows_nofollow(&mut options);
        let file = parent
            .open_with(file_name, &options)
            .map_err(classify_nofollow_error)?
            .into_std();
        let opened = file.metadata().map_err(InputFileError::io)?;
        if !opened.is_file() || is_windows_std_reparse(&opened) {
            return Err(InputFileError::NotRegularFile);
        }
        if InputFileIdentity::from_std_metadata(&opened)? != self.identity {
            return Err(InputFileError::IdentityChanged);
        }
        self.root.validate_root()?;
        Ok(VerifiedInputFile {
            root: self.root,
            components: self.components,
            parent_identities: self.parent_identities,
            identity: self.identity,
            file,
            maximum_bytes,
        })
    }
}

impl VerifiedInputFile {
    /// Revalidates the retained root, every parent identity, the name, and opened handle.
    ///
    /// # Errors
    ///
    /// Returns [`InputFileError::IdentityChanged`] for replacement or mutation and a typed
    /// confinement/I/O error for an unsafe path transition.
    pub fn validate_unchanged(&self) -> Result<(), InputFileError> {
        self.root.validate_root()?;
        let (parent, identities) = walk_relative_parents(&self.root.directory, &self.components)?;
        if identities != self.parent_identities {
            return Err(InputFileError::IdentityChanged);
        }
        let file_name = self
            .components
            .last()
            .ok_or(InputFileError::UnsafeComponent)?;
        let named = nofollow_regular_metadata(&parent, file_name)?;
        let opened = self.file.metadata().map_err(InputFileError::io)?;
        if InputFileIdentity::from_metadata(&named)? != self.identity
            || InputFileIdentity::from_std_metadata(&opened)? != self.identity
        {
            return Err(InputFileError::IdentityChanged);
        }
        self.root.validate_root()
    }

    /// Consumes the opened capability, acquires a nonblocking shared lock, performs two bounded
    /// digest passes, and releases exact bytes only after identity revalidation.
    ///
    /// # Errors
    ///
    /// Rejects short reads, concurrent mutation/replacement, limit overflow, and I/O failures.
    pub fn read_bounded(self) -> Result<BoundedInput, InputFileError> {
        match self.read_bounded_inner(|_| Ok::<(), Infallible>(())) {
            Ok(input) => Ok(input),
            Err(BoundedReadError::Input(error)) => Err(error),
            Err(BoundedReadError::Control(never)) => match never {},
        }
    }

    /// Performs the exact bounded read with caller-owned cooperative control.
    ///
    /// No individual filesystem read exceeds 64 KiB. Control is checked before and after each
    /// read in both digest passes, around the growth probe, and before identity validation and
    /// release. A control failure drops the retained handle and stability lock normally.
    ///
    /// # Errors
    ///
    /// Returns [`ControlledInputFileError::Control`] without changing its cancellation, deadline,
    /// or unavailable classification. All other failures retain their exact [`InputFileError`].
    pub fn read_bounded_with_control(
        self,
        control: &dyn InputReadControl,
    ) -> Result<BoundedInput, ControlledInputFileError> {
        match self.read_bounded_inner(|checkpoint| control.checkpoint(checkpoint)) {
            Ok(input) => Ok(input),
            Err(BoundedReadError::Input(error)) => Err(ControlledInputFileError::Input(error)),
            Err(BoundedReadError::Control(error)) => Err(ControlledInputFileError::Control(error)),
        }
    }

    fn read_bounded_inner<E>(
        mut self,
        mut checkpoint: impl FnMut(InputReadCheckpoint) -> Result<(), E>,
    ) -> Result<BoundedInput, BoundedReadError<E>> {
        checkpoint(InputReadCheckpoint::BeforeRead).map_err(BoundedReadError::Control)?;
        fs2::FileExt::try_lock_shared(&self.file)
            .map_err(|source| {
                if source.kind() == std::io::ErrorKind::WouldBlock {
                    InputFileError::FileBusy
                } else {
                    InputFileError::StabilityLockUnavailable { source }
                }
            })
            .map_err(BoundedReadError::Input)?;
        let read_ceiling = self
            .maximum_bytes
            .checked_add(1)
            .ok_or(InputFileError::InvalidByteLimit)
            .map_err(BoundedReadError::Input)?;
        let exact_length = usize::try_from(self.identity.size_bytes)
            .map_err(|_| BoundedReadError::Input(InputFileError::InvalidByteLimit))?;
        let mut bytes = Vec::new();
        reserve_fixed_input_buffer(&mut bytes, exact_length, self.maximum_bytes)
            .map_err(BoundedReadError::Input)?;
        let mut first_digest = Sha256::new();
        let mut first_length = 0_u64;
        for chunk in bytes.chunks_mut(INPUT_READ_CHUNK_BYTES) {
            checkpoint(InputReadCheckpoint::BeforeReadChunk {
                pass: InputReadPass::Primary,
                offset_bytes: first_length,
            })
            .map_err(BoundedReadError::Control)?;
            match self.file.read_exact(chunk) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Err(BoundedReadError::Input(InputFileError::IdentityChanged));
                }
                Err(error) => {
                    return Err(BoundedReadError::Input(InputFileError::io(error)));
                }
            }
            first_digest.update(&*chunk);
            first_length = first_length
                .checked_add(
                    u64::try_from(chunk.len())
                        .map_err(|_| BoundedReadError::Input(InputFileError::InvalidByteLimit))?,
                )
                .ok_or(InputFileError::InvalidByteLimit)
                .map_err(BoundedReadError::Input)?;
            checkpoint(InputReadCheckpoint::AfterReadChunk {
                pass: InputReadPass::Primary,
                offset_bytes: first_length,
            })
            .map_err(BoundedReadError::Control)?;
        }
        checkpoint(InputReadCheckpoint::BeforeGrowthProbe).map_err(BoundedReadError::Control)?;
        let mut growth_probe = [0_u8; 1];
        let growth = loop {
            match self.file.read(&mut growth_probe) {
                Ok(read) => break read,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => {
                    return Err(BoundedReadError::Input(InputFileError::io(error)));
                }
            }
        };
        checkpoint(InputReadCheckpoint::AfterGrowthProbe).map_err(BoundedReadError::Control)?;
        if growth != 0 {
            let current = self
                .file
                .metadata()
                .map_err(InputFileError::io)
                .map_err(BoundedReadError::Input)?;
            if self.identity.size_bytes >= self.maximum_bytes || current.len() > self.maximum_bytes
            {
                return Err(BoundedReadError::Input(InputFileError::ByteLimitExceeded {
                    max: self.maximum_bytes,
                }));
            }
            return Err(BoundedReadError::Input(InputFileError::IdentityChanged));
        }
        let first_digest = first_digest.finalize();
        self.file
            .seek(SeekFrom::Start(0))
            .map_err(InputFileError::io)
            .map_err(BoundedReadError::Input)?;
        let mut second_digest = Sha256::new();
        let mut second_length = 0_u64;
        let mut buffer = [0_u8; INPUT_READ_CHUNK_BYTES];
        loop {
            let remaining = read_ceiling.saturating_sub(second_length);
            if remaining == 0 {
                break;
            }
            let requested = usize::try_from(remaining.min(buffer.len() as u64))
                .map_err(|_| BoundedReadError::Input(InputFileError::InvalidByteLimit))?;
            checkpoint(InputReadCheckpoint::BeforeReadChunk {
                pass: InputReadPass::Verification,
                offset_bytes: second_length,
            })
            .map_err(BoundedReadError::Control)?;
            let read = loop {
                match self.file.read(&mut buffer[..requested]) {
                    Ok(read) => break read,
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(error) => {
                        return Err(BoundedReadError::Input(InputFileError::io(error)));
                    }
                }
            };
            if read != 0 {
                second_digest.update(&buffer[..read]);
                second_length =
                    second_length
                        .checked_add(u64::try_from(read).map_err(|_| {
                            BoundedReadError::Input(InputFileError::InvalidByteLimit)
                        })?)
                        .ok_or(InputFileError::InvalidByteLimit)
                        .map_err(BoundedReadError::Input)?;
            }
            checkpoint(InputReadCheckpoint::AfterReadChunk {
                pass: InputReadPass::Verification,
                offset_bytes: second_length,
            })
            .map_err(BoundedReadError::Control)?;
            if read == 0 {
                break;
            }
        }
        let second_digest = second_digest.finalize();
        if second_length > self.maximum_bytes {
            return Err(BoundedReadError::Input(InputFileError::ByteLimitExceeded {
                max: self.maximum_bytes,
            }));
        }
        if second_length != self.identity.size_bytes || first_digest != second_digest {
            return Err(BoundedReadError::Input(InputFileError::ContentChanged));
        }
        checkpoint(InputReadCheckpoint::BeforeIdentityRevalidation)
            .map_err(BoundedReadError::Control)?;
        self.validate_unchanged().map_err(BoundedReadError::Input)?;
        checkpoint(InputReadCheckpoint::BeforeRelease).map_err(BoundedReadError::Control)?;
        Ok(BoundedInput {
            bytes: bytes.into_boxed_slice(),
            identity: self.identity,
            digest: EvidenceDigest::new(DigestAlgorithm::Sha256, first_digest.into()),
            root_ownership_binding: Arc::clone(&self.root.ownership_binding),
        })
    }
}

fn reserve_fixed_input_buffer(
    bytes: &mut Vec<u8>,
    exact_length: usize,
    maximum_bytes: u64,
) -> Result<(), InputFileError> {
    let exact_u64 = u64::try_from(exact_length).map_err(|_| InputFileError::InvalidByteLimit)?;
    if exact_u64 > maximum_bytes {
        return Err(InputFileError::ByteLimitExceeded { max: maximum_bytes });
    }
    let maximum_capacity =
        usize::try_from(maximum_bytes).map_err(|_| InputFileError::InvalidByteLimit)?;
    bytes
        .try_reserve_exact(exact_length)
        .map_err(|_| InputFileError::AllocationFailed)?;
    if bytes.capacity() > maximum_capacity {
        return Err(InputFileError::AllocationFailed);
    }
    bytes.resize(exact_length, 0);
    Ok(())
}

impl InputFileIdentity {
    fn from_metadata(metadata: &cap_std::fs::Metadata) -> Result<Self, InputFileError> {
        if !metadata.is_file() || is_windows_reparse(metadata) {
            return Err(InputFileError::NotRegularFile);
        }
        Ok(Self {
            filesystem: filesystem_identity(metadata),
            size_bytes: metadata.len(),
            modified: metadata.modified().ok(),
        })
    }

    fn from_std_metadata(metadata: &std::fs::Metadata) -> Result<Self, InputFileError> {
        if !metadata.is_file() || is_windows_std_reparse(metadata) {
            return Err(InputFileError::NotRegularFile);
        }
        Ok(Self {
            filesystem: filesystem_std_identity(metadata),
            size_bytes: metadata.len(),
            modified: metadata.modified().ok().map(SystemTime::from_std),
        })
    }

    /// Returns the exact pre-read file length.
    pub const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }
}

impl BoundedInput {
    /// Returns exact bytes read from the retained handle.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns the retained opaque file identity.
    pub const fn identity(&self) -> &InputFileIdentity {
        &self.identity
    }

    /// Returns SHA-256 evidence verified by two bounded passes over the retained handle.
    pub const fn digest(&self) -> EvidenceDigest {
        self.digest
    }

    /// Consumes the bounded input into its exact bytes.
    pub fn into_bytes(self) -> Box<[u8]> {
        self.bytes
    }
}

impl InputFileError {
    fn io(source: std::io::Error) -> Self {
        Self::Io { source }
    }
}

impl fmt::Debug for UserAuthorizedInputRoot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("UserAuthorizedInputRoot([RETAINED ROOT CAPABILITY])")
    }
}

impl fmt::Debug for InputRootInner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("InputRootInner([RETAINED ROOT CAPABILITY])")
    }
}

impl fmt::Debug for InputFileCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("InputFileCapability([ONE-SHOT FILE CAPABILITY])")
    }
}

impl fmt::Debug for VerifiedInputFile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("VerifiedInputFile([VERIFIED ONE-SHOT HANDLE])")
    }
}

impl fmt::Debug for InputFileIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InputFileIdentity")
            .field("identity", &"[REDACTED]")
            .field("size_bytes", &self.size_bytes)
            .finish()
    }
}

impl fmt::Debug for BoundedInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BoundedInput")
            .field("bytes", &format_args!("[{} BYTES]", self.bytes.len()))
            .field("identity", &self.identity)
            .finish()
    }
}

fn validate_relative_reference(path: &Path) -> Result<Vec<OsString>, InputFileError> {
    if path.is_absolute() {
        return Err(InputFileError::UnsafeComponent);
    }
    let mut components = Vec::new();
    for component in path.components() {
        let Component::Normal(component) = component else {
            return Err(InputFileError::UnsafeComponent);
        };
        if component.is_empty() || component.as_encoded_bytes().len() > MAX_COMPONENT_BYTES {
            return Err(InputFileError::UnsafeComponent);
        }
        components.push(component.to_os_string());
        if components.len() > MAX_INPUT_DEPTH {
            return Err(InputFileError::UnsafeComponent);
        }
    }
    if components.is_empty() {
        return Err(InputFileError::UnsafeComponent);
    }
    Ok(components)
}

fn walk_relative_parents(
    root: &Dir,
    components: &[OsString],
) -> Result<(Dir, Vec<FileSystemIdentity>), InputFileError> {
    let mut current = root.try_clone().map_err(InputFileError::io)?;
    let mut identities = Vec::with_capacity(components.len().saturating_sub(1));
    for component in components.iter().take(components.len().saturating_sub(1)) {
        let metadata = current
            .symlink_metadata(component)
            .map_err(InputFileError::io)?;
        if metadata.file_type().is_symlink() || is_windows_reparse(&metadata) {
            return Err(InputFileError::SymlinkOrReparsePoint);
        }
        if !metadata.is_dir() {
            return Err(InputFileError::UnsafeComponent);
        }
        let next = current
            .open_dir_nofollow(component)
            .map_err(classify_nofollow_error)?;
        let opened = next.dir_metadata().map_err(InputFileError::io)?;
        let identity = filesystem_identity(&opened);
        if !opened.is_dir()
            || is_windows_reparse(&opened)
            || identity != filesystem_identity(&metadata)
        {
            return Err(InputFileError::IdentityChanged);
        }
        identities.push(identity);
        current = next;
    }
    Ok((current, identities))
}

fn nofollow_regular_metadata(
    parent: &Dir,
    file_name: &OsString,
) -> Result<cap_std::fs::Metadata, InputFileError> {
    let metadata = parent
        .symlink_metadata(file_name)
        .map_err(InputFileError::io)?;
    if metadata.file_type().is_symlink() || is_windows_reparse(&metadata) {
        return Err(InputFileError::SymlinkOrReparsePoint);
    }
    if !metadata.is_file() {
        return Err(InputFileError::NotRegularFile);
    }
    Ok(metadata)
}

fn filesystem_identity(metadata: &cap_std::fs::Metadata) -> FileSystemIdentity {
    FileSystemIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    }
}

fn filesystem_std_identity(metadata: &std::fs::Metadata) -> FileSystemIdentity {
    FileSystemIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    }
}

fn classify_nofollow_error(source: std::io::Error) -> InputFileError {
    if matches!(source.raw_os_error(), Some(libc::ELOOP)) {
        InputFileError::SymlinkOrReparsePoint
    } else {
        InputFileError::io(source)
    }
}

#[cfg(unix)]
fn open_absolute_directory(path: &Path) -> Result<Dir, InputFileError> {
    if !path.is_absolute() {
        return Err(InputFileError::RootNotAbsolute);
    }
    let mut components = path.components();
    if !matches!(components.next(), Some(Component::RootDir)) {
        return Err(InputFileError::RootNotAbsolute);
    }
    let mut current =
        Dir::open_ambient_dir(Path::new("/"), ambient_authority()).map_err(InputFileError::io)?;
    for component in components {
        let Component::Normal(component) = component else {
            return Err(InputFileError::UnsafeComponent);
        };
        let metadata = current
            .symlink_metadata(component)
            .map_err(InputFileError::io)?;
        if metadata.file_type().is_symlink() {
            return Err(InputFileError::SymlinkOrReparsePoint);
        }
        if !metadata.is_dir() {
            return Err(InputFileError::UnsafeComponent);
        }
        let next = current
            .open_dir_nofollow(component)
            .map_err(classify_nofollow_error)?;
        let opened = next.dir_metadata().map_err(InputFileError::io)?;
        if !opened.is_dir() || filesystem_identity(&opened) != filesystem_identity(&metadata) {
            return Err(InputFileError::IdentityChanged);
        }
        current = next;
    }
    Ok(current)
}

#[cfg(windows)]
fn open_absolute_directory(path: &Path) -> Result<Dir, InputFileError> {
    use std::path::Prefix;

    if !path.is_absolute() {
        return Err(InputFileError::RootNotAbsolute);
    }
    let mut components = path.components();
    let prefix = match components.next() {
        Some(Component::Prefix(prefix)) => match prefix.kind() {
            Prefix::Disk(letter) | Prefix::VerbatimDisk(letter) => letter,
            _ => return Err(InputFileError::UnsafeComponent),
        },
        _ => return Err(InputFileError::RootNotAbsolute),
    };
    if !matches!(components.next(), Some(Component::RootDir)) {
        return Err(InputFileError::RootNotAbsolute);
    }
    let anchor = PathBuf::from(format!("{}:\\", char::from(prefix)));
    let mut current =
        Dir::open_ambient_dir(anchor, ambient_authority()).map_err(InputFileError::io)?;
    for component in components {
        let Component::Normal(component) = component else {
            return Err(InputFileError::UnsafeComponent);
        };
        let metadata = current
            .symlink_metadata(component)
            .map_err(InputFileError::io)?;
        if metadata.file_type().is_symlink() || is_windows_reparse(&metadata) {
            return Err(InputFileError::SymlinkOrReparsePoint);
        }
        if !metadata.is_dir() {
            return Err(InputFileError::UnsafeComponent);
        }
        let next = current
            .open_dir_nofollow(component)
            .map_err(classify_nofollow_error)?;
        let opened = next.dir_metadata().map_err(InputFileError::io)?;
        if !opened.is_dir()
            || is_windows_reparse(&opened)
            || filesystem_identity(&opened) != filesystem_identity(&metadata)
        {
            return Err(InputFileError::IdentityChanged);
        }
        current = next;
    }
    Ok(current)
}

#[cfg(not(any(unix, windows)))]
fn open_absolute_directory(_path: &Path) -> Result<Dir, InputFileError> {
    Err(InputFileError::UnsupportedPlatform)
}

#[cfg(windows)]
fn is_windows_reparse(metadata: &cap_std::fs::Metadata) -> bool {
    use cap_std::fs::MetadataExt as _;

    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
    metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}

#[cfg(not(windows))]
const fn is_windows_reparse(_metadata: &cap_std::fs::Metadata) -> bool {
    false
}

#[cfg(windows)]
fn is_windows_std_reparse(metadata: &std::fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt as _;

    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
    metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}

#[cfg(not(windows))]
const fn is_windows_std_reparse(_metadata: &std::fs::Metadata) -> bool {
    false
}

#[cfg(windows)]
fn configure_windows_nofollow(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;

    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    options
        .share_mode(FILE_SHARE_READ)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
}

#[cfg(not(windows))]
fn configure_windows_nofollow(_options: &mut OpenOptions) {}

#[cfg(test)]
mod tests {
    use std::fs;

    use static_assertions::assert_not_impl_any;

    use super::*;

    assert_not_impl_any!(UserOwnedInputAuthority: Clone, serde::Serialize);
    assert_not_impl_any!(UserOwnedInputEvidence: Clone, serde::Serialize);

    #[test]
    fn fixed_input_reservation_rejects_before_capacity_growth() {
        let mut bytes = Vec::new();
        let initial_capacity = bytes.capacity();
        assert!(matches!(
            reserve_fixed_input_buffer(&mut bytes, 2, 1),
            Err(InputFileError::ByteLimitExceeded { max: 1 })
        ));
        assert_eq!(bytes.capacity(), initial_capacity);
    }

    #[test]
    fn user_owned_manifest_evidence_is_exact_stable_and_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let parent = tempfile::tempdir()?;
        let root_path = parent.path().join("authorized-input");
        fs::create_dir(&root_path)?;
        fs::write(root_path.join("manifest.json"), br#"{"schema_version":3}"#)?;
        let root_path = fs::canonicalize(root_path)?;

        let (root, authority) = UserAuthorizedInputRoot::open_with_ownership_authority(&root_path)?;
        let ordinary_adapter_root = root.clone();
        let manifest = ordinary_adapter_root
            .resolve("manifest.json")?
            .open_bounded(1_024)?
            .read_bounded()?;
        let expected_manifest_digest = manifest.digest();
        let evidence = authority.issue_manifest_evidence(&manifest)?;
        assert_eq!(evidence.manifest_digest(), expected_manifest_digest);
        assert_eq!(
            evidence
                .root_identity_digest()
                .evidence_digest()
                .algorithm(),
            DigestAlgorithm::Sha256
        );
        assert_eq!(
            format!("{:?}", evidence.root_identity_digest()),
            "UserOwnedInputRootIdentityDigest([REDACTED])"
        );
        assert_eq!(
            format!("{evidence:?}"),
            "UserOwnedInputEvidence([REDACTED])"
        );

        let (reopened_root, reopened_authority) =
            UserAuthorizedInputRoot::open_with_ownership_authority(&root_path)?;
        let reopened_manifest = reopened_root
            .resolve("manifest.json")?
            .open_bounded(1_024)?
            .read_bounded()?;
        let reopened_evidence = reopened_authority.issue_manifest_evidence(&reopened_manifest)?;
        assert_eq!(
            reopened_evidence.root_identity_digest(),
            evidence.root_identity_digest()
        );
        assert!(matches!(
            reopened_authority.issue_manifest_evidence(&manifest),
            Err(InputFileError::IdentityChanged)
        ));

        #[cfg(unix)]
        {
            let retained_path = parent.path().join("retained-input");
            fs::rename(&root_path, &retained_path)?;
            fs::create_dir(&root_path)?;
            fs::write(root_path.join("manifest.json"), br#"{"schema_version":3}"#)?;
            assert!(matches!(
                authority.issue_manifest_evidence(&manifest),
                Err(InputFileError::IdentityChanged)
            ));

            let (replacement_root, replacement_authority) =
                UserAuthorizedInputRoot::open_with_ownership_authority(&root_path)?;
            let replacement_manifest = replacement_root
                .resolve("manifest.json")?
                .open_bounded(1_024)?
                .read_bounded()?;
            let replacement_evidence =
                replacement_authority.issue_manifest_evidence(&replacement_manifest)?;
            assert_ne!(
                replacement_evidence.root_identity_digest(),
                evidence.root_identity_digest()
            );
        }

        Ok(())
    }
}
