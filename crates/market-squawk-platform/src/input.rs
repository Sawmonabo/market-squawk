//! One-shot, no-follow capabilities for explicitly user-authorized local input roots.

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

const MAX_INPUT_DEPTH: usize = 64;
const MAX_COMPONENT_BYTES: usize = 255;

/// A no-follow capability rooted at one directory explicitly selected by the local user.
#[derive(Clone)]
pub struct UserAuthorizedInputRoot {
    inner: Arc<InputRootInner>,
}

struct InputRootInner {
    display_root: PathBuf,
    directory: Dir,
    identity: FileSystemIdentity,
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
}

/// Failure to prepare, resolve, open, or consume a local input capability.
#[derive(Debug, Error)]
pub enum InputFileError {
    /// Input roots must be explicit absolute local paths.
    #[error("user-authorized input root must be an absolute local path")]
    RootNotAbsolute,
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
        Ok(Self {
            inner: Arc::new(InputRootInner {
                display_root,
                identity: filesystem_identity(&metadata),
                directory,
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
    pub fn read_bounded(mut self) -> Result<BoundedInput, InputFileError> {
        fs2::FileExt::try_lock_shared(&self.file).map_err(|source| {
            if source.kind() == std::io::ErrorKind::WouldBlock {
                InputFileError::FileBusy
            } else {
                InputFileError::StabilityLockUnavailable { source }
            }
        })?;
        let read_ceiling = self
            .maximum_bytes
            .checked_add(1)
            .ok_or(InputFileError::InvalidByteLimit)?;
        let capacity = usize::try_from(self.identity.size_bytes)
            .map_err(|_| InputFileError::InvalidByteLimit)?;
        let mut bytes = Vec::with_capacity(capacity);
        self.file
            .by_ref()
            .take(read_ceiling)
            .read_to_end(&mut bytes)
            .map_err(InputFileError::io)?;
        if u64::try_from(bytes.len()).map_or(true, |length| length > self.maximum_bytes) {
            return Err(InputFileError::ByteLimitExceeded {
                max: self.maximum_bytes,
            });
        }
        if u64::try_from(bytes.len()).ok() != Some(self.identity.size_bytes) {
            return Err(InputFileError::IdentityChanged);
        }
        let first_digest = Sha256::digest(&bytes);
        self.file
            .seek(SeekFrom::Start(0))
            .map_err(InputFileError::io)?;
        let mut second_digest = Sha256::new();
        let mut second_length = 0_u64;
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let remaining = read_ceiling.saturating_sub(second_length);
            if remaining == 0 {
                break;
            }
            let requested = usize::try_from(remaining.min(buffer.len() as u64))
                .map_err(|_| InputFileError::InvalidByteLimit)?;
            let read = self
                .file
                .read(&mut buffer[..requested])
                .map_err(InputFileError::io)?;
            if read == 0 {
                break;
            }
            second_digest.update(&buffer[..read]);
            second_length = second_length
                .checked_add(u64::try_from(read).map_err(|_| InputFileError::InvalidByteLimit)?)
                .ok_or(InputFileError::InvalidByteLimit)?;
        }
        let second_digest = second_digest.finalize();
        if second_length > self.maximum_bytes {
            return Err(InputFileError::ByteLimitExceeded {
                max: self.maximum_bytes,
            });
        }
        if second_length != self.identity.size_bytes || first_digest != second_digest {
            return Err(InputFileError::ContentChanged);
        }
        self.validate_unchanged()?;
        Ok(BoundedInput {
            bytes: bytes.into_boxed_slice(),
            identity: self.identity,
            digest: EvidenceDigest::new(DigestAlgorithm::Sha256, first_digest.into()),
        })
    }
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
