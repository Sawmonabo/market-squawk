//! Crash-safe local persistence for source authority state.

use std::fmt;
use std::fs;
use std::io::{self, Read, Write};
use std::path::Path;
use std::sync::Mutex;

use cap_fs_ext::{FollowSymlinks, MetadataExt as _, OpenOptionsFollowExt};
#[cfg(not(any(unix, windows)))]
use cap_std::ambient_authority;
use cap_std::fs::{Dir, OpenOptions};
use fs2::FileExt as _;
use sha2::{Digest, Sha256};
use thiserror::Error;
use zeroize::Zeroize;

const CANONICAL_FILE: &str = "authority-state.bin";
const TEMP_FILE: &str = ".authority-state.tmp";
const LOCK_FILE: &str = ".authority-state.lock";
const MAX_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;
const MAGIC: [u8; 8] = *b"MSQAUTH\0";
const VERSION: u16 = 1;
const LENGTH_BYTES: usize = size_of::<u64>();
const DIGEST_BYTES: usize = 32;
const HEADER_BYTES: usize = MAGIC.len() + size_of::<u16>() + LENGTH_BYTES + DIGEST_BYTES;
const MAX_ENVELOPE_BYTES: usize = HEADER_BYTES + MAX_PAYLOAD_BYTES;

/// A capability-confined, exclusively owned authority-state store.
///
/// The store retains an exclusive process lock for its lifetime. State is
/// checksummed, written to a same-directory temporary file, synchronized, and
/// atomically installed without deleting the prior canonical state first.
/// Neither an orphan temporary file nor a corrupt canonical file is authority.
pub struct LocalAuthorityStateStore {
    directory: Dir,
    _lock: fs::File,
    writer: Mutex<()>,
}

impl fmt::Debug for LocalAuthorityStateStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalAuthorityStateStore")
            .field("directory", &"[redacted capability]")
            .field("lock", &"[redacted handle]")
            .finish()
    }
}

/// Fail-closed errors produced by [`LocalAuthorityStateStore`].
#[derive(Debug, Error)]
pub enum LocalAuthorityStateStoreError {
    /// The configured root is a symbolic link, reparse point, or non-directory.
    #[error("authority-state root is not a safe directory")]
    UnsafeRoot,
    /// A reserved authority-state name is not a regular file.
    #[error("authority-state file has an unsafe or ambiguous type")]
    UnsafeFileType,
    /// Another store owner holds the lifetime lock.
    #[error("authority-state store is already locked")]
    AlreadyLocked,
    /// The serialized state exceeds the configured payload bound.
    #[error("authority-state payload is {bytes} bytes; maximum is {maximum}")]
    PayloadTooLarge {
        /// Observed payload size.
        bytes: usize,
        /// Maximum accepted payload size.
        maximum: usize,
    },
    /// The canonical envelope exceeds its bounded on-disk representation.
    #[error("authority-state envelope is {bytes} bytes; maximum is {maximum}")]
    EnvelopeTooLarge {
        /// Observed envelope size.
        bytes: u64,
        /// Maximum accepted envelope size.
        maximum: u64,
    },
    /// The envelope is truncated, inconsistent, unsupported, or fails its digest.
    #[error("authority-state envelope is corrupt or unsupported")]
    CorruptEnvelope,
    /// A bounded buffer could not be allocated.
    #[error("authority-state bounded allocation failed")]
    Allocation,
    /// In-process writer serialization was poisoned by an abnormal unwind.
    #[error("authority-state writer serialization is unavailable")]
    WriterUnavailable,
    /// The platform cannot safely replace an existing file atomically.
    #[error("atomic authority-state replacement is unsupported on this platform")]
    AtomicReplaceUnsupported,
    /// A post-installation read did not reproduce the submitted payload.
    #[error("installed authority state failed canonical verification")]
    VerificationFailed,
    /// A filesystem operation failed without exposing path or payload data.
    #[error("authority-state filesystem operation failed during {operation}")]
    Io {
        /// Bounded operation identifier.
        operation: &'static str,
        /// Underlying operating-system error.
        #[source]
        source: io::Error,
    },
}

impl LocalAuthorityStateStore {
    /// Opens or creates `root` and acquires its exclusive lifetime lock.
    ///
    /// All state access after this boundary is relative to an open directory
    /// capability. Reserved names are opened without following a final
    /// symbolic link.
    ///
    /// # Errors
    ///
    /// Returns [`LocalAuthorityStateStoreError::UnsafeRoot`] for a symbolic-link
    /// or non-directory root, [`LocalAuthorityStateStoreError::UnsafeFileType`]
    /// for unsafe reserved entries, and
    /// [`LocalAuthorityStateStoreError::AlreadyLocked`] when another owner is
    /// active. Filesystem and allocation failures are also returned.
    pub fn try_open(root: impl AsRef<Path>) -> Result<Self, LocalAuthorityStateStoreError> {
        let directory = open_root(root.as_ref())?;
        reject_unsafe_entry_if_present(&directory, TEMP_FILE)?;
        reject_unsafe_entry_if_present(&directory, LOCK_FILE)?;

        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true);
        options.follow(FollowSymlinks::No);
        set_private_creation_mode(&mut options);
        let lock = directory
            .open_with(LOCK_FILE, &options)
            .map_err(|source| io_error("open lock", source))?;
        let lock_metadata = lock
            .metadata()
            .map_err(|source| io_error("inspect lock", source))?;
        if !is_unambiguous_regular(&lock_metadata) {
            return Err(LocalAuthorityStateStoreError::UnsafeFileType);
        }
        let lock = lock.into_std();
        lock.try_lock_exclusive().map_err(|source| {
            if source.kind() == io::ErrorKind::WouldBlock {
                LocalAuthorityStateStoreError::AlreadyLocked
            } else {
                io_error("acquire lock", source)
            }
        })?;

        Ok(Self {
            directory,
            _lock: lock,
            writer: Mutex::new(()),
        })
    }

    /// Loads and verifies the canonical serialized state.
    ///
    /// An absent canonical file returns `Ok(None)`. Temporary files are never
    /// considered authority. The implementation validates the on-disk length
    /// before allocating and never allocates more than the configured envelope
    /// bound.
    ///
    /// # Errors
    ///
    /// Returns a typed error for an unsafe file, oversized or corrupt envelope,
    /// allocation failure, or filesystem failure.
    pub fn load(&self) -> Result<Option<Vec<u8>>, LocalAuthorityStateStoreError> {
        let Some(mut file) = open_existing_regular(&self.directory, CANONICAL_FILE)? else {
            return Ok(None);
        };
        let metadata = file
            .metadata()
            .map_err(|source| io_error("inspect canonical state", source))?;
        let file_bytes = metadata.len();
        let maximum = u64::try_from(MAX_ENVELOPE_BYTES)
            .map_err(|_| LocalAuthorityStateStoreError::Allocation)?;
        if file_bytes > maximum {
            return Err(LocalAuthorityStateStoreError::EnvelopeTooLarge {
                bytes: file_bytes,
                maximum,
            });
        }
        let header_bytes =
            u64::try_from(HEADER_BYTES).map_err(|_| LocalAuthorityStateStoreError::Allocation)?;
        if file_bytes < header_bytes {
            return Err(LocalAuthorityStateStoreError::CorruptEnvelope);
        }
        let buffer_len =
            usize::try_from(file_bytes).map_err(|_| LocalAuthorityStateStoreError::Allocation)?;
        let mut envelope = Vec::new();
        envelope
            .try_reserve_exact(buffer_len)
            .map_err(|_| LocalAuthorityStateStoreError::Allocation)?;
        envelope.resize(buffer_len, 0);
        if let Err(source) = file.read_exact(&mut envelope) {
            envelope.zeroize();
            return if source.kind() == io::ErrorKind::UnexpectedEof {
                Err(LocalAuthorityStateStoreError::CorruptEnvelope)
            } else {
                Err(io_error("read canonical state", source))
            };
        }
        let mut trailing = [0_u8; 1];
        match file.read(&mut trailing) {
            Ok(0) => {}
            Ok(_) => {
                envelope.zeroize();
                return Err(LocalAuthorityStateStoreError::CorruptEnvelope);
            }
            Err(source) => {
                envelope.zeroize();
                return Err(io_error("bound canonical state", source));
            }
        }

        decode_envelope(envelope).map(Some)
    }

    /// Durably installs `payload` as the new canonical authority state.
    ///
    /// The prior canonical state remains intact until a synchronized
    /// same-directory temporary file is atomically renamed over it. Successful
    /// return additionally requires directory synchronization where supported
    /// and a canonical reopen-and-verify pass.
    ///
    /// # Errors
    ///
    /// Returns a typed error without replacing canonical state when the payload
    /// is oversized or temporary-file preparation fails. A synchronization or
    /// post-installation verification error means authority durability was not
    /// proven and callers must fail closed.
    pub fn store(&self, payload: &[u8]) -> Result<(), LocalAuthorityStateStoreError> {
        if payload.len() > MAX_PAYLOAD_BYTES {
            return Err(LocalAuthorityStateStoreError::PayloadTooLarge {
                bytes: payload.len(),
                maximum: MAX_PAYLOAD_BYTES,
            });
        }
        let _writer = self
            .writer
            .lock()
            .map_err(|_| LocalAuthorityStateStoreError::WriterUnavailable)?;
        let mut envelope = encode_envelope(payload)?;

        self.remove_regular_orphan_temp()?;
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        options.follow(FollowSymlinks::No);
        set_private_creation_mode(&mut options);
        let mut temporary = match self.directory.open_with(TEMP_FILE, &options) {
            Ok(file) => file,
            Err(source) => {
                envelope.zeroize();
                return Err(io_error("create temporary state", source));
            }
        };
        let temporary_metadata = match temporary.metadata() {
            Ok(metadata) => metadata,
            Err(source) => {
                envelope.zeroize();
                drop(temporary);
                self.remove_regular_temp_best_effort();
                return Err(io_error("inspect temporary state", source));
            }
        };
        if !is_unambiguous_regular(&temporary_metadata) {
            envelope.zeroize();
            drop(temporary);
            self.remove_regular_temp_best_effort();
            return Err(LocalAuthorityStateStoreError::UnsafeFileType);
        }
        if let Err(source) = temporary.write_all(&envelope) {
            envelope.zeroize();
            drop(temporary);
            self.remove_regular_temp_best_effort();
            return Err(io_error("write temporary state", source));
        }
        envelope.zeroize();
        if let Err(source) = temporary.sync_all() {
            drop(temporary);
            self.remove_regular_temp_best_effort();
            return Err(io_error("synchronize temporary state", source));
        }
        drop(temporary);

        let canonical_exists = match reject_unsafe_entry_if_present(&self.directory, CANONICAL_FILE)
        {
            Ok(exists) => exists,
            Err(error) => {
                self.remove_regular_temp_best_effort();
                return Err(error);
            }
        };
        if let Err(error) = self.install_temporary(canonical_exists) {
            self.remove_regular_temp_best_effort();
            return Err(error);
        }
        self.synchronize_directory()?;
        match self.load()? {
            Some(mut installed) if installed == payload => {
                installed.zeroize();
                Ok(())
            }
            Some(mut installed) => {
                installed.zeroize();
                Err(LocalAuthorityStateStoreError::VerificationFailed)
            }
            None => Err(LocalAuthorityStateStoreError::VerificationFailed),
        }
    }

    fn remove_regular_orphan_temp(&self) -> Result<(), LocalAuthorityStateStoreError> {
        if reject_unsafe_entry_if_present(&self.directory, TEMP_FILE)? {
            self.directory
                .remove_file(TEMP_FILE)
                .map_err(|source| io_error("remove orphan temporary state", source))?;
        }
        Ok(())
    }

    fn remove_regular_temp_best_effort(&self) {
        if matches!(
            self.directory.symlink_metadata(TEMP_FILE),
            Ok(metadata) if metadata.is_file()
        ) {
            let _ignored = self.directory.remove_file(TEMP_FILE);
        }
    }

    #[cfg(unix)]
    fn install_temporary(
        &self,
        _canonical_exists: bool,
    ) -> Result<(), LocalAuthorityStateStoreError> {
        self.directory
            .rename(TEMP_FILE, &self.directory, CANONICAL_FILE)
            .map_err(|source| io_error("atomically install canonical state", source))
    }

    #[cfg(windows)]
    fn install_temporary(
        &self,
        canonical_exists: bool,
    ) -> Result<(), LocalAuthorityStateStoreError> {
        if canonical_exists {
            return Err(LocalAuthorityStateStoreError::AtomicReplaceUnsupported);
        }
        self.directory
            .rename(TEMP_FILE, &self.directory, CANONICAL_FILE)
            .map_err(|source| io_error("atomically install canonical state", source))
    }

    #[cfg(not(any(unix, windows)))]
    fn install_temporary(
        &self,
        _canonical_exists: bool,
    ) -> Result<(), LocalAuthorityStateStoreError> {
        Err(LocalAuthorityStateStoreError::AtomicReplaceUnsupported)
    }

    #[cfg(unix)]
    fn synchronize_directory(&self) -> Result<(), LocalAuthorityStateStoreError> {
        self.directory
            .try_clone()
            .map_err(|source| io_error("clone authority directory", source))?
            .into_std_file()
            .sync_all()
            .map_err(|source| io_error("synchronize authority directory", source))
    }

    #[cfg(not(unix))]
    fn synchronize_directory(&self) -> Result<(), LocalAuthorityStateStoreError> {
        Ok(())
    }
}

fn open_root(root: &Path) -> Result<Dir, LocalAuthorityStateStoreError> {
    match fs::symlink_metadata(root) {
        Ok(metadata) => ensure_safe_root_metadata(&metadata)?,
        Err(source) if source.kind() == io::ErrorKind::NotFound => {
            fs::create_dir_all(root).map_err(|source| io_error("create authority root", source))?;
        }
        Err(source) => return Err(io_error("inspect authority root", source)),
    }
    let metadata = fs::symlink_metadata(root)
        .map_err(|source| io_error("reinspect authority root", source))?;
    ensure_safe_root_metadata(&metadata)?;
    let expected_identity = (metadata.dev(), metadata.ino());

    let directory = open_root_without_following(root)?;
    let opened_metadata = directory
        .dir_metadata()
        .map_err(|source| io_error("inspect opened authority root", source))?;
    if !opened_metadata.is_dir()
        || (opened_metadata.dev(), opened_metadata.ino()) != expected_identity
    {
        return Err(LocalAuthorityStateStoreError::UnsafeRoot);
    }
    set_private_root_permissions(&directory)?;
    Ok(directory)
}

fn ensure_safe_root_metadata(metadata: &fs::Metadata) -> Result<(), LocalAuthorityStateStoreError> {
    if !metadata.is_dir() || metadata.file_type().is_symlink() || is_windows_reparse(metadata) {
        return Err(LocalAuthorityStateStoreError::UnsafeRoot);
    }
    Ok(())
}

#[cfg(unix)]
fn open_root_without_following(root: &Path) -> Result<Dir, LocalAuthorityStateStoreError> {
    use std::os::unix::fs::OpenOptionsExt as _;

    let file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(root)
        .map_err(|source| io_error("open authority root", source))?;
    Ok(Dir::from_std_file(file))
}

#[cfg(windows)]
fn open_root_without_following(root: &Path) -> Result<Dir, LocalAuthorityStateStoreError> {
    use std::os::windows::fs::OpenOptionsExt as _;

    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;

    let file = fs::OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS)
        .open(root)
        .map_err(|source| io_error("open authority root", source))?;
    let metadata = file
        .metadata()
        .map_err(|source| io_error("inspect authority root handle", source))?;
    ensure_safe_root_metadata(&metadata)?;
    Ok(Dir::from_std_file(file))
}

#[cfg(not(any(unix, windows)))]
fn open_root_without_following(root: &Path) -> Result<Dir, LocalAuthorityStateStoreError> {
    Dir::open_ambient_dir(root, ambient_authority())
        .map_err(|source| io_error("open authority root", source))
}

#[cfg(windows)]
fn is_windows_reparse(metadata: &fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt as _;

    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
    metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}

#[cfg(not(windows))]
const fn is_windows_reparse(_metadata: &fs::Metadata) -> bool {
    false
}

fn reject_unsafe_entry_if_present(
    directory: &Dir,
    name: &'static str,
) -> Result<bool, LocalAuthorityStateStoreError> {
    match directory.symlink_metadata(name) {
        Ok(metadata) if is_unambiguous_regular(&metadata) => Ok(true),
        Ok(_) => Err(LocalAuthorityStateStoreError::UnsafeFileType),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(io_error("inspect reserved state entry", source)),
    }
}

fn open_existing_regular(
    directory: &Dir,
    name: &'static str,
) -> Result<Option<cap_std::fs::File>, LocalAuthorityStateStoreError> {
    if !reject_unsafe_entry_if_present(directory, name)? {
        return Ok(None);
    }
    let mut options = OpenOptions::new();
    options.read(true);
    options.follow(FollowSymlinks::No);
    let file = directory
        .open_with(name, &options)
        .map_err(|source| io_error("open canonical state", source))?;
    let metadata = file
        .metadata()
        .map_err(|source| io_error("inspect canonical state handle", source))?;
    if !is_unambiguous_regular(&metadata) {
        return Err(LocalAuthorityStateStoreError::UnsafeFileType);
    }
    Ok(Some(file))
}

fn encode_envelope(payload: &[u8]) -> Result<Vec<u8>, LocalAuthorityStateStoreError> {
    let envelope_len = HEADER_BYTES
        .checked_add(payload.len())
        .ok_or(LocalAuthorityStateStoreError::Allocation)?;
    let payload_len =
        u64::try_from(payload.len()).map_err(|_| LocalAuthorityStateStoreError::Allocation)?;
    let digest = Sha256::digest(payload);
    let mut envelope = Vec::new();
    envelope
        .try_reserve_exact(envelope_len)
        .map_err(|_| LocalAuthorityStateStoreError::Allocation)?;
    envelope.extend_from_slice(&MAGIC);
    envelope.extend_from_slice(&VERSION.to_be_bytes());
    envelope.extend_from_slice(&payload_len.to_be_bytes());
    envelope.extend_from_slice(&digest);
    envelope.extend_from_slice(payload);
    Ok(envelope)
}

fn decode_envelope(mut envelope: Vec<u8>) -> Result<Vec<u8>, LocalAuthorityStateStoreError> {
    let result = (|| {
        if envelope.len() < HEADER_BYTES || envelope[..MAGIC.len()] != MAGIC {
            return Err(LocalAuthorityStateStoreError::CorruptEnvelope);
        }
        let version_start = MAGIC.len();
        let length_start = version_start + size_of::<u16>();
        let digest_start = length_start + LENGTH_BYTES;
        let payload_start = digest_start + DIGEST_BYTES;

        let mut version_bytes = [0_u8; size_of::<u16>()];
        version_bytes.copy_from_slice(&envelope[version_start..length_start]);
        if u16::from_be_bytes(version_bytes) != VERSION {
            return Err(LocalAuthorityStateStoreError::CorruptEnvelope);
        }
        let mut length_bytes = [0_u8; LENGTH_BYTES];
        length_bytes.copy_from_slice(&envelope[length_start..digest_start]);
        let payload_len = usize::try_from(u64::from_be_bytes(length_bytes))
            .map_err(|_| LocalAuthorityStateStoreError::CorruptEnvelope)?;
        let expected_len = payload_start
            .checked_add(payload_len)
            .ok_or(LocalAuthorityStateStoreError::CorruptEnvelope)?;
        if payload_len > MAX_PAYLOAD_BYTES || expected_len != envelope.len() {
            return Err(LocalAuthorityStateStoreError::CorruptEnvelope);
        }
        let actual_digest = Sha256::digest(&envelope[payload_start..]);
        if envelope[digest_start..payload_start] != actual_digest[..] {
            return Err(LocalAuthorityStateStoreError::CorruptEnvelope);
        }

        envelope.copy_within(payload_start..expected_len, 0);
        envelope[payload_len..].zeroize();
        envelope.truncate(payload_len);
        Ok(())
    })();
    match result {
        Ok(()) => Ok(envelope),
        Err(error) => {
            envelope.zeroize();
            Err(error)
        }
    }
}

#[cfg(unix)]
fn set_private_creation_mode(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;

    options.mode(0o600);
}

#[cfg(not(unix))]
fn set_private_creation_mode(_options: &mut OpenOptions) {}

#[cfg(unix)]
fn set_private_root_permissions(directory: &Dir) -> Result<(), LocalAuthorityStateStoreError> {
    use std::os::unix::fs::PermissionsExt as _;

    directory
        .try_clone()
        .map_err(|source| io_error("clone authority root for protection", source))?
        .into_std_file()
        .set_permissions(fs::Permissions::from_mode(0o700))
        .map_err(|source| io_error("protect authority root", source))
}

#[cfg(not(unix))]
fn set_private_root_permissions(_directory: &Dir) -> Result<(), LocalAuthorityStateStoreError> {
    Ok(())
}

fn io_error(operation: &'static str, source: io::Error) -> LocalAuthorityStateStoreError {
    LocalAuthorityStateStoreError::Io { operation, source }
}

fn is_unambiguous_regular(metadata: &cap_std::fs::Metadata) -> bool {
    metadata.is_file() && metadata.nlink() == 1
}
