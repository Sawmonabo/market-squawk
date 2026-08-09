//! Capability-confined root, fixed-slot safety, and platform publication primitives.

use std::fs;
use std::io::{self, Read as _, Write as _};
use std::path::Path;
#[cfg(windows)]
use std::path::PathBuf;

use cap_fs_ext::{FollowSymlinks, MetadataExt as _, OpenOptionsFollowExt};
use cap_std::fs::{Dir, OpenOptions};
use fs2::FileExt as _;
use zeroize::Zeroizing;

use super::LocalAuthorityStateStoreError;
use super::envelope::MAX_ENVELOPE_BYTES;

const SLOT_A_FILE: &str = "authority-state-a.bin";
const SLOT_B_FILE: &str = "authority-state-b.bin";
const TEMP_A_FILE: &str = ".authority-state-a.tmp";
const TEMP_B_FILE: &str = ".authority-state-b.tmp";
const LOCK_FILE: &str = ".authority-state.lock";

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum Slot {
    A,
    B,
}

pub(super) struct PublicationResidue {
    pub(super) slot: Slot,
    pub(super) bytes: Zeroizing<Vec<u8>>,
    #[cfg(unix)]
    identity: (u64, u64),
}

struct BoundedFile {
    bytes: Zeroizing<Vec<u8>>,
    metadata: cap_std::fs::Metadata,
}

impl Slot {
    pub(super) const fn other(self) -> Self {
        match self {
            Self::A => Self::B,
            Self::B => Self::A,
        }
    }

    pub(super) const fn file(self) -> &'static str {
        match self {
            Self::A => SLOT_A_FILE,
            Self::B => SLOT_B_FILE,
        }
    }

    const fn temporary(self) -> &'static str {
        match self {
            Self::A => TEMP_A_FILE,
            Self::B => TEMP_B_FILE,
        }
    }
}

pub(super) struct StateFiles {
    directory: Dir,
    #[cfg(windows)]
    root_path: PathBuf,
}

/// Releases the advisory lock before a fork-duplicated descriptor can outlive its logical owner.
#[derive(Debug)]
pub(super) struct LifetimeLock(fs::File);

impl Drop for LifetimeLock {
    fn drop(&mut self) {
        let _ignored = fs2::FileExt::unlock(&self.0);
    }
}

impl StateFiles {
    pub(super) fn try_open(
        root: &Path,
    ) -> Result<(Self, LifetimeLock), LocalAuthorityStateStoreError> {
        let directory = open_root(root)?;
        reject_unsafe_entry_if_present(&directory, LOCK_FILE)?;
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true);
        options.follow(FollowSymlinks::No);
        set_private_creation_mode(&mut options);
        let lock = directory
            .open_with(LOCK_FILE, &options)
            .map_err(|source| io_error("open lock", source))?;
        if !is_unambiguous_regular(
            &lock
                .metadata()
                .map_err(|source| io_error("inspect lock", source))?,
        ) {
            return Err(LocalAuthorityStateStoreError::UnsafeFileType);
        }
        let lock = lock.into_std();
        lock.try_lock_exclusive().map_err(|source| {
            if is_lock_contended(&source) {
                LocalAuthorityStateStoreError::AlreadyLocked
            } else {
                io_error("acquire lock", source)
            }
        })?;
        let lock = LifetimeLock(lock);
        #[cfg(windows)]
        let root_path = fs::canonicalize(root)
            .map_err(|source| io_error("canonicalize retained authority root", source))?;
        let files = Self {
            directory,
            #[cfg(windows)]
            root_path,
        };
        files.reconcile_publication_residue()?;
        for name in [SLOT_A_FILE, SLOT_B_FILE, TEMP_A_FILE, TEMP_B_FILE] {
            reject_unsafe_entry_if_present(&files.directory, name)?;
        }
        Ok((files, lock))
    }

    pub(super) fn read_slot(
        &self,
        slot: Slot,
    ) -> Result<Option<Zeroizing<Vec<u8>>>, LocalAuthorityStateStoreError> {
        self.read_bounded_regular(slot.file())
            .map(|result| result.map(|file| file.bytes))
    }

    fn read_bounded_regular(
        &self,
        name: &'static str,
    ) -> Result<Option<BoundedFile>, LocalAuthorityStateStoreError> {
        let Some(mut file) = open_existing_regular(&self.directory, name)? else {
            return Ok(None);
        };
        let metadata = file
            .metadata()
            .map_err(|source| io_error("inspect authority state", source))?;
        let file_bytes = metadata.len();
        let maximum = u64::try_from(MAX_ENVELOPE_BYTES)
            .map_err(|_| LocalAuthorityStateStoreError::Allocation)?;
        if file_bytes > maximum {
            return Err(LocalAuthorityStateStoreError::EnvelopeTooLarge {
                bytes: file_bytes,
                maximum,
            });
        }
        let buffer_len =
            usize::try_from(file_bytes).map_err(|_| LocalAuthorityStateStoreError::Allocation)?;
        let mut bytes = Zeroizing::new(Vec::new());
        bytes
            .try_reserve_exact(buffer_len)
            .map_err(|_| LocalAuthorityStateStoreError::Allocation)?;
        bytes.resize(buffer_len, 0);
        file.read_exact(bytes.as_mut_slice())
            .map_err(|source| io_error("read authority slot", source))?;
        let mut trailing = [0_u8; 1];
        if file
            .read(&mut trailing)
            .map_err(|source| io_error("bound authority slot", source))?
            != 0
        {
            return Err(LocalAuthorityStateStoreError::CorruptEnvelope);
        }
        Ok(Some(BoundedFile { bytes, metadata }))
    }

    #[cfg(unix)]
    pub(super) fn publication_residue(
        &self,
    ) -> Result<Option<PublicationResidue>, LocalAuthorityStateStoreError> {
        let mut residue = None;
        for slot in [Slot::A, Slot::B] {
            let Some(file) = self.read_bounded_regular(slot.temporary())? else {
                continue;
            };
            if residue.is_some() {
                return Err(LocalAuthorityStateStoreError::UnsafeFileType);
            }
            residue = Some(PublicationResidue {
                slot,
                identity: (file.metadata.dev(), file.metadata.ino()),
                bytes: file.bytes,
            });
        }
        Ok(residue)
    }

    #[cfg(not(unix))]
    pub(super) const fn publication_residue(
        &self,
    ) -> Result<Option<PublicationResidue>, LocalAuthorityStateStoreError> {
        Ok(None)
    }

    #[cfg(unix)]
    pub(super) fn discard_publication_residue(
        &self,
        residue: &PublicationResidue,
    ) -> Result<(), LocalAuthorityStateStoreError> {
        let observed = self
            .directory
            .symlink_metadata(residue.slot.temporary())
            .map_err(|_| LocalAuthorityStateStoreError::RecoveryRequired)?;
        if !is_unambiguous_regular(&observed)
            || (observed.dev(), observed.ino()) != residue.identity
        {
            return Err(LocalAuthorityStateStoreError::RecoveryRequired);
        }
        self.directory
            .remove_file(residue.slot.temporary())
            .map_err(|_| LocalAuthorityStateStoreError::RecoveryRequired)?;
        self.synchronize_directory()
            .map_err(|_| LocalAuthorityStateStoreError::RecoveryRequired)?;
        match self.directory.symlink_metadata(residue.slot.temporary()) {
            Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
            Ok(_) | Err(_) => Err(LocalAuthorityStateStoreError::RecoveryRequired),
        }
    }

    #[cfg(not(unix))]
    pub(super) const fn discard_publication_residue(
        &self,
        _residue: &PublicationResidue,
    ) -> Result<(), LocalAuthorityStateStoreError> {
        Err(LocalAuthorityStateStoreError::SecureRootUnsupported)
    }

    pub(super) fn publish(
        &self,
        slot: Slot,
        bytes: &[u8],
    ) -> Result<(), LocalAuthorityStateStoreError> {
        let existed = reject_unsafe_entry_if_present(&self.directory, slot.file())?;
        self.publish_bytes(slot, existed, bytes)
    }

    #[cfg(unix)]
    fn publish_bytes(
        &self,
        slot: Slot,
        existed: bool,
        bytes: &[u8],
    ) -> Result<(), LocalAuthorityStateStoreError> {
        self.require_temporary_absent(slot)?;
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        options.follow(FollowSymlinks::No);
        set_private_creation_mode(&mut options);
        let mut temporary = self
            .directory
            .open_with(slot.temporary(), &options)
            .map_err(|source| io_error("create temporary state", source))?;
        temporary
            .write_all(bytes)
            .map_err(|source| io_error("write temporary state", source))?;
        temporary
            .sync_all()
            .map_err(|source| io_error("synchronize temporary state", source))?;
        drop(temporary);
        if existed {
            self.directory
                .rename(slot.temporary(), &self.directory, slot.file())
                .map_err(|source| io_error("replace inactive authority slot", source))?;
        } else {
            self.directory
                .hard_link(slot.temporary(), &self.directory, slot.file())
                .map_err(|source| io_error("install new authority slot", source))?;
            self.directory
                .remove_file(slot.temporary())
                .map_err(|source| io_error("remove linked authority temporary", source))?;
        }
        self.synchronize_directory()
    }

    #[cfg(windows)]
    fn publish_bytes(
        &self,
        slot: Slot,
        existed: bool,
        bytes: &[u8],
    ) -> Result<(), LocalAuthorityStateStoreError> {
        self.require_temporary_absent(slot)?;
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        options.follow(FollowSymlinks::No);
        let mut temporary = self
            .directory
            .open_with(slot.temporary(), &options)
            .map_err(|source| io_error("create fixed temporary state", source))?;
        temporary
            .write_all(bytes)
            .map_err(|source| io_error("write fixed temporary state", source))?;
        temporary
            .sync_all()
            .map_err(|source| io_error("synchronize fixed temporary state", source))?;
        drop(temporary);

        self.validate_windows_root_identity()?;
        let source = self.root_path.join(slot.temporary());
        let destination = self.root_path.join(slot.file());
        let publication = if existed {
            match atomicwrites::replace_atomic(&source, &destination) {
                Ok(()) => Ok(()),
                Err(source) if source.kind() == io::ErrorKind::PermissionDenied => {
                    // MoveFileExW cannot always replace a destination that remains open. Rust's
                    // rename adds the FileRenameInfoEx POSIX-semantics fallback on supported
                    // Windows filesystems while this capability handle retains the authority root.
                    self.directory
                        .rename(slot.temporary(), &self.directory, slot.file())
                }
                Err(source) => Err(source),
            }
        } else {
            atomicwrites::move_atomic(&source, &destination)
        };
        if publication.is_err() {
            return Err(LocalAuthorityStateStoreError::RecoveryRequired);
        }
        self.validate_windows_root_identity()?;
        if self.directory.symlink_metadata(slot.temporary()).is_ok()
            || open_existing_regular(&self.directory, slot.file())?.is_none()
        {
            return Err(LocalAuthorityStateStoreError::RecoveryRequired);
        }
        Ok(())
    }

    #[cfg(not(any(unix, windows)))]
    fn publish_bytes(
        &self,
        _slot: Slot,
        _existed: bool,
        _bytes: &[u8],
    ) -> Result<(), LocalAuthorityStateStoreError> {
        Err(LocalAuthorityStateStoreError::AtomicReplaceUnsupported)
    }

    #[cfg(unix)]
    fn synchronize_directory(&self) -> Result<(), LocalAuthorityStateStoreError> {
        use cap_std::fs::OpenOptionsExt as _;

        let mut options = OpenOptions::new();
        options.read(true);
        options.follow(FollowSymlinks::No);
        options.custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC);
        self.directory
            .open_with(".", &options)
            .map_err(|source| io_error("open synchronizable authority directory", source))?
            .into_std()
            .sync_all()
            .map_err(|source| io_error("synchronize authority directory", source))
    }

    #[cfg(not(unix))]
    fn synchronize_directory(&self) -> Result<(), LocalAuthorityStateStoreError> {
        Ok(())
    }

    fn require_temporary_absent(&self, slot: Slot) -> Result<(), LocalAuthorityStateStoreError> {
        match self.directory.symlink_metadata(slot.temporary()) {
            Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
            Ok(_) => Err(LocalAuthorityStateStoreError::RecoveryRequired),
            Err(source) => Err(io_error("inspect fixed temporary state", source)),
        }
    }

    #[cfg(unix)]
    fn reconcile_publication_residue(&self) -> Result<(), LocalAuthorityStateStoreError> {
        for slot in [Slot::A, Slot::B] {
            let temporary = match self.directory.symlink_metadata(slot.temporary()) {
                Err(source) if source.kind() == io::ErrorKind::NotFound => continue,
                Ok(metadata) => metadata,
                Err(source) => return Err(io_error("inspect publication residue", source)),
            };
            if temporary.is_file() && temporary.nlink() == 1 {
                continue;
            }
            let installed = self
                .directory
                .symlink_metadata(slot.file())
                .map_err(|_| LocalAuthorityStateStoreError::UnsafeFileType)?;
            if !temporary.is_file()
                || !installed.is_file()
                || temporary.nlink() != 2
                || installed.nlink() != 2
                || (temporary.dev(), temporary.ino()) != (installed.dev(), installed.ino())
            {
                return Err(LocalAuthorityStateStoreError::UnsafeFileType);
            }
            let identity = (installed.dev(), installed.ino());
            self.directory
                .remove_file(slot.temporary())
                .map_err(|_| LocalAuthorityStateStoreError::RecoveryRequired)?;
            self.synchronize_directory()
                .map_err(|_| LocalAuthorityStateStoreError::RecoveryRequired)?;
            let recovered = self
                .directory
                .symlink_metadata(slot.file())
                .map_err(|_| LocalAuthorityStateStoreError::RecoveryRequired)?;
            if !recovered.is_file()
                || recovered.nlink() != 1
                || (recovered.dev(), recovered.ino()) != identity
            {
                return Err(LocalAuthorityStateStoreError::RecoveryRequired);
            }
        }
        Ok(())
    }

    #[cfg(windows)]
    fn reconcile_publication_residue(&self) -> Result<(), LocalAuthorityStateStoreError> {
        self.validate_windows_root_identity()?;
        for slot in [Slot::A, Slot::B] {
            match self.directory.symlink_metadata(slot.temporary()) {
                Err(source) if source.kind() == io::ErrorKind::NotFound => {}
                Ok(_) => return Err(LocalAuthorityStateStoreError::UnsafeFileType),
                Err(source) => return Err(io_error("inspect publication residue", source)),
            }
        }
        Ok(())
    }

    #[cfg(not(any(unix, windows)))]
    fn reconcile_publication_residue(&self) -> Result<(), LocalAuthorityStateStoreError> {
        Err(LocalAuthorityStateStoreError::SecureRootUnsupported)
    }

    #[cfg(windows)]
    fn validate_windows_root_identity(&self) -> Result<(), LocalAuthorityStateStoreError> {
        let reopened = open_root_without_following(&self.root_path)
            .map_err(|_| LocalAuthorityStateStoreError::RecoveryRequired)?;
        let retained = self
            .directory
            .dir_metadata()
            .map_err(|_| LocalAuthorityStateStoreError::RecoveryRequired)?;
        let observed = reopened
            .dir_metadata()
            .map_err(|_| LocalAuthorityStateStoreError::RecoveryRequired)?;
        if !same_opened_root_identity(&retained, &observed) {
            return Err(LocalAuthorityStateStoreError::RecoveryRequired);
        }
        Ok(())
    }
}

#[cfg(any(unix, windows))]
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
    #[cfg(unix)]
    let expected_identity = (metadata.dev(), metadata.ino());

    let directory = open_root_without_following(root)?;
    let opened_metadata = directory
        .dir_metadata()
        .map_err(|source| io_error("inspect opened authority root", source))?;
    #[cfg(unix)]
    let opened_expected_root = (opened_metadata.dev(), opened_metadata.ino()) == expected_identity;
    #[cfg(windows)]
    let opened_expected_root = {
        let verification_directory = open_root_without_following(root)?;
        let verification_metadata = verification_directory
            .dir_metadata()
            .map_err(|source| io_error("reinspect opened authority root", source))?;
        same_opened_root_identity(&opened_metadata, &verification_metadata)
    };
    if !opened_metadata.is_dir() || !opened_expected_root {
        return Err(LocalAuthorityStateStoreError::UnsafeRoot);
    }
    set_private_root_permissions(&directory)?;
    Ok(directory)
}

#[cfg(not(any(unix, windows)))]
fn open_root(_root: &Path) -> Result<Dir, LocalAuthorityStateStoreError> {
    Err(LocalAuthorityStateStoreError::SecureRootUnsupported)
}

#[cfg(windows)]
fn same_opened_root_identity(
    first: &cap_std::fs::Metadata,
    second: &cap_std::fs::Metadata,
) -> bool {
    (first.dev(), first.ino()) == (second.dev(), second.ino())
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
        .map_err(|source| io_error("open authority state", source))?;
    if !is_unambiguous_regular(
        &file
            .metadata()
            .map_err(|source| io_error("inspect authority state handle", source))?,
    ) {
        return Err(LocalAuthorityStateStoreError::UnsafeFileType);
    }
    Ok(Some(file))
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

fn is_lock_contended(source: &io::Error) -> bool {
    let expected = fs2::lock_contended_error();
    match (source.raw_os_error(), expected.raw_os_error()) {
        (Some(actual), Some(expected)) => actual == expected,
        _ => source.kind() == expected.kind(),
    }
}

fn is_unambiguous_regular(metadata: &cap_std::fs::Metadata) -> bool {
    metadata.is_file() && metadata.nlink() == 1
}

#[cfg(all(test, windows))]
mod windows_tests {
    use super::{
        LocalAuthorityStateStoreError, open_root_without_following, same_opened_root_identity,
    };

    #[test]
    fn opened_handle_rejects_a_windows_directory_reparse_point()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::windows::fs::symlink_dir;

        let parent = tempfile::tempdir()?;
        let real = parent.path().join("real-root");
        let alias = parent.path().join("root-alias");
        std::fs::create_dir(&real)?;
        symlink_dir(&real, &alias)?;
        assert!(matches!(
            open_root_without_following(&alias),
            Err(LocalAuthorityStateStoreError::UnsafeRoot)
        ));
        Ok(())
    }

    #[test]
    fn opened_root_identity_accepts_same_and_rejects_different()
    -> Result<(), Box<dyn std::error::Error>> {
        let parent = tempfile::tempdir()?;
        let first_path = parent.path().join("first");
        let second_path = parent.path().join("second");
        std::fs::create_dir(&first_path)?;
        std::fs::create_dir(&second_path)?;
        let first = open_root_without_following(&first_path)?;
        let first_reopened = open_root_without_following(&first_path)?;
        let second = open_root_without_following(&second_path)?;
        assert!(same_opened_root_identity(
            &first.dir_metadata()?,
            &first_reopened.dir_metadata()?
        ));
        assert!(!same_opened_root_identity(
            &first.dir_metadata()?,
            &second.dir_metadata()?
        ));
        Ok(())
    }
}
