//! Exact digest-addressed catalog restore stages and retained final capabilities.

use std::fmt::{self, Write as _};
use std::fs::File;
use std::io;
use std::path::PathBuf;

use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt as _};
use cap_std::fs::OpenOptions;

use crate::PathError;

#[cfg(unix)]
use super::validate_private_file_identity;
use super::{
    CATALOG_FILE, CatalogFileGuard, CatalogLocation, CatalogWriterGuard,
    configure_private_catalog_creation, validate_private_file_identity_with_links,
};

const RESTORE_STAGE_PREFIX: &str = ".catalog.restore.";
const RESTORE_STAGE_SUFFIX: &str = ".pending";

/// Capability-relative exact restore stage retained under the destination writer lock.
pub struct CatalogRestoreStage {
    pub(super) file: File,
    pub(super) name: String,
    pub(super) location: CatalogLocation,
    pub(super) writer: CatalogWriterGuard,
    pub(super) created: bool,
}

/// Final no-replace catalog capability retained with its destination writer lock.
pub struct InstalledCatalogFile {
    pub(super) guard: CatalogFileGuard,
    pub(super) _writer: CatalogWriterGuard,
}

/// Existing exact-final candidate or fixed private stage for a catalog restore retry.
pub enum CatalogRestoreTarget {
    /// The final catalog name already exists and must be receipt-verified by the caller.
    Installed(InstalledCatalogFile),
    /// The fixed digest-addressed stage is newly created or must be exact-retry verified.
    Staged(CatalogRestoreStage),
}

impl CatalogRestoreStage {
    /// Returns whether this call created a new empty stage.
    pub const fn created(&self) -> bool {
        self.created
    }

    /// Clones the retained stage file for exact bounded copy or receipt verification.
    pub fn try_clone_file(&self) -> Result<File, PathError> {
        self.file
            .try_clone()
            .map_err(|source| PathError::io("failed to clone catalog restore stage", source))
    }

    /// Returns the capability-validated SQLite stage path for a no-follow VFS open.
    ///
    /// The caller must call [`Self::validate_identity`] immediately before and after the SQLite
    /// open and verification. This path is not publication authority.
    pub fn sqlite_path(&self) -> PathBuf {
        self.location.root.join(&self.name)
    }

    /// Revalidates the exact stage name, private metadata, and retained opened identity.
    pub fn validate_identity(&self) -> Result<(), PathError> {
        validate_private_file_identity_with_links(&self.location, &self.name, &self.file, 1)
    }

    /// Publishes the stage to the fixed catalog name without replacing any existing file.
    pub fn publish_no_replace(self) -> Result<InstalledCatalogFile, PathError> {
        publish_catalog_restore_stage(self)
    }
}

impl InstalledCatalogFile {
    /// Returns the retained exact final catalog guard.
    pub const fn guard(&self) -> &CatalogFileGuard {
        &self.guard
    }

    /// Consumes the installed capability into its exact final file and retained writer lock.
    pub fn into_parts(self) -> (CatalogFileGuard, CatalogWriterGuard) {
        (self.guard, self._writer)
    }
}

impl CatalogLocation {
    /// Prepares an exact digest-addressed restore target under the catalog writer lock.
    ///
    /// This never truncates, replaces, or removes a conflicting stage or final catalog. A
    /// surviving stage is returned only with its private unique-file identity retained; the
    /// caller must verify its exact receipt before attempting publication.
    pub fn prepare_catalog_restore(
        &self,
        receipt_identity: [u8; 32],
    ) -> Result<CatalogRestoreTarget, PathError> {
        if receipt_identity == [0_u8; 32] {
            return Err(PathError::CatalogRestoreConflict);
        }
        let writer = self.acquire_writer()?;
        let stage_name = restore_stage_name(receipt_identity)?;
        let final_exists = entry_exists(self, CATALOG_FILE)?;
        let stage_exists = entry_exists(self, &stage_name)?;

        match (final_exists, stage_exists) {
            (true, false) => {
                let guard = self.open_catalog_file()?;
                Ok(CatalogRestoreTarget::Installed(InstalledCatalogFile {
                    guard,
                    _writer: writer,
                }))
            }
            (true, true) => reconcile_linked_restore_stage(self, &stage_name, writer),
            (false, true) => {
                let file = open_restore_stage(self, &stage_name, false)?;
                Ok(CatalogRestoreTarget::Staged(CatalogRestoreStage {
                    file,
                    name: stage_name,
                    location: self.clone(),
                    writer,
                    created: false,
                }))
            }
            (false, false) => {
                let file = open_restore_stage(self, &stage_name, true)?;
                synchronize_catalog_parent(self)?;
                Ok(CatalogRestoreTarget::Staged(CatalogRestoreStage {
                    file,
                    name: stage_name,
                    location: self.clone(),
                    writer,
                    created: true,
                }))
            }
        }
    }
}

fn restore_stage_name(receipt_identity: [u8; 32]) -> Result<String, PathError> {
    let mut name = String::with_capacity(
        RESTORE_STAGE_PREFIX.len() + (receipt_identity.len() * 2) + RESTORE_STAGE_SUFFIX.len(),
    );
    name.push_str(RESTORE_STAGE_PREFIX);
    for byte in receipt_identity {
        write!(&mut name, "{byte:02x}").map_err(|_| PathError::CatalogRestoreConflict)?;
    }
    name.push_str(RESTORE_STAGE_SUFFIX);
    Ok(name)
}

fn entry_exists(location: &CatalogLocation, name: &str) -> Result<bool, PathError> {
    match location.root_capability.symlink_metadata(name) {
        Ok(_) => Ok(true),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(PathError::io(
            "failed to inspect catalog restore entry",
            source,
        )),
    }
}

fn open_restore_stage(
    location: &CatalogLocation,
    name: &str,
    create_new: bool,
) -> Result<File, PathError> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(create_new);
    options.follow(FollowSymlinks::No);
    configure_private_catalog_creation(&mut options);
    let file = location
        .root_capability
        .open_with(name, &options)
        .map_err(|source| PathError::io("failed to open catalog restore stage", source))?
        .into_std();
    validate_private_file_identity_with_links(location, name, &file, 1)?;
    Ok(file)
}

fn open_named_read_only(location: &CatalogLocation, name: &str) -> Result<File, PathError> {
    let mut options = OpenOptions::new();
    options.read(true);
    options.follow(FollowSymlinks::No);
    configure_private_catalog_creation(&mut options);
    location
        .root_capability
        .open_with(name, &options)
        .map(cap_std::fs::File::into_std)
        .map_err(|source| PathError::io("failed to open catalog restore entry", source))
}

#[cfg(unix)]
fn reconcile_linked_restore_stage(
    location: &CatalogLocation,
    stage_name: &str,
    writer: CatalogWriterGuard,
) -> Result<CatalogRestoreTarget, PathError> {
    let stage = open_named_read_only(location, stage_name)
        .map_err(|_| PathError::CatalogRestoreConflict)?;
    let final_file = open_named_read_only(location, CATALOG_FILE)
        .map_err(|_| PathError::CatalogRestoreConflict)?;
    validate_private_file_identity_with_links(location, stage_name, &stage, 2)
        .map_err(|_| PathError::CatalogRestoreConflict)?;
    validate_private_file_identity_with_links(location, CATALOG_FILE, &final_file, 2)
        .map_err(|_| PathError::CatalogRestoreConflict)?;
    if !same_opened_file(&stage, &final_file)? {
        return Err(PathError::CatalogRestoreConflict);
    }
    synchronize_catalog_parent(location)?;
    drop(stage);
    location
        .root_capability
        .remove_file(stage_name)
        .map_err(|source| PathError::io("failed to finalize catalog restore stage", source))?;
    synchronize_catalog_parent(location)?;
    validate_private_file_identity(location, CATALOG_FILE, &final_file)?;
    Ok(CatalogRestoreTarget::Installed(InstalledCatalogFile {
        guard: CatalogFileGuard {
            file: final_file,
            location: location.clone(),
        },
        _writer: writer,
    }))
}

#[cfg(not(unix))]
fn reconcile_linked_restore_stage(
    _location: &CatalogLocation,
    _stage_name: &str,
    _writer: CatalogWriterGuard,
) -> Result<CatalogRestoreTarget, PathError> {
    Err(PathError::CatalogRestoreConflict)
}

#[cfg(unix)]
fn same_opened_file(first: &File, second: &File) -> Result<bool, PathError> {
    use cap_fs_ext::MetadataExt as _;

    let first = first
        .metadata()
        .map_err(|source| PathError::io("failed to inspect catalog restore stage", source))?;
    let second = second
        .metadata()
        .map_err(|source| PathError::io("failed to inspect restored catalog", source))?;
    Ok((first.dev(), first.ino()) == (second.dev(), second.ino()))
}

#[cfg(unix)]
fn publish_catalog_restore_stage(
    stage: CatalogRestoreStage,
) -> Result<InstalledCatalogFile, PathError> {
    stage.validate_identity()?;
    match stage.location.root_capability.hard_link(
        &stage.name,
        &stage.location.root_capability,
        CATALOG_FILE,
    ) {
        Ok(()) => {}
        Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {}
        Err(source) => {
            return Err(PathError::io(
                "failed to publish restored catalog without replacement",
                source,
            ));
        }
    }
    synchronize_catalog_parent(&stage.location)?;
    let final_file = open_named_read_only(&stage.location, CATALOG_FILE)
        .map_err(|_| PathError::CatalogRestoreConflict)?;
    validate_private_file_identity_with_links(&stage.location, &stage.name, &stage.file, 2)
        .map_err(|_| PathError::CatalogRestoreConflict)?;
    validate_private_file_identity_with_links(&stage.location, CATALOG_FILE, &final_file, 2)
        .map_err(|_| PathError::CatalogRestoreConflict)?;
    if !same_opened_file(&stage.file, &final_file)? {
        return Err(PathError::CatalogRestoreConflict);
    }
    let CatalogRestoreStage {
        file,
        name,
        location,
        writer,
        created: _,
    } = stage;
    drop(file);
    location
        .root_capability
        .remove_file(&name)
        .map_err(|source| PathError::io("failed to finalize restored catalog", source))?;
    synchronize_catalog_parent(&location)?;
    validate_private_file_identity(&location, CATALOG_FILE, &final_file)?;
    Ok(InstalledCatalogFile {
        guard: CatalogFileGuard {
            file: final_file,
            location,
        },
        _writer: writer,
    })
}

#[cfg(windows)]
fn publish_catalog_restore_stage(
    stage: CatalogRestoreStage,
) -> Result<InstalledCatalogFile, PathError> {
    stage.validate_identity()?;
    let CatalogRestoreStage {
        file,
        name,
        location,
        writer,
        created: _,
    } = stage;
    drop(file);
    location.validate_for_open()?;
    let source = location.root.join(&name);
    let destination = location.root.join(CATALOG_FILE);
    if let Err(source) = atomicwrites::move_atomic(&source, &destination) {
        return if source.kind() == io::ErrorKind::AlreadyExists {
            Err(PathError::CatalogRestoreConflict)
        } else {
            Err(PathError::CatalogRestoreIndeterminate)
        };
    }
    location.validate_for_open()?;
    if entry_exists(&location, &name)? {
        return Err(PathError::CatalogRestoreIndeterminate);
    }
    let guard = location.open_catalog_file()?;
    Ok(InstalledCatalogFile {
        guard,
        _writer: writer,
    })
}

#[cfg(not(any(unix, windows)))]
fn publish_catalog_restore_stage(
    _stage: CatalogRestoreStage,
) -> Result<InstalledCatalogFile, PathError> {
    Err(PathError::CatalogRestoreIndeterminate)
}

#[cfg(unix)]
fn synchronize_catalog_parent(location: &CatalogLocation) -> Result<(), PathError> {
    location
        .root_capability
        .try_clone()
        .map_err(|source| PathError::io("failed to clone catalog parent", source))?
        .into_std_file()
        .sync_all()
        .map_err(|source| PathError::io("failed to synchronize catalog parent", source))
}

#[cfg(not(unix))]
fn synchronize_catalog_parent(_location: &CatalogLocation) -> Result<(), PathError> {
    Ok(())
}

impl fmt::Debug for CatalogRestoreStage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CatalogRestoreStage")
            .field("file", &"[PRIVATE FILE CAPABILITY]")
            .field("name", &"[DIGEST-ADDRESSED RESTORE STAGE]")
            .field("location", &self.location)
            .field("writer", &self.writer)
            .field("created", &self.created)
            .finish()
    }
}

impl fmt::Debug for InstalledCatalogFile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("InstalledCatalogFile([LOCKED FINAL CATALOG CAPABILITY])")
    }
}

impl fmt::Debug for CatalogRestoreTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Installed(_) => formatter.write_str("CatalogRestoreTarget::Installed([EXACT])"),
            Self::Staged(_) => formatter.write_str("CatalogRestoreTarget::Staged([EXACT])"),
        }
    }
}
