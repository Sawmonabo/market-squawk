//! Retained immutable backup verification and no-replace restore installation.

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read as _, Seek as _, SeekFrom};

use market_squawk_platform::{
    CatalogFileGuard, CatalogLocation, CatalogRestoreStage, CatalogRestoreTarget,
    CatalogWriterGuard, InstalledCatalogFile, PathError,
};

use super::{
    BackupReceipt, backup_file_identity, receipt_for_file, verify_named_backup,
    verify_sqlite_backup,
};
use crate::catalog::{Catalog, CatalogError, map_catalog_location_error};

/// Non-duplicable lease over one exact, immutable, receipt-verified backup catalog.
pub(crate) struct VerifiedBackupCatalog {
    location: CatalogLocation,
    guard: CatalogFileGuard,
    _lease: CatalogWriterGuard,
    receipt: BackupReceipt,
}

impl VerifiedBackupCatalog {
    pub(crate) const fn receipt(&self) -> BackupReceipt {
        self.receipt
    }

    pub(crate) const fn location(&self) -> &CatalogLocation {
        &self.location
    }

    pub(crate) fn try_clone_file(&self) -> Result<File, CatalogError> {
        self.guard
            .try_clone_file()
            .map_err(map_catalog_location_error)
    }

    pub(crate) fn revalidate(&self) -> Result<(), CatalogError> {
        self.guard
            .validate_identity()
            .map_err(map_catalog_location_error)?;
        let file = self
            .guard
            .try_clone_file()
            .map_err(map_catalog_location_error)?;
        let identity = backup_file_identity(&file)?;
        verify_named_backup(self.location.path(), &identity, &self.receipt)?;
        self.guard
            .validate_identity()
            .map_err(map_catalog_location_error)
    }
}

impl fmt::Debug for VerifiedBackupCatalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("VerifiedBackupCatalog([LOCKED EXACT BACKUP CAPABILITY])")
    }
}

/// Exact restored final catalog retained under the destination writer lock.
pub(crate) struct InstalledBackupCatalog {
    installed: InstalledCatalogFile,
    location: CatalogLocation,
    receipt: BackupReceipt,
    state: InstalledCatalogState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InstalledCatalogState {
    ExactBackup,
    ExistingCandidate,
}

impl InstalledBackupCatalog {
    pub(crate) fn into_parts(
        self,
    ) -> (
        InstalledCatalogFile,
        CatalogLocation,
        BackupReceipt,
        InstalledCatalogState,
    ) {
        (self.installed, self.location, self.receipt, self.state)
    }
}

impl fmt::Debug for InstalledBackupCatalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("InstalledBackupCatalog([LOCKED EXACT FINAL CAPABILITY])")
    }
}

impl Catalog {
    /// Retains an exclusive non-mutating lease over one exact immutable backup.
    ///
    /// Verification acquires the already-published writer sidecar without creating filesystem
    /// state. The exclusive sidecar lease is retained with the exact readable catalog capability
    /// so receipt and immutable SQLite verification never conflict with an operating-system byte
    /// lock on the database itself.
    pub(crate) fn verify_backup_retained(
        location: &CatalogLocation,
        receipt: &BackupReceipt,
    ) -> Result<VerifiedBackupCatalog, CatalogError> {
        receipt.validate()?;
        location
            .validate_for_open()
            .map_err(map_catalog_location_error)?;
        let guard = location
            .open_catalog_file()
            .map_err(map_catalog_location_error)?;
        let lease = location.acquire_existing_writer().map_err(|error| {
            if matches!(error, PathError::CatalogAlreadyLocked) {
                CatalogError::BackupLeaseUnavailable
            } else {
                map_catalog_location_error(error)
            }
        })?;
        let retained = VerifiedBackupCatalog {
            location: location.clone(),
            guard,
            _lease: lease,
            receipt: *receipt,
        };
        retained.revalidate()?;
        location
            .validate_for_open()
            .map_err(map_catalog_location_error)?;
        Ok(retained)
    }

    /// Installs a retained exact backup to a fresh destination without replacement.
    ///
    /// A retry accepts only an exact digest-addressed stage or final receipt. Differing stage or
    /// final bytes are never truncated, replaced, or deleted.
    pub(crate) fn install_verified_backup_no_replace(
        source: &VerifiedBackupCatalog,
        destination: &CatalogLocation,
    ) -> Result<InstalledBackupCatalog, CatalogError> {
        source.revalidate()?;
        reject_catalog_alias(source, destination)?;
        let receipt = source.receipt();
        let (installed, state) = match destination
            .prepare_catalog_restore(receipt.sha256())
            .map_err(map_catalog_restore_error)?
        {
            CatalogRestoreTarget::Installed(installed) => {
                let state = match verify_installed_receipt(destination, &installed, &receipt) {
                    Ok(()) => InstalledCatalogState::ExactBackup,
                    Err(CatalogError::BackupReceiptMismatch) => {
                        InstalledCatalogState::ExistingCandidate
                    }
                    Err(error) => return Err(error),
                };
                (installed, state)
            }
            CatalogRestoreTarget::Staged(stage) => {
                if stage.created() {
                    copy_exact_backup(source, &stage, &receipt)?;
                } else {
                    verify_restore_stage(&stage, &receipt)?;
                }
                source.revalidate()?;
                let installed = stage
                    .publish_no_replace()
                    .map_err(map_catalog_restore_error)?;
                verify_installed_receipt(destination, &installed, &receipt)?;
                (installed, InstalledCatalogState::ExactBackup)
            }
        };
        Ok(InstalledBackupCatalog {
            installed,
            location: destination.clone(),
            receipt,
            state,
        })
    }
}

fn reject_catalog_alias(
    source: &VerifiedBackupCatalog,
    destination: &CatalogLocation,
) -> Result<(), CatalogError> {
    source.revalidate()?;
    destination
        .validate_for_open()
        .map_err(map_catalog_restore_error)?;
    if source.location().path() == destination.path() {
        return Err(CatalogError::BackupRestoreConflict);
    }
    let source_path = fs::canonicalize(source.location().path())?;
    match fs::canonicalize(destination.path()) {
        Ok(destination_path) if destination_path == source_path => {
            return Err(CatalogError::BackupRestoreConflict);
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    }
    let mut options = OpenOptions::new();
    options.read(true);
    configure_alias_read(&mut options);
    let destination_file = options.open(destination.path())?;
    let source_file = source.try_clone_file()?;
    if backup_file_identity(&source_file)? == backup_file_identity(&destination_file)? {
        return Err(CatalogError::BackupRestoreConflict);
    }
    source.revalidate()
}

#[cfg(unix)]
fn configure_alias_read(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt as _;

    options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
}

#[cfg(windows)]
fn configure_alias_read(options: &mut OpenOptions) {
    use std::os::windows::fs::OpenOptionsExt as _;

    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    options
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
}

#[cfg(not(any(unix, windows)))]
fn configure_alias_read(_options: &mut OpenOptions) {}

fn copy_exact_backup(
    source: &VerifiedBackupCatalog,
    stage: &CatalogRestoreStage,
    receipt: &BackupReceipt,
) -> Result<(), CatalogError> {
    source.revalidate()?;
    stage
        .validate_identity()
        .map_err(map_catalog_restore_error)?;
    let source_file = source.try_clone_file()?;
    if source_file.metadata()?.len() != receipt.byte_length() {
        return Err(CatalogError::BackupReceiptMismatch);
    }
    let mut stage_file = stage.try_clone_file().map_err(map_catalog_restore_error)?;
    if stage_file.metadata()?.len() != 0 {
        return Err(CatalogError::BackupRestoreConflict);
    }
    stage_file.seek(SeekFrom::Start(0))?;
    let copy_limit = receipt
        .byte_length()
        .checked_add(1)
        .ok_or(CatalogError::BackupReceiptMismatch)?;
    let mut bounded_source = source_file.take(copy_limit);
    let copied = std::io::copy(&mut bounded_source, &mut stage_file)?;
    if copied != receipt.byte_length() {
        return Err(CatalogError::BackupReceiptMismatch);
    }
    stage_file.sync_all()?;
    drop(stage_file);
    verify_restore_stage(stage, receipt)?;
    source.revalidate()
}

fn verify_restore_stage(
    stage: &CatalogRestoreStage,
    receipt: &BackupReceipt,
) -> Result<(), CatalogError> {
    receipt.validate()?;
    stage
        .validate_identity()
        .map_err(map_catalog_restore_error)?;
    let file = stage.try_clone_file().map_err(map_catalog_restore_error)?;
    if receipt_for_file(&file)? != *receipt {
        return Err(CatalogError::BackupRestoreConflict);
    }
    verify_sqlite_backup(&stage.sqlite_path())?;
    if receipt_for_file(&file)? != *receipt {
        return Err(CatalogError::BackupRestoreConflict);
    }
    stage.validate_identity().map_err(map_catalog_restore_error)
}

fn verify_installed_receipt(
    destination: &CatalogLocation,
    installed: &InstalledCatalogFile,
    receipt: &BackupReceipt,
) -> Result<(), CatalogError> {
    receipt.validate()?;
    destination
        .validate_for_open()
        .map_err(map_catalog_restore_error)?;
    let guard = installed.guard();
    guard
        .validate_identity()
        .map_err(map_catalog_restore_error)?;
    let file = guard.try_clone_file().map_err(map_catalog_restore_error)?;
    let identity = backup_file_identity(&file)?;
    verify_named_backup(destination.path(), &identity, receipt)?;
    guard
        .validate_identity()
        .map_err(map_catalog_restore_error)?;
    destination
        .validate_for_open()
        .map_err(map_catalog_restore_error)
}

fn map_catalog_restore_error(error: PathError) -> CatalogError {
    match error {
        PathError::CatalogRestoreConflict => CatalogError::BackupRestoreConflict,
        PathError::CatalogRestoreIndeterminate => CatalogError::BackupRestoreIndeterminate,
        PathError::CatalogAlreadyLocked => CatalogError::WriterAlreadyOpen,
        _ => CatalogError::UnsafePath,
    }
}
