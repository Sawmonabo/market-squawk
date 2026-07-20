//! Retained immutable backup verification and no-replace restore installation.

use std::fmt;
use std::fs::File;
use std::io::{Read as _, Seek as _, SeekFrom};

use fs2::FileExt as _;
use market_squawk_platform::{
    CatalogFileGuard, CatalogLocation, CatalogRestoreStage, CatalogRestoreTarget,
    InstalledCatalogFile, PathError,
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
    lease: File,
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
        let identity = backup_file_identity(&self.lease)?;
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
}

impl InstalledBackupCatalog {
    pub(crate) const fn receipt(&self) -> BackupReceipt {
        self.receipt
    }

    pub(crate) fn into_parts(self) -> (InstalledCatalogFile, CatalogLocation, BackupReceipt) {
        (self.installed, self.location, self.receipt)
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
    /// Unlike an ordinary catalog writer open, verification neither creates nor acquires the
    /// catalog writer sidecar. The lease is taken directly on the already-opened backup file and
    /// retained with the exact file capability.
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
        let lease = guard.try_clone_file().map_err(map_catalog_location_error)?;
        lease.try_lock_exclusive().map_err(|source| {
            if source.kind() == std::io::ErrorKind::WouldBlock {
                CatalogError::BackupLeaseUnavailable
            } else {
                CatalogError::Io(source)
            }
        })?;
        let retained = VerifiedBackupCatalog {
            location: location.clone(),
            guard,
            lease,
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
        let receipt = source.receipt();
        let installed = match destination
            .prepare_catalog_restore(receipt.sha256())
            .map_err(map_catalog_restore_error)?
        {
            CatalogRestoreTarget::Installed(installed) => {
                verify_installed_receipt(destination, &installed, &receipt)?;
                installed
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
                installed
            }
        };
        Ok(InstalledBackupCatalog {
            installed,
            location: destination.clone(),
            receipt,
        })
    }
}

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
