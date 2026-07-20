//! Receipt-bound analytical restore preflight and no-replace artifact materialization.

use market_squawk_domain::Timestamp;
use market_squawk_platform::ArtifactRoot;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::evidence::fs::{MaterializedArtifactRoot, VerifiedArtifactInventory};
use super::evidence::{CatalogContentEvidenceDigest, CatalogEvidenceSnapshot, EvidenceError};
use super::{
    ArtifactInventoryDigest, AuthorityEventDigest, AuthorityEvidenceDigest, AuthorityGeneration,
    AuthoritySnapshot, AuthorityState, CatalogEndpointIdentity, RootEndpointIdentity,
    StableArtifactRootIdentity,
};
use crate::ParquetObjectStore;
use crate::analytical_backup::AnalyticalBackupBundleReceipt;
use crate::catalog::RestoreCatalogBaseline;
use crate::catalog::VerifiedBackupCatalog;
use crate::{BackupReceipt, CatalogAuthority, CatalogError};

/// Exact receipt-bound facts derived from retained source catalog and artifact capabilities.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RestoreEvidenceSummary {
    catalog_identity: CatalogEndpointIdentity,
    authority_generation: AuthorityGeneration,
    authority_event: AuthorityEventDigest,
    source_authority_evidence: AuthorityEvidenceDigest,
    catalog_content_evidence: CatalogContentEvidenceDigest,
    stable_root_identity: StableArtifactRootIdentity,
    cutoff: Timestamp,
    artifact_count: u64,
    artifact_bytes: u64,
    artifact_inventory: ArtifactInventoryDigest,
}

/// Receipt-validated evidence owned by the analytical backup/restore coordinator.
///
/// This state is deliberately not `Clone`. The coordinator retains it through exact catalog
/// installation, artifact materialization, and the sealed authority-transition handoff.
pub(crate) struct ReceiptValidatedRestoreEvidence {
    receipt: AnalyticalBackupBundleReceipt,
    authority: AuthoritySnapshot,
    _catalog_evidence: CatalogEvidenceSnapshot,
    artifact_inventory: VerifiedArtifactInventory,
}

impl ReceiptValidatedRestoreEvidence {
    pub(crate) const fn receipt(&self) -> AnalyticalBackupBundleReceipt {
        self.receipt
    }

    pub(crate) const fn request(&self) -> super::evidence::EvidenceSnapshotRequest {
        self._catalog_evidence.request()
    }
}

impl std::fmt::Debug for ReceiptValidatedRestoreEvidence {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReceiptValidatedRestoreEvidence")
            .field("receipt", &self.receipt)
            .field("authority", &self.authority)
            .field("catalog_evidence", &"[RETAINED VALIDATED CATALOG EVIDENCE]")
            .field("artifact_inventory", &self.artifact_inventory)
            .finish()
    }
}

/// Validates every path-free bundle relationship before any destination mutation is permitted.
///
/// Exact immutable catalog-file retention is a separate mandatory gate because the catalog owns
/// the file lease and no-replace install primitive. This function cannot authorize destination
/// mutation on its own.
pub(crate) fn validate_restore_evidence(
    receipt: AnalyticalBackupBundleReceipt,
    authority: AuthoritySnapshot,
    catalog_evidence: CatalogEvidenceSnapshot,
    artifact_inventory: VerifiedArtifactInventory,
    cancellation: &CancellationToken,
) -> Result<ReceiptValidatedRestoreEvidence, RestoreValidationError> {
    catalog_evidence.check_cancellation(cancellation)?;
    let head = authority
        .head()
        .ok_or(RestoreValidationError::AuthorityNotBound)?;
    let bound = authority
        .bound()
        .ok_or(RestoreValidationError::AuthorityNotBound)?;
    let prepared = bound.prepared();
    let artifact_count = u64::try_from(artifact_inventory.artifacts().len())
        .map_err(|_| RestoreValidationError::ResourceLimitExceeded)?;
    let catalog_content_evidence = catalog_evidence.evidence_digest()?;
    let summary = RestoreEvidenceSummary {
        catalog_identity: prepared.target_catalog_identity(),
        authority_generation: prepared.authority_generation(),
        authority_event: head.event_digest(),
        source_authority_evidence: prepared.evidence_digest(),
        catalog_content_evidence,
        stable_root_identity: bound.stable_root_identity(),
        cutoff: catalog_evidence.request().cutoff(),
        artifact_count,
        artifact_bytes: artifact_inventory.total_bytes(),
        artifact_inventory: artifact_inventory.digest(),
    };
    validate_receipt_summary(&receipt, summary)?;
    if cancellation.is_cancelled() {
        return Err(RestoreValidationError::Cancelled);
    }
    Ok(ReceiptValidatedRestoreEvidence {
        receipt,
        authority,
        _catalog_evidence: catalog_evidence,
        artifact_inventory,
    })
}

/// Explicit recovery policy for no-replace artifact materialization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RestoreArtifactMode {
    /// Require a completely empty retained destination root.
    Fresh,
    /// Admit only an exact verified subset created by the same bundle and fill missing objects.
    ResumeExactSubset,
}

/// Retained destination writer admitted against one exact restore state and main-file receipt.
pub(crate) struct VerifiedRestoreCatalogAuthority {
    authority: CatalogAuthority,
    snapshot: AuthoritySnapshot,
    state_receipt: BackupReceipt,
    request: super::evidence::EvidenceSnapshotRequest,
    content_evidence: CatalogContentEvidenceDigest,
    baseline: RestoreCatalogBaseline,
    cancellation: CancellationToken,
}

impl VerifiedRestoreCatalogAuthority {
    pub(super) fn try_new(
        authority: CatalogAuthority,
        expected: AuthoritySnapshot,
        request: super::evidence::EvidenceSnapshotRequest,
        content_evidence: CatalogContentEvidenceDigest,
        baseline: RestoreCatalogBaseline,
        cancellation: &CancellationToken,
    ) -> Result<Self, CatalogError> {
        authority.acquire_restore_exclusive_locking()?;
        authority.verify_restore_baseline(baseline, cancellation)?;
        if authority.authority_snapshot_without_endpoint()? != expected {
            return Err(CatalogError::BackupRestoreConflict);
        }
        let state_receipt = authority.checkpoint_restore_state()?;
        let snapshot = authority.authority_snapshot_without_endpoint()?;
        if snapshot != expected {
            return Err(CatalogError::BackupRestoreConflict);
        }
        let retained = Self {
            authority,
            snapshot,
            state_receipt,
            request,
            content_evidence,
            baseline,
            cancellation: cancellation.clone(),
        };
        retained.revalidate()?;
        Ok(retained)
    }

    pub(crate) fn snapshot(&self) -> &AuthoritySnapshot {
        &self.snapshot
    }

    pub(crate) fn revalidate(&self) -> Result<(), CatalogError> {
        self.authority
            .verify_restore_baseline(self.baseline, &self.cancellation)?;
        self.authority
            .revalidate_restore_state(self.state_receipt)?;
        if self.authority.authority_snapshot_without_endpoint()? != self.snapshot {
            return Err(CatalogError::BackupRestoreConflict);
        }
        let (snapshot, evidence) = self.authority.analytical_evidence_snapshot(self.request)?;
        if snapshot != self.snapshot
            || evidence
                .evidence_digest()
                .map_err(|_| CatalogError::AnalyticalEvidenceInvalid)?
                != self.content_evidence
        {
            return Err(CatalogError::BackupRestoreConflict);
        }
        self.authority
            .verify_restore_baseline(self.baseline, &self.cancellation)?;
        self.authority.revalidate_restore_state(self.state_receipt)
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        CatalogAuthority,
        AuthoritySnapshot,
        BackupReceipt,
        RestoreCatalogBaseline,
        CancellationToken,
    ) {
        (
            self.authority,
            self.snapshot,
            self.state_receipt,
            self.baseline,
            self.cancellation,
        )
    }
}

impl std::fmt::Debug for VerifiedRestoreCatalogAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VerifiedRestoreCatalogAuthority")
            .field("authority", &"[RETAINED CATALOG WRITER]")
            .field("snapshot", &self.snapshot)
            .field("state_receipt", &self.state_receipt)
            .finish()
    }
}

/// Non-forgeable, retained-capability proof admitted by the sealed authority transition service.
pub(crate) struct VerifiedRestoreHandoff {
    source_catalog: VerifiedBackupCatalog,
    target_catalog: VerifiedRestoreCatalogAuthority,
    materialized_root: MaterializedArtifactRoot,
    source_evidence: ReceiptValidatedRestoreEvidence,
}

impl VerifiedRestoreHandoff {
    pub(super) const fn receipt(&self) -> AnalyticalBackupBundleReceipt {
        self.source_evidence.receipt
    }

    pub(super) fn into_retained_parts(
        self,
    ) -> (
        VerifiedBackupCatalog,
        VerifiedRestoreCatalogAuthority,
        MaterializedArtifactRoot,
        ReceiptValidatedRestoreEvidence,
    ) {
        (
            self.source_catalog,
            self.target_catalog,
            self.materialized_root,
            self.source_evidence,
        )
    }
}

pub(super) fn target_authority_evidence_from_receipt(
    receipt: AnalyticalBackupBundleReceipt,
    target_catalog: CatalogEndpointIdentity,
    target_root: RootEndpointIdentity,
) -> Result<AuthorityEvidenceDigest, RestoreValidationError> {
    let backup = receipt.catalog_backup();
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/verified-backup-restore-authority/v1");
    digest.update(receipt.source_catalog_identity().bytes());
    digest.update(receipt.source_authority_generation().get().to_be_bytes());
    digest.update(receipt.source_authority_event().bytes());
    digest.update(receipt.source_authority_evidence().bytes());
    digest.update(receipt.catalog_content_evidence().bytes());
    digest.update(receipt.source_root_identity().bytes());
    digest.update(receipt.cutoff().unix_nanos().to_be_bytes());
    digest.update(receipt.artifact_count().to_be_bytes());
    digest.update(receipt.artifact_bytes().to_be_bytes());
    digest.update(receipt.artifact_inventory_sha256().bytes());
    digest.update(backup.version().to_be_bytes());
    digest.update(backup.byte_length().to_be_bytes());
    digest.update(backup.sha256());
    digest.update(target_catalog.bytes());
    digest.update(target_root.bytes());
    AuthorityEvidenceDigest::try_new(digest.finalize().into())
        .ok_or(RestoreValidationError::InvalidAuthorityEvidence)
}

impl std::fmt::Debug for VerifiedRestoreHandoff {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VerifiedRestoreHandoff")
            .field("source_catalog", &self.source_catalog)
            .field("target_catalog", &self.target_catalog)
            .field("materialized_root", &self.materialized_root)
            .field("source_evidence", &self.source_evidence)
            .finish()
    }
}

/// Installs verified artifacts only after the exact catalog backup is retained at destination.
pub(crate) fn materialize_verified_restore(
    source_catalog: VerifiedBackupCatalog,
    target_catalog: VerifiedRestoreCatalogAuthority,
    source_evidence: ReceiptValidatedRestoreEvidence,
    destination: &ArtifactRoot,
    mode: RestoreArtifactMode,
    cancellation: &CancellationToken,
) -> Result<VerifiedRestoreHandoff, RestoreValidationError> {
    let expected_catalog = *source_evidence.receipt.catalog_backup();
    require_catalog_receipt(source_catalog.receipt(), expected_catalog)?;
    if cancellation.is_cancelled() {
        return Err(RestoreValidationError::Cancelled);
    }
    source_catalog.revalidate()?;
    target_catalog.revalidate()?;
    let materialized_root = match mode {
        RestoreArtifactMode::Fresh => source_evidence
            .artifact_inventory
            .materialize_no_replace(destination, cancellation)?,
        RestoreArtifactMode::ResumeExactSubset => {
            let (prepared, catalog_bound) = match target_catalog.snapshot().state() {
                AuthorityState::Prepared { transition, .. }
                    if transition.kind() == super::AuthorityTransitionKind::BackupRestore =>
                {
                    (transition, false)
                }
                AuthorityState::Bound { transition, .. }
                    if transition.prepared().kind()
                        == super::AuthorityTransitionKind::BackupRestore =>
                {
                    (transition.prepared(), true)
                }
                AuthorityState::InitializationRequired
                | AuthorityState::LegacyRequired { .. }
                | AuthorityState::Prepared { .. }
                | AuthorityState::Bound { .. } => {
                    return Err(RestoreValidationError::CatalogReceiptMismatch);
                }
            };
            let directory = destination
                .try_clone_directory()
                .map_err(|_| EvidenceError::DestinationConflict)?;
            let controls = ParquetObjectStore::validate_restore_control_subset(
                &directory,
                prepared,
                catalog_bound,
            )
            .map_err(|_| EvidenceError::DestinationConflict)?;
            source_evidence
                .artifact_inventory
                .resume_exact_subset_no_replace(destination, cancellation, &controls)?
        }
    };
    source_catalog.revalidate()?;
    target_catalog.revalidate()?;
    Ok(VerifiedRestoreHandoff {
        source_catalog,
        target_catalog,
        materialized_root,
        source_evidence,
    })
}

fn require_catalog_receipt(
    actual: BackupReceipt,
    expected: BackupReceipt,
) -> Result<(), RestoreValidationError> {
    if actual == expected {
        Ok(())
    } else {
        Err(RestoreValidationError::CatalogReceiptMismatch)
    }
}

fn validate_receipt_summary(
    receipt: &AnalyticalBackupBundleReceipt,
    summary: RestoreEvidenceSummary,
) -> Result<(), RestoreValidationError> {
    if receipt.source_catalog_identity() != summary.catalog_identity
        || receipt.source_authority_generation() != summary.authority_generation
        || receipt.source_authority_event() != summary.authority_event
        || receipt.source_authority_evidence() != summary.source_authority_evidence
        || receipt.catalog_content_evidence() != summary.catalog_content_evidence
        || receipt.source_root_identity() != summary.stable_root_identity
        || receipt.cutoff() != summary.cutoff
        || receipt.artifact_count() != summary.artifact_count
        || receipt.artifact_bytes() != summary.artifact_bytes
        || receipt.artifact_inventory_sha256() != summary.artifact_inventory
    {
        return Err(RestoreValidationError::ReceiptMismatch);
    }
    Ok(())
}

/// Typed failure before a restore has permission to mutate a destination endpoint.
#[derive(Debug, Error)]
pub(crate) enum RestoreValidationError {
    /// The source catalog lacks one fully validated bound authority relationship.
    #[error("analytical restore source authority is not bound")]
    AuthorityNotBound,
    /// Receipt fields differ from exact catalog or artifact evidence.
    #[error("analytical restore evidence does not match the bundle receipt")]
    ReceiptMismatch,
    /// An attacker-controlled evidence count could not be represented.
    #[error("analytical restore evidence exceeds a fixed resource ceiling")]
    ResourceLimitExceeded,
    /// The caller cancelled source verification before destination mutation.
    #[error("analytical restore source verification was cancelled")]
    Cancelled,
    /// Retained source or installed destination catalog differs from the bundle receipt.
    #[error("analytical restore catalog does not match the bundle receipt")]
    CatalogReceiptMismatch,
    /// The complete restore proof could not produce a valid transition evidence identity.
    #[error("analytical restore authority evidence identity is invalid")]
    InvalidAuthorityEvidence,
    /// Exact retained source-catalog verification failed during restore materialization.
    #[error("analytical restore catalog verification failed: {0}")]
    Catalog(#[from] CatalogError),
    /// Catalog relationships or exact source artifacts failed validation.
    #[error("analytical restore evidence validation failed: {0}")]
    Evidence(#[from] EvidenceError),
}

#[cfg(test)]
mod tests {
    use market_squawk_domain::Timestamp;

    use super::{RestoreEvidenceSummary, RestoreValidationError, validate_receipt_summary};
    use crate::BackupReceipt;
    use crate::analytical_backup::AnalyticalBackupBundleReceipt;
    use crate::authority_transition::evidence::CatalogContentEvidenceDigest;
    use crate::authority_transition::{
        ArtifactInventoryDigest, AuthorityEventDigest, AuthorityEvidenceDigest,
        AuthorityGeneration, CatalogEndpointIdentity, StableArtifactRootIdentity,
    };

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    fn summary() -> TestResult<RestoreEvidenceSummary> {
        Ok(RestoreEvidenceSummary {
            catalog_identity: CatalogEndpointIdentity::try_new([3; 32])
                .ok_or("invalid catalog identity")?,
            authority_generation: AuthorityGeneration::try_new(7)
                .ok_or("invalid authority generation")?,
            authority_event: AuthorityEventDigest::try_new([5; 32])
                .ok_or("invalid authority event")?,
            source_authority_evidence: AuthorityEvidenceDigest::try_new([7; 32])
                .ok_or("invalid authority evidence")?,
            catalog_content_evidence: CatalogContentEvidenceDigest::try_new([9; 32])
                .ok_or("invalid catalog content evidence")?,
            stable_root_identity: StableArtifactRootIdentity::try_new([11; 32])
                .ok_or("invalid root identity")?,
            cutoff: Timestamp::from_unix_nanos(1_721_491_200_000_000_000),
            artifact_count: 2,
            artifact_bytes: 16_384,
            artifact_inventory: ArtifactInventoryDigest::try_new([13; 32])
                .ok_or("invalid inventory digest")?,
        })
    }

    fn receipt(summary: RestoreEvidenceSummary) -> TestResult<AnalyticalBackupBundleReceipt> {
        let catalog = BackupReceipt::try_from_parts(BackupReceipt::VERSION, 8_192, [17; 32])?;
        Ok(AnalyticalBackupBundleReceipt::try_from_parts(
            catalog,
            summary.catalog_identity,
            summary.authority_generation,
            summary.authority_event,
            summary.source_authority_evidence,
            summary.catalog_content_evidence,
            summary.stable_root_identity,
            summary.cutoff,
            summary.artifact_count,
            summary.artifact_bytes,
            summary.artifact_inventory,
        )?)
    }

    #[test]
    fn preflight_accepts_only_an_exact_receipt_bound_summary() -> TestResult {
        let expected = summary()?;
        let receipt = receipt(expected)?;

        assert!(validate_receipt_summary(&receipt, expected).is_ok());

        let changed = RestoreEvidenceSummary {
            artifact_bytes: expected.artifact_bytes + 1,
            ..expected
        };
        assert!(matches!(
            validate_receipt_summary(&receipt, changed),
            Err(RestoreValidationError::ReceiptMismatch)
        ));
        Ok(())
    }
}
