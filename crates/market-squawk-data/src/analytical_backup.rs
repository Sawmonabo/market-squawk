//! Path-free, tamper-evident analytical backup bundle receipts.

use std::fmt;
use std::sync::{Arc, Mutex};

use market_squawk_domain::Timestamp;
use market_squawk_platform::{ArtifactRoot, CatalogLocation};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::sync::{Mutex as TokioMutex, OwnedMutexGuard};
use tokio_util::sync::CancellationToken;

use crate::BackupReceipt;
use crate::authority_transition::evidence::fs::{
    MaterializedArtifactRoot, VerifiedArtifactInventory, verify_artifact_inventory,
};
use crate::authority_transition::evidence::{
    CatalogContentEvidenceDigest, CatalogEvidenceSnapshot, EvidenceError, EvidenceLimits,
    EvidenceSnapshotRequest,
};
use crate::authority_transition::restore::{
    ReceiptValidatedRestoreEvidence, RestoreArtifactMode, RestoreValidationError,
    materialize_verified_restore, validate_restore_evidence,
};
use crate::authority_transition::{
    ArtifactInventoryDigest, AuthorityEventDigest, AuthorityEvidenceDigest, AuthorityGeneration,
    AuthoritySnapshot, AuthorityTransitionService, CatalogEndpointIdentity,
    StableArtifactRootIdentity,
};
use crate::{
    AnalyticalDataService, AnalyticalManifestCatalog, Catalog, CatalogAuthority, CatalogConfig,
    CatalogError, ManifestCatalogError, ObjectStoreConfig, ParquetObjectStore, ParquetStoreError,
};

pub(crate) const MAX_RECEIPT_ARTIFACTS: u64 = 100_000;
const MAX_RECEIPT_ARTIFACT_BYTES: u64 = 16 * 1024 * 1024 * 1024 * 1024;

/// Retained local capabilities containing one exact analytical backup bundle.
#[derive(Clone)]
pub struct AnalyticalBackupLocation {
    catalog: CatalogLocation,
    artifacts: ArtifactRoot,
}

impl fmt::Debug for AnalyticalBackupLocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AnalyticalBackupLocation([RETAINED LOCAL CAPABILITIES])")
    }
}

impl AnalyticalBackupLocation {
    /// Binds a prepared catalog placement to a separately retained artifact directory.
    pub fn try_new(
        catalog: CatalogLocation,
        artifacts: ArtifactRoot,
    ) -> Result<Self, AnalyticalBackupError> {
        require_disjoint_endpoints(&catalog, &artifacts)?;
        Ok(Self { catalog, artifacts })
    }

    /// Returns the fixed catalog placement capability.
    pub const fn catalog(&self) -> &CatalogLocation {
        &self.catalog
    }

    /// Returns the retained artifact-directory capability.
    pub const fn artifacts(&self) -> &ArtifactRoot {
        &self.artifacts
    }
}

/// Caller-selected analytical backup resource limits, capped by fixed process ceilings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AnalyticalBackupLimits {
    evidence: EvidenceLimits,
}

impl AnalyticalBackupLimits {
    /// Constructs bounded catalog, artifact, and Parquet-metadata verification limits.
    pub fn try_new(
        max_artifacts: usize,
        max_references: usize,
        max_total_bytes: u64,
        max_object_bytes: u64,
        max_parquet_metadata_bytes: u64,
    ) -> Result<Self, AnalyticalBackupError> {
        let evidence = EvidenceLimits::try_new(
            max_artifacts,
            max_references,
            max_total_bytes,
            max_object_bytes,
            max_parquet_metadata_bytes,
        )
        .map_err(|error| {
            if matches!(error, EvidenceError::InvalidLimits) {
                AnalyticalBackupError::InvalidConfiguration
            } else {
                AnalyticalBackupError::evidence(error)
            }
        })?;
        Ok(Self { evidence })
    }

    const fn request(self, cutoff: Timestamp) -> EvidenceSnapshotRequest {
        EvidenceSnapshotRequest::new(cutoff, self.evidence)
    }
}

/// Explicit no-replace artifact policy for an exact restore retry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AnalyticalRestoreMode {
    /// Require a completely empty retained destination artifact root.
    Fresh,
    /// Admit only an exact verified subset belonging to this same backup receipt.
    ResumeExactSubset,
}

impl From<AnalyticalRestoreMode> for RestoreArtifactMode {
    fn from(value: AnalyticalRestoreMode) -> Self {
        match value {
            AnalyticalRestoreMode::Fresh => Self::Fresh,
            AnalyticalRestoreMode::ResumeExactSubset => Self::ResumeExactSubset,
        }
    }
}

/// Complete local configuration for one fresh analytical restore target.
pub struct AnalyticalRestoreTarget {
    catalog: CatalogConfig,
    artifacts: ArtifactRoot,
    max_objects_per_generation: usize,
    objects: ObjectStoreConfig,
    mode: AnalyticalRestoreMode,
}

impl fmt::Debug for AnalyticalRestoreTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AnalyticalRestoreTarget")
            .field("catalog", &"[PREPARED CATALOG CAPABILITY]")
            .field("artifacts", &"[PREPARED ARTIFACT CAPABILITY]")
            .field(
                "max_objects_per_generation",
                &self.max_objects_per_generation,
            )
            .field("objects", &self.objects)
            .field("mode", &self.mode)
            .finish()
    }
}

impl AnalyticalRestoreTarget {
    /// Binds catalog, artifact, manifest, object-store, and retry policy into one target.
    pub fn try_new(
        catalog: CatalogConfig,
        artifacts: ArtifactRoot,
        max_objects_per_generation: usize,
        objects: ObjectStoreConfig,
        mode: AnalyticalRestoreMode,
    ) -> Result<Self, AnalyticalBackupError> {
        if !(1..=1024).contains(&max_objects_per_generation) {
            return Err(AnalyticalBackupError::InvalidConfiguration);
        }
        require_disjoint_endpoints(catalog.location(), &artifacts)?;
        Ok(Self {
            catalog,
            artifacts,
            max_objects_per_generation,
            objects,
            mode,
        })
    }
}

/// Shared async admission gate for every mutation of one analytical catalog/root composition.
#[derive(Clone, Default)]
pub(crate) struct AnalyticalOperationGate {
    owner: Arc<TokioMutex<()>>,
}

impl AnalyticalOperationGate {
    /// Acquires cancellable analytical-operation admission without holding a standard mutex.
    pub(crate) async fn acquire(
        &self,
        cancellation: &CancellationToken,
    ) -> Option<AnalyticalOperationLease> {
        tokio::select! {
            guard = Arc::clone(&self.owner).lock_owned() => Some(AnalyticalOperationLease {
                _guard: guard,
            }),
            _ = cancellation.cancelled() => None,
        }
    }

    /// Acquires admission for a legacy operation whose public contract is not yet cancellable.
    pub(crate) async fn acquire_uninterruptible(&self) -> AnalyticalOperationLease {
        AnalyticalOperationLease {
            _guard: Arc::clone(&self.owner).lock_owned().await,
        }
    }
}

impl fmt::Debug for AnalyticalOperationGate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AnalyticalOperationGate([SEALED ASYNC ADMISSION])")
    }
}

/// Non-cloneable proof of exclusive analytical-operation admission.
pub(crate) struct AnalyticalOperationLease {
    _guard: OwnedMutexGuard<()>,
}

/// Sealed backup authority for one active analytical catalog/root composition.
pub struct AnalyticalBackupService {
    gate: AnalyticalOperationGate,
    authority: Arc<Mutex<CatalogAuthority>>,
    objects: Arc<ParquetObjectStore>,
}

impl AnalyticalBackupService {
    pub(crate) fn new(
        gate: AnalyticalOperationGate,
        authority: Arc<Mutex<CatalogAuthority>>,
        objects: Arc<ParquetObjectStore>,
    ) -> Self {
        Self {
            gate,
            authority,
            objects,
        }
    }

    /// Creates and reopens one consistent, exact, no-replace analytical backup bundle.
    pub async fn create(
        &self,
        destination: AnalyticalBackupLocation,
        cutoff: Timestamp,
        limits: AnalyticalBackupLimits,
        cancellation: &CancellationToken,
    ) -> Result<VerifiedAnalyticalBackup, AnalyticalBackupError> {
        let _operation = self
            .gate
            .acquire(cancellation)
            .await
            .ok_or(AnalyticalBackupError::Cancelled)?;
        let _publication = self.objects.begin_publication(cancellation).await?;
        let request = limits.request(cutoff);
        let (source_authority, source_evidence) = {
            let authority = self
                .authority
                .lock()
                .map_err(|_| AnalyticalBackupError::LockPoisoned)?;
            let (source_authority, source_evidence) =
                authority.analytical_evidence_snapshot(request)?;
            require_source_composition(&source_authority, &self.objects)?;
            (source_authority, source_evidence)
        };
        let source_root = self.objects.try_clone_artifact_root()?;
        let source_inventory =
            verify_artifact_inventory(&source_root, &source_evidence, cancellation)
                .map_err(AnalyticalBackupError::evidence)?;
        let catalog_receipt = self
            .authority
            .lock()
            .map_err(|_| AnalyticalBackupError::LockPoisoned)?
            .backup_to(destination.catalog())
            .map_err(map_catalog_backup_creation_error)?;
        let materialized = source_inventory
            .materialize_no_replace(destination.artifacts(), cancellation)
            .map_err(|_| AnalyticalBackupError::BundleCreationIndeterminate)?;
        verify_created_bundle(
            destination,
            catalog_receipt,
            request,
            source_authority,
            source_evidence,
            source_inventory,
            materialized,
            cancellation,
        )
        .map_err(|_| AnalyticalBackupError::BundleCreationIndeterminate)
    }

    /// Reopens and retains an existing exact bundle before any restore is allowed.
    pub fn open_verified(
        location: AnalyticalBackupLocation,
        receipt: AnalyticalBackupBundleReceipt,
        limits: AnalyticalBackupLimits,
        cancellation: &CancellationToken,
    ) -> Result<VerifiedAnalyticalBackup, AnalyticalBackupError> {
        let source_catalog =
            Catalog::verify_backup_retained(location.catalog(), receipt.catalog_backup())?;
        let request = limits.request(receipt.cutoff());
        let (authority, evidence) = Catalog::verified_backup_evidence(&source_catalog, request)?;
        let inventory = verify_artifact_inventory(location.artifacts(), &evidence, cancellation)
            .map_err(AnalyticalBackupError::evidence)?;
        let source_evidence =
            validate_restore_evidence(receipt, authority, evidence, inventory, cancellation)
                .map_err(AnalyticalBackupError::restore_validation)?;
        Ok(VerifiedAnalyticalBackup {
            source_catalog,
            source_evidence,
        })
    }
}

impl fmt::Debug for AnalyticalBackupService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AnalyticalBackupService([SEALED CATALOG/ROOT AUTHORITY])")
    }
}

/// Exact retained backup capabilities admitted for no-replace restore.
pub struct VerifiedAnalyticalBackup {
    source_catalog: crate::catalog::VerifiedBackupCatalog,
    source_evidence: ReceiptValidatedRestoreEvidence,
}

impl VerifiedAnalyticalBackup {
    /// Returns the path-free exact bundle receipt.
    pub const fn receipt(&self) -> AnalyticalBackupBundleReceipt {
        self.source_evidence.receipt()
    }

    /// Restores the complete bundle to fresh or exact-retry local endpoints.
    pub fn restore(
        self,
        target: AnalyticalRestoreTarget,
        cancellation: &CancellationToken,
    ) -> Result<AnalyticalDataService, AnalyticalBackupError> {
        if cancellation.is_cancelled() {
            return Err(AnalyticalBackupError::Cancelled);
        }
        let catalog_location = target.catalog.location().clone();
        let installed =
            Catalog::install_verified_backup_no_replace(&self.source_catalog, &catalog_location)
                .map_err(map_catalog_restore_install_error)?;
        let handoff = materialize_verified_restore(
            self.source_catalog,
            installed,
            self.source_evidence,
            &target.artifacts,
            target.mode.into(),
            cancellation,
        )
        .map_err(|_| AnalyticalBackupError::RestoreIndeterminate)?;
        let (authority, objects) =
            AuthorityTransitionService::restore(handoff, target.catalog, target.objects)
                .map_err(|_| AnalyticalBackupError::RestoreIndeterminate)?;
        let manifests =
            AnalyticalManifestCatalog::open(&catalog_location, target.max_objects_per_generation)?;
        Ok(AnalyticalDataService::from_active_parts(
            authority, manifests, objects,
        ))
    }
}

impl fmt::Debug for VerifiedAnalyticalBackup {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("VerifiedAnalyticalBackup([RETAINED EXACT BUNDLE])")
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the verifier compares every independently retained bundle component"
)]
fn verify_created_bundle(
    destination: AnalyticalBackupLocation,
    catalog_receipt: BackupReceipt,
    request: EvidenceSnapshotRequest,
    source_authority: AuthoritySnapshot,
    source_evidence: CatalogEvidenceSnapshot,
    source_inventory: VerifiedArtifactInventory,
    _materialized: MaterializedArtifactRoot,
    cancellation: &CancellationToken,
) -> Result<VerifiedAnalyticalBackup, AnalyticalBackupError> {
    let source_catalog = Catalog::verify_backup_retained(destination.catalog(), &catalog_receipt)?;
    let (backup_authority, backup_evidence) =
        Catalog::verified_backup_evidence(&source_catalog, request)?;
    if source_authority != backup_authority
        || source_evidence
            .evidence_digest()
            .map_err(AnalyticalBackupError::evidence)?
            != backup_evidence
                .evidence_digest()
                .map_err(AnalyticalBackupError::evidence)?
    {
        return Err(AnalyticalBackupError::BundleCreationIndeterminate);
    }
    let backup_inventory =
        verify_artifact_inventory(destination.artifacts(), &backup_evidence, cancellation)
            .map_err(AnalyticalBackupError::evidence)?;
    if source_inventory.digest() != backup_inventory.digest()
        || source_inventory.total_bytes() != backup_inventory.total_bytes()
        || source_inventory.artifacts().len() != backup_inventory.artifacts().len()
    {
        return Err(AnalyticalBackupError::BundleCreationIndeterminate);
    }
    let receipt = issue_verified_bundle_receipt(
        catalog_receipt,
        &backup_authority,
        &backup_evidence,
        &backup_inventory,
    )?;
    let source_evidence = validate_restore_evidence(
        receipt,
        backup_authority,
        backup_evidence,
        backup_inventory,
        cancellation,
    )
    .map_err(AnalyticalBackupError::restore_validation)?;
    Ok(VerifiedAnalyticalBackup {
        source_catalog,
        source_evidence,
    })
}

fn require_source_composition(
    authority: &AuthoritySnapshot,
    objects: &ParquetObjectStore,
) -> Result<(), AnalyticalBackupError> {
    let bound = authority
        .bound()
        .ok_or(AnalyticalBackupError::SourceCompositionMismatch)?;
    if bound.stable_root_identity().bytes() != objects.stable_root_identity() {
        return Err(AnalyticalBackupError::SourceCompositionMismatch);
    }
    Ok(())
}

fn require_disjoint_endpoints(
    catalog: &CatalogLocation,
    artifacts: &ArtifactRoot,
) -> Result<(), AnalyticalBackupError> {
    let catalog_file = catalog.path();
    let artifact_root = artifacts.root();
    if catalog_file.starts_with(artifact_root) || artifact_root.starts_with(catalog_file) {
        return Err(AnalyticalBackupError::InvalidConfiguration);
    }
    Ok(())
}

fn map_catalog_backup_creation_error(error: CatalogError) -> AnalyticalBackupError {
    match error {
        CatalogError::BackupPublicationIndeterminate { .. }
        | CatalogError::BackupPublishedWithCleanupPending { .. } => {
            AnalyticalBackupError::BundleCreationIndeterminate
        }
        other => AnalyticalBackupError::Catalog(other),
    }
}

fn map_catalog_restore_install_error(error: CatalogError) -> AnalyticalBackupError {
    if matches!(error, CatalogError::BackupRestoreIndeterminate) {
        AnalyticalBackupError::RestoreIndeterminate
    } else {
        AnalyticalBackupError::Catalog(error)
    }
}

/// Typed failure of consistent analytical backup creation, verification, or restore.
#[derive(Debug, Error)]
pub enum AnalyticalBackupError {
    /// Caller-selected configuration violates fixed process limits.
    #[error("analytical backup configuration is invalid")]
    InvalidConfiguration,
    /// The operation was cancelled at a safe cancellation boundary.
    #[error("analytical backup operation was cancelled")]
    Cancelled,
    /// A poisoned process mutex prevents proof of exclusive catalog composition.
    #[error("analytical backup catalog authority lock is poisoned")]
    LockPoisoned,
    /// Bound catalog authority and the retained artifact store name different roots.
    #[error("analytical backup source catalog/root composition does not match")]
    SourceCompositionMismatch,
    /// Destination publication may be partial and must be inspected through exact recovery.
    #[error("analytical backup bundle creation is indeterminate")]
    BundleCreationIndeterminate,
    /// Restore publication may be partial and must be resumed with the same exact receipt.
    #[error("analytical backup restore is indeterminate")]
    RestoreIndeterminate,
    /// SQLite catalog preparation or exact-retention validation failed.
    #[error("analytical backup catalog operation failed: {0}")]
    Catalog(#[from] CatalogError),
    /// Artifact-root publication or authority activation failed.
    #[error("analytical backup artifact operation failed: {0}")]
    Artifact(#[from] ParquetStoreError),
    /// Manifest-catalog activation after exact restore failed.
    #[error("analytical backup manifest activation failed: {0}")]
    Manifest(#[from] ManifestCatalogError),
    /// The path-free exact bundle receipt is invalid.
    #[error("analytical backup receipt failed validation: {0}")]
    Receipt(#[from] AnalyticalBackupReceiptError),
    /// Catalog relationships or physical artifacts failed exact evidence validation.
    #[error("analytical backup evidence validation failed: {source}")]
    Evidence {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// A receipt-verified bundle failed sealed restore preflight.
    #[error("analytical backup restore preflight failed: {source}")]
    RestorePreflight {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl AnalyticalBackupError {
    fn evidence(error: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Evidence {
            source: Box::new(error),
        }
    }

    fn restore_validation(error: RestoreValidationError) -> Self {
        if matches!(error, RestoreValidationError::Cancelled) {
            Self::Cancelled
        } else {
            Self::RestorePreflight {
                source: Box::new(error),
            }
        }
    }
}

impl fmt::Debug for AnalyticalOperationLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AnalyticalOperationLease([EXCLUSIVE])")
    }
}

pub(crate) fn issue_verified_bundle_receipt(
    catalog_backup: BackupReceipt,
    authority: &AuthoritySnapshot,
    evidence: &CatalogEvidenceSnapshot,
    inventory: &VerifiedArtifactInventory,
) -> Result<AnalyticalBackupBundleReceipt, AnalyticalBackupReceiptError> {
    let head = authority
        .head()
        .ok_or(AnalyticalBackupReceiptError::AuthorityNotBound)?;
    let bound = authority
        .bound()
        .ok_or(AnalyticalBackupReceiptError::AuthorityNotBound)?;
    let prepared = bound.prepared();
    let catalog_content_evidence = evidence
        .evidence_digest()
        .map_err(|_| AnalyticalBackupReceiptError::InvalidMetadata)?;
    let artifact_count = u64::try_from(inventory.artifacts().len())
        .map_err(|_| AnalyticalBackupReceiptError::ResourceLimitExceeded)?;
    AnalyticalBackupBundleReceipt::try_from_parts(
        catalog_backup,
        prepared.target_catalog_identity(),
        prepared.authority_generation(),
        head.event_digest(),
        prepared.evidence_digest(),
        catalog_content_evidence,
        bound.stable_root_identity(),
        evidence.request().cutoff(),
        artifact_count,
        inventory.total_bytes(),
        inventory.digest(),
    )
}

/// Exact identity and bounded inventory summary for one consistent analytical backup bundle.
///
/// The receipt deliberately carries no source or destination path. A restore operation must pair
/// it with separately retained directory and catalog capabilities, revalidate every byte against
/// the receipt, and reject a changed capability before mutating a fresh destination.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct AnalyticalBackupBundleReceipt {
    version: u16,
    catalog_backup: BackupReceipt,
    source_catalog_identity: CatalogEndpointIdentity,
    source_authority_generation: AuthorityGeneration,
    source_authority_event: AuthorityEventDigest,
    source_authority_evidence: AuthorityEvidenceDigest,
    catalog_content_evidence: CatalogContentEvidenceDigest,
    source_root_identity: StableArtifactRootIdentity,
    cutoff: Timestamp,
    artifact_count: u64,
    artifact_bytes: u64,
    artifact_inventory_sha256: ArtifactInventoryDigest,
    bundle_sha256: [u8; 32],
}

impl AnalyticalBackupBundleReceipt {
    /// Current durable receipt schema.
    pub const VERSION: u16 = 1;

    /// Constructs a receipt and computes its canonical bundle identity.
    #[allow(
        clippy::too_many_arguments,
        reason = "each receipt field independently binds durable source or inventory evidence"
    )]
    pub fn try_from_parts(
        catalog_backup: BackupReceipt,
        source_catalog_identity: CatalogEndpointIdentity,
        source_authority_generation: AuthorityGeneration,
        source_authority_event: AuthorityEventDigest,
        source_authority_evidence: AuthorityEvidenceDigest,
        catalog_content_evidence: CatalogContentEvidenceDigest,
        source_root_identity: StableArtifactRootIdentity,
        cutoff: Timestamp,
        artifact_count: u64,
        artifact_bytes: u64,
        artifact_inventory_sha256: ArtifactInventoryDigest,
    ) -> Result<Self, AnalyticalBackupReceiptError> {
        let bundle_sha256 = bundle_digest(
            Self::VERSION,
            &catalog_backup,
            source_catalog_identity,
            source_authority_generation,
            source_authority_event,
            source_authority_evidence,
            catalog_content_evidence,
            source_root_identity,
            cutoff,
            artifact_count,
            artifact_bytes,
            artifact_inventory_sha256,
        );
        Self::try_from_exact_parts(
            Self::VERSION,
            catalog_backup,
            source_catalog_identity,
            source_authority_generation,
            source_authority_event,
            source_authority_evidence,
            catalog_content_evidence,
            source_root_identity,
            cutoff,
            artifact_count,
            artifact_bytes,
            artifact_inventory_sha256,
            bundle_sha256,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "deserialization must validate every independently persisted receipt field"
    )]
    fn try_from_exact_parts(
        version: u16,
        catalog_backup: BackupReceipt,
        source_catalog_identity: CatalogEndpointIdentity,
        source_authority_generation: AuthorityGeneration,
        source_authority_event: AuthorityEventDigest,
        source_authority_evidence: AuthorityEvidenceDigest,
        catalog_content_evidence: CatalogContentEvidenceDigest,
        source_root_identity: StableArtifactRootIdentity,
        cutoff: Timestamp,
        artifact_count: u64,
        artifact_bytes: u64,
        artifact_inventory_sha256: ArtifactInventoryDigest,
        bundle_sha256: [u8; 32],
    ) -> Result<Self, AnalyticalBackupReceiptError> {
        if version != Self::VERSION {
            return Err(AnalyticalBackupReceiptError::UnsupportedVersion);
        }
        BackupReceipt::try_from_parts(
            catalog_backup.version(),
            catalog_backup.byte_length(),
            catalog_backup.sha256(),
        )
        .map_err(|_| AnalyticalBackupReceiptError::InvalidCatalogReceipt)?;
        if (artifact_count == 0) != (artifact_bytes == 0) {
            return Err(AnalyticalBackupReceiptError::InvalidMetadata);
        }
        if artifact_count > MAX_RECEIPT_ARTIFACTS || artifact_bytes > MAX_RECEIPT_ARTIFACT_BYTES {
            return Err(AnalyticalBackupReceiptError::ResourceLimitExceeded);
        }
        let expected = bundle_digest(
            version,
            &catalog_backup,
            source_catalog_identity,
            source_authority_generation,
            source_authority_event,
            source_authority_evidence,
            catalog_content_evidence,
            source_root_identity,
            cutoff,
            artifact_count,
            artifact_bytes,
            artifact_inventory_sha256,
        );
        if expected != bundle_sha256 {
            return Err(AnalyticalBackupReceiptError::BundleDigestMismatch);
        }
        Ok(Self {
            version,
            catalog_backup,
            source_catalog_identity,
            source_authority_generation,
            source_authority_event,
            source_authority_evidence,
            catalog_content_evidence,
            source_root_identity,
            cutoff,
            artifact_count,
            artifact_bytes,
            artifact_inventory_sha256,
            bundle_sha256,
        })
    }

    /// Returns the durable receipt schema version.
    pub const fn version(&self) -> u16 {
        self.version
    }

    /// Returns the exact SQLite backup receipt.
    pub const fn catalog_backup(&self) -> &BackupReceipt {
        &self.catalog_backup
    }

    /// Returns the exact source catalog endpoint identity.
    pub const fn source_catalog_identity(&self) -> CatalogEndpointIdentity {
        self.source_catalog_identity
    }

    /// Returns the append-only source authority generation.
    pub const fn source_authority_generation(&self) -> AuthorityGeneration {
        self.source_authority_generation
    }

    /// Returns the exact source authority event hash.
    pub const fn source_authority_event(&self) -> AuthorityEventDigest {
        self.source_authority_event
    }

    /// Returns the immutable evidence committed by the source bound authority transition.
    pub const fn source_authority_evidence(&self) -> AuthorityEvidenceDigest {
        self.source_authority_evidence
    }

    /// Returns the snapshot-time relationship-bearing catalog content identity.
    pub const fn catalog_content_evidence(&self) -> CatalogContentEvidenceDigest {
        self.catalog_content_evidence
    }

    /// Returns the stable source artifact-root identity.
    pub const fn source_root_identity(&self) -> StableArtifactRootIdentity {
        self.source_root_identity
    }

    /// Returns the catalog-consistent cutoff in signed Unix nanoseconds.
    pub const fn cutoff(&self) -> Timestamp {
        self.cutoff
    }

    /// Returns the number of distinct retained artifact objects.
    pub const fn artifact_count(&self) -> u64 {
        self.artifact_count
    }

    /// Returns the checked total bytes across distinct retained artifact objects.
    pub const fn artifact_bytes(&self) -> u64 {
        self.artifact_bytes
    }

    /// Returns the canonical ordered inventory digest.
    pub const fn artifact_inventory_sha256(&self) -> ArtifactInventoryDigest {
        self.artifact_inventory_sha256
    }

    /// Returns the digest that binds every field in this receipt.
    pub const fn bundle_sha256(&self) -> [u8; 32] {
        self.bundle_sha256
    }
}

impl fmt::Debug for AnalyticalBackupBundleReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AnalyticalBackupBundleReceipt")
            .field("version", &self.version)
            .field("catalog_backup", &self.catalog_backup)
            .field("source_catalog_identity", &"[SHA-256]")
            .field(
                "source_authority_generation",
                &self.source_authority_generation,
            )
            .field("source_authority_event", &"[SHA-256]")
            .field("source_authority_evidence", &"[SHA-256]")
            .field("catalog_content_evidence", &"[SHA-256]")
            .field("source_root_identity", &"[SHA-256]")
            .field("cutoff", &self.cutoff)
            .field("artifact_count", &self.artifact_count)
            .field("artifact_bytes", &self.artifact_bytes)
            .field("artifact_inventory_sha256", &"[SHA-256]")
            .field("bundle_sha256", &"[SHA-256]")
            .finish()
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AnalyticalBackupBundleReceiptWire {
    version: u16,
    catalog_backup: BackupReceipt,
    source_catalog_identity: [u8; 32],
    source_authority_generation: u64,
    source_authority_event: [u8; 32],
    source_authority_evidence: [u8; 32],
    catalog_content_evidence: [u8; 32],
    source_root_identity: [u8; 32],
    cutoff_ns: i64,
    artifact_count: u64,
    artifact_bytes: u64,
    artifact_inventory_sha256: [u8; 32],
    bundle_sha256: [u8; 32],
}

impl Serialize for AnalyticalBackupBundleReceipt {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        AnalyticalBackupBundleReceiptWire {
            version: self.version,
            catalog_backup: self.catalog_backup,
            source_catalog_identity: self.source_catalog_identity.bytes(),
            source_authority_generation: self.source_authority_generation.get(),
            source_authority_event: self.source_authority_event.bytes(),
            source_authority_evidence: self.source_authority_evidence.bytes(),
            catalog_content_evidence: self.catalog_content_evidence.bytes(),
            source_root_identity: self.source_root_identity.bytes(),
            cutoff_ns: self.cutoff.unix_nanos(),
            artifact_count: self.artifact_count,
            artifact_bytes: self.artifact_bytes,
            artifact_inventory_sha256: self.artifact_inventory_sha256.bytes(),
            bundle_sha256: self.bundle_sha256,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for AnalyticalBackupBundleReceipt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = AnalyticalBackupBundleReceiptWire::deserialize(deserializer)?;
        Self::try_from_exact_parts(
            wire.version,
            wire.catalog_backup,
            CatalogEndpointIdentity::try_new(wire.source_catalog_identity).ok_or_else(|| {
                serde::de::Error::custom(AnalyticalBackupReceiptError::InvalidIdentity)
            })?,
            AuthorityGeneration::try_new(wire.source_authority_generation).ok_or_else(|| {
                serde::de::Error::custom(AnalyticalBackupReceiptError::InvalidIdentity)
            })?,
            AuthorityEventDigest::try_new(wire.source_authority_event).ok_or_else(|| {
                serde::de::Error::custom(AnalyticalBackupReceiptError::InvalidIdentity)
            })?,
            AuthorityEvidenceDigest::try_new(wire.source_authority_evidence).ok_or_else(|| {
                serde::de::Error::custom(AnalyticalBackupReceiptError::InvalidIdentity)
            })?,
            CatalogContentEvidenceDigest::try_from_bytes(wire.catalog_content_evidence)
                .ok_or_else(|| {
                    serde::de::Error::custom(AnalyticalBackupReceiptError::InvalidIdentity)
                })?,
            StableArtifactRootIdentity::try_new(wire.source_root_identity).ok_or_else(|| {
                serde::de::Error::custom(AnalyticalBackupReceiptError::InvalidIdentity)
            })?,
            Timestamp::from_unix_nanos(wire.cutoff_ns),
            wire.artifact_count,
            wire.artifact_bytes,
            ArtifactInventoryDigest::try_new(wire.artifact_inventory_sha256).ok_or_else(|| {
                serde::de::Error::custom(AnalyticalBackupReceiptError::InvalidIdentity)
            })?,
            wire.bundle_sha256,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Invalid or tampered analytical backup receipt.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum AnalyticalBackupReceiptError {
    /// The receipt schema is not supported by this binary.
    #[error("analytical backup receipt version is unsupported")]
    UnsupportedVersion,
    /// The nested SQLite backup receipt is invalid.
    #[error("analytical backup catalog receipt is invalid")]
    InvalidCatalogReceipt,
    /// Authority or artifact summary metadata violates the durable contract.
    #[error("analytical backup receipt metadata is invalid")]
    InvalidMetadata,
    /// A durable identity used an invalid reserved value.
    #[error("analytical backup identity is invalid")]
    InvalidIdentity,
    /// An attacker-controlled resource claim exceeded the fixed receipt ceiling.
    #[error("analytical backup receipt resource ceiling is exceeded")]
    ResourceLimitExceeded,
    /// Persisted metadata differs from the exact bundle digest.
    #[error("analytical backup bundle digest does not match")]
    BundleDigestMismatch,
    /// The source catalog does not have one fully validated bound authority event.
    #[error("analytical backup source authority is not bound")]
    AuthorityNotBound,
}

#[allow(
    clippy::too_many_arguments,
    reason = "canonical hashing must bind every independently persisted receipt field"
)]
fn bundle_digest(
    version: u16,
    catalog_backup: &BackupReceipt,
    source_catalog_identity: CatalogEndpointIdentity,
    source_authority_generation: AuthorityGeneration,
    source_authority_event: AuthorityEventDigest,
    source_authority_evidence: AuthorityEvidenceDigest,
    catalog_content_evidence: CatalogContentEvidenceDigest,
    source_root_identity: StableArtifactRootIdentity,
    cutoff: Timestamp,
    artifact_count: u64,
    artifact_bytes: u64,
    artifact_inventory_sha256: ArtifactInventoryDigest,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/analytical-backup-bundle/v1");
    digest.update(version.to_be_bytes());
    digest.update(catalog_backup.version().to_be_bytes());
    digest.update(catalog_backup.byte_length().to_be_bytes());
    digest.update(catalog_backup.sha256());
    digest.update(source_catalog_identity.bytes());
    digest.update(source_authority_generation.get().to_be_bytes());
    digest.update(source_authority_event.bytes());
    digest.update(source_authority_evidence.bytes());
    digest.update(catalog_content_evidence.bytes());
    digest.update(source_root_identity.bytes());
    digest.update(cutoff.unix_nanos().to_be_bytes());
    digest.update(artifact_count.to_be_bytes());
    digest.update(artifact_bytes.to_be_bytes());
    digest.update(artifact_inventory_sha256.bytes());
    digest.finalize().into()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use market_squawk_domain::Timestamp;
    use market_squawk_platform::LocalPaths;
    use serde_json::{from_value, json, to_value};
    use tokio_util::sync::CancellationToken;

    use super::{
        AnalyticalBackupBundleReceipt, AnalyticalBackupLimits, AnalyticalBackupLocation,
        AnalyticalBackupReceiptError, AnalyticalBackupService, AnalyticalRestoreMode,
        AnalyticalRestoreTarget, MAX_RECEIPT_ARTIFACTS,
    };
    use crate::authority_transition::evidence::CatalogContentEvidenceDigest;
    use crate::authority_transition::{
        ArtifactInventoryDigest, AuthorityEventDigest, AuthorityEvidenceDigest,
        AuthorityGeneration, CatalogEndpointIdentity, StableArtifactRootIdentity,
    };
    use crate::{
        AnalyticalDataService, AnalyticalManifestCatalog, BackupReceipt, CatalogAuthority,
        CatalogConfig, CatalogLimit, CatalogResultLimits, ObjectStoreConfig,
    };

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    #[tokio::test]
    async fn service_creates_reopens_and_restores_one_exact_bundle() -> TestResult {
        let directory = tempfile::tempdir()?;
        let source_paths = LocalPaths::prepare(directory.path().join("source"))?;
        let source_catalog = source_paths.catalog()?.clone();
        let source = AnalyticalDataService::initialize(
            CatalogAuthority::open(catalog_config(source_catalog.clone())?)?,
            AnalyticalManifestCatalog::open(&source_catalog, 8)?,
            source_paths.artifacts()?.clone(),
            object_config()?,
        )?;
        let backup_paths = LocalPaths::prepare(directory.path().join("backup"))?;
        let backup_location = AnalyticalBackupLocation::try_new(
            backup_paths.catalog()?.clone(),
            backup_paths.artifacts()?.clone(),
        )?;
        let limits = AnalyticalBackupLimits::try_new(
            32,
            128,
            8 * 1024 * 1024,
            8 * 1024 * 1024,
            1024 * 1024,
        )?;
        let cancellation = CancellationToken::new();
        let created = source
            .backup_service()
            .create(
                backup_location.clone(),
                Timestamp::from_unix_nanos(100),
                limits,
                &cancellation,
            )
            .await?;
        let receipt = created.receipt();
        assert_eq!(receipt.artifact_count(), 0);
        drop(created);

        let verified = AnalyticalBackupService::open_verified(
            backup_location,
            receipt,
            limits,
            &cancellation,
        )?;
        let restored_paths = LocalPaths::prepare(directory.path().join("restored"))?;
        let restored_catalog = restored_paths.catalog()?.clone();
        let restored = verified.restore(
            AnalyticalRestoreTarget::try_new(
                catalog_config(restored_catalog.clone())?,
                restored_paths.artifacts()?.clone(),
                8,
                object_config()?,
                AnalyticalRestoreMode::Fresh,
            )?,
            &cancellation,
        )?;
        drop(restored);

        let reopened = AnalyticalDataService::open(
            CatalogAuthority::open(catalog_config(restored_catalog.clone())?)?,
            AnalyticalManifestCatalog::open(&restored_catalog, 8)?,
            restored_paths.artifacts()?.clone(),
            object_config()?,
        )?;
        drop(reopened);
        Ok(())
    }

    fn catalog_config(
        location: market_squawk_platform::CatalogLocation,
    ) -> Result<CatalogConfig, crate::CatalogError> {
        CatalogConfig::try_new(
            location,
            Duration::from_millis(750),
            CatalogLimit::new(32)?,
            CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
        )
    }

    fn object_config() -> Result<ObjectStoreConfig, crate::ParquetStoreError> {
        ObjectStoreConfig::try_new(8 * 1024 * 1024, 1024, Duration::from_secs(60))
    }

    fn receipt() -> TestResult<AnalyticalBackupBundleReceipt> {
        let catalog = BackupReceipt::try_from_parts(BackupReceipt::VERSION, 8_192, [7; 32])?;
        Ok(AnalyticalBackupBundleReceipt::try_from_parts(
            catalog,
            CatalogEndpointIdentity::try_new([9; 32]).ok_or("invalid catalog identity")?,
            AuthorityGeneration::try_new(4).ok_or("invalid authority generation")?,
            AuthorityEventDigest::try_new([11; 32]).ok_or("invalid authority digest")?,
            AuthorityEvidenceDigest::try_new([12; 32])
                .ok_or("invalid authority evidence digest")?,
            CatalogContentEvidenceDigest::try_new([14; 32])
                .ok_or("invalid catalog content evidence digest")?,
            StableArtifactRootIdentity::try_new([13; 32]).ok_or("invalid root identity")?,
            Timestamp::from_unix_nanos(1_721_491_200_000_000_000),
            3,
            24_576,
            ArtifactInventoryDigest::try_new([17; 32]).ok_or("invalid inventory digest")?,
        )?)
    }

    #[test]
    fn receipt_round_trip_preserves_exact_bundle_identity() -> TestResult {
        let original = receipt()?;
        let encoded = to_value(original)?;
        let decoded: AnalyticalBackupBundleReceipt = from_value(encoded)?;

        assert_eq!(decoded, original);
        assert_eq!(decoded.catalog_backup().byte_length(), 8_192);
        assert_eq!(decoded.source_catalog_identity().bytes(), [9; 32]);
        assert_eq!(decoded.source_authority_evidence().bytes(), [12; 32]);
        assert_eq!(decoded.catalog_content_evidence().bytes(), [14; 32]);
        assert_eq!(decoded.artifact_count(), 3);
        assert_eq!(decoded.artifact_bytes(), 24_576);
        Ok(())
    }

    #[test]
    fn receipt_rejects_unknown_fields() -> TestResult {
        let mut encoded = to_value(receipt()?)?;
        let fields = encoded
            .as_object_mut()
            .ok_or("receipt did not serialize as an object")?;
        fields.insert(
            "artifact_root_path".to_owned(),
            json!("/outside/capability"),
        );

        assert!(from_value::<AnalyticalBackupBundleReceipt>(encoded).is_err());
        Ok(())
    }

    #[test]
    fn receipt_rejects_tampered_bounded_metadata() -> TestResult {
        let mut encoded = to_value(receipt()?)?;
        let fields = encoded
            .as_object_mut()
            .ok_or("receipt did not serialize as an object")?;
        fields.insert("artifact_bytes".to_owned(), json!(24_577_u64));

        let error = match from_value::<AnalyticalBackupBundleReceipt>(encoded) {
            Ok(_) => return Err("tampered receipt was accepted".into()),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains(&AnalyticalBackupReceiptError::BundleDigestMismatch.to_string())
        );
        Ok(())
    }

    #[test]
    fn receipt_rejects_resource_claims_above_fixed_ceiling() -> TestResult {
        let catalog = BackupReceipt::try_from_parts(BackupReceipt::VERSION, 8_192, [7; 32])?;
        let result = AnalyticalBackupBundleReceipt::try_from_parts(
            catalog,
            CatalogEndpointIdentity::try_new([9; 32]).ok_or("invalid catalog identity")?,
            AuthorityGeneration::try_new(4).ok_or("invalid authority generation")?,
            AuthorityEventDigest::try_new([11; 32]).ok_or("invalid authority digest")?,
            AuthorityEvidenceDigest::try_new([12; 32])
                .ok_or("invalid authority evidence digest")?,
            CatalogContentEvidenceDigest::try_new([14; 32])
                .ok_or("invalid catalog content evidence digest")?,
            StableArtifactRootIdentity::try_new([13; 32]).ok_or("invalid root identity")?,
            Timestamp::from_unix_nanos(1_721_491_200_000_000_000),
            MAX_RECEIPT_ARTIFACTS + 1,
            24_576,
            ArtifactInventoryDigest::try_new([17; 32]).ok_or("invalid inventory digest")?,
        );

        assert!(matches!(
            result,
            Err(AnalyticalBackupReceiptError::ResourceLimitExceeded)
        ));
        Ok(())
    }
}
