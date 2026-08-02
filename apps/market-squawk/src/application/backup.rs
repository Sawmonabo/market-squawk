//! Product-level backup manifest admission over exact analytical backup receipts.

mod inventory;

pub use inventory::{
    BackupBundleRemover, BackupInventoryPage, BackupRetentionApproval, BackupRetentionPreview,
    ProductBackupInventory,
};

use std::{collections::BTreeSet, io::Read as _, path::Path};

use async_trait::async_trait;
use market_squawk_data::{
    AnalyticalBackupBundleReceipt, AnalyticalBackupError, AnalyticalBackupLimits,
    AnalyticalBackupLocation, AnalyticalBackupService, AnalyticalDataService,
    AnalyticalRestoreTarget, VerifiedAnalyticalBackup,
};
use market_squawk_domain::Timestamp;
use market_squawk_platform::ArtifactRoot;
use market_squawk_runtime::{InstallationId, WorkspaceId};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::workspace::WorkspaceDescriptor;

const FORMAT_VERSION: u16 = 1;
const MAXIMUM_COMPONENTS: usize = 32;
const MAXIMUM_REFERENCE_BYTES: usize = 256;
const VERIFICATION_BUFFER_BYTES: usize = 64 * 1024;

/// Materialized non-analytical product state returned by the closed component writer set.
#[derive(Debug)]
pub struct MaterializedProductComponents {
    components: Vec<ProductBackupComponent>,
    encryption: ProductBackupEncryptionEvidence,
}

impl MaterializedProductComponents {
    /// Admits a complete component set before any product manifest can be issued.
    pub fn try_new(
        components: Vec<ProductBackupComponent>,
        encryption: ProductBackupEncryptionEvidence,
    ) -> Result<Self, ProductBackupError> {
        validate_components(&components, &encryption)?;
        Ok(Self {
            components,
            encryption,
        })
    }
}

/// Application composition boundary that materializes all non-analytical product authorities.
#[async_trait]
pub trait ProductBackupComponentWriter: std::fmt::Debug + Send + Sync {
    /// Writes exact component artifacts below the retained bundle root and returns their evidence.
    async fn materialize(
        &self,
        root: &ArtifactRoot,
        cutoff: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<MaterializedProductComponents, ProductBackupError>;
}

/// Sealed product backup service over the existing exact analytical backup authority.
pub struct ProductBackupService {
    analytical: std::sync::Arc<AnalyticalBackupService>,
    components: std::sync::Arc<dyn ProductBackupComponentWriter>,
}

impl ProductBackupService {
    /// Binds analytical and product component authorities into one manifest issuer.
    #[must_use]
    pub fn new(
        analytical: std::sync::Arc<AnalyticalBackupService>,
        components: std::sync::Arc<dyn ProductBackupComponentWriter>,
    ) -> Self {
        Self {
            analytical,
            components,
        }
    }

    /// Creates, materializes, and re-verifies one complete product bundle.
    pub async fn create(
        &self,
        destination: AnalyticalBackupLocation,
        cutoff: Timestamp,
        limits: AnalyticalBackupLimits,
        ownership: ProductBackupOwnership,
        cancellation: &CancellationToken,
    ) -> Result<VerifiedProductBackup, ProductBackupError> {
        let product_root = destination.artifacts().clone();
        let analytical = self
            .analytical
            .create(destination, cutoff, limits, cancellation)
            .await?;
        let materialized = self
            .components
            .materialize(&product_root, cutoff, cancellation)
            .await?;
        let manifest = ProductBackupManifest::try_new(
            cutoff,
            ownership,
            analytical.receipt(),
            materialized.components,
            materialized.encryption,
        )?;
        manifest.verify_component_artifacts(&product_root, cancellation)?;
        Ok(VerifiedProductBackup {
            analytical,
            manifest,
        })
    }

    /// Reopens and re-verifies one retained product bundle before preview or restore.
    pub fn open_verified(
        location: AnalyticalBackupLocation,
        manifest: ProductBackupManifest,
        limits: AnalyticalBackupLimits,
        cancellation: &CancellationToken,
    ) -> Result<VerifiedProductBackup, ProductBackupError> {
        manifest.verify()?;
        manifest.verify_component_artifacts(location.artifacts(), cancellation)?;
        let analytical = AnalyticalBackupService::open_verified(
            location,
            manifest.analytical_receipt(),
            limits,
            cancellation,
        )?;
        Ok(VerifiedProductBackup {
            analytical,
            manifest,
        })
    }
}

impl std::fmt::Debug for ProductBackupService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ProductBackupService([SEALED PRODUCT AUTHORITIES])")
    }
}

/// Complete verified product backup retained for restore staging.
pub struct VerifiedProductBackup {
    analytical: VerifiedAnalyticalBackup,
    manifest: ProductBackupManifest,
}

impl VerifiedProductBackup {
    /// Returns the exact product manifest admitted for restore preview.
    #[must_use]
    pub const fn manifest(&self) -> &ProductBackupManifest {
        &self.manifest
    }

    /// Separates verified analytical and product evidence for the restore coordinator.
    #[must_use]
    pub fn into_parts(self) -> (VerifiedAnalyticalBackup, ProductBackupManifest) {
        (self.analytical, self.manifest)
    }
}

impl std::fmt::Debug for VerifiedProductBackup {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("VerifiedProductBackup([EXACT VERIFIED BUNDLE])")
    }
}

/// Closed product-state component set required for a restorable v1 workspace.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProductBackupComponentKind {
    Configuration,
    ProviderMetadata,
    SourceData,
    Portfolios,
    Transactions,
    Models,
    DecisionTargets,
    JobsAndReceipts,
    FairValueEvidence,
}

impl ProductBackupComponentKind {
    const REQUIRED: [Self; 9] = [
        Self::Configuration,
        Self::ProviderMetadata,
        Self::SourceData,
        Self::Portfolios,
        Self::Transactions,
        Self::Models,
        Self::DecisionTargets,
        Self::JobsAndReceipts,
        Self::FairValueEvidence,
    ];
}

/// Whether a materialized component contains protected bytes.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProductBackupSensitivity {
    NonSecret,
    Protected,
    SecretPayload,
}

/// Exact controlled-bundle contribution produced by one product authority.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ProductBackupComponent {
    kind: ProductBackupComponentKind,
    artifact_reference: String,
    byte_length: u64,
    sha256: [u8; 32],
    sensitivity: ProductBackupSensitivity,
}

impl ProductBackupComponent {
    /// Binds a controlled artifact reference to exact length, digest, and sensitivity evidence.
    pub fn try_new(
        kind: ProductBackupComponentKind,
        artifact_reference: impl Into<String>,
        byte_length: u64,
        sha256: [u8; 32],
        sensitivity: ProductBackupSensitivity,
    ) -> Result<Self, ProductBackupError> {
        let artifact_reference = artifact_reference.into();
        if artifact_reference.is_empty()
            || artifact_reference.len() > MAXIMUM_REFERENCE_BYTES
            || artifact_reference.starts_with('/')
            || artifact_reference.contains("..")
            || artifact_reference.contains('\\')
            || artifact_reference.chars().any(char::is_control)
            || (byte_length == 0) != (sha256 == [0; 32])
        {
            return Err(ProductBackupError::InvalidComponent);
        }
        Ok(Self {
            kind,
            artifact_reference,
            byte_length,
            sha256,
            sensitivity,
        })
    }
}

/// Encryption proof supplied by the controlled bundle writer.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProductBackupEncryptionEvidence {
    /// Bundle has no secret payload; secret locators are represented only by one-way digests.
    UnencryptedNoSecretPayload,
    /// Protected component bytes were encrypted by the named versioned local scheme.
    Encrypted {
        scheme: String,
        key_reference_sha256: [u8; 32],
    },
}

/// Installation/workspace ownership bound into backup admission.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ProductBackupOwnership {
    installation_id: InstallationId,
    workspace_id: WorkspaceId,
}

impl ProductBackupOwnership {
    /// Creates ownership evidence without paths or credentials.
    #[must_use]
    pub const fn new(installation_id: InstallationId, workspace_id: WorkspaceId) -> Self {
        Self {
            installation_id,
            workspace_id,
        }
    }

    /// Returns the workspace that owned the source state.
    #[must_use]
    pub const fn workspace_id(self) -> WorkspaceId {
        self.workspace_id
    }
}

/// Versioned manifest that binds all product-state contributions to an analytical receipt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ProductBackupManifest {
    format_version: u16,
    backup_id: [u8; 32],
    created_at: Timestamp,
    ownership: ProductBackupOwnership,
    analytical_receipt: AnalyticalBackupBundleReceipt,
    components: Vec<ProductBackupComponent>,
    encryption: ProductBackupEncryptionEvidence,
    manifest_sha256: [u8; 32],
}

impl ProductBackupManifest {
    /// Admits one complete, exact product manifest after all component authorities materialize.
    pub fn try_new(
        created_at: Timestamp,
        ownership: ProductBackupOwnership,
        analytical_receipt: AnalyticalBackupBundleReceipt,
        mut components: Vec<ProductBackupComponent>,
        encryption: ProductBackupEncryptionEvidence,
    ) -> Result<Self, ProductBackupError> {
        components.sort_by_key(|component| component.kind);
        validate_components(&components, &encryption)?;
        let manifest_sha256 = manifest_digest(
            FORMAT_VERSION,
            created_at,
            ownership,
            analytical_receipt,
            &components,
            &encryption,
        )?;
        let backup_id = Sha256::digest(
            [
                b"market-squawk-product-backup-v1".as_slice(),
                manifest_sha256.as_slice(),
            ]
            .concat(),
        )
        .into();
        Ok(Self {
            format_version: FORMAT_VERSION,
            backup_id,
            created_at,
            ownership,
            analytical_receipt,
            components,
            encryption,
            manifest_sha256,
        })
    }

    /// Revalidates a deserialized manifest before inventory display or restore preview.
    pub fn verify(&self) -> Result<(), ProductBackupError> {
        if self.format_version != FORMAT_VERSION {
            return Err(ProductBackupError::UnsupportedVersion);
        }
        validate_components(&self.components, &self.encryption)?;
        let expected = manifest_digest(
            self.format_version,
            self.created_at,
            self.ownership,
            self.analytical_receipt,
            &self.components,
            &self.encryption,
        )?;
        let expected_backup_id: [u8; 32] = Sha256::digest(
            [
                b"market-squawk-product-backup-v1".as_slice(),
                expected.as_slice(),
            ]
            .concat(),
        )
        .into();
        if expected != self.manifest_sha256 || expected_backup_id != self.backup_id {
            return Err(ProductBackupError::DigestMismatch);
        }
        Ok(())
    }

    /// Returns the opaque content-derived backup identity.
    #[must_use]
    pub const fn backup_id(&self) -> [u8; 32] {
        self.backup_id
    }

    /// Returns when the source authorities captured this backup.
    #[must_use]
    pub const fn created_at(&self) -> Timestamp {
        self.created_at
    }

    /// Returns the exact analytical bundle receipt required for verification and restore.
    #[must_use]
    pub const fn analytical_receipt(&self) -> AnalyticalBackupBundleReceipt {
        self.analytical_receipt
    }

    /// Returns installation/workspace ownership evidence for restore admission.
    #[must_use]
    pub const fn ownership(&self) -> ProductBackupOwnership {
        self.ownership
    }

    /// Revalidates every product component through the retained bundle-root capability.
    pub fn verify_component_artifacts(
        &self,
        root: &ArtifactRoot,
        cancellation: &CancellationToken,
    ) -> Result<(), ProductBackupError> {
        self.verify()?;
        for component in &self.components {
            if cancellation.is_cancelled() {
                return Err(ProductBackupError::Cancelled);
            }
            let relative = Path::new(&component.artifact_reference);
            let resolved = root
                .resolve(relative)
                .map_err(|_| ProductBackupError::InvalidComponent)?;
            let mut file = resolved
                .open_read()
                .map_err(|_| ProductBackupError::ArtifactUnavailable)?;
            let metadata = file
                .metadata()
                .map_err(|_| ProductBackupError::ArtifactUnavailable)?;
            if !private_regular_file(&metadata) || metadata.len() != component.byte_length {
                return Err(ProductBackupError::ArtifactMismatch);
            }
            let mut digest = Sha256::new();
            let mut observed = 0_u64;
            let mut buffer = [0_u8; VERIFICATION_BUFFER_BYTES];
            loop {
                if cancellation.is_cancelled() {
                    return Err(ProductBackupError::Cancelled);
                }
                let read = file
                    .read(&mut buffer)
                    .map_err(|_| ProductBackupError::ArtifactUnavailable)?;
                if read == 0 {
                    break;
                }
                observed = observed
                    .checked_add(
                        u64::try_from(read).map_err(|_| ProductBackupError::ArtifactMismatch)?,
                    )
                    .ok_or(ProductBackupError::ArtifactMismatch)?;
                if observed > component.byte_length {
                    return Err(ProductBackupError::ArtifactMismatch);
                }
                digest.update(&buffer[..read]);
            }
            let observed_digest: [u8; 32] = digest.finalize().into();
            if observed != component.byte_length || observed_digest != component.sha256 {
                return Err(ProductBackupError::ArtifactMismatch);
            }
        }
        Ok(())
    }
}

#[cfg(unix)]
fn private_regular_file(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt as _;

    metadata.is_file() && metadata.nlink() == 1 && metadata.mode() & 0o077 == 0
}

#[cfg(windows)]
fn private_regular_file(metadata: &std::fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt as _;

    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
    metadata.is_file() && metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT == 0
}

#[cfg(not(any(unix, windows)))]
fn private_regular_file(_metadata: &std::fs::Metadata) -> bool {
    false
}

/// Fresh target capabilities prepared by all non-analytical product restore authorities.
pub struct StagedProductRestoreTarget {
    workspace: WorkspaceDescriptor,
    analytical: AnalyticalRestoreTarget,
    source_workspace: WorkspaceId,
    active_workspace: WorkspaceId,
}

impl StagedProductRestoreTarget {
    /// Binds a newly prepared workspace to its exact analytical restore target.
    pub fn try_new(
        workspace: WorkspaceDescriptor,
        analytical: AnalyticalRestoreTarget,
        source_workspace: WorkspaceId,
        active_workspace: WorkspaceId,
    ) -> Result<Self, ProductBackupError> {
        if !workspace.is_prepared()
            || workspace.workspace_id() == source_workspace
            || workspace.workspace_id() == active_workspace
        {
            return Err(ProductBackupError::InvalidRestoreTarget);
        }
        Ok(Self {
            workspace,
            analytical,
            source_workspace,
            active_workspace,
        })
    }
}

impl std::fmt::Debug for StagedProductRestoreTarget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StagedProductRestoreTarget")
            .field("workspace", &self.workspace)
            .field("analytical", &"[FRESH ANALYTICAL CAPABILITIES]")
            .finish()
    }
}

/// Product authorities that stage configuration, portfolio, model, job, and evidence state.
#[async_trait]
pub trait ProductRestoreComponentAuthority: std::fmt::Debug + Send + Sync {
    /// Materializes and verifies every manifest component into one fresh inactive workspace.
    async fn stage(
        &self,
        manifest: &ProductBackupManifest,
        cancellation: &CancellationToken,
    ) -> Result<StagedProductRestoreTarget, ProductBackupError>;

    /// Removes or marks unusable a failed inactive staging generation.
    async fn abandon(
        &self,
        workspace_id: WorkspaceId,
        cancellation: &CancellationToken,
    ) -> Result<(), ProductBackupError>;
}

/// Fully restored inactive workspace retained for registration and generation-fenced switching.
pub struct PreparedProductRestore {
    workspace: WorkspaceDescriptor,
    analytical: AnalyticalDataService,
    manifest: ProductBackupManifest,
}

impl PreparedProductRestore {
    /// Returns the prepared path-free workspace inventory record.
    #[must_use]
    pub const fn workspace(&self) -> &WorkspaceDescriptor {
        &self.workspace
    }

    /// Separates restored authorities for application composition and workspace registration.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        WorkspaceDescriptor,
        AnalyticalDataService,
        ProductBackupManifest,
    ) {
        (self.workspace, self.analytical, self.manifest)
    }
}

impl std::fmt::Debug for PreparedProductRestore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedProductRestore")
            .field("workspace", &self.workspace)
            .field("analytical", &"[RESTORED ANALYTICAL AUTHORITY]")
            .field("manifest", &self.manifest)
            .finish()
    }
}

impl VerifiedProductBackup {
    /// Restores all product state into a fresh inactive workspace without mutating active data.
    pub async fn stage_restore(
        self,
        active_workspace: WorkspaceId,
        components: &dyn ProductRestoreComponentAuthority,
        cancellation: &CancellationToken,
    ) -> Result<PreparedProductRestore, ProductBackupError> {
        let target = components.stage(&self.manifest, cancellation).await?;
        let workspace_id = target.workspace.workspace_id();
        if target.source_workspace != self.manifest.ownership().workspace_id()
            || target.active_workspace != active_workspace
            || workspace_id == target.source_workspace
            || workspace_id == active_workspace
        {
            components.abandon(workspace_id, cancellation).await?;
            return Err(ProductBackupError::InvalidRestoreTarget);
        }
        let restore_cancellation = cancellation.clone();
        let analytical_backup = self.analytical;
        let analytical_target = target.analytical;
        let restored = match tokio::task::spawn_blocking(move || {
            analytical_backup.restore(analytical_target, &restore_cancellation)
        })
        .await
        {
            Ok(restored) => restored,
            Err(_error) => {
                components.abandon(workspace_id, cancellation).await?;
                return Err(ProductBackupError::RestoreWorker);
            }
        };
        match restored {
            Ok(analytical) => Ok(PreparedProductRestore {
                workspace: target.workspace,
                analytical,
                manifest: self.manifest,
            }),
            Err(error) => {
                components.abandon(workspace_id, cancellation).await?;
                Err(ProductBackupError::Analytical(error))
            }
        }
    }
}

fn validate_components(
    components: &[ProductBackupComponent],
    encryption: &ProductBackupEncryptionEvidence,
) -> Result<(), ProductBackupError> {
    if components.len() != ProductBackupComponentKind::REQUIRED.len()
        || components.len() > MAXIMUM_COMPONENTS
    {
        return Err(ProductBackupError::IncompleteComponents);
    }
    let kinds = components
        .iter()
        .map(|component| component.kind)
        .collect::<BTreeSet<_>>();
    if kinds.len() != components.len()
        || ProductBackupComponentKind::REQUIRED
            .into_iter()
            .any(|required| !kinds.contains(&required))
    {
        return Err(ProductBackupError::IncompleteComponents);
    }
    if matches!(
        encryption,
        ProductBackupEncryptionEvidence::UnencryptedNoSecretPayload
    ) && components
        .iter()
        .any(|component| component.sensitivity == ProductBackupSensitivity::SecretPayload)
    {
        return Err(ProductBackupError::UnencryptedSecretPayload);
    }
    if let ProductBackupEncryptionEvidence::Encrypted {
        scheme,
        key_reference_sha256,
    } = encryption
        && (scheme.is_empty()
            || scheme.len() > MAXIMUM_REFERENCE_BYTES
            || scheme.chars().any(char::is_control)
            || *key_reference_sha256 == [0; 32])
    {
        return Err(ProductBackupError::InvalidEncryptionEvidence);
    }
    Ok(())
}

fn manifest_digest(
    format_version: u16,
    created_at: Timestamp,
    ownership: ProductBackupOwnership,
    analytical_receipt: AnalyticalBackupBundleReceipt,
    components: &[ProductBackupComponent],
    encryption: &ProductBackupEncryptionEvidence,
) -> Result<[u8; 32], ProductBackupError> {
    serde_json::to_vec(&(
        "market-squawk-product-backup-manifest-v1",
        format_version,
        created_at,
        ownership,
        analytical_receipt,
        components,
        encryption,
    ))
    .map(|encoded| Sha256::digest(encoded).into())
    .map_err(|_| ProductBackupError::Encoding)
}

/// Typed product-manifest admission failure.
#[derive(Debug, Error)]
pub enum ProductBackupError {
    #[error("product backup component is invalid")]
    InvalidComponent,
    #[error("product backup component set is incomplete or duplicated")]
    IncompleteComponents,
    #[error("unencrypted product backup cannot contain secret payloads")]
    UnencryptedSecretPayload,
    #[error("product backup encryption evidence is invalid")]
    InvalidEncryptionEvidence,
    #[error("product backup manifest version is unsupported")]
    UnsupportedVersion,
    #[error("product backup manifest digest does not match")]
    DigestMismatch,
    #[error("product backup manifest could not be encoded")]
    Encoding,
    #[error("product backup component artifact is unavailable")]
    ArtifactUnavailable,
    #[error("product backup component artifact does not match its manifest")]
    ArtifactMismatch,
    #[error("product backup verification was cancelled")]
    Cancelled,
    #[error("analytical backup authority failed")]
    Analytical(#[from] AnalyticalBackupError),
    #[error("product restore target must be fresh, prepared, and distinct from its source")]
    InvalidRestoreTarget,
    #[error("product restore component authority failed")]
    RestoreComponents,
    #[error("product restore worker did not return")]
    RestoreWorker,
    #[error("product backup inventory state is corrupt")]
    InventoryCorrupt,
    #[error("product backup inventory is unavailable")]
    InventoryUnavailable,
    #[error("product backup inventory capacity is exhausted")]
    InventoryCapacity,
    #[error("product backup inventory limit is invalid")]
    InvalidInventoryLimit,
    #[error("product backup inventory cursor is invalid")]
    InvalidInventoryCursor,
    #[error("product backup is not present in the verified inventory")]
    BackupNotFound,
    #[error("product backup retention policy is invalid")]
    InvalidRetentionPolicy,
    #[error("product backup retention preview is empty")]
    RetentionEmpty,
    #[error("product backup retention approval is stale")]
    StaleRetentionApproval,
    #[error("product backup inventory persistence failed")]
    InventoryPersistence,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unencrypted_manifest_rejects_secret_payload_component()
    -> Result<(), Box<dyn std::error::Error>> {
        let components = ProductBackupComponentKind::REQUIRED
            .into_iter()
            .enumerate()
            .map(|(index, kind)| {
                ProductBackupComponent::try_new(
                    kind,
                    format!("components/{index}.json"),
                    1,
                    [u8::try_from(index + 1)?; 32],
                    if kind == ProductBackupComponentKind::Configuration {
                        ProductBackupSensitivity::SecretPayload
                    } else {
                        ProductBackupSensitivity::NonSecret
                    },
                )
                .map_err(Into::into)
            })
            .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;

        assert!(matches!(
            validate_components(
                &components,
                &ProductBackupEncryptionEvidence::UnencryptedNoSecretPayload,
            ),
            Err(ProductBackupError::UnencryptedSecretPayload)
        ));
        Ok(())
    }
}
