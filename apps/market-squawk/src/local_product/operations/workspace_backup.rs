//! Fixed-order workspace snapshot and fresh-restore coordination.
//!
//! This module never copies a live authority file. Each component owner retains its own exact
//! state, emits a typed export through a bounded writer, and revalidates the owner-issued receipt
//! after materialization. Restore delegates decoding and persistence back to the owning fresh
//! workspace authorities.

use std::{
    fmt,
    io::{Read, Write},
    sync::Arc,
};

use async_trait::async_trait;
use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt as _};
use cap_std::fs::OpenOptions;
use market_squawk_domain::SourceIdentifier;
use market_squawk_platform::ArtifactRoot;
use market_squawk_runtime::WorkspaceId;
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use crate::application::backup::{
    MaterializedProductComponents, ProductBackupArtifactEvidence, ProductBackupComponent,
    ProductBackupComponentKind, ProductBackupComponentSchema, ProductBackupEncryptionEvidence,
    ProductBackupError, ProductBackupManifest, ProductBackupSensitivity, ProductBackupSnapshot,
    ProductBackupSnapshotAuthority, ProductBackupSnapshotLease, ProductRestoreComponentAuthority,
    StagedProductRestoreTarget,
};

const REQUIRED_COMPONENTS: [ProductBackupComponentKind; 9] = [
    ProductBackupComponentKind::Configuration,
    ProductBackupComponentKind::ProviderMetadata,
    ProductBackupComponentKind::SourceData,
    ProductBackupComponentKind::Portfolios,
    ProductBackupComponentKind::Transactions,
    ProductBackupComponentKind::Models,
    ProductBackupComponentKind::DecisionTargets,
    ProductBackupComponentKind::JobsAndReceipts,
    ProductBackupComponentKind::FairValueEvidence,
];
const MAXIMUM_COMPONENT_BYTES: u64 = 16 * 1024 * 1024 * 1024;

/// Immutable identity and schema declared by one workspace component owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct WorkspaceComponentDescriptor {
    kind: ProductBackupComponentKind,
    producer: SourceIdentifier,
    schema: ProductBackupComponentSchema,
    sensitivity: ProductBackupSensitivity,
}

impl WorkspaceComponentDescriptor {
    /// Binds one owner to exactly one closed v1 component kind.
    pub(super) fn try_new(
        kind: ProductBackupComponentKind,
        producer: SourceIdentifier,
        schema: ProductBackupComponentSchema,
        sensitivity: ProductBackupSensitivity,
    ) -> Result<Self, ProductBackupError> {
        if sensitivity == ProductBackupSensitivity::SecretPayload {
            return Err(ProductBackupError::UnencryptedSecretPayload);
        }
        Ok(Self {
            kind,
            producer,
            schema,
            sensitivity,
        })
    }

    const fn kind(&self) -> ProductBackupComponentKind {
        self.kind
    }
}

/// Owner-issued proof of the exact typed state written through the snapshot sink.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct WorkspaceComponentSnapshotReceipt {
    authority_revision_sha256: [u8; 32],
    byte_length: u64,
    sha256: [u8; 32],
}

impl WorkspaceComponentSnapshotReceipt {
    /// Admits nonempty, bounded owner evidence for one component export.
    pub(super) fn try_new(
        authority_revision_sha256: [u8; 32],
        byte_length: u64,
        sha256: [u8; 32],
    ) -> Result<Self, ProductBackupError> {
        if authority_revision_sha256 == [0; 32]
            || byte_length == 0
            || byte_length > MAXIMUM_COMPONENT_BYTES
            || sha256 == [0; 32]
        {
            return Err(ProductBackupError::InvalidComponent);
        }
        Ok(Self {
            authority_revision_sha256,
            byte_length,
            sha256,
        })
    }
}

/// Non-cloneable component-owner lease held across export and post-write validation.
#[async_trait]
pub(super) trait WorkspaceComponentSnapshotLease: fmt::Debug + Send {
    /// Returns the ordered component metadata retained under this one owner lease.
    fn descriptors(&self) -> &[WorkspaceComponentDescriptor];

    /// Emits one declared component from the owner's versioned typed export.
    ///
    /// The owner must reject a kind that is not present in [`Self::descriptors`]. A single lease
    /// may emit multiple components when one genuine authority owns their shared mutation gate.
    async fn write_snapshot(
        &mut self,
        kind: ProductBackupComponentKind,
        snapshot: ProductBackupSnapshot,
        writer: &mut (dyn Write + Send),
        cancellation: &CancellationToken,
    ) -> Result<WorkspaceComponentSnapshotReceipt, ProductBackupError>;

    /// Proves that the retained owner revision still matches the emitted receipt.
    async fn revalidate(
        &mut self,
        kind: ProductBackupComponentKind,
        snapshot: ProductBackupSnapshot,
        receipt: WorkspaceComponentSnapshotReceipt,
        cancellation: &CancellationToken,
    ) -> Result<(), ProductBackupError>;
}

/// Snapshot capability implemented by one genuine component-state owner.
#[async_trait]
pub(super) trait WorkspaceComponentSnapshotAuthority: fmt::Debug + Send + Sync {
    /// Returns ordered metadata for every component governed by this owner's mutation gate.
    fn descriptors(&self) -> &[WorkspaceComponentDescriptor];

    /// Acquires the owner's mutation fence or exact-export lease before the cutoff is allocated.
    async fn retain(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<Box<dyn WorkspaceComponentSnapshotLease>, ProductBackupError>;
}

/// Verified component stream owned by the managed backup repository.
pub(super) trait VerifiedWorkspaceComponentReader: Read + Send {}

impl<T> VerifiedWorkspaceComponentReader for T where T: Read + Send {}

/// Least-authority reader for component artifacts in one already verified product bundle.
#[async_trait]
pub(super) trait WorkspaceBackupBundleSource: fmt::Debug + Send + Sync {
    /// Opens only the exact manifest-bound component through retained repository authority.
    async fn open_verified_component(
        &self,
        manifest: &ProductBackupManifest,
        component: &ProductBackupComponent,
        cancellation: &CancellationToken,
    ) -> Result<Box<dyn VerifiedWorkspaceComponentReader>, ProductBackupError>;
}

/// Owner-composed restore session over one fresh inactive workspace.
#[async_trait]
pub(super) trait FreshWorkspaceRestoreSession: fmt::Debug + Send {
    /// Returns the fresh inactive workspace identity for failure cleanup.
    fn workspace_id(&self) -> WorkspaceId;

    /// Decodes, validates, and stages one component through its genuine target owner.
    async fn stage_component(
        &mut self,
        component: &ProductBackupComponent,
        reader: &mut (dyn Read + Send),
        cancellation: &CancellationToken,
    ) -> Result<(), ProductBackupError>;

    /// Verifies cross-component links and returns the fresh analytical restore capabilities.
    async fn complete(
        self: Box<Self>,
        manifest: &ProductBackupManifest,
        cancellation: &CancellationToken,
    ) -> Result<StagedProductRestoreTarget, ProductBackupError>;
}

/// Fresh-workspace factory and failed-staging cleanup authority.
#[async_trait]
pub(super) trait FreshWorkspaceRestoreAuthority: fmt::Debug + Send + Sync {
    /// Creates one fresh inactive workspace bound to the source and current active identities.
    async fn prepare(
        &self,
        manifest: &ProductBackupManifest,
        active_workspace: WorkspaceId,
        cancellation: &CancellationToken,
    ) -> Result<Box<dyn FreshWorkspaceRestoreSession>, ProductBackupError>;

    /// Removes or permanently marks unusable one failed inactive workspace.
    async fn abandon(
        &self,
        workspace_id: WorkspaceId,
        cancellation: &CancellationToken,
    ) -> Result<(), ProductBackupError>;
}

/// Installed aggregate over all nine owner-issued snapshot and restore capabilities.
pub(crate) struct InstalledWorkspaceBackupAuthority {
    snapshots: Vec<Arc<dyn WorkspaceComponentSnapshotAuthority>>,
    bundles: Arc<dyn WorkspaceBackupBundleSource>,
    restore: Arc<dyn FreshWorkspaceRestoreAuthority>,
    active_workspace: WorkspaceId,
}

impl InstalledWorkspaceBackupAuthority {
    /// Seals exact fixed-order component coverage and one fresh-workspace restore authority.
    pub(super) fn try_new(
        snapshots: Vec<Arc<dyn WorkspaceComponentSnapshotAuthority>>,
        bundles: Arc<dyn WorkspaceBackupBundleSource>,
        restore: Arc<dyn FreshWorkspaceRestoreAuthority>,
        active_workspace: WorkspaceId,
    ) -> Result<Self, ProductBackupError> {
        let declared = snapshots
            .iter()
            .flat_map(|authority| authority.descriptors());
        if snapshots.is_empty()
            || snapshots
                .iter()
                .any(|authority| authority.descriptors().is_empty())
            || !declared
                .map(WorkspaceComponentDescriptor::kind)
                .eq(REQUIRED_COMPONENTS)
        {
            return Err(ProductBackupError::IncompleteComponents);
        }
        Ok(Self {
            snapshots,
            bundles,
            restore,
            active_workspace,
        })
    }
}

impl fmt::Debug for InstalledWorkspaceBackupAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("InstalledWorkspaceBackupAuthority([SEALED WORKSPACE AUTHORITIES])")
    }
}

#[async_trait]
impl ProductBackupSnapshotAuthority for InstalledWorkspaceBackupAuthority {
    async fn retain(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<Box<dyn ProductBackupSnapshotLease>, ProductBackupError> {
        let mut retained = Vec::new();
        retained
            .try_reserve_exact(self.snapshots.len())
            .map_err(|_| ProductBackupError::IncompleteComponents)?;
        for authority in &self.snapshots {
            if cancellation.is_cancelled() {
                return Err(ProductBackupError::Cancelled);
            }
            let lease = authority.retain(cancellation).await?;
            if lease.descriptors() != authority.descriptors() {
                return Err(ProductBackupError::IncompleteComponents);
            }
            retained.push(RetainedWorkspaceOwner {
                descriptors: authority.descriptors().to_vec(),
                lease,
            });
        }
        Ok(Box::new(RetainedWorkspaceSnapshot {
            retained,
            receipts: None,
        }))
    }
}

#[async_trait]
impl ProductRestoreComponentAuthority for InstalledWorkspaceBackupAuthority {
    async fn stage(
        &self,
        manifest: &ProductBackupManifest,
        cancellation: &CancellationToken,
    ) -> Result<StagedProductRestoreTarget, ProductBackupError> {
        manifest.verify()?;
        let mut session = self
            .restore
            .prepare(manifest, self.active_workspace, cancellation)
            .await?;
        let staging_workspace = session.workspace_id();
        let staged = async {
            for expected in REQUIRED_COMPONENTS {
                if cancellation.is_cancelled() {
                    return Err(ProductBackupError::Cancelled);
                }
                let component = manifest
                    .components()
                    .iter()
                    .find(|component| component.kind() == expected)
                    .ok_or(ProductBackupError::IncompleteComponents)?;
                let reader = self
                    .bundles
                    .open_verified_component(manifest, component, cancellation)
                    .await?;
                let mut verified = DigestingReader::new(reader, component.byte_length());
                session
                    .stage_component(component, &mut verified, cancellation)
                    .await?;
                verified.finish(component.sha256())?;
            }
            session.complete(manifest, cancellation).await
        }
        .await;
        if staged.is_err() {
            self.restore
                .abandon(staging_workspace, cancellation)
                .await?;
        }
        staged
    }

    async fn abandon(
        &self,
        workspace_id: WorkspaceId,
        cancellation: &CancellationToken,
    ) -> Result<(), ProductBackupError> {
        self.restore.abandon(workspace_id, cancellation).await
    }
}

struct RetainedWorkspaceSnapshot {
    retained: Vec<RetainedWorkspaceOwner>,
    receipts: Option<Vec<Vec<WorkspaceComponentSnapshotReceipt>>>,
}

struct RetainedWorkspaceOwner {
    descriptors: Vec<WorkspaceComponentDescriptor>,
    lease: Box<dyn WorkspaceComponentSnapshotLease>,
}

impl fmt::Debug for RetainedWorkspaceSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RetainedWorkspaceSnapshot([NON-CLONEABLE OWNER LEASES])")
    }
}

#[async_trait]
impl ProductBackupSnapshotLease for RetainedWorkspaceSnapshot {
    async fn materialize(
        &mut self,
        root: &ArtifactRoot,
        snapshot: ProductBackupSnapshot,
        cancellation: &CancellationToken,
    ) -> Result<MaterializedProductComponents, ProductBackupError> {
        if self.receipts.is_some() {
            return Err(ProductBackupError::SnapshotMismatch);
        }
        let mut components = Vec::new();
        let mut receipts = Vec::new();
        components
            .try_reserve_exact(REQUIRED_COMPONENTS.len())
            .map_err(|_| ProductBackupError::IncompleteComponents)?;
        receipts
            .try_reserve_exact(self.retained.len())
            .map_err(|_| ProductBackupError::IncompleteComponents)?;
        for owner in &mut self.retained {
            if cancellation.is_cancelled() {
                return Err(ProductBackupError::Cancelled);
            }
            if owner.lease.descriptors() != owner.descriptors.as_slice() {
                return Err(ProductBackupError::IncompleteComponents);
            }
            let descriptors = owner.descriptors.clone();
            let mut owner_receipts = Vec::new();
            owner_receipts
                .try_reserve_exact(descriptors.len())
                .map_err(|_| ProductBackupError::IncompleteComponents)?;
            for descriptor in descriptors {
                if cancellation.is_cancelled() {
                    return Err(ProductBackupError::Cancelled);
                }
                let reference = component_reference(descriptor.kind);
                let resolved = root
                    .resolve(&reference)
                    .map_err(|_| ProductBackupError::InvalidComponent)?;
                let directory = root
                    .try_clone_directory()
                    .map_err(|_| ProductBackupError::ArtifactUnavailable)?;
                if let Some(parent) = resolved.relative().parent()
                    && !parent.as_os_str().is_empty()
                {
                    directory
                        .create_dir_all(parent)
                        .map_err(|_| ProductBackupError::ArtifactUnavailable)?;
                }
                let mut options = OpenOptions::new();
                options.write(true).create_new(true);
                options.follow(FollowSymlinks::No);
                configure_private_creation(&mut options);
                let mut file = directory
                    .open_with(resolved.relative(), &options)
                    .map_err(|_| ProductBackupError::ArtifactUnavailable)?;
                let mut writer = DigestingWriter::new(&mut file, MAXIMUM_COMPONENT_BYTES);
                let receipt = owner
                    .lease
                    .write_snapshot(descriptor.kind, snapshot, &mut writer, cancellation)
                    .await?;
                let observed = writer.finish()?;
                if observed.byte_length != receipt.byte_length
                    || observed.sha256 != receipt.sha256
                    || receipt.authority_revision_sha256 == [0; 32]
                {
                    return Err(ProductBackupError::ArtifactMismatch);
                }
                file.sync_all()
                    .map_err(|_| ProductBackupError::ArtifactUnavailable)?;
                components.push(ProductBackupComponent::try_new(
                    snapshot,
                    descriptor.kind,
                    descriptor.producer,
                    descriptor.schema,
                    ProductBackupArtifactEvidence::try_new(
                        reference,
                        receipt.byte_length,
                        receipt.sha256,
                        descriptor.sensitivity,
                    )?,
                )?);
                owner_receipts.push(receipt);
            }
            receipts.push(owner_receipts);
        }
        synchronize_directory(
            &root
                .try_clone_directory()
                .map_err(|_| ProductBackupError::ArtifactUnavailable)?,
        )?;
        self.receipts = Some(receipts);
        MaterializedProductComponents::try_new(
            snapshot,
            components,
            ProductBackupEncryptionEvidence::UnencryptedNoSecretPayload,
        )
    }

    async fn revalidate(
        &mut self,
        _root: &ArtifactRoot,
        snapshot: ProductBackupSnapshot,
        components: &[ProductBackupComponent],
        cancellation: &CancellationToken,
    ) -> Result<(), ProductBackupError> {
        let receipts = self
            .receipts
            .as_ref()
            .ok_or(ProductBackupError::SnapshotMismatch)?;
        if components.len() != REQUIRED_COMPONENTS.len() || receipts.len() != self.retained.len() {
            return Err(ProductBackupError::IncompleteComponents);
        }
        let mut component_index = 0_usize;
        for (owner, owner_receipts) in self.retained.iter_mut().zip(receipts) {
            if cancellation.is_cancelled() {
                return Err(ProductBackupError::Cancelled);
            }
            if owner.lease.descriptors() != owner.descriptors.as_slice() {
                return Err(ProductBackupError::IncompleteComponents);
            }
            let descriptors = owner.descriptors.clone();
            if owner_receipts.len() != descriptors.len() {
                return Err(ProductBackupError::IncompleteComponents);
            }
            for (descriptor, receipt) in descriptors.into_iter().zip(owner_receipts.iter().copied())
            {
                let component = components
                    .get(component_index)
                    .ok_or(ProductBackupError::IncompleteComponents)?;
                if component.snapshot() != snapshot
                    || component.kind() != descriptor.kind
                    || component.byte_length() != receipt.byte_length
                    || component.sha256() != receipt.sha256
                {
                    return Err(ProductBackupError::ArtifactMismatch);
                }
                owner
                    .lease
                    .revalidate(descriptor.kind, snapshot, receipt, cancellation)
                    .await?;
                component_index = component_index
                    .checked_add(1)
                    .ok_or(ProductBackupError::IncompleteComponents)?;
            }
        }
        if component_index != components.len() {
            return Err(ProductBackupError::IncompleteComponents);
        }
        Ok(())
    }
}

struct DigestingWriter<'writer> {
    writer: &'writer mut (dyn Write + Send),
    digest: Sha256,
    observed: u64,
    maximum: u64,
}

impl<'writer> DigestingWriter<'writer> {
    fn new(writer: &'writer mut (dyn Write + Send), maximum: u64) -> Self {
        Self {
            writer,
            digest: Sha256::new(),
            observed: 0,
            maximum,
        }
    }

    fn finish(self) -> Result<ObservedComponentArtifact, ProductBackupError> {
        if self.observed == 0 || self.observed > self.maximum {
            return Err(ProductBackupError::InvalidComponent);
        }
        Ok(ObservedComponentArtifact {
            byte_length: self.observed,
            sha256: self.digest.finalize().into(),
        })
    }
}

struct ObservedComponentArtifact {
    byte_length: u64,
    sha256: [u8; 32],
}

impl Write for DigestingWriter<'_> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let buffer_length = u64::try_from(buffer.len())
            .map_err(|_| std::io::Error::other("workspace component exceeds its bound"))?;
        let prospective = self
            .observed
            .checked_add(buffer_length)
            .ok_or_else(|| std::io::Error::other("workspace component exceeds its bound"))?;
        if prospective > self.maximum {
            return Err(std::io::Error::other(
                "workspace component exceeds its bound",
            ));
        }
        let written = self.writer.write(buffer)?;
        let written_length = u64::try_from(written)
            .map_err(|_| std::io::Error::other("workspace component exceeds its bound"))?;
        self.observed = self
            .observed
            .checked_add(written_length)
            .ok_or_else(|| std::io::Error::other("workspace component exceeds its bound"))?;
        self.digest.update(&buffer[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

struct DigestingReader {
    reader: Box<dyn VerifiedWorkspaceComponentReader>,
    digest: Sha256,
    observed: u64,
    expected_length: u64,
}

impl DigestingReader {
    fn new(reader: Box<dyn VerifiedWorkspaceComponentReader>, expected_length: u64) -> Self {
        Self {
            reader,
            digest: Sha256::new(),
            observed: 0,
            expected_length,
        }
    }

    fn finish(mut self, expected_sha256: [u8; 32]) -> Result<(), ProductBackupError> {
        let mut trailing = [0_u8; 1];
        if self
            .read(&mut trailing)
            .map_err(|_| ProductBackupError::ArtifactUnavailable)?
            != 0
            || self.observed != self.expected_length
            || <[u8; 32]>::from(self.digest.finalize()) != expected_sha256
        {
            return Err(ProductBackupError::ArtifactMismatch);
        }
        Ok(())
    }
}

impl Read for DigestingReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let read = self.reader.read(buffer)?;
        let read_length = u64::try_from(read)
            .map_err(|_| std::io::Error::other("workspace component exceeds its manifest"))?;
        self.observed = self
            .observed
            .checked_add(read_length)
            .ok_or_else(|| std::io::Error::other("workspace component exceeds its manifest"))?;
        if self.observed > self.expected_length {
            return Err(std::io::Error::other(
                "workspace component exceeds its manifest",
            ));
        }
        self.digest.update(&buffer[..read]);
        Ok(read)
    }
}

fn component_reference(kind: ProductBackupComponentKind) -> String {
    let name = match kind {
        ProductBackupComponentKind::Configuration => "configuration",
        ProductBackupComponentKind::ProviderMetadata => "provider-metadata",
        ProductBackupComponentKind::SourceData => "source-data",
        ProductBackupComponentKind::Portfolios => "portfolios",
        ProductBackupComponentKind::Transactions => "transactions",
        ProductBackupComponentKind::Models => "models",
        ProductBackupComponentKind::DecisionTargets => "decision-targets",
        ProductBackupComponentKind::JobsAndReceipts => "jobs-and-receipts",
        ProductBackupComponentKind::FairValueEvidence => "fair-value-evidence",
    };
    format!("product-components/{name}.msq")
}

#[cfg(unix)]
fn configure_private_creation(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;

    options.mode(0o600);
}

#[cfg(not(unix))]
fn configure_private_creation(_options: &mut OpenOptions) {}

#[cfg(unix)]
fn synchronize_directory(directory: &cap_std::fs::Dir) -> Result<(), ProductBackupError> {
    use cap_std::fs::OpenOptionsExt as _;

    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    options.custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC);
    directory
        .open_with("product-components", &options)
        .map(cap_std::fs::File::into_std)
        .and_then(|opened| opened.sync_all())
        .map_err(|_| ProductBackupError::ArtifactUnavailable)
}

#[cfg(windows)]
fn synchronize_directory(_directory: &cap_std::fs::Dir) -> Result<(), ProductBackupError> {
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn synchronize_directory(_directory: &cap_std::fs::Dir) -> Result<(), ProductBackupError> {
    Err(ProductBackupError::ArtifactUnavailable)
}
