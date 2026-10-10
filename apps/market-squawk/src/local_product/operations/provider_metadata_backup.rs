//! Workspace-backup adapter for the genuine provider-metadata owner.

use std::{fmt, io::Write};

use async_trait::async_trait;
use market_squawk_domain::{SchemaVersion, SourceIdentifier};
use tokio_util::sync::CancellationToken;

use crate::application::backup::{
    ProductBackupComponentKind, ProductBackupComponentSchema, ProductBackupError,
    ProductBackupSensitivity, ProductBackupSnapshot,
};
use crate::local_product::provider_activation_state::{
    PROVIDER_METADATA_BACKUP_PRODUCER, PROVIDER_METADATA_BACKUP_SCHEMA,
    ProviderMetadataBackupAuthority, ProviderMetadataBackupError, RetainedProviderMetadataBackup,
};

use super::workspace_backup::{
    WorkspaceComponentDescriptor, WorkspaceComponentSnapshotAuthority,
    WorkspaceComponentSnapshotLease, WorkspaceComponentSnapshotReceipt,
};

/// Protected workspace component backed by the combined activation/onboarding/registry owner.
pub(crate) struct ProviderMetadataWorkspaceBackupAuthority {
    owner: ProviderMetadataBackupAuthority,
    descriptors: [WorkspaceComponentDescriptor; 1],
}

impl ProviderMetadataWorkspaceBackupAuthority {
    /// Declares the provider-owned v1 schema to the installed workspace backup composition.
    pub(super) fn try_new(
        owner: ProviderMetadataBackupAuthority,
    ) -> Result<Self, ProductBackupError> {
        let producer = SourceIdentifier::try_from(PROVIDER_METADATA_BACKUP_PRODUCER)
            .map_err(|_| ProductBackupError::InvalidComponent)?;
        let schema_identity = SourceIdentifier::try_from(PROVIDER_METADATA_BACKUP_SCHEMA)
            .map_err(|_| ProductBackupError::InvalidComponentSchema)?;
        let schema =
            ProductBackupComponentSchema::try_new(schema_identity, SchemaVersion::CURRENT)?;
        let descriptor = WorkspaceComponentDescriptor::try_new(
            ProductBackupComponentKind::ProviderMetadata,
            producer,
            schema,
            ProductBackupSensitivity::Protected,
        )?;
        Ok(Self {
            owner,
            descriptors: [descriptor],
        })
    }
}

impl fmt::Debug for ProviderMetadataWorkspaceBackupAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .write_str("ProviderMetadataWorkspaceBackupAuthority([SEALED PROVIDER METADATA OWNER])")
    }
}

#[async_trait]
impl WorkspaceComponentSnapshotAuthority for ProviderMetadataWorkspaceBackupAuthority {
    fn descriptors(&self) -> &[WorkspaceComponentDescriptor] {
        &self.descriptors
    }

    async fn retain(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<Box<dyn WorkspaceComponentSnapshotLease>, ProductBackupError> {
        let retained = self
            .owner
            .retain(cancellation)
            .await
            .map_err(map_provider_metadata_error)?;
        Ok(Box::new(RetainedProviderMetadataWorkspaceBackup {
            retained,
            descriptors: self.descriptors.clone(),
            emission: None,
        }))
    }
}

struct RetainedProviderMetadataWorkspaceBackup {
    retained: RetainedProviderMetadataBackup,
    descriptors: [WorkspaceComponentDescriptor; 1],
    emission: Option<(ProductBackupSnapshot, WorkspaceComponentSnapshotReceipt)>,
}

impl fmt::Debug for RetainedProviderMetadataWorkspaceBackup {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedProviderMetadataWorkspaceBackup")
            .field("retained", &self.retained)
            .field("emitted", &self.emission.is_some())
            .finish()
    }
}

#[async_trait]
impl WorkspaceComponentSnapshotLease for RetainedProviderMetadataWorkspaceBackup {
    fn descriptors(&self) -> &[WorkspaceComponentDescriptor] {
        &self.descriptors
    }

    async fn write_snapshot(
        &mut self,
        kind: ProductBackupComponentKind,
        snapshot: ProductBackupSnapshot,
        writer: &mut (dyn Write + Send),
        cancellation: &CancellationToken,
    ) -> Result<WorkspaceComponentSnapshotReceipt, ProductBackupError> {
        if cancellation.is_cancelled() {
            return Err(ProductBackupError::Cancelled);
        }
        if kind != ProductBackupComponentKind::ProviderMetadata || self.emission.is_some() {
            return Err(ProductBackupError::InvalidComponent);
        }
        let bytes = self.retained.bytes();
        let byte_length =
            u64::try_from(bytes.len()).map_err(|_| ProductBackupError::InvalidComponent)?;
        writer
            .write_all(bytes)
            .map_err(|_| ProductBackupError::ArtifactUnavailable)?;
        if cancellation.is_cancelled() {
            return Err(ProductBackupError::Cancelled);
        }
        let revision = self.retained.authority_revision_sha256();
        let receipt = WorkspaceComponentSnapshotReceipt::try_new(revision, byte_length, revision)?;
        self.emission = Some((snapshot, receipt));
        Ok(receipt)
    }

    async fn revalidate(
        &mut self,
        kind: ProductBackupComponentKind,
        snapshot: ProductBackupSnapshot,
        receipt: WorkspaceComponentSnapshotReceipt,
        cancellation: &CancellationToken,
    ) -> Result<(), ProductBackupError> {
        if cancellation.is_cancelled() {
            return Err(ProductBackupError::Cancelled);
        }
        if kind != ProductBackupComponentKind::ProviderMetadata {
            return Err(ProductBackupError::InvalidComponent);
        }
        let Some((emitted_snapshot, emitted_receipt)) = self.emission else {
            return Err(ProductBackupError::InvalidComponent);
        };
        if snapshot != emitted_snapshot {
            return Err(ProductBackupError::SnapshotMismatch);
        }
        if receipt != emitted_receipt {
            return Err(ProductBackupError::InvalidComponent);
        }
        let byte_length = u64::try_from(self.retained.bytes().len())
            .map_err(|_| ProductBackupError::InvalidComponent)?;
        let revision = self.retained.authority_revision_sha256();
        self.retained
            .revalidate_emitted(revision, byte_length, revision)
            .map_err(map_provider_metadata_error)
    }
}

fn map_provider_metadata_error(error: ProviderMetadataBackupError) -> ProductBackupError {
    match error {
        ProviderMetadataBackupError::Cancelled => ProductBackupError::Cancelled,
        ProviderMetadataBackupError::Invalid => ProductBackupError::InvalidComponent,
        ProviderMetadataBackupError::RestoreTargetNotFresh => {
            ProductBackupError::InvalidRestoreTarget
        }
        ProviderMetadataBackupError::ResourceExhausted
        | ProviderMetadataBackupError::Activation(_)
        | ProviderMetadataBackupError::Research(_)
        | ProviderMetadataBackupError::Registry(_)
        | ProviderMetadataBackupError::Store(_) => ProductBackupError::ArtifactUnavailable,
    }
}
