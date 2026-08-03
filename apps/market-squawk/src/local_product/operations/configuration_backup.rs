//! Workspace-backup adapter for the genuine settings lifecycle authority.

use std::{fmt, io::Write, sync::Arc};

use async_trait::async_trait;
use market_squawk_domain::{SchemaVersion, SourceIdentifier};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use crate::application::backup::{
    ProductBackupComponentKind, ProductBackupComponentSchema, ProductBackupError,
    ProductBackupSensitivity, ProductBackupSnapshot,
};

use super::{
    settings::{ProductionSettingsOperations, RetainedWorkspaceConfiguration},
    workspace_backup::{
        WorkspaceComponentDescriptor, WorkspaceComponentSnapshotAuthority,
        WorkspaceComponentSnapshotLease, WorkspaceComponentSnapshotReceipt,
    },
};

const CONFIGURATION_PRODUCER: &str = "market-squawk-workspace-configuration-v1";

/// The one Configuration component owner: durable settings plus its completed lifecycle journal.
pub(crate) struct ConfigurationWorkspaceBackupAuthority {
    settings: Arc<ProductionSettingsOperations>,
    descriptors: [WorkspaceComponentDescriptor; 1],
}

impl ConfigurationWorkspaceBackupAuthority {
    /// Binds the Configuration component to the same transaction owner that persists settings.
    pub(super) fn try_new(
        settings: Arc<ProductionSettingsOperations>,
    ) -> Result<Self, ProductBackupError> {
        let producer = SourceIdentifier::try_from(CONFIGURATION_PRODUCER)
            .map_err(|_| ProductBackupError::InvalidComponent)?;
        let schema =
            ProductBackupComponentSchema::try_new(producer.clone(), SchemaVersion::CURRENT)?;
        Ok(Self {
            settings,
            descriptors: [WorkspaceComponentDescriptor::try_new(
                ProductBackupComponentKind::Configuration,
                producer,
                schema,
                ProductBackupSensitivity::Protected,
            )?],
        })
    }
}

impl fmt::Debug for ConfigurationWorkspaceBackupAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ConfigurationWorkspaceBackupAuthority([SETTINGS TRANSACTION OWNER])")
    }
}

#[async_trait]
impl WorkspaceComponentSnapshotAuthority for ConfigurationWorkspaceBackupAuthority {
    fn descriptors(&self) -> &[WorkspaceComponentDescriptor] {
        &self.descriptors
    }

    async fn retain(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<Box<dyn WorkspaceComponentSnapshotLease>, ProductBackupError> {
        if cancellation.is_cancelled() {
            return Err(ProductBackupError::Cancelled);
        }
        let retained = self
            .settings
            .retain_workspace_configuration()
            .map_err(|_| ProductBackupError::SnapshotMismatch)?;
        Ok(Box::new(RetainedConfigurationWorkspaceSnapshot {
            settings: Arc::clone(&self.settings),
            descriptors: self.descriptors.clone(),
            retained,
        }))
    }
}

struct RetainedConfigurationWorkspaceSnapshot {
    settings: Arc<ProductionSettingsOperations>,
    descriptors: [WorkspaceComponentDescriptor; 1],
    retained: RetainedWorkspaceConfiguration,
}

impl fmt::Debug for RetainedConfigurationWorkspaceSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RetainedConfigurationWorkspaceSnapshot([CANONICAL SETTINGS EXPORT])")
    }
}

#[async_trait]
impl WorkspaceComponentSnapshotLease for RetainedConfigurationWorkspaceSnapshot {
    fn descriptors(&self) -> &[WorkspaceComponentDescriptor] {
        &self.descriptors
    }

    async fn write_snapshot(
        &mut self,
        kind: ProductBackupComponentKind,
        _snapshot: ProductBackupSnapshot,
        writer: &mut (dyn Write + Send),
        cancellation: &CancellationToken,
    ) -> Result<WorkspaceComponentSnapshotReceipt, ProductBackupError> {
        if cancellation.is_cancelled() {
            return Err(ProductBackupError::Cancelled);
        }
        if kind != ProductBackupComponentKind::Configuration {
            return Err(ProductBackupError::InvalidComponent);
        }
        let bytes = self.retained.canonical_bytes();
        writer
            .write_all(bytes)
            .map_err(|_| ProductBackupError::ArtifactUnavailable)?;
        WorkspaceComponentSnapshotReceipt::try_new(
            self.retained.authority_revision_sha256(),
            u64::try_from(bytes.len()).map_err(|_| ProductBackupError::InvalidComponent)?,
            Sha256::digest(bytes).into(),
        )
    }

    async fn revalidate(
        &mut self,
        kind: ProductBackupComponentKind,
        _snapshot: ProductBackupSnapshot,
        _receipt: WorkspaceComponentSnapshotReceipt,
        cancellation: &CancellationToken,
    ) -> Result<(), ProductBackupError> {
        if cancellation.is_cancelled() {
            return Err(ProductBackupError::Cancelled);
        }
        if kind != ProductBackupComponentKind::Configuration {
            return Err(ProductBackupError::InvalidComponent);
        }
        self.settings
            .revalidate_workspace_configuration(&self.retained)
            .map_err(|_| ProductBackupError::SnapshotMismatch)
    }
}
