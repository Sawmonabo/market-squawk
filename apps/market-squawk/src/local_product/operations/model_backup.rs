//! Workspace-backup adapter for the admitted model and forecast authorities.

use std::{fmt, io::Write, sync::Arc};

use async_trait::async_trait;
use market_squawk_domain::{SchemaVersion, SourceIdentifier};
use tokio_util::sync::CancellationToken;

use crate::application::{
    backup::{
        ProductBackupComponentKind, ProductBackupComponentSchema, ProductBackupError,
        ProductBackupSensitivity, ProductBackupSnapshot,
    },
    model::backup::{
        MODEL_BACKUP_SCHEMA_VERSION, ModelBackupAuthority, ModelBackupError, ModelBackupSnapshot,
    },
};

use super::workspace_backup::{
    WorkspaceComponentDescriptor, WorkspaceComponentSnapshotAuthority,
    WorkspaceComponentSnapshotLease, WorkspaceComponentSnapshotReceipt,
};

const MODELS_PRODUCER: &str = "market-squawk-model-authority-v1";

/// The one Models component owner over admitted runtime and immutable forecast state.
pub(crate) struct ModelWorkspaceBackupAuthority {
    models: Arc<ModelBackupAuthority>,
    descriptors: [WorkspaceComponentDescriptor; 1],
}

impl ModelWorkspaceBackupAuthority {
    /// Binds the Models component to the same authorities used by inference and forecasts.
    pub(super) fn try_new(models: Arc<ModelBackupAuthority>) -> Result<Self, ProductBackupError> {
        if SchemaVersion::CURRENT.get() != MODEL_BACKUP_SCHEMA_VERSION {
            return Err(ProductBackupError::InvalidComponentSchema);
        }
        let producer = SourceIdentifier::try_from(MODELS_PRODUCER)
            .map_err(|_| ProductBackupError::InvalidComponent)?;
        let schema =
            ProductBackupComponentSchema::try_new(producer.clone(), SchemaVersion::CURRENT)?;
        Ok(Self {
            models,
            descriptors: [WorkspaceComponentDescriptor::try_new(
                ProductBackupComponentKind::Models,
                producer,
                schema,
                ProductBackupSensitivity::Protected,
            )?],
        })
    }
}

impl fmt::Debug for ModelWorkspaceBackupAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ModelWorkspaceBackupAuthority([MODEL AND FORECAST OWNERS])")
    }
}

#[async_trait]
impl WorkspaceComponentSnapshotAuthority for ModelWorkspaceBackupAuthority {
    fn descriptors(&self) -> &[WorkspaceComponentDescriptor] {
        &self.descriptors
    }

    async fn retain(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<Box<dyn WorkspaceComponentSnapshotLease>, ProductBackupError> {
        let retained = self
            .models
            .retain(cancellation)
            .await
            .map_err(map_model_backup_error)?;
        Ok(Box::new(RetainedModelWorkspaceSnapshot {
            descriptors: self.descriptors.clone(),
            retained,
            issued: None,
        }))
    }
}

struct RetainedModelWorkspaceSnapshot {
    descriptors: [WorkspaceComponentDescriptor; 1],
    retained: ModelBackupSnapshot,
    issued: Option<(ProductBackupSnapshot, WorkspaceComponentSnapshotReceipt)>,
}

impl fmt::Debug for RetainedModelWorkspaceSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedModelWorkspaceSnapshot")
            .field("retained", &self.retained)
            .field("issued", &self.issued.is_some())
            .finish()
    }
}

#[async_trait]
impl WorkspaceComponentSnapshotLease for RetainedModelWorkspaceSnapshot {
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
        if kind != ProductBackupComponentKind::Models {
            return Err(ProductBackupError::InvalidComponent);
        }
        if self.issued.is_some() {
            return Err(ProductBackupError::SnapshotMismatch);
        }
        let model_receipt = self
            .retained
            .write_to(writer, cancellation)
            .map_err(map_model_backup_error)?;
        let receipt = WorkspaceComponentSnapshotReceipt::try_new(
            model_receipt.semantic_authority_revision(),
            model_receipt.byte_length(),
            model_receipt.sha256(),
        )?;
        self.issued = Some((snapshot, receipt));
        Ok(receipt)
    }

    async fn revalidate(
        &mut self,
        kind: ProductBackupComponentKind,
        snapshot: ProductBackupSnapshot,
        receipt: WorkspaceComponentSnapshotReceipt,
        cancellation: &CancellationToken,
    ) -> Result<(), ProductBackupError> {
        if kind != ProductBackupComponentKind::Models {
            return Err(ProductBackupError::InvalidComponent);
        }
        if self.issued != Some((snapshot, receipt)) {
            return Err(ProductBackupError::SnapshotMismatch);
        }
        self.retained
            .revalidate(cancellation)
            .await
            .map_err(map_model_backup_error)
    }
}

fn map_model_backup_error(error: ModelBackupError) -> ProductBackupError {
    match error {
        ModelBackupError::Cancelled => ProductBackupError::Cancelled,
        ModelBackupError::AuthorityChanged => ProductBackupError::SnapshotMismatch,
        ModelBackupError::Archive
        | ModelBackupError::CoordinateMismatch
        | ModelBackupError::ArtifactMismatch => ProductBackupError::ArtifactMismatch,
        ModelBackupError::InvalidLimits | ModelBackupError::Capacity => {
            ProductBackupError::InvalidComponent
        }
        ModelBackupError::Runtime(_)
        | ModelBackupError::Forecast(_)
        | ModelBackupError::Artifact(_)
        | ModelBackupError::Path(_)
        | ModelBackupError::ArtifactPath(_)
        | ModelBackupError::Io(_) => ProductBackupError::ArtifactUnavailable,
    }
}
