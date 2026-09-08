//! Workspace-backup adapter for the complete durable decision-journal authority.

use std::{fmt, io::Write, sync::Arc};

use async_trait::async_trait;
use market_squawk_domain::{SchemaVersion, SourceIdentifier};
use tokio_util::sync::CancellationToken;

use crate::application::{
    backup::{
        ProductBackupComponentKind, ProductBackupComponentSchema, ProductBackupError,
        ProductBackupSensitivity, ProductBackupSnapshot,
    },
    decision::{DecisionApplication, RetainedDecisionBackupSnapshot},
};

use super::workspace_backup::{
    WorkspaceComponentDescriptor, WorkspaceComponentSnapshotAuthority,
    WorkspaceComponentSnapshotLease, WorkspaceComponentSnapshotReceipt,
};

const DECISION_PRODUCER: &str = "market-squawk-decision-journal-v1";

/// The sole DecisionTargets producer backed by the complete typed decision journal.
pub(crate) struct DecisionWorkspaceBackupAuthority {
    decisions: Arc<DecisionApplication>,
    descriptors: [WorkspaceComponentDescriptor; 1],
}

impl DecisionWorkspaceBackupAuthority {
    /// Binds DecisionTargets backup to the same application that owns every decision mutation.
    pub(super) fn try_new(decisions: Arc<DecisionApplication>) -> Result<Self, ProductBackupError> {
        let producer = SourceIdentifier::try_from(DECISION_PRODUCER)
            .map_err(|_| ProductBackupError::InvalidComponent)?;
        let schema =
            ProductBackupComponentSchema::try_new(producer.clone(), SchemaVersion::CURRENT)?;
        Ok(Self {
            decisions,
            descriptors: [WorkspaceComponentDescriptor::try_new(
                ProductBackupComponentKind::DecisionTargets,
                producer,
                schema,
                ProductBackupSensitivity::Protected,
            )?],
        })
    }
}

impl fmt::Debug for DecisionWorkspaceBackupAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DecisionWorkspaceBackupAuthority([DECISION JOURNAL OWNER])")
    }
}

#[async_trait]
impl WorkspaceComponentSnapshotAuthority for DecisionWorkspaceBackupAuthority {
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
            .decisions
            .retain_backup()
            .map_err(|_| ProductBackupError::SnapshotMismatch)?;
        if cancellation.is_cancelled() {
            return Err(ProductBackupError::Cancelled);
        }
        Ok(Box::new(RetainedDecisionWorkspaceSnapshot {
            descriptors: self.descriptors.clone(),
            retained,
            emitted: None,
        }))
    }
}

struct RetainedDecisionWorkspaceSnapshot {
    descriptors: [WorkspaceComponentDescriptor; 1],
    retained: RetainedDecisionBackupSnapshot,
    emitted: Option<EmittedDecisionSnapshot>,
}

#[derive(Clone, Copy)]
struct EmittedDecisionSnapshot {
    snapshot: ProductBackupSnapshot,
    receipt: WorkspaceComponentSnapshotReceipt,
    authority_revision_sha256: [u8; 32],
    byte_length: u64,
    content_sha256: [u8; 32],
}

impl fmt::Debug for RetainedDecisionWorkspaceSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RetainedDecisionWorkspaceSnapshot([FENCED SQLITE EXPORT])")
    }
}

#[async_trait]
impl WorkspaceComponentSnapshotLease for RetainedDecisionWorkspaceSnapshot {
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
        if kind != ProductBackupComponentKind::DecisionTargets || self.emitted.is_some() {
            return Err(ProductBackupError::InvalidComponent);
        }
        let bytes = self.retained.bytes();
        let byte_length =
            u64::try_from(bytes.len()).map_err(|_| ProductBackupError::InvalidComponent)?;
        let authority_revision_sha256 = self.retained.authority_revision_sha256();
        let content_sha256 = self.retained.content_sha256();
        writer
            .write_all(bytes)
            .map_err(|_| ProductBackupError::ArtifactUnavailable)?;
        if cancellation.is_cancelled() {
            return Err(ProductBackupError::Cancelled);
        }
        let receipt = WorkspaceComponentSnapshotReceipt::try_new(
            authority_revision_sha256,
            byte_length,
            content_sha256,
        )?;
        self.emitted = Some(EmittedDecisionSnapshot {
            snapshot,
            receipt,
            authority_revision_sha256,
            byte_length,
            content_sha256,
        });
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
        let emitted = self.emitted.ok_or(ProductBackupError::SnapshotMismatch)?;
        if kind != ProductBackupComponentKind::DecisionTargets
            || snapshot != emitted.snapshot
            || receipt != emitted.receipt
        {
            return Err(ProductBackupError::SnapshotMismatch);
        }
        self.retained
            .revalidate_emitted(
                emitted.authority_revision_sha256,
                emitted.byte_length,
                emitted.content_sha256,
            )
            .map_err(|_| ProductBackupError::SnapshotMismatch)
    }
}
