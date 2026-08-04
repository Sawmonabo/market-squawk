//! Workspace-backup adapter for the genuine durable jobs-and-receipts owner.

use std::{
    fmt,
    io::Write,
    sync::{Arc, Weak},
};

use async_trait::async_trait;
use market_squawk_domain::{SchemaVersion, SourceIdentifier};
use market_squawk_jobs::{
    JOBS_AND_RECEIPTS_BACKUP_SCHEMA, JobAuthority, JobRunner, JobsAndReceiptsBackupBinding,
    JobsAndReceiptsBackupReceipt, RetainedJobsAndReceiptsSnapshot, SqliteJobRepository,
};
use tokio_util::sync::CancellationToken;

use crate::{
    application::backup::{
        ProductBackupComponentKind, ProductBackupComponentSchema, ProductBackupError,
        ProductBackupSensitivity, ProductBackupSnapshot,
    },
    jobs::{BackupJobRunner, InstalledJobAuthority},
};

use super::workspace_backup::{
    WorkspaceComponentDescriptor, WorkspaceComponentSnapshotAuthority,
    WorkspaceComponentSnapshotLease, WorkspaceComponentSnapshotReceipt,
};

const PRODUCER: &str = "market-squawk-jobs-authority-v1";
const WRITE_CHUNK_BYTES: usize = 64 * 1024;

/// Fixed adapter binding the component to the installed job authority and code-owned backup kind.
pub(crate) struct JobsAndReceiptsWorkspaceBackupAuthority {
    authority: Weak<JobAuthority<SqliteJobRepository>>,
    backup_kind: SourceIdentifier,
    descriptors: [WorkspaceComponentDescriptor; 1],
}

impl JobsAndReceiptsWorkspaceBackupAuthority {
    /// Binds the adapter to the same installed authority and runner registration used at runtime.
    pub(super) fn try_new(
        jobs: &InstalledJobAuthority,
        backup_runner: &BackupJobRunner,
    ) -> Result<Self, ProductBackupError> {
        let producer = SourceIdentifier::try_from(PRODUCER)
            .map_err(|_| ProductBackupError::InvalidComponent)?;
        let schema_identity = SourceIdentifier::try_from(JOBS_AND_RECEIPTS_BACKUP_SCHEMA)
            .map_err(|_| ProductBackupError::InvalidComponentSchema)?;
        let schema =
            ProductBackupComponentSchema::try_new(schema_identity, SchemaVersion::CURRENT)?;
        let authority = jobs.authority();
        Ok(Self {
            authority: Arc::downgrade(&authority),
            backup_kind: backup_runner.kind().clone(),
            descriptors: [WorkspaceComponentDescriptor::try_new(
                ProductBackupComponentKind::JobsAndReceipts,
                producer,
                schema,
                ProductBackupSensitivity::Protected,
            )?],
        })
    }
}

impl fmt::Debug for JobsAndReceiptsWorkspaceBackupAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .write_str("JobsAndReceiptsWorkspaceBackupAuthority([JOB ADMISSION AND WRITER OWNER])")
    }
}

#[async_trait]
impl WorkspaceComponentSnapshotAuthority for JobsAndReceiptsWorkspaceBackupAuthority {
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
        let authority = self
            .authority
            .upgrade()
            .ok_or(ProductBackupError::SnapshotMismatch)?;
        let retained = authority
            .retain_jobs_and_receipts_backup(&self.backup_kind)
            .await
            .map_err(|_| ProductBackupError::SnapshotMismatch)?;
        Ok(Box::new(RetainedJobsAndReceiptsWorkspaceSnapshot {
            descriptors: self.descriptors.clone(),
            retained,
            materialized: None,
        }))
    }
}

struct RetainedJobsAndReceiptsWorkspaceSnapshot {
    descriptors: [WorkspaceComponentDescriptor; 1],
    retained: RetainedJobsAndReceiptsSnapshot,
    materialized: Option<MaterializedReceipt>,
}

#[derive(Clone, Copy)]
struct MaterializedReceipt {
    binding: JobsAndReceiptsBackupBinding,
    owner: JobsAndReceiptsBackupReceipt,
    component: WorkspaceComponentSnapshotReceipt,
}

impl fmt::Debug for RetainedJobsAndReceiptsWorkspaceSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(
            "RetainedJobsAndReceiptsWorkspaceSnapshot([ADMISSION SCHEDULER WRITER FENCES])",
        )
    }
}

#[async_trait]
impl WorkspaceComponentSnapshotLease for RetainedJobsAndReceiptsWorkspaceSnapshot {
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
        require_request(kind, cancellation)?;
        if self.materialized.is_some() {
            return Err(ProductBackupError::SnapshotMismatch);
        }
        let binding = binding(snapshot)?;
        let export = self
            .retained
            .materialize(binding)
            .await
            .map_err(|_| ProductBackupError::SnapshotMismatch)?;
        let owner = export.receipt();
        for chunk in export.as_bytes().chunks(WRITE_CHUNK_BYTES) {
            if cancellation.is_cancelled() {
                return Err(ProductBackupError::Cancelled);
            }
            writer
                .write_all(chunk)
                .map_err(|_| ProductBackupError::ArtifactUnavailable)?;
        }
        let component = WorkspaceComponentSnapshotReceipt::try_new(
            owner.authority_revision_sha256(),
            owner.byte_length(),
            owner.sha256(),
        )?;
        self.materialized = Some(MaterializedReceipt {
            binding,
            owner,
            component,
        });
        Ok(component)
    }

    async fn revalidate(
        &mut self,
        kind: ProductBackupComponentKind,
        snapshot: ProductBackupSnapshot,
        receipt: WorkspaceComponentSnapshotReceipt,
        cancellation: &CancellationToken,
    ) -> Result<(), ProductBackupError> {
        require_request(kind, cancellation)?;
        let materialized = self
            .materialized
            .ok_or(ProductBackupError::SnapshotMismatch)?;
        if materialized.binding != binding(snapshot)? || materialized.component != receipt {
            return Err(ProductBackupError::SnapshotMismatch);
        }
        self.retained
            .revalidate(materialized.binding, materialized.owner)
            .map_err(|_| ProductBackupError::SnapshotMismatch)
    }
}

fn require_request(
    kind: ProductBackupComponentKind,
    cancellation: &CancellationToken,
) -> Result<(), ProductBackupError> {
    if cancellation.is_cancelled() {
        return Err(ProductBackupError::Cancelled);
    }
    if kind != ProductBackupComponentKind::JobsAndReceipts {
        return Err(ProductBackupError::InvalidComponent);
    }
    Ok(())
}

fn binding(
    snapshot: ProductBackupSnapshot,
) -> Result<JobsAndReceiptsBackupBinding, ProductBackupError> {
    JobsAndReceiptsBackupBinding::try_new(snapshot.cutoff(), snapshot.snapshot_id())
        .map_err(|_| ProductBackupError::InvalidSnapshot)
}
