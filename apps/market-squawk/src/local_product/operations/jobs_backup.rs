//! Workspace-backup adapter for the genuine durable jobs-and-receipts owner.

use std::{
    fmt,
    io::{Read, Write},
    num::NonZeroUsize,
    sync::{Arc, Weak},
};

use crate::application::analysis::{
    GovernedBacktestInputAuthorityLimits, GovernedBacktestRepositoryLimits,
    ProductionGovernedBacktestInputAuthority, ProductionGovernedBacktestRepository,
};
use async_trait::async_trait;
use market_squawk_domain::{SchemaVersion, SourceIdentifier};
use market_squawk_jobs::JobRepositoryConfig;
use market_squawk_jobs::{
    JOBS_AND_RECEIPTS_BACKUP_SCHEMA, JobAuthority, JobRunner, JobsAndReceiptsBackupBinding,
    JobsAndReceiptsBackupReceipt, RetainedJobsAndReceiptsSnapshot, SqliteJobRepository,
};
use market_squawk_platform::{JobDatabaseLocation, LocalPaths};
use market_squawk_services::{ArtifactAuthority, ArtifactReference, ArtifactRepository};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

mod artifacts;

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

/// Fixed adapter binding the component to the installed job authority and code-owned backup kind.
pub(crate) struct JobsAndReceiptsWorkspaceBackupAuthority {
    authority: Weak<JobAuthority<SqliteJobRepository>>,
    backup_kind: SourceIdentifier,
    artifacts: Arc<dyn ArtifactAuthority>,
    backtest_inputs: Arc<ProductionGovernedBacktestInputAuthority>,
    backtest_terminals: Arc<ProductionGovernedBacktestRepository>,
    descriptors: [WorkspaceComponentDescriptor; 1],
}

impl JobsAndReceiptsWorkspaceBackupAuthority {
    /// Binds the adapter to the same installed authority and runner registration used at runtime.
    pub(super) fn try_new(
        jobs: &InstalledJobAuthority,
        backup_runner: &BackupJobRunner,
        artifacts: Arc<dyn ArtifactAuthority>,
        backtest_inputs: Arc<ProductionGovernedBacktestInputAuthority>,
        backtest_terminals: Arc<ProductionGovernedBacktestRepository>,
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
            artifacts,
            backtest_inputs,
            backtest_terminals,
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
        let (input_index, input_revision) = self
            .backtest_inputs
            .export_backup_index(cancellation, index_deadline()?)
            .await
            .map_err(map_index_error)?;
        let (terminal_index, terminal_revision) = self
            .backtest_terminals
            .export_backup_index(cancellation, index_deadline()?)
            .await
            .map_err(map_index_error)?;
        let terminal_artifacts = self
            .backtest_terminals
            .backup_index_artifacts(&terminal_index, artifacts::MAXIMUM_INPUT_ARTIFACTS)
            .map_err(map_index_error)?;
        Ok(Box::new(RetainedJobsAndReceiptsWorkspaceSnapshot {
            descriptors: self.descriptors.clone(),
            retained,
            artifacts: Arc::clone(&self.artifacts),
            backtest_inputs: Arc::clone(&self.backtest_inputs),
            backtest_terminals: Arc::clone(&self.backtest_terminals),
            input_index,
            terminal_index,
            terminal_artifacts,
            input_revision,
            terminal_revision,
            retained_artifacts: Vec::new(),
            materialized: None,
        }))
    }
}

struct RetainedJobsAndReceiptsWorkspaceSnapshot {
    descriptors: [WorkspaceComponentDescriptor; 1],
    retained: RetainedJobsAndReceiptsSnapshot,
    artifacts: Arc<dyn ArtifactAuthority>,
    backtest_inputs: Arc<ProductionGovernedBacktestInputAuthority>,
    backtest_terminals: Arc<ProductionGovernedBacktestRepository>,
    input_index: Vec<u8>,
    terminal_index: Vec<u8>,
    terminal_artifacts: Vec<ArtifactReference>,
    input_revision: [u8; 32],
    terminal_revision: [u8; 32],
    retained_artifacts: Vec<ArtifactReference>,
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
        let (component, retained_artifacts) = artifacts::write_component(
            &export,
            self.artifacts.as_ref(),
            [&self.input_index, &self.terminal_index],
            [self.input_revision, self.terminal_revision],
            &self.terminal_artifacts,
            writer,
            cancellation,
        )
        .await?;
        self.revalidate_indices(cancellation).await?;
        // The original owner's revalidation releases its sole-writer fence. Keep that
        // fence through component materialization until the aggregate's final validation.
        self.retained_artifacts = retained_artifacts;
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
        artifacts::revalidate_artifacts(
            self.artifacts.as_ref(),
            &self.retained_artifacts,
            cancellation,
        )
        .await?;
        self.revalidate_indices(cancellation).await?;
        require_request(kind, cancellation)?;
        // Validate the exact owner receipt once, after every dependent artifact and index
        // passed, and only then release the original repository writer fence.
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

/// Restores the same jobs component through its existing fresh database and artifact owners.
#[allow(
    clippy::too_many_arguments,
    reason = "fresh database and artifact authority plus original snapshot and limits remain explicit"
)]
pub(super) async fn restore_jobs_component_fresh(
    reader: &mut (dyn Read + Send),
    location: JobDatabaseLocation,
    paths: &LocalPaths,
    input_limits: GovernedBacktestInputAuthorityLimits,
    terminal_limits: GovernedBacktestRepositoryLimits,
    config: JobRepositoryConfig,
    artifacts: &dyn ArtifactRepository,
    snapshot: ProductBackupSnapshot,
    maximum_buffered_bytes: NonZeroUsize,
    cancellation: &CancellationToken,
) -> Result<(), ProductBackupError> {
    artifacts::restore_component(
        reader,
        location,
        paths,
        input_limits,
        terminal_limits,
        config,
        artifacts,
        snapshot,
        maximum_buffered_bytes,
        cancellation,
    )
    .await
}

impl RetainedJobsAndReceiptsWorkspaceSnapshot {
    async fn revalidate_indices(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<(), ProductBackupError> {
        self.backtest_inputs
            .revalidate_backup_index(self.input_revision, cancellation, index_deadline()?)
            .await
            .map_err(map_index_error)?;
        self.backtest_terminals
            .revalidate_backup_index(self.terminal_revision, cancellation, index_deadline()?)
            .await
            .map_err(map_index_error)
    }
}
fn index_deadline() -> Result<Instant, ProductBackupError> {
    Instant::now()
        .checked_add(Duration::from_secs(60))
        .ok_or(ProductBackupError::SnapshotMismatch)
}
fn map_index_error(error: market_squawk_services::ServiceError) -> ProductBackupError {
    if error == market_squawk_services::ServiceError::Cancelled {
        ProductBackupError::Cancelled
    } else {
        ProductBackupError::SnapshotMismatch
    }
}
