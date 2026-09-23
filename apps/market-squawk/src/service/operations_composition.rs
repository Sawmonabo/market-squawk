//! Type-state composition for the installed Operations application authority.

use std::{
    fmt,
    sync::{Arc, OnceLock},
    time::Instant,
};

use async_trait::async_trait;
use market_squawk_domain::SourceIdentifier;
use market_squawk_runtime::WorkspaceId;
use market_squawk_services::ServiceError;
use tokio_util::sync::CancellationToken;

use crate::{
    application::{
        backup::{BackupRetentionApproval, ProductBackupInventory, ProductBackupManifest},
        lifecycle::{
            ProgramGeneration, TrustedUpdateAuthority, UpdateApproval, WorkspaceActivitySnapshot,
            WorkspaceLifecycleAuthority, WorkspaceRuntimeIdentity, WorkspaceSwitchApproval,
        },
        logs::{DiagnosticArtifactPublisher, StructuredLogStore},
        operations::{
            ManagedBackupOperations, ManagedRecoveryOperations, ManagedSettingsOperations,
            ManagedUpdateOperations, OperationsApplicationServices, PreparedOperation,
            ProgramRollbackPreviewEvidence, RestorePreviewEvidence, TrustedStagedUpdate,
            UpdateStatusEvidence,
        },
        settings::DurableSettingsStore,
        setup::SetupPlanAuthority,
        workspace::DurableWorkspaceRegistry,
    },
    jobs::{
        BackupJobCommand, LifecycleJobExecutionError, LifecycleJobPublication,
        LifecycleJobPublicationError, RecoveryJobCommand, UpdateJobCommand,
    },
    service::operations_activity::RuntimeActivityCoordinator,
};

const UNBOUND_DIAGNOSTIC: &str = "operations-authority-not-bound";

/// Authorities that do not depend on the installed job repository or lifecycle runners.
pub(super) struct OperationsApplicationDependencies {
    pub(super) backups: Arc<ProductBackupInventory>,
    pub(super) workspaces: Arc<DurableWorkspaceRegistry>,
    pub(super) workspace_lifecycle: Arc<WorkspaceLifecycleAuthority>,
    pub(super) activity: Arc<RuntimeActivityCoordinator>,
    pub(super) updates: Arc<TrustedUpdateAuthority>,
    pub(super) logs: Arc<StructuredLogStore>,
    pub(super) log_artifacts: Arc<dyn DiagnosticArtifactPublisher>,
    pub(super) settings: Arc<DurableSettingsStore>,
    pub(super) settings_operations: Arc<dyn ManagedSettingsOperations>,
    pub(super) setup: Arc<SetupPlanAuthority>,
}

/// Exact installed authorities atomically published to the pre-composed application service.
struct BoundOperationsAuthorities {
    backup: Arc<dyn ManagedBackupOperations>,
    recovery: Arc<dyn ManagedRecoveryOperations>,
    update: Arc<dyn ManagedUpdateOperations>,
}

/// One fail-closed forwarding capability shared by all three lifecycle operation domains.
struct DeferredOperationsAuthorities {
    bound: OnceLock<BoundOperationsAuthorities>,
}

impl DeferredOperationsAuthorities {
    const fn new() -> Self {
        Self {
            bound: OnceLock::new(),
        }
    }

    fn bind(
        &self,
        backup: Arc<dyn ManagedBackupOperations>,
        recovery: Arc<dyn ManagedRecoveryOperations>,
        update: Arc<dyn ManagedUpdateOperations>,
    ) -> Result<(), ServiceError> {
        self.bound
            .set(BoundOperationsAuthorities {
                backup,
                recovery,
                update,
            })
            .map_err(|_already_bound| ServiceError::Unavailable)
    }

    fn bound(&self) -> Result<&BoundOperationsAuthorities, ServiceError> {
        self.bound.get().ok_or(ServiceError::Unavailable)
    }
}

impl fmt::Debug for DeferredOperationsAuthorities {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeferredOperationsAuthorities")
            .field("bound", &self.bound.get().is_some())
            .finish()
    }
}

/// Pre-runner Operations composition that cannot be used as transport publication authority.
pub(super) struct PendingOperationsComposition {
    application: Arc<OperationsApplicationServices>,
    authorities: Arc<DeferredOperationsAuthorities>,
}

impl PendingOperationsComposition {
    /// Builds the complete application service over one fail-closed atomic forwarding capability.
    pub(super) fn new(dependencies: OperationsApplicationDependencies) -> Self {
        let authorities = Arc::new(DeferredOperationsAuthorities::new());
        let backup_operations: Arc<dyn ManagedBackupOperations> = authorities.clone();
        let recovery_operations: Arc<dyn ManagedRecoveryOperations> = authorities.clone();
        let update_operations: Arc<dyn ManagedUpdateOperations> = authorities.clone();
        let application = Arc::new(OperationsApplicationServices::new(
            dependencies.backups,
            backup_operations,
            dependencies.workspaces,
            dependencies.workspace_lifecycle,
            dependencies.activity,
            recovery_operations,
            dependencies.updates,
            update_operations,
            dependencies.logs,
            dependencies.log_artifacts,
            dependencies.settings,
            dependencies.settings_operations,
            dependencies.setup,
        ));
        Self {
            application,
            authorities,
        }
    }

    /// Returns the sole Operations service only for constructing lifecycle job runners.
    pub(super) fn application_for_job_runners(&self) -> Arc<OperationsApplicationServices> {
        Arc::clone(&self.application)
    }

    /// Atomically binds all concrete authorities and advances the composition to publishable state.
    pub(super) fn bind(
        self,
        backup: Arc<dyn ManagedBackupOperations>,
        recovery: Arc<dyn ManagedRecoveryOperations>,
        update: Arc<dyn ManagedUpdateOperations>,
    ) -> Result<ReadyOperationsComposition, ServiceError> {
        self.authorities.bind(backup, recovery, update)?;
        Ok(ReadyOperationsComposition {
            application: self.application,
        })
    }
}

impl fmt::Debug for PendingOperationsComposition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingOperationsComposition")
            .field("authorities", &self.authorities)
            .finish_non_exhaustive()
    }
}

/// Fully bound Operations composition permitted to enter the installed transport graph.
pub(super) struct ReadyOperationsComposition {
    application: Arc<OperationsApplicationServices>,
}

impl ReadyOperationsComposition {
    /// Returns the exact service already retained by every lifecycle job runner.
    pub(super) fn application(&self) -> Arc<OperationsApplicationServices> {
        Arc::clone(&self.application)
    }
}

impl fmt::Debug for ReadyOperationsComposition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReadyOperationsComposition")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl ManagedBackupOperations for DeferredOperationsAuthorities {
    async fn prepare_create(
        &self,
        active: WorkspaceRuntimeIdentity,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<PreparedOperation, ServiceError> {
        self.bound()?
            .backup
            .prepare_create(active, cancellation, deadline)
            .await
    }

    async fn prepare_verify(
        &self,
        manifest: ProductBackupManifest,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<PreparedOperation, ServiceError> {
        self.bound()?
            .backup
            .prepare_verify(manifest, cancellation, deadline)
            .await
    }

    fn prepare_retention(
        &self,
        approval: BackupRetentionApproval,
    ) -> Result<PreparedOperation, ServiceError> {
        self.bound()?.backup.prepare_retention(approval)
    }

    fn revoke(&self, operation: &PreparedOperation) {
        if let Some(bound) = self.bound.get() {
            bound.backup.revoke(operation);
        }
    }

    async fn execute(
        &self,
        command: BackupJobCommand,
        cancellation: CancellationToken,
        deadline: Instant,
        publication: Arc<dyn LifecycleJobPublication>,
    ) -> Result<(), LifecycleJobExecutionError> {
        let bound = self.bound.get().ok_or_else(unbound_execution_error)?;
        bound
            .backup
            .execute(command, cancellation, deadline, publication)
            .await
    }
}

#[async_trait]
impl ManagedRecoveryOperations for DeferredOperationsAuthorities {
    fn workspace_activity(
        &self,
        target: WorkspaceId,
    ) -> Result<WorkspaceActivitySnapshot, ServiceError> {
        self.bound()?.recovery.workspace_activity(target)
    }

    async fn preview_restore(
        &self,
        manifest: ProductBackupManifest,
        active: WorkspaceRuntimeIdentity,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<RestorePreviewEvidence, ServiceError> {
        self.bound()?
            .recovery
            .preview_restore(manifest, active, cancellation, deadline)
            .await
    }

    fn prepare_restore(
        &self,
        evidence: RestorePreviewEvidence,
    ) -> Result<PreparedOperation, ServiceError> {
        self.bound()?.recovery.prepare_restore(evidence)
    }

    fn prepare_workspace_switch(
        &self,
        approval: WorkspaceSwitchApproval,
    ) -> Result<PreparedOperation, ServiceError> {
        self.bound()?.recovery.prepare_workspace_switch(approval)
    }

    fn preview_program_rollback(
        &self,
        current: ProgramGeneration,
    ) -> Result<ProgramRollbackPreviewEvidence, ServiceError> {
        self.bound()?.recovery.preview_program_rollback(current)
    }

    fn prepare_program_rollback(
        &self,
        evidence: ProgramRollbackPreviewEvidence,
    ) -> Result<PreparedOperation, ServiceError> {
        self.bound()?.recovery.prepare_program_rollback(evidence)
    }

    fn revoke(&self, operation: &PreparedOperation) {
        if let Some(bound) = self.bound.get() {
            bound.recovery.revoke(operation);
        }
    }

    async fn execute(
        &self,
        command: RecoveryJobCommand,
        cancellation: CancellationToken,
        deadline: Instant,
        publication: Arc<dyn LifecycleJobPublication>,
    ) -> Result<(), LifecycleJobExecutionError> {
        let bound = self.bound.get().ok_or_else(unbound_execution_error)?;
        bound
            .recovery
            .execute(command, cancellation, deadline, publication)
            .await
    }
}

#[async_trait]
impl ManagedUpdateOperations for DeferredOperationsAuthorities {
    fn status(&self, current: ProgramGeneration) -> Result<UpdateStatusEvidence, ServiceError> {
        self.bound()?.update.status(current)
    }

    async fn check_and_stage(
        &self,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<TrustedStagedUpdate, ServiceError> {
        self.bound()?
            .update
            .check_and_stage(cancellation, deadline)
            .await
    }

    fn current_staged(&self) -> Result<TrustedStagedUpdate, ServiceError> {
        self.bound()?.update.current_staged()
    }

    fn prepare_update(&self, approval: UpdateApproval) -> Result<PreparedOperation, ServiceError> {
        self.bound()?.update.prepare_update(approval)
    }

    fn revoke(&self, operation: &PreparedOperation) {
        if let Some(bound) = self.bound.get() {
            bound.update.revoke(operation);
        }
    }

    async fn execute(
        &self,
        command: UpdateJobCommand,
        cancellation: CancellationToken,
        deadline: Instant,
        publication: Arc<dyn LifecycleJobPublication>,
    ) -> Result<(), LifecycleJobExecutionError> {
        let bound = self.bound.get().ok_or_else(unbound_execution_error)?;
        bound
            .update
            .execute(command, cancellation, deadline, publication)
            .await
    }
}

fn unbound_execution_error() -> LifecycleJobExecutionError {
    SourceIdentifier::try_from(UNBOUND_DIAGNOSTIC).map_or_else(
        |_error| LifecycleJobExecutionError::Publication(LifecycleJobPublicationError::Revoked),
        |diagnostic| LifecycleJobExecutionError::failed(diagnostic, false),
    )
}
