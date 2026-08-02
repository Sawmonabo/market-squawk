//! Transport-neutral operational authority for backups, lifecycle, logs, and typed settings.

mod contracts;
mod previews;
mod requests;

pub use contracts::{
    ManagedBackupOperations, ManagedRecoveryOperations, ManagedSettingsOperations,
    ManagedSettingsRollbackApproval, ManagedSettingsRollbackPreview, ManagedUpdateOperations,
    PreparedOperation, ProgramRollbackPreviewEvidence, RestorePreviewEvidence, TrustedStagedUpdate,
    UpdateStatusEvidence,
};

use std::{fmt, sync::Arc, time::Instant};

use async_trait::async_trait;
use market_squawk_services::{
    RequestContext, ServiceDomain, ServiceError, ServiceLimits, ToolResultMetadata,
    TypedToolRequest, TypedToolResult,
};
use serde_json::{Map, Value};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    ApplicationDomainService,
    backup::ProductBackupInventory,
    domain_support::{DomainLifecycle, admitted_result_limits, ensure_request_live},
    lifecycle::{TrustedUpdateAuthority, WorkspaceLifecycleAuthority, WorkspaceRuntimeIdentity},
    logs::{DiagnosticArtifactPublisher, StructuredLogStore},
    settings::DurableSettingsStore,
    workspace::DurableWorkspaceRegistry,
};
use crate::jobs::{
    BackupJobAction, BackupJobCommand, LifecycleJobAuthority, LifecycleJobExecutionError,
    LifecycleJobPublication, RecoveryJobAction, RecoveryJobCommand, UpdateJobCommand,
};

const LIST_BACKUPS: &str = "Operations.ListBackups";
const GET_BACKUP: &str = "Operations.GetBackup";
const PREVIEW_BACKUP_RETENTION: &str = "Operations.PreviewBackupRetention";
const PREVIEW_RESTORE: &str = "Operations.PreviewRestore";
const LIST_WORKSPACES: &str = "Operations.ListWorkspaces";
const PREVIEW_WORKSPACE_SWITCH: &str = "Operations.PreviewWorkspaceSwitch";
const GET_UPDATE_STATUS: &str = "Operations.GetUpdateStatus";
const CHECK_FOR_UPDATES: &str = "Operations.CheckForUpdates";
const PREVIEW_UPDATE: &str = "Operations.PreviewUpdate";
const PREVIEW_PROGRAM_ROLLBACK: &str = "Operations.PreviewProgramRollback";
const QUERY_LOGS: &str = "Operations.QueryLogs";
const EXPORT_LOGS: &str = "Operations.ExportLogs";
const GET_SETTINGS: &str = "Operations.GetSettings";
const PREVIEW_SETTINGS_CHANGE: &str = "Operations.PreviewSettingsChange";
const APPLY_SETTINGS_CHANGE: &str = "Operations.ApplySettingsChange";
const PREVIEW_SETTINGS_ROLLBACK: &str = "Operations.PreviewSettingsRollback";
const ROLLBACK_SETTINGS: &str = "Operations.RollbackSettings";

pub(crate) const START_BACKUP: &str = "Operations.StartBackup";
pub(crate) const START_BACKUP_VERIFICATION: &str = "Operations.StartBackupVerification";
pub(crate) const START_BACKUP_RETENTION: &str = "Operations.StartBackupRetention";
pub(crate) const START_RESTORE: &str = "Operations.StartRestore";
pub(crate) const START_WORKSPACE_SWITCH: &str = "Operations.StartWorkspaceSwitch";
pub(crate) const START_UPDATE: &str = "Operations.StartUpdate";
pub(crate) const START_PROGRAM_ROLLBACK: &str = "Operations.StartProgramRollback";

use previews::{PreviewPayload, PreviewRegistry, project_digest_fields};
use requests::{
    BackupIdentityInput, BackupListInput, LogQueryInput, PreviewReferenceInput, RetentionInput,
    SettingsChangeInput, SettingsRollbackInput, WorkspaceListInput, WorkspaceTargetInput, decode,
    decode_mutation, map_backup_error, map_lifecycle_error, map_update_error, parse_sha256,
    parse_workspace, require_confirmation, result_item_count,
};

/// Exact job command prepared by the Operations application authority.
#[derive(Debug)]
pub(crate) enum PreparedOperationsJob {
    Backup {
        command: BackupJobCommand,
        operation: PreparedOperation,
    },
    Recovery {
        command: RecoveryJobCommand,
        operation: PreparedOperation,
    },
    Update {
        command: UpdateJobCommand,
        operation: PreparedOperation,
    },
}

/// Sole Operations-domain service shared by desktop, CLI, and MCP transports.
pub struct OperationsApplicationServices {
    lifecycle: Arc<DomainLifecycle>,
    backups: Arc<ProductBackupInventory>,
    backup_operations: Arc<dyn ManagedBackupOperations>,
    workspaces: Arc<DurableWorkspaceRegistry>,
    workspace_lifecycle: Arc<WorkspaceLifecycleAuthority>,
    recovery_operations: Arc<dyn ManagedRecoveryOperations>,
    updates: Arc<TrustedUpdateAuthority>,
    update_operations: Arc<dyn ManagedUpdateOperations>,
    logs: Arc<StructuredLogStore>,
    log_artifacts: Arc<dyn DiagnosticArtifactPublisher>,
    settings: Arc<DurableSettingsStore>,
    settings_operations: Arc<dyn ManagedSettingsOperations>,
    previews: PreviewRegistry,
}

impl OperationsApplicationServices {
    /// Requires every concrete operational authority before the domain can be published.
    #[allow(
        clippy::too_many_arguments,
        reason = "all operational authorities are explicit"
    )]
    pub fn new(
        backups: Arc<ProductBackupInventory>,
        backup_operations: Arc<dyn ManagedBackupOperations>,
        workspaces: Arc<DurableWorkspaceRegistry>,
        workspace_lifecycle: Arc<WorkspaceLifecycleAuthority>,
        recovery_operations: Arc<dyn ManagedRecoveryOperations>,
        updates: Arc<TrustedUpdateAuthority>,
        update_operations: Arc<dyn ManagedUpdateOperations>,
        logs: Arc<StructuredLogStore>,
        log_artifacts: Arc<dyn DiagnosticArtifactPublisher>,
        settings: Arc<DurableSettingsStore>,
        settings_operations: Arc<dyn ManagedSettingsOperations>,
    ) -> Self {
        Self {
            lifecycle: DomainLifecycle::new(),
            backups,
            backup_operations,
            workspaces,
            workspace_lifecycle,
            recovery_operations,
            updates,
            update_operations,
            logs,
            log_artifacts,
            settings,
            settings_operations,
            previews: PreviewRegistry::default(),
        }
    }

    /// Returns whether the installed adapter must intercept this job-backed operation.
    pub(crate) fn is_start_operation(operation: &str) -> bool {
        matches!(
            operation,
            START_BACKUP
                | START_BACKUP_VERIFICATION
                | START_BACKUP_RETENTION
                | START_RESTORE
                | START_WORKSPACE_SWITCH
                | START_UPDATE
                | START_PROGRAM_ROLLBACK
        )
    }

    /// Returns whether this closed operation belongs to the Operations domain.
    pub(crate) fn owns_operation(operation: &str) -> bool {
        Self::is_start_operation(operation)
            || matches!(
                operation,
                LIST_BACKUPS
                    | GET_BACKUP
                    | PREVIEW_BACKUP_RETENTION
                    | PREVIEW_RESTORE
                    | LIST_WORKSPACES
                    | PREVIEW_WORKSPACE_SWITCH
                    | GET_UPDATE_STATUS
                    | CHECK_FOR_UPDATES
                    | PREVIEW_UPDATE
                    | PREVIEW_PROGRAM_ROLLBACK
                    | QUERY_LOGS
                    | EXPORT_LOGS
                    | GET_SETTINGS
                    | PREVIEW_SETTINGS_CHANGE
                    | APPLY_SETTINGS_CHANGE
                    | PREVIEW_SETTINGS_ROLLBACK
                    | ROLLBACK_SETTINGS
            )
    }

    /// Consumes exact server-held authority and returns only a code-owned job command.
    pub(crate) async fn prepare_job(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<PreparedOperationsJob, ServiceError> {
        let _call = DomainLifecycle::enter(&self.lifecycle, context)?;
        require_confirmation(request.arguments())?;
        let active = self.current_for_origin(context)?;
        let prepared = match request.name() {
            START_BACKUP => {
                let operation = self
                    .backup_operations
                    .prepare_create(active, context.cancellation().clone(), context.deadline())
                    .await?;
                let command = BackupJobCommand::new(
                    BackupJobAction::Create,
                    operation.identity().clone(),
                    operation.evidence_digest(),
                );
                PreparedOperationsJob::Backup { command, operation }
            }
            START_BACKUP_VERIFICATION => {
                let input: BackupIdentityInput = decode_mutation(request.arguments())?;
                let manifest = self.backups.get(parse_sha256(&input.backup_id)?)?;
                let operation = self
                    .backup_operations
                    .prepare_verify(manifest, context.cancellation().clone(), context.deadline())
                    .await?;
                let command = BackupJobCommand::new(
                    BackupJobAction::Verify,
                    operation.identity().clone(),
                    operation.evidence_digest(),
                );
                PreparedOperationsJob::Backup { command, operation }
            }
            START_BACKUP_RETENTION => {
                let (payload, _) = self.consume_preview(request.arguments(), context)?;
                let PreviewPayload::BackupRetention(preview) = payload else {
                    return Err(ServiceError::InvalidRequest);
                };
                let operation = self
                    .backup_operations
                    .prepare_retention(preview.try_approve().map_err(map_backup_error)?)?;
                let command = BackupJobCommand::new(
                    BackupJobAction::EnforceRetention,
                    operation.identity().clone(),
                    operation.evidence_digest(),
                );
                PreparedOperationsJob::Backup { command, operation }
            }
            START_RESTORE => {
                let (payload, _) = self.consume_preview(request.arguments(), context)?;
                let PreviewPayload::Restore(preview) = payload else {
                    return Err(ServiceError::InvalidRequest);
                };
                if !preview.can_approve() {
                    return Err(ServiceError::InvalidRequest);
                }
                let operation = self.recovery_operations.prepare_restore(preview)?;
                let command = RecoveryJobCommand::new(
                    RecoveryJobAction::RestoreWorkspace,
                    operation.identity().clone(),
                    operation.evidence_digest(),
                );
                PreparedOperationsJob::Recovery { command, operation }
            }
            START_WORKSPACE_SWITCH => {
                let (payload, _) = self.consume_preview(request.arguments(), context)?;
                let PreviewPayload::Workspace(preview) = payload else {
                    return Err(ServiceError::InvalidRequest);
                };
                let operation = self.recovery_operations.prepare_workspace_switch(
                    preview.try_approve().map_err(map_lifecycle_error)?,
                )?;
                let command = RecoveryJobCommand::new(
                    RecoveryJobAction::SwitchWorkspace,
                    operation.identity().clone(),
                    operation.evidence_digest(),
                );
                PreparedOperationsJob::Recovery { command, operation }
            }
            START_UPDATE => {
                let (payload, _) = self.consume_preview(request.arguments(), context)?;
                let PreviewPayload::Update(preview) = payload else {
                    return Err(ServiceError::InvalidRequest);
                };
                let operation = self
                    .update_operations
                    .prepare_update(preview.try_approve().map_err(map_update_error)?)?;
                let command = UpdateJobCommand::new(
                    operation.identity().clone(),
                    operation.evidence_digest(),
                );
                PreparedOperationsJob::Update { command, operation }
            }
            START_PROGRAM_ROLLBACK => {
                let (payload, _) = self.consume_preview(request.arguments(), context)?;
                let PreviewPayload::ProgramRollback(preview) = payload else {
                    return Err(ServiceError::InvalidRequest);
                };
                if !preview.can_approve() {
                    return Err(ServiceError::InvalidRequest);
                }
                let operation = self.recovery_operations.prepare_program_rollback(preview)?;
                let command = RecoveryJobCommand::new(
                    RecoveryJobAction::RollbackProgram,
                    operation.identity().clone(),
                    operation.evidence_digest(),
                );
                PreparedOperationsJob::Recovery { command, operation }
            }
            _ => return Err(ServiceError::NotFound),
        };
        ensure_request_live(context, &self.lifecycle)?;
        Ok(prepared)
    }

    pub(crate) fn revoke_backup_operation(&self, operation: &PreparedOperation) {
        self.backup_operations.revoke(operation);
    }

    pub(crate) fn revoke_recovery_operation(&self, operation: &PreparedOperation) {
        self.recovery_operations.revoke(operation);
    }

    pub(crate) fn revoke_update_operation(&self, operation: &PreparedOperation) {
        self.update_operations.revoke(operation);
    }

    fn current_for_origin(
        &self,
        context: &RequestContext,
    ) -> Result<WorkspaceRuntimeIdentity, ServiceError> {
        let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
        let current = self
            .workspace_lifecycle
            .current()
            .map_err(map_lifecycle_error)?;
        if current.workspace_id().as_uuid() != origin.workspace_id() {
            return Err(ServiceError::Unauthorized);
        }
        Ok(current)
    }

    fn insert_preview(
        &self,
        context: &RequestContext,
        kind: &'static str,
        evidence: &impl serde::Serialize,
        payload: PreviewPayload,
    ) -> Result<Value, ServiceError> {
        self.previews.insert(context, kind, evidence, payload)
    }

    fn consume_preview(
        &self,
        arguments: &Map<String, Value>,
        context: &RequestContext,
    ) -> Result<(PreviewPayload, [u8; 32]), ServiceError> {
        let input: PreviewReferenceInput = decode_mutation(arguments)?;
        let preview_id =
            Uuid::parse_str(&input.preview_id).map_err(|_| ServiceError::InvalidRequest)?;
        let expected_digest = parse_sha256(&input.preview_digest)?;
        self.previews.consume(context, preview_id, expected_digest)
    }

    fn result(
        &self,
        value: Value,
        item_count: usize,
        limits: ServiceLimits,
    ) -> Result<TypedToolResult, ServiceError> {
        TypedToolResult::try_new(
            value,
            item_count.max(1),
            ToolResultMetadata::complete_not_applicable(),
            limits,
        )
        .map_err(Into::into)
    }
}

#[async_trait]
impl ApplicationDomainService for OperationsApplicationServices {
    fn domain(&self) -> ServiceDomain {
        ServiceDomain::Operations
    }

    async fn call(
        &self,
        request: TypedToolRequest,
        context: RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let _call = DomainLifecycle::enter(&self.lifecycle, &context)?;
        let active = self.current_for_origin(&context)?;
        let result_limits = if request.arguments().contains_key("resultLimits") {
            admitted_result_limits(&request, &context)?
        } else {
            context.limits()
        };
        let value = match request.name() {
            LIST_BACKUPS => {
                let input: BackupListInput = decode(request.arguments())?;
                serde_json::to_value(
                    self.backups.list(
                        input
                            .after_backup_id
                            .as_deref()
                            .map(parse_sha256)
                            .transpose()?,
                        input.limit,
                    )?,
                )
                .map_err(|_| ServiceError::Internal)?
            }
            GET_BACKUP => {
                let input: BackupIdentityInput = decode(request.arguments())?;
                serde_json::to_value(self.backups.get(parse_sha256(&input.backup_id)?)?)
                    .map_err(|_| ServiceError::Internal)?
            }
            PREVIEW_BACKUP_RETENTION => {
                let input: RetentionInput = decode(request.arguments())?;
                let preview = self.backups.preview_retention(input.keep_latest)?;
                self.insert_preview(
                    &context,
                    "backup-retention",
                    &preview,
                    PreviewPayload::BackupRetention(preview.clone()),
                )?
            }
            PREVIEW_RESTORE => {
                let input: BackupIdentityInput = decode(request.arguments())?;
                let manifest = self.backups.get(parse_sha256(&input.backup_id)?)?;
                let preview = self
                    .recovery_operations
                    .preview_restore(
                        manifest,
                        active,
                        context.cancellation().clone(),
                        context.deadline(),
                    )
                    .await?;
                self.insert_preview(
                    &context,
                    "restore",
                    &preview,
                    PreviewPayload::Restore(preview.clone()),
                )?
            }
            LIST_WORKSPACES => {
                let input: WorkspaceListInput = decode(request.arguments())?;
                serde_json::to_value(self.workspaces.list(
                    input.after_workspace_id.map(parse_workspace).transpose()?,
                    input.limit,
                )?)
                .map_err(|_| ServiceError::Internal)?
            }
            PREVIEW_WORKSPACE_SWITCH => {
                let input: WorkspaceTargetInput = decode(request.arguments())?;
                let target = parse_workspace(input.workspace_id)?;
                let activity = self.recovery_operations.workspace_activity(target)?;
                let preview = self
                    .workspace_lifecycle
                    .preview_switch(target, activity)
                    .map_err(map_lifecycle_error)?;
                self.insert_preview(
                    &context,
                    "workspace-switch",
                    &preview,
                    PreviewPayload::Workspace(preview.clone()),
                )?
            }
            GET_UPDATE_STATUS => serde_json::to_value(
                self.update_operations
                    .status(self.updates.current().map_err(map_update_error)?)?,
            )
            .map_err(|_| ServiceError::Internal)?,
            CHECK_FOR_UPDATES => {
                require_confirmation(request.arguments())?;
                let staged = self
                    .update_operations
                    .check_and_stage(context.cancellation().clone(), context.deadline())
                    .await?;
                let preview = self
                    .updates
                    .preview(staged.candidate, staged.activity)
                    .map_err(map_update_error)?;
                self.insert_preview(
                    &context,
                    "trusted-update",
                    &preview,
                    PreviewPayload::Update(preview.clone()),
                )?
            }
            PREVIEW_UPDATE => {
                let staged = self.update_operations.current_staged()?;
                let preview = self
                    .updates
                    .preview(staged.candidate, staged.activity)
                    .map_err(map_update_error)?;
                self.insert_preview(
                    &context,
                    "trusted-update",
                    &preview,
                    PreviewPayload::Update(preview.clone()),
                )?
            }
            PREVIEW_PROGRAM_ROLLBACK => {
                let preview = self
                    .recovery_operations
                    .preview_program_rollback(self.updates.current().map_err(map_update_error)?)?;
                self.insert_preview(
                    &context,
                    "program-rollback",
                    &preview,
                    PreviewPayload::ProgramRollback(preview.clone()),
                )?
            }
            QUERY_LOGS => {
                let input: LogQueryInput = decode(request.arguments())?;
                serde_json::to_value(self.logs.query(&input.into_query())?)
                    .map_err(|_| ServiceError::Internal)?
            }
            EXPORT_LOGS => {
                require_confirmation(request.arguments())?;
                let input: LogQueryInput = decode_mutation(request.arguments())?;
                serde_json::to_value(
                    self.logs
                        .export(
                            input.into_query(),
                            self.log_artifacts.as_ref(),
                            context.cancellation().clone(),
                            context.deadline(),
                        )
                        .await?,
                )
                .map_err(|_| ServiceError::Internal)?
            }
            GET_SETTINGS => serde_json::to_value(self.settings.snapshot()?)
                .map_err(|_| ServiceError::Internal)?,
            PREVIEW_SETTINGS_CHANGE => {
                let input: SettingsChangeInput = decode(request.arguments())?;
                let preview = self
                    .settings
                    .preview(input.expected_revision, input.changes)?;
                self.insert_preview(
                    &context,
                    "settings-change",
                    &preview,
                    PreviewPayload::SettingsChange(preview.clone()),
                )?
            }
            APPLY_SETTINGS_CHANGE => {
                require_confirmation(request.arguments())?;
                let (payload, _) = self.consume_preview(request.arguments(), &context)?;
                let PreviewPayload::SettingsChange(preview) = payload else {
                    return Err(ServiceError::InvalidRequest);
                };
                serde_json::to_value(self.settings_operations.apply_change(preview.approve())?)
                    .map_err(|_| ServiceError::Internal)?
            }
            PREVIEW_SETTINGS_ROLLBACK => {
                let input: SettingsRollbackInput = decode(request.arguments())?;
                let preview = self
                    .settings_operations
                    .preview_rollback(input.expected_revision, input.target_revision)?;
                self.insert_preview(
                    &context,
                    "settings-rollback",
                    &preview,
                    PreviewPayload::SettingsRollback(preview.clone()),
                )?
            }
            ROLLBACK_SETTINGS => {
                require_confirmation(request.arguments())?;
                let (payload, _) = self.consume_preview(request.arguments(), &context)?;
                let PreviewPayload::SettingsRollback(preview) = payload else {
                    return Err(ServiceError::InvalidRequest);
                };
                serde_json::to_value(self.settings_operations.apply_rollback(preview.approve())?)
                    .map_err(|_| ServiceError::Internal)?
            }
            _ if Self::is_start_operation(request.name()) => return Err(ServiceError::NotFound),
            _ => return Err(ServiceError::NotFound),
        };
        let value = project_digest_fields(value)?;
        ensure_request_live(&context, &self.lifecycle)?;
        let item_count = result_item_count(&value);
        self.result(value, item_count, result_limits)
    }

    fn begin_shutdown(&self) {
        self.lifecycle.begin_shutdown();
    }

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        self.lifecycle.finish_shutdown(deadline).await
    }
}

#[async_trait]
impl LifecycleJobAuthority<BackupJobCommand> for OperationsApplicationServices {
    async fn execute(
        &self,
        command: BackupJobCommand,
        cancellation: CancellationToken,
        deadline: Instant,
        publication: Arc<dyn LifecycleJobPublication>,
    ) -> Result<(), LifecycleJobExecutionError> {
        self.backup_operations
            .execute(command, cancellation, deadline, publication)
            .await
    }
}

#[async_trait]
impl LifecycleJobAuthority<RecoveryJobCommand> for OperationsApplicationServices {
    async fn execute(
        &self,
        command: RecoveryJobCommand,
        cancellation: CancellationToken,
        deadline: Instant,
        publication: Arc<dyn LifecycleJobPublication>,
    ) -> Result<(), LifecycleJobExecutionError> {
        self.recovery_operations
            .execute(command, cancellation, deadline, publication)
            .await
    }
}

#[async_trait]
impl LifecycleJobAuthority<UpdateJobCommand> for OperationsApplicationServices {
    async fn execute(
        &self,
        command: UpdateJobCommand,
        cancellation: CancellationToken,
        deadline: Instant,
        publication: Arc<dyn LifecycleJobPublication>,
    ) -> Result<(), LifecycleJobExecutionError> {
        self.update_operations
            .execute(command, cancellation, deadline, publication)
            .await
    }
}

impl fmt::Debug for OperationsApplicationServices {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OperationsApplicationServices([SEALED OPERATIONAL AUTHORITIES])")
    }
}

impl From<super::backup::ProductBackupError> for ServiceError {
    fn from(error: super::backup::ProductBackupError) -> Self {
        use super::backup::ProductBackupError;
        match error {
            ProductBackupError::BackupNotFound => ServiceError::NotFound,
            ProductBackupError::InvalidInventoryLimit
            | ProductBackupError::InvalidInventoryCursor
            | ProductBackupError::InvalidRetentionPolicy
            | ProductBackupError::RetentionEmpty
            | ProductBackupError::StaleRetentionApproval => ServiceError::InvalidRequest,
            ProductBackupError::InventoryCapacity => ServiceError::ResourceExhausted,
            _ => ServiceError::Unavailable,
        }
    }
}

impl From<super::workspace::WorkspaceRegistryError> for ServiceError {
    fn from(error: super::workspace::WorkspaceRegistryError) -> Self {
        use super::workspace::WorkspaceRegistryError;
        match error {
            WorkspaceRegistryError::InvalidLimit | WorkspaceRegistryError::InvalidDescriptor => {
                ServiceError::InvalidRequest
            }
            WorkspaceRegistryError::CapacityOrConflict => ServiceError::ResourceExhausted,
            WorkspaceRegistryError::CorruptState
            | WorkspaceRegistryError::Unavailable
            | WorkspaceRegistryError::Persistence(_) => ServiceError::Unavailable,
        }
    }
}

impl From<super::logs::StructuredLogError> for ServiceError {
    fn from(error: super::logs::StructuredLogError) -> Self {
        use super::logs::StructuredLogError;
        match error {
            StructuredLogError::InvalidQuery => ServiceError::InvalidRequest,
            StructuredLogError::ExportTooLarge
            | StructuredLogError::CapacityExceeded
            | StructuredLogError::RecordTooLarge => ServiceError::ResourceExhausted,
            _ => ServiceError::Unavailable,
        }
    }
}

impl From<super::settings::SettingsError> for ServiceError {
    fn from(error: super::settings::SettingsError) -> Self {
        use super::settings::SettingsError;
        match error {
            SettingsError::InvalidValue { .. }
            | SettingsError::InvalidChangeSet
            | SettingsError::ImmutableOrDuplicateSetting { .. }
            | SettingsError::StaleRevision
            | SettingsError::StaleOrInvalidApproval
            | SettingsError::UnknownRollbackRevision => ServiceError::InvalidRequest,
            SettingsError::CapacityExceeded => ServiceError::ResourceExhausted,
            _ => ServiceError::Unavailable,
        }
    }
}
