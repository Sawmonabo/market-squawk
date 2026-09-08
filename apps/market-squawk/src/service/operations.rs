//! Installed-service adapter for Operations reads, controls, and durable jobs.

use std::sync::Arc;

use market_squawk_services::{
    RequestContext, ServiceError, ToolResultMetadata, TypedToolRequest, TypedToolResult,
};

use crate::{
    application::{
        ApplicationDomainService,
        operations::{OperationsApplicationServices, PreparedOperationsJob},
    },
    jobs::{
        BackupJobRunner, InstalledJobAuthority, LifecycleJobRunnerError, RecoveryJobRunner,
        UpdateJobRunner,
    },
};

use super::jobs::InstalledJobOperations;

/// Sole installed transport adapter for the Operations domain.
pub(super) struct InstalledOperations {
    application: Arc<OperationsApplicationServices>,
    jobs: InstalledJobOperations,
    backup: Arc<BackupJobRunner>,
    recovery: Arc<RecoveryJobRunner>,
    update: Arc<UpdateJobRunner>,
}

impl InstalledOperations {
    pub(super) fn new(
        application: Arc<OperationsApplicationServices>,
        jobs: &InstalledJobAuthority,
        backup: Arc<BackupJobRunner>,
        recovery: Arc<RecoveryJobRunner>,
        update: Arc<UpdateJobRunner>,
    ) -> Self {
        Self {
            application,
            jobs: InstalledJobOperations::new(jobs),
            backup,
            recovery,
            update,
        }
    }

    pub(super) fn owns(operation: &str) -> bool {
        OperationsApplicationServices::owns_operation(operation)
    }

    pub(super) async fn call(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        if !Self::owns(request.name()) {
            return Err(ServiceError::NotFound);
        }
        if !OperationsApplicationServices::is_start_operation(request.name()) {
            return self
                .application
                .call(request.clone(), context.clone())
                .await;
        }
        let captured_at =
            super::runtime::current_timestamp().map_err(|_| ServiceError::Unavailable)?;
        match self.application.prepare_job(request, context).await? {
            PreparedOperationsJob::Backup { command, operation } => {
                let admission = match self.backup.admit(command, captured_at) {
                    Ok(admission) => admission,
                    Err(error) => {
                        self.application.revoke_backup_operation(&operation);
                        return Err(map_runner(error));
                    }
                };
                let retained = admission.clone();
                match self
                    .jobs
                    .start(
                        admission,
                        context,
                        ToolResultMetadata::complete_not_applicable(),
                    )
                    .await
                {
                    Ok(result) => Ok(result),
                    Err(error) => {
                        let _ignored = self.backup.revoke(&retained);
                        self.application.revoke_backup_operation(&operation);
                        Err(error)
                    }
                }
            }
            PreparedOperationsJob::Recovery { command, operation } => {
                let admission = match self.recovery.admit(command, captured_at) {
                    Ok(admission) => admission,
                    Err(error) => {
                        self.application.revoke_recovery_operation(&operation);
                        return Err(map_runner(error));
                    }
                };
                let retained = admission.clone();
                match self
                    .jobs
                    .start(
                        admission,
                        context,
                        ToolResultMetadata::complete_not_applicable(),
                    )
                    .await
                {
                    Ok(result) => Ok(result),
                    Err(error) => {
                        let _ignored = self.recovery.revoke(&retained);
                        self.application.revoke_recovery_operation(&operation);
                        Err(error)
                    }
                }
            }
            PreparedOperationsJob::Update { command, operation } => {
                let admission = match self.update.admit(command, captured_at) {
                    Ok(admission) => admission,
                    Err(error) => {
                        self.application.revoke_update_operation(&operation);
                        return Err(map_runner(error));
                    }
                };
                let retained = admission.clone();
                match self
                    .jobs
                    .start(
                        admission,
                        context,
                        ToolResultMetadata::complete_not_applicable(),
                    )
                    .await
                {
                    Ok(result) => Ok(result),
                    Err(error) => {
                        let _ignored = self.update.revoke(&retained);
                        self.application.revoke_update_operation(&operation);
                        Err(error)
                    }
                }
            }
        }
    }
}

impl std::fmt::Debug for InstalledOperations {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("InstalledOperations([SEALED OPERATIONS AND JOB AUTHORITY])")
    }
}

fn map_runner(error: LifecycleJobRunnerError) -> ServiceError {
    match error {
        LifecycleJobRunnerError::InvalidAdmission
        | LifecycleJobRunnerError::InvalidResult
        | LifecycleJobRunnerError::Conflict => ServiceError::InvalidRequest,
        LifecycleJobRunnerError::Capacity => ServiceError::ResourceExhausted,
        LifecycleJobRunnerError::InvalidConfiguration
        | LifecycleJobRunnerError::InvalidLimits
        | LifecycleJobRunnerError::Unavailable => ServiceError::Unavailable,
    }
}
