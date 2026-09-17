//! Exact production authority bindings for Operations lifecycle preflight.

use std::sync::Arc;

use market_squawk_runtime::RuntimeClientActivityReader;
use market_squawk_services::ServiceError;

use super::{
    mcp_control::InstalledMcpControl,
    operations_activity::{
        ExecutionActivityFacts, JobActivityFacts, RuntimeActivityReaders, WorkspaceStorageFacts,
    },
    workspace_selector::WorkspaceSelector,
};
use crate::{
    application::{
        PaperRuntimeActivityAuthority,
        source::{SourceLifecycleAuthority, SourceLifecycleError},
        workspace::DurableWorkspaceRegistry,
    },
    jobs::InstalledJobAuthority,
};

/// Builds one structurally complete reader bundle from the exact installed authorities.
pub(super) fn build_runtime_activity_readers(
    jobs: &InstalledJobAuthority,
    sources: Arc<dyn SourceLifecycleAuthority>,
    execution: Arc<dyn PaperRuntimeActivityAuthority>,
    native_clients: RuntimeClientActivityReader,
    mcp_clients: Arc<InstalledMcpControl>,
    workspaces: Arc<DurableWorkspaceRegistry>,
    workspace_selector: Arc<WorkspaceSelector>,
) -> RuntimeActivityReaders {
    let job_authority = Arc::downgrade(&jobs.authority());
    let job_reader = Arc::new(move || {
        let job_authority = job_authority.upgrade().ok_or(ServiceError::Unavailable)?;
        let activity = job_authority.activity();
        let running_jobs =
            u32::try_from(activity.running()).map_err(|_| ServiceError::InvalidResult)?;
        let running_mutations =
            u32::try_from(activity.running_mutations()).map_err(|_| ServiceError::InvalidResult)?;
        Ok(JobActivityFacts::new(running_jobs, running_mutations))
    });

    let sources = Arc::downgrade(&sources);
    let source_reader = Arc::new(move || {
        let sources = sources.upgrade().ok_or(ServiceError::Unavailable)?;
        sources
            .active_source_count()
            .map_err(map_source_activity_error)
            .and_then(|count| u32::try_from(count).map_err(|_| ServiceError::InvalidResult))
    });

    let execution = Arc::downgrade(&execution);
    let execution_reader = Arc::new(move || {
        let execution = execution.upgrade().ok_or(ServiceError::Unavailable)?;
        let activity = execution.activity()?;
        Ok(ExecutionActivityFacts::new(
            activity.paper_execution_active(),
            activity.reconciliation_pending(),
        ))
    });

    let mcp_clients = Arc::downgrade(&mcp_clients);
    let client_reader = Arc::new(move || {
        let mcp_clients = mcp_clients.upgrade().ok_or(ServiceError::Unavailable)?;
        let native = native_clients.connected_clients();
        let mcp = mcp_clients
            .active_client_count()
            .map_err(|_error| ServiceError::Unavailable)?;
        native
            .checked_add(mcp)
            .ok_or(ServiceError::InvalidResult)
            .and_then(|count| u32::try_from(count).map_err(|_| ServiceError::InvalidResult))
    });

    let workspaces = Arc::downgrade(&workspaces);
    let workspace_selector = Arc::downgrade(&workspace_selector);
    let workspace_reader = Arc::new(move |workspace_id| {
        let workspaces = workspaces.upgrade().ok_or(ServiceError::Unavailable)?;
        let workspace_selector = workspace_selector
            .upgrade()
            .ok_or(ServiceError::Unavailable)?;
        let descriptor = workspaces
            .descriptor(workspace_id)
            .map_err(|_error| ServiceError::Unavailable)?
            .ok_or(ServiceError::Unavailable)?;
        let paths = workspace_selector
            .workspace_paths(workspace_id)
            .map_err(|_error| ServiceError::Unavailable)?;
        let available_disk_bytes =
            fs2::available_space(paths.root()).map_err(|_error| ServiceError::Unavailable)?;
        Ok(WorkspaceStorageFacts::new(
            descriptor.schema_version(),
            available_disk_bytes,
        ))
    });

    RuntimeActivityReaders::new(
        job_reader,
        source_reader,
        execution_reader,
        client_reader,
        workspace_reader,
    )
}

const fn map_source_activity_error(error: SourceLifecycleError) -> ServiceError {
    match error {
        SourceLifecycleError::InvalidResult => ServiceError::InvalidResult,
        SourceLifecycleError::InvalidRequest
        | SourceLifecycleError::NotFound
        | SourceLifecycleError::Conflict
        | SourceLifecycleError::Unauthorized
        | SourceLifecycleError::RateLimited
        | SourceLifecycleError::Cancelled
        | SourceLifecycleError::DeadlineExceeded
        | SourceLifecycleError::Unavailable
        | SourceLifecycleError::ReconciliationRequired
        | SourceLifecycleError::Internal => ServiceError::Unavailable,
    }
}
