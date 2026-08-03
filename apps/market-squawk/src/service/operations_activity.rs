//! Bind-once access to live Operations preflight facts.

use std::{
    fmt,
    sync::{Arc, OnceLock},
};

use market_squawk_runtime::WorkspaceId;
use market_squawk_services::ServiceError;
use thiserror::Error;

use crate::application::lifecycle::{UpdateActivitySnapshot, WorkspaceActivitySnapshot};
use crate::local_product::operations::RecoveryActivityAuthority;

/// Synchronous, path-free view of the installed scheduler's exact running set.
pub(crate) type JobActivityReader =
    dyn Fn() -> Result<JobActivityFacts, ServiceError> + Send + Sync + 'static;
/// Synchronous, path-free count of currently active source runtimes.
pub(crate) type SourceActivityReader =
    dyn Fn() -> Result<u32, ServiceError> + Send + Sync + 'static;
/// Synchronous, path-free view of paper and execution reconciliation state.
pub(crate) type ExecutionActivityReader =
    dyn Fn() -> Result<ExecutionActivityFacts, ServiceError> + Send + Sync + 'static;
/// Synchronous, path-free count of clients attached to the current service generation.
pub(crate) type ClientActivityReader =
    dyn Fn() -> Result<u32, ServiceError> + Send + Sync + 'static;
/// Synchronous schema and disk view for the requested workspace identity.
pub(crate) type WorkspaceActivityReader =
    dyn Fn(WorkspaceId) -> Result<WorkspaceStorageFacts, ServiceError> + Send + Sync + 'static;

/// Maximum values accepted from the live authorities sampled for Operations preflight.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeActivityLimits {
    maximum_running_jobs: u32,
    maximum_active_sources: u32,
    maximum_connected_clients: u32,
    maximum_available_disk_bytes: u64,
    maximum_required_disk_bytes: u64,
    maximum_schema_version: u32,
}

impl RuntimeActivityLimits {
    /// Creates finite limits that the composition root must derive from its owned authorities.
    pub(crate) fn try_new(
        maximum_running_jobs: u32,
        maximum_active_sources: u32,
        maximum_connected_clients: u32,
        maximum_available_disk_bytes: u64,
        maximum_required_disk_bytes: u64,
        maximum_schema_version: u32,
    ) -> Result<Self, RuntimeActivityBindingError> {
        if maximum_running_jobs == 0
            || maximum_active_sources == 0
            || maximum_connected_clients == 0
            || maximum_available_disk_bytes == 0
            || maximum_required_disk_bytes == 0
            || maximum_required_disk_bytes > maximum_available_disk_bytes
            || maximum_schema_version == 0
        {
            return Err(RuntimeActivityBindingError::InvalidLimits);
        }
        Ok(Self {
            maximum_running_jobs,
            maximum_active_sources,
            maximum_connected_clients,
            maximum_available_disk_bytes,
            maximum_required_disk_bytes,
            maximum_schema_version,
        })
    }
}

/// Exact scheduler facts sampled from the installed job authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct JobActivityFacts {
    running_jobs: u32,
    running_mutation_jobs: u32,
}

impl JobActivityFacts {
    /// Captures all running jobs and the mutation-producing subset.
    pub(crate) const fn new(running_jobs: u32, running_mutation_jobs: u32) -> Self {
        Self {
            running_jobs,
            running_mutation_jobs,
        }
    }
}

/// Exact paper-execution and reconciliation facts sampled from execution authorities.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ExecutionActivityFacts {
    paper_execution_active: bool,
    reconciliation_pending: bool,
}

impl ExecutionActivityFacts {
    /// Captures the two independent execution blockers.
    pub(crate) const fn new(paper_execution_active: bool, reconciliation_pending: bool) -> Self {
        Self {
            paper_execution_active,
            reconciliation_pending,
        }
    }
}

/// Path-free storage and schema facts for one exact workspace.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WorkspaceStorageFacts {
    schema_version: u32,
    available_disk_bytes: u64,
}

impl WorkspaceStorageFacts {
    /// Captures facts already resolved by the workspace and storage authorities.
    pub(crate) const fn new(schema_version: u32, available_disk_bytes: u64) -> Self {
        Self {
            schema_version,
            available_disk_bytes,
        }
    }
}

/// Complete set of authority readers installed atomically after service composition.
///
/// Every field is mandatory, so a partial reader bundle cannot be represented or installed.
#[derive(Clone)]
pub(crate) struct RuntimeActivityReaders {
    jobs: Arc<JobActivityReader>,
    sources: Arc<SourceActivityReader>,
    execution: Arc<ExecutionActivityReader>,
    clients: Arc<ClientActivityReader>,
    workspace: Arc<WorkspaceActivityReader>,
}

impl RuntimeActivityReaders {
    /// Owns all readers required by recovery and update preflight.
    pub(crate) fn new(
        jobs: Arc<JobActivityReader>,
        sources: Arc<SourceActivityReader>,
        execution: Arc<ExecutionActivityReader>,
        clients: Arc<ClientActivityReader>,
        workspace: Arc<WorkspaceActivityReader>,
    ) -> Self {
        Self {
            jobs,
            sources,
            execution,
            clients,
            workspace,
        }
    }
}

impl fmt::Debug for RuntimeActivityReaders {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RuntimeActivityReaders([AUTHORITY READERS])")
    }
}

/// Service-owned bridge that breaks the Operations-to-runtime construction cycle.
///
/// The coordinator can be shared with Operations before jobs, clients, sources, and execution are
/// composed. Reads remain unavailable until the composition root atomically binds the complete
/// reader bundle. Binding is permanent for the process generation.
pub(crate) struct RuntimeActivityCoordinator {
    limits: RuntimeActivityLimits,
    readers: OnceLock<RuntimeActivityReaders>,
}

impl RuntimeActivityCoordinator {
    /// Creates an unready coordinator with explicit validation ceilings.
    pub(crate) const fn new(limits: RuntimeActivityLimits) -> Self {
        Self {
            limits,
            readers: OnceLock::new(),
        }
    }

    /// Installs the complete authority-reader bundle exactly once.
    pub(crate) fn bind(
        &self,
        readers: RuntimeActivityReaders,
    ) -> Result<(), RuntimeActivityBindingError> {
        self.readers
            .set(readers)
            .map_err(|_readers| RuntimeActivityBindingError::AlreadyBound)
    }

    /// Reports whether every required authority reader has been installed.
    #[must_use]
    pub(crate) fn is_ready(&self) -> bool {
        self.readers.get().is_some()
    }

    /// Samples and validates a complete workspace-switch preflight snapshot.
    pub(crate) fn recovery_snapshot(
        &self,
        workspace_id: WorkspaceId,
        required_disk_bytes: u64,
        minimum_schema_version: u32,
        maximum_schema_version: u32,
    ) -> Result<WorkspaceActivitySnapshot, ServiceError> {
        self.validate_request(
            required_disk_bytes,
            minimum_schema_version,
            maximum_schema_version,
        )?;
        let readers = self.readers()?;
        let jobs = (readers.jobs)()?;
        let active_sources = (readers.sources)()?;
        let execution = (readers.execution)()?;
        let connected_clients = (readers.clients)()?;
        let workspace = (readers.workspace)(workspace_id)?;
        self.validate_facts(jobs, active_sources, connected_clients, workspace)?;

        let schema_compatible = workspace.schema_version >= minimum_schema_version
            && workspace.schema_version <= maximum_schema_version;
        Ok(WorkspaceActivitySnapshot::new(
            jobs.running_jobs,
            active_sources,
            execution.paper_execution_active,
            execution.reconciliation_pending,
            connected_clients,
            workspace.available_disk_bytes,
            required_disk_bytes,
            schema_compatible,
        ))
    }

    /// Samples and validates a complete staged-update preflight snapshot.
    pub(crate) fn update_snapshot(
        &self,
        workspace_id: WorkspaceId,
        required_disk_bytes: u64,
    ) -> Result<UpdateActivitySnapshot, ServiceError> {
        if required_disk_bytes == 0 || required_disk_bytes > self.limits.maximum_required_disk_bytes
        {
            return Err(ServiceError::InvalidRequest);
        }
        let readers = self.readers()?;
        let jobs = (readers.jobs)()?;
        let execution = (readers.execution)()?;
        let workspace = (readers.workspace)(workspace_id)?;
        self.validate_facts(jobs, 0, 0, workspace)?;

        Ok(UpdateActivitySnapshot::new(
            workspace.schema_version,
            workspace.available_disk_bytes,
            required_disk_bytes,
            jobs.running_mutation_jobs,
            execution.paper_execution_active,
            execution.reconciliation_pending,
        ))
    }

    fn readers(&self) -> Result<&RuntimeActivityReaders, ServiceError> {
        self.readers.get().ok_or(ServiceError::Unavailable)
    }

    fn validate_request(
        &self,
        required_disk_bytes: u64,
        minimum_schema_version: u32,
        maximum_schema_version: u32,
    ) -> Result<(), ServiceError> {
        if required_disk_bytes > self.limits.maximum_required_disk_bytes
            || minimum_schema_version == 0
            || minimum_schema_version > maximum_schema_version
            || maximum_schema_version > self.limits.maximum_schema_version
        {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(())
    }

    fn validate_facts(
        &self,
        jobs: JobActivityFacts,
        active_sources: u32,
        connected_clients: u32,
        workspace: WorkspaceStorageFacts,
    ) -> Result<(), ServiceError> {
        if jobs.running_jobs > self.limits.maximum_running_jobs
            || jobs.running_mutation_jobs > jobs.running_jobs
            || active_sources > self.limits.maximum_active_sources
            || connected_clients > self.limits.maximum_connected_clients
            || workspace.available_disk_bytes > self.limits.maximum_available_disk_bytes
            || workspace.schema_version == 0
            || workspace.schema_version > self.limits.maximum_schema_version
        {
            return Err(ServiceError::InvalidResult);
        }
        Ok(())
    }
}

impl fmt::Debug for RuntimeActivityCoordinator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeActivityCoordinator")
            .field("limits", &self.limits)
            .field("ready", &self.is_ready())
            .finish()
    }
}

impl RecoveryActivityAuthority for RuntimeActivityCoordinator {
    fn snapshot(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<WorkspaceActivitySnapshot, ServiceError> {
        self.recovery_snapshot(workspace_id, 1, 1, self.limits.maximum_schema_version)
    }
}

/// Composition-time failure while establishing the process-generation activity bridge.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum RuntimeActivityBindingError {
    /// At least one configured validation ceiling was zero.
    #[error("runtime activity limits are invalid")]
    InvalidLimits,
    /// This process generation already owns a complete reader bundle.
    #[error("runtime activity authority readers are already bound")]
    AlreadyBound,
}
