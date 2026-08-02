//! Generation-fenced lifecycle coordination for workspace and restore activation.

mod update;

pub use update::{
    ProgramGeneration, StagedUpdateCandidate, TrustedUpdateAuthority, UpdateActivation,
    UpdateActivitySnapshot, UpdateApproval, UpdateError, UpdateJournal, UpdateOutcome,
    UpdatePreview, UpdateReceipt, UpdateTransitionRecord, UpdateTransitionState,
};

use std::{fmt, num::NonZeroU64, sync::Arc, time::Duration};

use async_trait::async_trait;
use market_squawk_runtime::{
    InstallationId, RuntimeContractError, RuntimeIdentity, ServiceGeneration, WorkspaceId,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::sync::Mutex;

/// Monotonic identity that fences every request and event after a workspace transition.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct WorkspaceGeneration(NonZeroU64);

impl WorkspaceGeneration {
    /// Creates a nonzero workspace generation.
    pub fn try_new(value: u64) -> Result<Self, LifecycleError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(LifecycleError::InvalidGeneration)
    }

    /// Returns the generation value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    fn advance(self, distance: u64) -> Result<Self, LifecycleError> {
        self.get()
            .checked_add(distance)
            .ok_or(LifecycleError::GenerationExhausted)
            .and_then(Self::try_new)
    }
}

/// Exact workspace and generation admitted by the installed service.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct WorkspaceRuntimeIdentity {
    workspace_id: WorkspaceId,
    generation: WorkspaceGeneration,
}

impl WorkspaceRuntimeIdentity {
    /// Binds a workspace identity to a nonzero generation.
    pub fn try_new(workspace_id: WorkspaceId, generation: u64) -> Result<Self, LifecycleError> {
        Ok(Self {
            workspace_id,
            generation: WorkspaceGeneration::try_new(generation)?,
        })
    }

    /// Returns the active workspace.
    #[must_use]
    pub const fn workspace_id(self) -> WorkspaceId {
        self.workspace_id
    }

    /// Returns the generation that clients must present.
    #[must_use]
    pub const fn generation(self) -> WorkspaceGeneration {
        self.generation
    }

    /// Derives the durable workspace fence from the installed runtime identity.
    pub fn try_from_runtime(runtime: RuntimeIdentity) -> Result<Self, LifecycleError> {
        Self::try_new(runtime.workspace_id(), runtime.service_generation().get())
    }

    /// Produces the exact runtime identity clients must use after reconnecting.
    pub fn to_runtime(
        self,
        installation_id: InstallationId,
    ) -> Result<RuntimeIdentity, LifecycleError> {
        Ok(RuntimeIdentity::try_new(
            installation_id,
            self.workspace_id,
            ServiceGeneration::try_new(self.generation.get())?,
        )?)
    }
}

/// Bounded preflight facts captured by the composition owner before switching workspaces.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct WorkspaceActivitySnapshot {
    running_jobs: u32,
    active_sources: u32,
    paper_execution_active: bool,
    execution_reconciliation_pending: bool,
    connected_clients: u32,
    available_disk_bytes: u64,
    required_disk_bytes: u64,
    schema_compatible: bool,
}

impl WorkspaceActivitySnapshot {
    /// Creates an explicit preflight snapshot without deriving authority from display state.
    #[allow(
        clippy::too_many_arguments,
        reason = "each preflight dimension is independently authoritative"
    )]
    pub const fn new(
        running_jobs: u32,
        active_sources: u32,
        paper_execution_active: bool,
        execution_reconciliation_pending: bool,
        connected_clients: u32,
        available_disk_bytes: u64,
        required_disk_bytes: u64,
        schema_compatible: bool,
    ) -> Self {
        Self {
            running_jobs,
            active_sources,
            paper_execution_active,
            execution_reconciliation_pending,
            connected_clients,
            available_disk_bytes,
            required_disk_bytes,
            schema_compatible,
        }
    }

    /// Creates a quiescent, schema-compatible preflight snapshot.
    pub const fn quiescent(
        available_disk_bytes: u64,
        required_disk_bytes: u64,
        connected_clients: u32,
    ) -> Self {
        Self::new(
            0,
            0,
            false,
            false,
            connected_clients,
            available_disk_bytes,
            required_disk_bytes,
            true,
        )
    }

    fn blockers(self) -> Vec<WorkspaceSwitchBlocker> {
        let mut blockers = Vec::new();
        if self.running_jobs > 0 {
            blockers.push(WorkspaceSwitchBlocker::RunningJobs);
        }
        if self.active_sources > 0 {
            blockers.push(WorkspaceSwitchBlocker::ActiveSources);
        }
        if self.paper_execution_active {
            blockers.push(WorkspaceSwitchBlocker::PaperExecutionActive);
        }
        if self.execution_reconciliation_pending {
            blockers.push(WorkspaceSwitchBlocker::ExecutionReconciliationPending);
        }
        if self.available_disk_bytes < self.required_disk_bytes {
            blockers.push(WorkspaceSwitchBlocker::InsufficientDisk);
        }
        if !self.schema_compatible {
            blockers.push(WorkspaceSwitchBlocker::IncompatibleSchema);
        }
        blockers
    }
}

/// Stable reason a requested workspace transition cannot be approved.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceSwitchBlocker {
    RunningJobs,
    ActiveSources,
    PaperExecutionActive,
    ExecutionReconciliationPending,
    InsufficientDisk,
    IncompatibleSchema,
}

/// Immutable switch preflight whose digest is bound into explicit approval.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct WorkspaceSwitchPreview {
    active: WorkspaceRuntimeIdentity,
    target: WorkspaceId,
    activity: WorkspaceActivitySnapshot,
    blockers: Vec<WorkspaceSwitchBlocker>,
    preview_sha256: [u8; 32],
}

impl WorkspaceSwitchPreview {
    /// Returns whether the exact preview is eligible for explicit approval.
    #[must_use]
    pub fn can_approve(&self) -> bool {
        self.blockers.is_empty()
    }

    /// Consumes an unblocked preview into a digest-bound approval.
    pub fn try_approve(self) -> Result<WorkspaceSwitchApproval, LifecycleError> {
        if !self.can_approve() {
            return Err(LifecycleError::PreflightBlocked);
        }
        Ok(WorkspaceSwitchApproval {
            active: self.active,
            target: self.target,
            preview_sha256: self.preview_sha256,
        })
    }
}

/// Non-forgeable-in-process approval bound to one exact preview and active generation.
#[derive(Clone, Copy, Debug)]
pub struct WorkspaceSwitchApproval {
    active: WorkspaceRuntimeIdentity,
    target: WorkspaceId,
    preview_sha256: [u8; 32],
}

/// Composition-owned drain, activation, and health-check boundary.
#[async_trait]
pub trait WorkspaceTransition: fmt::Debug + Send + Sync {
    /// Rejects new work, drains jobs, stops sources and paper execution, and reconciles execution.
    async fn drain_and_reconcile(&self, deadline: std::time::Instant)
    -> Result<(), LifecycleError>;

    /// Changes the process composition to the selected prepared workspace.
    async fn activate(&self, workspace_id: WorkspaceId) -> Result<(), LifecycleError>;

    /// Proves the activated composition is healthy before the fence is reopened.
    async fn health_check(&self, workspace_id: WorkspaceId) -> Result<(), LifecycleError>;
}

/// Durable audit sink that must commit before a switch result becomes current.
pub trait WorkspaceTransitionJournal: fmt::Debug + Send + Sync {
    /// Appends one complete success or rollback record.
    fn append(&self, record: &WorkspaceTransitionRecord) -> Result<(), LifecycleError>;
}

/// Durable result of one attempted switch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct WorkspaceTransitionRecord {
    previous: WorkspaceRuntimeIdentity,
    attempted: WorkspaceRuntimeIdentity,
    active: WorkspaceRuntimeIdentity,
    preview_sha256: [u8; 32],
    disposition: WorkspaceTransitionDisposition,
}

/// Whether the target became active or the prior workspace was restored.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceTransitionDisposition {
    Activated,
    RolledBack,
}

/// Client-visible receipt that forces resynchronization under the returned identity.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct WorkspaceSwitchReceipt {
    active: WorkspaceRuntimeIdentity,
    attempted: WorkspaceRuntimeIdentity,
}

impl WorkspaceSwitchReceipt {
    /// Returns the only workspace/generation current after the transition.
    #[must_use]
    pub const fn active(self) -> WorkspaceRuntimeIdentity {
        self.active
    }
}

impl WorkspaceTransitionRecord {
    /// Returns the only active identity after this transition record committed.
    #[must_use]
    pub const fn active(&self) -> WorkspaceRuntimeIdentity {
        self.active
    }

    /// Returns the target identity allocated for the attempted activation.
    #[must_use]
    pub const fn attempted(&self) -> WorkspaceRuntimeIdentity {
        self.attempted
    }

    /// Returns whether the target activated or the prior workspace was restored.
    #[must_use]
    pub const fn disposition(&self) -> WorkspaceTransitionDisposition {
        self.disposition
    }
}

/// Complete outcome of a switch attempt.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceSwitchOutcome {
    Activated(WorkspaceSwitchReceipt),
    RolledBack(WorkspaceSwitchReceipt),
}

#[derive(Debug)]
struct LifecycleState {
    active: WorkspaceRuntimeIdentity,
    fenced: bool,
}

/// Single-writer authority for active-workspace identity and request admission.
pub struct WorkspaceLifecycleAuthority {
    state: Mutex<LifecycleState>,
    journal: Arc<dyn WorkspaceTransitionJournal>,
}

impl WorkspaceLifecycleAuthority {
    /// Opens the authority at a previously restored durable identity.
    pub fn try_new(
        active: WorkspaceRuntimeIdentity,
        journal: Arc<dyn WorkspaceTransitionJournal>,
    ) -> Result<Self, LifecycleError> {
        if active.generation().get() == 0 {
            return Err(LifecycleError::InvalidGeneration);
        }
        Ok(Self {
            state: Mutex::new(LifecycleState {
                active,
                fenced: false,
            }),
            journal,
        })
    }

    /// Returns the current identity for reconnect and event-cursor reset.
    pub fn current(&self) -> Result<WorkspaceRuntimeIdentity, LifecycleError> {
        self.state
            .try_lock()
            .map(|state| state.active)
            .map_err(|_| LifecycleError::AuthorityBusy)
    }

    /// Rejects fenced, wrong-workspace, and stale-generation requests.
    pub fn admit_request(&self, presented: WorkspaceRuntimeIdentity) -> Result<(), LifecycleError> {
        let state = self
            .state
            .try_lock()
            .map_err(|_| LifecycleError::AuthorityBusy)?;
        if state.fenced {
            return Err(LifecycleError::RequestsFenced);
        }
        if presented.workspace_id() != state.active.workspace_id() {
            return Err(LifecycleError::WrongWorkspace);
        }
        if presented.generation() != state.active.generation() {
            return Err(LifecycleError::StaleWorkspaceGeneration);
        }
        Ok(())
    }

    /// Produces a digest-bound preview from exact current state and bounded activity evidence.
    pub fn preview_switch(
        &self,
        target: WorkspaceId,
        activity: WorkspaceActivitySnapshot,
    ) -> Result<WorkspaceSwitchPreview, LifecycleError> {
        let state = self
            .state
            .try_lock()
            .map_err(|_| LifecycleError::AuthorityBusy)?;
        if state.fenced || state.active.workspace_id() == target {
            return Err(LifecycleError::InvalidTarget);
        }
        let blockers = activity.blockers();
        let encoded = serde_json::to_vec(&(
            "market-squawk-workspace-switch-preview-v1",
            state.active,
            target,
            activity,
            &blockers,
        ))
        .map_err(|_| LifecycleError::Encoding)?;
        Ok(WorkspaceSwitchPreview {
            active: state.active,
            target,
            activity,
            blockers,
            preview_sha256: Sha256::digest(encoded).into(),
        })
    }

    /// Executes one approved transition while retaining the request fence across all awaits.
    pub async fn switch(
        &self,
        approval: WorkspaceSwitchApproval,
        transition: &dyn WorkspaceTransition,
        timeout: Duration,
    ) -> Result<WorkspaceSwitchOutcome, LifecycleError> {
        if timeout.is_zero() || timeout > Duration::from_secs(10 * 60) {
            return Err(LifecycleError::InvalidTimeout);
        }
        let mut state = self.state.lock().await;
        if state.fenced || state.active != approval.active {
            return Err(LifecycleError::StaleApproval);
        }
        let deadline = std::time::Instant::now()
            .checked_add(timeout)
            .ok_or(LifecycleError::InvalidTimeout)?;
        let attempted = WorkspaceRuntimeIdentity {
            workspace_id: approval.target,
            generation: approval.active.generation().advance(1)?,
        };
        state.fenced = true;
        if let Err(error) = transition.drain_and_reconcile(deadline).await {
            state.fenced = false;
            return Err(error);
        }
        let activated = transition.activate(approval.target).await.is_ok()
            && transition.health_check(approval.target).await.is_ok();
        if activated {
            let record = WorkspaceTransitionRecord {
                previous: approval.active,
                attempted,
                active: attempted,
                preview_sha256: approval.preview_sha256,
                disposition: WorkspaceTransitionDisposition::Activated,
            };
            if let Err(error) = self.journal.append(&record) {
                return rollback_after_failure(
                    &mut state,
                    approval,
                    attempted,
                    transition,
                    &*self.journal,
                    error,
                )
                .await;
            }
            state.active = attempted;
            state.fenced = false;
            return Ok(WorkspaceSwitchOutcome::Activated(WorkspaceSwitchReceipt {
                active: attempted,
                attempted,
            }));
        }
        rollback_after_failure(
            &mut state,
            approval,
            attempted,
            transition,
            &*self.journal,
            LifecycleError::HealthCheckFailed,
        )
        .await
    }
}

async fn rollback_after_failure(
    state: &mut LifecycleState,
    approval: WorkspaceSwitchApproval,
    attempted: WorkspaceRuntimeIdentity,
    transition: &dyn WorkspaceTransition,
    journal: &dyn WorkspaceTransitionJournal,
    _cause: LifecycleError,
) -> Result<WorkspaceSwitchOutcome, LifecycleError> {
    transition.activate(approval.active.workspace_id()).await?;
    transition
        .health_check(approval.active.workspace_id())
        .await
        .map_err(|_| LifecycleError::RollbackFailed)?;
    let restored = WorkspaceRuntimeIdentity {
        workspace_id: approval.active.workspace_id(),
        generation: attempted.generation().advance(1)?,
    };
    let record = WorkspaceTransitionRecord {
        previous: approval.active,
        attempted,
        active: restored,
        preview_sha256: approval.preview_sha256,
        disposition: WorkspaceTransitionDisposition::RolledBack,
    };
    journal.append(&record)?;
    state.active = restored;
    state.fenced = false;
    Ok(WorkspaceSwitchOutcome::RolledBack(WorkspaceSwitchReceipt {
        active: restored,
        attempted,
    }))
}

impl fmt::Debug for WorkspaceLifecycleAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("WorkspaceLifecycleAuthority([GENERATION-FENCED])")
    }
}

/// Typed lifecycle failure without workspace paths or sensitive state.
#[derive(Debug, Error)]
pub enum LifecycleError {
    #[error("workspace generation must be nonzero")]
    InvalidGeneration,
    #[error("workspace generation is exhausted")]
    GenerationExhausted,
    #[error("workspace lifecycle authority is busy")]
    AuthorityBusy,
    #[error("workspace lifecycle authority is unavailable")]
    AuthorityUnavailable,
    #[error("workspace request admission is fenced")]
    RequestsFenced,
    #[error("request names a different workspace")]
    WrongWorkspace,
    #[error("request names a stale workspace generation")]
    StaleWorkspaceGeneration,
    #[error("workspace switch target is invalid")]
    InvalidTarget,
    #[error("workspace switch preflight is blocked")]
    PreflightBlocked,
    #[error("workspace switch approval is stale")]
    StaleApproval,
    #[error("workspace lifecycle timeout is invalid")]
    InvalidTimeout,
    #[error("workspace lifecycle evidence could not be encoded")]
    Encoding,
    #[error("activated workspace failed its health check")]
    HealthCheckFailed,
    #[error("prior workspace could not be restored and verified")]
    RollbackFailed,
    #[error("workspace identity cannot be represented by the installed runtime protocol")]
    RuntimeIdentity(#[from] RuntimeContractError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    #[derive(Debug)]
    struct FixtureTransition {
        fail_target_health: bool,
        activated: Mutex<Vec<WorkspaceId>>,
    }

    #[async_trait]
    impl WorkspaceTransition for FixtureTransition {
        async fn drain_and_reconcile(
            &self,
            _deadline: std::time::Instant,
        ) -> Result<(), LifecycleError> {
            Ok(())
        }

        async fn activate(&self, workspace_id: WorkspaceId) -> Result<(), LifecycleError> {
            self.activated
                .lock()
                .map_err(|_| LifecycleError::AuthorityUnavailable)?
                .push(workspace_id);
            Ok(())
        }

        async fn health_check(&self, workspace_id: WorkspaceId) -> Result<(), LifecycleError> {
            if self.fail_target_health
                && self
                    .activated
                    .lock()
                    .map_err(|_| LifecycleError::AuthorityUnavailable)?
                    .first()
                    == Some(&workspace_id)
            {
                return Err(LifecycleError::HealthCheckFailed);
            }
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct RecordingJournal(Mutex<Vec<WorkspaceTransitionRecord>>);

    impl WorkspaceTransitionJournal for RecordingJournal {
        fn append(&self, record: &WorkspaceTransitionRecord) -> Result<(), LifecycleError> {
            self.0
                .lock()
                .map_err(|_| LifecycleError::AuthorityUnavailable)?
                .push(record.clone());
            Ok(())
        }
    }

    fn workspace(value: u128) -> Result<WorkspaceId, Box<dyn std::error::Error>> {
        Ok(WorkspaceId::try_from_uuid(Uuid::from_u128(value))?)
    }

    #[tokio::test]
    async fn failed_switch_restores_prior_workspace_under_strictly_newer_generation()
    -> Result<(), Box<dyn std::error::Error>> {
        let original = workspace(1)?;
        let target = workspace(2)?;
        let journal = Arc::new(RecordingJournal::default());
        let authority = WorkspaceLifecycleAuthority::try_new(
            WorkspaceRuntimeIdentity::try_new(original, 7)?,
            journal.clone(),
        )?;
        let preview = authority.preview_switch(
            target,
            WorkspaceActivitySnapshot::quiescent(1_000_000, 500_000, 1),
        )?;
        let approval = preview.try_approve()?;
        let transition = FixtureTransition {
            fail_target_health: true,
            activated: Mutex::new(Vec::new()),
        };

        let outcome = authority
            .switch(approval, &transition, std::time::Duration::from_secs(1))
            .await?;

        let WorkspaceSwitchOutcome::RolledBack(receipt) = outcome else {
            return Err("expected rollback".into());
        };
        assert_eq!(receipt.active().workspace_id(), original);
        assert_eq!(receipt.active().generation().get(), 9);
        assert_eq!(authority.current()?, receipt.active());
        assert_eq!(journal.0.lock().map_err(|_| "journal")?.len(), 1);
        assert!(matches!(
            authority.admit_request(WorkspaceRuntimeIdentity::try_new(original, 7)?),
            Err(LifecycleError::StaleWorkspaceGeneration)
        ));
        Ok(())
    }
}
