//! Concrete installed-product restore, workspace-selector, and program-rollback authority.

use std::{
    collections::BTreeMap,
    fmt,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use market_squawk_data::{AnalyticalBackupLimits, AnalyticalBackupLocation};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier};
use market_squawk_installer::{RollbackRequest, rollback, status};
use market_squawk_platform::{LocalAuthorityStateStore, LocalPaths};
use market_squawk_runtime::{InstallationId, WorkspaceId};
use market_squawk_services::ServiceError;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use crate::{
    application::{
        backup::{ProductBackupManifest, ProductBackupService, ProductRestoreComponentAuthority},
        lifecycle::{
            LifecycleError, ProgramGeneration, WorkspaceActivitySnapshot,
            WorkspaceLifecycleAuthority, WorkspaceRestartTransition, WorkspaceRuntimeIdentity,
            WorkspaceSwitchApproval,
        },
        operations::{
            ManagedRecoveryOperations, PreparedOperation, ProgramRollbackPreviewEvidence,
            RestorePreviewEvidence,
        },
        workspace::DurableWorkspaceRegistry,
    },
    jobs::{
        LifecycleJobExecutionError, LifecycleJobPublication, RecoveryJobAction, RecoveryJobCommand,
        RecoveryJobRunner,
    },
};

const FORMAT_VERSION: u16 = 1;
const AUTHORITY_DIRECTORY: &str = "installed-recovery-authority";
const MAXIMUM_PREVIEWS: usize = 64;
const MAXIMUM_OPERATIONS: usize = 128;
const MAXIMUM_RECEIPTS: usize = 256;

/// Live, bounded facts published by the sole installed composition owner.
///
/// The registry is deliberately concrete: application composition publishes snapshots after
/// consulting the job, source, paper, execution, client, disk, and schema authorities. Recovery
/// never reconstructs those facts from presentation state.
#[derive(Default)]
pub struct RecoveryRuntimeActivity {
    snapshots: RwLock<BTreeMap<WorkspaceId, WorkspaceActivitySnapshot>>,
}

impl RecoveryRuntimeActivity {
    /// Replaces the exact current snapshot for one known workspace.
    pub fn publish(
        &self,
        workspace_id: WorkspaceId,
        snapshot: WorkspaceActivitySnapshot,
    ) -> Result<(), ServiceError> {
        self.snapshots
            .write()
            .map_err(|_| ServiceError::Unavailable)?
            .insert(workspace_id, snapshot);
        Ok(())
    }

    /// Removes facts when a workspace has been permanently retired.
    pub fn remove(&self, workspace_id: WorkspaceId) -> Result<(), ServiceError> {
        self.snapshots
            .write()
            .map_err(|_| ServiceError::Unavailable)?
            .remove(&workspace_id);
        Ok(())
    }

    fn snapshot(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<WorkspaceActivitySnapshot, ServiceError> {
        self.snapshots
            .read()
            .map_err(|_| ServiceError::Unavailable)?
            .get(&workspace_id)
            .copied()
            .ok_or(ServiceError::Unavailable)
    }
}

impl fmt::Debug for RecoveryRuntimeActivity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RecoveryRuntimeActivity([COMPOSITION-OWNED FACTS])")
    }
}

/// Exact root-composition hooks for a supervisor-restart workspace transition.
///
/// `request_restart` signals the outer service lifecycle only after the durable selector commits.
/// It must not invoke native service control, in-process recomposition, or same-process health.
#[async_trait]
pub trait InstalledServiceRecoveryHooks: fmt::Debug + Send + Sync {
    /// Rejects new mutation work, drains running work and sources, stops paper execution, and
    /// completes execution reconciliation before the selector may change.
    async fn drain_and_reconcile(&self, deadline: Instant) -> Result<(), LifecycleError>;

    /// Requests orderly shutdown so the installed supervisor starts the replacement process.
    fn request_restart(
        &self,
        expected: market_squawk_runtime::RuntimeIdentity,
    ) -> Result<(), LifecycleError>;
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ArmedWorkspaceOperation {
    operation_identity: String,
    evidence_sha256: [u8; 32],
    original: WorkspaceRuntimeIdentity,
    target: WorkspaceId,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum SelectorDisposition {
    Activated,
    RolledBack,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct PendingWorkspaceSelector {
    previous: WorkspaceRuntimeIdentity,
    candidate: WorkspaceRuntimeIdentity,
    disposition: SelectorDisposition,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct PendingProgramRollback {
    operation_identity: String,
    evidence_sha256: [u8; 32],
    previous_generation: ProgramGeneration,
    attempted_generation: ProgramGeneration,
    source_version: String,
    target_version: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
enum DurableRecoveryReceipt {
    Workspace {
        operation_identity: String,
        evidence_sha256: [u8; 32],
        active: WorkspaceRuntimeIdentity,
        disposition: SelectorDisposition,
    },
    Program {
        operation_identity: String,
        evidence_sha256: [u8; 32],
        active_generation: ProgramGeneration,
        active_version: String,
    },
}

impl DurableRecoveryReceipt {
    fn operation_identity(&self) -> &str {
        match self {
            Self::Workspace {
                operation_identity, ..
            }
            | Self::Program {
                operation_identity, ..
            } => operation_identity,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct RecoveryDocument {
    format_version: u16,
    active_workspace: WorkspaceRuntimeIdentity,
    pending_workspace: Option<PendingWorkspaceSelector>,
    armed_workspace: Option<ArmedWorkspaceOperation>,
    program_generation: ProgramGeneration,
    pending_program: Option<PendingProgramRollback>,
    program_recovery_required: bool,
    receipts: Vec<DurableRecoveryReceipt>,
}

impl RecoveryDocument {
    fn initial(
        active_workspace: WorkspaceRuntimeIdentity,
        program_generation: ProgramGeneration,
    ) -> Self {
        Self {
            format_version: FORMAT_VERSION,
            active_workspace,
            pending_workspace: None,
            armed_workspace: None,
            program_generation,
            pending_program: None,
            program_recovery_required: false,
            receipts: Vec::new(),
        }
    }

    fn validate(self) -> Result<Self, ServiceError> {
        if self.format_version != FORMAT_VERSION
            || self.receipts.len() > MAXIMUM_RECEIPTS
            || self.pending_workspace.as_ref().is_some_and(|pending| {
                pending.previous != self.active_workspace
                    || pending.candidate.workspace_id() == pending.previous.workspace_id()
                    || pending.candidate.generation().get() <= pending.previous.generation().get()
            })
            || self.armed_workspace.as_ref().is_some_and(|armed| {
                armed.evidence_sha256 == [0; 32]
                    || armed.operation_identity.is_empty()
                    || armed.target == armed.original.workspace_id()
            })
            || self.pending_program.as_ref().is_some_and(|pending| {
                pending.evidence_sha256 == [0; 32]
                    || pending.operation_identity.is_empty()
                    || pending.source_version.is_empty()
                    || pending.target_version.is_empty()
                    || pending.attempted_generation.get() <= pending.previous_generation.get()
            })
        {
            return Err(ServiceError::Unavailable);
        }
        Ok(self)
    }
}

/// Two-copy crash-safe selector, program-generation, and terminal-receipt authority.
pub struct DurableRecoveryState {
    store: LocalAuthorityStateStore,
    document: Mutex<RecoveryDocument>,
}

impl DurableRecoveryState {
    /// Opens or initializes recovery state beneath the prepared control root.
    pub fn try_open(
        control_root: &Path,
        initial_workspace: WorkspaceRuntimeIdentity,
        initial_program_generation: ProgramGeneration,
    ) -> Result<Self, ServiceError> {
        let store = LocalAuthorityStateStore::try_open(control_root.join(AUTHORITY_DIRECTORY))
            .map_err(|_| ServiceError::Unavailable)?;
        let document = match store.load().map_err(|_| ServiceError::Unavailable)? {
            Some(encoded) => serde_json::from_slice::<RecoveryDocument>(&encoded)
                .map_err(|_| ServiceError::Unavailable)?
                .validate()?,
            None => {
                let document =
                    RecoveryDocument::initial(initial_workspace, initial_program_generation);
                store_document(&store, &document)?;
                document
            }
        };
        if document.pending_workspace.is_none() && document.active_workspace != initial_workspace
            || document.program_generation != initial_program_generation
                && document.pending_program.is_none()
        {
            return Err(ServiceError::Unavailable);
        }
        Ok(Self {
            store,
            document: Mutex::new(document),
        })
    }

    /// Returns the identity startup must compose: a durable pending selector wins over active.
    pub fn startup_identity(&self) -> Result<WorkspaceRuntimeIdentity, ServiceError> {
        let document = self
            .document
            .lock()
            .map_err(|_| ServiceError::Unavailable)?;
        Ok(document
            .pending_workspace
            .as_ref()
            .map_or(document.active_workspace, |pending| pending.candidate))
    }

    /// Finalizes a supervisor-started pending selector only after authenticated health succeeds.
    pub fn startup_healthy(&self, observed: WorkspaceRuntimeIdentity) -> Result<(), ServiceError> {
        let mut document = self
            .document
            .lock()
            .map_err(|_| ServiceError::Unavailable)?;
        if document.pending_workspace.is_none() && document.active_workspace == observed {
            return Ok(());
        }
        let pending = document
            .pending_workspace
            .as_ref()
            .filter(|pending| pending.candidate == observed)
            .cloned()
            .ok_or(ServiceError::InvalidRequest)?;
        let mut candidate = document.clone();
        candidate.active_workspace = observed;
        candidate.pending_workspace = None;
        if let Some(armed) = candidate.armed_workspace.as_ref() {
            upsert_receipt(
                &mut candidate.receipts,
                DurableRecoveryReceipt::Workspace {
                    operation_identity: armed.operation_identity.clone(),
                    evidence_sha256: armed.evidence_sha256,
                    active: observed,
                    disposition: pending.disposition,
                },
            );
        }
        store_document(&self.store, &candidate)?;
        *document = candidate;
        Ok(())
    }

    /// Converts a failed attempted startup into a rollback selector under a strictly newer fence.
    ///
    /// The caller must then return startup failure so the installed supervisor starts again.
    pub fn startup_failed(
        &self,
        failed: WorkspaceRuntimeIdentity,
    ) -> Result<WorkspaceRuntimeIdentity, ServiceError> {
        let mut document = self
            .document
            .lock()
            .map_err(|_| ServiceError::Unavailable)?;
        let pending = document
            .pending_workspace
            .as_ref()
            .filter(|pending| pending.candidate == failed)
            .cloned()
            .ok_or(ServiceError::InvalidRequest)?;
        if pending.disposition == SelectorDisposition::RolledBack {
            return Err(ServiceError::Unavailable);
        }
        let generation = pending
            .candidate
            .generation()
            .get()
            .checked_add(1)
            .ok_or(ServiceError::ResourceExhausted)?;
        let rollback =
            WorkspaceRuntimeIdentity::try_new(pending.previous.workspace_id(), generation)
                .map_err(|_| ServiceError::Unavailable)?;
        let mut candidate = document.clone();
        candidate.pending_workspace = Some(PendingWorkspaceSelector {
            previous: pending.previous,
            candidate: rollback,
            disposition: SelectorDisposition::RolledBack,
        });
        store_document(&self.store, &candidate)?;
        *document = candidate;
        Ok(rollback)
    }

    fn arm_workspace(
        &self,
        operation: &PreparedOperation,
        original: WorkspaceRuntimeIdentity,
        target: WorkspaceId,
    ) -> Result<(), ServiceError> {
        let mut document = self
            .document
            .lock()
            .map_err(|_| ServiceError::Unavailable)?;
        if document.armed_workspace.is_some()
            || document.pending_workspace.is_some()
            || document.active_workspace != original
        {
            return Err(ServiceError::Unavailable);
        }
        let mut candidate = document.clone();
        candidate.armed_workspace = Some(ArmedWorkspaceOperation {
            operation_identity: operation.identity().as_str().to_owned(),
            evidence_sha256: operation.evidence_digest().bytes(),
            original,
            target,
        });
        store_document(&self.store, &candidate)?;
        *document = candidate;
        Ok(())
    }

    fn stage_workspace(
        &self,
        target: WorkspaceId,
    ) -> Result<WorkspaceRuntimeIdentity, ServiceError> {
        let mut document = self
            .document
            .lock()
            .map_err(|_| ServiceError::Unavailable)?;
        if let Some(pending) = &document.pending_workspace {
            if pending.candidate.workspace_id() == target {
                return Ok(pending.candidate);
            }
            if pending.previous.workspace_id() != target
                || pending.disposition == SelectorDisposition::RolledBack
            {
                return Err(ServiceError::InvalidRequest);
            }
            let generation = pending
                .candidate
                .generation()
                .get()
                .checked_add(1)
                .ok_or(ServiceError::ResourceExhausted)?;
            let rollback = WorkspaceRuntimeIdentity::try_new(target, generation)
                .map_err(|_| ServiceError::Unavailable)?;
            let mut candidate = document.clone();
            candidate.pending_workspace = Some(PendingWorkspaceSelector {
                previous: pending.previous,
                candidate: rollback,
                disposition: SelectorDisposition::RolledBack,
            });
            store_document(&self.store, &candidate)?;
            *document = candidate;
            return Ok(rollback);
        }
        let armed = document
            .armed_workspace
            .as_ref()
            .filter(|armed| armed.target == target || armed.original.workspace_id() == target)
            .ok_or(ServiceError::InvalidRequest)?;
        if document.active_workspace.workspace_id() == target {
            return Ok(document.active_workspace);
        }
        let generation = document
            .active_workspace
            .generation()
            .get()
            .checked_add(1)
            .ok_or(ServiceError::ResourceExhausted)?;
        let selected = WorkspaceRuntimeIdentity::try_new(target, generation)
            .map_err(|_| ServiceError::Unavailable)?;
        let disposition = if armed.target == target {
            SelectorDisposition::Activated
        } else {
            SelectorDisposition::RolledBack
        };
        let mut candidate = document.clone();
        candidate.pending_workspace = Some(PendingWorkspaceSelector {
            previous: document.active_workspace,
            candidate: selected,
            disposition,
        });
        store_document(&self.store, &candidate)?;
        *document = candidate;
        Ok(selected)
    }

    fn selected_for(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<WorkspaceRuntimeIdentity, ServiceError> {
        let document = self
            .document
            .lock()
            .map_err(|_| ServiceError::Unavailable)?;
        document
            .pending_workspace
            .as_ref()
            .map(|pending| pending.candidate)
            .filter(|identity| identity.workspace_id() == workspace_id)
            .or_else(|| {
                (document.active_workspace.workspace_id() == workspace_id)
                    .then_some(document.active_workspace)
            })
            .ok_or(ServiceError::InvalidRequest)
    }

    fn complete_workspace(&self) -> Result<(), ServiceError> {
        let mut document = self
            .document
            .lock()
            .map_err(|_| ServiceError::Unavailable)?;
        if document.pending_workspace.is_some() {
            return Err(ServiceError::Unavailable);
        }
        if document.armed_workspace.is_none() {
            return Ok(());
        }
        let mut candidate = document.clone();
        candidate.armed_workspace = None;
        store_document(&self.store, &candidate)?;
        *document = candidate;
        Ok(())
    }

    fn abandon_unstarted_workspace(&self) -> Result<(), ServiceError> {
        let mut document = self
            .document
            .lock()
            .map_err(|_| ServiceError::Unavailable)?;
        if document.pending_workspace.is_some() {
            return Err(ServiceError::Unavailable);
        }
        let mut candidate = document.clone();
        candidate.armed_workspace = None;
        store_document(&self.store, &candidate)?;
        *document = candidate;
        Ok(())
    }

    fn program_generation(&self) -> Result<ProgramGeneration, ServiceError> {
        self.document
            .lock()
            .map_err(|_| ServiceError::Unavailable)
            .and_then(|document| {
                if document.program_recovery_required || document.pending_program.is_some() {
                    Err(ServiceError::Unavailable)
                } else {
                    Ok(document.program_generation)
                }
            })
    }

    fn begin_program(
        &self,
        operation: &PreparedOperation,
        current: ProgramGeneration,
        source_version: String,
        target_version: String,
    ) -> Result<ProgramGeneration, ServiceError> {
        let attempted = ProgramGeneration::try_new(
            current
                .get()
                .checked_add(1)
                .ok_or(ServiceError::ResourceExhausted)?,
        )
        .map_err(|_| ServiceError::ResourceExhausted)?;
        let mut document = self
            .document
            .lock()
            .map_err(|_| ServiceError::Unavailable)?;
        if document.program_recovery_required
            || document.pending_program.is_some()
            || document.program_generation != current
        {
            return Err(ServiceError::InvalidRequest);
        }
        let mut candidate = document.clone();
        candidate.pending_program = Some(PendingProgramRollback {
            operation_identity: operation.identity().as_str().to_owned(),
            evidence_sha256: operation.evidence_digest().bytes(),
            previous_generation: current,
            attempted_generation: attempted,
            source_version,
            target_version,
        });
        store_document(&self.store, &candidate)?;
        *document = candidate;
        Ok(attempted)
    }

    fn reconcile_program(&self, install_root: &Path) -> Result<bool, ServiceError> {
        let observed = status(install_root).map_err(|_| ServiceError::Unavailable)?;
        let mut document = self
            .document
            .lock()
            .map_err(|_| ServiceError::Unavailable)?;
        let Some(pending) = document.pending_program.clone() else {
            return Ok(false);
        };
        let active = observed.active_version().ok_or(ServiceError::Unavailable)?;
        let mut candidate = document.clone();
        if observed.is_healthy() && active == pending.target_version {
            candidate.program_generation = pending.attempted_generation;
            candidate.pending_program = None;
            candidate.program_recovery_required = false;
            upsert_receipt(
                &mut candidate.receipts,
                DurableRecoveryReceipt::Program {
                    operation_identity: pending.operation_identity,
                    evidence_sha256: pending.evidence_sha256,
                    active_generation: pending.attempted_generation,
                    active_version: pending.target_version,
                },
            );
            store_document(&self.store, &candidate)?;
            *document = candidate;
            return Ok(true);
        }
        if observed.is_healthy() && active == pending.source_version {
            candidate.pending_program = None;
            candidate.program_recovery_required = false;
            store_document(&self.store, &candidate)?;
            *document = candidate;
            return Ok(false);
        }
        candidate.program_recovery_required = true;
        store_document(&self.store, &candidate)?;
        *document = candidate;
        Err(ServiceError::Unavailable)
    }
}

impl fmt::Debug for DurableRecoveryState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DurableRecoveryState([TWO-COPY SELECTOR AND RECEIPTS])")
    }
}

fn store_document(
    store: &LocalAuthorityStateStore,
    document: &RecoveryDocument,
) -> Result<(), ServiceError> {
    let encoded = serde_json::to_vec(document).map_err(|_| ServiceError::Internal)?;
    if encoded.len() > LocalAuthorityStateStore::maximum_payload_bytes() {
        return Err(ServiceError::ResourceExhausted);
    }
    store.store(&encoded).map_err(|_| ServiceError::Unavailable)
}

fn upsert_receipt(receipts: &mut Vec<DurableRecoveryReceipt>, receipt: DurableRecoveryReceipt) {
    receipts.retain(|existing| existing.operation_identity() != receipt.operation_identity());
    if receipts.len() == MAXIMUM_RECEIPTS {
        receipts.remove(0);
    }
    receipts.push(receipt);
}

/// Workspace transition that publishes a durable selector and requests outer-process shutdown.
pub struct SupervisorRestartWorkspaceTransition {
    state: Arc<DurableRecoveryState>,
    hooks: Arc<dyn InstalledServiceRecoveryHooks>,
    installation_id: InstallationId,
}

impl SupervisorRestartWorkspaceTransition {
    /// Binds the durable selector to the installed service supervisor authority.
    #[must_use]
    pub fn new(
        state: Arc<DurableRecoveryState>,
        hooks: Arc<dyn InstalledServiceRecoveryHooks>,
        installation_id: InstallationId,
    ) -> Self {
        Self {
            state,
            hooks,
            installation_id,
        }
    }
}

#[async_trait]
impl WorkspaceRestartTransition for SupervisorRestartWorkspaceTransition {
    async fn drain_and_reconcile(&self, deadline: Instant) -> Result<(), LifecycleError> {
        if deadline <= Instant::now() {
            return Err(LifecycleError::InvalidTimeout);
        }
        self.hooks.drain_and_reconcile(deadline).await
    }

    async fn request_restart(
        &self,
        workspace_id: WorkspaceId,
        deadline: Instant,
    ) -> Result<WorkspaceRuntimeIdentity, LifecycleError> {
        if deadline <= Instant::now() {
            return Err(LifecycleError::InvalidTimeout);
        }
        let selected = self
            .state
            .stage_workspace(workspace_id)
            .map_err(map_service_lifecycle)?;
        let runtime = selected.to_runtime(self.installation_id)?;
        self.hooks.request_restart(runtime)?;
        Ok(selected)
    }
}

impl fmt::Debug for SupervisorRestartWorkspaceTransition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SupervisorRestartWorkspaceTransition([DURABLE RESTART SELECTOR])")
    }
}

fn map_service_lifecycle(error: ServiceError) -> LifecycleError {
    match error {
        ServiceError::InvalidRequest => LifecycleError::InvalidTarget,
        ServiceError::ResourceExhausted => LifecycleError::GenerationExhausted,
        _ => LifecycleError::AuthorityUnavailable,
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ActivityFacts {
    running_jobs: u32,
    active_sources: u32,
    paper_execution_active: bool,
    execution_reconciliation_pending: bool,
    connected_clients: u32,
}

struct RestorePreviewPlan {
    evidence_sha256: [u8; 32],
    target: WorkspaceId,
    approval: WorkspaceSwitchApproval,
}

struct ProgramPreviewPlan {
    evidence_sha256: [u8; 32],
    current: ProgramGeneration,
    target_version: String,
}

struct WorkspacePreviewPlan {
    target: WorkspaceId,
}

enum RecoveryPlan {
    Restore {
        operation: PreparedOperation,
        target: WorkspaceId,
        approval: WorkspaceSwitchApproval,
    },
    WorkspaceSwitch {
        operation: PreparedOperation,
        target: WorkspaceId,
        approval: WorkspaceSwitchApproval,
    },
    ProgramRollback {
        operation: PreparedOperation,
        current: ProgramGeneration,
        target_version: String,
    },
}

impl RecoveryPlan {
    fn operation(&self) -> &PreparedOperation {
        match self {
            Self::Restore { operation, .. }
            | Self::WorkspaceSwitch { operation, .. }
            | Self::ProgramRollback { operation, .. } => operation,
        }
    }
}

#[derive(Default)]
struct PlanState {
    restore_previews: BTreeMap<[u8; 32], RestorePreviewPlan>,
    workspace_previews: BTreeMap<[u8; 32], WorkspacePreviewPlan>,
    program_previews: BTreeMap<[u8; 32], ProgramPreviewPlan>,
    operations: BTreeMap<SourceIdentifier, RecoveryPlan>,
}

/// Concrete installed recovery authority used by the operations service and recovery job runner.
pub struct InstalledRecoveryOperations {
    backup_repository_root: PathBuf,
    workspace_repository_root: PathBuf,
    backup_limits: AnalyticalBackupLimits,
    minimum_schema_version: u32,
    maximum_schema_version: u32,
    install_root: PathBuf,
    workspaces: Arc<DurableWorkspaceRegistry>,
    lifecycle: Arc<WorkspaceLifecycleAuthority>,
    transition: Arc<SupervisorRestartWorkspaceTransition>,
    restore_components: Arc<dyn ProductRestoreComponentAuthority>,
    activity: Arc<RecoveryRuntimeActivity>,
    durable: Arc<DurableRecoveryState>,
    plans: Mutex<PlanState>,
    sequence: AtomicU64,
}

impl InstalledRecoveryOperations {
    /// Binds all concrete path, workspace, lifecycle, restore, supervisor, and installer inputs.
    #[allow(
        clippy::too_many_arguments,
        reason = "every installed recovery authority and compatibility bound is explicit"
    )]
    pub fn try_new(
        backup_repository_root: PathBuf,
        workspace_repository_root: PathBuf,
        backup_limits: AnalyticalBackupLimits,
        minimum_schema_version: u32,
        maximum_schema_version: u32,
        install_root: PathBuf,
        workspaces: Arc<DurableWorkspaceRegistry>,
        lifecycle: Arc<WorkspaceLifecycleAuthority>,
        transition: Arc<SupervisorRestartWorkspaceTransition>,
        restore_components: Arc<dyn ProductRestoreComponentAuthority>,
        activity: Arc<RecoveryRuntimeActivity>,
        durable: Arc<DurableRecoveryState>,
    ) -> Result<Self, ServiceError> {
        if minimum_schema_version == 0
            || minimum_schema_version > maximum_schema_version
            || !backup_repository_root.is_absolute()
            || !workspace_repository_root.is_absolute()
            || !install_root.is_absolute()
            || !backup_repository_root.is_dir()
            || !workspace_repository_root.is_dir()
        {
            return Err(ServiceError::InvalidRequest);
        }
        durable.reconcile_program(&install_root)?;
        Ok(Self {
            backup_repository_root,
            workspace_repository_root,
            backup_limits,
            minimum_schema_version,
            maximum_schema_version,
            install_root,
            workspaces,
            lifecycle,
            transition,
            restore_components,
            activity,
            durable,
            plans: Mutex::new(PlanState::default()),
            sequence: AtomicU64::new(1),
        })
    }

    fn retain_operation(&self, plan: RecoveryPlan) -> Result<PreparedOperation, ServiceError> {
        let operation = plan.operation().clone();
        let mut plans = self.plans.lock().map_err(|_| ServiceError::Unavailable)?;
        if plans.operations.len() >= MAXIMUM_OPERATIONS
            || plans
                .operations
                .insert(operation.identity().clone(), plan)
                .is_some()
        {
            return Err(ServiceError::ResourceExhausted);
        }
        Ok(operation)
    }

    fn operation(&self, command: &RecoveryJobCommand) -> Result<RecoveryPlan, ServiceError> {
        let mut plans = self.plans.lock().map_err(|_| ServiceError::Unavailable)?;
        let plan = plans
            .operations
            .remove(command.identity())
            .ok_or(ServiceError::NotFound)?;
        if plan.operation().evidence_digest() != command.evidence_digest() {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(plan)
    }

    fn new_operation(
        &self,
        domain: &'static str,
        evidence_sha256: [u8; 32],
    ) -> Result<PreparedOperation, ServiceError> {
        if evidence_sha256 == [0; 32] {
            return Err(ServiceError::InvalidResult);
        }
        let sequence = self.sequence.fetch_add(1, Ordering::AcqRel);
        let short = lower_hex(&evidence_sha256[..8]);
        let identity = SourceIdentifier::try_from(format!("{domain}-{short}-{sequence}"))
            .map_err(|_| ServiceError::Internal)?;
        PreparedOperation::try_new(
            identity,
            EvidenceDigest::new(DigestAlgorithm::Sha256, evidence_sha256),
        )
    }

    async fn execute_workspace(
        &self,
        action: RecoveryJobAction,
        operation: PreparedOperation,
        approval: WorkspaceSwitchApproval,
        target: WorkspaceId,
        cancellation: CancellationToken,
        deadline: Instant,
        _publication: Arc<dyn LifecycleJobPublication>,
    ) -> Result<(), LifecycleJobExecutionError> {
        pre_mutation_boundary(&cancellation, deadline)?;
        let original = self
            .lifecycle
            .current()
            .map_err(|_| execution_failure("workspace-authority-unavailable", true))?;
        self.durable
            .arm_workspace(&operation, original, target)
            .map_err(|_| execution_failure("workspace-selector-unavailable", true))?;
        let timeout = remaining(deadline)
            .map_err(|_| execution_failure("workspace-deadline-elapsed", false))?;
        let handoff = self
            .lifecycle
            .request_switch(approval, &*self.transition, timeout)
            .await
            .map_err(|_| execution_failure("workspace-transition-failed", true))?;
        if handoff.previous() != original
            || handoff.candidate().workspace_id() != target
            || !matches!(
                action,
                RecoveryJobAction::RestoreWorkspace | RecoveryJobAction::SwitchWorkspace
            )
        {
            return Err(execution_failure("workspace-outcome-invalid", false));
        }
        tokio::select! {
            () = cancellation.cancelled() => Err(LifecycleJobExecutionError::Cancelled),
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                Err(execution_failure("workspace-restart-not-observed", true))
            }
        }
    }

    async fn execute_program(
        &self,
        operation: PreparedOperation,
        current: ProgramGeneration,
        target_version: String,
        cancellation: CancellationToken,
        deadline: Instant,
        publication: Arc<dyn LifecycleJobPublication>,
    ) -> Result<(), LifecycleJobExecutionError> {
        pre_mutation_boundary(&cancellation, deadline)?;
        if self
            .durable
            .program_generation()
            .map_err(|_| execution_failure("program-authority-unavailable", true))?
            != current
        {
            return Err(execution_failure("program-approval-stale", false));
        }
        let installed = status(&self.install_root)
            .map_err(|_| execution_failure("installer-status-unavailable", true))?;
        let source_version = installed
            .active_version()
            .ok_or_else(|| execution_failure("program-not-installed", false))?
            .to_owned();
        if !installed.is_healthy() || installed.previous_version() != Some(target_version.as_str())
        {
            return Err(execution_failure("known-good-program-unavailable", false));
        }
        let result = RecoveryJobRunner::try_result_reference(
            operation.identity().clone(),
            operation.evidence_digest(),
            Vec::new(),
        )
        .map_err(|_| execution_failure("recovery-result-invalid", false))?;
        publication.prepare_and_claim(result)?;
        self.durable
            .begin_program(&operation, current, source_version, target_version.clone())
            .map_err(|_| execution_failure("program-receipt-unavailable", true))?;
        // The installer owns atomic selector replacement and exact known-good revalidation. Once
        // invoked it must run to a terminal installer state; cancellation/deadline are observed at
        // the safe pre-mutation boundary above rather than by abandoning an in-flight selector.
        let root = self.install_root.clone();
        let result = tokio::task::spawn_blocking(move || rollback(RollbackRequest::new(root)))
            .await
            .map_err(|_| execution_failure("installer-worker-failed", true))?;
        if result.is_err() {
            let committed = self
                .durable
                .reconcile_program(&self.install_root)
                .map_err(|_| execution_failure("program-recovery-required", true))?;
            if !committed {
                return Err(execution_failure("program-rollback-failed", true));
            }
        } else if !self
            .durable
            .reconcile_program(&self.install_root)
            .map_err(|_| execution_failure("program-receipt-unavailable", true))?
        {
            return Err(execution_failure("program-rollback-indeterminate", true));
        }
        publication.commit_succeeded();
        Ok(())
    }
}

#[async_trait]
impl ManagedRecoveryOperations for InstalledRecoveryOperations {
    fn workspace_activity(
        &self,
        target: WorkspaceId,
    ) -> Result<WorkspaceActivitySnapshot, ServiceError> {
        let activity = self.activity.snapshot(target)?;
        let preview = self
            .lifecycle
            .preview_switch(target, activity)
            .map_err(|_| ServiceError::InvalidRequest)?;
        if let Ok(approval) = preview.try_approve() {
            let evidence_sha256 = Sha256::digest(format!("{approval:?}").as_bytes()).into();
            let mut plans = self.plans.lock().map_err(|_| ServiceError::Unavailable)?;
            if plans.workspace_previews.len() >= MAXIMUM_PREVIEWS {
                return Err(ServiceError::ResourceExhausted);
            }
            plans
                .workspace_previews
                .insert(evidence_sha256, WorkspacePreviewPlan { target });
        }
        Ok(activity)
    }

    async fn preview_restore(
        &self,
        manifest: ProductBackupManifest,
        active: WorkspaceRuntimeIdentity,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<RestorePreviewEvidence, ServiceError> {
        pre_service_boundary(&cancellation, deadline)?;
        if self
            .lifecycle
            .current()
            .map_err(|_| ServiceError::Unavailable)?
            != active
        {
            return Err(ServiceError::InvalidRequest);
        }
        manifest
            .verify()
            .map_err(|_| ServiceError::InvalidRequest)?;
        let required_disk_bytes = manifest_required_bytes(&manifest)?;
        let available_disk_bytes = fs2::available_space(&self.workspace_repository_root)
            .map_err(|_| ServiceError::Unavailable)?;
        let backup_root = self
            .backup_repository_root
            .join(lower_hex(&manifest.backup_id()));
        let backup_paths =
            LocalPaths::open_existing(backup_root).map_err(|_| ServiceError::Unavailable)?;
        let location = AnalyticalBackupLocation::try_new(
            backup_paths
                .catalog()
                .map_err(|_| ServiceError::Unavailable)?
                .clone(),
            backup_paths
                .artifacts()
                .map_err(|_| ServiceError::Unavailable)?
                .clone(),
        )
        .map_err(|_| ServiceError::InvalidRequest)?;
        let limits = self.backup_limits;
        let verify_manifest = manifest.clone();
        let verify_cancellation = cancellation.clone();
        let verified = bounded(
            deadline,
            &cancellation,
            tokio::task::spawn_blocking(move || {
                ProductBackupService::open_verified(
                    location,
                    verify_manifest,
                    limits,
                    &verify_cancellation,
                )
            }),
        )
        .await?
        .map_err(|_| ServiceError::Unavailable)?
        .map_err(|_| ServiceError::InvalidRequest)?;
        let prepared = bounded(
            deadline,
            &cancellation,
            verified.stage_restore(
                active.workspace_id(),
                &*self.restore_components,
                &cancellation,
            ),
        )
        .await?
        .map_err(|_| ServiceError::InvalidRequest)?;
        let workspace = prepared.workspace().clone();
        let target = workspace.workspace_id();
        let facts = activity_facts(self.activity.snapshot(active.workspace_id())?)?;
        let schema_compatible = workspace.schema_version() >= self.minimum_schema_version
            && workspace.schema_version() <= self.maximum_schema_version;
        let snapshot = WorkspaceActivitySnapshot::new(
            facts.running_jobs,
            facts.active_sources,
            facts.paper_execution_active,
            facts.execution_reconciliation_pending,
            facts.connected_clients,
            available_disk_bytes,
            required_disk_bytes,
            schema_compatible,
        );
        let switch_preview = self
            .lifecycle
            .preview_switch(target, snapshot)
            .map_err(|_| ServiceError::InvalidRequest)?;
        let blockers = restore_blockers(
            facts,
            available_disk_bytes,
            required_disk_bytes,
            schema_compatible,
        )?;
        let evidence = RestorePreviewEvidence::try_new(
            manifest,
            active,
            available_disk_bytes,
            required_disk_bytes,
            schema_compatible,
            blockers,
        )?;
        let evidence_sha256 = serialized_sha256(&evidence)?;
        if let Ok(approval) = switch_preview.try_approve() {
            self.workspaces
                .register_prepared(workspace)
                .map_err(|_| ServiceError::Unavailable)?;
            drop(prepared);
            let retained = {
                let mut plans = self.plans.lock().map_err(|_| ServiceError::Unavailable)?;
                if plans.restore_previews.len() >= MAXIMUM_PREVIEWS {
                    false
                } else {
                    plans.restore_previews.insert(
                        evidence_sha256,
                        RestorePreviewPlan {
                            evidence_sha256,
                            target,
                            approval,
                        },
                    );
                    true
                }
            };
            if !retained {
                self.restore_components
                    .abandon(target, &cancellation)
                    .await
                    .map_err(|_| ServiceError::Unavailable)?;
                return Err(ServiceError::ResourceExhausted);
            }
        } else {
            drop(prepared);
            self.restore_components
                .abandon(target, &cancellation)
                .await
                .map_err(|_| ServiceError::Unavailable)?;
        }
        Ok(evidence)
    }

    fn prepare_restore(
        &self,
        evidence: RestorePreviewEvidence,
    ) -> Result<PreparedOperation, ServiceError> {
        let evidence_sha256 = serialized_sha256(&evidence)?;
        let preview = self
            .plans
            .lock()
            .map_err(|_| ServiceError::Unavailable)?
            .restore_previews
            .remove(&evidence_sha256)
            .filter(|preview| preview.evidence_sha256 == evidence_sha256)
            .ok_or(ServiceError::InvalidRequest)?;
        let operation = self.new_operation("restore", evidence_sha256)?;
        self.retain_operation(RecoveryPlan::Restore {
            operation,
            target: preview.target,
            approval: preview.approval,
        })
    }

    fn prepare_workspace_switch(
        &self,
        approval: WorkspaceSwitchApproval,
    ) -> Result<PreparedOperation, ServiceError> {
        let evidence_sha256 = Sha256::digest(format!("{approval:?}").as_bytes()).into();
        let preview = self
            .plans
            .lock()
            .map_err(|_| ServiceError::Unavailable)?
            .workspace_previews
            .remove(&evidence_sha256)
            .ok_or(ServiceError::InvalidRequest)?;
        let operation = self.new_operation("workspace-switch", evidence_sha256)?;
        self.retain_operation(RecoveryPlan::WorkspaceSwitch {
            operation,
            target: preview.target,
            approval,
        })
    }

    fn preview_program_rollback(
        &self,
        current: ProgramGeneration,
    ) -> Result<ProgramRollbackPreviewEvidence, ServiceError> {
        if self.durable.program_generation()? != current {
            return Err(ServiceError::InvalidRequest);
        }
        let installed = status(&self.install_root).map_err(|_| ServiceError::Unavailable)?;
        let target_version = installed
            .previous_version()
            .ok_or(ServiceError::NotFound)?
            .to_owned();
        let active = self
            .lifecycle
            .current()
            .map_err(|_| ServiceError::Unavailable)?;
        let facts = activity_facts(self.activity.snapshot(active.workspace_id())?)?;
        let active_work_blocked = facts.running_jobs > 0
            || facts.active_sources > 0
            || facts.paper_execution_active
            || facts.execution_reconciliation_pending;
        // The installer performs byte-for-byte retained-cache and immutable-tree revalidation
        // again during rollback. Preview admits only a healthy installed selector with a retained
        // previous version; execution never trusts this display fact as activation authority.
        let known_good_verified = installed.is_healthy();
        let evidence = ProgramRollbackPreviewEvidence::try_new(
            current,
            target_version.clone(),
            active_work_blocked,
            known_good_verified,
        )?;
        let evidence_sha256 = serialized_sha256(&evidence)?;
        let mut plans = self.plans.lock().map_err(|_| ServiceError::Unavailable)?;
        if plans.program_previews.len() >= MAXIMUM_PREVIEWS {
            return Err(ServiceError::ResourceExhausted);
        }
        plans.program_previews.insert(
            evidence_sha256,
            ProgramPreviewPlan {
                evidence_sha256,
                current,
                target_version,
            },
        );
        Ok(evidence)
    }

    fn prepare_program_rollback(
        &self,
        evidence: ProgramRollbackPreviewEvidence,
    ) -> Result<PreparedOperation, ServiceError> {
        let evidence_sha256 = serialized_sha256(&evidence)?;
        let preview = self
            .plans
            .lock()
            .map_err(|_| ServiceError::Unavailable)?
            .program_previews
            .remove(&evidence_sha256)
            .filter(|preview| preview.evidence_sha256 == evidence_sha256)
            .ok_or(ServiceError::InvalidRequest)?;
        if evidence.current_generation() != preview.current
            || evidence.target_version() != preview.target_version
        {
            return Err(ServiceError::InvalidRequest);
        }
        let operation = self.new_operation("program-rollback", evidence_sha256)?;
        self.retain_operation(RecoveryPlan::ProgramRollback {
            operation,
            current: preview.current,
            target_version: preview.target_version,
        })
    }

    fn revoke(&self, operation: &PreparedOperation) {
        let Ok(mut plans) = self.plans.lock() else {
            return;
        };
        if plans
            .operations
            .get(operation.identity())
            .is_some_and(|plan| plan.operation().evidence_digest() == operation.evidence_digest())
        {
            plans.operations.remove(operation.identity());
        }
    }

    async fn execute(
        &self,
        command: RecoveryJobCommand,
        cancellation: CancellationToken,
        deadline: Instant,
        publication: Arc<dyn LifecycleJobPublication>,
    ) -> Result<(), LifecycleJobExecutionError> {
        let action = command.action();
        let plan = self
            .operation(&command)
            .map_err(|_| execution_failure("recovery-plan-unavailable", false))?;
        match (action, plan) {
            (
                RecoveryJobAction::RestoreWorkspace,
                RecoveryPlan::Restore {
                    operation,
                    target,
                    approval,
                },
            ) => {
                self.execute_workspace(
                    action,
                    operation,
                    approval,
                    target,
                    cancellation,
                    deadline,
                    publication,
                )
                .await
            }
            (
                RecoveryJobAction::SwitchWorkspace,
                RecoveryPlan::WorkspaceSwitch {
                    operation,
                    target,
                    approval,
                },
            ) => {
                self.execute_workspace(
                    action,
                    operation,
                    approval,
                    target,
                    cancellation,
                    deadline,
                    publication,
                )
                .await
            }
            (
                RecoveryJobAction::RollbackProgram,
                RecoveryPlan::ProgramRollback {
                    operation,
                    current,
                    target_version,
                },
            ) => {
                self.execute_program(
                    operation,
                    current,
                    target_version,
                    cancellation,
                    deadline,
                    publication,
                )
                .await
            }
            _ => Err(execution_failure("recovery-action-mismatch", false)),
        }
    }
}

impl fmt::Debug for InstalledRecoveryOperations {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("InstalledRecoveryOperations([RETAINED EXACT RECOVERY PLANS])")
    }
}

fn activity_facts(snapshot: WorkspaceActivitySnapshot) -> Result<ActivityFacts, ServiceError> {
    serde_json::from_value(serde_json::to_value(snapshot).map_err(|_| ServiceError::Internal)?)
        .map_err(|_| ServiceError::InvalidResult)
}

fn restore_blockers(
    facts: ActivityFacts,
    available_disk_bytes: u64,
    required_disk_bytes: u64,
    schema_compatible: bool,
) -> Result<Vec<SourceIdentifier>, ServiceError> {
    let mut blockers = Vec::new();
    if facts.running_jobs > 0 {
        blockers.push("running-jobs");
    }
    if facts.active_sources > 0 {
        blockers.push("active-sources");
    }
    if facts.paper_execution_active {
        blockers.push("paper-execution-active");
    }
    if facts.execution_reconciliation_pending {
        blockers.push("execution-reconciliation-pending");
    }
    if available_disk_bytes < required_disk_bytes {
        blockers.push("insufficient-disk");
    }
    if !schema_compatible {
        blockers.push("incompatible-schema");
    }
    blockers
        .into_iter()
        .map(|blocker| SourceIdentifier::try_from(blocker).map_err(|_| ServiceError::Internal))
        .collect()
}

fn manifest_required_bytes(manifest: &ProductBackupManifest) -> Result<u64, ServiceError> {
    let analytical = manifest.analytical_receipt();
    let mut required = analytical
        .catalog_backup()
        .byte_length()
        .checked_add(analytical.artifact_bytes())
        .ok_or(ServiceError::ResourceExhausted)?;
    let value = serde_json::to_value(manifest).map_err(|_| ServiceError::Internal)?;
    let components = value
        .get("components")
        .and_then(serde_json::Value::as_array)
        .ok_or(ServiceError::InvalidResult)?;
    for component in components {
        let bytes = component
            .get("byteLength")
            .and_then(serde_json::Value::as_u64)
            .ok_or(ServiceError::InvalidResult)?;
        required = required
            .checked_add(bytes)
            .ok_or(ServiceError::ResourceExhausted)?;
    }
    Ok(required)
}

async fn bounded<F, T>(
    deadline: Instant,
    cancellation: &CancellationToken,
    future: F,
) -> Result<T, ServiceError>
where
    F: std::future::Future<Output = T>,
{
    if cancellation.is_cancelled() {
        return Err(ServiceError::Cancelled);
    }
    if deadline <= Instant::now() {
        return Err(ServiceError::DeadlineExceeded);
    }
    tokio::select! {
        result = future => Ok(result),
        () = cancellation.cancelled() => Err(ServiceError::Cancelled),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            Err(ServiceError::DeadlineExceeded)
        }
    }
}

fn serialized_sha256(value: &impl Serialize) -> Result<[u8; 32], ServiceError> {
    serde_json::to_vec(value)
        .map(|encoded| Sha256::digest(encoded).into())
        .map_err(|_| ServiceError::Internal)
}

fn lower_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn remaining(deadline: Instant) -> Result<Duration, ServiceError> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|duration| !duration.is_zero())
        .ok_or(ServiceError::DeadlineExceeded)
}

fn pre_service_boundary(
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<(), ServiceError> {
    if cancellation.is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if deadline <= Instant::now() {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn pre_mutation_boundary(
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<(), LifecycleJobExecutionError> {
    if cancellation.is_cancelled() {
        Err(LifecycleJobExecutionError::Cancelled)
    } else if deadline <= Instant::now() {
        Err(execution_failure("recovery-deadline-elapsed", false))
    } else {
        Ok(())
    }
}

fn execution_failure(diagnostic: &'static str, retryable: bool) -> LifecycleJobExecutionError {
    match SourceIdentifier::try_from(diagnostic) {
        Ok(diagnostic) => LifecycleJobExecutionError::failed(diagnostic, retryable),
        Err(_error) => LifecycleJobExecutionError::failed(
            SourceIdentifier::try_from("recovery-failure")
                .expect("code-owned recovery diagnostic must be valid"),
            false,
        ),
    }
}
