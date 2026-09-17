//! Closed, path-free contracts for installed-product operational authorities.

use std::{fmt, sync::Arc, time::Instant};

use async_trait::async_trait;
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp};
use market_squawk_runtime::WorkspaceId;
use market_squawk_services::ServiceError;
use serde::Serialize;
use tokio_util::sync::CancellationToken;

use super::super::{
    backup::{BackupRetentionApproval, ProductBackupManifest},
    lifecycle::{
        ProgramGeneration, StagedUpdateCandidate, UpdateActivitySnapshot, UpdateApproval,
        WorkspaceActivitySnapshot, WorkspaceRuntimeIdentity, WorkspaceSwitchApproval,
    },
    settings::{SettingsChangeApproval, SettingsReceipt},
};
use crate::jobs::{
    BackupJobCommand, LifecycleJobExecutionError, LifecycleJobPublication, RecoveryJobCommand,
    UpdateJobCommand,
};

const MAXIMUM_RESTORE_BLOCKERS: usize = 64;

/// One complete path-free view of the installed service and active workspace.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RuntimeStatusSnapshot {
    ready: bool,
    workspace: WorkspaceRuntimeIdentity,
    workspace_schema_version: u32,
    available_disk_bytes: u64,
    running_jobs: u32,
    running_mutation_jobs: u32,
    active_sources: u32,
    connected_clients: u32,
    paper_execution_active: bool,
    execution_reconciliation_pending: bool,
}

impl RuntimeStatusSnapshot {
    /// Captures a fully bound, ceiling-validated sample from the installed runtime authorities.
    #[allow(
        clippy::too_many_arguments,
        reason = "each operational fact is independently authoritative"
    )]
    pub(crate) const fn ready(
        workspace: WorkspaceRuntimeIdentity,
        workspace_schema_version: u32,
        available_disk_bytes: u64,
        running_jobs: u32,
        running_mutation_jobs: u32,
        active_sources: u32,
        connected_clients: u32,
        paper_execution_active: bool,
        execution_reconciliation_pending: bool,
    ) -> Self {
        Self {
            ready: true,
            workspace,
            workspace_schema_version,
            available_disk_bytes,
            running_jobs,
            running_mutation_jobs,
            active_sources,
            connected_clients,
            paper_execution_active,
            execution_reconciliation_pending,
        }
    }
}

/// Synchronous authority for the current installed runtime status.
pub trait RuntimeStatusAuthority: fmt::Debug + Send + Sync {
    /// Samples one complete fact set for the exact active workspace fence.
    fn snapshot(
        &self,
        active: WorkspaceRuntimeIdentity,
    ) -> Result<RuntimeStatusSnapshot, ServiceError>;
}

/// Exact path-free identity of one operation retained by a concrete lifecycle authority.
#[derive(Clone, Debug)]
pub struct PreparedOperation {
    identity: SourceIdentifier,
    evidence_digest: EvidenceDigest,
}

impl PreparedOperation {
    /// Admits one concrete operation only after its complete plan has been retained.
    pub fn try_new(
        identity: SourceIdentifier,
        evidence_digest: EvidenceDigest,
    ) -> Result<Self, ServiceError> {
        if evidence_digest.algorithm() != DigestAlgorithm::Sha256
            || evidence_digest.bytes() == [0; 32]
        {
            return Err(ServiceError::InvalidResult);
        }
        Ok(Self {
            identity,
            evidence_digest,
        })
    }

    /// Returns the exact retained operation identity.
    #[must_use]
    pub const fn identity(&self) -> &SourceIdentifier {
        &self.identity
    }

    /// Returns the digest of the retained canonical plan.
    #[must_use]
    pub const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }
}

/// Bounded restore evidence shown before an exact recovery plan can be approved.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RestorePreviewEvidence {
    backup: ProductBackupManifest,
    active: WorkspaceRuntimeIdentity,
    available_disk_bytes: u64,
    required_disk_bytes: u64,
    schema_compatible: bool,
    blockers: Vec<SourceIdentifier>,
}

impl RestorePreviewEvidence {
    /// Creates evidence only for a disk- and schema-qualified restore plan.
    pub fn try_new(
        backup: ProductBackupManifest,
        active: WorkspaceRuntimeIdentity,
        available_disk_bytes: u64,
        required_disk_bytes: u64,
        schema_compatible: bool,
        blockers: Vec<SourceIdentifier>,
    ) -> Result<Self, ServiceError> {
        if blockers.len() > MAXIMUM_RESTORE_BLOCKERS {
            return Err(ServiceError::InvalidResult);
        }
        Ok(Self {
            backup,
            active,
            available_disk_bytes,
            required_disk_bytes,
            schema_compatible,
            blockers,
        })
    }

    pub(super) fn can_approve(&self) -> bool {
        self.blockers.is_empty()
            && self.schema_compatible
            && self.available_disk_bytes >= self.required_disk_bytes
    }

    /// Returns the exact verified backup selected for restore.
    #[must_use]
    pub const fn backup(&self) -> &ProductBackupManifest {
        &self.backup
    }

    /// Returns the active workspace fence captured by this preview.
    #[must_use]
    pub const fn active(&self) -> WorkspaceRuntimeIdentity {
        self.active
    }
}

/// Program rollback evidence independent of data/workspace generations.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProgramRollbackPreviewEvidence {
    current_generation: ProgramGeneration,
    target_version: String,
    active_work_blocked: bool,
    known_good_verified: bool,
}

impl ProgramRollbackPreviewEvidence {
    /// Admits a bounded known-good rollback description.
    pub fn try_new(
        current_generation: ProgramGeneration,
        target_version: impl Into<String>,
        active_work_blocked: bool,
        known_good_verified: bool,
    ) -> Result<Self, ServiceError> {
        let target_version = target_version.into();
        if target_version.is_empty()
            || target_version.len() > 128
            || target_version.chars().any(char::is_control)
        {
            return Err(ServiceError::InvalidResult);
        }
        Ok(Self {
            current_generation,
            target_version,
            active_work_blocked,
            known_good_verified,
        })
    }

    pub(super) fn can_approve(&self) -> bool {
        !self.active_work_blocked && self.known_good_verified
    }

    /// Returns the program generation fenced by this preview.
    #[must_use]
    pub const fn current_generation(&self) -> ProgramGeneration {
        self.current_generation
    }

    /// Returns the verified known-good version identity.
    #[must_use]
    pub fn target_version(&self) -> &str {
        &self.target_version
    }
}

/// Package-derived availability of the installed trusted-update channel.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UpdateAvailabilityEvidence {
    /// The immutable package contains an admitted public root and repository contract.
    Available,
    /// This process is a source or development execution rather than an installed release.
    SourceOrDevelopmentExecution,
    /// The installed package was intentionally built without production update trust material.
    ProductionSigningMaterialUnavailable,
}

/// Current trusted-update state without paths, URLs, or signing material.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateStatusEvidence {
    availability: UpdateAvailabilityEvidence,
    current_generation: ProgramGeneration,
    known_good_version: String,
    staged_candidate: Option<StagedUpdateCandidate>,
    last_checked_at: Option<Timestamp>,
    recovery_required: bool,
}

impl UpdateStatusEvidence {
    /// Creates bounded status derived from the trusted update and installer authorities.
    pub fn try_new(
        availability: UpdateAvailabilityEvidence,
        current_generation: ProgramGeneration,
        known_good_version: impl Into<String>,
        staged_candidate: Option<StagedUpdateCandidate>,
        last_checked_at: Option<Timestamp>,
        recovery_required: bool,
    ) -> Result<Self, ServiceError> {
        let known_good_version = known_good_version.into();
        if known_good_version.is_empty()
            || known_good_version.len() > 128
            || known_good_version.chars().any(char::is_control)
        {
            return Err(ServiceError::InvalidResult);
        }
        Ok(Self {
            availability,
            current_generation,
            known_good_version,
            staged_candidate,
            last_checked_at,
            recovery_required,
        })
    }
}

/// Candidate and exact runtime facts returned only after trusted metadata and byte admission.
#[derive(Clone, Debug)]
pub struct TrustedStagedUpdate {
    pub(super) candidate: StagedUpdateCandidate,
    pub(super) activity: UpdateActivitySnapshot,
}

impl TrustedStagedUpdate {
    /// Binds a trusted staged candidate to current compatibility evidence.
    #[must_use]
    pub const fn new(candidate: StagedUpdateCandidate, activity: UpdateActivitySnapshot) -> Self {
        Self {
            candidate,
            activity,
        }
    }
}

/// Managed backup repository and materialization authority.
#[async_trait]
pub trait ManagedBackupOperations: fmt::Debug + Send + Sync {
    async fn prepare_create(
        &self,
        active: WorkspaceRuntimeIdentity,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<PreparedOperation, ServiceError>;

    async fn prepare_verify(
        &self,
        manifest: ProductBackupManifest,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<PreparedOperation, ServiceError>;

    fn prepare_retention(
        &self,
        approval: BackupRetentionApproval,
    ) -> Result<PreparedOperation, ServiceError>;

    fn revoke(&self, operation: &PreparedOperation);

    async fn execute(
        &self,
        command: BackupJobCommand,
        cancellation: CancellationToken,
        deadline: Instant,
        publication: Arc<dyn LifecycleJobPublication>,
    ) -> Result<(), LifecycleJobExecutionError>;
}

/// Managed restore, workspace transition, and program-rollback authority.
#[async_trait]
pub trait ManagedRecoveryOperations: fmt::Debug + Send + Sync {
    fn workspace_activity(
        &self,
        target: WorkspaceId,
    ) -> Result<WorkspaceActivitySnapshot, ServiceError>;

    async fn preview_restore(
        &self,
        manifest: ProductBackupManifest,
        active: WorkspaceRuntimeIdentity,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<RestorePreviewEvidence, ServiceError>;

    fn prepare_restore(
        &self,
        evidence: RestorePreviewEvidence,
    ) -> Result<PreparedOperation, ServiceError>;

    fn prepare_workspace_switch(
        &self,
        approval: WorkspaceSwitchApproval,
    ) -> Result<PreparedOperation, ServiceError>;

    fn preview_program_rollback(
        &self,
        current: ProgramGeneration,
    ) -> Result<ProgramRollbackPreviewEvidence, ServiceError>;

    fn prepare_program_rollback(
        &self,
        evidence: ProgramRollbackPreviewEvidence,
    ) -> Result<PreparedOperation, ServiceError>;

    fn revoke(&self, operation: &PreparedOperation);

    async fn execute(
        &self,
        command: RecoveryJobCommand,
        cancellation: CancellationToken,
        deadline: Instant,
        publication: Arc<dyn LifecycleJobPublication>,
    ) -> Result<(), LifecycleJobExecutionError>;
}

/// Trusted metadata, immutable download, and staged update authority.
#[async_trait]
pub trait ManagedUpdateOperations: fmt::Debug + Send + Sync {
    fn status(&self, current: ProgramGeneration) -> Result<UpdateStatusEvidence, ServiceError>;

    async fn check_and_stage(
        &self,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<TrustedStagedUpdate, ServiceError>;

    fn current_staged(&self) -> Result<TrustedStagedUpdate, ServiceError>;

    fn prepare_update(&self, approval: UpdateApproval) -> Result<PreparedOperation, ServiceError>;

    fn revoke(&self, operation: &PreparedOperation);

    async fn execute(
        &self,
        command: UpdateJobCommand,
        cancellation: CancellationToken,
        deadline: Instant,
        publication: Arc<dyn LifecycleJobPublication>,
    ) -> Result<(), LifecycleJobExecutionError>;
}

/// Settings persistence and lifecycle authority for applied changes and rollback.
pub trait ManagedSettingsOperations: fmt::Debug + Send + Sync {
    fn apply_change(
        &self,
        approval: SettingsChangeApproval,
    ) -> Result<SettingsReceipt, ServiceError>;

    fn preview_rollback(
        &self,
        expected_revision: u64,
        target_revision: u64,
    ) -> Result<ManagedSettingsRollbackPreview, ServiceError>;

    fn apply_rollback(
        &self,
        approval: ManagedSettingsRollbackApproval,
    ) -> Result<SettingsReceipt, ServiceError>;
}

/// Exact rollback evidence whose target existence was verified by the settings authority.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagedSettingsRollbackPreview {
    current_revision: u64,
    target_revision: u64,
    restart_required: bool,
    digest: [u8; 32],
}

impl ManagedSettingsRollbackPreview {
    /// Creates a preview after the target revision and resulting values were validated.
    pub fn try_new(
        current_revision: u64,
        target_revision: u64,
        restart_required: bool,
        digest: [u8; 32],
    ) -> Result<Self, ServiceError> {
        if current_revision == 0
            || target_revision == 0
            || target_revision >= current_revision
            || digest == [0; 32]
        {
            return Err(ServiceError::InvalidResult);
        }
        Ok(Self {
            current_revision,
            target_revision,
            restart_required,
            digest,
        })
    }

    pub(super) fn approve(self) -> ManagedSettingsRollbackApproval {
        ManagedSettingsRollbackApproval {
            current_revision: self.current_revision,
            target_revision: self.target_revision,
            digest: self.digest,
        }
    }
}

/// Non-serializable settings rollback approval.
#[derive(Debug)]
pub struct ManagedSettingsRollbackApproval {
    current_revision: u64,
    target_revision: u64,
    digest: [u8; 32],
}

impl ManagedSettingsRollbackApproval {
    /// Returns the revision that must still be active.
    #[must_use]
    pub const fn current_revision(&self) -> u64 {
        self.current_revision
    }

    /// Returns the retained historical revision selected for rollback.
    #[must_use]
    pub const fn target_revision(&self) -> u64 {
        self.target_revision
    }

    /// Returns the digest of the authority-validated rollback result.
    #[must_use]
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}
