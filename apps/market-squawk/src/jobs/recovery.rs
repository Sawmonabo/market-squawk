//! Durable job adapter for explicit workspace restore/switch and program rollback.

use std::{fmt, sync::Arc, time::Duration};

use async_trait::async_trait;
use market_squawk_domain::{EvidenceDigest, SourceIdentifier, Timestamp};
use market_squawk_jobs::{
    JobCompletion, JobRecoveryDisposition, JobResultReference, JobRunContext, JobRunError,
    JobRunner,
};
use market_squawk_services::ArtifactReference;

use crate::application::job::JobAdmission;

use super::backup::{
    LifecycleJobAuthority, LifecycleJobCommand, LifecycleJobRunnerError,
    LifecycleOperationJobRunner, lifecycle_result_reference,
};

const KIND: &str = "operations.recovery.v1";
const INPUT_AUTHORITY: &str = "operations.recovery-input.v1";
const RESULT_AUTHORITY: &str = "operations.recovery-result.v1";
const RESULT_AUTHORITY_IDENTITY: &str = "product-recovery-authority-v1";

/// Closed recovery operation admitted after preview and explicit approval.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryJobAction {
    /// Stage an exact backup as a fresh workspace, then switch through generation fencing.
    RestoreWorkspace,
    /// Switch to an already prepared workspace through generation fencing.
    SwitchWorkspace,
    /// Revalidate and reactivate the retained known-good program generation.
    RollbackProgram,
}

/// Opaque handle to one application-owned, approval-bound recovery plan.
#[derive(Debug)]
pub struct RecoveryJobCommand {
    action: RecoveryJobAction,
    identity: SourceIdentifier,
    evidence_digest: EvidenceDigest,
}

impl RecoveryJobCommand {
    /// Binds one recovery plan to its preview, current generation, and explicit approval evidence.
    #[must_use]
    pub(crate) const fn new(
        action: RecoveryJobAction,
        identity: SourceIdentifier,
        evidence_digest: EvidenceDigest,
    ) -> Self {
        Self {
            action,
            identity,
            evidence_digest,
        }
    }

    /// Recovery operation selected by the admitted application plan.
    #[must_use]
    pub const fn action(&self) -> RecoveryJobAction {
        self.action
    }

    /// Stable identity of the authority-owned recovery plan.
    #[must_use]
    pub const fn identity(&self) -> &SourceIdentifier {
        &self.identity
    }

    /// Digest of the exact preview, active generation, target, and approval evidence.
    #[must_use]
    pub const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }
}

impl LifecycleJobCommand for RecoveryJobCommand {
    fn identity(&self) -> &SourceIdentifier {
        self.identity()
    }

    fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest()
    }
}

/// Application authority for restore, workspace-switch, and program-rollback plans.
///
/// Implementations must execute workspace operations through the generation-fenced lifecycle
/// authority and program rollback through the retained known-good lifecycle authority. The result
/// may be committed only after activation, health checking, journaling, and client-resync identity
/// allocation complete; partial staging is not a terminal success.
pub trait RecoveryJobAuthority: LifecycleJobAuthority<RecoveryJobCommand> {}

impl<T> RecoveryJobAuthority for T where T: LifecycleJobAuthority<RecoveryJobCommand> + ?Sized {}

/// Bounded job runner for explicitly approved recovery operations.
pub struct RecoveryJobRunner {
    inner: LifecycleOperationJobRunner<RecoveryJobCommand, dyn RecoveryJobAuthority>,
}

impl RecoveryJobRunner {
    /// Binds the recovery authority under finite pending and execution ceilings.
    pub(crate) fn try_new(
        authority: Arc<dyn RecoveryJobAuthority>,
        maximum_pending: usize,
        run_timeout: Duration,
    ) -> Result<Self, LifecycleJobRunnerError> {
        LifecycleOperationJobRunner::try_new(
            KIND,
            INPUT_AUTHORITY,
            RESULT_AUTHORITY,
            RESULT_AUTHORITY_IDENTITY,
            "operations-recovery-failure",
            authority,
            maximum_pending,
            run_timeout,
        )
        .map(|inner| Self { inner })
    }

    /// Retains one exact recovery command until durable job creation succeeds or is revoked.
    pub(crate) fn admit(
        &self,
        command: RecoveryJobCommand,
        captured_at: Timestamp,
    ) -> Result<JobAdmission, LifecycleJobRunnerError> {
        self.inner.admit(command, captured_at)
    }

    /// Releases a process-owned recovery command after failed durable admission.
    pub(crate) fn revoke(&self, admission: &JobAdmission) -> Result<(), LifecycleJobRunnerError> {
        self.inner.revoke(admission)
    }

    /// Constructs the only terminal workspace/program recovery receipt accepted by this runner.
    pub fn try_result_reference(
        identity: SourceIdentifier,
        evidence_digest: EvidenceDigest,
        artifacts: Vec<ArtifactReference>,
    ) -> Result<JobResultReference, LifecycleJobRunnerError> {
        lifecycle_result_reference(RESULT_AUTHORITY, identity, evidence_digest, artifacts)
    }
}

#[async_trait]
impl JobRunner for RecoveryJobRunner {
    fn kind(&self) -> &SourceIdentifier {
        self.inner.kind()
    }

    async fn run(&self, context: JobRunContext) -> Result<JobCompletion, JobRunError> {
        self.inner.run(context).await
    }

    fn recover(&self, _snapshot: &market_squawk_jobs::JobSnapshot) -> JobRecoveryDisposition {
        // Workspace generations and program selectors are reconciled from their own durable
        // journals. The job never replays an interrupted mutation from an in-memory approval.
        JobRecoveryDisposition::MarkInterrupted
    }
}

impl fmt::Debug for RecoveryJobRunner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecoveryJobRunner")
            .field("inner", &self.inner)
            .finish()
    }
}
