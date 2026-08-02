//! Durable job adapter for trusted staged product updates.

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

const KIND: &str = "operations.trusted-update.v1";
const INPUT_AUTHORITY: &str = "operations.trusted-update-input.v1";
const RESULT_AUTHORITY: &str = "operations.trusted-update-result.v1";
const RESULT_AUTHORITY_IDENTITY: &str = "trusted-update-authority-v1";

/// Opaque handle to one trusted-metadata-admitted update approval.
///
/// The application authority retains the exact staged candidate, approval, activation adapter,
/// and compatibility evidence. The runner does not download releases or accept raw metadata.
#[derive(Debug)]
pub struct UpdateJobCommand {
    identity: SourceIdentifier,
    evidence_digest: EvidenceDigest,
}

impl UpdateJobCommand {
    /// Binds an already trusted and explicitly approved update plan to canonical evidence.
    #[must_use]
    pub(crate) const fn new(identity: SourceIdentifier, evidence_digest: EvidenceDigest) -> Self {
        Self {
            identity,
            evidence_digest,
        }
    }

    /// Stable identity of the application-owned staged update plan.
    #[must_use]
    pub const fn identity(&self) -> &SourceIdentifier {
        &self.identity
    }

    /// Digest binding trusted metadata, candidate bytes, compatibility preview, and approval.
    #[must_use]
    pub const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }
}

impl LifecycleJobCommand for UpdateJobCommand {
    fn identity(&self) -> &SourceIdentifier {
        self.identity()
    }

    fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest()
    }
}

/// Application authority for one already trusted staged update.
///
/// Implementations must resolve the command to its exact `UpdateApproval`, execute it through
/// `application::lifecycle::TrustedUpdateAuthority`, prepare the terminal result before selector
/// mutation, and report success only after activation or automatic known-good rollback is durably
/// journaled.
pub trait UpdateJobAuthority: LifecycleJobAuthority<UpdateJobCommand> {}

impl<T> UpdateJobAuthority for T where T: LifecycleJobAuthority<UpdateJobCommand> + ?Sized {}

/// Bounded job runner for explicit trusted update activation.
pub struct UpdateJobRunner {
    inner: LifecycleOperationJobRunner<UpdateJobCommand, dyn UpdateJobAuthority>,
}

impl UpdateJobRunner {
    /// Binds the trusted update authority under finite pending and execution ceilings.
    pub(crate) fn try_new(
        authority: Arc<dyn UpdateJobAuthority>,
        maximum_pending: usize,
        run_timeout: Duration,
    ) -> Result<Self, LifecycleJobRunnerError> {
        LifecycleOperationJobRunner::try_new(
            KIND,
            INPUT_AUTHORITY,
            RESULT_AUTHORITY,
            RESULT_AUTHORITY_IDENTITY,
            "operations-update-failure",
            authority,
            maximum_pending,
            run_timeout,
        )
        .map(|inner| Self { inner })
    }

    /// Retains one exact approved update until durable job creation succeeds or is revoked.
    pub(crate) fn admit(
        &self,
        command: UpdateJobCommand,
        captured_at: Timestamp,
    ) -> Result<JobAdmission, LifecycleJobRunnerError> {
        self.inner.admit(command, captured_at)
    }

    /// Releases a process-owned update command after failed durable admission.
    pub(crate) fn revoke(&self, admission: &JobAdmission) -> Result<(), LifecycleJobRunnerError> {
        self.inner.revoke(admission)
    }

    /// Constructs the only terminal update or automatic-rollback receipt accepted by this runner.
    pub fn try_result_reference(
        identity: SourceIdentifier,
        evidence_digest: EvidenceDigest,
        artifacts: Vec<ArtifactReference>,
    ) -> Result<JobResultReference, LifecycleJobRunnerError> {
        lifecycle_result_reference(RESULT_AUTHORITY, identity, evidence_digest, artifacts)
    }
}

#[async_trait]
impl JobRunner for UpdateJobRunner {
    fn kind(&self) -> &SourceIdentifier {
        self.inner.kind()
    }

    async fn run(&self, context: JobRunContext) -> Result<JobCompletion, JobRunError> {
        self.inner.run(context).await
    }

    fn recover(&self, _snapshot: &market_squawk_jobs::JobSnapshot) -> JobRecoveryDisposition {
        // Activation and rollback may cross process boundaries. Durable update journals remain
        // authoritative, while an orphaned job requires explicit reconciliation and re-admission.
        JobRecoveryDisposition::MarkInterrupted
    }
}

impl fmt::Debug for UpdateJobRunner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UpdateJobRunner")
            .field("inner", &self.inner)
            .finish()
    }
}
