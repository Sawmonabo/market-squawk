//! Durable job authority for product backup operations.

use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp};
use market_squawk_jobs::{
    AdmittedJobInput, JobAttemptLimit, JobAuthoritySnapshot, JobCompletion, JobFailure,
    JobProgress, JobRecoveryDisposition, JobResultReference, JobRunContext, JobRunError, JobRunner,
    JobRunnerEvent,
};
use market_squawk_services::ArtifactReference;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::application::job::JobAdmission;

use super::JobTerminalCommitSlot;

const KIND: &str = "operations.product-backup.v1";
const INPUT_AUTHORITY: &str = "operations.product-backup-input.v1";
const RESULT_AUTHORITY: &str = "operations.product-backup-result.v1";
const RESULT_AUTHORITY_IDENTITY: &str = "product-backup-authority-v1";

/// Closed product-backup operation admitted before durable job creation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackupJobAction {
    /// Materialize and verify a new complete product backup.
    Create,
    /// Revalidate an existing exact backup and its ownership evidence.
    Verify,
    /// Apply an already previewed bounded retention decision.
    EnforceRetention,
}

/// Opaque handle to one application-owned, immutable backup plan.
///
/// The application backup authority retains the concrete plan and admits this handle only after
/// destination, ownership, encryption, component, retention, or restore-preview validation. The
/// job runner never accepts a filesystem path or reconstructs a plan from presentation input.
#[derive(Debug)]
pub struct BackupJobCommand {
    action: BackupJobAction,
    identity: SourceIdentifier,
    evidence_digest: EvidenceDigest,
}

impl BackupJobCommand {
    /// Binds one authority-owned backup plan to its exact canonical evidence.
    #[must_use]
    pub(crate) const fn new(
        action: BackupJobAction,
        identity: SourceIdentifier,
        evidence_digest: EvidenceDigest,
    ) -> Self {
        Self {
            action,
            identity,
            evidence_digest,
        }
    }

    /// Backup operation selected by the admitted plan.
    #[must_use]
    pub const fn action(&self) -> BackupJobAction {
        self.action
    }

    /// Stable identity of the authority-owned plan.
    #[must_use]
    pub const fn identity(&self) -> &SourceIdentifier {
        &self.identity
    }

    /// Digest of the complete canonical plan and approval evidence.
    #[must_use]
    pub const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }
}

impl LifecycleJobCommand for BackupJobCommand {
    fn identity(&self) -> &SourceIdentifier {
        self.identity()
    }

    fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest()
    }
}

/// Application authority for exact backup create, verify, and retention plans.
///
/// An implementation must prepare the immutable result reference before its irreversible commit,
/// call [`LifecycleJobPublication::prepare_and_claim`], durably commit the backup or restored
/// workspace, then call [`LifecycleJobPublication::commit_succeeded`] before returning `Ok(())`.
/// It must never return success for a preview, partially copied bundle, or unactivated restore.
pub trait BackupJobAuthority: LifecycleJobAuthority<BackupJobCommand> {}

impl<T> BackupJobAuthority for T where T: LifecycleJobAuthority<BackupJobCommand> + ?Sized {}

/// Durable backup runner over the product backup authority.
pub struct BackupJobRunner {
    inner: LifecycleOperationJobRunner<BackupJobCommand, dyn BackupJobAuthority>,
}

impl BackupJobRunner {
    /// Binds the application backup authority under finite pending and execution ceilings.
    pub(crate) fn try_new(
        authority: Arc<dyn BackupJobAuthority>,
        maximum_pending: usize,
        run_timeout: Duration,
    ) -> Result<Self, LifecycleJobRunnerError> {
        LifecycleOperationJobRunner::try_new(
            KIND,
            INPUT_AUTHORITY,
            RESULT_AUTHORITY,
            RESULT_AUTHORITY_IDENTITY,
            "operations-backup-failure",
            authority,
            maximum_pending,
            run_timeout,
        )
        .map(|inner| Self { inner })
    }

    /// Retains one exact command until durable job creation succeeds or admission is revoked.
    pub(crate) fn admit(
        &self,
        command: BackupJobCommand,
        captured_at: Timestamp,
    ) -> Result<JobAdmission, LifecycleJobRunnerError> {
        self.inner.admit(command, captured_at)
    }

    /// Releases a process-owned command when durable job creation did not succeed.
    pub(crate) fn revoke(&self, admission: &JobAdmission) -> Result<(), LifecycleJobRunnerError> {
        self.inner.revoke(admission)
    }

    /// Constructs the only terminal result reference accepted by this runner.
    pub fn try_result_reference(
        identity: SourceIdentifier,
        evidence_digest: EvidenceDigest,
        artifacts: Vec<ArtifactReference>,
    ) -> Result<JobResultReference, LifecycleJobRunnerError> {
        lifecycle_result_reference(RESULT_AUTHORITY, identity, evidence_digest, artifacts)
    }
}

#[async_trait]
impl JobRunner for BackupJobRunner {
    fn kind(&self) -> &SourceIdentifier {
        self.inner.kind()
    }

    async fn run(&self, context: JobRunContext) -> Result<JobCompletion, JobRunError> {
        self.inner.run(context).await
    }

    fn recover(&self, _snapshot: &market_squawk_jobs::JobSnapshot) -> JobRecoveryDisposition {
        // Backup destinations, retention previews, and workspace approvals are process-owned.
        // Their terminal repositories remain queryable, but an interrupted mutation is never
        // guessed or replayed without fresh application admission.
        JobRecoveryDisposition::MarkInterrupted
    }
}

impl fmt::Debug for BackupJobRunner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BackupJobRunner")
            .field("inner", &self.inner)
            .finish()
    }
}

/// Generation-scoped publication capability supplied to a lifecycle application authority.
pub trait LifecycleJobPublication: fmt::Debug + Send + Sync {
    /// Validates the complete result reference and wins the terminal publication fence.
    fn prepare_and_claim(
        &self,
        result: JobResultReference,
    ) -> Result<(), LifecycleJobPublicationError>;

    /// Seals the won permit after the application authority durably commits the exact result.
    fn commit_succeeded(&self);
}

/// Exact application operation invoked by a lifecycle runner.
#[async_trait]
pub trait LifecycleJobAuthority<C>: fmt::Debug + Send + Sync
where
    C: LifecycleJobCommand,
{
    /// Executes one already admitted operation through the sole application authority.
    async fn execute(
        &self,
        command: C,
        cancellation: CancellationToken,
        deadline: Instant,
        publication: Arc<dyn LifecycleJobPublication>,
    ) -> Result<(), LifecycleJobExecutionError>;
}

/// Redaction-safe failure returned by a lifecycle application authority.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum LifecycleJobExecutionError {
    /// Cooperative cancellation won before terminal publication.
    #[error("lifecycle operation was cancelled")]
    Cancelled,
    /// The authority rejected or could not complete the operation.
    #[error("lifecycle operation failed")]
    Failed {
        /// Stable path-free diagnostic.
        diagnostic: SourceIdentifier,
        /// Whether explicit job retry policy may consider this failure.
        retryable: bool,
    },
    /// Terminal publication could not be acquired or sealed safely.
    #[error(transparent)]
    Publication(#[from] LifecycleJobPublicationError),
}

impl LifecycleJobExecutionError {
    /// Creates a redaction-safe domain failure.
    #[must_use]
    pub const fn failed(diagnostic: SourceIdentifier, retryable: bool) -> Self {
        Self::Failed {
            diagnostic,
            retryable,
        }
    }
}

/// Failure to obtain the exact generation's terminal publication authority.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LifecycleJobPublicationError {
    /// Durable cancellation won the generation race.
    #[error("lifecycle publication was cancelled")]
    Cancelled,
    /// The publication claim, result authority, or commit sequence is invalid.
    #[error("lifecycle publication authority was revoked")]
    Revoked,
}

pub trait LifecycleJobCommand: fmt::Debug + Send + 'static {
    /// Exact application-owned operation identity retained by the pending map.
    fn identity(&self) -> &SourceIdentifier;

    /// Digest of the complete canonical plan and its approval evidence.
    fn evidence_digest(&self) -> EvidenceDigest;
}

pub(super) struct LifecycleOperationJobRunner<C, A>
where
    C: LifecycleJobCommand,
    A: LifecycleJobAuthority<C> + ?Sized,
{
    kind: SourceIdentifier,
    input_authority: SourceIdentifier,
    result_authority: SourceIdentifier,
    result_authority_identity: SourceIdentifier,
    result_authority_digest: EvidenceDigest,
    failure_class: SourceIdentifier,
    authority: Arc<A>,
    pending: Mutex<BTreeMap<SourceIdentifier, C>>,
    maximum_pending: usize,
    run_timeout: Duration,
}

impl<C, A> LifecycleOperationJobRunner<C, A>
where
    C: LifecycleJobCommand,
    A: LifecycleJobAuthority<C> + ?Sized,
{
    #[allow(
        clippy::too_many_arguments,
        reason = "the runner keeps every authority identity and resource ceiling explicit"
    )]
    pub(super) fn try_new(
        kind: &'static str,
        input_authority: &'static str,
        result_authority: &'static str,
        result_authority_identity: &'static str,
        failure_class: &'static str,
        authority: Arc<A>,
        maximum_pending: usize,
        run_timeout: Duration,
    ) -> Result<Self, LifecycleJobRunnerError> {
        if maximum_pending == 0
            || maximum_pending > 4_096
            || run_timeout.is_zero()
            || run_timeout > Duration::from_secs(24 * 60 * 60)
        {
            return Err(LifecycleJobRunnerError::InvalidLimits);
        }
        Ok(Self {
            kind: identifier(kind)?,
            input_authority: identifier(input_authority)?,
            result_authority: identifier(result_authority)?,
            result_authority_identity: identifier(result_authority_identity)?,
            result_authority_digest: namespace_digest(result_authority),
            failure_class: identifier(failure_class)?,
            authority,
            pending: Mutex::new(BTreeMap::new()),
            maximum_pending,
            run_timeout,
        })
    }

    pub(super) const fn kind(&self) -> &SourceIdentifier {
        &self.kind
    }

    pub(super) fn admit(
        &self,
        command: C,
        captured_at: Timestamp,
    ) -> Result<JobAdmission, LifecycleJobRunnerError> {
        let identity = command.identity().clone();
        let digest = command.evidence_digest();
        let mut pending = self
            .pending
            .lock()
            .map_err(|_error| LifecycleJobRunnerError::Unavailable)?;
        match pending.get(&identity) {
            Some(_) => return Err(LifecycleJobRunnerError::Conflict),
            None if pending.len() >= self.maximum_pending => {
                return Err(LifecycleJobRunnerError::Capacity);
            }
            None => {
                pending.insert(identity.clone(), command);
            }
        }
        Ok(JobAdmission::new(
            self.kind.clone(),
            AdmittedJobInput::new(self.input_authority.clone(), identity, digest),
            JobAuthoritySnapshot::new(
                self.result_authority.clone(),
                self.result_authority_identity.clone(),
                self.result_authority_digest,
                captured_at,
            ),
            JobAttemptLimit::try_new(1).map_err(|_error| LifecycleJobRunnerError::InvalidLimits)?,
        ))
    }

    pub(super) fn revoke(&self, admission: &JobAdmission) -> Result<(), LifecycleJobRunnerError> {
        if admission.kind() != &self.kind || admission.input().authority() != &self.input_authority
        {
            return Err(LifecycleJobRunnerError::InvalidAdmission);
        }
        self.pending
            .lock()
            .map_err(|_error| LifecycleJobRunnerError::Unavailable)?
            .remove(admission.input().identity());
        Ok(())
    }

    pub(super) async fn run(&self, context: JobRunContext) -> Result<JobCompletion, JobRunError> {
        if context.cancellation().is_cancelled() {
            return Err(JobRunError::Cancelled);
        }
        let spec = context.snapshot().spec();
        if spec.kind() != &self.kind
            || spec.input().authority() != &self.input_authority
            || spec.authority().authority() != &self.result_authority
            || spec.authority().identity() != &self.result_authority_identity
            || spec.authority().digest() != self.result_authority_digest
        {
            return Err(JobRunError::Recovery);
        }
        let command = self
            .pending
            .lock()
            .map_err(|_error| JobRunError::Recovery)?
            .remove(spec.input().identity())
            .ok_or(JobRunError::Recovery)?;
        if command.identity() != spec.input().identity()
            || command.evidence_digest() != spec.input().digest()
        {
            return Err(JobRunError::Recovery);
        }
        append_progress(&context, "validated-admitted-operation", 0).await?;
        if context.cancellation().is_cancelled() {
            return Err(JobRunError::Cancelled);
        }
        let executing = append_progress(&context, "executing-lifecycle-operation", 1).await?;
        let deadline = Instant::now()
            .checked_add(self.run_timeout)
            .ok_or(JobRunError::Recovery)?;
        let publication = Arc::new(LifecyclePublicationSlot::new(
            &context,
            executing.sequence(),
            self.result_authority.clone(),
        ));
        let execution = self
            .authority
            .execute(
                command,
                context.cancellation().clone(),
                deadline,
                publication.clone(),
            )
            .await;
        match publication.take_completion() {
            Ok((result, permit)) => Ok(JobCompletion::Published(result, permit)),
            Err(_) => match execution {
                Ok(()) => Err(JobRunError::Recovery),
                Err(error) => Err(map_execution_error(error, &self.failure_class)),
            },
        }
    }
}

impl<C, A> fmt::Debug for LifecycleOperationJobRunner<C, A>
where
    C: LifecycleJobCommand,
    A: LifecycleJobAuthority<C> + ?Sized,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LifecycleOperationJobRunner")
            .field("kind", &self.kind)
            .field("authority", &"[SOLE LIFECYCLE AUTHORITY]")
            .field("pending", &"[BOUNDED IMMUTABLE COMMANDS]")
            .field("maximum_pending", &self.maximum_pending)
            .field("run_timeout", &self.run_timeout)
            .finish()
    }
}

struct LifecyclePublicationSlot {
    terminal: JobTerminalCommitSlot,
    result_authority: SourceIdentifier,
    result: Mutex<Option<JobResultReference>>,
}

impl LifecyclePublicationSlot {
    fn new(
        context: &JobRunContext,
        expected: market_squawk_jobs::JobEventSequence,
        result_authority: SourceIdentifier,
    ) -> Self {
        Self {
            terminal: JobTerminalCommitSlot::new(context, expected),
            result_authority,
            result: Mutex::new(None),
        }
    }

    fn take_completion(
        &self,
    ) -> Result<(JobResultReference, market_squawk_jobs::JobPublishedPermit), JobRunError> {
        let result = match self.result.lock() {
            Ok(mut result) => result.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        }
        .ok_or(JobRunError::Recovery)?;
        self.terminal
            .take_published()
            .map(|published| (result, published))
    }
}

impl LifecycleJobPublication for LifecyclePublicationSlot {
    fn prepare_and_claim(
        &self,
        result: JobResultReference,
    ) -> Result<(), LifecycleJobPublicationError> {
        if result.authority() != &self.result_authority {
            return Err(LifecycleJobPublicationError::Revoked);
        }
        let mut prepared = match self.result.lock() {
            Ok(prepared) => prepared,
            Err(poisoned) => poisoned.into_inner(),
        };
        if prepared.is_some() {
            return Err(LifecycleJobPublicationError::Revoked);
        }
        self.terminal.claim().map_err(|error| match error {
            JobRunError::Cancelled => LifecycleJobPublicationError::Cancelled,
            JobRunError::Failed(_) | JobRunError::Recovery => LifecycleJobPublicationError::Revoked,
        })?;
        *prepared = Some(result);
        Ok(())
    }

    fn commit_succeeded(&self) {
        self.terminal.seal_domain_commit();
    }
}

impl fmt::Debug for LifecyclePublicationSlot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LifecyclePublicationSlot")
            .field("terminal", &"[GENERATION PUBLICATION FENCE]")
            .field("result_authority", &self.result_authority)
            .field("result", &"[PREPARED TERMINAL REFERENCE]")
            .finish()
    }
}

async fn append_progress(
    context: &JobRunContext,
    phase: &'static str,
    completed: u64,
) -> Result<market_squawk_jobs::JobSnapshot, JobRunError> {
    let progress = JobProgress::try_new(
        identifier(phase).map_err(|_error| JobRunError::Recovery)?,
        completed,
        Some(2),
        context.snapshot().updated_at_timestamp(),
    )
    .map_err(|_error| JobRunError::Recovery)?;
    context
        .events()
        .append(JobRunnerEvent::Progress(progress))
        .await
        .map_err(|_error| lifecycle_failure("job-progress-unavailable", true))
}

fn map_execution_error(
    error: LifecycleJobExecutionError,
    failure_class: &SourceIdentifier,
) -> JobRunError {
    match error {
        LifecycleJobExecutionError::Cancelled
        | LifecycleJobExecutionError::Publication(LifecycleJobPublicationError::Cancelled) => {
            JobRunError::Cancelled
        }
        LifecycleJobExecutionError::Publication(LifecycleJobPublicationError::Revoked) => {
            JobRunError::Recovery
        }
        LifecycleJobExecutionError::Failed {
            diagnostic,
            retryable,
        } => JobRunError::Failed(JobFailure::new(
            failure_class.clone(),
            diagnostic,
            retryable,
        )),
    }
}

fn lifecycle_failure(diagnostic: &'static str, retryable: bool) -> JobRunError {
    match (
        SourceIdentifier::try_from("operations-lifecycle-failure"),
        SourceIdentifier::try_from(diagnostic),
    ) {
        (Ok(class), Ok(diagnostic)) => {
            JobRunError::Failed(JobFailure::new(class, diagnostic, retryable))
        }
        _ => JobRunError::Recovery,
    }
}

fn identifier(value: impl AsRef<str>) -> Result<SourceIdentifier, LifecycleJobRunnerError> {
    SourceIdentifier::try_from(value.as_ref())
        .map_err(|_error| LifecycleJobRunnerError::InvalidConfiguration)
}

fn namespace_digest(value: &str) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(value).into())
}

pub(super) fn lifecycle_result_reference(
    authority: &'static str,
    identity: SourceIdentifier,
    evidence_digest: EvidenceDigest,
    artifacts: Vec<ArtifactReference>,
) -> Result<JobResultReference, LifecycleJobRunnerError> {
    JobResultReference::try_new(identifier(authority)?, identity, evidence_digest, artifacts)
        .map_err(|_error| LifecycleJobRunnerError::InvalidResult)
}

/// Lifecycle runner admission or setup failure without path or payload disclosure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LifecycleJobRunnerError {
    /// A code-owned authority identity was invalid.
    #[error("lifecycle job runner configuration is invalid")]
    InvalidConfiguration,
    /// Pending-command or execution limits are invalid.
    #[error("lifecycle job runner limits are invalid")]
    InvalidLimits,
    /// The supplied admission belongs to another runner.
    #[error("lifecycle job admission is invalid")]
    InvalidAdmission,
    /// The application authority supplied an invalid or oversized terminal reference.
    #[error("lifecycle job result reference is invalid")]
    InvalidResult,
    /// The exact operation is already pending.
    #[error("lifecycle job operation is already pending")]
    Conflict,
    /// The bounded pending-operation ceiling was reached.
    #[error("lifecycle job runner capacity is exhausted")]
    Capacity,
    /// Process-local runner state is unavailable.
    #[error("lifecycle job runner is unavailable")]
    Unavailable,
}
