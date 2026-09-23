//! Durable job adapter for service-owned saved-screen execution.

use std::{fmt, sync::Arc};

use async_trait::async_trait;
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp};
use market_squawk_jobs::{
    AdmittedJobInput, JobAttemptLimit, JobAuthoritySnapshot, JobCompletion, JobFailure,
    JobProgress, JobRecoveryDisposition, JobResultReference, JobRunContext, JobRunError, JobRunner,
    JobRunnerEvent,
};
use sha2::{Digest as _, Sha256};

use crate::application::{
    decision::{AdmittedScreenJob, DecisionApplication},
    job::JobAdmission,
};

const SCREEN_KIND: &str = "decision.screen-run.v1";
const SCREEN_INPUT_AUTHORITY: &str = "decision.screen-input.v1";
const SCREEN_RESULT_AUTHORITY: &str = "decision.screen-result.v1";
const SCREEN_ATTEMPT_LIMIT: u64 = 3;

/// Opaque durable input locator returned by the decision preparation authority.
#[derive(Clone, Debug)]
pub struct ScreenJobCommand {
    admitted: AdmittedScreenJob,
}

impl ScreenJobCommand {
    /// Wraps a decision-authority admission. Raw runs and candidates cannot cross this boundary.
    #[must_use]
    pub const fn new(admitted: AdmittedScreenJob) -> Self {
        Self { admitted }
    }
}

/// Screen runner admission or execution setup failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScreenJobRunnerError {
    /// Runner limits or a stable authority identity were invalid.
    InvalidConfiguration,
    /// The supplied locator did not match a committed decision-authority input.
    Conflict,
}

impl fmt::Display for ScreenJobRunnerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfiguration => "screen job configuration is invalid",
            Self::Conflict => "screen job input does not match committed decision state",
        })
    }
}

impl std::error::Error for ScreenJobRunnerError {}

/// Recoverable runner over immutable inputs committed by the sole decision authority.
pub struct ScreenJobRunner {
    kind: SourceIdentifier,
    input_authority: SourceIdentifier,
    result_authority: SourceIdentifier,
    result_authority_digest: EvidenceDigest,
    decisions: Arc<DecisionApplication>,
}

impl ScreenJobRunner {
    /// Constructs an adapter only; runner registration remains owned by installed jobs.
    pub fn try_new(
        decisions: Arc<DecisionApplication>,
        maximum_pending: usize,
    ) -> Result<Self, ScreenJobRunnerError> {
        if maximum_pending == 0 || maximum_pending > 4_096 {
            return Err(ScreenJobRunnerError::InvalidConfiguration);
        }
        Ok(Self {
            kind: identifier(SCREEN_KIND)?,
            input_authority: identifier(SCREEN_INPUT_AUTHORITY)?,
            result_authority: identifier(SCREEN_RESULT_AUTHORITY)?,
            result_authority_digest: namespace_digest(SCREEN_RESULT_AUTHORITY),
            decisions,
        })
    }

    /// Returns a durable job admission only for an exact already-committed screen input.
    pub fn admit(
        &self,
        command: ScreenJobCommand,
        captured_at: Timestamp,
    ) -> Result<JobAdmission, ScreenJobRunnerError> {
        let admitted = command.admitted;
        let run_id = self
            .decisions
            .prepared_screen_run_id(admitted.input_identity(), admitted.input_digest())
            .map_err(|_error| ScreenJobRunnerError::Conflict)?;
        if &run_id != admitted.run_id() {
            return Err(ScreenJobRunnerError::Conflict);
        }
        Ok(JobAdmission::new(
            self.kind.clone(),
            AdmittedJobInput::new(
                self.input_authority.clone(),
                admitted.input_identity().clone(),
                admitted.input_digest(),
            ),
            JobAuthoritySnapshot::new(
                self.result_authority.clone(),
                identifier(SCREEN_RESULT_AUTHORITY)?,
                self.result_authority_digest,
                captured_at,
            ),
            JobAttemptLimit::try_new(SCREEN_ATTEMPT_LIMIT)
                .map_err(|_error| ScreenJobRunnerError::InvalidConfiguration)?,
        ))
    }

    /// Durable prepared inputs are append-only; failed job creation needs no process-memory undo.
    pub fn revoke(&self, admission: &JobAdmission) -> Result<(), ScreenJobRunnerError> {
        if admission.kind() != &self.kind || admission.input().authority() != &self.input_authority
        {
            return Err(ScreenJobRunnerError::InvalidConfiguration);
        }
        self.decisions
            .prepared_screen_run_id(admission.input().identity(), admission.input().digest())
            .map(|_run_id| ())
            .map_err(|_error| ScreenJobRunnerError::Conflict)
    }

    fn result_reference(
        &self,
        input_identity: &SourceIdentifier,
        input_digest: EvidenceDigest,
    ) -> Result<JobResultReference, JobRunError> {
        let run_id = self
            .decisions
            .prepared_screen_run_id(input_identity, input_digest)
            .map_err(|_error| JobRunError::Recovery)?;
        JobResultReference::try_new(
            self.result_authority.clone(),
            identifier(run_id.as_str()).map_err(|_error| JobRunError::Recovery)?,
            input_digest,
            Vec::new(),
        )
        .map_err(|_error| JobRunError::Recovery)
    }

    fn valid_snapshot(&self, snapshot: &market_squawk_jobs::JobSnapshot) -> bool {
        let spec = snapshot.spec();
        spec.kind() == &self.kind
            && spec.input().authority() == &self.input_authority
            && spec.authority().authority() == &self.result_authority
            && spec.authority().digest() == self.result_authority_digest
    }
}

#[async_trait]
impl JobRunner for ScreenJobRunner {
    fn kind(&self) -> &SourceIdentifier {
        &self.kind
    }

    async fn run(&self, context: JobRunContext) -> Result<JobCompletion, JobRunError> {
        if context.cancellation().is_cancelled() {
            return Err(JobRunError::Cancelled);
        }
        if !self.valid_snapshot(context.snapshot()) {
            return Err(JobRunError::Recovery);
        }
        let spec = context.snapshot().spec();
        let result = self.result_reference(spec.input().identity(), spec.input().digest())?;
        let progress = JobProgress::try_new(
            identifier("evaluating-screen").map_err(|_error| JobRunError::Recovery)?,
            0,
            None,
            context.snapshot().updated_at_timestamp(),
        )
        .map_err(|_error| JobRunError::Recovery)?;
        let progressed = context
            .events()
            .append(JobRunnerEvent::Progress(progress))
            .await
            .map_err(|_error| failed("screen-progress-unavailable", true))?;
        let permit = context.claim_terminal_publication(progressed.sequence())?;
        let existing = self
            .decisions
            .prepared_screen_result(spec.input().identity(), spec.input().digest())
            .map_err(|_error| failed("screen-input-unavailable", false))?;
        if existing.is_none() {
            self.decisions
                .run_prepared_screen_job(spec.input().identity(), spec.input().digest())
                .map_err(|_error| failed("screen-evaluation-rejected", false))?;
        }
        Ok(JobCompletion::Published(result, permit.seal()))
    }

    fn recover(&self, snapshot: &market_squawk_jobs::JobSnapshot) -> JobRecoveryDisposition {
        if !self.valid_snapshot(snapshot) {
            return recovery_failed("screen-recovery-invalid");
        }
        let spec = snapshot.spec();
        match self
            .decisions
            .prepared_screen_result(spec.input().identity(), spec.input().digest())
        {
            Ok(Some(_execution)) => {
                match self.result_reference(spec.input().identity(), spec.input().digest()) {
                    Ok(result) => JobRecoveryDisposition::CompleteAlreadyPublished(result),
                    Err(_error) => recovery_failed("screen-recovery-invalid"),
                }
            }
            Ok(None) => JobRecoveryDisposition::RetryFromImmutableInput,
            Err(_error) => recovery_failed("screen-input-unavailable"),
        }
    }
}

impl fmt::Debug for ScreenJobRunner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScreenJobRunner")
            .field("kind", &self.kind)
            .field("decisions", &"[SOLE DECISION AUTHORITY]")
            .field("input", &"[DURABLE SCREEN INPUTS]")
            .finish()
    }
}

fn identifier(value: impl AsRef<str>) -> Result<SourceIdentifier, ScreenJobRunnerError> {
    SourceIdentifier::try_from(value.as_ref())
        .map_err(|_error| ScreenJobRunnerError::InvalidConfiguration)
}

fn namespace_digest(value: &str) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(value).into())
}

fn failure(diagnostic: &str, retryable: bool) -> Option<JobFailure> {
    let class = SourceIdentifier::try_from("decision-screen-failure").ok()?;
    let diagnostic = SourceIdentifier::try_from(diagnostic).ok()?;
    Some(JobFailure::new(class, diagnostic, retryable))
}

fn failed(diagnostic: &str, retryable: bool) -> JobRunError {
    failure(diagnostic, retryable).map_or(JobRunError::Recovery, JobRunError::Failed)
}

fn recovery_failed(diagnostic: &str) -> JobRecoveryDisposition {
    failure(diagnostic, false).map_or(
        JobRecoveryDisposition::MarkInterrupted,
        JobRecoveryDisposition::Fail,
    )
}
