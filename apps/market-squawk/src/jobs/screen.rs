//! Durable job adapter for closed saved-screen execution.

use std::{
    fmt,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use market_squawk_decisions::{CandidateInput, ScreenRun};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp};
use market_squawk_jobs::{
    AdmittedJobInput, JobAttemptLimit, JobAuthoritySnapshot, JobCompletion, JobFailure,
    JobProgress, JobRecoveryDisposition, JobResultReference, JobRunContext, JobRunError, JobRunner,
    JobRunnerEvent,
};
use sha2::{Digest as _, Sha256};

use crate::application::{decision::DecisionApplication, job::JobAdmission};

const SCREEN_KIND: &str = "decision.screen-run.v1";
const SCREEN_INPUT_AUTHORITY: &str = "decision.screen-input.v1";
const SCREEN_RESULT_AUTHORITY: &str = "decision.screen-result.v1";
const MAXIMUM_RETAINED_CANDIDATES: usize = 1_000_000;

/// Immutable command registered before durable job admission.
#[derive(Clone, Debug)]
pub struct ScreenJobCommand {
    run: ScreenRun,
    candidates: Vec<CandidateInput>,
    selected_at: Timestamp,
    input_identity: SourceIdentifier,
    input_digest: EvidenceDigest,
}

impl ScreenJobCommand {
    /// Binds an upstream-admitted point-in-time batch to its exact identity and digest.
    #[must_use]
    pub const fn new(
        run: ScreenRun,
        candidates: Vec<CandidateInput>,
        selected_at: Timestamp,
        input_identity: SourceIdentifier,
        input_digest: EvidenceDigest,
    ) -> Self {
        Self {
            run,
            candidates,
            selected_at,
            input_identity,
            input_digest,
        }
    }
}

/// Screen runner admission or execution setup failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScreenJobRunnerError {
    /// Runner limits or a stable authority identity were invalid.
    InvalidConfiguration,
    /// The exact pending command already exists.
    Conflict,
    /// The fixed pending-command ceiling was reached.
    Capacity,
    /// The runner lock was poisoned.
    Unavailable,
}

impl fmt::Display for ScreenJobRunnerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfiguration => "screen job configuration is invalid",
            Self::Conflict => "screen job input is already pending",
            Self::Capacity => "screen job pending capacity is exhausted",
            Self::Unavailable => "screen job runner is unavailable",
        })
    }
}

impl std::error::Error for ScreenJobRunnerError {}

/// Process-bound runner over the sole application decision authority.
pub struct ScreenJobRunner {
    kind: SourceIdentifier,
    input_authority: SourceIdentifier,
    result_authority: SourceIdentifier,
    result_authority_digest: EvidenceDigest,
    decisions: Arc<DecisionApplication>,
    pending: Mutex<Vec<(SourceIdentifier, ScreenJobCommand)>>,
    maximum_pending: usize,
}

impl ScreenJobRunner {
    /// Constructs an adapter only; runner registration remains owned by the installed job authority.
    pub fn try_new(
        decisions: Arc<DecisionApplication>,
        maximum_pending: usize,
    ) -> Result<Self, ScreenJobRunnerError> {
        if maximum_pending == 0 || maximum_pending > 4_096 {
            return Err(ScreenJobRunnerError::InvalidConfiguration);
        }
        let kind = identifier(SCREEN_KIND)?;
        let input_authority = identifier(SCREEN_INPUT_AUTHORITY)?;
        let result_authority = identifier(SCREEN_RESULT_AUTHORITY)?;
        let mut pending = Vec::new();
        pending
            .try_reserve_exact(maximum_pending)
            .map_err(|_error| ScreenJobRunnerError::Capacity)?;
        Ok(Self {
            kind,
            input_authority,
            result_authority,
            result_authority_digest: namespace_digest(SCREEN_RESULT_AUTHORITY),
            decisions,
            pending: Mutex::new(pending),
            maximum_pending,
        })
    }

    /// Registers one exact command and returns its durable job admission.
    pub fn admit(
        &self,
        command: ScreenJobCommand,
        captured_at: Timestamp,
    ) -> Result<JobAdmission, ScreenJobRunnerError> {
        let identity = command.input_identity.clone();
        let digest = command.input_digest;
        let mut pending = self
            .pending
            .lock()
            .map_err(|_error| ScreenJobRunnerError::Unavailable)?;
        if pending
            .iter()
            .any(|(existing, _command)| existing == &identity)
        {
            return Err(ScreenJobRunnerError::Conflict);
        }
        if pending.len() >= self.maximum_pending {
            return Err(ScreenJobRunnerError::Capacity);
        }
        let retained_candidates = pending
            .iter()
            .try_fold(0_usize, |total, (_identity, pending)| {
                total.checked_add(pending.candidates.len())
            })
            .and_then(|total| total.checked_add(command.candidates.len()))
            .ok_or(ScreenJobRunnerError::Capacity)?;
        if command.candidates.len() > market_squawk_decisions::MAX_SCREEN_INPUT_ROWS
            || retained_candidates > MAXIMUM_RETAINED_CANDIDATES
        {
            return Err(ScreenJobRunnerError::Capacity);
        }
        pending.push((identity.clone(), command));
        Ok(JobAdmission::new(
            self.kind.clone(),
            AdmittedJobInput::new(self.input_authority.clone(), identity, digest),
            JobAuthoritySnapshot::new(
                self.result_authority.clone(),
                identifier(SCREEN_RESULT_AUTHORITY)?,
                self.result_authority_digest,
                captured_at,
            ),
            JobAttemptLimit::try_new(1)
                .map_err(|_error| ScreenJobRunnerError::InvalidConfiguration)?,
        ))
    }

    /// Releases a process-bound command if durable job creation fails.
    pub fn revoke(&self, admission: &JobAdmission) -> Result<(), ScreenJobRunnerError> {
        if admission.kind() != &self.kind || admission.input().authority() != &self.input_authority
        {
            return Err(ScreenJobRunnerError::InvalidConfiguration);
        }
        let mut pending = self
            .pending
            .lock()
            .map_err(|_error| ScreenJobRunnerError::Unavailable)?;
        if let Some(index) = pending
            .iter()
            .position(|(identity, _command)| identity == admission.input().identity())
        {
            pending.remove(index);
        }
        Ok(())
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
        let spec = context.snapshot().spec();
        if spec.kind() != &self.kind
            || spec.input().authority() != &self.input_authority
            || spec.authority().authority() != &self.result_authority
            || spec.authority().digest() != self.result_authority_digest
        {
            return Err(JobRunError::Recovery);
        }
        let command = {
            let mut pending = self
                .pending
                .lock()
                .map_err(|_error| JobRunError::Recovery)?;
            let index = pending
                .iter()
                .position(|(identity, _command)| identity == spec.input().identity())
                .ok_or(JobRunError::Recovery)?;
            pending.remove(index).1
        };
        if command.input_identity != *spec.input().identity()
            || command.input_digest != spec.input().digest()
        {
            return Err(JobRunError::Recovery);
        }
        let result_identity =
            identifier(command.run.id().as_str()).map_err(|_error| JobRunError::Recovery)?;
        let result = JobResultReference::try_new(
            self.result_authority.clone(),
            result_identity,
            command.input_digest,
            Vec::new(),
        )
        .map_err(|_error| JobRunError::Recovery)?;
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
        self.decisions
            .run_screen(command.run, command.candidates, command.selected_at)
            .map_err(|_error| failed("screen-evaluation-rejected", false))?;
        Ok(JobCompletion::Published(result, permit.seal()))
    }

    fn recover(&self, _snapshot: &market_squawk_jobs::JobSnapshot) -> JobRecoveryDisposition {
        // The command capability is process-bound. Any committed result remains queryable by run
        // identity, while an uncommitted generation is interrupted rather than reconstructed.
        JobRecoveryDisposition::MarkInterrupted
    }
}

impl fmt::Debug for ScreenJobRunner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScreenJobRunner")
            .field("kind", &self.kind)
            .field("decisions", &"[SOLE DECISION AUTHORITY]")
            .field("pending", &"[BOUNDED SCREEN COMMANDS]")
            .field("maximum_pending", &self.maximum_pending)
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

fn failed(diagnostic: &str, retryable: bool) -> JobRunError {
    let class = SourceIdentifier::try_from("decision-screen-failure");
    let diagnostic = SourceIdentifier::try_from(diagnostic);
    match (class, diagnostic) {
        (Ok(class), Ok(diagnostic)) => {
            JobRunError::Failed(JobFailure::new(class, diagnostic, retryable))
        }
        _ => JobRunError::Recovery,
    }
}
