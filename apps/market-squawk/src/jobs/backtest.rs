//! Durable runner adapter for governed point-in-time backtests.

use std::{collections::BTreeMap, fmt, sync::Arc, time::Duration};

use async_trait::async_trait;
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp};
use market_squawk_jobs::{
    AdmittedJobInput, JobAttemptLimit, JobAuthoritySnapshot, JobCompletion, JobFailure,
    JobProgress, JobRecoveryDisposition, JobResultReference, JobRunContext, JobRunError, JobRunner,
    JobRunnerEvent,
};
use market_squawk_services::ServiceError;
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::application::{
    analysis::{
        GovernedBacktestAuthority, GovernedBacktestCommand, GovernedBacktestInputRegistrar,
        GovernedBacktestInputRegistrationInput, GovernedBacktestInputRegistrationJsonError,
        GovernedBacktestPrepublishAuthority,
    },
    job::JobAdmission,
};

use super::JobTerminalCommitSlot;

const KIND: &str = "analysis.backtest.v1";
const INPUT_AUTHORITY: &str = "analysis.governed-backtest-input.v1";
const RESULT_AUTHORITY: &str = "analysis.governed-backtest-terminal.v1";
const AUTHORITY_IDENTITY: &str = "governed-backtest-authority-v1";

#[derive(Debug)]
struct JobBacktestPrepublishAuthority {
    slot: Arc<JobTerminalCommitSlot>,
}

impl GovernedBacktestPrepublishAuthority for JobBacktestPrepublishAuthority {
    fn validate_prepublish(&self) -> Result<(), ServiceError> {
        self.slot.claim().map_err(|error| match error {
            JobRunError::Cancelled => ServiceError::Cancelled,
            JobRunError::Failed(_) | JobRunError::Recovery => ServiceError::Unavailable,
        })
    }

    fn commit_succeeded(&self) {
        self.slot.seal_domain_commit();
    }
}

/// Bounded runner that delegates execution and terminal indexing to the governed backtest authority.
pub struct BacktestJobRunner {
    kind: SourceIdentifier,
    input_authority: SourceIdentifier,
    result_authority: SourceIdentifier,
    authority_identity: SourceIdentifier,
    authority_digest: EvidenceDigest,
    backtests: Arc<dyn GovernedBacktestAuthority>,
    pending: std::sync::Mutex<BTreeMap<SourceIdentifier, GovernedBacktestCommand>>,
    maximum_pending: usize,
    run_timeout: Duration,
}

impl BacktestJobRunner {
    /// Binds one existing terminal authority under finite admission and runtime ceilings.
    pub fn try_new(
        backtests: Arc<dyn GovernedBacktestAuthority>,
        maximum_pending: usize,
        run_timeout: Duration,
    ) -> Result<Self, BacktestJobRunnerError> {
        if maximum_pending == 0
            || maximum_pending > 4_096
            || run_timeout.is_zero()
            || run_timeout > Duration::from_secs(24 * 60 * 60)
        {
            return Err(BacktestJobRunnerError::InvalidLimits);
        }
        let kind = identifier(KIND)?;
        let input_authority = identifier(INPUT_AUTHORITY)?;
        let result_authority = identifier(RESULT_AUTHORITY)?;
        let authority_identity = identifier(AUTHORITY_IDENTITY)?;
        let authority_digest = namespace_digest(RESULT_AUTHORITY);
        Ok(Self {
            kind,
            input_authority,
            result_authority,
            authority_identity,
            authority_digest,
            backtests,
            pending: std::sync::Mutex::new(BTreeMap::new()),
            maximum_pending,
            run_timeout,
        })
    }

    /// Registers one immutable command before the durable job becomes runnable.
    pub fn admit(
        &self,
        command: GovernedBacktestCommand,
        captured_at: Timestamp,
    ) -> Result<JobAdmission, BacktestJobRunnerError> {
        let digest = command
            .evidence_digest()
            .map_err(|_error| BacktestJobRunnerError::InvalidCommand)?;
        let identity = identifier(format!("backtest-command-{}", encode_hex(digest.bytes())))?;
        let mut pending = self
            .pending
            .lock()
            .map_err(|_error| BacktestJobRunnerError::Unavailable)?;
        match pending.get(&identity) {
            Some(_) => return Err(BacktestJobRunnerError::Conflict),
            None if pending.len() >= self.maximum_pending => {
                return Err(BacktestJobRunnerError::Capacity);
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
                self.authority_identity.clone(),
                self.authority_digest,
                captured_at,
            ),
            JobAttemptLimit::try_new(1).map_err(|_error| BacktestJobRunnerError::InvalidCommand)?,
        ))
    }

    /// Registers and admits the exact closed `Analysis.StartBacktest.registration` object.
    pub async fn admit_registration(
        &self,
        registrar: &dyn GovernedBacktestInputRegistrar,
        registration: &serde_json::Map<String, serde_json::Value>,
        cancellation: tokio_util::sync::CancellationToken,
        deadline: std::time::Instant,
        captured_at: Timestamp,
    ) -> Result<JobAdmission, BacktestJobRunnerError> {
        let encoded = serde_json::to_vec(registration)
            .map_err(|_error| BacktestJobRunnerError::InvalidCommand)?;
        let input =
            GovernedBacktestInputRegistrationInput::try_from_json(&encoded).map_err(|error| {
                match error {
                    GovernedBacktestInputRegistrationJsonError::Invalid => {
                        BacktestJobRunnerError::InvalidCommand
                    }
                    GovernedBacktestInputRegistrationJsonError::ResourceExhausted => {
                        BacktestJobRunnerError::Capacity
                    }
                }
            })?;
        let receipt = registrar
            .register_input(input, cancellation, deadline)
            .await
            .map_err(map_admission_error)?;
        self.admit(receipt.into_command(), captured_at)
    }

    /// Registers and admits one server-prepared exact governed input without JSON reconstruction.
    pub async fn admit_prepared(
        &self,
        registrar: &dyn GovernedBacktestInputRegistrar,
        input: GovernedBacktestInputRegistrationInput,
        cancellation: tokio_util::sync::CancellationToken,
        deadline: std::time::Instant,
        captured_at: Timestamp,
    ) -> Result<JobAdmission, BacktestJobRunnerError> {
        let receipt = registrar
            .register_input(input, cancellation, deadline)
            .await
            .map_err(map_admission_error)?;
        self.admit(receipt.into_command(), captured_at)
    }

    /// Releases one pending admission when durable job creation did not succeed.
    pub fn revoke(&self, admission: &JobAdmission) -> Result<(), BacktestJobRunnerError> {
        if admission.kind() != &self.kind || admission.input().authority() != &self.input_authority
        {
            return Err(BacktestJobRunnerError::InvalidCommand);
        }
        self.pending
            .lock()
            .map_err(|_error| BacktestJobRunnerError::Unavailable)?
            .remove(admission.input().identity());
        Ok(())
    }

    fn take_command(
        &self,
        context: &JobRunContext,
    ) -> Result<GovernedBacktestCommand, JobRunError> {
        let spec = context.snapshot().spec();
        if spec.kind() != &self.kind
            || spec.input().authority() != &self.input_authority
            || spec.authority().authority() != &self.result_authority
            || spec.authority().identity() != &self.authority_identity
            || spec.authority().digest() != self.authority_digest
        {
            return Err(recovery_failure());
        }
        let command = self
            .pending
            .lock()
            .map_err(|_error| recovery_failure())?
            .remove(spec.input().identity())
            .ok_or_else(recovery_failure)?;
        if command
            .evidence_digest()
            .map_err(|_error| recovery_failure())?
            != spec.input().digest()
        {
            return Err(recovery_failure());
        }
        Ok(command)
    }
}

#[async_trait]
impl JobRunner for BacktestJobRunner {
    fn kind(&self) -> &SourceIdentifier {
        &self.kind
    }

    async fn run(&self, context: JobRunContext) -> Result<JobCompletion, JobRunError> {
        if context.cancellation().is_cancelled() {
            return Err(JobRunError::Cancelled);
        }
        let command = self.take_command(&context)?;
        let progress = JobProgress::try_new(
            identifier("resolving-inputs").map_err(|_error| recovery_failure())?,
            0,
            None,
            context.snapshot().updated_at_timestamp(),
        )
        .map_err(|_error| recovery_failure())?;
        let progressed = context
            .events()
            .append(JobRunnerEvent::Progress(progress))
            .await
            .map_err(|_error| failed("job-progress-unavailable", true))?;
        let deadline = std::time::Instant::now()
            .checked_add(self.run_timeout)
            .ok_or_else(recovery_failure)?;
        let slot = Arc::new(JobTerminalCommitSlot::new(&context, progressed.sequence()));
        let prepublish: Arc<dyn GovernedBacktestPrepublishAuthority> =
            Arc::new(JobBacktestPrepublishAuthority {
                slot: Arc::clone(&slot),
            });
        let record = self
            .backtests
            .run_with_prepublish(
                command,
                context.cancellation().clone(),
                deadline,
                prepublish,
            )
            .await
            .map_err(map_service_error)?;
        let published = slot.take_published()?;
        let digest = record.evidence_digest().map_err(map_service_error)?;
        let identity = identifier(record.run_id()).map_err(|_error| recovery_failure())?;
        let result = JobResultReference::try_new(
            self.result_authority.clone(),
            identity,
            digest,
            Vec::new(),
        )
        .map_err(|_error| recovery_failure())?;
        Ok(JobCompletion::Published(result, published))
    }

    fn recover(&self, _snapshot: &market_squawk_jobs::JobSnapshot) -> JobRecoveryDisposition {
        // The governed terminal repository is idempotent, but the job-owned command capability is
        // deliberately process-bound. A restart therefore interrupts this generation instead of
        // guessing whether unpublished execution may be replayed.
        JobRecoveryDisposition::MarkInterrupted
    }
}

impl fmt::Debug for BacktestJobRunner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BacktestJobRunner")
            .field("kind", &self.kind)
            .field("backtests", &"[GOVERNED BACKTEST AUTHORITY]")
            .field("pending", &"[BOUNDED IMMUTABLE COMMANDS]")
            .field("maximum_pending", &self.maximum_pending)
            .field("run_timeout", &self.run_timeout)
            .finish()
    }
}

/// Backtest-runner admission failure without input or storage disclosure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum BacktestJobRunnerError {
    /// Pending-command or runtime ceilings are invalid.
    #[error("backtest job runner limits are invalid")]
    InvalidLimits,
    /// The admitted command is not canonical.
    #[error("backtest job command is invalid")]
    InvalidCommand,
    /// Pending command capacity is exhausted.
    #[error("backtest job runner capacity is exhausted")]
    Capacity,
    /// The same immutable identity resolved to different command content.
    #[error("backtest job command identity conflicts")]
    Conflict,
    /// Pending command authority is unavailable.
    #[error("backtest job runner is unavailable")]
    Unavailable,
}

fn map_service_error(error: ServiceError) -> JobRunError {
    match error {
        ServiceError::Cancelled => JobRunError::Cancelled,
        ServiceError::DeadlineExceeded => failed("backtest-deadline-exceeded", true),
        ServiceError::InvalidRequest | ServiceError::NotFound => {
            failed("backtest-input-rejected", false)
        }
        ServiceError::ResourceExhausted => failed("backtest-resource-exhausted", true),
        ServiceError::Unauthorized => failed("backtest-authority-rejected", false),
        ServiceError::Unavailable => failed("backtest-authority-unavailable", true),
        ServiceError::InvalidResult | ServiceError::Internal => {
            failed("backtest-terminal-invalid", false)
        }
    }
}

fn map_admission_error(error: ServiceError) -> BacktestJobRunnerError {
    match error {
        ServiceError::InvalidRequest | ServiceError::NotFound | ServiceError::InvalidResult => {
            BacktestJobRunnerError::InvalidCommand
        }
        ServiceError::ResourceExhausted => BacktestJobRunnerError::Capacity,
        ServiceError::Cancelled
        | ServiceError::DeadlineExceeded
        | ServiceError::Unauthorized
        | ServiceError::Unavailable
        | ServiceError::Internal => BacktestJobRunnerError::Unavailable,
    }
}

fn recovery_failure() -> JobRunError {
    JobRunError::Recovery
}

fn failed(diagnostic: &str, retryable: bool) -> JobRunError {
    let Ok(class) = SourceIdentifier::try_from("backtest") else {
        return JobRunError::Recovery;
    };
    let Ok(diagnostic) = SourceIdentifier::try_from(diagnostic) else {
        return JobRunError::Recovery;
    };
    JobRunError::Failed(JobFailure::new(class, diagnostic, retryable))
}

fn namespace_digest(namespace: &str) -> EvidenceDigest {
    EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(namespace.as_bytes()).into(),
    )
}

fn identifier(
    value: impl TryInto<SourceIdentifier>,
) -> Result<SourceIdentifier, BacktestJobRunnerError> {
    value
        .try_into()
        .map_err(|_error| BacktestJobRunnerError::InvalidCommand)
}

fn encode_hex(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}
