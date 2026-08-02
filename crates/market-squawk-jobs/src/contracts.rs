use std::{num::NonZeroU64, sync::Arc};

use async_trait::async_trait;
use market_squawk_domain::{EvidenceDigest, SourceIdentifier, Timestamp};
use market_squawk_services::{ArtifactReference, RequestId};
use serde::Serialize;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const MAXIMUM_RECOVERY_PAGE_ITEMS: usize = 1_024;
const MAXIMUM_EVENT_PAGE_ITEMS: usize = 4_096;

/// Invalid durable-job contract input.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum JobContractError {
    /// UUID identities must not use the nil value.
    #[error("job identity must not be nil")]
    NilIdentity,
    /// A job generation is one-based.
    #[error("job generation must be nonzero")]
    ZeroGeneration,
    /// A requested page size was zero or exceeded its hard ceiling.
    #[error("job page limit is invalid")]
    InvalidPageLimit,
    /// Completed progress exceeded the declared total.
    #[error("completed job units exceed total units")]
    ProgressExceedsTotal,
    /// A page contained more records than the caller admitted.
    #[error("job page exceeds its admitted limit")]
    PageLimitExceeded,
    /// A completed snapshot omitted its immutable result reference.
    #[error("completed job snapshot requires a result reference")]
    MissingResult,
    /// A failed snapshot omitted its typed failure.
    #[error("failed job snapshot requires failure evidence")]
    MissingFailure,
    /// A nonterminal snapshot claimed terminal result or failure evidence.
    #[error("nonterminal job snapshot contains terminal evidence")]
    UnexpectedTerminalEvidence,
}

/// Stable identity for one durable job.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct JobId(Uuid);

impl JobId {
    /// Creates a non-nil job identity.
    pub fn try_from_uuid(value: Uuid) -> Result<Self, JobContractError> {
        if value.is_nil() {
            Err(JobContractError::NilIdentity)
        } else {
            Ok(Self(value))
        }
    }

    /// Returns the UUID value.
    #[must_use]
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

/// One-based execution generation for a durable job.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct JobGeneration(NonZeroU64);

impl JobGeneration {
    /// Creates a one-based job generation.
    pub fn try_new(value: u64) -> Result<Self, JobContractError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(JobContractError::ZeroGeneration)
    }

    /// Returns the one-based generation.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Monotonic event sequence within one job generation. Zero is the initial cursor.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct JobEventSequence(u64);

impl JobEventSequence {
    /// Creates a sequence value. Zero represents no event observed yet.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the sequence value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Maximum records returned by one nonterminal-recovery page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveryPageLimit(usize);

impl RecoveryPageLimit {
    /// Creates a positive limit no larger than 1,024 snapshots.
    pub fn try_new(value: usize) -> Result<Self, JobContractError> {
        if value == 0 || value > MAXIMUM_RECOVERY_PAGE_ITEMS {
            Err(JobContractError::InvalidPageLimit)
        } else {
            Ok(Self(value))
        }
    }

    /// Returns the admitted item count.
    #[must_use]
    pub const fn get(self) -> usize {
        self.0
    }
}

/// Maximum records returned by one job-event page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JobEventPageLimit(usize);

impl JobEventPageLimit {
    /// Creates a positive limit no larger than 4,096 events.
    pub fn try_new(value: usize) -> Result<Self, JobContractError> {
        if value == 0 || value > MAXIMUM_EVENT_PAGE_ITEMS {
            Err(JobContractError::InvalidPageLimit)
        } else {
            Ok(Self(value))
        }
    }

    /// Returns the admitted item count.
    #[must_use]
    pub const fn get(self) -> usize {
        self.0
    }
}

/// Opaque continuation returned by nonterminal recovery scans.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct RecoveryCursor(SourceIdentifier);

impl RecoveryCursor {
    /// Wraps an already validated opaque cursor.
    #[must_use]
    pub const fn new(value: SourceIdentifier) -> Self {
        Self(value)
    }

    /// Returns the opaque cursor value.
    #[must_use]
    pub const fn as_source_identifier(&self) -> &SourceIdentifier {
        &self.0
    }
}

/// Durable lifecycle state. Terminal states are completed, failed, and cancelled.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    /// Accepted and waiting for owned execution capacity.
    Queued,
    /// Validating immutable inputs and acquiring bounded resources.
    Preparing,
    /// Runner owns the current generation.
    Running,
    /// Runner requires an explicit application confirmation.
    AwaitingConfirmation,
    /// Cooperative cancellation has been requested.
    Cancelling,
    /// Immutable domain result authority published successfully.
    Completed,
    /// Execution terminated with typed failure evidence.
    Failed,
    /// Cooperative cancellation completed.
    Cancelled,
    /// Process or host interruption ended the prior generation.
    Interrupted,
    /// A new generation is applying its recovery policy.
    Recovering,
}

impl JobState {
    /// Returns whether this state cannot transition again.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

/// Immutable job specification admitted before persistence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdmittedJobSpec {
    id: JobId,
    generation: JobGeneration,
    kind: SourceIdentifier,
    request_id: RequestId,
    input_digest: EvidenceDigest,
    admitted_at: Timestamp,
}

impl AdmittedJobSpec {
    /// Creates an immutable specification from validated identities and exact input evidence.
    #[must_use]
    pub const fn new(
        id: JobId,
        generation: JobGeneration,
        kind: SourceIdentifier,
        request_id: RequestId,
        input_digest: EvidenceDigest,
        admitted_at: Timestamp,
    ) -> Self {
        Self {
            id,
            generation,
            kind,
            request_id,
            input_digest,
            admitted_at,
        }
    }

    /// Stable job identity.
    #[must_use]
    pub const fn id(&self) -> JobId {
        self.id
    }

    /// Current admitted generation.
    #[must_use]
    pub const fn generation(&self) -> JobGeneration {
        self.generation
    }

    /// Initiating transport-neutral request identity.
    #[must_use]
    pub const fn request_id(&self) -> &RequestId {
        &self.request_id
    }
}

/// Persistable durable progress evidence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobProgress {
    phase: SourceIdentifier,
    completed: u64,
    total: Option<u64>,
    recorded_at: Timestamp,
}

impl JobProgress {
    /// Creates progress whose completed units do not exceed a declared total.
    pub fn try_new(
        phase: SourceIdentifier,
        completed: u64,
        total: Option<u64>,
        recorded_at: Timestamp,
    ) -> Result<Self, JobContractError> {
        if total.is_some_and(|total| completed > total) {
            return Err(JobContractError::ProgressExceedsTotal);
        }
        Ok(Self {
            phase,
            completed,
            total,
            recorded_at,
        })
    }
}

/// Immutable reference to a result owned by a domain service.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobResultReference {
    authority: SourceIdentifier,
    identity: SourceIdentifier,
    evidence_digest: EvidenceDigest,
    artifacts: Box<[ArtifactReference]>,
}

impl JobResultReference {
    /// Records the exact domain authority, result identity, digest, and optional artifacts.
    #[must_use]
    pub fn new(
        authority: SourceIdentifier,
        identity: SourceIdentifier,
        evidence_digest: EvidenceDigest,
        artifacts: Vec<ArtifactReference>,
    ) -> Self {
        Self {
            authority,
            identity,
            evidence_digest,
            artifacts: artifacts.into_boxed_slice(),
        }
    }
}

/// Typed, redaction-safe runner failure.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobFailure {
    class: SourceIdentifier,
    diagnostic: SourceIdentifier,
    retryable: bool,
}

impl JobFailure {
    /// Creates bounded failure evidence without raw payloads or credentials.
    #[must_use]
    pub const fn new(
        class: SourceIdentifier,
        diagnostic: SourceIdentifier,
        retryable: bool,
    ) -> Self {
        Self {
            class,
            diagnostic,
            retryable,
        }
    }
}

/// One event proposed for an append-CAS operation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobEvent {
    state: JobState,
    occurred_at: Timestamp,
    progress: Option<JobProgress>,
    result: Option<JobResultReference>,
    failure: Option<JobFailure>,
}

impl JobEvent {
    /// Creates a state event with optional evidence validated as a coherent snapshot fragment.
    pub fn try_new(
        state: JobState,
        occurred_at: Timestamp,
        progress: Option<JobProgress>,
        result: Option<JobResultReference>,
        failure: Option<JobFailure>,
    ) -> Result<Self, JobContractError> {
        validate_terminal_evidence(state, result.as_ref(), failure.as_ref())?;
        Ok(Self {
            state,
            occurred_at,
            progress,
            result,
            failure,
        })
    }
}

/// Materialized durable state after an accepted event.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobSnapshot {
    spec: AdmittedJobSpec,
    sequence: JobEventSequence,
    state: JobState,
    progress: Option<JobProgress>,
    result: Option<JobResultReference>,
    failure: Option<JobFailure>,
    updated_at: Timestamp,
}

impl JobSnapshot {
    /// Creates a coherent materialized snapshot.
    pub fn try_new(
        spec: AdmittedJobSpec,
        sequence: JobEventSequence,
        state: JobState,
        progress: Option<JobProgress>,
        result: Option<JobResultReference>,
        failure: Option<JobFailure>,
        updated_at: Timestamp,
    ) -> Result<Self, JobContractError> {
        validate_terminal_evidence(state, result.as_ref(), failure.as_ref())?;
        Ok(Self {
            spec,
            sequence,
            state,
            progress,
            result,
            failure,
            updated_at,
        })
    }

    /// Stable job identity.
    #[must_use]
    pub const fn id(&self) -> JobId {
        self.spec.id()
    }

    /// Current job generation.
    #[must_use]
    pub const fn generation(&self) -> JobGeneration {
        self.spec.generation()
    }

    /// Last accepted event sequence.
    #[must_use]
    pub const fn sequence(&self) -> JobEventSequence {
        self.sequence
    }

    /// Current lifecycle state.
    #[must_use]
    pub const fn state(&self) -> JobState {
        self.state
    }
}

fn validate_terminal_evidence(
    state: JobState,
    result: Option<&JobResultReference>,
    failure: Option<&JobFailure>,
) -> Result<(), JobContractError> {
    match state {
        JobState::Completed if result.is_none() => Err(JobContractError::MissingResult),
        JobState::Failed if failure.is_none() => Err(JobContractError::MissingFailure),
        JobState::Completed if failure.is_some() => {
            Err(JobContractError::UnexpectedTerminalEvidence)
        }
        JobState::Failed if result.is_some() => Err(JobContractError::UnexpectedTerminalEvidence),
        JobState::Cancelled if result.is_some() || failure.is_some() => {
            Err(JobContractError::UnexpectedTerminalEvidence)
        }
        state if !state.is_terminal() && (result.is_some() || failure.is_some()) => {
            Err(JobContractError::UnexpectedTerminalEvidence)
        }
        _ => Ok(()),
    }
}

/// Bounded page of accepted events.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobEventPage {
    events: Box<[(JobEventSequence, JobEvent)]>,
    next: Option<JobEventSequence>,
}

impl JobEventPage {
    /// Creates a page no larger than the caller's admitted limit.
    pub fn try_new(
        events: Vec<(JobEventSequence, JobEvent)>,
        next: Option<JobEventSequence>,
        limit: JobEventPageLimit,
    ) -> Result<Self, JobContractError> {
        if events.len() > limit.get() {
            return Err(JobContractError::PageLimitExceeded);
        }
        Ok(Self {
            events: events.into_boxed_slice(),
            next,
        })
    }
}

/// Bounded page of nonterminal jobs awaiting recovery disposition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobRecoveryPage {
    snapshots: Box<[JobSnapshot]>,
    next: Option<RecoveryCursor>,
}

impl JobRecoveryPage {
    /// Creates a recovery page no larger than the caller's admitted limit.
    pub fn try_new(
        snapshots: Vec<JobSnapshot>,
        next: Option<RecoveryCursor>,
        limit: RecoveryPageLimit,
    ) -> Result<Self, JobContractError> {
        if snapshots.len() > limit.get() {
            return Err(JobContractError::PageLimitExceeded);
        }
        Ok(Self {
            snapshots: snapshots.into_boxed_slice(),
            next,
        })
    }
}

/// Atomic durable storage required by the job authority.
#[async_trait]
pub trait JobRepository: Send + Sync {
    /// Creates the initial queued snapshot exactly once.
    async fn create(&self, spec: &AdmittedJobSpec) -> Result<JobSnapshot, JobRepositoryError>;
    /// Appends one event only when identity, generation, and expected sequence all match.
    async fn append(
        &self,
        id: JobId,
        generation: JobGeneration,
        expected: JobEventSequence,
        event: JobEvent,
    ) -> Result<JobSnapshot, JobRepositoryError>;
    /// Reads one exact job generation.
    async fn get(
        &self,
        id: JobId,
        generation: JobGeneration,
    ) -> Result<JobSnapshot, JobRepositoryError>;
    /// Reads a bounded event page after an observed sequence.
    async fn events_after(
        &self,
        id: JobId,
        generation: JobGeneration,
        after: JobEventSequence,
        limit: JobEventPageLimit,
    ) -> Result<JobEventPage, JobRepositoryError>;
    /// Scans nonterminal work through an explicit bounded continuation.
    async fn recover_nonterminal(
        &self,
        cursor: Option<&RecoveryCursor>,
        limit: RecoveryPageLimit,
    ) -> Result<JobRecoveryPage, JobRepositoryError>;
}

/// Durable repository failure without storage implementation leakage.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum JobRepositoryError {
    /// The requested job or generation does not exist.
    #[error("job was not found")]
    NotFound,
    /// Append compare-and-swap preconditions did not match.
    #[error("job append conflict")]
    Conflict,
    /// Persisted state failed invariant validation.
    #[error("job repository contains invalid state")]
    InvalidState,
    /// Bounded durable storage is unavailable.
    #[error("job repository is unavailable")]
    Unavailable,
}

/// Event capability supplied to one owned runner generation.
#[async_trait]
pub trait JobEventSink: Send + Sync {
    /// Persists one event using the runner's current CAS sequence.
    async fn append(&self, event: JobEvent) -> Result<JobSnapshot, JobRepositoryError>;
}

/// Cancellation and event authority for one runner generation.
#[derive(Clone)]
pub struct JobRunContext {
    snapshot: JobSnapshot,
    cancellation: CancellationToken,
    events: Arc<dyn JobEventSink>,
}

impl JobRunContext {
    /// Binds a recovered or newly admitted snapshot to generation-scoped capabilities.
    #[must_use]
    pub const fn new(
        snapshot: JobSnapshot,
        cancellation: CancellationToken,
        events: Arc<dyn JobEventSink>,
    ) -> Self {
        Self {
            snapshot,
            cancellation,
            events,
        }
    }

    /// Starting snapshot for this runner generation.
    #[must_use]
    pub const fn snapshot(&self) -> &JobSnapshot {
        &self.snapshot
    }

    /// Cooperative cancellation capability.
    #[must_use]
    pub const fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    /// Generation-scoped durable event capability.
    #[must_use]
    pub const fn events(&self) -> &Arc<dyn JobEventSink> {
        &self.events
    }
}

impl std::fmt::Debug for JobRunContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JobRunContext")
            .field("snapshot", &self.snapshot)
            .field("cancellation", &"[CANCELLATION TOKEN]")
            .field("events", &"[JOB EVENT CAPABILITY]")
            .finish()
    }
}

/// Runner recovery decision made before acquiring execution ownership.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JobRecoveryDisposition {
    /// Resume from retained immutable inputs and checkpoints.
    Resume,
    /// End safely with typed failure evidence.
    Fail(JobFailure),
    /// The domain result was already durably published before interruption.
    Complete(JobResultReference),
}

/// Runner terminal outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JobCompletion {
    /// Domain authority published the immutable result.
    Completed(JobResultReference),
    /// Cooperative cancellation completed without publishing a result.
    Cancelled,
}

/// Runner failure class.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum JobRunError {
    /// The runner returned validated failure evidence.
    #[error("job runner failed")]
    Failed(JobFailure),
    /// Cancellation was observed before completion.
    #[error("job runner was cancelled")]
    Cancelled,
    /// Runner recovery state was invalid or unavailable.
    #[error("job runner recovery failed")]
    Recovery,
}

/// Application-supplied implementation of one closed job kind.
#[async_trait]
pub trait JobRunner: Send + Sync {
    /// Stable kind dispatched by the job authority.
    fn kind(&self) -> &SourceIdentifier;
    /// Executes using only generation-scoped capabilities.
    async fn run(&self, context: JobRunContext) -> Result<JobCompletion, JobRunError>;
    /// Determines how an interrupted snapshot may proceed.
    fn recover(&self, snapshot: &JobSnapshot) -> JobRecoveryDisposition;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_and_page_limits_reject_ambiguous_zero_values() {
        assert_eq!(
            JobId::try_from_uuid(Uuid::nil()),
            Err(JobContractError::NilIdentity)
        );
        assert_eq!(
            JobGeneration::try_new(0),
            Err(JobContractError::ZeroGeneration)
        );
        assert_eq!(
            RecoveryPageLimit::try_new(0),
            Err(JobContractError::InvalidPageLimit)
        );
    }

    #[test]
    fn progress_rejects_completed_units_beyond_total() -> Result<(), Box<dyn std::error::Error>> {
        let phase = SourceIdentifier::try_from("training")?;
        assert_eq!(
            JobProgress::try_new(phase, 2, Some(1), Timestamp::from_unix_nanos(1)),
            Err(JobContractError::ProgressExceedsTotal),
        );
        Ok(())
    }
}
