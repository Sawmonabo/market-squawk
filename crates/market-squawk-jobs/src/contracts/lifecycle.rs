use market_squawk_domain::{EvidenceDigest, SourceIdentifier, Timestamp};
use market_squawk_services::ArtifactReference;
use serde::Serialize;

use super::api::JobRepositoryError;
use super::{
    AdmittedJobSpec, JobContractError, JobEventSequence, JobGeneration, JobId, JobState,
    MAXIMUM_JOB_ARTIFACT_BYTES, MAXIMUM_JOB_ARTIFACTS,
};

/// Persistable durable progress evidence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobProgress {
    phase: SourceIdentifier,
    completed: u64,
    total: Option<u64>,
    recorded_at: Timestamp,
}

/// Generation-bound confirmation requirement emitted by a runner.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobConfirmationRequest {
    identity: SourceIdentifier,
    digest: EvidenceDigest,
    expires_at: Timestamp,
}

impl JobConfirmationRequest {
    /// Creates a path-free confirmation requirement with an absolute expiry.
    #[must_use]
    pub const fn new(
        identity: SourceIdentifier,
        digest: EvidenceDigest,
        expires_at: Timestamp,
    ) -> Self {
        Self {
            identity,
            digest,
            expires_at,
        }
    }

    /// Stable confirmation purpose identity.
    #[must_use]
    pub const fn identity(&self) -> &SourceIdentifier {
        &self.identity
    }

    /// Exact confirmation subject digest.
    #[must_use]
    pub const fn digest(&self) -> EvidenceDigest {
        self.digest
    }

    /// Absolute confirmation expiry.
    #[must_use]
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }
}

/// Optimistic confirmation mutation bound to one exact job generation and event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobConfirmation {
    id: JobId,
    generation: JobGeneration,
    expected: JobEventSequence,
    identity: SourceIdentifier,
    digest: EvidenceDigest,
}

impl JobConfirmation {
    /// Binds a user confirmation to the exact observed confirmation request.
    #[must_use]
    pub const fn new(
        id: JobId,
        generation: JobGeneration,
        expected: JobEventSequence,
        identity: SourceIdentifier,
        digest: EvidenceDigest,
    ) -> Self {
        Self {
            id,
            generation,
            expected,
            identity,
            digest,
        }
    }

    /// Stable job identity observed by the confirmer.
    #[must_use]
    pub const fn id(&self) -> JobId {
        self.id
    }

    /// Exact execution generation observed by the confirmer.
    #[must_use]
    pub const fn generation(&self) -> JobGeneration {
        self.generation
    }

    /// Exact event sequence containing the pending confirmation request.
    #[must_use]
    pub const fn expected(&self) -> JobEventSequence {
        self.expected
    }

    /// Stable confirmation purpose identity.
    #[must_use]
    pub const fn identity(&self) -> &SourceIdentifier {
        &self.identity
    }

    /// Exact confirmation subject digest.
    #[must_use]
    pub const fn digest(&self) -> EvidenceDigest {
        self.digest
    }
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

    pub(crate) const fn recorded_at(&self) -> Timestamp {
        self.recorded_at
    }

    /// Code-owned phase identity.
    #[must_use]
    pub const fn phase(&self) -> &SourceIdentifier {
        &self.phase
    }

    /// Completed objective units.
    #[must_use]
    pub const fn completed(&self) -> u64 {
        self.completed
    }

    /// Objective total units when measurable.
    #[must_use]
    pub const fn total(&self) -> Option<u64> {
        self.total
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
    /// Records a bounded immutable domain result and optional controlled artifacts.
    pub fn try_new(
        authority: SourceIdentifier,
        identity: SourceIdentifier,
        evidence_digest: EvidenceDigest,
        artifacts: Vec<ArtifactReference>,
    ) -> Result<Self, JobContractError> {
        let bytes = artifacts.iter().try_fold(0_usize, |total, artifact| {
            total.checked_add(artifact.byte_count())
        });
        if artifacts.len() > MAXIMUM_JOB_ARTIFACTS
            || bytes.is_none_or(|bytes| bytes > MAXIMUM_JOB_ARTIFACT_BYTES)
        {
            return Err(JobContractError::ArtifactLimitExceeded);
        }
        Ok(Self {
            authority,
            identity,
            evidence_digest,
            artifacts: artifacts.into_boxed_slice(),
        })
    }

    /// Domain service that owns this result.
    #[must_use]
    pub const fn authority(&self) -> &SourceIdentifier {
        &self.authority
    }

    /// Immutable result identity.
    #[must_use]
    pub const fn identity(&self) -> &SourceIdentifier {
        &self.identity
    }

    /// Exact result evidence digest.
    #[must_use]
    pub const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }

    /// Bounded controlled-artifact references.
    #[must_use]
    pub fn artifacts(&self) -> &[ArtifactReference] {
        &self.artifacts
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

    /// Stable failure class.
    #[must_use]
    pub const fn class(&self) -> &SourceIdentifier {
        &self.class
    }

    /// Redaction-safe diagnostic identity.
    #[must_use]
    pub const fn diagnostic(&self) -> &SourceIdentifier {
        &self.diagnostic
    }

    /// Whether explicit retry policy may consider this failure.
    #[must_use]
    pub const fn retryable(&self) -> bool {
        self.retryable
    }
}

/// One event proposed for an append-CAS operation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobEvent {
    state: JobState,
    occurred_at: Timestamp,
    progress: Option<JobProgress>,
    confirmation: Option<JobConfirmationRequest>,
    result: Option<JobResultReference>,
    failure: Option<JobFailure>,
}

impl JobEvent {
    /// Creates a state event with coherent terminal evidence.
    pub fn try_new(
        state: JobState,
        occurred_at: Timestamp,
        progress: Option<JobProgress>,
        result: Option<JobResultReference>,
        failure: Option<JobFailure>,
    ) -> Result<Self, JobContractError> {
        Self::try_new_with_confirmation(state, occurred_at, progress, None, result, failure)
    }

    /// Creates a state event that may carry one confirmation requirement.
    pub fn try_new_with_confirmation(
        state: JobState,
        occurred_at: Timestamp,
        progress: Option<JobProgress>,
        confirmation: Option<JobConfirmationRequest>,
        result: Option<JobResultReference>,
        failure: Option<JobFailure>,
    ) -> Result<Self, JobContractError> {
        validate_terminal_evidence(state, result.as_ref(), failure.as_ref())?;
        if (state == JobState::AwaitingConfirmation) != confirmation.is_some() {
            return Err(JobContractError::UnexpectedTerminalEvidence);
        }
        Ok(Self {
            state,
            occurred_at,
            progress,
            confirmation,
            result,
            failure,
        })
    }

    pub(crate) const fn state(&self) -> JobState {
        self.state
    }

    pub(crate) const fn occurred_at(&self) -> Timestamp {
        self.occurred_at
    }

    pub(crate) const fn progress(&self) -> Option<&JobProgress> {
        self.progress.as_ref()
    }

    pub(crate) const fn result(&self) -> Option<&JobResultReference> {
        self.result.as_ref()
    }

    pub(crate) const fn confirmation(&self) -> Option<&JobConfirmationRequest> {
        self.confirmation.as_ref()
    }

    pub(crate) const fn failure(&self) -> Option<&JobFailure> {
        self.failure.as_ref()
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
    confirmation: Option<JobConfirmationRequest>,
    result: Option<JobResultReference>,
    failure: Option<JobFailure>,
    updated_at: Timestamp,
    cancellation_requested: bool,
}

impl JobSnapshot {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        spec: AdmittedJobSpec,
        sequence: JobEventSequence,
        state: JobState,
        progress: Option<JobProgress>,
        confirmation: Option<JobConfirmationRequest>,
        result: Option<JobResultReference>,
        failure: Option<JobFailure>,
        updated_at: Timestamp,
        cancellation_requested: bool,
    ) -> Result<Self, JobContractError> {
        validate_terminal_evidence(state, result.as_ref(), failure.as_ref())?;
        if (state == JobState::AwaitingConfirmation) != confirmation.is_some() {
            return Err(JobContractError::UnexpectedTerminalEvidence);
        }
        Ok(Self {
            spec,
            sequence,
            state,
            progress,
            confirmation,
            result,
            failure,
            updated_at,
            cancellation_requested,
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

    /// Returns whether durable cancellation intent exists.
    #[must_use]
    pub const fn cancellation_requested(&self) -> bool {
        self.cancellation_requested
    }

    /// Immutable admitted specification.
    #[must_use]
    pub const fn spec(&self) -> &AdmittedJobSpec {
        &self.spec
    }

    /// Last objective progress evidence.
    #[must_use]
    pub const fn current_progress(&self) -> Option<&JobProgress> {
        self.progress.as_ref()
    }

    /// Pending confirmation requirement, only while awaiting confirmation.
    #[must_use]
    pub const fn pending_confirmation(&self) -> Option<&JobConfirmationRequest> {
        self.confirmation.as_ref()
    }

    /// Immutable terminal result, only for completed jobs.
    #[must_use]
    pub const fn terminal_result(&self) -> Option<&JobResultReference> {
        self.result.as_ref()
    }

    /// Typed terminal failure, only for failed jobs.
    #[must_use]
    pub const fn terminal_failure(&self) -> Option<&JobFailure> {
        self.failure.as_ref()
    }

    /// Last durable state timestamp.
    #[must_use]
    pub const fn updated_at_timestamp(&self) -> Timestamp {
        self.updated_at
    }

    pub(crate) const fn progress(&self) -> Option<&JobProgress> {
        self.progress.as_ref()
    }

    pub(crate) const fn result(&self) -> Option<&JobResultReference> {
        self.result.as_ref()
    }

    pub(crate) const fn confirmation(&self) -> Option<&JobConfirmationRequest> {
        self.confirmation.as_ref()
    }

    pub(crate) const fn failure(&self) -> Option<&JobFailure> {
        self.failure.as_ref()
    }

    pub(crate) const fn updated_at(&self) -> Timestamp {
        self.updated_at
    }
}

pub(crate) fn validate_transition(
    snapshot: &JobSnapshot,
    event: &JobEvent,
) -> Result<(), JobRepositoryError> {
    if snapshot.state.is_terminal() {
        return Err(JobRepositoryError::Terminal);
    }
    if event.occurred_at < snapshot.updated_at
        || event
            .progress
            .as_ref()
            .is_some_and(|progress| progress.recorded_at() > event.occurred_at)
    {
        return Err(JobRepositoryError::InvalidTransition);
    }
    let allowed = matches!(
        (snapshot.state, event.state),
        (JobState::Queued, JobState::Preparing | JobState::Cancelling)
            | (
                JobState::Preparing,
                JobState::Preparing
                    | JobState::Running
                    | JobState::Cancelling
                    | JobState::Failed
                    | JobState::Interrupted
            )
            | (
                JobState::Running,
                JobState::Running
                    | JobState::AwaitingConfirmation
                    | JobState::Cancelling
                    | JobState::Completed
                    | JobState::Failed
                    | JobState::Interrupted
            )
            | (
                JobState::AwaitingConfirmation,
                JobState::Running | JobState::Cancelling | JobState::Failed | JobState::Interrupted
            )
            | (
                JobState::Cancelling,
                JobState::Cancelled | JobState::Failed | JobState::Interrupted
            )
            | (
                JobState::Recovering,
                JobState::Recovering
                    | JobState::Running
                    | JobState::Cancelling
                    | JobState::Failed
                    | JobState::Interrupted
            )
    );
    if allowed {
        Ok(())
    } else {
        Err(JobRepositoryError::InvalidTransition)
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
        state if state != JobState::Completed && result.is_some() => {
            Err(JobContractError::UnexpectedTerminalEvidence)
        }
        state if state != JobState::Failed && failure.is_some() => {
            Err(JobContractError::UnexpectedTerminalEvidence)
        }
        _ => Ok(()),
    }
}
