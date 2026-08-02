use std::sync::Arc;

use async_trait::async_trait;
use market_squawk_domain::{SourceIdentifier, Timestamp};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::lifecycle::{
    JobConfirmationRequest, JobEvent, JobFailure, JobProgress, JobResultReference, JobSnapshot,
};
use super::{
    AdmittedJobSpec, JobContractError, JobEventPageLimit, JobEventSequence, JobGeneration, JobId,
    JobListCursor, JobListPageLimit, RecoveryCursor, RecoveryPageLimit,
};

/// Bounded page of accepted events.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobEventPage {
    events: Box<[(JobEventSequence, JobEvent)]>,
    next: Option<JobEventSequence>,
}

impl JobEventPage {
    pub(crate) fn try_new(
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

    /// Ordered accepted events in this page.
    #[must_use]
    pub fn events(&self) -> &[(JobEventSequence, JobEvent)] {
        &self.events
    }

    /// Cursor for the next bounded page when more events exist.
    #[must_use]
    pub const fn next(&self) -> Option<JobEventSequence> {
        self.next
    }
}

/// Bounded page of nonterminal jobs awaiting recovery disposition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobRecoveryPage {
    snapshots: Box<[JobSnapshot]>,
    next: Option<RecoveryCursor>,
}

/// Bounded latest-generation job list.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobListPage {
    snapshots: Box<[JobSnapshot]>,
    next: Option<JobListCursor>,
}

impl JobListPage {
    pub(crate) fn try_new(
        snapshots: Vec<JobSnapshot>,
        next: Option<JobListCursor>,
        limit: JobListPageLimit,
    ) -> Result<Self, JobContractError> {
        if snapshots.len() > limit.get() {
            return Err(JobContractError::PageLimitExceeded);
        }
        Ok(Self {
            snapshots: snapshots.into_boxed_slice(),
            next,
        })
    }

    /// Latest-generation snapshots in stable job-identity order.
    #[must_use]
    pub fn snapshots(&self) -> &[JobSnapshot] {
        &self.snapshots
    }

    /// Opaque continuation for the next page.
    #[must_use]
    pub const fn next(&self) -> Option<&JobListCursor> {
        self.next.as_ref()
    }
}

impl JobRecoveryPage {
    pub(crate) fn try_new(
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

    /// Ordered recoverable snapshots in this page.
    #[must_use]
    pub fn snapshots(&self) -> &[JobSnapshot] {
        &self.snapshots
    }

    /// Opaque cursor for the next bounded recovery page.
    #[must_use]
    pub const fn next(&self) -> Option<&RecoveryCursor> {
        self.next.as_ref()
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
    /// Durably records cancellation before any in-memory signal is emitted.
    async fn request_cancellation(
        &self,
        id: JobId,
        generation: JobGeneration,
        expected: JobEventSequence,
        at: Timestamp,
    ) -> Result<JobSnapshot, JobRepositoryError>;
    /// Ends an orphaned generation and atomically creates the exact next recovery generation.
    async fn begin_recovery(
        &self,
        orphaned: &JobSnapshot,
        at: Timestamp,
    ) -> Result<JobSnapshot, JobRepositoryError>;
    /// Atomically creates the exact next generation after an explicitly retryable failure.
    async fn begin_retry(
        &self,
        failed: &JobSnapshot,
        at: Timestamp,
    ) -> Result<JobSnapshot, JobRepositoryError>;
    /// Reads one exact job generation.
    async fn get(
        &self,
        id: JobId,
        generation: JobGeneration,
    ) -> Result<JobSnapshot, JobRepositoryError>;
    /// Lists one latest generation per job through a bounded stable cursor.
    async fn list(
        &self,
        cursor: Option<&JobListCursor>,
        limit: JobListPageLimit,
    ) -> Result<JobListPage, JobRepositoryError>;
    /// Reads a bounded event page after an observed sequence.
    async fn events_after(
        &self,
        id: JobId,
        generation: JobGeneration,
        after: JobEventSequence,
        limit: JobEventPageLimit,
    ) -> Result<JobEventPage, JobRepositoryError>;
    /// Scans orphanable work through an explicit bounded continuation.
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
    /// The requested state edge is not legal.
    #[error("job transition is invalid")]
    InvalidTransition,
    /// The execution generation already ended.
    #[error("job generation is terminal")]
    Terminal,
    /// Persisted state failed invariant validation.
    #[error("job repository contains invalid state")]
    InvalidState,
    /// Bounded durable storage is unavailable.
    #[error("job repository is unavailable")]
    Unavailable,
}

/// Nonterminal runner event; terminal publication remains authority-owned.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JobRunnerEvent {
    /// Durable progress in the current phase.
    Progress(JobProgress),
}

/// Event capability supplied to one owned runner generation.
#[async_trait]
pub trait JobEventSink: Send + Sync {
    /// Persists one nonterminal runner event using the current CAS sequence.
    async fn append(&self, event: JobRunnerEvent) -> Result<JobSnapshot, JobRepositoryError>;
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

    /// Generation-scoped durable nonterminal event capability.
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
    /// Start the next generation from an admitted checkpoint.
    ResumeFromCheckpoint,
    /// Start the next generation from the immutable original input.
    RetryFromImmutableInput,
    /// The domain result was already durably published before interruption.
    CompleteAlreadyPublished(JobResultReference),
    /// End the orphaned generation without automatic retry.
    MarkInterrupted,
    /// End safely with typed failure evidence.
    Fail(JobFailure),
}

/// Runner terminal outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JobCompletion {
    /// Domain authority published the immutable result.
    Completed(JobResultReference),
    /// Execution paused cleanly at a generation-bound confirmation boundary.
    AwaitingConfirmation(JobConfirmationRequest),
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
