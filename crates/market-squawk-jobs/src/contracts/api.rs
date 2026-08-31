use std::sync::{
    Arc,
    atomic::{AtomicU8, AtomicU64, Ordering},
};

use async_trait::async_trait;
use market_squawk_domain::{SourceIdentifier, Timestamp};
use serde::Serialize;
use thiserror::Error;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::lifecycle::{
    JobConfirmationRequest, JobEvent, JobFailure, JobProgress, JobResultReference, JobSnapshot,
};
use super::{
    AdmittedJobSpec, JobContractError, JobEventPageLimit, JobEventSequence, JobGeneration, JobId,
    JobListCursor, JobListPageLimit, RecoveryCursor, RecoveryPageLimit,
};

/// Bounded page of accepted events.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
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
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobRecoveryPage {
    snapshots: Box<[JobSnapshot]>,
    next: Option<RecoveryCursor>,
}

/// Bounded latest-generation job list.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
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

const PUBLICATION_OPEN: u8 = 0;
const PUBLICATION_ACTIVE: u8 = 1;
const CANCELLATION_ACTIVE: u8 = 2;
const CANCELLATION_PERSISTED: u8 = 3;

#[derive(Debug)]
struct JobTerminalPublicationState {
    state: AtomicU8,
    latest_sequence: AtomicU64,
    changed: Notify,
}

/// One process-local winner fence shared by cancellation and terminal publication.
#[derive(Clone, Debug)]
pub(crate) struct JobTerminalPublicationFence {
    id: JobId,
    generation: JobGeneration,
    inner: Arc<JobTerminalPublicationState>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum JobCancellationClaim {
    Won,
    PublicationActive,
    CancellationActive,
}

impl JobTerminalPublicationFence {
    pub(crate) fn new(
        id: JobId,
        generation: JobGeneration,
        latest_sequence: JobEventSequence,
    ) -> Self {
        Self {
            id,
            generation,
            inner: Arc::new(JobTerminalPublicationState {
                state: AtomicU8::new(PUBLICATION_OPEN),
                latest_sequence: AtomicU64::new(latest_sequence.get()),
                changed: Notify::new(),
            }),
        }
    }

    pub(crate) fn observe_sequence(&self, sequence: JobEventSequence) {
        if self.inner.state.load(Ordering::Acquire) == PUBLICATION_OPEN {
            self.inner
                .latest_sequence
                .store(sequence.get(), Ordering::Release);
        }
    }

    pub(crate) fn claim_publication(
        &self,
        expected: JobEventSequence,
    ) -> Result<JobTerminalPublicationPermit, JobRunError> {
        if self.inner.latest_sequence.load(Ordering::Acquire) != expected.get() {
            return Err(JobRunError::Recovery);
        }
        match self.inner.state.compare_exchange(
            PUBLICATION_OPEN,
            PUBLICATION_ACTIVE,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Ok(JobTerminalPublicationPermit {
                id: self.id,
                generation: self.generation,
                expected,
                fence: Some(self.clone()),
            }),
            Err(CANCELLATION_ACTIVE | CANCELLATION_PERSISTED) => Err(JobRunError::Cancelled),
            Err(_) => Err(JobRunError::Recovery),
        }
    }

    pub(crate) fn claim_cancellation(&self) -> JobCancellationClaim {
        match self.inner.state.compare_exchange(
            PUBLICATION_OPEN,
            CANCELLATION_ACTIVE,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => JobCancellationClaim::Won,
            Err(PUBLICATION_ACTIVE) => JobCancellationClaim::PublicationActive,
            Err(_) => JobCancellationClaim::CancellationActive,
        }
    }

    pub(crate) fn cancellation_persisted(&self) {
        if self
            .inner
            .state
            .compare_exchange(
                CANCELLATION_ACTIVE,
                CANCELLATION_PERSISTED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            self.inner.changed.notify_waiters();
        }
    }

    pub(crate) fn cancellation_failed(&self) {
        if self
            .inner
            .state
            .compare_exchange(
                CANCELLATION_ACTIVE,
                PUBLICATION_OPEN,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            self.inner.changed.notify_waiters();
        }
    }

    pub(crate) async fn wait_for_publication_release(&self) {
        loop {
            if self.inner.state.load(Ordering::Acquire) != PUBLICATION_ACTIVE {
                return;
            }
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.inner.state.load(Ordering::Acquire) != PUBLICATION_ACTIVE {
                return;
            }
            changed.await;
        }
    }

    pub(crate) fn publication_is_active(&self) -> bool {
        self.inner.state.load(Ordering::Acquire) == PUBLICATION_ACTIVE
    }

    pub(crate) async fn wait_for_cancellation_resolution(&self) {
        loop {
            if self.inner.state.load(Ordering::Acquire) != CANCELLATION_ACTIVE {
                return;
            }
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.inner.state.load(Ordering::Acquire) != CANCELLATION_ACTIVE {
                return;
            }
            changed.await;
        }
    }
}

/// Opaque proof that one runner won terminal publication for an exact durable sequence.
///
/// The permit owns the same nonblocking winner fence used by cancellation. It must be moved into
/// [`JobCompletion::Published`] after the domain result is durably published; the job authority
/// retains it until the matching `Completed` event has been appended.
pub struct JobTerminalPublicationPermit {
    id: JobId,
    generation: JobGeneration,
    expected: JobEventSequence,
    fence: Option<JobTerminalPublicationFence>,
}

impl JobTerminalPublicationPermit {
    /// Seals the permit after the domain authority has durably committed its immutable result.
    ///
    /// An unsealed permit reopens publication when dropped. A sealed permit remains in the
    /// publication state until the job authority durably appends `Completed`, including when that
    /// append fails and requires restart reconciliation.
    #[must_use]
    pub fn seal(mut self) -> JobPublishedPermit {
        JobPublishedPermit {
            id: self.id,
            generation: self.generation,
            expected: self.expected,
            fence: self.fence.take(),
        }
    }
}

impl Drop for JobTerminalPublicationPermit {
    fn drop(&mut self) {
        let Some(fence) = self.fence.take() else {
            return;
        };
        if fence
            .inner
            .state
            .compare_exchange(
                PUBLICATION_ACTIVE,
                PUBLICATION_OPEN,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            fence.inner.changed.notify_waiters();
        }
    }
}

impl std::fmt::Debug for JobTerminalPublicationPermit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JobTerminalPublicationPermit")
            .field("id", &self.id)
            .field("generation", &self.generation)
            .field("expected", &self.expected)
            .field("fence", &"[TERMINAL PUBLICATION FENCE]")
            .finish()
    }
}

/// Opaque proof that the immutable business result was durably published.
pub struct JobPublishedPermit {
    id: JobId,
    generation: JobGeneration,
    expected: JobEventSequence,
    fence: Option<JobTerminalPublicationFence>,
}

impl JobPublishedPermit {
    pub(crate) const fn id(&self) -> JobId {
        self.id
    }

    pub(crate) const fn generation(&self) -> JobGeneration {
        self.generation
    }

    pub(crate) const fn expected(&self) -> JobEventSequence {
        self.expected
    }

    pub(crate) fn completed(mut self) -> bool {
        if let Some(fence) = self.fence.take()
            && fence
                .inner
                .state
                .compare_exchange(
                    PUBLICATION_ACTIVE,
                    PUBLICATION_OPEN,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
        {
            fence.inner.changed.notify_waiters();
            true
        } else {
            false
        }
    }
}

impl std::fmt::Debug for JobPublishedPermit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JobPublishedPermit")
            .field("id", &self.id)
            .field("generation", &self.generation)
            .field("expected", &self.expected)
            .field("fence", &"[SEALED TERMINAL PUBLICATION FENCE]")
            .finish()
    }
}

/// Cancellation and event authority for one runner generation.
#[derive(Clone)]
pub struct JobRunContext {
    snapshot: JobSnapshot,
    cancellation: CancellationToken,
    events: Arc<dyn JobEventSink>,
    terminal_publication: JobTerminalPublicationFence,
}

impl JobRunContext {
    /// Binds a recovered or newly admitted snapshot to generation-scoped capabilities.
    #[must_use]
    pub(crate) fn new(
        snapshot: JobSnapshot,
        cancellation: CancellationToken,
        events: Arc<dyn JobEventSink>,
        terminal_publication: JobTerminalPublicationFence,
    ) -> Self {
        Self {
            snapshot,
            cancellation,
            events,
            terminal_publication,
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

    /// Claims the exact generation sequence immediately before irreversible domain publication.
    ///
    /// # Errors
    ///
    /// Returns [`JobRunError::Cancelled`] when durable cancellation won the generation gate.
    /// Returns [`JobRunError::Recovery`] when the expected sequence is stale or durable job state
    /// cannot be validated.
    pub fn claim_terminal_publication(
        &self,
        expected: JobEventSequence,
    ) -> Result<JobTerminalPublicationPermit, JobRunError> {
        self.terminal_publication.claim_publication(expected)
    }
}

impl std::fmt::Debug for JobRunContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JobRunContext")
            .field("snapshot", &self.snapshot)
            .field("cancellation", &"[CANCELLATION TOKEN]")
            .field("events", &"[JOB EVENT CAPABILITY]")
            .field("terminal_publication", &"[TERMINAL PUBLICATION AUTHORITY]")
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
#[derive(Debug)]
pub enum JobCompletion {
    /// Domain authority published the immutable result while holding the generation gate.
    Published(JobResultReference, JobPublishedPermit),
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
