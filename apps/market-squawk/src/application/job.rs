//! Transport-neutral application access to the sole durable job authority.

use std::{fmt, sync::Arc};

use market_squawk_domain::{SourceIdentifier, Timestamp};
use market_squawk_jobs::{
    AdmittedJobInput, AdmittedJobSpec, JobAttemptLimit, JobAuthority, JobAuthorityError,
    JobAuthoritySnapshot, JobConfirmation, JobContractError, JobEventPage, JobEventPageLimit,
    JobEventSequence, JobFailure, JobGeneration, JobId, JobListCursor, JobListPageLimit, JobOrigin,
    JobRepository, JobRepositoryError, JobResultReference, JobSnapshot, JobState,
};
use market_squawk_services::{RequestId, ToolAuthorization, ToolDescriptor, TypedToolRequest};
use rust_decimal::Decimal;
use serde::Serialize;
use thiserror::Error;
use uuid::Uuid;

/// Re-admits a `Start*` request through the exact existing terminal-operation descriptor.
///
/// This code-owned translation preserves the already admitted argument object while preventing a
/// caller from selecting either operation name dynamically. The `confirm` marker is removed only
/// when the terminal authority is read-only, because that descriptor correctly rejects mutation
/// fields.
pub fn terminal_request_for_start(
    start: &TypedToolRequest,
    expected_start: &str,
    terminal: &ToolDescriptor,
    expected_terminal: &str,
) -> Result<TypedToolRequest, JobApplicationError> {
    if start.name() != expected_start
        || terminal.name() != expected_terminal
        || start.version() != terminal.version()
        || start.contract().domain() != terminal.contract().domain()
    {
        return Err(JobApplicationError::Contract);
    }
    let mut arguments = start.arguments().clone();
    if matches!(
        terminal.contract().authorization(),
        ToolAuthorization::ReadOnly
    ) {
        arguments.remove("confirm");
    }
    terminal
        .admit(arguments)
        .map_err(|_error| JobApplicationError::Contract)
}

/// Immutable admission supplied by one code-owned application runner.
#[derive(Clone, Debug)]
pub struct JobAdmission {
    kind: SourceIdentifier,
    input: AdmittedJobInput,
    authority: JobAuthoritySnapshot,
    attempt_limit: JobAttemptLimit,
}

impl JobAdmission {
    /// Binds an exact runner kind, immutable input, terminal authority, and retry ceiling.
    #[must_use]
    pub const fn new(
        kind: SourceIdentifier,
        input: AdmittedJobInput,
        authority: JobAuthoritySnapshot,
        attempt_limit: JobAttemptLimit,
    ) -> Self {
        Self {
            kind,
            input,
            authority,
            attempt_limit,
        }
    }

    /// Code-owned runner kind.
    #[must_use]
    pub const fn kind(&self) -> &SourceIdentifier {
        &self.kind
    }

    /// Immutable path-free input identity.
    #[must_use]
    pub const fn input(&self) -> &AdmittedJobInput {
        &self.input
    }

    /// Converts this admission into the exact durable first-generation specification.
    pub fn into_spec(
        self,
        id: JobId,
        origin: JobOrigin,
        request_id: RequestId,
        admitted_at: Timestamp,
    ) -> Result<AdmittedJobSpec, JobApplicationError> {
        Ok(AdmittedJobSpec::try_new(
            id,
            JobGeneration::try_new(1)?,
            self.kind,
            origin,
            request_id,
            self.input,
            self.authority,
            self.attempt_limit,
            admitted_at,
        )?)
    }
}

/// Immediate durable acknowledgement returned by every `Start*` operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobReceipt {
    job_id: JobId,
    generation: JobGeneration,
    sequence: JobEventSequence,
    state: JobState,
}

impl JobReceipt {
    fn from_snapshot(snapshot: &JobSnapshot) -> Self {
        Self {
            job_id: snapshot.id(),
            generation: snapshot.generation(),
            sequence: snapshot.sequence(),
            state: snapshot.state(),
        }
    }

    /// Stable identity used to reconnect from another client session.
    #[must_use]
    pub const fn job_id(self) -> JobId {
        self.job_id
    }

    /// Exact execution generation admitted by this receipt.
    #[must_use]
    pub const fn generation(self) -> JobGeneration {
        self.generation
    }

    /// Last event sequence known when the receipt was returned.
    #[must_use]
    pub const fn sequence(self) -> JobEventSequence {
        self.sequence
    }

    /// Durable state at acknowledgement time.
    #[must_use]
    pub const fn state(self) -> JobState {
        self.state
    }
}

/// Bounded, path-free job presentation that omits internal authority snapshots.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobView {
    job_id: JobId,
    generation: JobGeneration,
    sequence: JobEventSequence,
    kind: SourceIdentifier,
    state: JobState,
    phase: Option<SourceIdentifier>,
    completed_units: Option<u64>,
    total_units: Option<u64>,
    cancellation_requested: bool,
    result: Option<JobResultReference>,
    failure: Option<JobFailure>,
    updated_at: Timestamp,
    recovery: Option<SourceIdentifier>,
    #[serde(skip)]
    started_at: Timestamp,
}

impl JobView {
    fn from_snapshot(snapshot: &JobSnapshot) -> Result<Self, JobApplicationError> {
        let progress = snapshot.current_progress();
        let recovery = match snapshot.state() {
            JobState::Interrupted => Some(identifier("interrupted-requires-explicit-retry")?),
            JobState::Recovering => Some(identifier("recovering-from-immutable-input")?),
            _ => None,
        };
        Ok(Self {
            job_id: snapshot.id(),
            generation: snapshot.generation(),
            sequence: snapshot.sequence(),
            kind: snapshot.spec().kind().clone(),
            state: snapshot.state(),
            phase: progress.map(|value| value.phase().clone()),
            completed_units: progress.map(|value| value.completed()),
            total_units: progress.and_then(|value| value.total()),
            cancellation_requested: snapshot.cancellation_requested(),
            result: snapshot.terminal_result().cloned(),
            failure: snapshot.terminal_failure().cloned(),
            updated_at: snapshot.updated_at_timestamp(),
            recovery,
            started_at: snapshot.spec().admitted_at(),
        })
    }

    /// Stable job identity.
    #[must_use]
    pub const fn job_id(&self) -> JobId {
        self.job_id
    }

    /// Exact generation represented by this view.
    #[must_use]
    pub const fn generation(&self) -> JobGeneration {
        self.generation
    }

    /// Last accepted event sequence.
    #[must_use]
    pub const fn sequence(&self) -> JobEventSequence {
        self.sequence
    }

    /// Code-owned runner kind for this job.
    #[must_use]
    pub const fn kind(&self) -> &SourceIdentifier {
        &self.kind
    }

    /// Current durable lifecycle state.
    #[must_use]
    pub const fn state(&self) -> JobState {
        self.state
    }

    /// Durable admission time retained for product activity projections.
    #[must_use]
    pub const fn started_at(&self) -> Timestamp {
        self.started_at
    }

    /// Last durable lifecycle update time.
    #[must_use]
    pub const fn updated_at(&self) -> Timestamp {
        self.updated_at
    }

    /// Last completed objective units when the runner reported measurable progress.
    #[must_use]
    pub const fn completed_units(&self) -> Option<u64> {
        self.completed_units
    }

    /// Last total objective units when the runner reported measurable progress.
    #[must_use]
    pub const fn total_units(&self) -> Option<u64> {
        self.total_units
    }

    /// Whether cooperative cancellation was requested for this generation.
    #[must_use]
    pub const fn cancellation_requested(&self) -> bool {
        self.cancellation_requested
    }

    /// Immutable terminal result reference, if the domain authority completed publication.
    #[must_use]
    pub const fn result(&self) -> Option<&JobResultReference> {
        self.result.as_ref()
    }

    /// Typed terminal failure, when the job failed.
    #[must_use]
    pub const fn failure(&self) -> Option<&JobFailure> {
        self.failure.as_ref()
    }
}

/// Bounded stable page of sanitized latest-generation jobs.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobViewPage {
    jobs: Box<[JobView]>,
    next: Option<JobListCursor>,
}

impl JobViewPage {
    /// Sanitized jobs in stable job-identity order.
    #[must_use]
    pub fn jobs(&self) -> &[JobView] {
        &self.jobs
    }

    /// Opaque bounded continuation, when another page exists.
    #[must_use]
    pub const fn next(&self) -> Option<&JobListCursor> {
        self.next.as_ref()
    }
}

/// Closed typed facade over one durable repository and its runner authority.
pub struct JobApplication<R: JobRepository + 'static> {
    repository: Arc<R>,
    authority: Arc<JobAuthority<R>>,
}

impl<R: JobRepository + 'static> JobApplication<R> {
    /// Binds the exact repository and authority pair owned by the installed service.
    #[must_use]
    pub const fn new(repository: Arc<R>, authority: Arc<JobAuthority<R>>) -> Self {
        Self {
            repository,
            authority,
        }
    }

    /// Durably admits one job and returns without waiting for runner completion.
    pub async fn start(
        &self,
        admission: JobAdmission,
        origin: JobOrigin,
        request_id: RequestId,
        admitted_at: Timestamp,
    ) -> Result<JobReceipt, JobApplicationError> {
        let spec = admission.into_spec(
            JobId::try_from_uuid(Uuid::new_v4())?,
            origin,
            request_id,
            admitted_at,
        )?;
        self.authority
            .start(&spec)
            .await
            .map(|snapshot| JobReceipt::from_snapshot(&snapshot))
            .map_err(Into::into)
    }

    /// Returns one sanitized view for an exact stable identity and execution generation.
    pub async fn get(
        &self,
        id: JobId,
        generation: JobGeneration,
    ) -> Result<JobView, JobApplicationError> {
        let snapshot = self.repository.get(id, generation).await?;
        JobView::from_snapshot(&snapshot)
    }

    /// Lists latest-generation views under the repository's fixed page ceiling.
    pub async fn list(
        &self,
        cursor: Option<&JobListCursor>,
        limit: JobListPageLimit,
    ) -> Result<JobViewPage, JobApplicationError> {
        let page = self.repository.list(cursor, limit).await?;
        let jobs = page
            .snapshots()
            .iter()
            .map(JobView::from_snapshot)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(JobViewPage {
            jobs: jobs.into_boxed_slice(),
            next: page.next().cloned(),
        })
    }

    /// Returns a bounded page of safe generation events after the exact observed sequence.
    pub async fn watch(
        &self,
        id: JobId,
        generation: JobGeneration,
        after: JobEventSequence,
        limit: JobEventPageLimit,
    ) -> Result<JobEventPage, JobApplicationError> {
        self.repository
            .events_after(id, generation, after, limit)
            .await
            .map_err(Into::into)
    }

    /// Requests cooperative cancellation using exact generation and sequence fencing.
    pub async fn cancel(
        &self,
        id: JobId,
        generation: JobGeneration,
        expected: JobEventSequence,
        at: Timestamp,
    ) -> Result<JobView, JobApplicationError> {
        let snapshot = self.authority.cancel(id, generation, expected, at).await?;
        JobView::from_snapshot(&snapshot)
    }

    /// Confirms one exact generation-bound request without replaying another sequence.
    pub async fn confirm(
        &self,
        confirmation: &JobConfirmation,
        at: Timestamp,
    ) -> Result<JobView, JobApplicationError> {
        let snapshot = self.authority.confirm(confirmation, at).await?;
        JobView::from_snapshot(&snapshot)
    }

    /// Starts the next bounded generation after one exact retryable failure.
    pub async fn retry(
        &self,
        id: JobId,
        generation: JobGeneration,
        expected: JobEventSequence,
        at: Timestamp,
    ) -> Result<JobReceipt, JobApplicationError> {
        self.authority
            .retry(id, generation, expected, at)
            .await
            .map(|snapshot| JobReceipt::from_snapshot(&snapshot))
            .map_err(Into::into)
    }
}

impl<R: JobRepository + 'static> fmt::Debug for JobApplication<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JobApplication")
            .field("repository", &"[DURABLE JOB REPOSITORY]")
            .field("authority", &"[JOB RUNNER AUTHORITY]")
            .finish()
    }
}

pub(crate) fn product_activity_state(view: &JobView) -> (&'static str, &'static str) {
    match view.state() {
        JobState::Queued => ("queued", "Waiting to start"),
        JobState::Preparing | JobState::Recovering => ("running", "Preparing research inputs"),
        JobState::Running => ("running", "Research is running"),
        JobState::AwaitingConfirmation => ("running", "Waiting for confirmation"),
        JobState::Cancelling => ("running", "Stopping research"),
        JobState::Completed => ("completed", "Research completed"),
        JobState::Failed | JobState::Cancelled | JobState::Interrupted => {
            ("failed", "Research did not complete")
        }
    }
}

pub(crate) fn product_progress_percent(view: &JobView) -> Option<String> {
    match (view.completed_units(), view.total_units()) {
        (Some(completed), Some(total)) if total > 0 && completed <= total => {
            let completed = Decimal::from(completed);
            let total = Decimal::from(total);
            Some(
                (completed * Decimal::from(100_u32) / total)
                    .normalize()
                    .to_string(),
            )
        }
        _ if view.state() == JobState::Completed => Some("100".to_owned()),
        _ if view.state() == JobState::Queued => Some("0".to_owned()),
        _ => None,
    }
}

/// Durable job application failure without database, path, process, or log disclosure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum JobApplicationError {
    /// An application runner supplied an invalid closed contract.
    #[error("job application contract is invalid")]
    Contract,
    /// The requested durable job identity does not exist.
    #[error("durable job was not found")]
    NotFound,
    /// Durable job state could not be read or changed.
    #[error("durable job repository is unavailable or rejected the operation")]
    Repository,
    /// The runner scheduler rejected or could not complete the operation.
    #[error("durable job authority rejected or could not complete the operation")]
    Authority,
}

impl From<JobContractError> for JobApplicationError {
    fn from(_error: JobContractError) -> Self {
        Self::Contract
    }
}

impl From<JobRepositoryError> for JobApplicationError {
    fn from(error: JobRepositoryError) -> Self {
        if matches!(error, JobRepositoryError::NotFound) {
            Self::NotFound
        } else {
            Self::Repository
        }
    }
}

impl From<JobAuthorityError> for JobApplicationError {
    fn from(_error: JobAuthorityError) -> Self {
        Self::Authority
    }
}

fn identifier(value: &str) -> Result<SourceIdentifier, JobApplicationError> {
    SourceIdentifier::try_from(value).map_err(|_error| JobApplicationError::Contract)
}
