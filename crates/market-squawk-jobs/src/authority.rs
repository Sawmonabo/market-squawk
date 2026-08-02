use std::{collections::BTreeMap, sync::Arc, time::Duration};

use market_squawk_domain::{SourceIdentifier, Timestamp};
use tokio::{
    sync::{RwLock, oneshot},
    task::AbortHandle,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    AdmittedJobSpec, FairJobScheduler, JobCompletion, JobConfirmation, JobContractError, JobEvent,
    JobEventSequence, JobFailure, JobGeneration, JobId, JobLease, JobRecoveryDisposition,
    JobRepository, JobRepositoryError, JobRunContext, JobRunError, JobRunner, JobSnapshot,
    JobState, ScheduledJob, SchedulerError, SchedulerLimits,
};

/// Complete job-authority failure without transport or storage leakage.
#[derive(Clone, Copy, Debug, Eq, thiserror::Error, PartialEq)]
pub enum JobAuthorityError {
    /// No runner owns the admitted job kind.
    #[error("job kind is not registered")]
    UnknownKind,
    /// The finite scheduler refused more queued work.
    #[error("job scheduler capacity is exhausted")]
    Capacity,
    /// Durable repository authority failed.
    #[error("durable job repository failed")]
    Repository,
    /// An admitted job contract could not advance safely.
    #[error("durable job contract failed")]
    Contract,
    /// Owned runner tasks did not reap after cancellation and forced abort.
    #[error("job authority shutdown did not reap owned tasks")]
    ShutdownIncomplete,
}

/// Bounded authority shutdown result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobShutdownOutcome {
    /// Every owned runner stopped inside the requested deadline.
    Clean,
    /// The deadline elapsed; remaining generations were durably interrupted.
    DeadlineExceeded {
        /// Number of still-owned generations durably ended as interrupted.
        interrupted_generations: usize,
    },
}

/// Recovers one orphaned generation according to the registered runner's closed policy.
pub async fn recover_one<R: JobRepository + ?Sized, J: JobRunner + ?Sized>(
    repository: &R,
    runner: &J,
    at: Timestamp,
) -> Result<JobSnapshot, JobAuthorityError> {
    let page = repository
        .recover_nonterminal(
            None,
            crate::RecoveryPageLimit::try_new(1).map_err(map_contract)?,
        )
        .await
        .map_err(map_repository)?;
    let orphaned = page
        .snapshots()
        .first()
        .ok_or(JobAuthorityError::Repository)?;
    if orphaned.spec().kind() != runner.kind() {
        return Err(JobAuthorityError::UnknownKind);
    }
    if orphaned.state() == JobState::Queued {
        return Ok(orphaned.clone());
    }
    apply_recovery_disposition(repository, orphaned, runner.recover(orphaned), at).await
}

async fn apply_recovery_disposition<R: JobRepository + ?Sized>(
    repository: &R,
    orphaned: &JobSnapshot,
    disposition: JobRecoveryDisposition,
    at: Timestamp,
) -> Result<JobSnapshot, JobAuthorityError> {
    match disposition {
        JobRecoveryDisposition::ResumeFromCheckpoint
        | JobRecoveryDisposition::RetryFromImmutableInput => repository
            .begin_recovery(orphaned, at)
            .await
            .map_err(map_repository),
        JobRecoveryDisposition::MarkInterrupted => {
            append_interrupted(repository, orphaned, at).await
        }
        JobRecoveryDisposition::Fail(failure) => {
            let event = JobEvent::try_new(JobState::Failed, at, None, None, Some(failure))
                .map_err(map_contract)?;
            repository
                .append(
                    orphaned.id(),
                    orphaned.generation(),
                    orphaned.sequence(),
                    event,
                )
                .await
                .map_err(map_repository)
        }
        JobRecoveryDisposition::CompleteAlreadyPublished(result) => {
            let event = JobEvent::try_new(JobState::Completed, at, None, Some(result), None)
                .map_err(map_contract)?;
            repository
                .append(
                    orphaned.id(),
                    orphaned.generation(),
                    orphaned.sequence(),
                    event,
                )
                .await
                .map_err(map_repository)
        }
    }
}

async fn append_interrupted<R: JobRepository + ?Sized>(
    repository: &R,
    orphaned: &JobSnapshot,
    at: Timestamp,
) -> Result<JobSnapshot, JobAuthorityError> {
    let event =
        JobEvent::try_new(JobState::Interrupted, at, None, None, None).map_err(map_contract)?;
    repository
        .append(
            orphaned.id(),
            orphaned.generation(),
            orphaned.sequence(),
            event,
        )
        .await
        .map_err(map_repository)
}

/// One-process scheduler and runner owner over a durable repository.
pub struct JobAuthority<R: JobRepository + 'static> {
    repository: Arc<R>,
    scheduler: FairJobScheduler,
    runners: Arc<BTreeMap<SourceIdentifier, Arc<dyn JobRunner>>>,
    cancellations: Arc<RwLock<BTreeMap<(JobId, JobGeneration), CancellationToken>>>,
    tasks: Arc<RwLock<BTreeMap<(JobId, JobGeneration), AbortHandle>>>,
    admission: Arc<RwLock<()>>,
    tracker: TaskTracker,
}

impl<R: JobRepository + 'static> std::fmt::Debug for JobAuthority<R> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JobAuthority")
            .field("repository", &"[DURABLE JOB REPOSITORY]")
            .field("scheduler", &self.scheduler)
            .field("runner_count", &self.runners.len())
            .field("cancellations", &"[GENERATION CANCELLATIONS]")
            .field("tasks", &"[GENERATION TASK HANDLES]")
            .finish_non_exhaustive()
    }
}

impl<R: JobRepository + 'static> JobAuthority<R> {
    /// Creates one authority and starts its bounded fair dispatch loop.
    pub fn try_new(
        repository: Arc<R>,
        limits: SchedulerLimits,
        runners: Vec<Arc<dyn JobRunner>>,
    ) -> Result<Self, JobAuthorityError> {
        let mut registry = BTreeMap::new();
        for runner in runners {
            if registry.insert(runner.kind().clone(), runner).is_some() {
                return Err(JobAuthorityError::UnknownKind);
            }
        }
        let scheduler = FairJobScheduler::new(limits);
        let tracker = TaskTracker::new();
        let authority = Self {
            repository,
            scheduler,
            runners: Arc::new(registry),
            cancellations: Arc::new(RwLock::new(BTreeMap::new())),
            tasks: Arc::new(RwLock::new(BTreeMap::new())),
            admission: Arc::new(RwLock::new(())),
            tracker,
        };
        authority.start_dispatch();
        Ok(authority)
    }

    /// Durably creates and fairly schedules one admitted job.
    pub async fn start(&self, spec: &AdmittedJobSpec) -> Result<JobSnapshot, JobAuthorityError> {
        let _admission = self.admission.read().await;
        if !self.runners.contains_key(spec.kind()) {
            return Err(JobAuthorityError::UnknownKind);
        }
        let reservation = self
            .scheduler
            .reserve(spec.kind().clone())
            .map_err(map_scheduler)?;
        let snapshot = self.repository.create(spec).await.map_err(map_repository)?;
        reservation
            .commit(ScheduledJob::new(
                snapshot.id(),
                snapshot.generation(),
                spec.kind().clone(),
            ))
            .map_err(map_scheduler)?;
        Ok(snapshot)
    }

    /// Persists cancellation intent before signalling the active runner generation.
    pub async fn cancel(
        &self,
        id: JobId,
        generation: JobGeneration,
        expected: JobEventSequence,
        at: Timestamp,
    ) -> Result<JobSnapshot, JobAuthorityError> {
        let current = self
            .repository
            .get(id, generation)
            .await
            .map_err(map_repository)?;
        if current.sequence() != expected {
            return Err(JobAuthorityError::Contract);
        }
        let snapshot = self
            .repository
            .request_cancellation(id, generation, expected, at)
            .await
            .map_err(map_repository)?;
        if let Some(cancellation) = self.cancellations.read().await.get(&(id, generation)) {
            cancellation.cancel();
        } else if current.state() == JobState::AwaitingConfirmation {
            let event =
                JobEvent::try_new(JobState::Cancelled, snapshot.updated_at(), None, None, None)
                    .map_err(map_contract)?;
            return self
                .repository
                .append(id, generation, snapshot.sequence(), event)
                .await
                .map_err(map_repository);
        }
        Ok(snapshot)
    }

    /// Confirms the exact pending request and resumes that same generation.
    pub async fn confirm(
        &self,
        confirmation: &JobConfirmation,
        at: Timestamp,
    ) -> Result<JobSnapshot, JobAuthorityError> {
        let _admission = self.admission.read().await;
        let current = self
            .repository
            .get(confirmation.id(), confirmation.generation())
            .await
            .map_err(map_repository)?;
        let pending = current
            .pending_confirmation()
            .ok_or(JobAuthorityError::Contract)?;
        if current.state() != JobState::AwaitingConfirmation
            || current.sequence() != confirmation.expected()
            || pending.identity() != confirmation.identity()
            || pending.digest() != confirmation.digest()
            || at > pending.expires_at()
        {
            return Err(JobAuthorityError::Contract);
        }
        let reservation = self
            .scheduler
            .reserve(current.spec().kind().clone())
            .map_err(map_scheduler)?;
        let event =
            JobEvent::try_new(JobState::Running, at, None, None, None).map_err(map_contract)?;
        let resumed = self
            .repository
            .append(
                confirmation.id(),
                confirmation.generation(),
                confirmation.expected(),
                event,
            )
            .await
            .map_err(map_repository)?;
        reservation
            .commit(ScheduledJob::new(
                resumed.id(),
                resumed.generation(),
                resumed.spec().kind().clone(),
            ))
            .map_err(map_scheduler)?;
        Ok(resumed)
    }

    /// Starts the exact next bounded generation after an explicitly retryable failure.
    pub async fn retry(
        &self,
        id: JobId,
        generation: JobGeneration,
        expected: JobEventSequence,
        at: Timestamp,
    ) -> Result<JobSnapshot, JobAuthorityError> {
        let _admission = self.admission.read().await;
        let failed = self
            .repository
            .get(id, generation)
            .await
            .map_err(map_repository)?;
        if failed.sequence() != expected {
            return Err(JobAuthorityError::Contract);
        }
        let reservation = self
            .scheduler
            .reserve(failed.spec().kind().clone())
            .map_err(map_scheduler)?;
        let retrying = self
            .repository
            .begin_retry(&failed, at)
            .await
            .map_err(map_repository)?;
        reservation
            .commit(ScheduledJob::new(
                retrying.id(),
                retrying.generation(),
                retrying.spec().kind().clone(),
            ))
            .map_err(map_scheduler)?;
        Ok(retrying)
    }

    /// Recovers all persisted nonterminal generations through bounded stable pages.
    pub async fn recover(&self, at: Timestamp) -> Result<usize, JobAuthorityError> {
        let _admission = self.admission.read().await;
        let limit = crate::RecoveryPageLimit::try_new(128).map_err(map_contract)?;
        let mut cursor = None;
        let mut recovered = 0_usize;
        loop {
            let page = self
                .repository
                .recover_nonterminal(cursor.as_ref(), limit)
                .await
                .map_err(map_repository)?;
            for orphaned in page.snapshots() {
                let runner = self
                    .runners
                    .get(orphaned.spec().kind())
                    .ok_or(JobAuthorityError::UnknownKind)?;
                let disposition =
                    (orphaned.state() != JobState::Queued).then(|| runner.recover(orphaned));
                let needs_queue = orphaned.state() == JobState::Queued
                    || matches!(
                        &disposition,
                        Some(
                            JobRecoveryDisposition::ResumeFromCheckpoint
                                | JobRecoveryDisposition::RetryFromImmutableInput
                        )
                    );
                let reservation = if needs_queue {
                    Some(
                        self.scheduler
                            .reserve(orphaned.spec().kind().clone())
                            .map_err(map_scheduler)?,
                    )
                } else {
                    None
                };
                let next = if orphaned.state() == JobState::Queued {
                    orphaned.clone()
                } else {
                    apply_recovery_disposition(
                        self.repository.as_ref(),
                        orphaned,
                        disposition.ok_or(JobAuthorityError::Contract)?,
                        at,
                    )
                    .await?
                };
                if !next.state().is_terminal() {
                    reservation
                        .ok_or(JobAuthorityError::Contract)?
                        .commit(ScheduledJob::new(
                            next.id(),
                            next.generation(),
                            next.spec().kind().clone(),
                        ))
                        .map_err(map_scheduler)?;
                }
                recovered = recovered
                    .checked_add(1)
                    .ok_or(JobAuthorityError::Contract)?;
            }
            let Some(next) = page.next() else {
                return Ok(recovered);
            };
            cursor = Some(next.clone());
        }
    }

    /// Stops admission, durably cancels owned work, and waits only until the explicit deadline.
    pub async fn shutdown(
        &self,
        at: Timestamp,
        deadline: Duration,
    ) -> Result<JobShutdownOutcome, JobAuthorityError> {
        if deadline.is_zero() || deadline > Duration::from_secs(30) {
            return Err(JobAuthorityError::Contract);
        }
        let _admission = self.admission.write().await;
        let queued = self.scheduler.close().map_err(map_scheduler)?;
        for job in queued {
            if let Some(cancelling) = request_shutdown_cancellation(
                self.repository.as_ref(),
                job.id(),
                job.generation(),
                at,
            )
            .await?
                && !cancelling.state().is_terminal()
            {
                let event = JobEvent::try_new(
                    JobState::Cancelled,
                    cancelling.updated_at(),
                    None,
                    None,
                    None,
                )
                .map_err(map_contract)?;
                self.repository
                    .append(
                        cancelling.id(),
                        cancelling.generation(),
                        cancelling.sequence(),
                        event,
                    )
                    .await
                    .map_err(map_repository)?;
            }
        }
        let running = self
            .cancellations
            .read()
            .await
            .iter()
            .map(|(identity, cancellation)| (*identity, cancellation.clone()))
            .collect::<Vec<_>>();
        for ((id, generation), cancellation) in &running {
            request_shutdown_cancellation(self.repository.as_ref(), *id, *generation, at).await?;
            cancellation.cancel();
        }
        self.tracker.close();
        if tokio::time::timeout(deadline, self.tracker.wait())
            .await
            .is_ok()
        {
            if !crate::process::await_contained_processes(Duration::from_secs(5)).await {
                return Err(JobAuthorityError::ShutdownIncomplete);
            }
            return Ok(JobShutdownOutcome::Clean);
        }
        let mut interrupted_generations = 0_usize;
        let active = self.tasks.read().await.keys().copied().collect::<Vec<_>>();
        for (id, generation) in &active {
            if let Some(cancellation) = self.cancellations.read().await.get(&(*id, *generation)) {
                cancellation.cancel();
            }
            let Ok(current) = self.repository.get(*id, *generation).await else {
                continue;
            };
            if current.state().is_terminal() {
                continue;
            }
            let event = JobEvent::try_new(
                JobState::Interrupted,
                current.updated_at(),
                None,
                None,
                None,
            )
            .map_err(map_contract)?;
            if self
                .repository
                .append(*id, *generation, current.sequence(), event)
                .await
                .is_ok()
            {
                interrupted_generations = interrupted_generations
                    .checked_add(1)
                    .ok_or(JobAuthorityError::Contract)?;
            }
        }
        let mut tasks = self.tasks.write().await;
        for (id, generation) in &active {
            if let Some(handle) = tasks.remove(&(*id, *generation)) {
                handle.abort();
            }
        }
        drop(tasks);
        let mut cancellations = self.cancellations.write().await;
        for (id, generation) in &active {
            cancellations.remove(&(*id, *generation));
        }
        drop(cancellations);
        tokio::time::timeout(Duration::from_secs(5), self.tracker.wait())
            .await
            .map_err(|_| JobAuthorityError::ShutdownIncomplete)?;
        if !crate::process::await_contained_processes(Duration::from_secs(5)).await {
            return Err(JobAuthorityError::ShutdownIncomplete);
        }
        Ok(JobShutdownOutcome::DeadlineExceeded {
            interrupted_generations,
        })
    }

    fn start_dispatch(&self) {
        let scheduler = self.scheduler.clone();
        let repository = self.repository.clone();
        let runners = self.runners.clone();
        let cancellations = self.cancellations.clone();
        let tasks = self.tasks.clone();
        let task_tracker = self.tracker.clone();
        self.tracker.spawn(async move {
            while let Some(lease) = scheduler.next().await {
                let job = lease.job().clone();
                let Some(runner) = runners.get(job.kind()).cloned() else {
                    lease.release();
                    continue;
                };
                let repository = repository.clone();
                let cancellations = cancellations.clone();
                let tasks_for_run = tasks.clone();
                let key = (job.id(), job.generation());
                let (start, started) = oneshot::channel();
                let handle = task_tracker.spawn(async move {
                    if started.await.is_ok() {
                        run_scheduled(repository, cancellations, tasks_for_run, runner, job, lease)
                            .await;
                    }
                });
                tasks.write().await.insert(key, handle.abort_handle());
                let _ignored = start.send(());
            }
        });
    }
}

async fn request_shutdown_cancellation<R: JobRepository + ?Sized>(
    repository: &R,
    id: JobId,
    generation: JobGeneration,
    at: Timestamp,
) -> Result<Option<JobSnapshot>, JobAuthorityError> {
    for _attempt in 0..8 {
        let current = repository
            .get(id, generation)
            .await
            .map_err(map_repository)?;
        if current.state().is_terminal() || current.state() == JobState::Cancelling {
            return Ok(Some(current));
        }
        match repository
            .request_cancellation(id, generation, current.sequence(), at)
            .await
        {
            Ok(snapshot) => return Ok(Some(snapshot)),
            Err(JobRepositoryError::Conflict) => continue,
            Err(error) => return Err(map_repository(error)),
        }
    }
    Err(JobAuthorityError::Repository)
}

async fn run_scheduled<R: JobRepository + 'static>(
    repository: Arc<R>,
    cancellations: Arc<RwLock<BTreeMap<(JobId, JobGeneration), CancellationToken>>>,
    tasks: Arc<RwLock<BTreeMap<(JobId, JobGeneration), AbortHandle>>>,
    runner: Arc<dyn JobRunner>,
    job: ScheduledJob,
    lease: JobLease,
) {
    let Ok(mut snapshot) = repository.get(job.id(), job.generation()).await else {
        finish_lease(&cancellations, &tasks, &job, lease).await;
        return;
    };
    let cancellation = CancellationToken::new();
    if snapshot.cancellation_requested() {
        cancellation.cancel();
    }
    cancellations
        .write()
        .await
        .insert((job.id(), job.generation()), cancellation.clone());

    if snapshot.state() == JobState::Queued {
        let Ok(preparing) =
            JobEvent::try_new(JobState::Preparing, snapshot.updated_at(), None, None, None)
        else {
            finish_lease(&cancellations, &tasks, &job, lease).await;
            return;
        };
        let Ok(next) = repository
            .append(job.id(), job.generation(), snapshot.sequence(), preparing)
            .await
        else {
            finish_lease(&cancellations, &tasks, &job, lease).await;
            return;
        };
        snapshot = next;
    }
    if matches!(snapshot.state(), JobState::Preparing | JobState::Recovering) {
        let Ok(running) =
            JobEvent::try_new(JobState::Running, snapshot.updated_at(), None, None, None)
        else {
            finish_lease(&cancellations, &tasks, &job, lease).await;
            return;
        };
        let Ok(next) = repository
            .append(job.id(), job.generation(), snapshot.sequence(), running)
            .await
        else {
            finish_lease(&cancellations, &tasks, &job, lease).await;
            return;
        };
        snapshot = next;
    }
    let sink = Arc::new(RepositoryEventSink {
        repository: repository.clone(),
        latest: RwLock::new(snapshot.clone()),
    });
    let context = JobRunContext::new(snapshot, cancellation, sink);
    let outcome = runner.run(context).await;
    if let Ok(latest) = repository.get(job.id(), job.generation()).await
        && let Ok(event) = completion_event(outcome, latest.updated_at())
    {
        let _ignored = repository
            .append(job.id(), job.generation(), latest.sequence(), event)
            .await;
    }
    finish_lease(&cancellations, &tasks, &job, lease).await;
}

async fn finish_lease(
    cancellations: &RwLock<BTreeMap<(JobId, JobGeneration), CancellationToken>>,
    tasks: &RwLock<BTreeMap<(JobId, JobGeneration), AbortHandle>>,
    job: &ScheduledJob,
    lease: JobLease,
) {
    cancellations
        .write()
        .await
        .remove(&(job.id(), job.generation()));
    tasks.write().await.remove(&(job.id(), job.generation()));
    lease.release();
}

#[derive(Debug)]
struct RepositoryEventSink<R: JobRepository> {
    repository: Arc<R>,
    latest: RwLock<JobSnapshot>,
}

#[async_trait::async_trait]
impl<R: JobRepository + 'static> crate::JobEventSink for RepositoryEventSink<R> {
    async fn append(
        &self,
        event: crate::JobRunnerEvent,
    ) -> Result<JobSnapshot, JobRepositoryError> {
        let current = self.latest.read().await.clone();
        let event = match event {
            crate::JobRunnerEvent::Progress(progress) => JobEvent::try_new(
                current.state(),
                progress.recorded_at(),
                Some(progress),
                None,
                None,
            ),
        }
        .map_err(|_| JobRepositoryError::InvalidState)?;
        let next = self
            .repository
            .append(
                current.id(),
                current.generation(),
                current.sequence(),
                event,
            )
            .await?;
        *self.latest.write().await = next.clone();
        Ok(next)
    }
}

fn completion_event(
    outcome: Result<JobCompletion, JobRunError>,
    at: Timestamp,
) -> Result<JobEvent, JobContractError> {
    match outcome {
        Ok(JobCompletion::Completed(result)) => {
            JobEvent::try_new(JobState::Completed, at, None, Some(result), None)
        }
        Ok(JobCompletion::AwaitingConfirmation(confirmation)) => {
            JobEvent::try_new_with_confirmation(
                JobState::AwaitingConfirmation,
                at,
                None,
                Some(confirmation),
                None,
                None,
            )
        }
        Ok(JobCompletion::Cancelled) | Err(JobRunError::Cancelled) => {
            JobEvent::try_new(JobState::Cancelled, at, None, None, None)
        }
        Err(JobRunError::Failed(failure)) => {
            JobEvent::try_new(JobState::Failed, at, None, None, Some(failure))
        }
        Err(JobRunError::Recovery) => JobEvent::try_new(
            JobState::Failed,
            at,
            None,
            None,
            Some(JobFailure::new(
                SourceIdentifier::try_from("recovery")
                    .map_err(|_| JobContractError::InvalidIdentityText)?,
                SourceIdentifier::try_from("runner-recovery-failed")
                    .map_err(|_| JobContractError::InvalidIdentityText)?,
                false,
            )),
        ),
    }
}

fn map_repository(_error: JobRepositoryError) -> JobAuthorityError {
    JobAuthorityError::Repository
}

fn map_scheduler(_error: SchedulerError) -> JobAuthorityError {
    JobAuthorityError::Capacity
}

fn map_contract(_error: JobContractError) -> JobAuthorityError {
    JobAuthorityError::Contract
}
