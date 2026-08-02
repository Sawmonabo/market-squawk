use std::{
    error::Error,
    future::ready,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp};
use market_squawk_platform::LocalPaths;
use market_squawk_services::{ArtifactReference, RequestId};
use tempfile::TempDir;
use tokio::sync::{Notify, oneshot};
use uuid::Uuid;

use super::*;

type TestError = Box<dyn Error + Send + Sync>;

fn source(value: &str) -> Result<SourceIdentifier, TestError> {
    Ok(SourceIdentifier::try_from(value)?)
}

fn generation(value: u64) -> Result<JobGeneration, TestError> {
    Ok(JobGeneration::try_new(value)?)
}

fn job_spec(kind: &str) -> Result<AdmittedJobSpec, TestError> {
    Ok(AdmittedJobSpec::try_new(
        JobId::try_from_uuid(Uuid::new_v4())?,
        generation(1)?,
        source(kind)?,
        JobOrigin::new(source("workspace-primary")?, source("desktop")?),
        RequestId::Integer(7),
        AdmittedJobInput::new(
            source("dataset-authority")?,
            source("dataset-v1")?,
            EvidenceDigest::new(DigestAlgorithm::Sha256, [7; 32]),
        ),
        JobAuthoritySnapshot::new(
            source("catalog-authority")?,
            source("catalog-generation-4")?,
            EvidenceDigest::new(DigestAlgorithm::Sha256, [9; 32]),
            Timestamp::from_unix_nanos(1),
        ),
        JobAttemptLimit::try_new(3)?,
        Timestamp::from_unix_nanos(2),
    )?)
}

async fn repository(temp: &TempDir) -> Result<Arc<SqliteJobRepository>, TestError> {
    let config = JobRepositoryConfig::try_new(Duration::from_millis(250), 16)?;
    let paths = LocalPaths::prepare(temp.path().join("data"))?;
    let location = paths.control_root()?.job_database_location();
    Ok(Arc::new(SqliteJobRepository::open(location, config).await?))
}

fn event(state: JobState, at: i64) -> Result<JobEvent, TestError> {
    Ok(JobEvent::try_new(
        state,
        Timestamp::from_unix_nanos(at),
        None,
        None,
        None,
    )?)
}

fn result_reference(identity: &str, byte: u8) -> Result<JobResultReference, TestError> {
    Ok(JobResultReference::try_new(
        source("result-authority")?,
        source(identity)?,
        EvidenceDigest::new(DigestAlgorithm::Sha256, [byte; 32]),
        Vec::new(),
    )?)
}

#[derive(Debug)]
struct PublicationRaceRunner {
    kind: SourceIdentifier,
    ready: StdMutex<Option<oneshot::Sender<JobSnapshot>>>,
    proceed: Notify,
    claimed: Notify,
    release: Notify,
    publication_began: AtomicBool,
    result: JobResultReference,
    terminal_error: Option<JobRunError>,
}

#[async_trait]
impl JobRunner for PublicationRaceRunner {
    fn kind(&self) -> &SourceIdentifier {
        &self.kind
    }

    async fn run(&self, context: JobRunContext) -> Result<JobCompletion, JobRunError> {
        let ready = self
            .ready
            .lock()
            .map_err(|_error| JobRunError::Recovery)?
            .take()
            .ok_or(JobRunError::Recovery)?;
        ready
            .send(context.snapshot().clone())
            .map_err(|_snapshot| JobRunError::Recovery)?;
        self.proceed.notified().await;
        let stale = context
            .snapshot()
            .sequence()
            .checked_next()
            .map_err(|_error| JobRunError::Recovery)?;
        if !matches!(
            context.claim_terminal_publication(stale),
            Err(JobRunError::Recovery)
        ) {
            return Err(JobRunError::Recovery);
        }
        let permit = context.claim_terminal_publication(context.snapshot().sequence())?;
        self.publication_began.store(true, Ordering::Release);
        self.claimed.notify_one();
        self.release.notified().await;
        let published = permit.seal();
        if let Some(error) = self.terminal_error.clone() {
            drop(published);
            return Err(error);
        }
        Ok(JobCompletion::Published(self.result.clone(), published))
    }

    fn recover(&self, _snapshot: &JobSnapshot) -> JobRecoveryDisposition {
        JobRecoveryDisposition::MarkInterrupted
    }
}

#[derive(Debug)]
struct StartSignalRunner {
    kind: SourceIdentifier,
    started: StdMutex<Option<oneshot::Sender<()>>>,
}

#[async_trait]
impl JobRunner for StartSignalRunner {
    fn kind(&self) -> &SourceIdentifier {
        &self.kind
    }

    async fn run(&self, _context: JobRunContext) -> Result<JobCompletion, JobRunError> {
        self.started
            .lock()
            .map_err(|_error| JobRunError::Recovery)?
            .take()
            .ok_or(JobRunError::Recovery)?
            .send(())
            .map_err(|()| JobRunError::Recovery)?;
        Ok(JobCompletion::Cancelled)
    }

    fn recover(&self, _snapshot: &JobSnapshot) -> JobRecoveryDisposition {
        JobRecoveryDisposition::MarkInterrupted
    }
}

async fn terminal_snapshot(
    repository: &SqliteJobRepository,
    id: JobId,
    generation: JobGeneration,
) -> Result<JobSnapshot, TestError> {
    Ok(tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let snapshot = repository.get(id, generation).await?;
            if snapshot.state().is_terminal() {
                return Ok::<_, JobRepositoryError>(snapshot);
            }
            tokio::task::yield_now().await;
        }
    })
    .await??)
}

#[tokio::test]
async fn cancellation_and_publication_have_one_generation_winner() -> Result<(), TestError> {
    let limits = SchedulerLimits::try_new(2, 2, 2, 2)?;

    let cancellation_temp = TempDir::new()?;
    let cancellation_repository = repository(&cancellation_temp).await?;
    let cancellation_spec = job_spec("cancellation-wins")?;
    let (cancellation_ready, cancellation_running) = oneshot::channel();
    let cancellation_runner = Arc::new(PublicationRaceRunner {
        kind: cancellation_spec.kind().clone(),
        ready: StdMutex::new(Some(cancellation_ready)),
        proceed: Notify::new(),
        claimed: Notify::new(),
        release: Notify::new(),
        publication_began: AtomicBool::new(false),
        result: result_reference("cancellation-result", 11)?,
        terminal_error: None,
    });
    let cancellation_authority = JobAuthority::try_new(
        Arc::clone(&cancellation_repository),
        limits,
        vec![cancellation_runner.clone()],
    )?;
    cancellation_authority.start(&cancellation_spec).await?;
    let running = cancellation_running.await?;
    let cancelling = cancellation_authority
        .cancel(
            running.id(),
            running.generation(),
            running.sequence(),
            Timestamp::from_unix_nanos(20),
        )
        .await?;
    assert_eq!(cancelling.state(), JobState::Cancelling);
    cancellation_runner.proceed.notify_one();
    let cancelled = terminal_snapshot(
        cancellation_repository.as_ref(),
        running.id(),
        running.generation(),
    )
    .await?;
    assert_eq!(cancelled.state(), JobState::Cancelled);
    assert!(
        !cancellation_runner
            .publication_began
            .load(Ordering::Acquire)
    );
    assert_eq!(
        cancellation_authority
            .shutdown(Timestamp::from_unix_nanos(21), Duration::from_secs(1))
            .await?,
        JobShutdownOutcome::Clean,
    );

    let publication_temp = TempDir::new()?;
    let publication_repository = repository(&publication_temp).await?;
    let publication_spec = job_spec("publication-wins")?;
    let (publication_ready, publication_running) = oneshot::channel();
    let publication_runner = Arc::new(PublicationRaceRunner {
        kind: publication_spec.kind().clone(),
        ready: StdMutex::new(Some(publication_ready)),
        proceed: Notify::new(),
        claimed: Notify::new(),
        release: Notify::new(),
        publication_began: AtomicBool::new(false),
        result: result_reference("published-result", 12)?,
        terminal_error: None,
    });
    let publication_authority = JobAuthority::try_new(
        Arc::clone(&publication_repository),
        limits,
        vec![publication_runner.clone()],
    )?;
    publication_authority.start(&publication_spec).await?;
    let running = publication_running.await?;
    publication_runner.proceed.notify_one();
    publication_runner.claimed.notified().await;
    let cancel = publication_authority.cancel(
        running.id(),
        running.generation(),
        running.sequence(),
        Timestamp::from_unix_nanos(30),
    );
    tokio::pin!(cancel);
    tokio::select! {
        biased;
        result = &mut cancel => return Err(format!("cancellation completed before publication: {result:?}").into()),
        () = ready(()) => {}
    }
    publication_runner.release.notify_one();
    let completed = cancel.await?;
    assert_eq!(completed.state(), JobState::Completed);
    assert!(publication_runner.publication_began.load(Ordering::Acquire));
    assert_eq!(
        publication_authority
            .shutdown(Timestamp::from_unix_nanos(31), Duration::from_secs(1))
            .await?,
        JobShutdownOutcome::Clean,
    );

    let reconciliation_temp = TempDir::new()?;
    let reconciliation_repository = repository(&reconciliation_temp).await?;
    let reconciliation_spec = job_spec("sealed-publication-error")?;
    let sentinel_spec = job_spec("publication-sentinel")?;
    let (reconciliation_ready, reconciliation_running) = oneshot::channel();
    let reconciliation_runner = Arc::new(PublicationRaceRunner {
        kind: reconciliation_spec.kind().clone(),
        ready: StdMutex::new(Some(reconciliation_ready)),
        proceed: Notify::new(),
        claimed: Notify::new(),
        release: Notify::new(),
        publication_began: AtomicBool::new(false),
        result: result_reference("reconciliation-result", 13)?,
        terminal_error: Some(JobRunError::Failed(JobFailure::new(
            source("result-shaping")?,
            source("post-commit-reference-failed")?,
            false,
        ))),
    });
    let (sentinel_started, sentinel_running) = oneshot::channel();
    let sentinel_runner = Arc::new(StartSignalRunner {
        kind: sentinel_spec.kind().clone(),
        started: StdMutex::new(Some(sentinel_started)),
    });
    let reconciliation_authority = JobAuthority::try_new(
        Arc::clone(&reconciliation_repository),
        SchedulerLimits::try_new(2, 1, 2, 1)?,
        vec![reconciliation_runner.clone(), sentinel_runner],
    )?;
    reconciliation_authority.start(&reconciliation_spec).await?;
    let running = reconciliation_running.await?;
    reconciliation_runner.proceed.notify_one();
    reconciliation_runner.claimed.notified().await;
    reconciliation_authority.start(&sentinel_spec).await?;
    reconciliation_runner.release.notify_one();
    sentinel_running.await?;
    let reconciliation = reconciliation_repository
        .get(running.id(), running.generation())
        .await?;
    assert_eq!(reconciliation.state(), JobState::Running);
    assert!(reconciliation.terminal_result().is_none());
    assert!(reconciliation.terminal_failure().is_none());
    assert!(
        reconciliation_runner
            .publication_began
            .load(Ordering::Acquire)
    );
    assert_eq!(
        reconciliation_authority
            .shutdown(Timestamp::from_unix_nanos(41), Duration::from_secs(1))
            .await?,
        JobShutdownOutcome::Clean,
    );
    Ok(())
}

#[tokio::test]
async fn lifecycle_rejects_illegal_transitions_and_stale_sequences() -> Result<(), TestError> {
    let temp = TempDir::new()?;
    let repository = repository(&temp).await?;
    let spec = job_spec("training")?;
    let queued = repository.create(&spec).await?;
    let preparing = repository
        .append(
            spec.id(),
            spec.generation(),
            queued.sequence(),
            event(JobState::Preparing, 3)?,
        )
        .await?;
    let running = repository
        .append(
            spec.id(),
            spec.generation(),
            preparing.sequence(),
            event(JobState::Running, 4)?,
        )
        .await?;

    assert_eq!(
        repository
            .append(
                spec.id(),
                spec.generation(),
                preparing.sequence(),
                event(JobState::Running, 5)?,
            )
            .await,
        Err(JobRepositoryError::Conflict),
    );
    assert_eq!(
        repository
            .append(
                spec.id(),
                spec.generation(),
                running.sequence(),
                event(JobState::Queued, 5)?,
            )
            .await,
        Err(JobRepositoryError::InvalidTransition),
    );
    assert_eq!(repository.get(spec.id(), spec.generation()).await?, running);
    Ok(())
}

#[tokio::test]
async fn cancellation_survives_disconnect_and_publishes_one_terminal_state() -> Result<(), TestError>
{
    let temp = TempDir::new()?;
    let spec = job_spec("dataset-build")?;
    let store = repository(&temp).await?;
    let queued = store.create(&spec).await?;
    let cancelling = store
        .request_cancellation(
            spec.id(),
            spec.generation(),
            queued.sequence(),
            Timestamp::from_unix_nanos(3),
        )
        .await?;
    store.shutdown().await?;
    drop(store);

    let reopened = repository(&temp).await?;
    let persisted = reopened.get(spec.id(), spec.generation()).await?;
    assert_eq!(persisted, cancelling);
    assert!(persisted.cancellation_requested());
    let cancelled = reopened
        .append(
            spec.id(),
            spec.generation(),
            persisted.sequence(),
            event(JobState::Cancelled, 4)?,
        )
        .await?;
    assert_eq!(cancelled.state(), JobState::Cancelled);
    assert_eq!(
        reopened
            .append(
                spec.id(),
                spec.generation(),
                cancelled.sequence(),
                event(JobState::Cancelled, 5)?,
            )
            .await,
        Err(JobRepositoryError::Terminal),
    );
    Ok(())
}

#[derive(Debug)]
struct RetryRunner {
    kind: SourceIdentifier,
}

#[async_trait]
impl JobRunner for RetryRunner {
    fn kind(&self) -> &SourceIdentifier {
        &self.kind
    }

    async fn run(&self, _context: JobRunContext) -> Result<JobCompletion, JobRunError> {
        Err(JobRunError::Cancelled)
    }

    fn recover(&self, _snapshot: &JobSnapshot) -> JobRecoveryDisposition {
        JobRecoveryDisposition::RetryFromImmutableInput
    }
}

#[tokio::test]
async fn reopen_applies_runner_policy_in_a_new_bounded_generation() -> Result<(), TestError> {
    let temp = TempDir::new()?;
    let spec = job_spec("training")?;
    let store = repository(&temp).await?;
    let queued = store.create(&spec).await?;
    let preparing = store
        .append(
            spec.id(),
            spec.generation(),
            queued.sequence(),
            event(JobState::Preparing, 3)?,
        )
        .await?;
    store
        .append(
            spec.id(),
            spec.generation(),
            preparing.sequence(),
            event(JobState::Running, 4)?,
        )
        .await?;
    store.shutdown().await?;
    drop(store);

    let reopened = repository(&temp).await?;
    let runner = RetryRunner {
        kind: source("training")?,
    };
    let recovered = recover_one(reopened.as_ref(), &runner, Timestamp::from_unix_nanos(5)).await?;
    assert_eq!(recovered.generation(), generation(2)?);
    assert_eq!(recovered.state(), JobState::Recovering);
    let interrupted = reopened.get(spec.id(), spec.generation()).await?;
    assert_eq!(interrupted.state(), JobState::Interrupted);
    Ok(())
}

#[test]
fn invalid_or_oversized_worker_output_never_releases_a_result() -> Result<(), TestError> {
    let result = JobResultReference::try_new(
        source("model-authority")?,
        source("candidate-v2")?,
        EvidenceDigest::new(DigestAlgorithm::Sha256, [4; 32]),
        vec![ArtifactReference::try_new(
            "model-bundle",
            "44b3b9942ed0f92d359ac3fcacc4d3e3285aac4d41020e88d2c8de2e2f09cc99",
            8,
            "application/octet-stream",
        )?],
    )?;
    let limits = WorkerProtocolLimits::try_new(256, 8)?;
    let mut protocol = WorkerProtocolSession::new(limits);
    protocol.accept(WorkerEvent::Result(result))?;
    assert_eq!(
        protocol.accept_encoded(&vec![b'x'; 257]),
        Err(WorkerProtocolError::EventTooLarge),
    );
    assert_eq!(protocol.finish_success(), Err(WorkerProtocolError::Sealed));
    assert!(protocol.result().is_none());

    let crash_result = JobResultReference::try_new(
        source("model-authority")?,
        source("candidate-v3")?,
        EvidenceDigest::new(DigestAlgorithm::Sha256, [5; 32]),
        Vec::new(),
    )?;
    let mut crashed = WorkerProtocolSession::new(limits);
    crashed.accept(WorkerEvent::Result(crash_result))?;
    crashed.finish_crashed();
    assert_eq!(crashed.finish_success(), Err(WorkerProtocolError::Sealed));
    assert!(crashed.result().is_none());
    Ok(())
}
