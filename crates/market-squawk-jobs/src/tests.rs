use std::{error::Error, sync::Arc, time::Duration};

use async_trait::async_trait;
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp};
use market_squawk_platform::LocalPaths;
use market_squawk_services::{ArtifactReference, RequestId};
use tempfile::TempDir;
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
