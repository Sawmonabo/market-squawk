//! Installed-service ownership of durable job persistence and bounded execution.

mod backtest;
mod backup;
mod forecast;
mod recovery;
mod research;
mod scenario;
mod screen;
mod training;
mod update;

pub use backtest::{BacktestJobRunner, BacktestJobRunnerError};
pub use backup::{
    BackupJobAction, BackupJobAuthority, BackupJobCommand, BackupJobRunner, LifecycleJobAuthority,
    LifecycleJobCommand, LifecycleJobExecutionError, LifecycleJobPublication,
    LifecycleJobPublicationError, LifecycleJobRunnerError,
};
pub use forecast::{ForecastJobRunner, ForecastJobRunnerError};
pub use recovery::{
    RecoveryJobAction, RecoveryJobAuthority, RecoveryJobCommand, RecoveryJobRunner,
};
pub use research::{
    DatasetJobRunner, ResearchExportJobRunner, ResearchJobRunner, ResearchJobRunnerError,
};
pub use scenario::ScenarioJobRunner;
pub use screen::{ScreenJobCommand, ScreenJobRunner, ScreenJobRunnerError};
pub use training::{GovernedTrainingInput, TrainingJobRunner, TrainingJobRunnerError};
pub use update::{UpdateJobAuthority, UpdateJobCommand, UpdateJobRunner};

use std::{
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

use market_squawk_domain::Timestamp;
use market_squawk_jobs::{
    JobAuthority, JobAuthorityError, JobEventSequence, JobPublishedPermit, JobRepositoryConfig,
    JobRepositoryError, JobRunContext, JobRunError, JobRunner, JobShutdownOutcome, SchedulerError,
    SchedulerLimits, SqliteJobRepository,
};
use market_squawk_platform::LocalPaths;
use thiserror::Error;

const JOB_DATABASE_BUSY_TIMEOUT: Duration = Duration::from_millis(750);
const JOB_WRITER_QUEUE_CAPACITY: usize = 256;
const MAXIMUM_QUEUED_JOBS: usize = 256;
const MAXIMUM_RUNNING_JOBS: usize = 8;
const MAXIMUM_QUEUED_PER_KIND: usize = 64;
const MAXIMUM_RUNNING_PER_KIND: usize = 2;
const RUNNER_PENDING_CAPACITY: usize = 256;
const RUNNER_DEADLINE: Duration = Duration::from_secs(60 * 60);

/// Code-owned installed runner set retained for both scheduling and typed admission.
pub struct InstalledJobRunners {
    ingest: Arc<ResearchJobRunner>,
    dataset: Arc<DatasetJobRunner>,
    feature: Arc<DatasetJobRunner>,
    export: Arc<ResearchExportJobRunner>,
    scenario: Arc<ScenarioJobRunner>,
    backtest: Arc<BacktestJobRunner>,
    backtest_registrar: Arc<dyn crate::application::analysis::GovernedBacktestInputRegistrar>,
    forecast: Arc<ForecastJobRunner>,
    training: Option<Arc<TrainingJobRunner>>,
    screen: Arc<ScreenJobRunner>,
}

impl InstalledJobRunners {
    /// Binds every installed runner to the same product authorities used by synchronous calls.
    pub fn try_new(product: &crate::LocalProduct) -> Result<Self, InstalledJobError> {
        let artifacts = product.artifacts();
        let ingest_authority: Arc<dyn crate::application::ResearchIngestCoordinator> =
            product.research_ingest();
        let ingest = Arc::new(
            ResearchJobRunner::try_new_ingest(
                product.research_domain(),
                ingest_authority,
                Arc::clone(&artifacts),
                RUNNER_PENDING_CAPACITY,
                RUNNER_DEADLINE,
            )
            .map_err(|_error| InstalledJobError::RunnerComposition)?,
        );
        let dataset = Arc::new(
            DatasetJobRunner::try_new_dataset(
                product.research(),
                Arc::clone(&artifacts),
                RUNNER_PENDING_CAPACITY,
                RUNNER_DEADLINE,
            )
            .map_err(|_error| InstalledJobError::RunnerComposition)?,
        );
        let feature = Arc::new(
            DatasetJobRunner::try_new_feature(
                product.research(),
                Arc::clone(&artifacts),
                RUNNER_PENDING_CAPACITY,
                RUNNER_DEADLINE,
            )
            .map_err(|_error| InstalledJobError::RunnerComposition)?,
        );
        let export = Arc::new(
            ResearchExportJobRunner::try_new(
                product.research_domain(),
                Arc::clone(&artifacts),
                RUNNER_PENDING_CAPACITY,
                RUNNER_DEADLINE,
            )
            .map_err(|_error| InstalledJobError::RunnerComposition)?,
        );
        let scenario = Arc::new(
            ScenarioJobRunner::try_new(
                product.analysis_domain(),
                Arc::clone(&artifacts),
                RUNNER_PENDING_CAPACITY,
                RUNNER_DEADLINE,
            )
            .map_err(|_error| InstalledJobError::RunnerComposition)?,
        );
        let backtest = Arc::new(
            BacktestJobRunner::try_new(
                product.backtests(),
                RUNNER_PENDING_CAPACITY,
                RUNNER_DEADLINE,
            )
            .map_err(|_error| InstalledJobError::RunnerComposition)?,
        );
        let forecast = Arc::new(
            ForecastJobRunner::try_new(
                product.model_domain(),
                artifacts,
                RUNNER_PENDING_CAPACITY,
                RUNNER_DEADLINE,
            )
            .map_err(|_error| InstalledJobError::RunnerComposition)?,
        );
        let training = product
            .model_runtime()
            .map(|runtime| {
                TrainingJobRunner::try_new(
                    product.paths(),
                    runtime,
                    RUNNER_PENDING_CAPACITY,
                    RUNNER_DEADLINE,
                )
                .map(Arc::new)
                .map_err(|_error| InstalledJobError::RunnerComposition)
            })
            .transpose()?;
        let screen = Arc::new(
            ScreenJobRunner::try_new(product.decisions(), RUNNER_PENDING_CAPACITY)
                .map_err(|_error| InstalledJobError::RunnerComposition)?,
        );
        Ok(Self {
            ingest,
            dataset,
            feature,
            export,
            scenario,
            backtest,
            backtest_registrar: product.backtest_registrar(),
            forecast,
            training,
            screen,
        })
    }

    /// Returns a deterministic registration list while retaining typed admission handles.
    pub fn registered(&self) -> Vec<Arc<dyn JobRunner>> {
        let mut runners: Vec<Arc<dyn JobRunner>> = vec![
            self.ingest.clone(),
            self.dataset.clone(),
            self.feature.clone(),
            self.export.clone(),
            self.scenario.clone(),
            self.backtest.clone(),
            self.forecast.clone(),
            self.screen.clone(),
        ];
        if let Some(training) = &self.training {
            runners.push(training.clone());
        }
        runners
    }

    pub(crate) const fn ingest(&self) -> &Arc<ResearchJobRunner> {
        &self.ingest
    }

    pub(crate) const fn export(&self) -> &Arc<ResearchExportJobRunner> {
        &self.export
    }

    pub(crate) const fn dataset(&self) -> &Arc<DatasetJobRunner> {
        &self.dataset
    }

    pub(crate) const fn feature(&self) -> &Arc<DatasetJobRunner> {
        &self.feature
    }

    pub(crate) const fn scenario(&self) -> &Arc<ScenarioJobRunner> {
        &self.scenario
    }

    pub(crate) const fn backtest(&self) -> &Arc<BacktestJobRunner> {
        &self.backtest
    }

    pub(crate) fn backtest_registrar(
        &self,
    ) -> Arc<dyn crate::application::analysis::GovernedBacktestInputRegistrar> {
        Arc::clone(&self.backtest_registrar)
    }

    pub(crate) fn training(&self) -> Option<&Arc<TrainingJobRunner>> {
        self.training.as_ref()
    }

    pub(crate) const fn forecast(&self) -> &Arc<ForecastJobRunner> {
        &self.forecast
    }
}

impl fmt::Debug for InstalledJobRunners {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstalledJobRunners")
            .field("registered", &self.registered().len())
            .field("training", &self.training.is_some())
            .finish()
    }
}

/// One application-private slot retaining an unsealed publication permit across a domain commit.
pub(super) struct JobTerminalCommitSlot {
    context: JobRunContext,
    expected: JobEventSequence,
    permit: Mutex<Option<market_squawk_jobs::JobTerminalPublicationPermit>>,
    published: Mutex<Option<JobPublishedPermit>>,
}

impl JobTerminalCommitSlot {
    pub(super) fn new(context: &JobRunContext, expected: JobEventSequence) -> Self {
        Self {
            context: context.clone(),
            expected,
            permit: Mutex::new(None),
            published: Mutex::new(None),
        }
    }

    pub(super) fn claim(&self) -> Result<(), JobRunError> {
        let mut permit = match self.permit.lock() {
            Ok(permit) => permit,
            Err(poisoned) => poisoned.into_inner(),
        };
        if permit.is_some() {
            return Err(JobRunError::Recovery);
        }
        *permit = Some(self.context.claim_terminal_publication(self.expected)?);
        Ok(())
    }

    pub(super) fn seal_domain_commit(&self) {
        let permit = match self.permit.lock() {
            Ok(mut permit) => permit.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };
        let Some(permit) = permit else {
            return;
        };
        let mut published = match self.published.lock() {
            Ok(published) => published,
            Err(poisoned) => poisoned.into_inner(),
        };
        if published.is_none() {
            *published = Some(permit.seal());
        }
    }

    pub(super) fn take_published(&self) -> Result<JobPublishedPermit, JobRunError> {
        match self.published.lock() {
            Ok(mut published) => published.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        }
        .ok_or(JobRunError::Recovery)
    }
}

impl fmt::Debug for JobTerminalCommitSlot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JobTerminalCommitSlot")
            .field("expected", &self.expected)
            .field("permit", &"[TERMINAL PUBLICATION PERMIT]")
            .field("published", &"[SEALED PUBLICATION PERMIT]")
            .finish()
    }
}

/// Sole installed-process owner for durable job state and runner scheduling.
#[derive(Debug)]
pub struct InstalledJobAuthority {
    repository: Arc<SqliteJobRepository>,
    authority: Arc<JobAuthority<SqliteJobRepository>>,
}

impl InstalledJobAuthority {
    /// Opens the capability-confined job database, registers code-owned runners, and recovers
    /// every durable nonterminal generation before service publication.
    pub async fn open(
        paths: &LocalPaths,
        runners: Vec<Arc<dyn JobRunner>>,
        at: Timestamp,
    ) -> Result<Self, InstalledJobError> {
        let config =
            JobRepositoryConfig::try_new(JOB_DATABASE_BUSY_TIMEOUT, JOB_WRITER_QUEUE_CAPACITY)?;
        let limits = SchedulerLimits::try_new(
            MAXIMUM_QUEUED_JOBS,
            MAXIMUM_RUNNING_JOBS,
            MAXIMUM_QUEUED_PER_KIND,
            MAXIMUM_RUNNING_PER_KIND,
        )?;
        let repository = Arc::new(
            SqliteJobRepository::open(paths.control_root()?.job_database_location(), config)
                .await?,
        );
        let authority = match JobAuthority::try_new(Arc::clone(&repository), limits, runners) {
            Ok(authority) => Arc::new(authority),
            Err(error) => {
                return match repository.shutdown().await {
                    Ok(()) => Err(error.into()),
                    Err(repository_cleanup) => Err(InstalledJobError::StartupCleanup {
                        cause: error,
                        authority_cleanup: None,
                        repository_cleanup: Some(repository_cleanup),
                    }),
                };
            }
        };
        if let Err(startup) = authority.recover(at).await {
            let authority_cleanup = authority.shutdown(at, Duration::from_secs(15)).await.err();
            let repository_cleanup = repository.shutdown().await.err();
            if authority_cleanup.is_none() && repository_cleanup.is_none() {
                return Err(startup.into());
            }
            return Err(InstalledJobError::StartupCleanup {
                cause: startup,
                authority_cleanup,
                repository_cleanup,
            });
        }
        Ok(Self {
            repository,
            authority,
        })
    }

    /// Durable read authority shared with job resources and product clients.
    #[must_use]
    pub fn repository(&self) -> Arc<SqliteJobRepository> {
        Arc::clone(&self.repository)
    }

    /// Mutable job lifecycle authority shared by registered application job services.
    #[must_use]
    pub fn authority(&self) -> Arc<JobAuthority<SqliteJobRepository>> {
        Arc::clone(&self.authority)
    }

    /// Stops job admission and owned runners while leaving durable storage available.
    ///
    /// The installed service calls this before shutting down the domain authorities that runners
    /// may still need. It must call [`Self::shutdown_repository`] only after those authorities and
    /// their audit sinks have finished.
    pub async fn shutdown_authority(
        &self,
        at: Timestamp,
        runner_deadline: Duration,
    ) -> Result<JobShutdownOutcome, InstalledJobError> {
        self.authority
            .shutdown(at, runner_deadline)
            .await
            .map_err(Into::into)
    }

    /// Drains the sole SQLite writer after job runners and dependent domains have stopped.
    pub async fn shutdown_repository(&self) -> Result<(), InstalledJobError> {
        self.repository.shutdown().await.map_err(Into::into)
    }
}

/// Installed job composition, recovery, or shutdown failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum InstalledJobError {
    /// The local path capability required for the job database is unavailable.
    #[error("installed job storage path is unavailable")]
    Path,
    /// Durable job storage is unavailable or invalid.
    #[error(transparent)]
    Repository(#[from] JobRepositoryError),
    /// Scheduler limits are invalid.
    #[error(transparent)]
    Scheduler(#[from] SchedulerError),
    /// One or more code-owned runner adapters could not bind the installed authorities.
    #[error("installed job runner composition is invalid")]
    RunnerComposition,
    /// Job scheduling, recovery, or shutdown failed.
    #[error(transparent)]
    Authority(#[from] JobAuthorityError),
    /// Startup failed and one or more owned cleanup steps also failed.
    #[error(
        "installed job startup failed ({cause}); authority cleanup: {authority_cleanup:?}; \
         repository cleanup: {repository_cleanup:?}"
    )]
    StartupCleanup {
        /// Original authority construction or recovery failure.
        cause: JobAuthorityError,
        /// Runner-authority cleanup failure, when an authority had been constructed.
        authority_cleanup: Option<JobAuthorityError>,
        /// Durable repository cleanup failure.
        repository_cleanup: Option<JobRepositoryError>,
    },
}

impl From<market_squawk_platform::PathError> for InstalledJobError {
    fn from(_error: market_squawk_platform::PathError) -> Self {
        Self::Path
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, time::Duration};

    use market_squawk_domain::Timestamp;
    use market_squawk_jobs::{JobRepository as _, RecoveryPageLimit};
    use market_squawk_platform::LocalPaths;

    use super::InstalledJobAuthority;

    type TestError = Box<dyn Error + Send + Sync>;

    #[tokio::test]
    async fn runner_authority_stops_before_durable_storage() -> Result<(), TestError> {
        let temporary = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(temporary.path().join("market-squawk"))?;
        let at = Timestamp::from_unix_nanos(1);
        let jobs = InstalledJobAuthority::open(&paths, Vec::new(), at).await?;
        let repository = jobs.repository();

        jobs.shutdown_authority(at, Duration::from_secs(1)).await?;
        let page = repository
            .recover_nonterminal(None, RecoveryPageLimit::try_new(1)?)
            .await?;
        assert!(page.snapshots().is_empty());

        jobs.shutdown_repository().await?;
        Ok(())
    }
}
