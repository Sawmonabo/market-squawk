//! Reserve-before-run application service for governed point-in-time experiments.

use market_squawk_data::Sha256Digest;
use market_squawk_domain::SourceIdentifier;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::experiments::{
    BacktestOverfittingDiagnostic, DeflatedPerformanceDiagnostic, ExperimentError,
    ExperimentInventory, TrialCompletionInput, TrialFailure, TrialMetric, TrialRecord, TrialSpec,
};
use crate::{BacktestEngine, BacktestError, BacktestRequest, BacktestRun, BacktestStrategy};

mod artifact;

/// Precomputed bounded evaluation evidence committed with one completed trial.
#[derive(Clone, Debug)]
pub struct BacktestEvaluation {
    metrics: Box<[TrialMetric]>,
    probability_of_backtest_overfitting: BacktestOverfittingDiagnostic,
    deflated_performance: DeflatedPerformanceDiagnostic,
    selected: bool,
}

impl BacktestEvaluation {
    /// Canonicalizes named metrics and rejects duplicate identities before a run is reserved.
    pub fn try_new(
        mut metrics: Vec<TrialMetric>,
        probability_of_backtest_overfitting: BacktestOverfittingDiagnostic,
        deflated_performance: DeflatedPerformanceDiagnostic,
        selected: bool,
    ) -> Result<Self, BacktestServiceError> {
        metrics.sort_unstable_by(|left, right| left.name().cmp(right.name()));
        if metrics
            .windows(2)
            .any(|pair| pair[0].name() == pair[1].name())
        {
            return Err(BacktestServiceError::InvalidEvaluation);
        }
        Ok(Self {
            metrics: metrics.into_boxed_slice(),
            probability_of_backtest_overfitting,
            deflated_performance,
            selected,
        })
    }
}

/// Successful governed run and its immutable terminal inventory record.
#[derive(Clone, Debug)]
pub struct BacktestResult {
    run: BacktestRun,
    trial: TrialRecord,
}

impl BacktestResult {
    /// Returns reconciled fills, portfolio state, and deterministic result identity.
    #[must_use]
    pub const fn run(&self) -> &BacktestRun {
        &self.run
    }

    /// Returns the immutable completed trial record.
    #[must_use]
    pub const fn trial(&self) -> &TrialRecord {
        &self.trial
    }
}

/// Audited engine failure whose immutable terminal was committed before returning.
#[derive(Debug)]
pub struct BacktestFailure {
    error: BacktestError,
    trial: TrialRecord,
}

impl BacktestFailure {
    /// Returns the typed backtest failure.
    #[must_use]
    pub const fn error(&self) -> &BacktestError {
        &self.error
    }

    /// Returns the immutable failed trial record.
    #[must_use]
    pub const fn trial(&self) -> &TrialRecord {
        &self.trial
    }
}

/// Domain outcome after durable reservation and exactly one terminal transition.
#[derive(Debug)]
pub enum BacktestOutcome {
    /// The run reconciled and its bounded artifact and completion were committed.
    Completed(Box<BacktestResult>),
    /// The engine failed closed and an immutable failure terminal was committed.
    Failed(Box<BacktestFailure>),
}

/// Sole orchestrator for reservation, execution, artifact publication, and terminal commit.
#[derive(Debug)]
pub struct BacktestService {
    inventory: ExperimentInventory,
}

impl BacktestService {
    /// Owns one capability-confined experiment inventory.
    #[must_use]
    pub const fn new(inventory: ExperimentInventory) -> Self {
        Self { inventory }
    }

    /// Runs one trial only after validating exact request/spec bindings and durable reservation.
    pub fn run(
        &self,
        spec: TrialSpec,
        request: BacktestRequest,
        strategy: &mut dyn BacktestStrategy,
        evaluation: BacktestEvaluation,
        cancellation: &CancellationToken,
    ) -> Result<BacktestOutcome, BacktestServiceError> {
        if spec.dataset_identity() != request.dataset_identity()
            || spec.object_graph_digest() != request.object_graph_digest()
            || spec.execution_assumption_digest() != request.assumption_digest()
            || spec.seed() != request.seed()
        {
            return Err(BacktestServiceError::BindingMismatch);
        }
        if evaluation.metrics.len() > self.inventory.limits().max_metrics() {
            return Err(BacktestServiceError::InvalidEvaluation);
        }
        let reservation = self.inventory.reserve(spec)?;
        let run = match BacktestEngine::run(&request, strategy, cancellation) {
            Ok(run) => run,
            Err(error) => {
                let trial =
                    self.commit_failure(reservation, "backtest-engine", &error.to_string())?;
                return Ok(BacktestOutcome::Failed(Box::new(BacktestFailure {
                    error,
                    trial,
                })));
            }
        };
        let artifact_bytes =
            match artifact::encode(&request, &run, self.inventory.limits().max_artifact_bytes()) {
                Ok(bytes) => bytes,
                Err(error) => {
                    self.commit_failure(
                        reservation,
                        "backtest-artifact-encoding",
                        "bounded encoding",
                    )?;
                    return Err(error);
                }
            };
        let artifact = match self.inventory.publish_artifact(&artifact_bytes) {
            Ok(artifact) => artifact,
            Err(error) => {
                self.commit_failure(
                    reservation,
                    "backtest-artifact-publication",
                    &error.to_string(),
                )?;
                return Err(error.into());
            }
        };
        let trial = self.inventory.complete(
            reservation,
            TrialCompletionInput {
                result_digest: run.result_digest(),
                artifact,
                metrics: evaluation.metrics.into_vec(),
                probability_of_backtest_overfitting: evaluation.probability_of_backtest_overfitting,
                deflated_performance: evaluation.deflated_performance,
                selected: evaluation.selected,
            },
        )?;
        Ok(BacktestOutcome::Completed(Box::new(BacktestResult {
            run,
            trial,
        })))
    }

    fn commit_failure(
        &self,
        reservation: crate::TrialReservation,
        code: &'static str,
        evidence: &str,
    ) -> Result<TrialRecord, BacktestServiceError> {
        let code =
            SourceIdentifier::try_from(code).map_err(|_| BacktestServiceError::FailureEncoding)?;
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/backtest-failure/v1");
        hash.update((code.as_str().len() as u64).to_be_bytes());
        hash.update(code.as_str().as_bytes());
        hash.update((evidence.len() as u64).to_be_bytes());
        hash.update(evidence.as_bytes());
        let failure = TrialFailure::try_new(code, Sha256Digest::new(hash.finalize().into()))?;
        self.inventory
            .fail(reservation, failure)
            .map_err(Into::into)
    }
}

/// Binding, evaluation, inventory, or bounded artifact failure outside engine domain outcomes.
#[derive(Debug, Error)]
pub enum BacktestServiceError {
    /// The trial specification does not identify the exact request inputs.
    #[error("backtest trial and request bindings differ")]
    BindingMismatch,
    /// Trial evaluation evidence is duplicate or exceeds the inventory ceiling.
    #[error("backtest evaluation evidence is invalid")]
    InvalidEvaluation,
    /// A fixed internal failure identity could not be encoded.
    #[error("backtest failure evidence could not be encoded")]
    FailureEncoding,
    /// The detailed result exceeded its bound or could not be encoded.
    #[error("backtest result artifact encoding failed")]
    ArtifactEncoding,
    /// Immutable inventory or artifact publication failed.
    #[error("backtest experiment inventory failed: {0}")]
    Experiment(#[from] ExperimentError),
}
