//! Immutable reserve-before-run experiment inventory and statistical diagnostics.

mod diagnostics;
mod inventory;
mod model;
mod wire;

pub use diagnostics::{
    BacktestOverfittingDiagnostic, BacktestOverfittingFold, BacktestOverfittingInput,
    BacktestOverfittingScore, DeflatedPerformanceDiagnostic, DeflatedPerformanceInput,
};
pub use inventory::{ExperimentInventory, TrialReservation};
pub use model::{
    BacktestArtifact, ExperimentLimits, ExperimentLimitsInput, TrialCompletion,
    TrialCompletionInput, TrialComponentBinding, TrialFailure, TrialId, TrialMetric,
    TrialParameter, TrialRecord, TrialSearchDimension, TrialSpec, TrialSpecInput, TrialStatus,
};

use thiserror::Error;

/// Experiment specification, durable inventory, artifact, or diagnostic failure.
#[derive(Debug, Error)]
pub enum ExperimentError {
    #[error("experiment specification is invalid")]
    InvalidSpec,
    #[error("experiment limits are invalid")]
    InvalidLimits,
    #[error("experiment completion is invalid")]
    InvalidCompletion,
    #[error("experiment diagnostic input is invalid")]
    InvalidDiagnostic,
    #[error("experiment resource limit exceeded")]
    LimitExceeded,
    #[error("trial identity already exists and cannot be overwritten")]
    TrialAlreadyExists,
    #[error("experiment record is corrupt or inconsistent")]
    CorruptRecord,
    #[error("experiment record encoding failed")]
    Encoding,
    #[error("experiment inventory is unavailable")]
    Unavailable,
    #[error("experiment inventory I/O failed: {0}")]
    Io(#[from] std::io::Error),
}
