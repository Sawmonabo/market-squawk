//! Short request admission for Alpaca transports sharing one account budget.

use std::time::{Duration, Instant};

use market_squawk_sources::{
    BudgetDispatchDecision, BudgetPermit, BudgetReservation, BudgetReservationDecision,
    BudgetUnavailableReason, MonotonicInstant, SharedProviderBudget, SourceError,
};
use tokio_util::sync::CancellationToken;

const CONCURRENCY_RECHECK: Duration = Duration::from_millis(25);

#[derive(Debug)]
pub(crate) enum AdmissionError {
    Cancelled,
    DeadlineExceeded,
    WaitUntil(MonotonicInstant),
    Unavailable(BudgetUnavailableReason),
}

impl AdmissionError {
    pub(crate) fn into_source_error(self) -> SourceError {
        match self {
            Self::Cancelled => SourceError::Cancelled,
            Self::DeadlineExceeded => SourceError::Network,
            Self::WaitUntil(deadline) => SourceError::BudgetWaitUntil { deadline },
            Self::Unavailable(reason) => SourceError::BudgetUnavailable { reason },
        }
    }
}

pub(crate) async fn reserve_request(
    budget: &SharedProviderBudget,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<BudgetReservation, AdmissionError> {
    loop {
        ensure_before(deadline, cancellation)?;
        match budget.try_reserve_request() {
            BudgetReservationDecision::Ready(reservation) => return Ok(reservation),
            BudgetReservationDecision::WaitUntil(wait_until) => {
                return Err(AdmissionError::WaitUntil(wait_until));
            }
            BudgetReservationDecision::Unavailable(
                BudgetUnavailableReason::ConcurrencyExhausted,
            ) => wait_for_concurrency(deadline, cancellation).await?,
            BudgetReservationDecision::Unavailable(reason) => {
                return Err(AdmissionError::Unavailable(reason));
            }
        }
    }
}

pub(crate) async fn commit_request(
    mut reservation: BudgetReservation,
    budget: &SharedProviderBudget,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<BudgetPermit, AdmissionError> {
    loop {
        ensure_before(deadline, cancellation)?;
        match reservation.commit_dispatch() {
            BudgetDispatchDecision::Ready(permit) => return Ok(permit),
            BudgetDispatchDecision::WaitUntil(wait_until) => {
                return Err(AdmissionError::WaitUntil(wait_until));
            }
            BudgetDispatchDecision::Unavailable(BudgetUnavailableReason::ConcurrencyExhausted) => {
                // Dispatch consumed the reservation. Never retain one while waiting for the
                // other short request (bootstrap, upgrade, or historical page) to finish.
                wait_for_concurrency(deadline, cancellation).await?;
                reservation = reserve_request(budget, deadline, cancellation).await?;
            }
            BudgetDispatchDecision::Unavailable(reason) => {
                return Err(AdmissionError::Unavailable(reason));
            }
        }
    }
}

fn ensure_before(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), AdmissionError> {
    if cancellation.is_cancelled() {
        return Err(AdmissionError::Cancelled);
    }
    if Instant::now() >= deadline {
        return Err(AdmissionError::DeadlineExceeded);
    }
    Ok(())
}

async fn wait_for_concurrency(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), AdmissionError> {
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(AdmissionError::Cancelled),
        () = tokio::time::sleep_until(deadline.into()) => Err(AdmissionError::DeadlineExceeded),
        () = tokio::time::sleep(CONCURRENCY_RECHECK) => Ok(()),
    }
}
