//! Finite generation-bound history work retained by the existing product startup task owner.

use super::ProductionSourceLifecycleAuthority;
use crate::application::AlpacaHistoricalRuntimeCapability;
use crate::application::{map_market_definition_read_error, map_source_research_error};
use market_squawk_services::{
    JsonStructureLimits, RequestContext, RequestId, ServiceError, ServiceLimits,
};
use std::{sync::Arc, time::Instant};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

/// Only the latest exact generation is retained; notifications never accumulate a work queue.
#[derive(Clone)]
pub(crate) struct StarterHistoryRequest {
    runtime: AlpacaHistoricalRuntimeCapability,
}

pub(crate) type StarterHistorySender = watch::Sender<Option<StarterHistoryRequest>>;
pub(crate) type StarterHistoryReceiver = watch::Receiver<Option<StarterHistoryRequest>>;

impl ProductionSourceLifecycleAuthority {
    pub(crate) fn starter_history_channel() -> (StarterHistorySender, StarterHistoryReceiver) {
        watch::channel(None)
    }

    /// Transfers finite preparation to product lifetime ownership after source activation.
    pub(super) fn admit_display_history(
        &self,
        runtime: AlpacaHistoricalRuntimeCapability,
        deadline: Instant,
    ) -> Result<(), ServiceError> {
        if Instant::now() >= deadline {
            return Err(ServiceError::DeadlineExceeded);
        }
        if runtime.is_revoked() {
            return Err(ServiceError::Unavailable);
        }
        self.display_history_requests
            .send(Some(StarterHistoryRequest { runtime }))
            .map_err(|_| ServiceError::Unavailable)
    }

    /// This entire future belongs to ProductStartupTasks. Cancellation signals the actual work
    /// and then awaits its cleanup; neither a replacement nor shutdown drops that work early.
    pub(crate) async fn run_display_history_worker(
        self: Arc<Self>,
        mut requests: StarterHistoryReceiver,
        shutdown: CancellationToken,
    ) {
        let mut pending = None;
        loop {
            let request = if let Some(request) = pending.take() {
                request
            } else {
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => return,
                    changed = requests.changed() => {
                        if changed.is_err() {
                            return;
                        }
                    }
                }
                let Some(request) = requests.borrow_and_update().clone() else {
                    continue;
                };
                request
            };
            if shutdown.is_cancelled() {
                return;
            }
            if request.runtime.is_revoked() {
                continue;
            }
            // Source activation admitted this work; its request has already completed. Start
            // this owned recovery operation's lifetime here, once, without renewing it on
            // duplicate notifications or while processing instruments.
            let Some(deadline) = Instant::now().checked_add(super::super::LOCAL_RECOVERY_TIMEOUT)
            else {
                record_history_outcome(Err(ServiceError::Internal));
                continue;
            };
            let cancellation = shutdown.child_token();
            let preparation =
                self.prepare_display_history(&request.runtime, deadline, &cancellation);
            tokio::pin!(preparation);
            loop {
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => {
                        cancellation.cancel();
                        record_history_outcome(preparation.await);
                        return;
                    }
                    () = request.runtime.wait_until_revoked() => {
                        cancellation.cancel();
                        record_history_outcome(preparation.await);
                        break;
                    }
                    changed = requests.changed() => {
                        if changed.is_err() {
                            cancellation.cancel();
                            record_history_outcome(preparation.await);
                            return;
                        }
                        let next = requests.borrow_and_update().clone();
                        if next.as_ref().is_some_and(|next| {
                            next.runtime.group_generation() == request.runtime.group_generation()
                        }) {
                            // A duplicate cannot extend this admitted operation's deadline.
                            continue;
                        }
                        cancellation.cancel();
                        record_history_outcome(preparation.await);
                        // Replacement may itself have been superseded while cleanup drained.
                        pending = requests.borrow_and_update().clone();
                        break;
                    }
                    result = &mut preparation => {
                        record_history_outcome(result);
                        break;
                    }
                }
            }
        }
    }

    async fn prepare_display_history(
        &self,
        runtime: &AlpacaHistoricalRuntimeCapability,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        runtime
            .require_current(deadline, cancellation)
            .await
            .map_err(|_| ServiceError::Unavailable)?;
        let instruments = self
            .live
            .equity_paper_instrument_ids(deadline, cancellation)
            .await
            .map_err(|error| {
                tracing::warn!(
                    ?error,
                    stage = "active-display-instruments",
                    "market display history preparation unavailable"
                );
                error
            })?;
        let reader = self.research.market_data_instruments();
        let records = self
            .research
            .run_owned_research_io(deadline, cancellation, move |operation_cancellation| {
                instruments
                    .into_iter()
                    .map(|instrument| {
                        reader
                            .latest(instrument, deadline, &operation_cancellation)
                            .map_err(map_market_definition_read_error)?
                            .ok_or(ServiceError::Unavailable)
                    })
                    .collect::<Result<Vec<_>, ServiceError>>()
            })
            .await
            .map_err(map_source_research_error)
            .and_then(std::convert::identity)
            .map_err(|error| {
                tracing::warn!(
                    ?error,
                    stage = "display-instrument-definitions",
                    "market display history preparation unavailable"
                );
                error
            })?;
        // These bounds govern the one publication receipt, not retained market history. The
        // provider coordinator retains its ordinary streaming, rate and acquisition policies.
        let structure = JsonStructureLimits::try_new(32, 1024 * 1024, 10_000, 1_000)
            .map_err(|_| ServiceError::Internal)?;
        let limits = ServiceLimits::try_new(256 * 1024, 1_000, 1024 * 1024, 1_000, structure)
            .map_err(|_| ServiceError::Internal)?;
        let context = RequestContext::new(
            RequestId::String(Arc::from("source.starter-market-history")),
            cancellation.child_token(),
            deadline,
            limits,
        );
        self.display_history
            .prepare_market_display_histories(runtime, &records, &context)
            .await
    }
}

fn record_history_outcome(result: Result<(), ServiceError>) {
    if let Err(error) = result
        && error != ServiceError::Cancelled
    {
        tracing::warn!(?error, "starter market history preparation is unavailable");
    }
}
