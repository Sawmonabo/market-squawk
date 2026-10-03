//! Retained Alpaca raw-custody and canonical-publication worker lifecycle.
use crate::application::{AlpacaMarketPublicationError, AlpacaPublicationRuntimeInput};
use crate::live_source::AlpacaCapturedPublicationReceiver;
use market_squawk_services::ServiceError;
use std::{
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub(crate) struct AlpacaPublicationRuntime {
    input: Arc<AlpacaPublicationRuntimeInput>,
    cancellation: CancellationToken,
    worker: Option<tokio::task::JoinHandle<AlpacaPublicationWorkerOutcome>>,
    run_failure: Option<AlpacaPublicationRuntimeError>,
    local_deadline_failure: Arc<AtomicBool>,
    result: Option<Result<(), ServiceError>>,
}
impl AlpacaPublicationRuntime {
    pub(crate) fn start(
        input: AlpacaPublicationRuntimeInput,
        mut receiver: AlpacaCapturedPublicationReceiver,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> Self {
        let input = Arc::new(input);
        let owned = Arc::clone(&input);
        let stop = cancellation.clone();
        let local_deadline_failure = Arc::new(AtomicBool::new(false));
        let worker_deadline_failure = Arc::clone(&local_deadline_failure);
        let worker = tokio::spawn(async move {
            let _cancel_on_exit = stop.clone().drop_guard();
            let mut outcome = AlpacaPublicationWorkerOutcome {
                failure: None,
                cleanup_failed: false,
                local_deadline_failure: worker_deadline_failure,
            };
            loop {
                let next = if stop.is_cancelled() {
                    owned.begin_shutdown();
                    receiver.close_admission();
                    // The source still owns one reserved slot and may transfer its pending
                    // producer allocation for raw-only sealing before dropping its sender.
                    receiver.recv().await
                } else {
                    tokio::select! { biased;
                        () = stop.cancelled() => {
                            owned.begin_shutdown();
                            receiver.close_admission();
                            receiver.recv().await
                        }
                        item = receiver.recv() => item,
                    }
                };
                let Some(item) = next else {
                    break;
                };
                if item._bytes.is_cancelled_producer() {
                    owned.begin_shutdown();
                    stop.cancel();
                    receiver.close_admission();
                }
                debug_assert!(item._bytes.retained_bytes() > 0);
                let deadline = Instant::now()
                    .checked_add(timeout)
                    .ok_or(AlpacaPublicationRuntimeError::Bounds);
                let result = match deadline {
                    Ok(deadline) => owned
                        .publish(item.rejoin, item.seal_request, item.observed_at, deadline)
                        .await
                        .map_err(AlpacaPublicationRuntimeError::Publication),
                    Err(error) => Err(error),
                };
                // item._bytes remains owned until publication completes, including error paths.
                drop(item._bytes);
                drop(item._frame);
                if let Err(error) = result {
                    let source_admitted =
                        !stop.is_cancelled() && owned.admits_publication_deadline_recovery();
                    outcome.record_failure(error, source_admitted);
                    owned.begin_shutdown();
                    stop.cancel();
                    receiver.close_admission();
                }
            }
            owned.begin_shutdown();
            owned.finish_shutdown().await;
            stop.cancel();
            outcome
        });
        Self {
            input,
            cancellation,
            worker: Some(worker),
            run_failure: None,
            local_deadline_failure,
            result: None,
        }
    }
    pub(crate) fn has_local_deadline_failure(&self) -> bool {
        self.local_deadline_failure.load(Ordering::Acquire)
    }
    pub(crate) fn is_healthy(&self) -> bool {
        self.run_failure.is_none()
            && !self.cancellation.is_cancelled()
            && self
                .worker
                .as_ref()
                .is_some_and(|worker| !worker.is_finished())
    }
    pub(crate) fn begin_shutdown(&self) {
        self.input.begin_shutdown();
        self.cancellation.cancel();
    }
    pub(crate) async fn finish_shutdown_before(
        &mut self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        self.begin_shutdown();
        tokio::select! { biased;
            ()=cancellation.cancelled()=>Err(ServiceError::Cancelled),
            ()=tokio::time::sleep_until(deadline.into())=>Err(ServiceError::DeadlineExceeded),
            result=self.finish_retained_shutdown()=>result,
        }
    }
    pub(crate) async fn finish_retained_shutdown(&mut self) -> Result<(), ServiceError> {
        self.begin_shutdown();
        if let Some(result) = &self.result {
            return result.clone();
        }
        let result = match self.worker.as_mut() {
            Some(worker) => {
                let outcome = joined_publication_outcome(worker.await);
                let cleanup = if outcome.cleanup_failed {
                    Err(ServiceError::Unavailable)
                } else {
                    Ok(())
                };
                self.run_failure = outcome.failure;
                cleanup
            }
            None => Err(ServiceError::Unavailable),
        };
        self.worker.take();
        self.result = Some(result.clone());
        result
    }
}

#[derive(Debug, Default)]
struct AlpacaPublicationWorkerOutcome {
    failure: Option<AlpacaPublicationRuntimeError>,
    cleanup_failed: bool,
    local_deadline_failure: Arc<AtomicBool>,
}

impl AlpacaPublicationWorkerOutcome {
    fn record_failure(&mut self, error: AlpacaPublicationRuntimeError, source_admitted: bool) {
        log_publication_failure(&error);
        // Every other publication error occurs after the original capture was sealed. The
        // worker still drains queued captures and revokes publication admission before joining.
        self.cleanup_failed |= matches!(
            &error,
            AlpacaPublicationRuntimeError::Bounds
                | AlpacaPublicationRuntimeError::Join(_)
                | AlpacaPublicationRuntimeError::Publication(
                    AlpacaMarketPublicationError::Custody(_)
                )
        );
        if self.failure.is_none() {
            self.local_deadline_failure.store(
                source_admitted
                    && matches!(
                        &error,
                        AlpacaPublicationRuntimeError::Publication(
                            AlpacaMarketPublicationError::LocalPublicationDeadline
                                | AlpacaMarketPublicationError::Ingest(
                                    market_squawk_data::IngestError::DeadlineExceeded
                                )
                                | AlpacaMarketPublicationError::Service(
                                    ServiceError::DeadlineExceeded
                                )
                                | AlpacaMarketPublicationError::Research(
                                    crate::ResearchServiceError::Ingest(
                                        market_squawk_data::IngestError::DeadlineExceeded
                                    )
                                )
                        )
                    ),
                Ordering::Release,
            );
            self.failure = Some(error);
        }
        if self.cleanup_failed {
            self.local_deadline_failure.store(false, Ordering::Release);
        }
    }
}

fn joined_publication_outcome(
    joined: Result<AlpacaPublicationWorkerOutcome, tokio::task::JoinError>,
) -> AlpacaPublicationWorkerOutcome {
    match joined {
        Ok(outcome) => outcome,
        Err(error) => {
            let mut outcome = AlpacaPublicationWorkerOutcome::default();
            outcome.record_failure(AlpacaPublicationRuntimeError::Join(error), false);
            outcome
        }
    }
}

fn log_publication_failure(error: &AlpacaPublicationRuntimeError) {
    tracing::error!(%error, "Alpaca publication worker failed");
    let mut cause: &(dyn Error + 'static) = error;
    for _ in 0..8 {
        if let Some(manifest) = cause.downcast_ref::<market_squawk_data::ManifestCatalogError>() {
            use market_squawk_data::ManifestCatalogError;
            match manifest {
                ManifestCatalogError::Sqlite(rusqlite::Error::SqliteFailure(code, _)) => {
                    tracing::error!(
                        manifest_error = %manifest,
                        sqlite_code = code.extended_code & 0xff,
                        sqlite_extended_code = code.extended_code,
                        "Alpaca publication manifest failure"
                    );
                }
                ManifestCatalogError::Plan(plan) => {
                    tracing::error!(manifest_error = %manifest, plan_error = %plan,
                        "Alpaca publication manifest failure");
                }
                ManifestCatalogError::PopulationResearchUse(_) => {
                    tracing::error!(
                        manifest_error = "population research use unavailable",
                        "Alpaca publication manifest failure"
                    );
                }
                _ => tracing::error!(manifest_error = %manifest,
                    "Alpaca publication manifest failure"),
            }
            break;
        }
        let Some(source) = cause.source() else {
            break;
        };
        cause = source;
    }
}
impl Drop for AlpacaPublicationRuntime {
    fn drop(&mut self) {
        self.begin_shutdown();
    }
}
#[derive(Debug, thiserror::Error)]
pub(crate) enum AlpacaPublicationRuntimeError {
    #[error("Alpaca publication bounds are invalid")]
    Bounds,
    #[error("Alpaca publication worker did not join successfully")]
    Join(#[source] tokio::task::JoinError),
    #[error(transparent)]
    Publication(#[from] AlpacaMarketPublicationError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn joined_publication_failure_preserves_custody_cleanup_authority() {
        for error in [
            AlpacaMarketPublicationError::LocalPublicationDeadline,
            AlpacaMarketPublicationError::Ingest(market_squawk_data::IngestError::DeadlineExceeded),
            AlpacaMarketPublicationError::Service(ServiceError::DeadlineExceeded),
            AlpacaMarketPublicationError::Research(crate::ResearchServiceError::Ingest(
                market_squawk_data::IngestError::DeadlineExceeded,
            )),
        ] {
            let mut deadline = AlpacaPublicationWorkerOutcome::default();
            let notification = Arc::clone(&deadline.local_deadline_failure);
            deadline.record_failure(AlpacaPublicationRuntimeError::Publication(error), true);
            // Classification is available while original queued captures still await sealing.
            assert!(notification.load(Ordering::Acquire));
            assert!(!deadline.cleanup_failed);
            deadline.record_failure(
                AlpacaPublicationRuntimeError::Publication(AlpacaMarketPublicationError::Custody(
                    crate::ResearchServiceError::ProviderCaptureSealWorkerUnavailable,
                )),
                false,
            );
            assert!(!notification.load(Ordering::Acquire));
            assert!(joined_publication_outcome(Ok(deadline)).cleanup_failed);
        }
        let mut stopped = AlpacaPublicationWorkerOutcome::default();
        stopped.record_failure(
            AlpacaPublicationRuntimeError::Publication(
                AlpacaMarketPublicationError::LocalPublicationDeadline,
            ),
            false,
        );
        assert!(!stopped.local_deadline_failure.load(Ordering::Acquire));
        let mut unclassified = AlpacaPublicationWorkerOutcome::default();
        unclassified.record_failure(
            AlpacaPublicationRuntimeError::Publication(AlpacaMarketPublicationError::Ingest(
                market_squawk_data::IngestError::Cancelled,
            )),
            true,
        );
        assert!(!unclassified.local_deadline_failure.load(Ordering::Acquire));

        let mut outcome = AlpacaPublicationWorkerOutcome::default();
        outcome.record_failure(
            AlpacaPublicationRuntimeError::Publication(AlpacaMarketPublicationError::Ingest(
                market_squawk_data::IngestError::Manifest(
                    market_squawk_data::ManifestCatalogError::AnchorMismatch,
                ),
            )),
            true,
        );
        assert!(!outcome.local_deadline_failure.load(Ordering::Acquire));
        // A later deadline cannot reclassify the original integrity failure as recoverable.
        outcome.record_failure(
            AlpacaPublicationRuntimeError::Publication(
                AlpacaMarketPublicationError::LocalPublicationDeadline,
            ),
            true,
        );
        assert!(!outcome.local_deadline_failure.load(Ordering::Acquire));
        let mut joined = joined_publication_outcome(Ok(outcome));
        assert!(!joined.cleanup_failed);
        assert!(matches!(
            joined.failure,
            Some(AlpacaPublicationRuntimeError::Publication(
                AlpacaMarketPublicationError::Ingest(_)
            ))
        ));

        // A later queued item's custody failure must still block cleanup even when the
        // first retained error describes a canonical publication failure.
        joined.record_failure(
            AlpacaPublicationRuntimeError::Publication(AlpacaMarketPublicationError::Custody(
                crate::ResearchServiceError::ProviderCaptureSealWorkerUnavailable,
            )),
            false,
        );
        assert!(joined.cleanup_failed);
        assert!(matches!(
            joined.failure,
            Some(AlpacaPublicationRuntimeError::Publication(
                AlpacaMarketPublicationError::Ingest(_)
            ))
        ));

        let mut unsealed = AlpacaPublicationWorkerOutcome::default();
        unsealed.record_failure(AlpacaPublicationRuntimeError::Bounds, false);
        assert!(joined_publication_outcome(Ok(unsealed)).cleanup_failed);

        let worker = tokio::spawn(std::future::pending::<AlpacaPublicationWorkerOutcome>());
        worker.abort();
        let interrupted = joined_publication_outcome(worker.await);
        assert!(interrupted.cleanup_failed);
        assert!(matches!(
            interrupted.failure,
            Some(AlpacaPublicationRuntimeError::Join(_))
        ));
    }
}
