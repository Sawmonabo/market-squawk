//! Retained preparation and shutdown of the sole paper runtime.

use std::{
    future::{Future, poll_fn},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use market_squawk_domain::SourceIdentifier;
use market_squawk_live::{ActiveLiveActionHookGroup, LiveActionHookGeneration, LiveSnapshotReader};
use market_squawk_services::ServiceError;
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::super::market_runtime::MarketRuntimeRegistry;
use crate::paper_bot::{
    ProductionPaperBotComposition, ProductionPaperBotRuntime, ProductionPaperBotShutdown,
    ProductionPaperBotStartError,
};

type Preparation = Pin<Box<dyn Future<Output = PreparationOutcome> + Send>>;
type Drain = Pin<Box<dyn Future<Output = StopOutcome> + Send>>;

pub(super) struct RetainedPaperRun {
    surface: SourceIdentifier,
    progress: Mutex<Progress>,
    cancellation: CancellationToken,
    market: Arc<MarketRuntimeRegistry>,
    cleanup_timeout: Duration,
}

enum Progress {
    Preparing(Preparation),
    Ready(ReadyRun),
    Draining(Drain),
    Stopped(StopOutcome),
    Released,
}

struct ReadyRun {
    runtime: ProductionPaperBotRuntime,
    hooks: Option<ActiveLiveActionHookGroup>,
    cleanup: Option<HookCleanup>,
}

#[derive(Clone)]
struct HookCleanup {
    surface: SourceIdentifier,
    incarnation: std::num::NonZeroU64,
    generation: LiveActionHookGeneration,
}

struct StopOutcome {
    startup_error: Option<ServiceError>,
    preparation_error: Option<ProductionPaperBotStartError>,
    shutdown: Option<ProductionPaperBotShutdown>,
    cleanup: Option<HookCleanup>,
    cleanup_error: Option<ServiceError>,
}

impl StopOutcome {
    fn is_complete(&self) -> bool {
        self.cleanup_error.is_none()
            && self
                .shutdown
                .as_ref()
                .is_none_or(ProductionPaperBotShutdown::is_complete)
            && !matches!(
                self.preparation_error.as_ref(),
                Some(
                    ProductionPaperBotStartError::Rollback { .. }
                        | ProductionPaperBotStartError::VirtualRouteRollback { .. }
                )
            )
    }
}

impl RetainedPaperRun {
    #[allow(
        clippy::too_many_arguments,
        reason = "the exact existing market and preparation authority remain explicit"
    )]
    pub(super) fn preparing(
        composition: ProductionPaperBotComposition,
        snapshots: Option<LiveSnapshotReader>,
        market: Arc<MarketRuntimeRegistry>,
        surface: SourceIdentifier,
        session: Option<Uuid>,
        deadline: Instant,
        cancellation: CancellationToken,
        cleanup_timeout: Duration,
        equity: super::EquityPaperServices,
        definitions: market_squawk_data::MarketDataInstrumentReadCapability,
        context: market_squawk_services::RequestContext,
    ) -> Arc<Self> {
        let retained_surface = surface.clone();
        let preparation = prepare(
            composition,
            snapshots,
            Arc::clone(&market),
            surface,
            session,
            deadline,
            cancellation.clone(),
            cleanup_timeout,
            equity,
            definitions,
            context,
        );
        Arc::new(Self {
            surface: retained_surface,
            progress: Mutex::new(Progress::Preparing(Box::pin(preparation))),
            cancellation,
            market,
            cleanup_timeout,
        })
    }

    pub(super) fn running(
        runtime: ProductionPaperBotRuntime,
        hooks: Option<ActiveLiveActionHookGroup>,
        surface: SourceIdentifier,
        cancellation: CancellationToken,
        market: Arc<MarketRuntimeRegistry>,
        cleanup_timeout: Duration,
    ) -> Arc<Self> {
        let cleanup = hooks.map(|hooks| {
            let disabled = hooks.disable();
            HookCleanup {
                surface: surface.clone(),
                incarnation: disabled.runtime_incarnation(),
                generation: disabled.generation(),
            }
        });
        cancellation.cancel();
        Arc::new(Self {
            surface,
            progress: Mutex::new(Progress::Draining(Box::pin(stop_runtime(
                runtime,
                cleanup,
                None,
                Arc::clone(&market),
                cleanup_timeout,
            )))),
            cancellation,
            market,
            cleanup_timeout,
        })
    }

    pub(super) fn requires_reconciliation(&self) -> bool {
        matches!(&*self.progress.lock(), Progress::Stopped(outcome) if !outcome.is_complete())
    }

    pub(super) fn surface_id(&self) -> &SourceIdentifier {
        &self.surface
    }

    pub(super) fn begin_shutdown(&self) {
        self.cancellation.cancel();
        let mut progress = self.progress.lock();
        begin_ready_shutdown(&mut progress, &self.market, self.cleanup_timeout);
    }

    pub(super) async fn wait_ready(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        wait_before(
            deadline,
            cancellation,
            poll_fn(|cx| {
                let mut progress = self.progress.lock();
                poll_preparation(&mut progress, cx);
                match &*progress {
                    Progress::Preparing(_) => Poll::Pending,
                    Progress::Ready(_) => Poll::Ready(Ok(())),
                    Progress::Stopped(outcome) => Poll::Ready(Err(outcome
                        .startup_error
                        .unwrap_or(ServiceError::Unavailable))),
                    Progress::Draining(_) | Progress::Released => {
                        Poll::Ready(Err(ServiceError::Unavailable))
                    }
                }
            }),
        )
        .await
    }

    /// Called only while the original controller state/owner gates still prove this start.
    pub(super) fn take_ready(
        &self,
    ) -> Option<(ProductionPaperBotRuntime, Option<ActiveLiveActionHookGroup>)> {
        let mut progress = self.progress.lock();
        if self.cancellation.is_cancelled()
            || !matches!(&*progress, Progress::Ready(ready) if ready.runtime.source_is_healthy())
        {
            return None;
        }
        let Progress::Ready(ready) = std::mem::replace(&mut *progress, Progress::Released) else {
            return None;
        };
        Some((ready.runtime, ready.hooks))
    }

    pub(super) async fn finish_shutdown(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<bool, ServiceError> {
        self.begin_shutdown();
        {
            let mut progress = self.progress.lock();
            let previous = std::mem::replace(&mut *progress, Progress::Released);
            *progress = match previous {
                Progress::Stopped(outcome)
                    if outcome.cleanup_error.is_some()
                        || outcome
                            .shutdown
                            .as_ref()
                            .is_some_and(|shutdown| shutdown.audit().can_resume()) =>
                {
                    // Resume only retained audit ownership and the exact outstanding hook reap.
                    // Every financial result remains unchanged; no checkpoint is run again.
                    Progress::Draining(Box::pin(resume_stop(
                        outcome,
                        Arc::clone(&self.market),
                        self.cleanup_timeout,
                    )))
                }
                other => other,
            };
        }
        wait_before(
            deadline,
            cancellation,
            poll_fn(|cx| {
                let mut progress = self.progress.lock();
                poll_preparation(&mut progress, cx);
                begin_ready_shutdown(&mut progress, &self.market, self.cleanup_timeout);
                if let Progress::Draining(future) = &mut *progress {
                    if let Poll::Ready(outcome) = future.as_mut().poll(cx) {
                        // Commit the actual result before another await can drop the waiter.
                        *progress = Progress::Stopped(outcome);
                    }
                }
                match &*progress {
                    Progress::Stopped(outcome) => Poll::Ready(Ok(outcome.is_complete())),
                    Progress::Released => Poll::Ready(Err(ServiceError::Unavailable)),
                    Progress::Preparing(_) | Progress::Ready(_) | Progress::Draining(_) => {
                        Poll::Pending
                    }
                }
            }),
        )
        .await
    }
}

impl Drop for RetainedPaperRun {
    fn drop(&mut self) {
        if matches!(self.progress.get_mut(), Progress::Released) {
            // Successful admission transferred this same cancellation token with the runtime.
            return;
        }
        self.begin_shutdown();
        // Final owner destruction is cancellation, not a successful financial shutdown claim.
        // Existing execution/source Drop owners reap their workers; the audit owner keeps and
        // joins its actual thread. Normal timeout/retry retains this complete continuation.
    }
}

pub(super) struct StartCancellation {
    retained: Arc<RetainedPaperRun>,
    armed: bool,
}

impl StartCancellation {
    pub(super) fn arm(retained: Arc<RetainedPaperRun>) -> Self {
        Self {
            retained,
            armed: true,
        }
    }
    pub(super) fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for StartCancellation {
    fn drop(&mut self) {
        if self.armed {
            self.retained.begin_shutdown();
        }
    }
}

enum PreparationOutcome {
    Ready(ReadyRun),
    Failed(StopOutcome),
}

fn begin_ready_shutdown(
    progress: &mut Progress,
    market: &Arc<MarketRuntimeRegistry>,
    cleanup_timeout: Duration,
) {
    let previous = std::mem::replace(progress, Progress::Released);
    *progress = match previous {
        Progress::Ready(ready) => {
            if let Some(hooks) = ready.hooks {
                drop(hooks.disable());
            }
            Progress::Draining(Box::pin(stop_runtime(
                ready.runtime,
                ready.cleanup,
                None,
                Arc::clone(market),
                cleanup_timeout,
            )))
        }
        other => other,
    };
}

fn poll_preparation(progress: &mut Progress, cx: &mut Context<'_>) {
    if let Progress::Preparing(future) = progress {
        if let Poll::Ready(outcome) = future.as_mut().poll(cx) {
            *progress = match outcome {
                PreparationOutcome::Ready(ready) => Progress::Ready(ready),
                PreparationOutcome::Failed(outcome) => Progress::Stopped(outcome),
            };
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "one retained preparation owns the exact existing runtime and hooks"
)]
async fn prepare(
    composition: ProductionPaperBotComposition,
    snapshots: Option<LiveSnapshotReader>,
    market: Arc<MarketRuntimeRegistry>,
    surface: SourceIdentifier,
    session: Option<Uuid>,
    deadline: Instant,
    cancellation: CancellationToken,
    cleanup_timeout: Duration,
    equity: super::EquityPaperServices,
    definitions: market_squawk_data::MarketDataInstrumentReadCapability,
    context: market_squawk_services::RequestContext,
) -> PreparationOutcome {
    let Some(snapshots) = snapshots else {
        return prepare_virtual(
            composition,
            market,
            equity,
            definitions,
            deadline,
            cancellation,
            cleanup_timeout,
            context,
        )
        .await;
    };
    let prepared = match composition
        .prepare_on_existing_live(snapshots, cancellation.clone())
        .await
    {
        Ok(prepared) => prepared,
        Err(error) => {
            tracing::error!(%error, "paper execution graph failed to start");
            return PreparationOutcome::Failed(StopOutcome {
                startup_error: Some(ServiceError::Unavailable),
                preparation_error: Some(error),
                shutdown: None,
                cleanup: None,
                cleanup_error: None,
            });
        }
    };
    let (runtime, hooks) = prepared.into_parts();
    if let Err(error) = super::ensure_before(deadline, &cancellation) {
        cancellation.cancel();
        drop(hooks);
        return PreparationOutcome::Failed(
            stop_runtime(runtime, None, Some(error), market, cleanup_timeout).await,
        );
    }
    let prepared = match market
        .prepare_action_hooks(&surface, session, hooks, deadline, &cancellation)
        .await
    {
        Ok(prepared) => prepared,
        Err(error) => {
            cancellation.cancel();
            return PreparationOutcome::Failed(
                stop_runtime(runtime, None, Some(error), market, cleanup_timeout).await,
            );
        }
    };
    let cleanup = HookCleanup {
        surface,
        incarnation: prepared.runtime_incarnation(),
        generation: prepared.generation(),
    };
    if let Err(error) = super::ensure_before(deadline, &cancellation) {
        drop(prepared);
        cancellation.cancel();
        return PreparationOutcome::Failed(
            stop_runtime(runtime, Some(cleanup), Some(error), market, cleanup_timeout).await,
        );
    }
    match prepared.activate() {
        Ok(hooks) => PreparationOutcome::Ready(ReadyRun {
            runtime,
            hooks: Some(hooks),
            cleanup: Some(cleanup),
        }),
        Err(error) => {
            cancellation.cancel();
            PreparationOutcome::Failed(
                stop_runtime(runtime, Some(cleanup), Some(error), market, cleanup_timeout).await,
            )
        }
    }
}

async fn prepare_virtual(
    composition: ProductionPaperBotComposition,
    market: Arc<MarketRuntimeRegistry>,
    equity: super::EquityPaperServices,
    definitions: market_squawk_data::MarketDataInstrumentReadCapability,
    deadline: Instant,
    cancellation: CancellationToken,
    cleanup_timeout: Duration,
    original_context: market_squawk_services::RequestContext,
) -> PreparationOutcome {
    let runtime = match composition.start_virtual_equity(cancellation.clone()).await {
        Ok(runtime) => runtime,
        Err(error) => {
            return PreparationOutcome::Failed(StopOutcome {
                startup_error: Some(ServiceError::Unavailable),
                preparation_error: Some(error),
                shutdown: None,
                cleanup: None,
                cleanup_error: None,
            });
        }
    };
    let context = market_squawk_services::RequestContext::new(
        original_context.request_id().clone(),
        cancellation.clone(),
        deadline,
        original_context.limits(),
    );
    let context = match original_context.origin() {
        Some(origin) => context.with_origin(origin),
        None => context,
    };
    let result = runtime
        .refresh_virtual_equity(&market, &equity.actions, &definitions, &context)
        .await;
    match result {
        Ok(()) => PreparationOutcome::Ready(ReadyRun {
            runtime,
            hooks: None,
            cleanup: None,
        }),
        Err(error) => {
            cancellation.cancel();
            PreparationOutcome::Failed(
                stop_runtime(runtime, None, Some(error), market, cleanup_timeout).await,
            )
        }
    }
}

async fn stop_runtime(
    runtime: ProductionPaperBotRuntime,
    cleanup: Option<HookCleanup>,
    startup_error: Option<ServiceError>,
    market: Arc<MarketRuntimeRegistry>,
    cleanup_timeout: Duration,
) -> StopOutcome {
    let shutdown = runtime.shutdown().await;
    reap(
        StopOutcome {
            startup_error,
            preparation_error: None,
            shutdown: Some(shutdown),
            cleanup,
            cleanup_error: None,
        },
        market,
        cleanup_timeout,
    )
    .await
}

async fn resume_stop(
    mut outcome: StopOutcome,
    market: Arc<MarketRuntimeRegistry>,
    cleanup_timeout: Duration,
) -> StopOutcome {
    if let Some(shutdown) = &mut outcome.shutdown {
        if shutdown.audit().can_resume() {
            if let Some(deadline) = Instant::now().checked_add(cleanup_timeout) {
                shutdown.resume_audit(deadline.into()).await;
            }
        }
    }
    reap(outcome, market, cleanup_timeout).await
}

async fn reap(
    mut outcome: StopOutcome,
    market: Arc<MarketRuntimeRegistry>,
    cleanup_timeout: Duration,
) -> StopOutcome {
    if let Some(cleanup) = &outcome.cleanup {
        outcome.cleanup_error = match Instant::now().checked_add(cleanup_timeout) {
            Some(deadline) => market
                .reap_action_hooks(
                    &cleanup.surface,
                    cleanup.incarnation,
                    cleanup.generation,
                    deadline,
                    &CancellationToken::new(),
                )
                .await
                .err(),
            None => Some(ServiceError::Unavailable),
        };
        if outcome.cleanup_error.is_none() {
            outcome.cleanup = None;
        }
    }
    outcome
}

async fn wait_before<T>(
    deadline: Instant,
    cancellation: &CancellationToken,
    future: impl Future<Output = Result<T, ServiceError>>,
) -> Result<T, ServiceError> {
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(ServiceError::Cancelled),
        () = tokio::time::sleep_until(deadline.into()) => Err(ServiceError::DeadlineExceeded),
        result = future => result,
    }
}
