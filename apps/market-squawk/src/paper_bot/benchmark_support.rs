mod observer;
mod source;

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, bail};
use market_squawk_adapter_paper::PaperOrderState;
use market_squawk_execution::Strategy;
use market_squawk_live::{LiveRouteConfig, LiveRuntimeConfig, LiveSnapshotReader, RouteActionHook};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use super::{PaperBotStartMode, ProductionPaperBotComposition, ProductionPaperBotRuntime};
use crate::{AppConfig, LiveRuntimeComposition, ProductionLiveSourceRuntimeError};
pub(crate) use observer::ReleaseMeasuredOutcomeLedger;
pub(super) use observer::{ObservedExecutionHook, ReleaseBenchmarkObserver};
use observer::{ObservedStrategy, PHASE_DISPATCH, PHASE_IDLE, PHASE_MEASURE};

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReleaseLatencyDistribution {
    pub(crate) operations: u64,
    pub(crate) elapsed_nanos: u64,
    pub(crate) operations_per_second: u64,
    pub(crate) p50_nanos: u64,
    pub(crate) p95_nanos: u64,
    pub(crate) p99_nanos: u64,
    pub(crate) maximum_nanos: u64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReleasePaperBotBenchmarkResult {
    pub(crate) event_count: u64,
    pub(crate) measured_outcomes: ReleaseMeasuredOutcomeLedger,
    pub(crate) strategy_decision: ReleaseLatencyDistribution,
    pub(crate) complete_action_disposition: ReleaseLatencyDistribution,
    pub(crate) dispatch_strategy_decision_nanos: u64,
    pub(crate) dispatch_action_disposition_nanos: u64,
    pub(crate) event_to_observed_paper_terminal_nanos: u64,
    pub(crate) dispatch_disposition: String,
    pub(crate) paper_terminal_state: String,
    pub(crate) paper_order_count: usize,
    pub(crate) paper_fill_count: usize,
    pub(crate) mailbox_capacity: usize,
    pub(crate) producer_observed_maximum_in_flight_batches: usize,
    pub(crate) observer_retained_bytes: usize,
    pub(crate) shutdown_complete: bool,
}

#[derive(Debug)]
pub(crate) struct ReleasePaperBotBenchmarkComposition {
    inner: ProductionPaperBotComposition,
    observer: Arc<ReleaseBenchmarkObserver>,
    mailbox_capacity: usize,
}

impl ReleasePaperBotBenchmarkComposition {
    pub(crate) fn try_new(config: AppConfig) -> Result<Self> {
        let observer = Arc::new(ReleaseBenchmarkObserver::try_new()?);
        let strategy_observer = Arc::clone(&observer);
        let hook_overhead = ObservedExecutionHook::retained_overhead_bytes()?;
        let inner = super::defaults::release_benchmark_paper_bot(
            config,
            source::instrument_definition()?,
            hook_overhead,
            move |route| {
                let strategy = super::defaults::controlled_paper_strategy(route)?;
                Ok(Box::new(ObservedStrategy::new(
                    strategy,
                    Arc::clone(&strategy_observer),
                )) as Box<dyn Strategy>)
            },
        )?;
        let mailbox_capacity = inner.runtime_config.mailbox_count_per_shard().get();
        Ok(Self {
            inner,
            observer,
            mailbox_capacity,
        })
    }

    pub(crate) async fn start(
        self,
        cancellation: CancellationToken,
    ) -> Result<ReleasePaperBotBenchmarkRuntime> {
        let started = self
            .inner
            .start_inner(
                PaperBotStartMode::ReleaseBenchmark(Arc::clone(&self.observer)),
                cancellation.clone(),
            )
            .await?;
        let producer = started
            .benchmark_producer
            .context("release benchmark producer ownership is missing")?;
        Ok(ReleasePaperBotBenchmarkRuntime {
            inner: Some(started.runtime),
            producer,
            observer: self.observer,
            mailbox_capacity: self.mailbox_capacity,
            measured_events: None,
            measurement_elapsed_nanos: None,
            cancellation,
        })
    }
}

#[derive(Debug)]
pub(crate) struct ReleasePaperBotBenchmarkRuntime {
    inner: Option<ProductionPaperBotRuntime>,
    producer: ReleaseBenchmarkProducer,
    observer: Arc<ReleaseBenchmarkObserver>,
    mailbox_capacity: usize,
    measured_events: Option<u64>,
    measurement_elapsed_nanos: Option<u64>,
    cancellation: CancellationToken,
}

impl ReleasePaperBotBenchmarkRuntime {
    pub(crate) async fn warm_up(&mut self, events: u64) -> Result<()> {
        self.observer.set_phase(PHASE_IDLE);
        let observer = Arc::clone(&self.observer);
        self.producer
            .publish_trades(events, observer.as_ref())
            .await
    }

    pub(crate) async fn measure(&mut self, events: u64) -> Result<()> {
        self.observer.reset_measurement();
        self.observer.set_phase(PHASE_MEASURE);
        let started = Instant::now();
        let observer = Arc::clone(&self.observer);
        self.producer
            .publish_trades(events, observer.as_ref())
            .await?;
        self.measurement_elapsed_nanos = Some(elapsed_nanos(started));
        self.measured_events = Some(events);
        Ok(())
    }

    pub(crate) async fn finish(mut self) -> Result<ReleasePaperBotBenchmarkResult> {
        let terminal = self.observe_dispatch_and_terminal().await;
        let runtime = self
            .inner
            .take()
            .context("release benchmark runtime ownership is missing")?;
        drop(self.producer);
        let shutdown = runtime.shutdown().await;
        if !shutdown.is_complete() {
            bail!("production paper-bot benchmark shutdown was incomplete: {shutdown:?}");
        }
        let terminal = terminal?;
        let events = self
            .measured_events
            .context("release benchmark measurement did not run")?;
        let elapsed = self
            .measurement_elapsed_nanos
            .context("release benchmark elapsed time is unavailable")?;
        let measured_outcomes = self.observer.measurement_outcomes(events)?;
        if !measured_outcomes.is_exact_non_signal_success() {
            bail!("release benchmark measured outcome ledger did not reconcile exactly");
        }
        let strategy_decision = self.observer.strategy_distribution(elapsed)?;
        let complete_action_disposition = self.observer.action_distribution(elapsed)?;
        if strategy_decision.operations != events
            || complete_action_disposition.operations != events
        {
            bail!("release benchmark observer did not reconcile the measured event count");
        }
        Ok(ReleasePaperBotBenchmarkResult {
            event_count: events,
            measured_outcomes,
            strategy_decision,
            complete_action_disposition,
            dispatch_strategy_decision_nanos: self.observer.dispatch_strategy_nanos(),
            dispatch_action_disposition_nanos: self.observer.dispatch_action_nanos(),
            event_to_observed_paper_terminal_nanos: terminal.elapsed_nanos,
            dispatch_disposition: self.observer.dispatch_disposition().to_owned(),
            paper_terminal_state: terminal.state.to_owned(),
            paper_order_count: terminal.order_count,
            paper_fill_count: terminal.fill_count,
            mailbox_capacity: self.mailbox_capacity,
            producer_observed_maximum_in_flight_batches: 1,
            observer_retained_bytes: self.observer.retained_bytes()?,
            shutdown_complete: true,
        })
    }

    async fn observe_dispatch_and_terminal(&mut self) -> Result<TerminalPaperEvidence> {
        self.observer.set_phase(PHASE_DISPATCH);
        let started = Instant::now();
        let observer = Arc::clone(&self.observer);
        self.producer
            .publish_dispatch_delta(observer.as_ref())
            .await?;
        if self.observer.dispatch_disposition() != "dispatched" {
            bail!(
                "production risk/dispatcher path returned {}",
                self.observer.dispatch_disposition()
            );
        }
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(2))
            .context("paper terminal deadline overflowed")?;
        loop {
            let runtime = self
                .inner
                .as_ref()
                .context("release benchmark runtime ownership is missing")?;
            let snapshot = runtime
                .paper_snapshot(deadline, &self.cancellation)
                .await
                .context("production paper snapshot failed")?;
            if let Some(order) = snapshot.orders().first()
                && is_terminal(order.state())
            {
                return Ok(TerminalPaperEvidence {
                    elapsed_nanos: elapsed_nanos(started),
                    state: paper_state(order.state()),
                    order_count: snapshot.orders().len(),
                    fill_count: snapshot.fills().len(),
                });
            }
            if Instant::now() >= deadline {
                bail!("production paper order did not reach a terminal state");
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}

#[derive(Debug)]
struct TerminalPaperEvidence {
    elapsed_nanos: u64,
    state: &'static str,
    order_count: usize,
    fill_count: usize,
}

#[derive(Debug)]
pub(super) struct ReleaseBenchmarkLiveRuntime {
    live: LiveRuntimeComposition,
}

impl ReleaseBenchmarkLiveRuntime {
    pub(super) async fn start(
        config: LiveRuntimeConfig,
        routes: Vec<LiveRouteConfig>,
        action_hooks: Vec<RouteActionHook>,
        observer: Arc<ReleaseBenchmarkObserver>,
        cancellation: CancellationToken,
    ) -> Result<(Self, ReleaseBenchmarkProducer), ProductionLiveSourceRuntimeError> {
        if routes.len() != 1 {
            return Err(release_source_error(
                "release benchmark requires exactly one live route",
            ));
        }
        let route = routes[0].route().clone();
        let live = LiveRuntimeComposition::start_with_action_hooks(config, routes, action_hooks)
            .await
            .map_err(ProductionLiveSourceRuntimeError::LiveRuntime)?;
        let mut source =
            match source::ReleaseBenchmarkSource::start(&live, route, cancellation.child_token())
                .await
            {
                Ok(source) => source,
                Err(error) => {
                    let _rollback = live.shutdown().await;
                    return Err(release_source_error(error));
                }
            };
        if let Err(error) = source.initialize(&observer).await {
            drop(source);
            let _rollback = live.shutdown().await;
            return Err(release_source_error(error));
        }
        Ok((Self { live }, ReleaseBenchmarkProducer { source }))
    }

    pub(super) fn snapshots(&self) -> LiveSnapshotReader {
        self.live.snapshots()
    }

    pub(super) async fn shutdown(self) -> Result<(), ProductionLiveSourceRuntimeError> {
        self.live
            .shutdown()
            .await
            .map(|_outcome| ())
            .map_err(ProductionLiveSourceRuntimeError::LiveRuntime)
    }
}

#[derive(Debug)]
pub(super) struct ReleaseBenchmarkProducer {
    source: source::ReleaseBenchmarkSource,
}

impl ReleaseBenchmarkProducer {
    async fn publish_trades(
        &mut self,
        events: u64,
        observer: &ReleaseBenchmarkObserver,
    ) -> Result<()> {
        self.source.publish_trades(events, observer).await
    }

    async fn publish_dispatch_delta(&mut self, observer: &ReleaseBenchmarkObserver) -> Result<()> {
        self.source.publish_dispatch_delta(observer).await
    }
}

fn release_source_error(error: impl std::fmt::Display) -> ProductionLiveSourceRuntimeError {
    ProductionLiveSourceRuntimeError::ReleaseBenchmark(error.to_string())
}

fn elapsed_nanos(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn is_terminal(state: PaperOrderState) -> bool {
    matches!(
        state,
        PaperOrderState::Filled
            | PaperOrderState::Canceled
            | PaperOrderState::Rejected
            | PaperOrderState::Expired
    )
}

fn paper_state(state: PaperOrderState) -> &'static str {
    match state {
        PaperOrderState::New => "new",
        PaperOrderState::Accepted => "accepted",
        PaperOrderState::PartiallyFilled => "partially_filled",
        PaperOrderState::Filled => "filled",
        PaperOrderState::CancelPending => "cancel_pending",
        PaperOrderState::Canceled => "canceled",
        PaperOrderState::Rejected => "rejected",
        PaperOrderState::Expired => "expired",
    }
}
