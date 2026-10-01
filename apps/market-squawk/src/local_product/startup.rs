//! Retained ownership of source startup and finite market-history preparation.

use std::{
    future::{Future, poll_fn},
    pin::Pin,
    sync::{Arc, OnceLock, Weak},
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

use market_squawk_domain::SourceIdentifier;
use market_squawk_services::ServiceError;

use super::source_lifecycle::ProductionSourceLifecycleAuthority;
use crate::application::source::{
    SourceLifecycleCommand, SourceLifecycleCommandInput, SourceLifecycleError,
    SourceLifecycleReceipt,
};
use parking_lot::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::application::ResearchApplicationServices;

pub(super) type StartupFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Retains outer publication futures until their own cancellation and blocking replay drains finish.
pub(crate) struct ProductStartupTasks {
    research: Arc<ResearchApplicationServices>,
    cancellation: CancellationToken,
    state: Mutex<State>,
    drain: tokio::sync::Mutex<()>,
    public_lifecycle: OnceLock<Weak<ProductionSourceLifecycleAuthority>>,
}

#[derive(Default)]
struct State {
    handles: Vec<JoinHandle<()>>,
    task_failed: bool,
    source_restore_started: bool,
    public: [PublicTaskSlot; 2],
}

const PUBLIC_SURFACES: [&str; 2] = [
    "coinbase.public-market-data",
    "kraken.spot-public-market-data",
];

#[derive(Default)]
struct PublicTaskSlot {
    handle: Option<JoinHandle<()>>,
    cancellation: Option<CancellationToken>,
    exclusions: usize,
}

/// Prevents readmission until the caller has committed Stop/Remove or another exact mutation.
pub(super) struct PublicPreparationExclusion {
    owner: Arc<ProductStartupTasks>,
    selected: [bool; 2],
}

impl Drop for PublicPreparationExclusion {
    fn drop(&mut self) {
        let mut state = self.owner.state.lock();
        for (index, selected) in self.selected.iter().enumerate() {
            if *selected {
                state.public[index].exclusions -= 1;
            }
        }
    }
}

impl ProductStartupTasks {
    /// Called only after every fallible local constructor has succeeded.
    pub(super) fn start(
        research: Arc<ResearchApplicationServices>,
        cancellation: CancellationToken,
        futures: [Option<StartupFuture>; 5],
    ) -> Arc<Self> {
        let owner = Arc::new(Self {
            research,
            cancellation,
            state: Mutex::new(State::default()),
            drain: tokio::sync::Mutex::new(()),
            public_lifecycle: OnceLock::new(),
        });
        let mut state = owner.state.lock();
        for future in futures.into_iter().flatten() {
            // External product/application Drop can cancel, but cannot destroy this owner while
            // the source is still joining a real blocking replay worker. Never abort this task.
            let retained = Arc::clone(&owner);
            state.handles.push(tokio::spawn(async move {
                future.await;
                drop(retained);
            }));
        }
        drop(state);
        owner
    }

    pub(super) fn bind_public_lifecycle(
        &self,
        lifecycle: Weak<ProductionSourceLifecycleAuthority>,
    ) -> Result<(), SourceLifecycleError> {
        self.public_lifecycle
            .set(lifecycle)
            .map_err(|_| SourceLifecycleError::Conflict)
    }

    /// Restores saved connections without holding service readiness behind provider I/O.
    pub(super) fn admit_source_restoration(
        self: &Arc<Self>,
        lifecycle: Arc<ProductionSourceLifecycleAuthority>,
        operation_timeout: Duration,
    ) -> Result<(), SourceLifecycleError> {
        let mut state = self.state.lock();
        if self.cancellation.is_cancelled() {
            return Err(SourceLifecycleError::Cancelled);
        }
        if state.source_restore_started {
            return Ok(());
        }
        state.source_restore_started = true;
        let cancellation = self.cancellation.child_token();
        let retained = Arc::clone(self);
        state.handles.push(tokio::spawn(async move {
            if let Err(error) = lifecycle
                .restore_ready_research_sources_independently(operation_timeout, &cancellation)
                .await
            {
                tracing::warn!(?error, "saved research connections could not be restored");
            }
            match lifecycle
                .restore_active_live_sources_independently(operation_timeout, &cancellation)
                .await
            {
                Ok(report) => {
                    tracing::info!(
                        restored_source_count = report.restored().len(),
                        "saved live connections restored"
                    );
                    for failure in report.failures() {
                        tracing::warn!(
                            provider = failure.provider().as_str(),
                            error = %failure.error(),
                            "saved live connection could not be restored"
                        );
                    }
                }
                Err(error) => {
                    tracing::warn!(?error, "saved live connections could not be restored")
                }
            }
            drop(retained);
        }));
        Ok(())
    }

    /// The request is only a waiter: its source preparation remains owned until cancellation joins.
    pub(super) async fn execute_public_command(
        self: &Arc<Self>,
        command: &SourceLifecycleCommand,
    ) -> Result<SourceLifecycleReceipt, SourceLifecycleError> {
        let exclusion = self
            .exclude_public_preparation(
                Some(command.provider()),
                command.deadline(),
                command.cancellation(),
            )
            .await
            .map_err(source_error)?;
        let lifecycle = self
            .public_lifecycle
            .get()
            .and_then(Weak::upgrade)
            .ok_or(SourceLifecycleError::Unavailable)?;
        let index = PUBLIC_SURFACES
            .iter()
            .position(|surface| *surface == command.provider().as_str())
            .ok_or(SourceLifecycleError::InvalidRequest)?;
        let cancellation = command.cancellation().child_token();
        let _cancel_on_drop = cancellation.clone().drop_guard();
        let retained_command = SourceLifecycleCommand::try_new(SourceLifecycleCommandInput {
            provider: command.provider().clone(),
            action: command.action(),
            expected_state_revision: command.expected_state_revision(),
            expected_generation: command.expected_generation(),
            expected_runtime_generation_digest: command.expected_runtime_generation_digest(),
            onboarding_session_id: command.onboarding_session_id(),
            public_configuration_digest: command.public_configuration_digest(),
            reason: command.reason().cloned(),
            cancellation: cancellation.clone(),
            deadline: command.deadline(),
        })?;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        {
            let mut state = self.state.lock();
            if self.cancellation.is_cancelled()
                || state.public[index].handle.is_some()
                || state.public[index].exclusions != 1
            {
                return Err(SourceLifecycleError::Unavailable);
            }
            state.public[index].cancellation = Some(cancellation);
            state.public[index].handle = Some(tokio::spawn(async move {
                let result = lifecycle
                    .execute_public_command_owned(&retained_command)
                    .await;
                let _delivered = sender.send(result);
                drop(exclusion);
            }));
        }
        tokio::select! { biased;
            () = command.cancellation().cancelled() => Err(SourceLifecycleError::Cancelled),
            () = tokio::time::sleep_until(command.deadline().into()) => Err(SourceLifecycleError::DeadlineExceeded),
            result = receiver => result.map_err(|_| SourceLifecycleError::Unavailable)?,
        }
    }

    /// Admits at most one finite preparation for each configured public surface.
    pub(super) fn admit_public_sources(
        self: &Arc<Self>,
        lifecycle: Arc<ProductionSourceLifecycleAuthority>,
    ) -> Result<(), ServiceError> {
        if self.cancellation.is_cancelled() {
            return Err(ServiceError::Unavailable);
        }
        let mut state = self.state.lock();
        for (index, surface) in PUBLIC_SURFACES.iter().enumerate() {
            let mut cx = Context::from_waker(Waker::noop());
            if state.public[index]
                .handle
                .as_ref()
                .is_some_and(JoinHandle::is_finished)
            {
                let Some(handle) = state.public[index].handle.as_mut() else {
                    continue;
                };
                if let Poll::Ready(result) = Pin::new(handle).poll(&mut cx) {
                    state.task_failed |= result.is_err();
                    state.public[index].handle.take();
                    state.public[index].cancellation.take();
                }
            }
            let slot = &mut state.public[index];
            if slot.handle.is_some() || slot.exclusions != 0 {
                continue;
            }
            let provider =
                SourceIdentifier::try_from(*surface).map_err(|_| ServiceError::Internal)?;
            let cancellation = self.cancellation.child_token();
            slot.cancellation = Some(cancellation.clone());
            let retained = Arc::clone(self);
            let lifecycle = Arc::clone(&lifecycle);
            slot.handle = Some(tokio::spawn(async move {
                if let Err(error) = lifecycle
                    .prepare_configured_public_source(provider.clone(), cancellation)
                    .await
                {
                    tracing::warn!(
                        provider = provider.as_str(),
                        ?error,
                        "public source preparation ended without activation"
                    );
                }
                drop(retained);
            }));
        }
        Ok(())
    }

    /// Closes admission before the first await and joins cancellation without holding lifecycle gates.
    pub(super) async fn exclude_public_preparation(
        self: &Arc<Self>,
        provider: Option<&SourceIdentifier>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PublicPreparationExclusion, ServiceError> {
        let selected =
            PUBLIC_SURFACES.map(|surface| provider.is_none_or(|value| value.as_str() == surface));
        let exclusion = PublicPreparationExclusion {
            owner: Arc::clone(self),
            selected,
        };
        {
            let mut state = self.state.lock();
            for (index, selected) in selected.iter().enumerate() {
                if *selected {
                    let slot = &mut state.public[index];
                    slot.exclusions += 1;
                    if let Some(token) = &slot.cancellation {
                        token.cancel();
                    }
                }
            }
        }
        let join = async {
            let _drain = self.drain.lock().await;
            let mut failed = false;
            poll_fn(|cx| {
                let mut state = self.state.lock();
                let mut pending = false;
                for (index, selected) in selected.iter().enumerate() {
                    if !selected {
                        continue;
                    }
                    let Some(handle) = state.public[index].handle.as_mut() else {
                        continue;
                    };
                    match Pin::new(handle).poll(cx) {
                        Poll::Ready(result) => {
                            failed |= result.is_err();
                            state.task_failed |= result.is_err();
                            state.public[index].handle.take();
                            state.public[index].cancellation.take();
                        }
                        Poll::Pending => pending = true,
                    }
                }
                if pending {
                    Poll::Pending
                } else {
                    Poll::Ready(if failed {
                        Err(ServiceError::Unavailable)
                    } else {
                        Ok(())
                    })
                }
            })
            .await
        };
        tokio::select! { biased;
            () = cancellation.cancelled() => Err(ServiceError::Cancelled),
            () = tokio::time::sleep_until(deadline.into()) => Err(ServiceError::DeadlineExceeded),
            result = join => { result?; Ok(exclusion) },
        }
    }

    pub(crate) fn begin_shutdown(&self) {
        self.research.begin_startup_shutdown();
        self.cancellation.cancel();
        for slot in &self.state.lock().public {
            if let Some(cancellation) = &slot.cancellation {
                cancellation.cancel();
            }
        }
    }

    pub(crate) fn is_drained(&self) -> bool {
        let state = self.state.lock();
        state.handles.is_empty() && state.public.iter().all(|slot| slot.handle.is_none())
    }

    pub(crate) async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        self.begin_shutdown();
        tokio::time::timeout_at(deadline.into(), async {
            let _drain = self.drain.lock().await;
            poll_fn(|cx| {
                let mut state = self.state.lock();
                for index in 0..state.public.len() {
                    if let Some(handle) = state.public[index].handle.take() {
                        state.handles.push(handle);
                    }
                    state.public[index].cancellation.take();
                }
                let mut index = 0;
                while index < state.handles.len() {
                    match Pin::new(&mut state.handles[index]).poll(cx) {
                        Poll::Ready(result) => {
                            state.task_failed |= result.is_err();
                            drop(state.handles.swap_remove(index));
                        }
                        Poll::Pending => index += 1,
                    }
                }
                if state.handles.is_empty() {
                    Poll::Ready(if state.task_failed {
                        Err(ServiceError::Unavailable)
                    } else {
                        Ok(())
                    })
                } else {
                    Poll::Pending
                }
            })
            .await
        })
        .await
        .map_err(|_| ServiceError::DeadlineExceeded)?
    }
}

impl std::fmt::Debug for ProductStartupTasks {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProductStartupTasks")
            .field("retained_tasks", &self.state.lock().handles.len())
            .finish_non_exhaustive()
    }
}

impl Drop for ProductStartupTasks {
    fn drop(&mut self) {
        self.cancellation.cancel();
        // Every started future retained this owner through its final await. Handles here can only
        // represent completed outer tasks, whose blocking replay was joined by the source owner.
        // Raw-capture recovery workers separately retain data::BlockingIoSupervisor reaper leases.
    }
}

fn source_error(error: ServiceError) -> SourceLifecycleError {
    match error {
        ServiceError::Cancelled => SourceLifecycleError::Cancelled,
        ServiceError::DeadlineExceeded => SourceLifecycleError::DeadlineExceeded,
        _ => SourceLifecycleError::Unavailable,
    }
}

/// Moves bounded cold event batches off the live writer path, with retained shutdown ownership.
pub(super) async fn run_market_event_archive(
    analytical: Arc<market_squawk_data::AnalyticalDataService>,
    cancellation: CancellationToken,
) {
    let mut cursor = None;
    let mut cadence = tokio::time::interval(Duration::from_secs(5));
    cadence.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! { biased;
            () = cancellation.cancelled() => return,
            _ = cadence.tick() => {},
        }
        let result = analytical
            .maintain_market_event_archive(
                cursor.as_ref(),
                market_squawk_data::MarketEventArchiveLimits::default(),
                Instant::now() + Duration::from_secs(30),
                cancellation.child_token(),
            )
            .await;
        if cancellation.is_cancelled() {
            return;
        }
        match result {
            Ok(turn) => cursor = turn.next_dataset().cloned(),
            Err(_) => {
                // Archive failure leaves active rows authoritative and does not stop a source.
                tracing::warn!("market event archive turn could not complete");
                cursor = None;
            }
        }
    }
}
