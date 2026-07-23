//! Bounded handoff from post-decision live exports to fair-value producer selection.

use std::collections::HashMap;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU8, Ordering},
};
use std::time::{Duration, Instant};

use market_squawk_domain::{InstrumentId, VenueId};
use market_squawk_live::{
    LiveRouteConfig, QualifiedMarketObservationLease, QualifiedMarketObservationReceiver,
    QualifiedMarketPrice, RouteQualifiedMarketExport, ShardKey,
};
use market_squawk_valuation::MarketPriceSelection;
use thiserror::Error;
use tokio::sync::{Mutex, MutexGuard};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const WRITE_TIMEOUT: Duration = Duration::from_secs(1);
const EXPORT_CHANNEL_CAPACITY: usize = 4;
const RETAINED_OBSERVATION_FAMILIES_PER_ROUTE: usize = 2;
const NO_DRAIN_FAILURE: u8 = 0;
const DRAIN_ROUTE_MISMATCH: u8 = 1;
const DRAIN_BUFFER_FAILURE: u8 = 2;
const DRAIN_CLOSED_EARLY: u8 = 3;
const DRAIN_TASK_FAILURE: u8 = 4;

/// Latest non-forgeable trade and quote lease retained for each exact live route.
///
/// Every stored value continues holding the byte permit minted by its route export channel, so
/// this buffer cannot escape the live runtime's admitted retained-byte budget. Count admission is
/// independently bounded by the configured route ceiling. A selected lease is removed and moved
/// into the fair-value receipt authority; it is never cloned or reconstructed.
pub struct LiveFairValueObservationBuffer {
    maximum_entries: usize,
    state: Mutex<HashMap<ObservationKey, QualifiedMarketObservationLease>>,
}

impl LiveFairValueObservationBuffer {
    /// Preallocates a two-observation ceiling for every admitted route.
    pub fn try_new(
        maximum_routes: NonZeroUsize,
    ) -> Result<Self, LiveFairValueObservationBufferError> {
        let maximum_entries = maximum_routes
            .get()
            .checked_mul(2)
            .ok_or(LiveFairValueObservationBufferError::InvalidCapacity)?;
        let mut observations = HashMap::new();
        observations
            .try_reserve(maximum_entries)
            .map_err(|_| LiveFairValueObservationBufferError::Allocation)?;
        Ok(Self {
            maximum_entries,
            state: Mutex::new(observations),
        })
    }

    /// Replaces only the latest observation of the same route and price family.
    ///
    /// The fixed lock deadline makes a blocked consumer fail closed. Dropping the returned error
    /// closes that route's receiver in the owning drain task, which causes the live runtime to
    /// report export loss instead of silently discarding execution-qualified observations.
    pub async fn replace(
        &self,
        lease: QualifiedMarketObservationLease,
        cancellation: &CancellationToken,
    ) -> Result<(), LiveFairValueObservationBufferError> {
        let key = ObservationKey::from_lease(&lease);
        let deadline = tokio::time::Instant::now()
            .checked_add(WRITE_TIMEOUT)
            .ok_or(LiveFairValueObservationBufferError::DeadlineExceeded)?;
        let mut state = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                return Err(LiveFairValueObservationBufferError::Cancelled);
            }
            () = tokio::time::sleep_until(deadline) => {
                return Err(LiveFairValueObservationBufferError::DeadlineExceeded);
            }
            state = self.state.lock() => state,
        };
        if let Some(existing) = state.get(&key)
            && existing.observation().committed_state_revision()
                >= lease.observation().committed_state_revision()
        {
            return Err(LiveFairValueObservationBufferError::NonMonotonicObservation);
        }
        if !state.contains_key(&key) && state.len() >= self.maximum_entries {
            return Err(LiveFairValueObservationBufferError::ResourceExhausted);
        }
        let _replaced = state.insert(key, lease);
        Ok(())
    }

    async fn clear(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), LiveFairValueObservationBufferError> {
        let mut state = lock_before(&self.state, deadline, cancellation).await?;
        state.clear();
        Ok(())
    }

    /// Moves the latest exact route observation into one caller-selected market-price receipt.
    pub async fn take(
        &self,
        venue: VenueId,
        instrument: InstrumentId,
        selection: MarketPriceSelection,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<QualifiedMarketObservationLease, LiveFairValueObservationBufferError> {
        if cancellation.is_cancelled() {
            return Err(LiveFairValueObservationBufferError::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(LiveFairValueObservationBufferError::DeadlineExceeded);
        }
        let key = ObservationKey {
            route: ShardKey::new(venue, instrument),
            kind: ObservationKind::from_selection(selection),
        };
        let deadline = tokio::time::Instant::from_std(deadline);
        let mut state = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                return Err(LiveFairValueObservationBufferError::Cancelled);
            }
            () = tokio::time::sleep_until(deadline) => {
                return Err(LiveFairValueObservationBufferError::DeadlineExceeded);
            }
            state = self.state.lock() => state,
        };
        let observation = state
            .get(&key)
            .ok_or(LiveFairValueObservationBufferError::NotFound)?
            .observation();
        if !selection_available(observation.price(), selection) {
            return Err(LiveFairValueObservationBufferError::NotFound);
        }
        state
            .remove(&key)
            .ok_or(LiveFairValueObservationBufferError::NotFound)
    }
}

/// Sole owner of the bounded post-action export consumers for one paper runtime.
///
/// A drain failure cancels the same run token supplied to the production source. Dropping this
/// owner aborts any remaining consumer tasks, so a failed application shutdown cannot detach a
/// receiver while the live sender continues operating.
pub(super) struct LiveFairValueExportDrains {
    buffer: Arc<LiveFairValueObservationBuffer>,
    expected_close: Arc<AtomicBool>,
    failure: Arc<AtomicU8>,
    cancellation: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
}

impl LiveFairValueExportDrains {
    /// Creates exactly one bounded export channel and owned drain for every configured route.
    ///
    /// The retained-byte budget covers the complete channel plus the two latest trade/quote
    /// leases that may remain in the shared buffer. Existing observations are cleared before a new
    /// runtime incarnation can publish.
    pub(super) async fn try_start(
        routes: &[LiveRouteConfig],
        maximum_message_bytes: NonZeroU32,
        buffer: Arc<LiveFairValueObservationBuffer>,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<(Vec<RouteQualifiedMarketExport>, Self), LiveFairValueObservationBufferError> {
        if routes.is_empty()
            || routes.len()
                > buffer
                    .maximum_entries
                    .checked_div(RETAINED_OBSERVATION_FAMILIES_PER_ROUTE)
                    .ok_or(LiveFairValueObservationBufferError::InvalidCapacity)?
        {
            return Err(LiveFairValueObservationBufferError::InvalidCapacity);
        }
        let retained_leases = EXPORT_CHANNEL_CAPACITY
            .checked_add(RETAINED_OBSERVATION_FAMILIES_PER_ROUTE)
            .ok_or(LiveFairValueObservationBufferError::InvalidCapacity)?;
        let maximum_message_bytes = usize::try_from(maximum_message_bytes.get())
            .map_err(|_| LiveFairValueObservationBufferError::InvalidCapacity)?;
        let maximum_retained_bytes = maximum_message_bytes
            .checked_mul(retained_leases)
            .ok_or(LiveFairValueObservationBufferError::InvalidCapacity)?;

        let mut exports = Vec::new();
        exports
            .try_reserve_exact(routes.len())
            .map_err(|_| LiveFairValueObservationBufferError::Allocation)?;
        let mut receivers = Vec::new();
        receivers
            .try_reserve_exact(routes.len())
            .map_err(|_| LiveFairValueObservationBufferError::Allocation)?;
        for route in routes {
            let (export, receiver) = RouteQualifiedMarketExport::try_new(
                route.route().clone(),
                EXPORT_CHANNEL_CAPACITY,
                maximum_retained_bytes,
            )
            .map_err(|_| LiveFairValueObservationBufferError::InvalidExportConfiguration)?;
            exports.push(export);
            receivers.push((route.route().clone(), receiver));
        }

        buffer.clear(deadline, &cancellation).await?;
        let expected_close = Arc::new(AtomicBool::new(false));
        let failure = Arc::new(AtomicU8::new(NO_DRAIN_FAILURE));
        let mut tasks = Vec::new();
        tasks
            .try_reserve_exact(receivers.len())
            .map_err(|_| LiveFairValueObservationBufferError::Allocation)?;
        for (route, receiver) in receivers {
            tasks.push(tokio::spawn(drain_route(
                route,
                receiver,
                Arc::clone(&buffer),
                Arc::clone(&expected_close),
                Arc::clone(&failure),
                cancellation.clone(),
            )));
        }
        Ok((
            exports,
            Self {
                buffer,
                expected_close,
                failure,
                cancellation,
                tasks,
            },
        ))
    }

    /// Reports whether every consumer remains available and invariant-preserving.
    pub(super) fn is_healthy(&self) -> bool {
        self.failure.load(Ordering::Acquire) == NO_DRAIN_FAILURE
            && self.tasks.iter().all(|task| !task.is_finished())
    }

    /// Marks sender closure as an expected part of ordered source/live shutdown.
    pub(super) fn begin_shutdown(&self) {
        self.expected_close.store(true, Ordering::Release);
    }

    /// Joins every drain and releases every retained live lease before the deadline.
    pub(super) async fn finish_before(
        mut self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), LiveFairValueObservationBufferError> {
        self.begin_shutdown();
        let mut tasks = std::mem::take(&mut self.tasks);
        while let Some(mut task) = tasks.pop() {
            let outcome = tokio::select! {
                biased;
                () = cancellation.cancelled() => {
                    task.abort();
                    let _aborted = task.await;
                    abort_tasks(&mut tasks).await;
                    return Err(LiveFairValueObservationBufferError::Cancelled);
                }
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                    task.abort();
                    let _aborted = task.await;
                    abort_tasks(&mut tasks).await;
                    return Err(LiveFairValueObservationBufferError::DeadlineExceeded);
                }
                outcome = &mut task => outcome,
            };
            if outcome.is_err() {
                record_failure(&self.failure, DRAIN_TASK_FAILURE, &self.cancellation);
            }
        }
        self.buffer.clear(deadline, cancellation).await?;
        if self.failure.load(Ordering::Acquire) == NO_DRAIN_FAILURE {
            Ok(())
        } else {
            Err(LiveFairValueObservationBufferError::DrainFailed)
        }
    }
}

impl std::fmt::Debug for LiveFairValueExportDrains {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LiveFairValueExportDrains")
            .field("task_count", &self.tasks.len())
            .field("healthy", &self.is_healthy())
            .finish()
    }
}

impl Drop for LiveFairValueExportDrains {
    fn drop(&mut self) {
        self.cancellation.cancel();
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl std::fmt::Debug for LiveFairValueObservationBuffer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LiveFairValueObservationBuffer")
            .field("maximum_entries", &self.maximum_entries)
            .field("observations", &"[NON-FORGEABLE LIVE LEASES]")
            .finish()
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ObservationKey {
    route: ShardKey,
    kind: ObservationKind,
}

impl ObservationKey {
    fn from_lease(lease: &QualifiedMarketObservationLease) -> Self {
        let observation = lease.observation();
        Self {
            route: ShardKey::new(observation.venue_id().clone(), observation.instrument_id()),
            kind: ObservationKind::from_price(observation.price()),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum ObservationKind {
    Trade,
    Quote,
}

impl ObservationKind {
    const fn from_price(price: QualifiedMarketPrice) -> Self {
        match price {
            QualifiedMarketPrice::Trade { .. } => Self::Trade,
            QualifiedMarketPrice::Quote { .. } => Self::Quote,
        }
    }

    const fn from_selection(selection: MarketPriceSelection) -> Self {
        match selection {
            MarketPriceSelection::Trade => Self::Trade,
            MarketPriceSelection::Bid | MarketPriceSelection::Ask => Self::Quote,
        }
    }
}

fn selection_available(price: QualifiedMarketPrice, selection: MarketPriceSelection) -> bool {
    matches!(
        (price, selection),
        (
            QualifiedMarketPrice::Trade { .. },
            MarketPriceSelection::Trade
        ) | (
            QualifiedMarketPrice::Quote { bid: Some(_), .. },
            MarketPriceSelection::Bid
        ) | (
            QualifiedMarketPrice::Quote { ask: Some(_), .. },
            MarketPriceSelection::Ask
        )
    )
}

async fn drain_route(
    route: ShardKey,
    mut receiver: QualifiedMarketObservationReceiver,
    buffer: Arc<LiveFairValueObservationBuffer>,
    expected_close: Arc<AtomicBool>,
    failure: Arc<AtomicU8>,
    cancellation: CancellationToken,
) {
    loop {
        let lease = tokio::select! {
            biased;
            () = cancellation.cancelled() => return,
            lease = receiver.recv() => lease,
        };
        let Some(lease) = lease else {
            if !expected_close.load(Ordering::Acquire) {
                record_failure(&failure, DRAIN_CLOSED_EARLY, &cancellation);
            }
            return;
        };
        let observation = lease.observation();
        if observation.venue_id() != route.venue()
            || observation.instrument_id() != route.instrument()
        {
            record_failure(&failure, DRAIN_ROUTE_MISMATCH, &cancellation);
            return;
        }
        if buffer.replace(lease, &cancellation).await.is_err() {
            if !cancellation.is_cancelled() {
                record_failure(&failure, DRAIN_BUFFER_FAILURE, &cancellation);
            }
            return;
        }
    }
}

fn record_failure(failure: &AtomicU8, code: u8, cancellation: &CancellationToken) {
    let _unchanged =
        failure.compare_exchange(NO_DRAIN_FAILURE, code, Ordering::AcqRel, Ordering::Acquire);
    cancellation.cancel();
}

async fn abort_tasks(tasks: &mut Vec<JoinHandle<()>>) {
    for task in tasks.iter() {
        task.abort();
    }
    while let Some(task) = tasks.pop() {
        let _aborted = task.await;
    }
}

async fn lock_before<'state>(
    state: &'state Mutex<HashMap<ObservationKey, QualifiedMarketObservationLease>>,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<
    MutexGuard<'state, HashMap<ObservationKey, QualifiedMarketObservationLease>>,
    LiveFairValueObservationBufferError,
> {
    if cancellation.is_cancelled() {
        return Err(LiveFairValueObservationBufferError::Cancelled);
    }
    if Instant::now() >= deadline {
        return Err(LiveFairValueObservationBufferError::DeadlineExceeded);
    }
    tokio::select! {
        biased;
        () = cancellation.cancelled() => {
            Err(LiveFairValueObservationBufferError::Cancelled)
        }
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            Err(LiveFairValueObservationBufferError::DeadlineExceeded)
        }
        state = state.lock() => Ok(state),
    }
}

/// Live export buffering or selection failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LiveFairValueObservationBufferError {
    /// The configured route count overflowed its two-family bound.
    #[error("live fair-value observation buffer capacity is invalid")]
    InvalidCapacity,
    /// Fixed buffer allocation failed before publication.
    #[error("live fair-value observation buffer allocation failed")]
    Allocation,
    /// A route export could not satisfy its fixed count-and-byte bounds.
    #[error("live fair-value route export configuration is invalid")]
    InvalidExportConfiguration,
    /// A route emitted a duplicate or older committed state revision.
    #[error("live fair-value observation revision is not monotonic")]
    NonMonotonicObservation,
    /// The bounded route/family count was exhausted.
    #[error("live fair-value observation buffer is full")]
    ResourceExhausted,
    /// No compatible current route observation is retained.
    #[error("live fair-value observation was not found")]
    NotFound,
    /// Buffer ownership was cancelled.
    #[error("live fair-value observation operation was cancelled")]
    Cancelled,
    /// Buffer lock ownership exceeded the admitted deadline.
    #[error("live fair-value observation operation deadline elapsed")]
    DeadlineExceeded,
    /// One owned export receiver, invariant, or task failed before ordered shutdown completed.
    #[error("live fair-value export drain failed")]
    DrainFailed,
}
