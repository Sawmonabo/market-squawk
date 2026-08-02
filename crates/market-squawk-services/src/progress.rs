//! Transport-neutral, bounded progress reporting.

use std::{
    fmt,
    sync::{Arc, Mutex},
    time::Instant,
};

use async_trait::async_trait;
use thiserror::Error;
use tokio::{sync::Notify, time::Instant as TokioInstant};
use tokio_util::sync::CancellationToken;

const MAXIMUM_PROGRESS_UPDATES: usize = 100_000;
const MAXIMUM_PROGRESS_MESSAGE_BYTES: usize = 64 * 1024;
const MAXIMUM_EXACT_PROGRESS_INTEGER: u64 = (1_u64 << 53) - 1;

/// Per-request progress ceilings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProgressLimits {
    maximum_updates: usize,
    maximum_message_bytes: usize,
}

impl ProgressLimits {
    /// Creates positive progress ceilings within implementation safety limits.
    ///
    /// # Errors
    ///
    /// Returns [`ProgressLimitsError`] for zero or unsupported values.
    pub fn try_new(
        maximum_updates: usize,
        maximum_message_bytes: usize,
    ) -> Result<Self, ProgressLimitsError> {
        if maximum_updates == 0 || maximum_message_bytes == 0 {
            return Err(ProgressLimitsError::Zero);
        }
        if maximum_updates > MAXIMUM_PROGRESS_UPDATES
            || maximum_message_bytes > MAXIMUM_PROGRESS_MESSAGE_BYTES
        {
            return Err(ProgressLimitsError::LimitTooLarge);
        }
        Ok(Self {
            maximum_updates,
            maximum_message_bytes,
        })
    }

    /// Maximum accepted reports for one request.
    #[must_use]
    pub const fn maximum_updates(self) -> usize {
        self.maximum_updates
    }

    /// Maximum UTF-8 bytes in an optional progress message.
    #[must_use]
    pub const fn maximum_message_bytes(self) -> usize {
        self.maximum_message_bytes
    }
}

impl Default for ProgressLimits {
    fn default() -> Self {
        Self {
            maximum_updates: 1_024,
            maximum_message_bytes: 1_024,
        }
    }
}

/// Invalid progress-limit configuration.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ProgressLimitsError {
    /// Both ceilings must be positive.
    #[error("progress limits must be nonzero")]
    Zero,
    /// A ceiling exceeded the supported bound.
    #[error("progress limit exceeds the supported maximum")]
    LimitTooLarge,
}

/// One validated transport-neutral progress update.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProgressUpdate {
    completed: u64,
    total: Option<u64>,
    message: Option<Arc<str>>,
}

/// One lifecycle-bound progress delivery accepted from a request reporter.
///
/// Dropping this value acknowledges the accepted delivery, including cancellation and transport
/// teardown paths. Transport sinks must retain it until they publish or suppress the update.
pub struct ProgressDelivery {
    update: ProgressUpdate,
    request_cancellation: CancellationToken,
    lifecycle: Arc<ProgressLifecycle>,
    deadline: Instant,
    _acknowledgement: ProgressAcknowledgement,
}

impl fmt::Debug for ProgressDelivery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProgressDelivery")
            .field("update", &"[PROGRESS UPDATE REDACTED]")
            .field("request_cancellation", &"[CANCELLATION TOKEN]")
            .field("lifecycle", &"[PROGRESS LIFECYCLE]")
            .field("deadline", &self.deadline)
            .field("acknowledgement", &"[PROGRESS ACKNOWLEDGEMENT]")
            .finish()
    }
}

impl ProgressDelivery {
    /// Validated progress values carried by this delivery.
    #[must_use]
    pub const fn update(&self) -> &ProgressUpdate {
        &self.update
    }

    /// Absolute monotonic deadline inherited from the request.
    #[must_use]
    pub const fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Revalidates request authority immediately before publication.
    ///
    /// # Errors
    ///
    /// Returns cancellation or deadline errors after the request or progress lifecycle ends.
    pub fn ensure_live(&self) -> Result<(), ProgressError> {
        if self.request_cancellation.is_cancelled() || self.lifecycle.is_closed() {
            return Err(ProgressError::Cancelled);
        }
        if Instant::now() >= self.deadline {
            return Err(ProgressError::DeadlineExceeded);
        }
        Ok(())
    }

    /// Waits until cancellation, progress closure, or the absolute deadline ends publication
    /// authority.
    pub async fn ended(&self) -> ProgressError {
        tokio::select! {
            biased;
            () = self.request_cancellation.cancelled() => ProgressError::Cancelled,
            () = self.lifecycle.closed.cancelled() => ProgressError::Cancelled,
            () = tokio::time::sleep_until(TokioInstant::from_std(self.deadline)) => {
                ProgressError::DeadlineExceeded
            }
        }
    }
}

impl ProgressUpdate {
    /// Completed work units.
    #[must_use]
    pub const fn completed(&self) -> u64 {
        self.completed
    }

    /// Total work units when known.
    #[must_use]
    pub const fn total(&self) -> Option<u64> {
        self.total
    }

    /// Optional bounded status text.
    #[must_use]
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }
}

/// Transport adapter for progress notifications.
#[async_trait]
pub trait ProgressSink: Send + Sync + 'static {
    /// Delivers one validated, lifecycle-bound progress update.
    ///
    /// # Errors
    ///
    /// Returns [`ProgressError::Delivery`] when the bounded transport cannot accept the update.
    async fn report(&self, delivery: ProgressDelivery) -> Result<(), ProgressError>;
}

#[derive(Debug, Default)]
struct ProgressState {
    accepted_updates: usize,
    last_completed: Option<u64>,
}

#[derive(Debug, Default)]
struct ProgressLifecycleState {
    closed: bool,
    outstanding: usize,
}

#[derive(Debug, Default)]
struct ProgressLifecycle {
    state: Mutex<ProgressLifecycleState>,
    closed: CancellationToken,
    changed: Notify,
}

impl ProgressLifecycle {
    fn is_closed(&self) -> bool {
        self.closed.is_cancelled()
    }

    fn admit(self: &Arc<Self>) -> Result<ProgressAcknowledgement, ProgressError> {
        let mut state = self.state.lock().map_err(|_| ProgressError::State)?;
        if state.closed {
            return Err(ProgressError::Cancelled);
        }
        state.outstanding = state
            .outstanding
            .checked_add(1)
            .ok_or(ProgressError::State)?;
        Ok(ProgressAcknowledgement {
            lifecycle: Arc::clone(self),
        })
    }

    async fn close_and_drain(&self) -> Result<(), ProgressError> {
        {
            let mut state = self.state.lock().map_err(|_| ProgressError::State)?;
            state.closed = true;
            self.closed.cancel();
        }
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self
                .state
                .lock()
                .map_err(|_| ProgressError::State)?
                .outstanding
                == 0
            {
                return Ok(());
            }
            changed.as_mut().await;
        }
    }

    fn acknowledge(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.outstanding = state.outstanding.saturating_sub(1);
        if state.outstanding == 0 {
            self.changed.notify_waiters();
        }
    }
}

struct ProgressAcknowledgement {
    lifecycle: Arc<ProgressLifecycle>,
}

impl fmt::Debug for ProgressAcknowledgement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProgressAcknowledgement")
            .finish_non_exhaustive()
    }
}

impl Drop for ProgressAcknowledgement {
    fn drop(&mut self) {
        self.lifecycle.acknowledge();
    }
}

/// Cloneable reporter enforcing per-request progress invariants before transport dispatch.
#[derive(Clone)]
pub struct ProgressReporter {
    sink: Option<Arc<dyn ProgressSink>>,
    cancellation: CancellationToken,
    deadline: Instant,
    limits: ProgressLimits,
    state: Arc<Mutex<ProgressState>>,
    delivery: Arc<tokio::sync::Mutex<()>>,
    lifecycle: Arc<ProgressLifecycle>,
}

impl ProgressReporter {
    pub(crate) fn enabled(
        sink: Arc<dyn ProgressSink>,
        cancellation: CancellationToken,
        deadline: Instant,
        limits: ProgressLimits,
    ) -> Self {
        Self {
            sink: Some(sink),
            cancellation,
            deadline,
            limits,
            state: Arc::new(Mutex::new(ProgressState::default())),
            delivery: Arc::new(tokio::sync::Mutex::new(())),
            lifecycle: Arc::new(ProgressLifecycle::default()),
        }
    }

    pub(crate) fn disabled(
        cancellation: CancellationToken,
        deadline: Instant,
        limits: ProgressLimits,
    ) -> Self {
        Self {
            sink: None,
            cancellation,
            deadline,
            limits,
            state: Arc::new(Mutex::new(ProgressState::default())),
            delivery: Arc::new(tokio::sync::Mutex::new(())),
            lifecycle: Arc::new(ProgressLifecycle::default()),
        }
    }

    /// True when the caller supplied a transport progress capability.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.sink.is_some()
    }

    /// Validates and sends one monotonic progress update.
    ///
    /// # Errors
    ///
    /// Returns [`ProgressError`] for missing capability, invalid or excessive updates,
    /// cancellation, deadline expiry, poisoned state, or bounded transport failure.
    pub async fn report(
        &self,
        completed: u64,
        total: Option<u64>,
        message: Option<&str>,
    ) -> Result<(), ProgressError> {
        let sink = self.sink.as_ref().ok_or(ProgressError::Unavailable)?;
        self.ensure_live()?;
        if completed > MAXIMUM_EXACT_PROGRESS_INTEGER
            || total
                .is_some_and(|value| value > MAXIMUM_EXACT_PROGRESS_INTEGER || value < completed)
        {
            return Err(ProgressError::InvalidValue);
        }
        if message.is_some_and(|value| value.len() > self.limits.maximum_message_bytes()) {
            return Err(ProgressError::MessageTooLong);
        }
        let _delivery = tokio::select! {
            biased;
            () = self.cancellation.cancelled() => return Err(ProgressError::Cancelled),
            () = tokio::time::sleep_until(TokioInstant::from_std(self.deadline)) => {
                return Err(ProgressError::DeadlineExceeded);
            }
            guard = self.delivery.lock() => guard,
        };
        self.ensure_live()?;
        {
            let mut state = self.state.lock().map_err(|_| ProgressError::State)?;
            if state.accepted_updates >= self.limits.maximum_updates() {
                return Err(ProgressError::TooManyUpdates);
            }
            if state
                .last_completed
                .is_some_and(|previous| completed < previous)
            {
                return Err(ProgressError::NonMonotonic);
            }
            state.accepted_updates += 1;
            state.last_completed = Some(completed);
        }
        let update = ProgressUpdate {
            completed,
            total,
            message: message.map(Arc::from),
        };
        let acknowledgement = self.lifecycle.admit()?;
        let delivery = ProgressDelivery {
            update,
            request_cancellation: self.cancellation.clone(),
            lifecycle: Arc::clone(&self.lifecycle),
            deadline: self.deadline,
            _acknowledgement: acknowledgement,
        };
        tokio::select! {
            biased;
            () = self.cancellation.cancelled() => Err(ProgressError::Cancelled),
            () = tokio::time::sleep_until(TokioInstant::from_std(self.deadline)) => {
                Err(ProgressError::DeadlineExceeded)
            }
            result = sink.report(delivery) => result,
        }
    }

    /// Closes this request's progress capability and waits for every accepted delivery to be
    /// published or suppressed.
    ///
    /// # Errors
    ///
    /// Returns [`ProgressError::State`] when lifecycle state was poisoned.
    pub async fn close(&self) -> Result<(), ProgressError> {
        self.lifecycle.close_and_drain().await
    }

    fn ensure_live(&self) -> Result<(), ProgressError> {
        if self.cancellation.is_cancelled() || self.lifecycle.is_closed() {
            return Err(ProgressError::Cancelled);
        }
        if Instant::now() >= self.deadline {
            return Err(ProgressError::DeadlineExceeded);
        }
        Ok(())
    }
}

impl fmt::Debug for ProgressReporter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProgressReporter")
            .field("enabled", &self.is_enabled())
            .field("cancellation", &"[CANCELLATION TOKEN]")
            .field("deadline", &self.deadline)
            .field("limits", &self.limits)
            .field("state", &"[PROGRESS STATE]")
            .field("delivery", &"[ORDERED DELIVERY]")
            .field("lifecycle", &"[PROGRESS LIFECYCLE]")
            .finish()
    }
}

/// Bounded progress validation or delivery failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ProgressError {
    /// The caller did not provide a progress capability.
    #[error("progress reporting is unavailable")]
    Unavailable,
    /// Completed work moved backwards.
    #[error("progress must be monotonic")]
    NonMonotonic,
    /// Completed or total work cannot be represented exactly or total is below completed work.
    #[error("progress value is invalid")]
    InvalidValue,
    /// The per-request update count was exhausted.
    #[error("progress update limit exceeded")]
    TooManyUpdates,
    /// Optional status text exceeded its byte ceiling.
    #[error("progress message byte limit exceeded")]
    MessageTooLong,
    /// Request cancellation won the lifecycle race.
    #[error("progress request was cancelled")]
    Cancelled,
    /// Request deadline elapsed.
    #[error("progress request deadline exceeded")]
    DeadlineExceeded,
    /// The bounded transport could not deliver the update.
    #[error("progress transport is unavailable")]
    Delivery,
    /// Internal progress state was poisoned.
    #[error("progress state is unavailable")]
    State,
}
