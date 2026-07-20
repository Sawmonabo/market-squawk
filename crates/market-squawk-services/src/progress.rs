//! Transport-neutral, bounded progress reporting.

use std::{
    fmt,
    sync::{Arc, Mutex},
    time::Instant,
};

use async_trait::async_trait;
use thiserror::Error;
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
    /// Delivers one already validated progress update.
    ///
    /// # Errors
    ///
    /// Returns [`ProgressError::Delivery`] when the bounded transport cannot accept the update.
    async fn report(&self, update: ProgressUpdate) -> Result<(), ProgressError>;
}

#[derive(Debug, Default)]
struct ProgressState {
    accepted_updates: usize,
    last_completed: Option<u64>,
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
        if self.cancellation.is_cancelled() {
            return Err(ProgressError::Cancelled);
        }
        if Instant::now() >= self.deadline {
            return Err(ProgressError::DeadlineExceeded);
        }
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
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(self.deadline)) => {
                return Err(ProgressError::DeadlineExceeded);
            }
            guard = self.delivery.lock() => guard,
        };
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
        tokio::select! {
            biased;
            () = self.cancellation.cancelled() => Err(ProgressError::Cancelled),
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(self.deadline)) => {
                Err(ProgressError::DeadlineExceeded)
            }
            result = sink.report(update) => result,
        }
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
