//! Safe, preallocated, bounded capture queue.

use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::TryLockError;
use std::time::{Duration, Instant};

#[cfg(loom)]
use loom::sync::atomic::{AtomicBool, Ordering};
#[cfg(loom)]
use loom::sync::{Arc, Condvar, Mutex};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(not(loom))]
use std::sync::{Arc, Condvar, Mutex};

use market_squawk_domain::checked_arc_value_allocation_bytes;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct FixedStorageReceipt {
    logical_capacity: usize,
    observed_slot_capacity: usize,
    retained_slot_bytes: usize,
    retained_queue_bytes: usize,
}

impl FixedStorageReceipt {
    pub(super) const fn logical_capacity(self) -> usize {
        self.logical_capacity
    }

    pub(super) const fn observed_slot_capacity(self) -> usize {
        self.observed_slot_capacity
    }

    pub(super) const fn retained_slot_bytes(self) -> usize {
        self.retained_slot_bytes
    }

    pub(super) const fn retained_queue_bytes(self) -> usize {
        self.retained_queue_bytes
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(super) enum QueueConstructionError {
    #[error("fixed queue slot allocation failed")]
    AllocationFailed,
    #[error("fixed queue slot accounting overflowed")]
    ArithmeticOverflow,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub(super) enum TrySendError<T> {
    #[error("fixed queue is full")]
    Full(T),
    #[error("fixed queue state is contended")]
    Contended(T),
    #[error("fixed queue receiver is closed")]
    Closed(T),
    #[error("fixed queue state is poisoned")]
    Poisoned(T),
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(super) enum TryCloneError {
    #[error("fixed queue receiver is closed")]
    Closed,
    #[error("fixed queue state is poisoned")]
    Poisoned,
    #[error("fixed queue sender count overflowed")]
    CountOverflow,
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(super) enum TryRecvError {
    #[error("fixed queue is empty")]
    Empty,
    #[error("fixed queue is closed")]
    Closed,
    #[error("fixed queue state is poisoned")]
    Poisoned,
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(super) enum RecvTimeoutError {
    #[error("fixed queue receive timed out")]
    Timeout,
    #[error("fixed queue is closed")]
    Closed,
    #[error("fixed queue state is poisoned")]
    Poisoned,
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(super) enum QueueControlError {
    #[error("fixed queue state is poisoned")]
    Poisoned,
}

#[cfg(all(feature = "capture-test", debug_assertions))]
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(super) enum ReceiverPauseError {
    #[error("capture receiver test coordination state is poisoned")]
    Poisoned,
    #[error("capture receiver did not reach the test coordination barrier before its deadline")]
    DeadlineElapsed,
}

#[cfg(all(feature = "capture-test", debug_assertions))]
#[derive(Debug, Default)]
struct ReceiverPauseState {
    requested: bool,
    parked: bool,
}

#[cfg(all(feature = "capture-test", debug_assertions))]
#[derive(Debug)]
struct ReceiverTestCoordination {
    requested_hint: AtomicBool,
    state: Mutex<ReceiverPauseState>,
    changed: Condvar,
}

#[cfg(all(feature = "capture-test", debug_assertions))]
impl ReceiverTestCoordination {
    fn new() -> Self {
        Self {
            requested_hint: AtomicBool::new(false),
            state: Mutex::new(ReceiverPauseState::default()),
            changed: Condvar::new(),
        }
    }

    fn request(&self) -> Result<ReceiverPauseGuard<'_>, ReceiverPauseError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_error| ReceiverPauseError::Poisoned)?;
        state.requested = true;
        state.parked = false;
        self.requested_hint.store(true, Ordering::Release);
        drop(state);
        Ok(ReceiverPauseGuard { coordination: self })
    }

    fn park_if_requested(&self) -> Result<(), ReceiverPauseError> {
        if !self.requested_hint.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_error| ReceiverPauseError::Poisoned)?;
        if !state.requested {
            return Ok(());
        }
        state.parked = true;
        self.changed.notify_all();
        while state.requested {
            state = self
                .changed
                .wait(state)
                .map_err(|_error| ReceiverPauseError::Poisoned)?;
        }
        state.parked = false;
        Ok(())
    }
}

#[cfg(all(feature = "capture-test", debug_assertions))]
#[derive(Debug)]
struct ReceiverPauseGuard<'a> {
    coordination: &'a ReceiverTestCoordination,
}

#[cfg(all(feature = "capture-test", debug_assertions))]
impl ReceiverPauseGuard<'_> {
    fn wait_until_parked(&self, timeout: Duration) -> Result<(), ReceiverPauseError> {
        let start = Instant::now();
        let deadline = start.checked_add(timeout).unwrap_or(start);
        let mut state = self
            .coordination
            .state
            .lock()
            .map_err(|_error| ReceiverPauseError::Poisoned)?;
        while !state.parked {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(ReceiverPauseError::DeadlineElapsed);
            }
            let (next, timeout_result) = self
                .coordination
                .changed
                .wait_timeout(state, remaining)
                .map_err(|_error| ReceiverPauseError::Poisoned)?;
            state = next;
            if timeout_result.timed_out() && !state.parked {
                return Err(ReceiverPauseError::DeadlineElapsed);
            }
        }
        Ok(())
    }
}

#[cfg(all(feature = "capture-test", debug_assertions))]
impl Drop for ReceiverPauseGuard<'_> {
    fn drop(&mut self) {
        match self.coordination.state.lock() {
            Ok(mut state) => state.requested = false,
            Err(poisoned) => poisoned.into_inner().requested = false,
        }
        self.coordination
            .requested_hint
            .store(false, Ordering::Release);
        self.coordination.changed.notify_all();
    }
}

impl RecvTimeoutError {
    #[cfg(test)]
    pub(super) const fn is_timeout(self) -> bool {
        matches!(self, Self::Timeout)
    }
}

#[derive(Debug)]
struct QueueState<T> {
    slots: Vec<Option<T>>,
    head: usize,
    len: usize,
    sender_count: usize,
    receiver_alive: bool,
    closed: bool,
}

impl<T> QueueState<T> {
    fn pop_front(&mut self) -> Result<T, TryRecvError> {
        if self.len == 0 {
            return if self.closed || self.sender_count == 0 {
                Err(TryRecvError::Closed)
            } else {
                Err(TryRecvError::Empty)
            };
        }
        let index = self.head;
        let value = self.slots[index].take().ok_or(TryRecvError::Poisoned)?;
        self.head = (self.head + 1) % self.slots.len();
        self.len -= 1;
        Ok(value)
    }
}

#[derive(Debug)]
struct QueueCore<T> {
    state: Mutex<QueueState<T>>,
    available: Condvar,
    closed_hint: AtomicBool,
    #[cfg(all(feature = "capture-test", debug_assertions))]
    receiver_test_coordination: ReceiverTestCoordination,
}

impl<T> QueueCore<T> {
    fn close_and_drain(&self, receiver_dropped: bool) -> Result<(), QueueControlError> {
        {
            let mut state = self.state.lock().map_err(|_error| {
                self.closed_hint.store(true, Ordering::Release);
                QueueControlError::Poisoned
            })?;
            state.closed = true;
            if receiver_dropped {
                state.receiver_alive = false;
            }
            self.closed_hint.store(true, Ordering::Release);
        }
        self.available.notify_all();
        loop {
            let next = {
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_error| QueueControlError::Poisoned)?;
                if state.len == 0 {
                    None
                } else {
                    Some(
                        state
                            .pop_front()
                            .map_err(|_error| QueueControlError::Poisoned)?,
                    )
                }
            };
            let Some(value) = next else {
                return Ok(());
            };
            drop(value);
        }
    }
}

#[derive(Debug)]
pub(super) struct FixedQueue<T>(PhantomData<fn() -> T>);

type FixedQueueParts<T> = (
    FixedSender<T>,
    FixedReceiver<T>,
    FixedQueueControl<T>,
    FixedStorageReceipt,
);

impl<T> FixedQueue<T> {
    pub(super) fn try_new(
        capacity: NonZeroUsize,
    ) -> Result<FixedQueueParts<T>, QueueConstructionError> {
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(capacity.get())
            .map_err(|_error| QueueConstructionError::AllocationFailed)?;
        slots.resize_with(capacity.get(), || None);
        let observed_slot_capacity = slots.capacity();
        let retained_slot_bytes = observed_slot_capacity
            .checked_mul(std::mem::size_of::<Option<T>>())
            .ok_or(QueueConstructionError::ArithmeticOverflow)?;
        let retained_queue_bytes =
            checked_arc_value_allocation_bytes::<QueueCore<T>>(retained_slot_bytes)
                .map_err(|_error| QueueConstructionError::ArithmeticOverflow)?;
        let receipt = FixedStorageReceipt {
            logical_capacity: capacity.get(),
            observed_slot_capacity,
            retained_slot_bytes,
            retained_queue_bytes,
        };
        let core = Arc::new(QueueCore {
            state: Mutex::new(QueueState {
                slots,
                head: 0,
                len: 0,
                sender_count: 1,
                receiver_alive: true,
                closed: false,
            }),
            available: Condvar::new(),
            closed_hint: AtomicBool::new(false),
            #[cfg(all(feature = "capture-test", debug_assertions))]
            receiver_test_coordination: ReceiverTestCoordination::new(),
        });
        Ok((
            FixedSender {
                core: Arc::clone(&core),
            },
            FixedReceiver {
                core: Arc::clone(&core),
            },
            FixedQueueControl { core },
            receipt,
        ))
    }
}

pub(super) struct FixedSender<T> {
    core: Arc<QueueCore<T>>,
}

impl<T> std::fmt::Debug for FixedSender<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("FixedSender { .. }")
    }
}

impl<T> FixedSender<T> {
    pub(super) fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        if self.core.closed_hint.load(Ordering::Acquire) {
            return Err(TrySendError::Closed(value));
        }
        let mut state = match self.core.state.try_lock() {
            Ok(state) => state,
            Err(TryLockError::WouldBlock) => return Err(TrySendError::Contended(value)),
            Err(TryLockError::Poisoned(_poisoned)) => {
                self.core.closed_hint.store(true, Ordering::Release);
                return Err(TrySendError::Poisoned(value));
            }
        };
        if state.closed || !state.receiver_alive {
            self.core.closed_hint.store(true, Ordering::Release);
            return Err(TrySendError::Closed(value));
        }
        if state.len == state.slots.len() {
            return Err(TrySendError::Full(value));
        }
        let index = (state.head + state.len) % state.slots.len();
        if state.slots[index].is_some() {
            state.closed = true;
            self.core.closed_hint.store(true, Ordering::Release);
            return Err(TrySendError::Poisoned(value));
        }
        state.slots[index] = Some(value);
        state.len += 1;
        drop(state);
        self.core.available.notify_one();
        Ok(())
    }

    pub(super) fn try_clone(&self) -> Result<Self, TryCloneError> {
        let mut state = match self.core.state.lock() {
            Ok(state) => state,
            Err(_poisoned) => {
                self.core.closed_hint.store(true, Ordering::Release);
                return Err(TryCloneError::Poisoned);
            }
        };
        if state.closed || !state.receiver_alive {
            return Err(TryCloneError::Closed);
        }
        state.sender_count = state
            .sender_count
            .checked_add(1)
            .ok_or(TryCloneError::CountOverflow)?;
        Ok(Self {
            core: Arc::clone(&self.core),
        })
    }

    #[cfg(test)]
    pub(super) fn sender_count(&self) -> Result<usize, TryCloneError> {
        match self.core.state.lock() {
            Ok(state) => Ok(state.sender_count),
            Err(_poisoned) => Err(TryCloneError::Poisoned),
        }
    }

    #[cfg(test)]
    pub(super) fn hold_state_for_test(
        &self,
        entered: &std::sync::Barrier,
        release: &std::sync::Barrier,
    ) {
        let _state = match self.core.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        entered.wait();
        release.wait();
    }
}

impl<T> Drop for FixedSender<T> {
    fn drop(&mut self) {
        let mut state = match self.core.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        if state.sender_count == 0 {
            state.closed = true;
        } else {
            state.sender_count -= 1;
            if state.sender_count == 0 {
                state.closed = true;
            }
        }
        if state.closed {
            self.core.closed_hint.store(true, Ordering::Release);
            drop(state);
            self.core.available.notify_all();
        }
    }
}

pub(super) struct FixedReceiver<T> {
    core: Arc<QueueCore<T>>,
}

impl<T> std::fmt::Debug for FixedReceiver<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("FixedReceiver { .. }")
    }
}

impl<T> FixedReceiver<T> {
    pub(super) fn try_recv(&self) -> Result<T, TryRecvError> {
        #[cfg(all(feature = "capture-test", debug_assertions))]
        self.core
            .receiver_test_coordination
            .park_if_requested()
            .map_err(|_error| TryRecvError::Poisoned)?;
        let mut state = self
            .core
            .state
            .lock()
            .map_err(|_error| TryRecvError::Poisoned)?;
        state.pop_front()
    }

    pub(super) fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        #[cfg(all(feature = "capture-test", debug_assertions))]
        self.core
            .receiver_test_coordination
            .park_if_requested()
            .map_err(|_error| RecvTimeoutError::Poisoned)?;
        let deadline = Instant::now()
            .checked_add(timeout)
            .unwrap_or_else(Instant::now);
        let mut state = self
            .core
            .state
            .lock()
            .map_err(|_error| RecvTimeoutError::Poisoned)?;
        loop {
            match state.pop_front() {
                Ok(value) => return Ok(value),
                Err(TryRecvError::Closed) => return Err(RecvTimeoutError::Closed),
                Err(TryRecvError::Poisoned) => return Err(RecvTimeoutError::Poisoned),
                Err(TryRecvError::Empty) => {}
            }
            #[cfg(all(feature = "capture-test", debug_assertions))]
            if self
                .core
                .receiver_test_coordination
                .requested_hint
                .load(Ordering::Acquire)
            {
                drop(state);
                self.core
                    .receiver_test_coordination
                    .park_if_requested()
                    .map_err(|_error| RecvTimeoutError::Poisoned)?;
                state = self
                    .core
                    .state
                    .lock()
                    .map_err(|_error| RecvTimeoutError::Poisoned)?;
                continue;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(RecvTimeoutError::Timeout);
            }
            let (next, timeout_result) = self
                .core
                .available
                .wait_timeout(state, remaining)
                .map_err(|_error| RecvTimeoutError::Poisoned)?;
            state = next;
            if timeout_result.timed_out() && state.len == 0 {
                return if state.closed || state.sender_count == 0 {
                    Err(RecvTimeoutError::Closed)
                } else {
                    Err(RecvTimeoutError::Timeout)
                };
            }
            #[cfg(all(feature = "capture-test", debug_assertions))]
            if self
                .core
                .receiver_test_coordination
                .requested_hint
                .load(Ordering::Acquire)
            {
                drop(state);
                self.core
                    .receiver_test_coordination
                    .park_if_requested()
                    .map_err(|_error| RecvTimeoutError::Poisoned)?;
                state = self
                    .core
                    .state
                    .lock()
                    .map_err(|_error| RecvTimeoutError::Poisoned)?;
            }
        }
    }
}

impl<T> Drop for FixedReceiver<T> {
    fn drop(&mut self) {
        let _cleanup = self.core.close_and_drain(true);
    }
}

pub(super) struct FixedQueueControl<T> {
    core: Arc<QueueCore<T>>,
}

impl<T> std::fmt::Debug for FixedQueueControl<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("FixedQueueControl { .. }")
    }
}

impl<T> Clone for FixedQueueControl<T> {
    fn clone(&self) -> Self {
        Self {
            core: Arc::clone(&self.core),
        }
    }
}

impl<T> FixedQueueControl<T> {
    pub(super) fn close(&self) -> Result<(), QueueControlError> {
        let mut state = self.core.state.lock().map_err(|_error| {
            self.core.closed_hint.store(true, Ordering::Release);
            QueueControlError::Poisoned
        })?;
        state.closed = true;
        self.core.closed_hint.store(true, Ordering::Release);
        drop(state);
        self.core.available.notify_all();
        Ok(())
    }

    pub(super) fn close_and_drain(&self) -> Result<(), QueueControlError> {
        self.core.close_and_drain(false)
    }

    #[cfg(all(feature = "capture-test", debug_assertions))]
    pub(super) fn with_receiver_paused_for_test<R>(
        &self,
        timeout: Duration,
        action: impl FnOnce() -> R,
    ) -> Result<R, ReceiverPauseError> {
        let guard = self.core.receiver_test_coordination.request()?;
        let queue_state = self
            .core
            .state
            .lock()
            .map_err(|_error| ReceiverPauseError::Poisoned)?;
        drop(queue_state);
        self.core.available.notify_all();
        guard.wait_until_parked(timeout)?;
        let result = action();
        drop(guard);
        Ok(result)
    }
}

#[cfg(test)]
mod tests;

#[cfg(all(test, loom))]
mod loom_model;
