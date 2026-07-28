//! Safe, preallocated, bounded capture queue.

mod core;
mod lifecycle;
mod receiver;
#[cfg(any(test, all(feature = "capture-test", debug_assertions)))]
mod receiver_wait;
mod sender;

use std::marker::PhantomData;
use std::num::NonZeroUsize;
#[cfg(any(test, all(feature = "capture-test", debug_assertions)))]
use std::time::Duration;

#[cfg(market_squawk_loom)]
use loom::sync::atomic::{AtomicBool, AtomicUsize};
#[cfg(market_squawk_loom)]
use loom::sync::{Arc, Mutex};
#[cfg(not(market_squawk_loom))]
use std::sync::atomic::{AtomicBool, AtomicUsize};
#[cfg(not(market_squawk_loom))]
use std::sync::{Arc, Mutex};

use market_squawk_domain::checked_arc_value_allocation_bytes;
use thiserror::Error;

use core::{QueueCore, QueueSlot};
pub(super) use lifecycle::OperationLifecycle;
#[cfg(any(
    all(
        feature = "capture-benchmark",
        any(test, capture_bench_backend = "standard")
    ),
    all(test, market_squawk_loom)
))]
pub(super) use lifecycle::OperationRegistrationError;
pub(super) use receiver::FixedReceiver;
#[cfg(any(test, all(feature = "capture-test", debug_assertions)))]
pub(super) use receiver_wait::ReceiverPauseError;
#[cfg(any(test, all(feature = "capture-test", debug_assertions)))]
use receiver_wait::ReceiverTestCoordination;
pub(super) use sender::FixedSender;

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
    #[error("fixed queue receiver is closed")]
    Closed(T),
    #[error("fixed queue state is poisoned")]
    Poisoned(T),
    #[error("fixed queue ownership invariant failed")]
    Invariant(T),
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(super) enum TryCloneError {
    #[error("fixed queue receiver is closed")]
    Closed,
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
    #[cfg(all(
        feature = "capture-benchmark",
        any(test, capture_bench_backend = "candidate")
    ))]
    #[error("fixed queue state is contended")]
    Contended,
    #[error("fixed queue state is poisoned")]
    Poisoned,
}

impl RecvTimeoutError {
    #[cfg(test)]
    pub(super) const fn is_timeout(self) -> bool {
        matches!(self, Self::Timeout)
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
        for sequence in 0..capacity.get() {
            slots.push(QueueSlot {
                sequence: AtomicUsize::new(sequence),
                ready: AtomicBool::new(false),
                value: Mutex::new(None),
            });
        }
        let observed_slot_capacity = slots.capacity();
        let retained_slot_bytes = observed_slot_capacity
            .checked_mul(std::mem::size_of::<QueueSlot<T>>())
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
        let position_modulus = usize::MAX - (usize::MAX % capacity.get());
        if position_modulus <= capacity.get() {
            return Err(QueueConstructionError::ArithmeticOverflow);
        }
        let core = Arc::new(QueueCore {
            slots,
            position_modulus,
            enqueue_position: AtomicUsize::new(0),
            dequeue_position: AtomicUsize::new(0),
            sender_count: AtomicUsize::new(1),
            operation_lifecycle: OperationLifecycle::new(),
            consumer_gate: Mutex::new(()),
            #[cfg(not(market_squawk_loom))]
            receiver_thread: Mutex::new(None),
            #[cfg(any(test, all(feature = "capture-test", debug_assertions)))]
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
    pub(super) fn request_close(&self) -> Result<(), QueueControlError> {
        self.core.request_close();
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn close(&self) -> Result<(), QueueControlError> {
        self.core.close();
        Ok(())
    }

    pub(super) fn close_and_drain(&self) -> Result<(), QueueControlError> {
        self.core.close_and_drain()
    }

    #[cfg(any(test, all(feature = "capture-test", debug_assertions)))]
    pub(super) fn with_receiver_paused_for_test<R>(
        &self,
        timeout: Duration,
        action: impl FnOnce() -> R,
    ) -> Result<R, ReceiverPauseError> {
        let guard = self.core.receiver_test_coordination.request()?;
        self.core.notify_receiver();
        guard.wait_until_parked(timeout)?;
        let result = action();
        drop(guard);
        Ok(result)
    }
}

#[cfg(test)]
mod tests;

#[cfg(all(test, market_squawk_loom))]
mod loom_model;
