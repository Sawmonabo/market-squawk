//! Fixed-capacity queue producer ownership and publication.

use std::sync::TryLockError;

#[cfg(loom)]
use loom::sync::Arc;
#[cfg(loom)]
use loom::sync::atomic::Ordering;
#[cfg(not(loom))]
use std::sync::Arc;
#[cfg(not(loom))]
use std::sync::atomic::Ordering;

#[cfg(any(
    test,
    all(feature = "capture-benchmark", capture_bench_backend = "candidate")
))]
use super::QueueControlError;
use super::core::{BeginOperationError, QueueCore};
use super::{TryCloneError, TrySendError};

pub(in crate::capture) struct FixedSender<T> {
    pub(super) core: Arc<QueueCore<T>>,
}

impl<T> std::fmt::Debug for FixedSender<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("FixedSender { .. }")
    }
}

impl<T> FixedSender<T> {
    pub(in crate::capture) fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        self.try_send_inner(value, || {})
    }

    fn try_send_inner(&self, value: T, registered: impl FnOnce()) -> Result<(), TrySendError<T>> {
        let _active = match self.core.begin_operation() {
            Ok(active) => active,
            Err(BeginOperationError::Closed) => return Err(TrySendError::Closed(value)),
            Err(BeginOperationError::Invariant) => return Err(TrySendError::Invariant(value)),
        };
        registered();
        let capacity = self.core.slots.len();
        let mut position = self.core.enqueue_position.load(Ordering::Relaxed);
        loop {
            let slot = &self.core.slots[position % capacity];
            let sequence = slot.sequence.load(Ordering::Acquire);
            let difference = self.core.sequence_difference(sequence, position);
            if difference == 0 {
                let next_position = self.core.advance_position(position, 1);
                match self.core.enqueue_position.compare_exchange_weak(
                    position,
                    next_position,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_previous) => {
                        let mut retained = match slot.value.try_lock() {
                            Ok(retained) => retained,
                            Err(TryLockError::WouldBlock) => {
                                self.core.fail_closed();
                                let _rollback = self.core.enqueue_position.compare_exchange(
                                    next_position,
                                    position,
                                    Ordering::Relaxed,
                                    Ordering::Relaxed,
                                );
                                return Err(TrySendError::Invariant(value));
                            }
                            Err(TryLockError::Poisoned(_poisoned)) => {
                                self.core.fail_closed();
                                return Err(TrySendError::Poisoned(value));
                            }
                        };
                        if retained.is_some() {
                            self.core.fail_closed();
                            return Err(TrySendError::Invariant(value));
                        }
                        if slot.ready.load(Ordering::Acquire) {
                            self.core.fail_closed();
                            return Err(TrySendError::Invariant(value));
                        }
                        *retained = Some(value);
                        drop(retained);
                        slot.ready.store(true, Ordering::Release);
                        self.core.notify_receiver();
                        return Ok(());
                    }
                    Err(current) => position = current,
                }
            } else if difference < 0 {
                return Err(TrySendError::Full(value));
            } else {
                position = self.core.enqueue_position.load(Ordering::Relaxed);
            }
        }
    }

    #[cfg(all(test, not(loom)))]
    pub(in crate::capture) fn try_send_after_registration_paused_for_test(
        &self,
        value: T,
        entered: &std::sync::Barrier,
        release: &std::sync::Barrier,
    ) -> Result<(), TrySendError<T>> {
        self.try_send_inner(value, || {
            entered.wait();
            release.wait();
        })
    }

    pub(in crate::capture) fn try_clone(&self) -> Result<Self, TryCloneError> {
        self.try_clone_inner(|| {})
    }

    fn try_clone_inner(&self, registered: impl FnOnce()) -> Result<Self, TryCloneError> {
        let _active = self.core.begin_operation().map_err(|error| match error {
            BeginOperationError::Closed => TryCloneError::Closed,
            BeginOperationError::Invariant => TryCloneError::CountOverflow,
        })?;
        registered();
        self.core
            .sender_count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1).filter(|_next| current != 0)
            })
            .map_err(|current| {
                if current == usize::MAX {
                    TryCloneError::CountOverflow
                } else {
                    TryCloneError::Closed
                }
            })?;
        Ok(Self {
            core: Arc::clone(&self.core),
        })
    }

    #[cfg(all(not(loom), any(test, all(feature = "capture-test", debug_assertions))))]
    pub(in crate::capture) fn try_clone_after_registration_paused_for_test(
        &self,
        entered: &std::sync::Barrier,
        release: &std::sync::Barrier,
    ) -> Result<Self, TryCloneError> {
        self.try_clone_inner(|| {
            entered.wait();
            release.wait();
        })
    }

    #[cfg(test)]
    pub(in crate::capture) fn sender_count(&self) -> Result<usize, TryCloneError> {
        Ok(self.core.sender_count.load(Ordering::Acquire))
    }

    #[cfg(test)]
    pub(in crate::capture) fn set_active_operations_for_test(&self, value: usize) {
        self.core
            .operation_lifecycle
            .set_active_operations_for_test(value);
    }

    #[cfg(test)]
    pub(in crate::capture) fn maximum_active_operations_for_test(&self) -> usize {
        self.core
            .operation_lifecycle
            .maximum_active_operations_for_test()
    }

    #[cfg(test)]
    pub(in crate::capture) fn seed_empty_near_position_wrap_for_test(
        &self,
    ) -> Result<(), QueueControlError> {
        let _consumer = self
            .core
            .consumer_gate
            .lock()
            .map_err(|_error| QueueControlError::Poisoned)?;
        if self.core.enqueue_position.load(Ordering::Acquire)
            != self.core.dequeue_position.load(Ordering::Acquire)
            || self.core.operation_lifecycle.active_operations() != 0
        {
            return Err(QueueControlError::Poisoned);
        }
        for slot in &self.core.slots {
            if slot
                .value
                .lock()
                .map_err(|_error| QueueControlError::Poisoned)?
                .is_some()
            {
                return Err(QueueControlError::Poisoned);
            }
            if slot.ready.load(Ordering::Acquire) {
                return Err(QueueControlError::Poisoned);
            }
        }
        let start = self.core.position_modulus - 2;
        let mut position = start;
        for _ordinal in 0..self.core.slots.len() {
            let slot = &self.core.slots[position % self.core.slots.len()];
            slot.sequence.store(position, Ordering::Release);
            slot.ready.store(false, Ordering::Release);
            position = self.core.advance_position(position, 1);
        }
        self.core.enqueue_position.store(start, Ordering::Release);
        self.core.dequeue_position.store(start, Ordering::Release);
        Ok(())
    }

    #[cfg(test)]
    pub(in crate::capture) fn hold_state_for_test(
        &self,
        entered: &std::sync::Barrier,
        release: &std::sync::Barrier,
    ) {
        let position = self.core.enqueue_position.load(Ordering::Acquire);
        let slot = &self.core.slots[position % self.core.slots.len()];
        let _state = match slot.value.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        entered.wait();
        release.wait();
    }

    #[cfg(all(
        feature = "capture-benchmark",
        any(test, capture_bench_backend = "candidate")
    ))]
    pub(in crate::capture) fn with_state_locked_for_benchmark<R>(
        &self,
        action: impl FnOnce() -> R,
    ) -> Result<R, QueueControlError> {
        let position = self.core.enqueue_position.load(Ordering::Acquire);
        let slot = &self.core.slots[position % self.core.slots.len()];
        let _state = match slot.value.try_lock() {
            Ok(state) => state,
            Err(TryLockError::WouldBlock) => return Err(QueueControlError::Contended),
            Err(TryLockError::Poisoned(_poisoned)) => return Err(QueueControlError::Poisoned),
        };
        Ok(action())
    }
}

impl<T> Drop for FixedSender<T> {
    fn drop(&mut self) {
        let previous =
            self.core
                .sender_count
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    current.checked_sub(1)
                });
        if !matches!(previous, Ok(current) if current > 1) {
            self.core.fail_closed();
        }
    }
}
