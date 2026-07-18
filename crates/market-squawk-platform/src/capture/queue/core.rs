//! Queue slot ownership, ring state, and terminal lifecycle transitions.

#[cfg(not(loom))]
use std::sync::TryLockError;

#[cfg(loom)]
use loom::sync::Mutex;
#[cfg(loom)]
use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
#[cfg(not(loom))]
use std::sync::Mutex;
#[cfg(not(loom))]
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use super::lifecycle::{OperationFinishError, OperationLifecycle, OperationRegistrationError};
#[cfg(any(test, all(feature = "capture-test", debug_assertions)))]
use super::receiver_wait::ReceiverTestCoordination;
use super::{QueueControlError, TryRecvError};

#[derive(Debug)]
pub(super) struct QueueSlot<T> {
    pub(super) sequence: AtomicUsize,
    pub(super) ready: AtomicBool,
    pub(super) value: Mutex<Option<T>>,
}

#[derive(Debug)]
pub(super) struct QueueCore<T> {
    pub(super) slots: Vec<QueueSlot<T>>,
    pub(super) position_modulus: usize,
    pub(super) enqueue_position: AtomicUsize,
    pub(super) dequeue_position: AtomicUsize,
    pub(super) sender_count: AtomicUsize,
    pub(super) operation_lifecycle: OperationLifecycle,
    pub(super) consumer_gate: Mutex<()>,
    #[cfg(not(loom))]
    pub(super) receiver_thread: Mutex<Option<std::thread::Thread>>,
    #[cfg(any(test, all(feature = "capture-test", debug_assertions)))]
    pub(super) receiver_test_coordination: ReceiverTestCoordination,
}

impl<T> QueueCore<T> {
    pub(super) fn advance_position(&self, position: usize, step: usize) -> usize {
        let wrap_at = self.position_modulus - step;
        if position >= wrap_at {
            position - wrap_at
        } else {
            position + step
        }
    }

    pub(super) fn sequence_difference(&self, sequence: usize, expected: usize) -> isize {
        let forward = if sequence >= expected {
            sequence - expected
        } else {
            self.position_modulus - (expected - sequence)
        };
        if forward == 0 {
            0
        } else if forward > self.position_modulus / 2 {
            -1
        } else {
            1
        }
    }

    pub(super) fn notify_receiver(&self) {
        #[cfg(not(loom))]
        {
            let registered = match self.receiver_thread.try_lock() {
                Ok(registered) => registered,
                Err(TryLockError::WouldBlock) => return,
                Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            };
            if let Some(thread) = registered.as_ref() {
                thread.unpark();
            }
        }
    }

    #[cfg(not(loom))]
    pub(super) fn clear_receiver_thread(&self) -> Result<(), QueueControlError> {
        match self.receiver_thread.lock() {
            Ok(mut registered) => {
                registered.take();
                Ok(())
            }
            Err(poisoned) => {
                poisoned.into_inner().take();
                self.fail_closed();
                Err(QueueControlError::Poisoned)
            }
        }
    }

    fn is_terminally_closed(&self) -> bool {
        self.operation_lifecycle.is_terminally_closed()
    }

    pub(super) fn fail_closed(&self) {
        self.operation_lifecycle.close_registration();
        self.notify_receiver();
    }

    pub(super) fn begin_operation(&self) -> Result<ActiveOperation<'_, T>, BeginOperationError> {
        match self.operation_lifecycle.begin() {
            Ok(()) => Ok(ActiveOperation { core: self }),
            Err(OperationRegistrationError::Closed) => Err(BeginOperationError::Closed),
            Err(OperationRegistrationError::CountOverflow) => {
                self.fail_closed();
                Err(BeginOperationError::Invariant)
            }
        }
    }

    fn wait_for_active_operations(&self) {
        while self.operation_lifecycle.active_operations() != 0 {
            #[cfg(loom)]
            loom::thread::yield_now();
            #[cfg(not(loom))]
            std::thread::yield_now();
        }
    }

    pub(super) fn close(&self) {
        self.request_close();
        self.wait_for_active_operations();
        self.notify_receiver();
    }

    pub(super) fn request_close(&self) {
        self.fail_closed();
    }

    pub(super) fn try_pop(&self) -> Result<T, TryRecvError> {
        let _consumer = self.consumer_gate.lock().map_err(|_error| {
            self.fail_closed();
            TryRecvError::Poisoned
        })?;
        let capacity = self.slots.len();
        let mut position = self.dequeue_position.load(Ordering::Relaxed);
        loop {
            let slot = &self.slots[position % capacity];
            let sequence = slot.sequence.load(Ordering::Acquire);
            let difference = self.sequence_difference(sequence, position);
            if difference == 0 {
                if !slot.ready.load(Ordering::Acquire) {
                    return if self.is_terminally_closed() {
                        Err(TryRecvError::Closed)
                    } else {
                        Err(TryRecvError::Empty)
                    };
                }
                let next_position = self.advance_position(position, 1);
                match self.dequeue_position.compare_exchange_weak(
                    position,
                    next_position,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_previous) => {
                        let value = {
                            let mut retained = slot.value.lock().map_err(|_error| {
                                self.fail_closed();
                                TryRecvError::Poisoned
                            })?;
                            retained.take().ok_or_else(|| {
                                self.fail_closed();
                                TryRecvError::Poisoned
                            })?
                        };
                        slot.ready.store(false, Ordering::Relaxed);
                        slot.sequence
                            .store(self.advance_position(position, capacity), Ordering::Release);
                        return Ok(value);
                    }
                    Err(current) => position = current,
                }
            } else if difference < 0 {
                return if self.is_terminally_closed() {
                    Err(TryRecvError::Closed)
                } else {
                    Err(TryRecvError::Empty)
                };
            } else {
                position = self.dequeue_position.load(Ordering::Relaxed);
            }
        }
    }

    fn drain_all_slots(&self) -> Result<(), QueueControlError> {
        let consumer = self.consumer_gate.lock().map_err(|_error| {
            self.fail_closed();
            QueueControlError::Poisoned
        })?;
        let end = self.enqueue_position.load(Ordering::Acquire);
        self.dequeue_position.store(end, Ordering::Release);
        drop(consumer);
        let mut poisoned = false;
        for slot in &self.slots {
            let value = match slot.value.lock() {
                Ok(mut retained) => {
                    let value = retained.take();
                    slot.ready.store(false, Ordering::Release);
                    value
                }
                Err(lock_error) => {
                    poisoned = true;
                    let value = lock_error.into_inner().take();
                    slot.ready.store(false, Ordering::Release);
                    value
                }
            };
            drop(value);
        }
        if poisoned {
            Err(QueueControlError::Poisoned)
        } else {
            Ok(())
        }
    }

    pub(super) fn close_and_drain(&self) -> Result<(), QueueControlError> {
        self.close();
        self.drain_all_slots()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BeginOperationError {
    Closed,
    Invariant,
}

#[derive(Debug)]
pub(super) struct ActiveOperation<'a, T> {
    core: &'a QueueCore<T>,
}

impl<T> Drop for ActiveOperation<'_, T> {
    fn drop(&mut self) {
        match self.core.operation_lifecycle.finish() {
            Ok(true) => self.core.notify_receiver(),
            Ok(false) => {}
            Err(OperationFinishError::CounterUnderflow) => self.core.fail_closed(),
        }
    }
}
