//! Single-owner queue consumption and bounded waiting.

use std::time::{Duration, Instant};

#[cfg(market_squawk_loom)]
use loom::sync::Arc;
#[cfg(all(
    market_squawk_loom,
    any(test, all(feature = "capture-test", debug_assertions))
))]
use loom::sync::atomic::Ordering;
#[cfg(not(market_squawk_loom))]
use std::sync::Arc;
#[cfg(all(
    not(market_squawk_loom),
    any(test, all(feature = "capture-test", debug_assertions))
))]
use std::sync::atomic::Ordering;

use super::core::QueueCore;
use super::{RecvTimeoutError, TryRecvError};

pub(in crate::capture) struct FixedReceiver<T> {
    pub(super) core: Arc<QueueCore<T>>,
}

impl<T> std::fmt::Debug for FixedReceiver<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("FixedReceiver { .. }")
    }
}

impl<T> FixedReceiver<T> {
    pub(in crate::capture) fn try_recv(&self) -> Result<T, TryRecvError> {
        #[cfg(any(test, all(feature = "capture-test", debug_assertions)))]
        self.core
            .receiver_test_coordination
            .park_if_requested()
            .map_err(|_error| TryRecvError::Poisoned)?;
        self.core.try_pop()
    }

    pub(in crate::capture) fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<T, RecvTimeoutError> {
        self.recv_timeout_inner(timeout, || {}, || {})
    }

    fn recv_timeout_inner(
        &self,
        timeout: Duration,
        registered_for_wait: impl FnOnce(),
        mut before_park: impl FnMut(),
    ) -> Result<T, RecvTimeoutError> {
        #[cfg(any(test, all(feature = "capture-test", debug_assertions)))]
        self.core
            .receiver_test_coordination
            .park_if_requested()
            .map_err(|_error| RecvTimeoutError::Poisoned)?;
        let deadline = Instant::now()
            .checked_add(timeout)
            .unwrap_or_else(Instant::now);
        #[cfg(not(market_squawk_loom))]
        let mut registered_for_wait = Some(registered_for_wait);
        #[cfg(market_squawk_loom)]
        let _registered_for_wait = registered_for_wait;
        loop {
            match self.core.try_pop() {
                Ok(value) => return Ok(value),
                Err(TryRecvError::Closed) => return Err(RecvTimeoutError::Closed),
                Err(TryRecvError::Poisoned) => return Err(RecvTimeoutError::Poisoned),
                Err(TryRecvError::Empty) => {}
            }
            #[cfg(any(test, all(feature = "capture-test", debug_assertions)))]
            if self
                .core
                .receiver_test_coordination
                .requested_hint
                .load(Ordering::Acquire)
            {
                self.core
                    .receiver_test_coordination
                    .park_if_requested()
                    .map_err(|_error| RecvTimeoutError::Poisoned)?;
                continue;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(RecvTimeoutError::Timeout);
            }
            #[cfg(not(market_squawk_loom))]
            {
                let current = std::thread::current();
                let mut registered = match self.core.receiver_thread.lock() {
                    Ok(registered) => registered,
                    Err(poisoned) => poisoned.into_inner(),
                };
                *registered = Some(current.clone());
                if let Some(hook) = registered_for_wait.take() {
                    hook();
                }
                drop(registered);
                #[cfg(any(test, all(feature = "capture-test", debug_assertions)))]
                if self
                    .core
                    .receiver_test_coordination
                    .requested_hint
                    .load(Ordering::Acquire)
                {
                    self.core
                        .receiver_test_coordination
                        .park_if_requested()
                        .map_err(|_error| RecvTimeoutError::Poisoned)?;
                    continue;
                }
                match self.core.try_pop() {
                    Ok(value) => return Ok(value),
                    Err(TryRecvError::Closed) => return Err(RecvTimeoutError::Closed),
                    Err(TryRecvError::Poisoned) => return Err(RecvTimeoutError::Poisoned),
                    Err(TryRecvError::Empty) => {
                        before_park();
                        std::thread::park_timeout(remaining);
                    }
                }
            }
            #[cfg(market_squawk_loom)]
            {
                before_park();
                loom::thread::yield_now();
            }
        }
    }

    #[cfg(all(test, not(market_squawk_loom)))]
    pub(in crate::capture) fn recv_timeout_with_registration_paused_for_test(
        &self,
        timeout: Duration,
        entered: &std::sync::Barrier,
        release: &std::sync::Barrier,
    ) -> Result<T, RecvTimeoutError> {
        self.recv_timeout_inner(
            timeout,
            || {
                entered.wait();
                release.wait();
            },
            || {},
        )
    }

    #[cfg(all(test, not(market_squawk_loom)))]
    pub(in crate::capture) fn recv_timeout_with_each_park_paused_for_test(
        &self,
        timeout: Duration,
        entered: &std::sync::Barrier,
        release: &std::sync::Barrier,
    ) -> Result<T, RecvTimeoutError> {
        self.recv_timeout_inner(
            timeout,
            || {},
            || {
                entered.wait();
                release.wait();
            },
        )
    }

    #[cfg(test)]
    pub(in crate::capture) fn with_next_slot_locked_for_test<R>(
        &self,
        action: impl FnOnce() -> R,
    ) -> Result<R, TryRecvError> {
        let position = self.core.dequeue_position.load(Ordering::Acquire);
        let slot = &self.core.slots[position % self.core.slots.len()];
        let _retained = slot.value.lock().map_err(|_error| TryRecvError::Poisoned)?;
        Ok(action())
    }
}

impl<T> Drop for FixedReceiver<T> {
    fn drop(&mut self) {
        #[cfg(not(market_squawk_loom))]
        let _registered_thread = self.core.clear_receiver_thread();
        let _cleanup = self.core.close_and_drain();
    }
}
