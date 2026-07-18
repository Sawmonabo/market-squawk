//! Sealed, statically dispatched record-queue transports for capture composition.

use std::num::NonZeroUsize;
#[cfg(all(
    feature = "capture-benchmark",
    any(test, capture_bench_backend = "standard")
))]
use std::sync::{Arc, mpsc};
use std::time::Duration;
#[cfg(all(
    feature = "capture-benchmark",
    any(test, capture_bench_backend = "standard")
))]
use std::time::Instant;

use super::queue::{
    FixedQueue, FixedQueueControl, FixedReceiver, FixedSender, FixedStorageReceipt,
    QueueConstructionError, QueueControlError, RecvTimeoutError, TryCloneError, TryRecvError,
    TrySendError,
};
#[cfg(all(
    feature = "capture-benchmark",
    any(test, capture_bench_backend = "standard")
))]
use super::queue::{OperationLifecycle, OperationRegistrationError};
mod sealed {
    pub(in crate::capture) trait Sealed {}
}

pub(super) trait CaptureQueueSender<T>: std::fmt::Debug + Send + Sized + 'static {
    fn try_send(&self, value: T) -> Result<(), TrySendError<T>>;
    fn try_clone(&self) -> Result<Self, TryCloneError>;
}

pub(super) trait CaptureQueueReceiver<T>: std::fmt::Debug + Send + Sized + 'static {
    fn try_recv(&self) -> Result<T, TryRecvError>;
    fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError>;
}

pub(super) trait CaptureQueueControl<T>:
    std::fmt::Debug + Clone + Send + Sync + Sized + 'static
{
}

type QueueTransportParts<T, Q> = (
    <Q as CaptureQueueTransport>::Sender<T>,
    <Q as CaptureQueueTransport>::Receiver<T>,
    <Q as CaptureQueueTransport>::Control<T>,
    QueueStorageReceipt,
);

pub(super) trait CaptureQueueTransport:
    sealed::Sealed + std::fmt::Debug + Sized + 'static
{
    #[cfg(feature = "capture-benchmark")]
    const IDENTITY: &'static str;
    #[cfg(feature = "capture-benchmark")]
    const PRIVATE_STORAGE_ACCOUNTING: &'static str;

    type Sender<T: Send + 'static>: CaptureQueueSender<T>;
    type Receiver<T: Send + 'static>: CaptureQueueReceiver<T>;
    type Control<T: Send + 'static>: CaptureQueueControl<T>;

    fn try_new<T: Send + 'static>(
        capacity: NonZeroUsize,
    ) -> Result<QueueTransportParts<T, Self>, QueueConstructionError>;

    #[cfg(feature = "capture-benchmark")]
    fn request_close<T: Send + 'static>(
        control: &Self::Control<T>,
    ) -> Result<(), QueueControlError>;

    fn close_and_drain<T: Send + 'static>(
        control: &Self::Control<T>,
        receiver: Option<&Self::Receiver<T>>,
    ) -> Result<(), QueueControlError>;
}

/// Storage truth for the selected queue implementation.
///
/// Stable `sync_channel` does not expose its allocator-retained private bytes. Its benchmark-only
/// receipt therefore carries no byte value at all: it must never be interpreted as zero, an
/// estimate, or an exact contribution to the production capture-memory ceiling.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum QueueStorageReceipt {
    FixedExact(FixedStorageReceipt),
    #[cfg(all(
        feature = "capture-benchmark",
        any(test, capture_bench_backend = "standard")
    ))]
    StandardReferenceOpaque {
        logical_capacity: usize,
    },
}

impl QueueStorageReceipt {
    pub(super) const fn logical_capacity(self) -> usize {
        match self {
            Self::FixedExact(receipt) => receipt.logical_capacity(),
            #[cfg(all(
                feature = "capture-benchmark",
                any(test, capture_bench_backend = "standard")
            ))]
            Self::StandardReferenceOpaque { logical_capacity } => logical_capacity,
        }
    }

    pub(super) const fn observed_slot_capacity(self) -> Option<usize> {
        match self {
            Self::FixedExact(receipt) => Some(receipt.observed_slot_capacity()),
            #[cfg(all(
                feature = "capture-benchmark",
                any(test, capture_bench_backend = "standard")
            ))]
            Self::StandardReferenceOpaque { .. } => None,
        }
    }

    pub(super) const fn retained_slot_bytes(self) -> Option<usize> {
        match self {
            Self::FixedExact(receipt) => Some(receipt.retained_slot_bytes()),
            #[cfg(all(
                feature = "capture-benchmark",
                any(test, capture_bench_backend = "standard")
            ))]
            Self::StandardReferenceOpaque { .. } => None,
        }
    }

    pub(super) const fn retained_queue_bytes(self) -> Option<usize> {
        match self {
            Self::FixedExact(receipt) => Some(receipt.retained_queue_bytes()),
            #[cfg(all(
                feature = "capture-benchmark",
                any(test, capture_bench_backend = "standard")
            ))]
            Self::StandardReferenceOpaque { .. } => None,
        }
    }

    #[cfg(all(test, feature = "capture-benchmark"))]
    pub(super) const fn is_standard_reference_opaque(self) -> bool {
        matches!(self, Self::StandardReferenceOpaque { .. })
    }
}

#[derive(Debug)]
pub(super) struct FixedRingTransport;

impl sealed::Sealed for FixedRingTransport {}

impl CaptureQueueTransport for FixedRingTransport {
    #[cfg(feature = "capture-benchmark")]
    const IDENTITY: &'static str = "candidate_fixed_ring";
    #[cfg(feature = "capture-benchmark")]
    const PRIVATE_STORAGE_ACCOUNTING: &'static str = "exact";

    type Sender<T: Send + 'static> = FixedSender<T>;
    type Receiver<T: Send + 'static> = FixedReceiver<T>;
    type Control<T: Send + 'static> = FixedQueueControl<T>;

    fn try_new<T: Send + 'static>(
        capacity: NonZeroUsize,
    ) -> Result<QueueTransportParts<T, Self>, QueueConstructionError> {
        let (sender, receiver, control, receipt) = FixedQueue::try_new(capacity)?;
        Ok((
            sender,
            receiver,
            control,
            QueueStorageReceipt::FixedExact(receipt),
        ))
    }

    #[cfg(feature = "capture-benchmark")]
    fn request_close<T: Send + 'static>(
        control: &Self::Control<T>,
    ) -> Result<(), QueueControlError> {
        control.request_close()
    }

    fn close_and_drain<T: Send + 'static>(
        control: &Self::Control<T>,
        _receiver: Option<&Self::Receiver<T>>,
    ) -> Result<(), QueueControlError> {
        control.close_and_drain()
    }
}

impl<T: Send + 'static> CaptureQueueSender<T> for FixedSender<T> {
    fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        Self::try_send(self, value)
    }

    fn try_clone(&self) -> Result<Self, TryCloneError> {
        Self::try_clone(self)
    }
}

impl<T: Send + 'static> CaptureQueueReceiver<T> for FixedReceiver<T> {
    fn try_recv(&self) -> Result<T, TryRecvError> {
        Self::try_recv(self)
    }

    fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        Self::recv_timeout(self, timeout)
    }
}

impl<T: Send + 'static> CaptureQueueControl<T> for FixedQueueControl<T> {}

#[cfg(all(
    feature = "capture-benchmark",
    any(test, capture_bench_backend = "standard")
))]
#[derive(Debug)]
struct StandardReferenceLifecycle {
    state: OperationLifecycle,
}

#[cfg(all(
    feature = "capture-benchmark",
    any(test, capture_bench_backend = "standard")
))]
impl StandardReferenceLifecycle {
    fn new() -> Self {
        Self {
            state: OperationLifecycle::new(),
        }
    }

    fn begin(&self) -> Result<StandardReferenceOperation<'_>, OperationRegistrationError> {
        self.state.begin()?;
        Ok(StandardReferenceOperation { lifecycle: self })
    }

    fn close(&self) {
        self.request_close();
        while self.state.active_operations() != 0 {
            std::thread::yield_now();
        }
    }

    fn request_close(&self) {
        self.state.close_registration();
    }

    fn is_closed(&self) -> bool {
        self.state.is_terminally_closed()
    }
}

#[cfg(all(
    feature = "capture-benchmark",
    any(test, capture_bench_backend = "standard")
))]
#[derive(Debug)]
struct StandardReferenceOperation<'a> {
    lifecycle: &'a StandardReferenceLifecycle,
}

#[cfg(all(
    feature = "capture-benchmark",
    any(test, capture_bench_backend = "standard")
))]
impl Drop for StandardReferenceOperation<'_> {
    fn drop(&mut self) {
        if self.lifecycle.state.finish().is_err() {
            self.lifecycle.state.close_registration();
        }
    }
}

#[cfg(all(
    feature = "capture-benchmark",
    any(test, capture_bench_backend = "standard")
))]
#[derive(Debug)]
struct StandardReferenceCore {
    lifecycle: StandardReferenceLifecycle,
}

#[cfg(all(
    feature = "capture-benchmark",
    any(test, capture_bench_backend = "standard")
))]
pub(super) struct StandardReferenceSender<T> {
    sender: mpsc::SyncSender<T>,
    core: Arc<StandardReferenceCore>,
}

#[cfg(all(
    feature = "capture-benchmark",
    any(test, capture_bench_backend = "standard")
))]
pub(super) struct StandardReferenceReceiver<T> {
    receiver: mpsc::Receiver<T>,
    core: Arc<StandardReferenceCore>,
}

#[cfg(all(
    feature = "capture-benchmark",
    any(test, capture_bench_backend = "standard")
))]
pub(super) struct StandardReferenceControl<T> {
    core: Arc<StandardReferenceCore>,
    _message: std::marker::PhantomData<fn() -> T>,
}

#[cfg(all(
    feature = "capture-benchmark",
    any(test, capture_bench_backend = "standard")
))]
impl<T> Clone for StandardReferenceControl<T> {
    fn clone(&self) -> Self {
        Self {
            core: Arc::clone(&self.core),
            _message: std::marker::PhantomData,
        }
    }
}

#[cfg(all(
    feature = "capture-benchmark",
    any(test, capture_bench_backend = "standard")
))]
impl<T> std::fmt::Debug for StandardReferenceSender<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("StandardReferenceSender { .. }")
    }
}

#[cfg(all(
    feature = "capture-benchmark",
    any(test, capture_bench_backend = "standard")
))]
impl<T> std::fmt::Debug for StandardReferenceReceiver<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("StandardReferenceReceiver { .. }")
    }
}

#[cfg(all(
    feature = "capture-benchmark",
    any(test, capture_bench_backend = "standard")
))]
impl<T> std::fmt::Debug for StandardReferenceControl<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("StandardReferenceControl { .. }")
    }
}

#[cfg(all(
    feature = "capture-benchmark",
    any(test, capture_bench_backend = "standard")
))]
impl<T: Send + 'static> StandardReferenceSender<T> {
    fn try_send_inner(&self, value: T, registered: impl FnOnce()) -> Result<(), TrySendError<T>> {
        let _operation = match self.core.lifecycle.begin() {
            Ok(operation) => operation,
            Err(OperationRegistrationError::Closed) => {
                return Err(TrySendError::Closed(value));
            }
            Err(OperationRegistrationError::CountOverflow) => {
                return Err(TrySendError::Invariant(value));
            }
        };
        registered();
        match self.sender.try_send(value) {
            Ok(()) => Ok(()),
            Err(mpsc::TrySendError::Full(value)) => Err(TrySendError::Full(value)),
            Err(mpsc::TrySendError::Disconnected(value)) => Err(TrySendError::Closed(value)),
        }
    }

    fn try_clone_inner(&self, registered: impl FnOnce()) -> Result<Self, TryCloneError> {
        let _operation = self.core.lifecycle.begin().map_err(|error| match error {
            OperationRegistrationError::Closed => TryCloneError::Closed,
            OperationRegistrationError::CountOverflow => TryCloneError::CountOverflow,
        })?;
        registered();
        Ok(Self {
            sender: self.sender.clone(),
            core: Arc::clone(&self.core),
        })
    }

    #[cfg(test)]
    pub(super) fn try_send_registered_for_test(
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

    #[cfg(test)]
    pub(super) fn try_clone_registered_for_test(
        &self,
        entered: &std::sync::Barrier,
        release: &std::sync::Barrier,
    ) -> Result<Self, TryCloneError> {
        self.try_clone_inner(|| {
            entered.wait();
            release.wait();
        })
    }
}

#[cfg(all(
    feature = "capture-benchmark",
    any(test, capture_bench_backend = "standard")
))]
impl<T> StandardReferenceControl<T> {
    #[cfg(test)]
    pub(super) fn wait_until_closing_for_test(
        &self,
        timeout: Duration,
    ) -> Result<(), QueueControlError> {
        let started = Instant::now();
        while !self.core.lifecycle.state.registration_is_closed_for_test() {
            if started.elapsed() >= timeout {
                return Err(QueueControlError::Contended);
            }
            std::thread::yield_now();
        }
        Ok(())
    }
}

#[cfg(all(
    feature = "capture-benchmark",
    any(test, capture_bench_backend = "standard")
))]
#[derive(Debug)]
pub(super) struct StandardReferenceTransport;

#[cfg(all(test, feature = "capture-benchmark"))]
impl StandardReferenceTransport {
    pub(super) fn close<T: Send + 'static>(
        control: &StandardReferenceControl<T>,
    ) -> Result<(), QueueControlError> {
        control.core.lifecycle.close();
        Ok(())
    }
}

#[cfg(all(
    feature = "capture-benchmark",
    any(test, capture_bench_backend = "standard")
))]
impl sealed::Sealed for StandardReferenceTransport {}

#[cfg(all(
    feature = "capture-benchmark",
    any(test, capture_bench_backend = "standard")
))]
impl CaptureQueueTransport for StandardReferenceTransport {
    #[cfg(feature = "capture-benchmark")]
    const IDENTITY: &'static str = "standard_sync_channel";
    #[cfg(feature = "capture-benchmark")]
    const PRIVATE_STORAGE_ACCOUNTING: &'static str = "not_measured";

    type Sender<T: Send + 'static> = StandardReferenceSender<T>;
    type Receiver<T: Send + 'static> = StandardReferenceReceiver<T>;
    type Control<T: Send + 'static> = StandardReferenceControl<T>;

    fn try_new<T: Send + 'static>(
        capacity: NonZeroUsize,
    ) -> Result<QueueTransportParts<T, Self>, QueueConstructionError> {
        let (sender, receiver) = mpsc::sync_channel(capacity.get());
        let core = Arc::new(StandardReferenceCore {
            lifecycle: StandardReferenceLifecycle::new(),
        });
        Ok((
            StandardReferenceSender {
                sender,
                core: Arc::clone(&core),
            },
            StandardReferenceReceiver {
                receiver,
                core: Arc::clone(&core),
            },
            StandardReferenceControl {
                core,
                _message: std::marker::PhantomData,
            },
            QueueStorageReceipt::StandardReferenceOpaque {
                logical_capacity: capacity.get(),
            },
        ))
    }

    #[cfg(feature = "capture-benchmark")]
    fn request_close<T: Send + 'static>(
        control: &Self::Control<T>,
    ) -> Result<(), QueueControlError> {
        control.core.lifecycle.request_close();
        Ok(())
    }

    fn close_and_drain<T: Send + 'static>(
        control: &Self::Control<T>,
        receiver: Option<&Self::Receiver<T>>,
    ) -> Result<(), QueueControlError> {
        control.core.lifecycle.close();
        if let Some(receiver) = receiver {
            while receiver.receiver.try_recv().is_ok() {}
        }
        Ok(())
    }
}

#[cfg(all(
    feature = "capture-benchmark",
    any(test, capture_bench_backend = "standard")
))]
impl<T: Send + 'static> CaptureQueueSender<T> for StandardReferenceSender<T> {
    fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        self.try_send_inner(value, || {})
    }

    fn try_clone(&self) -> Result<Self, TryCloneError> {
        self.try_clone_inner(|| {})
    }
}

#[cfg(all(
    feature = "capture-benchmark",
    any(test, capture_bench_backend = "standard")
))]
impl<T: Send + 'static> CaptureQueueReceiver<T> for StandardReferenceReceiver<T> {
    fn try_recv(&self) -> Result<T, TryRecvError> {
        match self.receiver.try_recv() {
            Ok(value) => Ok(value),
            Err(mpsc::TryRecvError::Empty) if self.core.lifecycle.is_closed() => {
                Err(TryRecvError::Closed)
            }
            Err(mpsc::TryRecvError::Empty) => Err(TryRecvError::Empty),
            Err(mpsc::TryRecvError::Disconnected) => Err(TryRecvError::Closed),
        }
    }

    fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        let started = Instant::now();
        let deadline = started.checked_add(timeout).unwrap_or(started);
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(RecvTimeoutError::Timeout);
            }
            let slice = remaining.min(Duration::from_millis(1));
            match self.receiver.recv_timeout(slice) {
                Ok(value) => return Ok(value),
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(RecvTimeoutError::Closed);
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
            }
            if self.core.lifecycle.is_closed() {
                return match self.try_recv() {
                    Ok(value) => Ok(value),
                    Err(TryRecvError::Poisoned) => Err(RecvTimeoutError::Poisoned),
                    Err(TryRecvError::Empty | TryRecvError::Closed) => {
                        Err(RecvTimeoutError::Closed)
                    }
                };
            }
        }
    }
}

#[cfg(all(
    feature = "capture-benchmark",
    any(test, capture_bench_backend = "standard")
))]
impl<T> Drop for StandardReferenceReceiver<T> {
    fn drop(&mut self) {
        self.core.lifecycle.close();
        while self.receiver.try_recv().is_ok() {}
    }
}

#[cfg(all(
    feature = "capture-benchmark",
    any(test, capture_bench_backend = "standard")
))]
impl<T: Send + 'static> CaptureQueueControl<T> for StandardReferenceControl<T> {}
