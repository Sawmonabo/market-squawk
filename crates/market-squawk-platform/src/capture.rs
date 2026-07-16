//! Non-blocking generic raw-capture publication and supervised storage.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak, mpsc};

use arc_swap::ArcSwap;
use market_squawk_domain::{
    CaptureAdmission, CaptureAuthorityBundle, CaptureAuthorityError, CaptureAuthorityIdentity,
    CaptureDegradation, CaptureIntegrityState, RawCaptureFrameView,
};
use thiserror::Error;

use crate::RawCaptureRecord;

const WRITER_NOT_STARTED: u8 = 0;
const WRITER_RUNNING: u8 = 1;
const WRITER_STOPPED: u8 = 2;
const HEALTH_EVENT_CAPACITY: usize = 64;
const CAPTURE_QUEUE_BYTE_BUDGET: usize = 64 * 1024 * 1024;

fn saturating_atomic_increment(counter: &AtomicU64) {
    let _previous = counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
        Some(current.saturating_add(1))
    });
}

/// Accepted diagnostic journal record derived from an exact authoritative raw frame.
///
/// This value carries audit identity only. Neither it nor its MSJ1 representation can recreate a
/// source-registry receipt or current live authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapturedRawRecord {
    identity: Arc<CaptureAuthorityIdentity>,
    frame_ordinal: std::num::NonZeroU64,
    record: RawCaptureRecord,
}

impl CapturedRawRecord {
    pub(super) fn new(
        identity: Arc<CaptureAuthorityIdentity>,
        frame_ordinal: std::num::NonZeroU64,
        record: RawCaptureRecord,
    ) -> Self {
        Self {
            identity,
            frame_ordinal,
            record,
        }
    }

    /// Returns immutable source/session/generation audit identity.
    pub fn identity(&self) -> &CaptureAuthorityIdentity {
        &self.identity
    }

    /// Returns the exact nonzero generation-local frame ordinal.
    pub const fn frame_ordinal(&self) -> std::num::NonZeroU64 {
        self.frame_ordinal
    }

    /// Returns the diagnostic committed-wire record.
    pub const fn record(&self) -> &RawCaptureRecord {
        &self.record
    }
}

#[derive(Debug)]
enum CaptureMessage<B: CaptureAuthorityBundle> {
    Record {
        allocation: Arc<GenerationCaptureState<B>>,
        frame: B::Frame,
        reservation: QueueByteReservation<B>,
    },
    Wake,
}

#[derive(Debug)]
struct QueueByteReservation<B: CaptureAuthorityBundle> {
    state: Weak<CaptureState<B>>,
    bytes: usize,
}

impl<B: CaptureAuthorityBundle> Drop for QueueByteReservation<B> {
    fn drop(&mut self) {
        if let Some(state) = self.state.upgrade() {
            state.release_queue_bytes_exact(self.bytes);
        }
    }
}

#[derive(Debug)]
struct GenerationCaptureState<B: CaptureAuthorityBundle> {
    identity: Arc<CaptureAuthorityIdentity>,
    admission: std::sync::Mutex<B::Admission>,
    degradation: B::Degradation,
    accepting: AtomicBool,
}

impl<B: CaptureAuthorityBundle> GenerationCaptureState<B> {
    fn new(
        identity: CaptureAuthorityIdentity,
        admission: B::Admission,
        degradation: B::Degradation,
    ) -> Self {
        Self {
            identity: Arc::new(identity),
            admission: std::sync::Mutex::new(admission),
            degradation,
            accepting: AtomicBool::new(true),
        }
    }

    fn integrity(&self) -> CaptureIntegrityState {
        self.degradation.integrity()
    }
}

#[derive(Debug)]
struct CaptureState<B: CaptureAuthorityBundle> {
    active: ArcSwap<GenerationCaptureState<B>>,
    lifecycle_transition: std::sync::Mutex<()>,
    writer_lifecycle: AtomicU8,
    records_written: AtomicU64,
    health_sender: mpsc::SyncSender<CaptureHealthEvent>,
    health_receiver: std::sync::Mutex<mpsc::Receiver<CaptureHealthEvent>>,
    dropped_health_events: AtomicU64,
    accounting_invariant_failures: AtomicU64,
    queued_bytes: AtomicUsize,
}

impl<B: CaptureAuthorityBundle> CaptureState<B> {
    fn mark_current_incomplete(&self, reason: CaptureHealthReason) {
        let active = self.active.load_full();
        self.mark_incomplete_for_generation(&active, reason);
    }

    fn mark_incomplete_for_generation(
        &self,
        generation: &Arc<GenerationCaptureState<B>>,
        reason: CaptureHealthReason,
    ) {
        generation.degradation.mark_incomplete();
        generation.accepting.store(false, Ordering::Release);
        if self
            .health_sender
            .try_send(CaptureHealthEvent {
                identity: generation.identity.as_ref().clone(),
                integrity: CaptureIntegrityState::Incomplete,
                reason,
            })
            .is_err()
        {
            saturating_atomic_increment(&self.dropped_health_events);
        }
    }

    fn mark_writer_failed(&self) {
        self.stop_writer(CaptureHealthReason::WriterFailed);
    }

    fn mark_writer_stopped(&self) {
        self.stop_writer(CaptureHealthReason::WriterStopped);
    }

    fn stop_writer(&self, reason: CaptureHealthReason) {
        let _transition = match self.lifecycle_transition.lock() {
            Ok(transition) => transition,
            Err(poisoned) => poisoned.into_inner(),
        };
        self.mark_current_incomplete(reason);
        self.writer_lifecycle
            .store(WRITER_STOPPED, Ordering::Release);
    }

    fn stop_writer_from_publisher(
        &self,
        observed: &Arc<GenerationCaptureState<B>>,
        reason: CaptureHealthReason,
    ) {
        // Publication must remain nonblocking. Publish STOPPED first so a control-thread rotation
        // recheck fails; then degrade both the observed allocation and any successor installed by
        // a rotation that linearized immediately before this store/load pair.
        self.writer_lifecycle
            .store(WRITER_STOPPED, Ordering::Release);
        self.mark_incomplete_for_generation(observed, reason);
        let current = self.active.load_full();
        if !Arc::ptr_eq(observed, &current) {
            self.mark_incomplete_for_generation(&current, reason);
        }
    }

    fn increment_written(&self, current: u64) -> Option<u64> {
        let next = current.checked_add(1)?;
        self.records_written.store(next, Ordering::Release);
        Some(next)
    }

    fn try_reserve_queue_bytes(self: &Arc<Self>, bytes: usize) -> Option<QueueByteReservation<B>> {
        let mut current = self.queued_bytes.load(Ordering::Acquire);
        loop {
            let next = current.checked_add(bytes)?;
            if next > CAPTURE_QUEUE_BYTE_BUDGET {
                return None;
            }
            match self.queued_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_previous) => {
                    return Some(QueueByteReservation {
                        state: Arc::downgrade(self),
                        bytes,
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }

    fn release_queue_bytes_exact(&self, bytes: usize) {
        let mut current = self.queued_bytes.load(Ordering::Acquire);
        loop {
            let Some(next) = current.checked_sub(bytes) else {
                saturating_atomic_increment(&self.accounting_invariant_failures);
                self.mark_current_incomplete(CaptureHealthReason::AccountingInvariant);
                return;
            };
            match self.queued_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_previous) => return,
                Err(observed) => current = observed,
            }
        }
    }
}

/// Why capture health changed outside the event-to-action path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CaptureHealthReason {
    /// The bounded raw-capture queue had no available count or byte capacity.
    Saturated,
    /// The capture receiver was closed.
    Closed,
    /// The supervised storage writer failed.
    WriterFailed,
    /// The writer stopped normally; capture authority still ends with the writer lifetime.
    WriterStopped,
    /// A publisher was used before supervision started or after it stopped.
    WriterUnavailable,
    /// Concrete source-registry admission failed.
    AuthorityRejected,
    /// The nonblocking admission issuer was already in use.
    AuthorityBusy,
    /// Retained-byte accounting overflowed.
    RetainedSizeOverflow,
    /// Writer-thread diagnostic conversion rejected an exact raw frame.
    DiagnosticConversion,
    /// Queue reservation accounting violated its exactly-once invariant.
    AccountingInvariant,
    /// The sole positive capture-allocation supervisor exited or was dropped.
    SupervisorStopped,
    /// Shutdown could not drain to the explicit deadline.
    ShutdownDeadline,
}

/// Bounded control-plane capture-health event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CaptureHealthEvent {
    identity: CaptureAuthorityIdentity,
    integrity: CaptureIntegrityState,
    reason: CaptureHealthReason,
}

impl CaptureHealthEvent {
    /// Returns the exact source/session/generation diagnostic identity.
    pub const fn identity(&self) -> &CaptureAuthorityIdentity {
        &self.identity
    }

    /// Returns the one-way integrity state.
    pub const fn integrity(&self) -> CaptureIntegrityState {
        self.integrity
    }

    /// Returns the failure reason.
    pub const fn reason(&self) -> CaptureHealthReason {
        self.reason
    }
}

/// Atomic identity-and-integrity view from one immutable generation allocation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CaptureHealthSnapshot {
    identity: Arc<CaptureAuthorityIdentity>,
    integrity: CaptureIntegrityState,
}

impl CaptureHealthSnapshot {
    /// Returns exact audit identity.
    pub fn identity(&self) -> &CaptureAuthorityIdentity {
        &self.identity
    }

    /// Returns exact one-way integrity.
    pub const fn integrity(&self) -> CaptureIntegrityState {
        self.integrity
    }
}

/// Immediate raw-capture publication failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CapturePublishError {
    /// Concrete registry authority rejected the frame or generation.
    #[error("capture authority rejected publication: {0}")]
    Authority(#[from] CaptureAuthorityError),
    /// Another publisher currently owns the sole non-clone admission issuer.
    #[error("capture admission authority is busy")]
    AuthorityBusy,
    /// Deep retained-size accounting overflowed.
    #[error("raw capture retained-size accounting overflowed")]
    RetainedSizeOverflow,
    /// The bounded queue is full or its byte budget is exhausted.
    #[error("raw capture queue is saturated")]
    Saturated,
    /// The writer receiver has closed.
    #[error("raw capture writer is closed")]
    Closed,
    /// Publication requires a running supervised writer.
    #[error("raw capture writer is not running")]
    WriterUnavailable,
}

impl From<CapturePublishError> for std::io::Error {
    fn from(error: CapturePublishError) -> Self {
        Self::other(error)
    }
}

/// Cloneable publisher that can only admit frames through its concrete bundle authority.
#[derive(Debug)]
pub struct RawCapturePublisher<B: CaptureAuthorityBundle> {
    sender: mpsc::SyncSender<CaptureMessage<B>>,
    state: Arc<CaptureState<B>>,
}

impl<B: CaptureAuthorityBundle> Clone for RawCapturePublisher<B> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            state: Arc::clone(&self.state),
        }
    }
}

impl<B: CaptureAuthorityBundle> RawCapturePublisher<B> {
    /// Admits one exact frame without waiting for queue or filesystem capacity.
    pub fn try_publish(&self, frame: &B::Frame) -> Result<B::Receipt, CapturePublishError> {
        let active = self.state.active.load_full();
        if self.state.writer_lifecycle.load(Ordering::Acquire) != WRITER_RUNNING
            || !active.accepting.load(Ordering::Acquire)
        {
            self.state
                .mark_incomplete_for_generation(&active, CaptureHealthReason::WriterUnavailable);
            return Err(CapturePublishError::WriterUnavailable);
        }
        {
            let admission = match active.admission.try_lock() {
                Ok(admission) => admission,
                Err(std::sync::TryLockError::WouldBlock) => {
                    self.state.mark_incomplete_for_generation(
                        &active,
                        CaptureHealthReason::AuthorityBusy,
                    );
                    return Err(CapturePublishError::AuthorityBusy);
                }
                Err(std::sync::TryLockError::Poisoned(_poisoned)) => {
                    self.state.mark_incomplete_for_generation(
                        &active,
                        CaptureHealthReason::AuthorityRejected,
                    );
                    return Err(CapturePublishError::Authority(
                        CaptureAuthorityError::GenerationIncomplete,
                    ));
                }
            };
            if let Err(error) = admission.preflight(frame) {
                if error != CaptureAuthorityError::FrameBindingMismatch {
                    self.state.mark_incomplete_for_generation(
                        &active,
                        CaptureHealthReason::AuthorityRejected,
                    );
                }
                return Err(error.into());
            }
        }
        let reserved_bytes = frame
            .retained_bytes()
            .checked_add(std::mem::size_of::<CaptureMessage<B>>())
            .ok_or_else(|| {
                self.state.mark_incomplete_for_generation(
                    &active,
                    CaptureHealthReason::RetainedSizeOverflow,
                );
                CapturePublishError::RetainedSizeOverflow
            })?;
        let reservation = self
            .state
            .try_reserve_queue_bytes(reserved_bytes)
            .ok_or_else(|| {
                self.state
                    .mark_incomplete_for_generation(&active, CaptureHealthReason::Saturated);
                CapturePublishError::Saturated
            })?;
        match self.sender.try_send(CaptureMessage::Record {
            allocation: Arc::clone(&active),
            frame: frame.clone(),
            reservation,
        }) {
            Ok(()) => {}
            Err(mpsc::TrySendError::Full(_message)) => {
                self.state
                    .mark_incomplete_for_generation(&active, CaptureHealthReason::Saturated);
                return Err(CapturePublishError::Saturated);
            }
            Err(mpsc::TrySendError::Disconnected(_message)) => {
                self.state
                    .stop_writer_from_publisher(&active, CaptureHealthReason::Closed);
                return Err(CapturePublishError::Closed);
            }
        }
        let mut admission = match active.admission.try_lock() {
            Ok(admission) => admission,
            Err(std::sync::TryLockError::WouldBlock) => {
                self.state
                    .mark_incomplete_for_generation(&active, CaptureHealthReason::AuthorityBusy);
                return Err(CapturePublishError::AuthorityBusy);
            }
            Err(std::sync::TryLockError::Poisoned(_poisoned)) => {
                self.state.mark_incomplete_for_generation(
                    &active,
                    CaptureHealthReason::AuthorityRejected,
                );
                return Err(CapturePublishError::Authority(
                    CaptureAuthorityError::GenerationIncomplete,
                ));
            }
        };
        let receipt = admission.issue_after_enqueue(frame).map_err(|error| {
            self.state
                .mark_incomplete_for_generation(&active, CaptureHealthReason::AuthorityRejected);
            CapturePublishError::Authority(error)
        })?;
        admission.validate_active(frame).map_err(|error| {
            self.state
                .mark_incomplete_for_generation(&active, CaptureHealthReason::AuthorityRejected);
            CapturePublishError::Authority(error)
        })?;
        let current = self.state.active.load_full();
        if self.state.writer_lifecycle.load(Ordering::Acquire) != WRITER_RUNNING
            || !active.accepting.load(Ordering::Acquire)
            || !Arc::ptr_eq(&active, &current)
        {
            self.state
                .mark_incomplete_for_generation(&active, CaptureHealthReason::WriterUnavailable);
            return Err(CapturePublishError::WriterUnavailable);
        }
        Ok(receipt)
    }

    /// Returns the active audit identity.
    pub fn identity(&self) -> Arc<CaptureAuthorityIdentity> {
        Arc::clone(&self.state.active.load_full().identity)
    }

    /// Returns one exact identity-and-integrity snapshot.
    pub fn health_snapshot(&self) -> CaptureHealthSnapshot {
        let active = self.state.active.load_full();
        CaptureHealthSnapshot {
            identity: Arc::clone(&active.identity),
            integrity: active.integrity(),
        }
    }

    /// Returns current capture integrity.
    pub fn integrity(&self) -> CaptureIntegrityState {
        self.health_snapshot().integrity()
    }

    /// Polls one bounded health event without blocking.
    pub fn try_next_health(&self) -> Option<CaptureHealthEvent> {
        let receiver = match self.state.health_receiver.lock() {
            Ok(receiver) => receiver,
            Err(poisoned) => poisoned.into_inner(),
        };
        receiver.try_recv().ok()
    }

    /// Returns bounded health events dropped due to control-plane saturation.
    pub fn dropped_health_events(&self) -> u64 {
        self.state.dropped_health_events.load(Ordering::Acquire)
    }

    /// Returns aggregate retained bytes awaiting writer processing.
    pub fn queued_bytes(&self) -> usize {
        self.state.queued_bytes.load(Ordering::Acquire)
    }

    /// Returns exactly-once queue-reservation invariant failures.
    pub fn accounting_invariant_failures(&self) -> u64 {
        self.state
            .accounting_invariant_failures
            .load(Ordering::Acquire)
    }
}

/// Receiver owned by the supervised raw-capture writer.
#[derive(Debug)]
pub struct RawCaptureWriter<B: CaptureAuthorityBundle> {
    receiver: Arc<std::sync::Mutex<mpsc::Receiver<CaptureMessage<B>>>>,
    sender: Option<mpsc::SyncSender<CaptureMessage<B>>>,
    state: Arc<CaptureState<B>>,
}

impl<B: CaptureAuthorityBundle> Drop for RawCaptureWriter<B> {
    fn drop(&mut self) {
        let receiver = match self.receiver.try_lock() {
            Ok(receiver) => Some(receiver),
            Err(std::sync::TryLockError::WouldBlock) => None,
            Err(std::sync::TryLockError::Poisoned(poisoned)) => Some(poisoned.into_inner()),
        };
        if let Some(receiver) = receiver {
            while receiver.try_recv().is_ok() {}
        }
        if self.state.writer_lifecycle.load(Ordering::Acquire) != WRITER_STOPPED {
            self.state.stop_writer(CaptureHealthReason::Closed);
        }
    }
}

/// Creates a bounded channel by consuming one registry-issued whole authority bundle.
pub fn raw_capture_channel<B: CaptureAuthorityBundle>(
    capacity: NonZeroUsize,
    bundle: B,
) -> (
    RawCapturePublisher<B>,
    RawCaptureControl<B>,
    RawCaptureWriter<B>,
) {
    let identity = bundle.identity();
    let (initializer, admission, degradation) = bundle.into_parts();
    let (sender, receiver) = mpsc::sync_channel(capacity.get());
    let receiver = Arc::new(std::sync::Mutex::new(receiver));
    let (health_sender, health_receiver) = mpsc::sync_channel(HEALTH_EVENT_CAPACITY);
    let state = Arc::new(CaptureState {
        active: ArcSwap::from_pointee(GenerationCaptureState::new(
            identity,
            admission,
            degradation,
        )),
        lifecycle_transition: std::sync::Mutex::new(()),
        writer_lifecycle: AtomicU8::new(WRITER_NOT_STARTED),
        records_written: AtomicU64::new(0),
        health_sender,
        health_receiver: std::sync::Mutex::new(health_receiver),
        dropped_health_events: AtomicU64::new(0),
        accounting_invariant_failures: AtomicU64::new(0),
        queued_bytes: AtomicUsize::new(0),
    });
    (
        RawCapturePublisher {
            sender: sender.clone(),
            state: Arc::clone(&state),
        },
        RawCaptureControl {
            state: Arc::clone(&state),
            initializer: Some(initializer),
        },
        RawCaptureWriter {
            receiver,
            sender: Some(sender),
            state,
        },
    )
}

mod control;
mod diagnostic;
mod policy;
mod writer;

pub use control::{CaptureGenerationError, RawCaptureControl};
pub use diagnostic::{
    DiagnosticCaptureBundle, DiagnosticCaptureError, DiagnosticCaptureFrame,
    DiagnosticCaptureReceipt,
};
pub use policy::{CaptureWriterPolicy, CaptureWriterPolicyError};
pub use writer::{
    CaptureDestination, CaptureDestinationError, CaptureShutdown, CaptureSink, CaptureSinkError,
    CaptureStorageErrorClass, CaptureWriterHandle, CaptureWriterOutcome, CaptureWriterSpawnError,
    MemoryCaptureSink, spawn_capture_writer,
};

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::saturating_atomic_increment;

    #[test]
    fn diagnostic_counter_increment_saturates_at_the_numeric_limit() {
        let counter = AtomicU64::new(u64::MAX);
        saturating_atomic_increment(&counter);
        assert_eq!(counter.load(Ordering::Acquire), u64::MAX);
    }
}
