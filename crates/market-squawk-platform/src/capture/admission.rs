//! Nonblocking capture admission and receipt issuance.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use market_squawk_domain::{
    CaptureAdmission, CaptureAuthorityBundle, CaptureAuthorityError, CaptureIntegrityState,
    CaptureResidentGenerationLease, CaptureRetainedComponent, CaptureRetainedReceipt,
    CaptureRetainedSizeError, MAX_LIVE_CAPTURE_PAYLOAD_BYTES, RawCaptureFrameView,
};

use super::queue::{TryCloneError, TrySendError};
use super::transport::{CaptureQueueSender, CaptureQueueTransport, FixedRingTransport};
use super::{
    CaptureHealthEvent, CaptureHealthReason, CaptureHealthSnapshot, CaptureIdentitySnapshot,
    CaptureMessage, CapturePublishError, CapturePublisherCloneError, CaptureState,
    RecordReservationQuote, WRITER_RUNNING,
};

#[derive(Debug)]
pub(super) struct CapturePublisherCore<B: CaptureAuthorityBundle, T: CaptureQueueTransport> {
    sender: T::Sender<CaptureMessage<B>>,
    state: Arc<CaptureState<B>>,
}

impl<B: CaptureAuthorityBundle, T: CaptureQueueTransport> CapturePublisherCore<B, T> {
    pub(super) fn new(sender: T::Sender<CaptureMessage<B>>, state: Arc<CaptureState<B>>) -> Self {
        Self { sender, state }
    }

    /// Creates another publisher without blocking on queue synchronization.
    ///
    /// # Errors
    ///
    /// Returns a typed failure when the queue is closed or its exact sender count cannot be
    /// incremented.
    pub(super) fn try_clone(&self) -> Result<Self, CapturePublisherCloneError> {
        self.try_clone_with(|sender| sender.try_clone())
    }

    fn try_clone_with(
        &self,
        clone_sender: impl FnOnce(
            &T::Sender<CaptureMessage<B>>,
        ) -> Result<T::Sender<CaptureMessage<B>>, TryCloneError>,
    ) -> Result<Self, CapturePublisherCloneError> {
        let active = self.state.active.load_full();
        let sender = clone_sender(&self.sender).map_err(|error| {
            let (public, health) = match error {
                TryCloneError::Closed => (
                    CapturePublisherCloneError::QueueClosed,
                    CaptureHealthReason::Closed,
                ),
                TryCloneError::CountOverflow => (
                    CapturePublisherCloneError::SenderCountOverflow,
                    CaptureHealthReason::SenderCountOverflow,
                ),
            };
            self.state.mark_incomplete_for_generation(&active, health);
            public
        })?;
        Ok(Self {
            sender,
            state: Arc::clone(&self.state),
        })
    }

    #[cfg(feature = "capture-benchmark")]
    pub(super) fn into_benchmark_sender(self) -> T::Sender<CaptureMessage<B>> {
        self.sender
    }

    #[cfg(all(test, not(loom), feature = "capture-benchmark"))]
    pub(super) fn benchmark_state_for_test(&self) -> Arc<CaptureState<B>> {
        Arc::clone(&self.state)
    }

    /// Admits one exact frame without waiting for queue or filesystem capacity.
    pub(super) fn try_publish(&self, frame: &B::Frame) -> Result<B::Receipt, CapturePublishError> {
        let active = self.state.active.load_full();
        if self.state.writer_lifecycle.load(Ordering::Acquire) != WRITER_RUNNING
            || !active.accepting.load(Ordering::Acquire)
        {
            self.state
                .mark_incomplete_for_generation(&active, CaptureHealthReason::WriterUnavailable);
            return Err(CapturePublishError::WriterUnavailable);
        }
        // Clone the caller-owned frame exactly once, then use this same immutable allocation for
        // every validation, reservation, enqueue, and receipt operation. `Clone` is an untrusted
        // implementation boundary; validating the caller's value and enqueueing a different clone
        // would permit the queued frame to escape the checked authority graph.
        let queued_frame = Arc::new(frame.clone());
        let frame = queued_frame.as_ref();
        let advertised_payload = frame.payload();
        let captured_payload = frame.capture_payload().as_bytes();
        let is_exact_view = advertised_payload.len() == captured_payload.len()
            && (advertised_payload.is_empty()
                || std::ptr::eq(advertised_payload.as_ptr(), captured_payload.as_ptr()));
        if !is_exact_view {
            self.state.mark_incomplete_for_generation(
                &active,
                CaptureHealthReason::InvalidAuthorityGraph,
            );
            return Err(CapturePublishError::InvalidPayloadView);
        }
        if frame.capture_payload().as_bytes().len() > MAX_LIVE_CAPTURE_PAYLOAD_BYTES {
            self.state.mark_incomplete_for_generation(
                &active,
                CaptureHealthReason::InvalidAuthorityGraph,
            );
            return Err(CapturePublishError::PayloadTooLarge {
                actual: frame.capture_payload().as_bytes().len(),
                maximum: MAX_LIVE_CAPTURE_PAYLOAD_BYTES,
            });
        }
        let footprint = frame.checked_retained_footprint().map_err(|error| {
            self.state.mark_incomplete_for_generation(
                &active,
                CaptureHealthReason::InvalidAuthorityGraph,
            );
            CapturePublishError::RetainedSize(error)
        })?;
        if footprint.inline_slot_funded_bytes() != std::mem::size_of_val(frame) {
            self.state.mark_incomplete_for_generation(
                &active,
                CaptureHealthReason::InvalidAuthorityGraph,
            );
            return Err(CapturePublishError::RetainedSizeUnderreported);
        }
        let payload_retained_bytes = frame
            .capture_payload()
            .checked_retained_allocation_bytes()
            .map_err(|error| {
                self.state.mark_incomplete_for_generation(
                    &active,
                    CaptureHealthReason::InvalidAuthorityGraph,
                );
                CapturePublishError::RetainedSize(error)
            })?;
        if footprint.unique_frame_dynamic_bytes() < payload_retained_bytes {
            self.state.mark_incomplete_for_generation(
                &active,
                CaptureHealthReason::InvalidAuthorityGraph,
            );
            return Err(CapturePublishError::RetainedSizeUnderreported);
        }
        enum PreflightFailure {
            Authority(CaptureAuthorityError),
            Retained(CaptureRetainedSizeError),
            ResidentMismatch,
        }
        let admission = match active.admission.try_lock() {
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
        let preflight = match admission.preflight(frame) {
            Ok(()) => match admission.checked_resident_shared_frame_bytes(frame) {
                Ok(resident) if resident == footprint.resident_shared_bytes() => Ok(()),
                Ok(_resident) => Err(PreflightFailure::ResidentMismatch),
                Err(error) => Err(PreflightFailure::Retained(error)),
            },
            Err(error) => Err(PreflightFailure::Authority(error)),
        };
        drop(admission);
        if let Err(error) = preflight {
            return match error {
                PreflightFailure::Authority(error) => {
                    if error != CaptureAuthorityError::FrameBindingMismatch {
                        self.state.mark_incomplete_for_generation(
                            &active,
                            CaptureHealthReason::AuthorityRejected,
                        );
                    }
                    Err(CapturePublishError::Authority(error))
                }
                PreflightFailure::Retained(error) => {
                    self.state.mark_incomplete_for_generation(
                        &active,
                        CaptureHealthReason::InvalidAuthorityGraph,
                    );
                    Err(CapturePublishError::RetainedSize(error))
                }
                PreflightFailure::ResidentMismatch => {
                    self.state.mark_incomplete_for_generation(
                        &active,
                        CaptureHealthReason::InvalidAuthorityGraph,
                    );
                    Err(CapturePublishError::RetainedSize(
                        CaptureRetainedSizeError::InvalidAuthorityGraph {
                            component: CaptureRetainedComponent::Frame,
                        },
                    ))
                }
            };
        }
        let complete_frame = footprint.checked_complete_bytes().map_err(|error| {
            self.state
                .mark_incomplete_for_generation(&active, CaptureHealthReason::RetainedSizeOverflow);
            CapturePublishError::RetainedSize(error)
        })?;
        let reservation_quote = RecordReservationQuote::try_for_frame::<B>(frame, complete_frame)
            .map_err(|error| {
            self.state
                .mark_incomplete_for_generation(&active, CaptureHealthReason::RetainedSizeOverflow);
            CapturePublishError::RetainedSize(error)
        })?;
        let reserved_bytes = reservation_quote.checked_total().map_err(|_error| {
            self.state
                .mark_incomplete_for_generation(&active, CaptureHealthReason::RetainedSizeOverflow);
            CapturePublishError::RetainedSizeOverflow
        })?;
        let reservation = self
            .state
            .try_reserve_queue_bytes(reserved_bytes)
            .map_err(|error| match error {
                super::accounting::CaptureAccountingError::BudgetExceeded { required, ceiling } => {
                    self.state.mark_incomplete_for_generation(
                        &active,
                        CaptureHealthReason::CaptureMemoryBudgetExceeded,
                    );
                    CapturePublishError::CaptureMemoryBudgetExceeded { required, ceiling }
                }
                super::accounting::CaptureAccountingError::ArithmeticOverflow
                | super::accounting::CaptureAccountingError::TransitionOverflow
                | super::accounting::CaptureAccountingError::EpochOverflow
                | super::accounting::CaptureAccountingError::InvariantViolated => {
                    self.state.mark_incomplete_for_generation(
                        &active,
                        CaptureHealthReason::AccountingInvariant,
                    );
                    CapturePublishError::AccountingInvariant
                }
            })?;
        match self.sender.try_send(CaptureMessage::Record {
            allocation: Arc::clone(&active),
            frame: Arc::clone(&queued_frame),
            reservation,
        }) {
            Ok(()) => {}
            Err(TrySendError::Full(_message)) => {
                self.state
                    .mark_incomplete_for_generation(&active, CaptureHealthReason::QueueFull);
                return Err(CapturePublishError::QueueFull);
            }
            Err(TrySendError::Closed(_message)) => {
                self.state
                    .stop_writer_from_publisher(&active, CaptureHealthReason::Closed);
                return Err(CapturePublishError::QueueClosed);
            }
            Err(TrySendError::Poisoned(_message)) => {
                self.state
                    .stop_writer_from_publisher(&active, CaptureHealthReason::QueuePoisoned);
                return Err(CapturePublishError::QueuePoisoned);
            }
            Err(TrySendError::Invariant(_message)) => {
                self.state
                    .stop_writer_from_publisher(&active, CaptureHealthReason::QueueInvariant);
                return Err(CapturePublishError::QueueInvariant);
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
        let resident = CaptureResidentGenerationLease::new(Arc::clone(&active.identity));
        let receipt = admission.issue_after_enqueue(frame, resident);
        let issued = match receipt {
            Ok(receipt) => {
                let validation = admission.validate_active(frame);
                Ok((receipt, validation))
            }
            Err(error) => Err(error),
        };
        drop(admission);
        let (receipt, validation) = match issued {
            Ok(issued) => issued,
            Err(error) => {
                self.state.mark_incomplete_for_generation(
                    &active,
                    CaptureHealthReason::AuthorityRejected,
                );
                return Err(CapturePublishError::Authority(error));
            }
        };
        if let Err(error) = validation {
            drop(receipt);
            self.state
                .mark_incomplete_for_generation(&active, CaptureHealthReason::AuthorityRejected);
            return Err(CapturePublishError::Authority(error));
        }
        if !receipt
            .resident_generation_lease()
            .shares_allocation_with(&active.identity)
            || receipt
                .checked_additional_dynamic_retained_bytes()
                .map_err(|error| {
                    self.state.mark_incomplete_for_generation(
                        &active,
                        CaptureHealthReason::InvalidAuthorityGraph,
                    );
                    CapturePublishError::RetainedSize(error)
                })?
                != 0
        {
            self.state.mark_incomplete_for_generation(
                &active,
                CaptureHealthReason::InvalidAuthorityGraph,
            );
            return Err(CapturePublishError::RetainedSize(
                CaptureRetainedSizeError::InvalidAuthorityGraph {
                    component: CaptureRetainedComponent::CaptureLease,
                },
            ));
        }
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
    pub(super) fn identity(&self) -> CaptureHealthSnapshot {
        self.health_snapshot()
    }

    /// Returns one exact identity-and-integrity snapshot.
    pub(super) fn health_snapshot(&self) -> CaptureHealthSnapshot {
        let active = self.state.active.load_full();
        CaptureHealthSnapshot {
            identity: CaptureIdentitySnapshot(Arc::clone(&active.identity)),
            integrity: active.integrity(),
        }
    }

    /// Returns current capture integrity.
    pub(super) fn integrity(&self) -> CaptureIntegrityState {
        self.health_snapshot().integrity()
    }

    /// Polls one bounded health event without blocking.
    pub(super) fn try_next_health(&self) -> Option<CaptureHealthEvent> {
        self.state.health_receiver.try_recv().ok()
    }

    /// Returns bounded health events dropped due to control-plane saturation.
    pub(super) fn dropped_health_events(&self) -> u64 {
        self.state.dropped_health_events.load(Ordering::Acquire)
    }

    /// Returns the exact configured logical fixed-ring depth.
    pub(super) fn fixed_queue_capacity(&self) -> usize {
        self.state.queue_storage.logical_capacity()
    }

    /// Returns the allocator-observed retained slot count backing the fixed ring.
    pub(super) fn fixed_queue_observed_slot_capacity(&self) -> usize {
        // This method is only exposed through `RawCapturePublisher<FixedRingTransport>`.
        self.state
            .queue_storage
            .observed_slot_capacity()
            .unwrap_or(0)
    }

    /// Returns the retained bytes of the allocator-observed fixed slot backing allocation.
    pub(super) fn fixed_queue_slot_bytes(&self) -> usize {
        // This method is only exposed through `RawCapturePublisher<FixedRingTransport>`.
        self.state.queue_storage.retained_slot_bytes().unwrap_or(0)
    }

    /// Returns the allocator-observed retained bytes for the complete record queue allocation.
    pub(super) fn fixed_queue_retained_bytes(&self) -> usize {
        // This method is only exposed through `RawCapturePublisher<FixedRingTransport>`.
        self.state.queue_storage.retained_queue_bytes().unwrap_or(0)
    }

    #[cfg(feature = "capture-benchmark")]
    pub(super) fn benchmark_queue_private_storage_bytes(&self) -> Option<usize> {
        self.state.queue_storage.retained_queue_bytes()
    }

    /// Returns the exact configured logical health-ring depth.
    pub(super) fn fixed_health_queue_capacity(&self) -> usize {
        self.state.health_fixed_storage.logical_capacity()
    }

    /// Attempts one coherent bounded unified-accounting snapshot.
    ///
    /// # Errors
    ///
    /// Returns contention only after exhausting `max_attempts`, or a durable typed terminal
    /// accounting state. No independent component read is exposed.
    pub(super) fn try_accounting_snapshot(
        &self,
        max_attempts: std::num::NonZeroUsize,
    ) -> Result<
        super::accounting::CaptureAccountingSnapshot,
        super::accounting::CaptureAccountingSnapshotError,
    > {
        self.state.accounting.try_snapshot(max_attempts)
    }
}

/// Publisher that can only admit frames through its concrete bundle authority.
#[derive(Debug)]
pub struct RawCapturePublisher<B: CaptureAuthorityBundle> {
    core: CapturePublisherCore<B, FixedRingTransport>,
}

impl<B: CaptureAuthorityBundle> RawCapturePublisher<B> {
    pub(super) const fn from_core(core: CapturePublisherCore<B, FixedRingTransport>) -> Self {
        Self { core }
    }

    /// Creates another publisher without blocking on queue synchronization.
    ///
    /// # Errors
    ///
    /// Returns a typed failure when the queue is closed or its exact sender count cannot be
    /// incremented.
    pub fn try_clone(&self) -> Result<Self, CapturePublisherCloneError> {
        self.core.try_clone().map(Self::from_core)
    }

    /// Creates another publisher after pausing an admitted clone at a deterministic test barrier.
    ///
    /// This boundary exists only in debug builds with the internal `capture-test` feature. It
    /// exposes no queue state and does not change production clone behavior.
    #[cfg(all(feature = "capture-test", debug_assertions, not(loom)))]
    #[doc(hidden)]
    pub fn try_clone_after_registration_paused_for_test(
        &self,
        entered: &std::sync::Barrier,
        release: &std::sync::Barrier,
    ) -> Result<Self, CapturePublisherCloneError> {
        self.core
            .try_clone_with(|sender| {
                sender.try_clone_after_registration_paused_for_test(entered, release)
            })
            .map(Self::from_core)
    }

    /// Admits one exact frame without waiting for queue or filesystem capacity.
    pub fn try_publish(&self, frame: &B::Frame) -> Result<B::Receipt, CapturePublishError> {
        self.core.try_publish(frame)
    }

    /// Returns the active audit identity.
    pub fn identity(&self) -> CaptureHealthSnapshot {
        self.core.identity()
    }

    /// Returns one exact identity-and-integrity snapshot.
    pub fn health_snapshot(&self) -> CaptureHealthSnapshot {
        self.core.health_snapshot()
    }

    /// Returns current capture integrity.
    pub fn integrity(&self) -> CaptureIntegrityState {
        self.core.integrity()
    }

    /// Polls one bounded health event without blocking.
    pub fn try_next_health(&self) -> Option<CaptureHealthEvent> {
        self.core.try_next_health()
    }

    /// Returns bounded health events dropped due to control-plane saturation.
    pub fn dropped_health_events(&self) -> u64 {
        self.core.dropped_health_events()
    }

    /// Returns the exact configured logical fixed-ring depth.
    pub fn fixed_queue_capacity(&self) -> usize {
        self.core.fixed_queue_capacity()
    }

    /// Returns the allocator-observed retained slot count backing the fixed ring.
    pub fn fixed_queue_observed_slot_capacity(&self) -> usize {
        self.core.fixed_queue_observed_slot_capacity()
    }

    /// Returns the retained bytes of the allocator-observed fixed slot backing allocation.
    pub fn fixed_queue_slot_bytes(&self) -> usize {
        self.core.fixed_queue_slot_bytes()
    }

    /// Returns the allocator-observed retained bytes for the complete record queue allocation.
    pub fn fixed_queue_retained_bytes(&self) -> usize {
        self.core.fixed_queue_retained_bytes()
    }

    /// Returns the exact configured logical health-ring depth.
    pub fn fixed_health_queue_capacity(&self) -> usize {
        self.core.fixed_health_queue_capacity()
    }

    /// Attempts one coherent bounded unified-accounting snapshot.
    ///
    /// # Errors
    ///
    /// Returns contention only after exhausting `max_attempts`, or a durable typed terminal
    /// accounting state. No independent component read is exposed.
    pub fn try_accounting_snapshot(
        &self,
        max_attempts: std::num::NonZeroUsize,
    ) -> Result<
        super::accounting::CaptureAccountingSnapshot,
        super::accounting::CaptureAccountingSnapshotError,
    > {
        self.core.try_accounting_snapshot(max_attempts)
    }
}
