//! Non-blocking generic raw-capture publication and supervised storage.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

use arc_swap::ArcSwap;
use market_squawk_domain::{
    CaptureAuthorityBundle, CaptureDegradation, CaptureIntegrityState, CaptureRetainedComponent,
    CaptureRetainedSizeError, RawCaptureFrameView, checked_arc_str_allocation_bytes,
    checked_arc_value_allocation_bytes,
};

use generation::{
    CaptureIdentitySnapshot, GenerationCaptureState, GenerationPreparationError,
    mark_bundle_incomplete, try_prepare_generation,
};
const WRITER_NOT_STARTED: u8 = 0;
const WRITER_RUNNING: u8 = 1;
const WRITER_STOPPED: u8 = 2;
const HEALTH_EVENT_CAPACITY: usize = 64;

fn saturating_atomic_increment(counter: &AtomicU64) {
    let _previous = counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
        Some(current.saturating_add(1))
    });
}

#[derive(Debug)]
enum CaptureMessage<B: CaptureAuthorityBundle> {
    Record {
        allocation: Arc<GenerationCaptureState<B>>,
        frame: Arc<B::Frame>,
        reservation: QueueByteReservation,
    },
}

/// Exact standard-channel reservation across enqueue, zero-copy conversion, append, and flush.
///
/// The complete queued frame already owns the payload allocation. Writer conversion clones that
/// exact [`market_squawk_domain::CapturePayload`] and proves allocation identity, so only the new
/// source `Arc<str>` and the queued-frame `Arc` header are additional dynamic storage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RecordReservationQuote {
    complete_frame: usize,
    queued_frame_allocation_overhead: usize,
    conversion_source_allocation: usize,
}

impl RecordReservationQuote {
    fn try_for_frame<B: CaptureAuthorityBundle>(
        frame: &B::Frame,
        complete_frame: usize,
    ) -> Result<Self, CaptureRetainedSizeError> {
        let queued_frame_allocation_overhead = checked_arc_value_allocation_bytes::<B::Frame>(0)
            .map_err(|_error| CaptureRetainedSizeError::Overflow {
                component: CaptureRetainedComponent::Frame,
            })?
            .checked_sub(std::mem::size_of::<B::Frame>())
            .ok_or(CaptureRetainedSizeError::InvalidAuthorityGraph {
                component: CaptureRetainedComponent::Frame,
            })?;
        let conversion_source_allocation = checked_arc_str_allocation_bytes(
            frame.source_id().as_str().len(),
        )
        .map_err(|_error| CaptureRetainedSizeError::Overflow {
            component: CaptureRetainedComponent::Frame,
        })?;
        Ok(Self {
            complete_frame,
            queued_frame_allocation_overhead,
            conversion_source_allocation,
        })
    }

    fn checked_total(self) -> Result<usize, CaptureRetainedSizeError> {
        self.complete_frame
            .checked_add(self.queued_frame_allocation_overhead)
            .and_then(|bytes| bytes.checked_add(self.conversion_source_allocation))
            .ok_or(CaptureRetainedSizeError::Overflow {
                component: CaptureRetainedComponent::Frame,
            })
    }
}

#[derive(Debug)]
struct QueueByteReservation {
    _reservation: accounting::CaptureMemoryReservation,
}

#[derive(Debug, Default)]
struct CaptureCompletionAccounting {
    records_written: u64,
    revoked: bool,
    records_written_at_revocation: u64,
    late_records_written: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CaptureCompletionSnapshot {
    records_written: u64,
    records_written_at_revocation: u64,
    late_records_written: u64,
}

impl CaptureCompletionAccounting {
    fn record_completed_append(&mut self) -> Option<u64> {
        let next = self.records_written.checked_add(1)?;
        let next_late = if self.revoked {
            Some(self.late_records_written.checked_add(1)?)
        } else {
            None
        };
        self.records_written = next;
        if let Some(next_late) = next_late {
            self.late_records_written = next_late;
        }
        Some(next)
    }

    fn revoke(&mut self) -> CaptureCompletionSnapshot {
        if !self.revoked {
            self.revoked = true;
            self.records_written_at_revocation = self.records_written;
        }
        self.snapshot()
    }

    const fn snapshot(&self) -> CaptureCompletionSnapshot {
        CaptureCompletionSnapshot {
            records_written: self.records_written,
            records_written_at_revocation: self.records_written_at_revocation,
            late_records_written: self.late_records_written,
        }
    }
}

#[derive(Debug)]
struct CaptureState<B: CaptureAuthorityBundle> {
    process: writer::CaptureProcessInfrastructure,
    active: ArcSwap<GenerationCaptureState<B>>,
    lifecycle_transition: std::sync::Mutex<()>,
    writer_lifecycle: AtomicU8,
    completion_accounting: std::sync::Mutex<CaptureCompletionAccounting>,
    health_sender: queue::FixedSender<CaptureHealthEvent>,
    health_receiver: queue::FixedReceiver<CaptureHealthEvent>,
    dropped_health_events: AtomicU64,
    accounting: Arc<accounting::CaptureMemoryAccounting>,
    _fixed_infrastructure: accounting::CaptureMemoryReservation,
    queue_storage: transport::QueueStorageReceipt,
    health_fixed_storage: queue::FixedStorageReceipt,
    writer_lifecycle_core: Arc<writer::WriterLifecycleCore>,
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
                identity: CaptureIdentitySnapshot(Arc::clone(&generation.identity)),
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

    fn record_completed_append(&self) -> Option<u64> {
        let mut accounting = match self.completion_accounting.lock() {
            Ok(accounting) => accounting,
            Err(poisoned) => poisoned.into_inner(),
        };
        accounting.record_completed_append()
    }

    fn revoke_writer_for_shutdown(&self, reason: CaptureHealthReason) -> CaptureCompletionSnapshot {
        let mut accounting = match self.completion_accounting.lock() {
            Ok(accounting) => accounting,
            Err(poisoned) => poisoned.into_inner(),
        };
        let _transition = match self.lifecycle_transition.lock() {
            Ok(transition) => transition,
            Err(poisoned) => poisoned.into_inner(),
        };
        self.mark_current_incomplete(reason);
        self.writer_lifecycle
            .store(WRITER_STOPPED, Ordering::Release);
        accounting.revoke()
    }

    fn completion_snapshot(&self) -> CaptureCompletionSnapshot {
        let accounting = match self.completion_accounting.lock() {
            Ok(accounting) => accounting,
            Err(poisoned) => poisoned.into_inner(),
        };
        accounting.snapshot()
    }

    fn try_reserve_queue_bytes(
        &self,
        bytes: usize,
    ) -> Result<QueueByteReservation, accounting::CaptureAccountingError> {
        self.accounting
            .try_reserve(accounting::AccountingComponent::Record, bytes)
            .map(|reservation| QueueByteReservation {
                _reservation: reservation,
            })
    }
}

#[derive(Debug)]
struct CaptureWriterCore<B: CaptureAuthorityBundle, T: transport::CaptureQueueTransport> {
    receiver: Option<T::Receiver<CaptureMessage<B>>>,
    queue_control: T::Control<CaptureMessage<B>>,
    state: Arc<CaptureState<B>>,
}

/// Receiver owned by the supervised raw-capture writer.
#[derive(Debug)]
pub struct RawCaptureWriter<B: CaptureAuthorityBundle> {
    core: CaptureWriterCore<B, transport::FixedRingTransport>,
}

/// The publisher, control authority, and sole writer receiver created for one capture channel.
pub type RawCaptureChannel<B> = (
    RawCapturePublisher<B>,
    RawCaptureControl<B>,
    RawCaptureWriter<B>,
);

type CaptureChannelCoreParts<B, T> = (
    admission::CapturePublisherCore<B, T>,
    RawCaptureControl<B>,
    CaptureWriterCore<B, T>,
);

impl<B: CaptureAuthorityBundle, T: transport::CaptureQueueTransport> Drop
    for CaptureWriterCore<B, T>
{
    fn drop(&mut self) {
        if T::close_and_drain(&self.queue_control, self.receiver.as_ref()).is_err() {
            self.state
                .mark_current_incomplete(CaptureHealthReason::QueuePoisoned);
        }
        self.receiver.take();
        if self.state.writer_lifecycle.load(Ordering::Acquire) != WRITER_STOPPED {
            self.state.stop_writer(CaptureHealthReason::Closed);
        }
    }
}

fn fixed_storage_error(
    error: accounting::CaptureAccountingError,
    ceiling: NonZeroUsize,
) -> CaptureChannelError {
    match error {
        accounting::CaptureAccountingError::BudgetExceeded { required, ceiling } => {
            CaptureChannelError::FixedStorageBudgetExceeded { required, ceiling }
        }
        accounting::CaptureAccountingError::ArithmeticOverflow
        | accounting::CaptureAccountingError::TransitionOverflow
        | accounting::CaptureAccountingError::EpochOverflow
        | accounting::CaptureAccountingError::InvariantViolated => {
            CaptureChannelError::FixedStorageBudgetExceeded {
                required: usize::MAX,
                ceiling: ceiling.get(),
            }
        }
    }
}

/// Creates a bounded channel by consuming one registry-issued whole authority bundle.
///
/// # Errors
///
/// Returns a typed error before publishing any handle when generation preparation, dominant ring
/// allocation, fixed-storage accounting, or the configured unified memory ceiling rejects the
/// complete initial channel graph.
pub fn raw_capture_channel<B: CaptureAuthorityBundle>(
    process: &CaptureProcessInfrastructure,
    limits: CaptureChannelLimits,
    bundle: B,
) -> Result<RawCaptureChannel<B>, CaptureChannelError> {
    let (publisher, control, writer) =
        capture_channel_core::<B, transport::FixedRingTransport>(process, limits, bundle)?;
    Ok((
        RawCapturePublisher::from_core(publisher),
        control,
        RawCaptureWriter { core: writer },
    ))
}

fn capture_channel_core<B: CaptureAuthorityBundle, T: transport::CaptureQueueTransport>(
    process: &CaptureProcessInfrastructure,
    limits: CaptureChannelLimits,
    bundle: B,
) -> Result<CaptureChannelCoreParts<B, T>, CaptureChannelError> {
    let capacity = limits.capture_queue_capacity();
    let ceiling = limits.capture_memory_ceiling_bytes();
    let (sender, receiver, queue_control, fixed_storage) = match T::try_new(capacity) {
        Ok(queue) => queue,
        Err(error) => {
            mark_bundle_incomplete(bundle);
            return Err(match error {
                queue::QueueConstructionError::AllocationFailed => {
                    CaptureChannelError::QueueAllocationFailed {
                        queue: CaptureQueueKind::Record,
                        requested_slots: capacity,
                    }
                }
                queue::QueueConstructionError::ArithmeticOverflow => {
                    CaptureChannelError::FixedStorageBudgetExceeded {
                        required: usize::MAX,
                        ceiling: ceiling.get(),
                    }
                }
            });
        }
    };
    let health_capacity = NonZeroUsize::new(HEALTH_EVENT_CAPACITY).unwrap_or(NonZeroUsize::MIN);
    let (health_sender, health_receiver, _health_control, health_fixed_storage) =
        match queue::FixedQueue::try_new(health_capacity) {
            Ok(queue) => queue,
            Err(error) => {
                mark_bundle_incomplete(bundle);
                return Err(match error {
                    queue::QueueConstructionError::AllocationFailed => {
                        CaptureChannelError::QueueAllocationFailed {
                            queue: CaptureQueueKind::Health,
                            requested_slots: health_capacity,
                        }
                    }
                    queue::QueueConstructionError::ArithmeticOverflow => {
                        CaptureChannelError::FixedStorageBudgetExceeded {
                            required: usize::MAX,
                            ceiling: ceiling.get(),
                        }
                    }
                });
            }
        };
    let accounting_core_base_bytes =
        match checked_arc_value_allocation_bytes::<accounting::CaptureMemoryAccounting>(0) {
            Ok(bytes) => bytes,
            Err(_error) => {
                mark_bundle_incomplete(bundle);
                return Err(CaptureChannelError::FixedStorageBudgetExceeded {
                    required: usize::MAX,
                    ceiling: ceiling.get(),
                });
            }
        };
    let capture_state_bytes = match checked_arc_value_allocation_bytes::<CaptureState<B>>(0) {
        Ok(bytes) => bytes,
        Err(_error) => {
            mark_bundle_incomplete(bundle);
            return Err(CaptureChannelError::FixedStorageBudgetExceeded {
                required: usize::MAX,
                ceiling: ceiling.get(),
            });
        }
    };
    let writer_lifecycle_core_bytes =
        match checked_arc_value_allocation_bytes::<writer::WriterLifecycleCore>(0) {
            Ok(bytes) => bytes,
            Err(_error) => {
                mark_bundle_incomplete(bundle);
                return Err(CaptureChannelError::FixedStorageBudgetExceeded {
                    required: usize::MAX,
                    ceiling: ceiling.get(),
                });
            }
        };
    // Production FixedQueue infrastructure contributes exact allocator-observed bytes. The
    // benchmark-only standard reference deliberately contributes no byte value because stable
    // `sync_channel` does not expose one; its evidence schema records `not_measured` and forbids
    // treating this accounting snapshot as a whole-graph memory comparison.
    let known_record_queue_fixed_bytes = fixed_storage.retained_queue_bytes().unwrap_or(0);
    let channel_state_fixed_bytes = match known_record_queue_fixed_bytes
        .checked_add(health_fixed_storage.retained_queue_bytes())
        .and_then(|bytes| bytes.checked_add(capture_state_bytes))
        .and_then(|bytes| bytes.checked_add(writer_lifecycle_core_bytes))
    {
        Some(bytes) => bytes,
        None => {
            mark_bundle_incomplete(bundle);
            return Err(CaptureChannelError::FixedStorageBudgetExceeded {
                required: usize::MAX,
                ceiling: ceiling.get(),
            });
        }
    };
    let accounting =
        match accounting::CaptureMemoryAccounting::try_new(accounting_core_base_bytes, ceiling) {
            Ok(accounting) => Arc::new(accounting),
            Err(error) => {
                mark_bundle_incomplete(bundle);
                return Err(fixed_storage_error(error, ceiling));
            }
        };
    let fixed_infrastructure = match accounting.try_reserve(
        accounting::AccountingComponent::Fixed,
        channel_state_fixed_bytes,
    ) {
        Ok(reservation) => reservation,
        Err(error) => {
            mark_bundle_incomplete(bundle);
            return Err(fixed_storage_error(error, ceiling));
        }
    };
    let (initializer, active) =
        try_prepare_generation(bundle, &accounting).map_err(|error| match error {
            GenerationPreparationError::Retained(error) => {
                CaptureChannelError::GenerationPreparation(error)
            }
            GenerationPreparationError::Accounting(error) => fixed_storage_error(error, ceiling),
        })?;
    let state = Arc::new(CaptureState {
        process: *process,
        active: ArcSwap::from(active),
        lifecycle_transition: std::sync::Mutex::new(()),
        writer_lifecycle: AtomicU8::new(WRITER_NOT_STARTED),
        completion_accounting: std::sync::Mutex::new(CaptureCompletionAccounting::default()),
        health_sender,
        health_receiver,
        dropped_health_events: AtomicU64::new(0),
        accounting,
        _fixed_infrastructure: fixed_infrastructure,
        queue_storage: fixed_storage,
        health_fixed_storage,
        writer_lifecycle_core: Arc::new(writer::WriterLifecycleCore::new()),
    });
    Ok((
        admission::CapturePublisherCore::new(sender, Arc::clone(&state)),
        RawCaptureControl {
            state: Arc::clone(&state),
            initializer: Some(initializer),
        },
        CaptureWriterCore {
            receiver: Some(receiver),
            queue_control,
            state,
        },
    ))
}

#[cfg(all(
    feature = "capture-benchmark",
    not(test),
    capture_bench_backend = "standard"
))]
type SelectedBenchmarkTransport = transport::StandardReferenceTransport;
#[cfg(all(
    feature = "capture-benchmark",
    any(test, capture_bench_backend = "candidate")
))]
type SelectedBenchmarkTransport = transport::FixedRingTransport;

#[cfg(feature = "capture-benchmark")]
type BenchmarkCapturePublisher<B> = admission::CapturePublisherCore<B, SelectedBenchmarkTransport>;
#[cfg(feature = "capture-benchmark")]
type BenchmarkCaptureWriter<B> = CaptureWriterCore<B, SelectedBenchmarkTransport>;
#[cfg(feature = "capture-benchmark")]
type BenchmarkCaptureChannelParts<B> = (
    BenchmarkCapturePublisher<B>,
    RawCaptureControl<B>,
    BenchmarkCaptureWriter<B>,
);

#[cfg(feature = "capture-benchmark")]
fn benchmark_capture_channel<B: CaptureAuthorityBundle>(
    process: &CaptureProcessInfrastructure,
    limits: CaptureChannelLimits,
    bundle: B,
) -> Result<BenchmarkCaptureChannelParts<B>, CaptureChannelError> {
    capture_channel_core::<B, SelectedBenchmarkTransport>(process, limits, bundle)
}

#[cfg(feature = "capture-benchmark")]
const fn benchmark_transport_identity() -> &'static str {
    <SelectedBenchmarkTransport as transport::CaptureQueueTransport>::IDENTITY
}

#[cfg(feature = "capture-benchmark")]
const fn benchmark_private_storage_accounting() -> &'static str {
    <SelectedBenchmarkTransport as transport::CaptureQueueTransport>::PRIVATE_STORAGE_ACCOUNTING
}

mod accounting;
mod admission;
#[cfg(feature = "capture-benchmark")]
pub mod benchmark_support;
mod contracts;
mod control;
mod diagnostic;
mod generation;
mod policy;
mod queue;
mod transport;
mod writer;

pub use accounting::{CaptureAccountingSnapshot, CaptureAccountingSnapshotError};
pub use admission::RawCapturePublisher;
pub use contracts::{
    CaptureChannelError, CaptureChannelLimits, CaptureHealthEvent, CaptureHealthReason,
    CaptureHealthSnapshot, CapturePublishError, CapturePublisherCloneError, CaptureQueueKind,
    CapturedRawRecord,
};
pub use control::{CaptureGenerationError, RawCaptureControl};
pub use diagnostic::{
    DiagnosticCaptureBundle, DiagnosticCaptureError, DiagnosticCaptureFrame,
    DiagnosticCaptureReceipt,
};
pub use policy::{CaptureWriterPolicy, CaptureWriterPolicyError};
#[cfg(all(feature = "capture-test", debug_assertions))]
pub use writer::CaptureReceiverTestCoordinationError;
pub use writer::{
    CaptureDestination, CaptureDestinationError, CaptureDestinationFenceError, CaptureIoContext,
    CaptureProcessInfrastructure, CaptureProcessInfrastructureLimits, CaptureShutdownStatus,
    CaptureSink, CaptureSinkError, CaptureStorageErrorClass, CaptureWorkerReapError,
    CaptureWorkerTermination, CaptureWriterHandle, CaptureWriterOutcome, CaptureWriterSpawnError,
    DestinationFenceRegistryInitializationError,
    DestinationFenceRegistryPermanentInitializationError, MemoryCaptureSink,
    MemoryCaptureSinkConstructionError, PendingCaptureWriter, WriterFixedStorageReceipt,
    WriterRuntimeProofError, initialize_capture_process_infrastructure, spawn_capture_writer,
};

#[cfg(test)]
mod tests;
