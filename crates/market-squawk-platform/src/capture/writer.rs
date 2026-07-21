//! Supervised capture-sink storage outside the live event-to-action path.

#[cfg(feature = "capture-benchmark")]
mod benchmark;
mod destination;
#[cfg(not(test))]
mod lifecycle;
#[cfg(test)]
pub(super) mod lifecycle;
mod runtime;
mod sink;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::{RawCaptureRecord, RawCaptureRecordError};
use market_squawk_domain::{CaptureAuthorityBundle, RawCaptureFrameView};
use thiserror::Error;

use super::accounting::CaptureAccountingError;
use super::queue::{RecvTimeoutError, TryRecvError};
use super::transport::{CaptureQueueReceiver, CaptureQueueTransport};
use super::{
    CaptureHealthReason, CaptureIdentitySnapshot, CaptureMessage, CaptureState, CaptureWriterCore,
    CaptureWriterPolicy, CapturedRawRecord, GenerationCaptureState, RawCaptureWriter,
    WRITER_NOT_STARTED, WRITER_RUNNING,
};
#[cfg(feature = "capture-benchmark")]
pub(super) use benchmark::{
    BenchmarkCaptureWriterHandle, BenchmarkCaptureWriterShutdown, spawn_benchmark_capture_writer,
};
use destination::{
    CaptureDestinationLease, acquire_destination_fence, destination_lease_allocation_bytes,
};
use lifecycle::CaptureWorkerFinalReport;
pub(super) use lifecycle::WriterLifecycleCore;
use runtime::{
    PreparedWriterRuntime, WriterFixedStorageError, WriterFixedStorageOwner,
    WriterRuntimePreparationError, WriterScratch, WriterScratchError, prepare_writer_runtime,
};

pub use destination::{
    CaptureDestination, CaptureDestinationError, CaptureDestinationFenceError,
    CaptureProcessInfrastructure, CaptureProcessInfrastructureLimits,
    DestinationFenceRegistryInitializationError,
    DestinationFenceRegistryPermanentInitializationError,
    initialize_capture_process_infrastructure,
};
#[cfg(all(feature = "capture-test", debug_assertions))]
pub use lifecycle::CaptureReceiverTestCoordinationError;
pub use lifecycle::{
    CaptureShutdownStatus, CaptureWorkerReapError, CaptureWorkerTermination, CaptureWriterHandle,
    PendingCaptureWriter,
};
pub use runtime::{WriterFixedStorageReceipt, WriterRuntimeProofError};
pub use sink::{
    CaptureIoContext, CaptureSink, CaptureSinkError, CaptureStorageErrorClass, MemoryCaptureSink,
    MemoryCaptureSinkConstructionError,
};

/// Final supervised writer outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CaptureWriterOutcome {
    /// Every accepted frame was written and final flush succeeded.
    Complete {
        /// Number of written frames.
        records_written: u64,
    },
    /// Capture is incomplete and cannot authorize execution for this binding.
    Incomplete {
        /// Number of frames written before failure or deadline.
        records_written: u64,
        /// Failure reason.
        reason: CaptureHealthReason,
    },
}

impl CaptureWriterOutcome {
    /// Returns whether capture was incomplete.
    pub const fn is_incomplete(&self) -> bool {
        matches!(self, Self::Incomplete { .. })
    }

    /// Returns the final number of successfully appended records.
    pub const fn records_written(&self) -> u64 {
        match self {
            Self::Complete { records_written }
            | Self::Incomplete {
                records_written, ..
            } => *records_written,
        }
    }
}

/// Failure to start the dedicated capture writer thread.
#[derive(Debug, Error)]
pub enum CaptureWriterSpawnError {
    /// The complete writer-start graph exceeded the channel memory ceiling.
    #[error("capture writer fixed storage requires {required} bytes but ceiling is {limit} bytes")]
    FixedStorageBudgetExceeded {
        /// Total channel bytes required by the rejected reservation.
        required: usize,
        /// Configured channel ceiling.
        limit: usize,
    },
    /// A fallible fixed writer scratch allocation was refused.
    #[error("capture writer scratch allocation of {requested_bytes} bytes failed")]
    ScratchAllocationFailed {
        /// Exact requested allocation length.
        requested_bytes: usize,
    },
    /// The fixed destination registry rejected this exact destination.
    #[error("capture destination fence rejected {destination:?}: {source}")]
    DestinationFence {
        /// Redacted exact destination identity.
        destination: CaptureDestination,
        /// Exact fixed-registry refusal.
        #[source]
        source: CaptureDestinationFenceError,
    },
    /// The pinned standard-library runtime proof did not match this binary.
    #[error("capture writer runtime proof failed: {0}")]
    RuntimeProof(#[from] WriterRuntimeProofError),
    /// The configured writer thread name exceeded its fixed bound.
    #[error("capture writer thread name is {actual} bytes; maximum is {limit}")]
    ThreadNameLimitExceeded {
        /// Actual UTF-8 thread-name length.
        actual: usize,
        /// Maximum admitted UTF-8 thread-name length.
        limit: usize,
    },
    /// The operating system rejected dedicated thread creation.
    #[error("failed to start dedicated capture writer thread: {source}")]
    ThreadSpawnFailed {
        /// Underlying operating-system thread creation failure.
        #[source]
        source: std::io::Error,
    },
}

pub(super) fn writer_failed<B: CaptureAuthorityBundle>(
    state: &CaptureState<B>,
    records_written: u64,
) -> CaptureWriterOutcome {
    state.mark_writer_failed();
    CaptureWriterOutcome::Incomplete {
        records_written,
        reason: CaptureHealthReason::WriterFailed,
    }
}

fn stop_accepting<B: CaptureAuthorityBundle>(state: &CaptureState<B>) {
    state.stop_writer(CaptureHealthReason::WriterStopped);
}

fn diagnostic_record<B: CaptureAuthorityBundle>(
    allocation: &Arc<GenerationCaptureState<B>>,
    frame: &B::Frame,
    scratch: &mut WriterScratch,
) -> Result<CapturedRawRecord, RawCaptureRecordError> {
    let (connection_id, event_id) = scratch.diagnostic_uuid_inputs(frame).map_err(|_error| {
        RawCaptureRecordError::RetainedSize(
            market_squawk_domain::CaptureRetainedSizeError::InvalidAuthorityGraph {
                component: market_squawk_domain::CaptureRetainedComponent::Frame,
            },
        )
    })?;
    let nanos = frame.received_at().unix_nanos();
    let seconds = nanos.div_euclid(1_000_000_000);
    let subsecond = u32::try_from(nanos.rem_euclid(1_000_000_000))
        .map_err(|_error| RawCaptureRecordError::InvalidReceivedAt)?;
    let received_at = chrono::DateTime::from_timestamp(seconds, subsecond)
        .ok_or(RawCaptureRecordError::InvalidReceivedAt)?;
    let record = RawCaptureRecord::try_new_live_payload(
        event_id,
        scratch
            .source_arc(frame.source_id().as_str())
            .map_err(|_error| {
                RawCaptureRecordError::RetainedSize(
                    market_squawk_domain::CaptureRetainedSizeError::InvalidAuthorityGraph {
                        component: market_squawk_domain::CaptureRetainedComponent::Frame,
                    },
                )
            })?,
        connection_id,
        None,
        None,
        received_at,
        frame.capture_payload().clone(),
    )?;
    if !frame
        .capture_payload()
        .shares_allocation_with(record.capture_payload())
    {
        return Err(RawCaptureRecordError::InvalidPayloadSharing);
    }
    let _complete_retained_bytes = record.checked_retained_bytes()?;
    Ok(CapturedRawRecord::new(
        CaptureIdentitySnapshot(Arc::clone(&allocation.identity)),
        frame.frame_ordinal(),
        record,
    ))
}

#[derive(Debug)]
struct CaptureWriterProgress {
    records_written: u64,
    since_flush: usize,
    scratch: WriterScratch,
}

impl CaptureWriterProgress {
    fn new(scratch: WriterScratch) -> Self {
        Self {
            records_written: 0,
            since_flush: 0,
            scratch,
        }
    }
}

fn append_frame<B: CaptureAuthorityBundle, S: CaptureSink>(
    sink: &mut S,
    allocation: &Arc<GenerationCaptureState<B>>,
    frame: &B::Frame,
    state: &CaptureState<B>,
    progress: &mut CaptureWriterProgress,
    policy: CaptureWriterPolicy,
    io_context: &CaptureIoContext,
) -> Result<(), CaptureWriterOutcome> {
    let captured =
        diagnostic_record(allocation, frame, &mut progress.scratch).map_err(|_error| {
            state.mark_incomplete_for_generation(
                allocation,
                CaptureHealthReason::DiagnosticConversion,
            );
            state.mark_writer_failed();
            CaptureWriterOutcome::Incomplete {
                records_written: progress.records_written,
                reason: CaptureHealthReason::DiagnosticConversion,
            }
        })?;
    if io_context.deadline_reached() {
        return Err(shutdown_deadline_outcome(state, progress.records_written));
    }
    match sink.append(&captured, io_context) {
        Ok(()) => {}
        Err(CaptureSinkError::ShutdownDeadline) => {
            return Err(shutdown_deadline_outcome(state, progress.records_written));
        }
        Err(_error) => {
            state.mark_incomplete_for_generation(allocation, CaptureHealthReason::WriterFailed);
            return Err(writer_failed(state, progress.records_written));
        }
    }
    // A successful append is committed accounting even if shutdown elapsed while the sink was
    // blocked. Count it before the post-I/O checkpoint so late durable work is never erased.
    let Some(next) = state.record_completed_append() else {
        state.mark_incomplete_for_generation(allocation, CaptureHealthReason::WriterFailed);
        return Err(writer_failed(state, progress.records_written));
    };
    progress.records_written = next;
    progress.since_flush = progress.since_flush.saturating_add(1);
    if io_context.deadline_reached() {
        return Err(shutdown_deadline_outcome(state, progress.records_written));
    }
    if progress.since_flush >= policy.flush_every_records.get() {
        match sink.flush(io_context) {
            Ok(()) => {}
            Err(CaptureSinkError::ShutdownDeadline) => {
                return Err(shutdown_deadline_outcome(state, progress.records_written));
            }
            Err(_error) => {
                state.mark_incomplete_for_generation(allocation, CaptureHealthReason::WriterFailed);
                return Err(writer_failed(state, progress.records_written));
            }
        }
        progress.since_flush = 0;
        if io_context.deadline_reached() {
            return Err(shutdown_deadline_outcome(state, progress.records_written));
        }
    }
    Ok(())
}

fn try_receive<B: CaptureAuthorityBundle, T: CaptureQueueTransport>(
    writer: &CaptureWriterCore<B, T>,
) -> Result<CaptureMessage<B>, TryRecvError> {
    writer
        .receiver
        .as_ref()
        .ok_or(TryRecvError::Closed)?
        .try_recv()
}

fn receive_timeout<B: CaptureAuthorityBundle, T: CaptureQueueTransport>(
    writer: &CaptureWriterCore<B, T>,
    timeout: Duration,
) -> Result<CaptureMessage<B>, RecvTimeoutError> {
    writer
        .receiver
        .as_ref()
        .ok_or(RecvTimeoutError::Closed)?
        .recv_timeout(timeout)
}

fn process_message<B: CaptureAuthorityBundle, S: CaptureSink>(
    message: CaptureMessage<B>,
    sink: &mut S,
    state: &CaptureState<B>,
    progress: &mut CaptureWriterProgress,
    policy: CaptureWriterPolicy,
    io_context: &CaptureIoContext,
) -> Result<(), CaptureWriterOutcome> {
    match message {
        CaptureMessage::Record {
            allocation,
            frame,
            reservation,
        } => {
            // Unified record accounting follows the owned record through conversion, append, and
            // any policy-triggered flush. The reservation releases exactly once only after this
            // complete writer operation returns or unwinds through an error path.
            let result = append_frame(
                sink,
                &allocation,
                &frame,
                state,
                progress,
                policy,
                io_context,
            );
            drop(reservation);
            result
        }
    }
}

pub(super) fn deadline_after(duration: Duration) -> std::time::Instant {
    let now = std::time::Instant::now();
    now.checked_add(duration).map_or(now, |deadline| deadline)
}

fn shutdown_deadline_outcome<B: CaptureAuthorityBundle>(
    state: &CaptureState<B>,
    records_written: u64,
) -> CaptureWriterOutcome {
    state.mark_current_incomplete(CaptureHealthReason::ShutdownDeadline);
    CaptureWriterOutcome::Incomplete {
        records_written,
        reason: CaptureHealthReason::ShutdownDeadline,
    }
}

fn drain_and_flush<B: CaptureAuthorityBundle, S: CaptureSink, T: CaptureQueueTransport>(
    writer: &CaptureWriterCore<B, T>,
    sink: &mut S,
    state: &CaptureState<B>,
    progress: &mut CaptureWriterProgress,
    policy: CaptureWriterPolicy,
    io_context: &CaptureIoContext,
) -> CaptureWriterOutcome {
    loop {
        if io_context.deadline_reached() {
            return shutdown_deadline_outcome(state, progress.records_written);
        }
        match try_receive(writer) {
            Ok(message) => {
                if let Err(outcome) =
                    process_message(message, sink, state, progress, policy, io_context)
                {
                    return outcome;
                }
            }
            Err(TryRecvError::Empty) => std::thread::yield_now(),
            Err(TryRecvError::Closed) => break,
            Err(TryRecvError::Poisoned) => {
                return writer_failed(state, progress.records_written);
            }
        }
    }
    if io_context.deadline_reached() {
        return shutdown_deadline_outcome(state, progress.records_written);
    }
    match sink.finish(io_context) {
        Ok(()) => {}
        Err(CaptureSinkError::ShutdownDeadline) => {
            return shutdown_deadline_outcome(state, progress.records_written);
        }
        Err(_error) => return writer_failed(state, progress.records_written),
    }
    if io_context.deadline_reached() {
        return shutdown_deadline_outcome(state, progress.records_written);
    }
    state.mark_writer_stopped();
    CaptureWriterOutcome::Complete {
        records_written: progress.records_written,
    }
}

fn run_capture_writer<B: CaptureAuthorityBundle, S: CaptureSink, T: CaptureQueueTransport>(
    writer: CaptureWriterCore<B, T>,
    mut sink: S,
    policy: CaptureWriterPolicy,
    io_context: &CaptureIoContext,
    scratch: WriterScratch,
) -> CaptureWriterOutcome {
    let state = Arc::clone(&writer.state);
    let mut progress = CaptureWriterProgress::new(scratch);
    let mut next_flush_at = deadline_after(policy.flush_interval);
    loop {
        if io_context.shutdown_requested() {
            stop_accepting(&state);
            return drain_and_flush(
                &writer,
                &mut sink,
                &state,
                &mut progress,
                policy,
                io_context,
            );
        }
        let wait_duration = next_flush_at.saturating_duration_since(std::time::Instant::now());
        match receive_timeout(&writer, wait_duration) {
            Ok(message) => {
                if let Err(outcome) = process_message(
                    message,
                    &mut sink,
                    &state,
                    &mut progress,
                    policy,
                    io_context,
                ) {
                    return outcome;
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                if progress.since_flush > 0 {
                    match sink.flush(io_context) {
                        Ok(()) => {}
                        Err(CaptureSinkError::ShutdownDeadline) => {
                            return shutdown_deadline_outcome(&state, progress.records_written);
                        }
                        Err(_error) => return writer_failed(&state, progress.records_written),
                    }
                    if io_context.deadline_reached() {
                        return shutdown_deadline_outcome(&state, progress.records_written);
                    }
                }
                progress.since_flush = 0;
                next_flush_at = deadline_after(policy.flush_interval);
            }
            Err(RecvTimeoutError::Closed) => {
                stop_accepting(&state);
                return drain_and_flush(
                    &writer,
                    &mut sink,
                    &state,
                    &mut progress,
                    policy,
                    io_context,
                );
            }
            Err(RecvTimeoutError::Poisoned) => {
                return writer_failed(&state, progress.records_written);
            }
        }
        if std::time::Instant::now() >= next_flush_at {
            if progress.since_flush > 0 {
                match sink.flush(io_context) {
                    Ok(()) => {}
                    Err(CaptureSinkError::ShutdownDeadline) => {
                        return shutdown_deadline_outcome(&state, progress.records_written);
                    }
                    Err(_error) => return writer_failed(&state, progress.records_written),
                }
                if io_context.deadline_reached() {
                    return shutdown_deadline_outcome(&state, progress.records_written);
                }
            }
            progress.since_flush = 0;
            next_flush_at = deadline_after(policy.flush_interval);
        }
    }
}

#[derive(Debug)]
struct WriterSpawnPacket<B: CaptureAuthorityBundle, S: CaptureSink, T: CaptureQueueTransport> {
    writer: CaptureWriterCore<B, T>,
    sink: S,
    policy: CaptureWriterPolicy,
    io_context: CaptureIoContext,
    destination_fence: Arc<CaptureDestinationLease>,
    fixed_storage: Arc<WriterFixedStorageOwner>,
    scratch: WriterScratch,
}

impl<B: CaptureAuthorityBundle, S: CaptureSink, T: CaptureQueueTransport>
    WriterSpawnPacket<B, S, T>
{
    fn run(self) {
        let lifecycle = Arc::clone(&self.io_context.lifecycle);
        let outcome = run_capture_writer(
            self.writer,
            self.sink,
            self.policy,
            &self.io_context,
            self.scratch,
        );
        let report = CaptureWorkerFinalReport {
            outcome,
            shutdown_deadline_elapsed_at_exit: self.io_context.deadline_reached(),
        };
        match lifecycle.final_report.lock() {
            Ok(mut retained) => *retained = Some(report),
            Err(poisoned) => *poisoned.into_inner() = Some(report),
        }
        lifecycle.completion.notify_one();
        drop(self.destination_fence);
        drop(self.fixed_storage);
    }
}

fn writer_runtime_error(
    error: WriterRuntimePreparationError,
    ceiling: usize,
) -> CaptureWriterSpawnError {
    match error {
        WriterRuntimePreparationError::Scratch(WriterScratchError::AllocationFailed {
            requested_bytes,
        })
        | WriterRuntimePreparationError::ThreadNameAllocationFailed { requested_bytes } => {
            CaptureWriterSpawnError::ScratchAllocationFailed { requested_bytes }
        }
        WriterRuntimePreparationError::Proof(error) => CaptureWriterSpawnError::RuntimeProof(error),
        WriterRuntimePreparationError::ThreadNameLimitExceeded { actual, limit } => {
            CaptureWriterSpawnError::ThreadNameLimitExceeded { actual, limit }
        }
        WriterRuntimePreparationError::Accounting(CaptureAccountingError::BudgetExceeded {
            required,
            ceiling,
        }) => CaptureWriterSpawnError::FixedStorageBudgetExceeded {
            required,
            limit: ceiling,
        },
        WriterRuntimePreparationError::FixedStorage(
            WriterFixedStorageError::ArithmeticOverflow,
        )
        | WriterRuntimePreparationError::Layout
        | WriterRuntimePreparationError::Accounting(
            CaptureAccountingError::ArithmeticOverflow
            | CaptureAccountingError::TransitionOverflow
            | CaptureAccountingError::EpochOverflow
            | CaptureAccountingError::InvariantViolated,
        ) => CaptureWriterSpawnError::FixedStorageBudgetExceeded {
            required: usize::MAX,
            limit: ceiling,
        },
    }
}

/// Starts one supervised dedicated capture writer thread.
pub fn spawn_capture_writer<B: CaptureAuthorityBundle, S: CaptureSink>(
    writer: RawCaptureWriter<B>,
    sink: S,
    policy: CaptureWriterPolicy,
) -> Result<CaptureWriterHandle<B>, CaptureWriterSpawnError> {
    let spawned = spawn_capture_writer_core(writer.core, sink, policy)?;
    Ok(CaptureWriterHandle {
        thread: spawned.thread,
        queue_control: spawned.queue_control,
        io_context: spawned.io_context,
        state: spawned.state,
        destination_fence: spawned.destination_fence,
        fixed_storage: spawned.fixed_storage,
        completed: false,
    })
}

#[derive(Debug)]
struct SpawnedCaptureWriter<B: CaptureAuthorityBundle, T: CaptureQueueTransport> {
    thread: Option<std::thread::JoinHandle<()>>,
    queue_control: T::Control<CaptureMessage<B>>,
    io_context: CaptureIoContext,
    state: Arc<CaptureState<B>>,
    destination_fence: Option<Arc<CaptureDestinationLease>>,
    fixed_storage: Option<Arc<WriterFixedStorageOwner>>,
}

fn spawn_capture_writer_core<
    B: CaptureAuthorityBundle,
    S: CaptureSink,
    T: CaptureQueueTransport,
>(
    writer: CaptureWriterCore<B, T>,
    sink: S,
    policy: CaptureWriterPolicy,
) -> Result<SpawnedCaptureWriter<B, T>, CaptureWriterSpawnError> {
    let destination = sink.destination();
    let (worker_destination_fence, owner_destination_fence) =
        match acquire_destination_fence(writer.state.process, &destination) {
            Ok(fences) => fences,
            Err(CaptureDestinationFenceError::Busy) => {
                return Err(CaptureWriterSpawnError::DestinationFence {
                    destination: destination.clone(),
                    source: CaptureDestinationFenceError::Busy,
                });
            }
            Err(CaptureDestinationFenceError::Capacity) => {
                return Err(CaptureWriterSpawnError::DestinationFence {
                    destination: destination.clone(),
                    source: CaptureDestinationFenceError::Capacity,
                });
            }
            Err(CaptureDestinationFenceError::RegistryPoisoned) => {
                return Err(CaptureWriterSpawnError::DestinationFence {
                    destination: destination.clone(),
                    source: CaptureDestinationFenceError::RegistryPoisoned,
                });
            }
        };
    let state = Arc::clone(&writer.state);
    let queue_control = writer.queue_control.clone();
    let ceiling = state.accounting.configured_ceiling().get();
    let destination_lease_bytes = destination_lease_allocation_bytes().map_err(|_error| {
        CaptureWriterSpawnError::FixedStorageBudgetExceeded {
            required: usize::MAX,
            limit: ceiling,
        }
    })?;
    let PreparedWriterRuntime {
        scratch,
        thread_name,
        fixed_storage,
    } = prepare_writer_runtime(
        &state.accounting,
        destination_lease_bytes,
        std::mem::size_of::<WriterSpawnPacket<B, S, T>>(),
    )
    .map_err(|error| writer_runtime_error(error, ceiling))?;
    writer
        .state
        .writer_lifecycle
        .compare_exchange(
            WRITER_NOT_STARTED,
            WRITER_RUNNING,
            Ordering::AcqRel,
            Ordering::Acquire,
        )
        .map_err(|_previous| CaptureWriterSpawnError::ThreadSpawnFailed {
            source: std::io::Error::other("capture writer lifecycle is not startable"),
        })?;
    let io_context = CaptureIoContext::new(Arc::clone(&state.writer_lifecycle_core));
    let packet = WriterSpawnPacket {
        writer,
        sink,
        policy,
        io_context: io_context.clone(),
        destination_fence: worker_destination_fence,
        fixed_storage: Arc::clone(&fixed_storage),
        scratch,
    };
    let thread = std::thread::Builder::new()
        .name(thread_name)
        .spawn(move || packet.run())
        .map_err(|source| {
            state.mark_writer_failed();
            CaptureWriterSpawnError::ThreadSpawnFailed { source }
        })?;
    Ok(SpawnedCaptureWriter {
        thread: Some(thread),
        queue_control,
        io_context,
        state,
        destination_fence: Some(owner_destination_fence),
        fixed_storage: Some(fixed_storage),
    })
}
