//! Supervised capture-sink storage outside the live event-to-action path.

mod destination;
mod lifecycle;
mod sink;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use bytes::Bytes;
use market_squawk_domain::{CaptureAuthorityBundle, RawCaptureFrameView};
use thiserror::Error;
use uuid::Uuid;

use crate::{RawCaptureRecord, RawCaptureRecordError};

use super::{
    CaptureHealthReason, CaptureMessage, CaptureState, CaptureWriterPolicy, CapturedRawRecord,
    GenerationCaptureState, RawCaptureWriter, WRITER_NOT_STARTED, WRITER_RUNNING,
};
use destination::{CaptureDestinationFenceError, acquire_destination_fence};
use lifecycle::CaptureWorkerFinalReport;

pub use destination::{CaptureDestination, CaptureDestinationError};
pub use lifecycle::{
    CaptureShutdownStatus, CaptureWorkerReapError, CaptureWorkerTermination, CaptureWriterHandle,
    PendingCaptureWriter,
};
pub use sink::{
    CaptureIoContext, CaptureSink, CaptureSinkError, CaptureStorageErrorClass, MemoryCaptureSink,
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
    /// Another worker or unreaped lifecycle owner fences the exact destination.
    #[error("capture destination already has an active or unreaped writer: {destination:?}")]
    DestinationBusy {
        /// Redacted exact destination identity.
        destination: CaptureDestination,
    },
    /// The bounded process-wide active-destination registry is full.
    #[error("active capture destination capacity is exhausted")]
    DestinationCapacity,
    /// The operating system rejected dedicated thread creation.
    #[error("failed to start dedicated capture writer thread: {source}")]
    Thread {
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

fn diagnostic_uuid_inputs<F: RawCaptureFrameView>(frame: &F) -> (Uuid, Uuid) {
    let mut generation = Vec::with_capacity(256);
    for field in [
        frame.source_id().as_str().as_bytes(),
        frame
            .metadata_revision()
            .as_source_identifier()
            .as_str()
            .as_bytes(),
        frame.session_identifier().as_str().as_bytes(),
    ] {
        generation.extend_from_slice(
            &u64::try_from(field.len())
                .map_or(u64::MAX, |length| length)
                .to_be_bytes(),
        );
        generation.extend_from_slice(field);
    }
    generation.extend_from_slice(&frame.connection_generation().get().to_be_bytes());
    let connection_id = Uuid::new_v5(&Uuid::NAMESPACE_OID, &generation);
    let mut event = Vec::with_capacity(24);
    event.extend_from_slice(connection_id.as_bytes());
    event.extend_from_slice(&frame.frame_ordinal().get().to_be_bytes());
    (connection_id, Uuid::new_v5(&Uuid::NAMESPACE_OID, &event))
}

fn diagnostic_record<B: CaptureAuthorityBundle>(
    allocation: &Arc<GenerationCaptureState<B>>,
    frame: &B::Frame,
) -> Result<CapturedRawRecord, RawCaptureRecordError> {
    let (connection_id, event_id) = diagnostic_uuid_inputs(frame);
    let nanos = frame.received_at().unix_nanos();
    let seconds = nanos.div_euclid(1_000_000_000);
    let subsecond = u32::try_from(nanos.rem_euclid(1_000_000_000))
        .map_err(|_error| RawCaptureRecordError::InvalidReceivedAt)?;
    let received_at = chrono::DateTime::from_timestamp(seconds, subsecond)
        .ok_or(RawCaptureRecordError::InvalidReceivedAt)?;
    let record = RawCaptureRecord::try_new_live(
        event_id,
        Arc::from(frame.source_id().as_str()),
        connection_id,
        None,
        None,
        received_at,
        Bytes::copy_from_slice(frame.payload()),
    )?;
    Ok(CapturedRawRecord::new(
        Arc::clone(&allocation.identity),
        frame.frame_ordinal(),
        record,
    ))
}

#[derive(Debug, Default)]
struct CaptureWriterProgress {
    records_written: u64,
    since_flush: usize,
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
    let captured = diagnostic_record(allocation, frame).map_err(|_error| {
        state.mark_incomplete_for_generation(allocation, CaptureHealthReason::DiagnosticConversion);
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

fn try_receive<B: CaptureAuthorityBundle>(
    writer: &RawCaptureWriter<B>,
) -> Result<CaptureMessage<B>, mpsc::TryRecvError> {
    let receiver = match writer.receiver.lock() {
        Ok(receiver) => receiver,
        Err(poisoned) => poisoned.into_inner(),
    };
    receiver.try_recv()
}

fn receive_timeout<B: CaptureAuthorityBundle>(
    writer: &RawCaptureWriter<B>,
    timeout: Duration,
) -> Result<CaptureMessage<B>, mpsc::RecvTimeoutError> {
    let receiver = match writer.receiver.lock() {
        Ok(receiver) => receiver,
        Err(poisoned) => poisoned.into_inner(),
    };
    receiver.recv_timeout(timeout)
}

pub(super) fn try_drain_pending<B: CaptureAuthorityBundle>(
    receiver: &std::sync::Mutex<mpsc::Receiver<CaptureMessage<B>>>,
) {
    let receiver = match receiver.try_lock() {
        Ok(receiver) => receiver,
        Err(std::sync::TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
        Err(std::sync::TryLockError::WouldBlock) => return,
    };
    while receiver.try_recv().is_ok() {}
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
            // The bounded-queue permit covers work awaiting the single writer. Once dequeued, the
            // writer owns the frame outside the queue budget; release before potentially blocking
            // sink I/O so handle-drop and deadline-detach can reclaim every queued permit.
            drop(reservation);
            append_frame(
                sink,
                &allocation,
                &frame,
                state,
                progress,
                policy,
                io_context,
            )
        }
        CaptureMessage::Wake => Ok(()),
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

fn drain_and_flush<B: CaptureAuthorityBundle, S: CaptureSink>(
    writer: &RawCaptureWriter<B>,
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
            Err(mpsc::TryRecvError::Empty | mpsc::TryRecvError::Disconnected) => break,
        }
    }
    if io_context.deadline_reached() {
        return shutdown_deadline_outcome(state, progress.records_written);
    }
    match sink.flush(io_context) {
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

fn run_capture_writer<B: CaptureAuthorityBundle, S: CaptureSink>(
    writer: RawCaptureWriter<B>,
    mut sink: S,
    policy: CaptureWriterPolicy,
    io_context: &CaptureIoContext,
) -> CaptureWriterOutcome {
    let state = Arc::clone(&writer.state);
    let mut progress = CaptureWriterProgress::default();
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
            Err(mpsc::RecvTimeoutError::Timeout) => {
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
            Err(mpsc::RecvTimeoutError::Disconnected) => {
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

/// Starts one supervised dedicated capture writer thread.
pub fn spawn_capture_writer<B: CaptureAuthorityBundle, S: CaptureSink>(
    mut writer: RawCaptureWriter<B>,
    sink: S,
    policy: CaptureWriterPolicy,
) -> Result<CaptureWriterHandle<B>, CaptureWriterSpawnError> {
    let destination = sink.destination();
    let (worker_destination_fence, owner_destination_fence) =
        match acquire_destination_fence(&destination) {
            Ok(fences) => fences,
            Err(CaptureDestinationFenceError::Busy) => {
                return Err(CaptureWriterSpawnError::DestinationBusy {
                    destination: destination.clone(),
                });
            }
            Err(CaptureDestinationFenceError::Capacity) => {
                return Err(CaptureWriterSpawnError::DestinationCapacity);
            }
        };
    let state = Arc::clone(&writer.state);
    let receiver = Arc::clone(&writer.receiver);
    writer
        .state
        .writer_lifecycle
        .compare_exchange(
            WRITER_NOT_STARTED,
            WRITER_RUNNING,
            Ordering::AcqRel,
            Ordering::Acquire,
        )
        .map_err(|_previous| CaptureWriterSpawnError::Thread {
            source: std::io::Error::other("capture writer lifecycle is not startable"),
        })?;
    let shutdown_requested = Arc::new(AtomicBool::new(false));
    let shutdown_deadline = Arc::new(std::sync::Mutex::new(None));
    let io_context = CaptureIoContext::new(shutdown_requested, shutdown_deadline);
    let thread_context = io_context.clone();
    let wake_sender = writer
        .sender
        .take()
        .ok_or_else(|| CaptureWriterSpawnError::Thread {
            source: std::io::Error::other("capture writer control sender is unavailable"),
        })?;
    let completion = Arc::new(tokio::sync::Notify::new());
    let thread_completion = Arc::clone(&completion);
    let final_report = Arc::new(std::sync::Mutex::new(None));
    let thread_report = Arc::clone(&final_report);
    let thread = std::thread::Builder::new()
        .name("market-squawk-capture".to_owned())
        .spawn(move || {
            let _worker_destination_fence = worker_destination_fence;
            let outcome = run_capture_writer(writer, sink, policy, &thread_context);
            let report = CaptureWorkerFinalReport {
                outcome,
                shutdown_deadline_elapsed_at_exit: thread_context.deadline_reached(),
            };
            match thread_report.lock() {
                Ok(mut retained) => *retained = Some(report),
                Err(poisoned) => *poisoned.into_inner() = Some(report),
            }
            thread_completion.notify_one();
        })
        .map_err(|source| {
            state.mark_writer_failed();
            CaptureWriterSpawnError::Thread { source }
        })?;
    Ok(CaptureWriterHandle {
        thread: Some(thread),
        completion,
        final_report,
        wake_sender: Some(wake_sender),
        io_context,
        receiver,
        state,
        destination_fence: Some(owner_destination_fence),
        completed: false,
    })
}
