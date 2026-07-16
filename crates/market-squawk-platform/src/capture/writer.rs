//! Supervised capture-sink storage outside the live event-to-action path.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock, Weak, mpsc};
use std::time::Duration;

use bytes::Bytes;
use market_squawk_domain::{CaptureAuthorityBundle, RawCaptureFrameView};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

use crate::{JournalError, JournalWriter, RawCaptureRecord, RawCaptureRecordError};

use super::{
    CaptureHealthReason, CaptureMessage, CaptureState, CaptureWriterPolicy, CapturedRawRecord,
    GenerationCaptureState, RawCaptureWriter, WRITER_NOT_STARTED, WRITER_RUNNING,
};

const MAX_CAPTURE_DESTINATION_LABEL_BYTES: usize = 1_024;
const CAPTURE_DESTINATION_DOMAIN: &[u8] = b"MSQKCAPTUREDESTINATION\x01";

/// Redacted exact identity for one capture storage destination.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct CaptureDestination([u8; 32]);

impl CaptureDestination {
    /// Constructs a destination from one bounded non-secret alternative-sink label.
    ///
    /// # Errors
    ///
    /// Rejects an empty label or one larger than 1,024 bytes.
    pub fn try_named(label: &str) -> Result<Self, CaptureDestinationError> {
        if label.is_empty() {
            return Err(CaptureDestinationError::Empty);
        }
        if label.len() > MAX_CAPTURE_DESTINATION_LABEL_BYTES {
            return Err(CaptureDestinationError::TooLong {
                max: MAX_CAPTURE_DESTINATION_LABEL_BYTES,
            });
        }
        Ok(Self::from_bytes(b"named", label.as_bytes()))
    }

    pub(crate) fn for_journal(path: &std::path::Path) -> Self {
        Self::from_bytes(b"journal", path.as_os_str().as_encoded_bytes())
    }

    fn unique_memory() -> Self {
        Self::from_bytes(b"memory", Uuid::new_v4().as_bytes())
    }

    fn from_bytes(kind: &[u8], value: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(CAPTURE_DESTINATION_DOMAIN);
        hasher.update(
            u64::try_from(kind.len())
                .map_or(u64::MAX, |length| length)
                .to_be_bytes(),
        );
        hasher.update(kind);
        hasher.update(
            u64::try_from(value.len())
                .map_or(u64::MAX, |length| length)
                .to_be_bytes(),
        );
        hasher.update(value);
        Self(hasher.finalize().into())
    }
}

impl fmt::Debug for CaptureDestination {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("CaptureDestination")
            .field(&self.0)
            .finish()
    }
}

/// Capture destination construction failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CaptureDestinationError {
    /// An empty destination cannot establish a stable fence.
    #[error("capture destination label cannot be empty")]
    Empty,
    /// The destination label exceeded its retained input bound.
    #[error("capture destination label exceeds maximum {max} bytes")]
    TooLong {
        /// Maximum accepted label bytes.
        max: usize,
    },
}

#[derive(Debug)]
struct CaptureDestinationLease;

static CAPTURE_DESTINATION_FENCES: OnceLock<
    std::sync::Mutex<HashMap<CaptureDestination, Weak<CaptureDestinationLease>>>,
> = OnceLock::new();

fn acquire_destination_fence(
    destination: &CaptureDestination,
) -> Option<(Arc<CaptureDestinationLease>, Arc<CaptureDestinationLease>)> {
    let registry = CAPTURE_DESTINATION_FENCES.get_or_init(|| std::sync::Mutex::new(HashMap::new()));
    let mut registry = match registry.lock() {
        Ok(registry) => registry,
        Err(poisoned) => poisoned.into_inner(),
    };
    registry.retain(|_destination, lease| lease.strong_count() > 0);
    if registry.get(destination).and_then(Weak::upgrade).is_some() {
        return None;
    }
    let lease = Arc::new(CaptureDestinationLease);
    registry.insert(destination.clone(), Arc::downgrade(&lease));
    Some((Arc::clone(&lease), lease))
}

/// Typed storage failure returned to the supervised capture writer.
#[derive(Debug, Error)]
pub enum CaptureSinkError {
    /// Journal storage rejected a record or flush.
    #[error(transparent)]
    Journal(#[from] JournalError),
    /// Alternative capture storage failed.
    #[error("capture storage failed: {class:?}")]
    Storage {
        /// Bounded non-secret error classification.
        class: CaptureStorageErrorClass,
    },
}

impl CaptureSinkError {
    /// Constructs an alternative-storage error from a bounded non-secret class.
    pub const fn storage(class: CaptureStorageErrorClass) -> Self {
        Self::Storage { class }
    }
}

/// Bounded, non-secret classification for alternative capture-sink failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CaptureStorageErrorClass {
    /// The storage endpoint is unavailable.
    Unavailable,
    /// The storage endpoint rejected data as corrupt.
    Corruption,
    /// A configured or physical capacity bound was exceeded.
    Capacity,
    /// An unclassified storage failure occurred.
    Other,
}

/// Synchronous diagnostic storage contract consumed only by the background writer.
pub trait CaptureSink: fmt::Debug + Send + 'static {
    /// Returns the exact process-local destination fence identity.
    fn destination(&self) -> CaptureDestination;
    /// Appends one bounded diagnostic record.
    fn append(&mut self, record: &CapturedRawRecord) -> Result<(), CaptureSinkError>;
    /// Flushes buffered records durably according to the sink contract.
    fn flush(&mut self) -> Result<(), CaptureSinkError>;
}

impl CaptureSink for JournalWriter {
    fn destination(&self) -> CaptureDestination {
        CaptureDestination::for_journal(self.path())
    }

    fn append(&mut self, captured: &CapturedRawRecord) -> Result<(), CaptureSinkError> {
        self.append(captured.record()).map_err(Into::into)
    }

    fn flush(&mut self) -> Result<(), CaptureSinkError> {
        self.flush().map_err(Into::into)
    }
}

/// In-memory sink for deterministic supervision tests and diagnostics.
#[derive(Debug)]
pub struct MemoryCaptureSink {
    destination: CaptureDestination,
    records: Vec<CapturedRawRecord>,
}

impl Default for MemoryCaptureSink {
    fn default() -> Self {
        Self {
            destination: CaptureDestination::unique_memory(),
            records: Vec::new(),
        }
    }
}

impl MemoryCaptureSink {
    /// Returns retained records.
    pub fn records(&self) -> &[CapturedRawRecord] {
        &self.records
    }
}

impl CaptureSink for MemoryCaptureSink {
    fn destination(&self) -> CaptureDestination {
        self.destination.clone()
    }

    fn append(&mut self, record: &CapturedRawRecord) -> Result<(), CaptureSinkError> {
        self.records.push(record.clone());
        Ok(())
    }

    fn flush(&mut self) -> Result<(), CaptureSinkError> {
        Ok(())
    }
}

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
}

/// Alias emphasizing shutdown result semantics.
pub type CaptureShutdown = CaptureWriterOutcome;

/// Failure to start the dedicated capture writer thread.
#[derive(Debug, Error)]
pub enum CaptureWriterSpawnError {
    /// Another worker or unreaped lifecycle owner fences the exact destination.
    #[error("capture destination already has an active or unreaped writer: {destination:?}")]
    DestinationBusy {
        /// Redacted exact destination identity.
        destination: CaptureDestination,
    },
    /// The operating system rejected dedicated thread creation.
    #[error("failed to start dedicated capture writer thread: {source}")]
    Thread {
        /// Underlying operating-system thread creation failure.
        #[source]
        source: std::io::Error,
    },
}

/// Supervised dedicated writer-thread handle.
#[derive(Debug)]
pub struct CaptureWriterHandle<B: CaptureAuthorityBundle> {
    thread: Option<std::thread::JoinHandle<()>>,
    completion: tokio::sync::oneshot::Receiver<CaptureWriterOutcome>,
    wake_sender: Option<mpsc::SyncSender<CaptureMessage<B>>>,
    shutdown_requested: Arc<AtomicBool>,
    shutdown_deadline: Arc<std::sync::Mutex<Option<std::time::Instant>>>,
    receiver: Arc<std::sync::Mutex<mpsc::Receiver<CaptureMessage<B>>>>,
    state: Arc<CaptureState<B>>,
    destination_fence: Option<Arc<CaptureDestinationLease>>,
    completed: bool,
}

fn writer_failed<B: CaptureAuthorityBundle>(
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
        generation.extend_from_slice(&u64::try_from(field.len()).unwrap_or(u64::MAX).to_be_bytes());
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

fn append_frame<B: CaptureAuthorityBundle, S: CaptureSink>(
    sink: &mut S,
    allocation: &Arc<GenerationCaptureState<B>>,
    frame: &B::Frame,
    state: &CaptureState<B>,
    records_written: &mut u64,
    since_flush: &mut usize,
    policy: CaptureWriterPolicy,
) -> Result<(), CaptureWriterOutcome> {
    let captured = diagnostic_record(allocation, frame).map_err(|_error| {
        state.mark_incomplete_for_generation(allocation, CaptureHealthReason::DiagnosticConversion);
        state.mark_writer_failed();
        CaptureWriterOutcome::Incomplete {
            records_written: *records_written,
            reason: CaptureHealthReason::DiagnosticConversion,
        }
    })?;
    if sink.append(&captured).is_err() {
        state.mark_incomplete_for_generation(allocation, CaptureHealthReason::WriterFailed);
        return Err(writer_failed(state, *records_written));
    }
    let Some(next) = state.increment_written(*records_written) else {
        state.mark_incomplete_for_generation(allocation, CaptureHealthReason::WriterFailed);
        return Err(writer_failed(state, *records_written));
    };
    *records_written = next;
    *since_flush = since_flush.saturating_add(1);
    if *since_flush >= policy.flush_every_records.get() {
        if sink.flush().is_err() {
            state.mark_incomplete_for_generation(allocation, CaptureHealthReason::WriterFailed);
            return Err(writer_failed(state, *records_written));
        }
        *since_flush = 0;
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

fn drain_pending<B: CaptureAuthorityBundle>(
    receiver: &std::sync::Mutex<mpsc::Receiver<CaptureMessage<B>>>,
) {
    let receiver = match receiver.lock() {
        Ok(receiver) => receiver,
        Err(poisoned) => poisoned.into_inner(),
    };
    while receiver.try_recv().is_ok() {}
}

fn try_drain_pending<B: CaptureAuthorityBundle>(
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
    records_written: &mut u64,
    since_flush: &mut usize,
    policy: CaptureWriterPolicy,
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
                records_written,
                since_flush,
                policy,
            )
        }
        CaptureMessage::Wake => Ok(()),
    }
}

fn shutdown_deadline_reached(
    shutdown_deadline: &std::sync::Mutex<Option<std::time::Instant>>,
) -> bool {
    let deadline = match shutdown_deadline.lock() {
        Ok(deadline) => *deadline,
        Err(poisoned) => *poisoned.into_inner(),
    };
    deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline)
}

fn deadline_after(duration: Duration) -> std::time::Instant {
    let now = std::time::Instant::now();
    now.checked_add(duration).unwrap_or(now)
}

fn drain_and_flush<B: CaptureAuthorityBundle, S: CaptureSink>(
    writer: &RawCaptureWriter<B>,
    sink: &mut S,
    state: &CaptureState<B>,
    records_written: &mut u64,
    since_flush: &mut usize,
    policy: CaptureWriterPolicy,
    shutdown_deadline: &std::sync::Mutex<Option<std::time::Instant>>,
) -> CaptureWriterOutcome {
    loop {
        if shutdown_deadline_reached(shutdown_deadline) {
            state.mark_current_incomplete(CaptureHealthReason::ShutdownDeadline);
            return CaptureWriterOutcome::Incomplete {
                records_written: *records_written,
                reason: CaptureHealthReason::ShutdownDeadline,
            };
        }
        match try_receive(writer) {
            Ok(message) => {
                if let Err(outcome) =
                    process_message(message, sink, state, records_written, since_flush, policy)
                {
                    return outcome;
                }
            }
            Err(mpsc::TryRecvError::Empty | mpsc::TryRecvError::Disconnected) => break,
        }
    }
    if shutdown_deadline_reached(shutdown_deadline) {
        state.mark_current_incomplete(CaptureHealthReason::ShutdownDeadline);
        return CaptureWriterOutcome::Incomplete {
            records_written: *records_written,
            reason: CaptureHealthReason::ShutdownDeadline,
        };
    }
    if sink.flush().is_err() {
        return writer_failed(state, *records_written);
    }
    state.mark_writer_stopped();
    CaptureWriterOutcome::Complete {
        records_written: *records_written,
    }
}

fn run_capture_writer<B: CaptureAuthorityBundle, S: CaptureSink>(
    writer: RawCaptureWriter<B>,
    mut sink: S,
    policy: CaptureWriterPolicy,
    shutdown_requested: &AtomicBool,
    shutdown_deadline: &std::sync::Mutex<Option<std::time::Instant>>,
) -> CaptureWriterOutcome {
    let state = Arc::clone(&writer.state);
    let mut records_written = 0_u64;
    let mut since_flush = 0_usize;
    let mut next_flush_at = deadline_after(policy.flush_interval);
    loop {
        if shutdown_requested.load(Ordering::Acquire) {
            stop_accepting(&state);
            return drain_and_flush(
                &writer,
                &mut sink,
                &state,
                &mut records_written,
                &mut since_flush,
                policy,
                shutdown_deadline,
            );
        }
        let wait_duration = next_flush_at.saturating_duration_since(std::time::Instant::now());
        match receive_timeout(&writer, wait_duration) {
            Ok(message) => {
                if let Err(outcome) = process_message(
                    message,
                    &mut sink,
                    &state,
                    &mut records_written,
                    &mut since_flush,
                    policy,
                ) {
                    return outcome;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if since_flush > 0 && sink.flush().is_err() {
                    return writer_failed(&state, records_written);
                }
                since_flush = 0;
                next_flush_at = deadline_after(policy.flush_interval);
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                stop_accepting(&state);
                return drain_and_flush(
                    &writer,
                    &mut sink,
                    &state,
                    &mut records_written,
                    &mut since_flush,
                    policy,
                    shutdown_deadline,
                );
            }
        }
        if std::time::Instant::now() >= next_flush_at {
            if since_flush > 0 && sink.flush().is_err() {
                return writer_failed(&state, records_written);
            }
            since_flush = 0;
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
        acquire_destination_fence(&destination).ok_or_else(|| {
            CaptureWriterSpawnError::DestinationBusy {
                destination: destination.clone(),
            }
        })?;
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
    let thread_shutdown = Arc::clone(&shutdown_requested);
    let thread_deadline = Arc::clone(&shutdown_deadline);
    let wake_sender = writer
        .sender
        .take()
        .ok_or_else(|| CaptureWriterSpawnError::Thread {
            source: std::io::Error::other("capture writer control sender is unavailable"),
        })?;
    let (completion_sender, completion) = tokio::sync::oneshot::channel();
    let thread = std::thread::Builder::new()
        .name("market-squawk-capture".to_owned())
        .spawn(move || {
            let _worker_destination_fence = worker_destination_fence;
            let outcome =
                run_capture_writer(writer, sink, policy, &thread_shutdown, &thread_deadline);
            let _completion_result = completion_sender.send(outcome);
        })
        .map_err(|source| {
            state.mark_writer_failed();
            CaptureWriterSpawnError::Thread { source }
        })?;
    Ok(CaptureWriterHandle {
        thread: Some(thread),
        completion,
        wake_sender: Some(wake_sender),
        shutdown_requested,
        shutdown_deadline,
        receiver,
        state,
        destination_fence: Some(owner_destination_fence),
        completed: false,
    })
}

impl<B: CaptureAuthorityBundle> CaptureWriterHandle<B> {
    fn request_shutdown(&self) {
        self.shutdown_requested.store(true, Ordering::Release);
        if let Some(sender) = &self.wake_sender {
            let _wake_result = sender.try_send(CaptureMessage::Wake);
        }
    }

    fn join_finished_thread(&mut self) -> bool {
        let joined = self
            .thread
            .take()
            .is_none_or(|thread| thread.join().is_ok());
        self.destination_fence.take();
        joined
    }

    /// Waits for natural writer completion and joins the dedicated thread.
    pub async fn wait(mut self) -> CaptureWriterOutcome {
        self.wake_sender.take();
        let outcome = match (&mut self.completion).await {
            Ok(outcome) if self.join_finished_thread() => outcome,
            Ok(_) | Err(_) => writer_failed(
                &self.state,
                self.state.records_written.load(Ordering::Acquire),
            ),
        };
        drain_pending(&self.receiver);
        self.completed = true;
        outcome
    }

    /// Requests cooperative drain and waits only to the explicit deadline.
    pub async fn shutdown(mut self, deadline: Duration) -> CaptureShutdown {
        let absolute_deadline = deadline_after(deadline);
        match self.shutdown_deadline.lock() {
            Ok(mut configured) => *configured = Some(absolute_deadline),
            Err(poisoned) => *poisoned.into_inner() = Some(absolute_deadline),
        }
        self.request_shutdown();
        self.wake_sender.take();
        let outcome = match tokio::time::timeout(deadline, &mut self.completion).await {
            Ok(Ok(outcome)) if self.join_finished_thread() => outcome,
            Ok(Ok(_)) | Ok(Err(_)) => writer_failed(
                &self.state,
                self.state.records_written.load(Ordering::Acquire),
            ),
            Err(_elapsed) => {
                stop_accepting(&self.state);
                try_drain_pending(&self.receiver);
                self.state
                    .mark_current_incomplete(CaptureHealthReason::ShutdownDeadline);
                CaptureWriterOutcome::Incomplete {
                    records_written: self.state.records_written.load(Ordering::Acquire),
                    reason: CaptureHealthReason::ShutdownDeadline,
                }
            }
        };
        self.completed = true;
        outcome
    }
}

impl<B: CaptureAuthorityBundle> Drop for CaptureWriterHandle<B> {
    fn drop(&mut self) {
        if !self.completed {
            self.request_shutdown();
            try_drain_pending(&self.receiver);
            self.state.mark_writer_failed();
        }
    }
}
