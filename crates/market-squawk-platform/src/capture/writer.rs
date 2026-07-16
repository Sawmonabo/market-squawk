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
const MAX_ACTIVE_CAPTURE_DESTINATIONS: usize = 1_024;
const CAPTURE_DESTINATION_DOMAIN: &[u8] = b"MSQKCAPTUREDESTINATION\x01";

/// Redacted exact identity for one capture storage destination.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct CaptureDestination([u8; 32]);

impl CaptureDestination {
    /// Constructs a destination from one bounded non-secret alternative-sink label.
    ///
    /// Every handle for the same underlying physical endpoint in this process must use the same
    /// stable, collision-resistant label. Per-instance or random aliases are valid only for truly
    /// independent storage. This identity provides no cross-process exclusion; custom sinks shared
    /// by multiple processes must also enforce an operating-system or storage-level ownership
    /// primitive.
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
struct CaptureDestinationLease {
    destination: CaptureDestination,
}

impl Drop for CaptureDestinationLease {
    fn drop(&mut self) {
        let Some(registry) = CAPTURE_DESTINATION_FENCES.get() else {
            return;
        };
        let mut registry = match registry.lock() {
            Ok(registry) => registry,
            Err(poisoned) => poisoned.into_inner(),
        };
        registry.remove_if_matches(&self.destination, self as *const Self);
    }
}

#[derive(Debug, Default)]
struct CaptureDestinationFenceRegistry {
    leases: HashMap<CaptureDestination, Weak<CaptureDestinationLease>>,
}

impl CaptureDestinationFenceRegistry {
    fn try_acquire(
        &mut self,
        destination: &CaptureDestination,
    ) -> Result<
        (Arc<CaptureDestinationLease>, Arc<CaptureDestinationLease>),
        CaptureDestinationFenceError,
    > {
        if self
            .leases
            .get(destination)
            .is_some_and(|lease| lease.strong_count() > 0)
        {
            return Err(CaptureDestinationFenceError::Busy);
        }
        self.leases.remove(destination);
        if self.leases.len() >= MAX_ACTIVE_CAPTURE_DESTINATIONS {
            return Err(CaptureDestinationFenceError::Capacity);
        }
        let lease = Arc::new(CaptureDestinationLease {
            destination: destination.clone(),
        });
        self.leases
            .insert(destination.clone(), Arc::downgrade(&lease));
        Ok((Arc::clone(&lease), lease))
    }

    fn remove_if_matches(
        &mut self,
        destination: &CaptureDestination,
        lease: *const CaptureDestinationLease,
    ) {
        if self
            .leases
            .get(destination)
            .is_some_and(|retained| std::ptr::eq(retained.as_ptr(), lease))
        {
            self.leases.remove(destination);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CaptureDestinationFenceError {
    Busy,
    Capacity,
}

static CAPTURE_DESTINATION_FENCES: OnceLock<std::sync::Mutex<CaptureDestinationFenceRegistry>> =
    OnceLock::new();

fn acquire_destination_fence(
    destination: &CaptureDestination,
) -> Result<
    (Arc<CaptureDestinationLease>, Arc<CaptureDestinationLease>),
    CaptureDestinationFenceError,
> {
    let registry = CAPTURE_DESTINATION_FENCES
        .get_or_init(|| std::sync::Mutex::new(CaptureDestinationFenceRegistry::default()));
    let mut registry = match registry.lock() {
        Ok(registry) => registry,
        Err(poisoned) => poisoned.into_inner(),
    };
    registry.try_acquire(destination)
}

/// Cooperative capture-I/O shutdown observation.
///
/// A successful checkpoint permits entering the next bounded sink operation. It does not prove an
/// already-running operating-system call was cancelled or completed.
#[derive(Clone, Debug)]
pub struct CaptureIoContext {
    shutdown_requested: Arc<AtomicBool>,
    shutdown_deadline: Arc<std::sync::Mutex<Option<std::time::Instant>>>,
}

impl CaptureIoContext {
    fn new(
        shutdown_requested: Arc<AtomicBool>,
        shutdown_deadline: Arc<std::sync::Mutex<Option<std::time::Instant>>>,
    ) -> Self {
        Self {
            shutdown_requested,
            shutdown_deadline,
        }
    }

    /// Fails when the configured shutdown deadline has elapsed.
    ///
    /// This is an operation-boundary check only. It never cancels a call already inside the sink.
    pub fn checkpoint(&self) -> Result<(), CaptureSinkError> {
        if self.deadline_reached() {
            Err(CaptureSinkError::ShutdownDeadline)
        } else {
            Ok(())
        }
    }

    fn shutdown_requested(&self) -> bool {
        self.shutdown_requested.load(Ordering::Acquire)
    }

    fn deadline_reached(&self) -> bool {
        let deadline = match self.shutdown_deadline.lock() {
            Ok(deadline) => *deadline,
            Err(poisoned) => *poisoned.into_inner(),
        };
        deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline)
    }
}

/// Typed storage failure returned to the supervised capture writer.
#[derive(Debug, Error)]
pub enum CaptureSinkError {
    /// Cooperative shutdown reached its deadline at an operation boundary.
    #[error("capture shutdown deadline reached")]
    ShutdownDeadline,
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
    /// Returns the mandatory stable identity of the underlying physical storage endpoint.
    ///
    /// Separate sink handles that can write the same endpoint must return an equal,
    /// collision-resistant identity for the entire process lifetime. Alternative sinks must not
    /// assign per-instance or random aliases to shared storage. This process-local fence does not
    /// provide cross-process exclusion, so a sink reachable from multiple processes must enforce
    /// its own operating-system or storage-level ownership primitive.
    ///
    /// [`JournalWriter`] derives this identity from its prepared canonical root and separately
    /// retains the journal's exclusive file lock.
    fn destination(&self) -> CaptureDestination;
    /// Appends one bounded diagnostic record.
    fn append(
        &mut self,
        record: &CapturedRawRecord,
        context: &CaptureIoContext,
    ) -> Result<(), CaptureSinkError>;
    /// Flushes buffered records durably according to the sink contract.
    fn flush(&mut self, context: &CaptureIoContext) -> Result<(), CaptureSinkError>;
}

impl CaptureSink for JournalWriter {
    fn destination(&self) -> CaptureDestination {
        CaptureDestination::for_journal(self.path())
    }

    fn append(
        &mut self,
        captured: &CapturedRawRecord,
        context: &CaptureIoContext,
    ) -> Result<(), CaptureSinkError> {
        context.checkpoint()?;
        self.append(captured.record()).map_err(Into::into)
    }

    fn flush(&mut self, context: &CaptureIoContext) -> Result<(), CaptureSinkError> {
        context.checkpoint()?;
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

    fn append(
        &mut self,
        record: &CapturedRawRecord,
        context: &CaptureIoContext,
    ) -> Result<(), CaptureSinkError> {
        context.checkpoint()?;
        self.records.push(record.clone());
        Ok(())
    }

    fn flush(&mut self, context: &CaptureIoContext) -> Result<(), CaptureSinkError> {
        context.checkpoint()
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

/// Supervised dedicated writer-thread handle.
#[derive(Debug)]
pub struct CaptureWriterHandle<B: CaptureAuthorityBundle> {
    thread: Option<std::thread::JoinHandle<()>>,
    completion: Arc<tokio::sync::Notify>,
    final_report: Arc<std::sync::Mutex<Option<CaptureWorkerFinalReport>>>,
    wake_sender: Option<mpsc::SyncSender<CaptureMessage<B>>>,
    io_context: CaptureIoContext,
    receiver: Arc<std::sync::Mutex<mpsc::Receiver<CaptureMessage<B>>>>,
    state: Arc<CaptureState<B>>,
    destination_fence: Option<Arc<CaptureDestinationLease>>,
    completed: bool,
}

/// Result of waiting on a retained capture worker without joining it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CaptureShutdownStatus {
    /// The worker thread exited but its lifecycle owner has not joined it yet.
    WorkerTerminated,
    /// Capture authority was revoked at the deadline while the worker was still running.
    DeadlineElapsed,
}

/// Final joined capture worker report.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CaptureWorkerTermination {
    outcome: CaptureWriterOutcome,
    shutdown_deadline_elapsed: bool,
    records_written_at_revocation: u64,
    final_records_written: u64,
    late_records_written: u64,
}

impl CaptureWorkerTermination {
    /// Returns the fail-closed storage outcome persisted after join.
    pub const fn outcome(&self) -> &CaptureWriterOutcome {
        &self.outcome
    }

    /// Returns whether the lifecycle owner observed the configured shutdown deadline elapse.
    ///
    /// This fact is independent of the storage outcome: a sink may return a storage error after
    /// the deadline, in which case the outcome remains `WriterFailed` and this flag is also true.
    pub const fn shutdown_deadline_elapsed(&self) -> bool {
        self.shutdown_deadline_elapsed
    }

    /// Returns records known complete when shutdown revoked positive authority.
    pub const fn records_written_at_revocation(&self) -> u64 {
        self.records_written_at_revocation
    }

    /// Returns all successful appends observed after worker join.
    pub const fn final_records_written(&self) -> u64 {
        self.final_records_written
    }

    /// Returns successful appends completed after authority revocation.
    pub const fn late_records_written(&self) -> u64 {
        self.late_records_written
    }
}

/// Nonblocking reap failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CaptureWorkerReapError {
    /// Joining is forbidden until thread termination is independently observable.
    #[error("capture worker is still running")]
    WorkerStillRunning,
}

/// Explicit lifecycle owner for a shutdown-requested capture worker.
///
/// Dropping an unreaped owner joins synchronously as a fail-closed fallback and can therefore block
/// the caller, including an async executor thread. Async compositions must retain the owner, use a
/// borrowing wait, and call [`Self::try_reap`] after termination is observable.
#[derive(Debug)]
#[must_use = "pending capture workers must remain owned and be explicitly reaped"]
pub struct PendingCaptureWriter<B: CaptureAuthorityBundle> {
    thread: Option<std::thread::JoinHandle<()>>,
    completion: Arc<tokio::sync::Notify>,
    final_report: Arc<std::sync::Mutex<Option<CaptureWorkerFinalReport>>>,
    wake_sender: Option<mpsc::SyncSender<CaptureMessage<B>>>,
    io_context: CaptureIoContext,
    receiver: Arc<std::sync::Mutex<mpsc::Receiver<CaptureMessage<B>>>>,
    state: Arc<CaptureState<B>>,
    destination_fence: Option<Arc<CaptureDestinationLease>>,
    deadline: std::time::Instant,
    records_written_at_revocation: u64,
    termination: Option<CaptureWorkerTermination>,
    deadline_recorded: bool,
}

#[derive(Debug)]
struct CaptureWorkerFinalReport {
    outcome: CaptureWriterOutcome,
    shutdown_deadline_elapsed_at_exit: bool,
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

fn deadline_after(duration: Duration) -> std::time::Instant {
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

impl<B: CaptureAuthorityBundle> CaptureWriterHandle<B> {
    fn request_shutdown(&self) {
        self.io_context
            .shutdown_requested
            .store(true, Ordering::Release);
        if let Some(sender) = &self.wake_sender {
            let _wake_result = sender.try_send(CaptureMessage::Wake);
        }
    }

    /// Consumes the ordinary handle, revokes positive authority, and returns explicit worker
    /// supervision ownership.
    ///
    /// The returned owner must be retained across its borrowing async wait and explicitly reaped.
    /// No thread termination is implied by this synchronous transition.
    pub fn shutdown(mut self, deadline: Duration) -> PendingCaptureWriter<B> {
        let absolute_deadline = deadline_after(deadline);
        match self.io_context.shutdown_deadline.lock() {
            Ok(mut configured) => *configured = Some(absolute_deadline),
            Err(poisoned) => *poisoned.into_inner() = Some(absolute_deadline),
        }
        let revocation = self
            .state
            .revoke_writer_for_shutdown(CaptureHealthReason::WriterStopped);
        self.request_shutdown();
        self.completed = true;
        PendingCaptureWriter {
            thread: self.thread.take(),
            completion: Arc::clone(&self.completion),
            final_report: Arc::clone(&self.final_report),
            wake_sender: self.wake_sender.take(),
            io_context: self.io_context.clone(),
            receiver: Arc::clone(&self.receiver),
            state: Arc::clone(&self.state),
            destination_fence: self.destination_fence.take(),
            deadline: absolute_deadline,
            records_written_at_revocation: revocation.records_written_at_revocation,
            termination: None,
            deadline_recorded: false,
        }
    }
}

impl<B: CaptureAuthorityBundle> Drop for CaptureWriterHandle<B> {
    fn drop(&mut self) {
        if !self.completed {
            let revocation = self
                .state
                .revoke_writer_for_shutdown(CaptureHealthReason::WriterFailed);
            self.request_shutdown();
            try_drain_pending(&self.receiver);
            self.wake_sender.take();
            let joined = self
                .thread
                .take()
                .is_none_or(|thread| thread.join().is_ok());
            let termination = termination_after_join(
                &self.state,
                &self.final_report,
                revocation.records_written_at_revocation,
                false,
                joined,
            );
            if termination.outcome().is_incomplete() {
                self.state
                    .mark_current_incomplete(CaptureHealthReason::WriterFailed);
            }
            self.destination_fence.take();
        }
    }
}

impl<B: CaptureAuthorityBundle> PendingCaptureWriter<B> {
    /// Returns whether the OS thread has exited. The destination remains fenced until reap.
    pub fn is_worker_terminated(&self) -> bool {
        self.thread
            .as_ref()
            .is_none_or(std::thread::JoinHandle::is_finished)
    }

    /// Waits until either the configured deadline elapses or thread exit is observable.
    ///
    /// This method borrows the owner and never joins or transfers it to another task.
    pub async fn wait_until_deadline(&mut self) -> CaptureShutdownStatus {
        loop {
            if self.is_worker_terminated() {
                return CaptureShutdownStatus::WorkerTerminated;
            }
            let now = std::time::Instant::now();
            if now >= self.deadline {
                self.record_deadline();
                return CaptureShutdownStatus::DeadlineElapsed;
            }
            let remaining = self.deadline.saturating_duration_since(now);
            tokio::select! {
                () = self.completion.notified() => {}
                () = tokio::time::sleep(remaining) => {}
            }
        }
    }

    /// Waits until thread exit is observable without joining it.
    ///
    /// This method borrows the owner, so cancellation leaves join ownership with the caller.
    pub async fn wait_until_terminated(&mut self) {
        while !self.is_worker_terminated() {
            tokio::select! {
                () = self.completion.notified() => {}
                () = tokio::time::sleep(Duration::from_millis(1)) => {}
            }
        }
    }

    /// Joins an already-terminated worker and persists its final report before releasing the
    /// destination fence.
    ///
    /// # Errors
    ///
    /// Returns [`CaptureWorkerReapError::WorkerStillRunning`] rather than blocking when thread exit
    /// is not yet independently observable.
    pub fn try_reap(
        &mut self,
    ) -> Result<Option<&CaptureWorkerTermination>, CaptureWorkerReapError> {
        if self.termination.is_some() {
            return Ok(self.termination.as_ref());
        }
        let Some(thread) = self.thread.as_ref() else {
            return Ok(None);
        };
        if !thread.is_finished() {
            return Err(CaptureWorkerReapError::WorkerStillRunning);
        }
        let joined = self
            .thread
            .take()
            .is_some_and(|thread| thread.join().is_ok());
        self.persist_termination(joined);
        Ok(self.termination.as_ref())
    }

    fn record_deadline(&mut self) {
        if self.deadline_recorded {
            return;
        }
        try_drain_pending(&self.receiver);
        self.state
            .mark_current_incomplete(CaptureHealthReason::ShutdownDeadline);
        self.deadline_recorded = true;
    }

    fn persist_termination(&mut self, joined: bool) {
        let termination = termination_after_join(
            &self.state,
            &self.final_report,
            self.records_written_at_revocation,
            self.deadline_recorded,
            joined,
        );
        self.termination = Some(termination);
        // The report is now retained in the lifecycle owner. Only this point may release the owner
        // side of the two-party destination fence.
        self.destination_fence.take();
    }

    fn request_shutdown(&self) {
        self.io_context
            .shutdown_requested
            .store(true, Ordering::Release);
        if let Some(sender) = &self.wake_sender {
            let _wake_result = sender.try_send(CaptureMessage::Wake);
        }
    }
}

impl<B: CaptureAuthorityBundle> Drop for PendingCaptureWriter<B> {
    // This join is deliberately synchronous and can block the caller, including an async executor
    // thread. Production callers must retain, wait, and explicitly reap rather than rely on Drop.
    fn drop(&mut self) {
        if self.termination.is_some() {
            return;
        }
        self.request_shutdown();
        try_drain_pending(&self.receiver);
        self.wake_sender.take();
        let joined = self
            .thread
            .take()
            .is_none_or(|thread| thread.join().is_ok());
        self.persist_termination(joined);
    }
}

fn termination_after_join<B: CaptureAuthorityBundle>(
    state: &CaptureState<B>,
    final_report: &std::sync::Mutex<Option<CaptureWorkerFinalReport>>,
    records_written_at_revocation: u64,
    shutdown_deadline_elapsed: bool,
    joined: bool,
) -> CaptureWorkerTermination {
    let retained = match final_report.lock() {
        Ok(mut retained) => retained.take(),
        Err(poisoned) => poisoned.into_inner().take(),
    };
    let accounting = state.completion_snapshot();
    let expected_late = accounting
        .records_written
        .checked_sub(records_written_at_revocation);
    let accounting_valid = accounting.records_written_at_revocation
        == records_written_at_revocation
        && expected_late == Some(accounting.late_records_written);
    let outcome_matches = retained
        .as_ref()
        .is_some_and(|report| report.outcome.records_written() == accounting.records_written);
    let shutdown_deadline_elapsed = shutdown_deadline_elapsed
        || retained
            .as_ref()
            .is_some_and(|report| report.shutdown_deadline_elapsed_at_exit);
    let outcome = if joined && accounting_valid && outcome_matches {
        match retained {
            Some(report) => report.outcome,
            None => writer_failed(state, accounting.records_written),
        }
    } else {
        state.mark_current_incomplete(CaptureHealthReason::AccountingInvariant);
        writer_failed(state, accounting.records_written)
    };
    CaptureWorkerTermination {
        outcome,
        shutdown_deadline_elapsed,
        records_written_at_revocation,
        final_records_written: accounting.records_written,
        late_records_written: expected_late.map_or(0, |late| late),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};
    use std::time::{Duration, Instant};

    use super::{
        CaptureDestination, CaptureDestinationFenceError, CaptureDestinationFenceRegistry,
        MAX_ACTIVE_CAPTURE_DESTINATIONS, acquire_destination_fence,
    };

    #[test]
    fn destination_registry_rejects_capacity_without_unbounded_growth()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut registry = CaptureDestinationFenceRegistry::default();
        let mut retained = Vec::with_capacity(MAX_ACTIVE_CAPTURE_DESTINATIONS);
        for index in 0..MAX_ACTIVE_CAPTURE_DESTINATIONS {
            let destination = CaptureDestination::try_named(&format!("registry-capacity-{index}"))?;
            let leases = registry
                .try_acquire(&destination)
                .map_err(|error| format!("unexpected registry acquisition failure: {error:?}"))?;
            retained.push(leases);
        }
        let overflow = CaptureDestination::try_named("registry-capacity-overflow")?;
        assert!(matches!(
            registry.try_acquire(&overflow),
            Err(CaptureDestinationFenceError::Capacity)
        ));
        assert_eq!(registry.leases.len(), MAX_ACTIVE_CAPTURE_DESTINATIONS);
        drop(retained);
        Ok(())
    }

    #[test]
    fn destination_registry_churn_removes_each_exact_dead_lease()
    -> Result<(), Box<dyn std::error::Error>> {
        for index in 0..MAX_ACTIVE_CAPTURE_DESTINATIONS.saturating_mul(2) {
            let destination = CaptureDestination::try_named(&format!("registry-churn-{index}"))?;
            let (worker, owner) = acquire_destination_fence(&destination)
                .map_err(|error| format!("unexpected registry acquisition failure: {error:?}"))?;
            drop(worker);
            drop(owner);
            let registry = super::CAPTURE_DESTINATION_FENCES
                .get()
                .ok_or("destination registry was not initialized")?;
            let registry = match registry.lock() {
                Ok(registry) => registry,
                Err(poisoned) => poisoned.into_inner(),
            };
            assert!(!registry.leases.contains_key(&destination));
        }
        Ok(())
    }

    #[test]
    fn final_lease_drop_can_race_same_destination_acquisition_without_deadlock()
    -> Result<(), Box<dyn std::error::Error>> {
        for index in 0..64 {
            let destination =
                CaptureDestination::try_named(&format!("registry-drop-race-{index}"))?;
            let (worker, owner) = acquire_destination_fence(&destination)
                .map_err(|error| format!("unexpected registry acquisition failure: {error:?}"))?;
            drop(worker);
            let race_start = Arc::new(Barrier::new(2));
            let drop_race_start = Arc::clone(&race_start);
            let (drop_complete_sender, drop_complete_receiver) = std::sync::mpsc::sync_channel(1);
            let drop_thread = std::thread::spawn(move || {
                drop_race_start.wait();
                drop(owner);
                let _sent = drop_complete_sender.send(());
            });

            race_start.wait();
            let acquisition_deadline = Instant::now() + Duration::from_secs(1);
            let replacement = loop {
                match acquire_destination_fence(&destination) {
                    Ok(leases) => break leases,
                    Err(CaptureDestinationFenceError::Busy)
                        if Instant::now() < acquisition_deadline =>
                    {
                        std::thread::yield_now();
                    }
                    Err(error) => {
                        return Err(format!(
                            "same-destination race did not acquire before deadline: {error:?}"
                        )
                        .into());
                    }
                }
            };
            drop_complete_receiver.recv_timeout(Duration::from_secs(1))?;
            drop_thread
                .join()
                .map_err(|_panic| "destination lease drop thread panicked")?;
            drop(replacement);
        }
        Ok(())
    }
}
