//! Supervised capture-sink storage outside the live event-to-action path.

use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    time::Duration,
};

use crate::{JournalError, JournalWriter};
use thiserror::Error;

use super::{
    CaptureHealthReason, CaptureMessage, CaptureState, CaptureWriterPolicy, CapturedRawRecord,
    RawCaptureWriter, WRITER_NOT_STARTED, WRITER_RUNNING, WRITER_STOPPED,
};

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
    /// An unclassified storage failure occurred; no free-form value is retained.
    Other,
}

/// Synchronous storage contract consumed only by the supervised background writer.
pub trait CaptureSink: fmt::Debug + Send + 'static {
    /// Appends one validated compatibility record.
    fn append(&mut self, record: &CapturedRawRecord) -> Result<(), CaptureSinkError>;
    /// Flushes buffered records durably according to the sink contract.
    fn flush(&mut self) -> Result<(), CaptureSinkError>;
}

impl CaptureSink for JournalWriter {
    fn append(&mut self, captured: &CapturedRawRecord) -> Result<(), CaptureSinkError> {
        // MSJ1 is a committed raw diagnostic wire. The exact live key stays out of band, and the
        // journal API explicitly reports that replay authority is unavailable by format.
        self.append(captured.record()).map_err(Into::into)
    }

    fn flush(&mut self) -> Result<(), CaptureSinkError> {
        self.flush().map_err(Into::into)
    }
}

/// In-memory sink for deterministic supervision tests and diagnostics.
#[derive(Debug, Default)]
pub struct MemoryCaptureSink {
    records: Vec<CapturedRawRecord>,
}

impl MemoryCaptureSink {
    /// Returns retained records.
    pub fn records(&self) -> &[CapturedRawRecord] {
        &self.records
    }
}

impl CaptureSink for MemoryCaptureSink {
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
#[error("failed to start dedicated capture writer thread: {source}")]
pub struct CaptureWriterSpawnError {
    /// Underlying operating-system thread creation failure.
    #[source]
    source: std::io::Error,
}

/// Supervised dedicated writer-thread handle.
#[derive(Debug)]
pub struct CaptureWriterHandle {
    thread: Option<std::thread::JoinHandle<()>>,
    completion: tokio::sync::oneshot::Receiver<CaptureWriterOutcome>,
    wake_sender: Option<mpsc::SyncSender<CaptureMessage>>,
    shutdown_requested: Arc<AtomicBool>,
    shutdown_deadline: Arc<std::sync::Mutex<Option<std::time::Instant>>>,
    receiver: Arc<std::sync::Mutex<mpsc::Receiver<CaptureMessage>>>,
    state: Arc<CaptureState>,
    completed: bool,
}

fn writer_failed(state: &CaptureState, records_written: u64) -> CaptureWriterOutcome {
    state.mark_writer_failed();
    CaptureWriterOutcome::Incomplete {
        records_written,
        reason: CaptureHealthReason::WriterFailed,
    }
}

fn stop_accepting(state: &CaptureState) {
    let active = state.active.load_full();
    active.accepting.store(false, Ordering::Release);
    state
        .writer_lifecycle
        .store(WRITER_STOPPED, Ordering::Release);
}

fn append_captured<S: CaptureSink>(
    sink: &mut S,
    captured: &CapturedRawRecord,
    state: &CaptureState,
    records_written: &mut u64,
    since_flush: &mut usize,
    policy: CaptureWriterPolicy,
) -> Result<(), CaptureWriterOutcome> {
    let append_result = sink.append(captured);
    if append_result.is_err() {
        return Err(writer_failed(state, *records_written));
    }
    let Some(next) = state.increment_written(*records_written) else {
        return Err(writer_failed(state, *records_written));
    };
    *records_written = next;
    *since_flush = since_flush.saturating_add(1);
    if *since_flush >= policy.flush_every_records.get() {
        if sink.flush().is_err() {
            return Err(writer_failed(state, *records_written));
        }
        *since_flush = 0;
    }
    Ok(())
}

fn try_receive(writer: &RawCaptureWriter) -> Result<CaptureMessage, mpsc::TryRecvError> {
    let receiver = match writer.receiver.lock() {
        Ok(receiver) => receiver,
        Err(poisoned) => poisoned.into_inner(),
    };
    receiver.try_recv()
}

fn receive_timeout(
    writer: &RawCaptureWriter,
    timeout: Duration,
) -> Result<CaptureMessage, mpsc::RecvTimeoutError> {
    let receiver = match writer.receiver.lock() {
        Ok(receiver) => receiver,
        Err(poisoned) => poisoned.into_inner(),
    };
    receiver.recv_timeout(timeout)
}

fn drain_pending_reservations(receiver: &std::sync::Mutex<mpsc::Receiver<CaptureMessage>>) {
    let receiver = match receiver.lock() {
        Ok(receiver) => receiver,
        Err(poisoned) => poisoned.into_inner(),
    };
    while receiver.try_recv().is_ok() {}
}

fn try_drain_pending_reservations(receiver: &std::sync::Mutex<mpsc::Receiver<CaptureMessage>>) {
    let receiver = match receiver.try_lock() {
        Ok(receiver) => receiver,
        Err(std::sync::TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
        Err(std::sync::TryLockError::WouldBlock) => return,
    };
    while receiver.try_recv().is_ok() {}
}

fn drain_and_flush<S: CaptureSink>(
    writer: &RawCaptureWriter,
    sink: &mut S,
    state: &CaptureState,
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
            Ok(CaptureMessage::Record {
                captured,
                reservation,
            }) => {
                drop(reservation);
                let append_result =
                    append_captured(sink, &captured, state, records_written, since_flush, policy);
                if let Err(outcome) = append_result {
                    return outcome;
                }
            }
            Ok(CaptureMessage::Wake) => {}
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
    match now.checked_add(duration) {
        Some(deadline) => deadline,
        None => now,
    }
}

fn run_capture_writer<S: CaptureSink>(
    writer: RawCaptureWriter,
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
            Ok(CaptureMessage::Record {
                captured,
                reservation,
            }) => {
                drop(reservation);
                let append_result = append_captured(
                    &mut sink,
                    &captured,
                    &state,
                    &mut records_written,
                    &mut since_flush,
                    policy,
                );
                if let Err(outcome) = append_result {
                    return outcome;
                }
            }
            Ok(CaptureMessage::Wake) => {}
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
///
/// Publication remains synchronous `try_send` with no per-frame acknowledgement. Filesystem
/// writes and durable flushes never execute on a Tokio cooperative worker.
pub fn spawn_capture_writer<S: CaptureSink>(
    mut writer: RawCaptureWriter,
    sink: S,
    policy: CaptureWriterPolicy,
) -> Result<CaptureWriterHandle, CaptureWriterSpawnError> {
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
        .map_err(|_previous| CaptureWriterSpawnError {
            source: std::io::Error::other("capture writer lifecycle is not startable"),
        })?;
    let shutdown_requested = Arc::new(AtomicBool::new(false));
    let shutdown_deadline = Arc::new(std::sync::Mutex::new(None));
    let thread_shutdown = Arc::clone(&shutdown_requested);
    let thread_deadline = Arc::clone(&shutdown_deadline);
    let wake_sender = writer
        .sender
        .take()
        .ok_or_else(|| CaptureWriterSpawnError {
            source: std::io::Error::other("capture writer control sender is unavailable"),
        })?;
    let (completion_sender, completion) = tokio::sync::oneshot::channel();
    let thread = std::thread::Builder::new()
        .name("market-squawk-capture".to_owned())
        .spawn(move || {
            let outcome =
                run_capture_writer(writer, sink, policy, &thread_shutdown, &thread_deadline);
            let _completion_result = completion_sender.send(outcome);
        })
        .map_err(|source| {
            state.mark_writer_failed();
            CaptureWriterSpawnError { source }
        })?;
    Ok(CaptureWriterHandle {
        thread: Some(thread),
        completion,
        wake_sender: Some(wake_sender),
        shutdown_requested,
        shutdown_deadline,
        receiver,
        state,
        completed: false,
    })
}

impl CaptureWriterHandle {
    fn request_shutdown(&self) {
        self.shutdown_requested.store(true, Ordering::Release);
        if let Some(sender) = &self.wake_sender {
            let _wake_result = sender.try_send(CaptureMessage::Wake);
        }
    }

    fn join_finished_thread(&mut self) -> bool {
        self.thread
            .take()
            .is_none_or(|thread| thread.join().is_ok())
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
        drain_pending_reservations(&self.receiver);
        self.completed = true;
        outcome
    }

    /// Requests cooperative drain and waits only to the explicit deadline.
    ///
    /// A blocking operating-system filesystem call cannot be force-cancelled safely. On deadline
    /// expiry this returns Incomplete immediately, permanently fails capture health closed, and
    /// detaches the already-stopping writer thread.
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
                try_drain_pending_reservations(&self.receiver);
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

impl Drop for CaptureWriterHandle {
    fn drop(&mut self) {
        if !self.completed {
            self.request_shutdown();
            try_drain_pending_reservations(&self.receiver);
            self.state.mark_writer_failed();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroUsize, time::Duration};

    use market_squawk_domain::{
        ConnectionGeneration, MetadataRevision, SourceId, SourceIdentifier,
    };
    use uuid::Uuid;

    use super::{CaptureWriterPolicy, MemoryCaptureSink, spawn_capture_writer};
    use crate::{CaptureGenerationKey, raw_capture_channel};

    #[test]
    fn dropping_a_handle_never_waits_for_the_receiver_lock()
    -> Result<(), Box<dyn std::error::Error>> {
        let key = CaptureGenerationKey::new(
            SourceId::try_from("source-a")?,
            MetadataRevision::new(SourceIdentifier::try_from("revision-a")?),
            SourceIdentifier::try_from("session-a")?,
            ConnectionGeneration::new(1)?,
            Uuid::from_u128(1),
        );
        let (_publisher, _control, writer) = raw_capture_channel(NonZeroUsize::MIN, key);
        let retained_receiver = std::sync::Arc::clone(&writer.receiver);
        let receiver_guard = match retained_receiver.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        let handle = spawn_capture_writer(
            writer,
            MemoryCaptureSink::default(),
            CaptureWriterPolicy::default(),
        )?;
        let started = std::time::Instant::now();

        drop(handle);

        assert!(started.elapsed() < Duration::from_millis(50));
        drop(receiver_guard);
        Ok(())
    }
}
