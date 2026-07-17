use std::sync::atomic::Ordering;
use std::sync::{Arc, mpsc};
use std::time::Duration;

use market_squawk_domain::CaptureAuthorityBundle;
use thiserror::Error;

use super::super::{CaptureHealthReason, CaptureMessage, CaptureState};
use super::destination::CaptureDestinationLease;
use super::sink::CaptureIoContext;
use super::{CaptureWriterOutcome, deadline_after, try_drain_pending, writer_failed};

/// Supervised dedicated writer-thread handle.
#[derive(Debug)]
pub struct CaptureWriterHandle<B: CaptureAuthorityBundle> {
    pub(super) thread: Option<std::thread::JoinHandle<()>>,
    pub(super) completion: Arc<tokio::sync::Notify>,
    pub(super) final_report: Arc<std::sync::Mutex<Option<CaptureWorkerFinalReport>>>,
    pub(super) wake_sender: Option<mpsc::SyncSender<CaptureMessage<B>>>,
    pub(super) io_context: CaptureIoContext,
    pub(super) receiver: Arc<std::sync::Mutex<mpsc::Receiver<CaptureMessage<B>>>>,
    pub(super) state: Arc<CaptureState<B>>,
    pub(super) destination_fence: Option<Arc<CaptureDestinationLease>>,
    pub(super) completed: bool,
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
pub(super) struct CaptureWorkerFinalReport {
    pub(super) outcome: CaptureWriterOutcome,
    pub(super) shutdown_deadline_elapsed_at_exit: bool,
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
