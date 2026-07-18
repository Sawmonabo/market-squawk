//! Benchmark-only bounded shutdown and pending-worker ownership.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use market_squawk_domain::CaptureAuthorityBundle;

use super::destination::CaptureDestinationLease;
use super::runtime::WriterFixedStorageOwner;
use super::sink::{CaptureIoContext, CaptureSink};
use super::{
    CaptureWriterSpawnError, SpawnedCaptureWriter, deadline_after, lifecycle,
    spawn_capture_writer_core,
};
use crate::capture::transport::CaptureQueueTransport;
use crate::capture::{
    BenchmarkCaptureWriter, CaptureHealthReason, CaptureMessage, CaptureState, CaptureWriterPolicy,
    SelectedBenchmarkTransport,
};

#[derive(Debug)]
pub(in crate::capture) struct BenchmarkCaptureWriterHandle<B: CaptureAuthorityBundle> {
    spawned: SpawnedCaptureWriter<B, SelectedBenchmarkTransport>,
    completed: bool,
}

/// Bounded benchmark-only shutdown result.
#[derive(Debug)]
#[must_use = "benchmark shutdown must retain or reap any deadline-surviving worker"]
pub(in crate::capture) enum BenchmarkCaptureWriterShutdown<B: CaptureAuthorityBundle> {
    /// The worker exited and was joined before the configured deadline.
    Terminated(lifecycle::CaptureWorkerTermination),
    /// The deadline revoked authority while explicit join ownership remains with the caller.
    DeadlineElapsed(PendingBenchmarkCaptureWriter<B>),
    /// Queue shutdown failed after revocation; explicit join ownership remains with the caller.
    ControlFailed(PendingBenchmarkCaptureWriter<B>),
}

/// Explicit owner for a benchmark worker that outlived its shutdown deadline.
///
/// Unlike the production pending owner, dropping this benchmark-only owner never joins. Benchmark
/// execution is contained in a host-supervised subprocess, so an unreaped worker is detached inside
/// that process and cannot make timeout reporting itself unbounded.
#[derive(Debug)]
#[must_use = "pending benchmark workers must be reaped or deliberately process-contained"]
pub(in crate::capture) struct PendingBenchmarkCaptureWriter<B: CaptureAuthorityBundle> {
    thread: Option<std::thread::JoinHandle<()>>,
    queue_control:
        <SelectedBenchmarkTransport as CaptureQueueTransport>::Control<CaptureMessage<B>>,
    io_context: CaptureIoContext,
    state: Arc<CaptureState<B>>,
    destination_fence: Option<Arc<CaptureDestinationLease>>,
    fixed_storage: Option<Arc<WriterFixedStorageOwner>>,
    records_written_at_revocation: u64,
    deadline_elapsed: bool,
    incomplete_reason: CaptureHealthReason,
    termination: Option<lifecycle::CaptureWorkerTermination>,
}

pub(in crate::capture) fn spawn_benchmark_capture_writer<
    B: CaptureAuthorityBundle,
    S: CaptureSink,
>(
    writer: BenchmarkCaptureWriter<B>,
    sink: S,
    policy: CaptureWriterPolicy,
) -> Result<BenchmarkCaptureWriterHandle<B>, CaptureWriterSpawnError> {
    Ok(BenchmarkCaptureWriterHandle {
        spawned: spawn_capture_writer_core(writer, sink, policy)?,
        completed: false,
    })
}

impl<B: CaptureAuthorityBundle> BenchmarkCaptureWriterHandle<B> {
    fn take_pending(
        &mut self,
        records_written_at_revocation: u64,
        deadline_elapsed: bool,
        incomplete_reason: CaptureHealthReason,
    ) -> PendingBenchmarkCaptureWriter<B> {
        self.completed = true;
        PendingBenchmarkCaptureWriter {
            thread: self.spawned.thread.take(),
            queue_control: self.spawned.queue_control.clone(),
            io_context: self.spawned.io_context.clone(),
            state: Arc::clone(&self.spawned.state),
            destination_fence: self.spawned.destination_fence.take(),
            fixed_storage: self.spawned.fixed_storage.take(),
            records_written_at_revocation,
            deadline_elapsed,
            incomplete_reason,
            termination: None,
        }
    }

    #[cfg(test)]
    pub(in crate::capture) fn with_receiver_paused_for_test<R>(
        &self,
        timeout: Duration,
        action: impl FnOnce() -> R,
    ) -> Result<R, lifecycle::CaptureReceiverTestCoordinationError> {
        self.spawned
            .queue_control
            .with_receiver_paused_for_test(timeout, action)
            .map_err(|error| match error {
                crate::capture::queue::ReceiverPauseError::Poisoned => {
                    lifecycle::CaptureReceiverTestCoordinationError::Poisoned
                }
                crate::capture::queue::ReceiverPauseError::DeadlineElapsed => {
                    lifecycle::CaptureReceiverTestCoordinationError::DeadlineElapsed
                }
            })
    }

    pub(in crate::capture) fn shutdown_and_join(
        mut self,
        timeout: Duration,
    ) -> Result<BenchmarkCaptureWriterShutdown<B>, CaptureWriterSpawnError> {
        let deadline = deadline_after(timeout);
        match self.spawned.io_context.lifecycle.shutdown_deadline.lock() {
            Ok(mut configured) => *configured = Some(deadline),
            Err(poisoned) => *poisoned.into_inner() = Some(deadline),
        }
        let revocation = self
            .spawned
            .state
            .revoke_writer_for_shutdown(CaptureHealthReason::WriterStopped);
        self.spawned
            .io_context
            .lifecycle
            .shutdown_requested
            .store(true, Ordering::Release);
        if <SelectedBenchmarkTransport as CaptureQueueTransport>::request_close(
            &self.spawned.queue_control,
        )
        .is_err()
        {
            self.spawned
                .state
                .mark_current_incomplete(CaptureHealthReason::QueuePoisoned);
            let pending = self.take_pending(
                revocation.records_written_at_revocation,
                false,
                CaptureHealthReason::QueuePoisoned,
            );
            return Ok(BenchmarkCaptureWriterShutdown::ControlFailed(pending));
        }
        while self
            .spawned
            .thread
            .as_ref()
            .is_some_and(|thread| !thread.is_finished())
        {
            if std::time::Instant::now() >= deadline {
                self.spawned
                    .state
                    .mark_current_incomplete(CaptureHealthReason::ShutdownDeadline);
                let pending = self.take_pending(
                    revocation.records_written_at_revocation,
                    true,
                    CaptureHealthReason::ShutdownDeadline,
                );
                return Ok(BenchmarkCaptureWriterShutdown::DeadlineElapsed(pending));
            }
            std::thread::yield_now();
        }
        let joined = self
            .spawned
            .thread
            .take()
            .is_none_or(|thread| thread.join().is_ok());
        let termination = lifecycle::termination_after_join(
            &self.spawned.state,
            &self.spawned.io_context.lifecycle.final_report,
            revocation.records_written_at_revocation,
            false,
            joined,
        );
        self.completed = true;
        self.spawned.destination_fence.take();
        self.spawned.fixed_storage.take();
        Ok(BenchmarkCaptureWriterShutdown::Terminated(termination))
    }
}

impl<B: CaptureAuthorityBundle> PendingBenchmarkCaptureWriter<B> {
    pub(in crate::capture) fn is_worker_terminated(&self) -> bool {
        self.thread
            .as_ref()
            .is_none_or(std::thread::JoinHandle::is_finished)
    }

    pub(in crate::capture) fn try_reap(
        &mut self,
    ) -> Result<Option<&lifecycle::CaptureWorkerTermination>, lifecycle::CaptureWorkerReapError>
    {
        if self.termination.is_some() {
            return Ok(self.termination.as_ref());
        }
        let Some(thread) = self.thread.as_ref() else {
            return Ok(None);
        };
        if !thread.is_finished() {
            return Err(lifecycle::CaptureWorkerReapError::WorkerStillRunning);
        }
        let joined = self
            .thread
            .take()
            .is_some_and(|thread| thread.join().is_ok());
        self.termination = Some(lifecycle::termination_after_join(
            &self.state,
            &self.io_context.lifecycle.final_report,
            self.records_written_at_revocation,
            self.deadline_elapsed,
            joined,
        ));
        self.destination_fence.take();
        self.fixed_storage.take();
        Ok(self.termination.as_ref())
    }

    #[cfg(test)]
    pub(in crate::capture) fn retains_owner_storage_for_test(&self) -> bool {
        self.destination_fence.is_some() && self.fixed_storage.is_some()
    }
}

impl<B: CaptureAuthorityBundle> Drop for PendingBenchmarkCaptureWriter<B> {
    fn drop(&mut self) {
        if self.termination.is_some() {
            return;
        }
        self.io_context
            .lifecycle
            .shutdown_requested
            .store(true, Ordering::Release);
        let _closed = <SelectedBenchmarkTransport as CaptureQueueTransport>::request_close(
            &self.queue_control,
        );
        // Dropping a live JoinHandle detaches; it never waits for the blocked sink operation.
        self.thread.take();
        self.state.mark_current_incomplete(self.incomplete_reason);
        self.destination_fence.take();
        self.fixed_storage.take();
    }
}

impl<B: CaptureAuthorityBundle> Drop for BenchmarkCaptureWriterHandle<B> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        self.spawned
            .io_context
            .lifecycle
            .shutdown_requested
            .store(true, Ordering::Release);
        let _closed = <SelectedBenchmarkTransport as CaptureQueueTransport>::request_close(
            &self.spawned.queue_control,
        );
        // The benchmark runs in a host-supervised subprocess. Dropping the live JoinHandle detaches
        // instead of turning an error path into an unbounded synchronous wait.
        self.spawned.thread.take();
        self.spawned
            .state
            .mark_current_incomplete(CaptureHealthReason::WriterFailed);
        self.spawned.destination_fence.take();
        self.spawned.fixed_storage.take();
    }
}
