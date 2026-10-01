//! Retained custody of the existing single synchronous research I/O lane.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
    thread::{self, Thread},
    time::Instant,
};

use market_squawk_data::IngestError;
use tokio::{
    sync::{Mutex, Semaphore, oneshot},
    task::{JoinError, JoinHandle},
};
use tokio_util::sync::CancellationToken;

use super::ResearchServiceError;

/// The one-permit limit is the existing capture-seal lane, shared with retained reads.
#[derive(Debug)]
pub(super) struct ResearchIoWorker {
    gate: Arc<Semaphore>,
    state: Mutex<State>,
    shutdown: CancellationToken,
}

#[derive(Debug, Default)]
struct State {
    worker: Option<JoinHandle<()>>,
    first_join_error: Option<JoinError>,
}

// The operation label is its compile-time closure type, never captured provider/request data.
// Drop covers a caller destroying its future while the owned worker remains in State.
struct WaitDiagnostic {
    operation: &'static str,
    stage: &'static str,
    completed: bool,
}

impl Drop for WaitDiagnostic {
    fn drop(&mut self) {
        if !self.completed {
            tracing::warn!(
                operation = self.operation,
                stage = self.stage,
                "owned research I/O wait interrupted"
            );
        }
    }
}

impl ResearchIoWorker {
    pub(super) fn new() -> Self {
        Self {
            gate: Arc::new(Semaphore::new(1)),
            state: Mutex::new(State::default()),
            shutdown: CancellationToken::new(),
        }
    }

    pub(super) async fn run<T, F>(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
        operation: F,
    ) -> Result<T, ResearchServiceError>
    where
        T: Send + 'static,
        F: FnOnce(CancellationToken) -> T + Send + 'static,
    {
        self.run_with_job_context(None, deadline, cancellation, operation)
            .await
    }

    /// An admitted job wrapper returns cancellation only after its original worker joins.
    /// Runner abort can drop that wrapper; the original handle remains in this owned slot.
    /// The same state guard owns cooperative cancellation joins; later work cannot replace it.
    pub(super) async fn run_with_job_context<T, F>(
        &self,
        job_cancellation: Option<&CancellationToken>,
        deadline: Instant,
        cancellation: &CancellationToken,
        operation: F,
    ) -> Result<T, ResearchServiceError>
    where
        T: Send + 'static,
        F: FnOnce(CancellationToken) -> T + Send + 'static,
    {
        let mut diagnostic = WaitDiagnostic {
            operation: std::any::type_name::<F>(),
            stage: "gate_admission",
            completed: false,
        };
        let operation_cancellation = self.shutdown.child_token();
        let _cancel_on_drop = operation_cancellation.clone().drop_guard();
        let deadline = tokio::time::Instant::from_std(deadline);
        let permit = wait(
            deadline,
            cancellation,
            job_cancellation,
            &operation_cancellation,
            Arc::clone(&self.gate).acquire_owned(),
        )
        .await?
        .map_err(|_| ResearchServiceError::ProviderCaptureSealWorkerUnavailable)?;
        diagnostic.stage = "state_lock";
        let mut state = wait(
            deadline,
            cancellation,
            job_cancellation,
            &operation_cancellation,
            self.state.lock(),
        )
        .await?;
        diagnostic.stage = "prior_join";
        // An abandoned request can leave a finished handle in this slot. Join that exact worker
        // before starting the next operation; never overwrite its result or failure.
        wait(
            deadline,
            cancellation,
            job_cancellation,
            &operation_cancellation,
            state.join(),
        )
        .await??;
        let (sender, mut result) = oneshot::channel();
        let worker_cancellation = operation_cancellation.clone();
        state.worker = Some(tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let output = operation(worker_cancellation);
            // A cancelled read no longer needs its typed output. A late sealed capture remains
            // durable and unreferenced for the existing startup quarantine pass.
            let _unclaimed_output = sender.send(output);
        }));
        diagnostic.stage = "current_join";
        match wait(
            deadline,
            cancellation,
            job_cancellation,
            &operation_cancellation,
            state.join(),
        )
        .await
        {
            Ok(joined) => joined?,
            Err(interrupted) => {
                operation_cancellation.cancel();
                if job_cancellation.is_some() {
                    // This drains already admitted work; it does not extend its operation
                    // deadline or permit more publication. Native filesystem completion may
                    // outlive that deadline. A dropped waiter still leaves this exact slot owned.
                    state.join().await?;
                }
                return Err(interrupted);
            }
        }
        if cancellation.is_cancelled()
            || job_cancellation.is_some_and(CancellationToken::is_cancelled)
            || operation_cancellation.is_cancelled()
        {
            return Err(IngestError::Cancelled.into());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(IngestError::DeadlineExceeded.into());
        }
        // Receiving a result alone cannot attest that the original thread joined. This read is
        // synchronous and occurs only after the actual JoinHandle returned successfully.
        let output = result
            .try_recv()
            .map_err(|_| ResearchServiceError::ProviderCaptureSealWorkerUnavailable);
        diagnostic.completed = output.is_ok();
        output
    }

    pub(super) fn begin_shutdown(&self) {
        self.shutdown.cancel();
        self.gate.close();
    }

    pub(super) async fn finish_shutdown(
        &self,
        deadline: Instant,
    ) -> Result<(), ResearchServiceError> {
        self.begin_shutdown();
        let deadline = tokio::time::Instant::from_std(deadline);
        let mut state = tokio::time::timeout_at(deadline, self.state.lock())
            .await
            .map_err(|_| IngestError::DeadlineExceeded)?;
        // Timeout drops only a borrow of the slot. A later shutdown resumes this same handle.
        tokio::time::timeout_at(deadline, state.join())
            .await
            .map_err(|_| IngestError::DeadlineExceeded)?
    }
}

impl State {
    async fn join(&mut self) -> Result<(), ResearchServiceError> {
        if let Some(worker) = self.worker.as_mut() {
            let joined = worker.await;
            self.record_join(joined);
        }
        if self.first_join_error.is_some() {
            Err(ResearchServiceError::ProviderCaptureSealWorkerUnavailable)
        } else {
            Ok(())
        }
    }

    fn record_join(&mut self, joined: Result<(), JoinError>) {
        self.worker = None;
        if let Err(error) = joined {
            self.first_join_error.get_or_insert(error);
        }
    }
}

async fn wait<T>(
    deadline: tokio::time::Instant,
    caller: &CancellationToken,
    job: Option<&CancellationToken>,
    operation: &CancellationToken,
    future: impl Future<Output = T>,
) -> Result<T, ResearchServiceError> {
    tokio::select! {
        biased;
        () = caller.cancelled() => Err(IngestError::Cancelled.into()),
        () = async {
            match job {
                Some(job) => job.cancelled().await,
                None => std::future::pending::<()>().await,
            }
        } => Err(IngestError::Cancelled.into()),
        () = operation.cancelled() => Err(IngestError::Cancelled.into()),
        () = tokio::time::sleep_until(deadline) => Err(IngestError::DeadlineExceeded.into()),
        result = future => Ok(result),
    }
}

struct CompletionWake(Thread);

impl Wake for CompletionWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

impl Drop for ResearchIoWorker {
    fn drop(&mut self) {
        self.begin_shutdown();
        let state = self.state.get_mut();
        let waker = Waker::from(Arc::new(CompletionWake(thread::current())));
        let mut context = Context::from_waker(&waker);
        while let Some(worker) = state.worker.as_mut() {
            match Pin::new(worker).poll(&mut context) {
                Poll::Ready(joined) => state.record_join(joined),
                // The worker is synchronous native work and owns no ResearchService Arc. Its
                // completion wakes this parked thread; no runtime, detached reaper or busy poll
                // is needed. Final Drop may wait on unavoidable filesystem completion. Bounded
                // product shutdown must retain this owner and retry finish_shutdown instead.
                Poll::Pending => thread::park(),
            }
        }
    }
}
