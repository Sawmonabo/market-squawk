//! Repository operation admission, cancellation, and bounded drain.

use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use market_squawk_services::ServiceError;
use tokio::{sync::Notify, task::JoinHandle};
use tokio_util::sync::CancellationToken;

const SHUTDOWN_BIT: usize = 1_usize << (usize::BITS - 1);
const ACTIVE_MASK: usize = SHUTDOWN_BIT - 1;

pub(super) struct RepositoryLifecycle {
    state: AtomicUsize,
    shutdown: CancellationToken,
    drained: Notify,
}

impl RepositoryLifecycle {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: AtomicUsize::new(0),
            shutdown: CancellationToken::new(),
            drained: Notify::new(),
        })
    }

    pub(super) fn enter(
        lifecycle: &Arc<Self>,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<RepositoryCall, ServiceError> {
        ensure_operation_live(cancellation, lifecycle, deadline)?;
        let mut current = lifecycle.state.load(Ordering::Acquire);
        loop {
            if current & SHUTDOWN_BIT != 0 {
                return Err(ServiceError::Unavailable);
            }
            if current & ACTIVE_MASK == ACTIVE_MASK {
                return Err(ServiceError::ResourceExhausted);
            }
            match lifecycle.state.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(RepositoryCall {
                        lifecycle: Arc::clone(lifecycle),
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }

    pub(super) fn begin_shutdown(&self) {
        let previous = self.state.fetch_or(SHUTDOWN_BIT, Ordering::AcqRel);
        self.shutdown.cancel();
        if previous & ACTIVE_MASK == 0 {
            self.drained.notify_waiters();
        }
    }

    pub(super) async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        self.begin_shutdown();
        let deadline = tokio::time::Instant::from_std(deadline);
        loop {
            if self.state.load(Ordering::Acquire) & ACTIVE_MASK == 0 {
                return Ok(());
            }
            let notified = self.drained.notified();
            if self.state.load(Ordering::Acquire) & ACTIVE_MASK == 0 {
                return Ok(());
            }
            tokio::time::timeout_at(deadline, notified)
                .await
                .map_err(|_| ServiceError::DeadlineExceeded)?;
        }
    }

    pub(super) const fn shutdown_token(&self) -> &CancellationToken {
        &self.shutdown
    }

    fn leave(&self) {
        let previous = self.state.fetch_sub(1, Ordering::AcqRel);
        if previous & ACTIVE_MASK == 1 {
            self.drained.notify_waiters();
        }
    }
}

impl fmt::Debug for RepositoryLifecycle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.load(Ordering::Acquire);
        formatter
            .debug_struct("RepositoryLifecycle")
            .field("accepting_operations", &(state & SHUTDOWN_BIT == 0))
            .field("active_operations", &(state & ACTIVE_MASK))
            .finish()
    }
}

pub(super) struct RepositoryCall {
    lifecycle: Arc<RepositoryLifecycle>,
}

impl Drop for RepositoryCall {
    fn drop(&mut self) {
        self.lifecycle.leave();
    }
}

pub(super) fn ensure_operation_live(
    cancellation: &CancellationToken,
    lifecycle: &RepositoryLifecycle,
    deadline: Instant,
) -> Result<(), ServiceError> {
    if cancellation.is_cancelled() {
        return Err(ServiceError::Cancelled);
    }
    if Instant::now() >= deadline {
        return Err(ServiceError::DeadlineExceeded);
    }
    if lifecycle.shutdown_token().is_cancelled() {
        return Err(ServiceError::Unavailable);
    }
    Ok(())
}

pub(super) async fn await_blocking<T>(
    mut worker: JoinHandle<Result<T, ServiceError>>,
    cancellation: &CancellationToken,
    shutdown: &CancellationToken,
    deadline: Instant,
) -> Result<T, ServiceError> {
    tokio::select! {
        biased;
        result = &mut worker => result.map_err(|_| ServiceError::Internal)?,
        () = cancellation.cancelled() => Err(ServiceError::Cancelled),
        () = shutdown.cancelled() => Err(ServiceError::Unavailable),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            Err(ServiceError::DeadlineExceeded)
        }
    }
}

pub(super) struct LinkedOperation {
    token: CancellationToken,
    monitor: JoinHandle<()>,
}

impl LinkedOperation {
    pub(super) fn new(
        request: CancellationToken,
        shutdown: CancellationToken,
        deadline: Instant,
    ) -> Self {
        let token = CancellationToken::new();
        let monitored = token.clone();
        let monitor = tokio::spawn(async move {
            tokio::select! {
                () = request.cancelled() => monitored.cancel(),
                () = shutdown.cancelled() => monitored.cancel(),
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                    monitored.cancel();
                }
            }
        });
        Self { token, monitor }
    }

    pub(super) const fn token(&self) -> &CancellationToken {
        &self.token
    }
}

impl Drop for LinkedOperation {
    fn drop(&mut self) {
        self.token.cancel();
        self.monitor.abort();
    }
}
