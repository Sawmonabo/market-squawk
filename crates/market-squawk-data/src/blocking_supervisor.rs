//! Globally admitted ownership and non-blocking reaping for query file operations.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

#[cfg(test)]
use tokio::sync::{Mutex as TokioMutex, OwnedMutexGuard};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;

const MAX_QUERY_BLOCKING_WORKERS: usize = 64;
const _: () = assert!(MAX_QUERY_BLOCKING_WORKERS <= Semaphore::MAX_PERMITS);
static QUERY_BLOCKING_WORKERS: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(MAX_QUERY_BLOCKING_WORKERS)));
#[cfg(test)]
static QUERY_BLOCKING_WORKER_TEST_SERIAL: LazyLock<Arc<TokioMutex<()>>> =
    LazyLock::new(|| Arc::new(TokioMutex::new(())));

#[derive(Clone, Debug)]
pub(crate) struct BlockingIoSupervisor {
    inner: Arc<SupervisorInner>,
}

#[derive(Debug)]
struct SupervisorInner {
    cancellation: CancellationToken,
    active: AtomicUsize,
    idle: Notify,
    #[cfg(test)]
    range_barrier: std::sync::Mutex<Option<RangeWorkerBarrier>>,
}

#[derive(Debug)]
pub(crate) struct BlockingIoLease {
    inner: Arc<SupervisorInner>,
    _global_permit: OwnedSemaphorePermit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BlockingIoAdmissionError {
    Cancelled,
    Saturated,
}

#[derive(Debug)]
pub(crate) struct BlockingIoTask<T: Send + 'static> {
    worker: Option<JoinHandle<T>>,
}

impl BlockingIoSupervisor {
    pub(crate) fn new(cancellation: CancellationToken) -> Self {
        Self {
            inner: Arc::new(SupervisorInner {
                cancellation,
                active: AtomicUsize::new(0),
                idle: Notify::new(),
                #[cfg(test)]
                range_barrier: std::sync::Mutex::new(None),
            }),
        }
    }

    pub(crate) fn cancellation(&self) -> &CancellationToken {
        &self.inner.cancellation
    }

    pub(crate) fn spawn_blocking<F, T>(
        &self,
        operation: F,
    ) -> Result<BlockingIoTask<T>, BlockingIoAdmissionError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        if self.inner.cancellation.is_cancelled() {
            return Err(BlockingIoAdmissionError::Cancelled);
        }
        let global_permit = Arc::clone(&QUERY_BLOCKING_WORKERS)
            .try_acquire_owned()
            .map_err(|_| BlockingIoAdmissionError::Saturated)?;
        self.inner.active.fetch_add(1, Ordering::AcqRel);
        let lease = BlockingIoLease {
            inner: Arc::clone(&self.inner),
            _global_permit: global_permit,
        };
        if self.inner.cancellation.is_cancelled() {
            drop(lease);
            return Err(BlockingIoAdmissionError::Cancelled);
        }
        Ok(BlockingIoTask {
            worker: Some(tokio::task::spawn_blocking(move || {
                let _lease = lease;
                operation()
            })),
        })
    }

    pub(crate) fn cancel(&self) {
        self.inner.cancellation.cancel();
    }

    #[cfg(test)]
    pub(crate) fn globally_available() -> usize {
        QUERY_BLOCKING_WORKERS.available_permits()
    }

    #[cfg(test)]
    pub(crate) const fn global_limit() -> usize {
        MAX_QUERY_BLOCKING_WORKERS
    }

    #[cfg(test)]
    pub(crate) async fn acquire_test_serial_guard() -> OwnedMutexGuard<()> {
        Arc::clone(&QUERY_BLOCKING_WORKER_TEST_SERIAL)
            .lock_owned()
            .await
    }

    #[cfg(test)]
    pub(crate) async fn drain(&self) {
        loop {
            let idle = self.inner.idle.notified();
            if self.inner.active.load(Ordering::Acquire) == 0 {
                return;
            }
            idle.await;
        }
    }

    #[cfg(test)]
    pub(crate) fn active(&self) -> usize {
        self.inner.active.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn install_test_range_barrier(&self) -> Result<RangeTestBarrier, &'static str> {
        let (entered_sender, entered_receiver) = std::sync::mpsc::sync_channel(1);
        let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
        *self
            .inner
            .range_barrier
            .lock()
            .map_err(|_| "test range barrier mutex was poisoned")? = Some(RangeWorkerBarrier {
            entered_sender,
            release_receiver,
        });
        Ok(RangeTestBarrier {
            entered_receiver: Some(entered_receiver),
            release_sender,
        })
    }

    #[cfg(test)]
    pub(crate) fn wait_at_test_range_barrier(&self) -> Result<(), &'static str> {
        let barrier = self
            .inner
            .range_barrier
            .lock()
            .map_err(|_| "test range barrier mutex was poisoned")?
            .take();
        if let Some(barrier) = barrier {
            barrier
                .entered_sender
                .send(())
                .map_err(|_| "test range entry receiver was dropped")?;
            barrier
                .release_receiver
                .recv()
                .map_err(|_| "test range release sender was dropped")?;
        } else {
            return Ok(());
        }
        Ok(())
    }
}

impl<T: Send + 'static> Future for BlockingIoTask<T> {
    type Output = Result<T, JoinError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(worker) = self.worker.as_mut() else {
            return Poll::Pending;
        };
        match Pin::new(worker).poll(context) {
            Poll::Ready(result) => {
                self.worker = None;
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T: Send + 'static> Drop for BlockingIoTask<T> {
    fn drop(&mut self) {
        let Some(worker) = self.worker.take() else {
            return;
        };
        if worker.is_finished() {
            return;
        }
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ignored = worker.await;
            });
        }
    }
}

impl Drop for BlockingIoLease {
    fn drop(&mut self) {
        if self.inner.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.inner.idle.notify_waiters();
        }
    }
}

#[cfg(test)]
#[derive(Debug)]
struct RangeWorkerBarrier {
    entered_sender: std::sync::mpsc::SyncSender<()>,
    release_receiver: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct RangeTestBarrier {
    entered_receiver: Option<std::sync::mpsc::Receiver<()>>,
    release_sender: std::sync::mpsc::SyncSender<()>,
}

#[cfg(test)]
impl RangeTestBarrier {
    pub(crate) async fn wait_until_entered(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let receiver = self
            .entered_receiver
            .take()
            .ok_or("test range barrier was already entered")?;
        tokio::task::spawn_blocking(move || receiver.recv()).await??;
        Ok(())
    }

    pub(crate) fn release(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.release_sender.send(())?;
        Ok(())
    }
}
