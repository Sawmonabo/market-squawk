//! Globally admitted ownership and non-blocking reaping for query file operations.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{SyncSender, sync_channel};
use std::sync::{Arc, LazyLock};
use std::task::{Context, Poll};
use std::thread::JoinHandle as ThreadJoinHandle;

use thiserror::Error;
#[cfg(test)]
use tokio::sync::{Mutex as TokioMutex, OwnedMutexGuard};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, oneshot};
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;

const MAX_QUERY_BLOCKING_WORKERS: usize = 64;
const _: () = assert!(MAX_QUERY_BLOCKING_WORKERS <= Semaphore::MAX_PERMITS);
static QUERY_BLOCKING_WORKERS: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(MAX_QUERY_BLOCKING_WORKERS)));
static BLOCKING_TASK_REAPER: LazyLock<BlockingTaskReaper> =
    LazyLock::new(BlockingTaskReaper::start);
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

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum BlockingIoAdmissionError {
    #[error("blocking I/O was cancelled before admission")]
    Cancelled,
    #[error("blocking I/O worker or reaper capacity is saturated")]
    Saturated,
    #[error("blocking I/O reaper could not be started")]
    ReaperUnavailable,
}

#[derive(Debug)]
pub(crate) struct BlockingIoTask<T: Send + 'static> {
    command: Option<ReapCommand>,
    result: oneshot::Receiver<T>,
}

#[derive(Debug, Error)]
pub(crate) enum BlockingIoTaskError {
    #[error("blocking I/O worker failed")]
    Join(#[from] JoinError),
    #[error("blocking I/O worker closed without returning its result")]
    ResultChannelClosed,
}

#[derive(Debug)]
struct ReapCommand {
    worker: JoinHandle<()>,
    _permit: OwnedSemaphorePermit,
}

struct BlockingTaskReaper {
    sender: Option<SyncSender<ReapCommand>>,
    capacity: Arc<Semaphore>,
    pending: Arc<AtomicUsize>,
    idle: Arc<Notify>,
    _thread: Option<ThreadJoinHandle<()>>,
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
        let reaper_permit = BLOCKING_TASK_REAPER.try_reserve()?;
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
        let (result_sender, result) = oneshot::channel();
        Ok(BlockingIoTask {
            command: Some(ReapCommand {
                worker: tokio::task::spawn_blocking(move || {
                    let _lease = lease;
                    let outcome = operation();
                    let _ignored = result_sender.send(outcome);
                }),
                _permit: reaper_permit,
            }),
            result,
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
                break;
            }
            idle.await;
        }
        BLOCKING_TASK_REAPER.drain().await;
    }

    #[cfg(test)]
    pub(crate) fn active(&self) -> usize {
        self.inner.active.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn reaper_pending() -> usize {
        BLOCKING_TASK_REAPER.pending.load(Ordering::Acquire)
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
    type Output = Result<T, BlockingIoTaskError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.result).poll(context) {
            Poll::Ready(Ok(result)) => {
                self.handoff_pending_worker();
                Poll::Ready(Ok(result))
            }
            Poll::Ready(Err(_closed)) => {
                let Some(command) = self.command.as_mut() else {
                    return Poll::Ready(Err(BlockingIoTaskError::ResultChannelClosed));
                };
                match Pin::new(&mut command.worker).poll(context) {
                    Poll::Ready(result) => {
                        self.command = None;
                        match result {
                            Ok(()) => Poll::Ready(Err(BlockingIoTaskError::ResultChannelClosed)),
                            Err(error) => Poll::Ready(Err(error.into())),
                        }
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T: Send + 'static> BlockingIoTask<T> {
    fn handoff_pending_worker(&mut self) {
        let Some(command) = self.command.take() else {
            return;
        };
        if command.worker.is_finished() {
            return;
        }
        BLOCKING_TASK_REAPER.reap(command);
    }
}

impl<T: Send + 'static> Drop for BlockingIoTask<T> {
    fn drop(&mut self) {
        self.handoff_pending_worker();
    }
}

impl BlockingTaskReaper {
    fn start() -> Self {
        let capacity = Arc::new(Semaphore::new(MAX_QUERY_BLOCKING_WORKERS));
        let pending = Arc::new(AtomicUsize::new(0));
        let idle = Arc::new(Notify::new());
        let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        else {
            return Self {
                sender: None,
                capacity,
                pending,
                idle,
                _thread: None,
            };
        };
        let (sender, receiver) = sync_channel::<ReapCommand>(MAX_QUERY_BLOCKING_WORKERS);
        let thread_pending = Arc::clone(&pending);
        let thread_idle = Arc::clone(&idle);
        let thread = std::thread::Builder::new()
            .name("market-squawk-blocking-reaper".to_owned())
            .spawn(move || {
                while let Ok(command) = receiver.recv() {
                    let _outcome = runtime.block_on(command.worker);
                    drop(command._permit);
                    if thread_pending.fetch_sub(1, Ordering::AcqRel) == 1 {
                        thread_idle.notify_waiters();
                    }
                }
            });
        match thread {
            Ok(thread) => Self {
                sender: Some(sender),
                capacity,
                pending,
                idle,
                _thread: Some(thread),
            },
            Err(_error) => Self {
                sender: None,
                capacity,
                pending,
                idle,
                _thread: None,
            },
        }
    }

    fn try_reserve(&self) -> Result<OwnedSemaphorePermit, BlockingIoAdmissionError> {
        if self.sender.is_none() {
            return Err(BlockingIoAdmissionError::ReaperUnavailable);
        }
        Arc::clone(&self.capacity)
            .try_acquire_owned()
            .map_err(|_| BlockingIoAdmissionError::Saturated)
    }

    fn reap(&self, command: ReapCommand) {
        self.pending.fetch_add(1, Ordering::AcqRel);
        let Some(sender) = self.sender.as_ref() else {
            self.reap_without_worker_thread(command);
            return;
        };
        if let Err(error) = sender.send(command) {
            self.reap_without_worker_thread(error.0);
        }
    }

    fn reap_without_worker_thread(&self, mut command: ReapCommand) {
        let waker = futures_util::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        while Pin::new(&mut command.worker)
            .poll(&mut context)
            .is_pending()
        {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        drop(command._permit);
        if self.pending.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.idle.notify_waiters();
        }
    }

    #[cfg(test)]
    async fn drain(&self) {
        loop {
            let idle = self.idle.notified();
            if self.pending.load(Ordering::Acquire) == 0 {
                return;
            }
            idle.await;
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

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::{Duration, Instant};

    use super::BlockingIoSupervisor;
    use tokio_util::sync::CancellationToken;

    type TestResult = Result<(), Box<dyn Error>>;

    #[test]
    fn no_runtime_drop_retains_and_reaps_the_origin_runtime_handle() -> TestResult {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;
        let serial = runtime.block_on(BlockingIoSupervisor::acquire_test_serial_guard());
        let supervisor = BlockingIoSupervisor::new(CancellationToken::new());
        let (entered_sender, entered_receiver) = std::sync::mpsc::sync_channel(1);
        let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
        let task = runtime
            .block_on(async {
                supervisor.spawn_blocking(move || {
                    let _ignored = entered_sender.send(());
                    let _ignored = release_receiver.recv();
                })
            })
            .map_err(|_| "blocking worker was not admitted")?;
        entered_receiver.recv_timeout(Duration::from_secs(1))?;
        runtime.shutdown_background();

        drop(task);
        wait_until(Duration::from_secs(1), || {
            BlockingIoSupervisor::reaper_pending() == 1
        })?;
        release_sender.send(())?;
        wait_until(Duration::from_secs(1), || {
            BlockingIoSupervisor::reaper_pending() == 0 && supervisor.active() == 0
        })?;
        drop(serial);
        Ok(())
    }

    fn wait_until(
        timeout: Duration,
        mut predicate: impl FnMut() -> bool,
    ) -> Result<(), &'static str> {
        let deadline = Instant::now() + timeout;
        while !predicate() {
            if Instant::now() >= deadline {
                return Err("timed out waiting for blocking-worker reaper");
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        Ok(())
    }
}
