//! Query-scoped ownership and draining for admitted blocking file operations.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

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

    pub(crate) fn start(&self) -> Option<BlockingIoLease> {
        if self.inner.cancellation.is_cancelled() {
            return None;
        }
        self.inner.active.fetch_add(1, Ordering::AcqRel);
        let lease = BlockingIoLease {
            inner: Arc::clone(&self.inner),
        };
        if self.inner.cancellation.is_cancelled() {
            drop(lease);
            None
        } else {
            Some(lease)
        }
    }

    pub(crate) async fn drain(&self) {
        loop {
            let idle = self.inner.idle.notified();
            if self.inner.active.load(Ordering::Acquire) == 0 {
                return;
            }
            idle.await;
        }
    }

    pub(crate) async fn cancel_and_drain(&self) {
        self.inner.cancellation.cancel();
        self.drain().await;
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
        }
        Ok(())
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
