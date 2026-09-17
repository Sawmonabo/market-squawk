//! Retained ownership of the three fixed source-startup publications.

use std::{
    future::{Future, poll_fn},
    pin::Pin,
    sync::Arc,
    task::Poll,
    time::Instant,
};

use market_squawk_services::ServiceError;
use parking_lot::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::application::ResearchApplicationServices;

pub(super) type StartupFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Retains outer publication futures until their own cancellation and blocking replay drains finish.
pub(crate) struct ProductStartupTasks {
    research: Arc<ResearchApplicationServices>,
    cancellation: CancellationToken,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    handles: Vec<JoinHandle<()>>,
    task_failed: bool,
}

impl ProductStartupTasks {
    /// Called only after every fallible local constructor has succeeded.
    pub(super) fn start(
        research: Arc<ResearchApplicationServices>,
        cancellation: CancellationToken,
        futures: [Option<StartupFuture>; 3],
    ) -> Arc<Self> {
        let owner = Arc::new(Self {
            research,
            cancellation,
            state: Mutex::new(State::default()),
        });
        let mut state = owner.state.lock();
        for future in futures.into_iter().flatten() {
            // External product/application Drop can cancel, but cannot destroy this owner while
            // the source is still joining a real blocking replay worker. Never abort this task.
            let retained = Arc::clone(&owner);
            state.handles.push(tokio::spawn(async move {
                future.await;
                drop(retained);
            }));
        }
        drop(state);
        owner
    }

    pub(crate) fn begin_shutdown(&self) {
        self.research.begin_startup_shutdown();
        self.cancellation.cancel();
    }

    pub(crate) fn is_drained(&self) -> bool {
        self.state.lock().handles.is_empty()
    }

    pub(crate) async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        self.begin_shutdown();
        tokio::time::timeout_at(
            deadline.into(),
            poll_fn(|cx| {
                let mut state = self.state.lock();
                let mut index = 0;
                while index < state.handles.len() {
                    match Pin::new(&mut state.handles[index]).poll(cx) {
                        Poll::Ready(result) => {
                            state.task_failed |= result.is_err();
                            drop(state.handles.swap_remove(index));
                        }
                        Poll::Pending => index += 1,
                    }
                }
                if state.handles.is_empty() {
                    Poll::Ready(if state.task_failed {
                        Err(ServiceError::Unavailable)
                    } else {
                        Ok(())
                    })
                } else {
                    Poll::Pending
                }
            }),
        )
        .await
        .map_err(|_| ServiceError::DeadlineExceeded)?
    }
}

impl std::fmt::Debug for ProductStartupTasks {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProductStartupTasks")
            .field("retained_tasks", &self.state.lock().handles.len())
            .finish_non_exhaustive()
    }
}

impl Drop for ProductStartupTasks {
    fn drop(&mut self) {
        self.cancellation.cancel();
        // Every started future retained this owner through its final await. Handles here can only
        // represent completed outer tasks, whose blocking replay was joined by the source owner.
        // Raw-capture recovery workers separately retain data::BlockingIoSupervisor reaper leases.
    }
}
