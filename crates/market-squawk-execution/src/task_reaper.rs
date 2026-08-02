use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::mpsc::{SyncSender, sync_channel};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard};
use std::thread::JoinHandle as ThreadJoinHandle;

use thiserror::Error;
use tokio::runtime::Handle;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

const MAX_TERMINAL_EXECUTION_TASKS: usize = 4_096;
const _: () = assert!(MAX_TERMINAL_EXECUTION_TASKS <= Semaphore::MAX_PERMITS);
static TERMINAL_TASK_REAPER: LazyLock<TerminalTaskReaper> =
    LazyLock::new(TerminalTaskReaper::start);

/// Fixed-capacity owner for adapter and worker tasks that outlive their immediate caller.
#[derive(Clone, Debug)]
pub struct ExecutionTaskReaper {
    inner: Arc<ReaperInner>,
}

impl ExecutionTaskReaper {
    /// Allocates every ownership slot before task admission begins.
    pub fn try_new(maximum_tasks: NonZeroUsize) -> Result<Self, ExecutionTaskReaperError> {
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(maximum_tasks.get())
            .map_err(|_| ExecutionTaskReaperError::Allocation)?;
        for _ in 0..maximum_tasks.get() {
            slots.push(Arc::new(Mutex::new(TaskSlot::Available)));
        }
        TERMINAL_TASK_REAPER.ensure_available()?;
        Ok(Self {
            inner: Arc::new(ReaperInner {
                slots: slots.into_boxed_slice(),
                changed: Notify::new(),
            }),
        })
    }

    /// Reserves one exact ownership slot before spawning work.
    pub fn try_reserve(&self) -> Result<ExecutionTaskPermit, ExecutionTaskReaperError> {
        for slot in &self.inner.slots {
            let mut state = lock_slot(slot);
            if matches!(*state, TaskSlot::Available) {
                *state = TaskSlot::Reserved;
                drop(state);
                let terminal_permit = match TERMINAL_TASK_REAPER.try_reserve() {
                    Ok(permit) => permit,
                    Err(error) => {
                        set_slot(&self.inner, slot, TaskSlot::Available);
                        return Err(error);
                    }
                };
                return Ok(ExecutionTaskPermit {
                    inner: Arc::clone(&self.inner),
                    slot: Arc::clone(slot),
                    terminal_permit: Some(terminal_permit),
                    armed: true,
                });
            }
        }
        Err(ExecutionTaskReaperError::Saturated)
    }

    /// Returns task handles currently retained after caller timeout or cancellation.
    pub fn retained_task_count(&self) -> usize {
        self.inner
            .slots
            .iter()
            .filter(|slot| matches!(*lock_slot(slot), TaskSlot::Retained(_) | TaskSlot::Draining))
            .count()
    }

    /// Drains every retained or still-reserved task until the absolute deadline.
    pub async fn drain(&self, deadline: Instant) -> ExecutionTaskDrain {
        let mut completed = 0_usize;
        loop {
            if let Some(mut retained) = take_retained(&self.inner) {
                let Some(handle) = retained.handle() else {
                    retained.complete();
                    continue;
                };
                match tokio::time::timeout_at(deadline, handle).await {
                    Ok(_) => {
                        completed = completed.saturating_add(1);
                        retained.complete();
                    }
                    Err(_) => {
                        return drain_outcome(&self.inner, completed, true);
                    }
                }
                continue;
            }
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let outcome = drain_outcome(&self.inner, completed, false);
            if outcome.is_complete() {
                return outcome;
            }
            if Instant::now() >= deadline {
                return drain_outcome(&self.inner, completed, true);
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return drain_outcome(&self.inner, completed, true);
            }
        }
    }
}

/// One non-cloneable pre-spawn task-ownership reservation.
#[derive(Debug)]
pub struct ExecutionTaskPermit {
    inner: Arc<ReaperInner>,
    slot: Arc<Mutex<TaskSlot>>,
    terminal_permit: Option<OwnedSemaphorePermit>,
    armed: bool,
}

impl ExecutionTaskPermit {
    /// Spawns one erased owned task on the current runtime while retaining its result separately.
    pub fn spawn<F, T>(self, future: F) -> Result<ExecutionTask<T>, ExecutionTaskReaperError>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let runtime =
            Handle::try_current().map_err(|_| ExecutionTaskReaperError::RuntimeUnavailable)?;
        let (output, receiver) = oneshot::channel();
        let handle = runtime.spawn(async move {
            let result = future.await;
            let _ = output.send(result);
        });
        Ok(ExecutionTask {
            handle: Some(handle),
            output: Some(receiver),
            permit: Some(self),
        })
    }

    fn transfer(mut self, handle: JoinHandle<()>) {
        let Some(terminal_permit) = self.terminal_permit.take() else {
            return;
        };
        set_slot(
            &self.inner,
            &self.slot,
            TaskSlot::Retained(ReapCommand {
                worker: handle,
                _permit: terminal_permit,
            }),
        );
        self.armed = false;
    }
}

impl Drop for ExecutionTaskPermit {
    fn drop(&mut self) {
        if self.armed {
            set_slot(&self.inner, &self.slot, TaskSlot::Available);
            self.armed = false;
        }
    }
}

/// One spawned task whose handle transfers automatically to its pre-reserved reaper slot.
#[derive(Debug)]
pub struct ExecutionTask<T> {
    handle: Option<JoinHandle<()>>,
    output: Option<oneshot::Receiver<T>>,
    permit: Option<ExecutionTaskPermit>,
}

impl<T> ExecutionTask<T> {
    /// Awaits both result delivery and task termination, releasing capacity only afterward.
    pub async fn join(&mut self) -> Result<T, ExecutionTaskReaperError> {
        let output = match self.output.as_mut() {
            Some(receiver) => receiver.await,
            None => return Err(ExecutionTaskReaperError::OutcomeLost),
        };
        let joined = match self.handle.take() {
            Some(handle) => handle.await,
            None => return Err(ExecutionTaskReaperError::OutcomeLost),
        };
        self.output = None;
        self.permit = None;
        if joined.is_err() {
            return Err(ExecutionTaskReaperError::JoinFailed);
        }
        output.map_err(|_| ExecutionTaskReaperError::OutcomeLost)
    }

    /// Transfers this handle into its exact pre-reserved reaper slot without allocation.
    pub fn transfer(mut self) {
        self.transfer_inner();
    }

    /// Requests cancellation while retaining ownership until join or transfer.
    pub fn abort(&self) {
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }

    fn transfer_inner(&mut self) {
        self.output = None;
        self.abort();
        if let (Some(permit), Some(handle)) = (self.permit.take(), self.handle.take()) {
            permit.transfer(handle);
        }
    }
}

impl<T> Drop for ExecutionTask<T> {
    fn drop(&mut self) {
        self.transfer_inner();
    }
}

#[derive(Debug)]
struct ReaperInner {
    slots: Box<[Arc<Mutex<TaskSlot>>]>,
    changed: Notify,
}

#[derive(Debug)]
enum TaskSlot {
    Available,
    Reserved,
    Retained(ReapCommand),
    Draining,
}

#[derive(Debug)]
struct ReapCommand {
    worker: JoinHandle<()>,
    _permit: OwnedSemaphorePermit,
}

struct RetainedTaskGuard {
    inner: Arc<ReaperInner>,
    slot: Arc<Mutex<TaskSlot>>,
    command: Option<ReapCommand>,
}

impl RetainedTaskGuard {
    fn handle(&mut self) -> Option<&mut JoinHandle<()>> {
        self.command.as_mut().map(|command| &mut command.worker)
    }

    fn complete(mut self) {
        self.command = None;
        set_slot(&self.inner, &self.slot, TaskSlot::Available);
    }
}

impl Drop for RetainedTaskGuard {
    fn drop(&mut self) {
        if let Some(command) = self.command.take() {
            set_slot(&self.inner, &self.slot, TaskSlot::Retained(command));
        }
    }
}

struct TerminalTaskReaper {
    sender: Option<SyncSender<ReapCommand>>,
    capacity: Arc<Semaphore>,
    _thread: Option<ThreadJoinHandle<()>>,
}

/// Bounded drain result, including ownership still active at the deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionTaskDrain {
    completed: usize,
    retained: usize,
    reserved: usize,
    deadline_exceeded: bool,
}

impl ExecutionTaskDrain {
    pub const fn is_complete(self) -> bool {
        self.retained == 0 && self.reserved == 0 && !self.deadline_exceeded
    }

    pub const fn completed(self) -> usize {
        self.completed
    }

    pub const fn retained(self) -> usize {
        self.retained
    }

    pub const fn reserved(self) -> usize {
        self.reserved
    }

    pub const fn deadline_exceeded(self) -> bool {
        self.deadline_exceeded
    }
}

/// Fail-closed task ownership admission or join failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ExecutionTaskReaperError {
    #[error("execution task reaper bounded allocation failed")]
    Allocation,
    #[error("process-lifetime execution task reaper is unavailable")]
    ReaperUnavailable,
    #[error("execution task reaper capacity is saturated")]
    Saturated,
    #[error("execution task spawning requires a current Tokio runtime")]
    RuntimeUnavailable,
    #[error("execution task result channel closed before delivery")]
    OutcomeLost,
    #[error("execution task terminated without a successful join")]
    JoinFailed,
}

fn lock_slot(slot: &Mutex<TaskSlot>) -> MutexGuard<'_, TaskSlot> {
    match slot.lock() {
        Ok(state) => state,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn set_slot(inner: &ReaperInner, slot: &Mutex<TaskSlot>, state: TaskSlot) {
    *lock_slot(slot) = state;
    inner.changed.notify_waiters();
}

fn take_retained(inner: &Arc<ReaperInner>) -> Option<RetainedTaskGuard> {
    for slot in &inner.slots {
        let mut state = lock_slot(slot);
        if matches!(*state, TaskSlot::Retained(_)) {
            let retained = std::mem::replace(&mut *state, TaskSlot::Draining);
            drop(state);
            if let TaskSlot::Retained(command) = retained {
                return Some(RetainedTaskGuard {
                    inner: Arc::clone(inner),
                    slot: Arc::clone(slot),
                    command: Some(command),
                });
            }
        }
    }
    None
}

impl Drop for ReaperInner {
    fn drop(&mut self) {
        for slot in &self.slots {
            let retained = {
                let mut state = lock_slot(slot);
                std::mem::replace(&mut *state, TaskSlot::Available)
            };
            if let TaskSlot::Retained(command) = retained {
                TERMINAL_TASK_REAPER.reap(command);
            }
        }
    }
}

impl TerminalTaskReaper {
    fn start() -> Self {
        let capacity = Arc::new(Semaphore::new(MAX_TERMINAL_EXECUTION_TASKS));
        let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        else {
            return Self {
                sender: None,
                capacity,
                _thread: None,
            };
        };
        let (sender, receiver) = sync_channel::<ReapCommand>(MAX_TERMINAL_EXECUTION_TASKS);
        let thread = std::thread::Builder::new()
            .name("market-squawk-execution-reaper".to_owned())
            .spawn(move || {
                while let Ok(command) = receiver.recv() {
                    let _outcome = runtime.block_on(command.worker);
                    drop(command._permit);
                }
            });
        match thread {
            Ok(thread) => Self {
                sender: Some(sender),
                capacity,
                _thread: Some(thread),
            },
            Err(_error) => Self {
                sender: None,
                capacity,
                _thread: None,
            },
        }
    }

    fn ensure_available(&self) -> Result<(), ExecutionTaskReaperError> {
        if self.sender.is_some() {
            Ok(())
        } else {
            Err(ExecutionTaskReaperError::ReaperUnavailable)
        }
    }

    fn try_reserve(&self) -> Result<OwnedSemaphorePermit, ExecutionTaskReaperError> {
        self.ensure_available()?;
        Arc::clone(&self.capacity)
            .try_acquire_owned()
            .map_err(|_| ExecutionTaskReaperError::Saturated)
    }

    fn reap(&self, command: ReapCommand) {
        command.worker.abort();
        let Some(sender) = self.sender.as_ref() else {
            std::mem::forget(command);
            return;
        };
        if let Err(error) = sender.send(command) {
            std::mem::forget(error.0);
        }
    }
}

fn drain_outcome(
    inner: &ReaperInner,
    completed: usize,
    deadline_exceeded: bool,
) -> ExecutionTaskDrain {
    let (retained, reserved) = inner.slots.iter().fold((0_usize, 0_usize), |counts, slot| {
        let state = lock_slot(slot);
        match *state {
            TaskSlot::Retained(_) | TaskSlot::Draining => (counts.0.saturating_add(1), counts.1),
            TaskSlot::Reserved => (counts.0, counts.1.saturating_add(1)),
            TaskSlot::Available => counts,
        }
    });
    ExecutionTaskDrain {
        completed,
        retained,
        reserved,
        deadline_exceeded,
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future as _;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    use tokio::sync::oneshot;

    use super::{ExecutionTaskReaper, ExecutionTaskReaperError};

    #[tokio::test]
    async fn transferred_attempt_remains_owned_and_capacity_recovers_only_after_drain()
    -> Result<(), ExecutionTaskReaperError> {
        let reaper = ExecutionTaskReaper::try_new(NonZeroUsize::MIN)?;
        let permit = reaper.try_reserve()?;
        assert_eq!(
            reaper.try_reserve().err(),
            Some(ExecutionTaskReaperError::Saturated)
        );

        let (_release, wait) = oneshot::channel::<()>();
        let task = permit.spawn(async move {
            let _ = wait.await;
        })?;
        task.transfer();
        assert_eq!(reaper.retained_task_count(), 1);
        assert_eq!(
            reaper.try_reserve().err(),
            Some(ExecutionTaskReaperError::Saturated)
        );

        assert!(
            reaper
                .drain(tokio::time::Instant::now() + Duration::from_secs(1))
                .await
                .is_complete()
        );
        assert_eq!(reaper.retained_task_count(), 0);
        assert!(reaper.try_reserve().is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn drain_registers_for_the_final_transition_before_returning_pending()
    -> Result<(), ExecutionTaskReaperError> {
        let reaper = ExecutionTaskReaper::try_new(NonZeroUsize::MIN)?;
        let permit = reaper.try_reserve()?;
        let mut drain =
            Box::pin(reaper.drain(tokio::time::Instant::now() + Duration::from_secs(1)));
        let mut context = Context::from_waker(Waker::noop());

        assert!(matches!(drain.as_mut().poll(&mut context), Poll::Pending));
        drop(permit);
        assert!(matches!(
            drain.as_mut().poll(&mut context),
            Poll::Ready(outcome) if outcome.is_complete()
        ));
        Ok(())
    }

    #[tokio::test]
    async fn final_owner_drop_aborts_and_terminally_reaps_without_the_origin_runtime_context()
    -> Result<(), ExecutionTaskReaperError> {
        #[derive(Debug)]
        struct DropSignal(Arc<AtomicBool>);

        impl Drop for DropSignal {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let dropped = Arc::new(AtomicBool::new(false));
        let reaper = ExecutionTaskReaper::try_new(NonZeroUsize::MIN)?;
        let permit = reaper.try_reserve()?;
        let (entered, entered_receiver) = oneshot::channel();
        let signal = DropSignal(Arc::clone(&dropped));
        let task = permit.spawn(async move {
            let _signal = signal;
            let _ = entered.send(());
            std::future::pending::<()>().await;
        })?;
        let _ = entered_receiver.await;

        drop(reaper);
        drop(task);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !dropped.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| ExecutionTaskReaperError::JoinFailed)?;
        Ok(())
    }
}
