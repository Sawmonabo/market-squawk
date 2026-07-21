//! Bounded ownership for blocking diagnostic-journal scans.

use std::{
    sync::{LazyLock, Mutex, MutexGuard, mpsc},
    thread::{JoinHandle, Thread},
    time::{Duration, Instant},
};

use market_squawk_platform::{ConfiguredJournalRead, ConfiguredJournalReadTarget};
use market_squawk_services::{RequestContext, ServiceError};
use thiserror::Error;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use crate::replay::ReplaySummary;

const MAXIMUM_JOURNAL_RECORDS: u64 = 100_000;
const MAXIMUM_JOURNAL_PAYLOAD_BYTES: u64 = 64 * 1024 * 1024;
const JOURNAL_JOB_CAPACITY: usize = 16;
const MAXIMUM_TERMINAL_JOURNAL_WORKERS: usize = 16;
const WORKER_POLL_INTERVAL: Duration = Duration::from_millis(1);
const _: () = assert!(MAXIMUM_TERMINAL_JOURNAL_WORKERS > 0);

static TERMINAL_REAPER: LazyLock<TerminalJournalWorkerReaper> =
    LazyLock::new(TerminalJournalWorkerReaper::start);
static TERMINAL_SLOTS: [Mutex<TerminalSlot>; MAXIMUM_TERMINAL_JOURNAL_WORKERS] =
    [const { Mutex::new(TerminalSlot::Available) }; MAXIMUM_TERMINAL_JOURNAL_WORKERS];

/// Failure to establish bounded ownership before the journal worker starts.
#[derive(Debug, Error)]
pub enum JournalWorkerStartError {
    /// Every fixed process-lifetime terminal-reaper slot is already owned.
    #[error("journal worker terminal-reaper capacity is exhausted")]
    Capacity,
    /// The process-lifetime terminal-reaper thread could not be created.
    #[error("journal worker terminal reaper is unavailable")]
    ReaperUnavailable,
    /// The dedicated journal worker thread could not be created.
    #[error("journal worker thread could not be created")]
    ThreadSpawn(#[source] std::io::Error),
}

/// Terminal ownership result for the dedicated journal worker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum JournalWorkerShutdown {
    /// The worker stopped and was joined before the deadline.
    Joined,
    /// The worker stopped but its join reported a panic.
    Panicked,
    /// The still-owned worker transferred to its pre-reserved process-lifetime reaper slot.
    Transferred,
    /// A prior shutdown or Drop path already consumed worker ownership.
    AlreadyTerminal,
}

/// One dedicated, bounded, non-Tokio worker for configured journal scans.
#[derive(Debug)]
pub(super) struct JournalSummaryWorker {
    cancellation: CancellationToken,
    state: Mutex<WorkerState>,
}

impl JournalSummaryWorker {
    pub(super) fn try_start(
        target: ConfiguredJournalReadTarget,
    ) -> Result<Self, JournalWorkerStartError> {
        let permit = TERMINAL_REAPER.try_reserve()?;
        let cancellation = CancellationToken::new();
        let worker_cancellation = cancellation.clone();
        let worker_target = target.clone();
        let (sender, receiver) = mpsc::sync_channel(JOURNAL_JOB_CAPACITY);
        let thread = match std::thread::Builder::new()
            .name("market-squawk-journal-summary".to_owned())
            .spawn(move || run_worker(worker_target, worker_cancellation, receiver))
        {
            Ok(thread) => thread,
            Err(source) => return Err(JournalWorkerStartError::ThreadSpawn(source)),
        };
        Ok(Self {
            cancellation,
            state: Mutex::new(WorkerState {
                sender: Some(sender),
                owner: Some(WorkerOwner::new(thread, permit)),
            }),
        })
    }

    pub(super) async fn summarize(
        &self,
        context: RequestContext,
    ) -> Result<ReplaySummary, ServiceError> {
        ensure_request_live(&context, &self.cancellation)?;
        let request_cancellation = context.cancellation().clone();
        let deadline = context.deadline();
        let (result_sender, result_receiver) = oneshot::channel();
        let job = JournalJob {
            context,
            result: result_sender,
        };
        let send = lock_state(&self.state)
            .sender
            .as_ref()
            .ok_or(ServiceError::Unavailable)?
            .try_send(job);
        match send {
            Ok(()) => {}
            Err(mpsc::TrySendError::Full(_job)) => {
                return Err(ServiceError::ResourceExhausted);
            }
            Err(mpsc::TrySendError::Disconnected(_job)) => {
                return Err(ServiceError::Unavailable);
            }
        }

        tokio::select! {
            biased;
            () = request_cancellation.cancelled() => Err(ServiceError::Cancelled),
            () = self.cancellation.cancelled() => Err(ServiceError::Unavailable),
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                Err(ServiceError::DeadlineExceeded)
            }
            result = result_receiver => result.map_err(|_closed| ServiceError::Unavailable)?,
        }
    }

    pub(super) async fn shutdown(&self, deadline: tokio::time::Instant) -> JournalWorkerShutdown {
        let Some(mut owner) = self.begin_shutdown() else {
            return JournalWorkerShutdown::AlreadyTerminal;
        };
        loop {
            if owner.is_finished() {
                return owner.join();
            }
            if tokio::time::Instant::now() >= deadline {
                owner.transfer();
                return JournalWorkerShutdown::Transferred;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            tokio::time::sleep(WORKER_POLL_INTERVAL.min(remaining)).await;
        }
    }

    fn begin_shutdown(&self) -> Option<WorkerOwner> {
        self.cancellation.cancel();
        let mut state = lock_state(&self.state);
        state.sender = None;
        state.owner.take()
    }
}

impl Drop for JournalSummaryWorker {
    fn drop(&mut self) {
        self.cancellation.cancel();
        let state = self
            .state
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.sender = None;
        drop(state.owner.take());
    }
}

#[derive(Debug)]
struct WorkerState {
    sender: Option<mpsc::SyncSender<JournalJob>>,
    owner: Option<WorkerOwner>,
}

#[derive(Debug)]
struct JournalJob {
    context: RequestContext,
    result: oneshot::Sender<Result<ReplaySummary, ServiceError>>,
}

fn run_worker(
    target: ConfiguredJournalReadTarget,
    cancellation: CancellationToken,
    receiver: mpsc::Receiver<JournalJob>,
) {
    while !cancellation.is_cancelled() {
        let job = match receiver.recv_timeout(WORKER_POLL_INTERVAL) {
            Ok(job) => job,
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        };
        let result = bounded_journal_summary(&target, &job.context, &cancellation);
        let _ignored = job.result.send(result);
    }
}

fn bounded_journal_summary(
    target: &ConfiguredJournalReadTarget,
    context: &RequestContext,
    worker_cancellation: &CancellationToken,
) -> Result<ReplaySummary, ServiceError> {
    ensure_request_live(context, worker_cancellation)?;
    let mut reader = match target.open().map_err(|_error| ServiceError::Unavailable)? {
        ConfiguredJournalRead::Missing => return Ok(ReplaySummary::default()),
        ConfiguredJournalRead::Reader(reader) => reader,
    };
    let mut summary = ReplaySummary::default();
    loop {
        ensure_request_live(context, worker_cancellation)?;
        let Some(record) = reader
            .next_record()
            .map_err(|_error| ServiceError::Unavailable)?
        else {
            return Ok(summary);
        };
        if summary.records >= MAXIMUM_JOURNAL_RECORDS {
            return Err(ServiceError::ResourceExhausted);
        }
        let payload_bytes = u64::try_from(record.payload().len())
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        let aggregate_payload_bytes = summary
            .bytes
            .checked_add(payload_bytes)
            .ok_or(ServiceError::ResourceExhausted)?;
        if aggregate_payload_bytes > MAXIMUM_JOURNAL_PAYLOAD_BYTES {
            return Err(ServiceError::ResourceExhausted);
        }
        summary
            .observe(&record)
            .map_err(|_error| ServiceError::Internal)?;
    }
}

fn ensure_request_live(
    context: &RequestContext,
    worker_cancellation: &CancellationToken,
) -> Result<(), ServiceError> {
    if context.cancellation().is_cancelled() {
        return Err(ServiceError::Cancelled);
    }
    if worker_cancellation.is_cancelled() {
        return Err(ServiceError::Unavailable);
    }
    if Instant::now() >= context.deadline() {
        return Err(ServiceError::DeadlineExceeded);
    }
    Ok(())
}

#[derive(Debug)]
struct WorkerOwner {
    thread: Option<JoinHandle<()>>,
    permit: Option<TerminalReaperPermit>,
}

impl WorkerOwner {
    fn new(thread: JoinHandle<()>, permit: TerminalReaperPermit) -> Self {
        Self {
            thread: Some(thread),
            permit: Some(permit),
        }
    }

    fn is_finished(&self) -> bool {
        self.thread.as_ref().is_none_or(JoinHandle::is_finished)
    }

    fn join(&mut self) -> JournalWorkerShutdown {
        let outcome = self.thread.take().map(JoinHandle::join);
        self.permit = None;
        if outcome.is_some_and(|result| result.is_err()) {
            JournalWorkerShutdown::Panicked
        } else {
            JournalWorkerShutdown::Joined
        }
    }

    fn transfer(&mut self) {
        if let (Some(thread), Some(permit)) = (self.thread.take(), self.permit.take()) {
            permit.transfer(thread);
        }
    }
}

impl Drop for WorkerOwner {
    fn drop(&mut self) {
        self.transfer();
    }
}

#[derive(Debug)]
struct TerminalReaperPermit {
    slot: usize,
    armed: bool,
}

impl TerminalReaperPermit {
    fn transfer(mut self, worker: JoinHandle<()>) {
        *lock_slot(&TERMINAL_SLOTS[self.slot]) = TerminalSlot::Retained(worker);
        self.armed = false;
        TERMINAL_REAPER.wake();
    }
}

impl Drop for TerminalReaperPermit {
    fn drop(&mut self) {
        if self.armed {
            *lock_slot(&TERMINAL_SLOTS[self.slot]) = TerminalSlot::Available;
            self.armed = false;
        }
    }
}

#[derive(Debug)]
enum TerminalSlot {
    Available,
    Reserved,
    Retained(JoinHandle<()>),
}

struct TerminalJournalWorkerReaper {
    thread: Option<Thread>,
    _owner: Option<JoinHandle<()>>,
}

impl TerminalJournalWorkerReaper {
    fn start() -> Self {
        match std::thread::Builder::new()
            .name("market-squawk-journal-reaper".to_owned())
            .spawn(run_terminal_reaper)
        {
            Ok(owner) => Self {
                thread: Some(owner.thread().clone()),
                _owner: Some(owner),
            },
            Err(_error) => Self {
                thread: None,
                _owner: None,
            },
        }
    }

    fn try_reserve(&self) -> Result<TerminalReaperPermit, JournalWorkerStartError> {
        if self.thread.is_none() {
            return Err(JournalWorkerStartError::ReaperUnavailable);
        }
        for (index, slot) in TERMINAL_SLOTS.iter().enumerate() {
            let mut state = lock_slot(slot);
            if matches!(*state, TerminalSlot::Available) {
                *state = TerminalSlot::Reserved;
                return Ok(TerminalReaperPermit {
                    slot: index,
                    armed: true,
                });
            }
        }
        Err(JournalWorkerStartError::Capacity)
    }

    fn wake(&self) {
        if let Some(thread) = &self.thread {
            thread.unpark();
        }
    }
}

fn run_terminal_reaper() {
    loop {
        let mut pending = false;
        for slot in &TERMINAL_SLOTS {
            let completed = {
                let mut state = lock_slot(slot);
                match &*state {
                    TerminalSlot::Retained(worker) if worker.is_finished() => {
                        match std::mem::replace(&mut *state, TerminalSlot::Available) {
                            TerminalSlot::Retained(worker) => Some(worker),
                            TerminalSlot::Available | TerminalSlot::Reserved => None,
                        }
                    }
                    TerminalSlot::Retained(_) => {
                        pending = true;
                        None
                    }
                    TerminalSlot::Available | TerminalSlot::Reserved => None,
                }
            };
            if let Some(worker) = completed {
                let _outcome = worker.join();
            }
        }
        if pending {
            std::thread::park_timeout(WORKER_POLL_INTERVAL);
        } else {
            std::thread::park();
        }
    }
}

fn lock_state(state: &Mutex<WorkerState>) -> MutexGuard<'_, WorkerState> {
    state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn lock_slot(slot: &Mutex<TerminalSlot>) -> MutexGuard<'_, TerminalSlot> {
    slot.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
