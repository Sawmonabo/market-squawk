//! Killable child ownership and fixed process-lifetime terminal reaper slots.

use std::process::{Child, ExitStatus};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread::JoinHandle;
use std::time::Duration;

use thiserror::Error;

use crate::capture::writer::CaptureWriterDestinationFences;

const MAX_TERMINAL_CAPTURE_REAPERS: usize = 16;
const PROCESS_POLL_INTERVAL: Duration = Duration::from_millis(1);

static TERMINAL_REAPER_SLOTS: [Mutex<TerminalReaperSlot>; MAX_TERMINAL_CAPTURE_REAPERS] =
    [const { Mutex::new(TerminalReaperSlot::Available) }; MAX_TERMINAL_CAPTURE_REAPERS];

#[derive(Debug)]
enum TerminalReaperSlot {
    Available,
    Reserved,
    Running(TerminalThreads),
}

#[derive(Debug)]
struct TerminalThreads {
    process: JoinHandle<()>,
    companion: Option<JoinHandle<()>>,
    destination_fences: Option<CaptureWriterDestinationFences>,
}

impl TerminalThreads {
    fn is_finished(&self) -> bool {
        self.process.is_finished() && self.companion.as_ref().is_none_or(JoinHandle::is_finished)
    }

    fn join(self) {
        let _process = self.process.join();
        if let Some(companion) = self.companion {
            let _companion = companion.join();
        }
        drop(self.destination_fences);
    }
}

#[derive(Debug)]
pub(super) struct TerminalReaperReservation {
    index: usize,
    active: bool,
}

impl TerminalReaperReservation {
    pub(super) fn try_acquire() -> Result<Self, ProcessSupervisionError> {
        for (index, slot) in TERMINAL_REAPER_SLOTS.iter().enumerate() {
            let finished = {
                let mut state = slot
                    .lock()
                    .map_err(|_error| ProcessSupervisionError::ReaperRegistryPoisoned)?;
                match &*state {
                    TerminalReaperSlot::Available => {
                        *state = TerminalReaperSlot::Reserved;
                        return Ok(Self {
                            index,
                            active: true,
                        });
                    }
                    TerminalReaperSlot::Reserved => None,
                    TerminalReaperSlot::Running(threads) if threads.is_finished() => {
                        match std::mem::replace(&mut *state, TerminalReaperSlot::Reserved) {
                            TerminalReaperSlot::Running(threads) => Some(threads),
                            TerminalReaperSlot::Available | TerminalReaperSlot::Reserved => None,
                        }
                    }
                    TerminalReaperSlot::Running(_) => None,
                }
            };
            if let Some(finished) = finished {
                finished.join();
                return Ok(Self {
                    index,
                    active: true,
                });
            }
        }
        Err(ProcessSupervisionError::ReaperCapacity)
    }

    pub(super) fn retain(
        mut self,
        process: JoinHandle<()>,
        companion: Option<JoinHandle<()>>,
        destination_fences: Option<CaptureWriterDestinationFences>,
    ) {
        let replacement = TerminalReaperSlot::Running(TerminalThreads {
            process,
            companion,
            destination_fences,
        });
        match TERMINAL_REAPER_SLOTS[self.index].lock() {
            Ok(mut state) if matches!(&*state, TerminalReaperSlot::Reserved) => {
                *state = replacement;
                self.active = false;
            }
            Ok(_) | Err(_) => {
                // No ownership may be detached if the fixed registry is corrupted. Joining here
                // is deliberately fail-closed even though an operating-system fault may block.
                if let TerminalReaperSlot::Running(threads) = replacement {
                    threads.join();
                }
            }
        }
    }

    fn release(&mut self) {
        if !self.active {
            return;
        }
        if let Ok(mut state) = TERMINAL_REAPER_SLOTS[self.index].lock()
            && matches!(&*state, TerminalReaperSlot::Reserved)
        {
            *state = TerminalReaperSlot::Available;
        }
        self.active = false;
    }
}

impl Drop for TerminalReaperReservation {
    fn drop(&mut self) {
        self.release();
    }
}

#[derive(Debug)]
struct ProcessObservation {
    reaped: AtomicBool,
    killed: AtomicBool,
    kill_requested: AtomicBool,
    failed: AtomicBool,
    wait_lock: Mutex<()>,
    wait_notification: Condvar,
}

impl ProcessObservation {
    fn new() -> Self {
        Self {
            reaped: AtomicBool::new(false),
            killed: AtomicBool::new(false),
            kill_requested: AtomicBool::new(false),
            failed: AtomicBool::new(false),
            wait_lock: Mutex::new(()),
            wait_notification: Condvar::new(),
        }
    }

    fn mark_reaped(&self) {
        self.reaped.store(true, Ordering::Release);
        self.wait_notification.notify_all();
    }
}

#[derive(Clone, Debug)]
pub(super) struct ProcessWaitHandle {
    commands: mpsc::SyncSender<ProcessCommand>,
    observation: Arc<ProcessObservation>,
}

impl ProcessWaitHandle {
    pub(super) fn kill(&self) {
        if self.is_reaped() || self.observation.kill_requested.swap(true, Ordering::AcqRel) {
            return;
        }
        if self.commands.try_send(ProcessCommand::Kill).is_err() && !self.is_reaped() {
            self.observation.failed.store(true, Ordering::Release);
        }
    }

    pub(super) fn is_reaped(&self) -> bool {
        self.observation.reaped.load(Ordering::Acquire)
    }

    pub(super) fn was_killed(&self) -> bool {
        self.observation.killed.load(Ordering::Acquire)
    }

    pub(super) fn failed(&self) -> bool {
        self.observation.failed.load(Ordering::Acquire)
    }

    pub(super) fn wait_blocking(&self) {
        let mut guard = match self.observation.wait_lock.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        while !self.is_reaped() {
            guard = match self.observation.wait_notification.wait(guard) {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
        }
    }
}

#[derive(Debug)]
pub(super) struct ProcessOwner {
    wait: ProcessWaitHandle,
    supervisor: Option<JoinHandle<()>>,
}

impl ProcessOwner {
    pub(super) fn try_start(
        child: Child,
        reap_observation_delay: Duration,
    ) -> Result<Self, ProcessSupervisionError> {
        let (commands, receiver) = mpsc::sync_channel(1);
        let observation = Arc::new(ProcessObservation::new());
        let task_observation = Arc::clone(&observation);
        let launch_child = Arc::new(Mutex::new(Some(child)));
        let task_child = Arc::clone(&launch_child);
        let supervisor = match std::thread::Builder::new()
            .name("msq-capture-child".to_owned())
            .stack_size(128 * 1024)
            .spawn(move || {
                let child = match task_child.lock() {
                    Ok(mut child) => child.take(),
                    Err(poisoned) => poisoned.into_inner().take(),
                };
                if let Some(child) = child {
                    supervise(child, receiver, &task_observation, reap_observation_delay);
                } else {
                    task_observation.failed.store(true, Ordering::Release);
                    task_observation.mark_reaped();
                }
            }) {
            Ok(supervisor) => supervisor,
            Err(source) => {
                let mut child = match launch_child.lock() {
                    Ok(child) => child,
                    Err(poisoned) => poisoned.into_inner(),
                };
                if let Some(mut child) = child.take() {
                    let _killed = child.kill();
                    let _reaped = child.wait();
                }
                return Err(ProcessSupervisionError::ThreadSpawn(source));
            }
        };
        Ok(Self {
            wait: ProcessWaitHandle {
                commands,
                observation,
            },
            supervisor: Some(supervisor),
        })
    }

    pub(super) fn wait_handle(&self) -> ProcessWaitHandle {
        self.wait.clone()
    }

    pub(super) fn kill(&self) {
        self.wait.kill();
    }

    pub(super) fn is_reaped(&self) -> bool {
        self.wait.is_reaped()
    }

    pub(super) fn was_killed(&self) -> bool {
        self.wait.was_killed()
    }

    pub(super) fn failed(&self) -> bool {
        self.wait.failed()
    }

    pub(super) fn take_supervisor(&mut self) -> Option<JoinHandle<()>> {
        self.supervisor.take()
    }

    pub(super) fn join_if_reaped(&mut self) -> bool {
        if !self.is_reaped() {
            return false;
        }
        self.supervisor
            .take()
            .is_none_or(|supervisor| supervisor.join().is_ok())
    }
}

impl Drop for ProcessOwner {
    fn drop(&mut self) {
        if self.supervisor.is_none() {
            return;
        }
        self.kill();
        self.wait.wait_blocking();
        let _joined = self.join_if_reaped();
    }
}

#[derive(Clone, Copy, Debug)]
enum ProcessCommand {
    Kill,
}

fn supervise(
    mut child: Child,
    commands: mpsc::Receiver<ProcessCommand>,
    observation: &ProcessObservation,
    reap_observation_delay: Duration,
) {
    let mut kill_requested = false;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                record_exit(status, kill_requested, observation, reap_observation_delay);
                return;
            }
            Ok(None) => {}
            Err(_error) => {
                observation.failed.store(true, Ordering::Release);
                let _killed = child.kill();
                break;
            }
        }
        match commands.recv_timeout(PROCESS_POLL_INTERVAL) {
            Ok(ProcessCommand::Kill) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                if !kill_requested {
                    kill_requested = true;
                    observation.killed.store(true, Ordering::Release);
                    if child.kill().is_err() {
                        match child.try_wait() {
                            Ok(Some(status)) => {
                                record_exit(
                                    status,
                                    kill_requested,
                                    observation,
                                    reap_observation_delay,
                                );
                                return;
                            }
                            Ok(None) | Err(_) => {
                                observation.failed.store(true, Ordering::Release);
                            }
                        }
                    }
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
    }
    if child.wait().is_err() {
        observation.failed.store(true, Ordering::Release);
    }
    delay_reap_observation(reap_observation_delay);
    observation.mark_reaped();
}

fn record_exit(
    status: ExitStatus,
    kill_requested: bool,
    observation: &ProcessObservation,
    reap_observation_delay: Duration,
) {
    if !kill_requested && !status.success() {
        observation.failed.store(true, Ordering::Release);
    }
    delay_reap_observation(reap_observation_delay);
    observation.mark_reaped();
}

fn delay_reap_observation(delay: Duration) {
    if !delay.is_zero() {
        std::thread::sleep(delay);
    }
}

#[derive(Debug, Error)]
pub(super) enum ProcessSupervisionError {
    #[error("fixed terminal capture-reaper capacity is exhausted")]
    ReaperCapacity,
    #[error("fixed terminal capture-reaper registry is poisoned")]
    ReaperRegistryPoisoned,
    #[error("capture helper process-supervisor thread could not be created")]
    ThreadSpawn(#[source] std::io::Error),
}
