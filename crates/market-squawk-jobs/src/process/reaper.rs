use std::{
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

use super::ContainedProcessError;

pub(super) const PROCESS_CLEANUP_DEADLINE: Duration = Duration::from_secs(5);
const MAXIMUM_RETAINED_PROCESS_CLEANUPS: usize = 16;

static PROCESS_CLEANUP_SLOTS: [Mutex<ProcessCleanupSlot>; MAXIMUM_RETAINED_PROCESS_CLEANUPS] =
    [const { Mutex::new(ProcessCleanupSlot::Available) }; MAXIMUM_RETAINED_PROCESS_CLEANUPS];
static PROCESS_EXECUTION_SLOTS: [AtomicBool; MAXIMUM_RETAINED_PROCESS_CLEANUPS] =
    [const { AtomicBool::new(false) }; MAXIMUM_RETAINED_PROCESS_CLEANUPS];

#[derive(Debug)]
pub(super) struct ProcessExecutionReservation {
    slot: usize,
}

impl ProcessExecutionReservation {
    pub(super) fn try_acquire() -> Result<Self, ContainedProcessError> {
        for (slot, active) in PROCESS_EXECUTION_SLOTS.iter().enumerate() {
            if active
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(Self { slot });
            }
        }
        Err(ContainedProcessError::ReaperCapacity)
    }
}

impl Drop for ProcessExecutionReservation {
    fn drop(&mut self) {
        PROCESS_EXECUTION_SLOTS[self.slot].store(false, Ordering::Release);
    }
}

#[derive(Debug)]
enum ProcessCleanupSlot {
    Available,
    Reserved,
    Running(thread::JoinHandle<()>),
}

#[derive(Debug)]
struct RetainedProcessCleanup {
    child: Box<dyn process_wrap::std::ChildWrapper>,
    readers: Vec<thread::JoinHandle<()>>,
}

#[derive(Debug)]
pub(super) struct ProcessCleanupReservation {
    slot: usize,
    retained: bool,
}

impl ProcessCleanupReservation {
    pub(super) fn try_acquire() -> Result<Self, ContainedProcessError> {
        for (slot, entry) in PROCESS_CLEANUP_SLOTS.iter().enumerate() {
            let mut state = entry
                .lock()
                .map_err(|_| ContainedProcessError::Unavailable)?;
            if matches!(&*state, ProcessCleanupSlot::Running(handle) if handle.is_finished()) {
                let completed = std::mem::replace(&mut *state, ProcessCleanupSlot::Available);
                drop(state);
                if let ProcessCleanupSlot::Running(handle) = completed {
                    let _ignored = handle.join();
                }
                state = entry
                    .lock()
                    .map_err(|_| ContainedProcessError::Unavailable)?;
            }
            if matches!(*state, ProcessCleanupSlot::Available) {
                *state = ProcessCleanupSlot::Reserved;
                return Ok(Self {
                    slot,
                    retained: false,
                });
            }
        }
        Err(ContainedProcessError::ReaperCapacity)
    }

    pub(super) fn retain(
        mut self,
        child: Box<dyn process_wrap::std::ChildWrapper>,
        readers: Vec<thread::JoinHandle<()>>,
    ) -> Result<(), ContainedProcessError> {
        let mut cleanup = RetainedProcessCleanup { child, readers };
        let mut state = match PROCESS_CLEANUP_SLOTS[self.slot].lock() {
            Ok(state) => state,
            Err(_) => {
                reap_process_cleanup(cleanup);
                return Err(ContainedProcessError::Unavailable);
            }
        };
        if !matches!(*state, ProcessCleanupSlot::Reserved) {
            drop(state);
            reap_process_cleanup(cleanup);
            return Err(ContainedProcessError::Unavailable);
        }
        let (sender, receiver) = mpsc::sync_channel(1);
        let slot = self.slot;
        let handle = match thread::Builder::new()
            .name(format!("market-squawk-process-reaper-{slot}"))
            .spawn(move || {
                if let Ok(cleanup) = receiver.recv() {
                    reap_process_cleanup(cleanup);
                }
                if let Ok(mut state) = PROCESS_CLEANUP_SLOTS[slot].lock() {
                    *state = ProcessCleanupSlot::Available;
                }
            }) {
            Ok(handle) => handle,
            Err(_) => {
                drop(state);
                reap_process_cleanup(cleanup);
                return Err(ContainedProcessError::Unavailable);
            }
        };
        *state = ProcessCleanupSlot::Running(handle);
        self.retained = true;
        drop(state);
        if let Err(error) = sender.send(cleanup) {
            cleanup = error.0;
            reap_process_cleanup(cleanup);
            return Err(ContainedProcessError::Unavailable);
        }
        Ok(())
    }
}

pub(crate) async fn await_contained_processes(deadline: Duration) -> bool {
    let started = tokio::time::Instant::now();
    loop {
        let executions_complete = PROCESS_EXECUTION_SLOTS
            .iter()
            .all(|active| !active.load(Ordering::Acquire));
        let cleanup_complete = process_cleanup_complete();
        if executions_complete && cleanup_complete {
            return true;
        }
        if started.elapsed() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn process_cleanup_complete() -> bool {
    let mut complete = true;
    for entry in &PROCESS_CLEANUP_SLOTS {
        let Ok(mut state) = entry.lock() else {
            return false;
        };
        if matches!(&*state, ProcessCleanupSlot::Running(handle) if handle.is_finished()) {
            let completed = std::mem::replace(&mut *state, ProcessCleanupSlot::Available);
            drop(state);
            if let ProcessCleanupSlot::Running(handle) = completed {
                let _ignored = handle.join();
            }
        } else if !matches!(*state, ProcessCleanupSlot::Available) {
            complete = false;
        }
    }
    complete
}

fn reap_process_cleanup(mut cleanup: RetainedProcessCleanup) {
    loop {
        let _kill_requested = cleanup.child.start_kill();
        if cleanup.child.wait().is_ok() {
            break;
        }
        thread::sleep(Duration::from_millis(5));
    }
    for reader in cleanup.readers {
        let _ignored = reader.join();
    }
}

impl Drop for ProcessCleanupReservation {
    fn drop(&mut self) {
        if self.retained {
            return;
        }
        if let Ok(mut state) = PROCESS_CLEANUP_SLOTS[self.slot].lock()
            && matches!(*state, ProcessCleanupSlot::Reserved)
        {
            *state = ProcessCleanupSlot::Available;
        }
    }
}
