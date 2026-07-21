//! Bounded two-deadline ownership for process-isolated journal capture.

use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use market_squawk_domain::CaptureAuthorityBundle;
use thiserror::Error;

use super::config::ProcessJournalCaptureConfig;
use super::process::{
    ProcessOwner, ProcessSupervisionError, ProcessWaitHandle, TerminalReaperReservation,
};
use super::sink::{ProcessJournalSink, ProcessJournalSinkStartError};
use crate::capture::writer::{
    CaptureShutdownStatus, CaptureWorkerTermination, CaptureWriterHandle, CaptureWriterSpawnError,
    PendingCaptureWriter, spawn_capture_writer,
};
use crate::capture::{CaptureWriterPolicy, RawCaptureWriter};

const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(1);

/// Independent capture-worker and killed-helper reap deadlines.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProcessCaptureShutdownPolicy {
    worker_deadline: Duration,
    helper_reap_deadline: Duration,
}

impl ProcessCaptureShutdownPolicy {
    /// Constructs a shutdown policy with two explicit nonzero bounds.
    pub fn try_new(
        worker_deadline: Duration,
        helper_reap_deadline: Duration,
    ) -> Result<Self, ProcessCaptureShutdownPolicyError> {
        if worker_deadline.is_zero() {
            return Err(ProcessCaptureShutdownPolicyError::ZeroWorkerDeadline);
        }
        if helper_reap_deadline.is_zero() {
            return Err(ProcessCaptureShutdownPolicyError::ZeroHelperReapDeadline);
        }
        Ok(Self {
            worker_deadline,
            helper_reap_deadline,
        })
    }
}

/// Invalid process capture shutdown policy.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ProcessCaptureShutdownPolicyError {
    #[error("capture worker shutdown deadline must be nonzero")]
    ZeroWorkerDeadline,
    #[error("capture helper reap deadline must be nonzero")]
    ZeroHelperReapDeadline,
}

/// Terminal classification of the helper boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessCaptureShutdownDisposition {
    /// Worker, protocol shutdown, and helper exit completed inside both deadlines.
    Complete,
    /// The first deadline revoked authority and the helper was killed and reaped.
    HelperKilled,
    /// The helper exited or supervision failed without a requested kill.
    HelperFailed,
    /// Ownership moved to a fixed terminal reaper after the second deadline.
    Unreaped,
}

/// Final bounded process-capture shutdown observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessCaptureShutdownOutcome {
    disposition: ProcessCaptureShutdownDisposition,
    helper_reaped: bool,
    worker_termination: Option<CaptureWorkerTermination>,
}

impl ProcessCaptureShutdownOutcome {
    /// Returns the terminal helper classification.
    pub const fn disposition(&self) -> ProcessCaptureShutdownDisposition {
        self.disposition
    }

    /// Returns whether this lifecycle owner observed and joined helper process reaping.
    pub const fn helper_reaped(&self) -> bool {
        self.helper_reaped
    }

    /// Returns the joined capture-worker report when both ownership boundaries terminated.
    pub const fn worker_termination(&self) -> Option<&CaptureWorkerTermination> {
        self.worker_termination.as_ref()
    }
}

#[derive(Debug)]
enum CompanionCommand<B: CaptureAuthorityBundle> {
    Stop,
    Own {
        pending: PendingCaptureWriter<B>,
        process: ProcessWaitHandle,
    },
}

/// Sole live owner of a process-isolated journal capture worker.
#[derive(Debug)]
#[must_use = "process capture writers must be shut down and explicitly reaped"]
pub struct ProcessJournalCaptureWriter<B: CaptureAuthorityBundle> {
    writer: Option<CaptureWriterHandle<B>>,
    pending: Option<PendingCaptureWriter<B>>,
    process: Option<ProcessOwner>,
    reaper: Option<TerminalReaperReservation>,
    companion_sender: mpsc::SyncSender<CompanionCommand<B>>,
    companion: Option<JoinHandle<()>>,
    completed: bool,
}

impl<B: CaptureAuthorityBundle> ProcessJournalCaptureWriter<B> {
    /// Revokes capture authority at the first deadline and bounds killed-process reaping with the
    /// second deadline. An unreaped terminal owner is retained in fixed process-lifetime storage.
    pub async fn shutdown(
        mut self,
        policy: ProcessCaptureShutdownPolicy,
    ) -> ProcessCaptureShutdownOutcome {
        let Some(writer) = self.writer.take() else {
            return ProcessCaptureShutdownOutcome {
                disposition: ProcessCaptureShutdownDisposition::HelperFailed,
                helper_reaped: false,
                worker_termination: None,
            };
        };
        self.pending = Some(writer.shutdown(policy.worker_deadline));
        let worker_status = match self.pending.as_mut() {
            Some(pending) => pending.wait_until_deadline().await,
            None => {
                return ProcessCaptureShutdownOutcome {
                    disposition: ProcessCaptureShutdownDisposition::HelperFailed,
                    helper_reaped: false,
                    worker_termination: None,
                };
            }
        };
        if worker_status == CaptureShutdownStatus::DeadlineElapsed
            && let Some(process) = self.process.as_ref()
        {
            process.kill();
        }
        let reap_deadline = Instant::now()
            .checked_add(policy.helper_reap_deadline)
            .unwrap_or_else(Instant::now);
        while !(self
            .pending
            .as_ref()
            .is_some_and(PendingCaptureWriter::is_worker_terminated)
            && self.process.as_ref().is_some_and(ProcessOwner::is_reaped))
            && Instant::now() < reap_deadline
        {
            tokio::time::sleep(SHUTDOWN_POLL_INTERVAL).await;
        }
        let fully_terminated = self
            .pending
            .as_ref()
            .is_some_and(PendingCaptureWriter::is_worker_terminated)
            && self.process.as_ref().is_some_and(ProcessOwner::is_reaped);
        if !fully_terminated {
            if let Some(process) = self.process.as_ref() {
                process.kill();
            }
            if let Some(pending) = self.pending.take() {
                self.retain_terminal_owner(pending);
            }
            self.completed = true;
            return ProcessCaptureShutdownOutcome {
                disposition: ProcessCaptureShutdownDisposition::Unreaped,
                helper_reaped: false,
                worker_termination: None,
            };
        }

        let worker_termination = self.pending.as_mut().and_then(|pending| {
            pending
                .try_reap()
                .ok()
                .and_then(|termination| termination.cloned())
        });
        drop(self.pending.take());
        let (was_killed, failed, helper_reaped) =
            self.process
                .as_mut()
                .map_or((false, true, false), |process| {
                    let was_killed = process.was_killed();
                    let failed = process.failed();
                    let helper_reaped = process.join_if_reaped();
                    (was_killed, failed, helper_reaped)
                });
        self.stop_and_join_companion();
        drop(self.process.take());
        drop(self.reaper.take());
        self.completed = true;
        let disposition = if !helper_reaped || worker_termination.is_none() || failed {
            ProcessCaptureShutdownDisposition::HelperFailed
        } else if was_killed || worker_status == CaptureShutdownStatus::DeadlineElapsed {
            ProcessCaptureShutdownDisposition::HelperKilled
        } else {
            ProcessCaptureShutdownDisposition::Complete
        };
        ProcessCaptureShutdownOutcome {
            disposition,
            helper_reaped,
            worker_termination,
        }
    }

    fn retain_terminal_owner(&mut self, pending: PendingCaptureWriter<B>) {
        let Some(process) = self.process.as_mut() else {
            drop(pending);
            return;
        };
        let command = CompanionCommand::Own {
            pending,
            process: process.wait_handle(),
        };
        if let Err(error) = self.companion_sender.try_send(command) {
            match error {
                mpsc::TrySendError::Full(command) | mpsc::TrySendError::Disconnected(command) => {
                    drop(command)
                }
            }
        }
        let supervisor = process.take_supervisor();
        let companion = self.companion.take();
        match (self.reaper.take(), supervisor) {
            (Some(reaper), Some(supervisor)) => reaper.retain(supervisor, companion),
            (reaper, _supervisor) => {
                drop(reaper);
                if let Some(companion) = companion {
                    let _joined = companion.join();
                }
            }
        }
        drop(self.process.take());
    }

    fn stop_and_join_companion(&mut self) {
        let _stopped = self.companion_sender.try_send(CompanionCommand::Stop);
        if let Some(companion) = self.companion.take() {
            let _joined = companion.join();
        }
    }
}

impl<B: CaptureAuthorityBundle> Drop for ProcessJournalCaptureWriter<B> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        if let Some(process) = self.process.as_ref() {
            process.kill();
        }
        if let Some(pending) = self.pending.take() {
            self.retain_terminal_owner(pending);
        } else if let Some(writer) = self.writer.take() {
            let pending = writer.shutdown(Duration::ZERO);
            self.retain_terminal_owner(pending);
        } else {
            self.stop_and_join_companion();
        }
        self.completed = true;
    }
}

/// Starts one process-isolated journal capture worker and its preallocated terminal owner.
pub fn spawn_process_journal_capture_writer<B: CaptureAuthorityBundle>(
    writer: RawCaptureWriter<B>,
    config: ProcessJournalCaptureConfig,
    policy: CaptureWriterPolicy,
) -> Result<ProcessJournalCaptureWriter<B>, ProcessCaptureWriterSpawnError> {
    let started = ProcessJournalSink::try_start(config)?;
    let (companion_sender, companion_receiver) = mpsc::sync_channel(1);
    let companion = std::thread::Builder::new()
        .name("msq-capture-terminal".to_owned())
        .stack_size(128 * 1024)
        .spawn(move || run_companion(companion_receiver))
        .map_err(ProcessCaptureWriterSpawnError::CompanionThread)?;
    let capture = match spawn_capture_writer(writer, started.sink, policy) {
        Ok(capture) => capture,
        Err(error) => {
            let _stopped = companion_sender.try_send(CompanionCommand::Stop);
            let _joined = companion.join();
            return Err(ProcessCaptureWriterSpawnError::CaptureWriter(error));
        }
    };
    Ok(ProcessJournalCaptureWriter {
        writer: Some(capture),
        pending: None,
        process: Some(started.process),
        reaper: Some(started.reaper),
        companion_sender,
        companion: Some(companion),
        completed: false,
    })
}

fn run_companion<B: CaptureAuthorityBundle>(receiver: mpsc::Receiver<CompanionCommand<B>>) {
    match receiver.recv() {
        Ok(CompanionCommand::Stop) | Err(_) => {}
        Ok(CompanionCommand::Own { pending, process }) => {
            process.wait_blocking();
            drop(pending);
        }
    }
}

/// Failure before process-isolated capture ownership is returned.
#[derive(Debug, Error)]
pub enum ProcessCaptureWriterSpawnError {
    /// The operating system rejected helper launch.
    #[error("capture helper launch failed")]
    HelperLaunch(#[source] std::io::Error),
    /// Configured helper pipes were not returned by the operating system.
    #[error("capture helper did not expose its configured standard pipes")]
    MissingPipe,
    /// The bounded startup reader could not be created.
    #[error("capture helper startup reader could not be created")]
    StartupThread(#[source] std::io::Error),
    /// The bounded startup reader panicked.
    #[error("capture helper startup reader panicked")]
    StartupThreadPanicked,
    /// The helper did not prove readiness inside the startup deadline.
    #[error("capture helper startup deadline elapsed")]
    StartupDeadline,
    /// The helper readiness frame failed protocol validation.
    #[error("capture helper startup protocol validation failed")]
    StartupProtocol,
    /// Fixed terminal-reaper capacity was exhausted before launch.
    #[error("fixed terminal capture-reaper capacity is exhausted")]
    ReaperCapacity,
    /// The fixed terminal-reaper registry was poisoned.
    #[error("fixed terminal capture-reaper registry is poisoned")]
    ReaperRegistryPoisoned,
    /// The helper process-supervisor thread could not be created.
    #[error("capture helper process-supervisor thread could not be created")]
    ProcessSupervisorThread(#[source] std::io::Error),
    /// The preallocated terminal-owner thread could not be created.
    #[error("capture helper terminal-owner thread could not be created")]
    CompanionThread(#[source] std::io::Error),
    /// The in-process capture writer refused startup.
    #[error(transparent)]
    CaptureWriter(#[from] CaptureWriterSpawnError),
}

impl From<ProcessJournalSinkStartError> for ProcessCaptureWriterSpawnError {
    fn from(error: ProcessJournalSinkStartError) -> Self {
        match error {
            ProcessJournalSinkStartError::HelperLaunch(source) => Self::HelperLaunch(source),
            ProcessJournalSinkStartError::MissingPipe => Self::MissingPipe,
            ProcessJournalSinkStartError::StartupThread(source) => Self::StartupThread(source),
            ProcessJournalSinkStartError::StartupThreadPanicked => Self::StartupThreadPanicked,
            ProcessJournalSinkStartError::StartupDeadline => Self::StartupDeadline,
            ProcessJournalSinkStartError::StartupProtocol => Self::StartupProtocol,
            ProcessJournalSinkStartError::Supervision(ProcessSupervisionError::ReaperCapacity) => {
                Self::ReaperCapacity
            }
            ProcessJournalSinkStartError::Supervision(
                ProcessSupervisionError::ReaperRegistryPoisoned,
            ) => Self::ReaperRegistryPoisoned,
            ProcessJournalSinkStartError::Supervision(ProcessSupervisionError::ThreadSpawn(
                source,
            )) => Self::ProcessSupervisorThread(source),
        }
    }
}
