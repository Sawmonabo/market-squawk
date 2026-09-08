use std::{
    fs::File,
    io::{Read, Write as _},
    path::Path,
    process::{ExitStatus, Stdio},
    sync::{Arc, mpsc as std_mpsc},
    thread,
    time::{Duration, Instant},
};

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier};
use process_wrap::std::CommandWrap;
#[cfg(windows)]
use process_wrap::std::JobObject;
#[cfg(unix)]
use process_wrap::std::ProcessGroup;
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::sync::mpsc as tokio_mpsc;
use tokio_util::sync::CancellationToken;

use crate::JobResultReference;

mod reaper;

pub(crate) use reaper::await_contained_processes;
use reaper::{PROCESS_CLEANUP_DEADLINE, ProcessCleanupReservation, ProcessExecutionReservation};

const MAXIMUM_PROGRAM_BYTES: u64 = 512 * 1024 * 1024;
const MAXIMUM_ARGUMENTS: usize = 128;
const MAXIMUM_ARGUMENT_BYTES: usize = 16 * 1024;
const STDOUT_FRAME_CHANNEL_CAPACITY: usize = 8;

/// Invalid or unsafe contained worker program.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ProcessProgramError {
    /// The program was not an absolute, regular, non-symlink executable.
    #[error("worker program is not a sealed executable")]
    InvalidProgram,
    /// Program bytes did not match the admitted SHA-256 evidence.
    #[error("worker program digest does not match admission")]
    DigestMismatch,
    /// Program bytes exceeded the hard admission ceiling.
    #[error("worker program exceeds the byte ceiling")]
    ProgramTooLarge,
    /// Program I/O was unavailable.
    #[error("worker program is unavailable")]
    Unavailable,
}

/// Exact executable admitted by path, metadata, and SHA-256 content.
#[derive(Clone, Debug)]
pub struct AdmittedProcessProgram {
    identity: SourceIdentifier,
    path: Arc<Path>,
    digest: EvidenceDigest,
}

impl AdmittedProcessProgram {
    /// Verifies an absolute non-symlink regular executable and its exact SHA-256 bytes.
    pub fn try_admit(
        identity: SourceIdentifier,
        path: impl AsRef<Path>,
        expected: EvidenceDigest,
    ) -> Result<Self, ProcessProgramError> {
        let path = path.as_ref();
        if !path.is_absolute() || expected.algorithm() != DigestAlgorithm::Sha256 {
            return Err(ProcessProgramError::InvalidProgram);
        }
        let metadata = path
            .symlink_metadata()
            .map_err(|_| ProcessProgramError::Unavailable)?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(ProcessProgramError::InvalidProgram);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            if metadata.permissions().mode() & 0o111 == 0 {
                return Err(ProcessProgramError::InvalidProgram);
            }
        }
        if metadata.len() > MAXIMUM_PROGRAM_BYTES {
            return Err(ProcessProgramError::ProgramTooLarge);
        }
        let canonical = path
            .canonicalize()
            .map_err(|_| ProcessProgramError::Unavailable)?;
        let digest = hash_file(&canonical, metadata.len())?;
        if digest != expected {
            return Err(ProcessProgramError::DigestMismatch);
        }
        Ok(Self {
            identity,
            path: Arc::from(canonical),
            digest,
        })
    }

    /// Stable code-owned program identity.
    #[must_use]
    pub const fn identity(&self) -> &SourceIdentifier {
        &self.identity
    }

    /// Exact digest rechecked immediately before every spawn.
    #[must_use]
    pub const fn digest(&self) -> EvidenceDigest {
        self.digest
    }
}

fn hash_file(path: &Path, expected_len: u64) -> Result<EvidenceDigest, ProcessProgramError> {
    let file = File::open(path).map_err(|_| ProcessProgramError::Unavailable)?;
    let mut hasher = Sha256::new();
    let copied = std::io::copy(&mut file.take(MAXIMUM_PROGRAM_BYTES + 1), &mut hasher)
        .map_err(|_| ProcessProgramError::Unavailable)?;
    if copied != expected_len || copied > MAXIMUM_PROGRAM_BYTES {
        return Err(ProcessProgramError::ProgramTooLarge);
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hasher.finalize().into(),
    ))
}

/// Checked worker process invocation. It cannot select an executable or environment.
#[derive(Clone, Debug)]
pub struct ContainedProcessRequest {
    program: AdmittedProcessProgram,
    arguments: Box<[String]>,
    stdin: Box<[u8]>,
}

impl ContainedProcessRequest {
    /// Creates a request with bounded NUL-free arguments and bounded input bytes.
    pub fn try_new(
        program: AdmittedProcessProgram,
        arguments: Vec<String>,
        stdin: Vec<u8>,
        maximum_stdin_bytes: usize,
    ) -> Result<Self, ContainedProcessError> {
        let argument_bytes = arguments
            .iter()
            .try_fold(0_usize, |total, value| total.checked_add(value.len()));
        if arguments.len() > MAXIMUM_ARGUMENTS
            || argument_bytes.is_none_or(|value| value > MAXIMUM_ARGUMENT_BYTES)
            || arguments.iter().any(|value| value.contains('\0'))
            || stdin.len() > maximum_stdin_bytes
        {
            return Err(ContainedProcessError::InvalidRequest);
        }
        Ok(Self {
            program,
            arguments: arguments.into_boxed_slice(),
            stdin: stdin.into_boxed_slice(),
        })
    }
}

/// Explicit process runtime and output ceilings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContainedProcessLimits {
    deadline: Duration,
    maximum_stdout_bytes: usize,
    maximum_stderr_bytes: usize,
}

impl ContainedProcessLimits {
    /// Creates positive ceilings with a deadline no longer than 24 hours.
    pub fn try_new(
        deadline: Duration,
        maximum_stdout_bytes: usize,
        maximum_stderr_bytes: usize,
    ) -> Result<Self, ContainedProcessError> {
        if deadline.is_zero()
            || deadline > Duration::from_secs(24 * 60 * 60)
            || maximum_stdout_bytes == 0
            || maximum_stderr_bytes == 0
        {
            return Err(ContainedProcessError::InvalidRequest);
        }
        Ok(Self {
            deadline,
            maximum_stdout_bytes,
            maximum_stderr_bytes,
        })
    }
}

/// Per-frame ceiling for optional live newline-delimited standard output.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContainedStdoutFrameLimits {
    maximum_frame_bytes: usize,
}

impl ContainedStdoutFrameLimits {
    /// Creates a positive frame-byte ceiling.
    pub fn try_new(maximum_frame_bytes: usize) -> Result<Self, ContainedProcessError> {
        if maximum_frame_bytes == 0 {
            return Err(ContainedProcessError::InvalidRequest);
        }
        Ok(Self {
            maximum_frame_bytes,
        })
    }
}

/// Opaque bounded live-frame capability consumed by one contained process run.
pub struct ContainedStdoutFrameSink {
    limits: ContainedStdoutFrameLimits,
    sender: tokio_mpsc::Sender<Box<[u8]>>,
}

impl std::fmt::Debug for ContainedStdoutFrameSink {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ContainedStdoutFrameSink")
            .field("limits", &self.limits)
            .field("sender", &"[BOUNDED STDOUT FRAME SENDER]")
            .finish()
    }
}

/// Exclusive receiver for one contained process's bounded live standard-output frames.
#[derive(Debug)]
pub struct ContainedStdoutFrameReceiver {
    receiver: tokio_mpsc::Receiver<Box<[u8]>>,
}

impl ContainedStdoutFrameReceiver {
    /// Waits for the next complete frame, or returns `None` after the run drops its sole sender.
    pub async fn recv(&mut self) -> Option<Box<[u8]>> {
        self.receiver.recv().await
    }
}

/// Creates one fixed-capacity live-frame channel for a single contained process run.
///
/// The sink is consumed by [`ContainedProcessSupervisor::run_with_stdout_frames`]. The receiver
/// must be drained concurrently with that future; eight queued frames are admitted before
/// backpressure fails the run closed.
#[must_use]
pub fn contained_stdout_frame_channel(
    limits: ContainedStdoutFrameLimits,
) -> (ContainedStdoutFrameSink, ContainedStdoutFrameReceiver) {
    let (sender, receiver) = tokio_mpsc::channel(STDOUT_FRAME_CHANNEL_CAPACITY);
    (
        ContainedStdoutFrameSink { limits, sender },
        ContainedStdoutFrameReceiver { receiver },
    )
}

/// Bounded process completion evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContainedProcessOutput {
    success: bool,
    stdout: Box<[u8]>,
    stderr: Box<[u8]>,
}

impl ContainedProcessOutput {
    /// Returns whether the supervised process exited successfully.
    #[must_use]
    pub const fn success(&self) -> bool {
        self.success
    }

    /// Returns the complete bounded standard-output bytes.
    #[must_use]
    pub fn stdout(&self) -> &[u8] {
        &self.stdout
    }

    /// Returns the complete bounded standard-error bytes.
    #[must_use]
    pub fn stderr(&self) -> &[u8] {
        &self.stderr
    }
}

/// Contained worker execution failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ContainedProcessError {
    /// Request or limit validation failed.
    #[error("contained worker request is invalid")]
    InvalidRequest,
    /// Program content changed after admission.
    #[error("contained worker program changed")]
    ProgramChanged,
    /// The worker could not be spawned or supervised.
    #[error("contained worker is unavailable")]
    Unavailable,
    /// The complete worker tree exceeded its deadline.
    #[error("contained worker exceeded its deadline")]
    Deadline,
    /// The complete worker tree was cancelled.
    #[error("contained worker was cancelled")]
    Cancelled,
    /// A worker output stream exceeded its exact byte ceiling.
    #[error("contained worker output exceeded its byte ceiling")]
    OutputTooLarge,
    /// One live standard-output frame exceeded its exact byte ceiling.
    #[error("contained worker stdout frame exceeded its byte ceiling")]
    StdoutFrameTooLarge,
    /// The worker ended with a nonempty standard-output frame lacking a newline delimiter.
    #[error("contained worker stdout ended with an incomplete frame")]
    StdoutFrameIncomplete,
    /// The bounded live-frame receiver was full or unavailable.
    #[error("contained worker stdout frame delivery failed")]
    StdoutFrameDelivery,
    /// Fixed cleanup ownership was exhausted before spawn.
    #[error("contained worker cleanup capacity is exhausted")]
    ReaperCapacity,
    /// Cleanup did not finish by deadline; ownership remains in fixed process-lifetime storage.
    #[error("contained worker cleanup remains pending")]
    CleanupPending,
}

/// Spawns only exact admitted programs under process-tree and output containment.
#[derive(Clone, Copy, Debug, Default)]
pub struct ContainedProcessSupervisor;

impl ContainedProcessSupervisor {
    /// Runs one contained process without shell lookup or inherited environment authority.
    pub async fn run(
        self,
        request: ContainedProcessRequest,
        limits: ContainedProcessLimits,
        cancellation: CancellationToken,
    ) -> Result<ContainedProcessOutput, ContainedProcessError> {
        let execution = ProcessExecutionReservation::try_acquire()?;
        tokio::task::spawn_blocking(move || {
            let _execution = execution;
            run_blocking(request, limits, cancellation, None)
        })
        .await
        .map_err(|_| ContainedProcessError::Unavailable)?
    }

    /// Runs one contained process while publishing bounded newline-delimited stdout frames.
    ///
    /// The returned future and the paired [`ContainedStdoutFrameReceiver`] must be polled
    /// concurrently. The sole sender is dropped before this future resolves, allowing the caller
    /// to drain every already-queued frame to `None` before accepting protocol completion.
    pub async fn run_with_stdout_frames(
        self,
        request: ContainedProcessRequest,
        limits: ContainedProcessLimits,
        cancellation: CancellationToken,
        frames: ContainedStdoutFrameSink,
    ) -> Result<ContainedProcessOutput, ContainedProcessError> {
        if frames.limits.maximum_frame_bytes > limits.maximum_stdout_bytes {
            return Err(ContainedProcessError::InvalidRequest);
        }
        let execution = ProcessExecutionReservation::try_acquire()?;
        tokio::task::spawn_blocking(move || {
            let _execution = execution;
            run_blocking(request, limits, cancellation, Some(frames))
        })
        .await
        .map_err(|_| ContainedProcessError::Unavailable)?
    }
}

fn run_blocking(
    request: ContainedProcessRequest,
    limits: ContainedProcessLimits,
    cancellation: CancellationToken,
    stdout_frames: Option<ContainedStdoutFrameSink>,
) -> Result<ContainedProcessOutput, ContainedProcessError> {
    let cleanup = ProcessCleanupReservation::try_acquire()?;
    let metadata = request
        .program
        .path
        .symlink_metadata()
        .map_err(|_| ContainedProcessError::ProgramChanged)?;
    let digest = hash_file(&request.program.path, metadata.len())
        .map_err(|_| ContainedProcessError::ProgramChanged)?;
    if digest != request.program.digest {
        return Err(ContainedProcessError::ProgramChanged);
    }

    let mut command = CommandWrap::with_new(&*request.program.path, |command| {
        command
            .args(request.arguments.iter())
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
    });
    #[cfg(unix)]
    command.wrap(ProcessGroup::leader());
    #[cfg(windows)]
    command.wrap(JobObject);
    let mut child = command
        .spawn()
        .map_err(|_| ContainedProcessError::Unavailable)?;
    // `process-wrap` must ultimately spawn by path. Rechecking immediately after spawn narrows and
    // detects path substitution; the retained digest remains the publication evidence boundary.
    let post_spawn_metadata = match request.program.path.symlink_metadata() {
        Ok(metadata) => metadata,
        Err(_) => {
            return fail_after_spawn(child, cleanup, ContainedProcessError::ProgramChanged);
        }
    };
    let post_spawn_digest = match hash_file(&request.program.path, post_spawn_metadata.len()) {
        Ok(digest) => digest,
        Err(_) => {
            return fail_after_spawn(child, cleanup, ContainedProcessError::ProgramChanged);
        }
    };
    if post_spawn_digest != request.program.digest {
        return fail_after_spawn(child, cleanup, ContainedProcessError::ProgramChanged);
    }

    let Some(mut stdin) = child.stdin().take() else {
        return fail_after_spawn(child, cleanup, ContainedProcessError::Unavailable);
    };
    if stdin.write_all(&request.stdin).is_err() {
        drop(stdin);
        return fail_after_spawn(child, cleanup, ContainedProcessError::Unavailable);
    }
    drop(stdin);
    let Some(stdout) = child.stdout().take() else {
        return fail_after_spawn(child, cleanup, ContainedProcessError::Unavailable);
    };
    let Some(stderr) = child.stderr().take() else {
        return fail_after_spawn(child, cleanup, ContainedProcessError::Unavailable);
    };
    let (sender, receiver) = std_mpsc::sync_channel(2);
    let stdout_reader = spawn_reader(
        OutputStream::Stdout,
        stdout,
        limits.maximum_stdout_bytes,
        sender.clone(),
        stdout_frames,
    );
    let stderr_reader = spawn_reader(
        OutputStream::Stderr,
        stderr,
        limits.maximum_stderr_bytes,
        sender,
        None,
    );
    let started = Instant::now();
    let mut status: Option<ExitStatus> = None;
    let mut stdout = None;
    let mut stderr = None;
    let mut failure = None;
    let mut cleanup_started = None;

    while status.is_none() || stdout.is_none() || stderr.is_none() {
        if failure.is_none() && cancellation.is_cancelled() {
            failure = Some(ContainedProcessError::Cancelled);
        }
        if failure.is_none() && started.elapsed() >= limits.deadline {
            failure = Some(ContainedProcessError::Deadline);
        }
        while let Ok(message) = receiver.try_recv() {
            match message.result {
                Ok(output) => match message.stream {
                    OutputStream::Stdout => stdout = Some(output),
                    OutputStream::Stderr => stderr = Some(output),
                },
                Err(error) => {
                    failure.get_or_insert(error);
                }
            };
        }
        if status.is_none() {
            status = match child.try_wait() {
                Ok(status) => status,
                Err(_) => {
                    failure.get_or_insert(ContainedProcessError::Unavailable);
                    None
                }
            };
            if status.is_some() {
                // The leader may exit while descendants retain its pipes. Explicitly terminate
                // the group/job so a successful leader cannot leave an unowned process tree.
                if child.start_kill().is_err() {
                    failure.get_or_insert(ContainedProcessError::Unavailable);
                }
                cleanup_started.get_or_insert_with(Instant::now);
            }
        }
        if failure.is_some() && cleanup_started.is_none() {
            if child.start_kill().is_err() {
                failure = Some(ContainedProcessError::Unavailable);
            }
            cleanup_started = Some(Instant::now());
        }
        if cleanup_started.is_some_and(|started| started.elapsed() >= PROCESS_CLEANUP_DEADLINE) {
            cleanup.retain(child, vec![stdout_reader, stderr_reader])?;
            return Err(ContainedProcessError::CleanupPending);
        }
        if status.is_none() || stdout.is_none() || stderr.is_none() {
            thread::sleep(Duration::from_millis(5));
        }
    }
    let stdout_joined = join_reader(stdout_reader).is_ok();
    let stderr_joined = join_reader(stderr_reader).is_ok();
    if !stdout_joined || !stderr_joined {
        failure = Some(ContainedProcessError::Unavailable);
    }
    if wait_after_tree_termination(&mut *child).is_err() {
        cleanup.retain(child, Vec::new())?;
        return Err(ContainedProcessError::CleanupPending);
    }
    if let Some(failure) = failure {
        return Err(failure);
    }
    let status = status.ok_or(ContainedProcessError::Unavailable)?;
    let stdout = stdout.ok_or(ContainedProcessError::Unavailable)?;
    let stderr = stderr.ok_or(ContainedProcessError::Unavailable)?;
    Ok(ContainedProcessOutput {
        success: status.success(),
        stdout: stdout.into_boxed_slice(),
        stderr: stderr.into_boxed_slice(),
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OutputStream {
    Stdout,
    Stderr,
}

#[derive(Debug)]
struct ReaderMessage {
    stream: OutputStream,
    result: Result<Vec<u8>, ContainedProcessError>,
}

fn spawn_reader<R: Read + Send + 'static>(
    stream: OutputStream,
    mut reader: R,
    limit: usize,
    sender: std_mpsc::SyncSender<ReaderMessage>,
    stdout_frames: Option<ContainedStdoutFrameSink>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut output = Vec::new();
        let mut frame_start = 0_usize;
        let mut buffer = [0_u8; 8 * 1024];
        let result = loop {
            let read = match reader.read(&mut buffer) {
                Ok(read) => read,
                Err(_) => break Err(ContainedProcessError::Unavailable),
            };
            if read == 0 {
                break Ok(output);
            }
            if output.len().saturating_add(read) > limit {
                break Err(ContainedProcessError::OutputTooLarge);
            }
            output.extend_from_slice(&buffer[..read]);
            if let Some(frames) = stdout_frames.as_ref()
                && let Err(error) = publish_stdout_frames(&output, &mut frame_start, frames)
            {
                break Err(error);
            }
        };
        let result = match (result, stdout_frames.as_ref()) {
            (Ok(_output), Some(frames)) if frames.sender.is_closed() => {
                Err(ContainedProcessError::StdoutFrameDelivery)
            }
            (Ok(output), Some(_frames)) if frame_start != output.len() => {
                Err(ContainedProcessError::StdoutFrameIncomplete)
            }
            (result, _) => result,
        };
        let _ignored = sender.send(ReaderMessage { stream, result });
    })
}

fn publish_stdout_frames(
    output: &[u8],
    frame_start: &mut usize,
    frames: &ContainedStdoutFrameSink,
) -> Result<(), ContainedProcessError> {
    while let Some(relative_end) = output[*frame_start..]
        .iter()
        .position(|byte| *byte == b'\n')
    {
        let frame_end = (*frame_start)
            .checked_add(relative_end)
            .ok_or(ContainedProcessError::StdoutFrameTooLarge)?;
        let frame = output
            .get(*frame_start..frame_end)
            .ok_or(ContainedProcessError::Unavailable)?;
        let frame = frame.strip_suffix(b"\r").unwrap_or(frame);
        if frame.len() > frames.limits.maximum_frame_bytes {
            return Err(ContainedProcessError::StdoutFrameTooLarge);
        }
        frames
            .sender
            .try_send(frame.to_vec().into_boxed_slice())
            .map_err(|_error| ContainedProcessError::StdoutFrameDelivery)?;
        *frame_start = frame_end
            .checked_add(1)
            .ok_or(ContainedProcessError::StdoutFrameTooLarge)?;
    }
    let pending = output
        .len()
        .checked_sub(*frame_start)
        .ok_or(ContainedProcessError::Unavailable)?;
    let maximum_pending = frames.limits.maximum_frame_bytes.saturating_add(1);
    if pending > maximum_pending {
        return Err(ContainedProcessError::StdoutFrameTooLarge);
    }
    Ok(())
}

fn join_reader(handle: thread::JoinHandle<()>) -> Result<(), ContainedProcessError> {
    handle
        .join()
        .map_err(|_| ContainedProcessError::Unavailable)
}

fn wait_after_tree_termination(
    child: &mut dyn process_wrap::std::ChildWrapper,
) -> std::io::Result<ExitStatus> {
    #[cfg(windows)]
    {
        // `JobObjectChild::wait` waits without a deadline for another completion-port message.
        // Successfully request termination of every current job member again, then reap the
        // leader through the inner child so an already-consumed or omitted best-effort job
        // notification cannot strand cleanup. Closing the job handle cannot release a
        // still-terminating tree.
        child.start_kill()?;
        child.inner_mut().wait()
    }
    #[cfg(not(windows))]
    {
        child.wait()
    }
}

fn fail_after_spawn(
    mut child: Box<dyn process_wrap::std::ChildWrapper>,
    cleanup: ProcessCleanupReservation,
    failure: ContainedProcessError,
) -> Result<ContainedProcessOutput, ContainedProcessError> {
    let _kill_requested = child.start_kill().is_ok();
    let started = Instant::now();
    while started.elapsed() < PROCESS_CLEANUP_DEADLINE {
        match child.try_wait() {
            Ok(Some(_status)) if wait_after_tree_termination(&mut *child).is_ok() => {
                return Err(failure);
            }
            Ok(None) | Err(_) => thread::sleep(Duration::from_millis(5)),
            Ok(Some(_status)) => thread::sleep(Duration::from_millis(5)),
        }
    }
    cleanup.retain(child, Vec::new())?;
    Err(ContainedProcessError::CleanupPending)
}

/// Hard ceilings for one machine-readable worker stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerProtocolLimits {
    maximum_event_bytes: usize,
    maximum_events: usize,
}

impl WorkerProtocolLimits {
    /// Creates positive event byte/count limits.
    pub fn try_new(
        maximum_event_bytes: usize,
        maximum_events: usize,
    ) -> Result<Self, WorkerProtocolError> {
        if maximum_event_bytes == 0 || maximum_events == 0 || maximum_events > 1_000_000 {
            return Err(WorkerProtocolError::InvalidLimits);
        }
        Ok(Self {
            maximum_event_bytes,
            maximum_events,
        })
    }
}

/// Validated worker event. A result remains staged until successful stream completion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkerEvent {
    /// Named phase changed.
    Phase(SourceIdentifier),
    /// Redaction-safe warning was emitted.
    Warning(SourceIdentifier),
    /// Candidate terminal result; the job authority has not published it.
    Result(JobResultReference),
}

/// Worker protocol validation failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum WorkerProtocolError {
    /// A configured ceiling was zero or excessive.
    #[error("worker protocol limits are invalid")]
    InvalidLimits,
    /// An encoded event exceeded the exact byte ceiling.
    #[error("worker event exceeds its byte ceiling")]
    EventTooLarge,
    /// Event syntax or shape was not admitted.
    #[error("worker event is invalid")]
    InvalidEvent,
    /// Event count exceeded its exact ceiling.
    #[error("worker stream exceeds its event ceiling")]
    EventLimitExceeded,
    /// The protocol session already failed or finished.
    #[error("worker protocol session is sealed")]
    Sealed,
    /// Successful completion omitted a result.
    #[error("worker protocol result is missing")]
    MissingResult,
}

/// Fail-closed worker stream validator that withholds result publication until clean EOF/exit.
#[derive(Debug)]
pub struct WorkerProtocolSession {
    limits: WorkerProtocolLimits,
    event_count: usize,
    result: Option<JobResultReference>,
    sealed: bool,
}

impl WorkerProtocolSession {
    /// Starts an empty bounded stream.
    #[must_use]
    pub const fn new(limits: WorkerProtocolLimits) -> Self {
        Self {
            limits,
            event_count: 0,
            result: None,
            sealed: false,
        }
    }

    /// Accepts one already decoded event under count and ordering rules.
    pub fn accept(&mut self, event: WorkerEvent) -> Result<(), WorkerProtocolError> {
        if self.sealed {
            return Err(WorkerProtocolError::Sealed);
        }
        self.event_count = self
            .event_count
            .checked_add(1)
            .ok_or(WorkerProtocolError::EventLimitExceeded)?;
        if self.event_count > self.limits.maximum_events {
            self.fail();
            return Err(WorkerProtocolError::EventLimitExceeded);
        }
        if let WorkerEvent::Result(result) = event
            && self.result.replace(result).is_some()
        {
            self.fail();
            return Err(WorkerProtocolError::InvalidEvent);
        }
        Ok(())
    }

    /// Validates one encoded non-result event without retaining arbitrary worker data.
    pub fn accept_encoded(&mut self, encoded: &[u8]) -> Result<(), WorkerProtocolError> {
        if self.sealed {
            return Err(WorkerProtocolError::Sealed);
        }
        if encoded.len() > self.limits.maximum_event_bytes {
            self.fail();
            return Err(WorkerProtocolError::EventTooLarge);
        }
        let value: Value = serde_json::from_slice(encoded).map_err(|_| {
            self.fail();
            WorkerProtocolError::InvalidEvent
        })?;
        let kind = value
            .as_object()
            .and_then(|object| object.get("kind"))
            .and_then(Value::as_str);
        if !matches!(kind, Some("phase" | "progress" | "warning" | "error")) {
            self.fail();
            return Err(WorkerProtocolError::InvalidEvent);
        }
        self.event_count = self.event_count.saturating_add(1);
        if self.event_count > self.limits.maximum_events {
            self.fail();
            return Err(WorkerProtocolError::EventLimitExceeded);
        }
        Ok(())
    }

    /// Releases the staged result only after the worker stream and process completed successfully.
    pub fn finish_success(&mut self) -> Result<JobResultReference, WorkerProtocolError> {
        if self.sealed {
            return Err(WorkerProtocolError::Sealed);
        }
        self.sealed = true;
        self.result.take().ok_or(WorkerProtocolError::MissingResult)
    }

    /// Seals a crashed or abnormally terminated worker and destroys any staged result.
    pub fn finish_crashed(&mut self) {
        self.fail();
    }

    /// Returns a staged result only while the session remains valid.
    #[must_use]
    pub const fn result(&self) -> Option<&JobResultReference> {
        if self.sealed {
            None
        } else {
            self.result.as_ref()
        }
    }

    fn fail(&mut self) {
        self.sealed = true;
        self.result = None;
    }
}
