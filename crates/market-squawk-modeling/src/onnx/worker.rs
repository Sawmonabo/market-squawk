//! Parent-owned supervision for resource-bounded ONNX helper processes.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};
use tempfile::TempDir;
use thiserror::Error;

mod process;
mod protocol;
mod resources;

pub use process::run_onnx_worker_process;
use protocol::{WorkerInitialization, response_loop};

const MAX_WORKER_PROGRAM_BYTES: u64 = 256 * 1024 * 1024;
const MAX_GENERATION_STARTUP: Duration = Duration::from_secs(15);
const MAX_REAPER_REQUESTS: usize = 1;

/// Exact private helper executable admitted by application composition.
#[derive(Clone, Debug)]
pub struct OnnxWorkerProgram {
    inner: Arc<WorkerProgramInner>,
}

#[derive(Debug)]
struct WorkerProgramInner {
    executable: PathBuf,
    digest: [u8; 32],
    active_generations: AtomicUsize,
    _private_generation: TempDir,
}

impl OnnxWorkerProgram {
    /// Copies one exact helper executable into a private, content-addressed generation.
    ///
    /// The helper is always spawned by this absolute sealed path; `PATH` lookup is never used.
    ///
    /// # Errors
    ///
    /// Rejects a symlink, non-file, oversized, unreadable, changed, or digest-mismatched helper.
    pub fn admit(
        executable: impl AsRef<Path>,
        expected_digest: [u8; 32],
    ) -> Result<Self, OnnxWorkerProgramError> {
        if expected_digest == [0; 32]
            || fs::symlink_metadata(executable.as_ref())
                .map_err(|_| OnnxWorkerProgramError::Unavailable)?
                .file_type()
                .is_symlink()
        {
            return Err(OnnxWorkerProgramError::Invalid);
        }
        let source_path =
            fs::canonicalize(executable).map_err(|_| OnnxWorkerProgramError::Unavailable)?;
        if !source_path.is_absolute() {
            return Err(OnnxWorkerProgramError::Invalid);
        }
        let mut source =
            File::open(&source_path).map_err(|_| OnnxWorkerProgramError::Unavailable)?;
        let metadata = source
            .metadata()
            .map_err(|_| OnnxWorkerProgramError::Unavailable)?;
        if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_WORKER_PROGRAM_BYTES {
            return Err(OnnxWorkerProgramError::Invalid);
        }
        let actual_digest = hash_open_file(&mut source, MAX_WORKER_PROGRAM_BYTES)
            .map_err(|_| OnnxWorkerProgramError::Changed)?;
        if actual_digest != expected_digest
            || source
                .metadata()
                .map_err(|_| OnnxWorkerProgramError::Changed)?
                .len()
                != metadata.len()
        {
            return Err(OnnxWorkerProgramError::Changed);
        }
        source
            .seek(SeekFrom::Start(0))
            .map_err(|_| OnnxWorkerProgramError::Unavailable)?;
        let private_generation = tempfile::Builder::new()
            .prefix("market-squawk-onnx-worker-")
            .tempdir()
            .map_err(|_| OnnxWorkerProgramError::Unavailable)?;
        let file_name = source_path
            .file_name()
            .ok_or(OnnxWorkerProgramError::Invalid)?;
        let sealed_name = format!(
            "{}-{}",
            encode_digest(expected_digest),
            file_name.to_string_lossy()
        );
        let sealed_path = private_generation.path().join(sealed_name);
        let mut sealed = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&sealed_path)
            .map_err(|_| OnnxWorkerProgramError::Unavailable)?;
        io::copy(&mut source.take(MAX_WORKER_PROGRAM_BYTES + 1), &mut sealed)
            .map_err(|_| OnnxWorkerProgramError::Unavailable)?;
        sealed
            .sync_all()
            .map_err(|_| OnnxWorkerProgramError::Unavailable)?;
        set_worker_permissions(&sealed_path)?;
        let mut sealed =
            File::open(&sealed_path).map_err(|_| OnnxWorkerProgramError::Unavailable)?;
        if hash_open_file(&mut sealed, MAX_WORKER_PROGRAM_BYTES)
            .map_err(|_| OnnxWorkerProgramError::Changed)?
            != expected_digest
        {
            return Err(OnnxWorkerProgramError::Changed);
        }
        Ok(Self {
            inner: Arc::new(WorkerProgramInner {
                executable: sealed_path,
                digest: expected_digest,
                active_generations: AtomicUsize::new(0),
                _private_generation: private_generation,
            }),
        })
    }

    /// Returns the number of owned helper processes that have not yet been reaped.
    #[must_use]
    pub fn active_generations(&self) -> usize {
        self.inner.active_generations.load(Ordering::Acquire)
    }

    fn verify(&self) -> Result<(), WorkerError> {
        let mut executable = File::open(&self.inner.executable).map_err(|_| WorkerError::Load)?;
        let digest = hash_open_file(&mut executable, MAX_WORKER_PROGRAM_BYTES)
            .map_err(|_| WorkerError::Load)?;
        (digest == self.inner.digest)
            .then_some(())
            .ok_or(WorkerError::Load)
    }
}

/// Helper-program admission failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum OnnxWorkerProgramError {
    #[error("ONNX worker executable reference is invalid")]
    Invalid,
    #[error("ONNX worker executable is unavailable")]
    Unavailable,
    #[error("ONNX worker executable changed or its digest differs")]
    Changed,
}

#[cfg(unix)]
fn set_worker_permissions(path: &Path) -> Result<(), OnnxWorkerProgramError> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o500))
        .map_err(|_| OnnxWorkerProgramError::Unavailable)
}

#[cfg(not(unix))]
fn set_worker_permissions(_path: &Path) -> Result<(), OnnxWorkerProgramError> {
    Ok(())
}

#[derive(Debug)]
pub(crate) struct OnnxWorker {
    #[cfg(feature = "onnx-runtime")]
    program: OnnxWorkerProgram,
    generation: Mutex<Option<Generation>>,
    reaper: GenerationReaper,
    deadline: Duration,
}

#[derive(Debug)]
struct Generation {
    child: Child,
    stdin: Option<ChildStdin>,
    responses: Receiver<Result<f32, WorkerError>>,
    reader: Option<JoinHandle<()>>,
    active_generations: Arc<WorkerProgramInner>,
}

type WriterHandle = JoinHandle<Result<ChildStdin, WorkerError>>;

#[derive(Debug)]
struct ReapRequest {
    generation: Generation,
    writer: Option<WriterHandle>,
}

impl ReapRequest {
    fn signal_termination(&mut self) {
        self.generation.stdin.take();
        let _ = self.generation.child.kill();
    }

    fn terminate(mut self) {
        self.signal_termination();
        let _ = self.generation.child.wait();
        if let Some(writer) = self.writer.take() {
            let _ = writer.join();
        }
        if let Some(reader) = self.generation.reader.take() {
            let _ = reader.join();
        }
        self.generation
            .active_generations
            .active_generations
            .fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Debug)]
struct GenerationReaper {
    sender: Option<SyncSender<ReapRequest>>,
    observer: Option<JoinHandle<()>>,
    failed_handoff: Mutex<Vec<ReapRequest>>,
}

impl GenerationReaper {
    fn start() -> Result<Self, WorkerError> {
        let (sender, receiver) = mpsc::sync_channel::<ReapRequest>(MAX_REAPER_REQUESTS);
        let observer = thread::Builder::new()
            .name("market-squawk-onnx-reaper".to_owned())
            .spawn(move || {
                while let Ok(request) = receiver.recv() {
                    request.terminate();
                }
            })
            .map_err(|_| WorkerError::Unavailable)?;
        Ok(Self {
            sender: Some(sender),
            observer: Some(observer),
            // One worker owns one generation, so only one handoff can occur. Reserving the sole
            // fallback slot keeps the disconnected-channel disposition allocation-free.
            failed_handoff: Mutex::new(Vec::with_capacity(MAX_REAPER_REQUESTS)),
        })
    }

    fn handoff(&self, request: ReapRequest) {
        let failed = match self.sender.as_ref() {
            Some(sender) => match sender.try_send(request) {
                Ok(()) => return,
                Err(TrySendError::Full(request) | TrySendError::Disconnected(request)) => request,
            },
            None => request,
        };
        let mut failed = failed;
        failed.signal_termination();
        let mut retained = match self.failed_handoff.lock() {
            Ok(retained) => retained,
            Err(poisoned) => poisoned.into_inner(),
        };
        retained.push(failed);
    }
}

impl Drop for GenerationReaper {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(observer) = self.observer.take() {
            let _ = observer.join();
        }
        let retained = match self.failed_handoff.get_mut() {
            Ok(retained) => retained,
            Err(poisoned) => poisoned.into_inner(),
        };
        for request in retained.drain(..) {
            request.terminate();
        }
    }
}

#[derive(Debug)]
enum GenerationExecution {
    Complete(f32),
    RetainedFailure(WorkerError),
    TerminalFailure {
        error: WorkerError,
        writer: Option<WriterHandle>,
    },
}

impl OnnxWorker {
    pub(crate) fn start_tract(
        program: &OnnxWorkerProgram,
        model_bytes: &[u8],
        input_shape: &[usize],
        input_elements: usize,
        deadline: Duration,
    ) -> Result<(Self, f32), WorkerError> {
        Self::start(
            program,
            WorkerInitialization::tract(model_bytes, input_shape, input_elements)?,
            deadline,
        )
    }

    #[cfg(feature = "onnx-runtime")]
    #[allow(
        clippy::too_many_arguments,
        reason = "the exact external runtime identity remains explicit at the process boundary"
    )]
    pub(crate) fn start_external(
        program: &OnnxWorkerProgram,
        model_bytes: &[u8],
        input_shape: &[usize],
        input_elements: usize,
        deadline: Duration,
        runtime_path: &Path,
        runtime_digest: [u8; 32],
        runtime_version: &str,
        runtime_platform: u8,
    ) -> Result<(Self, f32), WorkerError> {
        Self::start(
            program,
            WorkerInitialization::external(
                model_bytes,
                input_shape,
                input_elements,
                runtime_path,
                runtime_digest,
                runtime_version,
                runtime_platform,
            )?,
            deadline,
        )
    }

    fn start(
        program: &OnnxWorkerProgram,
        initialization: WorkerInitialization,
        deadline: Duration,
    ) -> Result<(Self, f32), WorkerError> {
        let startup_deadline = Instant::now()
            .checked_add(MAX_GENERATION_STARTUP)
            .ok_or(WorkerError::Deadline)?;
        let input_elements = initialization.input_elements;
        program.verify()?;
        let mut child = Command::new(&program.inner.executable)
            .arg("--stdio-worker")
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|_| WorkerError::Load)?;
        let Some(stdin) = child.stdin.take() else {
            kill_and_reap(&mut child);
            return Err(WorkerError::Load);
        };
        let Some(stdout) = child.stdout.take() else {
            drop(stdin);
            kill_and_reap(&mut child);
            return Err(WorkerError::Load);
        };
        program
            .inner
            .active_generations
            .fetch_add(1, Ordering::AcqRel);
        let (responses_sender, responses) = mpsc::sync_channel(1);
        let reader = match thread::Builder::new()
            .name("market-squawk-onnx-response".to_owned())
            .spawn(move || response_loop(stdout, responses_sender))
        {
            Ok(reader) => reader,
            Err(_) => {
                kill_and_reap(&mut child);
                program
                    .inner
                    .active_generations
                    .fetch_sub(1, Ordering::AcqRel);
                return Err(WorkerError::Load);
            }
        };
        let mut generation = Generation {
            child,
            stdin: None,
            responses,
            reader: Some(reader),
            active_generations: Arc::clone(&program.inner),
        };
        let writer = match spawn_writer(stdin, initialization.bytes) {
            Ok(writer) => writer,
            Err(error) => {
                terminate_generation(generation, None);
                return Err(error);
            }
        };
        let response = receive_until(&generation.responses, startup_deadline);
        match response {
            Ok(_) => {
                let stdin = match join_writer(writer) {
                    Ok(stdin) => stdin,
                    Err(error) => {
                        terminate_generation(generation, None);
                        return Err(error);
                    }
                };
                generation.stdin = Some(stdin);
                let reaper = match GenerationReaper::start() {
                    Ok(reaper) => reaper,
                    Err(error) => {
                        terminate_generation(generation, None);
                        return Err(error);
                    }
                };
                let worker = Self {
                    #[cfg(feature = "onnx-runtime")]
                    program: program.clone(),
                    generation: Mutex::new(Some(generation)),
                    reaper,
                    deadline,
                };
                let warm_up_values = vec![0.0; input_elements];
                let warm_up_deadline = Instant::now()
                    .checked_add(deadline)
                    .ok_or(WorkerError::Deadline)?;
                let warm_up = worker.execute_until(warm_up_values, warm_up_deadline)?;
                Ok((worker, warm_up))
            }
            Err(error) => {
                terminate_generation(generation, Some(writer));
                Err(error)
            }
        }
    }

    pub(crate) fn execute_until(
        &self,
        values: Vec<f32>,
        absolute_deadline: Instant,
    ) -> Result<f32, WorkerError> {
        self.execute_until_with_time_source(values, absolute_deadline, Instant::now)
    }

    fn execute_until_with_time_source<F>(
        &self,
        values: Vec<f32>,
        absolute_deadline: Instant,
        mut now: F,
    ) -> Result<f32, WorkerError>
    where
        F: FnMut() -> Instant,
    {
        if values.len() > super::MAX_ONNX_REQUEST_ELEMENTS {
            return Err(WorkerError::Resource);
        }
        let mut generation = {
            let mut state = match self.generation.lock() {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
            if now() >= absolute_deadline {
                return Err(WorkerError::Deadline);
            }
            state.take().ok_or(WorkerError::Unavailable)?
        };
        match execute_generation(&mut generation, values, absolute_deadline, &mut now) {
            GenerationExecution::Complete(score) => {
                self.restore_generation(generation);
                Ok(score)
            }
            GenerationExecution::RetainedFailure(error) => {
                self.restore_generation(generation);
                Err(error)
            }
            GenerationExecution::TerminalFailure { error, writer } => {
                self.reaper.handoff(ReapRequest { generation, writer });
                Err(error)
            }
        }
    }

    fn restore_generation(&self, generation: Generation) {
        let mut state = match self.generation.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        let displaced = state.replace(generation);
        drop(state);
        if let Some(generation) = displaced {
            self.reaper.handoff(ReapRequest {
                generation,
                writer: None,
            });
        }
    }

    pub(crate) const fn deadline(&self) -> Duration {
        self.deadline
    }

    #[cfg(feature = "onnx-runtime")]
    pub(crate) fn program(&self) -> &OnnxWorkerProgram {
        &self.program
    }
}

impl Drop for OnnxWorker {
    fn drop(&mut self) {
        let state = match self.generation.get_mut() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(generation) = state.take() {
            terminate_generation(generation, None);
        }
    }
}

fn execute_generation<F>(
    generation: &mut Generation,
    values: Vec<f32>,
    deadline: Instant,
    now: &mut F,
) -> GenerationExecution
where
    F: FnMut() -> Instant,
{
    let mut bytes = Vec::new();
    if bytes
        .try_reserve_exact(5_usize.saturating_add(values.len().saturating_mul(4)))
        .is_err()
    {
        return GenerationExecution::RetainedFailure(WorkerError::Resource);
    }
    bytes.push(protocol::REQUEST_INFER);
    let Ok(value_count) = u32::try_from(values.len()) else {
        return GenerationExecution::RetainedFailure(WorkerError::Resource);
    };
    bytes.extend_from_slice(&value_count.to_be_bytes());
    for value in values {
        bytes.extend_from_slice(&value.to_bits().to_be_bytes());
    }
    if now() >= deadline {
        return GenerationExecution::RetainedFailure(WorkerError::Deadline);
    }
    let Some(stdin) = generation.stdin.take() else {
        return GenerationExecution::TerminalFailure {
            error: WorkerError::Unavailable,
            writer: None,
        };
    };
    let writer = match spawn_writer(stdin, bytes) {
        Ok(writer) => writer,
        Err(error) => {
            return GenerationExecution::TerminalFailure {
                error,
                writer: None,
            };
        }
    };
    let response = receive_until(&generation.responses, deadline);
    match response {
        Ok(score) => match join_writer(writer) {
            Ok(stdin) => {
                generation.stdin = Some(stdin);
                GenerationExecution::Complete(score)
            }
            Err(error) => GenerationExecution::TerminalFailure {
                error,
                writer: None,
            },
        },
        Err(error) => GenerationExecution::TerminalFailure {
            error,
            writer: Some(writer),
        },
    }
}

fn spawn_writer(stdin: ChildStdin, bytes: Vec<u8>) -> Result<WriterHandle, WorkerError> {
    thread::Builder::new()
        .name("market-squawk-onnx-request".to_owned())
        .spawn(move || {
            let mut writer = BufWriter::new(stdin);
            writer
                .write_all(&bytes)
                .map_err(|_| WorkerError::Unavailable)?;
            writer.flush().map_err(|_| WorkerError::Unavailable)?;
            writer.into_inner().map_err(|_| WorkerError::Unavailable)
        })
        .map_err(|_| WorkerError::Unavailable)
}

fn join_writer(
    writer: JoinHandle<Result<ChildStdin, WorkerError>>,
) -> Result<ChildStdin, WorkerError> {
    writer.join().map_err(|_| WorkerError::Unavailable)?
}

fn receive_until(
    responses: &Receiver<Result<f32, WorkerError>>,
    deadline: Instant,
) -> Result<f32, WorkerError> {
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or(WorkerError::Deadline)?;
    match responses.recv_timeout(remaining) {
        Ok(result) => result,
        Err(mpsc::RecvTimeoutError::Timeout) => Err(WorkerError::Deadline),
        Err(mpsc::RecvTimeoutError::Disconnected) => Err(WorkerError::Unavailable),
    }
}

fn terminate_generation(generation: Generation, writer: Option<WriterHandle>) {
    ReapRequest { generation, writer }.terminate();
}

fn hash_open_file(file: &mut File, limit: u64) -> io::Result<[u8; 32]> {
    file.seek(SeekFrom::Start(0))?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > limit {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bounded file size",
        ));
    }
    let mut digest = Sha256::new();
    let mut total = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        total = total
            .checked_add(u64::try_from(read).map_err(io::Error::other)?)
            .filter(|value| *value <= limit)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bounded file size"))?;
        digest.update(&buffer[..read]);
    }
    if total != metadata.len() || file.metadata()?.len() != metadata.len() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "file changed"));
    }
    Ok(digest.finalize().into())
}

fn kill_and_reap(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn encode_digest(digest: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkerError {
    Load,
    Resource,
    Unavailable,
    Deadline,
    Runtime,
}

/// Private helper startup or protocol failure.
#[derive(Debug, Error)]
pub enum OnnxWorkerProcessError {
    #[error("ONNX worker process protocol failed")]
    Protocol,
    #[error("ONNX worker process resource containment failed")]
    Resource,
    #[error("ONNX worker process runtime failed")]
    Runtime,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn post_take_expiry_preserves_the_retained_generation() -> Result<(), Box<dyn std::error::Error>>
    {
        let private_generation = tempfile::tempdir()?;
        let executable = std::env::current_exe()?;
        let program_inner = Arc::new(WorkerProgramInner {
            executable,
            digest: [1; 32],
            active_generations: AtomicUsize::new(1),
            _private_generation: private_generation,
        });
        let mut child = Command::new(std::env::current_exe()?)
            .arg("--list")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        let stdin = child.stdin.take();
        let (responses_sender, responses) = mpsc::sync_channel(1);
        drop(responses_sender);
        let worker = OnnxWorker {
            #[cfg(feature = "onnx-runtime")]
            program: OnnxWorkerProgram {
                inner: Arc::clone(&program_inner),
            },
            generation: Mutex::new(Some(Generation {
                child,
                stdin,
                responses,
                reader: None,
                active_generations: program_inner,
            })),
            reaper: GenerationReaper::start().map_err(|_| "reaper unavailable")?,
            deadline: Duration::from_millis(10),
        };

        let before_deadline = Instant::now();
        let deadline = before_deadline + Duration::from_millis(1);
        let after_deadline = deadline + Duration::from_millis(1);
        let mut times = [before_deadline, after_deadline].into_iter();

        assert_eq!(
            worker.execute_until_with_time_source(Vec::new(), deadline, || {
                times.next().map_or(after_deadline, |now| now)
            }),
            Err(WorkerError::Deadline)
        );
        assert!(
            worker
                .generation
                .lock()
                .map_err(|_| "worker generation lock poisoned")?
                .is_some()
        );
        Ok(())
    }

    #[test]
    fn dispatched_failure_hands_cleanup_off_before_reader_join()
    -> Result<(), Box<dyn std::error::Error>> {
        let private_generation = tempfile::tempdir()?;
        let executable = std::env::current_exe()?;
        let program_inner = Arc::new(WorkerProgramInner {
            executable,
            digest: [1; 32],
            active_generations: AtomicUsize::new(1),
            _private_generation: private_generation,
        });
        let mut child = Command::new(std::env::current_exe()?)
            .arg("--list")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        let stdin = child.stdin.take();
        let (responses_sender, responses) = mpsc::sync_channel(1);
        drop(responses_sender);
        let (cleanup_release, cleanup_wait) = mpsc::sync_channel(1);
        let reader = thread::spawn(move || {
            let _ = cleanup_wait.recv();
        });
        let worker = Arc::new(OnnxWorker {
            #[cfg(feature = "onnx-runtime")]
            program: OnnxWorkerProgram {
                inner: Arc::clone(&program_inner),
            },
            generation: Mutex::new(Some(Generation {
                child,
                stdin,
                responses,
                reader: Some(reader),
                active_generations: Arc::clone(&program_inner),
            })),
            reaper: GenerationReaper::start().map_err(|_| "reaper unavailable")?,
            deadline: Duration::from_secs(1),
        });

        let request_worker = Arc::clone(&worker);
        let (result_sender, result_receiver) = mpsc::sync_channel(1);
        let request = thread::spawn(move || {
            let result =
                request_worker.execute_until(Vec::new(), Instant::now() + Duration::from_secs(1));
            let _ = result_sender.send(result);
        });
        assert_eq!(
            result_receiver.recv_timeout(Duration::from_secs(1))?,
            Err(WorkerError::Unavailable)
        );
        cleanup_release.send(())?;
        request.join().map_err(|_| "request thread panicked")?;
        drop(worker);
        assert_eq!(program_inner.active_generations.load(Ordering::Acquire), 0);
        Ok(())
    }
}
