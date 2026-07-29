//! Bounded, descriptor-checked helpers shared by the build script and its adversarial tests.

use std::error::Error;
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};

#[path = "build_support/filesystem.rs"]
mod filesystem;
#[path = "build_support/reader.rs"]
mod reader;

pub(crate) use filesystem::{
    BoundSourceFile, collect_rust_files, hash_bound_executable, hash_regular_file,
};
#[cfg(test)]
pub(crate) use filesystem::{
    collect_rust_files_with_test_replacement, collect_rust_files_with_test_root_replacement,
    hash_regular_file_with_test_mutation,
};
#[cfg(all(test, unix))]
pub(crate) use reader::cancel_non_eof_reader_for_test;
use reader::{BoundedReader, bounded_reader, receive_bounded_reader};

const PROCESS_GROUP_POLL_INTERVAL: Duration = Duration::from_millis(2);
const PROCESS_GROUP_TERM_GRACE_MILLIS: u64 = 100;
const PROCESS_GROUP_KILL_GRACE_MILLIS: u64 = 500;
const LEADER_REAP_GRACE_MILLIS: u64 = 500;
const PIPE_READER_GRACE_MILLIS: u64 = 500;
#[cfg(unix)]
const PROCESS_GROUP_TERM_GRACE: Duration = Duration::from_millis(PROCESS_GROUP_TERM_GRACE_MILLIS);
#[cfg(unix)]
const PROCESS_GROUP_KILL_GRACE: Duration = Duration::from_millis(PROCESS_GROUP_KILL_GRACE_MILLIS);
const LEADER_REAP_GRACE: Duration = Duration::from_millis(LEADER_REAP_GRACE_MILLIS);
const PIPE_READER_GRACE: Duration = Duration::from_millis(PIPE_READER_GRACE_MILLIS);
const OWNED_PROCESS_GROUP_SUPERVISION_SUPPORTED: bool =
    cfg!(any(target_os = "linux", target_os = "macos", windows));
/// Additive TERM + KILL + leader reap + concurrent pipe-finish grace after execution enforcement.
const MAXIMUM_CLEANUP_GRACE_MILLIS: u64 = PROCESS_GROUP_TERM_GRACE_MILLIS
    + PROCESS_GROUP_KILL_GRACE_MILLIS
    + LEADER_REAP_GRACE_MILLIS
    + PIPE_READER_GRACE_MILLIS;
const MAXIMUM_CLEANUP_GRACE: Duration = Duration::from_millis(MAXIMUM_CLEANUP_GRACE_MILLIS);
const BACKEND_DISPATCHER_RELATIVE_PATH: &str = "benches/capture_admission/backend.rs";
const STANDARD_BACKEND_RELATIVE_PATH: &str = "benches/capture_admission/backend/standard.rs";
const CANDIDATE_BACKEND_RELATIVE_PATH: &str = "benches/capture_admission/backend/candidate.rs";
const BACKEND_DIGEST_DOMAIN: &[u8] = b"market-squawk:capture-benchmark-backend:v1\0";

/// Closed compile-time identity for the capture benchmark implementation under measurement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BenchmarkBackend {
    Standard,
    Candidate,
}

impl BenchmarkBackend {
    pub(crate) fn parse(value: &str) -> Result<Self, &'static str> {
        match value {
            "standard" => Ok(Self::Standard),
            "candidate" => Ok(Self::Candidate),
            _ => Err("capture benchmark backend is not a closed identity"),
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::Candidate => "candidate",
        }
    }

    const fn selected_source_relative_path(self) -> &'static str {
        match self {
            Self::Standard => STANDARD_BACKEND_RELATIVE_PATH,
            Self::Candidate => CANDIDATE_BACKEND_RELATIVE_PATH,
        }
    }
}

/// Selects exactly one backend for the current compilation.
///
/// Development builds are pinned to the standard reference and reject ambient selection. An
/// authoritative build must explicitly provide one closed identity through its sanitized build
/// environment.
pub(crate) fn select_benchmark_backend(
    authoritative: bool,
    evidence_backend: Option<&str>,
    development_backend: Option<&str>,
) -> Result<BenchmarkBackend, &'static str> {
    match (authoritative, evidence_backend, development_backend) {
        (false, None, None) => Ok(BenchmarkBackend::Standard),
        (false, None, Some(value)) => BenchmarkBackend::parse(value),
        (false, Some(_), _) => Err("development build forbids the evidence backend variable"),
        (true, Some(value), None) => BenchmarkBackend::parse(value),
        (true, None, None) => Err("authoritative build requires an explicit benchmark backend"),
        (true, _, Some(_)) => Err("authoritative build forbids the development backend variable"),
    }
}

/// Immutable digest binding for the dispatcher and the one compile-time-selected backend source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BenchmarkBackendSourceBinding {
    backend: BenchmarkBackend,
    dispatcher_sha256: String,
    selected_source_relative_path: &'static str,
    selected_source_sha256: String,
    backend_sha256: String,
}

impl BenchmarkBackendSourceBinding {
    pub(crate) const fn backend(&self) -> BenchmarkBackend {
        self.backend
    }

    pub(crate) fn dispatcher_sha256(&self) -> &str {
        &self.dispatcher_sha256
    }

    pub(crate) const fn selected_source_relative_path(&self) -> &'static str {
        self.selected_source_relative_path
    }

    pub(crate) fn selected_source_sha256(&self) -> &str {
        &self.selected_source_sha256
    }

    pub(crate) fn backend_sha256(&self) -> &str {
        &self.backend_sha256
    }
}

/// Binds the immutable dispatcher and selected implementation using bounded descriptor hashes.
///
/// Both implementation sources are read even though only one is selected. This rejects an aliased
/// or byte-identical candidate/reference pair before either can become benchmark evidence.
pub(crate) fn bind_benchmark_backend_sources(
    package_root: &Path,
    backend: BenchmarkBackend,
    maximum_source_bytes: u64,
) -> Result<BenchmarkBackendSourceBinding, Box<dyn Error>> {
    let dispatcher_sha256 = hash_regular_file(
        &package_root.join(BACKEND_DISPATCHER_RELATIVE_PATH),
        maximum_source_bytes,
    )?;
    let standard_sha256 = hash_regular_file(
        &package_root.join(STANDARD_BACKEND_RELATIVE_PATH),
        maximum_source_bytes,
    )?;
    let candidate_sha256 = hash_regular_file(
        &package_root.join(CANDIDATE_BACKEND_RELATIVE_PATH),
        maximum_source_bytes,
    )?;
    if standard_sha256 == candidate_sha256 {
        return Err("capture benchmark backend sources are byte-identical".into());
    }
    let selected_source_sha256 = match backend {
        BenchmarkBackend::Standard => standard_sha256,
        BenchmarkBackend::Candidate => candidate_sha256,
    };
    let selected_source_relative_path = backend.selected_source_relative_path();
    let mut digest = Sha256::new();
    digest.update(BACKEND_DIGEST_DOMAIN);
    update_backend_digest_component(
        &mut digest,
        BACKEND_DISPATCHER_RELATIVE_PATH.as_bytes(),
        dispatcher_sha256.as_bytes(),
    )?;
    update_backend_digest_component(
        &mut digest,
        selected_source_relative_path.as_bytes(),
        selected_source_sha256.as_bytes(),
    )?;
    update_backend_digest_component(&mut digest, b"identity", backend.as_str().as_bytes())?;
    Ok(BenchmarkBackendSourceBinding {
        backend,
        dispatcher_sha256,
        selected_source_relative_path,
        selected_source_sha256,
        backend_sha256: format!("{:x}", digest.finalize()),
    })
}

fn update_backend_digest_component(
    digest: &mut Sha256,
    label: &[u8],
    value: &[u8],
) -> Result<(), Box<dyn Error>> {
    digest.update(u64::try_from(label.len())?.to_le_bytes());
    digest.update(label);
    digest.update(u64::try_from(value.len())?.to_le_bytes());
    digest.update(value);
    Ok(())
}

#[derive(Debug)]
pub(crate) struct BoundedOutput {
    pub(crate) stdout: Vec<u8>,
    pub(crate) stderr: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommandPolicy {
    AuthoritativeInheritOuter { expected_process_group: i32 },
    DevelopmentIsolated,
}

impl CommandPolicy {
    const fn is_authoritative(self) -> bool {
        matches!(self, Self::AuthoritativeInheritOuter { .. })
    }

    const fn owns_process_group(self) -> bool {
        matches!(self, Self::DevelopmentIsolated)
    }
}

pub(crate) const AUTHORITATIVE_PROCESS_GROUP_POLICY: &str = "inherit-outer-v1";

pub(crate) fn authoritative_command_policy(
    configured: Option<&str>,
    expected_process_group: Option<&str>,
) -> Result<CommandPolicy, Box<dyn Error>> {
    if configured != Some(AUTHORITATIVE_PROCESS_GROUP_POLICY) {
        return Err("authoritative build helper requires the exact inherited-group policy".into());
    }
    let expected = expected_process_group
        .ok_or("authoritative build helper requires its outer process-group identity")?
        .parse::<i32>()?;
    if expected <= 0 {
        return Err("authoritative outer process-group identity is invalid".into());
    }
    #[cfg(unix)]
    {
        if rustix::process::getpgrp().as_raw_pid() != expected {
            return Err(
                "authoritative build helper is outside its bound outer process group".into(),
            );
        }
        Ok(CommandPolicy::AuthoritativeInheritOuter {
            expected_process_group: expected,
        })
    }
    #[cfg(not(unix))]
    {
        let _ = expected;
        Err("authoritative inherited process-group policy is unsupported".into())
    }
}

impl CommandPolicy {
    fn validate_current_process_group(self) -> Result<(), Box<dyn Error>> {
        if let Self::AuthoritativeInheritOuter {
            expected_process_group,
        } = self
        {
            #[cfg(unix)]
            if rustix::process::getpgrp().as_raw_pid() != expected_process_group {
                return Err("authoritative process group changed before inner spawn".into());
            }
            #[cfg(not(unix))]
            {
                let _ = expected_process_group;
                return Err("authoritative inherited process-group policy is unsupported".into());
            }
        }
        Ok(())
    }
}

/// Runs a contained child whose post-spawn work is charged against `timeout`.
///
/// `std::process::Command::spawn` is synchronous and cannot be preempted by this process. The
/// deadline is created before that call so time consumed by process creation reduces the remaining
/// budget, but this function does not claim a hard wall-clock bound around `spawn`. Authoritative
/// callers must run the complete build helper beneath a separately supervised process boundary
/// that can terminate the inherited process group. The authoritative policy cannot create a nested
/// group: one outer supervisor remains the sole descendant-containment owner. Once `spawn` returns,
/// setup, polling, direct-child cleanup, and reader cancellation have explicit finite deadlines.
pub(crate) fn run_command_with_post_spawn_deadline(
    program: &Path,
    repository: &Path,
    arguments: &[&str],
    maximum_stdout: usize,
    maximum_stderr: usize,
    timeout: Duration,
    policy: CommandPolicy,
) -> Result<BoundedOutput, Box<dyn Error>> {
    run_command_with_charged_post_spawn_deadline_inner(
        BoundedCommandSpec {
            program,
            repository,
            arguments,
            maximum_stdout,
            maximum_stderr,
            timeout,
            policy,
            test_readiness: None,
        },
        RunFault::None,
    )
}

#[derive(Clone, Copy, Debug)]
struct BoundedCommandSpec<'a> {
    program: &'a Path,
    repository: &'a Path,
    arguments: &'a [&'a str],
    maximum_stdout: usize,
    maximum_stderr: usize,
    timeout: Duration,
    policy: CommandPolicy,
    test_readiness: Option<&'a Path>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RunFault {
    None,
    #[cfg(test)]
    SecondReaderSetup,
    #[cfg(test)]
    Poll,
    #[cfg(test)]
    StdoutRead,
}

impl RunFault {
    fn second_reader_setup(self) -> bool {
        #[cfg(test)]
        if matches!(self, Self::SecondReaderSetup) {
            return true;
        }
        false
    }

    fn poll(self) -> bool {
        #[cfg(test)]
        if matches!(self, Self::Poll | Self::StdoutRead) {
            return true;
        }
        false
    }

    fn stdout_read(self) -> bool {
        #[cfg(test)]
        if matches!(self, Self::StdoutRead) {
            return true;
        }
        false
    }
}

fn run_command_with_charged_post_spawn_deadline_inner(
    spec: BoundedCommandSpec<'_>,
    fault: RunFault,
) -> Result<BoundedOutput, Box<dyn Error>> {
    validate_process_group_support(spec.policy, OWNED_PROCESS_GROUP_SUPERVISION_SUPPORTED)?;
    spec.policy.validate_current_process_group()?;
    // Process creation is charged against this deadline but cannot be preempted in-process. After
    // spawn returns, reader initialization and child polling consume its remainder. Containment
    // cleanup may then consume the additive grace derived from the four constants above.
    let execution_deadline = Instant::now()
        .checked_add(spec.timeout)
        .ok_or("bounded command deadline overflowed")?;
    let mut command = Command::new(spec.program);
    command
        .args(spec.arguments)
        .current_dir(spec.repository)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if spec.policy.is_authoritative() {
        command.env_clear().envs([
            ("GIT_CONFIG_NOSYSTEM", "1"),
            ("GIT_CONFIG_GLOBAL", "/dev/null"),
            ("GIT_OPTIONAL_LOCKS", "0"),
            ("GIT_TERMINAL_PROMPT", "0"),
            ("GIT_NO_REPLACE_OBJECTS", "1"),
            ("LC_ALL", "C"),
            ("LANG", "C"),
            ("PATH", ""),
        ]);
    }
    #[cfg(unix)]
    if spec.policy.owns_process_group() {
        use std::os::unix::process::CommandExt as _;
        command.process_group(0);
    }
    let mut execution = BoundedExecution::new(command.spawn()?, spec.policy)?;
    let setup_result = execution.start_readers(
        spec.maximum_stdout,
        spec.maximum_stderr,
        execution_deadline,
        fault,
        spec.test_readiness,
    );
    let primary_result = setup_result
        .and_then(|()| execution.poll_until_exit(execution_deadline, fault, spec.test_readiness));
    let cleanup = execution.cleanup();
    finish_bounded_execution(
        primary_result,
        cleanup,
        spec.maximum_stdout,
        spec.maximum_stderr,
    )
}

#[cfg(test)]
pub(crate) fn run_command_with_post_spawn_deadline_and_test_fault(
    program: &Path,
    repository: &Path,
    arguments: &[&str],
    timeout: Duration,
    policy: CommandPolicy,
    fault: &str,
    readiness: &Path,
) -> Result<BoundedOutput, Box<dyn Error>> {
    let fault = match fault {
        "second_reader_setup" => RunFault::SecondReaderSetup,
        "poll" => RunFault::Poll,
        "stdout_read" => RunFault::StdoutRead,
        _ => return Err("unknown bounded-command test fault".into()),
    };
    run_command_with_charged_post_spawn_deadline_inner(
        BoundedCommandSpec {
            program,
            repository,
            arguments,
            maximum_stdout: 32,
            maximum_stderr: 32,
            timeout,
            policy,
            test_readiness: Some(readiness),
        },
        fault,
    )
}

#[derive(Debug)]
struct PollOutcome {
    timed_out: bool,
}

#[derive(Debug)]
struct CleanupOutcome {
    group_result: Result<(), String>,
    leader_result: Result<(), String>,
    stdout_result: Result<Vec<u8>, String>,
    stderr_result: Result<Vec<u8>, String>,
    status: Option<ExitStatus>,
}

#[derive(Debug)]
struct BoundedExecution {
    child: Child,
    policy: CommandPolicy,
    #[cfg(unix)]
    process_group_id: Option<rustix::process::Pid>,
    #[cfg(windows)]
    process_job: Option<win32job::Job>,
    stdout_reader: Option<BoundedReader>,
    stderr_reader: Option<BoundedReader>,
    status: Option<ExitStatus>,
    cleaned: bool,
}

impl BoundedExecution {
    fn new(child: Child, policy: CommandPolicy) -> Result<Self, Box<dyn Error>> {
        #[cfg(windows)]
        let mut child = child;
        #[cfg(unix)]
        let process_group_id = policy
            .owns_process_group()
            .then(|| process_group_id_from_raw(child.id()))
            .transpose()
            .ok()
            .flatten();
        #[cfg(windows)]
        let process_job = if policy.owns_process_group() {
            match create_windows_process_job(&child) {
                Ok(job) => Some(job),
                Err(error) => {
                    let _kill_result = child.kill();
                    let _reap_result = wait_for_child_exit(&mut child, LEADER_REAP_GRACE);
                    return Err(format!(
                        "failed to establish Windows bounded-command containment: {error}"
                    )
                    .into());
                }
            }
        } else {
            None
        };
        Ok(Self {
            child,
            policy,
            #[cfg(unix)]
            process_group_id,
            #[cfg(windows)]
            process_job,
            stdout_reader: None,
            stderr_reader: None,
            status: None,
            cleaned: false,
        })
    }

    fn start_readers(
        &mut self,
        maximum_stdout: usize,
        maximum_stderr: usize,
        deadline: Instant,
        fault: RunFault,
        test_readiness: Option<&Path>,
    ) -> Result<(), Box<dyn Error>> {
        ensure_before_execution_deadline(deadline, "stdout reader initialization")?;
        let stdout = self
            .child
            .stdout
            .take()
            .ok_or("bounded command stdout is absent")?;
        self.stdout_reader = Some(bounded_reader(stdout, maximum_stdout, fault.stdout_read())?);
        if fault.second_reader_setup() {
            wait_for_test_readiness(test_readiness, deadline)?;
            consume_test_deadline(deadline);
        }
        ensure_before_execution_deadline(deadline, "stderr reader initialization")?;
        if fault.second_reader_setup() {
            return Err("injected second-reader setup failure".into());
        }
        let stderr = self
            .child
            .stderr
            .take()
            .ok_or("bounded command stderr is absent")?;
        self.stderr_reader = Some(bounded_reader(stderr, maximum_stderr, false)?);
        ensure_before_execution_deadline(deadline, "reader initialization completion")?;
        Ok(())
    }

    fn poll_until_exit(
        &mut self,
        deadline: Instant,
        fault: RunFault,
        test_readiness: Option<&Path>,
    ) -> Result<PollOutcome, Box<dyn Error>> {
        if fault.poll() {
            wait_for_test_readiness(test_readiness, deadline)?;
            return Err("injected child-poll failure".into());
        }
        loop {
            if self.observe_leader_exit()? {
                return Ok(PollOutcome { timed_out: false });
            }
            if Instant::now() >= deadline {
                return Ok(PollOutcome { timed_out: true });
            }
            std::thread::sleep(PROCESS_GROUP_POLL_INTERVAL);
        }
    }

    fn observe_leader_exit(&mut self) -> Result<bool, Box<dyn Error>> {
        if self.policy.owns_process_group() {
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            {
                let process_id = self
                    .process_group_id
                    .ok_or("bounded child process-group ID is invalid")?;
                // Keep an exited leader waitable so its PID continues to reserve the owned process-
                // group identity while cleanup signals and checks the remaining group members.
                let options = rustix::process::WaitIdOptions::EXITED
                    | rustix::process::WaitIdOptions::NOHANG
                    | rustix::process::WaitIdOptions::NOWAIT;
                return rustix::process::waitid(rustix::process::WaitId::Pid(process_id), options)
                    .map(|status| status.is_some())
                    .map_err(Into::into);
            }
            #[cfg(windows)]
            {
                if let Some(status) = self.child.try_wait()? {
                    self.status = Some(status);
                    return Ok(true);
                }
                return Ok(false);
            }
            #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
            return Err("bounded command requires supported process-group control".into());
        }

        if let Some(status) = self.child.try_wait()? {
            self.status = Some(status);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn cleanup(&mut self) -> CleanupOutcome {
        self.cleaned = true;
        let cleanup_deadline = Instant::now()
            .checked_add(MAXIMUM_CLEANUP_GRACE)
            .unwrap_or_else(Instant::now);
        #[cfg(unix)]
        let group_result = if self.policy.owns_process_group() {
            match self.process_group_id {
                Some(process_id) => {
                    let controller = SystemProcessGroupControl;
                    terminate_process_group(&controller, process_id, || {
                        if self.status.is_none() {
                            self.status = self.child.try_wait()?;
                        }
                        Ok(())
                    })
                    .map_err(|error| error.to_string())
                }
                None => Err("bounded child process-group ID is invalid".to_owned()),
            }
        } else {
            // The authoritative child inherits the Cargo/build supervisor's process group. Killing
            // that group here would kill this build script and its parent Cargo before evidence can
            // report the failure. Kill/reap only the direct child below; the outer Python supervisor
            // owns final whole-group extinction, including grandchildren that retained pipe FDs.
            Ok(())
        };
        #[cfg(windows)]
        let group_result = if self.policy.owns_process_group() {
            self.process_job.take().map_or_else(
                || Err("bounded child Windows job object is absent".to_owned()),
                |job| {
                    // JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE makes the kernel terminate the entire
                    // contained process tree when this final handle is closed.
                    drop(job);
                    Ok(())
                },
            )
        } else {
            Ok(())
        };
        #[cfg(not(any(unix, windows)))]
        let group_result = Err("bounded execution has no supported process containment".to_owned());
        if self.status.is_none() {
            let _kill_result = self.child.kill();
        }
        let leader_result = if self.status.is_none() {
            let leader_grace = cleanup_deadline
                .saturating_duration_since(Instant::now())
                .min(LEADER_REAP_GRACE);
            match wait_for_child_exit(&mut self.child, leader_grace) {
                Ok(status) => {
                    self.status = Some(status);
                    Ok(())
                }
                Err(error) => Err(error.to_string()),
            }
        } else {
            Ok(())
        };
        let reader_grace = cleanup_deadline
            .saturating_duration_since(Instant::now())
            .min(PIPE_READER_GRACE);
        let reader_deadline = Instant::now()
            .checked_add(reader_grace)
            .unwrap_or_else(Instant::now);
        let stdout_result = finish_optional_reader(self.stdout_reader.take(), reader_deadline);
        let stderr_result = finish_optional_reader(self.stderr_reader.take(), reader_deadline);
        CleanupOutcome {
            group_result,
            leader_result,
            stdout_result,
            stderr_result,
            status: self.status,
        }
    }
}

impl Drop for BoundedExecution {
    fn drop(&mut self) {
        if !self.cleaned {
            let _cleanup = self.cleanup();
        }
    }
}

#[cfg(windows)]
fn create_windows_process_job(child: &Child) -> Result<win32job::Job, Box<dyn Error>> {
    use std::os::windows::io::AsRawHandle as _;

    let mut limits = win32job::ExtendedLimitInfo::new();
    limits.limit_kill_on_job_close();
    let job = win32job::Job::create_with_limit_info(&limits)?;
    job.assign_process(child.as_raw_handle() as isize)?;
    Ok(job)
}

fn ensure_before_execution_deadline(deadline: Instant, phase: &str) -> Result<(), Box<dyn Error>> {
    if Instant::now() >= deadline {
        return Err(format!("bounded command deadline elapsed during {phase}").into());
    }
    Ok(())
}

fn wait_for_test_readiness(
    readiness: Option<&Path>,
    deadline: Instant,
) -> Result<(), Box<dyn Error>> {
    #[cfg(test)]
    {
        let path = readiness.ok_or("injected command failure is missing its readiness path")?;
        // Creation and population are separate filesystem operations. Seeing the pathname alone
        // can race the writer between `open(O_CREAT)` and its first write, which would inject the
        // fault before the fixture has published the descendant identity needed to prove cleanup.
        while !path
            .metadata()
            .is_ok_and(|metadata| metadata.is_file() && metadata.len() > 0)
        {
            if Instant::now() >= deadline {
                return Err(
                    "test command did not become ready before its execution deadline".into(),
                );
            }
            std::thread::sleep(PROCESS_GROUP_POLL_INTERVAL);
        }
    }
    #[cfg(not(test))]
    {
        let _unused = (readiness, deadline);
    }
    Ok(())
}

fn consume_test_deadline(deadline: Instant) {
    #[cfg(test)]
    while Instant::now() < deadline {
        std::thread::sleep(PROCESS_GROUP_POLL_INTERVAL);
    }
    #[cfg(not(test))]
    let _unused = deadline;
}

#[cfg(test)]
pub(crate) fn maximum_enforced_post_spawn_duration_for_test(timeout: Duration) -> Option<Duration> {
    timeout.checked_add(MAXIMUM_CLEANUP_GRACE)
}

#[cfg(test)]
pub(crate) const fn cleanup_grace_milliseconds_for_test() -> (u64, u64, u64, u64, u64) {
    (
        PROCESS_GROUP_TERM_GRACE_MILLIS,
        PROCESS_GROUP_KILL_GRACE_MILLIS,
        LEADER_REAP_GRACE_MILLIS,
        PIPE_READER_GRACE_MILLIS,
        MAXIMUM_CLEANUP_GRACE_MILLIS,
    )
}

#[cfg(all(test, unix))]
pub(crate) fn current_process_group_id_for_test() -> i32 {
    rustix::process::getpgrp().as_raw_pid()
}

fn finish_optional_reader(
    reader: Option<BoundedReader>,
    deadline: Instant,
) -> Result<Vec<u8>, String> {
    reader.map_or_else(
        || Err("bounded pipe reader was not initialized".to_owned()),
        |reader| receive_bounded_reader(reader, deadline).map_err(|error| error.to_string()),
    )
}

fn finish_bounded_execution(
    primary_result: Result<PollOutcome, Box<dyn Error>>,
    cleanup: CleanupOutcome,
    maximum_stdout: usize,
    maximum_stderr: usize,
) -> Result<BoundedOutput, Box<dyn Error>> {
    let mut failures = Vec::new();
    let poll = match primary_result {
        Ok(poll) => Some(poll),
        Err(error) => {
            failures.push(format!("command execution failed: {error}"));
            None
        }
    };
    if let Err(error) = cleanup.group_result {
        failures.push(format!("process-group cleanup failed: {error}"));
    }
    if let Err(error) = cleanup.leader_result {
        failures.push(format!("leader cleanup failed: {error}"));
    }
    let stdout = match cleanup.stdout_result {
        Ok(stdout) => Some(stdout),
        Err(error) => {
            failures.push(format!("stdout cleanup failed: {error}"));
            None
        }
    };
    let stderr = match cleanup.stderr_result {
        Ok(stderr) => Some(stderr),
        Err(error) => {
            failures.push(format!("stderr cleanup failed: {error}"));
            None
        }
    };
    if poll.is_some_and(|poll| poll.timed_out) {
        failures.push("bounded command exceeded its execution deadline".to_owned());
    }
    if !cleanup.status.is_some_and(|status| status.success()) {
        failures.push("bounded command did not exit successfully".to_owned());
    }
    if stdout
        .as_ref()
        .is_some_and(|output| output.len() > maximum_stdout)
    {
        failures.push("bounded command exceeded its stdout limit".to_owned());
    }
    if stderr
        .as_ref()
        .is_some_and(|output| output.len() > maximum_stderr)
    {
        failures.push("bounded command exceeded its stderr limit".to_owned());
    }
    if !failures.is_empty() {
        return Err(failures.join("; ").into());
    }
    Ok(BoundedOutput {
        stdout: stdout.ok_or("bounded stdout disappeared after successful cleanup")?,
        stderr: stderr.ok_or("bounded stderr disappeared after successful cleanup")?,
    })
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProcessGroupSignal {
    Terminate,
    Kill,
}

#[cfg(unix)]
pub(crate) trait ProcessGroupControl {
    fn signal(
        &self,
        process_id: rustix::process::Pid,
        signal: ProcessGroupSignal,
    ) -> Result<(), String>;

    fn exists(&self, process_id: rustix::process::Pid) -> Result<bool, String>;
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug)]
struct SystemProcessGroupControl;

#[cfg(unix)]
impl ProcessGroupControl for SystemProcessGroupControl {
    fn signal(
        &self,
        process_id: rustix::process::Pid,
        signal: ProcessGroupSignal,
    ) -> Result<(), String> {
        let signal = match signal {
            ProcessGroupSignal::Terminate => rustix::process::Signal::TERM,
            ProcessGroupSignal::Kill => rustix::process::Signal::KILL,
        };
        match rustix::process::kill_process_group(process_id, signal) {
            // An exited, deliberately unreaped leader makes macOS return EPERM even though the
            // signal is still delivered to signalable group members. This is not success by
            // itself: cleanup requires a terminal extinction probe after reaping the leader.
            Ok(()) | Err(rustix::io::Errno::PERM) => Ok(()),
            Err(error) => Err(error.to_string()),
        }
    }

    fn exists(&self, process_id: rustix::process::Pid) -> Result<bool, String> {
        match rustix::process::test_kill_process_group(process_id) {
            Ok(()) => Ok(true),
            Err(rustix::io::Errno::SRCH) => Ok(false),
            // macOS reports EPERM while an exited group leader remains waitable. It therefore
            // proves presence, not extinction; the terminal post-reap probe below must still
            // establish ESRCH before cleanup succeeds.
            Err(rustix::io::Errno::PERM) => Ok(true),
            Err(error) => Err(error.to_string()),
        }
    }
}

#[cfg(unix)]
fn process_group_id_from_raw(process_id: u32) -> Result<rustix::process::Pid, Box<dyn Error>> {
    let raw = i32::try_from(process_id)?;
    rustix::process::Pid::from_raw(raw).ok_or_else(|| "child process ID is zero".into())
}

#[cfg(unix)]
pub(crate) fn terminate_process_group<C, F>(
    controller: &C,
    process_id: rustix::process::Pid,
    mut reap_leader_after_group: F,
) -> Result<(), Box<dyn Error>>
where
    C: ProcessGroupControl,
    F: FnMut() -> Result<(), Box<dyn Error>>,
{
    let mut failures = Vec::new();
    let mut extinct = match controller.exists(process_id) {
        Ok(false) => true,
        Ok(true) => false,
        Err(error) => {
            failures.push(format!("initial process-group probe failed: {error}"));
            false
        }
    };
    if !extinct {
        if let Err(error) = controller.signal(process_id, ProcessGroupSignal::Terminate) {
            failures.push(format!("process-group TERM failed: {error}"));
        }
        extinct = wait_for_group_extinction(
            controller,
            process_id,
            PROCESS_GROUP_TERM_GRACE,
            &mut failures,
        );
    }
    if !extinct {
        if let Err(error) = controller.signal(process_id, ProcessGroupSignal::Kill) {
            failures.push(format!("process-group KILL failed: {error}"));
        }
        extinct = wait_for_group_extinction(
            controller,
            process_id,
            PROCESS_GROUP_KILL_GRACE,
            &mut failures,
        );
    }

    if let Err(error) = reap_leader_after_group() {
        failures.push(format!(
            "leader reap failed after process-group termination: {error}"
        ));
    }

    if !extinct {
        extinct = match controller.exists(process_id) {
            Ok(false) => true,
            Ok(true) => false,
            Err(error) => {
                failures.push(format!("terminal process-group probe failed: {error}"));
                false
            }
        };
    }
    if !extinct {
        failures.push("bounded process group survived TERM and KILL deadlines".to_owned());
    }
    group_cleanup_result(failures)
}

#[cfg(unix)]
fn wait_for_group_extinction<C>(
    controller: &C,
    process_id: rustix::process::Pid,
    grace: Duration,
    failures: &mut Vec<String>,
) -> bool
where
    C: ProcessGroupControl,
{
    let deadline = Instant::now().checked_add(grace).unwrap_or_else(|| {
        failures.push("process-group extinction deadline overflowed".to_owned());
        Instant::now()
    });
    loop {
        match controller.exists(process_id) {
            Ok(false) => return true,
            Ok(true) => {}
            Err(error) => failures.push(format!("process-group probe failed: {error}")),
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(PROCESS_GROUP_POLL_INTERVAL);
    }
}

#[cfg(unix)]
fn group_cleanup_result(failures: Vec<String>) -> Result<(), Box<dyn Error>> {
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; ").into())
    }
}

pub(crate) fn validate_process_group_support(
    policy: CommandPolicy,
    platform_supported: bool,
) -> Result<(), Box<dyn Error>> {
    if policy.owns_process_group() && !platform_supported {
        return Err("bounded command requires supported process-group control".into());
    }
    Ok(())
}

fn wait_for_child_exit(child: &mut Child, grace: Duration) -> Result<ExitStatus, Box<dyn Error>> {
    let deadline = Instant::now()
        .checked_add(grace)
        .ok_or("child exit deadline overflowed")?;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            return Err("bounded command leader survived its extinction deadline".into());
        }
        std::thread::sleep(PROCESS_GROUP_POLL_INTERVAL);
    }
}
