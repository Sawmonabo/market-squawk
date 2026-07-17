//! Bounded, descriptor-checked helpers shared by the build script and its adversarial tests.

use std::error::Error;
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

#[path = "build_support/filesystem.rs"]
mod filesystem;
#[path = "build_support/reader.rs"]
mod reader;

pub(crate) use filesystem::{BoundSourceFile, collect_rust_files, hash_regular_file};
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
const PROCESS_GROUP_TERM_GRACE: Duration = Duration::from_millis(PROCESS_GROUP_TERM_GRACE_MILLIS);
const PROCESS_GROUP_KILL_GRACE: Duration = Duration::from_millis(PROCESS_GROUP_KILL_GRACE_MILLIS);
const LEADER_REAP_GRACE: Duration = Duration::from_millis(LEADER_REAP_GRACE_MILLIS);
const PIPE_READER_GRACE: Duration = Duration::from_millis(PIPE_READER_GRACE_MILLIS);
/// Additive TERM + KILL + leader reap + concurrent pipe-finish grace after execution enforcement.
const MAXIMUM_CLEANUP_GRACE_MILLIS: u64 = PROCESS_GROUP_TERM_GRACE_MILLIS
    + PROCESS_GROUP_KILL_GRACE_MILLIS
    + LEADER_REAP_GRACE_MILLIS
    + PIPE_READER_GRACE_MILLIS;
const MAXIMUM_CLEANUP_GRACE: Duration = Duration::from_millis(MAXIMUM_CLEANUP_GRACE_MILLIS);

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
    if rustix::process::getpgrp().as_raw_pid() != expected {
        return Err("authoritative build helper is outside its bound outer process group".into());
    }
    #[cfg(not(unix))]
    return Err("authoritative inherited process-group policy is unsupported".into());
    Ok(CommandPolicy::AuthoritativeInheritOuter {
        expected_process_group: expected,
    })
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
            return Err("authoritative inherited process-group policy is unsupported".into());
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
    validate_process_group_support(spec.policy, cfg!(unix))?;
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
    let mut execution = BoundedExecution::new(command.spawn()?, spec.policy);
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
    stdout_reader: Option<BoundedReader>,
    stderr_reader: Option<BoundedReader>,
    status: Option<ExitStatus>,
    cleaned: bool,
}

impl BoundedExecution {
    fn new(child: Child, policy: CommandPolicy) -> Self {
        #[cfg(unix)]
        let process_group_id = policy
            .owns_process_group()
            .then(|| process_group_id_from_raw(child.id()))
            .transpose()
            .ok()
            .flatten();
        Self {
            child,
            policy,
            #[cfg(unix)]
            process_group_id,
            stdout_reader: None,
            stderr_reader: None,
            status: None,
            cleaned: false,
        }
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
            if let Some(status) = self.child.try_wait()? {
                self.status = Some(status);
                return Ok(PollOutcome { timed_out: false });
            }
            if Instant::now() >= deadline {
                return Ok(PollOutcome { timed_out: true });
            }
            std::thread::sleep(PROCESS_GROUP_POLL_INTERVAL);
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
        #[cfg(not(unix))]
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
        while !path.is_file() {
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
        rustix::process::kill_process_group(process_id, signal).map_err(|error| error.to_string())
    }

    fn exists(&self, process_id: rustix::process::Pid) -> Result<bool, String> {
        match rustix::process::test_kill_process_group(process_id) {
            Ok(()) => Ok(true),
            Err(rustix::io::Errno::SRCH) => Ok(false),
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
    mut reap_leader: F,
) -> Result<(), Box<dyn Error>>
where
    C: ProcessGroupControl,
    F: FnMut() -> Result<(), Box<dyn Error>>,
{
    let mut failures = Vec::new();
    if let Err(error) = reap_leader() {
        failures.push(format!(
            "leader reap failed before group termination: {error}"
        ));
    }
    match controller.exists(process_id) {
        Ok(false) => return group_cleanup_result(failures),
        Ok(true) => {}
        Err(error) => failures.push(format!("initial process-group probe failed: {error}")),
    }
    if let Err(error) = controller.signal(process_id, ProcessGroupSignal::Terminate) {
        failures.push(format!("process-group TERM failed: {error}"));
    }
    if wait_for_group_extinction(
        controller,
        process_id,
        PROCESS_GROUP_TERM_GRACE,
        &mut reap_leader,
        &mut failures,
    ) {
        return group_cleanup_result(failures);
    }
    if let Err(error) = controller.signal(process_id, ProcessGroupSignal::Kill) {
        failures.push(format!("process-group KILL failed: {error}"));
    }
    if wait_for_group_extinction(
        controller,
        process_id,
        PROCESS_GROUP_KILL_GRACE,
        &mut reap_leader,
        &mut failures,
    ) {
        return group_cleanup_result(failures);
    }
    failures.push("bounded process group survived TERM and KILL deadlines".to_owned());
    group_cleanup_result(failures)
}

#[cfg(unix)]
fn wait_for_group_extinction<C, F>(
    controller: &C,
    process_id: rustix::process::Pid,
    grace: Duration,
    reap_leader: &mut F,
    failures: &mut Vec<String>,
) -> bool
where
    C: ProcessGroupControl,
    F: FnMut() -> Result<(), Box<dyn Error>>,
{
    let deadline = Instant::now().checked_add(grace).unwrap_or_else(|| {
        failures.push("process-group extinction deadline overflowed".to_owned());
        Instant::now()
    });
    loop {
        if let Err(error) = reap_leader() {
            failures.push(format!(
                "leader reap failed during group termination: {error}"
            ));
        }
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
