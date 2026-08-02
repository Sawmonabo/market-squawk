//! Bounded child-process supervision for evidence producers.

use std::ffi::{OsStr, OsString};
use std::io::Read;
use std::path::Path;
use std::process::{Command, ExitStatus, Stdio};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::io::hex_digest;

const OUTPUT_RETAINED_BYTES: usize = 64 * 1024;
const POLL_INTERVAL: Duration = Duration::from_millis(250);
const TERMINATION_GRACE: Duration = Duration::from_secs(2);
pub(super) const REMOVED_BUILD_ENVIRONMENT: [&str; 9] = [
    "CARGO_BUILD_RUSTC_WRAPPER",
    "CARGO_ENCODED_RUSTFLAGS",
    "CARGO_TARGET_DIR",
    "RUSTC",
    "RUSTC_WORKSPACE_WRAPPER",
    "RUSTC_WRAPPER",
    "RUSTDOC",
    "RUSTDOCFLAGS",
    "RUSTFLAGS",
];

pub(super) struct ProcessRequest<'a> {
    pub(super) program: &'a OsStr,
    pub(super) arguments: &'a [OsString],
    pub(super) current_dir: &'a Path,
    pub(super) environment: &'a [(OsString, OsString)],
    pub(super) limits: ProcessLimits,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct ProcessLimits {
    pub(super) timeout: Duration,
    pub(super) rss_bytes: u64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ProcessEvidence {
    pub(super) elapsed_millis: u64,
    pub(super) process_tree_rss_observation: ProcessTreeRssObservation,
    pub(super) exit_code: i32,
    pub(super) stdout_sha256: String,
    pub(super) stdout_bytes: u64,
    pub(super) stdout_truncated: bool,
    pub(super) stderr_sha256: String,
    pub(super) stderr_bytes: u64,
    pub(super) stderr_truncated: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ProcessTreeRssObservation {
    pub(super) observed_maximum_rss_bytes: Option<u64>,
    pub(super) successful_sample_count: u64,
    pub(super) observation_window_millis: u64,
    pub(super) configured_poll_sleep_millis: u64,
}

impl ProcessTreeRssObservation {
    #[cfg(feature = "release-evidence")]
    pub(super) fn admitted_observed_maximum_rss_bytes(&self) -> Result<u64> {
        if self.successful_sample_count == 0
            || self.configured_poll_sleep_millis != process_tree_rss_poll_sleep_millis()
        {
            bail!("process-tree RSS observation does not satisfy its sampling contract");
        }
        self.observed_maximum_rss_bytes
            .context("process-tree RSS observation contains no successful sample")
    }
}

pub(super) fn run(request: ProcessRequest<'_>) -> Result<ProcessOutput> {
    if request.limits.timeout.is_zero() || request.limits.rss_bytes == 0 {
        bail!("bounded process limits must be nonzero");
    }
    let deadline = Instant::now()
        .checked_add(request.limits.timeout)
        .ok_or_else(|| anyhow::anyhow!("bounded process deadline overflow"))?;
    let mut command = Command::new(request.program);
    command
        .args(request.arguments)
        .current_dir(request.current_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for variable in REMOVED_BUILD_ENVIRONMENT {
        command.env_remove(variable);
    }
    command
        .envs(request.environment.iter().map(|(key, value)| (key, value)))
        .env("CARGO_INCREMENTAL", "0");
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt as _;
        command.process_group(0);
    }
    let started = Instant::now();
    let mut child = command.spawn().context("failed to start bounded process")?;
    let process_id = child.id();
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("bounded process stdout is unavailable"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow::anyhow!("bounded process stderr is unavailable"))?;
    let stdout_reader = spawn_reader(stdout);
    let stderr_reader = spawn_reader(stderr);
    let mut observed_maximum_rss = None;
    let mut successful_rss_samples = 0_u64;
    let mut first_rss_sample_started_at = None;
    let mut last_rss_sample_completed_at = None;
    let mut primary_error: Option<anyhow::Error> = None;
    let status = loop {
        if let Some(status) = child
            .try_wait()
            .context("bounded process status could not be observed")?
        {
            break Some(status);
        }
        let sample_started_at = Instant::now();
        first_rss_sample_started_at.get_or_insert(sample_started_at);
        match process_group_rss_bytes(process_id) {
            Ok(rss) => {
                observed_maximum_rss =
                    Some(observed_maximum_rss.map_or(rss, |current: u64| current.max(rss)));
                let Some(sample_count) = successful_rss_samples.checked_add(1) else {
                    primary_error = Some(anyhow::anyhow!("process-tree RSS sample count overflow"));
                    break None;
                };
                successful_rss_samples = sample_count;
                last_rss_sample_completed_at = Some(Instant::now());
                if rss > request.limits.rss_bytes {
                    primary_error = Some(anyhow::anyhow!(
                        "bounded process resident memory {rss} bytes exceeded its {}-byte limit",
                        request.limits.rss_bytes
                    ));
                    break None;
                }
            }
            Err(error) => {
                primary_error = Some(error);
                break None;
            }
        }
        if Instant::now() >= deadline {
            primary_error = Some(anyhow::anyhow!("bounded process exceeded its deadline"));
            break None;
        }
        std::thread::sleep(POLL_INTERVAL);
    };
    terminate_process_group(process_id, &mut child)?;
    let final_status = match status {
        Some(status) => status,
        None => wait_for_child(&mut child, TERMINATION_GRACE)?,
    };
    let stdout = finish_reader(stdout_reader)?;
    let stderr = finish_reader(stderr_reader)?;
    if let Some(error) = primary_error {
        return Err(error);
    }
    if !final_status.success() {
        bail!(
            "bounded process failed with status {}: {}",
            status_label(final_status),
            stderr.retained_text()
        );
    }
    let elapsed_millis = u64::try_from(started.elapsed().as_millis())
        .context("bounded process elapsed time overflow")?;
    let observation_window_millis =
        match (first_rss_sample_started_at, last_rss_sample_completed_at) {
            (Some(first), Some(last)) => u64::try_from(last.duration_since(first).as_millis())
                .context("process-tree RSS observation window overflow")?,
            _ => 0,
        };
    Ok(ProcessOutput {
        evidence: ProcessEvidence {
            elapsed_millis,
            process_tree_rss_observation: ProcessTreeRssObservation {
                observed_maximum_rss_bytes: observed_maximum_rss,
                successful_sample_count: successful_rss_samples,
                observation_window_millis,
                configured_poll_sleep_millis: process_tree_rss_poll_sleep_millis(),
            },
            exit_code: final_status.code().unwrap_or_default(),
            stdout_sha256: stdout.sha256,
            stdout_bytes: stdout.byte_count,
            stdout_truncated: stdout.truncated,
            stderr_sha256: stderr.sha256,
            stderr_bytes: stderr.byte_count,
            stderr_truncated: stderr.truncated,
        },
        stdout: stdout.retained,
    })
}

fn spawn_reader<R: Read + Send + 'static>(reader: R) -> JoinHandle<Result<CapturedOutput>> {
    std::thread::spawn(move || capture_output(reader))
}

fn capture_output(mut reader: impl Read) -> Result<CapturedOutput> {
    let mut hasher = Sha256::new();
    let mut retained = Vec::new();
    retained
        .try_reserve_exact(OUTPUT_RETAINED_BYTES)
        .context("bounded output allocation failed")?;
    let mut buffer = [0_u8; 16 * 1024];
    let mut byte_count = 0_u64;
    loop {
        let read = reader
            .read(&mut buffer)
            .context("bounded process output read failed")?;
        if read == 0 {
            break;
        }
        byte_count = byte_count
            .checked_add(u64::try_from(read).context("bounded process output size overflow")?)
            .ok_or_else(|| anyhow::anyhow!("bounded process output size overflow"))?;
        hasher.update(&buffer[..read]);
        let remaining = OUTPUT_RETAINED_BYTES.saturating_sub(retained.len());
        retained.extend_from_slice(&buffer[..read.min(remaining)]);
    }
    Ok(CapturedOutput {
        sha256: hex_digest(hasher.finalize().into()),
        byte_count,
        truncated: byte_count
            > u64::try_from(OUTPUT_RETAINED_BYTES)
                .context("bounded output limit conversion failed")?,
        retained,
    })
}

fn finish_reader(reader: JoinHandle<Result<CapturedOutput>>) -> Result<CapturedOutput> {
    let deadline = Instant::now()
        .checked_add(TERMINATION_GRACE)
        .ok_or_else(|| anyhow::anyhow!("bounded reader deadline overflow"))?;
    while !reader.is_finished() {
        if Instant::now() >= deadline {
            bail!("bounded process output reader did not terminate");
        }
        std::thread::sleep(POLL_INTERVAL);
    }
    reader
        .join()
        .map_err(|_| anyhow::anyhow!("bounded process output reader panicked"))?
}

fn wait_for_child(child: &mut std::process::Child, grace: Duration) -> Result<ExitStatus> {
    let deadline = Instant::now()
        .checked_add(grace)
        .ok_or_else(|| anyhow::anyhow!("bounded child cleanup deadline overflow"))?;
    loop {
        if let Some(status) = child
            .try_wait()
            .context("bounded child cleanup status failed")?
        {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            bail!("bounded child survived its cleanup deadline");
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

#[cfg(unix)]
fn terminate_process_group(process_id: u32, child: &mut std::process::Child) -> Result<()> {
    let raw = i32::try_from(process_id).context("bounded process identifier overflow")?;
    let group = rustix::process::Pid::from_raw(raw)
        .ok_or_else(|| anyhow::anyhow!("bounded process identifier is zero"))?;
    signal_group(group, rustix::process::Signal::TERM)?;
    let deadline = Instant::now()
        .checked_add(TERMINATION_GRACE)
        .ok_or_else(|| anyhow::anyhow!("process-group cleanup deadline overflow"))?;
    while process_group_exists(group)? && Instant::now() < deadline {
        std::thread::sleep(POLL_INTERVAL);
    }
    if process_group_exists(group)? {
        signal_group(group, rustix::process::Signal::KILL)?;
    }
    if child.try_wait()?.is_none() {
        child.kill().context("bounded process leader kill failed")?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn terminate_process_group(_process_id: u32, child: &mut std::process::Child) -> Result<()> {
    child
        .kill()
        .context("bounded process containment is unavailable")?;
    Ok(())
}

#[cfg(unix)]
fn signal_group(group: rustix::process::Pid, signal: rustix::process::Signal) -> Result<()> {
    match rustix::process::kill_process_group(group, signal) {
        Ok(()) | Err(rustix::io::Errno::SRCH) | Err(rustix::io::Errno::PERM) => Ok(()),
        Err(error) => Err(error).context("process-group signal failed"),
    }
}

#[cfg(unix)]
fn process_group_exists(group: rustix::process::Pid) -> Result<bool> {
    match rustix::process::test_kill_process_group(group) {
        Ok(()) | Err(rustix::io::Errno::PERM) => Ok(true),
        Err(rustix::io::Errno::SRCH) => Ok(false),
        Err(error) => Err(error).context("process-group probe failed"),
    }
}

#[cfg(unix)]
fn process_group_rss_bytes(process_id: u32) -> Result<u64> {
    let output = Command::new("ps")
        .args(["-axo", "pgid=,rss="])
        .output()
        .context("process-tree RSS sampler failed")?;
    if !output.status.success() || output.stdout.len() > 16 * 1024 * 1024 {
        bail!("process-tree RSS sampler returned invalid output");
    }
    let process_group = u64::from(process_id);
    let kibibytes = std::str::from_utf8(&output.stdout)
        .context("process-tree RSS sampler returned non-UTF-8 output")?
        .lines()
        .try_fold(0_u64, |total, line| {
            let mut fields = line.split_ascii_whitespace();
            let group = fields
                .next()
                .ok_or_else(|| anyhow::anyhow!("process-tree RSS row omitted a group"))?
                .parse::<u64>()
                .context("process-tree RSS group is invalid")?;
            let rss = fields
                .next()
                .ok_or_else(|| anyhow::anyhow!("process-tree RSS row omitted memory"))?
                .parse::<u64>()
                .context("process-tree RSS value is invalid")?;
            if fields.next().is_some() {
                bail!("process-tree RSS row contains extra fields");
            }
            if group == process_group {
                total
                    .checked_add(rss)
                    .ok_or_else(|| anyhow::anyhow!("process-tree RSS total overflow"))
            } else {
                Ok(total)
            }
        })?;
    kibibytes
        .checked_mul(1024)
        .ok_or_else(|| anyhow::anyhow!("process-tree RSS byte total overflow"))
}

#[cfg(not(unix))]
fn process_group_rss_bytes(_process_id: u32) -> Result<u64> {
    bail!("release process-tree RSS measurement is unsupported on this platform")
}

fn status_label(status: ExitStatus) -> String {
    status
        .code()
        .map_or_else(|| "signal".to_owned(), |code| code.to_string())
}

struct CapturedOutput {
    sha256: String,
    byte_count: u64,
    truncated: bool,
    retained: Vec<u8>,
}

pub(super) struct ProcessOutput {
    pub(super) evidence: ProcessEvidence,
    pub(super) stdout: Vec<u8>,
}

pub(super) fn process_tree_rss_poll_sleep_millis() -> u64 {
    u64::try_from(POLL_INTERVAL.as_millis()).unwrap_or(u64::MAX)
}

impl CapturedOutput {
    fn retained_text(&self) -> String {
        String::from_utf8_lossy(&self.retained).into_owned()
    }
}
