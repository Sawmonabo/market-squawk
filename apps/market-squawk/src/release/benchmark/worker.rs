//! Exact argv admission and bounded child-process supervision for performance evidence.

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::{EvidenceAuthority, WorkerMeasurements};
use crate::AppConfig;
use crate::cli::ReleaseBenchmarkArguments;
use crate::release::identity::RepositoryIdentity;
use crate::release::io::{StableFileIdentity, hex_digest};
use crate::release::process::{self, ProcessEvidence, ProcessLimits, ProcessRequest};

const WORKER_SCHEMA_VERSION: u32 = 1;
const WORKER_KIND: &str = "market_squawk.release.performance.worker";
const WORKER_COMMAND: &str = "benchmark-worker";
const SUPERVISOR_TIMEOUT: Duration = Duration::from_secs(4 * 60 * 60);
const SUPERVISOR_RSS_BYTES: u64 = 16 * 1024 * 1024 * 1024;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WorkerEnvelope {
    schema_version: u32,
    kind: String,
    binding: WorkerBinding,
    measurements: WorkerMeasurements,
}

impl WorkerEnvelope {
    pub(super) fn new(binding: WorkerBinding, measurements: WorkerMeasurements) -> Self {
        Self {
            schema_version: WORKER_SCHEMA_VERSION,
            kind: WORKER_KIND.to_owned(),
            binding,
            measurements,
        }
    }

    fn validate(self, expected: &WorkerBinding) -> Result<(WorkerBinding, WorkerMeasurements)> {
        if self.schema_version != WORKER_SCHEMA_VERSION
            || self.kind != WORKER_KIND
            || &self.binding != expected
        {
            bail!("release benchmark worker binding did not match the admitted request");
        }
        Ok((self.binding, self.measurements))
    }
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WorkerBinding {
    authority: EvidenceAuthority,
    repository_head: String,
    repository_tree: String,
    repository_clean: bool,
    expected_head: Option<String>,
    expected_tree: Option<String>,
    warm_up_events: u64,
    measured_events: u64,
    storage_rows: u64,
    maximum_tail_growth_mib: u64,
    maximum_tail_growth_percent: u64,
    minimum_events_per_second: u64,
    maximum_warmed_p99_nanos: u64,
    requested_output: PathBuf,
    effective_config_sha256: String,
    effective_environment_sha256: String,
    executable_sha256: String,
    executable_bytes: u64,
    argv_sha256: String,
    argv_count: u64,
    supervisor_timeout_millis: u64,
    supervisor_rss_bytes: u64,
    supervisor_process_tree_rss_configured_poll_sleep_millis: u64,
}

impl WorkerBinding {
    pub(super) fn capture(
        authority: EvidenceAuthority,
        repository: &RepositoryIdentity,
        arguments: &ReleaseBenchmarkArguments,
        config: &AppConfig,
        executable: &StableFileIdentity,
        argv: &[OsString],
    ) -> Result<Self> {
        Ok(Self {
            authority,
            repository_head: repository.head.clone(),
            repository_tree: repository.tree.clone(),
            repository_clean: repository.clean,
            expected_head: arguments.repository.head.clone(),
            expected_tree: arguments.repository.tree.clone(),
            warm_up_events: arguments.warm_up_events,
            measured_events: arguments.events,
            storage_rows: arguments.storage_rows,
            maximum_tail_growth_mib: arguments.max_tail_growth_mib,
            maximum_tail_growth_percent: arguments.max_tail_growth_percent,
            minimum_events_per_second: arguments.min_events_per_second,
            maximum_warmed_p99_nanos: arguments.max_warmed_p99_ns,
            requested_output: arguments.output.clone(),
            effective_config_sha256: effective_config_hash(config),
            effective_environment_sha256: effective_environment_hash()?,
            executable_sha256: executable.sha256.clone(),
            executable_bytes: executable.byte_count,
            argv_sha256: hash_argv(argv)?,
            argv_count: u64::try_from(argv.len())
                .context("release benchmark argv count exceeds u64")?,
            supervisor_timeout_millis: supervisor_timeout_millis(),
            supervisor_rss_bytes: SUPERVISOR_RSS_BYTES,
            supervisor_process_tree_rss_configured_poll_sleep_millis:
                process::process_tree_rss_poll_sleep_millis(),
        })
    }
}

pub(super) struct SupervisedWorker {
    pub(super) binding: WorkerBinding,
    pub(super) measurements: WorkerMeasurements,
    pub(super) process: ProcessEvidence,
}

pub(super) fn child_arguments() -> Result<Vec<OsString>> {
    let mut arguments: Vec<OsString> = std::env::args_os().skip(1).collect();
    let command = [
        OsStr::new("release"),
        OsStr::new("evidence"),
        OsStr::new("benchmark"),
    ];
    let matches = arguments
        .windows(command.len())
        .enumerate()
        .filter_map(|(index, window)| {
            window
                .iter()
                .zip(command)
                .all(|(argument, expected)| argument == expected)
                .then_some(index)
        })
        .collect::<Vec<_>>();
    let [index] = matches.as_slice() else {
        bail!("release benchmark argv must contain exactly one admitted command chain");
    };
    let worker_index = index
        .checked_add(2)
        .context("release benchmark worker command index overflowed")?;
    let worker = arguments
        .get_mut(worker_index)
        .context("release benchmark worker command is unavailable")?;
    *worker = OsString::from(WORKER_COMMAND);
    Ok(arguments)
}

pub(super) fn current_arguments() -> Vec<OsString> {
    std::env::args_os().skip(1).collect()
}

pub(super) fn supervise(
    executable: &Path,
    repository: &RepositoryIdentity,
    arguments: &[OsString],
    expected: &WorkerBinding,
) -> Result<SupervisedWorker> {
    let output = process::run(ProcessRequest {
        program: executable.as_os_str(),
        arguments,
        current_dir: repository.root(),
        environment: &[],
        limits: ProcessLimits {
            timeout: SUPERVISOR_TIMEOUT,
            rss_bytes: SUPERVISOR_RSS_BYTES,
        },
    })
    .context("release benchmark worker supervision failed")?;
    if output.evidence.stdout_truncated || output.evidence.stderr_truncated {
        bail!("release benchmark worker output exceeded its fixed capture bound");
    }
    let envelope: WorkerEnvelope = serde_json::from_slice(&output.stdout)
        .context("release benchmark worker JSON is invalid")?;
    let canonical = canonical_stdout(&envelope)?;
    if output.stdout != canonical {
        bail!("release benchmark worker emitted noncanonical or additional stdout");
    }
    let (binding, measurements) = envelope.validate(expected)?;
    Ok(SupervisedWorker {
        binding,
        measurements,
        process: output.evidence,
    })
}

pub(super) fn canonical_value(envelope: &WorkerEnvelope) -> Result<serde_json::Value> {
    serde_json::to_value(envelope).context("release benchmark worker envelope is not serializable")
}

pub(super) const fn supervisor_rss_bytes() -> u64 {
    SUPERVISOR_RSS_BYTES
}

pub(super) fn supervisor_timeout_millis() -> u64 {
    u64::try_from(SUPERVISOR_TIMEOUT.as_millis()).unwrap_or(u64::MAX)
}

fn canonical_stdout(envelope: &WorkerEnvelope) -> Result<Vec<u8>> {
    let value = canonical_value(envelope)?;
    let mut bytes =
        serde_json::to_vec(&value).context("release benchmark worker JSON is not serializable")?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn hash_argv(arguments: &[OsString]) -> Result<String> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/release-benchmark-worker-argv/v1");
    hash.update(
        u64::try_from(arguments.len())
            .context("release benchmark argv count exceeds u64")?
            .to_be_bytes(),
    );
    for argument in arguments {
        let bytes = os_bytes(argument.as_os_str());
        hash.update(
            u64::try_from(bytes.len())
                .context("release benchmark argument length exceeds u64")?
                .to_be_bytes(),
        );
        hash.update(bytes);
    }
    Ok(hex_digest(hash.finalize().into()))
}

#[cfg(unix)]
fn os_bytes(value: &OsStr) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt as _;

    value.as_bytes().to_vec()
}

#[cfg(windows)]
fn os_bytes(value: &OsStr) -> Vec<u8> {
    use std::os::windows::ffi::OsStrExt as _;

    value
        .encode_wide()
        .flat_map(u16::to_le_bytes)
        .collect::<Vec<_>>()
}

#[cfg(not(any(unix, windows)))]
fn os_bytes(value: &OsStr) -> Vec<u8> {
    value.to_string_lossy().as_bytes().to_vec()
}

fn effective_config_hash(config: &AppConfig) -> String {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/release-benchmark-effective-config/v1");
    hash.update(format!("{config:?}").as_bytes());
    if let Some(reference) = config.source_secret() {
        hash.update([1]);
        hash.update(reference.expose_reference().as_bytes());
    } else {
        hash.update([0]);
    }
    hex_digest(hash.finalize().into())
}

fn effective_environment_hash() -> Result<String> {
    let mut environment = std::env::vars_os().collect::<BTreeMap<_, _>>();
    for variable in process::REMOVED_BUILD_ENVIRONMENT {
        environment.remove(OsStr::new(variable));
    }
    environment.insert(OsString::from("CARGO_INCREMENTAL"), OsString::from("0"));
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/release-benchmark-effective-environment/v1");
    hash.update(
        u64::try_from(environment.len())
            .context("release benchmark environment count exceeds u64")?
            .to_be_bytes(),
    );
    for (key, value) in environment {
        for field in [key, value] {
            let bytes = os_bytes(&field);
            hash.update(
                u64::try_from(bytes.len())
                    .context("release benchmark environment field exceeds u64")?
                    .to_be_bytes(),
            );
            hash.update(bytes);
        }
    }
    Ok(hex_digest(hash.finalize().into()))
}
