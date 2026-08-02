//! Parent-supervised exact-head full verification gate and receipt.

use std::collections::HashSet;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use chrono::{SecondsFormat, Utc};
use serde::Serialize;
use serde_json::Value;

use super::close::validate_gate_log;
use super::close_quality::TARGET_CEILING_KIB;
use super::identity::RepositoryIdentity;
use super::io::{
    PublishedReport, StableFileIdentity, hash_stable_file, publish_report_with_identity_barrier,
};
use super::process::{ProcessEvidence, ProcessLimits, ProcessRequest};
use crate::cli::ReleaseGateArguments;

const REPORT_KIND: &str = "market_squawk.release.full_gate";
const MAXIMUM_EXECUTABLE_BYTES: u64 = 1024 * 1024 * 1024;
const MAXIMUM_INPUT_BYTES: u64 = 64 * 1024 * 1024;
const MAXIMUM_TARGET_ENTRIES: usize = 5_000_000;
const GATE_TIMEOUT: Duration = Duration::from_secs(8 * 60 * 60);
const GATE_RSS_BYTES: u64 = 16 * 1024 * 1024 * 1024;
const GATE_LOG_ENVIRONMENT: &str = "MARKET_SQUAWK_FULL_GATE_LOG";
const GATE_SHELL: &str = "/bin/bash";
const GATE_SHELL_COMMAND: &str = "umask 077; set -o noclobber; \
    ./scripts/verify.sh 2>&1 | \
    (ulimit -f 65536; exec /bin/cat >\"${MARKET_SQUAWK_FULL_GATE_LOG:?}\")";

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct GateEvidence {
    repository: RepositoryIdentity,
    application_binary: StableFileIdentity,
    verify_script: StableFileIdentity,
    full_gate_log: StableFileIdentity,
    gate_process: ProcessEvidence,
    process_timeout_millis: u64,
    process_rss_limit_bytes: u64,
    target_ceiling_kib: u64,
    target_usage_kib: u64,
    started_at: String,
    completed_at: String,
}

pub(super) fn run(arguments: ReleaseGateArguments) -> Result<Value> {
    if arguments.repository.head.is_none() || arguments.repository.tree.is_none() {
        bail!("full-gate evidence requires exact --head and --tree");
    }
    let repository = RepositoryIdentity::admit(&arguments.repository)?;
    let gate_log_path = validate_layout(&arguments, &repository)?;
    ensure_absent(&gate_log_path, "full-gate log")?;
    ensure_absent(&arguments.output, "full-gate report")?;

    let executable = std::env::current_exe().context("running executable path is unavailable")?;
    let running_binary = hash_stable_file(&executable, MAXIMUM_EXECUTABLE_BYTES)?;
    let selected_binary = hash_stable_file(&arguments.binary, MAXIMUM_EXECUTABLE_BYTES)?;
    if running_binary != selected_binary {
        bail!("full-gate receipt must be produced by the exact selected release executable");
    }
    let verify_path = repository.root().join("scripts/verify.sh");
    let verify_script = hash_stable_file(&verify_path, MAXIMUM_INPUT_BYTES)?;
    let started_at = now();
    let gate_process = run_gate(&repository, &gate_log_path)?;
    validate_gate_log(&gate_log_path)?;
    let full_gate_log = hash_stable_file(&gate_log_path, MAXIMUM_INPUT_BYTES)?;
    let application_binary = hash_stable_file(&arguments.binary, MAXIMUM_EXECUTABLE_BYTES)?;
    if application_binary != selected_binary
        || hash_stable_file(&executable, MAXIMUM_EXECUTABLE_BYTES)? != application_binary
    {
        bail!("full-gate release executable changed while verification ran");
    }
    let target_usage_kib = measure_target_usage_kib(repository.root())?;
    if target_usage_kib > TARGET_CEILING_KIB {
        bail!("full-gate target usage exceeds the fixed 20 GiB ceiling");
    }

    let payload = GateEvidence {
        repository,
        application_binary,
        verify_script,
        full_gate_log,
        gate_process,
        process_timeout_millis: gate_timeout_millis(),
        process_rss_limit_bytes: GATE_RSS_BYTES,
        target_ceiling_kib: TARGET_CEILING_KIB,
        target_usage_kib,
        started_at,
        completed_at: now(),
    };
    let published =
        publish_report_with_identity_barrier(&arguments.output, REPORT_KIND, &payload, || {
            revalidate(&payload, &executable, &verify_path)
        })?;
    Ok(publication_value(&published))
}

fn validate_layout(
    arguments: &ReleaseGateArguments,
    repository: &RepositoryIdentity,
) -> Result<PathBuf> {
    for path in [&arguments.gate_log, &arguments.output] {
        if path
            .components()
            .any(|component| matches!(component, Component::ParentDir))
        {
            bail!("full-gate evidence path contains parent traversal");
        }
    }
    if arguments
        .gate_log
        .file_name()
        .and_then(|name| name.to_str())
        != Some("full-gate.log")
        || arguments.output.file_name().and_then(|name| name.to_str()) != Some("full-gate.json")
    {
        bail!("full-gate evidence requires full-gate.log and full-gate.json");
    }
    let log_parent = arguments
        .gate_log
        .parent()
        .context("full-gate log has no parent")?
        .canonicalize()
        .context("full-gate log parent is unavailable")?;
    let output_parent = arguments
        .output
        .parent()
        .context("full-gate output has no parent")?
        .canonicalize()
        .context("full-gate output parent is unavailable")?;
    if log_parent != output_parent
        || output_parent.file_name().and_then(|name| name.to_str())
            != Some(repository.head.as_str())
    {
        bail!("full-gate receipt and log are not in one exact-HEAD evidence root");
    }
    Ok(log_parent.join("full-gate.log"))
}

fn ensure_absent(path: &Path, label: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => bail!("{label} already exists"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("{label} state is unavailable")),
    }
}

fn run_gate(repository: &RepositoryIdentity, gate_log: &Path) -> Result<ProcessEvidence> {
    let arguments = [
        OsString::from("-o"),
        OsString::from("errexit"),
        OsString::from("-o"),
        OsString::from("nounset"),
        OsString::from("-o"),
        OsString::from("pipefail"),
        OsString::from("-c"),
        OsString::from(GATE_SHELL_COMMAND),
    ];
    let environment = [(
        OsString::from(GATE_LOG_ENVIRONMENT),
        gate_log.as_os_str().to_owned(),
    )];
    let output = super::process::run(ProcessRequest {
        program: OsStr::new(GATE_SHELL),
        arguments: &arguments,
        current_dir: repository.root(),
        environment: &environment,
        limits: ProcessLimits {
            timeout: GATE_TIMEOUT,
            rss_bytes: GATE_RSS_BYTES,
        },
    })
    .context("full verification gate failed")?;
    Ok(output.evidence)
}

fn revalidate(payload: &GateEvidence, executable: &Path, verify_path: &Path) -> Result<()> {
    validate_gate_log(payload.full_gate_log.canonical_path())?;
    let current_target_usage = measure_target_usage_kib(payload.repository.root())?;
    if hash_stable_file(executable, MAXIMUM_EXECUTABLE_BYTES)? != payload.application_binary
        || hash_stable_file(
            payload.application_binary.canonical_path(),
            MAXIMUM_EXECUTABLE_BYTES,
        )? != payload.application_binary
        || hash_stable_file(verify_path, MAXIMUM_INPUT_BYTES)? != payload.verify_script
        || hash_stable_file(payload.full_gate_log.canonical_path(), MAXIMUM_INPUT_BYTES)?
            != payload.full_gate_log
        || current_target_usage > payload.target_ceiling_kib
    {
        bail!("full-gate immutable inputs changed at the publication barrier");
    }
    payload.repository.verify_unchanged()
}

fn measure_target_usage_kib(repository_root: &Path) -> Result<u64> {
    let target = repository_root.join("target");
    let root =
        fs::symlink_metadata(&target).context("full-gate target directory is unavailable")?;
    if root.file_type().is_symlink() || !root.is_dir() {
        bail!("full-gate target path is not a real directory");
    }
    let mut pending = vec![target];
    let mut seen = HashSet::new();
    let mut entries = 0_usize;
    let mut allocated_bytes = 0_u64;
    while let Some(path) = pending.pop() {
        entries = entries
            .checked_add(1)
            .context("full-gate target entry count overflowed")?;
        if entries > MAXIMUM_TARGET_ENTRIES {
            bail!("full-gate target tree exceeds its entry-count bound");
        }
        let metadata =
            fs::symlink_metadata(&path).context("full-gate target entry is unavailable")?;
        if metadata.file_type().is_symlink() {
            bail!("full-gate target tree contains a symbolic link");
        }
        if !metadata.is_dir() && !metadata.is_file() {
            bail!("full-gate target tree contains a special file");
        }
        if seen.insert(file_identity(&path, &metadata)) {
            allocated_bytes = allocated_bytes
                .checked_add(allocated_file_bytes(&metadata)?)
                .context("full-gate target allocated-byte total overflowed")?;
        }
        if metadata.is_dir() {
            for entry in fs::read_dir(&path).context("full-gate target directory cannot be read")? {
                pending.push(
                    entry
                        .context("full-gate target entry cannot be read")?
                        .path(),
                );
                if pending.len() > MAXIMUM_TARGET_ENTRIES {
                    bail!("full-gate target traversal exceeds its pending-entry bound");
                }
            }
        }
    }
    Ok(allocated_bytes.div_ceil(1024))
}

#[cfg(unix)]
fn file_identity(_path: &Path, metadata: &fs::Metadata) -> (u64, u64) {
    use std::os::unix::fs::MetadataExt as _;

    (metadata.dev(), metadata.ino())
}

#[cfg(not(unix))]
fn file_identity(path: &Path, _metadata: &fs::Metadata) -> PathBuf {
    path.to_path_buf()
}

#[cfg(unix)]
fn allocated_file_bytes(metadata: &fs::Metadata) -> Result<u64> {
    use std::os::unix::fs::MetadataExt as _;

    metadata
        .blocks()
        .checked_mul(512)
        .context("full-gate target allocated-byte value overflowed")
}

#[cfg(not(unix))]
fn allocated_file_bytes(metadata: &fs::Metadata) -> Result<u64> {
    Ok(metadata.len())
}

fn gate_timeout_millis() -> u64 {
    u64::try_from(GATE_TIMEOUT.as_millis()).unwrap_or(u64::MAX)
}

fn now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn publication_value(published: &PublishedReport) -> Value {
    serde_json::json!({
        "path": published.path,
        "sha256": published.sha256,
        "byte_count": published.byte_count,
    })
}
