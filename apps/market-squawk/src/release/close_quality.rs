//! Strict semantic admission of fuzz and full-gate release evidence.

use anyhow::{Context as _, Result, bail};
use chrono::DateTime;
use serde::Deserialize;
use serde_json::Value;

use super::fuzz::{
    CARGO_FUZZ_VERSION, FUZZ_BUILD_RSS_LIMIT_BYTES, FUZZ_TOOLCHAIN, MAXIMUM_CORPUS_BYTES,
    MAXIMUM_CORPUS_FILES, MAXIMUM_FUZZ_TARGET_BYTES, TARGETS,
};
use super::identity::RepositoryIdentity;
use super::io::{RecordedRepositoryIdentity, StableFileIdentity, hash_stable_file, valid_sha256};
use super::process::{
    ProcessEvidence, ProcessTreeRssObservation, process_tree_rss_poll_sleep_millis,
};

const MAXIMUM_EXECUTABLE_BYTES: u64 = 1024 * 1024 * 1024;
const MAXIMUM_FUZZ_INPUT_BYTES: u64 = 1024 * 1024;
const EXACT_FUZZ_SECONDS: u64 = 120;
const EXACT_FUZZ_RSS_BYTES: u64 = 2_048 * 1024 * 1024;
const EXACT_GATE_TIMEOUT_MILLIS: u64 = 8 * 60 * 60 * 1_000;
const EXACT_GATE_RSS_BYTES: u64 = 16 * 1024 * 1024 * 1024;
pub(super) const TARGET_CEILING_KIB: u64 = 20 * 1024 * 1024;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordedFuzzEvidence {
    repository: RecordedRepositoryIdentity,
    evidence_authority: String,
    application_binary: StableFileIdentity,
    fuzz_manifest: StableFileIdentity,
    fuzz_lock: StableFileIdentity,
    fuzz_toolchain_file: StableFileIdentity,
    application_version: String,
    host_os: String,
    host_arch: String,
    toolchain: String,
    rustc_version: String,
    cargo_fuzz_version: String,
    sanitizer: String,
    seconds_per_target: u64,
    build_rss_limit_bytes: u64,
    rss_limit_bytes: u64,
    target_directory_limit_bytes: u64,
    started_at: String,
    completed_at: String,
    targets: Vec<RecordedFuzzTarget>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordedFuzzTarget {
    name: String,
    feature: String,
    maximum_input_bytes: usize,
    initial_corpus_sha256: String,
    final_corpus_sha256: String,
    final_corpus_files: usize,
    final_corpus_bytes: u64,
    build: ProcessEvidence,
    campaign: ProcessEvidence,
    target_directory_bytes_after_campaign: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordedGateEvidence {
    repository: RecordedRepositoryIdentity,
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

pub(super) fn validate_fuzz_evidence(
    payload: &Value,
    repository: &RepositoryIdentity,
    binary: &StableFileIdentity,
) -> Result<()> {
    let evidence: RecordedFuzzEvidence = serde_json::from_value(payload.clone())
        .context("fuzz evidence payload does not match its strict schema")?;
    evidence.repository.validate_exact(repository)?;
    validate_binary(&evidence.application_binary, binary, "fuzz application")?;
    evidence
        .fuzz_manifest
        .validate(MAXIMUM_FUZZ_INPUT_BYTES, "fuzz manifest")?;
    evidence
        .fuzz_lock
        .validate(MAXIMUM_FUZZ_INPUT_BYTES, "fuzz lock")?;
    evidence
        .fuzz_toolchain_file
        .validate(MAXIMUM_FUZZ_INPUT_BYTES, "fuzz toolchain")?;
    for (recorded, relative, label) in [
        (&evidence.fuzz_manifest, "fuzz/Cargo.toml", "fuzz manifest"),
        (&evidence.fuzz_lock, "fuzz/Cargo.lock", "fuzz lock"),
        (
            &evidence.fuzz_toolchain_file,
            "fuzz/rust-toolchain.toml",
            "fuzz toolchain",
        ),
    ] {
        let current =
            hash_stable_file(&repository.root().join(relative), MAXIMUM_FUZZ_INPUT_BYTES)?;
        validate_binary(recorded, &current, label)?;
    }
    if evidence.evidence_authority != "exact_head"
        || evidence.application_version != env!("CARGO_PKG_VERSION")
        || evidence.host_os.is_empty()
        || evidence.host_arch.is_empty()
        || evidence.toolchain != FUZZ_TOOLCHAIN
        || evidence.rustc_version.is_empty()
        || evidence.cargo_fuzz_version != CARGO_FUZZ_VERSION
        || evidence.sanitizer != "address"
        || evidence.seconds_per_target != EXACT_FUZZ_SECONDS
        || evidence.build_rss_limit_bytes != FUZZ_BUILD_RSS_LIMIT_BYTES
        || evidence.rss_limit_bytes != EXACT_FUZZ_RSS_BYTES
        || evidence.target_directory_limit_bytes != MAXIMUM_FUZZ_TARGET_BYTES
    {
        bail!("fuzz evidence omitted the exact release campaign contract");
    }
    validate_time_order(&evidence.started_at, &evidence.completed_at, "fuzz")?;
    validate_fuzz_targets(
        &evidence.targets,
        evidence.build_rss_limit_bytes,
        evidence.rss_limit_bytes,
    )
}

fn validate_fuzz_targets(
    targets: &[RecordedFuzzTarget],
    build_rss_limit: u64,
    campaign_rss_limit: u64,
) -> Result<()> {
    if targets.len() != TARGETS.len() {
        bail!("fuzz evidence does not contain the exact required target set");
    }
    for (recorded, expected) in targets.iter().zip(TARGETS) {
        if recorded.name != expected.name
            || recorded.feature != expected.feature
            || recorded.maximum_input_bytes != expected.maximum_input_bytes
            || !valid_sha256(&recorded.initial_corpus_sha256)
            || !valid_sha256(&recorded.final_corpus_sha256)
            || recorded.final_corpus_files > MAXIMUM_CORPUS_FILES
            || recorded.final_corpus_bytes > MAXIMUM_CORPUS_BYTES
            || recorded.target_directory_bytes_after_campaign > MAXIMUM_FUZZ_TARGET_BYTES
        {
            bail!("fuzz target evidence violates its exact bounded contract");
        }
        validate_process(&recorded.build, build_rss_limit, "fuzz build")?;
        validate_process(&recorded.campaign, campaign_rss_limit, "fuzz campaign")?;
    }
    Ok(())
}

pub(super) fn validate_gate_evidence(
    payload: &Value,
    repository: &RepositoryIdentity,
    binary: &StableFileIdentity,
    verify_script: &StableFileIdentity,
    full_gate_log: &StableFileIdentity,
) -> Result<()> {
    let evidence: RecordedGateEvidence = serde_json::from_value(payload.clone())
        .context("full-gate evidence payload does not match its strict schema")?;
    evidence.repository.validate_exact(repository)?;
    validate_binary(
        &evidence.application_binary,
        binary,
        "full-gate application",
    )?;
    validate_binary(
        &evidence.verify_script,
        verify_script,
        "full-gate verification script",
    )?;
    validate_binary(
        &evidence.full_gate_log,
        full_gate_log,
        "full-gate diagnostic log",
    )?;
    if evidence.process_timeout_millis != EXACT_GATE_TIMEOUT_MILLIS
        || evidence.process_rss_limit_bytes != EXACT_GATE_RSS_BYTES
        || evidence.gate_process.elapsed_millis == 0
        || evidence.gate_process.elapsed_millis > evidence.process_timeout_millis
        || evidence.target_ceiling_kib != TARGET_CEILING_KIB
        || evidence.target_usage_kib == 0
        || evidence.target_usage_kib > evidence.target_ceiling_kib
    {
        bail!("full-gate evidence does not prove successful bounded verification");
    }
    validate_process(
        &evidence.gate_process,
        evidence.process_rss_limit_bytes,
        "full gate",
    )?;
    validate_time_order(&evidence.started_at, &evidence.completed_at, "full gate")
}

pub(super) fn validate_binary(
    recorded: &StableFileIdentity,
    expected: &StableFileIdentity,
    label: &str,
) -> Result<()> {
    recorded.validate(MAXIMUM_EXECUTABLE_BYTES, label)?;
    if recorded.sha256 != expected.sha256 || recorded.byte_count != expected.byte_count {
        bail!("{label} does not match the exact selected file");
    }
    Ok(())
}

pub(super) fn validate_process(
    process: &ProcessEvidence,
    rss_limit: u64,
    label: &str,
) -> Result<()> {
    if process.exit_code != 0
        || !valid_sha256(&process.stdout_sha256)
        || !valid_sha256(&process.stderr_sha256)
    {
        bail!("{label} did not complete successfully with bounded output identities");
    }
    validate_rss_observation(&process.process_tree_rss_observation, rss_limit, label)
}

pub(super) fn validate_rss_observation(
    observation: &ProcessTreeRssObservation,
    rss_limit: u64,
    label: &str,
) -> Result<()> {
    if observation.successful_sample_count == 0
        || observation.configured_poll_sleep_millis != process_tree_rss_poll_sleep_millis()
        || observation
            .observed_maximum_rss_bytes
            .is_none_or(|bytes| bytes == 0 || bytes > rss_limit)
    {
        bail!("{label} RSS evidence does not satisfy its sampled process-tree bound");
    }
    Ok(())
}

pub(super) fn validate_time_order(started: &str, completed: &str, label: &str) -> Result<()> {
    let started = parse_time(started, &format!("{label} start"))?;
    let completed = parse_time(completed, &format!("{label} completion"))?;
    if completed < started {
        bail!("{label} completion precedes its start");
    }
    Ok(())
}

fn parse_time(value: &str, label: &str) -> Result<DateTime<chrono::FixedOffset>> {
    DateTime::parse_from_rfc3339(value).with_context(|| format!("{label} time is invalid"))
}

#[cfg(test)]
mod tests {
    use super::{TARGETS, validate_fuzz_targets};

    #[test]
    fn closing_contract_rejects_incomplete_fuzz_target_set() {
        assert!(validate_fuzz_targets(&[], 1, 1).is_err());
        assert_eq!(TARGETS.len(), 6);
    }
}
