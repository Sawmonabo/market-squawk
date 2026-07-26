//! Exact-head evidence-set validation and closed-manifest publication.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context as _, Result, bail};
use chrono::{SecondsFormat, Utc};
use serde::Serialize;
use serde_json::Value;

use super::close_demonstration::validate_demonstration_evidence;
use super::close_performance::validate_performance_evidence;
pub(super) use super::close_provider::{validate_provider_binary, validate_provider_evidence};
use super::close_quality::{validate_fuzz_evidence, validate_gate_evidence};
use super::identity::RepositoryIdentity;
use super::io::{
    PublishedReport, StableFileIdentity, VerifiedReport, hash_stable_file, is_pending_report_path,
    publish_report_with_pending_identity_barrier, read_report, read_stable_bytes,
};
use crate::cli::ReleaseCloseArguments;

const MAXIMUM_REPORT_BYTES: u64 = 64 * 1024 * 1024;
const MAXIMUM_ARTIFACT_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const MAXIMUM_EVIDENCE_BYTES: u64 = 16 * 1024 * 1024 * 1024;
const MAXIMUM_EVIDENCE_FILES: usize = 10_000;
const MAXIMUM_BINARY_BYTES: u64 = 1024 * 1024 * 1024;
const REQUIRED_ROOT_ENTRIES: [&str; 7] = [
    "demo.json",
    "full-gate.json",
    "full-gate.log",
    "fuzz.json",
    "performance.json",
    "providers",
    "python",
];
#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct ClosedEvidence {
    repository: RepositoryIdentity,
    binary: StableFileIdentity,
    closed_at: String,
    total_artifact_bytes: u64,
    artifacts: Vec<ArtifactIdentity>,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ArtifactIdentity {
    path: String,
    sha256: String,
    byte_count: u64,
}

pub(super) fn run(arguments: ReleaseCloseArguments) -> Result<Value> {
    if arguments.repository.head.is_none() || arguments.repository.tree.is_none() {
        bail!("release evidence closure requires exact --head and --tree");
    }
    let repository = RepositoryIdentity::admit(&arguments.repository)?;
    let evidence_root = admit_evidence_root(&arguments.evidence_dir, &repository)?;
    admit_output_path(&evidence_root, &arguments.output)?;
    validate_root_entries(&evidence_root, None)?;
    let reports = [
        (
            "fuzz.json",
            "market_squawk.release.fuzz",
            read_report(
                &evidence_root.join("fuzz.json"),
                MAXIMUM_REPORT_BYTES,
                "market_squawk.release.fuzz",
            )?,
        ),
        (
            "performance.json",
            "market_squawk.release.performance",
            read_report(
                &evidence_root.join("performance.json"),
                MAXIMUM_REPORT_BYTES,
                "market_squawk.release.performance",
            )?,
        ),
        (
            "providers/provider-evidence.json",
            "market_squawk.release.providers",
            read_report(
                &evidence_root.join("providers/provider-evidence.json"),
                MAXIMUM_REPORT_BYTES,
                "market_squawk.release.providers",
            )?,
        ),
        (
            "demo.json",
            "market_squawk.release.demonstration",
            read_report(
                &evidence_root.join("demo.json"),
                MAXIMUM_REPORT_BYTES,
                "market_squawk.release.demonstration",
            )?,
        ),
        (
            "full-gate.json",
            "market_squawk.release.full_gate",
            read_report(
                &evidence_root.join("full-gate.json"),
                MAXIMUM_REPORT_BYTES,
                "market_squawk.release.full_gate",
            )?,
        ),
    ];
    for (path, kind, report) in &reports {
        validate_report_identity(report, &repository)
            .with_context(|| format!("{kind} at {path} is not exact-head evidence"))?;
        reject_credentials(&report.payload)?;
        if report.payload_sha256.len() != 64 || report.file.byte_count == 0 {
            bail!("release-evidence report identity is invalid");
        }
    }
    let binary = hash_stable_file(&arguments.binary, MAXIMUM_BINARY_BYTES)?;
    let verify_path = repository.root().join("scripts/verify.sh");
    let verify_script = hash_stable_file(&verify_path, MAXIMUM_REPORT_BYTES)?;
    let gate_log_path = evidence_root.join("full-gate.log");
    validate_gate_log(&gate_log_path)?;
    let full_gate_log = hash_stable_file(&gate_log_path, MAXIMUM_REPORT_BYTES)?;
    validate_fuzz_evidence(&reports[0].2.payload, &repository, &binary)?;
    validate_performance_evidence(&reports[1].2.payload, &repository, &binary)?;
    validate_provider_evidence(&reports[2].2.payload)?;
    validate_provider_binary(&reports[2].2.payload, &binary)?;
    validate_python_evidence(&evidence_root.join("python"), &binary)?;
    validate_demonstration_evidence(
        &reports[3].2.payload,
        &reports[2].2.file,
        &evidence_root.join("python"),
        &binary,
    )?;
    validate_gate_evidence(
        &reports[4].2.payload,
        &repository,
        &binary,
        &verify_script,
        &full_gate_log,
    )?;
    let (artifacts, total_artifact_bytes) = inventory_artifacts(&evidence_root, None)?;
    repository.verify_unchanged()?;
    let payload = ClosedEvidence {
        repository,
        binary,
        closed_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
        total_artifact_bytes,
        artifacts,
    };
    let published = publish_report_with_pending_identity_barrier(
        &arguments.output,
        "market_squawk.release.closed_manifest",
        &payload,
        |pending| {
            revalidate_closure_inputs(
                &arguments,
                &evidence_root,
                &verify_script,
                &full_gate_log,
                &payload,
                pending,
            )
        },
    )?;
    Ok(publication_value(&published))
}

fn revalidate_closure_inputs(
    arguments: &ReleaseCloseArguments,
    evidence_root: &Path,
    verify_script: &StableFileIdentity,
    full_gate_log: &StableFileIdentity,
    payload: &ClosedEvidence,
    permitted_pending: Option<&Path>,
) -> Result<()> {
    if admit_evidence_root(&arguments.evidence_dir, &payload.repository)? != evidence_root {
        bail!("release-evidence root changed at the publication barrier");
    }
    validate_root_entries(evidence_root, permitted_pending)?;
    let verify_path = payload.repository.root().join("scripts/verify.sh");
    let gate_log_path = evidence_root.join("full-gate.log");
    validate_gate_log(&gate_log_path)?;
    if hash_stable_file(&arguments.binary, MAXIMUM_BINARY_BYTES)? != payload.binary
        || hash_stable_file(&verify_path, MAXIMUM_REPORT_BYTES)? != *verify_script
        || hash_stable_file(&gate_log_path, MAXIMUM_REPORT_BYTES)? != *full_gate_log
    {
        bail!("release-closure immutable input changed at the publication barrier");
    }
    let (artifacts, total_artifact_bytes) = inventory_artifacts(evidence_root, permitted_pending)?;
    if artifacts != payload.artifacts || total_artifact_bytes != payload.total_artifact_bytes {
        bail!("release-evidence artifact inventory changed at the publication barrier");
    }
    payload.repository.verify_unchanged()
}

pub(super) fn string_set(values: &[Value], label: &str) -> Result<BTreeSet<String>> {
    let mut set = BTreeSet::new();
    for value in values {
        let value = value
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("{label} contains a non-string identity"))?;
        if value.is_empty() || !set.insert(value.to_owned()) {
            bail!("{label} contains an empty or duplicate identity");
        }
    }
    if set.is_empty() {
        bail!("{label} is empty");
    }
    Ok(set)
}

fn admit_evidence_root(requested: &Path, repository: &RepositoryIdentity) -> Result<PathBuf> {
    let named =
        fs::symlink_metadata(requested).context("release-evidence directory is unavailable")?;
    if named.file_type().is_symlink() || !named.is_dir() {
        bail!("release-evidence root is not a real directory");
    }
    let canonical = requested
        .canonicalize()
        .context("release-evidence root cannot be canonicalized")?;
    let expected_name = repository.head.as_str();
    if canonical.file_name().and_then(|name| name.to_str()) != Some(expected_name) {
        bail!("release-evidence directory is not keyed by the exact candidate HEAD");
    }
    Ok(canonical)
}

fn admit_output_path(root: &Path, requested: &Path) -> Result<()> {
    if requested
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        bail!("closed-manifest output contains parent traversal");
    }
    let parent = requested
        .parent()
        .ok_or_else(|| anyhow::anyhow!("closed-manifest output has no parent"))?
        .canonicalize()
        .context("closed-manifest output parent is unavailable")?;
    if parent != root
        || requested.file_name().and_then(|name| name.to_str()) != Some("manifest.json")
    {
        bail!("closed manifest must be the evidence root's manifest.json");
    }
    if fs::symlink_metadata(root.join("manifest.json")).is_ok() {
        bail!("closed release manifest already exists");
    }
    Ok(())
}

fn validate_root_entries(root: &Path, permitted_pending: Option<&Path>) -> Result<()> {
    let mut actual = BTreeSet::new();
    for entry in fs::read_dir(root).context("release-evidence root cannot be read")? {
        let entry = entry.context("release-evidence root entry cannot be read")?;
        if permitted_pending.is_some_and(|path| entry.path() == path) {
            continue;
        }
        if is_pending_report_path(&entry.path()) {
            bail!("release-evidence root contains an unexpected pending report");
        }
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| anyhow::anyhow!("release-evidence root contains a non-UTF-8 entry"))?;
        actual.insert(name);
    }
    let expected = REQUIRED_ROOT_ENTRIES
        .into_iter()
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    if actual != expected {
        bail!("release-evidence root has missing or extra top-level entries");
    }
    for directory in ["providers", "python"] {
        let metadata = fs::symlink_metadata(root.join(directory))
            .context("release-evidence directory metadata is unavailable")?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            bail!("release-evidence child is not a real directory");
        }
    }
    Ok(())
}

pub(super) fn validate_report_identity(
    report: &VerifiedReport,
    repository: &RepositoryIdentity,
) -> Result<()> {
    let head = report
        .payload
        .pointer("/repository/head")
        .and_then(Value::as_str);
    let tree = report
        .payload
        .pointer("/repository/tree")
        .and_then(Value::as_str);
    let clean = report
        .payload
        .pointer("/repository/clean")
        .and_then(Value::as_bool);
    if head != Some(repository.head.as_str())
        || tree != Some(repository.tree.as_str())
        || clean != Some(true)
    {
        bail!("release-evidence report repository identity is invalid");
    }
    Ok(())
}

pub(super) fn validate_python_evidence(
    directory: &Path,
    application_binary: &StableFileIdentity,
) -> Result<()> {
    let evidence_path = directory.join("market-squawk-release-evidence.json");
    let release_path = directory.join("market-squawk-release.json");
    let evidence_bytes = read_stable_bytes(&evidence_path, MAXIMUM_REPORT_BYTES)?;
    let release_bytes = read_stable_bytes(&release_path, MAXIMUM_REPORT_BYTES)?;
    let evidence: Value = serde_json::from_slice(&evidence_bytes)
        .context("Python release evidence is invalid JSON")?;
    let release: Value = serde_json::from_slice(&release_bytes)
        .context("Python release manifest is invalid JSON")?;
    if evidence.pointer("/schema_version").and_then(Value::as_u64) != Some(5)
        || release.pointer("/schema_version").and_then(Value::as_u64) != Some(2)
        || release
            .pointer("/payload/schema_version")
            .and_then(Value::as_u64)
            != Some(2)
    {
        bail!("Python release evidence schema is invalid");
    }
    let declared = evidence
        .pointer("/release_manifest/sha256")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("Python evidence omitted its release-manifest identity"))?;
    let release_identity = hash_stable_file(&release_path, MAXIMUM_REPORT_BYTES)?;
    if declared != release_identity.sha256 {
        bail!("Python evidence does not bind its exact release manifest");
    }
    if release
        .pointer("/payload/application/sha256")
        .and_then(Value::as_str)
        != Some(application_binary.sha256.as_str())
        || release
            .pointer("/payload/application/size_bytes")
            .and_then(Value::as_u64)
            != Some(application_binary.byte_count)
    {
        bail!("Python release manifest does not bind the selected application binary");
    }
    reject_credentials(&evidence)?;
    reject_credentials(&release)?;
    Ok(())
}

pub(super) fn validate_gate_log(path: &Path) -> Result<()> {
    let bytes = read_stable_bytes(path, MAXIMUM_REPORT_BYTES)?;
    if bytes.is_empty() || bytes.contains(&0) {
        bail!("full release-gate log is empty or invalid");
    }
    let text = std::str::from_utf8(&bytes).context("full release-gate log is not UTF-8")?;
    reject_secret_text(text)
}

fn inventory_artifacts(
    root: &Path,
    permitted_pending: Option<&Path>,
) -> Result<(Vec<ArtifactIdentity>, u64)> {
    let mut pending = vec![root.to_path_buf()];
    let mut paths = Vec::new();
    while let Some(directory) = pending.pop() {
        for entry in
            fs::read_dir(&directory).context("release artifact directory cannot be read")?
        {
            let entry = entry.context("release artifact entry cannot be read")?;
            if permitted_pending.is_some_and(|path| entry.path() == path) {
                continue;
            }
            if is_pending_report_path(&entry.path()) {
                bail!("release evidence contains an unexpected pending report");
            }
            let file_type = entry
                .file_type()
                .context("release artifact type is unavailable")?;
            if file_type.is_symlink() {
                bail!("release evidence contains a symbolic link");
            }
            if file_type.is_dir() {
                pending.push(entry.path());
            } else if file_type.is_file() {
                paths.push(entry.path());
                if paths.len() > MAXIMUM_EVIDENCE_FILES {
                    bail!("release evidence exceeded its file-count bound");
                }
            } else {
                bail!("release evidence contains an unsupported file type");
            }
        }
    }
    paths.sort();
    let mut total = 0_u64;
    let mut artifacts = Vec::new();
    artifacts
        .try_reserve_exact(paths.len())
        .context("release artifact inventory allocation failed")?;
    for path in paths {
        let relative = path
            .strip_prefix(root)
            .context("release artifact escaped its evidence root")?;
        let relative = relative
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("release artifact path is not UTF-8"))?
            .replace('\\', "/");
        let identity = hash_stable_file(&path, MAXIMUM_ARTIFACT_BYTES)?;
        total = total
            .checked_add(identity.byte_count)
            .ok_or_else(|| anyhow::anyhow!("release artifact byte total overflow"))?;
        if total > MAXIMUM_EVIDENCE_BYTES {
            bail!("release evidence exceeded its total byte bound");
        }
        artifacts.push(ArtifactIdentity {
            path: relative,
            sha256: identity.sha256,
            byte_count: identity.byte_count,
        });
    }
    Ok((artifacts, total))
}

pub(super) fn reject_credentials(value: &Value) -> Result<()> {
    match value {
        Value::Object(object) => {
            for (key, child) in object {
                let normalized = key.to_ascii_lowercase().replace('-', "_");
                if matches!(
                    normalized.as_str(),
                    "api_key"
                        | "authorization_header"
                        | "cookie"
                        | "credential"
                        | "password"
                        | "private_key"
                        | "secret"
                        | "token"
                ) {
                    bail!("release evidence contains a credential-bearing field");
                }
                reject_credentials(child)?;
            }
        }
        Value::Array(values) => {
            for child in values {
                reject_credentials(child)?;
            }
        }
        Value::String(text) => reject_secret_text(text)?,
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
    Ok(())
}

fn reject_secret_text(text: &str) -> Result<()> {
    for marker in [
        "-----BEGIN PRIVATE KEY-----",
        "-----BEGIN EC PRIVATE KEY-----",
        "Authorization: Bearer ",
        "Authorization: Basic ",
    ] {
        if text.contains(marker) {
            bail!("release evidence contains credential material");
        }
    }
    Ok(())
}

fn publication_value(published: &PublishedReport) -> Value {
    serde_json::json!({
        "path": published.path,
        "sha256": published.sha256,
        "byte_count": published.byte_count,
    })
}
