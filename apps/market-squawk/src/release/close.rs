//! Exact-head evidence-set validation and closed-manifest publication.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context as _, Result, bail};
use chrono::{SecondsFormat, Utc};
use serde::Serialize;
use serde_json::Value;

use super::identity::RepositoryIdentity;
use super::io::{
    PublishedReport, StableFileIdentity, VerifiedReport, hash_stable_file, publish_report,
    read_report, read_stable_bytes,
};
use crate::cli::ReleaseCloseArguments;

const MAXIMUM_REPORT_BYTES: u64 = 64 * 1024 * 1024;
const MAXIMUM_ARTIFACT_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const MAXIMUM_EVIDENCE_BYTES: u64 = 16 * 1024 * 1024 * 1024;
const MAXIMUM_EVIDENCE_FILES: usize = 10_000;
const MAXIMUM_BINARY_BYTES: u64 = 1024 * 1024 * 1024;
const REQUIRED_ROOT_ENTRIES: [&str; 6] = [
    "demo.json",
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

#[derive(Debug, Serialize)]
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
    validate_root_entries(&evidence_root)?;
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
    ];
    for (path, kind, report) in &reports {
        validate_report_identity(report, &repository)
            .with_context(|| format!("{kind} at {path} is not exact-head evidence"))?;
        reject_credentials(&report.payload)?;
        if report.payload_sha256.len() != 64 || report.file.byte_count == 0 {
            bail!("release-evidence report identity is invalid");
        }
    }
    validate_python_evidence(&evidence_root.join("python"))?;
    validate_gate_log(&evidence_root.join("full-gate.log"))?;
    let (artifacts, total_artifact_bytes) = inventory_artifacts(&evidence_root)?;
    let binary = hash_stable_file(&arguments.binary, MAXIMUM_BINARY_BYTES)?;
    repository.verify_unchanged()?;
    let payload = ClosedEvidence {
        repository,
        binary,
        closed_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
        total_artifact_bytes,
        artifacts,
    };
    let published = publish_report(
        &arguments.output,
        "market_squawk.release.closed_manifest",
        &payload,
    )?;
    Ok(publication_value(&published))
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

fn validate_root_entries(root: &Path) -> Result<()> {
    let mut actual = BTreeSet::new();
    for entry in fs::read_dir(root).context("release-evidence root cannot be read")? {
        let entry = entry.context("release-evidence root entry cannot be read")?;
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

fn validate_report_identity(
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

fn validate_python_evidence(directory: &Path) -> Result<()> {
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
    reject_credentials(&evidence)?;
    reject_credentials(&release)?;
    Ok(())
}

fn validate_gate_log(path: &Path) -> Result<()> {
    let bytes = read_stable_bytes(path, MAXIMUM_REPORT_BYTES)?;
    if bytes.is_empty() || bytes.contains(&0) {
        bail!("full release-gate log is empty or invalid");
    }
    let text = std::str::from_utf8(&bytes).context("full release-gate log is not UTF-8")?;
    for marker in ["offline mock smoke test passed", "MCP smoke test passed"] {
        if !text.contains(marker) {
            bail!("full release-gate log omitted a terminal verification marker");
        }
    }
    reject_secret_text(text)
}

fn inventory_artifacts(root: &Path) -> Result<(Vec<ArtifactIdentity>, u64)> {
    let mut pending = vec![root.to_path_buf()];
    let mut paths = Vec::new();
    while let Some(directory) = pending.pop() {
        for entry in
            fs::read_dir(&directory).context("release artifact directory cannot be read")?
        {
            let entry = entry.context("release artifact entry cannot be read")?;
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

fn reject_credentials(value: &Value) -> Result<()> {
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
