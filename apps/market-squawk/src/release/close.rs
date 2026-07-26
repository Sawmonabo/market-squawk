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
const REQUIRED_PROVIDER_SURFACES: [&str; 8] = [
    "coinbase.public-market-data",
    "coinbase.exchange-direct-market-data",
    "kraken.spot-public-market-data",
    "sec.edgar-public",
    "fred-alfred.api-v1-v2",
    "bls.v1-unregistered",
    "treasury.daily-rates-xml",
    "treasury.fiscal-data",
];
const ALLOWED_PROVIDER_SURFACES: [&str; 9] = [
    "coinbase.public-market-data",
    "coinbase.exchange-direct-market-data",
    "kraken.spot-public-market-data",
    "sec.edgar-public",
    "fred-alfred.api-v1-v2",
    "bls.v1-unregistered",
    "bls.v2-registered",
    "treasury.daily-rates-xml",
    "treasury.fiscal-data",
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
    validate_provider_evidence(&reports[2].2.payload)?;
    validate_python_evidence(&evidence_root.join("python"))?;
    validate_gate_log(&evidence_root.join("full-gate.log"))?;
    let (artifacts, total_artifact_bytes) = inventory_artifacts(&evidence_root)?;
    let binary = hash_stable_file(&arguments.binary, MAXIMUM_BINARY_BYTES)?;
    validate_provider_binary(&reports[2].2.payload, &binary)?;
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

fn validate_provider_evidence(payload: &Value) -> Result<()> {
    if payload.pointer("/schema_version").and_then(Value::as_u64) != Some(1)
        || payload
            .pointer("/requirements/external_network_authorized")
            .and_then(Value::as_bool)
            != Some(true)
        || payload
            .pointer("/requirements/provider_terms_accepted")
            .and_then(Value::as_bool)
            != Some(true)
        || payload
            .pointer("/requirements/direct_verified_action_required")
            .and_then(Value::as_bool)
            != Some(true)
        || payload
            .pointer("/requirements/fred_alfred_rights_required")
            .and_then(Value::as_bool)
            != Some(true)
    {
        bail!("provider release evidence omitted a required acceptance gate");
    }

    let selected = string_set(
        payload
            .pointer("/selected_surfaces")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow::anyhow!("provider evidence omitted selected surfaces"))?,
        "provider selected surfaces",
    )?;
    if selected
        .iter()
        .any(|surface| !ALLOWED_PROVIDER_SURFACES.contains(&surface.as_str()))
        || REQUIRED_PROVIDER_SURFACES
            .iter()
            .any(|required| !selected.contains(*required))
    {
        bail!("provider evidence does not contain the closed mandatory surface set");
    }

    let recovered = string_set(
        payload
            .pointer("/restart_recovery/recovered_surfaces")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow::anyhow!("provider evidence omitted restart recovery"))?,
        "provider recovered surfaces",
    )?;
    if payload
        .pointer("/restart_recovery/completed")
        .and_then(Value::as_bool)
        != Some(true)
        || recovered != selected
    {
        bail!("provider evidence did not recover every selected surface after restart");
    }

    let surfaces = payload
        .pointer("/surfaces")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("provider evidence omitted surface records"))?;
    let mut represented = BTreeSet::new();
    let mut observed_direct_orders = None;
    for surface in surfaces {
        let surface_id = surface
            .pointer("/surface_id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("provider surface evidence omitted its identity"))?;
        if !selected.contains(surface_id) || !represented.insert(surface_id.to_owned()) {
            bail!("provider surface evidence is duplicated or outside the selected set");
        }
        if surface.pointer("/session/state").and_then(Value::as_str) != Some("active_scoped")
            || !nonzero_evidence_digest(
                surface
                    .pointer("/activation/capability_digest")
                    .ok_or_else(|| anyhow::anyhow!("provider capability digest is absent"))?,
            )
            || !nonzero_evidence_digest(
                surface
                    .pointer("/activation/rights_decision_digest")
                    .ok_or_else(|| anyhow::anyhow!("provider rights digest is absent"))?,
            )
            || !nonzero_evidence_digest(
                surface
                    .pointer("/activation/runtime_response_digest")
                    .ok_or_else(|| anyhow::anyhow!("provider runtime receipt is absent"))?,
            )
        {
            bail!("provider surface evidence omitted active immutable authority");
        }
        validate_provider_surface_runtime(surface_id, surface)?;
        if surface_id == "coinbase.exchange-direct-market-data" {
            observed_direct_orders = surface
                .pointer("/live_runtime/orders")
                .and_then(Value::as_array)
                .map(std::vec::Vec::len);
        }
    }
    if represented != selected {
        bail!("provider surface records do not exactly match the selected set");
    }

    if payload
        .pointer("/direct_verified_action/required")
        .and_then(Value::as_bool)
        != Some(true)
        || payload
            .pointer("/direct_verified_action/selected")
            .and_then(Value::as_bool)
            != Some(true)
        || payload
            .pointer("/direct_verified_action/completed")
            .and_then(Value::as_bool)
            != Some(true)
        || payload
            .pointer("/direct_verified_action/order_count")
            .and_then(Value::as_u64)
            .and_then(|count| usize::try_from(count).ok())
            .is_none_or(|count| count == 0 || Some(count) != observed_direct_orders)
    {
        bail!("provider evidence omitted the required DirectVerified paper action");
    }
    if payload
        .pointer("/fred_alfred_rights/required")
        .and_then(Value::as_bool)
        != Some(true)
        || payload
            .pointer("/fred_alfred_rights/selected")
            .and_then(Value::as_bool)
            != Some(true)
        || payload
            .pointer("/fred_alfred_rights/persistence_admitted")
            .and_then(Value::as_bool)
            != Some(true)
        || payload
            .pointer("/fred_alfred_rights/model_training_admitted")
            .and_then(Value::as_bool)
            != Some(true)
        || payload
            .pointer("/fred_alfred_rights/admitted")
            .and_then(Value::as_bool)
            != Some(true)
    {
        bail!("provider evidence omitted admitted FRED and ALFRED durable-use rights");
    }
    Ok(())
}

fn validate_provider_surface_runtime(surface_id: &str, surface: &Value) -> Result<()> {
    match surface_id {
        "coinbase.public-market-data" | "kraken.spot-public-market-data" => {
            let live = surface
                .pointer("/live_runtime")
                .ok_or_else(|| anyhow::anyhow!("public live-provider evidence is absent"))?;
            if live.pointer("/expected_quality").and_then(Value::as_str)
                != Some("direct_unverified")
                || surface
                    .pointer("/live_runtime/action_completed")
                    .and_then(Value::as_bool)
                    != Some(false)
                || !empty_result(live.pointer("/orders"))
                || !valid_live_source_evidence(live, surface_id, "direct_unverified")
            {
                bail!("public live-provider evidence violated its quality/action boundary");
            }
        }
        "coinbase.exchange-direct-market-data" => {
            let live = surface
                .pointer("/live_runtime")
                .ok_or_else(|| anyhow::anyhow!("Coinbase Direct live evidence is absent"))?;
            if live.pointer("/expected_quality").and_then(Value::as_str) != Some("direct_verified")
                || surface
                    .pointer("/live_runtime/action_completed")
                    .and_then(Value::as_bool)
                    != Some(true)
                || live
                    .pointer("/orders")
                    .and_then(Value::as_array)
                    .is_none_or(std::vec::Vec::is_empty)
                || !valid_live_source_evidence(live, surface_id, "direct_verified")
            {
                bail!("Coinbase Direct evidence omitted verified action authority");
            }
        }
        "sec.edgar-public"
        | "fred-alfred.api-v1-v2"
        | "bls.v1-unregistered"
        | "bls.v2-registered"
        | "treasury.fiscal-data" => {
            let runtime = surface
                .pointer("/research_runtime")
                .filter(|runtime| !runtime.is_null())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "durable research-provider evidence omitted its callable runtime"
                    )
                })?;
            if !nonzero_evidence_digest(
                runtime
                    .pointer("/runtime_generation_digest")
                    .ok_or_else(|| anyhow::anyhow!("research runtime digest is absent"))?,
            ) || !nonzero_evidence_digest(
                runtime
                    .pointer("/rights_authorization_digest")
                    .ok_or_else(|| anyhow::anyhow!("research rights digest is absent"))?,
            ) {
                bail!("durable research-provider runtime evidence is invalid");
            }
            if surface_id == "fred-alfred.api-v1-v2"
                && (surface
                    .pointer("/activation/data_use_admission/persist")
                    .and_then(Value::as_bool)
                    != Some(true)
                    || surface
                        .pointer("/activation/data_use_admission/model_training")
                        .and_then(Value::as_bool)
                        != Some(true))
            {
                bail!("FRED/ALFRED runtime lacks admitted durable-use operations");
            }
        }
        "treasury.daily-rates-xml" => {}
        _ => bail!("provider evidence contains an unknown surface"),
    }
    Ok(())
}

fn empty_result(value: Option<&Value>) -> bool {
    value.is_some_and(|value| {
        value.is_null() || value.as_array().is_some_and(std::vec::Vec::is_empty)
    })
}

fn valid_live_source_evidence(live: &Value, surface_id: &str, expected_quality: &str) -> bool {
    let status = live.pointer("/source_status").and_then(Value::as_array);
    let coverage = live.pointer("/source_coverage").and_then(Value::as_array);
    let health = live.pointer("/source_health").and_then(Value::as_array);
    status.is_some_and(|rows| {
        !rows.is_empty()
            && rows.iter().all(|row| {
                row.pointer("/profile/id").and_then(Value::as_str) == Some(surface_id)
                    && row.pointer("/runtime/state").and_then(Value::as_str) == Some("active")
                    && row.pointer("/runtime/quality").and_then(Value::as_str)
                        == Some(expected_quality)
            })
    }) && coverage.is_some_and(|rows| {
        !rows.is_empty()
            && rows.iter().all(|row| {
                row.pointer("/surfaceId").and_then(Value::as_str) == Some(surface_id)
                    && row
                        .pointer("/runtimeCoverage/state")
                        .and_then(Value::as_str)
                        == Some("established")
            })
    }) && health.is_some_and(|rows| {
        !rows.is_empty()
            && rows.iter().all(|row| {
                row.pointer("/surfaceId").and_then(Value::as_str) == Some(surface_id)
                    && row.pointer("/runtimeHealth/state").and_then(Value::as_str) == Some("active")
                    && row
                        .pointer("/runtimeHealth/quality")
                        .and_then(Value::as_str)
                        == Some(expected_quality)
            })
    })
}

fn validate_provider_binary(payload: &Value, binary: &StableFileIdentity) -> Result<()> {
    if payload
        .pointer("/executable/sha256")
        .and_then(Value::as_str)
        != Some(binary.sha256.as_str())
        || payload
            .pointer("/executable/byte_count")
            .and_then(Value::as_u64)
            != Some(binary.byte_count)
    {
        bail!("provider evidence does not bind the exact release executable");
    }
    Ok(())
}

fn string_set(values: &[Value], label: &str) -> Result<BTreeSet<String>> {
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

fn nonzero_evidence_digest(value: &Value) -> bool {
    value.pointer("/algorithm").and_then(Value::as_str) == Some("sha256")
        && value
            .pointer("/bytes")
            .and_then(Value::as_array)
            .is_some_and(|bytes| {
                bytes.len() == 32
                    && bytes
                        .iter()
                        .all(|byte| byte.as_u64().is_some_and(|byte| byte <= u64::from(u8::MAX)))
                    && bytes.iter().any(|byte| byte.as_u64() != Some(0))
            })
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
