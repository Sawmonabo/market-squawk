//! Deterministic offline release demonstration over production application boundaries.

#[path = "demonstrate/local.rs"]
mod local;

use std::fs;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context as _, Result, bail};
use chrono::{SecondsFormat, Utc};
use serde::Serialize;
use serde_json::Value;

use super::close::{
    reject_credentials, validate_provider_binary, validate_provider_evidence,
    validate_python_evidence, validate_report_identity,
};
use super::identity::RepositoryIdentity;
use super::io::{
    PublishedReport, StableFileIdentity, hash_stable_file, publish_report_with_identity_barrier,
    read_report,
};
use crate::AppConfig;
use crate::cli::ReleaseDemonstrateArguments;

const REPORT_KIND: &str = "market_squawk.release.demonstration";
const MAXIMUM_REPORT_BYTES: u64 = 64 * 1024 * 1024;
const MAXIMUM_EXECUTABLE_BYTES: u64 = 1024 * 1024 * 1024;

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct DemonstrationEvidence {
    schema_version: u32,
    repository: RepositoryIdentity,
    offline: bool,
    inputs: DemonstrationInputs,
    production_kernels: Value,
    local_application: local::LocalApplicationEvidence,
    completed: bool,
    completed_at: String,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct DemonstrationInputs {
    provider_report: StableFileIdentity,
    python_release_manifest: StableFileIdentity,
    python_release_evidence: StableFileIdentity,
    application_binary: StableFileIdentity,
}

#[derive(Debug)]
struct DemonstrationLayout {
    evidence_root: PathBuf,
    provider_directory: PathBuf,
    provider_report: PathBuf,
    python_directory: PathBuf,
    python_manifest: PathBuf,
}

pub(super) async fn run(
    config: AppConfig,
    arguments: ReleaseDemonstrateArguments,
) -> Result<Value> {
    if !arguments.offline {
        bail!("release demonstration requires --offline");
    }
    if arguments.repository.head.is_none() || arguments.repository.tree.is_none() {
        bail!("release demonstration requires exact --head and --tree");
    }
    let repository = RepositoryIdentity::admit(&arguments.repository)?;
    let layout = admit_layout(&arguments, &repository)?;
    let provider = read_report(
        &layout.provider_report,
        MAXIMUM_REPORT_BYTES,
        "market_squawk.release.providers",
    )?;
    validate_report_identity(&provider, &repository)?;
    validate_provider_evidence(&provider.payload)?;
    reject_credentials(&provider.payload)?;
    validate_python_evidence(&layout.python_directory)?;

    let executable = std::env::current_exe().context("release executable path is unavailable")?;
    let application_binary = hash_stable_file(&executable, MAXIMUM_EXECUTABLE_BYTES)?;
    validate_provider_binary(&provider.payload, &application_binary)?;
    let python_release_manifest = hash_stable_file(&layout.python_manifest, MAXIMUM_REPORT_BYTES)?;
    let python_release_evidence = hash_stable_file(
        &layout
            .python_directory
            .join("market-squawk-release-evidence.json"),
        MAXIMUM_REPORT_BYTES,
    )?;

    let scratch = tempfile::Builder::new()
        .prefix("market-squawk-release-demonstration-")
        .tempdir()
        .context("release demonstration scratch directory could not be created")?;
    let production_kernels =
        super::benchmark::run_demonstration_kernels(config.clone(), scratch.path()).await?;
    let local_application = local::run(config, scratch.path(), &layout.python_directory).await?;
    drop(scratch);

    let payload = DemonstrationEvidence {
        schema_version: 1,
        repository,
        offline: true,
        inputs: DemonstrationInputs {
            provider_report: provider.file,
            python_release_manifest,
            python_release_evidence,
            application_binary,
        },
        production_kernels,
        local_application,
        completed: true,
        completed_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
    };
    let published =
        publish_report_with_identity_barrier(&arguments.output, REPORT_KIND, &payload, || {
            revalidate_inputs(&layout, &payload, &executable)
        })?;
    Ok(publication_value(&published))
}

fn admit_layout(
    arguments: &ReleaseDemonstrateArguments,
    repository: &RepositoryIdentity,
) -> Result<DemonstrationLayout> {
    reject_parent_traversal(&arguments.provider_evidence)?;
    reject_parent_traversal(&arguments.python_evidence)?;
    reject_parent_traversal(&arguments.output)?;
    let provider_directory =
        real_directory(&arguments.provider_evidence, "provider evidence directory")?;
    if provider_directory
        .file_name()
        .and_then(|name| name.to_str())
        != Some("providers")
    {
        bail!("release demonstration provider evidence must be the providers directory");
    }
    let python_manifest = real_file(
        &arguments.python_evidence,
        "Python release manifest",
        MAXIMUM_REPORT_BYTES,
    )?;
    if python_manifest.file_name().and_then(|name| name.to_str())
        != Some("market-squawk-release.json")
    {
        bail!("release demonstration requires market-squawk-release.json");
    }
    let python_directory = python_manifest
        .parent()
        .context("Python release manifest has no parent")?
        .to_path_buf();
    if python_directory.file_name().and_then(|name| name.to_str()) != Some("python") {
        bail!("release demonstration Python evidence must be the python directory");
    }
    real_file(
        &python_directory.join("market-squawk-release-evidence.json"),
        "Python release evidence",
        MAXIMUM_REPORT_BYTES,
    )?;
    let evidence_root = provider_directory
        .parent()
        .context("provider evidence directory has no parent")?
        .to_path_buf();
    if python_directory.parent() != Some(evidence_root.as_path())
        || evidence_root.file_name().and_then(|name| name.to_str())
            != Some(repository.head.as_str())
    {
        bail!("release demonstration inputs are not one exact-HEAD evidence set");
    }
    let output_parent = arguments
        .output
        .parent()
        .context("release demonstration output has no parent")?
        .canonicalize()
        .context("release demonstration output parent is unavailable")?;
    if output_parent != evidence_root
        || arguments.output.file_name().and_then(|name| name.to_str()) != Some("demo.json")
    {
        bail!("release demonstration output must be the exact evidence root's demo.json");
    }
    Ok(DemonstrationLayout {
        provider_report: provider_directory.join("provider-evidence.json"),
        evidence_root,
        provider_directory,
        python_directory,
        python_manifest,
    })
}

fn revalidate_inputs(
    layout: &DemonstrationLayout,
    payload: &DemonstrationEvidence,
    executable: &Path,
) -> Result<()> {
    if real_directory(&layout.provider_directory, "provider evidence directory")?
        != layout.provider_directory
        || real_directory(&layout.python_directory, "Python evidence directory")?
            != layout.python_directory
        || layout
            .provider_directory
            .parent()
            .is_none_or(|parent| parent != layout.evidence_root)
    {
        bail!("release demonstration evidence layout changed");
    }
    let provider = read_report(
        &layout.provider_report,
        MAXIMUM_REPORT_BYTES,
        "market_squawk.release.providers",
    )?;
    validate_report_identity(&provider, &payload.repository)?;
    validate_provider_evidence(&provider.payload)?;
    reject_credentials(&provider.payload)?;
    if provider.file != payload.inputs.provider_report {
        bail!("release demonstration provider report changed");
    }
    validate_python_evidence(&layout.python_directory)?;
    local::revalidate_training_matrix(&layout.python_directory)?;
    if hash_stable_file(&layout.python_manifest, MAXIMUM_REPORT_BYTES)?
        != payload.inputs.python_release_manifest
        || hash_stable_file(
            &layout
                .python_directory
                .join("market-squawk-release-evidence.json"),
            MAXIMUM_REPORT_BYTES,
        )? != payload.inputs.python_release_evidence
        || hash_stable_file(executable, MAXIMUM_EXECUTABLE_BYTES)?
            != payload.inputs.application_binary
    {
        bail!("release demonstration immutable input changed");
    }
    validate_provider_binary(&provider.payload, &payload.inputs.application_binary)?;
    payload.repository.verify_unchanged()
}

fn reject_parent_traversal(path: &Path) -> Result<()> {
    if path
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        bail!("release demonstration path contains parent traversal");
    }
    Ok(())
}

fn real_directory(path: &Path, label: &str) -> Result<PathBuf> {
    let metadata = fs::symlink_metadata(path).with_context(|| format!("{label} is unavailable"))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!("{label} is not a real directory");
    }
    path.canonicalize()
        .with_context(|| format!("{label} cannot be canonicalized"))
}

fn real_file(path: &Path, label: &str, maximum_bytes: u64) -> Result<PathBuf> {
    let metadata = fs::symlink_metadata(path).with_context(|| format!("{label} is unavailable"))?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() == 0
        || metadata.len() > maximum_bytes
    {
        bail!("{label} is not a bounded real file");
    }
    path.canonicalize()
        .with_context(|| format!("{label} cannot be canonicalized"))
}

fn publication_value(published: &PublishedReport) -> Value {
    serde_json::json!({
        "path": published.path,
        "sha256": published.sha256,
        "byte_count": published.byte_count,
    })
}
