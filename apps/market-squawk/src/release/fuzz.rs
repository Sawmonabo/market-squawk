//! Closed, serial fuzz campaign and its exact machine-readable evidence.

use std::ffi::{OsStr, OsString};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use chrono::{SecondsFormat, Utc};
use serde::Serialize;
use sha2::{Digest as _, Sha256};

use super::identity::RepositoryIdentity;
use super::io::{
    PublishedReport, StableFileIdentity, hash_stable_file, publish_report_with_identity_barrier,
};
use super::process::{ProcessEvidence, ProcessLimits, ProcessRequest};
use crate::cli::ReleaseFuzzArguments;

pub(super) const FUZZ_TOOLCHAIN: &str = "nightly-2026-07-15";
pub(super) const CARGO_FUZZ_VERSION: &str = "cargo-fuzz 0.13.2";
const BUILD_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const MAXIMUM_CAMPAIGN_SECONDS: u64 = 60 * 60;
const MINIMUM_RSS_MIB: u64 = 64;
const MAXIMUM_RSS_MIB: u64 = 8 * 1024;
pub(super) const MAXIMUM_FUZZ_TARGET_BYTES: u64 = 10 * 1024 * 1024 * 1024;
const MAXIMUM_EXECUTABLE_BYTES: u64 = 1024 * 1024 * 1024;
const MAXIMUM_INPUT_FILE_BYTES: u64 = 1024 * 1024;
pub(super) const MAXIMUM_CORPUS_BYTES: u64 = 256 * 1024 * 1024;
pub(super) const MAXIMUM_CORPUS_FILES: usize = 100_000;

pub(super) const TARGETS: [FuzzTarget; 6] = [
    FuzzTarget {
        name: "capture_records",
        feature: "capture",
        maximum_input_bytes: 1024 * 1024,
    },
    FuzzTarget {
        name: "coinbase_decoder",
        feature: "coinbase",
        maximum_input_bytes: 256 * 1024,
    },
    FuzzTarget {
        name: "kraken_decoder",
        feature: "kraken",
        maximum_input_bytes: 1024 * 1024,
    },
    FuzzTarget {
        name: "mcp_requests",
        feature: "mcp",
        maximum_input_bytes: 1024 * 1024,
    },
    FuzzTarget {
        name: "model_artifacts",
        feature: "model",
        maximum_input_bytes: 1024 * 1024,
    },
    FuzzTarget {
        name: "research_document_parsers",
        feature: "research",
        maximum_input_bytes: 1024 * 1024,
    },
];

#[derive(Clone, Copy)]
pub(super) struct FuzzTarget {
    pub(super) name: &'static str,
    pub(super) feature: &'static str,
    pub(super) maximum_input_bytes: usize,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct FuzzEvidence {
    repository: RepositoryIdentity,
    evidence_authority: &'static str,
    application_binary: StableFileIdentity,
    fuzz_manifest: StableFileIdentity,
    fuzz_lock: StableFileIdentity,
    fuzz_toolchain_file: StableFileIdentity,
    application_version: &'static str,
    host_os: &'static str,
    host_arch: &'static str,
    toolchain: String,
    rustc_version: String,
    cargo_fuzz_version: String,
    sanitizer: &'static str,
    seconds_per_target: u64,
    rss_limit_bytes: u64,
    target_directory_limit_bytes: u64,
    started_at: String,
    completed_at: String,
    targets: Vec<FuzzTargetEvidence>,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct FuzzTargetEvidence {
    name: &'static str,
    feature: &'static str,
    maximum_input_bytes: usize,
    initial_corpus_sha256: String,
    final_corpus_sha256: String,
    final_corpus_files: usize,
    final_corpus_bytes: u64,
    build: ProcessEvidence,
    campaign: ProcessEvidence,
    target_directory_bytes_after_campaign: u64,
}

pub(super) fn run(arguments: ReleaseFuzzArguments) -> Result<serde_json::Value> {
    validate_arguments(&arguments)?;
    let evidence_authority =
        if arguments.repository.head.is_some() && arguments.repository.tree.is_some() {
            "exact_head"
        } else {
            "provisional"
        };
    let repository = RepositoryIdentity::admit(&arguments.repository)?;
    let started_at = now();
    let executable = std::env::current_exe().context("running executable path is unavailable")?;
    let application_binary = hash_stable_file(&executable, MAXIMUM_EXECUTABLE_BYTES)?;
    let fuzz_root = repository.root().join("fuzz");
    let manifest = hash_stable_file(&fuzz_root.join("Cargo.toml"), MAXIMUM_INPUT_FILE_BYTES)?;
    let lock = hash_stable_file(&fuzz_root.join("Cargo.lock"), MAXIMUM_INPUT_FILE_BYTES)?;
    let toolchain_file = hash_stable_file(
        &fuzz_root.join("rust-toolchain.toml"),
        MAXIMUM_INPUT_FILE_BYTES,
    )?;
    let rss_limit_bytes = arguments
        .rss_limit_mib
        .checked_mul(1024 * 1024)
        .ok_or_else(|| anyhow::anyhow!("fuzz RSS limit overflow"))?;
    let rustc = tool_version(
        repository.root(),
        &[
            OsString::from("run"),
            OsString::from(&arguments.toolchain),
            OsString::from("rustc"),
            OsString::from("--version"),
            OsString::from("--verbose"),
        ],
        OsStr::new("rustup"),
        rss_limit_bytes,
    )?;
    let cargo_fuzz = tool_version(
        repository.root(),
        &[
            OsString::from(format!("+{}", arguments.toolchain)),
            OsString::from("fuzz"),
            OsString::from("--version"),
        ],
        OsStr::new("cargo"),
        rss_limit_bytes,
    )?;
    if cargo_fuzz.trim() != CARGO_FUZZ_VERSION {
        bail!("release fuzz evidence requires {CARGO_FUZZ_VERSION}");
    }
    let target_root = prepare_campaign_root(&fuzz_root, &repository)?;
    let environment = vec![(OsString::from("CARGO_NET_OFFLINE"), OsString::from("true"))];
    let mut targets = Vec::new();
    targets
        .try_reserve_exact(TARGETS.len())
        .context("fuzz evidence allocation failed")?;
    for target in TARGETS {
        let campaign_root = prepare_target_root(&target_root, target.name)?;
        let corpus = campaign_root.join("corpus");
        let artifacts = campaign_root.join("artifacts");
        let initial_corpus = tree_identity(&corpus)?;
        let build_arguments = cargo_fuzz_arguments(
            &arguments.toolchain,
            "build",
            &fuzz_root,
            &fuzz_root.join("target"),
            target,
        );
        let build = super::process::run(ProcessRequest {
            program: OsStr::new("cargo"),
            arguments: &build_arguments,
            current_dir: repository.root(),
            environment: &environment,
            limits: ProcessLimits {
                timeout: BUILD_TIMEOUT,
                rss_bytes: rss_limit_bytes,
            },
        })?
        .evidence;
        enforce_target_directory_limit(&fuzz_root.join("target"))?;
        let mut run_arguments = cargo_fuzz_arguments(
            &arguments.toolchain,
            "run",
            &fuzz_root,
            &fuzz_root.join("target"),
            target,
        );
        run_arguments.push(OsString::from(corpus.as_os_str()));
        run_arguments.push(OsString::from("--"));
        run_arguments.push(OsString::from(format!(
            "-max_total_time={}",
            arguments.seconds_per_target
        )));
        run_arguments.push(OsString::from(format!(
            "-rss_limit_mb={}",
            arguments.rss_limit_mib
        )));
        run_arguments.push(OsString::from("-timeout=5"));
        run_arguments.push(OsString::from(format!(
            "-max_len={}",
            target.maximum_input_bytes
        )));
        run_arguments.push(OsString::from(format!(
            "-artifact_prefix={}/",
            artifacts.display()
        )));
        let run_timeout = Duration::from_secs(
            arguments
                .seconds_per_target
                .checked_add(60)
                .ok_or_else(|| anyhow::anyhow!("fuzz campaign timeout overflow"))?,
        );
        let campaign = super::process::run(ProcessRequest {
            program: OsStr::new("cargo"),
            arguments: &run_arguments,
            current_dir: repository.root(),
            environment: &environment,
            limits: ProcessLimits {
                timeout: run_timeout,
                rss_bytes: rss_limit_bytes,
            },
        })?
        .evidence;
        if directory_has_entries(&artifacts)? {
            bail!("fuzz target {} produced a crash artifact", target.name);
        }
        let final_corpus = tree_identity(&corpus)?;
        let target_directory_bytes = enforce_target_directory_limit(&fuzz_root.join("target"))?;
        targets.push(FuzzTargetEvidence {
            name: target.name,
            feature: target.feature,
            maximum_input_bytes: target.maximum_input_bytes,
            initial_corpus_sha256: initial_corpus.sha256,
            final_corpus_sha256: final_corpus.sha256,
            final_corpus_files: final_corpus.files,
            final_corpus_bytes: final_corpus.bytes,
            build,
            campaign,
            target_directory_bytes_after_campaign: target_directory_bytes,
        });
    }
    repository.verify_unchanged()?;
    let payload = FuzzEvidence {
        repository,
        evidence_authority,
        application_binary,
        fuzz_manifest: manifest,
        fuzz_lock: lock,
        fuzz_toolchain_file: toolchain_file,
        application_version: env!("CARGO_PKG_VERSION"),
        host_os: std::env::consts::OS,
        host_arch: std::env::consts::ARCH,
        toolchain: arguments.toolchain,
        rustc_version: rustc,
        cargo_fuzz_version: cargo_fuzz,
        sanitizer: "address",
        seconds_per_target: arguments.seconds_per_target,
        rss_limit_bytes,
        target_directory_limit_bytes: MAXIMUM_FUZZ_TARGET_BYTES,
        started_at,
        completed_at: now(),
        targets,
    };
    let published = publish_report_with_identity_barrier(
        &arguments.output,
        "market_squawk.release.fuzz",
        &payload,
        || {
            if hash_stable_file(&executable, MAXIMUM_EXECUTABLE_BYTES)?
                != payload.application_binary
                || hash_stable_file(&fuzz_root.join("Cargo.toml"), MAXIMUM_INPUT_FILE_BYTES)?
                    != payload.fuzz_manifest
                || hash_stable_file(&fuzz_root.join("Cargo.lock"), MAXIMUM_INPUT_FILE_BYTES)?
                    != payload.fuzz_lock
                || hash_stable_file(
                    &fuzz_root.join("rust-toolchain.toml"),
                    MAXIMUM_INPUT_FILE_BYTES,
                )? != payload.fuzz_toolchain_file
            {
                bail!("release fuzz immutable inputs changed at the publication barrier");
            }
            payload.repository.verify_unchanged()
        },
    )?;
    Ok(publication_value(&published))
}

fn validate_arguments(arguments: &ReleaseFuzzArguments) -> Result<()> {
    if arguments.toolchain != FUZZ_TOOLCHAIN {
        bail!("release fuzz evidence requires toolchain {FUZZ_TOOLCHAIN}");
    }
    if arguments.seconds_per_target == 0 || arguments.seconds_per_target > MAXIMUM_CAMPAIGN_SECONDS
    {
        bail!("fuzz seconds-per-target is outside its fixed bound");
    }
    if !(MINIMUM_RSS_MIB..=MAXIMUM_RSS_MIB).contains(&arguments.rss_limit_mib) {
        bail!("fuzz RSS limit is outside its fixed bound");
    }
    if arguments.repository.head.is_some()
        && (arguments.seconds_per_target != 120 || arguments.rss_limit_mib != 2_048)
    {
        bail!("exact-head fuzz evidence requires the full campaign duration and RSS limit");
    }
    Ok(())
}

fn cargo_fuzz_arguments(
    toolchain: &str,
    operation: &str,
    fuzz_root: &Path,
    target_root: &Path,
    target: FuzzTarget,
) -> Vec<OsString> {
    vec![
        OsString::from(format!("+{toolchain}")),
        OsString::from("fuzz"),
        OsString::from(operation),
        OsString::from("--fuzz-dir"),
        OsString::from(fuzz_root.as_os_str()),
        OsString::from("--target-dir"),
        OsString::from(target_root.as_os_str()),
        OsString::from("--features"),
        OsString::from(target.feature),
        OsString::from("--sanitizer"),
        OsString::from("address"),
        OsString::from("--codegen-units"),
        OsString::from("16"),
        OsString::from(target.name),
    ]
}

fn tool_version(
    root: &Path,
    arguments: &[OsString],
    program: &OsStr,
    rss_limit_bytes: u64,
) -> Result<String> {
    let output = super::process::run(ProcessRequest {
        program,
        arguments,
        current_dir: root,
        environment: &[],
        limits: ProcessLimits {
            timeout: Duration::from_secs(15),
            rss_bytes: rss_limit_bytes,
        },
    })?;
    if output.evidence.stdout_truncated {
        bail!("tool version output exceeded its fixed bound");
    }
    let version =
        String::from_utf8(output.stdout).context("tool version output is not valid UTF-8")?;
    if version.trim().is_empty() {
        bail!("tool version output is empty");
    }
    Ok(version.trim().to_owned())
}

fn prepare_campaign_root(fuzz_root: &Path, repository: &RepositoryIdentity) -> Result<PathBuf> {
    let target = prepare_directory(fuzz_root, "target", false)?;
    let campaigns = prepare_directory(&target, "release-campaigns", false)?;
    let identity = if repository.clean {
        repository.head.as_str()
    } else {
        "provisional"
    };
    prepare_directory(&campaigns, identity, true)
}

fn prepare_target_root(root: &Path, target: &str) -> Result<PathBuf> {
    let directory = prepare_directory(root, target, true)?;
    let _corpus = prepare_directory(&directory, "corpus", true)?;
    let _artifacts = prepare_directory(&directory, "artifacts", true)?;
    Ok(directory)
}

fn prepare_directory(parent: &Path, name: &str, require_new: bool) -> Result<PathBuf> {
    if name.is_empty()
        || name == "."
        || name == ".."
        || name.contains(['/', '\\'])
        || !name.is_ascii()
    {
        bail!("generated fuzz directory has an invalid component");
    }
    let directory = parent.join(name);
    match fs::symlink_metadata(&directory) {
        Ok(_) if require_new => bail!("generated fuzz campaign directory already exists"),
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
        Ok(_) => bail!("generated fuzz path is not a real directory"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(&directory).context("generated fuzz directory creation failed")?;
        }
        Err(error) => return Err(error).context("generated fuzz directory metadata failed"),
    }
    Ok(directory)
}

fn directory_has_entries(directory: &Path) -> Result<bool> {
    Ok(fs::read_dir(directory)
        .context("fuzz artifact directory cannot be read")?
        .next()
        .transpose()
        .context("fuzz artifact directory entry cannot be read")?
        .is_some())
}

fn enforce_target_directory_limit(directory: &Path) -> Result<u64> {
    let identity = bounded_tree_identity(directory, MAXIMUM_FUZZ_TARGET_BYTES, 2_000_000, false)?;
    if identity.bytes > MAXIMUM_FUZZ_TARGET_BYTES {
        bail!("fuzz target directory exceeded its fixed disk limit");
    }
    Ok(identity.bytes)
}

fn tree_identity(directory: &Path) -> Result<TreeIdentity> {
    bounded_tree_identity(directory, MAXIMUM_CORPUS_BYTES, MAXIMUM_CORPUS_FILES, true)
}

fn bounded_tree_identity(
    directory: &Path,
    maximum_bytes: u64,
    maximum_files: usize,
    hash_contents: bool,
) -> Result<TreeIdentity> {
    let root = directory
        .canonicalize()
        .context("bounded tree root cannot be canonicalized")?;
    let mut pending = vec![root.clone()];
    let mut files = Vec::new();
    while let Some(current) = pending.pop() {
        for entry in fs::read_dir(&current).context("bounded tree cannot be read")? {
            let entry = entry.context("bounded tree entry cannot be read")?;
            let metadata = entry
                .file_type()
                .context("bounded tree entry type is unavailable")?;
            if metadata.is_symlink() {
                bail!("bounded tree contains a symbolic link");
            }
            if metadata.is_dir() {
                pending.push(entry.path());
            } else if metadata.is_file() {
                let relative = entry
                    .path()
                    .strip_prefix(&root)
                    .context("bounded tree path escaped its root")?
                    .to_path_buf();
                files.push(relative);
                if files.len() > maximum_files {
                    bail!("bounded tree exceeded its file-count limit");
                }
            } else {
                bail!("bounded tree contains an unsupported file type");
            }
        }
    }
    files.sort();
    let mut hasher = Sha256::new();
    let mut bytes = 0_u64;
    for relative in &files {
        let name = relative
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("bounded tree contains a non-UTF-8 path"))?;
        hasher.update(u64::try_from(name.len())?.to_le_bytes());
        hasher.update(name.as_bytes());
        let metadata = fs::metadata(root.join(relative)).context("bounded tree metadata failed")?;
        bytes = bytes
            .checked_add(metadata.len())
            .ok_or_else(|| anyhow::anyhow!("bounded tree size overflow"))?;
        if bytes > maximum_bytes {
            bail!("bounded tree exceeded its byte limit");
        }
        hasher.update(metadata.len().to_le_bytes());
        if hash_contents {
            let file = hash_stable_file(&root.join(relative), maximum_bytes)?;
            hasher.update(file.sha256.as_bytes());
        }
    }
    Ok(TreeIdentity {
        sha256: hex_digest(hasher.finalize().into()),
        files: files.len(),
        bytes,
    })
}

fn hex_digest(digest: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(64);
    for byte in digest {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    value
}

fn publication_value(published: &PublishedReport) -> serde_json::Value {
    serde_json::json!({
        "path": published.path,
        "sha256": published.sha256,
        "byte_count": published.byte_count,
    })
}

fn now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

struct TreeIdentity {
    sha256: String,
    files: usize,
    bytes: u64,
}
