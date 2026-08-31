//! Compile-time source and tool-input bindings for capture benchmark evidence.

use std::error::Error;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use sha2::{Digest, Sha256};

mod build_support;

const MAX_BUILD_INPUT_BYTES: u64 = 64 * 1024 * 1024;
const MAX_SOURCE_ENTRIES: usize = 20_000;
const MAX_SOURCE_DEPTH: usize = 32;
const MAX_SOURCE_FILE_BYTES: u64 = 4 * 1024 * 1024;
const MAX_SOURCE_INVENTORY_BYTES: u64 = 64 * 1024 * 1024;
const IMMUTABLE_MODULES: [(&str, &str); 8] = [
    ("benchmark_identity", "benchmark_identity.rs"),
    ("collector", "collector.rs"),
    ("endpoints", "endpoints.rs"),
    ("evidence_io", "evidence_io.rs"),
    ("fixture", "fixture.rs"),
    ("producer_inventory", "producer_inventory.rs"),
    ("schema", "schema.rs"),
    ("workload", "workload.rs"),
];

const BUILD_SUPPORT_TREE: [&str; 3] = [
    "build_support.rs",
    "build_support/filesystem.rs",
    "build_support/reader.rs",
];
const BUILD_SUPPORT_TREE_DOMAIN: &[u8] = b"market-squawk/capture-build-support-tree/v1\0";
const MEASURED_SOURCE_CLOSURE_DOMAIN: &[u8] = b"market-squawk/capture-measured-source-closure/v1\0";

fn main() -> Result<(), Box<dyn Error>> {
    println!(
        "cargo:rustc-check-cfg=cfg(capture_bench_backend, values(\"standard\", \"candidate\"))"
    );
    println!("cargo:rustc-check-cfg=cfg(capture_bench_authoritative)");
    println!("cargo:rerun-if-env-changed=CAPTURE_BENCH_REQUIRE_CLEAN_BUILD");
    println!("cargo:rerun-if-env-changed=CAPTURE_BENCH_BUILD_POLICY");
    println!("cargo:rerun-if-env-changed=CAPTURE_BENCH_BUILD_COMMAND_SHA256");
    println!("cargo:rerun-if-env-changed=CAPTURE_BENCH_BUILD_ENV_SHA256");
    println!("cargo:rerun-if-env-changed=CAPTURE_BENCH_EVIDENCE_BACKEND");
    println!("cargo:rerun-if-env-changed=CAPTURE_BENCH_DEVELOPMENT_BACKEND");
    println!("cargo:rerun-if-env-changed=CAPTURE_BENCH_CARGO_EXECUTABLE");
    println!("cargo:rerun-if-env-changed=CAPTURE_BENCH_CARGO_EXECUTABLE_SHA256");
    println!("cargo:rerun-if-env-changed=CAPTURE_BENCH_GIT_EXECUTABLE");
    println!("cargo:rerun-if-env-changed=CAPTURE_BENCH_GIT_EXECUTABLE_SHA256");
    println!("cargo:rerun-if-env-changed=CAPTURE_BENCH_RUSTC_EXECUTABLE");
    println!("cargo:rerun-if-env-changed=CAPTURE_BENCH_RUSTC_EXECUTABLE_SHA256");
    println!("cargo:rerun-if-env-changed=RUSTC");
    println!("cargo:rerun-if-env-changed=CAPTURE_BENCH_PROCESS_GROUP_POLICY");
    println!("cargo:rerun-if-env-changed=CAPTURE_BENCH_EXPECTED_PROCESS_GROUP_ID");
    println!("cargo:rerun-if-env-changed=CAPTURE_BENCH_BASELINE_LOCK_SHA256");
    println!("cargo:rerun-if-env-changed=CAPTURE_BENCH_BASELINE_MANIFEST_SHA256");
    println!("cargo:rerun-if-env-changed=CAPTURE_BENCH_BASELINE_MEASURED_CODE_HEAD");
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_CAPTURE_BENCHMARK");
    match std::env::var("CARGO_FEATURE_CAPTURE_BENCHMARK") {
        Ok(value) if value == "1" => {}
        Ok(_invalid) => return Err("capture benchmark feature marker is invalid".into()),
        Err(std::env::VarError::NotPresent) => return Ok(()),
        Err(error) => return Err(error.into()),
    }
    let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let repository = manifest
        .parent()
        .and_then(Path::parent)
        .ok_or("platform manifest is not under the repository root")?;
    let clean_build_requested = std::env::var_os("CAPTURE_BENCH_REQUIRE_CLEAN_BUILD").is_some();
    let configured_backend = match std::env::var("CAPTURE_BENCH_EVIDENCE_BACKEND") {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => return Err(error.into()),
    };
    let development_backend = match std::env::var("CAPTURE_BENCH_DEVELOPMENT_BACKEND") {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => return Err(error.into()),
    };
    let selected_backend = build_support::select_benchmark_backend(
        clean_build_requested,
        configured_backend.as_deref(),
        development_backend.as_deref(),
    )?;
    println!(
        "cargo:rustc-cfg=capture_bench_backend={:?}",
        selected_backend.as_str()
    );
    if clean_build_requested {
        println!("cargo:rustc-cfg=capture_bench_authoritative");
        let configured = std::env::var("CAPTURE_BENCH_PROCESS_GROUP_POLICY").ok();
        let expected_group = std::env::var("CAPTURE_BENCH_EXPECTED_PROCESS_GROUP_ID").ok();
        let _policy = build_support::authoritative_command_policy(
            configured.as_deref(),
            expected_group.as_deref(),
        )?;
    }
    let (git_executable, git_executable_sha256, cargo_executable_sha256, rustc_executable_sha256) =
        if clean_build_requested {
            let git = absolute_bound_tool(
                "CAPTURE_BENCH_GIT_EXECUTABLE",
                "CAPTURE_BENCH_GIT_EXECUTABLE_SHA256",
            )?;
            let cargo = absolute_bound_tool(
                "CAPTURE_BENCH_CARGO_EXECUTABLE",
                "CAPTURE_BENCH_CARGO_EXECUTABLE_SHA256",
            )?;
            let rustc = absolute_bound_tool(
                "CAPTURE_BENCH_RUSTC_EXECUTABLE",
                "CAPTURE_BENCH_RUSTC_EXECUTABLE_SHA256",
            )?;
            let cargo_rustc = std::env::var_os("RUSTC")
                .ok_or("Cargo did not provide the selected Rust compiler identity")?;
            if Path::new(&cargo_rustc) != rustc.0.as_path() {
                return Err("Cargo did not preserve the bound Rust compiler identity".into());
            }
            (git.0, git.1, cargo.1, rustc.1)
        } else {
            (
                PathBuf::from("git"),
                "0".repeat(64),
                "0".repeat(64),
                "0".repeat(64),
            )
        };
    let bench_root = manifest.join("benches/capture_admission");
    let backend_binding = build_support::bind_benchmark_backend_sources(
        &manifest,
        selected_backend,
        MAX_SOURCE_FILE_BYTES,
    )?;
    if backend_binding.backend() != selected_backend {
        return Err("backend source binding selected a different identity".into());
    }
    let selected_backend_source_path = format!(
        "crates/market-squawk-platform/{}",
        backend_binding.selected_source_relative_path()
    );
    let mut module_hashes = Vec::new();
    for (name, file) in IMMUTABLE_MODULES {
        let path = bench_root.join(file);
        rerun(&path);
        module_hashes.push((name, hash_file(&path)?));
    }
    let entrypoint = manifest.join("benches/capture_admission.rs");
    let backend = bench_root.join("backend.rs");
    let standard_backend = bench_root.join("backend/standard.rs");
    let candidate_backend = bench_root.join("backend/candidate.rs");
    let criterion = manifest.join("benches/capture_admission_criterion.rs");
    let observer = manifest.join("src/capture/benchmark_support/observer.rs");
    for path in [
        &entrypoint,
        &backend,
        &standard_backend,
        &candidate_backend,
        &criterion,
        &observer,
    ] {
        rerun(path);
    }
    let platform_sources = collect_rust_files(&manifest.join("src"))?;
    let domain_sources = collect_rust_files(&repository.join("crates/market-squawk-domain/src"))?;
    for source in platform_sources.iter().chain(&domain_sources) {
        rerun(&source.path);
    }
    let cargo_lock = repository.join("Cargo.lock");
    let workspace_manifest = repository.join("Cargo.toml");
    let rust_toolchain = repository.join("rust-toolchain.toml");
    let package_manifest = manifest.join("Cargo.toml");
    let domain_manifest = repository.join("crates/market-squawk-domain/Cargo.toml");
    let build_script = manifest.join("build.rs");
    let build_support_paths = BUILD_SUPPORT_TREE.map(|relative| manifest.join(relative));
    for path in [
        &cargo_lock,
        &workspace_manifest,
        &rust_toolchain,
        &package_manifest,
        &domain_manifest,
        &build_script,
    ] {
        rerun(path);
    }
    for path in &build_support_paths {
        rerun(path);
    }
    let clean_build_enforced = match std::env::var("CAPTURE_BENCH_REQUIRE_CLEAN_BUILD") {
        Ok(value) if value == "1" => {
            if !command(
                &git_executable,
                repository,
                &["status", "--porcelain=v1", "--untracked-files=all"],
            )?
            .is_empty()
            {
                return Err("benchmark build requires a clean Git tree".into());
            }
            true
        }
        Ok(_value) => return Err("CAPTURE_BENCH_REQUIRE_CLEAN_BUILD must equal 1 when set".into()),
        Err(std::env::VarError::NotPresent) => false,
        Err(error) => return Err(error.into()),
    };
    // Repository commit/tree identity belongs to the external evidence envelope. Keeping Git refs
    // out of this compilation preserves the content-addressed source bindings below across commits
    // that change only documentation or unrelated verification scripts.
    let build_environment = if clean_build_enforced {
        validate_authoritative_build_environment()?
    } else {
        ValidatedBuildEnvironment {
            policy: "development-unverified".to_owned(),
            command_sha256: "0".repeat(64),
            environment_sha256: "0".repeat(64),
            evidence_backend: selected_backend.as_str().to_owned(),
            baseline_lock_sha256: None,
            baseline_manifest_sha256: None,
            baseline_measured_code_head: None,
        }
    };
    if build_environment.evidence_backend != selected_backend.as_str() {
        return Err("compiled backend differs from its build-environment identity".into());
    }
    let source_inventory =
        inventory_hash(repository, platform_sources.iter().chain(&domain_sources))?;
    let baseline_lock_path = repository
        .join("docs/reports/performance/2026-07-17-q2-a4-standard-channel-baseline.lock.json");
    if let Some(expected) = build_environment.baseline_lock_sha256.as_deref() {
        rerun(&baseline_lock_path);
        if hash_file(&baseline_lock_path)? != expected {
            return Err("tracked baseline lock differs from its build binding".into());
        }
    }
    let entrypoint_sha256 = hash_file(&entrypoint)?;
    let criterion_sha256 = hash_file(&criterion)?;
    let observer_sha256 = hash_file(&observer)?;
    let platform_source_sha256 = tree_hash(repository, &platform_sources)?;
    let domain_source_sha256 = tree_hash(repository, &domain_sources)?;
    let cargo_lock_sha256 = hash_file(&cargo_lock)?;
    let workspace_manifest_sha256 = hash_file(&workspace_manifest)?;
    let rust_toolchain_sha256 = hash_file(&rust_toolchain)?;
    let package_manifest_sha256 = hash_file(&package_manifest)?;
    let domain_manifest_sha256 = hash_file(&domain_manifest)?;
    let build_script_sha256 = hash_file(&build_script)?;
    let build_support_tree_sha256 = build_support_tree_hash(&manifest, &build_support_paths)?;
    let mut measured_source_components = vec![
        ("Cargo.lock".to_owned(), cargo_lock_sha256.clone()),
        ("Cargo.toml".to_owned(), workspace_manifest_sha256.clone()),
        (
            "rust-toolchain.toml".to_owned(),
            rust_toolchain_sha256.clone(),
        ),
        (
            "crates/market-squawk-platform/Cargo.toml".to_owned(),
            package_manifest_sha256.clone(),
        ),
        (
            "crates/market-squawk-domain/Cargo.toml".to_owned(),
            domain_manifest_sha256.clone(),
        ),
        (
            "crates/market-squawk-platform/build.rs".to_owned(),
            build_script_sha256.clone(),
        ),
        (
            "crates/market-squawk-platform/build-support-tree-v1".to_owned(),
            build_support_tree_sha256.clone(),
        ),
        (
            "crates/market-squawk-platform/src/**/*.rs".to_owned(),
            platform_source_sha256.clone(),
        ),
        (
            "crates/market-squawk-domain/src/**/*.rs".to_owned(),
            domain_source_sha256.clone(),
        ),
        ("rust-source-inventory".to_owned(), source_inventory.clone()),
        (
            "crates/market-squawk-platform/benches/capture_admission.rs".to_owned(),
            entrypoint_sha256.clone(),
        ),
        (
            "crates/market-squawk-platform/benches/capture_admission/backend.rs".to_owned(),
            backend_binding.dispatcher_sha256().to_owned(),
        ),
        (
            "crates/market-squawk-platform/benches/capture_admission/backend/standard.rs"
                .to_owned(),
            backend_binding.standard_source_sha256().to_owned(),
        ),
        (
            "crates/market-squawk-platform/benches/capture_admission/backend/candidate.rs"
                .to_owned(),
            backend_binding.candidate_source_sha256().to_owned(),
        ),
        (
            "crates/market-squawk-platform/benches/capture_admission_criterion.rs".to_owned(),
            criterion_sha256.clone(),
        ),
        (
            "crates/market-squawk-platform/src/capture/benchmark_support/observer.rs".to_owned(),
            observer_sha256.clone(),
        ),
        (
            "capture-benchmark-backend".to_owned(),
            selected_backend.as_str().to_owned(),
        ),
    ];
    for ((_, file), (_, digest)) in IMMUTABLE_MODULES.iter().zip(&module_hashes) {
        measured_source_components.push((
            format!("crates/market-squawk-platform/benches/capture_admission/{file}"),
            digest.clone(),
        ));
    }
    for (name, value) in [
        (
            "baseline-lock-sha256",
            build_environment.baseline_lock_sha256.as_deref(),
        ),
        (
            "baseline-manifest-sha256",
            build_environment.baseline_manifest_sha256.as_deref(),
        ),
        (
            "baseline-measured-code-head",
            build_environment.baseline_measured_code_head.as_deref(),
        ),
    ] {
        if let Some(value) = value {
            measured_source_components.push((name.to_owned(), value.to_owned()));
        }
    }
    let measured_source_closure_sha256 =
        measured_source_closure_hash(&mut measured_source_components)?;
    let generated = render(GeneratedBindings {
        clean_build_enforced,
        build_environment_policy: &build_environment.policy,
        build_command_sha256: &build_environment.command_sha256,
        build_environment_sha256: &build_environment.environment_sha256,
        evidence_backend: &build_environment.evidence_backend,
        baseline_lock_sha256: build_environment.baseline_lock_sha256.as_deref(),
        baseline_manifest_sha256: build_environment.baseline_manifest_sha256.as_deref(),
        baseline_measured_code_head: build_environment.baseline_measured_code_head.as_deref(),
        measured_source_closure_sha256: &measured_source_closure_sha256,
        module_hashes: &module_hashes,
        entrypoint_sha256: &entrypoint_sha256,
        backend_dispatcher_sha256: backend_binding.dispatcher_sha256(),
        selected_backend_source_path: &selected_backend_source_path,
        selected_backend_source_sha256: backend_binding.selected_source_sha256(),
        backend_sha256: backend_binding.backend_sha256(),
        criterion_sha256: &criterion_sha256,
        observer_sha256: &observer_sha256,
        platform_source_sha256: &platform_source_sha256,
        domain_source_sha256: &domain_source_sha256,
        source_inventory_sha256: &source_inventory,
        cargo_lock_sha256: &cargo_lock_sha256,
        workspace_manifest_sha256: &workspace_manifest_sha256,
        package_manifest_sha256: &package_manifest_sha256,
        build_script_sha256: &build_script_sha256,
        build_support_tree_sha256: &build_support_tree_sha256,
        cargo_executable_sha256: &cargo_executable_sha256,
        git_executable_sha256: &git_executable_sha256,
        rustc_executable_sha256: &rustc_executable_sha256,
    })?;
    let output = PathBuf::from(std::env::var("OUT_DIR")?).join("capture_benchmark_bindings.rs");
    fs::write(output, generated)?;
    Ok(())
}

#[derive(Debug)]
struct ValidatedBuildEnvironment {
    policy: String,
    command_sha256: String,
    environment_sha256: String,
    evidence_backend: String,
    baseline_lock_sha256: Option<String>,
    baseline_manifest_sha256: Option<String>,
    baseline_measured_code_head: Option<String>,
}

fn validate_authoritative_build_environment() -> Result<ValidatedBuildEnvironment, Box<dyn Error>> {
    const POLICY: &str = "sanitized-cargo-release-runner-v3";
    for key in [
        "RUSTFLAGS",
        "RUSTDOCFLAGS",
        "CARGO_BUILD_RUSTFLAGS",
        "CARGO_BUILD_RUSTC_WRAPPER",
        "RUSTC_WRAPPER",
        "RUSTC_WORKSPACE_WRAPPER",
        "CC",
        "CXX",
        "AR",
    ] {
        if std::env::var_os(key).is_some() {
            return Err("authoritative benchmark build contains a compiler override".into());
        }
    }
    if std::env::vars_os().any(|(key, _value)| {
        key.to_str().is_some_and(|key| {
            key.starts_with("CARGO_PROFILE_") || key.starts_with("CARGO_TARGET_")
        })
    }) {
        return Err(
            "authoritative benchmark build contains a Cargo profile or target override".into(),
        );
    }
    if std::env::var("CAPTURE_BENCH_BUILD_POLICY")?.as_str() != POLICY
        || std::env::var("PROFILE")?.as_str() != "release"
        || std::env::var("OPT_LEVEL")?.as_str() != "3"
        || std::env::var("DEBUG")?.as_str() != "false"
        // Cargo forces build scripts to unwind even when the selected target uses `panic=abort`.
        // The evidence binary's target-side compile guard is authoritative for its panic strategy.
        || std::env::var("CARGO_CFG_PANIC")?.as_str() != "unwind"
        || std::env::var("CARGO_FEATURE_CAPTURE_BENCHMARK")?.as_str() != "1"
        || !std::env::var("CARGO_ENCODED_RUSTFLAGS")?.is_empty()
    {
        return Err("authoritative benchmark build profile or feature contract is invalid".into());
    }
    let command_sha256 = std::env::var("CAPTURE_BENCH_BUILD_COMMAND_SHA256")?;
    let environment_sha256 = std::env::var("CAPTURE_BENCH_BUILD_ENV_SHA256")?;
    if !is_lower_digest(&command_sha256) || !is_lower_digest(&environment_sha256) {
        return Err("authoritative benchmark build binding digest is invalid".into());
    }
    let evidence_backend = std::env::var("CAPTURE_BENCH_EVIDENCE_BACKEND")?;
    let baseline_lock_sha256 = std::env::var("CAPTURE_BENCH_BASELINE_LOCK_SHA256").ok();
    let baseline_manifest_sha256 = std::env::var("CAPTURE_BENCH_BASELINE_MANIFEST_SHA256").ok();
    let baseline_measured_code_head =
        std::env::var("CAPTURE_BENCH_BASELINE_MEASURED_CODE_HEAD").ok();
    let valid_backend = match evidence_backend.as_str() {
        "standard" => {
            baseline_lock_sha256.is_none()
                && baseline_manifest_sha256.is_none()
                && baseline_measured_code_head.is_none()
        }
        "candidate" => {
            baseline_lock_sha256.as_deref().is_some_and(is_lower_digest)
                && baseline_manifest_sha256
                    .as_deref()
                    .is_some_and(is_lower_digest)
                && baseline_measured_code_head
                    .as_deref()
                    .is_some_and(is_lower_git_head)
        }
        _ => false,
    };
    if !valid_backend {
        return Err("authoritative benchmark backend or baseline binding is invalid".into());
    }
    Ok(ValidatedBuildEnvironment {
        policy: POLICY.to_owned(),
        command_sha256,
        environment_sha256,
        evidence_backend,
        baseline_lock_sha256,
        baseline_manifest_sha256,
        baseline_measured_code_head,
    })
}

fn is_lower_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_lower_git_head(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

struct GeneratedBindings<'a> {
    clean_build_enforced: bool,
    build_environment_policy: &'a str,
    build_command_sha256: &'a str,
    build_environment_sha256: &'a str,
    evidence_backend: &'a str,
    baseline_lock_sha256: Option<&'a str>,
    baseline_manifest_sha256: Option<&'a str>,
    baseline_measured_code_head: Option<&'a str>,
    measured_source_closure_sha256: &'a str,
    module_hashes: &'a [(&'a str, String)],
    entrypoint_sha256: &'a str,
    backend_dispatcher_sha256: &'a str,
    selected_backend_source_path: &'a str,
    selected_backend_source_sha256: &'a str,
    backend_sha256: &'a str,
    criterion_sha256: &'a str,
    observer_sha256: &'a str,
    platform_source_sha256: &'a str,
    domain_source_sha256: &'a str,
    source_inventory_sha256: &'a str,
    cargo_lock_sha256: &'a str,
    workspace_manifest_sha256: &'a str,
    package_manifest_sha256: &'a str,
    build_script_sha256: &'a str,
    build_support_tree_sha256: &'a str,
    cargo_executable_sha256: &'a str,
    git_executable_sha256: &'a str,
    rustc_executable_sha256: &'a str,
}

fn render(bindings: GeneratedBindings<'_>) -> Result<String, std::fmt::Error> {
    let mut output = String::new();
    writeln!(
        output,
        "pub(crate) const CLEAN_BUILD_ENFORCED: bool = {};",
        bindings.clean_build_enforced
    )?;
    writeln!(
        output,
        "pub(crate) const BUILD_ENVIRONMENT_POLICY: &str = {:?};",
        bindings.build_environment_policy
    )?;
    writeln!(
        output,
        "pub(crate) const BUILD_COMMAND_SHA256: &str = {:?};",
        bindings.build_command_sha256
    )?;
    writeln!(
        output,
        "pub(crate) const BUILD_ENVIRONMENT_SHA256: &str = {:?};",
        bindings.build_environment_sha256
    )?;
    writeln!(
        output,
        "pub(crate) const BUILD_EVIDENCE_BACKEND: &str = {:?};",
        bindings.evidence_backend
    )?;
    writeln!(
        output,
        "pub(crate) const BASELINE_LOCK_PATH: Option<&str> = {};",
        option_literal(
            bindings
                .baseline_lock_sha256
                .map(|_| "./baseline-lock.json")
        )
    )?;
    writeln!(
        output,
        "pub(crate) const BASELINE_LOCK_SHA256: Option<&str> = {};",
        option_literal(bindings.baseline_lock_sha256)
    )?;
    writeln!(
        output,
        "pub(crate) const BASELINE_MANIFEST_PATH: Option<&str> = {};",
        option_literal(
            bindings
                .baseline_manifest_sha256
                .map(|_| "./baseline-manifest.json")
        )
    )?;
    writeln!(
        output,
        "pub(crate) const BASELINE_MANIFEST_SHA256: Option<&str> = {};",
        option_literal(bindings.baseline_manifest_sha256)
    )?;
    writeln!(
        output,
        "pub(crate) const BASELINE_MEASURED_CODE_HEAD: Option<&str> = {};",
        option_literal(bindings.baseline_measured_code_head)
    )?;
    writeln!(
        output,
        "pub(crate) const IMMUTABLE_MODULE_SHA256: &[(&str, &str)] = &["
    )?;
    for (name, hash) in bindings.module_hashes {
        writeln!(output, "    ({name:?}, {hash:?}),")?;
    }
    writeln!(output, "];")?;
    for (name, value) in [
        (
            "MEASURED_SOURCE_CLOSURE_SHA256",
            bindings.measured_source_closure_sha256,
        ),
        ("ENTRYPOINT_SHA256", bindings.entrypoint_sha256),
        (
            "BACKEND_DISPATCHER_SHA256",
            bindings.backend_dispatcher_sha256,
        ),
        (
            "SELECTED_BACKEND_SOURCE_PATH",
            bindings.selected_backend_source_path,
        ),
        (
            "SELECTED_BACKEND_SOURCE_SHA256",
            bindings.selected_backend_source_sha256,
        ),
        ("BACKEND_SHA256", bindings.backend_sha256),
        ("CRITERION_SHA256", bindings.criterion_sha256),
        ("OBSERVER_SHA256", bindings.observer_sha256),
        ("PLATFORM_SOURCE_SHA256", bindings.platform_source_sha256),
        ("DOMAIN_SOURCE_SHA256", bindings.domain_source_sha256),
        ("SOURCE_INVENTORY_SHA256", bindings.source_inventory_sha256),
        ("CARGO_LOCK_SHA256", bindings.cargo_lock_sha256),
        (
            "WORKSPACE_MANIFEST_SHA256",
            bindings.workspace_manifest_sha256,
        ),
        ("PACKAGE_MANIFEST_SHA256", bindings.package_manifest_sha256),
        ("BUILD_SCRIPT_SHA256", bindings.build_script_sha256),
        (
            "BUILD_SUPPORT_TREE_SHA256",
            bindings.build_support_tree_sha256,
        ),
        ("CARGO_EXECUTABLE_SHA256", bindings.cargo_executable_sha256),
        ("GIT_EXECUTABLE_SHA256", bindings.git_executable_sha256),
        ("RUSTC_EXECUTABLE_SHA256", bindings.rustc_executable_sha256),
    ] {
        writeln!(output, "pub(crate) const {name}: &str = {value:?};")?;
    }
    Ok(output)
}

fn option_literal(value: Option<&str>) -> String {
    value.map_or_else(|| "None".to_owned(), |value| format!("Some({value:?})"))
}

fn collect_rust_files(root: &Path) -> Result<Vec<build_support::BoundSourceFile>, Box<dyn Error>> {
    build_support::collect_rust_files(
        root,
        MAX_SOURCE_ENTRIES,
        MAX_SOURCE_DEPTH,
        MAX_SOURCE_FILE_BYTES,
        MAX_SOURCE_INVENTORY_BYTES,
    )
}

fn tree_hash(
    repository: &Path,
    files: &[build_support::BoundSourceFile],
) -> Result<String, Box<dyn Error>> {
    let mut digest = Sha256::new();
    for source in files {
        let relative = source.path.strip_prefix(repository)?;
        digest.update(relative.as_os_str().as_encoded_bytes());
        digest.update([0]);
        digest.update(source.sha256.as_bytes());
        digest.update([0]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn build_support_tree_hash(
    manifest: &Path,
    paths: &[PathBuf; BUILD_SUPPORT_TREE.len()],
) -> Result<String, Box<dyn Error>> {
    let mut digest = Sha256::new();
    digest.update(BUILD_SUPPORT_TREE_DOMAIN);
    for path in paths {
        let relative = path.strip_prefix(manifest)?;
        let relative = relative
            .to_str()
            .ok_or("build-support path is not valid UTF-8")?;
        let length = u64::try_from(relative.len())?;
        digest.update(length.to_be_bytes());
        digest.update(relative.as_bytes());
        digest.update(hash_file(path)?.as_bytes());
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn measured_source_closure_hash(
    components: &mut [(String, String)],
) -> Result<String, Box<dyn Error>> {
    components.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    if components.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err("measured source closure contains a duplicate identity".into());
    }
    let mut digest = Sha256::new();
    digest.update(MEASURED_SOURCE_CLOSURE_DOMAIN);
    for (identity, value) in components {
        for component in [identity.as_bytes(), value.as_bytes()] {
            digest.update(u64::try_from(component.len())?.to_be_bytes());
            digest.update(component);
        }
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn inventory_hash<'a>(
    repository: &Path,
    files: impl Iterator<Item = &'a build_support::BoundSourceFile>,
) -> Result<String, Box<dyn Error>> {
    let mut paths = files
        .map(|source| {
            source
                .path
                .strip_prefix(repository)
                .map(|relative| relative.as_os_str().as_encoded_bytes().to_vec())
        })
        .collect::<Result<Vec<_>, _>>()?;
    paths.sort();
    let mut digest = Sha256::new();
    for path in paths {
        digest.update(path);
        digest.update([0]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn hash_file(path: &Path) -> Result<String, Box<dyn Error>> {
    build_support::hash_regular_file(path, MAX_BUILD_INPUT_BYTES)
}

fn command(
    program: &Path,
    repository: &Path,
    arguments: &[&str],
) -> Result<String, Box<dyn Error>> {
    let output = build_support::run_command_with_post_spawn_deadline(
        program,
        repository,
        arguments,
        16 * 1024,
        16 * 1024,
        Duration::from_secs(10),
        if std::env::var_os("CAPTURE_BENCH_REQUIRE_CLEAN_BUILD").is_some() {
            build_support::authoritative_command_policy(
                std::env::var("CAPTURE_BENCH_PROCESS_GROUP_POLICY")
                    .ok()
                    .as_deref(),
                std::env::var("CAPTURE_BENCH_EXPECTED_PROCESS_GROUP_ID")
                    .ok()
                    .as_deref(),
            )?
        } else {
            build_support::CommandPolicy::DevelopmentIsolated
        },
    )?;
    let _stderr_was_bounded = output.stderr;
    Ok(std::str::from_utf8(&output.stdout)?.trim().to_owned())
}

fn absolute_bound_tool(
    path_key: &str,
    digest_key: &str,
) -> Result<(PathBuf, String), Box<dyn Error>> {
    let path = PathBuf::from(std::env::var(path_key)?);
    let digest = std::env::var(digest_key)?;
    if !path.is_absolute()
        || !is_lower_digest(&digest)
        || build_support::hash_bound_executable(&path, MAX_BUILD_INPUT_BYTES)? != digest
    {
        return Err("authoritative benchmark executable binding is invalid".into());
    }
    Ok((path, digest))
}

fn rerun(path: &Path) {
    println!("cargo:rerun-if-changed={}", path.display());
}
