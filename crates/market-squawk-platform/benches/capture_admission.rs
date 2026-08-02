#![recursion_limit = "256"]

//! Fixed-operation diagnostic capture measurement executable.
//!
//! The project collector is authoritative: one external invocation executes each matrix cell
//! exactly once at its frozen operation quota. Adaptive Criterion output is produced only by the
//! separate `capture_admission_criterion` engineering target and has zero evidence authority.

#[cfg(all(capture_bench_authoritative, not(panic = "abort")))]
compile_error!("authoritative capture evidence requires the release abort panic strategy");

#[cfg(all(capture_bench_authoritative, debug_assertions))]
compile_error!("authoritative capture evidence requires debug assertions to be disabled");

#[path = "capture_admission/backend.rs"]
mod backend;
#[path = "capture_admission/benchmark_identity.rs"]
mod benchmark_identity;
mod build_bindings {
    include!(concat!(env!("OUT_DIR"), "/capture_benchmark_bindings.rs"));
}
#[path = "capture_admission/collector.rs"]
mod collector;
#[path = "capture_admission/endpoints.rs"]
mod endpoints;
#[path = "capture_admission/evidence_io.rs"]
mod evidence_io;
#[path = "capture_admission/fixture.rs"]
mod fixture;
#[path = "capture_admission/producer_inventory.rs"]
mod producer_inventory;
#[path = "capture_admission/schema.rs"]
mod schema;
#[path = "capture_admission/workload.rs"]
mod workload;

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::io::Write as _;
use std::num::NonZeroUsize;
use std::path::Path;

use endpoints::Endpoint;
use evidence_io::EvidenceDirectory;
use fixture::{
    PAYLOAD_BYTES, QUEUE_DEPTHS, SUSTAINED_FIXTURE, WRITER_QUEUE_DEPTHS, producer_cases,
};
use schema::{
    BaselineCompatibility, BaselineLock, BaselineManifest, BuildEvidence,
    CandidateBaselineExpectation, HostGateComparison, HostGateManifest, RESULT_SCHEMA_VERSION,
    RepetitionEvidence, validate_candidate_baseline_compatibility, validate_repetition,
};

fn main() -> Result<(), Box<dyn Error>> {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    if arguments.is_empty() || arguments.as_slice() == ["--test"] {
        return self_check();
    }
    if arguments.as_slice() == ["--print-build-bindings"] {
        return print_build_bindings();
    }
    if arguments.as_slice() != ["--bench"] {
        return Err("capture benchmark accepts only the closed --bench measurement marker".into());
    }
    validate_backend_contract()?;
    require_exact_environment("CAPTURE_BENCH_BACKEND", backend::EVIDENCE_BACKEND)?;
    let output = confined_output_directory()?;
    let build_evidence = validate_build_evidence(&output)?;
    let (baseline_manifest_sha256, baseline_lock_sha256) =
        validate_baseline_manifest(&output, &build_evidence)?;
    if let Some(value) = std::env::var_os("CAPTURE_BENCH_FINALIZE_ONLY") {
        if value != "1" {
            return Err("CAPTURE_BENCH_FINALIZE_ONLY must equal 1 when set".into());
        }
        return finalize(
            &output,
            &build_evidence,
            baseline_manifest_sha256,
            baseline_lock_sha256,
        );
    }
    require_exact_environment(
        "CAPTURE_BENCH_EXPECTED_FIXTURES",
        backend::EXPECTED_FIXTURES,
    )?;
    let repetition: u8 = std::env::var("CAPTURE_BENCH_REPETITION")?.parse()?;
    if !(1..=5).contains(&repetition) {
        return Err("capture benchmark repetition must be in 1..=5".into());
    }
    let mut matrix = Vec::new();
    let producer_cases = producer_cases()?;
    let depth_cells = 3_usize
        .checked_mul(QUEUE_DEPTHS.len())
        .and_then(|count| count.checked_add(2 * WRITER_QUEUE_DEPTHS.len()))
        .ok_or("matrix depth cell count overflowed")?;
    let matrix_cells = depth_cells
        .checked_mul(PAYLOAD_BYTES.len())
        .and_then(|count| count.checked_mul(producer_cases.len()))
        .ok_or("matrix cell count overflowed")?;
    matrix.try_reserve_exact(matrix_cells)?;
    for endpoint in Endpoint::ALL {
        for payload_bytes in PAYLOAD_BYTES {
            let queue_depths = if endpoint.has_deferred_writer_samples() {
                WRITER_QUEUE_DEPTHS.as_slice()
            } else {
                QUEUE_DEPTHS.as_slice()
            };
            for &queue_depth in queue_depths {
                for producer_case in &producer_cases {
                    matrix.push(workload::run_matrix_case(
                        endpoint,
                        payload_bytes,
                        queue_depth,
                        *producer_case,
                    )?);
                }
            }
        }
    }
    let evidence = RepetitionEvidence {
        schema_version: RESULT_SCHEMA_VERSION,
        runner: benchmark_identity::EVIDENCE_TARGET.to_owned(),
        evidence_mode: benchmark_identity::FIXED_QUOTA_EVIDENCE_MODE.to_owned(),
        build_evidence_sha256: output.hash_file(Path::new("build-evidence.json"), 1024 * 1024)?,
        measured_code_head: build_bindings::BUILD_GIT_HEAD.to_owned(),
        backend: backend::EVIDENCE_BACKEND.to_owned(),
        queue_transport: backend::QUEUE_TRANSPORT.to_owned(),
        queue_private_storage_accounting: backend::QUEUE_PRIVATE_STORAGE_ACCOUNTING.to_owned(),
        repetition,
        fixtures: backend::FIXTURES
            .iter()
            .map(|fixture| (*fixture).to_owned())
            .collect(),
        matrix,
        comparable_full: workload::run_comparable_full()?,
        forced_lock: backend::run_forced_lock()?,
        sustained_rss: workload::run_sustained(
            SUSTAINED_FIXTURE,
            producer_cases
                .iter()
                .find(|case| case.representative)
                .ok_or("representative producer case is absent")?
                .count,
        )?,
    };
    output.write_json(&format!("repetition-{repetition}.json"), &evidence)
}

fn self_check() -> Result<(), Box<dyn Error>> {
    schema::self_check_candidate_baseline_contracts()?;
    validate_backend_contract()?;
    workload::self_check_rss_adapter()?;
    schema::self_check_rss_sample_contract()?;
    schema::self_check_post_drain_memory_contract()?;
    let producers = producer_cases()?;
    if benchmark_identity::verify_distinct_authority_labels().is_err()
        || Endpoint::ALL.len() != 5
        || PAYLOAD_BYTES != [0, 1_024, 4_194_304]
        || QUEUE_DEPTHS != [1, 64, 16_384]
        || WRITER_QUEUE_DEPTHS != [64]
        || producers.is_empty()
    {
        return Err("frozen benchmark matrix contract changed".into());
    }
    for payload_bytes in PAYLOAD_BYTES {
        for producer in &producers {
            let requested = fixture::requested_operations(payload_bytes, producer.count)?;
            if payload_bytes == 4_194_304 {
                if requested != fixture::MAX_PAYLOAD_OPERATIONS {
                    return Err("maximum-payload operation quota changed".into());
                }
            } else {
                let scaled = producer
                    .count
                    .get()
                    .checked_mul(fixture::OPERATIONS_PER_PRODUCER)
                    .ok_or("scaled operation quota overflowed")?;
                if requested != fixture::MINIMUM_OPERATIONS.max(scaled) {
                    return Err("scaled operation quota changed".into());
                }
            }
        }
    }
    let future_fan_in = NonZeroUsize::new(11).ok_or("future fan-in fixture is zero")?;
    if fixture::requested_operations(1_024, future_fan_in)? != 11 * fixture::OPERATIONS_PER_PRODUCER
        || fixture::requested_operations(4_194_304, NonZeroUsize::MAX)?
            != fixture::MAX_PAYLOAD_OPERATIONS
        || fixture::requested_operations(1_024, NonZeroUsize::MAX).is_ok()
    {
        return Err("checked payload-aware operation quota contract changed".into());
    }
    let full = workload::run_comparable_full()?;
    if full.queue_full == 0 {
        return Err("comparable-full self-check did not refuse".into());
    }
    match (backend::REQUIRES_BASELINE, backend::run_forced_lock()?) {
        (false, None) => {}
        (true, Some(proof))
            if proof.schema_version == RESULT_SCHEMA_VERSION
                && proof.backend == backend::EVIDENCE_BACKEND
                && proof.slot_lock_unavailable == 1
                && proof.accepted == 0
                && proof.consumed == 0
                && proof.queued_bytes == 0
                && proof.record_reservations.is_none()
                && proof.queue_private_storage_bytes > 0
                && proof.accounting_invariant_failures == 0 => {}
        _ => return Err("backend forced-lock self-check contract failed".into()),
    }
    self_check_selected_transport_endpoints()?;
    Ok(())
}

fn self_check_selected_transport_endpoints() -> Result<(), Box<dyn Error>> {
    let queue_depth = NonZeroUsize::new(64).ok_or("self-check queue depth is zero")?;
    for endpoint in Endpoint::ALL {
        let case = backend::PreparedCase::try_new(endpoint, 8, queue_depth, 2)?;
        for _ordinal in 0..2 {
            let operation = case.try_producer()?.try_prepare_operation()?;
            let _attempt = operation.execute()?;
        }
        let reconciliation = case.finish()?;
        if reconciliation.accepted() != 2
            || reconciliation.consumed() != 2
            || reconciliation.queued_bytes() != 0
            || reconciliation.accounting_invariant_failures() != 0
            || !selected_transport_memory_is_valid(&reconciliation)
        {
            return Err(format!("selected transport failed {endpoint:?} reconciliation").into());
        }
        let samples = reconciliation.into_samples();
        let expected_samples = if endpoint.has_deferred_samples() {
            2
        } else {
            0
        };
        if samples.len() != expected_samples {
            return Err(format!("selected transport failed {endpoint:?} sample parity").into());
        }
    }
    Ok(())
}

fn selected_transport_memory_is_valid(
    reconciliation: &market_squawk_platform::capture_benchmark_support::BenchmarkCaseReconciliation,
) -> bool {
    match backend::QUEUE_PRIVATE_STORAGE_ACCOUNTING {
        "not_measured" => {
            reconciliation.queue_private_storage_bytes().is_none()
                && reconciliation.fixed_capture_bytes().is_none()
                && reconciliation.total_accounted_bytes().is_none()
        }
        "exact" => matches!(
            (
                reconciliation.queue_private_storage_bytes(),
                reconciliation.fixed_capture_bytes(),
                reconciliation.total_accounted_bytes(),
            ),
            (Some(queue), Some(fixed), Some(total))
                if queue > 0 && fixed >= queue && total >= fixed
        ),
        _ => false,
    }
}

fn finalize(
    output: &EvidenceDirectory,
    build_evidence: &BuildEvidence,
    baseline_manifest_sha256: Option<String>,
    baseline_lock_sha256: Option<String>,
) -> Result<(), Box<dyn Error>> {
    require_exact_artifact_set(output, false)?;
    let build_evidence_sha256 = output.hash_file(Path::new("build-evidence.json"), 1024 * 1024)?;
    let producer_cases = producer_cases()?;
    let mut repetition_sha256 = BTreeMap::new();
    for repetition in 1_u8..=5 {
        let path = Path::new(&format!("repetition-{repetition}.json")).to_owned();
        let evidence: RepetitionEvidence = output.read_json(&path, 128 * 1024 * 1024)?;
        if evidence.build_evidence_sha256 != build_evidence_sha256 {
            return Err("repetition does not bind the exact build evidence".into());
        }
        validate_repetition(&evidence, repetition, &producer_cases)?;
        repetition_sha256.insert(
            format!("repetition-{repetition}.json"),
            output.hash_file(&path, 128 * 1024 * 1024)?,
        );
    }
    let expected_host_evidence = output.path().join("host-gate/comparison.json");
    let host_evidence = Path::new(&std::env::var("CAPTURE_BENCH_HOST_EVIDENCE")?).to_owned();
    if host_evidence != expected_host_evidence {
        return Err("host evidence is not the controlled evidence-local comparison".into());
    }
    let host: HostGateComparison =
        output.read_json(Path::new("host-gate/comparison.json"), 1024 * 1024)?;
    host.validate()?;
    if host.baseline_manifest_sha256 != baseline_manifest_sha256 {
        return Err("host gate and runner disagree on the bound baseline manifest".into());
    }
    if host.baseline_lock_sha256 != baseline_lock_sha256 {
        return Err("host gate and runner disagree on the tracked baseline lock".into());
    }
    if backend::REQUIRES_BASELINE {
        let baseline: BaselineManifest =
            output.read_json(Path::new("baseline-manifest.json"), 4 * 1024 * 1024)?;
        if baseline.host_fingerprint_sha256 != host.host_fingerprint_sha256
            || baseline.toolchain_fingerprint_sha256 != host.toolchain_fingerprint_sha256
            || baseline.release_profile_sha256 != host.release_profile_sha256
        {
            return Err(
                "candidate host, toolchain, or release profile differs from baseline".into(),
            );
        }
    }
    let preflight_sha256 = output.hash_file(Path::new("host-gate/preflight.json"), 1024 * 1024)?;
    let postflight_sha256 =
        output.hash_file(Path::new("host-gate/postflight.json"), 1024 * 1024)?;
    let monitor_sha256 = output.hash_file(Path::new("host-gate/monitor.json"), 1024 * 1024)?;
    if preflight_sha256 != host.preflight_sha256 || postflight_sha256 != host.postflight_sha256 {
        return Err("host-gate comparison does not bind its phase artifacts".into());
    }
    if monitor_sha256 != host.monitor_sha256
        || host.build_evidence_sha256
            != output.hash_file(Path::new("build-evidence.json"), 1024 * 1024)?
        || host.runner_sha256
            != output.hash_file(
                Path::new("capture_admission_evidence-exe"),
                256 * 1024 * 1024,
            )?
    {
        return Err("continuous monitor does not bind its runner and build evidence".into());
    }
    let executable = std::env::current_exe()?;
    let immutable = build_bindings::IMMUTABLE_MODULE_SHA256
        .iter()
        .map(|(name, digest)| ((*name).to_owned(), (*digest).to_owned()))
        .collect();
    let production_library_sha256 = [
        (
            "market-squawk-platform".to_owned(),
            build_bindings::PLATFORM_SOURCE_SHA256.to_owned(),
        ),
        (
            "market-squawk-domain".to_owned(),
            build_bindings::DOMAIN_SOURCE_SHA256.to_owned(),
        ),
    ]
    .into_iter()
    .collect();
    let mut artifact_inputs = vec![
        ("active-agent-attestation.txt", 128_u64),
        ("build-evidence.json", 1024 * 1024),
        ("capture-bench-build.json", 64 * 1024 * 1024),
        ("capture_admission_evidence-exe", 256 * 1024 * 1024),
        ("host-gate/preflight.json", 1024 * 1024),
        ("host-gate/monitor.json", 1024 * 1024),
        ("host-gate/postflight.json", 1024 * 1024),
        ("host-gate/comparison.json", 1024 * 1024),
    ];
    if backend::REQUIRES_BASELINE {
        artifact_inputs.push(("baseline-manifest.json", 4 * 1024 * 1024));
        artifact_inputs.push(("baseline-lock.json", 4 * 1024 * 1024));
    }
    let artifact_sha256 = artifact_inputs
        .into_iter()
        .map(|(name, maximum)| {
            output
                .hash_file(Path::new(name), maximum)
                .map(|digest| (name.to_owned(), digest))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let tool_sha256 = tool_sha256(build_evidence);
    let manifest = BaselineManifest {
        schema_version: RESULT_SCHEMA_VERSION,
        runner: benchmark_identity::EVIDENCE_TARGET.to_owned(),
        evidence_mode: benchmark_identity::FIXED_QUOTA_EVIDENCE_MODE.to_owned(),
        criterion_evidence_mode: benchmark_identity::CRITERION_EVIDENCE_MODE.to_owned(),
        measured_code_head: build_bindings::BUILD_GIT_HEAD.to_owned(),
        build_evidence_sha256,
        build_environment_policy: build_evidence.build_environment_policy.clone(),
        build_command_sha256: build_evidence.build_command_sha256.clone(),
        build_environment_sha256: build_evidence.build_environment_sha256.clone(),
        cargo_executable_sha256: build_evidence.cargo_executable_sha256.clone(),
        git_executable_sha256: build_evidence.git_executable_sha256.clone(),
        rustc_executable_sha256: build_evidence.rustc_executable_sha256.clone(),
        cargo_json_sha256: build_evidence.cargo_json_sha256.clone(),
        source_inventory_sha256: build_bindings::SOURCE_INVENTORY_SHA256.to_owned(),
        cargo_lock_sha256: build_bindings::CARGO_LOCK_SHA256.to_owned(),
        criterion_sha256: build_bindings::CRITERION_SHA256.to_owned(),
        observer_sha256: build_bindings::OBSERVER_SHA256.to_owned(),
        baseline_manifest_sha256,
        baseline_lock_sha256,
        backend: backend::EVIDENCE_BACKEND.to_owned(),
        queue_transport: backend::QUEUE_TRANSPORT.to_owned(),
        queue_private_storage_accounting: backend::QUEUE_PRIVATE_STORAGE_ACCOUNTING.to_owned(),
        benchmark_support_feature: "capture-benchmark".to_owned(),
        fixtures: backend::FIXTURES
            .iter()
            .map(|fixture| (*fixture).to_owned())
            .collect(),
        repetitions: vec![1, 2, 3, 4, 5],
        executable_path: "./capture_admission_evidence-exe".to_owned(),
        executable_sha256: output.hash_file(
            Path::new(
                executable
                    .file_name()
                    .ok_or("benchmark executable has no file name")?,
            ),
            256 * 1024 * 1024,
        )?,
        immutable_module_sha256: immutable,
        entrypoint_sha256: build_bindings::ENTRYPOINT_SHA256.to_owned(),
        backend_sha256: build_bindings::BACKEND_SHA256.to_owned(),
        production_library_sha256,
        repetition_sha256,
        artifact_sha256,
        tool_sha256,
        host_fingerprint_sha256: host.host_fingerprint_sha256,
        toolchain_fingerprint_sha256: host.toolchain_fingerprint_sha256,
        release_profile_sha256: host.release_profile_sha256,
        host_gate: HostGateManifest {
            valid: true,
            preflight_sha256,
            postflight_sha256,
            comparison_sha256: output
                .hash_file(Path::new("host-gate/comparison.json"), 1024 * 1024)?,
            monitor_sha256,
        },
    };
    output.write_json("manifest.json", &manifest)?;
    require_exact_artifact_set(output, true)?;
    let persisted: BaselineManifest =
        output.read_json(Path::new("manifest.json"), 4 * 1024 * 1024)?;
    if persisted != manifest {
        return Err("persisted manifest failed exact self-validation".into());
    }
    Ok(())
}

fn require_exact_artifact_set(
    output: &EvidenceDirectory,
    include_manifest: bool,
) -> Result<(), Box<dyn Error>> {
    let mut expected = [
        "active-agent-attestation.txt".to_owned(),
        "build-evidence.json".to_owned(),
        "capture-bench-build.json".to_owned(),
        "capture_admission_evidence-exe".to_owned(),
        "host-gate".to_owned(),
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    if backend::REQUIRES_BASELINE {
        expected.insert("baseline-manifest.json".to_owned());
        expected.insert("baseline-lock.json".to_owned());
    }
    for repetition in 1_u8..=5 {
        expected.insert(format!("repetition-{repetition}.json"));
    }
    if include_manifest {
        expected.insert("manifest.json".to_owned());
    }
    if output.entry_names()?.into_iter().collect::<BTreeSet<_>>() != expected {
        return Err("evidence root has missing, extra, or non-authoritative artifacts".into());
    }
    output.require_directory(Path::new("host-gate"))?;
    let expected_host = [
        "comparison.json".to_owned(),
        "monitor.json".to_owned(),
        "postflight.json".to_owned(),
        "preflight.json".to_owned(),
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    if output
        .entry_names_at(Some(Path::new("host-gate")))?
        .into_iter()
        .collect::<BTreeSet<_>>()
        != expected_host
    {
        return Err("host evidence directory has missing or extra artifacts".into());
    }
    Ok(())
}

fn require_exact_environment(name: &str, expected: &str) -> Result<(), Box<dyn Error>> {
    if std::env::var(name)?.as_str() != expected {
        return Err(format!("{name} must equal the frozen value").into());
    }
    Ok(())
}

fn confined_output_directory() -> Result<EvidenceDirectory, Box<dyn Error>> {
    EvidenceDirectory::try_open(Path::new(&std::env::var("CAPTURE_BENCH_OUTPUT")?))
}

fn validate_build_evidence(output: &EvidenceDirectory) -> Result<BuildEvidence, Box<dyn Error>> {
    let expected = output.path().join("build-evidence.json");
    let supplied = Path::new(&std::env::var("CAPTURE_BENCH_BUILD_EVIDENCE")?).to_owned();
    if supplied != expected {
        return Err("build evidence is not the controlled evidence-local artifact".into());
    }
    let evidence: BuildEvidence =
        output.read_json(Path::new("build-evidence.json"), 1024 * 1024)?;
    evidence.validate()?;
    let executable_sha256 = output.hash_file(
        Path::new("capture_admission_evidence-exe"),
        256 * 1024 * 1024,
    )?;
    let cargo_json_sha256 =
        output.hash_file(Path::new("capture-bench-build.json"), 64 * 1024 * 1024)?;
    if evidence.executable_sha256 != executable_sha256
        || evidence.cargo_json_sha256 != cargo_json_sha256
    {
        return Err("build evidence does not bind the copied executable and Cargo JSON".into());
    }
    Ok(evidence)
}

fn validate_backend_contract() -> Result<(), Box<dyn Error>> {
    backend::validate_compiled_transport()?;
    let valid = match backend::EVIDENCE_BACKEND {
        "standard" => {
            !backend::REQUIRES_BASELINE
                && backend::QUEUE_TRANSPORT == "standard_sync_channel"
                && backend::QUEUE_PRIVATE_STORAGE_ACCOUNTING == "not_measured"
                && backend::FIXTURES == ["matrix", "comparable_full", "sustained_rss"]
                && backend::EXPECTED_FIXTURES == "matrix,comparable_full,sustained_rss"
        }
        "candidate" => {
            backend::REQUIRES_BASELINE
                && backend::QUEUE_TRANSPORT == "candidate_fixed_ring"
                && backend::QUEUE_PRIVATE_STORAGE_ACCOUNTING == "exact"
                && backend::FIXTURES
                    == ["matrix", "comparable_full", "forced_lock", "sustained_rss"]
                && backend::EXPECTED_FIXTURES == "matrix,comparable_full,forced_lock,sustained_rss"
        }
        _ => false,
    };
    if !valid {
        return Err("benchmark backend adapter declares an invalid closed contract".into());
    }
    Ok(())
}

fn validate_baseline_manifest(
    output: &EvidenceDirectory,
    build_evidence: &BuildEvidence,
) -> Result<(Option<String>, Option<String>), Box<dyn Error>> {
    if !backend::REQUIRES_BASELINE {
        if std::env::var_os("CAPTURE_BENCH_BASELINE_MANIFEST").is_some() {
            return Err("standard evidence forbids a candidate baseline input".into());
        }
        if std::env::var_os("CAPTURE_BENCH_BASELINE_LOCK").is_some() {
            return Err("standard evidence forbids a tracked baseline lock input".into());
        }
        return Ok((None, None));
    }
    let expected_path = output.path().join("baseline-manifest.json");
    let supplied = Path::new(&std::env::var("CAPTURE_BENCH_BASELINE_MANIFEST")?).to_owned();
    if supplied != expected_path {
        return Err("candidate baseline is not the evidence-local build-bound artifact".into());
    }
    let expected_lock_path = output.path().join("baseline-lock.json");
    let supplied_lock = Path::new(&std::env::var("CAPTURE_BENCH_BASELINE_LOCK")?).to_owned();
    if supplied_lock != expected_lock_path {
        return Err("candidate lock is not the evidence-local tracked authority".into());
    }
    let (lock, observed_lock_sha256): (BaselineLock, String) =
        output.read_json_and_hash(Path::new("baseline-lock.json"), 4 * 1024 * 1024)?;
    let (manifest, observed_manifest_sha256): (BaselineManifest, String) =
        output.read_json_and_hash(Path::new("baseline-manifest.json"), 4 * 1024 * 1024)?;
    let expected_manifest_sha256 = build_evidence
        .baseline_manifest_sha256
        .clone()
        .ok_or("candidate build evidence omitted its baseline digest")?;
    let expected_baseline_head = build_evidence
        .baseline_measured_code_head
        .clone()
        .ok_or("candidate build evidence omitted its baseline head")?;
    let expected_lock_sha256 = build_evidence
        .baseline_lock_sha256
        .clone()
        .ok_or("candidate build evidence omitted its tracked lock digest")?;
    let expected = CandidateBaselineExpectation {
        observed_manifest_sha256: observed_manifest_sha256.clone(),
        expected_manifest_sha256,
        observed_lock_sha256: observed_lock_sha256.clone(),
        expected_lock_sha256,
        expected_baseline_head,
        candidate_head: build_evidence.measured_code_head.clone(),
        immutable_module_sha256: build_evidence.immutable_module_sha256.clone(),
        entrypoint_sha256: build_evidence.entrypoint_sha256.clone(),
        backend_sha256: build_evidence.backend_sha256.clone(),
        criterion_sha256: build_evidence.criterion_sha256.clone(),
        observer_sha256: build_evidence.observer_sha256.clone(),
        tool_sha256: tool_sha256(build_evidence),
        rustc_executable_sha256: build_evidence.rustc_executable_sha256.clone(),
        lock,
    };
    validate_candidate_baseline_compatibility(&BaselineCompatibility::from(&manifest), &expected)?;
    Ok((Some(observed_manifest_sha256), Some(observed_lock_sha256)))
}

fn tool_sha256(build_evidence: &BuildEvidence) -> BTreeMap<String, String> {
    [
        ("build.rs", build_evidence.build_script_sha256.clone()),
        (
            "build_support-tree-v1",
            build_evidence.build_support_sha256.clone(),
        ),
        (
            "capture_benchmark_host_gate.sh",
            build_evidence.host_gate_shell_sha256.clone(),
        ),
        (
            "capture_benchmark_host_gate.py",
            build_evidence.host_gate_python_sha256.clone(),
        ),
        (
            "capture_benchmark_process.py",
            build_evidence.host_gate_process_sha256.clone(),
        ),
        (
            "capture_benchmark_evidence_io.py",
            build_evidence.host_gate_evidence_io_sha256.clone(),
        ),
        (
            "capture_benchmark_host_cli.py",
            build_evidence.host_gate_cli_sha256.clone(),
        ),
        (
            "capture_benchmark_host_schema.py",
            build_evidence.host_gate_schema_sha256.clone(),
        ),
        (
            "capture_benchmark_host_execution.py",
            build_evidence.host_gate_execution_sha256.clone(),
        ),
        (
            "capture_benchmark_host_observation.py",
            build_evidence.host_gate_observation_sha256.clone(),
        ),
        (
            "capture_benchmark_host_measured.py",
            build_evidence.host_gate_measured_sha256.clone(),
        ),
        (
            "capture_benchmark_prepare_build_evidence.py",
            build_evidence.build_evidence_python_sha256.clone(),
        ),
        (
            "cargo-executable",
            build_evidence.cargo_executable_sha256.clone(),
        ),
        (
            "git-executable",
            build_evidence.git_executable_sha256.clone(),
        ),
        (
            "rustc-executable",
            build_evidence.rustc_executable_sha256.clone(),
        ),
    ]
    .into_iter()
    .map(|(name, digest)| (name.to_owned(), digest))
    .collect()
}

fn print_build_bindings() -> Result<(), Box<dyn Error>> {
    let immutable = build_bindings::IMMUTABLE_MODULE_SHA256
        .iter()
        .map(|(name, digest)| (*name, *digest))
        .collect::<std::collections::BTreeMap<_, _>>();
    let value = serde_json::json!({
        "schema_version": RESULT_SCHEMA_VERSION,
        "runner": benchmark_identity::EVIDENCE_TARGET,
        "evidence_mode": benchmark_identity::FIXED_QUOTA_EVIDENCE_MODE,
        "evidence_backend": build_bindings::BUILD_EVIDENCE_BACKEND,
        "queue_transport": backend::QUEUE_TRANSPORT,
        "queue_private_storage_accounting": backend::QUEUE_PRIVATE_STORAGE_ACCOUNTING,
        "build_profile": "cargo-release-binary:opt-level=3:lto=thin:codegen-units=1:panic=abort:strip=symbols",
        "measured_code_head": build_bindings::BUILD_GIT_HEAD,
        "clean_build_enforced": build_bindings::CLEAN_BUILD_ENFORCED,
        "build_environment_policy": build_bindings::BUILD_ENVIRONMENT_POLICY,
        "build_command_sha256": build_bindings::BUILD_COMMAND_SHA256,
        "build_environment_sha256": build_bindings::BUILD_ENVIRONMENT_SHA256,
        "source_inventory_sha256": build_bindings::SOURCE_INVENTORY_SHA256,
        "cargo_lock_sha256": build_bindings::CARGO_LOCK_SHA256,
        "workspace_manifest_sha256": build_bindings::WORKSPACE_MANIFEST_SHA256,
        "package_manifest_sha256": build_bindings::PACKAGE_MANIFEST_SHA256,
        "build_script_sha256": build_bindings::BUILD_SCRIPT_SHA256,
        "build_support_sha256": build_bindings::BUILD_SUPPORT_TREE_SHA256,
        "cargo_executable_sha256": build_bindings::CARGO_EXECUTABLE_SHA256,
        "git_executable_sha256": build_bindings::GIT_EXECUTABLE_SHA256,
        "rustc_executable_sha256": build_bindings::RUSTC_EXECUTABLE_SHA256,
        "host_gate_shell_sha256": build_bindings::HOST_GATE_SHELL_SHA256,
        "host_gate_python_sha256": build_bindings::HOST_GATE_PYTHON_SHA256,
        "host_gate_process_sha256": build_bindings::HOST_GATE_PROCESS_SHA256,
        "host_gate_evidence_io_sha256": build_bindings::HOST_GATE_EVIDENCE_IO_SHA256,
        "host_gate_cli_sha256": build_bindings::HOST_GATE_CLI_SHA256,
        "host_gate_schema_sha256": build_bindings::HOST_GATE_SCHEMA_SHA256,
        "host_gate_execution_sha256": build_bindings::HOST_GATE_EXECUTION_SHA256,
        "host_gate_observation_sha256": build_bindings::HOST_GATE_OBSERVATION_SHA256,
        "host_gate_measured_sha256": build_bindings::HOST_GATE_MEASURED_SHA256,
        "build_evidence_python_sha256": build_bindings::BUILD_EVIDENCE_PYTHON_SHA256,
        "platform_source_sha256": build_bindings::PLATFORM_SOURCE_SHA256,
        "domain_source_sha256": build_bindings::DOMAIN_SOURCE_SHA256,
        "entrypoint_sha256": build_bindings::ENTRYPOINT_SHA256,
        "backend_dispatcher_sha256": build_bindings::BACKEND_DISPATCHER_SHA256,
        "selected_backend_source_path": build_bindings::SELECTED_BACKEND_SOURCE_PATH,
        "selected_backend_source_sha256": build_bindings::SELECTED_BACKEND_SOURCE_SHA256,
        "backend_sha256": build_bindings::BACKEND_SHA256,
        "criterion_sha256": build_bindings::CRITERION_SHA256,
        "observer_sha256": build_bindings::OBSERVER_SHA256,
        "baseline_lock_path": build_bindings::BASELINE_LOCK_PATH,
        "baseline_lock_sha256": build_bindings::BASELINE_LOCK_SHA256,
        "baseline_manifest_path": build_bindings::BASELINE_MANIFEST_PATH,
        "baseline_manifest_sha256": build_bindings::BASELINE_MANIFEST_SHA256,
        "baseline_measured_code_head": build_bindings::BASELINE_MEASURED_CODE_HEAD,
        "immutable_module_sha256": immutable,
    });
    let stdout = std::io::stdout();
    let mut locked = stdout.lock();
    serde_json::to_writer(&mut locked, &value)?;
    locked.write_all(b"\n")?;
    Ok(())
}
