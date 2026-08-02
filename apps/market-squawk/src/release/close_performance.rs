//! Strict semantic admission of exact-head release-performance evidence.

use std::path::PathBuf;

use anyhow::{Context as _, Result, bail};
use serde::Deserialize;
use serde_json::Value;

use super::close_quality::{
    validate_binary, validate_process, validate_rss_observation, validate_time_order,
};
use super::identity::RepositoryIdentity;
use super::io::{
    RecordedRepositoryIdentity, StableFileIdentity, hash_stable_file, read_stable_bytes,
    sha256_bytes, valid_sha256,
};
use super::process::{
    ProcessEvidence, ProcessTreeRssObservation, process_tree_rss_poll_sleep_millis,
};

const MAXIMUM_EXECUTABLE_BYTES: u64 = 1024 * 1024 * 1024;
const MAXIMUM_INPUT_BYTES: u64 = 16 * 1024 * 1024;
const MAXIMUM_FIXTURE_BYTES: u64 = 1024 * 1024;
const MAXIMUM_BACKEND_RETAINED_BYTES: usize = 64 * 1024 * 1024;
const EXACT_WARM_UP_EVENTS: u64 = 1_000_000;
const EXACT_MEASURED_EVENTS: u64 = 60_000_000;
const EXACT_STORAGE_ROWS: u64 = 10_000_000;
const EXACT_COMPONENT_OPERATIONS: u64 = 100_000;
const EXACT_ONNX_COMPONENT_OPERATIONS: u64 = 10_000;
const EXACT_MINIMUM_EVENTS_PER_SECOND: u64 = 100_000;
const EXACT_MAXIMUM_WARMED_P99_NANOS: u64 = 999_999;
const EXACT_MAXIMUM_TAIL_GROWTH_BYTES: u64 = 32 * 1024 * 1024;
const EXACT_MAXIMUM_TAIL_GROWTH_PERCENT: u64 = 1;
const EXACT_WORKER_TIMEOUT_MILLIS: u64 = 4 * 60 * 60 * 1_000;
const EXACT_WORKER_RSS_BYTES: u64 = 16 * 1024 * 1024 * 1024;
const EXACT_CURRENT_RSS_SAMPLES: u64 = 5;
const EXACT_CURRENT_RSS_POLL_MILLIS: u64 = 10;
const EXACT_KRAKEN_CHECKSUM: u32 = 3_310_070_434;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum RecordedEvidenceAuthority {
    Provisional,
    ExactHead,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordedPerformanceEvidence {
    repository: RecordedRepositoryIdentity,
    evidence_authority: RecordedEvidenceAuthority,
    application_binary: StableFileIdentity,
    worker_binding: RecordedWorkerBinding,
    onnx_worker_binary: StableFileIdentity,
    cargo_lock: StableFileIdentity,
    rust_toolchain_file: StableFileIdentity,
    application_version: String,
    compiled_features: Vec<String>,
    host: RecordedHostEvidence,
    toolchain: RecordedToolchainEvidence,
    fixtures: RecordedFixtureEvidence,
    configured: RecordedPerformanceConfiguration,
    components: RecordedComponentEvidence,
    integrated_live_path: RecordedIntegratedLivePath,
    analytical_storage: RecordedAnalyticalStorage,
    memory: RecordedMemoryEvidence,
    worker_process: ProcessEvidence,
    threshold_decision: RecordedThresholdDecision,
    worker_started_at: String,
    worker_completed_at: String,
    started_at: String,
    completed_at: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordedHostEvidence {
    operating_system: String,
    kernel: String,
    architecture: String,
    logical_cpus: usize,
    cpu_model: String,
    physical_memory_bytes: u64,
    load_state: String,
    power_state: String,
    thermal_state: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordedToolchainEvidence {
    rustc_verbose: String,
    cargo_version: String,
    stable_release_required: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordedFixtureEvidence {
    kraken_snapshot_sha256: String,
    kraken_expected_checksum: u32,
    native_artifact_sha256: String,
    onnx_artifact_sha256: String,
    onnx_policy_sha256: String,
    onnx_worker_sha256: String,
    onnx_runtime_semantics_sha256: String,
    onnx_warm_up_sha256: String,
    native_backend_retained_bytes: usize,
    onnx_backend_retained_bytes: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordedWorkerBinding {
    authority: RecordedEvidenceAuthority,
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordedPerformanceConfiguration {
    warm_up_events: u64,
    measured_events: u64,
    storage_rows: u64,
    minimum_events_per_second: u64,
    maximum_warmed_p99_nanos: u64,
    maximum_tail_growth_bytes: u64,
    maximum_tail_growth_percent: u64,
    worker_timeout_millis: u64,
    worker_rss_limit_bytes: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordedComponentEvidence {
    kraken_decoder_and_checksum: RecordedLatencyDistribution,
    sequence_validation: RecordedLatencyDistribution,
    checksum_canonicalization: RecordedLatencyDistribution,
    bounded_queue_push: RecordedQueueEvidence,
    order_book_update: RecordedLatencyDistribution,
    online_feature_update: RecordedLatencyDistribution,
    native_inference: RecordedLatencyDistribution,
    onnx_inference: RecordedLatencyDistribution,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordedQueueEvidence {
    latency: RecordedLatencyDistribution,
    configured_depth: usize,
    effective_capacity: usize,
    accepted: usize,
    consumed: usize,
    queued_bytes_after_drain: usize,
    accounting_invariant_failures: u64,
    queue_private_storage_bytes: Option<usize>,
    fixed_capture_bytes: Option<usize>,
    total_accounted_bytes: Option<usize>,
    transport: String,
    private_storage_accounting: String,
    real_full_refusal_verified: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordedIntegratedLivePath {
    latency_boundary: String,
    fixture_scope: String,
    production_path: String,
    event_count: u64,
    measured_outcomes: RecordedMeasuredOutcomes,
    strategy_decision: RecordedLatencyDistribution,
    complete_action_disposition: RecordedLatencyDistribution,
    dispatch_strategy_decision_nanos: u64,
    dispatch_action_disposition_nanos: u64,
    event_to_observed_paper_terminal_nanos: u64,
    dispatch_disposition: String,
    paper_terminal_state: String,
    paper_order_count: usize,
    paper_fill_count: usize,
    mailbox_capacity: usize,
    producer_observed_maximum_in_flight_batches: usize,
    observer_retained_bytes: usize,
    shutdown_complete: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordedMeasuredOutcomes {
    expected_events: u64,
    strategy_successful: u64,
    strategy_failed: u64,
    strategy_unobserved: u64,
    action_no_action: u64,
    action_suppressed: u64,
    action_dispatched: u64,
    action_failed: u64,
    action_unobserved: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordedLatencyDistribution {
    operations: u64,
    elapsed_nanos: u64,
    operations_per_second: u64,
    p50_nanos: u64,
    p95_nanos: u64,
    p99_nanos: u64,
    maximum_nanos: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordedAnalyticalStorage {
    requested_rows: u64,
    measured_rows: u64,
    physical_rows_per_object: u64,
    unique_parquet_objects: u64,
    parquet_content_sha256: [u8; 32],
    parquet_size_bytes: u64,
    arrow_conversion: RecordedStorageLatency,
    parquet_publication: RecordedStorageLatency,
    parquet_read: RecordedStorageLatency,
    datafusion_query: RecordedStorageLatency,
    point_in_time_selected_rows: u64,
    point_in_time_content_sha256: [u8; 32],
    point_in_time_audit_sha256: [u8; 32],
    point_in_time_retained_bytes: usize,
    python_verified_rows: u64,
    python_selected_rows_per_verification: u64,
    python_export_sha256: [u8; 32],
    python_catalog_identity: [u8; 32],
    python_selection_sha256: [u8; 32],
    python_dataset_admission_revalidation: RecordedStorageLatency,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordedStorageLatency {
    operations: u64,
    rows: u64,
    elapsed_nanos: u64,
    rows_per_second: u64,
    p50_nanos: u64,
    p95_nanos: u64,
    p99_nanos: u64,
    maximum_nanos: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordedMemoryEvidence {
    warm_window_current_process_rss_observation: RecordedCurrentRssObservation,
    post_measurement_window_current_process_rss_observation: RecordedCurrentRssObservation,
    supervised_worker_process_tree_rss_observation: ProcessTreeRssObservation,
    tail_growth_bytes: u64,
    permitted_tail_growth_bytes: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordedCurrentRssObservation {
    observed_maximum_rss_bytes: Option<u64>,
    successful_sample_count: u64,
    observation_window_millis: u64,
    configured_poll_sleep_millis: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordedThresholdDecision {
    passed: bool,
    live_throughput_passed: bool,
    live_p99_passed: bool,
    live_tail_growth_passed: bool,
    live_queue_bound_passed: bool,
    storage_completed: bool,
    worker_process_completed: bool,
    worker_process_observed_rss_passed: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OnnxFixtureManifest {
    schema_version: u32,
    models: Vec<OnnxFixtureModel>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OnnxFixtureModel {
    artifact_sha256: String,
    id: String,
    input_shape: Vec<usize>,
    model_hex: String,
    opset: u32,
    output_shape: Vec<usize>,
}

pub(super) fn validate_performance_evidence(
    payload: &Value,
    repository: &RepositoryIdentity,
    binary: &StableFileIdentity,
) -> Result<()> {
    let evidence: RecordedPerformanceEvidence = serde_json::from_value(payload.clone())
        .context("performance evidence payload does not match its strict schema")?;
    evidence.repository.validate_exact(repository)?;
    validate_binary(
        &evidence.application_binary,
        binary,
        "performance application",
    )?;
    validate_bound_files(&evidence, repository, binary)?;
    if evidence.evidence_authority != RecordedEvidenceAuthority::ExactHead
        || evidence.application_version != env!("CARGO_PKG_VERSION")
        || evidence.compiled_features != ["release-evidence"]
    {
        bail!("performance evidence is not the exact release application");
    }
    validate_host(&evidence.host)?;
    validate_toolchain(&evidence.toolchain)?;
    validate_fixtures(&evidence.fixtures, repository, &evidence.onnx_worker_binary)?;
    validate_performance_configuration(&evidence.configured)?;
    validate_worker_binding(
        &evidence.worker_binding,
        repository,
        &evidence.application_binary,
    )?;
    validate_components(&evidence.components)?;
    validate_thresholds(&evidence.threshold_decision)?;
    validate_process(
        &evidence.worker_process,
        evidence.configured.worker_rss_limit_bytes,
        "performance worker",
    )?;
    if evidence.worker_process.elapsed_millis == 0
        || evidence.worker_process.elapsed_millis > evidence.configured.worker_timeout_millis
        || !same_rss_observation(
            &evidence.worker_process.process_tree_rss_observation,
            &evidence
                .memory
                .supervised_worker_process_tree_rss_observation,
        )
    {
        bail!("performance worker process evidence is inconsistent with its fixed supervision");
    }
    validate_rss_observation(
        &evidence
            .memory
            .supervised_worker_process_tree_rss_observation,
        evidence.configured.worker_rss_limit_bytes,
        "performance worker",
    )?;
    validate_memory(&evidence.memory)?;
    validate_integrated_live_path(&evidence.integrated_live_path)?;
    validate_analytical_storage(&evidence.analytical_storage)?;
    validate_time_order(
        &evidence.worker_started_at,
        &evidence.worker_completed_at,
        "performance worker",
    )?;
    validate_time_order(&evidence.started_at, &evidence.completed_at, "performance")
}

fn validate_bound_files(
    evidence: &RecordedPerformanceEvidence,
    repository: &RepositoryIdentity,
    binary: &StableFileIdentity,
) -> Result<()> {
    let cargo_lock = hash_stable_file(&repository.root().join("Cargo.lock"), MAXIMUM_INPUT_BYTES)?;
    validate_binary(&evidence.cargo_lock, &cargo_lock, "performance Cargo lock")?;
    let rust_toolchain = hash_stable_file(
        &repository.root().join("rust-toolchain.toml"),
        MAXIMUM_INPUT_BYTES,
    )?;
    validate_binary(
        &evidence.rust_toolchain_file,
        &rust_toolchain,
        "performance Rust toolchain",
    )?;
    let onnx_worker = binary.canonical_path().with_file_name(if cfg!(windows) {
        "market-squawk-onnx-worker.exe"
    } else {
        "market-squawk-onnx-worker"
    });
    let onnx_worker = hash_stable_file(&onnx_worker, MAXIMUM_EXECUTABLE_BYTES)?;
    validate_binary(
        &evidence.onnx_worker_binary,
        &onnx_worker,
        "performance ONNX worker",
    )
}

fn validate_host(host: &RecordedHostEvidence) -> Result<()> {
    if host.operating_system.is_empty()
        || host.kernel.is_empty()
        || host.architecture != std::env::consts::ARCH
        || host.logical_cpus == 0
        || host.cpu_model.is_empty()
        || host.physical_memory_bytes == 0
        || host.load_state.is_empty()
        || host.power_state.is_empty()
        || host.thermal_state.is_empty()
    {
        bail!("performance host evidence is incomplete or from a different architecture");
    }
    Ok(())
}

fn validate_toolchain(toolchain: &RecordedToolchainEvidence) -> Result<()> {
    if toolchain.stable_release_required != "1.97.1"
        || !toolchain.rustc_verbose.lines().next().is_some_and(|line| {
            line == "rustc 1.97.1 (stable)" || line.starts_with("rustc 1.97.1 ")
        })
        || !toolchain.cargo_version.starts_with("cargo 1.97.1 ")
    {
        bail!("performance evidence does not bind stable Rust and Cargo 1.97.1");
    }
    Ok(())
}

fn validate_fixtures(
    fixtures: &RecordedFixtureEvidence,
    repository: &RepositoryIdentity,
    onnx_worker: &StableFileIdentity,
) -> Result<()> {
    let kraken = hash_stable_file(
        &repository
            .root()
            .join("adapters/market-squawk-adapter-kraken/fixtures/official_book_checksum.json"),
        MAXIMUM_FIXTURE_BYTES,
    )?;
    let manifest_path = repository
        .root()
        .join("crates/market-squawk-modeling/fixtures/onnx/manifest.json");
    let manifest: OnnxFixtureManifest =
        serde_json::from_slice(&read_stable_bytes(&manifest_path, MAXIMUM_FIXTURE_BYTES)?)
            .context("ONNX performance fixture manifest is invalid")?;
    let [model] = manifest.models.as_slice() else {
        bail!("ONNX performance fixture manifest does not contain one model");
    };
    if manifest.schema_version != 1
        || model.id != "bounded-gemm-v1"
        || model.opset != 13
        || model.input_shape != [1, 2]
        || model.output_shape != [1, 1]
        || !valid_sha256(&model.artifact_sha256)
        || sha256_bytes(&decode_hex(&model.model_hex)?) != model.artifact_sha256
        || fixtures.kraken_snapshot_sha256 != kraken.sha256
        || fixtures.kraken_expected_checksum != EXACT_KRAKEN_CHECKSUM
        || fixtures.native_artifact_sha256
            != sha256_bytes(b"market-squawk/release-evidence/native-linear/v1")
        || fixtures.onnx_artifact_sha256 != model.artifact_sha256
        || fixtures.onnx_worker_sha256 != onnx_worker.sha256
        || !valid_sha256(&fixtures.onnx_policy_sha256)
        || !valid_sha256(&fixtures.onnx_runtime_semantics_sha256)
        || !valid_sha256(&fixtures.onnx_warm_up_sha256)
        || fixtures.native_backend_retained_bytes == 0
        || fixtures.native_backend_retained_bytes > MAXIMUM_BACKEND_RETAINED_BYTES
        || fixtures.onnx_backend_retained_bytes == 0
        || fixtures.onnx_backend_retained_bytes > MAXIMUM_BACKEND_RETAINED_BYTES
    {
        bail!("performance fixture identities or retained bounds are invalid");
    }
    Ok(())
}

fn validate_performance_configuration(configured: &RecordedPerformanceConfiguration) -> Result<()> {
    if configured.warm_up_events != EXACT_WARM_UP_EVENTS
        || configured.measured_events != EXACT_MEASURED_EVENTS
        || configured.storage_rows != EXACT_STORAGE_ROWS
        || configured.minimum_events_per_second != EXACT_MINIMUM_EVENTS_PER_SECOND
        || configured.maximum_warmed_p99_nanos != EXACT_MAXIMUM_WARMED_P99_NANOS
        || configured.maximum_tail_growth_bytes != EXACT_MAXIMUM_TAIL_GROWTH_BYTES
        || configured.maximum_tail_growth_percent != EXACT_MAXIMUM_TAIL_GROWTH_PERCENT
        || configured.worker_timeout_millis != EXACT_WORKER_TIMEOUT_MILLIS
        || configured.worker_rss_limit_bytes != EXACT_WORKER_RSS_BYTES
    {
        bail!("performance evidence does not bind the fixed acceptance workload");
    }
    Ok(())
}

fn validate_worker_binding(
    binding: &RecordedWorkerBinding,
    repository: &RepositoryIdentity,
    binary: &StableFileIdentity,
) -> Result<()> {
    if binding.authority != RecordedEvidenceAuthority::ExactHead
        || binding.repository_head != repository.head
        || binding.repository_tree != repository.tree
        || !binding.repository_clean
        || binding.expected_head.as_deref() != Some(repository.head.as_str())
        || binding.expected_tree.as_deref() != Some(repository.tree.as_str())
        || binding.warm_up_events != EXACT_WARM_UP_EVENTS
        || binding.measured_events != EXACT_MEASURED_EVENTS
        || binding.storage_rows != EXACT_STORAGE_ROWS
        || binding.maximum_tail_growth_mib != 32
        || binding.maximum_tail_growth_percent != EXACT_MAXIMUM_TAIL_GROWTH_PERCENT
        || binding.minimum_events_per_second != EXACT_MINIMUM_EVENTS_PER_SECOND
        || binding.maximum_warmed_p99_nanos != EXACT_MAXIMUM_WARMED_P99_NANOS
        || binding.requested_output.as_os_str().is_empty()
        || !valid_sha256(&binding.effective_config_sha256)
        || !valid_sha256(&binding.effective_environment_sha256)
        || binding.executable_sha256 != binary.sha256
        || binding.executable_bytes != binary.byte_count
        || !valid_sha256(&binding.argv_sha256)
        || binding.argv_count == 0
        || binding.supervisor_timeout_millis != EXACT_WORKER_TIMEOUT_MILLIS
        || binding.supervisor_rss_bytes != EXACT_WORKER_RSS_BYTES
        || binding.supervisor_process_tree_rss_configured_poll_sleep_millis
            != process_tree_rss_poll_sleep_millis()
    {
        bail!("performance worker binding is not the exact admitted release request");
    }
    Ok(())
}

fn validate_components(components: &RecordedComponentEvidence) -> Result<()> {
    for distribution in [
        &components.kraken_decoder_and_checksum,
        &components.sequence_validation,
        &components.checksum_canonicalization,
        &components.bounded_queue_push.latency,
        &components.order_book_update,
        &components.online_feature_update,
        &components.native_inference,
    ] {
        validate_latency_distribution(distribution, EXACT_COMPONENT_OPERATIONS, false)?;
    }
    validate_latency_distribution(
        &components.onnx_inference,
        EXACT_ONNX_COMPONENT_OPERATIONS,
        false,
    )?;
    let queue = &components.bounded_queue_push;
    let accounting_valid = match (
        queue.transport.as_str(),
        queue.private_storage_accounting.as_str(),
    ) {
        ("standard_sync_channel", "not_measured") => {
            queue.queue_private_storage_bytes.is_none()
                && queue.fixed_capture_bytes.is_none()
                && queue.total_accounted_bytes.is_none()
        }
        ("candidate_fixed_ring", "exact") => {
            matches!(
                (
                    queue.queue_private_storage_bytes,
                    queue.fixed_capture_bytes,
                    queue.total_accounted_bytes,
                ),
                (Some(private), Some(fixed), Some(total))
                    if private > 0 && fixed >= private && total >= fixed
            )
        }
        _ => false,
    };
    if queue.configured_depth != 64
        || queue.effective_capacity == 0
        || queue.effective_capacity > queue.configured_depth
        || queue.accepted != usize::try_from(EXACT_COMPONENT_OPERATIONS)?
        || queue.consumed != queue.accepted
        || queue.queued_bytes_after_drain != 0
        || queue.accounting_invariant_failures != 0
        || !queue.real_full_refusal_verified
        || !accounting_valid
    {
        bail!("performance bounded-queue component evidence is invalid");
    }
    Ok(())
}

fn validate_thresholds(decision: &RecordedThresholdDecision) -> Result<()> {
    if !decision.passed
        || !decision.live_throughput_passed
        || !decision.live_p99_passed
        || !decision.live_tail_growth_passed
        || !decision.live_queue_bound_passed
        || !decision.storage_completed
        || !decision.worker_process_completed
        || !decision.worker_process_observed_rss_passed
    {
        bail!("performance evidence contains a failed threshold decision");
    }
    Ok(())
}

fn validate_memory(memory: &RecordedMemoryEvidence) -> Result<()> {
    let warm_rss = validate_current_rss(
        &memory.warm_window_current_process_rss_observation,
        "performance warm window",
    )?;
    let post_rss = validate_current_rss(
        &memory.post_measurement_window_current_process_rss_observation,
        "performance post-measurement window",
    )?;
    let expected_tail_growth = post_rss.saturating_sub(warm_rss);
    let percentage_bound = warm_rss
        .checked_mul(EXACT_MAXIMUM_TAIL_GROWTH_PERCENT)
        .and_then(|value| value.checked_div(100))
        .context("performance percentage memory bound overflowed")?;
    let expected_permitted = EXACT_MAXIMUM_TAIL_GROWTH_BYTES.max(percentage_bound);
    if memory.tail_growth_bytes != expected_tail_growth
        || memory.permitted_tail_growth_bytes != expected_permitted
        || memory.tail_growth_bytes > memory.permitted_tail_growth_bytes
    {
        bail!("performance memory evidence exceeds its admitted tail-growth bound");
    }
    Ok(())
}

fn same_rss_observation(
    left: &ProcessTreeRssObservation,
    right: &ProcessTreeRssObservation,
) -> bool {
    left.observed_maximum_rss_bytes == right.observed_maximum_rss_bytes
        && left.successful_sample_count == right.successful_sample_count
        && left.observation_window_millis == right.observation_window_millis
        && left.configured_poll_sleep_millis == right.configured_poll_sleep_millis
}

fn validate_integrated_live_path(live: &RecordedIntegratedLivePath) -> Result<()> {
    validate_measured_outcomes(&live.measured_outcomes)?;
    validate_latency_distribution(&live.strategy_decision, EXACT_MEASURED_EVENTS, false)?;
    validate_latency_distribution(
        &live.complete_action_disposition,
        EXACT_MEASURED_EVENTS,
        true,
    )?;
    if live.latency_boundary != "bounded_ingress_attempt_to_observed_strategy_and_action_completion"
        || live.fixture_scope != "sealed_feature_gated_diagnostic_source_not_provider_qualification"
        || live.production_path
            != "live_actor_to_strategy_to_central_risk_to_dispatcher_to_realistic_paper_adapter"
        || live.event_count != EXACT_MEASURED_EVENTS
        || live.dispatch_strategy_decision_nanos == 0
        || live.dispatch_action_disposition_nanos == 0
        || live.event_to_observed_paper_terminal_nanos == 0
        || live.dispatch_disposition != "dispatched"
        || live.paper_terminal_state != "filled"
        || live.paper_order_count != 1
        || live.paper_fill_count == 0
        || live.mailbox_capacity == 0
        || live.producer_observed_maximum_in_flight_batches > live.mailbox_capacity
        || live.observer_retained_bytes == 0
        || !live.shutdown_complete
    {
        bail!("performance evidence does not prove the complete bounded live path");
    }
    Ok(())
}

fn validate_measured_outcomes(outcomes: &RecordedMeasuredOutcomes) -> Result<()> {
    if outcomes.expected_events != EXACT_MEASURED_EVENTS
        || outcomes.strategy_successful != EXACT_MEASURED_EVENTS
        || outcomes.strategy_failed != 0
        || outcomes.strategy_unobserved != 0
        || outcomes.action_no_action != EXACT_MEASURED_EVENTS
        || outcomes.action_suppressed != 0
        || outcomes.action_dispatched != 0
        || outcomes.action_failed != 0
        || outcomes.action_unobserved != 0
    {
        bail!("performance evidence contains incomplete measured live outcomes");
    }
    Ok(())
}

fn validate_latency_distribution(
    distribution: &RecordedLatencyDistribution,
    expected_operations: u64,
    enforce_acceptance: bool,
) -> Result<()> {
    let expected_throughput = rate(distribution.operations, distribution.elapsed_nanos)?;
    if distribution.operations != expected_operations
        || distribution.elapsed_nanos == 0
        || distribution.operations_per_second != expected_throughput
        || distribution.p50_nanos > distribution.p95_nanos
        || distribution.p95_nanos > distribution.p99_nanos
        || distribution.p99_nanos > distribution.maximum_nanos
        || distribution.maximum_nanos > distribution.elapsed_nanos
        || (enforce_acceptance
            && (distribution.operations_per_second < EXACT_MINIMUM_EVENTS_PER_SECOND
                || distribution.p99_nanos > EXACT_MAXIMUM_WARMED_P99_NANOS))
    {
        bail!("performance evidence contains an invalid latency distribution");
    }
    Ok(())
}

fn validate_current_rss(observation: &RecordedCurrentRssObservation, label: &str) -> Result<u64> {
    if observation.successful_sample_count != EXACT_CURRENT_RSS_SAMPLES
        || observation.observation_window_millis == 0
        || observation.configured_poll_sleep_millis != EXACT_CURRENT_RSS_POLL_MILLIS
    {
        bail!("{label} RSS evidence does not satisfy its sampling contract");
    }
    observation
        .observed_maximum_rss_bytes
        .filter(|bytes| *bytes > 0)
        .with_context(|| format!("{label} RSS evidence contains no positive sample"))
}

fn validate_analytical_storage(storage: &RecordedAnalyticalStorage) -> Result<()> {
    if storage.requested_rows != EXACT_STORAGE_ROWS
        || storage.physical_rows_per_object != 4_096
        || storage.python_selected_rows_per_verification != 48_000
    {
        bail!("performance analytical-storage evidence violates the fixed workload");
    }
    let expected_measured_rows = storage
        .requested_rows
        .div_ceil(storage.physical_rows_per_object)
        .checked_mul(storage.physical_rows_per_object)
        .context("performance analytical-storage measured-row count overflowed")?;
    let repeated_operations = expected_measured_rows
        .checked_div(storage.physical_rows_per_object)
        .context("performance analytical-storage operation count is invalid")?;
    let query_operations = repeated_operations.min(64).max(8.min(repeated_operations));
    let query_rows = query_operations
        .checked_mul(storage.physical_rows_per_object)
        .context("performance analytical-storage query-row count overflowed")?;
    let python_operations = storage
        .requested_rows
        .div_ceil(storage.python_selected_rows_per_verification);
    let expected_python_rows = python_operations
        .checked_mul(storage.python_selected_rows_per_verification)
        .context("performance Python verified-row count overflowed")?;
    if storage.measured_rows != expected_measured_rows
        || storage.unique_parquet_objects != 1
        || storage.parquet_size_bytes == 0
        || storage.point_in_time_selected_rows != storage.physical_rows_per_object
        || storage.point_in_time_retained_bytes == 0
        || storage.python_verified_rows != expected_python_rows
    {
        bail!("performance analytical-storage evidence violates the fixed workload");
    }
    for digest in [
        &storage.parquet_content_sha256,
        &storage.point_in_time_content_sha256,
        &storage.point_in_time_audit_sha256,
        &storage.python_export_sha256,
        &storage.python_catalog_identity,
        &storage.python_selection_sha256,
    ] {
        if digest.iter().all(|byte| *byte == 0) {
            bail!("performance analytical-storage evidence contains a zero identity");
        }
    }
    for operation in [
        &storage.arrow_conversion,
        &storage.parquet_publication,
        &storage.parquet_read,
    ] {
        validate_storage_latency(operation, repeated_operations, expected_measured_rows)?;
    }
    validate_storage_latency(&storage.datafusion_query, query_operations, query_rows)?;
    validate_storage_latency(
        &storage.python_dataset_admission_revalidation,
        python_operations,
        expected_python_rows,
    )?;
    Ok(())
}

fn validate_storage_latency(
    distribution: &RecordedStorageLatency,
    expected_operations: u64,
    expected_rows: u64,
) -> Result<()> {
    let expected_rate = rate(distribution.rows, distribution.elapsed_nanos)?;
    if distribution.operations != expected_operations
        || distribution.rows != expected_rows
        || distribution.elapsed_nanos == 0
        || distribution.rows_per_second != expected_rate
        || distribution.p50_nanos > distribution.p95_nanos
        || distribution.p95_nanos > distribution.p99_nanos
        || distribution.p99_nanos > distribution.maximum_nanos
        || distribution.maximum_nanos > distribution.elapsed_nanos
    {
        bail!("performance storage operation evidence is invalid");
    }
    Ok(())
}

fn rate(quantity: u64, elapsed_nanos: u64) -> Result<u64> {
    if quantity == 0 || elapsed_nanos == 0 {
        bail!("performance rate evidence contains a zero quantity or duration");
    }
    let value = u64::try_from(
        u128::from(quantity)
            .checked_mul(1_000_000_000)
            .and_then(|value| value.checked_div(u128::from(elapsed_nanos)))
            .context("performance rate evidence overflowed")?,
    )
    .context("performance rate evidence exceeds its representation")?;
    if value == 0 {
        bail!("performance rate evidence rounds to zero");
    }
    Ok(value)
}

fn decode_hex(value: &str) -> Result<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        bail!("ONNX performance fixture hex length is invalid");
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let pair =
                std::str::from_utf8(pair).context("ONNX performance fixture hex is not UTF-8")?;
            u8::from_str_radix(pair, 16).context("ONNX performance fixture hex is invalid")
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{RecordedThresholdDecision, validate_thresholds};

    #[test]
    fn closing_contract_rejects_failed_performance_threshold() {
        let failed = RecordedThresholdDecision {
            passed: true,
            live_throughput_passed: true,
            live_p99_passed: false,
            live_tail_growth_passed: true,
            live_queue_bound_passed: true,
            storage_completed: true,
            worker_process_completed: true,
            worker_process_observed_rss_passed: true,
        };
        assert!(validate_thresholds(&failed).is_err());
    }
}
