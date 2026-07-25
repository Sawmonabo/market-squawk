//! Exact, threshold-enforced release-performance evidence.

mod components;
mod host;
mod worker;

use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::{Context as _, Result, bail};
use chrono::{SecondsFormat, Utc};
use market_squawk_data::{ReleaseEvidenceStorageResult, run_release_evidence_storage};
use market_squawk_modeling::{ReleaseEvidenceInferenceFixture, ReleaseEvidenceInferenceIdentity};
use market_squawk_platform::{ConfigOverrides, ConfigSources};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use super::identity::RepositoryIdentity;
use super::io::{
    PublishedReport, StableFileIdentity, hash_stable_file, hex_digest,
    publish_report_with_identity_barrier, sha256_bytes,
};
use super::process::{ProcessEvidence, process_tree_rss_sample_interval_millis};
use crate::AppConfig;
use crate::cli::ReleaseBenchmarkArguments;
use crate::paper_bot::{ReleasePaperBotBenchmarkComposition, ReleasePaperBotBenchmarkResult};

const REPORT_KIND: &str = "market_squawk.release.performance";
const MAXIMUM_EXECUTABLE_BYTES: u64 = 1024 * 1024 * 1024;
const MAXIMUM_INPUT_BYTES: u64 = 16 * 1024 * 1024;
const MAXIMUM_WARM_UP_EVENTS: u64 = 100_000_000;
const MAXIMUM_EVENTS: u64 = 1_000_000_000;
const MAXIMUM_STORAGE_ROWS: u64 = 100_000_000;
const EXACT_WARM_UP_EVENTS: u64 = 1_000_000;
const EXACT_EVENTS: u64 = 60_000_000;
const EXACT_STORAGE_ROWS: u64 = 10_000_000;
const MAXIMUM_TAIL_GROWTH_MIB: u64 = 32;
const MAXIMUM_TAIL_GROWTH_PERCENT: u64 = 1;

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct PerformanceEvidence {
    repository: RepositoryIdentity,
    evidence_authority: EvidenceAuthority,
    application_binary: StableFileIdentity,
    worker_binding: worker::WorkerBinding,
    onnx_worker_binary: StableFileIdentity,
    cargo_lock: StableFileIdentity,
    rust_toolchain_file: StableFileIdentity,
    application_version: String,
    compiled_features: Vec<String>,
    host: host::HostEvidence,
    toolchain: host::ToolchainEvidence,
    fixtures: FixtureEvidence,
    configured: ConfiguredEvidence,
    components: components::ComponentEvidence,
    integrated_live_path: IntegratedLiveEvidence,
    analytical_storage: ReleaseEvidenceStorageResult,
    memory: MemoryEvidence,
    worker_process: ProcessEvidence,
    threshold_decision: ThresholdDecision,
    worker_started_at: String,
    worker_completed_at: String,
    started_at: String,
    completed_at: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum EvidenceAuthority {
    Provisional,
    ExactHead,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FixtureEvidence {
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

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ConfiguredEvidence {
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

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct IntegratedLiveEvidence {
    latency_boundary: String,
    fixture_scope: String,
    production_path: String,
    event_count: u64,
    measured_outcomes: crate::paper_bot::ReleaseMeasuredOutcomeLedger,
    strategy_decision: crate::paper_bot::ReleaseLatencyDistribution,
    complete_action_disposition: crate::paper_bot::ReleaseLatencyDistribution,
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

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct MemoryEvidence {
    warm_plateau_rss_bytes: u64,
    post_measurement_rss_bytes: u64,
    observed_peak_process_tree_rss_bytes: u64,
    tail_growth_bytes: u64,
    permitted_tail_growth_bytes: u64,
    current_process_plateau_sample_interval_millis: u64,
    process_tree_rss_sample_interval_millis: u64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WorkerMemoryEvidence {
    warm_plateau_rss_bytes: u64,
    post_measurement_rss_bytes: u64,
    tail_growth_bytes: u64,
    permitted_tail_growth_bytes: u64,
    current_process_plateau_sample_interval_millis: u64,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct ThresholdDecision {
    passed: bool,
    live_throughput_passed: bool,
    live_p99_passed: bool,
    live_tail_growth_passed: bool,
    live_queue_bound_passed: bool,
    storage_completed: bool,
    worker_process_completed: bool,
    worker_process_observed_rss_passed: bool,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WorkerThresholdDecision {
    passed: bool,
    live_throughput_passed: bool,
    live_p99_passed: bool,
    live_tail_growth_passed: bool,
    live_queue_bound_passed: bool,
    storage_completed: bool,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WorkerMeasurements {
    onnx_worker_binary: StableFileIdentity,
    cargo_lock: StableFileIdentity,
    rust_toolchain_file: StableFileIdentity,
    application_version: String,
    compiled_features: Vec<String>,
    host: host::HostEvidence,
    toolchain: host::ToolchainEvidence,
    fixtures: FixtureEvidence,
    configured: ConfiguredEvidence,
    components: components::ComponentEvidence,
    integrated_live_path: IntegratedLiveEvidence,
    analytical_storage: ReleaseEvidenceStorageResult,
    memory: WorkerMemoryEvidence,
    threshold_decision: WorkerThresholdDecision,
    started_at: String,
    completed_at: String,
}

pub(super) async fn run(
    config: AppConfig,
    arguments: ReleaseBenchmarkArguments,
) -> Result<serde_json::Value> {
    let authority = validate_arguments(&arguments)?;
    let repository = RepositoryIdentity::admit(&arguments.repository)?;
    let started_at = now();
    let executable = std::env::current_exe().context("running executable path is unavailable")?;
    let application_binary = hash_stable_file(&executable, MAXIMUM_EXECUTABLE_BYTES)?;
    let child_arguments = worker::child_arguments()?;
    let expected_binding = worker::WorkerBinding::capture(
        authority,
        &repository,
        &arguments,
        &config,
        &application_binary,
        &child_arguments,
    )?;
    let supervised = worker::supervise(
        &executable,
        &repository,
        &child_arguments,
        &expected_binding,
    )?;
    let final_binary = hash_stable_file(&executable, MAXIMUM_EXECUTABLE_BYTES)?;
    if final_binary != application_binary {
        bail!("release benchmark executable changed while its worker was supervised");
    }
    repository.verify_unchanged()?;
    let WorkerMeasurements {
        onnx_worker_binary,
        cargo_lock,
        rust_toolchain_file,
        application_version,
        compiled_features,
        host,
        toolchain,
        fixtures,
        configured,
        components,
        integrated_live_path,
        analytical_storage,
        memory,
        threshold_decision,
        started_at: worker_started_at,
        completed_at: worker_completed_at,
    } = supervised.measurements;
    if !threshold_decision.passed {
        bail!("release benchmark worker returned a failed threshold decision");
    }
    let process_completed = supervised.process.exit_code == 0;
    let process_observed_rss_passed =
        supervised.process.peak_process_tree_rss_bytes <= worker::supervisor_rss_bytes();
    if !process_completed || !process_observed_rss_passed {
        bail!("release benchmark worker process did not satisfy its fixed observed limits");
    }
    let payload = PerformanceEvidence {
        repository,
        evidence_authority: authority,
        application_binary,
        worker_binding: supervised.binding,
        onnx_worker_binary,
        cargo_lock,
        rust_toolchain_file,
        application_version,
        compiled_features,
        host,
        toolchain,
        fixtures,
        configured,
        components,
        integrated_live_path,
        analytical_storage,
        memory: MemoryEvidence {
            warm_plateau_rss_bytes: memory.warm_plateau_rss_bytes,
            post_measurement_rss_bytes: memory.post_measurement_rss_bytes,
            observed_peak_process_tree_rss_bytes: supervised.process.peak_process_tree_rss_bytes,
            tail_growth_bytes: memory.tail_growth_bytes,
            permitted_tail_growth_bytes: memory.permitted_tail_growth_bytes,
            current_process_plateau_sample_interval_millis: memory
                .current_process_plateau_sample_interval_millis,
            process_tree_rss_sample_interval_millis: process_tree_rss_sample_interval_millis(),
        },
        worker_process: supervised.process,
        threshold_decision: ThresholdDecision {
            passed: threshold_decision.passed && process_completed && process_observed_rss_passed,
            live_throughput_passed: threshold_decision.live_throughput_passed,
            live_p99_passed: threshold_decision.live_p99_passed,
            live_tail_growth_passed: threshold_decision.live_tail_growth_passed,
            live_queue_bound_passed: threshold_decision.live_queue_bound_passed,
            storage_completed: threshold_decision.storage_completed,
            worker_process_completed: process_completed,
            worker_process_observed_rss_passed: process_observed_rss_passed,
        },
        worker_started_at,
        worker_completed_at,
        started_at,
        completed_at: now(),
    };
    let published =
        publish_report_with_identity_barrier(&arguments.output, REPORT_KIND, &payload, || {
            let binary = hash_stable_file(&executable, MAXIMUM_EXECUTABLE_BYTES)?;
            if binary != payload.application_binary {
                bail!("release benchmark executable changed at the publication barrier");
            }
            payload.repository.verify_unchanged()
        })?;
    Ok(publication_value(&published))
}

pub(super) async fn run_worker(
    config: AppConfig,
    arguments: ReleaseBenchmarkArguments,
) -> Result<serde_json::Value> {
    let authority = validate_arguments(&arguments)?;
    let repository = RepositoryIdentity::admit(&arguments.repository)?;
    let executable = std::env::current_exe().context("running executable path is unavailable")?;
    let application_binary = hash_stable_file(&executable, MAXIMUM_EXECUTABLE_BYTES)?;
    let current_arguments = worker::current_arguments();
    let binding = worker::WorkerBinding::capture(
        authority,
        &repository,
        &arguments,
        &config,
        &application_binary,
        &current_arguments,
    )?;
    let measurements =
        collect_worker_measurements(config, &arguments, authority, &repository).await?;
    let final_binary = hash_stable_file(&executable, MAXIMUM_EXECUTABLE_BYTES)?;
    if final_binary != application_binary {
        bail!("release benchmark worker executable changed during measurement");
    }
    repository.verify_unchanged()?;
    worker::canonical_value(&worker::WorkerEnvelope::new(binding, measurements))
}

async fn collect_worker_measurements(
    config: AppConfig,
    arguments: &ReleaseBenchmarkArguments,
    authority: EvidenceAuthority,
    repository: &RepositoryIdentity,
) -> Result<WorkerMeasurements> {
    let started_at = now();
    let worker_path = onnx_worker_path()?;
    let onnx_worker_binary = hash_stable_file(&worker_path, MAXIMUM_EXECUTABLE_BYTES)?;
    let worker_digest = decode_digest(&onnx_worker_binary.sha256)?;
    let cargo_lock = hash_stable_file(&repository.root().join("Cargo.lock"), MAXIMUM_INPUT_BYTES)?;
    let rust_toolchain_file = hash_stable_file(
        &repository.root().join("rust-toolchain.toml"),
        MAXIMUM_INPUT_BYTES,
    )?;
    let kraken_file = hash_stable_file(
        &repository
            .root()
            .join("adapters/market-squawk-adapter-kraken/fixtures/official_book_checksum.json"),
        MAXIMUM_INPUT_BYTES,
    )?;
    if kraken_file.sha256 != sha256_bytes(components::kraken_fixture()) {
        bail!("compiled Kraken benchmark fixture differs from the repository fixture");
    }
    let host_evidence = host::host_evidence()?;
    let toolchain = host::toolchain_evidence(authority)?;

    let mut inference = ReleaseEvidenceInferenceFixture::try_new(&worker_path, worker_digest)
        .context("production inference fixture admission failed")?;
    let inference_identity = inference.identity();
    let component_evidence = components::measure_all(&mut inference, arguments.events)?;
    drop(inference);

    let live_scratch = tempfile::Builder::new()
        .prefix("market-squawk-release-live-")
        .tempdir()
        .context("live benchmark scratch directory could not be created")?;
    let isolated_config = isolated_config(config, live_scratch.path().join("data"))?;
    let cancellation = CancellationToken::new();
    let mut live = ReleasePaperBotBenchmarkComposition::try_new(isolated_config)?
        .start(cancellation)
        .await
        .context("production paper-bot benchmark failed to start")?;
    let measurement = async {
        live.warm_up(arguments.warm_up_events).await?;
        let warm_plateau = host::rss_plateau()?;
        live.measure(arguments.events).await?;
        let post_measurement = host::rss_plateau()?;
        Ok::<_, anyhow::Error>((warm_plateau, post_measurement))
    }
    .await;
    let close = live.finish().await;
    let ((warm_plateau, post_measurement), live_result) =
        reconcile_live_measurement(measurement, close)?;
    drop(live_scratch);
    let integrated_live_path = integrated_live(live_result);

    let storage_scratch = tempfile::Builder::new()
        .prefix("market-squawk-release-storage-")
        .tempdir()
        .context("analytical benchmark scratch directory could not be created")?;
    let analytical_storage =
        run_release_evidence_storage(storage_scratch.path(), arguments.storage_rows)
            .await
            .context("production analytical-storage measurement failed")?;
    drop(storage_scratch);

    let tail_growth = post_measurement.saturating_sub(warm_plateau);
    let configured_tail_bytes = arguments
        .max_tail_growth_mib
        .checked_mul(1024 * 1024)
        .context("tail-growth byte limit overflow")?;
    let percentage_tail_bytes = warm_plateau
        .checked_mul(arguments.max_tail_growth_percent)
        .and_then(|value| value.checked_div(100))
        .context("tail-growth percentage limit overflow")?;
    let permitted_tail_growth = configured_tail_bytes.max(percentage_tail_bytes);
    let threshold_decision = worker_threshold_decision(
        &integrated_live_path,
        tail_growth,
        permitted_tail_growth,
        arguments,
    );
    if !threshold_decision.passed {
        bail!("release-performance thresholds were not satisfied");
    }

    Ok(WorkerMeasurements {
        onnx_worker_binary,
        cargo_lock,
        rust_toolchain_file,
        application_version: env!("CARGO_PKG_VERSION").to_owned(),
        compiled_features: vec!["release-evidence".to_owned()],
        host: host_evidence,
        toolchain,
        fixtures: fixture_evidence(inference_identity),
        configured: ConfiguredEvidence {
            warm_up_events: arguments.warm_up_events,
            measured_events: arguments.events,
            storage_rows: arguments.storage_rows,
            minimum_events_per_second: arguments.min_events_per_second,
            maximum_warmed_p99_nanos: arguments.max_warmed_p99_ns,
            maximum_tail_growth_bytes: configured_tail_bytes,
            maximum_tail_growth_percent: arguments.max_tail_growth_percent,
            worker_timeout_millis: worker::supervisor_timeout_millis(),
            worker_rss_limit_bytes: worker::supervisor_rss_bytes(),
        },
        components: component_evidence,
        integrated_live_path,
        analytical_storage,
        memory: WorkerMemoryEvidence {
            warm_plateau_rss_bytes: warm_plateau,
            post_measurement_rss_bytes: post_measurement,
            tail_growth_bytes: tail_growth,
            permitted_tail_growth_bytes: permitted_tail_growth,
            current_process_plateau_sample_interval_millis: host::memory_sample_interval_millis(),
        },
        threshold_decision,
        started_at,
        completed_at: now(),
    })
}

fn validate_arguments(arguments: &ReleaseBenchmarkArguments) -> Result<EvidenceAuthority> {
    if arguments.warm_up_events == 0
        || arguments.warm_up_events > MAXIMUM_WARM_UP_EVENTS
        || arguments.events == 0
        || arguments.events > MAXIMUM_EVENTS
        || arguments.storage_rows == 0
        || arguments.storage_rows > MAXIMUM_STORAGE_ROWS
        || arguments.min_events_per_second == 0
        || arguments.max_warmed_p99_ns == 0
        || arguments.max_warmed_p99_ns >= 1_000_000
        || arguments.max_tail_growth_mib == 0
        || arguments.max_tail_growth_percent == 0
    {
        bail!("release-performance arguments are outside their fixed bounds");
    }
    let exact = arguments.repository.head.is_some() && arguments.repository.tree.is_some();
    if exact
        && (arguments.warm_up_events < EXACT_WARM_UP_EVENTS
            || arguments.events < EXACT_EVENTS
            || arguments.storage_rows < EXACT_STORAGE_ROWS
            || arguments.min_events_per_second < 100_000
            || arguments.max_tail_growth_mib > MAXIMUM_TAIL_GROWTH_MIB
            || arguments.max_tail_growth_percent > MAXIMUM_TAIL_GROWTH_PERCENT)
    {
        bail!("exact-head release performance requires the full acceptance workload and limits");
    }
    Ok(if exact {
        EvidenceAuthority::ExactHead
    } else {
        EvidenceAuthority::Provisional
    })
}

fn integrated_live(result: ReleasePaperBotBenchmarkResult) -> IntegratedLiveEvidence {
    IntegratedLiveEvidence {
        latency_boundary: "bounded_ingress_attempt_to_observed_strategy_and_action_completion"
            .to_owned(),
        fixture_scope: "sealed_feature_gated_diagnostic_source_not_provider_qualification"
            .to_owned(),
        production_path:
            "live_actor_to_strategy_to_central_risk_to_dispatcher_to_realistic_paper_adapter"
                .to_owned(),
        event_count: result.event_count,
        measured_outcomes: result.measured_outcomes,
        strategy_decision: result.strategy_decision,
        complete_action_disposition: result.complete_action_disposition,
        dispatch_strategy_decision_nanos: result.dispatch_strategy_decision_nanos,
        dispatch_action_disposition_nanos: result.dispatch_action_disposition_nanos,
        event_to_observed_paper_terminal_nanos: result.event_to_observed_paper_terminal_nanos,
        dispatch_disposition: result.dispatch_disposition,
        paper_terminal_state: result.paper_terminal_state,
        paper_order_count: result.paper_order_count,
        paper_fill_count: result.paper_fill_count,
        mailbox_capacity: result.mailbox_capacity,
        producer_observed_maximum_in_flight_batches: result
            .producer_observed_maximum_in_flight_batches,
        observer_retained_bytes: result.observer_retained_bytes,
        shutdown_complete: result.shutdown_complete,
    }
}

fn worker_threshold_decision(
    live: &IntegratedLiveEvidence,
    tail_growth: u64,
    permitted_tail_growth: u64,
    arguments: &ReleaseBenchmarkArguments,
) -> WorkerThresholdDecision {
    let throughput =
        live.complete_action_disposition.operations_per_second >= arguments.min_events_per_second;
    let p99 = live.complete_action_disposition.p99_nanos <= arguments.max_warmed_p99_ns;
    let tail = tail_growth <= permitted_tail_growth;
    let queue = live.producer_observed_maximum_in_flight_batches <= live.mailbox_capacity;
    WorkerThresholdDecision {
        passed: throughput && p99 && tail && queue,
        live_throughput_passed: throughput,
        live_p99_passed: p99,
        live_tail_growth_passed: tail,
        live_queue_bound_passed: queue,
        storage_completed: true,
    }
}

fn fixture_evidence(identity: ReleaseEvidenceInferenceIdentity) -> FixtureEvidence {
    FixtureEvidence {
        kraken_snapshot_sha256: sha256_bytes(components::kraken_fixture()),
        kraken_expected_checksum: components::KRAKEN_CHECKSUM,
        native_artifact_sha256: hex_digest(identity.native_artifact_digest()),
        onnx_artifact_sha256: hex_digest(identity.onnx_artifact_digest()),
        onnx_policy_sha256: hex_digest(identity.onnx_policy_digest()),
        onnx_worker_sha256: hex_digest(identity.onnx_worker_digest()),
        onnx_runtime_semantics_sha256: hex_digest(identity.onnx_runtime_semantics_digest()),
        onnx_warm_up_sha256: hex_digest(identity.onnx_warm_up_digest()),
        native_backend_retained_bytes: identity.native_retained_bytes(),
        onnx_backend_retained_bytes: identity.onnx_retained_bytes(),
    }
}

fn isolated_config(config: AppConfig, data_dir: PathBuf) -> Result<AppConfig> {
    let mut overrides = ConfigOverrides::from(config);
    overrides.data_dir = Some(data_dir);
    AppConfig::load(ConfigSources::new(None, &BTreeMap::new(), overrides))
        .context("isolated release benchmark configuration is invalid")
}

fn reconcile_live_measurement(
    measurement: Result<(u64, u64)>,
    close: Result<ReleasePaperBotBenchmarkResult>,
) -> Result<((u64, u64), ReleasePaperBotBenchmarkResult)> {
    match (measurement, close) {
        (Ok(memory), Ok(result)) => Ok((memory, result)),
        (Err(error), Ok(_result)) => Err(error),
        (Ok(_memory), Err(error)) => Err(error),
        (Err(measurement), Err(close)) => {
            bail!("live measurement failed: {measurement:#}; closeout also failed: {close:#}")
        }
    }
}

fn onnx_worker_path() -> Result<PathBuf> {
    let executable = std::env::current_exe().context("running executable path is unavailable")?;
    Ok(executable.with_file_name(if cfg!(windows) {
        "market-squawk-onnx-worker.exe"
    } else {
        "market-squawk-onnx-worker"
    }))
}

fn decode_digest(value: &str) -> Result<[u8; 32]> {
    if value.len() != 64 {
        bail!("SHA-256 identity has an invalid length");
    }
    let mut digest = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let text = std::str::from_utf8(pair).context("SHA-256 identity is not UTF-8")?;
        digest[index] = u8::from_str_radix(text, 16).context("SHA-256 identity is invalid")?;
    }
    Ok(digest)
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
