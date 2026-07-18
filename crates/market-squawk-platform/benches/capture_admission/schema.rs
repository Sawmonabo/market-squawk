//! Stable JSON result schema for capture benchmark evidence.

use serde::{Deserialize, Serialize};
use std::num::NonZeroUsize;

use super::collector::LatencySummary;
use super::endpoints::Endpoint;
use super::fixture::{
    PAYLOAD_BYTES, ProducerCase, QUEUE_DEPTHS, RSS_SAMPLE_JITTER_TOLERANCE, SUSTAINED_FIXTURE,
    WRITER_QUEUE_DEPTHS, requested_operations,
};
use std::collections::BTreeMap;

pub(crate) const RESULT_SCHEMA_VERSION: u32 = 4;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SustainedEpochPhase {
    Warm,
    Measured,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OutcomeCounts {
    pub(crate) accepted: usize,
    pub(crate) queue_full: usize,
    pub(crate) queue_invariant: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PostDrainAccounting {
    pub(crate) accepted: usize,
    pub(crate) consumed: usize,
    pub(crate) queued_bytes: usize,
    pub(crate) record_reservations: usize,
    pub(crate) queue_private_storage_bytes: Option<usize>,
    pub(crate) fixed_capture_bytes: Option<usize>,
    pub(crate) total_accounted_bytes: Option<usize>,
    pub(crate) accounting_invariant_failures: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MatrixResult {
    pub(crate) schema_version: u32,
    pub(crate) backend: String,
    pub(crate) endpoint: Endpoint,
    pub(crate) payload_bytes: usize,
    pub(crate) configured_queue_depth: usize,
    pub(crate) effective_capacity: usize,
    pub(crate) producers: usize,
    pub(crate) representative: bool,
    pub(crate) requested_operations: usize,
    pub(crate) completed_operations: usize,
    pub(crate) outcomes: OutcomeCounts,
    pub(crate) post_drain: PostDrainAccounting,
    pub(crate) elapsed_nanos: u64,
    pub(crate) throughput_per_second: f64,
    pub(crate) latency: LatencySummary,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ComparableFullResult {
    pub(crate) schema_version: u32,
    pub(crate) backend: String,
    pub(crate) queue_full: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ForcedLockResult {
    pub(crate) schema_version: u32,
    pub(crate) backend: String,
    pub(crate) slot_lock_unavailable: usize,
    pub(crate) accepted: usize,
    pub(crate) consumed: usize,
    pub(crate) queued_bytes: usize,
    pub(crate) record_reservations: Option<usize>,
    pub(crate) queue_private_storage_bytes: usize,
    pub(crate) accounting_invariant_failures: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SustainedResult {
    pub(crate) schema_version: u32,
    pub(crate) backend: String,
    pub(crate) warm_epochs: usize,
    pub(crate) warm_duration_nanos: u64,
    pub(crate) measured_epochs: usize,
    pub(crate) measured_duration_nanos: u64,
    pub(crate) payload_bytes: usize,
    pub(crate) configured_queue_depth: usize,
    pub(crate) producers: usize,
    pub(crate) rss_interval_nanos: u64,
    pub(crate) accepted: usize,
    pub(crate) queue_full: usize,
    pub(crate) queue_invariant: usize,
    pub(crate) elapsed_nanos: u64,
    pub(crate) rss_samples: Vec<RssSample>,
    pub(crate) epochs: Vec<SustainedEpochEvidence>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RssSample {
    pub(crate) epoch_ordinal: usize,
    pub(crate) target_offset_nanos: u64,
    pub(crate) observed_offset_nanos: u64,
    pub(crate) rss_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SustainedEpochEvidence {
    pub(crate) ordinal: usize,
    pub(crate) phase: SustainedEpochPhase,
    pub(crate) target_duration_nanos: u64,
    pub(crate) elapsed_nanos: u64,
    pub(crate) outcomes: OutcomeCounts,
    pub(crate) active_rss_samples: Vec<RssSample>,
    pub(crate) post_drain_rss_bytes: u64,
    pub(crate) post_drain: PostDrainAccounting,
}

pub(crate) fn validate_repetition(
    evidence: &RepetitionEvidence,
    expected_repetition: u8,
    producer_cases: &[ProducerCase],
) -> Result<(), String> {
    if evidence.schema_version != RESULT_SCHEMA_VERSION
        || evidence.runner != super::benchmark_identity::EVIDENCE_TARGET
        || evidence.evidence_mode != super::benchmark_identity::FIXED_QUOTA_EVIDENCE_MODE
        || !is_digest(&evidence.build_evidence_sha256)
        || evidence.measured_code_head != super::build_bindings::BUILD_GIT_HEAD
        || evidence.backend != super::backend::EVIDENCE_BACKEND
        || evidence.queue_transport != super::backend::QUEUE_TRANSPORT
        || evidence.queue_private_storage_accounting
            != super::backend::QUEUE_PRIVATE_STORAGE_ACCOUNTING
        || evidence.repetition != expected_repetition
        || evidence.fixtures != super::backend::FIXTURES
    {
        return Err("repetition authority or top-level identity mismatch".to_owned());
    }
    let expected_cells = Endpoint::ALL.iter().try_fold(0_usize, |total, endpoint| {
        let depths = if endpoint.has_deferred_writer_samples() {
            WRITER_QUEUE_DEPTHS.len()
        } else {
            QUEUE_DEPTHS.len()
        };
        total
            .checked_add(PAYLOAD_BYTES.len() * depths * producer_cases.len())
            .ok_or_else(|| "expected matrix cell count overflowed".to_owned())
    })?;
    if evidence.matrix.len() != expected_cells {
        return Err("repetition matrix has missing or extra cells".to_owned());
    }
    let mut index = 0_usize;
    for endpoint in Endpoint::ALL {
        for payload_bytes in PAYLOAD_BYTES {
            let queue_depths = if endpoint.has_deferred_writer_samples() {
                WRITER_QUEUE_DEPTHS.as_slice()
            } else {
                QUEUE_DEPTHS.as_slice()
            };
            for &queue_depth in queue_depths {
                for producer_case in producer_cases {
                    let cell = evidence
                        .matrix
                        .get(index)
                        .ok_or_else(|| "repetition matrix ended early".to_owned())?;
                    let quota = requested_operations(payload_bytes, producer_case.count)?;
                    let configured = NonZeroUsize::new(queue_depth)
                        .ok_or_else(|| "configured queue depth is zero".to_owned())?;
                    let exact_effective = market_squawk_platform::capture_benchmark_support::benchmark_effective_capacity(
                        payload_bytes,
                        configured,
                    )
                    .map_err(|error| error.to_string())?
                    .get();
                    let exact_throughput =
                        (quota as f64) * 1_000_000_000.0 / (cell.elapsed_nanos as f64);
                    if cell.schema_version != RESULT_SCHEMA_VERSION
                        || cell.backend != super::backend::EVIDENCE_BACKEND
                        || cell.endpoint != endpoint
                        || cell.payload_bytes != payload_bytes
                        || cell.configured_queue_depth != queue_depth
                        || cell.effective_capacity != exact_effective
                        || cell.producers != producer_case.count.get()
                        || cell.representative != producer_case.representative
                        || cell.requested_operations != quota
                        || cell.completed_operations != quota
                        || cell.outcomes.accepted != quota
                        || cell.outcomes.queue_full != 0
                        || cell.outcomes.queue_invariant != 0
                        || cell.post_drain.accepted != quota
                        || cell.post_drain.consumed != quota
                        || cell.post_drain.queued_bytes != 0
                        || cell.post_drain.record_reservations != 0
                        || !post_drain_memory_is_valid(&cell.post_drain)
                        || cell.post_drain.accounting_invariant_failures != 0
                        || cell.elapsed_nanos == 0
                        || cell.throughput_per_second.to_bits() != exact_throughput.to_bits()
                        || cell.latency.samples != quota
                        || cell.latency.p50_nanos > cell.latency.p95_nanos
                        || cell.latency.p95_nanos > cell.latency.p99_nanos
                        || cell.latency.p99_nanos > cell.latency.maximum_nanos
                    {
                        return Err(format!("repetition matrix cell {index} is malformed"));
                    }
                    index = index
                        .checked_add(1)
                        .ok_or_else(|| "matrix validation index overflowed".to_owned())?;
                }
            }
        }
    }
    let sustained = &evidence.sustained_rss;
    let representative = producer_cases
        .iter()
        .find(|case| case.representative)
        .ok_or_else(|| "representative producer case is absent".to_owned())?;
    let expected_sustained_nanos = SUSTAINED_FIXTURE
        .warm_duration
        .as_nanos()
        .checked_mul(SUSTAINED_FIXTURE.warm_epochs as u128)
        .and_then(|value| {
            SUSTAINED_FIXTURE
                .measured_duration
                .as_nanos()
                .checked_mul(SUSTAINED_FIXTURE.measured_epochs as u128)
                .and_then(|measured| value.checked_add(measured))
        })
        .ok_or_else(|| "sustained duration overflowed".to_owned())?;
    let sustained_upper_nanos = expected_sustained_nanos
        .checked_add(60_000_000_000)
        .ok_or_else(|| "sustained duration upper bound overflowed".to_owned())?;
    let expected_rss_samples = SUSTAINED_FIXTURE
        .measured_duration
        .as_nanos()
        .div_ceil(SUSTAINED_FIXTURE.rss_interval.as_nanos())
        .checked_mul(SUSTAINED_FIXTURE.measured_epochs as u128)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| "sustained sample quota overflowed".to_owned())?;
    if evidence.comparable_full.schema_version != RESULT_SCHEMA_VERSION
        || evidence.comparable_full.backend != super::backend::EVIDENCE_BACKEND
        || evidence.comparable_full.queue_full != 1
        || evidence.forced_lock.is_some() != super::backend::REQUIRES_BASELINE
        || evidence.forced_lock.as_ref().is_some_and(|forced| {
            forced.schema_version != RESULT_SCHEMA_VERSION
                || forced.backend != super::backend::EVIDENCE_BACKEND
                || forced.slot_lock_unavailable != 1
                || forced.accepted != 0
                || forced.consumed != 0
                || forced.queued_bytes != 0
                || forced.record_reservations.is_some()
                || forced.queue_private_storage_bytes == 0
                || forced.accounting_invariant_failures != 0
        })
        || sustained.schema_version != RESULT_SCHEMA_VERSION
        || sustained.backend != super::backend::EVIDENCE_BACKEND
        || sustained.warm_epochs != SUSTAINED_FIXTURE.warm_epochs
        || sustained.warm_duration_nanos != duration_nanos(SUSTAINED_FIXTURE.warm_duration)?
        || sustained.measured_epochs != SUSTAINED_FIXTURE.measured_epochs
        || sustained.measured_duration_nanos != duration_nanos(SUSTAINED_FIXTURE.measured_duration)?
        || sustained.payload_bytes != SUSTAINED_FIXTURE.payload_bytes
        || sustained.configured_queue_depth != SUSTAINED_FIXTURE.queue_depth
        || sustained.producers != representative.count.get()
        || sustained.rss_interval_nanos != duration_nanos(SUSTAINED_FIXTURE.rss_interval)?
        || sustained.accepted == 0
        || sustained.queue_full == 0
        || sustained.queue_invariant != 0
        || u128::from(sustained.elapsed_nanos) < expected_sustained_nanos
        || u128::from(sustained.elapsed_nanos) > sustained_upper_nanos
        || sustained.rss_samples.len() != expected_rss_samples
        || sustained
            .rss_samples
            .iter()
            .any(|sample| sample.rss_bytes == 0)
    {
        return Err("repetition deterministic fixture evidence is malformed".to_owned());
    }
    validate_sustained_epochs(sustained)?;
    if super::backend::REQUIRES_BASELINE {
        validate_candidate_acceptance(evidence, producer_cases)?;
    }
    Ok(())
}

fn validate_sustained_epochs(sustained: &SustainedResult) -> Result<(), String> {
    let expected_epochs = SUSTAINED_FIXTURE
        .warm_epochs
        .checked_add(SUSTAINED_FIXTURE.measured_epochs)
        .ok_or_else(|| "sustained epoch count overflowed".to_owned())?;
    if sustained.epochs.len() != expected_epochs {
        return Err("sustained epoch evidence count is inexact".to_owned());
    }
    let mut accepted = 0_usize;
    let mut queue_full = 0_usize;
    let mut measured_samples = Vec::new();
    measured_samples
        .try_reserve_exact(sustained.rss_samples.len())
        .map_err(|error| error.to_string())?;
    let samples_per_measured_epoch = usize::try_from(
        SUSTAINED_FIXTURE
            .measured_duration
            .as_nanos()
            .div_ceil(SUSTAINED_FIXTURE.rss_interval.as_nanos()),
    )
    .map_err(|error| error.to_string())?;
    for (index, epoch) in sustained.epochs.iter().enumerate() {
        let measured = index >= SUSTAINED_FIXTURE.warm_epochs;
        let expected_phase = if measured {
            SustainedEpochPhase::Measured
        } else {
            SustainedEpochPhase::Warm
        };
        let expected_duration = if measured {
            SUSTAINED_FIXTURE.measured_duration
        } else {
            SUSTAINED_FIXTURE.warm_duration
        };
        let expected_samples = if measured {
            samples_per_measured_epoch
        } else {
            0
        };
        if epoch.ordinal != index + 1
            || epoch.phase != expected_phase
            || epoch.target_duration_nanos != duration_nanos(expected_duration)?
            || epoch.elapsed_nanos < epoch.target_duration_nanos
            || epoch.outcomes.accepted == 0
            || epoch.outcomes.queue_full == 0
            || epoch.outcomes.queue_invariant != 0
            || epoch.active_rss_samples.len() != expected_samples
            || !validate_epoch_rss_samples(epoch, expected_samples)?
            || epoch.post_drain_rss_bytes == 0
            || epoch.post_drain.accepted != epoch.outcomes.accepted
            || epoch.post_drain.consumed != epoch.outcomes.accepted
            || epoch.post_drain.queued_bytes != 0
            || epoch.post_drain.record_reservations != 0
            || !post_drain_memory_is_valid(&epoch.post_drain)
            || epoch.post_drain.accounting_invariant_failures != 0
        {
            return Err(format!("sustained epoch {index} is malformed"));
        }
        accepted = accepted
            .checked_add(epoch.outcomes.accepted)
            .ok_or_else(|| "sustained accepted sum overflowed".to_owned())?;
        queue_full = queue_full
            .checked_add(epoch.outcomes.queue_full)
            .ok_or_else(|| "sustained refusal sum overflowed".to_owned())?;
        if measured {
            measured_samples.extend_from_slice(&epoch.active_rss_samples);
        }
    }
    if accepted != sustained.accepted
        || queue_full != sustained.queue_full
        || measured_samples != sustained.rss_samples
    {
        return Err("sustained epoch arithmetic does not reconcile".to_owned());
    }
    Ok(())
}

fn post_drain_memory_is_valid(accounting: &PostDrainAccounting) -> bool {
    match super::backend::QUEUE_PRIVATE_STORAGE_ACCOUNTING {
        "not_measured" => {
            accounting.queue_private_storage_bytes.is_none()
                && accounting.fixed_capture_bytes.is_none()
                && accounting.total_accounted_bytes.is_none()
        }
        "exact" => matches!(
            (
                accounting.queue_private_storage_bytes,
                accounting.fixed_capture_bytes,
                accounting.total_accounted_bytes,
            ),
            (Some(queue), Some(fixed), Some(total))
                if queue > 0 && fixed >= queue && total >= fixed
        ),
        _ => false,
    }
}

pub(crate) fn self_check_post_drain_memory_contract() -> Result<(), String> {
    let mut accounting = PostDrainAccounting {
        accepted: 1,
        consumed: 1,
        queued_bytes: 0,
        record_reservations: 0,
        queue_private_storage_bytes: None,
        fixed_capture_bytes: None,
        total_accounted_bytes: None,
        accounting_invariant_failures: 0,
    };
    match super::backend::QUEUE_PRIVATE_STORAGE_ACCOUNTING {
        "not_measured" => {
            if !post_drain_memory_is_valid(&accounting) {
                return Err("standard opaque memory receipt was rejected".to_owned());
            }
            accounting.queue_private_storage_bytes = Some(1);
            accounting.fixed_capture_bytes = Some(2);
            accounting.total_accounted_bytes = Some(3);
            if post_drain_memory_is_valid(&accounting) {
                return Err("standard exact-memory forgery was accepted".to_owned());
            }
        }
        "exact" => {
            if post_drain_memory_is_valid(&accounting) {
                return Err("candidate missing memory receipt was accepted".to_owned());
            }
            accounting.queue_private_storage_bytes = Some(1);
            accounting.fixed_capture_bytes = Some(2);
            accounting.total_accounted_bytes = Some(3);
            if !post_drain_memory_is_valid(&accounting) {
                return Err("candidate exact memory receipt was rejected".to_owned());
            }
            accounting.total_accounted_bytes = Some(1);
            if post_drain_memory_is_valid(&accounting) {
                return Err("candidate non-reconciling memory receipt was accepted".to_owned());
            }
        }
        _ => return Err("unknown private-storage accounting identity".to_owned()),
    }
    Ok(())
}

fn validate_epoch_rss_samples(
    epoch: &SustainedEpochEvidence,
    expected_samples: usize,
) -> Result<bool, String> {
    if epoch.active_rss_samples.len() != expected_samples {
        return Ok(false);
    }
    let interval_nanos = duration_nanos(SUSTAINED_FIXTURE.rss_interval)?;
    let tolerance_nanos = duration_nanos(RSS_SAMPLE_JITTER_TOLERANCE)?;
    let mut previous_observed = None;
    for (index, sample) in epoch.active_rss_samples.iter().enumerate() {
        let target = u64::try_from(index)
            .ok()
            .and_then(|index| index.checked_mul(interval_nanos))
            .ok_or_else(|| "RSS target offset overflowed".to_owned())?;
        let latest = target
            .checked_add(tolerance_nanos)
            .ok_or_else(|| "RSS sample jitter deadline overflowed".to_owned())?;
        if sample.epoch_ordinal != epoch.ordinal
            || sample.target_offset_nanos != target
            || sample.observed_offset_nanos < target
            || sample.observed_offset_nanos > latest
            || sample.rss_bytes == 0
            || previous_observed.is_some_and(|previous| sample.observed_offset_nanos <= previous)
        {
            return Ok(false);
        }
        previous_observed = Some(sample.observed_offset_nanos);
    }
    Ok(true)
}

pub(crate) fn self_check_rss_sample_contract() -> Result<(), String> {
    let interval = duration_nanos(SUSTAINED_FIXTURE.rss_interval)?;
    let mut epoch = SustainedEpochEvidence {
        ordinal: 3,
        phase: SustainedEpochPhase::Measured,
        target_duration_nanos: interval * 3,
        elapsed_nanos: interval * 3,
        outcomes: OutcomeCounts {
            accepted: 1,
            queue_full: 1,
            queue_invariant: 0,
        },
        active_rss_samples: (0_u64..3)
            .map(|index| RssSample {
                epoch_ordinal: 3,
                target_offset_nanos: index * interval,
                observed_offset_nanos: index * interval + 1,
                rss_bytes: 4_096 + index,
            })
            .collect(),
        post_drain_rss_bytes: 4_096,
        post_drain: PostDrainAccounting {
            accepted: 1,
            consumed: 1,
            queued_bytes: 0,
            record_reservations: 0,
            queue_private_storage_bytes: if super::backend::REQUIRES_BASELINE {
                Some(1)
            } else {
                None
            },
            fixed_capture_bytes: if super::backend::REQUIRES_BASELINE {
                Some(2)
            } else {
                None
            },
            total_accounted_bytes: if super::backend::REQUIRES_BASELINE {
                Some(3)
            } else {
                None
            },
            accounting_invariant_failures: 0,
        },
    };
    if !validate_epoch_rss_samples(&epoch, 3)? {
        return Err("valid RSS cadence fixture was rejected".to_owned());
    }
    if validate_epoch_rss_samples(&epoch, 4)? {
        return Err("short RSS cadence fixture was accepted".to_owned());
    }
    epoch.active_rss_samples[1].observed_offset_nanos = interval * 2;
    if validate_epoch_rss_samples(&epoch, 3)? {
        return Err("missed RSS cadence fixture was accepted".to_owned());
    }
    epoch.active_rss_samples[1].observed_offset_nanos = interval + 1;
    epoch.active_rss_samples.push(RssSample {
        epoch_ordinal: 3,
        target_offset_nanos: interval * 3,
        observed_offset_nanos: interval * 3 + 1,
        rss_bytes: 5_000,
    });
    if validate_epoch_rss_samples(&epoch, 3)? {
        return Err("growing RSS cadence fixture was accepted".to_owned());
    }
    Ok(())
}

fn validate_candidate_acceptance(
    evidence: &RepetitionEvidence,
    producer_cases: &[ProducerCase],
) -> Result<(), String> {
    let representative = producer_cases
        .iter()
        .find(|case| case.representative)
        .ok_or_else(|| "representative producer case is absent".to_owned())?;
    let acceptance_cells = evidence.matrix.iter().filter(|cell| {
        cell.endpoint == Endpoint::CaptureAdmission
            && cell.payload_bytes == 1_024
            && cell.configured_queue_depth == SUSTAINED_FIXTURE.queue_depth
            && cell.producers == representative.count.get()
            && cell.representative
    });
    let mut acceptance_count = 0_usize;
    for cell in acceptance_cells {
        acceptance_count = acceptance_count
            .checked_add(1)
            .ok_or_else(|| "candidate acceptance cell count overflowed".to_owned())?;
        if cell.throughput_per_second < 100_000.0 || cell.latency.p99_nanos >= 1_000_000 {
            return Err("candidate capture-admission acceptance threshold failed".to_owned());
        }
    }
    if acceptance_count != 1 {
        return Err(
            "candidate representative acceptance fixture is missing or duplicated".to_owned(),
        );
    }
    let measured = evidence
        .sustained_rss
        .epochs
        .iter()
        .filter(|epoch| epoch.phase == SustainedEpochPhase::Measured)
        .map(|epoch| epoch.post_drain_rss_bytes)
        .collect::<Vec<_>>();
    let first = *measured
        .first()
        .ok_or_else(|| "candidate measured post-drain RSS is absent".to_owned())?;
    let final_rss = *measured
        .last()
        .ok_or_else(|| "candidate final post-drain RSS is absent".to_owned())?;
    let allowed_growth = (first / 20).max(8 * 1024 * 1024);
    if final_rss > first.saturating_add(allowed_growth) {
        return Err("candidate post-drain RSS growth exceeds its bound".to_owned());
    }
    let tail_start = measured.len().saturating_sub(5);
    let mut prior_maximum = measured[..tail_start].iter().copied().max().unwrap_or(0);
    let every_tail_value_is_new_maximum = measured[tail_start..].iter().all(|value| {
        let establishes = *value > prior_maximum;
        prior_maximum = prior_maximum.max(*value);
        establishes
    });
    if every_tail_value_is_new_maximum {
        return Err("candidate final post-drain RSS tail grows monotonically".to_owned());
    }
    Ok(())
}

fn duration_nanos(duration: std::time::Duration) -> Result<u64, String> {
    u64::try_from(duration.as_nanos()).map_err(|error| error.to_string())
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RepetitionEvidence {
    pub(crate) schema_version: u32,
    pub(crate) runner: String,
    pub(crate) evidence_mode: String,
    pub(crate) build_evidence_sha256: String,
    pub(crate) measured_code_head: String,
    pub(crate) backend: String,
    pub(crate) queue_transport: String,
    pub(crate) queue_private_storage_accounting: String,
    pub(crate) repetition: u8,
    pub(crate) fixtures: Vec<String>,
    pub(crate) matrix: Vec<MatrixResult>,
    pub(crate) comparable_full: ComparableFullResult,
    pub(crate) forced_lock: Option<ForcedLockResult>,
    pub(crate) sustained_rss: SustainedResult,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BuildEvidence {
    pub(crate) schema_version: u32,
    pub(crate) runner: String,
    pub(crate) evidence_mode: String,
    pub(crate) evidence_backend: String,
    pub(crate) queue_transport: String,
    pub(crate) queue_private_storage_accounting: String,
    pub(crate) cargo_target: String,
    pub(crate) benchmark_feature: String,
    pub(crate) build_profile: String,
    pub(crate) measured_code_head: String,
    pub(crate) clean_build_enforced: bool,
    pub(crate) build_command: Vec<String>,
    pub(crate) build_environment_policy: String,
    pub(crate) build_command_sha256: String,
    pub(crate) build_environment_sha256: String,
    pub(crate) cargo_executable_sha256: String,
    pub(crate) git_executable_sha256: String,
    pub(crate) rustc_executable_sha256: String,
    pub(crate) git_tree_clean: bool,
    pub(crate) cargo_locked: bool,
    pub(crate) all_features: bool,
    pub(crate) release: bool,
    pub(crate) executable_path: String,
    pub(crate) executable_sha256: String,
    pub(crate) cargo_json_path: String,
    pub(crate) cargo_json_sha256: String,
    pub(crate) source_inventory_sha256: String,
    pub(crate) cargo_lock_sha256: String,
    pub(crate) workspace_manifest_sha256: String,
    pub(crate) package_manifest_sha256: String,
    pub(crate) build_script_sha256: String,
    pub(crate) build_support_sha256: String,
    pub(crate) host_gate_shell_sha256: String,
    pub(crate) host_gate_python_sha256: String,
    pub(crate) host_gate_process_sha256: String,
    pub(crate) host_gate_evidence_io_sha256: String,
    pub(crate) host_gate_cli_sha256: String,
    pub(crate) host_gate_schema_sha256: String,
    pub(crate) host_gate_execution_sha256: String,
    pub(crate) host_gate_observation_sha256: String,
    pub(crate) host_gate_measured_sha256: String,
    pub(crate) build_evidence_python_sha256: String,
    pub(crate) platform_source_sha256: String,
    pub(crate) domain_source_sha256: String,
    pub(crate) entrypoint_sha256: String,
    pub(crate) backend_dispatcher_sha256: String,
    pub(crate) selected_backend_source_path: String,
    pub(crate) selected_backend_source_sha256: String,
    pub(crate) backend_sha256: String,
    pub(crate) criterion_sha256: String,
    pub(crate) observer_sha256: String,
    pub(crate) immutable_module_sha256: BTreeMap<String, String>,
    pub(crate) baseline_lock_path: Option<String>,
    pub(crate) baseline_lock_sha256: Option<String>,
    pub(crate) baseline_manifest_path: Option<String>,
    pub(crate) baseline_manifest_sha256: Option<String>,
    pub(crate) baseline_measured_code_head: Option<String>,
}

impl BuildEvidence {
    pub(crate) fn validate(&self) -> Result<(), String> {
        let expected_modules = super::build_bindings::IMMUTABLE_MODULE_SHA256
            .iter()
            .map(|(name, digest)| ((*name).to_owned(), (*digest).to_owned()))
            .collect::<BTreeMap<_, _>>();
        if self.schema_version != RESULT_SCHEMA_VERSION
            || self.runner != super::benchmark_identity::EVIDENCE_TARGET
            || self.evidence_mode != super::benchmark_identity::FIXED_QUOTA_EVIDENCE_MODE
            || self.evidence_backend != super::backend::EVIDENCE_BACKEND
            || self.evidence_backend != super::build_bindings::BUILD_EVIDENCE_BACKEND
            || self.queue_transport != super::backend::QUEUE_TRANSPORT
            || self.queue_private_storage_accounting
                != super::backend::QUEUE_PRIVATE_STORAGE_ACCOUNTING
            || self.cargo_target != super::benchmark_identity::EVIDENCE_TARGET
            || self.benchmark_feature != "capture-benchmark"
            || self.build_profile
                != "cargo-bench-inherits-release:opt-level=3:lto=thin:codegen-units=1:panic=abort:strip=symbols"
            || self.measured_code_head != super::build_bindings::BUILD_GIT_HEAD
            || !is_git_sha(&self.measured_code_head)
            || !self.clean_build_enforced
            || !super::build_bindings::CLEAN_BUILD_ENFORCED
            || self.build_command
                != [
                    "cargo",
                    "bench",
                    "-p",
                    "market-squawk-platform",
                    "--bench",
                    "capture_admission_evidence",
                    "--all-features",
                    "--locked",
                    "--no-run",
                    "--message-format=json-render-diagnostics",
                ]
            || self.build_environment_policy != super::build_bindings::BUILD_ENVIRONMENT_POLICY
            || self.build_environment_policy != "sanitized-cargo-bench-v2"
            || self.build_command_sha256 != super::build_bindings::BUILD_COMMAND_SHA256
            || self.build_environment_sha256 != super::build_bindings::BUILD_ENVIRONMENT_SHA256
            || self.cargo_executable_sha256 != super::build_bindings::CARGO_EXECUTABLE_SHA256
            || self.git_executable_sha256 != super::build_bindings::GIT_EXECUTABLE_SHA256
            || self.rustc_executable_sha256 != super::build_bindings::RUSTC_EXECUTABLE_SHA256
            || !self.git_tree_clean
            || !self.cargo_locked
            || !self.all_features
            || !self.release
            || self.executable_path != "./capture_admission_evidence-exe"
            || self.cargo_json_path != "./capture-bench-build.json"
            || self.source_inventory_sha256 != super::build_bindings::SOURCE_INVENTORY_SHA256
            || self.cargo_lock_sha256 != super::build_bindings::CARGO_LOCK_SHA256
            || self.workspace_manifest_sha256 != super::build_bindings::WORKSPACE_MANIFEST_SHA256
            || self.package_manifest_sha256 != super::build_bindings::PACKAGE_MANIFEST_SHA256
            || self.build_script_sha256 != super::build_bindings::BUILD_SCRIPT_SHA256
            || self.build_support_sha256 != super::build_bindings::BUILD_SUPPORT_SHA256
            || self.host_gate_shell_sha256 != super::build_bindings::HOST_GATE_SHELL_SHA256
            || self.host_gate_python_sha256 != super::build_bindings::HOST_GATE_PYTHON_SHA256
            || self.host_gate_process_sha256 != super::build_bindings::HOST_GATE_PROCESS_SHA256
            || self.host_gate_evidence_io_sha256
                != super::build_bindings::HOST_GATE_EVIDENCE_IO_SHA256
            || self.host_gate_cli_sha256 != super::build_bindings::HOST_GATE_CLI_SHA256
            || self.host_gate_schema_sha256 != super::build_bindings::HOST_GATE_SCHEMA_SHA256
            || self.host_gate_execution_sha256 != super::build_bindings::HOST_GATE_EXECUTION_SHA256
            || self.host_gate_observation_sha256
                != super::build_bindings::HOST_GATE_OBSERVATION_SHA256
            || self.host_gate_measured_sha256 != super::build_bindings::HOST_GATE_MEASURED_SHA256
            || self.build_evidence_python_sha256
                != super::build_bindings::BUILD_EVIDENCE_PYTHON_SHA256
            || self.platform_source_sha256 != super::build_bindings::PLATFORM_SOURCE_SHA256
            || self.domain_source_sha256 != super::build_bindings::DOMAIN_SOURCE_SHA256
            || self.entrypoint_sha256 != super::build_bindings::ENTRYPOINT_SHA256
            || self.backend_dispatcher_sha256 != super::build_bindings::BACKEND_DISPATCHER_SHA256
            || self.selected_backend_source_path
                != super::build_bindings::SELECTED_BACKEND_SOURCE_PATH
            || self.selected_backend_source_sha256
                != super::build_bindings::SELECTED_BACKEND_SOURCE_SHA256
            || self.backend_sha256 != super::build_bindings::BACKEND_SHA256
            || self.criterion_sha256 != super::build_bindings::CRITERION_SHA256
            || self.observer_sha256 != super::build_bindings::OBSERVER_SHA256
            || self.immutable_module_sha256 != expected_modules
            || self.baseline_lock_path.as_deref() != super::build_bindings::BASELINE_LOCK_PATH
            || self.baseline_lock_sha256.as_deref() != super::build_bindings::BASELINE_LOCK_SHA256
            || self.baseline_manifest_path.as_deref()
                != super::build_bindings::BASELINE_MANIFEST_PATH
            || self.baseline_manifest_sha256.as_deref()
                != super::build_bindings::BASELINE_MANIFEST_SHA256
            || self.baseline_measured_code_head.as_deref()
                != super::build_bindings::BASELINE_MEASURED_CODE_HEAD
            || !baseline_build_contract_is_valid(self)
        {
            return Err("build evidence does not match embedded compile-time bindings".to_owned());
        }
        for digest in [
            &self.executable_sha256,
            &self.cargo_json_sha256,
            &self.build_command_sha256,
            &self.build_environment_sha256,
            &self.cargo_executable_sha256,
            &self.git_executable_sha256,
            &self.rustc_executable_sha256,
            &self.source_inventory_sha256,
            &self.cargo_lock_sha256,
            &self.workspace_manifest_sha256,
            &self.package_manifest_sha256,
            &self.build_script_sha256,
            &self.build_support_sha256,
            &self.host_gate_shell_sha256,
            &self.host_gate_python_sha256,
            &self.host_gate_process_sha256,
            &self.host_gate_evidence_io_sha256,
            &self.host_gate_cli_sha256,
            &self.host_gate_schema_sha256,
            &self.host_gate_execution_sha256,
            &self.host_gate_observation_sha256,
            &self.host_gate_measured_sha256,
            &self.build_evidence_python_sha256,
            &self.platform_source_sha256,
            &self.domain_source_sha256,
            &self.entrypoint_sha256,
            &self.backend_dispatcher_sha256,
            &self.selected_backend_source_sha256,
            &self.backend_sha256,
            &self.criterion_sha256,
            &self.observer_sha256,
        ] {
            if !is_digest(digest) {
                return Err("build evidence contains an invalid digest".to_owned());
            }
        }
        Ok(())
    }
}

fn baseline_build_contract_is_valid(evidence: &BuildEvidence) -> bool {
    match super::backend::REQUIRES_BASELINE {
        false => {
            evidence.baseline_manifest_path.is_none()
                && evidence.baseline_lock_path.is_none()
                && evidence.baseline_lock_sha256.is_none()
                && evidence.baseline_manifest_sha256.is_none()
                && evidence.baseline_measured_code_head.is_none()
        }
        true => {
            evidence.baseline_lock_path.as_deref() == Some("./baseline-lock.json")
                && evidence
                    .baseline_lock_sha256
                    .as_deref()
                    .is_some_and(is_digest)
                && evidence.baseline_manifest_path.as_deref() == Some("./baseline-manifest.json")
                && evidence
                    .baseline_manifest_sha256
                    .as_deref()
                    .is_some_and(is_digest)
                && evidence
                    .baseline_measured_code_head
                    .as_deref()
                    .is_some_and(is_git_sha)
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HostGateManifest {
    pub(crate) valid: bool,
    pub(crate) preflight_sha256: String,
    pub(crate) postflight_sha256: String,
    pub(crate) comparison_sha256: String,
    pub(crate) monitor_sha256: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HostGateComparison {
    pub(crate) schema_version: u32,
    pub(crate) evidence_mode: String,
    pub(crate) valid: bool,
    pub(crate) host_fingerprint_sha256: String,
    pub(crate) toolchain_fingerprint_sha256: String,
    pub(crate) release_profile_sha256: String,
    pub(crate) preflight_sha256: String,
    pub(crate) postflight_sha256: String,
    pub(crate) lock_nonce_sha256: String,
    pub(crate) wall_elapsed_ns: u64,
    pub(crate) monotonic_elapsed_ns: u64,
    pub(crate) continuous_monitor: bool,
    pub(crate) monitor_sha256: String,
    pub(crate) runner_sha256: String,
    pub(crate) build_evidence_sha256: String,
    pub(crate) baseline_manifest_sha256: Option<String>,
    pub(crate) baseline_lock_sha256: Option<String>,
    pub(crate) monitored_repetitions: usize,
    pub(crate) monitor_samples: usize,
}

impl HostGateComparison {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.schema_version != RESULT_SCHEMA_VERSION
            || self.evidence_mode != "production"
            || !self.valid
            || self.wall_elapsed_ns == 0
            || self.monotonic_elapsed_ns == 0
            || self.wall_elapsed_ns.abs_diff(self.monotonic_elapsed_ns) > 2_000_000_000
            || !self.continuous_monitor
            || self.monitored_repetitions != 5
            || self.monitor_samples < 5
        {
            return Err("host-gate comparison is not authoritative production evidence".to_owned());
        }
        for digest in [
            &self.host_fingerprint_sha256,
            &self.toolchain_fingerprint_sha256,
            &self.release_profile_sha256,
            &self.preflight_sha256,
            &self.postflight_sha256,
            &self.lock_nonce_sha256,
            &self.monitor_sha256,
            &self.runner_sha256,
            &self.build_evidence_sha256,
        ] {
            if !is_digest(digest) {
                return Err("host-gate comparison contains an invalid digest".to_owned());
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BaselineManifest {
    pub(crate) schema_version: u32,
    pub(crate) runner: String,
    pub(crate) evidence_mode: String,
    pub(crate) criterion_evidence_mode: String,
    pub(crate) measured_code_head: String,
    pub(crate) build_evidence_sha256: String,
    pub(crate) baseline_manifest_sha256: Option<String>,
    pub(crate) baseline_lock_sha256: Option<String>,
    pub(crate) build_environment_policy: String,
    pub(crate) build_command_sha256: String,
    pub(crate) build_environment_sha256: String,
    pub(crate) cargo_executable_sha256: String,
    pub(crate) git_executable_sha256: String,
    pub(crate) rustc_executable_sha256: String,
    pub(crate) cargo_json_sha256: String,
    pub(crate) source_inventory_sha256: String,
    pub(crate) cargo_lock_sha256: String,
    pub(crate) criterion_sha256: String,
    pub(crate) observer_sha256: String,
    pub(crate) backend: String,
    pub(crate) queue_transport: String,
    pub(crate) queue_private_storage_accounting: String,
    pub(crate) benchmark_support_feature: String,
    pub(crate) fixtures: Vec<String>,
    pub(crate) repetitions: Vec<u8>,
    pub(crate) executable_path: String,
    pub(crate) executable_sha256: String,
    pub(crate) immutable_module_sha256: BTreeMap<String, String>,
    pub(crate) entrypoint_sha256: String,
    pub(crate) backend_sha256: String,
    pub(crate) production_library_sha256: BTreeMap<String, String>,
    pub(crate) repetition_sha256: BTreeMap<String, String>,
    pub(crate) artifact_sha256: BTreeMap<String, String>,
    pub(crate) tool_sha256: BTreeMap<String, String>,
    pub(crate) host_fingerprint_sha256: String,
    pub(crate) toolchain_fingerprint_sha256: String,
    pub(crate) release_profile_sha256: String,
    pub(crate) host_gate: HostGateManifest,
}

#[derive(Clone, Debug)]
pub(crate) struct CandidateBaselineExpectation {
    pub(crate) observed_manifest_sha256: String,
    pub(crate) expected_manifest_sha256: String,
    pub(crate) observed_lock_sha256: String,
    pub(crate) expected_lock_sha256: String,
    pub(crate) expected_baseline_head: String,
    pub(crate) candidate_head: String,
    pub(crate) immutable_module_sha256: BTreeMap<String, String>,
    pub(crate) entrypoint_sha256: String,
    pub(crate) backend_sha256: String,
    pub(crate) criterion_sha256: String,
    pub(crate) observer_sha256: String,
    pub(crate) tool_sha256: BTreeMap<String, String>,
    pub(crate) rustc_executable_sha256: String,
    pub(crate) lock: BaselineLock,
}

#[derive(Clone, Debug)]
pub(crate) struct BaselineCompatibility {
    pub(crate) schema_version: u32,
    pub(crate) runner: String,
    pub(crate) evidence_mode: String,
    pub(crate) criterion_evidence_mode: String,
    pub(crate) measured_code_head: String,
    pub(crate) build_evidence_sha256: String,
    pub(crate) cargo_executable_sha256: String,
    pub(crate) git_executable_sha256: String,
    pub(crate) rustc_executable_sha256: String,
    pub(crate) backend: String,
    pub(crate) queue_transport: String,
    pub(crate) queue_private_storage_accounting: String,
    pub(crate) benchmark_support_feature: String,
    pub(crate) fixtures: Vec<String>,
    pub(crate) repetitions: Vec<u8>,
    pub(crate) immutable_module_sha256: BTreeMap<String, String>,
    pub(crate) entrypoint_sha256: String,
    pub(crate) backend_sha256: String,
    pub(crate) criterion_sha256: String,
    pub(crate) observer_sha256: String,
    pub(crate) tool_sha256: BTreeMap<String, String>,
    pub(crate) artifact_sha256: BTreeMap<String, String>,
    pub(crate) repetition_sha256: BTreeMap<String, String>,
    pub(crate) host_fingerprint_sha256: String,
    pub(crate) toolchain_fingerprint_sha256: String,
    pub(crate) release_profile_sha256: String,
}

impl From<&BaselineManifest> for BaselineCompatibility {
    fn from(manifest: &BaselineManifest) -> Self {
        Self {
            schema_version: manifest.schema_version,
            runner: manifest.runner.clone(),
            evidence_mode: manifest.evidence_mode.clone(),
            criterion_evidence_mode: manifest.criterion_evidence_mode.clone(),
            measured_code_head: manifest.measured_code_head.clone(),
            build_evidence_sha256: manifest.build_evidence_sha256.clone(),
            cargo_executable_sha256: manifest.cargo_executable_sha256.clone(),
            git_executable_sha256: manifest.git_executable_sha256.clone(),
            rustc_executable_sha256: manifest.rustc_executable_sha256.clone(),
            backend: manifest.backend.clone(),
            queue_transport: manifest.queue_transport.clone(),
            queue_private_storage_accounting: manifest.queue_private_storage_accounting.clone(),
            benchmark_support_feature: manifest.benchmark_support_feature.clone(),
            fixtures: manifest.fixtures.clone(),
            repetitions: manifest.repetitions.clone(),
            immutable_module_sha256: manifest.immutable_module_sha256.clone(),
            entrypoint_sha256: manifest.entrypoint_sha256.clone(),
            backend_sha256: manifest.backend_sha256.clone(),
            criterion_sha256: manifest.criterion_sha256.clone(),
            observer_sha256: manifest.observer_sha256.clone(),
            tool_sha256: manifest.tool_sha256.clone(),
            artifact_sha256: manifest.artifact_sha256.clone(),
            repetition_sha256: manifest.repetition_sha256.clone(),
            host_fingerprint_sha256: manifest.host_fingerprint_sha256.clone(),
            toolchain_fingerprint_sha256: manifest.toolchain_fingerprint_sha256.clone(),
            release_profile_sha256: manifest.release_profile_sha256.clone(),
        }
    }
}

pub(crate) fn validate_candidate_baseline_compatibility(
    baseline: &BaselineCompatibility,
    expected: &CandidateBaselineExpectation,
) -> Result<(), String> {
    if expected.observed_manifest_sha256 != expected.expected_manifest_sha256
        || !is_digest(&expected.observed_manifest_sha256)
        || expected.observed_lock_sha256 != expected.expected_lock_sha256
        || !is_digest(&expected.observed_lock_sha256)
    {
        return Err("candidate baseline manifest digest is not the build-bound digest".to_owned());
    }
    if baseline.schema_version != RESULT_SCHEMA_VERSION
        || baseline.runner != super::benchmark_identity::EVIDENCE_TARGET
        || baseline.evidence_mode != super::benchmark_identity::FIXED_QUOTA_EVIDENCE_MODE
        || baseline.criterion_evidence_mode != super::benchmark_identity::CRITERION_EVIDENCE_MODE
        || baseline.backend != "standard"
        || baseline.queue_transport != "standard_sync_channel"
        || baseline.queue_private_storage_accounting != "not_measured"
        || baseline.benchmark_support_feature != "capture-benchmark"
        || baseline.fixtures != ["matrix", "comparable_full", "sustained_rss"]
        || baseline.repetitions != [1, 2, 3, 4, 5]
        || !is_digest(&baseline.cargo_executable_sha256)
        || !is_digest(&baseline.git_executable_sha256)
        || baseline.tool_sha256.get("cargo-executable") != Some(&baseline.cargo_executable_sha256)
        || baseline.tool_sha256.get("git-executable") != Some(&baseline.git_executable_sha256)
        || !is_digest(&baseline.rustc_executable_sha256)
        || baseline.rustc_executable_sha256 != expected.rustc_executable_sha256
        || baseline.tool_sha256.get("rustc-executable") != Some(&baseline.rustc_executable_sha256)
    {
        return Err("candidate baseline authority identity is invalid".to_owned());
    }
    if baseline.measured_code_head != expected.expected_baseline_head
        || !is_git_sha(&baseline.measured_code_head)
        || !is_git_sha(&expected.candidate_head)
    {
        return Err("candidate baseline code head is not the build-bound head".to_owned());
    }
    if expected.lock.schema_version != RESULT_SCHEMA_VERSION
        || expected.lock.state != "frozen_standard_baseline"
        || expected.lock.baseline_head != baseline.measured_code_head
        || expected.lock.manifest_sha256 != expected.observed_manifest_sha256
        || expected.lock.manifest_reference
            != format!(
                "target/q2-a4-capture-benchmark/standard-{}/manifest.json",
                baseline.measured_code_head
            )
        || expected.lock.report_reference
            != "docs/reports/performance/2026-07-17-q2-a4-standard-channel-baseline.md"
        || expected.lock.approval_state != "independent_seed_review_approved"
        || expected.lock.approval_identity != "q2-a4-seed-checkpoint-review"
        || expected.lock.backend != "standard"
        || expected.lock.queue_transport != baseline.queue_transport
        || expected.lock.queue_private_storage_accounting
            != baseline.queue_private_storage_accounting
        || expected.lock.backend_sha256 != baseline.backend_sha256
        || expected.lock.build_evidence_sha256 != baseline.build_evidence_sha256
        || expected.lock.artifact_sha256 != baseline.artifact_sha256
        || expected.lock.repetition_sha256 != baseline.repetition_sha256
        || expected.lock.host_fingerprint_sha256 != baseline.host_fingerprint_sha256
        || expected.lock.toolchain_fingerprint_sha256 != baseline.toolchain_fingerprint_sha256
        || expected.lock.release_profile_sha256 != baseline.release_profile_sha256
        || !is_digest(&expected.lock.report_sha256)
    {
        return Err("tracked baseline lock does not authorize this manifest".to_owned());
    }
    if baseline.measured_code_head == expected.candidate_head
        || baseline.backend_sha256 == expected.backend_sha256
    {
        return Err("candidate and baseline are not distinct implementations".to_owned());
    }
    if baseline.immutable_module_sha256 != expected.immutable_module_sha256
        || baseline.entrypoint_sha256 != expected.entrypoint_sha256
        || baseline.criterion_sha256 != expected.criterion_sha256
        || baseline.observer_sha256 != expected.observer_sha256
        || expected.lock.immutable_module_sha256 != baseline.immutable_module_sha256
        || expected.lock.entrypoint_sha256 != baseline.entrypoint_sha256
        || expected.lock.criterion_sha256 != baseline.criterion_sha256
        || expected.lock.observer_sha256 != baseline.observer_sha256
    {
        return Err("candidate immutable harness differs from the baseline".to_owned());
    }
    if baseline.tool_sha256 != expected.tool_sha256
        || expected.lock.tool_sha256 != baseline.tool_sha256
    {
        return Err("candidate evidence tools differ from the baseline".to_owned());
    }
    Ok(())
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BaselineLock {
    pub(crate) schema_version: u32,
    pub(crate) state: String,
    pub(crate) baseline_head: String,
    pub(crate) manifest_sha256: String,
    pub(crate) manifest_reference: String,
    pub(crate) report_reference: String,
    pub(crate) report_sha256: String,
    pub(crate) approval_state: String,
    pub(crate) approval_identity: String,
    pub(crate) backend: String,
    pub(crate) queue_transport: String,
    pub(crate) queue_private_storage_accounting: String,
    pub(crate) backend_sha256: String,
    pub(crate) build_evidence_sha256: String,
    pub(crate) immutable_module_sha256: BTreeMap<String, String>,
    pub(crate) entrypoint_sha256: String,
    pub(crate) criterion_sha256: String,
    pub(crate) observer_sha256: String,
    pub(crate) tool_sha256: BTreeMap<String, String>,
    pub(crate) artifact_sha256: BTreeMap<String, String>,
    pub(crate) repetition_sha256: BTreeMap<String, String>,
    pub(crate) host_fingerprint_sha256: String,
    pub(crate) toolchain_fingerprint_sha256: String,
    pub(crate) release_profile_sha256: String,
}

#[cfg(test)]
pub(crate) fn self_check_candidate_baseline_contracts() -> Result<(), String> {
    let digest = "1".repeat(64);
    let baseline_head = "2".repeat(40);
    let candidate_head = "3".repeat(40);
    let immutable = BTreeMap::from([("fixture".to_owned(), "4".repeat(64))]);
    let tools = BTreeMap::from([
        ("host-gate".to_owned(), "5".repeat(64)),
        ("cargo-executable".to_owned(), "8".repeat(64)),
        ("git-executable".to_owned(), "9".repeat(64)),
        ("rustc-executable".to_owned(), "b".repeat(64)),
    ]);
    let baseline = BaselineCompatibility {
        schema_version: RESULT_SCHEMA_VERSION,
        runner: super::benchmark_identity::EVIDENCE_TARGET.to_owned(),
        evidence_mode: super::benchmark_identity::FIXED_QUOTA_EVIDENCE_MODE.to_owned(),
        criterion_evidence_mode: super::benchmark_identity::CRITERION_EVIDENCE_MODE.to_owned(),
        measured_code_head: baseline_head.clone(),
        build_evidence_sha256: "c".repeat(64),
        cargo_executable_sha256: "8".repeat(64),
        git_executable_sha256: "9".repeat(64),
        rustc_executable_sha256: "b".repeat(64),
        backend: "standard".to_owned(),
        queue_transport: "standard_sync_channel".to_owned(),
        queue_private_storage_accounting: "not_measured".to_owned(),
        benchmark_support_feature: "capture-benchmark".to_owned(),
        fixtures: vec![
            "matrix".to_owned(),
            "comparable_full".to_owned(),
            "sustained_rss".to_owned(),
        ],
        repetitions: vec![1, 2, 3, 4, 5],
        immutable_module_sha256: immutable.clone(),
        entrypoint_sha256: "6".repeat(64),
        backend_sha256: "7".repeat(64),
        criterion_sha256: "8".repeat(64),
        observer_sha256: "a".repeat(64),
        tool_sha256: tools.clone(),
        artifact_sha256: BTreeMap::from([("artifact".to_owned(), "d".repeat(64))]),
        repetition_sha256: BTreeMap::from([("repetition-1.json".to_owned(), "e".repeat(64))]),
        host_fingerprint_sha256: "f".repeat(64),
        toolchain_fingerprint_sha256: "0".repeat(64),
        release_profile_sha256: "1".repeat(64),
    };
    let lock = BaselineLock {
        schema_version: RESULT_SCHEMA_VERSION,
        state: "frozen_standard_baseline".to_owned(),
        baseline_head: baseline_head.clone(),
        manifest_sha256: digest.clone(),
        manifest_reference: format!(
            "target/q2-a4-capture-benchmark/standard-{baseline_head}/manifest.json"
        ),
        report_reference: "docs/reports/performance/2026-07-17-q2-a4-standard-channel-baseline.md"
            .to_owned(),
        report_sha256: "2".repeat(64),
        approval_state: "independent_seed_review_approved".to_owned(),
        approval_identity: "q2-a4-seed-checkpoint-review".to_owned(),
        backend: "standard".to_owned(),
        queue_transport: baseline.queue_transport.clone(),
        queue_private_storage_accounting: baseline.queue_private_storage_accounting.clone(),
        backend_sha256: baseline.backend_sha256.clone(),
        build_evidence_sha256: baseline.build_evidence_sha256.clone(),
        immutable_module_sha256: immutable.clone(),
        entrypoint_sha256: baseline.entrypoint_sha256.clone(),
        criterion_sha256: baseline.criterion_sha256.clone(),
        observer_sha256: baseline.observer_sha256.clone(),
        tool_sha256: tools.clone(),
        artifact_sha256: baseline.artifact_sha256.clone(),
        repetition_sha256: baseline.repetition_sha256.clone(),
        host_fingerprint_sha256: baseline.host_fingerprint_sha256.clone(),
        toolchain_fingerprint_sha256: baseline.toolchain_fingerprint_sha256.clone(),
        release_profile_sha256: baseline.release_profile_sha256.clone(),
    };
    let expected = CandidateBaselineExpectation {
        observed_manifest_sha256: digest.clone(),
        expected_manifest_sha256: digest,
        observed_lock_sha256: "b".repeat(64),
        expected_lock_sha256: "b".repeat(64),
        expected_baseline_head: baseline_head,
        candidate_head,
        immutable_module_sha256: immutable,
        entrypoint_sha256: baseline.entrypoint_sha256.clone(),
        backend_sha256: "9".repeat(64),
        criterion_sha256: baseline.criterion_sha256.clone(),
        observer_sha256: baseline.observer_sha256.clone(),
        tool_sha256: tools,
        rustc_executable_sha256: baseline.rustc_executable_sha256.clone(),
        lock,
    };
    validate_candidate_baseline_compatibility(&baseline, &expected)?;

    let mut cases = Vec::new();
    let mut missing = expected.clone();
    missing.expected_manifest_sha256.clear();
    cases.push((baseline.clone(), missing));
    let mut tampered = expected.clone();
    tampered.observed_manifest_sha256 = "a".repeat(64);
    cases.push((baseline.clone(), tampered));
    let mut wrong_head = baseline.clone();
    wrong_head.measured_code_head = "b".repeat(40);
    cases.push((wrong_head, expected.clone()));
    let mut wrong_harness = baseline.clone();
    wrong_harness.entrypoint_sha256 = "c".repeat(64);
    cases.push((wrong_harness, expected.clone()));
    let mut wrong_tool = baseline.clone();
    wrong_tool
        .tool_sha256
        .insert("host-gate".to_owned(), "d".repeat(64));
    cases.push((wrong_tool, expected.clone()));
    let mut wrong_rustc = baseline.clone();
    wrong_rustc.rustc_executable_sha256 = "f".repeat(64);
    cases.push((wrong_rustc, expected.clone()));
    let mut matrix_drift = baseline.clone();
    matrix_drift
        .immutable_module_sha256
        .insert("fixture".to_owned(), "e".repeat(64));
    cases.push((matrix_drift, expected.clone()));
    let mut same_head = baseline.clone();
    same_head.measured_code_head = expected.candidate_head.clone();
    let mut same_head_expected = expected.clone();
    same_head_expected.expected_baseline_head = expected.candidate_head.clone();
    cases.push((same_head, same_head_expected));
    let mut same_backend = baseline.clone();
    same_backend.backend_sha256 = expected.backend_sha256.clone();
    cases.push((same_backend, expected.clone()));
    let mut forged_baseline = baseline.clone();
    forged_baseline.measured_code_head = "f".repeat(40);
    let mut forged_expected = expected;
    forged_expected.observed_manifest_sha256 = "0".repeat(64);
    forged_expected.expected_manifest_sha256 = "0".repeat(64);
    forged_expected.expected_baseline_head = forged_baseline.measured_code_head.clone();
    forged_expected.lock.baseline_head = forged_baseline.measured_code_head.clone();
    forged_expected.lock.manifest_sha256 = forged_expected.observed_manifest_sha256.clone();
    forged_expected.lock.manifest_reference = format!(
        "target/q2-a4-capture-benchmark/standard-{}/manifest.json",
        forged_baseline.measured_code_head
    );
    forged_expected.observed_lock_sha256 = "3".repeat(64);
    cases.push((forged_baseline, forged_expected));
    if cases.len() != 10
        || cases.into_iter().any(|(baseline, expected)| {
            validate_candidate_baseline_compatibility(&baseline, &expected).is_ok()
        })
    {
        return Err("candidate baseline rejection self-check failed".to_owned());
    }
    Ok(())
}

fn is_digest(digest: &str) -> bool {
    digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_git_sha(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
