use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{YahooAdapterError, YahooHttpRequest};

/// Caps provider-silent recovery at eight times the configured base cooldown.
///
/// Yahoo publishes no Finance API quota contract, so this is an application safety bound rather
/// than a claimed provider window. A successful observation resets the exponent immediately.
const MAX_FALLBACK_BACKOFF_EXPONENT: u32 = 3;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AdmissionPolicy {
    /// Application-owned base recovery delay when no usable provider instruction exists.
    pub fallback_cooldown_ms: u64,
    /// Inclusive upper bound for the per-opening fallback jitter added to the base delay.
    pub fallback_max_jitter_ms: u64,
    /// Application-owned count of consecutive transport/schema failures that opens the circuit.
    pub repeated_failure_threshold: u32,
}

impl AdmissionPolicy {
    pub fn new(
        fallback_cooldown_ms: u64,
        fallback_max_jitter_ms: u64,
        repeated_failure_threshold: u32,
    ) -> Result<Self, YahooAdapterError> {
        if fallback_cooldown_ms == 0 {
            return Err(YahooAdapterError::ZeroApplicationBound {
                name: "fallback_cooldown_ms",
            });
        }
        if fallback_max_jitter_ms == 0 {
            return Err(YahooAdapterError::ZeroApplicationBound {
                name: "fallback_max_jitter_ms",
            });
        }
        fallback_cooldown_ms
            .checked_mul(1_u64 << MAX_FALLBACK_BACKOFF_EXPONENT)
            .and_then(|maximum| maximum.checked_add(fallback_max_jitter_ms))
            .ok_or(YahooAdapterError::InvalidFallbackCooldown)?;
        if repeated_failure_threshold == 0 {
            return Err(YahooAdapterError::ZeroApplicationBound {
                name: "repeated_failure_threshold",
            });
        }
        Ok(Self {
            fallback_cooldown_ms,
            fallback_max_jitter_ms,
            repeated_failure_threshold,
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum AttemptKind {
    CookieBootstrap,
    CrumbAcquisition,
    ConsentBootstrap,
    ConsentSubmission,
    ConsentCopy,
    Primary,
    CookieStrategyFallback,
    RepairSubrequest,
    HalfOpenProbe,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum AttemptDisposition {
    Success,
    Partial,
    ProviderBackoff {
        status: u16,
        retry_after: Option<YahooRetryAfterDirective>,
    },
    TransportFailure,
    SchemaFailure,
    Cancelled,
    DeadlineExceeded,
}

/// Exact server recovery instruction retained without converting absolute dates into delays.
///
/// Delta seconds are applied from completion of the corresponding HTTP attempt. An HTTP-date is
/// already an absolute wall-clock deadline and is therefore never added to another timestamp.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "syntax", rename_all = "kebab-case")]
pub enum YahooRetryAfterDirective {
    DeltaSeconds { seconds: u64 },
    HttpDate { retry_at_unix_ms: i64 },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AttemptOutcome {
    pub returned_units: usize,
    pub missing_units: usize,
    pub returned_records: usize,
    pub response_bytes: usize,
    pub latency_ms: u64,
    pub disposition: AttemptDisposition,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, tag = "state", rename_all = "kebab-case")]
pub enum CircuitSnapshot {
    Closed,
    Open { retry_at_unix_ms: i64 },
    HalfOpen,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionSnapshot {
    pub logical_primary_operations_total: u64,
    pub actual_http_attempts_total: u64,
    pub requested_units_total: u64,
    pub returned_units_total: u64,
    pub missing_units_total: u64,
    pub returned_records_total: u64,
    pub response_bytes_total: u64,
    pub latency_ms_total: u64,
    pub maximum_observed_response_bytes: usize,
    pub maximum_observed_latency_ms: u64,
    pub provider_backoff_total: u64,
    pub http_429_total: u64,
    pub transport_failures_total: u64,
    pub schema_failures_total: u64,
    pub cancelled_attempts_total: u64,
    pub deadline_exceeded_attempts_total: u64,
    pub cache_hits_total: u64,
    pub coalesced_callers_total: u64,
    pub consecutive_failures: u32,
    /// Current bounded exponential step used only when no usable provider retry time exists.
    pub fallback_backoff_exponent: u32,
    pub active_request_key: Option<String>,
    pub circuit: CircuitSnapshot,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AdmissionRejection {
    #[error("Yahoo admission state is unavailable")]
    StateUnavailable,
    #[error("attempt outcome units do not equal the admitted requested units")]
    OutcomeUnitMismatch,
    #[error("attempt permit is no longer active")]
    StalePermit,
    #[error("admission telemetry counter overflowed")]
    CounterOverflow,
    #[error("persisted Yahoo admission state is not a complete quiescent snapshot")]
    InvalidPersistedState,
}

#[derive(Debug)]
pub enum AdmissionDecision {
    Execute(AttemptPermit),
    JoinInFlight { request_key: String },
    Busy { active_request_key: String },
    CircuitOpen { retry_at_unix_ms: i64 },
}

/// One process-shared serialized admission and health authority.
#[derive(Clone, Debug)]
pub struct YahooAdmission {
    inner: Arc<Mutex<AdmissionState>>,
}

#[derive(Debug)]
struct AdmissionState {
    policy: AdmissionPolicy,
    next_attempt_id: u64,
    active: Option<ActiveAttempt>,
    circuit: CircuitState,
    snapshot: AdmissionSnapshot,
}

#[derive(Clone, Debug)]
struct ActiveAttempt {
    id: u64,
    request_key: String,
    requested_units: usize,
    started_at_unix_ms: i64,
    half_open_probe: bool,
}

#[derive(Clone, Copy, Debug)]
enum CircuitState {
    Closed,
    Open { retry_at_unix_ms: i64 },
    HalfOpen,
}

impl CircuitState {
    const fn snapshot(self) -> CircuitSnapshot {
        match self {
            Self::Closed => CircuitSnapshot::Closed,
            Self::Open { retry_at_unix_ms } => CircuitSnapshot::Open { retry_at_unix_ms },
            Self::HalfOpen => CircuitSnapshot::HalfOpen,
        }
    }
}

impl YahooAdmission {
    pub fn new(policy: AdmissionPolicy) -> Self {
        let snapshot = AdmissionSnapshot {
            logical_primary_operations_total: 0,
            actual_http_attempts_total: 0,
            requested_units_total: 0,
            returned_units_total: 0,
            missing_units_total: 0,
            returned_records_total: 0,
            response_bytes_total: 0,
            latency_ms_total: 0,
            maximum_observed_response_bytes: 0,
            maximum_observed_latency_ms: 0,
            provider_backoff_total: 0,
            http_429_total: 0,
            transport_failures_total: 0,
            schema_failures_total: 0,
            cancelled_attempts_total: 0,
            deadline_exceeded_attempts_total: 0,
            cache_hits_total: 0,
            coalesced_callers_total: 0,
            consecutive_failures: 0,
            fallback_backoff_exponent: 0,
            active_request_key: None,
            circuit: CircuitSnapshot::Closed,
        };
        Self {
            inner: Arc::new(Mutex::new(AdmissionState {
                policy,
                next_attempt_id: 1,
                active: None,
                circuit: CircuitState::Closed,
                snapshot,
            })),
        }
    }

    /// Restores one complete quiescent provider-wide circuit and telemetry snapshot.
    ///
    /// A snapshot taken while a request or half-open probe is active is deliberately rejected:
    /// request execution is process-local and cannot be resumed after a crash. The application
    /// persists only snapshots returned after an operation completes, so no request target,
    /// cookie, crumb, response body, or cache entry becomes durable admission state.
    pub fn try_restore(
        policy: AdmissionPolicy,
        snapshot: AdmissionSnapshot,
    ) -> Result<Self, AdmissionRejection> {
        validate_restored_snapshot(policy, &snapshot)?;
        let circuit = match snapshot.circuit {
            CircuitSnapshot::Closed => CircuitState::Closed,
            CircuitSnapshot::Open { retry_at_unix_ms } => CircuitState::Open { retry_at_unix_ms },
            CircuitSnapshot::HalfOpen => return Err(AdmissionRejection::InvalidPersistedState),
        };
        let next_attempt_id = snapshot
            .logical_primary_operations_total
            .max(snapshot.actual_http_attempts_total)
            .checked_add(1)
            .ok_or(AdmissionRejection::CounterOverflow)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(AdmissionState {
                policy,
                next_attempt_id,
                active: None,
                circuit,
                snapshot,
            })),
        })
    }

    pub fn admit(
        &self,
        request: &YahooHttpRequest,
        request_identity: &str,
        attempt_kind: AttemptKind,
        now_unix_ms: i64,
    ) -> Result<AdmissionDecision, AdmissionRejection> {
        let mut state = self
            .inner
            .lock()
            .map_err(|_| AdmissionRejection::StateUnavailable)?;

        if let Some(active) = state.active.as_ref() {
            if active.request_key == request_identity {
                state.snapshot.coalesced_callers_total =
                    checked_add(state.snapshot.coalesced_callers_total, 1)?;
                return Ok(AdmissionDecision::JoinInFlight {
                    request_key: request_identity.to_owned(),
                });
            }
            return Ok(AdmissionDecision::Busy {
                active_request_key: active.request_key.clone(),
            });
        }

        let half_open_probe = match state.circuit {
            CircuitState::Closed => false,
            CircuitState::Open { retry_at_unix_ms } if now_unix_ms < retry_at_unix_ms => {
                return Ok(AdmissionDecision::CircuitOpen { retry_at_unix_ms });
            }
            CircuitState::Open { .. } => {
                state.circuit = CircuitState::HalfOpen;
                true
            }
            CircuitState::HalfOpen => {
                return Ok(AdmissionDecision::CircuitOpen {
                    retry_at_unix_ms: now_unix_ms,
                });
            }
        };

        let requested_units = request_accounting_units(request);
        let attempt_id = state.next_attempt_id;
        state.next_attempt_id = state
            .next_attempt_id
            .checked_add(1)
            .ok_or(AdmissionRejection::CounterOverflow)?;
        if attempt_kind == AttemptKind::Primary {
            state.snapshot.logical_primary_operations_total =
                checked_add(state.snapshot.logical_primary_operations_total, 1)?;
        }
        state.active = Some(ActiveAttempt {
            id: attempt_id,
            request_key: request_identity.to_owned(),
            requested_units,
            started_at_unix_ms: now_unix_ms,
            half_open_probe,
        });
        state.snapshot.active_request_key = Some(request_identity.to_owned());
        state.snapshot.circuit = state.circuit.snapshot();
        Ok(AdmissionDecision::Execute(AttemptPermit {
            inner: Arc::clone(&self.inner),
            attempt_id,
            completed: false,
        }))
    }

    /// Record a fresh-cache result without pretending it was an upstream attempt.
    pub fn record_cache_hit(&self) -> Result<(), AdmissionRejection> {
        let mut state = self
            .inner
            .lock()
            .map_err(|_| AdmissionRejection::StateUnavailable)?;
        state.snapshot.cache_hits_total = checked_add(state.snapshot.cache_hits_total, 1)?;
        Ok(())
    }

    /// Records a caller joining an identical request already owned by this process.
    pub fn record_coalesced_caller(&self) -> Result<(), AdmissionRejection> {
        let mut state = self
            .inner
            .lock()
            .map_err(|_| AdmissionRejection::StateUnavailable)?;
        state.snapshot.coalesced_callers_total =
            checked_add(state.snapshot.coalesced_callers_total, 1)?;
        Ok(())
    }

    pub fn snapshot(&self) -> Result<AdmissionSnapshot, AdmissionRejection> {
        let state = self
            .inner
            .lock()
            .map_err(|_| AdmissionRejection::StateUnavailable)?;
        let mut snapshot = state.snapshot.clone();
        snapshot.circuit = state.circuit.snapshot();
        Ok(snapshot)
    }
}

#[derive(Debug)]
pub struct AttemptPermit {
    inner: Arc<Mutex<AdmissionState>>,
    attempt_id: u64,
    completed: bool,
}

impl AttemptPermit {
    /// Records one actual upstream request completed inside this admitted logical operation.
    pub fn record_actual_attempt(
        &mut self,
        kind: AttemptKind,
        outcome: AttemptOutcome,
        completed_at_unix_ms: i64,
    ) -> Result<(), AdmissionRejection> {
        let mut state = self
            .inner
            .lock()
            .map_err(|_| AdmissionRejection::StateUnavailable)?;
        record_actual_attempt(
            &mut state,
            self.attempt_id,
            kind,
            outcome,
            completed_at_unix_ms,
        )
    }

    /// Releases the logical-operation lane after all actual attempts have been recorded.
    pub fn finish(
        mut self,
        successful: bool,
        completed_at_unix_ms: i64,
    ) -> Result<(), AdmissionRejection> {
        {
            let mut state = self
                .inner
                .lock()
                .map_err(|_| AdmissionRejection::StateUnavailable)?;
            finish_operation(
                &mut state,
                self.attempt_id,
                successful,
                completed_at_unix_ms,
            )?;
        }
        self.completed = true;
        Ok(())
    }

    pub fn complete(
        mut self,
        outcome: AttemptOutcome,
        completed_at_unix_ms: i64,
    ) -> Result<(), AdmissionRejection> {
        {
            let mut state = self
                .inner
                .lock()
                .map_err(|_| AdmissionRejection::StateUnavailable)?;
            let successful = matches!(
                outcome.disposition,
                AttemptDisposition::Success | AttemptDisposition::Partial
            );
            record_actual_attempt(
                &mut state,
                self.attempt_id,
                AttemptKind::Primary,
                outcome,
                completed_at_unix_ms,
            )?;
            finish_operation(
                &mut state,
                self.attempt_id,
                successful,
                completed_at_unix_ms,
            )?;
        }
        self.completed = true;
        Ok(())
    }
}

impl Drop for AttemptPermit {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        if let Ok(mut state) = self.inner.lock() {
            let Some(active) = state.active.as_ref() else {
                return;
            };
            if active.id != self.attempt_id {
                return;
            }
            let completed_at = active.started_at_unix_ms;
            state.snapshot.cancelled_attempts_total =
                state.snapshot.cancelled_attempts_total.saturating_add(1);
            let _ = finish_operation(&mut state, self.attempt_id, false, completed_at);
        }
    }
}

fn record_actual_attempt(
    state: &mut AdmissionState,
    attempt_id: u64,
    kind: AttemptKind,
    outcome: AttemptOutcome,
    completed_at_unix_ms: i64,
) -> Result<(), AdmissionRejection> {
    let active = state
        .active
        .as_ref()
        .filter(|active| active.id == attempt_id)
        .cloned()
        .ok_or(AdmissionRejection::StalePermit)?;
    let accounted_units = outcome
        .returned_units
        .checked_add(outcome.missing_units)
        .ok_or(AdmissionRejection::CounterOverflow)?;
    let expected_units = if attempt_carries_observation_units(kind) {
        active.requested_units
    } else {
        0
    };
    if accounted_units != expected_units {
        return Err(AdmissionRejection::OutcomeUnitMismatch);
    }

    state.snapshot.actual_http_attempts_total =
        checked_add(state.snapshot.actual_http_attempts_total, 1)?;
    state.snapshot.requested_units_total = checked_add(
        state.snapshot.requested_units_total,
        u64::try_from(accounted_units).map_err(|_| AdmissionRejection::CounterOverflow)?,
    )?;
    state.snapshot.returned_units_total = checked_add(
        state.snapshot.returned_units_total,
        u64::try_from(outcome.returned_units).map_err(|_| AdmissionRejection::CounterOverflow)?,
    )?;
    state.snapshot.missing_units_total = checked_add(
        state.snapshot.missing_units_total,
        u64::try_from(outcome.missing_units).map_err(|_| AdmissionRejection::CounterOverflow)?,
    )?;
    state.snapshot.returned_records_total = checked_add(
        state.snapshot.returned_records_total,
        u64::try_from(outcome.returned_records).map_err(|_| AdmissionRejection::CounterOverflow)?,
    )?;
    state.snapshot.response_bytes_total = checked_add(
        state.snapshot.response_bytes_total,
        u64::try_from(outcome.response_bytes).map_err(|_| AdmissionRejection::CounterOverflow)?,
    )?;
    state.snapshot.latency_ms_total =
        checked_add(state.snapshot.latency_ms_total, outcome.latency_ms)?;
    state.snapshot.maximum_observed_response_bytes = state
        .snapshot
        .maximum_observed_response_bytes
        .max(outcome.response_bytes);
    state.snapshot.maximum_observed_latency_ms = state
        .snapshot
        .maximum_observed_latency_ms
        .max(outcome.latency_ms);

    let reopen = match outcome.disposition {
        AttemptDisposition::Success => {
            // Session bootstrap/crumb progress is not a successful market-data observation and
            // cannot erase provider data failures. Only a successful observation-carrying attempt
            // restores provider health.
            if attempt_carries_observation_units(kind) {
                state.snapshot.consecutive_failures = 0;
                state.snapshot.fallback_backoff_exponent = 0;
            }
            false
        }
        AttemptDisposition::Partial => active.half_open_probe,
        AttemptDisposition::ProviderBackoff {
            status,
            retry_after,
        } => {
            state.snapshot.provider_backoff_total =
                checked_add(state.snapshot.provider_backoff_total, 1)?;
            if status == 429 {
                state.snapshot.http_429_total = checked_add(state.snapshot.http_429_total, 1)?;
            }
            open_circuit(state, completed_at_unix_ms, retry_after);
            false
        }
        AttemptDisposition::TransportFailure => {
            state.snapshot.transport_failures_total =
                checked_add(state.snapshot.transport_failures_total, 1)?;
            increment_failure(state)?
        }
        AttemptDisposition::SchemaFailure => {
            state.snapshot.schema_failures_total =
                checked_add(state.snapshot.schema_failures_total, 1)?;
            increment_failure(state)?
        }
        AttemptDisposition::Cancelled => {
            state.snapshot.cancelled_attempts_total =
                checked_add(state.snapshot.cancelled_attempts_total, 1)?;
            // Caller revocation is not provider-health evidence and must not open the adaptive
            // provider circuit.
            false
        }
        AttemptDisposition::DeadlineExceeded => {
            state.snapshot.deadline_exceeded_attempts_total =
                checked_add(state.snapshot.deadline_exceeded_attempts_total, 1)?;
            // The application deadline is a local work bound, not proof of provider throttling or
            // schema/transport failure.
            false
        }
    };
    if reopen {
        open_circuit(state, completed_at_unix_ms, None);
    }
    Ok(())
}

fn finish_operation(
    state: &mut AdmissionState,
    attempt_id: u64,
    successful: bool,
    completed_at_unix_ms: i64,
) -> Result<(), AdmissionRejection> {
    let active = state
        .active
        .as_ref()
        .filter(|active| active.id == attempt_id)
        .cloned()
        .ok_or(AdmissionRejection::StalePermit)?;
    if active.half_open_probe && !matches!(state.circuit, CircuitState::Open { .. }) {
        if successful {
            state.circuit = CircuitState::Closed;
            state.snapshot.consecutive_failures = 0;
        } else {
            // A half-open operation can end without new provider-failure evidence (for example,
            // caller cancellation or a local deadline). Preserve the single-probe gate, but make
            // the next explicit demand eligible immediately instead of inventing a cooldown.
            state.circuit = CircuitState::Open {
                retry_at_unix_ms: completed_at_unix_ms,
            };
        }
    }
    state.active = None;
    state.snapshot.active_request_key = None;
    state.snapshot.circuit = state.circuit.snapshot();
    Ok(())
}

const fn attempt_carries_observation_units(kind: AttemptKind) -> bool {
    matches!(
        kind,
        AttemptKind::Primary
            | AttemptKind::CookieStrategyFallback
            | AttemptKind::RepairSubrequest
            | AttemptKind::HalfOpenProbe
    )
}

fn increment_failure(state: &mut AdmissionState) -> Result<bool, AdmissionRejection> {
    state.snapshot.consecutive_failures = state
        .snapshot
        .consecutive_failures
        .checked_add(1)
        .ok_or(AdmissionRejection::CounterOverflow)?;
    Ok(state.snapshot.consecutive_failures >= state.policy.repeated_failure_threshold)
}

fn open_circuit(
    state: &mut AdmissionState,
    now_unix_ms: i64,
    retry_after: Option<YahooRetryAfterDirective>,
) {
    let provider_retry_at = match retry_after {
        Some(YahooRetryAfterDirective::DeltaSeconds { seconds }) => seconds
            .checked_mul(1_000)
            .map(|delay_ms| add_millis(now_unix_ms, delay_ms)),
        Some(YahooRetryAfterDirective::HttpDate { retry_at_unix_ms })
            if retry_at_unix_ms > now_unix_ms =>
        {
            Some(retry_at_unix_ms)
        }
        Some(YahooRetryAfterDirective::HttpDate { .. }) | None => None,
    };
    let retry_at_unix_ms = provider_retry_at.unwrap_or_else(|| {
        let cooldown = fallback_cooldown_with_jitter(state, now_unix_ms);
        state.snapshot.fallback_backoff_exponent = state
            .snapshot
            .fallback_backoff_exponent
            .saturating_add(1)
            .min(MAX_FALLBACK_BACKOFF_EXPONENT);
        add_millis(now_unix_ms, cooldown)
    });
    state.circuit = CircuitState::Open {
        // A valid provider instruction is exact and is never lengthened by local policy. Only an
        // absent, malformed, or already-expired instruction advances the bounded adaptive
        // fallback. The next successful provider observation resets that fallback immediately.
        retry_at_unix_ms,
    };
}

fn fallback_cooldown_with_jitter(state: &AdmissionState, now_unix_ms: i64) -> u64 {
    let mut sample = u64::from_ne_bytes(now_unix_ms.to_ne_bytes())
        ^ state.snapshot.actual_http_attempts_total.rotate_left(17)
        ^ state
            .snapshot
            .logical_primary_operations_total
            .rotate_left(41);
    // SplitMix64 provides a well-distributed, dependency-free process/time sample. This is load
    // spreading, not security randomness; the chosen absolute retry coordinate is persisted.
    sample = sample.wrapping_add(0x9e37_79b9_7f4a_7c15);
    sample = (sample ^ (sample >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    sample = (sample ^ (sample >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    sample ^= sample >> 31;
    let jitter = sample % state.policy.fallback_max_jitter_ms.saturating_add(1);
    let multiplier = 1_u64
        << state
            .snapshot
            .fallback_backoff_exponent
            .min(MAX_FALLBACK_BACKOFF_EXPONENT);
    state
        .policy
        .fallback_cooldown_ms
        .saturating_mul(multiplier)
        .saturating_add(jitter)
}

fn add_millis(now_unix_ms: i64, delay_ms: u64) -> i64 {
    let delay = i64::try_from(delay_ms).unwrap_or(i64::MAX);
    now_unix_ms.saturating_add(delay)
}

fn request_accounting_units(request: &YahooHttpRequest) -> usize {
    if !request.requested_targets.is_empty() {
        return request.requested_targets.len();
    }
    request
        .effective_arguments
        .get("requested_result_count")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1)
}

fn checked_add(left: u64, right: u64) -> Result<u64, AdmissionRejection> {
    left.checked_add(right)
        .ok_or(AdmissionRejection::CounterOverflow)
}

fn validate_restored_snapshot(
    policy: AdmissionPolicy,
    snapshot: &AdmissionSnapshot,
) -> Result<(), AdmissionRejection> {
    let accounted_units = snapshot
        .returned_units_total
        .checked_add(snapshot.missing_units_total)
        .ok_or(AdmissionRejection::InvalidPersistedState)?;
    let classified_attempts = snapshot
        .provider_backoff_total
        .checked_add(snapshot.transport_failures_total)
        .and_then(|value| value.checked_add(snapshot.schema_failures_total))
        .and_then(|value| value.checked_add(snapshot.cancelled_attempts_total))
        .and_then(|value| value.checked_add(snapshot.deadline_exceeded_attempts_total))
        .ok_or(AdmissionRejection::InvalidPersistedState)?;
    if snapshot.active_request_key.is_some()
        || matches!(snapshot.circuit, CircuitSnapshot::HalfOpen)
        || accounted_units != snapshot.requested_units_total
        || snapshot.maximum_observed_response_bytes
            > usize::try_from(snapshot.response_bytes_total).unwrap_or(usize::MAX)
        || snapshot.maximum_observed_latency_ms > snapshot.latency_ms_total
        || classified_attempts > snapshot.actual_http_attempts_total
        || snapshot.http_429_total > snapshot.provider_backoff_total
        || snapshot.fallback_backoff_exponent > MAX_FALLBACK_BACKOFF_EXPONENT
        || (matches!(snapshot.circuit, CircuitSnapshot::Closed)
            && snapshot.fallback_backoff_exponent != 0)
        || (matches!(snapshot.circuit, CircuitSnapshot::Closed)
            && snapshot.consecutive_failures >= policy.repeated_failure_threshold)
    {
        return Err(AdmissionRejection::InvalidPersistedState);
    }
    Ok(())
}
