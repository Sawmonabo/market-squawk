//! Source health evidence with independent liveness and market-freshness clocks.

use market_squawk_domain::{
    CaptureIntegrityState, ConnectionGeneration, ExactPayloadEvidence, MetadataRevision,
    ProviderChannel, ProviderProduct, SourceId, SourceIdentifier, StreamIntegrityState, Timestamp,
};
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

use crate::bounded::BoundedVec;
use crate::{CurrentSourceSession, FrameSessionBinding, FreshnessPolicy, SessionId};

const MAX_COVERAGE_LIMITATIONS: usize = 64;

/// Connection liveness state. Heartbeats only update this clock.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionLiveness {
    /// Connection setup is in progress.
    Connecting,
    /// Transport is connected; the timestamp is the latest connection-level activity.
    Live { last_activity_at: Timestamp },
    /// Transport exists but connection-level activity exceeded its configured idle limit.
    Stale { last_activity_at: Timestamp },
    /// Transport is disconnected.
    Disconnected { disconnected_at: Timestamp },
}

/// Derived market-data freshness, kept separate from connection liveness.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketFreshness {
    /// No market-bearing event has initialized freshness.
    Uninitialized,
    /// Latest market-bearing event remains within the configured limit.
    Fresh { last_market_at: Timestamp },
    /// Latest market-bearing event is outside the configured limit.
    Stale { last_market_at: Timestamp },
}

/// Raw transport-frame age independent of market-bearing event age.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TransportFreshness {
    /// No raw frame has initialized transport freshness.
    Uninitialized,
    /// Latest raw frame remains within the transport-age limit.
    Fresh { last_transport_at: Timestamp },
    /// Latest raw frame is outside the transport-age limit.
    Stale { last_transport_at: Timestamp },
}

/// Provider-source timestamp freshness, separate from receive/transport freshness.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceTimestampFreshness {
    /// No provider-source timestamp has initialized this generation.
    Uninitialized,
    /// Latest source timestamp satisfies age and future-skew limits.
    Fresh { last_source_at: Timestamp },
    /// Latest source timestamp is older than the configured source-age limit.
    Stale { last_source_at: Timestamp },
}

/// Shared provider-budget health without exposing credentials or alternate identities.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BudgetHealth {
    /// A request may be considered for dispatch.
    Available,
    /// Requests are waiting until an inclusive deadline.
    CoolingDown { until: Timestamp },
    /// Budget is unavailable until explicit reconfiguration or another external change.
    Unavailable,
}

/// Current authorization and entitlement state for the exact source session.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorizationHealth {
    /// Current credentials/terms/entitlements are evidenced through an inclusive deadline.
    Valid {
        /// Exact runtime authorization/entitlement evidence.
        evidence: ExactPayloadEvidence,
        /// Inclusive runtime authorization deadline.
        valid_until: Timestamp,
    },
    /// Authorization has not yet been established for this generation.
    Uninitialized,
    /// Provider rejected or revoked current authorization.
    Invalid,
}

/// Runtime subscription/coverage state independent of static metadata declarations.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CoverageHealth {
    /// Current subscription acknowledgements establish the exact product/channel scope.
    Sufficient {
        /// Exact provider subscription acknowledgement evidence.
        evidence: ExactPayloadEvidence,
        /// Exact provider product acknowledged at runtime.
        provider_product: ProviderProduct,
        /// Exact provider channel acknowledged at runtime.
        provider_channel: ProviderChannel,
        /// Inclusive runtime subscription deadline.
        valid_until: Timestamp,
    },
    /// Subscription scope has not yet been established for this generation.
    Uninitialized,
    /// Provider acknowledgements or observations establish only partial coverage.
    Limited,
}

/// Stable, non-secret source error classification for health reporting.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthErrorClass {
    /// Network connection or transport failure.
    Network,
    /// Provider authentication or entitlement failure.
    Authorization,
    /// Protocol decoding failure.
    Decode,
    /// Sequence, checksum, snapshot, or book-integrity failure.
    Integrity,
    /// Provider throttling or explicit blocking response.
    ProviderLimit,
    /// Bounded local queue saturation or closure.
    LocalBackpressure,
}

/// Immutable health snapshot bound to one exact metadata revision and connection generation.
#[derive(Clone, Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourceHealthSnapshot {
    #[serde(skip)]
    authority_binding: Option<FrameSessionBinding>,
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    session_id: SessionId,
    connection_generation: ConnectionGeneration,
    observed_at: Timestamp,
    connection: ConnectionLiveness,
    transport_freshness: TransportFreshness,
    market_freshness: MarketFreshness,
    source_freshness: SourceTimestampFreshness,
    max_transport_age_nanos: u64,
    max_source_age_nanos: u64,
    max_market_age_nanos: u64,
    max_clock_skew_nanos: u64,
    max_connection_idle_nanos: u64,
    stream_integrity: StreamIntegrityState,
    capture_integrity: CaptureIntegrityState,
    authorization: AuthorizationHealth,
    coverage: CoverageHealth,
    budget: BudgetHealth,
    last_error: Option<HealthErrorClass>,
    coverage_limitations: BoundedVec<SourceIdentifier, MAX_COVERAGE_LIMITATIONS>,
}

impl SourceHealthSnapshot {
    /// Constructs a health snapshot from a registry-issued current-session handle.
    ///
    /// Market freshness is derived only from a market-bearing timestamp. Connection activity can
    /// never refresh it.
    ///
    /// # Errors
    ///
    /// Rejects future observations, timestamp overflow, or excessive coverage limitations.
    #[allow(
        clippy::too_many_arguments,
        reason = "independent health dimensions remain explicit"
    )]
    pub fn try_new(
        session: &CurrentSourceSession,
        observed_at: Timestamp,
        connection: ConnectionLiveness,
        last_transport_at: Option<Timestamp>,
        last_market_at: Option<Timestamp>,
        last_source_at: Option<Timestamp>,
        freshness_policy: FreshnessPolicy,
        stream_integrity: StreamIntegrityState,
        capture_integrity: CaptureIntegrityState,
        authorization: AuthorizationHealth,
        coverage: CoverageHealth,
        budget: BudgetHealth,
        last_error: Option<HealthErrorClass>,
        coverage_limitations: Vec<SourceIdentifier>,
    ) -> Result<Self, SourceHealthError> {
        let connection = assess_connection_liveness(
            connection,
            observed_at,
            freshness_policy.max_connection_idle_nanos(),
        )?;
        validate_budget_health(budget, observed_at)?;
        let transport_freshness = assess_transport_freshness(
            last_transport_at,
            observed_at,
            freshness_policy.max_transport_age_nanos(),
        )?;
        let market_freshness = assess_market_freshness(
            last_market_at,
            observed_at,
            freshness_policy.max_market_age_nanos(),
        )?;
        let source_freshness = assess_source_freshness(
            last_source_at,
            observed_at,
            freshness_policy.max_source_age_nanos(),
            freshness_policy.max_clock_skew_nanos(),
        )?;
        validate_runtime_deadlines(&authorization, &coverage, observed_at)?;
        let coverage_limitations = BoundedVec::try_new(coverage_limitations)
            .map_err(|error| SourceHealthError::TooManyCoverageLimitations { max: error.max })?;
        Ok(Self {
            authority_binding: Some(session.frame_binding().clone()),
            source_id: session.source_id().clone(),
            metadata_revision: session.revision().clone(),
            session_id: session.session_id().clone(),
            connection_generation: session.generation(),
            observed_at,
            connection,
            transport_freshness,
            market_freshness,
            source_freshness,
            max_transport_age_nanos: freshness_policy.max_transport_age_nanos(),
            max_source_age_nanos: freshness_policy.max_source_age_nanos(),
            max_market_age_nanos: freshness_policy.max_market_age_nanos(),
            max_clock_skew_nanos: freshness_policy.max_clock_skew_nanos(),
            max_connection_idle_nanos: freshness_policy.max_connection_idle_nanos(),
            stream_integrity,
            capture_integrity,
            authorization,
            coverage,
            budget,
            last_error,
            coverage_limitations,
        })
    }

    /// Returns the exact source identity.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the metadata revision to which health applies.
    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    /// Returns the source-defined session identity.
    pub const fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    /// Returns the exact connection generation.
    pub const fn connection_generation(&self) -> ConnectionGeneration {
        self.connection_generation
    }

    /// Returns connection liveness independently of market freshness.
    pub const fn connection(&self) -> ConnectionLiveness {
        self.connection
    }

    /// Returns market freshness that no heartbeat can update.
    pub const fn market_freshness(&self) -> MarketFreshness {
        self.market_freshness
    }

    /// Returns raw transport freshness independently of market activity.
    pub const fn transport_freshness(&self) -> TransportFreshness {
        self.transport_freshness
    }

    /// Returns provider-source timestamp freshness independently of transport age.
    pub const fn source_freshness(&self) -> SourceTimestampFreshness {
        self.source_freshness
    }

    /// Returns asynchronous raw-capture integrity for this exact generation.
    pub const fn capture_integrity(&self) -> CaptureIntegrityState {
        self.capture_integrity
    }

    /// Returns current authorization/entitlement health.
    pub const fn authorization(&self) -> &AuthorizationHealth {
        &self.authorization
    }

    /// Returns current runtime subscription/coverage health.
    pub const fn coverage(&self) -> &CoverageHealth {
        &self.coverage
    }

    /// Returns when the snapshot was observed.
    pub const fn observed_at(&self) -> Timestamp {
        self.observed_at
    }

    /// Returns the operational stream-integrity state.
    pub const fn stream_integrity(&self) -> StreamIntegrityState {
        self.stream_integrity
    }

    /// Returns shared provider-budget health.
    pub const fn budget(&self) -> BudgetHealth {
        self.budget
    }

    /// Returns the last stable non-secret error class.
    pub const fn last_error(&self) -> Option<HealthErrorClass> {
        self.last_error
    }

    /// Returns the earliest inclusive deadline across every dynamic health dimension.
    ///
    /// `None` means the snapshot is not fully qualified, not that authority is unbounded.
    pub fn live_valid_until(&self) -> Option<Timestamp> {
        let connection_at = match self.connection {
            ConnectionLiveness::Live { last_activity_at } => last_activity_at,
            _ => return None,
        };
        let market_at = match self.market_freshness {
            MarketFreshness::Fresh { last_market_at } => last_market_at,
            _ => return None,
        };
        let transport_at = match self.transport_freshness {
            TransportFreshness::Fresh { last_transport_at } => last_transport_at,
            _ => return None,
        };
        let source_at = match self.source_freshness {
            SourceTimestampFreshness::Fresh { last_source_at } => last_source_at,
            _ => return None,
        };
        let authorization_until = match &self.authorization {
            AuthorizationHealth::Valid { valid_until, .. } => *valid_until,
            _ => return None,
        };
        let coverage_until = match &self.coverage {
            CoverageHealth::Sufficient { valid_until, .. } => *valid_until,
            _ => return None,
        };
        let connection_until = checked_deadline(connection_at, self.max_connection_idle_nanos)?;
        let transport_until = checked_deadline(transport_at, self.max_transport_age_nanos)?;
        let market_until = checked_deadline(market_at, self.max_market_age_nanos)?;
        let source_until =
            checked_deadline(source_at.min(self.observed_at), self.max_source_age_nanos)?;
        [
            connection_until,
            transport_until,
            market_until,
            source_until,
            authorization_until,
            coverage_until,
        ]
        .into_iter()
        .min()
    }

    /// Returns bounded explicit coverage limitations.
    pub fn coverage_limitations(&self) -> &[SourceIdentifier] {
        self.coverage_limitations.as_slice()
    }

    pub(crate) fn uses_freshness_policy(&self, policy: FreshnessPolicy) -> bool {
        self.max_connection_idle_nanos == policy.max_connection_idle_nanos()
            && self.max_transport_age_nanos == policy.max_transport_age_nanos()
            && self.max_source_age_nanos == policy.max_source_age_nanos()
            && self.max_market_age_nanos == policy.max_market_age_nanos()
            && self.max_clock_skew_nanos == policy.max_clock_skew_nanos()
    }

    pub(crate) const fn authority_binding(&self) -> Option<&FrameSessionBinding> {
        self.authority_binding.as_ref()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceHealthSnapshotWire {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    session_id: SessionId,
    connection_generation: ConnectionGeneration,
    observed_at: Timestamp,
    connection: ConnectionLiveness,
    transport_freshness: TransportFreshness,
    market_freshness: MarketFreshness,
    source_freshness: SourceTimestampFreshness,
    max_transport_age_nanos: u64,
    max_source_age_nanos: u64,
    max_market_age_nanos: u64,
    max_clock_skew_nanos: u64,
    max_connection_idle_nanos: u64,
    stream_integrity: StreamIntegrityState,
    capture_integrity: CaptureIntegrityState,
    authorization: AuthorizationHealth,
    coverage: CoverageHealth,
    budget: BudgetHealth,
    last_error: Option<HealthErrorClass>,
    coverage_limitations: BoundedVec<SourceIdentifier, MAX_COVERAGE_LIMITATIONS>,
}

impl<'de> Deserialize<'de> for SourceHealthSnapshot {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = SourceHealthSnapshotWire::deserialize(deserializer)?;
        if wire.max_transport_age_nanos == 0
            || wire.max_source_age_nanos == 0
            || wire.max_market_age_nanos == 0
            || wire.max_connection_idle_nanos == 0
        {
            return Err(serde::de::Error::custom(SourceHealthError::ZeroMarketAge));
        }
        let derived_connection = assess_connection_liveness(
            wire.connection,
            wire.observed_at,
            wire.max_connection_idle_nanos,
        )
        .map_err(serde::de::Error::custom)?;
        if derived_connection != wire.connection {
            return Err(serde::de::Error::custom(
                SourceHealthError::TamperedLiveness,
            ));
        }
        validate_budget_health(wire.budget, wire.observed_at).map_err(serde::de::Error::custom)?;
        let last_transport_at = match wire.transport_freshness {
            TransportFreshness::Uninitialized => None,
            TransportFreshness::Fresh { last_transport_at }
            | TransportFreshness::Stale { last_transport_at } => Some(last_transport_at),
        };
        let derived_transport = assess_transport_freshness(
            last_transport_at,
            wire.observed_at,
            wire.max_transport_age_nanos,
        )
        .map_err(serde::de::Error::custom)?;
        if derived_transport != wire.transport_freshness {
            return Err(serde::de::Error::custom(
                SourceHealthError::TamperedFreshness,
            ));
        }
        let last_market_at = match wire.market_freshness {
            MarketFreshness::Uninitialized => None,
            MarketFreshness::Fresh { last_market_at }
            | MarketFreshness::Stale { last_market_at } => Some(last_market_at),
        };
        let derived =
            assess_market_freshness(last_market_at, wire.observed_at, wire.max_market_age_nanos)
                .map_err(serde::de::Error::custom)?;
        if derived != wire.market_freshness {
            return Err(serde::de::Error::custom(
                SourceHealthError::TamperedFreshness,
            ));
        }
        let last_source_at = match wire.source_freshness {
            SourceTimestampFreshness::Uninitialized => None,
            SourceTimestampFreshness::Fresh { last_source_at }
            | SourceTimestampFreshness::Stale { last_source_at } => Some(last_source_at),
        };
        let derived_source = assess_source_freshness(
            last_source_at,
            wire.observed_at,
            wire.max_source_age_nanos,
            wire.max_clock_skew_nanos,
        )
        .map_err(serde::de::Error::custom)?;
        if derived_source != wire.source_freshness {
            return Err(serde::de::Error::custom(
                SourceHealthError::TamperedFreshness,
            ));
        }
        validate_runtime_deadlines(&wire.authorization, &wire.coverage, wire.observed_at)
            .map_err(serde::de::Error::custom)?;
        Ok(Self {
            authority_binding: None,
            source_id: wire.source_id,
            metadata_revision: wire.metadata_revision,
            session_id: wire.session_id,
            connection_generation: wire.connection_generation,
            observed_at: wire.observed_at,
            connection: wire.connection,
            transport_freshness: wire.transport_freshness,
            market_freshness: wire.market_freshness,
            source_freshness: wire.source_freshness,
            max_transport_age_nanos: wire.max_transport_age_nanos,
            max_source_age_nanos: wire.max_source_age_nanos,
            max_market_age_nanos: wire.max_market_age_nanos,
            max_clock_skew_nanos: wire.max_clock_skew_nanos,
            max_connection_idle_nanos: wire.max_connection_idle_nanos,
            stream_integrity: wire.stream_integrity,
            capture_integrity: wire.capture_integrity,
            authorization: wire.authorization,
            coverage: wire.coverage,
            budget: wire.budget,
            last_error: wire.last_error,
            coverage_limitations: wire.coverage_limitations,
        })
    }
}

/// Health construction or wire-validation failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum SourceHealthError {
    /// Connection or market timestamp was later than observation time.
    #[error("health timestamp cannot be later than observation time")]
    FutureTimestamp,
    /// Checked freshness arithmetic overflowed.
    #[error("market freshness arithmetic overflow")]
    FreshnessOverflow,
    /// Serialized freshness did not match the retained operands.
    #[error("serialized market freshness does not match retained timestamps")]
    TamperedFreshness,
    /// Serialized liveness did not match its retained activity timestamp and idle bound.
    #[error("serialized connection liveness does not match retained operands")]
    TamperedLiveness,
    /// Serialized market age was zero.
    #[error("market age threshold must be positive")]
    ZeroMarketAge,
    /// Coverage limitation count exceeded its bound.
    #[error("coverage limitations exceed maximum {max}")]
    TooManyCoverageLimitations {
        /// Maximum retained limitation count.
        max: usize,
    },
    /// A reported cooldown already expired at observation time.
    #[error("budget cooldown must be later than health observation time")]
    StaleCooldown,
    /// Provider timestamp exceeded the configured future-skew ceiling.
    #[error("provider timestamp exceeds maximum future clock skew")]
    ClockSkewExceeded,
    /// Runtime authorization or subscription evidence expired before observation.
    #[error("runtime authorization or coverage evidence is expired")]
    StaleRuntimeEvidence,
}

fn checked_deadline(at: Timestamp, nanos: u64) -> Option<Timestamp> {
    let nanos = i64::try_from(nanos).ok()?;
    at.checked_add_nanos(nanos).ok()
}

fn validate_runtime_deadlines(
    authorization: &AuthorizationHealth,
    coverage: &CoverageHealth,
    observed_at: Timestamp,
) -> Result<(), SourceHealthError> {
    let authorization_valid = !matches!(
        authorization,
        AuthorizationHealth::Valid { valid_until, .. } if *valid_until < observed_at
    );
    let coverage_valid = !matches!(
        coverage,
        CoverageHealth::Sufficient { valid_until, .. } if *valid_until < observed_at
    );
    if authorization_valid && coverage_valid {
        Ok(())
    } else {
        Err(SourceHealthError::StaleRuntimeEvidence)
    }
}

fn validate_connection_time(
    connection: ConnectionLiveness,
    observed_at: Timestamp,
) -> Result<(), SourceHealthError> {
    let timestamp = match connection {
        ConnectionLiveness::Connecting => None,
        ConnectionLiveness::Live { last_activity_at }
        | ConnectionLiveness::Stale { last_activity_at } => Some(last_activity_at),
        ConnectionLiveness::Disconnected { disconnected_at } => Some(disconnected_at),
    };
    if timestamp.is_some_and(|at| at > observed_at) {
        Err(SourceHealthError::FutureTimestamp)
    } else {
        Ok(())
    }
}

fn assess_connection_liveness(
    connection: ConnectionLiveness,
    observed_at: Timestamp,
    max_connection_idle_nanos: u64,
) -> Result<ConnectionLiveness, SourceHealthError> {
    validate_connection_time(connection, observed_at)?;
    let last_activity_at = match connection {
        ConnectionLiveness::Live { last_activity_at }
        | ConnectionLiveness::Stale { last_activity_at } => last_activity_at,
        other => return Ok(other),
    };
    let max_idle = i64::try_from(max_connection_idle_nanos)
        .map_err(|_| SourceHealthError::FreshnessOverflow)?;
    let stale_at = last_activity_at
        .checked_add_nanos(max_idle)
        .map_err(|_| SourceHealthError::FreshnessOverflow)?;
    if observed_at <= stale_at {
        Ok(ConnectionLiveness::Live { last_activity_at })
    } else {
        Ok(ConnectionLiveness::Stale { last_activity_at })
    }
}

fn validate_budget_health(
    budget: BudgetHealth,
    observed_at: Timestamp,
) -> Result<(), SourceHealthError> {
    if matches!(budget, BudgetHealth::CoolingDown { until } if until <= observed_at) {
        Err(SourceHealthError::StaleCooldown)
    } else {
        Ok(())
    }
}

fn assess_market_freshness(
    last_market_at: Option<Timestamp>,
    observed_at: Timestamp,
    max_market_age_nanos: u64,
) -> Result<MarketFreshness, SourceHealthError> {
    let Some(last_market_at) = last_market_at else {
        return Ok(MarketFreshness::Uninitialized);
    };
    if last_market_at > observed_at {
        return Err(SourceHealthError::FutureTimestamp);
    }
    let max_age =
        i64::try_from(max_market_age_nanos).map_err(|_| SourceHealthError::FreshnessOverflow)?;
    let stale_at = last_market_at
        .checked_add_nanos(max_age)
        .map_err(|_| SourceHealthError::FreshnessOverflow)?;
    if observed_at <= stale_at {
        Ok(MarketFreshness::Fresh { last_market_at })
    } else {
        Ok(MarketFreshness::Stale { last_market_at })
    }
}

fn assess_transport_freshness(
    last_transport_at: Option<Timestamp>,
    observed_at: Timestamp,
    max_transport_age_nanos: u64,
) -> Result<TransportFreshness, SourceHealthError> {
    let Some(last_transport_at) = last_transport_at else {
        return Ok(TransportFreshness::Uninitialized);
    };
    if last_transport_at > observed_at {
        return Err(SourceHealthError::FutureTimestamp);
    }
    let stale_at = checked_deadline(last_transport_at, max_transport_age_nanos)
        .ok_or(SourceHealthError::FreshnessOverflow)?;
    if observed_at <= stale_at {
        Ok(TransportFreshness::Fresh { last_transport_at })
    } else {
        Ok(TransportFreshness::Stale { last_transport_at })
    }
}

fn assess_source_freshness(
    last_source_at: Option<Timestamp>,
    observed_at: Timestamp,
    max_source_age_nanos: u64,
    max_clock_skew_nanos: u64,
) -> Result<SourceTimestampFreshness, SourceHealthError> {
    let Some(last_source_at) = last_source_at else {
        return Ok(SourceTimestampFreshness::Uninitialized);
    };
    if last_source_at > observed_at {
        let skew_limit = checked_deadline(observed_at, max_clock_skew_nanos)
            .ok_or(SourceHealthError::FreshnessOverflow)?;
        return if last_source_at <= skew_limit {
            Ok(SourceTimestampFreshness::Fresh { last_source_at })
        } else {
            Err(SourceHealthError::ClockSkewExceeded)
        };
    }
    let stale_at = checked_deadline(last_source_at, max_source_age_nanos)
        .ok_or(SourceHealthError::FreshnessOverflow)?;
    if observed_at <= stale_at {
        Ok(SourceTimestampFreshness::Fresh { last_source_at })
    } else {
        Ok(SourceTimestampFreshness::Stale { last_source_at })
    }
}
