//! Independent data, valuation, depth, integrity, and execution classifications.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};

use crate::Timestamp;

#[path = "classification/qualification.rs"]
mod qualification;

pub use qualification::{
    EligibilityFailure, EligibilityFailures, QualificationEvidence, QualificationEvidenceInput,
};

/// Fair-value input hierarchy under ASC 820 and IFRS 13.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FairValueHierarchy {
    /// Unadjusted quoted price for an identical asset or liability in an active accessible market.
    Level1,
    /// Observable input other than qualifying Level 1 evidence.
    Level2,
    /// Significant unobservable input.
    Level3,
    /// No hierarchy conclusion has been made.
    Unclassified,
}

/// Granularity supplied by a market-data source.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketDepth {
    /// Best bid and ask only.
    TopOfBook,
    /// Aggregated quantities at each price level.
    PriceLevel,
    /// Individual orders where the venue supplies them.
    OrderLevel,
}

/// Evidentiary quality of an observation, independent of depth and fair-value hierarchy.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DataQuality {
    /// Direct authorized delivery whose integrity evidence is fully verified.
    DirectVerified,
    /// Direct delivery lacking one or more verification capabilities or results.
    DirectUnverified,
    /// Official source data delivered with an explicit delay.
    OfficialDelayed,
    /// Data combined or redistributed by an aggregator.
    Aggregated,
    /// Non-firm or otherwise indicative data.
    Indicative,
    /// Output of a model rather than a direct observation.
    Modeled,
    /// Estimated input or value.
    Estimated,
    /// Observation outside its configured freshness limit.
    Stale,
    /// Observation isolated because an integrity invariant failed.
    Quarantined,
}

/// Operational integrity of a decoded live stream.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamIntegrityState {
    /// A connection exists but no synchronization evidence has arrived.
    Initializing,
    /// Snapshot and update state are being synchronized.
    Synchronizing,
    /// Candidate state is being checked before qualification.
    Validating,
    /// All required stream checks currently pass.
    Healthy,
    /// Market state is older than its configured limit.
    Stale,
    /// A required sequence value was skipped.
    GapDetected,
    /// A supported checksum did not match.
    ChecksumFailed,
    /// Independent source state disagrees with local state.
    Divergent,
    /// The stream is isolated until explicit resynchronization.
    Quarantined,
}

/// Operational integrity of optional asynchronous raw capture.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CaptureIntegrityState {
    /// Capture is explicitly disabled.
    Disabled,
    /// Capture is keeping up without known loss.
    Healthy,
    /// Capture is known to be incomplete, regardless of the control-plane failure cause.
    Incomplete,
}

/// Whether evidence may be consumed by immediate automated action.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionEligibility {
    /// Every required qualification condition passed.
    Eligible,
    /// At least one required qualification condition failed.
    Ineligible,
}

/// Authorization status established by source configuration and credentials.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceAuthorization {
    /// The configured direct source is authorized for this use.
    Authorized,
    /// Authorization is absent, unknown, or invalid.
    Unauthorized,
}

/// Delivery relationship established independently of a source's authorization status.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryEvidence {
    /// Data arrived directly from the identified trading venue.
    DirectVenue,
    /// Data arrived from a broker explicitly authorized for the account and market-data use.
    AuthorizedBroker,
    /// Data arrived through an aggregator, redistribution path, or other indirect channel.
    Indirect,
    /// The delivery relationship has not been established.
    Unknown,
}

/// Result of provider-specific sequence validation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SequenceIntegrity {
    /// Sequence progression was valid.
    Valid,
    /// Authoritative metadata establishes that the protocol supplies no sequence.
    NotSupported,
    /// A duplicate, gap, regression, or out-of-order update was observed.
    Invalid,
    /// Sequence qualification has not completed.
    Uninitialized,
}

/// Result of snapshot and incremental-update consistency validation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotConsistency {
    /// Snapshot and subsequent updates form one consistent generation.
    Consistent,
    /// Snapshot/update ordering or generation was inconsistent.
    Inconsistent,
    /// No qualifying snapshot has been established.
    Uninitialized,
}

/// Result of a provider-specific checksum capability and validation check.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChecksumIntegrity {
    /// A supported checksum matched.
    Valid,
    /// Authoritative metadata establishes that the protocol supplies no checksum.
    NotSupported,
    /// A supported checksum did not match.
    Failed,
    /// A checksum capability exists but has not been checked.
    Unchecked,
}

/// Result of instrument tick-size and lot-size validation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PrecisionIntegrity {
    /// Every price and quantity is exactly representable in configured ticks and lots.
    Valid,
    /// At least one value violates instrument precision.
    Invalid,
}

/// Whether the source explicitly covers the instrument, venue, and event class.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceCoverageEvidence {
    /// Coverage is explicit and sufficient for the candidate action.
    Explicit,
    /// Coverage is known to be partial or otherwise insufficient.
    Insufficient,
    /// Coverage has not been established.
    Unknown,
}

/// Structural state of the candidate market book.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BookIntegrity {
    /// The book is internally consistent and not crossed.
    Consistent,
    /// Best bid is at or above best ask.
    Crossed,
    /// Book consistency has not been established.
    Unknown,
}

/// Sanity result for exchange and receive timestamps.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TimestampIntegrity {
    /// A source timestamp exists and is within configured future skew.
    Valid,
    /// The source timestamp is missing or implausibly later than receive time.
    Invalid,
}

/// Freshness derived only from a valid market event, never from a heartbeat.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FreshnessState {
    /// The latest market event is within the configured age.
    Fresh,
    /// The latest market event exceeds the configured age.
    Stale,
    /// No market event has established freshness.
    Unknown,
}

/// A failure to assess time-based evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClassificationError {
    /// A nanosecond limit cannot be represented by the timestamp scalar.
    DurationTooLarge,
    /// An observation claims to occur after the evaluation instant.
    ObservationAfterEvaluation,
}

impl fmt::Display for ClassificationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DurationTooLarge => {
                formatter.write_str("nanosecond evidence limit exceeds signed timestamp range")
            }
            Self::ObservationAfterEvaluation => {
                formatter.write_str("evidence observation occurs after evaluation time")
            }
        }
    }
}

impl std::error::Error for ClassificationError {}

/// Exchange/receive timestamp values and their derived sanity result.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct EventTimingEvidence {
    source_timestamp: Option<Timestamp>,
    received_at: Timestamp,
    maximum_future_skew_nanos: u64,
    integrity: TimestampIntegrity,
}

#[derive(Deserialize)]
struct EventTimingEvidenceWire {
    source_timestamp: Option<Timestamp>,
    received_at: Timestamp,
    maximum_future_skew_nanos: u64,
    integrity: TimestampIntegrity,
}

impl<'de> Deserialize<'de> for EventTimingEvidence {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = EventTimingEvidenceWire::deserialize(deserializer)?;
        let evidence = Self::assess(
            wire.source_timestamp,
            wire.received_at,
            wire.maximum_future_skew_nanos,
        )
        .map_err(serde::de::Error::custom)?;
        if evidence.integrity != wire.integrity {
            return Err(serde::de::Error::custom(
                "serialized timestamp integrity does not match timestamp evidence",
            ));
        }
        Ok(evidence)
    }
}

impl EventTimingEvidence {
    /// Assesses timestamp sanity using an explicit source-clock skew allowance.
    ///
    /// A missing source timestamp is retained as invalid evidence instead of inventing one.
    ///
    /// # Errors
    ///
    /// Returns [`ClassificationError::DurationTooLarge`] when the skew limit exceeds `i64`.
    pub fn assess(
        source_timestamp: Option<Timestamp>,
        received_at: Timestamp,
        maximum_future_skew_nanos: u64,
    ) -> Result<Self, ClassificationError> {
        let maximum_future_skew = i64::try_from(maximum_future_skew_nanos)
            .map_err(|_| ClassificationError::DurationTooLarge)?;
        let integrity = match source_timestamp {
            Some(source_at) => {
                let latest_allowed = received_at
                    .checked_add_nanos(maximum_future_skew)
                    .unwrap_or(Timestamp::from_unix_nanos(i64::MAX));
                if source_at <= latest_allowed {
                    TimestampIntegrity::Valid
                } else {
                    TimestampIntegrity::Invalid
                }
            }
            None => TimestampIntegrity::Invalid,
        };
        Ok(Self {
            source_timestamp,
            received_at,
            maximum_future_skew_nanos,
            integrity,
        })
    }

    /// Returns the derived timestamp-sanity state.
    pub const fn integrity(self) -> TimestampIntegrity {
        self.integrity
    }

    /// Returns the source timestamp without manufacturing a missing value.
    pub const fn source_timestamp(self) -> Option<Timestamp> {
        self.source_timestamp
    }

    /// Returns when the frame reached the local process.
    pub const fn received_at(self) -> Timestamp {
        self.received_at
    }

    /// Returns the configured source-clock future-skew allowance in nanoseconds.
    pub const fn maximum_future_skew_nanos(self) -> u64 {
        self.maximum_future_skew_nanos
    }
}

/// Market-event and heartbeat timestamps with market-only freshness derivation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct FreshnessEvidence {
    last_market_event_at: Option<Timestamp>,
    last_heartbeat_at: Option<Timestamp>,
    evaluated_at: Timestamp,
    maximum_age_nanos: u64,
    state: FreshnessState,
}

#[derive(Deserialize)]
struct FreshnessEvidenceWire {
    last_market_event_at: Option<Timestamp>,
    last_heartbeat_at: Option<Timestamp>,
    evaluated_at: Timestamp,
    maximum_age_nanos: u64,
    state: FreshnessState,
}

impl<'de> Deserialize<'de> for FreshnessEvidence {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = FreshnessEvidenceWire::deserialize(deserializer)?;
        let evidence = Self::assess(
            wire.last_market_event_at,
            wire.last_heartbeat_at,
            wire.evaluated_at,
            wire.maximum_age_nanos,
        )
        .map_err(serde::de::Error::custom)?;
        if evidence.state != wire.state {
            return Err(serde::de::Error::custom(
                "serialized freshness state does not match market timestamps",
            ));
        }
        Ok(evidence)
    }
}

impl FreshnessEvidence {
    /// Assesses freshness from `last_market_event_at`; heartbeat time is retained only for liveness.
    ///
    /// # Errors
    ///
    /// Returns [`ClassificationError::DurationTooLarge`] for an unrepresentable age or
    /// [`ClassificationError::ObservationAfterEvaluation`] for future observations.
    pub fn assess(
        last_market_event_at: Option<Timestamp>,
        last_heartbeat_at: Option<Timestamp>,
        evaluated_at: Timestamp,
        maximum_age_nanos: u64,
    ) -> Result<Self, ClassificationError> {
        let maximum_age =
            i64::try_from(maximum_age_nanos).map_err(|_| ClassificationError::DurationTooLarge)?;
        for observation in [last_market_event_at, last_heartbeat_at]
            .into_iter()
            .flatten()
        {
            if observation > evaluated_at {
                return Err(ClassificationError::ObservationAfterEvaluation);
            }
        }
        let state = match last_market_event_at {
            Some(market_at) => {
                let oldest_fresh = evaluated_at
                    .checked_sub_nanos(maximum_age)
                    .unwrap_or(Timestamp::from_unix_nanos(i64::MIN));
                if market_at >= oldest_fresh {
                    FreshnessState::Fresh
                } else {
                    FreshnessState::Stale
                }
            }
            None => FreshnessState::Unknown,
        };
        Ok(Self {
            last_market_event_at,
            last_heartbeat_at,
            evaluated_at,
            maximum_age_nanos,
            state,
        })
    }

    /// Returns the newest valid market event used for freshness.
    pub const fn last_market_event_at(self) -> Option<Timestamp> {
        self.last_market_event_at
    }

    /// Returns the newest heartbeat used only for connection liveness.
    pub const fn last_heartbeat_at(self) -> Option<Timestamp> {
        self.last_heartbeat_at
    }

    /// Returns the instant at which freshness was assessed.
    pub const fn evaluated_at(self) -> Timestamp {
        self.evaluated_at
    }

    /// Returns the configured maximum market-event age in nanoseconds.
    pub const fn maximum_age_nanos(self) -> u64 {
        self.maximum_age_nanos
    }

    /// Returns the market-only freshness result.
    pub const fn state(self) -> FreshnessState {
        self.state
    }
}
