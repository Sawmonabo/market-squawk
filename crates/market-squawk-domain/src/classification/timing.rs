//! Atomic, generation-bound live timing assessment.

use std::fmt;

use serde::Serialize;

use super::{FreshnessState, TimestampIntegrity};
use crate::{ConnectionGeneration, Timestamp};

/// A failure to construct or evaluate live timing evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClassificationError {
    /// A configured nanosecond limit exceeds the signed timestamp scalar.
    DurationTooLarge,
    /// Evaluation occurred before the market frame was received.
    EvaluationBeforeReceive,
    /// A heartbeat claims to occur after the evaluation instant.
    ObservationAfterEvaluation,
}

impl fmt::Display for ClassificationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DurationTooLarge => {
                formatter.write_str("nanosecond evidence limit exceeds signed timestamp range")
            }
            Self::EvaluationBeforeReceive => {
                formatter.write_str("evaluation time must not precede market receive time")
            }
            Self::ObservationAfterEvaluation => {
                formatter.write_str("heartbeat observation occurs after evaluation time")
            }
        }
    }
}

impl std::error::Error for ClassificationError {}

/// Bounds used for one atomic live timing assessment.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct LiveTimingPolicy {
    maximum_future_skew_nanos: i64,
    maximum_transport_age_nanos: i64,
    maximum_source_age_nanos: i64,
    maximum_market_age_nanos: i64,
}

impl LiveTimingPolicy {
    /// Constructs checked timing limits.
    ///
    /// # Errors
    ///
    /// Returns [`ClassificationError::DurationTooLarge`] if any limit exceeds `i64::MAX`.
    pub fn new(
        maximum_future_skew_nanos: u64,
        maximum_transport_age_nanos: u64,
        maximum_source_age_nanos: u64,
        maximum_market_age_nanos: u64,
    ) -> Result<Self, ClassificationError> {
        Ok(Self {
            maximum_future_skew_nanos: checked_limit(maximum_future_skew_nanos)?,
            maximum_transport_age_nanos: checked_limit(maximum_transport_age_nanos)?,
            maximum_source_age_nanos: checked_limit(maximum_source_age_nanos)?,
            maximum_market_age_nanos: checked_limit(maximum_market_age_nanos)?,
        })
    }

    /// Returns the maximum source clock lead over local receive time.
    pub const fn maximum_future_skew_nanos(self) -> u64 {
        self.maximum_future_skew_nanos as u64
    }

    /// Returns the maximum source-to-receive transport age.
    pub const fn maximum_transport_age_nanos(self) -> u64 {
        self.maximum_transport_age_nanos as u64
    }

    /// Returns the maximum source-to-evaluation age.
    pub const fn maximum_source_age_nanos(self) -> u64 {
        self.maximum_source_age_nanos as u64
    }

    /// Returns the maximum receive-to-evaluation market age.
    pub const fn maximum_market_age_nanos(self) -> u64 {
        self.maximum_market_age_nanos as u64
    }
}

fn checked_limit(value: u64) -> Result<i64, ClassificationError> {
    i64::try_from(value).map_err(|_| ClassificationError::DurationTooLarge)
}

/// Source and local receive timestamps for one latest market event.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct MarketEventTiming {
    source_timestamp: Option<Timestamp>,
    received_at: Timestamp,
}

impl MarketEventTiming {
    /// Retains a market event's source time, including an explicit missing value, and receive time.
    pub const fn new(source_timestamp: Option<Timestamp>, received_at: Timestamp) -> Self {
        Self {
            source_timestamp,
            received_at,
        }
    }

    /// Returns the source timestamp without manufacturing one.
    pub const fn source_timestamp(self) -> Option<Timestamp> {
        self.source_timestamp
    }

    /// Returns when this market event reached the process.
    pub const fn received_at(self) -> Timestamp {
        self.received_at
    }
}

/// One immutable assessment tying generation, market time, receive time, and evaluation time.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct LiveTimingAssessment {
    connection_generation: ConnectionGeneration,
    latest_market_event: Option<MarketEventTiming>,
    last_heartbeat_at: Option<Timestamp>,
    evaluated_at: Timestamp,
    policy: LiveTimingPolicy,
    timestamp_integrity: TimestampIntegrity,
    freshness: FreshnessState,
}

impl LiveTimingAssessment {
    /// Atomically assesses source sanity and market freshness for one connection generation.
    ///
    /// Heartbeats are retained for connection health only and never refresh market freshness.
    /// Difference comparisons use `i128`, so `i64::MIN`/`MAX` inputs cannot wrap or saturate.
    ///
    /// # Errors
    ///
    /// Rejects evaluation before receive time and heartbeat observations after evaluation.
    pub fn assess(
        connection_generation: ConnectionGeneration,
        latest_market_event: Option<MarketEventTiming>,
        last_heartbeat_at: Option<Timestamp>,
        evaluated_at: Timestamp,
        policy: LiveTimingPolicy,
    ) -> Result<Self, ClassificationError> {
        if let Some(event) = latest_market_event
            && event.received_at > evaluated_at
        {
            return Err(ClassificationError::EvaluationBeforeReceive);
        }
        if last_heartbeat_at.is_some_and(|heartbeat| heartbeat > evaluated_at) {
            return Err(ClassificationError::ObservationAfterEvaluation);
        }

        let (timestamp_integrity, freshness) = match latest_market_event {
            None => (TimestampIntegrity::Invalid, FreshnessState::Unknown),
            Some(event) => {
                let freshness = if elapsed(event.received_at, evaluated_at)
                    <= i128::from(policy.maximum_market_age_nanos)
                {
                    FreshnessState::Fresh
                } else {
                    FreshnessState::Stale
                };
                let timestamp_integrity = match event.source_timestamp {
                    None => TimestampIntegrity::Invalid,
                    Some(source_timestamp) => {
                        let source_minus_receive =
                            signed_delta(event.received_at, source_timestamp);
                        let receive_minus_source =
                            signed_delta(source_timestamp, event.received_at);
                        let evaluation_minus_source = signed_delta(source_timestamp, evaluated_at);
                        if source_minus_receive <= i128::from(policy.maximum_future_skew_nanos)
                            && receive_minus_source
                                <= i128::from(policy.maximum_transport_age_nanos)
                            && evaluation_minus_source
                                <= i128::from(policy.maximum_source_age_nanos)
                        {
                            TimestampIntegrity::Valid
                        } else {
                            TimestampIntegrity::Invalid
                        }
                    }
                };
                (timestamp_integrity, freshness)
            }
        };

        Ok(Self {
            connection_generation,
            latest_market_event,
            last_heartbeat_at,
            evaluated_at,
            policy,
            timestamp_integrity,
            freshness,
        })
    }

    /// Returns the assessed connection generation.
    pub const fn connection_generation(self) -> ConnectionGeneration {
        self.connection_generation
    }

    /// Returns the latest market-event timing retained by the assessment.
    pub const fn latest_market_event(self) -> Option<MarketEventTiming> {
        self.latest_market_event
    }

    /// Returns the last heartbeat, used only for connection health.
    pub const fn last_heartbeat_at(self) -> Option<Timestamp> {
        self.last_heartbeat_at
    }

    /// Returns the assessment instant.
    pub const fn evaluated_at(self) -> Timestamp {
        self.evaluated_at
    }

    /// Returns the checked timing policy.
    pub const fn policy(self) -> LiveTimingPolicy {
        self.policy
    }

    /// Returns derived source/receive/evaluation integrity.
    pub const fn timestamp_integrity(self) -> TimestampIntegrity {
        self.timestamp_integrity
    }

    /// Returns market-only freshness.
    pub const fn freshness(self) -> FreshnessState {
        self.freshness
    }
}

fn elapsed(earlier: Timestamp, later: Timestamp) -> i128 {
    signed_delta(earlier, later)
}

fn signed_delta(earlier: Timestamp, later: Timestamp) -> i128 {
    i128::from(later.unix_nanos()) - i128::from(earlier.unix_nanos())
}
