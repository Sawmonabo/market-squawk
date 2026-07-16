//! Independent data, valuation, depth, integrity, and execution classifications.

use serde::{Deserialize, Serialize};

#[path = "classification/binding.rs"]
mod binding;
#[path = "classification/coverage.rs"]
mod coverage;
#[path = "classification/integrity.rs"]
mod integrity;
#[path = "classification/qualification.rs"]
mod qualification;
#[path = "classification/timing.rs"]
mod timing;

pub use integrity::{
    ChecksumCapability, ChecksumEvidence, ChecksumScope, ChecksumValue, InitializedSnapshot,
    IntegrityCapabilities, IntegrityEvidenceError, IntegrityRule, RuleVersion, SequenceCapability,
    SequenceEvidence, SequenceValidationRule, SnapshotApplicability, SnapshotEvidence,
    SnapshotState,
};
pub use qualification::{
    AssessmentStatus, EligibilityFailure, EligibilityFailures, IntegrityAssessmentSet,
    MarketAssessmentSet, QualificationAssessment, QualificationAssessmentId,
    QualificationAssessmentInput, QualificationComponent, QualificationError,
    SourcePolicyAssessment,
};
pub use timing::{ClassificationError, LiveTimingAssessment, LiveTimingPolicy, MarketEventTiming};

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
    /// Capture is explicitly disabled by policy.
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

/// Delivery relationship established independently of source authorization.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryEvidence {
    /// Data arrived directly from the identified trading venue.
    DirectVenue,
    /// Data arrived from a broker explicitly authorized for this market-data use.
    AuthorizedBroker,
    /// Data arrived through an aggregator or redistribution path.
    Indirect,
    /// The delivery relationship has not been established.
    Unknown,
}

/// Derived result of provider-specific sequence validation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SequenceIntegrity {
    /// Sequence progression was valid under the retained provider rule.
    Valid,
    /// Authoritative metadata establishes that the protocol supplies no sequence.
    NotSupported,
    /// A duplicate, gap, regression, or out-of-order update was observed.
    Invalid,
    /// Sequence qualification has not completed.
    Uninitialized,
}

/// Derived result of snapshot and incremental-update consistency validation.
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

/// Derived result of a provider-specific checksum validation check.
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

/// Result of the atomic source/receive/evaluation timing assessment.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TimestampIntegrity {
    /// Source time exists and all configured timing bounds pass.
    Valid,
    /// Source time is missing, too old, or outside the allowed receive-time skew.
    Invalid,
}

/// Freshness derived only from the latest market event, never from a heartbeat.
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
pub use binding::{
    AuthorizationBasis, BindingError, BookStateBinding, BoundAssessment, EvidenceDigest,
    LiveEventClass, LiveEvidenceBinding, MetadataRevision, ProviderChannel, ProviderProduct,
};
pub use coverage::{
    CoverageConsolidation, CoverageDelay, CoverageDimension, CoverageError, CoverageScope,
    CoverageStatus, SourceCoverageRecord,
};
