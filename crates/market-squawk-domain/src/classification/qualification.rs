//! Auditable live-policy assessment without execution authority.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};

use super::{
    BookIntegrity, BoundAssessment, CaptureIntegrityState, ChecksumCapability, ChecksumIntegrity,
    ChecksumTarget, CoverageStatus, DataQuality, DeliveryEvidence, FreshnessState,
    LiveEvidenceBinding, PrecisionIntegrity, SequenceCapability, SequenceIntegrity,
    SnapshotApplicability, SnapshotConsistency, SnapshotState, SourceAuthorization,
    StreamIntegrityState, TimestampIntegrity,
};
use crate::{Timestamp, TradingStatus};

#[path = "qualification/input.rs"]
mod input;

pub use input::{
    IntegrityAssessmentSet, MarketAssessmentSet, QualificationAssessmentId,
    QualificationAssessmentInput, SourcePolicyAssessment,
};

/// One explicit reason a policy assessment did not satisfy direct-verified conditions.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(u32)]
pub enum EligibilityFailure {
    /// The source's declared quality ceiling is not direct verified.
    QualityCeiling = 1 << 0,
    /// Source authorization is absent.
    SourceUnauthorized = 1 << 1,
    /// Delivery is neither direct venue nor authorized broker delivery.
    DeliveryNotDirect = 1 << 2,
    /// Sequence evidence is invalid, unsupported, or incomplete.
    SequenceIntegrity = 1 << 3,
    /// Snapshot state is inconsistent, uninitialized, or inapplicable contrary to metadata.
    SnapshotConsistency = 1 << 4,
    /// Supported checksum evidence failed or remains unchecked.
    ChecksumIntegrity = 1 << 5,
    /// Atomic source/receive/evaluation timestamp evidence is invalid.
    EventTiming = 1 << 6,
    /// Market data is stale or uninitialized.
    MarketFreshness = 1 << 7,
    /// The instrument is not actively trading.
    TradingStatus = 1 << 8,
    /// Price or quantity precision is invalid.
    Precision = 1 << 9,
    /// Explicit sufficient source coverage is absent.
    Coverage = 1 << 10,
    /// The candidate book is crossed or unvalidated.
    BookIntegrity = 1 << 11,
    /// Stream integrity is not healthy.
    StreamIntegrity = 1 << 12,
    /// Enabled capture is known to have lost or failed a frame.
    CaptureIntegrity = 1 << 13,
}

/// Compact derived set of policy-assessment failures.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct EligibilityFailures(u32);

impl EligibilityFailures {
    const fn empty() -> Self {
        Self(0)
    }
    fn insert(&mut self, failure: EligibilityFailure) {
        self.0 |= failure as u32;
    }
    /// Returns true when no failure was derived.
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }
    /// Returns true when a failure was derived.
    pub const fn contains(self, failure: EligibilityFailure) -> bool {
        self.0 & failure as u32 != 0
    }
}

/// Non-authoritative status of a retained policy assessment at an instant.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AssessmentStatus {
    /// Retained evidence satisfies the recorded policy at this instant.
    Satisfied,
    /// Evidence fails policy, lies outside a validity window, or coverage is ineffective.
    Rejected,
}

/// Component named in a relational construction failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QualificationComponent {
    /// Source policy.
    SourcePolicy,
    /// Sequence validation.
    Sequence,
    /// Snapshot consistency.
    Snapshot,
    /// Checksum validation.
    Checksum,
    /// Atomic timing.
    Timing,
    /// Trading status.
    TradingStatus,
    /// Precision.
    Precision,
    /// Coverage.
    Coverage,
    /// Book state.
    Book,
    /// Stream state.
    Stream,
    /// Capture state.
    Capture,
}

/// Contradiction across otherwise well-formed assessment evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QualificationError {
    /// One assessment has a different complete binding.
    BindingMismatch { component: QualificationComponent },
    /// Raw generation does not match the complete binding.
    GenerationMismatch { component: QualificationComponent },
    /// Evidence capability contradicts source metadata.
    CapabilityMismatch { component: QualificationComponent },
    /// Snapshot/observed sequence values disagree.
    EvidenceDisagreement { component: QualificationComponent },
    /// Assessment validity windows do not overlap.
    NonOverlappingValidity,
    /// A book event was not metadata-declared snapshot-required and initialized.
    BookSnapshotRequired,
    /// Non-applicability metadata contradicts initialized snapshot evidence.
    SnapshotApplicabilityContradiction,
    /// Initialized snapshot identity/digest disagrees with the bound book state.
    BookStateMismatch,
    /// Snapshot claims initialization after the snapshot assessment instant.
    SnapshotInitializedAfterEvaluation,
    /// Snapshot applicability does not match the bound event class.
    SnapshotEventClassMismatch,
    /// The checksum target does not match the bound event class.
    ChecksumEventClassMismatch,
    /// Book-integrity applicability does not match the bound event class.
    BookIntegrityEventClassMismatch,
}

impl fmt::Display for QualificationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BindingMismatch { component } => {
                write!(formatter, "{component:?} binding mismatch")
            }
            Self::GenerationMismatch { component } => {
                write!(formatter, "{component:?} generation mismatch")
            }
            Self::CapabilityMismatch { component } => {
                write!(formatter, "{component:?} capability mismatch")
            }
            Self::EvidenceDisagreement { component } => {
                write!(formatter, "{component:?} evidence disagreement")
            }
            Self::NonOverlappingValidity => {
                formatter.write_str("assessment validity windows do not overlap")
            }
            Self::BookSnapshotRequired => {
                formatter.write_str("book events require metadata-backed initialized snapshots")
            }
            Self::SnapshotApplicabilityContradiction => {
                formatter.write_str("snapshot state contradicts metadata applicability")
            }
            Self::BookStateMismatch => {
                formatter.write_str("snapshot state does not match bound book state")
            }
            Self::SnapshotInitializedAfterEvaluation => {
                formatter.write_str("snapshot initialization occurs after assessment")
            }
            Self::SnapshotEventClassMismatch => {
                formatter.write_str("snapshot applicability does not match event class")
            }
            Self::ChecksumEventClassMismatch => {
                formatter.write_str("checksum target does not match event class")
            }
            Self::BookIntegrityEventClassMismatch => {
                formatter.write_str("book-integrity applicability does not match event class")
            }
        }
    }
}

impl std::error::Error for QualificationError {}

/// Immutable, serializable audit assessment.
///
/// This value is deliberately cloneable and public because it is evidence, not authority. Its
/// future stateful live-plane service may inspect an assessment while producing a private,
/// short-lived token, but no dependent crate can derive such a token from this type alone. This
/// type intentionally has no execution-eligibility or authority API.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QualificationAssessment {
    assessment_id: QualificationAssessmentId,
    binding: LiveEvidenceBinding,
    source_policy: BoundAssessment<SourcePolicyAssessment>,
    integrity: IntegrityAssessmentSet,
    market: MarketAssessmentSet,
    recorded_quality: DataQuality,
    failures: EligibilityFailures,
    evaluated_at: Timestamp,
    valid_until: Timestamp,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QualificationAssessmentWire {
    assessment_id: QualificationAssessmentId,
    binding: LiveEvidenceBinding,
    source_policy: BoundAssessment<SourcePolicyAssessment>,
    integrity: IntegrityAssessmentSet,
    market: MarketAssessmentSet,
    recorded_quality: DataQuality,
    failures: EligibilityFailures,
    evaluated_at: Timestamp,
    valid_until: Timestamp,
}

impl<'de> Deserialize<'de> for QualificationAssessment {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = QualificationAssessmentWire::deserialize(deserializer)?;
        let recorded_quality = wire.recorded_quality;
        let failures = wire.failures;
        let evaluated_at = wire.evaluated_at;
        let valid_until = wire.valid_until;
        let rebuilt = Self::try_from(QualificationAssessmentInput::new(
            wire.assessment_id,
            wire.binding,
            wire.source_policy,
            wire.integrity,
            wire.market,
        ))
        .map_err(serde::de::Error::custom)?;
        if rebuilt.recorded_quality != recorded_quality
            || rebuilt.failures != failures
            || rebuilt.evaluated_at != evaluated_at
            || rebuilt.valid_until != valid_until
        {
            return Err(serde::de::Error::custom(
                "serialized qualification result contradicts reconstructed assessment",
            ));
        }
        Ok(rebuilt)
    }
}

impl QualificationAssessment {
    /// Returns the durable assessment identity.
    pub const fn assessment_id(&self) -> &QualificationAssessmentId {
        &self.assessment_id
    }
    /// Returns the complete immutable binding.
    pub const fn binding(&self) -> &LiveEvidenceBinding {
        &self.binding
    }
    /// Returns the archival quality result.
    pub const fn recorded_quality(&self) -> DataQuality {
        self.recorded_quality
    }
    /// Returns the retained source-policy assessment.
    pub const fn source_policy(&self) -> &BoundAssessment<SourcePolicyAssessment> {
        &self.source_policy
    }
    /// Returns integrity assessment evidence.
    pub const fn integrity(&self) -> &IntegrityAssessmentSet {
        &self.integrity
    }
    /// Returns market-state assessment evidence.
    pub const fn market(&self) -> &MarketAssessmentSet {
        &self.market
    }
    /// Returns derived failures.
    pub const fn failures(&self) -> EligibilityFailures {
        self.failures
    }
    /// Returns whether a failure was derived.
    pub const fn has_failure(&self, failure: EligibilityFailure) -> bool {
        self.failures.contains(failure)
    }
    /// Returns the latest component evaluation instant.
    pub const fn evaluated_at(&self) -> Timestamp {
        self.evaluated_at
    }
    /// Returns the strictest inclusive component expiry.
    pub const fn valid_until(&self) -> Timestamp {
        self.valid_until
    }

    /// Returns non-authoritative policy status at an instant.
    ///
    /// `Satisfied` is an audit conclusion, not an execution capability and must never be accepted
    /// by an execution adapter. Expiry is inclusive at `valid_until` and rejected one nanosecond
    /// later. Coverage effectiveness is checked independently at the requested instant.
    pub fn assessment_status_at(&self, at: Timestamp) -> AssessmentStatus {
        if self.failures.is_empty()
            && at >= self.evaluated_at
            && at <= self.valid_until
            && self.market.coverage.result().status_at(at) == CoverageStatus::Sufficient
        {
            AssessmentStatus::Satisfied
        } else {
            AssessmentStatus::Rejected
        }
    }
}

impl TryFrom<QualificationAssessmentInput> for QualificationAssessment {
    type Error = QualificationError;

    fn try_from(input: QualificationAssessmentInput) -> Result<Self, Self::Error> {
        validate_relations(&input)?;
        let (evaluated_at, valid_until) = validity_intersection(&input)?;
        let failures = derive_failures(&input, evaluated_at);
        let recorded_quality = derive_quality(&input, failures);
        Ok(Self {
            assessment_id: input.assessment_id,
            binding: input.binding,
            source_policy: input.source_policy,
            integrity: input.integrity,
            market: input.market,
            recorded_quality,
            failures,
            evaluated_at,
            valid_until,
        })
    }
}

fn validate_relations(input: &QualificationAssessmentInput) -> Result<(), QualificationError> {
    let components = [
        (
            QualificationComponent::SourcePolicy,
            input.source_policy.binding(),
        ),
        (
            QualificationComponent::Sequence,
            input.integrity.sequence.binding(),
        ),
        (
            QualificationComponent::Snapshot,
            input.integrity.snapshot.binding(),
        ),
        (
            QualificationComponent::Checksum,
            input.integrity.checksum.binding(),
        ),
        (
            QualificationComponent::Timing,
            input.integrity.timing.binding(),
        ),
        (
            QualificationComponent::TradingStatus,
            input.market.trading_status.binding(),
        ),
        (
            QualificationComponent::Precision,
            input.market.precision.binding(),
        ),
        (
            QualificationComponent::Coverage,
            input.market.coverage.binding(),
        ),
        (QualificationComponent::Book, input.market.book.binding()),
        (
            QualificationComponent::Stream,
            input.market.stream.binding(),
        ),
        (
            QualificationComponent::Capture,
            input.market.capture.binding(),
        ),
    ];
    for (component, found) in components {
        if found != &input.binding {
            return Err(QualificationError::BindingMismatch { component });
        }
    }
    if input.market.coverage.result().binding() != &input.binding {
        return Err(QualificationError::BindingMismatch {
            component: QualificationComponent::Coverage,
        });
    }
    if input.integrity.timing.result().evaluated_at() != input.integrity.timing.evaluated_at() {
        return Err(QualificationError::EvidenceDisagreement {
            component: QualificationComponent::Timing,
        });
    }

    let generation = input.binding.connection_generation();
    for (component, found) in [
        (
            QualificationComponent::Sequence,
            input.integrity.sequence.result().connection_generation(),
        ),
        (
            QualificationComponent::Snapshot,
            input.integrity.snapshot.result().connection_generation(),
        ),
        (
            QualificationComponent::Checksum,
            input.integrity.checksum.result().connection_generation(),
        ),
        (
            QualificationComponent::Timing,
            input.integrity.timing.result().connection_generation(),
        ),
    ] {
        if found != generation {
            return Err(QualificationError::GenerationMismatch { component });
        }
    }

    let capabilities = input.source_policy.result().integrity_capabilities();
    if capabilities.sequence() != input.integrity.sequence.result().capability() {
        return Err(QualificationError::CapabilityMismatch {
            component: QualificationComponent::Sequence,
        });
    }
    if capabilities.checksum() != input.integrity.checksum.result().capability() {
        return Err(QualificationError::CapabilityMismatch {
            component: QualificationComponent::Checksum,
        });
    }
    let is_book = input.binding.event_class().requires_book_state();
    if is_book && capabilities.sequence() == SequenceCapability::Provided {
        if input.integrity.sequence.result().snapshot_sequence()
            != input.integrity.snapshot.result().snapshot_sequence()
        {
            return Err(QualificationError::EvidenceDisagreement {
                component: QualificationComponent::Snapshot,
            });
        }
        if input.integrity.sequence.result().observed_sequence()
            != input.integrity.snapshot.result().observed_sequence()
        {
            return Err(QualificationError::EvidenceDisagreement {
                component: QualificationComponent::Sequence,
            });
        }
    }

    let snapshot = input.integrity.snapshot.result();
    if is_book
        != matches!(
            input.source_policy.result().snapshot_applicability(),
            SnapshotApplicability::Required
        )
    {
        return Err(QualificationError::SnapshotEventClassMismatch);
    }
    match input.source_policy.result().snapshot_applicability() {
        SnapshotApplicability::Required => {
            if !snapshot.is_initialized() {
                return Err(QualificationError::BookSnapshotRequired);
            }
        }
        SnapshotApplicability::NotApplicable { .. } => {
            if snapshot.is_initialized() {
                return Err(QualificationError::SnapshotApplicabilityContradiction);
            }
        }
    }
    if is_book && !snapshot.is_initialized() {
        return Err(QualificationError::BookSnapshotRequired);
    }
    if let (Some(book), SnapshotState::Initialized(initialized)) =
        (input.binding.book_state(), snapshot.state())
        && (book.state_id() != initialized.snapshot_identity()
            || book.state_digest() != initialized.state_digest())
    {
        return Err(QualificationError::BookStateMismatch);
    }
    if let SnapshotState::Initialized(initialized) = snapshot.state()
        && initialized.initialized_at() > input.integrity.snapshot.evaluated_at()
    {
        return Err(QualificationError::SnapshotInitializedAfterEvaluation);
    }
    let checksum = input.integrity.checksum.result();
    match (is_book, checksum.capability(), checksum.target()) {
        (true, ChecksumCapability::Provided, Some(ChecksumTarget::Book(scope))) => {
            let book = input
                .binding
                .book_state()
                .ok_or(QualificationError::ChecksumEventClassMismatch)?;
            if scope.depth() != book.depth() {
                return Err(QualificationError::EvidenceDisagreement {
                    component: QualificationComponent::Checksum,
                });
            }
        }
        (false, ChecksumCapability::Provided, Some(ChecksumTarget::Payload(_)))
        | (_, ChecksumCapability::Unsupported, None) => {}
        _ => return Err(QualificationError::ChecksumEventClassMismatch),
    }
    match (is_book, *input.market.book.result()) {
        (true, BookIntegrity::NotApplicable) | (false, BookIntegrity::Consistent) => {
            return Err(QualificationError::BookIntegrityEventClassMismatch);
        }
        (false, BookIntegrity::Crossed | BookIntegrity::Unknown) => {
            return Err(QualificationError::BookIntegrityEventClassMismatch);
        }
        _ => {}
    }
    Ok(())
}

fn validity_intersection(
    input: &QualificationAssessmentInput,
) -> Result<(Timestamp, Timestamp), QualificationError> {
    let windows = [
        (
            input.source_policy.evaluated_at(),
            input.source_policy.valid_until(),
        ),
        (
            input.integrity.sequence.evaluated_at(),
            input.integrity.sequence.valid_until(),
        ),
        (
            input.integrity.snapshot.evaluated_at(),
            input.integrity.snapshot.valid_until(),
        ),
        (
            input.integrity.checksum.evaluated_at(),
            input.integrity.checksum.valid_until(),
        ),
        (
            input.integrity.timing.evaluated_at(),
            input.integrity.timing.valid_until(),
        ),
        (
            input.market.trading_status.evaluated_at(),
            input.market.trading_status.valid_until(),
        ),
        (
            input.market.precision.evaluated_at(),
            input.market.precision.valid_until(),
        ),
        (
            input.market.coverage.evaluated_at(),
            input.market.coverage.valid_until(),
        ),
        (
            input.market.book.evaluated_at(),
            input.market.book.valid_until(),
        ),
        (
            input.market.stream.evaluated_at(),
            input.market.stream.valid_until(),
        ),
        (
            input.market.capture.evaluated_at(),
            input.market.capture.valid_until(),
        ),
    ];
    let mut evaluated_at = windows[0].0;
    let mut valid_until = windows[0].1;
    for (evaluated, valid) in windows.into_iter().skip(1) {
        evaluated_at = evaluated_at.max(evaluated);
        valid_until = valid_until.min(valid);
    }
    if valid_until < evaluated_at {
        return Err(QualificationError::NonOverlappingValidity);
    }
    Ok((evaluated_at, valid_until))
}

fn derive_failures(input: &QualificationAssessmentInput, at: Timestamp) -> EligibilityFailures {
    let mut failures = EligibilityFailures::empty();
    let policy = input.source_policy.result();
    if policy.quality_ceiling() != DataQuality::DirectVerified {
        failures.insert(EligibilityFailure::QualityCeiling);
    }
    if policy.source_authorization() != SourceAuthorization::Authorized {
        failures.insert(EligibilityFailure::SourceUnauthorized);
    }
    if !matches!(
        policy.delivery_evidence(),
        DeliveryEvidence::DirectVenue | DeliveryEvidence::AuthorizedBroker
    ) {
        failures.insert(EligibilityFailure::DeliveryNotDirect);
    }
    let sequence = input.integrity.sequence.result();
    if sequence.capability() != SequenceCapability::Provided
        || sequence.integrity() != SequenceIntegrity::Valid
    {
        failures.insert(EligibilityFailure::SequenceIntegrity);
    }
    if matches!(
        policy.snapshot_applicability(),
        SnapshotApplicability::Required
    ) && input.integrity.snapshot.result().consistency() != SnapshotConsistency::Consistent
    {
        failures.insert(EligibilityFailure::SnapshotConsistency);
    }
    let checksum = input.integrity.checksum.result();
    if !matches!(
        (checksum.capability(), checksum.integrity()),
        (ChecksumCapability::Provided, ChecksumIntegrity::Valid)
            | (
                ChecksumCapability::Unsupported,
                ChecksumIntegrity::NotSupported
            )
    ) {
        failures.insert(EligibilityFailure::ChecksumIntegrity);
    }
    let timing = input.integrity.timing.result();
    if timing.timestamp_integrity() != TimestampIntegrity::Valid {
        failures.insert(EligibilityFailure::EventTiming);
    }
    if timing.freshness() != FreshnessState::Fresh {
        failures.insert(EligibilityFailure::MarketFreshness);
    }
    if *input.market.trading_status.result() != TradingStatus::Active {
        failures.insert(EligibilityFailure::TradingStatus);
    }
    if *input.market.precision.result() != PrecisionIntegrity::Valid {
        failures.insert(EligibilityFailure::Precision);
    }
    if input.market.coverage.result().status_at(at) != CoverageStatus::Sufficient {
        failures.insert(EligibilityFailure::Coverage);
    }
    let expected_book_integrity = if input.binding.event_class().requires_book_state() {
        BookIntegrity::Consistent
    } else {
        BookIntegrity::NotApplicable
    };
    if *input.market.book.result() != expected_book_integrity {
        failures.insert(EligibilityFailure::BookIntegrity);
    }
    if *input.market.stream.result() != StreamIntegrityState::Healthy {
        failures.insert(EligibilityFailure::StreamIntegrity);
    }
    if *input.market.capture.result() == CaptureIntegrityState::Incomplete {
        failures.insert(EligibilityFailure::CaptureIntegrity);
    }
    failures
}

fn derive_quality(
    input: &QualificationAssessmentInput,
    failures: EligibilityFailures,
) -> DataQuality {
    if input.integrity.sequence.result().integrity() == SequenceIntegrity::Invalid
        || input.integrity.checksum.result().integrity() == ChecksumIntegrity::Failed
        || matches!(
            *input.market.stream.result(),
            StreamIntegrityState::GapDetected
                | StreamIntegrityState::ChecksumFailed
                | StreamIntegrityState::Divergent
                | StreamIntegrityState::Quarantined
        )
    {
        DataQuality::Quarantined
    } else if input.integrity.timing.result().freshness() == FreshnessState::Stale {
        DataQuality::Stale
    } else if failures.is_empty() {
        DataQuality::DirectVerified
    } else if input.source_policy.result().quality_ceiling() != DataQuality::DirectVerified {
        input.source_policy.result().quality_ceiling()
    } else {
        DataQuality::DirectUnverified
    }
}
