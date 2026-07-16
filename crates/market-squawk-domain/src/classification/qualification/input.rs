//! Immutable inputs to relational live-policy qualification.

use serde::{Deserialize, Deserializer, Serialize};

use super::super::{
    AssessmentValidity, BookIntegrity, BoundAssessment, CaptureIntegrityState, ChecksumEvidence,
    DataQuality, DeliveryEvidence, IntegrityCapabilities, LiveEvidenceBinding,
    LiveTimingAssessment, PrecisionIntegrity, SequenceEvidence, SnapshotApplicability,
    SnapshotEvidence, SourceAuthorization, SourceCoverageRecord, StreamIntegrityState,
};
use crate::{SourceIdentifier, TradingStatus};

/// Durable identity of a retained live-policy assessment.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct QualificationAssessmentId(SourceIdentifier);

impl QualificationAssessmentId {
    /// Constructs a bounded assessment identity.
    pub const fn new(value: SourceIdentifier) -> Self {
        Self(value)
    }

    /// Returns the retained audit reference.
    pub const fn as_source_identifier(&self) -> &SourceIdentifier {
        &self.0
    }
}

/// Source-registry values assessed as one metadata revision.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourcePolicyAssessment {
    quality_ceiling: DataQuality,
    integrity_capabilities: IntegrityCapabilities,
    source_authorization: SourceAuthorization,
    delivery_evidence: DeliveryEvidence,
    snapshot_applicability: SnapshotApplicability,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourcePolicyAssessmentWire {
    quality_ceiling: DataQuality,
    integrity_capabilities: IntegrityCapabilities,
    source_authorization: SourceAuthorization,
    delivery_evidence: DeliveryEvidence,
    snapshot_applicability: SnapshotApplicability,
}

impl<'de> Deserialize<'de> for SourcePolicyAssessment {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = SourcePolicyAssessmentWire::deserialize(deserializer)?;
        Ok(Self::new(
            wire.quality_ceiling,
            wire.integrity_capabilities,
            wire.source_authorization,
            wire.delivery_evidence,
            wire.snapshot_applicability,
        ))
    }
}

impl SourcePolicyAssessment {
    /// Constructs a cohesive source-registry policy result.
    pub const fn new(
        quality_ceiling: DataQuality,
        integrity_capabilities: IntegrityCapabilities,
        source_authorization: SourceAuthorization,
        delivery_evidence: DeliveryEvidence,
        snapshot_applicability: SnapshotApplicability,
    ) -> Self {
        Self {
            quality_ceiling,
            integrity_capabilities,
            source_authorization,
            delivery_evidence,
            snapshot_applicability,
        }
    }

    /// Returns the source quality ceiling.
    pub const fn quality_ceiling(&self) -> DataQuality {
        self.quality_ceiling
    }

    /// Returns declared integrity capabilities.
    pub const fn integrity_capabilities(&self) -> IntegrityCapabilities {
        self.integrity_capabilities
    }

    /// Returns source authorization.
    pub const fn source_authorization(&self) -> SourceAuthorization {
        self.source_authorization
    }

    /// Returns the delivery relationship.
    pub const fn delivery_evidence(&self) -> DeliveryEvidence {
        self.delivery_evidence
    }

    /// Returns snapshot applicability for the exact event class.
    pub const fn snapshot_applicability(&self) -> &SnapshotApplicability {
        &self.snapshot_applicability
    }
}

impl AssessmentValidity for SourcePolicyAssessment {}

impl AssessmentValidity for TradingStatus {}
impl AssessmentValidity for PrecisionIntegrity {}
impl AssessmentValidity for BookIntegrity {}
impl AssessmentValidity for StreamIntegrityState {}
impl AssessmentValidity for CaptureIntegrityState {}

/// Integrity assessments that must describe the exact same live observation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IntegrityAssessmentSet {
    pub(super) sequence: BoundAssessment<SequenceEvidence>,
    pub(super) snapshot: BoundAssessment<SnapshotEvidence>,
    pub(super) checksum: BoundAssessment<ChecksumEvidence>,
    pub(super) timing: BoundAssessment<LiveTimingAssessment>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IntegrityAssessmentSetWire {
    sequence: BoundAssessment<SequenceEvidence>,
    snapshot: BoundAssessment<SnapshotEvidence>,
    checksum: BoundAssessment<ChecksumEvidence>,
    timing: BoundAssessment<LiveTimingAssessment>,
}

impl<'de> Deserialize<'de> for IntegrityAssessmentSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = IntegrityAssessmentSetWire::deserialize(deserializer)?;
        Ok(Self::new(
            wire.sequence,
            wire.snapshot,
            wire.checksum,
            wire.timing,
        ))
    }
}

impl IntegrityAssessmentSet {
    /// Groups independently evaluated integrity evidence.
    pub const fn new(
        sequence: BoundAssessment<SequenceEvidence>,
        snapshot: BoundAssessment<SnapshotEvidence>,
        checksum: BoundAssessment<ChecksumEvidence>,
        timing: BoundAssessment<LiveTimingAssessment>,
    ) -> Self {
        Self {
            sequence,
            snapshot,
            checksum,
            timing,
        }
    }

    /// Returns sequence evidence.
    pub const fn sequence(&self) -> &BoundAssessment<SequenceEvidence> {
        &self.sequence
    }

    /// Returns snapshot evidence.
    pub const fn snapshot(&self) -> &BoundAssessment<SnapshotEvidence> {
        &self.snapshot
    }

    /// Returns checksum evidence.
    pub const fn checksum(&self) -> &BoundAssessment<ChecksumEvidence> {
        &self.checksum
    }

    /// Returns timing evidence.
    pub const fn timing(&self) -> &BoundAssessment<LiveTimingAssessment> {
        &self.timing
    }
}

/// Market-state assessments that must describe the exact same live observation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MarketAssessmentSet {
    pub(super) trading_status: BoundAssessment<TradingStatus>,
    pub(super) precision: BoundAssessment<PrecisionIntegrity>,
    pub(super) coverage: BoundAssessment<SourceCoverageRecord>,
    pub(super) book: BoundAssessment<BookIntegrity>,
    pub(super) stream: BoundAssessment<StreamIntegrityState>,
    pub(super) capture: BoundAssessment<CaptureIntegrityState>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MarketAssessmentSetWire {
    trading_status: BoundAssessment<TradingStatus>,
    precision: BoundAssessment<PrecisionIntegrity>,
    coverage: BoundAssessment<SourceCoverageRecord>,
    book: BoundAssessment<BookIntegrity>,
    stream: BoundAssessment<StreamIntegrityState>,
    capture: BoundAssessment<CaptureIntegrityState>,
}

impl<'de> Deserialize<'de> for MarketAssessmentSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = MarketAssessmentSetWire::deserialize(deserializer)?;
        Ok(Self::new(
            wire.trading_status,
            wire.precision,
            wire.coverage,
            wire.book,
            wire.stream,
            wire.capture,
        ))
    }
}

impl MarketAssessmentSet {
    /// Groups independently evaluated market-state evidence.
    pub const fn new(
        trading_status: BoundAssessment<TradingStatus>,
        precision: BoundAssessment<PrecisionIntegrity>,
        coverage: BoundAssessment<SourceCoverageRecord>,
        book: BoundAssessment<BookIntegrity>,
        stream: BoundAssessment<StreamIntegrityState>,
        capture: BoundAssessment<CaptureIntegrityState>,
    ) -> Self {
        Self {
            trading_status,
            precision,
            coverage,
            book,
            stream,
            capture,
        }
    }

    /// Returns trading status.
    pub const fn trading_status(&self) -> &BoundAssessment<TradingStatus> {
        &self.trading_status
    }

    /// Returns precision evidence.
    pub const fn precision(&self) -> &BoundAssessment<PrecisionIntegrity> {
        &self.precision
    }

    /// Returns coverage evidence.
    pub const fn coverage(&self) -> &BoundAssessment<SourceCoverageRecord> {
        &self.coverage
    }

    /// Returns book evidence.
    pub const fn book(&self) -> &BoundAssessment<BookIntegrity> {
        &self.book
    }

    /// Returns stream evidence.
    pub const fn stream(&self) -> &BoundAssessment<StreamIntegrityState> {
        &self.stream
    }

    /// Returns capture evidence.
    pub const fn capture(&self) -> &BoundAssessment<CaptureIntegrityState> {
        &self.capture
    }
}

/// Cohesive input to relational live-policy assessment.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QualificationAssessmentInput {
    pub(super) assessment_id: QualificationAssessmentId,
    pub(super) binding: LiveEvidenceBinding,
    pub(super) source_policy: BoundAssessment<SourcePolicyAssessment>,
    pub(super) integrity: IntegrityAssessmentSet,
    pub(super) market: MarketAssessmentSet,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QualificationAssessmentInputWire {
    assessment_id: QualificationAssessmentId,
    binding: LiveEvidenceBinding,
    source_policy: BoundAssessment<SourcePolicyAssessment>,
    integrity: IntegrityAssessmentSet,
    market: MarketAssessmentSet,
}

impl<'de> Deserialize<'de> for QualificationAssessmentInput {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = QualificationAssessmentInputWire::deserialize(deserializer)?;
        Ok(Self::new(
            wire.assessment_id,
            wire.binding,
            wire.source_policy,
            wire.integrity,
            wire.market,
        ))
    }
}

impl QualificationAssessmentInput {
    /// Collects complete evidence without accepting an eligibility or quality result.
    pub const fn new(
        assessment_id: QualificationAssessmentId,
        binding: LiveEvidenceBinding,
        source_policy: BoundAssessment<SourcePolicyAssessment>,
        integrity: IntegrityAssessmentSet,
        market: MarketAssessmentSet,
    ) -> Self {
        Self {
            assessment_id,
            binding,
            source_policy,
            integrity,
            market,
        }
    }
}
