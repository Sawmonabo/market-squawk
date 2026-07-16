//! Execution qualification derived from typed source and market integrity evidence.

use serde::Serialize;

use super::{
    BookIntegrity, CaptureIntegrityState, ChecksumIntegrity, DataQuality, DeliveryEvidence,
    EventTimingEvidence, ExecutionEligibility, FairValueHierarchy, FreshnessEvidence,
    FreshnessState, PrecisionIntegrity, SequenceIntegrity, SnapshotConsistency,
    SourceAuthorization, SourceCoverageEvidence, StreamIntegrityState, TimestampIntegrity,
};
use crate::{ConnectionGeneration, InstrumentId, SourceId, TradingStatus, VenueId};

/// One explicit reason execution qualification failed.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(u32)]
pub enum EligibilityFailure {
    /// Candidate quality is not `DirectVerified`.
    QualityNotDirectVerified = 1 << 0,
    /// Level 2 or Level 3 fair-value evidence was presented as action evidence.
    FairValueEvidenceNotLevel1 = 1 << 1,
    /// Source authorization is absent.
    SourceUnauthorized = 1 << 2,
    /// Sequence evidence is invalid or incomplete.
    SequenceIntegrity = 1 << 3,
    /// Snapshot/update state is not consistent.
    SnapshotConsistency = 1 << 4,
    /// Checksum evidence failed or remains unchecked.
    ChecksumIntegrity = 1 << 5,
    /// Exchange/receive timestamp evidence is invalid.
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
    /// Delivery is neither direct venue nor authorized broker delivery.
    DeliveryNotDirect = 1 << 14,
}

/// Compact derived set of execution qualification failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
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

    /// Returns true when the given failure was derived.
    pub const fn contains(self, failure: EligibilityFailure) -> bool {
        self.0 & failure as u32 != 0
    }
}

/// Caller-supplied typed observations from which eligibility is derived.
///
/// The input deliberately has no eligibility field or setter.
#[derive(Clone, Debug)]
pub struct QualificationEvidenceInput {
    quality: DataQuality,
    fair_value_hierarchy: Option<FairValueHierarchy>,
    source_authorization: SourceAuthorization,
    delivery_evidence: DeliveryEvidence,
    source_id: SourceId,
    venue_id: VenueId,
    instrument_id: InstrumentId,
    connection_generation: ConnectionGeneration,
    sequence_integrity: SequenceIntegrity,
    snapshot_consistency: SnapshotConsistency,
    checksum_integrity: ChecksumIntegrity,
    event_timing: EventTimingEvidence,
    freshness: FreshnessEvidence,
    trading_status: TradingStatus,
    precision_integrity: PrecisionIntegrity,
    source_coverage: SourceCoverageEvidence,
    book_integrity: BookIntegrity,
    stream_integrity: StreamIntegrityState,
    capture_integrity: CaptureIntegrityState,
}

impl QualificationEvidenceInput {
    /// Collects typed qualification observations without accepting an eligibility claim.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        quality: DataQuality,
        fair_value_hierarchy: Option<FairValueHierarchy>,
        source_authorization: SourceAuthorization,
        delivery_evidence: DeliveryEvidence,
        source_id: SourceId,
        venue_id: VenueId,
        instrument_id: InstrumentId,
        connection_generation: ConnectionGeneration,
        sequence_integrity: SequenceIntegrity,
        snapshot_consistency: SnapshotConsistency,
        checksum_integrity: ChecksumIntegrity,
        event_timing: EventTimingEvidence,
        freshness: FreshnessEvidence,
        trading_status: TradingStatus,
        precision_integrity: PrecisionIntegrity,
        source_coverage: SourceCoverageEvidence,
        book_integrity: BookIntegrity,
        stream_integrity: StreamIntegrityState,
        capture_integrity: CaptureIntegrityState,
    ) -> Self {
        Self {
            quality,
            fair_value_hierarchy,
            source_authorization,
            delivery_evidence,
            source_id,
            venue_id,
            instrument_id,
            connection_generation,
            sequence_integrity,
            snapshot_consistency,
            checksum_integrity,
            event_timing,
            freshness,
            trading_status,
            precision_integrity,
            source_coverage,
            book_integrity,
            stream_integrity,
            capture_integrity,
        }
    }

    /// Replaces capture evidence, primarily for state-machine evaluation and tests.
    pub fn with_capture_integrity(mut self, capture_integrity: CaptureIntegrityState) -> Self {
        self.capture_integrity = capture_integrity;
        self
    }

    /// Replaces sequence evidence for state-machine evaluation.
    pub fn with_sequence_integrity(mut self, sequence_integrity: SequenceIntegrity) -> Self {
        self.sequence_integrity = sequence_integrity;
        self
    }

    /// Replaces typed delivery evidence for state-machine evaluation.
    pub fn with_delivery_evidence(mut self, delivery_evidence: DeliveryEvidence) -> Self {
        self.delivery_evidence = delivery_evidence;
        self
    }
}

/// Immutable typed evidence with derived, non-caller-authored eligibility.
#[derive(Clone, Debug, Serialize)]
pub struct QualificationEvidence {
    quality: DataQuality,
    fair_value_hierarchy: Option<FairValueHierarchy>,
    source_authorization: SourceAuthorization,
    delivery_evidence: DeliveryEvidence,
    source_id: SourceId,
    venue_id: VenueId,
    instrument_id: InstrumentId,
    connection_generation: ConnectionGeneration,
    sequence_integrity: SequenceIntegrity,
    snapshot_consistency: SnapshotConsistency,
    checksum_integrity: ChecksumIntegrity,
    event_timing: EventTimingEvidence,
    freshness: FreshnessEvidence,
    trading_status: TradingStatus,
    precision_integrity: PrecisionIntegrity,
    source_coverage: SourceCoverageEvidence,
    book_integrity: BookIntegrity,
    stream_integrity: StreamIntegrityState,
    capture_integrity: CaptureIntegrityState,
    eligibility: ExecutionEligibility,
    failures: EligibilityFailures,
}

impl QualificationEvidence {
    /// Returns the eligibility computed from all retained evidence.
    pub const fn execution_eligibility(&self) -> ExecutionEligibility {
        self.eligibility
    }

    /// Returns all derived qualification failures.
    pub const fn failures(&self) -> EligibilityFailures {
        self.failures
    }

    /// Returns whether a particular qualification failure was derived.
    pub const fn has_failure(&self, failure: EligibilityFailure) -> bool {
        self.failures.contains(failure)
    }

    /// Returns the source whose authorization and coverage were evaluated.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the venue whose market state was evaluated.
    pub const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    /// Returns the stable instrument identity whose state was evaluated.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the evaluated connection generation.
    pub const fn connection_generation(&self) -> ConnectionGeneration {
        self.connection_generation
    }

    /// Returns the candidate data-quality class.
    pub const fn quality(&self) -> DataQuality {
        self.quality
    }

    /// Returns associated fair-value evidence, if this observation is used in a valuation.
    pub const fn fair_value_hierarchy(&self) -> Option<FairValueHierarchy> {
        self.fair_value_hierarchy
    }

    /// Returns the market-only freshness evidence.
    pub const fn freshness(&self) -> FreshnessEvidence {
        self.freshness
    }

    /// Returns configured source-authorization evidence.
    pub const fn source_authorization(&self) -> SourceAuthorization {
        self.source_authorization
    }

    /// Returns the assessed direct-delivery relationship.
    pub const fn delivery_evidence(&self) -> DeliveryEvidence {
        self.delivery_evidence
    }

    /// Returns sequence-validation evidence.
    pub const fn sequence_integrity(&self) -> SequenceIntegrity {
        self.sequence_integrity
    }

    /// Returns snapshot/update consistency evidence.
    pub const fn snapshot_consistency(&self) -> SnapshotConsistency {
        self.snapshot_consistency
    }

    /// Returns checksum capability and result evidence.
    pub const fn checksum_integrity(&self) -> ChecksumIntegrity {
        self.checksum_integrity
    }

    /// Returns exchange/receive timestamp evidence.
    pub const fn event_timing(&self) -> EventTimingEvidence {
        self.event_timing
    }

    /// Returns the evaluated instrument trading status.
    pub const fn trading_status(&self) -> TradingStatus {
        self.trading_status
    }

    /// Returns price/quantity precision evidence.
    pub const fn precision_integrity(&self) -> PrecisionIntegrity {
        self.precision_integrity
    }

    /// Returns explicit source coverage evidence.
    pub const fn source_coverage(&self) -> SourceCoverageEvidence {
        self.source_coverage
    }

    /// Returns candidate book-consistency evidence.
    pub const fn book_integrity(&self) -> BookIntegrity {
        self.book_integrity
    }

    /// Returns decoded-stream integrity evidence.
    pub const fn stream_integrity(&self) -> StreamIntegrityState {
        self.stream_integrity
    }

    /// Returns asynchronous raw-capture integrity evidence.
    pub const fn capture_integrity(&self) -> CaptureIntegrityState {
        self.capture_integrity
    }
}

impl From<QualificationEvidenceInput> for QualificationEvidence {
    fn from(input: QualificationEvidenceInput) -> Self {
        let mut failures = EligibilityFailures::empty();
        if input.quality != DataQuality::DirectVerified {
            failures.insert(EligibilityFailure::QualityNotDirectVerified);
        }
        if matches!(
            input.fair_value_hierarchy,
            Some(FairValueHierarchy::Level2 | FairValueHierarchy::Level3)
        ) {
            failures.insert(EligibilityFailure::FairValueEvidenceNotLevel1);
        }
        if input.source_authorization != SourceAuthorization::Authorized {
            failures.insert(EligibilityFailure::SourceUnauthorized);
        }
        if !matches!(
            input.delivery_evidence,
            DeliveryEvidence::DirectVenue | DeliveryEvidence::AuthorizedBroker
        ) {
            failures.insert(EligibilityFailure::DeliveryNotDirect);
        }
        if input.sequence_integrity != SequenceIntegrity::Valid {
            failures.insert(EligibilityFailure::SequenceIntegrity);
        }
        if input.snapshot_consistency != SnapshotConsistency::Consistent {
            failures.insert(EligibilityFailure::SnapshotConsistency);
        }
        if !matches!(
            input.checksum_integrity,
            ChecksumIntegrity::Valid | ChecksumIntegrity::NotSupported
        ) {
            failures.insert(EligibilityFailure::ChecksumIntegrity);
        }
        if input.event_timing.integrity() != TimestampIntegrity::Valid {
            failures.insert(EligibilityFailure::EventTiming);
        }
        if input.freshness.state() != FreshnessState::Fresh {
            failures.insert(EligibilityFailure::MarketFreshness);
        }
        if input.trading_status != TradingStatus::Active {
            failures.insert(EligibilityFailure::TradingStatus);
        }
        if input.precision_integrity != PrecisionIntegrity::Valid {
            failures.insert(EligibilityFailure::Precision);
        }
        if input.source_coverage != SourceCoverageEvidence::Explicit {
            failures.insert(EligibilityFailure::Coverage);
        }
        if input.book_integrity != BookIntegrity::Consistent {
            failures.insert(EligibilityFailure::BookIntegrity);
        }
        if input.stream_integrity != StreamIntegrityState::Healthy {
            failures.insert(EligibilityFailure::StreamIntegrity);
        }
        if input.capture_integrity == CaptureIntegrityState::Incomplete {
            failures.insert(EligibilityFailure::CaptureIntegrity);
        }
        let eligibility = if failures.is_empty() {
            ExecutionEligibility::Eligible
        } else {
            ExecutionEligibility::Ineligible
        };
        Self {
            quality: input.quality,
            fair_value_hierarchy: input.fair_value_hierarchy,
            source_authorization: input.source_authorization,
            delivery_evidence: input.delivery_evidence,
            source_id: input.source_id,
            venue_id: input.venue_id,
            instrument_id: input.instrument_id,
            connection_generation: input.connection_generation,
            sequence_integrity: input.sequence_integrity,
            snapshot_consistency: input.snapshot_consistency,
            checksum_integrity: input.checksum_integrity,
            event_timing: input.event_timing,
            freshness: input.freshness,
            trading_status: input.trading_status,
            precision_integrity: input.precision_integrity,
            source_coverage: input.source_coverage,
            book_integrity: input.book_integrity,
            stream_integrity: input.stream_integrity,
            capture_integrity: input.capture_integrity,
            eligibility,
            failures,
        }
    }
}
