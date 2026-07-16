//! Fallible execution qualification derived from retained source and market evidence.

use std::fmt;

use serde::{Deserialize, Serialize};

use super::{
    BookIntegrity, CaptureIntegrityState, ChecksumCapability, ChecksumEvidence, ChecksumIntegrity,
    DataQuality, DeliveryEvidence, ExecutionEligibility, FreshnessState, IntegrityCapabilities,
    LiveTimingAssessment, PrecisionIntegrity, SequenceEvidence, SequenceIntegrity,
    SnapshotConsistency, SnapshotEvidence, SourceAuthorization, SourceCoverageEvidence,
    StreamIntegrityState, TimestampIntegrity,
};
use crate::{
    ConnectionGeneration, InstrumentId, SourceId, SourceIdentifier, TradingStatus, VenueId,
};

/// Durable identity of the qualification audit record retained by promoted live provenance.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct QualificationEvidenceId(SourceIdentifier);

impl QualificationEvidenceId {
    /// Constructs an identity from a bounded source/audit reference.
    pub fn new(value: SourceIdentifier) -> Self {
        Self(value)
    }

    /// Returns the retained audit reference.
    pub const fn as_source_identifier(&self) -> &SourceIdentifier {
        &self.0
    }
}

/// One explicit reason execution qualification failed.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(u32)]
pub enum EligibilityFailure {
    /// The source's declared quality ceiling cannot produce `DirectVerified` output.
    QualityCeiling = 1 << 0,
    /// Source authorization is absent.
    SourceUnauthorized = 1 << 1,
    /// Delivery is neither direct venue nor authorized broker delivery.
    DeliveryNotDirect = 1 << 2,
    /// Sequence evidence is invalid, unsupported, or incomplete.
    SequenceIntegrity = 1 << 3,
    /// Snapshot/update state is not consistent.
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

/// Qualification component named in a relational construction failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QualificationComponent {
    /// Sequence validation evidence.
    Sequence,
    /// Snapshot consistency evidence.
    Snapshot,
    /// Checksum validation evidence.
    Checksum,
    /// Atomic timing evidence.
    Timing,
}

/// A contradiction across otherwise valid qualification evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QualificationError {
    /// Evidence belongs to a different connection generation.
    GenerationMismatch {
        /// Component whose generation disagreed.
        component: QualificationComponent,
        /// Generation being qualified.
        expected: ConnectionGeneration,
        /// Generation retained by the evidence component.
        found: ConnectionGeneration,
    },
    /// Evidence capability contradicts authoritative source metadata.
    CapabilityMismatch {
        /// Component whose capability disagreed.
        component: QualificationComponent,
    },
    /// Retained snapshot/observed sequence values disagree across components.
    EvidenceDisagreement {
        /// Component at which the disagreement was detected.
        component: QualificationComponent,
    },
}

impl fmt::Display for QualificationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GenerationMismatch {
                component,
                expected,
                found,
            } => write!(
                formatter,
                "{component:?} generation {found} does not match qualified generation {expected}"
            ),
            Self::CapabilityMismatch { component } => write!(
                formatter,
                "{component:?} capability contradicts source metadata"
            ),
            Self::EvidenceDisagreement { component } => {
                write!(formatter, "{component:?} evidence values disagree")
            }
        }
    }
}

impl std::error::Error for QualificationError {}

/// Typed observations from which quality and eligibility are derived.
///
/// This input has no quality-result or eligibility setter. `quality_ceiling` is a source metadata
/// constraint, not a claim about the resulting observation.
#[derive(Clone, Debug)]
pub struct QualificationEvidenceInput {
    evidence_id: QualificationEvidenceId,
    quality_ceiling: DataQuality,
    integrity_capabilities: IntegrityCapabilities,
    source_authorization: SourceAuthorization,
    delivery_evidence: DeliveryEvidence,
    source_id: SourceId,
    venue_id: VenueId,
    instrument_id: InstrumentId,
    connection_generation: ConnectionGeneration,
    sequence_evidence: SequenceEvidence,
    snapshot_evidence: SnapshotEvidence,
    checksum_evidence: ChecksumEvidence,
    timing: LiveTimingAssessment,
    trading_status: TradingStatus,
    precision_integrity: PrecisionIntegrity,
    source_coverage: SourceCoverageEvidence,
    book_integrity: BookIntegrity,
    stream_integrity: StreamIntegrityState,
    capture_integrity: CaptureIntegrityState,
}

impl QualificationEvidenceInput {
    /// Collects evidence without accepting a quality-result or eligibility claim.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        evidence_id: QualificationEvidenceId,
        quality_ceiling: DataQuality,
        integrity_capabilities: IntegrityCapabilities,
        source_authorization: SourceAuthorization,
        delivery_evidence: DeliveryEvidence,
        source_id: SourceId,
        venue_id: VenueId,
        instrument_id: InstrumentId,
        connection_generation: ConnectionGeneration,
        sequence_evidence: SequenceEvidence,
        snapshot_evidence: SnapshotEvidence,
        checksum_evidence: ChecksumEvidence,
        timing: LiveTimingAssessment,
        trading_status: TradingStatus,
        precision_integrity: PrecisionIntegrity,
        source_coverage: SourceCoverageEvidence,
        book_integrity: BookIntegrity,
        stream_integrity: StreamIntegrityState,
        capture_integrity: CaptureIntegrityState,
    ) -> Self {
        Self {
            evidence_id,
            quality_ceiling,
            integrity_capabilities,
            source_authorization,
            delivery_evidence,
            source_id,
            venue_id,
            instrument_id,
            connection_generation,
            sequence_evidence,
            snapshot_evidence,
            checksum_evidence,
            timing,
            trading_status,
            precision_integrity,
            source_coverage,
            book_integrity,
            stream_integrity,
            capture_integrity,
        }
    }

    /// Replaces the source quality ceiling for state-machine evaluation.
    pub fn with_quality_ceiling(mut self, quality_ceiling: DataQuality) -> Self {
        self.quality_ceiling = quality_ceiling;
        self
    }

    /// Replaces source metadata capabilities for contradiction tests and reconfiguration.
    pub fn with_integrity_capabilities(mut self, capabilities: IntegrityCapabilities) -> Self {
        self.integrity_capabilities = capabilities;
        self
    }

    /// Replaces sequence evidence for state-machine evaluation.
    pub fn with_sequence_evidence(mut self, evidence: SequenceEvidence) -> Self {
        self.sequence_evidence = evidence;
        self
    }

    /// Replaces snapshot evidence for state-machine evaluation.
    pub fn with_snapshot_evidence(mut self, evidence: SnapshotEvidence) -> Self {
        self.snapshot_evidence = evidence;
        self
    }

    /// Replaces checksum evidence for state-machine evaluation.
    pub fn with_checksum_evidence(mut self, evidence: ChecksumEvidence) -> Self {
        self.checksum_evidence = evidence;
        self
    }

    /// Replaces timing evidence for state-machine evaluation.
    pub fn with_timing(mut self, timing: LiveTimingAssessment) -> Self {
        self.timing = timing;
        self
    }

    /// Replaces capture evidence for state-machine evaluation.
    pub fn with_capture_integrity(mut self, capture_integrity: CaptureIntegrityState) -> Self {
        self.capture_integrity = capture_integrity;
        self
    }

    /// Replaces authorization evidence for table-driven qualification tests.
    pub fn with_source_authorization(mut self, value: SourceAuthorization) -> Self {
        self.source_authorization = value;
        self
    }

    /// Replaces delivery evidence for table-driven qualification tests.
    pub fn with_delivery_evidence(mut self, value: DeliveryEvidence) -> Self {
        self.delivery_evidence = value;
        self
    }

    /// Replaces trading status for table-driven qualification tests.
    pub fn with_trading_status(mut self, value: TradingStatus) -> Self {
        self.trading_status = value;
        self
    }

    /// Replaces precision evidence for table-driven qualification tests.
    pub fn with_precision_integrity(mut self, value: PrecisionIntegrity) -> Self {
        self.precision_integrity = value;
        self
    }

    /// Replaces coverage evidence for table-driven qualification tests.
    pub fn with_source_coverage(mut self, value: SourceCoverageEvidence) -> Self {
        self.source_coverage = value;
        self
    }

    /// Replaces book evidence for table-driven qualification tests.
    pub fn with_book_integrity(mut self, value: BookIntegrity) -> Self {
        self.book_integrity = value;
        self
    }

    /// Replaces stream evidence for table-driven qualification tests.
    pub fn with_stream_integrity(mut self, value: StreamIntegrityState) -> Self {
        self.stream_integrity = value;
        self
    }
}

/// Immutable evidence with derived quality, eligibility, and failure set.
#[derive(Clone, Debug, Serialize)]
pub struct QualificationEvidence {
    evidence_id: QualificationEvidenceId,
    quality_ceiling: DataQuality,
    quality: DataQuality,
    integrity_capabilities: IntegrityCapabilities,
    source_authorization: SourceAuthorization,
    delivery_evidence: DeliveryEvidence,
    source_id: SourceId,
    venue_id: VenueId,
    instrument_id: InstrumentId,
    connection_generation: ConnectionGeneration,
    sequence_evidence: SequenceEvidence,
    snapshot_evidence: SnapshotEvidence,
    checksum_evidence: ChecksumEvidence,
    timing: LiveTimingAssessment,
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
    /// Returns the durable qualification evidence identity.
    pub const fn evidence_id(&self) -> &QualificationEvidenceId {
        &self.evidence_id
    }

    /// Returns the data quality derived by the evaluator.
    pub const fn quality(&self) -> DataQuality {
        self.quality
    }

    /// Returns the source metadata quality ceiling.
    pub const fn quality_ceiling(&self) -> DataQuality {
        self.quality_ceiling
    }

    /// Returns the source metadata capabilities used by this evaluation.
    pub const fn integrity_capabilities(&self) -> IntegrityCapabilities {
        self.integrity_capabilities
    }

    /// Returns eligibility computed from all retained evidence.
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

    /// Returns source-authorization evidence.
    pub const fn source_authorization(&self) -> SourceAuthorization {
        self.source_authorization
    }

    /// Returns direct-delivery evidence.
    pub const fn delivery_evidence(&self) -> DeliveryEvidence {
        self.delivery_evidence
    }

    /// Returns auditable sequence evidence.
    pub const fn sequence_evidence(&self) -> &SequenceEvidence {
        &self.sequence_evidence
    }

    /// Returns generation-bound snapshot evidence.
    pub const fn snapshot_evidence(&self) -> SnapshotEvidence {
        self.snapshot_evidence
    }

    /// Returns auditable checksum evidence.
    pub const fn checksum_evidence(&self) -> &ChecksumEvidence {
        &self.checksum_evidence
    }

    /// Returns atomic live timing evidence.
    pub const fn timing(&self) -> LiveTimingAssessment {
        self.timing
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

impl TryFrom<QualificationEvidenceInput> for QualificationEvidence {
    type Error = QualificationError;

    fn try_from(input: QualificationEvidenceInput) -> Result<Self, Self::Error> {
        validate_relations(&input)?;
        let failures = derive_failures(&input);
        let eligibility = if failures.is_empty() {
            ExecutionEligibility::Eligible
        } else {
            ExecutionEligibility::Ineligible
        };
        let quality = derive_quality(&input, failures);
        Ok(Self {
            evidence_id: input.evidence_id,
            quality_ceiling: input.quality_ceiling,
            quality,
            integrity_capabilities: input.integrity_capabilities,
            source_authorization: input.source_authorization,
            delivery_evidence: input.delivery_evidence,
            source_id: input.source_id,
            venue_id: input.venue_id,
            instrument_id: input.instrument_id,
            connection_generation: input.connection_generation,
            sequence_evidence: input.sequence_evidence,
            snapshot_evidence: input.snapshot_evidence,
            checksum_evidence: input.checksum_evidence,
            timing: input.timing,
            trading_status: input.trading_status,
            precision_integrity: input.precision_integrity,
            source_coverage: input.source_coverage,
            book_integrity: input.book_integrity,
            stream_integrity: input.stream_integrity,
            capture_integrity: input.capture_integrity,
            eligibility,
            failures,
        })
    }
}

fn validate_relations(input: &QualificationEvidenceInput) -> Result<(), QualificationError> {
    validate_generation(
        input.connection_generation,
        input.sequence_evidence.connection_generation(),
        QualificationComponent::Sequence,
    )?;
    validate_generation(
        input.connection_generation,
        input.snapshot_evidence.connection_generation(),
        QualificationComponent::Snapshot,
    )?;
    validate_generation(
        input.connection_generation,
        input.checksum_evidence.connection_generation(),
        QualificationComponent::Checksum,
    )?;
    validate_generation(
        input.connection_generation,
        input.timing.connection_generation(),
        QualificationComponent::Timing,
    )?;
    if input.integrity_capabilities.sequence() != input.sequence_evidence.capability() {
        return Err(QualificationError::CapabilityMismatch {
            component: QualificationComponent::Sequence,
        });
    }
    if input.integrity_capabilities.checksum() != input.checksum_evidence.capability() {
        return Err(QualificationError::CapabilityMismatch {
            component: QualificationComponent::Checksum,
        });
    }
    if let (Some(sequence_snapshot), Some(snapshot_snapshot)) = (
        input.sequence_evidence.snapshot_sequence(),
        input.snapshot_evidence.snapshot_sequence(),
    ) && sequence_snapshot != snapshot_snapshot
    {
        return Err(QualificationError::EvidenceDisagreement {
            component: QualificationComponent::Snapshot,
        });
    }
    if let (Some(sequence_observed), Some(snapshot_observed)) = (
        input.sequence_evidence.observed_sequence(),
        input.snapshot_evidence.observed_sequence(),
    ) && sequence_observed != snapshot_observed
    {
        return Err(QualificationError::EvidenceDisagreement {
            component: QualificationComponent::Sequence,
        });
    }
    Ok(())
}

fn validate_generation(
    expected: ConnectionGeneration,
    found: ConnectionGeneration,
    component: QualificationComponent,
) -> Result<(), QualificationError> {
    if expected == found {
        Ok(())
    } else {
        Err(QualificationError::GenerationMismatch {
            component,
            expected,
            found,
        })
    }
}

fn derive_failures(input: &QualificationEvidenceInput) -> EligibilityFailures {
    let mut failures = EligibilityFailures::empty();
    if input.quality_ceiling != DataQuality::DirectVerified {
        failures.insert(EligibilityFailure::QualityCeiling);
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
    if input.sequence_evidence.integrity() != SequenceIntegrity::Valid {
        failures.insert(EligibilityFailure::SequenceIntegrity);
    }
    if input.snapshot_evidence.consistency() != SnapshotConsistency::Consistent {
        failures.insert(EligibilityFailure::SnapshotConsistency);
    }
    let checksum_passes = match input.integrity_capabilities.checksum() {
        ChecksumCapability::Provided => {
            input.checksum_evidence.integrity() == ChecksumIntegrity::Valid
        }
        ChecksumCapability::Unsupported => {
            input.checksum_evidence.integrity() == ChecksumIntegrity::NotSupported
        }
    };
    if !checksum_passes {
        failures.insert(EligibilityFailure::ChecksumIntegrity);
    }
    if input.timing.timestamp_integrity() != TimestampIntegrity::Valid {
        failures.insert(EligibilityFailure::EventTiming);
    }
    if input.timing.freshness() != FreshnessState::Fresh {
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
    failures
}

fn derive_quality(
    input: &QualificationEvidenceInput,
    failures: EligibilityFailures,
) -> DataQuality {
    let hard_integrity_failure = matches!(
        input.sequence_evidence.integrity(),
        SequenceIntegrity::Invalid
    ) || matches!(
        input.snapshot_evidence.consistency(),
        SnapshotConsistency::Inconsistent
    ) || matches!(
        input.checksum_evidence.integrity(),
        ChecksumIntegrity::Failed
    ) || matches!(input.book_integrity, BookIntegrity::Crossed)
        || matches!(
            input.stream_integrity,
            StreamIntegrityState::GapDetected
                | StreamIntegrityState::ChecksumFailed
                | StreamIntegrityState::Divergent
                | StreamIntegrityState::Quarantined
        );
    if hard_integrity_failure || input.quality_ceiling == DataQuality::Quarantined {
        DataQuality::Quarantined
    } else if input.timing.freshness() == FreshnessState::Stale
        || input.stream_integrity == StreamIntegrityState::Stale
        || input.quality_ceiling == DataQuality::Stale
    {
        DataQuality::Stale
    } else if input.quality_ceiling != DataQuality::DirectVerified {
        input.quality_ceiling
    } else if failures.is_empty() {
        DataQuality::DirectVerified
    } else {
        DataQuality::DirectUnverified
    }
}
