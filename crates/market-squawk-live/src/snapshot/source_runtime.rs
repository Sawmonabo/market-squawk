//! Authority-free source-health and qualification evidence retained at live commit.

use market_squawk_domain::{
    AssessmentStatus, CaptureIntegrityState, ConnectionGeneration, CoverageScope, CoverageStatus,
    DataQuality, InstrumentId, QualificationAssessment, QualificationAssessmentId, SourceId,
    SourceIdentifier, StreamIntegrityState, Timestamp, VenueId,
};
use market_squawk_sources::{
    ConnectionLiveness, MarketFreshness, SourceHealthSnapshot, SourceTimestampFreshness,
    TransportFreshness,
};
use serde::Serialize;
use thiserror::Error;

use super::StreamSnapshot;

/// Immutable current source evidence stripped of every execution and source-session capability.
///
/// Values are created only by the single-writer live processor from one registry-recorded health
/// snapshot and its relationally matching qualification assessment. There is deliberately no
/// public constructor or deserializer.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourceRuntimeEvidenceSnapshot {
    session_id: SourceIdentifier,
    instrument_id: InstrumentId,
    connection_generation: ConnectionGeneration,
    health_epoch: u64,
    state_revision: u64,
    assessment_id: QualificationAssessmentId,
    binding_digest: [u8; 32],
    connection: ConnectionLiveness,
    transport_freshness: TransportFreshness,
    market_freshness: MarketFreshness,
    source_freshness: SourceTimestampFreshness,
    stream_integrity: StreamIntegrityState,
    capture_integrity: CaptureIntegrityState,
    coverage_scope: CoverageScope,
    coverage_status: CoverageStatus,
    quality: DataQuality,
    health_observed_at: Timestamp,
    qualification_evaluated_at: Timestamp,
    qualification_valid_until: Timestamp,
}

impl SourceRuntimeEvidenceSnapshot {
    pub(crate) fn try_from_evidence(
        health: &SourceHealthSnapshot,
        assessment: &QualificationAssessment,
        health_epoch: u64,
        state_revision: u64,
        binding_digest: [u8; 32],
    ) -> Result<Self, SourceRuntimeEvidenceError> {
        let binding = assessment.binding();
        let coverage = assessment.market().coverage().result();
        let coverage_scope = coverage.scope();
        let evaluated_at = assessment.evaluated_at();
        if health_epoch == 0
            || state_revision == 0
            || health.source_id() != binding.source_id()
            || health.metadata_revision() != binding.metadata_revision()
            || health.session_id().as_source_identifier() != binding.session_id()
            || health.connection_generation() != binding.connection_generation()
            || health.stream_integrity() != *assessment.market().stream().result()
            || health.capture_integrity() != *assessment.market().capture().result()
            || coverage_scope.source_id() != binding.source_id()
            || coverage_scope.venue_id() != binding.venue_id()
            || coverage_scope.provider_product() != binding.provider_product()
            || coverage_scope.provider_channel() != binding.provider_channel()
            || coverage_scope.metadata_revision() != binding.metadata_revision()
            || health.observed_at() > evaluated_at
            || evaluated_at > assessment.valid_until()
            || (assessment.recorded_quality() == DataQuality::DirectVerified
                && (assessment.assessment_status_at(evaluated_at) != AssessmentStatus::Satisfied
                    || health
                        .live_valid_until()
                        .is_none_or(|valid_until| valid_until < evaluated_at)))
        {
            return Err(SourceRuntimeEvidenceError::EvidenceMismatch);
        }
        Ok(Self {
            session_id: health.session_id().as_source_identifier().clone(),
            instrument_id: binding.instrument_id(),
            connection_generation: binding.connection_generation(),
            health_epoch,
            state_revision,
            assessment_id: assessment.assessment_id().clone(),
            binding_digest,
            connection: health.connection(),
            transport_freshness: health.transport_freshness(),
            market_freshness: health.market_freshness(),
            source_freshness: health.source_freshness(),
            stream_integrity: health.stream_integrity(),
            capture_integrity: health.capture_integrity(),
            coverage_scope: coverage_scope.clone(),
            coverage_status: coverage.status_at(evaluated_at),
            quality: assessment.recorded_quality(),
            health_observed_at: health.observed_at(),
            qualification_evaluated_at: evaluated_at,
            qualification_valid_until: assessment.valid_until(),
        })
    }

    /// Exact source-defined session identity retained as evidence, not authority.
    pub const fn session_id(&self) -> &SourceIdentifier {
        &self.session_id
    }

    /// Stable internal instrument identity.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Exact source connection generation.
    pub const fn connection_generation(&self) -> ConnectionGeneration {
        self.connection_generation
    }

    /// Registry health revision attached to the committed observation.
    pub const fn health_epoch(&self) -> u64 {
        self.health_epoch
    }

    /// Instrument-owned state revision committed with this evidence pair.
    pub const fn state_revision(&self) -> u64 {
        self.state_revision
    }

    /// Exact qualification assessment identity.
    pub const fn assessment_id(&self) -> &QualificationAssessmentId {
        &self.assessment_id
    }

    /// Digest of the complete live binding and commit identity.
    pub const fn binding_digest(&self) -> [u8; 32] {
        self.binding_digest
    }

    /// Connection liveness, which heartbeat activity may update independently.
    pub const fn connection(&self) -> ConnectionLiveness {
        self.connection
    }

    /// Raw-transport freshness, independent of market-bearing activity.
    pub const fn transport_freshness(&self) -> TransportFreshness {
        self.transport_freshness
    }

    /// Market-bearing freshness, which heartbeat activity cannot refresh.
    pub const fn market_freshness(&self) -> MarketFreshness {
        self.market_freshness
    }

    /// Provider-source timestamp freshness.
    pub const fn source_freshness(&self) -> SourceTimestampFreshness {
        self.source_freshness
    }

    /// Stream-integrity result recorded by source health and qualification.
    pub const fn stream_integrity(&self) -> StreamIntegrityState {
        self.stream_integrity
    }

    /// Capture-integrity result recorded by source health and qualification.
    pub const fn capture_integrity(&self) -> CaptureIntegrityState {
        self.capture_integrity
    }

    /// Exact metadata-backed coverage scope retained by qualification.
    pub const fn coverage_scope(&self) -> &CoverageScope {
        &self.coverage_scope
    }

    /// Coverage result at the qualification evaluation instant.
    pub const fn coverage_status(&self) -> CoverageStatus {
        self.coverage_status
    }

    /// Original quality recorded by qualification.
    pub const fn quality(&self) -> DataQuality {
        self.quality
    }

    /// Source-health observation instant.
    pub const fn health_observed_at(&self) -> Timestamp {
        self.health_observed_at
    }

    /// Complete qualification evaluation instant.
    pub const fn qualification_evaluated_at(&self) -> Timestamp {
        self.qualification_evaluated_at
    }

    /// Inclusive qualification expiry.
    pub const fn qualification_valid_until(&self) -> Timestamp {
        self.qualification_valid_until
    }

    /// Verifies that the immutable evidence still describes its enclosing stream publication.
    pub fn matches_stream(&self, stream: &StreamSnapshot) -> bool {
        let scope = &self.coverage_scope;
        self.instrument_id == stream.instrument()
            && self.connection_generation == stream.connection_generation()
            && self.health_epoch == stream.health_epoch()
            && self.state_revision == stream.state_revision()
            && self.quality == stream.quality()
            && scope.source_id() == stream.source()
            && scope.venue_id() == stream.venue()
            && scope.provider_product() == stream.provider_product()
            && scope.provider_channel() == stream.provider_channel()
    }

    pub(crate) fn dynamic_retained_bytes(&self) -> Option<usize> {
        let scope = &self.coverage_scope;
        self.session_id
            .retained_bytes()
            .checked_add(self.assessment_id.as_source_identifier().retained_bytes())?
            .checked_add(scope.source_id().retained_bytes())?
            .checked_add(scope.venue_id().retained_bytes())?
            .checked_add(
                scope
                    .provider_product()
                    .as_source_identifier()
                    .retained_bytes(),
            )?
            .checked_add(
                scope
                    .provider_channel()
                    .as_source_identifier()
                    .retained_bytes(),
            )?
            .checked_add(
                scope
                    .metadata_revision()
                    .as_source_identifier()
                    .retained_bytes(),
            )
    }
}

pub(crate) const fn source_runtime_evidence_maximum_dynamic_bytes() -> usize {
    SourceId::MAX_LENGTH + VenueId::MAX_LENGTH + 5 * SourceIdentifier::MAX_LENGTH
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum SourceRuntimeEvidenceError {
    #[error("source health and qualification evidence do not share one exact live binding")]
    EvidenceMismatch,
}
