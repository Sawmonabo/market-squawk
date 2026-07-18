//! Complete live assessment, provenance, and event construction.

#![allow(
    dead_code,
    reason = "actor wiring reaches this crate-private builder through InstrumentLiveProcessor"
)]

use market_squawk_domain::{
    AssessmentValidity, BindingError, BookIntegrity, BookStateBinding, BoundAssessment,
    CanonicalStateDigest, CanonicalizationRule, CaptureIntegrityState, ChecksumCapability,
    ChecksumEvidence, ClassificationError, CoverageError, CoverageScope, CoverageStatus,
    DigestAlgorithm, EvidenceDigest, InitializedSnapshot, IntegrityAssessmentSet,
    IntegrityCapabilities, LiveEvidenceBinding, LiveProvenance, LiveTimingAssessment,
    LiveTimingPolicy, MarketAssessmentSet, MarketEvent, MarketEventError, MarketEventTiming,
    PayloadHash, PayloadReference, PrecisionIntegrity, ProvenanceError, QualificationAssessment,
    QualificationAssessmentId, QualificationAssessmentInput, QualificationError,
    RecordedLiveProvenanceInput, RuleVersion, SequenceCapability, SequenceEvidence,
    SnapshotEvidence, SourceAuthorization, SourceCoverageRecord, SourceIdentifier,
    SourcePolicyAssessment, StreamIntegrityState, Timestamp, TradingStatus,
};
use market_squawk_sources::{
    AuthorizationHealth, ChecksumValidationProfile, CoverageHealth, CurrentProviderObservation,
    ProviderTimestampEvidence, SequenceValidationProfile,
};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Exact initialized snapshot lineage retained by one stream generation.
#[derive(Clone, Debug)]
pub(crate) struct SnapshotOrigin {
    pub(crate) identity: SourceIdentifier,
    pub(crate) digest: CanonicalStateDigest,
    pub(crate) initialized_at: Timestamp,
    pub(crate) sequence: Option<market_squawk_domain::SequenceNumber>,
    pub(crate) state_revision: u64,
}

/// State-derived inputs that cannot be supplied by an external caller.
#[derive(Clone, Debug)]
pub(crate) struct CommittedQualificationEvidence {
    pub(crate) canonical_state_digest: CanonicalStateDigest,
    pub(crate) book_state: Option<BookStateBinding>,
    pub(crate) snapshot_origin: Option<SnapshotOrigin>,
    pub(crate) sequence: SequenceEvidence,
    pub(crate) checksum: ChecksumEvidence,
    pub(crate) trading_status: TradingStatus,
    pub(crate) state_revision: u64,
}

/// Canonical event and complete audit evidence produced from one committed current observation.
#[derive(Debug)]
pub(crate) struct QualifiedEvent {
    pub(crate) event: MarketEvent,
    pub(crate) assessment: QualificationAssessment,
    pub(crate) binding_digest: [u8; 32],
    pub(crate) valid_until: Timestamp,
}

pub(crate) fn build_qualified_event<F>(
    current: &CurrentProviderObservation,
    evidence: CommittedQualificationEvidence,
    evaluated_at: Timestamp,
    build_event: F,
) -> Result<QualifiedEvent, QualificationBuildError>
where
    F: FnOnce(LiveProvenance) -> Result<MarketEvent, MarketEventError>,
{
    current.current_lease().validate_at(evaluated_at)?;
    let observation = current.observation();
    let policy = current.policy();
    let frame = current.frame_evidence();
    let frame_binding = frame.binding();
    let source_timestamp = match observation.timestamp() {
        ProviderTimestampEvidence::Provided { value, .. } => Some(*value),
        ProviderTimestampEvidence::AuthoritativelyAbsent(_) => None,
    };
    let timing_policy = LiveTimingPolicy::new(
        policy.freshness().max_clock_skew_nanos(),
        policy.freshness().max_transport_age_nanos(),
        policy.freshness().max_source_age_nanos(),
        policy.freshness().max_market_age_nanos(),
    )?;
    let timing = LiveTimingAssessment::assess(
        frame_binding.connection_generation(),
        Some(MarketEventTiming::new(
            source_timestamp,
            frame.received_at(),
        )),
        None,
        evaluated_at,
        timing_policy,
    )?;
    let timing_deadline = timing.maximum_valid_instant();
    let mut valid_until = policy.valid_until();
    if let Some(deadline) = timing_deadline {
        valid_until = valid_until.min(deadline);
    }
    if valid_until < evaluated_at {
        return Err(QualificationBuildError::ExpiredWindow);
    }

    let binding = LiveEvidenceBinding::new(
        frame_binding.source_id().clone(),
        frame_binding.session_id().as_source_identifier().clone(),
        frame_binding.metadata_revision().clone(),
        policy.static_authorization().basis().clone(),
        observation.venue().clone(),
        observation.instrument(),
        frame_binding.connection_generation(),
        policy.provider_product().clone(),
        policy.provider_channel().clone(),
        observation.event_class(),
        observation.source_identifier().clone(),
        frame.payload_digest(),
        evidence.canonical_state_digest.clone(),
        evidence.book_state.clone(),
    )?;
    let binding_digest = digest_execution_binding(
        &binding,
        current.frame_evidence().frame_id(),
        evidence.state_revision,
    );
    let assessment_id =
        QualificationAssessmentId::new(digest_identifier("live-v2-", binding_digest)?);
    let source_policy = SourcePolicyAssessment::new(
        policy.quality_ceiling(),
        integrity_capabilities(policy.protocol()),
        source_authorization(current, evaluated_at),
        policy.coverage().delivery(),
        policy.rule().snapshot_applicability().clone(),
    );
    let coverage = build_coverage(current, &binding, evaluated_at)?;
    let snapshot = snapshot_evidence(current, &evidence)?;
    let book_integrity = if observation.event_class().requires_book_state() {
        BookIntegrity::Consistent
    } else {
        BookIntegrity::NotApplicable
    };
    let input = QualificationAssessmentInput::new(
        assessment_id.clone(),
        binding.clone(),
        bind(&binding, evaluated_at, valid_until, source_policy)?,
        IntegrityAssessmentSet::new(
            bind(&binding, evaluated_at, valid_until, evidence.sequence)?,
            bind(&binding, evaluated_at, valid_until, snapshot)?,
            bind(&binding, evaluated_at, valid_until, evidence.checksum)?,
            bind(&binding, evaluated_at, valid_until, timing)?,
        ),
        MarketAssessmentSet::new(
            bind(&binding, evaluated_at, valid_until, evidence.trading_status)?,
            bind(
                &binding,
                evaluated_at,
                valid_until,
                PrecisionIntegrity::Valid,
            )?,
            bind(&binding, evaluated_at, valid_until, coverage)?,
            bind(&binding, evaluated_at, valid_until, book_integrity)?,
            bind(
                &binding,
                evaluated_at,
                valid_until,
                StreamIntegrityState::Healthy,
            )?,
            bind(
                &binding,
                evaluated_at,
                valid_until,
                CaptureIntegrityState::Healthy,
            )?,
        ),
    );
    let assessment = QualificationAssessment::try_from(input)?;
    let recorded_coverage = assessment
        .market()
        .coverage()
        .result()
        .status_at(evaluated_at);
    let payload_digest = frame.payload_digest();
    let provenance = LiveProvenance::recorded(RecordedLiveProvenanceInput::new(
        binding.clone(),
        source_timestamp,
        frame.received_at(),
        evaluated_at,
        evaluated_at,
        assessment.recorded_quality(),
        recorded_coverage,
        PayloadReference::ContentHash(PayloadHash::new(
            payload_digest.algorithm(),
            payload_digest.bytes(),
        )),
        assessment_id.as_source_identifier().clone(),
    ))?;
    let event = build_event(provenance)?;
    Ok(QualifiedEvent {
        event,
        assessment,
        binding_digest,
        valid_until,
    })
}

fn bind<T>(
    binding: &LiveEvidenceBinding,
    evaluated_at: Timestamp,
    valid_until: Timestamp,
    result: T,
) -> Result<BoundAssessment<T>, BindingError>
where
    T: AssessmentValidity,
{
    BoundAssessment::new(binding.clone(), evaluated_at, valid_until, result)
}

fn source_authorization(
    current: &CurrentProviderObservation,
    evaluated_at: Timestamp,
) -> SourceAuthorization {
    let policy = current.policy();
    if policy.static_authorization().is_effective_at(evaluated_at)
        && matches!(
            policy.runtime_authorization(),
            AuthorizationHealth::Valid { valid_until, .. } if *valid_until >= evaluated_at
        )
    {
        SourceAuthorization::Authorized
    } else {
        SourceAuthorization::Unauthorized
    }
}

fn integrity_capabilities(
    protocol: &market_squawk_sources::LiveProtocolProfile,
) -> IntegrityCapabilities {
    let sequence = match protocol.sequence() {
        SequenceValidationProfile::Provided { .. } => SequenceCapability::Provided,
        SequenceValidationProfile::Unsupported { .. } => SequenceCapability::Unsupported,
    };
    let checksum = match protocol.checksum() {
        ChecksumValidationProfile::Provided { .. } => ChecksumCapability::Provided,
        ChecksumValidationProfile::Unsupported { .. } => ChecksumCapability::Unsupported,
    };
    IntegrityCapabilities::new(sequence, checksum)
}

fn build_coverage(
    current: &CurrentProviderObservation,
    binding: &LiveEvidenceBinding,
    evaluated_at: Timestamp,
) -> Result<SourceCoverageRecord, QualificationBuildError> {
    let policy = current.policy();
    let coverage = policy.coverage();
    let scope = CoverageScope::new(
        coverage.source_id().clone(),
        coverage.venue().clone(),
        coverage.provider_product().clone(),
        coverage.provider_channel().clone(),
        coverage.event_class(),
        coverage.depth(),
        coverage.delay(),
        coverage.consolidation(),
        coverage.effective_from(),
        coverage.effective_until(),
        coverage.metadata_revision().clone(),
    )?;
    let runtime_sufficient = matches!(
        policy.runtime_coverage(),
        CoverageHealth::Sufficient {
            provider_product,
            provider_channel,
            valid_until,
            ..
        } if provider_product == coverage.provider_product()
            && provider_channel == coverage.provider_channel()
            && *valid_until >= evaluated_at
    );
    let declared_sufficient = coverage.delay() == market_squawk_domain::CoverageDelay::RealTime
        && coverage.consolidation() != market_squawk_domain::CoverageConsolidation::Partial;
    let status = if runtime_sufficient && declared_sufficient {
        CoverageStatus::Sufficient
    } else {
        CoverageStatus::Insufficient
    };
    Ok(SourceCoverageRecord::new(binding.clone(), scope, status)?)
}

fn snapshot_evidence(
    current: &CurrentProviderObservation,
    evidence: &CommittedQualificationEvidence,
) -> Result<SnapshotEvidence, QualificationBuildError> {
    let generation = current.frame_evidence().binding().connection_generation();
    let Some(origin) = &evidence.snapshot_origin else {
        return Ok(SnapshotEvidence::uninitialized(generation));
    };
    let initialized = InitializedSnapshot::new(
        generation,
        origin.identity.clone(),
        origin.digest.clone(),
        origin.initialized_at,
        origin.sequence,
    );
    Ok(SnapshotEvidence::assess_initialized(
        initialized,
        generation,
        evidence.sequence.observed_sequence(),
    )?)
}

/// Hashes every complete binding dimension for nonce transplant resistance.
fn digest_execution_binding(
    binding: &LiveEvidenceBinding,
    frame_id: market_squawk_sources::FrameId,
    state_revision: u64,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"MSQKLIVEEXECUTIONBINDING\x02");
    digest_component(&mut hasher, binding.source_id().as_str().as_bytes());
    digest_component(&mut hasher, binding.session_id().as_str().as_bytes());
    digest_component(
        &mut hasher,
        binding
            .metadata_revision()
            .as_source_identifier()
            .as_str()
            .as_bytes(),
    );
    digest_component(
        &mut hasher,
        binding
            .authorization_basis()
            .as_source_identifier()
            .as_str()
            .as_bytes(),
    );
    digest_component(&mut hasher, binding.venue_id().as_str().as_bytes());
    hasher.update(binding.instrument_id().as_uuid().as_bytes());
    hasher.update(binding.connection_generation().get().to_be_bytes());
    digest_component(
        &mut hasher,
        binding
            .provider_product()
            .as_source_identifier()
            .as_str()
            .as_bytes(),
    );
    digest_component(
        &mut hasher,
        binding
            .provider_channel()
            .as_source_identifier()
            .as_str()
            .as_bytes(),
    );
    hasher.update([event_class_tag(binding.event_class())]);
    digest_component(&mut hasher, binding.source_identifier().as_str().as_bytes());
    digest_evidence(&mut hasher, binding.payload_digest());
    digest_canonical(&mut hasher, binding.canonical_state_digest());
    if let Some(book) = binding.book_state() {
        hasher.update([1]);
        hasher.update([market_depth_tag(book.depth())]);
        digest_component(&mut hasher, book.state_id().as_str().as_bytes());
        digest_canonical(&mut hasher, book.state_digest());
        digest_component(&mut hasher, book.snapshot_state_id().as_str().as_bytes());
        digest_canonical(&mut hasher, book.snapshot_state_digest());
    } else {
        hasher.update([0]);
    }
    hasher.update(frame_id.get().to_be_bytes());
    hasher.update(state_revision.to_be_bytes());
    hasher.finalize().into()
}

const fn market_depth_tag(depth: market_squawk_domain::MarketDepth) -> u8 {
    match depth {
        market_squawk_domain::MarketDepth::TopOfBook => 1,
        market_squawk_domain::MarketDepth::PriceLevel => 2,
        market_squawk_domain::MarketDepth::OrderLevel => 3,
    }
}

fn digest_identifier(
    prefix: &str,
    digest: [u8; 32],
) -> Result<SourceIdentifier, market_squawk_domain::IdentityError> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(prefix.len() + 64);
    value.push_str(prefix);
    for byte in digest {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    SourceIdentifier::try_from(value)
}

const fn event_class_tag(event_class: market_squawk_domain::LiveEventClass) -> u8 {
    match event_class {
        market_squawk_domain::LiveEventClass::Trade => 1,
        market_squawk_domain::LiveEventClass::Quote => 2,
        market_squawk_domain::LiveEventClass::BookSnapshot => 3,
        market_squawk_domain::LiveEventClass::BookDelta => 4,
        market_squawk_domain::LiveEventClass::Auction => 5,
        market_squawk_domain::LiveEventClass::TradingHalt => 6,
        market_squawk_domain::LiveEventClass::InstrumentStatus => 7,
        market_squawk_domain::LiveEventClass::CorporateAction => 8,
    }
}

fn digest_component(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(bytes);
}

fn digest_evidence(hasher: &mut Sha256, digest: EvidenceDigest) {
    hasher.update([match digest.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    hasher.update(digest.bytes());
}

fn digest_canonical(hasher: &mut Sha256, digest: &CanonicalStateDigest) {
    digest_evidence(hasher, digest.digest());
    digest_component(
        hasher,
        digest.canonicalization_rule().rule().as_str().as_bytes(),
    );
    hasher.update(digest.canonicalization_rule().version().get().to_be_bytes());
}

pub(crate) fn canonical_digest(
    bytes: &[u8],
) -> Result<CanonicalStateDigest, QualificationBuildError> {
    let digest: [u8; 32] = Sha256::digest(bytes).into();
    canonical_digest_from_sha256(digest)
}

pub(crate) fn canonical_digest_from_sha256(
    digest: [u8; 32],
) -> Result<CanonicalStateDigest, QualificationBuildError> {
    Ok(CanonicalStateDigest::new(
        EvidenceDigest::new(DigestAlgorithm::Sha256, digest),
        CanonicalizationRule::new(
            SourceIdentifier::try_from("market-squawk-live-state-v1")?,
            RuleVersion::new(1)?,
        ),
    ))
}

/// Failure to derive a complete relational assessment and canonical event.
#[derive(Debug, Error)]
pub(crate) enum QualificationBuildError {
    #[error(transparent)]
    Registry(#[from] market_squawk_sources::RegistryError),
    #[error(transparent)]
    Binding(#[from] BindingError),
    #[error(transparent)]
    Classification(#[from] ClassificationError),
    #[error(transparent)]
    Coverage(#[from] CoverageError),
    #[error(transparent)]
    Integrity(#[from] market_squawk_domain::IntegrityEvidenceError),
    #[error(transparent)]
    Qualification(#[from] QualificationError),
    #[error(transparent)]
    Provenance(#[from] ProvenanceError),
    #[error(transparent)]
    Market(#[from] MarketEventError),
    #[error(transparent)]
    Identity(#[from] market_squawk_domain::IdentityError),
    #[error("qualification validity window expired before evaluation")]
    ExpiredWindow,
}

#[cfg(test)]
#[path = "qualification/tests.rs"]
mod tests;
