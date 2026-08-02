#![allow(
    dead_code,
    reason = "each integration-test crate uses a different subset of the shared fixture"
)]

use std::error::Error;
use std::str::FromStr;

use market_squawk_domain::{
    AuthorizationBasis, BookIntegrity, BookStateBinding, BoundAssessment, CanonicalStateDigest,
    CanonicalizationRule, CaptureIntegrityState, ChecksumCapability, ChecksumEvidence,
    ChecksumScope, ChecksumValue, ConnectionGeneration, CoverageConsolidation, CoverageDelay,
    CoverageScope, CoverageStatus, DataQuality, DeliveryEvidence, EvidenceDigest,
    InitializedSnapshot, InstrumentId, IntegrityAssessmentSet, IntegrityCapabilities,
    IntegrityRule, LiveEventClass, LiveEvidenceBinding, LiveTimingAssessment, LiveTimingPolicy,
    MarketAssessmentSet, MarketDepth, MarketEventTiming, MetadataRevision, PayloadChecksumScope,
    PayloadHashAlgorithm, PrecisionIntegrity, ProviderChannel, ProviderProduct,
    QualificationAssessmentId, QualificationAssessmentInput, RuleVersion, SequenceCapability,
    SequenceEvidence, SequenceNumber, SequenceValidationRule, SnapshotApplicability,
    SnapshotEvidence, SourceAuthorization, SourceCoverageRecord, SourceId, SourceIdentifier,
    SourcePolicyAssessment, StreamIntegrityState, Timestamp, TradingStatus, VenueId,
};

#[derive(Clone, Debug)]
pub(crate) struct BindingSpec {
    pub source: &'static str,
    pub session: &'static str,
    pub metadata_revision: &'static str,
    pub authorization_basis: &'static str,
    pub venue: &'static str,
    pub instrument: &'static str,
    pub generation: u64,
    pub product: &'static str,
    pub channel: &'static str,
    pub event_class: LiveEventClass,
    pub source_identifier: &'static str,
    pub payload_digest: u8,
    pub state_digest: u8,
    pub book_state_id: &'static str,
    pub snapshot_state_digest: u8,
    pub snapshot_state_id: &'static str,
    pub depth: MarketDepth,
}

impl Default for BindingSpec {
    fn default() -> Self {
        Self {
            source: "coinbase-direct",
            session: "session-7",
            metadata_revision: "coinbase-advanced-trade-v3",
            authorization_basis: "user-authorized-account",
            venue: "COINBASE",
            instrument: "0187f5f1-6fc2-7fa2-bf05-2ce5354c55cb",
            generation: 7,
            product: "BTC-USD",
            channel: "level2",
            event_class: LiveEventClass::BookDelta,
            source_identifier: "update-42",
            payload_digest: 1,
            state_digest: 2,
            book_state_id: "book-state-42",
            snapshot_state_digest: 3,
            snapshot_state_id: "snapshot-state-40",
            depth: MarketDepth::PriceLevel,
        }
    }
}

pub(crate) fn binding(spec: &BindingSpec) -> Result<LiveEvidenceBinding, Box<dyn Error>> {
    let state_digest = CanonicalStateDigest::new(
        EvidenceDigest::new(PayloadHashAlgorithm::Sha256, [spec.state_digest; 32]),
        CanonicalizationRule::new(
            SourceIdentifier::try_from("market-squawk.book.price-level-v1")?,
            RuleVersion::new(1)?,
        ),
    );
    let snapshot_state_digest = CanonicalStateDigest::new(
        EvidenceDigest::new(
            PayloadHashAlgorithm::Sha256,
            [spec.snapshot_state_digest; 32],
        ),
        CanonicalizationRule::new(
            SourceIdentifier::try_from("market-squawk.book.price-level-v1")?,
            RuleVersion::new(1)?,
        ),
    );
    let book_state = if spec.event_class.requires_book_state() {
        Some(BookStateBinding::new_with_snapshot_origin(
            spec.depth,
            SourceIdentifier::try_from(spec.book_state_id)?,
            state_digest.clone(),
            SourceIdentifier::try_from(spec.snapshot_state_id)?,
            snapshot_state_digest,
        ))
    } else {
        None
    };
    Ok(LiveEvidenceBinding::new(
        SourceId::try_from(spec.source)?,
        SourceIdentifier::try_from(spec.session)?,
        MetadataRevision::new(SourceIdentifier::try_from(spec.metadata_revision)?),
        AuthorizationBasis::new(SourceIdentifier::try_from(spec.authorization_basis)?),
        VenueId::try_from(spec.venue)?,
        InstrumentId::from_str(spec.instrument)?,
        ConnectionGeneration::new(spec.generation)?,
        ProviderProduct::new(SourceIdentifier::try_from(spec.product)?),
        ProviderChannel::new(SourceIdentifier::try_from(spec.channel)?),
        spec.event_class,
        SourceIdentifier::try_from(spec.source_identifier)?,
        EvidenceDigest::new(PayloadHashAlgorithm::Sha256, [spec.payload_digest; 32]),
        state_digest,
        book_state,
    )?)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Component {
    SourcePolicy,
    Sequence,
    Snapshot,
    Checksum,
    Timing,
    TradingStatus,
    Precision,
    Coverage,
    Book,
    Stream,
    Capture,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ChecksumFixture {
    Book,
    Payload,
    Unsupported,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SnapshotPolicyFixture {
    Required,
    NotApplicable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RelationalEvidenceSpec {
    pub checksum: ChecksumFixture,
    pub snapshot_policy: SnapshotPolicyFixture,
    pub snapshot_initialized: bool,
    pub sequence_snapshot: Option<u64>,
    pub sequence_previous: Option<u64>,
    pub sequence_observed: Option<u64>,
    pub sequence_uninitialized: bool,
    pub sequence_unsupported: bool,
    pub snapshot_sequence: Option<u64>,
    pub snapshot_observed: Option<u64>,
    pub book_integrity: BookIntegrity,
}

impl RelationalEvidenceSpec {
    pub(crate) fn for_event(event_class: LiveEventClass) -> Self {
        if event_class.requires_book_state() {
            Self {
                checksum: ChecksumFixture::Book,
                snapshot_policy: SnapshotPolicyFixture::Required,
                snapshot_initialized: true,
                sequence_snapshot: Some(40),
                sequence_previous: Some(41),
                sequence_observed: Some(42),
                sequence_uninitialized: false,
                sequence_unsupported: false,
                snapshot_sequence: Some(40),
                snapshot_observed: Some(42),
                book_integrity: BookIntegrity::Consistent,
            }
        } else {
            Self {
                checksum: ChecksumFixture::Payload,
                snapshot_policy: SnapshotPolicyFixture::NotApplicable,
                snapshot_initialized: false,
                sequence_snapshot: Some(40),
                sequence_previous: Some(41),
                sequence_observed: Some(42),
                sequence_uninitialized: false,
                sequence_unsupported: false,
                snapshot_sequence: None,
                snapshot_observed: None,
                book_integrity: BookIntegrity::NotApplicable,
            }
        }
    }
}

fn selected(
    component: Component,
    override_component: Option<Component>,
    base: &LiveEvidenceBinding,
    replacement: &LiveEvidenceBinding,
) -> LiveEvidenceBinding {
    if override_component == Some(component) {
        replacement.clone()
    } else {
        base.clone()
    }
}

pub(crate) fn assessment_input(
    base: LiveEvidenceBinding,
    override_component: Option<Component>,
    replacement: LiveEvidenceBinding,
    strictest_valid_until: Timestamp,
) -> Result<QualificationAssessmentInput, Box<dyn Error>> {
    let spec = RelationalEvidenceSpec::for_event(base.event_class());
    assessment_input_with_relations(
        base,
        override_component,
        replacement,
        strictest_valid_until,
        spec,
    )
}

pub(crate) fn assessment_input_with_relations(
    base: LiveEvidenceBinding,
    override_component: Option<Component>,
    replacement: LiveEvidenceBinding,
    strictest_valid_until: Timestamp,
    spec: RelationalEvidenceSpec,
) -> Result<QualificationAssessmentInput, Box<dyn Error>> {
    let evaluated_at = Timestamp::from_unix_nanos(1_010);
    let ordinary_valid_until = Timestamp::from_unix_nanos(1_100);
    let source_binding = selected(
        Component::SourcePolicy,
        override_component,
        &base,
        &replacement,
    );
    let sequence_binding = selected(Component::Sequence, override_component, &base, &replacement);
    let snapshot_binding = selected(Component::Snapshot, override_component, &base, &replacement);
    let checksum_binding = selected(Component::Checksum, override_component, &base, &replacement);
    let timing_binding = selected(Component::Timing, override_component, &base, &replacement);
    let status_binding = selected(
        Component::TradingStatus,
        override_component,
        &base,
        &replacement,
    );
    let precision_binding = selected(
        Component::Precision,
        override_component,
        &base,
        &replacement,
    );
    let coverage_binding = selected(Component::Coverage, override_component, &base, &replacement);
    let book_binding = selected(Component::Book, override_component, &base, &replacement);
    let stream_binding = selected(Component::Stream, override_component, &base, &replacement);
    let capture_binding = selected(Component::Capture, override_component, &base, &replacement);

    let snapshot_applicability = match spec.snapshot_policy {
        SnapshotPolicyFixture::Required => SnapshotApplicability::Required,
        SnapshotPolicyFixture::NotApplicable => SnapshotApplicability::NotApplicable {
            metadata_rule: rule("provider.snapshot.not-applicable")?,
        },
    };
    let checksum_capability = match spec.checksum {
        ChecksumFixture::Unsupported => ChecksumCapability::Unsupported,
        ChecksumFixture::Book | ChecksumFixture::Payload => ChecksumCapability::Provided,
    };
    let sequence_capability = if spec.sequence_unsupported {
        SequenceCapability::Unsupported
    } else {
        SequenceCapability::Provided
    };
    let source_policy = BoundAssessment::new(
        source_binding,
        evaluated_at,
        ordinary_valid_until,
        SourcePolicyAssessment::new(
            DataQuality::DirectVerified,
            IntegrityCapabilities::new(sequence_capability, checksum_capability),
            SourceAuthorization::Authorized,
            DeliveryEvidence::DirectVenue,
            snapshot_applicability,
        ),
    )?;
    let sequence_generation = sequence_binding.connection_generation();
    let sequence_result = if spec.sequence_unsupported {
        SequenceEvidence::unsupported(sequence_generation)
    } else if spec.sequence_uninitialized {
        SequenceEvidence::uninitialized(
            rule("provider.sequence.consecutive")?,
            SequenceValidationRule::Consecutive,
            sequence_generation,
            spec.sequence_snapshot.map(SequenceNumber::new),
        )
    } else {
        SequenceEvidence::validate(
            SequenceCapability::Provided,
            Some(rule("provider.sequence.consecutive")?),
            SequenceValidationRule::Consecutive,
            sequence_generation,
            spec.sequence_snapshot.map(SequenceNumber::new),
            spec.sequence_previous.map(SequenceNumber::new),
            spec.sequence_observed.map(SequenceNumber::new),
        )?
    };
    let sequence = BoundAssessment::new(
        sequence_binding,
        evaluated_at,
        ordinary_valid_until,
        sequence_result,
    )?;
    let snapshot_generation = snapshot_binding.connection_generation();
    let snapshot_result = if spec.snapshot_initialized {
        let (snapshot_identity, snapshot_digest) = snapshot_binding.book_state().map_or_else(
            || {
                Ok::<_, Box<dyn Error>>((
                    SourceIdentifier::try_from("non-book-snapshot")?,
                    snapshot_binding.canonical_state_digest().clone(),
                ))
            },
            |snapshot_book| {
                Ok((
                    snapshot_book.snapshot_state_id().clone(),
                    snapshot_book.snapshot_state_digest().clone(),
                ))
            },
        )?;
        SnapshotEvidence::assess_initialized(
            InitializedSnapshot::new(
                snapshot_generation,
                snapshot_identity,
                snapshot_digest,
                Timestamp::from_unix_nanos(900),
                spec.snapshot_sequence.map(SequenceNumber::new),
            ),
            snapshot_generation,
            spec.snapshot_observed.map(SequenceNumber::new),
        )?
    } else {
        SnapshotEvidence::uninitialized(snapshot_generation)
    };
    let snapshot = BoundAssessment::new(
        snapshot_binding,
        evaluated_at,
        ordinary_valid_until,
        snapshot_result,
    )?;
    let checksum_generation = checksum_binding.connection_generation();
    let checksum_result = match spec.checksum {
        ChecksumFixture::Book => {
            let checksum_depth = checksum_binding
                .book_state()
                .map_or(MarketDepth::TopOfBook, BookStateBinding::depth);
            ChecksumEvidence::validate_book(
                ChecksumCapability::Provided,
                Some(rule("provider.checksum.crc32")?),
                checksum_generation,
                Some(ChecksumScope::new(
                    checksum_depth,
                    10,
                    SourceIdentifier::try_from("top-ten-bid-ask")?,
                )?),
                Some(ChecksumValue::new(10)),
                Some(ChecksumValue::new(10)),
            )?
        }
        ChecksumFixture::Payload => ChecksumEvidence::validate_payload(
            ChecksumCapability::Provided,
            Some(rule("provider.payload-checksum.crc32")?),
            checksum_generation,
            Some(PayloadChecksumScope::new(SourceIdentifier::try_from(
                "canonical-event-payload",
            )?)),
            Some(ChecksumValue::new(10)),
            Some(ChecksumValue::new(10)),
        )?,
        ChecksumFixture::Unsupported => ChecksumEvidence::unsupported(checksum_generation),
    };
    let checksum = BoundAssessment::new(
        checksum_binding,
        evaluated_at,
        ordinary_valid_until,
        checksum_result,
    )?;
    let timing_generation = timing_binding.connection_generation();
    let timing = BoundAssessment::new(
        timing_binding,
        evaluated_at,
        strictest_valid_until,
        LiveTimingAssessment::assess(
            timing_generation,
            Some(MarketEventTiming::new(
                Some(Timestamp::from_unix_nanos(995)),
                Timestamp::from_unix_nanos(1_000),
            )),
            Some(Timestamp::from_unix_nanos(1_005)),
            evaluated_at,
            LiveTimingPolicy::new(5, 50, 100, 50)?,
        )?,
    )?;

    let coverage = coverage_record(coverage_binding.clone())?;
    let market = MarketAssessmentSet::new(
        BoundAssessment::new(
            status_binding,
            evaluated_at,
            ordinary_valid_until,
            TradingStatus::Active,
        )?,
        BoundAssessment::new(
            precision_binding,
            evaluated_at,
            ordinary_valid_until,
            PrecisionIntegrity::Valid,
        )?,
        BoundAssessment::new(
            coverage_binding,
            evaluated_at,
            ordinary_valid_until,
            coverage,
        )?,
        BoundAssessment::new(
            book_binding,
            evaluated_at,
            ordinary_valid_until,
            spec.book_integrity,
        )?,
        BoundAssessment::new(
            stream_binding,
            evaluated_at,
            ordinary_valid_until,
            StreamIntegrityState::Healthy,
        )?,
        BoundAssessment::new(
            capture_binding,
            evaluated_at,
            ordinary_valid_until,
            CaptureIntegrityState::Healthy,
        )?,
    );
    Ok(QualificationAssessmentInput::new(
        QualificationAssessmentId::new(SourceIdentifier::try_from("assessment:7:42")?),
        base,
        source_policy,
        IntegrityAssessmentSet::new(sequence, snapshot, checksum, timing),
        market,
    ))
}

pub(crate) fn valid_assessment_input() -> Result<QualificationAssessmentInput, Box<dyn Error>> {
    let base = binding(&BindingSpec::default())?;
    assessment_input(base.clone(), None, base, Timestamp::from_unix_nanos(1_020))
}

pub(crate) fn coverage_record(
    binding: LiveEvidenceBinding,
) -> Result<SourceCoverageRecord, Box<dyn Error>> {
    let book_depth = binding.book_state().map(BookStateBinding::depth);
    let scope = CoverageScope::new(
        binding.source_id().clone(),
        binding.venue_id().clone(),
        binding.provider_product().clone(),
        binding.provider_channel().clone(),
        binding.event_class(),
        book_depth,
        CoverageDelay::RealTime,
        CoverageConsolidation::SingleVenue,
        Timestamp::from_unix_nanos(900),
        Some(Timestamp::from_unix_nanos(2_000)),
        binding.metadata_revision().clone(),
    )?;
    SourceCoverageRecord::new(binding, scope, CoverageStatus::Sufficient).map_err(Into::into)
}

pub(crate) fn rule(name: &str) -> Result<IntegrityRule, Box<dyn Error>> {
    Ok(IntegrityRule::new(
        SourceIdentifier::try_from(name)?,
        RuleVersion::new(1)?,
    ))
}
