use std::error::Error;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::str::FromStr;

use market_squawk_domain::{
    AggressorSide, AssessmentStatus, AssetClass, AuthorizationBasis, BookIntegrity,
    CaptureIntegrityState, ChecksumCapability, ChecksumEvidence, ChecksumIntegrity, ChecksumValue,
    ConnectionGeneration, CoverageDelay, CoverageStatus, DataQuality, DeliveryEvidence,
    DigestAlgorithm, EffectiveInterval, EligibilityFailure, EvidenceDigest, ExactPayloadEvidence,
    FreshnessState, IntegrityRule, LiveEventClass, MarketEvent, MarketEventError,
    PayloadChecksumScope, PrecisionIntegrity, PriceTicks, ProviderChannel, ProviderProduct,
    QuantityLots, RuleVersion, SchemaVersion, SequenceCapability, SequenceEvidence,
    SequenceIntegrity, SequenceNumber, SequenceValidationRule, SnapshotApplicability,
    SnapshotConsistency, SourceAuthorization, SourceId, SourceIdentifier, StreamIntegrityState,
    Timestamp, TimestampIntegrity, TradeEvent, TradingStatus, VenueId,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationHealth, AuthorizationMode,
    BackoffPolicy, BudgetHealth, BudgetScope, CaptureDegradationCapability, ChecksumAlgorithm,
    ChecksumValidationProfile, ConnectionLiveness, CoverageHealth, CoverageTopology, DecodeOutcome,
    DecodedProviderBatch, DecoderEvidence, EndpointPolicy, FreshnessPolicy, HistoricalCapability,
    InstrumentCoverage, LiveCoverageDeclaration, LiveCoverageRule, LiveProtocolProfile,
    NetworkAccessPolicy, ProviderAggressorEvidence, ProviderBudgetPolicy, ProviderChecksumEvidence,
    ProviderDecimalLexeme, ProviderNormalizedObservation, ProviderNumericPolicy,
    ProviderObservationPayload, ProviderPrice, ProviderQuantity, ProviderSequenceEvidence,
    ProviderSnapshotEvidence, ProviderTimestampEvidence, SemanticInterpretationProfile,
    SequenceValidationProfile, SessionId, SourceCapabilities, SourceClass, SourceCoverage,
    SourceHealthSnapshot, SourceMetadata, SourceMetadataInput, SourceProtocolProfile,
    TransportFrameKind, ValidatedSessionDecodeOutcome,
};

use super::{
    CommittedQualificationEvidence, QualificationBuildError, QualifiedEvent, build_qualified_event,
    canonical_digest,
};

#[path = "tests/assessment_contract.rs"]
mod assessment_contract;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[derive(Debug)]
struct FixtureResidentToken;

impl market_squawk_domain::CaptureResidentToken for FixtureResidentToken {}

fn fixture_resident_lease() -> market_squawk_domain::CaptureResidentGenerationLease {
    market_squawk_domain::CaptureResidentGenerationLease::new(std::sync::Arc::new(
        FixtureResidentToken,
    ))
}

const INSTRUMENT: &str = "4c74ab95-53b9-42ad-9b66-0ed403b88fed";
const HEALTH_AT: i64 = 0;
const FRAME_AT: i64 = 10_000_000;
const EVALUATED_AT: i64 = 20_000_000;
const AUTHORIZATION_UNTIL: i64 = 60_000_000_000;
const COVERAGE_UNTIL: i64 = 70_000_000_000;

#[derive(Clone, Copy, Debug)]
struct FixturePolicy {
    quality: DataQuality,
    checksum: ChecksumCapability,
    delivery: DeliveryEvidence,
    delay: CoverageDelay,
    runtime_authorized: bool,
    runtime_coverage: RuntimeCoverage,
}

#[derive(Clone, Copy, Debug)]
enum RuntimeCoverage {
    Matching,
    MismatchedProduct,
    Insufficient,
}

impl Default for FixturePolicy {
    fn default() -> Self {
        Self {
            quality: DataQuality::DirectVerified,
            checksum: ChecksumCapability::Unsupported,
            delivery: DeliveryEvidence::DirectVenue,
            delay: CoverageDelay::RealTime,
            runtime_authorized: true,
            runtime_coverage: RuntimeCoverage::Matching,
        }
    }
}

#[derive(Debug)]
struct CurrentFixture {
    observations: Vec<market_squawk_sources::CurrentProviderObservation>,
    capture_degradation: CaptureDegradationCapability,
    evaluated_at: Timestamp,
    authorization_until: Timestamp,
    _registry: AuthoritativeSourceRegistry,
}

fn id(value: &str) -> TestResult<SourceIdentifier> {
    Ok(SourceIdentifier::try_from(value)?)
}

fn rule(value: &str) -> TestResult<IntegrityRule> {
    Ok(IntegrityRule::new(id(value)?, RuleVersion::new(1)?))
}

fn exact_evidence(byte: u8) -> ExactPayloadEvidence {
    ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        [byte; 32],
    ))
}

fn freshness() -> TestResult<FreshnessPolicy> {
    Ok(FreshnessPolicy::try_new(
        120_000_000_000,
        120_000_000_000,
        120_000_000_000,
        120_000_000_000,
        100_000_000,
    )?)
}

fn metadata(policy: FixturePolicy) -> TestResult<SourceMetadata> {
    let source_id = SourceId::try_from("coinbase-test")?;
    let revision = market_squawk_domain::MetadataRevision::new(id("revision-1")?);
    let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
    let authorization = AuthorizationGrant::new(
        AuthorizationMode::PublicInterface,
        AuthorizationBasis::new(id("public-interface-terms-v1")?),
        exact_evidence(2),
        effective,
    );
    let live_rule = LiveCoverageRule::try_new(
        LiveEventClass::Trade,
        None,
        SnapshotApplicability::NotApplicable {
            metadata_rule: rule("trade-no-snapshot-v1")?,
        },
    )?;
    let live = LiveCoverageDeclaration::try_new(
        ProviderProduct::new(id("advanced-trade")?),
        ProviderChannel::new(id("market-trades")?),
        vec![live_rule],
    )?;
    let coverage = SourceCoverage::try_instrument(
        exact_evidence(3),
        effective,
        vec![AssetClass::Crypto],
        CoverageTopology::single_venue(VenueId::try_from("coinbase")?),
        InstrumentCoverage::enumerated(vec![market_squawk_domain::InstrumentId::from_str(
            INSTRUMENT,
        )?])?,
        Some(live),
        policy.delay,
        policy.delivery,
    )?;
    let budget = ProviderBudgetPolicy::try_new(
        BudgetScope::new(id("coinbase")?),
        NonZeroU32::new(10).ok_or("zero request budget")?,
        NonZeroU64::new(60_000_000_000).ok_or("zero budget window")?,
        NonZeroU16::new(1).ok_or("zero concurrency")?,
        BackoffPolicy::try_new(
            NonZeroU64::new(1_000_000).ok_or("zero initial backoff")?,
            NonZeroU64::new(60_000_000_000).ok_or("zero maximum backoff")?,
            1_000,
        )?,
    )?;
    let checksum_profile = match policy.checksum {
        ChecksumCapability::Unsupported => ChecksumValidationProfile::Unsupported {
            rule: rule("coinbase-no-checksum")?,
        },
        ChecksumCapability::Provided => ChecksumValidationProfile::Provided {
            rule: rule("coinbase-payload-checksum")?,
            algorithm: ChecksumAlgorithm::Sha256,
            canonicalization: id("coinbase-payload-v1")?,
            scope: id("trade-payload")?,
            book_scope: None,
        },
    };
    let input = SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        source_id,
        market_squawk_domain::RevisionBoundPayloadEvidence::new(revision, exact_evidence(1)),
        SourceClass::Exchange,
        id("coinbase")?,
        authorization,
        coverage,
        policy.quality,
        NetworkAccessPolicy::Allowlisted(EndpointPolicy::try_new([
            "wss://advanced-trade-ws.coinbase.com",
        ])?),
        freshness()?,
        Some(budget),
        SourceCapabilities::new(
            true,
            false,
            SequenceCapability::Provided,
            policy.checksum,
            HistoricalCapability::None,
            true,
        ),
        SourceProtocolProfile::Live(Box::new(LiveProtocolProfile::new(
            rule("coinbase-decoder")?,
            SemanticInterpretationProfile::new(
                rule("coinbase-aggressor")?,
                rule("coinbase-auction")?,
                rule("coinbase-trading-status")?,
                rule("coinbase-corporate-action")?,
            ),
            rule("coinbase-timestamp")?,
            SequenceValidationProfile::Provided {
                rule: rule("coinbase-sequence")?,
                progression: SequenceValidationRule::Consecutive,
            },
            checksum_profile,
            true,
            ProviderNumericPolicy::ExactDecimalLexeme,
        ))),
    );
    Ok(SourceMetadata::try_new(input)?)
}

fn current_fixture(policy: FixturePolicy, frame_count: usize) -> TestResult<CurrentFixture> {
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let registered = registry.register(metadata(policy)?, Timestamp::from_unix_nanos(1))?;
    let session = registry.begin_session(
        &registered,
        SessionId::new(id("session-1")?),
        ConnectionGeneration::new(1)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let capabilities = registry.take_capture_generation_capabilities(&session)?;
    let (mut capture_control, mut capture_admission, capture_degradation) =
        capabilities.into_parts();
    capture_control.mark_healthy()?;
    let mut frames = registry.take_raw_frame_factory(&session)?;
    let mut health_reporter = registry.take_current_health_reporter(&session)?;
    let origin = session.started_at();
    let health_at = origin.checked_add_nanos(HEALTH_AT)?;
    let frame_at = origin.checked_add_nanos(FRAME_AT)?;
    let evaluated_at = origin.checked_add_nanos(EVALUATED_AT)?;
    let authorization_until = origin.checked_add_nanos(AUTHORIZATION_UNTIL)?;
    let coverage_until = origin.checked_add_nanos(COVERAGE_UNTIL)?;
    let authorization = if policy.runtime_authorized {
        AuthorizationHealth::Valid {
            evidence: exact_evidence(11),
            valid_until: authorization_until,
        }
    } else {
        AuthorizationHealth::Invalid
    };
    let coverage = match policy.runtime_coverage {
        RuntimeCoverage::Matching => CoverageHealth::Sufficient {
            evidence: exact_evidence(12),
            provider_product: ProviderProduct::new(id("advanced-trade")?),
            provider_channel: ProviderChannel::new(id("market-trades")?),
            valid_until: coverage_until,
        },
        RuntimeCoverage::MismatchedProduct => CoverageHealth::Sufficient {
            evidence: exact_evidence(12),
            provider_product: ProviderProduct::new(id("wrong-product")?),
            provider_channel: ProviderChannel::new(id("market-trades")?),
            valid_until: coverage_until,
        },
        RuntimeCoverage::Insufficient => CoverageHealth::Limited,
    };
    let health = SourceHealthSnapshot::try_new(
        &session,
        health_at,
        ConnectionLiveness::Live {
            last_activity_at: health_at,
        },
        Some(health_at),
        Some(health_at),
        Some(health_at),
        freshness()?,
        StreamIntegrityState::Healthy,
        CaptureIntegrityState::Healthy,
        authorization,
        coverage,
        BudgetHealth::Available,
        None,
        Vec::new(),
    )?;
    let update = health_reporter.report(health)?;
    registry.record_health(&session, update)?;
    let mut observations = Vec::with_capacity(frame_count);
    for _ in 0..frame_count {
        let frame = frames.try_frame(
            TransportFrameKind::Binary,
            b"identical-wire-payload".as_slice().into(),
        )?;
        capture_admission.preflight(&frame)?;
        let receipt = capture_admission.issue_after_enqueue(&frame, fixture_resident_lease())?;
        let validated = session.validate_live_frame(&frame)?;
        let decoder = DecoderEvidence::from_validated_frame(&validated, rule("coinbase-decoder")?);
        let checksum = match policy.checksum {
            ChecksumCapability::Unsupported => ProviderChecksumEvidence::Unsupported {
                rule: rule("coinbase-no-checksum")?,
            },
            ChecksumCapability::Provided => ProviderChecksumEvidence::Provided {
                value: id("7")?,
                rule: rule("coinbase-payload-checksum")?,
            },
        };
        let observation = ProviderNormalizedObservation::try_new(
            id("trade-1")?,
            VenueId::try_from("coinbase")?,
            market_squawk_domain::InstrumentId::from_str(INSTRUMENT)?,
            ProviderTimestampEvidence::Provided {
                value: frame_at,
                rule: rule("coinbase-timestamp")?,
            },
            ProviderSequenceEvidence::Provided {
                value: SequenceNumber::new(2),
                rule: rule("coinbase-sequence")?,
            },
            ProviderSnapshotEvidence::NotApplicable(rule("trade-no-snapshot-v1")?),
            checksum,
            ProviderObservationPayload::Trade {
                trade_id: id("trade-1")?,
                price: ProviderPrice::new(ProviderDecimalLexeme::try_new("100.00")?),
                quantity: ProviderQuantity::new(ProviderDecimalLexeme::try_new("1.00")?),
                aggressor: ProviderAggressorEvidence::new(
                    AggressorSide::Buy,
                    Some(id("BUY")?),
                    rule("coinbase-aggressor")?,
                ),
                taker_order_type: None,
            },
        )?;
        let batch = DecodedProviderBatch::try_new(decoder, vec![observation])?;
        let validated_session = registry.validate_session(&session, frame.received_at())?;
        let validated_outcome =
            validated_session.validate_decode_outcome_owned(DecodeOutcome::Data(batch), receipt)?;
        let ValidatedSessionDecodeOutcome::Data(captured) = validated_outcome else {
            return Err("data outcome changed disposition".into());
        };
        let current = registry.validate_current_authority(&session)?;
        let routed = current.validate_data_outcome_owned(captured)?;
        let mut routed = routed.into_iter();
        let batch = routed.next().ok_or("missing routed batch")?;
        let mut current_observations = batch.into_observations();
        observations.push(
            current_observations
                .next()
                .ok_or("missing current observation")?,
        );
    }
    Ok(CurrentFixture {
        observations,
        capture_degradation,
        evaluated_at,
        authorization_until,
        _registry: registry,
    })
}

fn evidence(
    checksum: ChecksumEvidence,
    state_revision: u64,
    trading_status: TradingStatus,
) -> TestResult<CommittedQualificationEvidence> {
    Ok(CommittedQualificationEvidence {
        canonical_state_digest: canonical_digest(b"identical-canonical-state")?,
        book_state: None,
        snapshot_origin: None,
        sequence: SequenceEvidence::validate(
            SequenceCapability::Provided,
            Some(rule("coinbase-sequence")?),
            SequenceValidationRule::Consecutive,
            ConnectionGeneration::new(1)?,
            None,
            Some(SequenceNumber::new(1)),
            Some(SequenceNumber::new(2)),
        )?,
        checksum,
        trading_status,
        state_revision,
    })
}

fn unsupported_evidence(
    state_revision: u64,
    trading_status: TradingStatus,
) -> TestResult<CommittedQualificationEvidence> {
    evidence(
        ChecksumEvidence::unsupported(ConnectionGeneration::new(1)?),
        state_revision,
        trading_status,
    )
}

fn payload_checksum(expected: u64, computed: u64) -> TestResult<ChecksumEvidence> {
    Ok(ChecksumEvidence::validate_payload(
        ChecksumCapability::Provided,
        Some(rule("coinbase-payload-checksum")?),
        ConnectionGeneration::new(1)?,
        Some(PayloadChecksumScope::new(id("trade-payload")?)),
        Some(ChecksumValue::new(expected)),
        Some(ChecksumValue::new(computed)),
    )?)
}

fn qualify(
    current: &market_squawk_sources::CurrentProviderObservation,
    evidence: CommittedQualificationEvidence,
) -> Result<QualifiedEvent, QualificationBuildError> {
    build_qualified_event(
        current,
        evidence,
        current
            .frame_evidence()
            .received_at()
            .checked_add_nanos(EVALUATED_AT - FRAME_AT)
            .map_err(|_| QualificationBuildError::ExpiredWindow)?,
        |provenance| {
            Ok(MarketEvent::Trade(TradeEvent::new(
                provenance,
                PriceTicks::new(10_000),
                QuantityLots::new(100).map_err(|_| MarketEventError::ZeroQuantity)?,
                AggressorSide::Buy,
                None,
            )?))
        },
    )
}

fn provenance(event: &MarketEvent) -> TestResult<&market_squawk_domain::LiveProvenance> {
    match event {
        MarketEvent::Trade(trade) => Ok(trade.provenance()),
        _ => Err("fixture unexpectedly produced a non-trade event".into()),
    }
}

#[test]
fn assessment_and_execution_digest_bind_exact_frame_ordinal_and_committed_revision() -> TestResult {
    let fixture = current_fixture(FixturePolicy::default(), 2)?;
    let first = qualify(
        &fixture.observations[0],
        unsupported_evidence(7, TradingStatus::Active)?,
    )?;
    let second_frame = qualify(
        &fixture.observations[1],
        unsupported_evidence(7, TradingStatus::Active)?,
    )?;
    let next_revision = qualify(
        &fixture.observations[0],
        unsupported_evidence(8, TradingStatus::Active)?,
    )?;

    let assessment_id = first
        .assessment
        .assessment_id()
        .as_source_identifier()
        .as_str();
    assert!(assessment_id.starts_with("live-v2-"));
    assert_eq!(assessment_id.len(), "live-v2-".len() + 64);
    assert_eq!(first.binding_digest.len(), 32);
    assert_eq!(fixture.observations[0].frame_evidence().frame_id().get(), 1);
    assert_eq!(fixture.observations[1].frame_evidence().frame_id().get(), 2);
    assert_ne!(first.binding_digest, second_frame.binding_digest);
    assert_ne!(
        first.assessment.assessment_id(),
        second_frame.assessment.assessment_id()
    );
    assert_ne!(first.binding_digest, next_revision.binding_digest);
    assert_ne!(
        first.assessment.assessment_id(),
        next_revision.assessment.assessment_id()
    );
    Ok(())
}

#[test]
fn complete_evidence_mapping_derives_direct_verified_and_exact_deadline() -> TestResult {
    let fixture = current_fixture(FixturePolicy::default(), 1)?;
    let evaluated_at = fixture.evaluated_at;
    let authorization_until = fixture.authorization_until;
    let qualified = qualify(
        &fixture.observations[0],
        unsupported_evidence(1, TradingStatus::Active)?,
    )?;
    let assessment = &qualified.assessment;

    assert_eq!(assessment.recorded_quality(), DataQuality::DirectVerified);
    assert!(assessment.failures().is_empty());
    assert_eq!(
        assessment.source_policy().result().quality_ceiling(),
        DataQuality::DirectVerified
    );
    assert_eq!(
        assessment.source_policy().result().source_authorization(),
        SourceAuthorization::Authorized
    );
    assert_eq!(
        assessment.source_policy().result().delivery_evidence(),
        DeliveryEvidence::DirectVenue
    );
    assert_eq!(
        assessment
            .source_policy()
            .result()
            .integrity_capabilities()
            .sequence(),
        SequenceCapability::Provided
    );
    assert_eq!(
        assessment.integrity().sequence().result().integrity(),
        SequenceIntegrity::Valid
    );
    assert_eq!(
        assessment.integrity().snapshot().result().consistency(),
        SnapshotConsistency::Uninitialized
    );
    assert_eq!(
        assessment.integrity().checksum().result().integrity(),
        ChecksumIntegrity::NotSupported
    );
    assert_eq!(
        assessment
            .integrity()
            .timing()
            .result()
            .timestamp_integrity(),
        TimestampIntegrity::Valid
    );
    assert_eq!(
        assessment.integrity().timing().result().freshness(),
        FreshnessState::Fresh
    );
    assert_eq!(
        *assessment.market().trading_status().result(),
        TradingStatus::Active
    );
    assert_eq!(
        *assessment.market().precision().result(),
        PrecisionIntegrity::Valid
    );
    assert_eq!(
        assessment
            .market()
            .coverage()
            .result()
            .status_at(evaluated_at),
        CoverageStatus::Sufficient
    );
    assert_eq!(
        *assessment.market().book().result(),
        BookIntegrity::NotApplicable
    );
    assert_eq!(
        *assessment.market().stream().result(),
        StreamIntegrityState::Healthy
    );
    assert_eq!(
        *assessment.market().capture().result(),
        CaptureIntegrityState::Healthy
    );
    let deadline = authorization_until;
    assert_eq!(qualified.valid_until, deadline);
    assert_eq!(assessment.valid_until(), deadline);
    assert_eq!(
        assessment.assessment_status_at(deadline),
        AssessmentStatus::Satisfied
    );
    assert_eq!(
        assessment.assessment_status_at(deadline.checked_add_nanos(1)?),
        AssessmentStatus::Rejected
    );
    Ok(())
}

#[test]
fn checksum_semantics_are_fail_closed_and_derived() -> TestResult {
    let provided_policy = FixturePolicy {
        checksum: ChecksumCapability::Provided,
        ..FixturePolicy::default()
    };
    let provided = current_fixture(provided_policy, 1)?;
    let matching = qualify(
        &provided.observations[0],
        evidence(payload_checksum(7, 7)?, 1, TradingStatus::Active)?,
    )?;
    assert_eq!(
        matching.assessment.recorded_quality(),
        DataQuality::DirectVerified
    );
    assert_eq!(
        matching
            .assessment
            .integrity()
            .checksum()
            .result()
            .integrity(),
        ChecksumIntegrity::Valid
    );

    let mismatched = qualify(
        &provided.observations[0],
        evidence(payload_checksum(7, 8)?, 1, TradingStatus::Active)?,
    )?;
    assert_eq!(
        mismatched.assessment.recorded_quality(),
        DataQuality::Quarantined
    );
    assert!(
        mismatched
            .assessment
            .has_failure(EligibilityFailure::ChecksumIntegrity)
    );

    let unsupported = current_fixture(FixturePolicy::default(), 1)?;
    assert!(
        qualify(
            &unsupported.observations[0],
            evidence(payload_checksum(7, 7)?, 1, TradingStatus::Active)?,
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn declared_and_runtime_coverage_delivery_authorization_and_capture_fail_closed() -> TestResult {
    let indirect = current_fixture(
        FixturePolicy {
            quality: DataQuality::DirectUnverified,
            delivery: DeliveryEvidence::Indirect,
            ..FixturePolicy::default()
        },
        1,
    )?;
    let indirect = qualify(
        &indirect.observations[0],
        unsupported_evidence(1, TradingStatus::Active)?,
    )?;
    assert!(
        indirect
            .assessment
            .has_failure(EligibilityFailure::DeliveryNotDirect)
    );

    let delayed = current_fixture(
        FixturePolicy {
            quality: DataQuality::DirectUnverified,
            delay: CoverageDelay::Delayed(1),
            ..FixturePolicy::default()
        },
        1,
    )?;
    let evaluated_at = delayed.evaluated_at;
    let delayed = qualify(
        &delayed.observations[0],
        unsupported_evidence(1, TradingStatus::Active)?,
    )?;
    assert!(delayed.assessment.has_failure(EligibilityFailure::Coverage));
    assert_eq!(
        delayed
            .assessment
            .market()
            .coverage()
            .result()
            .status_at(evaluated_at),
        CoverageStatus::Insufficient
    );

    assert!(
        current_fixture(
            FixturePolicy {
                runtime_coverage: RuntimeCoverage::MismatchedProduct,
                ..FixturePolicy::default()
            },
            1,
        )
        .is_err()
    );
    assert!(
        current_fixture(
            FixturePolicy {
                runtime_authorized: false,
                ..FixturePolicy::default()
            },
            1,
        )
        .is_err()
    );
    assert!(
        current_fixture(
            FixturePolicy {
                runtime_coverage: RuntimeCoverage::Insufficient,
                ..FixturePolicy::default()
            },
            1,
        )
        .is_err()
    );

    let degraded = current_fixture(FixturePolicy::default(), 1)?;
    degraded.capture_degradation.mark_incomplete();
    assert!(
        qualify(
            &degraded.observations[0],
            unsupported_evidence(1, TradingStatus::Active)?,
        )
        .is_err()
    );
    Ok(())
}
