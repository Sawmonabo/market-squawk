use std::collections::HashMap;
use std::error::Error;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::str::FromStr;

use market_squawk_domain::{
    AssetClass, AuthorizationBasis, CaptureIntegrityState, ChecksumCapability,
    ConnectionGeneration, CoverageDelay, DataQuality, DeliveryEvidence, Denomination,
    DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, InstrumentDefinition,
    InstrumentDefinitionInput, InstrumentId, IntegrityRule, LiveEventClass, LotSize, MarketDepth,
    MetadataRevision, ProviderChannel, ProviderProduct, RevisionBoundPayloadEvidence, RuleVersion,
    SchemaVersion, SequenceCapability, SequenceNumber, SequenceValidationRule,
    SnapshotApplicability, SourceId, SourceIdentifier, StreamIntegrityState, TickSize, Timestamp,
    TradingStatus, VenueId, VenueMapping, VenueSymbol,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationHealth, AuthorizationMode,
    BackoffPolicy, BudgetHealth, BudgetScope, ChecksumValidationProfile, ConnectionLiveness,
    CoverageHealth, CoverageTopology, DecodedProviderBatch, DecoderEvidence, EndpointPolicy,
    FreshnessPolicy, HistoricalCapability, InstrumentCoverage, LiveCoverageDeclaration,
    LiveCoverageRule, LiveProtocolProfile, NetworkAccessPolicy, ProviderBookLevel,
    ProviderBudgetPolicy, ProviderChecksumEvidence, ProviderDecimalLexeme,
    ProviderNormalizedObservation, ProviderNumericPolicy, ProviderObservationPayload,
    ProviderPrice, ProviderQuantity, ProviderSequenceEvidence, ProviderSnapshotEvidence,
    ProviderTimestampEvidence, SemanticInterpretationProfile, SequenceValidationProfile, SessionId,
    SourceCapabilities, SourceClass, SourceCoverage, SourceHealthSnapshot, SourceMetadata,
    SourceMetadataInput, SourceProtocolProfile, TransportFrameKind,
};
use rust_decimal::Decimal;

use crate::DepthLimit;
use crate::authority::GenerationLeaseOwner;
use crate::processor::status::StatusBook;
use crate::processor::stream::{StreamState, preview_stream};
use crate::provider_book::BookProcessingScratch;

pub(super) type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[derive(Debug)]
struct FixtureResidentToken;

impl market_squawk_domain::CaptureResidentToken for FixtureResidentToken {}

fn fixture_resident_lease() -> market_squawk_domain::CaptureResidentGenerationLease {
    market_squawk_domain::CaptureResidentGenerationLease::new(std::sync::Arc::new(
        FixtureResidentToken,
    ))
}

pub(super) const CONFIGURED_DEPTH: usize = 4;
const SOURCE_TIMESTAMP: i64 = 10_000_000;
const RECEIVED_AT: i64 = 20_000_000;
const EVALUATED_AT: i64 = 30_000_000;
const SOURCE_VALID_UNTIL: i64 = 60_000_000_000;

const INSTRUMENT: &str = "4c74ab95-53b9-42ad-9b66-0ed403b88fed";
const HEALTH_AT: i64 = 0;
const COVERAGE_VALID_UNTIL: i64 = 70_000_000_000;

#[derive(Clone, Copy, Debug)]
struct SnapshotTimeline {
    source_timestamp: Timestamp,
    received_at: Timestamp,
    evaluated_at: Timestamp,
    source_valid_until: Timestamp,
}

pub(super) struct PopulatedState {
    pub(super) instrument: InstrumentId,
    pub(super) streams: HashMap<market_squawk_sources::CurrentStreamKey, StreamState>,
    pub(super) statuses: StatusBook,
    pub(super) source_timestamp: Timestamp,
    pub(super) received_at: Timestamp,
    pub(super) evaluated_at: Timestamp,
    pub(super) source_valid_until: Timestamp,
    _generation_owners: Vec<GenerationLeaseOwner>,
}

pub(super) fn populated_state() -> TestResult<PopulatedState> {
    let definition = instrument_definition()?;
    let instrument = definition.instrument_id();
    let mut streams = HashMap::new();
    let mut statuses = StatusBook::try_new(3)?;
    let mut generation_owners = Vec::new();
    let mut expected_timeline = None;
    for (allocation, source, product, channel, status) in [
        (
            30,
            "source-z",
            "product-z",
            "channel-z",
            TradingStatus::Active,
        ),
        (
            10,
            "source-a",
            "product-a",
            "channel-a",
            TradingStatus::Halted,
        ),
        (
            20,
            "source-m",
            "product-m",
            "channel-m",
            TradingStatus::Inactive,
        ),
    ] {
        let (current, timeline) = current_snapshot(source, product, channel)?;
        if source == "source-a" {
            expected_timeline = Some(timeline);
        }
        let owner = GenerationLeaseOwner::new(allocation);
        let mut state = StreamState::new(
            ConnectionGeneration::new(1)?,
            owner.lease(),
            current.policy().protocol(),
            DepthLimit::new(CONFIGURED_DEPTH)?,
        )?;
        let staged_status = statuses.stage(&current, status)?;
        statuses.validate_staged(&staged_status)?;
        let mut scratch = BookProcessingScratch::try_new(16)?;
        let candidate = preview_stream(
            &mut state,
            &current,
            &definition,
            status,
            timeline.evaluated_at,
            &mut scratch,
        )?;
        let committed = candidate.commit()?;
        assert_eq!(committed.expected_revision, 1);
        let binding = statuses.commit(staged_status);
        assert_eq!(binding.status, status);
        streams.insert(current.stream_key().clone(), state);
        generation_owners.push(owner);
    }
    let expected_timeline = expected_timeline.ok_or("source-a timeline was not constructed")?;
    Ok(PopulatedState {
        instrument,
        streams,
        statuses,
        source_timestamp: expected_timeline.source_timestamp,
        received_at: expected_timeline.received_at,
        evaluated_at: expected_timeline.evaluated_at,
        source_valid_until: expected_timeline.source_valid_until,
        _generation_owners: generation_owners,
    })
}

fn current_snapshot(
    source: &str,
    product: &str,
    channel: &str,
) -> TestResult<(
    market_squawk_sources::CurrentProviderObservation,
    SnapshotTimeline,
)> {
    let instrument = InstrumentId::from_str(INSTRUMENT)?;
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let registered = registry.register(
        metadata(source, product, channel, instrument)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let session = registry.begin_session(
        &registered,
        SessionId::new(id(&format!("session-{source}"))?),
        ConnectionGeneration::new(1)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let capabilities = registry.take_capture_generation_capabilities(&session)?;
    let (mut initialization, mut admission, _degradation) = capabilities.into_parts();
    initialization.mark_healthy()?;
    let mut frames = registry.take_raw_frame_factory(&session)?;
    let mut reporter = registry.take_current_health_reporter(&session)?;
    let origin = session.started_at();
    let mut timeline = SnapshotTimeline {
        source_timestamp: origin.checked_add_nanos(SOURCE_TIMESTAMP)?,
        received_at: origin.checked_add_nanos(RECEIVED_AT)?,
        evaluated_at: origin.checked_add_nanos(EVALUATED_AT)?,
        source_valid_until: origin.checked_add_nanos(SOURCE_VALID_UNTIL)?,
    };
    let health_at = origin.checked_add_nanos(HEALTH_AT)?;
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
        AuthorizationHealth::Valid {
            evidence: exact_evidence(11),
            valid_until: timeline.source_valid_until,
        },
        CoverageHealth::Sufficient {
            evidence: exact_evidence(12),
            provider_product: ProviderProduct::new(id(product)?),
            provider_channel: ProviderChannel::new(id(channel)?),
            valid_until: origin.checked_add_nanos(COVERAGE_VALID_UNTIL)?,
        },
        BudgetHealth::Available,
        None,
        Vec::new(),
    )?;
    registry.record_health(&session, reporter.report(health)?)?;
    let payload = format!("snapshot-{source}").into_bytes();
    let frame = frames.try_frame(TransportFrameKind::Binary, payload.into())?;
    timeline.received_at = frame.received_at();
    admission.preflight(&frame)?;
    let receipt = admission.issue_after_enqueue(&frame, fixture_resident_lease())?;
    let validated = session.validate_live_frame(&frame)?;
    let decoder = DecoderEvidence::from_validated_frame(&validated, rule("snapshot-decoder")?);
    let observation = ProviderNormalizedObservation::try_new(
        id(&format!("snapshot-{source}"))?,
        VenueId::try_from("coinbase")?,
        instrument,
        ProviderTimestampEvidence::Provided {
            value: timeline.source_timestamp,
            rule: rule("snapshot-timestamp")?,
        },
        ProviderSequenceEvidence::Provided {
            value: SequenceNumber::new(10),
            rule: rule("snapshot-sequence")?,
        },
        ProviderSnapshotEvidence::InitializingSnapshot {
            provider_reference: Some(id(&format!("origin-{source}"))?),
        },
        ProviderChecksumEvidence::Unsupported {
            rule: rule("snapshot-no-checksum")?,
        },
        ProviderObservationPayload::book_snapshot(
            MarketDepth::PriceLevel,
            vec![level("100.00")?, level("99.00")?, level("98.00")?],
            vec![level("101.00")?, level("102.00")?],
        )?,
    )?;
    let batch = DecodedProviderBatch::try_new(decoder, vec![observation])?;
    let current = registry.validate_current_authority(&session)?;
    let mut batches = current
        .validate_decoded_batch_owned(batch, receipt)?
        .into_iter();
    let mut observations = batches
        .next()
        .ok_or("snapshot fixture lost routed batch")?
        .into_observations();
    Ok((
        observations
            .next()
            .ok_or("snapshot fixture lost current observation")?,
        timeline,
    ))
}

fn metadata(
    source: &str,
    product: &str,
    channel: &str,
    instrument: InstrumentId,
) -> TestResult<SourceMetadata> {
    let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
    let revision = MetadataRevision::new(id("revision-1")?);
    let authorization = AuthorizationGrant::new(
        AuthorizationMode::PublicInterface,
        AuthorizationBasis::new(id("public-interface-terms-v1")?),
        exact_evidence(2),
        effective,
    );
    let live = LiveCoverageDeclaration::try_new(
        ProviderProduct::new(id(product)?),
        ProviderChannel::new(id(channel)?),
        vec![LiveCoverageRule::try_new(
            LiveEventClass::BookSnapshot,
            Some(MarketDepth::PriceLevel),
            SnapshotApplicability::Required,
        )?],
    )?;
    let coverage = SourceCoverage::try_instrument(
        exact_evidence(3),
        effective,
        vec![AssetClass::Crypto],
        CoverageTopology::single_venue(VenueId::try_from("coinbase")?),
        InstrumentCoverage::enumerated(vec![instrument])?,
        Some(live),
        CoverageDelay::RealTime,
        DeliveryEvidence::DirectVenue,
    )?;
    let provider = id(source)?;
    let budget = ProviderBudgetPolicy::try_new(
        BudgetScope::new(provider.clone()),
        NonZeroU32::new(10).ok_or("zero request budget")?,
        NonZeroU64::new(60_000_000_000).ok_or("zero budget window")?,
        NonZeroU16::new(1).ok_or("zero concurrency")?,
        BackoffPolicy::try_new(
            NonZeroU64::new(1_000_000).ok_or("zero initial backoff")?,
            NonZeroU64::new(60_000_000_000).ok_or("zero maximum backoff")?,
            1_000,
        )?,
    )?;
    Ok(SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        SourceId::try_from(source)?,
        RevisionBoundPayloadEvidence::new(revision, exact_evidence(1)),
        SourceClass::Exchange,
        provider,
        authorization,
        coverage,
        DataQuality::DirectVerified,
        NetworkAccessPolicy::Allowlisted(EndpointPolicy::try_new([
            "wss://advanced-trade-ws.coinbase.com",
        ])?),
        freshness()?,
        Some(budget),
        SourceCapabilities::new(
            true,
            false,
            SequenceCapability::Provided,
            ChecksumCapability::Unsupported,
            HistoricalCapability::None,
            true,
        ),
        SourceProtocolProfile::Live(Box::new(LiveProtocolProfile::new(
            rule("snapshot-decoder")?,
            SemanticInterpretationProfile::new(
                rule("snapshot-aggressor")?,
                rule("snapshot-auction")?,
                rule("snapshot-trading-status")?,
                rule("snapshot-corporate-action")?,
            ),
            rule("snapshot-timestamp")?,
            SequenceValidationProfile::Provided {
                rule: rule("snapshot-sequence")?,
                progression: SequenceValidationRule::Consecutive,
            },
            ChecksumValidationProfile::Unsupported {
                rule: rule("snapshot-no-checksum")?,
            },
            true,
            ProviderNumericPolicy::ExactDecimalLexeme,
        ))),
    ))?)
}

fn instrument_definition() -> TestResult<InstrumentDefinition> {
    let instrument = InstrumentId::from_str(INSTRUMENT)?;
    Ok(InstrumentDefinition::try_new(InstrumentDefinitionInput {
        instrument_id: instrument,
        asset_class: AssetClass::Crypto,
        primary_denomination: Denomination::Currency(market_squawk_domain::Currency::try_from(
            "USD",
        )?),
        tick_size: TickSize::try_from_decimal(Decimal::new(1, 2))?,
        lot_size: LotSize::try_from_decimal(Decimal::ONE)?,
        venue_mappings: vec![VenueMapping::new(
            VenueId::try_from("coinbase")?,
            VenueSymbol::try_from("BTC-USD")?,
        )],
        provider_identities: Vec::new(),
        identifiers: Vec::new(),
        trading_status: TradingStatus::Active,
    })?)
}

fn level(price: &str) -> TestResult<ProviderBookLevel> {
    Ok(ProviderBookLevel::new(
        ProviderPrice::new(ProviderDecimalLexeme::try_new(price)?),
        ProviderQuantity::new(ProviderDecimalLexeme::try_new("1")?),
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
