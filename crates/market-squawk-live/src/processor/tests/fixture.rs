use std::error::Error;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::str::FromStr;

use market_squawk_domain::{
    AggressorSide, AssetClass, AuthorizationBasis, CaptureIntegrityState, ChecksumCapability,
    ConnectionGeneration, Currency, DataQuality, DeliveryEvidence, Denomination, DigestAlgorithm,
    EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, InstrumentDefinition,
    InstrumentDefinitionInput, InstrumentId, IntegrityRule, LiveEventClass, LotSize, MarketDepth,
    MetadataRevision, ProviderChannel, ProviderProduct, RevisionBoundPayloadEvidence, RuleVersion,
    SchemaVersion, SequenceCapability, SequenceNumber, SequenceValidationRule,
    SnapshotApplicability, SourceId, SourceIdentifier, StreamIntegrityState, TickSize, Timestamp,
    TradingStatus, VenueId, VenueMapping, VenueSymbol,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationHealth, AuthorizationMode,
    BackoffPolicy, BudgetHealth, BudgetScope, CaptureAdmissionIssuer, CaptureDegradationCapability,
    ConnectionLiveness, CoverageHealth, CoverageTopology, CurrentDecodedProviderBatch,
    CurrentHealthReporter, CurrentSourceAuthorityLease, CurrentSourceSession, DecodeOutcome,
    DecodedProviderBatch, DecoderEvidence, EndpointPolicy, FreshnessPolicy, HistoricalCapability,
    InstrumentCoverage, LiveCoverageDeclaration, LiveCoverageRule, LiveProtocolProfile,
    NetworkAccessPolicy, ProviderAggressorEvidence, ProviderBookChange, ProviderBookLevel,
    ProviderBookSide, ProviderBudgetPolicy, ProviderChecksumEvidence, ProviderDecimalLexeme,
    ProviderNormalizedObservation, ProviderNumericPolicy, ProviderObservationPayload,
    ProviderPrice, ProviderQuantity, ProviderSequenceEvidence, ProviderSnapshotEvidence,
    ProviderStatusEvidence, ProviderTimestampEvidence, RawFrameFactory, RegisteredSource,
    SemanticInterpretationProfile, SequenceValidationProfile, SessionId, SourceCapabilities,
    SourceClass, SourceCoverage, SourceHealthSnapshot, SourceMetadata, SourceMetadataInput,
    SourceProtocolProfile, TransportFrameKind, ValidatedSessionDecodeOutcome,
};
use rust_decimal::Decimal;

pub(super) type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[derive(Debug)]
struct FixtureResidentToken;

impl market_squawk_domain::CaptureResidentToken for FixtureResidentToken {}

fn fixture_resident_lease() -> market_squawk_domain::CaptureResidentGenerationLease {
    market_squawk_domain::CaptureResidentGenerationLease::new(std::sync::Arc::new(
        FixtureResidentToken,
    ))
}

const INSTRUMENT: &str = "4c74ab95-53b9-42ad-9b66-0ed403b88fed";
const VENUE: &str = "coinbase";
pub(super) const HEALTH_AT: i64 = 0;
pub(super) const FRAME_AT: i64 = 10_000_000;
pub(super) const EVALUATED_AT: i64 = 20_000_000;
pub(super) const VALID_UNTIL: i64 = 60_000_000_000;

pub(super) fn id(value: &str) -> TestResult<SourceIdentifier> {
    Ok(SourceIdentifier::try_from(value)?)
}

fn rule(value: &str) -> TestResult<IntegrityRule> {
    Ok(IntegrityRule::new(id(value)?, RuleVersion::new(1)?))
}

fn evidence(byte: u8) -> ExactPayloadEvidence {
    ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        [byte; 32],
    ))
}

fn freshness() -> Result<FreshnessPolicy, market_squawk_sources::SourceMetadataError> {
    FreshnessPolicy::try_new(
        120_000_000_000,
        120_000_000_000,
        120_000_000_000,
        120_000_000_000,
        100_000_000,
    )
}

fn instrument_id() -> TestResult<InstrumentId> {
    Ok(InstrumentId::from_str(INSTRUMENT)?)
}

pub(super) fn definition() -> TestResult<InstrumentDefinition> {
    Ok(InstrumentDefinition::try_new(InstrumentDefinitionInput {
        instrument_id: instrument_id()?,
        definition_revision: market_squawk_domain::InstrumentDefinitionRevision::try_from(1_u64)?,
        asset_class: AssetClass::Crypto,
        primary_denomination: Denomination::Currency(Currency::try_from("USD")?),
        quote_currency: Currency::try_from("USD")?,
        tick_size: TickSize::try_from_decimal(Decimal::new(1, 2))?,
        lot_size: LotSize::try_from_decimal(Decimal::new(1, 2))?,
        contract_multiplier: Decimal::ONE,
        venue_mappings: vec![VenueMapping::new(
            VenueId::try_from(VENUE)?,
            VenueSymbol::try_from("BTC-USD")?,
        )],
        provider_identities: Vec::new(),
        identifiers: Vec::new(),
        trading_status: TradingStatus::Active,
    })?)
}

fn metadata(source: &str, revision: &str) -> TestResult<SourceMetadata> {
    let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
    let non_book = SnapshotApplicability::NotApplicable {
        metadata_rule: rule("non-book-no-snapshot-v1")?,
    };
    let rules = vec![
        LiveCoverageRule::try_new(LiveEventClass::Trade, None, non_book.clone())?,
        LiveCoverageRule::try_new(LiveEventClass::InstrumentStatus, None, non_book)?,
        LiveCoverageRule::try_new(
            LiveEventClass::BookSnapshot,
            Some(MarketDepth::PriceLevel),
            SnapshotApplicability::Required,
        )?,
        LiveCoverageRule::try_new(
            LiveEventClass::BookDelta,
            Some(MarketDepth::PriceLevel),
            SnapshotApplicability::Required,
        )?,
    ];
    let live = LiveCoverageDeclaration::try_new(
        ProviderProduct::new(id("advanced-trade")?),
        ProviderChannel::new(id("market-data")?),
        rules,
    )?;
    let coverage = SourceCoverage::try_instrument(
        evidence(3),
        effective,
        vec![AssetClass::Crypto],
        CoverageTopology::single_venue(VenueId::try_from(VENUE)?),
        InstrumentCoverage::enumerated(vec![instrument_id()?])?,
        Some(live),
        market_squawk_domain::CoverageDelay::RealTime,
        DeliveryEvidence::DirectVenue,
    )?;
    let metadata_revision = MetadataRevision::new(id(revision)?);
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
    let input = SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        SourceId::try_from(source)?,
        RevisionBoundPayloadEvidence::new(metadata_revision, evidence(1)),
        SourceClass::Exchange,
        id("coinbase")?,
        AuthorizationGrant::new(
            AuthorizationMode::PublicInterface,
            AuthorizationBasis::new(id("public-interface-terms-v1")?),
            evidence(2),
            effective,
        ),
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
            market_squawk_sources::ChecksumValidationProfile::Unsupported {
                rule: rule("coinbase-no-checksum")?,
            },
            true,
            ProviderNumericPolicy::ExactDecimalLexeme,
        ))),
    );
    Ok(SourceMetadata::try_new(input)?)
}

#[derive(Debug)]
pub(super) struct SourceHarness {
    registry: AuthoritativeSourceRegistry,
    registered: RegisteredSource,
    session: CurrentSourceSession,
    capture_admission: CaptureAdmissionIssuer,
    pub(super) capture_degradation: CaptureDegradationCapability,
    frames: RawFrameFactory,
    reporter: CurrentHealthReporter,
    timeline_origin: Timestamp,
    last_frame_received_at: Option<Timestamp>,
}

impl SourceHarness {
    pub(super) fn try_new(source: &str, generation: u64) -> TestResult<Self> {
        let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
        let revision = format!("{source}-revision");
        let registered =
            registry.register(metadata(source, &revision)?, Timestamp::from_unix_nanos(1))?;
        Self::activate(registry, registered, generation)
    }

    fn activate(
        mut registry: AuthoritativeSourceRegistry,
        registered: RegisteredSource,
        generation: u64,
    ) -> TestResult<Self> {
        let session = registry.begin_session(
            &registered,
            SessionId::new(id(&format!("session-{generation}"))?),
            ConnectionGeneration::new(generation)?,
            Timestamp::from_unix_nanos(1),
        )?;
        let capabilities = registry.take_capture_generation_capabilities(&session)?;
        let (mut capture_control, capture_admission, capture_degradation) =
            capabilities.into_parts();
        capture_control.mark_healthy()?;
        let frames = registry.take_raw_frame_factory(&session)?;
        let reporter = registry.take_current_health_reporter(&session)?;
        let timeline_origin = session.started_at();
        let mut harness = Self {
            registry,
            registered,
            session,
            capture_admission,
            capture_degradation,
            frames,
            reporter,
            timeline_origin,
            last_frame_received_at: None,
        };
        harness.refresh_health(HEALTH_AT)?;
        Ok(harness)
    }

    pub(super) fn timestamp(&self, offset_nanos: i64) -> TestResult<Timestamp> {
        Ok(self.timeline_origin.checked_add_nanos(offset_nanos)?)
    }

    pub(super) fn refresh_health(&mut self, observed_at: i64) -> TestResult {
        let observed = self.timestamp(observed_at)?;
        let valid_until = self.timestamp(VALID_UNTIL)?;
        let health = SourceHealthSnapshot::try_new(
            &self.session,
            observed,
            ConnectionLiveness::Live {
                last_activity_at: observed,
            },
            Some(observed),
            Some(observed),
            Some(observed),
            freshness()?,
            StreamIntegrityState::Healthy,
            CaptureIntegrityState::Healthy,
            AuthorizationHealth::Valid {
                evidence: evidence(11),
                valid_until,
            },
            CoverageHealth::Sufficient {
                evidence: evidence(12),
                provider_product: ProviderProduct::new(id("advanced-trade")?),
                provider_channel: ProviderChannel::new(id("market-data")?),
                valid_until,
            },
            BudgetHealth::Available,
            None,
            Vec::new(),
        )?;
        let update = self.reporter.report(health)?;
        self.registry.record_health(&self.session, update)?;
        Ok(())
    }

    pub(super) fn current_lease(&self, _at: i64) -> TestResult<CurrentSourceAuthorityLease> {
        Ok(self
            .registry
            .validate_current_authority(&self.session)?
            .try_current_lease()?)
    }

    pub(super) fn last_frame_received_at(&self) -> TestResult<Timestamp> {
        self.last_frame_received_at
            .ok_or_else(|| "source harness has not built a frame".into())
    }

    pub(super) fn batch(
        &mut self,
        source_identifier: &str,
        sequence: u64,
        payload: ProviderObservationPayload,
        snapshot: ProviderSnapshotEvidence,
    ) -> TestResult<(CurrentSourceAuthorityLease, CurrentDecodedProviderBatch)> {
        let frame = self.frames.try_frame(
            TransportFrameKind::Binary,
            source_identifier.as_bytes().to_vec().into(),
        )?;
        self.last_frame_received_at = Some(frame.received_at());
        self.capture_admission.preflight(&frame)?;
        let receipt = self
            .capture_admission
            .issue_after_enqueue(&frame, fixture_resident_lease())?;
        self.capture_admission.validate_active(&frame)?;
        let validated = self.session.validate_live_frame(&frame)?;
        let decoder = DecoderEvidence::from_validated_frame(&validated, rule("coinbase-decoder")?);
        let observation = ProviderNormalizedObservation::try_new(
            id(source_identifier)?,
            VenueId::try_from(VENUE)?,
            instrument_id()?,
            ProviderTimestampEvidence::Provided {
                value: self.timestamp(FRAME_AT)?,
                rule: rule("coinbase-timestamp")?,
            },
            ProviderSequenceEvidence::Provided {
                value: SequenceNumber::new(sequence),
                rule: rule("coinbase-sequence")?,
            },
            snapshot,
            ProviderChecksumEvidence::Unsupported {
                rule: rule("coinbase-no-checksum")?,
            },
            payload,
        )?;
        let decoded = DecodedProviderBatch::try_new(decoder, vec![observation])?;
        let validated_session = self
            .registry
            .validate_session(&self.session, frame.received_at())?;
        let validated_outcome = validated_session
            .validate_decode_outcome_owned(DecodeOutcome::Data(decoded), receipt)?;
        let ValidatedSessionDecodeOutcome::Data(captured) = validated_outcome else {
            return Err("data outcome changed disposition".into());
        };
        let current = self.registry.validate_current_authority(&self.session)?;
        let lease = current.try_current_lease()?;
        let batches = current.validate_data_outcome_owned(captured)?;
        let mut batches = batches.into_iter();
        let batch = batches.next().ok_or("missing routed current batch")?;
        if batches.next().is_some() {
            return Err("fixture unexpectedly produced multiple routes".into());
        }
        Ok((lease, batch))
    }

    pub(super) fn rollover(self, generation: u64, at: i64) -> TestResult<Self> {
        let Self {
            mut registry,
            registered,
            session,
            capture_admission: _,
            capture_degradation: _,
            frames: _,
            reporter: _,
            timeline_origin,
            last_frame_received_at: _,
        } = self;
        registry.end_session(&session, timeline_origin.checked_add_nanos(at)?)?;
        Self::activate(registry, registered, generation)
    }
}

pub(super) fn trade() -> TestResult<ProviderObservationPayload> {
    Ok(ProviderObservationPayload::Trade {
        trade_id: id("trade")?,
        price: ProviderPrice::new(ProviderDecimalLexeme::try_new("100.00")?),
        quantity: ProviderQuantity::new(ProviderDecimalLexeme::try_new("1.00")?),
        aggressor: ProviderAggressorEvidence::new(
            AggressorSide::Buy,
            Some(id("BUY")?),
            rule("coinbase-aggressor")?,
        ),
        taker_order_type: None,
    })
}

pub(super) fn status(status: TradingStatus) -> TestResult<ProviderObservationPayload> {
    Ok(ProviderObservationPayload::InstrumentStatus {
        status: ProviderStatusEvidence::new(
            id(match status {
                TradingStatus::Active => "ACTIVE",
                TradingStatus::Halted => "HALTED",
                TradingStatus::Inactive => "INACTIVE",
                TradingStatus::Delisted => "DELISTED",
            })?,
            rule("coinbase-trading-status")?,
        ),
        trading_status: status,
    })
}

fn provider_level(price: &str, quantity: &str) -> TestResult<ProviderBookLevel> {
    Ok(ProviderBookLevel::new(
        ProviderPrice::new(ProviderDecimalLexeme::try_new(price)?),
        ProviderQuantity::new(ProviderDecimalLexeme::try_new(quantity)?),
    ))
}

pub(super) fn snapshot() -> TestResult<ProviderObservationPayload> {
    Ok(ProviderObservationPayload::book_snapshot(
        MarketDepth::PriceLevel,
        vec![
            provider_level("100.00", "1.00")?,
            provider_level("99.00", "2.00")?,
        ],
        vec![
            provider_level("101.00", "1.00")?,
            provider_level("102.00", "2.00")?,
        ],
    )?)
}

pub(super) fn valid_multi_change_delta() -> TestResult<ProviderObservationPayload> {
    Ok(ProviderObservationPayload::book_delta(
        MarketDepth::PriceLevel,
        vec![
            ProviderBookChange::new(ProviderBookSide::Bid, provider_level("99.00", "7.00")?),
            ProviderBookChange::new(ProviderBookSide::Ask, provider_level("102.00", "0.00")?),
        ],
    )?)
}

pub(super) fn non_book_snapshot() -> TestResult<ProviderSnapshotEvidence> {
    Ok(ProviderSnapshotEvidence::NotApplicable(rule(
        "non-book-no-snapshot-v1",
    )?))
}
