use std::error::Error;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::{
    DepthLimit, LiveRouteConfig, LiveRouteConfigInput, LiveRuntimeConfig, LiveRuntimeConfigInput,
    ShardKey, ShardRoutingVersion, SnapshotLimits,
};
use market_squawk_domain::{
    AggressorSide, AssetClass, AuthorizationBasis, CaptureIntegrityState, ChecksumCapability,
    ConnectionGeneration, Currency, DataQuality, DeliveryEvidence, Denomination, DigestAlgorithm,
    EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, InstrumentDefinition,
    InstrumentDefinitionInput, IntegrityRule, LiveEventClass, LotSize, MetadataRevision,
    ProviderChannel, ProviderProduct, RevisionBoundPayloadEvidence, RuleVersion, SchemaVersion,
    SequenceCapability, SequenceNumber, SequenceValidationRule, SnapshotApplicability, SourceId,
    SourceIdentifier, StreamIntegrityState, TickSize, Timestamp, TradingStatus, VenueId,
    VenueMapping, VenueSymbol,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationHealth, AuthorizationMode,
    BackoffPolicy, BudgetHealth, BudgetScope, CaptureAdmissionIssuer, CaptureDegradationCapability,
    ConnectionLiveness, CoverageHealth, CoverageTopology, CurrentDecodedProviderBatch,
    CurrentHealthReporter, CurrentSourceAuthorityLease, CurrentSourceSession, DecodedProviderBatch,
    DecoderEvidence, EndpointPolicy, FreshnessPolicy, HistoricalCapability, InstrumentCoverage,
    LiveCoverageDeclaration, LiveCoverageRule, LiveProtocolProfile, NetworkAccessPolicy,
    ProviderAggressorEvidence, ProviderBookLevel, ProviderBudgetPolicy, ProviderChecksumEvidence,
    ProviderDecimalLexeme, ProviderNormalizedObservation, ProviderNumericPolicy,
    ProviderObservationPayload, ProviderPrice, ProviderQuantity, ProviderSequenceEvidence,
    ProviderSnapshotEvidence, ProviderTimestampEvidence, RawFrameFactory, RegisteredSource,
    SemanticInterpretationProfile, SequenceValidationProfile, SessionId, SourceCapabilities,
    SourceClass, SourceCoverage, SourceHealthSnapshot, SourceMetadata, SourceMetadataInput,
    SourceProtocolProfile, TransportFrameKind,
};
use rust_decimal::Decimal;

pub(super) type TestResult<T = ()> = Result<T, Box<dyn Error>>;

pub(super) const INSTRUMENT_ONE: &str = "018f0000-0000-7000-8000-000000000001";
pub(super) const INSTRUMENT_TWO: &str = "018f0000-0000-7000-8000-000000000002";
pub(super) const VENUE: &str = "coinbase";
const FRESHNESS_NANOS: u64 = 120_000_000_000;
const CLOCK_SKEW_NANOS: u64 = 1_000_000_000;
static SOURCE_INSTANCE: AtomicU64 = AtomicU64::new(1);

pub(super) fn now() -> TestResult<Timestamp> {
    let duration = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let nanos = i128::from(duration.as_secs())
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(i128::from(duration.subsec_nanos())))
        .ok_or("system timestamp overflow")?;
    Ok(Timestamp::from_unix_nanos(i64::try_from(nanos)?))
}

fn next_after(previous: Timestamp) -> TestResult<Timestamp> {
    let current = now()?;
    Ok(if current > previous {
        current
    } else {
        Timestamp::from_unix_nanos(
            previous
                .unix_nanos()
                .checked_add(1)
                .ok_or("fixture timestamp overflow")?,
        )
    })
}

fn instrument(value: &str) -> TestResult<market_squawk_domain::InstrumentId> {
    Ok(value.parse()?)
}

pub(super) fn route(instrument_id: &str) -> TestResult<ShardKey> {
    Ok(ShardKey::new(
        VenueId::try_from(VENUE)?,
        instrument(instrument_id)?,
    ))
}

pub(super) fn definition(instrument_id: &str) -> TestResult<InstrumentDefinition> {
    Ok(InstrumentDefinition::try_new(InstrumentDefinitionInput {
        instrument_id: instrument(instrument_id)?,
        asset_class: AssetClass::Crypto,
        primary_denomination: Denomination::Currency(Currency::try_from("USD")?),
        tick_size: TickSize::try_from_decimal(Decimal::new(1, 2))?,
        lot_size: LotSize::try_from_decimal(Decimal::new(1, 2))?,
        venue_mappings: vec![VenueMapping::new(
            VenueId::try_from(VENUE)?,
            VenueSymbol::try_from("BTC-USD")?,
        )],
        provider_identities: Vec::new(),
        identifiers: Vec::new(),
        trading_status: TradingStatus::Active,
    })?)
}

pub(super) fn route_config(instrument_id: &str) -> TestResult<LiveRouteConfig> {
    Ok(LiveRouteConfig::try_new(LiveRouteConfigInput {
        route: route(instrument_id)?,
        definition: definition(instrument_id)?,
        depth: DepthLimit::new(8)?,
        nonce_capacity: 16,
        nonce_reclaim_budget: 2,
        maximum_capability_lifetime: Duration::from_secs(1),
    })?)
}

pub(super) fn runtime_config(
    mailbox_count: usize,
    mailbox_bytes: u32,
    maximum_message_bytes: u32,
) -> TestResult<LiveRuntimeConfig> {
    Ok(LiveRuntimeConfig::try_new(LiveRuntimeConfigInput {
        routing_version: ShardRoutingVersion::V1,
        shard_count: 1,
        mailbox_count_per_shard: mailbox_count,
        mailbox_bytes_per_shard: mailbox_bytes,
        maximum_message_bytes,
        maximum_routes_per_shard: 4,
        maximum_sources_per_route: 4,
        registration_control_capacity: 4,
        registration_deadline: Duration::from_secs(2),
        health_event_capacity: 16,
        snapshot_event_budget: 16,
        snapshot_interval: Duration::from_millis(10),
        snapshot_limits: SnapshotLimits::try_new(4, 4, 4, 8, 1_048_576)?,
        maximum_retained_snapshot_readers: 4,
        shutdown_deadline: Duration::from_secs(2),
        maximum_runtime_bytes: 256 * 1024 * 1024,
    })?)
}

fn id(value: &str) -> TestResult<SourceIdentifier> {
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
        FRESHNESS_NANOS,
        FRESHNESS_NANOS,
        FRESHNESS_NANOS,
        FRESHNESS_NANOS,
        CLOCK_SKEW_NANOS,
    )
}

fn metadata(source: &str, revision: &str, instrument_id: &str) -> TestResult<SourceMetadata> {
    let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
    let non_book = SnapshotApplicability::NotApplicable {
        metadata_rule: rule("non-book-no-snapshot-v1")?,
    };
    let live = LiveCoverageDeclaration::try_new(
        ProviderProduct::new(id("advanced-trade")?),
        ProviderChannel::new(id("market-data")?),
        vec![
            LiveCoverageRule::try_new(LiveEventClass::Trade, None, non_book)?,
            LiveCoverageRule::try_new(
                LiveEventClass::BookSnapshot,
                Some(market_squawk_domain::MarketDepth::PriceLevel),
                SnapshotApplicability::Required,
            )?,
        ],
    )?;
    let coverage = SourceCoverage::try_instrument(
        evidence(3),
        effective,
        vec![AssetClass::Crypto],
        CoverageTopology::single_venue(VenueId::try_from(VENUE)?),
        InstrumentCoverage::enumerated(vec![instrument(instrument_id)?])?,
        Some(live),
        market_squawk_domain::CoverageDelay::RealTime,
        DeliveryEvidence::DirectVenue,
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
    Ok(SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        SourceId::try_from(source)?,
        RevisionBoundPayloadEvidence::new(MetadataRevision::new(id(revision)?), evidence(1)),
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
    ))?)
}

#[derive(Debug)]
pub(super) struct SourceHarness {
    source_id: String,
    instrument_id: String,
    registry: AuthoritativeSourceRegistry,
    registered: RegisteredSource,
    session: CurrentSourceSession,
    capture_admission: CaptureAdmissionIssuer,
    #[allow(
        dead_code,
        reason = "private admission tests exercise source invalidation; public overflow tests do not"
    )]
    pub(super) capture_degradation: CaptureDegradationCapability,
    frames: RawFrameFactory,
    reporter: CurrentHealthReporter,
    last_health_at: Timestamp,
    last_frame_at: Timestamp,
    valid_until: Timestamp,
}

impl SourceHarness {
    pub(super) fn try_new(source: &str, generation: u64, instrument_id: &str) -> TestResult<Self> {
        let mut registry = AuthoritativeSourceRegistry::try_new()?;
        let instance = SOURCE_INSTANCE.fetch_add(1, Ordering::Relaxed);
        let revision = format!("{source}-revision-{instance}");
        let at = now()?;
        let registered = registry.register(
            metadata(source, &revision, instrument_id)?,
            Timestamp::from_unix_nanos(0),
        )?;
        Self::activate(
            source.to_owned(),
            instrument_id.to_owned(),
            registry,
            registered,
            generation,
            at,
        )
    }

    fn activate(
        source_id: String,
        instrument_id: String,
        mut registry: AuthoritativeSourceRegistry,
        registered: RegisteredSource,
        generation: u64,
        at: Timestamp,
    ) -> TestResult<Self> {
        let session = registry.begin_session(
            &registered,
            SessionId::new(id(&format!("session-{generation}"))?),
            ConnectionGeneration::new(generation)?,
            at,
        )?;
        let capabilities = registry.take_capture_generation_capabilities(&session)?;
        let (mut capture_control, capture_admission, capture_degradation) =
            capabilities.into_parts();
        capture_control.mark_healthy()?;
        let frames = registry.take_raw_frame_factory(&session)?;
        let reporter = registry.take_current_health_reporter(&session)?;
        let valid_until = Timestamp::from_unix_nanos(
            at.unix_nanos()
                .checked_add(i64::try_from(FRESHNESS_NANOS)?)
                .ok_or("valid-until overflow")?,
        );
        let mut harness = Self {
            source_id,
            instrument_id,
            registry,
            registered,
            session,
            capture_admission,
            capture_degradation,
            frames,
            reporter,
            last_health_at: at,
            last_frame_at: at,
            valid_until,
        };
        harness.refresh_health_at(at)?;
        Ok(harness)
    }

    fn refresh_health_at(&mut self, observed: Timestamp) -> TestResult {
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
                valid_until: self.valid_until,
            },
            CoverageHealth::Sufficient {
                evidence: evidence(12),
                provider_product: ProviderProduct::new(id("advanced-trade")?),
                provider_channel: ProviderChannel::new(id("market-data")?),
                valid_until: self.valid_until,
            },
            BudgetHealth::Available,
            None,
            Vec::new(),
        )?;
        let update = self.reporter.report(health)?;
        self.registry.record_health(&self.session, update)?;
        self.last_health_at = observed;
        Ok(())
    }

    pub(super) fn refresh_health(&mut self) -> TestResult {
        let observed = next_after(self.last_health_at)?;
        self.refresh_health_at(observed)
    }

    pub(super) fn current_lease(&self) -> TestResult<CurrentSourceAuthorityLease> {
        let at = now()?;
        Ok(self
            .registry
            .validate_current_authority(&self.session, at)?
            .try_current_lease(at)?)
    }

    pub(super) fn batch(
        &mut self,
        source_identifier: &str,
        sequence: u64,
    ) -> TestResult<(CurrentSourceAuthorityLease, CurrentDecodedProviderBatch)> {
        self.batch_with_price(source_identifier, sequence, "100.00")
    }

    pub(super) fn batch_with_price(
        &mut self,
        source_identifier: &str,
        sequence: u64,
        price: &str,
    ) -> TestResult<(CurrentSourceAuthorityLease, CurrentDecodedProviderBatch)> {
        let frame_at = next_after(self.last_frame_at)?;
        self.last_frame_at = frame_at;
        let frame = self.frames.try_frame(
            frame_at,
            TransportFrameKind::Binary,
            source_identifier.as_bytes().to_vec().into(),
        )?;
        self.capture_admission.preflight(&frame)?;
        let receipt = self.capture_admission.issue_after_enqueue(&frame)?;
        self.capture_admission.validate_active(&frame)?;
        let validated = self.session.validate_live_frame(&frame)?;
        let decoder = DecoderEvidence::from_validated_frame(&validated, rule("coinbase-decoder")?);
        let observation = ProviderNormalizedObservation::try_new(
            id(source_identifier)?,
            VenueId::try_from(VENUE)?,
            instrument(&self.instrument_id)?,
            ProviderTimestampEvidence::Provided {
                value: frame_at,
                rule: rule("coinbase-timestamp")?,
            },
            ProviderSequenceEvidence::Provided {
                value: SequenceNumber::new(sequence),
                rule: rule("coinbase-sequence")?,
            },
            ProviderSnapshotEvidence::NotApplicable(rule("non-book-no-snapshot-v1")?),
            ProviderChecksumEvidence::Unsupported {
                rule: rule("coinbase-no-checksum")?,
            },
            ProviderObservationPayload::Trade {
                trade_id: id(source_identifier)?,
                price: ProviderPrice::new(ProviderDecimalLexeme::try_new(price)?),
                quantity: ProviderQuantity::new(ProviderDecimalLexeme::try_new("1.00")?),
                aggressor: ProviderAggressorEvidence::new(
                    AggressorSide::Buy,
                    Some(id("BUY")?),
                    rule("coinbase-aggressor")?,
                ),
            },
        )?;
        let decoded = DecodedProviderBatch::try_new(decoder, vec![observation])?;
        let evaluated_at = now()?;
        let current = self
            .registry
            .validate_current_authority(&self.session, evaluated_at)?;
        let lease = current.try_current_lease(evaluated_at)?;
        let batches = current.validate_decoded_batch_owned(decoded, receipt)?;
        let mut batches = batches.into_iter();
        let batch = batches.next().ok_or("missing routed current batch")?;
        if batches.next().is_some() {
            return Err("fixture unexpectedly produced multiple routes".into());
        }
        Ok((lease, batch))
    }

    #[allow(
        dead_code,
        reason = "the shared fixture's nested-book helper is used by private admission tests"
    )]
    pub(super) fn book_snapshot_batch(
        &mut self,
        source_identifier: &str,
        sequence: u64,
        level_count: usize,
    ) -> TestResult<(CurrentSourceAuthorityLease, CurrentDecodedProviderBatch)> {
        let frame_at = next_after(self.last_frame_at)?;
        self.last_frame_at = frame_at;
        let frame = self.frames.try_frame(
            frame_at,
            TransportFrameKind::Binary,
            source_identifier.as_bytes().to_vec().into(),
        )?;
        self.capture_admission.preflight(&frame)?;
        let receipt = self.capture_admission.issue_after_enqueue(&frame)?;
        self.capture_admission.validate_active(&frame)?;
        let validated = self.session.validate_live_frame(&frame)?;
        let decoder = DecoderEvidence::from_validated_frame(&validated, rule("coinbase-decoder")?);
        let levels = (0..level_count)
            .map(|_| {
                Ok(ProviderBookLevel::new(
                    ProviderPrice::new(ProviderDecimalLexeme::try_new("100.00")?),
                    ProviderQuantity::new(ProviderDecimalLexeme::try_new("1.00")?),
                ))
            })
            .collect::<TestResult<Vec<_>>>()?;
        let observation = ProviderNormalizedObservation::try_new(
            id(source_identifier)?,
            VenueId::try_from(VENUE)?,
            instrument(&self.instrument_id)?,
            ProviderTimestampEvidence::Provided {
                value: frame_at,
                rule: rule("coinbase-timestamp")?,
            },
            ProviderSequenceEvidence::Provided {
                value: SequenceNumber::new(sequence),
                rule: rule("coinbase-sequence")?,
            },
            ProviderSnapshotEvidence::InitializingSnapshot {
                provider_reference: Some(id(source_identifier)?),
            },
            ProviderChecksumEvidence::Unsupported {
                rule: rule("coinbase-no-checksum")?,
            },
            ProviderObservationPayload::book_snapshot(
                market_squawk_domain::MarketDepth::PriceLevel,
                levels,
                Vec::new(),
            )?,
        )?;
        let decoded = DecodedProviderBatch::try_new(decoder, vec![observation])?;
        let evaluated_at = now()?;
        let current = self
            .registry
            .validate_current_authority(&self.session, evaluated_at)?;
        let lease = current.try_current_lease(evaluated_at)?;
        let batches = current.validate_decoded_batch_owned(decoded, receipt)?;
        let mut batches = batches.into_iter();
        let batch = batches.next().ok_or("missing routed current batch")?;
        if batches.next().is_some() {
            return Err("fixture unexpectedly produced multiple routes".into());
        }
        Ok((lease, batch))
    }

    pub(super) fn rollover(self, generation: u64) -> TestResult<Self> {
        let Self {
            source_id,
            instrument_id,
            mut registry,
            registered,
            session,
            capture_admission: _,
            capture_degradation: _,
            frames: _,
            reporter: _,
            last_health_at: _,
            last_frame_at: _,
            valid_until: _,
        } = self;
        let at = now()?;
        registry.end_session(&session, at)?;
        Self::activate(
            source_id,
            instrument_id,
            registry,
            registered,
            generation,
            at,
        )
    }
}
