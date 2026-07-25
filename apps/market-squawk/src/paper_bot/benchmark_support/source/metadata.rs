use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};

use anyhow::{Context as _, Result};
use market_squawk_domain::{
    AssetClass, AuthorizationBasis, ChecksumCapability, Currency, DataQuality, DeliveryEvidence,
    Denomination, DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence,
    InstrumentDefinition, InstrumentDefinitionInput, IntegrityRule, LiveEventClass, LotSize,
    MetadataRevision, ProviderChannel, ProviderProduct, RevisionBoundPayloadEvidence, RuleVersion,
    SchemaVersion, SequenceCapability, SequenceValidationRule, SnapshotApplicability, SourceId,
    SourceIdentifier, TickSize, Timestamp, TradingStatus, VenueId, VenueMapping, VenueSymbol,
};
use market_squawk_sources::{
    AuthorizationGrant, AuthorizationMode, BackoffPolicy, BudgetScope, CoverageTopology,
    EndpointPolicy, FreshnessPolicy, HistoricalCapability, InstrumentCoverage,
    LiveCoverageDeclaration, LiveCoverageRule, LiveProtocolProfile, NetworkAccessPolicy,
    ProviderBudgetPolicy, ProviderNumericPolicy, SemanticInterpretationProfile,
    SequenceValidationProfile, SourceCapabilities, SourceClass, SourceCoverage, SourceMetadata,
    SourceMetadataInput, SourceProtocolProfile,
};
use rust_decimal::Decimal;

pub(super) const FRESHNESS_NANOS: u64 = 86_400_000_000_000;
pub(super) const INSTRUMENT_ID: &str = "018f0000-0000-7000-8000-000000000091";
pub(super) const VENUE_ID: &str = "release-benchmark-venue";

pub(super) fn instrument_definition() -> Result<InstrumentDefinition> {
    Ok(InstrumentDefinition::try_new(InstrumentDefinitionInput {
        instrument_id: INSTRUMENT_ID.parse()?,
        definition_revision: 1_u64.try_into()?,
        asset_class: AssetClass::Crypto,
        primary_denomination: Denomination::Currency(Currency::try_from("USD")?),
        quote_currency: Currency::try_from("USD")?,
        tick_size: TickSize::try_from_decimal(Decimal::new(1, 2))?,
        lot_size: LotSize::try_from_decimal(Decimal::new(1, 2))?,
        contract_multiplier: Decimal::ONE,
        venue_mappings: vec![VenueMapping::new(
            VenueId::try_from(VENUE_ID)?,
            VenueSymbol::try_from("BENCH-USD")?,
        )],
        provider_identities: Vec::new(),
        identifiers: Vec::new(),
        trading_status: TradingStatus::Active,
    })?)
}

pub(super) fn source_metadata() -> Result<SourceMetadata> {
    let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
    let provider = identifier("release-benchmark-local")?;
    let authorization = AuthorizationGrant::new(
        AuthorizationMode::PublicInterface,
        AuthorizationBasis::new(identifier("release-benchmark-local-evidence")?),
        evidence(2),
        effective,
    );
    let coverage = SourceCoverage::try_instrument(
        evidence(3),
        effective,
        vec![AssetClass::Crypto],
        CoverageTopology::single_venue(VenueId::try_from(VENUE_ID)?),
        InstrumentCoverage::enumerated(vec![INSTRUMENT_ID.parse()?])?,
        Some(live_coverage()?),
        market_squawk_domain::CoverageDelay::RealTime,
        DeliveryEvidence::DirectVenue,
    )?;
    let budget = ProviderBudgetPolicy::try_new(
        BudgetScope::new(provider.clone()),
        NonZeroU32::new(1_000_000).context("benchmark request budget must be nonzero")?,
        NonZeroU64::new(60_000_000_000).context("benchmark budget window must be nonzero")?,
        NonZeroU16::MIN,
        BackoffPolicy::try_new(
            NonZeroU64::MIN,
            NonZeroU64::new(1_000_000).context("benchmark maximum backoff must be nonzero")?,
            1_000,
        )?,
    )?;
    Ok(SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        SourceId::try_from("release-performance-diagnostic")?,
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(identifier("release-performance-diagnostic-v1")?),
            evidence(1),
        ),
        SourceClass::Exchange,
        provider,
        authorization,
        coverage,
        DataQuality::DirectVerified,
        NetworkAccessPolicy::Allowlisted(EndpointPolicy::try_new([
            "wss://release-benchmark.invalid",
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
            rule("release-benchmark-decoder")?,
            SemanticInterpretationProfile::new(
                rule("release-benchmark-aggressor")?,
                rule("release-benchmark-auction")?,
                rule("release-benchmark-status")?,
                rule("release-benchmark-corporate-action")?,
            ),
            rule("release-benchmark-timestamp")?,
            SequenceValidationProfile::Provided {
                rule: rule("release-benchmark-sequence")?,
                progression: SequenceValidationRule::Consecutive,
            },
            market_squawk_sources::ChecksumValidationProfile::Unsupported {
                rule: rule("release-benchmark-no-checksum")?,
            },
            true,
            ProviderNumericPolicy::ExactDecimalLexeme,
        ))),
    ))?)
}

fn live_coverage() -> Result<LiveCoverageDeclaration> {
    let not_applicable = SnapshotApplicability::NotApplicable {
        metadata_rule: rule("release-benchmark-non-book")?,
    };
    Ok(LiveCoverageDeclaration::try_new(
        ProviderProduct::new(identifier("release-performance-diagnostic")?),
        ProviderChannel::new(identifier("bounded-local-ingress")?),
        vec![
            LiveCoverageRule::try_new(LiveEventClass::Trade, None, not_applicable)?,
            LiveCoverageRule::try_new(
                LiveEventClass::BookSnapshot,
                Some(market_squawk_domain::MarketDepth::PriceLevel),
                SnapshotApplicability::Required,
            )?,
            LiveCoverageRule::try_new(
                LiveEventClass::BookDelta,
                Some(market_squawk_domain::MarketDepth::PriceLevel),
                SnapshotApplicability::Required,
            )?,
        ],
    )?)
}

pub(super) fn freshness() -> Result<FreshnessPolicy> {
    Ok(FreshnessPolicy::try_new(
        FRESHNESS_NANOS,
        FRESHNESS_NANOS,
        FRESHNESS_NANOS,
        FRESHNESS_NANOS,
        1_000_000_000,
    )?)
}

pub(super) fn identifier(value: impl AsRef<str>) -> Result<SourceIdentifier> {
    Ok(SourceIdentifier::try_from(value.as_ref())?)
}

pub(super) fn rule(value: &str) -> Result<IntegrityRule> {
    Ok(IntegrityRule::new(identifier(value)?, RuleVersion::new(1)?))
}

pub(super) fn evidence(byte: u8) -> ExactPayloadEvidence {
    ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        [byte; 32],
    ))
}
