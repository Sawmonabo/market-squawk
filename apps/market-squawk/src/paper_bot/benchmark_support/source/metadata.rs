use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result};
use market_squawk_data::{
    CatalogAuthority, CatalogConfig, CatalogLimit, CatalogResultLimits,
    MarketDataInstrumentReadCapability, MarketDataInstrumentSynchronization,
    MarketDataInstrumentSynchronizationCapability,
};
use market_squawk_domain::{
    AssetClass, AuthorizationBasis, ChecksumCapability, Currency, DataQuality, DeliveryEvidence,
    Denomination, DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence,
    InstrumentDefinition, InstrumentDefinitionInput, IntegrityRule, LiveEventClass, LotSize,
    MarketDataInstrumentDefinition, MarketDataInstrumentDefinitionInput, MetadataRevision,
    ProviderChannel, ProviderIdentityEvidence, ProviderIdentityRecord, ProviderIdentityRecordInput,
    ProviderInstrumentId, ProviderProduct, RevisionBoundPayloadEvidence, RuleVersion,
    SchemaVersion, SequenceCapability, SequenceValidationRule, SnapshotApplicability, SourceId,
    SourceIdentifier, TickSize, Timestamp, TradingStatus, VenueId, VenueMapping, VenueSymbol,
};
use market_squawk_sources::{
    AuthorizationGrant, AuthorizationMode, BackoffPolicy, BudgetScope, CoverageTopology,
    EndpointPolicy, FreshnessPolicy, HistoricalCapability, InstrumentCoverage,
    LiveCoverageDeclaration, LiveCoverageRule, LiveProtocolProfile, NetworkAccessPolicy,
    ProviderBudgetPolicy, ProviderNativeIdentityRequest, ProviderNativeInstrumentIdentity,
    ProviderNumericPolicy, SemanticInterpretationProfile, SequenceValidationProfile,
    SourceCapabilities, SourceClass, SourceCoverage, SourceMetadata, SourceMetadataInput,
    SourceProtocolProfile,
};
use rust_decimal::Decimal;
use tokio_util::sync::CancellationToken;

pub(super) const FRESHNESS_NANOS: u64 = 86_400_000_000_000;
pub(super) const INSTRUMENT_ID: &str = "018f0000-0000-7000-8000-000000000091";
pub(super) const VENUE_ID: &str = "release-benchmark-venue";
const IDENTITY_NAMESPACE: &str = "release-benchmark-local";
const NATIVE_SYMBOL: &str = "BENCH-USD";

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
            VenueSymbol::try_from(NATIVE_SYMBOL)?,
        )],
        provider_identities: vec![provider_identity()?],
        identifiers: Vec::new(),
        trading_status: TradingStatus::Active,
    })?)
}

/// Uses the production catalog publisher and selector over one explicitly synthetic local row.
/// This setup runs before warm-up or measurement and never touches the installed catalog.
pub(super) fn identity_catalog(
    cancellation: &CancellationToken,
) -> Result<(tempfile::TempDir, MarketDataInstrumentReadCapability)> {
    let directory = tempfile::tempdir()?;
    let paths = market_squawk_platform::LocalPaths::prepare(directory.path().join("catalog"))?;
    let authority = Arc::new(Mutex::new(CatalogAuthority::open(CatalogConfig::try_new(
        paths.catalog()?.clone(),
        Duration::from_millis(750),
        CatalogLimit::new(1)?,
        CatalogResultLimits::try_new(64 * 1024, 1024 * 1024)?,
    )?)?));
    let execution_definition = instrument_definition()?;
    let definition =
        MarketDataInstrumentDefinition::try_new(MarketDataInstrumentDefinitionInput {
            instrument_id: execution_definition.instrument_id(),
            reference_evidence: RevisionBoundPayloadEvidence::new(
                MetadataRevision::new(identifier("release-benchmark-reference-v1")?),
                evidence(13),
            ),
            effective_interval: EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?,
            asset_class: execution_definition.asset_class(),
            display_name: None,
            quote_currency: execution_definition.quote_currency(),
            quote_currency_evidence: evidence(14),
            venue_mappings: execution_definition.venue_mappings().to_vec(),
            provider_identities: execution_definition.provider_identities().to_vec(),
            identifiers: Vec::new(),
        })?;
    MarketDataInstrumentSynchronizationCapability::new(Arc::clone(&authority)).synchronize(
        MarketDataInstrumentSynchronization::try_new(vec![definition], 1)?,
        Instant::now() + Duration::from_secs(5),
        cancellation,
    )?;
    Ok((
        directory,
        MarketDataInstrumentReadCapability::new(
            authority,
            Instant::now() + Duration::from_secs(5),
            cancellation,
        )?,
    ))
}

pub(super) fn native_identity() -> Result<ProviderNativeInstrumentIdentity> {
    Ok(ProviderNativeInstrumentIdentity::new(
        SourceId::try_from(IDENTITY_NAMESPACE)?,
        ProviderInstrumentId::try_from(NATIVE_SYMBOL)?,
        VenueSymbol::try_from(NATIVE_SYMBOL)?,
    ))
}

pub(super) fn native_identity_request(at: Timestamp) -> Result<ProviderNativeIdentityRequest> {
    let native = native_identity()?;
    Ok(ProviderNativeIdentityRequest {
        namespace: native.namespace().clone(),
        provider_instrument_id: native.provider_instrument_id().clone(),
        instrument: INSTRUMENT_ID.parse()?,
        venue: VenueId::try_from(VENUE_ID)?,
        venue_symbol: native.venue_symbol().clone(),
        knowledge_at: at,
        effective_at: at,
    })
}

fn provider_identity() -> Result<ProviderIdentityRecord> {
    let native = native_identity()?;
    Ok(ProviderIdentityRecord::new(ProviderIdentityRecordInput {
        instrument_id: INSTRUMENT_ID.parse()?,
        source_id: native.namespace().clone(),
        provider_instrument_id: native.provider_instrument_id().clone(),
        evidence: ProviderIdentityEvidence::from_content_digest(evidence(15).content_digest()),
        source_timestamp: None,
        observed_at: Timestamp::from_unix_nanos(0),
        metadata_revision: MetadataRevision::new(identifier("release-benchmark-identity-v1")?),
        validity: EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?,
        supersedes: None,
    }))
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

#[cfg(test)]
mod tests {
    use super::*;
    use market_squawk_domain::ConnectionGeneration;
    use market_squawk_sources::{AuthoritativeSourceRegistry, RegistryError, SessionId};

    #[test]
    fn benchmark_session_requires_the_catalog_selected_native_identity() -> Result<()> {
        let cancellation = CancellationToken::new();
        let (_directory, reader) = identity_catalog(&cancellation)?;
        let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?
            .with_provider_identity_authority(Arc::new(reader))?;
        let at = super::super::now()?;
        let registered = registry.register(source_metadata()?, at)?;
        let session_id = SessionId::new(identifier("benchmark-catalog-test")?);
        assert!(matches!(
            registry.begin_session(
                &registered,
                session_id.clone(),
                ConnectionGeneration::new(1)?,
                at
            ),
            Err(RegistryError::LiveScopeNotCovered)
        ));
        let mut request = native_identity_request(at)?;
        request.provider_instrument_id = ProviderInstrumentId::try_from("UNSELECTED-USD")?;
        assert!(matches!(
            registry.record_provider_identities(
                &registered,
                &[request],
                Instant::now() + Duration::from_secs(5),
                &cancellation,
            ),
            Err(RegistryError::LiveScopeNotCovered)
        ));
        registry.record_provider_identities(
            &registered,
            &[native_identity_request(at)?],
            Instant::now() + Duration::from_secs(5),
            &cancellation,
        )?;
        let session =
            registry.begin_session(&registered, session_id, ConnectionGeneration::new(1)?, at)?;
        registry.validate_session(&session, super::super::now()?)?;
        Ok(())
    }
}
