use std::error::Error;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::str::FromStr;
use std::time::Duration;

use market_squawk_adapter_coinbase::{
    CoinbaseChannel, CoinbaseExchangeConfig, CoinbaseProductMapping, CoinbaseTransportLimits,
};
use market_squawk_domain::{
    AssetClass, AuthorizationBasis, Currency, DigestAlgorithm, EffectiveInterval, EvidenceDigest,
    ExactPayloadEvidence, InstrumentId, MarketDataInstrumentDefinition,
    MarketDataInstrumentDefinitionInput, MetadataRevision, ProviderIdentityEvidence,
    ProviderIdentityKey, ProviderIdentityRecord, ProviderIdentityRecordInput, ProviderInstrumentId,
    ProviderProduct, RevisionBoundPayloadEvidence, SourceId, SourceIdentifier, Timestamp, VenueId,
    VenueMapping, VenueSymbol,
};
use market_squawk_sources::{
    AuthorizationGrant, AuthorizationMode, BackoffPolicy, BudgetScope, FreshnessPolicy,
    ProviderBudgetPolicy, ProviderNativeInstrumentAttestation,
    ProviderNativeInstrumentAttestationInput,
};

pub(crate) type TestResult<T = ()> = Result<T, Box<dyn Error>>;

pub(crate) fn identifier(value: &str) -> TestResult<SourceIdentifier> {
    Ok(SourceIdentifier::try_from(value)?)
}

pub(crate) fn evidence(byte: u8) -> ExactPayloadEvidence {
    ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        [byte; 32],
    ))
}

pub(crate) fn coinbase_product_mapping(source: &str) -> TestResult<CoinbaseProductMapping> {
    let instrument = InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?;
    let source_id = SourceId::try_from(source)?;
    let provider_instrument_id = ProviderInstrumentId::try_from("BTC-USD")?;
    let venue_mapping = VenueMapping::new(
        VenueId::try_from("coinbase-exchange")?,
        VenueSymbol::try_from("BTC-USD")?,
    );
    let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
    let definition =
        MarketDataInstrumentDefinition::try_new(MarketDataInstrumentDefinitionInput {
            instrument_id: instrument,
            reference_evidence: RevisionBoundPayloadEvidence::new(
                MetadataRevision::new(identifier("coinbase-test-reference-v1")?),
                evidence(41),
            ),
            effective_interval: effective,
            asset_class: AssetClass::Crypto,
            display_name: None,
            quote_currency: Currency::try_from("USD")?,
            quote_currency_evidence: evidence(42),
            venue_mappings: vec![venue_mapping.clone()],
            provider_identities: vec![ProviderIdentityRecord::new(ProviderIdentityRecordInput {
                instrument_id: instrument,
                source_id: source_id.clone(),
                provider_instrument_id: provider_instrument_id.clone(),
                evidence: ProviderIdentityEvidence::from_content_digest(
                    evidence(43).content_digest(),
                ),
                source_timestamp: None,
                observed_at: Timestamp::from_unix_nanos(0),
                metadata_revision: MetadataRevision::new(identifier(
                    "coinbase-test-provider-identity-v1",
                )?),
                validity: effective,
                supersedes: None,
            })],
            identifiers: Vec::new(),
        })?;
    let attestation = ProviderNativeInstrumentAttestation::try_select(
        ProviderNativeInstrumentAttestationInput {
            definition: &definition,
            definition_revision_digest: evidence(44).content_digest(),
            definition_published_at: Timestamp::from_unix_nanos(0),
            provider_key: ProviderIdentityKey::new(source_id, provider_instrument_id),
            venue_mapping,
            selected_at: Timestamp::from_unix_nanos(0),
        },
    )?;
    Ok(CoinbaseProductMapping::try_new(
        ProviderProduct::new(identifier("BTC-USD")?),
        attestation,
    )?)
}

pub(crate) fn config() -> TestResult<CoinbaseExchangeConfig> {
    config_with_channels(vec![
        CoinbaseChannel::Level2,
        CoinbaseChannel::MarketTrades,
        CoinbaseChannel::Heartbeats,
    ])
}

pub(crate) fn config_with_channels(
    channels: Vec<CoinbaseChannel>,
) -> TestResult<CoinbaseExchangeConfig> {
    config_with_sources(
        channels,
        "coinbase-exchange-public",
        "coinbase-exchange-public",
    )
}

pub(crate) fn config_with_sources(
    channels: Vec<CoinbaseChannel>,
    mapping_source: &str,
    config_source: &str,
) -> TestResult<CoinbaseExchangeConfig> {
    let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
    let authorization = AuthorizationGrant::new(
        AuthorizationMode::PublicInterface,
        AuthorizationBasis::new(identifier("coinbase-public-interface-v1")?),
        evidence(2),
        effective,
    );
    let provider = identifier("coinbase-exchange")?;
    let budget = ProviderBudgetPolicy::try_new(
        BudgetScope::for_authorization(provider, &authorization)?,
        NonZeroU32::new(8).ok_or("request budget must be nonzero")?,
        NonZeroU64::new(1_000_000_000).ok_or("budget window must be nonzero")?,
        NonZeroU16::new(1).ok_or("budget concurrency must be nonzero")?,
        BackoffPolicy::try_new(
            NonZeroU64::new(1_000_000).ok_or("initial backoff must be nonzero")?,
            NonZeroU64::new(1_000_000_000).ok_or("maximum backoff must be nonzero")?,
            1_000,
        )?,
    )?;
    let mapping = coinbase_product_mapping(mapping_source)?;
    Ok(CoinbaseExchangeConfig::try_new(
        SourceId::try_from(config_source)?,
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(identifier("advanced-trade-v1-2026-08-08")?),
            evidence(3),
        ),
        authorization,
        evidence(4),
        effective,
        vec![mapping],
        channels,
        FreshnessPolicy::try_new(
            5_000_000_000,
            1_000_000_000,
            2_000_000_000,
            1_000_000_000,
            100_000_000,
        )?,
        budget,
        CoinbaseTransportLimits::try_new(
            market_squawk_sources::MAX_RAW_FRAME_BYTES,
            Duration::from_secs(5),
            Duration::from_secs(5),
        )?,
    )?)
}
