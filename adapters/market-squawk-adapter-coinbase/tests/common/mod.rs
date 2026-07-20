use std::error::Error;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::str::FromStr;
use std::time::Duration;

use market_squawk_adapter_coinbase::{
    CoinbaseChannel, CoinbaseExchangeConfig, CoinbaseProductMapping, CoinbaseTransportLimits,
};
use market_squawk_domain::{
    AuthorizationBasis, DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence,
    InstrumentId, MetadataRevision, ProviderProduct, RevisionBoundPayloadEvidence, SourceId,
    SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    AuthorizationGrant, AuthorizationMode, BackoffPolicy, BudgetScope, FreshnessPolicy,
    ProviderBudgetPolicy,
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

pub(crate) fn config() -> TestResult<CoinbaseExchangeConfig> {
    config_with_channels(vec![
        CoinbaseChannel::Level2,
        CoinbaseChannel::Matches,
        CoinbaseChannel::Heartbeat,
    ])
}

pub(crate) fn config_with_channels(
    channels: Vec<CoinbaseChannel>,
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
    let mapping = CoinbaseProductMapping::try_new(
        ProviderProduct::new(identifier("BTC-USD")?),
        InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?,
    )?;
    Ok(CoinbaseExchangeConfig::try_new(
        SourceId::try_from("coinbase-exchange-public")?,
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(identifier("exchange-v1-2026-07-20")?),
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
            256 * 1024,
            Duration::from_secs(5),
            Duration::from_secs(5),
        )?,
    )?)
}
