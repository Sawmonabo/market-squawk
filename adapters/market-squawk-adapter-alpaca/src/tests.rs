use std::error::Error;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::str::FromStr;
use std::time::Duration;

use market_squawk_domain::{
    AssetClass, AuthorizationBasis, CoverageDelay, DataQuality, DigestAlgorithm, EffectiveInterval,
    EvidenceDigest, ExactPayloadEvidence, InstrumentId, MetadataRevision,
    RevisionBoundPayloadEvidence, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    AuthorizationGrant, AuthorizationMode, BackoffPolicy, BudgetScope, FreshnessPolicy,
    HistoricalCapability, HttpRequestBounds, ProviderBudgetPolicy,
};

use crate::config::ALPACA_PROVIDER;
use crate::{
    ALPACA_BASIC_EQUITY_SYMBOL_LIMIT, ALPACA_HISTORICAL_EXCLUSION_NANOS, AlpacaAdjustment,
    AlpacaCredentials, AlpacaHistoricalEquityConfig, AlpacaHistoricalEquityDataset,
    AlpacaIexDecoder, AlpacaIexLiveConfig, AlpacaInstrumentMapping, AlpacaOptionMapping,
    AlpacaOptionsDecoder, AlpacaOptionsLiveConfig, AlpacaTimeframe, AlpacaTransportLimits,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[test]
fn alpaca_basic_surfaces_keep_limits_protocols_and_quality_separate() -> TestResult {
    let credentials = AlpacaCredentials::try_new(
        "PKTESTALPACA12345678".to_owned(),
        "secret-test-value-that-is-never-logged".to_owned(),
    )?;
    assert!(!format!("{credentials:?}").contains("PKTEST"));

    let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
    let authorization = AuthorizationGrant::new(
        AuthorizationMode::UserAuthorized,
        AuthorizationBasis::new(identifier("alpaca-account-record-test")?),
        evidence(1),
        effective,
    );
    let mapping =
        AlpacaInstrumentMapping::try_new("AAPL".to_owned(), instrument(1)?, AssetClass::Equity)?;
    let limits = AlpacaTransportLimits::try_new(
        1024 * 1024,
        Duration::from_secs(5),
        Duration::from_secs(10),
    )?;
    let iex = AlpacaIexLiveConfig::try_new(
        SourceId::try_from("alpaca-basic-iex-test")?,
        revision("alpaca-basic-iex-test-v1", 2)?,
        authorization.clone(),
        evidence(3),
        effective,
        vec![mapping.clone()],
        freshness()?,
        budget(&authorization)?,
        limits,
    )?;
    assert_eq!(
        iex.metadata().quality_ceiling(),
        DataQuality::DirectUnverified
    );
    assert_eq!(iex.metadata().coverage().delay(), CoverageDelay::RealTime);
    assert!(iex.metadata().coverage().topology().is_partial());
    assert!(!iex.metadata().coverage().topology().is_consolidated());
    assert_eq!(iex.mappings().len(), 1);
    let subscription: serde_json::Value = serde_json::from_str(iex.subscription())?;
    assert_eq!(subscription["trades"][0], "AAPL");
    assert_eq!(subscription["quotes"][0], "AAPL");
    assert_eq!(subscription["statuses"][0], "AAPL");
    let _decoder = AlpacaIexDecoder::try_new(&iex)?;

    let mut excessive = Vec::new();
    for index in 1..=ALPACA_BASIC_EQUITY_SYMBOL_LIMIT + 1 {
        excessive.push(AlpacaInstrumentMapping::try_new(
            format!("S{index}"),
            instrument(u128::try_from(index)?)?,
            AssetClass::Equity,
        )?);
    }
    assert!(
        AlpacaIexLiveConfig::try_new(
            SourceId::try_from("alpaca-basic-iex-excessive")?,
            revision("alpaca-basic-iex-excessive-v1", 4)?,
            authorization.clone(),
            evidence(5),
            effective,
            excessive,
            freshness()?,
            budget(&authorization)?,
            limits,
        )
        .is_err()
    );

    let option_mapping =
        AlpacaOptionMapping::try_new("AAPL260116C00250000".to_owned(), instrument(100)?)?;
    let options = AlpacaOptionsLiveConfig::try_new(
        SourceId::try_from("alpaca-basic-options-test")?,
        revision("alpaca-basic-options-test-v1", 6)?,
        authorization.clone(),
        evidence(7),
        effective,
        vec![option_mapping],
        freshness()?,
        budget(&authorization)?,
        limits,
    )?;
    assert_eq!(
        options.metadata().quality_ceiling(),
        DataQuality::Indicative
    );
    assert_eq!(
        options.metadata().coverage().delay(),
        CoverageDelay::Delayed(ALPACA_HISTORICAL_EXCLUSION_NANOS)
    );
    let option_subscription: serde_json::Value = rmp_serde::from_slice(options.subscription())?;
    assert_eq!(option_subscription["quotes"][0], "AAPL260116C00250000");
    assert_eq!(option_subscription["trades"][0], "AAPL260116C00250000");
    let _decoder = AlpacaOptionsDecoder::try_new(&options)?;
    assert!(AlpacaOptionMapping::try_new("*".to_owned(), instrument(101)?).is_err());

    let historical_dataset = AlpacaHistoricalEquityDataset::try_new(
        identifier("alpaca:iex-bars:AAPL:1Day:2024")?,
        mapping,
        AlpacaTimeframe::day(),
        Timestamp::from_unix_nanos(1_704_067_200_000_000_000),
        Timestamp::from_unix_nanos(1_735_689_599_000_000_000),
        AlpacaAdjustment::All,
        NonZeroU16::new(1_000).ok_or("page limit must be nonzero")?,
    )?;
    let historical = AlpacaHistoricalEquityConfig::try_new(
        SourceId::try_from("alpaca-basic-history-test")?,
        revision("alpaca-basic-history-test-v1", 8)?,
        authorization.clone(),
        evidence(9),
        effective,
        vec![historical_dataset],
        freshness()?,
        budget(&authorization)?,
        HttpRequestBounds::default(),
    )?;
    assert_eq!(
        historical.metadata().quality_ceiling(),
        DataQuality::Aggregated
    );
    assert_eq!(
        historical.metadata().capabilities().historical(),
        HistoricalCapability::Historical
    );
    assert!(!historical.metadata().capabilities().live());
    assert!(historical.metadata().capabilities().extraction());
    Ok(())
}

fn instrument(value: u128) -> TestResult<InstrumentId> {
    let text = format!("{:08x}-{:04x}-{:04x}-{:04x}-{:012x}", 1, 2, 3, 4, value);
    Ok(InstrumentId::from_str(&text)?)
}

fn identifier(value: &str) -> TestResult<SourceIdentifier> {
    Ok(SourceIdentifier::try_from(value)?)
}

fn evidence(byte: u8) -> ExactPayloadEvidence {
    ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        [byte; 32],
    ))
}

fn revision(value: &str, byte: u8) -> TestResult<RevisionBoundPayloadEvidence> {
    Ok(RevisionBoundPayloadEvidence::new(
        MetadataRevision::new(identifier(value)?),
        evidence(byte),
    ))
}

fn freshness() -> TestResult<FreshnessPolicy> {
    Ok(FreshnessPolicy::try_new(
        30_000_000_000,
        5_000_000_000,
        5_000_000_000,
        5_000_000_000,
        1_000_000_000,
    )?)
}

fn budget(authorization: &AuthorizationGrant) -> TestResult<ProviderBudgetPolicy> {
    Ok(ProviderBudgetPolicy::try_new(
        BudgetScope::for_authorization(identifier(ALPACA_PROVIDER)?, authorization)?,
        NonZeroU32::new(200).ok_or("request count must be nonzero")?,
        NonZeroU64::new(60_000_000_000).ok_or("request window must be nonzero")?,
        NonZeroU16::new(2).ok_or("concurrency must be nonzero")?,
        BackoffPolicy::try_new(
            NonZeroU64::new(1_000_000_000).ok_or("initial backoff must be nonzero")?,
            NonZeroU64::new(60_000_000_000).ok_or("maximum backoff must be nonzero")?,
            1_000,
        )?,
    )?)
}
