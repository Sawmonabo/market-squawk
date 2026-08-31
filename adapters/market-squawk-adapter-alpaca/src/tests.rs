use std::error::Error;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::str::FromStr;
use std::time::Duration;

use market_squawk_domain::{
    AssetClass, AuthorizationBasis, BarTimeSemantics, BarTimestampBasis, CoverageDelay, Currency,
    DataQuality, DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence,
    InstrumentId, MarketBarSessionEvidence, MarketBarSessionKind, MetadataRevision,
    ProviderIdentityEvidence, ProviderIdentityRecord, ProviderIdentityRecordInput,
    ProviderInstrumentId, RevisionBoundPayloadEvidence, SourceId, SourceIdentifier, Timestamp,
    VenueId, VenueMapping, VenueSymbol,
};
use market_squawk_sources::{
    AuthorizationGrant, AuthorizationMode, BackoffPolicy, BudgetScope, FreshnessPolicy,
    HistoricalCapability, HttpRequestBounds, ProviderBudgetPolicy,
};

use crate::config::ALPACA_PROVIDER;
use crate::{
    ALPACA_APPLICATION_MAX_REQUESTS_PER_MINUTE, ALPACA_BASIC_EQUITY_SYMBOL_LIMIT,
    ALPACA_HISTORICAL_EXCLUSION_NANOS, AlpacaAdjustment, AlpacaCredentials,
    AlpacaHistoricalBarTimeAuthority, AlpacaHistoricalBarTimeRequest, AlpacaHistoricalEquityConfig,
    AlpacaHistoricalEquityDatasetPlan, AlpacaHistoricalEquityPreflightPlan,
    AlpacaHistoricalLookback, AlpacaHistoricalSeriesSemantics, AlpacaIexDecoder,
    AlpacaIexLiveConfig, AlpacaInstrumentMapping, AlpacaOptionMapping, AlpacaOptionsDecoder,
    AlpacaOptionsLiveConfig, AlpacaTimeframe, AlpacaTransportLimits,
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
    let mapping_identity = provider_identity("AAPL-ASSET-ID", instrument(1)?, 10, 11)?;
    let mapping = AlpacaInstrumentMapping::try_new(
        &mapping_identity,
        &venue_mapping("iex", "AAPL")?,
        instrument(1)?,
        AssetClass::Equity,
    )?;
    assert_eq!(
        mapping
            .provider_coordinate()
            .identity_key()
            .source_id()
            .as_str(),
        ALPACA_PROVIDER
    );
    assert_eq!(
        mapping
            .provider_coordinate()
            .identity_key()
            .provider_instrument_id()
            .as_str(),
        "AAPL-ASSET-ID"
    );
    assert_eq!(mapping.provider_coordinate().venue().as_str(), "iex");
    assert_eq!(
        mapping.provider_coordinate().venue_symbol().as_str(),
        "AAPL"
    );
    assert_eq!(
        mapping.provider_coordinate().provider_identity_revision(),
        mapping_identity.metadata_revision()
    );
    assert_eq!(
        mapping.provider_coordinate().provider_identity_digest(),
        mapping_identity.evidence().content_digest()
    );
    let mismatched_identity = provider_identity("MSFT-ASSET-ID", instrument(2)?, 12, 13)?;
    assert!(
        AlpacaInstrumentMapping::try_new(
            &mismatched_identity,
            &venue_mapping("iex", "AAPL")?,
            instrument(1)?,
            AssetClass::Equity,
        )
        .is_err()
    );
    assert!(
        AlpacaInstrumentMapping::try_new(
            &mapping_identity,
            &venue_mapping("nasdaq", "AAPL")?,
            instrument(1)?,
            AssetClass::Equity,
        )
        .is_err()
    );
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
        let symbol = format!("S{index}");
        let instrument = instrument(u128::try_from(index)?)?;
        let identity = provider_identity(&format!("ALPACA-ASSET-{index}"), instrument, 14, 15)?;
        excessive.push(AlpacaInstrumentMapping::try_new(
            &identity,
            &venue_mapping("iex", &symbol)?,
            instrument,
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

    let option_identity = provider_identity("AAPL-OPTION-ASSET-ID", instrument(100)?, 16, 17)?;
    let option_mapping = AlpacaOptionMapping::try_new(
        &option_identity,
        &venue_mapping("alpaca-indicative-options", "AAPL260116C00250000")?,
        instrument(100)?,
    )?;
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
    assert!(
        AlpacaOptionMapping::try_new(
            &option_identity,
            &venue_mapping("alpaca-indicative-options", "*")?,
            instrument(101)?,
        )
        .is_err()
    );

    let session = MarketBarSessionEvidence::try_new(
        MarketBarSessionKind::Regular,
        identifier("iex-regular-session-rules-2024")?,
        EvidenceDigest::new(DigestAlgorithm::Sha256, [10; 32]),
    )?;
    let historical_dataset = AlpacaHistoricalEquityDatasetPlan::bind_preflight(
        AlpacaHistoricalEquityPreflightPlan::try_new(
            mapping.clone(),
            AlpacaTimeframe::day(),
            Timestamp::from_unix_nanos(1_735_690_500_000_000_000),
            AlpacaHistoricalLookback::try_from_days(366)?,
            AlpacaAdjustment::All,
        )?,
        AlpacaHistoricalSeriesSemantics::new(BarTimestampBasis::PeriodStart, session.clone()),
    );
    let later_window_same_series = AlpacaHistoricalEquityDatasetPlan::bind_preflight(
        AlpacaHistoricalEquityPreflightPlan::try_new(
            mapping,
            AlpacaTimeframe::day(),
            Timestamp::from_unix_nanos(1_735_776_900_000_000_000),
            AlpacaHistoricalLookback::try_from_days(366)?,
            AlpacaAdjustment::All,
        )?,
        AlpacaHistoricalSeriesSemantics::new(BarTimestampBasis::PeriodStart, session.clone()),
    );
    let historical = AlpacaHistoricalEquityConfig::try_new(
        SourceId::try_from("alpaca-basic-history-test")?,
        revision("alpaca-basic-history-test-v1", 8)?,
        authorization.clone(),
        evidence(9),
        effective,
        vec![historical_dataset, later_window_same_series],
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
    let provider_datasets = historical
        .provider_dataset_identifiers()
        .collect::<Vec<_>>();
    assert_eq!(provider_datasets.len(), 2);
    assert_ne!(provider_datasets[0], provider_datasets[1]);
    for provider_dataset in &provider_datasets {
        assert!(
            provider_dataset
                .as_str()
                .starts_with("alpaca:historical-equity:v1:")
        );
        assert_eq!(provider_dataset.as_str().len(), 92);
        assert!(!provider_dataset.as_str().contains("AAPL"));
    }
    let currency = Currency::try_from("USD")?;
    let first_analytical_dataset = historical
        .dataset(provider_datasets[0])
        .ok_or("registered historical dataset must exist")?
        .analytical_dataset_identifier(historical.metadata(), currency)?;
    let second_analytical_dataset = historical
        .dataset(provider_datasets[1])
        .ok_or("registered historical dataset must exist")?
        .analytical_dataset_identifier(historical.metadata(), currency)?;
    assert_eq!(first_analytical_dataset, second_analytical_dataset);
    assert!(
        first_analytical_dataset
            .as_str()
            .starts_with("alpaca.historical-equity.v1.")
    );
    assert_eq!(first_analytical_dataset.as_str().len(), 92);
    assert!(!first_analytical_dataset.as_str().contains("AAPL"));

    let changed_identity = provider_identity("AAPL-ASSET-ID", instrument(1)?, 18, 19)?;
    let changed_identity_history = AlpacaHistoricalEquityConfig::try_new(
        SourceId::try_from("alpaca-basic-history-test")?,
        revision("alpaca-basic-history-test-v1", 8)?,
        authorization.clone(),
        evidence(9),
        effective,
        vec![AlpacaHistoricalEquityDatasetPlan::bind_preflight(
            AlpacaHistoricalEquityPreflightPlan::try_new(
                AlpacaInstrumentMapping::try_new(
                    &changed_identity,
                    &venue_mapping("iex", "AAPL")?,
                    instrument(1)?,
                    AssetClass::Equity,
                )?,
                AlpacaTimeframe::day(),
                Timestamp::from_unix_nanos(1_735_690_500_000_000_000),
                AlpacaHistoricalLookback::try_from_days(366)?,
                AlpacaAdjustment::All,
            )?,
            AlpacaHistoricalSeriesSemantics::new(BarTimestampBasis::PeriodStart, session.clone()),
        )],
        freshness()?,
        budget(&authorization)?,
        HttpRequestBounds::default(),
    )?;
    let changed_identity_provider_dataset = changed_identity_history
        .provider_dataset_identifiers()
        .next()
        .ok_or("changed identity dataset must exist")?;
    assert!(
        !provider_datasets.contains(&changed_identity_provider_dataset),
        "provider identity revision and digest must change the provider dataset identity"
    );
    let changed_identity_analytical_dataset = changed_identity_history
        .dataset(changed_identity_provider_dataset)
        .ok_or("changed identity dataset must remain registered")?
        .analytical_dataset_identifier(changed_identity_history.metadata(), currency)?;
    assert_ne!(
        first_analytical_dataset, changed_identity_analytical_dataset,
        "provider identity revision and digest must change the analytical dataset identity"
    );

    let changed_calendar_session = MarketBarSessionEvidence::try_new(
        MarketBarSessionKind::Regular,
        identifier("iex-regular-session-rules-2024")?,
        EvidenceDigest::new(DigestAlgorithm::Sha256, [11; 32]),
    )?;
    let changed_calendar_history = AlpacaHistoricalEquityConfig::try_new(
        SourceId::try_from("alpaca-basic-history-test")?,
        revision("alpaca-basic-history-test-v1", 8)?,
        authorization.clone(),
        evidence(9),
        effective,
        vec![AlpacaHistoricalEquityDatasetPlan::bind_preflight(
            AlpacaHistoricalEquityPreflightPlan::try_new(
                AlpacaInstrumentMapping::try_new(
                    &mapping_identity,
                    &venue_mapping("iex", "AAPL")?,
                    instrument(1)?,
                    AssetClass::Equity,
                )?,
                AlpacaTimeframe::day(),
                Timestamp::from_unix_nanos(1_735_690_500_000_000_000),
                AlpacaHistoricalLookback::try_from_days(366)?,
                AlpacaAdjustment::All,
            )?,
            AlpacaHistoricalSeriesSemantics::new(
                BarTimestampBasis::PeriodStart,
                changed_calendar_session,
            ),
        )],
        freshness()?,
        budget(&authorization)?,
        HttpRequestBounds::default(),
    )?;
    let changed_provider_dataset = changed_calendar_history
        .provider_dataset_identifiers()
        .next()
        .ok_or("changed calendar dataset must exist")?;
    assert!(
        !provider_datasets.contains(&changed_provider_dataset),
        "exact calendar evidence must change the provider dataset identity"
    );
    let changed_analytical_dataset = changed_calendar_history
        .dataset(changed_provider_dataset)
        .ok_or("changed calendar dataset must remain registered")?
        .analytical_dataset_identifier(changed_calendar_history.metadata(), currency)?;
    assert_ne!(first_analytical_dataset, changed_analytical_dataset);

    let provider_timestamp = Timestamp::from_unix_nanos(1_704_067_200_000_000_000);
    let semantics = BarTimeSemantics::try_new(
        provider_timestamp,
        Timestamp::from_unix_nanos(1_704_153_600_000_000_000),
        BarTimestampBasis::PeriodStart,
        session,
    )?;
    let request = AlpacaHistoricalBarTimeRequest::new(
        instrument(1)?,
        VenueId::try_from("iex")?,
        ProviderInstrumentId::try_from("AAPL")?,
        identifier("1Day")?,
        provider_timestamp,
    );
    let time_authority = DeterministicBarTimeAuthority {
        expected: request.clone(),
        semantics: semantics.clone(),
    };
    time_authority.validate_current()?;
    assert_eq!(time_authority.resolve(&request)?, semantics);
    Ok(())
}

struct DeterministicBarTimeAuthority {
    expected: AlpacaHistoricalBarTimeRequest,
    semantics: BarTimeSemantics,
}

impl AlpacaHistoricalBarTimeAuthority for DeterministicBarTimeAuthority {
    fn validate_current(&self) -> Result<(), crate::AlpacaError> {
        Ok(())
    }

    fn resolve(
        &self,
        request: &AlpacaHistoricalBarTimeRequest,
    ) -> Result<BarTimeSemantics, crate::AlpacaError> {
        if request != &self.expected {
            return Err(crate::AlpacaError::Protocol);
        }
        Ok(self.semantics.clone())
    }
}

fn instrument(value: u128) -> TestResult<InstrumentId> {
    let text = format!("{:08x}-{:04x}-{:04x}-{:04x}-{:012x}", 1, 2, 3, 4, value);
    Ok(InstrumentId::from_str(&text)?)
}

fn provider_identity(
    symbol: &str,
    instrument_id: InstrumentId,
    revision_byte: u8,
    evidence_byte: u8,
) -> TestResult<ProviderIdentityRecord> {
    Ok(ProviderIdentityRecord::new(ProviderIdentityRecordInput {
        instrument_id,
        source_id: SourceId::try_from(ALPACA_PROVIDER)?,
        provider_instrument_id: ProviderInstrumentId::try_from(symbol)?,
        evidence: ProviderIdentityEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            [evidence_byte; 32],
        )),
        source_timestamp: None,
        observed_at: Timestamp::from_unix_nanos(0),
        metadata_revision: MetadataRevision::new(identifier(&format!(
            "alpaca-provider-identity-{revision_byte}"
        ))?),
        validity: EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?,
        supersedes: None,
    }))
}

fn venue_mapping(venue: &str, symbol: &str) -> TestResult<VenueMapping> {
    Ok(VenueMapping::new(
        VenueId::try_from(venue)?,
        VenueSymbol::try_from(symbol)?,
    ))
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
        NonZeroU32::new(ALPACA_APPLICATION_MAX_REQUESTS_PER_MINUTE)
            .ok_or("request count must be nonzero")?,
        NonZeroU64::new(60_000_000_000).ok_or("request window must be nonzero")?,
        NonZeroU16::new(2).ok_or("concurrency must be nonzero")?,
        BackoffPolicy::try_new(
            NonZeroU64::new(1_000_000_000).ok_or("initial backoff must be nonzero")?,
            NonZeroU64::new(60_000_000_000).ok_or("maximum backoff must be nonzero")?,
            1_000,
        )?,
    )?)
}
