use std::{
    collections::BTreeMap,
    ffi::OsString,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use market_squawk::{
    AppConfig, ProductionLiveSourceComposition, paper_bot::local_coinbase_paper_bot,
};
use market_squawk_data::{
    CatalogAuthority, CatalogConfig, CatalogLimit, CatalogResultLimits,
    MarketDataInstrumentReadCapability, MarketDataInstrumentRecord,
    MarketDataInstrumentSynchronization, MarketDataInstrumentSynchronizationCapability,
};
use market_squawk_domain::{
    AssetClass, DataQuality, DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence,
    MarketDataInstrumentDefinition, MarketDataInstrumentDefinitionInput, MetadataRevision,
    ProviderIdentityEvidence, ProviderIdentityRecord, ProviderIdentityRecordInput,
    ProviderInstrumentId, RevisionBoundPayloadEvidence, SourceId, SourceIdentifier, VenueId,
};
use market_squawk_execution::MAX_PAPER_FEE_BASIS_POINTS;
use market_squawk_live::{DepthLimit, LiveRouteConfig, LiveRouteConfigInput, ShardKey};
use market_squawk_platform::{CoinbaseSourceConfig, ConfigOverrides, ConfigSources, LocalPaths};
use rust_decimal::Decimal;
use tokio_util::sync::CancellationToken;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[test]
fn production_contract_is_exactly_allowlisted_typed_and_non_executable() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let config = app_config_with_overrides(ConfigOverrides {
        data_dir: Some(temporary.path().join("data")),
        ..ConfigOverrides::default()
    })?;
    let source = config
        .coinbase()
        .ok_or("Coinbase production configuration missing")?;
    let mapping = source
        .instruments()
        .first()
        .ok_or("Coinbase instrument mapping missing")?;
    let market_data_record = coinbase_market_data_record(source)?;
    let route = LiveRouteConfig::try_new(LiveRouteConfigInput {
        route: ShardKey::new(
            VenueId::try_from("coinbase-exchange")?,
            mapping.definition().instrument_id(),
        ),
        definition: mapping.definition().clone(),
        depth: DepthLimit::new(32)?,
        nonce_capacity: 32,
        nonce_reclaim_budget: 4,
        maximum_capability_lifetime: Duration::from_secs(1),
    })?;

    let production = ProductionLiveSourceComposition::try_new(
        config,
        vec![route],
        std::slice::from_ref(&market_data_record),
    )?;

    assert_eq!(
        production.endpoint(),
        "wss://advanced-trade-ws.coinbase.com"
    );
    assert_eq!(
        production.metadata().quality_ceiling(),
        DataQuality::DirectUnverified
    );
    assert_eq!(production.routes().len(), 1);
    assert!(production.metadata().coverage().live().is_some());
    Ok(())
}

#[test]
fn controlled_local_paper_service_composes_without_network_access() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let maximum_fee = u32::try_from(MAX_PAPER_FEE_BASIS_POINTS)?;
    let config = app_config_with_overrides(ConfigOverrides {
        data_dir: Some(temporary.path().join("data")),
        ..ConfigOverrides::default()
    })?;
    let market_data_record =
        coinbase_market_data_record(config.coinbase().ok_or("Coinbase configuration missing")?)?;
    let composition = local_coinbase_paper_bot(
        config,
        std::slice::from_ref(&market_data_record),
        Decimal::new(100_000, 0),
        maximum_fee,
    )?;
    drop(composition);

    let excessive_config = app_config_with_overrides(ConfigOverrides {
        data_dir: Some(temporary.path().join("excessive-fee-data")),
        ..ConfigOverrides::default()
    })?;
    let excessive_record = coinbase_market_data_record(
        excessive_config
            .coinbase()
            .ok_or("Coinbase configuration missing")?,
    )?;
    let excessive = local_coinbase_paper_bot(
        excessive_config,
        std::slice::from_ref(&excessive_record),
        Decimal::new(100_000, 0),
        maximum_fee
            .checked_add(1)
            .ok_or("paper fee fixture overflow")?,
    );
    assert!(excessive.is_err());
    Ok(())
}

fn coinbase_market_data_record(
    source: &CoinbaseSourceConfig,
) -> TestResult<MarketDataInstrumentRecord> {
    let configured = source
        .instruments()
        .first()
        .ok_or("Coinbase instrument mapping missing")?;
    let execution = configured.definition();
    let digest = |byte| EvidenceDigest::new(DigestAlgorithm::Sha256, [byte; 32]);
    let provider_instrument = ProviderInstrumentId::try_from(configured.product())?;
    let effective = source.authorization().effective_interval();
    let definition =
        MarketDataInstrumentDefinition::try_new(MarketDataInstrumentDefinitionInput {
            instrument_id: execution.instrument_id(),
            reference_evidence: RevisionBoundPayloadEvidence::new(
                MetadataRevision::new(SourceIdentifier::try_from("coinbase-journey-reference-v1")?),
                ExactPayloadEvidence::from_content_digest(digest(61)),
            ),
            effective_interval: effective,
            asset_class: AssetClass::Crypto,
            display_name: None,
            quote_currency: execution.quote_currency(),
            quote_currency_evidence: ExactPayloadEvidence::from_content_digest(digest(62)),
            venue_mappings: execution.venue_mappings().to_vec(),
            provider_identities: vec![ProviderIdentityRecord::new(ProviderIdentityRecordInput {
                instrument_id: execution.instrument_id(),
                source_id: SourceId::try_from("coinbase-exchange-public")?,
                provider_instrument_id: provider_instrument,
                evidence: ProviderIdentityEvidence::from_content_digest(digest(63)),
                source_timestamp: None,
                observed_at: effective.starts_at(),
                metadata_revision: MetadataRevision::new(SourceIdentifier::try_from(
                    "coinbase-journey-public-identity-v1",
                )?),
                validity: effective,
                supersedes: None,
            })],
            identifiers: Vec::new(),
        })?;
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("identity"))?;
    let catalog = CatalogConfig::try_new(
        paths.catalog()?.clone(),
        Duration::from_millis(750),
        CatalogLimit::new(32)?,
        CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
    )?;
    let authority = Arc::new(Mutex::new(CatalogAuthority::open(catalog)?));
    let writer = MarketDataInstrumentSynchronizationCapability::new(Arc::clone(&authority));
    let reader = MarketDataInstrumentReadCapability::new(authority);
    let deadline = Instant::now() + Duration::from_secs(2);
    let cancellation = CancellationToken::new();
    writer.synchronize(
        MarketDataInstrumentSynchronization::try_new(vec![definition], 1)?,
        deadline,
        &cancellation,
    )?;
    reader
        .latest(execution.instrument_id(), deadline, &cancellation)?
        .ok_or_else(|| "Coinbase durable identity record missing".into())
}

fn app_config_with_overrides(overrides: ConfigOverrides) -> TestResult<AppConfig> {
    let json = r#"{
      "endpoint":"wss://advanced-trade-ws.coinbase.com",
      "event_classes":["book_snapshot","book_delta","trade"],
      "depth":"price_level",
      "freshness_ms":5000,
      "max_frame_bytes":16777216,
      "subscription_ack_timeout_ms":5000,
      "control_message_capacity":64,
      "control_byte_capacity":65536,
      "authorization":{
        "mode":"public_interface",
        "provider":"coinbase-exchange",
        "basis":"user-reviewed-coinbase-public-interface",
        "evidence_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "evidence_reference":"https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-overview",
        "evidence_version":"reviewed-2026-08-08",
        "effective_from_unix_nanos":1700000000000000000,
        "effective_until_unix_nanos":1900000000000000000
      },
      "instruments":[{
        "product":"BTC-USD",
        "instrument_id":"4c74ab95-53b9-42ad-9b66-0ed403b88fed",
        "definition_revision":1,
        "asset_class":"crypto",
        "primary_asset":"b9f6d14f-9140-4ca3-a412-9bd59b3b5e67",
        "quote_currency":"USD",
        "tick_size":"0.01",
        "lot_size":"0.00000001",
        "contract_multiplier":"1",
        "venue":"coinbase-exchange",
        "trading_status":"active"
      }]
    }"#;
    let environment = BTreeMap::from([(
        OsString::from("MARKET_SQUAWK_COINBASE_JSON"),
        OsString::from(json),
    )]);
    Ok(AppConfig::load(ConfigSources::new(
        None,
        &environment,
        overrides,
    ))?)
}
