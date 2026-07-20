use std::{collections::BTreeMap, ffi::OsString, time::Duration};

use market_squawk::{AppConfig, ProductionLiveSourceComposition};
use market_squawk_domain::{DataQuality, VenueId};
use market_squawk_live::{DepthLimit, LiveRouteConfig, LiveRouteConfigInput, ShardKey};
use market_squawk_platform::{ConfigOverrides, ConfigSources};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[test]
fn production_contract_is_exactly_allowlisted_typed_and_non_executable() -> TestResult {
    let config = app_config()?;
    let source = config
        .coinbase()
        .ok_or("Coinbase production configuration missing")?;
    let mapping = source
        .instruments()
        .first()
        .ok_or("Coinbase instrument mapping missing")?;
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

    let production = ProductionLiveSourceComposition::try_new(config, vec![route])?;

    assert_eq!(production.endpoint(), "wss://ws-feed.exchange.coinbase.com");
    assert_eq!(
        production.metadata().quality_ceiling(),
        DataQuality::DirectUnverified
    );
    assert_eq!(production.routes().len(), 1);
    assert!(production.metadata().coverage().live().is_some());
    Ok(())
}

fn app_config() -> TestResult<AppConfig> {
    let json = r#"{
      "endpoint":"wss://ws-feed.exchange.coinbase.com",
      "event_classes":["book_snapshot","book_delta","trade"],
      "depth":"price_level",
      "freshness_ms":5000,
      "max_frame_bytes":1048576,
      "subscription_ack_timeout_ms":5000,
      "control_message_capacity":64,
      "control_byte_capacity":65536,
      "authorization":{
        "mode":"public_interface",
        "provider":"coinbase-exchange",
        "basis":"user-reviewed-coinbase-public-interface",
        "evidence_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "evidence_reference":"https://docs.cdp.coinbase.com/exchange/websocket-feed/overview",
        "evidence_version":"reviewed-2026-07-20",
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
        ConfigOverrides::default(),
    ))?)
}
