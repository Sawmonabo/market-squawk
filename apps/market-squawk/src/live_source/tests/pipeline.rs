use std::{collections::BTreeMap, ffi::OsString};

use market_squawk_domain::{DataQuality, Timestamp};
use market_squawk_platform::{AppConfig, ConfigOverrides, ConfigSources};
use market_squawk_sources::{InstrumentCoverageMembership, SourceMetadataProvider};

use super::super::{
    composition::{ProductionCoinbaseProfile, ProductionCoinbaseProfileError},
    instruments::ProductionInstrumentSet,
};

#[test]
fn validated_instruments_flow_to_adapter_mappings_without_identity_regeneration()
-> Result<(), Box<dyn std::error::Error>> {
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
    let parsed: serde_json::Value = serde_json::from_str(json)?;
    let mut missing = parsed.clone();
    missing
        .as_object_mut()
        .ok_or("Coinbase fixture is not an object")?
        .remove("authorization");
    let mut mismatched = parsed.clone();
    mismatched
        .get_mut("authorization")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or("Coinbase authorization fixture is not an object")?
        .insert(
            "provider".to_owned(),
            serde_json::Value::String("kraken".to_owned()),
        );
    for invalid in [missing, mismatched] {
        let invalid_json = serde_json::to_string(&invalid)?;
        let invalid_environment = BTreeMap::from([(
            OsString::from("MARKET_SQUAWK_COINBASE_JSON"),
            OsString::from(invalid_json),
        )]);
        assert!(
            AppConfig::load(ConfigSources::new(
                None,
                &invalid_environment,
                ConfigOverrides::default(),
            ))
            .is_err()
        );
    }

    let environment = BTreeMap::from([(
        OsString::from("MARKET_SQUAWK_COINBASE_JSON"),
        OsString::from(json),
    )]);
    let config = AppConfig::load(ConfigSources::new(
        None,
        &environment,
        ConfigOverrides::default(),
    ))?;
    let source = config.coinbase().ok_or("Coinbase source profile missing")?;

    let instruments = ProductionInstrumentSet::try_from(source)?;
    let definition = source
        .instruments()
        .first()
        .ok_or("canonical instrument definition missing")?
        .definition();
    let mapping = instruments
        .adapter_mappings()
        .first()
        .ok_or("adapter product mapping missing")?;

    assert_eq!(definition.instrument_id(), mapping.instrument());
    assert_eq!(mapping.product().as_source_identifier().as_str(), "BTC-USD");

    assert!(matches!(
        ProductionCoinbaseProfile::try_from_at(
            source,
            Timestamp::from_unix_nanos(1_900_000_000_000_000_000),
        ),
        Err(ProductionCoinbaseProfileError::AuthorizationNotEffective)
    ));
    let production = ProductionCoinbaseProfile::try_from_at(
        source,
        Timestamp::from_unix_nanos(1_800_000_000_000_000_000),
    )?;
    assert_eq!(
        production.decoder().metadata().quality_ceiling(),
        DataQuality::DirectUnverified
    );
    assert_eq!(production.metadata(), production.decoder().metadata());
    let reproduced = ProductionCoinbaseProfile::try_from_at(
        source,
        Timestamp::from_unix_nanos(1_800_000_000_000_000_000),
    )?;
    assert_eq!(
        production.metadata().revision_evidence(),
        reproduced.metadata().revision_evidence()
    );
    assert!(
        production
            .metadata()
            .revision()
            .as_source_identifier()
            .as_str()
            .starts_with("coinbase-v1-")
    );

    let mut changed = parsed;
    changed
        .as_object_mut()
        .ok_or("Coinbase fixture is not an object")?
        .insert("freshness_ms".to_owned(), serde_json::Value::from(4_000));
    let changed_environment = BTreeMap::from([(
        OsString::from("MARKET_SQUAWK_COINBASE_JSON"),
        OsString::from(serde_json::to_string(&changed)?),
    )]);
    let changed_config = AppConfig::load(ConfigSources::new(
        None,
        &changed_environment,
        ConfigOverrides::default(),
    ))?;
    let changed_source = changed_config
        .coinbase()
        .ok_or("changed Coinbase source profile missing")?;
    let changed_profile = ProductionCoinbaseProfile::try_from_at(
        changed_source,
        Timestamp::from_unix_nanos(1_800_000_000_000_000_000),
    )?;
    assert_ne!(
        production.metadata().revision_evidence(),
        changed_profile.metadata().revision_evidence()
    );
    assert_eq!(
        production
            .metadata()
            .coverage()
            .instruments()
            .membership(mapping.instrument()),
        InstrumentCoverageMembership::Enumerated
    );
    Ok(())
}
