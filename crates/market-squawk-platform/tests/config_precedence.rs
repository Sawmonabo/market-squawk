use std::{collections::BTreeMap, ffi::OsString, path::PathBuf};

use market_squawk_domain::{AssetClass, Denomination, LiveEventClass, MarketDepth, TradingStatus};
use market_squawk_platform::{
    AppConfig, COINBASE_EXCHANGE_ENDPOINT, ConfigError, ConfigOrigin, ConfigOverrides,
    ConfigSetting, ConfigSources, KRAKEN_WEBSOCKET_V2_ENDPOINT, SecretReference,
};
use tempfile::tempdir;

fn environment(values: &[(&str, &str)]) -> BTreeMap<OsString, OsString> {
    values
        .iter()
        .map(|(key, value)| (OsString::from(key), OsString::from(value)))
        .collect()
}

fn coinbase_config_toml(max_frame_bytes: usize) -> String {
    format!(
        r#"
[coinbase]
endpoint = "{COINBASE_EXCHANGE_ENDPOINT}"
event_classes = ["book_snapshot", "book_delta", "trade"]
depth = "price_level"
freshness_ms = 5000
max_frame_bytes = {max_frame_bytes}
subscription_ack_timeout_ms = 5000
control_message_capacity = 64
control_byte_capacity = 65536

[coinbase.authorization]
mode = "public_interface"
provider = "coinbase-exchange"
basis = "user-reviewed-coinbase-public-interface"
evidence_sha256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
evidence_reference = "https://docs.cdp.coinbase.com/exchange/websocket-feed/overview"
evidence_version = "reviewed-2026-07-20"
effective_from_unix_nanos = 1700000000000000000
effective_until_unix_nanos = 1900000000000000000

[[coinbase.instruments]]
product = "BTC-USD"
instrument_id = "4c74ab95-53b9-42ad-9b66-0ed403b88fed"
definition_revision = 1
asset_class = "crypto"
primary_asset = "b9f6d14f-9140-4ca3-a412-9bd59b3b5e67"
quote_currency = "USD"
tick_size = "0.01"
lot_size = "0.00000001"
contract_multiplier = "1"
venue = "coinbase-exchange"
trading_status = "active"
"#
    )
}

fn coinbase_config_json(max_frame_bytes: usize) -> String {
    format!(
        r#"{{
  "endpoint":"{COINBASE_EXCHANGE_ENDPOINT}",
  "event_classes":["book_snapshot","book_delta","trade"],
  "depth":"price_level",
  "freshness_ms":5000,
  "max_frame_bytes":{max_frame_bytes},
  "subscription_ack_timeout_ms":5000,
  "control_message_capacity":64,
  "control_byte_capacity":65536,
  "authorization":{{
    "mode":"public_interface",
    "provider":"coinbase-exchange",
    "basis":"user-reviewed-coinbase-public-interface",
    "evidence_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "evidence_reference":"https://docs.cdp.coinbase.com/exchange/websocket-feed/overview",
    "evidence_version":"reviewed-2026-07-20",
    "effective_from_unix_nanos":1700000000000000000,
    "effective_until_unix_nanos":1900000000000000000
  }},
  "instruments":[{{
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
  }}]
}}"#
    )
}

fn kraken_config_json(endpoint: &str) -> String {
    format!(
        r#"{{
  "endpoint":"{endpoint}",
  "channel":"book",
  "depth":10,
  "freshness_ms":5000,
  "max_frame_bytes":1048576,
  "subscription_ack_timeout_ms":5000,
  "control_message_capacity":64,
  "control_byte_capacity":65536,
  "authorization":{{
    "mode":"public_interface",
    "provider":"kraken",
    "basis":"user-reviewed-kraken-public-interface",
    "evidence_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    "evidence_reference":"https://docs.kraken.com/api/docs/websocket-v2/book/",
    "evidence_version":"reviewed-2026-07-21",
    "effective_from_unix_nanos":1700000000000000000,
    "effective_until_unix_nanos":1900000000000000000
  }},
  "instrument":{{
    "symbol":"BTC/USD",
    "instrument_id":"4c74ab95-53b9-42ad-9b66-0ed403b88fed",
    "definition_revision":1,
    "asset_class":"crypto",
    "primary_asset":"b9f6d14f-9140-4ca3-a412-9bd59b3b5e67",
    "quote_currency":"USD",
    "tick_size":"0.1",
    "lot_size":"0.00000001",
    "contract_multiplier":"1",
    "venue":"kraken",
    "trading_status":"active"
  }}
}}"#
    )
}

#[test]
fn production_kraken_config_is_explicit_typed_and_endpoint_sealed()
-> Result<(), Box<dyn std::error::Error>> {
    let json = kraken_config_json(KRAKEN_WEBSOCKET_V2_ENDPOINT);
    let kraken_environment = environment(&[("MARKET_SQUAWK_KRAKEN_JSON", &json)]);
    let config = AppConfig::load(ConfigSources::new(
        None,
        &kraken_environment,
        ConfigOverrides::default(),
    ))?;
    let kraken = config.kraken().ok_or("Kraken configuration missing")?;
    assert_eq!(kraken.endpoint(), KRAKEN_WEBSOCKET_V2_ENDPOINT);
    assert_eq!(kraken.symbol(), "BTC/USD");
    assert_eq!(kraken.depth(), 10);
    assert_eq!(
        kraken.definition().venue_mappings()[0].venue_id().as_str(),
        "kraken"
    );

    let local = kraken_config_json("ws://127.0.0.1:9000");
    let local_environment = environment(&[("MARKET_SQUAWK_KRAKEN_JSON", &local)]);
    assert!(
        AppConfig::load(ConfigSources::new(
            None,
            &local_environment,
            ConfigOverrides::default(),
        ))
        .is_err()
    );
    Ok(())
}

#[test]
fn production_coinbase_config_is_explicit_typed_and_obeys_precedence()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("coinbase.toml");
    std::fs::write(&path, coinbase_config_toml(1_048_576))?;
    let environment_json = coinbase_config_json(2_097_152);
    let environment_layer = environment(&[("MARKET_SQUAWK_COINBASE_JSON", &environment_json)]);
    let cli_json = coinbase_config_json(3_145_728);
    let cli_environment = environment(&[("MARKET_SQUAWK_COINBASE_JSON", &cli_json)]);
    let cli_config = AppConfig::load(ConfigSources::new(
        None,
        &cli_environment,
        ConfigOverrides::default(),
    ))?
    .coinbase()
    .cloned()
    .ok_or("typed Coinbase CLI fixture was not loaded")?;

    let config = AppConfig::load(ConfigSources::new(
        Some(&path),
        &environment_layer,
        ConfigOverrides {
            coinbase: Some(cli_config),
            ..ConfigOverrides::default()
        },
    ))?;
    let coinbase = config.coinbase().ok_or("Coinbase configuration missing")?;

    assert_eq!(coinbase.endpoint(), COINBASE_EXCHANGE_ENDPOINT);
    assert_eq!(coinbase.max_frame_bytes().get(), 3_145_728);
    assert_eq!(coinbase.depth(), MarketDepth::PriceLevel);
    assert_eq!(
        coinbase.event_classes(),
        [
            LiveEventClass::BookSnapshot,
            LiveEventClass::BookDelta,
            LiveEventClass::Trade,
        ]
    );
    assert_eq!(coinbase.freshness().as_millis(), 5_000);
    assert!(coinbase.subscription_bytes().get() <= 16 * 1024);
    let mapping = coinbase
        .instruments()
        .first()
        .ok_or("validated Coinbase mapping missing")?;
    assert_eq!(mapping.product(), "BTC-USD");
    assert_eq!(mapping.definition().asset_class(), AssetClass::Crypto);
    assert!(matches!(
        mapping.definition().primary_denomination(),
        Denomination::Asset(_)
    ));
    assert_eq!(mapping.definition().quote_currency().as_str(), "USD");
    assert_eq!(
        mapping.definition().venue_mappings()[0].venue_id().as_str(),
        "coinbase-exchange"
    );
    assert_eq!(mapping.definition().trading_status(), TradingStatus::Active);
    Ok(())
}

#[test]
fn production_coinbase_config_fails_closed_on_ambiguous_or_unsafe_mappings()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let invalid_cases = [
        coinbase_config_toml(1_048_576)
            .split("[[coinbase.instruments]]")
            .next()
            .ok_or("Coinbase fixture lost its source section")?
            .to_owned(),
        coinbase_config_toml(1_048_576).replace("coinbase-exchange\"", "kraken\""),
        coinbase_config_toml(1_048_576).replace("tick_size = \"0.01\"", "tick_size = \"0\""),
        format!(
            "{}\n{}",
            coinbase_config_toml(1_048_576),
            coinbase_config_toml(1_048_576)
                .split("[[coinbase.instruments]]")
                .nth(1)
                .map(|mapping| format!("[[coinbase.instruments]]{mapping}"))
                .ok_or("Coinbase fixture lost its mapping")?
        ),
    ];

    for (index, invalid) in invalid_cases.into_iter().enumerate() {
        let path = directory.path().join(format!("invalid-{index}.toml"));
        std::fs::write(&path, invalid)?;
        assert!(matches!(
            AppConfig::load(ConfigSources::new(
                Some(&path),
                &BTreeMap::new(),
                ConfigOverrides::default(),
            )),
            Err(ConfigError::FileParse)
        ));
    }
    Ok(())
}

#[test]
fn production_coinbase_config_rejects_custom_endpoints_and_subscription_overflow()
-> Result<(), Box<dyn std::error::Error>> {
    let custom_endpoint =
        coinbase_config_json(1_048_576).replace(COINBASE_EXCHANGE_ENDPOINT, "ws://127.0.0.1:9000");
    let custom_environment = environment(&[("MARKET_SQUAWK_COINBASE_JSON", &custom_endpoint)]);
    assert!(
        AppConfig::load(ConfigSources::new(
            None,
            &custom_environment,
            ConfigOverrides::default(),
        ))
        .is_err()
    );

    let mut oversized = coinbase_config_json(1_048_576);
    let mapping = oversized
        .find("\"instruments\":[")
        .map(|start| start + "\"instruments\":[".len())
        .and_then(|start| {
            oversized[start..]
                .find('}')
                .map(|end| (start, start + end + 1))
        });
    let (start, end) = mapping.ok_or("test fixture lost its instrument mapping")?;
    let template = oversized[start..end].to_owned();
    let mappings = (0..101)
        .map(|index| {
            template
                .replace("BTC-USD", &format!("P{index}-USD"))
                .replace(
                    "4c74ab95-53b9-42ad-9b66-0ed403b88fed",
                    &format!("00000000-0000-4000-8000-{index:012}"),
                )
        })
        .collect::<Vec<_>>()
        .join(",");
    oversized.replace_range(start..end, &mappings);
    let oversized_environment = environment(&[("MARKET_SQUAWK_COINBASE_JSON", &oversized)]);
    assert!(
        AppConfig::load(ConfigSources::new(
            None,
            &oversized_environment,
            ConfigOverrides::default(),
        ))
        .is_err()
    );
    Ok(())
}

#[test]
fn typed_coinbase_config_does_not_enable_a_hidden_production_default() -> Result<(), ConfigError> {
    let config = AppConfig::load(ConfigSources::new(
        None,
        &BTreeMap::new(),
        ConfigOverrides::default(),
    ))?;
    assert!(config.coinbase().is_none());
    Ok(())
}

#[test]
fn precedence_is_defaults_then_file_then_supplied_environment_then_cli()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let config_file = directory.path().join("market-squawk.toml");
    std::fs::write(
        &config_file,
        r#"
data_dir = "from-file"
products = ["FILE-USD"]
stale_after_ms = 4000
capture_queue_capacity = 8
capture_memory_ceiling_bytes = 67108864
capture_destination_registry_memory_ceiling_bytes = 2097152
paper_bot_enabled = false
capture_flush_interval_ms = 500
capture_shutdown_ms = 2000
source_shutdown_ms = 3000
source_secret = "keyring:coinbase"
"#,
    )?;
    let environment = environment(&[
        ("MARKET_SQUAWK_DATA_DIR", "from-env"),
        ("MARKET_SQUAWK_PRODUCTS", "ENV-USD,ENV-EUR"),
        ("MARKET_SQUAWK_CAPTURE_QUEUE_CAPACITY", "16"),
        ("MARKET_SQUAWK_CAPTURE_MEMORY_CEILING_BYTES", "68157440"),
        (
            "MARKET_SQUAWK_CAPTURE_DESTINATION_REGISTRY_MEMORY_CEILING_BYTES",
            "3145728",
        ),
        ("MARKET_SQUAWK_PAPER_BOT_ENABLED", "true"),
        ("MARKET_SQUAWK_SOURCE_SHUTDOWN_MS", "4000"),
    ]);
    let cli = ConfigOverrides {
        data_dir: Some(PathBuf::from("from-cli")),
        products: Some(vec!["CLI-USD".to_owned()]),
        capture_queue_capacity: Some(32),
        capture_memory_ceiling_bytes: Some(69_206_016),
        capture_destination_registry_memory_ceiling_bytes: Some(4_194_304),
        source_shutdown_ms: Some(6_000),
        ..ConfigOverrides::default()
    };

    let config = AppConfig::load(ConfigSources::new(Some(&config_file), &environment, cli))?;

    assert_eq!(config.data_dir(), PathBuf::from("from-cli"));
    assert_eq!(config.products(), ["CLI-USD"]);
    assert_eq!(config.capture_queue_capacity().get(), 32);
    assert_eq!(config.capture_memory_ceiling_bytes().get(), 69_206_016);
    assert_eq!(
        config
            .capture_destination_registry_memory_ceiling_bytes()
            .get(),
        4_194_304
    );
    assert_eq!(config.stale_after().as_millis(), 4_000);
    assert!(config.paper_bot_enabled());
    assert_eq!(config.source_shutdown().as_millis(), 6_000);
    assert_eq!(
        config.source_secret(),
        Some(&SecretReference::try_from("keyring:coinbase")?)
    );
    assert_eq!(
        config.provenance().origin(ConfigSetting::DataDirectory),
        ConfigOrigin::Cli
    );
    assert_eq!(
        config.provenance().origin(ConfigSetting::PaperBotEnabled),
        ConfigOrigin::Environment
    );
    assert_eq!(
        config.provenance().origin(ConfigSetting::StaleAfter),
        ConfigOrigin::LocalFile
    );
    assert_eq!(
        config.provenance().origin(ConfigSetting::Coinbase),
        ConfigOrigin::SafeDefault
    );
    Ok(())
}

#[test]
fn source_shutdown_has_an_independent_safe_default() -> Result<(), Box<dyn std::error::Error>> {
    let config = AppConfig::load(ConfigSources::new(
        None,
        &BTreeMap::new(),
        ConfigOverrides::default(),
    ))?;

    assert_eq!(config.source_shutdown().as_millis(), 5_000);
    assert_eq!(config.capture_queue_capacity().get(), 16_384);
    assert_eq!(
        config.capture_memory_ceiling_bytes().get(),
        64 * 1024 * 1024
    );
    assert_eq!(
        config
            .capture_destination_registry_memory_ceiling_bytes()
            .get(),
        1024 * 1024
    );
    Ok(())
}

#[test]
fn source_shutdown_accepts_exact_boundaries_and_rejects_zero_or_excess() {
    for accepted in [1, 60_000] {
        let config = AppConfig::load(ConfigSources::new(
            None,
            &BTreeMap::new(),
            ConfigOverrides {
                source_shutdown_ms: Some(accepted),
                ..ConfigOverrides::default()
            },
        ));
        assert!(config.is_ok(), "source shutdown {accepted}ms was rejected");
    }

    for rejected in [0, 60_001] {
        let error = AppConfig::load(ConfigSources::new(
            None,
            &BTreeMap::new(),
            ConfigOverrides {
                source_shutdown_ms: Some(rejected),
                ..ConfigOverrides::default()
            },
        ));
        assert!(matches!(
            error,
            Err(market_squawk_platform::ConfigError::InvalidSourceShutdownTiming)
        ));
    }
}

#[test]
fn debug_output_redacts_secret_references() -> Result<(), Box<dyn std::error::Error>> {
    let environment = environment(&[(
        "MARKET_SQUAWK_SOURCE_SECRET",
        "keyring:highly-sensitive-account",
    )]);
    let config = AppConfig::load(ConfigSources::new(
        None,
        &environment,
        ConfigOverrides::default(),
    ))?;

    let debug = format!("{config:?}");
    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains("highly-sensitive-account"));
    Ok(())
}

#[test]
fn supplied_environment_is_validated_without_reading_process_environment() {
    let environment = environment(&[("MARKET_SQUAWK_CAPTURE_QUEUE_CAPACITY", "0")]);
    assert!(
        AppConfig::load(ConfigSources::new(
            None,
            &environment,
            ConfigOverrides::default(),
        ))
        .is_err()
    );
}

#[test]
fn legacy_journal_queue_file_and_environment_names_are_rejected()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("legacy.toml");
    std::fs::write(&path, "journal_queue_capacity = 8\n")?;
    assert!(
        AppConfig::load(ConfigSources::new(
            Some(&path),
            &BTreeMap::new(),
            ConfigOverrides::default(),
        ))
        .is_err()
    );

    let legacy_environment = environment(&[("MARKET_SQUAWK_JOURNAL_QUEUE_CAPACITY", "8")]);
    assert!(
        AppConfig::load(ConfigSources::new(
            None,
            &legacy_environment,
            ConfigOverrides::default(),
        ))
        .is_err()
    );
    Ok(())
}

#[test]
fn capture_memory_ceilings_reject_zero() {
    for key in [
        "MARKET_SQUAWK_CAPTURE_MEMORY_CEILING_BYTES",
        "MARKET_SQUAWK_CAPTURE_DESTINATION_REGISTRY_MEMORY_CEILING_BYTES",
    ] {
        let environment = environment(&[(key, "0")]);
        assert!(
            AppConfig::load(ConfigSources::new(
                None,
                &environment,
                ConfigOverrides::default(),
            ))
            .is_err()
        );
    }
}

#[test]
fn combined_flush_and_shutdown_values_are_validated() {
    let environment = environment(&[
        ("MARKET_SQUAWK_CAPTURE_FLUSH_INTERVAL_MS", "5000"),
        ("MARKET_SQUAWK_CAPTURE_SHUTDOWN_MS", "1000"),
    ]);
    let error = AppConfig::load(ConfigSources::new(
        None,
        &environment,
        ConfigOverrides::default(),
    ));
    assert!(error.is_err());
}

#[test]
fn unknown_market_squawk_environment_keys_fail_closed() {
    let environment = environment(&[("MARKET_SQUAWK_ROTATE_ACCOUNTS", "true")]);
    assert!(
        AppConfig::load(ConfigSources::new(
            None,
            &environment,
            ConfigOverrides::default(),
        ))
        .is_err()
    );
}

#[test]
fn configuration_sources_debug_never_renders_environment_values() {
    let environment = environment(&[("UNRELATED_SECRET", "must-never-appear-in-debug-output")]);
    let sources = ConfigSources::new(None, &environment, ConfigOverrides::default());

    let debug = format!("{sources:?}");
    assert!(debug.contains("[ENVIRONMENT OMITTED]"));
    assert!(!debug.contains("must-never-appear-in-debug-output"));
}

#[test]
fn oversized_config_is_rejected_before_toml_parsing() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("oversized.toml");
    std::fs::write(&path, vec![b'x'; 1024 * 1024 + 1])?;

    let error = AppConfig::load(ConfigSources::new(
        Some(&path),
        &BTreeMap::new(),
        ConfigOverrides::default(),
    ));
    assert!(matches!(
        error,
        Err(market_squawk_platform::ConfigError::FileTooLarge)
    ));
    Ok(())
}

#[test]
fn malformed_secret_bearing_toml_is_redacted_from_errors() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempdir()?;
    let path = directory.path().join("invalid.toml");
    std::fs::write(&path, "source_secret = [\"sensitive-locator\"")?;

    let error = AppConfig::load(ConfigSources::new(
        Some(&path),
        &BTreeMap::new(),
        ConfigOverrides::default(),
    ))
    .err()
    .ok_or("invalid TOML was accepted")?;
    let rendered = format!("{error:?} {error}");
    assert!(!rendered.contains("sensitive-locator"));
    Ok(())
}

#[test]
fn duplicate_and_empty_products_fail_closed() {
    for products in [vec![], vec!["BTC-USD".to_owned(), "BTC-USD".to_owned()]] {
        let error = AppConfig::load(ConfigSources::new(
            None,
            &BTreeMap::new(),
            ConfigOverrides {
                products: Some(products),
                ..ConfigOverrides::default()
            },
        ));
        assert!(error.is_err());
    }
}
