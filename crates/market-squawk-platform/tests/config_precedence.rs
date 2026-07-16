use std::{collections::BTreeMap, ffi::OsString, path::PathBuf};

use market_squawk_platform::{AppConfig, ConfigOverrides, ConfigSources, SecretReference};
use tempfile::tempdir;

fn environment(values: &[(&str, &str)]) -> BTreeMap<OsString, OsString> {
    values
        .iter()
        .map(|(key, value)| (OsString::from(key), OsString::from(value)))
        .collect()
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
journal_queue_capacity = 8
paper_bot_enabled = false
capture_flush_interval_ms = 500
capture_shutdown_ms = 2000
source_secret = "keyring:coinbase"
"#,
    )?;
    let environment = environment(&[
        ("MARKET_SQUAWK_DATA_DIR", "from-env"),
        ("MARKET_SQUAWK_PRODUCTS", "ENV-USD,ENV-EUR"),
        ("MARKET_SQUAWK_JOURNAL_QUEUE_CAPACITY", "16"),
    ]);
    let cli = ConfigOverrides {
        data_dir: Some(PathBuf::from("from-cli")),
        products: Some(vec!["CLI-USD".to_owned()]),
        journal_queue_capacity: Some(32),
        ..ConfigOverrides::default()
    };

    let config = AppConfig::load(ConfigSources::new(Some(&config_file), &environment, cli))?;

    assert_eq!(config.data_dir(), PathBuf::from("from-cli"));
    assert_eq!(config.products(), ["CLI-USD"]);
    assert_eq!(config.journal_queue_capacity().get(), 32);
    assert_eq!(config.stale_after().as_millis(), 4_000);
    assert_eq!(
        config.source_secret(),
        Some(&SecretReference::try_from("keyring:coinbase")?)
    );
    Ok(())
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
    let environment = environment(&[("MARKET_SQUAWK_JOURNAL_QUEUE_CAPACITY", "0")]);
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
