use market_squawk_domain::{SchemaVersion, SchemaVersionError};

#[test]
fn zero_is_not_a_valid_schema_version() {
    assert_eq!(SchemaVersion::new(0), Err(SchemaVersionError::Zero));
    assert!(serde_json::from_str::<SchemaVersion>("0").is_err());
}

#[test]
fn version_one_round_trips_through_serde() -> Result<(), Box<dyn std::error::Error>> {
    let current = SchemaVersion::CURRENT;
    let encoded = serde_json::to_string(&current)?;
    let decoded: SchemaVersion = serde_json::from_str(&encoded)?;

    assert_eq!(decoded, current);
    Ok(())
}

#[test]
fn unsupported_future_version_is_preserved_in_a_typed_error()
-> Result<(), Box<dyn std::error::Error>> {
    let future: SchemaVersion = serde_json::from_str("2")?;

    assert_eq!(future.get(), 2);
    assert_eq!(
        future.ensure_supported(),
        Err(SchemaVersionError::UnsupportedFuture {
            found: future,
            current: SchemaVersion::CURRENT,
        })
    );
    Ok(())
}
