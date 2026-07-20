use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};

use market_squawk_analytics::{
    FeatureDataType, FeatureInput, FeatureInputSchema, FeatureKey, FeatureMetadata,
    FeatureMetadataError, FeatureNullPolicy, FeatureOutputType, FeatureParameter,
    FeatureParameterValue, FeatureParameters, FeatureRegistry, FeatureRegistryError,
    FeatureTimeSemantics, FeatureUnit, FeatureWarmUp, LiveFeatureCatalog, LiveFeatureCatalogConfig,
    LiveFeatureCatalogConfigError, REQUIRED_LIVE_FEATURE_COUNT, RegistrationOutcome,
    RequiredLiveFeature,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn spread_metadata(implementation_revision: &str) -> Result<FeatureMetadata, FeatureMetadataError> {
    metadata_with_output(
        implementation_revision,
        FeatureOutputType::PriceTicks,
        FeatureUnit::PriceTicks,
    )
}

fn metadata_with_output(
    implementation_revision: &str,
    output_type: FeatureOutputType,
    unit: FeatureUnit,
) -> Result<FeatureMetadata, FeatureMetadataError> {
    FeatureMetadata::try_new(
        FeatureKey::try_new("spread", NonZeroU32::MIN)?,
        FeatureInputSchema::try_new(vec![
            FeatureInput::try_new(
                "best_bid",
                FeatureDataType::PriceTicks,
                FeatureUnit::PriceTicks,
                false,
            )?,
            FeatureInput::try_new(
                "best_ask",
                FeatureDataType::PriceTicks,
                FeatureUnit::PriceTicks,
                false,
            )?,
        ])?,
        FeatureParameters::try_new(vec![FeatureParameter::try_new(
            "depth",
            FeatureParameterValue::UnsignedInteger(1),
        )?])?,
        FeatureTimeSemantics::EventTime,
        FeatureWarmUp::None,
        FeatureNullPolicy::Unavailable,
        output_type,
        unit,
        true,
        true,
        implementation_revision,
    )
}

#[test]
fn registration_is_idempotent_and_same_key_conflicts_fail_closed() -> TestResult {
    let metadata = spread_metadata("git:0123456789abcdef")?;
    let conflicting = spread_metadata("git:fedcba9876543210")?;
    let mut registry = FeatureRegistry::try_new(
        NonZeroUsize::new(4).ok_or("invalid registry test capacity")?,
        NonZeroUsize::new(64 * 1024).ok_or("invalid registry test budget")?,
    )?;

    assert_eq!(
        registry.try_register(metadata.clone())?,
        RegistrationOutcome::Inserted
    );
    assert_eq!(
        registry.try_register(metadata)?,
        RegistrationOutcome::AlreadyRegistered
    );
    assert_eq!(
        registry.try_register(conflicting),
        Err(FeatureRegistryError::MetadataConflict)
    );
    assert_eq!(registry.len(), 1);
    assert!(registry.retained_bytes() <= registry.retained_byte_limit().get());
    Ok(())
}

#[test]
fn output_type_unit_compatibility_is_closed() {
    const ALL_UNITS: [FeatureUnit; 10] = [
        FeatureUnit::PriceTicks,
        FeatureUnit::QuantityLots,
        FeatureUnit::BasisPoints,
        FeatureUnit::Ratio,
        FeatureUnit::Return,
        FeatureUnit::Volatility,
        FeatureUnit::LotsPerSecond,
        FeatureUnit::Count,
        FeatureUnit::Nanoseconds,
        FeatureUnit::Unitless,
    ];
    let cases: &[(FeatureOutputType, &[FeatureUnit])] = &[
        (FeatureOutputType::PriceTicks, &[FeatureUnit::PriceTicks]),
        (FeatureOutputType::HalfTickPrice, &[FeatureUnit::PriceTicks]),
        (
            FeatureOutputType::QuantityLots,
            &[FeatureUnit::QuantityLots],
        ),
        (FeatureOutputType::BasisPoints, &[FeatureUnit::BasisPoints]),
        (
            FeatureOutputType::SignedInteger,
            &[
                FeatureUnit::QuantityLots,
                FeatureUnit::Count,
                FeatureUnit::Nanoseconds,
            ],
        ),
        (
            FeatureOutputType::UnsignedInteger,
            &[FeatureUnit::Count, FeatureUnit::Nanoseconds],
        ),
        (
            FeatureOutputType::ExactRatio,
            &[
                FeatureUnit::PriceTicks,
                FeatureUnit::BasisPoints,
                FeatureUnit::Ratio,
                FeatureUnit::Return,
                FeatureUnit::Volatility,
                FeatureUnit::LotsPerSecond,
                FeatureUnit::Unitless,
            ],
        ),
        (
            FeatureOutputType::StatisticalF64,
            &[
                FeatureUnit::BasisPoints,
                FeatureUnit::Ratio,
                FeatureUnit::Return,
                FeatureUnit::Volatility,
                FeatureUnit::LotsPerSecond,
                FeatureUnit::Count,
                FeatureUnit::Nanoseconds,
                FeatureUnit::Unitless,
            ],
        ),
    ];

    for &(output_type, allowed_units) in cases {
        for unit in ALL_UNITS {
            assert_eq!(
                metadata_with_output("git:compatibility-matrix", output_type, unit).is_ok(),
                allowed_units.contains(&unit),
                "unexpected compatibility for {output_type:?} with {unit:?}",
            );
        }
    }
}

fn live_catalog_config() -> Result<LiveFeatureCatalogConfig, Box<dyn std::error::Error>> {
    Ok(LiveFeatureCatalogConfig::try_new(
        NonZeroU32::new(50).ok_or("book levels")?,
        NonZeroU32::new(1_024).ok_or("trade observations")?,
        NonZeroU32::new(4_096).ok_or("rolling observations")?,
        NonZeroU32::new(3).ok_or("rolling warm-up")?,
        NonZeroU64::new(60_000_000_000).ok_or("rolling duration")?,
        NonZeroU32::new(8).ok_or("cross-venue observations")?,
        NonZeroU64::new(250_000_000).ok_or("cross-venue skew")?,
    )?)
}

#[test]
fn required_live_catalog_is_complete_ordered_and_idempotent() -> TestResult {
    const EXPECTED: [(&str, FeatureOutputType, FeatureUnit); REQUIRED_LIVE_FEATURE_COUNT] = [
        (
            "book.spread",
            FeatureOutputType::PriceTicks,
            FeatureUnit::PriceTicks,
        ),
        (
            "book.midpoint",
            FeatureOutputType::HalfTickPrice,
            FeatureUnit::PriceTicks,
        ),
        (
            "book.microprice",
            FeatureOutputType::ExactRatio,
            FeatureUnit::PriceTicks,
        ),
        (
            "book.imbalance",
            FeatureOutputType::ExactRatio,
            FeatureUnit::Ratio,
        ),
        (
            "book.order-flow-imbalance",
            FeatureOutputType::SignedInteger,
            FeatureUnit::QuantityLots,
        ),
        (
            "book.depth-weighted-price",
            FeatureOutputType::ExactRatio,
            FeatureUnit::PriceTicks,
        ),
        (
            "trade.aggressor-imbalance",
            FeatureOutputType::ExactRatio,
            FeatureUnit::Ratio,
        ),
        (
            "trade.rolling-vwap",
            FeatureOutputType::ExactRatio,
            FeatureUnit::PriceTicks,
        ),
        (
            "trade.volume-velocity",
            FeatureOutputType::ExactRatio,
            FeatureUnit::LotsPerSecond,
        ),
        (
            "trade.momentum",
            FeatureOutputType::PriceTicks,
            FeatureUnit::PriceTicks,
        ),
        (
            "trade.rolling-return",
            FeatureOutputType::StatisticalF64,
            FeatureUnit::Return,
        ),
        (
            "trade.rolling-volatility",
            FeatureOutputType::StatisticalF64,
            FeatureUnit::Volatility,
        ),
        (
            "cross-venue.divergence",
            FeatureOutputType::ExactRatio,
            FeatureUnit::BasisPoints,
        ),
        (
            "liquidity.available-quantity",
            FeatureOutputType::SignedInteger,
            FeatureUnit::QuantityLots,
        ),
        (
            "liquidity.slippage",
            FeatureOutputType::ExactRatio,
            FeatureUnit::BasisPoints,
        ),
    ];
    let catalog = LiveFeatureCatalog::try_new(live_catalog_config()?, "git:live-catalog-v1")?;

    assert_eq!(catalog.entries().len(), REQUIRED_LIVE_FEATURE_COUNT);
    for (metadata, (name, output_type, unit)) in catalog.entries().iter().zip(EXPECTED) {
        assert_eq!(metadata.key().name(), name);
        assert_eq!(metadata.key().version(), NonZeroU32::MIN);
        assert_eq!(metadata.output_type(), output_type);
        assert_eq!(metadata.unit(), unit);
        assert!(!metadata.input_schema().fields().is_empty());
        assert!(!metadata.parameters().entries().is_empty());
        assert!(metadata.is_live_compatible());
        assert!(metadata.is_point_in_time_compatible());
        assert_eq!(metadata.implementation_revision(), "git:live-catalog-v1");
    }
    assert_eq!(
        catalog
            .metadata(RequiredLiveFeature::CrossVenueDivergence)
            .warm_up(),
        FeatureWarmUp::Observations(NonZeroU32::new(2).ok_or("cross-venue warm-up")?)
    );
    let liquidity_inputs = catalog
        .metadata(RequiredLiveFeature::AvailableLiquidity)
        .input_schema()
        .fields();
    assert!(liquidity_inputs.iter().any(|input| {
        input.name() == "order_side" && input.data_type() == FeatureDataType::OrderSide
    }));
    assert!(liquidity_inputs.iter().any(|input| {
        input.name() == "requested_quantity"
            && input.data_type() == FeatureDataType::QuantityLots
            && input.unit() == FeatureUnit::QuantityLots
    }));

    let mut registry = FeatureRegistry::try_new(
        NonZeroUsize::new(REQUIRED_LIVE_FEATURE_COUNT).ok_or("catalog capacity")?,
        NonZeroUsize::new(1024 * 1024).ok_or("catalog retained bytes")?,
    )?;
    let first = catalog.try_register(&mut registry)?;
    let second = catalog.try_register(&mut registry)?;
    assert_eq!(first.inserted(), REQUIRED_LIVE_FEATURE_COUNT);
    assert_eq!(first.already_registered(), 0);
    assert_eq!(second.inserted(), 0);
    assert_eq!(second.already_registered(), REQUIRED_LIVE_FEATURE_COUNT);
    assert_eq!(registry.entries().count(), REQUIRED_LIVE_FEATURE_COUNT);
    Ok(())
}

#[test]
fn catalog_registration_fails_atomically_on_conflict_or_capacity() -> TestResult {
    let catalog = LiveFeatureCatalog::try_new(live_catalog_config()?, "git:live-catalog-v1")?;
    let conflicting = FeatureMetadata::try_new(
        FeatureKey::try_new("trade.momentum", NonZeroU32::MIN)?,
        FeatureInputSchema::try_new(vec![FeatureInput::try_new(
            "price",
            FeatureDataType::PriceTicks,
            FeatureUnit::PriceTicks,
            false,
        )?])?,
        FeatureParameters::try_new(vec![FeatureParameter::try_new(
            "maximum_observations",
            FeatureParameterValue::UnsignedInteger(1),
        )?])?,
        FeatureTimeSemantics::EventTime,
        FeatureWarmUp::None,
        FeatureNullPolicy::Unavailable,
        FeatureOutputType::PriceTicks,
        FeatureUnit::PriceTicks,
        true,
        true,
        "git:conflicting-implementation",
    )?;
    let mut conflicting_registry = FeatureRegistry::try_new(
        NonZeroUsize::new(REQUIRED_LIVE_FEATURE_COUNT).ok_or("catalog capacity")?,
        NonZeroUsize::new(1024 * 1024).ok_or("catalog retained bytes")?,
    )?;
    conflicting_registry.try_register(conflicting)?;

    assert_eq!(
        catalog.try_register(&mut conflicting_registry),
        Err(FeatureRegistryError::MetadataConflict)
    );
    assert_eq!(conflicting_registry.len(), 1);

    let mut undersized_registry = FeatureRegistry::try_new(
        NonZeroUsize::new(REQUIRED_LIVE_FEATURE_COUNT - 1).ok_or("undersized capacity")?,
        NonZeroUsize::new(1024 * 1024).ok_or("catalog retained bytes")?,
    )?;
    assert_eq!(
        catalog.try_register(&mut undersized_registry),
        Err(FeatureRegistryError::RegistryFull)
    );
    assert!(undersized_registry.is_empty());

    let mut duplicate_registry = FeatureRegistry::try_new(
        NonZeroUsize::new(REQUIRED_LIVE_FEATURE_COUNT).ok_or("catalog capacity")?,
        NonZeroUsize::new(1024 * 1024).ok_or("catalog retained bytes")?,
    )?;
    let duplicate = [catalog.entries()[0].clone(), catalog.entries()[0].clone()];
    assert_eq!(
        duplicate_registry.try_register_batch(&duplicate),
        Err(FeatureRegistryError::DuplicateBatchKey)
    );
    assert!(duplicate_registry.is_empty());
    Ok(())
}

#[test]
fn catalog_rejects_capacities_that_cannot_produce_every_feature() -> TestResult {
    let nonzero_u32 = |value| NonZeroU32::new(value).ok_or("nonzero u32");
    let duration = NonZeroU64::new(1).ok_or("duration")?;

    assert_eq!(
        LiveFeatureCatalogConfig::try_new(
            nonzero_u32(1)?,
            nonzero_u32(1)?,
            nonzero_u32(2)?,
            nonzero_u32(1)?,
            duration,
            nonzero_u32(2)?,
            duration,
        ),
        Err(LiveFeatureCatalogConfigError::RollingCapacityTooSmall)
    );
    assert_eq!(
        LiveFeatureCatalogConfig::try_new(
            nonzero_u32(1)?,
            nonzero_u32(1)?,
            nonzero_u32(3)?,
            nonzero_u32(1)?,
            duration,
            nonzero_u32(1)?,
            duration,
        ),
        Err(LiveFeatureCatalogConfigError::CrossVenueBoundTooSmall)
    );
    Ok(())
}
