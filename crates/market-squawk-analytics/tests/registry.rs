use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};

use market_squawk_analytics::{
    FeatureCompatibility, FeatureDataType, FeatureImplementationDigest, FeatureInput,
    FeatureInputSchema, FeatureKey, FeatureMetadata, FeatureMetadataError, FeatureNullPolicy,
    FeatureOutputType, FeatureParameter, FeatureParameterValue, FeatureParameters, FeatureRegistry,
    FeatureRegistryError, FeatureTimeSemantics, FeatureUnit, FeatureWarmUp,
    KnownFeatureImplementation, LiveFeatureCatalog, LiveFeatureCatalogConfig,
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
    metadata_with_contract(
        implementation_revision,
        output_type,
        unit,
        true,
        RequiredLiveFeature::Spread.implementation_digest()?,
        false,
    )
}

fn metadata_with_contract(
    implementation_revision: &str,
    output_type: FeatureOutputType,
    unit: FeatureUnit,
    point_in_time_compatible: bool,
    implementation_digest: FeatureImplementationDigest,
    reverse_inputs: bool,
) -> Result<FeatureMetadata, FeatureMetadataError> {
    let mut inputs = vec![
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
    ];
    if reverse_inputs {
        inputs.reverse();
    }
    FeatureMetadata::try_new(
        FeatureKey::try_new(RequiredLiveFeature::Spread.name(), NonZeroU32::MIN)?,
        FeatureInputSchema::try_new(inputs)?,
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
        point_in_time_compatible,
        implementation_revision,
        implementation_digest,
    )
}

fn batch_return_metadata() -> Result<FeatureMetadata, FeatureMetadataError> {
    FeatureMetadata::try_new(
        FeatureKey::try_new("research.price-return", NonZeroU32::MIN)?,
        FeatureInputSchema::try_new(vec![
            FeatureInput::try_new(
                "previous_price",
                FeatureDataType::PriceTicks,
                FeatureUnit::PriceTicks,
                false,
            )?,
            FeatureInput::try_new(
                "current_price",
                FeatureDataType::PriceTicks,
                FeatureUnit::PriceTicks,
                false,
            )?,
        ])?,
        FeatureParameters::default(),
        FeatureTimeSemantics::EventTime,
        FeatureWarmUp::Observations(
            NonZeroU32::new(2).ok_or(FeatureMetadataError::RetainedSizeOverflow)?,
        ),
        FeatureNullPolicy::Unavailable,
        FeatureOutputType::StatisticalF64,
        FeatureUnit::Return,
        false,
        true,
        "git:batch-returns-v1",
        KnownFeatureImplementation::BatchReturns.implementation_digest()?,
    )
}

fn batch_risk_metadata(name: &str) -> Result<FeatureMetadata, FeatureMetadataError> {
    FeatureMetadata::try_new(
        FeatureKey::try_new(name, NonZeroU32::MIN)?,
        FeatureInputSchema::try_new(vec![
            FeatureInput::try_new(
                "asset_return",
                FeatureDataType::StatisticalF64,
                FeatureUnit::Return,
                false,
            )?,
            FeatureInput::try_new(
                "benchmark_return",
                FeatureDataType::StatisticalF64,
                FeatureUnit::Return,
                false,
            )?,
        ])?,
        FeatureParameters::default(),
        FeatureTimeSemantics::EventTime,
        FeatureWarmUp::Observations(
            NonZeroU32::new(2).ok_or(FeatureMetadataError::RetainedSizeOverflow)?,
        ),
        FeatureNullPolicy::Unavailable,
        FeatureOutputType::StatisticalF64,
        FeatureUnit::Return,
        false,
        true,
        "git:batch-risk-v1",
        KnownFeatureImplementation::BatchRisk.implementation_digest()?,
    )
}

fn exact_rate_metadata() -> Result<FeatureMetadata, FeatureMetadataError> {
    FeatureMetadata::try_new(
        FeatureKey::try_new("macro.yield-curve-slope", NonZeroU32::MIN)?,
        FeatureInputSchema::try_new(vec![
            FeatureInput::try_new(
                "short_rate",
                FeatureDataType::Decimal,
                FeatureUnit::Rate,
                false,
            )?,
            FeatureInput::try_new(
                "long_rate",
                FeatureDataType::Decimal,
                FeatureUnit::Rate,
                false,
            )?,
        ])?,
        FeatureParameters::default(),
        FeatureTimeSemantics::EventTime,
        FeatureWarmUp::Observations(
            NonZeroU32::new(2).ok_or(FeatureMetadataError::RetainedSizeOverflow)?,
        ),
        FeatureNullPolicy::Unavailable,
        FeatureOutputType::Decimal,
        FeatureUnit::Rate,
        false,
        true,
        "git:batch-macro-v1",
        KnownFeatureImplementation::BatchMacro.implementation_digest()?,
    )
}

#[test]
fn registration_binds_known_code_and_semantics_and_resolution_fails_closed() -> TestResult {
    let metadata = spread_metadata("git:0123456789abcdef")?;
    let conflicting = spread_metadata("git:fedcba9876543210")?;
    let reordered = metadata_with_contract(
        "git:0123456789abcdef",
        FeatureOutputType::PriceTicks,
        FeatureUnit::PriceTicks,
        true,
        RequiredLiveFeature::Spread.implementation_digest()?,
        true,
    )?;
    let live_only = metadata_with_contract(
        "git:0123456789abcdef",
        FeatureOutputType::PriceTicks,
        FeatureUnit::PriceTicks,
        false,
        RequiredLiveFeature::Spread.implementation_digest()?,
        false,
    )?;
    let unknown_digest = FeatureImplementationDigest::try_from_sha256([0xa5; 32])?;
    let unknown_implementation = metadata_with_contract(
        "git:unknown",
        FeatureOutputType::PriceTicks,
        FeatureUnit::PriceTicks,
        true,
        unknown_digest,
        false,
    )?;
    assert_eq!(
        FeatureImplementationDigest::try_from_sha256([0; 32]),
        Err(FeatureMetadataError::ZeroImplementationDigest)
    );
    assert_eq!(
        metadata.input_schema_digest(),
        metadata.clone().input_schema_digest()
    );
    assert_eq!(
        metadata.semantic_digest(),
        metadata.clone().semantic_digest()
    );
    assert_ne!(
        metadata.input_schema_digest(),
        reordered.input_schema_digest()
    );
    assert_ne!(metadata.semantic_digest(), reordered.semantic_digest());
    assert_ne!(metadata.semantic_digest(), conflicting.semantic_digest());
    assert_ne!(metadata.semantic_digest(), live_only.semantic_digest());
    let batch_only = batch_return_metadata()?;
    let exact_rate = exact_rate_metadata()?;
    let scalar_alpha = batch_risk_metadata("risk.alpha")?;
    let legacy_multi_output = batch_risk_metadata("risk.alpha-beta")?;
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
    assert_eq!(
        registry.try_register(batch_only.clone())?,
        RegistrationOutcome::Inserted
    );
    assert_eq!(
        registry.try_register(exact_rate.clone())?,
        RegistrationOutcome::Inserted
    );
    assert_eq!(
        registry.try_register(scalar_alpha)?,
        RegistrationOutcome::Inserted
    );
    assert_eq!(
        registry.try_register(unknown_implementation),
        Err(FeatureRegistryError::UnknownImplementationDigest)
    );
    assert_eq!(
        registry.try_register(legacy_multi_output),
        Err(FeatureRegistryError::UnknownImplementationDigest)
    );
    assert_eq!(registry.len(), 4);
    assert!(registry.retained_bytes() <= registry.retained_byte_limit().get());
    let key = FeatureKey::try_new(RequiredLiveFeature::Spread.name(), NonZeroU32::MIN)?;
    assert_eq!(
        registry
            .try_resolve(&key, FeatureCompatibility::Live)?
            .semantic_digest(),
        metadata_with_output(
            "git:0123456789abcdef",
            FeatureOutputType::PriceTicks,
            FeatureUnit::PriceTicks,
        )?
        .semantic_digest()
    );
    assert_eq!(
        registry.try_resolve(
            &FeatureKey::try_new(
                RequiredLiveFeature::Spread.name(),
                NonZeroU32::new(2).ok_or("version")?,
            )?,
            FeatureCompatibility::Live,
        ),
        Err(FeatureRegistryError::UnknownRequestedVersion)
    );
    assert_eq!(
        registry.try_resolve(
            &FeatureKey::try_new("unknown.feature", NonZeroU32::MIN)?,
            FeatureCompatibility::Live,
        ),
        Err(FeatureRegistryError::UnknownFeature)
    );
    let batch_key = batch_only.key().clone();
    assert_eq!(
        registry
            .try_resolve(&batch_key, FeatureCompatibility::PointInTime)?
            .semantic_digest(),
        batch_only.semantic_digest()
    );
    assert_eq!(
        registry.try_resolve(&batch_key, FeatureCompatibility::Live),
        Err(FeatureRegistryError::IncompatibleRequestedVersion)
    );
    assert_eq!(
        registry
            .try_resolve(exact_rate.key(), FeatureCompatibility::PointInTime)?
            .output_type(),
        FeatureOutputType::Decimal
    );
    assert_eq!(exact_rate.unit(), FeatureUnit::Rate);
    assert_eq!(
        registry
            .entries()
            .map(|entry| entry.key().name())
            .collect::<Vec<_>>(),
        vec![
            "book.spread",
            "macro.yield-curve-slope",
            "research.price-return",
            "risk.alpha",
        ]
    );
    Ok(())
}

#[test]
fn output_type_unit_compatibility_is_closed() {
    const ALL_UNITS: [FeatureUnit; 12] = [
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
        FeatureUnit::Rate,
        FeatureUnit::CurrencyAmount,
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
                FeatureUnit::Rate,
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
                FeatureUnit::Rate,
            ],
        ),
        (
            FeatureOutputType::Decimal,
            &[
                FeatureUnit::BasisPoints,
                FeatureUnit::Ratio,
                FeatureUnit::Return,
                FeatureUnit::Volatility,
                FeatureUnit::Unitless,
                FeatureUnit::Rate,
            ],
        ),
        (FeatureOutputType::Money, &[FeatureUnit::CurrencyAmount]),
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
        RequiredLiveFeature::Momentum.implementation_digest()?,
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
