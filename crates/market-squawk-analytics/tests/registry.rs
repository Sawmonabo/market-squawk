use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};

use market_squawk_analytics::{
    BatchFeatureCatalog, BatchFeatureCatalogConfig, BatchFeaturePolicies, FeatureCompatibility,
    FeatureDataType, FeatureImplementationDigest, FeatureInput, FeatureInputSchema, FeatureKey,
    FeatureMetadata, FeatureMetadataError, FeatureNullPolicy, FeatureOutputType, FeatureParameter,
    FeatureParameterValue, FeatureParameters, FeatureRegistry, FeatureRegistryError,
    FeatureTimeSemantics, FeatureUnit, FeatureWarmUp, KnownFeatureImplementation,
    LiveFeatureCatalog, LiveFeatureCatalogConfig, LiveFeatureCatalogConfigError,
    MissingValuePolicy, REQUIRED_BATCH_FEATURE_COUNT, REQUIRED_LIVE_FEATURE_COUNT,
    RegistrationOutcome, RequiredLiveFeature, ShockComposition, VarianceConvention, WeightPolicy,
};
use market_squawk_domain::RoundingPolicy;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn spread_metadata(
    implementation_revision: &str,
) -> Result<FeatureMetadata, Box<dyn std::error::Error>> {
    Ok(
        LiveFeatureCatalog::try_new(live_catalog_config()?, implementation_revision)?
            .metadata(RequiredLiveFeature::Spread)
            .clone(),
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
    batch_metadata("research.price-return")
}

fn untrusted_batch_risk_metadata(name: &str) -> Result<FeatureMetadata, FeatureMetadataError> {
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
    batch_metadata("macro.yield-curve-slope")
}

fn batch_metadata(name: &str) -> Result<FeatureMetadata, FeatureMetadataError> {
    let config = batch_catalog_config()?;
    let catalog = BatchFeatureCatalog::try_new(config, "git:batch-catalog-v1")?;
    let key = FeatureKey::try_new(name, NonZeroU32::MIN)?;
    catalog
        .metadata(&key)
        .cloned()
        .ok_or(FeatureMetadataError::InvalidBatchCatalogPolicy)
}

fn batch_catalog_config() -> Result<BatchFeatureCatalogConfig, FeatureMetadataError> {
    BatchFeatureCatalogConfig::try_new(
        NonZeroU32::new(252).ok_or(FeatureMetadataError::InvalidBatchCatalogPolicy)?,
        NonZeroU32::new(950_000).ok_or(FeatureMetadataError::InvalidBatchCatalogPolicy)?,
        6,
        BatchFeaturePolicies::new(
            VarianceConvention::Sample,
            MissingValuePolicy::Reject,
            WeightPolicy::PositiveNormalized,
            RoundingPolicy::NearestEven,
            ShockComposition::Compounded,
        ),
    )
}

fn expected_batch_contract(
    name: &str,
) -> Option<(&'static [&'static str], &'static [&'static str])> {
    let contract = match name {
        "research.price-return"
        | "risk.maximum-drawdown"
        | "risk.maximum-drawdown-peak-index"
        | "risk.maximum-drawdown-trough-index"
        | "risk.maximum-drawdown-recovery-index" => (&["timestamps", "prices"][..], &[][..]),
        "research.total-return" => (
            &["timestamps", "money_prices", "distributions"][..],
            &[][..],
        ),
        "research.cumulative-return" => (&["returns"][..], &[][..]),
        "risk.volatility" => (
            &["returns"][..],
            &[
                "periods_per_year",
                "variance_convention",
                "missing_value_policy",
            ][..],
        ),
        "risk.correlation" => (&["asset_returns", "benchmark_returns"][..], &[][..]),
        "risk.alpha" | "risk.beta" => (
            &["asset_returns", "benchmark_returns"][..],
            &["variance_convention"][..],
        ),
        "risk.sharpe" => (
            &["returns", "risk_free_return"][..],
            &["periods_per_year"][..],
        ),
        "risk.sortino" => (&["returns", "target_return"][..], &["periods_per_year"][..]),
        "risk.tracking-error" | "risk.information-ratio" => (
            &["asset_returns", "benchmark_returns"][..],
            &["periods_per_year"][..],
        ),
        "risk.historical-var" => (&["returns"][..], &["confidence_parts_per_million"][..]),
        "risk.parametric-var" => (
            &["mean", "standard_deviation"][..],
            &["periods_per_year", "confidence_parts_per_million"][..],
        ),
        "risk.expected-shortfall" => (
            &["returns", "weights"][..],
            &["confidence_parts_per_million", "weight_policy"][..],
        ),
        "factors.intercept" | "factors.exposure" | "factors.r-squared" => (
            &["factor_identifiers", "asset_returns", "factor_returns"][..],
            &[][..],
        ),
        "fundamentals.growth"
        | "fundamentals.margin"
        | "fundamentals.valuation-multiple"
        | "fundamentals.free-cash-flow-yield"
        | "fundamentals.earnings-surprise" => (
            &["numerator", "denominator"][..],
            &["decimal_scale", "rounding_policy"][..],
        ),
        "fundamentals.free-cash-flow" => {
            (&["operating_cash_flow", "capital_expenditure"][..], &[][..])
        }
        "macro.surprise" => (
            &["actual", "consensus", "scale"][..],
            &["decimal_scale", "rounding_policy"][..],
        ),
        "macro.yield-curve-short-rate"
        | "macro.yield-curve-middle-rate"
        | "macro.yield-curve-long-rate"
        | "macro.yield-curve-slope"
        | "macro.yield-curve-curvature" => (&["maturity_days", "rates"][..], &[][..]),
        "macro.rate-change-average-parallel-shift" => (
            &["maturity_days", "prior_rates", "current_rates"][..],
            &["decimal_scale", "rounding_policy"][..],
        ),
        "macro.rate-change-slope" | "macro.rate-change-short" | "macro.rate-change-long" => (
            &["maturity_days", "prior_rates", "current_rates"][..],
            &[][..],
        ),
        "portfolio.net-exposure"
        | "portfolio.gross-exposure"
        | "portfolio.attribution-contribution"
        | "portfolio.attribution-total" => (
            &["allocation_dimensions", "market_values", "return_rates"][..],
            &[][..],
        ),
        "scenario.stress-contribution" | "scenario.stress-total" => (
            &[
                "allocation_dimensions",
                "market_values",
                "shock_dimensions",
                "return_shocks",
            ][..],
            &["shock_composition"][..],
        ),
        _ => return None,
    };
    Some(contract)
}

fn expected_input_shape(name: &str) -> Option<(FeatureDataType, FeatureUnit)> {
    let shape = match name {
        "timestamps" => (FeatureDataType::Timestamp, FeatureUnit::Nanoseconds),
        "prices" => (FeatureDataType::StatisticalF64, FeatureUnit::CurrencyAmount),
        "money_prices" | "distributions" => (FeatureDataType::Money, FeatureUnit::CurrencyAmount),
        "returns" | "risk_free_return" | "target_return" | "asset_returns"
        | "benchmark_returns" | "factor_returns" => {
            (FeatureDataType::StatisticalF64, FeatureUnit::Return)
        }
        "weights" => (FeatureDataType::StatisticalF64, FeatureUnit::Unitless),
        "mean" => (FeatureDataType::StatisticalLocation, FeatureUnit::Return),
        "standard_deviation" => (
            FeatureDataType::StatisticalDispersion,
            FeatureUnit::Volatility,
        ),
        "factor_identifiers" | "allocation_dimensions" | "shock_dimensions" => {
            (FeatureDataType::CanonicalIdentifier, FeatureUnit::Unitless)
        }
        "numerator"
        | "denominator"
        | "operating_cash_flow"
        | "capital_expenditure"
        | "market_values" => (FeatureDataType::MonetaryValue, FeatureUnit::CurrencyAmount),
        "maturity_days" => (FeatureDataType::UnsignedInteger, FeatureUnit::Count),
        "rates" | "prior_rates" | "current_rates" | "return_rates" | "return_shocks" => {
            (FeatureDataType::ExactRate, FeatureUnit::Rate)
        }
        "actual" | "consensus" | "scale" => {
            (FeatureDataType::DecimalMeasurement, FeatureUnit::Unitless)
        }
        _ => return None,
    };
    Some(shape)
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
    let scalar_alpha = batch_metadata("risk.alpha")?;
    let legacy_multi_output = untrusted_batch_risk_metadata("risk.alpha-beta")?;
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
        registry.try_register(reordered),
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
        spread_metadata("git:0123456789abcdef")?.semantic_digest()
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

    let batch = BatchFeatureCatalog::try_new(batch_catalog_config()?, "git:batch-catalog-v1")?;
    assert_eq!(batch.entries().len(), REQUIRED_BATCH_FEATURE_COUNT);
    for metadata in batch.entries() {
        let (expected_inputs, expected_parameters) = expected_batch_contract(metadata.key().name())
            .ok_or("batch feature is missing its executable contract audit")?;
        assert_eq!(
            metadata
                .input_schema()
                .fields()
                .iter()
                .map(FeatureInput::name)
                .collect::<Vec<_>>(),
            expected_inputs,
            "input contract mismatch for {}",
            metadata.key().name(),
        );
        for input in metadata.input_schema().fields() {
            let expected = expected_input_shape(input.name())
                .ok_or("batch input is missing its typed-shape audit")?;
            assert_eq!(
                (input.data_type(), input.unit()),
                expected,
                "typed input mismatch for {}.{}",
                metadata.key().name(),
                input.name(),
            );
        }
        assert_eq!(
            metadata
                .parameters()
                .entries()
                .iter()
                .map(FeatureParameter::name)
                .collect::<Vec<_>>(),
            expected_parameters,
            "policy contract mismatch for {}",
            metadata.key().name(),
        );
        for parameter in metadata.parameters().entries() {
            let expected_value = match parameter.name() {
                "periods_per_year" => FeatureParameterValue::UnsignedInteger(252),
                "confidence_parts_per_million" => FeatureParameterValue::UnsignedInteger(950_000),
                "decimal_scale" => FeatureParameterValue::UnsignedInteger(6),
                "variance_convention" => {
                    FeatureParameterValue::VarianceConvention(VarianceConvention::Sample)
                }
                "missing_value_policy" => {
                    FeatureParameterValue::MissingValuePolicy(MissingValuePolicy::Reject)
                }
                "weight_policy" => {
                    FeatureParameterValue::WeightPolicy(WeightPolicy::PositiveNormalized)
                }
                "rounding_policy" => {
                    FeatureParameterValue::RoundingPolicy(RoundingPolicy::NearestEven)
                }
                "shock_composition" => {
                    FeatureParameterValue::ShockComposition(ShockComposition::Compounded)
                }
                _ => return Err("batch parameter is missing its typed-value audit".into()),
            };
            assert_eq!(parameter.value(), expected_value);
        }
    }
    let mut batch_registry = FeatureRegistry::try_new(
        BatchFeatureCatalog::minimum_registry_capacity(),
        NonZeroUsize::new(4 * 1024 * 1024).ok_or("batch registry retained bytes")?,
    )?;
    assert_eq!(
        batch.try_register(&mut batch_registry)?.inserted(),
        REQUIRED_BATCH_FEATURE_COUNT
    );
    Ok(())
}

#[test]
fn catalog_registration_fails_atomically_on_conflict_or_capacity() -> TestResult {
    let catalog = LiveFeatureCatalog::try_new(live_catalog_config()?, "git:live-catalog-v1")?;
    let conflicting_catalog =
        LiveFeatureCatalog::try_new(live_catalog_config()?, "git:conflicting-implementation")?;
    let conflicting = conflicting_catalog
        .metadata(RequiredLiveFeature::Momentum)
        .clone();
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
