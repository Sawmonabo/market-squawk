use std::num::{NonZeroU32, NonZeroUsize};

use market_squawk_analytics::{
    FeatureDataType, FeatureInput, FeatureInputSchema, FeatureKey, FeatureMetadata,
    FeatureMetadataError, FeatureNullPolicy, FeatureOutputType, FeatureParameter,
    FeatureParameterValue, FeatureParameters, FeatureRegistry, FeatureRegistryError,
    FeatureTimeSemantics, FeatureUnit, FeatureWarmUp, RegistrationOutcome,
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
