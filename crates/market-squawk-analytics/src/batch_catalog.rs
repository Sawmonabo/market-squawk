//! Code-owned canonical metadata for every production batch analytical feature.

use std::num::{NonZeroU32, NonZeroUsize};

use market_squawk_domain::{
    FeatureDatasetMacroComponentDescriptor, RoundingPolicy, feature_dataset_macro_components_v1,
};

use crate::{
    BatchRegistrationOutcome, FeatureDataType, FeatureInput, FeatureInputSchema, FeatureKey,
    FeatureMetadata, FeatureMetadataError, FeatureNullPolicy, FeatureOutputType, FeatureParameter,
    FeatureParameterValue, FeatureParameters, FeatureRegistry, FeatureRegistryError,
    FeatureTimeSemantics, FeatureUnit, FeatureWarmUp, KnownFeatureImplementation,
    MissingValuePolicy, ShockComposition, VarianceConvention, WeightPolicy,
};

/// Number of code-owned batch feature definitions compiled into this release.
pub const REQUIRED_BATCH_FEATURE_COUNT: usize =
    BATCH_SPECS.len() + feature_dataset_macro_components_v1().len();

/// Result-changing policies shared by canonical batch-feature definitions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BatchFeaturePolicies {
    variance: VarianceConvention,
    missing: MissingValuePolicy,
    weight: WeightPolicy,
    rounding: RoundingPolicy,
    shock_composition: ShockComposition,
}

impl BatchFeaturePolicies {
    /// Constructs one explicit batch policy set.
    #[must_use]
    pub const fn new(
        variance: VarianceConvention,
        missing: MissingValuePolicy,
        weight: WeightPolicy,
        rounding: RoundingPolicy,
        shock_composition: ShockComposition,
    ) -> Self {
        Self {
            variance,
            missing,
            weight,
            rounding,
            shock_composition,
        }
    }

    /// Returns the variance denominator policy.
    #[must_use]
    pub const fn variance(self) -> VarianceConvention {
        self.variance
    }

    /// Returns the missing-observation policy.
    #[must_use]
    pub const fn missing(self) -> MissingValuePolicy {
        self.missing
    }

    /// Returns the statistical weighting policy.
    #[must_use]
    pub const fn weight(self) -> WeightPolicy {
        self.weight
    }

    /// Returns the exact decimal rounding policy.
    #[must_use]
    pub const fn rounding(self) -> RoundingPolicy {
        self.rounding
    }

    /// Returns the scenario shock-composition policy.
    #[must_use]
    pub const fn shock_composition(self) -> ShockComposition {
        self.shock_composition
    }
}

/// Explicit policy values bound to the canonical batch-feature metadata set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BatchFeatureCatalogConfig {
    periods_per_year: NonZeroU32,
    confidence_parts_per_million: NonZeroU32,
    decimal_scale: u32,
    policies: BatchFeaturePolicies,
}

impl BatchFeatureCatalogConfig {
    /// Constructs the version-one batch policy used by every related metadata definition.
    ///
    /// # Errors
    ///
    /// Rejects confidence outside `(0, 1)` or a decimal scale unsupported by `Decimal`.
    pub fn try_new(
        periods_per_year: NonZeroU32,
        confidence_parts_per_million: NonZeroU32,
        decimal_scale: u32,
        policies: BatchFeaturePolicies,
    ) -> Result<Self, FeatureMetadataError> {
        if confidence_parts_per_million.get() >= 1_000_000 {
            return Err(FeatureMetadataError::InvalidBatchCatalogPolicy);
        }
        if decimal_scale > rust_decimal::Decimal::MAX_SCALE {
            return Err(FeatureMetadataError::InvalidBatchCatalogPolicy);
        }
        Ok(Self {
            periods_per_year,
            confidence_parts_per_million,
            decimal_scale,
            policies,
        })
    }

    /// Returns the bound period cadence.
    #[must_use]
    pub const fn periods_per_year(self) -> NonZeroU32 {
        self.periods_per_year
    }

    /// Returns confidence as integer parts per million.
    #[must_use]
    pub const fn confidence_parts_per_million(self) -> NonZeroU32 {
        self.confidence_parts_per_million
    }

    /// Returns exact decimal output places.
    #[must_use]
    pub const fn decimal_scale(self) -> u32 {
        self.decimal_scale
    }

    /// Returns the complete result-changing policy set.
    #[must_use]
    pub const fn policies(self) -> BatchFeaturePolicies {
        self.policies
    }
}

/// Immutable code-owned metadata catalog for all Task 12 batch kernels.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchFeatureCatalog {
    entries: Box<[FeatureMetadata]>,
}

impl BatchFeatureCatalog {
    /// Builds every canonical batch definition for one explicit policy and code revision.
    ///
    /// # Errors
    ///
    /// Returns a typed metadata error if an internal definition cannot be represented.
    pub fn try_new(
        config: BatchFeatureCatalogConfig,
        implementation_revision: &str,
    ) -> Result<Self, FeatureMetadataError> {
        let entries = BATCH_SPECS
            .iter()
            .map(|spec| metadata(*spec, config, implementation_revision))
            .chain(
                feature_dataset_macro_components_v1()
                    .iter()
                    .map(|descriptor| {
                        macro_component_metadata(*descriptor, config, implementation_revision)
                    }),
            )
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            entries: entries.into_boxed_slice(),
        })
    }

    /// Returns all definitions in stable catalog order.
    #[must_use]
    pub fn entries(&self) -> &[FeatureMetadata] {
        &self.entries
    }

    /// Returns one exact name/version definition.
    #[must_use]
    pub fn metadata(&self, key: &FeatureKey) -> Option<&FeatureMetadata> {
        self.entries.iter().find(|metadata| metadata.key() == key)
    }

    /// Atomically registers the complete catalog.
    ///
    /// # Errors
    ///
    /// Returns a registry conflict, capacity, or retained-byte error without partial mutation.
    pub fn try_register(
        &self,
        registry: &mut FeatureRegistry,
    ) -> Result<BatchRegistrationOutcome, FeatureRegistryError> {
        registry.try_register_batch(&self.entries)
    }

    /// Returns the minimum registry entry capacity required by this catalog.
    #[must_use]
    pub const fn minimum_registry_capacity() -> NonZeroUsize {
        match NonZeroUsize::new(REQUIRED_BATCH_FEATURE_COUNT) {
            Some(capacity) => capacity,
            None => NonZeroUsize::MIN,
        }
    }
}

#[derive(Clone, Copy)]
struct BatchSpec {
    name: &'static str,
    implementation: KnownFeatureImplementation,
    inputs: InputFamily,
    output_type: FeatureOutputType,
    output_unit: FeatureUnit,
    minimum_observations: u32,
    parameters: ParameterFamily,
}

#[derive(Clone, Copy)]
enum InputFamily {
    Prices,
    PricesAndDistributions,
    Returns,
    ReturnsAndRiskFree,
    ReturnsAndTarget,
    WeightedReturns,
    PairedReturns,
    MeanAndDeviation,
    FactorReturns,
    MoneyPair,
    FundamentalPeriod,
    RateCurve,
    RateCurvePair,
    MacroSurprise,
    MacroComponent,
    Portfolio,
    PortfolioAndScenario,
}

#[derive(Clone, Copy)]
enum ParameterFamily {
    None,
    Annualized,
    Volatility,
    Variance,
    Quantile,
    AnnualizedQuantile,
    QuantileWeight,
    Decimal,
    ShockComposition,
}

const fn spec(
    name: &'static str,
    implementation: KnownFeatureImplementation,
    inputs: InputFamily,
    output_type: FeatureOutputType,
    output_unit: FeatureUnit,
    minimum_observations: u32,
    parameters: ParameterFamily,
) -> BatchSpec {
    BatchSpec {
        name,
        implementation,
        inputs,
        output_type,
        output_unit,
        minimum_observations,
        parameters,
    }
}

const BATCH_SPECS: &[BatchSpec] = &[
    spec(
        "research.price-return",
        KnownFeatureImplementation::BatchReturns,
        InputFamily::Prices,
        FeatureOutputType::StatisticalF64,
        FeatureUnit::Return,
        2,
        ParameterFamily::None,
    ),
    spec(
        "research.total-return",
        KnownFeatureImplementation::BatchReturns,
        InputFamily::PricesAndDistributions,
        FeatureOutputType::StatisticalF64,
        FeatureUnit::Return,
        2,
        ParameterFamily::None,
    ),
    spec(
        "research.cumulative-return",
        KnownFeatureImplementation::BatchReturns,
        InputFamily::Returns,
        FeatureOutputType::StatisticalF64,
        FeatureUnit::Return,
        1,
        ParameterFamily::None,
    ),
    spec(
        "risk.volatility",
        KnownFeatureImplementation::BatchRisk,
        InputFamily::Returns,
        FeatureOutputType::StatisticalF64,
        FeatureUnit::Volatility,
        2,
        ParameterFamily::Volatility,
    ),
    spec(
        "risk.maximum-drawdown",
        KnownFeatureImplementation::BatchRisk,
        InputFamily::Prices,
        FeatureOutputType::StatisticalF64,
        FeatureUnit::Return,
        1,
        ParameterFamily::None,
    ),
    spec(
        "risk.maximum-drawdown-peak-index",
        KnownFeatureImplementation::BatchRisk,
        InputFamily::Prices,
        FeatureOutputType::UnsignedInteger,
        FeatureUnit::Count,
        1,
        ParameterFamily::None,
    ),
    spec(
        "risk.maximum-drawdown-trough-index",
        KnownFeatureImplementation::BatchRisk,
        InputFamily::Prices,
        FeatureOutputType::UnsignedInteger,
        FeatureUnit::Count,
        1,
        ParameterFamily::None,
    ),
    spec(
        "risk.maximum-drawdown-recovery-index",
        KnownFeatureImplementation::BatchRisk,
        InputFamily::Prices,
        FeatureOutputType::UnsignedInteger,
        FeatureUnit::Count,
        1,
        ParameterFamily::None,
    ),
    spec(
        "risk.correlation",
        KnownFeatureImplementation::BatchRisk,
        InputFamily::PairedReturns,
        FeatureOutputType::StatisticalF64,
        FeatureUnit::Unitless,
        2,
        ParameterFamily::None,
    ),
    spec(
        "risk.alpha",
        KnownFeatureImplementation::BatchRisk,
        InputFamily::PairedReturns,
        FeatureOutputType::StatisticalF64,
        FeatureUnit::Return,
        2,
        ParameterFamily::Variance,
    ),
    spec(
        "risk.beta",
        KnownFeatureImplementation::BatchRisk,
        InputFamily::PairedReturns,
        FeatureOutputType::StatisticalF64,
        FeatureUnit::Unitless,
        2,
        ParameterFamily::Variance,
    ),
    spec(
        "risk.sharpe",
        KnownFeatureImplementation::BatchRisk,
        InputFamily::ReturnsAndRiskFree,
        FeatureOutputType::StatisticalF64,
        FeatureUnit::Unitless,
        2,
        ParameterFamily::Annualized,
    ),
    spec(
        "risk.sortino",
        KnownFeatureImplementation::BatchRisk,
        InputFamily::ReturnsAndTarget,
        FeatureOutputType::StatisticalF64,
        FeatureUnit::Unitless,
        2,
        ParameterFamily::Annualized,
    ),
    spec(
        "risk.tracking-error",
        KnownFeatureImplementation::BatchRisk,
        InputFamily::PairedReturns,
        FeatureOutputType::StatisticalF64,
        FeatureUnit::Volatility,
        2,
        ParameterFamily::Annualized,
    ),
    spec(
        "risk.information-ratio",
        KnownFeatureImplementation::BatchRisk,
        InputFamily::PairedReturns,
        FeatureOutputType::StatisticalF64,
        FeatureUnit::Unitless,
        2,
        ParameterFamily::Annualized,
    ),
    spec(
        "risk.historical-var",
        KnownFeatureImplementation::BatchRisk,
        InputFamily::Returns,
        FeatureOutputType::StatisticalF64,
        FeatureUnit::Return,
        1,
        ParameterFamily::Quantile,
    ),
    spec(
        "risk.parametric-var",
        KnownFeatureImplementation::BatchRisk,
        InputFamily::MeanAndDeviation,
        FeatureOutputType::StatisticalF64,
        FeatureUnit::Return,
        1,
        ParameterFamily::AnnualizedQuantile,
    ),
    spec(
        "risk.expected-shortfall",
        KnownFeatureImplementation::BatchRisk,
        InputFamily::WeightedReturns,
        FeatureOutputType::StatisticalF64,
        FeatureUnit::Return,
        1,
        ParameterFamily::QuantileWeight,
    ),
    spec(
        "factors.intercept",
        KnownFeatureImplementation::BatchFactors,
        InputFamily::FactorReturns,
        FeatureOutputType::StatisticalF64,
        FeatureUnit::Return,
        3,
        ParameterFamily::None,
    ),
    spec(
        "factors.exposure",
        KnownFeatureImplementation::BatchFactors,
        InputFamily::FactorReturns,
        FeatureOutputType::StatisticalF64,
        FeatureUnit::Unitless,
        3,
        ParameterFamily::None,
    ),
    spec(
        "factors.r-squared",
        KnownFeatureImplementation::BatchFactors,
        InputFamily::FactorReturns,
        FeatureOutputType::StatisticalF64,
        FeatureUnit::Unitless,
        3,
        ParameterFamily::None,
    ),
    spec(
        "fundamentals.growth",
        KnownFeatureImplementation::BatchFundamentals,
        InputFamily::MoneyPair,
        FeatureOutputType::Decimal,
        FeatureUnit::Rate,
        2,
        ParameterFamily::Decimal,
    ),
    spec(
        "fundamentals.margin",
        KnownFeatureImplementation::BatchFundamentals,
        InputFamily::MoneyPair,
        FeatureOutputType::Decimal,
        FeatureUnit::Ratio,
        2,
        ParameterFamily::Decimal,
    ),
    spec(
        "fundamentals.valuation-multiple",
        KnownFeatureImplementation::BatchFundamentals,
        InputFamily::MoneyPair,
        FeatureOutputType::Decimal,
        FeatureUnit::Ratio,
        2,
        ParameterFamily::Decimal,
    ),
    spec(
        "fundamentals.free-cash-flow",
        KnownFeatureImplementation::BatchFundamentals,
        InputFamily::FundamentalPeriod,
        FeatureOutputType::Money,
        FeatureUnit::CurrencyAmount,
        1,
        ParameterFamily::None,
    ),
    spec(
        "fundamentals.free-cash-flow-yield",
        KnownFeatureImplementation::BatchFundamentals,
        InputFamily::MoneyPair,
        FeatureOutputType::Decimal,
        FeatureUnit::Rate,
        2,
        ParameterFamily::Decimal,
    ),
    spec(
        "fundamentals.earnings-surprise",
        KnownFeatureImplementation::BatchFundamentals,
        InputFamily::MoneyPair,
        FeatureOutputType::Decimal,
        FeatureUnit::Ratio,
        2,
        ParameterFamily::Decimal,
    ),
    spec(
        "macro.surprise",
        KnownFeatureImplementation::BatchMacro,
        InputFamily::MacroSurprise,
        FeatureOutputType::Decimal,
        FeatureUnit::Unitless,
        3,
        ParameterFamily::Decimal,
    ),
    spec(
        "macro.yield-curve-short-rate",
        KnownFeatureImplementation::BatchMacro,
        InputFamily::RateCurve,
        FeatureOutputType::Decimal,
        FeatureUnit::Rate,
        3,
        ParameterFamily::None,
    ),
    spec(
        "macro.yield-curve-middle-rate",
        KnownFeatureImplementation::BatchMacro,
        InputFamily::RateCurve,
        FeatureOutputType::Decimal,
        FeatureUnit::Rate,
        3,
        ParameterFamily::None,
    ),
    spec(
        "macro.yield-curve-long-rate",
        KnownFeatureImplementation::BatchMacro,
        InputFamily::RateCurve,
        FeatureOutputType::Decimal,
        FeatureUnit::Rate,
        3,
        ParameterFamily::None,
    ),
    spec(
        "macro.yield-curve-slope",
        KnownFeatureImplementation::BatchMacro,
        InputFamily::RateCurve,
        FeatureOutputType::Decimal,
        FeatureUnit::Rate,
        3,
        ParameterFamily::None,
    ),
    spec(
        "macro.yield-curve-curvature",
        KnownFeatureImplementation::BatchMacro,
        InputFamily::RateCurve,
        FeatureOutputType::Decimal,
        FeatureUnit::Rate,
        3,
        ParameterFamily::None,
    ),
    spec(
        "macro.rate-change-average-parallel-shift",
        KnownFeatureImplementation::BatchMacro,
        InputFamily::RateCurvePair,
        FeatureOutputType::Decimal,
        FeatureUnit::Rate,
        2,
        ParameterFamily::Decimal,
    ),
    spec(
        "macro.rate-change-slope",
        KnownFeatureImplementation::BatchMacro,
        InputFamily::RateCurvePair,
        FeatureOutputType::Decimal,
        FeatureUnit::Rate,
        2,
        ParameterFamily::None,
    ),
    spec(
        "macro.rate-change-short",
        KnownFeatureImplementation::BatchMacro,
        InputFamily::RateCurvePair,
        FeatureOutputType::Decimal,
        FeatureUnit::Rate,
        2,
        ParameterFamily::None,
    ),
    spec(
        "macro.rate-change-long",
        KnownFeatureImplementation::BatchMacro,
        InputFamily::RateCurvePair,
        FeatureOutputType::Decimal,
        FeatureUnit::Rate,
        2,
        ParameterFamily::None,
    ),
    spec(
        "portfolio.net-exposure",
        KnownFeatureImplementation::BatchPortfolioScenarios,
        InputFamily::Portfolio,
        FeatureOutputType::Money,
        FeatureUnit::CurrencyAmount,
        1,
        ParameterFamily::None,
    ),
    spec(
        "portfolio.gross-exposure",
        KnownFeatureImplementation::BatchPortfolioScenarios,
        InputFamily::Portfolio,
        FeatureOutputType::Money,
        FeatureUnit::CurrencyAmount,
        1,
        ParameterFamily::None,
    ),
    spec(
        "portfolio.attribution-contribution",
        KnownFeatureImplementation::BatchPortfolioScenarios,
        InputFamily::Portfolio,
        FeatureOutputType::Money,
        FeatureUnit::CurrencyAmount,
        1,
        ParameterFamily::None,
    ),
    spec(
        "portfolio.attribution-total",
        KnownFeatureImplementation::BatchPortfolioScenarios,
        InputFamily::Portfolio,
        FeatureOutputType::Money,
        FeatureUnit::CurrencyAmount,
        1,
        ParameterFamily::None,
    ),
    spec(
        "scenario.stress-contribution",
        KnownFeatureImplementation::BatchPortfolioScenarios,
        InputFamily::PortfolioAndScenario,
        FeatureOutputType::Money,
        FeatureUnit::CurrencyAmount,
        1,
        ParameterFamily::ShockComposition,
    ),
    spec(
        "scenario.stress-total",
        KnownFeatureImplementation::BatchPortfolioScenarios,
        InputFamily::PortfolioAndScenario,
        FeatureOutputType::Money,
        FeatureUnit::CurrencyAmount,
        1,
        ParameterFamily::ShockComposition,
    ),
];

fn metadata(
    spec: BatchSpec,
    config: BatchFeatureCatalogConfig,
    revision: &str,
) -> Result<FeatureMetadata, FeatureMetadataError> {
    FeatureMetadata::try_new_code_owned(
        FeatureKey::try_new(spec.name, NonZeroU32::MIN)?,
        schema(spec.inputs)?,
        parameters(spec.parameters, config)?,
        FeatureTimeSemantics::EventTime,
        FeatureWarmUp::Observations(
            NonZeroU32::new(spec.minimum_observations)
                .ok_or(FeatureMetadataError::InvalidBatchCatalogPolicy)?,
        ),
        FeatureNullPolicy::Unavailable,
        spec.output_type,
        spec.output_unit,
        false,
        true,
        revision,
        spec.implementation.implementation_digest()?,
    )
}

fn macro_component_metadata(
    descriptor: FeatureDatasetMacroComponentDescriptor,
    config: BatchFeatureCatalogConfig,
    revision: &str,
) -> Result<FeatureMetadata, FeatureMetadataError> {
    metadata(
        spec(
            descriptor.component_name(),
            KnownFeatureImplementation::BatchMacro,
            InputFamily::MacroComponent,
            FeatureOutputType::Decimal,
            FeatureUnit::Rate,
            1,
            ParameterFamily::None,
        ),
        config,
        revision,
    )
}

fn schema(family: InputFamily) -> Result<FeatureInputSchema, FeatureMetadataError> {
    let fields = match family {
        InputFamily::Prices => vec![
            field(
                "timestamps",
                FeatureDataType::Timestamp,
                FeatureUnit::Nanoseconds,
            )?,
            field(
                "prices",
                FeatureDataType::StatisticalF64,
                FeatureUnit::CurrencyAmount,
            )?,
        ],
        InputFamily::PricesAndDistributions => vec![
            field(
                "timestamps",
                FeatureDataType::Timestamp,
                FeatureUnit::Nanoseconds,
            )?,
            field(
                "money_prices",
                FeatureDataType::Money,
                FeatureUnit::CurrencyAmount,
            )?,
            field(
                "distributions",
                FeatureDataType::Money,
                FeatureUnit::CurrencyAmount,
            )?,
        ],
        InputFamily::Returns => vec![field(
            "returns",
            FeatureDataType::StatisticalF64,
            FeatureUnit::Return,
        )?],
        InputFamily::ReturnsAndRiskFree => vec![
            field(
                "returns",
                FeatureDataType::StatisticalF64,
                FeatureUnit::Return,
            )?,
            field(
                "risk_free_return",
                FeatureDataType::StatisticalF64,
                FeatureUnit::Return,
            )?,
        ],
        InputFamily::ReturnsAndTarget => vec![
            field(
                "returns",
                FeatureDataType::StatisticalF64,
                FeatureUnit::Return,
            )?,
            field(
                "target_return",
                FeatureDataType::StatisticalF64,
                FeatureUnit::Return,
            )?,
        ],
        InputFamily::WeightedReturns => vec![
            field(
                "returns",
                FeatureDataType::StatisticalF64,
                FeatureUnit::Return,
            )?,
            field(
                "weights",
                FeatureDataType::StatisticalF64,
                FeatureUnit::Unitless,
            )?,
        ],
        InputFamily::PairedReturns => vec![
            field(
                "asset_returns",
                FeatureDataType::StatisticalF64,
                FeatureUnit::Return,
            )?,
            field(
                "benchmark_returns",
                FeatureDataType::StatisticalF64,
                FeatureUnit::Return,
            )?,
        ],
        InputFamily::MeanAndDeviation => vec![
            field(
                "mean",
                FeatureDataType::StatisticalLocation,
                FeatureUnit::Return,
            )?,
            field(
                "standard_deviation",
                FeatureDataType::StatisticalDispersion,
                FeatureUnit::Volatility,
            )?,
        ],
        InputFamily::FactorReturns => vec![
            field(
                "factor_identifiers",
                FeatureDataType::CanonicalIdentifier,
                FeatureUnit::Unitless,
            )?,
            field(
                "asset_returns",
                FeatureDataType::StatisticalF64,
                FeatureUnit::Return,
            )?,
            field(
                "factor_returns",
                FeatureDataType::StatisticalF64,
                FeatureUnit::Return,
            )?,
        ],
        InputFamily::MoneyPair => vec![
            field(
                "numerator",
                FeatureDataType::MonetaryValue,
                FeatureUnit::CurrencyAmount,
            )?,
            field(
                "denominator",
                FeatureDataType::MonetaryValue,
                FeatureUnit::CurrencyAmount,
            )?,
        ],
        InputFamily::FundamentalPeriod => vec![
            field(
                "operating_cash_flow",
                FeatureDataType::MonetaryValue,
                FeatureUnit::CurrencyAmount,
            )?,
            field(
                "capital_expenditure",
                FeatureDataType::MonetaryValue,
                FeatureUnit::CurrencyAmount,
            )?,
        ],
        InputFamily::RateCurve => vec![
            field(
                "maturity_days",
                FeatureDataType::UnsignedInteger,
                FeatureUnit::Count,
            )?,
            field("rates", FeatureDataType::ExactRate, FeatureUnit::Rate)?,
        ],
        InputFamily::RateCurvePair => vec![
            field(
                "maturity_days",
                FeatureDataType::UnsignedInteger,
                FeatureUnit::Count,
            )?,
            field("prior_rates", FeatureDataType::ExactRate, FeatureUnit::Rate)?,
            field(
                "current_rates",
                FeatureDataType::ExactRate,
                FeatureUnit::Rate,
            )?,
        ],
        InputFamily::MacroSurprise => vec![
            field(
                "actual",
                FeatureDataType::DecimalMeasurement,
                FeatureUnit::Unitless,
            )?,
            field(
                "consensus",
                FeatureDataType::DecimalMeasurement,
                FeatureUnit::Unitless,
            )?,
            field(
                "scale",
                FeatureDataType::DecimalMeasurement,
                FeatureUnit::Unitless,
            )?,
        ],
        InputFamily::MacroComponent => {
            vec![field("value", FeatureDataType::Decimal, FeatureUnit::Rate)?]
        }
        InputFamily::Portfolio => vec![
            field(
                "allocation_dimensions",
                FeatureDataType::CanonicalIdentifier,
                FeatureUnit::Unitless,
            )?,
            field(
                "market_values",
                FeatureDataType::MonetaryValue,
                FeatureUnit::CurrencyAmount,
            )?,
            field(
                "return_rates",
                FeatureDataType::ExactRate,
                FeatureUnit::Rate,
            )?,
        ],
        InputFamily::PortfolioAndScenario => vec![
            field(
                "allocation_dimensions",
                FeatureDataType::CanonicalIdentifier,
                FeatureUnit::Unitless,
            )?,
            field(
                "market_values",
                FeatureDataType::MonetaryValue,
                FeatureUnit::CurrencyAmount,
            )?,
            field(
                "shock_dimensions",
                FeatureDataType::CanonicalIdentifier,
                FeatureUnit::Unitless,
            )?,
            field(
                "return_shocks",
                FeatureDataType::ExactRate,
                FeatureUnit::Rate,
            )?,
        ],
    };
    FeatureInputSchema::try_new(fields)
}

fn parameters(
    family: ParameterFamily,
    config: BatchFeatureCatalogConfig,
) -> Result<FeatureParameters, FeatureMetadataError> {
    let entries = match family {
        ParameterFamily::None => Vec::new(),
        ParameterFamily::Annualized => vec![unsigned_parameter(
            "periods_per_year",
            u64::from(config.periods_per_year.get()),
        )?],
        ParameterFamily::Volatility => vec![
            unsigned_parameter("periods_per_year", u64::from(config.periods_per_year.get()))?,
            FeatureParameter::try_new(
                "variance_convention",
                FeatureParameterValue::VarianceConvention(config.policies.variance),
            )?,
            FeatureParameter::try_new(
                "missing_value_policy",
                FeatureParameterValue::MissingValuePolicy(config.policies.missing),
            )?,
        ],
        ParameterFamily::Variance => vec![FeatureParameter::try_new(
            "variance_convention",
            FeatureParameterValue::VarianceConvention(config.policies.variance),
        )?],
        ParameterFamily::Quantile => vec![unsigned_parameter(
            "confidence_parts_per_million",
            u64::from(config.confidence_parts_per_million.get()),
        )?],
        ParameterFamily::AnnualizedQuantile => vec![
            unsigned_parameter("periods_per_year", u64::from(config.periods_per_year.get()))?,
            unsigned_parameter(
                "confidence_parts_per_million",
                u64::from(config.confidence_parts_per_million.get()),
            )?,
        ],
        ParameterFamily::QuantileWeight => vec![
            unsigned_parameter(
                "confidence_parts_per_million",
                u64::from(config.confidence_parts_per_million.get()),
            )?,
            FeatureParameter::try_new(
                "weight_policy",
                FeatureParameterValue::WeightPolicy(config.policies.weight),
            )?,
        ],
        ParameterFamily::Decimal => vec![
            unsigned_parameter("decimal_scale", u64::from(config.decimal_scale))?,
            FeatureParameter::try_new(
                "rounding_policy",
                FeatureParameterValue::RoundingPolicy(config.policies.rounding),
            )?,
        ],
        ParameterFamily::ShockComposition => vec![FeatureParameter::try_new(
            "shock_composition",
            FeatureParameterValue::ShockComposition(config.policies.shock_composition),
        )?],
    };
    FeatureParameters::try_new(entries)
}

fn field(
    name: &str,
    data_type: FeatureDataType,
    unit: FeatureUnit,
) -> Result<FeatureInput, FeatureMetadataError> {
    FeatureInput::try_new(name, data_type, unit, false)
}

fn unsigned_parameter(name: &str, value: u64) -> Result<FeatureParameter, FeatureMetadataError> {
    FeatureParameter::try_new(name, FeatureParameterValue::UnsignedInteger(value))
}
