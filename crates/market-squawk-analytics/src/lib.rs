//! Bounded exact feature values and immutable registry contracts.

mod batch;
mod batch_catalog;
mod book;
mod catalog;
mod catalog_config;
mod cross_venue;
mod exact;
mod factors;
mod fundamentals;
mod harmonics;
mod liquidity;
mod macro_features;
mod metadata;
mod registry;
mod returns;
mod risk;
mod rolling;
mod scenarios;
mod trade;
mod value;

pub use batch::{
    AnalyticsError, AnalyticsPolicy, Annualization, DatedMoney, DatedStatisticalInput,
    DecimalPolicy, InsufficientHistoryPolicy, MAX_ANALYTICS_IDENTIFIER_BYTES,
    MAX_BATCH_OBSERVATIONS, MAX_FACTOR_COUNT, MissingValuePolicy, Quantile, ReturnSeries,
    StatisticalDispersion, StatisticalInput, StatisticalLocation, StatisticalResult,
    StatisticalScale, StatisticalSeries, StatisticalUnit, VarianceConvention, WeightPolicy,
    WeightedStatisticalInput, resolve_optional_inputs,
};
pub use batch_catalog::{
    BatchFeatureCatalog, BatchFeatureCatalogConfig, BatchFeaturePolicies,
    REQUIRED_BATCH_FEATURE_COUNT,
};
pub use book::{
    BookDepthView, BookFeatureError, HalfTickPrice, MAX_BOOK_FEATURE_LEVELS, PriceLevelView,
    TopOfBookFeatures, TopOfBookView, depth_weighted_price, order_flow_imbalance,
    top_of_book_features,
};
pub use catalog::{
    KnownFeatureImplementation, LiveFeatureCatalog, REQUIRED_LIVE_FEATURE_COUNT,
    RequiredLiveFeature,
};
pub use catalog_config::{LiveFeatureCatalogConfig, LiveFeatureCatalogConfigError};
pub use cross_venue::{
    CrossVenueFeatureError, ExpectedVenueSet, MAX_CROSS_VENUE_OBSERVATIONS,
    VenueFeatureObservation, cross_venue_divergence,
};
pub use exact::{
    DecimalMeasurement, ExactDecimalResult, ExactDecimalScale, ExactDecimalUnit, ExactRate,
    MeasurementUnit, MonetaryBasis, MonetaryValue,
};
pub use factors::{FactorObservation, FactorRegressionResult, factor_regression};
pub use fundamentals::{
    FundamentalPeriod, earnings_surprise, free_cash_flow_yield, fundamental_growth, margin,
    valuation_multiple,
};
pub use harmonics::{
    HARMONIC_PIVOT_CONFIRMATION_BARS, HARMONIC_PIVOT_COUNT, HarmonicBar, HarmonicCompletionZone,
    HarmonicConfidenceAuthority, HarmonicDirection, HarmonicEvidenceBinding,
    HarmonicExecutionAuthority, HarmonicFraction, HarmonicPatternError, HarmonicPatternEvidence,
    HarmonicPatternInput, HarmonicPatternKind, HarmonicPatternRule, HarmonicPivot,
    HarmonicPivotEvidence, HarmonicPivotKind, HarmonicRatio, HarmonicRatioBand,
    HarmonicRatioConstraint, HarmonicRatioMeasurement, HarmonicRatioMeasurements,
    MAX_HARMONIC_BARS, classify_harmonic_pattern,
};
pub use liquidity::{
    LiquidityBookView, LiquidityEstimate, LiquidityFeatureError, estimate_market_order,
};
pub use macro_features::{
    RateChangeFeatures, RatePoint, YieldCurveFeatures, macro_surprise, yield_curve_change,
    yield_curve_features,
};
pub use metadata::{
    FeatureDataType, FeatureImplementationDigest, FeatureInput, FeatureInputSchema,
    FeatureInputSchemaDigest, FeatureKey, FeatureMetadata, FeatureMetadataError, FeatureNullPolicy,
    FeatureOutputType, FeatureParameter, FeatureParameterValue, FeatureParameters,
    FeatureSemanticDigest, FeatureTimeSemantics, FeatureUnit, FeatureWarmUp,
    MAX_FEATURE_FIELD_NAME_BYTES, MAX_FEATURE_INPUTS, MAX_FEATURE_NAME_BYTES,
    MAX_FEATURE_PARAMETERS, MAX_IMPLEMENTATION_REVISION_BYTES,
};
pub use registry::{
    BatchRegistrationOutcome, FeatureCompatibility, FeatureRegistry, FeatureRegistryError,
    LiveFeatureView, MAX_FEATURE_REGISTRY_ENTRIES, MAX_FEATURE_REGISTRY_RETAINED_BYTES,
    RegistrationOutcome,
};
pub use returns::{cumulative_return, simple_returns, total_returns};
pub use risk::{
    AlphaBetaResult, DrawdownResult, alpha_beta, correlation, discrete_expected_shortfall,
    historical_var, information_ratio, maximum_drawdown, parametric_var, sharpe_ratio,
    sortino_ratio, tracking_error, volatility, weighted_expected_shortfall,
};
pub use rolling::{
    MAX_ROLLING_OBSERVATIONS, MAX_ROLLING_RETAINED_BYTES, RollingFeatureError, RollingFeatureState,
    RollingFeatureValues, RollingWindowConfig,
};
pub use scenarios::{
    AttributionContribution, PortfolioAllocation, PortfolioAttribution, PortfolioExposure,
    ScenarioShock, ShockComposition, portfolio_attribution, portfolio_exposure, scenario_impact,
};
pub use trade::{
    MAX_TRADE_FEATURE_OBSERVATIONS, TradeFeatureError, TradeFeatureView, aggressor_imbalance,
};
pub use value::{
    ExactFeatureRatio, FeatureError, FeatureScalar, FeatureValidity, FeatureValue, StatisticalF64,
};
