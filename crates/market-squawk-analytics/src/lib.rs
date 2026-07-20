//! Bounded exact feature values and immutable registry contracts.

mod book;
mod catalog;
mod catalog_config;
mod cross_venue;
mod liquidity;
mod metadata;
mod registry;
mod rolling;
mod trade;
mod value;

pub use book::{
    BookDepthView, BookFeatureError, HalfTickPrice, MAX_BOOK_FEATURE_LEVELS, PriceLevelView,
    TopOfBookFeatures, TopOfBookView, depth_weighted_price, order_flow_imbalance,
    top_of_book_features,
};
pub use catalog::{LiveFeatureCatalog, REQUIRED_LIVE_FEATURE_COUNT, RequiredLiveFeature};
pub use catalog_config::{LiveFeatureCatalogConfig, LiveFeatureCatalogConfigError};
pub use cross_venue::{
    CrossVenueFeatureError, ExpectedVenueSet, MAX_CROSS_VENUE_OBSERVATIONS,
    VenueFeatureObservation, cross_venue_divergence,
};
pub use liquidity::{
    LiquidityBookView, LiquidityEstimate, LiquidityFeatureError, estimate_market_order,
};
pub use metadata::{
    FeatureDataType, FeatureInput, FeatureInputSchema, FeatureKey, FeatureMetadata,
    FeatureMetadataError, FeatureNullPolicy, FeatureOutputType, FeatureParameter,
    FeatureParameterValue, FeatureParameters, FeatureTimeSemantics, FeatureUnit, FeatureWarmUp,
    MAX_FEATURE_FIELD_NAME_BYTES, MAX_FEATURE_INPUTS, MAX_FEATURE_NAME_BYTES,
    MAX_FEATURE_PARAMETERS, MAX_IMPLEMENTATION_REVISION_BYTES,
};
pub use registry::{
    BatchRegistrationOutcome, FeatureRegistry, FeatureRegistryError, LiveFeatureView,
    MAX_FEATURE_REGISTRY_ENTRIES, MAX_FEATURE_REGISTRY_RETAINED_BYTES, RegistrationOutcome,
};
pub use rolling::{
    MAX_ROLLING_OBSERVATIONS, MAX_ROLLING_RETAINED_BYTES, RollingFeatureError, RollingFeatureState,
    RollingFeatureValues, RollingWindowConfig,
};
pub use trade::{
    MAX_TRADE_FEATURE_OBSERVATIONS, TradeFeatureError, TradeFeatureView, aggressor_imbalance,
};
pub use value::{
    ExactFeatureRatio, FeatureError, FeatureScalar, FeatureValidity, FeatureValue, StatisticalF64,
};
