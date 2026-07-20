//! Canonical metadata catalog for every required production live feature.

use std::num::{NonZeroU32, NonZeroU64};

use crate::{
    BatchRegistrationOutcome, FeatureDataType, FeatureInput, FeatureInputSchema, FeatureKey,
    FeatureMetadata, FeatureMetadataError, FeatureNullPolicy, FeatureOutputType, FeatureParameter,
    FeatureParameterValue, FeatureParameters, FeatureRegistry, FeatureRegistryError,
    FeatureTimeSemantics, FeatureUnit, FeatureWarmUp, LiveFeatureCatalogConfig,
};

/// Number of mandatory live feature definitions in the production catalog.
pub const REQUIRED_LIVE_FEATURE_COUNT: usize = 15;

/// Closed identity set for the mandatory production live features.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RequiredLiveFeature {
    /// Best-ask minus best-bid price ticks.
    Spread,
    /// Exact top-of-book midpoint in half-tick units.
    Midpoint,
    /// Exact quantity-weighted top-of-book microprice.
    Microprice,
    /// Exact top-of-book quantity imbalance.
    BookImbalance,
    /// Signed top-of-book order-flow imbalance.
    OrderFlowImbalance,
    /// Exact price weighted across bounded displayed depth.
    DepthWeightedPrice,
    /// Exact classified-trade aggressor imbalance.
    AggressorImbalance,
    /// Exact rolling volume-weighted average price.
    RollingVwap,
    /// Exact rolling traded lots per second.
    VolumeVelocity,
    /// Exact rolling price-tick momentum.
    Momentum,
    /// Statistical rolling return.
    RollingReturn,
    /// Statistical rolling return volatility.
    RollingVolatility,
    /// Exact complete-set cross-venue divergence.
    CrossVenueDivergence,
    /// Available displayed quantity for a side-aware depth walk.
    AvailableLiquidity,
    /// Exact adverse slippage for a side-aware depth walk.
    Slippage,
}

impl RequiredLiveFeature {
    /// Every required feature in canonical producer registration order.
    pub const ALL: [Self; REQUIRED_LIVE_FEATURE_COUNT] = [
        Self::Spread,
        Self::Midpoint,
        Self::Microprice,
        Self::BookImbalance,
        Self::OrderFlowImbalance,
        Self::DepthWeightedPrice,
        Self::AggressorImbalance,
        Self::RollingVwap,
        Self::VolumeVelocity,
        Self::Momentum,
        Self::RollingReturn,
        Self::RollingVolatility,
        Self::CrossVenueDivergence,
        Self::AvailableLiquidity,
        Self::Slippage,
    ];

    /// Returns the stable bounded catalog name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Spread => "book.spread",
            Self::Midpoint => "book.midpoint",
            Self::Microprice => "book.microprice",
            Self::BookImbalance => "book.imbalance",
            Self::OrderFlowImbalance => "book.order-flow-imbalance",
            Self::DepthWeightedPrice => "book.depth-weighted-price",
            Self::AggressorImbalance => "trade.aggressor-imbalance",
            Self::RollingVwap => "trade.rolling-vwap",
            Self::VolumeVelocity => "trade.volume-velocity",
            Self::Momentum => "trade.momentum",
            Self::RollingReturn => "trade.rolling-return",
            Self::RollingVolatility => "trade.rolling-volatility",
            Self::CrossVenueDivergence => "cross-venue.divergence",
            Self::AvailableLiquidity => "liquidity.available-quantity",
            Self::Slippage => "liquidity.slippage",
        }
    }

    const fn index(self) -> usize {
        match self {
            Self::Spread => 0,
            Self::Midpoint => 1,
            Self::Microprice => 2,
            Self::BookImbalance => 3,
            Self::OrderFlowImbalance => 4,
            Self::DepthWeightedPrice => 5,
            Self::AggressorImbalance => 6,
            Self::RollingVwap => 7,
            Self::VolumeVelocity => 8,
            Self::Momentum => 9,
            Self::RollingReturn => 10,
            Self::RollingVolatility => 11,
            Self::CrossVenueDivergence => 12,
            Self::AvailableLiquidity => 13,
            Self::Slippage => 14,
        }
    }
}

/// Immutable complete metadata catalog for the required live feature set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveFeatureCatalog {
    entries: [FeatureMetadata; REQUIRED_LIVE_FEATURE_COUNT],
}

impl LiveFeatureCatalog {
    /// Builds all required definitions with one bounded implementation revision.
    ///
    /// # Errors
    ///
    /// Returns a metadata error if a catalog invariant cannot be represented.
    pub fn try_new(
        config: LiveFeatureCatalogConfig,
        implementation_revision: &str,
    ) -> Result<Self, FeatureMetadataError> {
        let entries = [
            top_of_book_metadata(
                RequiredLiveFeature::Spread,
                false,
                FeatureOutputType::PriceTicks,
                FeatureUnit::PriceTicks,
                implementation_revision,
            )?,
            top_of_book_metadata(
                RequiredLiveFeature::Midpoint,
                false,
                FeatureOutputType::HalfTickPrice,
                FeatureUnit::PriceTicks,
                implementation_revision,
            )?,
            top_of_book_metadata(
                RequiredLiveFeature::Microprice,
                true,
                FeatureOutputType::ExactRatio,
                FeatureUnit::PriceTicks,
                implementation_revision,
            )?,
            book_imbalance_metadata(implementation_revision)?,
            order_flow_metadata(implementation_revision)?,
            depth_metadata(
                RequiredLiveFeature::DepthWeightedPrice,
                FeatureOutputType::ExactRatio,
                FeatureUnit::PriceTicks,
                config,
                implementation_revision,
            )?,
            aggressor_metadata(config, implementation_revision)?,
            rolling_metadata(
                RequiredLiveFeature::RollingVwap,
                FeatureOutputType::ExactRatio,
                FeatureUnit::PriceTicks,
                config.minimum_rolling_observations(),
                config,
                implementation_revision,
            )?,
            rolling_metadata(
                RequiredLiveFeature::VolumeVelocity,
                FeatureOutputType::ExactRatio,
                FeatureUnit::LotsPerSecond,
                config.minimum_rolling_observations(),
                config,
                implementation_revision,
            )?,
            rolling_metadata(
                RequiredLiveFeature::Momentum,
                FeatureOutputType::PriceTicks,
                FeatureUnit::PriceTicks,
                config.minimum_rolling_observations(),
                config,
                implementation_revision,
            )?,
            rolling_metadata(
                RequiredLiveFeature::RollingReturn,
                FeatureOutputType::StatisticalF64,
                FeatureUnit::Return,
                config.minimum_rolling_observations(),
                config,
                implementation_revision,
            )?,
            rolling_metadata(
                RequiredLiveFeature::RollingVolatility,
                FeatureOutputType::StatisticalF64,
                FeatureUnit::Volatility,
                nonzero_at_least(config.minimum_rolling_observations(), 3),
                config,
                implementation_revision,
            )?,
            cross_venue_metadata(config, implementation_revision)?,
            liquidity_metadata(
                RequiredLiveFeature::AvailableLiquidity,
                FeatureOutputType::SignedInteger,
                FeatureUnit::QuantityLots,
                config,
                implementation_revision,
            )?,
            liquidity_metadata(
                RequiredLiveFeature::Slippage,
                FeatureOutputType::ExactRatio,
                FeatureUnit::BasisPoints,
                config,
                implementation_revision,
            )?,
        ];
        Ok(Self { entries })
    }

    /// Returns all definitions in canonical producer registration order.
    #[must_use]
    pub const fn entries(&self) -> &[FeatureMetadata; REQUIRED_LIVE_FEATURE_COUNT] {
        &self.entries
    }

    /// Returns metadata for one closed required feature identity.
    #[must_use]
    pub const fn metadata(&self, feature: RequiredLiveFeature) -> &FeatureMetadata {
        &self.entries[feature.index()]
    }

    /// Atomically registers the complete catalog in an existing bounded registry.
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
}

fn top_of_book_metadata(
    feature: RequiredLiveFeature,
    include_quantities: bool,
    output_type: FeatureOutputType,
    unit: FeatureUnit,
    revision: &str,
) -> Result<FeatureMetadata, FeatureMetadataError> {
    let mut inputs = vec![
        input(
            "best_bid",
            FeatureDataType::PriceTicks,
            FeatureUnit::PriceTicks,
        )?,
        input(
            "best_ask",
            FeatureDataType::PriceTicks,
            FeatureUnit::PriceTicks,
        )?,
    ];
    if include_quantities {
        inputs.push(input(
            "best_bid_quantity",
            FeatureDataType::QuantityLots,
            FeatureUnit::QuantityLots,
        )?);
        inputs.push(input(
            "best_ask_quantity",
            FeatureDataType::QuantityLots,
            FeatureUnit::QuantityLots,
        )?);
    }
    inputs.push(observed_at()?);
    metadata(
        feature,
        inputs,
        vec![unsigned_parameter("book_levels", 1)?],
        FeatureTimeSemantics::EventTime,
        FeatureWarmUp::None,
        FeatureNullPolicy::Unavailable,
        output_type,
        unit,
        revision,
    )
}

fn book_imbalance_metadata(revision: &str) -> Result<FeatureMetadata, FeatureMetadataError> {
    metadata(
        RequiredLiveFeature::BookImbalance,
        vec![
            input(
                "best_bid_quantity",
                FeatureDataType::QuantityLots,
                FeatureUnit::QuantityLots,
            )?,
            input(
                "best_ask_quantity",
                FeatureDataType::QuantityLots,
                FeatureUnit::QuantityLots,
            )?,
            observed_at()?,
        ],
        vec![unsigned_parameter("book_levels", 1)?],
        FeatureTimeSemantics::EventTime,
        FeatureWarmUp::None,
        FeatureNullPolicy::Unavailable,
        FeatureOutputType::ExactRatio,
        FeatureUnit::Ratio,
        revision,
    )
}

fn order_flow_metadata(revision: &str) -> Result<FeatureMetadata, FeatureMetadataError> {
    let mut inputs = Vec::new();
    for prefix in ["previous", "current"] {
        inputs.push(input(
            &format!("{prefix}_best_bid"),
            FeatureDataType::PriceTicks,
            FeatureUnit::PriceTicks,
        )?);
        inputs.push(input(
            &format!("{prefix}_bid_quantity"),
            FeatureDataType::QuantityLots,
            FeatureUnit::QuantityLots,
        )?);
        inputs.push(input(
            &format!("{prefix}_best_ask"),
            FeatureDataType::PriceTicks,
            FeatureUnit::PriceTicks,
        )?);
        inputs.push(input(
            &format!("{prefix}_ask_quantity"),
            FeatureDataType::QuantityLots,
            FeatureUnit::QuantityLots,
        )?);
        inputs.push(input(
            &format!("{prefix}_observed_at"),
            FeatureDataType::Timestamp,
            FeatureUnit::Nanoseconds,
        )?);
    }
    metadata(
        RequiredLiveFeature::OrderFlowImbalance,
        inputs,
        vec![unsigned_parameter("minimum_observations", 2)?],
        FeatureTimeSemantics::EventTime,
        FeatureWarmUp::Observations(nonzero_at_least(NonZeroU32::MIN, 2)),
        FeatureNullPolicy::WarmingUp,
        FeatureOutputType::SignedInteger,
        FeatureUnit::QuantityLots,
        revision,
    )
}

fn depth_metadata(
    feature: RequiredLiveFeature,
    output_type: FeatureOutputType,
    unit: FeatureUnit,
    config: LiveFeatureCatalogConfig,
    revision: &str,
) -> Result<FeatureMetadata, FeatureMetadataError> {
    metadata(
        feature,
        vec![
            input(
                "bid_prices",
                FeatureDataType::PriceTicks,
                FeatureUnit::PriceTicks,
            )?,
            input(
                "bid_quantities",
                FeatureDataType::QuantityLots,
                FeatureUnit::QuantityLots,
            )?,
            input(
                "ask_prices",
                FeatureDataType::PriceTicks,
                FeatureUnit::PriceTicks,
            )?,
            input(
                "ask_quantities",
                FeatureDataType::QuantityLots,
                FeatureUnit::QuantityLots,
            )?,
            observed_at()?,
        ],
        vec![unsigned_parameter(
            "maximum_book_levels",
            u64::from(config.maximum_book_levels().get()),
        )?],
        FeatureTimeSemantics::EventTime,
        FeatureWarmUp::None,
        FeatureNullPolicy::Unavailable,
        output_type,
        unit,
        revision,
    )
}

fn liquidity_metadata(
    feature: RequiredLiveFeature,
    output_type: FeatureOutputType,
    unit: FeatureUnit,
    config: LiveFeatureCatalogConfig,
    revision: &str,
) -> Result<FeatureMetadata, FeatureMetadataError> {
    metadata(
        feature,
        vec![
            input(
                "bid_prices",
                FeatureDataType::PriceTicks,
                FeatureUnit::PriceTicks,
            )?,
            input(
                "bid_quantities",
                FeatureDataType::QuantityLots,
                FeatureUnit::QuantityLots,
            )?,
            input(
                "ask_prices",
                FeatureDataType::PriceTicks,
                FeatureUnit::PriceTicks,
            )?,
            input(
                "ask_quantities",
                FeatureDataType::QuantityLots,
                FeatureUnit::QuantityLots,
            )?,
            input(
                "order_side",
                FeatureDataType::OrderSide,
                FeatureUnit::Unitless,
            )?,
            input(
                "requested_quantity",
                FeatureDataType::QuantityLots,
                FeatureUnit::QuantityLots,
            )?,
            observed_at()?,
        ],
        vec![unsigned_parameter(
            "maximum_book_levels",
            u64::from(config.maximum_book_levels().get()),
        )?],
        FeatureTimeSemantics::EventTime,
        FeatureWarmUp::None,
        FeatureNullPolicy::Unavailable,
        output_type,
        unit,
        revision,
    )
}

fn aggressor_metadata(
    config: LiveFeatureCatalogConfig,
    revision: &str,
) -> Result<FeatureMetadata, FeatureMetadataError> {
    metadata(
        RequiredLiveFeature::AggressorImbalance,
        vec![
            input(
                "trade_quantities",
                FeatureDataType::QuantityLots,
                FeatureUnit::QuantityLots,
            )?,
            input(
                "aggressor_sides",
                FeatureDataType::AggressorSide,
                FeatureUnit::Unitless,
            )?,
            input(
                "observed_at",
                FeatureDataType::Timestamp,
                FeatureUnit::Nanoseconds,
            )?,
        ],
        vec![
            unsigned_parameter(
                "maximum_observations",
                u64::from(config.maximum_trade_observations().get()),
            )?,
            duration_parameter("window_duration_nanos", config.rolling_duration_nanos())?,
        ],
        FeatureTimeSemantics::TrailingWindow {
            duration_nanos: config.rolling_duration_nanos(),
        },
        FeatureWarmUp::Observations(NonZeroU32::MIN),
        FeatureNullPolicy::WarmingUp,
        FeatureOutputType::ExactRatio,
        FeatureUnit::Ratio,
        revision,
    )
}

fn rolling_metadata(
    feature: RequiredLiveFeature,
    output_type: FeatureOutputType,
    unit: FeatureUnit,
    warm_up: NonZeroU32,
    config: LiveFeatureCatalogConfig,
    revision: &str,
) -> Result<FeatureMetadata, FeatureMetadataError> {
    metadata(
        feature,
        vec![
            input(
                "trade_prices",
                FeatureDataType::PriceTicks,
                FeatureUnit::PriceTicks,
            )?,
            input(
                "trade_quantities",
                FeatureDataType::QuantityLots,
                FeatureUnit::QuantityLots,
            )?,
            observed_at()?,
        ],
        vec![
            unsigned_parameter(
                "maximum_observations",
                u64::from(config.maximum_rolling_observations().get()),
            )?,
            unsigned_parameter("minimum_observations", u64::from(warm_up.get()))?,
            duration_parameter("window_duration_nanos", config.rolling_duration_nanos())?,
        ],
        FeatureTimeSemantics::TrailingWindow {
            duration_nanos: config.rolling_duration_nanos(),
        },
        FeatureWarmUp::Observations(warm_up),
        FeatureNullPolicy::WarmingUp,
        output_type,
        unit,
        revision,
    )
}

fn cross_venue_metadata(
    config: LiveFeatureCatalogConfig,
    revision: &str,
) -> Result<FeatureMetadata, FeatureMetadataError> {
    metadata(
        RequiredLiveFeature::CrossVenueDivergence,
        vec![
            input("venue_ids", FeatureDataType::VenueId, FeatureUnit::Unitless)?,
            input(
                "midpoints",
                FeatureDataType::ExactRatio,
                FeatureUnit::PriceTicks,
            )?,
            input(
                "observed_at",
                FeatureDataType::Timestamp,
                FeatureUnit::Nanoseconds,
            )?,
        ],
        vec![
            unsigned_parameter(
                "maximum_venues",
                u64::from(config.maximum_cross_venue_observations().get()),
            )?,
            duration_parameter(
                "maximum_skew_nanos",
                config.maximum_cross_venue_skew_nanos(),
            )?,
        ],
        FeatureTimeSemantics::CrossVenue {
            maximum_skew_nanos: config.maximum_cross_venue_skew_nanos(),
        },
        FeatureWarmUp::Observations(nonzero_at_least(NonZeroU32::MIN, 2)),
        FeatureNullPolicy::Unavailable,
        FeatureOutputType::ExactRatio,
        FeatureUnit::BasisPoints,
        revision,
    )
}

#[allow(clippy::too_many_arguments)]
fn metadata(
    feature: RequiredLiveFeature,
    inputs: Vec<FeatureInput>,
    parameters: Vec<FeatureParameter>,
    time_semantics: FeatureTimeSemantics,
    warm_up: FeatureWarmUp,
    null_policy: FeatureNullPolicy,
    output_type: FeatureOutputType,
    unit: FeatureUnit,
    revision: &str,
) -> Result<FeatureMetadata, FeatureMetadataError> {
    FeatureMetadata::try_new(
        FeatureKey::try_new(feature.name(), NonZeroU32::MIN)?,
        FeatureInputSchema::try_new(inputs)?,
        FeatureParameters::try_new(parameters)?,
        time_semantics,
        warm_up,
        null_policy,
        output_type,
        unit,
        true,
        true,
        revision,
    )
}

fn input(
    name: &str,
    data_type: FeatureDataType,
    unit: FeatureUnit,
) -> Result<FeatureInput, FeatureMetadataError> {
    FeatureInput::try_new(name, data_type, unit, false)
}

fn observed_at() -> Result<FeatureInput, FeatureMetadataError> {
    input(
        "observed_at",
        FeatureDataType::Timestamp,
        FeatureUnit::Nanoseconds,
    )
}

fn unsigned_parameter(name: &str, value: u64) -> Result<FeatureParameter, FeatureMetadataError> {
    FeatureParameter::try_new(name, FeatureParameterValue::UnsignedInteger(value))
}

fn duration_parameter(
    name: &str,
    value: NonZeroU64,
) -> Result<FeatureParameter, FeatureMetadataError> {
    FeatureParameter::try_new(name, FeatureParameterValue::DurationNanos(value))
}

fn nonzero_at_least(value: NonZeroU32, minimum: u32) -> NonZeroU32 {
    NonZeroU32::new(value.get().max(minimum)).unwrap_or(value)
}
