//! Preallocated route-owned live feature state and authority-free feature views.

use std::mem::size_of;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};

use market_squawk_analytics::{
    FeatureError, FeatureKey, FeatureRegistry, FeatureRegistryError, FeatureScalar,
    FeatureValidity, FeatureValue, LiveFeatureCatalog, LiveFeatureCatalogConfig, LiveFeatureView,
    REQUIRED_LIVE_FEATURE_COUNT, RequiredLiveFeature, RollingFeatureError, RollingFeatureState,
    RollingWindowConfig,
};
use market_squawk_domain::{BookLevel, ConnectionGeneration, DataQuality, Timestamp};
use market_squawk_sources::CurrentStreamKey;
use thiserror::Error;

use crate::DepthLimit;
use crate::runtime::LiveFeatureCapacity;

#[path = "features/snapshot.rs"]
mod snapshot;
#[path = "features/state.rs"]
mod state;
#[path = "features/trade_window.rs"]
mod trade_window;

pub(crate) use state::CommittedFeatureInput;
use trade_window::AggressorWindow;

const MINIMUM_ROLLING_OBSERVATIONS: usize = 3;
const ROLLING_DURATION_NANOS: u64 = 60_000_000_000;
const CROSS_VENUE_SKEW_NANOS: u64 = 1_000_000_000;
const FEATURE_REGISTRY_BYTES: usize = 1024 * 1024;
const FEATURE_IMPLEMENTATION_REVISION: &str = "market-squawk-live-v1";

/// Why computed values were invalidated without granting or restoring authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FeatureInvalidationReason {
    GenerationReplacement,
    Resynchronization,
    Quarantined,
    TradingHalt,
    SourceReplacement,
    TimestampRegression,
    Overflow,
}

impl FeatureInvalidationReason {
    const fn validity(self) -> FeatureValidity {
        match self {
            Self::TimestampRegression => FeatureValidity::TimestampRegression,
            Self::Overflow => FeatureValidity::Overflow,
            Self::GenerationReplacement
            | Self::Resynchronization
            | Self::Quarantined
            | Self::TradingHalt
            | Self::SourceReplacement => FeatureValidity::Unavailable,
        }
    }
}

/// Non-authoritative result of one post-commit route feature update.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FeatureUpdateDisposition {
    Updated,
    Unavailable,
    Overflow,
}

/// Every preallocated stream feature set owned by one venue/instrument route.
#[derive(Debug)]
pub struct RouteFeatureState {
    registry: FeatureRegistry,
    slots: Box<[FeatureSetState]>,
    maximum_window_bytes: usize,
    retained_bytes: usize,
}

impl RouteFeatureState {
    pub(crate) fn try_new(
        capacity: LiveFeatureCapacity,
        depth: DepthLimit,
    ) -> Result<Self, RouteFeatureError> {
        let observations = capacity.maximum_feature_window_observations_per_route.get();
        if observations < MINIMUM_ROLLING_OBSERVATIONS {
            return Err(RouteFeatureError::RollingCapacityTooSmall);
        }
        let observations_u32 = NonZeroU32::new(
            u32::try_from(observations).map_err(|_| RouteFeatureError::CapacityOverflow)?,
        )
        .ok_or(RouteFeatureError::CapacityOverflow)?;
        let catalog = LiveFeatureCatalog::try_new(
            LiveFeatureCatalogConfig::try_new(
                nonzero_u32(depth.get())?,
                observations_u32,
                observations_u32,
                nonzero_u32(MINIMUM_ROLLING_OBSERVATIONS)?,
                NonZeroU64::new(ROLLING_DURATION_NANOS)
                    .ok_or(RouteFeatureError::CapacityOverflow)?,
                nonzero_u32(capacity.maximum_venues_per_cross_venue_instrument.get())?,
                NonZeroU64::new(CROSS_VENUE_SKEW_NANOS)
                    .ok_or(RouteFeatureError::CapacityOverflow)?,
            )?,
            FEATURE_IMPLEMENTATION_REVISION,
        )?;
        let mut registry = FeatureRegistry::try_new(
            NonZeroUsize::new(REQUIRED_LIVE_FEATURE_COUNT)
                .ok_or(RouteFeatureError::CapacityOverflow)?,
            NonZeroUsize::new(FEATURE_REGISTRY_BYTES).ok_or(RouteFeatureError::CapacityOverflow)?,
        )?;
        catalog.try_register(&mut registry)?;
        let rolling = RollingWindowConfig::try_new(
            capacity.maximum_feature_window_observations_per_route,
            NonZeroUsize::new(MINIMUM_ROLLING_OBSERVATIONS)
                .ok_or(RouteFeatureError::CapacityOverflow)?,
            NonZeroU64::new(ROLLING_DURATION_NANOS).ok_or(RouteFeatureError::CapacityOverflow)?,
            capacity.maximum_feature_window_bytes_per_route,
        )?;
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(capacity.maximum_feature_sets_per_route.get())
            .map_err(|_| RouteFeatureError::Allocation)?;
        for _ in 0..capacity.maximum_feature_sets_per_route.get() {
            slots.push(FeatureSetState::try_new(
                rolling,
                observations,
                depth.get(),
            )?);
        }
        let slots = slots.into_boxed_slice();
        let retained_bytes = retained_state_bytes(&registry, &slots)?;
        let retained_window_bytes = slots.iter().try_fold(0_usize, |total, slot| {
            total
                .checked_add(
                    dynamic_feature_set_bytes(slot).ok_or(RouteFeatureError::CapacityOverflow)?,
                )
                .ok_or(RouteFeatureError::CapacityOverflow)
        })?;
        let configured = capacity.maximum_feature_window_bytes_per_route.get();
        if retained_window_bytes > configured {
            return Err(RouteFeatureError::WindowByteCapacityExceeded {
                required: retained_window_bytes,
                configured,
            });
        }
        Ok(Self {
            registry,
            slots,
            maximum_window_bytes: configured,
            retained_bytes,
        })
    }

    pub(crate) fn invalidate_all(
        &mut self,
        reason: FeatureInvalidationReason,
        observed_at: Timestamp,
    ) -> Result<(), RouteFeatureError> {
        for slot in &mut self.slots {
            if slot.identity.is_some() {
                slot.reset(reason, observed_at)?;
            }
        }
        Ok(())
    }

    pub(crate) fn active_sets(&self) -> impl Iterator<Item = &FeatureSetState> {
        self.slots.iter().filter(|slot| slot.identity.is_some())
    }

    pub(crate) fn action_view(
        &self,
        stream: &CurrentStreamKey,
        generation: ConnectionGeneration,
    ) -> Result<&FeatureSetState, RouteFeatureError> {
        let slot = self
            .slots
            .iter()
            .find(|slot| slot.identity.as_ref() == Some(stream))
            .ok_or(RouteFeatureError::InternalStateInvariant)?;
        if slot.generation != Some(generation) {
            return Err(RouteFeatureError::InternalStateInvariant);
        }
        Ok(slot)
    }

    pub(crate) fn cross_venue_midpoint(
        &self,
        stream: &CurrentStreamKey,
        generation: ConnectionGeneration,
    ) -> Result<Option<market_squawk_analytics::ExactFeatureRatio>, RouteFeatureError> {
        let slot = self
            .slots
            .iter()
            .find(|slot| slot.identity.as_ref() == Some(stream))
            .ok_or(RouteFeatureError::InternalStateInvariant)?;
        if slot.generation != Some(generation) {
            return Err(RouteFeatureError::InternalStateInvariant);
        }
        match slot.values[feature_index(RequiredLiveFeature::Midpoint)].value() {
            Some(FeatureScalar::HalfTickPrice(value)) => Ok(Some(
                market_squawk_analytics::ExactFeatureRatio::try_new(value.half_ticks(), 2)?,
            )),
            Some(_) => Err(RouteFeatureError::InternalStateInvariant),
            None => Ok(None),
        }
    }

    pub(crate) fn apply_cross_venue(
        &mut self,
        stream: &CurrentStreamKey,
        generation: ConnectionGeneration,
        value: FeatureValue<market_squawk_analytics::ExactFeatureRatio>,
    ) -> Result<(), RouteFeatureError> {
        let slot = self
            .slots
            .iter_mut()
            .find(|slot| slot.identity.as_ref() == Some(stream))
            .ok_or(RouteFeatureError::InternalStateInvariant)?;
        if slot.generation != Some(generation) {
            return Err(RouteFeatureError::InternalStateInvariant);
        }
        slot.set(
            RequiredLiveFeature::CrossVenueDivergence,
            map_exact_ratio(value)?,
        );
        Ok(())
    }

    pub(crate) const fn registry(&self) -> &FeatureRegistry {
        &self.registry
    }

    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    pub const fn retained_byte_limit(&self) -> usize {
        self.maximum_window_bytes
    }

    fn slot_index(
        &mut self,
        stream: &CurrentStreamKey,
        generation: ConnectionGeneration,
    ) -> Result<usize, RouteFeatureError> {
        if let Some(index) = self
            .slots
            .iter()
            .position(|slot| slot.identity.as_ref() == Some(stream))
        {
            let slot = self
                .slots
                .get_mut(index)
                .ok_or(RouteFeatureError::InternalStateInvariant)?;
            if slot.generation != Some(generation) {
                slot.reset(
                    FeatureInvalidationReason::GenerationReplacement,
                    Timestamp::from_unix_nanos(0),
                )?;
                slot.generation = Some(generation);
            }
            return Ok(index);
        }
        let Some(index) = self.slots.iter().position(|slot| slot.identity.is_none()) else {
            return Err(RouteFeatureError::FeatureSetCapacityExceeded);
        };
        let slot = self
            .slots
            .get_mut(index)
            .ok_or(RouteFeatureError::InternalStateInvariant)?;
        slot.identity = Some(stream.clone());
        slot.generation = Some(generation);
        Ok(index)
    }
}

#[derive(Debug)]
pub(crate) struct FeatureSetState {
    identity: Option<CurrentStreamKey>,
    generation: Option<ConnectionGeneration>,
    rolling: RollingFeatureState,
    trades: AggressorWindow,
    bids: Vec<BookLevel>,
    asks: Vec<BookLevel>,
    bid_views: Vec<market_squawk_analytics::PriceLevelView>,
    ask_views: Vec<market_squawk_analytics::PriceLevelView>,
    previous_top: Option<market_squawk_analytics::TopOfBookView>,
    values: [FeatureValue<FeatureScalar>; REQUIRED_LIVE_FEATURE_COUNT],
    requires_snapshot: bool,
    last_observed_at: Option<Timestamp>,
    last_quality: DataQuality,
}

impl FeatureSetState {
    fn try_new(
        rolling: RollingWindowConfig,
        observations: usize,
        depth: usize,
    ) -> Result<Self, RouteFeatureError> {
        Ok(Self {
            identity: None,
            generation: None,
            rolling: RollingFeatureState::try_new(rolling)?,
            trades: AggressorWindow::try_new(observations)?,
            bids: reserved(depth)?,
            asks: reserved(depth)?,
            bid_views: reserved(depth)?,
            ask_views: reserved(depth)?,
            previous_top: None,
            values: invalid_values(FeatureValidity::Unavailable, Timestamp::from_unix_nanos(0))?,
            requires_snapshot: true,
            last_observed_at: None,
            last_quality: DataQuality::Quarantined,
        })
    }

    fn reset(
        &mut self,
        reason: FeatureInvalidationReason,
        observed_at: Timestamp,
    ) -> Result<(), RouteFeatureError> {
        self.rolling.reset();
        self.trades.reset();
        self.bids.clear();
        self.asks.clear();
        self.bid_views.clear();
        self.ask_views.clear();
        self.previous_top = None;
        self.values = invalid_values(reason.validity(), observed_at)?;
        self.requires_snapshot = true;
        self.last_observed_at = None;
        self.last_quality = DataQuality::Quarantined;
        Ok(())
    }

    pub(crate) const fn identity(&self) -> Option<&CurrentStreamKey> {
        self.identity.as_ref()
    }

    pub(crate) const fn generation(&self) -> Option<ConnectionGeneration> {
        self.generation
    }

    pub(crate) fn values(&self) -> &[FeatureValue<FeatureScalar>; REQUIRED_LIVE_FEATURE_COUNT] {
        &self.values
    }

    #[allow(
        dead_code,
        reason = "Task 11 action integration consumes committed bounded depth"
    )]
    pub(crate) fn bids(&self) -> &[BookLevel] {
        &self.bids
    }

    #[allow(
        dead_code,
        reason = "Task 11 action integration consumes committed bounded depth"
    )]
    pub(crate) fn asks(&self) -> &[BookLevel] {
        &self.asks
    }

    #[allow(
        dead_code,
        reason = "Task 11 action integration supplies the configured requirement set"
    )]
    pub(crate) fn required_ready(&self, required: &[RequiredLiveFeature]) -> bool {
        self.last_quality == DataQuality::DirectVerified
            && required
                .iter()
                .all(|feature| self.values[feature_index(*feature)].validity().is_ready())
    }
}

impl LiveFeatureView for FeatureSetState {
    fn feature(&self, key: &FeatureKey) -> Option<&FeatureValue<FeatureScalar>> {
        RequiredLiveFeature::ALL
            .iter()
            .position(|feature| feature.name() == key.name() && key.version() == NonZeroU32::MIN)
            .and_then(|index| self.values.get(index))
    }

    fn retained_bytes(&self) -> Result<usize, FeatureError> {
        retained_feature_set_bytes(self).ok_or(FeatureError::RetainedSizeOverflow)
    }
}

fn nonzero_u32(value: usize) -> Result<NonZeroU32, RouteFeatureError> {
    NonZeroU32::new(u32::try_from(value).map_err(|_| RouteFeatureError::CapacityOverflow)?)
        .ok_or(RouteFeatureError::CapacityOverflow)
}

fn reserved<T>(capacity: usize) -> Result<Vec<T>, RouteFeatureError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(capacity)
        .map_err(|_| RouteFeatureError::Allocation)?;
    Ok(values)
}

fn invalid_values(
    validity: FeatureValidity,
    observed_at: Timestamp,
) -> Result<[FeatureValue<FeatureScalar>; REQUIRED_LIVE_FEATURE_COUNT], FeatureError> {
    let value = FeatureValue::invalid(validity, observed_at)?;
    Ok(std::array::from_fn(|_| value.clone()))
}

fn map_exact_ratio(
    value: FeatureValue<market_squawk_analytics::ExactFeatureRatio>,
) -> Result<FeatureValue<FeatureScalar>, FeatureError> {
    match value.ready_value() {
        Some(inner) => Ok(FeatureValue::ready(
            FeatureScalar::ExactRatio(inner),
            value.observed_at(),
        )),
        None => FeatureValue::invalid(value.validity(), value.observed_at()),
    }
}

const fn feature_index(feature: RequiredLiveFeature) -> usize {
    match feature {
        RequiredLiveFeature::Spread => 0,
        RequiredLiveFeature::Midpoint => 1,
        RequiredLiveFeature::Microprice => 2,
        RequiredLiveFeature::BookImbalance => 3,
        RequiredLiveFeature::OrderFlowImbalance => 4,
        RequiredLiveFeature::DepthWeightedPrice => 5,
        RequiredLiveFeature::AggressorImbalance => 6,
        RequiredLiveFeature::RollingVwap => 7,
        RequiredLiveFeature::VolumeVelocity => 8,
        RequiredLiveFeature::Momentum => 9,
        RequiredLiveFeature::RollingReturn => 10,
        RequiredLiveFeature::RollingVolatility => 11,
        RequiredLiveFeature::CrossVenueDivergence => 12,
        RequiredLiveFeature::AvailableLiquidity => 13,
        RequiredLiveFeature::Slippage => 14,
    }
}

fn retained_state_bytes(
    registry: &FeatureRegistry,
    slots: &[FeatureSetState],
) -> Result<usize, RouteFeatureError> {
    slots.iter().try_fold(
        size_of::<RouteFeatureState>()
            .checked_add(registry.retained_bytes())
            .ok_or(RouteFeatureError::CapacityOverflow)?,
        |total, slot| {
            total
                .checked_add(
                    retained_feature_set_bytes(slot).ok_or(RouteFeatureError::CapacityOverflow)?,
                )
                .ok_or(RouteFeatureError::CapacityOverflow)
        },
    )
}

fn retained_feature_set_bytes(slot: &FeatureSetState) -> Option<usize> {
    size_of::<FeatureSetState>()
        .checked_add(market_squawk_domain::SourceId::MAX_LENGTH)?
        .checked_add(market_squawk_domain::VenueId::MAX_LENGTH)?
        .checked_add(2 * market_squawk_domain::SourceIdentifier::MAX_LENGTH)?
        .checked_add(dynamic_feature_set_bytes(slot)?)
}

fn dynamic_feature_set_bytes(slot: &FeatureSetState) -> Option<usize> {
    slot.rolling
        .retained_bytes()
        .checked_sub(size_of::<RollingFeatureState>())?
        .checked_add(slot.trades.retained_bytes())?
        .checked_add(slot.bids.capacity().checked_mul(size_of::<BookLevel>())?)?
        .checked_add(slot.asks.capacity().checked_mul(size_of::<BookLevel>())?)?
        .checked_add(
            slot.bid_views
                .capacity()
                .checked_mul(size_of::<market_squawk_analytics::PriceLevelView>())?,
        )?
        .checked_add(
            slot.ask_views
                .capacity()
                .checked_mul(size_of::<market_squawk_analytics::PriceLevelView>())?,
        )
}

/// Route feature construction, capacity, or pure-kernel failure.
#[derive(Debug, Error)]
pub enum RouteFeatureError {
    #[error("route feature rolling capacity must be at least three observations")]
    RollingCapacityTooSmall,
    #[error("route feature-set capacity is exhausted")]
    FeatureSetCapacityExceeded,
    #[error("route feature state requires {required} bytes but only {configured} were configured")]
    WindowByteCapacityExceeded { required: usize, configured: usize },
    #[error("route feature capacity arithmetic overflowed")]
    CapacityOverflow,
    #[error("route feature preallocation failed")]
    Allocation,
    #[error("route feature internal state invariant failed")]
    InternalStateInvariant,
    #[error("route feature snapshot construction failed")]
    SnapshotConstruction,
    #[error(transparent)]
    CatalogConfig(#[from] market_squawk_analytics::LiveFeatureCatalogConfigError),
    #[error(transparent)]
    Metadata(#[from] market_squawk_analytics::FeatureMetadataError),
    #[error(transparent)]
    Registry(#[from] FeatureRegistryError),
    #[error(transparent)]
    Rolling(#[from] RollingFeatureError),
    #[error(transparent)]
    BookFeature(#[from] market_squawk_analytics::BookFeatureError),
    #[error(transparent)]
    TradeFeature(#[from] market_squawk_analytics::TradeFeatureError),
    #[error(transparent)]
    Feature(#[from] FeatureError),
    #[error(transparent)]
    Market(#[from] market_squawk_domain::MarketEventError),
}

impl RouteFeatureError {
    fn is_expected_feature_failure(&self) -> bool {
        matches!(
            self,
            Self::Rolling(_)
                | Self::BookFeature(_)
                | Self::TradeFeature(_)
                | Self::Feature(_)
                | Self::Market(_)
        )
    }
}
