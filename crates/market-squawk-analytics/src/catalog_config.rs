//! Validated runtime parameters for immutable live feature metadata.

use std::num::{NonZeroU32, NonZeroU64};

use thiserror::Error;

use crate::{
    MAX_BOOK_FEATURE_LEVELS, MAX_CROSS_VENUE_OBSERVATIONS, MAX_ROLLING_OBSERVATIONS,
    MAX_TRADE_FEATURE_OBSERVATIONS,
};

/// Validated runtime parameters that determine immutable live feature metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LiveFeatureCatalogConfig {
    maximum_book_levels: NonZeroU32,
    maximum_trade_observations: NonZeroU32,
    maximum_rolling_observations: NonZeroU32,
    minimum_rolling_observations: NonZeroU32,
    rolling_duration_nanos: NonZeroU64,
    maximum_cross_venue_observations: NonZeroU32,
    maximum_cross_venue_skew_nanos: NonZeroU64,
}

impl LiveFeatureCatalogConfig {
    /// Constructs catalog parameters within the exact pure-kernel production bounds.
    ///
    /// # Errors
    ///
    /// Returns a typed error for a count outside its kernel bound or rolling warm-up above retained
    /// rolling capacity.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        maximum_book_levels: NonZeroU32,
        maximum_trade_observations: NonZeroU32,
        maximum_rolling_observations: NonZeroU32,
        minimum_rolling_observations: NonZeroU32,
        rolling_duration_nanos: NonZeroU64,
        maximum_cross_venue_observations: NonZeroU32,
        maximum_cross_venue_skew_nanos: NonZeroU64,
    ) -> Result<Self, LiveFeatureCatalogConfigError> {
        if exceeds_usize_bound(maximum_book_levels, MAX_BOOK_FEATURE_LEVELS)? {
            return Err(LiveFeatureCatalogConfigError::BookLevelBoundTooLarge);
        }
        if exceeds_usize_bound(maximum_trade_observations, MAX_TRADE_FEATURE_OBSERVATIONS)? {
            return Err(LiveFeatureCatalogConfigError::TradeObservationBoundTooLarge);
        }
        if exceeds_usize_bound(maximum_rolling_observations, MAX_ROLLING_OBSERVATIONS)? {
            return Err(LiveFeatureCatalogConfigError::RollingObservationBoundTooLarge);
        }
        if minimum_rolling_observations > maximum_rolling_observations {
            return Err(LiveFeatureCatalogConfigError::RollingWarmUpExceedsCapacity);
        }
        if maximum_rolling_observations.get() < 3 {
            return Err(LiveFeatureCatalogConfigError::RollingCapacityTooSmall);
        }
        if maximum_cross_venue_observations.get() < 2 {
            return Err(LiveFeatureCatalogConfigError::CrossVenueBoundTooSmall);
        }
        if exceeds_usize_bound(
            maximum_cross_venue_observations,
            MAX_CROSS_VENUE_OBSERVATIONS,
        )? {
            return Err(LiveFeatureCatalogConfigError::CrossVenueBoundTooLarge);
        }
        Ok(Self {
            maximum_book_levels,
            maximum_trade_observations,
            maximum_rolling_observations,
            minimum_rolling_observations,
            rolling_duration_nanos,
            maximum_cross_venue_observations,
            maximum_cross_venue_skew_nanos,
        })
    }

    pub(crate) const fn maximum_book_levels(self) -> NonZeroU32 {
        self.maximum_book_levels
    }

    pub(crate) const fn maximum_trade_observations(self) -> NonZeroU32 {
        self.maximum_trade_observations
    }

    pub(crate) const fn maximum_rolling_observations(self) -> NonZeroU32 {
        self.maximum_rolling_observations
    }

    pub(crate) const fn minimum_rolling_observations(self) -> NonZeroU32 {
        self.minimum_rolling_observations
    }

    pub(crate) const fn rolling_duration_nanos(self) -> NonZeroU64 {
        self.rolling_duration_nanos
    }

    pub(crate) const fn maximum_cross_venue_observations(self) -> NonZeroU32 {
        self.maximum_cross_venue_observations
    }

    pub(crate) const fn maximum_cross_venue_skew_nanos(self) -> NonZeroU64 {
        self.maximum_cross_venue_skew_nanos
    }
}

/// Invalid production live feature catalog capacity configuration.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LiveFeatureCatalogConfigError {
    /// The book depth bound exceeded the pure-kernel limit.
    #[error("live feature catalog book-level bound is too large")]
    BookLevelBoundTooLarge,
    /// The classified-trade bound exceeded the pure-kernel limit.
    #[error("live feature catalog trade-observation bound is too large")]
    TradeObservationBoundTooLarge,
    /// The rolling bound exceeded the fixed-ring production limit.
    #[error("live feature catalog rolling-observation bound is too large")]
    RollingObservationBoundTooLarge,
    /// Rolling warm-up exceeded retained rolling capacity.
    #[error("live feature catalog rolling warm-up exceeds capacity")]
    RollingWarmUpExceedsCapacity,
    /// Rolling capacity could never satisfy volatility's three-observation minimum.
    #[error("live feature catalog rolling capacity is too small")]
    RollingCapacityTooSmall,
    /// Cross-venue capacity could never contain the required two venues.
    #[error("live feature catalog cross-venue bound is too small")]
    CrossVenueBoundTooSmall,
    /// The cross-venue bound exceeded the pure-kernel limit.
    #[error("live feature catalog cross-venue bound is too large")]
    CrossVenueBoundTooLarge,
    /// A target-platform count could not be represented as `u64`.
    #[error("live feature catalog platform count is not representable")]
    PlatformCountOverflow,
}

fn exceeds_usize_bound(
    value: NonZeroU32,
    maximum: usize,
) -> Result<bool, LiveFeatureCatalogConfigError> {
    Ok(u64::from(value.get())
        > u64::try_from(maximum)
            .map_err(|_| LiveFeatureCatalogConfigError::PlatformCountOverflow)?)
}
