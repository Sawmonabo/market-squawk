//! Preallocated rolling market-feature state with deterministic invalidation.

use std::mem::size_of;
use std::num::{NonZeroU64, NonZeroUsize};

use market_squawk_domain::{PriceTicks, Timestamp};
use thiserror::Error;

use crate::{
    ExactFeatureRatio, FeatureError, FeatureValidity, FeatureValue, StatisticalF64,
    TradeFeatureView,
};

/// Maximum observations retained by one rolling feature state.
pub const MAX_ROLLING_OBSERVATIONS: usize = 1_048_576;
/// Maximum retained-byte limit accepted by one rolling feature state.
pub const MAX_ROLLING_RETAINED_BYTES: usize = 256 * 1024 * 1024;

/// Validated observation, warm-up, duration, and retained-memory bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RollingWindowConfig {
    maximum_observations: NonZeroUsize,
    minimum_observations: NonZeroUsize,
    duration_nanos: NonZeroU64,
    retained_byte_limit: NonZeroUsize,
}

impl RollingWindowConfig {
    /// Constructs bounded rolling-window configuration.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the observation or byte ceiling exceeds its production bound, or
    /// when warm-up exceeds retained capacity.
    pub fn try_new(
        maximum_observations: NonZeroUsize,
        minimum_observations: NonZeroUsize,
        duration_nanos: NonZeroU64,
        retained_byte_limit: NonZeroUsize,
    ) -> Result<Self, RollingFeatureError> {
        if maximum_observations.get() > MAX_ROLLING_OBSERVATIONS {
            return Err(RollingFeatureError::ObservationBoundTooLarge);
        }
        if minimum_observations > maximum_observations {
            return Err(RollingFeatureError::WarmUpExceedsCapacity);
        }
        if retained_byte_limit.get() > MAX_ROLLING_RETAINED_BYTES {
            return Err(RollingFeatureError::RetainedByteLimitTooLarge);
        }
        Ok(Self {
            maximum_observations,
            minimum_observations,
            duration_nanos,
            retained_byte_limit,
        })
    }

    /// Returns the fixed observation capacity.
    #[must_use]
    pub const fn maximum_observations(self) -> NonZeroUsize {
        self.maximum_observations
    }

    /// Returns the minimum observations required for base features.
    #[must_use]
    pub const fn minimum_observations(self) -> NonZeroUsize {
        self.minimum_observations
    }

    /// Returns the inclusive trailing duration.
    #[must_use]
    pub const fn duration_nanos(self) -> NonZeroU64 {
        self.duration_nanos
    }

    /// Returns the complete retained-byte ceiling.
    #[must_use]
    pub const fn retained_byte_limit(self) -> NonZeroUsize {
        self.retained_byte_limit
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RollingObservation {
    price: PriceTicks,
    quantity: i64,
    observed_at: Timestamp,
}

/// Single-owner preallocated rolling feature state.
///
/// Construction performs the only allocation. Updates evict, overwrite, scan, and calculate within
/// the fixed boxed ring; they never grow or allocate retained state.
#[derive(Debug)]
pub struct RollingFeatureState {
    config: RollingWindowConfig,
    slots: Box<[Option<RollingObservation>]>,
    head: usize,
    len: usize,
    last_observed_at: Option<Timestamp>,
    retained_bytes: usize,
}

impl RollingFeatureState {
    /// Allocates the complete fixed ring after exact retained-size admission.
    ///
    /// # Errors
    ///
    /// Returns a typed error on size overflow or when the configured byte limit cannot hold the
    /// complete state graph.
    pub fn try_new(config: RollingWindowConfig) -> Result<Self, RollingFeatureError> {
        let retained_bytes = size_of::<Self>()
            .checked_add(
                size_of::<Option<RollingObservation>>()
                    .checked_mul(config.maximum_observations.get())
                    .ok_or(RollingFeatureError::RetainedSizeOverflow)?,
            )
            .ok_or(RollingFeatureError::RetainedSizeOverflow)?;
        if retained_bytes > config.retained_byte_limit.get() {
            return Err(RollingFeatureError::RetainedByteLimitTooSmall);
        }
        let slots = std::iter::repeat_with(|| None)
            .take(config.maximum_observations.get())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(Self {
            config,
            slots,
            head: 0,
            len: 0,
            last_observed_at: None,
            retained_bytes,
        })
    }

    /// Applies one trade and returns a complete immutable feature bundle.
    ///
    /// Timestamp regression clears the ring before returning regression validity for every output.
    /// Capacity eviction and duration eviction perform work bounded by configured capacity.
    ///
    /// # Errors
    ///
    /// Returns a typed error only if the private ring invariant or foundational value contract is
    /// violated. Expected arithmetic failures are represented as value-free `Overflow` outputs.
    pub fn update(
        &mut self,
        trade: TradeFeatureView,
    ) -> Result<RollingFeatureValues, RollingFeatureError> {
        if self
            .last_observed_at
            .is_some_and(|last| trade.observed_at() < last)
        {
            self.reset();
            return RollingFeatureValues::invalid(
                FeatureValidity::TimestampRegression,
                trade.observed_at(),
            );
        }

        self.evict_expired(trade.observed_at())?;
        if self.len == self.slots.len() {
            self.slots[self.head] = None;
            self.head = (self.head + 1) % self.slots.len();
            self.len -= 1;
        }
        let tail = (self.head + self.len) % self.slots.len();
        self.slots[tail] = Some(RollingObservation {
            price: trade.price(),
            quantity: trade.quantity().get(),
            observed_at: trade.observed_at(),
        });
        self.len += 1;
        self.last_observed_at = Some(trade.observed_at());

        if self.len < self.config.minimum_observations.get() {
            return RollingFeatureValues::invalid(FeatureValidity::WarmingUp, trade.observed_at());
        }
        self.calculate(trade.observed_at())
    }

    /// Returns the current observation count.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Returns whether no observation is retained.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the exact fixed retained footprint.
    #[must_use]
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    /// Clears every retained observation without releasing or reallocating the fixed ring.
    ///
    /// The configured capacity and exact retained footprint remain unchanged. The next update
    /// therefore begins the original warm-up policy from an empty state.
    pub fn reset(&mut self) {
        for slot in &mut self.slots {
            *slot = None;
        }
        self.head = 0;
        self.len = 0;
        self.last_observed_at = None;
    }

    fn calculate(
        &self,
        observed_at: Timestamp,
    ) -> Result<RollingFeatureValues, RollingFeatureError> {
        let oldest = self
            .observation(0)
            .ok_or(RollingFeatureError::InternalStateInvariant)?;
        let latest = self
            .observation(self.len - 1)
            .ok_or(RollingFeatureError::InternalStateInvariant)?;

        let mut price_quantity = 0_i128;
        let mut total_quantity = 0_i128;
        let mut vwap_overflow = false;
        let mut quantity_overflow = false;
        for index in 0..self.len {
            let observation = self
                .observation(index)
                .ok_or(RollingFeatureError::InternalStateInvariant)?;
            let weighted =
                i128::from(observation.price.get()).checked_mul(i128::from(observation.quantity));
            match weighted.and_then(|value| price_quantity.checked_add(value)) {
                Some(next) => price_quantity = next,
                None => vwap_overflow = true,
            }
            match total_quantity.checked_add(i128::from(observation.quantity)) {
                Some(next) => total_quantity = next,
                None => quantity_overflow = true,
            }
        }

        let vwap = if vwap_overflow || quantity_overflow {
            invalid(FeatureValidity::Overflow, observed_at)?
        } else {
            let quantity = u128::try_from(total_quantity)
                .map_err(|_| RollingFeatureError::InternalStateInvariant)?;
            FeatureValue::ready(
                ExactFeatureRatio::try_new(price_quantity, quantity)?,
                observed_at,
            )
        };
        let volume_velocity = if quantity_overflow {
            invalid(FeatureValidity::Overflow, observed_at)?
        } else {
            let elapsed = i128::from(latest.observed_at.unix_nanos())
                - i128::from(oldest.observed_at.unix_nanos());
            if elapsed == 0 {
                invalid(FeatureValidity::Unavailable, observed_at)?
            } else {
                match total_quantity.checked_mul(1_000_000_000) {
                    Some(numerator) => {
                        let denominator = u128::try_from(elapsed)
                            .map_err(|_| RollingFeatureError::InternalStateInvariant)?;
                        FeatureValue::ready(
                            ExactFeatureRatio::try_new(numerator, denominator)?,
                            observed_at,
                        )
                    }
                    None => invalid(FeatureValidity::Overflow, observed_at)?,
                }
            }
        };

        let momentum = match latest.price.get().checked_sub(oldest.price.get()) {
            Some(value) => FeatureValue::ready(PriceTicks::new(value), observed_at),
            None => invalid(FeatureValidity::Overflow, observed_at)?,
        };
        let rolling_return = statistical_return(oldest.price, latest.price, observed_at)?;
        let rolling_volatility = self.volatility(observed_at)?;
        Ok(RollingFeatureValues {
            vwap,
            volume_velocity,
            momentum,
            rolling_return,
            rolling_volatility,
        })
    }

    fn volatility(
        &self,
        observed_at: Timestamp,
    ) -> Result<FeatureValue<StatisticalF64>, RollingFeatureError> {
        if self.len < 3 {
            return invalid(FeatureValidity::WarmingUp, observed_at);
        }
        let mut count = 0_u32;
        let mut mean = 0.0_f64;
        let mut squared_deviations = 0.0_f64;
        for index in 1..self.len {
            let previous = self
                .observation(index - 1)
                .ok_or(RollingFeatureError::InternalStateInvariant)?;
            let current = self
                .observation(index)
                .ok_or(RollingFeatureError::InternalStateInvariant)?;
            if previous.price.get() == 0 {
                return invalid(FeatureValidity::Unavailable, observed_at);
            }
            let value = current.price.get() as f64 / previous.price.get() as f64 - 1.0;
            if !value.is_finite() {
                return invalid(FeatureValidity::Overflow, observed_at);
            }
            count += 1;
            let delta = value - mean;
            mean += delta / f64::from(count);
            let next_delta = value - mean;
            squared_deviations += delta * next_delta;
        }
        let variance = squared_deviations / f64::from(count);
        let value = StatisticalF64::try_new(variance.max(0.0).sqrt())?;
        Ok(FeatureValue::ready(value, observed_at))
    }

    fn evict_expired(&mut self, observed_at: Timestamp) -> Result<(), RollingFeatureError> {
        while self.len > 0 {
            let oldest = self
                .observation(0)
                .ok_or(RollingFeatureError::InternalStateInvariant)?;
            let age =
                i128::from(observed_at.unix_nanos()) - i128::from(oldest.observed_at.unix_nanos());
            if age <= i128::from(self.config.duration_nanos.get()) {
                break;
            }
            self.slots[self.head] = None;
            self.head = (self.head + 1) % self.slots.len();
            self.len -= 1;
        }
        Ok(())
    }

    fn observation(&self, logical_index: usize) -> Option<RollingObservation> {
        if logical_index >= self.len {
            return None;
        }
        self.slots[(self.head + logical_index) % self.slots.len()]
    }
}

/// Complete rolling output for one accepted observation.
#[derive(Clone, Debug, PartialEq)]
pub struct RollingFeatureValues {
    vwap: FeatureValue<ExactFeatureRatio>,
    volume_velocity: FeatureValue<ExactFeatureRatio>,
    momentum: FeatureValue<PriceTicks>,
    rolling_return: FeatureValue<StatisticalF64>,
    rolling_volatility: FeatureValue<StatisticalF64>,
}

impl RollingFeatureValues {
    /// Returns exact rolling VWAP.
    #[must_use]
    pub const fn vwap(&self) -> &FeatureValue<ExactFeatureRatio> {
        &self.vwap
    }

    /// Returns exact lots-per-second velocity.
    #[must_use]
    pub const fn volume_velocity(&self) -> &FeatureValue<ExactFeatureRatio> {
        &self.volume_velocity
    }

    /// Returns exact oldest-to-latest tick momentum.
    #[must_use]
    pub const fn momentum(&self) -> &FeatureValue<PriceTicks> {
        &self.momentum
    }

    /// Returns the explicit statistical oldest-to-latest return.
    #[must_use]
    pub const fn rolling_return(&self) -> &FeatureValue<StatisticalF64> {
        &self.rolling_return
    }

    /// Returns population volatility of consecutive simple returns.
    #[must_use]
    pub const fn rolling_volatility(&self) -> &FeatureValue<StatisticalF64> {
        &self.rolling_volatility
    }

    fn invalid(
        validity: FeatureValidity,
        observed_at: Timestamp,
    ) -> Result<Self, RollingFeatureError> {
        Ok(Self {
            vwap: invalid(validity, observed_at)?,
            volume_velocity: invalid(validity, observed_at)?,
            momentum: invalid(validity, observed_at)?,
            rolling_return: invalid(validity, observed_at)?,
            rolling_volatility: invalid(validity, observed_at)?,
        })
    }
}

/// Rolling feature configuration, state, or arithmetic failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RollingFeatureError {
    /// The observation bound exceeded the production maximum.
    #[error("rolling observation bound exceeds its production maximum")]
    ObservationBoundTooLarge,
    /// Warm-up observations exceeded retained capacity.
    #[error("rolling warm-up exceeds observation capacity")]
    WarmUpExceedsCapacity,
    /// The retained-byte limit exceeded the production maximum.
    #[error("rolling retained-byte limit exceeds its production maximum")]
    RetainedByteLimitTooLarge,
    /// The retained-byte limit could not hold the fixed ring.
    #[error("rolling retained-byte limit is below fixed state storage")]
    RetainedByteLimitTooSmall,
    /// Exact retained-size arithmetic overflowed.
    #[error("rolling retained-byte accounting overflowed")]
    RetainedSizeOverflow,
    /// Private fixed-ring invariants were not satisfied.
    #[error("rolling feature state invariant failed")]
    InternalStateInvariant,
    /// Foundational feature-state construction failed.
    #[error(transparent)]
    FeatureState(#[from] FeatureError),
}

fn statistical_return(
    oldest: PriceTicks,
    latest: PriceTicks,
    observed_at: Timestamp,
) -> Result<FeatureValue<StatisticalF64>, RollingFeatureError> {
    if oldest.get() == 0 {
        return invalid(FeatureValidity::Unavailable, observed_at);
    }
    let value = latest.get() as f64 / oldest.get() as f64 - 1.0;
    match StatisticalF64::try_new(value) {
        Ok(value) => Ok(FeatureValue::ready(value, observed_at)),
        Err(FeatureError::NonFiniteStatisticalValue) => {
            invalid(FeatureValidity::Overflow, observed_at)
        }
        Err(error) => Err(error.into()),
    }
}

fn invalid<T>(
    validity: FeatureValidity,
    observed_at: Timestamp,
) -> Result<FeatureValue<T>, RollingFeatureError> {
    Ok(FeatureValue::invalid(validity, observed_at)?)
}
