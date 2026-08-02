//! Feature value invariants and authority-free scalar representations.

use std::num::NonZeroU128;

use market_squawk_domain::{BasisPoints, PriceTicks, QuantityLots, Timestamp};
use thiserror::Error;

use crate::{FeatureOutputType, HalfTickPrice};

/// Availability state attached to one feature observation.
///
/// Only [`Self::Ready`] may carry a value. Every other state deliberately removes any previously
/// ready payload so a caller cannot accidentally act on stale feature data.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FeatureValidity {
    /// The feature contains a current value.
    Ready,
    /// The feature's bounded state has not met its warm-up policy.
    WarmingUp,
    /// Required inputs are absent or otherwise unusable.
    Unavailable,
    /// Checked feature arithmetic overflowed.
    Overflow,
    /// An input timestamp moved backwards and invalidated the feature state.
    TimestampRegression,
    /// The most recent usable input exceeded its freshness policy.
    Stale,
}

impl FeatureValidity {
    /// Returns whether this validity state is permitted to carry a value.
    #[must_use]
    pub const fn is_ready(self) -> bool {
        matches!(self, Self::Ready)
    }
}

/// An exact rational feature scalar whose denominator is always positive.
///
/// The numerator and denominator are reduced to one canonical integer representation. The value is
/// intentionally not rounded to an executable price.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ExactFeatureRatio {
    numerator: i128,
    denominator: NonZeroU128,
}

impl ExactFeatureRatio {
    /// Constructs a canonical exact rational scalar without rounding.
    ///
    /// # Errors
    ///
    /// Returns [`FeatureError::ZeroDenominator`] when `denominator` is zero.
    pub fn try_new(numerator: i128, denominator: u128) -> Result<Self, FeatureError> {
        let denominator = NonZeroU128::new(denominator).ok_or(FeatureError::ZeroDenominator)?;
        let numerator_magnitude = numerator.unsigned_abs();
        let divisor = greatest_common_divisor(numerator_magnitude, denominator);
        let reduced_magnitude = numerator_magnitude / divisor.get();
        let reduced_numerator = match i128::try_from(reduced_magnitude) {
            Ok(magnitude) if numerator.is_negative() => -magnitude,
            Ok(magnitude) => magnitude,
            Err(_) => i128::MIN,
        };
        let reduced_denominator = NonZeroU128::new(denominator.get() / divisor.get())
            .ok_or(FeatureError::ZeroDenominator)?;

        Ok(Self {
            numerator: reduced_numerator,
            denominator: reduced_denominator,
        })
    }

    /// Returns the signed numerator.
    #[must_use]
    pub const fn numerator(self) -> i128 {
        self.numerator
    }

    /// Returns the positive denominator.
    #[must_use]
    pub const fn denominator(self) -> NonZeroU128 {
        self.denominator
    }
}

fn greatest_common_divisor(mut left: u128, mut right: NonZeroU128) -> NonZeroU128 {
    while let Some(left_nonzero) = NonZeroU128::new(left) {
        let remainder = right.get() % left_nonzero.get();
        right = left_nonzero;
        left = remainder;
    }
    right
}

/// A finite floating-point value admitted only at an explicit statistical boundary.
///
/// This type cannot be converted into an order price by the analytics API.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct StatisticalF64(f64);

impl StatisticalF64 {
    /// Constructs a finite statistical value.
    ///
    /// # Errors
    ///
    /// Returns [`FeatureError::NonFiniteStatisticalValue`] for NaN or either infinity.
    pub fn try_new(value: f64) -> Result<Self, FeatureError> {
        if value.is_finite() {
            Ok(Self(value))
        } else {
            Err(FeatureError::NonFiniteStatisticalValue)
        }
    }

    /// Returns the finite floating-point value.
    #[must_use]
    pub const fn get(self) -> f64 {
        self.0
    }
}

/// Closed scalar set exposed by authority-free live feature views.
///
/// Financial values remain typed, exact rational results remain unrounded, and floating point is
/// isolated behind [`StatisticalF64`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum FeatureScalar {
    /// An exact number of instrument price ticks.
    PriceTicks(PriceTicks),
    /// An exact price represented in half-tick units.
    HalfTickPrice(HalfTickPrice),
    /// An exact number of instrument quantity lots.
    QuantityLots(QuantityLots),
    /// An exact signed number of basis points.
    BasisPoints(BasisPoints),
    /// A signed dimensioned integer defined by registry metadata.
    SignedInteger(i128),
    /// An unsigned dimensioned integer defined by registry metadata.
    UnsignedInteger(u128),
    /// An exact, unrounded rational result.
    ExactRatio(ExactFeatureRatio),
    /// A finite statistical result that is not an executable price.
    Statistical(StatisticalF64),
}

impl FeatureScalar {
    /// Returns the sole metadata output type compatible with this closed scalar variant.
    #[must_use]
    pub const fn output_type(self) -> FeatureOutputType {
        match self {
            Self::PriceTicks(_) => FeatureOutputType::PriceTicks,
            Self::HalfTickPrice(_) => FeatureOutputType::HalfTickPrice,
            Self::QuantityLots(_) => FeatureOutputType::QuantityLots,
            Self::BasisPoints(_) => FeatureOutputType::BasisPoints,
            Self::SignedInteger(_) => FeatureOutputType::SignedInteger,
            Self::UnsignedInteger(_) => FeatureOutputType::UnsignedInteger,
            Self::ExactRatio(_) => FeatureOutputType::ExactRatio,
            Self::Statistical(_) => FeatureOutputType::StatisticalF64,
        }
    }
}

/// One timestamped feature result with stale-value exclusion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FeatureValue<T> {
    value: Option<T>,
    observed_at: Timestamp,
    validity: FeatureValidity,
}

impl<T> FeatureValue<T> {
    /// Constructs a ready feature observation.
    #[must_use]
    pub const fn ready(value: T, observed_at: Timestamp) -> Self {
        Self {
            value: Some(value),
            observed_at,
            validity: FeatureValidity::Ready,
        }
    }

    /// Constructs a feature observation without a value.
    ///
    /// # Errors
    ///
    /// Returns [`FeatureError::ReadyRequiresValue`] when `validity` is [`FeatureValidity::Ready`].
    pub fn invalid(
        validity: FeatureValidity,
        observed_at: Timestamp,
    ) -> Result<Self, FeatureError> {
        if validity.is_ready() {
            return Err(FeatureError::ReadyRequiresValue);
        }
        Ok(Self {
            value: None,
            observed_at,
            validity,
        })
    }

    /// Replaces the observation with a ready value.
    pub fn set_ready(&mut self, value: T, observed_at: Timestamp) {
        self.value = Some(value);
        self.observed_at = observed_at;
        self.validity = FeatureValidity::Ready;
    }

    /// Invalidates the observation and removes any previously ready value.
    ///
    /// # Errors
    ///
    /// Returns [`FeatureError::ReadyRequiresValue`] when asked to create a ready state without a
    /// replacement value. The original observation is left unchanged on error.
    pub fn invalidate(
        &mut self,
        validity: FeatureValidity,
        observed_at: Timestamp,
    ) -> Result<(), FeatureError> {
        if validity.is_ready() {
            return Err(FeatureError::ReadyRequiresValue);
        }
        self.value = None;
        self.observed_at = observed_at;
        self.validity = validity;
        Ok(())
    }

    /// Returns the value only when this observation is ready.
    #[must_use]
    pub const fn value(&self) -> Option<&T> {
        self.value.as_ref()
    }

    /// Returns the copied value only when this observation is ready.
    #[must_use]
    pub fn ready_value(&self) -> Option<T>
    where
        T: Copy,
    {
        self.value
    }

    /// Returns the input observation time represented by this state.
    #[must_use]
    pub const fn observed_at(&self) -> Timestamp {
        self.observed_at
    }

    /// Returns the feature's current validity.
    #[must_use]
    pub const fn validity(&self) -> FeatureValidity {
        self.validity
    }
}

/// Invariant or accounting failure for feature values and read-only views.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum FeatureError {
    /// A ready state was requested without a replacement value.
    #[error("ready feature state requires a value")]
    ReadyRequiresValue,
    /// A rational feature denominator was zero.
    #[error("feature ratio denominator must be nonzero")]
    ZeroDenominator,
    /// A floating-point statistical value was NaN or infinite.
    #[error("statistical feature values must be finite")]
    NonFiniteStatisticalValue,
    /// Exact retained-byte accounting overflowed `usize`.
    #[error("feature retained-byte accounting overflowed")]
    RetainedSizeOverflow,
}
