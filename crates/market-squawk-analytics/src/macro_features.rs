//! Exact macro-surprise, yield-curve, and rate-change kernels.

use rust_decimal::Decimal;

use crate::batch::{checked_decimal_ratio, validate_count};
use crate::{AnalyticsError, DecimalPolicy};

/// One exact continuously comparable rate at a positive maturity in calendar days.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RatePoint {
    maturity_days: u32,
    rate: Decimal,
}

impl RatePoint {
    /// Constructs a rate point with a positive maturity.
    ///
    /// # Errors
    ///
    /// Rejects zero maturity.
    pub fn try_new(maturity_days: u32, rate: Decimal) -> Result<Self, AnalyticsError> {
        if maturity_days == 0 {
            return Err(AnalyticsError::MaturityNotStrictlyIncreasing);
        }
        Ok(Self {
            maturity_days,
            rate,
        })
    }

    /// Returns maturity in calendar days.
    #[must_use]
    pub const fn maturity_days(self) -> u32 {
        self.maturity_days
    }

    /// Returns exact decimal rate.
    #[must_use]
    pub const fn rate(self) -> Decimal {
        self.rate
    }
}

/// Three-point yield-curve shape result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct YieldCurveFeatures {
    short_rate: Decimal,
    middle_rate: Decimal,
    long_rate: Decimal,
    slope: Decimal,
    curvature: Decimal,
}

impl YieldCurveFeatures {
    /// Returns shortest-maturity rate.
    #[must_use]
    pub const fn short_rate(self) -> Decimal {
        self.short_rate
    }

    /// Returns selected middle-maturity rate.
    #[must_use]
    pub const fn middle_rate(self) -> Decimal {
        self.middle_rate
    }

    /// Returns longest-maturity rate.
    #[must_use]
    pub const fn long_rate(self) -> Decimal {
        self.long_rate
    }

    /// Returns `long - short` slope.
    #[must_use]
    pub const fn slope(self) -> Decimal {
        self.slope
    }

    /// Returns butterfly curvature `2 * middle - short - long`.
    #[must_use]
    pub const fn curvature(self) -> Decimal {
        self.curvature
    }
}

/// Matched-curve changes without hidden interpolation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RateChangeFeatures {
    average_parallel_shift: Decimal,
    slope_change: Decimal,
    short_change: Decimal,
    long_change: Decimal,
}

impl RateChangeFeatures {
    /// Returns arithmetic mean change across exactly matched maturities.
    #[must_use]
    pub const fn average_parallel_shift(self) -> Decimal {
        self.average_parallel_shift
    }

    /// Returns change in long-minus-short slope.
    #[must_use]
    pub const fn slope_change(self) -> Decimal {
        self.slope_change
    }

    /// Returns shortest-maturity change.
    #[must_use]
    pub const fn short_change(self) -> Decimal {
        self.short_change
    }

    /// Returns longest-maturity change.
    #[must_use]
    pub const fn long_change(self) -> Decimal {
        self.long_change
    }
}

/// Computes short/middle/long exact curve shape using the middle array element.
///
/// # Errors
///
/// Requires at least three points, positive strictly increasing maturities, the batch bound, and
/// representable exact decimal arithmetic.
pub fn yield_curve_features(curve: &[RatePoint]) -> Result<YieldCurveFeatures, AnalyticsError> {
    validate_curve(curve, 3)?;
    let short_rate = curve[0].rate;
    let middle_rate = curve[curve.len() / 2].rate;
    let long_rate = curve[curve.len() - 1].rate;
    let slope = long_rate
        .checked_sub(short_rate)
        .ok_or(AnalyticsError::DecimalArithmetic)?;
    let curvature = middle_rate
        .checked_mul(Decimal::from(2_u32))
        .and_then(|value| value.checked_sub(short_rate))
        .and_then(|value| value.checked_sub(long_rate))
        .ok_or(AnalyticsError::DecimalArithmetic)?;
    Ok(YieldCurveFeatures {
        short_rate,
        middle_rate,
        long_rate,
        slope: slope.normalize(),
        curvature: curvature.normalize(),
    })
}

/// Computes rate changes only for exactly matched ordered maturity grids.
///
/// # Errors
///
/// Rejects length/maturity mismatch, invalid curve ordering, checked arithmetic failure, or an
/// unsupported decimal policy.
pub fn yield_curve_change(
    prior: &[RatePoint],
    current: &[RatePoint],
    policy: DecimalPolicy,
) -> Result<RateChangeFeatures, AnalyticsError> {
    validate_curve(prior, 2)?;
    validate_curve(current, 2)?;
    if prior.len() != current.len() {
        return Err(AnalyticsError::LengthMismatch);
    }
    if prior
        .iter()
        .zip(current)
        .any(|(prior, current)| prior.maturity_days != current.maturity_days)
    {
        return Err(AnalyticsError::MaturityNotStrictlyIncreasing);
    }
    let changes = prior
        .iter()
        .zip(current)
        .map(|(prior, current)| {
            current
                .rate
                .checked_sub(prior.rate)
                .ok_or(AnalyticsError::DecimalArithmetic)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let total = changes.iter().try_fold(Decimal::ZERO, |total, change| {
        total
            .checked_add(*change)
            .ok_or(AnalyticsError::DecimalArithmetic)
    })?;
    let count = u64::try_from(changes.len()).map_err(|_| AnalyticsError::DecimalArithmetic)?;
    let average_parallel_shift = checked_decimal_ratio(total, Decimal::from(count), policy)?;
    let short_change = changes[0];
    let long_change = changes[changes.len() - 1];
    let slope_change = long_change
        .checked_sub(short_change)
        .ok_or(AnalyticsError::DecimalArithmetic)?;
    Ok(RateChangeFeatures {
        average_parallel_shift,
        slope_change: slope_change.normalize(),
        short_change: short_change.normalize(),
        long_change: long_change.normalize(),
    })
}

/// Computes standardized macro surprise `(actual - consensus) / scale`.
///
/// `scale` is caller-supplied historical forecast-error standard deviation or another documented
/// positive normalization scale; this kernel does not infer one.
///
/// # Errors
///
/// Rejects nonpositive scale or checked decimal arithmetic failure.
pub fn macro_surprise(
    actual: Decimal,
    consensus: Decimal,
    scale: Decimal,
    policy: DecimalPolicy,
) -> Result<Decimal, AnalyticsError> {
    if scale <= Decimal::ZERO {
        return Err(AnalyticsError::DecimalArithmetic);
    }
    let difference = actual
        .checked_sub(consensus)
        .ok_or(AnalyticsError::DecimalArithmetic)?;
    checked_decimal_ratio(difference, scale, policy)
}

fn validate_curve(curve: &[RatePoint], required: usize) -> Result<(), AnalyticsError> {
    validate_count(curve.len(), required)?;
    if curve[0].maturity_days == 0
        || curve
            .windows(2)
            .any(|window| window[0].maturity_days >= window[1].maturity_days)
    {
        return Err(AnalyticsError::MaturityNotStrictlyIncreasing);
    }
    Ok(())
}
