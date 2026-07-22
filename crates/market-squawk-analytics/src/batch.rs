//! Explicit statistical conversion, policy, and bounded batch result contracts.

use std::num::NonZeroU32;

use market_squawk_domain::{Currency, Money, RoundingPolicy, Timestamp};
use rust_decimal::Decimal;
use rust_decimal::RoundingStrategy;
use rust_decimal::prelude::ToPrimitive;
use thiserror::Error;

use crate::StatisticalF64;

/// Maximum observations accepted by one in-process analytical kernel call.
pub const MAX_BATCH_OBSERVATIONS: usize = 1_000_000;
/// Maximum factor columns accepted by one regression.
pub const MAX_FACTOR_COUNT: usize = 64;
/// Maximum UTF-8 bytes in an analytical dimension identifier.
pub const MAX_ANALYTICS_IDENTIFIER_BYTES: usize = 96;

/// Unit carried across the floating-point statistical boundary.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum StatisticalUnit {
    /// Dimensionless value.
    Unitless,
    /// Dimensionless holding-period return.
    Return,
    /// Dimensionless rate.
    Rate,
    /// Amount denominated in one currency.
    Currency(Currency),
}

/// Scale of the source value before normalization.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum StatisticalScale {
    /// Source is already expressed in whole units.
    Unit,
    /// Source is expressed as percent, where `100` means one whole unit.
    Percent,
    /// Source is expressed in basis points, where `10_000` means one whole unit.
    BasisPoints,
}

impl StatisticalScale {
    fn normalize(self, value: f64) -> f64 {
        match self {
            Self::Unit => value,
            Self::Percent => value / 100.0,
            Self::BasisPoints => value / 10_000.0,
        }
    }
}

/// One finite value admitted to statistical code with its source unit and scale.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct StatisticalInput {
    value: StatisticalF64,
    unit: StatisticalUnit,
    source_scale: StatisticalScale,
}

impl StatisticalInput {
    /// Normalizes and validates one floating-point statistical input.
    ///
    /// # Errors
    ///
    /// Returns [`AnalyticsError::NonFiniteInput`] when either the source or normalized value is
    /// NaN or infinite.
    pub fn try_new(
        source_value: f64,
        unit: StatisticalUnit,
        source_scale: StatisticalScale,
    ) -> Result<Self, AnalyticsError> {
        if !matches!(source_scale, StatisticalScale::Unit)
            && !matches!(unit, StatisticalUnit::Return | StatisticalUnit::Rate)
        {
            return Err(AnalyticsError::IncompatibleScale);
        }
        StatisticalF64::try_new(source_value).map_err(|_| AnalyticsError::NonFiniteInput)?;
        let value = StatisticalF64::try_new(source_scale.normalize(source_value))
            .map_err(|_| AnalyticsError::NonFiniteInput)?;
        Ok(Self {
            value,
            unit,
            source_scale,
        })
    }

    /// Converts an exact decimal into an explicitly typed statistical value.
    ///
    /// # Errors
    ///
    /// Returns [`AnalyticsError::DecimalConversion`] if the decimal cannot be represented as a
    /// finite `f64`.
    pub fn try_from_decimal(
        value: Decimal,
        unit: StatisticalUnit,
        source_scale: StatisticalScale,
    ) -> Result<Self, AnalyticsError> {
        let value = value.to_f64().ok_or(AnalyticsError::DecimalConversion)?;
        Self::try_new(value, unit, source_scale)
    }

    /// Returns the normalized finite value.
    #[must_use]
    pub const fn value(self) -> f64 {
        self.value.get()
    }

    /// Returns the semantic unit.
    #[must_use]
    pub const fn unit(self) -> StatisticalUnit {
        self.unit
    }

    /// Returns the scale declared at the conversion boundary.
    #[must_use]
    pub const fn source_scale(self) -> StatisticalScale {
        self.source_scale
    }
}

/// One timestamped statistical input.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DatedStatisticalInput {
    at: Timestamp,
    input: StatisticalInput,
}

impl DatedStatisticalInput {
    /// Constructs a timestamped input without changing its value.
    #[must_use]
    pub const fn new(at: Timestamp, input: StatisticalInput) -> Self {
        Self { at, input }
    }

    /// Returns observation time.
    #[must_use]
    pub const fn at(self) -> Timestamp {
        self.at
    }

    /// Returns the statistical input.
    #[must_use]
    pub const fn input(self) -> StatisticalInput {
        self.input
    }
}

/// One exact timestamped money value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DatedMoney {
    at: Timestamp,
    value: Money,
}

impl DatedMoney {
    /// Constructs a timestamped exact money value.
    #[must_use]
    pub const fn new(at: Timestamp, value: Money) -> Self {
        Self { at, value }
    }

    /// Returns observation time.
    #[must_use]
    pub const fn at(self) -> Timestamp {
        self.at
    }

    /// Returns the exact amount.
    #[must_use]
    pub const fn value(self) -> Money {
        self.value
    }
}

/// Variance denominator convention.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VarianceConvention {
    /// Divide by `n - 1`; requires at least two observations.
    Sample,
    /// Divide by `n`; analytical kernels still require two observations.
    Population,
}

/// Explicit annualization rule.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Annualization {
    /// Do not annualize.
    None,
    /// Scale by a declared positive number of periods per year.
    PeriodsPerYear(NonZeroU32),
}

impl Annualization {
    pub(crate) fn volatility_multiplier(self) -> f64 {
        match self {
            Self::None => 1.0,
            Self::PeriodsPerYear(periods) => f64::from(periods.get()).sqrt(),
        }
    }
}

/// Explicit policy for absent observations at a statistical-series boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MissingValuePolicy {
    /// Reject any absent observation.
    Reject,
    /// Remove absent observations while reporting the retained count.
    Drop,
}

/// Weight interpretation for batch statistics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WeightPolicy {
    /// Give every retained observation equal probability mass.
    Equal,
    /// Normalize caller-supplied strictly positive weights by their finite total.
    PositiveNormalized,
}

/// Explicit insufficient-history policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InsufficientHistoryPolicy {
    /// Return a typed error instead of a placeholder value.
    Error,
}

/// Open probability in `(0, 1)` used by tail-risk kernels.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Quantile(StatisticalF64);

impl Quantile {
    /// Validates an open probability.
    ///
    /// # Errors
    ///
    /// Returns [`AnalyticsError::InvalidQuantile`] outside `(0, 1)` or for non-finite input.
    pub fn try_new(value: f64) -> Result<Self, AnalyticsError> {
        let value = StatisticalF64::try_new(value).map_err(|_| AnalyticsError::InvalidQuantile)?;
        if value.get() <= 0.0 || value.get() >= 1.0 {
            return Err(AnalyticsError::InvalidQuantile);
        }
        Ok(Self(value))
    }

    /// Returns the probability.
    #[must_use]
    pub const fn value(self) -> f64 {
        self.0.get()
    }
}

/// Explicit rounding policy for accounting and fundamental decimal ratios.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecimalPolicy {
    scale: u32,
    rounding: RoundingPolicy,
}

impl DecimalPolicy {
    /// Constructs a decimal result policy.
    ///
    /// # Errors
    ///
    /// Returns [`AnalyticsError::UnsupportedDecimalScale`] above Decimal's maximum scale.
    pub fn try_new(scale: u32, rounding: RoundingPolicy) -> Result<Self, AnalyticsError> {
        if scale > Decimal::MAX_SCALE {
            return Err(AnalyticsError::UnsupportedDecimalScale);
        }
        Ok(Self { scale, rounding })
    }

    /// Returns output decimal places.
    #[must_use]
    pub const fn scale(self) -> u32 {
        self.scale
    }

    /// Returns the explicit rounding rule.
    #[must_use]
    pub const fn rounding(self) -> RoundingPolicy {
        self.rounding
    }
}

/// Complete statistical defaults supplied by a caller, never inferred by a kernel.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AnalyticsPolicy {
    variance: VarianceConvention,
    annualization: Annualization,
    missing: MissingValuePolicy,
    insufficient_history: InsufficientHistoryPolicy,
}

impl AnalyticsPolicy {
    /// Constructs an explicit policy bundle.
    #[must_use]
    pub const fn new(
        variance: VarianceConvention,
        annualization: Annualization,
        missing: MissingValuePolicy,
        insufficient_history: InsufficientHistoryPolicy,
    ) -> Self {
        Self {
            variance,
            annualization,
            missing,
            insufficient_history,
        }
    }

    /// Returns the variance convention.
    #[must_use]
    pub const fn variance(self) -> VarianceConvention {
        self.variance
    }

    /// Returns the annualization convention.
    #[must_use]
    pub const fn annualization(self) -> Annualization {
        self.annualization
    }

    /// Returns the missing-value convention.
    #[must_use]
    pub const fn missing(self) -> MissingValuePolicy {
        self.missing
    }

    /// Returns the insufficient-history convention.
    #[must_use]
    pub const fn insufficient_history(self) -> InsufficientHistoryPolicy {
        self.insufficient_history
    }
}

/// One weighted statistical observation. Weights are finite and strictly positive.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct WeightedStatisticalInput {
    input: StatisticalInput,
    weight: StatisticalF64,
}

impl WeightedStatisticalInput {
    /// Constructs a weighted input.
    ///
    /// # Errors
    ///
    /// Returns [`AnalyticsError::InvalidWeight`] unless weight is finite and positive.
    pub fn try_new(input: StatisticalInput, weight: f64) -> Result<Self, AnalyticsError> {
        let weight = StatisticalF64::try_new(weight).map_err(|_| AnalyticsError::InvalidWeight)?;
        if weight.get() <= 0.0 {
            return Err(AnalyticsError::InvalidWeight);
        }
        Ok(Self { input, weight })
    }

    /// Returns the observation.
    #[must_use]
    pub const fn input(self) -> StatisticalInput {
        self.input
    }

    /// Returns the positive probability or frequency weight.
    #[must_use]
    pub const fn weight(self) -> f64 {
        self.weight.get()
    }
}

/// Bounded sequence of homogeneous statistical results.
#[derive(Clone, Debug, PartialEq)]
pub struct StatisticalSeries {
    values: Box<[StatisticalInput]>,
    unit: StatisticalUnit,
}

impl StatisticalSeries {
    pub(crate) fn try_new(
        values: Vec<StatisticalInput>,
        unit: StatisticalUnit,
    ) -> Result<Self, AnalyticsError> {
        validate_count(values.len(), 1)?;
        if values.iter().any(|value| value.unit() != unit) {
            return Err(AnalyticsError::UnitMismatch);
        }
        Ok(Self {
            values: values.into_boxed_slice(),
            unit,
        })
    }

    /// Returns normalized values.
    #[must_use]
    pub fn values(&self) -> &[StatisticalInput] {
        &self.values
    }

    /// Returns retained observation count.
    #[must_use]
    pub const fn observations(&self) -> usize {
        self.values.len()
    }

    /// Returns the common unit.
    #[must_use]
    pub const fn unit(&self) -> StatisticalUnit {
        self.unit
    }
}

/// One typed scalar statistical result with disclosed sample size and conventions.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct StatisticalResult {
    value: StatisticalInput,
    observations: usize,
    variance: Option<VarianceConvention>,
    annualization: Annualization,
    quantile: Option<Quantile>,
}

impl StatisticalResult {
    pub(crate) fn try_new(
        value: f64,
        unit: StatisticalUnit,
        observations: usize,
        variance: Option<VarianceConvention>,
        annualization: Annualization,
        quantile: Option<Quantile>,
    ) -> Result<Self, AnalyticsError> {
        validate_count(observations, 1)?;
        Ok(Self {
            value: StatisticalInput::try_new(value, unit, StatisticalScale::Unit)?,
            observations,
            variance,
            annualization,
            quantile,
        })
    }

    /// Returns normalized scalar value.
    #[must_use]
    pub const fn value(self) -> f64 {
        self.value.value()
    }

    /// Returns the result unit.
    #[must_use]
    pub const fn unit(self) -> StatisticalUnit {
        self.value.unit()
    }

    /// Returns contributing observations.
    #[must_use]
    pub const fn observations(self) -> usize {
        self.observations
    }

    /// Returns the variance convention when applicable.
    #[must_use]
    pub const fn variance_convention(self) -> Option<VarianceConvention> {
        self.variance
    }

    /// Returns annualization policy.
    #[must_use]
    pub const fn annualization(self) -> Annualization {
        self.annualization
    }

    /// Returns tail probability policy when applicable.
    #[must_use]
    pub const fn quantile(self) -> Option<Quantile> {
        self.quantile
    }
}

/// Analytical input or invariant failure.
#[derive(Clone, Debug, Error, PartialEq)]
pub enum AnalyticsError {
    /// A floating-point input was NaN or infinite.
    #[error("statistical input must be finite")]
    NonFiniteInput,
    /// Exact Decimal could not cross the finite statistical boundary.
    #[error("decimal cannot be represented as a finite statistical value")]
    DecimalConversion,
    /// Decimal scale exceeds the exact representation.
    #[error("decimal output scale is unsupported")]
    UnsupportedDecimalScale,
    /// Input units or currencies differ.
    #[error("analytical input units differ")]
    UnitMismatch,
    /// A percent or basis-point scale was paired with an amount or unitless value.
    #[error("statistical source scale is incompatible with its unit")]
    IncompatibleScale,
    /// Input currencies differ.
    #[error("money currencies differ")]
    CurrencyMismatch,
    /// A required value was absent.
    #[error("missing observation is rejected by policy")]
    MissingObservation,
    /// Input is too short for the requested statistic.
    #[error("insufficient history: required {required}, actual {actual}")]
    InsufficientHistory {
        /// Minimum required observations.
        required: usize,
        /// Supplied observations.
        actual: usize,
    },
    /// Input exceeds the bounded in-process observation limit.
    #[error("analytical batch exceeds its observation limit")]
    ObservationLimitExceeded,
    /// Timestamp order is not strictly increasing.
    #[error("timestamps must be strictly increasing")]
    TimestampNotStrictlyIncreasing,
    /// Price input is zero or negative.
    #[error("prices must be strictly positive")]
    NonPositivePrice,
    /// Paired input lengths differ.
    #[error("paired analytical inputs have different lengths")]
    LengthMismatch,
    /// Probability is not finite and strictly between zero and one.
    #[error("quantile must be finite and strictly between zero and one")]
    InvalidQuantile,
    /// A statistical weight is not finite and positive.
    #[error("statistical weights must be finite and positive")]
    InvalidWeight,
    /// Variance is zero where a denominator requires positive variance.
    #[error("statistic is undefined for zero variance")]
    ZeroVariance,
    /// Parametric standard deviation was negative.
    #[error("parametric standard deviation cannot be negative")]
    NegativeStandardDeviation,
    /// Regression design matrix is singular or numerically rank deficient.
    #[error("factor design matrix is rank deficient")]
    RankDeficient,
    /// Factor row widths differ, are empty, or exceed the bound.
    #[error("factor dimensions are invalid")]
    InvalidFactorDimensions,
    /// Checked decimal arithmetic failed.
    #[error("analytical decimal arithmetic overflowed or divided by zero")]
    DecimalArithmetic,
    /// Fundamental capital expenditure used the wrong sign convention.
    #[error("capital expenditure must be represented as a nonnegative cash outflow")]
    NegativeCapitalExpenditure,
    /// Yield maturities are not strictly increasing.
    #[error("yield-curve maturities must be strictly increasing")]
    MaturityNotStrictlyIncreasing,
    /// An analytical dimension identifier is invalid.
    #[error("analytical identifier is invalid")]
    InvalidIdentifier,
    /// A scenario shock does not map to a portfolio exposure.
    #[error("scenario shock has no matching portfolio exposure")]
    UnknownShockDimension,
}

pub(crate) fn validate_count(actual: usize, required: usize) -> Result<(), AnalyticsError> {
    if actual > MAX_BATCH_OBSERVATIONS {
        Err(AnalyticsError::ObservationLimitExceeded)
    } else if actual < required {
        Err(AnalyticsError::InsufficientHistory { required, actual })
    } else {
        Ok(())
    }
}

pub(crate) fn validate_homogeneous(
    inputs: &[StatisticalInput],
) -> Result<StatisticalUnit, AnalyticsError> {
    validate_count(inputs.len(), 1)?;
    let unit = inputs[0].unit();
    if inputs.iter().any(|input| input.unit() != unit) {
        return Err(AnalyticsError::UnitMismatch);
    }
    Ok(unit)
}

pub(crate) fn validate_identifier(value: &str) -> Result<(), AnalyticsError> {
    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else {
        return Err(AnalyticsError::InvalidIdentifier);
    };
    if value.len() > MAX_ANALYTICS_IDENTIFIER_BYTES
        || !first.is_ascii_lowercase()
        || bytes.any(|byte| {
            !(byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'-' | b'_' | b'.'))
        })
    {
        return Err(AnalyticsError::InvalidIdentifier);
    }
    Ok(())
}

pub(crate) fn checked_decimal_ratio(
    numerator: Decimal,
    denominator: Decimal,
    policy: DecimalPolicy,
) -> Result<Decimal, AnalyticsError> {
    let quotient = numerator
        .checked_div(denominator)
        .ok_or(AnalyticsError::DecimalArithmetic)?;
    Ok(quotient
        .round_dp_with_strategy(policy.scale, rounding_strategy(policy.rounding))
        .normalize())
}

fn rounding_strategy(policy: RoundingPolicy) -> RoundingStrategy {
    match policy {
        RoundingPolicy::NearestEven => RoundingStrategy::MidpointNearestEven,
        RoundingPolicy::AwayFromZero => RoundingStrategy::AwayFromZero,
        RoundingPolicy::TowardZero => RoundingStrategy::ToZero,
        RoundingPolicy::Floor => RoundingStrategy::ToNegativeInfinity,
        RoundingPolicy::Ceiling => RoundingStrategy::ToPositiveInfinity,
    }
}

pub(crate) fn neumaier_sum(values: impl Iterator<Item = f64>) -> f64 {
    let mut sum = 0.0;
    let mut compensation = 0.0;
    for value in values {
        let next = sum + value;
        if sum.abs() >= value.abs() {
            compensation += (sum - next) + value;
        } else {
            compensation += (value - next) + sum;
        }
        sum = next;
    }
    sum + compensation
}

/// Applies a declared missing-value policy before invoking a non-null kernel.
///
/// # Errors
///
/// Rejects missing values under [`MissingValuePolicy::Reject`], heterogeneous units, empty input,
/// or the batch bound.
pub fn resolve_optional_inputs(
    inputs: &[Option<StatisticalInput>],
    policy: MissingValuePolicy,
) -> Result<Box<[StatisticalInput]>, AnalyticsError> {
    if inputs.len() > MAX_BATCH_OBSERVATIONS {
        return Err(AnalyticsError::ObservationLimitExceeded);
    }
    if matches!(policy, MissingValuePolicy::Reject) && inputs.iter().any(Option::is_none) {
        return Err(AnalyticsError::MissingObservation);
    }
    let retained = inputs.iter().filter_map(|value| *value).collect::<Vec<_>>();
    validate_homogeneous(&retained)?;
    Ok(retained.into_boxed_slice())
}
