//! Holding-period price and total-return kernels.

use crate::batch::validate_count;
use crate::{
    AnalyticsError, Annualization, DatedMoney, DatedStatisticalInput, StatisticalInput,
    StatisticalResult, StatisticalScale, StatisticalSeries, StatisticalUnit,
};

/// Computes simple holding-period returns `(P[t] / P[t-1]) - 1`.
///
/// Timestamps may be irregular because this function does not annualize. They must be strictly
/// increasing. Inputs must be positive values in one currency unit.
///
/// # Errors
///
/// Rejects fewer than two prices, nonpositive prices, heterogeneous units, non-currency units,
/// timestamp regression, non-finite output, or the batch bound.
pub fn simple_returns(
    prices: &[DatedStatisticalInput],
) -> Result<StatisticalSeries, AnalyticsError> {
    validate_count(prices.len(), 2)?;
    let unit = prices[0].input().unit();
    if !matches!(unit, StatisticalUnit::Currency(_))
        || prices.iter().any(|price| price.input().unit() != unit)
    {
        return Err(AnalyticsError::UnitMismatch);
    }
    validate_dated_prices(prices)?;

    let mut output = Vec::with_capacity(prices.len() - 1);
    for window in prices.windows(2) {
        let previous = window[0].input().value();
        let current = window[1].input().value();
        output.push(StatisticalInput::try_new(
            current / previous - 1.0,
            StatisticalUnit::Return,
            StatisticalScale::Unit,
        )?);
    }
    StatisticalSeries::try_new(output, StatisticalUnit::Return)
}

/// Computes total holding-period returns including exact cash distributions.
///
/// `distributions[i]` is the cash distribution attributable to the interval from price `i` to
/// price `i + 1`. Money remains exact until numerator and denominator cross the explicitly typed
/// statistical boundary.
///
/// # Errors
///
/// Rejects length/currency mismatch, nonpositive prices, timestamp regression, checked money
/// arithmetic failure, non-finite conversion, insufficient input, or the batch bound.
pub fn total_returns(
    prices: &[DatedMoney],
    distributions: &[market_squawk_domain::Money],
) -> Result<StatisticalSeries, AnalyticsError> {
    validate_count(prices.len(), 2)?;
    if distributions.len() != prices.len() - 1 {
        return Err(AnalyticsError::LengthMismatch);
    }
    validate_dated_money(prices)?;
    let currency = prices[0].value().currency();
    if prices
        .iter()
        .any(|price| price.value().currency() != currency)
        || distributions
            .iter()
            .any(|distribution| distribution.currency() != currency)
    {
        return Err(AnalyticsError::CurrencyMismatch);
    }

    let mut output = Vec::with_capacity(distributions.len());
    for (index, distribution) in distributions.iter().copied().enumerate() {
        let previous = prices[index].value();
        let current = prices[index + 1].value();
        let numerator = current
            .checked_add(distribution)
            .and_then(|with_distribution| with_distribution.checked_sub(previous))
            .map_err(|_| AnalyticsError::DecimalArithmetic)?;
        let numerator = StatisticalInput::try_from_decimal(
            numerator.amount(),
            StatisticalUnit::Currency(currency),
            StatisticalScale::Unit,
        )?;
        let denominator = StatisticalInput::try_from_decimal(
            previous.amount(),
            StatisticalUnit::Currency(currency),
            StatisticalScale::Unit,
        )?;
        output.push(StatisticalInput::try_new(
            numerator.value() / denominator.value(),
            StatisticalUnit::Return,
            StatisticalScale::Unit,
        )?);
    }
    StatisticalSeries::try_new(output, StatisticalUnit::Return)
}

/// Compounds a homogeneous return series as `product(1 + r) - 1`.
///
/// # Errors
///
/// Rejects empty, excessive, heterogeneous, or non-return input and non-finite output.
pub fn cumulative_return(
    returns: &[StatisticalInput],
) -> Result<StatisticalResult, AnalyticsError> {
    validate_count(returns.len(), 1)?;
    if returns
        .iter()
        .any(|value| value.unit() != StatisticalUnit::Return)
    {
        return Err(AnalyticsError::UnitMismatch);
    }
    let compounded = returns.iter().try_fold(1.0_f64, |accumulator, value| {
        let next = accumulator * (1.0 + value.value());
        if next.is_finite() {
            Ok(next)
        } else {
            Err(AnalyticsError::NonFiniteInput)
        }
    })? - 1.0;
    StatisticalResult::try_new(
        compounded,
        StatisticalUnit::Return,
        returns.len(),
        None,
        Annualization::None,
        None,
    )
}

fn validate_dated_prices(prices: &[DatedStatisticalInput]) -> Result<(), AnalyticsError> {
    for price in prices {
        if price.input().value() <= 0.0 {
            return Err(AnalyticsError::NonPositivePrice);
        }
    }
    for window in prices.windows(2) {
        if window[0].at().unix_nanos() >= window[1].at().unix_nanos() {
            return Err(AnalyticsError::TimestampNotStrictlyIncreasing);
        }
    }
    Ok(())
}

fn validate_dated_money(prices: &[DatedMoney]) -> Result<(), AnalyticsError> {
    for price in prices {
        if price.value().amount() <= rust_decimal::Decimal::ZERO {
            return Err(AnalyticsError::NonPositivePrice);
        }
    }
    for window in prices.windows(2) {
        if window[0].at().unix_nanos() >= window[1].at().unix_nanos() {
            return Err(AnalyticsError::TimestampNotStrictlyIncreasing);
        }
    }
    Ok(())
}
