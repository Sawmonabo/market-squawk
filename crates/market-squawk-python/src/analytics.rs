//! Bounded financial analytics and canonical feature-contract Python bindings.

use std::mem::size_of;
use std::num::{NonZeroU32, NonZeroUsize};
use std::str::FromStr as _;

use market_squawk_analytics::{
    Annualization, BatchFeatureCatalog, BatchFeatureCatalogConfig, BatchFeaturePolicies,
    DatedMoney, DatedStatisticalInput, FeatureRegistry, MissingValuePolicy, Quantile, ReturnSeries,
    ShockComposition, StatisticalInput, StatisticalScale, StatisticalUnit, VarianceConvention,
    WeightPolicy, cumulative_return, discrete_expected_shortfall, historical_var, maximum_drawdown,
    simple_returns, total_returns, volatility,
};
use market_squawk_domain::{Currency, Money, RoundingPolicy, Timestamp};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PySequence, PyString};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;

use super::{
    CONTROL_CHECK_INTERVAL, FEATURE_IMPLEMENTATION_REVISION, MAX_ANALYTIC_RETAINED_BYTES,
    MAX_ANALYTIC_VALUES, MAX_DECIMAL_TEXT_BYTES, MAX_FEATURE_CONTRACTS, OperationContext,
    encode_hex, invalid_input,
};

fn bounded_len(length: usize, element_bytes: usize) -> PyResult<()> {
    let retained = length
        .checked_mul(element_bytes)
        .ok_or_else(invalid_input)?;
    if length > MAX_ANALYTIC_VALUES || retained > MAX_ANALYTIC_RETAINED_BYTES {
        return Err(invalid_input());
    }
    Ok(())
}

fn bounded_f64(values: &Bound<'_, PyAny>, context: &OperationContext) -> PyResult<Vec<f64>> {
    let sequence = values.cast::<PySequence>().map_err(|_| invalid_input())?;
    let length = sequence.len().map_err(|_| invalid_input())?;
    bounded_len(length, size_of::<f64>())?;
    context.admit(u64::try_from(length).map_err(|_| invalid_input())?.max(1))?;
    let mut admitted = Vec::new();
    admitted
        .try_reserve_exact(length)
        .map_err(|_| invalid_input())?;
    for index in 0..length {
        if index % CONTROL_CHECK_INTERVAL == 0 {
            context.check()?;
        }
        admitted.push(
            sequence
                .get_item(index)
                .and_then(|value| value.extract::<f64>())
                .map_err(|_| invalid_input())?,
        );
    }
    Ok(admitted)
}

fn bounded_i64(values: &Bound<'_, PyAny>, context: &OperationContext) -> PyResult<Vec<i64>> {
    let sequence = values.cast::<PySequence>().map_err(|_| invalid_input())?;
    let length = sequence.len().map_err(|_| invalid_input())?;
    bounded_len(length, size_of::<i64>())?;
    context.admit(u64::try_from(length).map_err(|_| invalid_input())?.max(1))?;
    let mut admitted = Vec::new();
    admitted
        .try_reserve_exact(length)
        .map_err(|_| invalid_input())?;
    for index in 0..length {
        if index % CONTROL_CHECK_INTERVAL == 0 {
            context.check()?;
        }
        admitted.push(
            sequence
                .get_item(index)
                .and_then(|value| value.extract::<i64>())
                .map_err(|_| invalid_input())?,
        );
    }
    Ok(admitted)
}

fn bounded_decimal_strings(
    values: &Bound<'_, PyAny>,
    context: &OperationContext,
) -> PyResult<Vec<String>> {
    let sequence = values.cast::<PySequence>().map_err(|_| invalid_input())?;
    let length = sequence.len().map_err(|_| invalid_input())?;
    bounded_len(length, MAX_DECIMAL_TEXT_BYTES)?;
    context.admit(u64::try_from(length).map_err(|_| invalid_input())?.max(1))?;
    let mut admitted = Vec::new();
    admitted
        .try_reserve_exact(length)
        .map_err(|_| invalid_input())?;
    for index in 0..length {
        if index % CONTROL_CHECK_INTERVAL == 0 {
            context.check()?;
        }
        let item = sequence.get_item(index).map_err(|_| invalid_input())?;
        let string = item.cast::<PyString>().map_err(|_| invalid_input())?;
        let text = string.to_str().map_err(|_| invalid_input())?;
        if text.is_empty() || text.len() > MAX_DECIMAL_TEXT_BYTES {
            return Err(invalid_input());
        }
        admitted.push(text.to_owned());
    }
    Ok(admitted)
}

fn bounded_output(values: Vec<f64>) -> PyResult<Vec<f64>> {
    bounded_len(values.len(), size_of::<f64>())?;
    Ok(values)
}

fn admit_linear_kernel(
    context: &OperationContext,
    value_count: usize,
    operations_per_value: u64,
) -> PyResult<()> {
    let values = u64::try_from(value_count)
        .map_err(|_| invalid_input())?
        .max(1);
    let operations = values
        .checked_mul(operations_per_value)
        .ok_or_else(invalid_input)?;
    context.admit(operations)
}

fn admit_sort_kernel(context: &OperationContext, value_count: usize) -> PyResult<()> {
    let values = u64::try_from(value_count)
        .map_err(|_| invalid_input())?
        .max(1);
    let comparisons = if values <= 1 {
        1
    } else {
        values
            .checked_mul(u64::from(values.ilog2()) + 1)
            .ok_or_else(invalid_input)?
    };
    context.admit(comparisons.checked_mul(16).ok_or_else(invalid_input)?)
}

fn statistical_values(values: Vec<f64>) -> PyResult<Vec<StatisticalInput>> {
    values
        .into_iter()
        .map(|value| {
            StatisticalInput::try_new(value, StatisticalUnit::Return, StatisticalScale::Unit)
                .map_err(|_| invalid_input())
        })
        .collect()
}

fn dated_exact_prices(
    prices: Vec<String>,
    timestamps: Vec<i64>,
    currency: Currency,
) -> PyResult<Vec<DatedStatisticalInput>> {
    if prices.len() != timestamps.len() {
        return Err(invalid_input());
    }
    prices
        .into_iter()
        .zip(timestamps)
        .map(|(price, timestamp)| {
            let exact = Decimal::from_str(&price).map_err(|_| invalid_input())?;
            let value = exact.to_f64().ok_or_else(invalid_input)?;
            StatisticalInput::try_new(
                value,
                StatisticalUnit::Currency(currency),
                StatisticalScale::Unit,
            )
            .map(|input| DatedStatisticalInput::new(Timestamp::from_unix_nanos(timestamp), input))
            .map_err(|_| invalid_input())
        })
        .collect()
}

#[pyfunction]
fn price_returns(
    py: Python<'_>,
    prices: &Bound<'_, PyAny>,
    timestamps: &Bound<'_, PyAny>,
    currency: &str,
    context: &OperationContext,
) -> PyResult<Vec<f64>> {
    let prices = bounded_decimal_strings(prices, context)?;
    let timestamps = bounded_i64(timestamps, context)?;
    admit_linear_kernel(context, prices.len(), 24)?;
    let currency = Currency::try_from(currency).map_err(|_| invalid_input())?;
    let context = context.clone();
    py.detach(move || {
        context.check()?;
        let values = simple_returns(&dated_exact_prices(prices, timestamps, currency)?)
            .map_err(|_| invalid_input())?;
        let output = bounded_output(values.values().iter().map(|value| value.value()).collect())?;
        context.check()?;
        Ok(output)
    })
}

#[pyfunction]
fn exact_total_returns(
    py: Python<'_>,
    prices: &Bound<'_, PyAny>,
    distributions: &Bound<'_, PyAny>,
    timestamps: &Bound<'_, PyAny>,
    currency: &str,
    context: &OperationContext,
) -> PyResult<Vec<f64>> {
    let prices = bounded_decimal_strings(prices, context)?;
    let distributions = bounded_decimal_strings(distributions, context)?;
    let timestamps = bounded_i64(timestamps, context)?;
    admit_linear_kernel(
        context,
        prices.len().saturating_add(distributions.len()),
        32,
    )?;
    let currency = Currency::try_from(currency).map_err(|_| invalid_input())?;
    let context = context.clone();
    py.detach(move || {
        context.check()?;
        if prices.len() != timestamps.len() {
            return Err(invalid_input());
        }
        let prices = prices
            .into_iter()
            .zip(timestamps)
            .map(|(amount, timestamp)| {
                Decimal::from_str(&amount)
                    .map(|amount| {
                        DatedMoney::new(
                            Timestamp::from_unix_nanos(timestamp),
                            Money::new(amount, currency),
                        )
                    })
                    .map_err(|_| invalid_input())
            })
            .collect::<PyResult<Vec<_>>>()?;
        let distributions = distributions
            .into_iter()
            .map(|amount| {
                Decimal::from_str(&amount)
                    .map(|amount| Money::new(amount, currency))
                    .map_err(|_| invalid_input())
            })
            .collect::<PyResult<Vec<_>>>()?;
        let values = total_returns(&prices, &distributions).map_err(|_| invalid_input())?;
        let output = bounded_output(values.values().iter().map(|value| value.value()).collect())?;
        context.check()?;
        Ok(output)
    })
}

#[pyfunction]
fn compound_returns(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    context: &OperationContext,
) -> PyResult<f64> {
    let values = bounded_f64(values, context)?;
    admit_linear_kernel(context, values.len(), 12)?;
    let context = context.clone();
    py.detach(move || {
        context.check()?;
        let result = cumulative_return(&statistical_values(values)?)
            .map(|result| result.value())
            .map_err(|_| invalid_input())?;
        context.check()?;
        Ok(result)
    })
}

#[pyfunction]
fn return_volatility(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
    periods_per_year: u32,
    context: &OperationContext,
) -> PyResult<f64> {
    let values = bounded_f64(values, context)?;
    admit_linear_kernel(context, values.len(), 24)?;
    let context = context.clone();
    py.detach(move || {
        context.check()?;
        let periods = NonZeroU32::new(periods_per_year).ok_or_else(invalid_input)?;
        let returns = ReturnSeries::try_new(
            statistical_values(values)?,
            Annualization::PeriodsPerYear(periods),
        )
        .map_err(|_| invalid_input())?;
        let result = volatility(
            &returns,
            VarianceConvention::Sample,
            MissingValuePolicy::Reject,
        )
        .map(|result| result.value())
        .map_err(|_| invalid_input())?;
        context.check()?;
        Ok(result)
    })
}

#[pyfunction]
fn drawdown(
    py: Python<'_>,
    prices: &Bound<'_, PyAny>,
    timestamps: &Bound<'_, PyAny>,
    currency: &str,
    context: &OperationContext,
) -> PyResult<(f64, usize, usize, Option<usize>)> {
    let prices = bounded_decimal_strings(prices, context)?;
    let timestamps = bounded_i64(timestamps, context)?;
    admit_linear_kernel(context, prices.len(), 24)?;
    let currency = Currency::try_from(currency).map_err(|_| invalid_input())?;
    let context = context.clone();
    py.detach(move || {
        context.check()?;
        let result = maximum_drawdown(&dated_exact_prices(prices, timestamps, currency)?)
            .map(|result| {
                (
                    result.magnitude().value(),
                    result.peak_index(),
                    result.trough_index(),
                    result.recovery_index(),
                )
            })
            .map_err(|_| invalid_input())?;
        context.check()?;
        Ok(result)
    })
}

#[pyfunction]
fn pearson_correlation(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    context: &OperationContext,
) -> PyResult<f64> {
    let left = bounded_f64(left, context)?;
    let right = bounded_f64(right, context)?;
    admit_linear_kernel(context, left.len().saturating_add(right.len()), 24)?;
    let context = context.clone();
    py.detach(move || {
        context.check()?;
        let result = market_squawk_analytics::correlation(
            &statistical_values(left)?,
            &statistical_values(right)?,
        )
        .map(|result| result.value())
        .map_err(|_| invalid_input())?;
        context.check()?;
        Ok(result)
    })
}

#[pyfunction]
fn value_at_risk(
    py: Python<'_>,
    losses: &Bound<'_, PyAny>,
    confidence: f64,
    context: &OperationContext,
) -> PyResult<f64> {
    let losses = bounded_f64(losses, context)?;
    admit_sort_kernel(context, losses.len())?;
    let context = context.clone();
    py.detach(move || {
        context.check()?;
        let result = historical_var(
            &statistical_values(losses)?,
            Quantile::try_new(confidence).map_err(|_| invalid_input())?,
        )
        .map(|result| result.value())
        .map_err(|_| invalid_input())?;
        context.check()?;
        Ok(result)
    })
}

#[pyfunction]
fn expected_shortfall(
    py: Python<'_>,
    losses: &Bound<'_, PyAny>,
    confidence: f64,
    context: &OperationContext,
) -> PyResult<f64> {
    let losses = bounded_f64(losses, context)?;
    admit_sort_kernel(context, losses.len())?;
    let context = context.clone();
    py.detach(move || {
        context.check()?;
        let result = discrete_expected_shortfall(
            &statistical_values(losses)?,
            Quantile::try_new(confidence).map_err(|_| invalid_input())?,
        )
        .map(|result| result.value())
        .map_err(|_| invalid_input())?;
        context.check()?;
        Ok(result)
    })
}

fn batch_catalog() -> PyResult<BatchFeatureCatalog> {
    let periods = NonZeroU32::new(252).ok_or_else(invalid_input)?;
    let confidence = NonZeroU32::new(950_000).ok_or_else(invalid_input)?;
    let config = BatchFeatureCatalogConfig::try_new(
        periods,
        confidence,
        6,
        BatchFeaturePolicies::new(
            VarianceConvention::Sample,
            MissingValuePolicy::Reject,
            WeightPolicy::PositiveNormalized,
            RoundingPolicy::NearestEven,
            ShockComposition::Compounded,
        ),
    )
    .map_err(|_| invalid_input())?;
    BatchFeatureCatalog::try_new(config, FEATURE_IMPLEMENTATION_REVISION)
        .map_err(|_| invalid_input())
}

#[pyfunction]
fn canonical_feature_contracts(
    py: Python<'_>,
    context: &OperationContext,
) -> PyResult<Vec<(String, u32, String, String)>> {
    context.admit(u64::try_from(MAX_FEATURE_CONTRACTS).map_err(|_| invalid_input())?)?;
    let context = context.clone();
    py.detach(move || {
        context.check()?;
        let catalog = batch_catalog()?;
        let capacity = NonZeroUsize::new(catalog.entries().len()).ok_or_else(invalid_input)?;
        let retained = NonZeroUsize::new(4 * 1024 * 1024).ok_or_else(invalid_input)?;
        let mut registry =
            FeatureRegistry::try_new(capacity, retained).map_err(|_| invalid_input())?;
        catalog
            .try_register(&mut registry)
            .map_err(|_| invalid_input())?;
        if catalog.entries().len() > MAX_FEATURE_CONTRACTS {
            return Err(invalid_input());
        }
        let output = catalog
            .entries()
            .iter()
            .map(|metadata| {
                (
                    metadata.key().name().to_owned(),
                    metadata.key().version().get(),
                    encode_hex(metadata.input_schema_digest().as_bytes()),
                    encode_hex(metadata.semantic_digest().as_bytes()),
                )
            })
            .collect();
        context.check()?;
        Ok(output)
    })
}

pub(super) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(price_returns, module)?)?;
    module.add_function(wrap_pyfunction!(exact_total_returns, module)?)?;
    module.add_function(wrap_pyfunction!(compound_returns, module)?)?;
    module.add_function(wrap_pyfunction!(return_volatility, module)?)?;
    module.add_function(wrap_pyfunction!(drawdown, module)?)?;
    module.add_function(wrap_pyfunction!(pearson_correlation, module)?)?;
    module.add_function(wrap_pyfunction!(value_at_risk, module)?)?;
    module.add_function(wrap_pyfunction!(expected_shortfall, module)?)?;
    module.add_function(wrap_pyfunction!(canonical_feature_contracts, module)?)?;
    Ok(())
}
