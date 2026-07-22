//! Stable-ABI Python bindings for bounded pure Rust analytical kernels.

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
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use rust_decimal::Decimal;

const FEATURE_IMPLEMENTATION_REVISION: &str = "task14-python-v1";

fn invalid_input() -> PyErr {
    PyValueError::new_err("financial input violates a bounded Rust analytics contract")
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

fn dated_prices(
    prices: Vec<f64>,
    timestamps: Vec<i64>,
    currency: &str,
) -> PyResult<Vec<DatedStatisticalInput>> {
    if prices.len() != timestamps.len() {
        return Err(invalid_input());
    }
    let currency = Currency::try_from(currency).map_err(|_| invalid_input())?;
    prices
        .into_iter()
        .zip(timestamps)
        .map(|(price, timestamp)| {
            StatisticalInput::try_new(
                price,
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
    prices: Vec<f64>,
    timestamps: Vec<i64>,
    currency: &str,
) -> PyResult<Vec<f64>> {
    let currency = currency.to_owned();
    py.detach(move || {
        let values = simple_returns(&dated_prices(prices, timestamps, &currency)?)
            .map_err(|_| invalid_input())?;
        Ok(values.values().iter().map(|value| value.value()).collect())
    })
}

#[pyfunction]
fn exact_total_returns(
    py: Python<'_>,
    prices: Vec<String>,
    distributions: Vec<String>,
    timestamps: Vec<i64>,
    currency: &str,
) -> PyResult<Vec<f64>> {
    let currency = currency.to_owned();
    py.detach(move || {
        if prices.len() != timestamps.len() {
            return Err(invalid_input());
        }
        let currency = Currency::try_from(currency.as_str()).map_err(|_| invalid_input())?;
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
        Ok(values.values().iter().map(|value| value.value()).collect())
    })
}

#[pyfunction]
fn compound_returns(py: Python<'_>, values: Vec<f64>) -> PyResult<f64> {
    py.detach(move || {
        cumulative_return(&statistical_values(values)?)
            .map(|result| result.value())
            .map_err(|_| invalid_input())
    })
}

#[pyfunction]
fn return_volatility(py: Python<'_>, values: Vec<f64>, periods_per_year: u32) -> PyResult<f64> {
    py.detach(move || {
        let periods = NonZeroU32::new(periods_per_year).ok_or_else(invalid_input)?;
        let returns = ReturnSeries::try_new(
            statistical_values(values)?,
            Annualization::PeriodsPerYear(periods),
        )
        .map_err(|_| invalid_input())?;
        volatility(
            &returns,
            VarianceConvention::Sample,
            MissingValuePolicy::Reject,
        )
        .map(|result| result.value())
        .map_err(|_| invalid_input())
    })
}

#[pyfunction]
fn drawdown(
    py: Python<'_>,
    prices: Vec<f64>,
    timestamps: Vec<i64>,
    currency: &str,
) -> PyResult<(f64, usize, usize, Option<usize>)> {
    let currency = currency.to_owned();
    py.detach(move || {
        maximum_drawdown(&dated_prices(prices, timestamps, &currency)?)
            .map(|result| {
                (
                    result.magnitude().value(),
                    result.peak_index(),
                    result.trough_index(),
                    result.recovery_index(),
                )
            })
            .map_err(|_| invalid_input())
    })
}

#[pyfunction]
fn pearson_correlation(py: Python<'_>, left: Vec<f64>, right: Vec<f64>) -> PyResult<f64> {
    py.detach(move || {
        market_squawk_analytics::correlation(
            &statistical_values(left)?,
            &statistical_values(right)?,
        )
        .map(|result| result.value())
        .map_err(|_| invalid_input())
    })
}

#[pyfunction]
fn value_at_risk(py: Python<'_>, losses: Vec<f64>, confidence: f64) -> PyResult<f64> {
    py.detach(move || {
        historical_var(
            &statistical_values(losses)?,
            Quantile::try_new(confidence).map_err(|_| invalid_input())?,
        )
        .map(|result| result.value())
        .map_err(|_| invalid_input())
    })
}

#[pyfunction]
fn expected_shortfall(py: Python<'_>, losses: Vec<f64>, confidence: f64) -> PyResult<f64> {
    py.detach(move || {
        discrete_expected_shortfall(
            &statistical_values(losses)?,
            Quantile::try_new(confidence).map_err(|_| invalid_input())?,
        )
        .map(|result| result.value())
        .map_err(|_| invalid_input())
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
fn canonical_feature_contracts(py: Python<'_>) -> PyResult<Vec<(String, u32, String, String)>> {
    py.detach(move || {
        let catalog = batch_catalog()?;
        let capacity = NonZeroUsize::new(catalog.entries().len()).ok_or_else(invalid_input)?;
        let retained = NonZeroUsize::new(4 * 1024 * 1024).ok_or_else(invalid_input)?;
        let mut registry =
            FeatureRegistry::try_new(capacity, retained).map_err(|_| invalid_input())?;
        catalog
            .try_register(&mut registry)
            .map_err(|_| invalid_input())?;
        Ok(catalog
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
            .collect())
    })
}

fn encode_hex(bytes: [u8; 32]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
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
