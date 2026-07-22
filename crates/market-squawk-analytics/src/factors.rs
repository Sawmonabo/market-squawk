//! Bounded ordinary-least-squares factor exposure kernels.

use crate::batch::{neumaier_sum, validate_count};
use crate::{
    AnalyticsError, Annualization, MAX_FACTOR_COUNT, StatisticalInput, StatisticalResult,
    StatisticalUnit, VarianceConvention,
};

/// One response and its ordered factor-return row.
#[derive(Clone, Debug, PartialEq)]
pub struct FactorObservation {
    response: StatisticalInput,
    factors: Box<[StatisticalInput]>,
}

impl FactorObservation {
    /// Validates one response/factor row already admitted at the statistical boundary.
    ///
    /// # Errors
    ///
    /// Rejects non-return inputs, an empty row, or more than [`MAX_FACTOR_COUNT`] factors.
    pub fn try_new(
        response: StatisticalInput,
        factors: Vec<StatisticalInput>,
    ) -> Result<Self, AnalyticsError> {
        if factors.is_empty() || factors.len() > MAX_FACTOR_COUNT {
            return Err(AnalyticsError::InvalidFactorDimensions);
        }
        if response.unit() != StatisticalUnit::Return
            || factors
                .iter()
                .any(|factor| factor.unit() != StatisticalUnit::Return)
        {
            return Err(AnalyticsError::UnitMismatch);
        }
        Ok(Self {
            response,
            factors: factors.into_boxed_slice(),
        })
    }

    /// Returns the response return.
    #[must_use]
    pub const fn response(&self) -> StatisticalInput {
        self.response
    }

    /// Returns ordered factor returns.
    #[must_use]
    pub fn factors(&self) -> &[StatisticalInput] {
        &self.factors
    }
}

/// Factor OLS result with per-period intercept, ordered exposures, and coefficient of determination.
#[derive(Clone, Debug, PartialEq)]
pub struct FactorRegressionResult {
    intercept: StatisticalResult,
    exposures: Box<[StatisticalResult]>,
    r_squared: StatisticalResult,
}

impl FactorRegressionResult {
    /// Returns the per-period intercept.
    #[must_use]
    pub const fn intercept(&self) -> StatisticalResult {
        self.intercept
    }

    /// Returns exposures in input-column order.
    #[must_use]
    pub fn exposures(&self) -> &[StatisticalResult] {
        &self.exposures
    }

    /// Returns coefficient of determination.
    #[must_use]
    pub const fn r_squared(&self) -> StatisticalResult {
        self.r_squared
    }
}

/// Fits a full-rank OLS factor model with an intercept using deterministic pivoted elimination.
///
/// At least one residual degree of freedom is required (`observations > factors + 1`). The
/// implementation fails closed on scale-relative numerical rank deficiency instead of silently
/// selecting a pseudo-inverse.
///
/// # Errors
///
/// Rejects inconsistent dimensions, insufficient/excessive history, rank deficiency, a constant
/// response, or non-finite results.
pub fn factor_regression(
    observations: &[FactorObservation],
) -> Result<FactorRegressionResult, AnalyticsError> {
    validate_count(observations.len(), 1)?;
    let factor_count = observations[0].factors.len();
    if factor_count == 0
        || factor_count > MAX_FACTOR_COUNT
        || observations
            .iter()
            .any(|observation| observation.factors.len() != factor_count)
    {
        return Err(AnalyticsError::InvalidFactorDimensions);
    }
    let parameter_count = factor_count + 1;
    validate_count(observations.len(), parameter_count + 1)?;

    let mut normal = vec![vec![0.0_f64; parameter_count + 1]; parameter_count];
    for (row, normal_row) in normal.iter_mut().enumerate() {
        for (column, entry) in normal_row.iter_mut().take(parameter_count).enumerate() {
            *entry = neumaier_sum(observations.iter().map(|observation| {
                design_value(observation, row) * design_value(observation, column)
            }));
        }
        normal_row[parameter_count] = neumaier_sum(
            observations
                .iter()
                .map(|observation| design_value(observation, row) * observation.response.value()),
        );
    }
    let coefficients = solve_full_rank(normal)?;
    let response_mean = neumaier_sum(
        observations
            .iter()
            .map(|observation| observation.response.value()),
    ) / observations.len() as f64;
    let total_sum_squares = neumaier_sum(observations.iter().map(|observation| {
        let centered = observation.response.value() - response_mean;
        centered * centered
    }));
    if total_sum_squares == 0.0 {
        return Err(AnalyticsError::ZeroVariance);
    }
    let residual_sum_squares = neumaier_sum(observations.iter().map(|observation| {
        let fitted = coefficients[0]
            + neumaier_sum(
                observation
                    .factors
                    .iter()
                    .zip(coefficients.iter().skip(1))
                    .map(|(factor, coefficient)| factor.value() * coefficient),
            );
        let residual = observation.response.value() - fitted;
        residual * residual
    }));
    let exposures = coefficients
        .iter()
        .skip(1)
        .map(|coefficient| {
            StatisticalResult::try_new(
                *coefficient,
                StatisticalUnit::Unitless,
                observations.len(),
                Some(VarianceConvention::Sample),
                Annualization::None,
                None,
            )
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_boxed_slice();
    Ok(FactorRegressionResult {
        intercept: StatisticalResult::try_new(
            coefficients[0],
            StatisticalUnit::Return,
            observations.len(),
            Some(VarianceConvention::Sample),
            Annualization::None,
            None,
        )?,
        exposures,
        r_squared: StatisticalResult::try_new(
            1.0 - residual_sum_squares / total_sum_squares,
            StatisticalUnit::Unitless,
            observations.len(),
            Some(VarianceConvention::Sample),
            Annualization::None,
            None,
        )?,
    })
}

fn design_value(observation: &FactorObservation, index: usize) -> f64 {
    if index == 0 {
        1.0
    } else {
        observation.factors[index - 1].value()
    }
}

fn solve_full_rank(mut matrix: Vec<Vec<f64>>) -> Result<Vec<f64>, AnalyticsError> {
    let dimension = matrix.len();
    let maximum = matrix
        .iter()
        .flat_map(|row| row[..dimension].iter())
        .map(|value| value.abs())
        .fold(0.0_f64, f64::max);
    let tolerance = f64::EPSILON * dimension as f64 * maximum.max(1.0) * 128.0;
    for column in 0..dimension {
        let pivot = (column..dimension)
            .max_by(|left, right| {
                matrix[*left][column]
                    .abs()
                    .total_cmp(&matrix[*right][column].abs())
            })
            .ok_or(AnalyticsError::RankDeficient)?;
        if matrix[pivot][column].abs() <= tolerance {
            return Err(AnalyticsError::RankDeficient);
        }
        matrix.swap(column, pivot);
        let divisor = matrix[column][column];
        for entry in &mut matrix[column][column..=dimension] {
            *entry /= divisor;
        }
        let pivot_row = matrix[column].clone();
        for (row, entries) in matrix.iter_mut().enumerate() {
            if row == column {
                continue;
            }
            let multiplier = entries[column];
            for (entry, pivot) in entries[column..=dimension]
                .iter_mut()
                .zip(&pivot_row[column..=dimension])
            {
                *entry -= multiplier * pivot;
            }
        }
    }
    Ok(matrix.into_iter().map(|row| row[dimension]).collect())
}
