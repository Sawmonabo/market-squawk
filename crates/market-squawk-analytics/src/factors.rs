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

/// Fits a full-rank OLS factor model with an intercept using scaled streaming Givens QR.
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
    let coefficients = solve_full_rank_qr(observations, parameter_count)?;
    let response_mean = neumaier_sum(
        observations
            .iter()
            .map(|observation| observation.response.value()),
    ) / observations.len() as f64;
    let response_scale = observations
        .iter()
        .map(|observation| (observation.response.value() - response_mean).abs())
        .fold(0.0_f64, f64::max);
    if response_scale == 0.0 {
        return Err(AnalyticsError::ZeroVariance);
    }
    let total_sum_squares = neumaier_sum(observations.iter().map(|observation| {
        let centered = (observation.response.value() - response_mean) / response_scale;
        centered * centered
    }));
    let residual_sum_squares = neumaier_sum(observations.iter().map(|observation| {
        let fitted = coefficients[0]
            + neumaier_sum(
                observation
                    .factors
                    .iter()
                    .zip(coefficients.iter().skip(1))
                    .map(|(factor, coefficient)| factor.value() * coefficient),
            );
        let residual = (observation.response.value() - fitted) / response_scale;
        residual * residual
    }));
    if !total_sum_squares.is_finite()
        || !residual_sum_squares.is_finite()
        || residual_sum_squares > total_sum_squares * (1.0 + f64::EPSILON.sqrt())
    {
        return Err(AnalyticsError::RankDeficient);
    }
    validate_normalized_residual(observations, &coefficients, response_scale)?;
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
            (1.0 - residual_sum_squares / total_sum_squares).clamp(0.0, 1.0),
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

fn solve_full_rank_qr(
    observations: &[FactorObservation],
    parameter_count: usize,
) -> Result<Vec<f64>, AnalyticsError> {
    let mut column_scales = vec![1.0_f64; parameter_count];
    for (column, scale) in column_scales.iter_mut().enumerate().skip(1) {
        *scale = observations
            .iter()
            .map(|observation| design_value(observation, column).abs())
            .fold(0.0_f64, f64::max);
        if *scale == 0.0 {
            return Err(AnalyticsError::RankDeficient);
        }
    }

    // Streaming Givens QR keeps O(p^2) memory while avoiding the condition-number squaring of
    // normal equations. Factor columns are scaled before decomposition so the relative rank test
    // is meaningful across heterogeneous factor magnitudes.
    let mut upper = vec![vec![0.0_f64; parameter_count]; parameter_count];
    let mut transformed_response = vec![0.0_f64; parameter_count];
    for observation in observations {
        let mut row = (0..parameter_count)
            .map(|column| design_value(observation, column) / column_scales[column])
            .collect::<Vec<_>>();
        let mut response = observation.response.value();
        for column in 0..parameter_count {
            let diagonal = upper[column][column].hypot(row[column]);
            if diagonal == 0.0 || !diagonal.is_finite() {
                continue;
            }
            let cosine = upper[column][column] / diagonal;
            let sine = row[column] / diagonal;
            upper[column][column] = diagonal;
            for next in column + 1..parameter_count {
                let prior_upper = upper[column][next];
                let prior_row = row[next];
                upper[column][next] = cosine * prior_upper + sine * prior_row;
                row[next] = -sine * prior_upper + cosine * prior_row;
            }
            let prior_response = transformed_response[column];
            transformed_response[column] = cosine * prior_response + sine * response;
            response = -sine * prior_response + cosine * response;
        }
    }

    let largest_diagonal = upper
        .iter()
        .enumerate()
        .map(|(index, row)| row[index].abs())
        .fold(0.0_f64, f64::max);
    let relative_rank_tolerance = f64::EPSILON.sqrt() * parameter_count as f64;
    if largest_diagonal == 0.0
        || upper.iter().enumerate().any(|(index, row)| {
            !row[index].is_finite()
                || row[index].abs() <= largest_diagonal * relative_rank_tolerance
        })
    {
        return Err(AnalyticsError::RankDeficient);
    }

    let mut scaled_coefficients = vec![0.0_f64; parameter_count];
    for row in (0..parameter_count).rev() {
        let known = neumaier_sum(
            upper[row][row + 1..]
                .iter()
                .zip(&scaled_coefficients[row + 1..])
                .map(|(coefficient, solved)| coefficient * solved),
        );
        scaled_coefficients[row] = (transformed_response[row] - known) / upper[row][row];
        if !scaled_coefficients[row].is_finite() {
            return Err(AnalyticsError::RankDeficient);
        }
    }
    Ok(scaled_coefficients
        .iter()
        .zip(column_scales)
        .map(|(coefficient, scale)| coefficient / scale)
        .collect())
}

fn validate_normalized_residual(
    observations: &[FactorObservation],
    coefficients: &[f64],
    response_scale: f64,
) -> Result<(), AnalyticsError> {
    let residual_scale = observations
        .iter()
        .map(|observation| {
            let fitted = coefficients[0]
                + neumaier_sum(
                    observation
                        .factors
                        .iter()
                        .zip(coefficients.iter().skip(1))
                        .map(|(factor, coefficient)| factor.value() * coefficient),
                );
            (observation.response.value() - fitted).abs()
        })
        .fold(0.0_f64, f64::max);
    if residual_scale <= response_scale * f64::EPSILON.sqrt() {
        return Ok(());
    }
    let residual_norm = neumaier_sum(observations.iter().map(|observation| {
        let fitted = coefficients[0]
            + neumaier_sum(
                observation
                    .factors
                    .iter()
                    .zip(coefficients.iter().skip(1))
                    .map(|(factor, coefficient)| factor.value() * coefficient),
            );
        let residual = (observation.response.value() - fitted) / residual_scale;
        residual * residual
    }))
    .sqrt();
    if !residual_norm.is_finite() || residual_norm == 0.0 {
        return Err(AnalyticsError::RankDeficient);
    }
    let orthogonality_tolerance = f64::EPSILON.sqrt() * coefficients.len() as f64 * 8.0;
    for column in 0..coefficients.len() {
        let column_scale = observations
            .iter()
            .map(|observation| design_value(observation, column).abs())
            .fold(0.0_f64, f64::max);
        let column_norm = neumaier_sum(observations.iter().map(|observation| {
            let value = design_value(observation, column) / column_scale;
            value * value
        }))
        .sqrt();
        let gradient = neumaier_sum(observations.iter().map(|observation| {
            let fitted = coefficients[0]
                + neumaier_sum(
                    observation
                        .factors
                        .iter()
                        .zip(coefficients.iter().skip(1))
                        .map(|(factor, coefficient)| factor.value() * coefficient),
                );
            (design_value(observation, column) / column_scale)
                * ((observation.response.value() - fitted) / residual_scale)
        }));
        if !gradient.is_finite()
            || gradient.abs() > orthogonality_tolerance * column_norm * residual_norm
        {
            return Err(AnalyticsError::RankDeficient);
        }
    }
    Ok(())
}
