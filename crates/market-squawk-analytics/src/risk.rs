//! Deterministic dispersion, regression, performance-ratio, and tail-risk kernels.

use crate::batch::{neumaier_sum, validate_count, validate_homogeneous};
use crate::{
    AnalyticsError, Annualization, DatedStatisticalInput, MissingValuePolicy, Quantile,
    ReturnSeries, StatisticalDispersion, StatisticalInput, StatisticalLocation, StatisticalResult,
    StatisticalScale, StatisticalUnit, VarianceConvention, WeightPolicy, WeightedStatisticalInput,
};

/// Maximum drawdown and the first complete peak/trough/recovery path that realizes it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DrawdownResult {
    magnitude: StatisticalResult,
    peak_index: usize,
    trough_index: usize,
    recovery_index: Option<usize>,
}

impl DrawdownResult {
    /// Returns nonnegative peak-to-trough loss magnitude.
    #[must_use]
    pub const fn magnitude(self) -> StatisticalResult {
        self.magnitude
    }

    /// Returns peak index.
    #[must_use]
    pub const fn peak_index(self) -> usize {
        self.peak_index
    }

    /// Returns trough index.
    #[must_use]
    pub const fn trough_index(self) -> usize {
        self.trough_index
    }

    /// Returns the first index at or above the drawdown peak after the trough.
    #[must_use]
    pub const fn recovery_index(self) -> Option<usize> {
        self.recovery_index
    }
}

/// Ordinary least-squares alpha and beta for one benchmark.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AlphaBetaResult {
    alpha: StatisticalResult,
    beta: StatisticalResult,
}

impl AlphaBetaResult {
    /// Returns intercept per input period; it is not implicitly annualized.
    #[must_use]
    pub const fn alpha(self) -> StatisticalResult {
        self.alpha
    }

    /// Returns dimensionless benchmark exposure.
    #[must_use]
    pub const fn beta(self) -> StatisticalResult {
        self.beta
    }
}

/// Typed volatility using a declared denominator and series-bound annualization convention.
///
/// Inputs have already crossed a non-null boundary. `missing_policy` is retained explicitly so a
/// caller cannot accidentally reuse a policy-free invocation after resolving optional data.
///
/// # Errors
///
/// Rejects fewer than two observations, heterogeneous units, non-return inputs, excessive input,
/// zero/non-finite arithmetic, or invalid dispersion output.
pub fn volatility(
    returns: &ReturnSeries,
    convention: VarianceConvention,
    _missing_policy: MissingValuePolicy,
) -> Result<StatisticalDispersion, AnalyticsError> {
    ensure_returns(returns.values(), 2)?;
    let variance = variance(returns.values(), convention)?;
    let annualization = returns.annualization();
    StatisticalDispersion::try_new(
        variance.sqrt() * annualization.volatility_multiplier(),
        StatisticalScale::Unit,
        StatisticalUnit::Return,
        returns.observations(),
        convention,
        annualization,
    )
}

/// Maximum peak-to-trough drawdown over a positive, ordered price series.
///
/// # Errors
///
/// Rejects empty, excessive, heterogeneous, non-currency, nonpositive, or non-monotonic-time
/// input.
pub fn maximum_drawdown(
    prices: &[DatedStatisticalInput],
) -> Result<DrawdownResult, AnalyticsError> {
    validate_count(prices.len(), 1)?;
    let unit = prices[0].input().unit();
    if !matches!(unit, StatisticalUnit::Currency(_))
        || prices.iter().any(|price| price.input().unit() != unit)
    {
        return Err(AnalyticsError::UnitMismatch);
    }
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

    let mut running_peak = prices[0].input().value();
    let mut running_peak_index = 0_usize;
    let mut maximum = 0.0_f64;
    let mut peak_index = 0_usize;
    let mut trough_index = 0_usize;
    let mut maximum_peak_value = running_peak;
    for (index, observation) in prices.iter().enumerate().skip(1) {
        let price = observation.input().value();
        if price > running_peak {
            running_peak = price;
            running_peak_index = index;
        }
        let drawdown = (running_peak - price) / running_peak;
        if drawdown > maximum {
            maximum = drawdown;
            peak_index = running_peak_index;
            trough_index = index;
            maximum_peak_value = running_peak;
        }
    }
    let recovery_index = if maximum == 0.0 {
        Some(peak_index)
    } else {
        prices
            .iter()
            .enumerate()
            .skip(trough_index + 1)
            .find(|(_, observation)| observation.input().value() >= maximum_peak_value)
            .map(|(index, _)| index)
    };
    Ok(DrawdownResult {
        magnitude: StatisticalResult::try_new(
            maximum,
            StatisticalUnit::Return,
            prices.len(),
            None,
            Annualization::None,
            None,
        )?,
        peak_index,
        trough_index,
        recovery_index,
    })
}

/// Pearson sample correlation. Sample versus population scaling cancels in the ratio.
///
/// # Errors
///
/// Rejects length/unit mismatch, fewer than two observations, excessive input, or zero variance.
pub fn correlation(
    left: &[StatisticalInput],
    right: &[StatisticalInput],
) -> Result<StatisticalResult, AnalyticsError> {
    validate_pair(left, right, 2)?;
    let left_mean = mean(left);
    let right_mean = mean(right);
    let left_scale = left
        .iter()
        .map(|value| (value.value() - left_mean).abs())
        .fold(0.0_f64, f64::max);
    let right_scale = right
        .iter()
        .map(|value| (value.value() - right_mean).abs())
        .fold(0.0_f64, f64::max);
    if left_scale == 0.0 || right_scale == 0.0 {
        return Err(AnalyticsError::ZeroVariance);
    }
    let covariance = neumaier_sum(left.iter().zip(right).map(|(left, right)| {
        ((left.value() - left_mean) / left_scale) * ((right.value() - right_mean) / right_scale)
    }));
    let left_sum_squares = neumaier_sum(left.iter().map(|value| {
        let centered = (value.value() - left_mean) / left_scale;
        centered * centered
    }));
    let right_sum_squares = neumaier_sum(right.iter().map(|value| {
        let centered = (value.value() - right_mean) / right_scale;
        centered * centered
    }));
    let denominator = left_sum_squares.sqrt() * right_sum_squares.sqrt();
    if denominator == 0.0 || !denominator.is_finite() {
        return Err(AnalyticsError::ZeroVariance);
    }
    StatisticalResult::try_new(
        (covariance / denominator).clamp(-1.0, 1.0),
        StatisticalUnit::Unitless,
        left.len(),
        Some(VarianceConvention::Sample),
        Annualization::None,
        None,
    )
}

/// Fits `asset = alpha + beta * benchmark` with a declared denominator convention.
///
/// # Errors
///
/// Rejects mismatched or non-return input, insufficient history, or zero benchmark variance.
pub fn alpha_beta(
    asset: &[StatisticalInput],
    benchmark: &[StatisticalInput],
    convention: VarianceConvention,
) -> Result<AlphaBetaResult, AnalyticsError> {
    validate_pair(asset, benchmark, 2)?;
    ensure_returns(asset, 2)?;
    let asset_mean = mean(asset);
    let benchmark_mean = mean(benchmark);
    let denominator = variance_denominator(asset.len(), convention);
    let covariance = neumaier_sum(asset.iter().zip(benchmark).map(|(asset, benchmark)| {
        (asset.value() - asset_mean) * (benchmark.value() - benchmark_mean)
    })) / denominator;
    let benchmark_variance = variance(benchmark, convention)?;
    if benchmark_variance == 0.0 {
        return Err(AnalyticsError::ZeroVariance);
    }
    let beta = covariance / benchmark_variance;
    let alpha = asset_mean - beta * benchmark_mean;
    Ok(AlphaBetaResult {
        alpha: StatisticalResult::try_new(
            alpha,
            StatisticalUnit::Return,
            asset.len(),
            Some(convention),
            Annualization::None,
            None,
        )?,
        beta: StatisticalResult::try_new(
            beta,
            StatisticalUnit::Unitless,
            asset.len(),
            Some(convention),
            Annualization::None,
            None,
        )?,
    })
}

/// Annualized Sharpe ratio using sample volatility and a per-period risk-free return.
///
/// # Errors
///
/// Rejects invalid returns, fewer than two observations, non-finite risk-free input, or zero
/// excess-return variance.
pub fn sharpe_ratio(
    returns: &ReturnSeries,
    risk_free_return: StatisticalInput,
) -> Result<StatisticalResult, AnalyticsError> {
    ratio_over_sample_deviation(returns, risk_free_return, false)
}

/// Annualized Sortino ratio using population target downside deviation.
///
/// # Errors
///
/// Rejects invalid returns, fewer than two observations, non-finite target, or no downside
/// deviation.
pub fn sortino_ratio(
    returns: &ReturnSeries,
    target_return: StatisticalInput,
) -> Result<StatisticalResult, AnalyticsError> {
    ratio_over_sample_deviation(returns, target_return, true)
}

/// Typed annualized sample standard deviation of active returns.
///
/// # Errors
///
/// Rejects invalid paired return series or zero/non-finite arithmetic.
pub fn tracking_error(
    portfolio: &ReturnSeries,
    benchmark: &ReturnSeries,
) -> Result<StatisticalDispersion, AnalyticsError> {
    let active = active_returns(portfolio, benchmark)?;
    let variance = numeric_sample_variance(&active)?;
    let annualization = portfolio.annualization();
    StatisticalDispersion::try_new(
        variance.sqrt() * annualization.volatility_multiplier(),
        StatisticalScale::Unit,
        StatisticalUnit::Return,
        active.len(),
        VarianceConvention::Sample,
        annualization,
    )
}

/// Annualized information ratio: mean active return divided by tracking error.
///
/// # Errors
///
/// Rejects invalid paired returns or zero tracking-error denominator.
pub fn information_ratio(
    portfolio: &ReturnSeries,
    benchmark: &ReturnSeries,
) -> Result<StatisticalResult, AnalyticsError> {
    let active = active_returns(portfolio, benchmark)?;
    let deviation = numeric_sample_variance(&active)?.sqrt();
    if deviation == 0.0 {
        return Err(AnalyticsError::ZeroVariance);
    }
    let mean_active = neumaier_sum(active.iter().copied()) / active.len() as f64;
    let annualization = portfolio.annualization();
    StatisticalResult::try_new(
        mean_active / deviation * annualization.volatility_multiplier(),
        StatisticalUnit::Unitless,
        active.len(),
        Some(VarianceConvention::Sample),
        annualization,
        None,
    )
}

/// Equal-weight empirical Value at Risk for a loss distribution using nearest-rank quantiles.
///
/// Losses are supplied as positive adverse amounts/returns. No sign reversal is implicit.
///
/// # Errors
///
/// Rejects empty, excessive, heterogeneous, or non-finite input.
pub fn historical_var(
    losses: &[StatisticalInput],
    confidence: Quantile,
) -> Result<StatisticalResult, AnalyticsError> {
    let unit = validate_homogeneous(losses)?;
    let mut sorted = losses.iter().map(|loss| loss.value()).collect::<Vec<_>>();
    sorted.sort_by(f64::total_cmp);
    let count =
        u32::try_from(sorted.len()).map_err(|_| AnalyticsError::ObservationLimitExceeded)?;
    let threshold = confidence.value() * f64::from(count);
    let index = (0..sorted.len())
        .find(|index| u32::try_from(index + 1).is_ok_and(|rank| f64::from(rank) >= threshold))
        .unwrap_or(sorted.len() - 1);
    StatisticalResult::try_new(
        sorted[index],
        unit,
        losses.len(),
        None,
        Annualization::None,
        Some(confidence),
    )
}

/// Equal-weight coherent discrete Expected Shortfall with a fractional boundary atom.
///
/// This averages exactly the worst `1 - confidence` probability mass. When the requested tail
/// cuts through an observation, only the required fraction of that atom is included. Sorting with
/// total ordering makes ties and point masses deterministic.
///
/// # Errors
///
/// Returns the same input errors as [`weighted_expected_shortfall`].
pub fn discrete_expected_shortfall(
    losses: &[StatisticalInput],
    confidence: Quantile,
) -> Result<StatisticalResult, AnalyticsError> {
    let weighted = losses
        .iter()
        .copied()
        .map(|loss| WeightedStatisticalInput::try_new(loss, 1.0))
        .collect::<Result<Vec<_>, _>>()?;
    weighted_expected_shortfall(&weighted, confidence, WeightPolicy::Equal)
}

/// Weighted coherent Expected Shortfall with normalized positive weights and fractional atoms.
///
/// # Errors
///
/// Rejects empty, excessive, heterogeneous, nonpositive/non-finite weights, invalid accumulated
/// weight, or non-finite output.
pub fn weighted_expected_shortfall(
    losses: &[WeightedStatisticalInput],
    confidence: Quantile,
    weight_policy: WeightPolicy,
) -> Result<StatisticalResult, AnalyticsError> {
    validate_count(losses.len(), 1)?;
    let raw = losses.iter().map(|loss| loss.input()).collect::<Vec<_>>();
    let unit = validate_homogeneous(&raw)?;
    let effective_weight = |loss: &WeightedStatisticalInput| match weight_policy {
        WeightPolicy::Equal => 1.0,
        WeightPolicy::PositiveNormalized => loss.weight(),
    };
    let total_weight = neumaier_sum(losses.iter().map(effective_weight));
    if !total_weight.is_finite() || total_weight <= 0.0 {
        return Err(AnalyticsError::InvalidWeight);
    }
    let target = total_weight * (1.0 - confidence.value());
    let mut sorted = losses.to_vec();
    sorted.sort_by(|left, right| right.input().value().total_cmp(&left.input().value()));
    let mut used = 0.0_f64;
    let mut tail_sum = 0.0_f64;
    for loss in sorted {
        if used >= target {
            break;
        }
        let admitted = effective_weight(&loss).min(target - used);
        tail_sum += loss.input().value() * admitted;
        used += admitted;
    }
    StatisticalResult::try_new(
        tail_sum / target,
        unit,
        losses.len(),
        None,
        Annualization::None,
        Some(confidence),
    )
}

/// Normal-distribution parametric Value at Risk `mean + z(confidence) * sigma`.
///
/// The distribution location and dispersion must carry the same underlying unit and identical
/// horizon/cadence contract.
///
/// # Errors
///
/// Rejects unit/horizon mismatch, non-finite input, negative deviation, or invalid output.
pub fn parametric_var(
    mean_loss: StatisticalLocation,
    standard_deviation: StatisticalDispersion,
    confidence: Quantile,
) -> Result<StatisticalResult, AnalyticsError> {
    if mean_loss.value().unit() != standard_deviation.underlying_unit() {
        return Err(AnalyticsError::UnitMismatch);
    }
    if mean_loss.annualization() != standard_deviation.annualization() {
        return Err(AnalyticsError::AnnualizationMismatch);
    }
    StatisticalResult::try_new(
        mean_loss.value().value()
            + inverse_standard_normal(confidence.value()) * standard_deviation.value(),
        mean_loss.value().unit(),
        standard_deviation.observations(),
        Some(standard_deviation.variance_convention()),
        mean_loss.annualization(),
        Some(confidence),
    )
}

fn ensure_returns(inputs: &[StatisticalInput], required: usize) -> Result<(), AnalyticsError> {
    validate_count(inputs.len(), required)?;
    if inputs
        .iter()
        .any(|input| input.unit() != StatisticalUnit::Return)
    {
        return Err(AnalyticsError::UnitMismatch);
    }
    Ok(())
}

fn validate_pair(
    left: &[StatisticalInput],
    right: &[StatisticalInput],
    required: usize,
) -> Result<(), AnalyticsError> {
    validate_count(left.len(), required)?;
    if left.len() != right.len() {
        return Err(AnalyticsError::LengthMismatch);
    }
    let left_unit = validate_homogeneous(left)?;
    let right_unit = validate_homogeneous(right)?;
    if left_unit != right_unit {
        return Err(AnalyticsError::UnitMismatch);
    }
    Ok(())
}

fn mean(inputs: &[StatisticalInput]) -> f64 {
    neumaier_sum(inputs.iter().map(|input| input.value())) / inputs.len() as f64
}

fn variance(
    inputs: &[StatisticalInput],
    convention: VarianceConvention,
) -> Result<f64, AnalyticsError> {
    validate_count(inputs.len(), 2)?;
    let mean = mean(inputs);
    let sum_squares = neumaier_sum(inputs.iter().map(|input| {
        let centered = input.value() - mean;
        centered * centered
    }));
    Ok(sum_squares / variance_denominator(inputs.len(), convention))
}

fn variance_denominator(len: usize, convention: VarianceConvention) -> f64 {
    match convention {
        VarianceConvention::Sample => (len - 1) as f64,
        VarianceConvention::Population => len as f64,
    }
}

fn ratio_over_sample_deviation(
    returns: &ReturnSeries,
    target: StatisticalInput,
    downside_only: bool,
) -> Result<StatisticalResult, AnalyticsError> {
    ensure_returns(returns.values(), 2)?;
    if target.unit() != StatisticalUnit::Return {
        return Err(AnalyticsError::UnitMismatch);
    }
    let excess = returns
        .values()
        .iter()
        .map(|value| value.value() - target.value())
        .collect::<Vec<_>>();
    let numerator = neumaier_sum(excess.iter().copied()) / excess.len() as f64;
    let deviation = if downside_only {
        (neumaier_sum(excess.iter().map(|value| value.min(0.0).powi(2))) / excess.len() as f64)
            .sqrt()
    } else {
        numeric_sample_variance(&excess)?.sqrt()
    };
    if deviation == 0.0 {
        return Err(AnalyticsError::ZeroVariance);
    }
    let annualization = returns.annualization();
    StatisticalResult::try_new(
        numerator / deviation * annualization.volatility_multiplier(),
        StatisticalUnit::Unitless,
        returns.observations(),
        Some(if downside_only {
            VarianceConvention::Population
        } else {
            VarianceConvention::Sample
        }),
        annualization,
        None,
    )
}

fn active_returns(
    portfolio: &ReturnSeries,
    benchmark: &ReturnSeries,
) -> Result<Vec<f64>, AnalyticsError> {
    if portfolio.annualization() != benchmark.annualization() {
        return Err(AnalyticsError::AnnualizationMismatch);
    }
    validate_pair(portfolio.values(), benchmark.values(), 2)?;
    ensure_returns(portfolio.values(), 2)?;
    Ok(portfolio
        .values()
        .iter()
        .zip(benchmark.values())
        .map(|(portfolio, benchmark)| portfolio.value() - benchmark.value())
        .collect())
}

fn numeric_sample_variance(values: &[f64]) -> Result<f64, AnalyticsError> {
    validate_count(values.len(), 2)?;
    let mean = neumaier_sum(values.iter().copied()) / values.len() as f64;
    Ok(neumaier_sum(values.iter().map(|value| (value - mean).powi(2))) / (values.len() - 1) as f64)
}

// Peter J. Acklam's deterministic rational approximation. The input is already bounded to (0, 1).
fn inverse_standard_normal(probability: f64) -> f64 {
    const A: [f64; 6] = [
        -3.969_683_028_665_376e1,
        2.209_460_984_245_205e2,
        -2.759_285_104_469_687e2,
        1.383_577_518_672_69e2,
        -3.066_479_806_614_716e1,
        2.506_628_277_459_239,
    ];
    const B: [f64; 5] = [
        -5.447_609_879_822_406e1,
        1.615_858_368_580_409e2,
        -1.556_989_798_598_866e2,
        6.680_131_188_771_972e1,
        -1.328_068_155_288_572e1,
    ];
    const C: [f64; 6] = [
        -7.784_894_002_430_293e-3,
        -3.223_964_580_411_365e-1,
        -2.400_758_277_161_838,
        -2.549_732_539_343_734,
        4.374_664_141_464_968,
        2.938_163_982_698_783,
    ];
    const D: [f64; 4] = [
        7.784_695_709_041_462e-3,
        3.224_671_290_700_398e-1,
        2.445_134_137_142_996,
        3.754_408_661_907_416,
    ];
    const LOW: f64 = 0.024_25;
    const HIGH: f64 = 1.0 - LOW;
    if probability < LOW {
        let q = (-2.0 * probability.ln()).sqrt();
        (((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    } else if probability <= HIGH {
        let q = probability - 0.5;
        let r = q * q;
        (((((A[0] * r + A[1]) * r + A[2]) * r + A[3]) * r + A[4]) * r + A[5]) * q
            / (((((B[0] * r + B[1]) * r + B[2]) * r + B[3]) * r + B[4]) * r + 1.0)
    } else {
        let q = (-2.0 * (1.0 - probability).ln()).sqrt();
        -(((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    }
}
