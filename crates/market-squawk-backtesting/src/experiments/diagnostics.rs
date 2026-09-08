//! Bounded probability-of-overfitting and deflated-performance diagnostics.

use super::ExperimentError;

const MAX_DIAGNOSTIC_FOLDS: usize = 4_096;
const MAX_DIAGNOSTIC_CANDIDATES: usize = 65_536;

/// In-sample and out-of-sample candidates for one predefined combinatorial fold.
#[derive(Clone, Debug)]
pub struct BacktestOverfittingFold {
    pub candidates: Vec<BacktestOverfittingScore>,
}

/// One strategy candidate's paired fold scores.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BacktestOverfittingScore {
    pub in_sample: f64,
    pub out_of_sample: f64,
}

/// Probability-of-backtest-overfitting diagnostic input.
#[derive(Clone, Debug)]
pub struct BacktestOverfittingInput {
    pub folds: Vec<BacktestOverfittingFold>,
}

/// Bounded probability that the in-sample winner ranks below median out of sample.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BacktestOverfittingDiagnostic {
    pub(super) probability: f64,
    pub(super) folds: usize,
}

impl BacktestOverfittingDiagnostic {
    /// Computes the CSCV-style below-median rank frequency across caller-defined folds.
    pub fn try_compute(input: &BacktestOverfittingInput) -> Result<Self, ExperimentError> {
        if input.folds.len() < 2 || input.folds.len() > MAX_DIAGNOSTIC_FOLDS {
            return Err(ExperimentError::InvalidDiagnostic);
        }
        let mut below_median = 0_usize;
        let mut candidates_total = 0_usize;
        for fold in &input.folds {
            if fold.candidates.len() < 2 {
                return Err(ExperimentError::InvalidDiagnostic);
            }
            candidates_total = candidates_total
                .checked_add(fold.candidates.len())
                .ok_or(ExperimentError::InvalidDiagnostic)?;
            if candidates_total > MAX_DIAGNOSTIC_CANDIDATES
                || fold
                    .candidates
                    .iter()
                    .any(|score| !score.in_sample.is_finite() || !score.out_of_sample.is_finite())
            {
                return Err(ExperimentError::InvalidDiagnostic);
            }
            let winner = fold
                .candidates
                .iter()
                .enumerate()
                .max_by(|(left_index, left), (right_index, right)| {
                    left.in_sample
                        .total_cmp(&right.in_sample)
                        .then_with(|| right_index.cmp(left_index))
                })
                .map(|(index, _)| index)
                .ok_or(ExperimentError::InvalidDiagnostic)?;
            let winner_out = fold
                .candidates
                .get(winner)
                .ok_or(ExperimentError::InvalidDiagnostic)?
                .out_of_sample;
            let not_better = fold
                .candidates
                .iter()
                .filter(|candidate| candidate.out_of_sample <= winner_out)
                .count();
            if not_better.saturating_mul(2) <= fold.candidates.len() {
                below_median = below_median
                    .checked_add(1)
                    .ok_or(ExperimentError::InvalidDiagnostic)?;
            }
        }
        Ok(Self {
            probability: below_median as f64 / input.folds.len() as f64,
            folds: input.folds.len(),
        })
    }

    /// Returns the probability in `[0, 1]`.
    #[must_use]
    pub const fn probability(self) -> f64 {
        self.probability
    }

    /// Returns the number of independently materialized folds used by the diagnostic.
    #[must_use]
    pub const fn fold_count(self) -> usize {
        self.folds
    }
}

/// Inputs to the multiple-testing-adjusted deflated Sharpe probability.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DeflatedPerformanceInput {
    pub observed_sharpe: f64,
    pub independent_trials: usize,
    pub observations: usize,
    pub trial_sharpe_variance: f64,
    pub return_skewness: f64,
    pub return_excess_kurtosis: f64,
}

/// Deflated-performance probability and the expected best spurious Sharpe hurdle.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DeflatedPerformanceDiagnostic {
    pub(super) probability: f64,
    pub(super) expected_maximum_sharpe: f64,
}

impl DeflatedPerformanceDiagnostic {
    /// Computes a finite multiple-testing and nonnormal-return-adjusted probability.
    pub fn try_compute(input: DeflatedPerformanceInput) -> Result<Self, ExperimentError> {
        let finite = [
            input.observed_sharpe,
            input.trial_sharpe_variance,
            input.return_skewness,
            input.return_excess_kurtosis,
        ]
        .into_iter()
        .all(f64::is_finite);
        if !finite
            || input.independent_trials < 2
            || input.observations < 3
            || input.trial_sharpe_variance < 0.0
            || input.return_excess_kurtosis < -2.0
        {
            return Err(ExperimentError::InvalidDiagnostic);
        }
        let trials = input.independent_trials as f64;
        let euler_gamma = 0.577_215_664_901_532_9_f64;
        let first = inverse_standard_normal(1.0 - 1.0 / trials)?;
        let second = inverse_standard_normal(1.0 - 1.0 / (trials * std::f64::consts::E))?;
        let expected_maximum_sharpe = input.trial_sharpe_variance.sqrt()
            * ((1.0 - euler_gamma) * first + euler_gamma * second);
        let denominator_squared = 1.0 - input.return_skewness * input.observed_sharpe
            + ((input.return_excess_kurtosis + 2.0) / 4.0)
                * input.observed_sharpe
                * input.observed_sharpe;
        if denominator_squared <= 0.0 || !expected_maximum_sharpe.is_finite() {
            return Err(ExperimentError::InvalidDiagnostic);
        }
        let z = (input.observed_sharpe - expected_maximum_sharpe)
            * ((input.observations - 1) as f64).sqrt()
            / denominator_squared.sqrt();
        let probability = standard_normal_cdf(z);
        if !probability.is_finite() || !(0.0..=1.0).contains(&probability) {
            return Err(ExperimentError::InvalidDiagnostic);
        }
        Ok(Self {
            probability,
            expected_maximum_sharpe,
        })
    }

    /// Returns the deflated probability in `[0, 1]`.
    #[must_use]
    pub const fn probability(self) -> f64 {
        self.probability
    }

    /// Returns the multiple-testing-adjusted Sharpe hurdle used by the diagnostic.
    #[must_use]
    pub const fn expected_maximum_sharpe(self) -> f64 {
        self.expected_maximum_sharpe
    }
}

fn standard_normal_cdf(value: f64) -> f64 {
    let absolute = value.abs();
    let t = 1.0 / (1.0 + 0.231_641_9 * absolute);
    let density = (-0.5 * absolute * absolute).exp() / (2.0 * std::f64::consts::PI).sqrt();
    let tail = density
        * t
        * (0.319_381_530
            + t * (-0.356_563_782
                + t * (1.781_477_937 + t * (-1.821_255_978 + t * 1.330_274_429))));
    if value >= 0.0 { 1.0 - tail } else { tail }
}

fn inverse_standard_normal(probability: f64) -> Result<f64, ExperimentError> {
    if !probability.is_finite() || !(0.0..1.0).contains(&probability) {
        return Err(ExperimentError::InvalidDiagnostic);
    }
    const A: [f64; 6] = [
        -39.696_830_286_653_76,
        220.946_098_424_520_5,
        -275.928_510_446_968_7,
        138.357_751_867_269,
        -30.664_798_066_147_16,
        2.506_628_277_459_239,
    ];
    const B: [f64; 5] = [
        -54.476_098_798_224_06,
        161.585_836_858_040_9,
        -155.698_979_859_886_6,
        66.801_311_887_719_72,
        -13.280_681_552_885_72,
    ];
    const C: [f64; 6] = [
        -0.007_784_894_002_430_293,
        -0.322_396_458_041_136_5,
        -2.400_758_277_161_838,
        -2.549_732_539_343_734,
        4.374_664_141_464_968,
        2.938_163_982_698_783,
    ];
    const D: [f64; 4] = [
        0.007_784_695_709_041_462,
        0.322_467_129_070_039_8,
        2.445_134_137_142_996,
        3.754_408_661_907_416,
    ];
    let result = if probability < 0.024_25 {
        let q = (-2.0 * probability.ln()).sqrt();
        (((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    } else if probability > 1.0 - 0.024_25 {
        let q = (-2.0 * (1.0 - probability).ln()).sqrt();
        -(((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    } else {
        let q = probability - 0.5;
        let r = q * q;
        (((((A[0] * r + A[1]) * r + A[2]) * r + A[3]) * r + A[4]) * r + A[5]) * q
            / (((((B[0] * r + B[1]) * r + B[2]) * r + B[3]) * r + B[4]) * r + 1.0)
    };
    if result.is_finite() {
        Ok(result)
    } else {
        Err(ExperimentError::InvalidDiagnostic)
    }
}
