//! Focused research forecast contracts.

use super::*;

/// Maximum future points in one admitted forecast path.
pub const MAX_FORECAST_POINTS: usize = 512;
/// Maximum qualified historical observations retained beside one forecast vintage.
pub const MAX_FORECAST_OBSERVED_POINTS: usize = 4_096;
/// Maximum base-10 fractional digits at the research forecast boundary.
pub const MAX_FORECAST_DECIMAL_SCALE: u8 = 12;
pub(super) const MAX_CALIBRATION_ASSUMPTION_BYTES: usize = 512;

/// Closed measurement carried by one admitted model output.
///
/// This is distinct from [`ModelOutputSemantics`]: a regression scalar is not a price unless the
/// admitted bundle authority explicitly binds an exact quote currency. Callers cannot construct a
/// [`ForecastOutputBinding`]; model admission owns that transition.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ForecastMeasurement {
    /// A modeled price in one exact quote currency.
    Price { currency: Currency },
    /// A dimensionless return under the admitted label contract.
    Return,
    /// A probability under the admitted binary-output contract.
    Probability,
    /// A regression whose measurement is explicitly not admitted as price, return, or probability.
    OtherRegression,
}

/// Admitted statistical meaning of the model's central scalar.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ForecastCentralStatistic {
    /// A sealed squared-error estimator of an exact fixed-horizon terminal price.
    ModelEstimatedConditionalMean,
    /// The output remains useful as a modeled scalar but is not admitted for expected-value use.
    Unavailable,
}

/// Exact target coordinate independently rederived from Task 11 rows.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ForecastTargetMeaning {
    /// One terminal observation at the same positive effective-time offset for every admitted row.
    FixedHorizonTerminal { horizon_nanos: NonZeroU64 },
    /// Row precision or varying offsets do not prove one terminal horizon.
    Unsupported,
}

/// Closed value transform applied at the target or model-output boundary.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ForecastTransform {
    /// Preserve the admitted value directly.
    Identity,
    /// Apply the sealed logistic link to the fitted score.
    Logistic,
}

/// Objective minimized by the sealed trainer.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ForecastTrainingObjective {
    /// Direct squared-error minimization.
    SquaredError,
    /// Direct binary cross-entropy minimization.
    BinaryCrossEntropy,
}

/// Code-owned estimator implementation profile admitted with the bundle.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ForecastEstimatorProfile {
    /// The sealed direct affine least-squares implementation in the installed Python wheel.
    SealedDirectLeastSquaresV1,
    /// The sealed direct scikit-learn Ridge profile, including the exact IEEE alpha bits.
    SealedDirectRidgeV1 { ridge_alpha_bits: u64 },
    /// The sealed direct binary logistic implementation in the installed Python wheel.
    SealedBinaryLogisticV1,
}

impl ForecastEstimatorProfile {
    /// Exact nonnegative finite Ridge alpha only for the Ridge profile.
    #[must_use]
    pub fn ridge_alpha(self) -> Option<f64> {
        match self {
            Self::SealedDirectRidgeV1 { ridge_alpha_bits } => {
                Some(f64::from_bits(ridge_alpha_bits))
            }
            Self::SealedDirectLeastSquaresV1 | Self::SealedBinaryLogisticV1 => None,
        }
    }
}

/// Versioned model-admission binding for one forecast output and exact label contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForecastOutputBinding {
    output_semantics: ModelOutputSemantics,
    measurement: ForecastMeasurement,
    central_statistic: ForecastCentralStatistic,
    target: ForecastTargetMeaning,
    target_transform: ForecastTransform,
    output_transform: ForecastTransform,
    objective: ForecastTrainingObjective,
    estimator: ForecastEstimatorProfile,
    label: FeatureLabelComponentSpec,
    identity: Sha256Digest,
}

impl ForecastOutputBinding {
    /// Creates a binding only inside the model-admission authority.
    ///
    /// The bundle metadata and its independent authority document must both carry the same closed
    /// measurement before this constructor is called. In particular, no forecast request or UI
    /// argument can assert that a generic regression is a price.
    pub(crate) fn try_from_admitted_model(
        output_semantics: ModelOutputSemantics,
        measurement: ForecastMeasurement,
        central_statistic: ForecastCentralStatistic,
        target: ForecastTargetMeaning,
        target_transform: ForecastTransform,
        output_transform: ForecastTransform,
        objective: ForecastTrainingObjective,
        estimator: ForecastEstimatorProfile,
        label: FeatureLabelComponentSpec,
    ) -> Result<Self, ForecastError> {
        let compatible = matches!(
            (output_semantics, measurement),
            (
                ModelOutputSemantics::Regression,
                ForecastMeasurement::Price { .. }
                    | ForecastMeasurement::Return
                    | ForecastMeasurement::OtherRegression
            ) | (
                ModelOutputSemantics::BinaryProbability,
                ForecastMeasurement::Probability
            )
        );
        let estimator_valid = match estimator {
            ForecastEstimatorProfile::SealedDirectRidgeV1 { ridge_alpha_bits } => {
                let alpha = f64::from_bits(ridge_alpha_bits);
                alpha.is_finite() && alpha >= 0.0
            }
            ForecastEstimatorProfile::SealedDirectLeastSquaresV1
            | ForecastEstimatorProfile::SealedBinaryLogisticV1 => true,
        };
        let regression_profile = matches!(
            estimator,
            ForecastEstimatorProfile::SealedDirectLeastSquaresV1
                | ForecastEstimatorProfile::SealedDirectRidgeV1 { .. }
        );
        let estimator_contract = match output_semantics {
            ModelOutputSemantics::Regression => {
                regression_profile
                    && objective == ForecastTrainingObjective::SquaredError
                    && output_transform == ForecastTransform::Identity
            }
            ModelOutputSemantics::BinaryProbability => {
                estimator == ForecastEstimatorProfile::SealedBinaryLogisticV1
                    && objective == ForecastTrainingObjective::BinaryCrossEntropy
                    && output_transform == ForecastTransform::Logistic
            }
        };
        let expected_value_qualified = matches!(measurement, ForecastMeasurement::Price { .. })
            && matches!(target, ForecastTargetMeaning::FixedHorizonTerminal { .. })
            && output_semantics == ModelOutputSemantics::Regression
            && target_transform == ForecastTransform::Identity
            && output_transform == ForecastTransform::Identity
            && objective == ForecastTrainingObjective::SquaredError
            && regression_profile;
        if label.kind() != ComponentKind::Label
            || label.scope() != ComponentScope::Instrument
            || !compatible
            || !estimator_valid
            || !estimator_contract
            || target_transform != ForecastTransform::Identity
            || (central_statistic == ForecastCentralStatistic::ModelEstimatedConditionalMean
                && !expected_value_qualified)
        {
            return Err(ForecastError::InvalidOutputBinding);
        }
        let identity = digest_output_binding(
            output_semantics,
            measurement,
            central_statistic,
            target,
            target_transform,
            output_transform,
            objective,
            estimator,
            &label,
        )?;
        Ok(Self {
            output_semantics,
            measurement,
            central_statistic,
            target,
            target_transform,
            output_transform,
            objective,
            estimator,
            label,
            identity,
        })
    }

    /// Closed scalar interpretation admitted with the model artifact.
    #[must_use]
    pub const fn output_semantics(&self) -> ModelOutputSemantics {
        self.output_semantics
    }

    /// Closed financial/statistical measurement admitted with the model label.
    #[must_use]
    pub const fn measurement(&self) -> ForecastMeasurement {
        self.measurement
    }

    /// Admitted statistical meaning of [`ForecastPoint::central`].
    #[must_use]
    pub const fn central_statistic(&self) -> ForecastCentralStatistic {
        self.central_statistic
    }

    /// Exact Task 11 terminal-target meaning.
    #[must_use]
    pub const fn target(&self) -> ForecastTargetMeaning {
        self.target
    }

    /// Transform applied to the admitted target before fitting.
    #[must_use]
    pub const fn target_transform(&self) -> ForecastTransform {
        self.target_transform
    }

    /// Transform applied to the fitted scalar before publication.
    #[must_use]
    pub const fn output_transform(&self) -> ForecastTransform {
        self.output_transform
    }

    /// Sealed training objective.
    #[must_use]
    pub const fn objective(&self) -> ForecastTrainingObjective {
        self.objective
    }

    /// Sealed estimator implementation profile.
    #[must_use]
    pub const fn estimator(&self) -> ForecastEstimatorProfile {
        self.estimator
    }

    /// Returns the expected-value horizon only when every required authority predicate is bound.
    #[must_use]
    pub const fn expected_terminal_price_horizon_nanos(&self) -> Option<NonZeroU64> {
        match (self.central_statistic, self.measurement, self.target) {
            (
                ForecastCentralStatistic::ModelEstimatedConditionalMean,
                ForecastMeasurement::Price { .. },
                ForecastTargetMeaning::FixedHorizonTerminal { horizon_nanos },
            ) => Some(horizon_nanos),
            _ => None,
        }
    }

    pub(super) const fn admits_path_horizon(&self, horizon: ForecastHorizon) -> bool {
        match self.expected_terminal_price_horizon_nanos() {
            Some(expected) => {
                horizon.points.get() == 1 && horizon.step_nanos.get() == expected.get()
            }
            None => true,
        }
    }

    /// Exact admitted label contract.
    #[must_use]
    pub const fn label(&self) -> &FeatureLabelComponentSpec {
        &self.label
    }

    /// Versioned canonical identity of the semantics, measurement, currency, and label contract.
    #[must_use]
    pub const fn identity(&self) -> Sha256Digest {
        self.identity
    }

    /// Exact currency only when model admission proved this path is a price forecast.
    #[must_use]
    pub const fn price_currency(&self) -> Option<Currency> {
        match self.measurement {
            ForecastMeasurement::Price { currency } => Some(currency),
            ForecastMeasurement::Return
            | ForecastMeasurement::Probability
            | ForecastMeasurement::OtherRegression => None,
        }
    }
}

fn digest_output_binding(
    output_semantics: ModelOutputSemantics,
    measurement: ForecastMeasurement,
    central_statistic: ForecastCentralStatistic,
    target: ForecastTargetMeaning,
    target_transform: ForecastTransform,
    output_transform: ForecastTransform,
    objective: ForecastTrainingObjective,
    estimator: ForecastEstimatorProfile,
    label: &FeatureLabelComponentSpec,
) -> Result<Sha256Digest, ForecastError> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/forecast-output-binding/v2\0");
    hash.update([match output_semantics {
        ModelOutputSemantics::Regression => 1,
        ModelOutputSemantics::BinaryProbability => 2,
    }]);
    match measurement {
        ForecastMeasurement::Price { currency } => {
            hash.update([1]);
            update_binding_bytes(&mut hash, currency.as_str().as_bytes())?;
        }
        ForecastMeasurement::Return => hash.update([2]),
        ForecastMeasurement::Probability => hash.update([3]),
        ForecastMeasurement::OtherRegression => hash.update([4]),
    }
    hash.update([match central_statistic {
        ForecastCentralStatistic::ModelEstimatedConditionalMean => 1,
        ForecastCentralStatistic::Unavailable => 2,
    }]);
    match target {
        ForecastTargetMeaning::FixedHorizonTerminal { horizon_nanos } => {
            hash.update([1]);
            hash.update(horizon_nanos.get().to_be_bytes());
        }
        ForecastTargetMeaning::Unsupported => hash.update([2]),
    }
    hash.update([
        transform_tag(target_transform),
        transform_tag(output_transform),
    ]);
    hash.update([match objective {
        ForecastTrainingObjective::SquaredError => 1,
        ForecastTrainingObjective::BinaryCrossEntropy => 2,
    }]);
    match estimator {
        ForecastEstimatorProfile::SealedDirectLeastSquaresV1 => hash.update([1]),
        ForecastEstimatorProfile::SealedDirectRidgeV1 { ridge_alpha_bits } => {
            hash.update([2]);
            hash.update(ridge_alpha_bits.to_be_bytes());
        }
        ForecastEstimatorProfile::SealedBinaryLogisticV1 => hash.update([3]),
    }
    hash.update([match label.kind() {
        ComponentKind::Feature => 1,
        ComponentKind::Label => 2,
    }]);
    hash.update([match label.scope() {
        ComponentScope::Instrument => 1,
        ComponentScope::Account => 2,
        ComponentScope::Global => 3,
    }]);
    hash.update([match label.corporate_actions() {
        CorporateActionSensitivity::NotApplicable => 1,
        CorporateActionSensitivity::RequiresAdjustment => 2,
    }]);
    update_binding_bytes(&mut hash, label.name().as_bytes())?;
    hash.update(label.version().get().to_be_bytes());
    Ok(Sha256Digest::new(hash.finalize().into()))
}

const fn transform_tag(value: ForecastTransform) -> u8 {
    match value {
        ForecastTransform::Identity => 1,
        ForecastTransform::Logistic => 2,
    }
}

fn update_binding_bytes(hash: &mut Sha256, bytes: &[u8]) -> Result<(), ForecastError> {
    let length = u64::try_from(bytes.len()).map_err(|_| ForecastError::InvalidOutputBinding)?;
    hash.update(length.to_be_bytes());
    hash.update(bytes);
    Ok(())
}

/// Exact count and spacing of one ordered forecast path.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ForecastHorizon {
    pub(super) points: NonZeroU16,
    pub(super) step_nanos: NonZeroU64,
}

impl ForecastHorizon {
    /// Constructs a bounded regularly spaced horizon.
    ///
    /// # Errors
    ///
    /// Rejects more than 512 points or a span outside signed timestamp arithmetic.
    pub fn try_new(points: NonZeroU16, step_nanos: NonZeroU64) -> Result<Self, ForecastError> {
        let point_count = usize::from(points.get());
        let maximum_offset = step_nanos
            .get()
            .checked_mul(u64::from(points.get()))
            .ok_or(ForecastError::InvalidHorizon)?;
        if point_count > MAX_FORECAST_POINTS || maximum_offset > i64::MAX as u64 {
            return Err(ForecastError::InvalidHorizon);
        }
        Ok(Self { points, step_nanos })
    }

    /// Number of future points.
    #[must_use]
    pub const fn points(self) -> NonZeroU16 {
        self.points
    }

    /// Exact positive spacing in nanoseconds.
    #[must_use]
    pub const fn step_nanos(self) -> NonZeroU64 {
        self.step_nanos
    }

    pub(super) fn target_at(
        self,
        cutoff: Timestamp,
        index: usize,
    ) -> Result<Timestamp, ForecastError> {
        let ordinal = u64::try_from(index)
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or(ForecastError::InvalidHorizon)?;
        let offset = self
            .step_nanos
            .get()
            .checked_mul(ordinal)
            .and_then(|value| i64::try_from(value).ok())
            .ok_or(ForecastError::InvalidHorizon)?;
        cutoff
            .checked_add_nanos(offset)
            .map_err(|_| ForecastError::InvalidHorizon)
    }
}

/// Fixed-scale signed decimal used at the research presentation boundary.
///
/// Backend `f64` scores are multiplied by `10^scale` and rounded to the nearest integer with
/// halfway values away from zero. The scale is carried with every value and never inferred from
/// display formatting.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ForecastValue {
    pub(super) mantissa: i128,
    pub(super) scale: u8,
}

impl ForecastValue {
    /// Constructs an exact mantissa/scale value.
    ///
    /// # Errors
    ///
    /// Rejects scales beyond the closed conversion policy.
    pub const fn try_new(mantissa: i128, scale: u8) -> Result<Self, ForecastError> {
        if scale > MAX_FORECAST_DECIMAL_SCALE {
            Err(ForecastError::InvalidDecimal)
        } else {
            Ok(Self { mantissa, scale })
        }
    }

    pub(super) fn try_from_f64(value: f64, scale: u8) -> Result<Self, ForecastError> {
        if !value.is_finite() || scale > MAX_FORECAST_DECIMAL_SCALE {
            return Err(ForecastError::InvalidDecimal);
        }
        let factor = 10_u64
            .checked_pow(u32::from(scale))
            .ok_or(ForecastError::InvalidDecimal)? as f64;
        let scaled = value * factor;
        if !scaled.is_finite() || scaled.abs() > i128::MAX as f64 {
            return Err(ForecastError::InvalidDecimal);
        }
        let rounded = scaled.round();
        if !rounded.is_finite() {
            return Err(ForecastError::InvalidDecimal);
        }
        Self::try_new(rounded as i128, scale)
    }

    /// Exact signed mantissa.
    #[must_use]
    pub const fn mantissa(self) -> i128 {
        self.mantissa
    }

    /// Exact base-10 fractional scale.
    #[must_use]
    pub const fn scale(self) -> u8 {
        self.scale
    }

    /// Converts back to a finite analytical scalar for metrics and rendering adapters.
    #[must_use]
    pub fn to_f64(self) -> f64 {
        self.mantissa as f64 / 10_f64.powi(i32::from(self.scale))
    }

    pub(super) fn checked_add_offset(self, offset: f64) -> Result<Self, ForecastError> {
        let offset = Self::try_from_f64(offset, self.scale)?;
        let mantissa = self
            .mantissa
            .checked_add(offset.mantissa)
            .ok_or(ForecastError::InvalidDecimal)?;
        Self::try_new(mantissa, self.scale)
    }

    fn compare(self, other: Self) -> Option<Ordering> {
        if self.scale == other.scale {
            return Some(self.mantissa.cmp(&other.mantissa));
        }
        let common = self.scale.max(other.scale);
        let left = scaled_mantissa(self, common)?;
        let right = scaled_mantissa(other, common)?;
        Some(left.cmp(&right))
    }
}

impl PartialOrd for ForecastValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.compare(*other)
    }
}

fn scaled_mantissa(value: ForecastValue, target_scale: u8) -> Option<i128> {
    let additional = u32::from(target_scale.checked_sub(value.scale)?);
    let factor = 10_i128.checked_pow(additional)?;
    value.mantissa.checked_mul(factor)
}

/// One qualified effective-time value retained as historical context for a forecast vintage.
///
/// Observations remain source/PIT-bound evidence. They cannot contain a modeled value and are
/// deliberately separate from future [`ForecastPoint`] values. [`Self::observed_at`] is the
/// effective observation time, while [`Self::available_at`] is the later-or-equal knowledge time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ForecastObservedPoint {
    observed_at: Timestamp,
    available_at: Timestamp,
    value: ForecastValue,
    source_pit_hash: Sha256Digest,
    quality: DataQuality,
}

impl ForecastObservedPoint {
    /// Constructs one non-modeled observed value with its source/PIT identity.
    ///
    /// # Errors
    ///
    /// Rejects zero source identity, availability before the effective observation, and modeled
    /// data.
    pub fn try_new(
        observed_at: Timestamp,
        available_at: Timestamp,
        value: ForecastValue,
        source_pit_hash: Sha256Digest,
        quality: DataQuality,
    ) -> Result<Self, ForecastError> {
        if source_pit_hash.bytes() == [0; 32]
            || available_at < observed_at
            || matches!(quality, DataQuality::Modeled)
        {
            return Err(ForecastError::InvalidObservedHistory);
        }
        Ok(Self {
            observed_at,
            available_at,
            value,
            source_pit_hash,
            quality,
        })
    }

    /// Effective time of the qualified observation.
    #[must_use]
    pub const fn observed_at(self) -> Timestamp {
        self.observed_at
    }

    /// Conservative knowledge time at which this observation became available to the forecast.
    #[must_use]
    pub const fn available_at(self) -> Timestamp {
        self.available_at
    }

    /// Exact observed value under the forecast decimal policy.
    #[must_use]
    pub const fn value(self) -> ForecastValue {
        self.value
    }

    /// Exact source/PIT identity.
    #[must_use]
    pub const fn source_pit_hash(self) -> Sha256Digest {
        self.source_pit_hash
    }

    /// Original observed-data quality; it is never upgraded by forecasting.
    #[must_use]
    pub const fn quality(self) -> DataQuality {
        self.quality
    }
}

#[cfg(test)]
mod observed_history_tests {
    use std::{
        num::{NonZeroU16, NonZeroU32, NonZeroU64},
        str::FromStr,
    };

    use market_squawk_data::{
        CatalogEndpointIdentity, ComponentKind, ComponentScope, CorporateActionSensitivity,
        DatasetBuildSpecDigest, DatasetId, DatasetManifestRef, DatasetSchemaRegistry,
        FeatureLabelComponentSpec, Sha256Digest, UniverseId,
    };
    use market_squawk_domain::{Currency, DataQuality, InstrumentId, ModelId, Timestamp};

    use super::{
        CalibrationBand, CalibrationEvidence, CalibrationMethod, CalibrationWindow,
        ForecastCentralStatistic, ForecastCoverage, ForecastError, ForecastEstimatorProfile,
        ForecastHorizon, ForecastIntervals, ForecastMeasurement, ForecastObservedPoint,
        ForecastOutputBinding, ForecastPath, ForecastPoint, ForecastTargetMeaning,
        ForecastTrainingObjective, ForecastTransform, ForecastValue, ForecastVintage,
        RealizedCoverage, validate_observed_history,
    };
    use crate::{
        BundleExpectations, BundleId, DecisionThresholds, ForecastCalibrationArtifacts,
        ModelFormat, ModelMetadata, ModelOutputSemantics, TrainingDatasetIdentity, TrainingPeriod,
    };

    #[derive(Clone)]
    struct CalibrationFixture {
        method: CalibrationMethod,
        window: CalibrationWindow,
        policy_hash: Sha256Digest,
        policy_size_bytes: u64,
        residuals_hash: Sha256Digest,
        residuals_size_bytes: u64,
        bands: [CalibrationBand; 3],
        assumptions: &'static str,
    }

    #[test]
    fn forecast_recommendation_evidence_is_semantic_pit_calibrated_and_content_bound()
    -> Result<(), ForecastError> {
        let value = ForecastValue::try_new(10_125, 2)?;
        assert!(
            ForecastObservedPoint::try_new(
                Timestamp::from_unix_nanos(10),
                Timestamp::from_unix_nanos(10),
                value,
                Sha256Digest::new([7; 32]),
                DataQuality::Modeled,
            )
            .is_err()
        );

        let global_label = FeatureLabelComponentSpec::try_new(
            ComponentKind::Label,
            ComponentScope::Global,
            CorporateActionSensitivity::NotApplicable,
            "price-target",
            NonZeroU32::MIN,
        )
        .map_err(|_| ForecastError::InvalidOutputBinding)?;
        assert_eq!(
            ForecastOutputBinding::try_from_admitted_model(
                ModelOutputSemantics::Regression,
                ForecastMeasurement::Price {
                    currency: Currency::try_from("USD")
                        .map_err(|_| ForecastError::InvalidOutputBinding)?,
                },
                ForecastCentralStatistic::ModelEstimatedConditionalMean,
                ForecastTargetMeaning::FixedHorizonTerminal {
                    horizon_nanos: NonZeroU64::MIN,
                },
                ForecastTransform::Identity,
                ForecastTransform::Identity,
                ForecastTrainingObjective::SquaredError,
                ForecastEstimatorProfile::SealedDirectLeastSquaresV1,
                global_label,
            ),
            Err(ForecastError::InvalidOutputBinding)
        );

        let instrument_label = instrument_label()?;
        let usd = ForecastOutputBinding::try_from_admitted_model(
            ModelOutputSemantics::Regression,
            ForecastMeasurement::Price {
                currency: Currency::try_from("USD")
                    .map_err(|_| ForecastError::InvalidOutputBinding)?,
            },
            ForecastCentralStatistic::ModelEstimatedConditionalMean,
            ForecastTargetMeaning::FixedHorizonTerminal {
                horizon_nanos: NonZeroU64::new(60).ok_or(ForecastError::InvalidOutputBinding)?,
            },
            ForecastTransform::Identity,
            ForecastTransform::Identity,
            ForecastTrainingObjective::SquaredError,
            ForecastEstimatorProfile::SealedDirectLeastSquaresV1,
            instrument_label.clone(),
        )?;
        let eur = ForecastOutputBinding::try_from_admitted_model(
            ModelOutputSemantics::Regression,
            ForecastMeasurement::Price {
                currency: Currency::try_from("EUR")
                    .map_err(|_| ForecastError::InvalidOutputBinding)?,
            },
            ForecastCentralStatistic::ModelEstimatedConditionalMean,
            ForecastTargetMeaning::FixedHorizonTerminal {
                horizon_nanos: NonZeroU64::new(60).ok_or(ForecastError::InvalidOutputBinding)?,
            },
            ForecastTransform::Identity,
            ForecastTransform::Identity,
            ForecastTrainingObjective::SquaredError,
            ForecastEstimatorProfile::SealedDirectLeastSquaresV1,
            instrument_label,
        )?;
        assert_eq!(
            usd.price_currency(),
            Some(Currency::try_from("USD").map_err(|_| ForecastError::InvalidOutputBinding)?)
        );
        assert_eq!(
            usd.central_statistic(),
            ForecastCentralStatistic::ModelEstimatedConditionalMean
        );
        assert_eq!(
            usd.expected_terminal_price_horizon_nanos(),
            NonZeroU64::new(60)
        );
        let later_horizon = ForecastOutputBinding::try_from_admitted_model(
            ModelOutputSemantics::Regression,
            ForecastMeasurement::Price {
                currency: Currency::try_from("USD")
                    .map_err(|_| ForecastError::InvalidOutputBinding)?,
            },
            ForecastCentralStatistic::ModelEstimatedConditionalMean,
            ForecastTargetMeaning::FixedHorizonTerminal {
                horizon_nanos: NonZeroU64::new(120).ok_or(ForecastError::InvalidOutputBinding)?,
            },
            ForecastTransform::Identity,
            ForecastTransform::Identity,
            ForecastTrainingObjective::SquaredError,
            ForecastEstimatorProfile::SealedDirectLeastSquaresV1,
            usd.label().clone(),
        )?;
        let unavailable_statistic = ForecastOutputBinding::try_from_admitted_model(
            ModelOutputSemantics::Regression,
            ForecastMeasurement::Price {
                currency: Currency::try_from("USD")
                    .map_err(|_| ForecastError::InvalidOutputBinding)?,
            },
            ForecastCentralStatistic::Unavailable,
            ForecastTargetMeaning::FixedHorizonTerminal {
                horizon_nanos: NonZeroU64::new(60).ok_or(ForecastError::InvalidOutputBinding)?,
            },
            ForecastTransform::Identity,
            ForecastTransform::Identity,
            ForecastTrainingObjective::SquaredError,
            ForecastEstimatorProfile::SealedDirectLeastSquaresV1,
            usd.label().clone(),
        )?;
        assert_ne!(usd.identity(), eur.identity());
        assert_ne!(usd.identity(), later_horizon.identity());
        assert_ne!(usd.identity(), unavailable_statistic.identity());
        assert_eq!(
            ForecastOutputBinding::try_from_admitted_model(
                ModelOutputSemantics::BinaryProbability,
                ForecastMeasurement::Return,
                ForecastCentralStatistic::Unavailable,
                ForecastTargetMeaning::Unsupported,
                ForecastTransform::Identity,
                ForecastTransform::Logistic,
                ForecastTrainingObjective::BinaryCrossEntropy,
                ForecastEstimatorProfile::SealedBinaryLogisticV1,
                usd.label().clone(),
            ),
            Err(ForecastError::InvalidOutputBinding)
        );

        let observed_history = [
            ForecastObservedPoint::try_new(
                Timestamp::from_unix_nanos(900),
                Timestamp::from_unix_nanos(950),
                ForecastValue::try_new(10_000, 2)?,
                Sha256Digest::new([8; 32]),
                DataQuality::Aggregated,
            )?,
            ForecastObservedPoint::try_new(
                Timestamp::from_unix_nanos(1_000),
                Timestamp::from_unix_nanos(1_100),
                ForecastValue::try_new(10_100, 2)?,
                Sha256Digest::new([9; 32]),
                DataQuality::Aggregated,
            )?,
        ];
        assert_eq!(
            validate_observed_history(
                Timestamp::from_unix_nanos(1_000),
                Timestamp::from_unix_nanos(1_099),
                2,
                &observed_history,
            ),
            Err(ForecastError::InvalidObservedHistory)
        );
        validate_observed_history(
            Timestamp::from_unix_nanos(1_000),
            Timestamp::from_unix_nanos(1_100),
            2,
            &observed_history,
        )?;

        let base = calibration_fixture()?;
        let absent = metadata(None)?;
        assert!(matches!(
            evidence(&absent, &base),
            Err(ForecastError::InvalidCalibration)
        ));

        let mut assumptions = base.clone();
        assumptions.assumptions = "stationary block dependence; marginal coverage only";
        let bound_metadata = metadata(Some(&base))?;
        let bound_calibration = evidence(&bound_metadata, &base)?;
        let usd_path = path(&bound_metadata, bound_calibration)?;
        assert_eq!(usd_path.output_binding().identity(), usd.identity());
        let mut eur_path = usd_path.clone();
        eur_path.output_binding = eur;
        let usd_vintage = ForecastVintage::try_new(
            usd_path,
            Timestamp::from_unix_nanos(1_020),
            Timestamp::from_unix_nanos(2_000),
            Sha256Digest::new([99; 32]),
        )?;
        let eur_vintage = ForecastVintage::try_new(
            eur_path,
            Timestamp::from_unix_nanos(1_020),
            Timestamp::from_unix_nanos(2_000),
            Sha256Digest::new([99; 32]),
        )?;
        assert_ne!(usd_vintage.id(), eur_vintage.id());
        let variants = [base, assumptions];
        let mut calibration_ids = Vec::new();
        let mut vintage_ids = Vec::new();
        for variant in &variants {
            let metadata = metadata(Some(variant))?;
            let calibration = evidence(&metadata, variant)?;
            calibration_ids.push(calibration.identity().bytes());
            let path = path(&metadata, calibration)?;
            let vintage = ForecastVintage::try_new(
                path,
                Timestamp::from_unix_nanos(1_020),
                Timestamp::from_unix_nanos(2_000),
                Sha256Digest::new([99; 32]),
            )?;
            vintage_ids.push(vintage.id().bytes());
        }
        assert_ne!(calibration_ids[0], calibration_ids[1]);
        assert_ne!(vintage_ids[0], vintage_ids[1]);
        Ok(())
    }

    fn instrument_label() -> Result<FeatureLabelComponentSpec, ForecastError> {
        FeatureLabelComponentSpec::try_new(
            ComponentKind::Label,
            ComponentScope::Instrument,
            CorporateActionSensitivity::RequiresAdjustment,
            "price-target",
            NonZeroU32::MIN,
        )
        .map_err(|_| ForecastError::InvalidOutputBinding)
    }

    fn calibration_fixture() -> Result<CalibrationFixture, ForecastError> {
        let total = std::num::NonZeroU64::new(80).ok_or(ForecastError::InvalidCalibration)?;
        Ok(CalibrationFixture {
            method: CalibrationMethod::MapieEnbpi,
            window: CalibrationWindow::try_new(
                Timestamp::from_unix_nanos(100),
                Timestamp::from_unix_nanos(800),
                NonZeroU32::new(80).ok_or(ForecastError::InvalidCalibration)?,
            )?,
            policy_hash: Sha256Digest::new([51; 32]),
            policy_size_bytes: 512,
            residuals_hash: Sha256Digest::new([52; 32]),
            residuals_size_bytes: 640,
            bands: [
                CalibrationBand::try_new(
                    ForecastCoverage::Fifty,
                    -0.5,
                    0.5,
                    RealizedCoverage::try_new(41, total)?,
                )?,
                CalibrationBand::try_new(
                    ForecastCoverage::Eighty,
                    -1.0,
                    1.0,
                    RealizedCoverage::try_new(65, total)?,
                )?,
                CalibrationBand::try_new(
                    ForecastCoverage::NinetyFive,
                    -2.0,
                    2.0,
                    RealizedCoverage::try_new(76, total)?,
                )?,
            ],
            assumptions: "block bootstrap dependence; marginal coverage only",
        })
    }

    fn metadata(calibration: Option<&CalibrationFixture>) -> Result<ModelMetadata, ForecastError> {
        let schema = DatasetSchemaRegistry::local()
            .canonical_feature_labels()
            .map_err(|_| ForecastError::InvalidCalibration)?;
        let manifest = DatasetManifestRef::try_new_with_schema(
            DatasetId::try_from("forecast-integrity")
                .map_err(|_| ForecastError::InvalidCalibration)?,
            1,
            schema,
            Sha256Digest::new([21; 32]),
        )
        .map_err(|_| ForecastError::InvalidCalibration)?;
        let dataset = TrainingDatasetIdentity::try_new(
            manifest,
            DatasetBuildSpecDigest::try_new([22; 32])
                .map_err(|_| ForecastError::InvalidCalibration)?,
            Sha256Digest::new([23; 32]),
            Sha256Digest::new([24; 32]),
            CatalogEndpointIdentity::try_from_bytes([25; 32])
                .ok_or(ForecastError::InvalidCalibration)?,
            Sha256Digest::new([26; 32]),
            Sha256Digest::new([27; 32]),
            Timestamp::from_unix_nanos(900),
            std::num::NonZeroU64::MIN,
        )
        .map_err(|_| ForecastError::InvalidCalibration)?;
        let label = instrument_label()?;
        let output_binding = ForecastOutputBinding::try_from_admitted_model(
            ModelOutputSemantics::Regression,
            ForecastMeasurement::Price {
                currency: Currency::try_from("USD")
                    .map_err(|_| ForecastError::InvalidOutputBinding)?,
            },
            ForecastCentralStatistic::ModelEstimatedConditionalMean,
            ForecastTargetMeaning::FixedHorizonTerminal {
                horizon_nanos: NonZeroU64::new(60).ok_or(ForecastError::InvalidOutputBinding)?,
            },
            ForecastTransform::Identity,
            ForecastTransform::Identity,
            ForecastTrainingObjective::SquaredError,
            ForecastEstimatorProfile::SealedDirectLeastSquaresV1,
            label.clone(),
        )?;
        let expectations = BundleExpectations::try_new_with_output_binding(
            ModelId::from_str("018f3c2a-91ab-7ccd-b3de-123456789aaa")
                .map_err(|_| ForecastError::InvalidCalibration)?,
            BundleId::try_new("forecast-integrity")
                .map_err(|_| ForecastError::InvalidCalibration)?,
            std::num::NonZeroU64::MIN,
            dataset,
            UniverseId::try_from("forecast-integrity")
                .map_err(|_| ForecastError::InvalidCalibration)?,
            TrainingPeriod::try_new(
                Timestamp::from_unix_nanos(10),
                Timestamp::from_unix_nanos(20),
            )
            .map_err(|_| ForecastError::InvalidCalibration)?,
            label,
            "forecast-integrity-v1",
            Sha256Digest::new([28; 32]),
            Sha256Digest::new([29; 32]),
            Sha256Digest::new([30; 32]),
            Sha256Digest::new([31; 32]),
            output_binding,
        )
        .map_err(|_| ForecastError::InvalidCalibration)?;
        let metadata = ModelMetadata::new(
            &expectations,
            Sha256Digest::new([29; 32]),
            Sha256Digest::new([30; 32]),
            ModelFormat::Onnx,
            1,
            Vec::new(),
            Vec::new(),
            DecisionThresholds::new(-0.5, 0.5, 0.0),
            "forecast integrity proof".to_owned(),
            vec!["research-only modeled evidence".to_owned()],
            "no action when evidence is unavailable".to_owned(),
        );
        Ok(metadata.with_forecast_calibration(calibration.map(|value| {
            ForecastCalibrationArtifacts::new(
                value.method,
                value.window,
                value.policy_hash,
                value.policy_size_bytes,
                value.residuals_hash,
                value.residuals_size_bytes,
                value.bands,
                value.assumptions.to_owned(),
            )
        })))
    }

    fn evidence(
        metadata: &ModelMetadata,
        value: &CalibrationFixture,
    ) -> Result<CalibrationEvidence, ForecastError> {
        CalibrationEvidence::try_new(
            metadata,
            value.method,
            value.window,
            value.policy_hash,
            value.residuals_hash,
            value.bands,
            value.assumptions,
        )
    }

    fn path(
        metadata: &ModelMetadata,
        calibration: CalibrationEvidence,
    ) -> Result<ForecastPath, ForecastError> {
        let horizon = ForecastHorizon::try_new(
            NonZeroU16::MIN,
            std::num::NonZeroU64::new(60).ok_or(ForecastError::InvalidHorizon)?,
        )?;
        let central = ForecastValue::try_new(10_500, 2)?;
        let intervals = ForecastIntervals::from_calibration(central, &calibration)?;
        let observation = ForecastObservedPoint::try_new(
            Timestamp::from_unix_nanos(1_000),
            Timestamp::from_unix_nanos(1_000),
            ForecastValue::try_new(10_100, 2)?,
            Sha256Digest::new([9; 32]),
            DataQuality::Aggregated,
        )?;
        Ok(ForecastPath {
            instrument_id: InstrumentId::from_str("018f3c2a-91ab-7ccd-b3de-123456789bbb")
                .map_err(|_| ForecastError::InvalidRequest)?,
            observed_cutoff: Timestamp::from_unix_nanos(1_000),
            available_at: Timestamp::from_unix_nanos(1_000),
            horizon,
            observed_history: vec![observation].into_boxed_slice(),
            points: vec![ForecastPoint {
                target_at: Timestamp::from_unix_nanos(1_060),
                central,
                intervals: Some(intervals),
            }]
            .into_boxed_slice(),
            model_id: metadata.model_id(),
            bundle_id: metadata.bundle_id().clone(),
            bundle_version: metadata.bundle_version(),
            metadata_hash: metadata.metadata_hash(),
            artifact_hash: metadata.artifact_hash(),
            training_run_hash: metadata.training_run_hash(),
            output_binding: metadata.output_binding().clone(),
            dataset: metadata.dataset().clone(),
            universe_id: metadata.universe_id().clone(),
            training_period: metadata.training_period(),
            feature_semantic_digests: metadata.feature_semantic_digests().into(),
            calibration: Some(calibration),
            quality: DataQuality::Modeled,
            limitations: metadata.limitations().into(),
            fallback_reason: metadata.fallback_reason().into(),
        })
    }
}

/// Exact ordered inputs for one research forecast operation.
#[derive(Clone, Copy, Debug)]
pub struct ForecastRequest<'input> {
    pub(super) instrument_id: InstrumentId,
    pub(super) observed_cutoff: Timestamp,
    pub(super) available_at: Timestamp,
    pub(super) horizon: ForecastHorizon,
    pub(super) decimal_scale: u8,
    pub(super) observed_history: &'input [ForecastObservedPoint],
    pub(super) inputs: &'input [ModelInput<'input>],
}

impl<'input> ForecastRequest<'input> {
    /// Constructs a point-in-time request with one input per future point.
    ///
    /// `observed_cutoff` is the final effective-time boundary. `available_at` is the conservative
    /// knowledge time of the complete input and may follow that boundary.
    #[allow(
        clippy::too_many_arguments,
        reason = "instrument, time, horizon, decimal, and exact model inputs are independent"
    )]
    pub fn try_new(
        instrument_id: InstrumentId,
        observed_cutoff: Timestamp,
        available_at: Timestamp,
        horizon: ForecastHorizon,
        decimal_scale: u8,
        inputs: &'input [ModelInput<'input>],
    ) -> Result<Self, ForecastError> {
        if decimal_scale > MAX_FORECAST_DECIMAL_SCALE
            || available_at < observed_cutoff
            || inputs.len() != usize::from(horizon.points().get())
        {
            return Err(ForecastError::InvalidRequest);
        }
        horizon.target_at(observed_cutoff, inputs.len() - 1)?;
        Ok(Self {
            instrument_id,
            observed_cutoff,
            available_at,
            horizon,
            decimal_scale,
            observed_history: &[],
            inputs,
        })
    }

    /// Constructs a point-in-time request with required qualified observed-history context.
    ///
    /// The public installed-product boundary uses this constructor so predictive presentation has
    /// the exact source-qualified series ending at the effective forecast boundary. The complete
    /// input's knowledge time may follow that boundary, including for delayed bars or fundamentals.
    #[allow(
        clippy::too_many_arguments,
        reason = "the observed evidence is independent from future inputs and PIT coordinates"
    )]
    pub fn try_new_with_observed_history(
        instrument_id: InstrumentId,
        observed_cutoff: Timestamp,
        available_at: Timestamp,
        horizon: ForecastHorizon,
        decimal_scale: u8,
        observed_history: &'input [ForecastObservedPoint],
        inputs: &'input [ModelInput<'input>],
    ) -> Result<Self, ForecastError> {
        if decimal_scale > MAX_FORECAST_DECIMAL_SCALE
            || available_at < observed_cutoff
            || inputs.len() != usize::from(horizon.points().get())
            || observed_history.is_empty()
            || observed_history.len() > MAX_FORECAST_OBSERVED_POINTS
        {
            return Err(ForecastError::InvalidRequest);
        }
        validate_observed_history(
            observed_cutoff,
            available_at,
            decimal_scale,
            observed_history,
        )?;
        horizon.target_at(observed_cutoff, inputs.len() - 1)?;
        Ok(Self {
            instrument_id,
            observed_cutoff,
            available_at,
            horizon,
            decimal_scale,
            observed_history,
            inputs,
        })
    }

    /// Stable instrument being forecast.
    #[must_use]
    pub const fn instrument_id(self) -> InstrumentId {
        self.instrument_id
    }

    /// Last effective observation cutoff; every target is strictly later in effective time.
    #[must_use]
    pub const fn observed_cutoff(self) -> Timestamp {
        self.observed_cutoff
    }

    /// Conservative knowledge time of the complete point-in-time input set.
    ///
    /// This may be later than [`Self::observed_cutoff`] when an input is delayed.
    #[must_use]
    pub const fn available_at(self) -> Timestamp {
        self.available_at
    }

    /// Exact count and spacing.
    #[must_use]
    pub const fn horizon(self) -> ForecastHorizon {
        self.horizon
    }

    /// Fixed decimal scale used for central values and every band.
    #[must_use]
    pub const fn decimal_scale(self) -> u8 {
        self.decimal_scale
    }

    /// Ordered qualified history ending exactly at the effective forecast cutoff.
    #[must_use]
    pub const fn observed_history(self) -> &'input [ForecastObservedPoint] {
        self.observed_history
    }
}

fn validate_observed_history(
    observed_cutoff: Timestamp,
    available_at: Timestamp,
    decimal_scale: u8,
    observed_history: &[ForecastObservedPoint],
) -> Result<(), ForecastError> {
    if observed_history.iter().any(|point| {
        point.value.scale() != decimal_scale
            || point.observed_at > observed_cutoff
            || point.available_at > available_at
    }) || observed_history
        .windows(2)
        .any(|pair| pair[0].observed_at >= pair[1].observed_at)
        || observed_history
            .last()
            .is_none_or(|point| point.observed_at != observed_cutoff)
    {
        Err(ForecastError::InvalidObservedHistory)
    } else {
        Ok(())
    }
}

/// One finite ordered interval.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ForecastInterval {
    pub(super) lower: ForecastValue,
    pub(super) upper: ForecastValue,
}

impl ForecastInterval {
    fn try_new(lower: ForecastValue, upper: ForecastValue) -> Result<Self, ForecastError> {
        if lower.scale() != upper.scale() || lower > upper {
            return Err(ForecastError::InvalidInterval);
        }
        Ok(Self { lower, upper })
    }

    /// Inclusive lower bound.
    #[must_use]
    pub const fn lower(self) -> ForecastValue {
        self.lower
    }

    /// Inclusive upper bound.
    #[must_use]
    pub const fn upper(self) -> ForecastValue {
        self.upper
    }
}

/// Exactly the three ordered product intervals.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ForecastIntervals {
    pub(super) interval_50: ForecastInterval,
    pub(super) interval_80: ForecastInterval,
    pub(super) interval_95: ForecastInterval,
}

impl ForecastIntervals {
    pub(super) fn from_calibration(
        central: ForecastValue,
        evidence: &CalibrationEvidence,
    ) -> Result<Self, ForecastError> {
        let intervals = evidence.bands.map(|band| {
            ForecastInterval::try_new(
                central.checked_add_offset(band.lower_offset)?,
                central.checked_add_offset(band.upper_offset)?,
            )
        });
        let [interval_50, interval_80, interval_95] = intervals;
        let interval_50 = interval_50?;
        let interval_80 = interval_80?;
        let interval_95 = interval_95?;
        if interval_95.lower > interval_80.lower
            || interval_80.lower > interval_50.lower
            || interval_50.lower > central
            || central > interval_50.upper
            || interval_50.upper > interval_80.upper
            || interval_80.upper > interval_95.upper
        {
            return Err(ForecastError::InvalidInterval);
        }
        Ok(Self {
            interval_50,
            interval_80,
            interval_95,
        })
    }

    /// 50 percent target interval.
    #[must_use]
    pub const fn interval_50(self) -> ForecastInterval {
        self.interval_50
    }

    /// 80 percent target interval.
    #[must_use]
    pub const fn interval_80(self) -> ForecastInterval {
        self.interval_80
    }

    /// 95 percent target interval.
    #[must_use]
    pub const fn interval_95(self) -> ForecastInterval {
        self.interval_95
    }
}

/// One future point, never confused with an observed market value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ForecastPoint {
    pub(super) target_at: Timestamp,
    pub(super) central: ForecastValue,
    pub(super) intervals: Option<ForecastIntervals>,
}

impl ForecastPoint {
    /// Exact future target time.
    #[must_use]
    pub const fn target_at(self) -> Timestamp {
        self.target_at
    }

    /// Central modeled value under the recorded decimal policy.
    #[must_use]
    pub const fn central(self) -> ForecastValue {
        self.central
    }

    /// Calibrated 50/80/95 intervals, or none when calibration is unavailable.
    #[must_use]
    pub const fn intervals(self) -> Option<ForecastIntervals> {
        self.intervals
    }
}

/// Complete research forecast path with exact model/data/PIT identities.
#[derive(Clone, Debug, PartialEq)]
pub struct ForecastPath {
    pub(super) instrument_id: InstrumentId,
    pub(super) observed_cutoff: Timestamp,
    pub(super) available_at: Timestamp,
    pub(super) horizon: ForecastHorizon,
    pub(super) observed_history: Box<[ForecastObservedPoint]>,
    pub(super) points: Box<[ForecastPoint]>,
    pub(super) model_id: ModelId,
    pub(super) bundle_id: BundleId,
    pub(super) bundle_version: NonZeroU64,
    pub(super) metadata_hash: Sha256Digest,
    pub(super) artifact_hash: Sha256Digest,
    pub(super) training_run_hash: Sha256Digest,
    pub(super) output_binding: ForecastOutputBinding,
    pub(super) dataset: TrainingDatasetIdentity,
    pub(super) universe_id: UniverseId,
    pub(super) training_period: TrainingPeriod,
    pub(super) feature_semantic_digests: Box<[FeatureSemanticDigest]>,
    pub(super) calibration: Option<CalibrationEvidence>,
    pub(super) quality: DataQuality,
    pub(super) limitations: Box<[Box<str>]>,
    pub(super) fallback_reason: Box<str>,
}

impl ForecastPath {
    /// Stable forecasted instrument.
    #[must_use]
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Last effective observation cutoff.
    #[must_use]
    pub const fn observed_cutoff(&self) -> Timestamp {
        self.observed_cutoff
    }

    /// Conservative knowledge time of the complete PIT input.
    ///
    /// This may follow [`Self::observed_cutoff`] for delayed observations or features.
    #[must_use]
    pub const fn available_at(&self) -> Timestamp {
        self.available_at
    }

    /// Exact path horizon.
    #[must_use]
    pub const fn horizon(&self) -> ForecastHorizon {
        self.horizon
    }

    /// Qualified effective-time history ending at [`Self::observed_cutoff`].
    #[must_use]
    pub fn observed_history(&self) -> &[ForecastObservedPoint] {
        &self.observed_history
    }

    /// Ordered future points.
    #[must_use]
    pub fn points(&self) -> &[ForecastPoint] {
        &self.points
    }

    /// Stable producing model.
    #[must_use]
    pub const fn model_id(&self) -> ModelId {
        self.model_id
    }

    /// Stable bundle series.
    #[must_use]
    pub const fn bundle_id(&self) -> &BundleId {
        &self.bundle_id
    }

    /// Immutable bundle generation.
    #[must_use]
    pub const fn bundle_version(&self) -> NonZeroU64 {
        self.bundle_version
    }

    /// Exact metadata artifact digest.
    #[must_use]
    pub const fn metadata_hash(&self) -> Sha256Digest {
        self.metadata_hash
    }

    /// Exact central model artifact digest.
    #[must_use]
    pub const fn artifact_hash(&self) -> Sha256Digest {
        self.artifact_hash
    }

    /// Exact training/calibration run artifact digest.
    #[must_use]
    pub const fn training_run_hash(&self) -> Sha256Digest {
        self.training_run_hash
    }

    /// Closed admitted output measurement and exact label contract.
    #[must_use]
    pub const fn output_binding(&self) -> &ForecastOutputBinding {
        &self.output_binding
    }

    /// Exact point-in-time training dataset generation.
    #[must_use]
    pub const fn dataset(&self) -> &TrainingDatasetIdentity {
        &self.dataset
    }

    /// Stable training universe.
    #[must_use]
    pub const fn universe_id(&self) -> &UniverseId {
        &self.universe_id
    }

    /// Training/validation time evidence.
    #[must_use]
    pub const fn training_period(&self) -> TrainingPeriod {
        self.training_period
    }

    /// Ordered feature semantics.
    #[must_use]
    pub fn feature_semantic_digests(&self) -> &[FeatureSemanticDigest] {
        &self.feature_semantic_digests
    }

    /// Interval evidence, absent when bands are unavailable.
    #[must_use]
    pub const fn calibration(&self) -> Option<&CalibrationEvidence> {
        self.calibration.as_ref()
    }

    /// Always [`DataQuality::Modeled`]; forecasts cannot mint direct evidence.
    #[must_use]
    pub const fn quality(&self) -> DataQuality {
        self.quality
    }

    /// Bounded declared limitations shown with the path.
    #[must_use]
    pub fn limitations(&self) -> &[Box<str>] {
        &self.limitations
    }

    /// Explicit no-forecast/no-action fallback reason.
    #[must_use]
    pub fn fallback_reason(&self) -> &str {
        &self.fallback_reason
    }

    /// Approximate retained heap charge for bounded repositories.
    #[must_use]
    pub fn retained_bytes(&self) -> usize {
        let calibration = self.calibration.as_ref().map_or(0, |value| {
            size_of::<CalibrationEvidence>()
                .saturating_add(
                    value.feature_semantic_digests.len() * size_of::<FeatureSemanticDigest>(),
                )
                .saturating_add(value.dependence_assumptions.len())
        });
        size_of::<Self>()
            .saturating_add(self.bundle_id.as_str().len())
            .saturating_add(self.output_binding.label().name().len())
            .saturating_add(self.universe_id.as_str().len())
            .saturating_add(self.observed_history.len() * size_of::<ForecastObservedPoint>())
            .saturating_add(self.points.len() * size_of::<ForecastPoint>())
            .saturating_add(
                self.feature_semantic_digests.len() * size_of::<FeatureSemanticDigest>(),
            )
            .saturating_add(
                self.limitations
                    .iter()
                    .map(|value| value.len())
                    .sum::<usize>(),
            )
            .saturating_add(self.fallback_reason.len())
            .saturating_add(calibration)
    }
}

/// Research forecast validation or inference failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ForecastError {
    /// Model admission did not bind a compatible closed output measurement and label contract.
    #[error("forecast output measurement binding is invalid")]
    InvalidOutputBinding,
    /// Horizon count, spacing, or timestamp arithmetic is invalid.
    #[error("forecast horizon is invalid")]
    InvalidHorizon,
    /// Point-in-time, scale, or input shape is invalid.
    #[error("forecast request is invalid")]
    InvalidRequest,
    /// Qualified observed-history context is missing, unordered, modeled, or incompatible.
    #[error("forecast observed-history context is invalid")]
    InvalidObservedHistory,
    /// A backend score cannot be represented under the fixed decimal policy.
    #[error("forecast decimal conversion is invalid")]
    InvalidDecimal,
    /// Calibration evidence, window, bands, or assumptions are invalid.
    #[error("forecast calibration evidence is invalid")]
    InvalidCalibration,
    /// Calibration belongs to another exact model/data generation.
    #[error("forecast calibration identity does not match the backend")]
    CalibrationIdentityMismatch,
    /// Produced intervals are nonordered or nonnested.
    #[error("forecast interval contract is invalid")]
    InvalidInterval,
    /// Immutable vintage publication time or artifact evidence is invalid.
    #[error("forecast vintage is invalid")]
    InvalidVintage,
    /// Outcome evidence is invalid or attempts an evidentiary quality promotion.
    #[error("forecast outcome evidence is invalid")]
    InvalidOutcome,
    /// Outcome does not name a target in the exact vintage.
    #[error("forecast outcome target does not exist in the vintage")]
    OutcomeTargetMismatch,
    /// Bounded retained allocation failed.
    #[error("forecast retained capacity is unavailable")]
    Capacity,
    /// Scalar inference failed; no partial path is returned.
    #[error("forecast central inference failed: {0}")]
    Inference(#[from] InferenceError),
}
