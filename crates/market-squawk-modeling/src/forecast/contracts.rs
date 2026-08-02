//! Focused research forecast contracts.

use super::*;

/// Maximum future points in one admitted forecast path.
pub const MAX_FORECAST_POINTS: usize = 512;
/// Maximum base-10 fractional digits at the research forecast boundary.
pub const MAX_FORECAST_DECIMAL_SCALE: u8 = 12;
pub(super) const MAX_CALIBRATION_ASSUMPTION_BYTES: usize = 512;

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

/// Exact ordered inputs for one research forecast operation.
#[derive(Clone, Copy, Debug)]
pub struct ForecastRequest<'input> {
    pub(super) instrument_id: InstrumentId,
    pub(super) observed_cutoff: Timestamp,
    pub(super) available_at: Timestamp,
    pub(super) horizon: ForecastHorizon,
    pub(super) decimal_scale: u8,
    pub(super) inputs: &'input [ModelInput<'input>],
}

impl<'input> ForecastRequest<'input> {
    /// Constructs a point-in-time request with one input per future point.
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
        if available_at > observed_cutoff
            || decimal_scale > MAX_FORECAST_DECIMAL_SCALE
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
            inputs,
        })
    }

    /// Stable instrument being forecast.
    #[must_use]
    pub const fn instrument_id(self) -> InstrumentId {
        self.instrument_id
    }

    /// Last observation cutoff; every target is strictly later.
    #[must_use]
    pub const fn observed_cutoff(self) -> Timestamp {
        self.observed_cutoff
    }

    /// Availability time of the complete point-in-time input set.
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
    pub(super) points: Box<[ForecastPoint]>,
    pub(super) model_id: ModelId,
    pub(super) bundle_id: BundleId,
    pub(super) bundle_version: NonZeroU64,
    pub(super) metadata_hash: Sha256Digest,
    pub(super) artifact_hash: Sha256Digest,
    pub(super) training_run_hash: Sha256Digest,
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

    /// Last observed cutoff.
    #[must_use]
    pub const fn observed_cutoff(&self) -> Timestamp {
        self.observed_cutoff
    }

    /// Availability time of the complete PIT input.
    #[must_use]
    pub const fn available_at(&self) -> Timestamp {
        self.available_at
    }

    /// Exact path horizon.
    #[must_use]
    pub const fn horizon(&self) -> ForecastHorizon {
        self.horizon
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
            .saturating_add(self.universe_id.as_str().len())
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
    /// Horizon count, spacing, or timestamp arithmetic is invalid.
    #[error("forecast horizon is invalid")]
    InvalidHorizon,
    /// Point-in-time, scale, or input shape is invalid.
    #[error("forecast request is invalid")]
    InvalidRequest,
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
