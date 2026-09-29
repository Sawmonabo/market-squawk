//! Bundle-bound calibration and interval-policy evidence.

use super::contracts::MAX_CALIBRATION_ASSUMPTION_BYTES;
use super::*;
use market_squawk_domain::{CalendarDate, ResearchTemporalCoordinate};

/// Closed target coverage for the three product forecast bands.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ForecastCoverage {
    /// 50 percent target marginal coverage.
    Fifty,
    /// 80 percent target marginal coverage.
    Eighty,
    /// 95 percent target marginal coverage.
    NinetyFive,
}

impl ForecastCoverage {
    /// Integer basis points used in canonical evidence identities.
    #[must_use]
    pub const fn basis_points(self) -> u16 {
        match self {
            Self::Fifty => 5_000,
            Self::Eighty => 8_000,
            Self::NinetyFive => 9_500,
        }
    }
}

/// Exact empirical coverage observation, never a future guarantee.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RealizedCoverage {
    pub(super) covered: u64,
    pub(super) total: NonZeroU64,
}

impl RealizedCoverage {
    /// Constructs an empirical covered/total observation.
    ///
    /// # Errors
    ///
    /// Rejects a covered count greater than the evaluated count.
    pub const fn try_new(covered: u64, total: NonZeroU64) -> Result<Self, ForecastError> {
        if covered > total.get() {
            Err(ForecastError::InvalidCalibration)
        } else {
            Ok(Self { covered, total })
        }
    }

    /// Covered validation observations.
    #[must_use]
    pub const fn covered(self) -> u64 {
        self.covered
    }

    /// Total validation observations.
    #[must_use]
    pub const fn total(self) -> NonZeroU64 {
        self.total
    }
}

/// Closed interval-production family retained as model-risk evidence.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CalibrationMethod {
    /// MAPIE EnbPI with explicitly recorded block-bootstrap dependence assumptions.
    MapieEnbpi,
    /// MAPIE adaptive conformal inference.
    MapieAci,
    /// Separately labelled empirical quantile interval, not conformal evidence.
    ResidualQuantile,
}

/// Exact historical interval used for calibration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CalibrationWindow {
    period: TrainingPeriod,
    pub(super) observations: NonZeroU32,
}

impl CalibrationWindow {
    /// Constructs a nonempty calibration window.
    pub fn try_new(
        start: Timestamp,
        end: Timestamp,
        observations: NonZeroU32,
    ) -> Result<Self, ForecastError> {
        let period =
            TrainingPeriod::try_new(start, end).map_err(|_| ForecastError::InvalidCalibration)?;
        Ok(Self {
            period,
            observations,
        })
    }

    /// Admits a native calendar-date calibration interval.
    pub fn try_fiscal(
        start: CalendarDate,
        end: CalendarDate,
        observations: NonZeroU32,
    ) -> Result<Self, ForecastError> {
        let period = TrainingPeriod::try_fiscal(start, end)
            .map_err(|_| ForecastError::InvalidCalibration)?;
        Ok(Self {
            period,
            observations,
        })
    }

    /// Inclusive calibration start.
    #[must_use]
    pub const fn start(self) -> Option<Timestamp> {
        self.period.start()
    }

    /// Exclusive calibration end.
    #[must_use]
    pub const fn end(self) -> Option<Timestamp> {
        self.period.end()
    }

    /// Exact fiscal bounds, with no assumed midnight.
    pub const fn fiscal_bounds(self) -> Option<[CalendarDate; 2]> {
        self.period.fiscal_bounds()
    }
    /// Inclusive economic start retaining its precision.
    pub fn start_coordinate(self) -> ResearchTemporalCoordinate {
        self.period.start_coordinate()
    }
    /// Exclusive economic end retaining its precision.
    pub fn end_coordinate(self) -> ResearchTemporalCoordinate {
        self.period.end_coordinate()
    }
    /// Whether all evaluated coordinates have ended by the actual clock.
    pub fn ends_by(self, as_of: Timestamp) -> bool {
        self.period.ends_by(as_of)
    }

    /// Admitted calibration observations.
    #[must_use]
    pub const fn observations(self) -> NonZeroU32 {
        self.observations
    }
}

/// Untouched evaluation of one already frozen model and interval policy.
///
/// This is deliberately absent from forecast-path calibration evidence. Its
/// observations become usable only after the separate evaluation window ends.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CalibrationCoverageEvaluation {
    window: CalibrationWindow,
    realized: [RealizedCoverage; 3],
}

impl CalibrationCoverageEvaluation {
    pub(crate) fn try_new(
        window: CalibrationWindow,
        realized: [RealizedCoverage; 3],
    ) -> Result<Self, ForecastError> {
        if realized
            .iter()
            .any(|item| item.total().get() != u64::from(window.observations().get()))
        {
            return Err(ForecastError::InvalidCalibration);
        }
        Ok(Self { window, realized })
    }

    /// Exact evaluation window, ending after all evaluated targets mature.
    #[must_use]
    pub const fn window(&self) -> CalibrationWindow {
        self.window
    }

    /// Ordered observed 50/80/95 coverage counts, never future probabilities.
    #[must_use]
    pub const fn realized(&self) -> &[RealizedCoverage; 3] {
        &self.realized
    }
}

/// One fitted target band expressed as finite offsets from each central point.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CalibrationBand {
    pub(super) coverage: ForecastCoverage,
    pub(super) lower_offset: f64,
    pub(super) upper_offset: f64,
}

impl CalibrationBand {
    /// Constructs a finite band straddling the central forecast.
    pub fn try_new(
        coverage: ForecastCoverage,
        lower_offset: f64,
        upper_offset: f64,
    ) -> Result<Self, ForecastError> {
        if !lower_offset.is_finite()
            || !upper_offset.is_finite()
            || lower_offset > 0.0
            || upper_offset < 0.0
            || lower_offset > upper_offset
        {
            return Err(ForecastError::InvalidCalibration);
        }
        Ok(Self {
            coverage,
            lower_offset,
            upper_offset,
        })
    }

    /// Target marginal coverage.
    #[must_use]
    pub const fn coverage(self) -> ForecastCoverage {
        self.coverage
    }

    /// Finite lower offset from the central value.
    #[must_use]
    pub const fn lower_offset(self) -> f64 {
        self.lower_offset
    }

    /// Finite upper offset from the central value.
    #[must_use]
    pub const fn upper_offset(self) -> f64 {
        self.upper_offset
    }
}

/// Complete bundle-bound evidence required before interval production.
#[derive(Clone, Debug, PartialEq)]
pub struct CalibrationEvidence {
    pub(super) identity: Sha256Digest,
    pub(super) model_id: ModelId,
    pub(super) bundle_id: BundleId,
    pub(super) bundle_version: NonZeroU64,
    pub(super) metadata_hash: Sha256Digest,
    pub(super) training_run_hash: Sha256Digest,
    pub(super) dataset_export_hash: Sha256Digest,
    pub(super) feature_semantic_digests: Box<[FeatureSemanticDigest]>,
    pub(super) method: CalibrationMethod,
    pub(super) window: CalibrationWindow,
    pub(super) policy_hash: Sha256Digest,
    pub(super) policy_size_bytes: u64,
    pub(super) residuals_hash: Sha256Digest,
    pub(super) residuals_size_bytes: u64,
    pub(super) bands: [CalibrationBand; 3],
    pub(super) dependence_assumptions: Box<str>,
}

impl CalibrationEvidence {
    /// Constructs interval evidence bound to one exact admitted model generation.
    #[allow(
        clippy::too_many_arguments,
        reason = "model, calibration artifact, window, coverage, and assumptions stay explicit"
    )]
    pub fn try_new(
        metadata: &ModelMetadata,
        method: CalibrationMethod,
        window: CalibrationWindow,
        policy_hash: Sha256Digest,
        residuals_hash: Sha256Digest,
        bands: [CalibrationBand; 3],
        dependence_assumptions: impl AsRef<str>,
    ) -> Result<Self, ForecastError> {
        let assumptions = dependence_assumptions.as_ref();
        let Some(admitted) = metadata.forecast_calibration() else {
            return Err(ForecastError::InvalidCalibration);
        };
        let admitted_matches = admitted.method() == method
            && admitted.window() == window
            && admitted.policy_hash() == policy_hash
            && admitted.residuals_hash() == residuals_hash
            && admitted.bands() == &bands
            && admitted.dependence_assumptions() == assumptions;
        if policy_hash.bytes() == [0; 32]
            || residuals_hash.bytes() == [0; 32]
            || assumptions.is_empty()
            || assumptions.len() > MAX_CALIBRATION_ASSUMPTION_BYTES
            || assumptions.bytes().any(|byte| byte.is_ascii_control())
            || bands[0].coverage != ForecastCoverage::Fifty
            || bands[1].coverage != ForecastCoverage::Eighty
            || bands[2].coverage != ForecastCoverage::NinetyFive
            || bands[2].lower_offset > bands[1].lower_offset
            || bands[1].lower_offset > bands[0].lower_offset
            || bands[0].upper_offset > bands[1].upper_offset
            || bands[1].upper_offset > bands[2].upper_offset
            || !admitted_matches
        {
            return Err(ForecastError::InvalidCalibration);
        }
        let identity = digest_calibration_evidence(
            metadata,
            method,
            window,
            policy_hash,
            admitted.policy_size_bytes(),
            residuals_hash,
            admitted.residuals_size_bytes(),
            &bands,
            assumptions,
        )?;
        Ok(Self {
            identity,
            model_id: metadata.model_id(),
            bundle_id: metadata.bundle_id().clone(),
            bundle_version: metadata.bundle_version(),
            metadata_hash: metadata.metadata_hash(),
            training_run_hash: metadata.training_run_hash(),
            dataset_export_hash: metadata.dataset().export_digest(),
            feature_semantic_digests: metadata.feature_semantic_digests().into(),
            method,
            window,
            policy_hash,
            policy_size_bytes: admitted.policy_size_bytes(),
            residuals_hash,
            residuals_size_bytes: admitted.residuals_size_bytes(),
            bands,
            dependence_assumptions: assumptions.into(),
        })
    }

    /// Versioned canonical identity of the complete admitted calibration evidence.
    #[must_use]
    pub const fn identity(&self) -> Sha256Digest {
        self.identity
    }

    /// Selected interval method.
    #[must_use]
    pub const fn method(&self) -> CalibrationMethod {
        self.method
    }

    /// Calibration observation window.
    #[must_use]
    pub const fn window(&self) -> CalibrationWindow {
        self.window
    }

    /// Exact canonical interval-policy artifact digest.
    #[must_use]
    pub const fn policy_hash(&self) -> Sha256Digest {
        self.policy_hash
    }

    /// Exact retained interval-policy artifact size.
    #[must_use]
    pub const fn policy_size_bytes(&self) -> u64 {
        self.policy_size_bytes
    }

    /// Exact canonical retained-residual artifact digest.
    #[must_use]
    pub const fn residuals_hash(&self) -> Sha256Digest {
        self.residuals_hash
    }

    /// Exact retained residual artifact size.
    #[must_use]
    pub const fn residuals_size_bytes(&self) -> u64 {
        self.residuals_size_bytes
    }

    /// Ordered 50/80/95 band definitions.
    #[must_use]
    pub const fn bands(&self) -> &[CalibrationBand; 3] {
        &self.bands
    }

    /// Explicit dependence and coverage interpretation.
    #[must_use]
    pub fn dependence_assumptions(&self) -> &str {
        &self.dependence_assumptions
    }

    pub(super) fn matches(&self, metadata: &ModelMetadata, cutoff: Timestamp) -> bool {
        self.matches_coordinate(metadata, &ResearchTemporalCoordinate::exact(cutoff))
    }

    pub(super) fn matches_coordinate(
        &self,
        metadata: &ModelMetadata,
        cutoff: &ResearchTemporalCoordinate,
    ) -> bool {
        self.model_id == metadata.model_id()
            && self.bundle_id == *metadata.bundle_id()
            && self.bundle_version == metadata.bundle_version()
            && self.metadata_hash == metadata.metadata_hash()
            && self.training_run_hash == metadata.training_run_hash()
            && self.dataset_export_hash == metadata.dataset().export_digest()
            && self.feature_semantic_digests.as_ref() == metadata.feature_semantic_digests()
            && metadata.forecast_calibration().is_some_and(|admitted| {
                admitted.method() == self.method
                    && admitted.window() == self.window
                    && admitted.policy_hash() == self.policy_hash
                    && admitted.policy_size_bytes() == self.policy_size_bytes
                    && admitted.residuals_hash() == self.residuals_hash
                    && admitted.residuals_size_bytes() == self.residuals_size_bytes
                    && admitted.bands() == &self.bands
                    && admitted.dependence_assumptions() == self.dependence_assumptions.as_ref()
            })
            && self.window.period.ends_before_coordinate(cutoff)
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the canonical identity binds every independently admitted calibration coordinate"
)]
fn digest_calibration_evidence(
    metadata: &ModelMetadata,
    method: CalibrationMethod,
    window: CalibrationWindow,
    policy_hash: Sha256Digest,
    policy_size_bytes: u64,
    residuals_hash: Sha256Digest,
    residuals_size_bytes: u64,
    bands: &[CalibrationBand; 3],
    assumptions: &str,
) -> Result<Sha256Digest, ForecastError> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/calibration-evidence/v1\0");
    hash.update(metadata.model_id().as_uuid().as_bytes());
    update_bounded(&mut hash, metadata.bundle_id().as_str().as_bytes())?;
    hash.update(metadata.bundle_version().get().to_be_bytes());
    hash.update(metadata.metadata_hash().bytes());
    hash.update(metadata.training_run_hash().bytes());
    hash.update(metadata.dataset().export_digest().bytes());
    hash.update(metadata.dataset().selection_digest().bytes());
    hash.update(
        u64::try_from(metadata.feature_semantic_digests().len())
            .map_err(|_| ForecastError::InvalidCalibration)?
            .to_be_bytes(),
    );
    for digest in metadata.feature_semantic_digests() {
        hash.update(digest.as_bytes());
    }
    hash.update([match method {
        CalibrationMethod::MapieEnbpi => 1,
        CalibrationMethod::MapieAci => 2,
        CalibrationMethod::ResidualQuantile => 3,
    }]);
    for coordinate in [window.start_coordinate(), window.end_coordinate()] {
        let bytes =
            serde_json::to_vec(&coordinate).map_err(|_| ForecastError::InvalidCalibration)?;
        update_bounded(&mut hash, &bytes)?;
    }
    hash.update(window.observations().get().to_be_bytes());
    hash.update(policy_hash.bytes());
    hash.update(policy_size_bytes.to_be_bytes());
    hash.update(residuals_hash.bytes());
    hash.update(residuals_size_bytes.to_be_bytes());
    for band in bands {
        hash.update(band.coverage().basis_points().to_be_bytes());
        hash.update(band.lower_offset().to_bits().to_be_bytes());
        hash.update(band.upper_offset().to_bits().to_be_bytes());
    }
    update_bounded(&mut hash, assumptions.as_bytes())?;
    Ok(Sha256Digest::new(hash.finalize().into()))
}

fn update_bounded(hash: &mut Sha256, value: &[u8]) -> Result<(), ForecastError> {
    hash.update(
        u64::try_from(value.len())
            .map_err(|_| ForecastError::InvalidCalibration)?
            .to_be_bytes(),
    );
    hash.update(value);
    Ok(())
}

/// One empirical residual bin. Its mass comes from validation observations, never interval coverage.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ForecastResidualMass {
    offset: f64,
    probability_ppm: NonZeroU32,
    observations: NonZeroU32,
}

impl ForecastResidualMass {
    /// Applies this admitted empirical residual under the forecast's fixed decimal policy.
    pub fn apply_to(self, central: ForecastValue) -> Result<ForecastValue, ForecastError> {
        central.checked_add_offset(self.offset)
    }
    /// Mean signed residual in this ordered empirical bin.
    #[must_use]
    pub const fn offset(self) -> f64 {
        self.offset
    }

    /// Normalized empirical frequency in parts per million.
    #[must_use]
    pub const fn probability_ppm(self) -> NonZeroU32 {
        self.probability_ppm
    }

    /// Number of distinct held-out examples contributing to this bin.
    #[must_use]
    pub const fn observations(self) -> NonZeroU32 {
        self.observations
    }
}

/// Bounded empirical predictive-error distribution from an admitted frozen direct estimator.
///
/// Validation residuals determine at most 32 equal-count bins and their means. Test residuals
/// remain excluded from fitting both the model and this distribution. Empirical masses assume
/// future errors resemble the retained validation cohort; they are not guaranteed probabilities.
/// Model admission alone can construct this value, after verifying the complete calibration pair.
#[derive(Clone, Debug, PartialEq)]
pub struct ForecastResidualDistribution {
    identity: Sha256Digest,
    residuals_hash: Sha256Digest,
    validation_observations: NonZeroU32,
    masses: Box<[ForecastResidualMass]>,
}

impl ForecastResidualDistribution {
    pub(crate) fn try_from_admitted_residuals(
        residuals: &[u8],
        validation_observations: usize,
        residuals_hash: Sha256Digest,
        training_run_hash: Sha256Digest,
    ) -> Result<Self, ForecastError> {
        let count = NonZeroU32::new(
            u32::try_from(validation_observations)
                .map_err(|_| ForecastError::InvalidCalibration)?,
        )
        .ok_or(ForecastError::InvalidCalibration)?;
        let boundary = validation_observations
            .checked_mul(size_of::<f64>())
            .ok_or(ForecastError::InvalidCalibration)?;
        if validation_observations < 2
            || residuals_hash.bytes() == [0; 32]
            || training_run_hash.bytes() == [0; 32]
            || boundary >= residuals.len()
            || residuals.len() > crate::MAX_FORECAST_RESIDUAL_BYTES
            || !residuals.len().is_multiple_of(size_of::<f64>())
            || <[u8; 32]>::from(Sha256::digest(residuals)) != residuals_hash.bytes()
        {
            return Err(ForecastError::InvalidCalibration);
        }
        let mut ordered = Vec::new();
        ordered
            .try_reserve_exact(validation_observations)
            .map_err(|_| ForecastError::Capacity)?;
        for chunk in residuals[..boundary].chunks_exact(size_of::<f64>()) {
            let value = f64::from_le_bytes(
                chunk
                    .try_into()
                    .map_err(|_| ForecastError::InvalidCalibration)?,
            );
            if !value.is_finite() {
                return Err(ForecastError::InvalidCalibration);
            }
            ordered.push(value);
        }
        ordered.sort_unstable_by(f64::total_cmp);
        let bins = ordered.len().min(32);
        let mut masses: Vec<ForecastResidualMass> = Vec::new();
        masses
            .try_reserve_exact(bins)
            .map_err(|_| ForecastError::Capacity)?;
        let mut previous_mass = 0;
        for bin in 0..bins {
            let start = bin * ordered.len() / bins;
            let end = (bin + 1) * ordered.len() / bins;
            let observations = end - start;
            // Divide before accumulation so a finite mean need not overflow its unscaled sum.
            let offset = ordered[start..end]
                .iter()
                .try_fold(0.0, |total, value| {
                    let next = total + *value / observations as f64;
                    next.is_finite().then_some(next)
                })
                .ok_or(ForecastError::InvalidCalibration)?;
            let cumulative = u32::try_from(
                u64::try_from(end).map_err(|_| ForecastError::InvalidCalibration)? * 1_000_000
                    / u64::from(count.get()),
            )
            .map_err(|_| ForecastError::InvalidCalibration)?;
            let probability_ppm = NonZeroU32::new(cumulative - previous_mass)
                .ok_or(ForecastError::InvalidCalibration)?;
            previous_mass = cumulative;
            let observations = NonZeroU32::new(
                u32::try_from(observations).map_err(|_| ForecastError::InvalidCalibration)?,
            )
            .ok_or(ForecastError::InvalidCalibration)?;
            // Preserve one support point for equal bin means, with the combined actual mass.
            if let Some(previous) = masses.last_mut()
                && previous.offset == offset
            {
                previous.probability_ppm =
                    NonZeroU32::new(previous.probability_ppm.get() + probability_ppm.get())
                        .ok_or(ForecastError::InvalidCalibration)?;
                previous.observations =
                    NonZeroU32::new(previous.observations.get() + observations.get())
                        .ok_or(ForecastError::InvalidCalibration)?;
            } else {
                masses.push(ForecastResidualMass {
                    offset,
                    probability_ppm,
                    observations,
                });
            }
        }
        if previous_mass != 1_000_000
            || masses
                .windows(2)
                .any(|pair| pair[0].offset >= pair[1].offset)
        {
            return Err(ForecastError::InvalidCalibration);
        }
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/validation-residual-distribution/v1\0");
        digest.update(residuals_hash.bytes());
        digest.update(training_run_hash.bytes());
        digest.update(count.get().to_be_bytes());
        for mass in &masses {
            digest.update(mass.offset.to_bits().to_be_bytes());
            digest.update(mass.probability_ppm.get().to_be_bytes());
            digest.update(mass.observations.get().to_be_bytes());
        }
        Ok(Self {
            identity: Sha256Digest::new(digest.finalize().into()),
            residuals_hash,
            validation_observations: count,
            masses: masses.into_boxed_slice(),
        })
    }

    /// Exact versioned identity of the source residuals, training run, and discretization.
    #[must_use]
    pub const fn identity(&self) -> Sha256Digest {
        self.identity
    }

    /// Source calibration residual member, including the separately retained test partition.
    #[must_use]
    pub const fn residuals_hash(&self) -> Sha256Digest {
        self.residuals_hash
    }

    /// Number of validation examples determining empirical frequencies.
    #[must_use]
    pub const fn validation_observations(&self) -> NonZeroU32 {
        self.validation_observations
    }

    /// Ordered empirical support, whose positive masses sum exactly to one million.
    #[must_use]
    pub fn masses(&self) -> &[ForecastResidualMass] {
        &self.masses
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        self.masses.len() * size_of::<ForecastResidualMass>()
    }
}
