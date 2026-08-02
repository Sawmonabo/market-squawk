//! Bundle-bound calibration and interval-policy evidence.

use super::contracts::MAX_CALIBRATION_ASSUMPTION_BYTES;
use super::*;

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
    pub(super) start: Timestamp,
    pub(super) end: Timestamp,
    pub(super) observations: NonZeroU32,
}

impl CalibrationWindow {
    /// Constructs a nonempty calibration window.
    pub fn try_new(
        start: Timestamp,
        end: Timestamp,
        observations: NonZeroU32,
    ) -> Result<Self, ForecastError> {
        if end <= start {
            Err(ForecastError::InvalidCalibration)
        } else {
            Ok(Self {
                start,
                end,
                observations,
            })
        }
    }

    /// Inclusive calibration start.
    #[must_use]
    pub const fn start(self) -> Timestamp {
        self.start
    }

    /// Exclusive calibration end.
    #[must_use]
    pub const fn end(self) -> Timestamp {
        self.end
    }

    /// Admitted calibration observations.
    #[must_use]
    pub const fn observations(self) -> NonZeroU32 {
        self.observations
    }
}

/// One target band expressed as finite offsets from each central point.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CalibrationBand {
    pub(super) coverage: ForecastCoverage,
    pub(super) lower_offset: f64,
    pub(super) upper_offset: f64,
    pub(super) realized: RealizedCoverage,
}

impl CalibrationBand {
    /// Constructs a finite band straddling the central forecast.
    pub fn try_new(
        coverage: ForecastCoverage,
        lower_offset: f64,
        upper_offset: f64,
        realized: RealizedCoverage,
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
            realized,
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

    /// Realized validation coverage observation.
    #[must_use]
    pub const fn realized(self) -> RealizedCoverage {
        self.realized
    }
}

/// Complete bundle-bound evidence required before interval production.
#[derive(Clone, Debug, PartialEq)]
pub struct CalibrationEvidence {
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
    pub(super) residuals_hash: Sha256Digest,
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
        let admitted_matches = metadata.forecast_calibration().is_none_or(|admitted| {
            admitted.method() == method
                && admitted.window() == window
                && admitted.policy_hash() == policy_hash
                && admitted.residuals_hash() == residuals_hash
                && admitted.bands() == &bands
                && admitted.dependence_assumptions() == assumptions
        });
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
        Ok(Self {
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
            residuals_hash,
            bands,
            dependence_assumptions: assumptions.into(),
        })
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

    /// Exact canonical retained-residual artifact digest.
    #[must_use]
    pub const fn residuals_hash(&self) -> Sha256Digest {
        self.residuals_hash
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
        self.model_id == metadata.model_id()
            && self.bundle_id == *metadata.bundle_id()
            && self.bundle_version == metadata.bundle_version()
            && self.metadata_hash == metadata.metadata_hash()
            && self.training_run_hash == metadata.training_run_hash()
            && self.dataset_export_hash == metadata.dataset().export_digest()
            && self.feature_semantic_digests.as_ref() == metadata.feature_semantic_digests()
            && self.window.end() <= cutoff
    }
}
