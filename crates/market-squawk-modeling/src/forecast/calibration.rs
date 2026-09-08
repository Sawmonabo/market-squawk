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
            && self.window.end() <= cutoff
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
    hash.update(window.start().unix_nanos().to_be_bytes());
    hash.update(window.end().unix_nanos().to_be_bytes());
    hash.update(window.observations().get().to_be_bytes());
    hash.update(policy_hash.bytes());
    hash.update(policy_size_bytes.to_be_bytes());
    hash.update(residuals_hash.bytes());
    hash.update(residuals_size_bytes.to_be_bytes());
    for band in bands {
        hash.update(band.coverage().basis_points().to_be_bytes());
        hash.update(band.lower_offset().to_bits().to_be_bytes());
        hash.update(band.upper_offset().to_bits().to_be_bytes());
        hash.update(band.realized().covered().to_be_bytes());
        hash.update(band.realized().total().get().to_be_bytes());
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
