//! Immutable content-addressed forecast vintages and realized outcomes.

use super::*;

/// Content-addressed immutable forecast vintage identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ForecastVintageId([u8; 32]);

impl ForecastVintageId {
    /// Exact identity bytes.
    #[must_use]
    pub const fn bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Immutable path publication made before any target can be observed.
#[derive(Clone, Debug, PartialEq)]
pub struct ForecastVintage {
    pub(super) id: ForecastVintageId,
    pub(super) path: ForecastPath,
    pub(super) created_at: Timestamp,
    pub(super) expires_at: Timestamp,
    pub(super) artifact_hash: Sha256Digest,
}

impl ForecastVintage {
    /// Creates one immutable, controlled-artifact-bound vintage.
    pub fn try_new(
        path: ForecastPath,
        created_at: Timestamp,
        expires_at: Timestamp,
        artifact_hash: Sha256Digest,
    ) -> Result<Self, ForecastError> {
        let first_target = path
            .points
            .first()
            .ok_or(ForecastError::InvalidHorizon)?
            .target_at;
        if artifact_hash.bytes() == [0; 32]
            || !path.output_binding.admits_path_horizon(path.horizon)
            || created_at < path.available_at
            || created_at >= first_target
            || expires_at <= created_at
        {
            return Err(ForecastError::InvalidVintage);
        }
        let id = ForecastVintageId(digest_vintage(
            &path,
            created_at,
            expires_at,
            artifact_hash,
        )?);
        Ok(Self {
            id,
            path,
            created_at,
            expires_at,
            artifact_hash,
        })
    }

    /// Content-addressed vintage identity.
    #[must_use]
    pub const fn id(&self) -> ForecastVintageId {
        self.id
    }

    /// Complete immutable path.
    #[must_use]
    pub const fn path(&self) -> &ForecastPath {
        &self.path
    }

    /// Publication time.
    #[must_use]
    pub const fn created_at(&self) -> Timestamp {
        self.created_at
    }

    /// Model-risk expiry time.
    #[must_use]
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    /// Controlled Arrow/Parquet/artifact payload identity.
    #[must_use]
    pub const fn artifact_hash(&self) -> Sha256Digest {
        self.artifact_hash
    }
}

/// Reconstitutes the single current forecast-vintage contract and verifies its content identity.
///
/// Durable adapters use this boundary after strict wire decoding. The verifier reconstructs the
/// private [`ForecastPath`] representation from exact admitted model metadata, recomputes any
/// calibrated intervals, and then delegates identity construction to [`ForecastVintage::try_new`].
/// No adapter-owned serialization or duplicate vintage digest policy is accepted.
#[allow(
    clippy::too_many_arguments,
    clippy::type_complexity,
    reason = "every retained path, model, calibration, publication, and artifact coordinate remains explicit"
)]
pub fn verify_forecast_vintage_identity(
    expected_vintage_id: Sha256Digest,
    metadata: &ModelMetadata,
    instrument_id: InstrumentId,
    observed_cutoff: Timestamp,
    available_at: Timestamp,
    horizon: ForecastHorizon,
    observed_history: &[ForecastObservedPoint],
    retained_points: &[(Timestamp, ForecastValue, Option<[[ForecastValue; 2]; 3]>)],
    calibration: Option<&CalibrationEvidence>,
    created_at: Timestamp,
    expires_at: Timestamp,
    controlled_artifact_hash: Sha256Digest,
) -> Result<(), ForecastError> {
    if available_at < observed_cutoff
        || retained_points.len() != usize::from(horizon.points().get())
        || !metadata.output_binding().admits_path_horizon(horizon)
        || metadata.forecast_calibration().is_some() != calibration.is_some()
        || calibration.is_some_and(|value| !value.matches(metadata, observed_cutoff))
    {
        return Err(ForecastError::InvalidVintage);
    }
    let decimal_scale = retained_points
        .first()
        .ok_or(ForecastError::InvalidHorizon)?
        .1
        .scale();
    if observed_history.len() > MAX_FORECAST_OBSERVED_POINTS
        || observed_history.iter().any(|point| {
            point.value().scale() != decimal_scale
                || point.observed_at() > observed_cutoff
                || point.available_at() > available_at
        })
        || observed_history
            .windows(2)
            .any(|pair| pair[0].observed_at() >= pair[1].observed_at())
        || (!observed_history.is_empty()
            && observed_history
                .last()
                .is_none_or(|point| point.observed_at() != observed_cutoff))
    {
        return Err(ForecastError::InvalidObservedHistory);
    }

    let price_bound = matches!(
        metadata.output_binding().measurement(),
        ForecastMeasurement::Price { .. }
    );
    let mut points = Vec::new();
    points
        .try_reserve_exact(retained_points.len())
        .map_err(|_| ForecastError::Capacity)?;
    for (index, (target_at, central, retained_intervals)) in
        retained_points.iter().copied().enumerate()
    {
        if horizon.target_at(observed_cutoff, index)? != target_at
            || central.scale() != decimal_scale
            || (price_bound && central.mantissa() <= 0)
        {
            return Err(ForecastError::InvalidVintage);
        }
        let intervals = calibration
            .map(|value| ForecastIntervals::from_calibration(central, value))
            .transpose()?;
        if retained_intervals != intervals.map(forecast_interval_bounds)
            || (price_bound
                && intervals.is_some_and(|value| value.interval_95().lower().mantissa() <= 0))
        {
            return Err(ForecastError::InvalidVintage);
        }
        points.push(ForecastPoint {
            target_at,
            central,
            intervals,
        });
    }

    let path = ForecastPath {
        instrument_id,
        observed_cutoff,
        available_at,
        horizon,
        observed_history: observed_history.into(),
        points: points.into_boxed_slice(),
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
        calibration: calibration.cloned(),
        quality: DataQuality::Modeled,
        limitations: metadata.limitations().into(),
        fallback_reason: metadata.fallback_reason().into(),
    };
    let vintage = ForecastVintage::try_new(path, created_at, expires_at, controlled_artifact_hash)?;
    if vintage.id().bytes() != expected_vintage_id.bytes() {
        return Err(ForecastError::InvalidVintage);
    }
    Ok(())
}

fn forecast_interval_bounds(value: ForecastIntervals) -> [[ForecastValue; 2]; 3] {
    [
        [value.interval_50().lower(), value.interval_50().upper()],
        [value.interval_80().lower(), value.interval_80().upper()],
        [value.interval_95().lower(), value.interval_95().upper()],
    ]
}

/// Content-addressed immutable realized-outcome identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ForecastOutcomeId([u8; 32]);

impl ForecastOutcomeId {
    /// Exact identity bytes.
    #[must_use]
    pub const fn bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Later-arriving actual evidence appended against one exact vintage point.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForecastOutcome {
    pub(super) id: ForecastOutcomeId,
    pub(super) vintage_id: ForecastVintageId,
    pub(super) target_at: Timestamp,
    pub(super) observed_at: Timestamp,
    pub(super) available_at: Timestamp,
    pub(super) actual: ForecastValue,
    pub(super) source_pit_hash: Sha256Digest,
    pub(super) quality: DataQuality,
}

impl ForecastOutcome {
    /// Constructs one immutable source/PIT-bound outcome without changing the vintage.
    #[allow(
        clippy::too_many_arguments,
        reason = "vintage, target, observed/available times, actual, source PIT, and quality stay explicit"
    )]
    pub fn try_new(
        vintage: &ForecastVintage,
        target_at: Timestamp,
        observed_at: Timestamp,
        available_at: Timestamp,
        actual: ForecastValue,
        source_pit_hash: Sha256Digest,
        quality: DataQuality,
    ) -> Result<Self, ForecastError> {
        let target = vintage
            .path
            .points
            .iter()
            .find(|point| point.target_at == target_at)
            .ok_or(ForecastError::OutcomeTargetMismatch)?;
        if source_pit_hash.bytes() == [0; 32]
            || observed_at < target_at
            || available_at < observed_at
            || actual.scale() != target.central.scale()
            || quality == DataQuality::Modeled
        {
            return Err(ForecastError::InvalidOutcome);
        }
        let id = ForecastOutcomeId(digest_outcome(
            vintage.id,
            target_at,
            observed_at,
            available_at,
            actual,
            source_pit_hash,
            quality,
        ));
        Ok(Self {
            id,
            vintage_id: vintage.id,
            target_at,
            observed_at,
            available_at,
            actual,
            source_pit_hash,
            quality,
        })
    }

    /// Content-addressed outcome identity.
    #[must_use]
    pub const fn id(&self) -> ForecastOutcomeId {
        self.id
    }

    /// Exact immutable vintage reference.
    #[must_use]
    pub const fn vintage_id(&self) -> ForecastVintageId {
        self.vintage_id
    }

    /// Forecast target coordinate.
    #[must_use]
    pub const fn target_at(&self) -> Timestamp {
        self.target_at
    }

    /// Source observation time.
    #[must_use]
    pub const fn observed_at(&self) -> Timestamp {
        self.observed_at
    }

    /// Point-in-time availability time.
    #[must_use]
    pub const fn available_at(&self) -> Timestamp {
        self.available_at
    }

    /// Exact actual value under the vintage decimal policy.
    #[must_use]
    pub const fn actual(&self) -> ForecastValue {
        self.actual
    }

    /// Exact source/PIT evidence identity.
    #[must_use]
    pub const fn source_pit_hash(&self) -> Sha256Digest {
        self.source_pit_hash
    }

    /// Observed evidence quality, never upgraded to `DirectVerified` by this model domain.
    #[must_use]
    pub const fn quality(&self) -> DataQuality {
        self.quality
    }
}

fn digest_vintage(
    path: &ForecastPath,
    created_at: Timestamp,
    expires_at: Timestamp,
    artifact_hash: Sha256Digest,
) -> Result<[u8; 32], ForecastError> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/forecast-vintage/v4\0");
    hash.update(path.instrument_id.as_uuid().as_bytes());
    hash.update(path.observed_cutoff.unix_nanos().to_be_bytes());
    hash.update(path.available_at.unix_nanos().to_be_bytes());
    hash.update(path.horizon.points.get().to_be_bytes());
    hash.update(path.horizon.step_nanos.get().to_be_bytes());
    hash.update(path.model_id.as_uuid().as_bytes());
    hash.update(path.bundle_id.as_str().as_bytes());
    hash.update(path.bundle_version.get().to_be_bytes());
    hash.update(path.metadata_hash.bytes());
    hash.update(path.artifact_hash.bytes());
    hash.update(path.training_run_hash.bytes());
    hash.update(path.output_binding.identity().bytes());
    hash.update(path.dataset.export_digest().bytes());
    hash.update(path.dataset.selection_digest().bytes());
    hash.update(created_at.unix_nanos().to_be_bytes());
    hash.update(expires_at.unix_nanos().to_be_bytes());
    hash.update(artifact_hash.bytes());
    for observation in &path.observed_history {
        hash.update(observation.observed_at().unix_nanos().to_be_bytes());
        hash.update(observation.available_at().unix_nanos().to_be_bytes());
        hash.update(observation.value().mantissa().to_be_bytes());
        hash.update([observation.value().scale()]);
        hash.update(observation.source_pit_hash().bytes());
        hash.update([quality_tag(observation.quality())]);
    }
    for point in &path.points {
        hash.update(point.target_at.unix_nanos().to_be_bytes());
        hash.update(point.central.mantissa.to_be_bytes());
        hash.update([point.central.scale]);
        if let Some(intervals) = point.intervals {
            hash.update([1]);
            for interval in [
                intervals.interval_50,
                intervals.interval_80,
                intervals.interval_95,
            ] {
                hash.update(interval.lower.mantissa.to_be_bytes());
                hash.update(interval.upper.mantissa.to_be_bytes());
            }
        } else {
            hash.update([0]);
        }
    }
    if let Some(calibration) = &path.calibration {
        hash.update([1]);
        hash.update(calibration.identity().bytes());
        hash.update([match calibration.method() {
            CalibrationMethod::MapieEnbpi => 1,
            CalibrationMethod::MapieAci => 2,
            CalibrationMethod::ResidualQuantile => 3,
        }]);
        hash.update(calibration.window().start().unix_nanos().to_be_bytes());
        hash.update(calibration.window().end().unix_nanos().to_be_bytes());
        hash.update(calibration.window().observations().get().to_be_bytes());
        hash.update(calibration.policy_hash.bytes());
        hash.update(calibration.policy_size_bytes().to_be_bytes());
        hash.update(calibration.residuals_hash.bytes());
        hash.update(calibration.residuals_size_bytes().to_be_bytes());
        for band in calibration.bands() {
            hash.update(band.coverage().basis_points().to_be_bytes());
            hash.update(band.lower_offset().to_bits().to_be_bytes());
            hash.update(band.upper_offset().to_bits().to_be_bytes());
            hash.update(band.realized().covered().to_be_bytes());
            hash.update(band.realized().total().get().to_be_bytes());
        }
        update_vintage_bytes(&mut hash, calibration.dependence_assumptions().as_bytes())?;
    } else {
        hash.update([0]);
    }
    Ok(hash.finalize().into())
}

fn update_vintage_bytes(hash: &mut Sha256, value: &[u8]) -> Result<(), ForecastError> {
    hash.update(
        u64::try_from(value.len())
            .map_err(|_| ForecastError::InvalidVintage)?
            .to_be_bytes(),
    );
    hash.update(value);
    Ok(())
}

fn digest_outcome(
    vintage: ForecastVintageId,
    target_at: Timestamp,
    observed_at: Timestamp,
    available_at: Timestamp,
    actual: ForecastValue,
    source_pit_hash: Sha256Digest,
    quality: DataQuality,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/forecast-outcome/v1\0");
    hash.update(vintage.0);
    hash.update(target_at.unix_nanos().to_be_bytes());
    hash.update(observed_at.unix_nanos().to_be_bytes());
    hash.update(available_at.unix_nanos().to_be_bytes());
    hash.update(actual.mantissa.to_be_bytes());
    hash.update([actual.scale]);
    hash.update(source_pit_hash.bytes());
    hash.update([quality_tag(quality)]);
    hash.finalize().into()
}

const fn quality_tag(quality: DataQuality) -> u8 {
    match quality {
        DataQuality::DirectVerified => 1,
        DataQuality::DirectUnverified => 2,
        DataQuality::OfficialDelayed => 3,
        DataQuality::Aggregated => 4,
        DataQuality::Indicative => 5,
        DataQuality::Modeled => 6,
        DataQuality::Estimated => 7,
        DataQuality::Stale => 8,
        DataQuality::Quarantined => 9,
    }
}
