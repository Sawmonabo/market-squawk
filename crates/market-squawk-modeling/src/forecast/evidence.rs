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
            || created_at < path.available_at
            || created_at >= first_target
            || expires_at <= created_at
        {
            return Err(ForecastError::InvalidVintage);
        }
        let id = ForecastVintageId(digest_vintage(&path, created_at, expires_at, artifact_hash));
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
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/forecast-vintage/v1\0");
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
    hash.update(path.dataset.export_digest().bytes());
    hash.update(path.dataset.selection_digest().bytes());
    hash.update(created_at.unix_nanos().to_be_bytes());
    hash.update(expires_at.unix_nanos().to_be_bytes());
    hash.update(artifact_hash.bytes());
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
        hash.update(calibration.policy_hash.bytes());
        hash.update(calibration.residuals_hash.bytes());
    } else {
        hash.update([0]);
    }
    hash.finalize().into()
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
