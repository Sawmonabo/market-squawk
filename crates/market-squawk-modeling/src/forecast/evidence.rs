//! Immutable content-addressed forecast vintages and realized outcomes.

use super::contracts::ForecastFinancialTarget;
use super::*;
use market_squawk_data::FinancialFiscalTargetBinding;
use market_squawk_domain::ResearchTemporalCoordinate;

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
        let first_target = path.points.first().ok_or(ForecastError::InvalidHorizon)?;
        let target_is_future = match (first_target.target_at, first_target.financial_target) {
            (Some(target), None) => created_at < target,
            (None, Some(target)) => target.period().is_none_or(|period| {
                created_at
                    .utc_calendar_date()
                    .is_ok_and(|date| date < period.end())
            }),
            _ => false,
        };
        if artifact_hash.bytes() == [0; 32]
            || !path.output_binding.admits_path_horizon(path.horizon)
            || created_at < path.available_at
            || !target_is_future
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
    observed_cutoff: Option<Timestamp>,
    financial_target: Option<&FinancialFiscalTargetBinding>,
    calibration_cutoff: ResearchTemporalCoordinate,
    available_at: Timestamp,
    horizon: ForecastHorizon,
    observed_history: &[ForecastObservedPoint],
    retained_points: &[(
        Option<Timestamp>,
        Option<ForecastFinancialTarget>,
        ForecastValue,
        Option<[[ForecastValue; 2]; 3]>,
    )],
    calibration: Option<&CalibrationEvidence>,
    created_at: Timestamp,
    expires_at: Timestamp,
    controlled_artifact_hash: Sha256Digest,
) -> Result<ForecastVintage, ForecastError> {
    let observed_coordinate = match (observed_cutoff, financial_target) {
        (Some(timestamp), None) => ResearchTemporalCoordinate::exact(timestamp),
        (None, Some(financial)) => {
            ResearchTemporalCoordinate::calendar_date(financial.observed_period().end())
        }
        _ => return Err(ForecastError::InvalidVintage),
    };
    if observed_cutoff.is_some_and(|cutoff| available_at < cutoff)
        || retained_points.len() != usize::from(horizon.points().get())
        || !metadata.output_binding().admits_path_horizon(horizon)
        || metadata.forecast_calibration().is_some() != calibration.is_some()
        || calibration.is_some_and(|value| !value.matches_coordinate(metadata, &calibration_cutoff))
        || (financial_target.is_none() && calibration_cutoff != observed_coordinate)
        || (financial_target.is_some()
            != matches!(
                metadata.output_binding().measurement(),
                ForecastMeasurement::FinancialAmount { .. }
            ))
    {
        return Err(ForecastError::InvalidVintage);
    }
    let probability = metadata.output_semantics() == ModelOutputSemantics::BinaryProbability;
    if probability
        && (calibration.is_some()
            || !observed_history.is_empty()
            || metadata.probability_calibration().is_none_or(|proof| {
                calibration_cutoff
                    .exact_timestamp()
                    .is_none_or(|cutoff| !proof.evaluation_window().ends_by(cutoff))
            }))
    {
        return Err(ForecastError::InvalidCalibration);
    }
    let decimal_scale = retained_points
        .first()
        .ok_or(ForecastError::InvalidHorizon)?
        .2
        .scale();
    if observed_history.len() > MAX_FORECAST_OBSERVED_POINTS
        || observed_history.iter().any(|point| {
            point.value().scale() != decimal_scale
                || observed_cutoff.is_none_or(|cutoff| point.observed_at() > cutoff)
                || point.available_at() > available_at
        })
        || observed_history
            .windows(2)
            .any(|pair| pair[0].observed_at() >= pair[1].observed_at())
        || (!observed_history.is_empty()
            && observed_history
                .last()
                .is_none_or(|point| Some(point.observed_at()) != observed_cutoff))
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
    for (index, (target_at, fiscal, central, retained_intervals)) in
        retained_points.iter().copied().enumerate()
    {
        let expected_timestamp = observed_cutoff
            .map(|cutoff| horizon.target_at(cutoff, index))
            .transpose()?;
        let expected_fiscal = financial_target.map(|binding| ForecastFinancialTarget {
            ordinal: binding.target_ordinal(),
            period: binding.target_period(),
        });
        if expected_timestamp != target_at
            || expected_fiscal != fiscal
            || central.scale() != decimal_scale
            || (price_bound && central.mantissa() <= 0)
            || (probability
                && (central.mantissa() < 0
                    || central.mantissa() > 10_i128.pow(u32::from(central.scale()))))
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
            financial_target: fiscal,
            central,
            intervals,
        });
    }

    let path = ForecastPath {
        instrument_id,
        observed_cutoff,
        financial_target: financial_target.cloned().map(Box::new),
        calibration_cutoff,
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
        probability_calibration: metadata.probability_calibration().cloned(),
        quality: DataQuality::Modeled,
        limitations: metadata.limitations().into(),
        fallback_reason: metadata.fallback_reason().into(),
    };
    let vintage = ForecastVintage::try_new(path, created_at, expires_at, controlled_artifact_hash)?;
    if vintage.id().bytes() != expected_vintage_id.bytes() {
        return Err(ForecastError::InvalidVintage);
    }
    Ok(vintage)
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
            .find(|point| point.target_at == Some(target_at))
            .ok_or(ForecastError::OutcomeTargetMismatch)?;
        if source_pit_hash.bytes() == [0; 32]
            || observed_at != target_at
            || available_at < observed_at
            || actual.scale() != target.central.scale()
            || quality == DataQuality::Modeled
            || (vintage.path.output_binding.output_semantics()
                == ModelOutputSemantics::BinaryProbability
                && !matches!(actual.mantissa(), 0)
                && actual.mantissa() != 10_i128.pow(u32::from(actual.scale())))
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

    /// Reopens a genuine binary label under its original producer receipt. Only this sealed
    /// route admits modeled after-cost outcomes; callers cannot provide a replacement value.
    pub fn try_from_probability_observation(
        vintage: &ForecastVintage,
        observation: &market_squawk_data::ForecastProbabilityOutcome,
        source_pit_hash: Sha256Digest,
    ) -> Result<Self, ForecastError> {
        let ForecastTargetMeaning::FixedHorizonEvent {
            horizon_nanos,
            origin_basis,
            event,
        } = vintage.path.output_binding.target()
        else {
            return Err(ForecastError::OutcomeTargetMismatch);
        };
        let expected_quality = match event {
            market_squawk_data::ProbabilityEventTarget::ProfitAfterCosts { .. } => {
                DataQuality::Modeled
            }
            _ => DataQuality::Aggregated,
        };
        if vintage.path.output_binding.output_semantics() != ModelOutputSemantics::BinaryProbability
            || event != observation.event()
            || origin_basis != observation.origin_basis()
            || vintage.path.instrument_id != observation.instrument_id()
            || vintage.path.observed_cutoff != Some(observation.origin())
            || observation
                .origin()
                .checked_add_nanos(
                    i64::try_from(horizon_nanos.get())
                        .map_err(|_| ForecastError::InvalidOutcome)?,
                )
                .ok()
                != Some(observation.target_at())
            || observation.quality() != expected_quality
            || observation.available_at() < observation.label_maturity()
            || observation.available_at() > observation.fence().as_of()
            || observation.production_receipt_sha256().bytes() == [0; 32]
            || observation.lineage_sha256().bytes() == [0; 32]
        {
            return Err(ForecastError::InvalidOutcome);
        }
        let target = vintage
            .path
            .points
            .iter()
            .find(|point| point.target_at == Some(observation.target_at()))
            .ok_or(ForecastError::OutcomeTargetMismatch)?;
        let actual = ForecastValue::try_new(
            if observation.value() {
                10_i128.pow(u32::from(target.central.scale()))
            } else {
                0
            },
            target.central.scale(),
        )?;
        let mut outcome = Self::try_new(
            vintage,
            observation.target_at(),
            observation.target_at(),
            observation.available_at(),
            actual,
            source_pit_hash,
            DataQuality::Aggregated,
        )?;
        outcome.quality = expected_quality;
        outcome.id = ForecastOutcomeId(digest_outcome(
            vintage.id,
            outcome.target_at,
            outcome.observed_at,
            outcome.available_at,
            outcome.actual,
            source_pit_hash,
            expected_quality,
        ));
        Ok(outcome)
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
    let origin = path
        .observed_coordinate()
        .ok_or(ForecastError::InvalidVintage)?;
    update_vintage_bytes(
        &mut hash,
        &serde_json::to_vec(&origin).map_err(|_| ForecastError::InvalidVintage)?,
    )?;
    update_vintage_bytes(
        &mut hash,
        &serde_json::to_vec(path.calibration_cutoff())
            .map_err(|_| ForecastError::InvalidVintage)?,
    )?;
    if let Some(financial) = path.financial_target() {
        update_vintage_bytes(
            &mut hash,
            &serde_json::to_vec(financial).map_err(|_| ForecastError::InvalidVintage)?,
        )?;
    }
    hash.update(path.available_at.unix_nanos().to_be_bytes());
    hash.update(path.horizon.points.get().to_be_bytes());
    match (path.horizon.step_nanos(), path.horizon.fiscal_periods()) {
        (Some(step), None) => {
            hash.update([1]);
            hash.update(step.get().to_be_bytes());
        }
        (None, Some((cadence, distance))) => {
            hash.update([2]);
            update_vintage_bytes(
                &mut hash,
                &serde_json::to_vec(&cadence).map_err(|_| ForecastError::InvalidVintage)?,
            )?;
            hash.update(distance.get().to_be_bytes());
        }
        _ => return Err(ForecastError::InvalidVintage),
    }
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
        match (point.target_at, point.financial_target) {
            (Some(target), None) => {
                hash.update([1]);
                hash.update(target.unix_nanos().to_be_bytes());
            }
            (None, Some(target)) => {
                hash.update([2]);
                hash.update(target.ordinal().to_be_bytes());
                update_vintage_bytes(
                    &mut hash,
                    &serde_json::to_vec(&target.period())
                        .map_err(|_| ForecastError::InvalidVintage)?,
                )?;
            }
            _ => return Err(ForecastError::InvalidVintage),
        }
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
        for coordinate in [
            calibration.window().start_coordinate(),
            calibration.window().end_coordinate(),
        ] {
            update_vintage_bytes(
                &mut hash,
                &serde_json::to_vec(&coordinate).map_err(|_| ForecastError::InvalidVintage)?,
            )?;
        }
        hash.update(calibration.window().observations().get().to_be_bytes());
        hash.update(calibration.policy_hash.bytes());
        hash.update(calibration.policy_size_bytes().to_be_bytes());
        hash.update(calibration.residuals_hash.bytes());
        hash.update(calibration.residuals_size_bytes().to_be_bytes());
        for band in calibration.bands() {
            hash.update(band.coverage().basis_points().to_be_bytes());
            hash.update(band.lower_offset().to_bits().to_be_bytes());
            hash.update(band.upper_offset().to_bits().to_be_bytes());
        }
        update_vintage_bytes(&mut hash, calibration.dependence_assumptions().as_bytes())?;
    } else {
        hash.update([0]);
    }
    if let Some(probability) = path.probability_calibration() {
        hash.update(b"binary-probability-calibration/v1\0");
        hash.update(probability.policy_hash().bytes());
        hash.update(probability.outcomes_hash().bytes());
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
