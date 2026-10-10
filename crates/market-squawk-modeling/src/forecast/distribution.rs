//! Terminal empirical distributions bound to admitted model and controlled forecast artifacts.

use market_squawk_data::DatasetManifestRef;
use market_squawk_domain::ResearchTemporalCoordinate;
use serde_json::json;

use super::*;
use crate::ModelBundle;

const MAX_CANONICAL_DISTRIBUTION_BYTES: usize = 64 * 1024;

/// A qualified hindcast distribution over an actual data-owned input coordinate.
///
/// It carries no live vintage or claimed historical publication. The model and
/// interval fit are frozen before the economic scoring origin; support consists
/// solely of admitted calibration-fit residuals.
#[derive(Clone, Debug, PartialEq)]
pub struct ForecastStudyDistribution {
    identity: Sha256Digest,
    path: ForecastPath,
    epoch: market_squawk_data::FeatureDatasetInputEpoch,
    input_manifest: DatasetManifestRef,
    input_production_identity: Sha256Digest,
    input_receipt_sha256: Sha256Digest,
    input_split_policy: market_squawk_data::ChronologicalSplitPolicy,
    residual_distribution: ForecastResidualDistribution,
    points: Box<[ForecastDistributionPoint]>,
}

impl ForecastStudyDistribution {
    /// Binds actual frozen-model inference to the original sealed feature coordinate.
    pub fn try_from_admitted_path(
        bundle: &ModelBundle,
        path: ForecastPath,
        coordinate: market_squawk_data::FeatureDatasetInputCoordinate<'_>,
    ) -> Result<Self, ForecastError> {
        let residuals = bundle
            .forecast_residual_distribution()
            .ok_or(ForecastError::InvalidCalibration)?;
        validate_distribution_path(bundle, &path, residuals)?;
        let epoch = coordinate.epoch();
        let dataset = coordinate.dataset();
        let training = path.dataset();
        let study = training
            .study_policy()
            .ok_or(ForecastError::InvalidRequest)?;
        let inputs = dataset
            .study_policy()
            .ok_or(ForecastError::InvalidRequest)?;
        let [terminal] = path.points() else {
            return Err(ForecastError::InvalidHorizon);
        };
        let [_, calibration_end, evaluation_end] = split_coordinates(training.split_policy())?;
        let decision = epoch.decision_coordinate();
        let origin = path
            .observed_coordinate()
            .ok_or(ForecastError::InvalidRequest)?;
        let calibration_cutoff = if epoch.financial_period().is_some() {
            decision.clone()
        } else {
            origin.clone()
        };
        if epoch.purpose() != market_squawk_data::DatasetBuildPurpose::StudyInputs
            || study.purpose() != market_squawk_data::DatasetBuildPurpose::Training
            || study.basis() != epoch.basis()
            || inputs.basis() != epoch.basis()
            || study.snapshot_as_of() != epoch.snapshot_as_of()
            || inputs.snapshot_as_of() != epoch.snapshot_as_of()
            || study.target_horizon() != inputs.target_horizon()
            || study.decision_lag() != inputs.decision_lag()
            || !decision
                .partial_cmp(&calibration_end)
                .is_some_and(|order| order.is_gt())
            || !decision
                .partial_cmp(&evaluation_end)
                .is_some_and(|order| !order.is_gt())
            || training.universe_digest() != dataset.universe_digest()
            || path.universe_id() != dataset.universe_id()
            || training.source_snapshot_digest() != Some(epoch.source_snapshot_digest())
            || dataset.source_snapshot_digest() != Some(epoch.source_snapshot_digest())
            || path.instrument_id() != epoch.instrument_id()
            || path.observed_cutoff() != epoch.target_origin()
            || path.available_at() != epoch.source_selection_as_of()
            || terminal.target_at() != epoch.target_at()
            || path.financial_target() != epoch.financial_period()
            || path.calibration_cutoff() != &calibration_cutoff
            || !path
                .training_period()
                .ends_before_coordinate(&calibration_cutoff)
        {
            return Err(ForecastError::InvalidRequest);
        }
        let points = terminal_support(terminal.central(), residuals)?.into_boxed_slice();
        let receipt = dataset.production_receipt();
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/forecast-study-distribution/v1\0");
        for digest in [
            path.metadata_hash(),
            path.artifact_hash(),
            path.training_run_hash(),
            path.output_binding().identity(),
            training.selection_digest(),
            residuals.identity(),
            dataset.generation().manifest().content_hash(),
            receipt.production_identity(),
            receipt.receipt_sha256(),
            epoch.source_snapshot_digest(),
        ] {
            hash.update(digest.bytes());
        }
        hash.update(
            u64::try_from(epoch.example_id().len())
                .map_err(|_| ForecastError::Capacity)?
                .to_be_bytes(),
        );
        hash.update(epoch.example_id().as_bytes());
        hash.update(epoch.instrument_id().as_uuid().as_bytes());
        for time in [epoch.source_selection_as_of(), epoch.calculated_at()] {
            hash.update(time.unix_nanos().to_be_bytes());
        }
        for coordinate in [decision.clone(), origin, calibration_cutoff] {
            hash_coordinate(&mut hash, &coordinate)?;
        }
        if let Some(financial) = epoch.financial_period() {
            let bytes = serde_json::to_vec(financial).map_err(|_| ForecastError::InvalidRequest)?;
            hash.update((bytes.len() as u64).to_be_bytes());
            hash.update(bytes);
        } else {
            hash_coordinate(
                &mut hash,
                &ResearchTemporalCoordinate::exact(
                    epoch.target_at().ok_or(ForecastError::InvalidRequest)?,
                ),
            )?;
        }
        for boundary in split_coordinates(training.split_policy())?
            .into_iter()
            .chain(split_coordinates(dataset.split_policy())?)
        {
            hash_coordinate(&mut hash, &boundary)?;
        }
        hash.update(terminal.central().mantissa().to_be_bytes());
        hash.update([terminal.central().scale()]);
        hash.update(
            u64::try_from(coordinate.rows().len())
                .map_err(|_| ForecastError::Capacity)?
                .to_be_bytes(),
        );
        for row in coordinate.rows() {
            hash.update(row.lineage_sha256().bytes());
        }
        for point in &points {
            hash.update(point.value().mantissa().to_be_bytes());
            hash.update([point.value().scale()]);
            hash.update(point.probability_ppm().get().to_be_bytes());
        }
        Ok(Self {
            identity: Sha256Digest::new(hash.finalize().into()),
            path,
            epoch: epoch.clone(),
            input_manifest: dataset.generation().manifest().clone(),
            input_production_identity: receipt.production_identity(),
            input_receipt_sha256: receipt.receipt_sha256(),
            input_split_policy: dataset.split_policy(),
            residual_distribution: residuals.clone(),
            points,
        })
    }

    /// Exact bound model, input-coordinate, and normalized support identity.
    #[must_use]
    pub const fn identity(&self) -> Sha256Digest {
        self.identity
    }
    /// Original model output and fit-only calibration; contains no test evaluation.
    #[must_use]
    pub const fn path(&self) -> &ForecastPath {
        &self.path
    }
    /// Actual source evidence and separately retained study/economic clocks.
    #[must_use]
    pub const fn epoch(&self) -> &market_squawk_data::FeatureDatasetInputEpoch {
        &self.epoch
    }
    /// Genuine feature-only producer output, rather than an invented forecast manifest.
    #[must_use]
    pub const fn input_manifest(&self) -> &DatasetManifestRef {
        &self.input_manifest
    }
    /// Actual sealed input recipe identity.
    #[must_use]
    pub const fn input_production_identity(&self) -> Sha256Digest {
        self.input_production_identity
    }
    /// Actual input publication and rights receipt identity.
    #[must_use]
    pub const fn input_receipt_sha256(&self) -> Sha256Digest {
        self.input_receipt_sha256
    }
    /// Full-population input partitions, separate from the selected model's training partitions.
    #[must_use]
    pub const fn input_split_policy(&self) -> market_squawk_data::ChronologicalSplitPolicy {
        self.input_split_policy
    }
    /// Fitted residual population only; no held-out test residual is exposed.
    #[must_use]
    pub const fn residual_distribution(&self) -> &ForecastResidualDistribution {
        &self.residual_distribution
    }
    /// Ordered native-unit outcomes with explicit masses summing to one million.
    #[must_use]
    pub fn points(&self) -> &[ForecastDistributionPoint] {
        &self.points
    }
}

/// One outcome in the admitted model's native unit, with explicit empirical probability mass.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ForecastDistributionPoint {
    value: ForecastValue,
    probability_ppm: NonZeroU32,
}

impl ForecastDistributionPoint {
    /// Outcome in the exact measurement carried by the terminal output binding.
    #[must_use]
    pub const fn value(self) -> ForecastValue {
        self.value
    }

    /// Positive empirical frequency; the complete support sums to one million.
    #[must_use]
    pub const fn probability_ppm(self) -> NonZeroU32 {
        self.probability_ppm
    }
}

/// A direct estimator's terminal prediction plus its frozen validation-error distribution.
///
/// Only an admitted bundle and its exact immutable vintage can produce this value. A return
/// remains a return; monetary conversion and authorization of source parents are downstream
/// responsibilities. The raw center and residual masses remain available for arithmetic audit.
#[derive(Clone, Debug, PartialEq)]
pub struct ForecastTerminalDistribution {
    identity: Sha256Digest,
    residual_distribution: ForecastResidualDistribution,
    vintage_id: ForecastVintageId,
    forecast_artifact_hash: Sha256Digest,
    metadata_hash: Sha256Digest,
    model_artifact_hash: Sha256Digest,
    training_run_hash: Sha256Digest,
    training_dataset: TrainingDatasetIdentity,
    output_binding: ForecastOutputBinding,
    instrument_id: InstrumentId,
    observed_through: Option<Timestamp>,
    financial_target: Option<Box<market_squawk_data::FinancialFiscalTargetBinding>>,
    calibration_cutoff: ResearchTemporalCoordinate,
    available_at: Timestamp,
    published_at: Timestamp,
    expires_at: Timestamp,
    target_at: Option<Timestamp>,
    central: ForecastValue,
    points: Box<[ForecastDistributionPoint]>,
}

impl ForecastTerminalDistribution {
    pub(crate) fn try_from_bundle(
        bundle: &ModelBundle,
        vintage: &ForecastVintage,
    ) -> Result<Option<Self>, ForecastError> {
        let Some(residual_distribution) = bundle.forecast_residual_distribution() else {
            return Ok(None);
        };
        let metadata = bundle.metadata();
        let path = vintage.path();
        validate_distribution_path(bundle, path, residual_distribution)?;
        let [terminal] = path.points() else {
            return Err(ForecastError::InvalidHorizon);
        };
        let points = terminal_support(terminal.central(), residual_distribution)?;
        let mut result = Self {
            identity: Sha256Digest::new([0; 32]),
            residual_distribution: residual_distribution.clone(),
            vintage_id: vintage.id(),
            forecast_artifact_hash: vintage.artifact_hash(),
            metadata_hash: metadata.metadata_hash(),
            model_artifact_hash: metadata.artifact_hash(),
            training_run_hash: metadata.training_run_hash(),
            training_dataset: metadata.dataset().clone(),
            output_binding: metadata.output_binding().clone(),
            instrument_id: path.instrument_id(),
            observed_through: path.observed_cutoff(),
            financial_target: path.financial_target().cloned().map(Box::new),
            calibration_cutoff: path.calibration_cutoff().clone(),
            available_at: path.available_at(),
            published_at: vintage.created_at(),
            expires_at: vintage.expires_at(),
            target_at: terminal.target_at(),
            central: terminal.central(),
            points: points.into_boxed_slice(),
        };
        result.identity = Sha256Digest::new(Sha256::digest(result.canonical_bytes()?).into());
        Ok(Some(result))
    }

    /// Reopens exact canonical bytes only against their authentic admitted bundle and vintage.
    ///
    /// Bytes alone cannot mint forecast authority. Recomputed support, inputs, and all provenance
    /// coordinates must reproduce the complete code-owned canonical payload.
    pub fn from_canonical_bytes(
        bytes: &[u8],
        bundle: &ModelBundle,
        vintage: &ForecastVintage,
    ) -> Result<Self, ForecastError> {
        if bytes.is_empty() || bytes.len() > MAX_CANONICAL_DISTRIBUTION_BYTES {
            return Err(ForecastError::InvalidVintage);
        }
        let result =
            Self::try_from_bundle(bundle, vintage)?.ok_or(ForecastError::InvalidVintage)?;
        if result.canonical_bytes()? != bytes {
            return Err(ForecastError::InvalidVintage);
        }
        Ok(result)
    }

    /// Exact bounded arithmetic/provenance payload; its SHA-256 is [`Self::identity`].
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, ForecastError> {
        let manifest = self.training_manifest();
        let residuals: Vec<_> = self
            .residual_distribution
            .masses()
            .iter()
            .map(|mass| {
                json!({
                    "offset_bits": mass.offset().to_bits(),
                    "probability_ppm": mass.probability_ppm().get(),
                    "observations": mass.observations().get(),
                })
            })
            .collect();
        let points: Vec<_> = self
            .points
            .iter()
            .map(|point| {
                json!({
                    "mantissa": point.value.mantissa().to_string(),
                    "scale": point.value.scale(),
                    "probability_ppm": point.probability_ppm.get(),
                })
            })
            .collect();
        let bytes = serde_json::to_vec(&json!({
            "contract": "market-squawk.forecast-terminal-distribution",
            "version": 1,
            "vintage_id": self.vintage_id.bytes(),
            "forecast_artifact_hash": self.forecast_artifact_hash.bytes(),
            "metadata_hash": self.metadata_hash.bytes(),
            "model_artifact_hash": self.model_artifact_hash.bytes(),
            "training_run_hash": self.training_run_hash.bytes(),
            "training_manifest": {
                "dataset": manifest.dataset_id().as_str(),
                "manifest_version": manifest.manifest_version(),
                "content_hash": manifest.content_hash().bytes(),
                "schema_name": manifest.schema().name(),
                "schema_version": manifest.schema_version().get(),
                "schema_fingerprint": manifest.schema().fingerprint(),
            },
            "training_selection_digest": self.training_dataset.selection_digest().bytes(),
            "training_export_digest": self.training_dataset.export_digest().bytes(),
            "training_selection_as_of": self.training_dataset.selection_as_of().unix_nanos(),
            "output_binding_identity": self.output_binding.identity().bytes(),
            "instrument_id": self.instrument_id.to_string(),
            "observed_through": self.observed_through.map(|time| time.unix_nanos()),
            "financial_target": self.financial_target,
            "calibration_cutoff": self.calibration_cutoff,
            "available_at": self.available_at.unix_nanos(),
            "published_at": self.published_at.unix_nanos(),
            "expires_at": self.expires_at.unix_nanos(),
            "target_at": self.target_at.map(|time| time.unix_nanos()),
            "central": {"mantissa": self.central.mantissa().to_string(), "scale": self.central.scale()},
            "residual_distribution_identity": self.residual_distribution.identity().bytes(),
            "residuals_hash": self.residual_distribution.residuals_hash().bytes(),
            "validation_observations": self.residual_distribution.validation_observations().get(),
            "residuals": residuals,
            "points": points,
        })).map_err(|_| ForecastError::InvalidVintage)?;
        if bytes.len() > MAX_CANONICAL_DISTRIBUTION_BYTES {
            return Err(ForecastError::Capacity);
        }
        Ok(bytes)
    }

    /// Identity of the complete canonical arithmetic and provenance payload.
    #[must_use]
    pub const fn identity(&self) -> Sha256Digest {
        self.identity
    }
    /// Exact frozen validation-error distribution identity.
    #[must_use]
    pub const fn residual_distribution_identity(&self) -> Sha256Digest {
        self.residual_distribution.identity()
    }
    /// Validation-only raw residual bins, before decimal rounding and equal-point merging.
    #[must_use]
    pub const fn residual_distribution(&self) -> &ForecastResidualDistribution {
        &self.residual_distribution
    }
    /// Immutable source forecast vintage.
    #[must_use]
    pub const fn vintage_id(&self) -> ForecastVintageId {
        self.vintage_id
    }
    /// Genuine controlled forecast artifact, distinct from the model artifact.
    #[must_use]
    pub const fn forecast_artifact_hash(&self) -> Sha256Digest {
        self.forecast_artifact_hash
    }
    /// Exact admitted model metadata artifact.
    #[must_use]
    pub const fn metadata_hash(&self) -> Sha256Digest {
        self.metadata_hash
    }
    /// Exact admitted model artifact.
    #[must_use]
    pub const fn model_artifact_hash(&self) -> Sha256Digest {
        self.model_artifact_hash
    }
    /// Exact admitted training/calibration run artifact.
    #[must_use]
    pub const fn training_run_hash(&self) -> Sha256Digest {
        self.training_run_hash
    }
    /// Genuine training source generation; this is not an invented forecast-output manifest.
    #[must_use]
    pub const fn training_manifest(&self) -> &DatasetManifestRef {
        self.training_dataset.manifest()
    }
    /// Exact training selection, export, and catalog authority.
    #[must_use]
    pub const fn training_dataset(&self) -> &TrainingDatasetIdentity {
        &self.training_dataset
    }
    /// Sealed native-unit measurement, target, and central-statistic contract.
    #[must_use]
    pub const fn output_binding(&self) -> &ForecastOutputBinding {
        &self.output_binding
    }
    /// Canonical target instrument.
    #[must_use]
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
    /// Original source effective cutoff.
    #[must_use]
    pub const fn observed_through(&self) -> Option<Timestamp> {
        self.observed_through
    }
    /// Source-sealed native period chain and target ordinal for a financial output.
    pub fn financial_target(&self) -> Option<&market_squawk_data::FinancialFiscalTargetBinding> {
        self.financial_target.as_deref()
    }
    /// Economic issuance coordinate at which model and fit evidence were admitted.
    pub const fn calibration_cutoff(&self) -> &ResearchTemporalCoordinate {
        &self.calibration_cutoff
    }
    /// Original source knowledge cutoff, not the later calculation/publication time.
    #[must_use]
    pub const fn available_at(&self) -> Timestamp {
        self.available_at
    }
    /// Actual forecast publication time.
    #[must_use]
    pub const fn published_at(&self) -> Timestamp {
        self.published_at
    }
    /// Original forecast risk expiry.
    #[must_use]
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }
    /// Exact terminal effective time.
    #[must_use]
    pub const fn target_at(&self) -> Option<Timestamp> {
        self.target_at
    }
    /// Raw admitted estimator output before addition of validation errors.
    #[must_use]
    pub const fn central(&self) -> ForecastValue {
        self.central
    }
    /// Ordered outcomes in the native measurement, with exact normalized masses.
    #[must_use]
    pub fn points(&self) -> &[ForecastDistributionPoint] {
        &self.points
    }
}

fn validate_distribution_path(
    bundle: &ModelBundle,
    path: &ForecastPath,
    residual_distribution: &ForecastResidualDistribution,
) -> Result<(), ForecastError> {
    let [_terminal] = path.points() else {
        return Err(ForecastError::InvalidHorizon);
    };
    if path.metadata_hash() != bundle.metadata().metadata_hash()
        || path.artifact_hash() != bundle.metadata().artifact_hash()
        || path.training_run_hash() != bundle.metadata().training_run_hash()
        || path.output_binding() != bundle.metadata().output_binding()
        || path.dataset() != bundle.metadata().dataset()
        || path.output_binding().central_statistic()
            != ForecastCentralStatistic::ModelEstimatedConditionalMean
        || !path.output_binding().admits_path_horizon(path.horizon())
        || path.calibration().is_none_or(|calibration| {
            !calibration.matches_coordinate(bundle.metadata(), path.calibration_cutoff())
                || calibration.residuals_hash() != residual_distribution.residuals_hash()
        })
    {
        return Err(ForecastError::InvalidVintage);
    }
    Ok(())
}

fn split_coordinates(
    policy: market_squawk_data::ChronologicalSplitPolicy,
) -> Result<[ResearchTemporalCoordinate; 3], ForecastError> {
    match (policy.timestamp_boundaries(), policy.fiscal_boundaries()) {
        (Some(times), None) => Ok(times.map(ResearchTemporalCoordinate::exact)),
        (None, Some(dates)) => Ok(dates.map(ResearchTemporalCoordinate::calendar_date)),
        _ => Err(ForecastError::InvalidRequest),
    }
}

fn hash_coordinate(
    hash: &mut Sha256,
    coordinate: &ResearchTemporalCoordinate,
) -> Result<(), ForecastError> {
    let bytes = serde_json::to_vec(coordinate).map_err(|_| ForecastError::InvalidRequest)?;
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
    Ok(())
}

fn terminal_support(
    central: ForecastValue,
    residual_distribution: &ForecastResidualDistribution,
) -> Result<Vec<ForecastDistributionPoint>, ForecastError> {
    let mut points: Vec<ForecastDistributionPoint> = Vec::new();
    points
        .try_reserve_exact(residual_distribution.masses().len())
        .map_err(|_| ForecastError::Capacity)?;
    for mass in residual_distribution.masses() {
        let value = mass.apply_to(central)?;
        if let Some(previous) = points.last_mut()
            && previous.value == value
        {
            previous.probability_ppm =
                NonZeroU32::new(previous.probability_ppm.get() + mass.probability_ppm().get())
                    .ok_or(ForecastError::InvalidCalibration)?;
        } else {
            points.push(ForecastDistributionPoint {
                value,
                probability_ppm: mass.probability_ppm(),
            });
        }
    }
    if points.is_empty()
        || points.windows(2).any(|pair| pair[0].value >= pair[1].value)
        || points
            .iter()
            .map(|point| point.probability_ppm.get())
            .sum::<u32>()
            != 1_000_000
    {
        return Err(ForecastError::InvalidCalibration);
    }
    Ok(points)
}
