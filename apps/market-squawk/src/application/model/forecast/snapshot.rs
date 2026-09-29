//! Basis-qualified historical inference over the same immutable admitted runtime image.

use market_squawk_data::{
    DatasetBuildPurpose, FeatureDatasetInputCoordinate, FeatureDatasetInputEpoch,
    ForecastFeatureValue, Sha256Digest,
};
use market_squawk_domain::{ModelId, Timestamp};
use market_squawk_modeling::{
    BundleId, CalibrationEvidence, CalibrationWindow, ForecastHorizon, ForecastMeasurement,
    ForecastOutputBinding, ForecastPath, ForecastRequest, ForecastStudyDistribution,
    ModelFeatureValue, ModelInput, ModelMetadata, ResearchForecastBackend, TrainingDatasetIdentity,
    TrainingPeriod,
};
use market_squawk_services::{RequestContext, ServiceError};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    num::{NonZeroU16, NonZeroU64},
    time::{SystemTime, UNIX_EPOCH},
};

use super::super::runtime::{ProductionModelRuntime, RetainedForecastRuntime};
use super::{SelectedPriceForecastPoint, SelectedPriceInterval, SelectedPriceIntervals};

mod fiscal;
pub(crate) use fiscal::HistoricalFinancialForecast;

/// A single retained image and exact backend selected before any held-out scoring.
#[derive(Debug)]
pub(crate) struct SelectedForecastRuntime {
    retained: RetainedForecastRuntime,
    metadata: ModelMetadata,
    reference: ForecastStudyRuntimeReference,
    reopened_at: Option<Timestamp>,
}

/// Inert exact-artifact coordinates; authority requires the original admitted request and reader.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ForecastStudyRuntimeReference {
    model_id: ModelId,
    #[serde(
        serialize_with = "serialize_bundle_id",
        deserialize_with = "deserialize_bundle_id"
    )]
    bundle_id: BundleId,
    bundle_version: NonZeroU64,
    metadata_hash: [u8; 32],
    artifact_hash: [u8; 32],
    training_run_hash: [u8; 32],
    output_binding_hash: [u8; 32],
    runtime_generation: [u8; 32],
    selected_at: Timestamp,
}

impl ForecastStudyRuntimeReference {
    pub(crate) fn identity(&self) -> Sha256Digest {
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/forecast-study-runtime-reference/v1\0");
        hash.update(self.model_id.as_uuid().as_bytes());
        hash.update((self.bundle_id.as_str().len() as u64).to_be_bytes());
        hash.update(self.bundle_id.as_str().as_bytes());
        hash.update(self.bundle_version.get().to_be_bytes());
        for digest in [
            self.metadata_hash,
            self.artifact_hash,
            self.training_run_hash,
            self.output_binding_hash,
            self.runtime_generation,
        ] {
            hash.update(digest);
        }
        hash.update(self.selected_at.unix_nanos().to_be_bytes());
        Sha256Digest::new(hash.finalize().into())
    }

    fn matches(&self, metadata: &ModelMetadata) -> bool {
        metadata.model_id() == self.model_id
            && metadata.bundle_id() == &self.bundle_id
            && metadata.bundle_version() == self.bundle_version
            && metadata.metadata_hash().bytes() == self.metadata_hash
            && metadata.artifact_hash().bytes() == self.artifact_hash
            && metadata.training_run_hash().bytes() == self.training_run_hash
            && metadata.output_binding().identity().bytes() == self.output_binding_hash
    }
}

/// Actual frozen-model inference with its original data-owned economic epoch.
#[derive(Clone, Debug)]
pub(crate) struct HistoricalPriceForecast {
    native_distribution: ForecastStudyDistribution,
    terminal: SelectedPriceForecastPoint,
    runtime_generation: Sha256Digest,
    runtime_selected_at: Timestamp,
    runtime_reopened_at: Option<Timestamp>,
    calculated_at: Timestamp,
}

impl ProductionModelRuntime {
    /// Select all predeclared fold models from one immutable runtime image after exact completed
    /// training-job receipts have been reopened. No newer or best-performing model can substitute.
    pub(crate) fn select_completed_historical_runtimes<const N: usize>(
        &self,
        admissions: &[super::super::runtime::ModelAdmissionReceipt; N],
    ) -> Result<[SelectedForecastRuntime; N], ServiceError> {
        if N == 0 || N > 27 {
            return Err(ServiceError::InvalidRequest);
        }
        let retained = self
            .retain_forecast_runtime()
            .map_err(|_| ServiceError::Unavailable)?;
        let selected_at = wall_now()?;
        let mut selected = Vec::new();
        selected
            .try_reserve_exact(N)
            .map_err(|_| ServiceError::ResourceExhausted)?;
        for admission in admissions {
            let bundle = retained
                .image
                .registry
                .get(admission.bundle_id(), admission.bundle_version())
                .map_err(|_| ServiceError::Unavailable)?
                .ok_or(ServiceError::InvalidResult)?;
            let metadata = bundle.metadata();
            if metadata.model_id() != admission.model_id()
                || metadata.metadata_hash() != admission.metadata_sha256()
                || metadata.artifact_hash() != admission.artifact_sha256()
                || metadata.training_run_hash() != admission.training_run_sha256()
                || metadata.dataset().selection_digest() != admission.dataset_selection_sha256()
            {
                return Err(ServiceError::InvalidResult);
            }
            let reference = ForecastStudyRuntimeReference {
                model_id: metadata.model_id(),
                bundle_id: metadata.bundle_id().clone(),
                bundle_version: metadata.bundle_version(),
                metadata_hash: metadata.metadata_hash().bytes(),
                artifact_hash: metadata.artifact_hash().bytes(),
                training_run_hash: metadata.training_run_hash().bytes(),
                output_binding_hash: metadata.output_binding().identity().bytes(),
                runtime_generation: retained.generation_sha256.bytes(),
                selected_at,
            };
            selected.push(SelectedForecastRuntime {
                retained: RetainedForecastRuntime {
                    generation_sha256: retained.generation_sha256,
                    image: std::sync::Arc::clone(&retained.image),
                },
                metadata: metadata.clone(),
                reference,
                reopened_at: None,
            });
        }
        selected.try_into().map_err(|_| ServiceError::Internal)
    }

    pub(crate) fn select_forecast_runtime(
        &self,
        model_id: ModelId,
        bundle_id: &BundleId,
        bundle_version: NonZeroU64,
        expected_generation: Sha256Digest,
    ) -> Result<SelectedForecastRuntime, ServiceError> {
        let retained = self
            .retain_forecast_runtime()
            .map_err(|_| ServiceError::Unavailable)?;
        if retained.generation_sha256 != expected_generation {
            return Err(ServiceError::Unavailable);
        }
        let bundle = retained
            .image
            .registry
            .get(bundle_id, bundle_version)
            .map_err(|_| ServiceError::Unavailable)?
            .ok_or(ServiceError::NotFound)?;
        let metadata = bundle.metadata();
        if metadata.model_id() != model_id {
            return Err(ServiceError::NotFound);
        }
        let reference = ForecastStudyRuntimeReference {
            model_id,
            bundle_id: metadata.bundle_id().clone(),
            bundle_version,
            metadata_hash: metadata.metadata_hash().bytes(),
            artifact_hash: metadata.artifact_hash().bytes(),
            training_run_hash: metadata.training_run_hash().bytes(),
            output_binding_hash: metadata.output_binding().identity().bytes(),
            runtime_generation: retained.generation_sha256.bytes(),
            selected_at: wall_now()?,
        };
        Ok(SelectedForecastRuntime {
            retained,
            metadata: metadata.clone(),
            reference,
            reopened_at: None,
        })
    }

    pub(crate) fn read_forecast_runtime_reference(
        &self,
        reference: &ForecastStudyRuntimeReference,
    ) -> Result<SelectedForecastRuntime, ServiceError> {
        let reopened_at = wall_now()?;
        if reference.selected_at > reopened_at
            || [
                reference.metadata_hash,
                reference.artifact_hash,
                reference.training_run_hash,
                reference.output_binding_hash,
                reference.runtime_generation,
            ]
            .contains(&[0; 32])
        {
            return Err(ServiceError::InvalidRequest);
        }
        let retained = self
            .retain_forecast_runtime()
            .map_err(|_| ServiceError::Unavailable)?;
        let bundle = retained
            .image
            .registry
            .get(&reference.bundle_id, reference.bundle_version)
            .map_err(|_| ServiceError::Unavailable)?
            .ok_or(ServiceError::NotFound)?;
        let metadata = bundle.metadata();
        if !reference.matches(metadata) {
            return Err(ServiceError::NotFound);
        }
        Ok(SelectedForecastRuntime {
            retained,
            metadata: metadata.clone(),
            reference: reference.clone(),
            reopened_at: Some(reopened_at),
        })
    }
}

impl SelectedForecastRuntime {
    fn metadata(&self) -> &ModelMetadata {
        &self.metadata
    }

    pub(crate) const fn runtime_generation(&self) -> Sha256Digest {
        self.retained.generation_sha256
    }
    pub(crate) const fn reference(&self) -> &ForecastStudyRuntimeReference {
        &self.reference
    }
    pub(crate) fn model_id(&self) -> ModelId {
        self.metadata().model_id()
    }
    pub(crate) fn bundle_id(&self) -> &BundleId {
        self.metadata().bundle_id()
    }
    pub(crate) fn bundle_version(&self) -> NonZeroU64 {
        self.metadata().bundle_version()
    }
    pub(crate) fn metadata_hash(&self) -> Sha256Digest {
        self.metadata().metadata_hash()
    }
    pub(crate) fn artifact_hash(&self) -> Sha256Digest {
        self.metadata().artifact_hash()
    }
    pub(crate) fn training_run_hash(&self) -> Sha256Digest {
        self.metadata().training_run_hash()
    }
    pub(crate) fn output_binding(&self) -> &ForecastOutputBinding {
        self.metadata().output_binding()
    }
    pub(crate) fn training_dataset(&self) -> &TrainingDatasetIdentity {
        self.metadata().dataset()
    }
    pub(crate) fn training_period(&self) -> TrainingPeriod {
        self.metadata().training_period()
    }
    pub(crate) fn calibration_window(&self) -> Option<CalibrationWindow> {
        self.metadata()
            .forecast_calibration()
            .map(|fit| fit.window())
    }
    pub(crate) fn calibration_policy_hash(&self) -> Option<Sha256Digest> {
        self.metadata()
            .forecast_calibration()
            .map(|fit| fit.policy_hash())
    }
    pub(crate) const fn selected_at(&self) -> Timestamp {
        self.reference.selected_at
    }
    pub(crate) const fn reopened_at(&self) -> Option<Timestamp> {
        self.reopened_at
    }

    pub(crate) fn forecast_coordinate(
        &self,
        inputs: FeatureDatasetInputCoordinate<'_>,
        context: &RequestContext,
    ) -> Result<HistoricalPriceForecast, ServiceError> {
        ensure_live(context)?;
        let epoch = inputs.epoch();
        let (Some(origin_at), Some(target_at), Some(decision_at), Some(_)) = (
            epoch.target_origin(),
            epoch.target_at(),
            epoch.decision_at(),
            epoch.market_bar(),
        ) else {
            return Err(ServiceError::Unavailable);
        };
        let dataset = inputs.dataset();
        let metadata = self.metadata();
        let training = metadata.dataset();
        let study = training.study_policy().ok_or(ServiceError::Unavailable)?;
        let input_policy = dataset.study_policy().ok_or(ServiceError::Unavailable)?;
        let [_, calibration_end, evaluation_end] = training
            .split_policy()
            .timestamp_boundaries()
            .ok_or(ServiceError::Unavailable)?;
        if epoch.purpose() != DatasetBuildPurpose::StudyInputs
            || study.purpose() != DatasetBuildPurpose::Training
            || study.basis() != epoch.basis()
            || input_policy.basis() != epoch.basis()
            || study.snapshot_as_of() != epoch.snapshot_as_of()
            || study.decision_lag() != input_policy.decision_lag()
            || study.target_horizon() != input_policy.target_horizon()
            || study.limitations() != epoch.limitations()
            || training.source_snapshot_digest() != Some(epoch.source_snapshot_digest())
            || dataset.source_snapshot_digest() != Some(epoch.source_snapshot_digest())
            || metadata
                .training_period()
                .end()
                .is_none_or(|end| end > origin_at)
            || calibration_end >= origin_at
            || decision_at > evaluation_end
        {
            return Err(ServiceError::Unavailable);
        }
        let horizon_nanos = metadata
            .output_binding()
            .expected_arithmetic_return_horizon_nanos()
            .or_else(|| {
                metadata
                    .output_binding()
                    .expected_terminal_price_horizon_nanos()
            })
            .ok_or(ServiceError::Unavailable)?;
        if origin_at.unix_nanos().checked_add(
            i64::try_from(horizon_nanos.get()).map_err(|_| ServiceError::InvalidRequest)?,
        ) != Some(target_at.unix_nanos())
        {
            return Err(ServiceError::InvalidRequest);
        }
        let mut values = Vec::new();
        values
            .try_reserve_exact(metadata.features().len())
            .map_err(|_| ServiceError::ResourceExhausted)?;
        for binding in metadata.features() {
            ensure_live(context)?;
            let mut candidates = inputs.rows().iter().filter(|row| {
                row.example_id() == epoch.example_id()
                    && row.instrument_id() == epoch.instrument_id()
                    && row.source_selection_as_of() == epoch.source_selection_as_of()
                    && row.decision_at() == epoch.decision_at()
                    && row.label_selection_as_of().is_none()
                    && row.observed_effective_at() == Some(origin_at)
                    && row.label_effective_at() == Some(target_at)
                    && row.target_coordinate_kind() == 3
                    && row.component_kind() == 1
                    && row.component_name() == binding.key().name()
                    && row.component_version() == binding.key().version().get()
            });
            let row = candidates.next().ok_or(ServiceError::Unavailable)?;
            if candidates.next().is_some() {
                return Err(ServiceError::InvalidResult);
            }
            let number = match row.value() {
                ForecastFeatureValue::Float(value) => *value,
                ForecastFeatureValue::Decimal { mantissa, scale } => {
                    *mantissa as f64 / 10_f64.powi(i32::from(*scale))
                }
                ForecastFeatureValue::Missing => return Err(ServiceError::Unavailable),
            };
            let mut value = ModelFeatureValue::from_binding(binding);
            value
                .try_set_value(number)
                .map_err(|_| ServiceError::InvalidResult)?;
            values.push(value);
        }
        let input =
            ModelInput::try_new(metadata, &values).map_err(|_| ServiceError::InvalidResult)?;
        let native_inputs = [input];
        let horizon = ForecastHorizon::try_new(NonZeroU16::MIN, horizon_nanos)
            .map_err(|_| ServiceError::InvalidRequest)?;
        let request = ForecastRequest::try_new(
            epoch.instrument_id(),
            origin_at,
            epoch.source_selection_as_of(),
            horizon,
            12,
            &native_inputs,
        )
        .map_err(|_| ServiceError::InvalidRequest)?;
        let artifacts = metadata
            .forecast_calibration()
            .ok_or(ServiceError::Unavailable)?;
        let calibration = CalibrationEvidence::try_new(
            metadata,
            artifacts.method(),
            artifacts.window(),
            artifacts.policy_hash(),
            artifacts.residuals_hash(),
            *artifacts.bands(),
            artifacts.dependence_assumptions(),
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        let active = self
            .retained
            .image
            .activate(
                metadata.bundle_id(),
                metadata.bundle_version(),
                context.deadline(),
                context.cancellation(),
            )
            .map_err(super::super::runtime_service_error)?;
        let path = active
            .backend()
            .forecast(&request, Some(&calibration))
            .map_err(|_| ServiceError::Unavailable)?;
        ensure_live(context)?;
        let [point] = path.points() else {
            return Err(ServiceError::InvalidResult);
        };
        let intervals = point.intervals().ok_or(ServiceError::InvalidResult)?;
        let pair = |band: market_squawk_modeling::ForecastInterval| SelectedPriceInterval {
            lower: band.lower(),
            upper: band.upper(),
        };
        let mut terminal = SelectedPriceForecastPoint {
            target_at: point.target_at().ok_or(ServiceError::InvalidResult)?,
            central: point.central(),
            intervals: Some(SelectedPriceIntervals {
                interval_50: pair(intervals.interval_50()),
                interval_80: pair(intervals.interval_80()),
                interval_95: pair(intervals.interval_95()),
            }),
        };
        let origin = epoch
            .current_unit_price()
            .map_err(|_| ServiceError::Unavailable)?;
        match metadata.output_binding().measurement() {
            ForecastMeasurement::Return => {
                super::price::convert_points(origin.amount(), std::slice::from_mut(&mut terminal))
                    .map_err(|_| ServiceError::Unavailable)?
            }
            ForecastMeasurement::Price { currency } if currency == origin.currency() => {}
            _ => return Err(ServiceError::Unavailable),
        }
        let calculated_at = wall_now()?;
        if calculated_at < epoch.calculated_at() || calculated_at < epoch.snapshot_as_of() {
            return Err(ServiceError::Unavailable);
        }
        let bundle = active.bundle();
        let native_distribution =
            ForecastStudyDistribution::try_from_admitted_path(&bundle, path, inputs)
                .map_err(|_| ServiceError::InvalidResult)?;
        Ok(HistoricalPriceForecast {
            native_distribution,
            terminal,
            runtime_generation: self.runtime_generation(),
            runtime_selected_at: self.selected_at(),
            runtime_reopened_at: self.reopened_at,
            calculated_at,
        })
    }
}

impl HistoricalPriceForecast {
    pub(crate) const fn path(&self) -> &ForecastPath {
        self.native_distribution.path()
    }
    pub(crate) const fn epoch(&self) -> &FeatureDatasetInputEpoch {
        self.native_distribution.epoch()
    }
    pub(crate) const fn native_distribution(&self) -> &ForecastStudyDistribution {
        &self.native_distribution
    }
    pub(crate) const fn terminal(&self) -> SelectedPriceForecastPoint {
        self.terminal
    }
    pub(crate) const fn runtime_generation(&self) -> Sha256Digest {
        self.runtime_generation
    }
    pub(crate) const fn calculated_at(&self) -> Timestamp {
        self.calculated_at
    }
    pub(crate) const fn runtime_selected_at(&self) -> Timestamp {
        self.runtime_selected_at
    }
    pub(crate) const fn runtime_reopened_at(&self) -> Option<Timestamp> {
        self.runtime_reopened_at
    }
}

fn serialize_bundle_id<S: serde::Serializer>(
    value: &BundleId,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(value.as_str())
}

fn deserialize_bundle_id<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<BundleId, D::Error> {
    struct Visitor;
    impl serde::de::Visitor<'_> for Visitor {
        type Value = BundleId;
        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a bounded canonical bundle identity")
        }
        fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
            BundleId::try_new(value).map_err(E::custom)
        }
    }
    deserializer.deserialize_str(Visitor)
}

fn ensure_live(context: &RequestContext) -> Result<(), ServiceError> {
    if context.cancellation().is_cancelled() {
        return Err(ServiceError::Cancelled);
    }
    if std::time::Instant::now() >= context.deadline() {
        return Err(ServiceError::DeadlineExceeded);
    }
    Ok(())
}

fn wall_now() -> Result<Timestamp, ServiceError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ServiceError::Unavailable)?;
    Ok(Timestamp::from_unix_nanos(
        i64::try_from(duration.as_nanos()).map_err(|_| ServiceError::Unavailable)?,
    ))
}
