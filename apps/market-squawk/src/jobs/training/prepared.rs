//! Product-owned preparation over the existing dataset receipt and training runner.

use std::{num::NonZeroU32, time::Instant};

use market_squawk_analytics::{FeatureCompatibility, FeatureKey};
use market_squawk_backtesting::{RECOMMENDATION_TARGET_HORIZON_NANOS_V1, RecommendationOosFoldV1};
use market_squawk_data::{
    ChronologicalSplitPolicy, ComponentKind, ComponentScope, CorporateActionSensitivity,
    DatasetBuildPurpose, DatasetManifestRef, DatasetStudyPolicy, DatasetTargetHorizon,
    FeatureDatasetProductContract, FeatureLabelComponentSpec, FeatureLabelMeasurement,
    FixedHorizonOriginBasis, PythonDatasetSelection, PythonDatasetVerificationLimits, Sha256Digest,
    verify_python_dataset,
};
use market_squawk_domain::{CalendarDate, HistoricalStudyBasis, ModelId};
use market_squawk_modeling::{
    ProductionFeatureRegistry, PythonDatasetAdmissionAuthority, VerifiedTrainingEnvironment,
};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::*;
use crate::application::HistoricalFiscalTrainingAuthority;
use crate::application::analysis::HistoricalFoldTrainingAuthorityV1;
use crate::application::analytical_profile::{
    AnalyticalModelBundlePolicy, ValidatedAnalyticalProfile,
};

/// This non-deserializable input owns the product's recipe before the worker starts.
/// It carries no caller-provided authority document, model vector, or file path.
pub(crate) struct PreparedProductTraining {
    config: Box<[u8]>,
    commitment: EvidenceDigest,
    dataset: PythonDatasetAdmissionAuthority,
    manifest: DatasetManifestRef,
    study: DatasetStudyPolicy,
    source_snapshot: Sha256Digest,
    splits: ChronologicalSplitPolicy,
    fixed_authority: Value,
    features: Value,
    probability: bool,
}

impl PreparedProductTraining {
    pub(crate) const fn commitment(&self) -> EvidenceDigest {
        self.commitment
    }
    fn try_new(
        paths: &LocalPaths,
        selection: &PythonDatasetSelection,
        expected_manifest: &DatasetManifestRef,
        profile: &ValidatedAnalyticalProfile,
        fold: Option<&RecommendationOosFoldV1>,
        environment: &VerifiedTrainingEnvironment,
        historical_role: Option<&HistoricalFoldTrainingAuthorityV1>,
        fiscal_role: Option<&HistoricalFiscalTrainingAuthority>,
    ) -> Result<Self, TrainingJobRunnerError> {
        let invalid = || TrainingJobRunnerError::InvalidInput;
        let contract = selection.product_contract();
        let probability = probability_training_contract(contract);
        if probability && (fold.is_some() || historical_role.is_some() || fiscal_role.is_some()) {
            return Err(invalid());
        }
        if fiscal_role.is_some_and(|role| {
            !role.admits(selection, profile)
                || !contract.is_financial()
                || fold.is_some()
                || historical_role.is_some()
        }) {
            return Err(invalid());
        }
        if historical_role
            .is_some_and(|role| !role.admits(selection, profile) || fold != Some(role.fold()))
        {
            return Err(invalid());
        }

        if selection.local_root() != paths.root()
            || selection.identity().manifest() != expected_manifest
            || !(probability || matches!(contract,
                FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonForwardReturnTrainingV1
                | FeatureDatasetProductContract::FinancialAmountFiscalPeriodsTrainingV1))
            // The catalog-validated pin selects the price/return forecast. Native fiscal
            // training derives its separate role and horizon from the admitted dataset below.
            || (!contract.is_financial()
                && !probability
                && historical_role.is_none()
                && !matches!(profile.resolution().configuration.model_bundle_policy,
                    AnalyticalModelBundlePolicy::BestAdmittedCalibratedMeanV1))
        { return Err(invalid()); }
        let study = *selection.study_policy().ok_or_else(invalid)?;
        let snapshot = selection.source_snapshot_digest().ok_or_else(invalid)?;
        if study.purpose() != DatasetBuildPurpose::Training
            || study.snapshot_as_of() != selection.as_of()
            || snapshot.bytes() == [0; 32]
            || (study.basis() == HistoricalStudyBasis::RetrospectiveFrozenSnapshot
                && !profile
                    .recommendation_policy()
                    .parameters()
                    .allow_retrospective_studies)
        {
            return Err(invalid());
        }
        let actions = if contract.is_financial() {
            CorporateActionSensitivity::NotApplicable
        } else {
            CorporateActionSensitivity::RequiresAdjustment
        };
        let label = FeatureLabelComponentSpec::try_new(
            ComponentKind::Label,
            ComponentScope::Instrument,
            actions,
            contract.label_component_name(),
            NonZeroU32::MIN,
        )
        .map_err(|_| invalid())?;
        let label_wire = json!({"kind":"label", "scope":"instrument",
            "corporate_action_sensitivity": if contract.is_financial() {"not_applicable"} else {"requires_adjustment"},
            "name":contract.label_component_name(), "version":1});
        let measurement = match selection.label_measurement(&label).ok_or_else(invalid)? {
            FeatureLabelMeasurement::Probability if probability => json!({"kind":"probability"}),
            FeatureLabelMeasurement::Return if !contract.is_financial() && !probability => {
                json!({"kind":"return"})
            }
            FeatureLabelMeasurement::FinancialAmount {
                currency,
                role,
                basis,
                share_convention,
            } if contract.is_financial() => {
                json!({"kind":"financial_amount", "currency":currency.as_str(),
                    "role":role, "basis":basis, "share_convention":share_convention})
            }
            _ => return Err(invalid()),
        };
        if probability {
            let samples = selection
                .probability_label_observations(&label)
                .ok_or_else(invalid)?;
            let boundaries = selection
                .split_policy()
                .timestamp_boundaries()
                .ok_or_else(invalid)?;
            let mut classes = [[false; 2]; 3];
            let mut origins: [std::collections::BTreeSet<Timestamp>; 3] =
                std::array::from_fn(|_| std::collections::BTreeSet::new());
            for sample in samples {
                let index = match sample.split() {
                    market_squawk_data::DatasetSplit::Train => 0,
                    market_squawk_data::DatasetSplit::Validation => 1,
                    market_squawk_data::DatasetSplit::Test => 2,
                };
                if sample.label_maturity() > boundaries[index]
                    || sample.features().len() != selection.probability_feature_specs().len()
                    || (index > 0 && sample.partition_origin() <= boundaries[index - 1])
                {
                    return Err(invalid());
                }
                classes[index][usize::from(sample.value())] = true;
                origins[index].insert(sample.partition_origin());
            }
            if classes[0] != [true, true]
                || classes[1] != [true, true]
                || origins[1].len() < 2
                || origins[2].len() < 2
            {
                return Err(invalid());
            }
        }
        let horizon = selection.label_target_horizon(&label).ok_or_else(invalid)?;
        if horizon != study.target_horizon() {
            return Err(invalid());
        }
        let target = match horizon {
            DatasetTargetHorizon::ExactElapsed(duration)
                if probability
                    && profile
                        .horizon()
                        .step_nanos()
                        .map(|step| u128::from(step.get()))
                        == Some(duration.as_nanos()) =>
            {
                let event = selection
                    .label_probability_event_target(&label)
                    .ok_or_else(invalid)?;
                event.validate().map_err(|_| invalid())?;
                let origin = selection
                    .label_fixed_horizon_origin_basis(&label)
                    .ok_or_else(invalid)?;
                let origin = match origin {
                    FixedHorizonOriginBasis::CompletedBarClose => "completed_bar_close",
                    FixedHorizonOriginBasis::NamedSessionCloseForNominalDailyBar => {
                        "named_session_close_for_nominal_daily_bar"
                    }
                    _ => return Err(invalid()),
                };
                json!({"kind":"fixed_horizon_event", "horizon_nanos":u64::try_from(duration.as_nanos()).map_err(|_| invalid())?,
                    "origin_basis":origin, "event":event})
            }
            DatasetTargetHorizon::ExactElapsed(duration)
                if !contract.is_financial()
                    && duration.as_nanos() == RECOMMENDATION_TARGET_HORIZON_NANOS_V1 as u128
                    && selection.label_fixed_horizon_origin_basis(&label)
                        == Some(FixedHorizonOriginBasis::CompletedBarClose)
                    && profile
                        .horizon()
                        .step_nanos()
                        .map(|step| u128::from(step.get()))
                        == Some(duration.as_nanos()) =>
            {
                json!({"kind":"fixed_horizon_terminal", "horizon_nanos":RECOMMENDATION_TARGET_HORIZON_NANOS_V1,
                        "origin_basis":"completed_bar_close"})
            }
            DatasetTargetHorizon::FiscalPeriods {
                cadence,
                periods_ahead,
            } if contract.is_financial()
                && selection.label_fixed_horizon_origin_basis(&label).is_none() =>
            {
                json!({"kind":"financial_period", "cadence":cadence, "periods_ahead":periods_ahead})
            }
            _ => return Err(invalid()),
        };
        if let Some(fold) = fold {
            let two_years = RECOMMENDATION_TARGET_HORIZON_NANOS_V1
                .checked_mul(2)
                .ok_or_else(invalid)?;
            let start = fold.starts_at().unix_nanos();
            let end = fold.ends_at().unix_nanos();
            if contract.is_financial()
                || end.checked_sub(start) != Some(two_years)
                || selection
                    .split_policy()
                    .timestamp_boundaries()
                    .map(|v| v.map(Timestamp::unix_nanos))
                    != Some([
                        start
                            .checked_sub(two_years)
                            .and_then(|v| v.checked_sub(1))
                            .ok_or_else(invalid)?,
                        start.checked_sub(1).ok_or_else(invalid)?,
                        end.checked_sub(1).ok_or_else(invalid)?,
                    ])
            {
                return Err(invalid());
            }
        }
        let registry = ProductionFeatureRegistry::try_new().map_err(|_| invalid())?;
        let names = std::iter::once(contract.feature_component_name()).chain(
            contract
                .macro_components()
                .iter()
                .map(|entry| entry.component_name()),
        );
        let mut features = Vec::new();
        for name in names {
            let key = FeatureKey::try_new(name, NonZeroU32::MIN).map_err(|_| invalid())?;
            let metadata = registry
                .feature_registry()
                .try_resolve(&key, FeatureCompatibility::PointInTime)
                .map_err(|_| invalid())?;
            features.push(json!({"name":name,"version":1,
                "input_schema_sha256":encode_hex(metadata.input_schema_digest().as_bytes()),
                "semantic_sha256":encode_hex(metadata.semantic_digest().as_bytes())}));
        }
        let output_statistic = if probability {
            json!({"statistic":"unavailable", "target":target,
                "target_transform":"identity", "output_transform":"logistic", "objective":"binary_cross_entropy",
                "estimator":{"kind":"sealed_binary_logistic_v1"}})
        } else {
            json!({"statistic":"model_estimated_conditional_mean", "target":target,
            "target_transform":"identity", "output_transform":"identity", "objective":"squared_error",
            "estimator":{"kind":"sealed_direct_least_squares_v1"}})
        };
        let mut fixed = json!({"schema_version":8, "label":label_wire,
            "training_code_revision":environment.training_code_revision(),
            "training_environment_sha256":encode_hex(environment.receipt_sha256()),
            "output_measurement":measurement, "output_statistic":output_statistic, "output_semantics":if probability {"binary_probability"} else {"regression"}});
        let plan = json!({"historicalFiscalOrigin":fiscal_role.map(|role| encode_hex(role.identity().bytes())), "historicalPlan":historical_role.map(|role| encode_hex(role.plan_digest().bytes())), "profile":profile.resolution(), "datasetExport":encode_hex(selection.export_sha256().bytes()),
            "selection":encode_hex(selection.selection_sha256().bytes()), "catalog":encode_hex(selection.catalog_identity().bytes()),
            "manifest":encode_hex(expected_manifest.content_hash().bytes()), "contract":contract.identity(),
            "sourceSnapshot":encode_hex(snapshot.bytes()), "fixed":fixed, "features":features,
            "fold":fold.map(|fold| json!({"id":fold.fold_id().as_str(), "start":fold.starts_at().unix_nanos(), "end":fold.ends_at().unix_nanos()}))});
        let plan_bytes = serde_json::to_vec(&plan).map_err(|_| invalid())?;
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/product-training-plan/v1\0");
        hash.update(plan_bytes);
        let plan_sha: [u8; 32] = hash.finalize().into();
        let mut uuid_bytes = [0_u8; 16];
        uuid_bytes.copy_from_slice(&plan_sha[..16]);
        let model_id =
            ModelId::try_from(uuid::Uuid::from_bytes(uuid_bytes)).map_err(|_| invalid())?;
        let bundle_id = format!("product-training-{}", encode_hex(plan_sha));
        fixed["model_id"] = json!(model_id);
        fixed["bundle_id"] = json!(bundle_id);
        fixed["bundle_version"] = json!(1);
        let config = serde_json::to_vec(&json!({"schemaVersion":1,
            "dataset":{"root":paths.root(), "exportSha256":encode_hex(selection.export_sha256().bytes()),
                "productContract":contract.identity(), "asOfUnixNanos":selection.as_of().unix_nanos(),
                "maximumRows":100_000,"maximumBytes":256*1024*1024},
            "training":{"features":features,"label":label_wire,"seed":0,"missingPolicy":if probability {"drop_row"} else {"reject"},
                "modelId":model_id,"bundleId":bundle_id,"bundleVersion":1,"modelKind":if probability {"logistic"} else {"linear"},"artifactFormat":"onnx"},
            "operation":{"timeoutMilliseconds":60_000,"maximumOperations":50_000_000},
            "onnx":{"opset":13,"inferenceDeadlineMilliseconds":5_000,"fallback":"no_action"}}))
            .map_err(|_| invalid())?;
        if config.len() as u64 > MAXIMUM_CONFIG_BYTES {
            return Err(invalid());
        }
        Ok(Self {
            config: config.into_boxed_slice(),
            commitment: EvidenceDigest::new(DigestAlgorithm::Sha256, plan_sha),
            dataset: dataset_authority(selection)?,
            manifest: expected_manifest.clone(),
            study,
            source_snapshot: snapshot,
            splits: selection.split_policy(),
            fixed_authority: fixed,
            features: json!(features),
            probability,
        })
    }

    fn revalidate(
        &self,
        paths: &LocalPaths,
        cancellation: &CancellationToken,
    ) -> Result<(), TrainingJobRunnerError> {
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(30))
            .ok_or(TrainingJobRunnerError::InvalidInput)?;
        let selection = verify_python_dataset(
            paths.root(),
            self.dataset.export_sha256(),
            self.dataset.product_contract(),
            self.dataset.as_of(),
            PythonDatasetVerificationLimits::try_new(100_000, 256 * 1024 * 1024)
                .map_err(|_| TrainingJobRunnerError::InvalidInput)?,
            deadline,
            cancellation,
        )
        .map_err(|_| TrainingJobRunnerError::InputChanged)?;
        if dataset_authority(&selection)? != self.dataset
            || selection.identity().manifest() != &self.manifest
            || selection.study_policy() != Some(&self.study)
            || selection.source_snapshot_digest() != Some(self.source_snapshot)
            || selection.split_policy() != self.splits
        {
            return Err(TrainingJobRunnerError::InputChanged);
        }
        Ok(())
    }

    fn authorize(
        &self,
        paths: &LocalPaths,
        staging: &TrainingStaging,
        request: &[u8],
        candidate: &market_squawk_modeling::TrainingWorkerCandidate,
        environment: &VerifiedTrainingEnvironment,
    ) -> Result<crate::application::model::runtime::ModelAdmissionRequest, TrainingJobRunnerError>
    {
        let invalid = || TrainingJobRunnerError::InvalidCandidate;
        let relative = PathBuf::from(&staging.candidate_directory).join("authority-proposal.json");
        let bytes = read_artifact(paths, &relative, MAX_BUNDLE_AUTHORITY_BYTES as u64)?;
        let authority: Value = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        for (key, expected) in self.fixed_authority.as_object().ok_or_else(invalid)? {
            if authority.get(key) != Some(expected) {
                return Err(invalid());
            }
        }
        let dataset = &authority["dataset"];
        if dataset["export_sha256"] != json!(encode_hex(self.dataset.export_sha256().bytes()))
            || dataset["selection_sha256"]
                != json!(encode_hex(self.dataset.selection_sha256().bytes()))
            || dataset["catalog_identity_sha256"]
                != json!(encode_hex(self.dataset.catalog_identity().bytes()))
            || dataset["selection_as_of_unix_nanos"] != json!(self.dataset.as_of().unix_nanos())
        {
            return Err(invalid());
        }
        validate_period(&authority["training_period"], self.splits)?;
        let run_bytes = read_artifact(
            paths,
            &PathBuf::from(&staging.candidate_directory).join("training-run.json"),
            256 * 1024,
        )?;
        if Sha256::digest(&run_bytes).as_slice() != candidate.training_run_sha256() {
            return Err(invalid());
        }
        let run: Value = serde_json::from_slice(&run_bytes).map_err(|_| invalid())?;
        let trial = &run["trial"];
        if trial["features"] != self.features
            || trial["seed"] != json!(0)
            || trial["missing_policy"]
                != if self.probability {
                    "drop_row"
                } else {
                    "reject"
                }
            || trial["model_kind"]
                != if self.probability {
                    "logistic"
                } else {
                    "linear"
                }
        {
            return Err(invalid());
        }
        // ModelBundle independently closes and cross-checks trial, metadata, output binding,
        // calibration, registry descriptors, hashes, release, and current catalog rights.
        let request =
            crate::application::model::runtime::ModelAdmissionRequest::decode_training_worker(
                request,
                bytes,
                &paths.artifacts()?.root().join(relative),
                candidate,
                environment,
            )
            .map_err(|_| invalid())?;
        request
            .require_dataset_authority(self.dataset)
            .map_err(|_| invalid())?;
        Ok(request)
    }
}

fn probability_training_contract(contract: FeatureDatasetProductContract) -> bool {
    matches!(contract,
        FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonPriceHigherTrainingV1
        | FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonBenchmarkOutperformanceTrainingV1
        | FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonProfitAfterCostsTrainingV1)
}

fn dataset_authority(
    selection: &PythonDatasetSelection,
) -> Result<PythonDatasetAdmissionAuthority, TrainingJobRunnerError> {
    PythonDatasetAdmissionAuthority::try_new(
        selection.export_sha256(),
        selection.as_of(),
        selection.selection_sha256(),
        selection.catalog_identity(),
        selection.product_contract(),
    )
    .map_err(|_| TrainingJobRunnerError::InvalidInput)
}

fn validate_period(
    value: &Value,
    splits: ChronologicalSplitPolicy,
) -> Result<(), TrainingJobRunnerError> {
    let invalid = || TrainingJobRunnerError::InvalidCandidate;
    let valid = if let Some([train_end, _, _]) = splits.timestamp_boundaries() {
        let start = value["start_unix_nanos"].as_i64().ok_or_else(invalid)?;
        let end = value["end_unix_nanos"].as_i64().ok_or_else(invalid)?;
        value["kind"] == "exact_time"
            && start < end
            && end
                .checked_sub(1)
                .is_some_and(|v| v <= train_end.unix_nanos())
    } else if let Some([train_end, _, _]) = splits.fiscal_boundaries() {
        let start: CalendarDate =
            serde_json::from_value(value["start"].clone()).map_err(|_| invalid())?;
        let end: CalendarDate =
            serde_json::from_value(value["end"].clone()).map_err(|_| invalid())?;
        value["kind"] == "fiscal_dates"
            && start < end
            && end
                .days_since_unix_epoch()
                .checked_sub(1)
                .is_some_and(|v| v <= train_end.days_since_unix_epoch())
    } else {
        false
    };
    if valid { Ok(()) } else { Err(invalid()) }
}

fn read_artifact(
    paths: &LocalPaths,
    relative: &Path,
    maximum: u64,
) -> Result<Box<[u8]>, TrainingJobRunnerError> {
    let file = paths.artifacts()?.resolve(relative)?.open_read()?;
    let length = file
        .metadata()
        .map_err(|_| TrainingJobRunnerError::InvalidCandidate)?
        .len();
    if length == 0 || length > maximum {
        return Err(TrainingJobRunnerError::InvalidCandidate);
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length as usize)
        .map_err(|_| TrainingJobRunnerError::Capacity)?;
    file.take(maximum + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| TrainingJobRunnerError::InvalidCandidate)?;
    if bytes.len() as u64 != length {
        return Err(TrainingJobRunnerError::InvalidCandidate);
    }
    Ok(bytes.into_boxed_slice())
}

pub(super) enum TrainingAdmission {
    Governed(GovernedTrainingInput),
    Product(PreparedProductTraining),
}
impl TrainingAdmission {
    pub(super) fn evidence_digest(&self) -> EvidenceDigest {
        match self {
            Self::Governed(value) => value.evidence_digest(),
            Self::Product(value) => value.commitment,
        }
    }
    pub(super) fn revalidate(
        &self,
        paths: &LocalPaths,
        cancellation: &CancellationToken,
    ) -> Result<(), TrainingJobRunnerError> {
        match self {
            Self::Governed(value) => value.revalidate(paths),
            Self::Product(value) => value.revalidate(paths, cancellation),
        }
    }
    pub(super) fn stdin(&self) -> Vec<u8> {
        match self {
            Self::Governed(_) => Vec::new(),
            Self::Product(value) => value.config.to_vec(),
        }
    }
    pub(super) fn authorize(
        &self,
        paths: &LocalPaths,
        staging: &TrainingStaging,
        request: &[u8],
        candidate: &market_squawk_modeling::TrainingWorkerCandidate,
        environment: &VerifiedTrainingEnvironment,
    ) -> Result<crate::application::model::runtime::ModelAdmissionRequest, TrainingJobRunnerError>
    {
        match self {
            Self::Product(value) => {
                value.authorize(paths, staging, request, candidate, environment)
            }
            Self::Governed(value) => {
                crate::application::model::runtime::ModelAdmissionRequest::decode_training_worker(
                    request,
                    value.authority_bytes.clone(),
                    value.authority.path(),
                    candidate,
                    environment,
                )
                .map_err(|_| TrainingJobRunnerError::InvalidCandidate)
            }
        }
    }
}
impl TrainingJobRunner {
    pub(crate) fn prepare_product(
        &self,
        selection: &PythonDatasetSelection,
        manifest: &DatasetManifestRef,
        profile: &ValidatedAnalyticalProfile,
        fold: Option<&RecommendationOosFoldV1>,
    ) -> Result<PreparedProductTraining, TrainingJobRunnerError> {
        PreparedProductTraining::try_new(
            &self.paths,
            selection,
            manifest,
            profile,
            fold,
            self.runtime
                .training_environment()
                .map_err(|_| TrainingJobRunnerError::WorkerUnavailable)?,
            None,
            None,
        )
    }
    /// Separate source-proven training role. An Exact live-price pin remains part of the
    /// unchanged profile and cannot be reused as an in-sample model for these held-out folds.
    pub(crate) fn prepare_historical_product(
        &self,
        selection: &PythonDatasetSelection,
        profile: &ValidatedAnalyticalProfile,
        role: &HistoricalFoldTrainingAuthorityV1,
    ) -> Result<PreparedProductTraining, TrainingJobRunnerError> {
        PreparedProductTraining::try_new(
            &self.paths,
            selection,
            selection.identity().manifest(),
            profile,
            Some(role.fold()),
            self.runtime
                .training_environment()
                .map_err(|_| TrainingJobRunnerError::WorkerUnavailable)?,
            Some(role),
            None,
        )
    }
    /// Source-owned native fiscal origin role. The full live-price profile pin is preserved.
    pub(crate) fn prepare_historical_fiscal_product(
        &self,
        selection: &PythonDatasetSelection,
        profile: &ValidatedAnalyticalProfile,
        role: &HistoricalFiscalTrainingAuthority,
    ) -> Result<PreparedProductTraining, TrainingJobRunnerError> {
        PreparedProductTraining::try_new(
            &self.paths,
            selection,
            selection.identity().manifest(),
            profile,
            None,
            self.runtime
                .training_environment()
                .map_err(|_| TrainingJobRunnerError::WorkerUnavailable)?,
            None,
            Some(role),
        )
    }
    pub(crate) fn admit_prepared(
        &self,
        prepared: PreparedProductTraining,
        captured_at: Timestamp,
    ) -> Result<JobAdmission, TrainingJobRunnerError> {
        self.admit_input(TrainingAdmission::Product(prepared), captured_at)
    }
}
