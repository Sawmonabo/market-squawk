//! Registry-verified model metadata, immutable bundle inventory, and local inference services.

use std::{
    collections::VecDeque,
    fmt,
    num::{NonZeroU64, NonZeroUsize},
    str::FromStr,
    sync::{Arc, Mutex},
    time::Instant,
};

use async_trait::async_trait;
use chrono::DateTime;
use market_squawk_data::{AnalyticalReadCapability, DatasetManifestRef, Sha256Digest};
use market_squawk_domain::{InstrumentId, ModelId, ResearchTemporalCoordinate, Timestamp};
use market_squawk_modeling::{
    BundleId, CalibrationEvidence, CalibrationMethod, FeatureNormalizer, ForecastCentralStatistic,
    ForecastHorizon, ForecastMeasurement, ForecastOutputBinding, ForecastTargetMeaning,
    ForecastValue, InferenceBackend, ModelBundle, ModelDecision, ModelFeatureValue, ModelFormat,
    ModelInput, ModelMetadata, ModelOutput, ModelRegistry, TrainingDatasetIdentity,
    ValidationMetricName,
};
use market_squawk_services::{
    ArtifactError, ArtifactReadContext, ArtifactReference, RequestContext, ServiceDomain,
    ServiceError, ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use super::{
    ApplicationDomainService,
    domain_support::{
        DomainLifecycle, admitted_result_limits, encode_hex, ensure_request_live,
        opaque_product_token,
    },
    job::{JobView, product_activity_state, product_progress_percent},
};

pub mod backup;
pub mod forecast;
pub mod forecast_preparation;
pub(crate) mod outcome_measurement;
mod outcome_preparation;
mod read_image;
pub mod runtime;

pub use forecast::{ForecastApplicationError, ForecastApplicationService};
pub(crate) use forecast::{
    ForecastJobExecutor, ForecastJobOutput, ForecastPrecommitAuthority,
    ForecastRecoveryCoordinates, ForecastStudyRuntimeReference, HistoricalPriceForecast,
    SelectedForecastDistribution, SelectedForecastDistributionPoint, SelectedForecastRuntime,
    financial_analysis_evidence, financial_coordinate_index, financial_feature_values,
    forecast_recovery_coordinates, reopen_financial_input,
};
pub(crate) use forecast_preparation::PreparedForecastJobInput;

use read_image::{ModelReadImage, ModelReadImageState};

const GET_METADATA: &str = "Model.GetMetadata";
const LIST_BUNDLES: &str = "Model.ListBundles";
const GET_BUNDLE: &str = "Model.GetBundle";
pub(crate) const LIST_PRODUCT_ACTIVITY: &str = "Model.ListProductActivity";
const EVALUATE: &str = "Model.Evaluate";
const PREDICT: &str = "Model.Predict";
const MAXIMUM_EVALUATION_RECORDS: usize = 100_000;

/// Application-owned model surface over atomically replaceable immutable runtime generations.
pub struct ModelDomainService {
    read_image: Arc<ModelReadImageState>,
    forecasts: Option<Arc<ForecastApplicationService>>,
    forecast_analytical: Option<AnalyticalReadCapability>,
    forecast_source_actions:
        Option<super::research::corporate_actions::SourceAppliedCorporateActionReadCapability>,
    forecast_outcome_preparation:
        Option<super::research::corporate_actions::SourceActionPreparationCapability>,
    forecast_calendar: Option<crate::application::market_calendar::ForecastSessionReadCapability>,
    evaluations: Mutex<EvaluationStore>,
    lifecycle: Arc<DomainLifecycle>,
}

impl ModelDomainService {
    /// Binds the existing source/calendar read owners before this service is shared.
    pub(crate) fn with_source_action_reads(
        mut self,
        source_actions: super::research::corporate_actions::SourceAppliedCorporateActionReadCapability,
    ) -> Self {
        self.forecast_source_actions = Some(source_actions);
        self
    }

    /// Binds later-history acquisition through the installed source owner.
    pub(crate) fn with_outcome_preparation(
        mut self,
        source: super::research::corporate_actions::SourceActionPreparationCapability,
    ) -> Self {
        self.forecast_outcome_preparation = Some(source);
        self
    }

    /// Attaches the existing calendar source owner for exact current forecast cohort replay.
    pub(crate) fn with_forecast_calendar(
        mut self,
        calendar: crate::application::market_calendar::CompletedMarketSessionReadCapability,
    ) -> Self {
        self.forecast_calendar = Some(
            crate::application::market_calendar::ForecastSessionReadCapability::Current(calendar),
        );
        self
    }

    /// Binds every retained bundle to exactly one admitted inference backend.
    ///
    /// # Errors
    ///
    /// Rejects an incomplete, duplicate, or registry-inconsistent backend set and excessive
    /// evaluation retention.
    pub fn try_new(
        registry: Arc<ModelRegistry>,
        backends: Vec<Arc<dyn InferenceBackend>>,
        maximum_evaluation_records: NonZeroUsize,
    ) -> Result<Self, ModelDomainServiceError> {
        let image = Arc::new(ModelReadImage::try_new(registry, backends)?);
        Self::try_from_read_image(
            Arc::new(ModelReadImageState::new(image)),
            maximum_evaluation_records,
            None,
            None,
        )
    }

    /// Binds this service directly to a runtime-owned atomically published read image.
    ///
    /// Existing calls retain the immutable generation loaded at call entry. A durable runtime
    /// admission replaces the shared image in one pointer publication, so later calls observe the
    /// complete new registry/backend set without an inconsistent intermediate state.
    pub fn try_from_runtime_snapshot(
        snapshot: runtime::ModelRuntimeSnapshot,
        maximum_evaluation_records: NonZeroUsize,
    ) -> Result<Self, ModelDomainServiceError> {
        Self::try_from_read_image(
            snapshot.into_read_image(),
            maximum_evaluation_records,
            None,
            None,
        )
    }

    /// Binds an installed runtime snapshot and its sole durable forecast publication authority.
    pub fn try_from_runtime_snapshot_with_forecasts(
        snapshot: runtime::ModelRuntimeSnapshot,
        maximum_evaluation_records: NonZeroUsize,
        forecasts: Arc<ForecastApplicationService>,
        analytical: AnalyticalReadCapability,
    ) -> Result<Self, ModelDomainServiceError> {
        Self::try_from_read_image(
            snapshot.into_read_image(),
            maximum_evaluation_records,
            Some(forecasts),
            Some(analytical),
        )
    }

    fn try_from_read_image(
        read_image: Arc<ModelReadImageState>,
        maximum_evaluation_records: NonZeroUsize,
        forecasts: Option<Arc<ForecastApplicationService>>,
        forecast_analytical: Option<AnalyticalReadCapability>,
    ) -> Result<Self, ModelDomainServiceError> {
        if maximum_evaluation_records.get() > MAXIMUM_EVALUATION_RECORDS {
            return Err(ModelDomainServiceError::EvaluationCapacity);
        }
        Ok(Self {
            read_image,
            forecasts,
            forecast_analytical,
            forecast_calendar: None,
            forecast_source_actions: None,
            forecast_outcome_preparation: None,
            evaluations: Mutex::new(EvaluationStore::new(maximum_evaluation_records)),
            lifecycle: DomainLifecycle::new(),
        })
    }

    #[cfg(test)]
    pub(crate) fn admitted_generation_count(&self) -> usize {
        self.read_image.load().len()
    }

    fn metadata(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let model_id = admitted_model_id(request.arguments())?;
        let image = self.read_image.load();
        let retained = image
            .registry
            .latest(model_id)
            .map_err(runtime_service_error)?
            .ok_or(ServiceError::NotFound)?;
        one_result(
            model_metadata_value(
                &retained,
                runtime_health_value(
                    image.len(),
                    image
                        .registry
                        .len()
                        .map_err(|_| ServiceError::Unavailable)?,
                ),
            )?,
            request,
            context,
        )
    }

    fn bundles(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let limits = admitted_result_limits(request, context)?;
        let limit = request
            .arguments()
            .get("limit")
            .map(|value| {
                value
                    .as_u64()
                    .and_then(|value| usize::try_from(value).ok())
                    .ok_or(ServiceError::InvalidRequest)
            })
            .transpose()?
            .unwrap_or(25);
        if limit == 0 || limit > 100 || limit > limits.maximum_result_items() {
            return Err(ServiceError::InvalidRequest);
        }
        let cursor = match request.arguments().get("cursor") {
            None | Some(Value::Null) => None,
            Some(Value::String(value)) => Some(value.as_str()),
            Some(_) => return Err(ServiceError::InvalidRequest),
        };
        let image = self.read_image.load();
        let (models, next_cursor) = match &image.registry {
            read_image::ModelBundleInventory::Disk(inventory) => {
                let (page, next, _) = inventory
                    .page_window(cursor, limit)
                    .map_err(runtime_service_error)?;
                (
                    page.into_iter()
                        .map(|entry| entry.product_summary)
                        .collect::<Vec<_>>(),
                    next,
                )
            }
            read_image::ModelBundleInventory::Memory(_) => {
                let after = cursor
                    .map(str::parse::<usize>)
                    .transpose()
                    .map_err(|_| ServiceError::InvalidRequest)?
                    .unwrap_or(0);
                if after > image.backends.len() {
                    return Err(ServiceError::InvalidRequest);
                }
                let mut models = Vec::new();
                for backend in image.backends.iter().skip(after).take(limit) {
                    ensure_request_live(context, &self.lifecycle)?;
                    let metadata = backend.metadata();
                    let bundle = image
                        .registry
                        .get(metadata.bundle_id(), metadata.bundle_version())
                        .map_err(runtime_service_error)?
                        .ok_or(ServiceError::Unavailable)?;
                    models.push(product_model_summary(&bundle)?);
                }
                let next = after + models.len();
                (
                    models,
                    (next < image.backends.len()).then(|| next.to_string()),
                )
            }
        };
        let count = models.len();
        TypedToolResult::try_new(
            json!({"models":models,"nextCursor":next_cursor}),
            count,
            ToolResultMetadata::complete_not_applicable(),
            limits,
        )
        .map_err(Into::into)
    }

    fn bundle_detail(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let token = request
            .arguments()
            .get("modelToken")
            .and_then(Value::as_str)
            .and_then(|value| value.parse::<uuid::Uuid>().ok())
            .ok_or(ServiceError::InvalidRequest)?;
        let image = self.read_image.load();
        let bundle = match &image.registry {
            read_image::ModelBundleInventory::Disk(inventory) => {
                let entry = inventory
                    .catalog
                    .by_token(inventory.head, token)
                    .map_err(|_| ServiceError::Unavailable)?
                    .ok_or(ServiceError::NotFound)?;
                let admission = runtime::inventory::RuntimeInventory::decode(entry)
                    .map_err(runtime_service_error)?;
                inventory
                    .selection_controlled(&admission, context.deadline(), context.cancellation())
                    .map_err(runtime_service_error)?
            }
            read_image::ModelBundleInventory::Memory(_) => {
                let mut selected = None;
                for backend in &image.backends {
                    ensure_request_live(context, &self.lifecycle)?;
                    let metadata = backend.metadata();
                    let candidate = image
                        .registry
                        .get(metadata.bundle_id(), metadata.bundle_version())
                        .map_err(runtime_service_error)?
                        .ok_or(ServiceError::Unavailable)?;
                    if forecast_model_evidence_projection(&candidate)?.model_token() == token {
                        selected = Some(candidate.selection_metadata());
                        break;
                    }
                }
                selected.ok_or(ServiceError::NotFound)?
            }
        };
        let content = product_model_evidence(&bundle)?;
        if content
            .get("modelToken")
            .and_then(Value::as_str)
            .and_then(|value| value.parse::<uuid::Uuid>().ok())
            != Some(token)
        {
            return Err(ServiceError::InvalidResult);
        }
        one_result(content, request, context)
    }

    fn infer(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        retain_evaluation: bool,
    ) -> Result<TypedToolResult, ServiceError> {
        let model_id = admitted_model_id(request.arguments())?;
        let input_value = request
            .arguments()
            .get("input")
            .and_then(Value::as_object)
            .ok_or(ServiceError::InvalidRequest)?;
        let parsed = ParsedModelInput::try_from(input_value)?;
        let image = self.read_image.load();
        let active = image
            .activate(
                &parsed.bundle_id,
                parsed.bundle_version,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(runtime_service_error)?;
        let backend = active.backend();
        let metadata = backend.metadata();
        if metadata.model_id() != model_id {
            return Err(ServiceError::NotFound);
        }
        if parsed.feature_values.len() != metadata.features().len() {
            return Err(ServiceError::InvalidRequest);
        }
        let mut values = metadata
            .features()
            .iter()
            .map(ModelFeatureValue::from_binding)
            .collect::<Vec<_>>();
        for (slot, value) in values.iter_mut().zip(parsed.feature_values.iter().copied()) {
            slot.try_set_value(value)
                .map_err(|_| ServiceError::InvalidRequest)?;
        }
        let input =
            ModelInput::try_new(metadata, &values).map_err(|_| ServiceError::InvalidRequest)?;
        ensure_request_live(context, &self.lifecycle)?;
        let output = backend
            .infer(&input)
            .map_err(|_| ServiceError::Unavailable)?;
        ensure_request_live(context, &self.lifecycle)?;
        let mut content = model_output_value(&output);
        if retain_evaluation {
            let evidence = self
                .evaluations
                .lock()
                .map_err(|_| ServiceError::Internal)?
                .record(context, input_value, &output)?;
            let object = content.as_object_mut().ok_or(ServiceError::Internal)?;
            object.insert(
                "evaluationEvidence".to_owned(),
                json!({
                    "sequence": evidence.sequence,
                    "digest": encode_hex(evidence.digest),
                    "retention": "bounded_process_local"
                }),
            );
            object.insert(
                "validationMetrics".to_owned(),
                validation_metrics_value(metadata),
            );
        }
        one_result(content, request, context)
    }

    async fn select_latest_valid_forecast(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let (instrument_id, as_of) = admitted_forecast_selection(request.arguments())?;
        let limits = admitted_result_limits(request, context)?;
        let maximum_artifact_bytes =
            NonZeroUsize::new(limits.maximum_result_bytes()).ok_or(ServiceError::InvalidRequest)?;
        ensure_request_live(context, &self.lifecycle)?;
        let selected = forecast::ForecastEvidenceReader::latest_valid_for_instrument(
            self,
            instrument_id,
            as_of,
            forecast::ForecastEvidenceReadContext::new(
                ArtifactReadContext::new(context.cancellation().clone(), context.deadline()),
                maximum_artifact_bytes,
            ),
        )
        .await
        .map_err(map_forecast_selection_error)?;
        ensure_request_live(context, &self.lifecycle)?;
        one_result(latest_valid_forecast_value(&selected)?, request, context)
    }
}

impl fmt::Debug for ModelDomainService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let image = self.read_image.load();
        formatter
            .debug_struct("ModelDomainService")
            .field("model_count", &image.len())
            .field("registry", &"[IMMUTABLE MODEL REGISTRY]")
            .field("forecasts_configured", &self.forecasts.is_some())
            .field("evaluations", &"[BOUNDED EVALUATION EVIDENCE]")
            .field("lifecycle", &self.lifecycle)
            .finish()
    }
}

#[async_trait]
impl ApplicationDomainService for ModelDomainService {
    fn domain(&self) -> ServiceDomain {
        ServiceDomain::Model
    }

    async fn call(
        &self,
        request: TypedToolRequest,
        context: RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        if request.contract().domain() != ServiceDomain::Model {
            return Err(ServiceError::InvalidRequest);
        }
        let _call = DomainLifecycle::enter(&self.lifecycle, &context)?;
        let forecast_commit = matches!(
            request.name(),
            forecast::GENERATE_FORECAST | outcome_measurement::MEASURE_FORECAST_OUTCOME
        );
        let result = match request.name() {
            GET_METADATA => self.metadata(&request, &context),
            LIST_BUNDLES => self.bundles(&request, &context),
            GET_BUNDLE => self.bundle_detail(&request, &context),
            EVALUATE => self.infer(&request, &context, true),
            PREDICT => self.infer(&request, &context, false),
            outcome_preparation::PREPARE_FORECAST_OUTCOME => {
                self.prepare_forecast_outcome(&request, &context).await
            }
            outcome_measurement::MEASURE_FORECAST_OUTCOME => {
                self.measure_forecast_outcome(&request, &context).await
            }
            forecast::GENERATE_FORECAST => self.generate_forecast(&request, &context).await,
            forecast::GET_FORECAST => self.get_forecast(&request, &context).await,
            "Model.GetForecastChart" => self.get_forecast_chart(&request, &context).await,
            forecast::SELECT_LATEST_VALID_FORECAST => {
                self.select_latest_valid_forecast(&request, &context).await
            }
            forecast::LIST_FORECASTS => self.list_forecasts(&request, &context).await,
            forecast::GET_FORECAST_OUTCOMES => self.get_forecast_outcomes(&request, &context).await,
            _ => Err(ServiceError::NotFound),
        }?;
        if !forecast_commit {
            ensure_request_live(&context, &self.lifecycle)?;
        }
        Ok(result)
    }

    fn begin_shutdown(&self) {
        self.lifecycle.begin_shutdown();
        if let read_image::ModelBundleInventory::Disk(inventory) = &self.read_image.load().registry
        {
            inventory.begin_shutdown();
        }
    }

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        self.lifecycle.finish_shutdown(deadline).await?;
        if let read_image::ModelBundleInventory::Disk(inventory) = &self.read_image.load().registry
        {
            inventory
                .shutdown(deadline)
                .map_err(runtime_service_error)?;
        }
        Ok(())
    }
}

impl Drop for ModelDomainService {
    fn drop(&mut self) {
        self.begin_shutdown();
    }
}

/// Model-domain construction failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ModelDomainServiceError {
    /// Evaluation retention exceeded the process hard bound.
    #[error("model evaluation retention capacity is invalid")]
    EvaluationCapacity,
    /// The model registry could not be read.
    #[error("model registry is unavailable")]
    Registry,
    /// The backend set does not cover every retained registry generation.
    #[error("model backend set is incomplete")]
    IncompleteBackendSet,
    /// Two backends claim the same immutable bundle coordinate.
    #[error("model backend coordinate is duplicated")]
    DuplicateBackend,
    /// A backend's metadata differs from the exact retained bundle.
    #[error("model backend identity differs from its retained bundle")]
    BackendIdentityMismatch,
}

struct ParsedModelInput {
    bundle_id: BundleId,
    bundle_version: NonZeroU64,
    feature_values: Box<[f64]>,
}

impl TryFrom<&Map<String, Value>> for ParsedModelInput {
    type Error = ServiceError;

    fn try_from(input: &Map<String, Value>) -> Result<Self, Self::Error> {
        if input.len() != 3
            || input
                .keys()
                .any(|key| !matches!(key.as_str(), "bundleId" | "bundleVersion" | "featureValues"))
        {
            return Err(ServiceError::InvalidRequest);
        }
        let bundle_id = input
            .get("bundleId")
            .and_then(Value::as_str)
            .ok_or(ServiceError::InvalidRequest)
            .and_then(|value| BundleId::try_new(value).map_err(|_| ServiceError::InvalidRequest))?;
        let bundle_version = input
            .get("bundleVersion")
            .and_then(Value::as_u64)
            .and_then(NonZeroU64::new)
            .ok_or(ServiceError::InvalidRequest)?;
        let feature_values = input
            .get("featureValues")
            .and_then(Value::as_array)
            .ok_or(ServiceError::InvalidRequest)?;
        if feature_values.is_empty()
            || feature_values.len() > market_squawk_modeling::MAX_MODEL_FEATURES
        {
            return Err(ServiceError::InvalidRequest);
        }
        let feature_values = feature_values
            .iter()
            .map(|value| {
                value
                    .as_f64()
                    .filter(|value| value.is_finite())
                    .ok_or(ServiceError::InvalidRequest)
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_boxed_slice();
        Ok(Self {
            bundle_id,
            bundle_version,
            feature_values,
        })
    }
}

struct EvaluationStore {
    records: VecDeque<EvaluationRecord>,
    capacity: NonZeroUsize,
    next_sequence: u64,
}

impl EvaluationStore {
    fn new(capacity: NonZeroUsize) -> Self {
        Self {
            records: VecDeque::with_capacity(capacity.get()),
            capacity,
            next_sequence: 1,
        }
    }

    fn record(
        &mut self,
        context: &RequestContext,
        input: &Map<String, Value>,
        output: &ModelOutput,
    ) -> Result<EvaluationReceipt, ServiceError> {
        let sequence = self.next_sequence;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(ServiceError::ResourceExhausted)?;
        let request_identity = context
            .request_id()
            .canonical_bytes()
            .map_err(|_| ServiceError::Internal)?;
        let input_bytes = serde_json::to_vec(input).map_err(|_| ServiceError::Internal)?;
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/model-evaluation-evidence/v1");
        hash.update(sequence.to_be_bytes());
        hash.update(request_identity);
        hash.update(input_bytes);
        hash.update(output.model_id().as_uuid().as_bytes());
        hash.update(output.bundle_id().as_str().as_bytes());
        hash.update(output.bundle_version().get().to_be_bytes());
        hash.update(output.score().to_bits().to_be_bytes());
        hash.update(output.confidence().to_bits().to_be_bytes());
        hash.update([model_decision_tag(output.decision())]);
        let digest: [u8; 32] = hash.finalize().into();
        if self.records.len() == self.capacity.get() {
            self.records.pop_front();
        }
        self.records.push_back(EvaluationRecord {
            sequence,
            digest,
            model_id: output.model_id(),
            bundle_version: output.bundle_version(),
        });
        Ok(EvaluationReceipt { sequence, digest })
    }
}

struct EvaluationRecord {
    #[allow(
        dead_code,
        reason = "retained evidence is intentionally opaque until a read contract exists"
    )]
    sequence: u64,
    #[allow(
        dead_code,
        reason = "retained evidence is intentionally opaque until a read contract exists"
    )]
    digest: [u8; 32],
    #[allow(
        dead_code,
        reason = "retained evidence is intentionally opaque until a read contract exists"
    )]
    model_id: ModelId,
    #[allow(
        dead_code,
        reason = "retained evidence is intentionally opaque until a read contract exists"
    )]
    bundle_version: NonZeroU64,
}

struct EvaluationReceipt {
    sequence: u64,
    digest: [u8; 32],
}

fn admitted_model_id(arguments: &Map<String, Value>) -> Result<ModelId, ServiceError> {
    arguments
        .get("modelId")
        .and_then(Value::as_str)
        .ok_or(ServiceError::InvalidRequest)
        .and_then(|value| ModelId::from_str(value).map_err(|_| ServiceError::InvalidRequest))
}

fn admitted_forecast_selection(
    arguments: &Map<String, Value>,
) -> Result<(InstrumentId, Timestamp), ServiceError> {
    let instrument_id = arguments
        .get("instrumentId")
        .and_then(Value::as_str)
        .ok_or(ServiceError::InvalidRequest)
        .and_then(|value| {
            InstrumentId::from_str(value).map_err(|_| ServiceError::InvalidRequest)
        })?;
    let as_of = arguments
        .get("asOf")
        .and_then(Value::as_str)
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .and_then(|value| value.timestamp_nanos_opt())
        .map(Timestamp::from_unix_nanos)
        .ok_or(ServiceError::InvalidRequest)?;
    Ok((instrument_id, as_of))
}

fn one_result(
    content: Value,
    request: &TypedToolRequest,
    context: &RequestContext,
) -> Result<TypedToolResult, ServiceError> {
    TypedToolResult::try_new(
        content,
        1,
        ToolResultMetadata::complete_not_applicable(),
        admitted_result_limits(request, context)?,
    )
    .map_err(Into::into)
}

fn latest_valid_forecast_value(
    selected: &forecast::LatestValidForecast,
) -> Result<Value, ServiceError> {
    if let Some(distribution) = selected.selected_distribution()
        && matches!(
            distribution.native_output().output_binding().measurement(),
            ForecastMeasurement::FinancialAmount { .. }
        )
    {
        return selected_financial_forecast_value(selected, distribution);
    }
    let evidence = match selected.price_evidence() {
        forecast::ForecastPriceEvidence::Available(price) => json!({
            "vintageId": digest_value(price.vintage_id()),
            "instrumentId": price.instrument_id().to_string(),
            "outputBinding": available_forecast_output_binding_value(price),
            "model": selected_forecast_model_value(price.model_metadata()),
            "forecastArtifact": forecast_artifact_value(price.forecast_artifact()),
            "freshness": forecast_freshness_value(
                selected.selection_receipt().as_of_unix_nanos(),
                Some(price.observed_through().unix_nanos()),
                price.available_at().unix_nanos(),
                price.created_at().unix_nanos(),
                price.expires_at().unix_nanos(),
            ),
            "points": price
                .points()
                .iter()
                .copied()
                .map(selected_price_point_value)
                .collect::<Vec<_>>(),
            "calibration": price.calibration().map(calibration_evidence_value).transpose()?,
            "coverageEvaluation": price.model_metadata().forecast_calibration()
                .and_then(|artifacts| artifacts.coverage_evaluation(Timestamp::from_unix_nanos(
                    selected.selection_receipt().as_of_unix_nanos(),
                )))
                .map(calibration_coverage_evaluation_value).transpose()?,
        }),
        forecast::ForecastPriceEvidence::Unavailable(unavailable) => json!({
            "vintageId": digest_value(unavailable.vintage_id()),
            "instrumentId": unavailable.instrument_id().to_string(),
            "outputBinding": forecast_output_binding_value(
                selected.model_metadata().output_binding(),
                unavailable.output_binding_identity(),
            ),
            "model": selected_forecast_model_value(selected.model_metadata()),
            "forecastArtifact": forecast_artifact_value(selected.forecast_artifact()),
            "freshness": forecast_freshness_value(
                selected.selection_receipt().as_of_unix_nanos(),
                selected.selection_receipt().selected_observed_through_unix_nanos(),
                selected.selection_receipt().selected_available_at_unix_nanos(),
                selected.selection_receipt().selected_created_at_unix_nanos(),
                selected.selection_receipt().selected_expires_at_unix_nanos(),
            ),
            "reason": forecast_price_unavailable_reason(unavailable.reason()),
        }),
    };
    Ok(json!({
        "status": match selected.price_evidence() {
            forecast::ForecastPriceEvidence::Available(_) => "available",
            forecast::ForecastPriceEvidence::Unavailable(_) => "unavailable",
        },
        "evidence": evidence,
        "selectionReceipt": forecast_selection_receipt_value(selected.selection_receipt()),
    }))
}

fn selected_financial_forecast_value(
    selected: &forecast::LatestValidForecast,
    distribution: &SelectedForecastDistribution,
) -> Result<Value, ServiceError> {
    let output = distribution.native_output();
    let target = output
        .financial_target()
        .ok_or(ServiceError::InvalidResult)?;
    let epoch = distribution
        .serving_binding()
        .financial_epoch()
        .ok_or(ServiceError::InvalidResult)?;
    let calibration = selected
        .model_metadata()
        .forecast_calibration()
        .ok_or(ServiceError::InvalidResult)?;
    if epoch.financial_period() != Some(target) {
        return Err(ServiceError::InvalidResult);
    }
    let input_epoch_json = String::from_utf8(
        epoch
            .canonical_bytes()
            .map_err(|_| ServiceError::InvalidResult)?,
    )
    .map_err(|_| ServiceError::InvalidResult)?;
    let horizon = target
        .target_horizon()
        .map_err(|_| ServiceError::InvalidResult)?;
    let market_squawk_data::DatasetTargetHorizon::FiscalPeriods {
        cadence,
        periods_ahead,
    } = horizon
    else {
        return Err(ServiceError::InvalidResult);
    };
    Ok(json!({
        "status": "available",
        "evidence": {
            "vintageId": digest_value(distribution.vintage_id()),
            "instrumentId": output.instrument_id().to_string(),
            "outputBinding": forecast_output_binding_value(output.output_binding(), output.output_binding().identity()),
            "model": selected_forecast_model_value(selected.model_metadata()),
            "forecastArtifact": forecast_artifact_value(selected.forecast_artifact()),
            "freshness": forecast_freshness_value(
                selected.selection_receipt().as_of_unix_nanos(),
                output.observed_through().map(Timestamp::unix_nanos),
                output.available_at().unix_nanos(),
                output.published_at().unix_nanos(),
                output.expires_at().unix_nanos(),
            ),
            "targetAtUnixNanos": output.target_at().map(|time| time.unix_nanos().to_string()),
            "financialTarget": {
                "cadence": cadence,
                "periodsAhead": periods_ahead.get(),
                "observedOrdinal": target.observed_ordinal(),
                "targetOrdinal": target.target_ordinal(),
                "observedPeriod": target.observed_period(),
                "targetPeriod": target.target_period(),
                "sourceSelectionSha256": encode_hex(target.source_selection_digest().bytes()),
                "identityReceiptSha256": encode_hex(target.identity_receipt_digest().bytes()),
            },
            "calibrationCutoff": model_temporal_coordinate_value(output.calibration_cutoff())?,
            "central": forecast_value(output.central()),
            "distribution": {
                "identitySha256": digest_value(distribution.identity()),
                "nativeOutputSha256": digest_value(output.identity()),
                "residualDistributionSha256": digest_value(distribution.residual_distribution_identity()),
                "points": distribution.points().iter().map(|point| json!({
                    "value": forecast_value(point.value()),
                    "probabilityPpm": point.probability_ppm().get(),
                })).collect::<Vec<_>>(),
                "semantics": "empirical_validation_residual_distribution",
            },
            "financialInput": {
                "inputEpochJson": input_epoch_json,
                "servingFeatureSha256": digest_value(distribution.serving_binding().feature_sha256()),
            },
            "calibrationFit": {
                "method": calibration_method_name(calibration.method()),
                "window": calibration_window_value(calibration.window())?,
                "policyArtifact": {
                    "sha256": digest_value(calibration.policy_hash()),
                    "byteCount": calibration.policy_size_bytes().to_string(),
                },
                "residualArtifact": {
                    "sha256": digest_value(calibration.residuals_hash()),
                    "byteCount": calibration.residuals_size_bytes().to_string(),
                },
                "dependenceAssumptions": calibration.dependence_assumptions(),
                "semantics": "fitted_interval_policy_not_evaluation",
            },
            "coverageEvaluation": calibration.coverage_evaluation(Timestamp::from_unix_nanos(
                selected.selection_receipt().as_of_unix_nanos(),
            )).map(calibration_coverage_evaluation_value).transpose()?,
        },
        "selectionReceipt": forecast_selection_receipt_value(selected.selection_receipt()),
    }))
}

fn available_forecast_output_binding_value(price: &forecast::SelectedPriceForecast) -> Value {
    json!({
        "identitySha256": digest_value(price.output_binding_identity()),
        "measurement": "price",
        "currency": price.currency().as_str(),
        "centralStatistic": match price.central_statistic() {
            ForecastCentralStatistic::ModelEstimatedConditionalMean => "model_estimated_conditional_mean",
            ForecastCentralStatistic::Unavailable => "unavailable",
        },
        "target": "fixed_horizon_terminal",
        "terminalHorizonNanos": price.terminal_horizon_nanos().get().to_string(),
    })
}

fn forecast_output_binding_value(
    binding: &ForecastOutputBinding,
    output_binding_identity: Sha256Digest,
) -> Value {
    let (measurement, currency, financial_amount) = match binding.measurement() {
        ForecastMeasurement::Price { currency } => {
            ("price", Some(currency.as_str().to_owned()), None)
        }
        ForecastMeasurement::FinancialAmount {
            currency,
            role,
            basis,
            share_convention,
        } => (
            "financial_amount",
            Some(currency.as_str().to_owned()),
            Some(json!({
                "role": role, "basis": basis, "shareConvention": share_convention,
            })),
        ),
        ForecastMeasurement::Return => ("return", None, None),
        ForecastMeasurement::Probability => ("probability", None, None),
        ForecastMeasurement::OtherRegression => ("other_regression", None, None),
    };
    let (target, terminal_horizon_nanos, origin_basis) = match binding.target() {
        ForecastTargetMeaning::FixedHorizonTerminal {
            horizon_nanos,
            origin_basis,
        } => (
            "fixed_horizon_terminal",
            Some(horizon_nanos.get().to_string()),
            Some(match origin_basis {
                market_squawk_data::FixedHorizonOriginBasis::ExactEffectiveTimestamp => {
                    "exact_effective_timestamp"
                }
                market_squawk_data::FixedHorizonOriginBasis::CompletedBarClose => {
                    "completed_bar_close"
                }
                market_squawk_data::FixedHorizonOriginBasis::NamedSessionCloseForNominalDailyBar => "named_session_close_for_nominal_daily_bar",
            }),
        ),
        ForecastTargetMeaning::FixedHorizonEvent { horizon_nanos, origin_basis, .. } => (
            "fixed_horizon_event", Some(horizon_nanos.get().to_string()), Some(match origin_basis {
                market_squawk_data::FixedHorizonOriginBasis::ExactEffectiveTimestamp => "exact_effective_timestamp",
                market_squawk_data::FixedHorizonOriginBasis::CompletedBarClose => "completed_bar_close",
                market_squawk_data::FixedHorizonOriginBasis::NamedSessionCloseForNominalDailyBar => "named_session_close_for_nominal_daily_bar",
            })),
        ForecastTargetMeaning::FinancialPeriod { .. } => ("financial_period", None, None),
        ForecastTargetMeaning::Unsupported => ("unsupported", None, None),
    };
    let fiscal_horizon = match binding.target() {
        ForecastTargetMeaning::FinancialPeriod {
            cadence,
            periods_ahead,
        } => Some(json!({
            "cadence": cadence, "periodsAhead": periods_ahead.get(),
        })),
        _ => None,
    };
    let mut value = json!({
        "identitySha256": digest_value(output_binding_identity),
        "measurement": measurement,
        "currency": currency,
        "financialAmount": financial_amount,
        "centralStatistic": match binding.central_statistic() {
            ForecastCentralStatistic::ModelEstimatedConditionalMean => "model_estimated_conditional_mean",
            ForecastCentralStatistic::Unavailable => "unavailable",
        },
        "target": target,
        "terminalHorizonNanos": terminal_horizon_nanos,
        "originBasis": origin_basis,
        "fiscalHorizon": fiscal_horizon,
    });
    if let ForecastTargetMeaning::FixedHorizonEvent {
        horizon_nanos,
        origin_basis,
        event,
    } = binding.target()
    {
        value["event"] = forecast::event_product_value(horizon_nanos, origin_basis, event);
    }
    value
}

fn selected_forecast_model_value(metadata: &ModelMetadata) -> Value {
    json!({
        "modelId": metadata.model_id().to_string(),
        "bundleId": metadata.bundle_id().as_str(),
        "bundleVersion": metadata.bundle_version().get().to_string(),
        "metadataSha256": digest_value(metadata.metadata_hash()),
        "modelArtifactSha256": digest_value(metadata.artifact_hash()),
        "trainingRunSha256": digest_value(metadata.training_run_hash()),
    })
}

fn forecast_artifact_value(artifact: &ArtifactReference) -> Value {
    json!({
        "artifactId": artifact.id(),
        "sha256": artifact.sha256(),
        "byteCount": artifact.byte_count().to_string(),
        "mediaType": artifact.media_type(),
    })
}

fn forecast_freshness_value(
    as_of_unix_nanos: i64,
    observed_through_unix_nanos: Option<i64>,
    available_at_unix_nanos: i64,
    created_at_unix_nanos: i64,
    expires_at_unix_nanos: i64,
) -> Value {
    json!({
        "asOfUnixNanos": as_of_unix_nanos.to_string(),
        "observedThroughUnixNanos": observed_through_unix_nanos.map(|value| value.to_string()),
        "availableAtUnixNanos": available_at_unix_nanos.to_string(),
        "createdAtUnixNanos": created_at_unix_nanos.to_string(),
        "expiresAtUnixNanos": expires_at_unix_nanos.to_string(),
        "availableAtOrBeforeAsOf": available_at_unix_nanos <= as_of_unix_nanos,
        "publishedAtOrBeforeAsOf": created_at_unix_nanos <= as_of_unix_nanos,
        "unexpiredAtAsOf": as_of_unix_nanos < expires_at_unix_nanos,
    })
}

fn selected_price_point_value(point: forecast::SelectedPriceForecastPoint) -> Value {
    let intervals = point.intervals().map(|intervals| {
        vec![
            selected_price_interval_value(5_000, intervals.interval_50()),
            selected_price_interval_value(8_000, intervals.interval_80()),
            selected_price_interval_value(9_500, intervals.interval_95()),
        ]
    });
    json!({
        "targetAtUnixNanos": point.target_at().unix_nanos().to_string(),
        "central": forecast_value(point.central()),
        "coverageIntervals": intervals,
    })
}

fn selected_price_interval_value(
    target_coverage_basis_points: u16,
    interval: forecast::SelectedPriceInterval,
) -> Value {
    json!({
        "targetCoverageBasisPoints": target_coverage_basis_points,
        "lower": forecast_value(interval.lower()),
        "upper": forecast_value(interval.upper()),
        "semantics": "marginal_coverage_interval_not_scenario_probability",
    })
}

fn forecast_value(value: ForecastValue) -> Value {
    json!({
        "mantissa": value.mantissa().to_string(),
        "scale": value.scale(),
    })
}

fn model_temporal_coordinate_value(
    coordinate: &ResearchTemporalCoordinate,
) -> Result<Value, ServiceError> {
    if let Some(timestamp) = coordinate.exact_timestamp() {
        Ok(json!({"kind": "exact_time", "unixNanos": timestamp.unix_nanos().to_string()}))
    } else if let Some(date) = coordinate.calendar_date_value() {
        Ok(json!({"kind": "fiscal_date", "date": date}))
    } else {
        Err(ServiceError::InvalidResult)
    }
}

fn model_period_value(
    start: Option<Timestamp>,
    end: Option<Timestamp>,
    fiscal_bounds: Option<[market_squawk_domain::CalendarDate; 2]>,
) -> Result<Value, ServiceError> {
    match (start, end, fiscal_bounds) {
        (None, None, Some([start, end])) if start < end => {
            Ok(json!({"kind": "fiscal_dates", "start": start, "end": end}))
        }
        (Some(start), Some(end), None) if start < end => Ok(json!({
            "kind": "exact_time",
            "startUnixNanos": start.unix_nanos().to_string(),
            "endUnixNanos": end.unix_nanos().to_string(),
        })),
        _ => Err(ServiceError::InvalidResult),
    }
}

fn calibration_window_value(
    window: market_squawk_modeling::CalibrationWindow,
) -> Result<Value, ServiceError> {
    Ok(json!({
        "period": model_period_value(window.start(), window.end(), window.fiscal_bounds())?,
        "observationCount": window.observations().get().to_string(),
    }))
}

const fn calibration_method_name(method: CalibrationMethod) -> &'static str {
    match method {
        CalibrationMethod::MapieEnbpi => "mapie_enbpi",
        CalibrationMethod::MapieAci => "mapie_aci",
        CalibrationMethod::ResidualQuantile => "residual_quantile",
    }
}

fn calibration_evidence_value(calibration: &CalibrationEvidence) -> Result<Value, ServiceError> {
    Ok(json!({
        "identitySha256": digest_value(calibration.identity()),
        "method": calibration_method_name(calibration.method()),
        "window": calibration_window_value(calibration.window())?,
        "policyArtifact": {
            "sha256": digest_value(calibration.policy_hash()),
            "byteCount": calibration.policy_size_bytes().to_string(),
        },
        "residualArtifact": {
            "sha256": digest_value(calibration.residuals_hash()),
            "byteCount": calibration.residuals_size_bytes().to_string(),
        },
        "coverageBands": calibration
            .bands()
            .iter()
            .map(|band| json!({
                "targetCoverageBasisPoints": band.coverage().basis_points(),
                "lowerOffsetIeee754Hex": format!("{:016x}", band.lower_offset().to_bits()),
                "upperOffsetIeee754Hex": format!("{:016x}", band.upper_offset().to_bits()),
            }))
            .collect::<Vec<_>>(),
        "dependenceAssumptions": calibration.dependence_assumptions(),
        "semantics": "fitted_interval_policy_not_evaluation",
    }))
}

fn calibration_coverage_evaluation_value(
    evaluation: &market_squawk_modeling::CalibrationCoverageEvaluation,
) -> Result<Value, ServiceError> {
    let coverage = [5_000_u16, 8_000, 9_500]
        .into_iter()
        .zip(evaluation.realized())
        .map(|(coverage, realized)| {
            json!({
                "targetCoverageBasisPoints": coverage,
                "realizedCoveredCount": realized.covered().to_string(),
                "realizedObservationCount": realized.total().get().to_string(),
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "window": calibration_window_value(evaluation.window())?,
        "coverage": coverage,
        "semantics": "held_out_empirical_coverage_not_profit_probability",
    }))
}

fn forecast_selection_receipt_value(receipt: &forecast::ForecastSelectionReceipt) -> Value {
    json!({
        "schema": "market-squawk/forecast-selection-receipt/v5",
        "policyRevision": receipt.policy_revision(),
        "selectionOrder": receipt.selection_order().as_str(),
        "qualification": forecast_selection_qualification_value(receipt.qualification()),
        "instrumentId": receipt.instrument_id().to_string(),
        "asOfUnixNanos": receipt.as_of_unix_nanos().to_string(),
        "consideredVintageCount": receipt.considered_vintage_count(),
        "inventoryVintageCount": receipt.inventory_vintage_count(),
        "eligibleVintageCount": receipt.eligible_vintage_count(),
        "competingEligibleVintageCount": receipt.competing_eligible_vintage_count(),
        "selectionComplete": receipt.selection_complete(),
        "selectedVintageId": receipt.selected_vintage_id(),
        "selectedCreatedAtUnixNanos": receipt.selected_created_at_unix_nanos().to_string(),
        "selectedObservedThroughUnixNanos": receipt
            .selected_observed_through_unix_nanos()
            .map(|value| value.to_string()),
        "selectedAvailableAtUnixNanos": receipt.selected_available_at_unix_nanos().to_string(),
        "selectedExpiresAtUnixNanos": receipt.selected_expires_at_unix_nanos().to_string(),
        "selectedTerminalTargetAtUnixNanos": receipt
            .selected_terminal_target_at_unix_nanos()
            .map(|value| value.to_string()),
        "receiptDigestSha256": encode_hex(receipt.receipt_digest().bytes()),
    })
}

fn forecast_selection_qualification_value(
    qualification: forecast::ForecastSelectionQualification,
) -> Value {
    match qualification {
        forecast::ForecastSelectionQualification::AnyValid => json!({
            "kind": "any_valid",
        }),
        forecast::ForecastSelectionQualification::ExactCalibratedConditionalMeanPrice {
            horizon_nanos,
        } => json!({
            "kind": "exact_calibrated_conditional_mean_price",
            "horizonNanos": horizon_nanos.get().to_string(),
        }),
    }
}

const fn forecast_price_unavailable_reason(
    reason: forecast::ForecastPriceUnavailableReason,
) -> &'static str {
    match reason {
        forecast::ForecastPriceUnavailableReason::FinancialAmountMeasurement => {
            "financial_amount_measurement"
        }
        forecast::ForecastPriceUnavailableReason::ReturnMeasurement => "return_measurement",
        forecast::ForecastPriceUnavailableReason::ProbabilityMeasurement => {
            "probability_measurement"
        }
        forecast::ForecastPriceUnavailableReason::OtherRegressionMeasurement => {
            "other_regression_measurement"
        }
        forecast::ForecastPriceUnavailableReason::TerminalHorizonUnavailable => {
            "terminal_horizon_unavailable"
        }
        forecast::ForecastPriceUnavailableReason::CentralStatisticUnavailable => {
            "central_statistic_unavailable"
        }
    }
}

pub(crate) fn map_forecast_selection_error(error: ForecastApplicationError) -> ServiceError {
    match error {
        ForecastApplicationError::CurrentInputRead(error) => error,
        ForecastApplicationError::InvalidLimits | ForecastApplicationError::InvalidRecord => {
            ServiceError::InvalidRequest
        }
        ForecastApplicationError::NotFound => ServiceError::NotFound,
        ForecastApplicationError::Capacity => ServiceError::ResourceExhausted,
        ForecastApplicationError::Artifact(ArtifactError::Cancelled) => ServiceError::Cancelled,
        ForecastApplicationError::Artifact(ArtifactError::DeadlineExceeded) => {
            ServiceError::DeadlineExceeded
        }
        ForecastApplicationError::Artifact(ArtifactError::ReadLimitExceeded) => {
            ServiceError::ResourceExhausted
        }
        ForecastApplicationError::Artifact(ArtifactError::NotFound) => ServiceError::NotFound,
        ForecastApplicationError::Artifact(ArtifactError::InvalidPublication)
        | ForecastApplicationError::Artifact(ArtifactError::InvalidReference) => {
            ServiceError::InvalidResult
        }
        ForecastApplicationError::Artifact(ArtifactError::Unavailable)
        | ForecastApplicationError::Inventory(_)
        | ForecastApplicationError::Unavailable => ServiceError::Unavailable,
        ForecastApplicationError::CorruptIndex => ServiceError::Internal,
    }
}

fn model_coordinate(metadata: &ModelMetadata) -> (String, String, u64) {
    (
        metadata.model_id().to_string(),
        metadata.bundle_id().as_str().to_owned(),
        metadata.bundle_version().get(),
    )
}

/// Closed product evidence state derived from one admitted model bundle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ForecastModelEvidenceState {
    Sufficient,
    Limited,
    Unavailable,
}

impl ForecastModelEvidenceState {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Sufficient => "sufficient",
            Self::Limited => "limited",
            Self::Unavailable => "unavailable",
        }
    }

    fn decode(value: &str) -> Option<Self> {
        match value {
            "sufficient" => Some(Self::Sufficient),
            "limited" => Some(Self::Limited),
            "unavailable" => Some(Self::Unavailable),
            _ => None,
        }
    }
}

/// Closed calibration state kept separate from the overall model-evidence gate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ForecastModelCalibrationState {
    Calibrated,
    Limited,
    Unavailable,
}

impl ForecastModelCalibrationState {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Calibrated => "calibrated",
            Self::Limited => "limited",
            Self::Unavailable => "unavailable",
        }
    }

    fn decode(value: &str) -> Option<Self> {
        match value {
            "calibrated" => Some(Self::Calibrated),
            "limited" => Some(Self::Limited),
            "unavailable" => Some(Self::Unavailable),
            _ => None,
        }
    }
}

/// Restart-bindable product projection of the same evidence gate used by Model.ListBundles.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ForecastModelEvidenceProjection {
    model_token: uuid::Uuid,
    selected_horizon: Option<ForecastHorizon>,
    overall: ForecastModelEvidenceState,
    pit_inputs: ForecastModelEvidenceState,
    out_of_sample: ForecastModelEvidenceState,
    horizon_alignment: ForecastModelEvidenceState,
    calibration: ForecastModelCalibrationState,
    interpretation: Box<str>,
}

impl ForecastModelEvidenceProjection {
    pub(crate) fn try_from_product_fields_for_horizon(
        model_token: uuid::Uuid,
        selected_horizon: ForecastHorizon,
        overall: &str,
        pit_inputs: &str,
        out_of_sample: &str,
        horizon_alignment: &str,
        calibration: &str,
        interpretation: &str,
    ) -> Option<Self> {
        Self::try_from_product_fields_inner(
            model_token,
            Some(selected_horizon),
            overall,
            pit_inputs,
            out_of_sample,
            horizon_alignment,
            calibration,
            interpretation,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the closed evidence dimensions and optional selected horizon remain explicit"
    )]
    fn try_from_product_fields_inner(
        model_token: uuid::Uuid,
        selected_horizon: Option<ForecastHorizon>,
        overall: &str,
        pit_inputs: &str,
        out_of_sample: &str,
        horizon_alignment: &str,
        calibration: &str,
        interpretation: &str,
    ) -> Option<Self> {
        let overall = ForecastModelEvidenceState::decode(overall)?;
        let pit_inputs = ForecastModelEvidenceState::decode(pit_inputs)?;
        let out_of_sample = ForecastModelEvidenceState::decode(out_of_sample)?;
        let horizon_alignment = ForecastModelEvidenceState::decode(horizon_alignment)?;
        let calibration = ForecastModelCalibrationState::decode(calibration)?;
        let expected_interpretation =
            model_evidence_interpretation(overall, horizon_alignment, selected_horizon.is_some());
        let core_sufficient = pit_inputs == ForecastModelEvidenceState::Sufficient
            && out_of_sample == ForecastModelEvidenceState::Sufficient
            && horizon_alignment == ForecastModelEvidenceState::Sufficient;
        if model_token.is_nil()
            || interpretation != expected_interpretation
            || (overall == ForecastModelEvidenceState::Sufficient && !core_sufficient)
            || (overall == ForecastModelEvidenceState::Limited && core_sufficient)
        {
            return None;
        }
        Some(Self {
            model_token,
            selected_horizon,
            overall,
            pit_inputs,
            out_of_sample,
            horizon_alignment,
            calibration,
            interpretation: expected_interpretation.into(),
        })
    }

    pub(crate) fn model_token(&self) -> uuid::Uuid {
        self.model_token
    }

    pub(crate) const fn selected_horizon(&self) -> Option<ForecastHorizon> {
        self.selected_horizon
    }

    pub(crate) const fn overall(&self) -> ForecastModelEvidenceState {
        self.overall
    }

    pub(crate) const fn pit_inputs(&self) -> ForecastModelEvidenceState {
        self.pit_inputs
    }

    pub(crate) const fn out_of_sample(&self) -> ForecastModelEvidenceState {
        self.out_of_sample
    }

    pub(crate) const fn horizon_alignment(&self) -> ForecastModelEvidenceState {
        self.horizon_alignment
    }

    pub(crate) const fn calibration(&self) -> ForecastModelCalibrationState {
        self.calibration
    }

    pub(crate) fn interpretation(&self) -> &str {
        &self.interpretation
    }

    pub(crate) fn product_value(&self) -> Value {
        json!({
            "modelToken": self.model_token,
            "overall": self.overall.as_str(),
            "pitInputs": self.pit_inputs.as_str(),
            "outOfSample": self.out_of_sample.as_str(),
            "horizonAlignment": self.horizon_alignment.as_str(),
            "calibration": self.calibration.as_str(),
            "interpretation": self.interpretation,
        })
    }
}

/// Derives the single forecast-product evidence projection from the admitted bundle authority.
pub(crate) fn forecast_model_evidence_projection(
    bundle: &ModelBundle,
) -> Result<ForecastModelEvidenceProjection, ServiceError> {
    forecast_model_evidence_projection_inner(bundle, None)
}

/// Derives evidence for one exact selected forecast horizon and policy coordinate.
pub(crate) fn forecast_model_evidence_projection_for_horizon(
    bundle: &ModelBundle,
    selected_horizon: ForecastHorizon,
) -> Result<ForecastModelEvidenceProjection, ServiceError> {
    forecast_model_evidence_projection_inner(bundle, Some(selected_horizon))
}

fn forecast_model_evidence_projection_inner(
    bundle: &ModelBundle,
    selected_horizon: Option<ForecastHorizon>,
) -> Result<ForecastModelEvidenceProjection, ServiceError> {
    forecast_model_evidence_projection_parts(
        bundle.metadata(),
        bundle.training_run_bytes(),
        bundle.forecast_residual_distribution().is_some(),
        selected_horizon,
    )
}

fn forecast_model_evidence_projection_parts(
    metadata: &ModelMetadata,
    training_run_bytes: &[u8],
    residual_distribution_available: bool,
    selected_horizon: Option<ForecastHorizon>,
) -> Result<ForecastModelEvidenceProjection, ServiceError> {
    let training = serde_json::from_slice::<TrainingRunEvidenceWire>(training_run_bytes)
        .map_err(|_error| ServiceError::InvalidResult)?;
    let split_counts = &training.trial.split_counts;
    let forecast = training.trial.forecast.as_ref();
    let out_of_sample_observations = split_counts.test;
    let selected_step_matches_output =
        selected_horizon.is_none_or(|selected| match metadata.output_binding().target() {
            ForecastTargetMeaning::FixedHorizonTerminal { horizon_nanos, .. }
            | ForecastTargetMeaning::FixedHorizonEvent { horizon_nanos, .. } => {
                Some(horizon_nanos) == selected.step_nanos()
            }
            ForecastTargetMeaning::FinancialPeriod {
                cadence,
                periods_ahead,
            } => selected.fiscal_periods() == Some((cadence, periods_ahead)),
            ForecastTargetMeaning::Unsupported => false,
        });
    let direct_terminal =
        direct_terminal_evidence_parts(metadata, residual_distribution_available, forecast);
    let event_evidence = matches!(
        metadata.output_binding().target(),
        ForecastTargetMeaning::FixedHorizonEvent { .. }
    ) && metadata.probability_calibration().is_some();
    let horizon_alignment = if direct_terminal || event_evidence {
        if selected_step_matches_output
            && selected_horizon.is_none_or(|selected| selected.points().get() == 1)
        {
            ForecastModelEvidenceState::Sufficient
        } else {
            ForecastModelEvidenceState::Limited
        }
    } else if matches!(
        metadata.output_binding().target(),
        ForecastTargetMeaning::FinancialPeriod { .. }
    ) {
        // An Advanced row-index schedule does not prove a native fiscal target.
        ForecastModelEvidenceState::Unavailable
    } else {
        match (forecast, selected_horizon) {
            (Some(evidence), Some(selected))
                if evidence.rolling_splits > 0
                    && selected_step_matches_output
                    && evidence
                        .horizons
                        .binary_search(&u32::from(selected.points().get()))
                        .is_ok() =>
            {
                ForecastModelEvidenceState::Sufficient
            }
            (Some(evidence), Some(_))
                if evidence.rolling_splits > 0 && !evidence.horizons.is_empty() =>
            {
                ForecastModelEvidenceState::Limited
            }
            (Some(evidence), None)
                if evidence.rolling_splits > 0 && !evidence.horizons.is_empty() =>
            {
                ForecastModelEvidenceState::Sufficient
            }
            (Some(_), _) => ForecastModelEvidenceState::Limited,
            (None, _) => ForecastModelEvidenceState::Unavailable,
        }
    };
    let point_in_time_bound = metadata
        .training_period()
        .ends_by(metadata.dataset().selection_as_of());
    let forecast_evidence_complete = horizon_alignment == ForecastModelEvidenceState::Sufficient;
    let held_out_evidence_complete = split_counts.validation > 0
        && out_of_sample_observations > 0
        && !metadata.validation_metrics().is_empty();
    let overall = if held_out_evidence_complete && point_in_time_bound && forecast_evidence_complete
    {
        ForecastModelEvidenceState::Sufficient
    } else if split_counts.train > 0 {
        ForecastModelEvidenceState::Limited
    } else {
        ForecastModelEvidenceState::Unavailable
    };
    let pit_inputs = if point_in_time_bound {
        ForecastModelEvidenceState::Sufficient
    } else {
        ForecastModelEvidenceState::Unavailable
    };
    let out_of_sample = if held_out_evidence_complete {
        ForecastModelEvidenceState::Sufficient
    } else if split_counts.validation > 0
        || out_of_sample_observations > 0
        || !metadata.validation_metrics().is_empty()
    {
        ForecastModelEvidenceState::Limited
    } else {
        ForecastModelEvidenceState::Unavailable
    };
    let calibration = if forecast.is_none() && !direct_terminal && !event_evidence {
        ForecastModelCalibrationState::Unavailable
    } else if metadata.forecast_calibration().is_some() || event_evidence {
        ForecastModelCalibrationState::Calibrated
    } else {
        ForecastModelCalibrationState::Limited
    };
    let interpretation: Box<str> =
        model_evidence_interpretation(overall, horizon_alignment, selected_horizon.is_some())
            .into();
    Ok(ForecastModelEvidenceProjection {
        model_token: opaque_product_token(
            b"market-squawk/product-model/v1\0",
            &[
                metadata.model_id().as_uuid().as_bytes(),
                metadata.bundle_id().as_str().as_bytes(),
                &metadata.bundle_version().get().to_be_bytes(),
            ],
        ),
        selected_horizon,
        overall,
        pit_inputs,
        out_of_sample,
        horizon_alignment,
        calibration,
        interpretation,
    })
}

fn direct_terminal_evidence(
    bundle: &ModelBundle,
    forecast: Option<&ForecastScheduleEvidenceWire>,
) -> bool {
    direct_terminal_evidence_parts(
        bundle.metadata(),
        bundle.forecast_residual_distribution().is_some(),
        forecast,
    )
}

fn direct_terminal_evidence_parts(
    metadata: &ModelMetadata,
    residual_distribution_available: bool,
    forecast: Option<&ForecastScheduleEvidenceWire>,
) -> bool {
    let binding = metadata.output_binding();
    let native_financial = matches!(
        binding.measurement(),
        ForecastMeasurement::FinancialAmount { .. }
    ) && matches!(
        binding.target(),
        ForecastTargetMeaning::FinancialPeriod { .. }
    ) && binding.central_statistic()
        == ForecastCentralStatistic::ModelEstimatedConditionalMean
        && residual_distribution_available;
    forecast.is_none()
        && (binding.expected_arithmetic_return_horizon_nanos().is_some() || native_financial)
        && binding.estimator()
            == market_squawk_modeling::ForecastEstimatorProfile::SealedDirectLeastSquaresV1
        && metadata.forecast_calibration().is_some()
}

const fn model_evidence_interpretation(
    state: ForecastModelEvidenceState,
    horizon_alignment: ForecastModelEvidenceState,
    selection_bound: bool,
) -> &'static str {
    match (state, horizon_alignment, selection_bound) {
        (ForecastModelEvidenceState::Sufficient, ForecastModelEvidenceState::Sufficient, _) => {
            "The model retains its source-knowledge cutoff and has held-out, horizon-matched evaluation. Calibration is shown separately and does not mean the forecast is certain."
        }
        (ForecastModelEvidenceState::Limited, ForecastModelEvidenceState::Limited, true) => {
            "The model's retained evaluation does not match the selected forecast horizon and policy. Use this forecast only as supporting research; Market Squawk suggests no action when required evidence is missing."
        }
        (ForecastModelEvidenceState::Limited, ForecastModelEvidenceState::Unavailable, true) => {
            "Retained horizon-matched evaluation is unavailable for the selected forecast horizon. Market Squawk suggests no action."
        }
        (ForecastModelEvidenceState::Limited, _, _) => {
            "Some required model evidence is limited or unavailable. Use this forecast only as supporting research, and take no action when required evidence is missing."
        }
        (ForecastModelEvidenceState::Unavailable, _, _) => {
            "The available model evidence cannot support this forecast. Market Squawk suggests no action."
        }
        (ForecastModelEvidenceState::Sufficient, _, _) => {
            "The available model evidence cannot support this forecast. Market Squawk suggests no action."
        }
    }
}

fn product_model_summary(bundle: &ModelBundle) -> Result<Value, ServiceError> {
    let evidence = forecast_model_evidence_projection(bundle)?;
    Ok(
        json!({"modelToken": evidence.model_token(), "label": bundle.metadata().label().name(), "evidenceState": evidence.overall().as_str()}),
    )
}

pub(crate) fn runtime_service_error(error: runtime::ProductionModelRuntimeError) -> ServiceError {
    match error {
        runtime::ProductionModelRuntimeError::InvalidAdmission => ServiceError::InvalidRequest,
        runtime::ProductionModelRuntimeError::ValidationDeadline
        | runtime::ProductionModelRuntimeError::TrainingEnvironment(
            market_squawk_modeling::TrainingEnvironmentError::DeadlineExceeded,
        ) => ServiceError::DeadlineExceeded,
        runtime::ProductionModelRuntimeError::TrainingEnvironment(
            market_squawk_modeling::TrainingEnvironmentError::Cancelled,
        ) => ServiceError::Cancelled,
        runtime::ProductionModelRuntimeError::Admission(
            market_squawk_modeling::ModelAdmissionError::Dataset(
                market_squawk_data::PythonDatasetCatalogError::Cancelled,
            ),
        ) => ServiceError::Cancelled,
        runtime::ProductionModelRuntimeError::ResourceExhausted => ServiceError::ResourceExhausted,
        _ => ServiceError::Unavailable,
    }
}

fn product_model_evidence(
    bundle: &market_squawk_modeling::ModelSelectionMetadata,
) -> Result<Value, ServiceError> {
    let metadata = bundle.metadata();
    let product_evidence = forecast_model_evidence_projection_parts(
        metadata,
        bundle.training_run_bytes(),
        bundle.residual_distribution_available(),
        None,
    )?;
    let training = serde_json::from_slice::<TrainingRunEvidenceWire>(bundle.training_run_bytes())
        .map_err(|_error| ServiceError::InvalidResult)?;
    let split_counts = &training.trial.split_counts;
    let forecast = training.trial.forecast.as_ref();
    let direct_terminal = direct_terminal_evidence_parts(
        metadata,
        bundle.residual_distribution_available(),
        forecast,
    );
    let event_evidence = matches!(
        metadata.output_binding().target(),
        ForecastTargetMeaning::FixedHorizonEvent { .. }
    ) && metadata.probability_calibration().is_some();
    let out_of_sample_observations = split_counts.test;
    let rolling_out_of_sample_folds = forecast.map_or(0, |evidence| evidence.rolling_splits);
    let evaluated_horizons = forecast
        .map_or(usize::from(direct_terminal || event_evidence), |evidence| {
            evidence.horizons.len()
        });
    let point_in_time_bound = metadata
        .training_period()
        .ends_by(metadata.dataset().selection_as_of());
    let forecast_evidence_complete =
        product_evidence.horizon_alignment() == ForecastModelEvidenceState::Sufficient;
    let mut limitations = metadata
        .limitations()
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    if out_of_sample_observations == 0 {
        limitations.push(
            "No held-out observations are retained, so the model cannot support an investment action."
                .to_owned(),
        );
    }
    if !point_in_time_bound {
        limitations.push(
            "The retained training evidence does not prove a point-in-time cutoff after the observation window."
                .to_owned(),
        );
    }
    if !forecast_evidence_complete {
        limitations.push(
            "Rolling out-of-sample evidence is incomplete for the evaluated forecast horizons."
                .to_owned(),
        );
    }
    let validation = metadata
        .validation_metrics()
        .iter()
        .map(|metric| {
            Ok(json!({
                "label": validation_metric_label(metric.name()),
                "value": finite_decimal(metric.value())?,
                "interpretation": validation_metric_interpretation(metric.name()),
            }))
        })
        .collect::<Result<Vec<_>, ServiceError>>()?;
    let coverage = vec![
        json!({
            "label": "Source-knowledge training cutoff",
            "state": if point_in_time_bound { "evaluated" } else { "unavailable" },
            "interpretation": if point_in_time_bound {
                "The retained selection cutoff is at or after the complete training observation window."
            } else {
                "The available history does not prove that only information known at the time was used."
            }
        }),
        json!({
            "label": "Held-out evaluation",
            "state": if out_of_sample_observations > 0 { "evaluated" } else { "unavailable" },
            "interpretation": if out_of_sample_observations > 0 {
                "The model was evaluated on observations that were not used to fit it."
            } else {
                "No held-out observations are retained for this model."
            }
        }),
        json!({
            "label": if direct_terminal || event_evidence { "Target-matched held-out evaluation" } else { "Horizon-aligned rolling evaluation" },
            "state": if (direct_terminal || event_evidence) && forecast_evidence_complete {
                "evaluated"
            } else if forecast.is_none() {
                "limited"
            } else if forecast_evidence_complete {
                "evaluated"
            } else {
                "unavailable"
            },
            "interpretation": if (direct_terminal || event_evidence) && forecast_evidence_complete {
                "The admitted direct model retains held-out evaluation for its exact output target. No rolling folds are inferred."
            } else if forecast.is_none() {
                "This model is not admitted as a forecast model, so no forecast-horizon evaluation is claimed."
            } else if forecast_evidence_complete {
                "The model was evaluated across rolling historical periods for these forecast horizons."
            } else {
                "Complete rolling historical evidence is unavailable for these forecast horizons."
            }
        }),
    ];
    Ok(json!({
        "modelToken": product_evidence.model_token(),
        "label": metadata.label().name(),
        "objective": match metadata.output_semantics() {
            market_squawk_modeling::ModelOutputSemantics::Regression => "numeric_outcome",
            market_squawk_modeling::ModelOutputSemantics::BinaryProbability => "likelihood",
        },
        "intendedUse": metadata.intended_use(),
        "evidenceState": product_evidence.overall().as_str(),
        "training": {
            "period": model_period_value(metadata.training_period().start(), metadata.training_period().end(), metadata.training_period().fiscal_bounds())?,
            "studyBasis": metadata.dataset().study_policy().map(|policy| policy.basis()),
            "availableAtUnixNanos": metadata.dataset().selection_as_of().unix_nanos().to_string(),
            "trainingObservations": split_counts.train,
            "validationObservations": split_counts.validation,
            "outOfSampleObservations": out_of_sample_observations,
            "rollingOutOfSampleFolds": rolling_out_of_sample_folds,
            "evaluatedHorizons": evaluated_horizons,
        },
        "validation": validation,
        "coverage": coverage,
        "limitations": limitations,
        "unavailableBehavior": "no_action",
        "analysisOnly": true,
    }))
}

pub(crate) fn product_model_activity(view: &JobView) -> Option<Value> {
    let label = match view.kind().as_str() {
        "model.training.v1" => "Model training",
        "model.forecast-generation.v1" => "Forecast generation",
        _ => return None,
    };
    let (state, _status_message) = product_activity_state(view);
    let job_id = view.job_id().as_uuid();
    let generation = view.generation().get().to_be_bytes();
    Some(json!({
        "activityToken": opaque_product_token(
            b"market-squawk/product-model-activity/v1\0",
            &[job_id.as_bytes(), &generation],
        ),
        "label": label,
        "state": state,
        "progressPercent": product_progress_percent(view),
        "updatedAtUnixNanos": view.updated_at().unix_nanos().to_string(),
    }))
}

fn finite_decimal(value: f64) -> Result<String, ServiceError> {
    Decimal::from_f64_retain(value)
        .map(|value| value.normalize().to_string())
        .ok_or(ServiceError::InvalidResult)
}

const fn validation_metric_label(name: ValidationMetricName) -> &'static str {
    match name {
        ValidationMetricName::MeanSquaredError => "Mean squared error",
        ValidationMetricName::Accuracy => "Accuracy",
        ValidationMetricName::LogLoss => "Log loss",
        ValidationMetricName::AreaUnderRoc => "Area under ROC curve",
    }
}

const fn validation_metric_interpretation(name: ValidationMetricName) -> &'static str {
    match name {
        ValidationMetricName::MeanSquaredError => {
            "Average squared validation error; lower values are better within the same target scale."
        }
        ValidationMetricName::Accuracy => {
            "Share of validation classifications that matched the observed class."
        }
        ValidationMetricName::LogLoss => {
            "Probability error that penalizes confident wrong classifications; lower values are better."
        }
        ValidationMetricName::AreaUnderRoc => {
            "Validation ranking discrimination across decision thresholds; it is not profit confidence."
        }
    }
}

fn bundle_coordinate(metadata: &ModelMetadata) -> (&str, u64) {
    (
        metadata.bundle_id().as_str(),
        metadata.bundle_version().get(),
    )
}

fn bundle_summary(metadata: &ModelMetadata) -> Value {
    json!({
        "modelId": metadata.model_id().to_string(),
        "bundleId": metadata.bundle_id().as_str(),
        "bundleVersion": metadata.bundle_version().get(),
        "metadataHash": digest_value(metadata.metadata_hash()),
        "artifactHash": digest_value(metadata.artifact_hash()),
        "format": model_format_name(metadata.format()),
        "formatVersion": metadata.format_version(),
        "trainingDataset": manifest_value(metadata.dataset().manifest()),
        "fallbackBehavior": {
            "decision": "no_action",
            "reason": metadata.fallback_reason()
        }
    })
}

fn model_metadata_value(
    bundle: &ModelBundle,
    runtime_health: Value,
) -> Result<Value, ServiceError> {
    let metadata = bundle.metadata();
    let thresholds = metadata.decision_thresholds();
    let dataset = metadata.dataset();
    let features = metadata
        .features()
        .iter()
        .map(|feature| {
            let normalizer = match feature.normalizer() {
                FeatureNormalizer::Identity => json!({"kind": "identity"}),
                FeatureNormalizer::Standard { mean, scale } => {
                    json!({"kind": "standard", "mean": mean, "scale": scale})
                }
            };
            json!({
                "name": feature.key().name(),
                "version": feature.key().version().get(),
                "inputSchemaDigest": encode_hex(feature.input_schema_digest().as_bytes()),
                "semanticDigest": encode_hex(feature.semantic_digest().as_bytes()),
                "normalizer": normalizer
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "modelId": metadata.model_id().to_string(),
        "bundleId": metadata.bundle_id().as_str(),
        "bundleVersion": metadata.bundle_version().get(),
        "metadataHash": digest_value(metadata.metadata_hash()),
        "artifactHash": digest_value(metadata.artifact_hash()),
        "trainingRunHash": digest_value(metadata.training_run_hash()),
        "format": model_format_name(metadata.format()),
        "formatVersion": metadata.format_version(),
        "features": features,
        "trainingDataset": training_dataset_value(dataset),
        "outputBinding": forecast_output_binding_value(metadata.output_binding(), metadata.output_binding().identity()),
        "universeId": metadata.universe_id().as_str(),
        "trainingPeriod": model_period_value(metadata.training_period().start(), metadata.training_period().end(), metadata.training_period().fiscal_bounds())?,
        "label": {
            "name": metadata.label().name(),
            "version": metadata.label().version().get(),
            "kind": component_kind_name(metadata.label().kind())
        },
        "trainingCodeRevision": metadata.training_code_revision(),
        "trainingEnvironmentHash": digest_value(metadata.training_environment_hash()),
        "validationMetrics": validation_metrics_value(metadata),
        "decisionThresholds": {
            "negativeMaximum": thresholds.negative_max(),
            "positiveMinimum": thresholds.positive_min(),
            "minimumConfidence": thresholds.minimum_confidence()
        },
        "intendedUse": metadata.intended_use(),
        "limitations": metadata.limitations(),
        "fallbackBehavior": {
            "decision": "no_action",
            "reason": metadata.fallback_reason()
        },
        "admissionEvidence": admission_evidence_value(metadata),
        "runtimeHealth": runtime_health,
        "trainingEvidence": training_evidence_value(bundle)?
    }))
}

fn admission_evidence_value(metadata: &ModelMetadata) -> Value {
    json!({
        "status": "admitted",
        "authority": "rust_verified_durable_registry",
        "metadataHash": digest_value(metadata.metadata_hash()),
        "artifactHash": digest_value(metadata.artifact_hash()),
        "trainingRunHash": digest_value(metadata.training_run_hash()),
        "rejectionPolicy": "A candidate that fails verification is never published into the registry or backend read image.",
        "failureBehavior": "no_action"
    })
}

fn runtime_health_value(backend_generations: usize, registry_generations: usize) -> Value {
    json!({
        "status": "ready",
        "probe": "registry_backend_identity_match",
        "backendGenerations": backend_generations,
        "registryGenerations": registry_generations,
        "failureBehavior": "unavailable inference returns no action"
    })
}

fn training_evidence_value(bundle: &ModelBundle) -> Result<Value, ServiceError> {
    let wire: TrainingRunEvidenceWire = serde_json::from_slice(bundle.training_run_bytes())
        .map_err(|_| ServiceError::InvalidResult)?;
    let direct_terminal = direct_terminal_evidence(bundle, wire.trial.forecast.as_ref());
    let event_evidence = bundle.metadata().probability_calibration().is_some();
    let splits = wire.trial.split_counts;
    let forecast = wire.trial.forecast.map(|forecast| {
        json!({
            "strategy": forecast.strategy,
            "horizons": forecast.horizons,
            "observedCutoffUnixNanos": forecast.observed_cutoff_unix_nanos.to_string(),
            "rollingSplits": forecast.rolling_splits,
            "selectionHash": forecast.selection_sha256
        })
    });
    let horizon_status = if forecast.is_some() || direct_terminal || event_evidence {
        "recorded"
    } else {
        "not_applicable"
    };
    Ok(json!({
        "schemaVersion": wire.schema_version,
        "trialHash": wire.trial_sha256,
        "seed": wire.trial.seed,
        "missingPolicy": wire.trial.missing_policy,
        "splits": {
            "train": splits.train,
            "validation": splits.validation,
            "test": splits.test,
            "splitHash": wire.trial.split_sha256
        },
        "forecastSchedule": forecast,
        "directTarget": (direct_terminal || event_evidence).then(|| forecast_output_binding_value(
            bundle.metadata().output_binding(), bundle.metadata().output_binding().identity(),
        )),
        "cohortEvidence": [
            {
                "dimension": "horizon",
                "status": horizon_status,
                "reason": if direct_terminal || event_evidence { "The exact direct output target and held-out calibration are retained in the admitted training run; no rolling schedule is inferred." } else if horizon_status == "recorded" { "Bound forecast horizons and rolling temporal splits are retained in the admitted training run." } else { "This admitted bundle is not a forecast model." }
            },
            {
                "dimension": "regime",
                "status": "not_recorded",
                "reason": "The admitted training-run schema has no regime partition; no regime metric is inferred."
            },
            {
                "dimension": "instrument",
                "status": "universe_bound_only",
                "reason": "The training universe is bound by immutable bundle metadata; no per-instrument metric is retained."
            },
            {
                "dimension": "confidence",
                "status": "calibration_bound_only",
                "reason": "Confidence evidence is limited to the admitted calibration coverage and interval policy when present."
            }
        ]
    }))
}

#[derive(Deserialize)]
struct TrainingRunEvidenceWire {
    schema_version: u32,
    trial_sha256: String,
    trial: TrainingTrialEvidenceWire,
}

#[derive(Deserialize)]
struct TrainingTrialEvidenceWire {
    seed: u64,
    missing_policy: String,
    split_counts: SplitCountsEvidenceWire,
    split_sha256: String,
    forecast: Option<ForecastScheduleEvidenceWire>,
}

#[derive(Deserialize)]
struct SplitCountsEvidenceWire {
    train: usize,
    validation: usize,
    test: usize,
}

#[derive(Deserialize)]
struct ForecastScheduleEvidenceWire {
    strategy: String,
    horizons: Vec<u32>,
    observed_cutoff_unix_nanos: i64,
    rolling_splits: u32,
    selection_sha256: String,
}

fn model_output_value(output: &ModelOutput) -> Value {
    json!({
        "modelId": output.model_id().to_string(),
        "bundleId": output.bundle_id().as_str(),
        "bundleVersion": output.bundle_version().get(),
        "trainingDataset": manifest_value(output.dataset().manifest()),
        "featureSemanticDigests": output
            .feature_semantic_digests()
            .iter()
            .map(|digest| encode_hex(digest.as_bytes()))
            .collect::<Vec<_>>(),
        "score": output.score(),
        "confidence": output.confidence(),
        "decision": model_decision_name(output.decision()),
        "executionAuthority": "none",
        "inferenceFailureBehavior": "no_action"
    })
}

fn validation_metrics_value(metadata: &ModelMetadata) -> Value {
    Value::Array(
        metadata
            .validation_metrics()
            .iter()
            .map(|metric| {
                json!({
                    "name": validation_metric_name(metric.name()),
                    "value": metric.value()
                })
            })
            .collect(),
    )
}

fn training_dataset_value(dataset: &TrainingDatasetIdentity) -> Value {
    let split = dataset.split_policy();
    let split_policy = if let Some([train, validation, test]) = split.fiscal_boundaries() {
        json!({"kind": "fiscal_dates", "trainEnd": train, "validationEnd": validation, "testEnd": test})
    } else {
        let bounds = split.timestamp_boundaries();
        json!({
            "kind": "exact_time",
            "trainEndUnixNanos": bounds.map(|times| times[0].unix_nanos().to_string()),
            "validationEndUnixNanos": bounds.map(|times| times[1].unix_nanos().to_string()),
            "testEndUnixNanos": bounds.map(|times| times[2].unix_nanos().to_string()),
        })
    };
    json!({
        "manifest": manifest_value(dataset.manifest()),
        "buildSpecDigest": encode_hex(dataset.build_spec_digest().digest().bytes()),
        "universeDigest": digest_value(dataset.universe_digest()),
        "policyDigest": digest_value(dataset.policy_digest()),
        "catalogIdentity": encode_hex(dataset.catalog_identity().bytes()),
        "exportDigest": digest_value(dataset.export_digest()),
        "selectionDigest": digest_value(dataset.selection_digest()),
        "selectionAsOfUnixNanos": dataset.selection_as_of().unix_nanos().to_string(),
        "selectedComponentRows": dataset.selected_component_rows().get(),
        "splitPolicy": split_policy,
        "study": dataset.study_policy().map(|policy| json!({
            "basis": policy.basis(),
            "purpose": policy.purpose(),
            "snapshotAsOfUnixNanos": policy.snapshot_as_of().unix_nanos().to_string(),
            "decisionLagNanos": policy.decision_lag().map(|lag| lag.as_nanos().to_string()),
            "targetHorizon": match policy.target_horizon() {
                market_squawk_data::DatasetTargetHorizon::ExactElapsed(duration) => json!({
                    "kind": "exact_elapsed", "nanos": duration.as_nanos().to_string(),
                }),
                market_squawk_data::DatasetTargetHorizon::FiscalPeriods { cadence, periods_ahead } => json!({
                    "kind": "fiscal_periods", "cadence": cadence, "periodsAhead": periods_ahead.get(),
                }),
            },
            "sourceSnapshotSha256": dataset.source_snapshot_digest().map(digest_value),
            "limitations": policy.limitations(),
        })),
    })
}

fn manifest_value(manifest: &DatasetManifestRef) -> Value {
    json!({
        "dataset": manifest.dataset_id().as_str(),
        "manifestVersion": manifest.manifest_version(),
        "schema": {
            "name": manifest.schema().name(),
            "version": manifest.schema_version().get(),
            "fingerprint": encode_hex(manifest.schema().fingerprint())
        },
        "contentHash": digest_value(manifest.content_hash())
    })
}

fn digest_value(digest: Sha256Digest) -> String {
    encode_hex(digest.bytes())
}

const fn model_format_name(format: ModelFormat) -> &'static str {
    match format {
        ModelFormat::NativeLinear => "native_linear",
        ModelFormat::NativeLogistic => "native_logistic",
        ModelFormat::Onnx => "onnx",
    }
}

const fn validation_metric_name(name: ValidationMetricName) -> &'static str {
    match name {
        ValidationMetricName::MeanSquaredError => "mean_squared_error",
        ValidationMetricName::Accuracy => "accuracy",
        ValidationMetricName::LogLoss => "log_loss",
        ValidationMetricName::AreaUnderRoc => "area_under_roc",
    }
}

const fn model_decision_name(decision: ModelDecision) -> &'static str {
    match decision {
        ModelDecision::Negative => "negative",
        ModelDecision::NoAction => "no_action",
        ModelDecision::Positive => "positive",
    }
}

const fn model_decision_tag(decision: ModelDecision) -> u8 {
    match decision {
        ModelDecision::Negative => 1,
        ModelDecision::NoAction => 2,
        ModelDecision::Positive => 3,
    }
}

const fn component_kind_name(kind: market_squawk_data::ComponentKind) -> &'static str {
    match kind {
        market_squawk_data::ComponentKind::Feature => "feature",
        market_squawk_data::ComponentKind::Label => "label",
    }
}
