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
use market_squawk_data::{DatasetManifestRef, Sha256Digest};
use market_squawk_domain::ModelId;
use market_squawk_modeling::{
    BundleId, FeatureNormalizer, InferenceBackend, ModelDecision, ModelFeatureValue, ModelFormat,
    ModelInput, ModelMetadata, ModelOutput, ModelRegistry, TrainingDatasetIdentity,
    ValidationMetricName,
};
use market_squawk_services::{
    RequestContext, ServiceDomain, ServiceError, ToolResultMetadata, TypedToolRequest,
    TypedToolResult,
};
use serde_json::{Map, Value, json};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use super::{
    ApplicationDomainService,
    domain_support::{DomainLifecycle, admitted_result_limits, encode_hex, ensure_request_live},
};

pub mod runtime;

const GET_METADATA: &str = "Model.GetMetadata";
const LIST_BUNDLES: &str = "Model.ListBundles";
const EVALUATE: &str = "Model.Evaluate";
const PREDICT: &str = "Model.Predict";
const MAXIMUM_EVALUATION_RECORDS: usize = 100_000;

/// Application-owned model surface over one complete immutable registry/backend generation set.
pub struct ModelDomainService {
    backends: Box<[Arc<dyn InferenceBackend>]>,
    evaluations: Mutex<EvaluationStore>,
    lifecycle: Arc<DomainLifecycle>,
}

impl ModelDomainService {
    /// Binds every retained bundle to exactly one admitted inference backend.
    ///
    /// # Errors
    ///
    /// Rejects an incomplete, duplicate, or registry-inconsistent backend set and excessive
    /// evaluation retention.
    pub fn try_new(
        registry: Arc<ModelRegistry>,
        mut backends: Vec<Arc<dyn InferenceBackend>>,
        maximum_evaluation_records: NonZeroUsize,
    ) -> Result<Self, ModelDomainServiceError> {
        if maximum_evaluation_records.get() > MAXIMUM_EVALUATION_RECORDS {
            return Err(ModelDomainServiceError::EvaluationCapacity);
        }
        backends.sort_unstable_by(|left, right| {
            model_coordinate(left.metadata()).cmp(&model_coordinate(right.metadata()))
        });
        if backends.windows(2).any(|pair| {
            bundle_coordinate(pair[0].metadata()) == bundle_coordinate(pair[1].metadata())
        }) {
            return Err(ModelDomainServiceError::DuplicateBackend);
        }
        let registry_length = registry
            .len()
            .map_err(|_| ModelDomainServiceError::Registry)?;
        if registry_length != backends.len() {
            return Err(ModelDomainServiceError::IncompleteBackendSet);
        }
        for backend in &backends {
            let metadata = backend.metadata();
            let registered = registry
                .get(metadata.bundle_id(), metadata.bundle_version())
                .map_err(|_| ModelDomainServiceError::Registry)?
                .ok_or(ModelDomainServiceError::IncompleteBackendSet)?;
            if registered.metadata() != metadata {
                return Err(ModelDomainServiceError::BackendIdentityMismatch);
            }
        }
        Ok(Self {
            backends: backends.into_boxed_slice(),
            evaluations: Mutex::new(EvaluationStore::new(maximum_evaluation_records)),
            lifecycle: DomainLifecycle::new(),
        })
    }

    fn metadata(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let model_id = admitted_model_id(request.arguments())?;
        let bundle = self
            .backends
            .iter()
            .filter(|backend| backend.metadata().model_id() == model_id)
            .max_by_key(|backend| backend.metadata().bundle_version())
            .ok_or(ServiceError::NotFound)?;
        one_result(model_metadata_value(bundle.metadata()), request, context)
    }

    fn bundles(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let limits = admitted_result_limits(request, context)?;
        let available = self.backends.len();
        let bundles = self
            .backends
            .iter()
            .take(limits.maximum_result_items())
            .map(|backend| bundle_summary(backend.metadata()))
            .collect::<Vec<_>>();
        let metadata = if bundles.len() < available {
            ToolResultMetadata::try_truncated_not_applicable(available)?
        } else {
            ToolResultMetadata::complete_not_applicable()
        };
        let item_count = bundles.len().max(1);
        TypedToolResult::try_new(json!({"bundles": bundles}), item_count, metadata, limits)
            .map_err(Into::into)
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
        let backend = self
            .backends
            .iter()
            .find(|backend| {
                let metadata = backend.metadata();
                metadata.model_id() == model_id
                    && metadata.bundle_id() == &parsed.bundle_id
                    && metadata.bundle_version() == parsed.bundle_version
            })
            .ok_or(ServiceError::NotFound)?;
        let metadata = backend.metadata();
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
}

impl fmt::Debug for ModelDomainService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelDomainService")
            .field("backend_count", &self.backends.len())
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
        let result = match request.name() {
            GET_METADATA => self.metadata(&request, &context),
            LIST_BUNDLES => self.bundles(&request, &context),
            EVALUATE => self.infer(&request, &context, true),
            PREDICT => self.infer(&request, &context, false),
            _ => Err(ServiceError::NotFound),
        }?;
        ensure_request_live(&context, &self.lifecycle)?;
        Ok(result)
    }

    fn begin_shutdown(&self) {
        self.lifecycle.begin_shutdown();
    }

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        self.lifecycle.finish_shutdown(deadline).await
    }
}

impl Drop for ModelDomainService {
    fn drop(&mut self) {
        self.lifecycle.begin_shutdown();
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

fn model_coordinate(metadata: &ModelMetadata) -> (String, String, u64) {
    (
        metadata.model_id().to_string(),
        metadata.bundle_id().as_str().to_owned(),
        metadata.bundle_version().get(),
    )
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

fn model_metadata_value(metadata: &ModelMetadata) -> Value {
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
    json!({
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
        "universeId": metadata.universe_id().as_str(),
        "trainingPeriod": {
            "startUnixNanos": metadata.training_period().start().unix_nanos(),
            "endUnixNanos": metadata.training_period().end().unix_nanos()
        },
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
        }
    })
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
    json!({
        "manifest": manifest_value(dataset.manifest()),
        "buildSpecDigest": encode_hex(dataset.build_spec_digest().digest().bytes()),
        "universeDigest": digest_value(dataset.universe_digest()),
        "policyDigest": digest_value(dataset.policy_digest()),
        "catalogIdentity": encode_hex(dataset.catalog_identity().bytes()),
        "exportDigest": digest_value(dataset.export_digest()),
        "selectionDigest": digest_value(dataset.selection_digest()),
        "selectionAsOfUnixNanos": dataset.selection_as_of().unix_nanos(),
        "selectedComponentRows": dataset.selected_component_rows().get()
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
