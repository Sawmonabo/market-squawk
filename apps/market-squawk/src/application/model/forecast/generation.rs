//! Exact admitted-model forecast generation and application request decoding.

use std::{
    num::{NonZeroU16, NonZeroU64, NonZeroUsize},
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

use market_squawk_data::{
    DatasetId, DatasetManifestRef, DatasetSchemaRef, DatasetSchemaRegistry, Sha256Digest,
};
use market_squawk_domain::{
    DataQuality, InstrumentId, ModelId, SchemaVersion, SourceId, Timestamp,
};
use market_squawk_modeling::{
    BundleId, CalibrationEvidence, ForecastError, ForecastHorizon, ForecastObservedPoint,
    ForecastRequest, ForecastValue, ModelFeatureValue, ModelInput, ResearchForecastBackend,
};
use market_squawk_services::{
    ArtifactError, ArtifactPublicationContext, RequestContext, ServiceError, TypedToolRequest,
    TypedToolResult,
};
use serde_json::{Map, Value};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use super::super::{ModelDomainService, admitted_model_id, one_result};
use super::{ForecastAnalysisEvidence, ForecastApplicationError, ForecastServingEvidence};
use crate::application::domain_support::{admitted_result_limits, ensure_request_live};

const MAXIMUM_FORECAST_VALIDITY_NANOS: u64 = 30 * 24 * 60 * 60 * 1_000_000_000;

impl ModelDomainService {
    pub(in crate::application::model) async fn generate_forecast(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let forecasts = self.forecasts.as_ref().ok_or(ServiceError::Unavailable)?;
        let model_id = admitted_model_id(request.arguments())?;
        let parsed = ParsedForecastRequest::try_from(
            request
                .arguments()
                .get("request")
                .and_then(Value::as_object)
                .ok_or(ServiceError::InvalidRequest)?,
        )?;
        let image = self.read_image.load();
        let backend = image
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
        let mut rows = Vec::new();
        rows.try_reserve_exact(parsed.inputs.len())
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        for row in &parsed.inputs {
            if row.len() != metadata.features().len() {
                return Err(ServiceError::InvalidRequest);
            }
            let mut values = metadata
                .features()
                .iter()
                .map(ModelFeatureValue::from_binding)
                .collect::<Vec<_>>();
            for (slot, value) in values.iter_mut().zip(row.iter().copied()) {
                slot.try_set_value(value)
                    .map_err(|_error| ServiceError::InvalidRequest)?;
            }
            rows.push(values.into_boxed_slice());
        }
        let inputs = rows
            .iter()
            .map(|values| {
                ModelInput::try_new(metadata, values).map_err(|_error| ServiceError::InvalidRequest)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let forecast_request = ForecastRequest::try_new_with_observed_history(
            parsed.instrument_id,
            parsed.observed_cutoff,
            parsed.available_at,
            parsed.horizon,
            parsed.decimal_scale,
            &parsed.observed_history,
            &inputs,
        )
        .map_err(|_error| ServiceError::InvalidRequest)?;
        let calibration = metadata
            .forecast_calibration()
            .map(|value| {
                CalibrationEvidence::try_new(
                    metadata,
                    value.method(),
                    value.window(),
                    value.policy_hash(),
                    value.residuals_hash(),
                    *value.bands(),
                    value.dependence_assumptions(),
                )
            })
            .transpose()
            .map_err(|_error| ServiceError::InvalidResult)?;
        ensure_request_live(context, &self.lifecycle)?;
        let path = backend
            .forecast(&forecast_request, calibration.as_ref())
            .map_err(map_modeling_forecast_error)?;
        ensure_request_live(context, &self.lifecycle)?;
        let created_at = wall_now()?;
        let validity =
            i64::try_from(parsed.validity_nanos).map_err(|_error| ServiceError::InvalidRequest)?;
        let expires_at = created_at
            .checked_add_nanos(validity)
            .map_err(|_error| ServiceError::InvalidRequest)?;
        let content = forecasts
            .publish_vintage(
                parsed.request_hash(model_id)?,
                path,
                parsed.analysis_evidence.clone(),
                parsed.serving_evidence.clone(),
                created_at,
                expires_at,
                ArtifactPublicationContext::new(context.cancellation().clone(), context.deadline()),
            )
            .await
            .map_err(map_forecast_error)?;
        one_result(content, request, context)
    }

    pub(in crate::application::model) async fn get_forecast(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let forecasts = self.forecasts.as_ref().ok_or(ServiceError::Unavailable)?;
        let vintage = admitted_vintage_id(request.arguments())?;
        one_result(
            forecasts
                .get_forecast(vintage)
                .await
                .map_err(map_forecast_error)?,
            request,
            context,
        )
    }

    pub(in crate::application::model) async fn list_forecasts(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let forecasts = self.forecasts.as_ref().ok_or(ServiceError::Unavailable)?;
        let limits = admitted_result_limits(request, context)?;
        let maximum =
            NonZeroUsize::new(limits.maximum_result_items()).ok_or(ServiceError::InvalidRequest)?;
        one_result(
            forecasts
                .list_forecasts(maximum)
                .await
                .map_err(map_forecast_error)?,
            request,
            context,
        )
    }

    pub(in crate::application::model) async fn get_forecast_outcomes(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let forecasts = self.forecasts.as_ref().ok_or(ServiceError::Unavailable)?;
        let vintage = admitted_vintage_id(request.arguments())?;
        let limits = admitted_result_limits(request, context)?;
        let maximum =
            NonZeroUsize::new(limits.maximum_result_items()).ok_or(ServiceError::InvalidRequest)?;
        one_result(
            forecasts
                .get_forecast_outcomes(vintage, maximum)
                .await
                .map_err(map_forecast_error)?,
            request,
            context,
        )
    }
}

struct ParsedForecastRequest {
    instrument_id: InstrumentId,
    bundle_id: BundleId,
    bundle_version: NonZeroU64,
    observed_cutoff: Timestamp,
    available_at: Timestamp,
    horizon: ForecastHorizon,
    decimal_scale: u8,
    validity_nanos: u64,
    observed_history: Box<[ForecastObservedPoint]>,
    inputs: Box<[Box<[f64]>]>,
    analysis_evidence: ForecastAnalysisEvidence,
    serving_evidence: ForecastServingEvidence,
}

impl ParsedForecastRequest {
    fn request_hash(&self, model_id: ModelId) -> Result<Sha256Digest, ServiceError> {
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/forecast-request/v3\0");
        digest.update(model_id.as_uuid().as_bytes());
        digest.update(self.instrument_id.as_uuid().as_bytes());
        digest.update(self.bundle_id.as_str().as_bytes());
        digest.update([0]);
        digest.update(self.bundle_version.get().to_be_bytes());
        digest.update(self.observed_cutoff.unix_nanos().to_be_bytes());
        digest.update(self.available_at.unix_nanos().to_be_bytes());
        digest.update(self.horizon.points().get().to_be_bytes());
        digest.update(self.horizon.step_nanos().get().to_be_bytes());
        digest.update([self.decimal_scale]);
        digest.update(self.validity_nanos.to_be_bytes());
        hash_manifest(&mut digest, self.analysis_evidence.manifest())?;
        digest.update(self.analysis_evidence.production_identity_sha256().bytes());
        digest.update(self.analysis_evidence.production_receipt_sha256().bytes());
        digest.update(self.analysis_evidence.pairing_sha256().bytes());
        hash_manifest(&mut digest, self.serving_evidence.manifest())?;
        hash_bytes(
            &mut digest,
            self.serving_evidence.source_id().as_str().as_bytes(),
        )?;
        digest.update(self.serving_evidence.object_graph_sha256().bytes());
        digest.update(self.serving_evidence.selection_sha256().bytes());
        digest.update(self.serving_evidence.result_sha256().bytes());
        digest.update(
            self.serving_evidence
                .knowledge_cutoff()
                .unix_nanos()
                .to_be_bytes(),
        );
        digest.update(
            self.serving_evidence
                .prior_observed_at()
                .unix_nanos()
                .to_be_bytes(),
        );
        digest.update(
            self.serving_evidence
                .observed_through()
                .unix_nanos()
                .to_be_bytes(),
        );
        digest.update(self.serving_evidence.feature_sha256().bytes());
        for point in &self.observed_history {
            digest.update(point.observed_at().unix_nanos().to_be_bytes());
            digest.update(point.available_at().unix_nanos().to_be_bytes());
            digest.update(point.value().mantissa().to_be_bytes());
            digest.update([point.value().scale()]);
            digest.update(point.source_pit_hash().bytes());
            digest.update([quality_tag(point.quality())]);
        }
        for row in &self.inputs {
            let row_length =
                u64::try_from(row.len()).map_err(|_error| ServiceError::InvalidRequest)?;
            digest.update(row_length.to_be_bytes());
            for value in row {
                digest.update(value.to_bits().to_be_bytes());
            }
        }
        Ok(Sha256Digest::new(digest.finalize().into()))
    }
}

impl TryFrom<&Map<String, Value>> for ParsedForecastRequest {
    type Error = ServiceError;

    fn try_from(input: &Map<String, Value>) -> Result<Self, Self::Error> {
        const FIELDS: [&str; 13] = [
            "instrumentId",
            "bundleId",
            "bundleVersion",
            "observedThroughUnixNanos",
            "availableAtUnixNanos",
            "horizonPoints",
            "horizonStepNanos",
            "decimalScale",
            "validityNanos",
            "observedHistory",
            "inputs",
            "analysisEvidence",
            "servingEvidence",
        ];
        if input.len() != FIELDS.len() || input.keys().any(|key| !FIELDS.contains(&key.as_str())) {
            return Err(ServiceError::InvalidRequest);
        }
        let instrument_id = identifier(input, "instrumentId")
            .and_then(|value| InstrumentId::from_str(value).map_err(invalid))?;
        let bundle_id = identifier(input, "bundleId")
            .and_then(|value| BundleId::try_new(value).map_err(invalid))?;
        let bundle_version = unsigned(input, "bundleVersion")
            .and_then(NonZeroU64::new)
            .ok_or(ServiceError::InvalidRequest)?;
        let observed_cutoff = timestamp(input, "observedThroughUnixNanos")?;
        let available_at = timestamp(input, "availableAtUnixNanos")?;
        let horizon_points = unsigned(input, "horizonPoints")
            .and_then(|value| u16::try_from(value).ok())
            .and_then(NonZeroU16::new)
            .ok_or(ServiceError::InvalidRequest)?;
        let horizon_step = unsigned(input, "horizonStepNanos")
            .and_then(NonZeroU64::new)
            .ok_or(ServiceError::InvalidRequest)?;
        let horizon = ForecastHorizon::try_new(horizon_points, horizon_step).map_err(invalid)?;
        let decimal_scale = unsigned(input, "decimalScale")
            .and_then(|value| u8::try_from(value).ok())
            .filter(|value| *value <= market_squawk_modeling::MAX_FORECAST_DECIMAL_SCALE)
            .ok_or(ServiceError::InvalidRequest)?;
        let validity_nanos = unsigned(input, "validityNanos")
            .filter(|value| *value > 0 && *value <= MAXIMUM_FORECAST_VALIDITY_NANOS)
            .ok_or(ServiceError::InvalidRequest)?;
        let observed_history = input
            .get("observedHistory")
            .and_then(Value::as_array)
            .filter(|values| {
                !values.is_empty()
                    && values.len() <= market_squawk_modeling::MAX_FORECAST_OBSERVED_POINTS
            })
            .ok_or(ServiceError::InvalidRequest)?;
        let mut observed = Vec::new();
        observed
            .try_reserve_exact(observed_history.len())
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        for value in observed_history {
            observed.push(parse_observed_point(value, decimal_scale)?);
        }
        let encoded_inputs = input
            .get("inputs")
            .and_then(Value::as_array)
            .filter(|values| values.len() == usize::from(horizon_points.get()))
            .ok_or(ServiceError::InvalidRequest)?;
        let mut inputs = Vec::new();
        inputs
            .try_reserve_exact(encoded_inputs.len())
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        for encoded in encoded_inputs {
            let values = encoded
                .as_array()
                .filter(|values| {
                    !values.is_empty() && values.len() <= market_squawk_modeling::MAX_MODEL_FEATURES
                })
                .ok_or(ServiceError::InvalidRequest)?;
            let mut row = Vec::new();
            row.try_reserve_exact(values.len())
                .map_err(|_error| ServiceError::ResourceExhausted)?;
            for value in values {
                row.push(
                    value
                        .as_f64()
                        .filter(|value| value.is_finite())
                        .ok_or(ServiceError::InvalidRequest)?,
                );
            }
            inputs.push(row.into_boxed_slice());
        }
        let analysis_evidence = parse_analysis_evidence(
            input
                .get("analysisEvidence")
                .and_then(Value::as_object)
                .ok_or(ServiceError::InvalidRequest)?,
        )?;
        let serving_evidence = parse_serving_evidence(
            input
                .get("servingEvidence")
                .and_then(Value::as_object)
                .ok_or(ServiceError::InvalidRequest)?,
        )?;
        if serving_evidence.observed_through() != observed_cutoff
            || available_at > serving_evidence.knowledge_cutoff()
        {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(Self {
            instrument_id,
            bundle_id,
            bundle_version,
            observed_cutoff,
            available_at,
            horizon,
            decimal_scale,
            validity_nanos,
            observed_history: observed.into_boxed_slice(),
            inputs: inputs.into_boxed_slice(),
            analysis_evidence,
            serving_evidence,
        })
    }
}

fn parse_serving_evidence(
    input: &Map<String, Value>,
) -> Result<ForecastServingEvidence, ServiceError> {
    const FIELDS: [&str; 9] = [
        "manifest",
        "sourceId",
        "objectGraphSha256",
        "selectionSha256",
        "resultSha256",
        "knowledgeCutoffUnixNanos",
        "priorObservedAtUnixNanos",
        "observedThroughUnixNanos",
        "featureSha256",
    ];
    if input.len() != FIELDS.len() || input.keys().any(|key| !FIELDS.contains(&key.as_str())) {
        return Err(ServiceError::InvalidRequest);
    }
    ForecastServingEvidence::try_new(
        parse_analysis_manifest(
            input
                .get("manifest")
                .and_then(Value::as_object)
                .ok_or(ServiceError::InvalidRequest)?,
        )?,
        SourceId::try_from(identifier(input, "sourceId")?).map_err(invalid)?,
        digest(input, "objectGraphSha256")?,
        digest(input, "selectionSha256")?,
        digest(input, "resultSha256")?,
        timestamp(input, "knowledgeCutoffUnixNanos")?,
        timestamp(input, "priorObservedAtUnixNanos")?,
        timestamp(input, "observedThroughUnixNanos")?,
        digest(input, "featureSha256")?,
    )
    .map_err(|_| ServiceError::InvalidRequest)
}

fn parse_analysis_evidence(
    input: &Map<String, Value>,
) -> Result<ForecastAnalysisEvidence, ServiceError> {
    const FIELDS: [&str; 4] = [
        "manifest",
        "productionIdentitySha256",
        "productionReceiptSha256",
        "pairingSha256",
    ];
    if input.len() != FIELDS.len() || input.keys().any(|key| !FIELDS.contains(&key.as_str())) {
        return Err(ServiceError::InvalidRequest);
    }
    let manifest = parse_analysis_manifest(
        input
            .get("manifest")
            .and_then(Value::as_object)
            .ok_or(ServiceError::InvalidRequest)?,
    )?;
    ForecastAnalysisEvidence::try_new(
        manifest,
        digest(input, "productionIdentitySha256")?,
        digest(input, "productionReceiptSha256")?,
        digest(input, "pairingSha256")?,
    )
    .map_err(|_| ServiceError::InvalidRequest)
}

fn parse_analysis_manifest(input: &Map<String, Value>) -> Result<DatasetManifestRef, ServiceError> {
    const FIELDS: [&str; 4] = ["dataset", "manifestVersion", "schema", "contentHash"];
    if input.len() != FIELDS.len() || input.keys().any(|key| !FIELDS.contains(&key.as_str())) {
        return Err(ServiceError::InvalidRequest);
    }
    let schema_input = input
        .get("schema")
        .and_then(Value::as_object)
        .ok_or(ServiceError::InvalidRequest)?;
    const SCHEMA_FIELDS: [&str; 3] = ["name", "version", "fingerprint"];
    if schema_input.len() != SCHEMA_FIELDS.len()
        || schema_input
            .keys()
            .any(|key| !SCHEMA_FIELDS.contains(&key.as_str()))
    {
        return Err(ServiceError::InvalidRequest);
    }
    let schema_version = unsigned(schema_input, "version")
        .and_then(|value| u16::try_from(value).ok())
        .and_then(|value| SchemaVersion::new(value).ok())
        .ok_or(ServiceError::InvalidRequest)?;
    let fingerprint = digest(schema_input, "fingerprint")?;
    let content_hash = digest(input, "contentHash")?;
    if fingerprint.bytes() == [0; 32] || content_hash.bytes() == [0; 32] {
        return Err(ServiceError::InvalidRequest);
    }
    let schema = DatasetSchemaRef::try_new(
        identifier(schema_input, "name")?,
        schema_version,
        fingerprint.bytes(),
    )
    .map_err(invalid)?;
    DatasetSchemaRegistry::local()
        .resolve(&schema)
        .map_err(invalid)?;
    DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from(identifier(input, "dataset")?).map_err(invalid)?,
        unsigned(input, "manifestVersion")
            .filter(|version| *version > 0)
            .ok_or(ServiceError::InvalidRequest)?,
        schema,
        content_hash,
    )
    .map_err(invalid)
}

fn hash_manifest(digest: &mut Sha256, manifest: &DatasetManifestRef) -> Result<(), ServiceError> {
    hash_bytes(digest, manifest.dataset_id().as_str().as_bytes())?;
    digest.update(manifest.manifest_version().to_be_bytes());
    hash_bytes(digest, manifest.schema().name().as_bytes())?;
    digest.update(manifest.schema_version().get().to_be_bytes());
    digest.update(manifest.schema().fingerprint());
    digest.update(manifest.content_hash().bytes());
    Ok(())
}

fn hash_bytes(digest: &mut Sha256, value: &[u8]) -> Result<(), ServiceError> {
    digest.update(
        u64::try_from(value.len())
            .map_err(|_| ServiceError::ResourceExhausted)?
            .to_be_bytes(),
    );
    digest.update(value);
    Ok(())
}

fn parse_observed_point(
    value: &Value,
    decimal_scale: u8,
) -> Result<ForecastObservedPoint, ServiceError> {
    let object = value.as_object().ok_or(ServiceError::InvalidRequest)?;
    const FIELDS: [&str; 5] = [
        "observedAtUnixNanos",
        "availableAtUnixNanos",
        "mantissa",
        "sourcePitHash",
        "quality",
    ];
    if object.len() != FIELDS.len() || object.keys().any(|key| !FIELDS.contains(&key.as_str())) {
        return Err(ServiceError::InvalidRequest);
    }
    let observed_at = timestamp(object, "observedAtUnixNanos")?;
    let available_at = timestamp(object, "availableAtUnixNanos")?;
    let mantissa = object
        .get("mantissa")
        .and_then(Value::as_str)
        .and_then(|value| value.parse::<i128>().ok())
        .ok_or(ServiceError::InvalidRequest)?;
    let source_pit_hash = digest(object, "sourcePitHash")?;
    let quality = data_quality(
        object
            .get("quality")
            .and_then(Value::as_str)
            .ok_or(ServiceError::InvalidRequest)?,
    )?;
    ForecastObservedPoint::try_new(
        observed_at,
        available_at,
        ForecastValue::try_new(mantissa, decimal_scale).map_err(invalid)?,
        source_pit_hash,
        quality,
    )
    .map_err(invalid)
}

fn digest(input: &Map<String, Value>, name: &str) -> Result<Sha256Digest, ServiceError> {
    let value = input
        .get(name)
        .and_then(Value::as_str)
        .filter(|value| valid_digest(value))
        .ok_or(ServiceError::InvalidRequest)?;
    let mut bytes = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = hex_nibble(pair[0]).ok_or(ServiceError::InvalidRequest)?;
        let low = hex_nibble(pair[1]).ok_or(ServiceError::InvalidRequest)?;
        bytes[index] = (high << 4) | low;
    }
    Ok(Sha256Digest::new(bytes))
}

fn data_quality(value: &str) -> Result<DataQuality, ServiceError> {
    match value {
        "direct_verified" => Ok(DataQuality::DirectVerified),
        "direct_unverified" => Ok(DataQuality::DirectUnverified),
        "official_delayed" => Ok(DataQuality::OfficialDelayed),
        "aggregated" => Ok(DataQuality::Aggregated),
        "indicative" => Ok(DataQuality::Indicative),
        "estimated" => Ok(DataQuality::Estimated),
        "stale" => Ok(DataQuality::Stale),
        "quarantined" => Ok(DataQuality::Quarantined),
        _ => Err(ServiceError::InvalidRequest),
    }
}

fn identifier<'value>(
    input: &'value Map<String, Value>,
    name: &str,
) -> Result<&'value str, ServiceError> {
    input
        .get(name)
        .and_then(Value::as_str)
        .ok_or(ServiceError::InvalidRequest)
}

fn unsigned(input: &Map<String, Value>, name: &str) -> Option<u64> {
    input.get(name).and_then(Value::as_u64)
}

fn timestamp(input: &Map<String, Value>, name: &str) -> Result<Timestamp, ServiceError> {
    input
        .get(name)
        .and_then(Value::as_i64)
        .map(Timestamp::from_unix_nanos)
        .ok_or(ServiceError::InvalidRequest)
}

fn admitted_vintage_id(arguments: &Map<String, Value>) -> Result<&str, ServiceError> {
    arguments
        .get("forecastToken")
        .and_then(Value::as_str)
        .filter(|value| Uuid::parse_str(value).is_ok())
        .ok_or(ServiceError::InvalidRequest)
}

fn wall_now() -> Result<Timestamp, ServiceError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_error| ServiceError::Unavailable)?;
    let nanos = i64::try_from(elapsed.as_nanos()).map_err(|_error| ServiceError::Unavailable)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn map_forecast_error(error: ForecastApplicationError) -> ServiceError {
    match error {
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
        | ForecastApplicationError::State(_)
        | ForecastApplicationError::Unavailable
        | ForecastApplicationError::RestoreTargetNotFresh => ServiceError::Unavailable,
        ForecastApplicationError::Conflict | ForecastApplicationError::CorruptIndex => {
            ServiceError::Internal
        }
    }
}

fn map_modeling_forecast_error(error: ForecastError) -> ServiceError {
    match error {
        ForecastError::Capacity => ServiceError::ResourceExhausted,
        ForecastError::Inference(_) => ServiceError::Unavailable,
        ForecastError::InvalidHorizon
        | ForecastError::InvalidRequest
        | ForecastError::InvalidObservedHistory
        | ForecastError::InvalidOutputBinding
        | ForecastError::InvalidDecimal
        | ForecastError::InvalidCalibration
        | ForecastError::CalibrationIdentityMismatch
        | ForecastError::InvalidInterval
        | ForecastError::InvalidVintage
        | ForecastError::InvalidOutcome
        | ForecastError::OutcomeTargetMismatch => ServiceError::InvalidResult,
    }
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

const fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
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

fn invalid<T>(_error: T) -> ServiceError {
    ServiceError::InvalidRequest
}
