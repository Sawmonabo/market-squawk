//! Exact admitted-model forecast generation and application request decoding.

use std::{
    num::{NonZeroU16, NonZeroU64, NonZeroUsize},
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

use market_squawk_data::Sha256Digest;
use market_squawk_domain::{InstrumentId, ModelId, Timestamp};
use market_squawk_modeling::{
    BundleId, CalibrationEvidence, ForecastError, ForecastHorizon, ForecastRequest,
    ModelFeatureValue, ModelInput, ResearchForecastBackend,
};
use market_squawk_services::{
    ArtifactError, ArtifactPublicationContext, RequestContext, ServiceError, TypedToolRequest,
    TypedToolResult,
};
use serde_json::{Map, Value};
use sha2::{Digest as _, Sha256};

use super::super::{ModelDomainService, admitted_model_id, one_result};
use super::ForecastApplicationError;
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
        let forecast_request = ForecastRequest::try_new(
            parsed.instrument_id,
            parsed.observed_cutoff,
            parsed.available_at,
            parsed.horizon,
            parsed.decimal_scale,
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
    inputs: Box<[Box<[f64]>]>,
}

impl ParsedForecastRequest {
    fn request_hash(&self, model_id: ModelId) -> Result<Sha256Digest, ServiceError> {
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/forecast-request/v1\0");
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
        const FIELDS: [&str; 10] = [
            "instrumentId",
            "bundleId",
            "bundleVersion",
            "observedThroughUnixNanos",
            "availableAtUnixNanos",
            "horizonPoints",
            "horizonStepNanos",
            "decimalScale",
            "validityNanos",
            "inputs",
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
        Ok(Self {
            instrument_id,
            bundle_id,
            bundle_version,
            observed_cutoff,
            available_at,
            horizon,
            decimal_scale,
            validity_nanos,
            inputs: inputs.into_boxed_slice(),
        })
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
        .get("vintageId")
        .and_then(Value::as_str)
        .filter(|value| valid_digest(value))
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
        | ForecastApplicationError::Unavailable => ServiceError::Unavailable,
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

fn invalid<T>(_error: T) -> ServiceError {
    ServiceError::InvalidRequest
}
