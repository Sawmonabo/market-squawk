//! Exact admitted-model forecast generation and application request decoding.

use std::{
    num::{NonZeroU16, NonZeroU64, NonZeroUsize},
    str::FromStr,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use market_squawk_data::{
    AnalyticalReadCapability, DatasetId, DatasetManifestRef, DatasetSchemaRef,
    DatasetSchemaRegistry, FeatureDatasetInputCoordinate, FeatureDatasetInputEpochOutput,
    FeatureDatasetProductContract, ForecastFeatureValue, QueryLimits, Sha256Digest,
};
use market_squawk_domain::{
    Currency, DataQuality, InstrumentId, ModelId, SchemaVersion, SourceId, Timestamp,
};
use market_squawk_modeling::{
    BundleId, CalibrationEvidence, ForecastError, ForecastHorizon, ForecastObservedPoint,
    ForecastRequest, ForecastValue, ModelFeatureValue, ModelInput, ResearchForecastBackend,
};
use market_squawk_services::{
    ArtifactError, ArtifactPublicationContext, RequestContext, ServiceDomain, ServiceError,
    ServiceLimits, ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use serde_json::{Map, Value};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use super::super::{
    ForecastModelEvidenceProjection, ModelDomainService, admitted_model_id,
    forecast_model_evidence_projection_for_horizon, one_result,
};
use super::{
    ForecastAnalysisEvidence, ForecastApplicationError, ForecastCollection, ForecastJobOutput,
    ForecastPrecommitAuthority, ForecastProductIdentity, ForecastProductTarget,
    ForecastServingEvidence,
};
use crate::application::domain_support::{
    DomainLifecycle, admitted_result_limits, ensure_request_live,
};

const MAXIMUM_FORECAST_VALIDITY_NANOS: u64 = 30 * 24 * 60 * 60 * 1_000_000_000;

impl ModelDomainService {
    pub(in crate::application::model) async fn generate_forecast(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        self.generate_forecast_with_precommit(request, context, None)
            .await
            .map(|output| output.result)
    }

    async fn generate_forecast_with_precommit(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        precommit: Option<&dyn ForecastPrecommitAuthority>,
    ) -> Result<ForecastJobOutput, ServiceError> {
        let forecasts = self.forecasts.as_ref().ok_or(ServiceError::Unavailable)?;
        let model_id = admitted_model_id(request.arguments())?;
        let parsed = ParsedForecastRequest::try_from(
            request
                .arguments()
                .get("request")
                .and_then(Value::as_object)
                .ok_or(ServiceError::InvalidRequest)?,
        )?;
        let request_hash = parsed.request_hash(model_id)?;
        // An exact durable retry must remain recoverable even after its model leaves the runtime.
        if let Some(result) = self
            .replay_forecast_with_hash(request_hash, request, context, precommit)
            .await?
        {
            return Ok(result);
        }
        let image = self.read_image.load();
        let active = image
            .activate(
                &parsed.bundle_id,
                parsed.bundle_version,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(|error| match error {
                super::super::runtime::ProductionModelRuntimeError::ValidationDeadline => {
                    ServiceError::DeadlineExceeded
                }
                _ if context.cancellation().is_cancelled() => ServiceError::Cancelled,
                _ => ServiceError::Unavailable,
            })?;
        let backend = active.backend();
        let bundle = active.bundle();
        let metadata = bundle.metadata();
        if metadata.model_id() != model_id {
            return Err(ServiceError::NotFound);
        }
        let authoritative_evidence =
            forecast_model_evidence_projection_for_horizon(bundle, parsed.horizon)?;
        if authoritative_evidence != parsed.model_evidence {
            return Err(ServiceError::InvalidRequest);
        }
        let product_target = ForecastProductTarget::try_from_binding(metadata.output_binding())
            .map_err(|_| ServiceError::Unavailable)?;
        if product_target
            .currency_code()
            .is_some_and(|currency| currency != parsed.product_identity.quote_currency().as_str())
        {
            return Err(ServiceError::InvalidRequest);
        }
        let financial_output = if parsed.horizon.fiscal_periods().is_some() {
            let analytical = self
                .forecast_analytical
                .as_ref()
                .ok_or(ServiceError::Unavailable)?;
            let output = reopen_financial_input(
                analytical,
                parsed.serving_evidence.manifest(),
                context.deadline(),
                context.cancellation().clone(),
            )
            .await?;
            let index = financial_coordinate_index(&output, &parsed.serving_evidence)?;
            let actual = ForecastServingEvidence::from_financial_output(&output, index)
                .map_err(|_| ServiceError::InvalidResult)?;
            if actual != parsed.serving_evidence {
                return Err(ServiceError::InvalidRequest);
            }
            if financial_analysis_evidence(metadata, output.dataset())? != parsed.analysis_evidence
            {
                return Err(ServiceError::InvalidRequest);
            }
            let coordinate = output
                .coordinate(index)
                .ok_or(ServiceError::InvalidResult)?;
            let actual_values = financial_feature_values(metadata, coordinate)?;
            if parsed.inputs.len() != 1
                || parsed.inputs[0].len() != actual_values.len()
                || parsed.inputs[0]
                    .iter()
                    .zip(&actual_values)
                    .any(|(left, right)| left.to_bits() != right.to_bits())
            {
                return Err(ServiceError::InvalidRequest);
            }
            Some((output, index))
        } else {
            None
        };
        if let Some(current) = parsed.serving_evidence.current_price_input() {
            let analytical = self
                .forecast_analytical
                .as_ref()
                .ok_or(ServiceError::Unavailable)?;
            let output = super::current_input::reopen_current_price_input(
                analytical,
                parsed.serving_evidence.manifest(),
                context.deadline(),
                context.cancellation().clone(),
            )
            .await?;
            let index =
                super::current_input::current_price_coordinate_index(&output, &current.example_id)?;
            let coordinate = output
                .coordinate(index)
                .ok_or(ServiceError::InvalidResult)?;
            let cohort = super::current_input::current_price_cohort_reference(current)?;
            super::current_input::current_price_session_origin(
                self.forecast_calendar.as_ref(),
                cohort.as_ref(),
                coordinate,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await?;
            let actual =
                ForecastServingEvidence::from_current_price_output(&output, index, cohort.as_ref())
                    .map_err(invalid)?;
            let values = super::current_input::current_price_feature_values(metadata, coordinate)?;
            let epoch = coordinate.epoch();
            let observed = super::current_input::current_price_observed_point(
                coordinate,
                *values.first().ok_or(ServiceError::InvalidResult)?,
            )?;
            if actual != parsed.serving_evidence
                || epoch.instrument_id() != parsed.instrument_id
                || epoch.source_selection_as_of() != parsed.available_at
                || epoch.target_origin() != parsed.observed_cutoff
                || parsed.inputs.len() != 1
                || parsed.inputs[0].len() != values.len()
                || parsed.inputs[0]
                    .iter()
                    .zip(&values)
                    .any(|(left, right)| left.to_bits() != right.to_bits())
                || if matches!(
                    metadata.output_binding().target(),
                    market_squawk_modeling::ForecastTargetMeaning::FixedHorizonEvent { .. }
                ) {
                    !parsed.observed_history.is_empty()
                } else {
                    parsed.observed_history.as_ref() != [observed].as_slice()
                }
            {
                return Err(ServiceError::InvalidRequest);
            }
        }
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
        let forecast_request = match &financial_output {
            Some((output, index)) => ForecastRequest::try_for_financial_coordinate(
                output
                    .coordinate(*index)
                    .ok_or(ServiceError::InvalidResult)?,
                parsed.decimal_scale,
                &inputs,
            ),
            None if matches!(
                metadata.output_binding().target(),
                market_squawk_modeling::ForecastTargetMeaning::FixedHorizonEvent { .. }
            ) =>
            {
                if !parsed.observed_history.is_empty()
                    || parsed.serving_evidence.current_price_input().is_none()
                {
                    return Err(ServiceError::InvalidRequest);
                }
                ForecastRequest::try_new(
                    parsed.instrument_id,
                    parsed.observed_cutoff.ok_or(ServiceError::InvalidRequest)?,
                    parsed.available_at,
                    parsed.horizon,
                    parsed.decimal_scale,
                    &inputs,
                )
            }
            None => ForecastRequest::try_new_with_observed_history(
                parsed.instrument_id,
                parsed.observed_cutoff.ok_or(ServiceError::InvalidRequest)?,
                parsed.available_at,
                parsed.horizon,
                parsed.decimal_scale,
                &parsed.observed_history,
                &inputs,
            ),
        }
        .map_err(|_| ServiceError::InvalidRequest)?;
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
        let validity_end = created_at
            .checked_add_nanos(validity)
            .map_err(|_| ServiceError::InvalidRequest)?;
        let expires_at = path
            .points()
            .first()
            .ok_or(ServiceError::InvalidResult)?
            .target_at()
            .map_or(validity_end, |target| target.min(validity_end));
        let result_limits = admitted_result_limits(request, context)?;
        forecasts
            .publish_vintage(
                request_hash,
                path,
                parsed.product_identity.clone(),
                parsed.model_evidence.clone(),
                parsed.analysis_evidence.clone(),
                parsed.serving_evidence.clone(),
                created_at,
                expires_at,
                ArtifactPublicationContext::new(context.cancellation().clone(), context.deadline()),
                precommit,
                |content, artifact| {
                    let result = TypedToolResult::try_new(
                        content,
                        1,
                        ToolResultMetadata::complete_not_applicable(),
                        result_limits,
                    )
                    .map_err(|_| ForecastApplicationError::Capacity)?;
                    Ok(ForecastJobOutput { result, artifact })
                },
            )
            .await
            .map_err(map_forecast_error)
    }

    async fn replay_forecast_with_hash(
        &self,
        request_hash: Sha256Digest,
        request: &TypedToolRequest,
        context: &RequestContext,
        precommit: Option<&dyn ForecastPrecommitAuthority>,
    ) -> Result<Option<ForecastJobOutput>, ServiceError> {
        let forecasts = self.forecasts.as_ref().ok_or(ServiceError::Unavailable)?;
        ensure_request_live(context, &self.lifecycle)?;
        let Some(existing) = forecasts
            .vintage_for_request(
                request_hash,
                &market_squawk_services::ArtifactReadContext::new(
                    context.cancellation().clone(),
                    context.deadline(),
                ),
            )
            .await
            .map_err(map_forecast_error)?
        else {
            return Ok(None);
        };
        let artifact = forecasts
            .read_vintage_artifact(
                &existing,
                &ArtifactPublicationContext::new(
                    context.cancellation().clone(),
                    context.deadline(),
                ),
            )
            .await
            .map_err(map_forecast_error)?;
        super::outcome::validate_event_for_read(
            self,
            existing.product_token().map_err(map_forecast_error)?,
            context,
        )
        .await
        .map_err(map_forecast_error)?;
        let content = forecasts
            .get_forecast_by_identity(
                &existing.vintage_id,
                &market_squawk_services::ArtifactReadContext::new(
                    context.cancellation().clone(),
                    context.deadline(),
                ),
            )
            .await
            .map_err(map_forecast_error)?;
        let result = one_result(content, request, context)?;
        ensure_request_live(context, &self.lifecycle)?;
        if let Some(precommit) = precommit {
            precommit.validate_precommit().map_err(map_forecast_error)?;
            precommit.commit_succeeded();
        }
        Ok(Some(ForecastJobOutput { result, artifact }))
    }

    pub(in crate::application::model) async fn get_forecast(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let forecasts = self.forecasts.as_ref().ok_or(ServiceError::Unavailable)?;
        let vintage = admitted_vintage_id(request.arguments())?;
        let token = Uuid::parse_str(vintage).map_err(|_| ServiceError::InvalidRequest)?;
        super::outcome::validate_event_for_read(self, token, context)
            .await
            .map_err(map_forecast_error)?;
        one_result(
            forecasts
                .get_forecast(
                    vintage,
                    &market_squawk_services::ArtifactReadContext::new(
                        context.cancellation().clone(),
                        context.deadline(),
                    ),
                )
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
        let requested = request
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
        if requested > 100 || requested > limits.maximum_result_items() {
            return Err(ServiceError::InvalidRequest);
        }
        let maximum = NonZeroUsize::new(requested).ok_or(ServiceError::InvalidRequest)?;
        collection_result(
            forecasts
                .list_forecasts(
                    maximum,
                    request.arguments().get("cursor").and_then(Value::as_str),
                )
                .await
                .map_err(map_forecast_error)?,
            limits,
        )
    }

    pub(in crate::application::model) async fn get_forecast_outcomes(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let forecasts = self.forecasts.as_ref().ok_or(ServiceError::Unavailable)?;
        let vintage = admitted_vintage_id(request.arguments())?;
        let token = Uuid::parse_str(vintage).map_err(|_| ServiceError::InvalidRequest)?;
        super::outcome::validate_event_for_read(self, token, context)
            .await
            .map_err(map_forecast_error)?;
        let limits = admitted_result_limits(request, context)?;
        let requested = request
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
        if requested > 100 || requested > limits.maximum_result_items() {
            return Err(ServiceError::InvalidRequest);
        }
        let maximum = NonZeroUsize::new(requested).ok_or(ServiceError::InvalidRequest)?;
        collection_result(
            forecasts
                .get_forecast_outcomes(
                    vintage,
                    maximum,
                    request.arguments().get("cursor").and_then(Value::as_str),
                    &market_squawk_services::ArtifactReadContext::new(
                        context.cancellation().clone(),
                        context.deadline(),
                    ),
                )
                .await
                .map_err(map_forecast_error)?,
            limits,
        )
    }
}

#[async_trait::async_trait]
impl super::ForecastJobExecutor for ModelDomainService {
    async fn recover_forecast_job_output(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<Option<ForecastJobOutput>, ServiceError> {
        let coordinates = forecast_recovery_coordinates(request)?;
        let _call = DomainLifecycle::enter(&self.lifecycle, context)?;
        self.replay_forecast_with_hash(coordinates.request_hash, request, context, None)
            .await
    }

    async fn replay_forecast_for_job(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        precommit: &dyn ForecastPrecommitAuthority,
    ) -> Result<Option<ForecastJobOutput>, ServiceError> {
        let coordinates = forecast_recovery_coordinates(request)?;
        let _call = DomainLifecycle::enter(&self.lifecycle, context)?;
        self.replay_forecast_with_hash(coordinates.request_hash, request, context, Some(precommit))
            .await
    }

    async fn generate_forecast_for_job(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        precommit: &dyn ForecastPrecommitAuthority,
    ) -> Result<ForecastJobOutput, ServiceError> {
        if request.contract().domain() != ServiceDomain::Model
            || request.name() != super::GENERATE_FORECAST
        {
            return Err(ServiceError::InvalidRequest);
        }
        let _call = DomainLifecycle::enter(&self.lifecycle, context)?;
        self.generate_forecast_with_precommit(request, context, Some(precommit))
            .await
    }

    async fn read_forecast_job_result(
        &self,
        request: &TypedToolRequest,
        artifact: &market_squawk_services::ArtifactReference,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let coordinates = forecast_recovery_coordinates(request)?;
        let _call = DomainLifecycle::enter(&self.lifecycle, context)?;
        let forecasts = self.forecasts.as_ref().ok_or(ServiceError::Unavailable)?;
        let vintage = forecasts
            .vintage_for_artifact(
                artifact,
                &market_squawk_services::ArtifactReadContext::new(
                    context.cancellation().clone(),
                    context.deadline(),
                ),
            )
            .await
            .map_err(map_forecast_error)?;
        if vintage.request_hash != super::persistence::hex(coordinates.request_hash.bytes()) {
            return Err(ServiceError::InvalidRequest);
        }
        forecasts
            .read_vintage_artifact(
                &vintage,
                &ArtifactPublicationContext::new(
                    context.cancellation().clone(),
                    context.deadline(),
                ),
            )
            .await
            .map_err(map_forecast_error)?;
        let content = forecasts
            .get_forecast_by_identity(
                &vintage.vintage_id,
                &market_squawk_services::ArtifactReadContext::new(
                    context.cancellation().clone(),
                    context.deadline(),
                ),
            )
            .await
            .map_err(map_forecast_error)?;
        ensure_request_live(context, &self.lifecycle)?;
        TypedToolResult::try_new(
            content,
            1,
            ToolResultMetadata::complete_not_applicable(),
            admitted_result_limits(request, context)?,
        )
        .map_err(Into::into)
    }
}

/// Exact typed immutable coordinates decoded by the same terminal request parser.
pub(crate) struct ForecastRecoveryCoordinates {
    pub(crate) model_id: ModelId,
    pub(crate) bundle_id: BundleId,
    pub(crate) bundle_version: NonZeroU64,
    pub(crate) instrument_id: InstrumentId,
    pub(crate) horizon: ForecastHorizon,
    pub(crate) validity_nanos: NonZeroU64,
    pub(crate) request_hash: Sha256Digest,
    pub(crate) analysis_evidence: ForecastAnalysisEvidence,
    pub(crate) serving_evidence: ForecastServingEvidence,
}

/// Exposes checked recovery coordinates without a second request decoder or a latest lookup.
pub(crate) fn forecast_recovery_coordinates(
    request: &TypedToolRequest,
) -> Result<ForecastRecoveryCoordinates, ServiceError> {
    if request.contract().domain() != ServiceDomain::Model
        || request.name() != super::GENERATE_FORECAST
    {
        return Err(ServiceError::InvalidRequest);
    }
    let model_id = admitted_model_id(request.arguments())?;
    let parsed = ParsedForecastRequest::try_from(
        request
            .arguments()
            .get("request")
            .and_then(Value::as_object)
            .ok_or(ServiceError::InvalidRequest)?,
    )?;
    let request_hash = parsed.request_hash(model_id)?;
    Ok(ForecastRecoveryCoordinates {
        model_id,
        bundle_id: parsed.bundle_id,
        bundle_version: parsed.bundle_version,
        instrument_id: parsed.instrument_id,
        horizon: parsed.horizon,
        validity_nanos: NonZeroU64::new(parsed.validity_nanos)
            .ok_or(ServiceError::InvalidRequest)?,
        request_hash,
        analysis_evidence: parsed.analysis_evidence,
        serving_evidence: parsed.serving_evidence,
    })
}

fn collection_result(
    collection: ForecastCollection,
    limits: ServiceLimits,
) -> Result<TypedToolResult, ServiceError> {
    let (content, returned, available) = collection.into_parts();
    let metadata = if returned < available {
        ToolResultMetadata::try_truncated_not_applicable(available)?
    } else {
        ToolResultMetadata::complete_not_applicable()
    };
    TypedToolResult::try_new(content, returned, metadata, limits).map_err(Into::into)
}

struct ParsedForecastRequest {
    instrument_id: InstrumentId,
    product_identity: ForecastProductIdentity,
    model_evidence: ForecastModelEvidenceProjection,
    bundle_id: BundleId,
    bundle_version: NonZeroU64,
    observed_cutoff: Option<Timestamp>,
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
        hash_bytes(&mut digest, self.product_identity.display_name().as_bytes())?;
        match self.product_identity.canonical_symbol() {
            Some(symbol) => {
                digest.update([1]);
                hash_bytes(&mut digest, symbol.as_bytes())?;
            }
            None => digest.update([0]),
        }
        hash_bytes(&mut digest, self.product_identity.description().as_bytes())?;
        hash_bytes(
            &mut digest,
            self.product_identity.quote_currency().as_str().as_bytes(),
        )?;
        digest.update(
            self.product_identity
                .knowledge_at()
                .unix_nanos()
                .to_be_bytes(),
        );
        digest.update(
            self.product_identity
                .effective_at()
                .unix_nanos()
                .to_be_bytes(),
        );
        digest.update(self.model_evidence.model_token().as_bytes());
        hash_bytes(
            &mut digest,
            self.model_evidence.overall().as_str().as_bytes(),
        )?;
        hash_bytes(
            &mut digest,
            self.model_evidence.pit_inputs().as_str().as_bytes(),
        )?;
        hash_bytes(
            &mut digest,
            self.model_evidence.out_of_sample().as_str().as_bytes(),
        )?;
        hash_bytes(
            &mut digest,
            self.model_evidence.horizon_alignment().as_str().as_bytes(),
        )?;
        hash_bytes(
            &mut digest,
            self.model_evidence.calibration().as_str().as_bytes(),
        )?;
        hash_bytes(&mut digest, self.model_evidence.interpretation().as_bytes())?;
        digest.update(self.bundle_id.as_str().as_bytes());
        digest.update([0]);
        digest.update(self.bundle_version.get().to_be_bytes());
        hash_bytes(
            &mut digest,
            &serde_json::to_vec(&self.observed_cutoff.map(|time| time.unix_nanos()))
                .map_err(invalid)?,
        )?;
        digest.update(self.available_at.unix_nanos().to_be_bytes());
        digest.update(self.horizon.points().get().to_be_bytes());
        hash_bytes(
            &mut digest,
            &serde_json::to_vec(&(self.horizon.step_nanos(), self.horizon.fiscal_periods()))
                .map_err(invalid)?,
        )?;
        digest.update([self.decimal_scale]);
        digest.update(self.validity_nanos.to_be_bytes());
        hash_manifest(&mut digest, self.analysis_evidence.manifest())?;
        digest.update(self.analysis_evidence.production_identity_sha256().bytes());
        digest.update(self.analysis_evidence.production_receipt_sha256().bytes());
        digest.update(self.analysis_evidence.pairing_sha256().bytes());
        hash_bytes(
            &mut digest,
            &serde_json::to_vec(&super::persistence::serving_evidence_record(
                &self.serving_evidence,
            ))
            .map_err(invalid)?,
        )?;
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

/// Applies the same closed request decoder at descriptor admission and forecast execution.
pub(crate) fn validate_forecast_request(input: &Map<String, Value>) -> Result<(), ServiceError> {
    ParsedForecastRequest::try_from(input).map(|_| ())
}

impl TryFrom<&Map<String, Value>> for ParsedForecastRequest {
    type Error = ServiceError;

    fn try_from(input: &Map<String, Value>) -> Result<Self, Self::Error> {
        const FIELDS: [&str; 16] = [
            "instrumentId",
            "productIdentity",
            "modelEvidence",
            "bundleId",
            "bundleVersion",
            "observedThroughUnixNanos",
            "availableAtUnixNanos",
            "horizonPoints",
            "horizonStepNanos",
            "fiscalHorizon",
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
        let product_identity = parse_product_identity(
            input
                .get("productIdentity")
                .and_then(Value::as_object)
                .ok_or(ServiceError::InvalidRequest)?,
        )?;
        let bundle_id = identifier(input, "bundleId")
            .and_then(|value| BundleId::try_new(value).map_err(invalid))?;
        let bundle_version = unsigned(input, "bundleVersion")
            .and_then(NonZeroU64::new)
            .ok_or(ServiceError::InvalidRequest)?;
        let observed_cutoff = match input.get("observedThroughUnixNanos") {
            Some(Value::Null) => None,
            Some(_) => Some(timestamp(input, "observedThroughUnixNanos")?),
            None => return Err(ServiceError::InvalidRequest),
        };
        let available_at = timestamp(input, "availableAtUnixNanos")?;
        let horizon_points = unsigned(input, "horizonPoints")
            .and_then(|value| u16::try_from(value).ok())
            .and_then(NonZeroU16::new)
            .ok_or(ServiceError::InvalidRequest)?;
        let horizon = match input.get("fiscalHorizon") {
            Some(Value::Null) if observed_cutoff.is_some() => {
                let step = unsigned(input, "horizonStepNanos")
                    .and_then(NonZeroU64::new)
                    .ok_or(ServiceError::InvalidRequest)?;
                ForecastHorizon::try_new(horizon_points, step).map_err(invalid)?
            }
            Some(value)
                if observed_cutoff.is_none()
                    && horizon_points.get() == 1
                    && input.get("horizonStepNanos").is_some_and(Value::is_null) =>
            {
                #[derive(serde::Deserialize)]
                #[serde(rename_all = "camelCase", deny_unknown_fields)]
                struct FiscalHorizon {
                    cadence: market_squawk_domain::FundamentalCadence,
                    periods_ahead: NonZeroU16,
                }
                let fiscal: FiscalHorizon =
                    serde_json::from_value(value.clone()).map_err(invalid)?;
                ForecastHorizon::try_fiscal(fiscal.cadence, fiscal.periods_ahead)
                    .map_err(invalid)?
            }
            _ => return Err(ServiceError::InvalidRequest),
        };
        let model_evidence = parse_model_evidence(
            input
                .get("modelEvidence")
                .and_then(Value::as_object)
                .ok_or(ServiceError::InvalidRequest)?,
            horizon,
        )?;
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
                values.len() <= market_squawk_modeling::MAX_FORECAST_OBSERVED_POINTS
                    && (horizon.fiscal_periods().is_none() || values.is_empty())
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
            || product_identity.knowledge_at() != serving_evidence.knowledge_cutoff()
            || observed_cutoff.is_some_and(|cutoff| product_identity.effective_at() != cutoff)
            || (serving_evidence.financial_input().is_some() != horizon.fiscal_periods().is_some())
            || serving_evidence.origin_bar().is_some_and(|bar| {
                bar.context().provenance().instrument_id() != Some(instrument_id)
                    || bar.currency() != product_identity.quote_currency()
            })
        {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(Self {
            instrument_id,
            product_identity,
            model_evidence,
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

fn parse_product_identity(
    input: &Map<String, Value>,
) -> Result<ForecastProductIdentity, ServiceError> {
    const FIELDS: [&str; 6] = [
        "displayName",
        "canonicalSymbol",
        "description",
        "quoteCurrency",
        "knowledgeAtUnixNanos",
        "effectiveAtUnixNanos",
    ];
    if input.len() != FIELDS.len() || input.keys().any(|key| !FIELDS.contains(&key.as_str())) {
        return Err(ServiceError::InvalidRequest);
    }
    let display_name = identifier(input, "displayName")?;
    let canonical_symbol = match input.get("canonicalSymbol") {
        Some(Value::String(value)) => Some(value.clone()),
        Some(Value::Null) => None,
        _ => return Err(ServiceError::InvalidRequest),
    };
    let description = identifier(input, "description")?;
    let encoded_currency = identifier(input, "quoteCurrency")?;
    let quote_currency = Currency::try_from(encoded_currency).map_err(invalid)?;
    if quote_currency.as_str() != encoded_currency {
        return Err(ServiceError::InvalidRequest);
    }
    ForecastProductIdentity::try_new(
        display_name,
        canonical_symbol,
        description,
        quote_currency,
        timestamp(input, "knowledgeAtUnixNanos")?,
        timestamp(input, "effectiveAtUnixNanos")?,
    )
    .map_err(|_| ServiceError::InvalidRequest)
}

fn parse_serving_evidence(
    input: &Map<String, Value>,
) -> Result<ForecastServingEvidence, ServiceError> {
    const FIELDS: [&str; 13] = [
        "currentPriceInput",
        "financialInput",
        "originBar",
        "manifest",
        "parentManifests",
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
    if input
        .get("currentPriceInput")
        .is_some_and(|value| !value.is_null())
    {
        let mut record_fields = input.clone();
        for key in ["knowledgeCutoffUnixNanos", "observedThroughUnixNanos"] {
            record_fields.insert(
                key.to_owned(),
                Value::from(timestamp(input, key)?.unix_nanos()),
            );
        }
        let record = serde_json::from_value(Value::Object(record_fields)).map_err(invalid)?;
        return ForecastServingEvidence::from_current_price_record(record).map_err(invalid);
    }
    if input
        .get("financialInput")
        .is_some_and(|value| !value.is_null())
    {
        let mut record_fields = input.clone();
        record_fields.insert(
            "knowledgeCutoffUnixNanos".to_owned(),
            Value::from(timestamp(input, "knowledgeCutoffUnixNanos")?.unix_nanos()),
        );
        let record = serde_json::from_value(Value::Object(record_fields)).map_err(invalid)?;
        return ForecastServingEvidence::from_financial_record(record).map_err(invalid);
    }
    let parents = input
        .get("parentManifests")
        .and_then(Value::as_array)
        .filter(|parents| {
            !parents.is_empty()
                && parents.len() <= market_squawk_modeling::MAX_FORECAST_SERVING_PARENTS
        })
        .ok_or(ServiceError::InvalidRequest)?
        .iter()
        .map(|parent| {
            parse_analysis_manifest(parent.as_object().ok_or(ServiceError::InvalidRequest)?)
        })
        .collect::<Result<Vec<_>, _>>()?;
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
    .and_then(|evidence| {
        evidence.with_origin_bar(
            serde_json::from_value(
                input
                    .get("originBar")
                    .cloned()
                    .ok_or(ForecastApplicationError::InvalidRecord)?,
            )
            .map_err(|_| ForecastApplicationError::InvalidRecord)?,
        )
    })
    .and_then(|evidence| evidence.with_parent_manifests(parents))
    .map_err(|_| ServiceError::InvalidRequest)
}

fn parse_model_evidence(
    input: &Map<String, Value>,
    selected_horizon: ForecastHorizon,
) -> Result<ForecastModelEvidenceProjection, ServiceError> {
    const FIELDS: [&str; 7] = [
        "modelToken",
        "overall",
        "pitInputs",
        "outOfSample",
        "horizonAlignment",
        "calibration",
        "interpretation",
    ];
    if input.len() != FIELDS.len() || input.keys().any(|key| !FIELDS.contains(&key.as_str())) {
        return Err(ServiceError::InvalidRequest);
    }
    let model_token = identifier(input, "modelToken")
        .and_then(|value| Uuid::parse_str(value).map_err(invalid))?;
    ForecastModelEvidenceProjection::try_from_product_fields_for_horizon(
        model_token,
        selected_horizon,
        identifier(input, "overall")?,
        identifier(input, "pitInputs")?,
        identifier(input, "outOfSample")?,
        identifier(input, "horizonAlignment")?,
        identifier(input, "calibration")?,
        identifier(input, "interpretation")?,
    )
    .ok_or(ServiceError::InvalidRequest)
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

/// Reuses the existing immutable feature-only query for native monetary serving and recovery.
pub(crate) async fn reopen_financial_input(
    analytical: &AnalyticalReadCapability,
    manifest: &DatasetManifestRef,
    deadline: Instant,
    cancellation: tokio_util::sync::CancellationToken,
) -> Result<FeatureDatasetInputEpochOutput, ServiceError> {
    let limits = QueryLimits::try_new_with_inline_bytes(
        4096,
        32 * 1024 * 1024,
        64 * 1024 * 1024,
        64 * 1024 * 1024,
        1,
        128,
        128,
        Duration::from_secs(30),
    )
    .map_err(|_| ServiceError::InvalidRequest)?;
    analytical
        .feature_dataset_input_epochs(
            FeatureDatasetProductContract::FinancialAmountFiscalPeriodsStudyInputsV1,
            manifest,
            limits,
            deadline,
            cancellation,
        )
        .await
        .map_err(|_| ServiceError::Unavailable)
}

pub(crate) fn financial_coordinate_index(
    output: &FeatureDatasetInputEpochOutput,
    serving: &ForecastServingEvidence,
) -> Result<usize, ServiceError> {
    let input = serving
        .financial_input()
        .ok_or(ServiceError::InvalidRequest)?;
    let mut indices = output
        .epochs()
        .iter()
        .enumerate()
        .filter(|(_, epoch)| epoch.example_id() == input.example_id);
    let (index, _) = indices.next().ok_or(ServiceError::NotFound)?;
    if indices.next().is_some() {
        return Err(ServiceError::InvalidResult);
    }
    Ok(index)
}

pub(crate) fn financial_feature_values(
    metadata: &market_squawk_modeling::ModelMetadata,
    coordinate: FeatureDatasetInputCoordinate<'_>,
) -> Result<Vec<f64>, ServiceError> {
    let epoch = coordinate.epoch();
    if epoch.financial_period().is_none() || coordinate.rows().len() != metadata.features().len() {
        return Err(ServiceError::InvalidRequest);
    }
    let mut values = Vec::new();
    values
        .try_reserve_exact(metadata.features().len())
        .map_err(|_| ServiceError::ResourceExhausted)?;
    for binding in metadata.features() {
        let mut rows = coordinate.rows().iter().filter(|row| {
            row.example_id() == epoch.example_id()
                && row.instrument_id() == epoch.instrument_id()
                && row.source_selection_as_of() == epoch.source_selection_as_of()
                && row.decision_coordinate() == epoch.decision_coordinate()
                && row.label_selection_as_of().is_none()
                && row.target_coordinate_kind() == 4
                && row.observed_effective_at().is_none()
                && row.label_effective_at().is_none()
                && row.component_kind() == 1
                && row.component_name() == binding.key().name()
                && row.component_version() == binding.key().version().get()
        });
        let row = rows.next().ok_or(ServiceError::Unavailable)?;
        if rows.next().is_some() {
            return Err(ServiceError::InvalidResult);
        }
        let value = match row.value() {
            ForecastFeatureValue::Float(value) => *value,
            ForecastFeatureValue::Decimal { mantissa, scale } => {
                *mantissa as f64 / 10_f64.powi(i32::from(*scale))
            }
            ForecastFeatureValue::Missing => return Err(ServiceError::Unavailable),
        };
        if !value.is_finite() {
            return Err(ServiceError::InvalidResult);
        }
        values.push(value);
    }
    Ok(values)
}

/// Binds the actual selected model training generation to one independently admitted native input.
pub(crate) fn financial_analysis_evidence(
    metadata: &market_squawk_modeling::ModelMetadata,
    input: &market_squawk_data::AnalyticalFeatureDataset,
) -> Result<ForecastAnalysisEvidence, ServiceError> {
    if input.product_contract()
        != FeatureDatasetProductContract::FinancialAmountFiscalPeriodsStudyInputsV1
        || !matches!(
            metadata.output_binding().measurement(),
            market_squawk_modeling::ForecastMeasurement::FinancialAmount { .. }
        )
        || !matches!(
            metadata.output_binding().target(),
            market_squawk_modeling::ForecastTargetMeaning::FinancialPeriod { .. }
        )
    {
        return Err(ServiceError::InvalidRequest);
    }
    let receipt = input.production_receipt();
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/native-fiscal-training-serving-pairing/v1\0");
    for identity in [
        metadata.metadata_hash(),
        metadata.training_run_hash(),
        metadata.output_binding().identity(),
        metadata.dataset().export_digest(),
        metadata.dataset().selection_digest(),
        input.universe_digest(),
    ] {
        digest.update(identity.bytes());
    }
    for manifest in [metadata.dataset().manifest(), input.generation().manifest()] {
        let record =
            market_squawk_modeling::ForecastArtifactManifestRecord::from_manifest(manifest);
        hash_bytes(&mut digest, &serde_json::to_vec(&record).map_err(invalid)?)?;
    }
    digest.update(receipt.production_identity().bytes());
    digest.update(receipt.receipt_sha256().bytes());
    ForecastAnalysisEvidence::try_new(
        input.generation().manifest().clone(),
        Sha256Digest::new(receipt.production_identity().bytes()),
        Sha256Digest::new(receipt.receipt_sha256().bytes()),
        Sha256Digest::new(digest.finalize().into()),
    )
    .map_err(invalid)
}
