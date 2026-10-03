//! Typed retained-token preparation; source acquisition stays in the existing source owner.
use super::forecast::{
    ForecastEvidenceReadContext, ForecastEvidenceReader, MAXIMUM_FORECAST_ARTIFACT_BYTES,
};
use super::{
    ModelDomainService, admitted_result_limits, encode_hex, ensure_request_live,
    map_forecast_selection_error,
};
use crate::application::research::corporate_actions::ForecastOutcomeSourcePreparation;
use market_squawk_services::{
    ArtifactReadContext, RequestContext, ServiceError, ToolResultMetadata, TypedToolRequest,
    TypedToolResult,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::num::NonZeroUsize;
use uuid::Uuid;
pub(crate) const PREPARE_FORECAST_OUTCOME: &str = "Model.PrepareForecastOutcome";
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PrepareForecastOutcomeInput {
    forecast_token: Uuid,
}
impl ModelDomainService {
    pub(super) async fn prepare_forecast_outcome(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        ensure_request_live(context, &self.lifecycle)?;
        if request.arguments().get("confirm") != Some(&Value::Bool(true)) {
            return Err(ServiceError::Unauthorized);
        }
        let limits = admitted_result_limits(request, context)?;
        let mut arguments = request.arguments().clone();
        arguments.remove("confirm");
        arguments.remove("resultLimits");
        let input: PrepareForecastOutcomeInput = serde_json::from_value(Value::Object(arguments))
            .map_err(|_| ServiceError::InvalidRequest)?;
        let analytical = self
            .forecast_analytical
            .as_ref()
            .ok_or(ServiceError::Unavailable)?;
        let read_context = ForecastEvidenceReadContext::new(
            ArtifactReadContext::new(context.cancellation().clone(), context.deadline()),
            NonZeroUsize::new(MAXIMUM_FORECAST_ARTIFACT_BYTES)
                .ok_or(ServiceError::ResourceExhausted)?,
        );
        let event_preparation = super::forecast::prepare_event_outcome(self, input.forecast_token, analytical, &read_context)
            .await.map_err(map_forecast_selection_error)?;
        use super::forecast::EventOutcomePreparation;
        let event_value = match event_preparation {
            EventOutcomePreparation::NotEvent => None,
            EventOutcomePreparation::NotYetMature => Some(json!({"forecastToken":input.forecast_token,"state":"unavailable","measurement":null,"reason":"not_yet_completed"})),
            EventOutcomePreparation::Unavailable => Some(json!({"forecastToken":input.forecast_token,"state":"unavailable","measurement":null,"reason":"source_unavailable"})),
            EventOutcomePreparation::Prepared { manifest, as_of } => Some(json!({
                "forecastToken":input.forecast_token,"state":"prepared","measurement":{
                    "forecastToken":input.forecast_token,
                    "outcomeManifest":{"dataset":manifest.dataset_id().as_str(),"manifestVersion":manifest.manifest_version(),
                        "schema":{"name":manifest.schema().name(),"version":manifest.schema().version().get(),"fingerprint":encode_hex(manifest.schema().fingerprint())},
                        "contentHash":encode_hex(manifest.content_hash().bytes())},
                    "asOfUnixNanos":as_of.unix_nanos().to_string()
                }
            })),
        };
        if let Some(value) = event_value {
            return TypedToolResult::try_new(value, 1, ToolResultMetadata::complete_not_applicable(), limits).map_err(Into::into);
        }
        let sources = self
            .forecast_outcome_preparation
            .as_ref()
            .ok_or(ServiceError::Unavailable)?;
        let origin = ForecastEvidenceReader::outcome_preparation_origin(
            self,
            input.forecast_token,
            analytical,
            read_context,
        )
        .await
        .map_err(map_forecast_selection_error)?;
        let prepared = match origin {
            Some(origin) => sources.prepare_outcome_measurement(origin, context).await?,
            None => ForecastOutcomeSourcePreparation::SourceUnavailable,
        };
        let unavailable = |reason| json!({"forecastToken":input.forecast_token,"state":"unavailable","measurement":null,"reason":reason});
        let value = match prepared {
            ForecastOutcomeSourcePreparation::NotYetCompleted => unavailable("not_yet_completed"),
            ForecastOutcomeSourcePreparation::SourceUnavailable => {
                unavailable("source_unavailable")
            }
            ForecastOutcomeSourcePreparation::UnsupportedHistory => {
                unavailable("unsupported_history")
            }
            ForecastOutcomeSourcePreparation::Prepared(prepared) => {
                let manifest = prepared.manifest();
                json!({"forecastToken":input.forecast_token,"state":"prepared","measurement":{
                    "forecastToken":input.forecast_token,
                    "outcomeManifest":{"dataset":manifest.dataset_id().as_str(),"manifestVersion":manifest.manifest_version(),
                        "schema":{"name":manifest.schema().name(),"version":manifest.schema().version().get(),"fingerprint":encode_hex(manifest.schema().fingerprint())},
                        "contentHash":encode_hex(manifest.content_hash().bytes())},
                    "asOfUnixNanos":prepared.cutoff().unix_nanos().to_string(),"sourceActionReference":prepared.reference()
                }})
            }
        };
        TypedToolResult::try_new(
            value,
            1,
            ToolResultMetadata::complete_not_applicable(),
            limits,
        )
        .map_err(Into::into)
    }
}
