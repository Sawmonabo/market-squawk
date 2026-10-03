//! Confirmed outcome measurement through the sole retained forecast and source read owners.

use std::num::NonZeroUsize;

use market_squawk_data::{
    DatasetId, DatasetManifestRef, DatasetSchemaRef, DatasetSchemaRegistry, Sha256Digest,
};
use market_squawk_domain::{SchemaVersion, Timestamp};
use market_squawk_services::{
    ArtifactReadContext, RequestContext, ServiceError, ToolResultMetadata, TypedToolRequest,
    TypedToolResult,
};
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

use super::forecast::{
    ForecastEvidenceReadContext, ForecastEvidenceReader, MAXIMUM_FORECAST_ARTIFACT_BYTES,
};
use super::{
    ModelDomainService, admitted_result_limits, ensure_request_live, map_forecast_selection_error,
};
use crate::application::research::corporate_actions::SourceAppliedCorporateActionPlanReference;

pub(crate) const MEASURE_FORECAST_OUTCOME: &str = "Model.MeasureForecastOutcome";

impl ModelDomainService {
    pub(super) async fn measure_forecast_outcome(
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
        let input: MeasureForecastOutcomeInput = serde_json::from_value(Value::Object(arguments))
            .map_err(|_| ServiceError::InvalidRequest)?;
        let as_of = input.as_of()?;
        let manifest = input.outcome_manifest.try_into_domain()?;
        let analytical = self
            .forecast_analytical
            .as_ref()
            .ok_or(ServiceError::Unavailable)?;
        let source_actions = self.forecast_source_actions.as_ref();
        let context = ForecastEvidenceReadContext::new(
            ArtifactReadContext::new(context.cancellation().clone(), context.deadline()),
            NonZeroUsize::new(MAXIMUM_FORECAST_ARTIFACT_BYTES)
                .ok_or(ServiceError::ResourceExhausted)?,
        );
        let measured = ForecastEvidenceReader::measure_outcome(
            self,
            input.forecast_token,
            manifest,
            as_of,
            input.source_action_reference.as_ref(),
            analytical,
            source_actions,
            context,
        )
        .await
        .map_err(map_forecast_selection_error)?;
        // The high-owned measurement returns an explicit unavailable state for an immature or
        // missing completed target. It never substitutes a timestamp or acquires source evidence.
        TypedToolResult::try_new(
            measured.product_value(),
            1,
            ToolResultMetadata::complete_not_applicable(),
            limits,
        )
        .map_err(Into::into)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MeasureForecastOutcomeInput {
    forecast_token: Uuid,
    outcome_manifest: OutcomeManifestInput,
    as_of_unix_nanos: String,
    source_action_reference: Option<SourceAppliedCorporateActionPlanReference>,
}
impl MeasureForecastOutcomeInput {
    fn as_of(&self) -> Result<Timestamp, ServiceError> {
        let nanos = self
            .as_of_unix_nanos
            .parse::<i64>()
            .map_err(|_| ServiceError::InvalidRequest)?;
        if self.as_of_unix_nanos != nanos.to_string() {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(Timestamp::from_unix_nanos(nanos))
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OutcomeManifestInput {
    dataset: String,
    manifest_version: u64,
    schema: OutcomeSchemaInput,
    content_hash: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OutcomeSchemaInput {
    name: String,
    version: u16,
    fingerprint: String,
}
impl OutcomeManifestInput {
    fn try_into_domain(self) -> Result<DatasetManifestRef, ServiceError> {
        let schema = DatasetSchemaRef::try_new(
            self.schema.name,
            SchemaVersion::new(self.schema.version).map_err(|_| ServiceError::InvalidRequest)?,
            parse_digest(&self.schema.fingerprint)?,
        )
        .map_err(|_| ServiceError::InvalidRequest)?;
        DatasetSchemaRegistry::local()
            .resolve(&schema)
            .map_err(|_| ServiceError::InvalidRequest)?;
        DatasetManifestRef::try_new_with_schema(
            DatasetId::try_from(self.dataset.as_str()).map_err(|_| ServiceError::InvalidRequest)?,
            self.manifest_version,
            schema,
            Sha256Digest::new(parse_digest(&self.content_hash)?),
        )
        .map_err(|_| ServiceError::InvalidRequest)
    }
}
fn parse_digest(value: &str) -> Result<[u8; 32], ServiceError> {
    if value.len() != 64 {
        return Err(ServiceError::InvalidRequest);
    }
    let mut output = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let nibble = |byte| match byte {
            b'0'..=b'9' => Ok(byte - b'0'),
            b'a'..=b'f' => Ok(byte - b'a' + 10),
            _ => Err(ServiceError::InvalidRequest),
        };
        output[index] = (nibble(pair[0])? << 4) | nibble(pair[1])?;
    }
    if output == [0; 32] {
        return Err(ServiceError::InvalidRequest);
    }
    Ok(output)
}

pub(crate) fn admit_outcome_manifest(value: &Value) -> Result<(), ServiceError> {
    serde_json::from_value::<OutcomeManifestInput>(value.clone())
        .map_err(|_| ServiceError::InvalidRequest)?
        .try_into_domain()
        .map(|_| ())
}
pub(crate) fn outcome_manifest_schema() -> Value {
    let digest = json!({"type":"string","pattern":"^[0-9a-f]{64}$"});
    json!({"type":"object","additionalProperties":false,
        "required":["dataset","manifestVersion","schema","contentHash"],
        "properties":{
            "dataset":{"type":"string","minLength":1,"maxLength":256},
            "manifestVersion":{"type":"integer","minimum":1},
            "schema":{"type":"object","additionalProperties":false,
                "required":["name","version","fingerprint"],"properties":{
                    "name":{"type":"string","minLength":1,"maxLength":256},
                    "version":{"type":"integer","minimum":1,"maximum":65535},
                    "fingerprint":digest.clone()}},
            "contentHash":digest}})
}

/// Parses inert immutable manifest coordinates; the data owner independently reopens authority.
pub(super) fn parse_outcome_manifest(value: &Value) -> Result<DatasetManifestRef, ServiceError> {
    serde_json::from_value::<OutcomeManifestInput>(value.clone())
        .map_err(|_| ServiceError::InvalidRequest)?.try_into_domain()
}
