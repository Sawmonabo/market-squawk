//! Closed request decoding for service-owned, durable saved-screen execution.

use std::sync::Arc;

use market_squawk_data::{
    AnalyticalReadCapability, DatasetId, DatasetManifestRef, DatasetSchemaRef,
    DatasetSchemaRegistry, Sha256Digest,
};
use market_squawk_decisions::ScreenId;
use market_squawk_domain::{RevisionNumber, SchemaVersion, Timestamp};
use market_squawk_services::{RequestContext, ServiceError, TypedToolRequest};
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::application::decision::{
    AdmittedScreenJob, DecisionApplication, ScreenJobRequest, ScreenWorkflowError,
};

pub(super) const RUN_SCREEN: &str = "Decision.RunScreen";

/// Service-owned preparation over the decision journal and analytical read authority.
pub(super) struct ScreenWorkflowOperations {
    decisions: Arc<DecisionApplication>,
    reader: AnalyticalReadCapability,
}

impl ScreenWorkflowOperations {
    pub(super) const fn new(
        decisions: Arc<DecisionApplication>,
        reader: AnalyticalReadCapability,
    ) -> Self {
        Self { decisions, reader }
    }

    pub(super) async fn prepare(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        selected_at: Timestamp,
    ) -> Result<AdmittedScreenJob, ServiceError> {
        ensure_live(context)?;
        let input: RunScreenRequest = decode(&mutation_arguments(request.arguments()))?;
        let request = ScreenJobRequest::new(
            ScreenId::try_new(input.screen_id).map_err(|_error| ServiceError::InvalidRequest)?,
            RevisionNumber::new(input.screen_revision)
                .map_err(|_error| ServiceError::InvalidRequest)?,
            input.dataset_manifest.try_into_domain()?,
            input.as_of,
        );
        let admitted = self
            .decisions
            .prepare_screen_job(
                request,
                &self.reader,
                selected_at,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(map_workflow)?;
        ensure_live(context)?;
        Ok(admitted)
    }
}

impl std::fmt::Debug for ScreenWorkflowOperations {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ScreenWorkflowOperations")
            .field("decisions", &"[DURABLE DECISION AUTHORITY]")
            .field("reader", &"[BOUNDED ANALYTICAL READ AUTHORITY]")
            .finish()
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RunScreenRequest {
    screen_id: String,
    screen_revision: u32,
    dataset_manifest: ManifestInput,
    as_of: Timestamp,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ManifestInput {
    dataset: String,
    manifest_version: u64,
    schema: SchemaInput,
    content_hash: String,
}

impl ManifestInput {
    fn try_into_domain(self) -> Result<DatasetManifestRef, ServiceError> {
        let schema = DatasetSchemaRef::try_new(
            self.schema.name,
            SchemaVersion::new(self.schema.version).map_err(|_| ServiceError::InvalidRequest)?,
            parse_sha256(&self.schema.fingerprint)?,
        )
        .map_err(|_error| ServiceError::InvalidRequest)?;
        DatasetSchemaRegistry::local()
            .resolve(&schema)
            .map_err(|_error| ServiceError::InvalidRequest)?;
        DatasetManifestRef::try_new_with_schema(
            DatasetId::try_from(self.dataset.as_str())
                .map_err(|_error| ServiceError::InvalidRequest)?,
            self.manifest_version,
            schema,
            Sha256Digest::new(parse_sha256(&self.content_hash)?),
        )
        .map_err(|_error| ServiceError::InvalidRequest)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SchemaInput {
    name: String,
    version: u16,
    fingerprint: String,
}

fn parse_sha256(value: &str) -> Result<[u8; 32], ServiceError> {
    if value.len() != 64 {
        return Err(ServiceError::InvalidRequest);
    }
    let mut bytes = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    if bytes == [0; 32] {
        return Err(ServiceError::InvalidRequest);
    }
    Ok(bytes)
}

fn hex_nibble(value: u8) -> Result<u8, ServiceError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(ServiceError::InvalidRequest),
    }
}

fn decode<T: for<'de> Deserialize<'de>>(arguments: &Map<String, Value>) -> Result<T, ServiceError> {
    serde_json::from_value(Value::Object(arguments.clone()))
        .map_err(|_error| ServiceError::InvalidRequest)
}

fn mutation_arguments(arguments: &Map<String, Value>) -> Map<String, Value> {
    let mut arguments = arguments.clone();
    arguments.remove("confirmation");
    arguments
}

fn ensure_live(context: &RequestContext) -> Result<(), ServiceError> {
    if context.cancellation().is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if std::time::Instant::now() >= context.deadline() {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn map_workflow(error: ScreenWorkflowError) -> ServiceError {
    match error {
        ScreenWorkflowError::InvalidRequest | ScreenWorkflowError::Conflict => {
            ServiceError::InvalidRequest
        }
        ScreenWorkflowError::NotFound => ServiceError::NotFound,
        ScreenWorkflowError::DatasetUnavailable => ServiceError::Unavailable,
        ScreenWorkflowError::Capacity => ServiceError::ResourceExhausted,
        ScreenWorkflowError::Application(error) => super::map_application(error),
    }
}
