//! Closed installed-service MCP resource resolution.

use std::{num::NonZeroUsize, sync::Arc};

use async_trait::async_trait;
use market_squawk_jobs::{JobRepository, JobRepositoryError, SqliteJobRepository};
use market_squawk_mcp::{
    McpResourceDocument, McpResourceError, McpResourceProvider, McpResourceRequest,
};
use market_squawk_runtime::RuntimeIdentity;
use market_squawk_services::{
    ArtifactAuthority, ArtifactError, ArtifactReadContext, ArtifactReadRequest,
    ArtifactResolveRequest, RequestContext, ServiceError, ServiceErrorClass,
};
use serde_json::{Map, Value, json};

use crate::application::Application;

/// Resource projection over the same application, job, and artifact authorities as tool calls.
pub(super) struct InstalledResourceProvider {
    runtime: RuntimeIdentity,
    application: Arc<Application>,
    jobs: Arc<SqliteJobRepository>,
    artifacts: Arc<dyn ArtifactAuthority>,
}

impl InstalledResourceProvider {
    pub(super) const fn new(
        runtime: RuntimeIdentity,
        application: Arc<Application>,
        jobs: Arc<SqliteJobRepository>,
        artifacts: Arc<dyn ArtifactAuthority>,
    ) -> Self {
        Self {
            runtime,
            application,
            jobs,
            artifacts,
        }
    }

    async fn application_resource(
        &self,
        operation: &'static str,
        arguments: Map<String, Value>,
        context: RequestContext,
    ) -> Result<McpResourceDocument, McpResourceError> {
        self.application
            .invoke(operation, arguments, context)
            .await
            .map(market_squawk_services::TypedToolResult::into_envelope)
            .map_err(map_service_error)
            .and_then(|value| McpResourceDocument::try_new(value, 1))
    }

    async fn artifact_resource(
        &self,
        id: &str,
        context: RequestContext,
    ) -> Result<McpResourceDocument, McpResourceError> {
        let maximum = NonZeroUsize::new(context.limits().maximum_result_bytes())
            .ok_or(McpResourceError::InvalidDocument)?;
        let reference = self
            .artifacts
            .resolve(
                ArtifactResolveRequest::try_new(id, maximum).map_err(map_artifact_error)?,
                ArtifactReadContext::new(context.cancellation().clone(), context.deadline()),
            )
            .await
            .map_err(map_artifact_error)?;
        let request =
            ArtifactReadRequest::try_new(reference.clone(), maximum).map_err(map_artifact_error)?;
        self.artifacts
            .read(
                request,
                ArtifactReadContext::new(context.cancellation().clone(), context.deadline()),
            )
            .await
            .map_err(map_artifact_error)?;
        McpResourceDocument::try_new(
            json!({
                "artifact": reference,
                "integrity": "verified",
            }),
            1,
        )
    }
}

impl std::fmt::Debug for InstalledResourceProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InstalledResourceProvider")
            .field("runtime", &self.runtime)
            .field("application", &"[APPLICATION AUTHORITY]")
            .field("jobs", &"[DURABLE JOB AUTHORITY]")
            .field("artifacts", &"[ARTIFACT AUTHORITY]")
            .finish()
    }
}

#[async_trait]
impl McpResourceProvider for InstalledResourceProvider {
    async fn read(
        &self,
        request: McpResourceRequest,
        context: RequestContext,
    ) -> Result<McpResourceDocument, McpResourceError> {
        ensure_live(&context)?;
        match request {
            McpResourceRequest::Service => McpResourceDocument::try_new(
                json!({
                    "installationId": self.runtime.installation_id(),
                    "serviceGeneration": self.runtime.service_generation(),
                    "status": "ready",
                }),
                1,
            ),
            McpResourceRequest::Workspace => McpResourceDocument::try_new(
                json!({
                    "workspaceId": self.runtime.workspace_id(),
                    "status": "active",
                }),
                1,
            ),
            McpResourceRequest::Source(source_id) => {
                let mut arguments = Map::new();
                arguments.insert("sourceCoverage".to_owned(), json!([source_id]));
                self.application_resource("Source.GetStatus", arguments, context)
                    .await
            }
            McpResourceRequest::Model(model_id) => {
                let mut arguments = Map::new();
                arguments.insert("modelId".to_owned(), Value::String(model_id.to_string()));
                self.application_resource("Model.GetMetadata", arguments, context)
                    .await
            }
            McpResourceRequest::Job(identity) => {
                let snapshot = self
                    .jobs
                    .get(identity.job_id(), identity.generation())
                    .await
                    .map_err(map_job_error)?;
                let value = serde_json::to_value(snapshot)
                    .map_err(|_error| McpResourceError::InvalidDocument)?;
                McpResourceDocument::try_new(value, 1)
            }
            McpResourceRequest::Artifact(artifact_id) => {
                self.artifact_resource(&artifact_id, context).await
            }
        }
    }
}

fn ensure_live(context: &RequestContext) -> Result<(), McpResourceError> {
    if context.cancellation().is_cancelled() || std::time::Instant::now() >= context.deadline() {
        Err(McpResourceError::Interrupted)
    } else {
        Ok(())
    }
}

fn map_service_error(error: ServiceError) -> McpResourceError {
    match error.class() {
        ServiceErrorClass::NotFound => McpResourceError::NotFound,
        ServiceErrorClass::Unauthorized => McpResourceError::Unauthorized,
        ServiceErrorClass::Cancelled | ServiceErrorClass::DeadlineExceeded => {
            McpResourceError::Interrupted
        }
        ServiceErrorClass::InvalidRequest
        | ServiceErrorClass::ResourceExhausted
        | ServiceErrorClass::InvalidResult => McpResourceError::InvalidDocument,
        ServiceErrorClass::Unavailable | ServiceErrorClass::Internal => {
            McpResourceError::Unavailable
        }
    }
}

fn map_job_error(error: JobRepositoryError) -> McpResourceError {
    match error {
        JobRepositoryError::NotFound => McpResourceError::NotFound,
        JobRepositoryError::Conflict
        | JobRepositoryError::InvalidTransition
        | JobRepositoryError::Terminal
        | JobRepositoryError::InvalidState => McpResourceError::InvalidDocument,
        JobRepositoryError::Unavailable => McpResourceError::Unavailable,
    }
}

fn map_artifact_error(error: ArtifactError) -> McpResourceError {
    match error {
        ArtifactError::InvalidReference | ArtifactError::NotFound => McpResourceError::NotFound,
        ArtifactError::ReadLimitExceeded => McpResourceError::InvalidDocument,
        ArtifactError::Cancelled | ArtifactError::DeadlineExceeded => McpResourceError::Interrupted,
        ArtifactError::InvalidPublication | ArtifactError::Unavailable => {
            McpResourceError::Unavailable
        }
    }
}
