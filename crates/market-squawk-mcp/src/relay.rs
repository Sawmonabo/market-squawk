//! Named-client stdio relay over the single authenticated application client.

use std::{
    fmt,
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use market_squawk_domain::{SourceIdentifier, Timestamp};
use market_squawk_runtime::{
    ApplicationClient, ApplicationClientError, ApplicationRequestScope, NamedClient,
};
use market_squawk_services::{
    RequestContext, ResultCompleteness, ServiceCapabilities, ServiceError, ServiceLimits,
    SourceEvidencePolicy, ToolResultMetadata, ToolServices, TypedToolRequest, TypedToolResult,
};
use serde_json::Value;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::sync::CancellationToken;

use crate::{ArtifactRepository, AuditSink, McpLimits, McpServer, ServerError, ServerExit};

#[derive(Debug)]
struct RelayToolServices {
    client: Arc<dyn ApplicationClient>,
    request_scope: ApplicationRequestScope,
    capabilities: ServiceCapabilities,
}

#[async_trait]
impl ToolServices for RelayToolServices {
    fn capabilities(&self) -> ServiceCapabilities {
        self.capabilities.clone()
    }

    async fn call(
        &self,
        request: TypedToolRequest,
        context: RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let (now, deadline) = wall_deadline(context.deadline())?;
        let operation = SourceIdentifier::try_from(request.name().to_owned())
            .map_err(|_error| ServiceError::InvalidRequest)?;
        let envelope = self
            .request_scope
            .request(
                context.request_id().clone(),
                deadline,
                now,
                operation,
                Value::Object(request.arguments().clone()),
            )
            .map_err(|_error| ServiceError::InvalidRequest)?;
        let expected_request_id = envelope.request_id().clone();
        let expected_generation = envelope.service_generation();
        let response = self
            .client
            .invoke(envelope, context.cancellation().clone())
            .await
            .map_err(application_error)?;
        if response.request_id() != &expected_request_id
            || response.service_generation() != expected_generation
        {
            return Err(ServiceError::InvalidResult);
        }
        decode_result(
            response.result(),
            request.contract().result().source_evidence(),
            context.limits(),
        )
    }
}

/// Stateless stdio adapter for one installer-registered Claude Code or Codex client.
pub struct McpStdioRelay {
    client: NamedClient,
    server: McpServer<RelayToolServices>,
}

impl fmt::Debug for McpStdioRelay {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpStdioRelay")
            .field("client", &self.client)
            .field("server", &"[STATELESS MCP RELAY]")
            .finish()
    }
}

impl McpStdioRelay {
    /// Creates a relay that owns no application, catalog, analytical engine, paper state, or
    /// protocol-session store.
    ///
    /// # Errors
    ///
    /// Returns [`McpRelayError::UnsupportedClient`] for non-relay client classes, or
    /// [`McpRelayError::Server`] when the bounded protocol surface cannot be composed.
    pub fn try_new(
        client: NamedClient,
        application: Arc<dyn ApplicationClient>,
        request_scope: ApplicationRequestScope,
        capabilities: ServiceCapabilities,
        limits: McpLimits,
        audit: Arc<dyn AuditSink>,
        artifacts: Arc<dyn ArtifactRepository>,
    ) -> Result<Self, McpRelayError> {
        if !matches!(client, NamedClient::ClaudeCode | NamedClient::Codex) {
            return Err(McpRelayError::UnsupportedClient);
        }
        let server = McpServer::try_new(
            Arc::new(RelayToolServices {
                client: application,
                request_scope,
                capabilities,
            }),
            limits,
            audit,
            artifacts,
        )?;
        Ok(Self { client, server })
    }

    /// Serves one inherited stdio client connection until EOF or cancellation.
    pub async fn serve_stdio(
        self,
        cancellation: CancellationToken,
    ) -> Result<ServerExit, McpRelayError> {
        self.server
            .serve_stdio(cancellation)
            .await
            .map_err(Into::into)
    }

    /// Serves caller-supplied relay I/O for deterministic integration verification.
    pub async fn serve_unverified_io<R, W>(
        self,
        reader: R,
        writer: W,
        cancellation: CancellationToken,
    ) -> Result<ServerExit, McpRelayError>
    where
        R: AsyncRead + Send + Unpin + 'static,
        W: AsyncWrite + Send + Unpin + 'static,
    {
        self.server
            .serve_unverified_io(reader, writer, cancellation)
            .await
            .map_err(Into::into)
    }
}

/// Named-client relay construction or lifecycle failure.
#[derive(Debug, Error)]
pub enum McpRelayError {
    /// Only installer-owned Claude Code and Codex registrations may use this adapter.
    #[error("named client is not an MCP relay")]
    UnsupportedClient,
    /// Bounded MCP server composition or lifecycle failed.
    #[error("MCP relay server failed")]
    Server(#[from] ServerError),
}

fn application_error(error: ApplicationClientError) -> ServiceError {
    match error {
        ApplicationClientError::Rejected => ServiceError::InvalidRequest,
        ApplicationClientError::Unavailable => ServiceError::Unavailable,
        ApplicationClientError::Interrupted => ServiceError::Cancelled,
        ApplicationClientError::InvalidResponse => ServiceError::InvalidResult,
    }
}

fn wall_deadline(deadline: Instant) -> Result<(Timestamp, Timestamp), ServiceError> {
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or(ServiceError::DeadlineExceeded)?;
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_error| ServiceError::Unavailable)?;
    let now_nanos =
        i64::try_from(elapsed.as_nanos()).map_err(|_error| ServiceError::Unavailable)?;
    let remaining_nanos =
        i64::try_from(remaining.as_nanos()).map_err(|_error| ServiceError::InvalidRequest)?;
    let now = Timestamp::from_unix_nanos(now_nanos);
    let deadline = now
        .checked_add_nanos(remaining_nanos)
        .map_err(|_error| ServiceError::InvalidRequest)?;
    Ok((now, deadline))
}

fn decode_result(
    envelope: &Value,
    evidence: SourceEvidencePolicy,
    limits: ServiceLimits,
) -> Result<TypedToolResult, ServiceError> {
    let object = envelope.as_object().ok_or(ServiceError::InvalidResult)?;
    let data = object
        .get("data")
        .cloned()
        .ok_or(ServiceError::InvalidResult)?;
    let metadata = object
        .get("metadata")
        .and_then(Value::as_object)
        .ok_or(ServiceError::InvalidResult)?;
    let returned = usize::try_from(
        metadata
            .get("returnedItems")
            .and_then(Value::as_u64)
            .ok_or(ServiceError::InvalidResult)?,
    )
    .map_err(|_error| ServiceError::InvalidResult)?;
    let available = usize::try_from(
        metadata
            .get("availableItems")
            .and_then(Value::as_u64)
            .ok_or(ServiceError::InvalidResult)?,
    )
    .map_err(|_error| ServiceError::InvalidResult)?;
    let completeness = match metadata.get("completeness").and_then(Value::as_str) {
        Some("complete") => ResultCompleteness::Complete,
        Some("truncated") => ResultCompleteness::Truncated,
        _ => return Err(ServiceError::InvalidResult),
    };
    if (matches!(completeness, ResultCompleteness::Complete) && available != returned)
        || (matches!(completeness, ResultCompleteness::Truncated) && available <= returned)
    {
        return Err(ServiceError::InvalidResult);
    }
    let result_metadata = match (evidence, completeness) {
        (SourceEvidencePolicy::NotApplicable, ResultCompleteness::Complete) => {
            ToolResultMetadata::complete_not_applicable()
        }
        (SourceEvidencePolicy::NotApplicable, ResultCompleteness::Truncated) => {
            ToolResultMetadata::try_truncated_not_applicable(available)
                .map_err(|_error| ServiceError::InvalidResult)?
        }
        (SourceEvidencePolicy::Required, ResultCompleteness::Complete) => {
            ToolResultMetadata::try_complete(
                metadata
                    .get("sourceCoverage")
                    .cloned()
                    .ok_or(ServiceError::InvalidResult)?,
                metadata
                    .get("dataQuality")
                    .cloned()
                    .ok_or(ServiceError::InvalidResult)?,
            )
            .map_err(|_error| ServiceError::InvalidResult)?
        }
        (SourceEvidencePolicy::Required, ResultCompleteness::Truncated) => {
            ToolResultMetadata::try_truncated(
                available,
                metadata
                    .get("sourceCoverage")
                    .cloned()
                    .ok_or(ServiceError::InvalidResult)?,
                metadata
                    .get("dataQuality")
                    .cloned()
                    .ok_or(ServiceError::InvalidResult)?,
            )
            .map_err(|_error| ServiceError::InvalidResult)?
        }
    };
    TypedToolResult::try_new(data, returned, result_metadata, limits)
        .map_err(|_error| ServiceError::InvalidResult)
}
