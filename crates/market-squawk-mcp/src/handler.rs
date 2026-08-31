//! Reusable stateless MCP handler over bounded product application services.

use std::{borrow::Cow, sync::Arc, time::Instant};

use async_trait::async_trait;
use market_squawk_runtime::WorkspaceId;
use market_squawk_services::{
    ProgressDelivery as ServiceProgressDelivery, ProgressError, ProgressSink,
    RequestContext as ServiceRequestContext, RequestId as ServiceRequestId, RequestOrigin,
    ResultEnvelopeProjection, ServiceCapabilities, ServiceError, ServiceErrorClass, ServiceLimits,
    ToolAuthorization, ToolDescriptor, ToolServices,
};
use rmcp::{
    ErrorData as McpError, RoleServer, ServerHandler,
    model::{
        CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, DiscoverResult,
        ErrorCode, Implementation, InitializeResult, ListResourceTemplatesResult,
        ListResourcesResult, ListToolsResult, NumberOrString, PaginatedRequestParams,
        ProgressNotificationParam, ProgressToken, ProtocolVersion, ReadResourceRequestParams,
        ReadResourceResponse, ReadResourceResult, Resource, ResourceContents, ResourceTemplate,
        ServerCapabilities, ServerInfo, Tool,
    },
    service::RequestContext as McpRequestContext,
};
use thiserror::Error;

use crate::{
    AuditCompletion, AuditError, AuditEvent, AuditOperation, AuditResultClass, AuditSink,
    AuthenticatedMcpClient, LocalProcessIdentityClass, McpLimits, MutationAuditBundle,
    resources::{
        ProductResource, ProductResourceError, stable_resource_templates, stable_resources,
        validate_catalog, validate_descriptor,
    },
};

const STABLE_PROTOCOLS: &[ProtocolVersion] = &[ProtocolVersion::V_2026_07_28];
const MAXIMUM_RESOURCE_BYTES: usize = 64 * 1024;
const MAXIMUM_RESOURCE_ITEMS: usize = 1_000;

/// Frozen factory for independent stateless MCP request handlers.
#[derive(Clone)]
pub struct McpHandlerFactory {
    services: Arc<dyn ToolServices>,
    capabilities: ServiceCapabilities,
    tools: Arc<[Tool]>,
    resources: Arc<[Resource]>,
    resource_templates: Arc<[ResourceTemplate]>,
    resource_limits: ServiceLimits,
    limits: McpLimits,
    audit: Arc<dyn AuditSink>,
    workspace_id: WorkspaceId,
}

impl std::fmt::Debug for McpHandlerFactory {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("McpHandlerFactory")
            .field("capabilities", &self.capabilities)
            .field("tool_count", &self.tools.len())
            .field("resource_count", &self.resources.len())
            .field("resource_template_count", &self.resource_templates.len())
            .field("limits", &self.limits)
            .field("audit", &"[AUDIT SINK]")
            .finish_non_exhaustive()
    }
}

impl McpHandlerFactory {
    /// Freezes the exact product-tool capability projection shared by all requests.
    ///
    /// # Errors
    ///
    /// Returns [`HandlerFactoryError::InvalidComposition`] when the complete discovery surfaces
    /// cannot fit the configured response ceiling.
    pub fn try_new(
        services: Arc<dyn ToolServices>,
        limits: McpLimits,
        audit: Arc<dyn AuditSink>,
        workspace_id: WorkspaceId,
    ) -> Result<Self, HandlerFactoryError> {
        let capabilities = services.capabilities();
        let tools = crate::server::validated_protocol_tools(&capabilities, limits)
            .map_err(|_error| HandlerFactoryError::InvalidComposition)?;
        validate_catalog(&capabilities)
            .map_err(|_error| HandlerFactoryError::InvalidComposition)?;
        let resources = stable_resources();
        let resource_templates = stable_resource_templates();
        let resource_bytes = serde_json::to_vec(&(resources.as_ref(), resource_templates.as_ref()))
            .map_err(|_error| HandlerFactoryError::InvalidComposition)?;
        if resource_bytes.len() > limits.maximum_frame_bytes() {
            return Err(HandlerFactoryError::InvalidComposition);
        }
        let resource_limits = resource_limits(limits.service_limits())
            .map_err(|_error| HandlerFactoryError::InvalidComposition)?;
        Ok(Self {
            services,
            capabilities,
            tools,
            resources,
            resource_templates,
            resource_limits,
            limits,
            audit,
            workspace_id,
        })
    }

    pub(crate) fn create(&self) -> StatelessMcpHandler {
        StatelessMcpHandler {
            services: Arc::clone(&self.services),
            capabilities: self.capabilities.clone(),
            tools: Arc::clone(&self.tools),
            resources: Arc::clone(&self.resources),
            resource_templates: Arc::clone(&self.resource_templates),
            resource_limits: self.resource_limits,
            limits: self.limits,
            audit: Arc::clone(&self.audit),
            workspace_id: self.workspace_id,
        }
    }

    pub(crate) const fn limits(&self) -> McpLimits {
        self.limits
    }
}

/// Invalid reusable handler composition.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum HandlerFactoryError {
    /// A complete discovery/list response exceeds the configured transport ceiling.
    #[error("MCP handler capability response exceeds its configured limit")]
    InvalidComposition,
}

/// One transport-independent stateless protocol handler.
#[derive(Clone)]
pub(crate) struct StatelessMcpHandler {
    services: Arc<dyn ToolServices>,
    capabilities: ServiceCapabilities,
    tools: Arc<[Tool]>,
    resources: Arc<[Resource]>,
    resource_templates: Arc<[ResourceTemplate]>,
    resource_limits: ServiceLimits,
    limits: McpLimits,
    audit: Arc<dyn AuditSink>,
    workspace_id: WorkspaceId,
}

impl std::fmt::Debug for StatelessMcpHandler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StatelessMcpHandler")
            .field("tool_count", &self.tools.len())
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl StatelessMcpHandler {
    async fn execute_tool(
        &self,
        request: CallToolRequestParams,
        context: McpRequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let authenticated = require_authenticated(&context)?;
        let origin = RequestOrigin::try_new(
            self.workspace_id.as_uuid(),
            authenticated.client_id().as_uuid(),
        )
        .map_err(|_error| McpError::internal_error("authenticated identity is invalid", None))?;
        let descriptor = self
            .capabilities
            .find(request.name.as_ref())
            .ok_or_else(|| {
                McpError::new(
                    ErrorCode::METHOD_NOT_FOUND,
                    "service operation is not registered",
                    None,
                )
            })?;
        let progress_token = context.meta.get_progress_token();
        if progress_token.as_ref().is_some_and(|token| {
            progress_token_exceeds(token, self.limits.maximum_progress_token_bytes())
        }) {
            return Err(resource_exhausted("progress token resource limit exceeded"));
        }
        let arguments = request.arguments.unwrap_or_default();
        let request_bytes = serde_json::to_vec(&arguments)
            .map_err(|_error| McpError::invalid_request("request encoding failed", None))?;
        let service_request = descriptor.admit(arguments).map_err(service_error)?;
        let request_id = service_request_id(&context.id)?;
        let operation = AuditOperation::CallTool {
            name: Arc::from(descriptor.name()),
            version: Arc::from(descriptor.version()),
        };
        let mut mutation = if matches!(
            descriptor.contract().authorization(),
            ToolAuthorization::ReadOnly
        ) {
            self.audit
                .record(
                    AuditEvent::admitted(
                        &request_id,
                        LocalProcessIdentityClass::AuthenticatedInstalledClient,
                        operation.clone(),
                        self.limits.service_limits(),
                        &request_bytes,
                    )
                    .map_err(audit_error)?,
                )
                .map_err(audit_error)?;
            None
        } else {
            Some(
                self.audit
                    .reserve_mutation(
                        MutationAuditBundle::new(
                            &request_id,
                            LocalProcessIdentityClass::AuthenticatedInstalledClient,
                            operation.clone(),
                            self.limits.service_limits(),
                            &request_bytes,
                        )
                        .map_err(audit_error)?,
                    )
                    .map_err(audit_error)?,
            )
        };
        let deadline = Instant::now()
            .checked_add(self.limits.request_timeout())
            .ok_or_else(|| McpError::internal_error("request deadline is invalid", None))?;
        let cancellation = context.ct.child_token();
        let service_context = match progress_token {
            Some(token) => ServiceRequestContext::with_progress(
                request_id.clone(),
                cancellation.clone(),
                deadline,
                self.limits.service_limits(),
                self.limits.progress_limits(),
                Arc::new(DirectProgressSink {
                    peer: context.peer.clone(),
                    token,
                    limits: self.limits,
                }),
            ),
            None => ServiceRequestContext::new(
                request_id.clone(),
                cancellation.clone(),
                deadline,
                self.limits.service_limits(),
            ),
        };
        let service_context = service_context.with_origin(origin);
        let progress = service_context.progress().clone();
        let service_outcome = tokio::select! {
            biased;
            () = context.ct.cancelled() => {
                cancellation.cancel();
                Err(ServiceError::Cancelled)
            }
            outcome = tokio::time::timeout(
                self.limits.request_timeout(),
                self.services.call(service_request, service_context),
            ) => match outcome {
                Ok(result) => result,
                Err(_) => {
                    cancellation.cancel();
                    Err(ServiceError::DeadlineExceeded)
                }
            }
        };
        if let Some(reservation) = mutation.as_mut() {
            reservation
                .commit_service(service_result_class(&service_outcome))
                .map_err(audit_error)?;
        }
        let rendered = match service_outcome {
            Ok(result) => self.render_result(descriptor, result),
            Err(error) => tool_execution_error(error),
        };
        progress.close().await.map_err(|_error| {
            McpError::internal_error("progress lifecycle closure failed", None)
        })?;
        let result_class = protocol_result_class(&rendered);
        let completion = AuditCompletion::new(
            &request_id,
            LocalProcessIdentityClass::AuthenticatedInstalledClient,
            operation,
            self.limits.service_limits(),
            b"bounded stateless MCP response",
        )
        .map_err(audit_error)?;
        match mutation.as_mut() {
            Some(reservation) => reservation
                .reserve_delivery(completion)
                .map_err(audit_error)?
                .commit(result_class)
                .map_err(audit_error)?,
            None => self
                .audit
                .reserve_completion(completion)
                .map_err(audit_error)?
                .commit(result_class)
                .map_err(audit_error)?,
        }
        rendered
    }

    fn render_result(
        &self,
        descriptor: &ToolDescriptor,
        result: market_squawk_services::TypedToolResult,
    ) -> Result<CallToolResult, McpError> {
        let limits = self.limits.service_limits();
        result
            .validate_against(limits)
            .and_then(|()| result.validate_for(descriptor))
            .map_err(|_error| service_error(ServiceError::InvalidResult))?;
        let projection = descriptor.result_projection();
        let projected_bytes = result
            .projected_encoded_bytes(projection, limits)
            .map_err(|_error| service_error(ServiceError::InvalidResult))?;
        let inline = projected_bytes <= limits.maximum_inline_bytes()
            && result.item_count() <= limits.maximum_inline_items();
        if inline {
            return Ok(structured_result(result.into_envelope(projection)));
        }
        Err(service_error(ServiceError::ResourceExhausted))
    }

    async fn read_product_resource(
        &self,
        request: ReadResourceRequestParams,
        context: McpRequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, McpError> {
        let authenticated = require_authenticated(&context)?;
        if request.input_responses.is_some() || request.request_state.is_some() {
            return Err(McpError::invalid_params(
                "resource continuation state is not supported",
                None,
            ));
        }
        let resource = ProductResource::try_from_uri(&request.uri).map_err(resource_error)?;
        let descriptor = self
            .capabilities
            .find(resource.operation())
            .ok_or_else(|| McpError::internal_error("resource operation is unavailable", None))?;
        validate_descriptor(descriptor)
            .map_err(|_error| McpError::internal_error("resource operation is invalid", None))?;
        let service_request = descriptor.admit(resource.arguments()).map_err(|_error| {
            McpError::internal_error("resource request mapping is invalid", None)
        })?;
        let request_id = service_request_id(&context.id)?;
        let origin = RequestOrigin::try_new(
            self.workspace_id.as_uuid(),
            authenticated.client_id().as_uuid(),
        )
        .map_err(|_error| McpError::internal_error("authenticated identity is invalid", None))?;
        let operation = AuditOperation::ReadResource {
            name: Arc::from(resource.name()),
            operation: Arc::from(descriptor.name()),
            version: Arc::from(descriptor.version()),
        };
        self.audit
            .record(
                AuditEvent::admitted(
                    &request_id,
                    LocalProcessIdentityClass::AuthenticatedInstalledClient,
                    operation.clone(),
                    self.resource_limits,
                    request.uri.as_bytes(),
                )
                .map_err(audit_error)?,
            )
            .map_err(audit_error)?;
        let deadline = Instant::now()
            .checked_add(self.limits.request_timeout())
            .ok_or_else(|| McpError::internal_error("request deadline is invalid", None))?;
        let cancellation = context.ct.child_token();
        let service_context = ServiceRequestContext::new(
            request_id.clone(),
            cancellation.clone(),
            deadline,
            self.resource_limits,
        )
        .with_origin(origin);
        let service_outcome = tokio::select! {
            biased;
            () = context.ct.cancelled() => {
                cancellation.cancel();
                Err(ServiceError::Cancelled)
            }
            outcome = tokio::time::timeout(
                self.limits.request_timeout(),
                self.services.call(service_request, service_context),
            ) => match outcome {
                Ok(result) => result,
                Err(_) => {
                    cancellation.cancel();
                    Err(ServiceError::DeadlineExceeded)
                }
            }
        };
        let rendered = match service_outcome {
            Ok(result) => self.render_resource_result(resource, descriptor, result),
            Err(error) => Err(resource_service_error(error)),
        };
        let completion = AuditCompletion::new(
            &request_id,
            LocalProcessIdentityClass::AuthenticatedInstalledClient,
            operation,
            self.resource_limits,
            b"bounded provider-neutral MCP resource response",
        )
        .map_err(audit_error)?;
        self.audit
            .reserve_completion(completion)
            .map_err(audit_error)?
            .commit(resource_result_class(&rendered))
            .map_err(audit_error)?;
        rendered
    }

    fn render_resource_result(
        &self,
        resource: ProductResource,
        descriptor: &ToolDescriptor,
        result: market_squawk_services::TypedToolResult,
    ) -> Result<ReadResourceResponse, McpError> {
        result
            .validate_against(self.resource_limits)
            .and_then(|()| result.validate_for(descriptor))
            .map_err(|_error| resource_exhausted("resource result is invalid"))?;
        let projection = descriptor.result_projection();
        if projection != ResultEnvelopeProjection::ProductV1
            || result
                .projected_encoded_bytes(projection, self.resource_limits)
                .map_err(|_error| resource_exhausted("resource result is invalid"))?
                > self.resource_limits.maximum_inline_bytes()
            || result.item_count() > self.resource_limits.maximum_inline_items()
        {
            return Err(resource_exhausted("resource result limit exceeded"));
        }
        let encoded = serde_json::to_string(&result.into_envelope(projection))
            .map_err(|_error| McpError::internal_error("resource encoding failed", None))?;
        Ok(ReadResourceResult::new(vec![
            ResourceContents::text(encoded, resource.uri()).with_mime_type("application/json"),
        ])
        .into())
    }
}

impl ServerHandler for StatelessMcpHandler {
    fn get_info(&self) -> ServerInfo {
        let mut capabilities = ServerCapabilities::builder().enable_resources().build();
        if self.capabilities.has_tools() {
            capabilities = ServerCapabilities::builder()
                .enable_resources()
                .enable_tools()
                .build();
        }
        InitializeResult::new(capabilities)
            .with_protocol_version(ProtocolVersion::V_2026_07_28)
            .with_server_info(Implementation::new(
                "market-squawk",
                env!("CARGO_PKG_VERSION"),
            ))
            .with_instructions(
                "Authenticated stateless access to the installed Market Squawk service.",
            )
    }

    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        Cow::Borrowed(STABLE_PROTOCOLS)
    }

    async fn discover(
        &self,
        context: McpRequestContext<RoleServer>,
    ) -> Result<DiscoverResult, McpError> {
        require_authenticated(&context)?;
        Ok(DiscoverResult::from_server_info(
            STABLE_PROTOCOLS.to_vec(),
            self.get_info(),
        ))
    }

    async fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        context: McpRequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        require_authenticated(&context)?;
        reject_cursor(request)?;
        Ok(ListToolsResult::with_all_items(self.tools.to_vec()))
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.tools
            .iter()
            .find(|tool| tool.name.as_ref() == name)
            .cloned()
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: McpRequestContext<RoleServer>,
    ) -> Result<CallToolResponse, McpError> {
        self.execute_tool(request, context).await.map(Into::into)
    }

    async fn list_resources(
        &self,
        request: Option<PaginatedRequestParams>,
        context: McpRequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, McpError> {
        require_authenticated(&context)?;
        reject_cursor(request)?;
        Ok(ListResourcesResult::with_all_items(self.resources.to_vec()))
    }

    async fn list_resource_templates(
        &self,
        request: Option<PaginatedRequestParams>,
        context: McpRequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, McpError> {
        require_authenticated(&context)?;
        reject_cursor(request)?;
        Ok(ListResourceTemplatesResult::with_all_items(
            self.resource_templates.to_vec(),
        ))
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        context: McpRequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, McpError> {
        self.read_product_resource(request, context).await
    }
}

#[derive(Clone)]
struct DirectProgressSink {
    peer: rmcp::service::Peer<RoleServer>,
    token: ProgressToken,
    limits: McpLimits,
}

impl std::fmt::Debug for DirectProgressSink {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DirectProgressSink")
            .field("peer", &"[MCP PEER]")
            .field("token", &"[PROGRESS TOKEN REDACTED]")
            .field("limits", &self.limits)
            .finish()
    }
}

#[async_trait]
impl ProgressSink for DirectProgressSink {
    async fn report(&self, delivery: ServiceProgressDelivery) -> Result<(), ProgressError> {
        delivery.ensure_live()?;
        let update = delivery.update();
        let mut notification =
            ProgressNotificationParam::new(self.token.clone(), update.completed() as f64);
        if let Some(total) = update.total() {
            notification = notification.with_total(total as f64);
        }
        if let Some(message) = update.message() {
            notification = notification.with_message(message);
        }
        let deadline = tokio::time::Instant::from_std(delivery.deadline()).min(
            tokio::time::Instant::now()
                .checked_add(self.limits.write_timeout())
                .ok_or(ProgressError::Delivery)?,
        );
        tokio::select! {
            biased;
            error = delivery.ended() => Err(error),
            result = tokio::time::timeout_at(deadline, self.peer.notify_progress(notification)) => {
                result.map_err(|_error| ProgressError::Delivery)?
                    .map_err(|_error| ProgressError::Delivery)
            }
        }
    }
}

fn reject_cursor(request: Option<PaginatedRequestParams>) -> Result<(), McpError> {
    if request.and_then(|params| params.cursor).is_some() {
        Err(McpError::invalid_params(
            "cursor is not supported for the bounded complete list",
            None,
        ))
    } else {
        Ok(())
    }
}

fn resource_limits(configured: ServiceLimits) -> Result<ServiceLimits, ()> {
    let maximum_bytes = configured
        .maximum_inline_bytes()
        .min(MAXIMUM_RESOURCE_BYTES);
    let maximum_items = configured
        .maximum_inline_items()
        .min(MAXIMUM_RESOURCE_ITEMS);
    ServiceLimits::try_new(
        maximum_bytes,
        maximum_items,
        maximum_bytes,
        maximum_items,
        configured.result_structure(),
    )
    .map_err(|_error| ())
}

fn structured_result(value: serde_json::Value) -> CallToolResult {
    let mut result = CallToolResult::success(Vec::new());
    result.structured_content = Some(value);
    result
}

fn progress_token_exceeds(token: &ProgressToken, maximum_bytes: usize) -> bool {
    matches!(&token.0, NumberOrString::String(value) if value.len() > maximum_bytes)
}

fn service_request_id(id: &rmcp::model::RequestId) -> Result<ServiceRequestId, McpError> {
    match id {
        rmcp::model::RequestId::Number(value) => Ok(ServiceRequestId::Integer(*value)),
        rmcp::model::RequestId::String(value) => ServiceRequestId::try_string(Arc::clone(value))
            .map_err(|_error| McpError::invalid_request("request identifier is invalid", None)),
    }
}

fn service_error(error: ServiceError) -> McpError {
    match error.class() {
        ServiceErrorClass::InvalidRequest => {
            McpError::invalid_params("service request is invalid", None)
        }
        ServiceErrorClass::NotFound => McpError::new(
            ErrorCode::METHOD_NOT_FOUND,
            "service operation not found",
            None,
        ),
        ServiceErrorClass::Unauthorized => McpError::new(
            ErrorCode(-32_003),
            "service request is not authorized",
            None,
        ),
        ServiceErrorClass::ResourceExhausted | ServiceErrorClass::InvalidResult => {
            resource_exhausted("service result limit exceeded")
        }
        ServiceErrorClass::Cancelled => cancelled_error(),
        ServiceErrorClass::DeadlineExceeded => deadline_error(),
        ServiceErrorClass::Unavailable => {
            McpError::new(ErrorCode(-32_001), "service is unavailable", None)
        }
        ServiceErrorClass::Internal => McpError::internal_error("service failed internally", None),
    }
}

fn tool_execution_error(error: ServiceError) -> Result<CallToolResult, McpError> {
    let message = match error.class() {
        ServiceErrorClass::InvalidRequest => "service request is invalid",
        ServiceErrorClass::NotFound => "requested service object was not found",
        ServiceErrorClass::Unauthorized => "service request is not authorized",
        ServiceErrorClass::ResourceExhausted => "service resource limit exceeded",
        ServiceErrorClass::Unavailable => "service is unavailable",
        ServiceErrorClass::Cancelled
        | ServiceErrorClass::DeadlineExceeded
        | ServiceErrorClass::InvalidResult
        | ServiceErrorClass::Internal => return Err(service_error(error)),
    };
    Ok(CallToolResult::error(vec![ContentBlock::text(message)]))
}

fn service_result_class(
    result: &Result<market_squawk_services::TypedToolResult, ServiceError>,
) -> AuditResultClass {
    match result {
        Ok(_) => AuditResultClass::Succeeded,
        Err(error) => match error.class() {
            ServiceErrorClass::Cancelled => AuditResultClass::Cancelled,
            ServiceErrorClass::DeadlineExceeded => AuditResultClass::DeadlineExceeded,
            ServiceErrorClass::ResourceExhausted => AuditResultClass::ResourceExhausted,
            ServiceErrorClass::InvalidRequest
            | ServiceErrorClass::NotFound
            | ServiceErrorClass::Unauthorized
            | ServiceErrorClass::Unavailable
            | ServiceErrorClass::InvalidResult
            | ServiceErrorClass::Internal => AuditResultClass::ServiceRejected,
        },
    }
}

fn protocol_result_class(result: &Result<CallToolResult, McpError>) -> AuditResultClass {
    match result {
        Ok(value) if value.is_error == Some(true) => AuditResultClass::ServiceRejected,
        Ok(_) => AuditResultClass::Succeeded,
        Err(error) if error.code == ErrorCode(-32_800) => AuditResultClass::Cancelled,
        Err(error) if error.code == ErrorCode(-32_008) => AuditResultClass::DeadlineExceeded,
        Err(error) if error.code == ErrorCode(-32_010) => AuditResultClass::ResourceExhausted,
        Err(_) => AuditResultClass::ServiceRejected,
    }
}

fn resource_result_class(result: &Result<ReadResourceResponse, McpError>) -> AuditResultClass {
    match result {
        Ok(_) => AuditResultClass::Succeeded,
        Err(error) if error.code == ErrorCode(-32_800) => AuditResultClass::Cancelled,
        Err(error) if error.code == ErrorCode(-32_008) => AuditResultClass::DeadlineExceeded,
        Err(error) if error.code == ErrorCode(-32_010) => AuditResultClass::ResourceExhausted,
        Err(_) => AuditResultClass::ServiceRejected,
    }
}

fn resource_error(error: ProductResourceError) -> McpError {
    match error {
        ProductResourceError::InvalidUri => {
            McpError::invalid_params("resource URI is invalid", None)
        }
        ProductResourceError::InvalidComposition => {
            McpError::internal_error("resource catalog is invalid", None)
        }
    }
}

fn resource_service_error(error: ServiceError) -> McpError {
    match error.class() {
        ServiceErrorClass::InvalidRequest => {
            McpError::invalid_params("resource request is invalid", None)
        }
        ServiceErrorClass::NotFound => McpError::resource_not_found("resource was not found", None),
        ServiceErrorClass::Unauthorized => {
            McpError::new(ErrorCode(-32_003), "resource is not authorized", None)
        }
        ServiceErrorClass::ResourceExhausted | ServiceErrorClass::InvalidResult => {
            resource_exhausted("resource result limit exceeded")
        }
        ServiceErrorClass::Cancelled => cancelled_error(),
        ServiceErrorClass::DeadlineExceeded => deadline_error(),
        ServiceErrorClass::Unavailable => {
            McpError::new(ErrorCode(-32_001), "resource is unavailable", None)
        }
        ServiceErrorClass::Internal => {
            McpError::internal_error("resource operation failed internally", None)
        }
    }
}

fn cancelled_error() -> McpError {
    McpError::new(ErrorCode(-32_800), "request was cancelled", None)
}

fn deadline_error() -> McpError {
    McpError::new(ErrorCode(-32_008), "request deadline exceeded", None)
}

fn resource_exhausted(message: &'static str) -> McpError {
    McpError::new(ErrorCode(-32_010), message, None)
}

fn audit_error(_error: AuditError) -> McpError {
    McpError::internal_error("request audit failed", None)
}

fn require_authenticated(
    context: &McpRequestContext<RoleServer>,
) -> Result<&AuthenticatedMcpClient, McpError> {
    context
        .extensions
        .get::<axum::http::request::Parts>()
        .and_then(|parts| parts.extensions.get::<AuthenticatedMcpClient>())
        .ok_or_else(|| {
            McpError::new(
                ErrorCode(-32_003),
                "authenticated installed-client identity is required",
                None,
            )
        })
}
