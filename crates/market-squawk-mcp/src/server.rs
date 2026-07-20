//! rmcp server handler and bounded lifecycle composition.

use std::{
    borrow::Cow,
    io::Write,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::Instant,
};

use market_squawk_services::{
    RequestContext as ServiceRequestContext, RequestId as ServiceRequestId, ServiceCapabilities,
    ServiceError, ServiceErrorClass, ToolDescriptor, ToolServices,
};
use rmcp::{
    ErrorData as McpError, RoleServer, ServerHandler,
    model::{
        CallToolRequestParams, CallToolResult, ErrorCode, Implementation, InitializeRequestParams,
        InitializeResult, ListToolsResult, PaginatedRequestParams, ProtocolVersion, RequestId,
        ServerCapabilities, ServerInfo, ServerJsonRpcMessage, ServerResult, TaskSupport, Tool,
        ToolAnnotations, ToolExecution,
    },
    service::{
        NotificationContext, QuitReason, RequestContext as McpRequestContext, serve_server_with_ct,
    },
};
use serde_json::json;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::sync::CancellationToken;

use crate::{
    ArtifactError, ArtifactPublication, ArtifactRepository, AuditSink, LocalProcessIdentityClass,
    McpLimits,
    protocol::{
        BoundedRmcpTransport, STATE_AWAIT_INITIALIZE, STATE_AWAIT_INITIALIZED, STATE_READY,
        TransportConfig, TransportError, TransportMonitor, input_audit_failed, input_io_failed,
        input_rejected, output_audit_failed, output_io_failed, output_peer_closed,
        output_queue_timed_out, output_write_timed_out,
    },
};

/// Bounded MCP server over one transport-neutral service surface.
pub struct McpServer<S: ToolServices> {
    services: Arc<S>,
    capabilities: ServiceCapabilities,
    tools: Arc<[Tool]>,
    limits: McpLimits,
    audit: Arc<dyn AuditSink>,
    artifacts: Arc<dyn ArtifactRepository>,
}

impl<S: ToolServices> std::fmt::Debug for McpServer<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("McpServer")
            .field("capabilities", &self.capabilities)
            .field("tool_count", &self.tools.len())
            .field("limits", &self.limits)
            .field("audit", &"[AUDIT SINK]")
            .field("artifacts", &"[ARTIFACT REPOSITORY]")
            .finish_non_exhaustive()
    }
}

impl<S: ToolServices> McpServer<S> {
    /// Binds protocol transport to an immutable capability snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError::InvalidComposition`] when the frozen complete capability response
    /// cannot fit within one configured output frame.
    pub fn try_new(
        services: Arc<S>,
        limits: McpLimits,
        audit: Arc<dyn AuditSink>,
        artifacts: Arc<dyn ArtifactRepository>,
    ) -> Result<Self, ServerError> {
        let capabilities = services.capabilities();
        let tools: Arc<[Tool]> = capabilities
            .tools()
            .iter()
            .map(to_rmcp_tool)
            .collect::<Vec<_>>()
            .into();
        validate_capability_response(&tools, limits)?;
        Ok(Self {
            services,
            capabilities,
            tools,
            limits,
            audit,
            artifacts,
        })
    }

    /// Serves one production session over inherited stdin/stdout.
    ///
    /// Inherited stdio does not authenticate the peer; audits record that limitation explicitly.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when framing construction, SDK initialization, or a runtime task
    /// fails.
    pub async fn serve_stdio(
        self,
        cancellation: CancellationToken,
    ) -> Result<ServerExit, ServerError> {
        self.serve_with_io(
            tokio::io::stdin(),
            tokio::io::stdout(),
            cancellation,
            LocalProcessIdentityClass::InheritedStdioUnverified,
        )
        .await
    }

    /// Serves caller-supplied I/O without asserting locality, inheritance, or peer identity.
    ///
    /// This explicit unverified surface supports embedding and deterministic integration tests.
    /// Production local MCP should use [`Self::serve_stdio`].
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when framing construction, SDK initialization, or a runtime task
    /// fails. EOF, cancellation, broken pipe, and configured resource ceilings are controlled
    /// [`ServerExit`] values.
    pub async fn serve_unverified_io<R, W>(
        self,
        reader: R,
        writer: W,
        cancellation: CancellationToken,
    ) -> Result<ServerExit, ServerError>
    where
        R: AsyncRead + Send + Unpin + 'static,
        W: AsyncWrite + Send + Unpin + 'static,
    {
        self.serve_with_io(
            reader,
            writer,
            cancellation,
            LocalProcessIdentityClass::CallerSuppliedIoUnverified,
        )
        .await
    }

    async fn serve_with_io<R, W>(
        self,
        reader: R,
        writer: W,
        cancellation: CancellationToken,
        identity_class: LocalProcessIdentityClass,
    ) -> Result<ServerExit, ServerError>
    where
        R: AsyncRead + Send + Unpin + 'static,
        W: AsyncWrite + Send + Unpin + 'static,
    {
        let initialization_state = Arc::new(AtomicU8::new(STATE_AWAIT_INITIALIZE));
        let handler = ServiceHandler {
            services: self.services,
            capabilities: self.capabilities.clone(),
            tools: Arc::clone(&self.tools),
            limits: self.limits,
            artifacts: self.artifacts,
            initialization_state: Arc::clone(&initialization_state),
            identity_class,
        };
        let (transport, monitor) = BoundedRmcpTransport::new(
            reader,
            writer,
            TransportConfig {
                limits: self.limits,
                cancellation: cancellation.clone(),
                audit: self.audit,
                identity_class,
                capabilities: self.capabilities,
                initialization_state,
            },
        )?;
        let sdk_cancellation = cancellation.child_token();
        let running = match serve_server_with_ct(handler, transport, sdk_cancellation).await {
            Ok(running) => running,
            Err(error) => {
                return controlled_or_initialization_error(&monitor, cancellation, error);
            }
        };
        let reason = running.waiting().await.map_err(ServerError::RuntimeTask)?;
        Ok(exit_from(reason, &monitor, cancellation.is_cancelled()))
    }
}

struct ServiceHandler<S: ToolServices> {
    services: Arc<S>,
    capabilities: ServiceCapabilities,
    tools: Arc<[Tool]>,
    limits: McpLimits,
    artifacts: Arc<dyn ArtifactRepository>,
    initialization_state: Arc<AtomicU8>,
    identity_class: LocalProcessIdentityClass,
}

impl<S: ToolServices> std::fmt::Debug for ServiceHandler<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServiceHandler")
            .field("capabilities", &self.capabilities)
            .field("tool_count", &self.tools.len())
            .field("limits", &self.limits)
            .field("artifacts", &"[ARTIFACT REPOSITORY]")
            .field("initialization_state", &self.initialization_state)
            .field("identity_class", &self.identity_class)
            .finish_non_exhaustive()
    }
}

impl<S: ToolServices> ServiceHandler<S> {
    fn require_ready(&self) -> Result<(), McpError> {
        if self.initialization_state.load(Ordering::Acquire) == STATE_READY {
            Ok(())
        } else {
            Err(McpError::new(
                ErrorCode(-32_002),
                "server initialization is not complete",
                None,
            ))
        }
    }

    async fn execute_tool(
        &self,
        request: CallToolRequestParams,
        context: McpRequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        self.require_ready()?;
        if !self.capabilities.has_tools() {
            return Err(McpError::new(
                ErrorCode::METHOD_NOT_FOUND,
                "tools capability is unavailable",
                None,
            ));
        }
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

        let arguments = request.arguments.unwrap_or_default();
        let service_request = descriptor.admit(arguments).map_err(service_error)?;
        let request_id = service_request_id(&context.id)?;
        let deadline = Instant::now()
            .checked_add(self.limits.request_timeout())
            .ok_or_else(|| McpError::internal_error("request deadline is invalid", None))?;
        let request_cancellation = context.ct.child_token();
        let service_context = ServiceRequestContext::new(
            request_id,
            request_cancellation.clone(),
            deadline,
            self.limits.service_limits(),
        );

        let execution = async {
            let result = self
                .services
                .call(service_request, service_context)
                .await
                .map_err(service_error)?;
            self.render_result(result).await
        };
        tokio::select! {
            biased;
            () = context.ct.cancelled() => {
                request_cancellation.cancel();
                Err(cancelled_error())
            }
            outcome = tokio::time::timeout(self.limits.request_timeout(), execution) => {
                match outcome {
                    Ok(result) => result,
                    Err(_) => {
                        request_cancellation.cancel();
                        Err(deadline_error())
                    }
                }
            }
        }
    }

    async fn render_result(
        &self,
        result: market_squawk_services::TypedToolResult,
    ) -> Result<CallToolResult, McpError> {
        let inline = result.encoded_bytes() <= self.limits.service_limits().maximum_inline_bytes()
            && result.item_count() <= self.limits.service_limits().maximum_inline_items();
        let (structured, _items, _encoded_bytes) = result.into_parts();
        if inline {
            return Ok(structured_result(structured));
        }

        let encoded = serde_json::to_vec(&structured)
            .map_err(|_| McpError::internal_error("result encoding failed", None))?;
        let publication = ArtifactPublication::try_json(encoded).map_err(artifact_error)?;
        let reference = self
            .artifacts
            .publish(publication.clone())
            .await
            .map_err(artifact_error)?;
        if !reference.matches(&publication) {
            return Err(McpError::internal_error(
                "artifact repository returned inconsistent metadata",
                None,
            ));
        }
        let value = serde_json::to_value(reference)
            .map_err(|_| McpError::internal_error("artifact reference encoding failed", None))?;
        Ok(structured_result(json!({ "artifact": value })))
    }
}

impl<S: ToolServices> ServerHandler for ServiceHandler<S> {
    fn get_info(&self) -> ServerInfo {
        let capabilities = if self.capabilities.has_tools() {
            ServerCapabilities::builder().enable_tools().build()
        } else {
            ServerCapabilities::default()
        };
        InitializeResult::new(capabilities)
            .with_protocol_version(ProtocolVersion::LATEST)
            .with_server_info(Implementation::new(
                "market-squawk",
                env!("CARGO_PKG_VERSION"),
            ))
            .with_instructions(match self.identity_class {
                LocalProcessIdentityClass::InheritedStdioUnverified => {
                    "Inherited local stdio; peer identity is unverified. No business-domain tools are present unless explicitly registered."
                }
                LocalProcessIdentityClass::CallerSuppliedIoUnverified => {
                    "Caller-supplied I/O; locality and peer identity are unverified. No business-domain tools are present unless explicitly registered."
                }
            })
    }

    async fn initialize(
        &self,
        _request: InitializeRequestParams,
        _context: McpRequestContext<RoleServer>,
    ) -> Result<InitializeResult, McpError> {
        self.initialization_state
            .compare_exchange(
                STATE_AWAIT_INITIALIZE,
                STATE_AWAIT_INITIALIZED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|_| {
                McpError::new(
                    ErrorCode::INVALID_REQUEST,
                    "server is already initialized",
                    None,
                )
            })?;
        Ok(self.get_info())
    }

    fn on_initialized(
        &self,
        _context: NotificationContext<RoleServer>,
    ) -> impl Future<Output = ()> + Send + '_ {
        std::future::ready(())
    }

    async fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        _context: McpRequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        self.require_ready()?;
        if !self.capabilities.has_tools() {
            return Err(McpError::new(
                ErrorCode::METHOD_NOT_FOUND,
                "tools capability is unavailable",
                None,
            ));
        }
        if request.and_then(|params| params.cursor).is_some() {
            return Err(McpError::invalid_params(
                "tools/list cursor is not supported for the bounded complete list",
                None,
            ));
        }
        Ok(ListToolsResult::with_all_items(
            self.tools.iter().cloned().collect(),
        ))
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
    ) -> Result<CallToolResult, McpError> {
        self.execute_tool(request, context).await
    }
}

fn structured_result(value: serde_json::Value) -> CallToolResult {
    let mut result = CallToolResult::success(Vec::new());
    result.structured_content = Some(value);
    result
}

fn validate_capability_response(tools: &[Tool], limits: McpLimits) -> Result<(), ServerError> {
    let result = ListToolsResult::with_all_items(tools.to_vec());
    let worst_case_id = RequestId::String(Arc::from("\0".repeat(1_024)));
    let message =
        ServerJsonRpcMessage::response(ServerResult::ListToolsResult(result), worst_case_id);
    let mut counter = FrameCounter {
        maximum: limits.maximum_frame_bytes(),
        written: 0,
    };
    serde_json::to_writer(&mut counter, &message).map_err(|_| ServerError::InvalidComposition)
}

struct FrameCounter {
    maximum: usize,
    written: usize,
}

impl Write for FrameCounter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.written = self
            .written
            .checked_add(buffer.len())
            .filter(|written| *written <= self.maximum)
            .ok_or_else(|| std::io::Error::other("MCP capability response exceeds frame"))?;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn to_rmcp_tool(descriptor: &ToolDescriptor) -> Tool {
    let effects = descriptor.effects();
    Tool::new(
        Cow::Owned(descriptor.name().to_owned()),
        Cow::Owned(descriptor.description().to_owned()),
        Arc::new(descriptor.input_schema().clone()),
    )
    .with_annotations(
        ToolAnnotations::new()
            .read_only(effects.read_only())
            .destructive(effects.destructive())
            .idempotent(effects.idempotent())
            .open_world(effects.open_world()),
    )
    .with_execution(ToolExecution::new().with_task_support(TaskSupport::Forbidden))
}

fn service_request_id(id: &rmcp::model::RequestId) -> Result<ServiceRequestId, McpError> {
    match id {
        rmcp::model::RequestId::Number(value) => Ok(ServiceRequestId::Integer(*value)),
        rmcp::model::RequestId::String(value) => ServiceRequestId::try_string(Arc::clone(value))
            .map_err(|_| McpError::invalid_request("request identifier is invalid", None)),
    }
}

fn service_error(error: ServiceError) -> McpError {
    match error.class() {
        ServiceErrorClass::InvalidRequest => {
            McpError::invalid_params("service request is invalid", None)
        }
        ServiceErrorClass::NotFound => McpError::new(
            ErrorCode::METHOD_NOT_FOUND,
            "service operation was not found",
            None,
        ),
        ServiceErrorClass::Unauthorized => McpError::new(
            ErrorCode(-32_003),
            "service request is not authorized",
            None,
        ),
        ServiceErrorClass::ResourceExhausted | ServiceErrorClass::InvalidResult => {
            McpError::new(ErrorCode(-32_010), "service result limit exceeded", None)
        }
        ServiceErrorClass::Cancelled => cancelled_error(),
        ServiceErrorClass::DeadlineExceeded => deadline_error(),
        ServiceErrorClass::Unavailable => {
            McpError::new(ErrorCode(-32_001), "service is unavailable", None)
        }
        ServiceErrorClass::Internal => McpError::internal_error("service failed internally", None),
    }
}

fn cancelled_error() -> McpError {
    McpError::new(ErrorCode(-32_800), "request was cancelled", None)
}

fn deadline_error() -> McpError {
    McpError::new(ErrorCode(-32_008), "request deadline exceeded", None)
}

fn artifact_error(_error: ArtifactError) -> McpError {
    McpError::internal_error("artifact publication failed", None)
}

fn controlled_or_initialization_error(
    monitor: &TransportMonitor,
    cancellation: CancellationToken,
    error: rmcp::service::ServerInitializeError,
) -> Result<ServerExit, ServerError> {
    let exit = exit_from(QuitReason::Closed, monitor, cancellation.is_cancelled());
    if matches!(
        exit,
        ServerExit::Cancelled
            | ServerExit::EndOfInput
            | ServerExit::PeerClosed
            | ServerExit::WriteTimedOut
            | ServerExit::InputRejected
    ) {
        Ok(exit)
    } else {
        Err(ServerError::Initialize(Box::new(error)))
    }
}

fn exit_from(reason: QuitReason, monitor: &TransportMonitor, cancelled: bool) -> ServerExit {
    match monitor.output_state() {
        state if state == output_peer_closed() => ServerExit::PeerClosed,
        state if state == output_write_timed_out() || state == output_queue_timed_out() => {
            ServerExit::WriteTimedOut
        }
        state if state == output_io_failed() => ServerExit::OutputFailed,
        state if state == output_audit_failed() => ServerExit::AuditFailed,
        _ => match monitor.input_state() {
            state if state == input_rejected() => ServerExit::InputRejected,
            state if state == input_io_failed() => ServerExit::InputFailed,
            state if state == input_audit_failed() => ServerExit::AuditFailed,
            _ if cancelled || matches!(reason, QuitReason::Cancelled) => ServerExit::Cancelled,
            _ => ServerExit::EndOfInput,
        },
    }
}

/// Controlled terminal session disposition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServerExit {
    /// Input reached EOF and all bounded shutdown work completed.
    EndOfInput,
    /// Caller cancelled the session.
    Cancelled,
    /// Peer closed the protocol output.
    PeerClosed,
    /// Queue admission or physical output exceeded its deadline.
    WriteTimedOut,
    /// Input exceeded a configured resource ceiling.
    InputRejected,
    /// Input failed outside a controlled EOF.
    InputFailed,
    /// Output failed outside a controlled broken pipe.
    OutputFailed,
    /// Required local audit admission failed closed.
    AuditFailed,
}

/// Server construction or SDK lifecycle failure.
#[derive(Debug, Error)]
pub enum ServerError {
    /// Frozen capabilities cannot be advertised within the configured output frame.
    #[error("MCP capability response exceeds the configured frame")]
    InvalidComposition,
    /// Bounded transport construction failed.
    #[error("bounded MCP transport construction failed")]
    Transport,
    /// Official SDK initialization failed.
    #[error("MCP initialization failed: {0}")]
    Initialize(#[source] Box<rmcp::service::ServerInitializeError>),
    /// Official SDK runtime task failed.
    #[error("MCP runtime task failed: {0}")]
    RuntimeTask(#[source] tokio::task::JoinError),
}

impl From<TransportError> for ServerError {
    fn from(_source: TransportError) -> Self {
        Self::Transport
    }
}
