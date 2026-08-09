//! Named-client stdio relay over the shared authenticated Streamable HTTP endpoint.

use std::{collections::HashMap, fmt, num::NonZeroUsize, sync::Arc};

use async_trait::async_trait;
use market_squawk_runtime::NamedClient;
use market_squawk_services::validate_json_contract;
use rmcp::model::{
    ClientJsonRpcMessage, ClientRequest, JsonRpcMessage, RequestId, ServerJsonRpcMessage,
};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    McpLimits, ServerExit,
    framing::{BoundedFrameReader, Frame, FramingError},
};

/// Exact stable MCP version admitted by the shared service and every installed relay.
pub const MCP_PROTOCOL_VERSION: &str = "2026-07-28";

/// One credential-free request for the installed connector to send to `/mcp`.
pub struct McpRelayExchange {
    protocol_version: Arc<str>,
    method: Arc<str>,
    name: Option<Arc<str>>,
    body: Box<[u8]>,
    maximum_response_bytes: usize,
}

impl McpRelayExchange {
    /// Negotiated MCP version for the `MCP-Protocol-Version` header.
    #[must_use]
    pub fn protocol_version(&self) -> &str {
        &self.protocol_version
    }

    /// Exact JSON-RPC method for the `Mcp-Method` header.
    #[must_use]
    pub fn method(&self) -> &str {
        &self.method
    }

    /// Tool name or resource URI for the `Mcp-Name` header, when applicable.
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Bounded JSON request body.
    #[must_use]
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    /// Hard response-buffer ceiling the connector must enforce while reading HTTP.
    #[must_use]
    pub const fn maximum_response_bytes(&self) -> usize {
        self.maximum_response_bytes
    }
}

impl fmt::Debug for McpRelayExchange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpRelayExchange")
            .field("protocol_version", &self.protocol_version)
            .field("method", &self.method)
            .field("name", &self.name)
            .field("body", &"[JSON BODY REDACTED]")
            .field("maximum_response_bytes", &self.maximum_response_bytes)
            .finish()
    }
}

/// Raw bounded HTTP outcome returned by the installed connector.
pub struct McpRelayResponse {
    status: u16,
    content_type: Option<Arc<str>>,
    body: Box<[u8]>,
}

impl McpRelayResponse {
    /// Creates one syntactically valid HTTP response projection.
    ///
    /// The relay separately validates the MCP-specific status, content type, body ceiling, and
    /// JSON-RPC identity for the originating exchange.
    ///
    /// # Errors
    ///
    /// Returns [`McpRelayResponseError::InvalidStatus`] outside the HTTP status-code range.
    pub fn try_new(
        status: u16,
        content_type: Option<&str>,
        body: Vec<u8>,
    ) -> Result<Self, McpRelayResponseError> {
        if !(100..=599).contains(&status) {
            return Err(McpRelayResponseError::InvalidStatus);
        }
        Ok(Self {
            status,
            content_type: content_type.map(Arc::from),
            body: body.into_boxed_slice(),
        })
    }
}

impl fmt::Debug for McpRelayResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpRelayResponse")
            .field("status", &self.status)
            .field("content_type", &self.content_type)
            .field("body", &"[HTTP BODY REDACTED]")
            .field("body_bytes", &self.body.len())
            .finish()
    }
}

/// Invalid raw response construction.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum McpRelayResponseError {
    /// Status was outside `100..=599`.
    #[error("relay HTTP status is invalid")]
    InvalidStatus,
}

/// Credential-owning installed connector for the shared `/mcp` endpoint.
///
/// Implementations read the current authenticated rendezvous, apply exact Host/Origin and bearer
/// headers internally, enforce [`McpRelayExchange::maximum_response_bytes`] while streaming the
/// response, and never expose the credential to this crate or the stdio peer.
#[async_trait]
pub trait McpRelayTransport: fmt::Debug + Send + Sync + 'static {
    /// Sends one independent authenticated Streamable HTTP exchange.
    async fn exchange(
        &self,
        request: McpRelayExchange,
        cancellation: CancellationToken,
    ) -> Result<McpRelayResponse, McpRelayTransportError>;
}

/// Connector-owned exchange failure without endpoint, credential, or response payload disclosure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum McpRelayTransportError {
    /// Relay request could not be represented by the connector.
    #[error("relay request is invalid")]
    InvalidRequest,
    /// Installed service rejected the connector authority.
    #[error("installed MCP authority rejected the relay")]
    Rejected,
    /// Installed service or its rendezvous was unavailable.
    #[error("installed MCP service is unavailable")]
    Unavailable,
    /// Exchange was cancelled or exceeded its bounded deadline.
    #[error("installed MCP exchange was interrupted")]
    Interrupted,
    /// Connector received an invalid or excessive HTTP response.
    #[error("installed MCP response is invalid")]
    InvalidResponse,
}

/// Stateless stdio adapter for one installer-registered Claude Code or Codex client.
pub struct McpStdioRelay {
    client: NamedClient,
    transport: Arc<dyn McpRelayTransport>,
    limits: McpLimits,
    request_namespace: Arc<str>,
}

impl fmt::Debug for McpStdioRelay {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpStdioRelay")
            .field("client", &self.client)
            .field("transport", &"[AUTHENTICATED MCP CONNECTOR]")
            .field("limits", &self.limits)
            .finish()
    }
}

impl McpStdioRelay {
    /// Creates a relay that owns no application, credential, catalog, protocol server, analytical
    /// engine, paper state, or shared-service lifecycle.
    ///
    /// # Errors
    ///
    /// Returns [`McpRelayError::UnsupportedClient`] for non-relay client classes.
    pub fn try_new(
        client: NamedClient,
        transport: Arc<dyn McpRelayTransport>,
        limits: McpLimits,
    ) -> Result<Self, McpRelayError> {
        if !matches!(client, NamedClient::ClaudeCode | NamedClient::Codex) {
            return Err(McpRelayError::UnsupportedClient);
        }
        Ok(Self {
            client,
            transport,
            limits,
            request_namespace: Arc::from(Uuid::new_v4().simple().to_string()),
        })
    }

    /// Serves one inherited stdio client connection until EOF or cancellation.
    pub async fn serve_stdio(
        self,
        cancellation: CancellationToken,
    ) -> Result<ServerExit, McpRelayError> {
        self.serve_io(tokio::io::stdin(), tokio::io::stdout(), cancellation)
            .await
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
        self.serve_io(reader, writer, cancellation).await
    }

    async fn serve_io<R, W>(
        self,
        reader: R,
        mut writer: W,
        cancellation: CancellationToken,
    ) -> Result<ServerExit, McpRelayError>
    where
        R: AsyncRead + Send + Unpin + 'static,
        W: AsyncWrite + Send + Unpin + 'static,
    {
        let maximum_frame =
            NonZeroUsize::new(self.limits.maximum_frame_bytes()).ok_or(McpRelayError::Framing)?;
        let mut reader = BoundedFrameReader::new(reader, maximum_frame)
            .map_err(|_error| McpRelayError::Framing)?;
        let session = cancellation.child_token();

        let initialization = match next_frame(&mut reader, &session, self.limits).await? {
            Some(frame) => prepare_initialization(frame, &self.request_namespace, self.limits)?,
            None if cancellation.is_cancelled() => return Ok(ServerExit::Cancelled),
            None => return Ok(ServerExit::EndOfInput),
        };
        let PreparedInitialization {
            request_id: initialization_id,
            transport_request_id,
            client_info,
            client_capabilities,
            exchange,
        } = initialization;
        let response = execute_exchange(
            Arc::clone(&self.transport),
            exchange,
            Some(RelayRequestIdentity {
                client: initialization_id.clone(),
                transport: transport_request_id,
            }),
            session.child_token(),
            self.limits,
        )
        .await?;
        let Some(response) = response else {
            return if cancellation.is_cancelled() {
                Ok(ServerExit::Cancelled)
            } else {
                Err(McpRelayError::InvalidResponse)
            };
        };
        let metadata = RelayClientMetadata::from_initialize_response(
            client_info,
            client_capabilities,
            &response,
            &initialization_id,
            self.limits,
        )?;
        if let Some(exit) = write_frame(&mut writer, &response, self.limits).await? {
            return Ok(exit);
        }

        let mut tasks = JoinSet::new();
        let mut pending = HashMap::<RequestId, CancellationToken>::new();
        loop {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => {
                    session.cancel();
                    drain_tasks(&mut tasks, self.limits).await;
                    return Ok(ServerExit::Cancelled);
                }
                completed = tasks.join_next(), if !tasks.is_empty() => {
                    let completed = completed.ok_or(McpRelayError::Task)?
                        .map_err(|_error| McpRelayError::Task)?;
                    if let Some(request_id) = completed.request_id.as_ref() {
                        pending.remove(request_id);
                    }
                    match completed.result {
                        Ok(Some(response)) => {
                            if let Some(exit) = write_frame(&mut writer, &response, self.limits).await? {
                                session.cancel();
                                drain_tasks(&mut tasks, self.limits).await;
                                return Ok(exit);
                            }
                        }
                        Ok(None) => {}
                        Err(error) => {
                            session.cancel();
                            drain_tasks(&mut tasks, self.limits).await;
                            return Err(error);
                        }
                    }
                }
                frame = next_frame(&mut reader, &session, self.limits) => {
                    let Some(frame) = frame? else {
                        session.cancel();
                        drain_tasks(&mut tasks, self.limits).await;
                        return Ok(ServerExit::EndOfInput);
                    };
                    match prepare_message(
                        frame,
                        &metadata,
                        &self.request_namespace,
                        self.limits,
                    )? {
                        PreparedRelayMessage::Initialized => {}
                        PreparedRelayMessage::Cancelled(request_id) => {
                            if let Some(request) = pending.get(&request_id) {
                                request.cancel();
                            }
                        }
                        PreparedRelayMessage::Exchange {
                            request_id,
                            transport_request_id,
                            exchange,
                        } => {
                            if tasks.len() >= self.limits.maximum_active_requests() {
                                session.cancel();
                                drain_tasks(&mut tasks, self.limits).await;
                                return Ok(ServerExit::InputRejected);
                            }
                            if let Some(request_id) = request_id.as_ref()
                                && pending.contains_key(request_id)
                            {
                                session.cancel();
                                drain_tasks(&mut tasks, self.limits).await;
                                return Ok(ServerExit::InputRejected);
                            }
                            let request_cancellation = session.child_token();
                            if let Some(request_id) = request_id.as_ref() {
                                pending.insert(request_id.clone(), request_cancellation.clone());
                            }
                            let transport = Arc::clone(&self.transport);
                            let limits = self.limits;
                            let response_identity = request_id.clone().zip(transport_request_id);
                            tasks.spawn(async move {
                                let result = execute_exchange(
                                    transport,
                                    exchange,
                                    response_identity.map(|(client, transport)| {
                                        RelayRequestIdentity { client, transport }
                                    }),
                                    request_cancellation,
                                    limits,
                                )
                                .await;
                                ExchangeCompletion { request_id, result }
                            });
                        }
                    }
                }
            }
        }
    }
}

/// Named-client relay construction or lifecycle failure.
#[derive(Debug, Error)]
pub enum McpRelayError {
    /// Only installer-owned Claude Code and Codex registrations may use this adapter.
    #[error("named client is not an MCP relay")]
    UnsupportedClient,
    /// Bounded input framing could not be created or maintained.
    #[error("bounded MCP relay framing failed")]
    Framing,
    /// Input was not a valid bounded MCP client message.
    #[error("MCP relay input was rejected")]
    InvalidInput,
    /// Shared endpoint returned an invalid status, content type, size, or JSON-RPC identity.
    #[error("shared MCP endpoint returned an invalid response")]
    InvalidResponse,
    /// Credential-owning connector failed without exposing sensitive transport details.
    #[error("shared MCP exchange failed")]
    Transport(#[source] McpRelayTransportError),
    /// Relay-owned request task failed.
    #[error("MCP relay request task failed")]
    Task,
}

struct PreparedInitialization {
    request_id: RequestId,
    transport_request_id: RequestId,
    client_info: Value,
    client_capabilities: Value,
    exchange: McpRelayExchange,
}

struct RelayClientMetadata {
    client_info: Value,
    client_capabilities: Value,
}

impl RelayClientMetadata {
    fn from_initialize_response(
        client_info: Value,
        client_capabilities: Value,
        response: &[u8],
        request_id: &RequestId,
        limits: McpLimits,
    ) -> Result<Self, McpRelayError> {
        validate_response_message(response, request_id, limits)?;
        let value: Value =
            serde_json::from_slice(response).map_err(|_error| McpRelayError::InvalidResponse)?;
        if value
            .pointer("/result/protocolVersion")
            .and_then(Value::as_str)
            != Some(MCP_PROTOCOL_VERSION)
        {
            return Err(McpRelayError::InvalidResponse);
        }
        Ok(Self {
            client_info,
            client_capabilities,
        })
    }

    fn attach(&self, value: &mut Value) -> Result<(), McpRelayError> {
        let root = value.as_object_mut().ok_or(McpRelayError::InvalidInput)?;
        let params = root
            .entry("params")
            .or_insert_with(|| Value::Object(Map::new()));
        if params.is_null() {
            *params = Value::Object(Map::new());
        }
        let params = params.as_object_mut().ok_or(McpRelayError::InvalidInput)?;
        let metadata = params
            .entry("_meta")
            .or_insert_with(|| Value::Object(Map::new()));
        if metadata.is_null() {
            *metadata = Value::Object(Map::new());
        }
        let metadata = metadata
            .as_object_mut()
            .ok_or(McpRelayError::InvalidInput)?;
        insert_exact(
            metadata,
            "io.modelcontextprotocol/protocolVersion",
            Value::String(MCP_PROTOCOL_VERSION.to_owned()),
        )?;
        insert_exact(
            metadata,
            "io.modelcontextprotocol/clientInfo",
            self.client_info.clone(),
        )?;
        insert_exact(
            metadata,
            "io.modelcontextprotocol/clientCapabilities",
            self.client_capabilities.clone(),
        )
    }
}

enum PreparedRelayMessage {
    Initialized,
    Cancelled(RequestId),
    Exchange {
        request_id: Option<RequestId>,
        transport_request_id: Option<RequestId>,
        exchange: McpRelayExchange,
    },
}

struct ExchangeCompletion {
    request_id: Option<RequestId>,
    result: Result<Option<Box<[u8]>>, McpRelayError>,
}

#[derive(Clone, Debug)]
struct RelayRequestIdentity {
    client: RequestId,
    transport: RequestId,
}

fn prepare_initialization(
    frame: Vec<u8>,
    request_namespace: &str,
    limits: McpLimits,
) -> Result<PreparedInitialization, McpRelayError> {
    let mut value = parse_message(&frame, limits)?;
    let message: ClientJsonRpcMessage =
        serde_json::from_value(value.clone()).map_err(|_error| McpRelayError::InvalidInput)?;
    let JsonRpcMessage::Request(request) = message else {
        return Err(McpRelayError::InvalidInput);
    };
    let ClientRequest::InitializeRequest(initialize) = request.request else {
        return Err(McpRelayError::InvalidInput);
    };
    let protocol_version: Arc<str> = Arc::from(initialize.params.protocol_version.as_str());
    let client_info = serde_json::to_value(initialize.params.client_info)
        .map_err(|_error| McpRelayError::InvalidInput)?;
    let client_capabilities = serde_json::to_value(initialize.params.capabilities)
        .map_err(|_error| McpRelayError::InvalidInput)?;
    let request_id = request.id;
    let transport_request_id = namespaced_request_id(request_namespace, &request_id)?;
    replace_request_id(&mut value, &transport_request_id)?;
    let body = serde_json::to_vec(&value).map_err(|_error| McpRelayError::InvalidInput)?;
    if body.len() > limits.maximum_body_bytes() {
        return Err(McpRelayError::InvalidInput);
    }
    Ok(PreparedInitialization {
        request_id,
        transport_request_id,
        client_info,
        client_capabilities,
        exchange: McpRelayExchange {
            protocol_version,
            method: Arc::from("initialize"),
            name: None,
            body: body.into_boxed_slice(),
            maximum_response_bytes: limits.maximum_frame_bytes(),
        },
    })
}

fn prepare_message(
    frame: Vec<u8>,
    metadata: &RelayClientMetadata,
    request_namespace: &str,
    limits: McpLimits,
) -> Result<PreparedRelayMessage, McpRelayError> {
    let mut value = parse_message(&frame, limits)?;
    let message: ClientJsonRpcMessage =
        serde_json::from_value(value.clone()).map_err(|_error| McpRelayError::InvalidInput)?;
    let (request_id, method) = match message {
        JsonRpcMessage::Request(request) => (Some(request.id), Arc::<str>::from(method(&value)?)),
        JsonRpcMessage::Notification(_) => (None, Arc::<str>::from(method(&value)?)),
        JsonRpcMessage::Response(_) | JsonRpcMessage::Error(_) => {
            return Err(McpRelayError::InvalidInput);
        }
    };
    if method.as_ref() == "notifications/initialized" {
        return Ok(PreparedRelayMessage::Initialized);
    }
    if method.as_ref() == "notifications/cancelled" {
        let request_id = value
            .pointer("/params/requestId")
            .cloned()
            .ok_or(McpRelayError::InvalidInput)
            .and_then(|value| {
                serde_json::from_value(value).map_err(|_error| McpRelayError::InvalidInput)
            })?;
        return Ok(PreparedRelayMessage::Cancelled(request_id));
    }
    let transport_request_id = request_id
        .as_ref()
        .map(|request_id| namespaced_request_id(request_namespace, request_id))
        .transpose()?;
    if let Some(transport_request_id) = transport_request_id.as_ref() {
        replace_request_id(&mut value, transport_request_id)?;
    }
    metadata.attach(&mut value)?;
    let body = serde_json::to_vec(&value).map_err(|_error| McpRelayError::InvalidInput)?;
    if body.len() > limits.maximum_body_bytes() {
        return Err(McpRelayError::InvalidInput);
    }
    let name = exchange_name(&value, &method).map(Arc::from);
    Ok(PreparedRelayMessage::Exchange {
        request_id,
        transport_request_id,
        exchange: McpRelayExchange {
            protocol_version: Arc::from(MCP_PROTOCOL_VERSION),
            method,
            name,
            body: body.into_boxed_slice(),
            maximum_response_bytes: limits.maximum_frame_bytes(),
        },
    })
}

fn parse_message(frame: &[u8], limits: McpLimits) -> Result<Value, McpRelayError> {
    if frame.len() > limits.maximum_body_bytes() {
        return Err(McpRelayError::InvalidInput);
    }
    let value = serde_json::from_slice(frame).map_err(|_error| McpRelayError::InvalidInput)?;
    validate_json_contract(
        &value,
        limits.input_structure(),
        limits.maximum_body_bytes(),
    )
    .map_err(|_error| McpRelayError::InvalidInput)?;
    Ok(value)
}

fn method(value: &Value) -> Result<&str, McpRelayError> {
    value
        .get("method")
        .and_then(Value::as_str)
        .ok_or(McpRelayError::InvalidInput)
}

fn exchange_name<'a>(value: &'a Value, method: &str) -> Option<&'a str> {
    match method {
        "tools/call" => value.pointer("/params/name").and_then(Value::as_str),
        "resources/read" => value.pointer("/params/uri").and_then(Value::as_str),
        _ => None,
    }
}

fn insert_exact(
    metadata: &mut Map<String, Value>,
    key: &str,
    value: Value,
) -> Result<(), McpRelayError> {
    if metadata.get(key).is_some_and(|existing| existing != &value) {
        return Err(McpRelayError::InvalidInput);
    }
    metadata.insert(key.to_owned(), value);
    Ok(())
}

fn namespaced_request_id(
    request_namespace: &str,
    request_id: &RequestId,
) -> Result<RequestId, McpRelayError> {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let encoded = serde_json::to_vec(request_id).map_err(|_error| McpRelayError::InvalidInput)?;
    let digest = Sha256::digest(encoded);
    let mut namespaced = String::with_capacity(11 + request_namespace.len() + 1 + digest.len() * 2);
    namespaced.push_str("msq-relay:");
    namespaced.push_str(request_namespace);
    namespaced.push(':');
    for byte in digest {
        namespaced.push(char::from(HEX[usize::from(byte >> 4)]));
        namespaced.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(RequestId::String(Arc::from(namespaced)))
}

fn replace_request_id(value: &mut Value, request_id: &RequestId) -> Result<(), McpRelayError> {
    let root = value.as_object_mut().ok_or(McpRelayError::InvalidInput)?;
    if !root.contains_key("id") {
        return Err(McpRelayError::InvalidInput);
    }
    root.insert(
        "id".to_owned(),
        serde_json::to_value(request_id).map_err(|_error| McpRelayError::InvalidInput)?,
    );
    Ok(())
}

async fn execute_exchange(
    transport: Arc<dyn McpRelayTransport>,
    exchange: McpRelayExchange,
    request_identity: Option<RelayRequestIdentity>,
    cancellation: CancellationToken,
    limits: McpLimits,
) -> Result<Option<Box<[u8]>>, McpRelayError> {
    let outcome = tokio::select! {
        biased;
        () = cancellation.cancelled() => return Ok(None),
        outcome = tokio::time::timeout(
            limits.request_timeout(),
            transport.exchange(exchange, cancellation.clone()),
        ) => outcome,
    };
    let response = match outcome {
        Ok(Ok(response)) => response,
        Ok(Err(McpRelayTransportError::Interrupted)) if cancellation.is_cancelled() => {
            return Ok(None);
        }
        Ok(Err(error)) => return Err(McpRelayError::Transport(error)),
        Err(_elapsed) => {
            cancellation.cancel();
            return Err(McpRelayError::Transport(
                McpRelayTransportError::Interrupted,
            ));
        }
    };
    validate_http_response(
        &response,
        request_identity
            .as_ref()
            .map(|identity| &identity.transport),
        limits,
    )?;
    match request_identity {
        Some(identity) => restore_response_identity(response.body, &identity, limits).map(Some),
        None => Ok(None),
    }
}

fn restore_response_identity(
    body: Box<[u8]>,
    identity: &RelayRequestIdentity,
    limits: McpLimits,
) -> Result<Box<[u8]>, McpRelayError> {
    validate_response_message(&body, &identity.transport, limits)?;
    let mut value: Value =
        serde_json::from_slice(&body).map_err(|_error| McpRelayError::InvalidResponse)?;
    let root = value
        .as_object_mut()
        .ok_or(McpRelayError::InvalidResponse)?;
    root.insert(
        "id".to_owned(),
        serde_json::to_value(&identity.client).map_err(|_error| McpRelayError::InvalidResponse)?,
    );
    let restored = serde_json::to_vec(&value).map_err(|_error| McpRelayError::InvalidResponse)?;
    if restored.len() > limits.maximum_frame_bytes() {
        return Err(McpRelayError::InvalidResponse);
    }
    validate_response_message(&restored, &identity.client, limits)?;
    Ok(restored.into_boxed_slice())
}

fn validate_http_response(
    response: &McpRelayResponse,
    request_id: Option<&RequestId>,
    limits: McpLimits,
) -> Result<(), McpRelayError> {
    if response.body.len() > limits.maximum_frame_bytes() {
        return Err(McpRelayError::InvalidResponse);
    }
    match request_id {
        Some(request_id) => {
            if !response
                .content_type
                .as_deref()
                .is_some_and(is_json_content_type)
                || response.body.is_empty()
            {
                return Err(McpRelayError::InvalidResponse);
            }
            let kind = validate_response_message(&response.body, request_id, limits)?;
            match (response.status, kind) {
                (200, _) | (400 | 404, ResponseMessageKind::Error) => Ok(()),
                _ => Err(McpRelayError::InvalidResponse),
            }
        }
        None => {
            if response.status == 202 && response.body.is_empty() {
                Ok(())
            } else {
                Err(McpRelayError::InvalidResponse)
            }
        }
    }
}

fn validate_response_message(
    body: &[u8],
    request_id: &RequestId,
    limits: McpLimits,
) -> Result<ResponseMessageKind, McpRelayError> {
    let value: Value =
        serde_json::from_slice(body).map_err(|_error| McpRelayError::InvalidResponse)?;
    validate_json_contract(
        &value,
        limits.input_structure(),
        limits.maximum_frame_bytes(),
    )
    .map_err(|_error| McpRelayError::InvalidResponse)?;
    let response: ServerJsonRpcMessage =
        serde_json::from_value(value).map_err(|_error| McpRelayError::InvalidResponse)?;
    let (response_id, kind) = match response {
        JsonRpcMessage::Response(response) => (response.id, ResponseMessageKind::Result),
        JsonRpcMessage::Error(error) => (
            error.id.ok_or(McpRelayError::InvalidResponse)?,
            ResponseMessageKind::Error,
        ),
        JsonRpcMessage::Request(_) | JsonRpcMessage::Notification(_) => {
            return Err(McpRelayError::InvalidResponse);
        }
    };
    if &response_id == request_id {
        Ok(kind)
    } else {
        Err(McpRelayError::InvalidResponse)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResponseMessageKind {
    Result,
    Error,
}

fn is_json_content_type(value: &str) -> bool {
    value
        .split(';')
        .next()
        .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("application/json"))
}

async fn next_frame<R>(
    reader: &mut BoundedFrameReader<R>,
    cancellation: &CancellationToken,
    limits: McpLimits,
) -> Result<Option<Vec<u8>>, McpRelayError>
where
    R: AsyncRead + Unpin,
{
    loop {
        match reader.next_frame(cancellation).await {
            Ok(Frame::Message(frame)) if frame.iter().all(u8::is_ascii_whitespace) => {}
            Ok(Frame::Message(frame)) if frame.len() <= limits.maximum_body_bytes() => {
                return Ok(Some(frame.to_vec()));
            }
            Ok(Frame::Message(_)) | Err(FramingError::Oversized { .. }) => {
                return Err(McpRelayError::InvalidInput);
            }
            Ok(Frame::EndOfInput) => return Ok(None),
            Err(FramingError::Cancelled) => return Ok(None),
            Err(FramingError::Io(_))
            | Err(FramingError::InvalidLimit)
            | Err(FramingError::Allocation) => return Err(McpRelayError::Framing),
        }
    }
}

async fn write_frame<W>(
    writer: &mut W,
    body: &[u8],
    limits: McpLimits,
) -> Result<Option<ServerExit>, McpRelayError>
where
    W: AsyncWrite + Unpin,
{
    if body.len() > limits.maximum_frame_bytes() {
        return Err(McpRelayError::InvalidResponse);
    }
    let write = async {
        writer.write_all(body).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await
    };
    match tokio::time::timeout(limits.write_timeout(), write).await {
        Ok(Ok(())) => Ok(None),
        Ok(Err(error)) if error.kind() == std::io::ErrorKind::BrokenPipe => {
            Ok(Some(ServerExit::PeerClosed))
        }
        Ok(Err(_error)) => Ok(Some(ServerExit::OutputFailed)),
        Err(_elapsed) => Ok(Some(ServerExit::WriteTimedOut)),
    }
}

async fn drain_tasks(tasks: &mut JoinSet<ExchangeCompletion>, limits: McpLimits) {
    let drain = async { while tasks.join_next().await.is_some() {} };
    if tokio::time::timeout(limits.shutdown_timeout(), drain)
        .await
        .is_err()
    {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }
}
