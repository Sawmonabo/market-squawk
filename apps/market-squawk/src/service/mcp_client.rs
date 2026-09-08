//! Credential-owning Streamable HTTP client for stateless installed MCP relays.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use futures_util::StreamExt as _;
use market_squawk_mcp::{
    McpRelayExchange, McpRelayResponse, McpRelayTransport, McpRelayTransportError,
};
use market_squawk_platform::SecretValue;
use market_squawk_runtime::RendezvousRecord;
use reqwest::{Client, redirect::Policy};
use tokio_util::sync::CancellationToken;

const MCP_PROTOCOL_VERSION_HEADER: &str = "mcp-protocol-version";
const MCP_METHOD_HEADER: &str = "mcp-method";
const MCP_NAME_HEADER: &str = "mcp-name";

/// Private endpoint and credential authority used by one named stdio relay.
pub(super) struct InstalledMcpRelayTransport {
    http: Client,
    endpoint: String,
    host: String,
    credential: Arc<SecretValue>,
    timeout: Duration,
}

impl InstalledMcpRelayTransport {
    pub(super) fn try_new(
        record: &RendezvousRecord,
        credential: SecretValue,
        timeout: Duration,
    ) -> Result<Self, McpRelayTransportError> {
        if timeout.is_zero() {
            return Err(McpRelayTransportError::InvalidRequest);
        }
        let host = record.endpoint().to_string();
        let http = Client::builder()
            .redirect(Policy::none())
            .no_proxy()
            .timeout(timeout)
            .build()
            .map_err(|_error| McpRelayTransportError::Unavailable)?;
        Ok(Self {
            http,
            endpoint: format!("http://{host}/mcp"),
            host,
            credential: Arc::new(credential),
            timeout,
        })
    }
}

impl std::fmt::Debug for InstalledMcpRelayTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InstalledMcpRelayTransport")
            .field("endpoint", &"[VERIFIED LOOPBACK ENDPOINT]")
            .field("credential", &"[REDACTED]")
            .field("timeout", &self.timeout)
            .finish()
    }
}

#[async_trait]
impl McpRelayTransport for InstalledMcpRelayTransport {
    async fn exchange(
        &self,
        request: McpRelayExchange,
        cancellation: CancellationToken,
    ) -> Result<McpRelayResponse, McpRelayTransportError> {
        if request.body().is_empty() || request.maximum_response_bytes() == 0 {
            return Err(McpRelayTransportError::InvalidRequest);
        }
        let mut builder = self
            .http
            .post(&self.endpoint)
            .header(reqwest::header::HOST, &self.host)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(
                reqwest::header::ACCEPT,
                "application/json, text/event-stream",
            )
            .header(MCP_PROTOCOL_VERSION_HEADER, request.protocol_version())
            .header(MCP_METHOD_HEADER, request.method())
            .bearer_auth(self.credential.expose_secret())
            .body(request.body().to_vec());
        if let Some(name) = request.name() {
            builder = builder.header(MCP_NAME_HEADER, name);
        }
        let response = tokio::select! {
            () = cancellation.cancelled() => return Err(McpRelayTransportError::Interrupted),
            result = tokio::time::timeout(self.timeout, builder.send()) => {
                result
                    .map_err(|_error| McpRelayTransportError::Interrupted)?
                    .map_err(|_error| McpRelayTransportError::Unavailable)?
            }
        };
        if matches!(response.status().as_u16(), 401 | 403) {
            return Err(McpRelayTransportError::Rejected);
        }
        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let maximum = request.maximum_response_bytes();
        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = tokio::select! {
            () = cancellation.cancelled() => return Err(McpRelayTransportError::Interrupted),
            next = stream.next() => next,
        } {
            let chunk = chunk.map_err(|_error| McpRelayTransportError::InvalidResponse)?;
            let next = body
                .len()
                .checked_add(chunk.len())
                .ok_or(McpRelayTransportError::InvalidResponse)?;
            if next > maximum {
                return Err(McpRelayTransportError::InvalidResponse);
            }
            body.extend_from_slice(&chunk);
        }
        McpRelayResponse::try_new(status, content_type.as_deref(), body)
            .map_err(|_error| McpRelayTransportError::InvalidResponse)
    }
}
