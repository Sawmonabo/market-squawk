//! Authenticated, POST-only stateless Streamable HTTP composition.

use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    body::Body,
    http::{
        HeaderMap, Method, Request, Response, StatusCode,
        header::{ALLOW, AUTHORIZATION, HOST, ORIGIN},
    },
};
use http_body_util::BodyExt;
use market_squawk_runtime::{ClientId, CredentialGeneration, NamedClient};
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::never::NeverSessionManager,
};
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::handler::{McpHandlerFactory, StatelessMcpHandler};

const MCP_SESSION_ID: &str = "mcp-session-id";
const LAST_EVENT_ID: &str = "last-event-id";

/// Authenticated installed-client identity with its own request ceiling.
#[derive(Clone)]
pub struct AuthenticatedMcpClient {
    client: NamedClient,
    client_id: ClientId,
    credential_generation: CredentialGeneration,
    requests: Arc<Semaphore>,
    maximum_active_requests: usize,
    telemetry: Arc<ClientRequestTelemetry>,
}

#[derive(Debug, Default)]
struct ClientRequestTelemetry {
    admitted_requests: AtomicU64,
    saturated_requests: AtomicU64,
    initialized_relays: AtomicU64,
    last_activity_unix_seconds: AtomicU64,
}

impl AuthenticatedMcpClient {
    /// Creates one bounded authenticated identity without retaining its credential.
    ///
    /// # Errors
    ///
    /// Returns [`McpHttpAuthError::InvalidIdentity`] for a non-MCP named client or a zero request
    /// ceiling.
    pub fn try_new(
        client: NamedClient,
        client_id: ClientId,
        credential_generation: CredentialGeneration,
        maximum_active_requests: usize,
    ) -> Result<Self, McpHttpAuthError> {
        if !matches!(client, NamedClient::ClaudeCode | NamedClient::Codex)
            || maximum_active_requests == 0
        {
            return Err(McpHttpAuthError::InvalidIdentity);
        }
        Ok(Self {
            client,
            client_id,
            credential_generation,
            requests: Arc::new(Semaphore::new(maximum_active_requests)),
            maximum_active_requests,
            telemetry: Arc::new(ClientRequestTelemetry::default()),
        })
    }

    /// Advances only the credential generation while retaining this client's request ceiling and
    /// telemetry authority.
    pub fn with_credential_generation(
        &self,
        client_id: ClientId,
        credential_generation: CredentialGeneration,
    ) -> Result<Self, McpHttpAuthError> {
        if client_id != self.client_id {
            return Err(McpHttpAuthError::InvalidIdentity);
        }
        Ok(Self {
            client: self.client,
            client_id,
            credential_generation,
            requests: Arc::clone(&self.requests),
            maximum_active_requests: self.maximum_active_requests,
            telemetry: Arc::clone(&self.telemetry),
        })
    }

    /// Registered installed-product client class.
    #[must_use]
    pub const fn client(&self) -> NamedClient {
        self.client
    }

    /// Stable registered client identity.
    #[must_use]
    pub const fn client_id(&self) -> ClientId {
        self.client_id
    }

    /// Exact installed credential generation that admitted the request.
    #[must_use]
    pub const fn credential_generation(&self) -> CredentialGeneration {
        self.credential_generation
    }

    /// Configured simultaneous request ceiling retained across credential rotations.
    #[must_use]
    pub const fn maximum_active_requests(&self) -> usize {
        self.maximum_active_requests
    }

    /// Requests currently retaining this client's semaphore permit.
    #[must_use]
    pub fn active_requests(&self) -> usize {
        self.maximum_active_requests
            .saturating_sub(self.requests.available_permits())
    }

    /// Requests admitted since this installed-service process started.
    #[must_use]
    pub fn admitted_requests(&self) -> u64 {
        self.telemetry.admitted_requests.load(Ordering::Relaxed)
    }

    /// Requests rejected because this client's request ceiling was already exhausted.
    #[must_use]
    pub fn saturated_requests(&self) -> u64 {
        self.telemetry.saturated_requests.load(Ordering::Relaxed)
    }

    /// Stateless relay initialization requests admitted during this service process.
    #[must_use]
    pub fn initialized_relays(&self) -> u64 {
        self.telemetry.initialized_relays.load(Ordering::Relaxed)
    }

    /// Last admitted request activity as Unix seconds, or `None` before the first request.
    #[must_use]
    pub fn last_activity_unix_seconds(&self) -> Option<u64> {
        match self
            .telemetry
            .last_activity_unix_seconds
            .load(Ordering::Relaxed)
        {
            0 => None,
            value => Some(value),
        }
    }

    fn try_acquire(&self) -> Result<OwnedSemaphorePermit, McpHttpAuthError> {
        match Arc::clone(&self.requests).try_acquire_owned() {
            Ok(permit) => {
                increment(&self.telemetry.admitted_requests);
                self.observe_activity();
                Ok(permit)
            }
            Err(_error) => {
                increment(&self.telemetry.saturated_requests);
                self.observe_activity();
                Err(McpHttpAuthError::Saturated)
            }
        }
    }

    fn observe_method(&self, method: Option<&str>) {
        if method == Some("initialize") {
            increment(&self.telemetry.initialized_relays);
        }
    }

    fn observe_activity(&self) {
        if let Ok(elapsed) = SystemTime::now().duration_since(UNIX_EPOCH) {
            self.telemetry
                .last_activity_unix_seconds
                .store(elapsed.as_secs(), Ordering::Relaxed);
        }
    }
}

fn increment(counter: &AtomicU64) {
    let _result = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_add(1))
    });
}

impl fmt::Debug for AuthenticatedMcpClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthenticatedMcpClient")
            .field("client", &self.client)
            .field("client_id", &self.client_id)
            .field("credential_generation", &self.credential_generation)
            .field("credential", &"[NOT RETAINED]")
            .finish_non_exhaustive()
    }
}

/// Installed-service credential verifier.
///
/// Implementations own secret storage and constant-time comparison. The HTTP boundary never
/// stores a bearer credential after this synchronous admission call returns.
pub trait McpHttpAuthenticator: fmt::Debug + Send + Sync + 'static {
    /// Authenticates one exact bearer token and returns its bounded registered identity.
    fn authenticate(&self, bearer_token: &str) -> Result<AuthenticatedMcpClient, McpHttpAuthError>;
}

/// Authentication or credential-scoped admission failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum McpHttpAuthError {
    /// Credential was missing, malformed, expired, or did not match.
    #[error("MCP credential was rejected")]
    Rejected,
    /// Registered identity or its configured ceiling was invalid.
    #[error("MCP authenticated identity is invalid")]
    InvalidIdentity,
    /// Credential already owns its maximum active requests.
    #[error("MCP credential request ceiling is exhausted")]
    Saturated,
}

/// Closed Streamable HTTP authority configuration.
#[derive(Clone, Debug)]
pub struct HttpMcpConfig {
    allowed_hosts: Vec<String>,
    allowed_origins: Vec<String>,
    cancellation: CancellationToken,
}

impl HttpMcpConfig {
    /// Creates exact Host and browser-Origin allowlists for one local endpoint.
    ///
    /// Missing `Origin` remains valid for native MCP clients; a supplied `Origin` must exactly
    /// match this nonempty allowlist. Wildcards and empty entries are rejected.
    ///
    /// # Errors
    ///
    /// Returns [`McpHttpConfigError::InvalidAllowlist`] unless both lists are nonempty, bounded,
    /// duplicate-free, and wildcard-free.
    pub fn try_new<H, O, HS, OS>(
        allowed_hosts: H,
        allowed_origins: O,
        cancellation: CancellationToken,
    ) -> Result<Self, McpHttpConfigError>
    where
        H: IntoIterator<Item = HS>,
        O: IntoIterator<Item = OS>,
        HS: Into<String>,
        OS: Into<String>,
    {
        let allowed_hosts: Vec<String> = allowed_hosts.into_iter().map(Into::into).collect();
        let allowed_origins: Vec<String> = allowed_origins.into_iter().map(Into::into).collect();
        validate_allowlist(&allowed_hosts, 16)?;
        validate_allowlist(&allowed_origins, 16)?;
        if allowed_hosts
            .iter()
            .any(|value| value.parse::<axum::http::uri::Authority>().is_err())
            || allowed_origins.iter().any(|value| {
                value.parse::<axum::http::Uri>().map_or(true, |uri| {
                    uri.scheme().is_none()
                        || uri.authority().is_none()
                        || uri.path() != "/"
                        || uri.query().is_some()
                })
            })
        {
            return Err(McpHttpConfigError::InvalidAllowlist);
        }
        Ok(Self {
            allowed_hosts,
            allowed_origins,
            cancellation,
        })
    }
}

/// Invalid closed HTTP transport configuration.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum McpHttpConfigError {
    /// An allowlist was empty, excessive, duplicated, malformed, or wildcard-bearing.
    #[error("MCP HTTP allowlist is invalid")]
    InvalidAllowlist,
}

/// Authenticated stateless RMCP Streamable HTTP service.
#[derive(Clone)]
pub struct McpHttpService {
    inner: StreamableHttpService<StatelessMcpHandler, NeverSessionManager>,
    authenticator: Arc<dyn McpHttpAuthenticator>,
    authority: Arc<HttpAuthority>,
    requests: Arc<Semaphore>,
}

impl fmt::Debug for McpHttpService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpHttpService")
            .field("inner", &"[RMCP STREAMABLE HTTP SERVICE]")
            .field("authenticator", &"[CREDENTIAL VERIFIER]")
            .field("authority", &self.authority)
            .field("requests", &"[GLOBAL REQUEST CEILING]")
            .finish_non_exhaustive()
    }
}

impl McpHttpService {
    /// Mounts the official RMCP transport in modern stateless mode.
    #[must_use]
    pub fn new(
        factory: McpHandlerFactory,
        authenticator: Arc<dyn McpHttpAuthenticator>,
        config: HttpMcpConfig,
    ) -> Self {
        let limits = factory.limits();
        let authority = Arc::new(HttpAuthority {
            allowed_hosts: config.allowed_hosts.clone(),
            allowed_origins: config.allowed_origins.clone(),
        });
        let rmcp_config = StreamableHttpServerConfig::default()
            .with_legacy_session_mode(false)
            .with_json_response(true)
            .with_sse_keep_alive(None)
            .with_sse_retry(None)
            .with_allowed_hosts(config.allowed_hosts)
            .with_allowed_origins(config.allowed_origins)
            .with_max_request_body_bytes(limits.maximum_body_bytes())
            .with_stateless_protocol_metadata_required(true)
            .with_cancellation_token(config.cancellation);
        let handler_factory = factory.clone();
        let inner = StreamableHttpService::new(
            move || Ok(handler_factory.create()),
            Arc::new(NeverSessionManager::default()),
            rmcp_config,
        );
        Self {
            inner,
            authenticator,
            authority,
            requests: Arc::new(Semaphore::new(limits.maximum_active_requests())),
        }
    }

    /// Handles one `/mcp` request after closed method, legacy-state, authentication, and
    /// concurrency admission, then delegates all MCP framing and protocol semantics to RMCP.
    pub async fn handle(&self, mut request: Request<Body>) -> Response<Body> {
        if request.method() != Method::POST {
            return response(StatusCode::METHOD_NOT_ALLOWED, Some((ALLOW, "POST")));
        }
        if request.headers().contains_key(MCP_SESSION_ID)
            || request.headers().contains_key(LAST_EVENT_ID)
        {
            return response(StatusCode::BAD_REQUEST, None);
        }
        if let Err(status) = self.authority.validate(request.headers()) {
            return response(status, None);
        }
        let token = match bearer_token(request.headers()) {
            Ok(token) => token,
            Err(_error) => return response(StatusCode::UNAUTHORIZED, None),
        };
        let identity = match self.authenticator.authenticate(token) {
            Ok(identity) => identity,
            Err(McpHttpAuthError::Saturated) => {
                return response(StatusCode::TOO_MANY_REQUESTS, None);
            }
            Err(McpHttpAuthError::Rejected | McpHttpAuthError::InvalidIdentity) => {
                return response(StatusCode::UNAUTHORIZED, None);
            }
        };
        request.headers_mut().remove(AUTHORIZATION);
        let global = match Arc::clone(&self.requests).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_error) => return response(StatusCode::SERVICE_UNAVAILABLE, None),
        };
        let credential = match identity.try_acquire() {
            Ok(permit) => permit,
            Err(_error) => return response(StatusCode::TOO_MANY_REQUESTS, None),
        };
        identity.observe_method(
            request
                .headers()
                .get("mcp-method")
                .and_then(|value| value.to_str().ok()),
        );
        request.extensions_mut().insert(identity);
        let inner = self.inner.handle(request).await;
        let (parts, body) = inner.into_parts();
        let permits = (global, credential);
        let body = body.map_frame(move |frame| {
            // The permit tuple remains owned by the mapped body until the response reaches EOF
            // or the caller drops it. This keeps active SSE streams inside both ceilings.
            let _ = &permits;
            frame
        });
        Response::from_parts(parts, Body::new(body))
    }
}

#[derive(Debug)]
struct HttpAuthority {
    allowed_hosts: Vec<String>,
    allowed_origins: Vec<String>,
}

impl HttpAuthority {
    fn validate(&self, headers: &HeaderMap) -> Result<(), StatusCode> {
        let host = single_header(headers, HOST).ok_or(StatusCode::MISDIRECTED_REQUEST)?;
        if !self.allowed_hosts.iter().any(|allowed| allowed == host) {
            return Err(StatusCode::MISDIRECTED_REQUEST);
        }

        let mut origins = headers.get_all(ORIGIN).iter();
        let Some(origin) = origins.next() else {
            return Ok(());
        };
        if origins.next().is_some() {
            return Err(StatusCode::FORBIDDEN);
        }
        let origin = origin.to_str().map_err(|_error| StatusCode::FORBIDDEN)?;
        if self.allowed_origins.iter().any(|allowed| allowed == origin) {
            Ok(())
        } else {
            Err(StatusCode::FORBIDDEN)
        }
    }
}

fn single_header(headers: &HeaderMap, name: axum::http::HeaderName) -> Option<&str> {
    let mut values = headers.get_all(name).iter();
    let value = values.next()?;
    if values.next().is_some() {
        return None;
    }
    value.to_str().ok()
}

fn bearer_token(headers: &HeaderMap) -> Result<&str, McpHttpAuthError> {
    if headers.get_all(AUTHORIZATION).iter().count() != 1 {
        return Err(McpHttpAuthError::Rejected);
    }
    let value = headers
        .get(AUTHORIZATION)
        .ok_or(McpHttpAuthError::Rejected)?
        .to_str()
        .map_err(|_error| McpHttpAuthError::Rejected)?;
    let token = value
        .strip_prefix("Bearer ")
        .filter(|token| !token.is_empty() && token.len() <= 4 * 1024)
        .ok_or(McpHttpAuthError::Rejected)?;
    if token.bytes().any(|byte| byte.is_ascii_control()) {
        Err(McpHttpAuthError::Rejected)
    } else {
        Ok(token)
    }
}

fn validate_allowlist(values: &[String], maximum_items: usize) -> Result<(), McpHttpConfigError> {
    if values.is_empty()
        || values.len() > maximum_items
        || values.iter().any(|value| {
            value.is_empty()
                || value.len() > 1_024
                || value.contains('*')
                || value.chars().any(char::is_control)
        })
        || values
            .iter()
            .enumerate()
            .any(|(index, value)| values[..index].contains(value))
    {
        Err(McpHttpConfigError::InvalidAllowlist)
    } else {
        Ok(())
    }
}

fn response(
    status: StatusCode,
    header: Option<(axum::http::HeaderName, &'static str)>,
) -> Response<Body> {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = status;
    if let Some((name, value)) = header
        && let Ok(value) = value.parse()
    {
        response.headers_mut().insert(name, value);
    }
    response
}
