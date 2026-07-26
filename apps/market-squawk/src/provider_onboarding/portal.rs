//! Bounded HTTP/1 loopback portal over the transport-neutral onboarding service.

use std::convert::Infallible;
use std::fmt;
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::body::Incoming;
use hyper::header::{CACHE_CONTROL, CONNECTION, CONTENT_TYPE, COOKIE, HOST, ORIGIN, SET_COOKIE};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use market_squawk_data::CatalogLimit;
use market_squawk_platform::{EncryptedFileFallbackStatus, LocalSecretStoreError, SecretValue};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq as _;
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::contracts::{
    OnboardingSessionView, ProviderPortalActivationRequest, ProviderPortalActivationView,
    ProviderProfileView,
};
use super::service::{ProviderOnboardingError, ProviderOnboardingService, StartOnboardingRequest};

const MAX_JSON_BODY_BYTES: usize = 1024 * 1024;
const MAX_SECRET_BODY_BYTES: usize = 8 * 1024;
const MAX_PATH_BYTES: usize = 256;
const SESSION_COOKIE_NAME: &str = "msq_onboarding";

/// Application-owned authority that completes onboarding and registers one durable adapter.
#[async_trait]
pub trait ProviderPortalActivationAuthority: Send + Sync {
    /// Activates the exact session and provider-specific configuration.
    async fn activate(
        &self,
        session_id: Uuid,
        request: ProviderPortalActivationRequest,
        cancellation: CancellationToken,
    ) -> Result<ProviderPortalActivationView, ProviderPortalActivationError>;

    /// Revokes callable runtime authority before deterministic onboarding cleanup.
    async fn cancel(
        &self,
        session_id: Uuid,
        cancellation: CancellationToken,
    ) -> Result<OnboardingSessionView, ProviderPortalActivationError>;

    /// Closes admission to application-owned activation work before portal transport teardown.
    fn begin_shutdown(&self) {}

    /// Joins any retained activation reconciliation through the application shutdown deadline.
    async fn finish_shutdown(
        &self,
        _deadline: Instant,
    ) -> Result<(), ProviderPortalActivationError> {
        Ok(())
    }
}

/// Closed portal-facing adapter activation failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ProviderPortalActivationError {
    /// The provider configuration or session/surface pairing is invalid.
    #[error("provider portal adapter request is invalid")]
    InvalidRequest,
    /// Onboarding or adapter activation is not currently admitted.
    #[error("provider portal adapter activation is unavailable")]
    Unavailable,
    /// Durable activation state could not be committed.
    #[error("provider portal adapter state is unavailable")]
    StateUnavailable,
    /// The caller or portal lifecycle cancelled the operation.
    #[error("provider portal adapter activation was cancelled")]
    Cancelled,
}

/// Bounded lifetime and request limits for one portal instance.
#[derive(Clone, Copy, Debug)]
pub struct ProviderPortalConfig {
    lifetime: Duration,
    request_timeout: Duration,
    max_requests: u64,
    max_connections: usize,
}

impl ProviderPortalConfig {
    /// Constructs a bounded portal configuration.
    pub fn try_new(
        lifetime: Duration,
        request_timeout: Duration,
        max_requests: u64,
        max_connections: usize,
    ) -> Result<Self, ProviderPortalError> {
        if lifetime < Duration::from_secs(30)
            || lifetime > Duration::from_secs(60 * 60)
            || request_timeout < Duration::from_secs(1)
            || request_timeout > Duration::from_secs(30)
            || request_timeout >= lifetime
            || max_requests == 0
            || max_requests > 4096
            || max_connections == 0
            || max_connections > 64
        {
            return Err(ProviderPortalError::InvalidConfiguration);
        }
        Ok(Self {
            lifetime,
            request_timeout,
            max_requests,
            max_connections,
        })
    }
}

impl Default for ProviderPortalConfig {
    fn default() -> Self {
        Self {
            lifetime: Duration::from_secs(15 * 60),
            request_timeout: Duration::from_secs(15),
            max_requests: 512,
            max_connections: 16,
        }
    }
}

/// Running loopback portal with explicit shutdown ownership.
pub struct ProviderOnboardingPortal {
    base_url: String,
    shutdown: CancellationToken,
    task: Option<JoinHandle<Result<(), ProviderPortalError>>>,
}

impl ProviderOnboardingPortal {
    /// Binds only an ephemeral IPv4 loopback port and starts the bounded server.
    pub async fn start(
        service: Arc<ProviderOnboardingService>,
        activation: Arc<dyn ProviderPortalActivationAuthority>,
        config: ProviderPortalConfig,
    ) -> Result<Self, ProviderPortalError> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        if address.ip() != Ipv4Addr::LOCALHOST {
            return Err(ProviderPortalError::LoopbackBinding);
        }
        let base_url = format!("http://{address}");
        let security = Arc::new(PortalSecurity::try_new(
            &address,
            &base_url,
            config.lifetime,
        )?);
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run_server(
            listener,
            service,
            activation,
            Arc::clone(&security),
            config,
            shutdown.clone(),
        ));
        Ok(Self {
            base_url,
            shutdown,
            task: Some(task),
        })
    }

    /// Returns the token-free loopback origin.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Stops admission, cancels in-flight work, and waits for connection tasks.
    pub async fn shutdown(mut self) -> Result<(), ProviderPortalError> {
        self.shutdown.cancel();
        let task = self.task.take().ok_or(ProviderPortalError::ServerTask)?;
        task.await.map_err(|_| ProviderPortalError::ServerTask)?
    }
}

impl fmt::Debug for ProviderOnboardingPortal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderOnboardingPortal")
            .field("base_url", &self.base_url)
            .finish_non_exhaustive()
    }
}

impl Drop for ProviderOnboardingPortal {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

struct PortalSecurity {
    expected_host: String,
    expected_origin: String,
    session_token: String,
    csrf_token: String,
    expires_at: Instant,
    cookie_max_age_secs: u64,
}

impl PortalSecurity {
    fn try_new(
        address: &SocketAddr,
        origin: &str,
        lifetime: Duration,
    ) -> Result<Self, ProviderPortalError> {
        let mut session = [0_u8; 32];
        let mut csrf = [0_u8; 32];
        getrandom::fill(&mut session).map_err(|_| ProviderPortalError::RandomUnavailable)?;
        getrandom::fill(&mut csrf).map_err(|_| ProviderPortalError::RandomUnavailable)?;
        let expires_at = Instant::now()
            .checked_add(lifetime)
            .ok_or(ProviderPortalError::Clock)?;
        Ok(Self {
            expected_host: address.to_string(),
            expected_origin: origin.to_owned(),
            session_token: encode_hex(&session),
            csrf_token: encode_hex(&csrf),
            expires_at,
            cookie_max_age_secs: lifetime.as_secs(),
        })
    }
}

impl fmt::Debug for PortalSecurity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PortalSecurity")
            .field("expected_host", &self.expected_host)
            .field("expected_origin", &self.expected_origin)
            .field("tokens", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

async fn run_server(
    listener: TcpListener,
    service: Arc<ProviderOnboardingService>,
    activation: Arc<dyn ProviderPortalActivationAuthority>,
    security: Arc<PortalSecurity>,
    config: ProviderPortalConfig,
    shutdown: CancellationToken,
) -> Result<(), ProviderPortalError> {
    let admission = Arc::new(Semaphore::new(config.max_connections));
    let requests = Arc::new(AtomicU64::new(0));
    let mut connections = JoinSet::new();
    let lifetime = tokio::time::sleep(config.lifetime);
    tokio::pin!(lifetime);

    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            () = &mut lifetime => {
                shutdown.cancel();
                break;
            }
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                if !peer.ip().is_loopback() {
                    continue;
                }
                let Ok(permit) = Arc::clone(&admission).try_acquire_owned() else {
                    continue;
                };
                let service = Arc::clone(&service);
                let activation = Arc::clone(&activation);
                let security = Arc::clone(&security);
                let requests = Arc::clone(&requests);
                let connection_shutdown = shutdown.clone();
                connections.spawn(async move {
                    let _permit = permit;
                    serve_connection(
                        stream,
                        service,
                        activation,
                        security,
                        requests,
                        config,
                        connection_shutdown,
                    )
                    .await;
                });
            }
        }
    }
    shutdown.cancel();
    while tokio::time::timeout(Duration::from_secs(2), connections.join_next())
        .await
        .ok()
        .flatten()
        .is_some()
    {}
    connections.abort_all();
    Ok(())
}

async fn serve_connection(
    stream: TcpStream,
    service: Arc<ProviderOnboardingService>,
    activation: Arc<dyn ProviderPortalActivationAuthority>,
    security: Arc<PortalSecurity>,
    requests: Arc<AtomicU64>,
    config: ProviderPortalConfig,
    shutdown: CancellationToken,
) {
    let connection_shutdown = shutdown.clone();
    let handler = service_fn(move |request| {
        handle_request(
            request,
            Arc::clone(&service),
            Arc::clone(&activation),
            Arc::clone(&security),
            Arc::clone(&requests),
            config,
            connection_shutdown.clone(),
        )
    });
    let mut builder = http1::Builder::new();
    builder
        .keep_alive(false)
        .max_headers(32)
        .max_buf_size(8192)
        .timer(TokioTimer::new())
        .header_read_timeout(Duration::from_secs(5));
    let connection = builder.serve_connection(TokioIo::new(stream), handler);
    tokio::select! {
        () = shutdown.cancelled() => {}
        result = connection => {
            let _connection_result = result;
        }
    }
}

async fn handle_request(
    request: Request<Incoming>,
    service: Arc<ProviderOnboardingService>,
    activation: Arc<dyn ProviderPortalActivationAuthority>,
    security: Arc<PortalSecurity>,
    requests: Arc<AtomicU64>,
    config: ProviderPortalConfig,
    cancellation: CancellationToken,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let request_number = requests.fetch_add(1, Ordering::AcqRel);
    if request_number >= config.max_requests || Instant::now() >= security.expires_at {
        return Ok(error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "portal_unavailable",
        ));
    }
    let request_cancellation = cancellation.child_token();
    let response = match await_portal_request(
        config.request_timeout,
        request_cancellation.clone(),
        dispatch(
            request,
            service,
            activation,
            security,
            request_cancellation.clone(),
        ),
    )
    .await
    {
        Some(Ok(response)) => response,
        Some(Err(error)) => map_error(error),
        None => error_response(StatusCode::REQUEST_TIMEOUT, "request_timeout"),
    };
    Ok(response)
}

async fn await_portal_request<F>(
    timeout: Duration,
    cancellation: CancellationToken,
    request: F,
) -> Option<F::Output>
where
    F: Future,
{
    tokio::pin!(request);
    tokio::select! {
        biased;
        output = &mut request => Some(output),
        () = tokio::time::sleep(timeout) => {
            cancellation.cancel();
            None
        }
    }
}

async fn dispatch(
    request: Request<Incoming>,
    service: Arc<ProviderOnboardingService>,
    activation: Arc<dyn ProviderPortalActivationAuthority>,
    security: Arc<PortalSecurity>,
    cancellation: CancellationToken,
) -> Result<Response<Full<Bytes>>, PortalRequestError> {
    validate_common_request(&request, &security)?;
    let method = request.method().clone();
    let path = request.uri().path().to_owned();

    if method == Method::GET && path == "/" {
        return Ok(text_response(
            StatusCode::OK,
            "text/html; charset=utf-8",
            INDEX_HTML,
        ));
    }
    if method == Method::GET && path == "/portal.js" {
        return Ok(text_response(
            StatusCode::OK,
            "text/javascript; charset=utf-8",
            PORTAL_JAVASCRIPT,
        ));
    }
    if method == Method::GET && path == "/api/v1/bootstrap" {
        let session_limit = CatalogLimit::new(32).map_err(ProviderOnboardingError::from)?;
        let response = BootstrapResponse {
            csrf_token: &security.csrf_token,
            encrypted_file_fallback: service.encrypted_file_fallback_status()?,
            profiles: service.profiles(),
            sessions: service.current_sessions(session_limit)?,
        };
        return with_session_cookie(json_response(StatusCode::OK, &response), &security);
    }
    if method == Method::POST && path == "/api/v1/secrets/fallback/unlock" {
        validate_mutation(&request, &security, "application/octet-stream")?;
        let unlock = collect_secret_body(request.into_body()).await?;
        let status = service
            .unlock_encrypted_file_fallback(unlock, cancellation)
            .await?;
        return Ok(json_response(
            StatusCode::OK,
            &SecretFallbackResponse {
                encrypted_file_fallback: status,
            },
        ));
    }
    if method == Method::POST && path == "/api/v1/secrets/fallback/lock" {
        validate_mutation(&request, &security, "application/json")?;
        let body = collect_body(request.into_body(), MAX_JSON_BODY_BYTES).await?;
        if !body.is_empty() && body != b"{}" {
            return Err(PortalRequestError::InvalidBody);
        }
        let status = service.lock_encrypted_file_fallback(cancellation).await?;
        return Ok(json_response(
            StatusCode::OK,
            &SecretFallbackResponse {
                encrypted_file_fallback: status,
            },
        ));
    }
    if method == Method::POST && path == "/api/v1/sessions" {
        validate_mutation(&request, &security, "application/json")?;
        let body = collect_body(request.into_body(), MAX_JSON_BODY_BYTES).await?;
        let input: StartRequestBody =
            serde_json::from_slice(&body).map_err(|_| PortalRequestError::InvalidBody)?;
        let start = StartOnboardingRequest::try_new(
            input.surface_id,
            input.organization,
            input.administrative_email,
        )?;
        let status = service.start(start, cancellation).await?;
        return Ok(json_response(StatusCode::OK, &status));
    }
    if let Some((session_id, action)) = parse_session_path(&path) {
        if method == Method::GET && action.is_none() {
            validate_session_cookie(&request, &security)?;
            return Ok(json_response(StatusCode::OK, &service.resume(session_id)?));
        }
        if method == Method::POST && action == Some("secret") {
            validate_mutation(&request, &security, "application/octet-stream")?;
            let secret = collect_secret_body(request.into_body()).await?;
            let status = service
                .submit_secret(session_id, secret, cancellation)
                .await?;
            return Ok(json_response(StatusCode::OK, &status));
        }
        if method == Method::POST && action == Some("activate") {
            validate_mutation(&request, &security, "application/json")?;
            let body = collect_body(request.into_body(), MAX_JSON_BODY_BYTES).await?;
            let input: ProviderPortalActivationRequest =
                serde_json::from_slice(&body).map_err(|_| PortalRequestError::InvalidBody)?;
            let activated = activation.activate(session_id, input, cancellation).await?;
            return Ok(json_response(StatusCode::OK, &activated));
        }
        if method == Method::POST && action == Some("renew") {
            validate_mutation(&request, &security, "application/json")?;
            let body = collect_body(request.into_body(), MAX_JSON_BODY_BYTES).await?;
            require_empty_json_body(&body)?;
            let status = service.begin_renewal(session_id).await?;
            return Ok(json_response(StatusCode::OK, &status));
        }
        if method == Method::POST && action == Some("cleanup") {
            validate_mutation(&request, &security, "application/json")?;
            let body = collect_body(request.into_body(), MAX_JSON_BODY_BYTES).await?;
            require_empty_json_body(&body)?;
            let status = service.reconcile_cleanup(session_id, cancellation).await?;
            return Ok(json_response(StatusCode::OK, &status));
        }
        if method == Method::POST && action == Some("cancel") {
            validate_mutation(&request, &security, "application/json")?;
            let body = collect_body(request.into_body(), MAX_JSON_BODY_BYTES).await?;
            require_empty_json_body(&body)?;
            let status = activation.cancel(session_id, cancellation).await?;
            return Ok(json_response(StatusCode::OK, &status));
        }
    }
    Err(PortalRequestError::NotFound)
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use super::*;

    struct PendingRequest {
        cancellation: CancellationToken,
        dropped: Option<tokio::sync::oneshot::Sender<bool>>,
    }

    impl Future for PendingRequest {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    impl Drop for PendingRequest {
        fn drop(&mut self) {
            if let Some(dropped) = self.dropped.take() {
                let _ignored = dropped.send(self.cancellation.is_cancelled());
            }
        }
    }

    #[tokio::test]
    async fn portal_timeout_cancels_request_before_dropping_dispatch_waiter() {
        let cancellation = CancellationToken::new();
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();

        let result = await_portal_request(
            Duration::ZERO,
            cancellation.clone(),
            PendingRequest {
                cancellation,
                dropped: Some(dropped_tx),
            },
        )
        .await;

        assert!(result.is_none());
        assert!(matches!(dropped_rx.await, Ok(true)));
    }
}

fn validate_common_request(
    request: &Request<Incoming>,
    security: &PortalSecurity,
) -> Result<(), PortalRequestError> {
    if request.uri().scheme().is_some()
        || request.uri().authority().is_some()
        || request.uri().query().is_some()
        || request.uri().path().len() > MAX_PATH_BYTES
        || request.version() != hyper::Version::HTTP_11
        || header_value(request, HOST)? != security.expected_host
    {
        return Err(PortalRequestError::InvalidRequest);
    }
    Ok(())
}

fn validate_mutation(
    request: &Request<Incoming>,
    security: &PortalSecurity,
    expected_content_type: &str,
) -> Result<(), PortalRequestError> {
    validate_session_cookie(request, security)?;
    let content_type = header_value(request, CONTENT_TYPE)?;
    if header_value(request, ORIGIN)? != security.expected_origin
        || !constant_time_equal(
            header_value_named(request, "x-csrf-token")?.as_bytes(),
            security.csrf_token.as_bytes(),
        )
        || content_type.split(';').next() != Some(expected_content_type)
    {
        return Err(PortalRequestError::Forbidden);
    }
    Ok(())
}

fn validate_session_cookie(
    request: &Request<Incoming>,
    security: &PortalSecurity,
) -> Result<(), PortalRequestError> {
    let mut values = request.headers().get_all(COOKIE).iter();
    let value = values
        .next()
        .ok_or(PortalRequestError::Forbidden)?
        .to_str()
        .map_err(|_| PortalRequestError::Forbidden)?;
    if values.next().is_some() {
        return Err(PortalRequestError::Forbidden);
    }
    let expected = format!("{SESSION_COOKIE_NAME}={}", security.session_token);
    let matched = value.split(';').map(str::trim).any(|cookie| {
        cookie.len() == expected.len()
            && constant_time_equal(cookie.as_bytes(), expected.as_bytes())
    });
    if matched {
        Ok(())
    } else {
        Err(PortalRequestError::Forbidden)
    }
}

fn header_value(
    request: &Request<Incoming>,
    name: hyper::header::HeaderName,
) -> Result<&str, PortalRequestError> {
    if request.headers().get_all(&name).iter().count() != 1 {
        return Err(PortalRequestError::InvalidRequest);
    }
    request
        .headers()
        .get(name)
        .ok_or(PortalRequestError::InvalidRequest)?
        .to_str()
        .map_err(|_| PortalRequestError::InvalidRequest)
}

fn header_value_named<'a>(
    request: &'a Request<Incoming>,
    name: &'static str,
) -> Result<&'a str, PortalRequestError> {
    let name = hyper::header::HeaderName::from_static(name);
    header_value(request, name)
}

async fn collect_body(mut body: Incoming, max_bytes: usize) -> Result<Vec<u8>, PortalRequestError> {
    let mut retained = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| PortalRequestError::InvalidBody)?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        let next = retained
            .len()
            .checked_add(data.len())
            .ok_or(PortalRequestError::BodyTooLarge)?;
        if next > max_bytes {
            return Err(PortalRequestError::BodyTooLarge);
        }
        retained.extend_from_slice(&data);
    }
    Ok(retained)
}

async fn collect_secret_body(mut body: Incoming) -> Result<SecretValue, PortalRequestError> {
    let mut retained = SecretBodyBuffer::try_new()?;
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| PortalRequestError::InvalidSecretBody)?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        retained.extend_from_slice(&data)?;
    }
    retained.into_secret()
}

struct SecretBodyBuffer {
    bytes: Option<Vec<u8>>,
}

impl SecretBodyBuffer {
    fn try_new() -> Result<Self, PortalRequestError> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(MAX_SECRET_BODY_BYTES)
            .map_err(|_error| PortalRequestError::Internal)?;
        Ok(Self { bytes: Some(bytes) })
    }

    fn extend_from_slice(&mut self, data: &[u8]) -> Result<(), PortalRequestError> {
        let bytes = self.bytes.as_mut().ok_or(PortalRequestError::Internal)?;
        let next = bytes
            .len()
            .checked_add(data.len())
            .ok_or(PortalRequestError::BodyTooLarge)?;
        if next > MAX_SECRET_BODY_BYTES {
            return Err(PortalRequestError::BodyTooLarge);
        }
        bytes.extend_from_slice(data);
        Ok(())
    }

    fn into_secret(mut self) -> Result<SecretValue, PortalRequestError> {
        let bytes = self.bytes.take().ok_or(PortalRequestError::Internal)?;
        SecretValue::from_utf8_bytes(bytes).map_err(|_error| PortalRequestError::InvalidSecretBody)
    }
}

impl Drop for SecretBodyBuffer {
    fn drop(&mut self) {
        if let Some(bytes) = self.bytes.take()
            && let Ok(secret) = SecretValue::from_utf8_bytes(bytes)
        {
            drop(secret);
        }
    }
}

fn parse_session_path(path: &str) -> Option<(Uuid, Option<&str>)> {
    let remainder = path.strip_prefix("/api/v1/sessions/")?;
    let mut segments = remainder.split('/');
    let session_id = Uuid::parse_str(segments.next()?).ok()?;
    let action = segments.next();
    if segments.next().is_some() || action.is_some_and(str::is_empty) {
        return None;
    }
    Some((session_id, action))
}

fn require_empty_json_body(body: &[u8]) -> Result<(), PortalRequestError> {
    if body.is_empty() || body == b"{}" {
        Ok(())
    } else {
        Err(PortalRequestError::InvalidBody)
    }
}

fn with_session_cookie(
    mut response: Response<Full<Bytes>>,
    security: &PortalSecurity,
) -> Result<Response<Full<Bytes>>, PortalRequestError> {
    let value = format!(
        "{SESSION_COOKIE_NAME}={}; HttpOnly; SameSite=Strict; Path=/; Max-Age={}",
        security.session_token, security.cookie_max_age_secs
    );
    let value =
        hyper::header::HeaderValue::from_str(&value).map_err(|_| PortalRequestError::Internal)?;
    let _prior = response.headers_mut().insert(SET_COOKIE, value);
    Ok(response)
}

fn json_response<T: Serialize>(status: StatusCode, value: &T) -> Response<Full<Bytes>> {
    match serde_json::to_vec(value) {
        Ok(body) => response(status, "application/json", body),
        Err(_) => error_response(StatusCode::INTERNAL_SERVER_ERROR, "serialization_failed"),
    }
}

fn text_response(
    status: StatusCode,
    content_type: &'static str,
    body: &'static str,
) -> Response<Full<Bytes>> {
    response(status, content_type, body.as_bytes().to_vec())
}

fn error_response(status: StatusCode, code: &'static str) -> Response<Full<Bytes>> {
    json_response(status, &ErrorBody { error: code })
}

fn response(
    status: StatusCode,
    content_type: &'static str,
    body: Vec<u8>,
) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(Bytes::from(body)));
    *response.status_mut() = status;
    let headers = response.headers_mut();
    let _prior = headers.insert(
        CONTENT_TYPE,
        hyper::header::HeaderValue::from_static(content_type),
    );
    let _prior = headers.insert(
        CACHE_CONTROL,
        hyper::header::HeaderValue::from_static("no-store"),
    );
    let _prior = headers.insert(CONNECTION, hyper::header::HeaderValue::from_static("close"));
    let _prior = headers.insert(
        hyper::header::HeaderName::from_static("content-security-policy"),
        hyper::header::HeaderValue::from_static(
            "default-src 'none'; script-src 'self'; connect-src 'self'; form-action 'none'; frame-ancestors 'none'; base-uri 'none'",
        ),
    );
    let _prior = headers.insert(
        hyper::header::HeaderName::from_static("x-content-type-options"),
        hyper::header::HeaderValue::from_static("nosniff"),
    );
    let _prior = headers.insert(
        hyper::header::HeaderName::from_static("referrer-policy"),
        hyper::header::HeaderValue::from_static("no-referrer"),
    );
    let _prior = headers.insert(
        hyper::header::HeaderName::from_static("x-frame-options"),
        hyper::header::HeaderValue::from_static("DENY"),
    );
    response
}

fn map_error(error: PortalRequestError) -> Response<Full<Bytes>> {
    match error {
        PortalRequestError::NotFound
        | PortalRequestError::Application(ProviderOnboardingError::UnknownProfile) => {
            error_response(StatusCode::NOT_FOUND, "not_found")
        }
        PortalRequestError::Forbidden => error_response(StatusCode::FORBIDDEN, "forbidden"),
        PortalRequestError::BodyTooLarge => {
            error_response(StatusCode::PAYLOAD_TOO_LARGE, "body_too_large")
        }
        PortalRequestError::InvalidRequest
        | PortalRequestError::InvalidBody
        | PortalRequestError::InvalidSecretBody
        | PortalRequestError::Application(ProviderOnboardingError::InvalidRequest)
        | PortalRequestError::Application(ProviderOnboardingError::AdministrativeContactRequired)
        | PortalRequestError::Application(ProviderOnboardingError::InvalidSecretShape) => {
            error_response(StatusCode::BAD_REQUEST, "invalid_request")
        }
        PortalRequestError::Application(ProviderOnboardingError::SecretImportUnavailable)
        | PortalRequestError::Application(ProviderOnboardingError::RenewalUnavailable)
        | PortalRequestError::Application(ProviderOnboardingError::InvalidSessionState)
        | PortalRequestError::Application(ProviderOnboardingError::ActivationUnavailable)
        | PortalRequestError::Application(ProviderOnboardingError::ActivationExpired)
        | PortalRequestError::Application(ProviderOnboardingError::EvidenceRefreshRequired)
        | PortalRequestError::Application(
            ProviderOnboardingError::RemoteReconciliationRequired
            | ProviderOnboardingError::SecretCleanupUnavailable,
        )
        | PortalRequestError::Application(ProviderOnboardingError::RightsBlocked) => {
            error_response(StatusCode::CONFLICT, "invalid_session_state")
        }
        PortalRequestError::Application(ProviderOnboardingError::ProbeRateLimited) => {
            error_response(StatusCode::TOO_MANY_REQUESTS, "provider_rate_limited")
        }
        PortalRequestError::Application(ProviderOnboardingError::ProbeDeadlineExceeded) => {
            error_response(StatusCode::GATEWAY_TIMEOUT, "provider_deadline_elapsed")
        }
        PortalRequestError::Application(ProviderOnboardingError::SecretStore(
            LocalSecretStoreError::AuthenticationFailed
            | LocalSecretStoreError::CandidateUnlockNotAuthoritative
            | LocalSecretStoreError::SupersededUnlock,
        )) => error_response(StatusCode::FORBIDDEN, "invalid_unlock"),
        PortalRequestError::Application(ProviderOnboardingError::SecretStore(
            LocalSecretStoreError::UnsupportedOperation | LocalSecretStoreError::Locked,
        )) => error_response(StatusCode::CONFLICT, "fallback_unavailable"),
        PortalRequestError::Application(ProviderOnboardingError::OperationCancelled) => {
            error_response(StatusCode::REQUEST_TIMEOUT, "operation_cancelled")
        }
        PortalRequestError::Activation(ProviderPortalActivationError::InvalidRequest) => {
            error_response(StatusCode::BAD_REQUEST, "invalid_adapter_request")
        }
        PortalRequestError::Activation(ProviderPortalActivationError::Unavailable) => {
            error_response(StatusCode::CONFLICT, "adapter_activation_unavailable")
        }
        PortalRequestError::Activation(ProviderPortalActivationError::Cancelled) => {
            error_response(StatusCode::REQUEST_TIMEOUT, "operation_cancelled")
        }
        PortalRequestError::Activation(ProviderPortalActivationError::StateUnavailable) => {
            error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "activation_state_unavailable",
            )
        }
        PortalRequestError::Application(_) | PortalRequestError::Internal => {
            error_response(StatusCode::SERVICE_UNAVAILABLE, "operation_unavailable")
        }
    }
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len() && bool::from(left.ct_eq(right))
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[derive(Serialize)]
struct BootstrapResponse<'a> {
    csrf_token: &'a str,
    encrypted_file_fallback: EncryptedFileFallbackStatus,
    profiles: Vec<ProviderProfileView>,
    sessions: Vec<OnboardingSessionView>,
}

#[derive(Serialize)]
struct SecretFallbackResponse {
    encrypted_file_fallback: EncryptedFileFallbackStatus,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StartRequestBody {
    surface_id: String,
    #[serde(default)]
    organization: Option<String>,
    #[serde(default)]
    administrative_email: Option<String>,
}

#[derive(Serialize)]
struct ErrorBody {
    error: &'static str,
}

#[derive(Debug, Error)]
enum PortalRequestError {
    #[error("portal request is invalid")]
    InvalidRequest,
    #[error("portal request is forbidden")]
    Forbidden,
    #[error("portal route was not found")]
    NotFound,
    #[error("portal body is invalid")]
    InvalidBody,
    #[error("portal secret body is invalid")]
    InvalidSecretBody,
    #[error("portal body exceeded its bound")]
    BodyTooLarge,
    #[error("portal response failed")]
    Internal,
    #[error(transparent)]
    Application(#[from] ProviderOnboardingError),
    #[error(transparent)]
    Activation(#[from] ProviderPortalActivationError),
}

/// Portal lifecycle failure.
#[derive(Debug, Error)]
pub enum ProviderPortalError {
    /// Configuration was zero, excessive, or internally inconsistent.
    #[error("provider portal configuration is invalid")]
    InvalidConfiguration,
    /// The listener was not an IPv4 loopback endpoint.
    #[error("provider portal did not bind loopback")]
    LoopbackBinding,
    /// Cryptographic session-token generation failed.
    #[error("provider portal random generation failed")]
    RandomUnavailable,
    /// The monotonic portal expiry could not be represented.
    #[error("provider portal clock is unavailable")]
    Clock,
    /// Loopback listener I/O failed.
    #[error("provider portal I/O failed")]
    Io(#[from] std::io::Error),
    /// The portal task terminated without a typed result.
    #[error("provider portal task failed")]
    ServerTask,
}

const INDEX_HTML: &str = r#"<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width">
<title>Market Squawk provider setup</title></head>
<body>
<main>
<h1>Market Squawk provider setup</h1>
<p>This local page lists exact provider requirements, release gates, and official handoffs.</p>
<section id="fallback"></section>
<div id="profiles">Loading code-owned provider profiles…</div>
<pre id="status" aria-live="polite"></pre>
</main>
<script src="/portal.js"></script>
</body></html>"#;

const PORTAL_JAVASCRIPT: &str = r#"'use strict';
let csrf = '';
async function mutate(path, body, type) {
  const response = await fetch(path, {
    method: 'POST',
    headers: {'x-csrf-token': csrf, 'content-type': type},
    body
  });
  const result = await response.json();
  document.getElementById('status').textContent = JSON.stringify(result, null, 2);
  if (!response.ok) throw new Error(result.error || 'operation_failed');
  return result;
}
function input(type, placeholder, maximum) {
  const node = document.createElement('input');
  node.type = type;
  node.placeholder = placeholder;
  node.required = true;
  if (maximum) node.maxLength = maximum;
  return node;
}
function requiredValue(node) {
  if (!node.reportValidity()) throw new Error('invalid_input');
  return node.value;
}
function dateValue(node) {
  const parts = requiredValue(node).split('-').map(Number);
  return {year: parts[0], month: parts[1], day: parts[2]};
}
function renderFallback(state) {
  const section = document.getElementById('fallback'); section.textContent = '';
  const title = document.createElement('h2'); title.textContent = 'Encrypted credential fallback';
  const detail = document.createElement('p');
  if (state === 'disabled') {
    detail.textContent = 'No encrypted fallback is configured; the operating-system credential store is required.';
    section.append(title, detail); return;
  }
  if (state === 'locked') {
    detail.textContent = 'The encrypted fallback is locked. Its unlock is submitted only to this process.';
    const unlock = input('password', 'Encrypted fallback unlock', 8192);
    unlock.autocomplete = 'new-password';
    const button = document.createElement('button'); button.textContent = 'Unlock fallback';
    button.addEventListener('click', async () => {
      const value = requiredValue(unlock); unlock.value = '';
      try {
        const result = await mutate('/api/v1/secrets/fallback/unlock',
          value, 'application/octet-stream');
        renderFallback(result.encrypted_file_fallback);
      } catch (error) {
        document.getElementById('status').textContent = String(error);
      }
    });
    section.append(title, detail, unlock, button); return;
  }
  detail.textContent = 'The encrypted fallback is ready in this process.';
  const button = document.createElement('button'); button.textContent = 'Lock fallback';
  button.addEventListener('click', async () => {
    try {
      const result = await mutate('/api/v1/secrets/fallback/lock', '{}', 'application/json');
      renderFallback(result.encrypted_file_fallback);
    } catch (error) {
      document.getElementById('status').textContent = String(error);
    }
  });
  section.append(title, detail, button);
}
function blsConfiguration(section) {
  const start = input('number', 'Start year');
  const end = input('number', 'End year');
  start.min = '1913'; start.max = '9999';
  end.min = '1913'; end.max = '9999';
  const rows = document.createElement('div');
  const add = document.createElement('button');
  add.type = 'button'; add.textContent = 'Add BLS series';
  const seriesRows = [];
  function addSeries() {
    const row = document.createElement('fieldset');
    const fields = [
      input('text', 'Series ID', 50), input('text', 'Verified title', 512),
      input('text', 'Unit', 128), input('text', 'Frequency', 128),
      input('text', 'Seasonal adjustment', 128), input('text', 'Measure', 128)
    ];
    row.append(...fields); rows.append(row); seriesRows.push(fields);
  }
  add.addEventListener('click', addSeries); addSeries();
  section.append(start, end, rows, add);
  return () => ({
    kind: 'bls', start_year: Number(requiredValue(start)), end_year: Number(requiredValue(end)),
    series: seriesRows.map(fields => ({
      series_id: requiredValue(fields[0]), title: requiredValue(fields[1]),
      unit: requiredValue(fields[2]), frequency: requiredValue(fields[3]),
      seasonal_adjustment: requiredValue(fields[4]), measure: requiredValue(fields[5])
    }))
  });
}
function configuration(profile, section) {
  if (profile.id === 'coinbase.public-market-data' ||
      profile.id === 'coinbase.exchange-direct-market-data' ||
      profile.id === 'kraken.spot-public-market-data' ||
      profile.id === 'treasury.daily-rates-xml') {
    return () => ({kind: 'source'});
  }
  if (profile.id === 'sec.edgar-public') return () => ({kind: 'sec'});
  if (profile.id === 'bls.v1-unregistered' || profile.id === 'bls.v2-registered') {
    return blsConfiguration(section);
  }
  if (profile.id === 'treasury.fiscal-data') {
    const first = input('date', 'First record date');
    const last = input('date', 'Last record date');
    const page = input('number', 'Page size');
    page.min = '1'; page.max = '10000'; page.value = '1000';
    section.append(first, last, page);
    return () => ({kind: 'treasury_fiscal', first_record_date: dateValue(first),
      last_record_date: dateValue(last), page_size: Number(requiredValue(page))});
  }
  return null;
}
async function activate(session, adapterRequest) {
  return mutate('/api/v1/sessions/' + session.session_id + '/activate',
    JSON.stringify(adapterRequest), 'application/json');
}
async function importSecret(session, adapterRequest, replacement) {
  const status = document.getElementById('status');
  status.textContent = '';
  let secretInputs;
  let secretValue;
  if (session.surface_id === 'coinbase.exchange-direct-market-data') {
    const apiKey = input('password', 'Coinbase Exchange API key', 1024);
    const passphrase = input('password', 'Coinbase Exchange passphrase', 1024);
    const signingSecret = input('password', 'Coinbase Exchange signing secret', 1024);
    secretInputs = [apiKey, passphrase, signingSecret];
    secretValue = () => JSON.stringify({version: 1, api_key: requiredValue(apiKey),
      passphrase: requiredValue(passphrase), signing_secret: requiredValue(signingSecret)});
  } else {
    const secret = input('password', replacement ? 'Provider-created replacement key' :
      'Provider-created API key', 8192);
    secretInputs = [secret];
    secretValue = () => requiredValue(secret);
  }
  for (const secret of secretInputs) secret.autocomplete = 'off';
  const submit = document.createElement('button');
  submit.textContent = replacement ? 'Import replacement and cut over' :
    'Import credentials and activate provider';
  submit.addEventListener('click', async () => {
    const value = secretValue();
    for (const secret of secretInputs) { secret.value = ''; secret.remove(); }
    submit.remove();
    const stored = await mutate('/api/v1/sessions/' + session.session_id + '/secret',
      value, 'application/octet-stream');
    if (stored.next_action === 'verify_and_activate' ||
        stored.next_action === 'verify_and_cutover') {
      await activate(stored, adapterRequest);
    }
  });
  status.before(...secretInputs, submit);
}
async function continueSession(session, adapterRequest) {
  if (session.next_action === 'active') return activate(session, adapterRequest);
  if (session.next_action === 'renew_credential') {
    const rotation = await mutate('/api/v1/sessions/' + session.session_id + '/renew',
      '{}', 'application/json');
    return continueSession(rotation, adapterRequest);
  }
  if (session.next_action === 'import_secret') {
    return importSecret(session, adapterRequest, false);
  }
  if (session.next_action === 'import_replacement') {
    return importSecret(session, adapterRequest, true);
  }
  if (session.next_action === 'verify_and_activate' ||
      session.next_action === 'verify_and_cutover') {
    return activate(session, adapterRequest);
  }
  if (session.next_action === 'reconcile_cleanup') {
    const reconciled = await mutate('/api/v1/sessions/' + session.session_id + '/cleanup',
      '{}', 'application/json');
    return continueSession(reconciled, adapterRequest);
  }
  document.getElementById('status').textContent = JSON.stringify(session, null, 2);
  return session;
}
async function start(profile, organization, email, adapterRequest) {
  if (profile.credential_requirement === 'required_provider_controlled' ||
      profile.account_requirement === 'required_provider_controlled') {
    window.open(profile.official_handoff_url, '_blank', 'noopener,noreferrer');
  }
  const request = {surface_id: profile.id};
  if (profile.administrative_contact_requirement === 'required_non_secret') {
    request.organization = requiredValue(organization);
    request.administrative_email = requiredValue(email);
  }
  const session = await mutate('/api/v1/sessions', JSON.stringify(request), 'application/json');
  return continueSession(session, adapterRequest);
}
fetch('/api/v1/bootstrap').then(response => response.json()).then(data => {
  csrf = data.csrf_token;
  renderFallback(data.encrypted_file_fallback);
  const sessions = new Map(data.sessions.map(session => [session.surface_id, session]));
  const root = document.getElementById('profiles'); root.textContent = '';
  for (const profile of data.profiles) {
    const section = document.createElement('section');
    const title = document.createElement('h2'); title.textContent = profile.display_name;
    const detail = document.createElement('p');
    detail.textContent = profile.handoff_instruction + ' Release: ' + profile.release_state + '.';
    const link = document.createElement('a'); link.href = profile.official_handoff_url;
    link.target = '_blank'; link.rel = 'noopener noreferrer'; link.textContent = 'Official provider page';
    const organization = input('text', 'Organization', 128); organization.autocomplete = 'organization';
    const email = input('email', 'Administrative email', 128); email.autocomplete = 'email';
    if (profile.administrative_contact_requirement !== 'required_non_secret') {
      organization.hidden = true; email.hidden = true; organization.required = false; email.required = false;
    }
    section.append(title, detail, link, document.createTextNode(' '), organization, email);
    const buildConfiguration = configuration(profile, section);
    const current = sessions.get(profile.id);
    const button = document.createElement('button');
    button.textContent = current ? 'Continue or manage provider' : 'Set up provider';
    const releaseAvailable = profile.release_state === 'available' ||
      profile.release_state === 'rights_limited';
    button.disabled = !releaseAvailable || buildConfiguration === null;
    button.addEventListener('click', () => {
      try {
        const operation = current && current.next_action !== 'start_new_session' ?
          continueSession(current, buildConfiguration()) :
          start(profile, organization, email, buildConfiguration());
        operation.catch(error => {
        document.getElementById('status').textContent = String(error);
        });
      }
      catch (error) { document.getElementById('status').textContent = String(error); }
    });
    section.append(button);
    if (current) {
      const remove = document.createElement('button');
      remove.textContent = 'Remove local provider authority';
      remove.addEventListener('click', async () => {
        try {
          await mutate('/api/v1/sessions/' + current.session_id + '/cancel',
            '{}', 'application/json');
        } catch (error) {
          document.getElementById('status').textContent = String(error);
        }
      });
      section.append(remove);
    }
    root.append(section);
  }
}).catch(() => { document.getElementById('profiles').textContent = 'Portal bootstrap unavailable.'; });"#;
