//! Bounded HTTP/1 loopback portal over the transport-neutral onboarding service.

use std::convert::Infallible;
use std::fmt;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::body::Incoming;
use hyper::header::{CACHE_CONTROL, CONNECTION, CONTENT_TYPE, COOKIE, HOST, ORIGIN, SET_COOKIE};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use market_squawk_platform::{SecretCancellation, SecretValue};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq as _;
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::contracts::ProviderProfileView;
use super::service::{ProviderOnboardingError, ProviderOnboardingService, StartOnboardingRequest};

const MAX_JSON_BODY_BYTES: usize = 2 * 1024;
const MAX_SECRET_BODY_BYTES: usize = 8 * 1024;
const MAX_PATH_BYTES: usize = 256;
const SESSION_COOKIE_NAME: &str = "msq_onboarding";

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
                let security = Arc::clone(&security);
                let requests = Arc::clone(&requests);
                let connection_shutdown = shutdown.clone();
                connections.spawn(async move {
                    let _permit = permit;
                    serve_connection(
                        stream,
                        service,
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
    let response = match tokio::time::timeout(
        config.request_timeout,
        dispatch(request, service, security, request_cancellation.clone()),
    )
    .await
    {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => map_error(error),
        Err(_) => {
            request_cancellation.cancel();
            error_response(StatusCode::REQUEST_TIMEOUT, "request_timeout")
        }
    };
    Ok(response)
}

async fn dispatch(
    request: Request<Incoming>,
    service: Arc<ProviderOnboardingService>,
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
        let response = BootstrapResponse {
            csrf_token: &security.csrf_token,
            profiles: service.profiles(),
        };
        return with_session_cookie(json_response(StatusCode::OK, &response), &security);
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
            let body = collect_body(request.into_body(), MAX_SECRET_BODY_BYTES).await?;
            let value =
                String::from_utf8(body).map_err(|_| PortalRequestError::InvalidSecretBody)?;
            let secret =
                SecretValue::new(value).map_err(|_| PortalRequestError::InvalidSecretBody)?;
            let secret_cancellation = SecretCancellation::new();
            let cancellation_monitor = secret_cancellation.clone();
            let combined = cancellation;
            let monitor = tokio::spawn(async move {
                combined.cancelled().await;
                cancellation_monitor.cancel();
            });
            let service = Arc::clone(&service);
            let operation_cancellation = secret_cancellation.clone();
            let operation = tokio::task::spawn_blocking(move || {
                service.submit_secret(session_id, secret, operation_cancellation)
            });
            let result = operation.await.map_err(|_| PortalRequestError::Internal);
            monitor.abort();
            let status = result??;
            return Ok(json_response(StatusCode::OK, &status));
        }
        if method == Method::POST && action == Some("activate") {
            validate_mutation(&request, &security, "application/json")?;
            let body = collect_body(request.into_body(), MAX_JSON_BODY_BYTES).await?;
            if !body.is_empty() && body != b"{}" {
                return Err(PortalRequestError::InvalidBody);
            }
            let lease = service.activate(session_id, cancellation).await?;
            return Ok(json_response(
                StatusCode::OK,
                &service.resume(lease.session_id())?,
            ));
        }
        if method == Method::POST && action == Some("cancel") {
            validate_mutation(&request, &security, "application/json")?;
            let body = collect_body(request.into_body(), MAX_JSON_BODY_BYTES).await?;
            if !body.is_empty() && body != b"{}" {
                return Err(PortalRequestError::InvalidBody);
            }
            return Ok(json_response(StatusCode::OK, &service.cancel(session_id)?));
        }
    }
    Err(PortalRequestError::NotFound)
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
        | PortalRequestError::Application(ProviderOnboardingError::InvalidSessionState)
        | PortalRequestError::Application(ProviderOnboardingError::ActivationUnavailable)
        | PortalRequestError::Application(ProviderOnboardingError::ActivationExpired)
        | PortalRequestError::Application(ProviderOnboardingError::EvidenceRefreshRequired)
        | PortalRequestError::Application(ProviderOnboardingError::RightsBlocked) => {
            error_response(StatusCode::CONFLICT, "invalid_session_state")
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
    profiles: Vec<ProviderProfileView>,
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
  return result;
}
async function start(profile, organization, email) {
  if (profile.credential_requirement === 'required_provider_controlled' ||
      profile.account_requirement === 'required_provider_controlled') {
    window.open(profile.official_handoff_url, '_blank', 'noopener,noreferrer');
  }
  const request = {surface_id: profile.id};
  if (profile.administrative_contact_requirement === 'required_non_secret') {
    request.organization = organization.value;
    request.administrative_email = email.value;
  }
  const session = await mutate('/api/v1/sessions', JSON.stringify(request), 'application/json');
  if (session.next_action === 'import_secret') {
    const status = document.getElementById('status');
    status.textContent = '';
    const input = document.createElement('input');
    input.type = 'password';
    input.autocomplete = 'off';
    input.setAttribute('aria-label', 'Provider-created key');
    const submit = document.createElement('button');
    submit.textContent = 'Import key into local secure store';
    submit.addEventListener('click', async () => {
      const value = input.value;
      input.value = '';
      input.remove();
      submit.remove();
      if (value) {
        const stored = await mutate('/api/v1/sessions/' + session.session_id + '/secret', value, 'application/octet-stream');
        if (stored.next_action === 'verify_and_activate') {
          await mutate('/api/v1/sessions/' + session.session_id + '/activate', '{}', 'application/json');
        }
      }
    });
    status.before(input, submit);
  }
}
fetch('/api/v1/bootstrap').then(response => response.json()).then(data => {
  csrf = data.csrf_token;
  const root = document.getElementById('profiles');
  root.textContent = '';
  for (const profile of data.profiles) {
    const section = document.createElement('section');
    const title = document.createElement('h2');
    title.textContent = profile.display_name;
    const detail = document.createElement('p');
    detail.textContent = profile.handoff_instruction + ' Release: ' + profile.release_state + '.';
    const link = document.createElement('a');
    link.href = profile.official_handoff_url;
    link.target = '_blank';
    link.rel = 'noopener noreferrer';
    link.textContent = 'Official provider page';
    const button = document.createElement('button');
    button.textContent = 'Start setup';
    const organization = document.createElement('input');
    organization.type = 'text';
    organization.autocomplete = 'organization';
    organization.placeholder = 'Organization';
    organization.maxLength = 128;
    const email = document.createElement('input');
    email.type = 'email';
    email.autocomplete = 'email';
    email.placeholder = 'Administrative email';
    email.maxLength = 128;
    if (profile.administrative_contact_requirement !== 'required_non_secret') {
      organization.hidden = true;
      email.hidden = true;
    } else {
      organization.required = true;
      email.required = true;
    }
    button.addEventListener('click', () => start(profile, organization, email));
    section.append(title, detail, link, document.createTextNode(' '), organization, email, button);
    root.append(section);
  }
}).catch(() => { document.getElementById('profiles').textContent = 'Portal bootstrap unavailable.'; });"#;
