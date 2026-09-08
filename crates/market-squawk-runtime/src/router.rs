//! Closed loopback application routing before business-service dispatch.

use std::{
    fmt,
    net::{Ipv4Addr, SocketAddr},
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use axum::{
    Router,
    body::{Body, to_bytes},
    extract::State,
    http::{
        HeaderMap, Method, Request, StatusCode,
        header::{AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, HOST, ORIGIN},
    },
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures_util::StreamExt as _;
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp};
use market_squawk_services::{JsonStructureLimits, RequestContext, ServiceLimits};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tower::limit::ConcurrencyLimitLayer;
use uuid::Uuid;

use crate::{
    AppRequestEnvelope, AppResponseEnvelope, ApplicationProtocolRange, ApplicationProtocolVersion,
    ClientId, CredentialGeneration, CredentialRegistry, EventCursor, EventHub, EventPageLimit,
    InputAdmission, InputStager, MutationReplayGuard, ReplayAdmission, ReplayKey, RuntimeIdentity,
};

/// Native application authorization header carrying the registered client UUID.
pub const CLIENT_ID_HEADER: &str = "x-market-squawk-client-id";
/// Native application header carrying the exact installation UUID.
pub const INSTALLATION_ID_HEADER: &str = "x-market-squawk-installation-id";
/// Native application header carrying the exact workspace UUID.
pub const WORKSPACE_ID_HEADER: &str = "x-market-squawk-workspace-id";
/// Native application header carrying the exact running-service generation.
pub const SERVICE_GENERATION_HEADER: &str = "x-market-squawk-service-generation";
/// Native application authorization header carrying the exact credential generation.
pub const CREDENTIAL_GENERATION_HEADER: &str = "x-market-squawk-credential-generation";
/// Native streamed-input media type header.
pub const INPUT_MEDIA_TYPE_HEADER: &str = "x-market-squawk-input-media-type";
/// Native streamed-input exact byte-count header.
pub const INPUT_LENGTH_HEADER: &str = "x-market-squawk-input-length";
/// Native streamed-input exact SHA-256 header.
pub const INPUT_SHA256_HEADER: &str = "x-market-squawk-input-sha256";

const JSON_MEDIA_TYPE: &str = "application/json";
const BINARY_MEDIA_TYPE: &str = "application/octet-stream";
const AUTHORIZATION_PREFIX: &[u8] = b"Bearer ";

/// Whether an application operation may change durable or runtime authority state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationEffect {
    /// Side-effect-free bounded query.
    Read,
    /// Mutation requiring request-identity replay protection.
    Mutation,
}

/// Application-owned dispatch seam mounted behind transport admission.
#[async_trait]
pub trait ApplicationDispatcher: fmt::Debug + Send + Sync + 'static {
    /// Returns the bounded, non-secret installed-product snapshot for native presentation clients.
    fn bootstrap(&self) -> Result<Value, DispatchError>;

    /// Returns the registered operation's effect before execution.
    fn effect(&self, operation: &SourceIdentifier) -> Result<OperationEffect, DispatchError>;

    /// Executes one admitted request using its exact monotonic context.
    async fn dispatch(
        &self,
        request: &AppRequestEnvelope,
        context: RequestContext,
    ) -> Result<Value, DispatchError>;

    /// Completes service-owned work that must follow durable mutation-response publication.
    ///
    /// This hook runs only after the exact mutation response has been committed to the replay
    /// authority. It must be idempotent because a later successful mutation may retry a pending
    /// lifecycle handoff that could not be signalled previously.
    fn mutation_response_committed(&self) -> Result<(), DispatchError>;
}

/// Closed dispatcher failure safe for transport mapping.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum DispatchError {
    /// Operation is not registered or its arguments are invalid.
    #[error("application operation was rejected")]
    Rejected,
    /// Owned application authority is unavailable.
    #[error("application operation is unavailable")]
    Unavailable,
    /// Cancellation or deadline ended the operation.
    #[error("application operation was interrupted")]
    Interrupted,
}

impl DispatchError {
    const fn code(self) -> &'static str {
        match self {
            Self::Rejected => "rejected",
            Self::Unavailable => "unavailable",
            Self::Interrupted => "interrupted",
        }
    }
}

/// Fixed transport and payload ceilings for the private application surface.
#[derive(Clone, Copy, Debug)]
pub struct RuntimeRouterLimits {
    request_body_bytes: NonZeroUsize,
    response_body_bytes: NonZeroUsize,
    event_request_bytes: NonZeroUsize,
    maximum_concurrency: NonZeroUsize,
    event_cursor_lifetime: Duration,
    input_ticket_lifetime: Duration,
    request_structure: JsonStructureLimits,
    service_limits: ServiceLimits,
}

impl RuntimeRouterLimits {
    /// Creates positive route ceilings and bounded cursor/ticket lifetimes.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        request_body_bytes: usize,
        response_body_bytes: usize,
        event_request_bytes: usize,
        maximum_concurrency: usize,
        event_cursor_lifetime: Duration,
        input_ticket_lifetime: Duration,
        request_structure: JsonStructureLimits,
        service_limits: ServiceLimits,
    ) -> Result<Self, RouterError> {
        if event_cursor_lifetime.is_zero() || input_ticket_lifetime.is_zero() {
            return Err(RouterError::InvalidConfiguration);
        }
        Ok(Self {
            request_body_bytes: NonZeroUsize::new(request_body_bytes)
                .ok_or(RouterError::InvalidConfiguration)?,
            response_body_bytes: NonZeroUsize::new(response_body_bytes)
                .ok_or(RouterError::InvalidConfiguration)?,
            event_request_bytes: NonZeroUsize::new(event_request_bytes)
                .ok_or(RouterError::InvalidConfiguration)?,
            maximum_concurrency: NonZeroUsize::new(maximum_concurrency)
                .ok_or(RouterError::InvalidConfiguration)?,
            event_cursor_lifetime,
            input_ticket_lifetime,
            request_structure,
            service_limits,
        })
    }
}

/// Exact allowlist for browser-originated native application requests.
#[derive(Clone, Debug)]
pub struct OriginPolicy(Arc<[Box<str>]>);

impl OriginPolicy {
    /// Creates a non-wildcard allowlist; absent Origin remains valid for native non-browser clients.
    pub fn try_new(origins: impl IntoIterator<Item = String>) -> Result<Self, RouterError> {
        let mut admitted = Vec::new();
        for origin in origins {
            if origin.is_empty()
                || origin == "*"
                || origin.bytes().any(|byte| byte.is_ascii_control())
                || !(origin.starts_with("tauri://")
                    || origin.starts_with("http://")
                    || origin.starts_with("https://"))
            {
                return Err(RouterError::InvalidConfiguration);
            }
            admitted.push(origin.into_boxed_str());
        }
        Ok(Self(admitted.into()))
    }

    fn admits(&self, origin: Option<&str>) -> bool {
        origin.is_none_or(|candidate| self.0.iter().any(|allowed| allowed.as_ref() == candidate))
    }
}

/// Builder for the closed `/app/v1` surface and same-listener MCP composition seam.
#[derive(Debug)]
pub struct RuntimeRouter {
    state: Arc<RouterState>,
}

struct RouterState {
    runtime: RuntimeIdentity,
    endpoint: SocketAddr,
    protocols: ApplicationProtocolRange,
    origins: OriginPolicy,
    limits: RuntimeRouterLimits,
    credentials: Arc<CredentialRegistry>,
    clients: RuntimeClientActivity,
    dispatcher: Arc<dyn ApplicationDispatcher>,
    replay: Arc<MutationReplayGuard>,
    events: Arc<EventHub>,
    inputs: Arc<InputStager>,
    accepting: AtomicBool,
    request_cancellation: CancellationToken,
}

#[derive(Debug)]
struct RuntimeClientActivitySlot {
    client_id: ClientId,
    active_requests: AtomicUsize,
}

#[derive(Clone, Debug)]
struct RuntimeClientActivity {
    slots: Arc<[RuntimeClientActivitySlot]>,
}

impl RuntimeClientActivity {
    fn try_new(client_ids: Box<[ClientId]>) -> Result<Self, RouterError> {
        if client_ids.is_empty() {
            return Err(RouterError::InvalidConfiguration);
        }
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(client_ids.len())
            .map_err(|_| RouterError::Unavailable)?;
        slots.extend(client_ids.into_vec().into_iter().map(|client_id| {
            RuntimeClientActivitySlot {
                client_id,
                active_requests: AtomicUsize::new(0),
            }
        }));
        Ok(Self {
            slots: Arc::from(slots),
        })
    }

    fn begin(&self, client_id: ClientId) -> Result<RuntimeClientActivityGuard, RouterError> {
        let slot = self
            .slots
            .iter()
            .find(|slot| slot.client_id == client_id)
            .ok_or(RouterError::Unavailable)?;
        slot.active_requests
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map_err(|_| RouterError::Unavailable)?;
        Ok(RuntimeClientActivityGuard {
            activity: self.clone(),
            client_id,
        })
    }

    fn connected_clients(&self) -> usize {
        self.slots
            .iter()
            .filter(|slot| slot.active_requests.load(Ordering::Acquire) != 0)
            .count()
    }

    fn finish(&self, client_id: ClientId) {
        if let Some(slot) = self.slots.iter().find(|slot| slot.client_id == client_id) {
            let _prior =
                slot.active_requests
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                        current.checked_sub(1)
                    });
        }
    }
}

#[derive(Debug)]
struct RuntimeClientActivityGuard {
    activity: RuntimeClientActivity,
    client_id: ClientId,
}

impl Drop for RuntimeClientActivityGuard {
    fn drop(&mut self) {
        self.activity.finish(self.client_id);
    }
}

/// Read-only generation-scoped count of native clients with active requests.
#[derive(Clone, Debug)]
pub struct RuntimeClientActivityReader {
    activity: RuntimeClientActivity,
}

impl RuntimeClientActivityReader {
    /// Returns the exact number of distinct registered clients currently holding requests.
    #[must_use]
    pub fn connected_clients(&self) -> usize {
        self.activity.connected_clients()
    }
}

impl fmt::Debug for RouterState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RouterState")
            .field("runtime", &self.runtime)
            .field("endpoint", &self.endpoint)
            .field("protocols", &self.protocols)
            .field("origins", &self.origins)
            .field("limits", &self.limits)
            .field("credentials", &"[AUTHORITY]")
            .field("dispatcher", &"[APPLICATION AUTHORITY]")
            .field("replay", &self.replay)
            .field("events", &self.events)
            .field("inputs", &self.inputs)
            .finish()
    }
}

impl RuntimeRouter {
    /// Creates a router that can bind only the exact IPv4 loopback endpoint.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        runtime: RuntimeIdentity,
        endpoint: SocketAddr,
        protocols: ApplicationProtocolRange,
        origins: OriginPolicy,
        limits: RuntimeRouterLimits,
        credentials: Arc<CredentialRegistry>,
        dispatcher: Arc<dyn ApplicationDispatcher>,
        replay: Arc<MutationReplayGuard>,
        events: Arc<EventHub>,
        inputs: Arc<InputStager>,
    ) -> Result<Self, RouterError> {
        if endpoint.ip() != Ipv4Addr::LOCALHOST || endpoint.port() == 0 {
            return Err(RouterError::NonLoopback);
        }
        let clients = RuntimeClientActivity::try_new(
            credentials
                .registered_client_ids()
                .map_err(|_| RouterError::Unavailable)?,
        )?;
        Ok(Self {
            state: Arc::new(RouterState {
                runtime,
                endpoint,
                protocols,
                origins,
                limits,
                credentials,
                clients,
                dispatcher,
                replay,
                events,
                inputs,
                accepting: AtomicBool::new(true),
                request_cancellation: CancellationToken::new(),
            }),
        })
    }

    /// Child cancellation shared with same-listener protocol adapters for forced request drain.
    #[must_use]
    pub fn request_cancellation(&self) -> CancellationToken {
        self.state.request_cancellation.child_token()
    }

    /// Returns a read-only handle to exact native client activity for this runtime generation.
    #[must_use]
    pub fn client_activity_reader(&self) -> RuntimeClientActivityReader {
        RuntimeClientActivityReader {
            activity: self.state.clients.clone(),
        }
    }

    /// Builds the private routes and optionally merges one separately closed MCP router.
    pub fn into_router(self, mcp: Option<Router>) -> Router {
        let concurrency = self.state.limits.maximum_concurrency.get();
        let mut router = Router::new()
            .route("/health", get(health))
            .route("/app/v1/bootstrap", get(bootstrap))
            .route("/app/v1/invoke", post(invoke))
            .route("/app/v1/inputs", post(stage_input))
            .route("/app/v1/events", post(read_events))
            .with_state(self.state)
            .layer(ConcurrencyLimitLayer::new(concurrency));
        if let Some(mcp) = mcp {
            router = router.merge(mcp);
        }
        router
    }

    /// Starts one owned server after validating the exact bound loopback listener.
    pub fn start(
        self,
        listener: TcpListener,
        mcp: Option<Router>,
    ) -> Result<RuntimeServer, RouterError> {
        let local = listener
            .local_addr()
            .map_err(|_| RouterError::Unavailable)?;
        if local != self.state.endpoint || local.ip() != Ipv4Addr::LOCALHOST {
            return Err(RouterError::NonLoopback);
        }
        let state = Arc::clone(&self.state);
        let admission = CancellationToken::new();
        let shutdown = admission.clone();
        let router = self.into_router(mcp);
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
                .map_err(|_| RouterError::Unavailable)
        });
        Ok(RuntimeServer {
            state,
            admission,
            task: Some(task),
        })
    }
}

/// Owned listener task with separate admission, graceful drain, and hard request cancellation.
#[derive(Debug)]
pub struct RuntimeServer {
    state: Arc<RouterState>,
    admission: CancellationToken,
    task: Option<JoinHandle<Result<(), RouterError>>>,
}

impl RuntimeServer {
    /// Returns a read-only handle to exact native client activity for this runtime generation.
    #[must_use]
    pub fn client_activity_reader(&self) -> RuntimeClientActivityReader {
        RuntimeClientActivityReader {
            activity: self.state.clients.clone(),
        }
    }

    /// True when the listener task has already ended unexpectedly.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.task.as_ref().is_none_or(JoinHandle::is_finished)
    }

    /// Stops accepting new requests without shutting down application/domain authorities.
    pub fn begin_shutdown(&self) {
        self.state.accepting.store(false, Ordering::Release);
        self.admission.cancel();
    }

    /// Runs until caller cancellation or listener failure, then performs a bounded drain followed
    /// by hard cancellation of request contexts and task abortion if required.
    pub async fn run_until(
        mut self,
        cancellation: CancellationToken,
        graceful_drain: Duration,
        forced_drain: Duration,
    ) -> Result<(), RouterError> {
        if graceful_drain.is_zero() || forced_drain.is_zero() {
            return Err(RouterError::InvalidConfiguration);
        }
        let mut task = self.task.take().ok_or(RouterError::Unavailable)?;
        tokio::select! {
            result = &mut task => {
                return result.map_err(|_| RouterError::Unavailable)?;
            }
            () = cancellation.cancelled() => {}
        }
        self.begin_shutdown();
        if let Ok(result) = tokio::time::timeout(graceful_drain, &mut task).await {
            return result.map_err(|_| RouterError::Unavailable)?;
        }
        self.state.request_cancellation.cancel();
        if let Ok(result) = tokio::time::timeout(forced_drain, &mut task).await {
            return result.map_err(|_| RouterError::Unavailable)?;
        }
        task.abort();
        let _ = task.await;
        Err(RouterError::Unavailable)
    }
}

impl Drop for RuntimeServer {
    fn drop(&mut self) {
        self.state.accepting.store(false, Ordering::Release);
        self.admission.cancel();
        self.state.request_cancellation.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn health(State(state): State<Arc<RouterState>>, request: Request<Body>) -> Response {
    let _authentication = match authenticate_transport(&state, &request, Method::GET, None) {
        Ok(authentication) => authentication,
        Err(_status) => return rejected(StatusCode::UNAUTHORIZED),
    };
    axum::Json(json!({
        "status": "ready",
        "runtime": state.runtime,
        "protocol": ApplicationProtocolVersion::V1,
    }))
    .into_response()
}

async fn bootstrap(State(state): State<Arc<RouterState>>, request: Request<Body>) -> Response {
    let _authentication = match authenticate_transport(&state, &request, Method::GET, None) {
        Ok(authentication) => authentication,
        Err(_status) => return rejected(StatusCode::UNAUTHORIZED),
    };
    match state.dispatcher.bootstrap() {
        Ok(snapshot) => axum::Json(snapshot).into_response(),
        Err(DispatchError::Rejected) => rejected(StatusCode::BAD_REQUEST),
        Err(DispatchError::Unavailable | DispatchError::Interrupted) => {
            rejected(StatusCode::SERVICE_UNAVAILABLE)
        }
    }
}

async fn invoke(State(state): State<Arc<RouterState>>, request: Request<Body>) -> Response {
    let authentication =
        match authenticate_transport(&state, &request, Method::POST, Some(JSON_MEDIA_TYPE)) {
            Ok(value) => value,
            Err(status) => return rejected(status),
        };
    let body = match to_bytes(request.into_body(), state.limits.request_body_bytes.get()).await {
        Ok(body) => body,
        Err(_) => return rejected(StatusCode::PAYLOAD_TOO_LARGE),
    };
    let now = match wall_now() {
        Ok(now) => now,
        Err(_) => return rejected(StatusCode::SERVICE_UNAVAILABLE),
    };
    let envelope = match AppRequestEnvelope::decode(
        &body,
        now,
        state.limits.request_structure,
        state.limits.request_body_bytes.get(),
    ) {
        Ok(envelope) => envelope,
        Err(_) => return rejected(StatusCode::BAD_REQUEST),
    };
    if envelope.client_id() != authentication.client_id
        || envelope.credential_generation() != authentication.generation
        || state.runtime.admit(&envelope).is_err()
        || !state.protocols.contains(envelope.protocol())
    {
        return rejected(StatusCode::CONFLICT);
    }
    let context = match envelope.to_request_context(
        now,
        Instant::now(),
        state.request_cancellation.child_token(),
        state.limits.service_limits,
    ) {
        Ok(context) => context,
        Err(_) => return rejected(StatusCode::REQUEST_TIMEOUT),
    };
    match dispatch_request(&state, &envelope, context).await {
        Ok(response) => axum::Json(response).into_response(),
        Err(status) => rejected(status),
    }
}

async fn dispatch_request(
    state: &RouterState,
    envelope: &AppRequestEnvelope,
    context: RequestContext,
) -> Result<AppResponseEnvelope, StatusCode> {
    let effect = state
        .dispatcher
        .effect(envelope.operation())
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let execute = || async {
        let (result, succeeded) = match state.dispatcher.dispatch(envelope, context).await {
            Ok(value) => (json!({"ok": true, "value": value}), true),
            Err(error) => (json!({"ok": false, "error": error.code()}), false),
        };
        let response = AppResponseEnvelope::try_success(
            envelope.request_id().clone(),
            state.runtime.service_generation(),
            result,
            state.limits.service_limits.result_structure(),
            state.limits.response_body_bytes.get(),
        )
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        Ok((response, succeeded))
    };
    if effect == OperationEffect::Read {
        return execute().await.map(|(response, _succeeded)| response);
    }
    let digest = request_digest(envelope).map_err(|_| StatusCode::BAD_REQUEST)?;
    let key = ReplayKey::new(envelope.client_id(), envelope.request_id().clone());
    match state.replay.begin(key, digest) {
        Ok(ReplayAdmission::Completed(response)) => Ok(response),
        Ok(ReplayAdmission::Execute(permit)) => {
            let (response, succeeded) = execute().await?;
            permit
                .complete(response.clone())
                .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
            if succeeded {
                state
                    .dispatcher
                    .mutation_response_committed()
                    .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
                let _ = state.events.publish(json!({
                    "type": "application.changed",
                    "operation": envelope.operation(),
                    "requestId": envelope.request_id(),
                }));
            }
            Ok(response)
        }
        Err(_) => Err(StatusCode::CONFLICT),
    }
}

async fn stage_input(State(state): State<Arc<RouterState>>, request: Request<Body>) -> Response {
    let authentication =
        match authenticate_transport(&state, &request, Method::POST, Some(BINARY_MEDIA_TYPE)) {
            Ok(value) => value,
            Err(status) => return rejected(status),
        };
    let admission = match input_admission(request.headers()) {
        Ok(value) => value,
        Err(status) => return rejected(status),
    };
    let now = match wall_now() {
        Ok(now) => now,
        Err(_) => return rejected(StatusCode::SERVICE_UNAVAILABLE),
    };
    let expires_at = match add_duration(now, state.limits.input_ticket_lifetime) {
        Ok(value) => value,
        Err(_) => return rejected(StatusCode::SERVICE_UNAVAILABLE),
    };
    let mut stage = match state
        .inputs
        .begin(authentication.client_id, admission, expires_at, now)
    {
        Ok(stage) => stage,
        Err(_) => return rejected(StatusCode::BAD_REQUEST),
    };
    let mut stream = request.into_body().into_data_stream();
    while let Some(chunk) = tokio::select! {
        () = state.request_cancellation.cancelled() => {
            return rejected(StatusCode::SERVICE_UNAVAILABLE);
        }
        chunk = stream.next() => chunk,
    } {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(_) => return rejected(StatusCode::BAD_REQUEST),
        };
        if stage.write_chunk(&chunk).await.is_err() {
            return rejected(StatusCode::PAYLOAD_TOO_LARGE);
        }
    }
    let completed_at = match wall_now() {
        Ok(now) => now,
        Err(_) => return rejected(StatusCode::SERVICE_UNAVAILABLE),
    };
    match stage.finish(completed_at).await {
        Ok(ticket) => axum::Json(ticket).into_response(),
        Err(_) => rejected(StatusCode::BAD_REQUEST),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct EventReadRequest {
    cursor: Option<EventCursor>,
    limit: usize,
}

async fn read_events(State(state): State<Arc<RouterState>>, request: Request<Body>) -> Response {
    let authentication =
        match authenticate_transport(&state, &request, Method::POST, Some(JSON_MEDIA_TYPE)) {
            Ok(value) => value,
            Err(status) => return rejected(status),
        };
    let body = match to_bytes(request.into_body(), state.limits.event_request_bytes.get()).await {
        Ok(body) => body,
        Err(_) => return rejected(StatusCode::PAYLOAD_TOO_LARGE),
    };
    let request: EventReadRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => return rejected(StatusCode::BAD_REQUEST),
    };
    let limit = match EventPageLimit::try_new(request.limit) {
        Ok(limit) => limit,
        Err(_) => return rejected(StatusCode::BAD_REQUEST),
    };
    let now = match wall_now() {
        Ok(now) => now,
        Err(_) => return rejected(StatusCode::SERVICE_UNAVAILABLE),
    };
    let expires_at = match add_duration(now, state.limits.event_cursor_lifetime) {
        Ok(value) => value,
        Err(_) => return rejected(StatusCode::SERVICE_UNAVAILABLE),
    };
    match state.events.read_after(
        authentication.client_id,
        request.cursor.as_ref(),
        limit,
        now,
        expires_at,
    ) {
        Ok(page) => axum::Json(page).into_response(),
        Err(_) => rejected(StatusCode::GONE),
    }
}

struct AuthenticatedClient {
    client_id: ClientId,
    generation: CredentialGeneration,
    _activity: RuntimeClientActivityGuard,
}

fn authenticate_transport(
    state: &RouterState,
    request: &Request<Body>,
    method: Method,
    content_type: Option<&str>,
) -> Result<AuthenticatedClient, StatusCode> {
    if !state.accepting.load(Ordering::Acquire) {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    if request.method() != method {
        return Err(StatusCode::METHOD_NOT_ALLOWED);
    }
    let headers = request.headers();
    if headers.get(HOST).and_then(|value| value.to_str().ok()) != Some(&state.endpoint.to_string())
    {
        return Err(StatusCode::MISDIRECTED_REQUEST);
    }
    if content_type.is_some()
        && headers
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            != content_type
    {
        return Err(StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }
    let origin = headers.get(ORIGIN).and_then(|value| value.to_str().ok());
    if !state.origins.admits(origin) {
        return Err(StatusCode::FORBIDDEN);
    }
    let client_id = headers
        .get(CLIENT_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| Uuid::parse_str(value).ok())
        .and_then(|value| ClientId::try_from_uuid(value).ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let installation_id = headers
        .get(INSTALLATION_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| Uuid::parse_str(value).ok())
        .and_then(|value| crate::InstallationId::try_from_uuid(value).ok())
        .ok_or(StatusCode::CONFLICT)?;
    let workspace_id = headers
        .get(WORKSPACE_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| Uuid::parse_str(value).ok())
        .and_then(|value| crate::WorkspaceId::try_from_uuid(value).ok())
        .ok_or(StatusCode::CONFLICT)?;
    let service_generation = headers
        .get(SERVICE_GENERATION_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .and_then(|value| crate::ServiceGeneration::try_new(value).ok())
        .ok_or(StatusCode::CONFLICT)?;
    if installation_id != state.runtime.installation_id()
        || workspace_id != state.runtime.workspace_id()
        || service_generation != state.runtime.service_generation()
    {
        return Err(StatusCode::CONFLICT);
    }
    let generation = headers
        .get(CREDENTIAL_GENERATION_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .and_then(|value| CredentialGeneration::try_new(value).ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let authorization = headers
        .get(AUTHORIZATION)
        .map(|value| value.as_bytes())
        .and_then(|value| value.strip_prefix(AUTHORIZATION_PREFIX))
        .filter(|value| !value.is_empty())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    state
        .credentials
        .authenticate(client_id, generation, authorization)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    let activity = state
        .clients
        .begin(client_id)
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    Ok(AuthenticatedClient {
        client_id,
        generation,
        _activity: activity,
    })
}

fn input_admission(headers: &HeaderMap) -> Result<InputAdmission, StatusCode> {
    let media_type = headers
        .get(INPUT_MEDIA_TYPE_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| SourceIdentifier::try_from(value).ok())
        .ok_or(StatusCode::BAD_REQUEST)?;
    let expected_bytes = headers
        .get(INPUT_LENGTH_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or(StatusCode::BAD_REQUEST)?;
    let content_length = headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or(StatusCode::LENGTH_REQUIRED)?;
    if content_length != expected_bytes {
        return Err(StatusCode::BAD_REQUEST);
    }
    let digest = headers
        .get(INPUT_SHA256_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(decode_sha256)
        .ok_or(StatusCode::BAD_REQUEST)?;
    InputAdmission::try_new(media_type, expected_bytes, digest).map_err(|_| StatusCode::BAD_REQUEST)
}

fn request_digest(request: &AppRequestEnvelope) -> Result<EvidenceDigest, RouterError> {
    let encoded = serde_json::to_vec(request).map_err(|_| RouterError::Unavailable)?;
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(encoded).into(),
    ))
}

fn decode_sha256(value: &str) -> Option<EvidenceDigest> {
    if value.len() != 64 {
        return None;
    }
    let mut bytes = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = decode_nibble(pair[0])?;
        let low = decode_nibble(pair[1])?;
        bytes[index] = (high << 4) | low;
    }
    Some(EvidenceDigest::new(DigestAlgorithm::Sha256, bytes))
}

const fn decode_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

fn wall_now() -> Result<Timestamp, RouterError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| RouterError::Unavailable)?;
    let nanos = i64::try_from(duration.as_nanos()).map_err(|_| RouterError::Unavailable)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn add_duration(now: Timestamp, duration: Duration) -> Result<Timestamp, RouterError> {
    let nanos = i64::try_from(duration.as_nanos()).map_err(|_| RouterError::Unavailable)?;
    let value = now
        .unix_nanos()
        .checked_add(nanos)
        .ok_or(RouterError::Unavailable)?;
    Ok(Timestamp::from_unix_nanos(value))
}

fn rejected(status: StatusCode) -> Response {
    (status, axum::Json(json!({"error": "request_rejected"}))).into_response()
}

/// Runtime routing configuration or listener failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RouterError {
    /// Route bounds, origins, or endpoint are invalid.
    #[error("runtime router configuration is invalid")]
    InvalidConfiguration,
    /// Application listener must be exactly IPv4 loopback.
    #[error("runtime listener is not the configured loopback endpoint")]
    NonLoopback,
    /// Listener, clock, encoding, or serving failed closed.
    #[error("runtime router is unavailable")]
    Unavailable,
}
