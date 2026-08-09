use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::{FutureExt as _, SinkExt as _, StreamExt as _, future::BoxFuture};
use market_squawk_sources::{
    ActiveLiveSourceGeneration, BudgetDecision, BudgetPermit, LiveMarketSource,
    LiveSourceGeneration, RawMarketSink, SharedProviderBudget, SourceError, SourceMetadata,
    SourceMetadataProvider, TransportFrameKind, apply_http_retry_after,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
use tokio_tungstenite::tungstenite::http::header::{CONTENT_TYPE, HeaderName, HeaderValue};
use tokio_tungstenite::tungstenite::{
    Error as WebSocketError, Message, error::CapacityError, protocol::WebSocketConfig,
};
use tokio_tungstenite::{WebSocketStream, connect_async_with_config};
use tokio_util::sync::CancellationToken;

use crate::{
    AlpacaCredentials, AlpacaIexLiveConfig, AlpacaOptionsLiveConfig, AlpacaTransportLimits,
};

const KEY_ID_HEADER: HeaderName = HeaderName::from_static("apca-api-key-id");
const SECRET_KEY_HEADER: HeaderName = HeaderName::from_static("apca-api-secret-key");

/// One registry-authorized Alpaca Basic IEX connection generation.
#[derive(Debug)]
pub struct AlpacaIexLiveSource {
    config: AlpacaIexLiveConfig,
    credentials: Arc<AlpacaCredentials>,
    authority: ActiveLiveSourceGeneration,
    budget: SharedProviderBudget,
    generation_started: bool,
}

impl AlpacaIexLiveSource {
    /// Binds credentials and an exact registry-minted generation to IEX-only source metadata.
    pub fn try_new(
        config: AlpacaIexLiveConfig,
        generation: LiveSourceGeneration,
        credentials: Arc<AlpacaCredentials>,
    ) -> Result<Self, SourceError> {
        let authority = generation.try_start(config.metadata())?;
        let budget = authority
            .budget()?
            .cloned()
            .ok_or(SourceError::GenerationAuthorityMismatch)?;
        Ok(Self {
            config,
            credentials,
            authority,
            budget,
            generation_started: false,
        })
    }

    async fn run_production(
        &mut self,
        sink: &mut dyn RawMarketSink,
        cancellation: CancellationToken,
    ) -> Result<(), SourceError> {
        begin_generation(&mut self.generation_started)?;
        validate_generation(&self.authority, &self.budget)?;
        self.config
            .metadata()
            .network_policy()
            .authorize(self.config.endpoint())
            .map_err(|_| SourceError::InvalidProtocolState)?;
        run_transport(
            self.config.endpoint(),
            self.config.limits(),
            SubscriptionPayload::Json(self.config.subscription()),
            false,
            &self.credentials,
            &mut self.authority,
            &self.budget,
            sink,
            cancellation,
        )
        .await
    }
}

impl SourceMetadataProvider for AlpacaIexLiveSource {
    fn metadata(&self) -> &SourceMetadata {
        self.config.metadata()
    }
}

impl LiveMarketSource for AlpacaIexLiveSource {
    fn run<'a>(
        &'a mut self,
        sink: &'a mut dyn RawMarketSink,
        cancellation: CancellationToken,
    ) -> BoxFuture<'a, Result<(), SourceError>> {
        self.run_production(sink, cancellation).boxed()
    }
}

/// One registry-authorized Alpaca Basic indicative-options connection generation.
#[derive(Debug)]
pub struct AlpacaOptionsLiveSource {
    config: AlpacaOptionsLiveConfig,
    credentials: Arc<AlpacaCredentials>,
    authority: ActiveLiveSourceGeneration,
    budget: SharedProviderBudget,
    generation_started: bool,
}

impl AlpacaOptionsLiveSource {
    /// Binds credentials and an exact registry-minted generation to the MessagePack-only feed.
    pub fn try_new(
        config: AlpacaOptionsLiveConfig,
        generation: LiveSourceGeneration,
        credentials: Arc<AlpacaCredentials>,
    ) -> Result<Self, SourceError> {
        let authority = generation.try_start(config.metadata())?;
        let budget = authority
            .budget()?
            .cloned()
            .ok_or(SourceError::GenerationAuthorityMismatch)?;
        Ok(Self {
            config,
            credentials,
            authority,
            budget,
            generation_started: false,
        })
    }

    async fn run_production(
        &mut self,
        sink: &mut dyn RawMarketSink,
        cancellation: CancellationToken,
    ) -> Result<(), SourceError> {
        begin_generation(&mut self.generation_started)?;
        validate_generation(&self.authority, &self.budget)?;
        self.config
            .metadata()
            .network_policy()
            .authorize(self.config.endpoint())
            .map_err(|_| SourceError::InvalidProtocolState)?;
        run_transport(
            self.config.endpoint(),
            self.config.limits(),
            SubscriptionPayload::MessagePack(self.config.subscription()),
            true,
            &self.credentials,
            &mut self.authority,
            &self.budget,
            sink,
            cancellation,
        )
        .await
    }
}

impl SourceMetadataProvider for AlpacaOptionsLiveSource {
    fn metadata(&self) -> &SourceMetadata {
        self.config.metadata()
    }
}

impl LiveMarketSource for AlpacaOptionsLiveSource {
    fn run<'a>(
        &'a mut self,
        sink: &'a mut dyn RawMarketSink,
        cancellation: CancellationToken,
    ) -> BoxFuture<'a, Result<(), SourceError>> {
        self.run_production(sink, cancellation).boxed()
    }
}

#[derive(Clone, Copy)]
enum SubscriptionPayload<'a> {
    Json(&'a str),
    MessagePack(&'a [u8]),
}

#[allow(
    clippy::too_many_arguments,
    reason = "transport authority inputs stay explicit"
)]
async fn run_transport(
    endpoint: &str,
    limits: AlpacaTransportLimits,
    subscription: SubscriptionPayload<'_>,
    messagepack: bool,
    credentials: &AlpacaCredentials,
    authority: &mut ActiveLiveSourceGeneration,
    budget: &SharedProviderBudget,
    sink: &mut dyn RawMarketSink,
    cancellation: CancellationToken,
) -> Result<(), SourceError> {
    if cancellation.is_cancelled() {
        return Err(SourceError::Cancelled);
    }
    authority.validate_current()?;
    let permit = acquire_budget(budget)?;
    let request = authenticated_request(endpoint, credentials, messagepack)?;
    let websocket_config = WebSocketConfig::default()
        .read_buffer_size(limits.max_frame_bytes().clamp(4 * 1024, 128 * 1024))
        .write_buffer_size(16 * 1024)
        .max_write_buffer_size(64 * 1024)
        .max_message_size(Some(limits.max_frame_bytes()))
        .max_frame_size(Some(limits.max_frame_bytes()));
    let connect = connect_async_with_config(request, Some(websocket_config), true);
    let (mut socket, response) =
        await_websocket(&cancellation, limits.connect_timeout(), connect, |error| {
            map_connect_error(error, budget)
        })
        .await?;
    drop(response);
    sink.bind_active_request_budget(permit.active_lease())?;
    let message = match subscription {
        SubscriptionPayload::Json(payload) => Message::Text(payload.into()),
        SubscriptionPayload::MessagePack(payload) => {
            Message::Binary(Bytes::copy_from_slice(payload))
        }
    };
    send_with_deadline(&mut socket, message, &cancellation, limits.io_timeout()).await?;
    let mut provider_message_observed = false;
    loop {
        let message = read_with_deadline(
            &mut socket,
            sink,
            &cancellation,
            limits.io_timeout(),
            limits.max_frame_bytes(),
        )
        .await?;
        match message {
            Message::Text(text) => {
                publish(
                    authority,
                    sink,
                    TransportFrameKind::Text,
                    Bytes::copy_from_slice(text.as_bytes()),
                    limits.max_frame_bytes(),
                )?;
                record_first_provider_message(budget, &mut provider_message_observed)?;
            }
            Message::Binary(payload) => {
                publish(
                    authority,
                    sink,
                    TransportFrameKind::Binary,
                    payload,
                    limits.max_frame_bytes(),
                )?;
                record_first_provider_message(budget, &mut provider_message_observed)?;
            }
            Message::Ping(payload) => {
                send_with_deadline(
                    &mut socket,
                    Message::Pong(payload),
                    &cancellation,
                    limits.io_timeout(),
                )
                .await?;
            }
            Message::Pong(_) => {}
            Message::Close(_) => {
                flush_with_deadline(&mut socket, &cancellation, limits.io_timeout()).await?;
                return Err(SourceError::GenerationResynchronizationRequired);
            }
            Message::Frame(_) => return Err(SourceError::InvalidProtocolState),
        }
    }
}

fn authenticated_request(
    endpoint: &str,
    credentials: &AlpacaCredentials,
    messagepack: bool,
) -> Result<tokio_tungstenite::tungstenite::http::Request<()>, SourceError> {
    let mut request = endpoint
        .into_client_request()
        .map_err(|_| SourceError::InvalidProtocolState)?;
    let mut key_id =
        HeaderValue::from_str(credentials.key_id()).map_err(|_| SourceError::Unauthorized)?;
    key_id.set_sensitive(true);
    let mut secret =
        HeaderValue::from_str(credentials.secret_key()).map_err(|_| SourceError::Unauthorized)?;
    secret.set_sensitive(true);
    request.headers_mut().insert(KEY_ID_HEADER, key_id);
    request.headers_mut().insert(SECRET_KEY_HEADER, secret);
    if messagepack {
        request.headers_mut().insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/msgpack"),
        );
    }
    Ok(request)
}

fn begin_generation(started: &mut bool) -> Result<(), SourceError> {
    if *started {
        return Err(SourceError::InvalidProtocolState);
    }
    *started = true;
    Ok(())
}

fn validate_generation(
    authority: &ActiveLiveSourceGeneration,
    budget: &SharedProviderBudget,
) -> Result<(), SourceError> {
    let issued = authority
        .budget()?
        .ok_or(SourceError::GenerationAuthorityMismatch)?;
    if !budget.shares_allocation_with(issued) {
        return Err(SourceError::GenerationAuthorityMismatch);
    }
    Ok(())
}

fn acquire_budget(budget: &SharedProviderBudget) -> Result<BudgetPermit, SourceError> {
    match budget.try_acquire() {
        BudgetDecision::Ready(permit) => Ok(permit),
        BudgetDecision::WaitUntil(deadline) => Err(SourceError::BudgetWaitUntil { deadline }),
        BudgetDecision::Unavailable(reason) => Err(SourceError::BudgetUnavailable { reason }),
    }
}

fn publish(
    authority: &mut ActiveLiveSourceGeneration,
    sink: &mut dyn RawMarketSink,
    kind: TransportFrameKind,
    payload: Bytes,
    maximum: usize,
) -> Result<(), SourceError> {
    if payload.len() > maximum {
        return Err(SourceError::FrameTooLarge { max: maximum });
    }
    let frame = authority.frames_mut()?.try_frame(kind, payload)?;
    sink.try_publish(frame)?;
    Ok(())
}

async fn await_websocket<T, E, F>(
    cancellation: &CancellationToken,
    deadline: Duration,
    operation: impl Future<Output = Result<T, E>>,
    map_error: F,
) -> Result<T, SourceError>
where
    F: FnOnce(E) -> SourceError,
{
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(SourceError::Cancelled),
        result = tokio::time::timeout(deadline, operation) => match result {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => Err(map_error(error)),
            Err(_) => Err(SourceError::Network),
        }
    }
}

async fn send_with_deadline<S>(
    socket: &mut WebSocketStream<S>,
    message: Message,
    cancellation: &CancellationToken,
    deadline: Duration,
) -> Result<(), SourceError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    await_websocket(cancellation, deadline, socket.send(message), |_| {
        SourceError::Network
    })
    .await
}

async fn flush_with_deadline<S>(
    socket: &mut WebSocketStream<S>,
    cancellation: &CancellationToken,
    deadline: Duration,
) -> Result<(), SourceError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    await_websocket(cancellation, deadline, socket.flush(), |_| {
        SourceError::Network
    })
    .await
}

async fn read_with_deadline<S>(
    socket: &mut WebSocketStream<S>,
    sink: &mut dyn RawMarketSink,
    cancellation: &CancellationToken,
    transport_timeout: Duration,
    maximum_frame_bytes: usize,
) -> Result<Message, SourceError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let transport = Instant::now()
        .checked_add(transport_timeout)
        .ok_or(SourceError::InvalidProtocolState)?;
    let (deadline, sink_owned) = match sink.next_deadline() {
        Some(sink_deadline) if sink_deadline <= transport => (sink_deadline, true),
        _ => (transport, false),
    };
    let next = tokio::select! {
        biased;
        () = cancellation.cancelled() => return Err(SourceError::Cancelled),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            if sink_owned {
                sink.poll_deadline(Instant::now())?;
                return Err(SourceError::InvalidProtocolState);
            }
            return Err(SourceError::Network);
        }
        result = socket.next() => result,
    };
    match next {
        Some(Ok(message)) => Ok(message),
        Some(Err(WebSocketError::Capacity(CapacityError::MessageTooLong { .. }))) => {
            Err(SourceError::FrameTooLarge {
                max: maximum_frame_bytes,
            })
        }
        Some(Err(_)) => Err(SourceError::Network),
        None => Err(SourceError::GenerationResynchronizationRequired),
    }
}

fn record_first_provider_message(
    budget: &SharedProviderBudget,
    observed: &mut bool,
) -> Result<(), SourceError> {
    if !*observed {
        budget
            .record_success()
            .map_err(|_| SourceError::ProviderUnavailable)?;
        *observed = true;
    }
    Ok(())
}

fn map_connect_error(error: WebSocketError, budget: &SharedProviderBudget) -> SourceError {
    if let WebSocketError::Http(response) = &error {
        let status = response.status().as_u16();
        if matches!(status, 401 | 403) {
            return SourceError::Unauthorized;
        }
        if status == 429 || status >= 500 {
            return SourceError::from_applied_budget_refusal(apply_http_retry_after(
                budget,
                response
                    .headers()
                    .get(tokio_tungstenite::tungstenite::http::header::RETRY_AFTER)
                    .map(|value| value.as_bytes()),
                1_000,
            ));
        }
    }
    SourceError::Network
}
