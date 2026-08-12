//! One-owner Schwab Streamer execution with bounded reconnect and exact payload microbatches.

use std::collections::BTreeSet;
use std::fmt;
use std::future::Future;
use std::num::NonZeroU64;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::{SinkExt as _, StreamExt as _};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, Timestamp};
use market_squawk_platform::RawCaptureRecord;
use market_squawk_sources::{
    ProviderCaptureMaterial, ProviderCapturePageReceipt, ProviderCaptureSetReceipt,
    ProviderCaptureTerminalDisposition,
};
use sha2::{Digest as _, Sha256};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::{
    Error as WebSocketError, Message, error::CapacityError, protocol::WebSocketConfig,
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::{
    ConnectionGeneration, ConnectionState, DesiredStateController, ParseBounds, ParsedNative,
    SchwabAdapterError, StreamerAdmission, StreamerBootstrap, StreamerFrame, StreamerResponseCode,
    StreamerSubscription, TransientStreamerRequest, parse_streamer_frame,
};

use super::{
    AccessTokenAdmission, AccessTokenGeneration, SchwabAccessTokenSource, SchwabCaptureCoordinates,
    SchwabTransportError, SchwabTransportTelemetry, StreamerTransportBounds, TransientAccessToken,
    hash_frame, hash_observation, unix_millis, unix_seconds,
};

/// Exact application payload kind delivered by the WebSocket implementation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RawStreamerFrameKind {
    Text,
    Binary,
    Ping,
    Pong,
}

impl RawStreamerFrameKind {
    const fn digest_tag(self) -> u8 {
        match self {
            Self::Text => 1,
            Self::Binary => 2,
            Self::Ping => 3,
            Self::Pong => 4,
        }
    }
}

/// One exact bounded WebSocket application payload and local observation receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawStreamerFrame {
    generation: ConnectionGeneration,
    ordinal: NonZeroU64,
    kind: RawStreamerFrameKind,
    received_at_unix_millis: u64,
    payload_sha256: [u8; 32],
    payload: Bytes,
}

impl RawStreamerFrame {
    fn try_new(
        generation: ConnectionGeneration,
        ordinal: NonZeroU64,
        kind: RawStreamerFrameKind,
        payload: Bytes,
        maximum: usize,
    ) -> Result<Self, SchwabTransportError> {
        if payload.len() > maximum {
            return Err(SchwabTransportError::PayloadTooLarge);
        }
        Ok(Self {
            generation,
            ordinal,
            kind,
            received_at_unix_millis: unix_millis()?,
            payload_sha256: Sha256::digest(&payload).into(),
            payload,
        })
    }

    pub const fn generation(&self) -> ConnectionGeneration {
        self.generation
    }

    pub const fn ordinal(&self) -> NonZeroU64 {
        self.ordinal
    }

    pub const fn kind(&self) -> RawStreamerFrameKind {
        self.kind
    }

    pub const fn received_at_unix_millis(&self) -> u64 {
        self.received_at_unix_millis
    }

    pub const fn payload_sha256(&self) -> [u8; 32] {
        self.payload_sha256
    }

    pub const fn payload(&self) -> &Bytes {
        &self.payload
    }
}

/// Exact bounded microbatch receipt suitable for conversion into the shared raw capture writer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamerMicrobatchReceipt {
    generation: ConnectionGeneration,
    token_generation: AccessTokenGeneration,
    first_ordinal: NonZeroU64,
    last_ordinal: NonZeroU64,
    frame_count: u64,
    payload_bytes: u64,
    first_received_at_unix_millis: u64,
    last_received_at_unix_millis: u64,
    content_sha256: [u8; 32],
    observation_sha256: [u8; 32],
}

impl StreamerMicrobatchReceipt {
    pub const fn generation(&self) -> ConnectionGeneration {
        self.generation
    }

    pub const fn token_generation(&self) -> AccessTokenGeneration {
        self.token_generation
    }

    pub const fn first_ordinal(&self) -> NonZeroU64 {
        self.first_ordinal
    }

    pub const fn last_ordinal(&self) -> NonZeroU64 {
        self.last_ordinal
    }

    pub const fn frame_count(&self) -> u64 {
        self.frame_count
    }

    pub const fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }

    pub const fn first_received_at_unix_millis(&self) -> u64 {
        self.first_received_at_unix_millis
    }

    pub const fn last_received_at_unix_millis(&self) -> u64 {
        self.last_received_at_unix_millis
    }

    /// Digest of ordered exact frame kinds, lengths, and payload digests; excludes receive time.
    pub const fn content_sha256(&self) -> [u8; 32] {
        self.content_sha256
    }

    /// Digest binding generation, ordinals, local receive times, and payload digests.
    pub const fn observation_sha256(&self) -> [u8; 32] {
        self.observation_sha256
    }
}

/// Exact raw Streamer microbatch retained before canonical decoding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamerMicrobatch {
    receipt: StreamerMicrobatchReceipt,
    frames: Box<[RawStreamerFrame]>,
}

impl StreamerMicrobatch {
    pub const fn receipt(&self) -> &StreamerMicrobatchReceipt {
        &self.receipt
    }

    pub fn frames(&self) -> &[RawStreamerFrame] {
        &self.frames
    }

    /// Converts exact ordered market-data/notification frames into shared source-neutral capture
    /// material. Login responses, subscription acknowledgements, OAuth/token bytes, ping/pong
    /// control payloads, and `userPreference` account material never enter this microbatch.
    pub fn try_into_provider_capture_material(
        self,
        coordinates: SchwabCaptureCoordinates,
        event_ids: Vec<Uuid>,
    ) -> Result<ProviderCaptureMaterial, SchwabTransportError> {
        let frame_count = self.frames.len();
        if event_ids.len() != self.frames.len()
            || self.frames.is_empty()
            || self.frames.len() > market_squawk_sources::MAX_PROVIDER_CAPTURE_PAGES
        {
            return Err(SchwabTransportError::CaptureMaterial);
        }
        let request_set_identity = stream_request_set_identity(&self.receipt);
        let mut pages = Vec::new();
        let mut records = Vec::new();
        let mut previous_response_identity = None;
        pages
            .try_reserve_exact(self.frames.len())
            .map_err(|_| SchwabTransportError::CaptureMaterial)?;
        records
            .try_reserve_exact(self.frames.len())
            .map_err(|_| SchwabTransportError::CaptureMaterial)?;
        for (index, (frame, event_id)) in self
            .frames
            .into_vec()
            .into_iter()
            .zip(event_ids)
            .enumerate()
        {
            if event_id.is_nil()
                || frame.kind != RawStreamerFrameKind::Text
                || frame.generation != self.receipt.generation
            {
                return Err(SchwabTransportError::CaptureMaterial);
            }
            let ordinal =
                u16::try_from(index).map_err(|_| SchwabTransportError::CaptureMaterial)?;
            let received_nanos = i64::try_from(frame.received_at_unix_millis)
                .ok()
                .and_then(|value| value.checked_mul(1_000_000))
                .ok_or(SchwabTransportError::CaptureMaterial)?;
            let received = Timestamp::from_unix_nanos(received_nanos);
            let frame_identity = stream_frame_request_identity(request_set_identity, frame.ordinal);
            let next_identity = if index + 1 < frame_count {
                let next = NonZeroU64::new(
                    frame
                        .ordinal
                        .get()
                        .checked_add(1)
                        .ok_or(SchwabTransportError::CaptureMaterial)?,
                )
                .ok_or(SchwabTransportError::CaptureMaterial)?;
                Some(stream_frame_request_identity(request_set_identity, next))
            } else {
                None
            };
            pages.push(
                ProviderCapturePageReceipt::try_new(
                    ordinal,
                    frame_identity,
                    previous_response_identity,
                    next_identity,
                    200,
                    u64::try_from(frame.payload.len())
                        .map_err(|_| SchwabTransportError::CaptureMaterial)?,
                    EvidenceDigest::new(DigestAlgorithm::Sha256, frame.payload_sha256),
                    received,
                )
                .map_err(|_| SchwabTransportError::CaptureMaterial)?,
            );
            previous_response_identity = next_identity;
            records.push(
                RawCaptureRecord::try_new_live(
                    event_id,
                    Arc::from(coordinates.source_id().as_str()),
                    coordinates.connection_id(),
                    Some(u64::from(ordinal)),
                    None,
                    chrono::DateTime::from_timestamp_nanos(received_nanos),
                    frame.payload,
                )
                .map_err(|_| SchwabTransportError::CaptureMaterial)?,
            );
        }
        let receipt = ProviderCaptureSetReceipt::try_new(
            coordinates.source_id().clone(),
            coordinates.metadata_revision().clone(),
            coordinates.dataset().clone(),
            request_set_identity,
            ProviderCaptureTerminalDisposition::ExhaustedWithoutNextPage,
            pages,
        )
        .map_err(|_| SchwabTransportError::CaptureMaterial)?;
        ProviderCaptureMaterial::try_new(receipt, records)
            .map_err(|_| SchwabTransportError::CaptureMaterial)
    }
}

fn stream_request_set_identity(receipt: &StreamerMicrobatchReceipt) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/schwab-streamer-microbatch/v1");
    hasher.update(receipt.generation.get().to_be_bytes());
    hasher.update(receipt.first_ordinal.get().to_be_bytes());
    hasher.update(receipt.last_ordinal.get().to_be_bytes());
    hasher.update(receipt.content_sha256);
    EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into())
}

fn stream_frame_request_identity(
    request_set_identity: EvidenceDigest,
    ordinal: NonZeroU64,
) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/schwab-streamer-frame/v1");
    hasher.update(request_set_identity.bytes());
    hasher.update(ordinal.get().to_be_bytes());
    EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into())
}

/// Fail-closed nonblocking sink failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamerCaptureSinkError {
    Saturated,
    Closed,
    Integrity,
}

/// Application-owned nonblocking bridge into the shared raw capture authority.
pub trait StreamerCaptureSink: Send {
    fn try_publish(
        &mut self,
        microbatch: StreamerMicrobatch,
    ) -> Result<(), StreamerCaptureSinkError>;
}

/// Provider frame delivered by an injectable connection boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InboundStreamerFrame {
    Text(Bytes),
    Binary(Bytes),
    Ping(Bytes),
    Pong(Bytes),
    Close,
}

/// One connected Streamer wire. Implementations must not retain outbound login bytes.
pub trait SchwabStreamerConnection: fmt::Debug + Send {
    fn send_text<'a>(
        &'a mut self,
        payload: Bytes,
    ) -> Pin<Box<dyn Future<Output = Result<(), SchwabTransportError>> + Send + 'a>>;

    fn send_pong<'a>(
        &'a mut self,
        payload: Bytes,
    ) -> Pin<Box<dyn Future<Output = Result<(), SchwabTransportError>> + Send + 'a>>;

    fn next<'a>(
        &'a mut self,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<InboundStreamerFrame>, SchwabTransportError>>
                + Send
                + 'a,
        >,
    >;

    fn close<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), SchwabTransportError>> + Send + 'a>>;
}

/// Injectable Streamer connector used by the production WSS client and local mock proof.
pub trait SchwabStreamerConnector: fmt::Debug + Send + Sync {
    fn connect<'a>(
        &'a self,
        endpoint: &'a str,
        bounds: StreamerTransportBounds,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Box<dyn SchwabStreamerConnection>, SchwabTransportError>>
                + Send
                + 'a,
        >,
    >;
}

/// Production rustls WebSocket connector.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProductionSchwabStreamerConnector;

impl SchwabStreamerConnector for ProductionSchwabStreamerConnector {
    fn connect<'a>(
        &'a self,
        endpoint: &'a str,
        bounds: StreamerTransportBounds,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Box<dyn SchwabStreamerConnection>, SchwabTransportError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            validate_wss_endpoint(endpoint)?;
            let config = WebSocketConfig::default()
                .read_buffer_size(bounds.max_frame_bytes().clamp(4 * 1024, 128 * 1024))
                .write_buffer_size(16 * 1024)
                .max_write_buffer_size(64 * 1024)
                .max_message_size(Some(bounds.max_frame_bytes()))
                .max_frame_size(Some(bounds.max_frame_bytes()));
            let (socket, response) = connect_async_with_config(endpoint, Some(config), true)
                .await
                .map_err(map_websocket_error)?;
            if response.status().as_u16() != 101 {
                return Err(SchwabTransportError::Protocol);
            }
            Ok(Box::new(TungsteniteSchwabConnection { socket })
                as Box<dyn SchwabStreamerConnection>)
        })
    }
}

struct TungsteniteSchwabConnection {
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl fmt::Debug for TungsteniteSchwabConnection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TungsteniteSchwabConnection(..)")
    }
}

impl SchwabStreamerConnection for TungsteniteSchwabConnection {
    fn send_text<'a>(
        &'a mut self,
        payload: Bytes,
    ) -> Pin<Box<dyn Future<Output = Result<(), SchwabTransportError>> + Send + 'a>> {
        Box::pin(async move {
            let text =
                String::from_utf8(payload.to_vec()).map_err(|_| SchwabTransportError::Protocol)?;
            self.socket
                .send(Message::Text(text.into()))
                .await
                .map_err(map_websocket_error)
        })
    }

    fn send_pong<'a>(
        &'a mut self,
        payload: Bytes,
    ) -> Pin<Box<dyn Future<Output = Result<(), SchwabTransportError>> + Send + 'a>> {
        Box::pin(async move {
            self.socket
                .send(Message::Pong(payload))
                .await
                .map_err(map_websocket_error)
        })
    }

    fn next<'a>(
        &'a mut self,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<InboundStreamerFrame>, SchwabTransportError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            match self.socket.next().await {
                Some(Ok(Message::Text(value))) => Ok(Some(InboundStreamerFrame::Text(
                    Bytes::copy_from_slice(value.as_bytes()),
                ))),
                Some(Ok(Message::Binary(value))) => Ok(Some(InboundStreamerFrame::Binary(value))),
                Some(Ok(Message::Ping(value))) => Ok(Some(InboundStreamerFrame::Ping(value))),
                Some(Ok(Message::Pong(value))) => Ok(Some(InboundStreamerFrame::Pong(value))),
                Some(Ok(Message::Close(_))) | None => Ok(Some(InboundStreamerFrame::Close)),
                Some(Ok(Message::Frame(_))) => Err(SchwabTransportError::Protocol),
                Some(Err(error)) => Err(map_websocket_error(error)),
            }
        })
    }

    fn close<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), SchwabTransportError>> + Send + 'a>> {
        Box::pin(async move { self.socket.close(None).await.map_err(map_websocket_error) })
    }
}

/// Normal terminal exit for a run-until-cancelled Streamer owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamerRunExit {
    Cancelled,
}

/// Sole Streamer connection owner around the frozen desired-state controller.
pub struct SchwabStreamerExecutor {
    connector: Arc<dyn SchwabStreamerConnector>,
    token_source: Arc<dyn SchwabAccessTokenSource>,
    controller: DesiredStateController,
    transport_bounds: StreamerTransportBounds,
    parse_bounds: ParseBounds,
    token_admission: AccessTokenAdmission,
    telemetry: SchwabTransportTelemetry,
    next_generation: NonZeroU64,
}

impl fmt::Debug for SchwabStreamerExecutor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabStreamerExecutor")
            .field("connector", &self.connector)
            .field("token_source", &"[PROTECTED AUTHORITY]")
            .field("controller", &self.controller)
            .field("transport_bounds", &self.transport_bounds)
            .field("parse_bounds", &self.parse_bounds)
            .field("token_admission", &self.token_admission)
            .field("telemetry", &self.telemetry)
            .field("next_generation", &self.next_generation)
            .finish()
    }
}

impl SchwabStreamerExecutor {
    /// Builds the production WSS executor.
    pub fn try_production(
        token_source: Arc<dyn SchwabAccessTokenSource>,
        admission: StreamerAdmission,
        transport_bounds: StreamerTransportBounds,
        parse_bounds: ParseBounds,
        token_admission: AccessTokenAdmission,
        telemetry: SchwabTransportTelemetry,
    ) -> Result<Self, SchwabTransportError> {
        Self::try_new(
            Arc::new(ProductionSchwabStreamerConnector),
            token_source,
            admission,
            transport_bounds,
            parse_bounds,
            token_admission,
            telemetry,
        )
    }

    /// Injects a connector while retaining the same single-owner lifecycle and bounds.
    #[allow(
        clippy::too_many_arguments,
        reason = "transport, authority, lifecycle, and resource inputs remain explicit"
    )]
    pub fn try_new(
        connector: Arc<dyn SchwabStreamerConnector>,
        token_source: Arc<dyn SchwabAccessTokenSource>,
        admission: StreamerAdmission,
        transport_bounds: StreamerTransportBounds,
        parse_bounds: ParseBounds,
        token_admission: AccessTokenAdmission,
        telemetry: SchwabTransportTelemetry,
    ) -> Result<Self, SchwabTransportError> {
        if parse_bounds.max_response_bytes() > transport_bounds.max_frame_bytes() {
            return Err(SchwabTransportError::InvalidConfiguration);
        }
        Ok(Self {
            connector,
            token_source,
            controller: DesiredStateController::new(admission),
            transport_bounds,
            parse_bounds,
            token_admission,
            telemetry,
            next_generation: NonZeroU64::MIN,
        })
    }

    /// Returns the currently desired read-only services.
    pub fn desired(
        &self,
    ) -> &std::collections::BTreeMap<crate::MarketDataService, StreamerSubscription> {
        self.controller.desired()
    }

    /// Replaces one service's desired state while disconnected.
    pub fn replace_desired(
        &mut self,
        subscription: StreamerSubscription,
    ) -> Result<(), SchwabAdapterError> {
        if self.controller.state() != ConnectionState::Disconnected {
            return Err(SchwabAdapterError::InvalidStreamerState);
        }
        if self.controller.replace_desired(subscription)?.is_some() {
            return Err(SchwabAdapterError::InvalidStreamerState);
        }
        Ok(())
    }

    /// Adds keys/fields to one service's desired state while disconnected.
    pub fn add_desired(
        &mut self,
        subscription: StreamerSubscription,
    ) -> Result<(), SchwabAdapterError> {
        if self.controller.state() != ConnectionState::Disconnected {
            return Err(SchwabAdapterError::InvalidStreamerState);
        }
        if self.controller.add_desired(subscription)?.is_some() {
            return Err(SchwabAdapterError::InvalidStreamerState);
        }
        Ok(())
    }

    pub const fn telemetry(&self) -> &SchwabTransportTelemetry {
        &self.telemetry
    }

    /// Owns exactly one connection at a time, replaying desired state after bounded reconnects.
    pub async fn run(
        &mut self,
        bootstrap: &StreamerBootstrap,
        sink: &mut dyn StreamerCaptureSink,
        cancellation: CancellationToken,
    ) -> Result<StreamerRunExit, SchwabTransportError> {
        if self.controller.state() != ConnectionState::Disconnected {
            return Err(SchwabTransportError::Protocol);
        }
        validate_wss_endpoint(bootstrap.socket_url())?;
        let mut attempt = 0usize;
        loop {
            if cancellation.is_cancelled() {
                return Ok(StreamerRunExit::Cancelled);
            }
            if attempt > self.transport_bounds.max_reconnect_attempts() {
                return Err(SchwabTransportError::ReconnectExhausted);
            }
            if attempt > 0 {
                wait_reconnect(self.transport_bounds.reconnect_delay(), &cancellation).await?;
            }
            let reconnect = attempt > 0;
            self.telemetry.record_stream_connect_attempt(reconnect)?;
            let generation = self.take_generation()?;
            let token = match acquire_token(
                &*self.token_source,
                self.token_admission,
                self.transport_bounds.io_timeout(),
                &cancellation,
            )
            .await
            {
                Ok(token) => token,
                Err(SchwabTransportError::Cancelled) => return Ok(StreamerRunExit::Cancelled),
                Err(error) if retryable(error) => {
                    self.telemetry.record_stream_connect_failure()?;
                    attempt = attempt
                        .checked_add(1)
                        .ok_or(SchwabTransportError::Overflow)?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            self.controller.begin_connect(generation)?;
            let connection = await_operation(
                self.connector
                    .connect(bootstrap.socket_url(), self.transport_bounds),
                self.transport_bounds.connect_timeout(),
                &cancellation,
            )
            .await;
            let mut connection = match connection {
                Ok(connection) => connection,
                Err(SchwabTransportError::Cancelled) => {
                    self.controller.disconnected(generation)?;
                    return Ok(StreamerRunExit::Cancelled);
                }
                Err(error) if retryable(error) => {
                    self.controller.disconnected(generation)?;
                    self.telemetry.record_stream_connect_failure()?;
                    attempt = attempt
                        .checked_add(1)
                        .ok_or(SchwabTransportError::Overflow)?;
                    continue;
                }
                Err(error) => {
                    self.controller.disconnected(generation)?;
                    return Err(error);
                }
            };
            self.controller.socket_connected(generation)?;
            let token_generation = token.generation();
            let refresh_deadline = token_refresh_deadline(&token, self.token_admission)?;
            let outcome = self
                .run_connection(
                    generation,
                    token_generation,
                    refresh_deadline,
                    token,
                    bootstrap,
                    &mut *connection,
                    sink,
                    &cancellation,
                )
                .await;
            match outcome {
                Ok(ConnectionExit::Cancelled) => {
                    self.controller.disconnected(generation)?;
                    self.telemetry.record_stream_clean_close()?;
                    return Ok(StreamerRunExit::Cancelled);
                }
                Ok(ConnectionExit::Retry) => {
                    self.controller.disconnected(generation)?;
                    self.telemetry.record_stream_disconnect()?;
                    attempt = attempt
                        .checked_add(1)
                        .ok_or(SchwabTransportError::Overflow)?;
                }
                Err(SchwabTransportError::Cancelled) => {
                    self.controller.disconnected(generation)?;
                    self.telemetry.record_stream_clean_close()?;
                    return Ok(StreamerRunExit::Cancelled);
                }
                Err(error) if retryable(error) => {
                    self.controller.disconnected(generation)?;
                    self.telemetry.record_stream_disconnect()?;
                    attempt = attempt
                        .checked_add(1)
                        .ok_or(SchwabTransportError::Overflow)?;
                }
                Err(error) => {
                    self.controller.disconnected(generation)?;
                    return Err(error);
                }
            }
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "one connection generation keeps authority and capture binding explicit"
    )]
    async fn run_connection(
        &mut self,
        generation: ConnectionGeneration,
        token_generation: AccessTokenGeneration,
        refresh_deadline: Instant,
        token: TransientAccessToken,
        bootstrap: &StreamerBootstrap,
        connection: &mut dyn SchwabStreamerConnection,
        sink: &mut dyn StreamerCaptureSink,
        cancellation: &CancellationToken,
    ) -> Result<ConnectionExit, SchwabTransportError> {
        let mut batch = MicrobatchBuilder::new(generation, token_generation, self.transport_bounds);
        let login = self
            .controller
            .login_request(bootstrap, token.expose_bearer())?;
        let login_id = login.request_id().get().to_string();
        send_request(
            connection,
            login,
            &self.telemetry,
            self.transport_bounds.io_timeout(),
            cancellation,
        )
        .await?;
        drop(token);
        let login_deadline = Instant::now()
            .checked_add(self.transport_bounds.io_timeout())
            .ok_or(SchwabTransportError::Overflow)?;
        loop {
            let incoming = match read_until(
                connection,
                login_deadline,
                cancellation,
                self.transport_bounds.io_timeout(),
            )
            .await
            {
                Ok(incoming) => incoming,
                Err(SchwabTransportError::Cancelled) => {
                    flush_batch(&mut batch, sink, &self.telemetry)?;
                    close_with_deadline(connection, self.transport_bounds.io_timeout()).await;
                    return Ok(ConnectionExit::Cancelled);
                }
                Err(error) if retryable(error) => {
                    flush_batch(&mut batch, sink, &self.telemetry)?;
                    return Ok(ConnectionExit::Retry);
                }
                Err(error) => {
                    flush_batch(&mut batch, sink, &self.telemetry)?;
                    return Err(error);
                }
            };
            match self
                .process_incoming(
                    generation,
                    connection,
                    incoming,
                    &mut batch,
                    sink,
                    cancellation,
                )
                .await?
            {
                ProcessedFrame::Parsed(frame) => {
                    if let Some(response) = frame.value().responses.iter().find(|response| {
                        response.service.as_ref() == "ADMIN"
                            && response.command.as_ref() == "LOGIN"
                            && response.request_id.as_ref() == login_id
                    }) {
                        if response.code != StreamerResponseCode::Success {
                            flush_batch(&mut batch, sink, &self.telemetry)?;
                            return Ok(ConnectionExit::Retry);
                        }
                        self.controller.login_accepted(generation)?;
                        self.telemetry.record_stream_connected()?;
                        break;
                    }
                }
                ProcessedFrame::Control => {}
                ProcessedFrame::Closed => {
                    flush_batch(&mut batch, sink, &self.telemetry)?;
                    return Ok(ConnectionExit::Retry);
                }
            }
        }

        let requests = self.controller.replay_desired()?;
        let mut pending = BTreeSet::new();
        for request in requests {
            pending.insert(request.request_id().get().to_string());
            send_request(
                connection,
                request,
                &self.telemetry,
                self.transport_bounds.io_timeout(),
                cancellation,
            )
            .await?;
        }
        let mut idle_deadline = Instant::now()
            .checked_add(self.transport_bounds.io_timeout())
            .ok_or(SchwabTransportError::Overflow)?;
        loop {
            let flush_deadline = batch.flush_deadline();
            let next_deadline = idle_deadline.min(refresh_deadline).min(flush_deadline);
            match read_or_deadline(connection, next_deadline, cancellation).await {
                Ok(incoming) => {
                    idle_deadline = Instant::now()
                        .checked_add(self.transport_bounds.io_timeout())
                        .ok_or(SchwabTransportError::Overflow)?;
                    match self
                        .process_incoming(
                            generation,
                            connection,
                            incoming,
                            &mut batch,
                            sink,
                            cancellation,
                        )
                        .await?
                    {
                        ProcessedFrame::Parsed(frame) => {
                            for response in &frame.value().responses {
                                if response.code != StreamerResponseCode::Success {
                                    flush_batch(&mut batch, sink, &self.telemetry)?;
                                    return Err(SchwabTransportError::Adapter);
                                }
                                if !pending.remove(response.request_id.as_ref()) {
                                    flush_batch(&mut batch, sink, &self.telemetry)?;
                                    return Err(SchwabTransportError::Protocol);
                                }
                            }
                        }
                        ProcessedFrame::Control => {}
                        ProcessedFrame::Closed => {
                            flush_batch(&mut batch, sink, &self.telemetry)?;
                            return Ok(ConnectionExit::Retry);
                        }
                    }
                }
                Err(SchwabTransportError::Deadline) => {
                    let now = Instant::now();
                    if now >= refresh_deadline || now >= idle_deadline {
                        flush_batch(&mut batch, sink, &self.telemetry)?;
                        close_with_deadline(connection, self.transport_bounds.io_timeout()).await;
                        return Ok(ConnectionExit::Retry);
                    }
                    flush_batch(&mut batch, sink, &self.telemetry)?;
                }
                Err(SchwabTransportError::Cancelled) => {
                    flush_batch(&mut batch, sink, &self.telemetry)?;
                    close_with_deadline(connection, self.transport_bounds.io_timeout()).await;
                    return Ok(ConnectionExit::Cancelled);
                }
                Err(error) if retryable(error) => {
                    flush_batch(&mut batch, sink, &self.telemetry)?;
                    return Ok(ConnectionExit::Retry);
                }
                Err(error) => return Err(error),
            }
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "exact frame capture and connection ownership remain explicit"
    )]
    async fn process_incoming(
        &self,
        generation: ConnectionGeneration,
        connection: &mut dyn SchwabStreamerConnection,
        incoming: InboundStreamerFrame,
        batch: &mut MicrobatchBuilder,
        sink: &mut dyn StreamerCaptureSink,
        cancellation: &CancellationToken,
    ) -> Result<ProcessedFrame, SchwabTransportError> {
        let payload = match incoming {
            InboundStreamerFrame::Text(payload) => payload,
            InboundStreamerFrame::Ping(payload) => {
                self.telemetry.record_stream_frame(
                    u64::try_from(payload.len()).map_err(|_| SchwabTransportError::Overflow)?,
                )?;
                await_operation(
                    connection.send_pong(payload),
                    self.transport_bounds.io_timeout(),
                    cancellation,
                )
                .await?;
                return Ok(ProcessedFrame::Control);
            }
            InboundStreamerFrame::Pong(payload) => {
                self.telemetry.record_stream_frame(
                    u64::try_from(payload.len()).map_err(|_| SchwabTransportError::Overflow)?,
                )?;
                return Ok(ProcessedFrame::Control);
            }
            InboundStreamerFrame::Binary(payload) => {
                self.telemetry.record_stream_frame(
                    u64::try_from(payload.len()).map_err(|_| SchwabTransportError::Overflow)?,
                )?;
                return Err(SchwabTransportError::Protocol);
            }
            InboundStreamerFrame::Close => return Ok(ProcessedFrame::Closed),
        };
        self.telemetry.record_stream_frame(
            u64::try_from(payload.len()).map_err(|_| SchwabTransportError::Overflow)?,
        )?;
        let parsed = match parse_streamer_frame(&payload, self.parse_bounds) {
            Ok(parsed) => parsed,
            Err(error) => {
                self.telemetry.record_validation_failure()?;
                flush_batch(batch, sink, &self.telemetry)?;
                return Err(error.into());
            }
        };
        let events = parsed.value().data.iter().try_fold(0u64, |total, data| {
            let count =
                u64::try_from(data.content.len()).map_err(|_| SchwabTransportError::Overflow)?;
            total
                .checked_add(count)
                .ok_or(SchwabTransportError::Overflow)
        })?;
        let responses = u64::try_from(parsed.value().responses.len())
            .map_err(|_| SchwabTransportError::Overflow)?;
        let notifications = u64::try_from(parsed.value().notifications.len())
            .map_err(|_| SchwabTransportError::Overflow)?;
        self.telemetry
            .record_stream_semantics(events, responses, notifications)?;
        if !parsed.value().data.is_empty() || !parsed.value().notifications.is_empty() {
            append_frame(
                generation,
                RawStreamerFrameKind::Text,
                payload,
                batch,
                sink,
                &self.telemetry,
            )?;
        }
        Ok(ProcessedFrame::Parsed(parsed))
    }

    fn take_generation(&mut self) -> Result<ConnectionGeneration, SchwabTransportError> {
        let current = self.next_generation;
        self.next_generation = NonZeroU64::new(
            current
                .get()
                .checked_add(1)
                .ok_or(SchwabTransportError::Overflow)?,
        )
        .ok_or(SchwabTransportError::Overflow)?;
        Ok(ConnectionGeneration::new(current))
    }
}

enum ConnectionExit {
    Cancelled,
    Retry,
}

enum ProcessedFrame {
    Parsed(ParsedNative<StreamerFrame>),
    Control,
    Closed,
}

struct MicrobatchBuilder {
    generation: ConnectionGeneration,
    token_generation: AccessTokenGeneration,
    bounds: StreamerTransportBounds,
    frames: Vec<RawStreamerFrame>,
    payload_bytes: usize,
    next_ordinal: NonZeroU64,
    opened_at: Instant,
}

impl MicrobatchBuilder {
    fn new(
        generation: ConnectionGeneration,
        token_generation: AccessTokenGeneration,
        bounds: StreamerTransportBounds,
    ) -> Self {
        Self {
            generation,
            token_generation,
            bounds,
            frames: Vec::new(),
            payload_bytes: 0,
            next_ordinal: NonZeroU64::MIN,
            opened_at: Instant::now(),
        }
    }

    fn flush_deadline(&self) -> Instant {
        self.opened_at
            .checked_add(self.bounds.microbatch_flush_interval())
            .unwrap_or(self.opened_at)
    }

    fn would_exceed(&self, additional: usize) -> Result<bool, SchwabTransportError> {
        let bytes = self
            .payload_bytes
            .checked_add(additional)
            .ok_or(SchwabTransportError::Overflow)?;
        Ok(!self.frames.is_empty()
            && (self.frames.len() >= self.bounds.max_microbatch_frames()
                || bytes > self.bounds.max_microbatch_bytes()))
    }

    fn push(
        &mut self,
        kind: RawStreamerFrameKind,
        payload: Bytes,
    ) -> Result<(), SchwabTransportError> {
        if payload.len() > self.bounds.max_frame_bytes()
            || payload.len() > self.bounds.max_microbatch_bytes()
        {
            return Err(SchwabTransportError::PayloadTooLarge);
        }
        let frame = RawStreamerFrame::try_new(
            self.generation,
            self.next_ordinal,
            kind,
            payload,
            self.bounds.max_frame_bytes(),
        )?;
        self.payload_bytes = self
            .payload_bytes
            .checked_add(frame.payload.len())
            .ok_or(SchwabTransportError::Overflow)?;
        self.next_ordinal = NonZeroU64::new(
            self.next_ordinal
                .get()
                .checked_add(1)
                .ok_or(SchwabTransportError::Overflow)?,
        )
        .ok_or(SchwabTransportError::Overflow)?;
        self.frames
            .try_reserve(1)
            .map_err(|_| SchwabTransportError::PayloadTooLarge)?;
        self.frames.push(frame);
        Ok(())
    }

    fn finish(&mut self) -> Result<Option<StreamerMicrobatch>, SchwabTransportError> {
        if self.frames.is_empty() {
            self.opened_at = Instant::now();
            return Ok(None);
        }
        let frames = std::mem::take(&mut self.frames);
        let first = frames.first().ok_or(SchwabTransportError::Protocol)?;
        let last = frames.last().ok_or(SchwabTransportError::Protocol)?;
        let mut content = Sha256::new();
        let mut observation = Sha256::new();
        for frame in &frames {
            hash_frame(&mut content, frame.kind.digest_tag(), &frame.payload)?;
            hash_observation(
                &mut observation,
                frame.generation,
                frame.ordinal,
                frame.received_at_unix_millis,
                frame.payload_sha256,
            );
        }
        let frame_count =
            u64::try_from(frames.len()).map_err(|_| SchwabTransportError::Overflow)?;
        let payload_bytes =
            u64::try_from(self.payload_bytes).map_err(|_| SchwabTransportError::Overflow)?;
        let receipt = StreamerMicrobatchReceipt {
            generation: self.generation,
            token_generation: self.token_generation,
            first_ordinal: first.ordinal,
            last_ordinal: last.ordinal,
            frame_count,
            payload_bytes,
            first_received_at_unix_millis: first.received_at_unix_millis,
            last_received_at_unix_millis: last.received_at_unix_millis,
            content_sha256: content.finalize().into(),
            observation_sha256: observation.finalize().into(),
        };
        self.payload_bytes = 0;
        self.opened_at = Instant::now();
        Ok(Some(StreamerMicrobatch {
            receipt,
            frames: frames.into_boxed_slice(),
        }))
    }
}

fn append_frame(
    _generation: ConnectionGeneration,
    kind: RawStreamerFrameKind,
    payload: Bytes,
    batch: &mut MicrobatchBuilder,
    sink: &mut dyn StreamerCaptureSink,
    telemetry: &SchwabTransportTelemetry,
) -> Result<(), SchwabTransportError> {
    if batch.would_exceed(payload.len())? {
        flush_batch(batch, sink, telemetry)?;
    }
    batch.push(kind, payload)
}

fn flush_batch(
    batch: &mut MicrobatchBuilder,
    sink: &mut dyn StreamerCaptureSink,
    telemetry: &SchwabTransportTelemetry,
) -> Result<(), SchwabTransportError> {
    let Some(microbatch) = batch.finish()? else {
        return Ok(());
    };
    let frames = microbatch.receipt().frame_count();
    let bytes = microbatch.receipt().payload_bytes();
    sink.try_publish(microbatch)
        .map_err(|_| SchwabTransportError::CaptureRejected)?;
    telemetry.record_stream_microbatch(frames, bytes)
}

async fn send_request(
    connection: &mut dyn SchwabStreamerConnection,
    request: TransientStreamerRequest,
    telemetry: &SchwabTransportTelemetry,
    timeout: Duration,
    cancellation: &CancellationToken,
) -> Result<(), SchwabTransportError> {
    let bytes = Bytes::copy_from_slice(request.expose_body());
    let length = u64::try_from(bytes.len()).map_err(|_| SchwabTransportError::Overflow)?;
    await_operation(connection.send_text(bytes), timeout, cancellation).await?;
    telemetry.record_stream_request(length)
}

async fn acquire_token(
    source: &dyn SchwabAccessTokenSource,
    admission: AccessTokenAdmission,
    timeout: Duration,
    cancellation: &CancellationToken,
) -> Result<TransientAccessToken, SchwabTransportError> {
    let operation = async { source.acquire().await.map_err(SchwabTransportError::from) };
    let token = await_operation(operation, timeout, cancellation).await?;
    token.validate_at(unix_seconds()?, admission)?;
    Ok(token)
}

fn token_refresh_deadline(
    token: &TransientAccessToken,
    admission: AccessTokenAdmission,
) -> Result<Instant, SchwabTransportError> {
    let now = unix_seconds()?;
    let refresh_at = token
        .expires_at_unix_seconds()
        .checked_sub(admission.minimum_remaining_lifetime().as_secs())
        .ok_or(SchwabTransportError::TokenRefreshRequired)?;
    if refresh_at <= now {
        return Err(SchwabTransportError::TokenRefreshRequired);
    }
    Instant::now()
        .checked_add(Duration::from_secs(refresh_at - now))
        .ok_or(SchwabTransportError::Overflow)
}

async fn read_until(
    connection: &mut dyn SchwabStreamerConnection,
    hard_deadline: Instant,
    cancellation: &CancellationToken,
    maximum_wait: Duration,
) -> Result<InboundStreamerFrame, SchwabTransportError> {
    let remaining = hard_deadline.saturating_duration_since(Instant::now());
    await_operation(connection.next(), remaining.min(maximum_wait), cancellation)
        .await?
        .ok_or(SchwabTransportError::ResynchronizationRequired)
}

async fn read_or_deadline(
    connection: &mut dyn SchwabStreamerConnection,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<InboundStreamerFrame, SchwabTransportError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    await_operation(connection.next(), remaining, cancellation)
        .await?
        .ok_or(SchwabTransportError::ResynchronizationRequired)
}

async fn await_operation<T>(
    operation: impl Future<Output = Result<T, SchwabTransportError>>,
    timeout: Duration,
    cancellation: &CancellationToken,
) -> Result<T, SchwabTransportError> {
    if timeout.is_zero() {
        return Err(SchwabTransportError::Deadline);
    }
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(SchwabTransportError::Cancelled),
        result = tokio::time::timeout(timeout, operation) => {
            result.map_err(|_| SchwabTransportError::Deadline)?
        }
    }
}

async fn wait_reconnect(
    delay: Duration,
    cancellation: &CancellationToken,
) -> Result<(), SchwabTransportError> {
    if delay.is_zero() {
        return if cancellation.is_cancelled() {
            Err(SchwabTransportError::Cancelled)
        } else {
            Ok(())
        };
    }
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(SchwabTransportError::Cancelled),
        () = tokio::time::sleep(delay) => Ok(()),
    }
}

async fn close_with_deadline(connection: &mut dyn SchwabStreamerConnection, timeout: Duration) {
    let _ignored = tokio::time::timeout(timeout, connection.close()).await;
}

fn retryable(error: SchwabTransportError) -> bool {
    matches!(
        error,
        SchwabTransportError::Network
            | SchwabTransportError::Deadline
            | SchwabTransportError::TokenRefreshRequired
            | SchwabTransportError::TokenAuthorityUnavailable
            | SchwabTransportError::ResynchronizationRequired
    )
}

fn validate_wss_endpoint(endpoint: &str) -> Result<(), SchwabTransportError> {
    let url = Url::parse(endpoint).map_err(|_| SchwabTransportError::Protocol)?;
    if url.scheme() != "wss"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(SchwabTransportError::Protocol);
    }
    Ok(())
}

fn map_websocket_error(error: WebSocketError) -> SchwabTransportError {
    match error {
        WebSocketError::Capacity(CapacityError::MessageTooLong { .. }) => {
            SchwabTransportError::PayloadTooLarge
        }
        WebSocketError::Http(response) if matches!(response.status().as_u16(), 401 | 403) => {
            SchwabTransportError::TokenRefreshRequired
        }
        WebSocketError::Http(_) => SchwabTransportError::Protocol,
        _ => SchwabTransportError::Network,
    }
}
