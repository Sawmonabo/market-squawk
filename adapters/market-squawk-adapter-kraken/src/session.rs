//! Bounded one-generation WebSocket session.

use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use market_squawk_domain::{LiveEventClass, Timestamp};
use market_squawk_sources::{
    BudgetDecision, LiveMarketSource, RawFrameFactory, RawMarketSink, SourceError, SourceMetadata,
    SourceMetadataProvider, TransportFrameKind,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::connect_async_tls_with_config;
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};
use tokio_util::sync::CancellationToken;

use crate::{
    KrakenChannel, KrakenConfig, KrakenControl, KrakenDecodeOutcome, KrakenDecoder,
    KrakenDecoderState, KrakenSubscription,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_SOCKET_OPERATION_TIMEOUT: Duration = Duration::from_secs(5);
const READ_BUFFER_BYTES: usize = 64 * 1024;
const WRITE_BUFFER_BYTES: usize = 8 * 1024;
const MAX_WRITE_BUFFER_BYTES: usize = 128 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SessionDeadlines {
    receive_idle: Duration,
    write: Duration,
    close: Duration,
}

impl SessionDeadlines {
    fn from_metadata(metadata: &SourceMetadata) -> Self {
        let receive_idle =
            Duration::from_nanos(metadata.freshness_policy().max_connection_idle_nanos());
        let operation = receive_idle.min(MAX_SOCKET_OPERATION_TIMEOUT);
        Self {
            receive_idle,
            write: operation,
            close: operation,
        }
    }
}

/// Non-authoritative operational counters for the current source instance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KrakenHealth {
    state: KrakenDecoderState,
    captured_frames: u64,
    market_messages: u64,
    control_messages: u64,
    last_market_timestamp: Option<Timestamp>,
    book_subscribed: bool,
    trade_subscribed: bool,
}

impl KrakenHealth {
    /// Returns book synchronization and quarantine state.
    pub const fn state(self) -> KrakenDecoderState {
        self.state
    }

    /// Returns the number of exact frames accepted by capture.
    pub const fn captured_frames(self) -> u64 {
        self.captured_frames
    }

    /// Returns the number of decoded market messages.
    pub const fn market_messages(self) -> u64 {
        self.market_messages
    }

    /// Returns the number of decoded connection/control messages.
    pub const fn control_messages(self) -> u64 {
        self.control_messages
    }

    /// Returns the last provider market timestamp, excluding heartbeats.
    pub const fn last_market_timestamp(self) -> Option<Timestamp> {
        self.last_market_timestamp
    }

    /// Returns whether the configured book subscription was acknowledged.
    pub const fn book_subscribed(self) -> bool {
        self.book_subscribed
    }

    /// Returns whether the configured trade subscription was acknowledged.
    pub const fn trade_subscribed(self) -> bool {
        self.trade_subscribed
    }
}

/// Production Kraken Spot WebSocket v2 source.
#[derive(Debug)]
pub struct KrakenSource {
    config: KrakenConfig,
    health: KrakenHealth,
}

impl KrakenSource {
    /// Constructs a source from checked immutable configuration.
    pub const fn new(config: KrakenConfig) -> Self {
        Self {
            config,
            health: KrakenHealth {
                state: KrakenDecoderState::AwaitingSnapshot,
                captured_frames: 0,
                market_messages: 0,
                control_messages: 0,
                last_market_timestamp: None,
                book_subscribed: false,
                trade_subscribed: false,
            },
        }
    }

    /// Returns the current non-authoritative operational snapshot.
    pub const fn health(&self) -> KrakenHealth {
        self.health
    }

    async fn run_generation(
        &mut self,
        frames: &mut RawFrameFactory,
        sink: &mut dyn RawMarketSink,
        cancellation: CancellationToken,
    ) -> Result<(), SourceError> {
        self.health.state = KrakenDecoderState::AwaitingSnapshot;
        self.health.book_subscribed = false;
        self.health.trade_subscribed = false;
        self.health.last_market_timestamp = None;
        let permit = match self.config.budget().try_acquire() {
            BudgetDecision::Ready(permit) => permit,
            BudgetDecision::WaitUntil(_) | BudgetDecision::Unavailable(_) => {
                return Err(SourceError::ProviderUnavailable);
            }
        };
        let socket_config = WebSocketConfig::default()
            .read_buffer_size(READ_BUFFER_BYTES)
            .write_buffer_size(WRITE_BUFFER_BYTES)
            .max_write_buffer_size(MAX_WRITE_BUFFER_BYTES)
            .max_message_size(Some(self.config.max_message_bytes()))
            .max_frame_size(Some(self.config.max_message_bytes()));
        let connect = connect_async_tls_with_config(
            self.config.endpoint().as_str(),
            Some(socket_config),
            true,
            None,
        );
        let (mut socket, response) = tokio::select! {
            _ = cancellation.cancelled() => return Err(SourceError::Cancelled),
            result = tokio::time::timeout(CONNECT_TIMEOUT, connect) => {
                let result = result.map_err(|_| SourceError::Network)?;
                result.map_err(|error| map_connect_error(error, self.config.budget()))?
            }
        };
        drop(permit);
        if response.status().is_redirection() {
            self.health.state = KrakenDecoderState::Quarantined;
            return Err(SourceError::InvalidProtocolState);
        }
        if self.config.budget().record_success().is_err() {
            self.health.state = KrakenDecoderState::Quarantined;
            return Err(SourceError::ProviderUnavailable);
        }
        self.run_established(&mut socket, frames, sink, cancellation)
            .await
    }

    async fn run_established<S>(
        &mut self,
        socket: &mut WebSocketStream<S>,
        frames: &mut RawFrameFactory,
        sink: &mut dyn RawMarketSink,
        cancellation: CancellationToken,
    ) -> Result<(), SourceError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let deadlines = SessionDeadlines::from_metadata(self.config.metadata());
        let result = self
            .run_established_inner(socket, frames, sink, &cancellation, deadlines)
            .await;
        if result.is_err() {
            self.health.state = KrakenDecoderState::Quarantined;
        }
        if result == Err(SourceError::Cancelled) {
            close_with_deadline(socket, deadlines.close).await;
        }
        result
    }

    async fn run_established_inner<S>(
        &mut self,
        socket: &mut WebSocketStream<S>,
        frames: &mut RawFrameFactory,
        sink: &mut dyn RawMarketSink,
        cancellation: &CancellationToken,
        deadlines: SessionDeadlines,
    ) -> Result<(), SourceError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let (channel, depth) = match self.config.channel() {
            KrakenChannel::Book(depth) => ("book", Some(depth.get())),
            KrakenChannel::Trades => ("trade", None),
        };
        let subscribe_payload = subscription(self.config.symbol(), channel, depth)?;
        send_subscription(
            socket,
            self.config.budget(),
            subscribe_payload,
            cancellation,
            deadlines.write,
        )
        .await?;

        let mut decoder = match self.config.channel() {
            KrakenChannel::Book(depth) => {
                KrakenDecoder::try_new(self.config.symbol(), self.config.instrument(), depth)
            }
            KrakenChannel::Trades => {
                KrakenDecoder::try_trades(self.config.symbol(), self.config.instrument())
            }
        }
        .map_err(|_| SourceError::InvalidProtocolState)?;
        loop {
            let message = tokio::select! {
                _ = cancellation.cancelled() => return Err(SourceError::Cancelled),
                result = tokio::time::timeout(deadlines.receive_idle, socket.next()) => {
                    match result {
                        Ok(message) => message,
                        Err(_) => {
                            self.health.state = KrakenDecoderState::Quarantined;
                            return Err(SourceError::ConnectionIdle);
                        }
                    }
                },
            };
            let Some(message) = message else {
                self.health.state = KrakenDecoderState::Quarantined;
                return Err(SourceError::Network);
            };
            let message = match message {
                Ok(message) => message,
                Err(error) => {
                    self.health.state = KrakenDecoderState::Quarantined;
                    return Err(map_websocket_error(error));
                }
            };
            match message {
                Message::Text(text) => {
                    let payload = Bytes::copy_from_slice(text.as_bytes());
                    self.capture_then_decode(
                        frames,
                        sink,
                        &mut decoder,
                        TransportFrameKind::Text,
                        payload,
                    )?;
                }
                Message::Binary(binary) => {
                    let payload = Bytes::copy_from_slice(binary.as_ref());
                    let frame = match frames.try_frame(TransportFrameKind::Binary, payload) {
                        Ok(frame) => frame,
                        Err(error) => {
                            self.health.state = KrakenDecoderState::Quarantined;
                            return Err(error);
                        }
                    };
                    if let Err(error) = sink.try_publish(frame) {
                        self.health.state = KrakenDecoderState::Quarantined;
                        return Err(SourceError::Sink(error));
                    }
                    self.health.captured_frames = self
                        .health
                        .captured_frames
                        .checked_add(1)
                        .ok_or(SourceError::InvalidProtocolState)?;
                    self.health.state = KrakenDecoderState::Quarantined;
                    return Err(SourceError::InvalidProtocolState);
                }
                Message::Ping(payload) => {
                    if let Err(error) = send_message_with_deadline(
                        socket,
                        Message::Pong(payload),
                        cancellation,
                        deadlines.write,
                    )
                    .await
                    {
                        self.health.state = KrakenDecoderState::Quarantined;
                        return Err(error);
                    }
                }
                Message::Pong(_) => {}
                Message::Close(_) => {
                    self.health.state = KrakenDecoderState::Quarantined;
                    return Err(SourceError::Network);
                }
                Message::Frame(_) => {
                    self.health.state = KrakenDecoderState::Quarantined;
                    return Err(SourceError::InvalidProtocolState);
                }
            }
        }
    }

    fn capture_then_decode(
        &mut self,
        frames: &mut RawFrameFactory,
        sink: &mut dyn RawMarketSink,
        decoder: &mut KrakenDecoder,
        transport: TransportFrameKind,
        payload: Bytes,
    ) -> Result<(), SourceError> {
        if payload.len() > self.config.max_message_bytes() {
            self.health.state = KrakenDecoderState::Quarantined;
            return Err(SourceError::FrameTooLarge {
                max: self.config.max_message_bytes(),
            });
        }
        let frame = match frames.try_frame(transport, payload.clone()) {
            Ok(frame) => frame,
            Err(error) => {
                self.health.state = KrakenDecoderState::Quarantined;
                return Err(error);
            }
        };
        if let Err(error) = sink.try_publish(frame) {
            self.health.state = KrakenDecoderState::Quarantined;
            return Err(SourceError::Sink(error));
        }
        self.health.captured_frames = self
            .health
            .captured_frames
            .checked_add(1)
            .ok_or(SourceError::InvalidProtocolState)?;
        match decoder.decode_payload(payload.as_ref()) {
            Ok(KrakenDecodeOutcome::Market(observations)) => {
                let acknowledged =
                    observations
                        .iter()
                        .all(|observation| match observation.event_class() {
                            LiveEventClass::Trade => self.health.trade_subscribed,
                            LiveEventClass::BookSnapshot | LiveEventClass::BookDelta => {
                                self.health.book_subscribed
                            }
                            _ => false,
                        });
                if !acknowledged {
                    self.health.state = KrakenDecoderState::Quarantined;
                    return Err(SourceError::InvalidProtocolState);
                }
                self.health.market_messages = self
                    .health
                    .market_messages
                    .checked_add(1)
                    .ok_or(SourceError::InvalidProtocolState)?;
                self.health.last_market_timestamp =
                    observations
                        .last()
                        .and_then(|observation| {
                            match observation.timestamp() {
                        market_squawk_sources::ProviderTimestampEvidence::Provided {
                            value,
                            ..
                        } => Some(*value),
                        market_squawk_sources::ProviderTimestampEvidence::AuthoritativelyAbsent(
                            _,
                        ) => None,
                    }
                        });
            }
            Ok(KrakenDecodeOutcome::Control(control)) => {
                match control {
                    KrakenControl::Subscribed(KrakenSubscription::Book) => {
                        if self.health.book_subscribed {
                            self.health.state = KrakenDecoderState::Quarantined;
                            return Err(SourceError::InvalidProtocolState);
                        }
                        self.health.book_subscribed = true;
                    }
                    KrakenControl::Subscribed(KrakenSubscription::Trade) => {
                        if self.health.trade_subscribed {
                            self.health.state = KrakenDecoderState::Quarantined;
                            return Err(SourceError::InvalidProtocolState);
                        }
                        self.health.trade_subscribed = true;
                    }
                    KrakenControl::Heartbeat | KrakenControl::Pong | KrakenControl::Online => {}
                }
                self.health.control_messages = self
                    .health
                    .control_messages
                    .checked_add(1)
                    .ok_or(SourceError::InvalidProtocolState)?;
            }
            Err(_) => {
                self.health.state = KrakenDecoderState::Quarantined;
                return Err(SourceError::InvalidProtocolState);
            }
        }
        self.health.state = decoder.state();
        Ok(())
    }
}

impl SourceMetadataProvider for KrakenSource {
    fn metadata(&self) -> &SourceMetadata {
        self.config.metadata()
    }
}

impl LiveMarketSource for KrakenSource {
    fn run<'a>(
        &'a mut self,
        frames: &'a mut RawFrameFactory,
        sink: &'a mut dyn RawMarketSink,
        cancellation: CancellationToken,
    ) -> futures_util::future::BoxFuture<'a, Result<(), SourceError>> {
        Box::pin(self.run_generation(frames, sink, cancellation))
    }
}

fn subscription(symbol: &str, channel: &str, depth: Option<usize>) -> Result<String, SourceError> {
    let mut params = serde_json::Map::new();
    params.insert(
        "channel".to_owned(),
        serde_json::Value::String(channel.to_owned()),
    );
    params.insert(
        "symbol".to_owned(),
        serde_json::Value::Array(vec![serde_json::Value::String(symbol.to_owned())]),
    );
    params.insert("snapshot".to_owned(), serde_json::Value::Bool(true));
    if let Some(depth) = depth {
        params.insert("depth".to_owned(), serde_json::Value::from(depth));
    }
    serde_json::to_string(&serde_json::json!({
        "method": "subscribe",
        "params": params,
    }))
    .map_err(|_| SourceError::InvalidProtocolState)
}

async fn send_subscription<S>(
    socket: &mut S,
    budget: &market_squawk_sources::SharedProviderBudget,
    payload: String,
    cancellation: &CancellationToken,
    deadline: Duration,
) -> Result<(), SourceError>
where
    S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let permit = match budget.try_acquire() {
        BudgetDecision::Ready(permit) => permit,
        BudgetDecision::WaitUntil(_) | BudgetDecision::Unavailable(_) => {
            return Err(SourceError::ProviderUnavailable);
        }
    };
    send_message_with_deadline(
        socket,
        Message::Text(payload.into()),
        cancellation,
        deadline,
    )
    .await?;
    drop(permit);
    budget
        .record_success()
        .map_err(|_| SourceError::ProviderUnavailable)
}

async fn send_message_with_deadline<S>(
    socket: &mut S,
    message: Message,
    cancellation: &CancellationToken,
    deadline: Duration,
) -> Result<(), SourceError>
where
    S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    tokio::select! {
        _ = cancellation.cancelled() => Err(SourceError::Cancelled),
        result = tokio::time::timeout(deadline, socket.send(message)) => {
            let result = result.map_err(|_| SourceError::Network)?;
            result.map_err(map_websocket_error)
        }
    }
}

async fn close_with_deadline<S>(socket: &mut WebSocketStream<S>, deadline: Duration)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let _close_result = tokio::time::timeout(deadline, socket.close(None)).await;
}

fn map_websocket_error(error: tokio_tungstenite::tungstenite::Error) -> SourceError {
    match error {
        tokio_tungstenite::tungstenite::Error::Http(response)
            if response.status().is_redirection() =>
        {
            SourceError::InvalidProtocolState
        }
        tokio_tungstenite::tungstenite::Error::Capacity(_) => SourceError::FrameTooLarge {
            max: market_squawk_sources::MAX_RAW_FRAME_BYTES,
        },
        _ => SourceError::Network,
    }
}

fn map_connect_error(
    error: tokio_tungstenite::tungstenite::Error,
    budget: &market_squawk_sources::SharedProviderBudget,
) -> SourceError {
    if let tokio_tungstenite::tungstenite::Error::Http(response) = &error
        && (response.status().as_u16() == 429 || response.status().is_server_error())
    {
        let _backoff = budget.apply_refusal(1_000);
        return SourceError::ProviderUnavailable;
    }
    map_websocket_error(error)
}

#[cfg(test)]
#[path = "session_tests.rs"]
mod tests;
