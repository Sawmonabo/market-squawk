//! One-generation Coinbase transport implementation.

use std::future::Future;

use bytes::Bytes;
use futures_util::{FutureExt, SinkExt, StreamExt, future::BoxFuture};
use market_squawk_sources::{
    BudgetDecision, BudgetPermit, CurrentSourceSession, LiveMarketSource, RawFrameFactory,
    RawMarketSink, SharedProviderBudget, SourceError, SourceMetadata, SourceMetadataProvider,
    TransportFrameKind, apply_http_retry_after,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::tungstenite::{Error as WebSocketError, Message, protocol::WebSocketConfig};
use tokio_tungstenite::{WebSocketStream, connect_async_with_config};
use tokio_util::sync::CancellationToken;

use crate::{CoinbaseConfigError, CoinbaseExchangeConfig};

/// Production Coinbase Exchange one-generation source.
#[derive(Debug)]
pub struct CoinbaseExchangeSource {
    config: CoinbaseExchangeConfig,
    budget: SharedProviderBudget,
    generation_started: bool,
}

impl CoinbaseExchangeSource {
    /// Binds a validated configuration to the exact registry-coordinated session budget.
    ///
    /// # Errors
    ///
    /// Rejects a session from another source/revision or a session without the required shared
    /// provider budget.
    pub fn try_new(
        config: CoinbaseExchangeConfig,
        session: &CurrentSourceSession,
    ) -> Result<Self, CoinbaseConfigError> {
        if session.source_id() != config.metadata().source_id()
            || session.revision() != config.metadata().revision()
        {
            return Err(CoinbaseConfigError::SessionMismatch);
        }
        let budget = session
            .budget()
            .cloned()
            .ok_or(CoinbaseConfigError::MissingSharedBudget)?;
        Ok(Self {
            config,
            budget,
            generation_started: false,
        })
    }

    fn begin_generation(&mut self) -> Result<(), SourceError> {
        if self.generation_started {
            return Err(SourceError::InvalidProtocolState);
        }
        self.generation_started = true;
        Ok(())
    }

    fn acquire_budget(&self) -> Result<BudgetPermit, SourceError> {
        match self.budget.try_acquire() {
            BudgetDecision::Ready(permit) => Ok(permit),
            BudgetDecision::WaitUntil(deadline) => Err(SourceError::BudgetWaitUntil { deadline }),
            BudgetDecision::Unavailable(reason) => Err(SourceError::BudgetUnavailable { reason }),
        }
    }

    async fn run_production(
        &mut self,
        frames: &mut RawFrameFactory,
        sink: &mut dyn RawMarketSink,
        cancellation: CancellationToken,
    ) -> Result<(), SourceError> {
        self.begin_generation()?;
        if cancellation.is_cancelled() {
            return Err(SourceError::Cancelled);
        }
        self.config
            .metadata()
            .network_policy()
            .authorize(self.config.endpoint())
            .map_err(|_| SourceError::InvalidProtocolState)?;
        let permit = self.acquire_budget()?;
        let limits = self.config.transport_limits();
        let websocket_config = WebSocketConfig::default()
            .read_buffer_size(limits.max_frame_bytes().clamp(4 * 1024, 128 * 1024))
            .write_buffer_size(16 * 1024)
            .max_write_buffer_size(32 * 1024)
            .max_message_size(Some(limits.max_frame_bytes()))
            .max_frame_size(Some(limits.max_frame_bytes()));
        let connect =
            connect_async_with_config(self.config.endpoint(), Some(websocket_config), true);
        let (socket, _response) =
            await_websocket(&cancellation, limits.connect_timeout(), connect, |error| {
                map_connect_error(error, &self.budget)
            })
            .await?;
        self.run_socket(socket, permit, frames, sink, cancellation)
            .await
    }

    async fn run_socket<S>(
        &self,
        mut socket: WebSocketStream<S>,
        _permit: BudgetPermit,
        frames: &mut RawFrameFactory,
        sink: &mut dyn RawMarketSink,
        cancellation: CancellationToken,
    ) -> Result<(), SourceError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let limits = self.config.transport_limits();
        send_with_deadline(
            &mut socket,
            Message::Text(self.config.subscription().into()),
            &cancellation,
            limits.io_timeout(),
        )
        .await?;
        self.budget
            .record_success()
            .map_err(|_| SourceError::ProviderUnavailable)?;

        loop {
            let message =
                read_with_deadline(&mut socket, &cancellation, limits.io_timeout()).await?;
            match message {
                Message::Text(text) => {
                    let payload = text.as_bytes();
                    ensure_frame_bound(payload.len(), limits.max_frame_bytes())?;
                    let frame = frames
                        .try_frame(TransportFrameKind::Text, Bytes::copy_from_slice(payload))?;
                    sink.try_publish(frame)?;
                }
                Message::Binary(payload) => {
                    ensure_frame_bound(payload.len(), limits.max_frame_bytes())?;
                    let frame = frames.try_frame(TransportFrameKind::Binary, payload)?;
                    sink.try_publish(frame)?;
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
                Message::Close(frame) => {
                    let _provider_close = frame;
                    flush_with_deadline(&mut socket, &cancellation, limits.io_timeout()).await?;
                    return Err(SourceError::ProviderUnavailable);
                }
                Message::Frame(_) => return Err(SourceError::InvalidProtocolState),
            }
        }
    }

    #[cfg(test)]
    async fn run_with_socket_for_test<S>(
        &mut self,
        socket: WebSocketStream<S>,
        frames: &mut RawFrameFactory,
        sink: &mut dyn RawMarketSink,
        cancellation: CancellationToken,
    ) -> Result<(), SourceError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        self.begin_generation()?;
        if cancellation.is_cancelled() {
            return Err(SourceError::Cancelled);
        }
        let permit = self.acquire_budget()?;
        self.run_socket(socket, permit, frames, sink, cancellation)
            .await
    }

    #[cfg(test)]
    fn begin_generation_for_test(&mut self) -> Result<(), SourceError> {
        self.begin_generation()
    }
}

impl SourceMetadataProvider for CoinbaseExchangeSource {
    fn metadata(&self) -> &SourceMetadata {
        self.config.metadata()
    }
}

impl LiveMarketSource for CoinbaseExchangeSource {
    fn run<'a>(
        &'a mut self,
        frames: &'a mut RawFrameFactory,
        sink: &'a mut dyn RawMarketSink,
        cancellation: CancellationToken,
    ) -> BoxFuture<'a, Result<(), SourceError>> {
        self.run_production(frames, sink, cancellation).boxed()
    }
}

async fn await_websocket<T, E, F>(
    cancellation: &CancellationToken,
    deadline: std::time::Duration,
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
    deadline: std::time::Duration,
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
    deadline: std::time::Duration,
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
    cancellation: &CancellationToken,
    deadline: std::time::Duration,
) -> Result<Message, SourceError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let next = tokio::select! {
        biased;
        () = cancellation.cancelled() => return Err(SourceError::Cancelled),
        result = tokio::time::timeout(deadline, socket.next()) => result,
    };
    match next {
        Ok(Some(Ok(message))) => Ok(message),
        Ok(Some(Err(_))) => Err(SourceError::Network),
        Ok(None) => Err(SourceError::ProviderUnavailable),
        Err(_) => Err(SourceError::Network),
    }
}

fn ensure_frame_bound(actual: usize, maximum: usize) -> Result<(), SourceError> {
    if actual > maximum {
        Err(SourceError::FrameTooLarge { max: maximum })
    } else {
        Ok(())
    }
}

fn map_connect_error(error: WebSocketError, budget: &SharedProviderBudget) -> SourceError {
    if let WebSocketError::Http(response) = &error {
        let status = response.status();
        if matches!(status.as_u16(), 401 | 403) {
            return SourceError::Unauthorized;
        }
        if status.as_u16() == 429 || status.is_server_error() {
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

#[cfg(test)]
#[path = "source/tests.rs"]
mod tests;
