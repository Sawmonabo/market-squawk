//! One-generation Coinbase transport implementation.

use std::{future::Future, time::Instant};

use bytes::Bytes;
use futures_util::{FutureExt, SinkExt, StreamExt, future::BoxFuture};
use market_squawk_sources::{
    ActiveLiveSourceGeneration, BudgetDispatchDecision, BudgetPermit, BudgetReservation,
    BudgetReservationDecision, LiveMarketSource, LiveSourceGeneration, RawMarketSink,
    SharedProviderBudget, SourceError, SourceMetadata, SourceMetadataProvider, TransportFrameKind,
    apply_http_retry_after,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::tungstenite::{
    Error as WebSocketError, Message, error::CapacityError, protocol::WebSocketConfig,
};
use tokio_tungstenite::{WebSocketStream, connect_async_with_config};
use tokio_util::sync::CancellationToken;

use crate::CoinbaseExchangeConfig;

/// Production Coinbase Advanced Trade public-market-data one-generation source.
#[derive(Debug)]
pub struct CoinbaseExchangeSource {
    config: CoinbaseExchangeConfig,
    authority: ActiveLiveSourceGeneration,
    budget: SharedProviderBudget,
    generation_started: bool,
}

impl CoinbaseExchangeSource {
    /// Consumes the exact registry-minted live-generation authority for this configuration.
    ///
    /// # Errors
    ///
    /// Rejects stale, capture-unhealthy, mismatched, or incomplete generation authority before any
    /// provider-budget or network operation can occur.
    pub fn try_new(
        config: CoinbaseExchangeConfig,
        generation: LiveSourceGeneration,
    ) -> Result<Self, SourceError> {
        let authority = generation.try_start(config.metadata())?;
        let budget = authority
            .budget()?
            .cloned()
            .ok_or(SourceError::GenerationAuthorityMismatch)?;
        Ok(Self {
            config,
            authority,
            budget,
            generation_started: false,
        })
    }

    fn validate_generation(&self) -> Result<(), SourceError> {
        let issued = self
            .authority
            .budget()?
            .ok_or(SourceError::GenerationAuthorityMismatch)?;
        if !self.budget.shares_allocation_with(issued) {
            return Err(SourceError::GenerationAuthorityMismatch);
        }
        Ok(())
    }

    fn begin_generation(&mut self) -> Result<(), SourceError> {
        if self.generation_started {
            return Err(SourceError::InvalidProtocolState);
        }
        self.generation_started = true;
        Ok(())
    }

    fn reserve_budget(&self) -> Result<BudgetReservation, SourceError> {
        match self.budget.try_reserve_request() {
            BudgetReservationDecision::Ready(reservation) => Ok(reservation),
            BudgetReservationDecision::WaitUntil(deadline) => {
                Err(SourceError::BudgetWaitUntil { deadline })
            }
            BudgetReservationDecision::Unavailable(reason) => {
                Err(SourceError::BudgetUnavailable { reason })
            }
        }
    }

    fn commit_budget(reservation: BudgetReservation) -> Result<BudgetPermit, SourceError> {
        match reservation.commit_dispatch() {
            BudgetDispatchDecision::Ready(permit) => Ok(permit),
            BudgetDispatchDecision::WaitUntil(deadline) => {
                Err(SourceError::BudgetWaitUntil { deadline })
            }
            BudgetDispatchDecision::Unavailable(reason) => {
                Err(SourceError::BudgetUnavailable { reason })
            }
        }
    }

    async fn run_production(
        &mut self,
        sink: &mut dyn RawMarketSink,
        cancellation: CancellationToken,
    ) -> Result<(), SourceError> {
        self.begin_generation()?;
        if cancellation.is_cancelled() {
            return Err(SourceError::Cancelled);
        }
        self.validate_generation()?;
        self.config
            .metadata()
            .network_policy()
            .authorize(self.config.endpoint())
            .map_err(|_| SourceError::InvalidProtocolState)?;
        let reservation = self.reserve_budget()?;
        let limits = self.config.transport_limits();
        let websocket_config = WebSocketConfig::default()
            .read_buffer_size(limits.max_frame_bytes().clamp(4 * 1024, 128 * 1024))
            .write_buffer_size(16 * 1024)
            .max_write_buffer_size(32 * 1024)
            .max_message_size(Some(limits.max_frame_bytes()))
            .max_frame_size(Some(limits.max_frame_bytes()));
        let permit = Self::commit_budget(reservation)?;
        let connect =
            connect_async_with_config(self.config.endpoint(), Some(websocket_config), true);
        let (socket, _response) =
            await_websocket(&cancellation, limits.connect_timeout(), connect, |error| {
                map_connect_error(error, &self.budget)
            })
            .await?;
        self.run_socket(socket, permit, sink, cancellation).await
    }

    async fn run_socket<S>(
        &mut self,
        mut socket: WebSocketStream<S>,
        permit: BudgetPermit,
        sink: &mut dyn RawMarketSink,
        cancellation: CancellationToken,
    ) -> Result<(), SourceError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        self.validate_generation()?;
        sink.bind_active_request_budget(permit.active_lease())?;
        let limits = self.config.transport_limits();
        for subscription in self.config.subscriptions() {
            send_with_deadline(
                &mut socket,
                Message::Text(subscription.as_ref().into()),
                &cancellation,
                limits.io_timeout(),
            )
            .await?;
        }
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
                    let payload = text.as_bytes();
                    ensure_frame_bound(payload.len(), limits.max_frame_bytes())?;
                    let frame = self
                        .authority
                        .frames_mut()?
                        .try_frame(TransportFrameKind::Text, Bytes::copy_from_slice(payload))?;
                    sink.try_publish(frame)?;
                    record_first_provider_message(&self.budget, &mut provider_message_observed)?;
                }
                Message::Binary(payload) => {
                    ensure_frame_bound(payload.len(), limits.max_frame_bytes())?;
                    let frame = self
                        .authority
                        .frames_mut()?
                        .try_frame(TransportFrameKind::Binary, payload)?;
                    sink.try_publish(frame)?;
                    record_first_provider_message(&self.budget, &mut provider_message_observed)?;
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
        self.validate_generation()?;
        let permit = Self::commit_budget(self.reserve_budget()?)?;
        self.run_socket(socket, permit, sink, cancellation).await
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
        sink: &'a mut dyn RawMarketSink,
        cancellation: CancellationToken,
    ) -> BoxFuture<'a, Result<(), SourceError>> {
        self.run_production(sink, cancellation).boxed()
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
    await_websocket(cancellation, deadline, socket.send(message), |_error| {
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
    sink: &mut dyn RawMarketSink,
    cancellation: &CancellationToken,
    transport_timeout: std::time::Duration,
    maximum_frame_bytes: usize,
) -> Result<Message, SourceError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let deadline = ReceiveDeadline::strictest(sink, transport_timeout)?;
    let next = tokio::select! {
        biased;
        () = cancellation.cancelled() => return Err(SourceError::Cancelled),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline.at)) => {
            if deadline.sink_owned {
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
        Some(Err(_error)) => Err(SourceError::Network),
        None => Err(SourceError::ProviderUnavailable),
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

#[derive(Clone, Copy, Debug)]
struct ReceiveDeadline {
    at: Instant,
    sink_owned: bool,
}

impl ReceiveDeadline {
    fn strictest(
        sink: &dyn RawMarketSink,
        transport_timeout: std::time::Duration,
    ) -> Result<Self, SourceError> {
        let transport = Instant::now()
            .checked_add(transport_timeout)
            .ok_or(SourceError::InvalidProtocolState)?;
        match sink.next_deadline() {
            Some(sink_deadline) if sink_deadline <= transport => Ok(Self {
                at: sink_deadline,
                sink_owned: true,
            }),
            _ => Ok(Self {
                at: transport,
                sink_owned: false,
            }),
        }
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
