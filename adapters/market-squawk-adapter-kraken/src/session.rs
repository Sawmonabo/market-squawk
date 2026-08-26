//! Bounded one-generation WebSocket session.

use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use market_squawk_domain::{
    ConnectionGeneration, LiveEventClass, MetadataRevision, SourceId, Timestamp,
};
use market_squawk_sources::{
    ActiveLiveSourceGeneration, BudgetDispatchDecision, BudgetPermit, BudgetReservation,
    BudgetReservationDecision, FrameSessionBinding, LiveMarketSource, LiveSourceGeneration,
    RawMarketSink, SharedProviderBudget, SourceError, SourceMetadata, SourceMetadataProvider,
    TransportFrameKind, ValidatedRawMarketFrame, apply_http_retry_after,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::connect_async_tls_with_config;
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};
use tokio_util::sync::CancellationToken;

use crate::subscription::{KrakenPendingSubscriptionWrite, KrakenPublicSubscriptionRequest};
use crate::{
    KrakenChannel, KrakenConfig, KrakenControlOrDiscontinuityKind, KrakenDecoderState,
    KrakenMarketDecoder, KrakenMarketEventHandoff, KrakenPublicControl,
    KrakenSubscriptionRequestEvidence,
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
    authority: ActiveLiveSourceGeneration,
    budget: SharedProviderBudget,
    generation_started: bool,
    health: KrakenHealth,
}

impl KrakenSource {
    /// Consumes one exact registry-minted current-generation authority.
    ///
    /// # Errors
    ///
    /// Rejects stale, capture-unhealthy, mismatched, or incomplete generation authority before any
    /// provider-budget or network operation can occur.
    pub fn try_new(
        config: KrakenConfig,
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
            health: KrakenHealth {
                state: KrakenDecoderState::AwaitingSnapshot,
                captured_frames: 0,
                market_messages: 0,
                control_messages: 0,
                last_market_timestamp: None,
                book_subscribed: false,
                trade_subscribed: false,
            },
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

    /// Returns the current non-authoritative operational snapshot.
    pub const fn health(&self) -> KrakenHealth {
        self.health
    }

    fn begin_generation(&mut self) -> Result<(), SourceError> {
        if self.generation_started {
            return Err(SourceError::InvalidProtocolState);
        }
        self.generation_started = true;
        Ok(())
    }

    async fn run_generation(
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
            .authorize_endpoint()
            .map_err(|_| SourceError::InvalidProtocolState)?;
        self.health.state = KrakenDecoderState::AwaitingSnapshot;
        self.health.book_subscribed = false;
        self.health.trade_subscribed = false;
        self.health.last_market_timestamp = None;
        let reservation = reserve_budget(&self.budget)?;
        let socket_config = WebSocketConfig::default()
            .read_buffer_size(READ_BUFFER_BYTES)
            .write_buffer_size(WRITE_BUFFER_BYTES)
            .max_write_buffer_size(MAX_WRITE_BUFFER_BYTES)
            .max_message_size(Some(self.config.max_message_bytes()))
            .max_frame_size(Some(self.config.max_message_bytes()));
        let permit = commit_budget(reservation)?;
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
                result.map_err(|error| map_connect_error(error, &self.budget))?
            }
        };
        if response.status().is_redirection() {
            self.health.state = KrakenDecoderState::Quarantined;
            return Err(SourceError::InvalidProtocolState);
        }
        if let Err(reason) = self.budget.record_success() {
            self.health.state = KrakenDecoderState::Quarantined;
            return Err(SourceError::BudgetUnavailable { reason });
        }
        drop(permit);
        self.run_established(&mut socket, sink, cancellation).await
    }

    async fn run_established<S>(
        &mut self,
        socket: &mut WebSocketStream<S>,
        sink: &mut dyn RawMarketSink,
        cancellation: CancellationToken,
    ) -> Result<(), SourceError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        self.validate_generation()?;
        let deadlines = SessionDeadlines::from_metadata(self.config.metadata());
        let result = self
            .run_established_inner(socket, sink, &cancellation, deadlines)
            .await;
        if result.is_err() && self.health.state != KrakenDecoderState::Retired {
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
        sink: &mut dyn RawMarketSink,
        cancellation: &CancellationToken,
        deadlines: SessionDeadlines,
    ) -> Result<(), SourceError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        self.validate_generation()?;
        let request = self
            .config
            .try_subscription_request(self.authority.generation())
            .map_err(|_| SourceError::InvalidProtocolState)?;
        let (subscription_permit, written_subscription) = send_subscription(
            socket,
            &mut self.authority,
            &self.budget,
            request,
            cancellation,
            deadlines.write,
        )
        .await?;
        let mut subscription_permit = Some(subscription_permit);
        let mut written_subscription = Some(written_subscription);

        let mut decoder = match self.config.channel() {
            KrakenChannel::Book(depth) => KrakenMarketDecoder::try_new(
                self.config.metadata().clone(),
                self.config.symbol(),
                self.config.instrument(),
                depth,
            ),
            KrakenChannel::Trades => KrakenMarketDecoder::try_trades(
                self.config.metadata().clone(),
                self.config.symbol(),
                self.config.instrument(),
            ),
        }
        .map_err(|_| SourceError::InvalidProtocolState)?;
        loop {
            let deadline = ReceiveDeadline::strictest(sink, deadlines.receive_idle)?;
            let message = tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(SourceError::Cancelled),
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline.at)) => {
                    self.health.state = KrakenDecoderState::Quarantined;
                    if deadline.sink_owned {
                        sink.poll_deadline(Instant::now())?;
                        return Err(SourceError::InvalidProtocolState);
                    }
                    return Err(SourceError::ConnectionIdle);
                },
                message = socket.next() => message,
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
                        sink,
                        &mut decoder,
                        TransportFrameKind::Text,
                        payload,
                        &mut subscription_permit,
                        &mut written_subscription,
                    )?;
                }
                Message::Binary(binary) => {
                    let payload = Bytes::copy_from_slice(binary.as_ref());
                    self.capture_then_decode(
                        sink,
                        &mut decoder,
                        TransportFrameKind::Binary,
                        payload,
                        &mut subscription_permit,
                        &mut written_subscription,
                    )?;
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
        sink: &mut dyn RawMarketSink,
        decoder: &mut KrakenMarketDecoder,
        transport: TransportFrameKind,
        payload: Bytes,
        subscription_permit: &mut Option<BudgetPermit>,
        written_subscription: &mut Option<KrakenWrittenSubscription>,
    ) -> Result<(), SourceError> {
        if payload.len() > self.config.max_message_bytes() {
            self.health.state = KrakenDecoderState::Quarantined;
            return Err(SourceError::FrameTooLarge {
                max: self.config.max_message_bytes(),
            });
        }
        let frame = match self.authority.frames_mut()?.try_frame(transport, payload) {
            Ok(frame) => frame,
            Err(error) => {
                self.health.state = KrakenDecoderState::Quarantined;
                return Err(error);
            }
        };
        let validated = self.authority.validate_live_frame(&frame)?;
        let captured = decoder
            .prepare_captured(&validated)
            .map_err(|_| SourceError::InvalidProtocolState)?;
        let sent_subscription = written_subscription
            .take()
            .map(|written| {
                written
                    .bind_to_frame(&validated)
                    .map_err(|_| SourceError::GenerationAuthorityMismatch)
            })
            .transpose()?;
        drop(validated);
        if let Err(error) = sink.try_publish(frame) {
            return Err(SourceError::Sink(error));
        }
        self.health.captured_frames = self
            .health
            .captured_frames
            .checked_add(1)
            .ok_or(SourceError::InvalidProtocolState)?;
        if let Some(sent_subscription) = sent_subscription {
            if decoder
                .register_sent_subscription(sent_subscription)
                .is_err()
            {
                self.health.state = decoder.state();
                return Err(SourceError::InvalidProtocolState);
            }
        }
        let handoff = decoder
            .decode_admitted(captured)
            .map_err(|_| SourceError::InvalidProtocolState)?;
        self.apply_handoff(handoff, decoder.state(), subscription_permit)
    }

    fn apply_handoff(
        &mut self,
        handoff: KrakenMarketEventHandoff,
        decoder_state: KrakenDecoderState,
        subscription_permit: &mut Option<BudgetPermit>,
    ) -> Result<(), SourceError> {
        match handoff {
            KrakenMarketEventHandoff::Public(handoff) => {
                let observations = handoff.batch().observations();
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
            KrakenMarketEventHandoff::ControlOrDiscontinuity(handoff) => match handoff.kind() {
                KrakenControlOrDiscontinuityKind::PublicControl(control) => {
                    self.health.control_messages = self
                        .health
                        .control_messages
                        .checked_add(1)
                        .ok_or(SourceError::InvalidProtocolState)?;
                    self.apply_control(control, subscription_permit)?;
                }
                KrakenControlOrDiscontinuityKind::PublicGenerationRetired(reason) => {
                    if matches!(
                        reason,
                        crate::KrakenGenerationRetirement::DuplicateSubscriptionAcknowledgement
                    ) {
                        self.health.control_messages = self
                            .health
                            .control_messages
                            .checked_add(1)
                            .ok_or(SourceError::InvalidProtocolState)?;
                    }
                    self.health.state = KrakenDecoderState::Retired;
                    return Err(SourceError::InvalidProtocolState);
                }
                KrakenControlOrDiscontinuityKind::PublicIgnored(_)
                | KrakenControlOrDiscontinuityKind::PublicResynchronize(_)
                | KrakenControlOrDiscontinuityKind::PublicQuarantine(_) => {
                    self.health.state = decoder_state;
                    return Err(SourceError::InvalidProtocolState);
                }
                KrakenControlOrDiscontinuityKind::AuthenticatedControl(_)
                | KrakenControlOrDiscontinuityKind::AuthenticatedDiscontinuity(_) => {
                    self.health.state = KrakenDecoderState::Quarantined;
                    return Err(SourceError::InvalidProtocolState);
                }
            },
            KrakenMarketEventHandoff::AuthenticatedLevel3(_) => {
                self.health.state = KrakenDecoderState::Quarantined;
                return Err(SourceError::InvalidProtocolState);
            }
        }
        self.health.state = decoder_state;
        Ok(())
    }

    fn apply_control(
        &mut self,
        control: &KrakenPublicControl,
        subscription_permit: &mut Option<BudgetPermit>,
    ) -> Result<(), SourceError> {
        match control {
            KrakenPublicControl::Subscribed {
                channel: KrakenChannel::Book(_),
                ..
            } => {
                if self.health.book_subscribed
                    || !matches!(self.config.channel(), KrakenChannel::Book(_))
                {
                    self.health.state = KrakenDecoderState::Quarantined;
                    return Err(SourceError::InvalidProtocolState);
                }
                settle_subscription_success(&self.budget, subscription_permit)?;
                self.health.book_subscribed = true;
            }
            KrakenPublicControl::Subscribed {
                channel: KrakenChannel::Trades,
                ..
            } => {
                if self.health.trade_subscribed || self.config.channel() != KrakenChannel::Trades {
                    self.health.state = KrakenDecoderState::Quarantined;
                    return Err(SourceError::InvalidProtocolState);
                }
                settle_subscription_success(&self.budget, subscription_permit)?;
                self.health.trade_subscribed = true;
            }
            KrakenPublicControl::SubscriptionRefused { .. } => {
                let permit = subscription_permit
                    .take()
                    .ok_or(SourceError::InvalidProtocolState)?;
                let refusal =
                    SourceError::from_applied_budget_refusal(self.budget.apply_refusal(0));
                drop(permit);
                self.health.state = KrakenDecoderState::Retired;
                return Err(refusal);
            }
            KrakenPublicControl::ProviderReset { .. } => {
                self.health.state = KrakenDecoderState::Retired;
                return Err(SourceError::InvalidProtocolState);
            }
            KrakenPublicControl::Heartbeat
            | KrakenPublicControl::Pong { .. }
            | KrakenPublicControl::Online => {}
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
struct ReceiveDeadline {
    at: Instant,
    sink_owned: bool,
}

impl ReceiveDeadline {
    fn strictest(
        sink: &dyn RawMarketSink,
        transport_timeout: Duration,
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

impl SourceMetadataProvider for KrakenSource {
    fn metadata(&self) -> &SourceMetadata {
        self.config.metadata()
    }
}

impl LiveMarketSource for KrakenSource {
    fn run<'a>(
        &'a mut self,
        sink: &'a mut dyn RawMarketSink,
        cancellation: CancellationToken,
    ) -> futures_util::future::BoxFuture<'a, Result<(), SourceError>> {
        Box::pin(self.run_generation(sink, cancellation))
    }
}

/// Successful outbound write held only inside one established adapter session.
///
/// The first validated inbound frame consumes this value and seals it to the exact registry
/// allocation. No constructor or minting operation is exported from this module.
#[derive(Debug)]
struct KrakenWrittenSubscription {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    connection_generation: ConnectionGeneration,
    request: KrakenSubscriptionRequestEvidence,
}

impl KrakenWrittenSubscription {
    fn bind_to_frame(
        self,
        frame: &ValidatedRawMarketFrame<'_>,
    ) -> Result<KrakenSentSubscriptionReceipt, KrakenSubscriptionReceiptError> {
        let binding = frame.frame().binding();
        if binding.source_id() != &self.source_id
            || binding.metadata_revision() != &self.metadata_revision
            || binding.connection_generation() != self.connection_generation
        {
            return Err(KrakenSubscriptionReceiptError::GenerationMismatch);
        }
        Ok(KrakenSentSubscriptionReceipt {
            binding: binding.clone(),
            request: self.request,
        })
    }
}

/// Nonconstructable proof that the established session sent one exact request and captured the
/// provider response through the same registry-issued connection allocation.
#[derive(Debug)]
pub struct KrakenSentSubscriptionReceipt {
    binding: FrameSessionBinding,
    request: KrakenSubscriptionRequestEvidence,
}

impl KrakenSentSubscriptionReceipt {
    /// Returns the exact registry-issued inbound connection allocation.
    pub const fn binding(&self) -> &FrameSessionBinding {
        &self.binding
    }

    /// Returns the exact public payload or authenticated secret-free request contract.
    pub const fn request(&self) -> &KrakenSubscriptionRequestEvidence {
        &self.request
    }

    pub(crate) fn into_parts(self) -> (FrameSessionBinding, KrakenSubscriptionRequestEvidence) {
        (self.binding, self.request)
    }
}

/// Failure to bind a successful subscription write to exact inbound capture authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum KrakenSubscriptionReceiptError {
    /// The validated provider response belongs to a different source/revision/generation.
    #[error("Kraken subscription write and inbound frame generation differ")]
    GenerationMismatch,
}

async fn send_subscription<S>(
    socket: &mut WebSocketStream<S>,
    authority: &mut ActiveLiveSourceGeneration,
    budget: &market_squawk_sources::SharedProviderBudget,
    request: KrakenPublicSubscriptionRequest,
    cancellation: &CancellationToken,
    deadline: Duration,
) -> Result<(BudgetPermit, KrakenWrittenSubscription), SourceError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let reservation = reserve_budget(budget)?;
    let permit = commit_budget(reservation)?;
    let pending = request.into_pending_write();
    let written = KrakenEstablishedSubscriptionSender::try_new(authority, socket)?
        .send(pending, cancellation, deadline)
        .await?;
    Ok((permit, written))
}

/// Adapter-owned sender for one established WebSocket and exact registry-minted generation.
///
/// This type never escapes the session module. A generic sink, drain, or caller-created value
/// cannot mint successful-send evidence.
struct KrakenEstablishedSubscriptionSender<'a, S> {
    authority: &'a mut ActiveLiveSourceGeneration,
    socket: &'a mut WebSocketStream<S>,
}

impl<'a, S> KrakenEstablishedSubscriptionSender<'a, S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn try_new(
        authority: &'a mut ActiveLiveSourceGeneration,
        socket: &'a mut WebSocketStream<S>,
    ) -> Result<Self, SourceError> {
        authority.validate_current()?;
        Ok(Self { authority, socket })
    }

    async fn send(
        &mut self,
        pending: KrakenPendingSubscriptionWrite,
        cancellation: &CancellationToken,
        deadline: Duration,
    ) -> Result<KrakenWrittenSubscription, SourceError> {
        self.authority.validate_current()?;
        let (message, source_id, revision, generation, request) = pending.into_parts();
        if generation != self.authority.generation() {
            return Err(SourceError::GenerationAuthorityMismatch);
        }
        tokio::select! {
            _ = cancellation.cancelled() => return Err(SourceError::Cancelled),
            result = tokio::time::timeout(deadline, self.socket.send(message)) => {
                let result = result.map_err(|_| SourceError::Network)?;
                result.map_err(map_websocket_error)?;
            }
        }
        self.authority.validate_current()?;
        Ok(KrakenWrittenSubscription {
            source_id,
            metadata_revision: revision,
            connection_generation: generation,
            request,
        })
    }
}

fn settle_subscription_success(
    budget: &SharedProviderBudget,
    subscription_permit: &mut Option<BudgetPermit>,
) -> Result<(), SourceError> {
    let permit = subscription_permit
        .take()
        .ok_or(SourceError::InvalidProtocolState)?;
    budget
        .record_success()
        .map_err(|reason| SourceError::BudgetUnavailable { reason })?;
    drop(permit);
    Ok(())
}

fn reserve_budget(budget: &SharedProviderBudget) -> Result<BudgetReservation, SourceError> {
    match budget.try_reserve_request() {
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
        return SourceError::from_applied_budget_refusal(apply_http_retry_after(
            budget,
            response
                .headers()
                .get(tokio_tungstenite::tungstenite::http::header::RETRY_AFTER)
                .map(|value| value.as_bytes()),
            1_000,
        ));
    }
    map_websocket_error(error)
}

#[cfg(test)]
#[path = "session_tests.rs"]
mod tests;
