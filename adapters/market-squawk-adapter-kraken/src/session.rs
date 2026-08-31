//! Bounded one-generation WebSocket session.

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use market_squawk_domain::{LiveEventClass, SourceIdentifier, Timestamp};
use market_squawk_sources::{
    ActiveLiveSourceGeneration, BudgetDispatchDecision, BudgetPermit, BudgetReservationDecision,
    DecodeError, DecodeInternalError, FrameId, FrameSessionBinding, LiveMarketSource,
    LiveSourceGeneration, RawMarketSink, SharedProviderBudget, SourceError, SourceMetadata,
    SourceMetadataProvider, TransportFrameKind, ValidatedRawMarketFrame, apply_http_retry_after,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::connect_async_tls_with_config;
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};
use tokio_util::sync::CancellationToken;

use crate::subscription::{KrakenPendingSubscriptionWrite, KrakenPublicSubscriptionRequest};
use crate::{
    KrakenChannel, KrakenConfig, KrakenControlOrDiscontinuityKind, KrakenDecoderState,
    KrakenL3Control, KrakenMarketDecodeHandoff, KrakenMarketDecoder, KrakenMarketEventHandoff,
    KrakenPublicControl, KrakenSubscriptionRequestEvidence,
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
    fn initial() -> Self {
        Self {
            state: KrakenDecoderState::AwaitingSnapshot,
            captured_frames: 0,
            market_messages: 0,
            control_messages: 0,
            last_market_timestamp: None,
            book_subscribed: false,
            trade_subscribed: false,
        }
    }

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

#[derive(Debug)]
struct KrakenSocketDecodeState {
    decoder: KrakenMarketDecoder,
    channel: KrakenChannel,
    health: KrakenHealth,
    budget: SharedProviderBudget,
    subscription_permit: Option<BudgetPermit>,
    written_subscription: Option<KrakenWrittenSubscription>,
    consumed_frame: Option<FrameId>,
    terminal: Option<SourceError>,
}

/// Source-side control for the sole generation-owned public socket decoder.
#[derive(Debug)]
struct KrakenSocketDecodeControl {
    state: Arc<Mutex<KrakenSocketDecodeState>>,
}

/// Sole sink-side consumer of the exact generation-owned Kraken public decoder.
///
/// Production capture invokes this only after the raw frame has been admitted and physically
/// captured. The consumer then mutates the one stateful decoder, settles subscription authority,
/// and returns a one-use live/publication handoff for that exact validated frame.
#[derive(Debug)]
pub struct KrakenSocketHandoffConsumer {
    metadata: SourceMetadata,
    state: Arc<Mutex<KrakenSocketDecodeState>>,
}

impl KrakenSocketHandoffConsumer {
    fn channel(
        config: &KrakenConfig,
        budget: SharedProviderBudget,
    ) -> Result<(KrakenSocketDecodeControl, Self), SourceError> {
        let decoder = match config.channel() {
            KrakenChannel::Book(depth) => KrakenMarketDecoder::try_new(
                config.metadata().clone(),
                config.native_coordinates().clone(),
                depth,
            ),
            KrakenChannel::Trades => KrakenMarketDecoder::try_trades(
                config.metadata().clone(),
                config.native_coordinates().clone(),
            ),
        }
        .map_err(|_| SourceError::InvalidProtocolState)?;
        let state = Arc::new(Mutex::new(KrakenSocketDecodeState {
            decoder,
            channel: config.channel(),
            health: KrakenHealth::initial(),
            budget,
            subscription_permit: None,
            written_subscription: None,
            consumed_frame: None,
            terminal: None,
        }));
        Ok((
            KrakenSocketDecodeControl {
                state: Arc::clone(&state),
            },
            Self {
                metadata: config.metadata().clone(),
                state,
            },
        ))
    }

    /// Decodes one exact validated frame after capture admission.
    pub fn consume(
        &mut self,
        frame: &ValidatedRawMarketFrame<'_>,
    ) -> Result<KrakenMarketDecodeHandoff, DecodeInternalError> {
        if frame.frame().source_id() != self.metadata.source_id()
            || frame.frame().metadata_revision() != self.metadata.revision()
        {
            return Err(DecodeInternalError::InvariantViolation);
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| DecodeInternalError::InvariantViolation)?;
        if state.consumed_frame.is_some() || state.terminal.is_some() {
            return Err(DecodeInternalError::InvariantViolation);
        }
        if let Some(written) = state.written_subscription.take() {
            let sent = written
                .bind_to_frame(frame)
                .map_err(|_| DecodeInternalError::InvariantViolation)?;
            state
                .decoder
                .register_sent_subscription(sent)
                .map_err(|error| match error {
                    DecodeError::RetainedSizeOverflow => DecodeInternalError::RetainedSizeOverflow,
                    _ => DecodeInternalError::InvariantViolation,
                })?;
        }
        let handoff = state.decoder.decode_captured(frame)?;
        handoff
            .evidence()
            .currentness_lease()
            .validate_current()
            .map_err(|_| DecodeInternalError::InvariantViolation)?;
        let decoder_state = state.decoder.state();
        let operational = state.apply_handoff(&handoff, decoder_state);
        state.health.captured_frames = state
            .health
            .captured_frames
            .checked_add(1)
            .ok_or(DecodeInternalError::InvariantViolation)?;
        state.consumed_frame = Some(frame.frame().frame_id());
        if let Err(error) = operational {
            state.terminal = Some(error);
        }
        KrakenMarketDecodeHandoff::try_from_socket_handoff(handoff)
    }
}

impl SourceMetadataProvider for KrakenSocketHandoffConsumer {
    fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }
}

impl KrakenSocketDecodeControl {
    fn install_subscription(
        &self,
        permit: BudgetPermit,
        written: KrakenWrittenSubscription,
    ) -> Result<(), SourceError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| SourceError::InvalidProtocolState)?;
        if state.subscription_permit.is_some()
            || state.written_subscription.is_some()
            || state.terminal.is_some()
        {
            return Err(SourceError::InvalidProtocolState);
        }
        state.subscription_permit = Some(permit);
        state.written_subscription = Some(written);
        Ok(())
    }

    fn reset_generation_health(&self) -> Result<(), SourceError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| SourceError::InvalidProtocolState)?;
        if state.subscription_permit.is_some()
            || state.written_subscription.is_some()
            || state.consumed_frame.is_some()
            || state.terminal.is_some()
        {
            return Err(SourceError::InvalidProtocolState);
        }
        state.health = KrakenHealth::initial();
        Ok(())
    }

    fn release_subscription_after_sink_rejection(&self) -> Result<(), SourceError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| SourceError::InvalidProtocolState)?;
        if state.consumed_frame.is_some() || state.terminal.is_some() {
            return Err(SourceError::InvalidProtocolState);
        }
        let permit = state
            .subscription_permit
            .take()
            .ok_or(SourceError::InvalidProtocolState)?;
        state
            .written_subscription
            .take()
            .ok_or(SourceError::InvalidProtocolState)?;
        permit.release();
        Ok(())
    }

    fn mark_quarantined(&self) -> Result<(), SourceError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| SourceError::InvalidProtocolState)?;
        state.health.state = KrakenDecoderState::Quarantined;
        Ok(())
    }

    fn mark_quarantined_unless_retired(&self) -> Result<(), SourceError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| SourceError::InvalidProtocolState)?;
        if state.health.state != KrakenDecoderState::Retired {
            state.health.state = KrakenDecoderState::Quarantined;
        }
        Ok(())
    }

    fn finish_frame(&self, frame_id: FrameId) -> Result<Option<SourceError>, SourceError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| SourceError::InvalidProtocolState)?;
        if state.consumed_frame != Some(frame_id) {
            return Err(SourceError::InvalidProtocolState);
        }
        state.consumed_frame = None;
        Ok(state.terminal.take())
    }

    fn health(&self) -> KrakenHealth {
        match self.state.lock() {
            Ok(state) => state.health,
            Err(poisoned) => poisoned.into_inner().health,
        }
    }
}

impl KrakenSocketDecodeState {
    fn apply_handoff(
        &mut self,
        handoff: &KrakenMarketEventHandoff,
        decoder_state: KrakenDecoderState,
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
                    self.apply_public_control(control)?;
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

    fn apply_public_control(&mut self, control: &KrakenPublicControl) -> Result<(), SourceError> {
        match control {
            KrakenPublicControl::Subscribed {
                channel: KrakenChannel::Book(_),
                ..
            } => {
                if self.health.book_subscribed || !matches!(self.channel, KrakenChannel::Book(_)) {
                    self.health.state = KrakenDecoderState::Quarantined;
                    return Err(SourceError::InvalidProtocolState);
                }
                settle_subscription_success(&self.budget, &mut self.subscription_permit)?;
                self.health.book_subscribed = true;
            }
            KrakenPublicControl::Subscribed {
                channel: KrakenChannel::Trades,
                ..
            } => {
                if self.health.trade_subscribed || self.channel != KrakenChannel::Trades {
                    self.health.state = KrakenDecoderState::Quarantined;
                    return Err(SourceError::InvalidProtocolState);
                }
                settle_subscription_success(&self.budget, &mut self.subscription_permit)?;
                self.health.trade_subscribed = true;
            }
            KrakenPublicControl::SubscriptionRefused { .. } => {
                let permit = self
                    .subscription_permit
                    .take()
                    .ok_or(SourceError::InvalidProtocolState)?;
                permit.release();
                let refusal =
                    SourceError::from_applied_budget_refusal(self.budget.apply_refusal(0));
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

/// Production Kraken Spot WebSocket v2 source.
#[derive(Debug)]
pub struct KrakenSource {
    config: KrakenConfig,
    authority: ActiveLiveSourceGeneration,
    budget: SharedProviderBudget,
    decode_control: KrakenSocketDecodeControl,
    generation_started: bool,
}

impl KrakenSource {
    /// Constructs one production source and the sole consumer of its socket-owned decode results.
    pub fn try_new_with_publication_handoff(
        config: KrakenConfig,
        generation: LiveSourceGeneration,
    ) -> Result<(Self, KrakenSocketHandoffConsumer), SourceError> {
        let authority = generation.try_start(config.metadata())?;
        let budget = authority
            .budget()?
            .cloned()
            .ok_or(SourceError::GenerationAuthorityMismatch)?;
        let (decode_control, consumer) =
            KrakenSocketHandoffConsumer::channel(&config, budget.clone())?;
        Ok((
            Self {
                config,
                authority,
                budget,
                decode_control,
                generation_started: false,
            },
            consumer,
        ))
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
    pub fn health(&self) -> KrakenHealth {
        self.decode_control.health()
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
        self.decode_control.reset_generation_health()?;
        let socket_config = WebSocketConfig::default()
            .read_buffer_size(READ_BUFFER_BYTES)
            .write_buffer_size(WRITE_BUFFER_BYTES)
            .max_write_buffer_size(MAX_WRITE_BUFFER_BYTES)
            .max_message_size(Some(self.config.max_message_bytes()))
            .max_frame_size(Some(self.config.max_message_bytes()));
        let permit = commit_budget_when_ready(&self.budget, &cancellation).await?;
        let connect = connect_async_tls_with_config(
            self.config.endpoint().as_str(),
            Some(socket_config),
            true,
            None,
        );
        let establishment = tokio::select! {
            _ = cancellation.cancelled() => Err(SourceError::Cancelled),
            result = tokio::time::timeout(CONNECT_TIMEOUT, connect) => {
                match result {
                    Ok(Ok(established)) => Ok(established),
                    Ok(Err(error)) => Err(map_connect_error(error, &self.budget)),
                    Err(_elapsed) => Err(SourceError::Network),
                }
            }
        };
        let (mut socket, response) = match establishment {
            Ok(established) => established,
            Err(error) => {
                permit.release();
                return Err(error);
            }
        };
        if response.status().is_redirection() {
            permit.release();
            self.decode_control.mark_quarantined()?;
            return Err(SourceError::InvalidProtocolState);
        }
        let establishment_settlement = self.budget.record_success();
        permit.release();
        if let Err(reason) = establishment_settlement {
            self.decode_control.mark_quarantined()?;
            return Err(SourceError::BudgetUnavailable { reason });
        }
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
        if result.is_err() && !matches!(result, Err(SourceError::Sink(_))) {
            self.decode_control.mark_quarantined_unless_retired()?;
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
        self.decode_control
            .install_subscription(subscription_permit, written_subscription)?;
        loop {
            let deadline = ReceiveDeadline::strictest(sink, deadlines.receive_idle)?;
            let message = tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(SourceError::Cancelled),
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline.at)) => {
                    self.decode_control.mark_quarantined()?;
                    if deadline.sink_owned {
                        sink.poll_deadline(Instant::now())?;
                        return Err(SourceError::InvalidProtocolState);
                    }
                    return Err(SourceError::ConnectionIdle);
                },
                message = socket.next() => message,
            };
            let Some(message) = message else {
                self.decode_control.mark_quarantined()?;
                return Err(SourceError::Network);
            };
            let message = match message {
                Ok(message) => message,
                Err(error) => {
                    self.decode_control.mark_quarantined()?;
                    return Err(map_websocket_error(error));
                }
            };
            match message {
                Message::Text(text) => {
                    let payload = Bytes::copy_from_slice(text.as_bytes());
                    self.capture_admitted(sink, TransportFrameKind::Text, payload)?;
                }
                Message::Binary(binary) => {
                    let payload = Bytes::copy_from_slice(binary.as_ref());
                    self.capture_admitted(sink, TransportFrameKind::Binary, payload)?;
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
                        self.decode_control.mark_quarantined()?;
                        return Err(error);
                    }
                }
                Message::Pong(_) => {}
                Message::Close(_) => {
                    self.decode_control.mark_quarantined()?;
                    return Err(SourceError::Network);
                }
                Message::Frame(_) => {
                    self.decode_control.mark_quarantined()?;
                    return Err(SourceError::InvalidProtocolState);
                }
            }
        }
    }

    fn capture_admitted(
        &mut self,
        sink: &mut dyn RawMarketSink,
        transport: TransportFrameKind,
        payload: Bytes,
    ) -> Result<(), SourceError> {
        if payload.len() > self.config.max_message_bytes() {
            self.decode_control.mark_quarantined()?;
            return Err(SourceError::FrameTooLarge {
                max: self.config.max_message_bytes(),
            });
        }
        let frame = match self.authority.frames_mut()?.try_frame(transport, payload) {
            Ok(frame) => frame,
            Err(error) => {
                self.decode_control.mark_quarantined()?;
                return Err(error);
            }
        };
        let frame_id = frame.frame_id();
        if let Err(error) = sink.try_publish(frame) {
            self.decode_control
                .release_subscription_after_sink_rejection()?;
            return Err(SourceError::Sink(error));
        }
        if let Some(error) = self.decode_control.finish_frame(frame_id)? {
            return Err(error);
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
pub struct KrakenWrittenSubscription {
    binding: FrameSessionBinding,
    request: KrakenSubscriptionRequestEvidence,
}

impl KrakenWrittenSubscription {
    /// Seals this one-use successful-write value to the first validated inbound frame from the
    /// exact registry-issued connection allocation.
    ///
    /// # Errors
    ///
    /// Rejects every value-equal but separately allocated source session, including a reconstructed
    /// source/revision/generation tuple.
    pub fn bind_to_frame(
        self,
        frame: &ValidatedRawMarketFrame<'_>,
    ) -> Result<KrakenSentSubscriptionReceipt, KrakenSubscriptionReceiptError> {
        let binding = frame.frame().binding();
        if !self.binding.shares_allocation_with(binding) {
            return Err(KrakenSubscriptionReceiptError::GenerationMismatch);
        }
        Ok(KrakenSentSubscriptionReceipt {
            binding: self.binding,
            request: self.request,
        })
    }
}

/// Authenticated L3 sender bound to one actual WebSocket and exact active source generation.
///
/// Construction validates the registry authority. Sending consumes the opaque secret-bearing
/// payload and returns a one-use successful-write value only after the exact socket accepts the
/// message and the source generation remains current. No generic sink can mint this evidence.
pub struct KrakenL3EstablishedSessionSender<'a, S> {
    authority: &'a mut ActiveLiveSourceGeneration,
    socket: &'a mut WebSocketStream<S>,
    budget: SharedProviderBudget,
}

impl<S> std::fmt::Debug for KrakenL3EstablishedSessionSender<'_, S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KrakenL3EstablishedSessionSender")
            .field("connection_generation", &self.authority.generation())
            .field("socket", &"[ESTABLISHED WEBSOCKET]")
            .finish()
    }
}

impl<'a, S> KrakenL3EstablishedSessionSender<'a, S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Binds the authenticated sender to one actual WebSocket and registry-issued generation.
    ///
    /// # Errors
    ///
    /// Fails when the active source generation is no longer current.
    pub fn try_new(
        authority: &'a mut ActiveLiveSourceGeneration,
        socket: &'a mut WebSocketStream<S>,
        budget: &SharedProviderBudget,
    ) -> Result<Self, SourceError> {
        authority.validate_current()?;
        let issued = authority
            .budget()?
            .ok_or(SourceError::GenerationAuthorityMismatch)?;
        if !budget.shares_allocation_with(issued) {
            return Err(SourceError::GenerationAuthorityMismatch);
        }
        Ok(Self {
            authority,
            socket,
            budget: budget.clone(),
        })
    }

    /// Sends one exact authenticated L3 subscription through this established session.
    ///
    /// The returned value is not yet a sent receipt: it must be consumed by
    /// [`KrakenWrittenSubscription::bind_to_frame`] using the first captured and validated
    /// provider response from the same source generation.
    ///
    /// # Errors
    ///
    /// Fails closed on payload invariants, cancellation, deadline, socket failure, or stale
    /// generation authority without minting successful-write evidence.
    pub async fn send_subscription(
        &mut self,
        payload: crate::KrakenL3SecretPayload,
        cancellation: &CancellationToken,
        deadline: Duration,
    ) -> Result<KrakenL3SubscriptionDispatch, SourceError> {
        let pending = payload
            .into_pending_write(self.authority.generation())
            .map_err(|_error| SourceError::InvalidProtocolState)?;
        let permit = commit_budget_when_ready(&self.budget, cancellation).await?;
        let written = KrakenEstablishedSubscriptionSender::try_new(self.authority, self.socket)?
            .send(pending, cancellation, deadline)
            .await?;
        KrakenL3SubscriptionDispatch::try_new(self.budget.clone(), permit, written)
    }
}

/// One exact authenticated subscription dispatch awaiting same-socket binding and provider
/// settlement.
///
/// The request-window dispatch is already durably recorded by the shared provider authority. This
/// value retains its in-flight permit until every symbol in the exact batch is acknowledged, a
/// refusal is applied, or the connection generation is dropped.
#[derive(Debug)]
pub struct KrakenL3SubscriptionDispatch {
    written: Option<KrakenWrittenSubscription>,
    budget: SharedProviderBudget,
    permit: Option<BudgetPermit>,
    request_id: Option<u64>,
    expected: Vec<(SourceIdentifier, market_squawk_domain::InstrumentId, bool)>,
}

impl KrakenL3SubscriptionDispatch {
    fn try_new(
        budget: SharedProviderBudget,
        permit: BudgetPermit,
        written: KrakenWrittenSubscription,
    ) -> Result<Self, SourceError> {
        let KrakenSubscriptionRequestEvidence::AuthenticatedSecretBearing { request_evidence } =
            &written.request
        else {
            return Err(SourceError::InvalidProtocolState);
        };
        let mut expected = Vec::new();
        expected
            .try_reserve_exact(request_evidence.instrument_bindings().len())
            .map_err(|_| SourceError::InvalidProtocolState)?;
        for binding in request_evidence.instrument_bindings() {
            expected.push((
                binding.native_symbol().clone(),
                binding.externally_resolved_instrument(),
                false,
            ));
        }
        if expected.is_empty() {
            return Err(SourceError::InvalidProtocolState);
        }
        let request_id = request_evidence.request_id();
        Ok(Self {
            written: Some(written),
            budget,
            permit: Some(permit),
            request_id,
            expected,
        })
    }

    /// Seals the successful write to the first captured frame from its exact socket allocation.
    ///
    /// Returns `None` after the one-use receipt has already been consumed by the decoder.
    pub fn bind_to_frame(
        &mut self,
        frame: &ValidatedRawMarketFrame<'_>,
    ) -> Result<Option<KrakenSentSubscriptionReceipt>, KrakenSubscriptionReceiptError> {
        self.written
            .take()
            .map(|written| written.bind_to_frame(frame))
            .transpose()
    }

    /// Applies one exact authenticated control to this durably admitted request.
    ///
    /// Returns `true` only when this call settles the complete batch successfully. Controls for a
    /// different request are ignored. A refusal consumes this request's in-flight permit and
    /// applies the shared provider cooldown before returning an error.
    pub fn apply_control(&mut self, control: &KrakenL3Control) -> Result<bool, SourceError> {
        match control {
            KrakenL3Control::Subscribed {
                symbol,
                instrument,
                request_id,
                ..
            } if request_id == &self.request_id => {
                let (_, _, acknowledged) = self
                    .expected
                    .iter_mut()
                    .find(|(expected_symbol, expected_instrument, _)| {
                        expected_symbol == symbol && expected_instrument == instrument
                    })
                    .ok_or(SourceError::InvalidProtocolState)?;
                if *acknowledged {
                    return Err(SourceError::InvalidProtocolState);
                }
                *acknowledged = true;
                if self.expected.iter().all(|(_, _, value)| *value) {
                    self.settle_success()?;
                    return Ok(true);
                }
                Ok(false)
            }
            KrakenL3Control::SubscriptionRefused { request_id, .. }
                if request_id.is_none() || request_id == &self.request_id =>
            {
                let permit = self
                    .permit
                    .take()
                    .ok_or(SourceError::InvalidProtocolState)?;
                permit.release();
                let refusal = SourceError::from_applied_budget_refusal(
                    self.budget
                        .apply_refusal(BACKOFF_JITTER_SAMPLE_BASIS_POINTS),
                );
                Err(refusal)
            }
            _ => Ok(false),
        }
    }

    fn settle_success(&mut self) -> Result<(), SourceError> {
        let permit = self
            .permit
            .take()
            .ok_or(SourceError::InvalidProtocolState)?;
        permit.release();
        let result = self
            .budget
            .record_success()
            .map_err(|reason| SourceError::BudgetUnavailable { reason });
        result
    }

    /// Returns whether every exact symbol acknowledgement settled this dispatch.
    pub fn is_settled(&self) -> bool {
        self.permit.is_none() && self.expected.iter().all(|(_, _, value)| *value)
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
    let permit = commit_budget_when_ready(budget, cancellation).await?;
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
        let binding = self.authority.frame_binding()?;
        if binding.source_id() != &source_id
            || binding.metadata_revision() != &revision
            || binding.connection_generation() != generation
        {
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
        if !binding.shares_allocation_with(&self.authority.frame_binding()?) {
            return Err(SourceError::GenerationAuthorityMismatch);
        }
        Ok(KrakenWrittenSubscription { binding, request })
    }
}

const BACKOFF_JITTER_SAMPLE_BASIS_POINTS: u16 = 1_000;

async fn commit_budget_when_ready(
    budget: &SharedProviderBudget,
    cancellation: &CancellationToken,
) -> Result<BudgetPermit, SourceError> {
    loop {
        let reservation = match budget.try_reserve_request() {
            BudgetReservationDecision::Ready(reservation) => reservation,
            BudgetReservationDecision::WaitUntil(deadline) => {
                let wait = budget
                    .remaining_wait(deadline)
                    .map_err(|reason| SourceError::BudgetUnavailable { reason })?;
                tokio::select! {
                    _ = cancellation.cancelled() => return Err(SourceError::Cancelled),
                    () = tokio::time::sleep(wait) => {}
                }
                continue;
            }
            BudgetReservationDecision::Unavailable(reason) => {
                return Err(SourceError::BudgetUnavailable { reason });
            }
        };
        match reservation.commit_dispatch() {
            BudgetDispatchDecision::Ready(permit) => return Ok(permit),
            BudgetDispatchDecision::WaitUntil(deadline) => {
                let wait = budget
                    .remaining_wait(deadline)
                    .map_err(|reason| SourceError::BudgetUnavailable { reason })?;
                tokio::select! {
                    _ = cancellation.cancelled() => return Err(SourceError::Cancelled),
                    () = tokio::time::sleep(wait) => {}
                }
            }
            BudgetDispatchDecision::Unavailable(reason) => {
                return Err(SourceError::BudgetUnavailable { reason });
            }
        }
    }
}

fn settle_subscription_success(
    budget: &SharedProviderBudget,
    subscription_permit: &mut Option<BudgetPermit>,
) -> Result<(), SourceError> {
    let permit = subscription_permit
        .take()
        .ok_or(SourceError::InvalidProtocolState)?;
    permit.release();
    budget
        .record_success()
        .map_err(|reason| SourceError::BudgetUnavailable { reason })
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
