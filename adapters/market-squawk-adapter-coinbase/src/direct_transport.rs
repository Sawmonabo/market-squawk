//! Authenticated Coinbase Direct snapshot/bootstrap and live transport.
//!
//! This is intentionally not a [`market_squawk_sources::LiveMarketSource`]. Direct bootstrap owns
//! a REST product response, a segmented level-3 snapshot, a bounded replay queue, and one
//! instrument book across the atomic snapshot-to-live handoff. The generic raw-only source
//! contract cannot express that ownership without hiding synchronization state.

mod http;

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures_util::{SinkExt as _, StreamExt as _, future::BoxFuture};
use market_squawk_domain::{ConnectionGeneration, SequenceNumber, Timestamp};
use market_squawk_live::{DirectOrderBook, DirectOrderBookError, DirectPublishedBook};
use market_squawk_sources::{
    ActiveLiveSourceGeneration, BudgetDecision, BudgetPermit, HttpCaptureMethod,
    LiveSourceGeneration, NetworkAccessPolicy, RawMarketSink, SegmentedHttpCaptureError,
    SegmentedHttpResponseCapture, SegmentedHttpResponseReceipt, SharedProviderBudget, SinkError,
    SourceError, SourceMetadata, SourceMetadataProvider, TlsProviderCapability, TransportFrameKind,
    apply_http_retry_after,
};
use serde::Deserialize;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::tungstenite::{Error as WebSocketError, Message, protocol::WebSocketConfig};
use tokio_tungstenite::{WebSocketStream, connect_async_with_config};
use tokio_util::sync::CancellationToken;

use self::http::{
    CoinbaseDirectHttpRequest, CoinbaseDirectHttpResponse, CoinbaseDirectHttpTransport,
    CoinbaseDirectHttpTransportError, ReqwestCoinbaseDirectHttpTransport,
};
use crate::{
    CoinbaseConfigError, CoinbaseDirectConfig, CoinbaseDirectDecodeError,
    CoinbaseDirectDecodeOutcome, CoinbaseDirectDecoder, CoinbaseDirectNonBookEvent,
    CoinbaseDirectProductError, CoinbaseDirectProductEvidence, CoinbaseDirectSigningCapability,
    CoinbaseDirectSigningError, CoinbaseDirectSnapshotDecoder, CoinbaseDirectSnapshotError,
    CoinbaseSignedSubscription,
};

/// Borrowed, read-only healthy book evidence emitted by one Direct owner.
///
/// This update is unqualified provider evidence. It does not mint `DirectVerified`, canonical
/// market events, order authority, or execution eligibility.
#[derive(Clone, Copy, Debug)]
pub struct CoinbaseDirectBookUpdate<'a> {
    sequence: SequenceNumber,
    source_timestamp: Timestamp,
    snapshot_receipt: &'a SegmentedHttpResponseReceipt,
    book: DirectPublishedBook<'a>,
}

impl<'a> CoinbaseDirectBookUpdate<'a> {
    /// Returns the exact healthy public product cursor.
    pub const fn sequence(self) -> SequenceNumber {
        self.sequence
    }

    /// Returns the provider event time associated with the healthy cursor.
    pub const fn source_timestamp(self) -> Timestamp {
        self.source_timestamp
    }

    /// Returns the complete generation-bound level-3 snapshot receipt.
    pub const fn snapshot_receipt(self) -> &'a SegmentedHttpResponseReceipt {
        self.snapshot_receipt
    }

    /// Returns an allocation-free bounded-depth view of the session-owned book.
    pub const fn book(self) -> DirectPublishedBook<'a> {
        self.book
    }
}

/// Nonblocking application boundary for one Coinbase Direct generation.
///
/// Every `try_*` callback is synchronous. Implementations must not hide an unbounded queue or
/// await downstream work. Raw frames are accepted through [`RawMarketSink`] before any decoded
/// outcome mutates the session.
pub trait CoinbaseDirectOutput: RawMarketSink {
    /// Accepts current provider product/status/tick/lot evidence without qualifying it.
    fn try_publish_product(
        &mut self,
        evidence: CoinbaseDirectProductEvidence,
    ) -> Result<(), SinkError>;

    /// Accepts one private lifecycle event that carries no public cursor or book authority.
    fn try_publish_non_book(&mut self, event: CoinbaseDirectNonBookEvent) -> Result<(), SinkError>;

    /// Accepts one borrowed read-only view after healthy handoff or a contiguous live successor.
    fn try_publish_book(&mut self, update: CoinbaseDirectBookUpdate<'_>) -> Result<(), SinkError>;
}

/// Terminal construction, transport, synchronization, or output failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum CoinbaseDirectSessionError {
    /// Immutable Coinbase configuration could not produce the pinned runtime profile.
    #[error("Coinbase Direct session configuration is invalid: {0}")]
    Configuration(#[from] CoinbaseConfigError),
    /// Registry generation, provider budget, network, cancellation, or raw sink failure.
    #[error("Coinbase Direct source authority or transport failed: {0}")]
    Source(#[from] SourceError),
    /// Application-owned signing capability or signed subscription construction failed.
    #[error("Coinbase Direct signing failed: {0}")]
    Signing(#[from] CoinbaseDirectSigningError),
    /// A captured order lifecycle frame failed the pinned decoder.
    #[error("Coinbase Direct frame decode failed: {0}")]
    Decode(#[from] CoinbaseDirectDecodeError),
    /// A complete captured product response failed typed evidence decoding.
    #[error("Coinbase Direct product evidence failed: {0}")]
    Product(#[from] CoinbaseDirectProductError),
    /// A complete captured level-3 response failed snapshot decoding.
    #[error("Coinbase Direct snapshot failed: {0}")]
    Snapshot(#[from] CoinbaseDirectSnapshotError),
    /// Same-owner queue, replay, or live mutation failed.
    #[error("Coinbase Direct book synchronization failed: {0}")]
    Book(#[from] DirectOrderBookError),
    /// Generation-bound segmented HTTP capture failed.
    #[error("Coinbase Direct HTTP capture failed: {0}")]
    Capture(#[from] SegmentedHttpCaptureError),
    /// The exact signed `full` subscription acknowledgement was absent, malformed, or duplicated.
    #[error("Coinbase Direct subscription acknowledgement is invalid")]
    Subscription,
    /// A WebSocket payload or internal protocol frame was not accepted by the pinned profile.
    #[error("Coinbase Direct WebSocket protocol is invalid")]
    WebSocketProtocol,
    /// HTTP response metadata, headers, media type, or final URL was not admitted.
    #[error("Coinbase Direct HTTP response is invalid")]
    HttpResponse,
    /// HTTP acquisition exceeded its configured total deadline.
    #[error("Coinbase Direct HTTP response deadline elapsed")]
    HttpDeadline,
    /// HTTP response bytes exceeded the configured complete-body ceiling.
    #[error("Coinbase Direct HTTP response exceeded its byte ceiling")]
    HttpBodyTooLarge,
    /// HTTP response segmentation exceeded the configured count ceiling.
    #[error("Coinbase Direct HTTP response exceeded its segment ceiling")]
    HttpSegmentLimit,
    /// A cancellation close handshake failed or exceeded its bound.
    #[error("Coinbase Direct WebSocket shutdown failed")]
    Shutdown,
}

/// Production one-generation Coinbase Direct session.
#[derive(Debug)]
pub struct CoinbaseDirectSession {
    config: CoinbaseDirectConfig,
    authority: ActiveLiveSourceGeneration,
    budget: SharedProviderBudget,
    decoder: CoinbaseDirectDecoder,
    snapshot_decoder: CoinbaseDirectSnapshotDecoder,
    http: Arc<dyn CoinbaseDirectHttpTransport>,
    http_timeout: Duration,
    book: DirectOrderBook,
    snapshot_receipt: Option<SegmentedHttpResponseReceipt>,
    generation_started: bool,
}

impl CoinbaseDirectSession {
    /// Consumes one registry-minted generation and the project-installed TLS capability before
    /// creating the hardened production HTTP client.
    ///
    /// No credentials are read or retained. The signing capability is supplied only to
    /// [`Self::run`] at first use.
    pub fn try_new(
        config: CoinbaseDirectConfig,
        generation: LiveSourceGeneration,
        tls_provider: TlsProviderCapability,
    ) -> Result<Self, CoinbaseDirectSessionError> {
        let bounds = direct_http_bounds(&config)?;
        let http = Arc::new(
            ReqwestCoinbaseDirectHttpTransport::try_new(bounds, tls_provider)
                .map_err(|_error| CoinbaseDirectSessionError::HttpResponse)?,
        );
        Self::try_new_inner(config, generation, http)
    }

    #[cfg(test)]
    fn try_new_with_transport(
        config: CoinbaseDirectConfig,
        generation: LiveSourceGeneration,
        http: Arc<dyn CoinbaseDirectHttpTransport>,
    ) -> Result<Self, CoinbaseDirectSessionError> {
        Self::try_new_inner(config, generation, http)
    }

    fn try_new_inner(
        config: CoinbaseDirectConfig,
        generation: LiveSourceGeneration,
        http: Arc<dyn CoinbaseDirectHttpTransport>,
    ) -> Result<Self, CoinbaseDirectSessionError> {
        let authority = generation.try_start(config.metadata())?;
        let budget = authority
            .budget()?
            .cloned()
            .ok_or(SourceError::GenerationAuthorityMismatch)?;
        let decoder = CoinbaseDirectDecoder::try_new(&config)?;
        let snapshot_decoder = CoinbaseDirectSnapshotDecoder::try_new(&config)?;
        let book = DirectOrderBook::try_new(
            authority.generation(),
            config.product().clone(),
            config.execution_terms(),
            config.limits().book(),
        )?;
        let bounds = direct_http_bounds(&config)?;
        Ok(Self {
            config,
            authority,
            budget,
            decoder,
            snapshot_decoder,
            http,
            http_timeout: Duration::from_nanos(bounds.total_timeout_nanos()),
            book,
            snapshot_receipt: None,
            generation_started: false,
        })
    }

    /// Returns immutable source metadata. Its quality is a ceiling declaration, not current
    /// qualification minted by this session.
    pub const fn metadata(&self) -> &SourceMetadata {
        self.config.metadata()
    }

    /// Runs one production connection until cancellation or a typed fail-closed terminal defect.
    pub async fn run(
        &mut self,
        signer: &dyn CoinbaseDirectSigningCapability,
        output: &mut dyn CoinbaseDirectOutput,
        cancellation: CancellationToken,
    ) -> Result<(), CoinbaseDirectSessionError> {
        let outcome = self.run_production(signer, output, cancellation).await;
        self.finish_generation(outcome)
    }

    async fn run_production(
        &mut self,
        signer: &dyn CoinbaseDirectSigningCapability,
        output: &mut dyn CoinbaseDirectOutput,
        cancellation: CancellationToken,
    ) -> Result<(), CoinbaseDirectSessionError> {
        self.begin_generation()?;
        if cancellation.is_cancelled() {
            return Err(SourceError::Cancelled.into());
        }
        self.validate_generation()?;
        self.authorize_endpoint(self.config.websocket_endpoint())?;
        let permit = self.acquire_budget()?;
        let limits = self.config.limits().websocket();
        let websocket_config = WebSocketConfig::default()
            .read_buffer_size(limits.max_frame_bytes().clamp(4 * 1024, 128 * 1024))
            .write_buffer_size(16 * 1024)
            .max_write_buffer_size(32 * 1024)
            .max_message_size(Some(limits.max_frame_bytes()))
            .max_frame_size(Some(limits.max_frame_bytes()));
        let connect = connect_async_with_config(
            self.config.websocket_endpoint(),
            Some(websocket_config),
            true,
        );
        let (socket, _response) =
            await_websocket(&cancellation, limits.connect_timeout(), connect, |error| {
                map_connect_error(error, &self.budget)
            })
            .await?;
        self.run_connected(socket, permit, signer, None, output, cancellation)
            .await
    }

    #[cfg(test)]
    async fn run_with_socket_for_test<S>(
        &mut self,
        socket: WebSocketStream<S>,
        signer: &dyn CoinbaseDirectSigningCapability,
        output: &mut dyn CoinbaseDirectOutput,
        cancellation: CancellationToken,
        unix_seconds: u64,
    ) -> Result<(), CoinbaseDirectSessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let outcome = async {
            self.begin_generation()?;
            if cancellation.is_cancelled() {
                return Err(SourceError::Cancelled.into());
            }
            self.validate_generation()?;
            let permit = self.acquire_budget()?;
            self.run_connected(
                socket,
                permit,
                signer,
                Some(unix_seconds),
                output,
                cancellation,
            )
            .await
        }
        .await;
        self.finish_generation(outcome)
    }

    async fn run_connected<S>(
        &mut self,
        socket: WebSocketStream<S>,
        connection_guard: BudgetPermit,
        signer: &dyn CoinbaseDirectSigningCapability,
        fixed_unix_seconds: Option<u64>,
        output: &mut dyn CoinbaseDirectOutput,
        cancellation: CancellationToken,
    ) -> Result<(), CoinbaseDirectSessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let limits = self.config.limits().websocket();
        let (outcome, shutdown) = {
            let mut socket = socket;
            let outcome = async {
                let unix_seconds = match fixed_unix_seconds {
                    Some(unix_seconds) => unix_seconds,
                    None => current_unix_seconds()?,
                };
                if cancellation.is_cancelled() {
                    return Err(SourceError::Cancelled.into());
                }
                self.validate_generation()?;
                let subscription = self.config.try_signed_subscription(unix_seconds, signer)?;
                self.validate_generation()?;
                if cancellation.is_cancelled() {
                    return Err(SourceError::Cancelled.into());
                }
                self.run_connected_inner(&mut socket, subscription, output, &cancellation)
                    .await
            };
            let outcome = outcome.await;
            let shutdown = if matches!(
                outcome,
                Err(CoinbaseDirectSessionError::Source(SourceError::Cancelled))
            ) {
                self.shutdown_socket(&mut socket, output, limits.io_timeout())
                    .await
            } else {
                Ok(())
            };
            (outcome, shutdown)
        };
        // The socket has completed its bounded shutdown or terminal drop before this release.
        connection_guard.release();
        shutdown?;
        outcome
    }

    async fn run_connected_inner<S>(
        &mut self,
        socket: &mut WebSocketStream<S>,
        subscription: CoinbaseSignedSubscription,
        output: &mut dyn CoinbaseDirectOutput,
        cancellation: &CancellationToken,
    ) -> Result<(), CoinbaseDirectSessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let limits = self.config.limits().websocket();
        self.validate_generation()?;
        send_with_deadline(
            socket,
            Message::Text(subscription.as_str().into()),
            cancellation,
            limits.io_timeout(),
        )
        .await?;
        drop(subscription);
        self.await_subscription(socket, output, cancellation)
            .await?;
        self.budget
            .record_success()
            .map_err(|reason| SourceError::BudgetUnavailable { reason })?;

        self.bootstrap_and_run_live(socket, output, cancellation)
            .await
    }

    async fn await_subscription<S>(
        &mut self,
        socket: &mut WebSocketStream<S>,
        output: &mut dyn CoinbaseDirectOutput,
        cancellation: &CancellationToken,
    ) -> Result<(), CoinbaseDirectSessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let io_timeout = self.config.limits().websocket().io_timeout();
        let acknowledgement = async {
            loop {
                let message = read_with_deadline(socket, output, cancellation, io_timeout).await?;
                if self
                    .handle_message(
                        socket,
                        message,
                        output,
                        cancellation,
                        InboundMode::AwaitingAck,
                    )
                    .await?
                    == MessageDisposition::Acknowledged
                {
                    return Ok(());
                }
            }
        };
        match tokio::time::timeout(io_timeout, acknowledgement).await {
            Ok(outcome) => outcome,
            Err(_elapsed) => Err(SourceError::ConnectionIdle.into()),
        }
    }

    async fn bootstrap_and_run_live<S>(
        &mut self,
        socket: &mut WebSocketStream<S>,
        output: &mut dyn CoinbaseDirectOutput,
        cancellation: &CancellationToken,
    ) -> Result<(), CoinbaseDirectSessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let product_url = self.config.product_url().to_owned();
        let product_response = self
            .fetch_while_queueing(socket, output, cancellation, &product_url)
            .await?;
        let product_capture = self.finish_http_capture(&product_url, product_response)?;
        let product_evidence = self.config.decode_product_evidence(&product_capture)?;
        output
            .try_publish_product(product_evidence)
            .map_err(SourceError::Sink)?;

        let snapshot_url = self.config.snapshot_url().to_owned();
        let snapshot_response = self
            .fetch_while_queueing(socket, output, cancellation, &snapshot_url)
            .await?;
        let snapshot_capture = self.finish_http_capture(&snapshot_url, snapshot_response)?;
        let snapshot_receipt = snapshot_capture.receipt().clone();
        let frontier = handoff_frontier_payload(&snapshot_receipt, self.authority.generation());
        self.await_handoff_frontier(socket, output, cancellation, frontier)
            .await?;
        self.snapshot_decoder
            .decode_into(&snapshot_capture, &mut self.book)?;
        self.book.begin_replay()?;
        loop {
            if cancellation.is_cancelled() {
                return Err(SourceError::Cancelled.into());
            }
            if !self.book.replay_next()? {
                break;
            }
        }
        self.book.finish_replay()?;
        self.validate_generation()?;
        self.snapshot_receipt = Some(snapshot_receipt);
        self.publish_book(output)?;

        loop {
            let message = read_with_deadline(
                socket,
                output,
                cancellation,
                self.config.limits().websocket().io_timeout(),
            )
            .await?;
            let _disposition = self
                .handle_message(socket, message, output, cancellation, InboundMode::Live)
                .await?;
        }
    }

    async fn await_handoff_frontier<S>(
        &mut self,
        socket: &mut WebSocketStream<S>,
        output: &mut dyn CoinbaseDirectOutput,
        cancellation: &CancellationToken,
        frontier: Bytes,
    ) -> Result<(), CoinbaseDirectSessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let io_timeout = self.config.limits().websocket().io_timeout();
        let operation = async {
            self.validate_generation()?;
            send_with_deadline(
                socket,
                Message::Ping(frontier.clone()),
                cancellation,
                io_timeout,
            )
            .await?;
            loop {
                let message = read_with_deadline(socket, output, cancellation, io_timeout).await?;
                if matches!(&message, Message::Pong(payload) if payload == &frontier) {
                    self.validate_generation()?;
                    return Ok(());
                }
                let _disposition = self
                    .handle_message(socket, message, output, cancellation, InboundMode::Queueing)
                    .await?;
            }
        };
        match tokio::time::timeout(io_timeout, operation).await {
            Ok(outcome) => outcome,
            Err(_elapsed) => Err(SourceError::ConnectionIdle.into()),
        }
    }

    async fn fetch_while_queueing<S>(
        &mut self,
        socket: &mut WebSocketStream<S>,
        output: &mut dyn CoinbaseDirectOutput,
        cancellation: &CancellationToken,
        url: &str,
    ) -> Result<CoinbaseDirectHttpResponse, CoinbaseDirectSessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let mut response = self.start_http_request(url, cancellation.clone())?;
        loop {
            let deadline =
                ReceiveDeadline::strictest(output, self.config.limits().websocket().io_timeout())?;
            tokio::select! {
                biased;
                () = cancellation.cancelled() => {
                    return Err(SourceError::Cancelled.into());
                }
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline.at)) => {
                    if deadline.sink_owned {
                        output
                            .poll_deadline(Instant::now())
                            .map_err(SourceError::Sink)?;
                        return Err(SourceError::InvalidProtocolState.into());
                    }
                    return Err(SourceError::ConnectionIdle.into());
                }
                completed = &mut response => {
                    return completed.map_err(map_http_transport_error);
                }
                message = socket.next() => {
                    let message = map_next_message(message)?;
                    let _disposition = self
                        .handle_message(
                            socket,
                            message,
                            output,
                            cancellation,
                            InboundMode::Queueing,
                        )
                        .await?;
                }
            }
        }
    }

    fn start_http_request(
        &self,
        url: &str,
        cancellation: CancellationToken,
    ) -> Result<
        BoxFuture<'static, Result<CoinbaseDirectHttpResponse, CoinbaseDirectHttpTransportError>>,
        CoinbaseDirectSessionError,
    > {
        self.validate_generation()?;
        self.authorize_endpoint(url)?;
        let permit = self.acquire_budget()?;
        let limits = self.config.limits();
        let request = CoinbaseDirectHttpRequest::new(
            url,
            limits.max_snapshot_bytes(),
            limits.max_snapshot_segments(),
            self.http_timeout,
            cancellation,
        );
        let transport = Arc::clone(&self.http);
        Ok(Box::pin(async move {
            let outcome = transport.get(request).await;
            drop(permit);
            outcome
        }))
    }

    fn finish_http_capture(
        &mut self,
        expected_url: &str,
        response: CoinbaseDirectHttpResponse,
    ) -> Result<SegmentedHttpResponseCapture, CoinbaseDirectSessionError> {
        self.validate_generation()?;
        self.authorize_endpoint(response.final_url.as_ref())?;
        if response.final_url.as_ref() != expected_url {
            return Err(CoinbaseDirectSessionError::HttpResponse);
        }
        if matches!(response.status, 401 | 403) {
            return Err(SourceError::Unauthorized.into());
        }
        if response.status == 429 || (500..=599).contains(&response.status) {
            return Err(
                SourceError::from_applied_budget_refusal(apply_http_retry_after(
                    &self.budget,
                    response.retry_after.as_deref(),
                    1_000,
                ))
                .into(),
            );
        }
        if response.status != 200
            || !content_type_is_json(response.content_type.as_deref())
            || response
                .content_encoding
                .as_deref()
                .is_some_and(|value| !value.eq_ignore_ascii_case(b"identity"))
        {
            return Err(CoinbaseDirectSessionError::HttpResponse);
        }
        let limits = self.config.limits();
        let mut builder = self.authority.frames_mut()?.try_http_response_builder(
            HttpCaptureMethod::Get,
            response.final_url.as_ref(),
            response.status,
            response.declared_body_length,
            limits.max_snapshot_bytes(),
            limits.max_snapshot_segments(),
        )?;
        for segment in response.segments {
            builder.try_push_segment(segment)?;
        }
        let capture = builder.finish()?;
        self.validate_generation()?;
        self.budget
            .record_success()
            .map_err(|reason| SourceError::BudgetUnavailable { reason })?;
        Ok(capture)
    }

    async fn handle_message<S>(
        &mut self,
        socket: &mut WebSocketStream<S>,
        message: Message,
        output: &mut dyn CoinbaseDirectOutput,
        cancellation: &CancellationToken,
        mode: InboundMode,
    ) -> Result<MessageDisposition, CoinbaseDirectSessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        match message {
            Message::Text(text) => self.capture_decode_commit(
                TransportFrameKind::Text,
                Bytes::copy_from_slice(text.as_bytes()),
                output,
                mode,
            ),
            Message::Binary(payload) => {
                self.capture_rejected_protocol_frame(TransportFrameKind::Binary, payload, output)?;
                Err(if mode == InboundMode::AwaitingAck {
                    CoinbaseDirectSessionError::Subscription
                } else {
                    CoinbaseDirectSessionError::WebSocketProtocol
                })
            }
            Message::Ping(payload) => {
                send_with_deadline(
                    socket,
                    Message::Pong(payload),
                    cancellation,
                    self.config.limits().websocket().io_timeout(),
                )
                .await?;
                Ok(MessageDisposition::Control)
            }
            Message::Pong(_) => Ok(MessageDisposition::Control),
            Message::Close(_frame) => {
                flush_with_deadline(
                    socket,
                    cancellation,
                    self.config.limits().websocket().io_timeout(),
                )
                .await?;
                Err(SourceError::ProviderUnavailable.into())
            }
            Message::Frame(_) => Err(CoinbaseDirectSessionError::WebSocketProtocol),
        }
    }

    fn capture_decode_commit(
        &mut self,
        transport: TransportFrameKind,
        payload: Bytes,
        output: &mut dyn CoinbaseDirectOutput,
        mode: InboundMode,
    ) -> Result<MessageDisposition, CoinbaseDirectSessionError> {
        ensure_frame_bound(
            payload.len(),
            self.config.limits().websocket().max_frame_bytes(),
        )?;
        let frame = self.authority.frames_mut()?.try_frame(transport, payload)?;
        let decoded = {
            let validated = self.authority.validate_live_frame(&frame)?;
            if mode == InboundMode::AwaitingAck {
                validate_subscription_ack(validated.frame().payload(), self.config.product())
                    .map(|()| None)
            } else {
                match self.decoder.decode(&validated) {
                    Ok(outcome) => Ok(Some(outcome)),
                    Err(CoinbaseDirectDecodeError::UnsupportedMessage)
                        if validate_subscription_ack(
                            validated.frame().payload(),
                            self.config.product(),
                        )
                        .is_ok() =>
                    {
                        Err(CoinbaseDirectSessionError::Subscription)
                    }
                    Err(error) => Err(error.into()),
                }
            }
        };
        output.try_publish(frame).map_err(SourceError::Sink)?;
        let Some(decoded) = decoded? else {
            return Ok(MessageDisposition::Acknowledged);
        };
        match decoded {
            CoinbaseDirectDecodeOutcome::Sequenced(event) => match mode {
                InboundMode::Queueing => self.book.try_queue(event)?,
                InboundMode::Live => {
                    self.book.try_apply_live(event)?;
                    self.publish_book(output)?;
                }
                InboundMode::AwaitingAck => {
                    return Err(CoinbaseDirectSessionError::Subscription);
                }
            },
            CoinbaseDirectDecodeOutcome::NonBook(event) => {
                output
                    .try_publish_non_book(event)
                    .map_err(SourceError::Sink)?;
            }
        }
        Ok(MessageDisposition::Data)
    }

    fn capture_rejected_protocol_frame(
        &mut self,
        transport: TransportFrameKind,
        payload: Bytes,
        output: &mut dyn CoinbaseDirectOutput,
    ) -> Result<(), CoinbaseDirectSessionError> {
        ensure_frame_bound(
            payload.len(),
            self.config.limits().websocket().max_frame_bytes(),
        )?;
        let frame = self.authority.frames_mut()?.try_frame(transport, payload)?;
        let _validated = self.authority.validate_live_frame(&frame)?;
        output.try_publish(frame).map_err(SourceError::Sink)?;
        Ok(())
    }

    fn publish_book(
        &mut self,
        output: &mut dyn CoinbaseDirectOutput,
    ) -> Result<(), CoinbaseDirectSessionError> {
        let sequence = self
            .book
            .last_sequence()
            .ok_or(DirectOrderBookError::WrongPhase)?;
        let source_timestamp = self
            .book
            .source_timestamp()
            .ok_or(DirectOrderBookError::SnapshotTimestampRequired)?;
        let snapshot_receipt = self
            .snapshot_receipt
            .as_ref()
            .ok_or(DirectOrderBookError::SnapshotReceiptRequired)?;
        let book = self
            .book
            .published_book()
            .ok_or(DirectOrderBookError::WrongPhase)?;
        output
            .try_publish_book(CoinbaseDirectBookUpdate {
                sequence,
                source_timestamp,
                snapshot_receipt,
                book,
            })
            .map_err(|error| SourceError::Sink(error).into())
    }

    fn validate_generation(&self) -> Result<(), SourceError> {
        self.authority.validate_current()?;
        let issued = self
            .authority
            .budget()?
            .ok_or(SourceError::GenerationAuthorityMismatch)?;
        if !self.budget.shares_allocation_with(issued) {
            return Err(SourceError::GenerationAuthorityMismatch);
        }
        Ok(())
    }

    fn authorize_endpoint(&self, target: &str) -> Result<(), SourceError> {
        self.config
            .metadata()
            .network_policy()
            .authorize(target)
            .map_err(|_error| SourceError::InvalidProtocolState)
    }

    fn acquire_budget(&self) -> Result<BudgetPermit, SourceError> {
        self.validate_generation()?;
        match self.budget.try_acquire() {
            BudgetDecision::Ready(permit) => Ok(permit),
            BudgetDecision::WaitUntil(deadline) => Err(SourceError::BudgetWaitUntil { deadline }),
            BudgetDecision::Unavailable(reason) => Err(SourceError::BudgetUnavailable { reason }),
        }
    }

    fn begin_generation(&mut self) -> Result<(), SourceError> {
        if self.generation_started {
            return Err(SourceError::InvalidProtocolState);
        }
        self.generation_started = true;
        Ok(())
    }

    fn finish_generation(
        &mut self,
        outcome: Result<(), CoinbaseDirectSessionError>,
    ) -> Result<(), CoinbaseDirectSessionError> {
        if outcome.is_err() {
            self.book.invalidate_generation();
            self.snapshot_receipt = None;
        }
        outcome
    }

    async fn shutdown_socket<S>(
        &mut self,
        socket: &mut WebSocketStream<S>,
        output: &mut dyn CoinbaseDirectOutput,
        deadline: Duration,
    ) -> Result<(), CoinbaseDirectSessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let operation = async {
            socket
                .send(Message::Close(None))
                .await
                .map_err(|_error| CoinbaseDirectSessionError::Shutdown)?;
            loop {
                match socket.next().await {
                    Some(Ok(Message::Close(_))) | None => return Ok(()),
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Text(text))) => {
                        self.capture_rejected_protocol_frame(
                            TransportFrameKind::Text,
                            Bytes::copy_from_slice(text.as_bytes()),
                            output,
                        )?;
                        return Err(CoinbaseDirectSessionError::Shutdown);
                    }
                    Some(Ok(Message::Binary(payload))) => {
                        self.capture_rejected_protocol_frame(
                            TransportFrameKind::Binary,
                            payload,
                            output,
                        )?;
                        return Err(CoinbaseDirectSessionError::Shutdown);
                    }
                    Some(Ok(Message::Ping(_) | Message::Frame(_))) | Some(Err(_)) => {
                        return Err(CoinbaseDirectSessionError::Shutdown);
                    }
                }
            }
        };
        tokio::time::timeout(deadline, operation)
            .await
            .map_err(|_elapsed| CoinbaseDirectSessionError::Shutdown)?
    }
}

impl SourceMetadataProvider for CoinbaseDirectSession {
    fn metadata(&self) -> &SourceMetadata {
        self.config.metadata()
    }
}

fn direct_http_bounds(
    config: &CoinbaseDirectConfig,
) -> Result<market_squawk_sources::HttpRequestBounds, CoinbaseDirectSessionError> {
    match config.metadata().network_policy() {
        NetworkAccessPolicy::Allowlisted(policy) => Ok(policy.request_bounds()),
        NetworkAccessPolicy::Denied => Err(SourceError::InvalidProtocolState.into()),
    }
}

fn current_unix_seconds() -> Result<u64, CoinbaseDirectSigningError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs())
        .filter(|seconds| *seconds > 0)
        .ok_or(CoinbaseDirectSigningError::InvalidTimestamp)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InboundMode {
    AwaitingAck,
    Queueing,
    Live,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MessageDisposition {
    Acknowledged,
    Control,
    Data,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubscriptionAck {
    #[serde(rename = "type")]
    kind: String,
    channels: [SubscriptionAckChannel; 1],
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubscriptionAckChannel {
    name: String,
    product_ids: [String; 1],
}

fn validate_subscription_ack(
    payload: &[u8],
    product: &market_squawk_domain::ProviderProduct,
) -> Result<(), CoinbaseDirectSessionError> {
    let ack: SubscriptionAck = serde_json::from_slice(payload)
        .map_err(|_error| CoinbaseDirectSessionError::Subscription)?;
    let channel = &ack.channels[0];
    if ack.kind != "subscriptions"
        || channel.name != "full"
        || channel.product_ids[0] != product.as_source_identifier().as_str()
    {
        return Err(CoinbaseDirectSessionError::Subscription);
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
        output: &dyn CoinbaseDirectOutput,
        transport_timeout: Duration,
    ) -> Result<Self, SourceError> {
        let transport = Instant::now()
            .checked_add(transport_timeout)
            .ok_or(SourceError::InvalidProtocolState)?;
        match output.next_deadline() {
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

async fn read_with_deadline<S>(
    socket: &mut WebSocketStream<S>,
    output: &mut dyn CoinbaseDirectOutput,
    cancellation: &CancellationToken,
    transport_timeout: Duration,
) -> Result<Message, CoinbaseDirectSessionError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let deadline = ReceiveDeadline::strictest(output, transport_timeout)?;
    let next = tokio::select! {
        biased;
        () = cancellation.cancelled() => return Err(SourceError::Cancelled.into()),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline.at)) => {
            if deadline.sink_owned {
                output
                    .poll_deadline(Instant::now())
                    .map_err(SourceError::Sink)?;
                return Err(SourceError::InvalidProtocolState.into());
            }
            return Err(SourceError::ConnectionIdle.into());
        }
        result = socket.next() => result,
    };
    map_next_message(next)
}

fn map_next_message(
    message: Option<Result<Message, WebSocketError>>,
) -> Result<Message, CoinbaseDirectSessionError> {
    match message {
        Some(Ok(message)) => Ok(message),
        Some(Err(_error)) => Err(SourceError::Network.into()),
        None => Err(SourceError::ProviderUnavailable.into()),
    }
}

async fn await_websocket<T, E, F>(
    cancellation: &CancellationToken,
    deadline: Duration,
    operation: impl Future<Output = Result<T, E>>,
    map_error: F,
) -> Result<T, CoinbaseDirectSessionError>
where
    F: FnOnce(E) -> SourceError,
{
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(SourceError::Cancelled.into()),
        result = tokio::time::timeout(deadline, operation) => match result {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => Err(map_error(error).into()),
            Err(_elapsed) => Err(SourceError::Network.into()),
        }
    }
}

async fn send_with_deadline<S>(
    socket: &mut WebSocketStream<S>,
    message: Message,
    cancellation: &CancellationToken,
    deadline: Duration,
) -> Result<(), CoinbaseDirectSessionError>
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
    deadline: Duration,
) -> Result<(), CoinbaseDirectSessionError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    await_websocket(cancellation, deadline, socket.flush(), |_error| {
        SourceError::Network
    })
    .await
}

fn ensure_frame_bound(actual: usize, maximum: usize) -> Result<(), CoinbaseDirectSessionError> {
    if actual > maximum {
        Err(SourceError::FrameTooLarge { max: maximum }.into())
    } else {
        Ok(())
    }
}

fn handoff_frontier_payload(
    receipt: &SegmentedHttpResponseReceipt,
    generation: ConnectionGeneration,
) -> Bytes {
    let mut payload = [0_u8; 56];
    payload[..8].copy_from_slice(b"MSQCBF01");
    payload[8..40].copy_from_slice(&receipt.body_digest().bytes());
    payload[40..48].copy_from_slice(&receipt.received_at().unix_nanos().to_be_bytes());
    payload[48..].copy_from_slice(&generation.get().to_be_bytes());
    Bytes::copy_from_slice(&payload)
}

fn content_type_is_json(value: Option<&[u8]>) -> bool {
    value
        .and_then(|value| std::str::from_utf8(value).ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("application/json"))
}

fn map_http_transport_error(error: CoinbaseDirectHttpTransportError) -> CoinbaseDirectSessionError {
    match error {
        CoinbaseDirectHttpTransportError::Network => SourceError::Network.into(),
        CoinbaseDirectHttpTransportError::Deadline => CoinbaseDirectSessionError::HttpDeadline,
        CoinbaseDirectHttpTransportError::Cancelled => SourceError::Cancelled.into(),
        CoinbaseDirectHttpTransportError::BodyTooLarge => {
            CoinbaseDirectSessionError::HttpBodyTooLarge
        }
        CoinbaseDirectHttpTransportError::SegmentLimit => {
            CoinbaseDirectSessionError::HttpSegmentLimit
        }
        CoinbaseDirectHttpTransportError::Protocol
        | CoinbaseDirectHttpTransportError::Allocation => CoinbaseDirectSessionError::HttpResponse,
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
#[path = "direct_transport/tests.rs"]
mod tests;
