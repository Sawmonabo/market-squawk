use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::{FutureExt, SinkExt, StreamExt, future::BoxFuture};
use market_squawk_domain::{InstrumentId, MetadataRevision, SourceId, SourceIdentifier};
use market_squawk_sources::{
    ActiveLiveSourceGeneration, BudgetDecision, BudgetPermit, LiveMarketSource,
    LiveSourceGeneration, RawMarketSink, RetryAfter, SharedProviderBudget, SourceError,
    SourceMetadata, SourceMetadataProvider, TlsProviderCapability, TransportFrameKind,
    apply_http_retry_after,
};
use reqwest::header::{
    ACCEPT, ACCEPT_ENCODING, AUTHORIZATION, CONTENT_ENCODING, CONTENT_TYPE, HeaderValue,
    RETRY_AFTER,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::{
    Error as WebSocketError, Message, error::CapacityError, protocol::WebSocketConfig,
};
use tokio_tungstenite::{WebSocketStream, connect_async_with_config};
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

use crate::config::{
    MAX_STREAM_SYMBOLS, TRADIER_MARKET_SESSION_ENDPOINT, TRADIER_WEBSOCKET_ENDPOINT,
    TradierAccessSurface, TradierInstrumentKind, TradierSourceConfig, TradierTransportLimits,
};
use crate::{TradierAccessToken, TradierConfigError, TradierRateLimitEvidence};

const USER_AGENT_VALUE: &str = concat!(
    "market-squawk/",
    env!("CARGO_PKG_VERSION"),
    " tradier-market-data"
);
const TRADIER_HTTP_STREAM_ENDPOINT: &str = "https://stream.tradier.com/v1/markets/events";
const MAX_SESSION_RESPONSE_BYTES: usize = 64 * 1024;
const MAX_SESSION_ID_BYTES: usize = 256;
const MAX_SUBSCRIPTION_BYTES: usize = 16 * 1024;
const MAX_EVENTS_PER_WEBSOCKET_MESSAGE: usize = 1_024;

/// One account-owned Tradier market-data transport shared by every logical surface.
///
/// This owner is intentionally not `Clone`. Sources and snapshot clients receive private shared
/// state from it, including one WebSocket-session gate and one exact provider-budget binding.
#[derive(Debug)]
pub struct TradierAccountMarketData {
    pub(crate) inner: Arc<TradierAccountInner>,
}

pub(crate) struct TradierAccountInner {
    pub(crate) token: TradierAccessToken,
    pub(crate) client: reqwest::Client,
    pub(crate) limits: TradierTransportLimits,
    active_stream: AtomicBool,
    budget: Mutex<Option<SharedProviderBudget>>,
    latest_rate_limit: Mutex<Option<TradierRateLimitEvidence>>,
}

impl std::fmt::Debug for TradierAccountInner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TradierAccountInner")
            .field("token", &"[REDACTED]")
            .field("limits", &self.limits)
            .field("active_stream", &self.active_stream.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl TradierAccountMarketData {
    /// Constructs a hardened production transport after explicit TLS-provider installation.
    ///
    /// # Errors
    ///
    /// Returns an error if the pinned Reqwest/Rustls client cannot be constructed.
    pub fn try_new(
        token: TradierAccessToken,
        limits: TradierTransportLimits,
        tls_provider: TlsProviderCapability,
    ) -> Result<Self, TradierAccountMarketDataError> {
        let _provider_identity = tls_provider.provider_id();
        let http = limits.http();
        let client = reqwest::Client::builder()
            .https_only(true)
            .tls_backend_rustls()
            .min_tls_version(reqwest::tls::Version::TLS_1_2)
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .referer(false)
            .retry(reqwest::retry::never())
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .connect_timeout(Duration::from_nanos(http.connect_timeout_nanos()))
            .read_timeout(Duration::from_nanos(http.read_timeout_nanos()))
            .timeout(Duration::from_nanos(http.total_timeout_nanos()))
            .user_agent(USER_AGENT_VALUE)
            .build()
            .map_err(|_| TradierAccountMarketDataError::HttpClient)?;
        Ok(Self {
            inner: Arc::new(TradierAccountInner {
                token,
                client,
                limits,
                active_stream: AtomicBool::new(false),
                budget: Mutex::new(None),
                latest_rate_limit: Mutex::new(None),
            }),
        })
    }

    /// Creates an account-owned subscription authority that survives connection generations.
    ///
    /// `initial_symbols` must be a nonempty subset of the configured streaming mappings. The
    /// authority retains only the latest complete selection even while no generation is running.
    ///
    /// # Errors
    ///
    /// Rejects non-streaming profiles, mismatched account bounds, and an invalid initial subset.
    pub fn subscription_authority(
        &self,
        config: &TradierSourceConfig,
        initial_symbols: Vec<SourceIdentifier>,
    ) -> Result<TradierSubscriptionAuthority, TradierAccountMarketDataError> {
        validate_streaming_config(config)?;
        self.ensure_limits(config)?;
        let binding = Arc::new(SubscriptionBinding::from_config(config));
        let initial = SubscriptionSet::try_new(
            binding.mappings.as_ref(),
            initial_symbols
                .into_iter()
                .map(|symbol| symbol.as_str().to_owned())
                .collect(),
        )?;
        let (sender, _receiver) = watch::channel(Arc::new(initial));
        Ok(TradierSubscriptionAuthority {
            owner: Arc::downgrade(&self.inner),
            binding,
            sender,
        })
    }

    /// Creates one exact-generation WebSocket source from a stable subscription authority.
    ///
    /// Central supervision retains `subscriptions` across failures and calls this method with each
    /// freshly minted generation. The source immediately observes the latest retained subset.
    ///
    /// # Errors
    ///
    /// Rejects a different account owner, source/revision/mapping set, transport limits, budget
    /// allocation, non-streaming profile, or stale generation authority.
    pub fn streaming_source_with_authority(
        &self,
        config: TradierSourceConfig,
        generation: LiveSourceGeneration,
        subscriptions: &TradierSubscriptionAuthority,
    ) -> Result<TradierStreamingSource, TradierAccountMarketDataError> {
        validate_streaming_config(&config)?;
        self.ensure_limits(&config)?;
        let receiver = subscriptions.subscribe(self, &config)?;
        let authority = generation.try_start(config.metadata())?;
        let budget = authority
            .budget()?
            .cloned()
            .ok_or(TradierAccountMarketDataError::MissingBudget)?;
        self.bind_budget(&budget)?;
        Ok(TradierStreamingSource {
            config,
            authority,
            account: Arc::clone(&self.inner),
            budget,
            subscriptions: receiver,
            generation_started: false,
        })
    }

    /// Creates one exact-generation source and a new all-symbol subscription authority.
    ///
    /// This compatibility wrapper is suitable for one generation. Supervisors that reconnect must
    /// retain [`TradierSubscriptionAuthority`] and use [`Self::streaming_source_with_authority`].
    ///
    /// # Errors
    ///
    /// Rejects derived-index profiles, mismatched transport limits, stale generation authority,
    /// or a budget allocation different from the account owner's existing logical surfaces.
    pub fn streaming_source(
        &self,
        config: TradierSourceConfig,
        generation: LiveSourceGeneration,
    ) -> Result<
        (TradierStreamingSource, TradierSubscriptionController),
        TradierAccountMarketDataError,
    > {
        let initial = config
            .mappings()
            .iter()
            .map(|mapping| mapping.symbol().clone())
            .collect::<Vec<_>>();
        let subscriptions = self.subscription_authority(&config, initial)?;
        let source = self.streaming_source_with_authority(config, generation, &subscriptions)?;
        Ok((source, subscriptions))
    }

    pub(crate) fn ensure_limits(
        &self,
        config: &TradierSourceConfig,
    ) -> Result<(), TradierAccountMarketDataError> {
        if config.transport_limits() != self.inner.limits {
            Err(TradierAccountMarketDataError::TransportMismatch)
        } else {
            Ok(())
        }
    }

    pub(crate) fn bind_budget(
        &self,
        budget: &SharedProviderBudget,
    ) -> Result<(), TradierAccountMarketDataError> {
        let mut binding = self
            .inner
            .budget
            .lock()
            .map_err(|_| TradierAccountMarketDataError::BudgetBindingUnavailable)?;
        match binding.as_ref() {
            Some(existing) if !existing.shares_allocation_with(budget) => {
                Err(TradierAccountMarketDataError::BudgetMismatch)
            }
            Some(_) => Ok(()),
            None => {
                *binding = Some(budget.clone());
                Ok(())
            }
        }
    }

    /// Returns the latest complete provider rate-limit header set seen by any logical surface.
    ///
    /// # Errors
    ///
    /// Fails closed if the process-local evidence lock was poisoned.
    pub fn latest_rate_limit_evidence(
        &self,
    ) -> Result<Option<TradierRateLimitEvidence>, TradierAccountMarketDataError> {
        self.inner
            .latest_rate_limit
            .lock()
            .map(|evidence| *evidence)
            .map_err(|_| TradierAccountMarketDataError::RateEvidenceUnavailable)
    }

    /// Returns whether this account owner currently holds Tradier's sole physical market stream.
    pub fn has_active_stream(&self) -> bool {
        self.inner.active_stream.load(Ordering::Acquire)
    }
}

impl TradierAccountInner {
    pub(crate) fn record_rate_limit(
        &self,
        evidence: TradierRateLimitEvidence,
    ) -> Result<(), SourceError> {
        let mut latest = self
            .latest_rate_limit
            .lock()
            .map_err(|_| SourceError::InvalidProtocolState)?;
        *latest = Some(evidence);
        Ok(())
    }
}

fn validate_streaming_config(
    config: &TradierSourceConfig,
) -> Result<(), TradierAccountMarketDataError> {
    if !config.profile().supports_streaming()
        || config.access_surface() != TradierAccessSurface::Streaming
    {
        Err(TradierAccountMarketDataError::StreamingUnsupported)
    } else {
        Ok(())
    }
}

/// Account-owned latest-value subscription intent retained across connection generations.
#[derive(Clone)]
pub struct TradierSubscriptionAuthority {
    owner: Weak<TradierAccountInner>,
    binding: Arc<SubscriptionBinding>,
    sender: watch::Sender<Arc<SubscriptionSet>>,
}

impl std::fmt::Debug for TradierSubscriptionAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TradierSubscriptionAuthority")
            .field("source_id", &self.binding.source_id)
            .field("metadata_revision", &self.binding.metadata_revision)
            .field("allowed_symbols", &self.binding.mappings.len())
            .field("active_symbols", &self.active_symbol_count())
            .finish_non_exhaustive()
    }
}

impl TradierSubscriptionAuthority {
    /// Replaces the active symbols with a nonempty, duplicate-free configured subset.
    ///
    /// The watch channel retains only the newest complete selection, so rapid UI/client updates
    /// cannot create an unbounded queue. The latest value is retained when no generation is
    /// connected and becomes the initial value for the next generation.
    ///
    /// # Errors
    ///
    /// Rejects unknown, duplicate, empty, or oversized selections.
    pub fn replace(&self, symbols: Vec<SourceIdentifier>) -> Result<(), TradierSubscriptionError> {
        let symbols = symbols
            .into_iter()
            .map(|symbol| symbol.as_str().to_owned())
            .collect::<Vec<_>>();
        let selection = SubscriptionSet::try_new(self.binding.mappings.as_ref(), symbols)?;
        let _previous = self.sender.send_replace(Arc::new(selection));
        Ok(())
    }

    /// Returns the active symbol count most recently accepted by the controller.
    pub fn active_symbol_count(&self) -> usize {
        self.sender.borrow().symbols.len()
    }

    /// Returns an owned, internally consistent snapshot of the exact active provider symbols.
    ///
    /// The snapshot reads one retained watch value and therefore cannot mix selections across a
    /// concurrent replacement. Its size cannot exceed the configured 256-symbol streaming bound.
    ///
    /// # Errors
    ///
    /// Returns [`TradierSubscriptionError::SnapshotAllocation`] if bounded vector or identifier
    /// storage cannot be reserved, or [`TradierSubscriptionError::InvalidRetainedState`] if an
    /// internal retained symbol no longer satisfies the domain identity invariant.
    pub fn current_symbols(&self) -> Result<Vec<SourceIdentifier>, TradierSubscriptionError> {
        let current = self.sender.borrow();
        let mut snapshot = Vec::new();
        snapshot
            .try_reserve_exact(current.symbols.len())
            .map_err(|_| TradierSubscriptionError::SnapshotAllocation)?;
        for symbol in &current.symbols {
            let mut owned = String::new();
            owned
                .try_reserve_exact(symbol.len())
                .map_err(|_| TradierSubscriptionError::SnapshotAllocation)?;
            owned.push_str(symbol);
            snapshot.push(
                SourceIdentifier::try_from(owned)
                    .map_err(|_| TradierSubscriptionError::InvalidRetainedState)?,
            );
        }
        Ok(snapshot)
    }

    fn subscribe(
        &self,
        account: &TradierAccountMarketData,
        config: &TradierSourceConfig,
    ) -> Result<watch::Receiver<Arc<SubscriptionSet>>, TradierAccountMarketDataError> {
        let same_owner = self
            .owner
            .upgrade()
            .is_some_and(|owner| Arc::ptr_eq(&owner, &account.inner));
        if !same_owner || !self.binding.matches(config) {
            return Err(TradierAccountMarketDataError::SubscriptionAuthorityMismatch);
        }
        Ok(self.sender.subscribe())
    }
}

/// Compatibility name for the stable account-owned subscription authority.
pub type TradierSubscriptionController = TradierSubscriptionAuthority;

#[derive(Debug)]
struct SubscriptionBinding {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    mappings: Arc<[SubscriptionMapping]>,
}

impl SubscriptionBinding {
    fn from_config(config: &TradierSourceConfig) -> Self {
        let mut mappings = config
            .mappings()
            .iter()
            .map(|mapping| SubscriptionMapping {
                symbol: mapping.symbol().as_str().to_owned(),
                instrument: mapping.instrument(),
                kind: mapping.kind(),
            })
            .collect::<Vec<_>>();
        mappings.sort_unstable_by(|left, right| left.symbol.cmp(&right.symbol));
        Self {
            source_id: config.metadata().source_id().clone(),
            metadata_revision: config.metadata().revision().clone(),
            mappings: mappings.into(),
        }
    }

    fn matches(&self, config: &TradierSourceConfig) -> bool {
        &self.source_id == config.metadata().source_id()
            && &self.metadata_revision == config.metadata().revision()
            && self.mappings.len() == config.mappings().len()
            && self.mappings.iter().all(|expected| {
                config.mappings().iter().any(|mapping| {
                    expected.symbol.as_str() == mapping.symbol().as_str()
                        && expected.instrument == mapping.instrument()
                        && expected.kind == mapping.kind()
                })
            })
    }
}

#[derive(Debug, Eq, PartialEq)]
struct SubscriptionMapping {
    symbol: String,
    instrument: InstrumentId,
    kind: TradierInstrumentKind,
}

#[derive(Debug)]
struct SubscriptionSet {
    symbols: Box<[String]>,
}

impl SubscriptionSet {
    fn try_new(
        allowed: &[SubscriptionMapping],
        mut symbols: Vec<String>,
    ) -> Result<Self, TradierSubscriptionError> {
        if symbols.is_empty() || symbols.len() > MAX_STREAM_SYMBOLS {
            return Err(TradierSubscriptionError::InvalidCount);
        }
        symbols.sort_unstable();
        if symbols.windows(2).any(|window| window[0] == window[1]) {
            return Err(TradierSubscriptionError::DuplicateSymbol);
        }
        if symbols.iter().any(|symbol| {
            !allowed
                .iter()
                .any(|allowed| allowed.symbol.as_str() == symbol.as_str())
        }) {
            return Err(TradierSubscriptionError::UnknownSymbol);
        }
        Ok(Self {
            symbols: symbols.into_boxed_slice(),
        })
    }

    fn payload(&self, session_id: &str) -> Result<Zeroizing<String>, SourceError> {
        let wire = SubscriptionWire {
            symbols: self.symbols.iter().map(String::as_str).collect(),
            session_id,
            filter: ["quote", "tradex"],
            linebreak: true,
            valid_only: true,
            advanced_details: false,
        };
        let payload =
            serde_json::to_string(&wire).map_err(|_| SourceError::InvalidProtocolState)?;
        if payload.len() > MAX_SUBSCRIPTION_BYTES {
            return Err(SourceError::InvalidProtocolState);
        }
        Ok(Zeroizing::new(payload))
    }
}

#[derive(Serialize)]
struct SubscriptionWire<'a> {
    symbols: Vec<&'a str>,
    #[serde(rename = "sessionid")]
    session_id: &'a str,
    filter: [&'static str; 2],
    linebreak: bool,
    #[serde(rename = "validOnly")]
    valid_only: bool,
    #[serde(rename = "advancedDetails")]
    advanced_details: bool,
}

/// Production one-generation Tradier WebSocket source.
#[derive(Debug)]
pub struct TradierStreamingSource {
    config: TradierSourceConfig,
    authority: ActiveLiveSourceGeneration,
    account: Arc<TradierAccountInner>,
    budget: SharedProviderBudget,
    subscriptions: watch::Receiver<Arc<SubscriptionSet>>,
    generation_started: bool,
}

impl TradierStreamingSource {
    fn begin_generation(&mut self) -> Result<(), SourceError> {
        if self.generation_started {
            return Err(SourceError::InvalidProtocolState);
        }
        self.generation_started = true;
        Ok(())
    }

    fn validate_generation(&self) -> Result<(), SourceError> {
        let issued = self
            .authority
            .budget()?
            .ok_or(SourceError::GenerationAuthorityMismatch)?;
        if !issued.shares_allocation_with(&self.budget) {
            return Err(SourceError::GenerationAuthorityMismatch);
        }
        Ok(())
    }

    fn acquire_budget(&self) -> Result<BudgetPermit, SourceError> {
        match self.budget.try_acquire() {
            BudgetDecision::Ready(permit) => Ok(permit),
            refusal => Err(SourceError::from_applied_budget_refusal(refusal)),
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
            .authorize(TRADIER_MARKET_SESSION_ENDPOINT)
            .map_err(|_| SourceError::InvalidProtocolState)?;
        self.config
            .metadata()
            .network_policy()
            .authorize(TRADIER_WEBSOCKET_ENDPOINT)
            .map_err(|_| SourceError::InvalidProtocolState)?;
        let _session_gate = AccountStreamLease::try_acquire(&self.account)?;
        let permit = self.acquire_budget()?;
        let session = create_session(
            &self.account,
            &self.budget,
            &cancellation,
            self.config.transport_limits(),
        )
        .await?;
        let limits = self.config.transport_limits();
        let websocket = WebSocketConfig::default()
            .read_buffer_size(limits.max_frame_bytes().clamp(4 * 1024, 128 * 1024))
            .write_buffer_size(16 * 1024)
            .max_write_buffer_size(32 * 1024)
            .max_message_size(Some(limits.max_frame_bytes()))
            .max_frame_size(Some(limits.max_frame_bytes()));
        let connect = connect_async_with_config(TRADIER_WEBSOCKET_ENDPOINT, Some(websocket), true);
        let (socket, _response) = await_operation(
            &cancellation,
            Duration::from_nanos(limits.http().connect_timeout_nanos()),
            connect,
            |error| map_websocket_error(error, &self.budget),
        )
        .await?;
        self.run_socket(socket, session, permit, sink, cancellation)
            .await
    }

    async fn run_socket<S>(
        &mut self,
        mut socket: WebSocketStream<S>,
        session: StreamingSession,
        permit: BudgetPermit,
        sink: &mut dyn RawMarketSink,
        cancellation: CancellationToken,
    ) -> Result<(), SourceError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        self.validate_generation()?;
        let initial = self.subscriptions.borrow().clone();
        send_subscription(
            &mut socket,
            &initial,
            session.session_id.as_str(),
            &cancellation,
            self.config.transport_limits().io_timeout(),
        )
        .await?;
        sink.bind_active_request_budget(permit.active_lease())?;
        let mut subscription_channel_open = true;
        loop {
            let deadline =
                ReceiveDeadline::strictest(sink, self.config.transport_limits().io_timeout())?;
            let next = tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err(SourceError::Cancelled),
                changed = self.subscriptions.changed(), if subscription_channel_open => {
                    match changed {
                        Ok(()) => SocketAction::SubscriptionChanged,
                        Err(_) => {
                            subscription_channel_open = false;
                            SocketAction::SubscriptionChannelClosed
                        }
                    }
                }
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline.at)) => {
                    if deadline.sink_owned {
                        sink.poll_deadline(Instant::now())?;
                        return Err(SourceError::InvalidProtocolState);
                    }
                    return Err(SourceError::ConnectionIdle);
                }
                message = socket.next() => SocketAction::Message(message),
            };
            match next {
                SocketAction::SubscriptionChanged => {
                    let selection = self.subscriptions.borrow_and_update().clone();
                    send_subscription(
                        &mut socket,
                        &selection,
                        session.session_id.as_str(),
                        &cancellation,
                        self.config.transport_limits().io_timeout(),
                    )
                    .await?;
                }
                SocketAction::SubscriptionChannelClosed => {}
                SocketAction::Message(Some(Ok(Message::Text(text)))) => {
                    self.publish_text_message(text.as_bytes(), sink)?;
                }
                SocketAction::Message(Some(Ok(Message::Binary(payload)))) => {
                    ensure_frame_bound(
                        payload.len(),
                        self.config.transport_limits().max_frame_bytes(),
                    )?;
                    let frame = self
                        .authority
                        .frames_mut()?
                        .try_frame(TransportFrameKind::Binary, payload)?;
                    sink.try_publish(frame)?;
                }
                SocketAction::Message(Some(Ok(Message::Ping(payload)))) => {
                    send_message(
                        &mut socket,
                        Message::Pong(payload),
                        &cancellation,
                        self.config.transport_limits().io_timeout(),
                    )
                    .await?;
                }
                SocketAction::Message(Some(Ok(Message::Pong(_)))) => {}
                SocketAction::Message(Some(Ok(Message::Close(_))))
                | SocketAction::Message(None) => {
                    return Err(SourceError::ProviderUnavailable);
                }
                SocketAction::Message(Some(Ok(Message::Frame(_)))) => {
                    return Err(SourceError::InvalidProtocolState);
                }
                SocketAction::Message(Some(Err(error))) => {
                    return Err(map_websocket_error(error, &self.budget));
                }
            }
        }
    }

    fn publish_text_message(
        &mut self,
        payload: &[u8],
        sink: &mut dyn RawMarketSink,
    ) -> Result<(), SourceError> {
        ensure_frame_bound(
            payload.len(),
            self.config.transport_limits().max_frame_bytes(),
        )?;
        let mut count = 0_usize;
        for line in payload.split(|byte| *byte == b'\n') {
            let line = trim_ascii_whitespace(line);
            if line.is_empty() {
                continue;
            }
            count = count
                .checked_add(1)
                .ok_or(SourceError::InvalidProtocolState)?;
            if count > MAX_EVENTS_PER_WEBSOCKET_MESSAGE {
                return Err(SourceError::InvalidProtocolState);
            }
            let frame = self
                .authority
                .frames_mut()?
                .try_frame(TransportFrameKind::Text, Bytes::copy_from_slice(line))?;
            sink.try_publish(frame)?;
        }
        if count == 0 {
            return Err(SourceError::InvalidProtocolState);
        }
        Ok(())
    }
}

impl SourceMetadataProvider for TradierStreamingSource {
    fn metadata(&self) -> &SourceMetadata {
        self.config.metadata()
    }
}

impl LiveMarketSource for TradierStreamingSource {
    fn run<'a>(
        &'a mut self,
        sink: &'a mut dyn RawMarketSink,
        cancellation: CancellationToken,
    ) -> BoxFuture<'a, Result<(), SourceError>> {
        self.run_production(sink, cancellation).boxed()
    }
}

enum SocketAction {
    SubscriptionChanged,
    SubscriptionChannelClosed,
    Message(Option<Result<Message, WebSocketError>>),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionEnvelope {
    stream: SessionWire,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionWire {
    url: String,
    sessionid: String,
}

struct StreamingSession {
    session_id: Zeroizing<String>,
}

async fn create_session(
    account: &TradierAccountInner,
    budget: &SharedProviderBudget,
    cancellation: &CancellationToken,
    limits: TradierTransportLimits,
) -> Result<StreamingSession, SourceError> {
    let operation = async {
        let authorization = format!("Bearer {}", account.token.expose());
        let mut authorization = Zeroizing::new(authorization);
        let mut authorization_header =
            HeaderValue::try_from(authorization.as_str()).map_err(|_| SourceError::Unauthorized)?;
        authorization_header.set_sensitive(true);
        authorization.clear();
        let response = account
            .client
            .post(TRADIER_MARKET_SESSION_ENDPOINT)
            .header(ACCEPT, "application/json")
            .header(ACCEPT_ENCODING, "identity")
            .header(AUTHORIZATION, authorization_header)
            .send()
            .await
            .map_err(|_| SourceError::Network)?;
        let status = response.status();
        if response.url().as_str() != TRADIER_MARKET_SESSION_ENDPOINT {
            return Err(SourceError::InvalidProtocolState);
        }
        let headers = response.headers().clone();
        if matches!(status.as_u16(), 401 | 403) {
            return Err(SourceError::Unauthorized);
        }
        let rate = TradierRateLimitEvidence::try_from_headers(&headers)
            .map_err(|_| SourceError::InvalidProtocolState)?;
        account.record_rate_limit(rate)?;
        if status.as_u16() == 429 || status.is_server_error() {
            return Err(rate_limited_error(budget, &headers, Some(rate)));
        }
        if !status.is_success() {
            return Err(SourceError::ProviderUnavailable);
        }
        let content_type = singleton_header(&headers, CONTENT_TYPE)?
            .and_then(|value| std::str::from_utf8(value).ok())
            .ok_or(SourceError::InvalidProtocolState)?;
        if !content_type
            .split(';')
            .next()
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
        {
            return Err(SourceError::InvalidProtocolState);
        }
        if singleton_header(&headers, CONTENT_ENCODING)?
            .is_some_and(|value| !value.eq_ignore_ascii_case(b"identity"))
        {
            return Err(SourceError::InvalidProtocolState);
        }
        if response.content_length().is_some_and(|length| {
            usize::try_from(length).map_or(true, |length| length > MAX_SESSION_RESPONSE_BYTES)
        }) {
            return Err(SourceError::FrameTooLarge {
                max: MAX_SESSION_RESPONSE_BYTES,
            });
        }
        let body = collect_bounded(response.bytes_stream(), MAX_SESSION_RESPONSE_BYTES).await?;
        let session = serde_json::from_slice::<SessionEnvelope>(&body)
            .map_err(|_| SourceError::InvalidProtocolState)?;
        if session.stream.url != TRADIER_HTTP_STREAM_ENDPOINT
            || session.stream.sessionid.is_empty()
            || session.stream.sessionid.len() > MAX_SESSION_ID_BYTES
            || !session
                .stream
                .sessionid
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err(SourceError::InvalidProtocolState);
        }
        budget
            .record_success()
            .map_err(|_| SourceError::ProviderUnavailable)?;
        Ok(StreamingSession {
            session_id: Zeroizing::new(session.stream.sessionid),
        })
    };
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(SourceError::Cancelled),
        result = tokio::time::timeout(
            Duration::from_nanos(limits.http().total_timeout_nanos()),
            operation,
        ) => result.map_err(|_| SourceError::Network)?,
    }
}

pub(crate) async fn collect_bounded<S, E>(
    mut stream: S,
    maximum: usize,
) -> Result<Bytes, SourceError>
where
    S: futures_util::Stream<Item = Result<Bytes, E>> + Unpin,
{
    let mut body = Vec::new();
    body.try_reserve_exact(maximum.min(64 * 1024))
        .map_err(|_| SourceError::InvalidProtocolState)?;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| SourceError::Network)?;
        let next = body
            .len()
            .checked_add(chunk.len())
            .ok_or(SourceError::FrameTooLarge { max: maximum })?;
        if next > maximum {
            return Err(SourceError::FrameTooLarge { max: maximum });
        }
        body.extend_from_slice(&chunk);
    }
    Ok(Bytes::from(body))
}

async fn send_subscription<S>(
    socket: &mut WebSocketStream<S>,
    selection: &SubscriptionSet,
    session_id: &str,
    cancellation: &CancellationToken,
    timeout: Duration,
) -> Result<(), SourceError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let payload = selection.payload(session_id)?;
    send_message(
        socket,
        Message::Text(payload.as_str().into()),
        cancellation,
        timeout,
    )
    .await
}

async fn send_message<S>(
    socket: &mut WebSocketStream<S>,
    message: Message,
    cancellation: &CancellationToken,
    timeout: Duration,
) -> Result<(), SourceError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    await_operation(cancellation, timeout, socket.send(message), |_| {
        SourceError::Network
    })
    .await
}

async fn await_operation<T, E, F>(
    cancellation: &CancellationToken,
    timeout: Duration,
    operation: impl std::future::Future<Output = Result<T, E>>,
    map_error: F,
) -> Result<T, SourceError>
where
    F: FnOnce(E) -> SourceError,
{
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(SourceError::Cancelled),
        result = tokio::time::timeout(timeout, operation) => match result {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => Err(map_error(error)),
            Err(_) => Err(SourceError::Network),
        }
    }
}

pub(crate) fn rate_limited_error(
    budget: &SharedProviderBudget,
    headers: &reqwest::header::HeaderMap,
    rate: Option<TradierRateLimitEvidence>,
) -> SourceError {
    let decision = match headers.get(RETRY_AFTER) {
        Some(value) => apply_http_retry_after(budget, Some(value.as_bytes()), 1_000),
        None => match rate
            .and_then(|evidence| evidence.expires_at_unix_millis().checked_mul(1_000_000))
            .and_then(|nanos| i64::try_from(nanos).ok())
        {
            Some(nanos) => budget.apply_retry_after(RetryAfter::AtWallClock(
                market_squawk_domain::Timestamp::from_unix_nanos(nanos),
            )),
            None => budget.apply_refusal(1_000),
        },
    };
    SourceError::from_applied_budget_refusal(decision)
}

fn singleton_header(
    headers: &reqwest::header::HeaderMap,
    name: reqwest::header::HeaderName,
) -> Result<Option<&[u8]>, SourceError> {
    let mut values = headers.get_all(name).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() || value.as_bytes().len() > 256 {
        return Err(SourceError::InvalidProtocolState);
    }
    Ok(Some(value.as_bytes()))
}

fn map_websocket_error(error: WebSocketError, budget: &SharedProviderBudget) -> SourceError {
    match error {
        WebSocketError::Capacity(CapacityError::MessageTooLong { .. }) => {
            SourceError::FrameTooLarge {
                max: market_squawk_sources::MAX_RAW_FRAME_BYTES,
            }
        }
        WebSocketError::Http(response) if matches!(response.status().as_u16(), 401 | 403) => {
            SourceError::Unauthorized
        }
        WebSocketError::Http(response)
            if response.status().as_u16() == 429 || response.status().is_server_error() =>
        {
            SourceError::from_applied_budget_refusal(apply_http_retry_after(
                budget,
                response
                    .headers()
                    .get(RETRY_AFTER)
                    .map(|value| value.as_bytes()),
                1_000,
            ))
        }
        _ => SourceError::Network,
    }
}

fn ensure_frame_bound(actual: usize, maximum: usize) -> Result<(), SourceError> {
    if actual > maximum {
        Err(SourceError::FrameTooLarge { max: maximum })
    } else {
        Ok(())
    }
}

fn trim_ascii_whitespace(mut value: &[u8]) -> &[u8] {
    while value.first().is_some_and(u8::is_ascii_whitespace) {
        value = &value[1..];
    }
    while value.last().is_some_and(u8::is_ascii_whitespace) {
        value = &value[..value.len().saturating_sub(1)];
    }
    value
}

struct AccountStreamLease {
    account: Arc<TradierAccountInner>,
}

impl AccountStreamLease {
    fn try_acquire(account: &Arc<TradierAccountInner>) -> Result<Self, SourceError> {
        account
            .active_stream
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| SourceError::ProviderUnavailable)?;
        Ok(Self {
            account: Arc::clone(account),
        })
    }
}

impl Drop for AccountStreamLease {
    fn drop(&mut self) {
        self.account.active_stream.store(false, Ordering::Release);
    }
}

#[derive(Clone, Copy, Debug)]
struct ReceiveDeadline {
    at: Instant,
    sink_owned: bool,
}

impl ReceiveDeadline {
    fn strictest(sink: &dyn RawMarketSink, timeout: Duration) -> Result<Self, SourceError> {
        let transport = Instant::now()
            .checked_add(timeout)
            .ok_or(SourceError::InvalidProtocolState)?;
        match sink.next_deadline() {
            Some(deadline) if deadline <= transport => Ok(Self {
                at: deadline,
                sink_owned: true,
            }),
            _ => Ok(Self {
                at: transport,
                sink_owned: false,
            }),
        }
    }
}

/// Account-owner construction or source-composition failure.
#[derive(Debug, Error)]
pub enum TradierAccountMarketDataError {
    /// A hardened Reqwest client could not be constructed.
    #[error("Tradier HTTP client construction failed")]
    HttpClient,
    /// This logical profile is REST-only under the official provider contract.
    #[error("Tradier logical profile does not support WebSocket streaming")]
    StreamingUnsupported,
    /// This source registration describes streaming rather than REST/bootstrap access.
    #[error("Tradier source registration does not support REST snapshots")]
    RestUnsupported,
    /// Source and account transport bounds differ.
    #[error("Tradier logical source transport limits differ from its account owner")]
    TransportMismatch,
    /// The source generation omitted its required provider budget.
    #[error("Tradier source generation has no provider budget")]
    MissingBudget,
    /// Logical surfaces were issued different provider budget allocations.
    #[error("Tradier logical surfaces do not share one account budget authority")]
    BudgetMismatch,
    /// The process-local budget binding lock was unavailable.
    #[error("Tradier account budget binding is unavailable")]
    BudgetBindingUnavailable,
    /// The process-local provider rate-evidence lock was unavailable.
    #[error("Tradier rate-limit evidence is unavailable")]
    RateEvidenceUnavailable,
    /// A subscription authority belonged to a different account or source mapping revision.
    #[error("Tradier subscription authority does not match the account and source generation")]
    SubscriptionAuthorityMismatch,
    /// Registry-minted generation authority was invalid or stale.
    #[error("Tradier source generation is invalid: {0}")]
    Source(#[from] SourceError),
    /// Initial or replacement subscriptions were invalid.
    #[error("Tradier subscription is invalid: {0}")]
    Subscription(#[from] TradierSubscriptionError),
    /// A logical source configuration was invalid.
    #[error("Tradier configuration is invalid: {0}")]
    Config(#[from] TradierConfigError),
}

/// Bounded subscription replacement failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum TradierSubscriptionError {
    /// Selection is empty or beyond the local streaming ceiling.
    #[error("Tradier subscription symbol count is invalid")]
    InvalidCount,
    /// Selection contains a duplicate symbol.
    #[error("Tradier subscription contains a duplicate symbol")]
    DuplicateSymbol,
    /// Selection names a symbol outside this source's metadata.
    #[error("Tradier subscription contains an unknown symbol")]
    UnknownSymbol,
    /// An owned subscription snapshot could not reserve its bounded storage.
    #[error("Tradier subscription snapshot allocation failed")]
    SnapshotAllocation,
    /// Retained authority state violated an identifier invariant.
    #[error("Tradier subscription authority retained invalid state")]
    InvalidRetainedState,
}
