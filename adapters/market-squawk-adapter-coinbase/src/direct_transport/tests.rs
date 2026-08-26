use std::collections::VecDeque;
use std::error::Error;
use std::io::Read as _;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::str::FromStr as _;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt as _, StreamExt as _, future::BoxFuture};
use market_squawk_domain::{
    AuthorizationBasis, ConnectionGeneration, Currency, DataQuality, Denomination, DigestAlgorithm,
    EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, InstrumentDefinitionRevision,
    InstrumentExecutionTerms, InstrumentId, LiveEventClass, LotSize, MarketDepth, MetadataRevision,
    ProviderProduct, RevisionBoundPayloadEvidence, SourceId, SourceIdentifier, TickSize, Timestamp,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationMode,
    AuthorizationSubjectResolutionError, AuthorizationSubjectResolver, BackoffPolicy, BudgetScope,
    BudgetUnavailableReason, DecodedControlFrame, DecoderEvidence, DirectBookLimits,
    DirectSyncPhase, FreshnessPolicy, LiveSourceGeneration, ProviderBookSide, ProviderBudgetPolicy,
    ProviderDecimalLexeme, ProviderObservationPayload, ProviderOrderEvent, ProviderOrderEventKind,
    RawMarketFrame, RawMarketSink, SessionId, SinkError, SourceError,
};
use sha2::{Digest as _, Sha256};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, oneshot};
use tokio_tungstenite::{accept_async, client_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;

use super::{
    CoinbaseDirectHttpRequest, CoinbaseDirectHttpResponse, CoinbaseDirectHttpTransport,
    CoinbaseDirectHttpTransportError, CoinbaseDirectOrderLevelPayload,
    CoinbaseDirectOrderLevelUpdate, CoinbaseDirectOutput, CoinbaseDirectOutputAdmission,
    CoinbaseDirectSession, CoinbaseDirectSessionError,
};
use crate::{
    CoinbaseDirectAuthentication, CoinbaseDirectConfig, CoinbaseDirectLimits,
    CoinbaseDirectNonBookEvent, CoinbaseDirectProductEvidence, CoinbaseDirectSigningCapability,
    CoinbaseDirectSigningError, CoinbaseDirectSigningRequest, CoinbaseMarketChannel,
    CoinbaseMarketContinuity, CoinbaseMarketFeed, CoinbaseProductMapping, CoinbaseTransportLimits,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

const PRODUCT_BODY: &[u8] = br#"{"id":"BTC-USD","status":"online","base_increment":"0.00000001","quote_increment":"0.01","trading_disabled":false,"cancel_only":false,"post_only":false,"limit_only":false,"auction_mode":false}"#;
const SNAPSHOT_BODY: &[u8] = br#"{"sequence":104,"time":"2026-07-24T21:34:10.604Z","bids":[["100.00","1.00000000","bid-1"]],"asks":[["101.00","2.00000000","ask-1"]]}"#;
const SEQUENCE_101: &str = r#"{"type":"received","time":"2026-07-24T21:34:10.601Z","product_id":"BTC-USD","sequence":101,"order_id":"order-101","order_type":"limit","size":"1.00000000","price":"100.00","side":"buy"}"#;
const SEQUENCE_102: &str = r#"{"type":"received","time":"2026-07-24T21:34:10.602Z","product_id":"BTC-USD","sequence":102,"order_id":"order-102","order_type":"limit","size":"1.00000000","price":"100.00","side":"buy"}"#;
const SEQUENCE_103: &str = r#"{"type":"received","time":"2026-07-24T21:34:10.603Z","product_id":"BTC-USD","sequence":103,"order_id":"order-103","order_type":"limit","size":"1.00000000","price":"100.00","side":"buy"}"#;
const SEQUENCE_104: &str = r#"{"type":"received","time":"2026-07-24T21:34:10.604Z","product_id":"BTC-USD","sequence":104,"order_id":"order-104","order_type":"limit","size":"1.00000000","price":"100.00","side":"buy"}"#;
const SEQUENCE_105: &str = r#"{"type":"match","time":"2026-07-24T21:34:10.605Z","product_id":"BTC-USD","sequence":105,"trade_id":7005,"maker_order_id":"bid-1","taker_order_id":"taker-105","size":"0.25000000","price":"100.00","side":"buy"}"#;
const SEQUENCE_106: &str = r#"{"type":"received","time":"2026-07-24T21:34:10.606Z","product_id":"BTC-USD","sequence":106,"order_id":"order-106","order_type":"limit","size":"1.00000000","price":"100.00","side":"buy"}"#;
const PRIVATE_RECEIVED: &str = r#"{"type":"received","time":"2026-07-24T21:34:10.607Z","product_id":"BTC-USD","order_id":"private-order","order_type":"limit","size":"1.00000000","price":"100.00","side":"buy","user_id":"fixture-user"}"#;
const SUBSCRIPTION_ACK: &str =
    r#"{"type":"subscriptions","channels":[{"name":"full","product_ids":["BTC-USD"]}]}"#;

#[derive(Debug)]
struct FixtureSigner;

impl CoinbaseDirectSigningCapability for FixtureSigner {
    fn sign(
        &self,
        request: CoinbaseDirectSigningRequest<'_>,
    ) -> Result<CoinbaseDirectAuthentication, CoinbaseDirectSigningError> {
        assert_eq!(request.timestamp(), "1721847600");
        assert_eq!(request.method(), "GET");
        assert_eq!(request.path(), "/users/self/verify");
        CoinbaseDirectAuthentication::try_new(
            "fixture-key".to_owned(),
            "fixture-passphrase".to_owned(),
            "fixture-signature".to_owned(),
        )
    }
}

#[derive(Debug)]
struct FixtureAuthorizationSubjectResolver {
    subject: SourceIdentifier,
}

impl AuthorizationSubjectResolver for FixtureAuthorizationSubjectResolver {
    fn resolve_subject_record(
        &self,
        mode: AuthorizationMode,
        _evidence: EvidenceDigest,
    ) -> Result<SourceIdentifier, AuthorizationSubjectResolutionError> {
        if mode != AuthorizationMode::UserAuthorized {
            return Err(AuthorizationSubjectResolutionError::UnsupportedMode);
        }
        Ok(self.subject.clone())
    }
}

struct ScriptedHttpResponse {
    expected_url: Box<str>,
    started: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
    response: CoinbaseDirectHttpResponse,
}

struct ScriptedHttpTransport {
    scripts: Mutex<VecDeque<ScriptedHttpResponse>>,
}

impl std::fmt::Debug for ScriptedHttpTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ScriptedHttpTransport")
    }
}

impl CoinbaseDirectHttpTransport for ScriptedHttpTransport {
    fn get(
        &self,
        request: CoinbaseDirectHttpRequest,
    ) -> BoxFuture<'_, Result<CoinbaseDirectHttpResponse, CoinbaseDirectHttpTransportError>> {
        let script = self
            .scripts
            .lock()
            .map_err(|_| CoinbaseDirectHttpTransportError::Protocol)
            .and_then(|mut scripts| {
                scripts
                    .pop_front()
                    .ok_or(CoinbaseDirectHttpTransportError::Protocol)
            });
        Box::pin(async move {
            let script = script?;
            if request.url() != script.expected_url.as_ref() {
                return Err(CoinbaseDirectHttpTransportError::Protocol);
            }
            let _started = script.started.send(());
            tokio::select! {
                biased;
                () = request.cancellation().cancelled() => {
                    Err(CoinbaseDirectHttpTransportError::Cancelled)
                }
                released = script.release => {
                    released.map_err(|_| CoinbaseDirectHttpTransportError::Network)?;
                    Ok(script.response)
                }
            }
        })
    }
}

struct HttpControls {
    product_started: oneshot::Receiver<()>,
    release_product: oneshot::Sender<()>,
    snapshot_started: oneshot::Receiver<()>,
    release_snapshot: oneshot::Sender<()>,
}

fn scripted_http(
    config: &CoinbaseDirectConfig,
) -> (Arc<dyn CoinbaseDirectHttpTransport>, HttpControls) {
    let (product_started_tx, product_started) = oneshot::channel();
    let (release_product, product_release_rx) = oneshot::channel();
    let (snapshot_started_tx, snapshot_started) = oneshot::channel();
    let (release_snapshot, snapshot_release_rx) = oneshot::channel();
    let scripts = VecDeque::from([
        ScriptedHttpResponse {
            expected_url: config.product_url().to_owned().into_boxed_str(),
            started: product_started_tx,
            release: product_release_rx,
            response: successful_http_response(config.product_url(), PRODUCT_BODY),
        },
        ScriptedHttpResponse {
            expected_url: config.snapshot_url().to_owned().into_boxed_str(),
            started: snapshot_started_tx,
            release: snapshot_release_rx,
            response: successful_http_response(config.snapshot_url(), SNAPSHOT_BODY),
        },
    ]);
    (
        Arc::new(ScriptedHttpTransport {
            scripts: Mutex::new(scripts),
        }),
        HttpControls {
            product_started,
            release_product,
            snapshot_started,
            release_snapshot,
        },
    )
}

fn successful_http_response(url: &str, body: &'static [u8]) -> CoinbaseDirectHttpResponse {
    CoinbaseDirectHttpResponse {
        status: 200,
        final_url: url.to_owned().into_boxed_str(),
        declared_body_length: Some(u64::try_from(body.len()).unwrap_or(u64::MAX)),
        retry_after: None,
        content_type: Some(Box::from(&b"application/json"[..])),
        content_encoding: Some(Box::from(&b"identity"[..])),
        segments: vec![Bytes::from_static(body)],
    }
}

#[derive(Debug, Eq, PartialEq)]
enum RecordedPublication {
    Snapshot {
        bids: Vec<(String, String)>,
        asks: Vec<(String, String)>,
    },
    Delta {
        changes: Vec<(ProviderBookSide, String, String)>,
    },
    Quote {
        bid: Option<(String, String)>,
        ask: Option<(String, String)>,
    },
}

#[derive(Debug, Eq, PartialEq)]
struct RecordedBook {
    sequence: u64,
    snapshot_url: String,
    source_identifier: String,
    event_class: LiveEventClass,
    input_depth: Option<MarketDepth>,
    output_depth: Option<MarketDepth>,
    publication: RecordedPublication,
}

#[derive(Debug, Eq, PartialEq)]
struct RecordedNativeTrade {
    trade_id: u64,
    maker_order_id: String,
    taker_order_id: String,
    maker_side: ProviderBookSide,
    price: i64,
    quantity: i64,
    sequence: u64,
    provider_timestamp: i64,
}

#[derive(Debug, Eq, PartialEq)]
struct RecordedReplayFrame {
    sequence: u64,
    payload: Vec<u8>,
    native_trade: Option<RecordedNativeTrade>,
}

#[derive(Debug, Eq, PartialEq)]
struct RecordedPendingHandoff {
    snapshot_sequence: u64,
    terminal_sequence: u64,
    snapshot_body: Vec<u8>,
    replay: Vec<RecordedReplayFrame>,
    request_set_digest: [u8; 32],
    subscription_digest: [u8; 32],
}

#[derive(Debug, Eq, PartialEq)]
enum RecordedOrderLevel {
    Snapshot {
        generation: u64,
        snapshot_sequence: u64,
        orders: Vec<(String, ProviderBookSide, i64, i64)>,
        replay_sequences: Vec<u64>,
    },
    Event {
        sequence: u64,
        order_identity: Option<String>,
    },
}

#[derive(Debug, Default)]
struct RecordingOutput {
    frames: Vec<RawMarketFrame>,
    product_statuses: Vec<String>,
    private_events: usize,
    books: Vec<RecordedBook>,
    order_level: Vec<RecordedOrderLevel>,
    handoffs: Vec<RecordedPendingHandoff>,
    pending_handoff: Option<crate::CoinbaseMarketHandoff>,
    output_admission: Option<CoinbaseDirectOutputAdmission>,
    discarded_sequences: Vec<u64>,
    reject_raw_at: Option<usize>,
    sequence_101_captured: Arc<Notify>,
    sequence_102_captured: Arc<Notify>,
    sequence_106_captured: Arc<Notify>,
    first_book: Arc<Notify>,
}

impl RawMarketSink for RecordingOutput {
    fn try_publish(&mut self, frame: RawMarketFrame) -> Result<(), SinkError> {
        let ordinal = self.frames.len().saturating_add(1);
        if self.reject_raw_at == Some(ordinal) {
            return Err(SinkError::Saturated);
        }
        if frame.payload() == SEQUENCE_101.as_bytes() {
            self.sequence_101_captured.notify_one();
        }
        if frame.payload() == SEQUENCE_102.as_bytes() {
            self.sequence_102_captured.notify_one();
        }
        if frame.payload() == SEQUENCE_106.as_bytes() {
            self.sequence_106_captured.notify_one();
        }
        self.frames.push(frame);
        Ok(())
    }
}

impl CoinbaseDirectOutput for RecordingOutput {
    fn try_admit_replay(
        &mut self,
        admission: CoinbaseDirectOutputAdmission,
    ) -> Result<(), SinkError> {
        if self.output_admission.replace(admission).is_some() {
            return Err(SinkError::CaptureIncomplete);
        }
        Ok(())
    }

    fn try_publish_subscription_acknowledgement(
        &mut self,
        acknowledgement: DecodedControlFrame,
    ) -> Result<(), SinkError> {
        if self
            .frames
            .last()
            .is_none_or(|frame| frame.frame_id() != acknowledgement.evidence().frame_id())
        {
            return Err(SinkError::CaptureIncomplete);
        }
        Ok(())
    }

    fn try_publish_product(
        &mut self,
        evidence: CoinbaseDirectProductEvidence,
    ) -> Result<(), SinkError> {
        self.product_statuses
            .push(evidence.provider_status().as_str().to_owned());
        Ok(())
    }

    fn try_publish_non_book(
        &mut self,
        _event: CoinbaseDirectNonBookEvent,
    ) -> Result<(), SinkError> {
        self.private_events = self.private_events.saturating_add(1);
        Ok(())
    }

    fn try_retain_sequenced_frame(&mut self, evidence: &DecoderEvidence) -> Result<(), SinkError> {
        if self
            .frames
            .last()
            .is_none_or(|frame| frame.frame_id() != evidence.frame_id())
        {
            return Err(SinkError::CaptureIncomplete);
        }
        Ok(())
    }

    fn try_discard_sequenced_frame(&mut self, evidence: &DecoderEvidence) -> Result<(), SinkError> {
        let frame = self
            .frames
            .iter()
            .find(|frame| frame.frame_id() == evidence.frame_id())
            .ok_or(SinkError::CaptureIncomplete)?;
        let sequence = serde_json::from_slice::<serde_json::Value>(frame.payload())
            .ok()
            .and_then(|value| value.get("sequence").and_then(serde_json::Value::as_u64))
            .ok_or(SinkError::CaptureIncomplete)?;
        self.discarded_sequences.push(sequence);
        Ok(())
    }

    fn try_publish_order_level(
        &mut self,
        update: CoinbaseDirectOrderLevelUpdate<'_>,
    ) -> Result<(), SinkError> {
        update
            .validate_current()
            .map_err(|_error| SinkError::CaptureIncomplete)?;
        if update.market_depth() != MarketDepth::OrderLevel {
            return Err(SinkError::CaptureIncomplete);
        }
        let recorded = match update.payload() {
            CoinbaseDirectOrderLevelPayload::Snapshot {
                snapshot_sequence,
                orders,
                replay,
                ..
            } => RecordedOrderLevel::Snapshot {
                generation: update.connection_generation().get(),
                snapshot_sequence: snapshot_sequence.get(),
                orders: orders
                    .iter()
                    .map(|order| {
                        (
                            order.order_id().as_str().to_owned(),
                            order.side(),
                            order.price().get(),
                            order.quantity().get(),
                        )
                    })
                    .collect(),
                replay_sequences: replay.iter().map(|event| event.sequence().get()).collect(),
            },
            CoinbaseDirectOrderLevelPayload::Event(event) => RecordedOrderLevel::Event {
                sequence: event.sequence().get(),
                order_identity: order_identity(event),
            },
        };
        self.order_level.push(recorded);
        Ok(())
    }

    fn try_publish_book(&mut self, handoff: crate::CoinbaseMarketHandoff) -> Result<(), SinkError> {
        let evidence = handoff.evidence();
        let (snapshot_sequence, terminal_sequence) = match evidence.continuity() {
            CoinbaseMarketContinuity::SnapshotContiguous { snapshot, terminal } => {
                (snapshot.get(), terminal.get())
            }
            CoinbaseMarketContinuity::ProviderCursorUnverified { .. } => {
                return Err(SinkError::CaptureIncomplete);
            }
        };
        if evidence.feed() != CoinbaseMarketFeed::ExchangeDirectFull
            || evidence.channel() != CoinbaseMarketChannel::Full
            || evidence.product().as_source_identifier().as_str() != "BTC-USD"
            || evidence.venue().as_str() != "coinbase-exchange"
            || evidence.event_class() != LiveEventClass::BookSnapshot
            || evidence.native_input_depth() != Some(MarketDepth::OrderLevel)
            || evidence.output_depth() != Some(MarketDepth::PriceLevel)
            || evidence.subscription_acknowledgement().is_none()
            || evidence.request_set_digest().bytes()
                != [
                    0xe1, 0x65, 0x33, 0x9d, 0x1c, 0xfb, 0x77, 0xb9, 0x21, 0xa2, 0x51, 0x30, 0xf3,
                    0x6a, 0x0d, 0xc4, 0xfa, 0x68, 0x56, 0x2e, 0xc1, 0xcb, 0xc7, 0x2b, 0xfa, 0xd6,
                    0xdb, 0x16, 0x4e, 0x15, 0x99, 0xb6,
                ]
            || evidence.subscription_digest().bytes()
                != [
                    0x1f, 0xc6, 0x84, 0x81, 0x41, 0x1f, 0xb9, 0x07, 0x5f, 0xa8, 0xe7, 0x84, 0x69,
                    0x94, 0x68, 0x2e, 0x21, 0x32, 0x70, 0x6d, 0xf2, 0x94, 0x1b, 0x5f, 0xd2, 0xfb,
                    0x06, 0xb6, 0xe6, 0x7a, 0xa9, 0x8f,
                ]
            || handoff.raw_payload_digest() != handoff.typed_batch().evidence().payload_digest()
        {
            return Err(SinkError::CaptureIncomplete);
        }
        let crate::CoinbaseMarketRawLineage::DirectInitial(lineage) = handoff.raw_lineage() else {
            return Err(SinkError::CaptureIncomplete);
        };
        let snapshot_receipt = lineage.snapshot().receipt();
        let mut snapshot_body = Vec::new();
        lineage
            .snapshot()
            .reader()
            .read_to_end(&mut snapshot_body)
            .map_err(|_error| SinkError::CaptureIncomplete)?;
        let replay = lineage
            .replay()
            .iter()
            .map(|frame| {
                let native_trade = frame.native_trade().map(|trade| RecordedNativeTrade {
                    trade_id: trade.trade_id(),
                    maker_order_id: trade.maker_order_id().as_str().to_owned(),
                    taker_order_id: trade.taker_order_id().as_str().to_owned(),
                    maker_side: trade.maker_side(),
                    price: trade.price().get(),
                    quantity: trade.quantity().get(),
                    sequence: trade.sequence().get(),
                    provider_timestamp: trade.provider_timestamp().unix_nanos(),
                });
                RecordedReplayFrame {
                    sequence: frame.sequence().get(),
                    payload: frame.raw_payload().as_bytes().to_vec(),
                    native_trade,
                }
            })
            .collect::<Vec<_>>();
        let observation = handoff
            .typed_batch()
            .observations()
            .first()
            .ok_or(SinkError::CaptureIncomplete)?;
        if observation.event_class() != evidence.event_class()
            || observation.depth() != evidence.output_depth()
            || snapshot_receipt.status() != 200
            || snapshot_receipt.body_length() != SNAPSHOT_BODY.len() as u64
            || snapshot_receipt.body_digest().bytes()
                != <[u8; 32]>::from(Sha256::digest(SNAPSHOT_BODY))
            || snapshot_body != SNAPSHOT_BODY
            || replay.last().is_none_or(|frame| {
                frame.sequence != terminal_sequence || frame.payload != SEQUENCE_106.as_bytes()
            })
        {
            return Err(SinkError::CaptureIncomplete);
        }
        let publication = match observation.payload() {
            ProviderObservationPayload::BookSnapshot(snapshot) => RecordedPublication::Snapshot {
                bids: snapshot.bids().iter().map(record_level).collect::<Vec<_>>(),
                asks: snapshot.asks().iter().map(record_level).collect::<Vec<_>>(),
            },
            ProviderObservationPayload::BookDelta(delta) => RecordedPublication::Delta {
                changes: delta
                    .changes()
                    .iter()
                    .map(|change| {
                        let (price, quantity) = record_level(change.level());
                        (change.side(), price, quantity)
                    })
                    .collect::<Vec<_>>(),
            },
            ProviderObservationPayload::Quote { bid, ask } => RecordedPublication::Quote {
                bid: bid.as_ref().map(record_level),
                ask: ask.as_ref().map(record_level),
            },
            _ => return Err(SinkError::CaptureIncomplete),
        };
        self.handoffs.push(RecordedPendingHandoff {
            snapshot_sequence,
            terminal_sequence,
            snapshot_body,
            replay,
            request_set_digest: evidence.request_set_digest().bytes(),
            subscription_digest: evidence.subscription_digest().bytes(),
        });
        self.books.push(RecordedBook {
            sequence: terminal_sequence,
            snapshot_url: snapshot_receipt.final_url().to_owned(),
            source_identifier: observation.source_identifier().as_str().to_owned(),
            event_class: observation.event_class(),
            input_depth: evidence.native_input_depth(),
            output_depth: observation.depth(),
            publication,
        });
        self.first_book.notify_one();
        self.pending_handoff = Some(handoff);
        Ok(())
    }
}

fn order_identity(event: &ProviderOrderEvent) -> Option<String> {
    match event.kind() {
        ProviderOrderEventKind::Open(order) => Some(order.order_id().as_str().to_owned()),
        ProviderOrderEventKind::Match { maker_order_id, .. }
        | ProviderOrderEventKind::Done {
            order_id: maker_order_id,
            ..
        }
        | ProviderOrderEventKind::Change {
            order_id: maker_order_id,
            ..
        } => Some(maker_order_id.as_str().to_owned()),
        ProviderOrderEventKind::CursorOnly(_) => None,
    }
}

fn record_level(level: &market_squawk_sources::ProviderBookLevel) -> (String, String) {
    (
        level.price().value().as_str().to_owned(),
        level.quantity().value().as_str().to_owned(),
    )
}

#[tokio::test]
async fn direct_session_queues_during_http_replays_then_hands_the_same_owner_to_live() -> TestResult
{
    let config = config()?;
    assert_eq!(config.publication_depth(), MarketDepth::OrderLevel);
    assert_eq!(
        config.metadata().quality_ceiling(),
        DataQuality::DirectUnverified
    );
    let complete_session_bytes = config.limits().checked_maximum_retained_bytes()?;
    assert_eq!(
        config.checked_maximum_retained_bytes()?,
        complete_session_bytes
    );
    let live_coverage = config
        .metadata()
        .coverage()
        .live()
        .ok_or("missing Coinbase order-level coverage")?;
    assert!(
        live_coverage
            .rule_for(LiveEventClass::BookSnapshot, Some(MarketDepth::OrderLevel))
            .is_some()
    );
    assert!(
        live_coverage
            .rule_for(LiveEventClass::BookDelta, Some(MarketDepth::OrderLevel))
            .is_some()
    );
    let (mut registry, session, generation) = live_generation(&config, "direct-transport-happy")?;
    let budget = session
        .budget()
        .ok_or("fixture session lacks its shared provider budget")?
        .clone();
    let (http, controls) = scripted_http(&config);
    let mut direct =
        CoinbaseDirectSession::try_new_with_transport(config.clone(), generation, http)?;
    let first_book = Arc::new(Notify::new());
    let sequence_101_captured = Arc::new(Notify::new());
    let sequence_102_captured = Arc::new(Notify::new());
    let sequence_106_captured = Arc::new(Notify::new());
    let mut output = RecordingOutput {
        first_book: Arc::clone(&first_book),
        sequence_101_captured: Arc::clone(&sequence_101_captured),
        sequence_102_captured: Arc::clone(&sequence_102_captured),
        sequence_106_captured: Arc::clone(&sequence_106_captured),
        ..RecordingOutput::default()
    };
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let mut socket = accept_async(stream).await?;
        let subscription = socket
            .next()
            .await
            .ok_or("signed subscription was not sent")??
            .into_text()?;
        let subscription: serde_json::Value = serde_json::from_str(&subscription)?;
        assert_eq!(subscription["type"], "subscribe");
        assert_eq!(subscription["product_ids"][0], "BTC-USD");
        assert_eq!(subscription["channels"][0], "full");
        assert_eq!(subscription["key"], "fixture-key");
        assert_eq!(subscription["passphrase"], "fixture-passphrase");
        assert_eq!(subscription["signature"], "fixture-signature");
        assert!(matches!(
            budget.try_reserve_request(),
            market_squawk_sources::BudgetReservationDecision::Unavailable(
                BudgetUnavailableReason::ConcurrencyExhausted
            )
        ));
        socket.send(Message::Text(SUBSCRIPTION_ACK.into())).await?;

        controls.product_started.await?;
        assert!(matches!(
            budget.try_reserve_request(),
            market_squawk_sources::BudgetReservationDecision::Unavailable(
                BudgetUnavailableReason::ConcurrencyExhausted
            )
        ));
        socket.send(Message::Text(SEQUENCE_101.into())).await?;
        sequence_101_captured.notified().await;
        controls
            .release_product
            .send(())
            .map_err(|_| "product request was dropped")?;

        controls.snapshot_started.await?;
        assert!(matches!(
            budget.try_reserve_request(),
            market_squawk_sources::BudgetReservationDecision::Unavailable(
                BudgetUnavailableReason::ConcurrencyExhausted
            )
        ));
        socket.send(Message::Text(SEQUENCE_102.into())).await?;
        sequence_102_captured.notified().await;
        controls
            .release_snapshot
            .send(())
            .map_err(|_| "snapshot request was dropped")?;

        let frontier = socket
            .next()
            .await
            .ok_or("snapshot handoff frontier Ping was not sent")??;
        let Message::Ping(frontier) = frontier else {
            return Err("snapshot handoff frontier was not a Ping".into());
        };
        assert_eq!(frontier.len(), 56);
        assert_eq!(&frontier[..8], b"MSQCBF01");
        socket.send(Message::Pong(frontier)).await?;

        socket.send(Message::Text(PRIVATE_RECEIVED.into())).await?;
        socket.send(Message::Text(SEQUENCE_103.into())).await?;
        socket.send(Message::Text(SEQUENCE_104.into())).await?;
        socket.send(Message::Text(SEQUENCE_105.into())).await?;
        let publication_frontier = socket
            .next()
            .await
            .ok_or("snapshot publication frontier Ping was not sent")??;
        let Message::Ping(publication_frontier) = publication_frontier else {
            return Err("snapshot publication frontier was not a Ping".into());
        };
        assert_eq!(publication_frontier.len(), 56);
        assert_eq!(&publication_frontier[..8], b"MSQCBF02");
        socket.send(Message::Text(SEQUENCE_106.into())).await?;
        sequence_106_captured.notified().await;
        socket.send(Message::Pong(publication_frontier)).await?;

        first_book.notified().await;
        Ok::<(), Box<dyn Error + Send + Sync>>(())
    });
    let stream = TcpStream::connect(address).await?;
    let (socket, _) = client_async(format!("ws://{address}"), stream).await?;

    let outcome = tokio::time::timeout(
        Duration::from_secs(3),
        direct.run_with_socket_for_test(
            socket,
            &FixtureSigner,
            &mut output,
            CancellationToken::new(),
            1_721_847_600,
        ),
    )
    .await?;
    assert!(
        matches!(
            outcome,
            Err(CoinbaseDirectSessionError::SnapshotClaimRequired)
        ),
        "unexpected terminal outcome: {outcome:?}; output: {output:?}"
    );
    server
        .await?
        .map_err(|error| std::io::Error::other(error.to_string()))?;

    assert_eq!(output.frames.len(), 8);
    assert_eq!(output.frames[0].payload(), SUBSCRIPTION_ACK.as_bytes());
    assert_eq!(output.product_statuses, ["online"]);
    assert_eq!(output.private_events, 1);
    let output_admission = CoinbaseDirectOutputAdmission::try_from_config(&config)?;
    assert_eq!(output.output_admission, Some(output_admission));
    assert_eq!(
        output_admission.complete_retained_bytes(),
        complete_session_bytes
    );
    assert_eq!(output.discarded_sequences, [101, 102, 103, 104]);
    assert!(output.order_level.is_empty());
    assert_eq!(
        output.handoffs,
        [RecordedPendingHandoff {
            snapshot_sequence: 104,
            terminal_sequence: 106,
            snapshot_body: SNAPSHOT_BODY.to_vec(),
            replay: vec![
                RecordedReplayFrame {
                    sequence: 105,
                    payload: SEQUENCE_105.as_bytes().to_vec(),
                    native_trade: Some(RecordedNativeTrade {
                        trade_id: 7005,
                        maker_order_id: "bid-1".to_owned(),
                        taker_order_id: "taker-105".to_owned(),
                        maker_side: ProviderBookSide::Bid,
                        price: 10_000,
                        quantity: 25_000_000,
                        sequence: 105,
                        provider_timestamp: 1_784_928_850_605_000_000,
                    }),
                },
                RecordedReplayFrame {
                    sequence: 106,
                    payload: SEQUENCE_106.as_bytes().to_vec(),
                    native_trade: None,
                },
            ],
            request_set_digest: [
                0xe1, 0x65, 0x33, 0x9d, 0x1c, 0xfb, 0x77, 0xb9, 0x21, 0xa2, 0x51, 0x30, 0xf3, 0x6a,
                0x0d, 0xc4, 0xfa, 0x68, 0x56, 0x2e, 0xc1, 0xcb, 0xc7, 0x2b, 0xfa, 0xd6, 0xdb, 0x16,
                0x4e, 0x15, 0x99, 0xb6,
            ],
            subscription_digest: [
                0x1f, 0xc6, 0x84, 0x81, 0x41, 0x1f, 0xb9, 0x07, 0x5f, 0xa8, 0xe7, 0x84, 0x69, 0x94,
                0x68, 0x2e, 0x21, 0x32, 0x70, 0x6d, 0xf2, 0x94, 0x1b, 0x5f, 0xd2, 0xfb, 0x06, 0xb6,
                0xe6, 0x7a, 0xa9, 0x8f,
            ],
        }]
    );
    assert_eq!(
        output.books,
        [RecordedBook {
            sequence: 106,
            snapshot_url: config.snapshot_url().to_owned(),
            source_identifier: snapshot_identity(106),
            event_class: LiveEventClass::BookSnapshot,
            input_depth: Some(MarketDepth::OrderLevel),
            output_depth: Some(MarketDepth::PriceLevel),
            publication: RecordedPublication::Snapshot {
                bids: vec![("100.00".to_owned(), "0.75000000".to_owned())],
                asks: vec![("101.00".to_owned(), "2.00000000".to_owned())],
            },
        }]
    );
    assert!(output.pending_handoff.is_some());
    assert_eq!(direct.book.phase(), DirectSyncPhase::Quarantined);
    registry.end_session(&session, Timestamp::from_unix_nanos(2))?;
    Ok(())
}

#[tokio::test]
async fn raw_sink_rejection_precedes_every_decoded_state_mutation() -> TestResult {
    let config = config()?;
    let (_registry, _session, generation) =
        live_generation(&config, "direct-transport-raw-rejection")?;
    let (http, controls) = scripted_http(&config);
    let mut direct = CoinbaseDirectSession::try_new_with_transport(config, generation, http)?;
    let mut output = RecordingOutput {
        reject_raw_at: Some(2),
        ..RecordingOutput::default()
    };
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (finish_tx, finish_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let mut socket = accept_async(stream).await?;
        let _subscription = socket.next().await;
        socket.send(Message::Text(SUBSCRIPTION_ACK.into())).await?;
        controls.product_started.await?;
        socket.send(Message::Text(SEQUENCE_101.into())).await?;
        let _keep_product_pending = controls.release_product;
        let _keep_snapshot_pending = controls.release_snapshot;
        let _finish = finish_rx.await;
        Ok::<(), Box<dyn Error + Send + Sync>>(())
    });
    let stream = TcpStream::connect(address).await?;
    let (socket, _) = client_async(format!("ws://{address}"), stream).await?;

    let outcome = tokio::time::timeout(
        Duration::from_secs(3),
        direct.run_with_socket_for_test(
            socket,
            &FixtureSigner,
            &mut output,
            CancellationToken::new(),
            1_721_847_600,
        ),
    )
    .await?;
    assert!(matches!(
        outcome,
        Err(CoinbaseDirectSessionError::Source(SourceError::Sink(
            SinkError::Saturated
        )))
    ));
    assert_eq!(output.frames.len(), 1);
    assert!(output.product_statuses.is_empty());
    assert_eq!(output.private_events, 0);
    assert!(output.books.is_empty());
    assert_eq!(direct.book.phase(), DirectSyncPhase::Quarantined);
    let _finished = finish_tx.send(());
    server
        .await?
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    Ok(())
}

fn live_generation(
    config: &CoinbaseDirectConfig,
    session_id: &str,
) -> TestResult<(
    AuthoritativeSourceRegistry,
    market_squawk_sources::CurrentSourceSession,
    LiveSourceGeneration,
)> {
    let resolver = FixtureAuthorizationSubjectResolver {
        subject: identifier(&format!("{session_id}-credential"))?,
    };
    let mut registry =
        AuthoritativeSourceRegistry::try_new_ephemeral_with_authorization_subject_resolver_for_diagnostics(
            Arc::new(resolver),
        )?;
    let registered = registry.register(config.metadata().clone(), Timestamp::from_unix_nanos(1))?;
    let session = registry.begin_session(
        &registered,
        SessionId::new(identifier(session_id)?),
        ConnectionGeneration::new(1)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let capture = registry.take_capture_generation_capabilities(&session)?;
    let (mut initialization, _admission, _degradation) = capture.into_parts();
    initialization.mark_healthy()?;
    let generation = registry.take_live_source_generation(&session)?;
    Ok((registry, session, generation))
}

fn config() -> TestResult<CoinbaseDirectConfig> {
    let instrument = InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?;
    let terms = InstrumentExecutionTerms::try_new(
        instrument,
        InstrumentDefinitionRevision::try_from(1)?,
        TickSize::try_from_decimal(ProviderDecimalLexeme::try_new("0.01")?.decimal())?,
        LotSize::try_from_decimal(ProviderDecimalLexeme::try_new("0.00000001")?.decimal())?,
        Currency::try_from("USD")?,
        Denomination::Currency(Currency::try_from("BTC")?),
        ProviderDecimalLexeme::try_new("1")?.decimal(),
    )?;
    let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
    let authorization = AuthorizationGrant::new(
        AuthorizationMode::UserAuthorized,
        AuthorizationBasis::new(identifier("coinbase-read-only-market-data-account")?),
        evidence(2),
        effective,
    );
    let budget = ProviderBudgetPolicy::try_new(
        BudgetScope::for_authorization(identifier("coinbase-exchange")?, &authorization)?,
        NonZeroU32::new(8).ok_or("zero request budget")?,
        NonZeroU64::new(1_000_000_000).ok_or("zero budget window")?,
        NonZeroU16::new(1).ok_or("zero concurrency")?,
        BackoffPolicy::try_new(
            NonZeroU64::new(1_000_000).ok_or("zero initial backoff")?,
            NonZeroU64::new(1_000_000_000).ok_or("zero maximum backoff")?,
            1_000,
        )?,
    )?;
    CoinbaseDirectConfig::try_new_order_level(
        SourceId::try_from("coinbase-exchange-direct")?,
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(identifier("coinbase-direct-transport-2026-07-24")?),
            evidence(3),
        ),
        authorization,
        evidence(4),
        effective,
        CoinbaseProductMapping::try_new(ProviderProduct::new(identifier("BTC-USD")?), instrument)?,
        terms,
        FreshnessPolicy::try_new(
            5_000_000_000,
            1_000_000_000,
            2_000_000_000,
            1_000_000_000,
            100_000_000,
        )?,
        budget,
        CoinbaseDirectLimits::try_new(
            CoinbaseTransportLimits::try_new(
                256 * 1024,
                Duration::from_secs(5),
                Duration::from_secs(5),
            )?,
            16 * 1024 * 1024,
            8,
            Duration::from_secs(1),
            DirectBookLimits::try_new(128, 64, 32, 512 * 1024, 8)?,
        )?,
    )
    .map_err(Into::into)
}

fn identifier(value: &str) -> TestResult<SourceIdentifier> {
    Ok(SourceIdentifier::try_from(value)?)
}

fn evidence(byte: u8) -> ExactPayloadEvidence {
    ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        [byte; 32],
    ))
}

fn hex_digest(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn snapshot_identity(sequence: u64) -> String {
    let digest: [u8; 32] = Sha256::digest(SNAPSHOT_BODY).into();
    format!("coinbase-direct-book-{sequence}-{}", hex_digest(&digest))
}
