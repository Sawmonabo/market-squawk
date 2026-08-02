use std::error::Error;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64, NonZeroUsize};
use std::str::FromStr;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use market_squawk_domain::{
    AuthorizationBasis, ConnectionGeneration, DigestAlgorithm, EffectiveInterval, EvidenceDigest,
    ExactPayloadEvidence, InstrumentId, MetadataRevision, RevisionBoundPayloadEvidence, SourceId,
    SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationMode, BackoffPolicy,
    BudgetDecision, BudgetScope, DecodeOutcome, FreshnessPolicy, LiveMarketSource,
    LiveSourceGeneration, MarketDecoder, ProviderBudgetPolicy, RawMarketFrame, RawMarketSink,
    RegistryError, SessionId, SinkError, SourceError, SourceMetadataProvider,
};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use super::{KrakenDecoderState, KrakenSource};
use crate::{KrakenConfig, KrakenDepth, KrakenMarketDecoder, KrakenMetadataInput};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Debug)]
struct RecordingSink {
    frames: Vec<RawMarketFrame>,
    limit: usize,
}

impl Default for RecordingSink {
    fn default() -> Self {
        Self {
            frames: Vec::with_capacity(2),
            limit: 2,
        }
    }
}

impl RawMarketSink for RecordingSink {
    fn try_publish(&mut self, frame: RawMarketFrame) -> Result<(), SinkError> {
        if self.frames.len() == self.limit {
            return Err(SinkError::Saturated);
        }
        self.frames.push(frame);
        Ok(())
    }
}

const BOOK_ACK: &str = r#"{"method":"subscribe","result":{"channel":"book","depth":10,"snapshot":true,"symbol":"BTC/USD","warnings":["advisory"]},"success":true,"time_in":"2023-10-04T07:48:25Z","time_out":"2023-10-04T07:48:25Z"}"#;
const UPDATE_BEFORE_SNAPSHOT: &str = r#"{"channel":"book","type":"update","data":[{"symbol":"BTC/USD","bids":[{"price":"45283.5","qty":"0"}],"asks":[],"checksum":1,"timestamp":"2023-10-04T07:48:26Z"}]}"#;

#[tokio::test]
async fn successor_generation_requires_a_fresh_snapshot_before_health() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let mut first = accept_book_source(&listener).await?;
        first.send(Message::Text(BOOK_ACK.into())).await?;
        first
            .send(Message::Text(UPDATE_BEFORE_SNAPSHOT.into()))
            .await?;
        drop(first);

        let mut socket = accept_book_source(&listener).await?;
        socket
            .send(Message::Ping(b"health".as_slice().into()))
            .await?;
        let Some(Ok(Message::Pong(payload))) =
            tokio::time::timeout(Duration::from_secs(1), socket.next()).await?
        else {
            return Err("source did not answer the protocol ping".into());
        };
        if payload.as_ref() != b"health" {
            return Err("source changed the protocol pong payload".into());
        }
        socket.send(Message::Text(BOOK_ACK.into())).await?;
        socket
            .send(Message::Text(
                include_str!("../fixtures/official_book_checksum.json").into(),
            ))
            .await?;
        socket.close(None).await?;
        TestResult::Ok(())
    });

    let (config, mut registry, registered) =
        test_source("kraken-public-book-v2", "kraken-policy-v1")?;
    let first_session = registry.begin_session(
        &registered,
        SessionId::new(SourceIdentifier::try_from("kraken-session-quarantined")?),
        ConnectionGeneration::new(1)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let first_generation = live_generation(&mut registry, &first_session)?;
    let (mut first_socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}")).await?;
    let mut first_source = KrakenSource::try_new(config.clone(), first_generation)?;
    let mut first_sink = RecordingSink::default();

    let first_result = first_source
        .run_established(&mut first_socket, &mut first_sink, CancellationToken::new())
        .await;

    assert_eq!(first_result, Err(SourceError::InvalidProtocolState));
    assert_eq!(
        first_source.health().state(),
        KrakenDecoderState::Quarantined
    );
    assert_eq!(first_source.health().captured_frames(), 2);
    assert_eq!(first_source.health().market_messages(), 0);
    let mut first_bridge = KrakenMarketDecoder::try_new(
        first_source.metadata().clone(),
        "BTC/USD",
        InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?,
        KrakenDepth::Ten,
    )?;
    assert!(matches!(
        first_bridge.decode(&first_session.validate_live_frame(&first_sink.frames[0])?)?,
        DecodeOutcome::Control(_)
    ));
    assert!(matches!(
        first_bridge.decode(&first_session.validate_live_frame(&first_sink.frames[1])?)?,
        DecodeOutcome::Resynchronize(_)
    ));
    assert_eq!(first_bridge.state(), KrakenDecoderState::Quarantined);
    registry.end_session(&first_session, Timestamp::from_unix_nanos(2))?;
    assert!(
        first_session
            .validate_live_frame(&first_sink.frames[0])
            .is_err()
    );

    let successor_session = registry.begin_session(
        &registered,
        SessionId::new(SourceIdentifier::try_from("kraken-session-successor")?),
        ConnectionGeneration::new(2)?,
        Timestamp::from_unix_nanos(3),
    )?;
    let successor_generation = live_generation(&mut registry, &successor_session)?;
    let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}")).await?;
    let budget = successor_session
        .budget()
        .cloned()
        .ok_or("source session has no coordinated budget")?;
    let mut source = KrakenSource::try_new(config, successor_generation)?;
    let mut sink = RecordingSink::default();

    let result = source
        .run_established(&mut socket, &mut sink, CancellationToken::new())
        .await;

    assert_eq!(result, Err(SourceError::Network));
    assert_eq!(source.health().state(), KrakenDecoderState::Quarantined);
    assert_eq!(source.health().captured_frames(), 2);
    assert_eq!(source.health().control_messages(), 1);
    assert_eq!(source.health().market_messages(), 1);
    assert!(source.health().book_subscribed());
    assert!(source.health().last_market_timestamp().is_some());
    assert_eq!(sink.frames.len(), 2);
    let validated_ack = successor_session.validate_live_frame(&sink.frames[0])?;
    let mut bridge = KrakenMarketDecoder::try_new(
        source.metadata().clone(),
        "BTC/USD",
        InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?,
        KrakenDepth::Ten,
    )?;
    assert!(matches!(
        bridge.decode(&validated_ack)?,
        DecodeOutcome::Control(_)
    ));
    assert_eq!(bridge.state(), KrakenDecoderState::AwaitingSnapshot);
    let validated_snapshot = successor_session.validate_live_frame(&sink.frames[1])?;
    assert!(matches!(
        bridge.decode(&validated_snapshot)?,
        DecodeOutcome::Data(_)
    ));
    assert_eq!(bridge.state(), KrakenDecoderState::Healthy);
    assert!(first_session.validate_live_frame(&sink.frames[1]).is_err());

    let refusal = tokio_tungstenite::tungstenite::Error::Http(Box::new(
        tokio_tungstenite::tungstenite::http::Response::builder()
            .status(429)
            .header(
                tokio_tungstenite::tungstenite::http::header::RETRY_AFTER,
                "1",
            )
            .body(None)?,
    ));
    let returned_deadline = match super::map_connect_error(refusal, &budget) {
        SourceError::BudgetWaitUntil { deadline } => deadline,
        error => return Err(format!("429 mapped to {error:?} instead of a budget wait").into()),
    };
    assert!(matches!(
        budget.try_acquire(),
        BudgetDecision::WaitUntil(recorded_deadline) if recorded_deadline == returned_deadline
    ));

    server.await??;
    Ok(())
}

async fn accept_book_source(listener: &TcpListener) -> TestResult<WebSocketStream<TcpStream>> {
    let (stream, _) = listener.accept().await?;
    let mut socket = tokio_tungstenite::accept_async(stream).await?;
    let Some(Ok(Message::Text(subscription))) =
        tokio::time::timeout(Duration::from_secs(1), socket.next()).await?
    else {
        return Err("source did not send a text subscription".into());
    };
    let request: serde_json::Value = serde_json::from_str(&subscription)?;
    if request["method"] != "subscribe" || request["params"]["channel"] != "book" {
        return Err("source sent the wrong subscription".into());
    }
    Ok(socket)
}

#[test]
fn source_authority_rejects_rollover_factory_grafting_and_cross_registry_sessions() -> TestResult {
    let (config, mut registry, registered) =
        test_source("kraken-public-book-v2", "kraken-policy-v1")?;
    let first = registry.begin_session(
        &registered,
        SessionId::new(SourceIdentifier::try_from("kraken-session-first")?),
        ConnectionGeneration::new(1)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let stale_generation = live_generation(&mut registry, &first)?;
    let successor = registry.begin_session(
        &registered,
        SessionId::new(SourceIdentifier::try_from("kraken-session-successor")?),
        ConnectionGeneration::new(2)?,
        Timestamp::from_unix_nanos(2),
    )?;
    assert!(matches!(
        KrakenSource::try_new(config.clone(), stale_generation),
        Err(SourceError::SessionNotCurrent)
    ));

    let successor_capture = registry.take_capture_generation_capabilities(&successor)?;
    let (mut successor_initialization, _successor_admission, _successor_degradation) =
        successor_capture.into_parts();
    successor_initialization.mark_healthy()?;
    let _successor_factory = registry.take_raw_frame_factory(&successor)?;
    assert!(matches!(
        registry.take_live_source_generation(&successor),
        Err(RegistryError::RawFrameFactoryAlreadyTaken)
    ));

    let (foreign_config, mut foreign_registry, foreign_registered) =
        test_source("kraken-public-book-v2", "kraken-policy-v1")?;
    assert_eq!(
        foreign_config.metadata().source_id(),
        config.metadata().source_id()
    );
    assert_eq!(
        foreign_config.metadata().revision(),
        config.metadata().revision()
    );
    let foreign = foreign_registry.begin_session(
        &foreign_registered,
        SessionId::new(SourceIdentifier::try_from("kraken-session-successor")?),
        ConnectionGeneration::new(1)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let foreign_capture = foreign_registry.take_capture_generation_capabilities(&foreign)?;
    let (mut foreign_initialization, _foreign_admission, _foreign_degradation) =
        foreign_capture.into_parts();
    foreign_initialization.mark_healthy()?;
    assert!(matches!(
        foreign_registry.take_live_source_generation(&successor),
        Err(RegistryError::HandleTransplanted)
    ));
    Ok(())
}

#[tokio::test]
async fn source_uses_the_session_budget_and_cannot_run_twice() -> TestResult {
    let (config, mut registry, registered) =
        test_source("kraken-public-book-v2", "kraken-policy-v1")?;
    let session = registry.begin_session(
        &registered,
        SessionId::new(SourceIdentifier::try_from("kraken-session-single-run")?),
        ConnectionGeneration::new(1)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let expected_budget = session
        .budget()
        .cloned()
        .ok_or("source session has no coordinated budget")?;
    let generation = live_generation(&mut registry, &session)?;
    let mut source = KrakenSource::try_new(config, generation)?;
    assert!(source.budget.shares_allocation_with(&expected_budget));

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let mut sink = RecordingSink::default();
    assert_eq!(
        source.run(&mut sink, cancellation.clone()).await,
        Err(SourceError::Cancelled)
    );
    assert_eq!(
        source.run(&mut sink, cancellation).await,
        Err(SourceError::InvalidProtocolState)
    );
    Ok(())
}

fn live_generation(
    registry: &mut AuthoritativeSourceRegistry,
    session: &market_squawk_sources::CurrentSourceSession,
) -> TestResult<LiveSourceGeneration> {
    let capture = registry.take_capture_generation_capabilities(session)?;
    let (mut initialization, _admission, _degradation) = capture.into_parts();
    initialization.mark_healthy()?;
    Ok(registry.take_live_source_generation(session)?)
}

fn test_source(
    source_id: &str,
    metadata_revision: &str,
) -> TestResult<(
    KrakenConfig,
    AuthoritativeSourceRegistry,
    market_squawk_sources::RegisteredSource,
)> {
    let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
    let exact = |byte| {
        ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            [byte; 32],
        ))
    };
    let provider = SourceIdentifier::try_from("kraken")?;
    let authorization = AuthorizationGrant::new(
        AuthorizationMode::PublicInterface,
        AuthorizationBasis::new(SourceIdentifier::try_from("kraken-terms-reviewed")?),
        exact(2),
        effective,
    );
    let budget = ProviderBudgetPolicy::try_new(
        BudgetScope::new(provider),
        NonZeroU32::new(20).ok_or("zero request budget")?,
        NonZeroU64::new(1_000_000_000).ok_or("zero budget window")?,
        NonZeroU16::new(3).ok_or("zero concurrency")?,
        BackoffPolicy::try_new(
            NonZeroU64::new(10_000_000).ok_or("zero initial backoff")?,
            NonZeroU64::new(1_000_000_000).ok_or("zero maximum backoff")?,
            1_000,
        )?,
    )?;
    let instrument = InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?;
    let metadata = KrakenMetadataInput::new(
        SourceId::try_from(source_id)?,
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(SourceIdentifier::try_from(metadata_revision)?),
            exact(1),
        ),
        authorization,
        exact(3),
        effective,
        instrument,
        FreshnessPolicy::try_new(
            25_000_000,
            1_000_000_000,
            2_000_000_000,
            1_000_000_000,
            100_000_000,
        )?,
        budget,
    )
    .try_build()?;
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let registered = registry.register(metadata.clone(), Timestamp::from_unix_nanos(1))?;
    let config = KrakenConfig::try_new(
        metadata,
        "BTC/USD",
        instrument,
        KrakenDepth::Ten,
        NonZeroUsize::new(1 << 20).ok_or("zero frame bound")?,
    )?;
    Ok((config, registry, registered))
}
