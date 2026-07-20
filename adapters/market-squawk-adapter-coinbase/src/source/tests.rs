use std::error::Error;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::str::FromStr;
use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use market_squawk_domain::{
    AuthorizationBasis, ConnectionGeneration, DigestAlgorithm, EffectiveInterval, EvidenceDigest,
    ExactPayloadEvidence, InstrumentId, MetadataRevision, ProviderProduct,
    RevisionBoundPayloadEvidence, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationMode, BackoffPolicy, BudgetScope,
    DecodeOutcome, FreshnessPolicy, MarketDecoder, ProviderBudgetPolicy, RawMarketFrame,
    RawMarketSink, SessionId, SinkError, SourceError,
};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, client_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;

use super::CoinbaseExchangeSource;
use crate::{
    CoinbaseChannel, CoinbaseExchangeConfig, CoinbaseExchangeDecoder, CoinbaseProductMapping,
    CoinbaseTransportLimits,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[derive(Debug, Default)]
struct RecordingSink {
    frames: Vec<RawMarketFrame>,
}

impl RawMarketSink for RecordingSink {
    fn try_publish(&mut self, frame: RawMarketFrame) -> Result<(), SinkError> {
        self.frames.push(frame);
        Ok(())
    }
}

#[tokio::test]
async fn one_generation_subscribes_captures_controls_and_returns_typed_close() -> TestResult {
    let config = config()?;
    let (mut registry, session) = session(&config, "source-local-1")?;
    let mut frames = registry.take_raw_frame_factory(&session)?;
    let mut source = CoinbaseExchangeSource::try_new(config.clone(), &session)?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let mut socket = accept_async(stream).await?;
        let subscription = socket
            .next()
            .await
            .ok_or("subscription was not sent")??
            .into_text()?;
        assert_eq!(
            subscription,
            r#"{"type":"subscribe","product_ids":["BTC-USD"],"channels":["level2","matches","heartbeat"]}"#
        );
        socket
            .send(Message::Text(
                include_str!("../../fixtures/subscriptions.json")
                    .trim()
                    .into(),
            ))
            .await?;
        socket
            .send(Message::Text(
                include_str!("../../fixtures/snapshot.json").trim().into(),
            ))
            .await?;
        socket
            .send(Message::Ping(Bytes::from_static(b"probe")))
            .await?;
        assert!(matches!(
            socket.next().await,
            Some(Ok(Message::Pong(payload))) if payload == Bytes::from_static(b"probe")
        ));
        socket.send(Message::Close(None)).await?;
        let _client_close = socket.next().await;
        Ok::<(), Box<dyn Error + Send + Sync>>(())
    });
    let stream = TcpStream::connect(address).await?;
    let (socket, _) = client_async(format!("ws://{address}"), stream).await?;
    let mut sink = RecordingSink::default();
    let outcome = source
        .run_with_socket_for_test(socket, &mut frames, &mut sink, CancellationToken::new())
        .await;
    assert_eq!(outcome, Err(SourceError::ProviderUnavailable));
    server
        .await?
        .map_err(|error| std::io::Error::other(error.to_string()))?;

    assert_eq!(sink.frames.len(), 2);
    assert_eq!(
        sink.frames[0].payload(),
        include_str!("../../fixtures/subscriptions.json")
            .trim()
            .as_bytes()
    );
    assert_eq!(
        sink.frames[1].payload(),
        include_str!("../../fixtures/snapshot.json")
            .trim()
            .as_bytes()
    );
    let mut decoder = CoinbaseExchangeDecoder::try_new(&config)?;
    assert!(matches!(
        decoder.decode(&session.validate_live_frame(&sink.frames[0])?)?,
        DecodeOutcome::Control(_)
    ));
    assert!(matches!(
        decoder.decode(&session.validate_live_frame(&sink.frames[1])?)?,
        DecodeOutcome::Data(_)
    ));
    Ok(())
}

#[tokio::test]
async fn cancellation_preempts_read_and_source_refuses_same_generation_restart() -> TestResult {
    let config = config()?;
    let (mut registry, session) = session(&config, "source-local-2")?;
    let mut frames = registry.take_raw_frame_factory(&session)?;
    let mut source = CoinbaseExchangeSource::try_new(config, &session)?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let mut socket = accept_async(stream).await?;
        let _subscription = socket.next().await;
        std::future::pending::<Result<(), Box<dyn Error + Send + Sync>>>().await
    });
    let stream = TcpStream::connect(address).await?;
    let (socket, _) = client_async(format!("ws://{address}"), stream).await?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let outcome = tokio::time::timeout(
        Duration::from_secs(1),
        source.run_with_socket_for_test(
            socket,
            &mut frames,
            &mut RecordingSink::default(),
            cancellation,
        ),
    )
    .await?;
    assert_eq!(outcome, Err(SourceError::Cancelled));
    assert_eq!(
        source.begin_generation_for_test(),
        Err(SourceError::InvalidProtocolState)
    );
    server.abort();
    let _server_result = server.await;
    Ok(())
}

fn session(
    config: &CoinbaseExchangeConfig,
    session_id: &str,
) -> TestResult<(
    AuthoritativeSourceRegistry,
    market_squawk_sources::CurrentSourceSession,
)> {
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let registered = registry.register(config.metadata().clone(), Timestamp::from_unix_nanos(1))?;
    let session = registry.begin_session(
        &registered,
        SessionId::new(identifier(session_id)?),
        ConnectionGeneration::new(1)?,
        Timestamp::from_unix_nanos(1),
    )?;
    Ok((registry, session))
}

fn config() -> TestResult<CoinbaseExchangeConfig> {
    let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
    let authorization = AuthorizationGrant::new(
        AuthorizationMode::PublicInterface,
        AuthorizationBasis::new(identifier("coinbase-public-interface-v1")?),
        evidence(2),
        effective,
    );
    let budget = ProviderBudgetPolicy::try_new(
        BudgetScope::for_authorization(identifier("coinbase-exchange")?, &authorization)?,
        NonZeroU32::new(8).ok_or("request budget must be nonzero")?,
        NonZeroU64::new(1_000_000_000).ok_or("budget window must be nonzero")?,
        NonZeroU16::new(1).ok_or("budget concurrency must be nonzero")?,
        BackoffPolicy::try_new(
            NonZeroU64::new(1_000_000).ok_or("initial backoff must be nonzero")?,
            NonZeroU64::new(1_000_000_000).ok_or("maximum backoff must be nonzero")?,
            1_000,
        )?,
    )?;
    CoinbaseExchangeConfig::try_new(
        SourceId::try_from("coinbase-exchange-public")?,
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(identifier("exchange-v1-2026-07-20")?),
            evidence(3),
        ),
        authorization,
        evidence(4),
        effective,
        vec![CoinbaseProductMapping::try_new(
            ProviderProduct::new(identifier("BTC-USD")?),
            InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?,
        )?],
        vec![
            CoinbaseChannel::Level2,
            CoinbaseChannel::Matches,
            CoinbaseChannel::Heartbeat,
        ],
        FreshnessPolicy::try_new(
            5_000_000_000,
            1_000_000_000,
            2_000_000_000,
            1_000_000_000,
            100_000_000,
        )?,
        budget,
        CoinbaseTransportLimits::try_new(
            256 * 1024,
            Duration::from_secs(5),
            Duration::from_secs(5),
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
