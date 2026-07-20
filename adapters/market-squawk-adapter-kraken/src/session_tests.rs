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
    AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationMode, BackoffPolicy, BudgetScope,
    DecodeOutcome, FreshnessPolicy, MarketDecoder, ProviderBudgetPolicy, RawMarketFrame,
    RawMarketSink, SessionId, SinkError, SourceError, SourceMetadataProvider,
};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
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

#[tokio::test]
async fn source_captures_and_decodes_then_quarantines_on_metadata_idle() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (release_tx, release_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
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
        socket
            .send(Message::Text(
                r#"{"method":"subscribe","result":{"channel":"book","depth":10,"snapshot":true,"symbol":"BTC/USD","warnings":["advisory"]},"success":true,"time_in":"2023-10-04T07:48:25Z","time_out":"2023-10-04T07:48:25Z"}"#
                    .into(),
            ))
            .await?;
        socket
            .send(Message::Text(
                include_str!("../fixtures/official_book_checksum.json").into(),
            ))
            .await?;
        let _released = release_rx.await;
        let _closed = socket.close(None).await;
        TestResult::Ok(())
    });

    let (config, mut registry, registered) = test_source()?;
    let session = registry.begin_session(
        &registered,
        SessionId::new(SourceIdentifier::try_from("kraken-session-test")?),
        ConnectionGeneration::new(1)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let mut frames = registry.take_raw_frame_factory(&session)?;
    let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}")).await?;
    let mut source = KrakenSource::new(config);
    let mut sink = RecordingSink::default();

    let result = source
        .run_established(
            &mut socket,
            &mut frames,
            &mut sink,
            CancellationToken::new(),
        )
        .await;

    assert_eq!(result, Err(SourceError::ConnectionIdle));
    assert_eq!(source.health().state(), KrakenDecoderState::Quarantined);
    assert_eq!(source.health().captured_frames(), 2);
    assert_eq!(source.health().control_messages(), 1);
    assert_eq!(source.health().market_messages(), 1);
    assert!(source.health().book_subscribed());
    assert!(source.health().last_market_timestamp().is_some());
    assert_eq!(sink.frames.len(), 2);
    let validated_ack = session.validate_live_frame(&sink.frames[0])?;
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
    let validated_snapshot = session.validate_live_frame(&sink.frames[1])?;
    assert!(matches!(
        bridge.decode(&validated_snapshot)?,
        DecodeOutcome::Data(_)
    ));

    let _release_result = release_tx.send(());
    server.await??;
    Ok(())
}

fn test_source() -> TestResult<(
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
        SourceId::try_from("kraken-public-book-v2")?,
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(SourceIdentifier::try_from("kraken-policy-v1")?),
            exact(1),
        ),
        AuthorizationGrant::new(
            AuthorizationMode::PublicInterface,
            AuthorizationBasis::new(SourceIdentifier::try_from("kraken-terms-reviewed")?),
            exact(2),
            effective,
        ),
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
    let source_budget = registered
        .budget()
        .cloned()
        .ok_or("source registration has no budget")?;
    let config = KrakenConfig::try_new(
        metadata,
        source_budget,
        "BTC/USD",
        instrument,
        KrakenDepth::Ten,
        NonZeroUsize::new(1 << 20).ok_or("zero frame bound")?,
    )?;
    Ok((config, registry, registered))
}
