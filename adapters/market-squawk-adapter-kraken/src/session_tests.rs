use std::error::Error;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64, NonZeroUsize};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use market_squawk_domain::{
    AuthorizationBasis, ConnectionGeneration, Currency, Denomination, DigestAlgorithm,
    EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, InstrumentDefinition,
    InstrumentDefinitionInput, InstrumentDefinitionRevision, InstrumentId, LotSize,
    MetadataRevision, ProviderIdentityEvidence, ProviderIdentityRecord,
    ProviderIdentityRecordInput, ProviderInstrumentId, RevisionBoundPayloadEvidence, SourceId,
    SourceIdentifier, TickSize, Timestamp, TradingStatus, VenueId, VenueMapping, VenueSymbol,
};
use market_squawk_sources::{
    ActiveLiveSourceGeneration, AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationMode,
    AuthorizationSubjectResolutionError, AuthorizationSubjectResolver, BackoffPolicy,
    BudgetReservationDecision, BudgetScope, CurrentSourceSession, DecodeOutcome, FreshnessPolicy,
    LiveMarketSource, LiveSourceGeneration, ProviderBudgetPolicy, ProviderChecksumEvidence,
    RawMarketFrame, RawMarketSink, RegistryError, SessionId, SinkError, SourceError,
    SourceMetadata, SourceMetadataProvider, TransportFrameKind,
};
use rust_decimal::Decimal;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use super::{
    KrakenDecoderState, KrakenSocketDecodeControl, KrakenSocketHandoffConsumer, KrakenSource,
    send_subscription,
};
use crate::{
    KRAKEN_L3_WEBSOCKET_ENDPOINT, KrakenAuthenticatedDiscontinuity, KrakenBookTransition,
    KrakenChecksumAvailability, KrakenConfig, KrakenControlOrDiscontinuityKind, KrakenDepth,
    KrakenL3ClientTier, KrakenL3Config, KrakenL3CredentialAuthority, KrakenL3Decoder,
    KrakenL3DecoderState, KrakenL3Depth, KrakenL3EstablishedSessionSender, KrakenL3MetadataInput,
    KrakenL3ProductMapping, KrakenL3SubscriptionDispatch, KrakenMarketContinuity,
    KrakenMarketDecodeHandoff, KrakenMarketEventHandoff, KrakenMetadataInput,
    KrakenSubscriptionRequestEvidence,
};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Debug)]
struct RecordingSink<'a> {
    frames: Vec<RawMarketFrame>,
    limit: usize,
    terminal_after_capture: bool,
    session: Option<&'a CurrentSourceSession>,
    decoder: Option<KrakenSocketHandoffConsumer>,
}

impl Default for RecordingSink<'_> {
    fn default() -> Self {
        Self {
            frames: Vec::with_capacity(2),
            limit: 2,
            terminal_after_capture: false,
            session: None,
            decoder: None,
        }
    }
}

impl RawMarketSink for RecordingSink<'_> {
    fn try_publish(&mut self, frame: RawMarketFrame) -> Result<(), SinkError> {
        if self.frames.len() == self.limit {
            return Err(SinkError::Saturated);
        }
        self.frames.push(frame.clone());
        if self.terminal_after_capture {
            return Err(SinkError::CaptureIncomplete);
        }
        if let Some(decoder) = &mut self.decoder {
            let session = self.session.ok_or(SinkError::CaptureIncomplete)?;
            let validated = session
                .validate_live_frame(&frame)
                .map_err(|_| SinkError::CaptureIncomplete)?;
            decoder
                .consume(&validated)
                .map_err(|_| SinkError::CaptureIncomplete)?;
        }
        Ok(())
    }
}

const SUBSCRIPTION_REFUSAL: &str = r#"{"method":"subscribe","success":false,"error":"rate limit exceeded","time_in":"2023-10-04T07:48:25Z","time_out":"2023-10-04T07:48:25.010Z","req_id":1}"#;
const PUBLIC_BOOK_ACK: &str = r#"{"method":"subscribe","result":{"channel":"book","depth":10,"snapshot":true,"symbol":"BTC/USD"},"success":true,"time_in":"2023-10-04T07:48:25Z","time_out":"2023-10-04T07:48:25.010Z","req_id":1}"#;
const PUBLIC_RESET: &str = r#"{"channel":"status","type":"update","data":[{"system":"maintenance","api_version":"v2","connection_id":42,"version":"2.0.0"}]}"#;
const LEVEL3_ACK: &str = r#"{"method":"subscribe","result":{"channel":"level3","depth":10,"snapshot":true,"symbol":"BTC/USD"},"success":true,"time_in":"2024-01-08T12:26:45.900000000Z","time_out":"2024-01-08T12:26:45.910000000Z","req_id":7}"#;
const LEVEL3_INVALID: &str = r#"{"channel":"level3","type":"update","data":[{"symbol":"BTC/USD","timestamp":"2024-01-08T12:26:46.600000000Z","checksum":1,"bids":[{"event":"modify","order_id":"OJPMIN-NXZL5-SOWP6V","limit_price":"44937.1","order_qty":"0.01000000","timestamp":"2024-01-08T12:26:46.500000000Z"}]}]}"#;

#[tokio::test]
async fn captured_public_and_level3_handoffs_preserve_identity_continuity_and_atomic_recovery()
-> TestResult {
    assert_eq!(KRAKEN_L3_WEBSOCKET_ENDPOINT, "wss://ws-l3.kraken.com/v2");
    let instrument = InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?;
    let public_snapshot = include_bytes!("../fixtures/official_book_checksum.json");
    let level3_snapshot = include_bytes!("../fixtures/official_level3_checksum.json");
    let mut recovery_snapshot: serde_json::Value = serde_json::from_slice(level3_snapshot)?;
    recovery_snapshot["data"][0]["timestamp"] =
        serde_json::Value::String("2024-01-08T12:26:47.000000000Z".to_owned());
    let recovery_snapshot = serde_json::to_vec(&recovery_snapshot)?;

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server_recovery = recovery_snapshot.clone();
    let server = tokio::spawn(async move {
        let mut public_socket = accept_subscription(&listener).await?;
        public_socket
            .send(Message::Text(PUBLIC_BOOK_ACK.into()))
            .await?;
        public_socket
            .send(Message::Text(std::str::from_utf8(public_snapshot)?.into()))
            .await?;
        public_socket
            .send(Message::Text(PUBLIC_RESET.into()))
            .await?;

        let mut level3_socket = accept_subscription(&listener).await?;
        level3_socket.send(Message::Text(LEVEL3_ACK.into())).await?;
        level3_socket
            .send(Message::Text(std::str::from_utf8(level3_snapshot)?.into()))
            .await?;
        level3_socket
            .send(Message::Text(LEVEL3_INVALID.into()))
            .await?;
        level3_socket
            .send(Message::Text(String::from_utf8(server_recovery)?.into()))
            .await?;
        TestResult::Ok(())
    });

    let (public_config, mut public_registry, public_registered) =
        test_source("kraken-public-book-v2", "kraken-policy-v1")?;
    let public_session = public_registry.begin_session(
        &public_registered,
        SessionId::new(SourceIdentifier::try_from("kraken-public-critical")?),
        ConnectionGeneration::new(1)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let public_generation = live_generation(&mut public_registry, &public_session)?;
    let mut public_authority = public_generation.try_start(public_config.metadata())?;
    let public_budget = public_authority
        .budget()?
        .cloned()
        .ok_or("public session has no coordinated budget")?;
    let (public_decode_control, mut public_handoff_consumer) =
        KrakenSocketHandoffConsumer::channel(&public_config, public_budget.clone())?;
    let (mut public_socket, _) =
        tokio_tungstenite::connect_async(format!("ws://{address}")).await?;
    let public_request = public_config.try_subscription_request(public_authority.generation())?;
    let (public_permit, public_written) = send_subscription(
        &mut public_socket,
        &mut public_authority,
        &public_budget,
        public_request,
        &CancellationToken::new(),
        Duration::from_secs(1),
    )
    .await?;
    public_decode_control.install_subscription(public_permit, public_written)?;
    let (acknowledgement, terminal) = decode_public_frame_through_socket_handoff(
        &mut public_authority,
        receive_text(&mut public_socket).await?,
        &mut public_handoff_consumer,
        &public_decode_control,
    )?;
    let (acknowledgement, acknowledgement_publication) = acknowledgement.into_parts();
    assert!(matches!(
        acknowledgement,
        DecodeOutcome::Control(control)
            if control.kind() == market_squawk_sources::ControlFrameKind::SubscriptionAcknowledgement
    ));
    assert!(acknowledgement_publication.is_some());
    assert!(terminal.is_none());
    assert!(public_decode_control.health().book_subscribed());

    let (public_snapshot_handoff, terminal) = decode_public_frame_through_socket_handoff(
        &mut public_authority,
        receive_text(&mut public_socket).await?,
        &mut public_handoff_consumer,
        &public_decode_control,
    )?;
    let (public_snapshot, publication) = public_snapshot_handoff.into_parts();
    let DecodeOutcome::Data(public_snapshot) = public_snapshot else {
        return Err("public book escaped the capture-owned decoder".into());
    };
    assert!(public_snapshot.observations().iter().all(|observation| {
        matches!(
            observation.checksum(),
            ProviderChecksumEvidence::Provided { value, .. }
                if value.as_str() == "3310070434"
        )
    }));
    let publication = publication.ok_or("public book lost its publication lineage")?;
    assert_eq!(
        publication.native_coordinates(),
        public_config.native_coordinates()
    );
    assert!(terminal.is_none());
    assert_eq!(public_decode_control.health().market_messages(), 1);

    let (reset, terminal) = decode_public_frame_through_socket_handoff(
        &mut public_authority,
        receive_text(&mut public_socket).await?,
        &mut public_handoff_consumer,
        &public_decode_control,
    )?;
    let (reset, reset_publication) = reset.into_parts();
    assert!(matches!(
        reset,
        DecodeOutcome::Resynchronize(recovery)
            if recovery.reason()
                == market_squawk_sources::ResynchronizationReason::ProviderRequestedReset
    ));
    assert!(reset_publication.is_some());
    assert_eq!(terminal, Some(SourceError::InvalidProtocolState));
    assert_eq!(
        public_decode_control.health().state(),
        KrakenDecoderState::Retired
    );

    let credential_record = SourceIdentifier::try_from("kraken-read-only-market-data-account")?;
    let credential_authority = KrakenL3CredentialAuthority::new(
        credential_record.clone(),
        NonZeroU64::new(3).ok_or("zero authorization generation")?,
    );
    let level3_config = KrakenL3Config::try_new(
        level3_metadata(instrument, credential_record.clone())?,
        vec![KrakenL3ProductMapping::try_new("BTC/USD", instrument)?],
        KrakenL3Depth::Ten,
        KrakenL3ClientTier::Standard,
        &credential_authority,
        NonZeroUsize::new(1 << 20).ok_or("zero frame bound")?,
    )?;
    let foreign_authority = KrakenL3CredentialAuthority::new(
        credential_record,
        NonZeroU64::new(3).ok_or("zero authorization generation")?,
    );
    assert!(matches!(
        level3_config.try_subscription_payload(
            foreign_authority
                .try_mint_subscription_capability("fixture-foreign-token".to_owned())?,
            0,
            Some(7),
        ),
        Err(crate::KrakenL3ConfigError::CredentialAuthorityMismatch)
    ));

    let mut level3_registry =
        AuthoritativeSourceRegistry::try_new_ephemeral_with_authorization_subject_resolver_for_diagnostics(
            Arc::new(FixtureAuthorizationSubject),
        )?;
    let level3_registered = level3_registry.register(
        level3_config.metadata().clone(),
        Timestamp::from_unix_nanos(1),
    )?;
    let level3_session = level3_registry.begin_session(
        &level3_registered,
        SessionId::new(SourceIdentifier::try_from("kraken-level3-critical")?),
        ConnectionGeneration::new(7)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let level3_generation = live_generation(&mut level3_registry, &level3_session)?;
    let mut level3_authority = level3_generation.try_start(level3_config.metadata())?;
    let level3_budget = level3_authority
        .budget()?
        .cloned()
        .ok_or("level-3 session has no coordinated budget")?;
    let (mut level3_socket, _) =
        tokio_tungstenite::connect_async(format!("ws://{address}")).await?;
    let level3_payload = level3_config.try_subscription_payload(
        credential_authority
            .try_mint_subscription_capability("fixture-ephemeral-token".to_owned())?,
        0,
        Some(7),
    )?;
    let mut level3_dispatch = KrakenL3EstablishedSessionSender::try_new(
        &mut level3_authority,
        &mut level3_socket,
        &level3_budget,
    )?
    .send_subscription(
        level3_payload,
        &CancellationToken::new(),
        Duration::from_secs(1),
    )
    .await?;
    let mut level3_decoder = KrakenL3Decoder::try_new(&level3_config)?;
    let level3_ack = receive_text(&mut level3_socket).await?;
    decode_level3_frame(
        &mut level3_authority,
        &mut level3_decoder,
        level3_ack,
        Some(&mut level3_dispatch),
    )?;
    assert!(level3_dispatch.is_settled());
    let level3_snapshot_handoff = decode_level3_frame(
        &mut level3_authority,
        &mut level3_decoder,
        receive_text(&mut level3_socket).await?,
        None,
    )?;
    let KrakenMarketEventHandoff::AuthenticatedLevel3(level3_snapshot_handoff) =
        level3_snapshot_handoff
    else {
        return Err("level-3 book escaped the authenticated market handoff".into());
    };
    let Some(KrakenSubscriptionRequestEvidence::AuthenticatedSecretBearing { request_evidence }) =
        level3_snapshot_handoff.connection().subscription_request()
    else {
        return Err("protected credential authority was absent from the L3 handoff".into());
    };
    assert_eq!(
        request_evidence.credential_record_id(),
        credential_authority.credential_record_id()
    );
    assert_eq!(
        request_evidence.authorization_generation(),
        credential_authority.authorization_generation()
    );
    assert!(matches!(
        level3_snapshot_handoff.continuity(),
        KrakenMarketContinuity::AuthenticatedLevel3 {
            transition: KrakenBookTransition::Snapshot,
            checksum: KrakenChecksumAvailability::Validated(1_063_832_831),
            local_generation_ordinal: 1,
            ..
        }
    ));
    let invalid = decode_level3_frame(
        &mut level3_authority,
        &mut level3_decoder,
        receive_text(&mut level3_socket).await?,
        None,
    )?;
    assert!(matches!(
        disposition_kind(&invalid)?,
        KrakenControlOrDiscontinuityKind::AuthenticatedDiscontinuity(
            KrakenAuthenticatedDiscontinuity::Decode {
                error: crate::KrakenL3DecodeError::ChecksumMismatch { expected: 1, .. },
                ..
            }
        )
    ));
    assert_eq!(
        level3_decoder.state("BTC/USD"),
        Some(KrakenL3DecoderState::Quarantined)
    );
    decode_level3_frame(
        &mut level3_authority,
        &mut level3_decoder,
        receive_text(&mut level3_socket).await?,
        None,
    )?;
    assert_eq!(
        level3_decoder.state("BTC/USD"),
        Some(KrakenL3DecoderState::Healthy)
    );
    server.await??;
    Ok(())
}

async fn accept_subscription(listener: &TcpListener) -> TestResult<WebSocketStream<TcpStream>> {
    let (stream, _) = listener.accept().await?;
    let mut socket = tokio_tungstenite::accept_async(stream).await?;
    let Some(Ok(Message::Text(subscription))) =
        tokio::time::timeout(Duration::from_secs(1), socket.next()).await?
    else {
        return Err("session did not send a text subscription".into());
    };
    let request: FixtureSubscription<'_> = serde_json::from_str(&subscription)?;
    if request.method != "subscribe" {
        return Err("session sent a non-subscription request".into());
    }
    match request.params.channel {
        "book" if request.request_id == 1 && request.params.token.is_none() => {}
        "level3"
            if request.request_id == 7
                && request.params.token == Some("fixture-ephemeral-token") => {}
        _ => return Err("session sent the wrong subscription contract".into()),
    }
    Ok(socket)
}

#[derive(serde::Deserialize)]
struct FixtureSubscription<'a> {
    method: &'a str,
    params: FixtureSubscriptionParams<'a>,
    #[serde(rename = "req_id")]
    request_id: u64,
}

#[derive(serde::Deserialize)]
struct FixtureSubscriptionParams<'a> {
    channel: &'a str,
    #[serde(default)]
    token: Option<&'a str>,
}

async fn receive_text<S>(socket: &mut WebSocketStream<S>) -> TestResult<Bytes>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let Some(message) = socket.next().await else {
        return Err("provider connection ended before the expected frame".into());
    };
    let Message::Text(text) = message? else {
        return Err("provider sent a non-text fixture frame".into());
    };
    Ok(Bytes::copy_from_slice(text.as_bytes()))
}

fn decode_public_frame_through_socket_handoff(
    authority: &mut ActiveLiveSourceGeneration,
    payload: Bytes,
    consumer: &mut KrakenSocketHandoffConsumer,
    control: &KrakenSocketDecodeControl,
) -> TestResult<(KrakenMarketDecodeHandoff, Option<SourceError>)> {
    let frame = authority
        .frames_mut()?
        .try_frame(TransportFrameKind::Text, payload)?;
    let frame_id = frame.frame_id();
    let validated = authority.validate_live_frame(&frame)?;
    let handoff = consumer.consume(&validated)?;
    Ok((handoff, control.finish_frame(frame_id)?))
}

fn decode_level3_frame(
    authority: &mut ActiveLiveSourceGeneration,
    decoder: &mut KrakenL3Decoder,
    payload: Bytes,
    mut dispatch: Option<&mut KrakenL3SubscriptionDispatch>,
) -> TestResult<KrakenMarketEventHandoff> {
    let frame = authority
        .frames_mut()?
        .try_frame(TransportFrameKind::Text, payload)?;
    let validated = authority.validate_live_frame(&frame)?;
    if let Some(dispatch) = dispatch.as_deref_mut() {
        if let Some(sent) = dispatch.bind_to_frame(&validated)? {
            decoder.register_sent_subscription(sent)?;
        }
    }
    let handoff = decoder.decode_captured(&validated)?;
    if let Some(dispatch) = dispatch {
        if let KrakenMarketEventHandoff::ControlOrDiscontinuity(control) = &handoff {
            if let KrakenControlOrDiscontinuityKind::AuthenticatedControl(control) = control.kind()
            {
                dispatch.apply_control(control)?;
            }
        }
    }
    Ok(handoff)
}

fn disposition_kind(
    handoff: &KrakenMarketEventHandoff,
) -> TestResult<&KrakenControlOrDiscontinuityKind> {
    let KrakenMarketEventHandoff::ControlOrDiscontinuity(handoff) = handoff else {
        return Err("expected a Kraken control or discontinuity handoff".into());
    };
    Ok(handoff.kind())
}

#[derive(Debug)]
struct FixtureAuthorizationSubject;

impl AuthorizationSubjectResolver for FixtureAuthorizationSubject {
    fn resolve_subject_record(
        &self,
        mode: AuthorizationMode,
        _evidence: EvidenceDigest,
    ) -> Result<SourceIdentifier, AuthorizationSubjectResolutionError> {
        if mode != AuthorizationMode::UserAuthorized {
            return Err(AuthorizationSubjectResolutionError::UnsupportedMode);
        }
        SourceIdentifier::try_from("kraken-read-only-market-data-account")
            .map_err(|_| AuthorizationSubjectResolutionError::EvidenceUnresolved)
    }
}

fn level3_metadata(
    instrument: InstrumentId,
    credential_record: SourceIdentifier,
) -> TestResult<SourceMetadata> {
    let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
    let authorization = AuthorizationGrant::new(
        AuthorizationMode::UserAuthorized,
        AuthorizationBasis::new(credential_record.clone()),
        exact_evidence(2),
        effective,
    );
    let budget = ProviderBudgetPolicy::try_new(
        BudgetScope::with_authorization_account(
            SourceIdentifier::try_from("kraken")?,
            credential_record,
        ),
        NonZeroU32::new(1).ok_or("zero request budget")?,
        NonZeroU64::new(1_000_000_000).ok_or("zero budget window")?,
        NonZeroU16::new(1).ok_or("zero concurrency")?,
        BackoffPolicy::try_new(
            NonZeroU64::new(100_000_000).ok_or("zero initial backoff")?,
            NonZeroU64::new(30_000_000_000).ok_or("zero maximum backoff")?,
            1_000,
        )?,
    )?;
    Ok(KrakenL3MetadataInput::new(
        SourceId::try_from("kraken-authenticated-level3-v2")?,
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(SourceIdentifier::try_from("kraken-level3-policy-v1")?),
            exact_evidence(1),
        ),
        authorization,
        exact_evidence(3),
        effective,
        vec![instrument],
        FreshnessPolicy::try_new(
            5_000_000_000,
            1_000_000_000,
            2_000_000_000,
            1_000_000_000,
            100_000_000,
        )?,
        budget,
    )
    .try_build()?)
}

fn exact_evidence(byte: u8) -> ExactPayloadEvidence {
    ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        [byte; 32],
    ))
}

#[tokio::test]
async fn sink_admission_precedes_decode_and_terminal_controls_are_counted() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let mut socket = accept_book_source(&listener).await?;
            socket
                .send(Message::Text(SUBSCRIPTION_REFUSAL.into()))
                .await?;
            socket.close(None).await?;
        }
        TestResult::Ok(())
    });

    let endpoint = format!("ws://{address}");
    let (first_config, mut first_registry, first_registered) =
        test_source("kraken-public-book-v2", "kraken-policy-v1")?;
    let first_config = first_config.with_local_endpoint_for_test(&endpoint)?;
    let first_session = first_registry.begin_session(
        &first_registered,
        SessionId::new(SourceIdentifier::try_from("kraken-sink-rejected")?),
        ConnectionGeneration::new(1)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let first_generation = live_generation(&mut first_registry, &first_session)?;
    let (mut first_source, first_decoder) =
        KrakenSource::try_new_with_publication_handoff(first_config, first_generation)?;
    let mut first_sink = RecordingSink {
        terminal_after_capture: true,
        session: Some(&first_session),
        decoder: Some(first_decoder),
        ..RecordingSink::default()
    };

    let first_result = first_source
        .run(&mut first_sink, CancellationToken::new())
        .await;

    assert_eq!(
        first_result,
        Err(SourceError::Sink(SinkError::CaptureIncomplete))
    );
    assert_eq!(first_source.health().captured_frames(), 0);
    assert_eq!(first_source.health().control_messages(), 0);
    assert_eq!(first_source.health().market_messages(), 0);
    assert!(!first_source.health().book_subscribed());

    let (config, mut registry, registered) =
        test_source("kraken-public-book-v2", "kraken-policy-v1")?;
    let config = config.with_local_endpoint_for_test(&endpoint)?;
    let session = registry.begin_session(
        &registered,
        SessionId::new(SourceIdentifier::try_from("kraken-refusal-admitted")?),
        ConnectionGeneration::new(1)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let generation = live_generation(&mut registry, &session)?;
    let budget = session
        .budget()
        .cloned()
        .ok_or("source session has no coordinated budget")?;
    let (mut source, decoder) = KrakenSource::try_new_with_publication_handoff(config, generation)?;
    let mut sink = RecordingSink {
        session: Some(&session),
        decoder: Some(decoder),
        ..RecordingSink::default()
    };

    let result = source.run(&mut sink, CancellationToken::new()).await;

    let refusal_deadline = match result {
        Err(SourceError::BudgetWaitUntil { deadline }) => deadline,
        result => return Err(format!("provider refusal settled as {result:?}").into()),
    };
    assert_eq!(source.health().state(), KrakenDecoderState::Retired);
    assert_eq!(source.health().captured_frames(), 1);
    assert_eq!(source.health().control_messages(), 1);
    assert!(matches!(
        budget.try_reserve_request(),
        BudgetReservationDecision::WaitUntil(recorded_deadline)
            if recorded_deadline == refusal_deadline
    ));

    server.await??;
    Ok(())
}

async fn accept_book_source(listener: &TcpListener) -> TestResult<WebSocketStream<TcpStream>> {
    let (stream, _) = listener.accept().await?;
    let mut socket = tokio_tungstenite::accept_async(stream).await?;
    let Some(Ok(Message::Text(subscription))) =
        tokio::time::timeout(Duration::from_secs(2), socket.next()).await?
    else {
        return Err("source did not send a text subscription".into());
    };
    let request: serde_json::Value = serde_json::from_str(&subscription)?;
    if request["method"] != "subscribe"
        || request["req_id"] != 1
        || request["params"]["channel"] != "book"
    {
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
        KrakenSource::try_new_with_publication_handoff(config.clone(), stale_generation),
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
    let (mut source, _decoder) =
        KrakenSource::try_new_with_publication_handoff(config, generation)?;
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
        NonZeroU32::new(1).ok_or("zero request budget")?,
        NonZeroU64::new(1_000_000_000).ok_or("zero budget window")?,
        NonZeroU16::new(1).ok_or("zero concurrency")?,
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
    let definition = public_definition(instrument)?;
    let provider_identity_key = definition.provider_identities()[0].key();
    let config = KrakenConfig::try_new(
        metadata,
        &definition,
        &provider_identity_key,
        Timestamp::from_unix_nanos(1),
        KrakenDepth::Ten,
        NonZeroUsize::new(1 << 20).ok_or("zero frame bound")?,
    )?;
    Ok((config, registry, registered))
}

fn public_definition(instrument: InstrumentId) -> TestResult<InstrumentDefinition> {
    let usd = Currency::try_from("USD")?;
    let provider_identity = ProviderIdentityRecord::new(ProviderIdentityRecordInput {
        instrument_id: instrument,
        source_id: SourceId::try_from("kraken")?,
        provider_instrument_id: ProviderInstrumentId::try_from("XBTUSD")?,
        evidence: ProviderIdentityEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            [4; 32],
        )),
        source_timestamp: None,
        observed_at: Timestamp::from_unix_nanos(1),
        metadata_revision: MetadataRevision::new(SourceIdentifier::try_from(
            "kraken-instrument-identity-v1",
        )?),
        validity: EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?,
        supersedes: None,
    });
    Ok(InstrumentDefinition::try_new(InstrumentDefinitionInput {
        instrument_id: instrument,
        definition_revision: InstrumentDefinitionRevision::try_from(1)?,
        asset_class: market_squawk_domain::AssetClass::Crypto,
        primary_denomination: Denomination::Currency(usd),
        quote_currency: usd,
        tick_size: TickSize::try_from_decimal(Decimal::new(1, 2))?,
        lot_size: LotSize::try_from_decimal(Decimal::new(1, 8))?,
        contract_multiplier: Decimal::ONE,
        venue_mappings: vec![VenueMapping::new(
            VenueId::try_from("kraken")?,
            VenueSymbol::try_from("BTC/USD")?,
        )],
        provider_identities: vec![provider_identity],
        identifiers: Vec::new(),
        trading_status: TradingStatus::Active,
    })?)
}
