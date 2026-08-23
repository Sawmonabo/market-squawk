use std::error::Error;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64, NonZeroUsize};
use std::str::FromStr;

use market_squawk_adapter_kraken::{
    KrakenControl, KrakenDecodeOutcome, KrakenDecoder, KrakenDecoderState, KrakenDepth,
    KrakenL3BatchKind, KrakenL3ClientTier, KrakenL3Config, KrakenL3Control, KrakenL3DecodeOutcome,
    KrakenL3Decoder, KrakenL3DecoderState, KrakenL3Depth, KrakenL3MetadataInput,
    KrakenL3OrderEventKind, KrakenL3ProductMapping, KrakenL3WebSocketToken, KrakenSubscription,
};
use market_squawk_domain::{
    AuthorizationBasis, DataQuality, DigestAlgorithm, EffectiveInterval, EvidenceDigest,
    ExactPayloadEvidence, InstrumentId, LiveEventClass, MarketDepth, MetadataRevision,
    RevisionBoundPayloadEvidence, SourceId, SourceIdentifier, Timestamp, TradeTakerOrderType,
};
use market_squawk_sources::{
    AuthorizationGrant, AuthorizationMode, BackoffPolicy, BudgetScope, DecodeError,
    FreshnessPolicy, MAX_DECODED_EVENTS, ProviderBudgetPolicy, ProviderObservationPayload,
    ProviderSequenceEvidence,
};

type TestResult = Result<(), Box<dyn Error>>;

#[test]
fn official_snapshot_preserves_exact_lexemes_and_validates_checksum() -> TestResult {
    let instrument = InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?;
    let mut decoder = KrakenDecoder::try_new("BTC/USD", instrument, KrakenDepth::Ten)?;
    let payload = include_bytes!("../fixtures/official_book_checksum.json");

    let KrakenDecodeOutcome::Market(batch) = decoder.decode_payload(payload)? else {
        return Err("official fixture decoded as control traffic".into());
    };
    assert_eq!(decoder.state(), KrakenDecoderState::Healthy);
    assert_eq!(decoder.last_checksum(), Some(3_310_070_434));
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].event_class(), LiveEventClass::BookSnapshot);
    assert!(matches!(
        batch[0].sequence(),
        ProviderSequenceEvidence::Unsupported { .. }
    ));
    let ProviderObservationPayload::BookSnapshot(book) = batch[0].payload() else {
        return Err("snapshot decoded to the wrong payload".into());
    };
    assert_eq!(book.bids()[0].quantity().value().as_str(), "0.10000000");
    assert_eq!(book.asks()[0].price().value().as_str(), "45285.2");
    Ok(())
}

#[test]
fn subscription_ack_is_symbol_and_depth_exact() -> TestResult {
    let instrument = InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?;
    let mut decoder = KrakenDecoder::try_new("BTC/USD", instrument, KrakenDepth::Ten)?;
    let valid = br#"{"method":"subscribe","result":{"channel":"book","depth":10,"snapshot":true,"symbol":"BTC/USD"},"success":true,"time_in":"2023-10-04T07:48:25Z","time_out":"2023-10-04T07:48:25Z","req_id":1}"#;
    assert!(matches!(
        decoder.decode_payload(valid)?,
        KrakenDecodeOutcome::Control(KrakenControl::Subscribed(KrakenSubscription::Book))
    ));
    let warned = br#"{"method":"subscribe","result":{"channel":"book","depth":10,"snapshot":true,"symbol":"BTC/USD","warnings":["field will be deprecated"]},"success":true,"time_in":"2023-10-04T07:48:25Z","time_out":"2023-10-04T07:48:25Z","req_id":1}"#;
    assert!(matches!(
        decoder.decode_payload(warned)?,
        KrakenDecodeOutcome::Control(KrakenControl::Subscribed(KrakenSubscription::Book))
    ));
    let refused = br#"{"method":"subscribe","success":false,"error":"rate limit exceeded","time_in":"2023-10-04T07:48:25Z","time_out":"2023-10-04T07:48:25Z","req_id":1}"#;
    assert!(matches!(
        decoder.decode_payload(refused)?,
        KrakenDecodeOutcome::Control(KrakenControl::SubscriptionRefused)
    ));
    let mut wrong_request_decoder =
        KrakenDecoder::try_new("BTC/USD", instrument, KrakenDepth::Ten)?;
    let wrong_request = br#"{"method":"subscribe","result":{"channel":"book","depth":10,"snapshot":true,"symbol":"BTC/USD"},"success":true,"time_in":"2023-10-04T07:48:25Z","time_out":"2023-10-04T07:48:25Z","req_id":2}"#;
    assert!(wrong_request_decoder.decode_payload(wrong_request).is_err());
    let wrong_depth = br#"{"method":"subscribe","result":{"channel":"book","depth":25,"snapshot":true,"symbol":"BTC/USD"},"success":true,"time_in":"2023-10-04T07:48:25Z","time_out":"2023-10-04T07:48:25Z","req_id":1}"#;
    assert!(decoder.decode_payload(wrong_depth).is_err());
    assert_eq!(decoder.state(), KrakenDecoderState::Quarantined);

    let mut strict_decoder = KrakenDecoder::try_new("BTC/USD", instrument, KrakenDepth::Ten)?;
    let unknown = br#"{"method":"subscribe","result":{"channel":"book","depth":10,"snapshot":true,"symbol":"BTC/USD","unexpected":true},"success":true,"time_in":"2023-10-04T07:48:25Z","time_out":"2023-10-04T07:48:25Z","req_id":1}"#;
    assert!(strict_decoder.decode_payload(unknown).is_err());
    Ok(())
}

#[test]
fn trade_batch_rejects_max_plus_one_before_conversion() -> TestResult {
    let instrument = InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?;
    let mut exact = KrakenDecoder::try_trades("BTC/USD", instrument)?;
    let exact_payload = trade_payload(MAX_DECODED_EVENTS)?;
    let KrakenDecodeOutcome::Market(observations) = exact.decode_payload(&exact_payload)? else {
        return Err("exact-bound trade batch decoded as control traffic".into());
    };
    assert_eq!(observations.len(), MAX_DECODED_EVENTS);
    assert!(matches!(
        observations[0].payload(),
        ProviderObservationPayload::Trade {
            taker_order_type: Some(TradeTakerOrderType::Market),
            ..
        }
    ));

    let mut excessive = KrakenDecoder::try_trades("BTC/USD", instrument)?;
    let excessive_payload = trade_payload(MAX_DECODED_EVENTS + 1)?;
    assert!(matches!(
        excessive.decode_payload(&excessive_payload),
        Err(DecodeError::TooManyEvents {
            max: MAX_DECODED_EVENTS,
        })
    ));
    Ok(())
}

#[test]
fn checksum_failure_is_atomic_and_quarantines_until_a_fresh_snapshot() -> TestResult {
    let instrument = InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?;
    let mut decoder = KrakenDecoder::try_new("BTC/USD", instrument, KrakenDepth::Ten)?;
    let snapshot = include_bytes!("../fixtures/official_book_checksum.json");
    let _snapshot = decoder.decode_payload(snapshot)?;
    let before = decoder.book_digest();
    let invalid = br#"{"channel":"book","type":"update","data":[{"symbol":"BTC/USD","bids":[{"price":"45283.5","qty":"0"}],"asks":[],"checksum":1,"timestamp":"2023-10-04T07:48:26Z"}]}"#;

    assert!(decoder.decode_payload(invalid).is_err());
    assert_eq!(decoder.book_digest(), before);
    assert_eq!(decoder.state(), KrakenDecoderState::Quarantined);
    assert!(decoder.decode_payload(invalid).is_err());

    let _resnapshot = decoder.decode_payload(snapshot)?;
    assert_eq!(decoder.state(), KrakenDecoderState::Healthy);
    Ok(())
}

#[test]
fn repeated_price_changes_apply_in_wire_order_before_one_checksum_commit() -> TestResult {
    let instrument = InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?;
    let mut decoder = KrakenDecoder::try_new("BTC/USD", instrument, KrakenDepth::Ten)?;
    let _snapshot =
        decoder.decode_payload(include_bytes!("../fixtures/official_book_checksum.json"))?;
    let update = br#"{"channel":"book","type":"update","data":[{"symbol":"BTC/USD","bids":[{"price":"45283.5","qty":"0"},{"price":"45283.5","qty":"0.10000000"},{"price":"1.0","qty":"1.0"}],"asks":[],"checksum":3310070434,"timestamp":"2023-10-04T07:48:26Z"}]}"#;

    let KrakenDecodeOutcome::Market(batch) = decoder.decode_payload(update)? else {
        return Err("valid update decoded as control traffic".into());
    };
    assert_eq!(decoder.last_checksum(), Some(3_310_070_434));
    let ProviderObservationPayload::BookDelta(delta) = batch[0].payload() else {
        return Err("update decoded to the wrong payload".into());
    };
    assert_eq!(delta.changes().len(), 3);
    assert_eq!(
        delta.changes()[1].level().quantity().value().as_str(),
        "0.10000000"
    );
    Ok(())
}

#[test]
fn authenticated_level3_identity_checksum_quarantine_and_recovery_are_atomic() -> TestResult {
    let instrument = InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?;
    let credential_record = SourceIdentifier::try_from("kraken-read-only-market-data-account")?;
    let config = KrakenL3Config::try_new(
        level3_metadata(instrument, credential_record.clone())?,
        vec![KrakenL3ProductMapping::try_new("BTC/USD", instrument)?],
        KrakenL3Depth::Ten,
        KrakenL3ClientTier::Standard,
        credential_record,
        NonZeroUsize::new(1 << 20).ok_or("zero frame bound")?,
    )?;
    assert_eq!(config.market_depth(), MarketDepth::OrderLevel);
    assert_eq!(
        config.metadata().quality_ceiling(),
        DataQuality::DirectUnverified
    );

    let token = KrakenL3WebSocketToken::try_new("fixture-ephemeral-token")?;
    let subscription = config.try_subscription_payload(token, 0, Some(7))?;
    let subscription: serde_json::Value = serde_json::from_slice(subscription.as_bytes())?;
    assert_eq!(subscription["params"]["channel"], "level3");
    assert_eq!(subscription["params"]["depth"], 10);
    assert_eq!(subscription["params"]["snapshot"], true);
    assert_eq!(subscription["params"]["symbol"][0], "BTC/USD");

    let mut decoder = KrakenL3Decoder::try_new(&config)?;
    let acknowledgement = br#"{"method":"subscribe","result":{"channel":"level3","depth":10,"snapshot":true,"symbol":"BTC/USD"},"success":true,"time_in":"2024-01-08T12:26:45.900000000Z","time_out":"2024-01-08T12:26:45.910000000Z","req_id":7}"#;
    assert!(matches!(
        decoder.decode_payload(acknowledgement)?,
        KrakenL3DecodeOutcome::Control(KrakenL3Control::Subscribed { instrument: value, .. })
            if value == instrument
    ));

    let snapshot = include_bytes!("../fixtures/official_level3_checksum.json");
    let KrakenL3DecodeOutcome::Book(snapshot_batch) = decoder.decode_payload(snapshot)? else {
        return Err("official L3 fixture decoded as control traffic".into());
    };
    assert_eq!(snapshot_batch.kind(), KrakenL3BatchKind::Snapshot);
    assert_eq!(snapshot_batch.market_depth(), MarketDepth::OrderLevel);
    assert_eq!(
        snapshot_batch.quality_ceiling(),
        DataQuality::DirectUnverified
    );
    assert_eq!(snapshot_batch.checksum(), 1_063_832_831);
    assert_eq!(snapshot_batch.local_generation_ordinal(), 1);
    assert_eq!(snapshot_batch.events().len(), 35);
    assert_eq!(
        decoder.state("BTC/USD"),
        Some(KrakenL3DecoderState::Healthy)
    );
    assert_eq!(decoder.order_count("BTC/USD"), Some(35));

    let update = br#"{"channel":"level3","type":"update","data":[{"symbol":"BTC/USD","timestamp":"2024-01-08T12:26:46.400000000Z","checksum":1063832831,"bids":[{"event":"modify","order_id":"OJPMIN-NXZL5-SOWP6V","limit_price":"44937.1","order_qty":"0.01000000","timestamp":"2024-01-08T12:26:46.100000000Z"},{"event":"delete","order_id":"OJPMIN-NXZL5-SOWP6V","limit_price":"44937.1","order_qty":"0","timestamp":"2024-01-08T12:26:46.200000000Z"},{"event":"add","order_id":"OJPMIN-NXZL5-SOWP6V","limit_price":"44937.1","order_qty":"0.03346877","timestamp":"2024-01-08T12:26:46.300000000Z"}]}]}"#;
    let KrakenL3DecodeOutcome::Book(update_batch) = decoder.decode_payload(update)? else {
        return Err("valid L3 update decoded as control traffic".into());
    };
    assert_eq!(update_batch.kind(), KrakenL3BatchKind::Update);
    assert_eq!(update_batch.local_generation_ordinal(), 2);
    assert_eq!(update_batch.events().len(), 3);
    assert_eq!(
        update_batch.events()[0].kind(),
        KrakenL3OrderEventKind::Modify
    );
    assert_eq!(
        update_batch.events()[1].kind(),
        KrakenL3OrderEventKind::Delete
    );
    assert_eq!(update_batch.events()[2].kind(), KrakenL3OrderEventKind::Add);
    assert_eq!(
        decoder
            .order("BTC/USD", "OJPMIN-NXZL5-SOWP6V")
            .ok_or("updated order missing")?
            .quantity()
            .value()
            .as_str(),
        "0.03346877"
    );

    let invalid = br#"{"channel":"level3","type":"update","data":[{"symbol":"BTC/USD","timestamp":"2024-01-08T12:26:46.600000000Z","checksum":1,"bids":[{"event":"modify","order_id":"OJPMIN-NXZL5-SOWP6V","limit_price":"44937.1","order_qty":"0.01000000","timestamp":"2024-01-08T12:26:46.500000000Z"}]}]}"#;
    assert!(decoder.decode_payload(invalid).is_err());
    assert_eq!(
        decoder.state("BTC/USD"),
        Some(KrakenL3DecoderState::Quarantined)
    );
    assert_eq!(decoder.last_checksum("BTC/USD"), Some(1_063_832_831));
    assert_eq!(
        decoder
            .order("BTC/USD", "OJPMIN-NXZL5-SOWP6V")
            .ok_or("atomic rollback lost the order")?
            .quantity()
            .value()
            .as_str(),
        "0.03346877"
    );

    let mut recovery_snapshot: serde_json::Value = serde_json::from_slice(snapshot)?;
    recovery_snapshot["data"][0]["timestamp"] =
        serde_json::Value::String("2024-01-08T12:26:47.000000000Z".to_owned());
    let recovery_snapshot = serde_json::to_vec(&recovery_snapshot)?;
    let _recovered = decoder.decode_payload(&recovery_snapshot)?;
    assert_eq!(
        decoder.state("BTC/USD"),
        Some(KrakenL3DecoderState::Healthy)
    );
    decoder.reset_for_reconnect();
    assert_eq!(
        decoder.state("BTC/USD"),
        Some(KrakenL3DecoderState::AwaitingSnapshot)
    );
    assert_eq!(decoder.order_count("BTC/USD"), Some(0));
    assert_eq!(decoder.last_checksum("BTC/USD"), None);
    Ok(())
}

fn level3_metadata(
    instrument: InstrumentId,
    credential_record: SourceIdentifier,
) -> Result<market_squawk_sources::SourceMetadata, Box<dyn Error>> {
    let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
    let evidence = |byte| {
        ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            [byte; 32],
        ))
    };
    let authorization = AuthorizationGrant::new(
        AuthorizationMode::UserAuthorized,
        AuthorizationBasis::new(credential_record.clone()),
        evidence(2),
        effective,
    );
    let budget = ProviderBudgetPolicy::try_new(
        BudgetScope::with_authorization_account(
            SourceIdentifier::try_from("kraken")?,
            credential_record,
        ),
        NonZeroU32::new(200).ok_or("zero request budget")?,
        NonZeroU64::new(1_000_000_000).ok_or("zero budget window")?,
        NonZeroU16::new(2).ok_or("zero concurrency")?,
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
            evidence(1),
        ),
        authorization,
        evidence(3),
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

fn trade_payload(count: usize) -> Result<Vec<u8>, serde_json::Error> {
    let data = (0..count)
        .map(|trade_id| {
            serde_json::json!({
                "symbol": "BTC/USD",
                "side": "buy",
                "price": "45283.50000",
                "qty": "0.01000000",
                "ord_type": "market",
                "trade_id": trade_id,
                "timestamp": "2023-10-04T07:48:26Z",
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_vec(&serde_json::json!({
        "channel": "trade",
        "type": "update",
        "data": data,
    }))
}
