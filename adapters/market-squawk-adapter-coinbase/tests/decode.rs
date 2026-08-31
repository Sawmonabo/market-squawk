mod common;

use bytes::Bytes;
use common::{TestResult, config, identifier};
use market_squawk_adapter_coinbase::{
    CoinbaseExchangeDecoder, CoinbaseMarketChannel, CoinbaseMarketContinuity,
    CoinbaseMarketDecodeOutcome, CoinbaseMarketFeed, CoinbaseMarketHandoff,
};
use market_squawk_domain::{
    AggressorSide, ConnectionGeneration, LiveEventClass, MarketDepth, SequenceCapability, Timestamp,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, ControlFrameKind, DecodeOutcome, IgnoredFrameReason,
    MAX_DECODED_BOOK_ITEMS, ProviderBookSide, ProviderChecksumEvidence, ProviderObservationPayload,
    ProviderSequenceEvidence, QuarantineReason, SessionId, TransportFrameKind,
};
use sha2::{Digest, Sha256};

fn decode_provider(payload: &[u8]) -> TestResult<CoinbaseMarketDecodeOutcome> {
    let config = config()?;
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let registered = registry.register(config.metadata().clone(), Timestamp::from_unix_nanos(1))?;
    let session = registry.begin_session(
        &registered,
        SessionId::new(identifier("coinbase-decoder-session")?),
        ConnectionGeneration::new(1)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let mut frames = registry.take_raw_frame_factory(&session)?;
    let frame = frames.try_frame(TransportFrameKind::Text, Bytes::copy_from_slice(payload))?;
    let validated = session.validate_live_frame(&frame)?;
    let mut decoder = CoinbaseExchangeDecoder::try_new(&config)?;
    Ok(decoder.decode_market_handoff(&validated)?)
}

fn decode(payload: &[u8]) -> TestResult<DecodeOutcome> {
    match decode_provider(payload)? {
        CoinbaseMarketDecodeOutcome::Market(handoff) => {
            let (_evidence, _payload, batch) = handoff.into_parts();
            Ok(DecodeOutcome::Data(batch))
        }
        CoinbaseMarketDecodeOutcome::Other(outcome) => Ok(outcome),
    }
}

fn decode_market(payload: &[u8]) -> TestResult<CoinbaseMarketHandoff> {
    match decode_provider(payload)? {
        CoinbaseMarketDecodeOutcome::Market(handoff) => Ok(handoff),
        CoinbaseMarketDecodeOutcome::Other(_) => {
            Err("market frame did not produce a handoff".into())
        }
    }
}

#[test]
fn official_protocol_fixtures_match_the_pinned_manifest() -> TestResult {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures");
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("manifest.json"))?)?;
    for (field, expected) in [
        (
            "authoritative_url",
            "https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-channels",
        ),
        ("retrieved_at", "2026-08-08"),
        (
            "derivation",
            "Minimal deterministic fixtures transcribed from the documented public Advanced Trade level2, market_trades, heartbeats, and cumulative subscriptions schemas; product, price, quantity, identifiers, and timestamps were normalized for decoder coverage",
        ),
        (
            "protocol_revision",
            "Coinbase Advanced Trade WebSocket documentation (unversioned; retrieved 2026-08-08)",
        ),
        ("terms_url", "https://www.coinbase.com/legal/market_data"),
    ] {
        assert_eq!(
            manifest.get(field).and_then(serde_json::Value::as_str),
            Some(expected),
            "fixture manifest omitted exact {field} provenance"
        );
    }
    let fixtures = manifest
        .get("fixtures")
        .and_then(serde_json::Value::as_array)
        .ok_or("fixture manifest omitted fixtures")?;
    assert_eq!(fixtures.len(), 5);
    for fixture in fixtures {
        let path = fixture
            .get("path")
            .and_then(serde_json::Value::as_str)
            .ok_or("fixture path was missing")?;
        let expected = fixture
            .get("sha256")
            .and_then(serde_json::Value::as_str)
            .ok_or("fixture digest was missing")?;
        let digest = Sha256::digest(std::fs::read(root.join(path))?);
        let actual = digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert_eq!(actual, expected, "fixture digest changed: {path}");
    }
    Ok(())
}

#[test]
fn decodes_exact_book_and_trade_evidence_without_promoting_integrity() -> TestResult {
    let source_config = config()?;
    let snapshot_handoff = decode_market(include_bytes!("../fixtures/snapshot.json"))?;
    assert_eq!(
        snapshot_handoff.evidence().feed(),
        CoinbaseMarketFeed::AdvancedTradePublic
    );
    assert_eq!(
        snapshot_handoff.evidence().channel(),
        CoinbaseMarketChannel::Level2
    );
    assert!(matches!(
        snapshot_handoff.evidence().continuity(),
        CoinbaseMarketContinuity::ProviderCursorUnverified { terminal: 0 }
    ));
    assert_eq!(
        snapshot_handoff
            .evidence()
            .product()
            .as_source_identifier()
            .as_str(),
        "BTC-USD"
    );
    assert_eq!(
        snapshot_handoff.evidence().request_set_digest().bytes(),
        [
            0x51, 0xbd, 0x61, 0xa8, 0x5d, 0x8d, 0x0e, 0x4f, 0xd6, 0x6e, 0xde, 0x54, 0x92, 0x59,
            0x3b, 0x69, 0xf1, 0x42, 0xa8, 0x6e, 0xd5, 0x86, 0x87, 0x77, 0x25, 0x22, 0x65, 0x6a,
            0xc2, 0x90, 0xd2, 0x7f,
        ]
    );
    assert_eq!(
        snapshot_handoff.evidence().subscription_digest().bytes(),
        [
            0x54, 0xba, 0xd8, 0xc6, 0x3d, 0x47, 0xfe, 0xa9, 0x4c, 0x1a, 0xf6, 0x52, 0x0e, 0x56,
            0x62, 0x9e, 0x1b, 0x5c, 0x86, 0xce, 0xac, 0x39, 0x4c, 0x09, 0x33, 0xd0, 0xfe, 0x57,
            0x07, 0xeb, 0xc0, 0xa5,
        ]
    );
    assert_eq!(
        snapshot_handoff.evidence().event_class(),
        LiveEventClass::BookSnapshot
    );
    assert_eq!(
        snapshot_handoff.evidence().native_input_depth(),
        Some(MarketDepth::PriceLevel)
    );
    assert_eq!(
        snapshot_handoff.evidence().output_depth(),
        Some(MarketDepth::PriceLevel)
    );
    assert_eq!(
        snapshot_handoff.raw_payload().as_bytes(),
        include_bytes!("../fixtures/snapshot.json")
    );
    assert_eq!(
        snapshot_handoff.raw_payload_digest(),
        snapshot_handoff.typed_batch().evidence().payload_digest()
    );
    let (evidence, _raw_payload, snapshot) = snapshot_handoff.into_parts();
    assert_eq!(
        evidence.provider_identity_key().source_id().as_str(),
        "coinbase-exchange-public"
    );
    assert_eq!(
        evidence
            .provider_identity_key()
            .provider_instrument_id()
            .as_str(),
        "BTC-USD"
    );
    assert_eq!(evidence.venue_symbol().as_str(), "BTC-USD");
    assert_eq!(
        evidence.provider_identity_revision(),
        source_config.metadata().revision()
    );
    assert_eq!(
        evidence.provider_identity_digest(),
        source_config
            .metadata()
            .revision_evidence()
            .payload_evidence()
            .content_digest()
    );
    let observation = &snapshot.observations()[0];
    assert_eq!(observation.event_class(), LiveEventClass::BookSnapshot);
    assert_eq!(observation.depth(), Some(MarketDepth::PriceLevel));
    assert!(matches!(
        observation.sequence(),
        ProviderSequenceEvidence::Unsupported { .. }
    ));
    assert!(matches!(
        observation.checksum(),
        ProviderChecksumEvidence::Unsupported { .. }
    ));
    let ProviderObservationPayload::BookSnapshot(book) = observation.payload() else {
        return Err("snapshot payload was not a book".into());
    };
    assert_eq!(book.bids()[0].price().value().as_str(), "100.10");
    assert_eq!(book.bids()[0].quantity().value().as_str(), "1.2500");

    let documented_snapshot_time = Timestamp::from_unix_nanos(1_786_190_400_000_000_000);
    let stale_level_snapshot = decode(
        br#"{"channel":"l2_data","client_id":"","timestamp":"2026-08-08T12:00:00Z","sequence_num":0,"events":[{"type":"snapshot","product_id":"BTC-USD","updates":[{"side":"bid","event_time":"1970-01-01T00:00:00Z","price_level":"100.10","new_quantity":"1.25"}]}]}"#,
    )?;
    let DecodeOutcome::Data(stale_level_snapshot) = stale_level_snapshot else {
        return Err("documented stale-level snapshot was not data".into());
    };
    assert!(matches!(
        stale_level_snapshot.observations()[0].timestamp(),
        market_squawk_sources::ProviderTimestampEvidence::Provided { value, .. }
            if *value == documented_snapshot_time
    ));

    let delta = decode(include_bytes!("../fixtures/l2update.json"))?;
    let DecodeOutcome::Data(delta) = delta else {
        return Err("delta was not data".into());
    };
    let ProviderObservationPayload::BookDelta(book) = delta.observations()[0].payload() else {
        return Err("delta payload was not a book".into());
    };
    assert_eq!(book.changes()[0].side(), ProviderBookSide::Bid);
    assert_eq!(book.changes()[1].level().quantity().value().as_str(), "0");

    let trade_handoff = decode_market(include_bytes!("../fixtures/match.json"))?;
    assert_eq!(
        trade_handoff.evidence().channel(),
        CoinbaseMarketChannel::MarketTrades
    );
    assert!(matches!(
        trade_handoff.evidence().continuity(),
        CoinbaseMarketContinuity::ProviderCursorUnverified { terminal: 2 }
    ));
    assert_eq!(
        trade_handoff.evidence().event_class(),
        LiveEventClass::Trade
    );
    assert_eq!(trade_handoff.evidence().native_input_depth(), None);
    assert_eq!(trade_handoff.evidence().output_depth(), None);
    let (_evidence, _raw_payload, trade) = trade_handoff.into_parts();
    let ProviderObservationPayload::Trade {
        aggressor,
        price,
        quantity,
        ..
    } = trade.observations()[0].payload()
    else {
        return Err("match payload was not a trade".into());
    };
    assert_eq!(aggressor.side(), AggressorSide::Buy);
    assert_eq!(price.value().as_str(), "100.2500");
    assert_eq!(quantity.value().as_str(), "0.5000");
    assert_eq!(
        config()?.metadata().capabilities().sequence(),
        SequenceCapability::Unsupported
    );
    Ok(())
}

#[test]
fn classifies_control_extensions_and_provider_input_failures() -> TestResult {
    let subscriptions = decode(include_bytes!("../fixtures/subscriptions.json"))?;
    assert!(matches!(
        subscriptions,
        DecodeOutcome::Control(value)
            if value.kind() == ControlFrameKind::SubscriptionAcknowledgement
    ));
    let heartbeat = decode(include_bytes!("../fixtures/heartbeat.json"))?;
    assert!(matches!(
        heartbeat,
        DecodeOutcome::Control(value) if value.kind() == ControlFrameKind::Heartbeat
    ));
    let live_numeric_heartbeat = decode(
        br#"{"channel":"heartbeats","timestamp":"2026-08-08T12:00:00.423456Z","sequence_num":3,"events":[{"current_time":"2026-08-08 12:00:00.423456 +0000 UTC","heartbeat_counter":90}]}"#,
    )?;
    assert!(matches!(
        live_numeric_heartbeat,
        DecodeOutcome::Control(value) if value.kind() == ControlFrameKind::Heartbeat
    ));
    let extension = decode(br#"{"type":"future_safe_extension","bounded":"value"}"#)?;
    assert!(matches!(
        extension,
        DecodeOutcome::Ignored(value)
            if value.reason() == IgnoredFrameReason::DocumentedForwardCompatibleExtension
    ));

    for (payload, reason) in [
        (
            br#"{"channel":"l2_data","client_id":"","timestamp":"2026-08-08T12:00:00Z","sequence_num":1,"events":[{"type":"update","product_id":"BTC-USD","updates":[{"side":"bid","event_time":"not-a-time","price_level":"100.00","new_quantity":"1.00"}]}]}"#.as_slice(),
            QuarantineReason::InvalidTimestamp,
        ),
        (
            br#"{"channel":"market_trades","client_id":"","timestamp":"2026-08-08T12:00:00Z","sequence_num":1,"events":[{"type":"update","trades":[{"trade_id":"10","product_id":"BTC-USD","price":"100","size":"-1","side":"SELL","time":"2026-08-08T12:00:00Z"}]}]}"#.as_slice(),
            QuarantineReason::NegativeQuantity,
        ),
        (
            br#"{"channel":"l2_data","client_id":"","timestamp":"2026-08-08T12:00:00Z","sequence_num":1,"events":[{"type":"snapshot","product_id":"ETH-USD","updates":[{"side":"bid","event_time":"2026-08-08T12:00:00Z","price_level":"100","new_quantity":"1"}]}]}"#.as_slice(),
            QuarantineReason::WrongProduct,
        ),
        (
            br#"{"channel":"l2_data","channel":"l2_data","client_id":"","timestamp":"2026-08-08T12:00:00Z","sequence_num":1,"events":[]}"#.as_slice(),
            QuarantineReason::SchemaViolation,
        ),
        (
            br#"{"channel":"l2_data","client_id":"","timestamp":"2026-08-08T12:00:00Z","sequence_num":1,"events":[],"unexpected":true}"#.as_slice(),
            QuarantineReason::SchemaViolation,
        ),
        (
            br#"{"channel":"l2_data","client_id":"","timestamp":"2026-08-08T12:00:00Z","sequence_num":1,"events":[{"type":"snapshot","product_id":"BTC-USD","updates":[{"side":"bid","event_time":"2026-08-08T12:00:00Z","price_level":"1e2","new_quantity":"1"}]}]}"#.as_slice(),
            QuarantineReason::InexactNumericValue,
        ),
        (
            br#"{"channel":"subscriptions","client_id":"","timestamp":"2026-08-08T12:00:00Z","sequence_num":1,"events":[{"subscriptions":{"ticker":["BTC-USD"]}}]}"#.as_slice(),
            QuarantineReason::WrongChannel,
        ),
    ] {
        let outcome = decode(payload)?;
        assert!(matches!(outcome, DecodeOutcome::Quarantine(value) if value.reason() == reason));
    }
    assert!(matches!(
        decode(br#"{"type":"error","message":"Failed to subscribe"}"#)?,
        DecodeOutcome::Resynchronize(value)
            if value.reason()
                == market_squawk_sources::ResynchronizationReason::ProviderRequestedReset
    ));
    Ok(())
}

#[test]
fn binary_and_oversized_cardinality_fail_closed() -> TestResult {
    let config = config()?;
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let registered = registry.register(config.metadata().clone(), Timestamp::from_unix_nanos(1))?;
    let session = registry.begin_session(
        &registered,
        SessionId::new(identifier("coinbase-session-binary")?),
        ConnectionGeneration::new(1)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let mut frames = registry.take_raw_frame_factory(&session)?;
    let frame = frames.try_frame(TransportFrameKind::Binary, Bytes::from_static(b"{}"))?;
    let validated = session.validate_live_frame(&frame)?;
    let mut decoder = CoinbaseExchangeDecoder::try_new(&config)?;
    assert!(matches!(
        match decoder.decode_market_handoff(&validated)? {
            CoinbaseMarketDecodeOutcome::Other(outcome) => outcome,
            CoinbaseMarketDecodeOutcome::Market(_) => {
                return Err("binary frame unexpectedly produced market data".into());
            }
        },
        DecodeOutcome::Quarantine(value)
            if value.reason() == QuarantineReason::SchemaViolation
    ));

    let mut levels = String::from(
        r#"{"channel":"l2_data","client_id":"","timestamp":"2026-08-08T12:00:00Z","sequence_num":1,"events":[{"type":"snapshot","product_id":"BTC-USD","updates":["#,
    );
    for index in 0..=MAX_DECODED_BOOK_ITEMS {
        if index > 0 {
            levels.push(',');
        }
        levels.push_str(
            r#"{"side":"bid","event_time":"2026-08-08T12:00:00Z","price_level":"1","new_quantity":"1"}"#,
        );
    }
    levels.push_str(r#"]}]}"#);
    let outcome = decode(levels.as_bytes())?;
    assert!(matches!(
        outcome,
        DecodeOutcome::Quarantine(value)
            if value.reason() == QuarantineReason::SchemaViolation
    ));
    Ok(())
}
