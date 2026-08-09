mod common;

use bytes::Bytes;
use common::{TestResult, config, identifier};
use market_squawk_adapter_coinbase::CoinbaseExchangeDecoder;
use market_squawk_domain::{
    AggressorSide, ConnectionGeneration, LiveEventClass, MarketDepth, SequenceCapability, Timestamp,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, ControlFrameKind, DecodeOutcome, IgnoredFrameReason,
    MAX_DECODED_BOOK_ITEMS, MarketDecoder, ProviderBookSide, ProviderChecksumEvidence,
    ProviderObservationPayload, ProviderSequenceEvidence, QuarantineReason, SessionId,
    TransportFrameKind,
};
use sha2::{Digest, Sha256};

fn decode(payload: &[u8]) -> TestResult<DecodeOutcome> {
    let config = config()?;
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let registered = registry.register(config.metadata().clone(), Timestamp::from_unix_nanos(1))?;
    let session = registry.begin_session(
        &registered,
        SessionId::new(identifier("coinbase-session-1")?),
        ConnectionGeneration::new(1)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let mut frames = registry.take_raw_frame_factory(&session)?;
    let frame = frames.try_frame(TransportFrameKind::Text, Bytes::copy_from_slice(payload))?;
    let validated = session.validate_live_frame(&frame)?;
    let mut decoder = CoinbaseExchangeDecoder::try_new(&config)?;
    Ok(decoder.decode(&validated)?)
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
    let snapshot = decode(include_bytes!("../fixtures/snapshot.json"))?;
    let DecodeOutcome::Data(snapshot) = snapshot else {
        return Err("snapshot was not data".into());
    };
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

    let trade = decode(include_bytes!("../fixtures/match.json"))?;
    let DecodeOutcome::Data(trade) = trade else {
        return Err("match was not data".into());
    };
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
        decoder.decode(&validated)?,
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
