mod common;

use bytes::Bytes;
use common::{TestResult, config, identifier};
use market_squawk_adapter_coinbase::CoinbaseExchangeDecoder;
use market_squawk_domain::{
    AggressorSide, ConnectionGeneration, LiveEventClass, MarketDepth, SequenceCapability, Timestamp,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, ControlFrameKind, DecodeOutcome, IgnoredFrameReason,
    MarketDecoder, ProviderBookSide, ProviderChecksumEvidence, ProviderObservationPayload,
    ProviderSequenceEvidence, QuarantineReason, SessionId, TransportFrameKind,
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
            "https://docs.cdp.coinbase.com/exchange/websocket-feed/channels",
        ),
        ("retrieved_at", "2026-07-21"),
        (
            "derivation",
            "Minimal deterministic fixtures transcribed from the documented unauthenticated level2_batch snapshot/update, match, heartbeat, and subscriptions schemas; product, price, quantity, identifiers, and timestamps were normalized for decoder coverage",
        ),
        (
            "protocol_revision",
            "Coinbase Exchange WebSocket Feed documentation (unversioned; retrieved 2026-07-21)",
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
    let extension = decode(br#"{"type":"future_safe_extension","bounded":"value"}"#)?;
    assert!(matches!(
        extension,
        DecodeOutcome::Ignored(value)
            if value.reason() == IgnoredFrameReason::DocumentedForwardCompatibleExtension
    ));

    for (payload, reason) in [
        (
            br#"{"type":"l2update","product_id":"BTC-USD","time":"not-a-time","changes":[["buy","100.00","1.00"]]}"#.as_slice(),
            QuarantineReason::InvalidTimestamp,
        ),
        (
            br#"{"type":"match","trade_id":10,"sequence":50,"maker_order_id":"a","taker_order_id":"b","time":"2026-07-20T12:00:00Z","product_id":"BTC-USD","size":"-1","price":"100","side":"sell"}"#.as_slice(),
            QuarantineReason::NegativeQuantity,
        ),
        (
            br#"{"type":"snapshot","product_id":"ETH-USD","bids":[["100","1"]],"asks":[["101","1"]]}"#.as_slice(),
            QuarantineReason::WrongProduct,
        ),
        (
            br#"{"type":"snapshot","type":"snapshot","product_id":"BTC-USD","bids":[],"asks":[]}"#.as_slice(),
            QuarantineReason::SchemaViolation,
        ),
        (
            br#"{"type":"snapshot","product_id":"BTC-USD","bids":[],"asks":[],"unexpected":true}"#.as_slice(),
            QuarantineReason::SchemaViolation,
        ),
        (
            br#"{"type":"snapshot","product_id":"BTC-USD","bids":[["1e2","1"]],"asks":[["101","1"]]}"#.as_slice(),
            QuarantineReason::InexactNumericValue,
        ),
        (
            br#"{"type":"subscriptions","channels":[{"name":"level2","product_ids":["BTC-USD"]},{"name":"matches","product_ids":["BTC-USD"]}]}"#.as_slice(),
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

    let mut levels = String::from(r#"{"type":"snapshot","product_id":"BTC-USD","bids":["#);
    for index in 0..=10_000 {
        if index > 0 {
            levels.push(',');
        }
        levels.push_str(r#"["1","1"]"#);
    }
    levels.push_str(r#"],"asks":[["2","1"]]}"#);
    let outcome = decode(levels.as_bytes())?;
    assert!(matches!(
        outcome,
        DecodeOutcome::Quarantine(value)
            if value.reason() == QuarantineReason::SchemaViolation
    ));
    Ok(())
}
