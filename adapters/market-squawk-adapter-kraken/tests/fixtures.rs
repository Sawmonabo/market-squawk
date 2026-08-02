use std::error::Error;
use std::str::FromStr;

use market_squawk_adapter_kraken::{
    KrakenControl, KrakenDecodeOutcome, KrakenDecoder, KrakenDecoderState, KrakenDepth,
    KrakenSubscription,
};
use market_squawk_domain::{InstrumentId, LiveEventClass};
use market_squawk_sources::{
    DecodeError, MAX_DECODED_EVENTS, ProviderObservationPayload, ProviderSequenceEvidence,
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
    let valid = br#"{"method":"subscribe","result":{"channel":"book","depth":10,"snapshot":true,"symbol":"BTC/USD"},"success":true,"time_in":"2023-10-04T07:48:25Z","time_out":"2023-10-04T07:48:25Z"}"#;
    assert!(matches!(
        decoder.decode_payload(valid)?,
        KrakenDecodeOutcome::Control(KrakenControl::Subscribed(KrakenSubscription::Book))
    ));
    let warned = br#"{"method":"subscribe","result":{"channel":"book","depth":10,"snapshot":true,"symbol":"BTC/USD","warnings":["field will be deprecated"]},"success":true,"time_in":"2023-10-04T07:48:25Z","time_out":"2023-10-04T07:48:25Z"}"#;
    assert!(matches!(
        decoder.decode_payload(warned)?,
        KrakenDecodeOutcome::Control(KrakenControl::Subscribed(KrakenSubscription::Book))
    ));
    let wrong_depth = br#"{"method":"subscribe","result":{"channel":"book","depth":25,"snapshot":true,"symbol":"BTC/USD"},"success":true,"time_in":"2023-10-04T07:48:25Z","time_out":"2023-10-04T07:48:25Z"}"#;
    assert!(decoder.decode_payload(wrong_depth).is_err());
    assert_eq!(decoder.state(), KrakenDecoderState::Quarantined);

    let mut strict_decoder = KrakenDecoder::try_new("BTC/USD", instrument, KrakenDepth::Ten)?;
    let unknown = br#"{"method":"subscribe","result":{"channel":"book","depth":10,"snapshot":true,"symbol":"BTC/USD","unexpected":true},"success":true,"time_in":"2023-10-04T07:48:25Z","time_out":"2023-10-04T07:48:25Z"}"#;
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
