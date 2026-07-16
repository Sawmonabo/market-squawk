use chrono::Utc;
use market_engine::{domain::MarketEvent, source::coinbase::decode_message};
use rust_decimal::Decimal;
use serde_json::json;

#[test]
fn decodes_level_two_snapshot_without_floating_point() {
    let event = decode_message(
        &json!({
            "type": "snapshot",
            "product_id": "BTC-USD",
            "bids": [["100.10", "1.25"]],
            "asks": [["100.20", "2.50"]]
        }),
        Utc::now(),
    )
    .expect("decode succeeds")
    .expect("snapshot is supported");

    let (bids, asks) = match event {
        MarketEvent::BookSnapshot { bids, asks, .. } => (bids, asks),
        _ => panic!("expected snapshot"),
    };
    assert_eq!(bids[0].price, Decimal::new(10010, 2));
    assert_eq!(asks[0].size, Decimal::new(250, 2));
}

#[test]
fn decodes_heartbeat_sequence_for_gap_detection() {
    let event = decode_message(
        &json!({
            "type": "heartbeat",
            "sequence": 90,
            "last_trade_id": 20,
            "product_id": "BTC-USD",
            "time": "2026-07-15T20:00:00Z"
        }),
        Utc::now(),
    )
    .expect("decode succeeds")
    .expect("heartbeat is supported");

    let sequence = match event {
        MarketEvent::Heartbeat { sequence, .. } => sequence,
        _ => panic!("expected heartbeat"),
    };
    assert_eq!(sequence, 90);
}

#[test]
fn decodes_match_side_as_the_maker_side() {
    let event = decode_message(
        &json!({
            "type": "match",
            "trade_id": 10,
            "product_id": "BTC-USD",
            "price": "100.25",
            "size": "0.50",
            "side": "sell",
            "time": "2026-07-15T20:00:00Z"
        }),
        Utc::now(),
    )
    .expect("decode succeeds")
    .expect("match is supported");

    let maker_side = match event {
        MarketEvent::Trade { maker_side, .. } => maker_side,
        _ => panic!("expected trade"),
    };
    assert_eq!(maker_side, market_engine::domain::Side::Sell);
}

#[test]
fn rejects_an_invalid_present_exchange_timestamp() {
    let error = decode_message(
        &json!({
            "type": "l2update",
            "product_id": "BTC-USD",
            "changes": [["buy", "100.00", "1.00"]],
            "time": "not-a-timestamp"
        }),
        Utc::now(),
    )
    .expect_err("invalid timestamps must fail decoding");

    assert!(error.to_string().contains("invalid RFC 3339 timestamp"));
}
