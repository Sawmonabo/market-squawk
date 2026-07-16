use anyhow::{Context, Result, bail};
use chrono::Utc;
use market_squawk::{domain::MarketEvent, source::coinbase::decode_message};
use rust_decimal::Decimal;
use serde_json::json;

#[test]
fn decodes_level_two_snapshot_without_floating_point() -> Result<()> {
    let event = decode_message(
        &json!({
            "type": "snapshot",
            "product_id": "BTC-USD",
            "bids": [["100.10", "1.25"]],
            "asks": [["100.20", "2.50"]]
        }),
        Utc::now(),
    )?
    .context("snapshot is supported")?;

    let MarketEvent::BookSnapshot { bids, asks, .. } = event else {
        bail!("expected snapshot");
    };
    assert_eq!(bids[0].price, Decimal::new(10010, 2));
    assert_eq!(asks[0].size, Decimal::new(250, 2));
    Ok(())
}

#[test]
fn decodes_heartbeat_sequence_for_gap_detection() -> Result<()> {
    let event = decode_message(
        &json!({
            "type": "heartbeat",
            "sequence": 90,
            "last_trade_id": 20,
            "product_id": "BTC-USD",
            "time": "2026-07-15T20:00:00Z"
        }),
        Utc::now(),
    )?
    .context("heartbeat is supported")?;

    let MarketEvent::Heartbeat { sequence, .. } = event else {
        bail!("expected heartbeat");
    };
    assert_eq!(sequence, 90);
    Ok(())
}

#[test]
fn decodes_match_side_as_the_maker_side() -> Result<()> {
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
    )?
    .context("match is supported")?;

    let MarketEvent::Trade { maker_side, .. } = event else {
        bail!("expected trade");
    };
    assert_eq!(maker_side, market_squawk::domain::Side::Sell);
    Ok(())
}

#[test]
fn rejects_an_invalid_present_exchange_timestamp() -> Result<()> {
    let Err(error) = decode_message(
        &json!({
            "type": "l2update",
            "product_id": "BTC-USD",
            "changes": [["buy", "100.00", "1.00"]],
            "time": "not-a-timestamp"
        }),
        Utc::now(),
    ) else {
        bail!("invalid timestamps must fail decoding");
    };

    assert!(error.to_string().contains("invalid RFC 3339 timestamp"));
    Ok(())
}
