use anyhow::{Context, Result};
use chrono::Utc;
use market_squawk::{domain::RawEnvelope, journal::JournalWriter, replay::replay_coinbase_journal};
use tempfile::tempdir;
use uuid::Uuid;

#[test]
fn coinbase_journal_replay_rebuilds_the_order_book() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("coinbase.msj");
    let connection_id = Uuid::new_v4();
    let received_at = Utc::now();
    let payload = br#"{
        "type":"snapshot",
        "product_id":"BTC-USD",
        "bids":[["100.00","2.00"]],
        "asks":[["101.00","3.00"]]
    }"#
    .to_vec();

    let mut writer = JournalWriter::open(&path)?;
    writer.append(&RawEnvelope {
        event_id: Uuid::new_v4(),
        source: "coinbase-exchange".to_owned(),
        connection_id,
        source_sequence: None,
        exchange_at: None,
        received_at,
        payload,
    })?;
    writer.flush()?;

    let replay = replay_coinbase_journal(&path, 5_000, false)?;
    let product = replay
        .snapshot
        .products
        .get("BTC-USD")
        .context("product snapshot")?;
    let top = product.top.as_ref().context("top of book")?;

    assert_eq!(top.bid.to_string(), "100.00");
    assert_eq!(top.ask.to_string(), "101.00");
    assert_eq!(replay.summary.records, 1);
    Ok(())
}
