use anyhow::{Context, Result};
use chrono::Utc;
use market_squawk::{AppPaths, DiagnosticRawEnvelope, replay::replay_coinbase_journal};
use tempfile::tempdir;
use uuid::Uuid;

#[test]
fn coinbase_journal_replay_rebuilds_the_order_book() -> Result<()> {
    let directory = tempdir()?;
    let paths = AppPaths::prepare(directory.path().join("data"))?;
    let path = paths.journal_write_file("coinbase-exchange")?;
    let connection_id = Uuid::new_v4();
    let received_at = Utc::now();
    let payload = br#"{
        "type":"snapshot",
        "product_id":"BTC-USD",
        "bids":[["100.00","2.00"]],
        "asks":[["101.00","3.00"]]
    }"#
    .to_vec();

    let mut writer = paths.open_journal_writer("coinbase-exchange")?;
    writer.append(&DiagnosticRawEnvelope::try_from_compatibility_parts(
        Uuid::new_v4(),
        "coinbase-exchange".to_owned(),
        connection_id,
        None,
        None,
        received_at,
        payload,
    )?)?;
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
