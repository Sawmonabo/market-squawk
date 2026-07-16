use chrono::Utc;
use market_engine::{domain::RawEnvelope, journal::JournalWriter, replay::replay_coinbase_journal};
use tempfile::tempdir;
use uuid::Uuid;

#[test]
fn coinbase_journal_replay_rebuilds_the_order_book() {
    let directory = tempdir().expect("temp directory");
    let path = directory.path().join("coinbase.mej");
    let connection_id = Uuid::new_v4();
    let received_at = Utc::now();
    let payload = br#"{
        "type":"snapshot",
        "product_id":"BTC-USD",
        "bids":[["100.00","2.00"]],
        "asks":[["101.00","3.00"]]
    }"#
    .to_vec();

    let mut writer = JournalWriter::open(&path).expect("open writer");
    writer
        .append(&RawEnvelope {
            event_id: Uuid::new_v4(),
            source: "coinbase-exchange".to_owned(),
            connection_id,
            source_sequence: None,
            exchange_at: None,
            received_at,
            payload,
        })
        .expect("append snapshot");
    writer.flush().expect("flush journal");

    let replay = replay_coinbase_journal(&path, 5_000, false).expect("replay journal");
    let product = replay
        .snapshot
        .products
        .get("BTC-USD")
        .expect("product snapshot");
    let top = product.top.as_ref().expect("top of book");

    assert_eq!(top.bid.to_string(), "100.00");
    assert_eq!(top.ask.to_string(), "101.00");
    assert_eq!(replay.summary.records, 1);
}
