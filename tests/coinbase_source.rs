use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use market_engine::{
    domain::MarketEvent,
    journal::{JournalReader, JournalSink},
    source::{MarketSource, coinbase::CoinbaseSource},
};
use serde_json::Value;
use tempfile::tempdir;
use tokio::{
    net::TcpListener,
    sync::{mpsc, watch},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

#[tokio::test]
async fn coinbase_source_journals_and_publishes_local_websocket_messages() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind local websocket server");
    let address = listener.local_addr().expect("local server address");

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept client");
        let mut socket = accept_async(stream).await.expect("websocket handshake");
        let subscription = socket
            .next()
            .await
            .expect("subscription frame")
            .expect("valid subscription frame")
            .into_text()
            .expect("text subscription");
        let subscription: Value = serde_json::from_str(&subscription).expect("subscription JSON");
        assert_eq!(subscription["type"], "subscribe");
        assert_eq!(subscription["product_ids"][0], "BTC-USD");

        socket
            .send(Message::Text(
                r#"{"type":"snapshot","product_id":"BTC-USD","bids":[["100.00","2.00"]],"asks":[["101.00","3.00"]]}"#
                    .into(),
            ))
            .await
            .expect("send snapshot");
        socket
            .send(Message::Text(
                r#"{"type":"heartbeat","sequence":42,"last_trade_id":7,"product_id":"BTC-USD","time":"2026-07-15T20:00:00Z"}"#
                    .into(),
            ))
            .await
            .expect("send heartbeat");

        while let Some(message) = socket.next().await {
            match message.expect("valid client frame") {
                Message::Close(_) => break,
                Message::Ping(payload) => socket
                    .send(Message::Pong(payload))
                    .await
                    .expect("send pong"),
                _ => {}
            }
        }
    });

    let directory = tempdir().expect("temp directory");
    let journal_path = directory.path().join("coinbase.mej");
    let (journal, journal_task) = JournalSink::spawn(&journal_path, 32).expect("journal sink");
    let (event_sender, mut event_receiver) = mpsc::channel(32);
    let (cancel_sender, cancel_receiver) = watch::channel(false);
    let source: Box<dyn MarketSource> = Box::new(
        CoinbaseSource::new(vec!["BTC-USD".to_owned()]).with_url(format!("ws://{address}")),
    );

    let source_task = tokio::spawn(source.run(journal.clone(), event_sender, cancel_receiver));
    let mut saw_snapshot = false;
    let mut saw_heartbeat = false;

    tokio::time::timeout(Duration::from_secs(5), async {
        while !(saw_snapshot && saw_heartbeat) {
            match event_receiver.recv().await.expect("source event") {
                MarketEvent::BookSnapshot { product, .. } => {
                    assert_eq!(product, "BTC-USD");
                    saw_snapshot = true;
                }
                MarketEvent::Heartbeat {
                    product, sequence, ..
                } => {
                    assert_eq!(product, "BTC-USD");
                    assert_eq!(sequence, 42);
                    saw_heartbeat = true;
                }
                _ => {}
            }
        }
    })
    .await
    .expect("source messages arrived before timeout");

    cancel_sender.send(true).expect("cancel source");
    source_task
        .await
        .expect("source task join")
        .expect("source stops cleanly");
    journal.shutdown().await.expect("journal shutdown");
    journal_task
        .await
        .expect("journal task join")
        .expect("journal writer stops cleanly");
    server.await.expect("server task join");

    let records = JournalReader::open(&journal_path)
        .expect("open journal")
        .read_all()
        .expect("read journal");
    assert_eq!(records.len(), 2);
    assert!(records[0].payload.starts_with(b"{\"type\":\"snapshot\""));
    assert!(records[1].payload.starts_with(b"{\"type\":\"heartbeat\""));
}
