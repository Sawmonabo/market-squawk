use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use market_squawk::{
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
async fn coinbase_source_journals_and_publishes_local_websocket_messages() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let mut socket = accept_async(stream).await?;
        let subscription = socket
            .next()
            .await
            .context("subscription frame")??
            .into_text()?;
        let subscription: Value = serde_json::from_str(&subscription)?;
        assert_eq!(subscription["type"], "subscribe");
        assert_eq!(subscription["product_ids"][0], "BTC-USD");

        socket
            .send(Message::Text(
                r#"{"type":"snapshot","product_id":"BTC-USD","bids":[["100.00","2.00"]],"asks":[["101.00","3.00"]]}"#
                    .into(),
            ))
            .await?;
        socket
            .send(Message::Text(
                r#"{"type":"heartbeat","sequence":42,"last_trade_id":7,"product_id":"BTC-USD","time":"2026-07-15T20:00:00Z"}"#
                    .into(),
            ))
            .await?;

        while let Some(message) = socket.next().await {
            match message? {
                Message::Close(_) => break,
                Message::Ping(payload) => socket.send(Message::Pong(payload)).await?,
                _ => {}
            }
        }
        Ok::<(), anyhow::Error>(())
    });

    let directory = tempdir()?;
    let journal_path = directory.path().join("coinbase.msj");
    let (journal, journal_task) = JournalSink::spawn(&journal_path, 32)?;
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
            match event_receiver.recv().await.context("source event")? {
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
        Ok::<(), anyhow::Error>(())
    })
    .await
    .context("source messages arrived before timeout")??;

    cancel_sender.send(true)?;
    source_task.await??;
    journal.shutdown().await?;
    journal_task.await??;
    server.await??;

    let records = JournalReader::open(&journal_path)?.read_all()?;
    assert_eq!(records.len(), 2);
    assert!(records[0].payload.starts_with(b"{\"type\":\"snapshot\""));
    assert!(records[1].payload.starts_with(b"{\"type\":\"heartbeat\""));
    Ok(())
}
