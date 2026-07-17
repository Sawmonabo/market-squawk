use std::{num::NonZeroUsize, time::Duration};

use anyhow::{Context, Result, anyhow};
use futures_util::{SinkExt, StreamExt};
use market_squawk::{
    DiagnosticMarketEvent as MarketEvent,
    journal::JournalReader,
    source::{CaptureContext, MarketSource, coinbase::CoinbaseSource},
};
use market_squawk_domain::{
    CaptureAuthorityIdentity, ConnectionGeneration, MetadataRevision, SourceId, SourceIdentifier,
};
use market_squawk_platform::{
    CaptureShutdownStatus, CaptureWriterPolicy, DiagnosticCaptureBundle, LocalPaths,
    MemoryCaptureSink, raw_capture_channel, spawn_capture_writer,
};
use serde_json::Value;
use tempfile::tempdir;
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;

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

        let saw_client_close = loop {
            match socket.next().await {
                Some(Ok(Message::Close(_))) => break true,
                Some(Ok(Message::Ping(payload))) => {
                    socket.send(Message::Pong(payload)).await?;
                }
                Some(Ok(_)) => {}
                Some(Err(_)) | None => break false,
            }
        };
        Ok::<bool, anyhow::Error>(saw_client_close)
    });

    let directory = tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("data"))?;
    let journal_path = paths.journal_write_file("coinbase-exchange")?;
    let identity = CaptureAuthorityIdentity::new(
        SourceId::try_from("coinbase-exchange")?,
        MetadataRevision::new(SourceIdentifier::try_from("test-v1")?),
        SourceIdentifier::try_from("test-session")?,
        ConnectionGeneration::new(1)?,
    );
    let connection_id = uuid::Uuid::new_v4();
    let (publisher, mut control, writer) = raw_capture_channel(
        NonZeroUsize::new(32).ok_or_else(|| anyhow::anyhow!("invalid test capacity"))?,
        DiagnosticCaptureBundle::new(identity.clone()),
    );
    let capture_handle = spawn_capture_writer(
        writer,
        paths.open_journal_writer("coinbase-exchange")?,
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;
    let (event_sender, mut event_receiver) = mpsc::channel(32);
    let cancellation = CancellationToken::new();
    let source_cancellation = cancellation.clone();
    let mut source: Box<dyn MarketSource> = Box::new(
        CoinbaseSource::new(vec!["BTC-USD".to_owned()]).with_url(format!("ws://{address}")),
    );

    let source_task = tokio::spawn(async move {
        source
            .run_session(
                CaptureContext::new(publisher, identity, connection_id),
                event_sender,
                source_cancellation,
            )
            .await
    });
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

    cancellation.cancel();
    source_task.await??;
    let mut pending_capture = capture_handle.shutdown(Duration::from_secs(2));
    assert_eq!(
        pending_capture.wait_until_deadline().await,
        CaptureShutdownStatus::WorkerTerminated
    );
    let capture_termination = pending_capture
        .try_reap()?
        .ok_or_else(|| anyhow!("terminated capture worker did not retain a final report"))?;
    assert!(!capture_termination.outcome().is_incomplete());
    let saw_client_close = server.await??;
    assert!(
        !saw_client_close,
        "client cancellation must immediately drop the transport without awaiting a close write"
    );

    let records = JournalReader::open(&journal_path)?.read_all()?;
    assert_eq!(records.len(), 2);
    assert!(records[0].payload().starts_with(b"{\"type\":\"snapshot\""));
    assert!(records[1].payload().starts_with(b"{\"type\":\"heartbeat\""));
    Ok(())
}

fn test_capture() -> Result<(
    CaptureContext,
    market_squawk_platform::CaptureWriterHandle<DiagnosticCaptureBundle>,
)> {
    let identity = CaptureAuthorityIdentity::new(
        SourceId::try_from("coinbase-exchange")?,
        MetadataRevision::new(SourceIdentifier::try_from("test-v1")?),
        SourceIdentifier::try_from("test-session")?,
        ConnectionGeneration::new(1)?,
    );
    let connection_id = uuid::Uuid::new_v4();
    let (publisher, mut control, writer) = raw_capture_channel(
        NonZeroUsize::new(8).ok_or_else(|| anyhow!("invalid test capacity"))?,
        DiagnosticCaptureBundle::new(identity.clone()),
    );
    let handle = spawn_capture_writer(
        writer,
        MemoryCaptureSink::default(),
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;
    Ok((
        CaptureContext::new(publisher, identity, connection_id),
        handle,
    ))
}

async fn shutdown_memory_capture(
    handle: market_squawk_platform::CaptureWriterHandle<DiagnosticCaptureBundle>,
) -> Result<()> {
    let mut pending = handle.shutdown(Duration::from_secs(1));
    assert_eq!(
        pending.wait_until_deadline().await,
        CaptureShutdownStatus::WorkerTerminated
    );
    let report = pending
        .try_reap()?
        .ok_or_else(|| anyhow!("capture worker did not retain its termination report"))?;
    assert!(!report.outcome().is_incomplete());
    Ok(())
}

#[tokio::test]
async fn cancellation_preempts_a_full_source_status_channel() -> Result<()> {
    let (capture, handle) = test_capture()?;
    let (events, _receiver) = mpsc::channel(1);
    events
        .send(MarketEvent::SourceStatus {
            source: "occupied".to_owned(),
            status: "occupied".to_owned(),
            detail: None,
            received_at: chrono::Utc::now(),
        })
        .await?;
    let cancellation = CancellationToken::new();
    let mut source = CoinbaseSource::new(vec!["BTC-USD".to_owned()]);
    let task_cancellation = cancellation.clone();
    let source_task =
        tokio::spawn(async move { source.run_session(capture, events, task_cancellation).await });
    tokio::task::yield_now().await;

    cancellation.cancel();
    let outcome = tokio::time::timeout(Duration::from_secs(1), source_task).await???;

    assert_eq!(outcome, market_squawk::source::SourceRunOutcome::Cancelled);
    shutdown_memory_capture(handle).await?;
    Ok(())
}

#[tokio::test]
async fn cancellation_preempts_a_stalled_websocket_handshake() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (accepted_sender, accepted_receiver) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (_stream, _) = listener.accept().await?;
        let _ = accepted_sender.send(());
        std::future::pending::<Result<()>>().await
    });
    let (capture, handle) = test_capture()?;
    let (events, mut receiver) = mpsc::channel(4);
    let cancellation = CancellationToken::new();
    let mut source =
        CoinbaseSource::new(vec!["BTC-USD".to_owned()]).with_url(format!("ws://{address}"));
    let task_cancellation = cancellation.clone();
    let source_task =
        tokio::spawn(async move { source.run_session(capture, events, task_cancellation).await });
    let status = receiver.recv().await.context("connecting status")?;
    assert!(matches!(
        status,
        MarketEvent::SourceStatus { ref status, .. } if status == "connecting"
    ));
    accepted_receiver.await?;

    cancellation.cancel();
    let outcome = tokio::time::timeout(Duration::from_secs(1), source_task).await???;

    assert_eq!(outcome, market_squawk::source::SourceRunOutcome::Cancelled);
    server.abort();
    let join_error = server
        .await
        .err()
        .context("server unexpectedly completed")?;
    assert!(join_error.is_cancelled());
    shutdown_memory_capture(handle).await?;
    Ok(())
}
