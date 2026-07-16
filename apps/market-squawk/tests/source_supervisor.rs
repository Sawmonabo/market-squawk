use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use market_squawk::{
    MarketEvent,
    source::{CaptureContext, MarketSource, SourceRunOutcome},
    source_supervisor::SourceSupervisor,
};
use market_squawk_domain::{
    CaptureIntegrityState, ConnectionGeneration, MetadataRevision, SourceId, SourceIdentifier,
};
use market_squawk_platform::{
    CaptureAdmissionReceipt, CaptureGenerationKey, CaptureWriterHandle, CaptureWriterPolicy,
    MemoryCaptureSink, RawCaptureControl, RawCapturePublisher, raw_capture_channel,
    spawn_capture_writer,
};
use tokio::sync::{mpsc, watch};
use uuid::Uuid;

#[derive(Debug)]
struct ReconnectOnceSource {
    sessions: usize,
    receipts: Arc<std::sync::Mutex<Vec<CaptureAdmissionReceipt>>>,
}

#[async_trait]
impl MarketSource for ReconnectOnceSource {
    async fn run_session(
        &mut self,
        capture: CaptureContext,
        _events: mpsc::Sender<MarketEvent>,
        _cancel: watch::Receiver<bool>,
    ) -> anyhow::Result<SourceRunOutcome> {
        self.sessions = self.sessions.saturating_add(1);
        let receipt = capture.publish(
            Uuid::new_v4(),
            Arc::from("source-a"),
            Some(u64::try_from(self.sessions)?),
            None,
            Utc::now(),
            Bytes::from_static(b"{}"),
        )?;
        match self.receipts.lock() {
            Ok(mut receipts) => receipts.push(receipt),
            Err(poisoned) => poisoned.into_inner().push(receipt),
        }
        Ok(if self.sessions == 1 {
            SourceRunOutcome::ReconnectRequired
        } else {
            SourceRunOutcome::Completed
        })
    }
}

fn initial_key() -> Result<CaptureGenerationKey, Box<dyn std::error::Error>> {
    Ok(CaptureGenerationKey::new(
        SourceId::try_from("source-a")?,
        MetadataRevision::new(SourceIdentifier::try_from("revision-a")?),
        SourceIdentifier::try_from("session-a")?,
        ConnectionGeneration::new(1)?,
        Uuid::new_v4(),
    ))
}

fn activated_capture() -> Result<
    (
        RawCapturePublisher,
        RawCaptureControl,
        CaptureWriterHandle,
        CaptureGenerationKey,
    ),
    Box<dyn std::error::Error>,
> {
    let key = initial_key()?;
    let capacity = NonZeroUsize::new(8).ok_or("invalid fixed test capacity")?;
    let (publisher, mut control, writer) = raw_capture_channel(capacity, key.clone());
    let writer_handle = spawn_capture_writer(
        writer,
        MemoryCaptureSink::default(),
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial(&key)?;
    Ok((publisher, control, writer_handle, key))
}

#[derive(Debug)]
struct ImmediateOutcomeSource(SourceRunOutcome);

#[async_trait]
impl MarketSource for ImmediateOutcomeSource {
    async fn run_session(
        &mut self,
        _capture: CaptureContext,
        _events: mpsc::Sender<MarketEvent>,
        _cancel: watch::Receiver<bool>,
    ) -> anyhow::Result<SourceRunOutcome> {
        Ok(self.0)
    }
}

#[derive(Debug)]
struct ErrorSource;

#[async_trait]
impl MarketSource for ErrorSource {
    async fn run_session(
        &mut self,
        _capture: CaptureContext,
        _events: mpsc::Sender<MarketEvent>,
        _cancel: watch::Receiver<bool>,
    ) -> anyhow::Result<SourceRunOutcome> {
        Err(anyhow::anyhow!("deterministic source failure"))
    }
}

#[derive(Debug)]
struct HangingSource;

#[async_trait]
impl MarketSource for HangingSource {
    async fn run_session(
        &mut self,
        _capture: CaptureContext,
        _events: mpsc::Sender<MarketEvent>,
        _cancel: watch::Receiver<bool>,
    ) -> anyhow::Result<SourceRunOutcome> {
        std::future::pending().await
    }
}

#[tokio::test]
async fn only_the_supervisor_rotates_after_a_typed_reconnect_outcome()
-> Result<(), Box<dyn std::error::Error>> {
    let key = initial_key()?;
    let capacity = NonZeroUsize::new(8).ok_or("invalid fixed test capacity")?;
    let (publisher, mut control, writer) = raw_capture_channel(capacity, key.clone());
    let capture_handle = spawn_capture_writer(
        writer,
        MemoryCaptureSink::default(),
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial(&key)?;
    let receipts = Arc::new(std::sync::Mutex::new(Vec::new()));
    let source: Box<dyn MarketSource> = Box::new(ReconnectOnceSource {
        sessions: 0,
        receipts: Arc::clone(&receipts),
    });
    let supervisor = SourceSupervisor::new(publisher.clone(), control, key.clone());
    let (events, _event_receiver) = mpsc::channel(8);
    let (_cancel_sender, cancel) = watch::channel(false);

    tokio::time::timeout(
        Duration::from_secs(3),
        supervisor.run(source, events, cancel),
    )
    .await??;

    let current = publisher.key()?;
    assert_eq!(current.generation().get(), 2);
    assert_ne!(current.connection_id(), key.connection_id());
    {
        let receipts = match receipts.lock() {
            Ok(receipts) => receipts,
            Err(poisoned) => poisoned.into_inner(),
        };
        assert_eq!(receipts.len(), 2);
        assert!(!receipts[0].allocation_is_healthy());
        assert!(!receipts[1].allocation_is_healthy());
    }
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Incomplete);
    let outcome = capture_handle.shutdown(Duration::from_secs(1)).await;
    assert!(!outcome.is_incomplete());
    Ok(())
}

#[tokio::test]
async fn normal_and_cancelled_source_completion_invalidate_the_active_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    for outcome in [SourceRunOutcome::Completed, SourceRunOutcome::Cancelled] {
        let (publisher, control, writer_handle, key) = activated_capture()?;
        let supervisor = SourceSupervisor::new(publisher.clone(), control, key);
        let (events, _event_receiver) = mpsc::channel(1);
        let (_cancel_sender, cancel) = watch::channel(false);

        supervisor
            .run(Box::new(ImmediateOutcomeSource(outcome)), events, cancel)
            .await?;

        assert_eq!(publisher.integrity(), CaptureIntegrityState::Incomplete);
        assert_eq!(publisher.accounting_invariant_failures(), 0);
        assert!(
            !writer_handle
                .shutdown(Duration::from_secs(1))
                .await
                .is_incomplete()
        );
    }
    Ok(())
}

#[tokio::test]
async fn source_error_invalidates_the_active_allocation() -> Result<(), Box<dyn std::error::Error>>
{
    let (publisher, control, writer_handle, key) = activated_capture()?;
    let supervisor = SourceSupervisor::new(publisher.clone(), control, key);
    let (events, _event_receiver) = mpsc::channel(1);
    let (_cancel_sender, cancel) = watch::channel(false);

    let error = supervisor.run(Box::new(ErrorSource), events, cancel).await;

    assert!(error.is_err());
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Incomplete);
    assert_eq!(publisher.accounting_invariant_failures(), 0);
    assert!(
        !writer_handle
            .shutdown(Duration::from_secs(1))
            .await
            .is_incomplete()
    );
    Ok(())
}

#[tokio::test]
async fn aborting_the_supervisor_invalidates_the_active_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    let (publisher, control, writer_handle, key) = activated_capture()?;
    let supervisor = SourceSupervisor::new(publisher.clone(), control, key);
    let (events, _event_receiver) = mpsc::channel(1);
    let (_cancel_sender, cancel) = watch::channel(false);
    let task = tokio::spawn(supervisor.run(Box::new(HangingSource), events, cancel));
    tokio::task::yield_now().await;
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Healthy);

    task.abort();
    let join_error = task
        .await
        .err()
        .ok_or("aborted task unexpectedly completed")?;

    assert!(join_error.is_cancelled());
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Incomplete);
    assert_eq!(publisher.accounting_invariant_failures(), 0);
    assert!(
        !writer_handle
            .shutdown(Duration::from_secs(1))
            .await
            .is_incomplete()
    );
    Ok(())
}
