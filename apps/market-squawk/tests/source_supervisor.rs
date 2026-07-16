use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use market_squawk::{
    DiagnosticMarketEvent as MarketEvent,
    source::{CaptureContext, MarketSource, SourceRunOutcome},
    source_supervisor::SourceSupervisor,
};
use market_squawk_domain::{
    CaptureAuthorityIdentity, CaptureIntegrityState, ConnectionGeneration, MetadataRevision,
    SourceId, SourceIdentifier,
};
use market_squawk_platform::{
    CaptureShutdownStatus, CaptureWorkerTermination, CaptureWriterHandle, CaptureWriterPolicy,
    DiagnosticCaptureBundle, DiagnosticCaptureReceipt, MemoryCaptureSink, RawCaptureControl,
    RawCapturePublisher, raw_capture_channel, spawn_capture_writer,
};
use tokio::sync::{mpsc, watch};
use uuid::Uuid;

#[derive(Debug)]
struct ReconnectOnceSource {
    sessions: usize,
    receipts: Arc<std::sync::Mutex<Vec<DiagnosticCaptureReceipt>>>,
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

fn initial_identity() -> Result<(CaptureAuthorityIdentity, Uuid), Box<dyn std::error::Error>> {
    Ok((
        CaptureAuthorityIdentity::new(
            SourceId::try_from("source-a")?,
            MetadataRevision::new(SourceIdentifier::try_from("revision-a")?),
            SourceIdentifier::try_from("session-a")?,
            ConnectionGeneration::new(1)?,
        ),
        Uuid::new_v4(),
    ))
}

#[derive(Debug)]
struct ActivatedCapture {
    publisher: RawCapturePublisher<DiagnosticCaptureBundle>,
    control: RawCaptureControl<DiagnosticCaptureBundle>,
    writer_handle: CaptureWriterHandle<DiagnosticCaptureBundle>,
    identity: CaptureAuthorityIdentity,
    connection_id: Uuid,
}

fn activated_capture() -> Result<ActivatedCapture, Box<dyn std::error::Error>> {
    let (identity, connection_id) = initial_identity()?;
    let capacity = NonZeroUsize::new(8).ok_or("invalid fixed test capacity")?;
    let (publisher, mut control, writer) =
        raw_capture_channel(capacity, DiagnosticCaptureBundle::new(identity.clone()));
    let writer_handle = spawn_capture_writer(
        writer,
        MemoryCaptureSink::default(),
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;
    Ok(ActivatedCapture {
        publisher,
        control,
        writer_handle,
        identity,
        connection_id,
    })
}

async fn shutdown_and_reap(
    handle: CaptureWriterHandle<DiagnosticCaptureBundle>,
) -> Result<CaptureWorkerTermination, Box<dyn std::error::Error>> {
    let mut pending = handle.shutdown(Duration::from_secs(1));
    if pending.wait_until_deadline().await != CaptureShutdownStatus::WorkerTerminated {
        return Err("capture worker exceeded the fixed test deadline".into());
    }
    pending
        .try_reap()?
        .cloned()
        .ok_or_else(|| "terminated capture worker did not retain a final report".into())
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
    let (identity, connection_id) = initial_identity()?;
    let capacity = NonZeroUsize::new(8).ok_or("invalid fixed test capacity")?;
    let (publisher, mut control, writer) =
        raw_capture_channel(capacity, DiagnosticCaptureBundle::new(identity.clone()));
    let capture_handle = spawn_capture_writer(
        writer,
        MemoryCaptureSink::default(),
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;
    let receipts = Arc::new(std::sync::Mutex::new(Vec::new()));
    let source: Box<dyn MarketSource> = Box::new(ReconnectOnceSource {
        sessions: 0,
        receipts: Arc::clone(&receipts),
    });
    let supervisor =
        SourceSupervisor::new(publisher.clone(), control, identity.clone(), connection_id);
    let (events, _event_receiver) = mpsc::channel(8);
    let (_cancel_sender, cancel) = watch::channel(false);

    tokio::time::timeout(
        Duration::from_secs(3),
        supervisor.run(source, events, cancel),
    )
    .await??;

    let current = publisher.identity();
    assert_eq!(current.connection_generation().get(), 2);
    {
        let receipts = match receipts.lock() {
            Ok(receipts) => receipts,
            Err(poisoned) => poisoned.into_inner(),
        };
        assert_eq!(receipts.len(), 2);
        assert!(!receipts[0].generation_is_complete());
        assert!(!receipts[1].generation_is_complete());
    }
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Incomplete);
    let termination = shutdown_and_reap(capture_handle).await?;
    assert!(!termination.outcome().is_incomplete());
    Ok(())
}

#[tokio::test]
async fn normal_and_cancelled_source_completion_invalidate_the_active_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    for outcome in [SourceRunOutcome::Completed, SourceRunOutcome::Cancelled] {
        let ActivatedCapture {
            publisher,
            control,
            writer_handle,
            identity,
            connection_id,
        } = activated_capture()?;
        let supervisor = SourceSupervisor::new(publisher.clone(), control, identity, connection_id);
        let (events, _event_receiver) = mpsc::channel(1);
        let (_cancel_sender, cancel) = watch::channel(false);

        supervisor
            .run(Box::new(ImmediateOutcomeSource(outcome)), events, cancel)
            .await?;

        assert_eq!(publisher.integrity(), CaptureIntegrityState::Incomplete);
        assert_eq!(publisher.accounting_invariant_failures(), 0);
        assert!(
            !shutdown_and_reap(writer_handle)
                .await?
                .outcome()
                .is_incomplete()
        );
    }
    Ok(())
}

#[tokio::test]
async fn source_error_invalidates_the_active_allocation() -> Result<(), Box<dyn std::error::Error>>
{
    let ActivatedCapture {
        publisher,
        control,
        writer_handle,
        identity,
        connection_id,
    } = activated_capture()?;
    let supervisor = SourceSupervisor::new(publisher.clone(), control, identity, connection_id);
    let (events, _event_receiver) = mpsc::channel(1);
    let (_cancel_sender, cancel) = watch::channel(false);

    let error = supervisor.run(Box::new(ErrorSource), events, cancel).await;

    assert!(error.is_err());
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Incomplete);
    assert_eq!(publisher.accounting_invariant_failures(), 0);
    assert!(
        !shutdown_and_reap(writer_handle)
            .await?
            .outcome()
            .is_incomplete()
    );
    Ok(())
}

#[tokio::test]
async fn aborting_the_supervisor_invalidates_the_active_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    let ActivatedCapture {
        publisher,
        control,
        writer_handle,
        identity,
        connection_id,
    } = activated_capture()?;
    let supervisor = SourceSupervisor::new(publisher.clone(), control, identity, connection_id);
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
        !shutdown_and_reap(writer_handle)
            .await?
            .outcome()
            .is_incomplete()
    );
    Ok(())
}
