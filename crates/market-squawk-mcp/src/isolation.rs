//! Owned bounded bridges around the payload-suppressing official-SDK runtime.

use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc::{SyncSender, TrySendError, sync_channel},
    },
    thread::JoinHandle as ThreadJoinHandle,
    time::Duration,
};

use async_trait::async_trait;
use market_squawk_services::{
    ProgressDelivery as ServiceProgressDelivery, ProgressError, ProgressSink, RequestContext,
    ServiceCapabilities, ServiceError, ToolServices, TypedToolRequest, TypedToolResult,
};
use rmcp::{
    RoleServer,
    model::{
        ClientJsonRpcMessage, Notification, ProgressNotificationParam, ProgressToken,
        ServerJsonRpcMessage, ServerNotification,
    },
    service::{QuitReason, RxJsonRpcMessage, TxJsonRpcMessage, serve_server_with_ct},
    transport::Transport,
};
use thiserror::Error;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
    task::{JoinHandle as TokioJoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;

use crate::{
    ArtifactError, ArtifactPublication, ArtifactPublicationContext, ArtifactReference,
    ArtifactRepository, AuditResultClass, McpLimits,
    framing::OutputChannel,
    protocol::{SdkInboundRequest, TransportError, WriterSupervisor},
    server::{ServerError, ServiceHandler},
};

pub(crate) struct SdkOutbound {
    message: TxJsonRpcMessage<RoleServer>,
    acknowledgement: oneshot::Sender<Result<(), TransportError>>,
}

impl std::fmt::Debug for SdkOutbound {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SdkOutbound")
            .field("message", &"[PROTOCOL MESSAGE REDACTED]")
            .field("acknowledgement", &"[DELIVERY ACKNOWLEDGEMENT]")
            .finish()
    }
}

/// Minimal bounded channel transport used only inside the isolated official SDK runtime.
pub(crate) struct SdkTransport {
    incoming: Option<mpsc::Sender<SdkInboundRequest>>,
    pending_incoming: Option<oneshot::Receiver<Option<ClientJsonRpcMessage>>>,
    outgoing: Option<mpsc::Sender<SdkOutbound>>,
    limits: McpLimits,
}

impl std::fmt::Debug for SdkTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SdkTransport")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

pub(crate) fn sdk_transport(
    limits: McpLimits,
) -> (
    SdkTransport,
    mpsc::Receiver<SdkInboundRequest>,
    mpsc::Receiver<SdkOutbound>,
) {
    let (incoming, incoming_rx) = mpsc::channel(limits.maximum_active_requests());
    let (outgoing, outgoing_rx) = mpsc::channel(limits.writer_queue_capacity());
    (
        SdkTransport {
            incoming: Some(incoming),
            pending_incoming: None,
            outgoing: Some(outgoing),
            limits,
        },
        incoming_rx,
        outgoing_rx,
    )
}

pub(crate) async fn run_sdk_output(
    output: Arc<OutputChannel>,
    mut outgoing: mpsc::Receiver<SdkOutbound>,
) {
    while let Some(outbound) = outgoing.recv().await {
        let result = output.send_message(outbound.message).await;
        let failed = result.is_err();
        let _ = outbound.acknowledgement.send(result);
        if failed {
            return;
        }
    }
}

impl Transport<RoleServer> for SdkTransport {
    type Error = TransportError;

    fn send(
        &mut self,
        item: TxJsonRpcMessage<RoleServer>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        let outgoing = self.outgoing.clone();
        let timeout = self.limits.write_timeout();
        async move {
            let outgoing = outgoing.ok_or(TransportError::PeerClosed)?;
            let deadline = tokio::time::Instant::now()
                .checked_add(timeout)
                .ok_or(TransportError::InvalidLimit)?;
            let permit = tokio::time::timeout_at(deadline, outgoing.reserve_owned())
                .await
                .map_err(|_| TransportError::WriteTimedOut)?
                .map_err(|_| TransportError::PeerClosed)?;
            let (acknowledgement, delivered) = oneshot::channel();
            permit.send(SdkOutbound {
                message: item,
                acknowledgement,
            });
            tokio::time::timeout_at(deadline, delivered)
                .await
                .map_err(|_| TransportError::WriteTimedOut)?
                .map_err(|_| TransportError::WriterTask)?
        }
    }

    async fn receive(&mut self) -> Option<RxJsonRpcMessage<RoleServer>> {
        if self.pending_incoming.is_none() {
            let incoming = self.incoming.as_ref()?;
            let (response, message) = oneshot::channel();
            incoming.send(SdkInboundRequest { response }).await.ok()?;
            self.pending_incoming = Some(message);
        }
        let result = match self.pending_incoming.as_mut() {
            Some(message) => message.await.ok().flatten(),
            None => return None,
        };
        self.pending_incoming.take();
        result
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        self.incoming.take();
        self.outgoing.take();
        Ok(())
    }
}

pub(crate) struct ServiceCall {
    request: TypedToolRequest,
    context: RequestContext,
    response: oneshot::Sender<Result<TypedToolResult, ServiceError>>,
    ownership: OwnedSemaphorePermit,
}

#[derive(Clone)]
pub(crate) struct SdkToolServices {
    pub(crate) capabilities: ServiceCapabilities,
    pub(crate) calls: mpsc::Sender<ServiceCall>,
    pub(crate) ownership: Arc<Semaphore>,
}

impl std::fmt::Debug for SdkToolServices {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SdkToolServices")
            .field("capabilities", &self.capabilities)
            .field("calls", &"[BOUNDED SERVICE CHANNEL]")
            .field("ownership", &"[BOUNDED HOST WORK OWNERSHIP]")
            .finish()
    }
}

#[async_trait]
impl ToolServices for SdkToolServices {
    fn capabilities(&self) -> ServiceCapabilities {
        self.capabilities.clone()
    }

    async fn call(
        &self,
        request: TypedToolRequest,
        context: RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let cancellation = context.cancellation().clone();
        let deadline = tokio::time::Instant::from_std(context.deadline());
        let ownership = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(ServiceError::Cancelled),
            () = tokio::time::sleep_until(deadline) => {
                return Err(ServiceError::DeadlineExceeded);
            }
            permit = Arc::clone(&self.ownership).acquire_owned() => {
                permit.map_err(|_| ServiceError::Unavailable)?
            }
        };
        let sender = self.calls.clone();
        let permit = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(ServiceError::Cancelled),
            () = tokio::time::sleep_until(deadline) => {
                return Err(ServiceError::DeadlineExceeded);
            }
            permit = sender.reserve_owned() => {
                permit.map_err(|_| ServiceError::Unavailable)?
            }
        };
        let (response, result) = oneshot::channel();
        permit.send(ServiceCall {
            request,
            context,
            response,
            ownership,
        });
        tokio::select! {
            biased;
            () = cancellation.cancelled() => Err(ServiceError::Cancelled),
            () = tokio::time::sleep_until(deadline) => Err(ServiceError::DeadlineExceeded),
            outcome = result => outcome.map_err(|_| ServiceError::Unavailable)?,
        }
    }
}

pub(crate) async fn run_service_calls<S: ToolServices>(
    services: Arc<S>,
    mut receiver: mpsc::Receiver<ServiceCall>,
    cancellation: CancellationToken,
) {
    let mut active = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => break,
            joined = active.join_next(), if !active.is_empty() => {
                let _ = joined;
            }
            call = receiver.recv() => {
                let Some(call) = call else {
                    break;
                };
                let services = Arc::clone(&services);
                active.spawn(async move {
                    let ServiceCall {
                        request,
                        context,
                        response,
                        ownership,
                    } = call;
                    let result = services.call(request, context).await;
                    drop(ownership);
                    let _ = response.send(result);
                });
            }
        }
    }
    drop(receiver);
    while active.join_next().await.is_some() {}
}

pub(crate) struct ArtifactCall {
    publication: ArtifactPublication,
    context: ArtifactPublicationContext,
    response: oneshot::Sender<Result<ArtifactReference, ArtifactError>>,
    ownership: OwnedSemaphorePermit,
}

#[derive(Clone)]
pub(crate) struct SdkArtifactRepository {
    pub(crate) publications: mpsc::Sender<ArtifactCall>,
    pub(crate) ownership: Arc<Semaphore>,
}

impl std::fmt::Debug for SdkArtifactRepository {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SdkArtifactRepository")
            .field("publications", &"[BOUNDED ARTIFACT CHANNEL]")
            .field("ownership", &"[BOUNDED HOST WORK OWNERSHIP]")
            .finish()
    }
}

#[async_trait]
impl ArtifactRepository for SdkArtifactRepository {
    async fn publish(
        &self,
        publication: ArtifactPublication,
        context: ArtifactPublicationContext,
    ) -> Result<ArtifactReference, ArtifactError> {
        context.ensure_live()?;
        let deadline = tokio::time::Instant::from_std(context.deadline());
        let cancellation = context.cancellation().clone();
        let ownership = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(ArtifactError::Cancelled),
            () = tokio::time::sleep_until(deadline) => {
                return Err(ArtifactError::DeadlineExceeded);
            }
            permit = Arc::clone(&self.ownership).acquire_owned() => {
                permit.map_err(|_| ArtifactError::Unavailable)?
            }
        };
        let sender = self.publications.clone();
        let permit = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(ArtifactError::Cancelled),
            () = tokio::time::sleep_until(deadline) => {
                return Err(ArtifactError::DeadlineExceeded);
            }
            permit = sender.reserve_owned() => {
                permit.map_err(|_| ArtifactError::Unavailable)?
            }
        };
        let (response, result) = oneshot::channel();
        permit.send(ArtifactCall {
            publication,
            context,
            response,
            ownership,
        });
        tokio::select! {
            biased;
            () = cancellation.cancelled() => Err(ArtifactError::Cancelled),
            () = tokio::time::sleep_until(deadline) => Err(ArtifactError::DeadlineExceeded),
            outcome = result => outcome.map_err(|_| ArtifactError::Unavailable)?,
        }
    }
}

pub(crate) async fn run_artifact_calls(
    artifacts: Arc<dyn ArtifactRepository>,
    mut receiver: mpsc::Receiver<ArtifactCall>,
    cancellation: CancellationToken,
) {
    let mut active = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => break,
            joined = active.join_next(), if !active.is_empty() => {
                let _ = joined;
            }
            call = receiver.recv() => {
                let Some(call) = call else {
                    break;
                };
                let artifacts = Arc::clone(&artifacts);
                active.spawn(async move {
                    let ArtifactCall {
                        publication,
                        context,
                        response,
                        ownership,
                    } = call;
                    let result = artifacts.publish(publication, context).await;
                    drop(ownership);
                    let _ = response.send(result);
                });
            }
        }
    }
    drop(receiver);
    while active.join_next().await.is_some() {}
}

#[derive(Clone)]
pub(crate) struct McpProgressSink {
    pub(crate) sender: mpsc::Sender<ProgressDelivery>,
    pub(crate) peer: rmcp::service::Peer<RoleServer>,
    pub(crate) token: ProgressToken,
    pub(crate) limits: McpLimits,
}

impl std::fmt::Debug for McpProgressSink {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("McpProgressSink")
            .field("sender", &"[BOUNDED PROGRESS CHANNEL]")
            .field("peer", &"[MCP PEER]")
            .field("token", &"[PROGRESS TOKEN REDACTED]")
            .field("limits", &self.limits)
            .finish()
    }
}

#[async_trait]
impl ProgressSink for McpProgressSink {
    async fn report(&self, report: ServiceProgressDelivery) -> Result<(), ProgressError> {
        report.ensure_live()?;
        let notification = progress_notification(self.token.clone(), report.update());
        validate_progress_notification(&notification, self.limits)?;
        let write_deadline = tokio::time::Instant::now()
            .checked_add(self.limits.write_timeout())
            .ok_or(ProgressError::Delivery)?;
        let deadline = write_deadline.min(tokio::time::Instant::from_std(report.deadline()));
        let permit = tokio::time::timeout_at(deadline, self.sender.clone().reserve_owned())
            .await
            .map_err(|_| ProgressError::Delivery)?
            .map_err(|_| ProgressError::Delivery)?;
        let (acknowledgement, delivered) = oneshot::channel();
        permit.send(ProgressDelivery {
            peer: self.peer.clone(),
            notification,
            report,
            acknowledgement,
        });
        tokio::time::timeout_at(deadline, delivered)
            .await
            .map_err(|_| ProgressError::Delivery)?
            .map_err(|_| ProgressError::Delivery)?
    }
}

pub(crate) struct ProgressDelivery {
    peer: rmcp::service::Peer<RoleServer>,
    notification: ProgressNotificationParam,
    report: ServiceProgressDelivery,
    acknowledgement: oneshot::Sender<Result<(), ProgressError>>,
}

impl std::fmt::Debug for ProgressDelivery {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProgressDelivery")
            .field("peer", &"[MCP PEER]")
            .field("notification", &"[PROGRESS NOTIFICATION REDACTED]")
            .field("report", &"[PROGRESS LIFECYCLE]")
            .field("acknowledgement", &"[DELIVERY ACKNOWLEDGEMENT]")
            .finish()
    }
}

async fn run_sdk_progress(
    mut receiver: mpsc::Receiver<ProgressDelivery>,
    cancellation: CancellationToken,
) {
    loop {
        let delivery = tokio::select! {
            biased;
            () = cancellation.cancelled() => break,
            delivery = receiver.recv() => delivery,
        };
        let Some(delivery) = delivery else {
            break;
        };
        let ProgressDelivery {
            peer,
            notification,
            report,
            acknowledgement,
        } = delivery;
        let result = match report.ensure_live() {
            Ok(()) => tokio::select! {
                biased;
                error = report.ended() => Err(error),
                result = peer.notify_progress(notification) => {
                    result.map_err(|_| ProgressError::Delivery)
                }
            },
            Err(error) => Err(error),
        };
        drop(report);
        let _ = acknowledgement.send(result);
    }
}

fn progress_notification(
    token: ProgressToken,
    update: &market_squawk_services::ProgressUpdate,
) -> ProgressNotificationParam {
    let mut notification = ProgressNotificationParam::new(token, update.completed() as f64);
    if let Some(total) = update.total() {
        notification = notification.with_total(total as f64);
    }
    if let Some(message) = update.message() {
        notification = notification.with_message(message);
    }
    notification
}

fn validate_progress_notification(
    params: &ProgressNotificationParam,
    limits: McpLimits,
) -> Result<(), ProgressError> {
    let notification = Notification::new(params.clone());
    let message =
        ServerJsonRpcMessage::notification(ServerNotification::ProgressNotification(notification));
    let encoded = serde_json::to_vec(&message).map_err(|_| ProgressError::Delivery)?;
    if encoded.len() > limits.maximum_frame_bytes()
        || encoded
            .len()
            .checked_add(1)
            .is_none_or(|framed| framed > limits.maximum_writer_queue_bytes())
    {
        return Err(ProgressError::Delivery);
    }
    Ok(())
}

#[derive(Debug)]
pub(crate) enum IsolatedSdkOutcome {
    RuntimeBuild(std::io::Error),
    InitializeFailed(Box<rmcp::service::ServerInitializeError>),
    Finished(Result<QuitReason, tokio::task::JoinError>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SdkThreadShutdown {
    Joined,
    TransferredToReaper,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SdkReaperDrain {
    Complete,
    Pending,
}

#[derive(Debug, Error)]
pub(crate) enum SdkThreadError {
    #[error("MCP SDK reaper capacity is exhausted")]
    ReaperCapacity,
    #[error("MCP SDK reaper thread could not be spawned")]
    ReaperSpawn(#[source] std::io::Error),
    #[error("MCP SDK worker thread could not be spawned")]
    WorkerSpawn(#[source] std::io::Error),
    #[error("MCP SDK worker thread panicked")]
    WorkerPanicked,
    #[error("MCP SDK reaper is unavailable")]
    ReaperUnavailable,
    #[error("MCP SDK reaper observed a worker panic")]
    ReapedWorkerPanicked,
}

#[derive(Debug)]
struct ReapRequest {
    thread: ThreadJoinHandle<()>,
    _permit: OwnedSemaphorePermit,
}

#[derive(Clone, Debug)]
struct SdkThreadReaperHandle {
    sender: SyncSender<ReapRequest>,
    pending: Arc<AtomicUsize>,
}

impl SdkThreadReaperHandle {
    fn transfer(&self, request: ReapRequest) -> Result<(), ReapRequest> {
        self.pending.fetch_add(1, Ordering::SeqCst);
        match self.sender.try_send(request) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(request) | TrySendError::Disconnected(request)) => {
                self.pending.fetch_sub(1, Ordering::SeqCst);
                Err(request)
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct SdkThreadReaper {
    capacity: Arc<Semaphore>,
    sender: Option<SyncSender<ReapRequest>>,
    pending: Arc<AtomicUsize>,
    failed: Arc<AtomicBool>,
    changed: Arc<tokio::sync::Notify>,
    thread: Option<ThreadJoinHandle<()>>,
}

impl SdkThreadReaper {
    pub(crate) fn try_new(maximum_pending: NonZeroUsize) -> Result<Self, SdkThreadError> {
        let (sender, receiver) = sync_channel::<ReapRequest>(maximum_pending.get());
        let pending = Arc::new(AtomicUsize::new(0));
        let failed = Arc::new(AtomicBool::new(false));
        let changed = Arc::new(tokio::sync::Notify::new());
        let reaper_pending = Arc::clone(&pending);
        let reaper_failed = Arc::clone(&failed);
        let reaper_changed = Arc::clone(&changed);
        let thread = std::thread::Builder::new()
            .name("market-squawk-mcp-sdk-reaper".to_owned())
            .spawn(move || {
                while let Ok(request) = receiver.recv() {
                    if request.thread.join().is_err() {
                        reaper_failed.store(true, Ordering::SeqCst);
                    }
                    drop(request._permit);
                    reaper_pending.fetch_sub(1, Ordering::SeqCst);
                    reaper_changed.notify_waiters();
                }
            })
            .map_err(SdkThreadError::ReaperSpawn)?;
        Ok(Self {
            capacity: Arc::new(Semaphore::new(maximum_pending.get())),
            sender: Some(sender),
            pending,
            failed,
            changed,
            thread: Some(thread),
        })
    }

    fn handle(&self) -> Result<SdkThreadReaperHandle, SdkThreadError> {
        let sender = self
            .sender
            .as_ref()
            .cloned()
            .ok_or(SdkThreadError::ReaperUnavailable)?;
        Ok(SdkThreadReaperHandle {
            sender,
            pending: Arc::clone(&self.pending),
        })
    }

    fn try_reserve(&self) -> Result<OwnedSemaphorePermit, SdkThreadError> {
        Arc::clone(&self.capacity)
            .try_acquire_owned()
            .map_err(|_error| SdkThreadError::ReaperCapacity)
    }

    pub(crate) fn pending_count(&self) -> usize {
        self.pending.load(Ordering::SeqCst)
    }

    pub(crate) async fn drain(&self, timeout: Duration) -> Result<SdkReaperDrain, SdkThreadError> {
        let deadline = tokio::time::Instant::now()
            .checked_add(timeout)
            .ok_or(SdkThreadError::ReaperUnavailable)?;
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.pending_count() == 0 {
                if self.failed.load(Ordering::SeqCst) {
                    return Err(SdkThreadError::ReapedWorkerPanicked);
                }
                return Ok(SdkReaperDrain::Complete);
            }
            if tokio::time::timeout_at(deadline, changed.as_mut())
                .await
                .is_err()
            {
                return Ok(SdkReaperDrain::Pending);
            }
        }
    }
}

impl Drop for SdkThreadReaper {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

pub(crate) struct OwnedSdkThread<T: Send + 'static> {
    cancellation: CancellationToken,
    outcome: oneshot::Receiver<T>,
    thread: Option<ThreadJoinHandle<()>>,
    reaper: SdkThreadReaperHandle,
    reaper_permit: Option<OwnedSemaphorePermit>,
}

impl<T: Send + 'static> std::fmt::Debug for OwnedSdkThread<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OwnedSdkThread")
            .field("thread_owned", &self.thread.is_some())
            .field("reaper_permit_owned", &self.reaper_permit.is_some())
            .finish_non_exhaustive()
    }
}

impl<T> OwnedSdkThread<T>
where
    T: Send + 'static,
{
    pub(crate) fn try_spawn<F>(
        reaper: &SdkThreadReaper,
        name: &str,
        work: F,
    ) -> Result<Self, SdkThreadError>
    where
        F: FnOnce(CancellationToken) -> T + Send + 'static,
    {
        let reaper_permit = reaper.try_reserve()?;
        let reaper = reaper.handle()?;
        let cancellation = CancellationToken::new();
        let worker_cancellation = cancellation.clone();
        let (outcome_sender, outcome) = oneshot::channel();
        let thread = std::thread::Builder::new()
            .name(name.to_owned())
            .spawn(move || {
                let outcome = work(worker_cancellation);
                let _ = outcome_sender.send(outcome);
            })
            .map_err(SdkThreadError::WorkerSpawn)?;
        Ok(Self {
            cancellation,
            outcome,
            thread: Some(thread),
            reaper,
            reaper_permit: Some(reaper_permit),
        })
    }

    pub(crate) async fn wait(mut self) -> Result<T, SdkThreadError> {
        let outcome = (&mut self.outcome)
            .await
            .map_err(|_error| SdkThreadError::WorkerPanicked)?;
        self.join()?;
        Ok(outcome)
    }

    pub(crate) async fn shutdown(
        mut self,
        timeout: Duration,
    ) -> Result<SdkThreadShutdown, SdkThreadError> {
        self.cancellation.cancel();
        match tokio::time::timeout(timeout, &mut self.outcome).await {
            Ok(Ok(_outcome)) => {
                self.join()?;
                Ok(SdkThreadShutdown::Joined)
            }
            Ok(Err(_error)) => {
                self.join()?;
                Err(SdkThreadError::WorkerPanicked)
            }
            Err(_elapsed) => {
                self.transfer_or_join();
                Ok(SdkThreadShutdown::TransferredToReaper)
            }
        }
    }

    fn join(&mut self) -> Result<(), SdkThreadError> {
        let thread = self.thread.take().ok_or(SdkThreadError::WorkerPanicked)?;
        let joined = thread
            .join()
            .map_err(|_panic| SdkThreadError::WorkerPanicked);
        self.reaper_permit.take();
        joined
    }

    fn transfer_or_join(&mut self) {
        let (Some(thread), Some(permit)) = (self.thread.take(), self.reaper_permit.take()) else {
            return;
        };
        let request = ReapRequest {
            thread,
            _permit: permit,
        };
        if let Err(request) = self.reaper.transfer(request) {
            let _ = request.thread.join();
            drop(request._permit);
        }
    }
}

impl<T: Send + 'static> Drop for OwnedSdkThread<T> {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.transfer_or_join();
    }
}

pub(crate) fn run_isolated_sdk(
    handler: ServiceHandler<SdkToolServices>,
    transport: SdkTransport,
    progress_receiver: mpsc::Receiver<ProgressDelivery>,
    cancellation: CancellationToken,
    shutdown_timeout: Duration,
) -> IsolatedSdkOutcome {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => return IsolatedSdkOutcome::RuntimeBuild(error),
    };
    let subscriber = tracing_subscriber::registry();
    tracing::subscriber::with_default(subscriber, || {
        runtime.block_on(async move {
            let progress_cancellation = cancellation.child_token();
            let mut progress_task = tokio::spawn(run_sdk_progress(
                progress_receiver,
                progress_cancellation.clone(),
            ));
            let outcome = match serve_server_with_ct(handler, transport, cancellation).await {
                Ok(running) => IsolatedSdkOutcome::Finished(running.waiting().await),
                Err(error) => IsolatedSdkOutcome::InitializeFailed(Box::new(error)),
            };
            progress_cancellation.cancel();
            if tokio::time::timeout(shutdown_timeout, &mut progress_task)
                .await
                .is_err()
            {
                progress_task.abort();
                let _ = progress_task.await;
            }
            outcome
        })
    })
}

pub(crate) struct SessionSupervisor {
    cancellation: CancellationToken,
    sdk_thread: Option<OwnedSdkThread<IsolatedSdkOutcome>>,
    sdk_reaper: SdkThreadReaper,
    host_tasks: Vec<TokioJoinHandle<()>>,
    writer: WriterSupervisor,
    shutdown_timeout: Duration,
}

impl std::fmt::Debug for SessionSupervisor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SessionSupervisor")
            .field("sdk_thread_owned", &self.sdk_thread.is_some())
            .field("sdk_reaper", &self.sdk_reaper)
            .field("host_task_count", &self.host_tasks.len())
            .field("writer", &self.writer)
            .field("shutdown_timeout", &self.shutdown_timeout)
            .finish_non_exhaustive()
    }
}

impl SessionSupervisor {
    pub(crate) fn new(
        cancellation: CancellationToken,
        sdk_thread: OwnedSdkThread<IsolatedSdkOutcome>,
        sdk_reaper: SdkThreadReaper,
        host_tasks: Vec<TokioJoinHandle<()>>,
        writer: WriterSupervisor,
        shutdown_timeout: Duration,
    ) -> Self {
        Self {
            cancellation,
            sdk_thread: Some(sdk_thread),
            sdk_reaper,
            host_tasks,
            writer,
            shutdown_timeout,
        }
    }

    pub(crate) async fn wait_sdk(&mut self) -> Result<IsolatedSdkOutcome, ServerError> {
        let thread = self.sdk_thread.take().ok_or(ServerError::Transport)?;
        thread.wait().await.map_err(|_error| ServerError::SdkThread)
    }

    pub(crate) async fn shutdown(
        &mut self,
        result_class: AuditResultClass,
        terminal_marker: &'static [u8],
    ) -> Result<(), TransportError> {
        self.cancellation.cancel();
        let sdk_result = match self.sdk_thread.take() {
            Some(thread) => thread
                .shutdown(self.shutdown_timeout)
                .await
                .map(|_shutdown| ())
                .map_err(|_error| TransportError::WriterTask),
            None => Ok(()),
        };
        let reaper_result = match self.sdk_reaper.drain(self.shutdown_timeout).await {
            Ok(SdkReaperDrain::Complete) => Ok(()),
            Ok(SdkReaperDrain::Pending) => Err(TransportError::WriteTimedOut),
            Err(_error) => Err(TransportError::WriterTask),
        };
        let mut host_tasks = std::mem::take(&mut self.host_tasks);
        let host_result = match tokio::time::timeout(self.shutdown_timeout, async {
            let mut joined_cleanly = true;
            for task in &mut host_tasks {
                joined_cleanly &= task.await.is_ok();
            }
            joined_cleanly
        })
        .await
        {
            Ok(true) => Ok(()),
            Ok(false) => Err(TransportError::WriterTask),
            Err(_) => {
                for task in &host_tasks {
                    task.abort();
                }
                for task in &mut host_tasks {
                    let _ = task.await;
                }
                Err(TransportError::WriteTimedOut)
            }
        };
        let writer_result = self.writer.shutdown(result_class, terminal_marker).await;
        sdk_result
            .and(reaper_result)
            .and(host_result)
            .and(writer_result)
    }
}

impl Drop for SessionSupervisor {
    fn drop(&mut self) {
        // Cancellation is the public service/artifact Drop-safety boundary. Keep it before
        // transferring the SDK thread or aborting contract-covered host futures.
        self.cancellation.cancel();
        self.sdk_thread.take();
        for task in self.host_tasks.drain(..) {
            task.abort();
        }
    }
}

#[cfg(test)]
mod thread_reaper_tests {
    use std::{
        num::NonZeroUsize,
        sync::{
            Arc, Condvar, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use super::{OwnedSdkThread, SdkReaperDrain, SdkThreadReaper, SdkThreadShutdown};

    #[tokio::test]
    async fn sdk_thread_start_pre_reserves_reaping_and_timeout_never_detaches_the_join_handle()
    -> Result<(), Box<dyn std::error::Error>> {
        let reaper = SdkThreadReaper::try_new(NonZeroUsize::MIN)?;
        let entered = Arc::new(AtomicUsize::new(0));
        let cancelled = Arc::new(AtomicBool::new(false));
        let latch = Arc::new((Mutex::new(false), Condvar::new()));
        let worker = {
            let entered = Arc::clone(&entered);
            let cancelled = Arc::clone(&cancelled);
            let latch = Arc::clone(&latch);
            OwnedSdkThread::try_spawn(&reaper, "market-squawk-mcp-test-sdk", move |token| {
                entered.fetch_add(1, Ordering::SeqCst);
                while !token.is_cancelled() {
                    std::thread::yield_now();
                }
                cancelled.store(true, Ordering::SeqCst);
                let (released, changed) = &*latch;
                let mut released = released
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                while !*released {
                    released = changed
                        .wait(released)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                }
            })?
        };
        while entered.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }

        let rejected_spawns = Arc::new(AtomicUsize::new(0));
        let rejected_counter = Arc::clone(&rejected_spawns);
        assert!(
            OwnedSdkThread::try_spawn(&reaper, "must-not-spawn", move |_token| {
                rejected_counter.fetch_add(1, Ordering::SeqCst);
            })
            .is_err()
        );
        assert_eq!(rejected_spawns.load(Ordering::SeqCst), 0);

        assert_eq!(
            worker.shutdown(Duration::from_millis(10)).await?,
            SdkThreadShutdown::TransferredToReaper
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            while !cancelled.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert_eq!(reaper.pending_count(), 1);
        assert_eq!(
            reaper.drain(Duration::from_millis(10)).await?,
            SdkReaperDrain::Pending
        );
        assert_eq!(reaper.pending_count(), 1);

        let (released, changed) = &*latch;
        *released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        changed.notify_all();
        assert_eq!(
            reaper.drain(Duration::from_secs(1)).await?,
            SdkReaperDrain::Complete
        );
        assert_eq!(reaper.pending_count(), 0);
        Ok(())
    }
}
