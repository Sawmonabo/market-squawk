//! Owned bounded bridges around the payload-suppressing official-SDK runtime.

use std::{sync::Arc, time::Duration};

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
use tokio::{
    sync::{mpsc, oneshot},
    task::{JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;

use crate::{
    ArtifactError, ArtifactPublication, ArtifactReference, ArtifactRepository, AuditResultClass,
    McpLimits,
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
}

#[derive(Clone)]
pub(crate) struct SdkToolServices {
    pub(crate) capabilities: ServiceCapabilities,
    pub(crate) calls: mpsc::Sender<ServiceCall>,
}

impl std::fmt::Debug for SdkToolServices {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SdkToolServices")
            .field("capabilities", &self.capabilities)
            .field("calls", &"[BOUNDED SERVICE CHANNEL]")
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
                    let result = services.call(call.request, call.context).await;
                    let _ = call.response.send(result);
                });
            }
        }
    }
    active.abort_all();
    while active.join_next().await.is_some() {}
}

pub(crate) struct ArtifactCall {
    publication: ArtifactPublication,
    response: oneshot::Sender<Result<ArtifactReference, ArtifactError>>,
}

#[derive(Clone)]
pub(crate) struct SdkArtifactRepository {
    pub(crate) publications: mpsc::Sender<ArtifactCall>,
    pub(crate) timeout: Duration,
}

impl std::fmt::Debug for SdkArtifactRepository {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SdkArtifactRepository")
            .field("publications", &"[BOUNDED ARTIFACT CHANNEL]")
            .field("timeout", &self.timeout)
            .finish()
    }
}

#[async_trait]
impl ArtifactRepository for SdkArtifactRepository {
    async fn publish(
        &self,
        publication: ArtifactPublication,
    ) -> Result<ArtifactReference, ArtifactError> {
        let deadline = tokio::time::Instant::now()
            .checked_add(self.timeout)
            .ok_or(ArtifactError::Unavailable)?;
        let permit = tokio::time::timeout_at(deadline, self.publications.clone().reserve_owned())
            .await
            .map_err(|_| ArtifactError::Unavailable)?
            .map_err(|_| ArtifactError::Unavailable)?;
        let (response, result) = oneshot::channel();
        permit.send(ArtifactCall {
            publication,
            response,
        });
        tokio::time::timeout_at(deadline, result)
            .await
            .map_err(|_| ArtifactError::Unavailable)?
            .map_err(|_| ArtifactError::Unavailable)?
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
                    let result = artifacts.publish(call.publication).await;
                    let _ = call.response.send(result);
                });
            }
        }
    }
    active.abort_all();
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
    sdk_task: Option<JoinHandle<IsolatedSdkOutcome>>,
    host_tasks: Vec<JoinHandle<()>>,
    writer: WriterSupervisor,
    shutdown_timeout: Duration,
}

impl std::fmt::Debug for SessionSupervisor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SessionSupervisor")
            .field("sdk_task_owned", &self.sdk_task.is_some())
            .field("host_task_count", &self.host_tasks.len())
            .field("writer", &self.writer)
            .field("shutdown_timeout", &self.shutdown_timeout)
            .finish_non_exhaustive()
    }
}

impl SessionSupervisor {
    pub(crate) fn new(
        cancellation: CancellationToken,
        sdk_task: JoinHandle<IsolatedSdkOutcome>,
        host_tasks: Vec<JoinHandle<()>>,
        writer: WriterSupervisor,
        shutdown_timeout: Duration,
    ) -> Self {
        Self {
            cancellation,
            sdk_task: Some(sdk_task),
            host_tasks,
            writer,
            shutdown_timeout,
        }
    }

    pub(crate) async fn wait_sdk(&mut self) -> Result<IsolatedSdkOutcome, ServerError> {
        let result = match self.sdk_task.as_mut() {
            Some(task) => task.await.map_err(ServerError::RuntimeTask),
            None => return Err(ServerError::Transport),
        };
        self.sdk_task.take();
        result
    }

    pub(crate) async fn shutdown(
        &mut self,
        result_class: AuditResultClass,
        terminal_marker: &'static [u8],
    ) -> Result<(), TransportError> {
        self.cancellation.cancel();
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
        host_result.and(writer_result)
    }
}

impl Drop for SessionSupervisor {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(task) = self.sdk_task.take() {
            task.abort();
        }
        for task in self.host_tasks.drain(..) {
            task.abort();
        }
    }
}
