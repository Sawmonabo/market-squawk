//! Bounded rmcp transport with pre-dispatch structural admission and single-writer backpressure.

use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
};

use market_squawk_services::{
    RequestId as ServiceRequestId, ServiceCapabilities, validate_json_contract,
};
use rmcp::model::{
    ClientJsonRpcMessage, ClientNotification, ClientRequest, ErrorCode, ErrorData, GetExtensions,
    JsonRpcMessage, RequestId, ServerJsonRpcMessage,
};
use serde_json::Value;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    AuditError, AuditEvent, AuditOperation, AuditResultClass, AuditSink, LocalProcessIdentityClass,
    McpLimits, MutationAuditBundle,
    framing::{
        ActiveAdmission, BoundedFrameReader, Frame, FramingError, OutputChannel, PendingAudit,
        run_writer,
    },
};

pub(crate) use crate::framing::{
    TransportError, output_audit_failed, output_io_failed, output_peer_closed,
    output_queue_timed_out, output_write_timed_out,
};

const INPUT_RUNNING: u8 = 0;
const INPUT_ENDED: u8 = 1;
const INPUT_CANCELLED: u8 = 2;
const INPUT_REJECTED: u8 = 3;
const INPUT_IO_FAILED: u8 = 4;
const INPUT_AUDIT_FAILED: u8 = 5;

pub(crate) const STATE_AWAIT_INITIALIZE: u8 = 0;
pub(crate) const STATE_AWAIT_INITIALIZED: u8 = 1;
pub(crate) const STATE_READY: u8 = 2;

pub(crate) struct TransportConfig {
    pub(crate) limits: McpLimits,
    pub(crate) cancellation: CancellationToken,
    pub(crate) audit: Arc<dyn AuditSink>,
    pub(crate) identity_class: LocalProcessIdentityClass,
    pub(crate) capabilities: ServiceCapabilities,
    pub(crate) initialization_state: Arc<AtomicU8>,
}

enum RequestAdmission {
    Admitted(PendingAudit),
    InvalidIdentifier,
    AuditFailed,
}

/// Read-only terminal status retained after rmcp consumes the transport.
#[derive(Clone, Debug)]
pub(crate) struct TransportMonitor {
    output_state: Arc<AtomicU8>,
    input_state: Arc<AtomicU8>,
}

impl TransportMonitor {
    pub(crate) fn output_state(&self) -> u8 {
        self.output_state.load(Ordering::SeqCst)
    }

    pub(crate) fn input_state(&self) -> u8 {
        self.input_state.load(Ordering::SeqCst)
    }
}

/// Official-SDK transport with bounded owned framing and output.
pub(crate) struct BoundedInputDriver<R> {
    reader: BoundedFrameReader<R>,
    limits: McpLimits,
    cancellation: CancellationToken,
    output: Arc<OutputChannel>,
    input_state: Arc<AtomicU8>,
    capabilities: ServiceCapabilities,
    initialization_state: Arc<AtomicU8>,
    notification_admission: NotificationAdmissionController,
}

#[derive(Clone, Debug)]
struct NotificationAdmission {
    _permit: Arc<OwnedSemaphorePermit>,
}

#[derive(Clone, Debug)]
struct NotificationAdmissionController {
    permits: Arc<Semaphore>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct NotificationAdmissionSaturated;

impl NotificationAdmissionController {
    fn new(maximum_active: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(maximum_active)),
        }
    }

    fn admit(
        &self,
        notification: &mut ClientNotification,
    ) -> Result<(), NotificationAdmissionSaturated> {
        let permit = Arc::clone(&self.permits)
            .try_acquire_owned()
            .map_err(|_| NotificationAdmissionSaturated)?;
        notification.extensions_mut().insert(NotificationAdmission {
            _permit: Arc::new(permit),
        });
        Ok(())
    }
}

pub(crate) struct WriterSupervisor {
    output: Arc<OutputChannel>,
    limits: McpLimits,
    writer_task: Option<JoinHandle<()>>,
}

impl std::fmt::Debug for WriterSupervisor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WriterSupervisor")
            .field("limits", &self.limits)
            .field("writer_task_owned", &self.writer_task.is_some())
            .finish_non_exhaustive()
    }
}

impl WriterSupervisor {
    pub(crate) async fn shutdown(
        &mut self,
        result_class: AuditResultClass,
        terminal_marker: &'static [u8],
    ) -> Result<(), TransportError> {
        let terminalized = self
            .output
            .terminalize_pending(result_class, terminal_marker);
        let sender_closed = self.output.close_sender();
        let writer_result = if let Some(mut writer_task) = self.writer_task.take() {
            match tokio::time::timeout(self.limits.shutdown_timeout(), &mut writer_task).await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(_)) => Err(TransportError::WriterTask),
                Err(_) => {
                    writer_task.abort();
                    let _ = writer_task.await;
                    self.output.fail(output_write_timed_out());
                    Err(TransportError::WriteTimedOut)
                }
            }
        } else {
            Ok(())
        };
        terminalized.and(sender_closed).and(writer_result)
    }
}

impl Drop for WriterSupervisor {
    fn drop(&mut self) {
        if self
            .output
            .terminalize_pending(
                AuditResultClass::OutputUnavailable,
                b"writer supervisor dropped",
            )
            .is_err()
        {
            self.output.fail(output_audit_failed());
        }
        let _ = self.output.close_sender();
        if let Some(writer_task) = self.writer_task.take() {
            writer_task.abort();
        }
    }
}

impl<R> std::fmt::Debug for BoundedInputDriver<R> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BoundedInputDriver")
            .field("limits", &self.limits)
            .field("input_state", &self.input_state)
            .field("capabilities", &self.capabilities)
            .finish_non_exhaustive()
    }
}

impl<R> BoundedInputDriver<R>
where
    R: AsyncRead + Send + Unpin + 'static,
{
    pub(crate) fn new<W>(
        reader: R,
        writer: W,
        config: TransportConfig,
    ) -> Result<(Self, TransportMonitor, WriterSupervisor), TransportError>
    where
        W: AsyncWrite + Send + Unpin + 'static,
    {
        let maximum_frame = NonZeroUsize::new(config.limits.maximum_frame_bytes())
            .ok_or(TransportError::InvalidLimit)?;
        let reader = BoundedFrameReader::new(reader, maximum_frame)?;
        let (sender, receiver) = mpsc::channel(config.limits.writer_queue_capacity());
        let output_state = Arc::new(AtomicU8::new(0));
        let input_state = Arc::new(AtomicU8::new(INPUT_RUNNING));
        let output = Arc::new(OutputChannel::new(
            sender,
            Arc::clone(&output_state),
            config.limits,
            config.audit,
            config.identity_class,
        ));
        let writer_task = tokio::spawn(run_writer(writer, receiver, Arc::clone(&output)));
        let monitor = TransportMonitor {
            output_state,
            input_state: Arc::clone(&input_state),
        };
        let transport = Self {
            reader,
            limits: config.limits,
            cancellation: config.cancellation,
            output: Arc::clone(&output),
            input_state,
            capabilities: config.capabilities,
            initialization_state: config.initialization_state,
            notification_admission: NotificationAdmissionController::new(
                config.limits.maximum_active_requests(),
            ),
        };
        let supervisor = WriterSupervisor {
            output,
            limits: config.limits,
            writer_task: Some(writer_task),
        };
        Ok((transport, monitor, supervisor))
    }

    async fn receive_next(&mut self) -> Option<ClientJsonRpcMessage> {
        loop {
            let output = Arc::clone(&self.output);
            let capabilities = self.capabilities.clone();
            let limits = self.limits;
            let input_state = Arc::clone(&self.input_state);
            let frame_result = tokio::select! {
                biased;
                () = self.output.failed() => {
                    let _ = self.output.terminalize_pending(
                        AuditResultClass::OutputUnavailable,
                        b"output unavailable",
                    );
                    return None;
                },
                frame = self.reader.next_frame(&self.cancellation) => frame,
            };
            let frame = match frame_result {
                Ok(Frame::Message(bytes)) if bytes.iter().all(u8::is_ascii_whitespace) => continue,
                Ok(Frame::Message(bytes)) => bytes,
                Ok(Frame::EndOfInput) => {
                    self.input_state.store(INPUT_ENDED, Ordering::SeqCst);
                    return None;
                }
                Err(FramingError::Cancelled) => {
                    self.input_state.store(INPUT_CANCELLED, Ordering::SeqCst);
                    if self
                        .output
                        .terminalize_pending(AuditResultClass::Cancelled, b"session cancelled")
                        .is_err()
                    {
                        self.input_state.store(INPUT_AUDIT_FAILED, Ordering::SeqCst);
                    }
                    return None;
                }
                Err(FramingError::Oversized { .. }) => {
                    self.input_state.store(INPUT_REJECTED, Ordering::SeqCst);
                    if self
                        .output
                        .terminalize_pending(
                            AuditResultClass::OutputUnavailable,
                            b"oversized input terminated session",
                        )
                        .is_err()
                    {
                        self.output.fail(output_audit_failed());
                        self.input_state.store(INPUT_AUDIT_FAILED, Ordering::SeqCst);
                        return None;
                    }
                    let error = ServerJsonRpcMessage::error(
                        ErrorData::new(ErrorCode(-32_010), "request resource limit exceeded", None),
                        None,
                    );
                    let _ = self
                        .output
                        .send_direct(error, None, AuditResultClass::ResourceExhausted)
                        .await;
                    return None;
                }
                Err(FramingError::Io(_))
                | Err(FramingError::InvalidLimit)
                | Err(FramingError::Allocation) => {
                    self.input_state.store(INPUT_IO_FAILED, Ordering::SeqCst);
                    if self
                        .output
                        .terminalize_pending(
                            AuditResultClass::OutputUnavailable,
                            b"input failure terminated session",
                        )
                        .is_err()
                    {
                        self.output.fail(output_audit_failed());
                        self.input_state.store(INPUT_AUDIT_FAILED, Ordering::SeqCst);
                    }
                    return None;
                }
            };

            if frame.len() > self.limits.maximum_body_bytes() {
                self.input_state.store(INPUT_REJECTED, Ordering::SeqCst);
                let _ = self
                    .output
                    .send_direct(
                        ServerJsonRpcMessage::error(
                            ErrorData::new(
                                ErrorCode(-32_010),
                                "request resource limit exceeded",
                                None,
                            ),
                            None,
                        ),
                        None,
                        AuditResultClass::ResourceExhausted,
                    )
                    .await;
                continue;
            }

            let value: Value = match serde_json::from_slice(frame) {
                Ok(value) => value,
                Err(_) => {
                    let _ = self
                        .output
                        .send_direct(
                            ServerJsonRpcMessage::error(
                                ErrorData::parse_error("parse error", None),
                                None,
                            ),
                            None,
                            AuditResultClass::ProtocolRejected,
                        )
                        .await;
                    continue;
                }
            };
            if validate_json_contract(
                &value,
                self.limits.input_structure(),
                self.limits.maximum_body_bytes(),
            )
            .is_err()
            {
                let request_id = request_id_from_value(&value);
                let pending = if let Some(id) = request_id.as_ref() {
                    match admit_direct(&output, limits, id, AuditOperation::Other, frame) {
                        Ok(pending) => Some(pending),
                        Err(_) => {
                            output.fail(output_audit_failed());
                            input_state.store(INPUT_AUDIT_FAILED, Ordering::SeqCst);
                            return None;
                        }
                    }
                } else {
                    None
                };
                let _ = self
                    .output
                    .send_direct(
                        ServerJsonRpcMessage::error(
                            ErrorData::new(
                                ErrorCode(-32_010),
                                "request resource limit exceeded",
                                None,
                            ),
                            request_id,
                        ),
                        pending,
                        AuditResultClass::ResourceExhausted,
                    )
                    .await;
                continue;
            }

            let request_id = request_id_from_value(&value);
            let mut message: ClientJsonRpcMessage = match serde_json::from_value(value) {
                Ok(message) => message,
                Err(_) => {
                    let pending = if let Some(id) = request_id.as_ref() {
                        match admit_direct(&output, limits, id, AuditOperation::Other, frame) {
                            Ok(pending) => Some(pending),
                            Err(_) => {
                                output.fail(output_audit_failed());
                                input_state.store(INPUT_AUDIT_FAILED, Ordering::SeqCst);
                                return None;
                            }
                        }
                    } else {
                        None
                    };
                    let _ = self
                        .output
                        .send_direct(
                            ServerJsonRpcMessage::error(
                                ErrorData::invalid_request("invalid request", None),
                                request_id,
                            ),
                            pending,
                            AuditResultClass::ProtocolRejected,
                        )
                        .await;
                    continue;
                }
            };

            if let JsonRpcMessage::Notification(notification) = &mut message
                && self
                    .notification_admission
                    .admit(&mut notification.notification)
                    .is_err()
            {
                self.input_state.store(INPUT_REJECTED, Ordering::SeqCst);
                if self
                    .output
                    .terminalize_pending(
                        AuditResultClass::OutputUnavailable,
                        b"notification work limit exceeded",
                    )
                    .is_err()
                {
                    self.output.fail(output_audit_failed());
                    self.input_state.store(INPUT_AUDIT_FAILED, Ordering::SeqCst);
                }
                return None;
            }

            if let JsonRpcMessage::Notification(notification) = &message
                && let ClientNotification::InitializedNotification(_) = &notification.notification
            {
                let _ = self.initialization_state.compare_exchange(
                    STATE_AWAIT_INITIALIZED,
                    STATE_READY,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
            }

            if let JsonRpcMessage::Notification(notification) = &message
                && let ClientNotification::CancelledNotification(cancelled) =
                    &notification.notification
                && let Some(request_id) = &cancelled.params.request_id
                && self.output.mark_pending_cancelled(request_id).is_err()
            {
                self.output.fail(output_audit_failed());
                self.input_state.store(INPUT_AUDIT_FAILED, Ordering::SeqCst);
                return None;
            }

            if let JsonRpcMessage::Request(request) = &message {
                let pending = match admit_request(&output, limits, &capabilities, request, frame) {
                    RequestAdmission::Admitted(pending) => pending,
                    RequestAdmission::InvalidIdentifier => {
                        let _ = output
                            .send_direct(
                                ServerJsonRpcMessage::error(
                                    ErrorData::invalid_request(
                                        "request identifier is invalid",
                                        None,
                                    ),
                                    None,
                                ),
                                None,
                                AuditResultClass::ProtocolRejected,
                            )
                            .await;
                        continue;
                    }
                    RequestAdmission::AuditFailed => {
                        output.fail(output_audit_failed());
                        input_state.store(INPUT_AUDIT_FAILED, Ordering::SeqCst);
                        return None;
                    }
                };
                let admission = self.output.admit_active(
                    request.id.clone(),
                    pending,
                    self.limits.maximum_active_requests(),
                );
                match admission {
                    Ok(ActiveAdmission::Accepted) => {}
                    Err(_) => {
                        self.output.fail(output_audit_failed());
                        self.input_state.store(INPUT_AUDIT_FAILED, Ordering::SeqCst);
                        return None;
                    }
                    Ok(ActiveAdmission::Duplicate(pending)) => {
                        let _ = self
                            .output
                            .send_direct(
                                ServerJsonRpcMessage::error(
                                    ErrorData::new(
                                        ErrorCode(-32_009),
                                        "duplicate active request identifier",
                                        None,
                                    ),
                                    Some(request.id.clone()),
                                ),
                                Some(pending),
                                AuditResultClass::ProtocolRejected,
                            )
                            .await;
                        continue;
                    }
                    Ok(ActiveAdmission::Full(pending)) => {
                        let _ = self
                            .output
                            .send_direct(
                                ServerJsonRpcMessage::error(
                                    ErrorData::new(
                                        ErrorCode(-32_010),
                                        "active request limit exceeded",
                                        None,
                                    ),
                                    Some(request.id.clone()),
                                ),
                                Some(pending),
                                AuditResultClass::ResourceExhausted,
                            )
                            .await;
                        continue;
                    }
                }
            }
            return Some(message);
        }
    }
}

fn admit_request(
    output: &OutputChannel,
    limits: McpLimits,
    capabilities: &ServiceCapabilities,
    request: &rmcp::model::JsonRpcRequest<ClientRequest>,
    frame: &[u8],
) -> RequestAdmission {
    let Ok(request_id) = service_request_id(&request.id) else {
        return RequestAdmission::InvalidIdentifier;
    };
    let operation = operation_for(&request.request, capabilities);
    if request_is_mutating(&request.request, capabilities) {
        let Ok(bundle) = MutationAuditBundle::new(
            &request_id,
            output.identity_class(),
            operation.clone(),
            limits.service_limits(),
            frame,
        ) else {
            return RequestAdmission::AuditFailed;
        };
        let Ok(mutation) = output.reserve_mutation(bundle) else {
            return RequestAdmission::AuditFailed;
        };
        return RequestAdmission::Admitted(PendingAudit::new_mutation(
            request_id, operation, mutation,
        ));
    }
    let Ok(event) = AuditEvent::admitted(
        &request_id,
        output.identity_class(),
        operation.clone(),
        limits.service_limits(),
        frame,
    ) else {
        return RequestAdmission::AuditFailed;
    };
    if output.record_admitted(event).is_err() {
        return RequestAdmission::AuditFailed;
    }
    RequestAdmission::Admitted(PendingAudit::new(request_id, operation))
}

fn request_is_mutating(request: &ClientRequest, capabilities: &ServiceCapabilities) -> bool {
    match request {
        ClientRequest::CallToolRequest(call) => capabilities
            .find(call.params.name.as_ref())
            .is_some_and(|descriptor| !descriptor.effects().read_only()),
        _ => false,
    }
}

fn admit_direct(
    output: &OutputChannel,
    limits: McpLimits,
    request_id: &RequestId,
    operation: AuditOperation,
    frame: &[u8],
) -> Result<PendingAudit, AuditError> {
    let request_id = service_request_id(request_id).map_err(|_| AuditError::Encoding)?;
    output.record_admitted(AuditEvent::admitted(
        &request_id,
        output.identity_class(),
        operation.clone(),
        limits.service_limits(),
        frame,
    )?)?;
    Ok(PendingAudit::new(request_id, operation))
}

impl<R> BoundedInputDriver<R>
where
    R: AsyncRead + Send + Unpin + 'static,
{
    pub(crate) fn output_channel(&self) -> Arc<OutputChannel> {
        Arc::clone(&self.output)
    }

    pub(crate) async fn run(mut self, mut requests: mpsc::Receiver<SdkInboundRequest>) {
        while let Some(request) = requests.recv().await {
            let message = self.receive_next().await;
            let ended = message.is_none();
            if request.response.send(message).is_err() {
                let _ = self.output.terminalize_pending(
                    AuditResultClass::OutputUnavailable,
                    b"SDK input channel closed",
                );
                return;
            }
            if ended {
                return;
            }
        }
        let _ = self.output.terminalize_pending(
            AuditResultClass::OutputUnavailable,
            b"SDK input channel closed",
        );
    }
}

#[derive(Debug)]
pub(crate) struct SdkInboundRequest {
    pub(crate) response: oneshot::Sender<Option<ClientJsonRpcMessage>>,
}

fn operation_for(request: &ClientRequest, capabilities: &ServiceCapabilities) -> AuditOperation {
    match request {
        ClientRequest::InitializeRequest(_) => AuditOperation::Initialize,
        ClientRequest::PingRequest(_) => AuditOperation::Ping,
        ClientRequest::ListToolsRequest(_) => AuditOperation::ListTools,
        ClientRequest::CallToolRequest(call) => capabilities
            .find(call.params.name.as_ref())
            .map_or(AuditOperation::Other, |descriptor| {
                AuditOperation::CallTool {
                    name: Arc::from(descriptor.name()),
                    version: Arc::from(descriptor.version()),
                }
            }),
        _ => AuditOperation::Other,
    }
}

fn service_request_id(request_id: &RequestId) -> Result<ServiceRequestId, ()> {
    match request_id {
        RequestId::Number(value) => Ok(ServiceRequestId::Integer(*value)),
        RequestId::String(value) => ServiceRequestId::try_string(Arc::clone(value)).map_err(|_| ()),
    }
}

fn request_id_from_value(value: &Value) -> Option<RequestId> {
    let request_id = value
        .get("id")
        .cloned()
        .and_then(|id| serde_json::from_value(id).ok())?;
    service_request_id(&request_id).ok().map(|_| request_id)
}

pub(crate) const fn input_rejected() -> u8 {
    INPUT_REJECTED
}

pub(crate) const fn input_ended() -> u8 {
    INPUT_ENDED
}

pub(crate) const fn input_io_failed() -> u8 {
    INPUT_IO_FAILED
}

pub(crate) const fn input_audit_failed() -> u8 {
    INPUT_AUDIT_FAILED
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use rmcp::model::{ClientJsonRpcMessage, GetExtensions, JsonRpcMessage};
    use serde_json::json;

    use super::{NotificationAdmissionController, NotificationAdmissionSaturated};

    #[test]
    fn notification_extensions_own_the_exact_admission_permit() -> Result<(), Box<dyn Error>> {
        let admission = NotificationAdmissionController::new(1);
        let mut first: ClientJsonRpcMessage = serde_json::from_value(json!({
            "jsonrpc":"2.0",
            "method":"notifications/initialized"
        }))?;
        let mut second: ClientJsonRpcMessage = serde_json::from_value(json!({
            "jsonrpc":"2.0",
            "method":"notifications/initialized"
        }))?;
        let JsonRpcMessage::Notification(first) = &mut first else {
            return Err(std::io::Error::other("expected first notification").into());
        };
        let JsonRpcMessage::Notification(second) = &mut second else {
            return Err(std::io::Error::other("expected second notification").into());
        };

        admission
            .admit(&mut first.notification)
            .map_err(|_| std::io::Error::other("first notification was not admitted"))?;
        assert_eq!(
            admission.admit(&mut second.notification),
            Err(NotificationAdmissionSaturated)
        );
        let owned_extensions = std::mem::take(first.notification.extensions_mut());
        drop(owned_extensions);
        admission
            .admit(&mut second.notification)
            .map_err(|_| std::io::Error::other("notification permit was not released"))?;
        Ok(())
    }
}
