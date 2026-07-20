//! Maximum-plus-one input framing and bounded, acknowledged output delivery.

use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
};

use market_squawk_services::RequestId as ServiceRequestId;
use rmcp::{
    RoleServer,
    model::{JsonRpcMessage, RequestId, ServerJsonRpcMessage, ServerResult},
    service::TxJsonRpcMessage,
};
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
};
use tokio_util::sync::CancellationToken;

use crate::{
    AuditError, AuditEvent, AuditOperation, AuditResultClass, AuditSink, LocalProcessIdentityClass,
    McpLimits,
};

const OUTPUT_RUNNING: u8 = 0;
const OUTPUT_PEER_CLOSED: u8 = 1;
const OUTPUT_WRITE_TIMED_OUT: u8 = 2;
const OUTPUT_IO_FAILED: u8 = 3;
const OUTPUT_AUDIT_FAILED: u8 = 4;
const OUTPUT_QUEUE_TIMED_OUT: u8 = 5;

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum Frame<'a> {
    Message(&'a [u8]),
    EndOfInput,
}

/// Bounded frame-reader failure.
#[derive(Debug, Error)]
pub(crate) enum FramingError {
    #[error("MCP input read failed")]
    Io(#[source] std::io::Error),
    #[error("MCP request exceeds {maximum_bytes} bytes")]
    Oversized { maximum_bytes: usize },
    #[error("MCP input read was cancelled")]
    Cancelled,
    #[error("MCP frame limit cannot reserve its detection byte")]
    InvalidLimit,
    #[error("MCP bounded frame allocation failed")]
    Allocation,
}

impl From<std::io::Error> for FramingError {
    fn from(source: std::io::Error) -> Self {
        Self::Io(source)
    }
}

/// Fixed-storage reader that never retains more than the configured frame plus one detection byte.
#[derive(Debug)]
pub(crate) struct BoundedFrameReader<R> {
    reader: BufReader<R>,
    frame: Box<[u8]>,
    frame_len: usize,
    maximum_bytes: usize,
}

impl<R> BoundedFrameReader<R>
where
    R: AsyncRead + Unpin,
{
    pub(crate) fn new(reader: R, maximum_bytes: NonZeroUsize) -> Result<Self, FramingError> {
        let frame_bytes = maximum_bytes
            .get()
            .checked_add(1)
            .ok_or(FramingError::InvalidLimit)?;
        let scratch_bytes = maximum_bytes.get().min(8 * 1024);
        let mut frame = Vec::new();
        frame
            .try_reserve_exact(frame_bytes)
            .map_err(|_| FramingError::Allocation)?;
        frame.resize(frame_bytes, 0);
        Ok(Self {
            reader: BufReader::with_capacity(scratch_bytes, reader),
            frame: frame.into_boxed_slice(),
            frame_len: 0,
            maximum_bytes: maximum_bytes.get(),
        })
    }

    pub(crate) async fn next_frame<'a>(
        &'a mut self,
        cancellation: &CancellationToken,
    ) -> Result<Frame<'a>, FramingError> {
        loop {
            let available = tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err(FramingError::Cancelled),
                available = self.reader.fill_buf() => available?,
            };
            if available.is_empty() {
                return self.finish_at_eof();
            }

            let newline = available.iter().position(|byte| *byte == b'\n');
            let bytes_before_newline = newline.unwrap_or(available.len());
            let remaining = self.frame.len().saturating_sub(self.frame_len);
            let copied = bytes_before_newline.min(remaining);
            let end = self.frame_len.saturating_add(copied);
            self.frame[self.frame_len..end].copy_from_slice(&available[..copied]);
            self.frame_len = end;
            let overflowed = copied < bytes_before_newline;
            let consumed =
                newline.map_or(bytes_before_newline, |position| position.saturating_add(1));
            self.reader.consume(consumed);

            if overflowed {
                return Err(FramingError::Oversized {
                    maximum_bytes: self.maximum_bytes,
                });
            }
            if newline.is_some() {
                return self.finish_at_newline();
            }
            if self.frame_len > self.maximum_bytes {
                return Err(FramingError::Oversized {
                    maximum_bytes: self.maximum_bytes,
                });
            }
        }
    }

    fn finish_at_eof(&mut self) -> Result<Frame<'_>, FramingError> {
        if self.frame_len == 0 {
            return Ok(Frame::EndOfInput);
        }
        if self.frame_len > self.maximum_bytes {
            return Err(FramingError::Oversized {
                maximum_bytes: self.maximum_bytes,
            });
        }
        let length = trim_carriage_return(&self.frame[..self.frame_len]);
        self.frame_len = 0;
        Ok(Frame::Message(&self.frame[..length]))
    }

    fn finish_at_newline(&mut self) -> Result<Frame<'_>, FramingError> {
        let length = trim_carriage_return(&self.frame[..self.frame_len]);
        if length > self.maximum_bytes {
            return Err(FramingError::Oversized {
                maximum_bytes: self.maximum_bytes,
            });
        }
        self.frame_len = 0;
        Ok(Frame::Message(&self.frame[..length]))
    }
}

fn trim_carriage_return(bytes: &[u8]) -> usize {
    bytes.strip_suffix(b"\r").map_or(bytes.len(), <[u8]>::len)
}

#[derive(Clone, Debug)]
pub(crate) struct PendingAudit {
    pub(crate) request_id: ServiceRequestId,
    pub(crate) operation: AuditOperation,
    cancellation_requested: bool,
}

impl PendingAudit {
    pub(crate) fn new(request_id: ServiceRequestId, operation: AuditOperation) -> Self {
        Self {
            request_id,
            operation,
            cancellation_requested: false,
        }
    }

    fn shutdown_result_class(&self, default: AuditResultClass) -> AuditResultClass {
        if self.cancellation_requested {
            AuditResultClass::Cancelled
        } else {
            default
        }
    }
}

#[derive(Clone, Debug)]
struct PendingCompletion {
    audit: PendingAudit,
    result_class: AuditResultClass,
    terminalized: Arc<AtomicBool>,
}

impl PendingCompletion {
    fn new(audit: PendingAudit, result_class: AuditResultClass) -> Self {
        Self {
            audit,
            result_class,
            terminalized: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[derive(Debug)]
pub(crate) struct OutboundMessage {
    encoded: Vec<u8>,
    completion: Option<PendingCompletion>,
    deadline: tokio::time::Instant,
    acknowledgement: oneshot::Sender<Result<(), DeliveryFailure>>,
    _retained_bytes: OwnedSemaphorePermit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DeliveryFailure {
    PeerClosed,
    TimedOut,
    Io,
    Audit,
}

pub(crate) enum ActiveAdmission {
    Accepted,
    Duplicate(PendingAudit),
    Full(PendingAudit),
}

pub(crate) struct OutputChannel {
    sender: Mutex<Option<mpsc::Sender<OutboundMessage>>>,
    state: Arc<AtomicU8>,
    failed: CancellationToken,
    maximum_message_bytes: usize,
    limits: McpLimits,
    audit: Arc<dyn AuditSink>,
    identity_class: LocalProcessIdentityClass,
    pending: Mutex<HashMap<RequestId, PendingAudit>>,
    retained_bytes: Arc<Semaphore>,
}

impl std::fmt::Debug for OutputChannel {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OutputChannel")
            .field("state", &self.state)
            .field("maximum_message_bytes", &self.maximum_message_bytes)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl OutputChannel {
    pub(crate) fn new(
        sender: mpsc::Sender<OutboundMessage>,
        state: Arc<AtomicU8>,
        limits: McpLimits,
        audit: Arc<dyn AuditSink>,
        identity_class: LocalProcessIdentityClass,
    ) -> Self {
        Self {
            sender: Mutex::new(Some(sender)),
            state,
            failed: CancellationToken::new(),
            maximum_message_bytes: limits.maximum_frame_bytes(),
            limits,
            audit,
            identity_class,
            pending: Mutex::new(HashMap::new()),
            retained_bytes: Arc::new(Semaphore::new(limits.maximum_writer_queue_bytes())),
        }
    }

    pub(crate) async fn failed(&self) {
        self.failed.cancelled().await;
    }

    pub(crate) fn record_admitted(&self, event: AuditEvent) -> Result<(), AuditError> {
        self.audit.record(event)
    }

    pub(crate) const fn identity_class(&self) -> LocalProcessIdentityClass {
        self.identity_class
    }

    pub(crate) fn admit_active(
        &self,
        request_id: RequestId,
        pending: PendingAudit,
        maximum_active_requests: usize,
    ) -> Result<ActiveAdmission, TransportError> {
        let mut active = self.pending.lock().map_err(|_| TransportError::State)?;
        if active.contains_key(&request_id) {
            return Ok(ActiveAdmission::Duplicate(pending));
        }
        if active.len() >= maximum_active_requests {
            return Ok(ActiveAdmission::Full(pending));
        }
        active.insert(request_id, pending);
        Ok(ActiveAdmission::Accepted)
    }

    pub(crate) fn remove_pending(
        &self,
        request_id: &RequestId,
    ) -> Result<Option<PendingAudit>, TransportError> {
        self.pending
            .lock()
            .map_err(|_| TransportError::State)
            .map(|mut pending| pending.remove(request_id))
    }

    pub(crate) fn mark_pending_cancelled(
        &self,
        request_id: &RequestId,
    ) -> Result<(), TransportError> {
        if let Some(pending) = self
            .pending
            .lock()
            .map_err(|_| TransportError::State)?
            .get_mut(request_id)
        {
            pending.cancellation_requested = true;
        }
        Ok(())
    }

    pub(crate) async fn send_message(
        &self,
        message: TxJsonRpcMessage<RoleServer>,
    ) -> Result<(), TransportError> {
        let response = response_identity(&message);
        let mut encoded = serde_json::to_vec(&message).map_err(|_| TransportError::Encoding)?;
        if encoded.len() > self.maximum_message_bytes {
            self.fail(OUTPUT_IO_FAILED);
            return Err(TransportError::OutputLimit);
        }
        encoded.push(b'\n');

        let completion = if let Some((request_id, result_class)) = response {
            self.remove_pending(&request_id)?
                .map(|audit| PendingCompletion::new(audit, result_class))
        } else {
            None
        };
        self.enqueue(encoded, completion).await
    }

    pub(crate) async fn send_direct(
        &self,
        message: ServerJsonRpcMessage,
        pending: Option<PendingAudit>,
        result_class: AuditResultClass,
    ) -> Result<(), TransportError> {
        let mut encoded = serde_json::to_vec(&message).map_err(|_| TransportError::Encoding)?;
        if encoded.len() > self.maximum_message_bytes {
            self.fail(OUTPUT_IO_FAILED);
            return Err(TransportError::OutputLimit);
        }
        encoded.push(b'\n');
        let completion = pending.map(|audit| PendingCompletion::new(audit, result_class));
        self.enqueue(encoded, completion).await
    }

    async fn enqueue(
        &self,
        encoded: Vec<u8>,
        completion: Option<PendingCompletion>,
    ) -> Result<(), TransportError> {
        let sender = self
            .sender
            .lock()
            .map_err(|_| TransportError::State)?
            .clone()
            .ok_or_else(|| self.current_error())?;
        let deadline = tokio::time::Instant::now()
            .checked_add(self.limits.write_timeout())
            .ok_or(TransportError::InvalidLimit)?;
        let fallback = completion.clone();
        let retained_byte_count =
            u32::try_from(encoded.len()).map_err(|_| TransportError::InvalidLimit)?;
        let retained_bytes = match tokio::time::timeout_at(
            deadline,
            Arc::clone(&self.retained_bytes).acquire_many_owned(retained_byte_count),
        )
        .await
        {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) => {
                self.record_output_unavailable(fallback, b"writer byte budget unavailable")?;
                self.fail(OUTPUT_IO_FAILED);
                return Err(TransportError::WriterTask);
            }
            Err(_) => {
                self.record_output_unavailable(fallback, b"writer byte budget timed out")?;
                self.fail(OUTPUT_QUEUE_TIMED_OUT);
                return Err(TransportError::WriteTimedOut);
            }
        };
        let (acknowledgement, delivered) = oneshot::channel();
        let outbound = OutboundMessage {
            encoded,
            completion,
            deadline,
            acknowledgement,
            _retained_bytes: retained_bytes,
        };
        let queue_permit = match tokio::time::timeout_at(deadline, sender.reserve_owned()).await {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) => {
                self.record_output_unavailable(fallback, b"writer queue unavailable")?;
                return Err(self.current_error());
            }
            Err(_) => {
                self.record_output_unavailable(fallback, b"writer queue admission timed out")?;
                self.fail(OUTPUT_QUEUE_TIMED_OUT);
                return Err(TransportError::WriteTimedOut);
            }
        };
        queue_permit.send(outbound);
        match delivered.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(failure)) => Err(failure.into()),
            Err(_) => {
                self.record_output_unavailable(fallback, b"writer ended before delivery")?;
                self.fail(OUTPUT_IO_FAILED);
                Err(TransportError::WriterTask)
            }
        }
    }

    fn record_completion(
        &self,
        completion: Option<PendingCompletion>,
        encoded: &[u8],
    ) -> Result<(), TransportError> {
        let Some(completion) = completion else {
            return Ok(());
        };
        if completion
            .terminalized
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }
        let event = AuditEvent::completed(
            &completion.audit.request_id,
            self.identity_class,
            completion.audit.operation,
            self.limits.service_limits(),
            encoded,
            completion.result_class,
        )?;
        if self.audit.record(event).is_err() {
            self.fail(OUTPUT_AUDIT_FAILED);
            return Err(TransportError::Audit);
        }
        Ok(())
    }

    fn record_output_unavailable(
        &self,
        completion: Option<PendingCompletion>,
        encoded: &[u8],
    ) -> Result<(), TransportError> {
        self.record_completion(
            completion.map(|completion| PendingCompletion {
                audit: completion.audit,
                result_class: AuditResultClass::OutputUnavailable,
                terminalized: completion.terminalized,
            }),
            encoded,
        )
    }

    pub(crate) fn close_sender(&self) -> Result<(), TransportError> {
        self.sender
            .lock()
            .map_err(|_| TransportError::State)?
            .take();
        Ok(())
    }

    pub(crate) fn terminalize_pending(
        &self,
        result_class: AuditResultClass,
        terminal_marker: &[u8],
    ) -> Result<(), TransportError> {
        let pending = self
            .pending
            .lock()
            .map_err(|_| TransportError::State)?
            .drain()
            .map(|(_, audit)| audit)
            .collect::<Vec<_>>();
        for audit in pending {
            let result_class = audit.shutdown_result_class(result_class);
            self.record_completion(
                Some(PendingCompletion::new(audit, result_class)),
                terminal_marker,
            )?;
        }
        Ok(())
    }

    pub(crate) fn fail(&self, state: u8) {
        let _ =
            self.state
                .compare_exchange(OUTPUT_RUNNING, state, Ordering::SeqCst, Ordering::SeqCst);
        self.failed.cancel();
    }

    fn current_error(&self) -> TransportError {
        match self.state.load(Ordering::SeqCst) {
            OUTPUT_PEER_CLOSED => TransportError::PeerClosed,
            OUTPUT_WRITE_TIMED_OUT | OUTPUT_QUEUE_TIMED_OUT => TransportError::WriteTimedOut,
            OUTPUT_AUDIT_FAILED => TransportError::Audit,
            OUTPUT_IO_FAILED => TransportError::Io,
            _ => TransportError::PeerClosed,
        }
    }
}

pub(crate) async fn run_writer<W>(
    mut writer: W,
    mut receiver: mpsc::Receiver<OutboundMessage>,
    output: Arc<OutputChannel>,
) where
    W: AsyncWrite + Unpin,
{
    while let Some(outbound) = receiver.recv().await {
        let result = tokio::time::timeout_at(outbound.deadline, async {
            writer.write_all(&outbound.encoded).await?;
            writer.flush().await
        })
        .await;
        let delivery = match result {
            Ok(Ok(())) => match output.record_completion(outbound.completion, &outbound.encoded) {
                Ok(()) => Ok(()),
                Err(_) => Err(DeliveryFailure::Audit),
            },
            Ok(Err(error)) if error.kind() == std::io::ErrorKind::BrokenPipe => {
                let _ = output.record_output_unavailable(outbound.completion, &outbound.encoded);
                output.fail(OUTPUT_PEER_CLOSED);
                Err(DeliveryFailure::PeerClosed)
            }
            Ok(Err(_)) => {
                let _ = output.record_output_unavailable(outbound.completion, &outbound.encoded);
                output.fail(OUTPUT_IO_FAILED);
                Err(DeliveryFailure::Io)
            }
            Err(_) => {
                let _ = output.record_output_unavailable(outbound.completion, &outbound.encoded);
                output.fail(OUTPUT_WRITE_TIMED_OUT);
                Err(DeliveryFailure::TimedOut)
            }
        };
        let failed = delivery.is_err();
        let _ = outbound.acknowledgement.send(delivery);
        if failed {
            return;
        }
    }
}

fn response_identity(message: &ServerJsonRpcMessage) -> Option<(RequestId, AuditResultClass)> {
    match message {
        JsonRpcMessage::Response(response) => {
            let class = match &response.result {
                ServerResult::CallToolResult(result)
                    if result
                        .structured_content
                        .as_ref()
                        .and_then(|value| value.get("artifact"))
                        .is_some() =>
                {
                    AuditResultClass::ArtifactPublished
                }
                _ => AuditResultClass::Succeeded,
            };
            Some((response.id.clone(), class))
        }
        JsonRpcMessage::Error(error) => error.id.clone().map(|id| {
            let class = match error.error.code.0 {
                -32_800 => AuditResultClass::Cancelled,
                -32_008 => AuditResultClass::DeadlineExceeded,
                -32_010 => AuditResultClass::ResourceExhausted,
                -32_001 | -32_003 => AuditResultClass::ServiceRejected,
                _ => AuditResultClass::ProtocolRejected,
            };
            (id, class)
        }),
        _ => None,
    }
}

/// Failure exposed to the rmcp service runtime without raw peer payloads.
#[derive(Debug, Error)]
pub(crate) enum TransportError {
    #[error("invalid bounded MCP transport limit")]
    InvalidLimit,
    #[error("MCP frame construction failed")]
    Framing(#[from] FramingError),
    #[error("MCP message encoding failed")]
    Encoding,
    #[error("MCP output message exceeds its byte ceiling")]
    OutputLimit,
    #[error("MCP peer output closed")]
    PeerClosed,
    #[error("MCP output write timed out")]
    WriteTimedOut,
    #[error("MCP output failed")]
    Io,
    #[error("MCP audit failed")]
    Audit,
    #[error("MCP transport state is unavailable")]
    State,
    #[error("MCP writer task failed")]
    WriterTask,
}

impl From<AuditError> for TransportError {
    fn from(_source: AuditError) -> Self {
        Self::Audit
    }
}

impl From<DeliveryFailure> for TransportError {
    fn from(failure: DeliveryFailure) -> Self {
        match failure {
            DeliveryFailure::PeerClosed => Self::PeerClosed,
            DeliveryFailure::TimedOut => Self::WriteTimedOut,
            DeliveryFailure::Io => Self::Io,
            DeliveryFailure::Audit => Self::Audit,
        }
    }
}

pub(crate) const fn output_peer_closed() -> u8 {
    OUTPUT_PEER_CLOSED
}

pub(crate) const fn output_write_timed_out() -> u8 {
    OUTPUT_WRITE_TIMED_OUT
}

pub(crate) const fn output_queue_timed_out() -> u8 {
    OUTPUT_QUEUE_TIMED_OUT
}

pub(crate) const fn output_io_failed() -> u8 {
    OUTPUT_IO_FAILED
}

pub(crate) const fn output_audit_failed() -> u8 {
    OUTPUT_AUDIT_FAILED
}
