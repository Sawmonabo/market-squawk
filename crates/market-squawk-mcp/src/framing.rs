//! Maximum-plus-one input framing and bounded, acknowledged output delivery.

use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU8, Ordering},
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
    AuditCompletion, AuditCompletionReservation, AuditError, AuditEvent, AuditOperation,
    AuditResultClass, AuditSink, LocalProcessIdentityClass, McpLimits, MutationAuditBundle,
    MutationAuditReservation,
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
            let waiting_for_fragmented_line_feed = self.frame_len
                == self.maximum_bytes.saturating_add(1)
                && self.frame.get(self.maximum_bytes) == Some(&b'\r');
            if self.frame_len > self.maximum_bytes && !waiting_for_fragmented_line_feed {
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

#[derive(Debug)]
pub(crate) struct PendingAudit {
    pub(crate) request_id: ServiceRequestId,
    pub(crate) operation: AuditOperation,
    cancellation_requested: bool,
    service_dispatched: bool,
    mutation: Option<MutationAuditReservation>,
}

impl PendingAudit {
    pub(crate) fn new(request_id: ServiceRequestId, operation: AuditOperation) -> Self {
        Self {
            request_id,
            operation,
            cancellation_requested: false,
            service_dispatched: false,
            mutation: None,
        }
    }

    pub(crate) fn new_mutation(
        request_id: ServiceRequestId,
        operation: AuditOperation,
        mutation: MutationAuditReservation,
    ) -> Self {
        Self {
            request_id,
            operation,
            cancellation_requested: false,
            service_dispatched: false,
            mutation: Some(mutation),
        }
    }

    fn begin_service_dispatch(&mut self) -> bool {
        if self.cancellation_requested {
            return false;
        }
        self.service_dispatched = true;
        true
    }

    fn commit_mutation_service(
        &mut self,
        result_class: AuditResultClass,
    ) -> Result<(), AuditError> {
        if let Some(mutation) = &mut self.mutation {
            mutation.commit_service(result_class)?;
        }
        Ok(())
    }

    fn commit_undispatched_mutation_service(
        &mut self,
        result_class: AuditResultClass,
    ) -> Result<(), AuditError> {
        if !self.service_dispatched {
            self.commit_mutation_service(result_class)?;
        }
        Ok(())
    }

    fn reserve_delivery(
        &mut self,
        completion: AuditCompletion,
        audit: &dyn AuditSink,
    ) -> Result<AuditCompletionReservation, AuditError> {
        match &mut self.mutation {
            Some(mutation) => mutation.reserve_delivery(completion),
            None => audit.reserve_completion(completion),
        }
    }

    fn shutdown_result_class(&self, default: AuditResultClass) -> AuditResultClass {
        if self.cancellation_requested {
            AuditResultClass::Cancelled
        } else {
            default
        }
    }

    fn needs_delivery(&self) -> bool {
        self.mutation
            .as_ref()
            .is_none_or(MutationAuditReservation::delivery_pending)
    }

    fn can_remove(&self) -> bool {
        self.mutation
            .as_ref()
            .is_none_or(MutationAuditReservation::is_terminalized)
    }

    fn mutation_is_terminalized(&self) -> bool {
        self.mutation
            .as_ref()
            .is_some_and(MutationAuditReservation::is_terminalized)
    }
}

#[derive(Debug)]
struct ReservedCompletion {
    intended_result_class: AuditResultClass,
    reservation: AuditCompletionReservation,
}

impl ReservedCompletion {
    fn commit(self, result_class: AuditResultClass) -> Result<(), AuditError> {
        self.reservation.commit(result_class)
    }
}

#[derive(Debug)]
pub(crate) struct OutboundMessage {
    encoded: Vec<u8>,
    completion: Option<ReservedCompletion>,
    deadline: tokio::time::Instant,
    acknowledgement: oneshot::Sender<Result<(), DeliveryFailure>>,
    _retained_bytes: OwnedSemaphorePermit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DeliveryFailure {
    Audit,
    PeerClosed,
    TimedOut,
    Io,
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

    pub(crate) fn reserve_mutation(
        &self,
        bundle: MutationAuditBundle,
    ) -> Result<MutationAuditReservation, AuditError> {
        self.audit.reserve_mutation(bundle)
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

    pub(crate) fn begin_service_dispatch(
        &self,
        request_id: &RequestId,
    ) -> Result<bool, TransportError> {
        let mut pending = self.pending.lock().map_err(|_| TransportError::State)?;
        Ok(pending
            .get_mut(request_id)
            .is_some_and(PendingAudit::begin_service_dispatch))
    }

    pub(crate) fn commit_mutation_service(
        &self,
        request_id: &RequestId,
        result_class: AuditResultClass,
    ) -> Result<(), TransportError> {
        let mut pending = self.pending.lock().map_err(|_| TransportError::State)?;
        let remove = if let Some(audit) = pending.get_mut(request_id) {
            audit.commit_mutation_service(result_class).map_err(|_| {
                self.fail(OUTPUT_AUDIT_FAILED);
                TransportError::Audit
            })?;
            audit.mutation_is_terminalized()
        } else {
            false
        };
        if remove {
            pending.remove(request_id);
        }
        Ok(())
    }

    pub(crate) fn complete_cancelled(&self, request_id: &RequestId) -> Result<(), TransportError> {
        let mut pending = self.pending.lock().map_err(|_| TransportError::State)?;
        let (completion, remove) = {
            let Some(audit) = pending
                .get_mut(request_id)
                .filter(|audit| audit.cancellation_requested && audit.needs_delivery())
            else {
                return Ok(());
            };
            audit
                .commit_undispatched_mutation_service(AuditResultClass::Cancelled)
                .map_err(|_| {
                    self.fail(OUTPUT_AUDIT_FAILED);
                    TransportError::Audit
                })?;
            let completion =
                self.reserve_completion(audit, AuditResultClass::Cancelled, b"request cancelled")?;
            (completion, audit.can_remove())
        };
        if remove {
            pending.remove(request_id);
        }
        drop(pending);
        completion
            .commit(AuditResultClass::Cancelled)
            .map_err(|_| {
                self.fail(OUTPUT_AUDIT_FAILED);
                TransportError::Audit
            })?;
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

        let completion = match response {
            Some((request_id, result_class)) => {
                let Some(completion) =
                    self.reserve_pending_completion(&request_id, result_class, &encoded)?
                else {
                    return Ok(());
                };
                Some(completion)
            }
            None => None,
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
        let completion = pending
            .map(|mut audit| {
                audit.commit_mutation_service(result_class).map_err(|_| {
                    self.fail(OUTPUT_AUDIT_FAILED);
                    TransportError::Audit
                })?;
                self.reserve_completion(&mut audit, result_class, &encoded)
            })
            .transpose()?;
        self.enqueue(encoded, completion).await
    }

    async fn enqueue(
        &self,
        encoded: Vec<u8>,
        completion: Option<ReservedCompletion>,
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
                self.fail(OUTPUT_IO_FAILED);
                return Err(TransportError::WriterTask);
            }
            Err(_) => {
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
            Ok(Err(_)) => return Err(self.current_error()),
            Err(_) => {
                self.fail(OUTPUT_QUEUE_TIMED_OUT);
                return Err(TransportError::WriteTimedOut);
            }
        };
        queue_permit.send(outbound);
        match delivered.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(failure)) => Err(failure.into()),
            Err(_) => {
                self.fail(OUTPUT_IO_FAILED);
                Err(TransportError::WriterTask)
            }
        }
    }

    fn reserve_completion(
        &self,
        audit: &mut PendingAudit,
        intended_result_class: AuditResultClass,
        encoded: &[u8],
    ) -> Result<ReservedCompletion, TransportError> {
        let audit_completion = AuditCompletion::new(
            &audit.request_id,
            self.identity_class,
            audit.operation.clone(),
            self.limits.service_limits(),
            encoded,
        )?;
        let reservation = audit
            .reserve_delivery(audit_completion, self.audit.as_ref())
            .map_err(|_error| {
                self.fail(OUTPUT_AUDIT_FAILED);
                TransportError::Audit
            })?;
        Ok(ReservedCompletion {
            intended_result_class,
            reservation,
        })
    }

    fn reserve_pending_completion(
        &self,
        request_id: &RequestId,
        intended_result_class: AuditResultClass,
        encoded: &[u8],
    ) -> Result<Option<ReservedCompletion>, TransportError> {
        let mut pending = self.pending.lock().map_err(|_| TransportError::State)?;
        let (completion, remove) = {
            let Some(audit) = pending
                .get_mut(request_id)
                .filter(|audit| !audit.cancellation_requested && audit.needs_delivery())
            else {
                return Ok(None);
            };
            audit
                .commit_undispatched_mutation_service(intended_result_class)
                .map_err(|_| {
                    self.fail(OUTPUT_AUDIT_FAILED);
                    TransportError::Audit
                })?;
            let completion = self.reserve_completion(audit, intended_result_class, encoded)?;
            (completion, audit.can_remove())
        };
        if remove {
            pending.remove(request_id);
        }
        Ok(Some(completion))
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
        loop {
            let mut pending = self.pending.lock().map_err(|_| TransportError::State)?;
            let Some(request_id) = pending
                .iter()
                .find_map(|(request_id, audit)| audit.needs_delivery().then(|| request_id.clone()))
            else {
                return Ok(());
            };
            let (completion, terminal_class, remove) = {
                let audit = pending.get_mut(&request_id).ok_or(TransportError::State)?;
                let terminal_class = audit.shutdown_result_class(result_class);
                audit
                    .commit_undispatched_mutation_service(terminal_class)
                    .map_err(|_| {
                        self.fail(OUTPUT_AUDIT_FAILED);
                        TransportError::Audit
                    })?;
                let completion = self.reserve_completion(audit, terminal_class, terminal_marker)?;
                (completion, terminal_class, audit.can_remove())
            };
            if remove {
                pending.remove(&request_id);
            }
            drop(pending);
            completion.commit(terminal_class).map_err(|_| {
                self.fail(OUTPUT_AUDIT_FAILED);
                TransportError::Audit
            })?;
        }
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
        let completion = outbound.completion;
        let result = tokio::time::timeout_at(outbound.deadline, async {
            writer.write_all(&outbound.encoded).await?;
            writer.flush().await
        })
        .await;
        let delivery = match result {
            Ok(Ok(())) => {
                let committed = if let Some(completion) = completion {
                    let result_class = completion.intended_result_class;
                    completion.commit(result_class)
                } else {
                    Ok(())
                };
                if committed.is_err() {
                    output.fail(OUTPUT_AUDIT_FAILED);
                    Err(DeliveryFailure::Audit)
                } else {
                    Ok(())
                }
            }
            Ok(Err(error)) if error.kind() == std::io::ErrorKind::BrokenPipe => {
                let committed = completion.map_or(Ok(()), |completion| {
                    completion.commit(AuditResultClass::OutputUnavailable)
                });
                if committed.is_err() {
                    output.fail(OUTPUT_AUDIT_FAILED);
                    Err(DeliveryFailure::Audit)
                } else {
                    output.fail(OUTPUT_PEER_CLOSED);
                    Err(DeliveryFailure::PeerClosed)
                }
            }
            Ok(Err(_)) => {
                let committed = completion.map_or(Ok(()), |completion| {
                    completion.commit(AuditResultClass::OutputUnavailable)
                });
                if committed.is_err() {
                    output.fail(OUTPUT_AUDIT_FAILED);
                    Err(DeliveryFailure::Audit)
                } else {
                    output.fail(OUTPUT_IO_FAILED);
                    Err(DeliveryFailure::Io)
                }
            }
            Err(_) => {
                let committed = completion.map_or(Ok(()), |completion| {
                    completion.commit(AuditResultClass::OutputUnavailable)
                });
                if committed.is_err() {
                    output.fail(OUTPUT_AUDIT_FAILED);
                    Err(DeliveryFailure::Audit)
                } else {
                    output.fail(OUTPUT_WRITE_TIMED_OUT);
                    Err(DeliveryFailure::TimedOut)
                }
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
                ServerResult::CallToolResult(result) if result.is_error == Some(true) => {
                    AuditResultClass::ServiceRejected
                }
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
            DeliveryFailure::Audit => Self::Audit,
            DeliveryFailure::PeerClosed => Self::PeerClosed,
            DeliveryFailure::TimedOut => Self::WriteTimedOut,
            DeliveryFailure::Io => Self::Io,
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

#[cfg(test)]
mod tests {
    use std::{error::Error, sync::atomic::AtomicUsize, time::Duration};

    use market_squawk_services::RequestId as ServiceRequestId;
    use rmcp::model::{RequestId, ServerJsonRpcMessage, ServerResult};
    use tokio::sync::Notify;

    use super::*;
    use crate::{McpLimitSpec, McpLimits};

    #[derive(Debug, Default)]
    struct TrackingAudit {
        events: Arc<Mutex<Vec<AuditEvent>>>,
        reservations: AtomicUsize,
        changed: Notify,
    }

    impl TrackingAudit {
        async fn wait_for_reservations(&self, count: usize) {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.reservations.load(Ordering::SeqCst) >= count {
                    return;
                }
                changed.as_mut().await;
            }
        }

        fn events(&self) -> Result<Vec<AuditEvent>, AuditError> {
            self.events
                .lock()
                .map(|events| events.clone())
                .map_err(|_| AuditError::Unavailable)
        }
    }

    impl AuditSink for TrackingAudit {
        fn record(&self, event: AuditEvent) -> Result<(), AuditError> {
            self.events
                .lock()
                .map_err(|_| AuditError::Unavailable)?
                .push(event);
            Ok(())
        }

        fn reserve_completion(
            &self,
            completion: AuditCompletion,
        ) -> Result<AuditCompletionReservation, AuditError> {
            let events = Arc::clone(&self.events);
            self.reservations.fetch_add(1, Ordering::SeqCst);
            self.changed.notify_waiters();
            Ok(AuditCompletionReservation::new(completion, move |event| {
                events
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(event);
                Ok(())
            }))
        }

        fn reserve_mutation(
            &self,
            bundle: MutationAuditBundle,
        ) -> Result<MutationAuditReservation, AuditError> {
            let admitted = Arc::clone(&self.events);
            let service = Arc::clone(&self.events);
            let delivery = Arc::clone(&self.events);
            MutationAuditReservation::try_new(
                bundle,
                move |event| {
                    admitted
                        .lock()
                        .map_err(|_| AuditError::Unavailable)?
                        .push(event);
                    Ok(())
                },
                move |event| {
                    service
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(event);
                    Ok(())
                },
                move |event| {
                    delivery
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(event);
                    Ok(())
                },
            )
        }
    }

    #[test]
    fn marked_cancellation_pins_its_generation_until_the_callback_claims_it()
    -> Result<(), Box<dyn Error>> {
        let limits = McpLimits::try_from(McpLimitSpec::default())?;
        let audit = Arc::new(TrackingAudit::default());
        let (sender, _receiver) = mpsc::channel(1);
        let output = OutputChannel::new(
            sender,
            Arc::new(AtomicU8::new(OUTPUT_RUNNING)),
            limits,
            audit.clone(),
            LocalProcessIdentityClass::CallerSuppliedIoUnverified,
        );
        let request_id = RequestId::String(Arc::from("reused-id"));
        let pending = || {
            ServiceRequestId::try_string(Arc::from("reused-id"))
                .map(|id| PendingAudit::new(id, AuditOperation::Ping))
        };

        assert!(matches!(
            output.admit_active(request_id.clone(), pending()?, 1)?,
            ActiveAdmission::Accepted
        ));
        output.mark_pending_cancelled(&request_id)?;
        assert!(
            output
                .reserve_pending_completion(
                    &request_id,
                    AuditResultClass::Succeeded,
                    b"late response",
                )?
                .is_none()
        );
        assert!(matches!(
            output.admit_active(request_id.clone(), pending()?, 1)?,
            ActiveAdmission::Duplicate(_)
        ));

        output.complete_cancelled(&request_id)?;
        assert!(matches!(
            output.admit_active(request_id.clone(), pending()?, 1)?,
            ActiveAdmission::Accepted
        ));
        output.complete_cancelled(&request_id)?;
        let replacement = output
            .reserve_pending_completion(
                &request_id,
                AuditResultClass::Succeeded,
                b"replacement response",
            )?
            .ok_or("delayed cancellation callback claimed the replacement generation")?;
        replacement.commit(AuditResultClass::Succeeded)?;

        let events = audit.events()?;
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].result_class(), Some(AuditResultClass::Cancelled));
        assert_eq!(events[1].result_class(), Some(AuditResultClass::Succeeded));
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_enqueue_and_queue_drop_terminalize_reserved_audits()
    -> Result<(), Box<dyn Error>> {
        let limits = McpLimits::try_from(McpLimitSpec {
            writer_queue_capacity: 1,
            write_timeout: Duration::from_secs(1),
            shutdown_timeout: Duration::from_millis(10),
            ..McpLimitSpec::default()
        })?;
        let audit = Arc::new(TrackingAudit::default());
        let (sender, receiver) = mpsc::channel(1);
        let output = Arc::new(OutputChannel::new(
            sender,
            Arc::new(AtomicU8::new(OUTPUT_RUNNING)),
            limits,
            audit.clone(),
            LocalProcessIdentityClass::CallerSuppliedIoUnverified,
        ));
        for id in ["first", "second"] {
            let service_id = ServiceRequestId::try_string(Arc::from(id))?;
            output.record_admitted(AuditEvent::admitted(
                &service_id,
                output.identity_class(),
                AuditOperation::Ping,
                limits.service_limits(),
                id.as_bytes(),
            )?)?;
            assert!(matches!(
                output.admit_active(
                    RequestId::String(Arc::from(id)),
                    PendingAudit::new(service_id, AuditOperation::Ping),
                    2,
                )?,
                ActiveAdmission::Accepted
            ));
        }

        let first_output = Arc::clone(&output);
        let first = tokio::spawn(async move {
            first_output
                .send_message(ServerJsonRpcMessage::response(
                    ServerResult::empty(()),
                    RequestId::String(Arc::from("first")),
                ))
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), audit.wait_for_reservations(1)).await?;

        let second_output = Arc::clone(&output);
        let second = tokio::spawn(async move {
            second_output
                .send_message(ServerJsonRpcMessage::response(
                    ServerResult::empty(()),
                    RequestId::String(Arc::from("second")),
                ))
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), audit.wait_for_reservations(2)).await?;
        second.abort();
        assert!(second.await.is_err());
        drop(receiver);
        assert!(first.await?.is_err());

        let events = audit.events()?;
        assert_eq!(
            events
                .iter()
                .filter(|event| event.phase() == crate::AuditPhase::Admitted)
                .count(),
            2
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| {
                    event.result_class() == Some(AuditResultClass::OutputUnavailable)
                })
                .count(),
            2
        );
        assert_eq!(events.len(), 4);
        Ok(())
    }
}
