use std::{
    collections::VecDeque,
    error::Error,
    num::NonZeroUsize,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use market_squawk_mcp::{
    ArtifactError, ArtifactPublication, ArtifactPublicationContext, ArtifactRead,
    ArtifactReadContext, ArtifactReadRequest, ArtifactReference, ArtifactRepository,
    AuditCompletion, AuditCompletionReservation, AuditError, AuditEvent, AuditOperation,
    AuditPhase, AuditResultClass, AuditSink, McpLimitError, McpLimitSpec, McpLimits, McpServer,
    MutationAuditBundle, MutationAuditReservation, ServerExit,
};
use market_squawk_services::{
    JsonStructureLimits, RequestContext, ScopeRequirement, ServiceCapabilities, ServiceDomain,
    ServiceError, ServiceLimits, SourceEvidencePolicy, TOOL_SOURCE_COVERAGE_FIELD,
    ToolArtifactPolicy, ToolAuthorization, ToolContract, ToolDescriptor, ToolEffects,
    ToolInputError, ToolResultMetadata, ToolResultPolicy, ToolScope, ToolServices,
    TypedToolRequest, TypedToolResult,
};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, ReadBuf};
use tokio::sync::{Notify, Semaphore, oneshot};
use tokio_util::sync::CancellationToken;

const TRACE_SENTINEL: &str = "trace-sentinel-private-value";

#[derive(Debug, Default)]
struct CountingAudit(Arc<Mutex<Vec<AuditEvent>>>);

impl CountingAudit {
    fn result_classes(&self) -> Result<Vec<AuditResultClass>, AuditError> {
        self.0
            .lock()
            .map(|events| events.iter().filter_map(AuditEvent::result_class).collect())
            .map_err(|_| AuditError::Unavailable)
    }

    fn tool_result_classes(&self) -> Result<Vec<AuditResultClass>, AuditError> {
        self.0
            .lock()
            .map(|events| {
                events
                    .iter()
                    .filter(|event| matches!(event.operation(), AuditOperation::CallTool { .. }))
                    .filter_map(AuditEvent::result_class)
                    .collect()
            })
            .map_err(|_| AuditError::Unavailable)
    }
}

#[derive(Debug, Default)]
struct RejectingCompletionAudit;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MutationAuditFault {
    None,
    Reservation,
    AdmissionPersist,
    ServiceTerminal,
    DeliveryTerminal,
}

#[derive(Debug)]
struct AtomicMutationAudit {
    events: Arc<Mutex<Vec<AuditEvent>>>,
    fault: MutationAuditFault,
}

impl AtomicMutationAudit {
    fn new(fault: MutationAuditFault) -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
            fault,
        }
    }

    fn events(&self) -> Result<Vec<AuditEvent>, AuditError> {
        self.events
            .lock()
            .map(|events| events.clone())
            .map_err(|_| AuditError::Unavailable)
    }
}

impl AuditSink for RejectingCompletionAudit {
    fn record(&self, event: AuditEvent) -> Result<(), AuditError> {
        if event.result_class().is_some() {
            Err(AuditError::Unavailable)
        } else {
            Ok(())
        }
    }

    fn reserve_completion(
        &self,
        _completion: AuditCompletion,
    ) -> Result<AuditCompletionReservation, AuditError> {
        Err(AuditError::Unavailable)
    }

    fn reserve_mutation(
        &self,
        _bundle: MutationAuditBundle,
    ) -> Result<MutationAuditReservation, AuditError> {
        Err(AuditError::Unavailable)
    }
}

impl AuditSink for CountingAudit {
    fn record(&self, event: AuditEvent) -> Result<(), AuditError> {
        self.0
            .lock()
            .map_err(|_| AuditError::Unavailable)?
            .push(event);
        Ok(())
    }

    fn reserve_completion(
        &self,
        completion: AuditCompletion,
    ) -> Result<AuditCompletionReservation, AuditError> {
        let events = Arc::clone(&self.0);
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
        let admitted = Arc::clone(&self.0);
        let service = Arc::clone(&self.0);
        let delivery = Arc::clone(&self.0);
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

impl AuditSink for AtomicMutationAudit {
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
        if self.fault == MutationAuditFault::Reservation {
            return Err(AuditError::Unavailable);
        }
        let admitted = Arc::clone(&self.events);
        let service = Arc::clone(&self.events);
        let delivery = Arc::clone(&self.events);
        let fail_admission = self.fault == MutationAuditFault::AdmissionPersist;
        let fail_service_terminal = self.fault == MutationAuditFault::ServiceTerminal;
        let fail_delivery_terminal = self.fault == MutationAuditFault::DeliveryTerminal;
        MutationAuditReservation::try_new(
            bundle,
            move |event| {
                if fail_admission {
                    return Err(AuditError::Unavailable);
                }
                admitted
                    .lock()
                    .map_err(|_| AuditError::Unavailable)?
                    .push(event);
                Ok(())
            },
            move |event| {
                if !fail_service_terminal {
                    service
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(event);
                }
                if fail_service_terminal {
                    return Err(AuditError::Unavailable);
                }
                Ok(())
            },
            move |event| {
                if fail_delivery_terminal {
                    return Err(AuditError::Unavailable);
                }
                delivery
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(event);
                Ok(())
            },
        )
    }
}

#[derive(Debug, Default)]
struct RecordingArtifacts {
    publications: Mutex<Vec<ArtifactPublication>>,
}

#[derive(Clone, Debug)]
struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

#[derive(Debug)]
struct CaptureGuard(Arc<Mutex<Vec<u8>>>);

impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for CaptureWriter {
    type Writer = CaptureGuard;

    fn make_writer(&'writer self) -> Self::Writer {
        CaptureGuard(Arc::clone(&self.0))
    }
}

impl std::io::Write for CaptureGuard {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .map_err(|_| std::io::Error::other("trace capture is poisoned"))?
            .extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl RecordingArtifacts {
    fn publication_count(&self) -> Result<usize, ArtifactError> {
        self.publications
            .lock()
            .map(|publications| publications.len())
            .map_err(|_| ArtifactError::Unavailable)
    }
}

#[async_trait]
impl ArtifactRepository for RecordingArtifacts {
    async fn publish(
        &self,
        publication: ArtifactPublication,
        _context: ArtifactPublicationContext,
    ) -> Result<ArtifactReference, ArtifactError> {
        let reference = ArtifactReference::try_new(
            format!("artifact_{}", publication.sha256_hex()),
            publication.sha256_hex(),
            publication.byte_count(),
            publication.media_type(),
        )?;
        self.publications
            .lock()
            .map_err(|_| ArtifactError::Unavailable)?
            .push(publication);
        Ok(reference)
    }

    async fn read(
        &self,
        request: ArtifactReadRequest,
        context: ArtifactReadContext,
    ) -> Result<ArtifactRead, ArtifactError> {
        context.ensure_live()?;
        let publications = self
            .publications
            .lock()
            .map_err(|_| ArtifactError::Unavailable)?;
        let publication = publications
            .iter()
            .find(|publication| publication.sha256_hex() == request.reference().sha256())
            .ok_or(ArtifactError::NotFound)?;
        ArtifactRead::try_new(request.into_reference(), publication.content().to_vec())
    }
}

#[derive(Debug, Default)]
struct BoundaryService {
    calls: AtomicUsize,
}

fn boundary_contract(source_evidence: SourceEvidencePolicy) -> ToolContract {
    let source_coverage = match source_evidence {
        SourceEvidencePolicy::Required => ScopeRequirement::Required,
        SourceEvidencePolicy::NotApplicable => ScopeRequirement::NotApplicable,
    };
    ToolContract::new(
        ServiceDomain::Research,
        ToolAuthorization::ReadOnly,
        ToolScope::new(
            ScopeRequirement::NotApplicable,
            ScopeRequirement::NotApplicable,
            ScopeRequirement::NotApplicable,
            source_coverage,
        ),
        ToolResultPolicy::new(source_evidence, ToolArtifactPolicy::OpaqueOnOverflow),
    )
}

fn confirmed_mutation_contract() -> ToolContract {
    ToolContract::new(
        ServiceDomain::Bot,
        ToolAuthorization::LocalConfirmation,
        ToolScope::new(
            ScopeRequirement::NotApplicable,
            ScopeRequirement::NotApplicable,
            ScopeRequirement::NotApplicable,
            ScopeRequirement::NotApplicable,
        ),
        ToolResultPolicy::new(
            SourceEvidencePolicy::NotApplicable,
            ToolArtifactPolicy::InlineOnly,
        ),
    )
}

fn boundary_schema(source_evidence: SourceEvidencePolicy) -> Value {
    match source_evidence {
        SourceEvidencePolicy::Required => json!({
            "type":"object",
            "properties":{TOOL_SOURCE_COVERAGE_FIELD:{"type":"object"}},
            "required":[TOOL_SOURCE_COVERAGE_FIELD],
            "additionalProperties":false
        }),
        SourceEvidencePolicy::NotApplicable => {
            json!({"type":"object","properties":{},"additionalProperties":false})
        }
    }
}

fn boundary_output_schema(name: &str) -> Value {
    let (field, schema) = match name {
        "test.large" => ("privatePayload", json!({"type":"string"})),
        "test.loose" => ("items", json!({"type":"array","items":{"type":"string"}})),
        "test.invalid-evidence" => ("invalid", json!({"type":"boolean"})),
        "test.invalid-output" => ("expected", json!({"type":"boolean"})),
        "test.block" | "test.failure" => return json!({"type":"null"}),
        _ => return json!({"type":"null"}),
    };
    json!({
        "type":"object",
        "properties":{(field):schema},
        "required":[field],
        "additionalProperties":false
    })
}

fn admit_boundary_scope(
    arguments: &serde_json::Map<String, Value>,
    source_evidence: SourceEvidencePolicy,
) -> Result<(), ToolInputError> {
    match source_evidence {
        SourceEvidencePolicy::Required => (arguments.len() == 1
            && arguments
                .get(TOOL_SOURCE_COVERAGE_FIELD)
                .is_some_and(Value::is_object))
        .then_some(())
        .ok_or(ToolInputError::Invalid),
        SourceEvidencePolicy::NotApplicable => arguments
            .is_empty()
            .then_some(())
            .ok_or(ToolInputError::Invalid),
    }
}

impl BoundaryService {
    fn capabilities() -> ServiceCapabilities {
        let descriptors = [
            (
                "test.large",
                "Return a result larger than the inline ceiling.",
                SourceEvidencePolicy::Required,
            ),
            (
                "test.loose",
                "Return a result constructed under a looser service contract.",
                SourceEvidencePolicy::NotApplicable,
            ),
            (
                "test.block",
                "Wait until the transport deadline expires.",
                SourceEvidencePolicy::NotApplicable,
            ),
            (
                "test.invalid-evidence",
                "Return a result that violates its declared source-evidence policy.",
                SourceEvidencePolicy::Required,
            ),
            (
                "test.failure",
                "Return one bounded test-only runtime failure after dispatch.",
                SourceEvidencePolicy::NotApplicable,
            ),
            (
                "test.invalid-output",
                "Return a result that violates its descriptor-owned output schema.",
                SourceEvidencePolicy::NotApplicable,
            ),
        ]
        .into_iter()
        .filter_map(|(name, description, source_evidence)| {
            ToolDescriptor::try_new_with_output(
                name,
                "1",
                description,
                boundary_schema(source_evidence),
                boundary_output_schema(name),
                boundary_contract(source_evidence),
                ToolEffects::read_only_closed_world(),
                move |arguments: &serde_json::Map<String, Value>| {
                    admit_boundary_scope(arguments, source_evidence)
                },
            )
            .ok()
        })
        .collect();
        ServiceCapabilities::try_new(descriptors).unwrap_or_else(|_| ServiceCapabilities::empty())
    }
}

#[async_trait]
impl ToolServices for BoundaryService {
    fn capabilities(&self) -> ServiceCapabilities {
        Self::capabilities()
    }

    async fn call(
        &self,
        request: TypedToolRequest,
        context: RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match request.name() {
            "test.large" => TypedToolResult::try_new(
                json!({"privatePayload": "sensitive-value".repeat(64)}),
                1,
                ToolResultMetadata::try_truncated(
                    2,
                    json!({"status":"partial","venues":["test"]}),
                    json!(["aggregated"]),
                )?,
                context.limits(),
            )
            .map_err(Into::into),
            "test.loose" => {
                let structure = JsonStructureLimits::try_new(8, 16 * 1024, 16, 16)
                    .map_err(|_| ServiceError::Internal)?;
                let loose = ServiceLimits::try_new(64, 1, 16 * 1024, 16, structure)
                    .map_err(|_| ServiceError::Internal)?;
                TypedToolResult::try_new(
                    json!({"items": ["x".repeat(8 * 1024)]}),
                    1,
                    ToolResultMetadata::complete_not_applicable(),
                    loose,
                )
                .map_err(Into::into)
            }
            "test.block" => {
                context.cancellation().cancelled().await;
                Err(ServiceError::Cancelled)
            }
            "test.invalid-evidence" => TypedToolResult::try_new(
                json!({"invalid": true}),
                1,
                ToolResultMetadata::complete_not_applicable(),
                context.limits(),
            )
            .map_err(Into::into),
            "test.failure" => Err(ServiceError::Unavailable),
            "test.invalid-output" => TypedToolResult::try_new(
                json!({"unexpected": true}),
                1,
                ToolResultMetadata::complete_not_applicable(),
                context.limits(),
            )
            .map_err(Into::into),
            _ => Err(ServiceError::NotFound),
        }
    }
}

#[derive(Debug, Default)]
struct TraceService;

#[derive(Debug)]
struct KillSwitchService {
    mutations: AtomicUsize,
    audit: Arc<AtomicMutationAudit>,
    work: Option<Arc<HeldWork>>,
}

impl KillSwitchService {
    fn new(audit: Arc<AtomicMutationAudit>) -> Self {
        Self {
            mutations: AtomicUsize::new(0),
            audit,
            work: None,
        }
    }

    fn held(audit: Arc<AtomicMutationAudit>, work: Arc<HeldWork>) -> Self {
        Self {
            mutations: AtomicUsize::new(0),
            audit,
            work: Some(work),
        }
    }
}

#[async_trait]
impl ToolServices for KillSwitchService {
    fn capabilities(&self) -> ServiceCapabilities {
        let effects = ToolEffects::try_new(false, true, false, false);
        let descriptor = effects.ok().and_then(|effects| {
            ToolDescriptor::try_new_with_output(
                "test.kill-switch",
                "1",
                "Irreversibly trigger the test-only kill switch.",
                json!({
                    "type":"object",
                    "properties":{"confirm":{"type":"boolean","const":true}},
                    "required":["confirm"],
                    "additionalProperties":false
                }),
                json!({
                    "type":"object",
                    "properties":{"triggered":{"type":"boolean"}},
                    "required":["triggered"],
                    "additionalProperties":false
                }),
                confirmed_mutation_contract(),
                effects,
                |_arguments: &serde_json::Map<String, Value>| Ok(()),
            )
            .ok()
        });
        ServiceCapabilities::try_new(descriptor.into_iter().collect())
            .unwrap_or_else(|_| ServiceCapabilities::empty())
    }

    async fn call(
        &self,
        _request: TypedToolRequest,
        context: RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        if let Some(work) = &self.work {
            work.hold().await;
        }
        if !self
            .audit
            .events()
            .map_err(|_| ServiceError::Unavailable)?
            .iter()
            .any(|event| event.phase() == AuditPhase::MutationAdmitted)
        {
            return Err(ServiceError::Internal);
        }
        self.mutations.fetch_add(1, Ordering::SeqCst);
        TypedToolResult::try_new(
            json!({"triggered":true}),
            1,
            ToolResultMetadata::complete_not_applicable(),
            context.limits(),
        )
        .map_err(Into::into)
    }
}

#[async_trait]
impl ToolServices for TraceService {
    fn capabilities(&self) -> ServiceCapabilities {
        let descriptor = ToolDescriptor::try_new_with_output(
            "test.trace",
            "1",
            "Echo a test sentinel through the bounded result path.",
            json!({
                "type":"object",
                "properties":{"secret":{"type":"string"}},
                "required":["secret"],
                "additionalProperties":false
            }),
            json!({
                "type":"object",
                "properties":{"echo":{"type":"string"}},
                "required":["echo"],
                "additionalProperties":false
            }),
            boundary_contract(SourceEvidencePolicy::NotApplicable),
            ToolEffects::read_only_closed_world(),
            |arguments: &serde_json::Map<String, Value>| {
                (arguments.len() == 1 && arguments.get("secret").is_some_and(Value::is_string))
                    .then_some(())
                    .ok_or(ToolInputError::Invalid)
            },
        );
        ServiceCapabilities::try_new(descriptor.into_iter().collect())
            .unwrap_or_else(|_| ServiceCapabilities::empty())
    }

    async fn call(
        &self,
        request: TypedToolRequest,
        context: RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let secret = request
            .arguments()
            .get("secret")
            .cloned()
            .ok_or(ServiceError::InvalidRequest)?;
        TypedToolResult::try_new(
            json!({"echo": secret}),
            1,
            ToolResultMetadata::complete_not_applicable(),
            context.limits(),
        )
        .map_err(Into::into)
    }
}

async fn send<W: AsyncWrite + Unpin>(writer: &mut W, message: Value) -> Result<(), Box<dyn Error>> {
    writer.write_all(&serde_json::to_vec(&message)?).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    Ok(())
}

async fn receive<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
) -> Result<Value, Box<dyn Error>> {
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    Ok(serde_json::from_str(&line)?)
}

#[derive(Debug)]
struct InjectedWriteFailure<W> {
    inner: W,
    fail_next_write: Arc<AtomicBool>,
}

impl<W: AsyncWrite + Unpin> AsyncWrite for InjectedWriteFailure<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let this = self.get_mut();
        if this.fail_next_write.swap(false, Ordering::SeqCst) {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "injected peer output closure",
            )));
        }
        Pin::new(&mut this.inner).poll_write(context, buffer)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(context)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(context)
    }
}

async fn initialized_mutation_session(
    service: Arc<KillSwitchService>,
    audit: Arc<AtomicMutationAudit>,
    inject_output_failure: bool,
) -> Result<
    (
        BufReader<tokio::io::ReadHalf<tokio::io::DuplexStream>>,
        tokio::io::WriteHalf<tokio::io::DuplexStream>,
        tokio::task::JoinHandle<Result<ServerExit, market_squawk_mcp::ServerError>>,
    ),
    Box<dyn Error>,
> {
    let server = McpServer::try_new(
        service,
        McpLimits::try_from(McpLimitSpec::default())?,
        audit,
        Arc::new(RecordingArtifacts::default()),
    )?;
    let (client, server_io) = tokio::io::duplex(64 * 1024);
    let (server_reader, server_writer) = tokio::io::split(server_io);
    let fail_next_write = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn(server.serve_unverified_io(
        server_reader,
        InjectedWriteFailure {
            inner: server_writer,
            fail_next_write: Arc::clone(&fail_next_write),
        },
        CancellationToken::new(),
    ));
    let (client_reader, mut client_writer) = tokio::io::split(client);
    let mut client_reader = BufReader::new(client_reader);
    send(
        &mut client_writer,
        json!({
            "jsonrpc":"2.0","id":"initialize","method":"initialize",
            "params":{
                "protocolVersion":"2025-11-25","capabilities":{},
                "clientInfo":{"name":"tests","version":"1"}
            }
        }),
    )
    .await?;
    let _initialize = receive(&mut client_reader).await?;
    send(
        &mut client_writer,
        json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
    )
    .await?;
    fail_next_write.store(inject_output_failure, Ordering::SeqCst);
    Ok((client_reader, client_writer, task))
}

#[derive(Debug)]
struct SegmentedReader {
    segments: VecDeque<Vec<u8>>,
    offset: usize,
}

impl SegmentedReader {
    fn new(segments: impl IntoIterator<Item = Vec<u8>>) -> Self {
        Self {
            segments: segments.into_iter().collect(),
            offset: 0,
        }
    }
}

impl AsyncRead for SegmentedReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let (copied, segment_len) = {
            let Some(segment) = self.segments.front() else {
                return Poll::Ready(Ok(()));
            };
            let available = &segment[self.offset..];
            let copied = available.len().min(buffer.remaining());
            buffer.put_slice(&available[..copied]);
            (copied, segment.len())
        };
        self.offset += copied;
        if self.offset == segment_len {
            self.segments.pop_front();
            self.offset = 0;
        }
        Poll::Ready(Ok(()))
    }
}

async fn ready_server<S, A>(
    service: Arc<S>,
    artifacts: Arc<A>,
    limits: McpLimits,
) -> Result<
    (
        BufReader<tokio::io::ReadHalf<tokio::io::DuplexStream>>,
        tokio::io::WriteHalf<tokio::io::DuplexStream>,
        tokio::task::JoinHandle<Result<ServerExit, market_squawk_mcp::ServerError>>,
    ),
    Box<dyn Error>,
>
where
    S: ToolServices,
    A: ArtifactRepository,
{
    let server = McpServer::try_new(
        service,
        limits,
        Arc::new(CountingAudit::default()),
        artifacts,
    )?;
    let (client, server_io) = tokio::io::duplex(64 * 1024);
    let (server_reader, server_writer) = tokio::io::split(server_io);
    let task = tokio::spawn(server.serve_unverified_io(
        server_reader,
        server_writer,
        CancellationToken::new(),
    ));
    let (client_reader, mut client_writer) = tokio::io::split(client);
    let mut client_reader = BufReader::new(client_reader);
    send(
        &mut client_writer,
        json!({
            "jsonrpc":"2.0",
            "id":1,
            "method":"initialize",
            "params":{
                "protocolVersion":"2025-11-25",
                "capabilities":{},
                "clientInfo":{"name":"tests","version":"1"}
            }
        }),
    )
    .await?;
    let _initialize = receive(&mut client_reader).await?;
    send(
        &mut client_writer,
        json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
    )
    .await?;
    Ok((client_reader, client_writer, task))
}

#[derive(Debug)]
struct HeldWork {
    entered: AtomicUsize,
    completed: AtomicUsize,
    release: Semaphore,
    changed: Notify,
}

impl HeldWork {
    fn new() -> Self {
        Self {
            entered: AtomicUsize::new(0),
            completed: AtomicUsize::new(0),
            release: Semaphore::new(0),
            changed: Notify::new(),
        }
    }

    async fn hold(&self) {
        self.entered.fetch_add(1, Ordering::SeqCst);
        self.changed.notify_waiters();
        if let Ok(permit) = self.release.acquire().await {
            permit.forget();
            self.completed.fetch_add(1, Ordering::SeqCst);
            self.changed.notify_waiters();
        }
    }

    async fn wait_for(&self, counter: &AtomicUsize, expected: usize) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if counter.load(Ordering::SeqCst) >= expected {
                return;
            }
            changed.as_mut().await;
        }
    }
}

#[derive(Debug)]
struct NonCooperativeService {
    work: Arc<HeldWork>,
    drop_observer: Mutex<Option<oneshot::Sender<bool>>>,
}

#[derive(Debug)]
struct CancellationDropProbe {
    cancellation: CancellationToken,
    observer: Option<oneshot::Sender<bool>>,
}

impl Drop for CancellationDropProbe {
    fn drop(&mut self) {
        if let Some(observer) = self.observer.take() {
            let _ = observer.send(self.cancellation.is_cancelled());
        }
    }
}

#[async_trait]
impl ToolServices for NonCooperativeService {
    fn capabilities(&self) -> ServiceCapabilities {
        let descriptor = ToolDescriptor::try_new_with_output(
            "test.owned-work",
            "1",
            "Exercise bounded host work ownership.",
            json!({
                "type":"object",
                "properties":{"artifact":{"type":"boolean"}},
                "required":["artifact"],
                "additionalProperties":false
            }),
            json!({
                "oneOf":[
                    {
                        "type":"object",
                        "properties":{"payload":{"type":"string"}},
                        "required":["payload"],
                        "additionalProperties":false
                    },
                    {
                        "type":"object",
                        "properties":{"released":{"type":"boolean"}},
                        "required":["released"],
                        "additionalProperties":false
                    }
                ]
            }),
            boundary_contract(SourceEvidencePolicy::NotApplicable),
            ToolEffects::read_only_closed_world(),
            |arguments: &serde_json::Map<String, Value>| {
                arguments
                    .get("artifact")
                    .and_then(Value::as_bool)
                    .map(|_| ())
                    .ok_or(ToolInputError::Invalid)
            },
        );
        ServiceCapabilities::try_new(descriptor.into_iter().collect())
            .unwrap_or_else(|_| ServiceCapabilities::empty())
    }

    async fn call(
        &self,
        request: TypedToolRequest,
        context: RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let artifact = request
            .arguments()
            .get("artifact")
            .and_then(Value::as_bool)
            .ok_or(ServiceError::InvalidRequest)?;
        if artifact {
            TypedToolResult::try_new(
                json!({"payload":"x".repeat(512)}),
                1,
                ToolResultMetadata::complete_not_applicable(),
                context.limits(),
            )
            .map_err(Into::into)
        } else {
            let observer = self
                .drop_observer
                .lock()
                .map_err(|_| ServiceError::Unavailable)?
                .take();
            let _drop_probe = CancellationDropProbe {
                cancellation: context.cancellation().clone(),
                observer,
            };
            self.work.hold().await;
            TypedToolResult::try_new(
                json!({"released":true}),
                1,
                ToolResultMetadata::complete_not_applicable(),
                context.limits(),
            )
            .map_err(Into::into)
        }
    }
}

#[derive(Debug)]
struct NonCooperativeArtifacts {
    work: Arc<HeldWork>,
}

#[async_trait]
impl ArtifactRepository for NonCooperativeArtifacts {
    async fn publish(
        &self,
        publication: ArtifactPublication,
        _context: ArtifactPublicationContext,
    ) -> Result<ArtifactReference, ArtifactError> {
        self.work.hold().await;
        ArtifactReference::try_new(
            format!("artifact_{}", publication.sha256_hex()),
            publication.sha256_hex(),
            publication.byte_count(),
            publication.media_type(),
        )
    }

    async fn read(
        &self,
        _request: ArtifactReadRequest,
        _context: ArtifactReadContext,
    ) -> Result<ArtifactRead, ArtifactError> {
        Err(ArtifactError::Unavailable)
    }
}

#[tokio::test]
async fn hostile_json_limits_reject_before_service_dispatch() -> Result<(), Box<dyn Error>> {
    let service = Arc::new(BoundaryService::default());
    let artifacts = Arc::new(RecordingArtifacts::default());
    let spec = McpLimitSpec {
        maximum_frame_bytes: 32 * 1024,
        maximum_body_bytes: 32 * 1024,
        maximum_depth: 8,
        maximum_string_bytes: 64,
        maximum_array_items: 2,
        maximum_map_entries: 4,
        maximum_inline_bytes: 8 * 1024,
        ..McpLimitSpec::default()
    };
    let (mut reader, mut writer, task) =
        ready_server(Arc::clone(&service), artifacts, McpLimits::try_from(spec)?).await?;

    let hostile_arguments = [
        json!({"oversized": "x".repeat(65)}),
        json!({"oversized": [1, 2, 3]}),
        json!({"oversized": {"a":1,"b":2,"c":3,"d":4,"e":5}}),
        json!({"oversized": {"a":{"b":{"c":{"d":{"e":{"f":{"g":1}}}}}}}}),
    ];
    for (index, arguments) in hostile_arguments.into_iter().enumerate() {
        send(
            &mut writer,
            json!({
                "jsonrpc":"2.0",
                "id": 10 + index,
                "method":"tools/call",
                "params":{"name":"test.large","arguments":arguments}
            }),
        )
        .await?;
        assert_eq!(receive(&mut reader).await?["error"]["code"], -32010);
    }
    assert_eq!(service.calls.load(Ordering::SeqCst), 0);
    writer.shutdown().await?;
    assert_eq!(task.await??, ServerExit::EndOfInput);
    Ok(())
}

#[tokio::test]
async fn exact_limit_crlf_survives_a_fragmented_delimiter() -> Result<(), Box<dyn Error>> {
    const FRAME_BYTES: usize = 20 * 1_024;
    let mut initialize = serde_json::to_vec(&json!({
        "jsonrpc":"2.0","id":1,"method":"initialize",
        "params":{
            "protocolVersion":"2025-11-25","capabilities":{},
            "clientInfo":{"name":"tests","version":"1"}
        }
    }))?;
    initialize.push(b'\n');
    let mut initialized =
        serde_json::to_vec(&json!({"jsonrpc":"2.0","method":"notifications/initialized"}))?;
    initialized.push(b'\n');

    let empty_ping = serde_json::to_vec(&json!({
        "jsonrpc":"2.0","id":"crlf","method":"tools/list","params":{"cursor":""}
    }))?;
    let padding = FRAME_BYTES
        .checked_sub(empty_ping.len())
        .ok_or("ping envelope exceeds test frame")?;
    let ping = serde_json::to_vec(&json!({
        "jsonrpc":"2.0","id":"crlf","method":"tools/list",
        "params":{"cursor":"p".repeat(padding)}
    }))?;
    assert_eq!(ping.len(), FRAME_BYTES);
    let mut ping_with_cr = ping;
    ping_with_cr.push(b'\r');

    let input = SegmentedReader::new([initialize, initialized, ping_with_cr, vec![b'\n']]);
    let spec = McpLimitSpec {
        maximum_frame_bytes: FRAME_BYTES,
        maximum_body_bytes: FRAME_BYTES,
        maximum_inline_bytes: 64,
        maximum_writer_queue_bytes: FRAME_BYTES + 1,
        ..McpLimitSpec::default()
    };
    let server = McpServer::try_new(
        Arc::new(TraceService),
        McpLimits::try_from(spec)?,
        Arc::new(CountingAudit::default()),
        Arc::new(RecordingArtifacts::default()),
    )?;
    let (client, server_output) = tokio::io::duplex(8 * 1024);
    let task =
        tokio::spawn(server.serve_unverified_io(input, server_output, CancellationToken::new()));
    let mut reader = BufReader::new(client);
    assert_eq!(receive(&mut reader).await?["id"], 1);
    assert_eq!(receive(&mut reader).await?["error"]["code"], -32602);
    assert_eq!(task.await??, ServerExit::EndOfInput);
    Ok(())
}

#[tokio::test]
async fn rejected_completion_audit_releases_no_response_bytes() -> Result<(), Box<dyn Error>> {
    let mut initialize = serde_json::to_vec(&json!({
        "jsonrpc":"2.0","id":1,"method":"initialize",
        "params":{
            "protocolVersion":"2025-11-25","capabilities":{},
            "clientInfo":{"name":"tests","version":"1"}
        }
    }))?;
    initialize.push(b'\n');
    let server = McpServer::try_new(
        Arc::new(BoundaryService::default()),
        McpLimits::try_from(McpLimitSpec::default())?,
        Arc::new(RejectingCompletionAudit),
        Arc::new(RecordingArtifacts::default()),
    )?;
    let (client, server_output) = tokio::io::duplex(8 * 1024);
    let task = tokio::spawn(server.serve_unverified_io(
        SegmentedReader::new([initialize]),
        server_output,
        CancellationToken::new(),
    ));
    let mut reader = BufReader::new(client);
    let mut line = String::new();
    let read = tokio::time::timeout(Duration::from_secs(1), reader.read_line(&mut line)).await??;
    assert_eq!(read, 0, "completion audit failure leaked: {line}");
    assert_eq!(task.await??, ServerExit::AuditFailed);
    Ok(())
}

#[tokio::test]
async fn dispatched_service_failure_is_a_redacted_tool_error_and_audited_as_failure()
-> Result<(), Box<dyn Error>> {
    let audit = Arc::new(CountingAudit::default());
    let server = McpServer::try_new(
        Arc::new(BoundaryService::default()),
        McpLimits::try_from(McpLimitSpec::default())?,
        audit.clone(),
        Arc::new(RecordingArtifacts::default()),
    )?;
    let (client, server_io) = tokio::io::duplex(64 * 1024);
    let (server_reader, server_writer) = tokio::io::split(server_io);
    let task = tokio::spawn(server.serve_unverified_io(
        server_reader,
        server_writer,
        CancellationToken::new(),
    ));
    let (client_reader, mut writer) = tokio::io::split(client);
    let mut reader = BufReader::new(client_reader);
    send(
        &mut writer,
        json!({
            "jsonrpc":"2.0","id":1,"method":"initialize",
            "params":{
                "protocolVersion":"2025-11-25",
                "capabilities":{},
                "clientInfo":{"name":"tests","version":"1"}
            }
        }),
    )
    .await?;
    let _initialize = receive(&mut reader).await?;
    send(
        &mut writer,
        json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
    )
    .await?;
    send(
        &mut writer,
        json!({
            "jsonrpc":"2.0","id":"runtime-failure","method":"tools/call",
            "params":{"name":"test.failure","arguments":{}}
        }),
    )
    .await?;

    let response = receive(&mut reader).await?;
    assert!(response.get("error").is_none());
    assert_eq!(response["result"]["isError"], true);
    assert_eq!(
        response["result"]["content"][0]["text"],
        "service is unavailable"
    );
    assert!(response["result"].get("structuredContent").is_none());
    let encoded = serde_json::to_string(&response)?;
    assert!(!encoded.contains("provider"));
    assert!(!encoded.contains("/Users/"));

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if audit
                .tool_result_classes()
                .is_ok_and(|classes| classes.contains(&AuditResultClass::ServiceRejected))
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let classes = audit.tool_result_classes()?;
    assert!(classes.contains(&AuditResultClass::ServiceRejected));
    assert!(!classes.contains(&AuditResultClass::Succeeded));

    writer.shutdown().await?;
    assert_eq!(task.await??, ServerExit::EndOfInput);
    Ok(())
}

#[tokio::test]
async fn mutation_audit_is_atomic_and_output_failure_cannot_rewrite_service_success()
-> Result<(), Box<dyn Error>> {
    let audit = Arc::new(AtomicMutationAudit::new(MutationAuditFault::None));
    let service = Arc::new(KillSwitchService::new(Arc::clone(&audit)));
    let (mut reader, mut writer, task) =
        initialized_mutation_session(Arc::clone(&service), Arc::clone(&audit), false).await?;
    send(
        &mut writer,
        json!({
            "jsonrpc":"2.0","id":"kill-unconfirmed","method":"tools/call",
            "params":{"name":"test.kill-switch","arguments":{"confirm":false}}
        }),
    )
    .await?;
    assert_eq!(receive(&mut reader).await?["error"]["code"], -32602);
    assert_eq!(service.mutations.load(Ordering::SeqCst), 0);
    writer.shutdown().await?;
    assert_eq!(task.await??, ServerExit::EndOfInput);

    for fault in [
        MutationAuditFault::Reservation,
        MutationAuditFault::AdmissionPersist,
    ] {
        let audit = Arc::new(AtomicMutationAudit::new(fault));
        let service = Arc::new(KillSwitchService::new(Arc::clone(&audit)));
        let (_reader, mut writer, task) =
            initialized_mutation_session(Arc::clone(&service), Arc::clone(&audit), true).await?;
        send(
            &mut writer,
            json!({
                "jsonrpc":"2.0","id":"kill-rejected","method":"tools/call",
                "params":{"name":"test.kill-switch","arguments":{"confirm":true}}
            }),
        )
        .await?;
        assert_eq!(task.await??, ServerExit::AuditFailed);
        assert_eq!(service.mutations.load(Ordering::SeqCst), 0);
        assert!(audit.events()?.iter().all(|event| {
            event.phase() != AuditPhase::MutationServiceCompleted
                || event.result_class() != Some(AuditResultClass::Succeeded)
        }));
    }

    let audit = Arc::new(AtomicMutationAudit::new(
        MutationAuditFault::ServiceTerminal,
    ));
    let service = Arc::new(KillSwitchService::new(Arc::clone(&audit)));
    let (mut reader, mut writer, task) =
        initialized_mutation_session(Arc::clone(&service), Arc::clone(&audit), false).await?;
    send(
        &mut writer,
        json!({
            "jsonrpc":"2.0","id":"kill-terminal-audit-fails","method":"tools/call",
            "params":{"name":"test.kill-switch","arguments":{"confirm":true}}
        }),
    )
    .await?;
    let mut leaked = String::new();
    let read =
        tokio::time::timeout(Duration::from_secs(1), reader.read_line(&mut leaked)).await??;
    assert_eq!(read, 0, "service-terminal audit failure leaked: {leaked}");
    assert_eq!(task.await??, ServerExit::AuditFailed);
    assert_eq!(service.mutations.load(Ordering::SeqCst), 1);
    let events = audit.events()?;
    assert_eq!(
        events
            .iter()
            .filter(|event| event.phase() == AuditPhase::MutationAdmitted)
            .count(),
        1
    );
    assert!(
        events
            .iter()
            .all(|event| event.phase() != AuditPhase::MutationServiceCompleted),
        "failed service-terminal audit was reported as durable"
    );

    let audit = Arc::new(AtomicMutationAudit::new(
        MutationAuditFault::DeliveryTerminal,
    ));
    let service = Arc::new(KillSwitchService::new(Arc::clone(&audit)));
    let (mut reader, mut writer, task) =
        initialized_mutation_session(Arc::clone(&service), Arc::clone(&audit), false).await?;
    send(
        &mut writer,
        json!({
            "jsonrpc":"2.0","id":"kill-delivery-audit-fails","method":"tools/call",
            "params":{"name":"test.kill-switch","arguments":{"confirm":true}}
        }),
    )
    .await?;
    let response = tokio::time::timeout(Duration::from_secs(1), receive(&mut reader)).await??;
    assert_eq!(
        response["result"]["structuredContent"]["data"]["triggered"],
        true
    );
    assert_eq!(task.await??, ServerExit::AuditFailed);
    assert_eq!(service.mutations.load(Ordering::SeqCst), 1);
    let events = audit.events()?;
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                event.phase() == AuditPhase::MutationServiceCompleted
                    && event.result_class() == Some(AuditResultClass::Succeeded)
            })
            .count(),
        1
    );
    assert!(
        events.iter().all(|event| {
            event.phase() != AuditPhase::Completed
                || !matches!(
                    event.operation(),
                    market_squawk_mcp::AuditOperation::CallTool { .. }
                )
        }),
        "failed delivery-terminal audit was reported as durable"
    );

    let audit = Arc::new(AtomicMutationAudit::new(MutationAuditFault::None));
    let service = Arc::new(KillSwitchService::new(Arc::clone(&audit)));
    let (_reader, mut writer, task) =
        initialized_mutation_session(Arc::clone(&service), Arc::clone(&audit), true).await?;
    send(
        &mut writer,
        json!({
            "jsonrpc":"2.0","id":"kill-delivery-fails","method":"tools/call",
            "params":{"name":"test.kill-switch","arguments":{"confirm":true}}
        }),
    )
    .await?;
    tokio::time::timeout(Duration::from_secs(1), async {
        while service.mutations.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let exit = task.await??;
    assert!(matches!(
        exit,
        ServerExit::PeerClosed | ServerExit::OutputFailed
    ));
    assert_eq!(service.mutations.load(Ordering::SeqCst), 1);

    let events = audit.events()?;
    assert_eq!(
        events
            .iter()
            .filter(|event| event.phase() == AuditPhase::MutationAdmitted)
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                event.phase() == AuditPhase::MutationServiceCompleted
                    && event.result_class() == Some(AuditResultClass::Succeeded)
            })
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                event.phase() == AuditPhase::Completed
                    && event.result_class() == Some(AuditResultClass::OutputUnavailable)
            })
            .count(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn post_dispatch_cancellation_preserves_the_authoritative_mutation_outcome()
-> Result<(), Box<dyn Error>> {
    let audit = Arc::new(AtomicMutationAudit::new(MutationAuditFault::None));
    let work = Arc::new(HeldWork::new());
    let service = Arc::new(KillSwitchService::held(
        Arc::clone(&audit),
        Arc::clone(&work),
    ));
    let (reader, mut writer, task) =
        initialized_mutation_session(Arc::clone(&service), Arc::clone(&audit), false).await?;
    send(
        &mut writer,
        json!({
            "jsonrpc":"2.0","id":"cancelled-after-dispatch","method":"tools/call",
            "params":{"name":"test.kill-switch","arguments":{"confirm":true}}
        }),
    )
    .await?;
    tokio::time::timeout(Duration::from_secs(1), work.wait_for(&work.entered, 1)).await?;
    send(
        &mut writer,
        json!({
            "jsonrpc":"2.0","method":"notifications/cancelled",
            "params":{"requestId":"cancelled-after-dispatch","reason":"test cancellation"}
        }),
    )
    .await?;
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if audit.events().is_ok_and(|events| {
                events.iter().any(|event| {
                    event.phase() == AuditPhase::Completed
                        && event.result_class() == Some(AuditResultClass::Cancelled)
                })
            }) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert!(
        audit
            .events()?
            .iter()
            .all(|event| event.phase() != AuditPhase::MutationServiceCompleted),
        "post-dispatch cancellation terminalized the authoritative service slot"
    );

    work.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if audit.events().is_ok_and(|events| {
                events.iter().any(|event| {
                    event.phase() == AuditPhase::MutationServiceCompleted
                        && event.result_class() == Some(AuditResultClass::Succeeded)
                })
            }) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert_eq!(service.mutations.load(Ordering::SeqCst), 1);
    drop(reader);
    drop(writer);
    let _exit = tokio::time::timeout(Duration::from_secs(1), task).await???;
    Ok(())
}

#[tokio::test]
async fn sdk_tracing_cannot_emit_protocol_payloads_to_the_host_subscriber()
-> Result<(), Box<dyn Error>> {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .without_time()
        .with_ansi(false)
        .with_writer(CaptureWriter(Arc::clone(&captured)))
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;
    tracing::info!("trace-capture-control");

    let server = McpServer::try_new(
        Arc::new(TraceService),
        McpLimits::try_from(McpLimitSpec::default())?,
        Arc::new(CountingAudit::default()),
        Arc::new(RecordingArtifacts::default()),
    )?;
    let (client, server_io) = tokio::io::duplex(64 * 1024);
    let (server_reader, server_writer) = tokio::io::split(server_io);
    let task = tokio::spawn(server.serve_unverified_io(
        server_reader,
        server_writer,
        CancellationToken::new(),
    ));
    let (client_reader, mut client_writer) = tokio::io::split(client);
    let mut client_reader = BufReader::new(client_reader);
    send(
        &mut client_writer,
        json!({
            "jsonrpc":"2.0","id":1,"method":"initialize",
            "params":{
                "protocolVersion":"2025-11-25","capabilities":{},
                "clientInfo":{"name":"tests","version":"1"}
            }
        }),
    )
    .await?;
    let _ = receive(&mut client_reader).await?;
    send(
        &mut client_writer,
        json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
    )
    .await?;
    send(
        &mut client_writer,
        json!({
            "jsonrpc":"2.0","id":"trace","method":"tools/call",
            "params":{"name":"test.trace","arguments":{"secret":TRACE_SENTINEL}}
        }),
    )
    .await?;
    assert_eq!(
        receive(&mut client_reader).await?["result"]["structuredContent"]["data"]["echo"],
        TRACE_SENTINEL
    );
    send(
        &mut client_writer,
        json!({
            "jsonrpc":"2.0","method":"notifications/cancelled",
            "params":{"requestId":"unknown","reason":TRACE_SENTINEL}
        }),
    )
    .await?;
    client_writer.shutdown().await?;
    assert_eq!(task.await??, ServerExit::EndOfInput);

    let logs = String::from_utf8(
        captured
            .lock()
            .map_err(|_| "trace capture is poisoned")?
            .clone(),
    )?;
    assert!(logs.contains("trace-capture-control"));
    assert!(!logs.contains(TRACE_SENTINEL), "SDK trace leaked: {logs}");
    Ok(())
}

#[tokio::test]
async fn deadline_and_large_output_fail_closed_or_return_an_opaque_artifact()
-> Result<(), Box<dyn Error>> {
    let service = Arc::new(BoundaryService::default());
    let artifacts = Arc::new(RecordingArtifacts::default());
    let spec = McpLimitSpec {
        maximum_inline_bytes: 64,
        maximum_result_bytes: 4 * 1024,
        request_timeout: Duration::from_millis(25),
        ..McpLimitSpec::default()
    };
    let (mut reader, mut writer, task) =
        ready_server(service, Arc::clone(&artifacts), McpLimits::try_from(spec)?).await?;

    send(
        &mut writer,
        json!({
            "jsonrpc":"2.0","id":"large","method":"tools/call",
            "params":{
                "name":"test.large",
                "arguments":{"sourceCoverage":{"status":"partial"}}
            }
        }),
    )
    .await?;
    let large = receive(&mut reader).await?;
    let encoded = serde_json::to_string(&large)?;
    assert_eq!(
        large["result"]["structuredContent"]["artifact"]["mediaType"],
        "application/json"
    );
    assert_eq!(
        large["result"]["structuredContent"]["metadata"]["completeness"],
        "truncated"
    );
    assert_eq!(
        large["result"]["structuredContent"]["metadata"]["returnedItems"],
        1
    );
    assert_eq!(
        large["result"]["structuredContent"]["metadata"]["availableItems"],
        2
    );
    assert_eq!(
        large["result"]["structuredContent"]["metadata"]["sourceCoverage"]["status"],
        "partial"
    );
    assert!(!encoded.contains("sensitive-value"));
    assert!(!encoded.contains("path"));
    assert_eq!(artifacts.publication_count()?, 1);
    let artifact = &large["result"]["structuredContent"]["artifact"];
    let byte_count = usize::try_from(
        artifact["byteCount"]
            .as_u64()
            .ok_or("artifact byte count is missing")?,
    )?;
    let reference = ArtifactReference::try_new(
        artifact["id"].as_str().ok_or("artifact id is missing")?,
        artifact["sha256"]
            .as_str()
            .ok_or("artifact digest is missing")?,
        byte_count,
        artifact["mediaType"]
            .as_str()
            .ok_or("artifact media type is missing")?,
    )?;
    let under_bound = NonZeroUsize::new(byte_count.saturating_sub(1))
        .ok_or("artifact unexpectedly had one byte")?;
    assert_eq!(
        ArtifactReadRequest::try_new(reference.clone(), under_bound),
        Err(ArtifactError::ReadLimitExceeded)
    );
    let read = artifacts
        .read(
            ArtifactReadRequest::try_new(
                reference.clone(),
                NonZeroUsize::new(byte_count).ok_or("artifact byte bound is zero")?,
            )?,
            ArtifactReadContext::new(
                CancellationToken::new(),
                Instant::now() + Duration::from_secs(1),
            ),
        )
        .await?;
    assert_eq!(read.reference(), &reference);
    assert_eq!(read.content().len(), byte_count);
    assert!(
        serde_json::from_slice::<Value>(read.content())?
            .to_string()
            .contains("sensitive-value")
    );

    send(
        &mut writer,
        json!({
            "jsonrpc":"2.0","id":"loose","method":"tools/call",
            "params":{"name":"test.loose","arguments":{}}
        }),
    )
    .await?;
    assert_eq!(receive(&mut reader).await?["error"]["code"], -32010);
    assert_eq!(artifacts.publication_count()?, 1);

    send(
        &mut writer,
        json!({
            "jsonrpc":"2.0","id":"invalid-output","method":"tools/call",
            "params":{"name":"test.invalid-output","arguments":{}}
        }),
    )
    .await?;
    assert_eq!(receive(&mut reader).await?["error"]["code"], -32010);
    assert_eq!(artifacts.publication_count()?, 1);

    send(
        &mut writer,
        json!({
            "jsonrpc":"2.0","id":"invalid-evidence","method":"tools/call",
            "params":{
                "name":"test.invalid-evidence",
                "arguments":{"sourceCoverage":{"status":"partial"}}
            }
        }),
    )
    .await?;
    assert_eq!(receive(&mut reader).await?["error"]["code"], -32010);
    assert_eq!(artifacts.publication_count()?, 1);

    send(
        &mut writer,
        json!({
            "jsonrpc":"2.0","id":"deadline","method":"tools/call",
            "params":{"name":"test.block","arguments":{}}
        }),
    )
    .await?;
    assert_eq!(receive(&mut reader).await?["error"]["code"], -32008);

    writer.shutdown().await?;
    assert_eq!(task.await??, ServerExit::EndOfInput);
    Ok(())
}

#[tokio::test]
async fn timed_out_host_work_stays_within_session_ownership_and_drains_on_shutdown()
-> Result<(), Box<dyn Error>> {
    let service_work = Arc::new(HeldWork::new());
    let artifact_work = Arc::new(HeldWork::new());
    let service = Arc::new(NonCooperativeService {
        work: Arc::clone(&service_work),
        drop_observer: Mutex::new(None),
    });
    let artifacts = Arc::new(NonCooperativeArtifacts {
        work: Arc::clone(&artifact_work),
    });
    let limits = McpLimits::try_from(McpLimitSpec {
        maximum_active_requests: 2,
        maximum_inline_bytes: 64,
        maximum_result_bytes: 4 * 1024,
        request_timeout: Duration::from_millis(100),
        shutdown_timeout: Duration::from_millis(500),
        ..McpLimitSpec::default()
    })?;
    let (mut reader, mut writer, mut task) = ready_server(service, artifacts, limits).await?;

    for index in 0..3 {
        send(
            &mut writer,
            json!({
                "jsonrpc":"2.0","id":format!("service-{index}"),"method":"tools/call",
                "params":{"name":"test.owned-work","arguments":{"artifact":false}}
            }),
        )
        .await?;
        if index < 2 {
            tokio::time::timeout(
                Duration::from_secs(1),
                service_work.wait_for(&service_work.entered, index + 1),
            )
            .await?;
        }
        assert_eq!(receive(&mut reader).await?["error"]["code"], -32008);
    }
    assert_eq!(service_work.entered.load(Ordering::SeqCst), 2);
    service_work.release.add_permits(2);
    tokio::time::timeout(
        Duration::from_secs(1),
        service_work.wait_for(&service_work.completed, 2),
    )
    .await?;

    for index in 0..3 {
        send(
            &mut writer,
            json!({
                "jsonrpc":"2.0","id":format!("artifact-{index}"),"method":"tools/call",
                "params":{"name":"test.owned-work","arguments":{"artifact":true}}
            }),
        )
        .await?;
        if index < 2 {
            tokio::time::timeout(
                Duration::from_secs(1),
                artifact_work.wait_for(&artifact_work.entered, index + 1),
            )
            .await?;
        }
        assert_eq!(receive(&mut reader).await?["error"]["code"], -32008);
    }
    assert_eq!(artifact_work.entered.load(Ordering::SeqCst), 2);

    writer.shutdown().await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut task)
            .await
            .is_err(),
        "session abandoned lifecycle-owned artifact work"
    );
    artifact_work.release.add_permits(2);
    tokio::time::timeout(
        Duration::from_secs(1),
        artifact_work.wait_for(&artifact_work.completed, 2),
    )
    .await?;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), task).await???,
        ServerExit::EndOfInput
    );
    Ok(())
}

#[tokio::test]
async fn supervisor_drop_cancels_before_dropping_held_service_work() -> Result<(), Box<dyn Error>> {
    let service_work = Arc::new(HeldWork::new());
    let (drop_observer, observed) = oneshot::channel();
    let service = Arc::new(NonCooperativeService {
        work: Arc::clone(&service_work),
        drop_observer: Mutex::new(Some(drop_observer)),
    });
    let (_reader, mut writer, task) = ready_server(
        service,
        Arc::new(RecordingArtifacts::default()),
        McpLimits::try_from(McpLimitSpec::default())?,
    )
    .await?;

    send(
        &mut writer,
        json!({
            "jsonrpc":"2.0","id":"drop-order","method":"tools/call",
            "params":{"name":"test.owned-work","arguments":{"artifact":false}}
        }),
    )
    .await?;
    tokio::time::timeout(
        Duration::from_secs(1),
        service_work.wait_for(&service_work.entered, 1),
    )
    .await?;

    task.abort();
    assert!(task.await.is_err());
    assert!(tokio::time::timeout(Duration::from_secs(1), observed).await??);
    Ok(())
}

#[derive(Debug, Default)]
struct StalledWriter;

impl AsyncWrite for StalledWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        _buffer: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        Poll::Pending
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn stalled_output_hits_the_write_deadline_instead_of_growing_unbounded()
-> Result<(), Box<dyn Error>> {
    assert!(matches!(
        McpLimits::try_from(McpLimitSpec {
            maximum_writer_queue_bytes: McpLimitSpec::default().maximum_frame_bytes,
            ..McpLimitSpec::default()
        }),
        Err(McpLimitError::WriterBudgetBelowFrame)
    ));
    assert!(matches!(
        McpLimits::try_from(McpLimitSpec {
            write_timeout: Duration::MAX,
            ..McpLimitSpec::default()
        }),
        Err(McpLimitError::DurationTooLarge)
    ));
    assert!(matches!(
        McpLimits::try_from(McpLimitSpec {
            maximum_depth: usize::MAX,
            ..McpLimitSpec::default()
        }),
        Err(McpLimitError::LimitTooLarge)
    ));

    let input = serde_json::to_vec(&json!({
        "jsonrpc":"2.0","id":1,"method":"initialize",
        "params":{
            "protocolVersion":"2025-11-25","capabilities":{},
            "clientInfo":{"name":"tests","version":"1"}
        }
    }))?;
    let mut framed = input;
    framed.push(b'\n');
    let spec = McpLimitSpec {
        writer_queue_capacity: 1,
        write_timeout: Duration::from_millis(25),
        ..McpLimitSpec::default()
    };
    let audit = Arc::new(CountingAudit::default());
    let server = McpServer::try_new(
        Arc::new(BoundaryService::default()),
        McpLimits::try_from(spec)?,
        audit.clone(),
        Arc::new(RecordingArtifacts::default()),
    )?;
    let (mut input_writer, input_reader) = tokio::io::duplex(4096);
    input_writer.write_all(&framed).await?;
    input_writer.shutdown().await?;

    let exit = server
        .serve_unverified_io(input_reader, StalledWriter, CancellationToken::new())
        .await?;
    assert_eq!(exit, ServerExit::WriteTimedOut);
    assert_eq!(
        audit.result_classes()?,
        vec![AuditResultClass::OutputUnavailable]
    );
    Ok(())
}
