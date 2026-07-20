use std::{
    collections::VecDeque,
    error::Error,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use async_trait::async_trait;
use market_squawk_mcp::{
    ArtifactError, ArtifactPublication, ArtifactReference, ArtifactRepository, AuditCompletion,
    AuditCompletionReservation, AuditError, AuditEvent, AuditResultClass, AuditSink, McpLimitError,
    McpLimitSpec, McpLimits, McpServer, ServerExit,
};
use market_squawk_services::{
    JsonStructureLimits, RequestContext, ServiceCapabilities, ServiceError, ServiceLimits,
    ToolDescriptor, ToolEffects, ToolInputError, ToolServices, TypedToolRequest, TypedToolResult,
};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, ReadBuf};
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
}

#[derive(Debug, Default)]
struct RejectingCompletionAudit;

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
        }))
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
}

#[derive(Debug, Default)]
struct BoundaryService {
    calls: AtomicUsize,
}

impl BoundaryService {
    fn capabilities() -> ServiceCapabilities {
        let descriptors = [
            (
                "test.large",
                "Return a result larger than the inline ceiling.",
            ),
            (
                "test.loose",
                "Return a result constructed under a looser service contract.",
            ),
            ("test.block", "Wait until the transport deadline expires."),
        ]
        .into_iter()
        .filter_map(|(name, description)| {
            ToolDescriptor::try_new(
                name,
                "1",
                description,
                json!({"type":"object","properties":{},"additionalProperties":false}),
                ToolEffects::read_only_closed_world(),
                |arguments: &serde_json::Map<String, Value>| {
                    arguments
                        .is_empty()
                        .then_some(())
                        .ok_or(ToolInputError::Invalid)
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
                context.limits(),
            )
            .map_err(Into::into),
            "test.loose" => {
                let structure = JsonStructureLimits::try_new(8, 16 * 1024, 16, 16)
                    .map_err(|_| ServiceError::Internal)?;
                let loose = ServiceLimits::try_new(64, 1, 16 * 1024, 16, structure)
                    .map_err(|_| ServiceError::Internal)?;
                TypedToolResult::try_new(json!({"items": ["x".repeat(8 * 1024)]}), 1, loose)
                    .map_err(Into::into)
            }
            "test.block" => {
                context.cancellation().cancelled().await;
                Err(ServiceError::Cancelled)
            }
            _ => Err(ServiceError::NotFound),
        }
    }
}

#[derive(Debug, Default)]
struct TraceService;

#[async_trait]
impl ToolServices for TraceService {
    fn capabilities(&self) -> ServiceCapabilities {
        let descriptor = ToolDescriptor::try_new(
            "test.trace",
            "1",
            "Echo a test sentinel through the bounded result path.",
            json!({
                "type":"object",
                "properties":{"secret":{"type":"string"}},
                "required":["secret"],
                "additionalProperties":false
            }),
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
        TypedToolResult::try_new(json!({"echo": secret}), 1, context.limits()).map_err(Into::into)
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

async fn ready_server(
    service: Arc<BoundaryService>,
    artifacts: Arc<RecordingArtifacts>,
    limits: McpLimits,
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
        Arc::new(BoundaryService::default()),
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
        receive(&mut client_reader).await?["result"]["structuredContent"]["echo"],
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
            "params":{"name":"test.large","arguments":{}}
        }),
    )
    .await?;
    let large = receive(&mut reader).await?;
    let encoded = serde_json::to_string(&large)?;
    assert_eq!(
        large["result"]["structuredContent"]["artifact"]["mediaType"],
        "application/json"
    );
    assert!(!encoded.contains("sensitive-value"));
    assert!(!encoded.contains("path"));
    assert_eq!(artifacts.publication_count()?, 1);

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
