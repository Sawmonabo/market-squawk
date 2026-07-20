use std::{
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
    ArtifactError, ArtifactPublication, ArtifactReference, ArtifactRepository, AuditError,
    AuditEvent, AuditResultClass, AuditSink, McpLimitError, McpLimitSpec, McpLimits, McpServer,
    ServerExit,
};
use market_squawk_services::{
    RequestContext, ServiceCapabilities, ServiceError, ToolDescriptor, ToolEffects, ToolInputError,
    ToolServices, TypedToolRequest, TypedToolResult,
};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Default)]
struct CountingAudit(Mutex<Vec<AuditEvent>>);

impl CountingAudit {
    fn result_classes(&self) -> Result<Vec<AuditResultClass>, AuditError> {
        self.0
            .lock()
            .map(|events| events.iter().filter_map(AuditEvent::result_class).collect())
            .map_err(|_| AuditError::Unavailable)
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
}

#[derive(Debug, Default)]
struct RecordingArtifacts {
    publications: Mutex<Vec<ArtifactPublication>>,
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
            "test.block" => {
                context.cancellation().cancelled().await;
                Err(ServiceError::Cancelled)
            }
            _ => Err(ServiceError::NotFound),
        }
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
async fn deadline_and_large_output_fail_closed_or_return_an_opaque_artifact()
-> Result<(), Box<dyn Error>> {
    let service = Arc::new(BoundaryService::default());
    let artifacts = Arc::new(RecordingArtifacts::default());
    let spec = McpLimitSpec {
        maximum_inline_bytes: 64,
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
