use std::{
    collections::HashMap,
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use market_squawk_mcp::{
    ArtifactError, ArtifactPublication, ArtifactReference, ArtifactRepository, AuditError,
    AuditEvent, AuditPhase, AuditSink, LocalProcessIdentityClass, McpLimitSpec, McpLimits,
    McpServer, ServerError, ServerExit,
};
use market_squawk_services::{
    RequestContext, ServiceCapabilities, ServiceError, ToolDescriptor, ToolEffects, ToolInputError,
    ToolServices, TypedToolRequest, TypedToolResult,
};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream, ReadHalf, WriteHalf};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Default)]
struct CollectingAudit {
    events: Mutex<Vec<AuditEvent>>,
}

impl CollectingAudit {
    fn phases(&self) -> Result<Vec<AuditPhase>, AuditError> {
        self.events
            .lock()
            .map(|events| events.iter().map(AuditEvent::phase).collect())
            .map_err(|_| AuditError::Unavailable)
    }

    fn events(&self) -> Result<Vec<AuditEvent>, AuditError> {
        self.events
            .lock()
            .map(|events| events.clone())
            .map_err(|_| AuditError::Unavailable)
    }
}

impl AuditSink for CollectingAudit {
    fn record(&self, event: AuditEvent) -> Result<(), AuditError> {
        self.events
            .lock()
            .map_err(|_| AuditError::Unavailable)?
            .push(event);
        Ok(())
    }
}

#[derive(Debug, Default)]
struct RejectingArtifacts;

#[async_trait]
impl ArtifactRepository for RejectingArtifacts {
    async fn publish(
        &self,
        _publication: ArtifactPublication,
    ) -> Result<ArtifactReference, ArtifactError> {
        Err(ArtifactError::Unavailable)
    }
}

#[derive(Debug, Default)]
struct EmptyServices;

#[async_trait]
impl ToolServices for EmptyServices {
    fn capabilities(&self) -> ServiceCapabilities {
        ServiceCapabilities::empty()
    }

    async fn call(
        &self,
        _request: TypedToolRequest,
        _context: RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        Err(ServiceError::NotFound)
    }
}

struct Harness {
    reader: BufReader<ReadHalf<DuplexStream>>,
    writer: WriteHalf<DuplexStream>,
    task: tokio::task::JoinHandle<Result<ServerExit, market_squawk_mcp::ServerError>>,
}

impl Harness {
    async fn start<S: ToolServices>(
        services: Arc<S>,
        audit: Arc<CollectingAudit>,
        limits: McpLimits,
    ) -> Result<Self, ServerError> {
        let server = McpServer::try_new(services, limits, audit, Arc::new(RejectingArtifacts))?;
        let (client, server_io) = tokio::io::duplex(64 * 1024);
        let (reader, writer) = tokio::io::split(server_io);
        let task =
            tokio::spawn(server.serve_unverified_io(reader, writer, CancellationToken::new()));
        let (reader, writer) = tokio::io::split(client);
        Ok(Self {
            reader: BufReader::new(reader),
            writer,
            task,
        })
    }

    async fn send(&mut self, message: Value) -> Result<(), Box<dyn Error>> {
        self.writer
            .write_all(&serde_json::to_vec(&message)?)
            .await?;
        self.writer.write_all(b"\n").await?;
        self.writer.flush().await?;
        Ok(())
    }

    async fn receive(&mut self) -> Result<Value, Box<dyn Error>> {
        let mut line = String::new();
        self.reader.read_line(&mut line).await?;
        Ok(serde_json::from_str(&line)?)
    }

    async fn initialize(&mut self, id: Value) -> Result<Value, Box<dyn Error>> {
        self.send(json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "initialize",
            "params": {
                "protocolVersion": "2099-01-01",
                "capabilities": {},
                "clientInfo": { "name": "market-squawk-tests", "version": "1" }
            }
        }))
        .await?;
        self.receive().await
    }

    async fn initialized(&mut self) -> Result<(), Box<dyn Error>> {
        self.send(json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized"
        }))
        .await
    }

    async fn close(mut self) -> Result<ServerExit, Box<dyn Error>> {
        self.writer.shutdown().await?;
        Ok(self.task.await??)
    }
}

#[tokio::test]
async fn lifecycle_negotiates_protocol_and_advertises_no_unregistered_domains()
-> Result<(), Box<dyn Error>> {
    let audit = Arc::new(CollectingAudit::default());
    let mut harness = Harness::start(
        Arc::new(EmptyServices),
        Arc::clone(&audit),
        McpLimits::try_from(McpLimitSpec::default())?,
    )
    .await?;

    let initialized = harness.initialize(json!(17)).await?;
    assert_eq!(initialized["id"], 17);
    assert_eq!(initialized["result"]["protocolVersion"], "2025-11-25");
    assert!(initialized["result"]["capabilities"].get("tools").is_none());

    harness
        .send(json!({"jsonrpc":"2.0","id":"early","method":"tools/list"}))
        .await?;
    assert_eq!(harness.receive().await?["error"]["code"], -32002);

    harness.initialized().await?;
    harness
        .send(json!({
            "jsonrpc":"2.0",
            "id":"x".repeat(1_025),
            "method":"ping"
        }))
        .await?;
    let invalid_id = harness.receive().await?;
    assert_eq!(invalid_id["error"]["code"], -32600);
    assert!(invalid_id["id"].is_null());

    harness
        .send(json!({
            "jsonrpc":"2.0",
            "id":"invalid-shape",
            "method":17
        }))
        .await?;
    assert_eq!(harness.receive().await?["error"]["code"], -32600);

    harness
        .send(json!({"jsonrpc":"2.0","id":"ping-1","method":"ping"}))
        .await?;
    let ping = harness.receive().await?;
    assert_eq!(ping["id"], "ping-1");
    assert_eq!(ping["result"], json!({}));

    harness
        .send(json!({"jsonrpc":"2.0","id":18,"method":"tools/list"}))
        .await?;
    assert_eq!(harness.receive().await?["error"]["code"], -32601);

    assert_eq!(harness.close().await?, ServerExit::EndOfInput);
    let phases = audit.phases()?;
    assert!(audit.events()?.iter().all(|event| {
        event.identity_class() == LocalProcessIdentityClass::CallerSuppliedIoUnverified
    }));
    assert_eq!(
        phases,
        vec![
            AuditPhase::Admitted,
            AuditPhase::Completed,
            AuditPhase::Admitted,
            AuditPhase::Completed,
            AuditPhase::Admitted,
            AuditPhase::Completed,
            AuditPhase::Admitted,
            AuditPhase::Completed,
            AuditPhase::Admitted,
            AuditPhase::Completed,
        ]
    );
    Ok(())
}

#[derive(Debug, Default)]
struct WaitingService {
    started: Notify,
    calls: AtomicUsize,
}

#[async_trait]
impl ToolServices for WaitingService {
    fn capabilities(&self) -> ServiceCapabilities {
        let descriptor = ToolDescriptor::try_new(
            "test.wait",
            "1",
            "Wait until the transport cancels this test-only operation.",
            json!({
                "type": "object",
                "description": "x".repeat(8 * 1_024),
                "examples": ["y".repeat(8 * 1_024)],
                "properties": {},
                "additionalProperties": false
            }),
            ToolEffects::read_only_closed_world(),
            |arguments: &serde_json::Map<String, Value>| {
                arguments
                    .is_empty()
                    .then_some(())
                    .ok_or(ToolInputError::Invalid)
            },
        );
        ServiceCapabilities::try_new(descriptor.into_iter().collect())
            .unwrap_or_else(|_| ServiceCapabilities::empty())
    }

    async fn call(
        &self,
        _request: TypedToolRequest,
        context: RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        context.cancellation().cancelled().await;
        Err(ServiceError::Cancelled)
    }
}

#[tokio::test]
async fn duplicate_active_ids_are_rejected_and_cancellation_reaches_the_service()
-> Result<(), Box<dyn Error>> {
    let service = Arc::new(WaitingService::default());
    let constrained = McpLimits::try_from(McpLimitSpec {
        maximum_frame_bytes: 20 * 1024,
        maximum_body_bytes: 20 * 1024,
        maximum_inline_bytes: 1024,
        maximum_inline_items: 1,
        maximum_writer_queue_bytes: 20 * 1024 + 1,
        ..McpLimitSpec::default()
    })?;
    assert!(matches!(
        McpServer::try_new(
            Arc::clone(&service),
            constrained,
            Arc::new(CollectingAudit::default()),
            Arc::new(RejectingArtifacts),
        ),
        Err(ServerError::InvalidComposition)
    ));
    let audit = Arc::new(CollectingAudit::default());
    let mut harness = Harness::start(
        Arc::clone(&service),
        Arc::clone(&audit),
        McpLimits::try_from(McpLimitSpec::default())?,
    )
    .await?;
    let initialized = harness.initialize(json!("init")).await?;
    assert!(initialized["result"]["capabilities"].get("tools").is_some());
    harness.initialized().await?;

    harness
        .send(json!({
            "jsonrpc":"2.0",
            "id":"cursor",
            "method":"tools/list",
            "params":{"cursor":"unsupported"}
        }))
        .await?;
    assert_eq!(harness.receive().await?["error"]["code"], -32602);

    harness
        .send(json!({"jsonrpc":"2.0","id":"list","method":"tools/list"}))
        .await?;
    let listed = harness.receive().await?;
    assert_eq!(listed["result"]["tools"][0]["name"], "test.wait");
    assert_eq!(
        listed["result"]["tools"][0]["annotations"]["readOnlyHint"],
        true
    );
    assert_eq!(
        listed["result"]["tools"][0]["annotations"]["destructiveHint"],
        false
    );

    harness
        .send(json!({
            "jsonrpc":"2.0",
            "id":"invalid-arguments",
            "method":"tools/call",
            "params":{"name":"test.wait","arguments":{"unknown":true}}
        }))
        .await?;
    assert_eq!(harness.receive().await?["error"]["code"], -32602);
    assert_eq!(service.calls.load(Ordering::SeqCst), 0);

    let call = json!({
        "jsonrpc": "2.0",
        "id": "active-id",
        "method": "tools/call",
        "params": { "name": "test.wait", "arguments": {} }
    });
    harness.send(call.clone()).await?;
    service.started.notified().await;
    harness.send(call.clone()).await?;
    harness
        .send(json!({
            "jsonrpc": "2.0",
            "method": "notifications/cancelled",
            "params": { "requestId": "active-id", "reason": "test shutdown" }
        }))
        .await?;
    harness.send(call).await?;

    harness
        .send(json!({"jsonrpc":"2.0","id":"after-cancel","method":"ping"}))
        .await?;

    let duplicate = harness.receive().await?;
    assert_eq!(duplicate["error"]["code"], -32009);
    let duplicate_after_cancel = harness.receive().await?;
    assert_eq!(duplicate_after_cancel["error"]["code"], -32009);
    let ping = harness.receive().await?;
    assert_eq!(ping["id"], "after-cancel");
    assert_eq!(ping["result"], json!({}));
    assert_eq!(service.calls.load(Ordering::SeqCst), 1);
    assert_eq!(harness.close().await?, ServerExit::EndOfInput);

    let events = audit.events()?;
    assert!(events.iter().any(|event| {
        event.result_class() == Some(market_squawk_mcp::AuditResultClass::Cancelled)
    }));
    let mut by_request = HashMap::<String, (usize, usize)>::new();
    for event in events {
        let counts = by_request
            .entry(event.request_id_sha256().to_owned())
            .or_default();
        match event.phase() {
            AuditPhase::Admitted => counts.0 += 1,
            AuditPhase::Completed => counts.1 += 1,
        }
    }
    assert!(
        by_request
            .values()
            .all(|counts| counts.0 > 0 && counts.0 == counts.1)
    );
    Ok(())
}
