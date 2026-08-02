use std::{
    collections::HashMap,
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
use axum::{
    body::{Body, to_bytes},
    http::{Method, Request, StatusCode, header},
};
use market_squawk_jobs::JobId;
use market_squawk_mcp::{
    ArtifactError, ArtifactPublication, ArtifactPublicationContext, ArtifactRead,
    ArtifactReadContext, ArtifactReadRequest, ArtifactReference, ArtifactRepository,
    AuditCompletion, AuditCompletionReservation, AuditError, AuditEvent, AuditPhase, AuditSink,
    AuthenticatedMcpClient, HttpMcpConfig, LocalProcessIdentityClass, McpHandlerFactory,
    McpHttpAuthError, McpHttpAuthenticator, McpHttpService, McpLimitSpec, McpLimits, McpRelayError,
    McpRelayExchange, McpRelayResponse, McpRelayTransport, McpRelayTransportError,
    McpResourceDocument, McpResourceError, McpResourceProvider, McpResourceRequest, McpServer,
    McpStdioRelay, MutationAuditBundle, MutationAuditReservation, ServerError, ServerExit,
};
use market_squawk_runtime::{ClientId, CredentialGeneration, NamedClient};
use market_squawk_services::{
    ProgressError, RequestContext, ScopeRequirement, ServiceCapabilities, ServiceCapabilityError,
    ServiceDomain, ServiceError, SourceEvidencePolicy, TOOL_INSTRUMENT_IDS_FIELD,
    TOOL_RESULT_LIMITS_FIELD, TOOL_SOURCE_COVERAGE_FIELD, TOOL_TIME_RANGE_FIELD,
    ToolArtifactPolicy, ToolAuthorization, ToolContract, ToolDescriptor, ToolEffects,
    ToolInputError, ToolResultMetadata, ToolResultPolicy, ToolScope, ToolServices,
    TypedToolRequest, TypedToolResult,
};
use rmcp::model::{
    Notification, NumberOrString, ProgressNotificationParam, ProgressToken, ServerJsonRpcMessage,
    ServerNotification,
};
use serde_json::{Value, json};
use tokio::io::{
    AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader, DuplexStream, ReadHalf, WriteHalf,
};
use tokio::sync::{Notify, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Default)]
struct CollectingAudit {
    events: Arc<Mutex<Vec<AuditEvent>>>,
    changed: Arc<Notify>,
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

    async fn wait_for_result_count(
        &self,
        result_class: market_squawk_mcp::AuditResultClass,
        count: usize,
    ) -> Result<(), AuditError> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self
                .events()?
                .iter()
                .filter(|event| event.result_class() == Some(result_class))
                .count()
                >= count
            {
                return Ok(());
            }
            changed.as_mut().await;
        }
    }
}

impl AuditSink for CollectingAudit {
    fn record(&self, event: AuditEvent) -> Result<(), AuditError> {
        self.events
            .lock()
            .map_err(|_| AuditError::Unavailable)?
            .push(event);
        self.changed.notify_waiters();
        Ok(())
    }

    fn reserve_completion(
        &self,
        completion: AuditCompletion,
    ) -> Result<AuditCompletionReservation, AuditError> {
        let events = Arc::clone(&self.events);
        let changed = Arc::clone(&self.changed);
        Ok(AuditCompletionReservation::new(completion, move |event| {
            events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(event);
            changed.notify_waiters();
            Ok(())
        }))
    }

    fn reserve_mutation(
        &self,
        bundle: MutationAuditBundle,
    ) -> Result<MutationAuditReservation, AuditError> {
        let admitted = Arc::clone(&self.events);
        let admitted_changed = Arc::clone(&self.changed);
        let service = Arc::clone(&self.events);
        let service_changed = Arc::clone(&self.changed);
        let delivery = Arc::clone(&self.events);
        let delivery_changed = Arc::clone(&self.changed);
        MutationAuditReservation::try_new(
            bundle,
            move |event| {
                admitted
                    .lock()
                    .map_err(|_| AuditError::Unavailable)?
                    .push(event);
                admitted_changed.notify_waiters();
                Ok(())
            },
            move |event| {
                service
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(event);
                service_changed.notify_waiters();
                Ok(())
            },
            move |event| {
                delivery
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(event);
                delivery_changed.notify_waiters();
                Ok(())
            },
        )
    }
}

#[derive(Debug, Default)]
struct RejectingArtifacts;

#[async_trait]
impl ArtifactRepository for RejectingArtifacts {
    async fn publish(
        &self,
        _publication: ArtifactPublication,
        _context: ArtifactPublicationContext,
    ) -> Result<ArtifactReference, ArtifactError> {
        Err(ArtifactError::Unavailable)
    }

    async fn read(
        &self,
        _request: ArtifactReadRequest,
        _context: ArtifactReadContext,
    ) -> Result<ArtifactRead, ArtifactError> {
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

#[derive(Debug)]
struct DropTrackedWriter(Arc<std::sync::atomic::AtomicBool>);

impl Drop for DropTrackedWriter {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

impl AsyncWrite for DropTrackedWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(Ok(buffer.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
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
        audit.clone(),
        McpLimits::try_from(McpLimitSpec::default())?,
    )
    .await?;

    let initialized = harness.initialize(json!(17)).await?;
    assert_eq!(initialized["id"], 17);
    assert_eq!(initialized["result"]["protocolVersion"], "2026-07-28");
    assert!(initialized["result"]["capabilities"].get("tools").is_none());

    harness
        .send(json!({"jsonrpc":"2.0","id":"early","method":"tools/list"}))
        .await?;
    assert_eq!(harness.receive().await?["error"]["code"], -32602);

    harness.initialized().await?;
    harness
        .send(json!({"jsonrpc":"2.0","id":"","method":"ping"}))
        .await?;
    let empty_id = harness.receive().await?;
    assert_eq!(empty_id["id"], "");
    assert_eq!(empty_id["result"], Value::Null);

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
    assert_eq!(ping["result"], Value::Null);

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
            AuditPhase::Admitted,
            AuditPhase::Completed,
        ]
    );
    Ok(())
}

#[tokio::test]
async fn preinitialize_exits_own_the_writer_and_terminalize_admitted_audits()
-> Result<(), Box<dyn Error>> {
    let audit = Arc::new(CollectingAudit::default());
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let server = McpServer::try_new(
        Arc::new(EmptyServices),
        McpLimits::try_from(McpLimitSpec::default())?,
        audit.clone(),
        Arc::new(RejectingArtifacts),
    )?;
    let (mut input, reader) = tokio::io::duplex(4 * 1024);
    input
        .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":\"wrong\",\"method\":\"tools/list\"}\n")
        .await?;
    input.shutdown().await?;
    let wrong = tokio::time::timeout(
        Duration::from_secs(1),
        server.serve_unverified_io(
            reader,
            DropTrackedWriter(Arc::clone(&dropped)),
            CancellationToken::new(),
        ),
    )
    .await?;
    assert!(
        matches!(wrong, Err(ServerError::Initialize)),
        "unexpected pre-initialize result: {wrong:?}"
    );
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(
        audit.phases()?,
        vec![AuditPhase::Admitted, AuditPhase::Completed]
    );

    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let server = McpServer::try_new(
        Arc::new(EmptyServices),
        McpLimits::try_from(McpLimitSpec::default())?,
        Arc::new(CollectingAudit::default()),
        Arc::new(RejectingArtifacts),
    )?;
    let (input_guard, reader) = tokio::io::duplex(64);
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let cancelled = tokio::time::timeout(
        Duration::from_secs(1),
        server.serve_unverified_io(
            reader,
            DropTrackedWriter(Arc::clone(&dropped)),
            cancellation,
        ),
    )
    .await??;
    drop(input_guard);
    assert_eq!(cancelled, ServerExit::Cancelled);
    assert!(dropped.load(Ordering::SeqCst));
    Ok(())
}

#[derive(Debug, Default)]
struct WaitingService {
    started: Notify,
    calls: AtomicUsize,
}

#[derive(Clone, Copy)]
struct RegistryEntry {
    name: &'static str,
    domain: ServiceDomain,
    scope: ToolScope,
}

const NO_SCOPE: ToolScope = ToolScope::new(
    ScopeRequirement::NotApplicable,
    ScopeRequirement::NotApplicable,
    ScopeRequirement::NotApplicable,
    ScopeRequirement::NotApplicable,
);

const COMPLETE_REGISTRY: [RegistryEntry; 11] = [
    RegistryEntry {
        name: "Analysis.Test",
        domain: ServiceDomain::Analysis,
        scope: ToolScope::new(
            ScopeRequirement::Required,
            ScopeRequirement::Required,
            ScopeRequirement::Required,
            ScopeRequirement::Required,
        ),
    },
    RegistryEntry {
        name: "Bot.Test",
        domain: ServiceDomain::Bot,
        scope: NO_SCOPE,
    },
    RegistryEntry {
        name: "Execution.Test",
        domain: ServiceDomain::Execution,
        scope: ToolScope::new(
            ScopeRequirement::Required,
            ScopeRequirement::NotApplicable,
            ScopeRequirement::Required,
            ScopeRequirement::Required,
        ),
    },
    RegistryEntry {
        name: "FairValue.Test",
        domain: ServiceDomain::FairValue,
        scope: ToolScope::new(
            ScopeRequirement::Required,
            ScopeRequirement::Required,
            ScopeRequirement::Optional,
            ScopeRequirement::Required,
        ),
    },
    RegistryEntry {
        name: "Fundamental.Test",
        domain: ServiceDomain::Fundamental,
        scope: ToolScope::new(
            ScopeRequirement::Optional,
            ScopeRequirement::Required,
            ScopeRequirement::Optional,
            ScopeRequirement::Required,
        ),
    },
    RegistryEntry {
        name: "Macro.Test",
        domain: ServiceDomain::Macro,
        scope: ToolScope::new(
            ScopeRequirement::NotApplicable,
            ScopeRequirement::Required,
            ScopeRequirement::Optional,
            ScopeRequirement::Required,
        ),
    },
    RegistryEntry {
        name: "Market.Wait",
        domain: ServiceDomain::Market,
        scope: NO_SCOPE,
    },
    RegistryEntry {
        name: "Model.Test",
        domain: ServiceDomain::Model,
        scope: ToolScope::new(
            ScopeRequirement::Optional,
            ScopeRequirement::Optional,
            ScopeRequirement::Optional,
            ScopeRequirement::Required,
        ),
    },
    RegistryEntry {
        name: "Portfolio.Test",
        domain: ServiceDomain::Portfolio,
        scope: ToolScope::new(
            ScopeRequirement::Optional,
            ScopeRequirement::Required,
            ScopeRequirement::Optional,
            ScopeRequirement::Required,
        ),
    },
    RegistryEntry {
        name: "Research.Test",
        domain: ServiceDomain::Research,
        scope: ToolScope::new(
            ScopeRequirement::Optional,
            ScopeRequirement::Required,
            ScopeRequirement::Required,
            ScopeRequirement::Required,
        ),
    },
    RegistryEntry {
        name: "Source.Test",
        domain: ServiceDomain::Source,
        scope: ToolScope::new(
            ScopeRequirement::NotApplicable,
            ScopeRequirement::NotApplicable,
            ScopeRequirement::Optional,
            ScopeRequirement::Required,
        ),
    },
];

fn read_only_contract(domain: ServiceDomain, scope: ToolScope) -> ToolContract {
    let source_evidence = match scope.source_coverage() {
        ScopeRequirement::NotApplicable => SourceEvidencePolicy::NotApplicable,
        ScopeRequirement::Required | ScopeRequirement::Optional => SourceEvidencePolicy::Required,
    };
    ToolContract::new(
        domain,
        ToolAuthorization::ReadOnly,
        scope,
        ToolResultPolicy::new(source_evidence, ToolArtifactPolicy::OpaqueOnOverflow),
    )
}

fn registry_schema(scope: ToolScope) -> Value {
    let mut properties = serde_json::Map::new();
    let mut required = Vec::new();
    for (name, requirement) in [
        (TOOL_INSTRUMENT_IDS_FIELD, scope.instruments()),
        (TOOL_TIME_RANGE_FIELD, scope.time_range()),
        (TOOL_RESULT_LIMITS_FIELD, scope.result_limits()),
        (TOOL_SOURCE_COVERAGE_FIELD, scope.source_coverage()),
    ] {
        if !matches!(requirement, ScopeRequirement::NotApplicable) {
            properties.insert(name.to_owned(), json!({"type":"object"}));
        }
        if matches!(requirement, ScopeRequirement::Required) {
            required.push(Value::String(name.to_owned()));
        }
    }
    let mut schema = serde_json::Map::new();
    schema.insert("type".to_owned(), Value::String("object".to_owned()));
    schema.insert("properties".to_owned(), Value::Object(properties));
    if !required.is_empty() {
        schema.insert("required".to_owned(), Value::Array(required));
    }
    schema.insert("additionalProperties".to_owned(), Value::Bool(false));
    Value::Object(schema)
}

fn admit_registry_scope(
    arguments: &serde_json::Map<String, Value>,
    scope: ToolScope,
) -> Result<(), ToolInputError> {
    for (name, requirement) in [
        (TOOL_INSTRUMENT_IDS_FIELD, scope.instruments()),
        (TOOL_TIME_RANGE_FIELD, scope.time_range()),
        (TOOL_RESULT_LIMITS_FIELD, scope.result_limits()),
        (TOOL_SOURCE_COVERAGE_FIELD, scope.source_coverage()),
    ] {
        if matches!(requirement, ScopeRequirement::Required) && !arguments.contains_key(name) {
            return Err(ToolInputError::Invalid);
        }
        if matches!(requirement, ScopeRequirement::NotApplicable) && arguments.contains_key(name) {
            return Err(ToolInputError::Invalid);
        }
    }
    arguments
        .keys()
        .all(|name| {
            [
                TOOL_INSTRUMENT_IDS_FIELD,
                TOOL_TIME_RANGE_FIELD,
                TOOL_RESULT_LIMITS_FIELD,
                TOOL_SOURCE_COVERAGE_FIELD,
            ]
            .contains(&name.as_str())
        })
        .then_some(())
        .ok_or(ToolInputError::Invalid)
}

#[derive(Debug, Default)]
struct ProgressService {
    calls: AtomicUsize,
    rejected_bounds: AtomicUsize,
}

#[async_trait]
impl ToolServices for ProgressService {
    fn capabilities(&self) -> ServiceCapabilities {
        let descriptor = ToolDescriptor::try_new_with_output(
            "test.progress",
            "1",
            "Report bounded progress for a test-only operation.",
            json!({"type":"object","properties":{},"additionalProperties":false}),
            json!({
                "type":"object",
                "properties":{"done":{"type":"boolean"}},
                "required":["done"],
                "additionalProperties":false
            }),
            read_only_contract(ServiceDomain::Analysis, NO_SCOPE),
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
        context
            .progress()
            .report(1, Some(2), Some("phase one"))
            .await?;
        if matches!(
            context.progress().report(0, Some(2), None).await,
            Err(ProgressError::NonMonotonic)
        ) {
            self.rejected_bounds.fetch_add(1, Ordering::SeqCst);
        }
        if matches!(
            context
                .progress()
                .report(2, Some(2), Some("message exceeds limit"))
                .await,
            Err(ProgressError::MessageTooLong)
        ) {
            self.rejected_bounds.fetch_add(1, Ordering::SeqCst);
        }
        context
            .progress()
            .report(2, Some(2), Some("phase two"))
            .await?;
        if matches!(
            context.progress().report(2, Some(2), None).await,
            Err(ProgressError::TooManyUpdates)
        ) {
            self.rejected_bounds.fetch_add(1, Ordering::SeqCst);
        }
        TypedToolResult::try_new(
            json!({"done": true}),
            1,
            ToolResultMetadata::complete_not_applicable(),
            context.limits(),
        )
        .map_err(Into::into)
    }
}

#[derive(Debug)]
struct TerminalProgressService {
    release: Arc<Semaphore>,
    started: Notify,
    outcomes: mpsc::UnboundedSender<Result<(), ProgressError>>,
}

#[async_trait]
impl ToolServices for TerminalProgressService {
    fn capabilities(&self) -> ServiceCapabilities {
        let descriptor = ToolDescriptor::try_new_with_output(
            "test.terminal-progress",
            "1",
            "Attempt delayed progress around a terminal result.",
            json!({
                "type":"object",
                "properties":{"wait":{"type":"boolean"}},
                "required":["wait"],
                "additionalProperties":false
            }),
            json!({
                "type":"object",
                "properties":{"done":{"type":"boolean"}},
                "required":["done"],
                "additionalProperties":false
            }),
            read_only_contract(ServiceDomain::Analysis, NO_SCOPE),
            ToolEffects::read_only_closed_world(),
            |arguments: &serde_json::Map<String, Value>| {
                arguments
                    .get("wait")
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
        let wait = request
            .arguments()
            .get("wait")
            .and_then(Value::as_bool)
            .ok_or(ServiceError::InvalidRequest)?;
        let release = Arc::clone(&self.release);
        let progress = context.progress().clone();
        let outcomes = self.outcomes.clone();
        tokio::spawn(async move {
            if let Ok(permit) = release.acquire_owned().await {
                permit.forget();
                let _ = outcomes.send(progress.report(1, Some(1), Some("too late")).await);
            }
        });
        self.started.notify_one();
        if wait {
            context.cancellation().cancel();
            return Err(ServiceError::Cancelled);
        }
        TypedToolResult::try_new(
            json!({"done": true}),
            1,
            ToolResultMetadata::complete_not_applicable(),
            context.limits(),
        )
        .map_err(Into::into)
    }
}

#[async_trait]
impl ToolServices for WaitingService {
    fn capabilities(&self) -> ServiceCapabilities {
        let descriptors = COMPLETE_REGISTRY
            .into_iter()
            .filter_map(|entry| {
                let mut schema = registry_schema(entry.scope);
                if entry.name == "Market.Wait"
                    && let Value::Object(schema) = &mut schema
                {
                    schema.insert(
                        "description".to_owned(),
                        Value::String("x".repeat(8 * 1_024)),
                    );
                    schema.insert(
                        "examples".to_owned(),
                        Value::Array(vec![Value::String("y".repeat(8 * 1_024))]),
                    );
                }
                ToolDescriptor::try_new(
                    entry.name,
                    "1",
                    "Exercise one complete generic registry domain.",
                    schema,
                    read_only_contract(entry.domain, entry.scope),
                    ToolEffects::read_only_closed_world(),
                    move |arguments: &serde_json::Map<String, Value>| {
                        admit_registry_scope(arguments, entry.scope)
                    },
                )
                .ok()
            })
            .collect();
        ServiceCapabilities::try_new(descriptors).unwrap_or_else(|_| ServiceCapabilities::empty())
    }

    async fn call(
        &self,
        request: TypedToolRequest,
        context: RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if request.name() != "Market.Wait" {
            return TypedToolResult::try_new(
                json!({"ok":true}),
                1,
                ToolResultMetadata::complete_not_applicable(),
                context.limits(),
            )
            .map_err(Into::into);
        }
        self.started.notify_one();
        context.cancellation().cancelled().await;
        Err(ServiceError::Cancelled)
    }
}

#[tokio::test]
async fn duplicate_active_ids_are_rejected_and_cancellation_reaches_the_service()
-> Result<(), Box<dyn Error>> {
    let required_instrument = ToolScope::new(
        ScopeRequirement::Required,
        ScopeRequirement::NotApplicable,
        ScopeRequirement::NotApplicable,
        ScopeRequirement::NotApplicable,
    );
    let optional_instrument = ToolScope::new(
        ScopeRequirement::Optional,
        ScopeRequirement::NotApplicable,
        ScopeRequirement::NotApplicable,
        ScopeRequirement::NotApplicable,
    );
    let invalid_schemas = [
        (
            json!({"type":"object","properties":{},"additionalProperties":false}),
            required_instrument,
        ),
        (
            json!({
                "type":"object",
                "properties":{TOOL_INSTRUMENT_IDS_FIELD:{"type":"array"}},
                "required":[TOOL_INSTRUMENT_IDS_FIELD,TOOL_INSTRUMENT_IDS_FIELD],
                "additionalProperties":false
            }),
            required_instrument,
        ),
        (
            json!({
                "type":"object",
                "properties":{TOOL_INSTRUMENT_IDS_FIELD:{"type":"array"}},
                "required":[7],
                "additionalProperties":false
            }),
            required_instrument,
        ),
        (
            json!({
                "type":"object",
                "properties":{TOOL_INSTRUMENT_IDS_FIELD:{"type":"array"}},
                "required":["undeclared"],
                "additionalProperties":false
            }),
            required_instrument,
        ),
        (
            json!({
                "type":"object",
                "properties":{TOOL_INSTRUMENT_IDS_FIELD:{"type":"array"}},
                "required":[TOOL_INSTRUMENT_IDS_FIELD],
                "additionalProperties":false
            }),
            optional_instrument,
        ),
        (
            json!({
                "type":"object",
                "properties":{TOOL_INSTRUMENT_IDS_FIELD:{"type":"array"}},
                "additionalProperties":false
            }),
            NO_SCOPE,
        ),
    ];
    for (schema, scope) in invalid_schemas {
        assert!(matches!(
            ToolDescriptor::try_new(
                "Analysis.Invalid",
                "1",
                "Reject an inconsistent scope schema.",
                schema,
                read_only_contract(ServiceDomain::Analysis, scope),
                ToolEffects::read_only_closed_world(),
                |_arguments: &serde_json::Map<String, Value>| Ok(()),
            ),
            Err(ServiceCapabilityError::InvalidContract)
        ));
    }
    let service = Arc::new(WaitingService::default());
    assert_eq!(
        service.capabilities().tools().len(),
        COMPLETE_REGISTRY.len()
    );
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
        McpLimits::try_from(McpLimitSpec {
            maximum_active_requests: 1,
            ..McpLimitSpec::default()
        })?,
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
    let listed_tools = listed["result"]["tools"]
        .as_array()
        .ok_or("tools/list did not return an array")?;
    assert_eq!(listed_tools.len(), COMPLETE_REGISTRY.len());
    for (tool, expected) in listed_tools.iter().zip(COMPLETE_REGISTRY) {
        assert_eq!(tool["name"], expected.name);
        assert_eq!(tool["outputSchema"]["type"], "object");
        let output_variants = tool["outputSchema"]["oneOf"]
            .as_array()
            .ok_or("tool output schema must own the exact structured-content variants")?;
        assert!(
            !output_variants.is_empty()
                && output_variants.iter().all(|variant| {
                    variant["type"] == "object"
                        && variant["additionalProperties"] == false
                        && variant["required"].is_array()
                }),
            "tool output schema must be typed and closed: {tool}"
        );
        assert_eq!(
            tool["_meta"]["org.market-squawk/tool-contract"]["domain"],
            expected.domain.as_str()
        );
        assert_eq!(
            tool["_meta"]["org.market-squawk/tool-contract"]["schemaVersion"],
            1
        );
    }
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
            "params":{"name":"Market.Wait","arguments":{"unknown":true}}
        }))
        .await?;
    assert_eq!(harness.receive().await?["error"]["code"], -32602);
    assert_eq!(service.calls.load(Ordering::SeqCst), 0);

    let call = json!({
        "jsonrpc": "2.0",
        "id": "active-id",
        "method": "tools/call",
        "params": { "name": "Market.Wait", "arguments": {} }
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

    let duplicate = harness.receive().await?;
    assert_eq!(duplicate["error"]["code"], -32009);
    assert_eq!(service.calls.load(Ordering::SeqCst), 1);

    tokio::time::timeout(
        Duration::from_secs(1),
        audit.wait_for_result_count(market_squawk_mcp::AuditResultClass::Cancelled, 1),
    )
    .await??;
    harness
        .send(json!({"jsonrpc":"2.0","id":"after-release","method":"ping"}))
        .await?;
    assert_eq!(harness.receive().await?["result"], Value::Null);
    for cycle in 0..3 {
        let started = service.started.notified();
        let id = format!("cancel-cycle-{cycle}");
        harness
            .send(json!({
                "jsonrpc":"2.0","id":id.clone(),"method":"tools/call",
                "params":{"name":"Market.Wait","arguments":{}}
            }))
            .await?;
        tokio::time::timeout(Duration::from_secs(1), started).await?;
        harness
            .send(json!({
                "jsonrpc":"2.0","method":"notifications/cancelled",
                "params":{"requestId":id,"reason":"capacity reuse"}
            }))
            .await?;
        tokio::time::timeout(
            Duration::from_secs(1),
            audit.wait_for_result_count(market_squawk_mcp::AuditResultClass::Cancelled, cycle + 2),
        )
        .await??;
    }
    assert_eq!(service.calls.load(Ordering::SeqCst), 4);
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
            AuditPhase::MutationAdmitted | AuditPhase::MutationServiceCompleted => {}
        }
    }
    assert!(
        by_request
            .values()
            .all(|counts| counts.0 > 0 && counts.0 == counts.1)
    );
    Ok(())
}

#[tokio::test]
async fn progress_tokens_bridge_through_a_bounded_transport_neutral_reporter()
-> Result<(), Box<dyn Error>> {
    let maximum_message = 2_800;
    let encoded_progress = |token| {
        let params = ProgressNotificationParam::new(token, 9_007_199_254_740_991_f64)
            .with_total(9_007_199_254_740_991_f64)
            .with_message("\0".repeat(maximum_message));
        serde_json::to_vec(&ServerJsonRpcMessage::notification(
            ServerNotification::ProgressNotification(Notification::new(params)),
        ))
    };
    let string_frame = encoded_progress(ProgressToken(NumberOrString::String(Arc::from("\0"))))?;
    let numeric_frame = encoded_progress(ProgressToken(NumberOrString::Number(i64::MIN)))?;
    assert!(numeric_frame.len() > string_frame.len());
    let numeric_spec = McpLimitSpec {
        maximum_frame_bytes: numeric_frame.len(),
        maximum_body_bytes: numeric_frame.len(),
        maximum_progress_message_bytes: maximum_message,
        maximum_progress_token_bytes: 1,
        maximum_inline_bytes: 1,
        maximum_writer_queue_bytes: numeric_frame.len() + 1,
        ..McpLimitSpec::default()
    };
    assert!(McpLimits::try_from(numeric_spec).is_ok());
    assert!(matches!(
        McpLimits::try_from(McpLimitSpec {
            maximum_frame_bytes: numeric_frame.len() - 1,
            maximum_body_bytes: numeric_frame.len() - 1,
            maximum_writer_queue_bytes: numeric_frame.len(),
            ..numeric_spec
        }),
        Err(market_squawk_mcp::McpLimitError::ProgressExceedsFrame)
    ));

    let service = Arc::new(ProgressService::default());
    let mut harness = Harness::start(
        Arc::clone(&service),
        Arc::new(CollectingAudit::default()),
        McpLimits::try_from(McpLimitSpec {
            maximum_progress_updates: 2,
            maximum_progress_message_bytes: 16,
            maximum_progress_token_bytes: 8,
            ..McpLimitSpec::default()
        })?,
    )
    .await?;
    let _ = harness.initialize(json!("init-progress")).await?;
    harness.initialized().await?;
    harness
        .send(json!({
            "jsonrpc":"2.0","id":"progress","method":"tools/call",
            "params":{
                "name":"test.progress","arguments":{},
                "_meta":{"progressToken":"12345678"}
            }
        }))
        .await?;
    let first = harness.receive().await?;
    let second = harness.receive().await?;
    let result = harness.receive().await?;
    assert_eq!(first["method"], "notifications/progress");
    assert_eq!(first["params"]["progressToken"], "12345678");
    assert_eq!(first["params"]["progress"], 1.0);
    assert_eq!(first["params"]["message"], "phase one");
    assert_eq!(second["params"]["progress"], 2.0);
    assert_eq!(second["params"]["message"], "phase two");
    assert_eq!(result["id"], "progress");
    assert_eq!(result["result"]["structuredContent"]["data"]["done"], true);
    assert_eq!(
        result["result"]["structuredContent"]["metadata"]["completeness"],
        "complete"
    );
    assert_eq!(service.rejected_bounds.load(Ordering::SeqCst), 3);
    harness
        .send(json!({
            "jsonrpc":"2.0","id":"too-long","method":"tools/call",
            "params":{
                "name":"test.progress","arguments":{},
                "_meta":{"progressToken":"123456789"}
            }
        }))
        .await?;
    assert_eq!(harness.receive().await?["error"]["code"], -32010);
    assert_eq!(service.calls.load(Ordering::SeqCst), 1);
    assert_eq!(harness.close().await?, ServerExit::EndOfInput);
    Ok(())
}

#[tokio::test]
async fn terminal_results_close_delayed_progress_for_normal_and_cancelled_calls()
-> Result<(), Box<dyn Error>> {
    let (outcomes, mut reported) = mpsc::unbounded_channel();
    let service = Arc::new(TerminalProgressService {
        release: Arc::new(Semaphore::new(0)),
        started: Notify::new(),
        outcomes,
    });
    let mut harness = Harness::start(
        Arc::clone(&service),
        Arc::new(CollectingAudit::default()),
        McpLimits::try_from(McpLimitSpec::default())?,
    )
    .await?;
    let _ = harness.initialize(json!("init-terminal-progress")).await?;
    harness.initialized().await?;

    for (id, wait) in [("normal", false), ("cancelled", true)] {
        let started = service.started.notified();
        harness
            .send(json!({
                "jsonrpc":"2.0","id":id,"method":"tools/call",
                "params":{
                    "name":"test.terminal-progress","arguments":{"wait":wait},
                    "_meta":{"progressToken":id}
                }
            }))
            .await?;
        started.await;
        let terminal = harness.receive().await?;
        assert_eq!(terminal["id"], id);
        service.release.add_permits(1);
        let outcome = reported.recv().await;
        assert_eq!(outcome, Some(Err(ProgressError::Cancelled)));
        harness
            .send(json!({"jsonrpc":"2.0","id":format!("after-{id}"),"method":"ping"}))
            .await?;
        assert_eq!(harness.receive().await?["id"], format!("after-{id}"));
    }

    assert_eq!(harness.close().await?, ServerExit::EndOfInput);
    Ok(())
}

#[derive(Debug, Default)]
struct HttpResources;

#[async_trait]
impl McpResourceProvider for HttpResources {
    async fn read(
        &self,
        request: McpResourceRequest,
        _context: RequestContext,
    ) -> Result<McpResourceDocument, McpResourceError> {
        let kind = match request {
            McpResourceRequest::Service => "service",
            McpResourceRequest::Workspace => "workspace",
            McpResourceRequest::Source(_) => "source",
            McpResourceRequest::Model(_) => "model",
            McpResourceRequest::Job(_) => "job",
            McpResourceRequest::Artifact(_) => "artifact",
        };
        McpResourceDocument::try_new(json!({"kind":kind}), 1)
    }
}

#[derive(Debug)]
struct FixedHttpAuthenticator {
    alpha: AuthenticatedMcpClient,
    beta: AuthenticatedMcpClient,
    calls: AtomicUsize,
}

impl FixedHttpAuthenticator {
    fn try_new() -> Result<Self, Box<dyn Error>> {
        let alpha_id: ClientId = serde_json::from_str("\"00000000-0000-0000-0000-000000000011\"")?;
        let beta_id: ClientId = serde_json::from_str("\"00000000-0000-0000-0000-000000000012\"")?;
        let generation = CredentialGeneration::try_new(1)?;
        Ok(Self {
            alpha: AuthenticatedMcpClient::try_new(
                NamedClient::ClaudeCode,
                alpha_id,
                generation,
                1,
            )?,
            beta: AuthenticatedMcpClient::try_new(NamedClient::Codex, beta_id, generation, 1)?,
            calls: AtomicUsize::new(0),
        })
    }

    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl McpHttpAuthenticator for FixedHttpAuthenticator {
    fn authenticate(&self, bearer_token: &str) -> Result<AuthenticatedMcpClient, McpHttpAuthError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match bearer_token {
            "alpha" => Ok(self.alpha.clone()),
            "beta" => Ok(self.beta.clone()),
            _ => Err(McpHttpAuthError::Rejected),
        }
    }
}

fn stateless_request(method: Method, token: Option<&str>, body: Value) -> Request<Body> {
    let rpc_method = body.get("method").and_then(Value::as_str);
    let rpc_name = rpc_method.and_then(|method| match method {
        "tools/call" => body.pointer("/params/name").and_then(Value::as_str),
        "resources/read" => body.pointer("/params/uri").and_then(Value::as_str),
        _ => None,
    });
    let mut builder = Request::builder()
        .method(method)
        .uri("/mcp")
        .header(header::HOST, "127.0.0.1:43123")
        .header(header::ORIGIN, "http://localhost:43123")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::ACCEPT, "application/json, text/event-stream")
        .header("MCP-Protocol-Version", "2026-07-28");
    if let Some(method) = rpc_method {
        builder = builder.header("Mcp-Method", method);
    }
    if let Some(name) = rpc_name {
        builder = builder.header("Mcp-Name", name);
    }
    if let Some(token) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    builder
        .body(Body::from(body.to_string()))
        .unwrap_or_else(|_| Request::new(Body::empty()))
}

fn stateless_message(id: &str, method: &str, params: Value) -> Value {
    let mut params = params.as_object().cloned().unwrap_or_default();
    params.insert(
        "_meta".to_owned(),
        json!({
            "io.modelcontextprotocol/protocolVersion":"2026-07-28",
            "io.modelcontextprotocol/clientInfo":{"name":"tests","version":"1"},
            "io.modelcontextprotocol/clientCapabilities":{}
        }),
    );
    json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})
}

async fn response_json(response: axum::response::Response) -> Result<Value, Box<dyn Error>> {
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

async fn stateless_rpc(
    http: &McpHttpService,
    token: &str,
    method: &str,
    params: Value,
) -> Result<Value, Box<dyn Error>> {
    let response = http
        .handle(stateless_request(
            Method::POST,
            Some(token),
            stateless_message(method, method, params),
        ))
        .await;
    let status = response.status();
    let value = response_json(response).await?;
    assert_eq!(status, StatusCode::OK, "unexpected MCP response: {value}");
    Ok(value)
}

#[tokio::test]
async fn stateless_http_is_authenticated_bounded_and_has_only_stable_v1_capabilities()
-> Result<(), Box<dyn Error>> {
    let services = Arc::new(WaitingService::default());
    let limits = McpLimits::try_from(McpLimitSpec {
        maximum_frame_bytes: 256 * 1024,
        maximum_body_bytes: 8 * 1024,
        maximum_writer_queue_bytes: 256 * 1024 + 1,
        ..McpLimitSpec::default()
    })?;
    let audit = Arc::new(CollectingAudit::default());
    let factory = McpHandlerFactory::try_new(
        services.clone(),
        limits,
        audit.clone(),
        Arc::new(RejectingArtifacts),
        Arc::new(HttpResources),
    )?;
    let authenticator = Arc::new(FixedHttpAuthenticator::try_new()?);
    let http = McpHttpService::new(
        factory,
        authenticator.clone(),
        HttpMcpConfig::try_new(
            ["127.0.0.1:43123"],
            ["http://localhost:43123"],
            CancellationToken::new(),
        )?,
    );

    for token in ["alpha", "beta"] {
        let discover = stateless_rpc(&http, token, "server/discover", json!({})).await?;
        assert_eq!(
            discover["result"]["supportedVersions"],
            json!(["2026-07-28"]),
            "unexpected discovery response: {discover}"
        );
        assert!(discover["result"]["capabilities"].get("tools").is_some());
        assert!(
            discover["result"]["capabilities"]
                .get("resources")
                .is_some()
        );
        assert!(discover["result"]["capabilities"].get("tasks").is_none());
        let tools = stateless_rpc(&http, token, "tools/list", json!({})).await?;
        assert!(
            tools["result"]["tools"]
                .as_array()
                .is_some_and(|tools| tools.iter().any(|tool| tool["name"] == "Bot.Test"))
        );
        let resources = stateless_rpc(&http, token, "resources/templates/list", json!({})).await?;
        assert!(
            resources["result"]["resourceTemplates"]
                .as_array()
                .is_some_and(|templates| templates
                    .iter()
                    .any(|template| template["uriTemplate"] == "market-squawk://jobs/{job_id}"))
        );
        let read = stateless_rpc(
            &http,
            token,
            "tools/call",
            json!({"name":"Bot.Test","arguments":{}}),
        )
        .await?;
        assert_eq!(read["result"]["structuredContent"]["data"]["ok"], true);
    }

    let held = http
        .handle(stateless_request(
            Method::POST,
            Some("alpha"),
            stateless_message(
                "read",
                "tools/call",
                json!({"name":"Bot.Test","arguments":{}}),
            ),
        ))
        .await;
    assert_eq!(held.status(), StatusCode::OK);
    assert_eq!(
        http.handle(stateless_request(
            Method::POST,
            Some("alpha"),
            stateless_message("saturated", "server/discover", json!({})),
        ))
        .await
        .status(),
        StatusCode::TOO_MANY_REQUESTS
    );
    assert_eq!(
        http.handle(stateless_request(
            Method::POST,
            Some("beta"),
            stateless_message("isolated", "server/discover", json!({})),
        ))
        .await
        .status(),
        StatusCode::OK
    );
    let read = response_json(held).await?;
    assert_eq!(read["result"]["structuredContent"]["data"]["ok"], true);

    let interrupted_http = http.clone();
    let interrupted = tokio::spawn(async move {
        interrupted_http
            .handle(stateless_request(
                Method::POST,
                Some("alpha"),
                stateless_message(
                    "interrupted",
                    "tools/call",
                    json!({"name":"Market.Wait","arguments":{}}),
                ),
            ))
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), services.started.notified())
        .await
        .map_err(|_error| "interrupted MCP service call did not start")?;
    interrupted.abort();
    let _ = interrupted.await;
    tokio::time::timeout(
        Duration::from_secs(1),
        audit.wait_for_result_count(market_squawk_mcp::AuditResultClass::Cancelled, 1),
    )
    .await
    .map_err(|_error| "dropping the MCP request did not cancel its service call")??;
    let unaffected = stateless_rpc(&http, "beta", "server/discover", json!({})).await?;
    assert_eq!(
        unaffected["result"]["supportedVersions"],
        json!(["2026-07-28"])
    );

    for (request, expected) in [
        (
            stateless_request(
                Method::POST,
                None,
                stateless_message("missing", "server/discover", json!({})),
            ),
            StatusCode::UNAUTHORIZED,
        ),
        (
            stateless_request(
                Method::POST,
                Some("wrong"),
                stateless_message("wrong", "server/discover", json!({})),
            ),
            StatusCode::UNAUTHORIZED,
        ),
        (
            stateless_request(Method::GET, Some("alpha"), json!({})),
            StatusCode::METHOD_NOT_ALLOWED,
        ),
        (
            stateless_request(Method::DELETE, Some("alpha"), json!({})),
            StatusCode::METHOD_NOT_ALLOWED,
        ),
    ] {
        assert_eq!(http.handle(request).await.status(), expected);
    }

    let missing_metadata = stateless_request(
        Method::POST,
        Some("alpha"),
        json!({"jsonrpc":"2.0","id":"metadata","method":"server/discover","params":{}}),
    );
    assert_ne!(http.handle(missing_metadata).await.status(), StatusCode::OK);

    let authenticated_before_transport_rejections = authenticator.call_count();
    let mut wrong_host = stateless_request(
        Method::POST,
        Some("alpha"),
        stateless_message("host", "server/discover", json!({})),
    );
    wrong_host.headers_mut().insert(
        header::HOST,
        header::HeaderValue::from_static("example.com"),
    );
    assert_eq!(
        http.handle(wrong_host).await.status(),
        StatusCode::MISDIRECTED_REQUEST
    );

    let mut wrong_origin = stateless_request(
        Method::POST,
        Some("alpha"),
        stateless_message("origin", "server/discover", json!({})),
    );
    wrong_origin.headers_mut().insert(
        header::ORIGIN,
        header::HeaderValue::from_static("https://example.com"),
    );
    assert_eq!(
        http.handle(wrong_origin).await.status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        authenticator.call_count(),
        authenticated_before_transport_rejections,
        "Host and Origin must be rejected before credential verification"
    );

    let mut disagreement = stateless_request(
        Method::POST,
        Some("alpha"),
        stateless_message("version", "tools/list", json!({})),
    );
    disagreement.headers_mut().insert(
        "MCP-Protocol-Version",
        header::HeaderValue::from_static("2025-11-25"),
    );
    assert_ne!(http.handle(disagreement).await.status(), StatusCode::OK);

    let mut unsupported_body = stateless_message("unsupported", "tools/list", json!({}));
    unsupported_body["params"]["_meta"]["io.modelcontextprotocol/protocolVersion"] =
        json!("2025-11-25");
    let mut unsupported = stateless_request(Method::POST, Some("alpha"), unsupported_body);
    unsupported.headers_mut().insert(
        "MCP-Protocol-Version",
        header::HeaderValue::from_static("2025-11-25"),
    );
    assert_ne!(http.handle(unsupported).await.status(), StatusCode::OK);

    let mut legacy = stateless_request(
        Method::POST,
        Some("alpha"),
        stateless_message("legacy", "server/discover", json!({})),
    );
    legacy.headers_mut().insert(
        "Mcp-Session-Id",
        header::HeaderValue::from_static("legacy-session"),
    );
    legacy.headers_mut().insert(
        "Last-Event-ID",
        header::HeaderValue::from_static("legacy-event"),
    );
    assert_eq!(http.handle(legacy).await.status(), StatusCode::BAD_REQUEST);

    let oversized = stateless_request(
        Method::POST,
        Some("alpha"),
        json!({"padding":"x".repeat(9 * 1024)}),
    );
    assert_eq!(
        http.handle(oversized).await.status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );

    let job_id: JobId = "00000000-0000-0000-0000-000000000001".parse()?;
    let job = response_json(
        http.handle(stateless_request(
            Method::POST,
            Some("beta"),
            stateless_message(
                "job",
                "resources/read",
                json!({"uri":format!("market-squawk://jobs/{}", job_id.as_uuid())}),
            ),
        ))
        .await,
    )
    .await?;
    assert_eq!(
        job["result"]["contents"][0]["uri"],
        format!("market-squawk://jobs/{}", job_id.as_uuid())
    );
    assert_eq!(job["result"]["contents"][0]["mimeType"], "application/json");
    Ok(())
}

#[derive(Debug, Default)]
struct RecordingRelayTransport {
    exchanges: Mutex<Vec<Value>>,
    waiting_started: Notify,
    waiting_cancelled: Notify,
}

struct RelayCancellationGuard<'a>(&'a Notify);

impl Drop for RelayCancellationGuard<'_> {
    fn drop(&mut self) {
        self.0.notify_waiters();
    }
}

#[derive(Clone, Copy, Debug)]
enum InvalidRelayBoundary {
    Status,
    Oversized,
}

#[derive(Debug)]
struct InvalidRelayTransport(InvalidRelayBoundary);

#[async_trait]
impl McpRelayTransport for InvalidRelayTransport {
    async fn exchange(
        &self,
        request: McpRelayExchange,
        _cancellation: CancellationToken,
    ) -> Result<McpRelayResponse, McpRelayTransportError> {
        let value: Value = serde_json::from_slice(request.body())
            .map_err(|_error| McpRelayTransportError::InvalidRequest)?;
        let id = value.get("id").cloned().unwrap_or(Value::Null);
        if request.method() == "initialize" {
            let body = serde_json::to_vec(&json!({
                "jsonrpc":"2.0",
                "id":id,
                "result":{
                    "protocolVersion":"2026-07-28",
                    "capabilities":{},
                    "serverInfo":{"name":"market-squawk","version":"1.0.0"}
                }
            }))
            .map_err(|_error| McpRelayTransportError::InvalidResponse)?;
            return McpRelayResponse::try_new(200, Some("application/json"), body)
                .map_err(|_error| McpRelayTransportError::InvalidResponse);
        }
        match self.0 {
            InvalidRelayBoundary::Status => McpRelayResponse::try_new(401, None, Vec::new()),
            InvalidRelayBoundary::Oversized => McpRelayResponse::try_new(
                200,
                Some("application/json"),
                vec![b' '; request.maximum_response_bytes() + 1],
            ),
        }
        .map_err(|_error| McpRelayTransportError::InvalidResponse)
    }
}

#[async_trait]
impl McpRelayTransport for RecordingRelayTransport {
    async fn exchange(
        &self,
        request: McpRelayExchange,
        _cancellation: CancellationToken,
    ) -> Result<McpRelayResponse, McpRelayTransportError> {
        assert!(request.maximum_response_bytes() > 0);
        let body: Value = serde_json::from_slice(request.body())
            .map_err(|_error| McpRelayTransportError::InvalidRequest)?;
        self.exchanges
            .lock()
            .map_err(|_error| McpRelayTransportError::Unavailable)?
            .push(body.clone());
        let id = body.get("id").cloned().unwrap_or(Value::Null);
        let result = match request.method() {
            "initialize" => json!({
                "protocolVersion":"2026-07-28",
                "capabilities":{"tools":{},"resources":{}},
                "serverInfo":{"name":"market-squawk","version":"1.0.0"}
            }),
            "tools/list" => json!({"tools":[{"name":"Market.Lookup"}]}),
            "tools/call" => {
                assert_eq!(request.name(), Some("Market.Wait"));
                let _cancelled = RelayCancellationGuard(&self.waiting_cancelled);
                self.waiting_started.notify_waiters();
                std::future::pending::<()>().await;
                return Err(McpRelayTransportError::Interrupted);
            }
            "resources/read" => {
                assert_eq!(request.name(), Some("market-squawk://service"));
                json!({"contents":[{
                    "uri":"market-squawk://service",
                    "mimeType":"application/json",
                    "text":"{\"service\":\"shared\"}"
                }]})
            }
            _ => return Err(McpRelayTransportError::InvalidRequest),
        };
        let encoded = serde_json::to_vec(&json!({"jsonrpc":"2.0","id":id,"result":result}))
            .map_err(|_error| McpRelayTransportError::InvalidResponse)?;
        McpRelayResponse::try_new(200, Some("application/json"), encoded)
            .map_err(|_error| McpRelayTransportError::InvalidResponse)
    }
}

#[tokio::test]
async fn stdio_relay_proxies_tools_and_resources_to_the_shared_service()
-> Result<(), Box<dyn Error>> {
    let transport = Arc::new(RecordingRelayTransport::default());
    let relay = McpStdioRelay::try_new(
        NamedClient::Codex,
        transport.clone(),
        McpLimits::try_from(McpLimitSpec::default())?,
    )?;
    let (client, service) = tokio::io::duplex(64 * 1024);
    let (service_reader, service_writer) = tokio::io::split(service);
    let task = tokio::spawn(relay.serve_unverified_io(
        service_reader,
        service_writer,
        CancellationToken::new(),
    ));
    let (client_reader, mut client_writer) = tokio::io::split(client);
    let mut client_reader = BufReader::new(client_reader);

    async fn round_trip(
        writer: &mut WriteHalf<DuplexStream>,
        reader: &mut BufReader<ReadHalf<DuplexStream>>,
        message: Value,
    ) -> Result<Value, Box<dyn Error>> {
        writer.write_all(&serde_json::to_vec(&message)?).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
        let mut line = String::new();
        reader.read_line(&mut line).await?;
        Ok(serde_json::from_str(&line)?)
    }

    let initialized = round_trip(
        &mut client_writer,
        &mut client_reader,
        json!({
            "jsonrpc":"2.0",
            "id":"initialize",
            "method":"initialize",
            "params":{
                "protocolVersion":"2025-11-25",
                "capabilities":{},
                "clientInfo":{"name":"codex","version":"1"}
            }
        }),
    )
    .await?;
    assert_eq!(initialized["result"]["protocolVersion"], "2026-07-28");
    client_writer
        .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n")
        .await?;

    let tools = round_trip(
        &mut client_writer,
        &mut client_reader,
        json!({"jsonrpc":"2.0","id":"tools","method":"tools/list","params":{}}),
    )
    .await?;
    assert_eq!(tools["result"]["tools"][0]["name"], "Market.Lookup");
    let resource = round_trip(
        &mut client_writer,
        &mut client_reader,
        json!({
            "jsonrpc":"2.0",
            "id":"resource",
            "method":"resources/read",
            "params":{"uri":"market-squawk://service"}
        }),
    )
    .await?;
    assert_eq!(
        resource["result"]["contents"][0]["uri"],
        "market-squawk://service"
    );

    let waiting_started = transport.waiting_started.notified();
    tokio::pin!(waiting_started);
    waiting_started.as_mut().enable();
    client_writer
        .write_all(
            b"{\"jsonrpc\":\"2.0\",\"id\":\"waiting\",\"method\":\"tools/call\",\"params\":{\"name\":\"Market.Wait\",\"arguments\":{}}}\n",
        )
        .await?;
    tokio::time::timeout(Duration::from_secs(1), waiting_started).await?;
    let waiting_cancelled = transport.waiting_cancelled.notified();
    tokio::pin!(waiting_cancelled);
    waiting_cancelled.as_mut().enable();
    client_writer
        .write_all(
            b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled\",\"params\":{\"requestId\":\"waiting\"}}\n",
        )
        .await?;
    tokio::time::timeout(Duration::from_secs(1), waiting_cancelled).await?;

    client_writer.shutdown().await?;
    assert_eq!(task.await??, ServerExit::EndOfInput);
    let exchanges = transport
        .exchanges
        .lock()
        .map_err(|_error| "relay exchange log was poisoned")?;
    assert_eq!(exchanges.len(), 4);
    for exchange in &exchanges[1..] {
        assert_eq!(
            exchange.pointer("/params/_meta/io.modelcontextprotocol~1protocolVersion"),
            Some(&json!("2026-07-28"))
        );
        assert_eq!(
            exchange.pointer("/params/_meta/io.modelcontextprotocol~1clientInfo/name"),
            Some(&json!("codex"))
        );
    }
    Ok(())
}

#[tokio::test]
async fn stdio_relay_rejects_invalid_http_status_and_excessive_response()
-> Result<(), Box<dyn Error>> {
    for boundary in [
        InvalidRelayBoundary::Status,
        InvalidRelayBoundary::Oversized,
    ] {
        let relay = McpStdioRelay::try_new(
            NamedClient::ClaudeCode,
            Arc::new(InvalidRelayTransport(boundary)),
            McpLimits::try_from(McpLimitSpec::default())?,
        )?;
        let (mut client, service) = tokio::io::duplex(64 * 1024);
        let (service_reader, service_writer) = tokio::io::split(service);
        let task = tokio::spawn(relay.serve_unverified_io(
            service_reader,
            service_writer,
            CancellationToken::new(),
        ));
        client
            .write_all(
                b"{\"jsonrpc\":\"2.0\",\"id\":\"init\",\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2026-07-28\",\"capabilities\":{},\"clientInfo\":{\"name\":\"claude-code\",\"version\":\"1\"}}}\n",
            )
            .await?;
        let mut initialized = String::new();
        BufReader::new(&mut client)
            .read_line(&mut initialized)
            .await?;
        client
            .write_all(
                b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n{\"jsonrpc\":\"2.0\",\"id\":\"tools\",\"method\":\"tools/list\",\"params\":{}}\n",
            )
            .await?;
        let outcome = tokio::time::timeout(Duration::from_secs(1), task).await??;
        assert!(matches!(outcome, Err(McpRelayError::InvalidResponse)));
    }
    Ok(())
}
