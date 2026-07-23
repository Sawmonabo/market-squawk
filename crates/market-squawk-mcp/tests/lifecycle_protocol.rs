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
use market_squawk_mcp::{
    ArtifactError, ArtifactPublication, ArtifactPublicationContext, ArtifactReference,
    ArtifactRepository, AuditCompletion, AuditCompletionReservation, AuditError, AuditEvent,
    AuditPhase, AuditSink, LocalProcessIdentityClass, McpLimitSpec, McpLimits, McpServer,
    MutationAuditBundle, MutationAuditReservation, ServerError, ServerExit,
};
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
    assert_eq!(initialized["result"]["protocolVersion"], "2025-11-25");
    assert!(initialized["result"]["capabilities"].get("tools").is_none());

    harness
        .send(json!({"jsonrpc":"2.0","id":"early","method":"tools/list"}))
        .await?;
    assert_eq!(harness.receive().await?["error"]["code"], -32002);

    harness.initialized().await?;
    harness
        .send(json!({"jsonrpc":"2.0","id":"","method":"ping"}))
        .await?;
    let empty_id = harness.receive().await?;
    assert_eq!(empty_id["id"], "");
    assert_eq!(empty_id["result"], json!({}));

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
        let descriptor = ToolDescriptor::try_new(
            "test.progress",
            "1",
            "Report bounded progress for a test-only operation.",
            json!({"type":"object","properties":{},"additionalProperties":false}),
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
        let descriptor = ToolDescriptor::try_new(
            "test.terminal-progress",
            "1",
            "Attempt delayed progress around a terminal result.",
            json!({
                "type":"object",
                "properties":{"wait":{"type":"boolean"}},
                "required":["wait"],
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
    assert_eq!(harness.receive().await?["result"], json!({}));
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
