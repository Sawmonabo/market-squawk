//! Test-only transport-neutral ownership for the retired local diagnostic MCP surface.

use std::time::Instant;

use async_trait::async_trait;
use market_squawk_platform::ConfiguredJournalReadTarget;
use market_squawk_services::{
    RequestContext, ScopeRequirement, ServiceCapabilities, ServiceCapabilityError, ServiceDomain,
    ServiceError, SourceEvidencePolicy, ToolArtifactPolicy, ToolAuthorization, ToolContract,
    ToolDescriptor, ToolEffects, ToolInputError, ToolResultMetadata, ToolResultPolicy, ToolScope,
    ToolServices, TypedToolRequest, TypedToolResult,
};
use serde_json::{Map, Value, json};
use thiserror::Error;

use crate::{
    diagnostic_engine::SharedDiagnosticEngine,
    mcp::journal_worker::{JournalSummaryWorker, JournalWorkerShutdown, JournalWorkerStartError},
};

const CONTRACT_VERSION: &str = "1";
const MARKET_GET_SNAPSHOT: &str = "Market.GetSnapshot";
const MARKET_GET_QUALITY: &str = "Market.GetQuality";
const BOT_GET_STATUS: &str = "Bot.GetStatus";
const JOURNAL_GET_SUMMARY: &str = "Journal.GetSummary";
const RISK_TRIGGER_KILL_SWITCH: &str = "Risk.TriggerKillSwitch";
const MAXIMUM_PRODUCT_BYTES: usize = 128;
const MAXIMUM_REASON_BYTES: usize = 500;
const LOCAL_SCOPE: ToolScope = ToolScope::new(
    ScopeRequirement::NotApplicable,
    ScopeRequirement::NotApplicable,
    ScopeRequirement::NotApplicable,
    ScopeRequirement::NotApplicable,
);

/// Frozen application state retained by the hardened MCP composition.
#[derive(Debug)]
pub(super) struct LocalToolServices {
    diagnostic_engine: SharedDiagnosticEngine,
    journal_worker: JournalSummaryWorker,
    capabilities: ServiceCapabilities,
}

impl LocalToolServices {
    pub(super) fn try_new(
        diagnostic_engine: SharedDiagnosticEngine,
        journal_target: ConfiguredJournalReadTarget,
    ) -> Result<Self, LocalToolServicesError> {
        let capabilities = diagnostic_capabilities()?;
        let journal_worker = JournalSummaryWorker::try_start(journal_target)?;
        Ok(Self {
            diagnostic_engine,
            journal_worker,
            capabilities,
        })
    }

    pub(super) async fn shutdown(&self, deadline: tokio::time::Instant) -> JournalWorkerShutdown {
        self.journal_worker.shutdown(deadline).await
    }
}

#[derive(Debug, Error)]
pub(super) enum LocalToolServicesError {
    #[error(transparent)]
    Capability(#[from] ServiceCapabilityError),
    #[error(transparent)]
    JournalWorker(#[from] JournalWorkerStartError),
}

#[async_trait]
impl ToolServices for LocalToolServices {
    fn capabilities(&self) -> ServiceCapabilities {
        self.capabilities.clone()
    }

    async fn call(
        &self,
        request: TypedToolRequest,
        context: RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        ensure_request_live(&context)?;
        let source_derived = matches!(request.name(), MARKET_GET_SNAPSHOT | MARKET_GET_QUALITY);
        let (content, item_count) = match request.name() {
            MARKET_GET_SNAPSHOT => self.market_snapshot(&request)?,
            MARKET_GET_QUALITY => self.market_quality()?,
            BOT_GET_STATUS => self.bot_status(),
            JOURNAL_GET_SUMMARY => {
                let summary = self.journal_worker.summarize(context.clone()).await?;
                (
                    serde_json::to_value(summary).map_err(|_error| ServiceError::Internal)?,
                    1,
                )
            }
            RISK_TRIGGER_KILL_SWITCH => {
                ensure_request_live(&context)?;
                let reason = admitted_string(request.arguments(), "reason", MAXIMUM_REASON_BYTES)?;
                self.diagnostic_engine.write().trigger_kill_switch();
                (json!({"triggered": true, "reason": reason}), 1)
            }
            _ => return Err(ServiceError::NotFound),
        };
        let metadata = if source_derived {
            ToolResultMetadata::try_complete(
                json!({
                    "source": "coinbase-exchange",
                    "coverage": "single_venue_partial_diagnostic"
                }),
                json!({
                    "maximumQuality": "direct_unverified",
                    "executionEligible": false
                }),
            )?
        } else {
            ToolResultMetadata::complete_not_applicable()
        };
        TypedToolResult::try_new(content, item_count, metadata, context.limits())
            .map_err(Into::into)
    }
}

impl LocalToolServices {
    fn market_snapshot(&self, request: &TypedToolRequest) -> Result<(Value, usize), ServiceError> {
        let snapshot = self.diagnostic_engine.read().snapshot();
        let Some(product) = request.arguments().get("product") else {
            let item_count = snapshot.products.len().max(1);
            let content =
                serde_json::to_value(snapshot).map_err(|_error| ServiceError::Internal)?;
            return Ok((content, item_count));
        };
        let product = product.as_str().ok_or(ServiceError::InvalidRequest)?.trim();
        let product_snapshot = snapshot
            .products
            .get(product)
            .ok_or(ServiceError::NotFound)?;
        Ok((
            serde_json::to_value(product_snapshot).map_err(|_error| ServiceError::Internal)?,
            1,
        ))
    }

    fn market_quality(&self) -> Result<(Value, usize), ServiceError> {
        let snapshot = self.diagnostic_engine.read().snapshot();
        let item_count = snapshot.products.len().max(1);
        let mut quality = Map::new();
        for (product, state) in snapshot.products {
            quality.insert(
                product,
                serde_json::to_value(state.quality).map_err(|_error| ServiceError::Internal)?,
            );
        }
        Ok((Value::Object(quality), item_count))
    }

    fn bot_status(&self) -> (Value, usize) {
        let snapshot = self.diagnostic_engine.read().snapshot();
        (
            json!({
                "mode": "paper_only",
                "account": snapshot.paper_account,
                "risk": snapshot.risk,
            }),
            1,
        )
    }
}

fn diagnostic_capabilities() -> Result<ServiceCapabilities, ServiceCapabilityError> {
    let mut tools = Vec::with_capacity(5);
    tools.push(ToolDescriptor::try_new(
        MARKET_GET_SNAPSHOT,
        CONTRACT_VERSION,
        "Get the latest diagnostic and authority-free compatibility snapshot from Coinbase Exchange single-venue, partial coverage. Omit product to return all observed products. Diagnostic values cannot mint production live authority.",
        json!({
            "type": "object",
            "properties": {
                "product": {"type": "string", "minLength": 1, "maxLength": 128}
            },
            "additionalProperties": false
        }),
        contract(
            ServiceDomain::Market,
            ToolAuthorization::ReadOnly,
            SourceEvidencePolicy::Required,
        ),
        ToolEffects::read_only_closed_world(),
        |arguments: &Map<String, Value>| admit_optional_string(arguments, "product", MAXIMUM_PRODUCT_BYTES),
    )?);
    tools.push(ToolDescriptor::try_new(
        MARKET_GET_QUALITY,
        CONTRACT_VERSION,
        "Get the app-local diagnostic `QualityState`, book and heartbeat timestamps, sequence information, and gap counters; diagnostic `VALID` is not canonical `DataQuality` and can never establish `DataQuality::DirectVerified`. This state cannot mint production live authority.",
        empty_schema(),
        contract(
            ServiceDomain::Market,
            ToolAuthorization::ReadOnly,
            SourceEvidencePolicy::Required,
        ),
        ToolEffects::read_only_closed_world(),
        admit_empty,
    )?);
    tools.push(ToolDescriptor::try_new(
        BOT_GET_STATUS,
        CONTRACT_VERSION,
        "Get diagnostic positions, fills, cash flow, and current risk state. This is paper simulation only, with no production order authority; this server never submits live orders.",
        empty_schema(),
        contract(
            ServiceDomain::Bot,
            ToolAuthorization::ReadOnly,
            SourceEvidencePolicy::NotApplicable,
        ),
        ToolEffects::read_only_closed_world(),
        admit_empty,
    )?);
    tools.push(ToolDescriptor::try_new(
        JOURNAL_GET_SUMMARY,
        CONTRACT_VERSION,
        "Validate and summarize the configured immutable local raw-data journal without accepting arbitrary filesystem paths.",
        empty_schema(),
        contract(
            ServiceDomain::Research,
            ToolAuthorization::ReadOnly,
            SourceEvidencePolicy::NotApplicable,
        ),
        ToolEffects::read_only_closed_world(),
        admit_empty,
    )?);
    tools.push(ToolDescriptor::try_new(
        RISK_TRIGGER_KILL_SWITCH,
        CONTRACT_VERSION,
        "Irreversibly stop the compatibility paper simulation only for the current run. It has no production order authority and cannot control production execution. Requires explicit confirmation and a reason.",
        json!({
            "type": "object",
            "properties": {
                "confirm": {"type": "boolean", "const": true},
                "reason": {"type": "string", "minLength": 1, "maxLength": 500}
            },
            "required": ["confirm", "reason"],
            "additionalProperties": false
        }),
        contract(
            ServiceDomain::Bot,
            ToolAuthorization::LocalConfirmation,
            SourceEvidencePolicy::NotApplicable,
        ),
        ToolEffects::try_new(false, true, true, false)?,
        |arguments: &Map<String, Value>| {
            if arguments.len() != 2 || arguments.get("confirm") != Some(&Value::Bool(true)) {
                return Err(ToolInputError::Invalid);
            }
            admitted_string(arguments, "reason", MAXIMUM_REASON_BYTES)
                .map(|_reason| ())
                .map_err(|_error| ToolInputError::Invalid)
        },
    )?);
    ServiceCapabilities::try_new(tools)
}

const fn contract(
    domain: ServiceDomain,
    authorization: ToolAuthorization,
    evidence: SourceEvidencePolicy,
) -> ToolContract {
    ToolContract::new(
        domain,
        authorization,
        LOCAL_SCOPE,
        ToolResultPolicy::new(evidence, ToolArtifactPolicy::OpaqueOnOverflow),
    )
}

fn empty_schema() -> Value {
    json!({"type": "object", "additionalProperties": false})
}

fn admit_empty(arguments: &Map<String, Value>) -> Result<(), ToolInputError> {
    arguments
        .is_empty()
        .then_some(())
        .ok_or(ToolInputError::Invalid)
}

fn admit_optional_string(
    arguments: &Map<String, Value>,
    key: &str,
    maximum_bytes: usize,
) -> Result<(), ToolInputError> {
    if arguments.len() > 1 || arguments.keys().any(|candidate| candidate != key) {
        return Err(ToolInputError::Invalid);
    }
    match arguments.get(key) {
        None => Ok(()),
        Some(_value) => admitted_string(arguments, key, maximum_bytes)
            .map(|_value| ())
            .map_err(|_error| ToolInputError::Invalid),
    }
}

fn admitted_string<'arguments>(
    arguments: &'arguments Map<String, Value>,
    key: &str,
    maximum_bytes: usize,
) -> Result<&'arguments str, ServiceError> {
    let raw = arguments
        .get(key)
        .and_then(Value::as_str)
        .ok_or(ServiceError::InvalidRequest)?;
    if raw.len() > maximum_bytes {
        return Err(ServiceError::InvalidRequest);
    }
    let value = raw.trim();
    if value.is_empty() {
        return Err(ServiceError::InvalidRequest);
    }
    Ok(value)
}

fn ensure_request_live(context: &RequestContext) -> Result<(), ServiceError> {
    if context.cancellation().is_cancelled() {
        return Err(ServiceError::Cancelled);
    }
    if Instant::now() >= context.deadline() {
        return Err(ServiceError::DeadlineExceeded);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{error::Error, sync::Arc, time::Duration};

    use market_squawk_services::{JsonStructureLimits, RequestId, ServiceLimits, ToolServices};
    use parking_lot::RwLock;
    use serde_json::{Map, Value};
    use tokio_util::sync::CancellationToken;

    use super::LocalToolServices;
    use crate::{
        AppPaths,
        application::Application,
        diagnostic_engine::DiagnosticEngine,
        mcp::{LocalMcpComposition, LocalMcpCompositionError},
    };

    #[tokio::test]
    async fn capabilities_and_journal_summary_preserve_the_five_diagnostic_contracts()
    -> Result<(), Box<dyn Error>> {
        let _shipping_constructor: fn(
            &AppPaths,
            Arc<Application>,
        )
            -> Result<LocalMcpComposition, LocalMcpCompositionError> = LocalMcpComposition::try_new;
        let temporary = tempfile::tempdir()?;
        let paths = AppPaths::prepare(temporary.path())?;
        paths.open_journal_writer("services")?.flush()?;
        let journal_target = paths.configured_journal_read_target("services", None)?;
        let services = LocalToolServices::try_new(
            Arc::new(RwLock::new(DiagnosticEngine::new(5_000, false))),
            journal_target,
        )?;
        let capabilities = services.capabilities();
        let names = capabilities
            .tools()
            .iter()
            .map(|descriptor| descriptor.name())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            [
                "Bot.GetStatus",
                "Journal.GetSummary",
                "Market.GetQuality",
                "Market.GetSnapshot",
                "Risk.TriggerKillSwitch",
            ]
        );
        let mut oversized_product = Map::new();
        oversized_product.insert("product".to_owned(), Value::String(" ".repeat(129)));
        assert!(
            capabilities
                .find("Market.GetSnapshot")
                .ok_or("Market.GetSnapshot capability is missing")?
                .admit(oversized_product)
                .is_err()
        );

        let request = capabilities
            .find("Journal.GetSummary")
            .ok_or("Journal.GetSummary capability is missing")?
            .admit(Map::new())?;
        let structure = JsonStructureLimits::try_new(16, 64 * 1024, 1_000, 1_000)?;
        let limits = ServiceLimits::try_new(64 * 1024, 1_000, 1024 * 1024, 10_000, structure)?;
        let deadline = std::time::Instant::now()
            .checked_add(Duration::from_secs(1))
            .ok_or("test deadline overflow")?;
        let result = services
            .call(
                request,
                market_squawk_services::RequestContext::new(
                    RequestId::Integer(1),
                    CancellationToken::new(),
                    deadline,
                    limits,
                ),
            )
            .await?;
        assert_eq!(result.structured_content()["records"], 0);
        assert_eq!(result.item_count(), 1);
        let deadline = tokio::time::Instant::now()
            .checked_add(Duration::from_secs(1))
            .ok_or("shutdown deadline overflow")?;
        assert_eq!(
            services.shutdown(deadline).await,
            crate::mcp::journal_worker::JournalWorkerShutdown::Joined
        );
        Ok(())
    }
}
