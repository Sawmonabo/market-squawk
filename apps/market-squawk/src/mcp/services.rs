//! Transport-neutral ownership for the current local diagnostic state.

use std::{path::PathBuf, time::Instant};

use async_trait::async_trait;
use market_squawk_services::{
    RequestContext, ServiceCapabilities, ServiceCapabilityError, ServiceError, ToolDescriptor,
    ToolEffects, ToolInputError, ToolServices, TypedToolRequest, TypedToolResult,
};
use serde_json::{Map, Value, json};

use crate::{
    diagnostic_engine::SharedDiagnosticEngine,
    journal::{JournalError, JournalReader},
    replay::ReplaySummary,
};

const CONTRACT_VERSION: &str = "1";
const MARKET_GET_SNAPSHOT: &str = "Market.GetSnapshot";
const MARKET_GET_QUALITY: &str = "Market.GetQuality";
const BOT_GET_STATUS: &str = "Bot.GetStatus";
const JOURNAL_GET_SUMMARY: &str = "Journal.GetSummary";
const RISK_TRIGGER_KILL_SWITCH: &str = "Risk.TriggerKillSwitch";
const MAXIMUM_PRODUCT_BYTES: usize = 128;
const MAXIMUM_REASON_BYTES: usize = 500;
const MAXIMUM_JOURNAL_RECORDS: u64 = 100_000;
const MAXIMUM_JOURNAL_PAYLOAD_BYTES: u64 = 64 * 1024 * 1024;

/// Frozen application state retained by the hardened MCP composition.
#[derive(Debug)]
pub(super) struct LocalToolServices {
    diagnostic_engine: SharedDiagnosticEngine,
    journal_path: PathBuf,
    capabilities: ServiceCapabilities,
}

impl LocalToolServices {
    pub(super) fn try_new(
        diagnostic_engine: SharedDiagnosticEngine,
        journal_path: PathBuf,
    ) -> Result<Self, ServiceCapabilityError> {
        Ok(Self {
            diagnostic_engine,
            journal_path,
            capabilities: diagnostic_capabilities()?,
        })
    }
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
        let (content, item_count) = match request.name() {
            MARKET_GET_SNAPSHOT => self.market_snapshot(&request)?,
            MARKET_GET_QUALITY => self.market_quality()?,
            BOT_GET_STATUS => self.bot_status(),
            JOURNAL_GET_SUMMARY => {
                let summary = bounded_journal_summary(&self.journal_path, &context)?;
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
        TypedToolResult::try_new(content, item_count, context.limits()).map_err(Into::into)
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
        ToolEffects::read_only_closed_world(),
        |arguments: &Map<String, Value>| admit_optional_string(arguments, "product", MAXIMUM_PRODUCT_BYTES),
    )?);
    tools.push(ToolDescriptor::try_new(
        MARKET_GET_QUALITY,
        CONTRACT_VERSION,
        "Get the app-local diagnostic `QualityState`, book and heartbeat timestamps, sequence information, and gap counters; diagnostic `VALID` is not canonical `DataQuality` and can never establish `DataQuality::DirectVerified`. This state cannot mint production live authority.",
        empty_schema(),
        ToolEffects::read_only_closed_world(),
        admit_empty,
    )?);
    tools.push(ToolDescriptor::try_new(
        BOT_GET_STATUS,
        CONTRACT_VERSION,
        "Get diagnostic positions, fills, cash flow, and current risk state. This is paper simulation only, with no production order authority; this server never submits live orders.",
        empty_schema(),
        ToolEffects::read_only_closed_world(),
        admit_empty,
    )?);
    tools.push(ToolDescriptor::try_new(
        JOURNAL_GET_SUMMARY,
        CONTRACT_VERSION,
        "Validate and summarize the configured immutable local raw-data journal without accepting arbitrary filesystem paths.",
        empty_schema(),
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

fn bounded_journal_summary(
    path: &std::path::Path,
    context: &RequestContext,
) -> Result<ReplaySummary, ServiceError> {
    ensure_request_live(context)?;
    let mut reader = match JournalReader::open(path) {
        Ok(reader) => reader,
        Err(JournalError::Io { source, .. }) if source.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ReplaySummary::default());
        }
        Err(_error) => return Err(ServiceError::Unavailable),
    };
    let mut summary = ReplaySummary::default();
    loop {
        ensure_request_live(context)?;
        let Some(record) = reader
            .next_record()
            .map_err(|_error| ServiceError::Unavailable)?
        else {
            return Ok(summary);
        };
        if summary.records >= MAXIMUM_JOURNAL_RECORDS {
            return Err(ServiceError::ResourceExhausted);
        }
        let payload_bytes = u64::try_from(record.payload().len())
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        let aggregate_payload_bytes = summary
            .bytes
            .checked_add(payload_bytes)
            .ok_or(ServiceError::ResourceExhausted)?;
        if aggregate_payload_bytes > MAXIMUM_JOURNAL_PAYLOAD_BYTES {
            return Err(ServiceError::ResourceExhausted);
        }
        summary
            .observe(&record)
            .map_err(|_error| ServiceError::Internal)?;
    }
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
    use crate::{AppPaths, diagnostic_engine::DiagnosticEngine};

    #[tokio::test]
    async fn capabilities_and_journal_summary_preserve_the_five_diagnostic_contracts()
    -> Result<(), Box<dyn Error>> {
        let temporary = tempfile::tempdir()?;
        let paths = AppPaths::prepare(temporary.path())?;
        let journal_path = paths.journal_write_file("services")?;
        paths.open_journal_writer("services")?.flush()?;
        let services = LocalToolServices::try_new(
            Arc::new(RwLock::new(DiagnosticEngine::new(5_000, false))),
            journal_path,
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
        Ok(())
    }
}
