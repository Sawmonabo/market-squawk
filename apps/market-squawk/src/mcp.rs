use std::{
    collections::VecDeque,
    num::NonZeroUsize,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use market_squawk_domain::DataQuality;
use parking_lot::Mutex;
use serde::Serialize;
use serde_json::{Map, Value, json};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio_util::sync::CancellationToken;

use crate::{diagnostic_engine::SharedDiagnosticEngine, replay::summarize_journal};

const PROTOCOL_VERSION: &str = "2025-11-25";
const MAX_TOOL_CALLS_PER_SECOND: usize = 100;
const MAX_MCP_LINE_BYTES: usize = 1024 * 1024;
const MCP_READER_SCRATCH_BYTES: usize = 8 * 1024;
const DIAGNOSTIC_CONTRACT_SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum McpSurface {
    DiagnosticCompatibility,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum ExecutionAuthority {
    None,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum DiagnosticCoverage {
    CoinbaseExchangeSingleVenuePartial,
    LocalDiagnosticState,
    ConfiguredLocalJournal,
    CurrentLocalRun,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum SimulationAccess {
    None,
    ReadOnly,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum ControlAuthority {
    None,
    PaperSimulationStopOnly,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum ResourceScope {
    CoinbaseDiagnosticMarketState,
    CurrentPaperSimulationRun,
    ConfiguredLocalJournal,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DiagnosticServerContract {
    schema_version: u16,
    surface: McpSurface,
    execution_authority: ExecutionAuthority,
    maximum_data_quality: DataQuality,
}

impl DiagnosticServerContract {
    const fn new() -> Self {
        Self {
            schema_version: DIAGNOSTIC_CONTRACT_SCHEMA_VERSION,
            surface: McpSurface::DiagnosticCompatibility,
            execution_authority: ExecutionAuthority::None,
            maximum_data_quality: DataQuality::DirectUnverified,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DiagnosticToolContract {
    schema_version: u16,
    surface: McpSurface,
    coverage: DiagnosticCoverage,
    maximum_data_quality: Option<DataQuality>,
    execution_authority: ExecutionAuthority,
    simulation_access: SimulationAccess,
    control_authority: ControlAuthority,
    resource_scope: ResourceScope,
}

impl DiagnosticToolContract {
    const fn new(
        coverage: DiagnosticCoverage,
        maximum_data_quality: Option<DataQuality>,
        simulation_access: SimulationAccess,
        control_authority: ControlAuthority,
        resource_scope: ResourceScope,
    ) -> Self {
        Self {
            schema_version: DIAGNOSTIC_CONTRACT_SCHEMA_VERSION,
            surface: McpSurface::DiagnosticCompatibility,
            coverage,
            maximum_data_quality,
            execution_authority: ExecutionAuthority::None,
            simulation_access,
            control_authority,
            resource_scope,
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
enum McpFrame<'a> {
    Frame(&'a [u8]),
    EndOfInput,
}

#[derive(Debug, Error)]
enum McpFramingError {
    #[error("MCP input read failed")]
    Io(#[source] std::io::Error),
    #[error("MCP request exceeds {maximum_bytes} bytes")]
    Oversized { maximum_bytes: usize },
    #[error("MCP input read was cancelled")]
    Cancelled,
    #[error("MCP framing limit cannot reserve a detection byte")]
    InvalidLimit,
}

impl From<std::io::Error> for McpFramingError {
    fn from(source: std::io::Error) -> Self {
        Self::Io(source)
    }
}

/// Incremental newline framing with one fixed request buffer and fixed reader scratch.
#[derive(Debug)]
struct BoundedMcpReader<R> {
    reader: BufReader<R>,
    frame: Box<[u8]>,
    frame_len: usize,
    maximum_bytes: usize,
}

impl<R> BoundedMcpReader<R>
where
    R: AsyncRead + Unpin,
{
    fn new(
        reader: R,
        maximum_bytes: NonZeroUsize,
        scratch_bytes: NonZeroUsize,
    ) -> std::result::Result<Self, McpFramingError> {
        let frame_bytes = maximum_bytes
            .get()
            .checked_add(1)
            .ok_or(McpFramingError::InvalidLimit)?;
        Ok(Self {
            reader: BufReader::with_capacity(scratch_bytes.get(), reader),
            frame: vec![0; frame_bytes].into_boxed_slice(),
            frame_len: 0,
            maximum_bytes: maximum_bytes.get(),
        })
    }

    async fn next_frame<'a>(
        &'a mut self,
        cancellation: &CancellationToken,
    ) -> std::result::Result<McpFrame<'a>, McpFramingError> {
        loop {
            if cancellation.is_cancelled() {
                return Err(McpFramingError::Cancelled);
            }

            let available = tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err(McpFramingError::Cancelled),
                available = self.reader.fill_buf() => available?,
            };
            if available.is_empty() {
                if self.frame_len == 0 {
                    return Ok(McpFrame::EndOfInput);
                }
                if self.frame_len > self.maximum_bytes {
                    return Err(McpFramingError::Oversized {
                        maximum_bytes: self.maximum_bytes,
                    });
                }
                let length = self.frame_len;
                self.frame_len = 0;
                return Ok(McpFrame::Frame(&self.frame[..length]));
            }

            let newline = available.iter().position(|byte| *byte == b'\n');
            let bytes_before_newline = newline.unwrap_or(available.len());
            let remaining = self.frame.len().saturating_sub(self.frame_len);
            let copy_bytes = bytes_before_newline.min(remaining);
            let end = self.frame_len.saturating_add(copy_bytes);
            self.frame[self.frame_len..end].copy_from_slice(&available[..copy_bytes]);
            self.frame_len = end;
            let overflowed = bytes_before_newline > copy_bytes;
            let consumed =
                newline.map_or(bytes_before_newline, |position| position.saturating_add(1));
            self.reader.consume(consumed);

            if overflowed {
                return Err(McpFramingError::Oversized {
                    maximum_bytes: self.maximum_bytes,
                });
            }
            if newline.is_some() {
                if self.frame_len > self.maximum_bytes {
                    if self.frame_len == self.maximum_bytes.saturating_add(1)
                        && self.frame.get(self.frame_len.saturating_sub(1)) == Some(&b'\r')
                    {
                        self.frame_len = self.frame_len.saturating_sub(1);
                    } else {
                        return Err(McpFramingError::Oversized {
                            maximum_bytes: self.maximum_bytes,
                        });
                    }
                } else if self.frame.get(self.frame_len.saturating_sub(1)) == Some(&b'\r') {
                    self.frame_len = self.frame_len.saturating_sub(1);
                }
                let length = self.frame_len;
                self.frame_len = 0;
                return Ok(McpFrame::Frame(&self.frame[..length]));
            }

            if self.frame_len > self.maximum_bytes
                && self.frame.get(self.frame_len.saturating_sub(1)) != Some(&b'\r')
            {
                return Err(McpFramingError::Oversized {
                    maximum_bytes: self.maximum_bytes,
                });
            }
        }
    }

    #[cfg(test)]
    fn frame_storage_bytes(&self) -> usize {
        self.frame.len()
    }
}

#[derive(Debug)]
struct ToolRateLimiter {
    calls: VecDeque<Instant>,
    limit: usize,
    window: Duration,
}

impl ToolRateLimiter {
    fn new(limit: usize, window: Duration) -> Self {
        Self {
            calls: VecDeque::with_capacity(limit),
            limit,
            window,
        }
    }

    fn allow(&mut self, now: Instant) -> bool {
        while self
            .calls
            .front()
            .is_some_and(|oldest| now.duration_since(*oldest) >= self.window)
        {
            self.calls.pop_front();
        }

        if self.calls.len() >= self.limit {
            return false;
        }
        self.calls.push_back(now);
        true
    }
}

#[derive(Clone, Debug)]
pub struct McpServer {
    diagnostic_engine: SharedDiagnosticEngine,
    journal_path: PathBuf,
    tool_rate_limiter: Arc<Mutex<ToolRateLimiter>>,
}

impl McpServer {
    #[must_use]
    pub fn new(diagnostic_engine: SharedDiagnosticEngine, journal_path: PathBuf) -> Self {
        Self {
            diagnostic_engine,
            journal_path,
            tool_rate_limiter: Arc::new(Mutex::new(ToolRateLimiter::new(
                MAX_TOOL_CALLS_PER_SECOND,
                Duration::from_secs(1),
            ))),
        }
    }

    pub async fn serve_stdio(self) -> Result<()> {
        let stdin = tokio::io::stdin();
        let stdout = tokio::io::stdout();
        self.serve_io(
            stdin,
            tokio::io::BufWriter::new(stdout),
            NonZeroUsize::new(MAX_MCP_LINE_BYTES)
                .ok_or_else(|| anyhow::anyhow!("MCP request limit must be nonzero"))?,
            NonZeroUsize::new(MCP_READER_SCRATCH_BYTES)
                .ok_or_else(|| anyhow::anyhow!("MCP reader scratch must be nonzero"))?,
            CancellationToken::new(),
        )
        .await
    }

    async fn serve_io<R, W>(
        self,
        reader: R,
        mut writer: W,
        maximum_bytes: NonZeroUsize,
        scratch_bytes: NonZeroUsize,
        cancellation: CancellationToken,
    ) -> Result<()>
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
    {
        let mut frames = BoundedMcpReader::new(reader, maximum_bytes, scratch_bytes)?;

        loop {
            let line = match frames.next_frame(&cancellation).await {
                Ok(McpFrame::Frame(line)) => line,
                Ok(McpFrame::EndOfInput) | Err(McpFramingError::Cancelled) => break,
                Err(McpFramingError::Oversized { maximum_bytes }) => {
                    write_message(
                        &mut writer,
                        &json_rpc_error(
                            Value::Null,
                            -32600,
                            format!("request exceeds {maximum_bytes} bytes"),
                        ),
                    )
                    .await?;
                    break;
                }
                Err(error) => return Err(error.into()),
            };
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }

            let request: Value = match serde_json::from_slice(line) {
                Ok(request) => request,
                Err(error) => {
                    write_message(
                        &mut writer,
                        &json_rpc_error(Value::Null, -32700, format!("parse error: {error}")),
                    )
                    .await?;
                    continue;
                }
            };

            if let Some(response) = self.handle_request(&request) {
                write_message(&mut writer, &response).await?;
            }
        }
        Ok(())
    }

    fn handle_request(&self, request: &Value) -> Option<Value> {
        let id = request.get("id").cloned();
        let is_notification = id.is_none();

        if request.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
            return (!is_notification).then(|| {
                json_rpc_error(
                    id.unwrap_or(Value::Null),
                    -32600,
                    "invalid request".to_owned(),
                )
            });
        }

        let method = match request.get("method").and_then(Value::as_str) {
            Some(method) => method,
            None => {
                return (!is_notification).then(|| {
                    json_rpc_error(
                        id.unwrap_or(Value::Null),
                        -32600,
                        "missing method".to_owned(),
                    )
                });
            }
        };

        if is_notification {
            return None;
        }
        let id = id.unwrap_or(Value::Null);

        let result = match method {
            "initialize" => Ok(json!({
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {
                    "tools": { "listChanged": false }
                },
                "serverInfo": {
                    "name": "market-squawk",
                    "version": env!("CARGO_PKG_VERSION")
                },
                "_meta": {
                    "org.market-squawk/server-contract": DiagnosticServerContract::new()
                },
                "instructions": "Local compatibility data is diagnostic and authority-free. Bot behavior is paper simulation only, with no production order authority; risk controls fail closed."
            })),
            "ping" => Ok(json!({})),
            "tools/list" => Ok(json!({ "tools": tool_definitions() })),
            "tools/call" => {
                if !self.tool_rate_limiter.lock().allow(Instant::now()) {
                    Err((-32000, "tool call rate limit exceeded".to_owned()))
                } else {
                    self.call_tool(request.get("params").unwrap_or(&Value::Null))
                }
            }
            _ => Err((-32601, format!("method not found: {method}"))),
        };

        Some(match result {
            Ok(value) => json!({ "jsonrpc": "2.0", "id": id, "result": value }),
            Err((code, message)) => json_rpc_error(id, code, message),
        })
    }

    fn call_tool(&self, params: &Value) -> std::result::Result<Value, (i64, String)> {
        let params = params
            .as_object()
            .ok_or_else(|| (-32602, "tool call params must be an object".to_owned()))?;
        reject_unknown_keys(params, &["name", "arguments", "_meta"])?;

        let name = params
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| (-32602, "tool call missing name".to_owned()))?;
        let arguments = argument_map(params.get("arguments"))?;

        let value = match name {
            "Market.GetSnapshot" => {
                reject_unknown_keys(&arguments, &["product"])?;
                let product = optional_bounded_string(&arguments, "product", 128)?;
                let snapshot = self.diagnostic_engine.read().snapshot();
                if let Some(product) = product {
                    let product_snapshot = snapshot.products.get(product).ok_or_else(|| {
                        (
                            -32602,
                            format!("unknown or not-yet-observed product: {product}"),
                        )
                    })?;
                    serde_json::to_value(product_snapshot)
                        .map_err(|error| (-32603, error.to_string()))?
                } else {
                    serde_json::to_value(snapshot).map_err(|error| (-32603, error.to_string()))?
                }
            }
            "Market.GetQuality" => {
                reject_unknown_keys(&arguments, &[])?;
                let snapshot = self.diagnostic_engine.read().snapshot();
                let quality: Map<String, Value> = snapshot
                    .products
                    .into_iter()
                    .map(|(product, state)| {
                        (
                            product,
                            serde_json::to_value(state.quality).unwrap_or(Value::Null),
                        )
                    })
                    .collect();
                Value::Object(quality)
            }
            "Bot.GetStatus" => {
                reject_unknown_keys(&arguments, &[])?;
                let snapshot = self.diagnostic_engine.read().snapshot();
                json!({
                    "mode": "paper_only",
                    "account": snapshot.paper_account,
                    "risk": snapshot.risk
                })
            }
            "Journal.GetSummary" => {
                reject_unknown_keys(&arguments, &[])?;
                serde_json::to_value(
                    summarize_journal(&self.journal_path)
                        .map_err(|error| (-32603, format!("journal summary failed: {error:#}")))?,
                )
                .map_err(|error| (-32603, error.to_string()))?
            }
            "Risk.TriggerKillSwitch" => {
                reject_unknown_keys(&arguments, &["confirm", "reason"])?;
                let confirmed = arguments
                    .get("confirm")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let reason = required_bounded_string(&arguments, "reason", 500)?;
                if !confirmed {
                    return Err((-32602, "confirm=true is required".to_owned()));
                }
                self.diagnostic_engine.write().trigger_kill_switch();
                json!({ "triggered": true, "reason": reason })
            }
            _ => return Err((-32602, format!("unknown tool: {name}"))),
        };

        let text =
            serde_json::to_string_pretty(&value).map_err(|error| (-32603, error.to_string()))?;
        Ok(json!({
            "content": [{ "type": "text", "text": text }],
            "structuredContent": value,
            "isError": false
        }))
    }
}

fn argument_map(value: Option<&Value>) -> std::result::Result<Map<String, Value>, (i64, String)> {
    match value {
        None | Some(Value::Null) => Ok(Map::new()),
        Some(Value::Object(arguments)) => Ok(arguments.clone()),
        Some(_) => Err((-32602, "tool arguments must be an object".to_owned())),
    }
}

fn reject_unknown_keys(
    values: &Map<String, Value>,
    allowed: &[&str],
) -> std::result::Result<(), (i64, String)> {
    if let Some(key) = values.keys().find(|key| !allowed.contains(&key.as_str())) {
        return Err((-32602, format!("unknown argument: {key}")));
    }
    Ok(())
}

fn optional_bounded_string<'a>(
    values: &'a Map<String, Value>,
    key: &str,
    max_len: usize,
) -> std::result::Result<Option<&'a str>, (i64, String)> {
    let value = match values.get(key) {
        Some(value) => value,
        None => return Ok(None),
    };
    let value = value
        .as_str()
        .ok_or_else(|| (-32602, format!("{key} must be a string")))?
        .trim();
    if value.is_empty() || value.len() > max_len {
        return Err((
            -32602,
            format!("{key} must contain between 1 and {max_len} bytes"),
        ));
    }
    Ok(Some(value))
}

fn required_bounded_string<'a>(
    values: &'a Map<String, Value>,
    key: &str,
    max_len: usize,
) -> std::result::Result<&'a str, (i64, String)> {
    optional_bounded_string(values, key, max_len)?
        .ok_or_else(|| (-32602, format!("missing required argument: {key}")))
}

async fn write_message<W>(writer: &mut W, message: &Value) -> Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let encoded = serde_json::to_vec(message).context("failed to encode MCP response")?;
    writer.write_all(&encoded).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    Ok(())
}

fn json_rpc_error(id: Value, code: i64, message: String) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message }
    })
}

fn tool_definitions() -> Vec<Value> {
    vec![
        json!({
            "name": "Market.GetSnapshot",
            "title": "Diagnostic Market Snapshot",
            "description": "Get the latest diagnostic and authority-free compatibility snapshot from Coinbase Exchange single-venue, partial coverage. Omit product to return all observed products. Diagnostic values cannot mint production live authority.",
            "inputSchema": {
                "type": "object",
                "properties": { "product": { "type": "string", "minLength": 1, "maxLength": 128 } },
                "additionalProperties": false
            },
            "annotations": { "readOnlyHint": true, "destructiveHint": false },
            "execution": { "taskSupport": "forbidden" },
            "_meta": diagnostic_tool_contract(
                DiagnosticCoverage::CoinbaseExchangeSingleVenuePartial,
                Some(DataQuality::DirectUnverified),
                SimulationAccess::None,
                ControlAuthority::None,
                ResourceScope::CoinbaseDiagnosticMarketState,
            )
        }),
        json!({
            "name": "Market.GetQuality",
            "title": "Diagnostic Feed State",
            "description": "Get the app-local diagnostic `QualityState`, book and heartbeat timestamps, sequence information, and gap counters; diagnostic `VALID` is not canonical `DataQuality` and can never establish `DataQuality::DirectVerified`. This state cannot mint production live authority.",
            "inputSchema": { "type": "object", "additionalProperties": false },
            "annotations": { "readOnlyHint": true, "destructiveHint": false },
            "execution": { "taskSupport": "forbidden" },
            "_meta": diagnostic_tool_contract(
                DiagnosticCoverage::CoinbaseExchangeSingleVenuePartial,
                Some(DataQuality::DirectUnverified),
                SimulationAccess::None,
                ControlAuthority::None,
                ResourceScope::CoinbaseDiagnosticMarketState,
            )
        }),
        json!({
            "name": "Bot.GetStatus",
            "title": "Diagnostic Paper Simulation Status",
            "description": "Get diagnostic positions, fills, cash flow, and current risk state. This is paper simulation only, with no production order authority; this server never submits live orders.",
            "inputSchema": { "type": "object", "additionalProperties": false },
            "annotations": { "readOnlyHint": true, "destructiveHint": false },
            "execution": { "taskSupport": "forbidden" },
            "_meta": diagnostic_tool_contract(
                DiagnosticCoverage::LocalDiagnosticState,
                None,
                SimulationAccess::ReadOnly,
                ControlAuthority::None,
                ResourceScope::CurrentPaperSimulationRun,
            )
        }),
        json!({
            "name": "Journal.GetSummary",
            "title": "Journal Integrity Summary",
            "description": "Validate and summarize the configured immutable local raw-data journal without accepting arbitrary filesystem paths.",
            "inputSchema": { "type": "object", "additionalProperties": false },
            "annotations": { "readOnlyHint": true, "destructiveHint": false },
            "execution": { "taskSupport": "forbidden" },
            "_meta": diagnostic_tool_contract(
                DiagnosticCoverage::ConfiguredLocalJournal,
                None,
                SimulationAccess::None,
                ControlAuthority::None,
                ResourceScope::ConfiguredLocalJournal,
            )
        }),
        json!({
            "name": "Risk.TriggerKillSwitch",
            "title": "Trigger Diagnostic Simulation Kill Switch",
            "description": "Irreversibly stop the compatibility paper simulation only for the current run. It has no production order authority and cannot control production execution. Requires explicit confirmation and a reason.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "confirm": { "type": "boolean", "const": true },
                    "reason": { "type": "string", "minLength": 1, "maxLength": 500 }
                },
                "required": ["confirm", "reason"],
                "additionalProperties": false
            },
            "annotations": { "readOnlyHint": false, "destructiveHint": true },
            "execution": { "taskSupport": "forbidden" },
            "_meta": diagnostic_tool_contract(
                DiagnosticCoverage::CurrentLocalRun,
                None,
                SimulationAccess::None,
                ControlAuthority::PaperSimulationStopOnly,
                ResourceScope::CurrentPaperSimulationRun,
            )
        }),
    ]
}

fn diagnostic_tool_contract(
    coverage: DiagnosticCoverage,
    maximum_data_quality: Option<DataQuality>,
    simulation_access: SimulationAccess,
    control_authority: ControlAuthority,
    resource_scope: ResourceScope,
) -> Value {
    json!({
        "org.market-squawk/tool-contract": DiagnosticToolContract::new(
            coverage,
            maximum_data_quality,
            simulation_access,
            control_authority,
            resource_scope,
        )
    })
}

#[cfg(test)]
#[path = "mcp/framing_tests.rs"]
mod framing_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diagnostic_engine::DiagnosticEngine;
    use parking_lot::RwLock;

    #[test]
    fn initialize_exposes_a_structured_authority_ceiling() -> Result<(), &'static str> {
        let server = McpServer::new(
            Arc::new(RwLock::new(DiagnosticEngine::new(5_000, false))),
            PathBuf::from("unused.msj"),
        );
        let response = server
            .handle_request(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {}
            }))
            .ok_or("request should produce a response")?;
        assert_eq!(response["result"]["serverInfo"]["name"], "market-squawk");
        assert_eq!(response["result"]["protocolVersion"], PROTOCOL_VERSION);
        assert_eq!(
            response["result"]["_meta"]["org.market-squawk/server-contract"],
            json!({
                "schemaVersion": 1,
                "surface": "diagnostic_compatibility",
                "executionAuthority": "none",
                "maximumDataQuality": "direct_unverified"
            })
        );
        Ok(())
    }

    #[test]
    fn tool_contracts_structurally_bound_coverage_quality_and_authority() -> Result<(), &'static str>
    {
        let definitions = tool_definitions();
        let definition = |name: &str| {
            definitions
                .iter()
                .find(|definition| definition["name"] == name)
                .ok_or("required tool definition is missing")
        };
        let contract = |name: &str| {
            definition(name)?["_meta"]["org.market-squawk/tool-contract"]
                .as_object()
                .ok_or("tool contract metadata must be an object")
        };

        let expected = [
            (
                "Market.GetSnapshot",
                "coinbase_exchange_single_venue_partial",
                Value::String("direct_unverified".to_owned()),
                "none",
                "none",
                "coinbase_diagnostic_market_state",
            ),
            (
                "Market.GetQuality",
                "coinbase_exchange_single_venue_partial",
                Value::String("direct_unverified".to_owned()),
                "none",
                "none",
                "coinbase_diagnostic_market_state",
            ),
            (
                "Bot.GetStatus",
                "local_diagnostic_state",
                Value::Null,
                "read_only",
                "none",
                "current_paper_simulation_run",
            ),
            (
                "Journal.GetSummary",
                "configured_local_journal",
                Value::Null,
                "none",
                "none",
                "configured_local_journal",
            ),
            (
                "Risk.TriggerKillSwitch",
                "current_local_run",
                Value::Null,
                "none",
                "paper_simulation_stop_only",
                "current_paper_simulation_run",
            ),
        ];

        for (
            name,
            coverage,
            maximum_data_quality,
            simulation_access,
            control_authority,
            resource_scope,
        ) in expected
        {
            let tool_contract = contract(name)?;
            assert_eq!(tool_contract["schemaVersion"], 1);
            assert_eq!(tool_contract["surface"], "diagnostic_compatibility");
            assert_eq!(tool_contract["coverage"], coverage);
            assert_eq!(tool_contract["maximumDataQuality"], maximum_data_quality);
            assert_eq!(tool_contract["executionAuthority"], "none");
            assert_eq!(tool_contract["simulationAccess"], simulation_access);
            assert_eq!(tool_contract["controlAuthority"], control_authority);
            assert_eq!(tool_contract["resourceScope"], resource_scope);
        }
        Ok(())
    }

    #[test]
    fn tool_arguments_reject_unknown_fields() -> Result<(), &'static str> {
        let server = McpServer::new(
            Arc::new(RwLock::new(DiagnosticEngine::new(5_000, false))),
            PathBuf::from("unused.msj"),
        );
        let response = server
            .handle_request(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "Bot.GetStatus",
                    "arguments": { "unexpected": true }
                }
            }))
            .ok_or("request should produce a response")?;
        assert_eq!(response["error"]["code"], -32602);
        Ok(())
    }

    #[test]
    fn rate_limiter_rejects_bursts_beyond_the_limit() {
        let now = Instant::now();
        let mut limiter = ToolRateLimiter::new(1, Duration::from_secs(1));
        assert!(limiter.allow(now));
        assert!(!limiter.allow(now));
        assert!(limiter.allow(now + Duration::from_secs(1)));
    }
}
