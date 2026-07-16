use std::{
    collections::VecDeque,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use parking_lot::Mutex;
use serde_json::{Map, Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::{engine::SharedEngine, replay::summarize_journal};

const PROTOCOL_VERSION: &str = "2025-11-25";
const MAX_TOOL_CALLS_PER_SECOND: usize = 100;
const MAX_MCP_LINE_BYTES: usize = 1024 * 1024;

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

#[derive(Clone)]
pub struct McpServer {
    engine: SharedEngine,
    journal_path: PathBuf,
    tool_rate_limiter: Arc<Mutex<ToolRateLimiter>>,
}

impl McpServer {
    #[must_use]
    pub fn new(engine: SharedEngine, journal_path: PathBuf) -> Self {
        Self {
            engine,
            journal_path,
            tool_rate_limiter: Arc::new(Mutex::new(ToolRateLimiter::new(
                MAX_TOOL_CALLS_PER_SECOND,
                Duration::from_secs(1),
            ))),
        }
    }

    pub async fn serve_stdio(self) -> Result<()> {
        let stdin = tokio::io::stdin();
        let mut lines = BufReader::new(stdin).lines();
        let stdout = tokio::io::stdout();
        let mut stdout = tokio::io::BufWriter::new(stdout);

        while let Some(line) = lines.next_line().await? {
            if line.trim().is_empty() {
                continue;
            }
            if line.len() > MAX_MCP_LINE_BYTES {
                write_message(
                    &mut stdout,
                    &json_rpc_error(
                        Value::Null,
                        -32600,
                        format!("request exceeds {MAX_MCP_LINE_BYTES} bytes"),
                    ),
                )
                .await?;
                continue;
            }

            let request: Value = match serde_json::from_str(&line) {
                Ok(request) => request,
                Err(error) => {
                    write_message(
                        &mut stdout,
                        &json_rpc_error(Value::Null, -32700, format!("parse error: {error}")),
                    )
                    .await?;
                    continue;
                }
            };

            if let Some(response) = self.handle_request(&request) {
                write_message(&mut stdout, &response).await?;
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
                    "name": "market-engine",
                    "version": env!("CARGO_PKG_VERSION")
                },
                "instructions": "Local, read-rich market data server. Bot actions are paper-only; risk controls fail closed."
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
                let snapshot = self.engine.read().snapshot();
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
                let snapshot = self.engine.read().snapshot();
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
                let snapshot = self.engine.read().snapshot();
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
                self.engine.write().trigger_kill_switch();
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
            "title": "Latest Market Snapshot",
            "description": "Get the latest validated local market snapshot. Omit product to return all observed products.",
            "inputSchema": {
                "type": "object",
                "properties": { "product": { "type": "string", "minLength": 1, "maxLength": 128 } },
                "additionalProperties": false
            },
            "annotations": { "readOnlyHint": true, "destructiveHint": false },
            "execution": { "taskSupport": "forbidden" }
        }),
        json!({
            "name": "Market.GetQuality",
            "title": "Market Data Quality",
            "description": "Get feed-quality states, book and heartbeat timestamps, sequence information, and gap counters for every product.",
            "inputSchema": { "type": "object", "additionalProperties": false },
            "annotations": { "readOnlyHint": true, "destructiveHint": false },
            "execution": { "taskSupport": "forbidden" }
        }),
        json!({
            "name": "Bot.GetStatus",
            "title": "Paper Bot Status",
            "description": "Get paper-bot positions, fills, cash flow, and current risk state. This server never submits live orders.",
            "inputSchema": { "type": "object", "additionalProperties": false },
            "annotations": { "readOnlyHint": true, "destructiveHint": false },
            "execution": { "taskSupport": "forbidden" }
        }),
        json!({
            "name": "Journal.GetSummary",
            "title": "Journal Integrity Summary",
            "description": "Validate and summarize the configured immutable local raw-data journal without accepting arbitrary filesystem paths.",
            "inputSchema": { "type": "object", "additionalProperties": false },
            "annotations": { "readOnlyHint": true, "destructiveHint": false },
            "execution": { "taskSupport": "forbidden" }
        }),
        json!({
            "name": "Risk.TriggerKillSwitch",
            "title": "Trigger Paper Risk Kill Switch",
            "description": "Irreversibly activate the in-process risk kill switch for the current run. Requires explicit confirmation and a reason.",
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
            "execution": { "taskSupport": "forbidden" }
        }),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Engine;
    use parking_lot::RwLock;

    #[test]
    fn initialize_advertises_tools_without_live_execution() {
        let server = McpServer::new(
            Arc::new(RwLock::new(Engine::new(5_000, false))),
            PathBuf::from("unused.mej"),
        );
        let response = server
            .handle_request(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {}
            }))
            .expect("request should produce a response");
        assert_eq!(response["result"]["serverInfo"]["name"], "market-engine");
        assert_eq!(response["result"]["protocolVersion"], PROTOCOL_VERSION);
    }

    #[test]
    fn tool_arguments_reject_unknown_fields() {
        let server = McpServer::new(
            Arc::new(RwLock::new(Engine::new(5_000, false))),
            PathBuf::from("unused.mej"),
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
            .expect("request should produce a response");
        assert_eq!(response["error"]["code"], -32602);
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
