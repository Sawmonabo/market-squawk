//! In-process stdio MCP release demonstration over the shipping composition.

use std::path::Path;
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncBufRead, AsyncBufReadExt as _, AsyncWrite, AsyncWriteExt as _, BufReader};
use tokio_util::sync::CancellationToken;

use crate::LocalProduct;
use crate::mcp::LocalMcpComposition;
use crate::release::io::ordered_strings_sha256;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct McpEvidence {
    pub(super) protocol_version: String,
    pub(super) tool_count: usize,
    pub(super) tool_names_sha256: [u8; 32],
    pub(super) descriptor_parity: bool,
    pub(super) read_call_completed: bool,
    pub(super) durable_audit_written: bool,
    pub(super) shutdown_complete: bool,
}

pub(super) async fn run(product: &LocalProduct, expected_names: &[String]) -> Result<McpEvidence> {
    let composition =
        LocalMcpComposition::try_new(product.paths(), product.application(), product.artifacts())
            .context("shipping MCP release composition failed")?;
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (server_reader, server_writer) = tokio::io::split(server);
    let task = tokio::spawn(composition.serve_unverified_io(
        server_reader,
        server_writer,
        CancellationToken::new(),
    ));
    let (client_reader, mut client_writer) = tokio::io::split(client);
    let mut client_reader = BufReader::new(client_reader);

    write_message(
        &mut client_writer,
        json!({
            "jsonrpc": "2.0",
            "id": "release-init",
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": {"name": "market-squawk-release", "version": "1"}
            }
        }),
    )
    .await?;
    let initialized = read_message(&mut client_reader).await?;
    if initialized
        .pointer("/result/protocolVersion")
        .and_then(Value::as_str)
        != Some("2025-11-25")
    {
        bail!("shipping MCP initialization returned an unexpected protocol version");
    }
    write_message(
        &mut client_writer,
        json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
    )
    .await?;
    write_message(
        &mut client_writer,
        json!({"jsonrpc": "2.0", "id": "release-tools", "method": "tools/list"}),
    )
    .await?;
    let tools = read_message(&mut client_reader).await?;
    let names = tools
        .pointer("/result/tools")
        .and_then(Value::as_array)
        .context("shipping MCP tools/list omitted its tool array")?
        .iter()
        .map(|tool| {
            tool.pointer("/name")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .context("shipping MCP tool omitted its name")
        })
        .collect::<Result<Vec<_>>>()?;
    let descriptor_parity = names == expected_names;
    if !descriptor_parity {
        bail!("shipping MCP tool inventory differs from the application descriptor");
    }
    write_message(
        &mut client_writer,
        json!({
            "jsonrpc": "2.0",
            "id": "release-status",
            "method": "tools/call",
            "params": {
                "name": "Bot.GetStatus",
                "arguments": {
                    "resultLimits": {"maximumItems": 16, "maximumBytes": 65536}
                }
            }
        }),
    )
    .await?;
    let status = read_message(&mut client_reader).await?;
    let read_call_completed = status
        .pointer("/result/structuredContent/data/state")
        .and_then(Value::as_str)
        == Some("stopped");
    if !read_call_completed {
        bail!("shipping MCP read call did not return stopped paper state");
    }

    client_writer
        .shutdown()
        .await
        .context("shipping MCP client shutdown failed")?;
    let _exit = tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .context("shipping MCP session exceeded its shutdown deadline")?
        .context("shipping MCP task failed")?
        .context("shipping MCP session failed")?;
    let audit = product
        .paths()
        .control_root()
        .context("shipping MCP audit root is unavailable")?
        .root()
        .join("mcp-audit.jsonl");
    let durable_audit_written = regular_nonempty_file(&audit)?;
    if !durable_audit_written {
        bail!("shipping MCP durable audit was not written");
    }
    Ok(McpEvidence {
        protocol_version: "2025-11-25".to_owned(),
        tool_count: names.len(),
        tool_names_sha256: ordered_strings_sha256(&names)?,
        descriptor_parity,
        read_call_completed,
        durable_audit_written,
        shutdown_complete: true,
    })
}

async fn write_message<W: AsyncWrite + Unpin>(writer: &mut W, value: Value) -> Result<()> {
    writer
        .write_all(&serde_json::to_vec(&value)?)
        .await
        .context("shipping MCP request write failed")?;
    writer
        .write_all(b"\n")
        .await
        .context("shipping MCP request delimiter write failed")?;
    writer
        .flush()
        .await
        .context("shipping MCP request flush failed")
}

async fn read_message<R: AsyncBufRead + Unpin>(reader: &mut R) -> Result<Value> {
    let mut line = String::new();
    let bytes = tokio::time::timeout(Duration::from_secs(10), reader.read_line(&mut line))
        .await
        .context("shipping MCP response exceeded its deadline")?
        .context("shipping MCP response read failed")?;
    if bytes == 0 || bytes > 8 * 1024 * 1024 {
        bail!("shipping MCP response length is invalid");
    }
    serde_json::from_str(&line).context("shipping MCP response is invalid JSON")
}

fn regular_nonempty_file(path: &Path) -> Result<bool> {
    let metadata =
        std::fs::symlink_metadata(path).context("shipping MCP audit metadata is unavailable")?;
    Ok(metadata.is_file() && !metadata.file_type().is_symlink() && metadata.len() > 0)
}
