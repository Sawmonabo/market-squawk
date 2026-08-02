//! Real bounded MCP verification through one installed named-client relay.

use std::{
    collections::BTreeMap,
    io::{BufRead as _, BufReader, BufWriter, Write},
    path::Path,
    process::{Child, Command, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{McpClientKind, McpClientRegistrationError, read_bounded};

/// Exact stable protocol admitted by the shared installed service and stateless relay.
pub use market_squawk_mcp::MCP_PROTOCOL_VERSION;
const VERIFICATION_TIMEOUT: Duration = Duration::from_secs(15);
const MAXIMUM_FRAME_BYTES: usize = 1024 * 1024;
const SAFE_READ_TOOL: &str = "Job.List";

/// Secret-free proof of one complete initialized MCP discovery and safe-read exchange.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct McpProtocolVerification {
    client: McpClientKind,
    protocol_version: String,
    client_info_name: String,
    server_name: String,
    tool_count: usize,
    resource_count: usize,
    safe_read_tool: String,
    verified_at_unix_seconds: u64,
}

impl McpProtocolVerification {
    #[must_use]
    pub const fn client(&self) -> McpClientKind {
        self.client
    }

    #[must_use]
    pub fn protocol_version(&self) -> &str {
        &self.protocol_version
    }

    #[must_use]
    pub fn client_info_name(&self) -> &str {
        &self.client_info_name
    }

    #[must_use]
    pub fn server_name(&self) -> &str {
        &self.server_name
    }

    #[must_use]
    pub const fn tool_count(&self) -> usize {
        self.tool_count
    }

    #[must_use]
    pub const fn resource_count(&self) -> usize {
        self.resource_count
    }

    #[must_use]
    pub fn safe_read_tool(&self) -> &str {
        &self.safe_read_tool
    }

    #[must_use]
    pub const fn verified_at_unix_seconds(&self) -> u64 {
        self.verified_at_unix_seconds
    }

    pub(super) fn is_valid(&self) -> bool {
        let expected_client_info = match self.client {
            McpClientKind::ClaudeCode => "market-squawk-verifier-claude-code",
            McpClientKind::Codex => "market-squawk-verifier-codex",
        };
        self.protocol_version == MCP_PROTOCOL_VERSION
            && self.client_info_name == expected_client_info
            && self.server_name == "market-squawk"
            && self.tool_count > 0
            && self.resource_count > 0
            && self.safe_read_tool == SAFE_READ_TOOL
    }
}

pub(super) fn verify(
    relay_program: &Path,
    client: McpClientKind,
) -> Result<McpProtocolVerification, McpClientRegistrationError> {
    let mut child = ChildGuard::new(
        Command::new(relay_program)
            .arg("--client")
            .arg(match client {
                McpClientKind::ClaudeCode => "claude",
                McpClientKind::Codex => "codex",
            })
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|_error| McpClientRegistrationError::Protocol)?,
    );
    let stdin = child
        .stdin
        .take()
        .ok_or(McpClientRegistrationError::Protocol)?;
    let stdout = child
        .stdout
        .take()
        .ok_or(McpClientRegistrationError::Protocol)?;
    let stderr = child
        .stderr
        .take()
        .ok_or(McpClientRegistrationError::Protocol)?;
    let (sender, receiver) = mpsc::sync_channel(8);
    let stdout_reader = thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        loop {
            let mut frame = Vec::new();
            match reader.read_until(b'\n', &mut frame) {
                Ok(0) => break,
                Ok(_) if frame.len() <= MAXIMUM_FRAME_BYTES => {
                    if sender.send(Ok(frame)).is_err() {
                        break;
                    }
                }
                Ok(_) | Err(_) => {
                    let _ = sender.send(Err(()));
                    break;
                }
            }
        }
    });
    let stderr_reader = thread::spawn(move || read_bounded(stderr));
    let mut writer = BufWriter::new(stdin);
    let deadline = Instant::now()
        .checked_add(VERIFICATION_TIMEOUT)
        .ok_or(McpClientRegistrationError::Protocol)?;
    let client_info_name = match client {
        McpClientKind::ClaudeCode => "market-squawk-verifier-claude-code",
        McpClientKind::Codex => "market-squawk-verifier-codex",
    }
    .to_owned();
    write_message(
        &mut writer,
        &json!({
            "jsonrpc": "2.0",
            "id": "verify-initialize",
            "method": "initialize",
            "params": {
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": client_info_name, "version": env!("CARGO_PKG_VERSION")}
            }
        }),
    )?;
    let initialized = receive_message(&receiver, deadline)?;
    if initialized
        .pointer("/result/protocolVersion")
        .and_then(Value::as_str)
        != Some(MCP_PROTOCOL_VERSION)
    {
        return Err(McpClientRegistrationError::Protocol);
    }
    let server_name = initialized
        .pointer("/result/serverInfo/name")
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty())
        .ok_or(McpClientRegistrationError::Protocol)?
        .to_owned();
    write_message(
        &mut writer,
        &json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
    )?;
    write_message(
        &mut writer,
        &json!({"jsonrpc":"2.0","id":"verify-tools","method":"tools/list"}),
    )?;
    write_message(
        &mut writer,
        &json!({"jsonrpc":"2.0","id":"verify-resources","method":"resources/list"}),
    )?;
    write_message(
        &mut writer,
        &json!({
            "jsonrpc":"2.0",
            "id":"verify-read",
            "method":"tools/call",
            "params":{"name":SAFE_READ_TOOL,"arguments":{"limit":1}}
        }),
    )?;
    let mut responses = BTreeMap::new();
    for _ in 0..3 {
        let response = receive_message(&receiver, deadline)?;
        let id = response
            .get("id")
            .and_then(Value::as_str)
            .ok_or(McpClientRegistrationError::Protocol)?
            .to_owned();
        if responses.insert(id, response).is_some() {
            return Err(McpClientRegistrationError::Protocol);
        }
    }
    let tools = responses
        .get("verify-tools")
        .and_then(|response| response.pointer("/result/tools"))
        .and_then(Value::as_array)
        .ok_or(McpClientRegistrationError::Protocol)?;
    let resources = responses
        .get("verify-resources")
        .and_then(|response| response.pointer("/result/resources"))
        .and_then(Value::as_array)
        .ok_or(McpClientRegistrationError::Protocol)?;
    let read = responses
        .get("verify-read")
        .ok_or(McpClientRegistrationError::Protocol)?;
    if tools.is_empty()
        || resources.is_empty()
        || read.pointer("/result/isError").and_then(Value::as_bool) == Some(true)
        || read.get("error").is_some()
    {
        return Err(McpClientRegistrationError::Protocol);
    }
    drop(writer);
    wait_for_exit(&mut child.child, deadline)?;
    child.disarm();
    stdout_reader
        .join()
        .map_err(|_| McpClientRegistrationError::Protocol)?;
    stderr_reader
        .join()
        .map_err(|_| McpClientRegistrationError::Protocol)?
        .map_err(|_error| McpClientRegistrationError::Protocol)?;
    let verified_at_unix_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_error| McpClientRegistrationError::Clock)?
        .as_secs();
    Ok(McpProtocolVerification {
        client,
        protocol_version: MCP_PROTOCOL_VERSION.to_owned(),
        client_info_name,
        server_name,
        tool_count: tools.len(),
        resource_count: resources.len(),
        safe_read_tool: SAFE_READ_TOOL.to_owned(),
        verified_at_unix_seconds,
    })
}

fn write_message(
    writer: &mut BufWriter<impl Write>,
    value: &Value,
) -> Result<(), McpClientRegistrationError> {
    serde_json::to_writer(&mut *writer, value)
        .map_err(|_error| McpClientRegistrationError::Protocol)?;
    writer
        .write_all(b"\n")
        .and_then(|()| writer.flush())
        .map_err(|_error| McpClientRegistrationError::Protocol)
}

fn receive_message(
    receiver: &mpsc::Receiver<Result<Vec<u8>, ()>>,
    deadline: Instant,
) -> Result<Value, McpClientRegistrationError> {
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or(McpClientRegistrationError::Protocol)?;
    let frame = receiver
        .recv_timeout(remaining)
        .map_err(|_error| McpClientRegistrationError::Protocol)?
        .map_err(|()| McpClientRegistrationError::Protocol)?;
    serde_json::from_slice(&frame).map_err(|_error| McpClientRegistrationError::Protocol)
}

fn wait_for_exit(child: &mut Child, deadline: Instant) -> Result<(), McpClientRegistrationError> {
    loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|_error| McpClientRegistrationError::Protocol)?
        {
            return status
                .success()
                .then_some(())
                .ok_or(McpClientRegistrationError::Protocol);
        }
        if Instant::now() >= deadline {
            terminate(child);
            return Err(McpClientRegistrationError::Protocol);
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn terminate(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

struct ChildGuard {
    child: Child,
    armed: bool,
}

impl ChildGuard {
    const fn new(child: Child) -> Self {
        Self { child, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl std::ops::Deref for ChildGuard {
    type Target = Child;

    fn deref(&self) -> &Self::Target {
        &self.child
    }
}

impl std::ops::DerefMut for ChildGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.child
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.armed {
            terminate(&mut self.child);
        }
    }
}
