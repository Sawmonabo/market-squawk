//! Installed-service stdio MCP release demonstration.

use std::{
    collections::BTreeMap,
    ffi::OsString,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context as _, Result, bail};
use market_squawk_mcp::{McpLimitSpec, McpLimits, McpStdioRelay};
use market_squawk_platform::{
    AppConfig, ConfigOverrides, ConfigSources, EncryptedFileSecretStore, SecretStore, SecretValue,
};
use market_squawk_runtime::{ApplicationClient, NamedClient};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncBufRead, AsyncBufReadExt as _, AsyncWrite, AsyncWriteExt as _, BufReader};
use tokio_util::sync::CancellationToken;

use crate::release::io::ordered_strings_sha256;
use crate::{
    LocalProduct,
    service::{InstalledService, InstalledServiceConnector, InstalledServiceRunOutcome},
};

const RELEASE_SERVICE_UNLOCK: &str = "market-squawk-release-installed-service";
const RELEASE_SERVICE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(90);

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

pub(super) async fn run(_product: &LocalProduct, expected_names: &[String]) -> Result<McpEvidence> {
    let temporary = tempfile::tempdir().context("create installed MCP release root")?;
    let workspace_root = temporary.path().join("workspace");
    let installation_root = temporary.path().join(".market-squawk-installed-service");
    let environment = BTreeMap::<OsString, OsString>::new();
    let config = AppConfig::load(ConfigSources::new(
        None,
        &environment,
        ConfigOverrides {
            data_dir: Some(workspace_root.clone()),
            source_shutdown_ms: Some(60_000),
            ..ConfigOverrides::default()
        },
    ))
    .context("load installed MCP release configuration")?;
    let secrets: Arc<dyn SecretStore> = Arc::new(
        EncryptedFileSecretStore::try_open(
            temporary.path().join("runtime-secrets"),
            SecretValue::new(RELEASE_SERVICE_UNLOCK.to_owned())
                .context("construct installed MCP release unlock")?,
        )
        .context("open installed MCP release secret store")?,
    );
    let connector =
        InstalledServiceConnector::try_new_at_installation_root(&config, &installation_root)
            .context("construct installed MCP release connector")?;
    let service = InstalledService::start_with_secret_store(config, secrets)
        .await
        .context("start installed MCP release service")?;
    let shutdown = CancellationToken::new();
    let mut service_task = tokio::spawn(service.run(shutdown.clone()));
    let session = exercise_installed_session(
        &connector,
        &installation_root,
        &workspace_root,
        expected_names,
    )
    .await;
    shutdown.cancel();
    let service_outcome =
        match tokio::time::timeout(RELEASE_SERVICE_SHUTDOWN_TIMEOUT, &mut service_task).await {
            Ok(result) => result
                .context("join installed MCP release service")?
                .context("stop installed MCP release service")?,
            Err(_elapsed) => {
                service_task.abort();
                let _aborted = service_task.await;
                bail!("installed MCP release service exceeded its shutdown deadline");
            }
        };
    if service_outcome != InstalledServiceRunOutcome::Stopped {
        bail!("installed MCP release service requested an unexpected restart");
    }
    let (names, read_call_completed, audit) = session?;
    let durable_audit_written = regular_nonempty_file(&audit)?;
    if !durable_audit_written {
        bail!("shipping MCP durable audit was not written");
    }
    Ok(McpEvidence {
        protocol_version: "2026-07-28".to_owned(),
        tool_count: names.len(),
        tool_names_sha256: ordered_strings_sha256(&names)?,
        descriptor_parity: true,
        read_call_completed,
        durable_audit_written,
        shutdown_complete: true,
    })
}

async fn exercise_installed_session(
    connector: &InstalledServiceConnector,
    installation_root: &Path,
    legacy_workspace_root: &Path,
    expected_names: &[String],
) -> Result<(Vec<String>, bool, PathBuf)> {
    let desktop = connector
        .connect(NamedClient::Desktop, Some("tauri://localhost".to_owned()))
        .context("admit installed MCP release native client")?;
    let bootstrap = desktop
        .bootstrap(CancellationToken::new())
        .await
        .context("read installed MCP release bootstrap")?;
    if bootstrap
        .pointer("/readiness/service")
        .and_then(Value::as_bool)
        != Some(true)
    {
        bail!("installed MCP release native client was not ready");
    }
    let workspace_id = bootstrap
        .pointer("/workspace/id")
        .and_then(Value::as_str)
        .context("installed MCP release bootstrap omitted its workspace identity")?;
    let workspace_root = match bootstrap
        .pointer("/workspace/placement")
        .and_then(Value::as_str)
    {
        Some("managed") => installation_root.join("workspaces").join(workspace_id),
        Some("legacy_migration_required") => legacy_workspace_root.to_path_buf(),
        _ => bail!("installed MCP release bootstrap returned an invalid workspace placement"),
    };
    let transport = connector
        .connect_mcp_relay(NamedClient::Codex)
        .context("admit installed MCP release relay")?;
    let relay = McpStdioRelay::try_new(
        NamedClient::Codex,
        transport,
        McpLimits::try_from(McpLimitSpec::default())
            .context("construct installed MCP release limits")?,
    )
    .context("construct installed MCP release relay")?;
    let (client, relay_io) = tokio::io::duplex(64 * 1024);
    let (relay_reader, relay_writer) = tokio::io::split(relay_io);
    let task = tokio::spawn(relay.serve_unverified_io(
        relay_reader,
        relay_writer,
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
                "protocolVersion": "2026-07-28",
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
        != Some("2026-07-28")
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
        bail!("shipping MCP tool inventory differs from the installed application descriptor");
    }
    write_message(
        &mut client_writer,
        json!({
            "jsonrpc": "2.0",
            "id": "release-jobs",
            "method": "tools/call",
            "params": {
                "name": "Job.List",
                "arguments": {"limit": 16}
            }
        }),
    )
    .await?;
    let jobs = read_message(&mut client_reader).await?;
    let read_call_completed = jobs
        .pointer("/result/structuredContent/data/jobs")
        .and_then(Value::as_array)
        .is_some();
    if !read_call_completed {
        bail!("shipping MCP did not dispatch the installed Job.List authority");
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
    Ok((
        names,
        read_call_completed,
        workspace_root.join("control").join("mcp-audit.jsonl"),
    ))
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
