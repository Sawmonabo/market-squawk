use std::{
    collections::BTreeMap,
    ffi::OsString,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    time::{Duration, Instant},
};

use market_squawk::{
    LocalProduct,
    application::application_capabilities,
    mcp::LocalMcpComposition,
    service::{
        BootstrapRequirement, InstalledService, InstalledServiceBootstrapState,
        InstalledServiceConnector, InstalledServiceError, InstalledServiceRunOutcome,
    },
};
use market_squawk_mcp::{McpLimitSpec, McpLimits, McpStdioRelay, validate_service_capabilities};
use market_squawk_platform::{
    AppConfig, ConfigOverrides, ConfigSources, EncryptedFileSecretStore, PreferredSecretStore,
    SecretStore, SecretValue,
};
use market_squawk_runtime::{ApplicationClient, EventPageLimit, NamedClient};
use market_squawk_services::{ArtifactPublication, ArtifactPublicationContext, RequestId};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio_util::sync::CancellationToken;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const BOOTSTRAP_SEED_ROOT_ENV: &str = "MARKET_SQUAWK_TEST_BOOTSTRAP_SEED_ROOT";
const INSTALLED_SERVICE_TEST_UNLOCK: &str = "installed-service-test-unlock";

#[tokio::test]
async fn service_runtime_is_the_single_authority_for_native_and_mcp_clients() -> TestResult {
    if let Some(root) = std::env::var_os(BOOTSTRAP_SEED_ROOT_ENV) {
        return seed_encrypted_runtime(PathBuf::from(root)).await;
    }
    let temporary = tempfile::tempdir()?;
    let environment = BTreeMap::<OsString, OsString>::new();
    let config = AppConfig::load(ConfigSources::new(
        None,
        &environment,
        ConfigOverrides {
            data_dir: Some(temporary.path().join("product")),
            source_shutdown_ms: Some(60_000),
            ..ConfigOverrides::default()
        },
    ))?;
    let secret_root = temporary.path().join("runtime-secrets");
    let secrets: Arc<dyn SecretStore> = Arc::new(EncryptedFileSecretStore::try_open(
        &secret_root,
        SecretValue::new(INSTALLED_SERVICE_TEST_UNLOCK.to_owned())?,
    )?);
    let service =
        InstalledService::start_with_secret_store(config.clone(), Arc::clone(&secrets)).await?;
    assert!(matches!(
        InstalledService::start_with_secret_store(config.clone(), Arc::clone(&secrets)).await,
        Err(InstalledServiceError::AlreadyRunning)
    ));

    let connector =
        InstalledServiceConnector::try_new_with_secret_store(&config, Arc::clone(&secrets))?;
    let desktop = connector.connect(NamedClient::Desktop, Some("tauri://localhost".to_owned()))?;
    let cli = connector.connect(NamedClient::Cli, None)?;
    let bootstrap = desktop.bootstrap(CancellationToken::new()).await?;
    assert_eq!(bootstrap["readiness"]["service"], true);
    assert!(bootstrap["runtime"]["workspaceId"].is_string());
    let provider = bootstrap["sources"]["profiles"][0]["id"]
        .as_str()
        .ok_or("installed bootstrap did not expose a provider")?;
    let registration = desktop
        .invoke_operation(
            RequestId::try_string("installed-source-registration")?,
            "Source.Register",
            json!({
                "provider": provider,
                "confirm": true,
                "resultLimits": {"maximumItems": 16, "maximumBytes": 1_048_576},
            }),
            Duration::from_secs(5),
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(
        registration.result()["ok"],
        true,
        "{}",
        registration.result()
    );
    let (events, cursor) = desktop
        .read_events(None, EventPageLimit::try_new(4)?, CancellationToken::new())
        .await?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["type"], "application.changed");
    assert_eq!(events[0]["operation"], "Source.Register");
    assert_eq!(cursor.sequence(), 1);

    let jobs = cli
        .invoke_operation(
            RequestId::try_string("installed-job-list")?,
            "Job.List",
            json!({"limit": 16}),
            Duration::from_secs(5),
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(jobs.result()["ok"], true);
    assert_eq!(jobs.result()["value"]["data"]["jobs"], json!([]));

    let shutdown = CancellationToken::new();
    let service_task = tokio::spawn(service.run(shutdown.clone()));
    let claude = exercise_installed_relay(
        NamedClient::ClaudeCode,
        connector.connect_mcp_relay(NamedClient::ClaudeCode)?,
    );
    let codex = exercise_installed_relay(
        NamedClient::Codex,
        connector.connect_mcp_relay(NamedClient::Codex)?,
    );
    tokio::try_join!(claude, codex)?;

    cli.probe_ready(CancellationToken::new()).await?;
    shutdown.cancel();
    assert_eq!(service_task.await??, InstalledServiceRunOutcome::Stopped);
    assert!(connector.connect(NamedClient::Cli, None).is_err());
    drop(desktop);
    drop(cli);
    drop(connector);
    assert_eq!(Arc::strong_count(&secrets), 1);
    drop(secrets);

    let bootstrap_seed_root = temporary.path().join("bootstrap-seed");
    seed_encrypted_runtime_subprocess(&bootstrap_seed_root).await?;
    let bootstrap_config = AppConfig::load(ConfigSources::new(
        None,
        &environment,
        ConfigOverrides {
            data_dir: Some(bootstrap_seed_root.join("product")),
            source_shutdown_ms: Some(60_000),
            ..ConfigOverrides::default()
        },
    ))?;
    let bootstrap_secret_root = bootstrap_seed_root.join("runtime-secrets");
    let locked_secrets: Arc<dyn SecretStore> = Arc::new(
        PreferredSecretStore::try_new_with_locked_encrypted_file_fallback(
            "market-squawk-runtime-bootstrap-test",
            &bootstrap_secret_root,
        )?,
    );
    let locked_connector = InstalledServiceConnector::try_new_with_secret_store(
        &bootstrap_config,
        Arc::clone(&locked_secrets),
    )?;
    let restarting = tokio::spawn(InstalledService::start_with_secret_store(
        bootstrap_config,
        Arc::clone(&locked_secrets),
    ));
    let bootstrap = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match locked_connector.bootstrap_status().await {
                Ok(status) => break Ok::<_, InstalledServiceError>(status),
                Err(InstalledServiceError::ServiceUnavailable) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(error) => break Err(error),
            }
        }
    })
    .await??;
    assert_eq!(bootstrap.state(), InstalledServiceBootstrapState::Required);
    assert_eq!(
        bootstrap.requirement(),
        Some(BootstrapRequirement::EncryptedFallbackLocked)
    );
    assert!(locked_connector.connect(NamedClient::Cli, None).is_err());
    let installation = bootstrap.installation_id();

    let accepted = locked_connector
        .bootstrap_unlock(SecretValue::new(INSTALLED_SERVICE_TEST_UNLOCK.to_owned())?)
        .await?;
    assert_eq!(accepted.state(), InstalledServiceBootstrapState::Retrying);
    assert!(
        locked_connector
            .bootstrap_unlock(SecretValue::new(INSTALLED_SERVICE_TEST_UNLOCK.to_owned())?)
            .await
            .is_err()
    );

    let restarted = tokio::time::timeout(Duration::from_secs(30), restarting).await???;
    let restarted_cli = locked_connector.connect(NamedClient::Cli, None)?;
    let restarted_snapshot = restarted_cli.bootstrap(CancellationToken::new()).await?;
    assert_eq!(
        restarted_snapshot["runtime"]["installationId"],
        installation.as_uuid().to_string()
    );
    let restarted_shutdown = CancellationToken::new();
    let restarted_task = tokio::spawn(restarted.run(restarted_shutdown.clone()));
    restarted_cli.probe_ready(CancellationToken::new()).await?;
    restarted_shutdown.cancel();
    assert_eq!(restarted_task.await??, InstalledServiceRunOutcome::Stopped);
    Ok(())
}

async fn seed_encrypted_runtime(root: PathBuf) -> TestResult {
    let environment = BTreeMap::<OsString, OsString>::new();
    let config = AppConfig::load(ConfigSources::new(
        None,
        &environment,
        ConfigOverrides {
            data_dir: Some(root.join("product")),
            source_shutdown_ms: Some(60_000),
            ..ConfigOverrides::default()
        },
    ))?;
    let secrets: Arc<dyn SecretStore> = Arc::new(EncryptedFileSecretStore::try_open(
        root.join("runtime-secrets"),
        SecretValue::new(INSTALLED_SERVICE_TEST_UNLOCK.to_owned())?,
    )?);
    let service = InstalledService::start_with_secret_store(config, secrets).await?;
    let cancellation = CancellationToken::new();
    let task = tokio::spawn(service.run(cancellation.clone()));
    cancellation.cancel();
    assert_eq!(task.await??, InstalledServiceRunOutcome::Stopped);
    Ok(())
}

async fn seed_encrypted_runtime_subprocess(root: &Path) -> TestResult {
    let executable = std::env::current_exe()?;
    let root = root.to_path_buf();
    let output = tokio::task::spawn_blocking(move || {
        Command::new(executable)
            .args([
                "--exact",
                "production_mcp_composition::service_runtime_is_the_single_authority_for_native_and_mcp_clients",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(BOOTSTRAP_SEED_ROOT_ENV, root)
            .output()
    })
    .await??;
    if !output.status.success() {
        return Err(format!(
            "encrypted-runtime seed subprocess failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(())
}

async fn exercise_installed_relay(
    client: NamedClient,
    transport: Arc<dyn market_squawk_mcp::McpRelayTransport>,
) -> TestResult {
    let relay = McpStdioRelay::try_new(
        client,
        transport,
        McpLimits::try_from(McpLimitSpec::default())?,
    )?;
    let (peer, relay_io) = tokio::io::duplex(64 * 1024);
    let (relay_reader, relay_writer) = tokio::io::split(relay_io);
    let task = tokio::spawn(relay.serve_unverified_io(
        relay_reader,
        relay_writer,
        CancellationToken::new(),
    ));
    let (peer_reader, mut peer_writer) = tokio::io::split(peer);
    let mut peer_reader = BufReader::new(peer_reader);
    write_message(
        &mut peer_writer,
        json!({
            "jsonrpc":"2.0","id":"installed-init","method":"initialize",
            "params":{
                "protocolVersion":"2026-07-28","capabilities":{},
                "clientInfo":{"name":"market-squawk-tests","version":"1"}
            }
        }),
    )
    .await?;
    let initialized = read_message(&mut peer_reader).await?;
    assert_eq!(initialized["result"]["protocolVersion"], "2026-07-28");
    write_message(
        &mut peer_writer,
        json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
    )
    .await?;
    write_message(
        &mut peer_writer,
        json!({"jsonrpc":"2.0","id":"installed-resources","method":"resources/list"}),
    )
    .await?;
    let resources = read_message(&mut peer_reader).await?;
    assert!(
        resources["result"]["resources"]
            .as_array()
            .is_some_and(|resources| !resources.is_empty())
    );
    peer_writer.shutdown().await?;
    let _exit = task.await??;
    Ok(())
}

#[tokio::test]
async fn shipping_mcp_constructor_uses_the_bounded_sdk_durable_audit_and_controlled_artifacts()
-> TestResult {
    let temporary = tempfile::tempdir()?;
    let environment = BTreeMap::<OsString, OsString>::new();
    let config = AppConfig::load(ConfigSources::new(
        None,
        &environment,
        ConfigOverrides {
            data_dir: Some(temporary.path().to_path_buf()),
            source_shutdown_ms: Some(60_000),
            ..ConfigOverrides::default()
        },
    ))?;
    let product = LocalProduct::try_new(config)?;
    assert_eq!(
        product.application().shutdown_timeout(),
        Duration::from_secs(65)
    );
    let artifacts = product.artifacts();
    let artifact = artifacts
        .publish(
            ArtifactPublication::try_json(br#"{"value":1}"#.to_vec())?,
            ArtifactPublicationContext::new(
                CancellationToken::new(),
                Instant::now()
                    .checked_add(Duration::from_secs(5))
                    .ok_or("artifact publication deadline overflow")?,
            ),
        )
        .await?;
    let composition =
        LocalMcpComposition::try_new(product.paths(), product.application(), artifacts)?;
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (server_reader, server_writer) = tokio::io::split(server);
    let cancellation = CancellationToken::new();
    let task =
        tokio::spawn(composition.serve_unverified_io(server_reader, server_writer, cancellation));
    let (client_reader, mut client_writer) = tokio::io::split(client);
    let mut client_reader = BufReader::new(client_reader);

    write_message(
        &mut client_writer,
        json!({
            "jsonrpc":"2.0","id":"shipping-init","method":"initialize",
            "params":{
                "protocolVersion":"2026-07-28","capabilities":{},
                "clientInfo":{"name":"market-squawk-tests","version":"1"}
            }
        }),
    )
    .await?;
    let initialized = read_message(&mut client_reader).await?;
    assert_eq!(initialized["result"]["protocolVersion"], "2026-07-28");
    assert_eq!(initialized["result"]["serverInfo"]["name"], "market-squawk");
    write_message(
        &mut client_writer,
        json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
    )
    .await?;
    write_message(
        &mut client_writer,
        json!({"jsonrpc":"2.0","id":"shipping-tools","method":"tools/list"}),
    )
    .await?;
    let tools = read_message(&mut client_reader).await?;
    let names = tools["result"]["tools"]
        .as_array()
        .ok_or("tools/list response is missing tools")?
        .iter()
        .map(|tool| tool["name"].as_str().ok_or("tool is missing its name"))
        .collect::<Result<Vec<_>, _>>()?;
    let expected_capabilities = application_capabilities()?;
    validate_service_capabilities(
        &expected_capabilities,
        McpLimits::try_from(McpLimitSpec::default())?,
    )?;
    let expected_names = expected_capabilities
        .tools()
        .iter()
        .map(|tool| tool.name())
        .collect::<Vec<_>>();
    assert_eq!(names, expected_names);
    assert!(names.contains(&"Analysis.ReadArtifact"));
    write_message(
        &mut client_writer,
        json!({
            "jsonrpc":"2.0","id":"shipping-artifact","method":"tools/call",
            "params":{"name":"Analysis.ReadArtifact","arguments":{
                "artifactId":artifact.id(),
                "sha256":artifact.sha256(),
                "byteCount":artifact.byte_count(),
                "mediaType":artifact.media_type(),
                "offset":0,
                "maximumBytes":32768,
                "resultLimits":{"maximumItems":1,"maximumBytes":65536}
            }}
        }),
    )
    .await?;
    let artifact_read = read_message(&mut client_reader).await?;
    assert_eq!(
        artifact_read["result"]["structuredContent"]["data"]["artifact"]["artifactId"],
        artifact.id()
    );
    assert_eq!(
        artifact_read["result"]["structuredContent"]["data"]["contentBase64"],
        "eyJ2YWx1ZSI6MX0="
    );
    assert_eq!(
        artifact_read["result"]["structuredContent"]["data"]["complete"],
        true
    );
    write_message(
        &mut client_writer,
        json!({
            "jsonrpc":"2.0","id":"shipping-read","method":"tools/call",
            "params":{"name":"Bot.GetStatus","arguments":{
                "resultLimits":{"maximumItems":16,"maximumBytes":65536}
            }}
        }),
    )
    .await?;
    let status = read_message(&mut client_reader).await?;
    assert_eq!(
        status["result"]["structuredContent"]["data"]["state"], "stopped",
        "unexpected status response: {status}"
    );
    write_message(
        &mut client_writer,
        json!({
            "jsonrpc":"2.0","id":"shipping-mutation","method":"tools/call",
            "params":{
                "name":"Risk.TriggerKillSwitch",
                "arguments":{
                    "confirm":true,
                    "reason":"production composition test",
                    "resultLimits":{"maximumItems":16,"maximumBytes":65536}
                }
            }
        }),
    )
    .await?;
    let mutation = read_message(&mut client_reader).await?;
    assert_eq!(
        mutation["result"]["structuredContent"]["data"]["state"],
        "stopped"
    );
    assert_eq!(
        mutation["result"]["structuredContent"]["data"]["shutdownComplete"],
        true
    );
    client_writer.shutdown().await?;
    let _exit = task.await??;

    let audit = temporary.path().join("control").join("mcp-audit.jsonl");
    assert!(audit.is_file());
    assert!(std::fs::metadata(&audit)?.len() > 0);
    let mutation_phases = std::fs::read_to_string(&audit)?
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|record| {
            record["operation"]["kind"] == "call_tool"
                && record["operation"]["name"] == "Risk.TriggerKillSwitch"
        })
        .map(|record| record["phase"].clone())
        .collect::<Vec<_>>();
    assert_eq!(
        mutation_phases,
        [
            "mutation_admitted",
            "mutation_service_completed",
            "completed"
        ]
    );
    assert!(temporary.path().join("artifacts").join("mcp").is_dir());
    Ok(())
}

async fn write_message<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    value: Value,
) -> TestResult {
    writer.write_all(&serde_json::to_vec(&value)?).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    Ok(())
}

async fn read_message<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
) -> TestResult<Value> {
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    Ok(serde_json::from_str(&line)?)
}
