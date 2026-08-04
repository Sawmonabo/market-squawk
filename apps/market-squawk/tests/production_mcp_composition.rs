use std::{
    collections::BTreeMap,
    ffi::OsString,
    io::Read as _,
    panic::{AssertUnwindSafe, resume_unwind},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Context as _;
use futures_util::FutureExt as _;
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
    AppConfig, ConfigOverrides, ConfigSources, EncryptedFileSecretStore, SecretStore, SecretValue,
};
use market_squawk_runtime::{ApplicationClient, EventPageLimit, NamedClient};
use market_squawk_services::{ArtifactPublication, ArtifactPublicationContext, RequestId};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio_util::sync::CancellationToken;

type TestResult<T = ()> = anyhow::Result<T>;

const INSTALLED_SERVICE_PROCESS_ROLE_ENV: &str = "MARKET_SQUAWK_TEST_SERVICE_PROCESS_ROLE";
const INSTALLED_SERVICE_PROCESS_ROOT_ENV: &str = "MARKET_SQUAWK_TEST_SERVICE_PROCESS_ROOT";
const INSTALLED_SERVICE_TEST_UNLOCK: &str = "installed-service-test-unlock";
const CRASH_RECOVERY_SOURCE_PROFILE: &str = "coinbase.public-market-data";
const INSTALLED_MCP_SERVICE_TIMEOUT: Duration = Duration::from_secs(30);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_runtime_is_the_single_authority_for_native_and_mcp_clients() -> TestResult {
    if let Some(role) = std::env::var_os(INSTALLED_SERVICE_PROCESS_ROLE_ENV) {
        let root = std::env::var_os(INSTALLED_SERVICE_PROCESS_ROOT_ENV)
            .map(PathBuf::from)
            .context("resolve installed-service subprocess root")?;
        return run_installed_service_process_role(&role, root).await;
    }
    let temporary = tempfile::tempdir().context("create installed-service scenario root")?;
    let environment = BTreeMap::<OsString, OsString>::new();
    let config = AppConfig::load(ConfigSources::new(
        None,
        &environment,
        ConfigOverrides {
            data_dir: Some(temporary.path().join("product")),
            source_shutdown_ms: Some(60_000),
            ..ConfigOverrides::default()
        },
    ))
    .context("load installed-service scenario configuration")?;
    let secret_root = temporary.path().join("runtime-secrets");
    let secrets: Arc<dyn SecretStore> = Arc::new(
        EncryptedFileSecretStore::try_open(
            &secret_root,
            SecretValue::new(INSTALLED_SERVICE_TEST_UNLOCK.to_owned())
                .context("construct installed-service test unlock")?,
        )
        .context("open installed-service scenario secret store")?,
    );
    let connector = InstalledServiceConnector::try_new_at_installation_root(
        &config,
        temporary.path().join(".market-squawk-installed-service"),
    )
    .context("construct initial installed-service connector")?;
    let service = InstalledService::start_with_secret_store(config.clone(), Arc::clone(&secrets))
        .await
        .context("start initial installed service")?;
    let shutdown = CancellationToken::new();
    let service_task = tokio::spawn(service.run(shutdown.clone()));
    let initial_phase = AssertUnwindSafe(async {
        assert!(matches!(
            InstalledService::start_with_secret_store(config.clone(), Arc::clone(&secrets)).await,
            Err(InstalledServiceError::AlreadyRunning)
        ));
        let desktop = connector
            .connect(NamedClient::Desktop, Some("tauri://localhost".to_owned()))
            .context("admit initial desktop client")?;
        let cli = connector
            .connect(NamedClient::Cli, None)
            .context("admit initial CLI client")?;
        let bootstrap = desktop
            .bootstrap(CancellationToken::new())
            .await
            .context("fetch initial desktop bootstrap")?;
        assert_eq!(bootstrap["readiness"]["service"], true);
        assert!(bootstrap["runtime"]["workspaceId"].is_string());
        let provider = bootstrap["sources"]["profiles"][0]["id"]
            .as_str()
            .context("read provider from initial desktop bootstrap")?;
        let registration = desktop
            .invoke_operation(
                RequestId::try_string("installed-source-registration")
                    .context("construct source-registration request ID")?,
                "Source.Register",
                json!({
                    "provider": provider,
                    "confirm": true,
                    "resultLimits": {"maximumItems": 16, "maximumBytes": 1_048_576},
                }),
                Duration::from_secs(5),
                CancellationToken::new(),
            )
            .await
            .context("register source through initial desktop client")?;
        assert_eq!(
            registration.result()["ok"],
            true,
            "{}",
            registration.result()
        );
        let (events, cursor) = desktop
            .read_events(
                None,
                EventPageLimit::try_new(4).context("construct installed event-page limit")?,
                CancellationToken::new(),
            )
            .await
            .context("read initial installed-service events")?;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], "application.changed");
        assert_eq!(events[0]["operation"], "Source.Register");
        assert_eq!(cursor.sequence(), 1);

        let jobs = cli
            .invoke_operation(
                RequestId::try_string("installed-job-list")
                    .context("construct job-list request ID")?,
                "Job.List",
                json!({"limit": 16}),
                Duration::from_secs(5),
                CancellationToken::new(),
            )
            .await
            .context("list jobs through initial CLI client")?;
        assert_eq!(jobs.result()["ok"], true);
        assert_eq!(jobs.result()["value"]["data"]["jobs"], json!([]));

        let rotated = desktop
            .invoke_operation(
                RequestId::try_string("installed-claude-rotation")
                    .context("construct Claude rotation request ID")?,
                "Mcp.RotateCredential",
                json!({"client": "claude_code"}),
                INSTALLED_MCP_SERVICE_TIMEOUT,
                CancellationToken::new(),
            )
            .await
            .context("rotate Claude credential through desktop authority")?;
        assert_eq!(rotated.result()["value"]["credentialGeneration"], json!(2));
        let revoked = desktop
            .invoke_operation(
                RequestId::try_string("installed-codex-revocation")
                    .context("construct Codex revocation request ID")?,
                "Mcp.RevokeCredential",
                json!({"client": "codex"}),
                INSTALLED_MCP_SERVICE_TIMEOUT,
                CancellationToken::new(),
            )
            .await
            .context("revoke Codex credential through desktop authority")?;
        assert_eq!(revoked.result()["value"]["accessRevoked"], true);

        exercise_installed_relay(
            NamedClient::ClaudeCode,
            connector
                .connect_mcp_relay(NamedClient::ClaudeCode)
                .context("admit rotated Claude relay")?,
        )
        .await
        .context("exercise rotated Claude relay")?;
        assert!(matches!(
            connector.connect_mcp_relay(NamedClient::Codex),
            Err(InstalledServiceError::AdmissionRejected)
        ));
        cli.probe_ready(CancellationToken::new())
            .await
            .context("probe initial CLI client readiness")?;
        Ok::<(), anyhow::Error>(())
    })
    .catch_unwind()
    .await;
    shutdown.cancel();
    let service_result = service_task
        .await
        .context("join initial installed-service task");
    match initial_phase {
        Ok(result) => result?,
        Err(panic) => {
            drop(service_result);
            resume_unwind(panic);
        }
    }
    assert_eq!(
        service_result?.context("stop initial installed service after interaction phase")?,
        InstalledServiceRunOutcome::Stopped
    );
    assert!(connector.connect(NamedClient::Cli, None).is_err());

    let restarted = InstalledService::start_with_secret_store(config.clone(), Arc::clone(&secrets))
        .await
        .context("restart installed service with durable credentials")?;
    let restarted_shutdown = CancellationToken::new();
    let restarted_task = tokio::spawn(restarted.run(restarted_shutdown.clone()));
    let restarted_phase = AssertUnwindSafe(async {
        let restarted_desktop = connector
            .connect(NamedClient::Desktop, Some("tauri://localhost".to_owned()))
            .context("admit desktop client after service restart")?;
        exercise_installed_relay(
            NamedClient::ClaudeCode,
            connector
                .connect_mcp_relay(NamedClient::ClaudeCode)
                .context("admit persisted Claude relay after restart")?,
        )
        .await
        .context("exercise persisted Claude relay after restart")?;
        assert!(matches!(
            connector.connect_mcp_relay(NamedClient::Codex),
            Err(InstalledServiceError::AdmissionRejected)
        ));
        let activated = restarted_desktop
            .invoke_operation(
                RequestId::try_string("installed-codex-reactivation")
                    .context("construct Codex reactivation request ID")?,
                "Mcp.ActivateCredential",
                json!({"client": "codex"}),
                INSTALLED_MCP_SERVICE_TIMEOUT,
                CancellationToken::new(),
            )
            .await
            .context("reactivate Codex credential after restart")?;
        assert_eq!(activated.result()["value"]["accessRevoked"], false);
        exercise_installed_relay(
            NamedClient::Codex,
            connector
                .connect_mcp_relay(NamedClient::Codex)
                .context("admit reactivated Codex relay")?,
        )
        .await
        .context("exercise reactivated Codex relay")?;
        Ok::<(), anyhow::Error>(())
    })
    .catch_unwind()
    .await;
    restarted_shutdown.cancel();
    let restarted_result = restarted_task
        .await
        .context("join restarted installed-service task");
    match restarted_phase {
        Ok(result) => result?,
        Err(panic) => {
            drop(restarted_result);
            resume_unwind(panic);
        }
    }
    assert_eq!(
        restarted_result?.context("stop restarted installed service after interaction phase")?,
        InstalledServiceRunOutcome::Stopped
    );
    drop(connector);
    assert_eq!(Arc::strong_count(&secrets), 1);
    drop(secrets);

    let bootstrap_seed_root = temporary.path().join("bootstrap-seed");
    run_installed_subprocess(&bootstrap_seed_root, "seed")
        .await
        .context("seed encrypted installed-service runtime")?;
    let bootstrap_config = installed_service_process_config(&bootstrap_seed_root)
        .context("load locked-bootstrap service configuration")?;
    let locked_connector = InstalledServiceConnector::try_new_at_installation_root(
        &bootstrap_config,
        installed_service_authority_root(&bootstrap_seed_root),
    )
    .context("construct locked-bootstrap connector")?;
    let mut service = start_installed_service_subprocess(&bootstrap_seed_root)
        .context("start locked installed-service subprocess")?;
    let bootstrap = wait_for_bootstrap(&locked_connector)
        .await
        .context("wait for initial encrypted-fallback bootstrap")?;
    assert_eq!(bootstrap.state(), InstalledServiceBootstrapState::Required);
    assert_eq!(
        bootstrap.requirement(),
        Some(BootstrapRequirement::EncryptedFallbackLocked)
    );
    assert!(locked_connector.connect(NamedClient::Cli, None).is_err());
    let installation = bootstrap.installation_id();
    let first_bootstrap_generation = bootstrap.generation();

    let accepted = locked_connector
        .bootstrap_unlock(
            SecretValue::new(INSTALLED_SERVICE_TEST_UNLOCK.to_owned())
                .context("construct initial bootstrap unlock")?,
        )
        .await
        .context("submit initial encrypted-fallback unlock")?;
    assert_eq!(accepted.state(), InstalledServiceBootstrapState::Retrying);
    assert!(
        locked_connector
            .bootstrap_unlock(
                SecretValue::new(INSTALLED_SERVICE_TEST_UNLOCK.to_owned())
                    .context("construct rejected repeated bootstrap unlock")?,
            )
            .await
            .is_err()
    );
    wait_until_ready(&locked_connector)
        .await
        .context("wait for initially unlocked installed service")?;
    assert!(matches!(
        locked_connector.bootstrap_status().await,
        Err(InstalledServiceError::ServiceUnavailable)
    ));

    run_installed_subprocess(&bootstrap_seed_root, "clients")
        .await
        .context("exercise installed subprocess clients before crash")?;
    service
        .crash()
        .context("crash installed-service subprocess")?;
    assert!(matches!(
        locked_connector.connect(NamedClient::Cli, None),
        Err(InstalledServiceError::ServiceUnavailable)
    ));

    let mut restarted_service = start_installed_service_subprocess(&bootstrap_seed_root)
        .context("restart crashed installed-service subprocess")?;
    let restarted_bootstrap = wait_for_bootstrap(&locked_connector)
        .await
        .context("wait for restarted encrypted-fallback bootstrap")?;
    assert_eq!(
        restarted_bootstrap.state(),
        InstalledServiceBootstrapState::Required
    );
    assert_eq!(
        restarted_bootstrap.requirement(),
        Some(BootstrapRequirement::EncryptedFallbackLocked)
    );
    assert_eq!(restarted_bootstrap.installation_id(), installation);
    assert_ne!(restarted_bootstrap.generation(), first_bootstrap_generation);
    let restarted_accepted = locked_connector
        .bootstrap_unlock(
            SecretValue::new(INSTALLED_SERVICE_TEST_UNLOCK.to_owned())
                .context("construct restarted bootstrap unlock")?,
        )
        .await
        .context("submit restarted encrypted-fallback unlock")?;
    assert_eq!(
        restarted_accepted.state(),
        InstalledServiceBootstrapState::Retrying
    );
    assert_eq!(
        restarted_accepted.generation(),
        restarted_bootstrap.generation()
    );
    wait_until_ready(&locked_connector)
        .await
        .context("wait for restarted unlocked installed service")?;
    run_installed_subprocess(&bootstrap_seed_root, "cli")
        .await
        .context("exercise CLI subprocess after service restart")?;
    restarted_service
        .stop()
        .context("stop restarted installed-service subprocess")?;
    Ok(())
}

async fn run_installed_service_process_role(role: &OsString, root: PathBuf) -> TestResult {
    match role.to_str() {
        Some("seed") => seed_encrypted_runtime(root)
            .await
            .context("run encrypted-runtime seed subprocess role")
            .map_err(Into::into),
        Some("service") => {
            let config = installed_service_process_config(&root)
                .context("load installed-service subprocess configuration")?;
            let service = InstalledService::start_at_installation_root(
                config,
                installed_service_authority_root(&root),
            )
            .await
            .context("start installed-service subprocess role")?;
            let cancellation = CancellationToken::new();
            let task = tokio::spawn(service.run(cancellation.clone()));
            let mut stop = [0_u8; 1];
            let _read = tokio::io::AsyncReadExt::read(&mut tokio::io::stdin(), &mut stop)
                .await
                .context("wait for installed-service subprocess stop signal")?;
            cancellation.cancel();
            assert_eq!(
                task.await
                    .context("join installed-service subprocess task")?
                    .context("stop installed-service subprocess role")?,
                InstalledServiceRunOutcome::Stopped
            );
            Ok(())
        }
        Some("clients" | "cli") => {
            let role = role
                .to_str()
                .context("decode installed client subprocess role")?;
            let config = installed_service_process_config(&root)
                .context("load installed client subprocess configuration")?;
            let connector = InstalledServiceConnector::try_new_at_installation_root(
                &config,
                installed_service_authority_root(&root),
            )
            .context("construct installed client subprocess connector")?;
            let cli = connector
                .connect(NamedClient::Cli, None)
                .context("admit installed subprocess CLI client")?;
            cli.probe_ready(CancellationToken::new())
                .await
                .context("probe installed subprocess CLI readiness")?;
            if role == "clients" {
                let desktop = connector
                    .connect(NamedClient::Desktop, Some("tauri://localhost".to_owned()))
                    .context("admit installed subprocess desktop client")?;
                let snapshot = desktop
                    .bootstrap(CancellationToken::new())
                    .await
                    .context("fetch installed subprocess desktop bootstrap")?;
                assert_eq!(snapshot["readiness"]["service"], true);
                let registration = desktop
                    .invoke_operation(
                        RequestId::try_string("installed-crash-source-registration")
                            .context("construct crash source-registration request ID")?,
                        "Source.Register",
                        json!({
                            "provider": CRASH_RECOVERY_SOURCE_PROFILE,
                            "confirm": true,
                            "resultLimits": {"maximumItems": 16, "maximumBytes": 1_048_576},
                        }),
                        Duration::from_secs(5),
                        CancellationToken::new(),
                    )
                    .await
                    .context("register durable source before installed-service crash")?;
                assert_eq!(
                    registration.result()["value"]["data"]["outcome"],
                    "inserted",
                    "{}",
                    registration.result()
                );
                for client in [NamedClient::ClaudeCode, NamedClient::Codex] {
                    exercise_installed_relay(
                        client,
                        connector
                            .connect_mcp_relay(client)
                            .context("admit installed subprocess MCP relay")?,
                    )
                    .await
                    .context("exercise installed subprocess MCP relay")?;
                }
            } else {
                let replay = cli
                    .invoke_operation(
                        RequestId::try_string("installed-crash-source-replay")
                            .context("construct recovered source-registration request ID")?,
                        "Source.Register",
                        json!({
                            "provider": CRASH_RECOVERY_SOURCE_PROFILE,
                            "confirm": true,
                            "resultLimits": {"maximumItems": 16, "maximumBytes": 1_048_576},
                        }),
                        Duration::from_secs(5),
                        CancellationToken::new(),
                    )
                    .await
                    .context("prove crashed source registration survived restart")?;
                assert_eq!(replay.result()["value"]["data"]["outcome"], "replay");
                let status = cli
                    .invoke_operation(
                        RequestId::try_string("installed-crash-source-status")
                            .context("construct recovered source-status request ID")?,
                        "Source.GetStatus",
                        json!({
                            "sourceCoverage": [CRASH_RECOVERY_SOURCE_PROFILE],
                            "resultLimits": {"maximumItems": 16, "maximumBytes": 1_048_576},
                        }),
                        Duration::from_secs(5),
                        CancellationToken::new(),
                    )
                    .await
                    .context("query recovered source registration after restart")?;
                let rows = status.result()["value"]["data"]
                    .as_array()
                    .context("recovered source status was not an array")?;
                assert_eq!(rows.len(), 1, "{}", status.result());
                assert_eq!(
                    rows[0]["profile"]["id"],
                    CRASH_RECOVERY_SOURCE_PROFILE,
                    "{}",
                    status.result()
                );
            }
            Ok(())
        }
        _ => anyhow::bail!("unsupported installed-service subprocess role"),
    }
}

fn installed_service_process_config(root: &Path) -> TestResult<AppConfig> {
    Ok(AppConfig::load(ConfigSources::new(
        None,
        &BTreeMap::<OsString, OsString>::new(),
        ConfigOverrides {
            data_dir: Some(root.join("product")),
            source_shutdown_ms: Some(60_000),
            ..ConfigOverrides::default()
        },
    ))
    .context("load installed-service process configuration")?)
}

fn installed_service_authority_root(root: &Path) -> PathBuf {
    root.join(".market-squawk-installed-service")
}

async fn wait_for_bootstrap(
    connector: &InstalledServiceConnector,
) -> TestResult<market_squawk::service::InstalledServiceBootstrapStatus> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match connector.bootstrap_status().await {
                Ok(status) => break Ok::<_, InstalledServiceError>(status),
                Err(InstalledServiceError::ServiceUnavailable) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(error) => break Err(error),
            }
        }
    })
    .await
    .context("time out waiting for installed-service bootstrap status")?
    .context("poll installed-service bootstrap status")
    .map_err(Into::into)
}

async fn wait_until_ready(connector: &InstalledServiceConnector) -> TestResult {
    let client = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            match connector.connect(NamedClient::Cli, None) {
                Ok(client) => break Ok::<_, InstalledServiceError>(client),
                Err(InstalledServiceError::ServiceUnavailable) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(error) => break Err(error),
            }
        }
    })
    .await
    .context("time out waiting for installed-service readiness")?
    .context("poll installed-service readiness")?;
    client
        .probe_ready(CancellationToken::new())
        .await
        .context("probe newly ready installed-service client")?;
    Ok(())
}

async fn run_installed_subprocess(root: &Path, role: &str) -> TestResult {
    let executable = std::env::current_exe().context("resolve installed test executable")?;
    let root = root.to_path_buf();
    let role = role.to_owned();
    let output = tokio::task::spawn_blocking(move || {
        Command::new(executable)
            .args([
                "--exact",
                "production_mcp_composition::service_runtime_is_the_single_authority_for_native_and_mcp_clients",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(INSTALLED_SERVICE_PROCESS_ROLE_ENV, role)
            .env(INSTALLED_SERVICE_PROCESS_ROOT_ENV, root)
            .output()
    })
    .await
    .context("join installed client subprocess launcher")?
    .context("execute installed client subprocess role")?;
    if !output.status.success() {
        anyhow::bail!(
            "installed subprocess role failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

struct InstalledServiceProcess {
    child: Child,
}

impl InstalledServiceProcess {
    fn stop(&mut self) -> TestResult {
        drop(self.child.stdin.take());
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(15))
            .context("compute installed-service subprocess stop deadline")?;
        loop {
            if let Some(status) = self
                .child
                .try_wait()
                .context("poll installed-service subprocess exit")?
            {
                if status.success() {
                    return Ok(());
                }
                let mut stderr = String::new();
                if let Some(source) = self.child.stderr.take() {
                    source
                        .take(8 * 1024)
                        .read_to_string(&mut stderr)
                        .context("read failed installed-service subprocess stderr")?;
                }
                anyhow::bail!("installed service subprocess failed: {stderr}");
            }
            if Instant::now() >= deadline {
                self.child
                    .kill()
                    .context("kill installed-service subprocess after stop timeout")?;
                let _status = self
                    .child
                    .wait()
                    .context("reap installed-service subprocess after stop timeout")?;
                anyhow::bail!("installed service subprocess missed its stop deadline");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn crash(&mut self) -> TestResult {
        self.child
            .kill()
            .context("kill installed-service subprocess for crash simulation")?;
        let status = self
            .child
            .wait()
            .context("reap crashed installed-service subprocess")?;
        if status.success() {
            anyhow::bail!("installed service crash unexpectedly exited successfully");
        }
        Ok(())
    }
}

impl Drop for InstalledServiceProcess {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn start_installed_service_subprocess(root: &Path) -> TestResult<InstalledServiceProcess> {
    let executable = std::env::current_exe().context("resolve installed test executable")?;
    let child = Command::new(executable)
        .args([
            "--exact",
            "production_mcp_composition::service_runtime_is_the_single_authority_for_native_and_mcp_clients",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(INSTALLED_SERVICE_PROCESS_ROLE_ENV, "service")
        .env(INSTALLED_SERVICE_PROCESS_ROOT_ENV, root)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .context("spawn installed-service subprocess")?;
    Ok(InstalledServiceProcess { child })
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
    ))
    .context("load encrypted-runtime seed configuration")?;
    let secrets: Arc<dyn SecretStore> = Arc::new(
        EncryptedFileSecretStore::try_open(
            installed_service_authority_root(&root)
                .join("control")
                .join("secrets/installed-runtime"),
            SecretValue::new(INSTALLED_SERVICE_TEST_UNLOCK.to_owned())
                .context("construct encrypted-runtime seed unlock")?,
        )
        .context("open encrypted-runtime seed secret store")?,
    );
    let service = InstalledService::start_with_secret_store(config, secrets)
        .await
        .context("start encrypted-runtime seed service")?;
    let cancellation = CancellationToken::new();
    let task = tokio::spawn(service.run(cancellation.clone()));
    cancellation.cancel();
    assert_eq!(
        task.await
            .context("join encrypted-runtime seed service task")?
            .context("stop encrypted-runtime seed service")?,
        InstalledServiceRunOutcome::Stopped
    );
    Ok(())
}

async fn exercise_installed_relay(
    client: NamedClient,
    transport: Arc<dyn market_squawk_mcp::McpRelayTransport>,
) -> TestResult {
    let relay = McpStdioRelay::try_new(
        client,
        transport,
        McpLimits::try_from(McpLimitSpec::default()).context("construct installed relay limits")?,
    )
    .context("construct installed stdio relay")?;
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
    .await
    .context("write installed relay initialize request")?;
    let initialized = read_message(&mut peer_reader)
        .await
        .context("read installed relay initialize response")?;
    assert_eq!(initialized["result"]["protocolVersion"], "2026-07-28");
    write_message(
        &mut peer_writer,
        json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
    )
    .await
    .context("write installed relay initialized notification")?;
    write_message(
        &mut peer_writer,
        json!({"jsonrpc":"2.0","id":"installed-resources","method":"resources/list"}),
    )
    .await
    .context("write installed relay resources-list request")?;
    let resources = read_message(&mut peer_reader)
        .await
        .context("read installed relay resources-list response")?;
    assert!(
        resources["result"]["resources"]
            .as_array()
            .is_some_and(|resources| !resources.is_empty())
    );
    peer_writer
        .shutdown()
        .await
        .context("close installed relay peer request stream")?;
    let _exit = task
        .await
        .context("join installed stdio relay task")?
        .context("serve installed stdio relay protocol")?;
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
                    .context("artifact publication deadline overflow")?,
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
        .context("tools/list response is missing tools")?
        .iter()
        .map(|tool| {
            tool["name"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("tool is missing its name"))
        })
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
