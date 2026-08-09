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
        InstalledService, InstalledServiceConnector, InstalledServiceError,
        InstalledServiceRunOutcome,
    },
};
use market_squawk_mcp::{McpLimitSpec, McpLimits, McpStdioRelay, validate_service_capabilities};
use market_squawk_platform::{
    AppConfig, ConfigOverrides, ConfigSources, EncryptedFileSecretStore, SecretStore, SecretValue,
};
use market_squawk_runtime::{
    ApplicationClient, EventPageLimit, InputAdmission, LoopbackApplicationClient, NamedClient,
};
use market_squawk_services::{ArtifactPublication, ArtifactPublicationContext, RequestId};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio_util::sync::CancellationToken;

type TestResult<T = ()> = anyhow::Result<T>;

const INSTALLED_SERVICE_PROCESS_ROLE_ENV: &str = "MARKET_SQUAWK_TEST_SERVICE_PROCESS_ROLE";
const INSTALLED_SERVICE_PROCESS_ROOT_ENV: &str = "MARKET_SQUAWK_TEST_SERVICE_PROCESS_ROOT";
const INSTALLED_SERVICE_TEST_UNLOCK: &str = "installed-service-test-unlock";
const INSTALLED_SERVICE_MAIN_STACK_BYTES: usize = 8 * 1024 * 1024;
const CRASH_RECOVERY_SOURCE_PROFILE: &str = "kraken.spot-public-market-data";
const INSTALLED_MCP_SERVICE_TIMEOUT: Duration = Duration::from_secs(30);
const OWNER_RESEARCH_DATASET: &str = "owner_price_history";
const OWNER_RESEARCH_CSV: &[u8] = b"row_id,Close Price\nrow-1,12.34\nrow-2,13.05\n";

#[test]
fn service_runtime_is_the_single_authority_for_native_and_mcp_clients() -> TestResult {
    let scenario = std::thread::Builder::new()
        .name("market-squawk-installed-service-test".to_owned())
        .stack_size(INSTALLED_SERVICE_MAIN_STACK_BYTES)
        .spawn(|| -> TestResult {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .context("construct installed-service test runtime")?
                .block_on(Box::pin(run_installed_service_authority_scenario()))
        })
        .context("start installed-service test thread")?;
    scenario.join().map_err(|_panic| {
        anyhow::anyhow!("installed-service test thread terminated unexpectedly")
    })?
}

async fn run_installed_service_authority_scenario() -> TestResult {
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
    let initial_phase = AssertUnwindSafe(Box::pin(async {
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

        import_owner_research_file(&desktop)
            .await
            .context("import and query one guided owner research file")?;

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
    }))
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
    let restarted_phase = AssertUnwindSafe(Box::pin(async {
        let restarted_desktop = connector
            .connect(NamedClient::Desktop, Some("tauri://localhost".to_owned()))
            .context("admit desktop client after service restart")?;
        assert_owner_research_file_available(&restarted_desktop)
            .await
            .context("query guided owner research file after service restart")?;
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
    }))
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

    let process_root = temporary.path().join("process-restart");
    let process_config = installed_service_process_config(&process_root)
        .context("load installed process-restart service configuration")?;
    let process_connector = InstalledServiceConnector::try_new_at_installation_root(
        &process_config,
        installed_service_authority_root(&process_root),
    )
    .context("construct installed process-restart connector")?;
    let mut service = start_installed_service_subprocess(&process_root)
        .context("start installed-service subprocess")?;
    wait_until_ready(&process_connector)
        .await
        .context("wait for initial installed-service subprocess")?;
    assert!(matches!(
        process_connector.bootstrap_status().await,
        Err(InstalledServiceError::ServiceUnavailable)
    ));

    run_installed_subprocess(&process_root, "clients")
        .await
        .context("exercise installed subprocess clients before crash")?;
    service
        .crash()
        .context("crash installed-service subprocess")?;
    assert!(matches!(
        process_connector.connect(NamedClient::Cli, None),
        Err(InstalledServiceError::ServiceUnavailable)
    ));

    let mut restarted_service = start_installed_service_subprocess(&process_root)
        .context("restart crashed installed-service subprocess")?;
    wait_until_ready(&process_connector)
        .await
        .context("wait for restarted installed-service subprocess")?;
    assert!(matches!(
        process_connector.bootstrap_status().await,
        Err(InstalledServiceError::ServiceUnavailable)
    ));
    run_installed_subprocess(&process_root, "cli")
        .await
        .context("exercise CLI subprocess after service restart")?;
    restarted_service
        .stop()
        .context("stop restarted installed-service subprocess")?;
    Ok(())
}

async fn import_owner_research_file(client: &LoopbackApplicationClient) -> TestResult {
    let admission = InputAdmission::try_sha256(
        "market-squawk.research-source-file.v1",
        u64::try_from(OWNER_RESEARCH_CSV.len()).context("measure owner research CSV")?,
        Sha256::digest(OWNER_RESEARCH_CSV).into(),
    )
    .context("admit owner research CSV")?;
    let mut bytes = OWNER_RESEARCH_CSV;
    let ticket = client
        .stage_input(admission, &mut bytes, CancellationToken::new())
        .await
        .context("stage owner research CSV")?;
    let preview = client
        .invoke_operation(
            RequestId::try_string("installed-research-file-preview")
                .context("construct research-file preview request ID")?,
            "Research.PreviewStagedFile",
            json!({
                "inputTicketId": ticket.id(),
                "format": "csv",
                "confirm": true,
                "resultLimits": {"maximumItems": 64, "maximumBytes": 1_048_576},
            }),
            INSTALLED_MCP_SERVICE_TIMEOUT,
            CancellationToken::new(),
        )
        .await
        .context("preview owner research CSV")?;
    assert_eq!(preview.result()["ok"], true, "{}", preview.result());
    let preview_data = &preview.result()["value"]["data"];
    assert_eq!(preview_data["rowCount"], 2);
    assert!(
        preview_data["columns"].as_array().is_some_and(|columns| {
            columns
                .iter()
                .any(|column| column["name"] == "Close Price" && column["kind"] == "exact_decimal")
        }),
        "{}",
        preview.result()
    );
    let encoded_preview = serde_json::to_string(preview_data)?;
    assert!(!encoded_preview.contains(&ticket.id().as_uuid().to_string()));
    assert!(!encoded_preview.contains("inputTicketId"));
    assert!(!encoded_preview.contains("path"));
    let preview_id = preview_data["previewId"]
        .as_str()
        .context("research preview omitted its identity")?;

    let committed = client
        .invoke_operation(
            RequestId::try_string("installed-research-file-commit")
                .context("construct research-file commit request ID")?,
            "Research.CommitStagedFile",
            json!({
                "previewId": preview_id,
                "mapping": {
                    "dataset": OWNER_RESEARCH_DATASET,
                    "identityField": "row_id",
                    "fields": [{
                        "source": "Close Price",
                        "field": "close_price",
                        "decimalScale": 2,
                        "unit": "USD"
                    }],
                    "effectiveAt": "2026-08-08T00:00:00Z"
                },
                "confirm": true,
                "resultLimits": {"maximumItems": 64, "maximumBytes": 1_048_576},
            }),
            INSTALLED_MCP_SERVICE_TIMEOUT,
            CancellationToken::new(),
        )
        .await
        .context("commit owner research CSV")?;
    assert_eq!(committed.result()["ok"], true, "{}", committed.result());
    assert_eq!(committed.result()["value"]["data"]["state"], "queued");
    let job_id = committed.result()["value"]["data"]["jobId"]
        .as_str()
        .context("research commit omitted its durable job identity")?;
    wait_for_job_completion(client, job_id).await?;
    assert_owner_research_file_available(client).await
}

async fn wait_for_job_completion(client: &LoopbackApplicationClient, job_id: &str) -> TestResult {
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(30))
        .context("compute research-file job deadline")?;
    loop {
        let job = client
            .invoke_operation(
                RequestId::try_string(format!("installed-research-job-{job_id}"))
                    .context("construct research job request ID")?,
                "Job.Get",
                json!({"jobId": job_id}),
                Duration::from_secs(5),
                CancellationToken::new(),
            )
            .await
            .context("read guided research-file job")?;
        assert_eq!(job.result()["ok"], true, "{}", job.result());
        match job.result()["value"]["data"]["state"].as_str() {
            Some("completed") => return Ok(()),
            Some("failed" | "cancelled" | "interrupted") => {
                anyhow::bail!(
                    "guided research-file job did not complete: {}",
                    job.result()
                );
            }
            Some(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            _ => anyhow::bail!(
                "guided research-file job missed its deadline: {}",
                job.result()
            ),
        }
    }
}

async fn assert_owner_research_file_available(client: &LoopbackApplicationClient) -> TestResult {
    let manifest = client
        .invoke_operation(
            RequestId::try_string("installed-research-file-manifest")
                .context("construct research manifest request ID")?,
            "Research.GetManifest",
            json!({
                "dataset": OWNER_RESEARCH_DATASET,
                "resultLimits": {"maximumItems": 64, "maximumBytes": 1_048_576},
            }),
            Duration::from_secs(5),
            CancellationToken::new(),
        )
        .await
        .context("read guided research-file manifest")?;
    assert_eq!(manifest.result()["ok"], true, "{}", manifest.result());
    assert_eq!(
        manifest.result()["value"]["data"]["manifest"]["datasetId"],
        OWNER_RESEARCH_DATASET
    );
    assert_eq!(manifest.result()["value"]["data"]["rowCount"], 2);

    let history = client
        .invoke_operation(
            RequestId::try_string("installed-research-file-history")
                .context("construct research history request ID")?,
            "Research.GetHistory",
            json!({
                "dataset": OWNER_RESEARCH_DATASET,
                "resultLimits": {"maximumItems": 64, "maximumBytes": 1_048_576},
            }),
            Duration::from_secs(5),
            CancellationToken::new(),
        )
        .await
        .context("query guided research-file observations")?;
    assert_eq!(history.result()["ok"], true, "{}", history.result());
    let rows = history.result()["value"]["data"]["rows"]
        .as_array()
        .context("guided research-file query did not return inline rows")?;
    assert_eq!(rows.len(), 2, "{}", history.result());
    assert!(
        rows.iter().any(|row| {
            row["source_identifier"] == "row-1"
                && row["value_mantissa"] == 1_234
                && row["value_scale"] == 2
        }),
        "{}",
        history.result()
    );
    Ok(())
}

async fn run_installed_service_process_role(role: &OsString, root: PathBuf) -> TestResult {
    match role.to_str() {
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
                    .context(
                        "resolve durable source registration before installed-service crash",
                    )?;
                assert_eq!(
                    registration.result()["value"]["data"]["outcome"],
                    "replay",
                    "{}",
                    registration.result()
                );
                assert_eq!(
                    registration.result()["value"]["data"]["profile"]["id"],
                    CRASH_RECOVERY_SOURCE_PROFILE,
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
    AppConfig::load(ConfigSources::new(
        None,
        &BTreeMap::<OsString, OsString>::new(),
        ConfigOverrides {
            data_dir: Some(root.join("product")),
            source_shutdown_ms: Some(60_000),
            ..ConfigOverrides::default()
        },
    ))
    .context("load installed-service process configuration")
}

fn installed_service_authority_root(root: &Path) -> PathBuf {
    root.join(".market-squawk-installed-service")
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
