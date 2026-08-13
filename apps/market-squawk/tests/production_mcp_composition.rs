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
#[cfg(all(feature = "board-installed-fixture", debug_assertions))]
use chrono::{Datelike as _, NaiveDate};
use futures_util::FutureExt as _;
#[cfg(all(feature = "board-installed-fixture", debug_assertions))]
use market_squawk::BoardInstalledFixtureBundle;
use market_squawk::{
    application::application_capabilities,
    service::{
        InstalledService, InstalledServiceConnector, InstalledServiceError,
        InstalledServiceRunOutcome,
    },
};
#[cfg(all(feature = "board-installed-fixture", debug_assertions))]
use market_squawk_adapter_federal_reserve::{
    BOARD_H15_TREASURY_CONSTANT_MATURITIES_ROLLING_DASHBOARD_DATE_COUNT,
    BOARD_H15_TREASURY_CONSTANT_MATURITIES_ROLLING_DASHBOARD_OBSERVATION_COUNT,
    BoardDatasetProfile, BoardScriptedCsvResponse, BoardScriptedTransportCounters,
    BoardScriptedTransportFactory,
};
#[cfg(all(feature = "board-installed-fixture", debug_assertions))]
use market_squawk_domain::Timestamp;
use market_squawk_mcp::{McpLimitSpec, McpLimits, McpStdioRelay};
use market_squawk_platform::{
    AppConfig, ConfigOverrides, ConfigSources, EncryptedFileSecretStore, SecretStore, SecretValue,
};
use market_squawk_runtime::{
    ApplicationClient, EventPageLimit, InputAdmission, LoopbackApplicationClient, NamedClient,
};
use market_squawk_services::RequestId;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

type TestResult<T = ()> = anyhow::Result<T>;

const INSTALLED_SERVICE_PROCESS_ROLE_ENV: &str = "MARKET_SQUAWK_TEST_SERVICE_PROCESS_ROLE";
const INSTALLED_SERVICE_PROCESS_ROOT_ENV: &str = "MARKET_SQUAWK_TEST_SERVICE_PROCESS_ROOT";
const INSTALLED_SERVICE_TEST_UNLOCK: &str = "installed-service-test-unlock";
const INSTALLED_SERVICE_MAIN_STACK_BYTES: usize = 8 * 1024 * 1024;
const CRASH_RECOVERY_SOURCE_PROFILE: &str = "kraken.spot-public-market-data";
const INSTALLED_MCP_SERVICE_TIMEOUT: Duration = Duration::from_secs(30);
const REAL_ALPACA_BUNDLE_PATH_ENV: &str = "MARKET_SQUAWK_TEST_REAL_ALPACA_BUNDLE_PATH";
const REAL_ALPACA_SURFACE: &str = "alpaca.basic-market-data";
const REAL_ALPACA_CREDENTIAL_MEDIA_TYPE: &str = "market-squawk.provider-credentials.v1";
const REAL_ALPACA_CREDENTIAL_SCHEMA: &str = "market-squawk-provider-credentials/v1";
const REAL_ALPACA_CREDENTIAL_MAXIMUM_BYTES: u64 = 64 * 1024;
const REAL_ALPACA_LIFECYCLE_TIMEOUT: Duration = Duration::from_secs(120);
const REAL_ALPACA_MARKET_READY_TIMEOUT: Duration = Duration::from_secs(60);
const OWNER_RESEARCH_DATASET: &str = "owner_price_history";
const OWNER_RESEARCH_CSV: &[u8] = b"row_id,Close Price\nrow-1,12.34\nrow-2,13.05\n";
#[cfg(all(feature = "board-installed-fixture", debug_assertions))]
const BOARD_SURFACE: &str = "federal-reserve-board.data-download-program";
#[cfg(all(feature = "board-installed-fixture", debug_assertions))]
const INSTALLED_BOARD_HISTORY_MAXIMUM_BYTES: usize = 8 * 1024 * 1024;

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
    let real_alpaca_bundle_path = protected_real_alpaca_bundle_path()?;
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
    #[cfg(all(feature = "board-installed-fixture", debug_assertions))]
    let board_fixture = installed_board_fixture().context("construct installed Board fixture")?;
    #[cfg(all(feature = "board-installed-fixture", debug_assertions))]
    let service = InstalledService::start_with_secret_store_and_board_fixture(
        config.clone(),
        Arc::clone(&secrets),
        board_fixture.clone(),
    )
    .await
    .context("start initial installed service with Board fixture")?;
    #[cfg(not(all(feature = "board-installed-fixture", debug_assertions)))]
    let service = InstalledService::start_with_secret_store(config.clone(), Arc::clone(&secrets))
        .await
        .context("start initial installed service")?;
    let shutdown = CancellationToken::new();
    let service_task = tokio::spawn(service.run(shutdown.clone()));
    #[cfg(all(feature = "board-installed-fixture", debug_assertions))]
    let mut board_evidence = None;
    let mut real_alpaca_evidence = None;
    let initial_phase = AssertUnwindSafe(Box::pin(async {
        #[cfg(all(feature = "board-installed-fixture", debug_assertions))]
        assert!(matches!(
            InstalledService::start_with_secret_store_and_board_fixture(
                config.clone(),
                Arc::clone(&secrets),
                board_fixture.clone(),
            )
            .await,
            Err(InstalledServiceError::AlreadyRunning)
        ));
        #[cfg(not(all(feature = "board-installed-fixture", debug_assertions)))]
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

        #[cfg(all(feature = "board-installed-fixture", debug_assertions))]
        {
            board_evidence = Some(
                exercise_installed_board_vertical(&desktop, temporary.path(), &board_fixture)
                    .await
                    .context("exercise installed Federal Reserve Board vertical")?,
            );
        }

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

        if let Some(path) = real_alpaca_bundle_path.as_deref() {
            real_alpaca_evidence = Some(
                exercise_real_alpaca_vertical(&desktop, path)
                    .await
                    .context("exercise opt-in real Alpaca production vertical")?,
            );
        }

        exercise_installed_relay_with_market(
            NamedClient::ClaudeCode,
            connector
                .connect_mcp_relay(NamedClient::ClaudeCode)
                .context("admit rotated Claude relay")?,
            real_alpaca_evidence.as_ref(),
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
    let real_alpaca_evidence = match real_alpaca_bundle_path {
        Some(_) => Some(real_alpaca_evidence.context("real Alpaca evidence was not retained")?),
        None => None,
    };
    #[cfg(all(feature = "board-installed-fixture", debug_assertions))]
    board_fixture
        .synchronize_provider_clock_for_restart()
        .context("synchronize the retained Board provider clock after clean service shutdown")?;

    #[cfg(all(feature = "board-installed-fixture", debug_assertions))]
    let board_evidence = board_evidence.context("installed Board evidence was not retained")?;
    #[cfg(all(feature = "board-installed-fixture", debug_assertions))]
    let board_counters_before_restart = board_fixture.transport_counters();
    #[cfg(all(feature = "board-installed-fixture", debug_assertions))]
    let restarted = InstalledService::start_with_secret_store_and_board_fixture(
        config.clone(),
        Arc::clone(&secrets),
        board_fixture.clone(),
    )
    .await
    .context("restart installed service with durable Board composition")?;
    #[cfg(not(all(feature = "board-installed-fixture", debug_assertions)))]
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
        #[cfg(all(feature = "board-installed-fixture", debug_assertions))]
        assert_installed_board_restored(
            &restarted_desktop,
            temporary.path(),
            &board_fixture,
            board_counters_before_restart,
            &board_evidence,
        )
        .await
        .context("verify durable Federal Reserve Board state after restart")?;
        let restarted_real_alpaca_evidence = match real_alpaca_evidence.as_ref() {
            Some(initial) => Some(
                assert_real_alpaca_restored(&restarted_desktop, initial)
                    .await
                    .context("verify real Alpaca production state after restart")?,
            ),
            None => None,
        };
        exercise_installed_relay_with_market(
            NamedClient::ClaudeCode,
            connector
                .connect_mcp_relay(NamedClient::ClaudeCode)
                .context("admit persisted Claude relay after restart")?,
            restarted_real_alpaca_evidence.as_ref(),
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct RealAlpacaCredentialAuthority {
    onboarding_session_id: String,
    public_configuration_sha256: String,
    doctor_credential_generation: u64,
    doctor: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RealAlpacaAuthority {
    credential: RealAlpacaCredentialAuthority,
    lifecycle_state_revision: u64,
    runtime_generation_sha256: String,
}

#[derive(Debug)]
struct ActiveRealAlpacaRuntime {
    source_id: String,
    instrument_id: String,
    connection_generation: u64,
}

#[derive(Debug)]
struct ActiveRealAlpacaStatus {
    authority: RealAlpacaAuthority,
    runtimes: Vec<ActiveRealAlpacaRuntime>,
}

#[derive(Debug)]
struct RealAlpacaEvidence {
    authority: RealAlpacaAuthority,
    source_id: String,
    instrument_id: String,
    connection_generation: u64,
    stable_market_identity: Value,
}

#[derive(Debug)]
struct RealAlpacaMarketObservation {
    source_id: String,
    instrument_id: String,
    connection_generation: u64,
    stable_identity: Value,
}

fn protected_real_alpaca_bundle_path() -> TestResult<Option<PathBuf>> {
    let Some(raw) = std::env::var_os(REAL_ALPACA_BUNDLE_PATH_ENV) else {
        return Ok(None);
    };
    if raw.is_empty() {
        anyhow::bail!("real Alpaca credential-bundle path is empty");
    }
    let path = PathBuf::from(raw);
    if !path.is_absolute() {
        anyhow::bail!("real Alpaca credential-bundle path is not absolute");
    }
    let metadata = std::fs::symlink_metadata(&path)
        .context("inspect protected real Alpaca credential bundle")?;
    validate_real_alpaca_bundle_metadata(&metadata)?;
    Ok(Some(path))
}

#[cfg(unix)]
fn validate_real_alpaca_bundle_metadata(metadata: &std::fs::Metadata) -> TestResult {
    use std::os::unix::fs::PermissionsExt as _;

    if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
        anyhow::bail!("real Alpaca credential bundle is not a regular non-symlink file");
    }
    if metadata.permissions().mode() & 0o077 != 0 {
        anyhow::bail!("real Alpaca credential bundle is not mode-private");
    }
    if metadata.len() == 0 || metadata.len() > REAL_ALPACA_CREDENTIAL_MAXIMUM_BYTES {
        anyhow::bail!("real Alpaca credential bundle violates its byte bound");
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_real_alpaca_bundle_metadata(_metadata: &std::fs::Metadata) -> TestResult {
    anyhow::bail!("real Alpaca credential-bundle gate requires Unix mode protections")
}

#[cfg(unix)]
fn read_real_alpaca_bundle(path: &Path) -> TestResult<Zeroizing<Vec<u8>>> {
    use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _};

    let named = std::fs::symlink_metadata(path)
        .context("reinspect protected real Alpaca credential bundle")?;
    validate_real_alpaca_bundle_metadata(&named)?;
    let mut options = std::fs::OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let file = options
        .open(path)
        .context("open protected real Alpaca credential bundle")?;
    let opened = file
        .metadata()
        .context("inspect opened real Alpaca credential bundle")?;
    validate_real_alpaca_bundle_metadata(&opened)?;
    if named.dev() != opened.dev() || named.ino() != opened.ino() || named.len() != opened.len() {
        anyhow::bail!("real Alpaca credential bundle changed during protected open");
    }

    let capacity = usize::try_from(opened.len()).context("size real Alpaca credential bundle")?;
    let mut bytes = Zeroizing::new(Vec::new());
    bytes
        .try_reserve_exact(capacity)
        .context("reserve bounded real Alpaca credential bundle")?;
    file.take(REAL_ALPACA_CREDENTIAL_MAXIMUM_BYTES + 1)
        .read_to_end(&mut bytes)
        .context("read protected real Alpaca credential bundle")?;
    if u64::try_from(bytes.len()).ok() != Some(opened.len()) {
        anyhow::bail!("real Alpaca credential bundle changed during bounded read");
    }
    let after = std::fs::symlink_metadata(path)
        .context("finalize protected real Alpaca credential-bundle read")?;
    validate_real_alpaca_bundle_metadata(&after)?;
    if opened.dev() != after.dev() || opened.ino() != after.ino() || opened.len() != after.len() {
        anyhow::bail!("real Alpaca credential bundle changed during bounded read");
    }
    Ok(bytes)
}

#[cfg(not(unix))]
fn read_real_alpaca_bundle(_path: &Path) -> TestResult<Zeroizing<Vec<u8>>> {
    anyhow::bail!("real Alpaca credential-bundle gate requires Unix mode protections")
}

async fn exercise_real_alpaca_vertical(
    client: &LoopbackApplicationClient,
    bundle_path: &Path,
) -> TestResult<RealAlpacaEvidence> {
    let bytes = read_real_alpaca_bundle(bundle_path)?;
    let admission = InputAdmission::try_sha256(
        REAL_ALPACA_CREDENTIAL_MEDIA_TYPE,
        u64::try_from(bytes.len()).context("measure real Alpaca credential bundle")?,
        Sha256::digest(bytes.as_slice()).into(),
    )
    .context("admit bounded real Alpaca credential bundle")?;
    let ticket = {
        let mut reader = bytes.as_slice();
        client
            .stage_input(admission, &mut reader, CancellationToken::new())
            .await
            .context("stage real Alpaca credential bundle through desktop authority")?
    };
    drop(bytes);

    let imported = invoke_real_alpaca(
        client,
        "import",
        0,
        "Source.ImportCredentialBundle",
        json!({
            "inputTicketId": ticket.id(),
            "confirm": true,
            "resultLimits": {"maximumItems": 32, "maximumBytes": 1_048_576},
        }),
        REAL_ALPACA_LIFECYCLE_TIMEOUT,
    )
    .await?;
    assert_eq!(imported["schema"], REAL_ALPACA_CREDENTIAL_SCHEMA);
    let providers = imported["providers"]
        .as_array()
        .context("real credential import omitted provider dispositions")?;
    let alpaca = exactly_one_value(providers, |provider| provider["provider"] == "alpaca")
        .context("real credential import omitted exact Alpaca disposition")?;
    assert_eq!(alpaca["enabled"], true);
    assert_eq!(alpaca["disposition"], "credential_stored_unverified");
    for provider in providers
        .iter()
        .filter(|provider| provider["provider"] != "alpaca")
    {
        assert_eq!(provider["enabled"], false);
        assert_eq!(provider["disposition"], "disabled");
        assert!(provider["onboardingSessionId"].is_null());
    }
    let onboarding_session_id = required_uuid_string(
        &alpaca["onboardingSessionId"],
        "imported Alpaca onboarding session",
    )?;

    let status = invoke_real_alpaca(
        client,
        "status-after-import",
        0,
        "Source.GetStatus",
        real_alpaca_source_status_arguments(),
        INSTALLED_MCP_SERVICE_TIMEOUT,
    )
    .await?;
    let (import_revision, public_configuration_sha256) =
        assert_real_alpaca_stopped_status(&status, &onboarding_session_id)?;

    let verified = invoke_real_alpaca(
        client,
        "verify",
        0,
        "Source.Verify",
        real_alpaca_lifecycle_arguments(
            import_revision,
            &onboarding_session_id,
            &public_configuration_sha256,
        ),
        REAL_ALPACA_LIFECYCLE_TIMEOUT,
    )
    .await?;
    assert_eq!(verified["provider"], REAL_ALPACA_SURFACE);
    assert_eq!(verified["action"], "verify");
    assert_eq!(verified["disposition"], "applied");
    assert_eq!(verified["state"], "stopped");
    assert!(verified["previousGeneration"].is_null());
    assert!(verified["currentGeneration"].is_null());
    assert!(verified["runtimeGenerationSha256"].is_null());
    assert_eq!(verified["configurationSessionId"], onboarding_session_id);
    assert_eq!(
        verified["publicConfigurationSha256"],
        public_configuration_sha256
    );
    assert_eq!(verified["startEligibility"], "eligible");
    assert!(verified["blocker"].is_null());
    let verify_revision = canonical_positive_u64(
        &verified["stateRevision"],
        "verified Alpaca lifecycle revision",
    )?;
    assert!(verify_revision > import_revision);
    let doctor_credential_generation = assert_real_alpaca_doctor(
        &verified["doctor"],
        &onboarding_session_id,
        &public_configuration_sha256,
    )?;

    let started = invoke_real_alpaca(
        client,
        "start",
        0,
        "Source.Start",
        real_alpaca_lifecycle_arguments(
            verify_revision,
            &onboarding_session_id,
            &public_configuration_sha256,
        ),
        REAL_ALPACA_LIFECYCLE_TIMEOUT,
    )
    .await?;
    assert_eq!(started["provider"], REAL_ALPACA_SURFACE);
    assert_eq!(started["action"], "start");
    assert_eq!(started["disposition"], "applied");
    assert_eq!(started["state"], "active");
    assert!(started["previousGeneration"].is_null());
    assert!(started["currentGeneration"].is_null());
    assert_eq!(started["configurationSessionId"], onboarding_session_id);
    assert_eq!(
        started["publicConfigurationSha256"],
        public_configuration_sha256
    );
    assert_eq!(started["doctor"], verified["doctor"]);
    assert_eq!(started["startEligibility"], "already_active");
    assert!(started["blocker"].is_null());
    let lifecycle_state_revision = canonical_positive_u64(
        &started["stateRevision"],
        "started Alpaca lifecycle revision",
    )?;
    assert!(lifecycle_state_revision > verify_revision);
    let runtime_generation_sha256 = required_sha256(
        &started["runtimeGenerationSha256"],
        "started Alpaca runtime generation",
    )?;
    let expected = RealAlpacaAuthority {
        credential: RealAlpacaCredentialAuthority {
            onboarding_session_id,
            public_configuration_sha256,
            doctor_credential_generation,
            doctor: verified["doctor"].clone(),
        },
        lifecycle_state_revision,
        runtime_generation_sha256,
    };
    await_real_alpaca_market(client, &expected, false).await
}

async fn assert_real_alpaca_restored(
    client: &LoopbackApplicationClient,
    initial: &RealAlpacaEvidence,
) -> TestResult<RealAlpacaEvidence> {
    let restored = await_real_alpaca_market(client, &initial.authority, true).await?;
    assert_eq!(restored.source_id, initial.source_id);
    assert_eq!(restored.instrument_id, initial.instrument_id);
    assert!(restored.connection_generation > initial.connection_generation);
    assert_eq!(
        restored.stable_market_identity,
        initial.stable_market_identity
    );
    Ok(restored)
}

async fn await_real_alpaca_market(
    client: &LoopbackApplicationClient,
    expected: &RealAlpacaAuthority,
    require_new_runtime_generation: bool,
) -> TestResult<RealAlpacaEvidence> {
    let deadline = Instant::now()
        .checked_add(REAL_ALPACA_MARKET_READY_TIMEOUT)
        .context("compute real Alpaca market-readiness deadline")?;
    let mut attempt = 0_u64;
    loop {
        attempt = attempt
            .checked_add(1)
            .context("advance real Alpaca market-readiness attempt")?;
        let result = async {
            let status = invoke_real_alpaca(
                client,
                "status-live",
                attempt,
                "Source.GetStatus",
                real_alpaca_source_status_arguments(),
                real_alpaca_remaining_timeout(deadline)?,
            )
            .await?;
            let status = active_real_alpaca_status(&status)?;
            assert_real_alpaca_authority(
                &status.authority,
                expected,
                require_new_runtime_generation,
            )?;
            let instrument_ids = status
                .runtimes
                .iter()
                .map(|runtime| runtime.instrument_id.clone())
                .collect::<Vec<_>>();
            let market = invoke_real_alpaca(
                client,
                "market-native",
                attempt,
                "Market.GetUnifiedFeed",
                real_alpaca_market_arguments(&instrument_ids),
                real_alpaca_remaining_timeout(deadline)?,
            )
            .await?;
            let market = real_alpaca_market_observation(&market)?;
            let runtime = status
                .runtimes
                .iter()
                .find(|runtime| {
                    runtime.source_id == market.source_id
                        && runtime.instrument_id == market.instrument_id
                        && runtime.connection_generation == market.connection_generation
                })
                .context("native AAPL market evidence did not match current Source authority")?;
            Ok::<_, anyhow::Error>(RealAlpacaEvidence {
                authority: status.authority,
                source_id: runtime.source_id.clone(),
                instrument_id: runtime.instrument_id.clone(),
                connection_generation: runtime.connection_generation,
                stable_market_identity: market.stable_identity,
            })
        }
        .await;
        match result {
            Ok(evidence) => return Ok(evidence),
            Err(error) if Instant::now() >= deadline => {
                anyhow::bail!("real Alpaca market readiness missed its deadline: {error:#}");
            }
            Err(_) => {}
        }
        tokio::time::sleep(
            Duration::from_millis(100).min(deadline.saturating_duration_since(Instant::now())),
        )
        .await;
    }
}

fn real_alpaca_remaining_timeout(deadline: Instant) -> TestResult<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        anyhow::bail!("real Alpaca market-readiness deadline elapsed");
    }
    Ok(remaining.min(INSTALLED_MCP_SERVICE_TIMEOUT))
}

async fn invoke_real_alpaca(
    client: &LoopbackApplicationClient,
    phase: &str,
    attempt: u64,
    operation: &'static str,
    arguments: Value,
    timeout: Duration,
) -> TestResult<Value> {
    let response = client
        .invoke_operation(
            RequestId::try_string(format!("real-alpaca-{phase}-{attempt}"))
                .context("construct real Alpaca request ID")?,
            operation,
            arguments,
            timeout,
            CancellationToken::new(),
        )
        .await
        .with_context(|| format!("invoke {operation} for real Alpaca production evidence"))?;
    if response.result()["ok"] != true {
        anyhow::bail!(
            "{operation} rejected the real Alpaca production request: {}",
            response.result()
        );
    }
    Ok(response.result()["value"]["data"].clone())
}

fn real_alpaca_source_status_arguments() -> Value {
    json!({
        "sourceCoverage": [REAL_ALPACA_SURFACE],
        "resultLimits": {"maximumItems": 32, "maximumBytes": 1_048_576},
    })
}

fn real_alpaca_lifecycle_arguments(
    expected_state_revision: u64,
    onboarding_session_id: &str,
    public_configuration_sha256: &str,
) -> Value {
    json!({
        "provider": REAL_ALPACA_SURFACE,
        "expectedStateRevision": expected_state_revision.to_string(),
        "onboardingSessionId": onboarding_session_id,
        "publicConfigurationSha256": public_configuration_sha256,
        "sourceCoverage": [REAL_ALPACA_SURFACE],
        "confirm": true,
        "resultLimits": {"maximumItems": 32, "maximumBytes": 1_048_576},
    })
}

fn real_alpaca_market_arguments(instrument_ids: &[String]) -> Value {
    json!({
        "instrumentIds": instrument_ids,
        "sourceCoverage": [REAL_ALPACA_SURFACE],
        "resultLimits": {"maximumItems": 32, "maximumBytes": 1_048_576},
    })
}

fn assert_real_alpaca_stopped_status(
    data: &Value,
    onboarding_session_id: &str,
) -> TestResult<(u64, String)> {
    let rows = data
        .as_array()
        .context("real Alpaca status after import was not an array")?;
    if rows.len() != 1 {
        anyhow::bail!("real Alpaca status after import was not exact-scoped");
    }
    let row = &rows[0];
    assert_eq!(row["profile"]["id"], REAL_ALPACA_SURFACE);
    assert_eq!(row["currentSession"]["session_id"], onboarding_session_id);
    assert_eq!(row["currentSession"]["credential_stored"], true);
    assert_eq!(row["currentSession"]["state"], "stored_unverified");
    assert_eq!(row["lifecycleSupport"], "managed");
    assert_eq!(row["runtime"]["state"], "not_active");
    let lifecycle = &row["lifecycle"];
    assert_eq!(lifecycle["provider"], REAL_ALPACA_SURFACE);
    assert_eq!(lifecycle["state"], "stopped");
    assert_eq!(lifecycle["configurationSessionId"], onboarding_session_id);
    assert!(lifecycle["currentGeneration"].is_null());
    assert!(lifecycle["runtimeGenerationSha256"].is_null());
    assert!(lifecycle["doctor"].is_null());
    assert_eq!(lifecycle["startEligibility"], "doctor_required");
    assert!(lifecycle["blocker"].is_null());
    Ok((
        canonical_positive_u64(
            &lifecycle["stateRevision"],
            "imported Alpaca lifecycle revision",
        )?,
        required_sha256(
            &lifecycle["publicConfigurationSha256"],
            "imported Alpaca public configuration",
        )?,
    ))
}

fn active_real_alpaca_status(data: &Value) -> TestResult<ActiveRealAlpacaStatus> {
    let rows = data
        .as_array()
        .context("active real Alpaca status was not an array")?;
    if rows.is_empty() {
        anyhow::bail!("active real Alpaca status omitted its IEX runtimes");
    }
    let mut authority = None;
    let mut runtimes = Vec::new();
    runtimes
        .try_reserve_exact(rows.len())
        .context("reserve active real Alpaca runtime evidence")?;
    for row in rows {
        assert_eq!(row["profile"]["id"], REAL_ALPACA_SURFACE);
        assert_eq!(row["lifecycleSupport"], "managed");
        let lifecycle = &row["lifecycle"];
        assert_eq!(lifecycle["provider"], REAL_ALPACA_SURFACE);
        assert_eq!(lifecycle["state"], "active");
        assert!(lifecycle["currentGeneration"].is_null());
        assert_eq!(lifecycle["startEligibility"], "already_active");
        assert!(lifecycle["blocker"].is_null());
        let onboarding_session_id = required_uuid_string(
            &lifecycle["configurationSessionId"],
            "active Alpaca configuration session",
        )?;
        let public_configuration_sha256 = required_sha256(
            &lifecycle["publicConfigurationSha256"],
            "active Alpaca public configuration",
        )?;
        let doctor_credential_generation = assert_real_alpaca_doctor(
            &lifecycle["doctor"],
            &onboarding_session_id,
            &public_configuration_sha256,
        )?;
        let candidate = RealAlpacaAuthority {
            credential: RealAlpacaCredentialAuthority {
                onboarding_session_id,
                public_configuration_sha256,
                doctor_credential_generation,
                doctor: lifecycle["doctor"].clone(),
            },
            lifecycle_state_revision: canonical_positive_u64(
                &lifecycle["stateRevision"],
                "active Alpaca lifecycle revision",
            )?,
            runtime_generation_sha256: required_sha256(
                &lifecycle["runtimeGenerationSha256"],
                "active Alpaca runtime generation",
            )?,
        };
        if let Some(authority) = authority.as_ref() {
            if authority != &candidate {
                anyhow::bail!("active Alpaca child runtimes disagreed on account authority");
            }
        } else {
            authority = Some(candidate);
        }
        assert_eq!(
            row["currentSession"]["session_id"],
            lifecycle["configurationSessionId"]
        );
        assert_eq!(row["currentSession"]["state"], "active_scoped");
        assert_eq!(row["currentSession"]["credential_stored"], true);
        assert_eq!(
            row["currentSession"]["active_generation"],
            doctor_credential_generation
        );
        let runtime = &row["runtime"];
        assert_eq!(runtime["state"], "active");
        assert_eq!(runtime["venueId"], "iex");
        let source_id = required_nonempty_string(&runtime["sourceId"], "Alpaca source ID")?;
        let instrument_id = required_uuid_string(&runtime["instrumentId"], "Alpaca instrument ID")?;
        let connection_generation = canonical_positive_u64(
            &runtime["connectionGeneration"],
            "Alpaca connection generation",
        )?;
        required_sha256(&runtime["bindingDigest"], "Alpaca runtime binding")?;
        if runtimes.iter().any(|prior: &ActiveRealAlpacaRuntime| {
            prior.source_id == source_id && prior.instrument_id == instrument_id
        }) {
            anyhow::bail!("active Alpaca status duplicated one source instrument");
        }
        runtimes.push(ActiveRealAlpacaRuntime {
            source_id,
            instrument_id,
            connection_generation,
        });
    }
    Ok(ActiveRealAlpacaStatus {
        authority: authority.context("active Alpaca status omitted account authority")?,
        runtimes,
    })
}

fn assert_real_alpaca_authority(
    actual: &RealAlpacaAuthority,
    expected: &RealAlpacaAuthority,
    require_new_runtime_generation: bool,
) -> TestResult {
    if actual.credential != expected.credential
        || actual.lifecycle_state_revision != expected.lifecycle_state_revision
    {
        anyhow::bail!("restored Alpaca credential or durable lifecycle authority changed");
    }
    if require_new_runtime_generation {
        if actual.runtime_generation_sha256 == expected.runtime_generation_sha256 {
            anyhow::bail!("restored Alpaca runtime did not mint a new group generation");
        }
    } else if actual.runtime_generation_sha256 != expected.runtime_generation_sha256 {
        anyhow::bail!("started Alpaca runtime disagreed with its lifecycle receipt");
    }
    Ok(())
}

fn assert_real_alpaca_doctor(
    doctor: &Value,
    onboarding_session_id: &str,
    public_configuration_sha256: &str,
) -> TestResult<u64> {
    assert_eq!(doctor["schema"], "market-squawk.alpaca-paper-iex-doctor/v1");
    assert_eq!(doctor["surfaceId"], REAL_ALPACA_SURFACE);
    assert_eq!(doctor["onboardingSessionId"], onboarding_session_id);
    assert_eq!(doctor["realm"], "paper");
    assert_eq!(
        doctor["publicConfigurationSha256"],
        public_configuration_sha256
    );
    assert_eq!(doctor["dataQuality"], "direct_unverified");
    assert_eq!(doctor["current"], true);
    required_sha256(&doctor["receiptSha256"], "Alpaca doctor receipt")?;
    let generation = canonical_positive_u64(
        &doctor["credentialGeneration"],
        "Alpaca doctor credential generation",
    )?;
    for capability in [
        "iexLatestQuote",
        "iexSnapshotBatch",
        "iexWebSocket",
        "iexHistoricalBars",
        "iexUtcCalendar",
    ] {
        let probe = &doctor["capabilities"][capability];
        if !matches!(
            probe["disposition"].as_str(),
            Some("available" | "degraded")
        ) {
            anyhow::bail!("Alpaca doctor did not complete the {capability} production probe");
        }
        required_sha256(
            &probe["evidenceSha256"],
            "Alpaca doctor capability evidence",
        )?;
        if !probe["observation"].is_object() {
            anyhow::bail!("Alpaca doctor omitted the {capability} production observation");
        }
    }
    Ok(generation)
}

fn real_alpaca_market_observation(data: &Value) -> TestResult<RealAlpacaMarketObservation> {
    let rows = data
        .as_array()
        .context("real Alpaca unified Market result was not an array")?;
    let row = exactly_one_value(rows, |row| {
        row["symbol"] == "AAPL" && row["selectedSource"]["surfaceId"] == REAL_ALPACA_SURFACE
    })
    .context("real Alpaca unified Market result omitted exact live AAPL evidence")?;
    assert_eq!(row["symbolKind"], "provider_subscription_symbol");
    assert_eq!(row["symbolVenueId"], "iex");
    assert_eq!(row["assetClass"], "equity");
    assert_eq!(row["quoteCurrency"], "USD");
    assert_eq!(row["definitionKind"], "market_data");
    assert_eq!(row["executionTermsAvailable"], false);
    assert!(row["orderBook"].is_null());
    assert_eq!(row["availability"], "Live");
    assert_eq!(row["confidence"], "Direct, unverified");
    required_nonempty_string(&row["permanentFigi"], "AAPL permanent FIGI")?;
    for field in ["bidPrice", "askPrice", "midPrice"] {
        required_nonempty_string(&row["quote"][field], "AAPL live quote value")?;
    }
    assert_eq!(
        row["quote"]["midPriceBasis"],
        "calculated_from_selected_bid_and_ask"
    );
    if !row["quote"]["quoteEvidence"].is_object() {
        anyhow::bail!("real Alpaca AAPL quote omitted provider receipt evidence");
    }
    let selected = &row["selectedSource"];
    assert_eq!(selected["providerId"], "alpaca");
    assert_eq!(selected["providerSymbol"], "AAPL");
    assert_eq!(selected["venueId"], "iex");
    assert_eq!(selected["quality"], "direct_unverified");
    assert_eq!(selected["health"], "healthy");
    assert_eq!(selected["rights"]["state"], "admitted");
    assert_eq!(selected["rights"]["snapshotDisplayPermitted"], true);
    assert_eq!(selected["freshness"]["freshAtSelection"], true);
    let connection_generation = canonical_positive_u64(
        &selected["integrity"]["connectionGeneration"],
        "selected AAPL connection generation",
    )?;
    let instrument_id = required_uuid_string(&row["instrumentId"], "selected AAPL instrument")?;
    let source_id = required_nonempty_string(&selected["sourceId"], "selected AAPL source")?;
    let observation = &row["marketObservation"];
    assert_eq!(observation["availability"], "available");
    assert_eq!(observation["instrumentId"], instrument_id);
    assert_eq!(observation["mark"]["basis"], "fresh_bid_ask_midpoint");
    assert_eq!(observation["mark"]["value"], row["quote"]["midPrice"]);
    assert_eq!(
        canonical_positive_u64(
            &observation["generation"],
            "AAPL investment observation generation",
        )?,
        connection_generation
    );
    Ok(RealAlpacaMarketObservation {
        source_id,
        instrument_id,
        connection_generation,
        stable_identity: json!({
            "instrumentId": row["instrumentId"],
            "symbol": row["symbol"],
            "symbolKind": row["symbolKind"],
            "symbolVenueId": row["symbolVenueId"],
            "assetClass": row["assetClass"],
            "quoteCurrency": row["quoteCurrency"],
            "definitionKind": row["definitionKind"],
            "permanentFigi": row["permanentFigi"],
            "surfaceId": selected["surfaceId"],
            "providerId": selected["providerId"],
            "providerSymbol": selected["providerSymbol"],
            "sourceId": selected["sourceId"],
            "venueId": selected["venueId"],
            "providerProduct": selected["providerProduct"],
            "providerChannel": selected["providerChannel"],
        }),
    })
}

fn assert_real_alpaca_mcp_market(data: &Value, expected: &RealAlpacaEvidence) -> TestResult {
    let observed = real_alpaca_market_observation(data)?;
    assert_eq!(observed.source_id, expected.source_id);
    assert_eq!(observed.instrument_id, expected.instrument_id);
    assert!(observed.connection_generation >= expected.connection_generation);
    assert_eq!(observed.stable_identity, expected.stable_market_identity);
    Ok(())
}

fn exactly_one_value<'a>(
    values: &'a [Value],
    predicate: impl Fn(&Value) -> bool,
) -> Option<&'a Value> {
    let mut matches = values.iter().filter(|value| predicate(value));
    let selected = matches.next()?;
    matches.next().is_none().then_some(selected)
}

fn required_nonempty_string(value: &Value, label: &str) -> TestResult<String> {
    value
        .as_str()
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .with_context(|| format!("{label} was not a nonempty string"))
}

fn required_uuid_string(value: &Value, label: &str) -> TestResult<String> {
    let value = required_nonempty_string(value, label)?;
    uuid::Uuid::parse_str(&value).with_context(|| format!("{label} was not a UUID"))?;
    Ok(value)
}

fn canonical_positive_u64(value: &Value, label: &str) -> TestResult<u64> {
    let value = value
        .as_str()
        .with_context(|| format!("{label} was not a decimal string"))?;
    if value.is_empty()
        || value.starts_with('0')
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        anyhow::bail!("{label} was not a canonical positive decimal string");
    }
    value
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .with_context(|| format!("{label} was outside the positive u64 range"))
}

fn required_sha256(value: &Value, label: &str) -> TestResult<String> {
    let value = value
        .as_str()
        .with_context(|| format!("{label} was not a SHA-256 string"))?;
    if value.len() != 64
        || value.bytes().all(|byte| byte == b'0')
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        anyhow::bail!("{label} was not a canonical nonzero SHA-256 string");
    }
    Ok(value.to_owned())
}

#[cfg(all(feature = "board-installed-fixture", debug_assertions))]
#[derive(Clone, Debug, Eq, PartialEq)]
struct InstalledBoardFileEvidence {
    relative_path: PathBuf,
    bytes: u64,
    sha256: String,
}

#[cfg(all(feature = "board-installed-fixture", debug_assertions))]
#[derive(Debug)]
struct InstalledBoardEvidence {
    manifest: Value,
    history_artifact: Value,
    dashboard_stable: Value,
    msj: Vec<InstalledBoardFileEvidence>,
    parquet: Vec<InstalledBoardFileEvidence>,
}

#[cfg(all(feature = "board-installed-fixture", debug_assertions))]
fn installed_board_fixture() -> TestResult<BoardInstalledFixtureBundle> {
    let doctor_dates = [
        "2026-07-27",
        "2026-07-28",
        "2026-07-29",
        "2026-07-30",
        "2026-07-31",
        "2026-08-03",
        "2026-08-04",
        "2026-08-05",
        "2026-08-06",
        "2026-08-07",
    ];
    let production_dates = installed_board_rolling_dates()?;
    let doctor = BoardScriptedCsvResponse::try_new(
        installed_board_csv(&doctor_dates, false)?,
        Duration::from_millis(1),
    )
    .context("construct exact 11-series by 10-row Board doctor response")?;
    let production = BoardScriptedCsvResponse::try_new(
        installed_board_csv(&production_dates, true)?,
        Duration::from_millis(1),
    )
    .context("construct exact 11-series by 100-date rolling Board production response")?;
    let transport = BoardScriptedTransportFactory::try_new(doctor, production)
        .context("validate separate Board doctor and production responses")?;
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("observe initial Board provider-rate wall clock")?;
    let unix_nanos = i64::try_from(elapsed.as_nanos())
        .context("convert initial Board provider-rate wall clock")?;
    let initial_wall_clock = unix_nanos
        .checked_sub(60_000_000_000)
        .context("place the Board provider-rate fixture exactly one minute before real time")?;
    Ok(BoardInstalledFixtureBundle::new(
        transport,
        Timestamp::from_unix_nanos(initial_wall_clock),
    ))
}

#[cfg(all(feature = "board-installed-fixture", debug_assertions))]
fn installed_board_rolling_dates() -> TestResult<Vec<String>> {
    let mut date = NaiveDate::from_ymd_opt(2026, 8, 10)
        .context("construct final rolling Board fixture date")?;
    let mut dates = Vec::new();
    dates
        .try_reserve_exact(BOARD_H15_TREASURY_CONSTANT_MATURITIES_ROLLING_DASHBOARD_DATE_COUNT)
        .context("reserve rolling Board fixture dates")?;
    while dates.len() < BOARD_H15_TREASURY_CONSTANT_MATURITIES_ROLLING_DASHBOARD_DATE_COUNT {
        if date.weekday().number_from_monday() <= 5 {
            dates.push(date.to_string());
        }
        date = date
            .pred_opt()
            .context("walk backward through rolling Board fixture dates")?;
    }
    dates.reverse();
    Ok(dates)
}

#[cfg(all(feature = "board-installed-fixture", debug_assertions))]
fn installed_board_profile() -> TestResult<BoardDatasetProfile> {
    BoardDatasetProfile::h15_treasury_constant_maturities_rolling_dashboard()
        .context("construct rolling Board H.15 dashboard profile")
}

#[cfg(all(feature = "board-installed-fixture", debug_assertions))]
fn installed_board_csv<S: AsRef<str>>(
    dates: &[S],
    latest_twenty_year_missing: bool,
) -> TestResult<String> {
    let profile = installed_board_profile()?;
    let series = profile
        .contract()
        .series_scope()
        .exact_series()
        .context("Board H.15 contract did not retain its exact series")?;
    assert_eq!(series.len(), 11);

    let mut rows = Vec::new();
    rows.push(
        std::iter::once("Series Description".to_owned())
            .chain(series.iter().map(|item| item.series_name().to_owned()))
            .collect::<Vec<_>>(),
    );
    rows.push(
        std::iter::once("Unit:".to_owned())
            .chain(series.iter().map(|item| item.unit().to_owned()))
            .collect::<Vec<_>>(),
    );
    rows.push(
        std::iter::once("Multiplier:".to_owned())
            .chain(series.iter().map(|item| item.multiplier().to_string()))
            .collect::<Vec<_>>(),
    );
    rows.push(
        std::iter::once("Currency:".to_owned())
            .chain(series.iter().map(|item| item.currency().to_owned()))
            .collect::<Vec<_>>(),
    );
    rows.push(
        std::iter::once("Unique Identifier: ".to_owned())
            .chain(series.iter().map(|item| item.unique_id().to_owned()))
            .collect::<Vec<_>>(),
    );
    rows.push(
        std::iter::once("Time Period".to_owned())
            .chain(series.iter().map(|item| item.series_name().to_owned()))
            .collect::<Vec<_>>(),
    );
    for (date_index, date) in dates.iter().enumerate() {
        let latest = date_index + 1 == dates.len();
        let mut row = Vec::with_capacity(series.len() + 1);
        row.push(date.as_ref().to_owned());
        for series_index in 0..series.len() {
            let value = if latest_twenty_year_missing && latest && series_index == 9 {
                "ND".to_owned()
            } else if latest_twenty_year_missing && !latest && series_index == 9 {
                "4.60".to_owned()
            } else {
                format!("4.{:02}", (series_index + date_index) % 100)
            };
            row.push(value);
        }
        rows.push(row);
    }
    let mut csv = rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|field| format!("\"{}\"", field.replace('"', "\"\"")))
                .collect::<Vec<_>>()
                .join(",")
        })
        .collect::<Vec<_>>()
        .join("\n");
    csv.push('\n');
    Ok(csv)
}

#[cfg(all(feature = "board-installed-fixture", debug_assertions))]
async fn exercise_installed_board_vertical(
    client: &LoopbackApplicationClient,
    installation_root: &Path,
    fixture: &BoardInstalledFixtureBundle,
) -> TestResult<InstalledBoardEvidence> {
    let board_profile = installed_board_profile()?;
    let board_provider_dataset = board_profile.dataset().as_str();
    let board_analytical_dataset = board_profile.analytical_dataset().as_str();
    let before_msj = installed_file_evidence(installation_root, "msj")?;
    let before_parquet = installed_file_evidence(installation_root, "parquet")?;
    let registered = invoke_installed_board(
        client,
        "register",
        "Source.Register",
        json!({
            "provider": BOARD_SURFACE,
            "sourceCoverage": [BOARD_SURFACE],
            "confirm": true,
            "resultLimits": {"maximumItems": 16, "maximumBytes": 1_048_576},
        }),
    )
    .await?;
    let profile = &registered["profile"];
    assert_eq!(profile["id"], BOARD_SURFACE);
    assert_eq!(profile["capability_revision"], 4);
    assert_eq!(profile["selected_setup_mode"], "no_credential");
    assert_eq!(profile["credential_kind"], "none");
    assert_eq!(profile["release_state"], "available");

    let setup = invoke_installed_board(
        client,
        "setup",
        "Source.Setup",
        json!({
            "provider": BOARD_SURFACE,
            "sourceCoverage": [BOARD_SURFACE],
            "confirm": true,
            "resultLimits": {"maximumItems": 16, "maximumBytes": 1_048_576},
        }),
    )
    .await?;
    let portal = setup["portal"]["url"]
        .as_str()
        .context("Board setup did not return its local portal")?;
    let http = reqwest::Client::new();
    let bootstrap_response = http
        .get(format!("{portal}/api/v1/bootstrap"))
        .send()
        .await
        .context("request Board portal bootstrap")?;
    let cookie = bootstrap_response
        .headers()
        .get(reqwest::header::SET_COOKIE)
        .context("Board portal bootstrap did not issue a session cookie")?
        .to_str()
        .context("decode Board portal session cookie")?
        .split(';')
        .next()
        .context("Board portal session cookie was empty")?
        .to_owned();
    let bootstrap: Value = bootstrap_response
        .json()
        .await
        .context("decode Board portal bootstrap")?;
    let csrf = bootstrap["csrf_token"]
        .as_str()
        .context("Board portal bootstrap omitted its CSRF token")?;
    let board_profile = bootstrap["profiles"]
        .as_array()
        .and_then(|profiles| profiles.iter().find(|item| item["id"] == BOARD_SURFACE))
        .context("Board portal bootstrap omitted the rev4 profile")?;
    assert_eq!(board_profile["capability_revision"], 4);
    assert_eq!(board_profile["selected_setup_mode"], "no_credential");
    assert_eq!(board_profile["credential_kind"], "none");

    let started_response = http
        .post(format!("{portal}/api/v1/sessions"))
        .header(reqwest::header::COOKIE, &cookie)
        .header(reqwest::header::ORIGIN, portal)
        .header("x-csrf-token", csrf)
        .json(&json!({"surface_id": BOARD_SURFACE}))
        .send()
        .await
        .context("run Board no-key doctor")?;
    assert_eq!(started_response.status(), reqwest::StatusCode::OK);
    let started: Value = started_response
        .json()
        .await
        .context("decode Board no-key doctor result")?;
    let session_id = started["session_id"]
        .as_str()
        .context("Board doctor omitted its durable session")?;
    assert_eq!(started["surface_id"], BOARD_SURFACE);
    assert_eq!(started["capability_revision"], 4);
    assert_eq!(started["credential_stored"], false);
    assert!(started["active_generation"].is_null());
    assert!(started["candidate_generation"].is_null());
    assert_eq!(started["generations"], json!([]));
    assert_eq!(started["public_configuration"], json!({}));
    assert_installed_board_transport_counts(fixture.transport_counters(), 1, 1, 0, 0);

    let activated_response = http
        .post(format!("{portal}/api/v1/sessions/{session_id}/activate"))
        .header(reqwest::header::COOKIE, &cookie)
        .header(reqwest::header::ORIGIN, portal)
        .header("x-csrf-token", csrf)
        .json(&json!({"kind": "federal_reserve_board_h15"}))
        .send()
        .await
        .context("activate Board production source")?;
    assert_eq!(activated_response.status(), reqwest::StatusCode::OK);
    let activated: Value = activated_response
        .json()
        .await
        .context("decode Board production activation")?;
    assert_eq!(activated["profile"], BOARD_SURFACE);
    assert_eq!(
        activated["provider_dataset_identifier"],
        board_provider_dataset
    );
    assert_eq!(activated["capability_revision"], 4);
    assert!(activated["credential_generation"].is_null());

    let refused = client
        .invoke_operation(
            RequestId::try_string("installed-board-discover-rate-refusal")
                .context("construct refused Board discovery request ID")?,
            "Source.Discover",
            json!({
                "provider": BOARD_SURFACE,
                "dataset": board_provider_dataset,
                "sourceCoverage": [BOARD_SURFACE],
                "confirm": true,
                "resultLimits": {"maximumItems": 16, "maximumBytes": 1_048_576},
            }),
            Duration::from_secs(5),
            CancellationToken::new(),
        )
        .await
        .context("dispatch refused Board discovery")?;
    assert_eq!(refused.result()["ok"], false, "{}", refused.result());
    assert_installed_board_transport_counts(fixture.transport_counters(), 1, 1, 0, 0);

    fixture
        .advance_provider_clock(Duration::from_secs(60))
        .context("advance the shared Board provider clock by exactly one minute")?;
    let discovery_response = client
        .invoke_operation(
            RequestId::try_string("installed-board-discover-after-minute")
                .context("construct admitted Board discovery request ID")?,
            "Source.Discover",
            json!({
                "provider": BOARD_SURFACE,
                "dataset": board_provider_dataset,
                "sourceCoverage": [BOARD_SURFACE],
                "confirm": true,
                "resultLimits": {"maximumItems": 16, "maximumBytes": 1_048_576},
            }),
            INSTALLED_MCP_SERVICE_TIMEOUT,
            CancellationToken::new(),
        )
        .await
        .context("dispatch admitted Board discovery")?;
    assert_eq!(
        discovery_response.result()["ok"],
        true,
        "{}; transport={:?}",
        discovery_response.result(),
        fixture.transport_counters()
    );
    let discovery = discovery_response.result()["value"]["data"].clone();
    assert_eq!(discovery["profile"], BOARD_SURFACE);
    assert_eq!(discovery["receipts_survive_restart"], false);
    let objects = discovery["objects"]
        .as_array()
        .context("Board discovery did not return an object array")?;
    assert_eq!(objects.len(), 1);
    let object = &objects[0];
    assert_eq!(object["dataset"], board_provider_dataset);
    assert_installed_board_transport_counts(fixture.transport_counters(), 1, 1, 1, 1);

    let ingest = invoke_installed_board(
        client,
        "ingest",
        "Research.IngestSource",
        json!({
            "provider": BOARD_SURFACE,
            "object": object["object_id"],
            "dataset": board_provider_dataset,
            "discoveryReceipt": object["discovery_receipt"],
            "sourceCoverage": [BOARD_SURFACE],
            "confirm": true,
            "resultLimits": {"maximumItems": 64, "maximumBytes": 1_048_576},
        }),
    )
    .await?;
    assert_eq!(ingest["manifest"]["datasetId"], board_analytical_dataset);
    assert_eq!(
        ingest["rowCount"],
        BOARD_H15_TREASURY_CONSTANT_MATURITIES_ROLLING_DASHBOARD_OBSERVATION_COUNT
    );
    assert_eq!(ingest["objectCount"], 1);
    assert!(ingest["totalBytes"].as_u64().is_some_and(|bytes| bytes > 0));
    assert!(
        ingest["lineageDigest"]
            .as_str()
            .is_some_and(|value| value.len() == 64)
    );
    assert_installed_board_transport_counts(fixture.transport_counters(), 1, 1, 1, 1);

    let manifest = invoke_installed_board(
        client,
        "manifest",
        "Research.GetManifest",
        json!({
            "dataset": board_analytical_dataset,
            "resultLimits": {"maximumItems": 64, "maximumBytes": 1_048_576},
        }),
    )
    .await?;
    assert_eq!(manifest["manifest"]["datasetId"], board_analytical_dataset);
    assert_eq!(
        manifest["rowCount"],
        BOARD_H15_TREASURY_CONSTANT_MATURITIES_ROLLING_DASHBOARD_OBSERVATION_COUNT
    );
    assert_eq!(manifest["objectCount"], 1);

    let history = invoke_installed_board(
        client,
        "history",
        "Research.GetHistory",
        json!({
            "dataset": board_analytical_dataset,
            "resultLimits": {
                "maximumItems": BOARD_H15_TREASURY_CONSTANT_MATURITIES_ROLLING_DASHBOARD_OBSERVATION_COUNT,
                "maximumBytes": INSTALLED_BOARD_HISTORY_MAXIMUM_BYTES
            },
        }),
    )
    .await?;
    assert_eq!(history["manifest"], manifest["manifest"]);
    let history_artifact = stable_installed_board_history(&history)?;

    let dashboard = installed_board_dashboard(client, "initial").await?;
    assert_installed_board_dashboard(&dashboard)?;
    let dashboard_stable = stable_installed_board_dashboard(&dashboard);

    let msj = new_installed_files(
        &before_msj,
        installed_file_evidence(installation_root, "msj")?,
    );
    let parquet = new_installed_files(
        &before_parquet,
        installed_file_evidence(installation_root, "parquet")?,
    );
    assert_eq!(msj.len(), 1, "expected one new sealed Board MSJ1 object");
    assert!(
        !parquet.is_empty(),
        "expected durable Board Parquet evidence"
    );
    assert_file_magic(installation_root, &msj[0], b"MSJ1", None)?;
    for file in &parquet {
        assert_file_magic(installation_root, file, b"PAR1", Some(b"PAR1"))?;
    }

    Ok(InstalledBoardEvidence {
        manifest,
        history_artifact,
        dashboard_stable,
        msj,
        parquet,
    })
}

#[cfg(all(feature = "board-installed-fixture", debug_assertions))]
async fn assert_installed_board_restored(
    client: &LoopbackApplicationClient,
    installation_root: &Path,
    fixture: &BoardInstalledFixtureBundle,
    counters_before_restart: BoardScriptedTransportCounters,
    evidence: &InstalledBoardEvidence,
) -> TestResult {
    let board_profile = installed_board_profile()?;
    let board_provider_dataset = board_profile.dataset().as_str();
    let board_analytical_dataset = board_profile.analytical_dataset().as_str();
    assert_eq!(fixture.transport_counters(), counters_before_restart);
    let status = invoke_installed_board(
        client,
        "status-after-restart",
        "Source.GetStatus",
        json!({
            "sourceCoverage": [BOARD_SURFACE],
            "resultLimits": {"maximumItems": 16, "maximumBytes": 1_048_576},
        }),
    )
    .await?;
    let rows = status
        .as_array()
        .context("restored Board source status was not an array")?;
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row["profile"]["id"], BOARD_SURFACE);
    assert_eq!(row["profile"]["capability_revision"], 4);
    assert_eq!(row["currentSession"]["state"], "active_scoped");
    assert_eq!(row["currentSession"]["credential_stored"], false);
    assert!(row["currentSession"]["active_generation"].is_null());
    assert!(row["currentSession"]["candidate_generation"].is_null());
    assert_eq!(row["currentSession"]["generations"], json!([]));
    assert_eq!(row["providerDatasetIdentifier"], board_provider_dataset);
    assert_eq!(row["lifecycleSupport"], "managed");
    assert_eq!(row["runtime"]["state"], "not_active");

    let manifest = invoke_installed_board(
        client,
        "manifest-after-restart",
        "Research.GetManifest",
        json!({
            "dataset": board_analytical_dataset,
            "resultLimits": {"maximumItems": 64, "maximumBytes": 1_048_576},
        }),
    )
    .await?;
    assert_eq!(manifest, evidence.manifest);
    let history = invoke_installed_board(
        client,
        "history-after-restart",
        "Research.GetHistory",
        json!({
            "dataset": board_analytical_dataset,
            "resultLimits": {
                "maximumItems": BOARD_H15_TREASURY_CONSTANT_MATURITIES_ROLLING_DASHBOARD_OBSERVATION_COUNT,
                "maximumBytes": INSTALLED_BOARD_HISTORY_MAXIMUM_BYTES
            },
        }),
    )
    .await?;
    assert_eq!(
        stable_installed_board_history(&history)?,
        evidence.history_artifact
    );
    let dashboard = installed_board_dashboard(client, "after-restart").await?;
    assert_installed_board_dashboard(&dashboard)?;
    assert_eq!(
        stable_installed_board_dashboard(&dashboard),
        evidence.dashboard_stable
    );
    assert_installed_file_evidence(installation_root, &evidence.msj)?;
    assert_installed_file_evidence(installation_root, &evidence.parquet)?;
    assert_eq!(fixture.transport_counters(), counters_before_restart);
    Ok(())
}

#[cfg(all(feature = "board-installed-fixture", debug_assertions))]
async fn invoke_installed_board(
    client: &LoopbackApplicationClient,
    request_suffix: &str,
    operation: &str,
    arguments: Value,
) -> TestResult<Value> {
    let response = client
        .invoke_operation(
            RequestId::try_string(format!("installed-board-{request_suffix}"))
                .context("construct installed Board request ID")?,
            operation,
            arguments,
            INSTALLED_MCP_SERVICE_TIMEOUT,
            CancellationToken::new(),
        )
        .await
        .with_context(|| format!("invoke {operation} for installed Board proof"))?;
    assert_eq!(
        response.result()["ok"],
        true,
        "{operation} ({request_suffix}) failed: {}",
        response.result()
    );
    Ok(response.result()["value"]["data"].clone())
}

#[cfg(all(feature = "board-installed-fixture", debug_assertions))]
async fn installed_board_dashboard(
    client: &LoopbackApplicationClient,
    request_suffix: &str,
) -> TestResult<Value> {
    invoke_installed_board(
        client,
        &format!("dashboard-{request_suffix}"),
        "Macro.GetDashboard",
        json!({
            "provider": BOARD_SURFACE,
            "release": "h15",
            "resultLimits": {"maximumItems": 64, "maximumBytes": 1_048_576},
        }),
    )
    .await
}

#[cfg(all(feature = "board-installed-fixture", debug_assertions))]
fn assert_installed_board_dashboard(dashboard: &Value) -> TestResult {
    let board_profile = installed_board_profile()?;
    assert_eq!(
        dashboard["schemaIdentity"],
        "market-squawk-macro-dashboard/v1"
    );
    assert_eq!(dashboard["binding"]["surfaceId"], BOARD_SURFACE);
    assert_eq!(
        dashboard["binding"]["providerDatasetId"],
        board_profile.dataset().as_str()
    );
    assert_eq!(
        dashboard["binding"]["analyticalDatasetId"],
        board_profile.analytical_dataset().as_str()
    );
    assert_eq!(dashboard["release"]["code"], "H15");
    assert_eq!(dashboard["selection"]["returnedSeries"], 11);
    assert_eq!(dashboard["selection"]["availableSeries"], 10);
    assert_eq!(dashboard["selection"]["missingSeries"], 1);
    assert_eq!(dashboard["selection"]["complete"], true);
    let observations = dashboard["observations"]
        .as_array()
        .context("Board dashboard observations were not an array")?;
    assert_eq!(
        observations
            .iter()
            .map(|row| row["slot"].as_str().unwrap_or_default())
            .collect::<Vec<_>>(),
        [
            "1m", "3m", "6m", "1y", "2y", "3y", "5y", "7y", "10y", "20y", "30y"
        ],
    );
    let twenty_year = observations
        .iter()
        .find(|row| row["slot"] == "20y")
        .context("Board dashboard omitted the 20-year slot")?;
    assert_eq!(twenty_year["effectiveDate"], "2026-08-10");
    assert_eq!(twenty_year["observation"]["state"], "missing");
    assert_eq!(twenty_year["observation"]["marker"], "ND");
    Ok(())
}

#[cfg(all(feature = "board-installed-fixture", debug_assertions))]
fn assert_installed_board_transport_counts(
    counters: BoardScriptedTransportCounters,
    doctor_attempts: u64,
    doctor_responses: u64,
    production_attempts: u64,
    production_responses: u64,
) {
    assert_eq!(counters.doctor_attempts(), doctor_attempts);
    assert_eq!(counters.doctor_responses(), doctor_responses);
    assert_eq!(counters.production_attempts(), production_attempts);
    assert_eq!(counters.production_responses(), production_responses);
}

#[cfg(all(feature = "board-installed-fixture", debug_assertions))]
fn stable_installed_board_dashboard(dashboard: &Value) -> Value {
    json!({
        "binding": {
            "surfaceId": dashboard["binding"]["surfaceId"],
            "sourceId": dashboard["binding"]["sourceId"],
            "providerDatasetId": dashboard["binding"]["providerDatasetId"],
            "analyticalDatasetId": dashboard["binding"]["analyticalDatasetId"],
            "manifest": dashboard["binding"]["manifest"],
            "objectGraphDigest": dashboard["binding"]["objectGraphDigest"],
        },
        "release": dashboard["release"],
        "selection": {
            "policy": dashboard["selection"]["policy"],
            "returnedSeries": dashboard["selection"]["returnedSeries"],
            "availableSeries": dashboard["selection"]["availableSeries"],
            "missingSeries": dashboard["selection"]["missingSeries"],
            "complete": dashboard["selection"]["complete"],
        },
        "observations": dashboard["observations"],
    })
}

#[cfg(all(feature = "board-installed-fixture", debug_assertions))]
fn stable_installed_board_history(history: &Value) -> TestResult<Value> {
    let artifact = history["artifact"]
        .as_object()
        .context("Board history did not publish its bounded Parquet artifact")?;
    assert!(
        artifact
            .get("artifactId")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.is_empty())
    );
    assert_eq!(
        artifact.get("mediaType"),
        Some(&json!("application/vnd.apache.parquet"))
    );
    assert_eq!(
        artifact.get("rowCount"),
        Some(&json!(
            BOARD_H15_TREASURY_CONSTANT_MATURITIES_ROLLING_DASHBOARD_OBSERVATION_COUNT
        ))
    );
    assert!(
        artifact
            .get("byteCount")
            .and_then(Value::as_u64)
            .is_some_and(|value| value > 0)
    );
    assert!(artifact.get("sha256").and_then(Value::as_str).is_some_and(
        |value| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
    ));
    Ok(json!({
        "manifest": history["manifest"],
        "artifact": {
            "sha256": artifact["sha256"],
            "byteCount": artifact["byteCount"],
            "mediaType": artifact["mediaType"],
            "rowCount": artifact["rowCount"],
        },
    }))
}

#[cfg(all(feature = "board-installed-fixture", debug_assertions))]
fn installed_file_evidence(
    root: &Path,
    extension: &str,
) -> TestResult<Vec<InstalledBoardFileEvidence>> {
    fn visit(
        root: &Path,
        current: &Path,
        extension: &str,
        files: &mut Vec<InstalledBoardFileEvidence>,
    ) -> TestResult {
        for entry in std::fs::read_dir(current)
            .with_context(|| format!("read installed evidence directory {}", current.display()))?
        {
            let entry = entry.context("read installed evidence directory entry")?;
            let file_type = entry
                .file_type()
                .context("inspect installed evidence directory entry")?;
            let path = entry.path();
            if file_type.is_dir() {
                visit(root, &path, extension, files)?;
            } else if file_type.is_file()
                && path.extension().and_then(|value| value.to_str()) == Some(extension)
            {
                let bytes = std::fs::read(&path)
                    .with_context(|| format!("read installed evidence {}", path.display()))?;
                files.push(InstalledBoardFileEvidence {
                    relative_path: path
                        .strip_prefix(root)
                        .context("installed evidence escaped its scenario root")?
                        .to_path_buf(),
                    bytes: u64::try_from(bytes.len())
                        .context("measure installed evidence bytes")?,
                    sha256: format!("{:x}", Sha256::digest(&bytes)),
                });
            }
        }
        Ok(())
    }

    let mut files = Vec::new();
    visit(root, root, extension, &mut files)?;
    files.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    Ok(files)
}

#[cfg(all(feature = "board-installed-fixture", debug_assertions))]
fn new_installed_files(
    before: &[InstalledBoardFileEvidence],
    after: Vec<InstalledBoardFileEvidence>,
) -> Vec<InstalledBoardFileEvidence> {
    after
        .into_iter()
        .filter(|candidate| {
            !before
                .iter()
                .any(|existing| existing.relative_path == candidate.relative_path)
        })
        .collect()
}

#[cfg(all(feature = "board-installed-fixture", debug_assertions))]
fn assert_installed_file_evidence(
    root: &Path,
    expected: &[InstalledBoardFileEvidence],
) -> TestResult {
    for item in expected {
        let bytes = std::fs::read(root.join(&item.relative_path)).with_context(|| {
            format!("reopen installed evidence {}", item.relative_path.display())
        })?;
        assert_eq!(u64::try_from(bytes.len())?, item.bytes);
        assert_eq!(format!("{:x}", Sha256::digest(&bytes)), item.sha256);
    }
    Ok(())
}

#[cfg(all(feature = "board-installed-fixture", debug_assertions))]
fn assert_file_magic(
    root: &Path,
    evidence: &InstalledBoardFileEvidence,
    prefix: &[u8],
    suffix: Option<&[u8]>,
) -> TestResult {
    let bytes = std::fs::read(root.join(&evidence.relative_path))?;
    assert!(bytes.starts_with(prefix));
    if let Some(suffix) = suffix {
        assert!(bytes.ends_with(suffix));
    }
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
    let generation = committed.result()["value"]["data"]["generation"]
        .as_u64()
        .context("research commit omitted its durable job generation")?;
    wait_for_job_completion(client, job_id, generation).await?;
    assert_owner_research_file_available(client).await
}

async fn wait_for_job_completion(
    client: &LoopbackApplicationClient,
    job_id: &str,
    generation: u64,
) -> TestResult {
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(30))
        .context("compute research-file job deadline")?;
    loop {
        let job = client
            .invoke_operation(
                RequestId::try_string(format!("installed-research-job-{job_id}"))
                    .context("construct research job request ID")?,
                "Job.Get",
                json!({"jobId": job_id, "generation": generation}),
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
                exercise_concurrent_installed_relays(&connector, &desktop, &snapshot)
                    .await
                    .context("exercise concurrent installed subprocess MCP relays")?;
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
    exercise_installed_relay_with_gate(client, transport, None, None).await
}

async fn exercise_installed_relay_with_market(
    client: NamedClient,
    transport: Arc<dyn market_squawk_mcp::McpRelayTransport>,
    real_alpaca: Option<&RealAlpacaEvidence>,
) -> TestResult {
    exercise_installed_relay_with_gate(client, transport, None, real_alpaca).await
}

struct ConcurrentRelayGate {
    initialized: tokio::sync::Barrier,
    release: tokio::sync::Barrier,
}

async fn exercise_concurrent_installed_relays(
    connector: &InstalledServiceConnector,
    desktop: &LoopbackApplicationClient,
    initial_bootstrap: &Value,
) -> TestResult {
    let initial_runtime = initial_bootstrap["runtime"].clone();
    let before = installed_mcp_runtime_status(desktop, "before").await?;

    let gate = Arc::new(ConcurrentRelayGate {
        initialized: tokio::sync::Barrier::new(3),
        release: tokio::sync::Barrier::new(3),
    });
    let claude_gate = Arc::clone(&gate);
    let codex_gate = Arc::clone(&gate);
    let evidence_gate = Arc::clone(&gate);
    let claude = exercise_installed_relay_with_gate(
        NamedClient::ClaudeCode,
        connector
            .connect_mcp_relay(NamedClient::ClaudeCode)
            .context("admit concurrent Claude Code relay")?,
        Some(claude_gate),
        None,
    );
    let codex = exercise_installed_relay_with_gate(
        NamedClient::Codex,
        connector
            .connect_mcp_relay(NamedClient::Codex)
            .context("admit concurrent Codex relay")?,
        Some(codex_gate),
        None,
    );
    let collect_evidence = async {
        evidence_gate.initialized.wait().await;
        let during = installed_mcp_runtime_status(desktop, "during").await?;
        let clients = during["clients"]
            .as_array()
            .context("installed MCP runtime clients were not an array")?;
        assert_eq!(clients.len(), 2);
        let claude = mcp_runtime_client(&during, "claude_code")?;
        let codex = mcp_runtime_client(&during, "codex")?;
        assert_ne!(claude["clientId"], codex["clientId"]);
        assert_ne!(claude["credentialIdentity"], codex["credentialIdentity"]);
        for client in ["claude_code", "codex"] {
            let before_client = mcp_runtime_client(&before, client)?;
            let during_client = mcp_runtime_client(&during, client)?;
            assert_eq!(
                during_client["observedRelayInitializations"].as_u64(),
                before_client["observedRelayInitializations"]
                    .as_u64()
                    .and_then(|count| count.checked_add(1)),
                "both named relays must initialize before either concurrent session is released"
            );
            let active = during_client["activeRequests"]
                .as_u64()
                .context("installed MCP client omitted its active-request count")?;
            let maximum = during_client["maximumActiveRequests"]
                .as_u64()
                .filter(|maximum| *maximum > 0)
                .context("installed MCP client omitted its active-request bound")?;
            assert!(active <= maximum);
        }
        let active = during["activeRequests"]
            .as_u64()
            .context("installed MCP runtime omitted its active-request count")?;
        let maximum = during["limits"]["maximumActiveRequests"]
            .as_u64()
            .filter(|maximum| *maximum > 0)
            .context("installed MCP runtime omitted its active-request bound")?;
        assert!(active <= maximum);
        evidence_gate.release.wait().await;
        Ok::<(), anyhow::Error>(())
    };
    let ((), (), ()) = tokio::time::timeout(INSTALLED_MCP_SERVICE_TIMEOUT, async {
        tokio::try_join!(claude, codex, collect_evidence)
    })
    .await
    .context("time out concurrent installed MCP relay verification")??;

    let after = installed_mcp_runtime_status(desktop, "after").await?;
    assert_eq!(after["activeClients"], 0);
    assert_eq!(after["activeRequests"], 0);
    let final_bootstrap = desktop
        .bootstrap(CancellationToken::new())
        .await
        .context("refresh bootstrap after concurrent MCP relay verification")?;
    assert_eq!(final_bootstrap["runtime"], initial_runtime);
    Ok(())
}

async fn installed_mcp_runtime_status(
    desktop: &LoopbackApplicationClient,
    phase: &str,
) -> TestResult<Value> {
    let response = desktop
        .invoke_operation(
            RequestId::try_string(format!("installed-concurrent-mcp-status-{phase}"))
                .context("construct concurrent MCP status request ID")?,
            "Mcp.GetRuntimeStatus",
            json!({}),
            INSTALLED_MCP_SERVICE_TIMEOUT,
            CancellationToken::new(),
        )
        .await
        .context("read installed MCP runtime status")?;
    assert_eq!(response.result()["ok"], true, "{}", response.result());
    Ok(response.result()["value"].clone())
}

fn mcp_runtime_client<'a>(status: &'a Value, client: &str) -> TestResult<&'a Value> {
    status["clients"]
        .as_array()
        .and_then(|clients| clients.iter().find(|entry| entry["client"] == client))
        .with_context(|| format!("installed MCP runtime omitted {client}"))
}

async fn exercise_installed_relay_with_gate(
    client: NamedClient,
    transport: Arc<dyn market_squawk_mcp::McpRelayTransport>,
    concurrent_gate: Option<Arc<ConcurrentRelayGate>>,
    real_alpaca: Option<&RealAlpacaEvidence>,
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
    if let Some(gate) = concurrent_gate {
        gate.initialized.wait().await;
        gate.release.wait().await;
    }
    write_message(
        &mut peer_writer,
        json!({"jsonrpc":"2.0","id":"installed-tools","method":"tools/list"}),
    )
    .await
    .context("write installed relay tools-list request")?;
    let tools = read_message(&mut peer_reader)
        .await
        .context("read installed relay tools-list response")?;
    let names = tools["result"]["tools"]
        .as_array()
        .context("installed relay tools/list omitted its tools")?
        .iter()
        .map(|tool| {
            tool["name"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("installed relay tool omitted its name"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let capabilities = application_capabilities()?;
    let expected = capabilities
        .tools()
        .iter()
        .map(|tool| tool.name())
        .collect::<Vec<_>>();
    assert_eq!(names, expected);
    write_message(
        &mut peer_writer,
        json!({
            "jsonrpc":"2.0","id":"installed-jobs","method":"tools/call",
            "params":{"name":"Job.List","arguments":{"limit":16}}
        }),
    )
    .await
    .context("write installed relay Job.List request")?;
    let jobs = read_message(&mut peer_reader)
        .await
        .context("read installed relay Job.List response")?;
    assert!(
        jobs["result"]["structuredContent"]["data"]["jobs"]
            .as_array()
            .is_some(),
        "installed MCP did not dispatch the non-core Job.List authority: {jobs}"
    );
    if let Some(real_alpaca) = real_alpaca {
        write_message(
            &mut peer_writer,
            json!({
                "jsonrpc":"2.0","id":"installed-real-alpaca-market","method":"tools/call",
                "params":{
                    "name":"Market.GetUnifiedFeed",
                    "arguments":real_alpaca_market_arguments(std::slice::from_ref(
                        &real_alpaca.instrument_id,
                    ))
                }
            }),
        )
        .await
        .context("write installed relay real Alpaca Market request")?;
        let market = read_message(&mut peer_reader)
            .await
            .context("read installed relay real Alpaca Market response")?;
        assert_real_alpaca_mcp_market(&market["result"]["structuredContent"]["data"], real_alpaca)
            .context("verify installed MCP real Alpaca Market evidence")?;
    }
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
