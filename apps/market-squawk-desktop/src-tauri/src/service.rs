//! Native lifecycle boundary for the shared installed Market Squawk service.

use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

#[cfg(debug_assertions)]
use market_squawk::verified_development_service_program;
use market_squawk::{
    service::{
        BootstrapRequirement, InstalledServiceBootstrapState, InstalledServiceConnector,
        InstalledServiceError,
    },
    verified_installed_service_program,
};
use market_squawk_installer::default_installation_data_root;
use market_squawk_platform::{AppConfig, SecretValue};
use market_squawk_runtime::{
    ApplicationClientError, LoopbackApplicationClient, NamedClient, ServiceStartupEvidenceError,
    ServiceStartupPhase, ServiceStartupState, read_service_startup_evidence,
};
use serde_json::Value;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

const DESKTOP_ORIGIN: &str = "tauri://localhost";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);
const FOREGROUND_BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const POLL_INTERVAL: Duration = Duration::from_millis(100);
#[cfg(debug_assertions)]
const DEVELOPMENT_SERVICE_PROGRAM: &str = "MARKET_SQUAWK_DEVELOPMENT_SERVICE_PROGRAM";

pub(crate) struct DesktopServiceConnection {
    pub(crate) application: LoopbackApplicationClient,
    pub(crate) bootstrap: Value,
}

pub(crate) enum DesktopServiceStartup {
    Ready(Box<DesktopServiceConnection>),
    BootstrapRequired(DesktopServiceBootstrap),
}

pub(crate) struct DesktopServiceBootstrap {
    connector: Arc<InstalledServiceConnector>,
    installation_data_root: PathBuf,
    requirement: BootstrapRequirement,
}

impl DesktopServiceBootstrap {
    pub(crate) const fn requirement(&self) -> BootstrapRequirement {
        self.requirement
    }
}

pub(crate) enum DesktopBootstrapAction {
    Unlock(SecretValue),
    RetryAfterForegroundKeyring,
}

#[derive(Debug, Error)]
pub(crate) enum DesktopServiceError {
    #[error("installed service discovery is unavailable")]
    Discovery,
    #[error("the verified installed service could not start")]
    Launch(#[source] std::io::Error),
    #[error("the installed service did not become ready before the startup deadline")]
    StartupDeadline,
    #[error("the installed service process exited before becoming ready")]
    StartupExited,
    #[error("the installed service failed during {phase:?}")]
    StartupFailed { phase: ServiceStartupPhase },
    #[error("installed-service startup evidence is unavailable")]
    StartupEvidence(#[from] ServiceStartupEvidenceError),
    #[error("the installed service process state is unavailable")]
    ProcessState(#[source] std::io::Error),
    #[error("the installed service returned an invalid bootstrap contract")]
    InvalidBootstrap,
}

pub(crate) async fn connect_or_start(
    config: &AppConfig,
    config_path: Option<&Path>,
    installation_data_root: Option<&Path>,
) -> Result<DesktopServiceStartup, DesktopServiceError> {
    let installation_data_root = installation_data_root
        .map(Path::to_path_buf)
        .map_or_else(default_installation_data_root, Ok)
        .map_err(|_error| DesktopServiceError::Discovery)?;
    let connector = Arc::new(
        InstalledServiceConnector::try_new_at_installation_root(config, &installation_data_root)
            .map_err(|_error| DesktopServiceError::Discovery)?,
    );
    match connect(&connector).await {
        Ok(connection) => return Ok(DesktopServiceStartup::Ready(Box::new(connection))),
        Err(ConnectionAttempt::NotRunning) => {}
        Err(ConnectionAttempt::InvalidBootstrap) => {
            return Err(DesktopServiceError::InvalidBootstrap);
        }
    }
    if let Some(bootstrap) = bootstrap_required(&connector, &installation_data_root).await? {
        return Ok(DesktopServiceStartup::BootstrapRequired(bootstrap));
    }

    let program = selected_service_program()?;
    let mut command = Command::new(program);
    command
        .env_remove("MARKET_SQUAWK_DEVELOPMENT_SERVICE_PROGRAM")
        .env_remove("MARKET_SQUAWK_DEVELOPMENT_MCP_RELAY_PROGRAM")
        .arg("--data-dir")
        .arg(config.data_dir())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(path) = config_path {
        command.arg("--config").arg(path);
    }
    if let Some(path) = config.training_release_root() {
        command.arg("--training-release-root").arg(path);
    }
    command
        .arg("--installation-data-root")
        .arg(&installation_data_root);
    let mut child = command.spawn().map_err(DesktopServiceError::Launch)?;
    wait_for_started_service(&connector, &installation_data_root, &mut child).await
}

fn selected_service_program() -> Result<PathBuf, DesktopServiceError> {
    #[cfg(debug_assertions)]
    if let Some(program) = std::env::var_os(DEVELOPMENT_SERVICE_PROGRAM) {
        return verified_development_service_program(Path::new(&program))
            .map_err(|_error| DesktopServiceError::Discovery);
    }
    verified_installed_service_program().map_err(|_error| DesktopServiceError::Discovery)
}

async fn wait_for_started_service(
    connector: &Arc<InstalledServiceConnector>,
    installation_data_root: &Path,
    child: &mut Child,
) -> Result<DesktopServiceStartup, DesktopServiceError> {
    let deadline = Instant::now()
        .checked_add(STARTUP_TIMEOUT)
        .ok_or(DesktopServiceError::StartupDeadline)?;
    let mut observed_fresh_start = false;
    loop {
        if child
            .try_wait()
            .map_err(DesktopServiceError::ProcessState)?
            .is_some()
        {
            return connect_after_competing_start(connector, installation_data_root).await;
        }
        let startup = read_service_startup_evidence(installation_data_root)
            .map(|evidence| evidence.map(|evidence| evidence.state()));
        match startup {
            Ok(Some(ServiceStartupState::Starting { .. })) => {
                observed_fresh_start = true;
                if let Some(bootstrap) =
                    bootstrap_required(connector, installation_data_root).await?
                {
                    return Ok(DesktopServiceStartup::BootstrapRequired(bootstrap));
                }
            }
            Ok(Some(ServiceStartupState::Ready)) => match connect(connector).await {
                Ok(connection) => {
                    return Ok(DesktopServiceStartup::Ready(Box::new(connection)));
                }
                Err(ConnectionAttempt::NotRunning) => {}
                Err(ConnectionAttempt::InvalidBootstrap) if !observed_fresh_start => {}
                Err(ConnectionAttempt::InvalidBootstrap) => {
                    stop_failed_start(child);
                    return Err(DesktopServiceError::InvalidBootstrap);
                }
            },
            Ok(Some(ServiceStartupState::Failed { phase })) if observed_fresh_start => {
                stop_failed_start(child);
                return Err(DesktopServiceError::StartupFailed { phase });
            }
            Ok(Some(ServiceStartupState::Stopped)) if observed_fresh_start => {
                stop_failed_start(child);
                return Err(DesktopServiceError::StartupExited);
            }
            Ok(Some(ServiceStartupState::Failed { .. } | ServiceStartupState::Stopped) | None)
            | Err(_) => {}
        }
        if Instant::now() >= deadline {
            stop_failed_start(child);
            return Err(DesktopServiceError::StartupDeadline);
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn connect_after_competing_start(
    connector: &Arc<InstalledServiceConnector>,
    installation_data_root: &Path,
) -> Result<DesktopServiceStartup, DesktopServiceError> {
    if let Ok(connection) = connect(connector).await {
        return Ok(DesktopServiceStartup::Ready(Box::new(connection)));
    }
    if let Some(bootstrap) = bootstrap_required(connector, installation_data_root).await? {
        return Ok(DesktopServiceStartup::BootstrapRequired(bootstrap));
    }
    Err(DesktopServiceError::StartupExited)
}

pub(crate) async fn complete_bootstrap(
    bootstrap: &DesktopServiceBootstrap,
    action: DesktopBootstrapAction,
) -> Result<DesktopServiceConnection, DesktopServiceError> {
    let action = admit_bootstrap_action(bootstrap.requirement, action)?;
    let status = match action {
        DesktopBootstrapAction::Unlock(unlock) => {
            bootstrap.connector.bootstrap_unlock(unlock).await
        }
        DesktopBootstrapAction::RetryAfterForegroundKeyring => {
            bootstrap
                .connector
                .bootstrap_retry_after_foreground_keyring()
                .await
        }
    }
    .map_err(|_error| DesktopServiceError::InvalidBootstrap)?;
    if status.state() != InstalledServiceBootstrapState::Retrying || status.requirement().is_some()
    {
        return Err(DesktopServiceError::InvalidBootstrap);
    }
    connect_until_ready(
        &bootstrap.connector,
        &bootstrap.installation_data_root,
        FOREGROUND_BOOTSTRAP_TIMEOUT,
    )
    .await
}

fn admit_bootstrap_action(
    requirement: BootstrapRequirement,
    action: DesktopBootstrapAction,
) -> Result<DesktopBootstrapAction, DesktopServiceError> {
    if matches!(
        (requirement, &action),
        (
            BootstrapRequirement::EncryptedFallbackLocked,
            DesktopBootstrapAction::Unlock(_)
        ) | (
            BootstrapRequirement::ForegroundKeyringRetry,
            DesktopBootstrapAction::RetryAfterForegroundKeyring
        )
    ) {
        Ok(action)
    } else {
        Err(DesktopServiceError::InvalidBootstrap)
    }
}

async fn bootstrap_required(
    connector: &Arc<InstalledServiceConnector>,
    installation_data_root: &Path,
) -> Result<Option<DesktopServiceBootstrap>, DesktopServiceError> {
    match connector.bootstrap_status().await {
        Ok(status)
            if status.state() == InstalledServiceBootstrapState::Required
                && status.requirement().is_some() =>
        {
            Ok(Some(DesktopServiceBootstrap {
                connector: Arc::clone(connector),
                installation_data_root: installation_data_root.to_path_buf(),
                requirement: status
                    .requirement()
                    .ok_or(DesktopServiceError::InvalidBootstrap)?,
            }))
        }
        Ok(status)
            if status.state() == InstalledServiceBootstrapState::Retrying
                && status.requirement().is_none() =>
        {
            Ok(None)
        }
        Err(InstalledServiceError::BootstrapUnavailable) => Ok(None),
        Err(InstalledServiceError::ServiceUnavailable) => Ok(None),
        Err(InstalledServiceError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(None)
        }
        Ok(_) | Err(_) => Err(DesktopServiceError::InvalidBootstrap),
    }
}

async fn connect_until_ready(
    connector: &InstalledServiceConnector,
    installation_data_root: &Path,
    timeout: Duration,
) -> Result<DesktopServiceConnection, DesktopServiceError> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or(DesktopServiceError::StartupDeadline)?;
    loop {
        let startup =
            read_service_startup_evidence(installation_data_root)?.map(|evidence| evidence.state());
        match startup {
            Some(ServiceStartupState::Starting { .. }) => {}
            Some(ServiceStartupState::Failed { phase }) => {
                return Err(DesktopServiceError::StartupFailed { phase });
            }
            Some(ServiceStartupState::Stopped) => {
                return Err(DesktopServiceError::StartupExited);
            }
            Some(ServiceStartupState::Ready) | None => match connect(connector).await {
                Ok(connection) => return Ok(connection),
                Err(ConnectionAttempt::InvalidBootstrap) => {
                    return Err(DesktopServiceError::InvalidBootstrap);
                }
                Err(ConnectionAttempt::NotRunning) => {}
            },
        }
        if Instant::now() >= deadline {
            return Err(DesktopServiceError::StartupDeadline);
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn connect(
    connector: &InstalledServiceConnector,
) -> Result<DesktopServiceConnection, ConnectionAttempt> {
    let application = connector
        .connect_with_timeout(
            NamedClient::Desktop,
            Some(DESKTOP_ORIGIN.to_owned()),
            CONNECT_TIMEOUT,
        )
        .map_err(map_connect_error)?;
    let bootstrap = application
        .bootstrap(CancellationToken::new())
        .await
        .map_err(map_client_error)?;
    Ok(DesktopServiceConnection {
        application,
        bootstrap,
    })
}

#[derive(Clone, Copy, Debug)]
enum ConnectionAttempt {
    NotRunning,
    InvalidBootstrap,
}

fn map_connect_error(error: InstalledServiceError) -> ConnectionAttempt {
    match error {
        InstalledServiceError::ServiceUnavailable
        | InstalledServiceError::Client(ApplicationClientError::Unavailable) => {
            ConnectionAttempt::NotRunning
        }
        _ => ConnectionAttempt::InvalidBootstrap,
    }
}

fn map_client_error(error: ApplicationClientError) -> ConnectionAttempt {
    match error {
        ApplicationClientError::Unavailable => ConnectionAttempt::NotRunning,
        ApplicationClientError::Rejected
        | ApplicationClientError::Interrupted
        | ApplicationClientError::InvalidResponse => ConnectionAttempt::InvalidBootstrap,
    }
}

fn stop_failed_start(child: &mut Child) {
    if child.try_wait().ok().flatten().is_none() {
        let _ = child.kill();
        let _ = child.wait();
    }
}

#[cfg(test)]
mod tests {
    use market_squawk::service::BootstrapRequirement;
    use market_squawk_platform::SecretValue;

    use super::{DesktopBootstrapAction, admit_bootstrap_action};

    #[test]
    fn bootstrap_action_must_match_the_native_typed_requirement()
    -> Result<(), market_squawk_platform::SecretError> {
        let unlock = SecretValue::new("process-local-test-unlock".to_owned())?;
        assert!(matches!(
            admit_bootstrap_action(
                BootstrapRequirement::EncryptedFallbackLocked,
                DesktopBootstrapAction::Unlock(unlock),
            ),
            Ok(DesktopBootstrapAction::Unlock(_))
        ));
        assert!(
            admit_bootstrap_action(
                BootstrapRequirement::EncryptedFallbackLocked,
                DesktopBootstrapAction::RetryAfterForegroundKeyring,
            )
            .is_err()
        );
        let unlock = SecretValue::new("process-local-test-unlock".to_owned())?;
        assert!(
            admit_bootstrap_action(
                BootstrapRequirement::ForegroundKeyringRetry,
                DesktopBootstrapAction::Unlock(unlock),
            )
            .is_err()
        );
        Ok(())
    }
}
