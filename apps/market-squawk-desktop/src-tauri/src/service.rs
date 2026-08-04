//! Native lifecycle boundary for the shared installed Market Squawk service.

use std::{
    path::Path,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

use market_squawk::{
    service::{
        BootstrapRequirement, InstalledServiceBootstrapState, InstalledServiceConnector,
        InstalledServiceError,
    },
    verified_installed_service_program,
};
use market_squawk_platform::{AppConfig, SecretValue};
use market_squawk_runtime::{ApplicationClientError, LoopbackApplicationClient, NamedClient};
use serde_json::Value;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

const DESKTOP_ORIGIN: &str = "tauri://localhost";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_millis(100);

pub(crate) struct DesktopServiceConnection {
    pub(crate) application: LoopbackApplicationClient,
    pub(crate) bootstrap: Value,
}

pub(crate) enum DesktopServiceStartup {
    Ready(DesktopServiceConnection),
    BootstrapRequired(DesktopServiceBootstrap),
}

pub(crate) struct DesktopServiceBootstrap {
    connector: Arc<InstalledServiceConnector>,
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
    #[error("the installed service returned an invalid bootstrap contract")]
    InvalidBootstrap,
}

pub(crate) async fn connect_or_start(
    config: &AppConfig,
    config_path: Option<&Path>,
    installation_data_root: Option<&Path>,
) -> Result<DesktopServiceStartup, DesktopServiceError> {
    let connector = Arc::new(
        installation_data_root
            .map_or_else(
                || InstalledServiceConnector::try_new(config),
                |root| InstalledServiceConnector::try_new_at_installation_root(config, root),
            )
            .map_err(|_error| DesktopServiceError::Discovery)?,
    );
    match connect(&connector).await {
        Ok(connection) => return Ok(DesktopServiceStartup::Ready(connection)),
        Err(ConnectionAttempt::NotRunning) => {}
        Err(ConnectionAttempt::InvalidBootstrap) => {
            return Err(DesktopServiceError::InvalidBootstrap);
        }
    }
    if let Some(bootstrap) = bootstrap_required(&connector).await? {
        return Ok(DesktopServiceStartup::BootstrapRequired(bootstrap));
    }

    let program =
        verified_installed_service_program().map_err(|_error| DesktopServiceError::Discovery)?;
    let mut command = Command::new(program);
    command
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
    if let Some(root) = installation_data_root {
        command.arg("--installation-data-root").arg(root);
    }
    let mut child = command.spawn().map_err(DesktopServiceError::Launch)?;
    let deadline = Instant::now()
        .checked_add(STARTUP_TIMEOUT)
        .ok_or(DesktopServiceError::StartupDeadline)?;
    loop {
        match connect(&connector).await {
            Ok(connection) => return Ok(DesktopServiceStartup::Ready(connection)),
            Err(ConnectionAttempt::InvalidBootstrap) => {
                stop_failed_start(&mut child);
                return Err(DesktopServiceError::InvalidBootstrap);
            }
            Err(ConnectionAttempt::NotRunning) => match bootstrap_required(&connector).await {
                Ok(Some(bootstrap)) => {
                    return Ok(DesktopServiceStartup::BootstrapRequired(bootstrap));
                }
                Ok(None) => {}
                Err(error) => {
                    stop_failed_start(&mut child);
                    return Err(error);
                }
            },
        }
        if Instant::now() >= deadline {
            stop_failed_start(&mut child);
            return Err(DesktopServiceError::StartupDeadline);
        }
        let _ = child.try_wait();
        tokio::time::sleep(POLL_INTERVAL).await;
    }
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
    connect_until_ready(&bootstrap.connector).await
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
) -> Result<Option<DesktopServiceBootstrap>, DesktopServiceError> {
    match connector.bootstrap_status().await {
        Ok(status)
            if status.state() == InstalledServiceBootstrapState::Required
                && status.requirement().is_some() =>
        {
            Ok(Some(DesktopServiceBootstrap {
                connector: Arc::clone(connector),
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
        Err(InstalledServiceError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(None)
        }
        Ok(_) | Err(_) => Err(DesktopServiceError::InvalidBootstrap),
    }
}

async fn connect_until_ready(
    connector: &InstalledServiceConnector,
) -> Result<DesktopServiceConnection, DesktopServiceError> {
    let deadline = Instant::now()
        .checked_add(STARTUP_TIMEOUT)
        .ok_or(DesktopServiceError::StartupDeadline)?;
    loop {
        match connect(connector).await {
            Ok(connection) => return Ok(connection),
            Err(ConnectionAttempt::InvalidBootstrap) => {
                return Err(DesktopServiceError::InvalidBootstrap);
            }
            Err(ConnectionAttempt::NotRunning) => {}
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
    fn bootstrap_action_must_match_the_native_typed_requirement() {
        assert!(matches!(
            admit_bootstrap_action(
                BootstrapRequirement::EncryptedFallbackLocked,
                DesktopBootstrapAction::Unlock(
                    SecretValue::new("process-local-test-unlock".to_owned())
                        .expect("test unlock is valid"),
                ),
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
        assert!(
            admit_bootstrap_action(
                BootstrapRequirement::ForegroundKeyringRetry,
                DesktopBootstrapAction::Unlock(
                    SecretValue::new("process-local-test-unlock".to_owned())
                        .expect("test unlock is valid"),
                ),
            )
            .is_err()
        );
    }
}
