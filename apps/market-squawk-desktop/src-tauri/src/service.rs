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
    SchwabOAuthInstallationCapabilityError, SchwabOAuthInstallationTrustAction,
    SchwabOAuthInstallationTrustState,
    service::{
        BootstrapRequirement, InstalledServiceBootstrapState, InstalledServiceBootstrapStatus,
        InstalledServiceConnector, InstalledServiceError, SystemProcessIdentityVerifier,
        launch_foreground_keyring_broker,
    },
    verified_installed_service_program,
};
use market_squawk_installer::default_installation_data_root;
use market_squawk_platform::{AppConfig, SecretValue};
use market_squawk_runtime::{
    ApplicationClientError, LoopbackApplicationClient, NamedClient, ProcessIdentity,
    ProcessIdentityVerifier, ServiceStartupEvidenceError, ServiceStartupPhase, ServiceStartupState,
    read_service_startup_evidence,
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
    pub(crate) authority: Arc<DesktopServiceAuthority>,
}

pub(crate) enum DesktopServiceStartup {
    Ready(Box<DesktopServiceConnection>),
    BootstrapRequired(DesktopServiceBootstrap),
}

pub(crate) struct DesktopServiceBootstrap {
    authority: Arc<DesktopServiceAuthority>,
    status: InstalledServiceBootstrapStatus,
}

impl DesktopServiceBootstrap {
    pub(crate) const fn requirement(&self) -> Option<BootstrapRequirement> {
        self.status.requirement()
    }
}

impl DesktopServiceAuthority {
    /// Runs the explicit foreground macOS trust transition outside the background service.
    pub(crate) async fn schwab_oauth_installation_trust(
        &self,
        action: SchwabOAuthInstallationTrustAction,
        cancellation: CancellationToken,
    ) -> Result<SchwabOAuthInstallationTrustState, DesktopServiceError> {
        self.connector
            .schwab_oauth_installation_trust(action, cancellation)
            .await
            .map_err(|error| match error {
                InstalledServiceError::SchwabOAuthTrust(error) => {
                    DesktopServiceError::SchwabOAuthTrust(error)
                }
                _ => DesktopServiceError::Discovery,
            })
    }
}

/// Native-only authority for reconnecting to or restarting the exact installed service.
///
/// The retained launch specification contains only already validated paths. It contains no
/// credential value and is never constructed from WebView input.
pub(crate) struct DesktopServiceAuthority {
    connector: Arc<InstalledServiceConnector>,
    launch: DesktopServiceLaunch,
}

struct DesktopServiceLaunch {
    program: PathBuf,
    data_dir: PathBuf,
    config_path: Option<PathBuf>,
    training_release_root: Option<PathBuf>,
    installation_data_root: PathBuf,
    /// Debug launcher owns replacement; this Desktop must never spawn a competitor.
    externally_managed: bool,
}

pub(crate) enum DesktopBootstrapAction {
    Unlock(SecretValue),
    CompleteForegroundKeyring,
}

#[derive(Debug, Error)]
pub(crate) enum DesktopServiceError {
    #[error("desktop service startup was cancelled")]
    StartupCancelled,
    #[error("installed service discovery is unavailable")]
    Discovery,
    #[error("Schwab OAuth callback trust is unavailable")]
    SchwabOAuthTrust(#[source] SchwabOAuthInstallationCapabilityError),
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
    cancellation: CancellationToken,
) -> Result<DesktopServiceStartup, DesktopServiceError> {
    let installation_data_root = installation_data_root
        .map(Path::to_path_buf)
        .map_or_else(default_installation_data_root, Ok)
        .map_err(|_error| DesktopServiceError::Discovery)?;
    let connector = Arc::new(
        InstalledServiceConnector::try_new_at_installation_root(config, &installation_data_root)
            .map_err(|_error| DesktopServiceError::Discovery)?,
    );
    let authority = Arc::new(DesktopServiceAuthority {
        connector,
        launch: DesktopServiceLaunch {
            program: selected_service_program()?,
            data_dir: config.data_dir().to_path_buf(),
            config_path: config_path.map(Path::to_path_buf),
            training_release_root: config.training_release_root().map(Path::to_path_buf),
            installation_data_root,
            externally_managed: {
                #[cfg(debug_assertions)]
                {
                    std::env::var_os("MARKET_SQUAWK_DEVELOPMENT_EXTERNAL_SERVICE")
                        .is_some_and(|value| value == "1")
                }
                #[cfg(not(debug_assertions))]
                {
                    false
                }
            },
        },
    });
    reconnect_or_start(&authority, cancellation).await
}

pub(crate) async fn reconnect_or_start(
    authority: &Arc<DesktopServiceAuthority>,
    cancellation: CancellationToken,
) -> Result<DesktopServiceStartup, DesktopServiceError> {
    let deadline = Instant::now()
        .checked_add(STARTUP_TIMEOUT)
        .ok_or(DesktopServiceError::StartupDeadline)?;
    let mut child = None;
    let result = tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(DesktopServiceError::StartupCancelled),
        _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            Err(DesktopServiceError::StartupDeadline)
        }
        result = reconnect_or_start_until(authority, &mut child, deadline, &cancellation) => result,
    };
    let result = if cancellation.is_cancelled() {
        Err(DesktopServiceError::StartupCancelled)
    } else if Instant::now() >= deadline {
        Err(DesktopServiceError::StartupDeadline)
    } else {
        result
    };
    if result.is_err()
        && let Some(child) = child.as_mut()
    {
        // Only the child created by this attempt is ours to stop; a joined owner is not.
        stop_failed_start(child);
    }
    result
}

async fn reconnect_or_start_until(
    authority: &Arc<DesktopServiceAuthority>,
    child: &mut Option<Child>,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<DesktopServiceStartup, DesktopServiceError> {
    if authority.launch.externally_managed {
        return wait_for_started_service(authority, None, None, None, deadline, cancellation).await;
    }
    let previous = read_service_startup_evidence(&authority.launch.installation_data_root)?;
    if let Some(evidence) = previous
        && process_is_current(evidence.process_identity())?
    {
        return wait_for_started_service(
            authority,
            None,
            Some(evidence.process_identity()),
            previous.map(|evidence| evidence.process_identity()),
            deadline,
            cancellation,
        )
        .await;
    }
    match connect(authority, deadline, cancellation).await {
        Ok(connection) => return Ok(DesktopServiceStartup::Ready(Box::new(connection))),
        Err(ConnectionAttempt::NotRunning) => {}
        Err(ConnectionAttempt::InvalidBootstrap) => {
            return Err(DesktopServiceError::InvalidBootstrap);
        }
    }
    if let Some(bootstrap) = bootstrap_required(authority).await? {
        return Ok(DesktopServiceStartup::BootstrapRequired(bootstrap));
    }

    let program = selected_service_program()?;
    if program != authority.launch.program {
        return Err(DesktopServiceError::InvalidBootstrap);
    }
    let mut command = Command::new(program);
    command
        .env_remove("MARKET_SQUAWK_DEVELOPMENT_SERVICE_PROGRAM")
        .env_remove("MARKET_SQUAWK_DEVELOPMENT_MCP_RELAY_PROGRAM")
        .arg("--data-dir")
        .arg(&authority.launch.data_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(path) = &authority.launch.config_path {
        command.arg("--config").arg(path);
    }
    if let Some(path) = &authority.launch.training_release_root {
        command.arg("--training-release-root").arg(path);
    }
    command
        .arg("--installation-data-root")
        .arg(&authority.launch.installation_data_root);
    if cancellation.is_cancelled() {
        return Err(DesktopServiceError::StartupCancelled);
    }
    if Instant::now() >= deadline {
        return Err(DesktopServiceError::StartupDeadline);
    }
    *child = Some(command.spawn().map_err(DesktopServiceError::Launch)?);
    wait_for_started_service(
        authority,
        child.as_mut(),
        None,
        previous.map(|evidence| evidence.process_identity()),
        deadline,
        cancellation,
    )
    .await
}

fn selected_service_program() -> Result<PathBuf, DesktopServiceError> {
    #[cfg(debug_assertions)]
    if let Some(program) = std::env::var_os(DEVELOPMENT_SERVICE_PROGRAM) {
        return verified_development_service_program(Path::new(&program))
            .map_err(|_error| DesktopServiceError::Discovery);
    }
    verified_installed_service_program().map_err(|_error| DesktopServiceError::Discovery)
}

fn process_is_current(process: ProcessIdentity) -> Result<bool, DesktopServiceError> {
    SystemProcessIdentityVerifier
        .is_current(process)
        .map_err(|_| DesktopServiceError::Discovery)
}

async fn wait_for_started_service(
    authority: &Arc<DesktopServiceAuthority>,
    mut child: Option<&mut Child>,
    mut owner: Option<ProcessIdentity>,
    previous: Option<ProcessIdentity>,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<DesktopServiceStartup, DesktopServiceError> {
    loop {
        if Instant::now() >= deadline {
            return Err(DesktopServiceError::StartupDeadline);
        }
        let child_exited = child
            .as_mut()
            .map(|child| child.try_wait())
            .transpose()
            .map_err(DesktopServiceError::ProcessState)?
            .flatten()
            .is_some();
        let evidence = read_service_startup_evidence(&authority.launch.installation_data_root)?;
        let mut live_state = None;
        if let Some(evidence) = evidence {
            let process = evidence.process_identity();
            if process_is_current(process)? {
                owner = Some(process);
                live_state = Some(evidence.state());
            } else if !authority.launch.externally_managed
                && (owner == Some(process)
                    || (child_exited
                        && previous != Some(process)
                        && child
                            .as_ref()
                            .is_some_and(|child| child.id() == process.process_id())))
            {
                return Err(match evidence.state() {
                    ServiceStartupState::Failed { phase } => {
                        DesktopServiceError::StartupFailed { phase }
                    }
                    _ => DesktopServiceError::StartupExited,
                });
            }
        }
        if live_state.is_none()
            && !authority.launch.externally_managed
            && let Some(process) = owner
            && !process_is_current(process)?
        {
            return Err(DesktopServiceError::StartupExited);
        }
        match live_state {
            Some(ServiceStartupState::Failed { phase }) => {
                return Err(DesktopServiceError::StartupFailed { phase });
            }
            Some(ServiceStartupState::Stopped) if authority.launch.externally_managed => {}
            Some(ServiceStartupState::Stopped) => return Err(DesktopServiceError::StartupExited),
            Some(ServiceStartupState::Starting { .. }) => {
                if let Some(bootstrap) = bootstrap_required(authority).await? {
                    return Ok(DesktopServiceStartup::BootstrapRequired(bootstrap));
                }
            }
            Some(ServiceStartupState::Ready) | None => {
                match connect(authority, deadline, cancellation).await {
                    Ok(connection) => {
                        return Ok(DesktopServiceStartup::Ready(Box::new(connection)));
                    }
                    Err(ConnectionAttempt::InvalidBootstrap) => {
                        return Err(DesktopServiceError::InvalidBootstrap);
                    }
                    Err(ConnectionAttempt::NotRunning) => {}
                }
                if let Some(bootstrap) = bootstrap_required(authority).await? {
                    return Ok(DesktopServiceStartup::BootstrapRequired(bootstrap));
                }
            }
        }
        if Instant::now() >= deadline {
            return Err(DesktopServiceError::StartupDeadline);
        }
        // A contender can exit before the owner publishes its first evidence. Wait only for
        // authenticated readiness or verified process evidence within this original deadline.
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

pub(crate) async fn complete_bootstrap(
    bootstrap: &DesktopServiceBootstrap,
    action: DesktopBootstrapAction,
) -> Result<DesktopServiceConnection, DesktopServiceError> {
    let requirement = bootstrap
        .requirement()
        .ok_or(DesktopServiceError::InvalidBootstrap)?;
    let action = admit_bootstrap_action(requirement, action)?;
    match action {
        DesktopBootstrapAction::Unlock(unlock) => {
            let status = bootstrap
                .authority
                .connector
                .bootstrap_unlock(bootstrap.status, unlock)
                .await
                .map_err(|_error| DesktopServiceError::InvalidBootstrap)?;
            if status.state() != InstalledServiceBootstrapState::Retrying
                || status.requirement().is_some()
            {
                return Err(DesktopServiceError::InvalidBootstrap);
            }
        }
        DesktopBootstrapAction::CompleteForegroundKeyring => {
            launch_foreground_keyring_broker(
                &bootstrap.authority.launch.installation_data_root,
                bootstrap.status,
            )
            .await
            .map_err(|_error| DesktopServiceError::InvalidBootstrap)?;
        }
    }
    connect_until_ready(&bootstrap.authority, FOREGROUND_BOOTSTRAP_TIMEOUT).await
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
            BootstrapRequirement::ForegroundKeyringCredential,
            DesktopBootstrapAction::CompleteForegroundKeyring
        )
    ) {
        Ok(action)
    } else {
        Err(DesktopServiceError::InvalidBootstrap)
    }
}

async fn bootstrap_required(
    authority: &Arc<DesktopServiceAuthority>,
) -> Result<Option<DesktopServiceBootstrap>, DesktopServiceError> {
    match authority.connector.bootstrap_status().await {
        Ok(status)
            if status.state() == InstalledServiceBootstrapState::Required
                && status.requirement().is_some() =>
        {
            Ok(Some(DesktopServiceBootstrap {
                authority: Arc::clone(authority),
                status,
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
    authority: &Arc<DesktopServiceAuthority>,
    timeout: Duration,
) -> Result<DesktopServiceConnection, DesktopServiceError> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or(DesktopServiceError::StartupDeadline)?;
    loop {
        let startup = read_service_startup_evidence(&authority.launch.installation_data_root)?
            .map(|evidence| evidence.state());
        match startup {
            Some(ServiceStartupState::Starting { .. }) => {}
            Some(ServiceStartupState::Failed { phase }) => {
                return Err(DesktopServiceError::StartupFailed { phase });
            }
            Some(ServiceStartupState::Stopped) => {
                return Err(DesktopServiceError::StartupExited);
            }
            Some(ServiceStartupState::Ready) | None => {
                match connect(authority, deadline, &CancellationToken::new()).await {
                    Ok(connection) => return Ok(connection),
                    Err(ConnectionAttempt::InvalidBootstrap) => {
                        return Err(DesktopServiceError::InvalidBootstrap);
                    }
                    Err(ConnectionAttempt::NotRunning) => {}
                }
            }
        }
        if Instant::now() >= deadline {
            return Err(DesktopServiceError::StartupDeadline);
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn connect(
    authority: &Arc<DesktopServiceAuthority>,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<DesktopServiceConnection, ConnectionAttempt> {
    let timeout = deadline
        .saturating_duration_since(Instant::now())
        .min(CONNECT_TIMEOUT);
    if timeout.is_zero() || cancellation.is_cancelled() {
        return Err(ConnectionAttempt::NotRunning);
    }
    let application = authority
        .connector
        .connect_with_timeout(
            NamedClient::Desktop,
            Some(DESKTOP_ORIGIN.to_owned()),
            timeout,
        )
        .map_err(map_connect_error)?;
    let bootstrap = application
        .bootstrap(cancellation.clone())
        .await
        .map_err(map_client_error)?;
    Ok(DesktopServiceConnection {
        application,
        bootstrap,
        authority: Arc::clone(authority),
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
                DesktopBootstrapAction::CompleteForegroundKeyring,
            )
            .is_err()
        );
        let unlock = SecretValue::new("process-local-test-unlock".to_owned())?;
        assert!(
            admit_bootstrap_action(
                BootstrapRequirement::ForegroundKeyringCredential,
                DesktopBootstrapAction::Unlock(unlock),
            )
            .is_err()
        );
        Ok(())
    }
}
