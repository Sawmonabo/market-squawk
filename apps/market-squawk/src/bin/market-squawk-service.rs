// Rust #159105: this macOS-only dev/test-link diagnostic is caused by the measured
// `__eh_frame` exceeding arm64 compact-unwind's 24-bit offset range. Release diagnostics remain
// enabled because this allowance is restricted to debug-assertion builds.
#![cfg_attr(all(target_os = "macos", debug_assertions), allow(linker_messages))]

use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    process::ExitCode,
    time::Duration,
};

use anyhow::{Context as _, Result};
use clap::Parser;
use market_squawk::{
    AppConfig,
    service::{
        EphemeralVerificationRoot, InstalledSecretBackendPolicy, InstalledService,
        InstalledServiceLogging, InstalledServiceRunOutcome, TerminalLogFormat,
        complete_foreground_keyring_bootstrap_at_installation_root,
    },
    termination::TerminationSignals,
};
use market_squawk_installer::default_installation_data_root;
#[cfg(target_os = "macos")]
use market_squawk_installer::{NativeTrustMode, active_native_trust_mode};
use market_squawk_installer::{ProgramName, installation_root_for_installed_program};
use market_squawk_platform::{ConfigOverrides, ConfigSources};
use market_squawk_runtime::{
    InstallationId, ServiceStartupEvidenceWriter, ServiceStartupPhase, ServiceStartupState,
};
use tokio_util::sync::CancellationToken;

const APPLICATION_MAIN_STACK_BYTES: usize = 8 * 1024 * 1024;
const SUPERVISED_RESTART_EXIT_CODE: u8 = 75;
const LOG_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Parser)]
#[command(name = "market-squawk-service", version)]
struct ServiceArguments {
    /// Local Market Squawk data root.
    #[arg(long)]
    data_dir: Option<PathBuf>,
    /// Explicit installed-service authority root for isolated verification.
    #[arg(long, hide = true)]
    installation_data_root: Option<PathBuf>,
    /// Retire all credentials created under a fresh verification-only installation root on exit.
    #[arg(long, hide = true, requires = "installation_data_root")]
    ephemeral_verification_credentials: bool,
    /// Explicit local configuration file.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Absolute installed Python training-release root used to verify admitted models.
    #[arg(long)]
    training_release_root: Option<PathBuf>,
    /// Local tracing filter.
    #[arg(long, env = "MARKET_SQUAWK_LOG", default_value = "info")]
    log: String,
    /// Render local tracing as JSON.
    #[arg(long)]
    json_logs: bool,
    /// Complete one exact foreground credential handoff, then exit.
    #[arg(long, hide = true)]
    foreground_keyring_broker: bool,
    /// Exact installation identity captured from the short-lived bootstrap channel.
    #[arg(long, hide = true, requires = "foreground_keyring_broker")]
    bootstrap_installation_id: Option<uuid::Uuid>,
    /// Exact anti-replay generation captured from the short-lived bootstrap channel.
    #[arg(long, hide = true, requires = "foreground_keyring_broker")]
    bootstrap_generation: Option<u64>,
}

fn main() -> ExitCode {
    let service = match std::thread::Builder::new()
        .name("market-squawk-service-main".to_owned())
        .stack_size(APPLICATION_MAIN_STACK_BYTES)
        .spawn(run_service)
        .context("failed to start the Market Squawk service thread")
    {
        Ok(service) => service,
        Err(error) => {
            eprintln!("Market Squawk service failed: {error:#}");
            return ExitCode::FAILURE;
        }
    };
    match service.join() {
        Ok(Ok(InstalledServiceRunOutcome::Stopped)) => ExitCode::SUCCESS,
        Ok(Ok(InstalledServiceRunOutcome::RestartRequested { .. })) => {
            ExitCode::from(SUPERVISED_RESTART_EXIT_CODE)
        }
        Ok(Err(error)) => {
            eprintln!("Market Squawk service failed: {error:#}");
            ExitCode::FAILURE
        }
        Err(_) => {
            eprintln!("Market Squawk service thread terminated unexpectedly");
            ExitCode::FAILURE
        }
    }
}

fn run_service() -> Result<InstalledServiceRunOutcome> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(Box::pin(run()))
}

async fn run() -> Result<InstalledServiceRunOutcome> {
    let arguments = ServiceArguments::parse();
    let installation_data_root = arguments
        .installation_data_root
        .clone()
        .map_or_else(default_installation_data_root, Ok)?;
    if arguments.foreground_keyring_broker {
        return run_foreground_keyring_broker(&arguments, &installation_data_root)
            .await
            .map(|()| InstalledServiceRunOutcome::Stopped)
            .map_err(|_error| anyhow::anyhow!("secure startup could not be completed"));
    }
    let secret_backend_policy = installed_secret_backend_policy(&installation_data_root)?;
    let ephemeral_verification_root = if arguments.ephemeral_verification_credentials {
        Some(EphemeralVerificationRoot::try_new(&installation_data_root)?)
    } else {
        None
    };
    let startup = Some(ServiceStartupEvidenceWriter::try_open(
        &installation_data_root,
    )?);
    publish_startup(
        startup.as_ref(),
        ServiceStartupState::Starting {
            phase: ServiceStartupPhase::ProcessStarted,
        },
    )?;
    let config = match load_config(
        arguments.config.as_deref(),
        arguments.data_dir,
        arguments.training_release_root,
    ) {
        Ok(config) => config,
        Err(error) => {
            return fail_startup(
                startup.as_ref(),
                ServiceStartupPhase::ConfigurationLoaded,
                error,
            );
        }
    };
    publish_startup(
        startup.as_ref(),
        ServiceStartupState::Starting {
            phase: ServiceStartupPhase::ConfigurationLoaded,
        },
    )?;
    let terminal_format = if arguments.json_logs {
        TerminalLogFormat::Json
    } else {
        TerminalLogFormat::Human
    };
    let logging_result = InstalledServiceLogging::install_at_installation_root(
        &arguments.log,
        terminal_format,
        &installation_data_root,
    );
    let mut logging = match logging_result {
        Ok(logging) => logging,
        Err(error) => {
            return fail_startup(
                startup.as_ref(),
                ServiceStartupPhase::LoggingReady,
                error.into(),
            );
        }
    };
    publish_startup(
        startup.as_ref(),
        ServiceStartupState::Starting {
            phase: ServiceStartupPhase::LoggingReady,
        },
    )?;
    let result = run_installed_service(
        config,
        logging.store(),
        &installation_data_root,
        ephemeral_verification_root,
        secret_backend_policy,
        startup.as_ref(),
    )
    .await;
    let log_shutdown = logging.shutdown(LOG_SHUTDOWN_TIMEOUT).and_then(|evidence| {
        if evidence.accepted == evidence.persisted
            && evidence.dropped_overflow == 0
            && evidence.rejected_unsafe == 0
            && evidence.write_failures == 0
        {
            Ok(evidence)
        } else {
            Err(market_squawk::service::InstalledServiceLoggingError::IncompleteDrain)
        }
    });
    match (result, log_shutdown) {
        (Ok(outcome), Ok(_evidence)) => Ok(outcome),
        (Err(error), Ok(_evidence)) => Err(error),
        (Ok(_outcome), Err(error)) => Err(error.into()),
        (Err(error), Err(log_error)) => {
            Err(error.context(format!("structured-log shutdown also failed: {log_error}")))
        }
    }
}

async fn run_installed_service(
    config: AppConfig,
    logs: std::sync::Arc<market_squawk::application::logs::StructuredLogStore>,
    installation_data_root: &Path,
    ephemeral_verification_root: Option<EphemeralVerificationRoot>,
    secret_backend_policy: InstalledSecretBackendPolicy,
    startup: Option<&ServiceStartupEvidenceWriter>,
) -> Result<InstalledServiceRunOutcome> {
    publish_startup(
        startup,
        ServiceStartupState::Starting {
            phase: ServiceStartupPhase::RuntimeComposition,
        },
    )?;
    let mut termination = match TerminationSignals::install() {
        Ok(termination) => termination,
        Err(error) => {
            return fail_startup(
                startup,
                ServiceStartupPhase::RuntimeComposition,
                error.into(),
            );
        }
    };
    let mut starting = Box::pin(async move {
        match ephemeral_verification_root {
            Some(ephemeral_root) => {
                InstalledService::start_ephemeral_verification_with_logging_store_at_installation_root(
                    config,
                    ephemeral_root,
                    logs,
                    secret_backend_policy,
                )
                .await
            }
            None => {
                InstalledService::start_with_logging_store_at_installation_root(
                    config,
                    installation_data_root,
                    logs,
                    secret_backend_policy,
                )
                    .await
            }
        }
    });
    let service_result = tokio::select! {
        result = &mut starting => result,
        signal = termination.wait() => {
            signal?;
            publish_startup(
                startup,
                ServiceStartupState::Starting {
                    phase: ServiceStartupPhase::Shutdown,
                },
            )?;
            drop(starting);
            publish_startup(startup, ServiceStartupState::Stopped)?;
            return Ok(InstalledServiceRunOutcome::Stopped);
        }
    };
    let service = match service_result {
        Ok(service) => service,
        Err(error) => {
            tracing::error!(
                target: "market_squawk::installed_service",
                stage = "runtime-composition",
                reason = %error,
                "installed service startup failed"
            );
            return fail_startup(
                startup,
                ServiceStartupPhase::RuntimeComposition,
                error.into(),
            );
        }
    };
    publish_startup(startup, ServiceStartupState::Ready)?;
    let cancellation = CancellationToken::new();
    let mut serving = Box::pin(service.run(cancellation.clone()));
    let result = tokio::select! {
        result = &mut serving => result.map_err(Into::into),
        signal = termination.wait() => {
            cancellation.cancel();
            let evidence = publish_startup(
                startup,
                ServiceStartupState::Starting {
                    phase: ServiceStartupPhase::Shutdown,
                },
            ).err();
            if let Some(error) = evidence.as_ref() {
                tracing::warn!(
                    target: "market_squawk::installed_service",
                    stage = "shutdown",
                    reason = %error,
                    "installed service shutdown evidence could not be updated"
                );
            }
            combine_shutdown_results(
                signal.map_err(Into::into),
                serving.await.map_err(Into::into),
                evidence,
            )
        }
    };
    match result {
        Ok(outcome) => {
            if let Err(error) = publish_startup(startup, ServiceStartupState::Stopped) {
                tracing::warn!(
                    target: "market_squawk::installed_service",
                    stage = "stopped",
                    reason = %error,
                    "installed service stop evidence could not be updated"
                );
            }
            Ok(outcome)
        }
        Err(error) => fail_startup(startup, ServiceStartupPhase::Serving, error),
    }
}

async fn run_foreground_keyring_broker(
    arguments: &ServiceArguments,
    installation_data_root: &Path,
) -> Result<()> {
    if installed_secret_backend_policy(installation_data_root)?
        != InstalledSecretBackendPolicy::PlatformKeyring
        || arguments.ephemeral_verification_credentials
        || arguments.data_dir.is_some()
        || arguments.config.is_some()
        || arguments.training_release_root.is_some()
        || arguments.json_logs
    {
        anyhow::bail!("invalid secure-startup broker invocation");
    }
    let installation_id = arguments
        .bootstrap_installation_id
        .ok_or_else(|| anyhow::anyhow!("missing secure-startup identity"))?;
    let installation_id = InstallationId::try_from_uuid(installation_id)?;
    let generation = arguments
        .bootstrap_generation
        .filter(|generation| *generation != 0)
        .ok_or_else(|| anyhow::anyhow!("missing secure-startup generation"))?;
    complete_foreground_keyring_bootstrap_at_installation_root(
        installation_data_root,
        installation_id,
        generation,
    )
    .await
    .map_err(Into::into)
}

fn installed_secret_backend_policy(
    installation_data_root: &Path,
) -> Result<InstalledSecretBackendPolicy> {
    let executable = std::env::current_exe()?;
    let Some(program_root) =
        installation_root_for_installed_program(&executable, ProgramName::Service)?
    else {
        return Ok(InstalledSecretBackendPolicy::EncryptedFileOnly);
    };
    let Some(expected_data_root) = program_root.parent() else {
        return Ok(InstalledSecretBackendPolicy::EncryptedFileOnly);
    };
    let roots_match = std::fs::canonicalize(expected_data_root)
        .ok()
        .zip(std::fs::canonicalize(installation_data_root).ok())
        .is_some_and(|(expected, selected)| expected == selected);
    if !roots_match {
        return Ok(InstalledSecretBackendPolicy::EncryptedFileOnly);
    }
    #[cfg(target_os = "macos")]
    {
        return Ok(
            if active_native_trust_mode(&program_root)?
                == NativeTrustMode::DeveloperIdSignedAndNotarized
            {
                InstalledSecretBackendPolicy::PlatformKeyring
            } else {
                InstalledSecretBackendPolicy::EncryptedFileOnly
            },
        );
    }
    #[cfg(not(target_os = "macos"))]
    {
        Ok(InstalledSecretBackendPolicy::PlatformKeyring)
    }
}

fn combine_shutdown_results<T>(
    signal: Result<()>,
    service: Result<T>,
    evidence: Option<anyhow::Error>,
) -> Result<T> {
    let result = match (signal, service) {
        (Ok(()), Ok(outcome)) => Ok(outcome),
        (Err(signal), Ok(_)) => Err(signal),
        (Ok(()), Err(service)) => Err(service),
        (Err(signal), Err(service)) => Err(service.context(format!(
            "termination-signal handling also failed: {signal:#}"
        ))),
    };
    match (result, evidence) {
        (Ok(outcome), _) => Ok(outcome),
        (Err(primary), Some(evidence)) => Err(primary.context(format!(
            "shutdown-evidence publication also failed: {evidence:#}"
        ))),
        (Err(primary), None) => Err(primary),
    }
}

fn publish_startup(
    startup: Option<&ServiceStartupEvidenceWriter>,
    state: ServiceStartupState,
) -> Result<()> {
    startup
        .map(|startup| startup.publish(state))
        .transpose()
        .context("failed to publish installed-service startup evidence")?;
    Ok(())
}

fn fail_startup<T>(
    startup: Option<&ServiceStartupEvidenceWriter>,
    phase: ServiceStartupPhase,
    primary: anyhow::Error,
) -> Result<T> {
    match publish_startup(startup, ServiceStartupState::Failed { phase }) {
        Ok(()) => Err(primary),
        Err(evidence) => Err(primary.context(format!(
            "startup-evidence failure while recording {phase:?}: {evidence:#}"
        ))),
    }
}

fn load_config(
    config_file: Option<&std::path::Path>,
    data_dir: Option<PathBuf>,
    training_release_root: Option<PathBuf>,
) -> Result<AppConfig> {
    let mut environment = ConfigSources::process_environment();
    environment.remove(&OsString::from("MARKET_SQUAWK_LOG"));
    environment.remove(&OsString::from("MARKET_SQUAWK_EXTERNAL_NETWORK"));
    environment.remove(&OsString::from("MARKET_SQUAWK_PROVIDER_TERMS_ACCEPTED"));
    Ok(AppConfig::load(ConfigSources::new(
        config_file,
        &environment,
        ConfigOverrides {
            data_dir,
            training_release_root,
            ..ConfigOverrides::default()
        },
    ))?
    .with_installed_public_market_profiles()?)
}
