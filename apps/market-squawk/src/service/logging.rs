//! Process-owned structured-log and terminal-tracing bootstrap.

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use market_squawk_domain::Timestamp;
use market_squawk_platform::{AppConfig, LocalPaths, PathError};
use thiserror::Error;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt as _};

use crate::application::logs::{
    LogStoragePolicy, RedactedEventFormatter, StructuredLogDrain, StructuredLogDrainEvidence,
    StructuredLogError, StructuredLogLayer, StructuredLogStore, StructuredLogWorker,
};

const STRUCTURED_LOG_QUEUE_CAPACITY: usize = 8_192;

/// Terminal representation selected by the installed-service command line.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TerminalLogFormat {
    /// Escaped, human-readable local diagnostics.
    Human,
    /// Redacted JSON Lines for local machine processing.
    Json,
}

/// Sole process owner of the installed service's tracing subscriber and persistence worker.
#[derive(Debug)]
pub struct InstalledServiceLogging {
    store: Arc<StructuredLogStore>,
    drain: StructuredLogDrain,
    worker: Option<StructuredLogWorker>,
}

impl InstalledServiceLogging {
    /// Opens bounded storage and installs one subscriber containing persistence and terminal layers.
    ///
    /// The effective configuration must already be loaded so the structured store is confined to
    /// the selected workspace. Calling this more than once fails without replacing the process
    /// subscriber.
    pub fn install(
        config: &AppConfig,
        filter: &str,
        terminal_format: TerminalLogFormat,
    ) -> Result<Self, InstalledServiceLoggingError> {
        let filter =
            EnvFilter::try_new(filter).map_err(|_| InstalledServiceLoggingError::InvalidFilter)?;
        let paths = LocalPaths::prepare(config.data_dir())?;
        let store = Arc::new(StructuredLogStore::try_open(
            paths.control_root()?,
            LogStoragePolicy::default(),
            current_timestamp()?,
        )?);
        let (structured, drain, mut worker) =
            StructuredLogLayer::try_spawn(Arc::clone(&store), STRUCTURED_LOG_QUEUE_CAPACITY)?;
        let formatter = match terminal_format {
            TerminalLogFormat::Human => RedactedEventFormatter::human(),
            TerminalLogFormat::Json => RedactedEventFormatter::json(),
        };
        let subscriber = tracing_subscriber::registry()
            .with(filter)
            .with(structured)
            .with(
                tracing_subscriber::fmt::layer()
                    .event_format(formatter)
                    .with_ansi(terminal_format == TerminalLogFormat::Human),
            );
        if tracing::subscriber::set_global_default(subscriber).is_err() {
            worker.shutdown(Duration::from_secs(5))?;
            return Err(InstalledServiceLoggingError::SubscriberAlreadyInstalled);
        }
        Ok(Self {
            store,
            drain,
            worker: Some(worker),
        })
    }

    /// Returns the sole structured store shared with operations query/export services.
    #[must_use]
    pub fn store(&self) -> Arc<StructuredLogStore> {
        Arc::clone(&self.store)
    }

    /// Returns a flush handle over the same bounded persistence pipeline.
    #[must_use]
    pub fn drain(&self) -> StructuredLogDrain {
        self.drain.clone()
    }

    /// Drains every previously admitted event, joins the worker, and returns exact accounting.
    pub fn shutdown(
        &mut self,
        timeout: Duration,
    ) -> Result<StructuredLogDrainEvidence, InstalledServiceLoggingError> {
        let worker = self
            .worker
            .as_mut()
            .ok_or(InstalledServiceLoggingError::AlreadyShutdown)?;
        let evidence = worker.shutdown(timeout)?;
        self.worker = None;
        Ok(evidence)
    }
}

fn current_timestamp() -> Result<Timestamp, InstalledServiceLoggingError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| InstalledServiceLoggingError::InvalidSystemClock)?
        .as_nanos();
    let nanos =
        i64::try_from(nanos).map_err(|_| InstalledServiceLoggingError::InvalidSystemClock)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

/// Typed bootstrap or terminal-drain failure without secret-bearing values.
#[derive(Debug, Error)]
pub enum InstalledServiceLoggingError {
    /// The configured tracing filter is invalid.
    #[error("the installed-service tracing filter is invalid")]
    InvalidFilter,
    /// The system clock cannot produce a supported log timestamp.
    #[error("the installed-service clock is outside the supported timestamp range")]
    InvalidSystemClock,
    /// Another process-global subscriber was already installed.
    #[error("the installed-service tracing subscriber is already initialized")]
    SubscriberAlreadyInstalled,
    /// The logging worker was already shut down.
    #[error("the installed-service structured-log worker is already shut down")]
    AlreadyShutdown,
    /// One or more admitted log records were not durably persisted at shutdown.
    #[error("the installed-service structured-log drain was incomplete")]
    IncompleteDrain,
    /// The selected workspace cannot provide the required controlled paths.
    #[error("the installed-service logging workspace is unavailable")]
    Path(#[from] PathError),
    /// The bounded structured-log pipeline failed.
    #[error("the installed-service structured-log pipeline failed")]
    Structured(#[from] StructuredLogError),
}
