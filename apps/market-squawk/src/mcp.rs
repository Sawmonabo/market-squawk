//! Sole shipping MCP composition for the application.
//!
//! The superseded compatibility server is intentionally absent so every caller crosses the
//! hardened lifecycle, durable-audit, and bounded-result boundary.
//!
//! ```compile_fail
//! use market_squawk::mcp::McpServer;
//! ```

mod audit;
#[cfg(test)]
mod journal_worker;
#[cfg(test)]
mod services;

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use audit::DurableAuditSink;
use market_squawk_mcp::{
    McpLimitError, McpLimitSpec, McpLimits, McpServer as HardenedMcpServer, ServerError, ServerExit,
};
use market_squawk_platform::{LocalPaths, PathError};
use market_squawk_services::ArtifactRepository;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::sync::CancellationToken;

pub use audit::LocalAuditError;

use crate::application::{Application, ApplicationShutdownReport};

/// Shipping local MCP ownership composed over the hardened protocol crate.
pub struct LocalMcpComposition {
    server: HardenedMcpServer<Application>,
    audit: Arc<DurableAuditSink>,
    application: ApplicationOwner,
    application_shutdown_timeout: Duration,
}

impl std::fmt::Debug for LocalMcpComposition {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalMcpComposition")
            .field("server", &self.server)
            .field("audit", &"[DURABLE BOUNDED AUDIT]")
            .field("application", &"[LIFECYCLE-OWNED APPLICATION]")
            .field(
                "application_shutdown_timeout",
                &self.application_shutdown_timeout,
            )
            .finish_non_exhaustive()
    }
}

impl LocalMcpComposition {
    /// Prepares local capabilities and acquires the hardened process SDK reaper before serving.
    ///
    /// # Errors
    ///
    /// Returns a typed error when local path, limit, audit, artifact, or server ownership cannot be
    /// established without opening a protocol session.
    pub fn try_new(
        paths: &LocalPaths,
        application: Arc<Application>,
        artifacts: Arc<dyn ArtifactRepository>,
    ) -> std::result::Result<Self, LocalMcpCompositionError> {
        let control = paths.control_root()?.try_clone_directory()?;
        let audit = Arc::new(DurableAuditSink::try_new(control)?);
        let limit_spec = McpLimitSpec::default();
        let application_shutdown_timeout = limit_spec.shutdown_timeout;
        let limits = McpLimits::try_from(limit_spec)?;
        let server =
            HardenedMcpServer::try_new(Arc::clone(&application), limits, audit.clone(), artifacts)?;
        Ok(Self {
            server,
            audit,
            application: ApplicationOwner::new(application),
            application_shutdown_timeout,
        })
    }

    /// Serves one inherited-stdio session and durably drains accepted audit records before return.
    pub async fn serve_stdio(
        self,
        cancellation: CancellationToken,
    ) -> std::result::Result<ServerExit, LocalMcpCompositionError> {
        let Self {
            server,
            audit,
            application,
            application_shutdown_timeout,
        } = self;
        let server = server.serve_stdio(cancellation).await;
        let application =
            shutdown_application(application.application(), application_shutdown_timeout).await;
        finish_hardened_session(server, audit.flush(), application)
    }

    /// Serves caller-supplied test or embedding I/O without asserting peer identity.
    pub async fn serve_unverified_io<R, W>(
        self,
        reader: R,
        writer: W,
        cancellation: CancellationToken,
    ) -> std::result::Result<ServerExit, LocalMcpCompositionError>
    where
        R: AsyncRead + Send + Unpin + 'static,
        W: AsyncWrite + Send + Unpin + 'static,
    {
        let Self {
            server,
            audit,
            application,
            application_shutdown_timeout,
        } = self;
        let server = server
            .serve_unverified_io(reader, writer, cancellation)
            .await;
        let application =
            shutdown_application(application.application(), application_shutdown_timeout).await;
        finish_hardened_session(server, audit.flush(), application)
    }
}

/// Fail-safe admission closure when composition/session ownership is dropped before async drain.
struct ApplicationOwner {
    application: Arc<Application>,
}

impl ApplicationOwner {
    const fn new(application: Arc<Application>) -> Self {
        Self { application }
    }

    fn application(&self) -> &Application {
        &self.application
    }
}

impl Drop for ApplicationOwner {
    fn drop(&mut self) {
        self.application.begin_shutdown();
    }
}

async fn shutdown_application(
    application: &Application,
    shutdown_timeout: Duration,
) -> Result<ApplicationShutdownReport, LocalMcpApplicationShutdownError> {
    application.begin_shutdown();
    let deadline = Instant::now()
        .checked_add(shutdown_timeout)
        .ok_or(LocalMcpApplicationShutdownError::InvalidDeadline)?;
    let report = application.shutdown(deadline).await;
    if report.is_complete() {
        Ok(report)
    } else {
        Err(LocalMcpApplicationShutdownError::Incomplete(report))
    }
}

fn finish_hardened_session(
    server: std::result::Result<ServerExit, ServerError>,
    audit: std::result::Result<(), LocalAuditError>,
    application: std::result::Result<ApplicationShutdownReport, LocalMcpApplicationShutdownError>,
) -> std::result::Result<ServerExit, LocalMcpCompositionError> {
    match (server, audit, application) {
        (Ok(exit), Ok(()), Ok(_report)) => Ok(exit),
        (Err(server), Ok(()), Ok(_report)) => Err(LocalMcpCompositionError::Server(server)),
        (Ok(_exit), Err(audit), Ok(_report)) => Err(LocalMcpCompositionError::Audit(audit)),
        (Ok(_exit), Ok(()), Err(application)) => {
            Err(LocalMcpCompositionError::Application(application))
        }
        (server, audit, application) => Err(LocalMcpCompositionError::SessionTermination {
            server: server.err().map(Box::new),
            audit: audit.err().map(Box::new),
            application: application.err(),
        }),
    }
}

/// Terminal failure of the lifecycle-owned application after an MCP session.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LocalMcpApplicationShutdownError {
    /// The bounded absolute shutdown deadline could not be represented.
    #[error("local MCP application shutdown deadline is invalid")]
    InvalidDeadline,
    /// One or more application domains did not reach its terminal barrier.
    #[error("local MCP application shutdown was incomplete")]
    Incomplete(ApplicationShutdownReport),
}

impl LocalMcpApplicationShutdownError {
    /// Returns per-domain shutdown evidence when shutdown was attempted.
    #[must_use]
    pub const fn report(self) -> Option<ApplicationShutdownReport> {
        match self {
            Self::InvalidDeadline => None,
            Self::Incomplete(report) => Some(report),
        }
    }
}

/// Shipping MCP composition, session, or durable-drain failure.
#[derive(Debug, Error)]
pub enum LocalMcpCompositionError {
    #[error(transparent)]
    Path(#[from] PathError),
    #[error(transparent)]
    Limits(#[from] McpLimitError),
    #[error(transparent)]
    Audit(#[from] LocalAuditError),
    #[error(transparent)]
    Server(#[from] ServerError),
    #[error(transparent)]
    Application(#[from] LocalMcpApplicationShutdownError),
    #[error("local MCP session termination did not complete cleanly")]
    SessionTermination {
        /// Protocol-server failure, if the server did not terminate normally.
        server: Option<Box<ServerError>>,
        /// Durable audit-drain failure, if buffered audit evidence was not flushed.
        audit: Option<Box<LocalAuditError>>,
        /// Bounded application-shutdown failure, if any domain missed its terminal barrier.
        application: Option<LocalMcpApplicationShutdownError>,
    },
}
