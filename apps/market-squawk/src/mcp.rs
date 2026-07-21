//! Sole shipping MCP composition for the application.
//!
//! The superseded compatibility server is intentionally absent so every caller crosses the
//! hardened lifecycle, durable-audit, and bounded-result boundary.
//!
//! ```compile_fail
//! use market_squawk::mcp::McpServer;
//! ```

mod artifact;
mod audit;
mod journal_worker;
mod services;

use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use artifact::ControlledArtifactRepository;
use audit::DurableAuditSink;
use market_squawk_mcp::{
    ArtifactError, McpLimitError, McpLimitSpec, McpLimits, McpServer as HardenedMcpServer,
    ServerError, ServerExit,
};
use market_squawk_platform::{JournalFileFormat, JournalSelectionError, LocalPaths, PathError};
use market_squawk_services::ServiceCapabilityError;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::sync::CancellationToken;

use journal_worker::JournalWorkerShutdown;
use services::{LocalToolServices, LocalToolServicesError};

pub use audit::LocalAuditError;
pub use journal_worker::JournalWorkerStartError;

use crate::diagnostic_engine::SharedDiagnosticEngine;

const LOCAL_MCP_MAXIMUM_ARTIFACT_BYTES: usize = 64 * 1024 * 1024;

/// Shipping local MCP ownership composed over the hardened protocol crate.
pub struct LocalMcpComposition {
    server: HardenedMcpServer<LocalToolServices>,
    audit: Arc<DurableAuditSink>,
    services: Arc<LocalToolServices>,
    journal_shutdown_timeout: Duration,
}

impl std::fmt::Debug for LocalMcpComposition {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalMcpComposition")
            .field("server", &self.server)
            .field("audit", &"[DURABLE BOUNDED AUDIT]")
            .field("services", &"[BOUNDED LOCAL SERVICES]")
            .field("journal_shutdown_timeout", &self.journal_shutdown_timeout)
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
        diagnostic_engine: SharedDiagnosticEngine,
        journal_source: &str,
        journal_format: Option<JournalFileFormat>,
    ) -> std::result::Result<Self, LocalMcpCompositionError> {
        let control = paths.control_root()?.try_clone_directory()?;
        let audit = Arc::new(DurableAuditSink::try_new(control)?);
        let maximum_artifact_bytes = NonZeroUsize::new(LOCAL_MCP_MAXIMUM_ARTIFACT_BYTES)
            .ok_or(LocalMcpCompositionError::InvalidArtifactLimit)?;
        let artifacts = Arc::new(ControlledArtifactRepository::try_new(
            paths.artifacts()?.clone(),
            maximum_artifact_bytes,
        )?);
        let journal_target =
            paths.configured_journal_read_target(journal_source, journal_format)?;
        let services = Arc::new(
            LocalToolServices::try_new(diagnostic_engine, journal_target).map_err(|error| {
                match error {
                    LocalToolServicesError::Capability(error) => {
                        LocalMcpCompositionError::Capability(error)
                    }
                    LocalToolServicesError::JournalWorker(error) => {
                        LocalMcpCompositionError::JournalWorkerStart(error)
                    }
                }
            })?,
        );
        let limit_spec = McpLimitSpec::default();
        let journal_shutdown_timeout = limit_spec.shutdown_timeout;
        let limits = McpLimits::try_from(limit_spec)?;
        let server =
            HardenedMcpServer::try_new(Arc::clone(&services), limits, audit.clone(), artifacts)?;
        Ok(Self {
            server,
            audit,
            services,
            journal_shutdown_timeout,
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
            services,
            journal_shutdown_timeout,
        } = self;
        let server = server.serve_stdio(cancellation).await;
        let worker = shutdown_journal_worker(&services, journal_shutdown_timeout).await?;
        finish_hardened_session(server, audit.flush(), worker)
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
            services,
            journal_shutdown_timeout,
        } = self;
        let server = server
            .serve_unverified_io(reader, writer, cancellation)
            .await;
        let worker = shutdown_journal_worker(&services, journal_shutdown_timeout).await?;
        finish_hardened_session(server, audit.flush(), worker)
    }
}

async fn shutdown_journal_worker(
    services: &LocalToolServices,
    shutdown_timeout: Duration,
) -> Result<JournalWorkerShutdown, LocalMcpCompositionError> {
    let deadline = tokio::time::Instant::now()
        .checked_add(shutdown_timeout)
        .ok_or(LocalMcpCompositionError::InvalidShutdownDeadline)?;
    Ok(services.shutdown(deadline).await)
}

fn finish_hardened_session(
    server: std::result::Result<ServerExit, ServerError>,
    audit: std::result::Result<(), LocalAuditError>,
    worker: JournalWorkerShutdown,
) -> std::result::Result<ServerExit, LocalMcpCompositionError> {
    let journal_worker = match worker {
        JournalWorkerShutdown::Joined | JournalWorkerShutdown::AlreadyTerminal => None,
        JournalWorkerShutdown::Panicked => Some(JournalWorkerTerminalError::Panicked),
        JournalWorkerShutdown::Transferred => {
            Some(JournalWorkerTerminalError::ShutdownDeadlineExceeded)
        }
    };
    if let Some(journal_worker) = journal_worker {
        return Err(LocalMcpCompositionError::SessionTermination {
            server: server.err().map(Box::new),
            audit: audit.err().map(Box::new),
            journal_worker,
        });
    }
    match (server, audit) {
        (Ok(exit), Ok(())) => Ok(exit),
        (Err(server), Ok(())) => Err(LocalMcpCompositionError::Server(server)),
        (Ok(_exit), Err(audit)) => Err(LocalMcpCompositionError::Audit(audit)),
        (Err(server), Err(audit)) => {
            Err(LocalMcpCompositionError::ServerAndAudit { server, audit })
        }
    }
}

/// Terminal failure of the dedicated configured-journal worker.
#[derive(Clone, Copy, Debug, Error)]
pub enum JournalWorkerTerminalError {
    #[error("dedicated configured-journal worker panicked")]
    Panicked,
    #[error("dedicated configured-journal worker exceeded its shutdown deadline")]
    ShutdownDeadlineExceeded,
}

/// Shipping MCP composition, session, or durable-drain failure.
#[derive(Debug, Error)]
pub enum LocalMcpCompositionError {
    #[error(transparent)]
    Path(#[from] PathError),
    #[error(transparent)]
    JournalSelection(#[from] JournalSelectionError),
    #[error(transparent)]
    Limits(#[from] McpLimitError),
    #[error(transparent)]
    Artifact(#[from] ArtifactError),
    #[error(transparent)]
    Capability(#[from] ServiceCapabilityError),
    #[error(transparent)]
    JournalWorkerStart(#[from] JournalWorkerStartError),
    #[error(transparent)]
    Audit(#[from] LocalAuditError),
    #[error(transparent)]
    Server(#[from] ServerError),
    #[error("local MCP artifact limit is invalid")]
    InvalidArtifactLimit,
    #[error("local MCP journal-worker shutdown deadline is invalid")]
    InvalidShutdownDeadline,
    #[error("local MCP server failed and audit drain also failed")]
    ServerAndAudit {
        #[source]
        server: ServerError,
        audit: LocalAuditError,
    },
    #[error("local MCP session ended with a journal-worker lifecycle failure")]
    SessionTermination {
        server: Option<Box<ServerError>>,
        audit: Option<Box<LocalAuditError>>,
        journal_worker: JournalWorkerTerminalError,
    },
}
