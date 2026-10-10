//! Cross-platform process-termination admission shared by installed executables.

use thiserror::Error;

/// One installed process's complete supported termination signal set.
#[cfg(unix)]
#[derive(Debug)]
pub struct TerminationSignals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl TerminationSignals {
    /// Installs SIGINT and SIGTERM before the owned service is published.
    pub fn install() -> Result<Self, TerminationSignalError> {
        Ok(Self {
            interrupt: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?,
            terminate: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?,
        })
    }

    /// Waits for the first admitted process-termination signal.
    pub async fn wait(&mut self) -> Result<(), TerminationSignalError> {
        tokio::select! {
            observed = self.interrupt.recv() => observed,
            observed = self.terminate.recv() => observed,
        }
        .ok_or(TerminationSignalError::Closed)
    }
}

/// One installed process's complete supported termination signal set.
#[cfg(windows)]
#[derive(Debug)]
pub struct TerminationSignals {
    ctrl_c: tokio::signal::windows::CtrlC,
    ctrl_break: tokio::signal::windows::CtrlBreak,
    ctrl_close: tokio::signal::windows::CtrlClose,
    ctrl_logoff: tokio::signal::windows::CtrlLogoff,
    ctrl_shutdown: tokio::signal::windows::CtrlShutdown,
}

#[cfg(windows)]
impl TerminationSignals {
    /// Installs all shutdown notifications supported by Tokio's Windows console integration.
    pub fn install() -> Result<Self, TerminationSignalError> {
        Ok(Self {
            ctrl_c: tokio::signal::windows::ctrl_c()?,
            ctrl_break: tokio::signal::windows::ctrl_break()?,
            ctrl_close: tokio::signal::windows::ctrl_close()?,
            ctrl_logoff: tokio::signal::windows::ctrl_logoff()?,
            ctrl_shutdown: tokio::signal::windows::ctrl_shutdown()?,
        })
    }

    /// Waits for the first admitted process-termination signal.
    pub async fn wait(&mut self) -> Result<(), TerminationSignalError> {
        tokio::select! {
            observed = self.ctrl_c.recv() => observed,
            observed = self.ctrl_break.recv() => observed,
            observed = self.ctrl_close.recv() => observed,
            observed = self.ctrl_logoff.recv() => observed,
            observed = self.ctrl_shutdown.recv() => observed,
        }
        .ok_or(TerminationSignalError::Closed)
    }
}

/// Portable fallback termination listener.
#[cfg(not(any(unix, windows)))]
#[derive(Debug)]
pub struct TerminationSignals;

#[cfg(not(any(unix, windows)))]
impl TerminationSignals {
    /// Constructs the portable Ctrl-C listener.
    pub const fn install() -> Result<Self, TerminationSignalError> {
        Ok(Self)
    }

    /// Waits for portable Ctrl-C termination.
    pub async fn wait(&mut self) -> Result<(), TerminationSignalError> {
        tokio::signal::ctrl_c().await.map_err(Into::into)
    }
}

/// Termination listener construction or delivery failure.
#[derive(Debug, Error)]
pub enum TerminationSignalError {
    /// The operating system rejected signal-listener construction.
    #[error("failed to install the process termination listener")]
    Io(#[from] std::io::Error),
    /// Every registered listener closed before delivering a signal.
    #[error("the process termination listener closed unexpectedly")]
    Closed,
}
