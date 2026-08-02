//! Explicit process-wide rustls crypto-provider installation boundary.

use std::sync::OnceLock;

use thiserror::Error;

const RING_PROVIDER_ID: &str = "rustls-ring-0.23.42";
static RING_INSTALLATION: OnceLock<Result<(), TlsProviderError>> = OnceLock::new();

/// Unforgeable proof that Market Squawk installed its pinned ring crypto provider.
///
/// Remote adapter constructors consume this capability before constructing an HTTP client. The
/// zero-sized token carries no secret; its private field makes the installation gate mandatory.
#[derive(Debug)]
pub struct TlsProviderCapability {
    _private: (),
}

impl TlsProviderCapability {
    /// Returns the pinned provider identity selected at the process boundary.
    pub const fn provider_id(&self) -> &'static str {
        RING_PROVIDER_ID
    }
}

/// Installs the pinned rustls ring provider once and mints a client-construction capability.
///
/// Repeated calls through this project boundary are idempotent. If any other code installed a
/// process provider first, installation fails closed because this boundary cannot prove its
/// configuration or provenance.
///
/// # Errors
///
/// Returns [`TlsProviderError::ProviderAlreadyInstalled`] when this boundary did not win the
/// one-time process installation.
pub fn install_ring_tls_provider() -> Result<TlsProviderCapability, TlsProviderError> {
    let installed = RING_INSTALLATION.get_or_init(|| {
        if rustls::crypto::CryptoProvider::get_default().is_some() {
            return Err(TlsProviderError::ProviderAlreadyInstalled);
        }
        rustls::crypto::ring::default_provider()
            .install_default()
            .map_err(|_| TlsProviderError::ProviderAlreadyInstalled)
    });
    match installed {
        Ok(()) => Ok(TlsProviderCapability { _private: () }),
        Err(error) => Err(*error),
    }
}

/// Failure to establish the process TLS crypto-provider boundary.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum TlsProviderError {
    /// A provider was installed outside this project-owned initialization boundary.
    #[error("a process rustls crypto provider was already installed outside Market Squawk")]
    ProviderAlreadyInstalled,
}
