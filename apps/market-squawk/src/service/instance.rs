//! Installation ownership acquired before startup evidence or logging can be changed.

use std::path::Path;

use market_squawk_platform::{InstalledServiceInstanceGuard, LocalPaths};

use super::{
    EphemeralVerificationRoot, InstalledServiceError, prepare_installation_paths, runtime,
};

/// The selected installation paths paired with their sole process capability.
/// Workspace binding consumes this capability exactly once during service composition.
#[derive(Debug)]
pub struct InstalledServiceInstance {
    pub(super) paths: LocalPaths,
    pub(super) guard: InstalledServiceInstanceGuard,
    pub(super) ephemeral_verification_credentials: bool,
}

impl InstalledServiceInstance {
    /// Acquires installation ownership before opening shared startup state or logging.
    pub fn try_acquire(root: impl AsRef<Path>) -> Result<Self, InstalledServiceError> {
        let paths = prepare_installation_paths(root.as_ref())?;
        let guard = runtime::acquire_instance(&paths)?;
        Ok(Self {
            paths,
            guard,
            ephemeral_verification_credentials: false,
        })
    }

    /// Acquires a previously validated fresh verification root, retaining its cleanup policy.
    pub fn try_acquire_ephemeral(
        root: EphemeralVerificationRoot,
    ) -> Result<Self, InstalledServiceError> {
        let mut instance = Self::try_acquire(root.as_path())?;
        instance.ephemeral_verification_credentials = true;
        Ok(instance)
    }

    /// Exact installation root owned by this process capability.
    pub fn root(&self) -> &Path {
        self.paths.root()
    }

    /// Retains only the lock lifetime through final status publication and log drain.
    /// This hold cannot bind a workspace or start another service.
    pub fn retain_until_shutdown(&self) -> impl Send + Sync + use<> {
        self.guard.retain_until_shutdown()
    }
}
