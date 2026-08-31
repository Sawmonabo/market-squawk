//! Two-copy crash-safe local persistence for source authority state.

mod envelope;
mod filesystem;
mod recovery;

use std::fmt;
use std::io;
use std::path::Path;
use std::sync::Mutex;

use thiserror::Error;

use crate::paths::{ControlRoot, LocalPaths};

pub use self::envelope::{AuthorityCommitContext, AuthorityStateSnapshot};
use self::envelope::{Envelope, next_context, validate_payload_size};
use self::filesystem::{LifetimeLock, Slot, StateFiles};
use self::recovery::{Head, publish_envelope, reconcile};

/// A capability-confined, exclusively owned authority-state store.
///
/// Every acknowledged update is retained in two linked, independently verified slots. The
/// highest complete successor is authority; interrupted writes are repaired under the exclusive
/// lock before ordinary access. A hostile rollback of both slots requires an external monotonic
/// anchor and is outside this local-files-only durability contract.
pub struct LocalAuthorityStateStore {
    files: StateFiles,
    _lock: LifetimeLock,
    gate: Mutex<StoreGate>,
}

/// Exclusive lifetime guard for one selected installation's service process.
///
/// Unlike a general [`LocalAuthorityStateStore`], this capability can only be acquired at the
/// code-owned `installed-service/instance` authority beneath a prepared control root. Holding it
/// proves that no other service process owns that installation authority for the guard's lifetime.
pub struct InstalledServiceInstanceGuard {
    _store: LocalAuthorityStateStore,
}

/// One installation-global service instance bound linearly to one selected workspace.
///
/// The private fields and consuming bind prevent a caller from retaining the unbound installation
/// lock, substituting a second workspace, or supplying an unrelated source-authority store after
/// the workspace has been selected.
pub struct InstalledServiceSelectedWorkspaceGuard {
    _instance: InstalledServiceInstanceGuard,
    workspace_paths: LocalPaths,
}

impl fmt::Debug for InstalledServiceInstanceGuard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstalledServiceInstanceGuard")
            .field("authority", &"[INSTALLATION SERVICE INSTANCE]")
            .finish_non_exhaustive()
    }
}

impl InstalledServiceInstanceGuard {
    /// Acquires the exact installed-service instance authority under `installation_control`.
    ///
    /// # Errors
    ///
    /// Returns [`LocalAuthorityStateStoreError`] when the authority root is unsafe, unavailable,
    /// corrupt, or already owned by another service process.
    pub fn try_acquire(
        installation_control: &ControlRoot,
    ) -> Result<Self, LocalAuthorityStateStoreError> {
        const SERVICE_DIRECTORY: &str = "installed-service";
        const INSTANCE_DIRECTORY: &str = "instance";

        let store = LocalAuthorityStateStore::try_open(
            installation_control
                .root()
                .join(SERVICE_DIRECTORY)
                .join(INSTANCE_DIRECTORY),
        )?;
        Ok(Self { _store: store })
    }

    /// Consumes the installation instance and binds it to one already selected workspace.
    ///
    /// # Errors
    ///
    /// Returns [`crate::PathError`] when the selected workspace no longer exposes its exact
    /// prepared control-directory capability.
    pub fn bind_selected_workspace(
        self,
        workspace_paths: LocalPaths,
    ) -> Result<InstalledServiceSelectedWorkspaceGuard, crate::PathError> {
        workspace_paths.control_root()?.try_clone_directory()?;
        Ok(InstalledServiceSelectedWorkspaceGuard {
            _instance: self,
            workspace_paths,
        })
    }
}

impl fmt::Debug for InstalledServiceSelectedWorkspaceGuard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstalledServiceSelectedWorkspaceGuard")
            .field("authority", &"[INSTALLATION-BOUND SELECTED WORKSPACE]")
            .finish_non_exhaustive()
    }
}

impl InstalledServiceSelectedWorkspaceGuard {
    /// Returns the exact prepared workspace capability bound to the installation instance.
    #[must_use]
    pub const fn workspace_paths(&self) -> &LocalPaths {
        &self.workspace_paths
    }
}

struct StoreGate {
    recovery_required: bool,
}

impl fmt::Debug for LocalAuthorityStateStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalAuthorityStateStore")
            .field("directory", &"[redacted capability]")
            .field("lock", &"[redacted handle]")
            .finish()
    }
}

/// Fail-closed errors produced by [`LocalAuthorityStateStore`].
#[derive(Debug, Error)]
pub enum LocalAuthorityStateStoreError {
    /// The configured root is a symbolic link, reparse point, or non-directory.
    #[error("authority-state root is not a safe directory")]
    UnsafeRoot,
    /// A reserved authority-state name is not a regular single-link file.
    #[error("authority-state file has an unsafe or ambiguous type")]
    UnsafeFileType,
    /// Another store owner holds the lifetime lock.
    #[error("authority-state store is already locked")]
    AlreadyLocked,
    /// The serialized state exceeds the configured payload bound.
    #[error("authority-state payload is {bytes} bytes; maximum is {maximum}")]
    PayloadTooLarge {
        /// Observed payload size.
        bytes: usize,
        /// Maximum accepted payload size.
        maximum: usize,
    },
    /// A slot envelope exceeds its bounded on-disk representation.
    #[error("authority-state envelope is {bytes} bytes; maximum is {maximum}")]
    EnvelopeTooLarge {
        /// Observed envelope size.
        bytes: u64,
        /// Maximum accepted envelope size.
        maximum: u64,
    },
    /// An envelope is truncated, inconsistent, unsupported, or fails its digest.
    #[error("authority-state envelope is corrupt or unsupported")]
    CorruptEnvelope,
    /// The two final slots do not form one unambiguous successor chain.
    #[error("authority-state generations are ambiguous or unrelated")]
    GenerationConflict,
    /// The durable generation counter cannot advance without overflow.
    #[error("authority-state generation space is exhausted")]
    GenerationExhausted,
    /// Interrupted publication must be repaired before ordinary authority access.
    #[error("authority-state recovery is required")]
    RecoveryRequired,
    /// One new copy is durable but peer-copy acknowledgement is incomplete.
    #[error("authority-state peer-copy finalization is pending")]
    FinalizationPending,
    /// A prepared logical context no longer names the next durable generation.
    #[error("authority-state commit context is stale")]
    StaleCommitContext,
    /// A bounded buffer could not be allocated.
    #[error("authority-state bounded allocation failed")]
    Allocation,
    /// In-process operation serialization was poisoned by an abnormal unwind.
    #[error("authority-state serialization is unavailable")]
    WriterUnavailable,
    /// The platform cannot safely replace an inactive slot.
    #[error("atomic authority-state replacement is unsupported on this platform")]
    AtomicReplaceUnsupported,
    /// The platform has no implemented no-follow root-identity contract.
    #[error("secure authority-state root handling is unsupported on this platform")]
    SecureRootUnsupported,
    /// A post-installation read did not reproduce the submitted envelope.
    #[error("installed authority state failed canonical verification")]
    VerificationFailed,
    /// A filesystem operation failed without exposing path or payload data.
    #[error("authority-state filesystem operation failed during {operation}")]
    Io {
        /// Bounded operation identifier.
        operation: &'static str,
        /// Underlying operating-system error.
        #[source]
        source: io::Error,
    },
}

impl LocalAuthorityStateStore {
    /// Returns the maximum logical payload accepted by one authority-state commit.
    pub const fn maximum_payload_bytes() -> usize {
        envelope::MAX_PAYLOAD_BYTES
    }

    /// Opens or creates `root`, acquires its exclusive lifetime lock, and repairs a provably older
    /// missing or invalid peer before returning.
    pub fn try_open(root: impl AsRef<Path>) -> Result<Self, LocalAuthorityStateStoreError> {
        let (files, lock) = StateFiles::try_open(root.as_ref())?;
        let store = Self {
            files,
            _lock: lock,
            gate: Mutex::new(StoreGate {
                recovery_required: false,
            }),
        };
        let mut gate = store.lock_gate()?;
        store.reconcile_locked(&mut gate)?;
        drop(gate);
        Ok(store)
    }

    /// Loads the highest verified payload after completing deterministic peer recovery.
    pub fn load(&self) -> Result<Option<Vec<u8>>, LocalAuthorityStateStoreError> {
        self.load_snapshot()
            .map(|snapshot| snapshot.map(AuthorityStateSnapshot::into_payload))
    }

    /// Loads the highest verified payload with its whole-payload authentication context.
    pub fn load_snapshot(
        &self,
    ) -> Result<Option<AuthorityStateSnapshot>, LocalAuthorityStateStoreError> {
        let mut gate = self.lock_gate()?;
        let head = self.reconcile_locked(&mut gate)?;
        head.map(|head| head.envelope.into_snapshot()).transpose()
    }

    /// Returns the only context valid for the next logical commit.
    pub fn prepare_commit(&self) -> Result<AuthorityCommitContext, LocalAuthorityStateStoreError> {
        let mut gate = self.lock_gate()?;
        let head = self.reconcile_locked(&mut gate)?;
        next_context(head.as_ref().map(|head| &head.envelope))
    }

    /// Durably stores one payload in both fixed slots under a new logical commit context.
    pub fn store(&self, payload: &[u8]) -> Result<(), LocalAuthorityStateStoreError> {
        let mut gate = self.lock_gate()?;
        let head = self.reconcile_locked(&mut gate)?;
        let context = next_context(head.as_ref().map(|head| &head.envelope))?;
        self.store_locked(&mut gate, head, &context, payload)
    }

    /// Stores a payload authenticated against a previously prepared, still-current context.
    pub fn store_contextual(
        &self,
        context: &AuthorityCommitContext,
        payload: &[u8],
    ) -> Result<(), LocalAuthorityStateStoreError> {
        let mut gate = self.lock_gate()?;
        let head = self.reconcile_locked(&mut gate)?;
        if next_context(head.as_ref().map(|head| &head.envelope))? != *context {
            return Err(LocalAuthorityStateStoreError::StaleCommitContext);
        }
        self.store_locked(&mut gate, head, context, payload)
    }

    fn store_locked(
        &self,
        gate: &mut StoreGate,
        head: Option<Head>,
        context: &AuthorityCommitContext,
        payload: &[u8],
    ) -> Result<(), LocalAuthorityStateStoreError> {
        validate_payload_size(payload)?;
        let first_slot = head.as_ref().map_or(Slot::A, |head| head.slot.other());
        let second_generation = context
            .generation
            .checked_add(1)
            .ok_or(LocalAuthorityStateStoreError::GenerationExhausted)?;
        let first = Envelope::new(
            context.generation,
            context.predecessor,
            context,
            payload.to_vec(),
        )?;
        let second = Envelope::new(
            second_generation,
            first.envelope_digest,
            context,
            payload.to_vec(),
        )?;
        if publish_envelope(&self.files, first_slot, &first).is_err() {
            gate.recovery_required = true;
            return Err(LocalAuthorityStateStoreError::RecoveryRequired);
        }
        if publish_envelope(&self.files, first_slot.other(), &second).is_err() {
            gate.recovery_required = true;
            return Err(LocalAuthorityStateStoreError::FinalizationPending);
        }
        gate.recovery_required = false;
        Ok(())
    }

    fn reconcile_locked(
        &self,
        gate: &mut StoreGate,
    ) -> Result<Option<Head>, LocalAuthorityStateStoreError> {
        match reconcile(&self.files) {
            Ok(head) => {
                gate.recovery_required = false;
                Ok(head)
            }
            Err(_) if gate.recovery_required => {
                Err(LocalAuthorityStateStoreError::RecoveryRequired)
            }
            Err(error) => Err(error),
        }
    }

    fn lock_gate(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, StoreGate>, LocalAuthorityStateStoreError> {
        self.gate
            .lock()
            .map_err(|_| LocalAuthorityStateStoreError::WriterUnavailable)
    }
}
