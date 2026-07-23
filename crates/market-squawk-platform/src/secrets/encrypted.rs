//! Crash-consistent encrypted-file vault and unlock-rotation state machine.

use std::fmt;
use std::path::Path;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use zeroize::Zeroize as _;

use super::crypto::{
    EncryptedSet, MAX_ENTRIES, VAULT_VERSION, VaultAuthenticator, VaultAuthenticatorRole,
    decrypt_entries, validate_matching_keys,
};
use super::{
    LocalSecretStoreError, RotationAuthority, RotationOutcome, SecretBackend, SecretGeneration,
    SecretInteractionCapability, SecretKey, SecretOperationControl, SecretRef, SecretStore,
    SecretStoreCapabilities, map_state_error,
};
use crate::{AuthorityCommitContext, LocalAuthorityStateStore, SecretValue};

const MAX_SERIALIZED_VAULT_BYTES: usize = 7 * 1024 * 1024;
const AUTHENTICATION_DOMAIN: &[u8; 16] = b"MSQVAULTAUTH\0\0\0\0";

/// Argon2id/XChaCha20-Poly1305 fallback using crash-safe capability-confined publication.
pub struct EncryptedFileSecretStore {
    state: LocalAuthorityStateStore,
    unlocks: Mutex<UnlockState>,
}

impl EncryptedFileSecretStore {
    /// Opens an exclusively locked no-follow secret root.
    ///
    /// The unlock secret is retained only in zeroizing process memory and is never serialized.
    pub fn try_open(
        root: impl AsRef<Path>,
        unlock: SecretValue,
    ) -> Result<Self, LocalSecretStoreError> {
        Ok(Self {
            state: LocalAuthorityStateStore::try_open(root).map_err(map_state_error)?,
            unlocks: Mutex::new(UnlockState {
                current: unlock,
                candidate: None,
            }),
        })
    }

    /// Re-encrypts every entry under a new unlock using a durable three-phase transition.
    ///
    /// A prepared vault keeps the prior unlock authoritative, a committed vault makes the
    /// candidate authoritative while retaining recovery ciphertext, and the final stable vault
    /// removes the prior ciphertext. Typed recovery errors prevent callers from guessing which
    /// unlock owns authority after an ambiguous post-installation filesystem error.
    pub fn rotate_unlock(
        &mut self,
        new_unlock: SecretValue,
    ) -> Result<RotationOutcome, LocalSecretStoreError> {
        let mut unlocks = self
            .unlocks
            .lock()
            .map_err(|_| LocalSecretStoreError::WriterUnavailable)?;
        if unlocks.candidate.is_some() {
            return Err(LocalSecretStoreError::RotationRecoveryRequired);
        }
        let Some(loaded) = self.load_vault()? else {
            unlocks.current = new_unlock;
            return Ok(RotationOutcome::Complete);
        };
        let active = loaded.into_authenticated_stable(&unlocks.current)?;
        let plaintext = decrypt_entries(&active, &unlocks.current)?;
        let candidate = EncryptedSet::from_plaintext(&plaintext, &new_unlock)?;
        let prepared = self.publish_prepared(active, candidate, &unlocks.current, &new_unlock)?;

        let (prior, candidate) = prepared.into_prepared()?;
        let committed =
            match self.publish_committed(candidate, prior, &new_unlock, &unlocks.current) {
                Ok(committed) => committed,
                Err(_) => {
                    unlocks.candidate = Some(new_unlock);
                    return Err(LocalSecretStoreError::RotationRecoveryRequired);
                }
            };

        unlocks.current = new_unlock;
        let active = committed.into_active_from_committed()?;
        if self.publish_stable(active, &unlocks.current).is_err() {
            return Err(LocalSecretStoreError::RotationFinalizationPending);
        }
        Ok(RotationOutcome::Complete)
    }

    /// Resolves an interrupted rotation from the durable phase and removes the losing keyset.
    pub fn recover_rotation(&self) -> Result<RotationAuthority, LocalSecretStoreError> {
        let mut unlocks = self
            .unlocks
            .lock()
            .map_err(|_| LocalSecretStoreError::WriterUnavailable)?;
        let Some(loaded) = self.load_vault()? else {
            unlocks.candidate = None;
            return Ok(RotationAuthority::Prior);
        };
        let active_current = loaded.authenticates_active(&unlocks.current);
        let secondary_current = loaded.authenticates_secondary(&unlocks.current);
        let active_candidate = unlocks
            .candidate
            .as_ref()
            .is_some_and(|candidate| loaded.authenticates_active(candidate));

        match loaded.into_state() {
            VaultState::Stable { .. } => {
                if !active_current {
                    return Err(LocalSecretStoreError::AuthenticationFailed);
                }
                if unlocks.candidate.is_some() {
                    return Err(LocalSecretStoreError::CorruptVault);
                }
                Ok(RotationAuthority::Prior)
            }
            VaultState::Prepared { active, .. } => {
                if active_current {
                    if self.publish_stable(active, &unlocks.current).is_err() {
                        return Err(LocalSecretStoreError::RotationRecoveryRequired);
                    }
                    unlocks.candidate = None;
                    Ok(RotationAuthority::Prior)
                } else if secondary_current {
                    Err(LocalSecretStoreError::CandidateUnlockNotAuthoritative)
                } else {
                    Err(LocalSecretStoreError::AuthenticationFailed)
                }
            }
            VaultState::Committed { active, .. } => {
                if active_current {
                    unlocks.candidate = None;
                } else if active_candidate {
                    let candidate = unlocks
                        .candidate
                        .take()
                        .ok_or(LocalSecretStoreError::RotationRecoveryRequired)?;
                    unlocks.current = candidate;
                } else if secondary_current {
                    return Err(LocalSecretStoreError::SupersededUnlock);
                } else {
                    return Err(LocalSecretStoreError::AuthenticationFailed);
                }
                if self.publish_stable(active, &unlocks.current).is_err() {
                    return Err(LocalSecretStoreError::RotationFinalizationPending);
                }
                Ok(RotationAuthority::Candidate)
            }
        }
    }

    /// Idempotently removes prior-key recovery material after candidate authority is known.
    pub fn finalize_rotation(&self) -> Result<RotationOutcome, LocalSecretStoreError> {
        let unlocks = self
            .unlocks
            .lock()
            .map_err(|_| LocalSecretStoreError::WriterUnavailable)?;
        if unlocks.candidate.is_some() {
            return Err(LocalSecretStoreError::RotationRecoveryRequired);
        }
        let Some(loaded) = self.load_vault()? else {
            return Ok(RotationOutcome::Complete);
        };
        let active_current = loaded.authenticates_active(&unlocks.current);
        let secondary_current = loaded.authenticates_secondary(&unlocks.current);

        match loaded.into_state() {
            VaultState::Stable { .. } => {
                if !active_current {
                    return Err(LocalSecretStoreError::AuthenticationFailed);
                }
                Ok(RotationOutcome::Complete)
            }
            VaultState::Prepared { .. } => {
                if active_current {
                    Err(LocalSecretStoreError::RotationRecoveryRequired)
                } else if secondary_current {
                    Err(LocalSecretStoreError::CandidateUnlockNotAuthoritative)
                } else {
                    Err(LocalSecretStoreError::AuthenticationFailed)
                }
            }
            VaultState::Committed { active, .. } => {
                if !active_current {
                    return if secondary_current {
                        Err(LocalSecretStoreError::SupersededUnlock)
                    } else {
                        Err(LocalSecretStoreError::AuthenticationFailed)
                    };
                }
                if self.publish_stable(active, &unlocks.current).is_err() {
                    return Err(LocalSecretStoreError::RotationFinalizationPending);
                }
                Ok(RotationOutcome::Complete)
            }
        }
    }

    fn load_vault(&self) -> Result<Option<LoadedVault>, LocalSecretStoreError> {
        let Some(snapshot) = self.state.load_snapshot().map_err(map_state_error)? else {
            return Ok(None);
        };
        if snapshot.payload().len() > MAX_SERIALIZED_VAULT_BYTES {
            return Err(LocalSecretStoreError::CorruptVault);
        }
        let vault = serde_json::from_slice::<Vault>(snapshot.payload())
            .map_err(|_| LocalSecretStoreError::CorruptVault)?
            .validate()?;
        let authentication_digest = vault.authentication_digest(snapshot.context())?;
        Ok(Some(LoadedVault {
            vault,
            authentication_digest,
        }))
    }

    fn publish_stable(
        &self,
        active: EncryptedSet,
        unlock: &SecretValue,
    ) -> Result<(), LocalSecretStoreError> {
        self.publish_vault(|context| Vault::stable(active, unlock, context))
            .map(drop)
    }

    fn publish_prepared(
        &self,
        active: EncryptedSet,
        candidate: EncryptedSet,
        prior_unlock: &SecretValue,
        candidate_unlock: &SecretValue,
    ) -> Result<Vault, LocalSecretStoreError> {
        self.publish_vault(|context| {
            Vault::prepared(active, candidate, prior_unlock, candidate_unlock, context)
        })
    }

    fn publish_committed(
        &self,
        active: EncryptedSet,
        recovery: EncryptedSet,
        active_unlock: &SecretValue,
        recovery_unlock: &SecretValue,
    ) -> Result<Vault, LocalSecretStoreError> {
        self.publish_vault(|context| {
            Vault::committed(active, recovery, active_unlock, recovery_unlock, context)
        })
    }

    fn publish_vault(
        &self,
        build: impl FnOnce(&AuthorityCommitContext) -> Result<Vault, LocalSecretStoreError>,
    ) -> Result<Vault, LocalSecretStoreError> {
        let context = self.state.prepare_commit().map_err(map_state_error)?;
        let vault = build(&context)?;
        let mut bytes =
            serde_json::to_vec(&vault).map_err(|_| LocalSecretStoreError::CorruptVault)?;
        if bytes.len() > MAX_SERIALIZED_VAULT_BYTES {
            bytes.zeroize();
            return Err(LocalSecretStoreError::CapacityExceeded);
        }
        let result = self
            .state
            .store_contextual(&context, &bytes)
            .map_err(map_state_error);
        bytes.zeroize();
        result.map(|()| vault)
    }
}

impl fmt::Debug for EncryptedFileSecretStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("EncryptedFileSecretStore([REDACTED CAPABILITY])")
    }
}

impl SecretStore for EncryptedFileSecretStore {
    fn probe(
        &self,
        control: &SecretOperationControl,
    ) -> Result<SecretStoreCapabilities, LocalSecretStoreError> {
        let capabilities = encrypted_capabilities();
        control.preflight(capabilities)?;
        let unlocks = self
            .unlocks
            .lock()
            .map_err(|_| LocalSecretStoreError::WriterUnavailable)?;
        if unlocks.candidate.is_some() {
            return Err(LocalSecretStoreError::RotationRecoveryRequired);
        }
        drop(unlocks);
        control.read_postflight()?;
        Ok(capabilities)
    }

    fn create(
        &self,
        key: &SecretKey,
        generation: SecretGeneration,
        value: SecretValue,
        control: &SecretOperationControl,
    ) -> Result<SecretRef, LocalSecretStoreError> {
        let capabilities = encrypted_capabilities();
        control.preflight(capabilities)?;
        let reference = SecretRef::from_key(key, capabilities.backend(), generation)?;
        let unlocks = self
            .unlocks
            .lock()
            .map_err(|_| LocalSecretStoreError::WriterUnavailable)?;
        if unlocks.candidate.is_some() {
            return Err(LocalSecretStoreError::RotationRecoveryRequired);
        }
        let mut active = match self.load_vault()? {
            None => EncryptedSet::empty(&unlocks.current)?,
            Some(loaded) => loaded.into_authenticated_stable(&unlocks.current)?,
        };
        if active.contains(reference.locator()) {
            return Err(LocalSecretStoreError::Conflict);
        }
        if active.entries.len() == MAX_ENTRIES {
            return Err(LocalSecretStoreError::CapacityExceeded);
        }
        active.insert(reference.locator().to_owned(), &value, &unlocks.current)?;
        self.publish_stable(active, &unlocks.current)?;
        drop(unlocks);
        control.mutation_postflight()?;
        Ok(reference)
    }

    fn read(
        &self,
        reference: &SecretRef,
        control: &SecretOperationControl,
    ) -> Result<SecretValue, LocalSecretStoreError> {
        let capabilities = encrypted_capabilities();
        control.preflight(capabilities)?;
        require_backend(reference, capabilities.backend())?;
        let unlocks = self
            .unlocks
            .lock()
            .map_err(|_| LocalSecretStoreError::WriterUnavailable)?;
        if unlocks.candidate.is_some() {
            return Err(LocalSecretStoreError::RotationRecoveryRequired);
        }
        let loaded = self.load_vault()?.ok_or(LocalSecretStoreError::NotFound)?;
        let active = loaded.into_authenticated_stable(&unlocks.current)?;
        let value = active.decrypt(reference.locator(), &unlocks.current)?;
        drop(unlocks);
        control.read_postflight()?;
        Ok(value)
    }

    fn replace(
        &self,
        key: &SecretKey,
        current: &SecretRef,
        candidate_generation: SecretGeneration,
        value: SecretValue,
        control: &SecretOperationControl,
    ) -> Result<SecretRef, LocalSecretStoreError> {
        let capabilities = encrypted_capabilities();
        control.preflight(capabilities)?;
        require_backend(current, capabilities.backend())?;
        if candidate_generation <= current.generation()
            || SecretRef::from_key(key, capabilities.backend(), current.generation())? != *current
        {
            return Err(LocalSecretStoreError::Conflict);
        }
        let candidate = SecretRef::from_key(key, capabilities.backend(), candidate_generation)?;
        let unlocks = self
            .unlocks
            .lock()
            .map_err(|_| LocalSecretStoreError::WriterUnavailable)?;
        if unlocks.candidate.is_some() {
            return Err(LocalSecretStoreError::RotationRecoveryRequired);
        }
        let loaded = self.load_vault()?.ok_or(LocalSecretStoreError::NotFound)?;
        let mut active = loaded.into_authenticated_stable(&unlocks.current)?;
        if !active.contains(current.locator()) {
            return Err(LocalSecretStoreError::NotFound);
        }
        if active.contains(candidate.locator()) {
            return Err(LocalSecretStoreError::Conflict);
        }
        if active.entries.len() == MAX_ENTRIES {
            return Err(LocalSecretStoreError::CapacityExceeded);
        }
        active.insert(candidate.locator().to_owned(), &value, &unlocks.current)?;
        self.publish_stable(active, &unlocks.current)?;
        drop(unlocks);
        control.mutation_postflight()?;
        Ok(candidate)
    }

    fn delete(
        &self,
        reference: &SecretRef,
        control: &SecretOperationControl,
    ) -> Result<(), LocalSecretStoreError> {
        let capabilities = encrypted_capabilities();
        control.preflight(capabilities)?;
        require_backend(reference, capabilities.backend())?;
        let unlocks = self
            .unlocks
            .lock()
            .map_err(|_| LocalSecretStoreError::WriterUnavailable)?;
        if unlocks.candidate.is_some() {
            return Err(LocalSecretStoreError::RotationRecoveryRequired);
        }
        let loaded = self.load_vault()?.ok_or(LocalSecretStoreError::NotFound)?;
        let mut active = loaded.into_authenticated_stable(&unlocks.current)?;
        active.remove(reference.locator())?;
        self.publish_stable(active, &unlocks.current)?;
        drop(unlocks);
        control.mutation_postflight()
    }

    fn store(&self, key: &SecretKey, value: SecretValue) -> Result<(), LocalSecretStoreError> {
        let token = key.token()?;
        let unlocks = self
            .unlocks
            .lock()
            .map_err(|_| LocalSecretStoreError::WriterUnavailable)?;
        if unlocks.candidate.is_some() {
            return Err(LocalSecretStoreError::RotationRecoveryRequired);
        }
        let mut active = match self.load_vault()? {
            None => EncryptedSet::empty(&unlocks.current)?,
            Some(loaded) => loaded.into_authenticated_stable(&unlocks.current)?,
        };
        if !active.entries.contains_key(&token) && active.entries.len() == MAX_ENTRIES {
            return Err(LocalSecretStoreError::CapacityExceeded);
        }
        active.insert(token, &value, &unlocks.current)?;
        self.publish_stable(active, &unlocks.current)
    }

    fn load(&self, key: &SecretKey) -> Result<SecretValue, LocalSecretStoreError> {
        let token = key.token()?;
        let unlocks = self
            .unlocks
            .lock()
            .map_err(|_| LocalSecretStoreError::WriterUnavailable)?;
        if unlocks.candidate.is_some() {
            return Err(LocalSecretStoreError::RotationRecoveryRequired);
        }
        let loaded = self.load_vault()?.ok_or(LocalSecretStoreError::NotFound)?;
        let active_current = loaded.authenticates_active(&unlocks.current);
        let secondary_current = loaded.authenticates_secondary(&unlocks.current);

        match loaded.into_state() {
            VaultState::Stable { active, .. } => {
                if !active_current {
                    return Err(LocalSecretStoreError::AuthenticationFailed);
                }
                active.decrypt(&token, &unlocks.current)
            }
            VaultState::Prepared { .. } => {
                if active_current {
                    Err(LocalSecretStoreError::RotationRecoveryRequired)
                } else if secondary_current {
                    Err(LocalSecretStoreError::CandidateUnlockNotAuthoritative)
                } else {
                    Err(LocalSecretStoreError::AuthenticationFailed)
                }
            }
            VaultState::Committed { .. } => {
                if active_current {
                    Err(LocalSecretStoreError::RotationFinalizationPending)
                } else if secondary_current {
                    Err(LocalSecretStoreError::SupersededUnlock)
                } else {
                    Err(LocalSecretStoreError::AuthenticationFailed)
                }
            }
        }
    }
}

const fn encrypted_capabilities() -> SecretStoreCapabilities {
    SecretStoreCapabilities::new(
        SecretBackend::EncryptedFile,
        SecretInteractionCapability::Never,
    )
}

fn require_backend(
    reference: &SecretRef,
    expected: SecretBackend,
) -> Result<(), LocalSecretStoreError> {
    if reference.backend() == expected {
        Ok(())
    } else {
        Err(LocalSecretStoreError::InvalidReference)
    }
}

struct UnlockState {
    current: SecretValue,
    candidate: Option<SecretValue>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Vault {
    version: u16,
    state: VaultState,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
enum VaultState {
    Stable {
        active: EncryptedSet,
        authentication: VaultAuthenticator,
    },
    Prepared {
        active: EncryptedSet,
        candidate: EncryptedSet,
        active_authentication: VaultAuthenticator,
        candidate_authentication: VaultAuthenticator,
    },
    Committed {
        active: EncryptedSet,
        recovery: EncryptedSet,
        active_authentication: VaultAuthenticator,
        recovery_authentication: VaultAuthenticator,
    },
}

#[derive(Serialize)]
struct VaultBody<'a> {
    version: u16,
    state: VaultBodyState<'a>,
}

#[derive(Serialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
enum VaultBodyState<'a> {
    Stable {
        active: &'a EncryptedSet,
    },
    Prepared {
        active: &'a EncryptedSet,
        candidate: &'a EncryptedSet,
    },
    Committed {
        active: &'a EncryptedSet,
        recovery: &'a EncryptedSet,
    },
}

impl Vault {
    fn stable(
        active: EncryptedSet,
        unlock: &SecretValue,
        context: &AuthorityCommitContext,
    ) -> Result<Self, LocalSecretStoreError> {
        let digest = authentication_digest(VaultBodyState::Stable { active: &active }, context)?;
        Ok(Self {
            version: VAULT_VERSION,
            state: VaultState::Stable {
                active,
                authentication: VaultAuthenticator::seal(
                    VaultAuthenticatorRole::StableActive,
                    &digest,
                    unlock,
                )?,
            },
        })
    }

    fn prepared(
        active: EncryptedSet,
        candidate: EncryptedSet,
        prior_unlock: &SecretValue,
        candidate_unlock: &SecretValue,
        context: &AuthorityCommitContext,
    ) -> Result<Self, LocalSecretStoreError> {
        let digest = authentication_digest(
            VaultBodyState::Prepared {
                active: &active,
                candidate: &candidate,
            },
            context,
        )?;
        Ok(Self {
            version: VAULT_VERSION,
            state: VaultState::Prepared {
                active,
                candidate,
                active_authentication: VaultAuthenticator::seal(
                    VaultAuthenticatorRole::PreparedPrior,
                    &digest,
                    prior_unlock,
                )?,
                candidate_authentication: VaultAuthenticator::seal(
                    VaultAuthenticatorRole::PreparedCandidate,
                    &digest,
                    candidate_unlock,
                )?,
            },
        })
    }

    fn committed(
        active: EncryptedSet,
        recovery: EncryptedSet,
        active_unlock: &SecretValue,
        recovery_unlock: &SecretValue,
        context: &AuthorityCommitContext,
    ) -> Result<Self, LocalSecretStoreError> {
        let digest = authentication_digest(
            VaultBodyState::Committed {
                active: &active,
                recovery: &recovery,
            },
            context,
        )?;
        Ok(Self {
            version: VAULT_VERSION,
            state: VaultState::Committed {
                active,
                recovery,
                active_authentication: VaultAuthenticator::seal(
                    VaultAuthenticatorRole::CommittedCandidate,
                    &digest,
                    active_unlock,
                )?,
                recovery_authentication: VaultAuthenticator::seal(
                    VaultAuthenticatorRole::CommittedRecovery,
                    &digest,
                    recovery_unlock,
                )?,
            },
        })
    }

    fn validate(self) -> Result<Self, LocalSecretStoreError> {
        if self.version != VAULT_VERSION {
            return Err(LocalSecretStoreError::CorruptVault);
        }
        match &self.state {
            VaultState::Stable {
                active,
                authentication,
            } => {
                active.validate()?;
                authentication.validate(VaultAuthenticatorRole::StableActive)?;
            }
            VaultState::Prepared {
                active,
                candidate,
                active_authentication,
                candidate_authentication,
            } => {
                active.validate()?;
                candidate.validate()?;
                validate_matching_keys(active, candidate)?;
                active_authentication.validate(VaultAuthenticatorRole::PreparedPrior)?;
                candidate_authentication.validate(VaultAuthenticatorRole::PreparedCandidate)?;
            }
            VaultState::Committed {
                active,
                recovery,
                active_authentication,
                recovery_authentication,
            } => {
                active.validate()?;
                recovery.validate()?;
                validate_matching_keys(active, recovery)?;
                active_authentication.validate(VaultAuthenticatorRole::CommittedCandidate)?;
                recovery_authentication.validate(VaultAuthenticatorRole::CommittedRecovery)?;
            }
        }
        Ok(self)
    }

    fn authentication_digest(
        &self,
        context: &AuthorityCommitContext,
    ) -> Result<[u8; 32], LocalSecretStoreError> {
        let state = match &self.state {
            VaultState::Stable { active, .. } => VaultBodyState::Stable { active },
            VaultState::Prepared {
                active, candidate, ..
            } => VaultBodyState::Prepared { active, candidate },
            VaultState::Committed {
                active, recovery, ..
            } => VaultBodyState::Committed { active, recovery },
        };
        authentication_digest(state, context)
    }

    fn into_prepared(self) -> Result<(EncryptedSet, EncryptedSet), LocalSecretStoreError> {
        match self.state {
            VaultState::Prepared {
                active, candidate, ..
            } => Ok((active, candidate)),
            VaultState::Stable { .. } | VaultState::Committed { .. } => {
                Err(LocalSecretStoreError::CorruptVault)
            }
        }
    }

    fn into_active_from_committed(self) -> Result<EncryptedSet, LocalSecretStoreError> {
        match self.state {
            VaultState::Committed { active, .. } => Ok(active),
            VaultState::Stable { .. } | VaultState::Prepared { .. } => {
                Err(LocalSecretStoreError::CorruptVault)
            }
        }
    }
}

struct LoadedVault {
    vault: Vault,
    authentication_digest: [u8; 32],
}

impl LoadedVault {
    fn authenticates_active(&self, unlock: &SecretValue) -> bool {
        match &self.vault.state {
            VaultState::Stable { authentication, .. } => {
                self.authenticates(authentication, VaultAuthenticatorRole::StableActive, unlock)
            }
            VaultState::Prepared {
                active_authentication,
                ..
            } => self.authenticates(
                active_authentication,
                VaultAuthenticatorRole::PreparedPrior,
                unlock,
            ),
            VaultState::Committed {
                active_authentication,
                ..
            } => self.authenticates(
                active_authentication,
                VaultAuthenticatorRole::CommittedCandidate,
                unlock,
            ),
        }
    }

    fn authenticates_secondary(&self, unlock: &SecretValue) -> bool {
        match &self.vault.state {
            VaultState::Stable { .. } => false,
            VaultState::Prepared {
                candidate_authentication,
                ..
            } => self.authenticates(
                candidate_authentication,
                VaultAuthenticatorRole::PreparedCandidate,
                unlock,
            ),
            VaultState::Committed {
                recovery_authentication,
                ..
            } => self.authenticates(
                recovery_authentication,
                VaultAuthenticatorRole::CommittedRecovery,
                unlock,
            ),
        }
    }

    fn authenticates(
        &self,
        authentication: &VaultAuthenticator,
        role: VaultAuthenticatorRole,
        unlock: &SecretValue,
    ) -> bool {
        authentication
            .authenticate(role, &self.authentication_digest, unlock)
            .is_ok()
    }

    fn into_authenticated_stable(
        self,
        unlock: &SecretValue,
    ) -> Result<EncryptedSet, LocalSecretStoreError> {
        match &self.vault.state {
            VaultState::Stable { .. } => {
                if !self.authenticates_active(unlock) {
                    return Err(LocalSecretStoreError::AuthenticationFailed);
                }
            }
            VaultState::Prepared { .. } => {
                return Err(LocalSecretStoreError::RotationRecoveryRequired);
            }
            VaultState::Committed { .. } => {
                return Err(LocalSecretStoreError::RotationFinalizationPending);
            }
        }
        match self.vault.state {
            VaultState::Stable { active, .. } => Ok(active),
            VaultState::Prepared { .. } | VaultState::Committed { .. } => {
                Err(LocalSecretStoreError::CorruptVault)
            }
        }
    }

    fn into_state(self) -> VaultState {
        self.vault.state
    }
}

fn authentication_digest(
    state: VaultBodyState<'_>,
    context: &AuthorityCommitContext,
) -> Result<[u8; 32], LocalSecretStoreError> {
    let mut body = serde_json::to_vec(&VaultBody {
        version: VAULT_VERSION,
        state,
    })
    .map_err(|_| LocalSecretStoreError::CorruptVault)?;
    if body.len() > MAX_SERIALIZED_VAULT_BYTES {
        body.zeroize();
        return Err(LocalSecretStoreError::CapacityExceeded);
    }
    let body_len = u64::try_from(body.len()).map_err(|_| LocalSecretStoreError::Allocation)?;
    let mut hasher = Sha256::new();
    hasher.update(AUTHENTICATION_DOMAIN);
    hasher.update(context.authentication_bytes());
    hasher.update(body_len.to_be_bytes());
    hasher.update(&body);
    body.zeroize();
    Ok(hasher.finalize().into())
}
