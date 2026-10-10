//! Retained automatic vault access and explicit, provider-scoped application locking.

use std::{
    fmt,
    path::Path,
    sync::{Mutex, MutexGuard},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use zeroize::Zeroizing;

use super::{
    EncryptedFileFallbackStatus, EncryptedFileUnlockCapability, LocalSecretStoreError,
    OsKeyringSecretStore, PreferredSecretStore, SecretBackend, SecretCancellation,
    SecretDeletionDisposition, SecretGeneration, SecretInteractionPolicy, SecretKey,
    SecretMutationDisposition, SecretMutationFailure, SecretMutationPlan, SecretOperationControl,
    SecretReconciliationObservation, SecretRef, SecretStore, SecretStoreCapabilities,
    map_state_error, secret_values_match,
};
use crate::{LocalAuthorityStateStore, SecretValue};

const FORMAT_VERSION: u16 = 1;
const ACCESS_DIRECTORY: &str = "access-policy";
const MAXIMUM_DOCUMENT_BYTES: usize = 16 * 1024;

/// User-selected application locking; ordinary installations default to automatic access.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SecretAccessPolicy {
    /// Whether a user-selected application unlock protects this vault.
    pub enabled: bool,
    /// Whether the unlock is remembered in the operating-system credential store.
    pub remember_in_keychain: bool,
    /// Optional interval since explicit authentication; absent means no timed reauthentication.
    pub reauthenticate_after_seconds: Option<u64>,
}

impl SecretAccessPolicy {
    fn validate(self) -> Result<(), LocalSecretStoreError> {
        if (!self.enabled
            && (self.remember_in_keychain || self.reauthenticate_after_seconds.is_some()))
            || self.reauthenticate_after_seconds == Some(0)
        {
            return Err(LocalSecretStoreError::InvalidOperationControl);
        }
        if let Some(interval) = self.reauthenticate_after_seconds {
            unix_now()?
                .checked_add(interval)
                .ok_or(LocalSecretStoreError::InvalidOperationControl)?;
        }
        Ok(())
    }
}

/// Secret-free access state, independent of service and saved-data readiness.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SecretAccessState {
    /// The vault admits credential operations.
    Ready,
    /// Optional locking requires explicit authentication.
    Locked,
    /// Existing unlock authority or an interrupted policy change needs recovery.
    RecoveryRequired,
}

/// Closed presentation receipt. No key, locator, path or credential is included.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SecretAccessStatus {
    /// Current committed policy; an incomplete change is never reported as protected access.
    #[serde(flatten)]
    pub policy: SecretAccessPolicy,
    /// Current vault access state.
    pub access: SecretAccessState,
    /// A remembered unlock was committed and has not subsequently proved unavailable.
    pub remembered_access_available: bool,
    /// Persisted deadline for the service's credential-runtime drain timer.
    pub reauthenticate_at_unix_seconds: Option<u64>,
}

// Only generated random automatic unlocks are serialized here. User passwords are never
// serialized into this filesystem authority, including while a rotation is pending.
#[derive(Clone)]
struct AutomaticUnlock(Zeroizing<String>);

impl AutomaticUnlock {
    fn from_secret(secret: &SecretValue) -> Self {
        Self(Zeroizing::new(secret.expose_secret().to_owned()))
    }

    fn secret(&self) -> Result<SecretValue, LocalSecretStoreError> {
        SecretValue::new(self.0.to_string()).map_err(|_| LocalSecretStoreError::InvalidSecret)
    }
}

impl Serialize for AutomaticUnlock {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for AutomaticUnlock {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Zeroizing::new(String::deserialize(deserializer)?);
        if !valid_random_token(&value) {
            return Err(serde::de::Error::custom("invalid automatic unlock"));
        }
        Ok(Self(value))
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AccessRecord {
    policy: SecretAccessPolicy,
    automatic_unlock: Option<AutomaticUnlock>,
    explicitly_locked: bool,
    authenticated_at_unix_seconds: Option<u64>,
    remembered_plan: Option<SecretMutationPlan>,
    remembered_verified: bool,
}

impl AccessRecord {
    fn automatic() -> Self {
        Self {
            policy: SecretAccessPolicy::default(),
            automatic_unlock: None,
            explicitly_locked: false,
            authenticated_at_unix_seconds: None,
            remembered_plan: None,
            remembered_verified: false,
        }
    }

    fn validate(&self) -> Result<(), LocalSecretStoreError> {
        self.policy
            .validate()
            .map_err(|_| LocalSecretStoreError::CorruptVault)?;
        if (self.policy.enabled && self.automatic_unlock.is_some())
            || (!self.policy.enabled && (self.explicitly_locked || self.remembered_plan.is_some()))
            || (self.remembered_verified && self.remembered_plan.is_none())
            || (self.policy.remember_in_keychain != self.remembered_plan.is_some())
        {
            return Err(LocalSecretStoreError::CorruptVault);
        }
        let _deadline = self.reauthenticate_at()?;
        Ok(())
    }

    fn reauthenticate_at(&self) -> Result<Option<u64>, LocalSecretStoreError> {
        self.policy
            .reauthenticate_after_seconds
            .map(|interval| {
                self.authenticated_at_unix_seconds
                    .ok_or(LocalSecretStoreError::CorruptVault)?
                    .checked_add(interval)
                    .ok_or(LocalSecretStoreError::InvalidOperationControl)
            })
            .transpose()
    }

    fn authentication_expired(&self) -> Result<bool, LocalSecretStoreError> {
        let Some(deadline) = self.reauthenticate_at()? else {
            return Ok(false);
        };
        let now = unix_now()?;
        Ok(now >= deadline
            || self
                .authenticated_at_unix_seconds
                .is_some_and(|authenticated| now < authenticated))
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AccessDocument {
    format_version: u16,
    identity: String,
    remembered_generation: u64,
    active: AccessRecord,
    // Existing vault rotation determines which record owns authority after a crash.
    pending: Option<AccessRecord>,
}

impl AccessDocument {
    fn validate(&self) -> Result<(), LocalSecretStoreError> {
        if self.format_version != FORMAT_VERSION || !valid_random_token(&self.identity) {
            return Err(LocalSecretStoreError::CorruptVault);
        }
        self.active.validate()?;
        if let Some(pending) = &self.pending {
            pending.validate()?;
            if pending.remembered_plan.is_some()
                || (pending.automatic_unlock.is_none() && self.active.automatic_unlock.is_none())
            {
                return Err(LocalSecretStoreError::CorruptVault);
            }
        }
        if let Some(plan) = &self.active.remembered_plan {
            plan.validate_for(&self.remembered_key()?)?;
            if plan.target().generation().get() != self.remembered_generation {
                return Err(LocalSecretStoreError::CorruptVault);
            }
        }
        Ok(())
    }

    fn remembered_key(&self) -> Result<SecretKey, LocalSecretStoreError> {
        SecretKey::try_new("vault-access", &self.identity)
    }
}

struct AccessState {
    document: AccessDocument,
    held_unlock: Option<SecretValue>,
    faulted: bool,
    remembered_available: bool,
}

/// One access gate over the existing exact-reference secret router.
///
/// Automatic access uses a separately generated unlock in the private local authority. Optional
/// locking rotates that key out of both local state slots; only explicit OS remembering may
/// retain the user unlock. The application must drain credential-bearing runtimes before Lock
/// and at the published reauthentication deadline; this gate controls new secret operations.
pub struct AccessControlledSecretStore {
    store: PreferredSecretStore,
    keyring: OsKeyringSecretStore,
    authority: LocalAuthorityStateStore,
    state: Mutex<AccessState>,
}

impl fmt::Debug for AccessControlledSecretStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AccessControlledSecretStore([REDACTED])")
    }
}

impl AccessControlledSecretStore {
    /// Opens automatic access or returns a locked/recovery receipt for retained user protection.
    /// Existing vaults are never replaced when their original unlock is unavailable.
    ///
    /// # Errors
    /// Returns a redacted error for unsafe storage, corrupt authority, failed publication,
    /// cancellation or expiry. A retained password without automatic access returns a usable
    /// store whose status requires recovery rather than deleting its credential generations.
    pub fn try_open(
        vault_root: impl AsRef<Path>,
        keyring_namespace: &str,
        control: &SecretOperationControl,
    ) -> Result<Self, LocalSecretStoreError> {
        control.read_postflight()?;
        let root = vault_root.as_ref();
        let authority = LocalAuthorityStateStore::try_open(root.join(ACCESS_DIRECTORY))
            .map_err(map_state_error)?;
        let retained = read_document(&authority)?;
        let fresh = retained.is_none();
        let document = match retained {
            Some(document) => document,
            None => AccessDocument {
                format_version: FORMAT_VERSION,
                identity: random_unlock()?.expose_secret().to_owned(),
                remembered_generation: 0,
                active: AccessRecord::automatic(),
                pending: None,
            },
        };
        let result = Self {
            store: PreferredSecretStore::managed(keyring_namespace, root)?,
            keyring: OsKeyringSecretStore::try_new(keyring_namespace)?,
            authority,
            state: Mutex::new(AccessState {
                remembered_available: document.active.remembered_verified,
                document,
                held_unlock: None,
                faulted: false,
            }),
        };
        {
            let mut state = result.lock_state()?;
            if fresh {
                let automatic = random_unlock()?;
                if result.try_candidate(&automatic, control)? {
                    let mut document = state.document.clone();
                    document.active.automatic_unlock =
                        Some(AutomaticUnlock::from_secret(&automatic));
                    // Retain the key before the first encrypted marker can become durable.
                    result.commit(&mut state, document)?;
                    state.held_unlock = Some(automatic);
                } else {
                    let document = state.document.clone();
                    result.commit(&mut state, document)?;
                }
            } else {
                result.restore(&mut state, control)?;
            }
            if state.held_unlock.is_some() {
                result.ensure_authentication_marker(control)?;
            }
        }
        control.read_postflight()?;
        Ok(result)
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, AccessState>, LocalSecretStoreError> {
        self.state
            .lock()
            .map_err(|_| LocalSecretStoreError::WriterUnavailable)
    }

    fn fail_closed(&self, state: &mut AccessState) {
        state.faulted = true;
        state.held_unlock = None;
        let _closed = self.store.close_managed_fallback();
    }

    fn commit(
        &self,
        state: &mut AccessState,
        document: AccessDocument,
    ) -> Result<(), LocalSecretStoreError> {
        document.validate()?;
        let bytes = Zeroizing::new(
            serde_json::to_vec(&document).map_err(|_| LocalSecretStoreError::PublicationFailed)?,
        );
        if bytes.len() > MAXIMUM_DOCUMENT_BYTES {
            return Err(LocalSecretStoreError::CapacityExceeded);
        }
        if let Err(error) = self.authority.store(&bytes).map_err(map_state_error) {
            self.fail_closed(state);
            return Err(error);
        }
        state.document = document;
        Ok(())
    }

    fn try_candidate(
        &self,
        unlock: &SecretValue,
        control: &SecretOperationControl,
    ) -> Result<bool, LocalSecretStoreError> {
        match self
            .store
            .open_managed_fallback(duplicate(unlock)?, control)
        {
            Ok(()) => Ok(true),
            Err(
                LocalSecretStoreError::AuthenticationFailed
                | LocalSecretStoreError::CandidateUnlockNotAuthoritative
                | LocalSecretStoreError::SupersededUnlock,
            ) => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn restore(
        &self,
        state: &mut AccessState,
        control: &SecretOperationControl,
    ) -> Result<(), LocalSecretStoreError> {
        if let Some(pending) = state.document.pending.clone() {
            for record in [pending, state.document.active.clone()] {
                if let Some(automatic) = &record.automatic_unlock {
                    let unlock = automatic.secret()?;
                    if self.try_candidate(&unlock, control)? {
                        let mut document = state.document.clone();
                        document.active = record;
                        document.pending = None;
                        self.commit(state, document)?;
                        state.held_unlock = Some(unlock);
                        return Ok(());
                    }
                }
            }
            return Ok(());
        }
        let record = state.document.active.clone();
        if let Some(automatic) = record.automatic_unlock {
            let unlock = automatic.secret()?;
            if self.try_candidate(&unlock, control)? {
                state.held_unlock = Some(unlock);
            } else {
                self.fail_closed(state);
            }
            return Ok(());
        }
        self.expire(state)?;
        if !record.policy.enabled || state.document.active.explicitly_locked {
            return Ok(());
        }
        if let Some(plan) = record.remembered_plan {
            // Construction never inherits foreground permission to display an OS prompt.
            match self.keyring.read(plan.target(), &background_control()?) {
                Ok(unlock) if self.try_candidate(&unlock, control)? => {
                    if !state.document.active.remembered_verified {
                        let mut document = state.document.clone();
                        document.active.remembered_verified = true;
                        self.commit(state, document)?;
                    }
                    state.held_unlock = Some(unlock);
                    state.remembered_available = true;
                }
                Ok(_) => self.fail_closed(state),
                Err(
                    LocalSecretStoreError::NotFound
                    | LocalSecretStoreError::Locked
                    | LocalSecretStoreError::InteractionRequired
                    | LocalSecretStoreError::ProviderUnavailable
                    | LocalSecretStoreError::SessionUnavailable
                    | LocalSecretStoreError::UnsupportedOperation,
                ) => state.remembered_available = false,
                // Optional OS remembering must not prevent composing saved-data services.
                // Unexpected backend state remains an explicit recovery receipt.
                Err(_) => self.fail_closed(state),
            }
        }
        Ok(())
    }

    fn ensure_authentication_marker(
        &self,
        control: &SecretOperationControl,
    ) -> Result<(), LocalSecretStoreError> {
        let key = SecretKey::try_new("market-squawk-access", "vault-authentication")?;
        let plan = SecretMutationPlan::create(
            &key,
            SecretBackend::EncryptedFile,
            SecretGeneration::new(1)?,
        )?;
        if self.store.inspect_planned(&key, &plan, control)?
            == SecretReconciliationObservation::Absent
        {
            self.store
                .execute_planned(&key, &plan, random_unlock()?, control)
                .map_err(SecretMutationFailure::into_error)?;
        }
        Ok(())
    }

    fn expire(&self, state: &mut AccessState) -> Result<(), LocalSecretStoreError> {
        if !state.faulted
            && state.document.pending.is_none()
            && !state.document.active.explicitly_locked
            && state.document.active.authentication_expired()?
        {
            let mut document = state.document.clone();
            document.active.explicitly_locked = true;
            self.commit(state, document)?;
            state.held_unlock = None;
            self.store.close_managed_fallback()?;
        }
        Ok(())
    }

    fn status(&self, state: &mut AccessState) -> Result<SecretAccessStatus, LocalSecretStoreError> {
        self.expire(state)?;
        let record = &state.document.active;
        let access = if state.faulted || state.document.pending.is_some() {
            SecretAccessState::RecoveryRequired
        } else if state.held_unlock.is_some() {
            SecretAccessState::Ready
        } else if record.policy.enabled {
            SecretAccessState::Locked
        } else {
            SecretAccessState::RecoveryRequired
        };
        Ok(SecretAccessStatus {
            policy: record.policy,
            access,
            remembered_access_available: state.remembered_available && record.remembered_verified,
            reauthenticate_at_unix_seconds: record.reauthenticate_at()?,
        })
    }

    fn require_ready(&self, state: &mut AccessState) -> Result<(), LocalSecretStoreError> {
        match self.status(state)?.access {
            SecretAccessState::Ready => Ok(()),
            SecretAccessState::Locked => Err(LocalSecretStoreError::Locked),
            SecretAccessState::RecoveryRequired => {
                Err(LocalSecretStoreError::AuthorityRecoveryRequired)
            }
        }
    }

    fn with_ready<T>(
        &self,
        operation: impl FnOnce(&PreferredSecretStore) -> Result<T, LocalSecretStoreError>,
    ) -> Result<T, LocalSecretStoreError> {
        let mut state = self.lock_state()?;
        self.require_ready(&mut state)?;
        operation(&self.store)
    }

    fn with_ready_mutation<T>(
        &self,
        operation: impl FnOnce(&PreferredSecretStore) -> Result<T, SecretMutationFailure>,
    ) -> Result<T, SecretMutationFailure> {
        let mut state = self
            .lock_state()
            .map_err(SecretMutationFailure::no_effect)?;
        self.require_ready(&mut state)
            .map_err(SecretMutationFailure::no_effect)?;
        operation(&self.store)
    }

    fn forget(
        &self,
        state: &mut AccessState,
        control: &SecretOperationControl,
    ) -> Result<(), LocalSecretStoreError> {
        if let Some(plan) = state.document.active.remembered_plan.clone() {
            self.keyring
                .delete_planned(&state.document.remembered_key()?, &plan, control)
                .map_err(SecretMutationFailure::into_error)?;
        }
        let mut document = state.document.clone();
        document.active.policy.remember_in_keychain = false;
        document.active.remembered_plan = None;
        document.active.remembered_verified = false;
        self.commit(state, document)?;
        state.remembered_available = false;
        Ok(())
    }

    fn remember(
        &self,
        state: &mut AccessState,
        control: &SecretOperationControl,
    ) -> Result<(), LocalSecretStoreError> {
        if state.document.active.remembered_verified
            && state.document.active.policy.remember_in_keychain
        {
            return Ok(());
        }
        if state.document.active.remembered_plan.is_some() {
            self.forget(state, control)?;
        }
        let unlock = duplicate(
            state
                .held_unlock
                .as_ref()
                .ok_or(LocalSecretStoreError::Locked)?,
        )?;
        let key = state.document.remembered_key()?;
        let generation = state
            .document
            .remembered_generation
            .checked_add(1)
            .ok_or(LocalSecretStoreError::InvalidGeneration)?;
        let plan = self
            .keyring
            .plan_create(&key, SecretGeneration::new(generation)?, control)?;
        let mut document = state.document.clone();
        document.remembered_generation = generation;
        document.active.policy.remember_in_keychain = true;
        document.active.remembered_plan = Some(plan.clone());
        document.active.remembered_verified = false;
        // The exact deletion/reconciliation locator is durable before the OS mutation.
        self.commit(state, document)?;
        self.keyring
            .execute_planned(&key, &plan, unlock, control)
            .map_err(SecretMutationFailure::into_error)?;
        let mut document = state.document.clone();
        document.active.remembered_verified = true;
        self.commit(state, document)?;
        state.remembered_available = true;
        Ok(())
    }

    fn rotate_policy(
        &self,
        state: &mut AccessState,
        mut policy: SecretAccessPolicy,
        unlock: SecretValue,
        control: &SecretOperationControl,
    ) -> Result<(), LocalSecretStoreError> {
        self.forget(state, control)?;
        policy.remember_in_keychain = false;
        let mut record = AccessRecord {
            policy,
            automatic_unlock: if policy.enabled {
                None
            } else {
                Some(AutomaticUnlock::from_secret(&unlock))
            },
            explicitly_locked: false,
            authenticated_at_unix_seconds: if policy.enabled {
                Some(unix_now()?)
            } else {
                None
            },
            remembered_plan: None,
            remembered_verified: false,
        };
        record.validate()?;
        let mut document = state.document.clone();
        document.pending = Some(record.clone());
        self.commit(state, document)?;
        if let Err(error) = self
            .store
            .rotate_managed_fallback(duplicate(&unlock)?, control)
        {
            self.fail_closed(state);
            return Err(error);
        }
        // Rotation performs password derivation and durable vault publication. Start the
        // selected interval when that work finishes, not when the change was prepared.
        record.authenticated_at_unix_seconds = if policy.enabled {
            Some(unix_now()?)
        } else {
            None
        };
        let mut document = state.document.clone();
        document.active = record;
        document.pending = None;
        // Success replaces both authority slots, retiring every live local automatic-key copy.
        self.commit(state, document)?;
        state.held_unlock = Some(unlock);
        Ok(())
    }
}

impl SecretStore for AccessControlledSecretStore {
    fn access_status(&self) -> Result<SecretAccessStatus, LocalSecretStoreError> {
        let mut state = self.lock_state()?;
        self.status(&mut state)
    }

    fn configure_access(
        &self,
        policy: SecretAccessPolicy,
        new_unlock: Option<EncryptedFileUnlockCapability>,
        control: &SecretOperationControl,
    ) -> Result<SecretAccessStatus, LocalSecretStoreError> {
        control.read_postflight()?;
        policy.validate()?;
        let mut state = self.lock_state()?;
        self.require_ready(&mut state)?;
        let enabling = policy.enabled && !state.document.active.policy.enabled;
        if state.document.active.policy.enabled != policy.enabled {
            let unlock = if policy.enabled {
                new_unlock
                    .ok_or(LocalSecretStoreError::InvalidSecret)?
                    .into_secret()
            } else {
                if new_unlock.is_some() {
                    return Err(LocalSecretStoreError::InvalidOperationControl);
                }
                random_unlock()?
            };
            self.rotate_policy(&mut state, policy, unlock, control)?;
        } else {
            if new_unlock.is_some() {
                return Err(LocalSecretStoreError::InvalidOperationControl);
            }
            if !policy.remember_in_keychain {
                self.forget(&mut state, control)?;
            }
            let mut document = state.document.clone();
            document.active.policy.reauthenticate_after_seconds =
                policy.reauthenticate_after_seconds;
            self.commit(&mut state, document)?;
        }
        if policy.remember_in_keychain {
            self.remember(&mut state, control)?;
        }
        if enabling {
            // An optional foreground OS remembering operation may also take time. This is
            // the completion of a new authentication, not a renewal on ordinary policy edits.
            let mut document = state.document.clone();
            document.active.authenticated_at_unix_seconds = Some(unix_now()?);
            self.commit(&mut state, document)?;
        }
        control.mutation_postflight()?;
        self.status(&mut state)
    }

    fn unlock_access(
        &self,
        unlock: EncryptedFileUnlockCapability,
        control: &SecretOperationControl,
    ) -> Result<SecretAccessStatus, LocalSecretStoreError> {
        control.read_postflight()?;
        let unlock = unlock.into_secret();
        let mut state = self.lock_state()?;
        if state.faulted {
            self.store.close_managed_fallback()?;
            state.held_unlock = None;
            state.document =
                read_document(&self.authority)?.ok_or(LocalSecretStoreError::CorruptVault)?;
            state.faulted = false;
            self.restore(&mut state, control)?;
            if state.document.pending.is_none()
                && !state.document.active.policy.enabled
                && state.held_unlock.is_some()
            {
                return self.status(&mut state);
            }
        }
        if let Some(held) = &state.held_unlock {
            if !secret_values_match(held, &unlock) {
                return Err(LocalSecretStoreError::AuthenticationFailed);
            }
        } else if !self.try_candidate(&unlock, control)? {
            return Err(LocalSecretStoreError::AuthenticationFailed);
        }
        // Marker verification derives the vault key as well. Keep the old access decision
        // until all authentication work succeeds, then publish the fresh interval once.
        if let Err(error) = self.ensure_authentication_marker(control) {
            self.fail_closed(&mut state);
            return Err(error);
        }
        let mut document = state.document.clone();
        if let Some(pending) = document.pending.take() {
            // A target with an automatic key can be recovered unattended; an explicitly
            // supplied unlock in that transition is the prior user-held authority.
            if pending.automatic_unlock.is_none() {
                document.active = pending;
            }
        }
        document.active.explicitly_locked = false;
        document.active.authenticated_at_unix_seconds = if document.active.policy.enabled {
            Some(unix_now()?)
        } else {
            None
        };
        self.commit(&mut state, document)?;
        state.held_unlock = Some(unlock);
        if !state.document.active.policy.enabled && state.document.active.automatic_unlock.is_none()
        {
            // Adopt an existing password vault once, without saving that user password locally.
            self.rotate_policy(
                &mut state,
                SecretAccessPolicy::default(),
                random_unlock()?,
                control,
            )?;
        }
        control.mutation_postflight()?;
        self.status(&mut state)
    }

    fn lock_access(
        &self,
        control: &SecretOperationControl,
    ) -> Result<SecretAccessStatus, LocalSecretStoreError> {
        control.read_postflight()?;
        let mut state = self.lock_state()?;
        if state.faulted || state.document.pending.is_some() {
            return Err(LocalSecretStoreError::AuthorityRecoveryRequired);
        }
        if !state.document.active.policy.enabled {
            return Err(LocalSecretStoreError::UnsupportedOperation);
        }
        let mut document = state.document.clone();
        document.active.explicitly_locked = true;
        self.commit(&mut state, document)?;
        state.held_unlock = None;
        self.store.close_managed_fallback()?;
        control.mutation_postflight()?;
        self.status(&mut state)
    }

    fn forget_remembered_access(
        &self,
        control: &SecretOperationControl,
    ) -> Result<SecretAccessStatus, LocalSecretStoreError> {
        control.read_postflight()?;
        let mut state = self.lock_state()?;
        if state.faulted || state.document.pending.is_some() {
            return Err(LocalSecretStoreError::AuthorityRecoveryRequired);
        }
        self.forget(&mut state, control)?;
        control.mutation_postflight()?;
        self.status(&mut state)
    }

    fn encrypted_file_fallback_status(
        &self,
    ) -> Result<EncryptedFileFallbackStatus, LocalSecretStoreError> {
        Ok(
            if self.access_status()?.access == SecretAccessState::Ready {
                EncryptedFileFallbackStatus::Ready
            } else {
                EncryptedFileFallbackStatus::Locked
            },
        )
    }

    fn unlock_encrypted_file_fallback(
        &self,
        unlock: EncryptedFileUnlockCapability,
        control: &SecretOperationControl,
    ) -> Result<EncryptedFileFallbackStatus, LocalSecretStoreError> {
        self.unlock_access(unlock, control)?;
        self.encrypted_file_fallback_status()
    }

    fn lock_encrypted_file_fallback(
        &self,
        control: &SecretOperationControl,
    ) -> Result<EncryptedFileFallbackStatus, LocalSecretStoreError> {
        self.lock_access(control)?;
        self.encrypted_file_fallback_status()
    }

    fn probe(
        &self,
        control: &SecretOperationControl,
    ) -> Result<SecretStoreCapabilities, LocalSecretStoreError> {
        self.with_ready(|store| store.probe(control))
    }
    fn plan_create(
        &self,
        key: &SecretKey,
        generation: SecretGeneration,
        control: &SecretOperationControl,
    ) -> Result<SecretMutationPlan, LocalSecretStoreError> {
        self.with_ready(|store| store.plan_create(key, generation, control))
    }
    fn plan_replace(
        &self,
        key: &SecretKey,
        current: &SecretRef,
        generation: SecretGeneration,
        control: &SecretOperationControl,
    ) -> Result<SecretMutationPlan, LocalSecretStoreError> {
        self.with_ready(|store| store.plan_replace(key, current, generation, control))
    }
    fn execute_planned(
        &self,
        key: &SecretKey,
        plan: &SecretMutationPlan,
        value: SecretValue,
        control: &SecretOperationControl,
    ) -> Result<SecretMutationDisposition, SecretMutationFailure> {
        self.with_ready_mutation(|store| store.execute_planned(key, plan, value, control))
    }
    fn inspect_planned(
        &self,
        key: &SecretKey,
        plan: &SecretMutationPlan,
        control: &SecretOperationControl,
    ) -> Result<SecretReconciliationObservation, LocalSecretStoreError> {
        self.with_ready(|store| store.inspect_planned(key, plan, control))
    }
    fn matches_planned(
        &self,
        key: &SecretKey,
        plan: &SecretMutationPlan,
        expected: &SecretValue,
        control: &SecretOperationControl,
    ) -> Result<SecretReconciliationObservation, LocalSecretStoreError> {
        self.with_ready(|store| store.matches_planned(key, plan, expected, control))
    }
    fn delete_planned(
        &self,
        key: &SecretKey,
        plan: &SecretMutationPlan,
        control: &SecretOperationControl,
    ) -> Result<SecretDeletionDisposition, SecretMutationFailure> {
        self.with_ready_mutation(|store| store.delete_planned(key, plan, control))
    }
    fn create(
        &self,
        key: &SecretKey,
        generation: SecretGeneration,
        value: SecretValue,
        control: &SecretOperationControl,
    ) -> Result<SecretRef, LocalSecretStoreError> {
        self.with_ready(|store| store.create(key, generation, value, control))
    }
    fn read(
        &self,
        reference: &SecretRef,
        control: &SecretOperationControl,
    ) -> Result<SecretValue, LocalSecretStoreError> {
        self.with_ready(|store| store.read(reference, control))
    }
    fn replace(
        &self,
        key: &SecretKey,
        current: &SecretRef,
        generation: SecretGeneration,
        value: SecretValue,
        control: &SecretOperationControl,
    ) -> Result<SecretRef, LocalSecretStoreError> {
        self.with_ready(|store| store.replace(key, current, generation, value, control))
    }
    fn delete(
        &self,
        reference: &SecretRef,
        control: &SecretOperationControl,
    ) -> Result<(), LocalSecretStoreError> {
        self.with_ready(|store| store.delete(reference, control))
    }
    fn store(&self, key: &SecretKey, value: SecretValue) -> Result<(), LocalSecretStoreError> {
        self.with_ready(|store| store.store(key, value))
    }
    fn load(&self, key: &SecretKey) -> Result<SecretValue, LocalSecretStoreError> {
        self.with_ready(|store| store.load(key))
    }
}

fn read_document(
    authority: &LocalAuthorityStateStore,
) -> Result<Option<AccessDocument>, LocalSecretStoreError> {
    let Some(bytes) = authority
        .load()
        .map_err(map_state_error)?
        .map(Zeroizing::new)
    else {
        return Ok(None);
    };
    if bytes.len() > MAXIMUM_DOCUMENT_BYTES {
        return Err(LocalSecretStoreError::CorruptVault);
    }
    let document: AccessDocument =
        serde_json::from_slice(&bytes).map_err(|_| LocalSecretStoreError::CorruptVault)?;
    document.validate()?;
    Ok(Some(document))
}

fn duplicate(secret: &SecretValue) -> Result<SecretValue, LocalSecretStoreError> {
    SecretValue::new(secret.expose_secret().to_owned())
        .map_err(|_| LocalSecretStoreError::InvalidSecret)
}

fn random_unlock() -> Result<SecretValue, LocalSecretStoreError> {
    let mut bytes = Zeroizing::new([0_u8; 32]);
    getrandom::fill(&mut *bytes).map_err(|_| LocalSecretStoreError::RandomUnavailable)?;
    if bytes.iter().all(|byte| *byte == 0) {
        return Err(LocalSecretStoreError::RandomUnavailable);
    }
    SecretValue::new(super::crypto::encode_hex(&*bytes)?)
        .map_err(|_| LocalSecretStoreError::InvalidSecret)
}

fn valid_random_token(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn unix_now() -> Result<u64, LocalSecretStoreError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| LocalSecretStoreError::InvalidOperationControl)
}

fn background_control() -> Result<SecretOperationControl, LocalSecretStoreError> {
    SecretOperationControl::try_new(
        "vault-remembered-access",
        Instant::now() + Duration::from_secs(30),
        0,
        SecretInteractionPolicy::Forbid,
        SecretCancellation::new(),
    )
}
