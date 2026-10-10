//! Durable, bounded local persistence for governance principals and redacted audit receipts.

use std::{
    collections::BTreeSet,
    fs::File,
    io::{Read as _, Seek as _, SeekFrom, Write as _},
    num::NonZeroU64,
    sync::{Arc, Mutex},
};

use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt as _};
use cap_std::fs::{Dir, OpenOptions};
use chrono::{DateTime, SecondsFormat, Utc};
use market_squawk_platform::{
    LocalAuthorityStateStore, LocalAuthorityStateStoreError, LocalPaths, PathError,
    SecretGeneration, SecretMutationPlan, SecretStore, SecretValue,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use subtle::ConstantTimeEq as _;
use thiserror::Error;
use uuid::Uuid;

use crate::application::governance::{
    GovernanceAuditError, GovernanceAuditReceipt, GovernanceAuditSink, GovernanceAuthority,
    GovernanceError, GovernanceLimits, GovernancePrincipal, GovernancePrincipalId,
    GovernancePrincipalRegistration, GovernanceRole, GovernanceRoleSet,
    governance_principal_secret_key, governance_secret_operation_control,
};

const REGISTRATION_DIRECTORY: &str = "governance-principals";
const LEGACY_REGISTRATION_SCHEMA_VERSION: u16 = 1;
const REGISTRATION_SCHEMA_VERSION: u16 = 2;
const MAXIMUM_REGISTRATIONS: usize = 64;
const MAXIMUM_REGISTRATION_BYTES: usize = 64 * 1024;

const AUDIT_FILE_NAME: &str = "governance-audit.msj";
const AUDIT_RECORD_SCHEMA_VERSION: u16 = 1;
const AUDIT_FRAME_MAGIC: [u8; 4] = *b"MSGV";
const AUDIT_FRAME_VERSION: u16 = 1;
const AUDIT_FRAME_HEADER_BYTES: usize = 4 + 2 + 4 + 32;
const MAXIMUM_AUDIT_RECORDS: usize = 16_384;
const MAXIMUM_AUDIT_RECORD_BYTES: usize = 8 * 1024;
const MAXIMUM_AUDIT_FILE_BYTES: u64 =
    MAXIMUM_AUDIT_RECORDS as u64 * (AUDIT_FRAME_HEADER_BYTES + MAXIMUM_AUDIT_RECORD_BYTES) as u64;
const MAXIMUM_AUDIT_ROLES: usize = 8;
const MAXIMUM_AUDIT_PRINCIPALS: usize = 2;

/// Sole composition seam for durable governance registration and audit authority.
pub(crate) struct GovernancePersistence {
    registrations: LocalAuthorityStateStore,
    audit: Arc<GovernanceAuditJournal>,
    provisioning: Mutex<()>,
}

pub(super) struct GovernanceProvisioningRequest {
    pub(super) primary_display_name: String,
    pub(super) primary_credential: SecretValue,
    pub(super) reviewer_display_name: String,
    pub(super) reviewer_credential: SecretValue,
    pub(super) limits: GovernanceLimits,
}

impl std::fmt::Debug for GovernanceProvisioningRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GovernanceProvisioningRequest")
            .field("primary_display_name", &self.primary_display_name)
            .field("primary_credential", &"[REDACTED]")
            .field("reviewer_display_name", &self.reviewer_display_name)
            .field("reviewer_credential", &"[REDACTED]")
            .field("limits", &self.limits)
            .finish()
    }
}

impl std::fmt::Debug for GovernancePersistence {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GovernancePersistence")
            .field("registrations", &self.registrations)
            .field("audit", &"[LOCKED REDACTED AUDIT AUTHORITY]")
            .finish()
    }
}

impl GovernancePersistence {
    /// Opens the two-copy principal store and the exclusively owned append journal.
    pub(crate) fn try_open(paths: &LocalPaths) -> Result<Self, GovernancePersistenceError> {
        let control = paths.control_root()?;
        let audit_control = control.try_clone_directory()?;
        let registrations =
            LocalAuthorityStateStore::try_open(control.root().join(REGISTRATION_DIRECTORY))?;
        let audit = Arc::new(GovernanceAuditJournal::try_open(audit_control)?);
        Ok(Self {
            registrations,
            audit,
            provisioning: Mutex::new(()),
        })
    }

    /// Loads a canonical registration set, or `None` when governance has not been provisioned.
    pub(crate) fn load_registrations(
        &self,
    ) -> Result<Option<Box<[GovernancePrincipalRegistration]>>, GovernancePersistenceError> {
        let _guard = self
            .provisioning
            .lock()
            .map_err(|_| GovernancePersistenceError::StateUnavailable)?;
        match self.load_state()? {
            ProvisioningState::Unprovisioned => Ok(None),
            ProvisioningState::Preparing { .. } => {
                Err(GovernancePersistenceError::ProvisioningRecoveryRequired)
            }
            ProvisioningState::Active { registrations } => Ok(Some(registrations)),
        }
    }

    /// Removes only exact secret generations retained by an interrupted provisioning plan.
    pub(crate) fn recover_pending(
        &self,
        store: &dyn SecretStore,
    ) -> Result<(), GovernancePersistenceError> {
        let _guard = self
            .provisioning
            .lock()
            .map_err(|_| GovernancePersistenceError::StateUnavailable)?;
        let ProvisioningState::Preparing { principals } = self.load_state()? else {
            return Ok(());
        };
        cleanup_prepared(store, &principals)?;
        self.store_state(&ProvisioningState::Unprovisioned)
    }

    /// Durably provisions the fixed two-person V1 governance set and returns its live authority.
    pub(crate) fn provision_principal_set<T>(
        &self,
        store: Arc<dyn SecretStore>,
        request: GovernanceProvisioningRequest,
        compose: impl FnOnce(GovernanceAuthority) -> Result<T, ()>,
    ) -> Result<T, GovernancePersistenceError> {
        let GovernanceProvisioningRequest {
            primary_display_name,
            primary_credential,
            reviewer_display_name,
            reviewer_credential,
            limits,
        } = request;
        let _guard = self
            .provisioning
            .lock()
            .map_err(|_| GovernancePersistenceError::StateUnavailable)?;
        match self.load_state()? {
            ProvisioningState::Unprovisioned => {}
            ProvisioningState::Preparing { .. } => {
                return Err(GovernancePersistenceError::ProvisioningRecoveryRequired);
            }
            ProvisioningState::Active { .. } => {
                return Err(GovernancePersistenceError::AlreadyProvisioned);
            }
        }
        if primary_display_name == reviewer_display_name
            || credentials_match(&primary_credential, &reviewer_credential)
        {
            return Err(GovernancePersistenceError::InvalidPrincipalSet);
        }

        let primary = GovernancePrincipal::try_new(
            next_principal_id()?,
            primary_display_name,
            GovernanceRoleSet::try_new([
                GovernanceRole::DecisionReviewer,
                GovernanceRole::DecisionInvalidator,
                GovernanceRole::FairValueApprover,
                GovernanceRole::FairValueOverrideApprover,
                GovernanceRole::FairValueRevoker,
                GovernanceRole::FairValueMarketAccessApprover,
                GovernanceRole::PortfolioImportResolver,
            ])?,
        )?;
        let reviewer = GovernancePrincipal::try_new(
            next_distinct_principal_id(primary.id())?,
            reviewer_display_name,
            GovernanceRoleSet::try_new([GovernanceRole::FairValueMarketAccessApprover])?,
        )?;
        let generation =
            SecretGeneration::new(1).map_err(|_| GovernancePersistenceError::SecretOperation)?;
        let planning = governance_secret_operation_control("governance-provision-plan")?;
        let primary_key = governance_principal_secret_key(primary.id())?;
        let reviewer_key = governance_principal_secret_key(reviewer.id())?;
        let primary_plan = store
            .plan_create(&primary_key, generation, &planning)
            .map_err(|_| GovernancePersistenceError::SecretOperation)?;
        let reviewer_plan = store
            .plan_create(&reviewer_key, generation, &planning)
            .map_err(|_| GovernancePersistenceError::SecretOperation)?;
        if primary_plan.target() == reviewer_plan.target() {
            return Err(GovernancePersistenceError::InvalidPrincipalSet);
        }
        let principals = vec![
            PreparedPrincipal {
                principal: primary,
                credential_plan: primary_plan,
            },
            PreparedPrincipal {
                principal: reviewer,
                credential_plan: reviewer_plan,
            },
        ]
        .into_boxed_slice();
        self.store_state(&ProvisioningState::Preparing {
            principals: principals.clone(),
        })?;

        let mutation = governance_secret_operation_control("governance-provision-commit")?;
        if store
            .execute_planned(
                &primary_key,
                &principals[0].credential_plan,
                primary_credential,
                &mutation,
            )
            .is_err()
            || store
                .execute_planned(
                    &reviewer_key,
                    &principals[1].credential_plan,
                    reviewer_credential,
                    &mutation,
                )
                .is_err()
        {
            return self.rollback_failed_provision(store.as_ref(), &principals);
        }

        let registrations = canonical_registrations(&[
            GovernancePrincipalRegistration::new(
                principals[0].principal.clone(),
                principals[0].credential_plan.target().clone(),
            ),
            GovernancePrincipalRegistration::new(
                principals[1].principal.clone(),
                principals[1].credential_plan.target().clone(),
            ),
        ])?;
        let authority = match GovernanceAuthority::try_load(
            Arc::clone(&store),
            registrations.clone(),
            self.audit_sink(),
            limits,
        ) {
            Ok(authority) => authority,
            Err(_error) => return self.rollback_failed_provision(store.as_ref(), &principals),
        };
        let composed = match compose(authority) {
            Ok(composed) => composed,
            Err(()) => return self.rollback_failed_provision(store.as_ref(), &principals),
        };
        if self
            .store_state(&ProvisioningState::Active {
                registrations: registrations.clone(),
            })
            .is_err()
        {
            return self.rollback_failed_provision(store.as_ref(), &principals);
        }
        Ok(composed)
    }

    /// Returns the shared append-and-sync sink consumed by `GovernanceAuthority`.
    pub(crate) fn audit_sink(&self) -> Arc<dyn GovernanceAuditSink> {
        self.audit.clone()
    }

    fn load_state(&self) -> Result<ProvisioningState, GovernancePersistenceError> {
        self.registrations
            .load()?
            .map(|bytes| decode_state(&bytes))
            .transpose()
            .map(|state| state.unwrap_or(ProvisioningState::Unprovisioned))
    }

    fn store_state(&self, state: &ProvisioningState) -> Result<(), GovernancePersistenceError> {
        let bytes = encode_state(state)?;
        self.registrations.store(&bytes)?;
        Ok(())
    }

    fn rollback_failed_provision<T>(
        &self,
        store: &dyn SecretStore,
        principals: &[PreparedPrincipal],
    ) -> Result<T, GovernancePersistenceError> {
        cleanup_prepared(store, principals)?;
        self.store_state(&ProvisioningState::Unprovisioned)?;
        Err(GovernancePersistenceError::SecretOperation)
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct DurableRegistrationState {
    schema_version: u16,
    state: ProvisioningState,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
enum ProvisioningState {
    Unprovisioned,
    Preparing {
        principals: Box<[PreparedPrincipal]>,
    },
    Active {
        registrations: Box<[GovernancePrincipalRegistration]>,
    },
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct PreparedPrincipal {
    principal: GovernancePrincipal,
    credential_plan: SecretMutationPlan,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct LegacyRegistrationState {
    schema_version: u16,
    registrations: Box<[GovernancePrincipalRegistration]>,
}

fn decode_state(bytes: &[u8]) -> Result<ProvisioningState, GovernancePersistenceError> {
    if bytes.is_empty() || bytes.len() > MAXIMUM_REGISTRATION_BYTES {
        return Err(GovernancePersistenceError::InvalidRegistrationState);
    }
    if let Ok(durable) = serde_json::from_slice::<DurableRegistrationState>(bytes) {
        if durable.schema_version != REGISTRATION_SCHEMA_VERSION {
            return Err(GovernancePersistenceError::InvalidRegistrationState);
        }
        validate_state(&durable.state)?;
        let canonical = serde_json::to_vec(&durable)
            .map_err(|_| GovernancePersistenceError::InvalidRegistrationState)?;
        if canonical != bytes {
            return Err(GovernancePersistenceError::InvalidRegistrationState);
        }
        return Ok(durable.state);
    }

    let legacy: LegacyRegistrationState = serde_json::from_slice(bytes)
        .map_err(|_| GovernancePersistenceError::InvalidRegistrationState)?;
    if legacy.schema_version != LEGACY_REGISTRATION_SCHEMA_VERSION {
        return Err(GovernancePersistenceError::InvalidRegistrationState);
    }
    validate_registrations(&legacy.registrations)?;
    let canonical = serde_json::to_vec(&legacy)
        .map_err(|_| GovernancePersistenceError::InvalidRegistrationState)?;
    if canonical != bytes {
        return Err(GovernancePersistenceError::InvalidRegistrationState);
    }
    Ok(ProvisioningState::Active {
        registrations: legacy.registrations,
    })
}

fn validate_state(state: &ProvisioningState) -> Result<(), GovernancePersistenceError> {
    match state {
        ProvisioningState::Unprovisioned => Ok(()),
        ProvisioningState::Preparing { principals } => validate_prepared(principals),
        ProvisioningState::Active { registrations } => validate_registrations(registrations),
    }
}

fn validate_prepared(principals: &[PreparedPrincipal]) -> Result<(), GovernancePersistenceError> {
    if principals.len() != 2
        || principals[0].principal.id() == principals[1].principal.id()
        || principals[0].credential_plan.target() == principals[1].credential_plan.target()
    {
        return Err(GovernancePersistenceError::InvalidRegistrationState);
    }
    for principal in principals {
        GovernancePrincipal::try_new(
            principal.principal.id(),
            principal.principal.display_name().to_owned(),
            principal.principal.roles().clone(),
        )
        .map_err(|_| GovernancePersistenceError::InvalidRegistrationState)?;
    }
    Ok(())
}

fn canonical_registrations(
    registrations: &[GovernancePrincipalRegistration],
) -> Result<Box<[GovernancePrincipalRegistration]>, GovernancePersistenceError> {
    if registrations.is_empty() || registrations.len() > MAXIMUM_REGISTRATIONS {
        return Err(GovernancePersistenceError::InvalidRegistrationState);
    }
    let mut canonical = Vec::new();
    canonical
        .try_reserve_exact(registrations.len())
        .map_err(|_| GovernancePersistenceError::Capacity)?;
    canonical.extend_from_slice(registrations);
    canonical.sort_unstable_by_key(|registration| registration.principal().id());
    validate_registrations(&canonical)?;
    Ok(canonical.into_boxed_slice())
}

fn validate_registrations(
    registrations: &[GovernancePrincipalRegistration],
) -> Result<(), GovernancePersistenceError> {
    if registrations.is_empty()
        || registrations.len() > MAXIMUM_REGISTRATIONS
        || !registration_order_is_canonical(registrations)
    {
        return Err(GovernancePersistenceError::InvalidRegistrationState);
    }

    let mut credentials = BTreeSet::new();
    for registration in registrations {
        let principal = registration.principal();
        if principal.id().as_uuid().is_nil() {
            return Err(GovernancePersistenceError::InvalidRegistrationState);
        }
        GovernanceRoleSet::try_new(principal.roles().as_slice().iter().copied())
            .map_err(|_| GovernancePersistenceError::InvalidRegistrationState)?;
        crate::application::governance::GovernancePrincipal::try_new(
            principal.id(),
            principal.display_name().to_owned(),
            principal.roles().clone(),
        )
        .map_err(|_| GovernancePersistenceError::InvalidRegistrationState)?;
        if !credentials.insert(registration.credential().clone()) {
            return Err(GovernancePersistenceError::InvalidRegistrationState);
        }
    }
    Ok(())
}

fn registration_order_is_canonical(registrations: &[GovernancePrincipalRegistration]) -> bool {
    registrations
        .windows(2)
        .all(|pair| pair[0].principal().id() < pair[1].principal().id())
}

fn encode_state(state: &ProvisioningState) -> Result<Vec<u8>, GovernancePersistenceError> {
    validate_state(state)?;
    let bytes = serde_json::to_vec(&DurableRegistrationState {
        schema_version: REGISTRATION_SCHEMA_VERSION,
        state: state.clone(),
    })
    .map_err(|_| GovernancePersistenceError::InvalidRegistrationState)?;
    if bytes.len() > MAXIMUM_REGISTRATION_BYTES
        || bytes.len() > LocalAuthorityStateStore::maximum_payload_bytes()
    {
        return Err(GovernancePersistenceError::Capacity);
    }
    Ok(bytes)
}

fn cleanup_prepared(
    store: &dyn SecretStore,
    principals: &[PreparedPrincipal],
) -> Result<(), GovernancePersistenceError> {
    let control = governance_secret_operation_control("governance-provision-recovery")?;
    for prepared in principals {
        let key = governance_principal_secret_key(prepared.principal.id())?;
        store
            .delete_planned(&key, &prepared.credential_plan, &control)
            .map_err(|_| GovernancePersistenceError::ProvisioningRecoveryRequired)?;
    }
    Ok(())
}

fn credentials_match(left: &SecretValue, right: &SecretValue) -> bool {
    let left_digest = Sha256::digest(left.expose_secret().as_bytes());
    let right_digest = Sha256::digest(right.expose_secret().as_bytes());
    bool::from(left_digest.ct_eq(&right_digest))
}

fn next_principal_id() -> Result<GovernancePrincipalId, GovernancePersistenceError> {
    GovernancePrincipalId::try_from_uuid(Uuid::new_v4()).map_err(Into::into)
}

fn next_distinct_principal_id(
    other: GovernancePrincipalId,
) -> Result<GovernancePrincipalId, GovernancePersistenceError> {
    for _ in 0..8 {
        let candidate = next_principal_id()?;
        if candidate != other {
            return Ok(candidate);
        }
    }
    Err(GovernancePersistenceError::InvalidPrincipalSet)
}

/// Private, lifetime-locked governance journal; its debug form never exposes durable content.
struct GovernanceAuditJournal {
    state: Mutex<AuditState>,
}

impl std::fmt::Debug for GovernanceAuditJournal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("GovernanceAuditJournal([LOCKED REDACTED AUDIT AUTHORITY])")
    }
}

impl GovernanceAuditJournal {
    fn try_open(control: Dir) -> Result<Self, GovernancePersistenceError> {
        let file = open_audit_file(&control)?;
        validate_private_file_identity(&control, &file)?;
        fs2::FileExt::try_lock_exclusive(&file).map_err(|source| {
            if source.kind() == std::io::ErrorKind::WouldBlock {
                GovernancePersistenceError::AuditAlreadyLocked
            } else {
                GovernancePersistenceError::Io(source)
            }
        })?;
        let mut file = AuditWriterGuard(file);
        validate_private_file_identity(&control, &file)?;
        synchronize_parent_directory(&control, &file)?;
        let records = recover_audit_file(&mut file)?;
        validate_private_file_identity(&control, &file)?;
        Ok(Self {
            state: Mutex::new(AuditState {
                file,
                records,
                poisoned: false,
            }),
        })
    }
}

impl GovernanceAuditSink for GovernanceAuditJournal {
    fn append(&self, receipt: &GovernanceAuditReceipt) -> Result<(), GovernanceAuditError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| GovernanceAuditError::Unavailable)?;
        append_durable(&mut state, receipt).map_err(|_| GovernanceAuditError::Unavailable)
    }
}

struct AuditState {
    file: AuditWriterGuard,
    records: usize,
    poisoned: bool,
}

struct AuditWriterGuard(File);

impl std::ops::Deref for AuditWriterGuard {
    type Target = File;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for AuditWriterGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Drop for AuditWriterGuard {
    fn drop(&mut self) {
        let _ignored = fs2::FileExt::unlock(&self.0);
    }
}

fn append_durable(
    state: &mut AuditState,
    receipt: &GovernanceAuditReceipt,
) -> Result<(), GovernancePersistenceError> {
    if state.poisoned {
        return Err(GovernancePersistenceError::PoisonedAudit);
    }
    if state.records >= MAXIMUM_AUDIT_RECORDS {
        return Err(GovernancePersistenceError::Capacity);
    }
    let payload = encode_audit_record(receipt)?;
    let header = audit_frame_header(&payload)?;
    let frame_bytes = header
        .len()
        .checked_add(payload.len())
        .ok_or(GovernancePersistenceError::Capacity)?;

    let offset = state
        .file
        .seek(SeekFrom::End(0))
        .map_err(GovernancePersistenceError::Io)?;
    let observed_length = state
        .file
        .metadata()
        .map_err(GovernancePersistenceError::Io)?
        .len();
    if offset != observed_length {
        state.poisoned = true;
        return Err(GovernancePersistenceError::PoisonedAudit);
    }
    let final_length = offset
        .checked_add(u64::try_from(frame_bytes).map_err(|_| GovernancePersistenceError::Capacity)?)
        .ok_or(GovernancePersistenceError::Capacity)?;
    if final_length > MAXIMUM_AUDIT_FILE_BYTES {
        return Err(GovernancePersistenceError::Capacity);
    }

    let append = state
        .file
        .write_all(&header)
        .and_then(|()| state.file.write_all(&payload))
        .and_then(|()| {
            if state.file.metadata()?.len() != final_length {
                return Err(std::io::Error::other(
                    "governance audit append length did not match its frame",
                ));
            }
            state.file.sync_all()
        });
    if let Err(source) = append {
        if rollback_append(&mut state.file, offset).is_err() {
            state.poisoned = true;
            return Err(GovernancePersistenceError::PoisonedAudit);
        }
        return Err(GovernancePersistenceError::Io(source));
    }
    state.records = state
        .records
        .checked_add(1)
        .ok_or(GovernancePersistenceError::Capacity)?;
    Ok(())
}

fn rollback_append(file: &mut File, offset: u64) -> Result<(), GovernancePersistenceError> {
    file.set_len(offset)
        .map_err(GovernancePersistenceError::Io)?;
    file.sync_all().map_err(GovernancePersistenceError::Io)?;
    let recovered = file
        .seek(SeekFrom::End(0))
        .map_err(GovernancePersistenceError::Io)?;
    if recovered != offset {
        return Err(GovernancePersistenceError::PoisonedAudit);
    }
    Ok(())
}

fn recover_audit_file(file: &mut File) -> Result<usize, GovernancePersistenceError> {
    let file_length = file
        .metadata()
        .map_err(GovernancePersistenceError::Io)?
        .len();
    if file_length > MAXIMUM_AUDIT_FILE_BYTES {
        return Err(GovernancePersistenceError::Capacity);
    }

    let mut reader = file.try_clone().map_err(GovernancePersistenceError::Io)?;
    reader
        .seek(SeekFrom::Start(0))
        .map_err(GovernancePersistenceError::Io)?;
    let mut boundary = 0_u64;
    let mut records = 0_usize;
    let header_bytes = u64::try_from(AUDIT_FRAME_HEADER_BYTES)
        .map_err(|_| GovernancePersistenceError::Capacity)?;

    while boundary < file_length {
        let remaining = file_length
            .checked_sub(boundary)
            .ok_or(GovernancePersistenceError::CorruptAuditRecord)?;
        if remaining < header_bytes {
            truncate_incomplete_tail(file, file_length, boundary)?;
            return Ok(records);
        }

        let mut header = [0_u8; AUDIT_FRAME_HEADER_BYTES];
        reader
            .read_exact(&mut header)
            .map_err(GovernancePersistenceError::Io)?;
        let payload_len = parse_audit_frame_header(&header)?;
        let frame_end = boundary
            .checked_add(header_bytes)
            .and_then(|value| value.checked_add(u64::try_from(payload_len).ok()?))
            .ok_or(GovernancePersistenceError::Capacity)?;
        if frame_end > file_length {
            truncate_incomplete_tail(file, file_length, boundary)?;
            return Ok(records);
        }
        if records >= MAXIMUM_AUDIT_RECORDS {
            return Err(GovernancePersistenceError::Capacity);
        }

        let mut payload = Vec::new();
        payload
            .try_reserve_exact(payload_len)
            .map_err(|_| GovernancePersistenceError::Capacity)?;
        payload.resize(payload_len, 0);
        reader
            .read_exact(&mut payload)
            .map_err(GovernancePersistenceError::Io)?;
        let expected_digest = &header[10..AUDIT_FRAME_HEADER_BYTES];
        if Sha256::digest(&payload).as_slice() != expected_digest
            || !is_valid_audit_record(&payload)
        {
            return Err(GovernancePersistenceError::CorruptAuditRecord);
        }
        records = records
            .checked_add(1)
            .ok_or(GovernancePersistenceError::Capacity)?;
        boundary = frame_end;
    }

    if file
        .metadata()
        .map_err(GovernancePersistenceError::Io)?
        .len()
        != file_length
    {
        return Err(GovernancePersistenceError::PoisonedAudit);
    }
    Ok(records)
}

fn truncate_incomplete_tail(
    file: &mut File,
    observed_length: u64,
    boundary: u64,
) -> Result<(), GovernancePersistenceError> {
    if file
        .metadata()
        .map_err(GovernancePersistenceError::Io)?
        .len()
        != observed_length
    {
        return Err(GovernancePersistenceError::PoisonedAudit);
    }
    file.set_len(boundary)
        .map_err(GovernancePersistenceError::Io)?;
    file.sync_all().map_err(GovernancePersistenceError::Io)?;
    let recovered = file
        .seek(SeekFrom::End(0))
        .map_err(GovernancePersistenceError::Io)?;
    if recovered != boundary {
        return Err(GovernancePersistenceError::PoisonedAudit);
    }
    Ok(())
}

fn audit_frame_header(
    payload: &[u8],
) -> Result<[u8; AUDIT_FRAME_HEADER_BYTES], GovernancePersistenceError> {
    if payload.is_empty() || payload.len() > MAXIMUM_AUDIT_RECORD_BYTES {
        return Err(GovernancePersistenceError::Capacity);
    }
    let payload_len =
        u32::try_from(payload.len()).map_err(|_| GovernancePersistenceError::Capacity)?;
    let mut header = [0_u8; AUDIT_FRAME_HEADER_BYTES];
    header[..4].copy_from_slice(&AUDIT_FRAME_MAGIC);
    header[4..6].copy_from_slice(&AUDIT_FRAME_VERSION.to_le_bytes());
    header[6..10].copy_from_slice(&payload_len.to_le_bytes());
    header[10..].copy_from_slice(&Sha256::digest(payload));
    Ok(header)
}

fn parse_audit_frame_header(
    header: &[u8; AUDIT_FRAME_HEADER_BYTES],
) -> Result<usize, GovernancePersistenceError> {
    let version = u16::from_le_bytes([header[4], header[5]]);
    let payload_len = u32::from_le_bytes([header[6], header[7], header[8], header[9]]);
    let payload_len =
        usize::try_from(payload_len).map_err(|_| GovernancePersistenceError::Capacity)?;
    if header[..4] != AUDIT_FRAME_MAGIC
        || version != AUDIT_FRAME_VERSION
        || payload_len == 0
        || payload_len > MAXIMUM_AUDIT_RECORD_BYTES
    {
        return Err(GovernancePersistenceError::CorruptAuditRecord);
    }
    Ok(payload_len)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AuditRecordView<'receipt> {
    schema_version: u16,
    receipt: &'receipt GovernanceAuditReceipt,
}

fn encode_audit_record(
    receipt: &GovernanceAuditReceipt,
) -> Result<Vec<u8>, GovernancePersistenceError> {
    let bytes = serde_json::to_vec(&AuditRecordView {
        schema_version: AUDIT_RECORD_SCHEMA_VERSION,
        receipt,
    })
    .map_err(|_| GovernancePersistenceError::AuditEncoding)?;
    if bytes.is_empty() || bytes.len() > MAXIMUM_AUDIT_RECORD_BYTES {
        return Err(GovernancePersistenceError::Capacity);
    }
    Ok(bytes)
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct AuditRecordWire {
    schema_version: u16,
    receipt: AuditReceiptWire,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct AuditReceiptWire {
    receipt_id: Uuid,
    kind: AuditKindWire,
    preview_id: Uuid,
    binding: AuditBindingWire,
    digest: String,
    required_roles: Box<[AuditRoleWire]>,
    principal_ids: Box<[Uuid]>,
    occurred_at: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct AuditBindingWire {
    workspace_id: Uuid,
    service_generation: NonZeroU64,
    client_id: Uuid,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
enum AuditKindWire {
    AuthenticationTicketIssued,
    CommitAuthorized,
}

#[derive(Clone, Copy, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "camelCase")]
enum AuditRoleWire {
    DecisionReviewer,
    DecisionInvalidator,
    FairValueApprover,
    FairValueOverrideApprover,
    FairValueRevoker,
    FairValueMarketAccessApprover,
    PortfolioImportResolver,
}

fn is_valid_audit_record(bytes: &[u8]) -> bool {
    let Ok(record) = serde_json::from_slice::<AuditRecordWire>(bytes) else {
        return false;
    };
    if record.schema_version != AUDIT_RECORD_SCHEMA_VERSION
        || record.receipt.receipt_id.is_nil()
        || record.receipt.preview_id.is_nil()
        || record.receipt.binding.workspace_id.is_nil()
        || record.receipt.binding.client_id.is_nil()
        || record.receipt.digest.len() != 64
        || !record
            .receipt
            .digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || record.receipt.required_roles.is_empty()
        || record.receipt.required_roles.len() > MAXIMUM_AUDIT_ROLES
        || record
            .receipt
            .required_roles
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        || record.receipt.principal_ids.is_empty()
        || record.receipt.principal_ids.len() > MAXIMUM_AUDIT_PRINCIPALS
        || record.receipt.principal_ids.iter().any(Uuid::is_nil)
        || record
            .receipt
            .principal_ids
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        || matches!(
            record.receipt.kind,
            AuditKindWire::AuthenticationTicketIssued
        ) && record.receipt.principal_ids.len() != 1
        || !canonical_audit_timestamp(&record.receipt.occurred_at)
    {
        return false;
    }
    serde_json::to_vec(&record).is_ok_and(|canonical| canonical == bytes)
}

fn canonical_audit_timestamp(value: &str) -> bool {
    if value.len() > 40 {
        return false;
    }
    let Ok(parsed) = DateTime::parse_from_rfc3339(value) else {
        return false;
    };
    parsed
        .with_timezone(&Utc)
        .to_rfc3339_opts(SecondsFormat::Nanos, true)
        == value
}

fn validate_private_file_identity(
    control: &Dir,
    file: &File,
) -> Result<(), GovernancePersistenceError> {
    use cap_fs_ext::MetadataExt as _;

    let opened =
        cap_std::fs::File::from_std(file.try_clone().map_err(GovernancePersistenceError::Io)?)
            .metadata()
            .map_err(GovernancePersistenceError::Io)?;
    let named = control
        .symlink_metadata(AUDIT_FILE_NAME)
        .map_err(GovernancePersistenceError::Io)?;
    if !opened.is_file()
        || !named.is_file()
        || opened.nlink() != 1
        || named.nlink() != 1
        || (opened.dev(), opened.ino()) != (named.dev(), named.ino())
    {
        return Err(GovernancePersistenceError::UnsafeAuditIdentity);
    }
    validate_private_permissions(&opened, file)
}

#[cfg(unix)]
fn validate_private_permissions(
    metadata: &cap_std::fs::Metadata,
    _file: &File,
) -> Result<(), GovernancePersistenceError> {
    if cap_fs_ext::OsMetadataExt::uid(metadata) != rustix::process::geteuid().as_raw() {
        return Err(GovernancePersistenceError::AuditOwnerMismatch);
    }
    if cap_fs_ext::OsMetadataExt::mode(metadata) & 0o077 == 0 {
        Ok(())
    } else {
        Err(GovernancePersistenceError::InsecureAuditPermissions)
    }
}

#[cfg(windows)]
fn validate_private_permissions(
    metadata: &cap_std::fs::Metadata,
    file: &File,
) -> Result<(), GovernancePersistenceError> {
    use cap_std::fs::MetadataExt as _;
    use win_security_identifier::{GetCurrentSid as _, SecurityIdentifier};
    use windows_permissions::{
        constants::{AccessRights, AceType, SeObjectType, SecurityInformation},
        wrappers::{ConvertSecurityDescriptorToStringSecurityDescriptor, GetSecurityInfo},
    };

    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
    if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(GovernancePersistenceError::UnsafeAuditIdentity);
    }

    let current_user = SecurityIdentifier::get_current_user_sid()
        .map_err(|_| GovernancePersistenceError::AuditPermissionProofUnavailable)?
        .to_string();
    let security = GetSecurityInfo(
        file,
        SeObjectType::SE_FILE_OBJECT,
        SecurityInformation::Owner | SecurityInformation::Dacl,
    )
    .map_err(GovernancePersistenceError::Io)?;
    let owner = security
        .owner()
        .ok_or(GovernancePersistenceError::AuditPermissionProofUnavailable)?;
    if owner.to_string() != current_user {
        return Err(GovernancePersistenceError::AuditOwnerMismatch);
    }

    let dacl = security
        .dacl()
        .ok_or(GovernancePersistenceError::InsecureAuditPermissions)?;
    if dacl.len() != 1 {
        return Err(GovernancePersistenceError::InsecureAuditPermissions);
    }
    let ace = dacl
        .get_ace(0)
        .ok_or(GovernancePersistenceError::AuditPermissionProofUnavailable)?;
    if ace.ace_type() != AceType::ACCESS_ALLOWED_ACE_TYPE
        || !ace.flags().is_empty()
        || ace.mask() != AccessRights::FileAllAccess
        || ace
            .sid()
            .is_none_or(|allowed| allowed.to_string() != current_user)
    {
        return Err(GovernancePersistenceError::InsecureAuditPermissions);
    }

    let dacl_sddl =
        ConvertSecurityDescriptorToStringSecurityDescriptor(&security, SecurityInformation::Dacl)
            .map_err(GovernancePersistenceError::Io)?;
    if !dacl_sddl.to_string_lossy().starts_with("D:P") {
        return Err(GovernancePersistenceError::InsecureAuditPermissions);
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn validate_private_permissions(
    _metadata: &cap_std::fs::Metadata,
    _file: &File,
) -> Result<(), GovernancePersistenceError> {
    Err(GovernancePersistenceError::AuditPermissionProofUnavailable)
}

#[cfg(not(windows))]
fn open_audit_file(control: &Dir) -> Result<File, GovernancePersistenceError> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).append(true).create(true);
    options.follow(FollowSymlinks::No);
    configure_private_creation(&mut options);
    control
        .open_with(AUDIT_FILE_NAME, &options)
        .map(cap_std::fs::File::into_std)
        .map_err(GovernancePersistenceError::Io)
}

#[cfg(windows)]
fn open_audit_file(control: &Dir) -> Result<File, GovernancePersistenceError> {
    let mut create_options = windows_audit_open_options(0);
    create_options.create_new(true);
    match control.open_with(AUDIT_FILE_NAME, &create_options) {
        Ok(file) => {
            let mut file = file.into_std();
            if let Err(error) = initialize_private_windows_file(&mut file) {
                drop(file);
                let _ignored = control.remove_file(AUDIT_FILE_NAME);
                return Err(error);
            }
            drop(file);
        }
        Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(source) => return Err(GovernancePersistenceError::Io(source)),
    }

    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    let existing_options = windows_audit_open_options(FILE_SHARE_READ | FILE_SHARE_WRITE);
    control
        .open_with(AUDIT_FILE_NAME, &existing_options)
        .map(cap_std::fs::File::into_std)
        .map_err(GovernancePersistenceError::Io)
}

#[cfg(windows)]
fn windows_audit_open_options(share_mode: u32) -> OpenOptions {
    use cap_std::fs::OpenOptionsExt as _;
    use windows_permissions::constants::AccessRights;

    let access = AccessRights::GenericRead
        | AccessRights::GenericWrite
        | AccessRights::ReadControl
        | AccessRights::WriteDac
        | AccessRights::WriteOwner;
    let mut options = OpenOptions::new();
    options.read(true).write(true).append(true);
    options.follow(FollowSymlinks::No);
    options.access_mode(access.bits()).share_mode(share_mode);
    options
}

#[cfg(windows)]
fn initialize_private_windows_file(file: &mut File) -> Result<(), GovernancePersistenceError> {
    use win_security_identifier::{GetCurrentSid as _, SecurityIdentifier};
    use windows_permissions::{
        LocalBox, SecurityDescriptor,
        constants::{SeObjectType, SecurityInformation},
        wrappers::SetSecurityInfo,
    };

    let current_user = SecurityIdentifier::get_current_user_sid()
        .map_err(|_| GovernancePersistenceError::AuditPermissionProofUnavailable)?
        .to_string();
    let descriptor: LocalBox<SecurityDescriptor> =
        format!("O:{current_user}D:P(A;;FA;;;{current_user})")
            .parse()
            .map_err(GovernancePersistenceError::Io)?;
    let owner = descriptor
        .owner()
        .ok_or(GovernancePersistenceError::AuditPermissionProofUnavailable)?;
    let dacl = descriptor
        .dacl()
        .ok_or(GovernancePersistenceError::AuditPermissionProofUnavailable)?;
    SetSecurityInfo(
        file,
        SeObjectType::SE_FILE_OBJECT,
        SecurityInformation::Owner | SecurityInformation::Dacl | SecurityInformation::ProtectedDacl,
        Some(owner),
        None,
        Some(dacl),
        None,
    )
    .map_err(GovernancePersistenceError::Io)?;
    file.sync_all().map_err(GovernancePersistenceError::Io)
}

#[cfg(unix)]
fn configure_private_creation(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;

    options
        .mode(0o600)
        .custom_flags(libc::O_NONBLOCK | libc::O_CLOEXEC);
}

#[cfg(not(unix))]
fn configure_private_creation(_options: &mut OpenOptions) {}

#[cfg(unix)]
fn synchronize_parent_directory(
    control: &Dir,
    _file: &File,
) -> Result<(), GovernancePersistenceError> {
    use cap_std::fs::OpenOptionsExt as _;

    let mut options = OpenOptions::new();
    options.read(true);
    options.custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let directory = control
        .open_with(".", &options)
        .map_err(GovernancePersistenceError::Io)?
        .into_std();
    directory.sync_all().map_err(GovernancePersistenceError::Io)
}

#[cfg(windows)]
fn synchronize_parent_directory(
    _control: &Dir,
    file: &File,
) -> Result<(), GovernancePersistenceError> {
    file.sync_all().map_err(GovernancePersistenceError::Io)
}

#[cfg(not(any(unix, windows)))]
fn synchronize_parent_directory(
    _control: &Dir,
    _file: &File,
) -> Result<(), GovernancePersistenceError> {
    Err(GovernancePersistenceError::DirectoryDurabilityUnavailable)
}

/// Fail-closed durable governance construction, recovery, or mutation error.
#[derive(Debug, Error)]
pub(crate) enum GovernancePersistenceError {
    /// A prepared local path capability was unavailable or changed identity.
    #[error(transparent)]
    Path(#[from] PathError),
    /// The two-copy principal authority was unavailable or corrupt.
    #[error(transparent)]
    State(#[from] LocalAuthorityStateStoreError),
    /// The in-process provisioning serialization boundary was poisoned.
    #[error("governance provisioning state is unavailable")]
    StateUnavailable,
    /// Principal names, credentials, or generated identities cannot form the fixed V1 set.
    #[error("governance principal set is invalid")]
    InvalidPrincipalSet,
    /// Governance is already durably provisioned and cannot be overwritten by setup.
    #[error("governance principal set is already provisioned")]
    AlreadyProvisioned,
    /// An interrupted secret mutation must be recovered before setup can continue.
    #[error("governance principal provisioning requires recovery")]
    ProvisioningRecoveryRequired,
    /// A protected secret-store operation failed without exposing provider or credential details.
    #[error("governance protected credential operation failed")]
    SecretOperation,
    /// Canonical governance validation rejected the fixed principal set or protected authority.
    #[error(transparent)]
    Governance(#[from] GovernanceError),
    /// Principal metadata was malformed, noncanonical, unsupported, or internally inconsistent.
    #[error("governance principal registration state is invalid")]
    InvalidRegistrationState,
    /// A fixed registration or audit bound could not admit more data.
    #[error("governance persistence capacity is unavailable")]
    Capacity,
    /// A filesystem operation failed without exposing path or receipt content.
    #[error("governance persistence filesystem operation failed")]
    Io(#[source] std::io::Error),
    /// The named audit endpoint is not the exact single-link regular file that was opened.
    #[error("governance audit endpoint identity is unsafe or ambiguous")]
    UnsafeAuditIdentity,
    /// The audit endpoint is accessible beyond its owner.
    #[error("governance audit endpoint permissions are not private")]
    InsecureAuditPermissions,
    /// The audit endpoint is not owned by the effective user.
    #[error("governance audit endpoint owner does not match the service owner")]
    AuditOwnerMismatch,
    /// The platform could not prove an owner-only audit access policy.
    #[cfg(not(unix))]
    #[error("governance audit endpoint privacy cannot be proven")]
    AuditPermissionProofUnavailable,
    /// The platform cannot durably synchronize the audit endpoint's directory entry.
    #[cfg(not(any(unix, windows)))]
    #[error("governance audit parent-directory durability cannot be established")]
    DirectoryDurabilityUnavailable,
    /// Another process already owns the governance journal.
    #[error("governance audit endpoint is already locked")]
    AuditAlreadyLocked,
    /// A complete audit frame was malformed, unsupported, noncanonical, or failed its digest.
    #[error("governance audit contains a corrupt complete record")]
    CorruptAuditRecord,
    /// An append or recovery failure made further journal durability indeterminate.
    #[error("governance audit durability is indeterminate")]
    PoisonedAudit,
    /// A redacted receipt could not be encoded into the fixed audit schema.
    #[error("governance audit receipt encoding failed")]
    AuditEncoding,
}
