use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt,
    num::NonZeroUsize,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use market_squawk_domain::Timestamp;
use market_squawk_platform::{
    LocalSecretStoreError, SecretCancellation, SecretGeneration, SecretInteractionPolicy,
    SecretKey, SecretOperationControl, SecretStore, SecretValue,
};
use subtle::ConstantTimeEq as _;
use uuid::Uuid;

use super::identity::{GOVERNANCE_SECRET_SCOPE, GovernanceEffect, SECRET_OPERATION_TIMEOUT};
use super::{
    GovernanceActionDigest, GovernanceActionPreview, GovernanceAuditKind, GovernanceAuditReceipt,
    GovernanceAuditSink, GovernanceAuthenticationTicket, GovernanceAuthorizedPrincipal,
    GovernanceCommitReceipt, GovernanceError, GovernanceLimits, GovernancePreviewId,
    GovernancePreviewRequest, GovernancePrincipal, GovernancePrincipalAdmission,
    GovernancePrincipalId, GovernancePrincipalPage, GovernancePrincipalRegistration,
    GovernancePrincipalSummary, GovernanceReceiptId, GovernanceRequestBinding, GovernanceRoleSet,
    GovernanceTicketId, GovernanceTimestamp,
};

struct LoadedPrincipal {
    principal: GovernancePrincipal,
    credential: SecretValue,
}

impl fmt::Debug for LoadedPrincipal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LoadedPrincipal")
            .field("principal", &self.principal)
            .field("credential", &"[REDACTED]")
            .finish()
    }
}

struct StoredPreview {
    preview: GovernanceActionPreview,
    binding: GovernanceRequestBinding,
    expires_at: Instant,
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
struct ReauthenticationKey {
    preview_id: GovernancePreviewId,
    principal_id: GovernancePrincipalId,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum TicketDisposition {
    Available,
    Consumed,
}

struct StoredTicket {
    ticket: GovernanceAuthenticationTicket,
    binding: GovernanceRequestBinding,
    digest: GovernanceActionDigest,
    roles: GovernanceRoleSet,
    expires_at: Instant,
    disposition: TicketDisposition,
}

struct GovernanceState {
    principals: BTreeMap<GovernancePrincipalId, LoadedPrincipal>,
    previews: HashMap<GovernancePreviewId, StoredPreview>,
    tickets: HashMap<GovernanceTicketId, StoredTicket>,
    attempts: HashMap<ReauthenticationKey, u8>,
}

/// Shared local authority for admitted humans, bounded reauthentication, and action-bound tickets.
pub struct GovernanceAuthority {
    limits: GovernanceLimits,
    audit: Arc<dyn GovernanceAuditSink>,
    state: RwLock<GovernanceState>,
}

impl fmt::Debug for GovernanceAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("GovernanceAuthority([REDACTED PRINCIPAL CREDENTIALS])")
    }
}

impl GovernanceAuthority {
    /// Loads only exact durable registrations and their protected secret-store generations.
    pub fn try_load(
        store: Arc<dyn SecretStore>,
        registrations: impl IntoIterator<Item = GovernancePrincipalRegistration>,
        audit: Arc<dyn GovernanceAuditSink>,
        limits: GovernanceLimits,
    ) -> Result<Self, GovernanceError> {
        let mut principals = BTreeMap::new();
        for registration in registrations {
            if principals.len() >= limits.maximum_principals.get()
                || principals.contains_key(&registration.principal.id)
            {
                return Err(GovernanceError::InvalidPrincipal);
            }
            let credential = store
                .read(
                    registration.credential(),
                    &secret_control("governance-load")?,
                )
                .map_err(map_secret_error)?;
            principals.insert(
                registration.principal.id,
                LoadedPrincipal {
                    principal: registration.principal,
                    credential,
                },
            );
        }
        Self::from_principals(principals, audit, limits)
    }

    /// Provisions a complete independent local principal set using the existing exact secret store.
    ///
    /// The caller must persist the returned registrations before publishing the resulting authority.
    pub fn provision_set(
        store: Arc<dyn SecretStore>,
        admissions: impl IntoIterator<Item = GovernancePrincipalAdmission>,
        audit: Arc<dyn GovernanceAuditSink>,
        limits: GovernanceLimits,
    ) -> Result<(Self, Box<[GovernancePrincipalRegistration]>), GovernanceError> {
        let admissions = admissions.into_iter().collect::<Vec<_>>();
        if admissions.is_empty() || admissions.len() > limits.maximum_principals.get() {
            return Err(GovernanceError::InvalidPrincipal);
        }
        let mut ids = HashSet::new();
        if admissions
            .iter()
            .any(|admission| !ids.insert(admission.principal.id))
        {
            return Err(GovernanceError::InvalidPrincipal);
        }
        let generation = SecretGeneration::new(1).map_err(map_secret_error)?;
        let mut registrations = Vec::new();
        registrations
            .try_reserve_exact(admissions.len())
            .map_err(|_| GovernanceError::CapacityExceeded)?;
        for admission in admissions {
            let key = principal_secret_key(admission.principal.id)?;
            let created = store.create(
                &key,
                generation,
                admission.credential,
                &secret_control("governance-provision")?,
            );
            match created {
                Ok(credential) => registrations.push(GovernancePrincipalRegistration::new(
                    admission.principal,
                    credential,
                )),
                Err(error) => {
                    cleanup_registrations(&store, &registrations);
                    return Err(map_secret_error(error));
                }
            }
        }
        let authority =
            match Self::try_load(Arc::clone(&store), registrations.clone(), audit, limits) {
                Ok(authority) => authority,
                Err(error) => {
                    cleanup_registrations(&store, &registrations);
                    return Err(error);
                }
            };
        Ok((authority, registrations.into_boxed_slice()))
    }

    /// Lists locally admitted principals without credential material or implicit authority.
    pub fn list_principals(
        &self,
        after: Option<GovernancePrincipalId>,
        limit: NonZeroUsize,
    ) -> Result<GovernancePrincipalPage, GovernanceError> {
        if limit > self.limits.maximum_principals {
            return Err(GovernanceError::InvalidLimits);
        }
        let state = self
            .state
            .read()
            .map_err(|_| GovernanceError::StateUnavailable)?;
        let mut principals: Vec<GovernancePrincipalSummary> = Vec::new();
        principals
            .try_reserve_exact(limit.get())
            .map_err(|_| GovernanceError::CapacityExceeded)?;
        let mut next_after = None;
        for (id, principal) in &state.principals {
            if after.is_some_and(|cursor| *id <= cursor) {
                continue;
            }
            if principals.len() == limit.get() {
                next_after = principals
                    .last()
                    .map(GovernancePrincipalSummary::principal_id);
                break;
            }
            principals.push(GovernancePrincipalSummary::from(&principal.principal));
        }
        Ok(GovernancePrincipalPage {
            principals: principals.into_boxed_slice(),
            next_after,
        })
    }

    /// Stores bounded exact-preview metadata after the domain persisted the canonical action bytes.
    pub fn preview_action(
        &self,
        request: GovernancePreviewRequest,
        now: Instant,
        observed_at: Timestamp,
    ) -> Result<GovernanceActionPreview, GovernanceError> {
        if request.requested_lifetime > self.limits.maximum_preview_lifetime {
            return Err(GovernanceError::InvalidPreview);
        }
        let expires_at = add_wall_duration(observed_at, request.requested_lifetime)?;
        let expires_instant = now
            .checked_add(request.requested_lifetime)
            .ok_or(GovernanceError::TimeUnavailable)?;
        let mut state = self
            .state
            .write()
            .map_err(|_| GovernanceError::StateUnavailable)?;
        prune_expired(&mut state, now);
        if state.previews.len() >= self.limits.maximum_previews.get() {
            return Err(GovernanceError::CapacityExceeded);
        }
        for principal_id in &request.eligible_principal_ids {
            let principal = state
                .principals
                .get(principal_id)
                .ok_or(GovernanceError::PrincipalNotEligible)?;
            if !principal
                .principal
                .roles
                .includes_all(&request.required_roles)
            {
                return Err(GovernanceError::PrincipalNotEligible);
            }
        }
        let preview_id = next_preview_id(&state)?;
        let preview = GovernanceActionPreview {
            preview_id,
            digest: request.digest,
            required_roles: request.required_roles,
            distinct_principal_count: request.distinct_principal_count,
            eligible_principal_ids: request.eligible_principal_ids,
            expires_at: GovernanceTimestamp::from_timestamp(expires_at),
            effects: Box::new([GovernanceEffect::for_action(request.kind)]),
        };
        state.previews.insert(
            preview_id,
            StoredPreview {
                preview: preview.clone(),
                binding: request.binding,
                expires_at: expires_instant,
            },
        );
        Ok(preview)
    }

    /// Reauthenticates one eligible admitted principal and durably issues one exact one-use ticket.
    pub fn authenticate_action(
        &self,
        preview_id: GovernancePreviewId,
        principal_id: GovernancePrincipalId,
        credential: SecretValue,
        now: Instant,
        observed_at: Timestamp,
    ) -> Result<GovernanceAuthenticationTicket, GovernanceError> {
        let mut state = self
            .state
            .write()
            .map_err(|_| GovernanceError::StateUnavailable)?;
        prune_expired(&mut state, now);
        let (preview, binding, preview_expiry) = {
            let stored = state
                .previews
                .get(&preview_id)
                .ok_or(GovernanceError::PreviewNotFound)?;
            (stored.preview.clone(), stored.binding, stored.expires_at)
        };
        if now >= preview_expiry {
            return Err(GovernanceError::PreviewExpired);
        }
        if !preview.eligible_principal_ids.contains(&principal_id) {
            return Err(GovernanceError::PrincipalNotEligible);
        }
        let attempt_key = ReauthenticationKey {
            preview_id,
            principal_id,
        };
        if state.attempts.get(&attempt_key).is_some_and(|attempts| {
            *attempts >= self.limits.maximum_reauthentication_attempts.get()
        }) {
            return Err(GovernanceError::ReauthenticationLocked);
        }
        if state.tickets.values().any(|ticket| {
            ticket.ticket.preview_id == preview_id && ticket.ticket.principal_id == principal_id
        }) {
            return Err(GovernanceError::TicketAlreadyIssued);
        }
        let principal = state
            .principals
            .get(&principal_id)
            .ok_or(GovernanceError::PrincipalNotFound)?;
        if !principal
            .principal
            .roles
            .includes_all(&preview.required_roles)
        {
            return Err(GovernanceError::PrincipalNotEligible);
        }
        let expected = principal.credential.expose_secret().as_bytes();
        let presented = credential.expose_secret().as_bytes();
        let credential_matches = expected.ct_eq(presented).unwrap_u8() == 1;
        if expected.len() != presented.len() || !credential_matches {
            if !state.attempts.contains_key(&attempt_key)
                && state.attempts.len() >= self.limits.maximum_tickets.get()
            {
                return Err(GovernanceError::CapacityExceeded);
            }
            let attempts = state.attempts.entry(attempt_key).or_insert(0);
            *attempts = attempts.saturating_add(1);
            if *attempts >= self.limits.maximum_reauthentication_attempts.get() {
                return Err(GovernanceError::ReauthenticationLocked);
            }
            return Err(GovernanceError::InvalidCredential);
        }
        state.attempts.remove(&attempt_key);
        if state.tickets.len() >= self.limits.maximum_tickets.get() {
            return Err(GovernanceError::CapacityExceeded);
        }
        let ticket_expiry = preview_expiry.min(
            now.checked_add(self.limits.maximum_ticket_lifetime)
                .ok_or(GovernanceError::TimeUnavailable)?,
        );
        let ticket_wall_expiry =
            add_wall_duration(observed_at, ticket_expiry.saturating_duration_since(now))?;
        let ticket = GovernanceAuthenticationTicket {
            ticket_id: next_ticket_id(&state)?,
            preview_id,
            principal_id,
            expires_at: GovernanceTimestamp::from_timestamp(ticket_wall_expiry),
        };
        let audit = GovernanceAuditReceipt {
            receipt_id: next_receipt_id()?,
            kind: GovernanceAuditKind::AuthenticationTicketIssued,
            preview_id,
            binding,
            digest: preview.digest,
            required_roles: preview.required_roles.clone(),
            principal_ids: Box::new([principal_id]),
            occurred_at: GovernanceTimestamp::from_timestamp(observed_at),
        };
        self.audit
            .append(&audit)
            .map_err(|_| GovernanceError::AuditUnavailable)?;
        state.tickets.insert(
            ticket.ticket_id,
            StoredTicket {
                ticket: ticket.clone(),
                binding,
                digest: preview.digest,
                roles: preview.required_roles.clone(),
                expires_at: ticket_expiry,
                disposition: TicketDisposition::Available,
            },
        );
        Ok(ticket)
    }

    /// Verifies and consumes exact ticket capabilities, returning only server-derived actor facts.
    pub fn commit_action(
        &self,
        preview_id: GovernancePreviewId,
        tickets: impl IntoIterator<Item = GovernanceAuthenticationTicket>,
        now: Instant,
        observed_at: Timestamp,
    ) -> Result<GovernanceCommitReceipt, GovernanceError> {
        self.commit_action_ids(
            preview_id,
            tickets.into_iter().map(|ticket| ticket.ticket_id),
            now,
            observed_at,
        )
    }

    /// Resolves native-held opaque ticket identities and consumes their server-owned capabilities.
    pub(crate) fn commit_action_ids(
        &self,
        preview_id: GovernancePreviewId,
        ticket_ids: impl IntoIterator<Item = GovernanceTicketId>,
        now: Instant,
        observed_at: Timestamp,
    ) -> Result<GovernanceCommitReceipt, GovernanceError> {
        let submitted_ticket_ids = ticket_ids.into_iter().collect::<Vec<_>>();
        let mut state = self
            .state
            .write()
            .map_err(|_| GovernanceError::StateUnavailable)?;
        prune_expired(&mut state, now);
        let (preview, binding, preview_expiry) = {
            let stored = state
                .previews
                .get(&preview_id)
                .ok_or(GovernanceError::PreviewNotFound)?;
            (stored.preview.clone(), stored.binding, stored.expires_at)
        };
        if now >= preview_expiry {
            return Err(GovernanceError::PreviewExpired);
        }
        if submitted_ticket_ids.len() != usize::from(preview.distinct_principal_count) {
            return Err(GovernanceError::IncorrectTicketCount);
        }
        let mut ticket_ids = HashSet::new();
        let mut principal_ids = HashSet::new();
        ticket_ids
            .try_reserve(submitted_ticket_ids.len())
            .map_err(|_| GovernanceError::CapacityExceeded)?;
        principal_ids
            .try_reserve(submitted_ticket_ids.len())
            .map_err(|_| GovernanceError::CapacityExceeded)?;
        let mut authorized = Vec::new();
        authorized
            .try_reserve_exact(submitted_ticket_ids.len())
            .map_err(|_| GovernanceError::CapacityExceeded)?;
        for submitted_ticket_id in submitted_ticket_ids {
            if !ticket_ids.insert(submitted_ticket_id) {
                return Err(GovernanceError::DuplicateTicket);
            }
            let ticket = state
                .tickets
                .get(&submitted_ticket_id)
                .ok_or(GovernanceError::TicketNotFound)?;
            if ticket.ticket.preview_id != preview_id {
                return Err(GovernanceError::TicketPreviewMismatch);
            }
            if ticket.disposition == TicketDisposition::Consumed {
                return Err(GovernanceError::TicketConsumed);
            }
            if now >= ticket.expires_at {
                return Err(GovernanceError::TicketExpired);
            }
            if ticket.binding != binding || ticket.digest != preview.digest {
                return Err(GovernanceError::TicketPreviewMismatch);
            }
            if ticket.roles != preview.required_roles
                || !preview
                    .eligible_principal_ids
                    .contains(&ticket.ticket.principal_id)
            {
                return Err(GovernanceError::PrincipalNotEligible);
            }
            if !principal_ids.insert(ticket.ticket.principal_id) {
                return Err(GovernanceError::DuplicatePrincipal);
            }
            authorized.push(GovernanceAuthorizedPrincipal {
                principal_id: ticket.ticket.principal_id,
                roles: ticket.roles.clone(),
            });
        }
        authorized.sort_unstable_by_key(|principal| principal.principal_id);
        let receipt = GovernanceCommitReceipt {
            receipt_id: next_receipt_id()?,
            preview_id,
            digest: preview.digest,
            committed_at: GovernanceTimestamp::from_timestamp(observed_at),
            authorized_principals: authorized.into_boxed_slice(),
            effects: preview.effects.clone(),
        };
        let audit = GovernanceAuditReceipt {
            receipt_id: receipt.receipt_id,
            kind: GovernanceAuditKind::CommitAuthorized,
            preview_id,
            binding,
            digest: preview.digest,
            required_roles: preview.required_roles.clone(),
            principal_ids: receipt
                .authorized_principals
                .iter()
                .map(GovernanceAuthorizedPrincipal::principal_id)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            occurred_at: GovernanceTimestamp::from_timestamp(observed_at),
        };
        self.audit
            .append(&audit)
            .map_err(|_| GovernanceError::AuditUnavailable)?;
        for ticket_id in ticket_ids {
            let ticket = state
                .tickets
                .get_mut(&ticket_id)
                .ok_or(GovernanceError::StateUnavailable)?;
            ticket.disposition = TicketDisposition::Consumed;
        }
        Ok(receipt)
    }

    fn from_principals(
        principals: BTreeMap<GovernancePrincipalId, LoadedPrincipal>,
        audit: Arc<dyn GovernanceAuditSink>,
        limits: GovernanceLimits,
    ) -> Result<Self, GovernanceError> {
        if principals.is_empty() || principals.len() > limits.maximum_principals.get() {
            return Err(GovernanceError::InvalidPrincipal);
        }
        let mut previews = HashMap::new();
        previews
            .try_reserve(limits.maximum_previews.get())
            .map_err(|_| GovernanceError::CapacityExceeded)?;
        let mut tickets = HashMap::new();
        tickets
            .try_reserve(limits.maximum_tickets.get())
            .map_err(|_| GovernanceError::CapacityExceeded)?;
        let mut attempts = HashMap::new();
        attempts
            .try_reserve(limits.maximum_tickets.get())
            .map_err(|_| GovernanceError::CapacityExceeded)?;
        Ok(Self {
            limits,
            audit,
            state: RwLock::new(GovernanceState {
                principals,
                previews,
                tickets,
                attempts,
            }),
        })
    }

    #[cfg(test)]
    pub(super) fn for_test(
        principals: impl IntoIterator<Item = TestPrincipal>,
    ) -> Result<Self, GovernanceError> {
        let mut loaded = BTreeMap::new();
        for principal in principals {
            if loaded
                .insert(
                    principal.principal.id,
                    LoadedPrincipal {
                        principal: principal.principal,
                        credential: principal.credential,
                    },
                )
                .is_some()
            {
                return Err(GovernanceError::InvalidPrincipal);
            }
        }
        Self::from_principals(loaded, Arc::new(TestAudit), GovernanceLimits::standard()?)
    }
}

fn prune_expired(state: &mut GovernanceState, now: Instant) {
    state.previews.retain(|_, preview| now < preview.expires_at);
    state.tickets.retain(|_, ticket| {
        now < ticket.expires_at && state.previews.contains_key(&ticket.ticket.preview_id)
    });
    state
        .attempts
        .retain(|key, _| state.previews.contains_key(&key.preview_id));
}

fn add_wall_duration(now: Timestamp, duration: Duration) -> Result<Timestamp, GovernanceError> {
    let nanos = i64::try_from(duration.as_nanos()).map_err(|_| GovernanceError::TimeUnavailable)?;
    now.unix_nanos()
        .checked_add(nanos)
        .map(Timestamp::from_unix_nanos)
        .ok_or(GovernanceError::TimeUnavailable)
}

fn next_preview_id(state: &GovernanceState) -> Result<GovernancePreviewId, GovernanceError> {
    for _ in 0..8 {
        let id = GovernancePreviewId::try_from_uuid(Uuid::new_v4())?;
        if !state.previews.contains_key(&id) {
            return Ok(id);
        }
    }
    Err(GovernanceError::RandomUnavailable)
}

fn next_ticket_id(state: &GovernanceState) -> Result<GovernanceTicketId, GovernanceError> {
    for _ in 0..8 {
        let id = GovernanceTicketId::try_from_uuid(Uuid::new_v4())?;
        if !state.tickets.contains_key(&id) {
            return Ok(id);
        }
    }
    Err(GovernanceError::RandomUnavailable)
}

fn next_receipt_id() -> Result<GovernanceReceiptId, GovernanceError> {
    GovernanceReceiptId::try_from_uuid(Uuid::new_v4())
}

fn principal_secret_key(id: GovernancePrincipalId) -> Result<SecretKey, GovernanceError> {
    SecretKey::try_new(GOVERNANCE_SECRET_SCOPE, &id.as_uuid().to_string()).map_err(map_secret_error)
}

fn secret_control(owner: &'static str) -> Result<SecretOperationControl, GovernanceError> {
    let deadline = Instant::now()
        .checked_add(SECRET_OPERATION_TIMEOUT)
        .ok_or(GovernanceError::SecretStoreUnavailable)?;
    SecretOperationControl::try_new(
        owner,
        deadline,
        1,
        SecretInteractionPolicy::Forbid,
        SecretCancellation::new(),
    )
    .map_err(map_secret_error)
}

fn cleanup_registrations(
    store: &Arc<dyn SecretStore>,
    registrations: &[GovernancePrincipalRegistration],
) {
    let Ok(control) = secret_control("governance-provision-cleanup") else {
        return;
    };
    for registration in registrations {
        let _cleanup = store.delete(registration.credential(), &control);
    }
}

fn map_secret_error(_error: LocalSecretStoreError) -> GovernanceError {
    GovernanceError::SecretStoreUnavailable
}

#[cfg(test)]
pub(super) struct TestPrincipal {
    principal: GovernancePrincipal,
    credential: SecretValue,
}

#[cfg(test)]
impl TestPrincipal {
    pub(super) fn try_new(principal: GovernancePrincipal, credential: SecretValue) -> Self {
        Self {
            principal,
            credential,
        }
    }
}

#[cfg(test)]
#[derive(Debug)]
struct TestAudit;

#[cfg(test)]
impl GovernanceAuditSink for TestAudit {
    fn append(&self, _receipt: &GovernanceAuditReceipt) -> Result<(), GovernanceAuditError> {
        Ok(())
    }
}
