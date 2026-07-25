//! Append-only non-secret provider onboarding state.

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp};
use market_squawk_platform::SecretGeneration;
use market_squawk_sources::{
    AuthoritySet, CapabilityRegistrationOutcome, OnboardingEvent, OnboardingLifecycle,
    OnboardingState, ProviderCapability, ProviderCapabilityRevision, ProviderPublicConfiguration,
};
use rusqlite::{OptionalExtension as _, Row, Transaction, params};
use serde::Serialize;
use uuid::Uuid;

use super::CatalogError;
use super::runs::CatalogAuthority;
use super::storage::{ResultBudget, append_audit, sha256, trusted_catalog_now};

const MAX_ONBOARDING_EVENTS: u64 = 1_024;
const MAX_CAPABILITY_JSON_BYTES: usize = 65_536;
const MAX_AUTHORITY_JSON_BYTES: usize = 16_384;
const MAX_EVENT_JSON_BYTES: usize = 65_536;
const MAX_RETRY_BUDGET: u8 = 8;
const MAX_PROVIDER_SURFACES: i64 = 64;
const MAX_PROVIDER_REVISIONS: u64 = 256;
const LEGACY_RESERVATION_SCHEMA_VERSION: i64 = 1;
const RESERVATION_SCHEMA_VERSION: i64 = 2;

/// Validated immutable input for one durable onboarding reservation.
#[derive(Clone, Debug)]
pub struct OnboardingReservationRequest {
    capability: ProviderCapability,
    public_configuration: ProviderPublicConfiguration,
    requested_authority: AuthoritySet,
    actor_class: SourceIdentifier,
    operation_owner: SourceIdentifier,
    deadline_at: Timestamp,
    retry_budget: u8,
}

impl OnboardingReservationRequest {
    /// Binds a reservation to one exact capability revision, owner, deadline, and retry ceiling.
    pub fn try_new(
        capability: &ProviderCapability,
        public_configuration: ProviderPublicConfiguration,
        requested_authority: AuthoritySet,
        actor_class: SourceIdentifier,
        operation_owner: SourceIdentifier,
        deadline_at: Timestamp,
        retry_budget: u8,
    ) -> Result<Self, CatalogError> {
        if retry_budget > MAX_RETRY_BUDGET {
            return Err(CatalogError::InvalidRecord);
        }
        OnboardingLifecycle::reserve(capability, requested_authority.clone())?;
        Ok(Self {
            capability: capability.clone(),
            public_configuration,
            requested_authority,
            actor_class,
            operation_owner,
            deadline_at,
            retry_budget,
        })
    }

    /// Returns the immutable capability revision.
    pub const fn capability(&self) -> &ProviderCapability {
        &self.capability
    }

    /// Returns the bounded canonical non-secret provider configuration.
    pub const fn public_configuration(&self) -> &ProviderPublicConfiguration {
        &self.public_configuration
    }

    /// Returns exact requested authority.
    pub const fn requested_authority(&self) -> &AuthoritySet {
        &self.requested_authority
    }

    /// Returns the non-secret actor class.
    pub const fn actor_class(&self) -> &SourceIdentifier {
        &self.actor_class
    }

    /// Returns the unique operation owner.
    pub const fn operation_owner(&self) -> &SourceIdentifier {
        &self.operation_owner
    }

    /// Returns the fixed wall-clock deadline retained beside monotonic runtime control.
    pub const fn deadline_at(&self) -> Timestamp {
        self.deadline_at
    }

    /// Returns the fixed retry ceiling.
    pub const fn retry_budget(&self) -> u8 {
        self.retry_budget
    }
}

/// Catalog-session-sealed authority for appending one onboarding event stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OnboardingReservation {
    catalog_id: Uuid,
    session_id: Uuid,
    capability_digest: EvidenceDigest,
    public_configuration_digest: EvidenceDigest,
    initial_state: OnboardingState,
    created_at: Timestamp,
    deadline_at: Timestamp,
}

impl OnboardingReservation {
    /// Returns the opaque durable resume identity.
    pub const fn session_id(&self) -> Uuid {
        self.session_id
    }

    /// Returns the exact capability digest.
    pub const fn capability_digest(&self) -> EvidenceDigest {
        self.capability_digest
    }

    /// Returns the exact canonical public-configuration digest.
    pub const fn public_configuration_digest(&self) -> EvidenceDigest {
        self.public_configuration_digest
    }

    /// Returns the reservation's pure initial state.
    pub const fn initial_state(&self) -> OnboardingState {
        self.initial_state
    }

    /// Returns trusted catalog creation time.
    pub const fn created_at(&self) -> Timestamp {
        self.created_at
    }

    /// Returns the retained wall-clock deadline.
    pub const fn deadline_at(&self) -> Timestamp {
        self.deadline_at
    }
}

/// Idempotent append disposition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OnboardingAppendOutcome {
    /// A new contiguous event was committed with its audit record.
    Inserted,
    /// The exact sequence, canonical event, and resulting state were already committed.
    Replay,
}

/// Fully revalidated onboarding session sealed to the current catalog writer.
#[derive(Clone, Debug)]
pub struct ResumedProviderOnboarding {
    reservation: OnboardingReservation,
    public_configuration: ProviderPublicConfiguration,
    lifecycle: OnboardingLifecycle,
    next_sequence: u64,
}

impl ResumedProviderOnboarding {
    /// Returns freshly sealed append authority.
    pub const fn reservation(&self) -> &OnboardingReservation {
        &self.reservation
    }

    /// Returns the recovered canonical non-secret provider configuration.
    pub const fn public_configuration(&self) -> &ProviderPublicConfiguration {
        &self.public_configuration
    }

    /// Returns the exact replayed pure lifecycle.
    pub const fn lifecycle(&self) -> &OnboardingLifecycle {
        &self.lifecycle
    }

    /// Returns the only contiguous sequence accepted for a new event.
    pub const fn next_sequence(&self) -> u64 {
        self.next_sequence
    }
}

impl CatalogAuthority {
    /// Registers one immutable contiguous provider-capability revision.
    pub fn register_provider_capability(
        &self,
        capability: &ProviderCapability,
    ) -> Result<CapabilityRegistrationOutcome, CatalogError> {
        let json = capability.canonical_json()?;
        if json.len() > MAX_CAPABILITY_JSON_BYTES
            || capability.revision().get() > MAX_PROVIDER_REVISIONS
        {
            return Err(CatalogError::InvalidRecord);
        }
        let transaction = self.catalog().connection.unchecked_transaction()?;
        if let Some((digest, retained_json)) = transaction
            .query_row(
                "SELECT capability_sha256, capability_json
                 FROM provider_capability_revisions
                 WHERE surface_id=?1 AND revision=?2",
                params![
                    capability.surface_id().as_str(),
                    to_sql_u64(capability.revision().get())?
                ],
                |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
            )
            .optional()?
        {
            let exact = digest.as_slice() == capability.content_digest().bytes()
                && retained_json == json
                && ProviderCapability::try_from_json(&retained_json)? == *capability;
            return if exact {
                Ok(CapabilityRegistrationOutcome::Replay)
            } else {
                Err(CatalogError::ProviderCapabilityConflict)
            };
        }
        verify_new_capability_position(&transaction, capability)?;
        let registered_at = trusted_catalog_now(&transaction)?;
        transaction.execute(
            "INSERT INTO provider_capability_revisions
             (surface_id, revision, capability_sha256, capability_json, registered_at_ns)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                capability.surface_id().as_str(),
                to_sql_u64(capability.revision().get())?,
                capability.content_digest().bytes(),
                json,
                registered_at.unix_nanos()
            ],
        )?;
        append_audit(
            &transaction,
            "provider-capability.registered",
            capability.surface_id().as_str(),
            capability.content_digest().bytes(),
            registered_at,
        )?;
        transaction.commit()?;
        Ok(CapabilityRegistrationOutcome::Inserted)
    }

    /// Creates a durable non-secret reservation after exact capability registration.
    pub fn reserve_provider_onboarding(
        &self,
        request: &OnboardingReservationRequest,
    ) -> Result<OnboardingReservation, CatalogError> {
        let transaction = self.catalog().connection.unchecked_transaction()?;
        let created_at = trusted_catalog_now(&transaction)?;
        if request.deadline_at() <= created_at {
            return Err(CatalogError::OnboardingDeadlineExceeded);
        }
        require_registered_capability(&transaction, request.capability())?;
        let lifecycle = OnboardingLifecycle::reserve(
            request.capability(),
            request.requested_authority.clone(),
        )?;
        let authority_json = serde_json::to_vec(request.requested_authority())?;
        if authority_json.len() > MAX_AUTHORITY_JSON_BYTES {
            return Err(CatalogError::InvalidRecord);
        }
        let authority_digest = sha256(&authority_json);
        let public_configuration_json = request.public_configuration().canonical_json()?;
        let public_configuration_digest = sha256(&public_configuration_json);
        let session_id = Uuid::new_v4();
        let audit_digest = reservation_audit_digest(
            RESERVATION_SCHEMA_VERSION,
            session_id,
            request,
            lifecycle.state(),
            authority_digest,
            public_configuration_digest,
        )?;
        append_audit(
            &transaction,
            "provider-onboarding.reserved",
            &session_id.to_string(),
            audit_digest,
            created_at,
        )?;
        let audit_sequence = transaction.last_insert_rowid();
        transaction.execute(
            "INSERT INTO provider_onboarding_sessions
             (session_id, surface_id, capability_revision, capability_sha256, setup_mode,
              actor_class, operation_owner, requested_authority_sha256,
              requested_authority_json, initial_state, deadline_at_ns, retry_budget,
              created_at_ns, reservation_audit_sequence, reservation_schema_version,
              public_configuration_sha256, public_configuration_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15,
                     ?16, ?17)",
            params![
                session_id.to_string(),
                request.capability().surface_id().as_str(),
                to_sql_u64(request.capability().revision().get())?,
                request.capability().content_digest().bytes(),
                request.capability().setup_mode().database_name(),
                request.actor_class().as_str(),
                request.operation_owner().as_str(),
                authority_digest,
                authority_json,
                lifecycle.state().database_name(),
                request.deadline_at().unix_nanos(),
                i64::from(request.retry_budget()),
                created_at.unix_nanos(),
                audit_sequence,
                RESERVATION_SCHEMA_VERSION,
                public_configuration_digest,
                public_configuration_json,
            ],
        )?;
        transaction.commit()?;
        Ok(OnboardingReservation {
            catalog_id: self.session_id(),
            session_id,
            capability_digest: request.capability().content_digest(),
            public_configuration_digest: EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                public_configuration_digest,
            ),
            initial_state: lifecycle.state(),
            created_at,
            deadline_at: request.deadline_at(),
        })
    }

    /// Appends one contiguous event, or confirms an exact prior commit.
    pub fn append_provider_onboarding_event(
        &self,
        reservation: &OnboardingReservation,
        sequence: u64,
        event: OnboardingEvent,
    ) -> Result<OnboardingAppendOutcome, CatalogError> {
        if reservation.catalog_id != self.session_id()
            || sequence == 0
            || sequence > MAX_ONBOARDING_EVENTS
        {
            return Err(CatalogError::InvalidOnboardingReservationCapability);
        }
        let event_json = event.canonical_json()?;
        if event_json.len() > MAX_EVENT_JSON_BYTES {
            return Err(CatalogError::InvalidRecord);
        }
        let event_digest = sha256(&event_json);
        let transaction = self.catalog().connection.unchecked_transaction()?;
        let mut budget = ResultBudget::new(self.catalog().result_bytes);
        let mut resumed = load_session(
            &transaction,
            self.session_id(),
            reservation.session_id,
            &mut budget,
        )?;
        if &resumed.reservation != reservation {
            return Err(CatalogError::InvalidOnboardingReservationCapability);
        }
        if sequence < resumed.next_sequence {
            return verify_event_replay(
                &transaction,
                reservation.session_id,
                sequence,
                &event_json,
                event_digest,
            );
        }
        if sequence != resumed.next_sequence {
            return Err(CatalogError::OnboardingSequenceConflict);
        }
        let occurred_at = trusted_catalog_now(&transaction)?;
        if occurred_at >= reservation.deadline_at()
            && !event_allowed_after_deadline(&resumed.lifecycle, &event)
        {
            return Err(CatalogError::OnboardingDeadlineExceeded);
        }
        let resulting_state =
            resumed
                .lifecycle
                .apply(&resumed.capability, event.clone(), occurred_at)?;
        append_audit(
            &transaction,
            onboarding_audit_type(&event),
            &reservation.session_id.to_string(),
            event_digest,
            occurred_at,
        )?;
        let audit_sequence = transaction.last_insert_rowid();
        transaction.execute(
            "INSERT INTO provider_onboarding_events
             (session_id, sequence, event_kind, event_sha256, event_json, resulting_state,
              credential_generation, prior_generation, occurred_at_ns, audit_sequence)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                reservation.session_id.to_string(),
                to_sql_u64(sequence)?,
                event.kind().database_name(),
                event_digest,
                event_json,
                resulting_state.database_name(),
                optional_generation(event.generation())?,
                optional_generation(event.prior_generation())?,
                occurred_at.unix_nanos(),
                audit_sequence
            ],
        )?;
        transaction.commit()?;
        Ok(OnboardingAppendOutcome::Inserted)
    }

    /// Replays and validates every non-secret event, then seals the session to this writer.
    pub fn resume_provider_onboarding(
        &self,
        session_id: Uuid,
    ) -> Result<ResumedProviderOnboarding, CatalogError> {
        let transaction = self.catalog().connection.unchecked_transaction()?;
        let mut budget = ResultBudget::new(self.catalog().result_bytes);
        let loaded = load_session(&transaction, self.session_id(), session_id, &mut budget)?;
        transaction.commit()?;
        Ok(loaded.into_public())
    }

    /// Returns newest-first durable sessions within one global row and byte bound.
    pub fn provider_onboarding_sessions(
        &self,
        limit: super::CatalogLimit,
    ) -> Result<Vec<ResumedProviderOnboarding>, CatalogError> {
        self.list_provider_onboarding_sessions(limit, false)
    }

    /// Returns the latest durable session for each surface in canonical surface order.
    pub fn current_provider_onboarding_sessions(
        &self,
        limit: super::CatalogLimit,
    ) -> Result<Vec<ResumedProviderOnboarding>, CatalogError> {
        self.list_provider_onboarding_sessions(limit, true)
    }

    /// Returns one deterministic page of durable session identities for complete startup scans.
    pub fn provider_onboarding_session_ids_after(
        &self,
        after: Option<Uuid>,
        limit: super::CatalogLimit,
    ) -> Result<Vec<Uuid>, CatalogError> {
        self.catalog().enforce_limit(limit)?;
        let row_limit = i64::try_from(limit.get()).map_err(|_| CatalogError::InvalidLimit)?;
        let after = after.map(|session_id| session_id.hyphenated().to_string());
        let mut statement = self.catalog().connection.prepare(
            "SELECT session_id
             FROM provider_onboarding_sessions
             WHERE (?1 IS NULL OR session_id > ?1)
             ORDER BY session_id
             LIMIT ?2",
        )?;
        let rows = statement.query_map(params![after, row_limit], |row| row.get::<_, String>(0))?;
        let mut budget = ResultBudget::new(self.catalog().result_bytes);
        let mut sessions = Vec::new();
        sessions
            .try_reserve_exact(budget.bounded_row_capacity(limit.get()))
            .map_err(|_| CatalogError::Allocation)?;
        for row in rows {
            let encoded = row?;
            budget.charge([encoded.len()])?;
            sessions.push(Uuid::parse_str(&encoded).map_err(|_| CatalogError::CorruptCatalog)?);
        }
        Ok(sessions)
    }

    fn list_provider_onboarding_sessions(
        &self,
        limit: super::CatalogLimit,
        current_only: bool,
    ) -> Result<Vec<ResumedProviderOnboarding>, CatalogError> {
        self.catalog().enforce_limit(limit)?;
        let transaction = self.catalog().connection.unchecked_transaction()?;
        let row_limit = i64::try_from(limit.get()).map_err(|_| CatalogError::InvalidLimit)?;
        let sql = if current_only {
            "SELECT candidate.session_id
             FROM provider_onboarding_sessions AS candidate
             WHERE NOT EXISTS (
                 SELECT 1
                 FROM provider_onboarding_sessions AS newer
                 WHERE newer.surface_id=candidate.surface_id
                   AND (
                       newer.created_at_ns > candidate.created_at_ns
                       OR (
                           newer.created_at_ns = candidate.created_at_ns
                           AND newer.session_id > candidate.session_id
                       )
                   )
             )
             ORDER BY candidate.surface_id, candidate.session_id
             LIMIT ?1"
        } else {
            "SELECT session_id
             FROM provider_onboarding_sessions
             ORDER BY created_at_ns DESC, surface_id, session_id
             LIMIT ?1"
        };
        let mut statement = transaction.prepare(sql)?;
        let rows = statement.query_map([row_limit], |row| row.get::<_, String>(0))?;
        let mut budget = ResultBudget::new(self.catalog().result_bytes);
        let mut session_ids = Vec::new();
        session_ids
            .try_reserve_exact(budget.bounded_row_capacity(limit.get()))
            .map_err(|_| CatalogError::Allocation)?;
        for row in rows {
            let session_id = row?;
            budget.charge([session_id.len()])?;
            session_ids
                .push(Uuid::parse_str(&session_id).map_err(|_| CatalogError::CorruptCatalog)?);
        }
        drop(statement);
        let mut sessions = Vec::new();
        sessions
            .try_reserve_exact(session_ids.len())
            .map_err(|_| CatalogError::Allocation)?;
        for session_id in session_ids {
            sessions.push(
                load_session(&transaction, self.session_id(), session_id, &mut budget)?
                    .into_public(),
            );
        }
        transaction.commit()?;
        Ok(sessions)
    }
}

struct LoadedOnboarding {
    reservation: OnboardingReservation,
    capability: ProviderCapability,
    public_configuration: ProviderPublicConfiguration,
    lifecycle: OnboardingLifecycle,
    next_sequence: u64,
}

impl LoadedOnboarding {
    fn into_public(self) -> ResumedProviderOnboarding {
        ResumedProviderOnboarding {
            reservation: self.reservation,
            public_configuration: self.public_configuration,
            lifecycle: self.lifecycle,
            next_sequence: self.next_sequence,
        }
    }
}

struct StoredSession {
    surface_id: String,
    capability_revision: i64,
    capability_sha256: Vec<u8>,
    setup_mode: String,
    actor_class: String,
    operation_owner: String,
    authority_sha256: Vec<u8>,
    authority_json: Vec<u8>,
    initial_state: String,
    deadline_at_ns: i64,
    retry_budget: i64,
    created_at_ns: i64,
    reservation_audit_sequence: i64,
    reservation_schema_version: i64,
    public_configuration_sha256: Vec<u8>,
    public_configuration_json: Vec<u8>,
    capability_json: Vec<u8>,
}

impl StoredSession {
    fn from_row(row: &Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            surface_id: row.get(0)?,
            capability_revision: row.get(1)?,
            capability_sha256: row.get(2)?,
            setup_mode: row.get(3)?,
            actor_class: row.get(4)?,
            operation_owner: row.get(5)?,
            authority_sha256: row.get(6)?,
            authority_json: row.get(7)?,
            initial_state: row.get(8)?,
            deadline_at_ns: row.get(9)?,
            retry_budget: row.get(10)?,
            created_at_ns: row.get(11)?,
            reservation_audit_sequence: row.get(12)?,
            reservation_schema_version: row.get(13)?,
            public_configuration_sha256: row.get(14)?,
            public_configuration_json: row.get(15)?,
            capability_json: row.get(16)?,
        })
    }
}

fn load_session(
    transaction: &Transaction<'_>,
    catalog_id: Uuid,
    session_id: Uuid,
    budget: &mut ResultBudget,
) -> Result<LoadedOnboarding, CatalogError> {
    let stored = transaction
        .query_row(
            "SELECT s.surface_id, s.capability_revision, s.capability_sha256, s.setup_mode,
                    s.actor_class, s.operation_owner, s.requested_authority_sha256,
                    s.requested_authority_json, s.initial_state, s.deadline_at_ns,
                    s.retry_budget, s.created_at_ns, s.reservation_audit_sequence,
                    s.reservation_schema_version, s.public_configuration_sha256,
                    s.public_configuration_json,
                    c.capability_json
             FROM provider_onboarding_sessions s
             JOIN provider_capability_revisions c
               ON c.surface_id=s.surface_id
              AND c.revision=s.capability_revision
              AND c.capability_sha256=s.capability_sha256
             WHERE s.session_id=?1",
            [session_id.to_string()],
            StoredSession::from_row,
        )
        .optional()?
        .ok_or(CatalogError::OnboardingSessionNotFound)?;
    budget.charge([
        stored.surface_id.len(),
        stored.actor_class.len(),
        stored.operation_owner.len(),
        stored.authority_sha256.len(),
        stored.authority_json.len(),
        stored.public_configuration_sha256.len(),
        stored.public_configuration_json.len(),
        stored.capability_json.len(),
    ])?;
    let capability = ProviderCapability::try_from_json(&stored.capability_json)?;
    let requested_authority: AuthoritySet = serde_json::from_slice(&stored.authority_json)?;
    let canonical_authority = serde_json::to_vec(&requested_authority)?;
    let public_configuration =
        ProviderPublicConfiguration::try_from_json(&stored.public_configuration_json)?;
    let canonical_public_configuration = public_configuration.canonical_json()?;
    let capability_digest = sha256_digest(&stored.capability_sha256)?;
    let authority_digest = exact_sha256(&stored.authority_sha256)?;
    let public_configuration_digest = exact_sha256(&stored.public_configuration_sha256)?;
    if sha256(&stored.capability_json) != capability.content_digest().bytes()
        || capability.content_digest() != capability_digest
        || sha256(&stored.authority_json) != authority_digest
        || canonical_authority != stored.authority_json
        || sha256(&stored.public_configuration_json) != public_configuration_digest
        || canonical_public_configuration != stored.public_configuration_json
        || capability.surface_id().as_str() != stored.surface_id
        || to_sql_u64(capability.revision().get())? != stored.capability_revision
        || capability.setup_mode().database_name() != stored.setup_mode
        || !(0..=i64::from(MAX_RETRY_BUDGET)).contains(&stored.retry_budget)
    {
        return Err(CatalogError::CorruptCatalog);
    }
    let actor_class = SourceIdentifier::try_from(stored.actor_class.as_str())
        .map_err(|_| CatalogError::CorruptCatalog)?;
    let operation_owner = SourceIdentifier::try_from(stored.operation_owner.as_str())
        .map_err(|_| CatalogError::CorruptCatalog)?;
    let mut lifecycle = OnboardingLifecycle::reserve(&capability, requested_authority.clone())?;
    if lifecycle.state().database_name() != stored.initial_state {
        return Err(CatalogError::CorruptCatalog);
    }
    let created_at = Timestamp::from_unix_nanos(stored.created_at_ns);
    let deadline_at = Timestamp::from_unix_nanos(stored.deadline_at_ns);
    if created_at >= deadline_at {
        return Err(CatalogError::CorruptCatalog);
    }
    let request = OnboardingReservationRequest::try_new(
        &capability,
        public_configuration.clone(),
        requested_authority,
        actor_class,
        operation_owner,
        deadline_at,
        u8::try_from(stored.retry_budget).map_err(|_| CatalogError::CorruptCatalog)?,
    )?;
    verify_reservation_audit(
        transaction,
        &stored,
        session_id,
        &request,
        lifecycle.state(),
        authority_digest,
        public_configuration_digest,
    )?;
    let reservation = OnboardingReservation {
        catalog_id,
        session_id,
        capability_digest,
        public_configuration_digest: EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            public_configuration_digest,
        ),
        initial_state: lifecycle.state(),
        created_at,
        deadline_at,
    };
    let next_sequence =
        replay_events(transaction, session_id, &capability, &mut lifecycle, budget)?;
    Ok(LoadedOnboarding {
        reservation,
        capability,
        public_configuration,
        lifecycle,
        next_sequence,
    })
}

fn replay_events(
    transaction: &Transaction<'_>,
    session_id: Uuid,
    capability: &ProviderCapability,
    lifecycle: &mut OnboardingLifecycle,
    budget: &mut ResultBudget,
) -> Result<u64, CatalogError> {
    let mut statement = transaction.prepare(
        "SELECT e.sequence, e.event_kind, e.event_sha256, e.event_json, e.resulting_state,
                e.credential_generation, e.prior_generation, e.occurred_at_ns,
                a.event_type, a.subject_id, a.details_digest, a.occurred_at_ns
         FROM provider_onboarding_events e
         JOIN audit_events a ON a.sequence=e.audit_sequence
         WHERE e.session_id=?1
         ORDER BY e.sequence",
    )?;
    let mut rows = statement.query([session_id.to_string()])?;
    let mut expected = 1_u64;
    let mut prior_time = lifecycle_created_at(transaction, session_id)?;
    while let Some(row) = rows.next()? {
        if expected > MAX_ONBOARDING_EVENTS {
            return Err(CatalogError::CorruptCatalog);
        }
        let sequence =
            u64::try_from(row.get::<_, i64>(0)?).map_err(|_| CatalogError::CorruptCatalog)?;
        let kind: String = row.get(1)?;
        let event_sha256: Vec<u8> = row.get(2)?;
        let event_json: Vec<u8> = row.get(3)?;
        let resulting_state: String = row.get(4)?;
        let stored_generation = parse_optional_generation(row.get(5)?)?;
        let stored_prior = parse_optional_generation(row.get(6)?)?;
        let occurred_at = Timestamp::from_unix_nanos(row.get(7)?);
        let audit_type: String = row.get(8)?;
        let audit_subject: String = row.get(9)?;
        let audit_digest: Vec<u8> = row.get(10)?;
        let audit_at = Timestamp::from_unix_nanos(row.get(11)?);
        budget.charge([
            kind.len(),
            event_sha256.len(),
            event_json.len(),
            resulting_state.len(),
            audit_type.len(),
            audit_subject.len(),
            audit_digest.len(),
        ])?;
        let digest = exact_sha256(&event_sha256)?;
        let event = OnboardingEvent::try_from_json(&event_json)?;
        if sequence != expected
            || sha256(&event_json) != digest
            || event.canonical_json()? != event_json
            || event.kind().database_name() != kind
            || event.generation() != stored_generation
            || event.prior_generation() != stored_prior
            || onboarding_audit_type(&event) != audit_type
            || audit_subject != session_id.to_string()
            || audit_digest != event_sha256
            || audit_at != occurred_at
            || occurred_at < prior_time
        {
            return Err(CatalogError::CorruptCatalog);
        }
        let state = lifecycle.apply(capability, event, occurred_at)?;
        if state.database_name() != resulting_state {
            return Err(CatalogError::CorruptCatalog);
        }
        expected = expected
            .checked_add(1)
            .ok_or(CatalogError::CorruptCatalog)?;
        prior_time = occurred_at;
    }
    Ok(expected)
}

fn verify_event_replay(
    transaction: &Transaction<'_>,
    session_id: Uuid,
    sequence: u64,
    expected_json: &[u8],
    expected_digest: [u8; 32],
) -> Result<OnboardingAppendOutcome, CatalogError> {
    let existing = transaction
        .query_row(
            "SELECT event_sha256, event_json
             FROM provider_onboarding_events WHERE session_id=?1 AND sequence=?2",
            params![session_id.to_string(), to_sql_u64(sequence)?],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )
        .optional()?;
    match existing {
        Some((digest, json))
            if digest.as_slice() == expected_digest && json.as_slice() == expected_json =>
        {
            Ok(OnboardingAppendOutcome::Replay)
        }
        _ => Err(CatalogError::OnboardingSequenceConflict),
    }
}

fn verify_new_capability_position(
    transaction: &Transaction<'_>,
    capability: &ProviderCapability,
) -> Result<(), CatalogError> {
    let maximum: Option<i64> = transaction.query_row(
        "SELECT MAX(revision) FROM provider_capability_revisions WHERE surface_id=?1",
        [capability.surface_id().as_str()],
        |row| row.get(0),
    )?;
    let expected = maximum.map_or(1_i64, |revision| revision.saturating_add(1));
    if to_sql_u64(capability.revision().get())? != expected {
        return Err(CatalogError::ProviderCapabilityConflict);
    }
    if maximum.is_none() {
        let surfaces: i64 = transaction.query_row(
            "SELECT COUNT(DISTINCT surface_id) FROM provider_capability_revisions",
            [],
            |row| row.get(0),
        )?;
        if surfaces >= MAX_PROVIDER_SURFACES {
            return Err(CatalogError::InvalidRecord);
        }
    }
    Ok(())
}

fn require_registered_capability(
    transaction: &Transaction<'_>,
    capability: &ProviderCapability,
) -> Result<(), CatalogError> {
    let retained: Option<Vec<u8>> = transaction
        .query_row(
            "SELECT capability_json FROM provider_capability_revisions
             WHERE surface_id=?1 AND revision=?2 AND capability_sha256=?3",
            params![
                capability.surface_id().as_str(),
                to_sql_u64(capability.revision().get())?,
                capability.content_digest().bytes()
            ],
            |row| row.get(0),
        )
        .optional()?;
    match retained {
        Some(json)
            if json == capability.canonical_json()?
                && ProviderCapability::try_from_json(&json)? == *capability =>
        {
            Ok(())
        }
        _ => Err(CatalogError::ProviderCapabilityNotRegistered),
    }
}

#[derive(Serialize)]
struct LegacyReservationAudit<'a> {
    version: u8,
    session_id: Uuid,
    surface_id: &'a SourceIdentifier,
    capability_revision: ProviderCapabilityRevision,
    capability_digest: EvidenceDigest,
    setup_mode: market_squawk_sources::SetupMode,
    actor_class: &'a SourceIdentifier,
    operation_owner: &'a SourceIdentifier,
    requested_authority_digest: [u8; 32],
    initial_state: OnboardingState,
    deadline_at: Timestamp,
    retry_budget: u8,
}

#[derive(Serialize)]
struct ReservationAudit<'a> {
    version: u8,
    session_id: Uuid,
    surface_id: &'a SourceIdentifier,
    capability_revision: ProviderCapabilityRevision,
    capability_digest: EvidenceDigest,
    setup_mode: market_squawk_sources::SetupMode,
    actor_class: &'a SourceIdentifier,
    operation_owner: &'a SourceIdentifier,
    requested_authority_digest: [u8; 32],
    public_configuration_digest: [u8; 32],
    initial_state: OnboardingState,
    deadline_at: Timestamp,
    retry_budget: u8,
}

fn reservation_audit_digest(
    schema_version: i64,
    session_id: Uuid,
    request: &OnboardingReservationRequest,
    initial_state: OnboardingState,
    authority_digest: [u8; 32],
    public_configuration_digest: [u8; 32],
) -> Result<[u8; 32], CatalogError> {
    let canonical = match schema_version {
        LEGACY_RESERVATION_SCHEMA_VERSION if request.public_configuration().is_empty() => {
            serde_json::to_vec(&LegacyReservationAudit {
                version: 1,
                session_id,
                surface_id: request.capability().surface_id(),
                capability_revision: request.capability().revision(),
                capability_digest: request.capability().content_digest(),
                setup_mode: request.capability().setup_mode(),
                actor_class: request.actor_class(),
                operation_owner: request.operation_owner(),
                requested_authority_digest: authority_digest,
                initial_state,
                deadline_at: request.deadline_at(),
                retry_budget: request.retry_budget(),
            })?
        }
        RESERVATION_SCHEMA_VERSION => serde_json::to_vec(&ReservationAudit {
            version: 2,
            session_id,
            surface_id: request.capability().surface_id(),
            capability_revision: request.capability().revision(),
            capability_digest: request.capability().content_digest(),
            setup_mode: request.capability().setup_mode(),
            actor_class: request.actor_class(),
            operation_owner: request.operation_owner(),
            requested_authority_digest: authority_digest,
            public_configuration_digest,
            initial_state,
            deadline_at: request.deadline_at(),
            retry_budget: request.retry_budget(),
        })?,
        _ => return Err(CatalogError::CorruptCatalog),
    };
    Ok(sha256(&canonical))
}

fn verify_reservation_audit(
    transaction: &Transaction<'_>,
    stored: &StoredSession,
    session_id: Uuid,
    request: &OnboardingReservationRequest,
    initial_state: OnboardingState,
    authority_digest: [u8; 32],
    public_configuration_digest: [u8; 32],
) -> Result<(), CatalogError> {
    let (event_type, subject, digest, occurred_at): (String, String, Vec<u8>, i64) = transaction
        .query_row(
            "SELECT event_type, subject_id, details_digest, occurred_at_ns
             FROM audit_events WHERE sequence=?1",
            [stored.reservation_audit_sequence],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
    if event_type != "provider-onboarding.reserved"
        || subject != session_id.to_string()
        || digest
            != reservation_audit_digest(
                stored.reservation_schema_version,
                session_id,
                request,
                initial_state,
                authority_digest,
                public_configuration_digest,
            )?
        || occurred_at != stored.created_at_ns
    {
        return Err(CatalogError::CorruptCatalog);
    }
    Ok(())
}

fn lifecycle_created_at(
    transaction: &Transaction<'_>,
    session_id: Uuid,
) -> Result<Timestamp, CatalogError> {
    transaction
        .query_row(
            "SELECT created_at_ns FROM provider_onboarding_sessions WHERE session_id=?1",
            [session_id.to_string()],
            |row| row.get::<_, i64>(0),
        )
        .map(Timestamp::from_unix_nanos)
        .map_err(Into::into)
}

fn event_allowed_after_deadline(lifecycle: &OnboardingLifecycle, event: &OnboardingEvent) -> bool {
    matches!(
        event,
        OnboardingEvent::Cancelled { .. }
            | OnboardingEvent::RefreshRequired { .. }
            | OnboardingEvent::SecretStoreReconciliationRequired { .. }
            | OnboardingEvent::SecretStoreCleared { .. }
            | OnboardingEvent::CandidateCancelledNoEffect { .. }
            | OnboardingEvent::IndeterminateRemoteState { .. }
            | OnboardingEvent::CleanupRequired { .. }
            | OnboardingEvent::RemoteRevocation { .. }
            | OnboardingEvent::LocalDeletion { .. }
            | OnboardingEvent::Retire { .. }
            | OnboardingEvent::Tombstone { .. }
    ) || matches!(
        (lifecycle.state(), event),
        (
            OnboardingState::ActiveScoped,
            OnboardingEvent::RenewalRequired { .. } | OnboardingEvent::BeginRotation { .. }
        ) | (
            OnboardingState::RenewalRequired,
            OnboardingEvent::BeginRotation { .. }
        ) | (
            OnboardingState::RotationPending,
            OnboardingEvent::CredentialImported { .. }
                | OnboardingEvent::SecretStorePlanned { .. }
                | OnboardingEvent::ProtocolValidated { .. }
                | OnboardingEvent::CredentialStored { .. }
                | OnboardingEvent::AuthorityVerified { .. }
                | OnboardingEvent::RightsAdmitted { .. }
                | OnboardingEvent::RatePolicyAdmitted { .. }
                | OnboardingEvent::RuntimeVerified { .. }
                | OnboardingEvent::Cutover { .. }
        )
    )
}

fn onboarding_audit_type(event: &OnboardingEvent) -> &'static str {
    match event.kind() {
        market_squawk_sources::OnboardingEventKind::CredentialImported => {
            "provider-onboarding.credential-imported"
        }
        market_squawk_sources::OnboardingEventKind::SecretStorePlanned => {
            "provider-onboarding.secret-store-planned"
        }
        market_squawk_sources::OnboardingEventKind::SecretStoreReconciliationRequired => {
            "provider-onboarding.secret-store-reconciliation-required"
        }
        market_squawk_sources::OnboardingEventKind::SecretStoreCleared => {
            "provider-onboarding.secret-store-cleared"
        }
        market_squawk_sources::OnboardingEventKind::ProtocolValidated => {
            "provider-onboarding.protocol-validated"
        }
        market_squawk_sources::OnboardingEventKind::CredentialStored => {
            "provider-onboarding.credential-stored"
        }
        market_squawk_sources::OnboardingEventKind::AuthorityVerified => {
            "provider-onboarding.authority-verified"
        }
        market_squawk_sources::OnboardingEventKind::RightsAdmitted => {
            "provider-onboarding.rights-admitted"
        }
        market_squawk_sources::OnboardingEventKind::RatePolicyAdmitted => {
            "provider-onboarding.rate-policy-admitted"
        }
        market_squawk_sources::OnboardingEventKind::RuntimeVerified => {
            "provider-onboarding.runtime-verified"
        }
        market_squawk_sources::OnboardingEventKind::Activate => "provider-onboarding.activated",
        market_squawk_sources::OnboardingEventKind::RenewalRequired => {
            "provider-onboarding.renewal-required"
        }
        market_squawk_sources::OnboardingEventKind::BeginRotation => {
            "provider-onboarding.rotation-begun"
        }
        market_squawk_sources::OnboardingEventKind::CandidateCancelledNoEffect => {
            "provider-onboarding.candidate-cancelled-no-effect"
        }
        market_squawk_sources::OnboardingEventKind::Cutover => "provider-onboarding.cutover",
        market_squawk_sources::OnboardingEventKind::RemoteRevocation => {
            "provider-onboarding.remote-revocation"
        }
        market_squawk_sources::OnboardingEventKind::LocalDeletion => {
            "provider-onboarding.local-deletion"
        }
        market_squawk_sources::OnboardingEventKind::Retire => "provider-onboarding.retired",
        market_squawk_sources::OnboardingEventKind::Tombstone => "provider-onboarding.tombstoned",
        market_squawk_sources::OnboardingEventKind::RefreshRequired => {
            "provider-onboarding.refresh-required"
        }
        market_squawk_sources::OnboardingEventKind::Unavailable => {
            "provider-onboarding.unavailable"
        }
        market_squawk_sources::OnboardingEventKind::IndeterminateRemoteState => {
            "provider-onboarding.indeterminate-remote-state"
        }
        market_squawk_sources::OnboardingEventKind::CleanupRequired => {
            "provider-onboarding.cleanup-required"
        }
        market_squawk_sources::OnboardingEventKind::Blocked => "provider-onboarding.blocked",
        market_squawk_sources::OnboardingEventKind::Cancelled => "provider-onboarding.cancelled",
    }
}

fn optional_generation(value: Option<SecretGeneration>) -> Result<Option<i64>, CatalogError> {
    value
        .map(|generation| to_sql_u64(generation.get()))
        .transpose()
}

fn parse_optional_generation(value: Option<i64>) -> Result<Option<SecretGeneration>, CatalogError> {
    value
        .map(|value| {
            let value = u64::try_from(value).map_err(|_| CatalogError::CorruptCatalog)?;
            SecretGeneration::new(value).map_err(|_| CatalogError::CorruptCatalog)
        })
        .transpose()
}

fn to_sql_u64(value: u64) -> Result<i64, CatalogError> {
    i64::try_from(value).map_err(|_| CatalogError::InvalidRecord)
}

fn exact_sha256(bytes: &[u8]) -> Result<[u8; 32], CatalogError> {
    bytes.try_into().map_err(|_| CatalogError::CorruptCatalog)
}

fn sha256_digest(bytes: &[u8]) -> Result<EvidenceDigest, CatalogError> {
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        exact_sha256(bytes)?,
    ))
}
