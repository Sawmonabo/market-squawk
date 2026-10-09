//! Stateful source registration and current-session authority handles.

use std::cell::Cell;
use std::collections::{BTreeMap, HashMap};
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

use arc_swap::ArcSwap;
use market_squawk_domain::SchemaVersion;
use market_squawk_domain::{
    ConnectionGeneration, CoverageConsolidation, CoverageDelay, DeliveryEvidence,
    EffectiveInterval, ExactPayloadEvidence, InstrumentId, LiveEventClass, MarketDepth,
    MetadataRevision, ProviderProduct, RevisionBoundPayloadEvidence, SourceId, SourceIdentifier,
    Timestamp, VenueId,
};
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

use crate::authority_time::{
    AuthorityTimeContinuity, RawRegistryClockSource, RegistryMonotonicInstant, SealedRegistryClock,
    SystemRawRegistryClock, TrustedReceiptObservation, TrustedRegistryTime,
};
use crate::bounded::BoundedVec;
use crate::policy::{
    AuthorityDurabilitySession, AuthorityPersistenceError, BudgetAvailabilityLease,
    BudgetPermitLease, BudgetPolicyResolutionError, DurableBudgetGroup,
    PersistedProviderBudgetPolicy, ProviderBudgetPool, ResolvedProviderBudgetPolicy,
};
use crate::{FrameSessionBinding, SessionId, SharedProviderBudget, SourceMetadata};

static NEXT_REGISTRY_ID: AtomicU64 = AtomicU64::new(1);
const MAX_REVISIONS_PER_SOURCE: usize = 4_096;
const MAX_AUTHORITY_SOURCES: usize = 4_096;
const MAX_BUDGET_SCOPES: usize = 4_096;

#[derive(Clone, Debug)]
struct ActiveSessionKey {
    session_id: SessionId,
    generation: ConnectionGeneration,
    lease: Arc<SessionLeaseState>,
    capture_issuer_taken: bool,
    health_reporter_taken: bool,
    raw_frame_factory_taken: bool,
    capture: crate::CaptureGenerationLease,
    started_at: TrustedRegistryTime,
}

#[derive(Debug)]
struct SessionLeaseState {
    current: AtomicBool,
    terminal: AtomicBool,
    health: ArcSwap<SessionHealthQualification>,
    last_health_observed_nanos: AtomicI64,
    frame_ordinal: AtomicU64,
    continuity: AuthorityTimeContinuity,
    started_at: TrustedRegistryTime,
}

#[derive(Clone, Copy, Debug)]
struct HealthEpochInterval {
    epoch: u64,
    valid_from: Timestamp,
    valid_until: Timestamp,
}

#[derive(Clone, Debug, Default)]
struct SessionHealthQualification {
    epoch: u64,
    current: Option<HealthEpochInterval>,
    first_valid_epoch: Option<u64>,
}

#[derive(Debug)]
struct RegistrationLeaseState {
    current: AtomicBool,
    invalidated: tokio::sync::Notify,
}

impl RegistrationLeaseState {
    fn new() -> Self {
        Self {
            current: AtomicBool::new(true),
            invalidated: tokio::sync::Notify::new(),
        }
    }

    fn invalidate(&self) {
        self.current.store(false, Ordering::Release);
        self.invalidated.notify_waiters();
    }

    fn is_current(&self) -> bool {
        self.current.load(Ordering::Acquire)
    }
}

impl SessionLeaseState {
    fn invalidate(&self) {
        self.current.store(false, Ordering::Release);
        self.health.rcu(|health| SessionHealthQualification {
            epoch: health.epoch.saturating_add(1),
            ..SessionHealthQualification::default()
        });
    }

    fn is_current(&self) -> bool {
        self.current.load(Ordering::Acquire)
            && !self.is_terminal()
            && self.continuity.is_continuous()
    }

    fn is_terminal(&self) -> bool {
        self.terminal.load(Ordering::Acquire)
    }

    fn terminally_invalidate_health_authority(&self) {
        self.current.store(false, Ordering::Release);
        self.terminal.store(true, Ordering::Release);
        self.health.rcu(|health| SessionHealthQualification {
            epoch: health.epoch,
            ..SessionHealthQualification::default()
        });
    }

    fn next_health_epoch(&self) -> Option<u64> {
        if self.is_terminal() {
            return None;
        }
        self.health.load().epoch.checked_add(1)
    }

    fn commit_live_qualification(
        &self,
        epoch: u64,
        qualified: bool,
        benign_renewal: bool,
        valid_from: Option<Timestamp>,
        valid_until: Option<Timestamp>,
    ) {
        let current = match (qualified, valid_from, valid_until) {
            (true, Some(valid_from), Some(valid_until)) => Some(HealthEpochInterval {
                epoch,
                valid_from,
                valid_until,
            }),
            _ => None,
        };
        // Keep a constant-size uninterrupted healthy run, not a count-limited queue window.
        // Every retained lease still checks its own original wall/monotonic interval. A scope
        // change, narrowing, gap or unhealthy update starts a new run and cannot revive old work.
        let prior = self.health.load();
        let first_valid_epoch = current.map(|next| {
            if benign_renewal
                && prior.current.is_some_and(|old| {
                    old.valid_from <= next.valid_from && next.valid_from <= old.valid_until
                })
            {
                prior.first_valid_epoch.unwrap_or(epoch)
            } else {
                epoch
            }
        });
        self.health.store(Arc::new(SessionHealthQualification {
            epoch,
            current,
            first_valid_epoch,
        }));
    }

    // This checks continuity only. All callers separately check their captured original interval.
    fn validate_health_epoch(&self, epoch: u64) -> bool {
        let health = self.health.load();
        self.is_current()
            && health.first_valid_epoch.is_some_and(|first| first <= epoch)
            && health.current.is_some_and(|current| epoch <= current.epoch)
    }

    fn shared_allocation_charge() -> Option<usize> {
        let health = market_squawk_domain::checked_arc_value_allocation_bytes::<
            SessionHealthQualification,
        >(0)
        .ok()?;
        market_squawk_domain::checked_arc_value_allocation_bytes::<Self>(health).ok()
    }

    fn next_frame_id(&self) -> Result<crate::FrameId, crate::SourceError> {
        if !self.is_current() {
            return Err(crate::SourceError::SessionNotCurrent);
        }
        let previous = self
            .frame_ordinal
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map_err(|_| {
                self.invalidate();
                crate::SourceError::FrameIdentityExhausted
            })?;
        let value = previous
            .checked_add(1)
            .and_then(std::num::NonZeroU64::new)
            .ok_or_else(|| {
                self.invalidate();
                crate::SourceError::FrameIdentityExhausted
            })?;
        Ok(crate::FrameId::new(value))
    }

    fn validate_receipt(&self, receipt: &TrustedReceiptObservation) -> Result<(), RegistryError> {
        self.continuity.validate_receipt(receipt, self.started_at)
    }
}

#[derive(Clone, Debug)]
struct CurrentHealthAuthority {
    snapshot: Arc<crate::SourceHealthSnapshot>,
    epoch: u64,
    observed_at: Timestamp,
    trusted_reported_at: TrustedRegistryTime,
    accepted_at: TrustedRegistryTime,
    valid_from: Timestamp,
    valid_until: Timestamp,
    valid_until_monotonic: RegistryMonotonicInstant,
    permission_valid_until: Timestamp,
    permission_valid_until_monotonic: RegistryMonotonicInstant,
    authorization: crate::AuthorizationHealth,
    coverage: crate::CoverageHealth,
    budget: CurrentBudgetAuthority,
}

/// Result of recording one exact-generation health observation.
///
/// An unqualified result never carries live-data authority. Its retained cause classification is
/// intentionally opaque so callers cannot reconstruct or weaken the registry's qualification
/// predicate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub enum CurrentHealthRecording {
    /// Every registry-owned current-data requirement was satisfied.
    Qualified,
    /// The observation was retained but issued no current-data authority.
    Unqualified(CurrentHealthUnqualification),
}

/// Opaque reason set for a retained health observation that issued no current-data authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CurrentHealthUnqualification {
    causes: u16,
}

impl CurrentHealthUnqualification {
    const CAPTURE: u16 = 1 << 0;
    const CONNECTION_FRESHNESS: u16 = 1 << 1;
    const TRANSPORT_FRESHNESS: u16 = 1 << 2;
    const MARKET_FRESHNESS: u16 = 1 << 3;
    const SOURCE_FRESHNESS: u16 = 1 << 4;
    const STREAM_INTEGRITY: u16 = 1 << 5;
    const CAPTURE_INTEGRITY: u16 = 1 << 6;
    const AUTHORIZATION: u16 = 1 << 7;
    const COVERAGE: u16 = 1 << 8;
    const SNAPSHOT_BUDGET: u16 = 1 << 9;
    const REPORTER_BUDGET: u16 = 1 << 10;
    const LAST_ERROR: u16 = 1 << 11;
    const CURRENT_DATA_DEADLINE: u16 = 1 << 12;
    const STATIC_DEADLINE: u16 = 1 << 13;
    const OBSERVATION_TIME: u16 = 1 << 14;
    const FRESHNESS: u16 = Self::CONNECTION_FRESHNESS
        | Self::TRANSPORT_FRESHNESS
        | Self::MARKET_FRESHNESS
        | Self::SOURCE_FRESHNESS
        | Self::CURRENT_DATA_DEADLINE;

    const fn new(causes: u16) -> Self {
        Self { causes }
    }

    /// Returns true only when every rejected dimension is a current-data freshness clock.
    ///
    /// Authorization, coverage, budget, capture, integrity, and provider-error failures can never
    /// be classified as freshness-only, including when one also coexists with stale data.
    pub const fn is_freshness_only(self) -> bool {
        self.causes != 0 && self.causes & !Self::FRESHNESS == 0
    }
}

#[derive(Debug)]
struct UnconfiguredAuthorizationSubjectResolver;

impl crate::AuthorizationSubjectResolver for UnconfiguredAuthorizationSubjectResolver {
    fn resolve_subject_record(
        &self,
        _mode: crate::AuthorizationMode,
        _evidence: market_squawk_domain::EvidenceDigest,
    ) -> Result<SourceIdentifier, crate::AuthorizationSubjectResolutionError> {
        Err(crate::AuthorizationSubjectResolutionError::EvidenceUnresolved)
    }
}

#[derive(Clone, Debug)]
enum CurrentBudgetAuthority {
    NotRequired,
    Available(BudgetAvailabilityLease),
    ActiveRequest(BudgetPermitLease),
    Unavailable,
}

impl CurrentBudgetAuthority {
    fn observe(budget: Option<&SharedProviderBudget>) -> Self {
        let Some(budget) = budget else {
            return Self::NotRequired;
        };
        match budget.availability_lease() {
            Ok(lease) => Self::Available(lease),
            Err(_) => Self::Unavailable,
        }
    }

    fn observe_active_request(
        budget: Option<&SharedProviderBudget>,
        lease: &BudgetPermitLease,
    ) -> Result<Self, RegistryError> {
        let budget = budget.ok_or(RegistryError::BudgetAuthorityMismatch)?;
        if !lease.shares_allocation_with(budget) || !lease.is_current() {
            return Err(RegistryError::BudgetAuthorityMismatch);
        }
        Ok(Self::ActiveRequest(lease.clone()))
    }

    fn is_available(&self) -> bool {
        match self {
            Self::NotRequired => true,
            Self::Available(lease) => lease.is_available(),
            Self::ActiveRequest(lease) => lease.is_current(),
            Self::Unavailable => false,
        }
    }

    fn health(&self) -> crate::BudgetHealth {
        if self.is_available() {
            crate::BudgetHealth::Available
        } else {
            crate::BudgetHealth::Unavailable
        }
    }

    fn shared_allocation_charge(&self) -> Result<usize, RegistryError> {
        match self {
            Self::Available(lease) => lease
                .shared_allocation_charge()
                .ok_or(RegistryError::RetainedSizeOverflow),
            Self::ActiveRequest(lease) => lease
                .shared_allocation_charge()
                .ok_or(RegistryError::RetainedSizeOverflow),
            Self::NotRequired | Self::Unavailable => Ok(0),
        }
    }
}

#[derive(Clone, Debug)]
struct RegistryEntry {
    metadata: SourceMetadata,
    epoch: u64,
    revoked: bool,
    registration_lease: Arc<RegistrationLeaseState>,
    active: Option<ActiveSessionKey>,
    health_authority: Option<CurrentHealthAuthority>,
    universe_attestation: Option<InstrumentUniverseAttestation>,
    provider_identities: Vec<CurrentProviderIdentity>,
    generation_high_water: Option<ConnectionGeneration>,
    used_revisions: Vec<MetadataRevision>,
}

impl RegistryEntry {
    fn terminally_invalidate_health_authority(&mut self) {
        if let Some(active) = &self.active {
            active.lease.terminally_invalidate_health_authority();
            active.capture.mark_incomplete();
        }
        self.health_authority = None;
    }
}

/// Native coordinates supplied to the catalog by trusted source composition.
/// Values describe a requested route; constructing them grants no authority.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderNativeIdentityRequest {
    /// Identity namespace, independent of the live source identifier.
    pub namespace: SourceId,
    /// Exact identity in that namespace.
    pub provider_instrument_id: market_squawk_domain::ProviderInstrumentId,
    /// Exact canonical route expected by the application.
    pub instrument: InstrumentId,
    /// Source-metadata feed-route venue; it need not be a canonical trading venue.
    pub venue: VenueId,
    /// Explicit route symbol: either the byte-exact selected provider ID or a symbol proven by
    /// the canonical definition's exact venue mapping. No inferred or normalized alias is admitted.
    pub venue_symbol: market_squawk_domain::VenueSymbol,
    /// Inclusive catalog knowledge cutoff.
    pub knowledge_at: Timestamp,
    /// Effective identity cutoff.
    pub effective_at: Timestamp,
}

/// Replayable evidence only; a copied projection cannot mint a current registry mapping.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderIdentitySelectionEvidence {
    /// Complete native and canonical coordinates and original selection cutoffs.
    pub native: ProviderNativeIdentityRequest,
    /// Exact immutable definition revision.
    pub definition_digest: market_squawk_domain::EvidenceDigest,
    /// Exact immutable definition position.
    pub definition_sequence: u32,
    /// Reference assertion revision.
    pub reference_revision: MetadataRevision,
    /// Reference assertion payload.
    pub reference_payload_digest: market_squawk_domain::EvidenceDigest,
    /// First durable publication time of the selected definition.
    pub definition_published_at: Timestamp,
    /// Definition validity; the end is exclusive.
    pub definition_validity: EffectiveInterval,
    /// Provider assertion revision.
    pub provider_revision: MetadataRevision,
    /// Provider assertion payload.
    pub provider_payload_digest: market_squawk_domain::EvidenceDigest,
    /// Provider assertion validity; the end is exclusive.
    pub provider_validity: EffectiveInterval,
    /// Digest of the original source-qualified catalog resolution.
    pub resolution_digest: market_squawk_domain::EvidenceDigest,
    /// Digest of the original opaque catalog selection.
    pub selection_digest: market_squawk_domain::EvidenceDigest,
}

impl ProviderIdentitySelectionEvidence {
    /// Checked retained dynamic bytes for this bounded evidence projection.
    pub fn dynamic_retained_bytes(&self) -> Option<usize> {
        [
            self.native.namespace.retained_bytes(),
            self.native.provider_instrument_id.retained_bytes(),
            self.native.venue.retained_bytes(),
            self.native.venue_symbol.retained_bytes(),
            self.reference_revision
                .as_source_identifier()
                .retained_bytes(),
            self.provider_revision
                .as_source_identifier()
                .retained_bytes(),
        ]
        .into_iter()
        .try_fold(0usize, usize::checked_add)
    }
}

/// Validation-only catalog selection installed by trusted application composition.
///
/// This open dependency-inversion seam is not a compiler-enforced proof against arbitrary
/// composition code. Production installs the data catalog implementation once; adapters receive
/// native coordinates, never a selectable verifier. Implementations must retain an opaque exact
/// catalog selection, reject replacement/expiry, and perform no catalog I/O in `validate_at`.
pub trait CurrentCatalogProviderIdentity: std::fmt::Debug + Send + Sync {
    /// Immutable evidence describing the exact selection, not a minting input.
    fn evidence(&self) -> &ProviderIdentitySelectionEvidence;
    /// Rechecks revocation and half-open validity without catalog I/O.
    fn validate_at(&self, at: Timestamp) -> Result<(), RegistryError>;
    /// Complete checked shared allocation charge, including retained selection evidence.
    fn retained_bytes(&self) -> Result<usize, RegistryError>;
}

/// Catalog read authority fixed by composition before the registry registers any sources.
pub trait CatalogProviderIdentityAuthority: std::fmt::Debug + Send + Sync {
    /// Selects and verifies the exact current catalog route with bounded control-plane I/O.
    fn select_current(
        &self,
        request: &ProviderNativeIdentityRequest,
        deadline: std::time::Instant,
        cancellation: &tokio_util::sync::CancellationToken,
    ) -> Result<Arc<dyn CurrentCatalogProviderIdentity>, RegistryError>;
}

/// Opaque catalog selection bound privately to one exact source registration.
///
/// This is identity authority only. Current observations must also retain and validate their
/// existing source session, account/authorization generation, health, capture, and budget lease.
#[derive(Clone, Debug)]
pub struct CurrentProviderIdentity {
    source_id: SourceId,
    source_revision: RevisionBoundPayloadEvidence,
    registration: Arc<RegistrationLeaseState>,
    selected: Arc<dyn CurrentCatalogProviderIdentity>,
}

impl CurrentProviderIdentity {
    /// Returns replayable catalog evidence without exposing a constructor.
    pub fn evidence(&self) -> &ProviderIdentitySelectionEvidence {
        self.selected.evidence()
    }

    /// Returns the independently bound live source identifier.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact source metadata and authorization binding.
    pub const fn source_revision(&self) -> &RevisionBoundPayloadEvidence {
        &self.source_revision
    }

    /// Checks catalog replacement/expiry and source registration replacement/revocation.
    pub fn validate_at(&self, at: Timestamp) -> Result<(), RegistryError> {
        if !self.registration.is_current() {
            return Err(RegistryError::StaleHandle);
        }
        self.selected.validate_at(at)
    }

    /// Earliest inclusive end of the catalog definition and provider assertion, when bounded.
    /// Current source metadata, authorization, and health can only shorten this deadline.
    pub fn inclusive_deadline(&self) -> Option<Timestamp> {
        let evidence = self.evidence();
        [
            evidence.definition_validity.ends_at(),
            evidence.provider_validity.ends_at(),
        ]
        .into_iter()
        .flatten()
        .min()
        .and_then(|end| end.checked_sub_nanos(1).ok())
    }

    /// Returns a conservative complete checked charge for retained identity authority.
    pub fn retained_bytes(&self) -> Result<usize, RegistryError> {
        std::mem::size_of::<Self>()
            .checked_add(self.source_id.retained_bytes())
            .and_then(|size| {
                size.checked_add(
                    self.source_revision
                        .metadata_revision()
                        .as_source_identifier()
                        .retained_bytes(),
                )
            })
            .and_then(|size| {
                size.checked_add(
                    self.source_revision
                        .payload_evidence()
                        .dynamic_retained_bytes()?,
                )
            })
            .and_then(|size| size.checked_add(std::mem::size_of::<RegistrationLeaseState>()))
            .and_then(|size| {
                size.checked_add(crate::conservative_arc_control_block_charge::<
                    RegistrationLeaseState,
                >())
            })
            .and_then(|size| size.checked_add(self.selected.retained_bytes().ok()?))
            .ok_or(RegistryError::RetainedSizeOverflow)
    }
}

/// Exact registry-recorded provider-universe membership attestation.
#[derive(Clone, Debug)]
pub struct InstrumentUniverseAttestation {
    provider_product: ProviderProduct,
    evidence: ExactPayloadEvidence,
    effective: EffectiveInterval,
    instruments: crate::InstrumentCoverage,
}

impl InstrumentUniverseAttestation {
    /// Constructs a bounded exact universe set; this value is evidence input, not authority until
    /// it is recorded by the authoritative registry for one current metadata revision.
    ///
    /// # Errors
    ///
    /// Rejects an empty, duplicate, or oversized instrument set.
    pub fn try_new(
        provider_product: ProviderProduct,
        evidence: ExactPayloadEvidence,
        effective: EffectiveInterval,
        instruments: Vec<InstrumentId>,
    ) -> Result<Self, crate::SourceMetadataError> {
        Ok(Self {
            provider_product,
            evidence,
            effective,
            instruments: crate::InstrumentCoverage::enumerated(instruments)?,
        })
    }

    /// Returns exact attestation evidence.
    pub const fn evidence(&self) -> &ExactPayloadEvidence {
        &self.evidence
    }

    fn contains(&self, instrument: InstrumentId) -> bool {
        self.instruments.membership(instrument) == crate::InstrumentCoverageMembership::Enumerated
    }

    fn is_effective_at(&self, at: Timestamp) -> bool {
        at >= self.effective.starts_at() && self.effective.ends_at().is_none_or(|end| at < end)
    }

    fn inclusive_deadline(&self) -> Option<Timestamp> {
        self.effective
            .ends_at()
            .and_then(|end| end.checked_sub_nanos(1).ok())
    }
}

#[derive(Clone, Debug)]
struct SourceAuthorityHistory {
    used_revisions: Vec<MetadataRevision>,
    latest_revision_evidence: Option<RevisionBoundPayloadEvidence>,
    revoked: bool,
    last_epoch: u64,
    generation_high_water: Option<ConnectionGeneration>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedSourceAuthority {
    source_id: SourceId,
    used_revisions: BoundedVec<MetadataRevision, MAX_REVISIONS_PER_SOURCE>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    latest_revision_evidence: Option<RevisionBoundPayloadEvidence>,
    #[serde(default, skip_serializing_if = "is_false")]
    revoked: bool,
    last_epoch: u64,
    generation_high_water: Option<ConnectionGeneration>,
}

/// Bounded, versioned restart state for source authority tombstones and shared budget scopes.
///
/// This serializable control-plane value contains no registered/current handles, session leases,
/// credentials, or live health authority.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RegistryAuthorityState {
    schema_version: SchemaVersion,
    sources: BoundedVec<PersistedSourceAuthority, MAX_AUTHORITY_SOURCES>,
    budget_policies: BoundedVec<PersistedProviderBudgetPolicy, MAX_BUDGET_SCOPES>,
}

/// Canonical clean-restart image for registry tombstones and durable provider-budget checkpoints.
///
/// The opaque payload contains no registry handles, active sessions, request permits, health
/// authority, runtime clock handles, or in-use run marker. It can only be minted after the live
/// registry proves that every durable budget allocation has zero in-flight requests.
pub(crate) struct RegistryCleanRestartBackup {
    bytes: Box<[u8]>,
}

impl RegistryCleanRestartBackup {
    /// Validates one canonical owner-issued clean-restart image.
    ///
    /// # Errors
    ///
    /// Rejects malformed, noncanonical, in-use, future-dated, or non-clean budget state.
    pub(crate) fn try_from_bytes(bytes: &[u8]) -> Result<Self, RegistryError> {
        let now = current_registry_wall_time()?;
        let envelope = crate::policy::deserialize_clean_restart_backup(bytes, now)
            .map_err(map_authority_persistence_error)?;
        let canonical = crate::policy::serialize_clean_restart_backup(&envelope)
            .map_err(map_authority_persistence_error)?;
        Ok(Self {
            bytes: canonical.into_boxed_slice(),
        })
    }

    /// Returns the exact canonical clean-restart bytes.
    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Seeds an absent production store without opening registry or runtime authority.
    ///
    /// Normal startup must subsequently reconstruct the registry and adapters through their usual
    /// constructors. Existing authority state is never overwritten.
    pub(crate) fn restore_fresh(
        &self,
        store: market_squawk_platform::LocalAuthorityStateStore,
    ) -> Result<(), RegistryError> {
        if store
            .load()
            .map_err(|_error| RegistryError::AuthorityPersistence)?
            .is_some()
        {
            return Err(RegistryError::InvalidAuthorityState);
        }
        store
            .store(&self.bytes)
            .map_err(|_error| RegistryError::AuthorityPersistence)
    }
}

impl std::fmt::Debug for RegistryCleanRestartBackup {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RegistryCleanRestartBackup")
            .field("byte_length", &self.bytes.len())
            .finish_non_exhaustive()
    }
}

fn current_registry_wall_time() -> Result<Timestamp, RegistryError> {
    let clock = SealedRegistryClock::new(Arc::new(SystemRawRegistryClock::try_new()?));
    clock.observe().map(TrustedRegistryTime::wall)
}

impl RegistryAuthorityState {
    pub(crate) fn empty() -> Self {
        Self {
            schema_version: SchemaVersion::CURRENT,
            sources: BoundedVec::empty(),
            budget_policies: BoundedVec::empty(),
        }
    }

    fn try_new(
        sources: Vec<PersistedSourceAuthority>,
        budget_policies: Vec<PersistedProviderBudgetPolicy>,
    ) -> Result<Self, RegistryError> {
        if sources.iter().any(|source| {
            source.last_epoch == 0
                || source.used_revisions.is_empty()
                || contains_duplicate_revisions(source.used_revisions.as_slice())
                || (source.revoked && source.latest_revision_evidence.is_some())
                || source
                    .latest_revision_evidence
                    .as_ref()
                    .is_some_and(|evidence| {
                        source
                            .used_revisions
                            .as_slice()
                            .last()
                            .is_none_or(|latest| latest != evidence.metadata_revision())
                    })
        }) || sources.iter().enumerate().any(|(index, source)| {
            sources[index.saturating_add(1)..]
                .iter()
                .any(|other| source.source_id == other.source_id)
        }) || budget_policies.iter().enumerate().any(|(index, policy)| {
            budget_policies[index.saturating_add(1)..]
                .iter()
                .any(|other| policy == other)
        }) {
            return Err(RegistryError::InvalidAuthorityState);
        }
        Ok(Self {
            schema_version: SchemaVersion::CURRENT,
            sources: BoundedVec::try_new(sources)
                .map_err(|_| RegistryError::AuthorityStateCapacity)?,
            budget_policies: BoundedVec::try_new(budget_policies)
                .map_err(|_| RegistryError::AuthorityStateCapacity)?,
        })
    }

    pub(crate) fn canonicalize(&mut self) -> Result<(), crate::policy::AuthorityPersistenceError> {
        let mut sources = Vec::new();
        sources
            .try_reserve(self.sources.len())
            .map_err(|_| crate::policy::AuthorityPersistenceError::StateTooLarge)?;
        for source in self.sources.as_slice() {
            if contains_duplicate_revisions(source.used_revisions.as_slice())
                || (source.revoked && source.latest_revision_evidence.is_some())
                || source
                    .latest_revision_evidence
                    .as_ref()
                    .is_some_and(|evidence| {
                        source
                            .used_revisions
                            .as_slice()
                            .last()
                            .is_none_or(|latest| latest != evidence.metadata_revision())
                    })
            {
                return Err(crate::policy::AuthorityPersistenceError::InvalidState);
            }
            sources.push(source.clone());
        }
        sources.sort_by(|left, right| left.source_id.cmp(&right.source_id));
        if sources
            .windows(2)
            .any(|pair| pair[0].source_id == pair[1].source_id)
        {
            return Err(crate::policy::AuthorityPersistenceError::InvalidState);
        }
        let mut policies = Vec::new();
        policies
            .try_reserve(self.budget_policies.len())
            .map_err(|_| crate::policy::AuthorityPersistenceError::StateTooLarge)?;
        for policy in self.budget_policies.as_slice() {
            let key = serde_json::to_vec(policy)
                .map_err(|_| crate::policy::AuthorityPersistenceError::InvalidState)?;
            policies.push((key, policy.clone()));
        }
        policies.sort_by(|left, right| left.0.cmp(&right.0));
        if policies.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return Err(crate::policy::AuthorityPersistenceError::InvalidState);
        }
        let mut canonical_policies = Vec::new();
        canonical_policies
            .try_reserve(policies.len())
            .map_err(|_| crate::policy::AuthorityPersistenceError::StateTooLarge)?;
        for (_key, policy) in policies {
            canonical_policies.push(policy);
        }
        self.sources = BoundedVec::try_new(sources)
            .map_err(|_| crate::policy::AuthorityPersistenceError::StateTooLarge)?;
        self.budget_policies = BoundedVec::try_new(canonical_policies)
            .map_err(|_| crate::policy::AuthorityPersistenceError::StateTooLarge)?;
        Ok(())
    }

    pub(crate) fn budget_policies(&self) -> &[PersistedProviderBudgetPolicy] {
        self.budget_policies.as_slice()
    }
}

const fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RegistryAuthorityStateWire {
    schema_version: SchemaVersion,
    sources: BoundedVec<PersistedSourceAuthority, MAX_AUTHORITY_SOURCES>,
    budget_policies: BoundedVec<PersistedProviderBudgetPolicy, MAX_BUDGET_SCOPES>,
}

impl<'de> Deserialize<'de> for RegistryAuthorityState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = RegistryAuthorityStateWire::deserialize(deserializer)?;
        wire.schema_version
            .ensure_supported()
            .map_err(serde::de::Error::custom)?;
        Self::try_new(
            wire.sources.as_slice().to_vec(),
            wire.budget_policies.as_slice().to_vec(),
        )
        .map_err(serde::de::Error::custom)
    }
}

include!("registry/catalog.rs");
#[path = "registry/catalog/construction.rs"]
mod catalog_construction;
pub use catalog_construction::RESEARCH_SOURCE_AUTHORITY_DIRECTORY;
#[path = "registry/catalog/persistence.rs"]
mod catalog_persistence;
include!("registry/health_authority.rs");
include!("registry/authority.rs");
include!("registry/decode_outcome.rs");
include!("registry/current_batch.rs");
#[cfg(test)]
#[path = "registry/canonicalization_tests.rs"]
mod canonicalization_tests;
#[cfg(test)]
#[path = "registry/test_support.rs"]
mod test_support;
include!("registry/tests.rs");
