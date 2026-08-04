//! Stateful source registration and current-session authority handles.

use std::cell::Cell;
use std::collections::{BTreeMap, HashMap};
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

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
    BudgetPolicyResolutionError, DurableBudgetGroup, PersistedProviderBudgetPolicy,
    ProviderBudgetPool, ResolvedProviderBudgetPolicy,
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
    live_qualified: AtomicBool,
    health_epoch: AtomicU64,
    valid_from_nanos: AtomicI64,
    valid_until_nanos: AtomicI64,
    last_health_observed_nanos: AtomicI64,
    frame_ordinal: AtomicU64,
    continuity: AuthorityTimeContinuity,
    started_at: TrustedRegistryTime,
}

#[derive(Debug)]
struct RegistrationLeaseState {
    current: AtomicBool,
}

impl RegistrationLeaseState {
    fn new() -> Self {
        Self {
            current: AtomicBool::new(true),
        }
    }

    fn invalidate(&self) {
        self.current.store(false, Ordering::Release);
    }

    fn is_current(&self) -> bool {
        self.current.load(Ordering::Acquire)
    }
}

impl SessionLeaseState {
    fn invalidate(&self) {
        self.current.store(false, Ordering::Release);
        self.live_qualified.store(false, Ordering::Release);
        if self.advance_health_epoch().is_none() {
            self.valid_until_nanos.store(i64::MIN, Ordering::Release);
        }
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
        self.live_qualified.store(false, Ordering::Release);
        self.valid_from_nanos.store(i64::MAX, Ordering::Release);
        self.valid_until_nanos.store(i64::MIN, Ordering::Release);
        self.current.store(false, Ordering::Release);
        self.terminal.store(true, Ordering::Release);
    }

    fn next_health_epoch(&self) -> Option<u64> {
        if self.is_terminal() {
            return None;
        }
        self.health_epoch.load(Ordering::Acquire).checked_add(1)
    }

    fn commit_live_qualification(
        &self,
        epoch: u64,
        qualified: bool,
        valid_from: Option<Timestamp>,
        valid_until: Option<Timestamp>,
    ) {
        self.live_qualified.store(false, Ordering::Release);
        self.valid_from_nanos.store(
            valid_from.map_or(i64::MAX, Timestamp::unix_nanos),
            Ordering::Release,
        );
        self.valid_until_nanos.store(
            valid_until.map_or(i64::MIN, Timestamp::unix_nanos),
            Ordering::Release,
        );
        self.health_epoch.store(epoch, Ordering::Release);
        self.live_qualified.store(qualified, Ordering::Release);
    }

    fn is_live_qualified(&self) -> bool {
        self.live_qualified.load(Ordering::Acquire)
    }

    fn validate_health_epoch(&self, epoch: u64, at: Timestamp) -> bool {
        self.is_current()
            && self.is_live_qualified()
            && self.health_epoch.load(Ordering::Acquire) == epoch
            && at.unix_nanos() >= self.valid_from_nanos.load(Ordering::Acquire)
            && at.unix_nanos() <= self.valid_until_nanos.load(Ordering::Acquire)
    }

    fn advance_health_epoch(&self) -> Option<u64> {
        self.health_epoch
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .ok()
            .and_then(|previous| previous.checked_add(1))
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
    authorization: crate::AuthorizationHealth,
    coverage: crate::CoverageHealth,
    budget: CurrentBudgetAuthority,
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

    fn is_available(&self) -> bool {
        match self {
            Self::NotRequired => true,
            Self::Available(lease) => lease.is_available(),
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

    pub(crate) fn is_exactly_empty(&self) -> bool {
        self.sources.is_empty() && self.budget_policies.is_empty()
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
