//! Stateful source registration and current-session authority handles.

use std::cell::Cell;
use std::collections::{BTreeMap, HashMap};
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use market_squawk_domain::SchemaVersion;
use market_squawk_domain::{
    ConnectionGeneration, CoverageConsolidation, CoverageDelay, DeliveryEvidence,
    EffectiveInterval, ExactPayloadEvidence, InstrumentId, LiveEventClass, MarketDepth,
    MetadataRevision, ProviderProduct, SourceId, Timestamp, VenueId,
};
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

use crate::ProviderBudgetPolicy;
use crate::bounded::BoundedVec;
use crate::policy::{BudgetAvailabilityLease, ProviderBudgetPool};
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
    live_qualified: AtomicBool,
    health_epoch: AtomicU64,
    valid_from_nanos: AtomicI64,
    valid_until_nanos: AtomicI64,
    last_health_observed_nanos: AtomicI64,
    frame_ordinal: AtomicU64,
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
    }

    fn next_health_epoch(&self) -> Option<u64> {
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
}

#[derive(Clone, Debug)]
struct CurrentHealthAuthority {
    epoch: u64,
    observed_at: Timestamp,
    trusted_reported_at: TrustedRegistryTime,
    accepted_at: TrustedRegistryTime,
    valid_from: Timestamp,
    valid_until: Timestamp,
    valid_until_monotonic: Instant,
    authorization: crate::AuthorizationHealth,
    coverage: crate::CoverageHealth,
    budget: CurrentBudgetAuthority,
}

#[derive(Clone, Copy, Debug)]
struct TrustedRegistryTime {
    wall: Timestamp,
    monotonic: Instant,
}

impl TrustedRegistryTime {
    const fn new(wall: Timestamp, monotonic: Instant) -> Self {
        Self { wall, monotonic }
    }

    const fn wall(self) -> Timestamp {
        self.wall
    }

    const fn monotonic(self) -> Instant {
        self.monotonic
    }

    fn checked_deadline(self, until: Timestamp) -> Result<Option<Instant>, RegistryError> {
        if until < self.wall {
            return Ok(None);
        }
        let delta = until
            .unix_nanos()
            .checked_sub(self.wall.unix_nanos())
            .ok_or(RegistryError::HealthDeadlineOverflow)?;
        let nanos = u64::try_from(delta).map_err(|_| RegistryError::HealthDeadlineOverflow)?;
        self.monotonic
            .checked_add(Duration::from_nanos(nanos))
            .map(Some)
            .ok_or(RegistryError::HealthDeadlineOverflow)
    }
}

trait RegistryClock: Send + Sync + std::fmt::Debug {
    fn observe(&self) -> Result<TrustedRegistryTime, RegistryError>;

    fn shared_allocation_charge(&self) -> usize;
}

#[derive(Debug)]
struct SystemRegistryClock;

impl SystemRegistryClock {
    fn try_new() -> Result<Self, RegistryError> {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| RegistryError::TrustedClockUnavailable)?;
        let nanos = i64::try_from(duration.as_nanos())
            .map_err(|_| RegistryError::TrustedClockUnavailable)?;
        let _representable_wall_origin = Timestamp::from_unix_nanos(nanos);
        Ok(Self)
    }
}

impl RegistryClock for SystemRegistryClock {
    fn observe(&self) -> Result<TrustedRegistryTime, RegistryError> {
        let wall_duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| RegistryError::TrustedClockUnavailable)?;
        let wall_nanos = i64::try_from(wall_duration.as_nanos())
            .map_err(|_| RegistryError::TrustedClockUnavailable)?;
        Ok(TrustedRegistryTime::new(
            Timestamp::from_unix_nanos(wall_nanos),
            Instant::now(),
        ))
    }

    fn shared_allocation_charge(&self) -> usize {
        std::mem::size_of::<Self>() + crate::conservative_arc_control_block_charge::<Self>()
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
    active: Option<ActiveSessionKey>,
    health_authority: Option<CurrentHealthAuthority>,
    universe_attestation: Option<InstrumentUniverseAttestation>,
    generation_high_water: Option<ConnectionGeneration>,
    used_revisions: Vec<MetadataRevision>,
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
    last_epoch: u64,
    generation_high_water: Option<ConnectionGeneration>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedSourceAuthority {
    source_id: SourceId,
    used_revisions: BoundedVec<MetadataRevision, MAX_REVISIONS_PER_SOURCE>,
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
    budget_policies: BoundedVec<ProviderBudgetPolicy, MAX_BUDGET_SCOPES>,
}

impl RegistryAuthorityState {
    fn empty() -> Self {
        Self {
            schema_version: SchemaVersion::CURRENT,
            sources: BoundedVec::empty(),
            budget_policies: BoundedVec::empty(),
        }
    }

    fn try_new(
        sources: Vec<PersistedSourceAuthority>,
        budget_policies: Vec<ProviderBudgetPolicy>,
    ) -> Result<Self, RegistryError> {
        if sources.iter().any(|source| {
            source.last_epoch == 0
                || source.used_revisions.is_empty()
                || contains_duplicate_revisions(source.used_revisions.as_slice())
        }) || sources.iter().enumerate().any(|(index, source)| {
            sources[index.saturating_add(1)..]
                .iter()
                .any(|other| source.source_id == other.source_id)
        }) || budget_policies.iter().enumerate().any(|(index, policy)| {
            budget_policies[index.saturating_add(1)..]
                .iter()
                .any(|other| policy.scope() == other.scope())
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
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RegistryAuthorityStateWire {
    schema_version: SchemaVersion,
    sources: BoundedVec<PersistedSourceAuthority, MAX_AUTHORITY_SOURCES>,
    budget_policies: BoundedVec<ProviderBudgetPolicy, MAX_BUDGET_SCOPES>,
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
include!("registry/authority.rs");
include!("registry/current_batch.rs");
#[cfg(test)]
#[path = "registry/test_support.rs"]
mod test_support;
include!("registry/tests.rs");
