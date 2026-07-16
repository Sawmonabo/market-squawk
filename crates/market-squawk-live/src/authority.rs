//! Single-use current live execution authority.

#![allow(
    dead_code,
    reason = "Task 8 actor wiring is the sole production consumer of crate-private authority gates"
)]

use std::cell::Cell;
use std::marker::PhantomData;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use market_squawk_domain::{
    DataQuality, LiveEvidenceBinding, QualificationAssessmentId, Timestamp,
};
use market_squawk_sources::CurrentSourceAuthorityLease;
use thiserror::Error;

#[path = "authority/lease.rs"]
mod lease;
#[path = "authority/nonce.rs"]
mod nonce;

use lease::LeaseError;
#[allow(
    unused_imports,
    reason = "Task 8 constructs the typed shard/runtime owners in its actor supervisor"
)]
pub(crate) use lease::{
    GenerationLease, GenerationLeaseOwner, RegistryLifecycleLease, RegistryLifecycleOwner,
    RuntimeLease, RuntimeLeaseOwner, ShardLease, ShardLeaseOwner, StatusLease, StatusLeaseOwner,
    StatusRevisionLease, StatusRevisionOwner, StreamRevisionLease, StreamRevisionOwner,
};
use nonce::{NonceError, NonceRegistry, NonceTicket};

/// Opaque, non-serializable, non-cloneable, single-use current execution authority.
///
/// Only the instrument-owned live processor can construct this type. Audit assessments, replay,
/// snapshots, caller-authored quality values, and archived provenance cannot be converted into it.
#[derive(Debug)]
pub struct LiveExecutionCapability {
    source: CurrentSourceAuthorityLease,
    generation: GenerationLease,
    shard: ShardLease,
    runtime: RuntimeLease,
    status: StatusLease,
    revision: StreamRevisionLease,
    expected_revision: u64,
    status_revision: StatusRevisionLease,
    expected_status_revision: u64,
    assessment_id: QualificationAssessmentId,
    binding: LiveEvidenceBinding,
    binding_digest: [u8; 32],
    valid_until: Timestamp,
    monotonic_deadline: Instant,
    ticket: NonceTicket,
    not_sync: PhantomData<Cell<()>>,
}

/// Authority consumed once by risk and moved onward to dispatch validation.
#[derive(Debug)]
pub struct ConsumedLiveAuthority {
    source: CurrentSourceAuthorityLease,
    generation: GenerationLease,
    shard: ShardLease,
    runtime: RuntimeLease,
    status: StatusLease,
    revision: StreamRevisionLease,
    expected_revision: u64,
    status_revision: StatusRevisionLease,
    expected_status_revision: u64,
    assessment_id: QualificationAssessmentId,
    binding: LiveEvidenceBinding,
    valid_until: Timestamp,
    monotonic_deadline: Instant,
    not_sync: PhantomData<Cell<()>>,
}

impl ConsumedLiveAuthority {
    /// Revalidates source, generation, shard, runtime, state revision, and both deadlines.
    ///
    /// Risk and final dispatch call this independently. All revocation loads use Acquire semantics;
    /// expiration is inclusive at both wall and monotonic deadlines.
    ///
    /// # Errors
    ///
    /// Returns a typed fail-closed error after any revocation, replacement, stale revision, clock
    /// failure, or expiry.
    pub fn validate_current(&self) -> Result<(), AuthorityError> {
        self.validate_at(ClockReading::system_now()?)
    }

    /// Returns the exact durable assessment identity explained by this current authority.
    pub const fn assessment_id(&self) -> &QualificationAssessmentId {
        &self.assessment_id
    }

    /// Returns the complete evidence binding fixed at issuance.
    pub const fn binding(&self) -> &LiveEvidenceBinding {
        &self.binding
    }

    fn validate_at(&self, now: ClockReading) -> Result<(), AuthorityError> {
        validate_allocations(
            &self.source,
            &self.generation,
            &self.shard,
            &self.runtime,
            &self.status,
            &self.revision,
            self.expected_revision,
            &self.status_revision,
            self.expected_status_revision,
            self.valid_until,
            self.monotonic_deadline,
            now,
        )
    }

    #[cfg(test)]
    pub(crate) fn validate_at_for_test(&self, now: ClockReading) -> Result<(), AuthorityError> {
        self.validate_at(now)
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ClockReading {
    wall: Timestamp,
    monotonic: Instant,
}

impl ClockReading {
    pub(crate) const fn new(wall: Timestamp, monotonic: Instant) -> Self {
        Self { wall, monotonic }
    }

    pub(crate) const fn wall(self) -> Timestamp {
        self.wall
    }

    pub(crate) const fn monotonic(self) -> Instant {
        self.monotonic
    }

    fn system_now() -> Result<Self, AuthorityError> {
        let system = SystemTime::now();
        let unix_nanos = match system.duration_since(UNIX_EPOCH) {
            Ok(duration) => duration_to_i128(duration),
            Err(error) => -duration_to_i128(error.duration()),
        };
        let unix_nanos = i64::try_from(unix_nanos).map_err(|_| AuthorityError::ClockRange)?;
        Ok(Self {
            wall: Timestamp::from_unix_nanos(unix_nanos),
            monotonic: Instant::now(),
        })
    }
}

fn duration_to_i128(duration: Duration) -> i128 {
    i128::from(duration.as_secs()) * 1_000_000_000 + i128::from(duration.subsec_nanos())
}

mod sealed {
    pub(crate) trait Sealed {}
}

/// Sealed wall-plus-monotonic production clock contract.
pub(crate) trait TrustedClock: sealed::Sealed + std::fmt::Debug {
    fn now(&self) -> Result<ClockReading, AuthorityError>;
}

/// Production clock. Tests use a crate-private deterministic implementation.
#[derive(Debug)]
pub(crate) struct SystemTrustedClock;

impl sealed::Sealed for SystemTrustedClock {}

impl TrustedClock for SystemTrustedClock {
    fn now(&self) -> Result<ClockReading, AuthorityError> {
        ClockReading::system_now()
    }
}

#[cfg(test)]
#[derive(Clone, Debug)]
pub(crate) struct ScriptedTrustedClock {
    readings: std::sync::Arc<[ClockReading]>,
    cursor: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

#[cfg(test)]
impl ScriptedTrustedClock {
    pub(crate) fn try_new(readings: Vec<ClockReading>) -> Result<Self, AuthorityError> {
        if readings.is_empty() {
            return Err(AuthorityError::ClockRange);
        }
        Ok(Self {
            readings: readings.into(),
            cursor: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        })
    }
}

#[cfg(test)]
impl sealed::Sealed for ScriptedTrustedClock {}

#[cfg(test)]
impl TrustedClock for ScriptedTrustedClock {
    fn now(&self) -> Result<ClockReading, AuthorityError> {
        let index = self
            .cursor
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .min(self.readings.len() - 1);
        Ok(self.readings[index])
    }
}

/// Exact current authority produced only after a committed live observation.
#[derive(Debug)]
pub(crate) struct AppliedObservationAuthority {
    pub(crate) source: CurrentSourceAuthorityLease,
    pub(crate) generation: GenerationLease,
    pub(crate) shard: ShardLease,
    pub(crate) runtime: RuntimeLease,
    pub(crate) status: StatusLease,
    pub(crate) revision: StreamRevisionLease,
    pub(crate) expected_revision: u64,
    pub(crate) status_revision: StatusRevisionLease,
    pub(crate) expected_status_revision: u64,
    pub(crate) assessment_id: QualificationAssessmentId,
    pub(crate) binding: LiveEvidenceBinding,
    pub(crate) binding_digest: [u8; 32],
    pub(crate) valid_until: Timestamp,
    pub(crate) monotonic_deadline: Instant,
    pub(crate) quality: DataQuality,
}

impl AppliedObservationAuthority {
    #[allow(
        clippy::too_many_arguments,
        reason = "every independently revocable authority binding is explicit"
    )]
    pub(crate) fn new(
        source: CurrentSourceAuthorityLease,
        generation: GenerationLease,
        shard: ShardLease,
        runtime: RuntimeLease,
        status: StatusLease,
        revision: StreamRevisionLease,
        expected_revision: u64,
        status_revision: StatusRevisionLease,
        expected_status_revision: u64,
        assessment_id: QualificationAssessmentId,
        binding: LiveEvidenceBinding,
        binding_digest: [u8; 32],
        valid_until: Timestamp,
        monotonic_deadline: Instant,
        quality: DataQuality,
    ) -> Self {
        Self {
            source,
            generation,
            shard,
            runtime,
            status,
            revision,
            expected_revision,
            status_revision,
            expected_status_revision,
            assessment_id,
            binding,
            binding_digest,
            valid_until,
            monotonic_deadline,
            quality,
        }
    }
}

/// Instrument-owner-only fixed-capacity authority issuer and consumer.
#[derive(Debug)]
pub(crate) struct AuthorityGate {
    nonces: NonceRegistry,
    reclaim_budget: usize,
}

impl AuthorityGate {
    pub(crate) fn new(capacity: usize, reclaim_budget: usize) -> Result<Self, AuthorityError> {
        if reclaim_budget == 0 {
            return Err(AuthorityError::InvalidReclaimBudget);
        }
        Ok(Self {
            nonces: NonceRegistry::new(capacity)?,
            reclaim_budget,
        })
    }

    pub(crate) fn issue(
        &mut self,
        applied: &AppliedObservationAuthority,
        now: ClockReading,
    ) -> Result<LiveExecutionCapability, AuthorityError> {
        validate_applied(applied, now)?;
        let mono_nanos = monotonic_key(now.monotonic);
        let deadline_nanos = monotonic_key(applied.monotonic_deadline);
        let _ = self.nonces.reclaim(mono_nanos, self.reclaim_budget);
        let ticket = match self.nonces.register(applied.binding_digest, deadline_nanos) {
            Ok(ticket) => ticket,
            Err(error @ (NonceError::NonceExhausted | NonceError::SlotEpochExhausted)) => {
                applied.generation.invalidate();
                return Err(error.into());
            }
            Err(error) => return Err(error.into()),
        };
        if let Err(error) = validate_applied(applied, now) {
            let _ = self.nonces.retire(&ticket);
            return Err(error);
        }
        Ok(LiveExecutionCapability {
            source: applied.source.clone(),
            generation: applied.generation.clone(),
            shard: applied.shard.clone(),
            runtime: applied.runtime.clone(),
            status: applied.status.clone(),
            revision: applied.revision.clone(),
            expected_revision: applied.expected_revision,
            status_revision: applied.status_revision.clone(),
            expected_status_revision: applied.expected_status_revision,
            assessment_id: applied.assessment_id.clone(),
            binding: applied.binding.clone(),
            binding_digest: applied.binding_digest,
            valid_until: applied.valid_until,
            monotonic_deadline: applied.monotonic_deadline,
            ticket,
            not_sync: PhantomData,
        })
    }

    pub(crate) fn validate_applied_current(
        &self,
        applied: &AppliedObservationAuthority,
        now: ClockReading,
    ) -> Result<(), AuthorityError> {
        validate_applied(applied, now)
    }

    pub(crate) fn consume(
        &mut self,
        capability: LiveExecutionCapability,
        now: ClockReading,
    ) -> Result<ConsumedLiveAuthority, AuthorityError> {
        let LiveExecutionCapability {
            source,
            generation,
            shard,
            runtime,
            status,
            revision,
            expected_revision,
            status_revision,
            expected_status_revision,
            assessment_id,
            binding,
            binding_digest,
            valid_until,
            monotonic_deadline,
            ticket,
            not_sync: _,
        } = capability;
        if let Err(error) = validate_allocations(
            &source,
            &generation,
            &shard,
            &runtime,
            &status,
            &revision,
            expected_revision,
            &status_revision,
            expected_status_revision,
            valid_until,
            monotonic_deadline,
            now,
        ) {
            let _ = self.nonces.retire(&ticket);
            return Err(error);
        }
        self.nonces
            .consume(&ticket, binding_digest, monotonic_key(now.monotonic))?;
        validate_allocations(
            &source,
            &generation,
            &shard,
            &runtime,
            &status,
            &revision,
            expected_revision,
            &status_revision,
            expected_status_revision,
            valid_until,
            monotonic_deadline,
            now,
        )?;
        Ok(ConsumedLiveAuthority {
            source,
            generation,
            shard,
            runtime,
            status,
            revision,
            expected_revision,
            status_revision,
            expected_status_revision,
            assessment_id,
            binding,
            valid_until,
            monotonic_deadline,
            not_sync: PhantomData,
        })
    }
}

fn validate_applied(
    applied: &AppliedObservationAuthority,
    now: ClockReading,
) -> Result<(), AuthorityError> {
    if applied.quality != DataQuality::DirectVerified {
        return Err(AuthorityError::QualityNotDirectVerified);
    }
    validate_allocations(
        &applied.source,
        &applied.generation,
        &applied.shard,
        &applied.runtime,
        &applied.status,
        &applied.revision,
        applied.expected_revision,
        &applied.status_revision,
        applied.expected_status_revision,
        applied.valid_until,
        applied.monotonic_deadline,
        now,
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "each independently revocable authority dimension is explicit"
)]
fn validate_allocations(
    source: &CurrentSourceAuthorityLease,
    generation: &GenerationLease,
    shard: &ShardLease,
    runtime: &RuntimeLease,
    status: &StatusLease,
    revision: &StreamRevisionLease,
    expected_revision: u64,
    status_revision: &StatusRevisionLease,
    expected_status_revision: u64,
    valid_until: Timestamp,
    monotonic_deadline: Instant,
    now: ClockReading,
) -> Result<(), AuthorityError> {
    source
        .validate_at(now.wall)
        .map_err(|_| AuthorityError::SourceRevoked)?;
    generation.validate().map_err(AuthorityError::from)?;
    shard.validate().map_err(AuthorityError::from)?;
    runtime.validate().map_err(AuthorityError::from)?;
    status.validate().map_err(AuthorityError::from)?;
    revision
        .validate(expected_revision)
        .map_err(AuthorityError::from)?;
    status_revision
        .validate(expected_status_revision)
        .map_err(AuthorityError::from)?;
    if now.wall > valid_until || now.monotonic > monotonic_deadline {
        return Err(AuthorityError::Expired);
    }
    Ok(())
}

fn monotonic_key(instant: Instant) -> u64 {
    // `Instant` has no portable epoch. The exact value never leaves this process; duration from a
    // lazily initialized process-local origin is sufficient for ordering within one nonce registry.
    use std::sync::OnceLock;
    static ORIGIN: OnceLock<Instant> = OnceLock::new();
    let origin = *ORIGIN.get_or_init(Instant::now);
    let duration = instant
        .checked_duration_since(origin)
        .unwrap_or(Duration::ZERO);
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

/// Current live authority issuance or validation failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum AuthorityError {
    /// Stateful assessment quality was not directly verified.
    #[error("current observation is not direct verified")]
    QualityNotDirectVerified,
    /// Task 5 source/current-health/capture authority is no longer current.
    #[error("source authority is revoked or expired")]
    SourceRevoked,
    /// A one-way generation, shard, runtime, or revision allocation was revoked.
    #[error("live authority allocation is revoked")]
    Revoked,
    /// The instrument state advanced after authority issuance.
    #[error("live authority state revision is stale: expected {expected}, current {current}")]
    StaleRevision { expected: u64, current: u64 },
    /// The checked state revision counter exhausted and was invalidated.
    #[error("live authority state revision exhausted")]
    RevisionExhausted,
    /// Nonce registry capacity is exhausted until bounded reclamation progresses.
    #[error("live authority nonce capacity is exhausted")]
    NonceCapacityExhausted,
    /// Global nonce identity exhausted and the generation was invalidated.
    #[error("live authority nonce identity exhausted")]
    NonceExhausted,
    /// Per-slot epoch exhausted and the generation was invalidated.
    #[error("live authority nonce slot epoch exhausted")]
    NonceSlotEpochExhausted,
    /// Nonce registry configuration or allocation failed.
    #[error("live authority nonce registry initialization failed")]
    NonceRegistryInitialization,
    /// Nonce lookup did not identify the exact current issued capability.
    #[error("live authority nonce is stale")]
    StaleNonce,
    /// The nonce was already consumed or retired.
    #[error("live authority nonce was already consumed")]
    NonceAlreadyConsumed,
    /// The nonce binding differs from the capability binding.
    #[error("live authority nonce binding does not match")]
    NonceBindingMismatch,
    /// The nonce expired before consumption.
    #[error("live authority nonce expired")]
    NonceExpired,
    /// Internal fixed-capacity nonce state violated an invariant.
    #[error("live authority nonce registry invariant failed")]
    NonceInvariant,
    /// Wall or monotonic policy deadline passed.
    #[error("live authority expired")]
    Expired,
    /// System wall time cannot be represented by the domain timestamp.
    #[error("trusted system clock is outside supported timestamp range")]
    ClockRange,
    /// Incremental reclamation must make bounded positive progress.
    #[error("nonce reclaim budget must be positive")]
    InvalidReclaimBudget,
}

impl From<LeaseError> for AuthorityError {
    fn from(value: LeaseError) -> Self {
        match value {
            LeaseError::Revoked => Self::Revoked,
            LeaseError::StaleRevision { expected, current } => {
                Self::StaleRevision { expected, current }
            }
            LeaseError::RevisionExhausted => Self::RevisionExhausted,
        }
    }
}

impl From<NonceError> for AuthorityError {
    fn from(value: NonceError) -> Self {
        match value {
            NonceError::InvalidCapacity { .. } | NonceError::Allocation => {
                Self::NonceRegistryInitialization
            }
            NonceError::CapacityExhausted => Self::NonceCapacityExhausted,
            NonceError::NonceExhausted => Self::NonceExhausted,
            NonceError::SlotEpochExhausted => Self::NonceSlotEpochExhausted,
            NonceError::RegistryInvariant => Self::NonceInvariant,
            NonceError::StaleTicket => Self::StaleNonce,
            NonceError::BindingMismatch => Self::NonceBindingMismatch,
            NonceError::AlreadyConsumed => Self::NonceAlreadyConsumed,
            NonceError::Expired => Self::NonceExpired,
        }
    }
}

#[cfg(test)]
mod tests {
    use static_assertions::assert_type_ne_all;

    use super::lease::{
        GenerationLease, GenerationLeaseOwner, LeaseError, RuntimeLease, ShardLease, StatusLease,
        StatusRevisionLease, StreamRevisionLease, StreamRevisionOwner,
    };
    use super::nonce::{NonceError, NonceRegistry};

    assert_type_ne_all!(GenerationLease, ShardLease, RuntimeLease, StatusLease);
    assert_type_ne_all!(StreamRevisionLease, StatusRevisionLease);

    #[test]
    fn one_way_leases_never_reactivate_and_revision_overflow_invalidates() {
        let mut owner = GenerationLeaseOwner::new(7);
        let lease = owner.lease();
        assert!(lease.validate().is_ok());
        owner.invalidate();
        owner.invalidate();
        assert_eq!(lease.validate(), Err(LeaseError::Revoked));

        let mut revision = StreamRevisionOwner::new_for_test(u64::MAX);
        let revision_lease = revision.lease();
        assert_eq!(revision.advance(), Err(LeaseError::RevisionExhausted));
        assert_eq!(revision_lease.validate(u64::MAX), Err(LeaseError::Revoked));
    }

    #[test]
    fn dropping_an_owner_release_invalidates_every_retained_lease() {
        let lease = {
            let owner = GenerationLeaseOwner::new(11);
            owner.lease()
        };

        assert_eq!(lease.validate(), Err(LeaseError::Revoked));
    }

    #[test]
    fn nonce_registry_is_fixed_capacity_single_use_and_epoch_bound()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut registry = NonceRegistry::new(2)?;
        let first = registry.register([1; 32], 100)?;
        let second = registry.register([2; 32], 100)?;
        assert_eq!(
            registry.register([3; 32], 100),
            Err(NonceError::CapacityExhausted)
        );

        registry.consume(&first, [1; 32], 100)?;
        assert_eq!(
            registry.consume(&first, [1; 32], 100),
            Err(NonceError::AlreadyConsumed)
        );
        assert_eq!(
            registry.consume(&second, [9; 32], 100),
            Err(NonceError::BindingMismatch)
        );
        Ok(())
    }

    #[test]
    fn bounded_reclamation_never_scans_the_registry_on_registration()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut registry = NonceRegistry::new(4)?;
        let first = registry.register([1; 32], 5)?;
        let _second = registry.register([2; 32], 10)?;
        registry.retire(&first)?;

        assert_eq!(registry.reclaim(5, 1), 1);
        assert!(registry.register([3; 32], 20).is_ok());
        assert!(registry.last_reclaim_scan_count() <= 1);
        Ok(())
    }

    #[test]
    fn nonce_and_slot_epoch_overflow_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
        let mut nonce_overflow = NonceRegistry::new_for_test(1, u64::MAX, 0)?;
        assert_eq!(
            nonce_overflow.register([1; 32], 1),
            Err(NonceError::NonceExhausted)
        );

        let mut epoch_overflow = NonceRegistry::new_for_test(1, 0, u64::MAX)?;
        assert_eq!(
            epoch_overflow.register([1; 32], 1),
            Err(NonceError::SlotEpochExhausted)
        );
        Ok(())
    }
}
