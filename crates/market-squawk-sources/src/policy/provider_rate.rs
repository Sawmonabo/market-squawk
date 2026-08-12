//! Product-wide durable provider request and connection admission.

use std::sync::Arc;
#[cfg(debug_assertions)]
use std::sync::Mutex;
#[cfg(debug_assertions)]
use std::time::Duration;

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::{AuthorizationMode, AuthorizationSubjectResolutionError, AuthorizationSubjectResolver};

#[cfg(debug_assertions)]
use super::ClockObservation;
use super::{
    BudgetClock, BudgetCollisionKey, BudgetPoolError, BudgetUnavailableReason, EndpointPolicy,
    MonotonicInstant, ProviderBudgetPolicy, ResolvedProviderBudgetPolicy, RetryAfter,
    SharedProviderBudget, SystemBudgetClock,
};

const MAX_RATE_COLLISION_IDENTITIES: usize = 64;

/// Collision namespace for one product-wide provider rate allocation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderRateCollisionKind {
    /// One normalized public network authority.
    PublicNetworkAuthority,
    /// One trusted stable provider-account subject.
    AuthorizationSubject,
}

/// Domain-separated digest of one non-secret provider collision identity.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderRateCollisionIdentity {
    kind: ProviderRateCollisionKind,
    digest: EvidenceDigest,
}

impl ProviderRateCollisionIdentity {
    /// Returns the collision namespace.
    pub const fn kind(self) -> ProviderRateCollisionKind {
        self.kind
    }

    /// Returns the exact domain-separated identity digest.
    pub const fn digest(self) -> EvidenceDigest {
        self.digest
    }
}

/// Canonical provider-rate declaration registered by onboarding or a source runtime.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderRateDeclaration {
    policy: ProviderBudgetPolicy,
    collision_identities: Vec<ProviderRateCollisionIdentity>,
    policy_digest: EvidenceDigest,
    declaration_digest: EvidenceDigest,
}

impl ProviderRateDeclaration {
    /// Derives the conservative governed subject used when a provider has not supplied a verified
    /// immutable account identifier.
    ///
    /// The result is stable across onboarding sessions, credential generations, and process
    /// restarts. It is derived only from the code-owned provider identity; secret material,
    /// verification evidence, and session identifiers are deliberately excluded.
    ///
    /// # Errors
    ///
    /// Returns an allocation failure when the canonical subject cannot be represented.
    pub fn governed_provider_subject(
        provider: &SourceIdentifier,
    ) -> Result<SourceIdentifier, BudgetPoolError> {
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/provider-rate-governed-provider-subject/v1\0");
        let length = u64::try_from(provider.as_str().len())
            .map_err(|_| BudgetPoolError::CoordinatorAllocation)?;
        digest.update(length.to_be_bytes());
        digest.update(provider.as_str().as_bytes());
        let digest: [u8; 32] = digest.finalize().into();
        let mut encoded = String::with_capacity(83);
        encoded.push_str("governed-provider-");
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for byte in digest {
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        SourceIdentifier::try_from(encoded).map_err(|_| BudgetPoolError::CoordinatorAllocation)
    }

    /// Builds a public-interface declaration from normalized endpoint authority.
    ///
    /// # Errors
    ///
    /// Rejects an account-qualified policy, invalid endpoint authority, or an oversized identity
    /// set.
    pub fn try_for_endpoint(
        policy: ProviderBudgetPolicy,
        endpoints: &EndpointPolicy,
    ) -> Result<Self, BudgetPoolError> {
        if policy.scope().authorization_account().is_some() {
            return Err(BudgetPoolError::ConflictingPolicy);
        }
        let authorities = endpoints
            .canonical_network_authorities()
            .map_err(|_| BudgetPoolError::ConflictingPolicy)?;
        let identities = authorities
            .as_slice()
            .iter()
            .map(|authority| {
                collision_identity(
                    ProviderRateCollisionKind::PublicNetworkAuthority,
                    &[
                        authority.host.as_str().as_bytes(),
                        &authority.port.to_be_bytes(),
                    ],
                )
            })
            .collect::<Vec<_>>();
        Self::try_from_identities(policy, identities)
    }

    /// Builds an account-qualified declaration from a trusted stable subject.
    ///
    /// # Errors
    ///
    /// Rejects a public policy. The code-owned account label is replaced by the trusted stable
    /// subject before the declaration is retained.
    pub fn try_for_authorization_subject(
        policy: ProviderBudgetPolicy,
        subject: &SourceIdentifier,
    ) -> Result<Self, BudgetPoolError> {
        let policy = policy
            .with_authorization_subject(subject.clone())
            .map_err(|_| BudgetPoolError::ConflictingPolicy)?;
        Self::try_from_identities(
            policy,
            vec![collision_identity(
                ProviderRateCollisionKind::AuthorizationSubject,
                &[subject.as_str().as_bytes()],
            )],
        )
    }

    pub(in crate::policy) fn from_resolved(
        resolved: &ResolvedProviderBudgetPolicy,
    ) -> Result<Self, BudgetPoolError> {
        let identities = match resolved.collision_key() {
            BudgetCollisionKey::Public(authorities) => authorities
                .iter()
                .map(|authority| {
                    collision_identity(
                        ProviderRateCollisionKind::PublicNetworkAuthority,
                        &[
                            authority.host.as_str().as_bytes(),
                            &authority.port.to_be_bytes(),
                        ],
                    )
                })
                .collect(),
            BudgetCollisionKey::Account(subject) => vec![collision_identity(
                ProviderRateCollisionKind::AuthorizationSubject,
                &[subject.as_str().as_bytes()],
            )],
        };
        Self::try_from_identities(resolved.policy().clone(), identities)
    }

    fn try_from_identities(
        policy: ProviderBudgetPolicy,
        mut collision_identities: Vec<ProviderRateCollisionIdentity>,
    ) -> Result<Self, BudgetPoolError> {
        collision_identities.sort_unstable_by_key(|identity| {
            (
                match identity.kind {
                    ProviderRateCollisionKind::PublicNetworkAuthority => 0_u8,
                    ProviderRateCollisionKind::AuthorizationSubject => 1_u8,
                },
                identity.digest.bytes(),
            )
        });
        collision_identities.dedup();
        if collision_identities.is_empty()
            || collision_identities.len() > MAX_RATE_COLLISION_IDENTITIES
        {
            return Err(BudgetPoolError::CanonicalAuthorityCapacity);
        }
        let policy_digest = Self::policy_digest_for(&policy)?;
        let declaration_digest = digest_serialized(
            b"market-squawk/provider-rate-declaration/v1\0",
            &ProviderRateDeclarationWire {
                policy_digest,
                collision_identities: &collision_identities,
            },
        )?;
        Ok(Self {
            policy,
            collision_identities,
            policy_digest,
            declaration_digest,
        })
    }

    /// Returns the exact typed local enforcement policy.
    pub const fn policy(&self) -> &ProviderBudgetPolicy {
        &self.policy
    }

    /// Returns the bounded canonical collision identities.
    pub fn collision_identities(&self) -> &[ProviderRateCollisionIdentity] {
        &self.collision_identities
    }

    /// Returns the digest of enforcement dimensions, excluding diagnostic scope labels.
    pub const fn policy_digest(&self) -> EvidenceDigest {
        self.policy_digest
    }

    /// Returns the exact declaration digest.
    pub const fn declaration_digest(&self) -> EvidenceDigest {
        self.declaration_digest
    }

    /// Computes the canonical aggregate-limit digest for one validated provider policy.
    ///
    /// Diagnostic provider/account labels are intentionally excluded; request windows,
    /// concurrency, and refusal backoff are the complete enforcement identity.
    ///
    /// # Errors
    ///
    /// Returns an allocation failure when the bounded canonical representation cannot be
    /// serialized.
    pub fn policy_digest_for(
        policy: &ProviderBudgetPolicy,
    ) -> Result<EvidenceDigest, BudgetPoolError> {
        digest_serialized(
            b"market-squawk/provider-rate-limits/v1\0",
            &ProviderRateLimitsWire::from_policy(policy),
        )
    }

    /// Recomputes every canonical digest and structural bound.
    ///
    /// # Errors
    ///
    /// Rejects a declaration whose serialized fields no longer match its identity digests.
    pub fn validate(&self) -> Result<(), BudgetPoolError> {
        let rebuilt =
            Self::try_from_identities(self.policy.clone(), self.collision_identities.clone())?;
        if rebuilt.policy_digest == self.policy_digest
            && rebuilt.declaration_digest == self.declaration_digest
            && rebuilt.collision_identities == self.collision_identities
        {
            Ok(())
        } else {
            Err(BudgetPoolError::CoordinatorCorrupt)
        }
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct ProviderRateLimitsWire {
    windows: Vec<super::ProviderBudgetWindow>,
    max_concurrent: u16,
    backoff: super::BackoffPolicy,
}

impl ProviderRateLimitsWire {
    fn from_policy(policy: &ProviderBudgetPolicy) -> Self {
        Self {
            windows: policy.windows().collect(),
            max_concurrent: policy.max_concurrent(),
            backoff: policy.backoff(),
        }
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct ProviderRateDeclarationWire<'a> {
    policy_digest: EvidenceDigest,
    collision_identities: &'a [ProviderRateCollisionIdentity],
}

macro_rules! opaque_rate_id {
    ($name:ident) => {
        #[doc = "Opaque SQLite authority identity."]
        #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
        pub struct $name([u8; 16]);

        impl $name {
            /// Constructs the opaque identity from durable bytes.
            pub const fn from_bytes(bytes: [u8; 16]) -> Self {
                Self(bytes)
            }

            /// Returns the exact durable bytes.
            pub const fn bytes(self) -> [u8; 16] {
                self.0
            }
        }
    };
}

opaque_rate_id!(ProviderRateRunId);
opaque_rate_id!(ProviderRateGroupId);
opaque_rate_id!(ProviderRatePermitId);

/// Exact registration returned by the durable aggregate authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderRateRegistration {
    group_id: ProviderRateGroupId,
    policy_digest: EvidenceDigest,
    declaration_digest: EvidenceDigest,
}

impl ProviderRateRegistration {
    /// Constructs a verified registration returned by a store implementation.
    pub const fn new(
        group_id: ProviderRateGroupId,
        policy_digest: EvidenceDigest,
        declaration_digest: EvidenceDigest,
    ) -> Self {
        Self {
            group_id,
            policy_digest,
            declaration_digest,
        }
    }

    /// Returns the aggregate group.
    pub const fn group_id(self) -> ProviderRateGroupId {
        self.group_id
    }

    /// Returns the registered limit-policy digest.
    pub const fn policy_digest(self) -> EvidenceDigest {
        self.policy_digest
    }

    /// Returns the registered declaration digest.
    pub const fn declaration_digest(self) -> EvidenceDigest {
        self.declaration_digest
    }
}

/// Durable aggregate request-admission result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderRateDecision {
    /// One concurrency slot and all request windows were atomically charged.
    Ready(ProviderRatePermitId),
    /// No request was charged; retry at or after this wall-clock instant.
    WaitUntil(Timestamp),
    /// No request was charged and progress requires an external state change.
    Unavailable(BudgetUnavailableReason),
}

/// Stable failure classes exposed by a provider-rate store.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ProviderRateStoreError {
    /// Another live product process owns this exact provider-rate data root.
    #[error("provider rate authority is already owned")]
    AlreadyOwned,
    /// SQLite or its controlled path is unavailable.
    #[error("provider rate persistence is unavailable")]
    Unavailable,
    /// Durable state failed structural, digest, or arithmetic validation.
    #[error("provider rate state is corrupt")]
    Corrupt,
    /// A colliding declaration has incompatible policy or bridges independent groups.
    #[error("provider rate declaration conflicts with existing authority")]
    Conflict,
    /// A bounded durable collection reached its configured capacity.
    #[error("provider rate authority capacity is exhausted")]
    Capacity,
    /// Wall-clock observation or conversion failed.
    #[error("provider rate clock is unavailable")]
    Clock,
}

/// Synchronous SQLite-capable persistence boundary used outside the live event-to-action path.
pub trait ProviderRateStore: std::fmt::Debug + Send + Sync {
    /// Starts one process run and reconciles crash-retained permit ownership.
    fn start_run(&self, now: Timestamp) -> Result<ProviderRateRunId, ProviderRateStoreError>;

    /// Idempotently registers one declaration and returns its aggregate collision group.
    fn register(
        &self,
        run_id: ProviderRateRunId,
        declaration: &ProviderRateDeclaration,
        now: Timestamp,
    ) -> Result<ProviderRateRegistration, ProviderRateStoreError>;

    /// Atomically charges every request window and reserves one concurrency slot.
    fn try_acquire(
        &self,
        run_id: ProviderRateRunId,
        registration: ProviderRateRegistration,
        now: Timestamp,
    ) -> Result<ProviderRateDecision, ProviderRateStoreError>;

    /// Releases only the concurrency slot; request-window consumption remains durable.
    fn release(
        &self,
        run_id: ProviderRateRunId,
        registration: ProviderRateRegistration,
        permit_id: ProviderRatePermitId,
    ) -> Result<(), ProviderRateStoreError>;

    /// Applies one standards-parsed provider retry instruction.
    fn apply_retry_after(
        &self,
        run_id: ProviderRateRunId,
        registration: ProviderRateRegistration,
        now: Timestamp,
        retry_after: RetryAfter,
    ) -> Result<ProviderRateDecision, ProviderRateStoreError>;

    /// Applies the registered bounded exponential fallback after a provider refusal.
    fn apply_refusal(
        &self,
        run_id: ProviderRateRunId,
        registration: ProviderRateRegistration,
        now: Timestamp,
        jitter_sample_basis_points: u16,
    ) -> Result<ProviderRateDecision, ProviderRateStoreError>;

    /// Clears shared refusal escalation after a confirmed successful response.
    fn record_success(
        &self,
        run_id: ProviderRateRunId,
        registration: ProviderRateRegistration,
        now: Timestamp,
    ) -> Result<(), ProviderRateStoreError>;

    /// Idempotently binds exact authorization evidence to one trusted stable local subject.
    fn bind_authorization_subject(
        &self,
        run_id: ProviderRateRunId,
        mode: AuthorizationMode,
        evidence: EvidenceDigest,
        subject: &SourceIdentifier,
        now: Timestamp,
    ) -> Result<(), ProviderRateStoreError>;

    /// Resolves an exact durable authorization-evidence binding.
    fn resolve_authorization_subject(
        &self,
        mode: AuthorizationMode,
        evidence: EvidenceDigest,
    ) -> Result<Option<SourceIdentifier>, ProviderRateStoreError>;
}

/// Cloneable capability over one process run in the product-owned provider-rate store.
#[derive(Clone)]
pub struct ProviderRateAuthority {
    inner: Arc<ProviderRateAuthorityInner>,
}

struct ProviderRateAuthorityInner {
    store: Arc<dyn ProviderRateStore>,
    run_id: ProviderRateRunId,
    clock: Arc<dyn BudgetClock>,
    #[cfg(debug_assertions)]
    manual_clock: Option<Arc<ManualProviderRateClock>>,
}

#[cfg(debug_assertions)]
#[derive(Debug)]
struct ManualProviderRateClock {
    observation: Mutex<ClockObservation>,
}

#[cfg(debug_assertions)]
impl ManualProviderRateClock {
    fn new(wall_clock: Timestamp) -> Self {
        Self {
            observation: Mutex::new(ClockObservation::new(
                wall_clock,
                MonotonicInstant::from_nanos(0),
            )),
        }
    }

    fn advance(&self, duration: Duration) -> Result<(), ProviderRateStoreError> {
        let wall_delta =
            i64::try_from(duration.as_nanos()).map_err(|_| ProviderRateStoreError::Clock)?;
        let monotonic_delta =
            u64::try_from(duration.as_nanos()).map_err(|_| ProviderRateStoreError::Clock)?;
        let mut observation = self
            .observation
            .lock()
            .map_err(|_| ProviderRateStoreError::Clock)?;
        let wall_clock = observation
            .wall_clock
            .unix_nanos()
            .checked_add(wall_delta)
            .map(Timestamp::from_unix_nanos)
            .ok_or(ProviderRateStoreError::Clock)?;
        let monotonic = observation
            .monotonic
            .checked_add(monotonic_delta)
            .ok_or(ProviderRateStoreError::Clock)?;
        *observation = ClockObservation::new(wall_clock, monotonic);
        Ok(())
    }
}

#[cfg(debug_assertions)]
impl BudgetClock for ManualProviderRateClock {
    fn observation(&self) -> Result<ClockObservation, BudgetUnavailableReason> {
        self.observation
            .lock()
            .map(|observation| *observation)
            .map_err(|_| BudgetUnavailableReason::ClockUnavailable)
    }

    fn shared_allocation_charge(&self) -> usize {
        std::mem::size_of::<Self>() + crate::conservative_arc_control_block_charge::<Self>()
    }
}

impl std::fmt::Debug for ProviderRateAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProviderRateAuthority")
            .field("run_id", &self.inner.run_id)
            .finish_non_exhaustive()
    }
}

impl ProviderRateAuthority {
    /// Opens the product-wide authority and starts one crash-reconcilable process run.
    ///
    /// # Errors
    ///
    /// Fails closed when durable state cannot be opened or reconciled.
    pub fn try_new(store: Arc<dyn ProviderRateStore>) -> Result<Self, ProviderRateStoreError> {
        let clock: Arc<dyn BudgetClock> = Arc::new(SystemBudgetClock::new());
        #[cfg(debug_assertions)]
        {
            Self::try_new_with_clock(store, clock, None)
        }
        #[cfg(not(debug_assertions))]
        {
            Self::try_new_with_clock(store, clock)
        }
    }

    /// Opens the real durable authority over a manually advanced paired clock for debug fixtures.
    ///
    /// The returned authority still performs normal store ownership, declaration registration,
    /// request-window charging, and local shared-budget admission. Only its wall and monotonic
    /// observation source is controlled. This API is absent from release builds.
    ///
    /// # Errors
    ///
    /// Fails closed when the initial clock observation or durable run admission fails.
    #[cfg(debug_assertions)]
    pub fn try_new_with_debug_manual_clock(
        store: Arc<dyn ProviderRateStore>,
        wall_clock: Timestamp,
    ) -> Result<Self, ProviderRateStoreError> {
        let manual_clock = Arc::new(ManualProviderRateClock::new(wall_clock));
        let clock: Arc<dyn BudgetClock> = manual_clock.clone();
        Self::try_new_with_clock(store, clock, Some(manual_clock))
    }

    /// Advances both paired clock coordinates by exactly the supplied duration.
    ///
    /// No budget or durable state is cleared or rewritten. The next ordinary authority operation
    /// observes the advanced time and must still pass its real admission policy. This API is absent
    /// from release builds.
    ///
    /// # Errors
    ///
    /// Rejects an authority not created by [`Self::try_new_with_debug_manual_clock`], a poisoned
    /// clock, or checked wall/monotonic overflow.
    #[cfg(debug_assertions)]
    pub fn advance_debug_manual_clock(
        &self,
        duration: Duration,
    ) -> Result<(), ProviderRateStoreError> {
        self.inner
            .manual_clock
            .as_ref()
            .ok_or(ProviderRateStoreError::Clock)?
            .advance(duration)
    }

    fn try_new_with_clock(
        store: Arc<dyn ProviderRateStore>,
        clock: Arc<dyn BudgetClock>,
        #[cfg(debug_assertions)] manual_clock: Option<Arc<ManualProviderRateClock>>,
    ) -> Result<Self, ProviderRateStoreError> {
        let now = clock
            .observation()
            .map_err(|_| ProviderRateStoreError::Clock)?
            .wall_clock;
        let run_id = store.start_run(now)?;
        Ok(Self {
            inner: Arc::new(ProviderRateAuthorityInner {
                store,
                run_id,
                clock,
                #[cfg(debug_assertions)]
                manual_clock,
            }),
        })
    }

    /// Registers a declaration and returns a locally enforced budget bound to the aggregate store.
    ///
    /// This performs control-plane SQLite work and must not be called from the live event-to-action
    /// path.
    pub fn register_budget(
        &self,
        declaration: ProviderRateDeclaration,
    ) -> Result<SharedProviderBudget, BudgetPoolError> {
        let binding = self.register_binding(&declaration)?;
        SharedProviderBudget::new_with_provider_rate(declaration.policy, binding)
    }

    /// Binds account-qualified authorization evidence to the stable credential record used for
    /// aggregate provider-rate collision.
    ///
    /// # Errors
    ///
    /// Rejects public/local authorization modes, conflicting bindings, unavailable persistence,
    /// clock rollback, or corrupt durable state.
    pub fn bind_authorization_subject(
        &self,
        mode: AuthorizationMode,
        evidence: EvidenceDigest,
        subject: &SourceIdentifier,
    ) -> Result<(), ProviderRateStoreError> {
        if !matches!(
            mode,
            AuthorizationMode::UserAuthorized | AuthorizationMode::Licensed
        ) {
            return Err(ProviderRateStoreError::Conflict);
        }
        let now = self.wall_clock()?;
        self.inner
            .store
            .bind_authorization_subject(self.inner.run_id, mode, evidence, subject, now)
    }

    pub(in crate::policy) fn register_binding(
        &self,
        declaration: &ProviderRateDeclaration,
    ) -> Result<ProviderRateBinding, BudgetPoolError> {
        let now = self.wall_clock().map_err(map_store_registration_error)?;
        let registration = self
            .inner
            .store
            .register(self.inner.run_id, declaration, now)
            .map_err(map_store_registration_error)?;
        if registration.policy_digest() != declaration.policy_digest()
            || registration.declaration_digest() != declaration.declaration_digest()
        {
            return Err(BudgetPoolError::Persistence);
        }
        Ok(ProviderRateBinding {
            authority: self.clone(),
            registration,
        })
    }

    fn wall_clock(&self) -> Result<Timestamp, ProviderRateStoreError> {
        self.inner
            .clock
            .observation()
            .map(|observation| observation.wall_clock)
            .map_err(|_| ProviderRateStoreError::Clock)
    }
}

impl AuthorizationSubjectResolver for ProviderRateAuthority {
    fn resolve_subject_record(
        &self,
        mode: AuthorizationMode,
        evidence: EvidenceDigest,
    ) -> Result<SourceIdentifier, AuthorizationSubjectResolutionError> {
        if !matches!(
            mode,
            AuthorizationMode::UserAuthorized | AuthorizationMode::Licensed
        ) {
            return Err(AuthorizationSubjectResolutionError::UnsupportedMode);
        }
        self.inner
            .store
            .resolve_authorization_subject(mode, evidence)
            .ok()
            .flatten()
            .ok_or(AuthorizationSubjectResolutionError::EvidenceUnresolved)
    }
}

#[derive(Clone)]
pub(in crate::policy) struct ProviderRateBinding {
    authority: ProviderRateAuthority,
    registration: ProviderRateRegistration,
}

impl std::fmt::Debug for ProviderRateBinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProviderRateBinding")
            .field("registration", &self.registration)
            .finish_non_exhaustive()
    }
}

impl ProviderRateBinding {
    pub(in crate::policy) fn same_group(&self, other: &Self) -> bool {
        self.registration.group_id() == other.registration.group_id()
            && self.registration.policy_digest() == other.registration.policy_digest()
            && Arc::ptr_eq(&self.authority.inner.clock, &other.authority.inner.clock)
    }

    pub(in crate::policy) fn clock(&self) -> Arc<dyn BudgetClock> {
        Arc::clone(&self.authority.inner.clock)
    }

    pub(in crate::policy) fn try_acquire_decision(
        &self,
        now: Timestamp,
    ) -> Result<ProviderRateDecision, BudgetUnavailableReason> {
        self.authority
            .inner
            .store
            .try_acquire(self.authority.inner.run_id, self.registration, now)
            .map_err(map_store_runtime_error)
    }

    pub(in crate::policy) fn apply_retry_after(
        &self,
        now: Timestamp,
        retry_after: RetryAfter,
    ) -> Result<ProviderRateDecision, BudgetUnavailableReason> {
        self.authority
            .inner
            .store
            .apply_retry_after(
                self.authority.inner.run_id,
                self.registration,
                now,
                retry_after,
            )
            .map_err(map_store_runtime_error)
    }

    pub(in crate::policy) fn apply_refusal(
        &self,
        now: Timestamp,
        jitter_sample_basis_points: u16,
    ) -> Result<ProviderRateDecision, BudgetUnavailableReason> {
        self.authority
            .inner
            .store
            .apply_refusal(
                self.authority.inner.run_id,
                self.registration,
                now,
                jitter_sample_basis_points,
            )
            .map_err(map_store_runtime_error)
    }

    pub(in crate::policy) fn record_success(
        &self,
        now: Timestamp,
    ) -> Result<(), BudgetUnavailableReason> {
        self.authority
            .inner
            .store
            .record_success(self.authority.inner.run_id, self.registration, now)
            .map_err(map_store_runtime_error)
    }
}

pub(in crate::policy) struct ProviderRatePermit {
    binding: ProviderRateBinding,
    permit_id: ProviderRatePermitId,
    released: bool,
}

impl std::fmt::Debug for ProviderRatePermit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProviderRatePermit")
            .field("permit_id", &self.permit_id)
            .field("released", &self.released)
            .finish_non_exhaustive()
    }
}

impl ProviderRatePermit {
    pub(in crate::policy) fn new(
        binding: ProviderRateBinding,
        permit_id: ProviderRatePermitId,
    ) -> Self {
        Self {
            binding,
            permit_id,
            released: false,
        }
    }

    pub(in crate::policy) fn release(&mut self) -> Result<(), BudgetUnavailableReason> {
        if self.released {
            return Ok(());
        }
        self.binding
            .authority
            .inner
            .store
            .release(
                self.binding.authority.inner.run_id,
                self.binding.registration,
                self.permit_id,
            )
            .map_err(map_store_runtime_error)?;
        self.released = true;
        Ok(())
    }
}

impl Drop for ProviderRatePermit {
    fn drop(&mut self) {
        let _released = self.release();
    }
}

fn collision_identity(
    kind: ProviderRateCollisionKind,
    components: &[&[u8]],
) -> ProviderRateCollisionIdentity {
    let domain = match kind {
        ProviderRateCollisionKind::PublicNetworkAuthority => {
            b"market-squawk/provider-rate-public-authority/v1\0".as_slice()
        }
        ProviderRateCollisionKind::AuthorizationSubject => {
            b"market-squawk/provider-rate-authorization-subject/v1\0".as_slice()
        }
    };
    let mut digest = Sha256::new();
    digest.update(domain);
    for component in components {
        digest.update((component.len() as u64).to_be_bytes());
        digest.update(component);
    }
    ProviderRateCollisionIdentity {
        kind,
        digest: EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into()),
    }
}

fn digest_serialized<T: Serialize>(
    domain: &[u8],
    value: &T,
) -> Result<EvidenceDigest, BudgetPoolError> {
    let payload = serde_json::to_vec(value).map_err(|_| BudgetPoolError::CoordinatorAllocation)?;
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update(payload);
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn map_store_registration_error(error: ProviderRateStoreError) -> BudgetPoolError {
    match error {
        ProviderRateStoreError::Conflict => BudgetPoolError::ConflictingPolicy,
        ProviderRateStoreError::Capacity => BudgetPoolError::CoordinatorCapacity,
        ProviderRateStoreError::Clock => BudgetPoolError::ClockUnavailable,
        ProviderRateStoreError::AlreadyOwned
        | ProviderRateStoreError::Unavailable
        | ProviderRateStoreError::Corrupt => BudgetPoolError::Persistence,
    }
}

fn map_store_runtime_error(error: ProviderRateStoreError) -> BudgetUnavailableReason {
    match error {
        ProviderRateStoreError::Clock => BudgetUnavailableReason::ClockUnavailable,
        ProviderRateStoreError::Corrupt => BudgetUnavailableReason::StateCorrupt,
        ProviderRateStoreError::AlreadyOwned
        | ProviderRateStoreError::Unavailable
        | ProviderRateStoreError::Conflict
        | ProviderRateStoreError::Capacity => BudgetUnavailableReason::PersistenceUnavailable,
    }
}

pub(in crate::policy) fn wall_deadline_to_monotonic(
    now: Timestamp,
    monotonic: MonotonicInstant,
    deadline: Timestamp,
) -> Result<MonotonicInstant, BudgetUnavailableReason> {
    let remaining = deadline
        .unix_nanos()
        .checked_sub(now.unix_nanos())
        .ok_or(BudgetUnavailableReason::DeadlineOverflow)?;
    if remaining <= 0 {
        return Ok(monotonic);
    }
    monotonic
        .checked_add(remaining.unsigned_abs())
        .ok_or(BudgetUnavailableReason::DeadlineOverflow)
}
