//! Product-wide durable provider request and connection admission.

use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};
#[cfg(debug_assertions)]
use std::time::Duration;

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::{AuthorizationMode, AuthorizationSubjectResolutionError, AuthorizationSubjectResolver};
use crate::{
    ProviderRateDispatchClaim, ProviderRateResponseSettlement,
    ProviderRateResponseSettlementReceipt,
};

use super::{
    BudgetClock, BudgetCollisionKey, BudgetPoolError, BudgetUnavailableReason, ClockObservation,
    EndpointPolicy, MonotonicInstant, ProviderBudgetPolicy, ResolvedProviderBudgetPolicy,
    RetryAfter, SharedProviderBudget, SystemBudgetClock,
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
            b"market-squawk/provider-rate-declaration/v2\0",
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
    /// Diagnostic provider/account labels are intentionally excluded. Request windows, weighted
    /// response windows, concurrency, refusal backoff, and the dispatch/terminalization semantic
    /// versions form the complete enforcement identity.
    ///
    /// # Errors
    ///
    /// Returns an allocation failure when the bounded canonical representation cannot be
    /// serialized.
    pub fn policy_digest_for(
        policy: &ProviderBudgetPolicy,
    ) -> Result<EvidenceDigest, BudgetPoolError> {
        digest_serialized(
            b"market-squawk/provider-rate-limits/v2\0",
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
    weighted_windows: Vec<crate::ProviderRateWeightedWindow>,
    max_concurrent: u16,
    backoff: super::BackoffPolicy,
    dispatch_claim_semantics_version: u16,
    response_terminalization_semantics_version: u16,
}

impl ProviderRateLimitsWire {
    fn from_policy(policy: &ProviderBudgetPolicy) -> Self {
        Self {
            windows: policy.windows().collect(),
            weighted_windows: policy.weighted_windows().collect(),
            max_concurrent: policy.max_concurrent(),
            backoff: policy.backoff(),
            dispatch_claim_semantics_version: 1,
            response_terminalization_semantics_version: 1,
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
opaque_rate_id!(ProviderRateReservationId);
opaque_rate_id!(ProviderRatePermitId);

/// Exact registration returned by the durable aggregate authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderRateRegistration {
    group_id: ProviderRateGroupId,
    policy_digest: EvidenceDigest,
    declaration_digest: EvidenceDigest,
}

/// Stable key for opaque provider-specific control state retained by the shared rate authority.
///
/// The key is derived from one exact generic provider-rate declaration. Account-qualified
/// declarations use their trusted stable authorization subject; public declarations use the
/// code-owned governed-provider subject. The extension and schema identities are code-owned and
/// keep unrelated provider state from sharing one durable row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderRateExtensionKey {
    provider_subject: SourceIdentifier,
    extension_id: SourceIdentifier,
    schema_id: SourceIdentifier,
    policy_digest: EvidenceDigest,
    declaration_digest: EvidenceDigest,
}

impl ProviderRateExtensionKey {
    /// Binds one provider-specific state schema to an exact validated generic declaration.
    ///
    /// # Errors
    ///
    /// Rejects an invalid declaration or an unrepresentable governed public-provider subject.
    pub fn try_from_declaration(
        declaration: &ProviderRateDeclaration,
        extension_id: SourceIdentifier,
        schema_id: SourceIdentifier,
    ) -> Result<Self, BudgetPoolError> {
        declaration.validate()?;
        let provider_subject = declaration
            .policy()
            .scope()
            .authorization_account()
            .cloned()
            .map_or_else(
                || {
                    ProviderRateDeclaration::governed_provider_subject(
                        declaration.policy().scope().as_source_identifier(),
                    )
                },
                Ok,
            )?;
        Ok(Self {
            provider_subject,
            extension_id,
            schema_id,
            policy_digest: declaration.policy_digest(),
            declaration_digest: declaration.declaration_digest(),
        })
    }

    /// Returns the stable provider/account subject retained in the durable key.
    pub const fn provider_subject(&self) -> &SourceIdentifier {
        &self.provider_subject
    }

    /// Returns the code-owned provider extension identity.
    pub const fn extension_id(&self) -> &SourceIdentifier {
        &self.extension_id
    }

    /// Returns the exact extension-state schema identity.
    pub const fn schema_id(&self) -> &SourceIdentifier {
        &self.schema_id
    }

    /// Returns the exact generic limit policy bound to this extension.
    pub const fn policy_digest(&self) -> EvidenceDigest {
        self.policy_digest
    }

    /// Returns the exact registered generic declaration bound to this extension.
    pub const fn declaration_digest(&self) -> EvidenceDigest {
        self.declaration_digest
    }
}

/// Exact predecessor identity used for a provider-extension compare-and-exchange transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderRateExtensionRevision {
    version: NonZeroU64,
    digest: EvidenceDigest,
}

impl ProviderRateExtensionRevision {
    /// Constructs one store-verified opaque-state revision.
    pub const fn new(version: NonZeroU64, digest: EvidenceDigest) -> Self {
        Self { version, digest }
    }

    /// Returns the strictly increasing row version.
    pub const fn version(self) -> NonZeroU64 {
        self.version
    }

    /// Returns the domain-separated digest of the exact opaque state bytes and durable key.
    pub const fn digest(self) -> EvidenceDigest {
        self.digest
    }
}

/// Bounded opaque provider-extension state returned by the shared durable authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderRateExtensionState {
    key: ProviderRateExtensionKey,
    revision: ProviderRateExtensionRevision,
    bytes: Box<[u8]>,
    updated_at: Timestamp,
}

impl ProviderRateExtensionState {
    /// Maximum opaque payload retained in one provider-extension row.
    pub const MAXIMUM_BYTES: usize = 1024 * 1024;

    /// Constructs state after a store has verified its exact row and digest.
    pub fn from_verified_store(
        key: ProviderRateExtensionKey,
        revision: ProviderRateExtensionRevision,
        bytes: Box<[u8]>,
        updated_at: Timestamp,
    ) -> Self {
        Self {
            key,
            revision,
            bytes,
            updated_at,
        }
    }

    /// Returns the exact durable extension key.
    pub const fn key(&self) -> &ProviderRateExtensionKey {
        &self.key
    }

    /// Returns the compare-and-exchange predecessor identity.
    pub const fn revision(&self) -> ProviderRateExtensionRevision {
        self.revision
    }

    /// Returns the exact bounded opaque state bytes.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns when this state transition committed in the shared authority clock.
    pub const fn updated_at(&self) -> Timestamp {
        self.updated_at
    }
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
pub enum ProviderRateReservationDecision {
    /// One concurrency slot was reserved; request windows remain uncharged until dispatch.
    Ready(ProviderRateReservationId),
    /// No request was charged; retry at or after this wall-clock instant.
    WaitUntil(Timestamp),
    /// No request was charged and progress requires an external state change.
    Unavailable(BudgetUnavailableReason),
}

/// Durable aggregate dispatch result for one previously reserved request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderRateDispatchDecision {
    /// Every request window was atomically charged at the dispatch boundary.
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

/// Retained writer transaction for one fully validated provider-rate registration batch.
///
/// Implementations must keep every staged group and declaration invisible until [`Self::commit`]
/// succeeds. Dropping the capability must roll the entire batch back. The registrations are
/// exposed before commit only so the control plane can construct non-escaping local allocations;
/// they do not authorize request admission until this capability has committed.
pub trait PreparedProviderRateRegistrationBatch: std::fmt::Debug {
    /// Returns one exact registration for each declaration supplied to the prepare call, in the
    /// same order.
    fn registrations(&self) -> &[ProviderRateRegistration];

    /// Atomically publishes every staged group and declaration.
    fn commit(self: Box<Self>) -> Result<(), ProviderRateStoreError>;
}

/// Synchronous SQLite-capable persistence boundary used outside the live event-to-action path.
pub trait ProviderRateStore: std::fmt::Debug + Send + Sync {
    /// Starts one process run and reconciles crash-retained permit ownership.
    fn start_run(&self, now: Timestamp) -> Result<ProviderRateRunId, ProviderRateStoreError>;

    /// Stages a bounded declaration batch under one retained writer transaction.
    ///
    /// The implementation must validate the complete batch, including declarations staged
    /// earlier in the same batch, before returning. No staged row may be visible unless the
    /// returned capability is committed; dropping it must roll the transaction back.
    fn prepare_registration_batch(
        &self,
        run_id: ProviderRateRunId,
        declarations: &[ProviderRateDeclaration],
        now: Timestamp,
    ) -> Result<Box<dyn PreparedProviderRateRegistrationBatch>, ProviderRateStoreError>;

    /// Idempotently registers one declaration and returns its aggregate collision group.
    fn register(
        &self,
        run_id: ProviderRateRunId,
        declaration: &ProviderRateDeclaration,
        now: Timestamp,
    ) -> Result<ProviderRateRegistration, ProviderRateStoreError> {
        let prepared =
            self.prepare_registration_batch(run_id, std::slice::from_ref(declaration), now)?;
        let registration = prepared
            .registrations()
            .first()
            .copied()
            .ok_or(ProviderRateStoreError::Corrupt)?;
        if prepared.registrations().len() != 1
            || registration.policy_digest() != declaration.policy_digest()
            || registration.declaration_digest() != declaration.declaration_digest()
        {
            return Err(ProviderRateStoreError::Corrupt);
        }
        prepared.commit()?;
        Ok(registration)
    }

    /// Reserves one concurrency slot without charging any request window.
    fn try_reserve(
        &self,
        run_id: ProviderRateRunId,
        registration: ProviderRateRegistration,
        now: Timestamp,
    ) -> Result<ProviderRateReservationDecision, ProviderRateStoreError>;

    /// Atomically charges every request window for one exact reserved request at dispatch.
    fn commit_dispatch(
        &self,
        run_id: ProviderRateRunId,
        registration: ProviderRateRegistration,
        reservation_id: ProviderRateReservationId,
        now: Timestamp,
    ) -> Result<ProviderRateDispatchDecision, ProviderRateStoreError>;

    /// Atomically charges request windows and reserves the exact worst-case weighted response
    /// claim for one reserved request at dispatch.
    ///
    /// The default preserves request-only stores without claiming weighted support. A nonempty
    /// claim fails closed until the durable store implements weighted dispatch.
    fn commit_dispatch_with_claim(
        &self,
        run_id: ProviderRateRunId,
        registration: ProviderRateRegistration,
        reservation_id: ProviderRateReservationId,
        now: Timestamp,
        claim: ProviderRateDispatchClaim,
    ) -> Result<ProviderRateDispatchDecision, ProviderRateStoreError> {
        if claim.is_request_only() {
            self.commit_dispatch(run_id, registration, reservation_id, now)
        } else {
            Err(ProviderRateStoreError::Conflict)
        }
    }

    /// Cancels only an undispatched concurrency reservation without charging a request window.
    fn cancel_reservation(
        &self,
        run_id: ProviderRateRunId,
        registration: ProviderRateRegistration,
        reservation_id: ProviderRateReservationId,
    ) -> Result<(), ProviderRateStoreError>;

    /// Releases an in-flight permit whose response was not explicitly terminalized.
    ///
    /// Request-window consumption remains durable. A weighted implementation must conservatively
    /// replace every pending response claim with its maximum byte/error charge before releasing
    /// concurrency; a request-only implementation releases only concurrency.
    fn release(
        &self,
        run_id: ProviderRateRunId,
        registration: ProviderRateRegistration,
        permit_id: ProviderRatePermitId,
    ) -> Result<(), ProviderRateStoreError>;

    /// Atomically terminalizes one exact dispatched response, replaces its pending maximum claim
    /// with the derived exact or conservative units, applies refusal state, removes permit
    /// ownership, and releases concurrency.
    ///
    /// Request-only stores fail closed by default and therefore cannot return a false weighted
    /// settlement receipt.
    fn settle_response(
        &self,
        _run_id: ProviderRateRunId,
        _registration: ProviderRateRegistration,
        _permit_id: ProviderRatePermitId,
        _now: Timestamp,
        _settlement: ProviderRateResponseSettlement,
    ) -> Result<ProviderRateResponseSettlementReceipt, ProviderRateStoreError> {
        Err(ProviderRateStoreError::Conflict)
    }

    /// Applies one standards-parsed provider retry instruction.
    fn apply_retry_after(
        &self,
        run_id: ProviderRateRunId,
        registration: ProviderRateRegistration,
        now: Timestamp,
        retry_after: RetryAfter,
    ) -> Result<ProviderRateReservationDecision, ProviderRateStoreError>;

    /// Applies the registered bounded exponential fallback after a provider refusal.
    fn apply_refusal(
        &self,
        run_id: ProviderRateRunId,
        registration: ProviderRateRegistration,
        now: Timestamp,
        jitter_sample_basis_points: u16,
    ) -> Result<ProviderRateReservationDecision, ProviderRateStoreError>;

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

    /// Loads one exact provider-specific extension state after validating its generic declaration.
    fn load_extension(
        &self,
        _run_id: ProviderRateRunId,
        _key: &ProviderRateExtensionKey,
        _now: Timestamp,
    ) -> Result<Option<ProviderRateExtensionState>, ProviderRateStoreError> {
        Err(ProviderRateStoreError::Conflict)
    }

    /// Atomically creates or replaces one bounded opaque extension state.
    ///
    /// `None` is the only valid predecessor for initial creation. A replacement requires the
    /// exact current version and digest. Stale, missing, or unexpected predecessors fail closed.
    fn compare_exchange_extension(
        &self,
        _run_id: ProviderRateRunId,
        _key: &ProviderRateExtensionKey,
        _expected: Option<ProviderRateExtensionRevision>,
        _replacement: &[u8],
        _now: Timestamp,
    ) -> Result<ProviderRateExtensionState, ProviderRateStoreError> {
        Err(ProviderRateStoreError::Conflict)
    }
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
    operation_gate: Mutex<()>,
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
        let _operation = self
            .inner
            .operation_gate
            .lock()
            .map_err(|_| ProviderRateStoreError::Corrupt)?;
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
                operation_gate: Mutex::new(()),
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
        self.serialized_timed_store_operation(|store, run_id, now| {
            store.bind_authorization_subject(run_id, mode, evidence, subject, now)
        })
        .map(|(_observation, ())| ())
    }

    /// Loads one provider-specific state row through the same serialized SQLite authority.
    ///
    /// # Errors
    ///
    /// Fails closed for an unregistered or mismatched declaration, corrupt state, capacity, or
    /// unavailable authority clock/storage.
    pub fn load_extension(
        &self,
        key: &ProviderRateExtensionKey,
    ) -> Result<Option<ProviderRateExtensionState>, ProviderRateStoreError> {
        self.serialized_timed_store_operation(|store, run_id, now| {
            store.load_extension(run_id, key, now)
        })
        .map(|(_observation, state)| state)
    }

    /// Returns the wall-clock coordinate used by serialized provider-extension transitions.
    ///
    /// This preserves the paired manual clock used by debug authority fixtures and avoids a
    /// provider-specific second clock. The value is an observation, not a state mutation.
    ///
    /// # Errors
    ///
    /// Fails closed when the shared operation gate or authority clock is unavailable.
    pub fn extension_clock_timestamp(&self) -> Result<Timestamp, ProviderRateStoreError> {
        self.clock_observation()
            .map(|observation| observation.wall_clock)
    }

    /// Performs one serialized durable compare-and-exchange of bounded opaque provider state.
    ///
    /// # Errors
    ///
    /// Fails closed when the exact predecessor no longer matches or any declaration, schema,
    /// digest, size, clock, persistence, or integrity invariant fails.
    pub fn compare_exchange_extension(
        &self,
        key: &ProviderRateExtensionKey,
        expected: Option<ProviderRateExtensionRevision>,
        replacement: &[u8],
    ) -> Result<ProviderRateExtensionState, ProviderRateStoreError> {
        self.serialized_timed_store_operation(|store, run_id, now| {
            store.compare_exchange_extension(run_id, key, expected, replacement, now)
        })
        .map(|(_observation, state)| state)
    }

    pub(in crate::policy) fn register_binding(
        &self,
        declaration: &ProviderRateDeclaration,
    ) -> Result<ProviderRateBinding, BudgetPoolError> {
        let (_observation, registration) = self
            .serialized_timed_store_operation(|store, run_id, now| {
                store.register(run_id, declaration, now)
            })
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

    pub(in crate::policy) fn with_prepared_registration_bindings<T>(
        &self,
        declarations: &[ProviderRateDeclaration],
        operation: impl FnOnce(&[ProviderRateBinding], ClockObservation) -> Result<T, BudgetPoolError>,
    ) -> Result<T, BudgetPoolError> {
        if declarations.is_empty() {
            return operation(
                &[],
                self.clock_observation()
                    .map_err(map_store_registration_error)?,
            );
        }
        let _operation = self
            .inner
            .operation_gate
            .lock()
            .map_err(|_| BudgetPoolError::Persistence)?;
        let observation = self
            .inner
            .clock
            .observation()
            .map_err(|_| BudgetPoolError::ClockUnavailable)?;
        let prepared = self
            .inner
            .store
            .prepare_registration_batch(self.inner.run_id, declarations, observation.wall_clock)
            .map_err(map_store_registration_error)?;
        if prepared.registrations().len() != declarations.len() {
            return Err(BudgetPoolError::Persistence);
        }
        let mut bindings = Vec::new();
        bindings
            .try_reserve_exact(declarations.len())
            .map_err(|_| BudgetPoolError::CoordinatorAllocation)?;
        for (registration, declaration) in prepared.registrations().iter().zip(declarations) {
            if registration.policy_digest() != declaration.policy_digest()
                || registration.declaration_digest() != declaration.declaration_digest()
            {
                return Err(BudgetPoolError::Persistence);
            }
            bindings.push(ProviderRateBinding {
                authority: self.clone(),
                registration: *registration,
            });
        }
        let result = operation(&bindings, observation)?;
        prepared.commit().map_err(map_store_registration_error)?;
        Ok(result)
    }

    fn clock_observation(&self) -> Result<ClockObservation, ProviderRateStoreError> {
        let _operation = self
            .inner
            .operation_gate
            .lock()
            .map_err(|_| ProviderRateStoreError::Corrupt)?;
        self.inner
            .clock
            .observation()
            .map_err(|_| ProviderRateStoreError::Clock)
    }

    fn serialized_timed_store_operation<T>(
        &self,
        operation: impl FnOnce(
            &dyn ProviderRateStore,
            ProviderRateRunId,
            Timestamp,
        ) -> Result<T, ProviderRateStoreError>,
    ) -> Result<(ClockObservation, T), ProviderRateStoreError> {
        let _operation = self
            .inner
            .operation_gate
            .lock()
            .map_err(|_| ProviderRateStoreError::Corrupt)?;
        let observation = self
            .inner
            .clock
            .observation()
            .map_err(|_| ProviderRateStoreError::Clock)?;
        let result = operation(
            self.inner.store.as_ref(),
            self.inner.run_id,
            observation.wall_clock,
        )?;
        Ok((observation, result))
    }

    fn serialized_store_operation<T>(
        &self,
        operation: impl FnOnce(
            &dyn ProviderRateStore,
            ProviderRateRunId,
        ) -> Result<T, ProviderRateStoreError>,
    ) -> Result<T, ProviderRateStoreError> {
        let _operation = self
            .inner
            .operation_gate
            .lock()
            .map_err(|_| ProviderRateStoreError::Corrupt)?;
        operation(self.inner.store.as_ref(), self.inner.run_id)
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
        self.serialized_store_operation(|store, _run_id| {
            store.resolve_authorization_subject(mode, evidence)
        })
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

    pub(in crate::policy) fn try_reserve_decision(
        &self,
    ) -> Result<(ClockObservation, ProviderRateReservationDecision), BudgetUnavailableReason> {
        self.authority
            .serialized_timed_store_operation(|store, run_id, now| {
                store.try_reserve(run_id, self.registration, now)
            })
            .map_err(map_store_runtime_error)
    }

    pub(in crate::policy) fn commit_dispatch(
        &self,
        reservation_id: ProviderRateReservationId,
        claim: ProviderRateDispatchClaim,
    ) -> Result<(ClockObservation, ProviderRateDispatchDecision), BudgetUnavailableReason> {
        self.authority
            .serialized_timed_store_operation(|store, run_id, now| {
                store.commit_dispatch_with_claim(
                    run_id,
                    self.registration,
                    reservation_id,
                    now,
                    claim,
                )
            })
            .map_err(map_store_runtime_error)
    }

    pub(in crate::policy) fn apply_retry_after(
        &self,
        retry_after: RetryAfter,
    ) -> Result<(ClockObservation, ProviderRateReservationDecision), BudgetUnavailableReason> {
        self.authority
            .serialized_timed_store_operation(|store, run_id, now| {
                store.apply_retry_after(run_id, self.registration, now, retry_after)
            })
            .map_err(map_store_runtime_error)
    }

    pub(in crate::policy) fn apply_refusal(
        &self,
        jitter_sample_basis_points: u16,
    ) -> Result<(ClockObservation, ProviderRateReservationDecision), BudgetUnavailableReason> {
        self.authority
            .serialized_timed_store_operation(|store, run_id, now| {
                store.apply_refusal(run_id, self.registration, now, jitter_sample_basis_points)
            })
            .map_err(map_store_runtime_error)
    }

    pub(in crate::policy) fn record_success(&self) -> Result<(), BudgetUnavailableReason> {
        self.authority
            .serialized_timed_store_operation(|store, run_id, now| {
                store.record_success(run_id, self.registration, now)
            })
            .map(|(_observation, ())| ())
            .map_err(map_store_runtime_error)
    }
}

pub(in crate::policy) struct ProviderRateReservation {
    binding: ProviderRateBinding,
    reservation_id: ProviderRateReservationId,
    released: bool,
}

impl std::fmt::Debug for ProviderRateReservation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProviderRateReservation")
            .field("reservation_id", &self.reservation_id)
            .field("released", &self.released)
            .finish_non_exhaustive()
    }
}

impl ProviderRateReservation {
    pub(in crate::policy) fn new(
        binding: ProviderRateBinding,
        reservation_id: ProviderRateReservationId,
    ) -> Self {
        Self {
            binding,
            reservation_id,
            released: false,
        }
    }

    pub(in crate::policy) fn commit_dispatch(
        mut self,
        claim: ProviderRateDispatchClaim,
    ) -> Result<ProviderRateReservationDispatch, BudgetUnavailableReason> {
        let (observation, decision) = self.binding.commit_dispatch(self.reservation_id, claim)?;
        self.released = true;
        match decision {
            ProviderRateDispatchDecision::Ready(permit_id) => {
                Ok(ProviderRateReservationDispatch::Ready {
                    observation,
                    permit: ProviderRatePermit {
                        binding: self.binding.clone(),
                        permit_id,
                        claim,
                        released: false,
                    },
                })
            }
            ProviderRateDispatchDecision::WaitUntil(deadline) => {
                Ok(ProviderRateReservationDispatch::WaitUntil {
                    observation,
                    deadline,
                })
            }
            ProviderRateDispatchDecision::Unavailable(reason) => {
                Ok(ProviderRateReservationDispatch::Unavailable { reason })
            }
        }
    }

    pub(in crate::policy) fn release(&mut self) -> Result<(), BudgetUnavailableReason> {
        if self.released {
            return Ok(());
        }
        self.binding
            .authority
            .serialized_store_operation(|store, run_id| {
                store.cancel_reservation(run_id, self.binding.registration, self.reservation_id)
            })
            .map_err(map_store_runtime_error)?;
        self.released = true;
        Ok(())
    }
}

pub(in crate::policy) enum ProviderRateReservationDispatch {
    Ready {
        observation: ClockObservation,
        permit: ProviderRatePermit,
    },
    WaitUntil {
        observation: ClockObservation,
        deadline: Timestamp,
    },
    Unavailable {
        reason: BudgetUnavailableReason,
    },
}

impl Drop for ProviderRateReservation {
    fn drop(&mut self) {
        let _released = self.release();
    }
}

pub(in crate::policy) struct ProviderRatePermit {
    binding: ProviderRateBinding,
    permit_id: ProviderRatePermitId,
    claim: ProviderRateDispatchClaim,
    released: bool,
}

impl std::fmt::Debug for ProviderRatePermit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProviderRatePermit")
            .field("permit_id", &self.permit_id)
            .field("claim", &self.claim)
            .field("released", &self.released)
            .finish_non_exhaustive()
    }
}

impl ProviderRatePermit {
    pub(in crate::policy) fn settle_response(
        &mut self,
        settlement: ProviderRateResponseSettlement,
    ) -> Result<ProviderRateResponseSettlementReceipt, BudgetUnavailableReason> {
        if self.released {
            return Err(BudgetUnavailableReason::StateCorrupt);
        }
        let receipt = self
            .binding
            .authority
            .serialized_timed_store_operation(|store, run_id, now| {
                store.settle_response(
                    run_id,
                    self.binding.registration,
                    self.permit_id,
                    now,
                    settlement,
                )
            })
            .map(|(_observation, receipt)| receipt)
            .map_err(map_store_runtime_error)?;
        // The store has consumed the exact permit even if its returned receipt is malformed.
        self.released = true;
        if receipt.group_id() != self.binding.registration.group_id()
            || receipt.permit_id() != self.permit_id
            || receipt.settlement() != settlement
            || self
                .claim
                .maximum_response_bytes()
                .is_some_and(|maximum| receipt.charged_response_bytes() > maximum)
            || (settlement.response_class() == crate::ProviderRateResponseClass::AbandonedUnknown
                && receipt.charged_response_bytes()
                    != self.claim.maximum_response_bytes().unwrap_or(0))
        {
            return Err(BudgetUnavailableReason::StateCorrupt);
        }
        Ok(receipt)
    }

    pub(in crate::policy) fn release(&mut self) -> Result<(), BudgetUnavailableReason> {
        if self.released {
            return Ok(());
        }
        if self.claim.is_request_only() {
            self.binding
                .authority
                .serialized_store_operation(|store, run_id| {
                    store.release(run_id, self.binding.registration, self.permit_id)
                })
                .map_err(map_store_runtime_error)?;
        } else {
            let settlement = ProviderRateResponseSettlement::abandoned_unknown();
            let receipt = self
                .binding
                .authority
                .serialized_timed_store_operation(|store, run_id, now| {
                    store.settle_response(
                        run_id,
                        self.binding.registration,
                        self.permit_id,
                        now,
                        settlement,
                    )
                })
                .map(|(_observation, receipt)| receipt)
                .map_err(map_store_runtime_error)?;
            // The store has consumed the exact permit even if its returned receipt is malformed.
            self.released = true;
            if receipt.group_id() != self.binding.registration.group_id()
                || receipt.permit_id() != self.permit_id
                || receipt.settlement() != settlement
                || receipt.charged_response_bytes()
                    != self.claim.maximum_response_bytes().unwrap_or(0)
            {
                return Err(BudgetUnavailableReason::StateCorrupt);
            }
        }
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
