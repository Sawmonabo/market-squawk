//! Provider budget identity, policy evidence, and canonical collision semantics.

use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BudgetCollisionKey {
    Public(Vec<CanonicalNetworkAuthority>),
    Account(SourceIdentifier),
}

impl BudgetCollisionKey {
    pub(in crate::policy) fn collides_with(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Public(left), Self::Public(right)) => left
                .as_slice()
                .iter()
                .any(|authority| right.as_slice().binary_search(authority).is_ok()),
            (Self::Account(left), Self::Account(right)) => left == right,
            (Self::Public(_), Self::Account(_)) | (Self::Account(_), Self::Public(_)) => false,
        }
    }

    pub(in crate::policy) fn merge_public_authorities(
        &mut self,
        other: &Self,
    ) -> Result<(), BudgetCollisionMergeError> {
        self.merge_public_authorities_with_limit(other, MAX_MERGED_CANONICAL_AUTHORITIES)
    }

    pub(in crate::policy) fn merge_public_authorities_with_limit(
        &mut self,
        other: &Self,
        maximum: usize,
    ) -> Result<(), BudgetCollisionMergeError> {
        let (Self::Public(current), Self::Public(additional)) = (self, other) else {
            return Ok(());
        };
        let additional_count = additional
            .iter()
            .filter(|authority| current.binary_search(authority).is_err())
            .count();
        let merged_len = current
            .len()
            .checked_add(additional_count)
            .filter(|length| *length <= maximum)
            .ok_or(BudgetCollisionMergeError::Capacity)?;
        current
            .try_reserve(merged_len.saturating_sub(current.len()))
            .map_err(|_| BudgetCollisionMergeError::Allocation)?;
        let original_len = current.len();
        for authority in additional {
            if current[..original_len].binary_search(authority).is_err() {
                current.push(authority.clone());
            }
        }
        current.sort_unstable();
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(in crate::policy) enum BudgetCollisionMergeError {
    #[error("canonical network authority capacity exhausted")]
    Capacity,
    #[error("canonical network authority allocation failed")]
    Allocation,
}

/// Human-readable provider/account declaration retained for audit metadata only.
///
/// Registry and process budget coordination never key allocations by these caller-authored
/// labels. They conservatively collide normalized public network authorities or trusted stable
/// authorization subjects.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetScope {
    provider: SourceIdentifier,
    authorization_account: Option<SourceIdentifier>,
}

impl BudgetScope {
    /// Constructs a bounded public provider declaration for metadata and diagnostics.
    pub const fn new(value: SourceIdentifier) -> Self {
        Self {
            provider: value,
            authorization_account: None,
        }
    }

    /// Constructs a provider scope qualified by one non-secret authorization/account reference.
    pub const fn with_authorization_account(
        provider: SourceIdentifier,
        authorization_account: SourceIdentifier,
    ) -> Self {
        Self {
            provider,
            authorization_account: Some(authorization_account),
        }
    }

    /// Derives the only valid provider/account scope for an evidenced authorization grant.
    ///
    /// # Errors
    ///
    /// Rejects local user-owned authorization because it must not have a remote provider budget.
    pub fn for_authorization(
        provider: SourceIdentifier,
        authorization: &crate::AuthorizationGrant,
    ) -> Result<Self, NetworkPolicyError> {
        match authorization.mode() {
            crate::AuthorizationMode::PublicInterface => Ok(Self::new(provider)),
            crate::AuthorizationMode::UserAuthorized | crate::AuthorizationMode::Licensed => {
                Ok(Self::with_authorization_account(
                    provider,
                    authorization.basis().as_source_identifier().clone(),
                ))
            }
            crate::AuthorizationMode::UserOwnedLocal => {
                Err(NetworkPolicyError::InvalidBudgetScope)
            }
        }
    }

    /// Returns the caller-declared provider label retained for metadata and diagnostics.
    pub const fn as_source_identifier(&self) -> &SourceIdentifier {
        &self.provider
    }

    /// Returns the non-secret authorization/account reference when configured.
    pub const fn authorization_account(&self) -> Option<&SourceIdentifier> {
        self.authorization_account.as_ref()
    }

    fn dynamic_retained_bytes(&self) -> Option<usize> {
        let Self {
            provider,
            authorization_account,
        } = self;
        provider.retained_bytes().checked_add(
            authorization_account
                .as_ref()
                .map_or(0, SourceIdentifier::retained_bytes),
        )
    }
}

/// Bounded exponential-backoff settings applied to provider refusal responses.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BackoffPolicy {
    initial_nanos: NonZeroU64,
    maximum_nanos: NonZeroU64,
    jitter_basis_points: u16,
}

impl BackoffPolicy {
    /// Constructs bounded backoff settings.
    ///
    /// # Errors
    ///
    /// Rejects an initial delay above its maximum or jitter above 100 percent.
    pub const fn try_new(
        initial_nanos: NonZeroU64,
        maximum_nanos: NonZeroU64,
        jitter_basis_points: u16,
    ) -> Result<Self, NetworkPolicyError> {
        if initial_nanos.get() > maximum_nanos.get() || jitter_basis_points > 10_000 {
            Err(NetworkPolicyError::InvalidBudgetPolicy)
        } else {
            Ok(Self {
                initial_nanos,
                maximum_nanos,
                jitter_basis_points,
            })
        }
    }

    /// Returns the maximum provider backoff in nanoseconds.
    pub const fn maximum_nanos(self) -> u64 {
        self.maximum_nanos.get()
    }

    pub(in crate::policy) fn delay_nanos(
        self,
        attempt: u32,
        jitter_sample_basis_points: u16,
    ) -> u64 {
        let shift = attempt.min(63);
        let base = self
            .initial_nanos
            .get()
            .checked_shl(shift)
            .unwrap_or(self.maximum_nanos.get())
            .min(self.maximum_nanos.get());
        let sample = jitter_sample_basis_points.min(self.jitter_basis_points);
        let jitter =
            (u128::from(base) * u128::from(sample) / 10_000).min(u128::from(u64::MAX)) as u64;
        base.checked_add(jitter)
            .unwrap_or(self.maximum_nanos.get())
            .min(self.maximum_nanos.get())
    }

    pub(in crate::policy) fn dynamic_retained_bytes(&self) -> Option<usize> {
        let Self {
            initial_nanos: _,
            maximum_nanos: _,
            jitter_basis_points: _,
        } = self;
        Some(0)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BackoffPolicyWire {
    initial_nanos: NonZeroU64,
    maximum_nanos: NonZeroU64,
    jitter_basis_points: u16,
}

impl<'de> Deserialize<'de> for BackoffPolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = BackoffPolicyWire::deserialize(deserializer)?;
        Self::try_new(
            wire.initial_nanos,
            wire.maximum_nanos,
            wire.jitter_basis_points,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Published request-window and local concurrency limits for one shared scope.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderBudgetPolicy {
    scope: BudgetScope,
    requests_per_window: NonZeroU32,
    window_nanos: NonZeroU64,
    max_concurrent: NonZeroU16,
    backoff: BackoffPolicy,
}

impl ProviderBudgetPolicy {
    /// Constructs a provider budget with no alternate identity, endpoint, or shard policy.
    pub fn try_new(
        scope: BudgetScope,
        requests_per_window: NonZeroU32,
        window_nanos: NonZeroU64,
        max_concurrent: NonZeroU16,
        backoff: BackoffPolicy,
    ) -> Result<Self, NetworkPolicyError> {
        if window_nanos.get() > i64::MAX as u64
            || u32::from(max_concurrent.get()) > requests_per_window.get()
        {
            return Err(NetworkPolicyError::InvalidBudgetPolicy);
        }
        Ok(Self {
            scope,
            requests_per_window,
            window_nanos,
            max_concurrent,
            backoff,
        })
    }

    /// Returns the human-readable provider/account declaration.
    pub const fn scope(&self) -> &BudgetScope {
        &self.scope
    }

    /// Returns the maximum number of requests accepted in one window.
    pub const fn requests_per_window(&self) -> u32 {
        self.requests_per_window.get()
    }

    /// Returns the request-window duration in nanoseconds.
    pub const fn window_nanos(&self) -> u64 {
        self.window_nanos.get()
    }

    /// Returns the maximum number of requests concurrently in flight.
    pub const fn max_concurrent(&self) -> u16 {
        self.max_concurrent.get()
    }

    /// Returns the provider-refusal backoff policy.
    pub const fn backoff(&self) -> BackoffPolicy {
        self.backoff
    }

    pub(crate) fn has_same_limits_as(&self, other: &Self) -> bool {
        self.requests_per_window == other.requests_per_window
            && self.window_nanos == other.window_nanos
            && self.max_concurrent == other.max_concurrent
            && self.backoff == other.backoff
    }

    pub(in crate::policy) fn dynamic_retained_bytes(&self) -> Option<usize> {
        let Self {
            scope,
            requests_per_window: _,
            window_nanos: _,
            max_concurrent: _,
            backoff,
        } = self;
        scope
            .dynamic_retained_bytes()?
            .checked_add(backoff.dynamic_retained_bytes()?)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderBudgetPolicyWire {
    scope: BudgetScope,
    requests_per_window: NonZeroU32,
    window_nanos: NonZeroU64,
    max_concurrent: NonZeroU16,
    backoff: BackoffPolicy,
}

impl<'de> Deserialize<'de> for ProviderBudgetPolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ProviderBudgetPolicyWire::deserialize(deserializer)?;
        Self::try_new(
            wire.scope,
            wire.requests_per_window,
            wire.window_nanos,
            wire.max_concurrent,
            wire.backoff,
        )
        .map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PersistedProviderBudgetPolicy {
    policy: ProviderBudgetPolicy,
    endpoint_policy: EndpointPolicy,
    authorization: crate::AuthorizationGrant,
    resolved_subject_record: Option<SourceIdentifier>,
}

impl PersistedProviderBudgetPolicy {
    pub(crate) fn try_new(
        policy: ProviderBudgetPolicy,
        endpoint_policy: EndpointPolicy,
        authorization: crate::AuthorizationGrant,
        resolved_subject_record: Option<SourceIdentifier>,
    ) -> Result<Self, NetworkPolicyError> {
        let persisted = Self {
            policy,
            endpoint_policy,
            authorization,
            resolved_subject_record,
        };
        persisted.validate_structure()?;
        Ok(persisted)
    }

    pub(crate) const fn policy(&self) -> &ProviderBudgetPolicy {
        &self.policy
    }

    pub(crate) fn resolve(
        &self,
        resolver: &dyn crate::AuthorizationSubjectResolver,
    ) -> Result<ResolvedProviderBudgetPolicy, BudgetPolicyResolutionError> {
        self.validate_structure()
            .map_err(|_| BudgetPolicyResolutionError::InvalidPolicy)?;
        let collision_key = match self.authorization.mode() {
            crate::AuthorizationMode::PublicInterface => {
                BudgetCollisionKey::Public(
                    self.endpoint_policy
                        .canonical_network_authorities()
                        .map_err(|_| BudgetPolicyResolutionError::InvalidPolicy)?
                        .into_vec(),
                )
            }
            crate::AuthorizationMode::UserAuthorized | crate::AuthorizationMode::Licensed => {
                let resolved = resolver
                    .resolve_subject_record(
                        self.authorization.mode(),
                        self.authorization.evidence().content_digest(),
                    )
                    .map_err(BudgetPolicyResolutionError::SubjectResolution)?;
                if self.resolved_subject_record.as_ref() != Some(&resolved) {
                    return Err(BudgetPolicyResolutionError::SubjectMismatch);
                }
                BudgetCollisionKey::Account(resolved)
            }
            crate::AuthorizationMode::UserOwnedLocal => {
                return Err(BudgetPolicyResolutionError::InvalidPolicy);
            }
        };
        Ok(ResolvedProviderBudgetPolicy {
            persisted: self.clone(),
            collision_key,
        })
    }

    fn validate_structure(&self) -> Result<(), NetworkPolicyError> {
        let expected = BudgetScope::for_authorization(
            self.policy.scope().as_source_identifier().clone(),
            &self.authorization,
        )?;
        if self.policy.scope() != &expected {
            return Err(NetworkPolicyError::InvalidBudgetScope);
        }
        let subject_shape_is_valid = match self.authorization.mode() {
            crate::AuthorizationMode::PublicInterface => self.resolved_subject_record.is_none(),
            crate::AuthorizationMode::UserAuthorized | crate::AuthorizationMode::Licensed => {
                self.resolved_subject_record.is_some()
            }
            crate::AuthorizationMode::UserOwnedLocal => false,
        };
        if !subject_shape_is_valid {
            return Err(NetworkPolicyError::InvalidBudgetScope);
        }
        let _authorities = self.endpoint_policy.canonical_network_authorities()?;
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedProviderBudgetPolicyWire {
    policy: ProviderBudgetPolicy,
    endpoint_policy: EndpointPolicy,
    authorization: crate::AuthorizationGrant,
    resolved_subject_record: Option<SourceIdentifier>,
}

impl<'de> Deserialize<'de> for PersistedProviderBudgetPolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = PersistedProviderBudgetPolicyWire::deserialize(deserializer)?;
        Self::try_new(
            wire.policy,
            wire.endpoint_policy,
            wire.authorization,
            wire.resolved_subject_record,
        )
        .map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedProviderBudgetPolicy {
    persisted: PersistedProviderBudgetPolicy,
    collision_key: BudgetCollisionKey,
}

impl ResolvedProviderBudgetPolicy {
    pub(in crate::policy) const fn from_canonical_parts(
        persisted: PersistedProviderBudgetPolicy,
        collision_key: BudgetCollisionKey,
    ) -> Self {
        Self {
            persisted,
            collision_key,
        }
    }

    pub(crate) fn try_new(
        policy: ProviderBudgetPolicy,
        endpoint_policy: EndpointPolicy,
        authorization: crate::AuthorizationGrant,
        resolver: &dyn crate::AuthorizationSubjectResolver,
    ) -> Result<Self, BudgetPolicyResolutionError> {
        let (resolved_subject_record, collision_key) = match authorization.mode() {
            crate::AuthorizationMode::PublicInterface => (
                None,
                BudgetCollisionKey::Public(
                    endpoint_policy
                        .canonical_network_authorities()
                        .map_err(|_| BudgetPolicyResolutionError::InvalidPolicy)?
                        .into_vec(),
                ),
            ),
            crate::AuthorizationMode::UserAuthorized | crate::AuthorizationMode::Licensed => {
                let subject = resolver
                    .resolve_subject_record(
                        authorization.mode(),
                        authorization.evidence().content_digest(),
                    )
                    .map_err(BudgetPolicyResolutionError::SubjectResolution)?;
                (Some(subject.clone()), BudgetCollisionKey::Account(subject))
            }
            crate::AuthorizationMode::UserOwnedLocal => {
                return Err(BudgetPolicyResolutionError::InvalidPolicy);
            }
        };
        let persisted = PersistedProviderBudgetPolicy::try_new(
            policy,
            endpoint_policy,
            authorization,
            resolved_subject_record,
        )
        .map_err(|_| BudgetPolicyResolutionError::InvalidPolicy)?;
        Ok(Self {
            persisted,
            collision_key,
        })
    }

    pub(crate) const fn persisted(&self) -> &PersistedProviderBudgetPolicy {
        &self.persisted
    }

    pub(crate) const fn policy(&self) -> &ProviderBudgetPolicy {
        self.persisted.policy()
    }

    pub(crate) const fn collision_key(&self) -> &BudgetCollisionKey {
        &self.collision_key
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum BudgetPolicyResolutionError {
    #[error("provider budget evidence is structurally invalid")]
    InvalidPolicy,
    #[error("authorization subject resolution failed: {0}")]
    SubjectResolution(crate::AuthorizationSubjectResolutionError),
    #[error("persisted authorization subject differs from trusted resolution")]
    SubjectMismatch,
}
