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
            crate::AuthorizationMode::UserOwnedLocal => Err(NetworkPolicyError::InvalidBudgetScope),
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

    /// Returns the bounded exponential fallback delay for one refusal attempt.
    pub fn delay_nanos(self, attempt: u32, jitter_sample_basis_points: u16) -> u64 {
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

pub(in crate::policy) const MAX_PROVIDER_BUDGET_WINDOWS: usize = 4;
const MAX_ADDITIONAL_PROVIDER_BUDGET_WINDOWS: usize = MAX_PROVIDER_BUDGET_WINDOWS - 1;
pub(in crate::policy) const MAX_SLIDING_WINDOW_RELEASES: usize = 4_096;
pub(in crate::policy) const MAX_PROVIDER_WEIGHTED_WINDOWS: usize = 8;

/// Whether a provider request limit resets as a fixed interval or rolls per request.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BudgetWindowSemantics {
    /// Capacity resets at the end of the currently anchored interval.
    #[default]
    Tumbling,
    /// Each admitted request returns capacity exactly one duration after admission.
    Sliding,
}

impl BudgetWindowSemantics {
    const fn is_tumbling(&self) -> bool {
        matches!(self, Self::Tumbling)
    }
}

/// One explicit, checked request-limit window in a conjunctive provider policy.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderBudgetWindow {
    requests_per_window: NonZeroU32,
    window_nanos: NonZeroU64,
    semantics: BudgetWindowSemantics,
}

impl ProviderBudgetWindow {
    /// Constructs one bounded provider request window.
    ///
    /// # Errors
    ///
    /// Returns [`NetworkPolicyError::InvalidBudgetPolicy`] when the duration cannot be represented
    /// by durable wall-clock arithmetic.
    pub fn try_new(
        requests_per_window: NonZeroU32,
        window_nanos: NonZeroU64,
        semantics: BudgetWindowSemantics,
    ) -> Result<Self, NetworkPolicyError> {
        if window_nanos.get() > i64::MAX as u64 {
            return Err(NetworkPolicyError::InvalidBudgetPolicy);
        }
        Ok(Self {
            requests_per_window,
            window_nanos,
            semantics,
        })
    }

    /// Returns the maximum number of requests admitted within this window.
    pub const fn requests_per_window(self) -> u32 {
        self.requests_per_window.get()
    }

    /// Returns this window's duration in nanoseconds.
    pub const fn window_nanos(self) -> u64 {
        self.window_nanos.get()
    }

    /// Returns this window's reset semantics.
    pub const fn semantics(self) -> BudgetWindowSemantics {
        self.semantics
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderBudgetWindowWire {
    requests_per_window: NonZeroU32,
    window_nanos: NonZeroU64,
    semantics: BudgetWindowSemantics,
}

impl<'de> Deserialize<'de> for ProviderBudgetWindow {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ProviderBudgetWindowWire::deserialize(deserializer)?;
        Self::try_new(wire.requests_per_window, wire.window_nanos, wire.semantics)
            .map_err(serde::de::Error::custom)
    }
}

fn default_window_semantics() -> BudgetWindowSemantics {
    BudgetWindowSemantics::Tumbling
}

fn empty_additional_budget_windows()
-> BoundedVec<ProviderBudgetWindow, MAX_ADDITIONAL_PROVIDER_BUDGET_WINDOWS> {
    BoundedVec::empty()
}

fn no_additional_budget_windows(
    windows: &BoundedVec<ProviderBudgetWindow, MAX_ADDITIONAL_PROVIDER_BUDGET_WINDOWS>,
) -> bool {
    windows.is_empty()
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
    #[serde(
        default = "default_window_semantics",
        skip_serializing_if = "BudgetWindowSemantics::is_tumbling"
    )]
    window_semantics: BudgetWindowSemantics,
    #[serde(
        default = "empty_additional_budget_windows",
        skip_serializing_if = "no_additional_budget_windows"
    )]
    additional_windows: BoundedVec<ProviderBudgetWindow, MAX_ADDITIONAL_PROVIDER_BUDGET_WINDOWS>,
    weighted_windows: BoundedVec<crate::ProviderRateWeightedWindow, MAX_PROVIDER_WEIGHTED_WINDOWS>,
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
        Self::try_new_conjunctive(
            scope,
            &[ProviderBudgetWindow::try_new(
                requests_per_window,
                window_nanos,
                BudgetWindowSemantics::Tumbling,
            )?],
            max_concurrent,
            backoff,
        )
    }

    /// Constructs a canonical conjunction of one to four unique request windows.
    ///
    /// Windows are ordered by duration, and every admission must have capacity in every window.
    ///
    /// # Errors
    ///
    /// Returns [`NetworkPolicyError::InvalidBudgetPolicy`] for an empty or oversized conjunction,
    /// duplicate durations, an unrepresentable duration, concurrency above any request limit, or
    /// more than 4,096 total preallocated sliding-window release slots.
    pub fn try_new_conjunctive(
        scope: BudgetScope,
        windows: &[ProviderBudgetWindow],
        max_concurrent: NonZeroU16,
        backoff: BackoffPolicy,
    ) -> Result<Self, NetworkPolicyError> {
        Self::try_new_weighted_conjunctive(scope, windows, &[], max_concurrent, backoff)
    }

    /// Constructs a canonical conjunction of request and weighted response windows.
    ///
    /// Request windows are ordered by duration. Weighted windows are ordered by dimension and
    /// duration. Duplicate request durations or duplicate weighted dimension/duration pairs are
    /// rejected. Dispatch against a policy containing weighted windows must reserve an exact
    /// worst-case response claim and terminalize the resulting permit once.
    ///
    /// # Errors
    ///
    /// Returns [`NetworkPolicyError::InvalidBudgetPolicy`] for the request-window failures
    /// documented by [`Self::try_new_conjunctive`], an oversized weighted conjunction, or a
    /// duplicate/unrepresentable weighted window.
    pub fn try_new_weighted_conjunctive(
        scope: BudgetScope,
        windows: &[ProviderBudgetWindow],
        weighted_windows: &[crate::ProviderRateWeightedWindow],
        max_concurrent: NonZeroU16,
        backoff: BackoffPolicy,
    ) -> Result<Self, NetworkPolicyError> {
        if windows.is_empty() || windows.len() > MAX_PROVIDER_BUDGET_WINDOWS {
            return Err(NetworkPolicyError::InvalidBudgetPolicy);
        }
        if weighted_windows.len() > MAX_PROVIDER_WEIGHTED_WINDOWS {
            return Err(NetworkPolicyError::InvalidBudgetPolicy);
        }
        let mut canonical = Vec::new();
        canonical
            .try_reserve(windows.len())
            .map_err(|_| NetworkPolicyError::InvalidBudgetPolicy)?;
        canonical.extend_from_slice(windows);
        canonical.sort_unstable_by_key(|window| window.window_nanos());
        let concurrency = u32::from(max_concurrent.get());
        let mut sliding_capacity = 0_usize;
        let mut previous_duration = None;
        for window in canonical.iter().copied() {
            if window.window_nanos() > i64::MAX as u64
                || concurrency > window.requests_per_window()
                || previous_duration == Some(window.window_nanos())
            {
                return Err(NetworkPolicyError::InvalidBudgetPolicy);
            }
            previous_duration = Some(window.window_nanos());
            if window.semantics() == BudgetWindowSemantics::Sliding {
                sliding_capacity = sliding_capacity
                    .checked_add(
                        usize::try_from(window.requests_per_window())
                            .map_err(|_| NetworkPolicyError::InvalidBudgetPolicy)?,
                    )
                    .filter(|capacity| *capacity <= MAX_SLIDING_WINDOW_RELEASES)
                    .ok_or(NetworkPolicyError::InvalidBudgetPolicy)?;
            }
        }
        let mut canonical = canonical.into_iter();
        let primary = canonical
            .next()
            .ok_or(NetworkPolicyError::InvalidBudgetPolicy)?;
        let mut additional = Vec::new();
        additional
            .try_reserve(MAX_ADDITIONAL_PROVIDER_BUDGET_WINDOWS)
            .map_err(|_| NetworkPolicyError::InvalidBudgetPolicy)?;
        additional.extend(canonical);
        let additional_windows =
            BoundedVec::try_new(additional).map_err(|_| NetworkPolicyError::InvalidBudgetPolicy)?;
        let mut canonical_weighted = Vec::new();
        canonical_weighted
            .try_reserve(weighted_windows.len())
            .map_err(|_| NetworkPolicyError::InvalidBudgetPolicy)?;
        canonical_weighted.extend_from_slice(weighted_windows);
        canonical_weighted
            .sort_unstable_by_key(|window| (window.dimension(), window.window_nanos()));
        if canonical_weighted
            .iter()
            .zip(canonical_weighted.iter().skip(1))
            .any(|(left, right)| {
                left.dimension() == right.dimension() && left.window_nanos() == right.window_nanos()
            })
        {
            return Err(NetworkPolicyError::InvalidBudgetPolicy);
        }
        let weighted_windows = BoundedVec::try_new(canonical_weighted)
            .map_err(|_| NetworkPolicyError::InvalidBudgetPolicy)?;
        Ok(Self {
            scope,
            requests_per_window: primary.requests_per_window,
            window_nanos: primary.window_nanos,
            max_concurrent,
            backoff,
            window_semantics: primary.semantics,
            additional_windows,
            weighted_windows,
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

    /// Returns the number of conjunctive request windows.
    pub fn window_count(&self) -> usize {
        self.additional_windows.len() + 1
    }

    /// Returns a canonical request window by ascending duration.
    pub fn window(&self, index: usize) -> Option<ProviderBudgetWindow> {
        if index == 0 {
            return Some(self.primary_window());
        }
        self.additional_windows.as_slice().get(index - 1).copied()
    }

    pub(in crate::policy) fn windows(&self) -> impl Iterator<Item = ProviderBudgetWindow> + '_ {
        std::iter::once(self.primary_window())
            .chain(self.additional_windows.as_slice().iter().copied())
    }

    /// Returns the number of conjunctive weighted response windows.
    pub fn weighted_window_count(&self) -> usize {
        self.weighted_windows.len()
    }

    /// Returns one canonical weighted response window.
    pub fn weighted_window(&self, index: usize) -> Option<crate::ProviderRateWeightedWindow> {
        self.weighted_windows.as_slice().get(index).copied()
    }

    pub(in crate::policy) fn weighted_windows(
        &self,
    ) -> impl Iterator<Item = crate::ProviderRateWeightedWindow> + '_ {
        self.weighted_windows.as_slice().iter().copied()
    }

    pub(in crate::policy) fn has_weighted_windows(&self) -> bool {
        !self.weighted_windows.is_empty()
    }

    pub(in crate::policy) fn dispatch_claim(
        &self,
        maximum_response_bytes: NonZeroU64,
    ) -> Result<crate::ProviderRateDispatchClaim, NetworkPolicyError> {
        let response_bytes = self
            .weighted_windows()
            .any(|window| window.dimension() == crate::ProviderRateWeightedDimension::ResponseBytes)
            .then_some(maximum_response_bytes);
        let provider_error_units = u8::from(self.weighted_windows().any(|window| {
            window.dimension() == crate::ProviderRateWeightedDimension::ProviderErrors
        }));
        crate::ProviderRateDispatchClaim::try_new(response_bytes, provider_error_units)
            .map_err(|_| NetworkPolicyError::InvalidBudgetPolicy)
    }

    const fn primary_window(&self) -> ProviderBudgetWindow {
        ProviderBudgetWindow {
            requests_per_window: self.requests_per_window,
            window_nanos: self.window_nanos,
            semantics: self.window_semantics,
        }
    }

    /// Returns the maximum number of requests concurrently in flight.
    pub const fn max_concurrent(&self) -> u16 {
        self.max_concurrent.get()
    }

    /// Returns the provider-refusal backoff policy.
    pub const fn backoff(&self) -> BackoffPolicy {
        self.backoff
    }

    pub(in crate::policy) fn with_authorization_subject(
        &self,
        subject: SourceIdentifier,
    ) -> Result<Self, NetworkPolicyError> {
        if self.scope.authorization_account().is_none() {
            return Err(NetworkPolicyError::InvalidBudgetScope);
        }
        let mut qualified = self.clone();
        qualified.scope = BudgetScope::with_authorization_account(
            self.scope.as_source_identifier().clone(),
            subject,
        );
        Ok(qualified)
    }

    pub(crate) fn has_same_limits_as(&self, other: &Self) -> bool {
        self.requests_per_window == other.requests_per_window
            && self.window_nanos == other.window_nanos
            && self.max_concurrent == other.max_concurrent
            && self.backoff == other.backoff
            && self.window_semantics == other.window_semantics
            && self.additional_windows == other.additional_windows
            && self.weighted_windows == other.weighted_windows
    }

    pub(in crate::policy) fn dynamic_retained_bytes(&self) -> Option<usize> {
        let Self {
            scope,
            requests_per_window: _,
            window_nanos: _,
            max_concurrent: _,
            backoff,
            window_semantics: _,
            additional_windows,
            weighted_windows,
        } = self;
        scope
            .dynamic_retained_bytes()?
            .checked_add(backoff.dynamic_retained_bytes()?)
            .and_then(|bytes| {
                additional_windows
                    .checked_allocation_bytes()
                    .and_then(|windows| bytes.checked_add(windows))
            })
            .and_then(|bytes| {
                weighted_windows
                    .checked_allocation_bytes()
                    .and_then(|windows| bytes.checked_add(windows))
            })
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
    #[serde(default = "default_window_semantics")]
    window_semantics: BudgetWindowSemantics,
    #[serde(default = "empty_additional_budget_windows")]
    additional_windows: BoundedVec<ProviderBudgetWindow, MAX_ADDITIONAL_PROVIDER_BUDGET_WINDOWS>,
    weighted_windows: BoundedVec<crate::ProviderRateWeightedWindow, MAX_PROVIDER_WEIGHTED_WINDOWS>,
}

impl<'de> Deserialize<'de> for ProviderBudgetPolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ProviderBudgetPolicyWire::deserialize(deserializer)?;
        let mut windows = Vec::new();
        windows
            .try_reserve(wire.additional_windows.len() + 1)
            .map_err(|_| serde::de::Error::custom(NetworkPolicyError::InvalidBudgetPolicy))?;
        windows.push(
            ProviderBudgetWindow::try_new(
                wire.requests_per_window,
                wire.window_nanos,
                wire.window_semantics,
            )
            .map_err(serde::de::Error::custom)?,
        );
        windows.extend(wire.additional_windows.into_vec());
        Self::try_new_weighted_conjunctive(
            wire.scope,
            &windows,
            wire.weighted_windows.as_slice(),
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
            crate::AuthorizationMode::PublicInterface => BudgetCollisionKey::Public(
                self.endpoint_policy
                    .canonical_network_authorities()
                    .map_err(|_| BudgetPolicyResolutionError::InvalidPolicy)?
                    .into_vec(),
            ),
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
