//! Actor-scoped live-action contracts with no caller-mintable authority surface.

use std::marker::PhantomData;
use std::num::{NonZeroU64, NonZeroUsize};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use market_squawk_analytics::{LiveFeatureView, RequiredLiveFeature};
use market_squawk_domain::{
    BookLevel, DataQuality, InstrumentExecutionTerms, LiveProvenance, MarketEvent,
    QualificationAssessmentId, Timestamp,
};
use thiserror::Error;

use crate::authority::{AppliedObservationAuthority, RuntimeLease, SystemTrustedClock};
use crate::processor::InstrumentLiveProcessor;
use crate::{AuthorityError, ConsumedLiveAuthority, LiveExecutionCapability, ShardKey};

/// Hard maximum capabilities one route hook may request for one committed observation.
pub const MAX_ACTION_AUTHORITY_ISSUES_PER_OBSERVATION: usize = 64;

const PREPARED_ACTION_HOOK_GENERATION: u64 = 0;
const RETIRED_ACTION_HOOK_GENERATION: u64 = u64::MAX;
/// Shared allocation and allocator slack conservatively charged to every route in a dynamic group.
const ACTION_HOOK_ACTIVATION_ALLOCATION_BYTES: usize = 256;

/// Validated closed authority-issue bound for one route hook invocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActionAuthorityIssueLimit(NonZeroUsize);

impl ActionAuthorityIssueLimit {
    /// Smallest valid per-observation issue bound.
    pub const MIN: Self = Self(NonZeroUsize::MIN);

    /// Validates a positive issue count against the process hard maximum.
    ///
    /// # Errors
    ///
    /// Rejects zero or a value above [`MAX_ACTION_AUTHORITY_ISSUES_PER_OBSERVATION`].
    pub fn try_new(value: usize) -> Result<Self, RouteActionHookError> {
        let value = NonZeroUsize::new(value).ok_or(RouteActionHookError::ZeroIssueLimit)?;
        if value.get() > MAX_ACTION_AUTHORITY_ISSUES_PER_OBSERVATION {
            return Err(RouteActionHookError::IssueLimitExceedsHardMaximum {
                requested: value.get(),
                maximum: MAX_ACTION_AUTHORITY_ISSUES_PER_OBSERVATION,
            });
        }
        Ok(Self(value))
    }

    /// Returns the positive validated issue count.
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

/// Non-cloneable exact route ownership transferred into one action-enabled runtime.
#[derive(Debug)]
pub struct RouteActionHook {
    route: ShardKey,
    hook: Box<dyn LiveActionHook>,
    required_features: Box<[RequiredLiveFeature]>,
    issue_limit: ActionAuthorityIssueLimit,
    declared_retained_bytes: usize,
    activation: RouteActionActivation,
}

impl RouteActionHook {
    /// Returns the exact complete route-hook charge before transferring hook ownership.
    pub fn retained_bytes_for_composition(
        route: &ShardKey,
        required_feature_count: usize,
        hook_retained_bytes: usize,
    ) -> Result<usize, RouteActionHookError> {
        route_action_retained_bytes(route, required_feature_count, hook_retained_bytes)
    }

    /// Binds one hook, requirement set, and issue bound to one exact route.
    ///
    /// # Errors
    ///
    /// Rejects duplicate requirements and retained-size accounting failure. An empty requirement
    /// set explicitly selects a canonical-market-reference-only strategy.
    pub fn try_new(
        route: ShardKey,
        hook: Box<dyn LiveActionHook>,
        mut required_features: Vec<RequiredLiveFeature>,
    ) -> Result<Self, RouteActionHookError> {
        required_features.sort_unstable();
        if required_features.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(RouteActionHookError::DuplicateRequiredFeature);
        }
        let declared_retained_bytes =
            route_action_retained_bytes(&route, required_features.len(), hook.retained_bytes()?)?;
        let issue_limit = hook.maximum_authority_issues();
        Ok(Self {
            route,
            hook,
            required_features: required_features.into_boxed_slice(),
            issue_limit,
            declared_retained_bytes,
            activation: RouteActionActivation::Startup,
        })
    }

    /// Returns the exact venue/instrument owner for this non-shareable hook.
    pub const fn route(&self) -> &ShardKey {
        &self.route
    }

    /// Returns the closed feature set that must all be ready before invocation.
    pub const fn required_features(&self) -> &[RequiredLiveFeature] {
        &self.required_features
    }

    /// Returns the maximum capabilities this hook may request per observation.
    pub const fn issue_limit(&self) -> ActionAuthorityIssueLimit {
        self.issue_limit
    }

    /// Returns the startup-declared maximum retained footprint.
    pub const fn declared_retained_bytes(&self) -> usize {
        self.declared_retained_bytes
    }

    pub(crate) fn validate_retained_bytes(
        &self,
        maximum: usize,
    ) -> Result<(), RouteActionHookError> {
        let observed = route_action_retained_bytes(
            &self.route,
            self.required_features.len(),
            self.hook.retained_bytes()?,
        )?;
        if observed != self.declared_retained_bytes {
            return Err(RouteActionHookError::RetainedSizeChanged {
                declared: self.declared_retained_bytes,
                observed,
            });
        }
        if observed > maximum {
            return Err(RouteActionHookError::RetainedSizeExceedsRouteMaximum {
                retained: observed,
                maximum,
            });
        }
        Ok(())
    }

    pub(crate) fn hook_mut(&mut self) -> &mut dyn LiveActionHook {
        self.hook.as_mut()
    }

    pub(crate) fn into_prepared_dynamic(mut self, activation: ActionHookActivationLease) -> Self {
        self.activation = RouteActionActivation::Dynamic(activation);
        self
    }

    pub(crate) fn action_enabled(&self) -> bool {
        match &self.activation {
            RouteActionActivation::Startup => true,
            RouteActionActivation::Dynamic(activation) => activation.is_active(),
        }
    }

    pub(crate) fn belongs_to_dynamic_group(&self, activation: &ActionHookActivationLease) -> bool {
        matches!(
            &self.activation,
            RouteActionActivation::Dynamic(current) if current.same_group(activation)
        )
    }
}

#[derive(Debug)]
enum RouteActionActivation {
    Startup,
    Dynamic(ActionHookActivationLease),
}

/// Route hook construction or startup accounting failure.
#[derive(Debug, Error)]
pub enum RouteActionHookError {
    /// At least one authority issue must be possible for an enabled hook.
    #[error("route action authority issue limit must be positive")]
    ZeroIssueLimit,
    /// Per-observation capability issuance exceeded the closed hard maximum.
    #[error("route action authority issue limit {requested} exceeds hard maximum {maximum}")]
    IssueLimitExceedsHardMaximum { requested: usize, maximum: usize },
    /// A readiness prerequisite appeared more than once.
    #[error("route action hook contains a duplicate required feature")]
    DuplicateRequiredFeature,
    /// Retained accounting changed between construction and runtime admission.
    #[error("route action retained bytes changed from {declared} to {observed}")]
    RetainedSizeChanged { declared: usize, observed: usize },
    /// The hook exceeds the already-reserved per-route ceiling.
    #[error("route action retains {retained} bytes but route maximum is {maximum}")]
    RetainedSizeExceedsRouteMaximum { retained: usize, maximum: usize },
    /// Wrapper, route, requirement, or hook retained accounting overflowed.
    #[error("route action retained-byte accounting overflowed")]
    RetainedSizeOverflow,
    /// Hook-owned retained-size accounting failed.
    #[error(transparent)]
    Hook(#[from] LiveActionHookError),
}

fn route_action_retained_bytes(
    route: &ShardKey,
    required_feature_count: usize,
    hook_retained_bytes: usize,
) -> Result<usize, RouteActionHookError> {
    std::mem::size_of::<RouteActionHook>()
        .checked_add(route.venue().as_str().len())
        .and_then(|value| {
            value.checked_add(
                required_feature_count.checked_mul(std::mem::size_of::<RequiredLiveFeature>())?,
            )
        })
        .and_then(|value| value.checked_add(hook_retained_bytes))
        .and_then(|value| value.checked_add(ACTION_HOOK_ACTIVATION_ALLOCATION_BYTES))
        .ok_or(RouteActionHookError::RetainedSizeOverflow)
}

/// Exact process-local generation assigned to one complete dynamically installed hook group.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LiveActionHookGeneration(NonZeroU64);

impl LiveActionHookGeneration {
    pub(crate) const fn new(value: NonZeroU64) -> Self {
        Self(value)
    }

    /// Returns the nonzero process-local generation value.
    pub const fn get(self) -> NonZeroU64 {
        self.0
    }
}

#[derive(Debug)]
struct ActionHookActivationState {
    runtime_incarnation: NonZeroU64,
    generation: LiveActionHookGeneration,
    active_generation: AtomicU64,
    runtime: RuntimeLease,
}

/// Internal validation-only view shared by every actor-owned route in one prepared group.
#[derive(Debug)]
pub(crate) struct ActionHookActivationLease {
    state: Arc<ActionHookActivationState>,
}

impl Clone for ActionHookActivationLease {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

impl ActionHookActivationLease {
    pub(crate) fn prepare(
        runtime_incarnation: NonZeroU64,
        generation: LiveActionHookGeneration,
        runtime: RuntimeLease,
    ) -> (Self, PreparedLiveActionHookGroup) {
        let activation = Self {
            state: Arc::new(ActionHookActivationState {
                runtime_incarnation,
                generation,
                active_generation: AtomicU64::new(PREPARED_ACTION_HOOK_GENERATION),
                runtime,
            }),
        };
        let prepared = PreparedLiveActionHookGroup {
            activation: activation.clone(),
        };
        (activation, prepared)
    }

    pub(crate) fn validate_prepared(
        &self,
        runtime_incarnation: NonZeroU64,
        generation: LiveActionHookGeneration,
    ) -> Result<(), LiveActionHookActivationError> {
        self.validate_identity(runtime_incarnation, generation)?;
        match self.state.active_generation.load(Ordering::Acquire) {
            PREPARED_ACTION_HOOK_GENERATION => Ok(()),
            RETIRED_ACTION_HOOK_GENERATION => Err(LiveActionHookActivationError::Retired),
            _ => Err(LiveActionHookActivationError::AlreadyActive),
        }
    }

    pub(crate) fn validate_disabled(
        &self,
        runtime_incarnation: NonZeroU64,
        generation: LiveActionHookGeneration,
    ) -> Result<(), LiveActionHookActivationError> {
        self.validate_identity(runtime_incarnation, generation)?;
        match self.state.active_generation.load(Ordering::Acquire) {
            PREPARED_ACTION_HOOK_GENERATION => Ok(()),
            RETIRED_ACTION_HOOK_GENERATION => Err(LiveActionHookActivationError::Retired),
            _ => Err(LiveActionHookActivationError::StillActive),
        }
    }

    pub(crate) fn disable(&self) {
        let current = self.state.active_generation.load(Ordering::Acquire);
        if current == self.state.generation.get().get() {
            let _ = self.state.active_generation.compare_exchange(
                current,
                PREPARED_ACTION_HOOK_GENERATION,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
    }

    pub(crate) fn retire(&self) -> Result<(), LiveActionHookActivationError> {
        self.state
            .active_generation
            .compare_exchange(
                PREPARED_ACTION_HOOK_GENERATION,
                RETIRED_ACTION_HOOK_GENERATION,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map(|_| ())
            .map_err(|observed| match observed {
                RETIRED_ACTION_HOOK_GENERATION => LiveActionHookActivationError::Retired,
                _ => LiveActionHookActivationError::StillActive,
            })
    }

    pub(crate) fn is_active(&self) -> bool {
        self.state.active_generation.load(Ordering::Acquire) == self.state.generation.get().get()
            && self.state.runtime.validate().is_ok()
    }

    pub(crate) fn same_group(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
            && self.state.runtime_incarnation == other.state.runtime_incarnation
            && self.state.generation == other.state.generation
    }

    fn activate(&self) -> Result<(), LiveActionHookActivationError> {
        self.state
            .runtime
            .validate()
            .map_err(|_| LiveActionHookActivationError::RuntimeClosed)?;
        self.state
            .active_generation
            .compare_exchange(
                PREPARED_ACTION_HOOK_GENERATION,
                self.state.generation.get().get(),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map(|_| ())
            .map_err(|observed| match observed {
                RETIRED_ACTION_HOOK_GENERATION => LiveActionHookActivationError::Retired,
                _ => LiveActionHookActivationError::AlreadyActive,
            })
    }

    fn validate_identity(
        &self,
        runtime_incarnation: NonZeroU64,
        generation: LiveActionHookGeneration,
    ) -> Result<(), LiveActionHookActivationError> {
        if self.state.runtime_incarnation != runtime_incarnation {
            return Err(LiveActionHookActivationError::RuntimeMismatch);
        }
        if self.state.generation != generation {
            return Err(LiveActionHookActivationError::GenerationMismatch);
        }
        self.state
            .runtime
            .validate()
            .map_err(|_| LiveActionHookActivationError::RuntimeClosed)
    }
}

/// Non-cloneable owner token returned only after every route acknowledged disabled installation.
#[derive(Debug)]
pub struct PreparedLiveActionHookGroup {
    activation: ActionHookActivationLease,
}

impl PreparedLiveActionHookGroup {
    /// Atomically enables the exact generation for every installed route in this group.
    ///
    /// # Errors
    ///
    /// Fails closed when the runtime is closed or the generation was activated or reaped already.
    pub fn activate(self) -> Result<ActiveLiveActionHookGroup, LiveActionHookActivationError> {
        if let Err(error) = self.activation.activate() {
            self.activation.disable();
            return Err(error);
        }
        Ok(ActiveLiveActionHookGroup {
            activation: self.activation,
        })
    }

    /// Returns the exact runtime incarnation bound to this non-transferable gate.
    pub fn runtime_incarnation(&self) -> NonZeroU64 {
        self.activation.state.runtime_incarnation
    }

    /// Returns the exact prepared group generation.
    pub fn generation(&self) -> LiveActionHookGeneration {
        self.activation.state.generation
    }

    #[cfg(test)]
    pub(crate) fn activation_for_test(&self) -> ActionHookActivationLease {
        self.activation.clone()
    }
}

/// Non-cloneable active owner token whose drop path always disables the complete hook group.
#[derive(Debug)]
pub struct ActiveLiveActionHookGroup {
    activation: ActionHookActivationLease,
}

impl ActiveLiveActionHookGroup {
    /// Synchronously disables the shared generation before hook removal or runtime shutdown.
    pub fn disable(self) -> DisabledLiveActionHookGroup {
        self.activation.disable();
        DisabledLiveActionHookGroup {
            activation: self.activation.clone(),
        }
    }

    /// Returns the exact runtime incarnation bound to this active gate.
    pub fn runtime_incarnation(&self) -> NonZeroU64 {
        self.activation.state.runtime_incarnation
    }

    /// Returns the exact active group generation.
    pub fn generation(&self) -> LiveActionHookGeneration {
        self.activation.state.generation
    }
}

impl Drop for ActiveLiveActionHookGroup {
    fn drop(&mut self) {
        self.activation.disable();
    }
}

/// Non-cloneable diagnostic receipt proving the caller synchronously disabled its exact group.
#[derive(Debug)]
pub struct DisabledLiveActionHookGroup {
    activation: ActionHookActivationLease,
}

impl DisabledLiveActionHookGroup {
    /// Returns the exact runtime incarnation formerly bound to this disabled gate.
    pub fn runtime_incarnation(&self) -> NonZeroU64 {
        self.activation.state.runtime_incarnation
    }

    /// Returns the exact disabled group generation.
    pub fn generation(&self) -> LiveActionHookGeneration {
        self.activation.state.generation
    }
}

/// Dynamic hook-group activation failure. No variant grants or restores execution authority.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LiveActionHookActivationError {
    /// The exact runtime incarnation has closed.
    #[error("live runtime incarnation is closed")]
    RuntimeClosed,
    /// The gate belongs to a different runtime incarnation.
    #[error("live action hook gate belongs to a different runtime incarnation")]
    RuntimeMismatch,
    /// The gate belongs to a different hook generation.
    #[error("live action hook gate belongs to a different generation")]
    GenerationMismatch,
    /// This exact generation was already activated.
    #[error("live action hook generation is already active")]
    AlreadyActive,
    /// Removal requires synchronous group disablement first.
    #[error("live action hook generation is still active")]
    StillActive,
    /// The exact generation was removed and cannot be reactivated.
    #[error("live action hook generation is retired")]
    Retired,
}

/// Exact acknowledgement receipt for one actor-owned dynamic hook-group cleanup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LiveActionHookReapReceipt {
    runtime_incarnation: NonZeroU64,
    generation: LiveActionHookGeneration,
    removed_hooks: usize,
}

impl LiveActionHookReapReceipt {
    pub(crate) const fn new(
        runtime_incarnation: NonZeroU64,
        generation: LiveActionHookGeneration,
        removed_hooks: usize,
    ) -> Self {
        Self {
            runtime_incarnation,
            generation,
            removed_hooks,
        }
    }

    /// Returns the runtime incarnation that owned every removed hook.
    pub const fn runtime_incarnation(self) -> NonZeroU64 {
        self.runtime_incarnation
    }

    /// Returns the exact retired hook generation.
    pub const fn generation(self) -> LiveActionHookGeneration {
        self.generation
    }

    /// Returns the number of route-owned hooks synchronously dropped before acknowledgement.
    pub const fn removed_hooks(self) -> usize {
        self.removed_hooks
    }
}

/// Closed actor-rejection reasons for dynamic hook-group control.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LiveActionControlRejection {
    RuntimeMismatch,
    InvalidActivation,
    EmptyGroup,
    DuplicateRoute,
    UnknownRoute,
    HookAlreadyInstalled,
    PartialGroup,
    InvalidHook,
}

/// Bounded dynamic hook-group preparation and cleanup failure.
#[derive(Debug, Error)]
pub enum LiveActionControlError {
    #[error("startup action hooks already own every configured route")]
    StartupHooksInstalled,
    #[error("one dynamic action hook group is already prepared")]
    GroupAlreadyPrepared,
    #[error("dynamic action hook group must contain at least one route")]
    EmptyGroup,
    #[error("dynamic action hook group contains duplicate route {route:?}")]
    DuplicateRoute { route: ShardKey },
    #[error("dynamic action hook group contains unknown route {route:?}")]
    UnknownRoute { route: ShardKey },
    #[error("dynamic route action hook {route:?} failed admission")]
    InvalidHook {
        route: ShardKey,
        #[source]
        error: RouteActionHookError,
    },
    #[error("runtime incarnation is closed")]
    RuntimeClosed,
    #[error("route shard {shard} is closed")]
    ShardClosed { shard: crate::ShardId },
    #[error("dynamic action hook control was cancelled")]
    Cancelled,
    #[error("dynamic action hook control exceeded its bounded deadline")]
    DeadlineExceeded,
    #[error("dynamic action hook control deadline cannot be represented")]
    DeadlineRange,
    #[error("dynamic action hook control channel is closed")]
    ControlClosed,
    #[error("dynamic action hook control bounded allocation failed")]
    Allocation,
    #[error("dynamic action hook retained-byte accounting overflowed")]
    RetainedSizeOverflow,
    #[error("dynamic action hook generation identity exhausted")]
    GenerationExhausted,
    #[error("dynamic action hook control violated deterministic shard ownership")]
    ShardInvariant,
    #[error("actor {shard} rejected dynamic action hook control: {reason:?}")]
    ActorRejected {
        shard: crate::ShardId,
        reason: LiveActionControlRejection,
    },
    #[error("actor {shard} acknowledged {observed} hooks but exact control required {expected}")]
    AcknowledgementMismatch {
        shard: crate::ShardId,
        expected: usize,
        observed: usize,
    },
    #[error("dynamic action hook preparation failed and bounded rollback remains incomplete")]
    RollbackIncomplete {
        generation: LiveActionHookGeneration,
    },
    #[error("dynamic action hook group state was lost before complete installation")]
    GroupStateLost,
    #[error("no dynamic action hook group is prepared")]
    NoPreparedGroup,
    #[error(transparent)]
    Activation(#[from] LiveActionHookActivationError),
    #[error(transparent)]
    Routing(#[from] crate::ShardRoutingError),
}

/// Authority-free reference to the exact committed market state exposed to one action hook call.
///
/// This value intentionally cannot create execution authority or report execution eligibility. It
/// borrows bounded canonical depth from route-owned state after the corresponding event commits.
#[derive(Debug)]
pub struct CommittedMarketReference<'event> {
    execution_terms: InstrumentExecutionTerms,
    bids: &'event [BookLevel],
    asks: &'event [BookLevel],
    observed_at: Timestamp,
}

impl<'market> CommittedMarketReference<'market> {
    /// Returns immutable revision-bound execution terms from the route reference master.
    pub const fn execution_terms(&self) -> InstrumentExecutionTerms {
        self.execution_terms
    }

    /// Returns bounded bid depth in best-to-worst order.
    pub const fn bids(&self) -> &[BookLevel] {
        self.bids
    }

    /// Returns bounded ask depth in best-to-worst order.
    pub const fn asks(&self) -> &[BookLevel] {
        self.asks
    }

    /// Returns the best bid, if the committed book has one.
    pub const fn best_bid(&self) -> Option<BookLevel> {
        self.bids.first().copied()
    }

    /// Returns the best ask, if the committed book has one.
    pub const fn best_ask(&self) -> Option<BookLevel> {
        self.asks.first().copied()
    }

    /// Returns the local trusted receive time of the committed event.
    pub const fn observed_at(&self) -> Timestamp {
        self.observed_at
    }

    #[allow(
        dead_code,
        reason = "route feature ownership constructs real bounded market references before action evaluation"
    )]
    pub(crate) fn try_new(
        execution_terms: InstrumentExecutionTerms,
        bids: &'market [BookLevel],
        asks: &'market [BookLevel],
        observed_at: Timestamp,
    ) -> Result<Self, LiveActionHookError> {
        if !bids
            .windows(2)
            .all(|levels| levels[0].price() > levels[1].price())
            || !asks
                .windows(2)
                .all(|levels| levels[0].price() < levels[1].price())
            || bids
                .first()
                .zip(asks.first())
                .is_some_and(|(bid, ask)| bid.price() >= ask.price())
        {
            return Err(LiveActionHookError::InvalidCommittedBook);
        }
        Ok(CommittedMarketReference {
            execution_terms,
            bids,
            asks,
            observed_at,
        })
    }
}

/// Borrowed context presented exactly once after a canonical live observation commits.
///
/// The context is authority-free and actor-scoped. It is deliberately neither cloneable,
/// serializable, sendable, nor shareable across threads.
#[derive(Debug)]
pub struct CommittedActionContext<'actor> {
    route: &'actor ShardKey,
    event: &'actor MarketEvent,
    assessment_id: &'actor QualificationAssessmentId,
    market: CommittedMarketReference<'actor>,
    features: &'actor dyn LiveFeatureView,
    not_send_or_sync: PhantomData<Rc<()>>,
}

impl<'actor> CommittedActionContext<'actor> {
    /// Returns the deterministic route that owns this committed state.
    pub const fn route(&self) -> &ShardKey {
        self.route
    }

    /// Returns the canonical event that committed before feature and action evaluation.
    pub const fn event(&self) -> &MarketEvent {
        self.event
    }

    /// Returns the retained qualification assessment identity.
    pub const fn assessment_id(&self) -> &QualificationAssessmentId {
        self.assessment_id
    }

    /// Returns the exact committed market reference.
    pub const fn market(&self) -> &CommittedMarketReference<'_> {
        &self.market
    }

    /// Returns the allocation-free, authority-free feature view owned by this route actor.
    pub const fn features(&self) -> &dyn LiveFeatureView {
        self.features
    }

    /// Returns the provider event timestamp when supplied by the source.
    pub fn source_timestamp(&self) -> Option<Timestamp> {
        event_provenance(self.event).source_timestamp()
    }

    /// Returns the trusted local receive time.
    pub fn received_at(&self) -> Timestamp {
        event_provenance(self.event).received_at()
    }

    /// Returns when the event became available to the local application.
    pub fn available_at(&self) -> Timestamp {
        event_provenance(self.event).available_at()
    }

    /// Returns when the canonical event was ingested.
    pub fn ingested_at(&self) -> Timestamp {
        event_provenance(self.event).ingested_at()
    }

    #[allow(
        dead_code,
        reason = "route feature ownership constructs the action context at the committed actor seam"
    )]
    pub(crate) fn try_new(
        route: &'actor ShardKey,
        event: &'actor MarketEvent,
        authority: &'actor AppliedObservationAuthority,
        market: CommittedMarketReference<'actor>,
        features: &'actor dyn LiveFeatureView,
    ) -> Result<Self, LiveActionHookError> {
        let provenance = event_provenance(event);
        if authority.quality != DataQuality::DirectVerified {
            return Err(LiveActionHookError::IneligibleQuality);
        }
        if provenance.binding() != &authority.binding {
            return Err(LiveActionHookError::EvidenceBindingMismatch);
        }
        if route.instrument() != market.execution_terms.instrument_id()
            || provenance.instrument_id() != Some(market.execution_terms.instrument_id())
            || provenance.venue_id() != Some(route.venue())
            || provenance.received_at() != market.observed_at
        {
            return Err(LiveActionHookError::RouteMismatch);
        }
        Ok(Self {
            route,
            event,
            assessment_id: &authority.assessment_id,
            market,
            features,
            not_send_or_sync: PhantomData,
        })
    }
}

/// Actor-scoped gateway to the processor's single-use current execution authority.
///
/// Only the live crate can construct this gateway. Holding it exclusively borrows the exact route
/// processor and applied authority, so hook evaluation cannot race another issuer for that route.
#[derive(Debug)]
pub struct CurrentAuthorityGate<'actor> {
    processor: &'actor mut InstrumentLiveProcessor<SystemTrustedClock>,
    applied: &'actor AppliedObservationAuthority,
    remaining_issues: usize,
    not_send_or_sync: PhantomData<Rc<()>>,
}

impl CurrentAuthorityGate<'_> {
    /// Issues one opaque capability after a fresh validation of the exact applied authority.
    ///
    /// # Errors
    ///
    /// Fails closed after any source, generation, route, state, status, clock, or deadline change.
    pub fn issue(&mut self) -> Result<LiveExecutionCapability, CurrentAuthorityGateError> {
        self.remaining_issues = self
            .remaining_issues
            .checked_sub(1)
            .ok_or(CurrentAuthorityGateError::IssueLimitExceeded)?;
        self.processor.issue(self.applied).map_err(Into::into)
    }

    /// Consumes one capability through the same processor-owned nonce registry.
    ///
    /// # Errors
    ///
    /// Rejects replay, transplant, revocation, stale state, or expiration.
    pub fn consume(
        &mut self,
        capability: LiveExecutionCapability,
    ) -> Result<ConsumedLiveAuthority, CurrentAuthorityGateError> {
        let consumed = self.processor.consume(capability)?;
        if consumed.assessment_id() != &self.applied.assessment_id
            || consumed.binding() != &self.applied.binding
        {
            return Err(CurrentAuthorityGateError::AuthorityTransplant);
        }
        Ok(consumed)
    }
}

#[allow(
    dead_code,
    reason = "the committed actor seam is the sole constructor after feature-state integration"
)]
pub(crate) const fn current_authority_gate<'actor>(
    processor: &'actor mut InstrumentLiveProcessor<SystemTrustedClock>,
    applied: &'actor AppliedObservationAuthority,
    maximum_issues: ActionAuthorityIssueLimit,
) -> CurrentAuthorityGate<'actor> {
    CurrentAuthorityGate {
        processor,
        applied,
        remaining_issues: maximum_issues.get(),
        not_send_or_sync: PhantomData,
    }
}

/// Bounded outcome reported by one action hook invocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActionHookDisposition {
    /// The strategy intentionally produced no order intent.
    NoAction,
    /// An intent was rejected or suppressed before dispatch.
    Suppressed,
    /// One approved order was admitted to bounded dispatch.
    Dispatched,
    /// Hook evaluation failed closed without dispatching an order.
    Failed,
}

/// Route-owned live action consumer.
///
/// Implementations execute synchronously on their instrument-owning actor. They must perform no
/// I/O, waiting, unbounded allocation, or unbounded queue writes. Any downstream dispatch must be a
/// nonblocking bounded admission operation.
pub trait LiveActionHook: Send + std::fmt::Debug {
    /// Evaluates one committed, currently executable observation.
    fn on_committed(
        &mut self,
        context: CommittedActionContext<'_>,
        authority: &mut CurrentAuthorityGate<'_>,
    ) -> ActionHookDisposition;

    /// Returns the configured maximum retained footprint of this hook-owned graph.
    ///
    /// # Errors
    ///
    /// Returns [`LiveActionHookError::RetainedSizeOverflow`] when exact accounting is not
    /// representable.
    fn retained_bytes(&self) -> Result<usize, LiveActionHookError>;

    /// Returns the validated per-observation capability bound enforced by the actor gate.
    fn maximum_authority_issues(&self) -> ActionAuthorityIssueLimit;
}

/// Action-context or retained-accounting failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LiveActionHookError {
    /// The committed observation did not retain execution-eligible quality.
    #[error("live action requires DirectVerified quality")]
    IneligibleQuality,
    /// Event provenance differs from the exact applied authority binding.
    #[error("committed event and applied authority evidence bindings differ")]
    EvidenceBindingMismatch,
    /// Route, event, and immutable execution terms do not identify the same market.
    #[error("committed event, route, and execution terms are inconsistent")]
    RouteMismatch,
    /// Route-owned committed depth was crossed, duplicated, or out of canonical order.
    #[error("committed action book is not canonical and uncrossed")]
    InvalidCommittedBook,
    /// Complete hook retained-size accounting overflowed `usize`.
    #[error("live action hook retained-byte accounting overflowed")]
    RetainedSizeOverflow,
}

/// Actor-scoped authority issuance or consumption failure.
#[derive(Debug, Error)]
pub enum CurrentAuthorityGateError {
    /// This hook exhausted its configured issue allowance for the observation.
    #[error("live action authority issue limit exceeded")]
    IssueLimitExceeded,
    /// A consumed capability did not belong to this exact applied observation.
    #[error("consumed live authority was transplanted across committed observations")]
    AuthorityTransplant,
    /// Processor-owned current authority validation failed closed.
    #[error(transparent)]
    Authority(#[from] AuthorityError),
}

fn event_provenance(event: &MarketEvent) -> &LiveProvenance {
    match event {
        MarketEvent::Trade(value) => value.provenance(),
        MarketEvent::Quote(value) => value.provenance(),
        MarketEvent::BookSnapshot(value) => value.provenance(),
        MarketEvent::BookDelta(value) => value.provenance(),
        MarketEvent::Auction(value) => value.provenance(),
        MarketEvent::TradingHalt(value) => value.provenance(),
        MarketEvent::InstrumentStatus(value) => value.provenance(),
        MarketEvent::CorporateAction(value) => value.provenance(),
    }
}
