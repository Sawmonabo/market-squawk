//! Actor-scoped live-action contracts with no caller-mintable authority surface.

use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::rc::Rc;

use market_squawk_analytics::{LiveFeatureView, RequiredLiveFeature};
use market_squawk_domain::{
    BookLevel, DataQuality, InstrumentExecutionTerms, LiveProvenance, MarketEvent,
    QualificationAssessmentId, Timestamp,
};
use thiserror::Error;

use crate::authority::{AppliedObservationAuthority, SystemTrustedClock};
use crate::processor::InstrumentLiveProcessor;
use crate::{AuthorityError, ConsumedLiveAuthority, LiveExecutionCapability, ShardKey};

/// Hard maximum capabilities one route hook may request for one committed observation.
pub const MAX_ACTION_AUTHORITY_ISSUES_PER_OBSERVATION: usize = 64;

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
        .ok_or(RouteActionHookError::RetainedSizeOverflow)
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
