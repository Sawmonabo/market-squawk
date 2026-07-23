//! Bounded strategy output and authority-free committed-market context.

use std::{
    cmp::Ordering,
    mem::size_of,
    num::{NonZeroU16, NonZeroU32},
};

use market_squawk_analytics::{
    ExactFeatureRatio, FeatureKey, FeatureScalar, LiveFeatureView, RequiredLiveFeature,
};
use market_squawk_domain::{
    AccountId, BasisPoints, ClientOrderId, DataQuality, InstrumentExecutionTerms, MarketEvent,
    OrderId, OrderReasonCode, OrderSide, OrderType, PriceTicks, QualificationAssessmentId,
    QuantityLots, StrategyId, TimeInForce, Timestamp,
};
use market_squawk_live::ShardKey;
use market_squawk_modeling::{
    InferenceBackend, ModelFailure, ModelFailurePhase, ModelFeatureValue, ModelInput,
    ModelInputError, ModelOutput, NativeLinearBackend,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{ExecutionMarketReference, OrderIntent, OrderIntentInput};

/// Hard output bound kept equal to live's per-observation authority ceiling.
pub const MAX_STRATEGY_ORDER_INTENTS: usize =
    market_squawk_live::MAX_ACTION_AUTHORITY_ISSUES_PER_OBSERVATION;

/// The built-in paper signal has 250 milliseconds to clear central risk and dispatch.
pub const PAPER_BOOK_IMBALANCE_INTENT_LIFETIME_NANOS: i64 = 250_000_000;

/// The built-in paper signal accepts at most one percent adverse slippage.
pub const PAPER_BOOK_IMBALANCE_MAXIMUM_SLIPPAGE_BASIS_POINTS: i32 = 100;

/// The built-in paper signal emits exactly one instrument lot.
pub const PAPER_BOOK_IMBALANCE_ORDER_QUANTITY_LOTS: i64 = 1;

/// Closed producer domain for a typed strategy no-action fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyNoActionDomain {
    /// A locally admitted model bundle or native inference operation failed closed.
    Model,
}

/// Closed model lifecycle phase that caused no action to be emitted.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyNoActionPhase {
    /// Untrusted persisted relationships failed validation.
    Validation,
    /// A controlled read, immutable registry lookup, or backend load failed.
    Load,
    /// Finite input or pure native inference failed.
    Inference,
}

/// Immutable machine-readable no-action audit fact carrying no order authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct StrategyNoAction {
    domain: StrategyNoActionDomain,
    phase: StrategyNoActionPhase,
    source_code: NonZeroU16,
    source_digest: [u8; 32],
    audit_digest: [u8; 32],
}

impl StrategyNoAction {
    /// Constructs a model no-action fact from a typed nonzero code and exact source evidence.
    #[must_use]
    pub fn model(
        phase: StrategyNoActionPhase,
        source_code: NonZeroU16,
        source_digest: [u8; 32],
    ) -> Self {
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/strategy-no-action/v1");
        hash.update([1]);
        hash.update([match phase {
            StrategyNoActionPhase::Validation => 1,
            StrategyNoActionPhase::Load => 2,
            StrategyNoActionPhase::Inference => 3,
        }]);
        hash.update(source_code.get().to_be_bytes());
        hash.update(source_digest);
        Self {
            domain: StrategyNoActionDomain::Model,
            phase,
            source_code,
            source_digest,
            audit_digest: hash.finalize().into(),
        }
    }

    fn from_model_failure(failure: ModelFailure) -> Self {
        let evidence = failure.audit();
        let phase = match evidence.phase() {
            ModelFailurePhase::Validation => StrategyNoActionPhase::Validation,
            ModelFailurePhase::Load => StrategyNoActionPhase::Load,
            ModelFailurePhase::Inference => StrategyNoActionPhase::Inference,
        };
        Self::model(phase, evidence.source_code(), evidence.source_digest())
    }

    /// Returns the closed producer domain.
    #[must_use]
    pub const fn domain(self) -> StrategyNoActionDomain {
        self.domain
    }

    /// Returns the closed failure phase.
    #[must_use]
    pub const fn phase(self) -> StrategyNoActionPhase {
        self.phase
    }

    /// Returns the producer-defined nonzero typed error code.
    #[must_use]
    pub const fn source_code(self) -> NonZeroU16 {
        self.source_code
    }

    /// Returns the exact producer error evidence identity.
    #[must_use]
    pub const fn source_digest(self) -> [u8; 32] {
        self.source_digest
    }

    /// Returns the canonical execution-boundary audit identity.
    #[must_use]
    pub const fn audit_digest(self) -> [u8; 32] {
        self.audit_digest
    }
}

/// Borrowed, authority-free state presented to a strategy after market-update handoff.
#[derive(Debug)]
pub struct StrategyContext<'event> {
    route: &'event ShardKey,
    assessment_id: &'event QualificationAssessmentId,
    market: ExecutionMarketReference,
    features: &'event dyn LiveFeatureView,
}

impl<'event> StrategyContext<'event> {
    pub(crate) const fn from_committed(
        route: &'event ShardKey,
        assessment_id: &'event QualificationAssessmentId,
        market: ExecutionMarketReference,
        features: &'event dyn LiveFeatureView,
    ) -> Self {
        Self {
            route,
            assessment_id,
            market,
            features,
        }
    }

    pub const fn route(&self) -> &ShardKey {
        self.route
    }
    pub const fn assessment_id(&self) -> &QualificationAssessmentId {
        self.assessment_id
    }
    pub const fn market(&self) -> ExecutionMarketReference {
        self.market
    }
    pub const fn features(&self) -> &dyn LiveFeatureView {
        self.features
    }
}

/// Untrusted identities and exact guards for one route-owned built-in paper strategy.
#[derive(Debug)]
pub struct BookImbalancePaperStrategyConfigInput {
    /// Exact live route that owns this strategy instance.
    pub route: ShardKey,
    /// Paper account evaluated later by central portfolio and account risk.
    pub account_id: AccountId,
    /// Stable identity for the single order this run may produce.
    pub order_id: OrderId,
    /// Idempotency identity for the single order this run may produce.
    pub client_order_id: ClientOrderId,
    /// Stable identity of this transparent built-in strategy.
    pub strategy_id: StrategyId,
    /// Stable machine-readable rationale retained with the order.
    pub reason_code: OrderReasonCode,
    /// Largest ready top-of-book spread permitted for a signal.
    pub maximum_spread: PriceTicks,
    /// Positive exact bid-side top-of-book imbalance required for a signal.
    pub minimum_book_imbalance: ExactFeatureRatio,
}

/// Validated immutable configuration for one transparent route-owned paper strategy.
#[derive(Debug)]
pub struct BookImbalancePaperStrategyConfig {
    route: ShardKey,
    account_id: AccountId,
    order_id: OrderId,
    client_order_id: ClientOrderId,
    strategy_id: StrategyId,
    reason_code: OrderReasonCode,
    maximum_spread: PriceTicks,
    minimum_book_imbalance: ExactFeatureRatio,
    spread_key: FeatureKey,
    imbalance_key: FeatureKey,
    dynamic_retained_bytes: usize,
}

impl BookImbalancePaperStrategyConfig {
    /// Validates positive bounded signal guards and freezes the built-in feature identities.
    ///
    /// # Errors
    ///
    /// Rejects a nonpositive spread guard, an imbalance threshold outside `(0, 1]`, an impossible
    /// code-owned feature identity, or unrepresentable retained-byte accounting.
    pub fn try_new(
        input: BookImbalancePaperStrategyConfigInput,
    ) -> Result<Self, BookImbalancePaperStrategyConfigError> {
        if input.maximum_spread.get() <= 0 {
            return Err(BookImbalancePaperStrategyConfigError::NonPositiveMaximumSpread);
        }
        let minimum_numerator = u128::try_from(input.minimum_book_imbalance.numerator())
            .map_err(|_| BookImbalancePaperStrategyConfigError::NonPositiveBookImbalance)?;
        if minimum_numerator == 0 {
            return Err(BookImbalancePaperStrategyConfigError::NonPositiveBookImbalance);
        }
        if minimum_numerator > input.minimum_book_imbalance.denominator().get() {
            return Err(BookImbalancePaperStrategyConfigError::BookImbalanceAboveOne);
        }
        let spread_key =
            FeatureKey::try_new(RequiredLiveFeature::Spread.name(), NonZeroU32::MIN)
                .map_err(|_| BookImbalancePaperStrategyConfigError::BuiltInFeatureIdentity)?;
        let imbalance_key =
            FeatureKey::try_new(RequiredLiveFeature::BookImbalance.name(), NonZeroU32::MIN)
                .map_err(|_| BookImbalancePaperStrategyConfigError::BuiltInFeatureIdentity)?;
        let dynamic_retained_bytes = input
            .route
            .venue()
            .retained_bytes()
            .checked_add(input.client_order_id.retained_bytes())
            .and_then(|value| value.checked_add(input.reason_code.retained_bytes()))
            .and_then(|value| value.checked_add(spread_key.name().len()))
            .and_then(|value| value.checked_add(imbalance_key.name().len()))
            .ok_or(BookImbalancePaperStrategyConfigError::RetainedSize)?;
        Ok(Self {
            route: input.route,
            account_id: input.account_id,
            order_id: input.order_id,
            client_order_id: input.client_order_id,
            strategy_id: input.strategy_id,
            reason_code: input.reason_code,
            maximum_spread: input.maximum_spread,
            minimum_book_imbalance: input.minimum_book_imbalance,
            spread_key,
            imbalance_key,
            dynamic_retained_bytes,
        })
    }

    /// Returns the exact route that must own this strategy.
    pub const fn route(&self) -> &ShardKey {
        &self.route
    }

    /// Returns the paper account evaluated later by central risk.
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }

    /// Returns the identity of the sole order this strategy may produce.
    pub const fn order_id(&self) -> OrderId {
        self.order_id
    }

    /// Returns the stable built-in strategy identity.
    pub const fn strategy_id(&self) -> StrategyId {
        self.strategy_id
    }

    /// Returns the exact complete retained footprint of this immutable configuration.
    pub fn retained_bytes(&self) -> Result<usize, BookImbalancePaperStrategyConfigError> {
        size_of::<Self>()
            .checked_add(self.dynamic_retained_bytes)
            .ok_or(BookImbalancePaperStrategyConfigError::RetainedSize)
    }
}

/// Configuration failure for the transparent built-in paper strategy.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum BookImbalancePaperStrategyConfigError {
    /// The maximum accepted spread must be a positive tick count.
    #[error("paper strategy maximum spread must be positive")]
    NonPositiveMaximumSpread,
    /// The required buy-side imbalance must be strictly positive.
    #[error("paper strategy book-imbalance threshold must be positive")]
    NonPositiveBookImbalance,
    /// Top-of-book imbalance cannot exceed its exact mathematical maximum.
    #[error("paper strategy book-imbalance threshold must not exceed one")]
    BookImbalanceAboveOne,
    /// A code-owned required feature name or version became invalid.
    #[error("paper strategy built-in feature identity is invalid")]
    BuiltInFeatureIdentity,
    /// Exact retained-byte accounting overflowed.
    #[error("paper strategy configuration retained-size accounting failed")]
    RetainedSize,
}

/// Transparent bounded paper strategy driven only by ready spread and book-imbalance features.
///
/// This type owns no broker or risk authority. A qualifying event can create one typed intent;
/// current live authority, portfolio state, central risk, dispatch, and the paper adapter remain
/// mandatory downstream boundaries.
#[derive(Debug)]
pub struct BookImbalancePaperStrategy {
    config: BookImbalancePaperStrategyConfig,
    terminal: bool,
    retained_bytes: usize,
}

impl BookImbalancePaperStrategy {
    /// Creates a one-order strategy from already validated immutable route configuration.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError::RetainedSize`] if the exact retained footprint cannot be
    /// represented.
    pub fn try_new(config: BookImbalancePaperStrategyConfig) -> Result<Self, StrategyError> {
        let retained_bytes = size_of::<Self>()
            .checked_add(config.dynamic_retained_bytes)
            .ok_or(StrategyError::RetainedSize)?;
        Ok(Self {
            config,
            terminal: false,
            retained_bytes,
        })
    }

    fn signal_ready(&self, features: &dyn LiveFeatureView) -> Result<bool, StrategyError> {
        let Some(spread) = features
            .feature(&self.config.spread_key)
            .and_then(|value| value.ready_value())
        else {
            return Ok(false);
        };
        let FeatureScalar::PriceTicks(spread) = spread else {
            return Err(StrategyError::Evaluation);
        };
        if spread.get() < 0 {
            return Err(StrategyError::Evaluation);
        }
        if spread > self.config.maximum_spread {
            return Ok(false);
        }

        let Some(imbalance) = features
            .feature(&self.config.imbalance_key)
            .and_then(|value| value.ready_value())
        else {
            return Ok(false);
        };
        let FeatureScalar::ExactRatio(imbalance) = imbalance else {
            return Err(StrategyError::Evaluation);
        };
        Ok(nonnegative_ratio_cmp(imbalance, self.config.minimum_book_imbalance) != Ordering::Less)
    }

    fn try_emit(
        &mut self,
        execution_terms: InstrumentExecutionTerms,
        signal_at: Timestamp,
    ) -> Result<BoundedOrderIntents, StrategyError> {
        if self.terminal {
            return Ok(BoundedOrderIntents::new());
        }
        if execution_terms.instrument_id() != self.config.route.instrument() {
            return Err(StrategyError::Evaluation);
        }
        // A qualifying signal is terminal even if an unexpected downstream representation error
        // prevents construction. Repeated market events can never turn one route into a retry loop.
        self.terminal = true;
        let expires_at = signal_at
            .checked_add_nanos(PAPER_BOOK_IMBALANCE_INTENT_LIFETIME_NANOS)
            .map_err(|_| StrategyError::Evaluation)?;
        let intent = OrderIntent::try_new(OrderIntentInput {
            order_id: self.config.order_id,
            client_order_id: self.config.client_order_id.clone(),
            strategy_id: self.config.strategy_id,
            model_id: None,
            account_id: self.config.account_id,
            execution_terms,
            side: OrderSide::Buy,
            order_type: OrderType::Market,
            quantity: QuantityLots::new(PAPER_BOOK_IMBALANCE_ORDER_QUANTITY_LOTS)
                .map_err(|_| StrategyError::Evaluation)?,
            limit_price: None,
            stop_price: None,
            time_in_force: TimeInForce::ImmediateOrCancel,
            signal_at,
            expires_at,
            reason_codes: vec![self.config.reason_code.clone()],
            maximum_slippage: BasisPoints::new(PAPER_BOOK_IMBALANCE_MAXIMUM_SLIPPAGE_BASIS_POINTS),
            required_quality: DataQuality::DirectVerified,
        })
        .map_err(|_| StrategyError::Evaluation)?;
        let mut output = BoundedOrderIntents::new();
        output.try_push(intent)?;
        Ok(output)
    }
}

impl Strategy for BookImbalancePaperStrategy {
    fn on_market_event(
        &mut self,
        context: &StrategyContext<'_>,
        event: &MarketEvent,
    ) -> Result<BoundedOrderIntents, StrategyError> {
        if self.terminal {
            return Ok(BoundedOrderIntents::new());
        }
        if context.route() != self.config.route() {
            return Err(StrategyError::Evaluation);
        }
        if !matches!(
            event,
            MarketEvent::BookSnapshot(_) | MarketEvent::BookDelta(_)
        ) || !self.signal_ready(context.features())?
        {
            return Ok(BoundedOrderIntents::new());
        }
        self.try_emit(
            context.market().execution_terms(),
            context.market().observed_at(),
        )
    }

    fn retained_bytes(&self) -> Result<usize, StrategyError> {
        Ok(self.retained_bytes)
    }
}

fn nonnegative_ratio_cmp(left: ExactFeatureRatio, right: ExactFeatureRatio) -> Ordering {
    let Ok(mut left_numerator) = u128::try_from(left.numerator()) else {
        return Ordering::Less;
    };
    let Ok(mut right_numerator) = u128::try_from(right.numerator()) else {
        return Ordering::Greater;
    };
    let mut left_denominator = left.denominator().get();
    let mut right_denominator = right.denominator().get();
    let mut reversed = false;

    loop {
        let whole_ordering =
            (left_numerator / left_denominator).cmp(&(right_numerator / right_denominator));
        if whole_ordering != Ordering::Equal {
            return maybe_reverse(whole_ordering, reversed);
        }

        let left_remainder = left_numerator % left_denominator;
        let right_remainder = right_numerator % right_denominator;
        match (left_remainder == 0, right_remainder == 0) {
            (true, true) => return Ordering::Equal,
            (true, false) => return maybe_reverse(Ordering::Less, reversed),
            (false, true) => return maybe_reverse(Ordering::Greater, reversed),
            (false, false) => {
                left_numerator = left_denominator;
                left_denominator = left_remainder;
                right_numerator = right_denominator;
                right_denominator = right_remainder;
                reversed = !reversed;
            }
        }
    }
}

const fn maybe_reverse(ordering: Ordering, reversed: bool) -> Ordering {
    if reversed {
        ordering.reverse()
    } else {
        ordering
    }
}

/// Fixed-slot, non-cloneable strategy output with no unbounded queue or collection growth.
#[derive(Debug)]
pub struct BoundedOrderIntents {
    intents: [Option<OrderIntent>; MAX_STRATEGY_ORDER_INTENTS],
    len: u8,
    no_action: Option<StrategyNoAction>,
}

impl BoundedOrderIntents {
    /// Creates an empty bounded output.
    pub fn new() -> Self {
        Self {
            intents: std::array::from_fn(|_| None),
            len: 0,
            no_action: None,
        }
    }

    /// Creates an explicitly audited no-action output containing no order intent.
    #[must_use]
    pub fn from_no_action(no_action: StrategyNoAction) -> Self {
        Self {
            intents: std::array::from_fn(|_| None),
            len: 0,
            no_action: Some(no_action),
        }
    }

    /// Maps an exact model failure to an audited output with no order authority.
    #[must_use]
    pub fn from_model_failure(failure: ModelFailure) -> Self {
        Self::from_no_action(StrategyNoAction::from_model_failure(failure))
    }

    /// Appends one validated authority-free intent.
    pub fn try_push(&mut self, intent: OrderIntent) -> Result<(), StrategyError> {
        if self.no_action.is_some() {
            return Err(StrategyError::AuditedNoActionCannotContainIntent);
        }
        let index = usize::from(self.len);
        let slot = self
            .intents
            .get_mut(index)
            .ok_or(StrategyError::IntentCapacity)?;
        *slot = Some(intent);
        self.len = self
            .len
            .checked_add(1)
            .ok_or(StrategyError::IntentCapacity)?;
        Ok(())
    }

    pub const fn len(&self) -> usize {
        self.len as usize
    }
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the typed no-action fact, if this output represents an audited failure.
    #[must_use]
    pub const fn no_action(&self) -> Option<StrategyNoAction> {
        self.no_action
    }
}

impl Default for BoundedOrderIntents {
    fn default() -> Self {
        Self::new()
    }
}

impl IntoIterator for BoundedOrderIntents {
    type Item = OrderIntent;
    type IntoIter = BoundedOrderIntentIterator;

    fn into_iter(self) -> Self::IntoIter {
        BoundedOrderIntentIterator {
            intents: self.intents.into_iter(),
            remaining: self.len,
        }
    }
}

/// Owning fixed-slot intent iterator.
#[derive(Debug)]
pub struct BoundedOrderIntentIterator {
    intents: std::array::IntoIter<Option<OrderIntent>, MAX_STRATEGY_ORDER_INTENTS>,
    remaining: u8,
}

impl Iterator for BoundedOrderIntentIterator {
    type Item = OrderIntent;

    fn next(&mut self) -> Option<Self::Item> {
        while self.remaining > 0 {
            let intent = self.intents.next()?;
            self.remaining -= 1;
            if intent.is_some() {
                return intent;
            }
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = usize::from(self.remaining);
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for BoundedOrderIntentIterator {}

/// Route-owned bounded strategy contract.
pub trait Strategy: Send + std::fmt::Debug {
    fn on_market_event(
        &mut self,
        context: &StrategyContext<'_>,
        event: &MarketEvent,
    ) -> Result<BoundedOrderIntents, StrategyError>;

    fn retained_bytes(&self) -> Result<usize, StrategyError>;
}

/// Execution-owned admitted model path that produces one identity-bound result from live features.
pub trait ModelInferencePath: Send + std::fmt::Debug {
    /// Evaluates current allocation-free live feature state.
    fn infer_live(&mut self, features: &dyn LiveFeatureView) -> Result<ModelOutput, ModelFailure>;

    /// Returns the complete retained path charge.
    fn retained_bytes(&self) -> Result<usize, StrategyError>;
}

/// Strategy-specific mapping applied only after successful model inference.
pub trait ModelDecisionMapper: Send + std::fmt::Debug {
    /// Maps a successful exact model output to bounded authority-free intents.
    fn map(
        &mut self,
        context: &StrategyContext<'_>,
        event: &MarketEvent,
        output: &ModelOutput,
    ) -> Result<BoundedOrderIntents, StrategyError>;

    /// Returns the complete retained mapper charge.
    fn retained_bytes(&self) -> Result<usize, StrategyError>;
}

/// Native backend plus reusable coefficient-ordered live input slots.
#[derive(Debug)]
pub struct NativeModelInferencePath {
    backend: NativeLinearBackend,
    values: Box<[ModelFeatureValue]>,
    retained_bytes: usize,
}

impl NativeModelInferencePath {
    /// Creates one reusable live path from an already admitted native backend.
    pub fn try_new(backend: NativeLinearBackend) -> Result<Self, StrategyError> {
        let values: Box<[_]> = backend
            .metadata()
            .features()
            .iter()
            .map(ModelFeatureValue::from_binding)
            .collect();
        let dynamic_values = std::mem::size_of::<ModelFeatureValue>()
            .checked_mul(values.len())
            .ok_or(StrategyError::RetainedSize)?;
        let key_bytes = values.iter().try_fold(0usize, |total, value| {
            total
                .checked_add(value.key().name().len())
                .ok_or(StrategyError::RetainedSize)
        })?;
        let retained_bytes = std::mem::size_of::<Self>()
            .checked_add(backend.retained_bytes())
            .and_then(|total| total.checked_add(dynamic_values))
            .and_then(|total| total.checked_add(key_bytes))
            .ok_or(StrategyError::RetainedSize)?;
        Ok(Self {
            backend,
            values,
            retained_bytes,
        })
    }
}

impl ModelInferencePath for NativeModelInferencePath {
    fn infer_live(&mut self, features: &dyn LiveFeatureView) -> Result<ModelOutput, ModelFailure> {
        for value in &mut self.values {
            let scalar = features
                .feature(value.key())
                .and_then(|feature| feature.ready_value())
                .ok_or(ModelInputError::FeatureUnavailable)?;
            value.try_set_value(
                model_scalar(scalar).ok_or(ModelInputError::UnsupportedFeatureScalar)?,
            )?;
        }
        let input = ModelInput::try_new(self.backend.metadata(), &self.values)?;
        self.backend.infer(&input).map_err(ModelFailure::from)
    }

    fn retained_bytes(&self) -> Result<usize, StrategyError> {
        Ok(self.retained_bytes)
    }
}

/// Production strategy adapter that makes every model-path error an audited empty output.
#[derive(Debug)]
pub struct ModelStrategy {
    path: Result<Box<dyn ModelInferencePath>, ModelFailure>,
    mapper: Box<dyn ModelDecisionMapper>,
    retained_bytes: usize,
}

impl ModelStrategy {
    /// Owns either one admitted model path or its exact load/validation failure.
    pub fn try_new(
        path: Result<Box<dyn ModelInferencePath>, ModelFailure>,
        mapper: Box<dyn ModelDecisionMapper>,
    ) -> Result<Self, StrategyError> {
        let path_bytes = match &path {
            Ok(path) => path.retained_bytes()?,
            Err(_) => 0,
        };
        let mapper_bytes = mapper.retained_bytes()?;
        let retained_bytes = std::mem::size_of::<Self>()
            .checked_add(path_bytes)
            .and_then(|total| total.checked_add(mapper_bytes))
            .ok_or(StrategyError::RetainedSize)?;
        Ok(Self {
            path,
            mapper,
            retained_bytes,
        })
    }

    /// Runs only the owned model path and maps any error before decision mapping can run.
    pub fn evaluate_model(
        &mut self,
        features: &dyn LiveFeatureView,
    ) -> Result<ModelOutput, StrategyNoAction> {
        let output = match &mut self.path {
            Ok(path) => path.infer_live(features),
            Err(failure) => Err(*failure),
        };
        output.map_err(StrategyNoAction::from_model_failure)
    }
}

impl Strategy for ModelStrategy {
    fn on_market_event(
        &mut self,
        context: &StrategyContext<'_>,
        event: &MarketEvent,
    ) -> Result<BoundedOrderIntents, StrategyError> {
        let output = match self.evaluate_model(context.features()) {
            Ok(output) => output,
            Err(no_action) => return Ok(BoundedOrderIntents::from_no_action(no_action)),
        };
        self.mapper.map(context, event, &output)
    }

    fn retained_bytes(&self) -> Result<usize, StrategyError> {
        Ok(self.retained_bytes)
    }
}

fn model_scalar(value: FeatureScalar) -> Option<f64> {
    let value = match value {
        FeatureScalar::PriceTicks(value) => value.get() as f64,
        FeatureScalar::HalfTickPrice(value) => value.half_ticks() as f64 / 2.0,
        FeatureScalar::QuantityLots(value) => value.get() as f64,
        FeatureScalar::BasisPoints(value) => f64::from(value.get()),
        FeatureScalar::SignedInteger(value) => value as f64,
        FeatureScalar::UnsignedInteger(value) => value as f64,
        FeatureScalar::ExactRatio(value) => {
            value.numerator() as f64 / value.denominator().get() as f64
        }
        FeatureScalar::Statistical(value) => value.get(),
    };
    value.is_finite().then_some(value)
}

/// Closed strategy-boundary failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum StrategyError {
    #[error("strategy order-intent capacity is exhausted")]
    IntentCapacity,
    #[error("strategy evaluation failed closed")]
    Evaluation,
    #[error("strategy retained-size accounting failed")]
    RetainedSize,
    #[error("audited no-action output cannot also contain an order intent")]
    AuditedNoActionCannotContainIntent,
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroUsize};
    use std::str::FromStr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use market_squawk_analytics::{
        ExactFeatureRatio, FeatureError, FeatureKey, FeatureScalar, FeatureValue, LiveFeatureView,
        RequiredLiveFeature,
    };
    use market_squawk_domain::{
        AccountId, AggressorSide, AuthorizationBasis, BookLevel, BookSnapshotEvent,
        BookStateBinding, CanonicalStateDigest, CanonicalizationRule, ClientOrderId,
        ConnectionGeneration, CoverageStatus, Currency, DataQuality, Denomination, EvidenceDigest,
        InstrumentExecutionTerms, LiveEventClass, LiveEvidenceBinding, LiveProvenance, LotSize,
        MarketDepth, MarketEvent, MetadataRevision, OrderId, OrderReasonCode, OrderSide, OrderType,
        PayloadHashAlgorithm, PayloadReference, PriceTicks, ProviderChannel, ProviderProduct,
        QualificationAssessmentId, QuantityLots, RecordedLiveProvenanceInput, RuleVersion,
        SequenceNumber, SourceId, SourceIdentifier, StrategyId, TickSize, TimeInForce, Timestamp,
        TradeEvent, VenueId,
    };
    use market_squawk_modeling::{
        BundleError, InferenceError, ModelFailure, ModelInputError, ModelOutput,
        ModelRegistryError, NativeBackendError,
    };
    use rust_decimal::Decimal;

    use crate::live_hook::record_audited_no_action;
    use crate::{
        BookImbalancePaperStrategy, BookImbalancePaperStrategyConfig,
        BookImbalancePaperStrategyConfigError, BookImbalancePaperStrategyConfigInput,
        ExecutionAuditConfig, ExecutionAuditWriter, ExecutionMarketReference,
        StrategyNoActionDomain, StrategyNoActionPhase,
    };

    use super::{
        BoundedOrderIntents, ModelDecisionMapper, ModelInferencePath, ModelStrategy, Strategy,
        StrategyContext, StrategyError,
    };

    #[derive(Debug)]
    struct EmptyFeatureView;

    impl LiveFeatureView for EmptyFeatureView {
        fn feature(&self, _key: &FeatureKey) -> Option<&FeatureValue<FeatureScalar>> {
            None
        }

        fn retained_bytes(&self) -> Result<usize, FeatureError> {
            Ok(std::mem::size_of::<Self>())
        }
    }

    #[derive(Debug)]
    struct PaperSignalFeatureView {
        spread_key: FeatureKey,
        spread: FeatureValue<FeatureScalar>,
        imbalance_key: FeatureKey,
        imbalance: FeatureValue<FeatureScalar>,
    }

    impl PaperSignalFeatureView {
        fn try_ready(
            spread: i64,
            imbalance_numerator: i128,
            imbalance_denominator: u128,
        ) -> Result<Self, Box<dyn std::error::Error>> {
            let observed_at = Timestamp::from_unix_nanos(7);
            Ok(Self {
                spread_key: FeatureKey::try_new(
                    RequiredLiveFeature::Spread.name(),
                    NonZeroU32::MIN,
                )?,
                spread: FeatureValue::ready(
                    FeatureScalar::PriceTicks(PriceTicks::new(spread)),
                    observed_at,
                ),
                imbalance_key: FeatureKey::try_new(
                    RequiredLiveFeature::BookImbalance.name(),
                    NonZeroU32::MIN,
                )?,
                imbalance: FeatureValue::ready(
                    FeatureScalar::ExactRatio(ExactFeatureRatio::try_new(
                        imbalance_numerator,
                        imbalance_denominator,
                    )?),
                    observed_at,
                ),
            })
        }
    }

    impl LiveFeatureView for PaperSignalFeatureView {
        fn feature(&self, key: &FeatureKey) -> Option<&FeatureValue<FeatureScalar>> {
            if key == &self.spread_key {
                Some(&self.spread)
            } else if key == &self.imbalance_key {
                Some(&self.imbalance)
            } else {
                None
            }
        }

        fn retained_bytes(&self) -> Result<usize, FeatureError> {
            Ok(std::mem::size_of::<Self>()
                + self.spread_key.name().len()
                + self.imbalance_key.name().len())
        }
    }

    fn paper_strategy_config(
        maximum_spread_ticks: i64,
        imbalance_numerator: i128,
        imbalance_denominator: u128,
    ) -> Result<
        Result<BookImbalancePaperStrategyConfig, BookImbalancePaperStrategyConfigError>,
        Box<dyn std::error::Error>,
    > {
        Ok(BookImbalancePaperStrategyConfig::try_new(
            BookImbalancePaperStrategyConfigInput {
                route: market_squawk_live::ShardKey::new(
                    VenueId::try_from("paper-test")?,
                    "018f0000-0000-7000-8000-000000000001".parse()?,
                ),
                account_id: AccountId::from_str("50000000-0000-0000-0000-000000000001")?,
                order_id: OrderId::from_str("20000000-0000-0000-0000-000000000001")?,
                client_order_id: ClientOrderId::try_from("paper-book-imbalance-1")?,
                strategy_id: StrategyId::from_str("30000000-0000-0000-0000-000000000001")?,
                reason_code: OrderReasonCode::try_from("paper.book-imbalance.buy")?,
                maximum_spread: PriceTicks::new(maximum_spread_ticks),
                minimum_book_imbalance: ExactFeatureRatio::try_new(
                    imbalance_numerator,
                    imbalance_denominator,
                )?,
            },
        ))
    }

    fn paper_execution_terms() -> Result<InstrumentExecutionTerms, Box<dyn std::error::Error>> {
        let currency = Currency::try_from("USD")?;
        Ok(InstrumentExecutionTerms::try_new(
            "018f0000-0000-7000-8000-000000000001".parse()?,
            1_u64.try_into()?,
            TickSize::try_from_decimal(Decimal::new(1, 2))?,
            LotSize::try_from_decimal(Decimal::new(1, 2))?,
            currency,
            Denomination::Currency(currency),
            Decimal::ONE,
        )?)
    }

    fn paper_market_event(
        event_class: LiveEventClass,
    ) -> Result<MarketEvent, Box<dyn std::error::Error>> {
        let provenance = LiveProvenance::recorded(RecordedLiveProvenanceInput::new(
            paper_live_binding(event_class)?,
            Some(Timestamp::from_unix_nanos(900)),
            Timestamp::from_unix_nanos(1_000),
            Timestamp::from_unix_nanos(1_000),
            Timestamp::from_unix_nanos(1_000),
            DataQuality::DirectVerified,
            CoverageStatus::Sufficient,
            PayloadReference::SourceReference(SourceIdentifier::try_from("paper-event")?),
            SourceIdentifier::try_from("paper-assessment")?,
        ))?;
        Ok(match event_class {
            LiveEventClass::BookSnapshot => MarketEvent::BookSnapshot(BookSnapshotEvent::new(
                provenance,
                MarketDepth::PriceLevel,
                vec![BookLevel::new(PriceTicks::new(100), QuantityLots::new(3)?)?],
                vec![BookLevel::new(PriceTicks::new(101), QuantityLots::new(1)?)?],
                Some(SequenceNumber::new(1)),
            )?),
            LiveEventClass::Trade => MarketEvent::Trade(TradeEvent::new(
                provenance,
                PriceTicks::new(100),
                QuantityLots::new(1)?,
                AggressorSide::Buy,
            )?),
            _ => return Err("unsupported paper strategy event fixture".into()),
        })
    }

    fn paper_live_binding(
        event_class: LiveEventClass,
    ) -> Result<LiveEvidenceBinding, Box<dyn std::error::Error>> {
        let canonicalization = || -> Result<CanonicalizationRule, Box<dyn std::error::Error>> {
            Ok(CanonicalizationRule::new(
                SourceIdentifier::try_from("paper-book-state-v1")?,
                RuleVersion::new(1)?,
            ))
        };
        let state_digest = CanonicalStateDigest::new(
            EvidenceDigest::new(PayloadHashAlgorithm::Sha256, [2; 32]),
            canonicalization()?,
        );
        let book_state = if event_class.requires_book_state() {
            Some(BookStateBinding::new_with_snapshot_origin(
                MarketDepth::PriceLevel,
                SourceIdentifier::try_from("paper-book-state")?,
                state_digest.clone(),
                SourceIdentifier::try_from("paper-snapshot-state")?,
                CanonicalStateDigest::new(
                    EvidenceDigest::new(PayloadHashAlgorithm::Sha256, [3; 32]),
                    canonicalization()?,
                ),
            ))
        } else {
            None
        };
        Ok(LiveEvidenceBinding::new(
            SourceId::try_from("paper-direct")?,
            SourceIdentifier::try_from("paper-session")?,
            MetadataRevision::new(SourceIdentifier::try_from("paper-metadata-v1")?),
            AuthorizationBasis::new(SourceIdentifier::try_from("paper-authorization")?),
            VenueId::try_from("paper-test")?,
            "018f0000-0000-7000-8000-000000000001".parse()?,
            ConnectionGeneration::new(1)?,
            ProviderProduct::new(SourceIdentifier::try_from("paper-product")?),
            ProviderChannel::new(SourceIdentifier::try_from("paper-channel")?),
            event_class,
            SourceIdentifier::try_from("paper-event")?,
            EvidenceDigest::new(PayloadHashAlgorithm::Sha256, [1; 32]),
            state_digest,
            book_state,
        )?)
    }

    #[test]
    fn paper_strategy_configuration_requires_positive_bounded_guards()
    -> Result<(), Box<dyn std::error::Error>> {
        assert!(matches!(
            paper_strategy_config(0, 1, 5)?,
            Err(BookImbalancePaperStrategyConfigError::NonPositiveMaximumSpread)
        ));
        assert!(matches!(
            paper_strategy_config(5, 0, 1)?,
            Err(BookImbalancePaperStrategyConfigError::NonPositiveBookImbalance)
        ));
        assert!(matches!(
            paper_strategy_config(5, 6, 5)?,
            Err(BookImbalancePaperStrategyConfigError::BookImbalanceAboveOne)
        ));
        Ok(())
    }

    #[test]
    fn paper_strategy_boundary_enforces_route_book_features_and_one_shot_intent_bounds()
    -> Result<(), Box<dyn std::error::Error>> {
        let config = paper_strategy_config(5, 1, 5)??;
        let route = config.route().clone();
        let account_id = config.account_id();
        let order_id = config.order_id();
        let strategy_id = config.strategy_id();
        let mut strategy = BookImbalancePaperStrategy::try_new(config)?;
        let retained = strategy.retained_bytes()?;

        let signal_at = Timestamp::from_unix_nanos(1_000_000_000);
        let terms = paper_execution_terms()?;
        let bids = [BookLevel::new(PriceTicks::new(100), QuantityLots::new(3)?)?];
        let asks = [BookLevel::new(PriceTicks::new(101), QuantityLots::new(1)?)?];
        let market = ExecutionMarketReference::for_strategy_test(terms, signal_at, &bids, &asks);
        let assessment = QualificationAssessmentId::new(SourceIdentifier::try_from(
            "paper-strategy-assessment",
        )?);
        let ready_features = PaperSignalFeatureView::try_ready(5, 1, 3)?;
        let book = paper_market_event(LiveEventClass::BookSnapshot)?;

        let wrong_route = market_squawk_live::ShardKey::new(
            VenueId::try_from("wrong-paper-route")?,
            route.instrument(),
        );
        let wrong_context =
            StrategyContext::from_committed(&wrong_route, &assessment, market, &ready_features);
        assert!(matches!(
            strategy.on_market_event(&wrong_context, &book),
            Err(StrategyError::Evaluation)
        ));

        let context = StrategyContext::from_committed(&route, &assessment, market, &ready_features);
        let trade = paper_market_event(LiveEventClass::Trade)?;
        assert!(strategy.on_market_event(&context, &trade)?.is_empty());

        let below_threshold = PaperSignalFeatureView::try_ready(5, 1, 6)?;
        let below_context =
            StrategyContext::from_committed(&route, &assessment, market, &below_threshold);
        assert!(strategy.on_market_event(&below_context, &book)?.is_empty());

        let output = strategy.on_market_event(&context, &book)?;
        assert_eq!(output.len(), 1);
        let intent = output
            .into_iter()
            .next()
            .ok_or("ready paper signal did not produce its one bounded intent")?;
        assert_eq!(intent.account_id(), account_id);
        assert_eq!(intent.order_id(), order_id);
        assert_eq!(intent.strategy_id(), strategy_id);
        assert_eq!(intent.side(), OrderSide::Buy);
        assert_eq!(intent.order_type(), OrderType::Market);
        assert_eq!(intent.quantity().get(), 1);
        assert_eq!(intent.time_in_force(), TimeInForce::ImmediateOrCancel);
        assert_eq!(intent.signal_at(), signal_at);
        assert_eq!(
            intent.expires_at(),
            signal_at.checked_add_nanos(250_000_000)?
        );
        assert_eq!(intent.maximum_slippage().get(), 100);
        assert_eq!(intent.reason_codes().len(), 1);
        assert_eq!(
            intent.reason_codes()[0].as_str(),
            "paper.book-imbalance.buy"
        );

        assert!(strategy.on_market_event(&context, &book)?.is_empty());
        assert_eq!(strategy.retained_bytes()?, retained);
        Ok(())
    }

    #[derive(Debug)]
    struct FailingInferencePath;

    impl ModelInferencePath for FailingInferencePath {
        fn infer_live(
            &mut self,
            _features: &dyn LiveFeatureView,
        ) -> Result<ModelOutput, ModelFailure> {
            Err(ModelFailure::from(InferenceError::NonFiniteComputation))
        }

        fn retained_bytes(&self) -> Result<usize, StrategyError> {
            Ok(std::mem::size_of::<Self>())
        }
    }

    #[derive(Debug)]
    struct UnreachableDecisionMapper {
        called: Arc<AtomicBool>,
    }

    impl ModelDecisionMapper for UnreachableDecisionMapper {
        fn map(
            &mut self,
            _context: &StrategyContext<'_>,
            _event: &MarketEvent,
            _output: &ModelOutput,
        ) -> Result<BoundedOrderIntents, StrategyError> {
            self.called.store(true, Ordering::Release);
            Ok(BoundedOrderIntents::new())
        }

        fn retained_bytes(&self) -> Result<usize, StrategyError> {
            Ok(std::mem::size_of::<Self>())
        }
    }

    #[test]
    fn every_model_failure_plane_becomes_audited_no_action()
    -> Result<(), Box<dyn std::error::Error>> {
        let failures = [
            ModelFailure::from(BundleError::MetadataHashMismatch),
            ModelFailure::from(ModelRegistryError::RegistryFull),
            ModelFailure::from(ModelInputError::FeatureShapeMismatch),
            ModelFailure::from(NativeBackendError::UnsupportedBundleFormat),
            ModelFailure::from(InferenceError::NonFiniteComputation),
        ];

        for failure in failures {
            let output = BoundedOrderIntents::from_model_failure(failure);
            assert!(output.is_empty());
            assert_eq!(output.len(), 0);
            let audit = output
                .no_action()
                .ok_or_else(|| std::io::Error::other("model failure must be audited"))?;
            assert_eq!(audit.domain(), StrategyNoActionDomain::Model);
            assert_ne!(audit.source_code().get(), 0);
            assert_ne!(audit.audit_digest(), [0; 32]);
        }
        Ok(())
    }

    #[test]
    fn model_failure_phases_remain_distinct_at_strategy_boundary() {
        let cases = [
            (
                ModelFailure::from(BundleError::InvalidNormalizer),
                StrategyNoActionPhase::Validation,
            ),
            (
                ModelFailure::from(ModelRegistryError::RegistryUnavailable),
                StrategyNoActionPhase::Load,
            ),
            (
                ModelFailure::from(InferenceError::BundleMismatch),
                StrategyNoActionPhase::Inference,
            ),
        ];

        for (failure, expected) in cases {
            let output = BoundedOrderIntents::from_model_failure(failure);
            assert_eq!(
                output.no_action().map(|audit| audit.phase()),
                Some(expected)
            );
        }
    }

    #[test]
    fn failing_backend_reaches_live_hook_as_bounded_audited_no_action()
    -> Result<(), Box<dyn std::error::Error>> {
        let mapper_called = Arc::new(AtomicBool::new(false));
        let mut strategy = ModelStrategy::try_new(
            Ok(Box::new(FailingInferencePath)),
            Box::new(UnreachableDecisionMapper {
                called: Arc::clone(&mapper_called),
            }),
        )?;
        let no_action = strategy
            .evaluate_model(&EmptyFeatureView)
            .err()
            .ok_or_else(|| std::io::Error::other("failing backend produced a model output"))?;
        let intents = BoundedOrderIntents::from_no_action(no_action);
        assert!(!mapper_called.load(Ordering::Acquire));

        let (audit, mut reader) = ExecutionAuditWriter::try_new(ExecutionAuditConfig {
            maximum_records: NonZeroUsize::MIN,
            maximum_bytes: NonZeroU32::new(64 * 1024)
                .ok_or_else(|| std::io::Error::other("audit bytes must be nonzero"))?,
        })?;
        assert_eq!(
            record_audited_no_action(&audit, &intents, Timestamp::from_unix_nanos(7))?,
            Some(market_squawk_live::ActionHookDisposition::NoAction)
        );
        let record = reader
            .try_next_record()?
            .ok_or_else(|| std::io::Error::other("live hook did not forward model no-action"))?;
        let event = record
            .strategy_no_action_event()
            .ok_or_else(|| std::io::Error::other("unexpected execution audit record"))?;
        assert_eq!(Some(event.no_action()), intents.no_action());
        assert_eq!(event.observed_at(), Timestamp::from_unix_nanos(7));
        assert!(reader.try_next_record()?.is_none());
        Ok(())
    }
}
