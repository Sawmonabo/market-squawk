//! Bounded strategy output and authority-free committed-market context.

use std::num::NonZeroU16;

use market_squawk_analytics::{FeatureScalar, LiveFeatureView};
use market_squawk_domain::{MarketEvent, QualificationAssessmentId};
use market_squawk_live::ShardKey;
use market_squawk_modeling::{
    InferenceBackend, ModelFailure, ModelFailurePhase, ModelFeatureValue, ModelInput,
    ModelInputError, ModelOutput, NativeLinearBackend,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{ExecutionMarketReference, OrderIntent};

/// Hard output bound kept equal to live's per-observation authority ceiling.
pub const MAX_STRATEGY_ORDER_INTENTS: usize =
    market_squawk_live::MAX_ACTION_AUTHORITY_ISSUES_PER_OBSERVATION;

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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use market_squawk_analytics::{
        FeatureError, FeatureKey, FeatureScalar, FeatureValue, LiveFeatureView,
    };
    use market_squawk_domain::{MarketEvent, Timestamp};
    use market_squawk_modeling::{
        BundleError, InferenceError, ModelFailure, ModelInputError, ModelOutput,
        ModelRegistryError, NativeBackendError,
    };

    use crate::live_hook::record_audited_no_action;
    use crate::{
        ExecutionAuditConfig, ExecutionAuditWriter, StrategyNoActionDomain, StrategyNoActionPhase,
    };

    use super::{
        BoundedOrderIntents, ModelDecisionMapper, ModelInferencePath, ModelStrategy,
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
