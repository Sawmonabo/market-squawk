//! Bounded strategy output and authority-free committed-market context.

use std::num::NonZeroU16;

use market_squawk_analytics::LiveFeatureView;
use market_squawk_domain::{MarketEvent, QualificationAssessmentId};
use market_squawk_live::ShardKey;
use market_squawk_modeling::{ModelFailure, ModelFailurePhase};
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
        let evidence = failure.audit();
        let phase = match evidence.phase() {
            ModelFailurePhase::Validation => StrategyNoActionPhase::Validation,
            ModelFailurePhase::Load => StrategyNoActionPhase::Load,
            ModelFailurePhase::Inference => StrategyNoActionPhase::Inference,
        };
        Self::from_no_action(StrategyNoAction::model(
            phase,
            evidence.source_code(),
            evidence.source_digest(),
        ))
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
    use market_squawk_modeling::{
        BundleError, InferenceError, ModelFailure, ModelInputError, ModelRegistryError,
        NativeBackendError,
    };

    use super::{BoundedOrderIntents, StrategyNoActionDomain, StrategyNoActionPhase};

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
}
