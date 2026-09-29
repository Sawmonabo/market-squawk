//! Virtual-only purpose through the same risk, reservations, dispatcher, and paper market sink.

use crate::{
    ExecutionDispatcherHandle, ExecutionMarketReference, ExecutionMarketSink,
    ExecutionMarketUpdate, RiskOutcome, RiskService, Strategy,
};
use market_squawk_domain::{LiveEvidenceBinding, QualificationAssessmentId, Timestamp};
use market_squawk_live::virtual_paper::{
    ConsumedVirtualPaperAuthority, VirtualPaperError, VirtualPaperEvidence,
};
use market_squawk_live::{
    ActionHookDisposition, ConsumedLiveAuthority, ConsumedLiveEvidence, ShardKey,
};
use std::sync::Arc;
use thiserror::Error;

/// A real admitted quote used only to value the atomic source-action reconciliation.
/// Construction preserves a revocable original-source guard; no caller-authored price exists.
#[derive(Debug)]
pub struct VirtualPaperValuationMark {
    update: ExecutionMarketUpdate,
    currentness: market_squawk_live::virtual_paper::VirtualPaperCurrentness,
}
impl VirtualPaperValuationMark {
    pub fn from_authority(
        authority: &ConsumedVirtualPaperAuthority,
    ) -> Result<Self, VirtualPaperError> {
        authority.validate_current()?;
        Ok(Self {
            update: ExecutionMarketUpdate::from_virtual_paper(
                authority,
                ExecutionMarketReference::from_virtual_paper(authority),
            ),
            currentness: authority.currentness(),
        })
    }
    pub fn is_current(&self) -> bool {
        self.currentness.is_current()
    }
    pub const fn update(&self) -> ExecutionMarketUpdate {
        self.update
    }
}

/// Closed private authority union. Virtual admission has no conversion to live authority.
#[derive(Debug)]
pub(crate) enum ExecutionAuthority {
    Live(ConsumedLiveAuthority),
    VirtualPaper(ConsumedVirtualPaperAuthority),
}
impl ExecutionAuthority {
    pub(crate) const fn is_virtual_paper(&self) -> bool {
        matches!(self, Self::VirtualPaper(_))
    }
    pub(crate) fn validate_current(&self) -> Result<(), ExecutionAuthorityError> {
        match self {
            Self::Live(value) => value
                .validate_current()
                .map_err(ExecutionAuthorityError::Live),
            Self::VirtualPaper(value) => value
                .validate_current()
                .map_err(ExecutionAuthorityError::VirtualPaper),
        }
    }
    pub(crate) const fn assessment_id(&self) -> &QualificationAssessmentId {
        match self {
            Self::Live(value) => value.assessment_id(),
            Self::VirtualPaper(value) => value.assessment_id(),
        }
    }
    pub(crate) const fn binding(&self) -> &LiveEvidenceBinding {
        match self {
            Self::Live(value) => value.binding(),
            Self::VirtualPaper(value) => value.binding(),
        }
    }
    pub(crate) const fn binding_digest(&self) -> [u8; 32] {
        match self {
            Self::Live(value) => value.binding_digest(),
            Self::VirtualPaper(value) => value.binding_digest(),
        }
    }
    pub(crate) const fn valid_until(&self) -> Timestamp {
        match self {
            Self::Live(value) => value.valid_until(),
            Self::VirtualPaper(value) => value.valid_until(),
        }
    }
    pub(crate) fn into_evidence(self) -> ExecutionEvidence {
        match self {
            Self::Live(value) => ExecutionEvidence::Live(value.into_evidence()),
            Self::VirtualPaper(value) => ExecutionEvidence::VirtualPaper(value.into_evidence()),
        }
    }
}
#[derive(Debug, Error)]
pub(crate) enum ExecutionAuthorityError {
    #[error(transparent)]
    Live(market_squawk_live::AuthorityError),
    #[error(transparent)]
    VirtualPaper(VirtualPaperError),
}
#[derive(Debug)]
pub(crate) enum ExecutionEvidence {
    Live(ConsumedLiveEvidence),
    VirtualPaper(VirtualPaperEvidence),
}
impl ExecutionEvidence {
    pub(crate) const fn is_virtual_paper(&self) -> bool {
        matches!(self, Self::VirtualPaper(_))
    }
    pub(crate) const fn assessment_id(&self) -> &QualificationAssessmentId {
        match self {
            Self::Live(value) => value.assessment_id(),
            Self::VirtualPaper(value) => value.assessment_id(),
        }
    }
    pub(crate) const fn binding(&self) -> &LiveEvidenceBinding {
        match self {
            Self::Live(value) => value.binding(),
            Self::VirtualPaper(value) => value.binding(),
        }
    }
    pub(crate) const fn binding_digest(&self) -> [u8; 32] {
        match self {
            Self::Live(value) => value.binding_digest(),
            Self::VirtualPaper(value) => value.binding_digest(),
        }
    }
    pub(crate) const fn valid_until(&self) -> Timestamp {
        match self {
            Self::Live(value) => value.valid_until(),
            Self::VirtualPaper(value) => value.valid_until(),
        }
    }
}

/// Single-route manual virtual execution consumer. The caller owns its bounded source lifecycle;
/// this synchronous consumer performs no provider I/O and never creates a second execution ledger.
#[derive(Debug)]
pub struct ExecutionVirtualPaperHook {
    route: ShardKey,
    last_published_quote: Option<[u8; 32]>,
    strategy: Box<dyn Strategy>,
    risk: RiskService,
    dispatcher: ExecutionDispatcherHandle,
    market_sink: Arc<dyn ExecutionMarketSink>,
}
impl ExecutionVirtualPaperHook {
    pub fn try_new(
        route: ShardKey,
        strategy: Box<dyn Strategy>,
        risk: RiskService,
        dispatcher: ExecutionDispatcherHandle,
        market_sink: Arc<dyn ExecutionMarketSink>,
    ) -> Result<Self, crate::StrategyError> {
        if !strategy.supports_virtual_paper() {
            return Err(crate::StrategyError::Evaluation);
        }
        Ok(Self {
            route,
            last_published_quote: None,
            strategy,
            risk,
            dispatcher,
            market_sink,
        })
    }
    /// Complete fixed hook graph retained outside the existing shared risk/dispatch owners.
    pub fn retained_bytes(&self) -> Result<usize, crate::StrategyError> {
        use crate::Strategy as _;
        std::mem::size_of::<Self>()
            .checked_add(self.route.venue().retained_bytes())
            .and_then(|bytes| bytes.checked_add(self.strategy.retained_bytes().ok()?))
            .and_then(|bytes| bytes.checked_add(self.risk.retained_bytes()))
            .and_then(|bytes| bytes.checked_add(self.dispatcher.retained_bytes()))
            .and_then(|bytes| bytes.checked_add(self.market_sink.retained_bytes().ok()?))
            .ok_or(crate::StrategyError::RetainedSize)
    }

    /// Original quote first enters the same bounded paper market queue; only then may a single
    /// target-bound draft enter shared risk. Action coverage and portfolio/native sequence fences
    /// remain enforced by the existing paper worker and portfolio read capability.
    pub fn on_quote(&mut self, authority: ConsumedVirtualPaperAuthority) -> ActionHookDisposition {
        if authority.validate_current().is_err()
            || authority.binding().instrument_id() != Some(self.route.instrument())
            || authority.binding().venue_id() != self.route.venue()
        {
            return ActionHookDisposition::Failed;
        }
        let market = ExecutionMarketReference::from_virtual_paper(&authority);
        let update = ExecutionMarketUpdate::from_virtual_paper(&authority, market);
        if self.last_published_quote != Some(authority.quote_digest()) {
            if self.market_sink.try_publish(update).is_err() {
                return ActionHookDisposition::Failed;
            }
            self.last_published_quote = Some(authority.quote_digest());
        }
        let intents = match self.strategy.on_virtual_paper_quote(&self.route, market) {
            Ok(intents) => intents,
            Err(_) => return ActionHookDisposition::Failed,
        };
        if intents.len() > 1 {
            return ActionHookDisposition::Failed;
        }
        let Some(intent) = intents.into_iter().next() else {
            return ActionHookDisposition::NoAction;
        };
        match self.risk.evaluate_virtual_paper(authority, intent, &market) {
            RiskOutcome::Rejected(_) => ActionHookDisposition::Suppressed,
            RiskOutcome::Approved(approval) => {
                if self.dispatcher.try_submit(approval).is_ok() {
                    ActionHookDisposition::Dispatched
                } else {
                    ActionHookDisposition::Suppressed
                }
            }
        }
    }
}
