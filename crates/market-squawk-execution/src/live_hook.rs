//! Execution-owned live action hook with mandatory market, risk, and dispatch ordering.

use std::sync::Arc;

use market_squawk_live::{
    ActionAuthorityIssueLimit, ActionHookDisposition, CommittedActionContext, CurrentAuthorityGate,
    LiveActionHook, LiveActionHookError,
};
use thiserror::Error;

use crate::{
    ExecutionDispatcherHandle, ExecutionMarketReference, ExecutionMarketSink,
    ExecutionMarketUpdate, RiskOutcome, RiskService, Strategy, StrategyContext,
};

/// Route-owned execution graph invoked synchronously at the committed actor point.
#[derive(Debug)]
pub struct ExecutionLiveActionHook {
    strategy: Box<dyn Strategy>,
    risk: RiskService,
    dispatcher: ExecutionDispatcherHandle,
    market_sink: Arc<dyn ExecutionMarketSink>,
    maximum_intents: ActionAuthorityIssueLimit,
    declared_retained_bytes: usize,
}

impl ExecutionLiveActionHook {
    /// Validates one bounded route graph and fixes its exact per-observation authority ceiling.
    pub fn try_new(
        strategy: Box<dyn Strategy>,
        risk: RiskService,
        dispatcher: ExecutionDispatcherHandle,
        market_sink: Arc<dyn ExecutionMarketSink>,
        maximum_intents: ActionAuthorityIssueLimit,
    ) -> Result<Self, ExecutionLiveActionHookError> {
        let declared_retained_bytes =
            hook_retained_bytes(strategy.as_ref(), &risk, &dispatcher, market_sink.as_ref())?;
        Ok(Self {
            strategy,
            risk,
            dispatcher,
            market_sink,
            maximum_intents,
            declared_retained_bytes,
        })
    }
}

impl LiveActionHook for ExecutionLiveActionHook {
    fn on_committed(
        &mut self,
        context: CommittedActionContext<'_>,
        authority: &mut CurrentAuthorityGate<'_>,
    ) -> ActionHookDisposition {
        let market = ExecutionMarketReference::from_committed_context(&context);
        let update = ExecutionMarketUpdate::from_committed_context(&context, market);
        if self.market_sink.try_publish(update).is_err() {
            return ActionHookDisposition::Failed;
        }
        let strategy_context = StrategyContext::from_committed(
            context.route(),
            context.assessment_id(),
            market,
            context.features(),
        );
        let intents = match self
            .strategy
            .on_market_event(&strategy_context, context.event())
        {
            Ok(intents) => intents,
            Err(_) => return ActionHookDisposition::Failed,
        };
        if intents.is_empty() {
            return ActionHookDisposition::NoAction;
        }
        if intents.len() > self.maximum_intents.get() {
            return ActionHookDisposition::Failed;
        }

        let mut dispatched = false;
        let mut suppressed = false;
        for intent in intents {
            let capability = match authority.issue() {
                Ok(capability) => capability,
                Err(_) => return ActionHookDisposition::Failed,
            };
            match self.risk.evaluate(authority, capability, intent, &market) {
                RiskOutcome::Rejected(_) => suppressed = true,
                RiskOutcome::Approved(approval) => {
                    if self.dispatcher.try_submit(approval).is_ok() {
                        dispatched = true;
                    } else {
                        suppressed = true;
                    }
                }
            }
        }
        if dispatched {
            ActionHookDisposition::Dispatched
        } else if suppressed {
            ActionHookDisposition::Suppressed
        } else {
            ActionHookDisposition::NoAction
        }
    }

    fn retained_bytes(&self) -> Result<usize, LiveActionHookError> {
        let observed = hook_retained_bytes(
            self.strategy.as_ref(),
            &self.risk,
            &self.dispatcher,
            self.market_sink.as_ref(),
        )
        .map_err(|_| LiveActionHookError::RetainedSizeOverflow)?;
        if observed != self.declared_retained_bytes {
            return Err(LiveActionHookError::RetainedSizeOverflow);
        }
        Ok(observed)
    }

    fn maximum_authority_issues(&self) -> ActionAuthorityIssueLimit {
        self.maximum_intents
    }
}

fn hook_retained_bytes(
    strategy: &dyn Strategy,
    risk: &RiskService,
    dispatcher: &ExecutionDispatcherHandle,
    market_sink: &dyn ExecutionMarketSink,
) -> Result<usize, ExecutionLiveActionHookError> {
    std::mem::size_of::<ExecutionLiveActionHook>()
        .checked_add(strategy.retained_bytes()?)
        .and_then(|value| value.checked_add(risk.retained_bytes()))
        .and_then(|value| value.checked_add(dispatcher.retained_bytes()))
        .and_then(|value| value.checked_add(market_sink.retained_bytes().ok()?))
        .ok_or(ExecutionLiveActionHookError::RetainedSize)
}

/// Hook construction failure before live runtime ownership transfer.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ExecutionLiveActionHookError {
    #[error(transparent)]
    Strategy(#[from] crate::StrategyError),
    #[error("execution action-hook retained-size accounting failed")]
    RetainedSize,
}
