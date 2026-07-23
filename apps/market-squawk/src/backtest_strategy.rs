//! Code-owned baseline research strategy and immutable build registration.

use std::sync::Arc;

use market_squawk_backtesting::{
    BacktestBuildReceipt, BacktestBuildRegistration, BacktestContext, BacktestStrategy,
    BacktestStrategyClass, BacktestStrategyFactory, BacktestStrategyInstance,
    BacktestStrategyRegistry,
};
use market_squawk_domain::{
    BasisPoints, ClientOrderId, DataQuality, OrderReasonCode, OrderSide, OrderType, QuantityLots,
    SourceIdentifier, TimeInForce,
};
use market_squawk_execution::{BoundedOrderIntents, OrderIntent, OrderIntentInput, StrategyError};

const BASELINE_BUILD_ID: &str = "market-squawk-baseline-buy-once-v1";
const BASELINE_STRATEGY_NAME: &str = "baseline-buy-once";
const BASELINE_ORDER_ID: &str = "6221dcb8-6518-4434-b3d4-054290358078";
const BASELINE_STRATEGY_ID: &str = "30a14e50-3d0d-4621-a4fc-d349ac42b2af";
const BASELINE_CONFIGURATION: &[u8] =
    br#"{"maximumSlippageBasisPoints":100,"quantityLots":1,"timeInForce":"day"}"#;
const BASELINE_SOURCE: &[u8] = include_bytes!("backtest_strategy.rs");

/// Returns the stable build identity accepted by the baseline production registry.
pub fn baseline_backtest_build_id() -> Result<SourceIdentifier, BacktestStrategyCompositionError> {
    SourceIdentifier::try_from(BASELINE_BUILD_ID)
        .map_err(|_| BacktestStrategyCompositionError::InvalidCodeOwnedIdentity)
}

/// Registers the caller-free baseline strategy against the exact running executable identity.
///
/// `executable_sha256` must be derived from the executable file opened by the application
/// composition before this function is called. The registry then binds the code-owned strategy
/// implementation, that executable identity, and the canonical fixed configuration into one
/// immutable build receipt.
pub fn production_backtest_strategy_registry(
    executable_sha256: [u8; 32],
) -> Result<BacktestStrategyRegistry, BacktestStrategyCompositionError> {
    if executable_sha256 == [0; 32] {
        return Err(BacktestStrategyCompositionError::InvalidExecutableIdentity);
    }
    let build_id = baseline_backtest_build_id()?;
    let strategy_name = SourceIdentifier::try_from(BASELINE_STRATEGY_NAME)
        .map_err(|_| BacktestStrategyCompositionError::InvalidCodeOwnedIdentity)?;
    let receipt = BacktestBuildReceipt::try_from_evidence(
        build_id,
        BacktestStrategyClass::RuleBased,
        strategy_name,
        BASELINE_SOURCE,
        &executable_sha256,
        BASELINE_CONFIGURATION,
    )?;
    BacktestStrategyRegistry::try_new(vec![BacktestBuildRegistration::new(
        receipt,
        Arc::new(BaselineBuyOnceFactory),
    )])
    .map_err(Into::into)
}

/// Code-owned baseline strategy composition failed before request admission.
#[derive(Debug, thiserror::Error)]
pub enum BacktestStrategyCompositionError {
    /// A source-controlled identity is invalid.
    #[error("baseline backtest strategy identity is invalid")]
    InvalidCodeOwnedIdentity,
    /// The exact executable digest was absent.
    #[error("baseline backtest executable identity is invalid")]
    InvalidExecutableIdentity,
    /// The backtest registry rejected code-owned build evidence.
    #[error("baseline backtest strategy registration failed: {0}")]
    Admission(#[from] market_squawk_backtesting::BacktestAdmissionError),
}

#[derive(Debug)]
struct BaselineBuyOnceFactory;

impl BacktestStrategyFactory for BaselineBuyOnceFactory {
    fn build(
        &self,
    ) -> Result<BacktestStrategyInstance, market_squawk_backtesting::BacktestAdmissionError> {
        Ok(BacktestStrategyInstance::RuleBased(Box::new(
            BaselineBuyOnce { emitted: false },
        )))
    }
}

#[derive(Debug)]
struct BaselineBuyOnce {
    emitted: bool,
}

impl BacktestStrategy for BaselineBuyOnce {
    fn on_observation(
        &mut self,
        context: &BacktestContext<'_>,
    ) -> Result<BoundedOrderIntents, StrategyError> {
        let mut intents = BoundedOrderIntents::new();
        if self.emitted {
            return Ok(intents);
        }
        intents.try_push(
            OrderIntent::try_new(OrderIntentInput {
                order_id: BASELINE_ORDER_ID
                    .parse()
                    .map_err(|_| StrategyError::Evaluation)?,
                client_order_id: ClientOrderId::try_from("baseline-buy-once")
                    .map_err(|_| StrategyError::Evaluation)?,
                strategy_id: BASELINE_STRATEGY_ID
                    .parse()
                    .map_err(|_| StrategyError::Evaluation)?,
                model_id: None,
                account_id: context.account_id(),
                execution_terms: context.execution_terms(),
                side: OrderSide::Buy,
                order_type: OrderType::Market,
                quantity: QuantityLots::new(1).map_err(|_| StrategyError::Evaluation)?,
                limit_price: None,
                stop_price: None,
                time_in_force: TimeInForce::Day,
                signal_at: context.decision_at(),
                expires_at: context
                    .decision_at()
                    .checked_add_nanos(1_000_000_000)
                    .map_err(|_| StrategyError::Evaluation)?,
                reason_codes: vec![
                    OrderReasonCode::try_from("baseline-buy-once")
                        .map_err(|_| StrategyError::Evaluation)?,
                ],
                maximum_slippage: BasisPoints::new(100),
                required_quality: DataQuality::DirectVerified,
            })
            .map_err(|_| StrategyError::Evaluation)?,
        )?;
        self.emitted = true;
        Ok(intents)
    }
}
