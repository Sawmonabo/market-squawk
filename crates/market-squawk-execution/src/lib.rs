//! Risk-enforced execution contracts with no public authority or dispatch bypass.

mod account;
mod adapter;
mod clock;
mod intent;
mod limits;
mod risk;

pub use account::{
    AccountBootstrap, AccountCoordinatorConfig, AccountCoordinatorError, AccountReservationError,
    AccountRiskCoordinator, AccountRiskReservation,
};
pub use adapter::{
    CancelReceipt, CancelStatus, ExecutionAdapterError, ExecutionReceipt, ExecutionState,
};
pub use intent::{
    MAX_INTENT_SLIPPAGE_BASIS_POINTS, MAX_ORDER_REASON_CODES, OrderIntent, OrderIntentDigest,
    OrderIntentError, OrderIntentInput,
};
pub use limits::{
    AccountRiskViolation, MAX_RISK_INSTRUMENTS, RiskLimits, RiskLimitsError, RiskLimitsInput,
};
pub use risk::{
    MarketRiskInput, MarketRiskInputError, PreAuthorityRiskOutcome, RiskRejection,
    RiskRejectionCode, RiskService,
};
