//! Deterministic bounded realistic paper execution.

mod adapter;
mod audit;
mod config;
mod fees;
mod latency;
mod ledger;
mod matching;
mod order;
mod session;
mod slippage;
mod snapshot;
mod state;
mod worker;

pub use adapter::{
    PaperControlContext, PaperControlError, PaperExecutionAdapter, PaperExecutionRuntime,
    PaperMarketIngress, PaperStartError,
};
pub use audit::{PaperAuditKind, PaperAuditReader, PaperAuditRecord};
pub use config::{PaperConfigError, PaperExecutionConfig, PaperExecutionConfigInput};
pub use fees::{FeeError, FeeSchedule, LiquidityRole};
pub use ledger::{
    PaperAccountBootstrap, PaperAccountRiskSnapshot, PaperCashBalance, PaperExposureValuation,
    PaperFill, PaperLedger, PaperLedgerConfig, PaperLedgerError, PaperPosition,
};
pub use session::{
    MAX_PAPER_VENUE_SESSIONS, PaperSessionCalendarError, PaperVenueSession,
    PaperVenueSessionCalendar,
};
pub use snapshot::{
    PaperCheckpointError, PaperCheckpointPersistenceEvidence, PaperExecutionCheckpoint,
    PaperExecutionSnapshot, PaperFillSnapshot, PaperOrderSnapshot,
};
pub use state::{PaperOrderLifecycle, PaperOrderState, PaperStateError};
