#[path = "budget/identity.rs"]
mod budget_identity;
#[path = "budget/runtime_types.rs"]
mod budget_runtime_types;
#[path = "budget/retry_after.rs"]
mod budget_retry_after;
#[path = "budget/runtime.rs"]
mod budget_runtime;
#[path = "budget/checkpoint.rs"]
mod budget_checkpoint;
#[path = "budget/coordinator.rs"]
mod budget_coordinator;
#[path = "budget/persistence.rs"]
pub(crate) mod persistence;

pub use budget_identity::{
    BackoffPolicy, BudgetScope, BudgetWindowSemantics, ProviderBudgetPolicy,
    ProviderBudgetWindow,
};
pub(crate) use budget_identity::{
    BudgetCollisionKey, BudgetPolicyResolutionError, PersistedProviderBudgetPolicy,
    ResolvedProviderBudgetPolicy,
};
pub(in crate::policy) use budget_identity::BudgetCollisionMergeError;
pub(in crate::policy) use budget_identity::{
    MAX_PROVIDER_BUDGET_WINDOWS, MAX_SLIDING_WINDOW_RELEASES,
};
pub use budget_runtime::SharedProviderBudget;
pub(in crate::policy) use budget_runtime::evaluate_budget_windows;
pub use budget_retry_after::apply_http_retry_after;
pub(in crate::policy) use budget_runtime::RuntimeOperationAdmission;
pub use budget_runtime_types::{
    BudgetDecision, BudgetUnavailableReason, MonotonicInstant, RetryAfter,
};
pub(in crate::policy) use budget_runtime_types::{
    BudgetAllocation, BudgetDurabilityBinding, BudgetState, ClockObservation,
};
pub(in crate::policy) use budget_checkpoint::{
    checkpoint_from_runtime, runtime_state_from_checkpoint, validate_checkpoint,
};
use budget_coordinator::BudgetClock;
pub use budget_coordinator::{BudgetPermit, BudgetPoolError};
pub(crate) use budget_coordinator::{BudgetAvailabilityLease, ProviderBudgetPool};
pub(in crate::policy) use budget_coordinator::CleanShutdownProof;
pub(crate) use persistence::{AuthorityPersistenceError, AuthorityStateStore};
pub(crate) use persistence::{
    AuthorityDurabilitySession, BudgetCheckpointState, BudgetWindowCheckpointState,
    DurableBudgetGroup,
};
pub(in crate::policy) use persistence::lifecycle::AuthorityOperationAdmission;
