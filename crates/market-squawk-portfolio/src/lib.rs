#![forbid(unsafe_code)]
//! Deterministic portfolio accounting and analytics over immutable source evidence.
//!
//! The crate deliberately exposes read-only revisions and rebalance proposals. It contains no
//! order, approval, dispatch, credential, or live-execution authority.

mod accounting;
mod attribution;
mod evidence;
mod exposure;
mod ledger;
mod lots;
mod performance;
mod publication;
mod rebalance;
mod reconcile;
mod risk;
mod transaction;

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;

use market_squawk_domain::{AccountId, Currency, Money};
use rust_decimal::Decimal;
use thiserror::Error;

pub use attribution::{AttributionInput, AttributionLine, AttributionReport};
pub use evidence::{
    CashBalance, CorporateActionBinding, FeatureBinding, FxRateEvidence, PortfolioRevision,
    PortfolioRevisionId, PortfolioRevisionToken, Position, PriceEvidence, RevisionEvidence,
    ValuationSet,
};
pub use exposure::{ExposureLine, ExposureReport, FactorLoading, InstrumentClassification};
pub use ledger::PortfolioLedger;
pub use lots::{Lot, LotDirection, LotSelection};
pub use performance::{
    CashFlowTiming, MoneyWeightedMethod, PerformancePeriod, PerformancePolicy, PerformanceReport,
};
pub use rebalance::{
    ProposedTrade, RebalanceConstraintInput, RebalanceConstraints, RebalanceProposal,
    RebalanceTarget,
};
pub use reconcile::{
    ReconciliationDiscrepancy, ReconciliationField, ReconciliationTolerance, SourcePortfolioTotals,
};
pub use risk::{PortfolioRiskReport, ScenarioDefinition, ScenarioResult};
pub use transaction::{
    CashFlow, CashFlowKind, LedgerEntry, LedgerEntryKind, Task10EconomicKind,
    Task10TransactionInstruction, Trade, TradeSide, TransactionRevision,
};

const HARD_MAX_ACCOUNTS: usize = 16_384;
const HARD_MAX_INSTRUMENTS: usize = 1_000_000;
const HARD_MAX_LOTS: usize = 4_000_000;
const HARD_MAX_TRANSACTIONS: usize = 4_000_000;
const HARD_MAX_FACTORS: usize = 16_384;
const HARD_MAX_SCENARIOS: usize = 16_384;
const HARD_MAX_HISTORY: usize = 65_536;
const HARD_MAX_RESULTS: usize = 1_000_000;
const HARD_MAX_RETAINED_BYTES: usize = 512 * 1024 * 1024;

/// Caller-selected portfolio work and retained-memory limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PortfolioLimitInput {
    /// Maximum accounts represented by an operation.
    pub max_accounts: usize,
    /// Maximum distinct instruments represented by an operation.
    pub max_instruments: usize,
    /// Maximum open tax lots retained by a revision.
    pub max_lots: usize,
    /// Maximum logical transaction histories retained by a ledger.
    pub max_transactions: usize,
    /// Maximum factor dimensions retained by an analytics result.
    pub max_factors: usize,
    /// Maximum scenario definitions retained by a risk result.
    pub max_scenarios: usize,
    /// Maximum immutable revisions retained by a ledger or service operation.
    pub max_history: usize,
    /// Maximum result rows returned by a bounded operation.
    pub max_results: usize,
    /// Maximum estimated Rust-visible bytes retained by one result.
    pub max_retained_bytes: usize,
}

/// Validated portfolio work and memory limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PortfolioLimits {
    pub(crate) max_accounts: usize,
    pub(crate) max_instruments: usize,
    pub(crate) max_lots: usize,
    pub(crate) max_transactions: usize,
    pub(crate) max_factors: usize,
    pub(crate) max_scenarios: usize,
    pub(crate) max_history: usize,
    pub(crate) max_results: usize,
    pub(crate) max_retained_bytes: usize,
}

impl PortfolioLimits {
    /// Validates positive caller limits against fixed process ceilings.
    ///
    /// # Errors
    ///
    /// Returns [`PortfolioError::InvalidLimits`] for zero or excessive values.
    pub fn try_new(input: PortfolioLimitInput) -> Result<Self, PortfolioError> {
        let values = [
            (input.max_accounts, HARD_MAX_ACCOUNTS),
            (input.max_instruments, HARD_MAX_INSTRUMENTS),
            (input.max_lots, HARD_MAX_LOTS),
            (input.max_transactions, HARD_MAX_TRANSACTIONS),
            (input.max_factors, HARD_MAX_FACTORS),
            (input.max_scenarios, HARD_MAX_SCENARIOS),
            (input.max_history, HARD_MAX_HISTORY),
            (input.max_results, HARD_MAX_RESULTS),
            (input.max_retained_bytes, HARD_MAX_RETAINED_BYTES),
        ];
        if values
            .into_iter()
            .any(|(value, ceiling)| value == 0 || value > ceiling)
        {
            return Err(PortfolioError::InvalidLimits);
        }
        Ok(Self {
            max_accounts: input.max_accounts,
            max_instruments: input.max_instruments,
            max_lots: input.max_lots,
            max_transactions: input.max_transactions,
            max_factors: input.max_factors,
            max_scenarios: input.max_scenarios,
            max_history: input.max_history,
            max_results: input.max_results,
            max_retained_bytes: input.max_retained_bytes,
        })
    }
}

/// Typed portfolio construction, accounting, evidence, and analytics failures.
#[derive(Clone, Debug, Error, PartialEq)]
pub enum PortfolioError {
    /// A caller-selected bound is zero or above its fixed ceiling.
    #[error("portfolio limits are invalid")]
    InvalidLimits,
    /// An operation would exceed a caller-selected item bound.
    #[error("portfolio {resource} count {observed} exceeds limit {limit}")]
    LimitExceeded {
        /// Bounded resource family.
        resource: &'static str,
        /// Submitted or generated count.
        observed: usize,
        /// Caller-selected limit.
        limit: usize,
    },
    /// Estimated retained memory exceeds the caller-selected bound.
    #[error("portfolio retained bytes {observed} exceed limit {limit}")]
    RetainedBytesExceeded {
        /// Estimated retained bytes.
        observed: usize,
        /// Caller-selected byte limit.
        limit: usize,
    },
    /// Fallible allocation reservation failed.
    #[error("portfolio bounded allocation failed")]
    AllocationFailed,
    /// Checked decimal, integer, or size arithmetic failed.
    #[error("portfolio checked arithmetic failed")]
    Arithmetic,
    /// A transaction belongs to another account.
    #[error("portfolio account binding does not match")]
    AccountMismatch,
    /// Currency evidence is absent or inconsistent.
    #[error("portfolio currency or FX binding does not match")]
    CurrencyMismatch,
    /// A required price is absent or does not match valuation evidence.
    #[error("portfolio valuation price is missing or inconsistent")]
    MissingPrice,
    /// Exact source/dataset/as-of evidence does not bind the requested operation.
    #[error("portfolio revision evidence does not match")]
    EvidenceMismatch,
    /// A transaction revision was already observed.
    #[error("portfolio transaction revision is duplicated")]
    DuplicateTransactionRevision,
    /// A logical transaction correction omitted a supersession pointer.
    #[error("portfolio correction must supersede the active transaction revision")]
    SupersessionRequired,
    /// A correction does not supersede the active revision exactly.
    #[error("portfolio correction supersession does not match")]
    SupersessionMismatch,
    /// A correction does not strictly advance its logical transaction revision.
    #[error("portfolio transaction revision must strictly increase")]
    NonIncreasingRevision,
    /// A transaction's fields violate its closed economic classification.
    #[error("portfolio transaction is invalid")]
    InvalidTransaction,
    /// A requested lot identity is missing, duplicated, or belongs to the wrong direction.
    #[error("portfolio specific lot selection is invalid")]
    InvalidLotSelection,
    /// A disposal exceeds open inventory for the requested direction.
    #[error("portfolio inventory is insufficient")]
    InsufficientInventory,
    /// A corporate-action plan contains unresolved economics.
    #[error("portfolio corporate action has unresolved economics")]
    UnresolvedCorporateAction,
    /// A report input does not bind the supplied immutable revision.
    #[error("portfolio analytics revision binding does not match")]
    RevisionMismatch,
    /// A policy or dimension input is invalid.
    #[error("portfolio analytics policy or dimension is invalid")]
    InvalidPolicy,
    /// A required target, classification, or dimension is absent or duplicated.
    #[error("portfolio analytical dimension is missing or duplicated")]
    InvalidDimension,
    /// A Task 10 record requires a caller policy that was not supplied.
    #[error("normalized portfolio import cannot be interpreted without an explicit policy")]
    AmbiguousNormalizedRecord,
    /// A Task 10 reconciliation operation failed.
    #[error("portfolio reconciliation failed")]
    Reconciliation,
    /// A Task 12 pure analytical kernel rejected the bounded input.
    #[error("portfolio analytical kernel rejected input")]
    Analytics,
}

/// Caller-selected limits for the immutable portfolio read service.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PortfolioServiceLimitInput {
    /// Maximum accounts retained by the service.
    pub max_accounts: NonZeroUsize,
    /// Maximum immutable history entries retained per account.
    pub max_history_per_account: NonZeroUsize,
    /// Maximum DTO rows returned by a query.
    pub max_results: NonZeroUsize,
    /// Maximum retained bytes in service state or one DTO.
    pub max_retained_bytes: NonZeroUsize,
}

/// Validated limits for [`PortfolioService`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PortfolioServiceLimits {
    max_accounts: usize,
    max_history_per_account: usize,
    max_results: usize,
    max_retained_bytes: usize,
}

impl PortfolioServiceLimits {
    /// Validates service bounds against portfolio process ceilings.
    ///
    /// # Errors
    ///
    /// Returns [`PortfolioServiceError::InvalidLimits`] for excessive values.
    pub fn try_new(input: PortfolioServiceLimitInput) -> Result<Self, PortfolioServiceError> {
        if input.max_accounts.get() > HARD_MAX_ACCOUNTS
            || input.max_history_per_account.get() > HARD_MAX_HISTORY
            || input.max_results.get() > HARD_MAX_RESULTS
            || input.max_retained_bytes.get() > HARD_MAX_RETAINED_BYTES
        {
            return Err(PortfolioServiceError::InvalidLimits);
        }
        Ok(Self {
            max_accounts: input.max_accounts.get(),
            max_history_per_account: input.max_history_per_account.get(),
            max_results: input.max_results.get(),
            max_retained_bytes: input.max_retained_bytes.get(),
        })
    }
}

/// A bounded immutable-revision query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortfolioQuery {
    account_id: AccountId,
    revision: PortfolioRevisionToken,
    max_results: usize,
    max_retained_bytes: usize,
}

impl PortfolioQuery {
    /// Constructs a query carrying the caller's opaque current-revision precondition.
    ///
    /// # Errors
    ///
    /// Rejects values above fixed process ceilings.
    pub fn try_new(
        account_id: AccountId,
        revision: PortfolioRevisionToken,
        max_results: NonZeroUsize,
        max_retained_bytes: NonZeroUsize,
    ) -> Result<Self, PortfolioServiceError> {
        if max_results.get() > HARD_MAX_RESULTS
            || max_retained_bytes.get() > HARD_MAX_RETAINED_BYTES
        {
            return Err(PortfolioServiceError::InvalidLimits);
        }
        Ok(Self {
            account_id,
            revision,
            max_results: max_results.get(),
            max_retained_bytes: max_retained_bytes.get(),
        })
    }
}

/// One bounded, read-only portfolio DTO.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortfolioSnapshot {
    revision: PortfolioRevisionToken,
    account_id: AccountId,
    base_currency: Currency,
    cash: Money,
    holdings: Vec<Position>,
    retained_bytes: usize,
}

impl PortfolioSnapshot {
    /// Returns the opaque revision precondition satisfied by this snapshot.
    pub const fn revision(&self) -> &PortfolioRevisionToken {
        &self.revision
    }

    /// Returns the canonical account identity.
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }

    /// Returns the reporting currency.
    pub const fn base_currency(&self) -> Currency {
        self.base_currency
    }

    /// Returns base-currency cash, including explicit FX valuation.
    pub const fn cash(&self) -> Money {
        self.cash
    }

    /// Returns bounded position DTOs in stable instrument order.
    pub fn holdings(&self) -> &[Position] {
        &self.holdings
    }

    /// Returns estimated bytes retained by this DTO.
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}

/// Read-service construction and fail-closed revision-precondition failures.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum PortfolioServiceError {
    /// Service limits exceed fixed process ceilings.
    #[error("portfolio service limits are invalid")]
    InvalidLimits,
    /// No account revision is published for the requested account.
    #[error("portfolio account is missing")]
    MissingAccount,
    /// The query omitted its required opaque revision precondition.
    #[error("portfolio query is missing its revision precondition")]
    MissingPrecondition,
    /// The supplied revision has been explicitly revoked.
    #[error("portfolio revision is revoked")]
    RevokedRevision,
    /// The supplied revision is valid but is not current.
    #[error("portfolio revision is stale")]
    StaleRevision,
    /// The query's requested row count is smaller than the required result.
    #[error("portfolio result has {observed} rows; query limit is {limit}")]
    ResultLimitExceeded {
        /// Required result rows.
        observed: usize,
        /// Query limit.
        limit: usize,
    },
    /// Service state or a DTO exceeds its byte ceiling.
    #[error("portfolio service retained bytes exceed the configured limit")]
    RetainedBytesExceeded,
    /// Duplicate or conflicting account revisions were supplied.
    #[error("portfolio service revision set is inconsistent")]
    InconsistentRevisions,
    /// Bounded allocation failed.
    #[error("portfolio service bounded allocation failed")]
    AllocationFailed,
    /// Checked retained-size arithmetic failed.
    #[error("portfolio service retained-size arithmetic failed")]
    Arithmetic,
}

/// Immutable read-only portfolio revision service.
#[derive(Clone, Debug)]
pub struct PortfolioService {
    current: BTreeMap<AccountId, PortfolioRevision>,
    revoked: BTreeSet<PortfolioRevisionToken>,
    limits: PortfolioServiceLimits,
}

impl PortfolioService {
    /// Constructs a bounded service from one current revision per account.
    ///
    /// # Errors
    ///
    /// Rejects duplicate accounts, excessive state, current revocations, or allocation failure.
    pub fn try_new(
        revisions: Vec<PortfolioRevision>,
        revoked: Vec<PortfolioRevisionToken>,
        limits: PortfolioServiceLimits,
    ) -> Result<Self, PortfolioServiceError> {
        if revisions.len() > limits.max_accounts
            || revoked.len()
                > limits
                    .max_history_per_account
                    .saturating_mul(limits.max_accounts)
        {
            return Err(PortfolioServiceError::InvalidLimits);
        }
        let mut current = BTreeMap::new();
        for revision in revisions {
            if current.insert(revision.account_id(), revision).is_some() {
                return Err(PortfolioServiceError::InconsistentRevisions);
            }
        }
        let revoked = revoked.into_iter().collect::<BTreeSet<_>>();
        if current
            .values()
            .any(|revision| revoked.contains(&revision.token()))
        {
            return Err(PortfolioServiceError::InconsistentRevisions);
        }
        let retained = current
            .values()
            .try_fold(0_usize, |total, revision| {
                total.checked_add(revision.retained_bytes())
            })
            .ok_or(PortfolioServiceError::Arithmetic)?;
        if retained > limits.max_retained_bytes {
            return Err(PortfolioServiceError::RetainedBytesExceeded);
        }
        Ok(Self {
            current,
            revoked,
            limits,
        })
    }

    /// Returns the opaque current revision token for one account.
    ///
    /// # Errors
    ///
    /// Returns [`PortfolioServiceError::MissingAccount`] for an unpublished account.
    pub fn head(
        &self,
        account_id: AccountId,
    ) -> Result<PortfolioRevisionToken, PortfolioServiceError> {
        self.current
            .get(&account_id)
            .map(PortfolioRevision::token)
            .ok_or(PortfolioServiceError::MissingAccount)
    }

    /// Returns a bounded immutable snapshot after validating a current revision precondition.
    ///
    /// # Errors
    ///
    /// Rejects missing, stale, revoked, excessive, or account-mismatched preconditions.
    pub fn query(
        &self,
        query: Option<PortfolioQuery>,
    ) -> Result<PortfolioSnapshot, PortfolioServiceError> {
        let query = query.ok_or(PortfolioServiceError::MissingPrecondition)?;
        if self.revoked.contains(&query.revision) {
            return Err(PortfolioServiceError::RevokedRevision);
        }
        let revision = self
            .current
            .get(&query.account_id)
            .ok_or(PortfolioServiceError::MissingAccount)?;
        if revision.token() != query.revision {
            return Err(PortfolioServiceError::StaleRevision);
        }
        let limit = query.max_results.min(self.limits.max_results);
        if revision.positions().len() > limit {
            return Err(PortfolioServiceError::ResultLimitExceeded {
                observed: revision.positions().len(),
                limit,
            });
        }
        let mut holdings = Vec::new();
        holdings
            .try_reserve_exact(revision.positions().len())
            .map_err(|_| PortfolioServiceError::AllocationFailed)?;
        holdings.extend_from_slice(revision.positions());
        let retained_bytes = std::mem::size_of::<PortfolioSnapshot>()
            .checked_add(
                holdings
                    .capacity()
                    .checked_mul(std::mem::size_of::<Position>())
                    .ok_or(PortfolioServiceError::Arithmetic)?,
            )
            .ok_or(PortfolioServiceError::Arithmetic)?;
        let byte_limit = query.max_retained_bytes.min(self.limits.max_retained_bytes);
        if retained_bytes > byte_limit {
            return Err(PortfolioServiceError::RetainedBytesExceeded);
        }
        Ok(PortfolioSnapshot {
            revision: revision.token(),
            account_id: revision.account_id(),
            base_currency: revision.base_currency(),
            cash: revision.cash(),
            holdings,
            retained_bytes,
        })
    }
}

pub(crate) fn checked_decimal_add(
    left: Decimal,
    right: Decimal,
) -> Result<Decimal, PortfolioError> {
    left.checked_add(right)
        .map(|value| value.normalize())
        .ok_or(PortfolioError::Arithmetic)
}

pub(crate) fn checked_decimal_sub(
    left: Decimal,
    right: Decimal,
) -> Result<Decimal, PortfolioError> {
    left.checked_sub(right)
        .map(|value| value.normalize())
        .ok_or(PortfolioError::Arithmetic)
}

pub(crate) fn checked_decimal_mul(
    left: Decimal,
    right: Decimal,
) -> Result<Decimal, PortfolioError> {
    left.checked_mul(right)
        .map(|value| value.normalize())
        .ok_or(PortfolioError::Arithmetic)
}

pub(crate) fn checked_decimal_div(
    left: Decimal,
    right: Decimal,
) -> Result<Decimal, PortfolioError> {
    if right.is_zero() {
        return Err(PortfolioError::Arithmetic);
    }
    left.checked_div(right)
        .map(|value| value.normalize())
        .ok_or(PortfolioError::Arithmetic)
}
