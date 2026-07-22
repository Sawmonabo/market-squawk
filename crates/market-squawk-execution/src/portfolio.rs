//! Lock-free, read-only portfolio authority used by risk and dispatch.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arc_swap::ArcSwap;
use market_squawk_domain::{AccountId, Currency, InstrumentId, Money, OrderSide};
use market_squawk_portfolio::{
    BasisMeasurement, LotDirection, PortfolioQuery, PortfolioRevisionToken, PortfolioService,
    PortfolioServiceError, PortfolioServiceLimitInput, PortfolioServiceLimits, PortfolioSnapshot,
};
use rust_decimal::Decimal;
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Fixed query bounds retained with an execution portfolio capability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PortfolioReadLimits {
    max_results: NonZeroUsize,
    max_retained_bytes: NonZeroUsize,
}

impl PortfolioReadLimits {
    /// Constructs positive portfolio query limits.
    pub const fn new(max_results: NonZeroUsize, max_retained_bytes: NonZeroUsize) -> Self {
        Self {
            max_results,
            max_retained_bytes,
        }
    }
}

impl Default for PortfolioReadLimits {
    fn default() -> Self {
        Self {
            max_results: NonZeroUsize::new(4_096).unwrap_or(NonZeroUsize::MIN),
            max_retained_bytes: NonZeroUsize::new(4 * 1024 * 1024).unwrap_or(NonZeroUsize::MIN),
        }
    }
}

#[derive(Debug)]
struct PortfolioAuthority {
    service: ArcSwap<PortfolioService>,
    revoked: AtomicBool,
}

/// Non-cloneable owner that atomically publishes replacement immutable portfolio services.
#[derive(Debug)]
pub struct PortfolioServicePublisher {
    authority: Arc<PortfolioAuthority>,
}

impl PortfolioServicePublisher {
    /// Atomically replaces the immutable service observed by risk and pending dispatches.
    ///
    /// # Errors
    ///
    /// Returns [`PortfolioReadError::RevokedCapability`] after terminal revocation.
    pub fn publish(&self, service: PortfolioService) -> Result<(), PortfolioReadError> {
        if self.authority.revoked.load(Ordering::Acquire) {
            return Err(PortfolioReadError::RevokedCapability);
        }
        self.authority.service.store(Arc::new(service));
        if self.authority.revoked.load(Ordering::Acquire) {
            return Err(PortfolioReadError::RevokedCapability);
        }
        Ok(())
    }

    /// Permanently revokes every derived read capability.
    pub fn revoke(&self) {
        self.authority.revoked.store(true, Ordering::Release);
    }
}

/// Cloneable read-only capability shared by risk and approvals without publication authority.
#[derive(Clone, Debug)]
pub struct PortfolioReadCapability {
    authority: Arc<PortfolioAuthority>,
    limits: PortfolioReadLimits,
}

/// Creates one publication owner and its bounded read-only execution capability.
pub fn portfolio_execution_state(
    initial: PortfolioService,
    limits: PortfolioReadLimits,
) -> (PortfolioServicePublisher, PortfolioReadCapability) {
    let authority = Arc::new(PortfolioAuthority {
        service: ArcSwap::from_pointee(initial),
        revoked: AtomicBool::new(false),
    });
    (
        PortfolioServicePublisher {
            authority: Arc::clone(&authority),
        },
        PortfolioReadCapability { authority, limits },
    )
}

/// Exact immutable portfolio identity captured by risk and rechecked before dispatch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortfolioRiskBinding {
    revision: PortfolioRevisionToken,
    content_digest: [u8; 32],
    account_id: AccountId,
    base_currency: Currency,
}

impl PortfolioRiskBinding {
    /// Returns the exact opaque portfolio revision precondition.
    pub const fn revision(&self) -> &PortfolioRevisionToken {
        &self.revision
    }

    /// Returns the stable digest of the complete bounded portfolio snapshot used by risk.
    pub const fn content_digest(&self) -> [u8; 32] {
        self.content_digest
    }

    /// Returns the portfolio account bound to the approval.
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }

    /// Returns the reporting currency bound to the approval.
    pub const fn base_currency(&self) -> Currency {
        self.base_currency
    }

    pub(crate) fn approval_digest(
        &self,
        intent_digest: crate::OrderIntentDigest,
        price_bound: crate::ExecutionPriceBound,
    ) -> [u8; 32] {
        price_bound.portfolio_bound_audit_digest(intent_digest, self.content_digest)
    }
}

/// Fail-closed portfolio authority, query, and execution-binding failures.
#[derive(Clone, Copy, Debug, Eq, Error, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PortfolioReadError {
    /// Publication authority permanently revoked the capability.
    #[error("portfolio execution capability is revoked")]
    RevokedCapability,
    /// No current revision exists for the intent account.
    #[error("portfolio account is missing")]
    MissingAccount,
    /// The approved portfolio revision is no longer current.
    #[error("portfolio revision is stale")]
    StaleRevision,
    /// The approved revision was explicitly revoked.
    #[error("portfolio revision is revoked")]
    RevokedRevision,
    /// Portfolio service bounds or retained state rejected the query.
    #[error("portfolio query exceeded its execution bound")]
    QueryBound,
    /// Intent, portfolio, and risk-policy currencies do not agree.
    #[error("portfolio execution currency does not match")]
    CurrencyMismatch,
    /// A basis-dependent disposal lacks complete portfolio evidence.
    #[error("portfolio position basis is incomplete")]
    IncompleteBasis,
    /// The current snapshot no longer matches the approved content identity.
    #[error("portfolio snapshot content does not match the approved binding")]
    ContentMismatch,
}

impl PortfolioReadCapability {
    /// Creates an immutable fail-closed capability for market-only diagnostics.
    ///
    /// This capability contains no account revisions, has no publication owner, and therefore
    /// always rejects execution risk with [`PortfolioReadError::MissingAccount`].
    ///
    /// # Errors
    ///
    /// Returns [`PortfolioReadError::QueryBound`] when the supplied limits exceed portfolio
    /// service ceilings.
    pub fn unavailable(limits: PortfolioReadLimits) -> Result<Self, PortfolioReadError> {
        let service_limits = PortfolioServiceLimits::try_new(PortfolioServiceLimitInput {
            max_accounts: NonZeroUsize::MIN,
            max_history_per_account: NonZeroUsize::MIN,
            max_results: limits.max_results,
            max_retained_bytes: limits.max_retained_bytes,
        })
        .map_err(map_service_error)?;
        let service = PortfolioService::try_new(Vec::new(), Vec::new(), service_limits)
            .map_err(map_service_error)?;
        Ok(portfolio_execution_state(service, limits).1)
    }

    /// Loads and validates the authoritative current snapshot for one prospective order.
    ///
    /// The returned binding is audit evidence only; [`crate::RiskService`] always performs this
    /// lookup itself and never accepts a caller-provided binding.
    pub fn bind_current(
        &self,
        account_id: AccountId,
        instrument_id: InstrumentId,
        side: OrderSide,
        execution_currency: Currency,
    ) -> Result<(PortfolioRiskBinding, PortfolioSnapshot), PortfolioReadError> {
        self.ensure_current()?;
        let service = self.authority.service.load_full();
        let revision = service.head(account_id).map_err(map_service_error)?;
        let snapshot = service
            .query(Some(
                PortfolioQuery::try_new(
                    account_id,
                    revision,
                    self.limits.max_results,
                    self.limits.max_retained_bytes,
                )
                .map_err(map_service_error)?,
            ))
            .map_err(map_service_error)?;
        self.ensure_service_current(&service)?;
        validate_order_snapshot(&snapshot, instrument_id, side, execution_currency)?;
        Ok((binding_from_snapshot(&snapshot), snapshot))
    }

    /// Rechecks an exact prior binding against the atomically current service.
    pub fn recheck(&self, binding: &PortfolioRiskBinding) -> Result<(), PortfolioReadError> {
        self.ensure_current()?;
        let service = self.authority.service.load_full();
        let snapshot = service
            .query(Some(
                PortfolioQuery::try_new(
                    binding.account_id,
                    binding.revision.clone(),
                    self.limits.max_results,
                    self.limits.max_retained_bytes,
                )
                .map_err(map_service_error)?,
            ))
            .map_err(map_service_error)?;
        self.ensure_service_current(&service)?;
        if binding_from_snapshot(&snapshot).content_digest != binding.content_digest {
            return Err(PortfolioReadError::ContentMismatch);
        }
        Ok(())
    }

    fn ensure_current(&self) -> Result<(), PortfolioReadError> {
        if self.authority.revoked.load(Ordering::Acquire) {
            Err(PortfolioReadError::RevokedCapability)
        } else {
            Ok(())
        }
    }

    fn ensure_service_current(
        &self,
        observed: &Arc<PortfolioService>,
    ) -> Result<(), PortfolioReadError> {
        self.ensure_current()?;
        let current = self.authority.service.load_full();
        if !Arc::ptr_eq(observed, &current) {
            return Err(PortfolioReadError::StaleRevision);
        }
        self.ensure_current()
    }
}

fn validate_order_snapshot(
    snapshot: &PortfolioSnapshot,
    instrument_id: InstrumentId,
    side: OrderSide,
    execution_currency: Currency,
) -> Result<(), PortfolioReadError> {
    if snapshot.base_currency() != execution_currency
        || snapshot.cash().currency() != execution_currency
    {
        return Err(PortfolioReadError::CurrencyMismatch);
    }
    if side == OrderSide::Sell
        && snapshot
            .holdings()
            .iter()
            .find(|position| position.instrument_id() == instrument_id)
            .is_some_and(|position| !position.basis_complete())
    {
        return Err(PortfolioReadError::IncompleteBasis);
    }
    Ok(())
}

fn binding_from_snapshot(snapshot: &PortfolioSnapshot) -> PortfolioRiskBinding {
    PortfolioRiskBinding {
        revision: snapshot.revision().clone(),
        content_digest: snapshot_digest(snapshot),
        account_id: snapshot.account_id(),
        base_currency: snapshot.base_currency(),
    }
}

fn snapshot_digest(snapshot: &PortfolioSnapshot) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/execution-portfolio-snapshot/v1\0");
    digest.update(snapshot.revision().bytes());
    digest.update(snapshot.account_id().as_uuid().as_bytes());
    hash_money(&mut digest, snapshot.cash());
    for position in snapshot.holdings() {
        digest.update(position.instrument_id().as_uuid().as_bytes());
        hash_decimal(&mut digest, position.quantity());
        hash_basis(&mut digest, position.cost_basis());
        hash_money(&mut digest, position.market_value());
        hash_basis(&mut digest, position.unrealized_gain());
        for lot in position.lots() {
            hash_bytes(&mut digest, lot.id().as_str().as_bytes());
            digest.update(lot.instrument_id().as_uuid().as_bytes());
            digest.update([match lot.direction() {
                LotDirection::Long => 0,
                LotDirection::Short => 1,
            }]);
            digest.update(lot.opened_at().unix_nanos().to_be_bytes());
            hash_decimal(&mut digest, lot.quantity());
            hash_money(&mut digest, lot.basis());
            digest.update([u8::from(lot.basis_complete())]);
        }
    }
    digest.finalize().into()
}

fn hash_basis(digest: &mut Sha256, basis: BasisMeasurement) {
    match basis {
        BasisMeasurement::Complete(value) => {
            digest.update([1]);
            hash_money(digest, value);
        }
        BasisMeasurement::Incomplete => digest.update([0]),
    }
}

fn hash_money(digest: &mut Sha256, money: Money) {
    hash_decimal(digest, money.amount());
    hash_bytes(digest, money.currency().as_str().as_bytes());
}

fn hash_decimal(digest: &mut Sha256, value: Decimal) {
    hash_bytes(digest, value.normalize().to_string().as_bytes());
}

fn hash_bytes(digest: &mut Sha256, value: &[u8]) {
    let length = u64::try_from(value.len()).unwrap_or(u64::MAX);
    digest.update(length.to_be_bytes());
    digest.update(value);
}

fn map_service_error(error: PortfolioServiceError) -> PortfolioReadError {
    match error {
        PortfolioServiceError::MissingAccount | PortfolioServiceError::MissingPrecondition => {
            PortfolioReadError::MissingAccount
        }
        PortfolioServiceError::RevokedRevision => PortfolioReadError::RevokedRevision,
        PortfolioServiceError::StaleRevision => PortfolioReadError::StaleRevision,
        PortfolioServiceError::InvalidLimits
        | PortfolioServiceError::ResultLimitExceeded { .. }
        | PortfolioServiceError::RetainedBytesExceeded
        | PortfolioServiceError::InconsistentRevisions
        | PortfolioServiceError::AllocationFailed
        | PortfolioServiceError::Arithmetic => PortfolioReadError::QueryBound,
    }
}
