//! Checked account, holding, and source cost-basis observations.

use std::fmt;

use market_squawk_domain::{
    AccountId, Currency, InstrumentId, LotSize, Money, SourceIdentifier, Timestamp,
};
use rust_decimal::Decimal;
use serde::Serialize;

use crate::PortfolioImportError;

/// A nonzero exact portfolio quantity whose sign is retained.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct SignedQuantity(Decimal);

impl SignedQuantity {
    pub(crate) fn try_new(value: Decimal) -> Result<Self, PortfolioImportError> {
        if value.is_zero() {
            Err(PortfolioImportError::ZeroQuantity)
        } else {
            Ok(Self(value.normalize()))
        }
    }

    /// Returns the signed exact decimal quantity.
    pub const fn as_decimal(self) -> Decimal {
        self.0
    }

    pub(crate) fn absolute(self) -> Decimal {
        self.0.abs()
    }
}

impl fmt::Display for SignedQuantity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Explicit source lot-selection method.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LotMethod {
    /// First acquired units are disposed first.
    Fifo,
    /// Last acquired units are disposed first.
    Lifo,
    /// The source identified the exact disposed lots.
    SpecificIdentification,
    /// A source-defined average-cost pool is used.
    AverageCost,
}

/// One stable account and exact source cash observation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AccountObservation {
    account_id: AccountId,
    currency: Currency,
    cash_balance: Money,
    as_of: Timestamp,
    source_reference: SourceIdentifier,
}

impl AccountObservation {
    pub(crate) const fn new(
        account_id: AccountId,
        currency: Currency,
        cash_balance: Money,
        as_of: Timestamp,
        source_reference: SourceIdentifier,
    ) -> Self {
        Self {
            account_id,
            currency,
            cash_balance,
            as_of,
            source_reference,
        }
    }

    /// Returns the checked stable account identity.
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }

    /// Returns the normalized account currency.
    pub const fn currency(&self) -> Currency {
        self.currency
    }

    /// Returns the exact source cash balance.
    pub const fn cash_balance(&self) -> Money {
        self.cash_balance
    }

    /// Returns the checked source as-of timestamp.
    pub const fn as_of(&self) -> Timestamp {
        self.as_of
    }

    /// Returns the immutable raw source reference.
    pub const fn source_reference(&self) -> &SourceIdentifier {
        &self.source_reference
    }
}

/// One resolved source cost basis with an explicit lot method.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CostBasisObservation {
    account_id: AccountId,
    instrument_id: InstrumentId,
    amount: Money,
    lot_method: LotMethod,
    source_reference: SourceIdentifier,
}

impl CostBasisObservation {
    pub(crate) const fn new(
        account_id: AccountId,
        instrument_id: InstrumentId,
        amount: Money,
        lot_method: LotMethod,
        source_reference: SourceIdentifier,
    ) -> Self {
        Self {
            account_id,
            instrument_id,
            amount,
            lot_method,
            source_reference,
        }
    }

    /// Returns the checked account identity.
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }

    /// Returns the checked instrument identity.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the exact nonnegative source basis.
    pub const fn amount(&self) -> Money {
        self.amount
    }

    /// Returns the explicit lot-selection method.
    pub const fn lot_method(&self) -> LotMethod {
        self.lot_method
    }

    /// Returns the immutable raw source reference.
    pub const fn source_reference(&self) -> &SourceIdentifier {
        &self.source_reference
    }
}

/// Source basis state retained without inventing a value.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum BasisResolution {
    /// One exact basis and lot method were supplied.
    Resolved { observation: CostBasisObservation },
    /// The source supplied no basis.
    Missing,
    /// The source supplied multiple basis candidates that cannot be selected silently.
    Ambiguous {
        /// Exact candidate amounts retained in source order.
        candidates: Vec<Money>,
        /// Explicit method attached to the candidates.
        lot_method: LotMethod,
    },
}

/// One normalized signed holding.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct HoldingObservation {
    account_id: AccountId,
    instrument_id: InstrumentId,
    currency: Currency,
    quantity: SignedQuantity,
    lot_size: LotSize,
    market_value: Money,
    as_of: Timestamp,
    basis: BasisResolution,
    source_reference: SourceIdentifier,
}

impl HoldingObservation {
    #[allow(
        clippy::too_many_arguments,
        reason = "financial lineage fields stay explicit"
    )]
    pub(crate) const fn new(
        account_id: AccountId,
        instrument_id: InstrumentId,
        currency: Currency,
        quantity: SignedQuantity,
        lot_size: LotSize,
        market_value: Money,
        as_of: Timestamp,
        basis: BasisResolution,
        source_reference: SourceIdentifier,
    ) -> Self {
        Self {
            account_id,
            instrument_id,
            currency,
            quantity,
            lot_size,
            market_value,
            as_of,
            basis,
            source_reference,
        }
    }

    /// Returns the checked account identity.
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }

    /// Returns the checked instrument identity.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the normalized currency.
    pub const fn currency(&self) -> Currency {
        self.currency
    }

    /// Returns the signed exact quantity.
    pub const fn quantity(&self) -> SignedQuantity {
        self.quantity
    }

    /// Returns the checked positive source lot size.
    pub const fn lot_size(&self) -> LotSize {
        self.lot_size
    }

    /// Returns the exact source market value.
    pub const fn market_value(&self) -> Money {
        self.market_value
    }

    /// Returns the checked source as-of timestamp.
    pub const fn as_of(&self) -> Timestamp {
        self.as_of
    }

    /// Returns the source basis state without resolving ambiguity.
    pub const fn basis(&self) -> &BasisResolution {
        &self.basis
    }

    /// Returns the immutable raw source reference.
    pub const fn source_reference(&self) -> &SourceIdentifier {
        &self.source_reference
    }
}
