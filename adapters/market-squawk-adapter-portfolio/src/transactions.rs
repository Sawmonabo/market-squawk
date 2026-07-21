//! Checked portfolio transactions and derived cash-flow observations.

use market_squawk_domain::{AccountId, InstrumentId, Money, SourceIdentifier, Timestamp};
use serde::Serialize;

use crate::{LotMethod, SignedQuantity};

/// Closed normalized transaction classification.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionKind {
    /// A security or digital-asset trade.
    Trade,
    /// An external cash deposit or withdrawal.
    CashTransfer,
    /// Dividend, interest, staking, or other source-classified income.
    Income,
    /// A charged commission or fee.
    Fee,
    /// A source-recorded corporate action affecting the account.
    CorporateAction,
}

/// One checked source transaction.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PortfolioTransaction {
    broker_transaction_id: SourceIdentifier,
    account_id: AccountId,
    instrument_id: Option<InstrumentId>,
    kind: TransactionKind,
    amount: Money,
    quantity: Option<SignedQuantity>,
    occurred_at: Timestamp,
    lot_method: Option<LotMethod>,
    source_reference: SourceIdentifier,
}

impl PortfolioTransaction {
    #[allow(
        clippy::too_many_arguments,
        reason = "transaction evidence stays explicit"
    )]
    pub(crate) const fn new(
        broker_transaction_id: SourceIdentifier,
        account_id: AccountId,
        instrument_id: Option<InstrumentId>,
        kind: TransactionKind,
        amount: Money,
        quantity: Option<SignedQuantity>,
        occurred_at: Timestamp,
        lot_method: Option<LotMethod>,
        source_reference: SourceIdentifier,
    ) -> Self {
        Self {
            broker_transaction_id,
            account_id,
            instrument_id,
            kind,
            amount,
            quantity,
            occurred_at,
            lot_method,
            source_reference,
        }
    }

    /// Returns the bounded provider transaction identity.
    pub const fn broker_transaction_id(&self) -> &SourceIdentifier {
        &self.broker_transaction_id
    }

    /// Returns the checked stable account identity.
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }

    /// Returns the checked instrument identity when the transaction is instrument-scoped.
    pub const fn instrument_id(&self) -> Option<InstrumentId> {
        self.instrument_id
    }

    /// Returns the closed normalized classification.
    pub const fn kind(&self) -> TransactionKind {
        self.kind
    }

    /// Returns exact signed transaction money.
    pub const fn amount(&self) -> Money {
        self.amount
    }

    /// Returns an exact signed quantity when supplied.
    pub const fn quantity(&self) -> Option<SignedQuantity> {
        self.quantity
    }

    /// Returns the checked source event timestamp.
    pub const fn occurred_at(&self) -> Timestamp {
        self.occurred_at
    }

    /// Returns the explicit lot method for a trade.
    pub const fn lot_method(&self) -> Option<LotMethod> {
        self.lot_method
    }

    /// Returns the immutable raw source reference.
    pub const fn source_reference(&self) -> &SourceIdentifier {
        &self.source_reference
    }
}

/// Cash-flow classification derived from a source transaction without changing its amount.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CashFlowKind {
    /// External cash transfer.
    Transfer,
    /// Income received.
    Income,
    /// Fee paid.
    Fee,
}

/// One exact cash flow linked to its immutable source transaction.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CashFlowObservation {
    account_id: AccountId,
    instrument_id: Option<InstrumentId>,
    kind: CashFlowKind,
    amount: Money,
    occurred_at: Timestamp,
    source_reference: SourceIdentifier,
}

impl CashFlowObservation {
    pub(crate) const fn new(
        account_id: AccountId,
        instrument_id: Option<InstrumentId>,
        kind: CashFlowKind,
        amount: Money,
        occurred_at: Timestamp,
        source_reference: SourceIdentifier,
    ) -> Self {
        Self {
            account_id,
            instrument_id,
            kind,
            amount,
            occurred_at,
            source_reference,
        }
    }

    /// Returns the checked stable account identity.
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }

    /// Returns the checked instrument identity when supplied.
    pub const fn instrument_id(&self) -> Option<InstrumentId> {
        self.instrument_id
    }

    /// Returns the closed cash-flow classification.
    pub const fn kind(&self) -> CashFlowKind {
        self.kind
    }

    /// Returns exact signed cash-flow money.
    pub const fn amount(&self) -> Money {
        self.amount
    }

    /// Returns the checked source event timestamp.
    pub const fn occurred_at(&self) -> Timestamp {
        self.occurred_at
    }

    /// Returns the immutable raw source reference.
    pub const fn source_reference(&self) -> &SourceIdentifier {
        &self.source_reference
    }
}
