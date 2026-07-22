//! Normalized transaction, cash-flow, and source-revision models.

use market_squawk_domain::{
    AccountId, InstrumentId, Money, NormalizedPortfolioTransactionEvidence, RevisionNumber,
    SourceIdentifier, Timestamp,
};
use rust_decimal::Decimal;

use crate::PortfolioError;
use crate::lots::LotSelection;

/// Explicit normalized logical transaction identity and correction lineage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionRevision {
    pub(crate) transaction_id: SourceIdentifier,
    pub(crate) revision: RevisionNumber,
    pub(crate) supersedes: Option<RevisionNumber>,
}

impl TransactionRevision {
    /// Constructs a logical transaction revision.
    ///
    /// # Errors
    ///
    /// Rejects self/non-advancing supersession.
    pub fn try_new(
        transaction_id: SourceIdentifier,
        revision: RevisionNumber,
        supersedes: Option<RevisionNumber>,
    ) -> Result<Self, PortfolioError> {
        if supersedes.is_some_and(|prior| prior.get() >= revision.get()) {
            return Err(PortfolioError::NonIncreasingRevision);
        }
        Ok(Self {
            transaction_id,
            revision,
            supersedes,
        })
    }

    /// Returns the stable logical source transaction identity.
    pub const fn transaction_id(&self) -> &SourceIdentifier {
        &self.transaction_id
    }

    /// Returns the one-based source revision.
    pub const fn revision(&self) -> RevisionNumber {
        self.revision
    }

    /// Returns the exact prior revision replaced by this correction.
    pub const fn supersedes(&self) -> Option<RevisionNumber> {
        self.supersedes
    }
}

/// Closed trade lifecycle side.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TradeSide {
    /// Open or add long inventory.
    Buy,
    /// Dispose long inventory.
    Sell,
    /// Open or add borrowed short inventory.
    SellShort,
    /// Dispose borrowed short inventory.
    BuyToCover,
}

/// One exact trade with explicit fee and lot policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Trade {
    pub(crate) side: TradeSide,
    pub(crate) instrument_id: InstrumentId,
    pub(crate) quantity: Decimal,
    pub(crate) price: Money,
    pub(crate) fee: Money,
    pub(crate) lot_selection: LotSelection,
}

impl Trade {
    /// Constructs a trade from positive units, nonnegative price/fee, and a validated lot policy.
    ///
    /// # Errors
    ///
    /// Rejects zero/negative values, mixed currencies, or invalid specific-lot selection.
    pub fn try_new(
        side: TradeSide,
        instrument_id: InstrumentId,
        quantity: Decimal,
        price: Money,
        fee: Money,
        lot_selection: LotSelection,
    ) -> Result<Self, PortfolioError> {
        lot_selection.validate()?;
        if quantity <= Decimal::ZERO
            || price.amount().is_sign_negative()
            || fee.amount().is_sign_negative()
            || price.currency() != fee.currency()
            || matches!(side, TradeSide::Buy | TradeSide::SellShort)
                && !matches!(lot_selection, LotSelection::Fifo)
        {
            return Err(PortfolioError::InvalidTransaction);
        }
        Ok(Self {
            side,
            instrument_id,
            quantity: quantity.normalize(),
            price,
            fee,
            lot_selection,
        })
    }

    /// Returns the closed lifecycle side.
    pub const fn side(&self) -> TradeSide {
        self.side
    }

    /// Returns the canonical instrument identity.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns strictly positive units.
    pub const fn quantity(&self) -> Decimal {
        self.quantity
    }

    /// Returns exact price per unit.
    pub const fn price(&self) -> Money {
        self.price
    }

    /// Returns a nonnegative explicit fee.
    pub const fn fee(&self) -> Money {
        self.fee
    }

    /// Returns the disposal policy.
    pub const fn lot_selection(&self) -> &LotSelection {
        &self.lot_selection
    }
}

/// Closed non-trade cash-flow classification.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CashFlowKind {
    /// External contribution.
    Deposit,
    /// External withdrawal.
    Withdrawal,
    /// Dividend income.
    Dividend,
    /// Interest income.
    Interest,
    /// Tax withholding.
    Withholding,
    /// Standalone account fee.
    Fee,
}

/// One exact positive-magnitude cash flow.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CashFlow {
    pub(crate) kind: CashFlowKind,
    pub(crate) amount: Money,
    pub(crate) instrument_id: Option<InstrumentId>,
}

impl CashFlow {
    /// Constructs a cash flow whose direction is defined only by its closed kind.
    ///
    /// # Errors
    ///
    /// Rejects negative amounts and missing dividend instrument identity.
    pub fn try_new(
        kind: CashFlowKind,
        amount: Money,
        instrument_id: Option<InstrumentId>,
    ) -> Result<Self, PortfolioError> {
        if amount.amount().is_sign_negative()
            || matches!(kind, CashFlowKind::Dividend) && instrument_id.is_none()
        {
            return Err(PortfolioError::InvalidTransaction);
        }
        Ok(Self {
            kind,
            amount,
            instrument_id,
        })
    }

    /// Returns the closed flow kind.
    pub const fn kind(self) -> CashFlowKind {
        self.kind
    }

    /// Returns the positive exact magnitude.
    pub const fn amount(self) -> Money {
        self.amount
    }

    /// Returns optional instrument scope.
    pub const fn instrument_id(self) -> Option<InstrumentId> {
        self.instrument_id
    }
}

/// Closed ledger-entry economic payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LedgerEntryKind {
    /// Buy, sell, short, or cover.
    Trade(Trade),
    /// External cash, income, withholding, or fee flow.
    CashFlow(CashFlow),
}

/// One normalized, source-bound ledger entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LedgerEntry {
    pub(crate) account_id: AccountId,
    pub(crate) transaction: TransactionRevision,
    pub(crate) occurred_at: Timestamp,
    pub(crate) source: SourceIdentifier,
    pub(crate) kind: LedgerEntryKind,
    pub(crate) normalized_evidence: Option<NormalizedPortfolioTransactionEvidence>,
}

impl LedgerEntry {
    /// Constructs one normalized ledger entry.
    ///
    /// # Errors
    ///
    /// Returns a typed error only when nested invariants are invalid.
    pub fn try_new(
        account_id: AccountId,
        transaction: TransactionRevision,
        occurred_at: Timestamp,
        source: SourceIdentifier,
        kind: LedgerEntryKind,
    ) -> Result<Self, PortfolioError> {
        Ok(Self {
            account_id,
            transaction,
            occurred_at,
            source,
            kind,
            normalized_evidence: None,
        })
    }

    pub(crate) fn from_normalized_evidence(
        transaction: TransactionRevision,
        kind: LedgerEntryKind,
        evidence: NormalizedPortfolioTransactionEvidence,
    ) -> Self {
        Self {
            account_id: evidence.account_id(),
            occurred_at: evidence.occurred_at(),
            source: evidence.raw_source_reference().clone(),
            transaction,
            kind,
            normalized_evidence: Some(evidence),
        }
    }

    /// Returns account binding.
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }

    /// Returns logical source revision and correction lineage.
    pub const fn transaction(&self) -> &TransactionRevision {
        &self.transaction
    }

    /// Returns economic event time.
    pub const fn occurred_at(&self) -> Timestamp {
        self.occurred_at
    }

    /// Returns normalized source authority identity.
    pub const fn source(&self) -> &SourceIdentifier {
        &self.source
    }

    /// Returns the closed economic payload.
    pub const fn kind(&self) -> &LedgerEntryKind {
        &self.kind
    }
}

/// Explicit economic interpretation for one normalized Task 10 transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Task10EconomicKind {
    /// Resolves only the lifecycle ambiguity left by signed Task 10 trade evidence.
    Trade {
        /// Buy/sell/short/cover interpretation consistent with the evidenced quantity sign.
        side: TradeSide,
        /// Disposal policy constrained by the evidenced source lot method.
        lot_selection: LotSelection,
    },
    /// Resolves source-classified income as dividend income.
    Dividend,
    /// Resolves source-classified income as interest income.
    Interest,
    /// Resolves source-classified income as tax withholding.
    Withholding,
}

/// Source-revision and economic policy for one Task 10 broker transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Task10TransactionInstruction {
    broker_transaction_id: SourceIdentifier,
    economic_kind: Task10EconomicKind,
}

impl Task10TransactionInstruction {
    /// Constructs one explicit normalized-record interpretation.
    ///
    /// # Errors
    ///
    /// Retains only a genuine classification/lot-policy interpretation.
    pub fn try_new(
        broker_transaction_id: SourceIdentifier,
        economic_kind: Task10EconomicKind,
    ) -> Result<Self, PortfolioError> {
        if let Task10EconomicKind::Trade { lot_selection, .. } = &economic_kind {
            lot_selection.validate()?;
        }
        Ok(Self {
            broker_transaction_id,
            economic_kind,
        })
    }

    /// Returns the stable Task 10 broker identity.
    pub const fn broker_transaction_id(&self) -> &SourceIdentifier {
        &self.broker_transaction_id
    }

    /// Returns the economic interpretation policy.
    pub const fn economic_kind(&self) -> &Task10EconomicKind {
        &self.economic_kind
    }
}
